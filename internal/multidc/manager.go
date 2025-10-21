package multidc

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/serkank2/kafkatwin/internal/cluster"
	"github.com/serkank2/kafkatwin/internal/monitoring"
)

// Manager manages multi-datacenter operations
type Manager struct {
	datacenters map[string]*Datacenter
	strategy    ReplicationStrategy
	mu          sync.RWMutex
}

// Datacenter represents a data center
type Datacenter struct {
	ID          string
	Name        string
	Region      string
	Priority    int
	Clusters    []*cluster.Cluster
	Latency     time.Duration
	Healthy     bool
	mu          sync.RWMutex
}

// ReplicationStrategy defines how data is replicated across DCs
type ReplicationStrategy string

const (
	// StrategyActiveActive replicates to all DCs simultaneously
	StrategyActiveActive ReplicationStrategy = "active-active"

	// StrategyActivePassive replicates to primary DC, others are standby
	StrategyActivePassive ReplicationStrategy = "active-passive"

	// StrategyRegionalActive replicates within region, async cross-region
	StrategyRegionalActive ReplicationStrategy = "regional-active"

	// StrategyPreferred prefers local DC, falls back to others
	StrategyPreferred ReplicationStrategy = "preferred"
)

// Config contains multi-DC configuration
type Config struct {
	Strategy        ReplicationStrategy
	LocalDC         string
	HealthCheckInterval time.Duration
	LatencyThreshold    time.Duration
	PreferLocalReads    bool
}

// NewManager creates a new multi-DC manager
func NewManager(config Config) *Manager {
	if config.HealthCheckInterval == 0 {
		config.HealthCheckInterval = 30 * time.Second
	}
	if config.LatencyThreshold == 0 {
		config.LatencyThreshold = 100 * time.Millisecond
	}

	m := &Manager{
		datacenters: make(map[string]*Datacenter),
		strategy:    config.Strategy,
	}

	// Start health monitoring
	go m.healthMonitor(config.HealthCheckInterval)

	return m
}

// RegisterDatacenter registers a datacenter
func (m *Manager) RegisterDatacenter(dc *Datacenter) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.datacenters[dc.ID]; exists {
		return fmt.Errorf("datacenter %s already registered", dc.ID)
	}

	m.datacenters[dc.ID] = dc

	monitoring.Info("Datacenter registered",
		zap.String("dc_id", dc.ID),
		zap.String("name", dc.Name),
		zap.String("region", dc.Region),
		zap.Int("clusters", len(dc.Clusters)),
	)

	return nil
}

// GetDatacenter returns a datacenter by ID
func (m *Manager) GetDatacenter(id string) (*Datacenter, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	dc, exists := m.datacenters[id]
	if !exists {
		return nil, fmt.Errorf("datacenter %s not found", id)
	}

	return dc, nil
}

// GetHealthyDatacenters returns all healthy datacenters
func (m *Manager) GetHealthyDatacenters() []*Datacenter {
	m.mu.RLock()
	defer m.mu.RUnlock()

	healthy := make([]*Datacenter, 0)
	for _, dc := range m.datacenters {
		if dc.IsHealthy() {
			healthy = append(healthy, dc)
		}
	}

	return healthy
}

// SelectDatacentersForWrite selects datacenters for write operation based on strategy
func (m *Manager) SelectDatacentersForWrite(ctx context.Context) ([]*Datacenter, error) {
	healthy := m.GetHealthyDatacenters()
	if len(healthy) == 0 {
		return nil, fmt.Errorf("no healthy datacenters available")
	}

	switch m.strategy {
	case StrategyActiveActive:
		// Write to all healthy DCs
		return healthy, nil

	case StrategyActivePassive:
		// Write to primary DC only
		primary := m.getPrimaryDatacenter(healthy)
		if primary == nil {
			return nil, fmt.Errorf("no primary datacenter available")
		}
		return []*Datacenter{primary}, nil

	case StrategyRegionalActive:
		// Write to all DCs in the same region
		localDC := m.getLocalDatacenter(healthy)
		if localDC == nil {
			return healthy, nil
		}

		regional := make([]*Datacenter, 0)
		for _, dc := range healthy {
			if dc.Region == localDC.Region {
				regional = append(regional, dc)
			}
		}

		if len(regional) == 0 {
			return healthy, nil
		}
		return regional, nil

	case StrategyPreferred:
		// Prefer local DC, but write to all if local fails
		return healthy, nil

	default:
		return healthy, nil
	}
}

// SelectDatacenterForRead selects a datacenter for read operation
func (m *Manager) SelectDatacenterForRead(ctx context.Context, preferLocal bool) (*Datacenter, error) {
	healthy := m.GetHealthyDatacenters()
	if len(healthy) == 0 {
		return nil, fmt.Errorf("no healthy datacenters available")
	}

	if preferLocal {
		localDC := m.getLocalDatacenter(healthy)
		if localDC != nil {
			return localDC, nil
		}
	}

	// Select DC with lowest latency
	var selected *Datacenter
	var minLatency time.Duration = time.Hour

	for _, dc := range healthy {
		if dc.Latency < minLatency {
			minLatency = dc.Latency
			selected = dc
		}
	}

	if selected == nil {
		selected = healthy[0]
	}

	return selected, nil
}

// healthMonitor monitors datacenter health
func (m *Manager) healthMonitor(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		m.mu.RLock()
		for _, dc := range m.datacenters {
			go dc.CheckHealth()
		}
		m.mu.RUnlock()
	}
}

// getPrimaryDatacenter returns the highest priority datacenter
func (m *Manager) getPrimaryDatacenter(dcs []*Datacenter) *Datacenter {
	if len(dcs) == 0 {
		return nil
	}

	primary := dcs[0]
	for _, dc := range dcs[1:] {
		if dc.Priority > primary.Priority {
			primary = dc
		}
	}

	return primary
}

// getLocalDatacenter returns the local datacenter (lowest latency)
func (m *Manager) getLocalDatacenter(dcs []*Datacenter) *Datacenter {
	if len(dcs) == 0 {
		return nil
	}

	local := dcs[0]
	for _, dc := range dcs[1:] {
		if dc.Latency < local.Latency {
			local = dc
		}
	}

	return local
}

// GetAllDatacenters returns all datacenters
func (m *Manager) GetAllDatacenters() []*Datacenter {
	m.mu.RLock()
	defer m.mu.RUnlock()

	dcs := make([]*Datacenter, 0, len(m.datacenters))
	for _, dc := range m.datacenters {
		dcs = append(dcs, dc)
	}

	return dcs
}

// IsHealthy checks if the datacenter is healthy
func (dc *Datacenter) IsHealthy() bool {
	dc.mu.RLock()
	defer dc.mu.RUnlock()

	return dc.Healthy
}

// CheckHealth checks the health of the datacenter
func (dc *Datacenter) CheckHealth() {
	healthyClusters := 0
	for _, c := range dc.Clusters {
		if c.IsHealthy() {
			healthyClusters++
		}
	}

	dc.mu.Lock()
	dc.Healthy = healthyClusters > 0
	dc.mu.Unlock()

	if !dc.Healthy {
		monitoring.Warn("Datacenter unhealthy",
			zap.String("dc_id", dc.ID),
			zap.Int("healthy_clusters", healthyClusters),
			zap.Int("total_clusters", len(dc.Clusters)),
		)
	}
}

// MeasureLatency measures latency to the datacenter
func (dc *Datacenter) MeasureLatency() time.Duration {
	if len(dc.Clusters) == 0 {
		return 0
	}

	// Use the average latency of all clusters in the DC
	var totalLatency time.Duration
	count := 0

	for _, c := range dc.Clusters {
		if c.Health != nil {
			latency := c.Health.GetLatency()
			if latency > 0 {
				totalLatency += latency
				count++
			}
		}
	}

	if count == 0 {
		return 0
	}

	avgLatency := totalLatency / time.Duration(count)

	dc.mu.Lock()
	dc.Latency = avgLatency
	dc.mu.Unlock()

	return avgLatency
}

// GetStats returns datacenter statistics
func (dc *Datacenter) GetStats() map[string]interface{} {
	dc.mu.RLock()
	defer dc.mu.RUnlock()

	healthyClusters := 0
	for _, c := range dc.Clusters {
		if c.IsHealthy() {
			healthyClusters++
		}
	}

	return map[string]interface{}{
		"id":               dc.ID,
		"name":             dc.Name,
		"region":           dc.Region,
		"priority":         dc.Priority,
		"healthy":          dc.Healthy,
		"latency":          dc.Latency.String(),
		"total_clusters":   len(dc.Clusters),
		"healthy_clusters": healthyClusters,
	}
}
