package cluster

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"go.uber.org/zap"

	"github.com/serkank2/kafkatwin/internal/config"
	"github.com/serkank2/kafkatwin/internal/monitoring"
	"github.com/serkank2/kafkatwin/internal/retry"
)

// Manager manages connections to multiple Kafka clusters
type Manager struct {
	clusters map[string]*Cluster
	config   *config.Config
	mu       sync.RWMutex
	ctx      context.Context
	cancel   context.CancelFunc
}

// Cluster represents a Kafka cluster
type Cluster struct {
	ID               string
	Config           config.ClusterConfig
	Client           sarama.Client
	Admin            sarama.ClusterAdmin
	ConnectionPool   *ConnectionPool
	CircuitBreaker   *retry.CircuitBreaker
	Health           *HealthMonitor
	Weight           int       // Weight for load balancing (higher = more traffic)
	Priority         int       // Priority for failover (higher = preferred)
	CreatedAt        time.Time
	LastReconnectAt  time.Time
	mu               sync.RWMutex
}

// NewManager creates a new cluster manager
func NewManager(cfg *config.Config) (*Manager, error) {
	ctx, cancel := context.WithCancel(context.Background())

	m := &Manager{
		clusters: make(map[string]*Cluster),
		config:   cfg,
		ctx:      ctx,
		cancel:   cancel,
	}

	// Initialize clusters
	for _, clusterCfg := range cfg.Clusters {
		if err := m.AddCluster(clusterCfg); err != nil {
			monitoring.Error("Failed to initialize cluster",
				zap.String("cluster", clusterCfg.ID),
				zap.Error(err),
			)
			// Continue with other clusters
			continue
		}
	}

	if len(m.clusters) == 0 {
		cancel()
		return nil, fmt.Errorf("no healthy clusters available")
	}

	return m, nil
}

// AddCluster adds a new cluster to the manager
func (m *Manager) AddCluster(clusterCfg config.ClusterConfig) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.clusters[clusterCfg.ID]; exists {
		return fmt.Errorf("cluster %s already exists", clusterCfg.ID)
	}

	cluster, err := m.createCluster(clusterCfg)
	if err != nil {
		return fmt.Errorf("failed to create cluster %s: %w", clusterCfg.ID, err)
	}

	m.clusters[clusterCfg.ID] = cluster

	monitoring.Info("Cluster added",
		zap.String("cluster", clusterCfg.ID),
		zap.Strings("brokers", clusterCfg.BootstrapServers),
	)

	return nil
}

// createCluster creates a new cluster instance
func (m *Manager) createCluster(clusterCfg config.ClusterConfig) (*Cluster, error) {
	// Create Sarama configuration
	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.V2_8_0_0
	saramaConfig.ClientID = "kafkatwin-proxy"

	// Set timeouts
	if clusterCfg.Timeout.Connection > 0 {
		saramaConfig.Net.DialTimeout = clusterCfg.Timeout.Connection
		saramaConfig.Net.ReadTimeout = clusterCfg.Timeout.Request
		saramaConfig.Net.WriteTimeout = clusterCfg.Timeout.Request
	}

	// Configure security
	if err := configureSecurity(saramaConfig, clusterCfg.Security); err != nil {
		return nil, fmt.Errorf("failed to configure security: %w", err)
	}

	// Create Kafka client
	client, err := sarama.NewClient(clusterCfg.BootstrapServers, saramaConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka client: %w", err)
	}

	// Create cluster admin
	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to create cluster admin: %w", err)
	}

	// Create circuit breaker with cluster-specific name
	circuitBreaker := retry.NewCircuitBreaker(retry.CircuitBreakerConfig{
		Name:              fmt.Sprintf("cluster-%s", clusterCfg.ID),
		MaxFailures:       5,
		ResetTimeout:      60 * time.Second,
		HalfOpenSuccesses: 2,
		WindowDuration:    60 * time.Second,
	})

	// Create connection pool
	connPool := NewConnectionPool(clusterCfg, m.config.Performance.ConnectionPool)

	// Warmup connection pool
	if err := connPool.Warmup(); err != nil {
		monitoring.Warn("Failed to warmup connection pool",
			zap.String("cluster", clusterCfg.ID),
			zap.Error(err),
		)
	}

	cluster := &Cluster{
		ID:             clusterCfg.ID,
		Config:         clusterCfg,
		Client:         client,
		Admin:          admin,
		ConnectionPool: connPool,
		CircuitBreaker: circuitBreaker,
		Health:         NewHealthMonitor(clusterCfg.ID, client),
		Weight:         clusterCfg.Weight,
		Priority:       clusterCfg.Priority,
		CreatedAt:      time.Now(),
	}

	// Start health monitoring
	cluster.Health.Start(m.ctx, m.config.Monitoring.Health.Interval)

	monitoring.Info("Cluster created and initialized",
		zap.String("cluster", clusterCfg.ID),
		zap.Int("weight", cluster.Weight),
		zap.Int("priority", cluster.Priority),
		zap.Strings("brokers", clusterCfg.BootstrapServers),
	)

	return cluster, nil
}

// GetCluster returns a cluster by ID
func (m *Manager) GetCluster(id string) (*Cluster, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	cluster, exists := m.clusters[id]
	if !exists {
		return nil, fmt.Errorf("cluster %s not found", id)
	}

	return cluster, nil
}

// GetHealthyClusters returns all healthy clusters sorted by priority (highest first)
func (m *Manager) GetHealthyClusters() []*Cluster {
	m.mu.RLock()
	defer m.mu.RUnlock()

	healthy := make([]*Cluster, 0)
	for _, cluster := range m.clusters {
		if cluster.IsHealthy() {
			healthy = append(healthy, cluster)
		}
	}

	// Sort by priority (descending) and then by weight (descending)
	// This ensures higher priority clusters are preferred
	for i := 0; i < len(healthy); i++ {
		for j := i + 1; j < len(healthy); j++ {
			if healthy[i].Priority < healthy[j].Priority ||
				(healthy[i].Priority == healthy[j].Priority && healthy[i].Weight < healthy[j].Weight) {
				healthy[i], healthy[j] = healthy[j], healthy[i]
			}
		}
	}

	return healthy
}

// GetHealthyClustersByWeight returns healthy clusters sorted by weight for load balancing
func (m *Manager) GetHealthyClustersByWeight() []*Cluster {
	m.mu.RLock()
	defer m.mu.RUnlock()

	healthy := make([]*Cluster, 0)
	for _, cluster := range m.clusters {
		if cluster.IsHealthy() {
			healthy = append(healthy, cluster)
		}
	}

	// Sort by weight (descending)
	for i := 0; i < len(healthy); i++ {
		for j := i + 1; j < len(healthy); j++ {
			if healthy[i].Weight < healthy[j].Weight {
				healthy[i], healthy[j] = healthy[j], healthy[i]
			}
		}
	}

	return healthy
}

// GetAllClusters returns all clusters
func (m *Manager) GetAllClusters() []*Cluster {
	m.mu.RLock()
	defer m.mu.RUnlock()

	clusters := make([]*Cluster, 0, len(m.clusters))
	for _, cluster := range m.clusters {
		clusters = append(clusters, cluster)
	}

	return clusters
}

// RemoveCluster removes a cluster from the manager
func (m *Manager) RemoveCluster(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	cluster, exists := m.clusters[id]
	if !exists {
		return fmt.Errorf("cluster %s not found", id)
	}

	// Stop health monitoring
	cluster.Health.Stop()

	// Close connection pool
	if cluster.ConnectionPool != nil {
		cluster.ConnectionPool.Close()
	}

	// Close admin
	if cluster.Admin != nil {
		if err := cluster.Admin.Close(); err != nil {
			monitoring.Warn("Error closing cluster admin",
				zap.String("cluster", id),
				zap.Error(err),
			)
		}
	}

	// Close client
	if cluster.Client != nil {
		if err := cluster.Client.Close(); err != nil {
			monitoring.Warn("Error closing cluster client",
				zap.String("cluster", id),
				zap.Error(err),
			)
		}
	}

	delete(m.clusters, id)

	monitoring.Info("Cluster removed", zap.String("cluster", id))

	return nil
}

// ReconnectCluster attempts to reconnect to a cluster
func (m *Manager) ReconnectCluster(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	cluster, exists := m.clusters[id]
	if !exists {
		return fmt.Errorf("cluster %s not found", id)
	}

	monitoring.Info("Attempting to reconnect cluster",
		zap.String("cluster", id),
	)

	// Stop existing health monitor
	cluster.Health.Stop()

	// Close existing connections
	if cluster.ConnectionPool != nil {
		cluster.ConnectionPool.Close()
	}
	if cluster.Admin != nil {
		cluster.Admin.Close()
	}
	if cluster.Client != nil {
		cluster.Client.Close()
	}

	// Recreate cluster
	newCluster, err := m.createCluster(cluster.Config)
	if err != nil {
		monitoring.Error("Failed to reconnect cluster",
			zap.String("cluster", id),
			zap.Error(err),
		)
		return fmt.Errorf("failed to reconnect cluster: %w", err)
	}

	// Preserve weight and priority
	newCluster.Weight = cluster.Weight
	newCluster.Priority = cluster.Priority
	newCluster.LastReconnectAt = time.Now()

	// Replace old cluster
	m.clusters[id] = newCluster

	monitoring.Info("Cluster reconnected successfully",
		zap.String("cluster", id),
	)

	return nil
}

// GetClusterStats returns statistics for a specific cluster
func (m *Manager) GetClusterStats(id string) (map[string]interface{}, error) {
	cluster, err := m.GetCluster(id)
	if err != nil {
		return nil, err
	}

	stats := make(map[string]interface{})
	stats["id"] = cluster.ID
	stats["healthy"] = cluster.IsHealthy()
	stats["weight"] = cluster.Weight
	stats["priority"] = cluster.Priority
	stats["created_at"] = cluster.CreatedAt
	stats["last_reconnect_at"] = cluster.LastReconnectAt

	// Health monitor stats
	if cluster.Health != nil {
		healthStats := cluster.Health.GetStats()
		stats["health"] = map[string]interface{}{
			"healthy":           healthStats.Healthy,
			"last_check_time":   healthStats.LastCheckTime,
			"latency":           healthStats.Latency,
			"error_count":       healthStats.ErrorCount,
			"success_count":     healthStats.SuccessCount,
			"error_rate":        healthStats.ErrorRate,
		}
	}

	// Circuit breaker stats
	if cluster.CircuitBreaker != nil {
		cbStats := cluster.CircuitBreaker.GetStats()
		stats["circuit_breaker"] = map[string]interface{}{
			"state":                 cbStats.State.String(),
			"failure_count":         cbStats.FailureCount,
			"success_count":         cbStats.SuccessCount,
			"consecutive_failures":  cbStats.ConsecutiveFailures,
			"consecutive_successes": cbStats.ConsecutiveSuccesses,
			"total_requests":        cbStats.TotalRequests,
			"total_failures":        cbStats.TotalFailures,
			"total_successes":       cbStats.TotalSuccesses,
			"failure_rate":          cbStats.FailureRate,
			"window_size":           cbStats.WindowSize,
		}
	}

	// Connection pool stats
	if cluster.ConnectionPool != nil {
		poolStats := cluster.ConnectionPool.GetStats()
		stats["connection_pool"] = map[string]interface{}{
			"producers_available":      poolStats.ProducersAvailable,
			"sync_producers_available": poolStats.SyncProducersAvailable,
			"consumers_available":      poolStats.ConsumersAvailable,
			"total_created":            poolStats.TotalCreated,
			"total_returned":           poolStats.TotalReturned,
			"total_errors":             poolStats.TotalErrors,
			"pool_utilization":         poolStats.PoolUtilization,
		}
	}

	return stats, nil
}

// GetAllStats returns statistics for all clusters
func (m *Manager) GetAllStats() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	allStats := make(map[string]interface{})
	clusterStats := make(map[string]interface{})

	healthyCount := 0
	for id, cluster := range m.clusters {
		stats, _ := m.GetClusterStats(id)
		clusterStats[id] = stats

		if cluster.IsHealthy() {
			healthyCount++
		}
	}

	allStats["clusters"] = clusterStats
	allStats["total_clusters"] = len(m.clusters)
	allStats["healthy_clusters"] = healthyCount
	allStats["unhealthy_clusters"] = len(m.clusters) - healthyCount

	return allStats
}

// Close closes all cluster connections
func (m *Manager) Close() error {
	m.cancel()

	m.mu.Lock()
	defer m.mu.Unlock()

	monitoring.Info("Closing cluster manager", zap.Int("cluster_count", len(m.clusters)))

	for id, cluster := range m.clusters {
		// Stop health monitoring
		if cluster.Health != nil {
			cluster.Health.Stop()
		}

		// Close connection pool
		if cluster.ConnectionPool != nil {
			cluster.ConnectionPool.Close()
		}

		// Close admin
		if cluster.Admin != nil {
			if err := cluster.Admin.Close(); err != nil {
				monitoring.Warn("Error closing cluster admin during shutdown",
					zap.String("cluster", id),
					zap.Error(err),
				)
			}
		}

		// Close client
		if cluster.Client != nil {
			if err := cluster.Client.Close(); err != nil {
				monitoring.Warn("Error closing cluster client during shutdown",
					zap.String("cluster", id),
					zap.Error(err),
				)
			}
		}

		monitoring.Info("Cluster connection closed", zap.String("cluster", id))
	}

	monitoring.Info("Cluster manager closed successfully")

	return nil
}

// IsHealthy checks if the cluster is healthy
func (c *Cluster) IsHealthy() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Check circuit breaker state - if open, cluster is unhealthy
	if c.CircuitBreaker != nil && c.CircuitBreaker.GetState() == retry.StateOpen {
		return false
	}

	// Check health monitor
	if c.Health != nil {
		return c.Health.IsHealthy()
	}

	// Check if client is connected
	if c.Client != nil && c.Client.Closed() {
		return false
	}

	return true
}

// GetHealthScore returns a health score (0-100) for the cluster
// Higher score means healthier cluster
func (c *Cluster) GetHealthScore() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	score := 100

	// Circuit breaker penalty
	if c.CircuitBreaker != nil {
		cbStats := c.CircuitBreaker.GetStats()
		switch cbStats.State {
		case retry.StateOpen:
			score -= 100 // Cluster is down
		case retry.StateHalfOpen:
			score -= 30 // Cluster is recovering
		case retry.StateClosed:
			// Apply penalty based on failure rate
			if cbStats.FailureRate > 0 {
				score -= int(cbStats.FailureRate * 40) // Up to 40 points penalty
			}
		}

		// Consecutive failures penalty
		if cbStats.ConsecutiveFailures > 0 {
			penalty := cbStats.ConsecutiveFailures * 5
			if penalty > 20 {
				penalty = 20
			}
			score -= penalty
		}
	}

	// Health monitor penalty
	if c.Health != nil {
		healthStats := c.Health.GetStats()
		if !healthStats.Healthy {
			score -= 50
		}

		// Error rate penalty
		if healthStats.ErrorRate > 0 {
			score -= int(healthStats.ErrorRate * 30) // Up to 30 points penalty
		}

		// Latency penalty (if latency > 1s, apply penalty)
		if healthStats.Latency > time.Second {
			latencySeconds := int(healthStats.Latency.Seconds())
			penalty := latencySeconds * 2
			if penalty > 20 {
				penalty = 20
			}
			score -= penalty
		}
	}

	// Ensure score is in valid range
	if score < 0 {
		score = 0
	}
	if score > 100 {
		score = 100
	}

	return score
}

// HealthCheck implements HealthChecker interface
func (c *Cluster) HealthCheck() monitoring.HealthCheck {
	status := monitoring.StatusHealthy
	message := "Cluster is healthy"

	if !c.IsHealthy() {
		status = monitoring.StatusUnhealthy
		message = fmt.Sprintf("Cluster is unhealthy, circuit breaker state: %s", c.CircuitBreaker.GetState())
	}

	return monitoring.HealthCheck{
		Name:      fmt.Sprintf("cluster_%s", c.ID),
		Status:    status,
		Message:   message,
		Timestamp: time.Now(),
	}
}

// configureSecurity configures security settings for Sarama
func configureSecurity(cfg *sarama.Config, security config.ClusterSecurityConfig) error {
	// Configure SASL
	if security.SASL.Enabled {
		cfg.Net.SASL.Enable = true
		cfg.Net.SASL.User = security.SASL.Username
		cfg.Net.SASL.Password = security.SASL.Password

		switch security.SASL.Mechanism {
		case "PLAIN":
			cfg.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		case "SCRAM-SHA-256":
			cfg.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
		case "SCRAM-SHA-512":
			cfg.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
		default:
			return fmt.Errorf("unsupported SASL mechanism: %s", security.SASL.Mechanism)
		}
	}

	// Configure TLS
	if security.TLS.Enabled {
		cfg.Net.TLS.Enable = true
		// TLS config would be set here if cert files are provided
	}

	return nil
}
