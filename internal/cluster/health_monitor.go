package cluster

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
	"go.uber.org/zap"

	"github.com/serkank2/kafkatwin/internal/monitoring"
)

// HealthMonitor monitors the health of a Kafka cluster
type HealthMonitor struct {
	clusterID     string
	client        sarama.Client
	healthy       atomic.Bool
	lastCheckTime atomic.Value // time.Time
	latency       atomic.Value // time.Duration
	errorCount    atomic.Int64
	successCount  atomic.Int64
	cancel        context.CancelFunc
}

// NewHealthMonitor creates a new health monitor
func NewHealthMonitor(clusterID string, client sarama.Client) *HealthMonitor {
	hm := &HealthMonitor{
		clusterID: clusterID,
		client:    client,
	}
	hm.healthy.Store(true)
	hm.lastCheckTime.Store(time.Now())
	hm.latency.Store(time.Duration(0))

	return hm
}

// Start starts the health monitoring loop
func (hm *HealthMonitor) Start(ctx context.Context, interval time.Duration) {
	ctx, cancel := context.WithCancel(ctx)
	hm.cancel = cancel

	go hm.monitorLoop(ctx, interval)
}

// Stop stops the health monitoring
func (hm *HealthMonitor) Stop() {
	if hm.cancel != nil {
		hm.cancel()
	}
}

// monitorLoop runs periodic health checks
func (hm *HealthMonitor) monitorLoop(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			hm.performHealthCheck()
		}
	}
}

// performHealthCheck performs a comprehensive health check on the cluster
func (hm *HealthMonitor) performHealthCheck() {
	start := time.Now()
	defer func() {
		hm.lastCheckTime.Store(time.Now())
	}()

	// Perform multiple health checks
	healthy := true
	var checkErrors []error

	// 1. Check if client is closed
	if hm.client.Closed() {
		checkErrors = append(checkErrors, fmt.Errorf("client is closed"))
		healthy = false
	} else {
		// 2. Check broker connectivity
		brokers := hm.client.Brokers()
		if len(brokers) == 0 {
			checkErrors = append(checkErrors, fmt.Errorf("no brokers available"))
			healthy = false
		} else {
			// Verify at least one broker is connected
			connectedCount := 0
			for _, broker := range brokers {
				if broker.Connected() {
					connectedCount++
				}
			}

			if connectedCount == 0 {
				checkErrors = append(checkErrors, fmt.Errorf("no brokers connected"))
				healthy = false
			}

			// Update active connections metric
			monitoring.ClusterConnectionsActive.WithLabelValues(hm.clusterID).Set(float64(connectedCount))
		}

		// 3. Try to refresh metadata
		if err := hm.client.RefreshMetadata(); err != nil {
			checkErrors = append(checkErrors, fmt.Errorf("metadata refresh failed: %w", err))
			healthy = false
		}

		// 4. Verify controller is available
		if controller, err := hm.client.Controller(); err != nil {
			checkErrors = append(checkErrors, fmt.Errorf("controller check failed: %w", err))
			healthy = false
		} else if controller == nil {
			checkErrors = append(checkErrors, fmt.Errorf("no controller available"))
			healthy = false
		} else if !controller.Connected() {
			checkErrors = append(checkErrors, fmt.Errorf("controller not connected"))
			healthy = false
		}
	}

	latency := time.Since(start)
	hm.latency.Store(latency)

	if !healthy {
		hm.errorCount.Add(1)
		hm.healthy.Store(false)

		monitoring.UpdateClusterHealth(hm.clusterID, false)
		monitoring.RecordError("health_check", hm.clusterID, "health_check_failed")

		monitoring.Warn("Cluster health check failed",
			zap.String("cluster", hm.clusterID),
			zap.Errors("errors", checkErrors),
			zap.Duration("latency", latency),
			zap.Int("error_count", len(checkErrors)),
		)
	} else {
		hm.successCount.Add(1)
		hm.healthy.Store(true)

		monitoring.UpdateClusterHealth(hm.clusterID, true)

		monitoring.Debug("Cluster health check succeeded",
			zap.String("cluster", hm.clusterID),
			zap.Duration("latency", latency),
		)
	}
}

// ForceHealthCheck performs an immediate health check (useful for testing or manual triggers)
func (hm *HealthMonitor) ForceHealthCheck() {
	hm.performHealthCheck()
}

// IsHealthy returns the current health status
func (hm *HealthMonitor) IsHealthy() bool {
	return hm.healthy.Load()
}

// GetLastCheckTime returns the last health check time
func (hm *HealthMonitor) GetLastCheckTime() time.Time {
	if t, ok := hm.lastCheckTime.Load().(time.Time); ok {
		return t
	}
	return time.Time{}
}

// GetLatency returns the last health check latency
func (hm *HealthMonitor) GetLatency() time.Duration {
	if d, ok := hm.latency.Load().(time.Duration); ok {
		return d
	}
	return 0
}

// GetErrorCount returns the total error count
func (hm *HealthMonitor) GetErrorCount() int64 {
	return hm.errorCount.Load()
}

// GetSuccessCount returns the total success count
func (hm *HealthMonitor) GetSuccessCount() int64 {
	return hm.successCount.Load()
}

// GetStats returns health statistics
type HealthStats struct {
	Healthy       bool
	LastCheckTime time.Time
	Latency       time.Duration
	ErrorCount    int64
	SuccessCount  int64
	ErrorRate     float64
}

// GetStats returns the health statistics
func (hm *HealthMonitor) GetStats() HealthStats {
	errorCount := hm.GetErrorCount()
	successCount := hm.GetSuccessCount()
	total := errorCount + successCount

	var errorRate float64
	if total > 0 {
		errorRate = float64(errorCount) / float64(total)
	}

	return HealthStats{
		Healthy:       hm.IsHealthy(),
		LastCheckTime: hm.GetLastCheckTime(),
		Latency:       hm.GetLatency(),
		ErrorCount:    errorCount,
		SuccessCount:  successCount,
		ErrorRate:     errorRate,
	}
}
