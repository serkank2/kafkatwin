package cluster

import (
	"context"
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

// performHealthCheck performs a health check on the cluster
func (hm *HealthMonitor) performHealthCheck() {
	start := time.Now()
	defer func() {
		hm.lastCheckTime.Store(time.Now())
	}()

	// Try to refresh metadata as a health check
	err := hm.client.RefreshMetadata()

	latency := time.Since(start)
	hm.latency.Store(latency)

	if err != nil {
		hm.errorCount.Add(1)
		hm.healthy.Store(false)

		monitoring.UpdateClusterHealth(hm.clusterID, false)
		monitoring.RecordError("health_check", hm.clusterID, "metadata_refresh_failed")

		monitoring.Warn("Cluster health check failed",
			zap.String("cluster", hm.clusterID),
			zap.Error(err),
			zap.Duration("latency", latency),
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

	// Update metrics
	if hm.client != nil && len(hm.client.Brokers()) > 0 {
		monitoring.ClusterConnectionsActive.WithLabelValues(hm.clusterID).Set(float64(len(hm.client.Brokers())))
	}
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
