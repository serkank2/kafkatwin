package cluster

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
	"go.uber.org/zap"

	"github.com/serkank2/kafkatwin/internal/config"
	"github.com/serkank2/kafkatwin/internal/monitoring"
)

// ConnectionPool manages a pool of Kafka connections
type ConnectionPool struct {
	clusterConfig     config.ClusterConfig
	poolConfig        config.ConnectionPoolConfig
	producers         chan sarama.AsyncProducer
	syncProducers     chan sarama.SyncProducer
	consumers         chan sarama.Consumer
	mu                sync.RWMutex
	closed            bool
	maintenanceCancel context.CancelFunc

	// Statistics
	totalCreated      int64
	totalReturned     int64
	totalErrors       int64
}

// ConnectionMetrics tracks connection pool metrics
type ConnectionMetrics struct {
	TotalCreated  int64
	TotalReturned int64
	TotalErrors   int64
}

// NewConnectionPool creates a new connection pool
func NewConnectionPool(clusterCfg config.ClusterConfig, poolCfg config.ConnectionPoolConfig) *ConnectionPool {
	pool := &ConnectionPool{
		clusterConfig: clusterCfg,
		poolConfig:    poolCfg,
		producers:     make(chan sarama.AsyncProducer, poolCfg.MaxConnections),
		syncProducers: make(chan sarama.SyncProducer, poolCfg.MaxConnections),
		consumers:     make(chan sarama.Consumer, poolCfg.MaxConnections),
	}

	// Pre-create minimum connections
	for i := 0; i < poolCfg.MinConnections; i++ {
		if producer, err := pool.createSyncProducer(); err == nil {
			pool.syncProducers <- producer
		}
	}

	return pool
}

// GetProducer gets a producer from the pool or creates a new one
func (p *ConnectionPool) GetProducer() (sarama.AsyncProducer, error) {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return nil, fmt.Errorf("connection pool is closed")
	}
	p.mu.RUnlock()

	select {
	case producer := <-p.producers:
		monitoring.Debug("Reusing async producer from pool",
			zap.String("cluster", p.clusterConfig.ID),
		)
		return producer, nil
	default:
		monitoring.Debug("Creating new async producer",
			zap.String("cluster", p.clusterConfig.ID),
		)
		producer, err := p.createProducer()
		if err != nil {
			atomic.AddInt64(&p.totalErrors, 1)
			return nil, err
		}
		atomic.AddInt64(&p.totalCreated, 1)
		return producer, nil
	}
}

// ReturnProducer returns a producer to the pool
func (p *ConnectionPool) ReturnProducer(producer sarama.AsyncProducer) {
	if producer == nil {
		return
	}

	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		producer.Close()
		return
	}

	select {
	case p.producers <- producer:
		atomic.AddInt64(&p.totalReturned, 1)
	default:
		// Pool is full, close the producer
		monitoring.Debug("Pool full, closing excess async producer",
			zap.String("cluster", p.clusterConfig.ID),
		)
		producer.Close()
	}
}

// GetConsumer gets a consumer from the pool or creates a new one
func (p *ConnectionPool) GetConsumer() (sarama.Consumer, error) {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return nil, fmt.Errorf("connection pool is closed")
	}
	p.mu.RUnlock()

	select {
	case consumer := <-p.consumers:
		monitoring.Debug("Reusing consumer from pool",
			zap.String("cluster", p.clusterConfig.ID),
		)
		return consumer, nil
	default:
		monitoring.Debug("Creating new consumer",
			zap.String("cluster", p.clusterConfig.ID),
		)
		consumer, err := p.createConsumer()
		if err != nil {
			atomic.AddInt64(&p.totalErrors, 1)
			return nil, err
		}
		atomic.AddInt64(&p.totalCreated, 1)
		return consumer, nil
	}
}

// ReturnConsumer returns a consumer to the pool
func (p *ConnectionPool) ReturnConsumer(consumer sarama.Consumer) {
	if consumer == nil {
		return
	}

	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		consumer.Close()
		return
	}

	select {
	case p.consumers <- consumer:
		atomic.AddInt64(&p.totalReturned, 1)
	default:
		// Pool is full, close the consumer
		monitoring.Debug("Pool full, closing excess consumer",
			zap.String("cluster", p.clusterConfig.ID),
		)
		consumer.Close()
	}
}

// GetSyncProducer gets a sync producer from the pool or creates a new one
func (p *ConnectionPool) GetSyncProducer() (sarama.SyncProducer, error) {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return nil, fmt.Errorf("connection pool is closed")
	}
	p.mu.RUnlock()

	select {
	case producer := <-p.syncProducers:
		monitoring.Debug("Reusing sync producer from pool",
			zap.String("cluster", p.clusterConfig.ID),
		)
		return producer, nil
	default:
		monitoring.Debug("Creating new sync producer",
			zap.String("cluster", p.clusterConfig.ID),
		)
		producer, err := p.createSyncProducer()
		if err != nil {
			atomic.AddInt64(&p.totalErrors, 1)
			return nil, err
		}
		atomic.AddInt64(&p.totalCreated, 1)
		return producer, nil
	}
}

// ReturnSyncProducer returns a sync producer to the pool
func (p *ConnectionPool) ReturnSyncProducer(producer sarama.SyncProducer) {
	if producer == nil {
		return
	}

	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		producer.Close()
		return
	}

	select {
	case p.syncProducers <- producer:
		atomic.AddInt64(&p.totalReturned, 1)
	default:
		// Pool is full, close the producer
		monitoring.Debug("Pool full, closing excess sync producer",
			zap.String("cluster", p.clusterConfig.ID),
		)
		producer.Close()
	}
}

// createProducer creates a new async producer
func (p *ConnectionPool) createProducer() (sarama.AsyncProducer, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 3
	config.Producer.Idempotent = true
	config.Producer.MaxMessageBytes = 1000000

	if p.clusterConfig.Timeout.Connection > 0 {
		config.Net.DialTimeout = p.clusterConfig.Timeout.Connection
		config.Net.ReadTimeout = p.clusterConfig.Timeout.Request
		config.Net.WriteTimeout = p.clusterConfig.Timeout.Request
	}

	// Configure security
	if err := configureSecurity(config, p.clusterConfig.Security); err != nil {
		return nil, err
	}

	producer, err := sarama.NewAsyncProducer(p.clusterConfig.BootstrapServers, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create async producer: %w", err)
	}

	return producer, nil
}

// createSyncProducer creates a new sync producer
func (p *ConnectionPool) createSyncProducer() (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 3
	config.Producer.Idempotent = true
	config.Producer.MaxMessageBytes = 1000000

	if p.clusterConfig.Timeout.Connection > 0 {
		config.Net.DialTimeout = p.clusterConfig.Timeout.Connection
		config.Net.ReadTimeout = p.clusterConfig.Timeout.Request
		config.Net.WriteTimeout = p.clusterConfig.Timeout.Request
	}

	// Configure security
	if err := configureSecurity(config, p.clusterConfig.Security); err != nil {
		return nil, err
	}

	producer, err := sarama.NewSyncProducer(p.clusterConfig.BootstrapServers, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create sync producer: %w", err)
	}

	return producer, nil
}

// createConsumer creates a new consumer
func (p *ConnectionPool) createConsumer() (sarama.Consumer, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Consumer.Return.Errors = true

	if p.clusterConfig.Timeout.Connection > 0 {
		config.Net.DialTimeout = p.clusterConfig.Timeout.Connection
		config.Net.ReadTimeout = p.clusterConfig.Timeout.Request
		config.Net.WriteTimeout = p.clusterConfig.Timeout.Request
	}

	// Configure security
	if err := configureSecurity(config, p.clusterConfig.Security); err != nil {
		return nil, err
	}

	consumer, err := sarama.NewConsumer(p.clusterConfig.BootstrapServers, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	return consumer, nil
}

// Close closes the connection pool
func (p *ConnectionPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return
	}

	p.closed = true

	// Close all async producers
	close(p.producers)
	for producer := range p.producers {
		producer.Close()
	}

	// Close all sync producers
	close(p.syncProducers)
	for producer := range p.syncProducers {
		producer.Close()
	}

	// Close all consumers
	close(p.consumers)
	for consumer := range p.consumers {
		consumer.Close()
	}
}

// PoolStats returns pool statistics
type PoolStats struct {
	ProducersAvailable     int
	SyncProducersAvailable int
	ConsumersAvailable     int
	TotalCreated           int64
	TotalReturned          int64
	TotalErrors            int64
	PoolUtilization        float64
}

// GetStats returns current pool statistics
func (p *ConnectionPool) GetStats() PoolStats {
	p.mu.RLock()
	defer p.mu.RUnlock()

	asyncProducers := len(p.producers)
	syncProducers := len(p.syncProducers)
	consumers := len(p.consumers)

	totalAvailable := asyncProducers + syncProducers + consumers
	maxConnections := p.poolConfig.MaxConnections * 3 // 3 types of connections

	utilization := 0.0
	if maxConnections > 0 {
		utilization = float64(totalAvailable) / float64(maxConnections) * 100
	}

	return PoolStats{
		ProducersAvailable:     asyncProducers,
		SyncProducersAvailable: syncProducers,
		ConsumersAvailable:     consumers,
		TotalCreated:           atomic.LoadInt64(&p.totalCreated),
		TotalReturned:          atomic.LoadInt64(&p.totalReturned),
		TotalErrors:            atomic.LoadInt64(&p.totalErrors),
		PoolUtilization:        utilization,
	}
}

// Warmup pre-creates connections up to the min connections threshold
func (p *ConnectionPool) Warmup() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	monitoring.Info("Warming up connection pool",
		zap.String("cluster", p.clusterConfig.ID),
		zap.Int("min_connections", p.poolConfig.MinConnections),
	)

	var errors []error

	// Warmup async producers
	for len(p.producers) < p.poolConfig.MinConnections {
		producer, err := p.createProducer()
		if err != nil {
			errors = append(errors, fmt.Errorf("async producer: %w", err))
			break
		}
		p.producers <- producer
		atomic.AddInt64(&p.totalCreated, 1)
	}

	// Warmup sync producers
	for len(p.syncProducers) < p.poolConfig.MinConnections {
		producer, err := p.createSyncProducer()
		if err != nil {
			errors = append(errors, fmt.Errorf("sync producer: %w", err))
			break
		}
		p.syncProducers <- producer
		atomic.AddInt64(&p.totalCreated, 1)
	}

	// Warmup consumers
	for len(p.consumers) < p.poolConfig.MinConnections {
		consumer, err := p.createConsumer()
		if err != nil {
			errors = append(errors, fmt.Errorf("consumer: %w", err))
			break
		}
		p.consumers <- consumer
		atomic.AddInt64(&p.totalCreated, 1)
	}

	if len(errors) > 0 {
		monitoring.Warn("Connection pool warmup completed with errors",
			zap.String("cluster", p.clusterConfig.ID),
			zap.Int("error_count", len(errors)),
		)
		return fmt.Errorf("warmup errors: %v", errors)
	}

	monitoring.Info("Connection pool warmup completed successfully",
		zap.String("cluster", p.clusterConfig.ID),
		zap.Int("async_producers", len(p.producers)),
		zap.Int("sync_producers", len(p.syncProducers)),
		zap.Int("consumers", len(p.consumers)),
	)

	return nil
}

// Cleanup removes idle connections exceeding the min connections threshold
func (p *ConnectionPool) Cleanup() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return
	}

	initialTotal := len(p.producers) + len(p.syncProducers) + len(p.consumers)

	// Cleanup excess async producers
	for len(p.producers) > p.poolConfig.MinConnections {
		select {
		case producer := <-p.producers:
			producer.Close()
		default:
			break
		}
	}

	// Cleanup excess sync producers
	for len(p.syncProducers) > p.poolConfig.MinConnections {
		select {
		case producer := <-p.syncProducers:
			producer.Close()
		default:
			break
		}
	}

	// Cleanup excess consumers
	for len(p.consumers) > p.poolConfig.MinConnections {
		select {
		case consumer := <-p.consumers:
			consumer.Close()
		default:
			break
		}
	}

	finalTotal := len(p.producers) + len(p.syncProducers) + len(p.consumers)
	if initialTotal != finalTotal {
		monitoring.Debug("Connection pool cleanup completed",
			zap.String("cluster", p.clusterConfig.ID),
			zap.Int("connections_before", initialTotal),
			zap.Int("connections_after", finalTotal),
			zap.Int("cleaned", initialTotal-finalTotal),
		)
	}
}

// StartMaintenanceLoop starts a background goroutine for pool maintenance
func (p *ConnectionPool) StartMaintenanceLoop(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)

	monitoring.Info("Starting connection pool maintenance loop",
		zap.String("cluster", p.clusterConfig.ID),
		zap.Duration("interval", interval),
	)

	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				monitoring.Info("Stopping connection pool maintenance loop",
					zap.String("cluster", p.clusterConfig.ID),
				)
				return
			case <-ticker.C:
				p.Cleanup()

				// Log stats periodically
				stats := p.GetStats()
				monitoring.Debug("Connection pool stats",
					zap.String("cluster", p.clusterConfig.ID),
					zap.Int("async_producers", stats.ProducersAvailable),
					zap.Int("sync_producers", stats.SyncProducersAvailable),
					zap.Int("consumers", stats.ConsumersAvailable),
					zap.Int64("total_created", stats.TotalCreated),
					zap.Int64("total_returned", stats.TotalReturned),
					zap.Int64("total_errors", stats.TotalErrors),
					zap.Float64("utilization_pct", stats.PoolUtilization),
				)
			}
		}
	}()
}
