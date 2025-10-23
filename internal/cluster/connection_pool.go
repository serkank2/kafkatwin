package cluster

import (
	"fmt"
	"sync"
	"time"

	"github.com/IBM/sarama"

	"github.com/serkank2/kafkatwin/internal/config"
)

// ConnectionPool manages a pool of Kafka connections
type ConnectionPool struct {
	clusterConfig  config.ClusterConfig
	poolConfig     config.ConnectionPoolConfig
	producers      chan sarama.AsyncProducer
	syncProducers  chan sarama.SyncProducer
	consumers      chan sarama.Consumer
	mu             sync.RWMutex
	closed         bool
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
		return producer, nil
	default:
		return p.createProducer()
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
	default:
		// Pool is full, close the producer
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
		return consumer, nil
	default:
		return p.createConsumer()
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
	default:
		// Pool is full, close the consumer
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
		return producer, nil
	default:
		return p.createSyncProducer()
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
	default:
		// Pool is full, close the producer
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

// Stats returns pool statistics
type PoolStats struct {
	ProducersAvailable int
	ConsumersAvailable int
}

// GetStats returns current pool statistics
func (p *ConnectionPool) GetStats() PoolStats {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return PoolStats{
		ProducersAvailable: len(p.producers),
		ConsumersAvailable: len(p.consumers),
	}
}

// Warmup pre-creates connections up to the min connections threshold
func (p *ConnectionPool) Warmup() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for len(p.producers) < p.poolConfig.MinConnections {
		producer, err := p.createProducer()
		if err != nil {
			return fmt.Errorf("failed to warmup producer pool: %w", err)
		}
		p.producers <- producer
	}

	return nil
}

// Cleanup removes idle connections
func (p *ConnectionPool) Cleanup() {
	// This would implement idle connection cleanup based on idle timeout
	// For now, it's a placeholder for future implementation
}

// StartMaintenanceLoop starts a background goroutine for pool maintenance
func (p *ConnectionPool) StartMaintenanceLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				p.Cleanup()
			}
		}
	}()
}
