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

	// Create circuit breaker
	circuitBreaker := retry.NewCircuitBreaker(retry.CircuitBreakerConfig{
		MaxFailures:       5,
		ResetTimeout:      60 * time.Second,
		HalfOpenSuccesses: 2,
	})

	// Create connection pool
	connPool := NewConnectionPool(clusterCfg, m.config.Performance.ConnectionPool)

	cluster := &Cluster{
		ID:             clusterCfg.ID,
		Config:         clusterCfg,
		Client:         client,
		Admin:          admin,
		ConnectionPool: connPool,
		CircuitBreaker: circuitBreaker,
		Health:         NewHealthMonitor(clusterCfg.ID, client),
	}

	// Start health monitoring
	cluster.Health.Start(m.ctx, m.config.Monitoring.Health.Interval)

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

// GetHealthyClusters returns all healthy clusters
func (m *Manager) GetHealthyClusters() []*Cluster {
	m.mu.RLock()
	defer m.mu.RUnlock()

	healthy := make([]*Cluster, 0)
	for _, cluster := range m.clusters {
		if cluster.IsHealthy() {
			healthy = append(healthy, cluster)
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

	// Close connections
	if cluster.Admin != nil {
		cluster.Admin.Close()
	}
	if cluster.Client != nil {
		cluster.Client.Close()
	}

	delete(m.clusters, id)

	monitoring.Info("Cluster removed", zap.String("cluster", id))

	return nil
}

// Close closes all cluster connections
func (m *Manager) Close() error {
	m.cancel()

	m.mu.Lock()
	defer m.mu.Unlock()

	for id, cluster := range m.clusters {
		cluster.Health.Stop()

		if cluster.Admin != nil {
			cluster.Admin.Close()
		}
		if cluster.Client != nil {
			cluster.Client.Close()
		}

		monitoring.Info("Cluster connection closed", zap.String("cluster", id))
	}

	return nil
}

// IsHealthy checks if the cluster is healthy
func (c *Cluster) IsHealthy() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Check circuit breaker state
	if c.CircuitBreaker.GetState() == retry.StateOpen {
		return false
	}

	// Check health monitor
	if c.Health != nil {
		return c.Health.IsHealthy()
	}

	return true
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
