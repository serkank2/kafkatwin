package metadata

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"go.uber.org/zap"

	"github.com/serkank2/kafkatwin/internal/cluster"
	"github.com/serkank2/kafkatwin/internal/config"
	"github.com/serkank2/kafkatwin/internal/monitoring"
)

// Manager manages metadata cache for topics and partitions
type Manager struct {
	clusterManager *cluster.Manager
	cache          *Cache
	config         config.CacheConfig
	ctx            context.Context
	cancel         context.CancelFunc
	mu             sync.RWMutex
}

// TopicMetadata represents topic metadata
type TopicMetadata struct {
	Name              string
	Partitions        map[int32]*PartitionMetadata
	ReplicationFactor int16
	Config            map[string]string
	ClusterIDs        []string // List of clusters where this topic exists
	LastUpdated       time.Time
}

// PartitionMetadata represents partition metadata
type PartitionMetadata struct {
	ID       int32
	Leader   int32
	Replicas []int32
	ISR      []int32
	Offline  bool
}

// BrokerMetadata represents broker metadata
type BrokerMetadata struct {
	ID   int32
	Host string
	Port int32
	Rack string
}

// Cache holds metadata cache
type Cache struct {
	topics  map[string]*TopicMetadata
	brokers map[string]map[int32]*BrokerMetadata // cluster -> broker map
	mu      sync.RWMutex
}

// NewManager creates a new metadata manager
func NewManager(clusterMgr *cluster.Manager, cfg config.CacheConfig) *Manager {
	ctx, cancel := context.WithCancel(context.Background())

	m := &Manager{
		clusterManager: clusterMgr,
		cache: &Cache{
			topics:  make(map[string]*TopicMetadata),
			brokers: make(map[string]map[int32]*BrokerMetadata),
		},
		config: cfg,
		ctx:    ctx,
		cancel: cancel,
	}

	// Start periodic refresh
	go m.refreshLoop()

	return m
}

// refreshLoop periodically refreshes metadata
func (m *Manager) refreshLoop() {
	ticker := time.NewTicker(m.config.MetadataTTL)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			if err := m.RefreshAll(); err != nil {
				monitoring.Error("Failed to refresh metadata", zap.Error(err))
			}
		}
	}
}

// RefreshAll refreshes metadata from all clusters
func (m *Manager) RefreshAll() error {
	clusters := m.clusterManager.GetHealthyClusters()
	if len(clusters) == 0 {
		return fmt.Errorf("no healthy clusters available")
	}

	var wg sync.WaitGroup
	errChan := make(chan error, len(clusters))

	for _, c := range clusters {
		wg.Add(1)
		go func(cluster *cluster.Cluster) {
			defer wg.Done()
			if err := m.refreshClusterMetadata(cluster); err != nil {
				errChan <- fmt.Errorf("cluster %s: %w", cluster.ID, err)
			}
		}(c)
	}

	wg.Wait()
	close(errChan)

	// Collect errors
	var errors []error
	for err := range errChan {
		errors = append(errors, err)
	}

	if len(errors) > 0 {
		return fmt.Errorf("metadata refresh errors: %v", errors)
	}

	return nil
}

// refreshClusterMetadata refreshes metadata from a specific cluster
func (m *Manager) refreshClusterMetadata(c *cluster.Cluster) error {
	// Refresh metadata
	if err := c.Client.RefreshMetadata(); err != nil {
		return fmt.Errorf("failed to refresh metadata: %w", err)
	}

	// Get topics
	topics, err := c.Client.Topics()
	if err != nil {
		return fmt.Errorf("failed to get topics: %w", err)
	}

	// Update topic metadata
	for _, topic := range topics {
		if err := m.updateTopicMetadata(c, topic); err != nil {
			monitoring.Warn("Failed to update topic metadata",
				zap.String("cluster", c.ID),
				zap.String("topic", topic),
				zap.Error(err),
			)
		}
	}

	// Update broker metadata
	m.updateBrokerMetadata(c)

	monitoring.Debug("Metadata refreshed",
		zap.String("cluster", c.ID),
		zap.Int("topics", len(topics)),
	)

	return nil
}

// updateTopicMetadata updates metadata for a specific topic
func (m *Manager) updateTopicMetadata(c *cluster.Cluster, topicName string) error {
	partitions, err := c.Client.Partitions(topicName)
	if err != nil {
		return fmt.Errorf("failed to get partitions: %w", err)
	}

	m.cache.mu.Lock()
	defer m.cache.mu.Unlock()

	// Get or create topic metadata
	topicMeta, exists := m.cache.topics[topicName]
	if !exists {
		topicMeta = &TopicMetadata{
			Name:        topicName,
			Partitions:  make(map[int32]*PartitionMetadata),
			ClusterIDs:  []string{},
		}
		m.cache.topics[topicName] = topicMeta
	}

	// Add cluster ID if not already present
	clusterExists := false
	for _, cid := range topicMeta.ClusterIDs {
		if cid == c.ID {
			clusterExists = true
			break
		}
	}
	if !clusterExists {
		topicMeta.ClusterIDs = append(topicMeta.ClusterIDs, c.ID)
	}

	// Update partition metadata
	for _, partitionID := range partitions {
		replicas, err := c.Client.Replicas(topicName, partitionID)
		if err != nil {
			continue
		}

		isr, err := c.Client.InSyncReplicas(topicName, partitionID)
		if err != nil {
			continue
		}

		leader, err := c.Client.Leader(topicName, partitionID)
		if err != nil {
			continue
		}

		offline := c.Client.Offline(topicName, partitionID)
		if err != nil {
			offline = nil
		}

		topicMeta.Partitions[partitionID] = &PartitionMetadata{
			ID:       partitionID,
			Leader:   leader.ID(),
			Replicas: replicas,
			ISR:      isr,
			Offline:  len(offline) > 0,
		}
	}

	topicMeta.LastUpdated = time.Now()

	return nil
}

// updateBrokerMetadata updates broker metadata
func (m *Manager) updateBrokerMetadata(c *cluster.Cluster) {
	brokers := c.Client.Brokers()

	m.cache.mu.Lock()
	defer m.cache.mu.Unlock()

	if m.cache.brokers[c.ID] == nil {
		m.cache.brokers[c.ID] = make(map[int32]*BrokerMetadata)
	}

	for _, broker := range brokers {
		m.cache.brokers[c.ID][broker.ID()] = &BrokerMetadata{
			ID:   broker.ID(),
			Host: broker.Addr(),
			Port: 0, // Sarama doesn't provide port separately
		}
	}
}

// GetTopicMetadata returns metadata for a topic
func (m *Manager) GetTopicMetadata(topic string) (*TopicMetadata, error) {
	m.cache.mu.RLock()
	defer m.cache.mu.RUnlock()

	meta, exists := m.cache.topics[topic]
	if !exists {
		// Try to refresh
		m.cache.mu.RUnlock()
		if err := m.RefreshAll(); err != nil {
			m.cache.mu.RLock()
			return nil, fmt.Errorf("topic not found and refresh failed: %w", err)
		}
		m.cache.mu.RLock()

		meta, exists = m.cache.topics[topic]
		if !exists {
			return nil, fmt.Errorf("topic %s not found", topic)
		}
	}

	return meta, nil
}

// GetPartitionMetadata returns metadata for a specific partition
func (m *Manager) GetPartitionMetadata(topic string, partition int32) (*PartitionMetadata, error) {
	topicMeta, err := m.GetTopicMetadata(topic)
	if err != nil {
		return nil, err
	}

	partMeta, exists := topicMeta.Partitions[partition]
	if !exists {
		return nil, fmt.Errorf("partition %d not found for topic %s", partition, topic)
	}

	return partMeta, nil
}

// GetAllTopics returns all cached topics
func (m *Manager) GetAllTopics() []string {
	m.cache.mu.RLock()
	defer m.cache.mu.RUnlock()

	topics := make([]string, 0, len(m.cache.topics))
	for topic := range m.cache.topics {
		topics = append(topics, topic)
	}

	return topics
}

// GetTopicClusters returns the list of clusters where a topic exists
func (m *Manager) GetTopicClusters(topic string) ([]string, error) {
	m.cache.mu.RLock()
	defer m.cache.mu.RUnlock()

	meta, exists := m.cache.topics[topic]
	if !exists {
		return nil, fmt.Errorf("topic %s not found", topic)
	}

	return meta.ClusterIDs, nil
}

// GetBrokerMetadata returns broker metadata for a cluster
func (m *Manager) GetBrokerMetadata(clusterID string) (map[int32]*BrokerMetadata, error) {
	m.cache.mu.RLock()
	defer m.cache.mu.RUnlock()

	brokers, exists := m.cache.brokers[clusterID]
	if !exists {
		return nil, fmt.Errorf("no broker metadata for cluster %s", clusterID)
	}

	return brokers, nil
}

// InvalidateTopicCache invalidates cache for a specific topic
func (m *Manager) InvalidateTopicCache(topic string) {
	m.cache.mu.Lock()
	defer m.cache.mu.Unlock()

	delete(m.cache.topics, topic)

	monitoring.Debug("Topic cache invalidated", zap.String("topic", topic))
}

// GetCacheStats returns cache statistics
type CacheStats struct {
	TopicCount  int
	ClusterCount int
	LastUpdate  time.Time
}

// GetCacheStats returns cache statistics
func (m *Manager) GetCacheStats() CacheStats {
	m.cache.mu.RLock()
	defer m.cache.mu.RUnlock()

	var lastUpdate time.Time
	for _, topic := range m.cache.topics {
		if topic.LastUpdated.After(lastUpdate) {
			lastUpdate = topic.LastUpdated
		}
	}

	return CacheStats{
		TopicCount:   len(m.cache.topics),
		ClusterCount: len(m.cache.brokers),
		LastUpdate:   lastUpdate,
	}
}

// Close stops the metadata manager
func (m *Manager) Close() {
	m.cancel()
}
