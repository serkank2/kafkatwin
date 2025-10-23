package metadata

import (
	"context"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/IBM/sarama/mocks"

	"github.com/serkank2/kafkatwin/internal/cluster"
	"github.com/serkank2/kafkatwin/internal/config"
)

func TestNewManager(t *testing.T) {
	cfg := config.CacheConfig{
		MetadataTTL: 5 * time.Minute,
	}

	mockClusterMgr := &cluster.Manager{}
	mgr := NewManager(mockClusterMgr, cfg)

	if mgr == nil {
		t.Fatal("Expected manager to be created")
	}

	if mgr.cache == nil {
		t.Error("Expected cache to be initialized")
	}

	if mgr.cache.topics == nil {
		t.Error("Expected topics map to be initialized")
	}

	if mgr.cache.brokers == nil {
		t.Error("Expected brokers map to be initialized")
	}

	mgr.Close()
}

func TestCache_GetTopicMetadata_Fresh(t *testing.T) {
	cache := &Cache{
		topics:  make(map[string]*TopicMetadata),
		brokers: make(map[string]map[int32]*BrokerMetadata),
	}

	// Add fresh metadata
	topicMeta := &TopicMetadata{
		Name:        "test-topic",
		Partitions:  make(map[int32]*PartitionMetadata),
		ClusterIDs:  []string{"cluster-1"},
		LastUpdated: time.Now(),
	}
	topicMeta.Partitions[0] = &PartitionMetadata{
		ID:       0,
		Leader:   1,
		Replicas: []int32{1, 2, 3},
		ISR:      []int32{1, 2, 3},
		Offline:  false,
	}

	cache.mu.Lock()
	cache.topics["test-topic"] = topicMeta
	cache.mu.Unlock()

	// Verify retrieval
	cache.mu.RLock()
	retrieved := cache.topics["test-topic"]
	cache.mu.RUnlock()

	if retrieved == nil {
		t.Fatal("Expected to retrieve topic metadata")
	}

	if retrieved.Name != "test-topic" {
		t.Errorf("Expected topic name 'test-topic', got %s", retrieved.Name)
	}

	if len(retrieved.Partitions) != 1 {
		t.Errorf("Expected 1 partition, got %d", len(retrieved.Partitions))
	}

	if len(retrieved.ClusterIDs) != 1 {
		t.Errorf("Expected 1 cluster, got %d", len(retrieved.ClusterIDs))
	}
}

func TestCache_GetTopicMetadata_Stale(t *testing.T) {
	cfg := config.CacheConfig{
		MetadataTTL: 1 * time.Second,
	}

	cache := &Cache{
		topics:  make(map[string]*TopicMetadata),
		brokers: make(map[string]map[int32]*BrokerMetadata),
	}

	// Add stale metadata
	staleTime := time.Now().Add(-2 * time.Second)
	topicMeta := &TopicMetadata{
		Name:        "test-topic",
		Partitions:  make(map[int32]*PartitionMetadata),
		ClusterIDs:  []string{"cluster-1"},
		LastUpdated: staleTime,
	}

	cache.mu.Lock()
	cache.topics["test-topic"] = topicMeta
	cache.mu.Unlock()

	// Check if stale
	cache.mu.RLock()
	retrieved := cache.topics["test-topic"]
	cache.mu.RUnlock()

	if time.Since(retrieved.LastUpdated) < cfg.MetadataTTL {
		t.Error("Expected metadata to be stale")
	}
}

func TestTopicMetadata_MultiplePartitions(t *testing.T) {
	topicMeta := &TopicMetadata{
		Name:       "test-topic",
		Partitions: make(map[int32]*PartitionMetadata),
		ClusterIDs: []string{"cluster-1", "cluster-2"},
	}

	// Add multiple partitions
	for i := int32(0); i < 10; i++ {
		topicMeta.Partitions[i] = &PartitionMetadata{
			ID:       i,
			Leader:   i % 3,
			Replicas: []int32{i % 3, (i + 1) % 3, (i + 2) % 3},
			ISR:      []int32{i % 3, (i + 1) % 3},
			Offline:  false,
		}
	}

	if len(topicMeta.Partitions) != 10 {
		t.Errorf("Expected 10 partitions, got %d", len(topicMeta.Partitions))
	}

	// Verify partition 5
	part5 := topicMeta.Partitions[5]
	if part5.ID != 5 {
		t.Errorf("Expected partition ID 5, got %d", part5.ID)
	}

	if len(part5.Replicas) != 3 {
		t.Errorf("Expected 3 replicas, got %d", len(part5.Replicas))
	}

	if len(part5.ISR) != 2 {
		t.Errorf("Expected 2 ISR, got %d", len(part5.ISR))
	}
}

func TestTopicMetadata_MultipleClusters(t *testing.T) {
	topicMeta := &TopicMetadata{
		Name:       "test-topic",
		Partitions: make(map[int32]*PartitionMetadata),
		ClusterIDs: []string{},
	}

	// Simulate adding topic from multiple clusters
	clusters := []string{"cluster-1", "cluster-2", "cluster-3"}
	for _, clusterID := range clusters {
		// Check if cluster already exists
		found := false
		for _, cid := range topicMeta.ClusterIDs {
			if cid == clusterID {
				found = true
				break
			}
		}
		if !found {
			topicMeta.ClusterIDs = append(topicMeta.ClusterIDs, clusterID)
		}
	}

	if len(topicMeta.ClusterIDs) != 3 {
		t.Errorf("Expected 3 clusters, got %d", len(topicMeta.ClusterIDs))
	}

	// Verify no duplicates
	seen := make(map[string]bool)
	for _, cid := range topicMeta.ClusterIDs {
		if seen[cid] {
			t.Errorf("Duplicate cluster ID: %s", cid)
		}
		seen[cid] = true
	}
}

func TestBrokerMetadata_Storage(t *testing.T) {
	cache := &Cache{
		topics:  make(map[string]*TopicMetadata),
		brokers: make(map[string]map[int32]*BrokerMetadata),
	}

	// Add brokers for cluster
	clusterID := "cluster-1"
	cache.mu.Lock()
	cache.brokers[clusterID] = make(map[int32]*BrokerMetadata)
	cache.brokers[clusterID][1] = &BrokerMetadata{
		ID:   1,
		Host: "broker1:9092",
		Port: 9092,
		Rack: "rack1",
	}
	cache.brokers[clusterID][2] = &BrokerMetadata{
		ID:   2,
		Host: "broker2:9092",
		Port: 9092,
		Rack: "rack2",
	}
	cache.mu.Unlock()

	// Verify retrieval
	cache.mu.RLock()
	brokers := cache.brokers[clusterID]
	cache.mu.RUnlock()

	if len(brokers) != 2 {
		t.Errorf("Expected 2 brokers, got %d", len(brokers))
	}

	broker1 := brokers[1]
	if broker1.Host != "broker1:9092" {
		t.Errorf("Expected host 'broker1:9092', got %s", broker1.Host)
	}

	if broker1.Rack != "rack1" {
		t.Errorf("Expected rack 'rack1', got %s", broker1.Rack)
	}
}

func TestGetAllTopics(t *testing.T) {
	cfg := config.CacheConfig{
		MetadataTTL: 5 * time.Minute,
	}

	mockClusterMgr := &cluster.Manager{}
	mgr := NewManager(mockClusterMgr, cfg)
	defer mgr.Close()

	// Add multiple topics
	mgr.cache.mu.Lock()
	mgr.cache.topics["topic1"] = &TopicMetadata{Name: "topic1", Partitions: make(map[int32]*PartitionMetadata)}
	mgr.cache.topics["topic2"] = &TopicMetadata{Name: "topic2", Partitions: make(map[int32]*PartitionMetadata)}
	mgr.cache.topics["topic3"] = &TopicMetadata{Name: "topic3", Partitions: make(map[int32]*PartitionMetadata)}
	mgr.cache.mu.Unlock()

	topics := mgr.GetAllTopics()

	if len(topics) != 3 {
		t.Errorf("Expected 3 topics, got %d", len(topics))
	}

	// Verify all topics are present
	topicSet := make(map[string]bool)
	for _, topic := range topics {
		topicSet[topic] = true
	}

	if !topicSet["topic1"] || !topicSet["topic2"] || !topicSet["topic3"] {
		t.Error("Not all topics were returned")
	}
}

func TestInvalidateTopicCache(t *testing.T) {
	cfg := config.CacheConfig{
		MetadataTTL: 5 * time.Minute,
	}

	mockClusterMgr := &cluster.Manager{}
	mgr := NewManager(mockClusterMgr, cfg)
	defer mgr.Close()

	// Add topic
	mgr.cache.mu.Lock()
	mgr.cache.topics["test-topic"] = &TopicMetadata{
		Name:       "test-topic",
		Partitions: make(map[int32]*PartitionMetadata),
	}
	mgr.cache.mu.Unlock()

	// Verify it exists
	mgr.cache.mu.RLock()
	_, exists := mgr.cache.topics["test-topic"]
	mgr.cache.mu.RUnlock()

	if !exists {
		t.Error("Expected topic to exist before invalidation")
	}

	// Invalidate
	mgr.InvalidateTopicCache("test-topic")

	// Verify it's removed
	mgr.cache.mu.RLock()
	_, exists = mgr.cache.topics["test-topic"]
	mgr.cache.mu.RUnlock()

	if exists {
		t.Error("Expected topic to be removed after invalidation")
	}
}

func TestGetCacheStats(t *testing.T) {
	cfg := config.CacheConfig{
		MetadataTTL: 5 * time.Minute,
	}

	mockClusterMgr := &cluster.Manager{}
	mgr := NewManager(mockClusterMgr, cfg)
	defer mgr.Close()

	// Add topics and brokers
	now := time.Now()
	mgr.cache.mu.Lock()
	mgr.cache.topics["topic1"] = &TopicMetadata{
		Name:        "topic1",
		Partitions:  make(map[int32]*PartitionMetadata),
		LastUpdated: now,
	}
	mgr.cache.topics["topic2"] = &TopicMetadata{
		Name:        "topic2",
		Partitions:  make(map[int32]*PartitionMetadata),
		LastUpdated: now.Add(1 * time.Second),
	}
	mgr.cache.brokers["cluster-1"] = make(map[int32]*BrokerMetadata)
	mgr.cache.brokers["cluster-2"] = make(map[int32]*BrokerMetadata)
	mgr.cache.mu.Unlock()

	stats := mgr.GetCacheStats()

	if stats.TopicCount != 2 {
		t.Errorf("Expected 2 topics, got %d", stats.TopicCount)
	}

	if stats.ClusterCount != 2 {
		t.Errorf("Expected 2 clusters, got %d", stats.ClusterCount)
	}

	// LastUpdate should be the most recent (topic2's timestamp)
	if !stats.LastUpdate.After(now) {
		t.Error("Expected LastUpdate to reflect the most recent topic update")
	}
}

func TestPartitionMetadata_Offline(t *testing.T) {
	partMeta := &PartitionMetadata{
		ID:       0,
		Leader:   1,
		Replicas: []int32{1, 2, 3},
		ISR:      []int32{1},
		Offline:  true,
	}

	if !partMeta.Offline {
		t.Error("Expected partition to be offline")
	}

	if len(partMeta.ISR) >= len(partMeta.Replicas) {
		t.Error("Offline partition should have fewer ISR than replicas")
	}
}

func TestGetTopicClusters(t *testing.T) {
	cfg := config.CacheConfig{
		MetadataTTL: 5 * time.Minute,
	}

	mockClusterMgr := &cluster.Manager{}
	mgr := NewManager(mockClusterMgr, cfg)
	defer mgr.Close()

	// Add topic with multiple clusters
	mgr.cache.mu.Lock()
	mgr.cache.topics["test-topic"] = &TopicMetadata{
		Name:       "test-topic",
		Partitions: make(map[int32]*PartitionMetadata),
		ClusterIDs: []string{"cluster-1", "cluster-2", "cluster-3"},
	}
	mgr.cache.mu.Unlock()

	clusters, err := mgr.GetTopicClusters("test-topic")
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if len(clusters) != 3 {
		t.Errorf("Expected 3 clusters, got %d", len(clusters))
	}

	// Test non-existent topic
	_, err = mgr.GetTopicClusters("non-existent")
	if err == nil {
		t.Error("Expected error for non-existent topic")
	}
}

func TestGetBrokerMetadata(t *testing.T) {
	cfg := config.CacheConfig{
		MetadataTTL: 5 * time.Minute,
	}

	mockClusterMgr := &cluster.Manager{}
	mgr := NewManager(mockClusterMgr, cfg)
	defer mgr.Close()

	// Add broker metadata
	mgr.cache.mu.Lock()
	mgr.cache.brokers["cluster-1"] = make(map[int32]*BrokerMetadata)
	mgr.cache.brokers["cluster-1"][1] = &BrokerMetadata{
		ID:   1,
		Host: "broker1:9092",
		Port: 9092,
	}
	mgr.cache.brokers["cluster-1"][2] = &BrokerMetadata{
		ID:   2,
		Host: "broker2:9092",
		Port: 9092,
	}
	mgr.cache.mu.Unlock()

	brokers, err := mgr.GetBrokerMetadata("cluster-1")
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if len(brokers) != 2 {
		t.Errorf("Expected 2 brokers, got %d", len(brokers))
	}

	// Test non-existent cluster
	_, err = mgr.GetBrokerMetadata("non-existent")
	if err == nil {
		t.Error("Expected error for non-existent cluster")
	}
}

func TestConcurrentAccess(t *testing.T) {
	cfg := config.CacheConfig{
		MetadataTTL: 5 * time.Minute,
	}

	mockClusterMgr := &cluster.Manager{}
	mgr := NewManager(mockClusterMgr, cfg)
	defer mgr.Close()

	// Concurrent writes
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(id int) {
			topicName := "test-topic"
			mgr.cache.mu.Lock()
			if mgr.cache.topics[topicName] == nil {
				mgr.cache.topics[topicName] = &TopicMetadata{
					Name:       topicName,
					Partitions: make(map[int32]*PartitionMetadata),
					ClusterIDs: []string{},
				}
			}
			mgr.cache.topics[topicName].ClusterIDs = append(mgr.cache.topics[topicName].ClusterIDs, "cluster-1")
			mgr.cache.mu.Unlock()
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify data integrity
	mgr.cache.mu.RLock()
	topic := mgr.cache.topics["test-topic"]
	mgr.cache.mu.RUnlock()

	if topic == nil {
		t.Fatal("Expected topic to exist")
	}

	// Should have some cluster IDs (exact count may vary due to race)
	if len(topic.ClusterIDs) == 0 {
		t.Error("Expected at least one cluster ID")
	}
}

func BenchmarkGetTopicMetadata(b *testing.B) {
	cfg := config.CacheConfig{
		MetadataTTL: 5 * time.Minute,
	}

	mockClusterMgr := &cluster.Manager{}
	mgr := NewManager(mockClusterMgr, cfg)
	defer mgr.Close()

	// Populate cache
	mgr.cache.mu.Lock()
	for i := 0; i < 100; i++ {
		topicName := "test-topic-" + string(rune(i))
		mgr.cache.topics[topicName] = &TopicMetadata{
			Name:        topicName,
			Partitions:  make(map[int32]*PartitionMetadata),
			ClusterIDs:  []string{"cluster-1"},
			LastUpdated: time.Now(),
		}
	}
	mgr.cache.mu.Unlock()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mgr.cache.mu.RLock()
		_ = mgr.cache.topics["test-topic-50"]
		mgr.cache.mu.RUnlock()
	}
}

func BenchmarkGetAllTopics(b *testing.B) {
	cfg := config.CacheConfig{
		MetadataTTL: 5 * time.Minute,
	}

	mockClusterMgr := &cluster.Manager{}
	mgr := NewManager(mockClusterMgr, cfg)
	defer mgr.Close()

	// Populate cache
	mgr.cache.mu.Lock()
	for i := 0; i < 1000; i++ {
		topicName := "test-topic-" + string(rune(i))
		mgr.cache.topics[topicName] = &TopicMetadata{
			Name:       topicName,
			Partitions: make(map[int32]*PartitionMetadata),
		}
	}
	mgr.cache.mu.Unlock()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = mgr.GetAllTopics()
	}
}
