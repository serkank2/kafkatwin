package consumer

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"go.uber.org/zap"

	"github.com/serkank2/kafkatwin/internal/cluster"
	"github.com/serkank2/kafkatwin/internal/config"
	"github.com/serkank2/kafkatwin/internal/metadata"
	"github.com/serkank2/kafkatwin/internal/monitoring"
)

// Handler handles consumer operations across multiple clusters
type Handler struct {
	clusterManager  *cluster.Manager
	metadataManager *metadata.Manager
	offsetStorage   OffsetStorage
	offsetMapper    *OffsetMapper
	config          config.ConsumerConfig
}

// FetchRequest represents a fetch request
type FetchRequest struct {
	Topic         string
	Partition     int32
	Offset        int64
	MaxBytes      int32
	MinBytes      int32
	MaxWait       time.Duration
	ConsumerGroup string
}

// FetchResponse represents a fetch response
type FetchResponse struct {
	Topic          string
	Partition      int32
	Messages       []*Message
	HighWaterMark  int64
	ClusterResults map[string]*ClusterFetchResult
}

// ClusterFetchResult represents the result from a single cluster
type ClusterFetchResult struct {
	ClusterID     string
	Success       bool
	Messages      []*sarama.ConsumerMessage
	HighWaterMark int64
	Error         error
	Latency       time.Duration
}

// Message represents a consumed message
type Message struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   []*sarama.RecordHeader
	Timestamp time.Time
	ClusterID string
}

// NewHandler creates a new consumer handler
func NewHandler(clusterMgr *cluster.Manager, metaMgr *metadata.Manager, cfg config.ConsumerConfig) (*Handler, error) {
	offsetStorage, err := NewOffsetStorage(cfg.OffsetStorage)
	if err != nil {
		return nil, fmt.Errorf("failed to create offset storage: %w", err)
	}

	return &Handler{
		clusterManager:  clusterMgr,
		metadataManager: metaMgr,
		offsetStorage:   offsetStorage,
		offsetMapper:    NewOffsetMapper(),
		config:          cfg,
	}, nil
}

// Fetch fetches messages from all healthy clusters
func (h *Handler) Fetch(ctx context.Context, req *FetchRequest) (*FetchResponse, error) {
	// Get healthy clusters
	clusters := h.clusterManager.GetHealthyClusters()
	if len(clusters) == 0 {
		return nil, fmt.Errorf("no healthy clusters available")
	}

	// Get current offsets from storage
	offsetInfo, err := h.getOffsetInfo(ctx, req)
	if err != nil {
		monitoring.Warn("Failed to get offset info", zap.Error(err))
		offsetInfo = &OffsetInfo{
			ClusterOffsets: make(map[string]int64),
		}
	}

	// Fetch from all clusters in parallel
	results := h.fetchFromAllClusters(ctx, clusters, req, offsetInfo)

	// Merge and sort messages
	response := h.mergeResults(req, results, offsetInfo)

	return response, nil
}

// fetchFromAllClusters fetches messages from all clusters in parallel
func (h *Handler) fetchFromAllClusters(ctx context.Context, clusters []*cluster.Cluster, req *FetchRequest, offsetInfo *OffsetInfo) []*ClusterFetchResult {
	var wg sync.WaitGroup
	results := make([]*ClusterFetchResult, len(clusters))

	for i, c := range clusters {
		wg.Add(1)
		go func(idx int, cluster *cluster.Cluster) {
			defer wg.Done()

			// Get cluster-specific offset
			clusterOffset := offsetInfo.ClusterOffsets[cluster.ID]
			if clusterOffset == 0 {
				clusterOffset = req.Offset
			}

			results[idx] = h.fetchFromCluster(ctx, cluster, req, clusterOffset)
		}(i, c)
	}

	wg.Wait()
	return results
}

// fetchFromCluster fetches messages from a single cluster
func (h *Handler) fetchFromCluster(ctx context.Context, c *cluster.Cluster, req *FetchRequest, offset int64) *ClusterFetchResult {
	start := time.Now()

	result := &ClusterFetchResult{
		ClusterID: c.ID,
		Messages:  make([]*sarama.ConsumerMessage, 0),
	}

	// Check circuit breaker
	if err := c.CircuitBreaker.Execute(ctx, func() error {
		return h.fetchMessages(ctx, c, req, offset, result)
	}); err != nil {
		result.Success = false
		result.Error = err
		result.Latency = time.Since(start)

		monitoring.RecordFetchRequest(req.Topic, c.ID, "error", 0, result.Latency)
		monitoring.RecordError("fetch", c.ID, "fetch_failed")

		monitoring.Warn("Failed to fetch from cluster",
			zap.String("cluster", c.ID),
			zap.String("topic", req.Topic),
			zap.Error(err),
		)

		return result
	}

	result.Success = true
	result.Latency = time.Since(start)

	// Calculate total bytes
	var totalBytes int64
	for _, msg := range result.Messages {
		totalBytes += int64(len(msg.Value))
	}

	monitoring.RecordFetchRequest(req.Topic, c.ID, "success", totalBytes, result.Latency)

	monitoring.Debug("Successfully fetched from cluster",
		zap.String("cluster", c.ID),
		zap.String("topic", req.Topic),
		zap.Int("messages", len(result.Messages)),
		zap.Duration("latency", result.Latency),
	)

	return result
}

// fetchMessages fetches messages from a cluster
func (h *Handler) fetchMessages(ctx context.Context, c *cluster.Cluster, req *FetchRequest, offset int64, result *ClusterFetchResult) error {
	consumer, err := c.ConnectionPool.GetConsumer()
	if err != nil {
		return fmt.Errorf("failed to get consumer: %w", err)
	}
	defer c.ConnectionPool.ReturnConsumer(consumer)

	// Create partition consumer
	partitionConsumer, err := consumer.ConsumePartition(req.Topic, req.Partition, offset)
	if err != nil {
		return fmt.Errorf("failed to create partition consumer: %w", err)
	}
	defer partitionConsumer.Close()

	// Fetch messages with timeout
	fetchCtx, cancel := context.WithTimeout(ctx, req.MaxWait)
	defer cancel()

	messages := make([]*sarama.ConsumerMessage, 0)
	var totalBytes int32

	for {
		select {
		case msg := <-partitionConsumer.Messages():
			messages = append(messages, msg)
			totalBytes += int32(len(msg.Value))

			if len(messages) >= h.config.MaxPollRecords || totalBytes >= req.MaxBytes {
				result.Messages = messages
				result.HighWaterMark = partitionConsumer.HighWaterMarkOffset()
				return nil
			}

		case err := <-partitionConsumer.Errors():
			return err

		case <-fetchCtx.Done():
			// Return what we have so far
			result.Messages = messages
			result.HighWaterMark = partitionConsumer.HighWaterMarkOffset()
			return nil
		}
	}
}

// mergeResults merges results from all clusters
func (h *Handler) mergeResults(req *FetchRequest, results []*ClusterFetchResult, offsetInfo *OffsetInfo) *FetchResponse {
	response := &FetchResponse{
		Topic:          req.Topic,
		Partition:      req.Partition,
		Messages:       make([]*Message, 0),
		ClusterResults: make(map[string]*ClusterFetchResult),
	}

	// Collect all messages
	allMessages := make([]*Message, 0)
	var maxHighWaterMark int64

	for _, result := range results {
		response.ClusterResults[result.ClusterID] = result

		if !result.Success {
			continue
		}

		if result.HighWaterMark > maxHighWaterMark {
			maxHighWaterMark = result.HighWaterMark
		}

		// Convert Sarama messages to our message format
		for _, msg := range result.Messages {
			allMessages = append(allMessages, &Message{
				Topic:     msg.Topic,
				Partition: msg.Partition,
				Offset:    msg.Offset,
				Key:       msg.Key,
				Value:     msg.Value,
				Headers:   msg.Headers,
				Timestamp: msg.Timestamp,
				ClusterID: result.ClusterID,
			})
		}
	}

	// Sort messages by offset (within partition) and timestamp
	sort.Slice(allMessages, func(i, j int) bool {
		if allMessages[i].Offset == allMessages[j].Offset {
			return allMessages[i].Timestamp.Before(allMessages[j].Timestamp)
		}
		return allMessages[i].Offset < allMessages[j].Offset
	})

	// Deduplicate messages (same offset from different clusters)
	response.Messages = h.deduplicateMessages(allMessages)

	// Limit to max poll records
	if len(response.Messages) > h.config.MaxPollRecords {
		response.Messages = response.Messages[:h.config.MaxPollRecords]
	}

	response.HighWaterMark = maxHighWaterMark

	return response
}

// deduplicateMessages removes duplicate messages
func (h *Handler) deduplicateMessages(messages []*Message) []*Message {
	seen := make(map[int64]bool)
	result := make([]*Message, 0)

	for _, msg := range messages {
		if !seen[msg.Offset] {
			seen[msg.Offset] = true
			result = append(result, msg)
		}
	}

	return result
}

// CommitOffset commits consumer offset
func (h *Handler) CommitOffset(ctx context.Context, group string, topic string, partition int32, offset int64) error {
	// Get current offset info
	offsetInfo, err := h.getOffsetInfo(ctx, &FetchRequest{
		Topic:         topic,
		Partition:     partition,
		ConsumerGroup: group,
	})
	if err != nil {
		offsetInfo = &OffsetInfo{
			ClusterOffsets: make(map[string]int64),
		}
	}

	// Update virtual offset
	offsetInfo.VirtualOffset = offset

	// For now, use the same offset for all clusters
	// In a more sophisticated implementation, we would track per-cluster offsets
	clusters := h.clusterManager.GetAllClusters()
	for _, c := range clusters {
		offsetInfo.ClusterOffsets[c.ID] = offset
	}

	// Commit to storage
	offsets := map[string]map[int32]*OffsetInfo{
		topic: {
			partition: offsetInfo,
		},
	}

	if err := h.offsetStorage.Commit(ctx, group, offsets); err != nil {
		return fmt.Errorf("failed to commit offset: %w", err)
	}

	monitoring.Debug("Offset committed",
		zap.String("group", group),
		zap.String("topic", topic),
		zap.Int32("partition", partition),
		zap.Int64("offset", offset),
	)

	return nil
}

// getOffsetInfo retrieves offset information
func (h *Handler) getOffsetInfo(ctx context.Context, req *FetchRequest) (*OffsetInfo, error) {
	if req.ConsumerGroup == "" {
		return &OffsetInfo{
			ClusterOffsets: make(map[string]int64),
		}, nil
	}

	offsets, err := h.offsetStorage.Fetch(ctx, req.ConsumerGroup, req.Topic)
	if err != nil {
		return nil, err
	}

	offsetInfo, exists := offsets[req.Partition]
	if !exists {
		return &OffsetInfo{
			ClusterOffsets: make(map[string]int64),
		}, nil
	}

	return offsetInfo, nil
}

// Close closes the consumer handler
func (h *Handler) Close() error {
	return nil
}
