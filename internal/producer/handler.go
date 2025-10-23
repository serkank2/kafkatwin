package producer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"go.uber.org/zap"

	"github.com/serkank2/kafkatwin/internal/cluster"
	"github.com/serkank2/kafkatwin/internal/config"
	"github.com/serkank2/kafkatwin/internal/metadata"
	"github.com/serkank2/kafkatwin/internal/monitoring"
)

// Handler handles producer operations across multiple clusters
type Handler struct {
	clusterManager  *cluster.Manager
	metadataManager *metadata.Manager
	config          config.ProducerConfig
}

// ProduceRequest represents a produce request
type ProduceRequest struct {
	Topic     string
	Partition int32
	Messages  []*sarama.ProducerMessage
}

// ProduceResponse represents a produce response
type ProduceResponse struct {
	Topic      string
	Partition  int32
	Offset     int64
	Timestamp  time.Time
	ClusterResults map[string]*ClusterProduceResult
}

// ClusterProduceResult represents the result from a single cluster
type ClusterProduceResult struct {
	ClusterID string
	Success   bool
	Offset    int64
	Error     error
	Latency   time.Duration
}

// NewHandler creates a new producer handler
func NewHandler(clusterMgr *cluster.Manager, metaMgr *metadata.Manager, cfg config.ProducerConfig) *Handler {
	return &Handler{
		clusterManager:  clusterMgr,
		metadataManager: metaMgr,
		config:          cfg,
	}
}

// Produce sends messages to all healthy clusters
func (h *Handler) Produce(ctx context.Context, req *ProduceRequest) (*ProduceResponse, error) {
	// Get healthy clusters
	clusters := h.clusterManager.GetHealthyClusters()
	if len(clusters) == 0 {
		return nil, fmt.Errorf("no healthy clusters available")
	}

	// Create context with timeout
	produceCtx, cancel := context.WithTimeout(ctx, h.config.Timeout)
	defer cancel()

	// Produce to all clusters in parallel
	results := h.produceToAllClusters(produceCtx, clusters, req)

	// Evaluate results based on ack policy
	response, err := h.evaluateResults(req, results)
	if err != nil {
		return nil, err
	}

	return response, nil
}

// produceToAllClusters sends messages to all clusters in parallel
func (h *Handler) produceToAllClusters(ctx context.Context, clusters []*cluster.Cluster, req *ProduceRequest) []*ClusterProduceResult {
	var wg sync.WaitGroup
	results := make([]*ClusterProduceResult, len(clusters))

	for i, c := range clusters {
		wg.Add(1)
		go func(idx int, cluster *cluster.Cluster) {
			defer wg.Done()
			results[idx] = h.produceToCluster(ctx, cluster, req)
		}(i, c)
	}

	wg.Wait()
	return results
}

// produceToCluster sends messages to a single cluster
func (h *Handler) produceToCluster(ctx context.Context, c *cluster.Cluster, req *ProduceRequest) *ClusterProduceResult {
	start := time.Now()

	result := &ClusterProduceResult{
		ClusterID: c.ID,
	}

	// Check circuit breaker
	if err := c.CircuitBreaker.Execute(ctx, func() error {
		return h.sendMessages(ctx, c, req, result)
	}); err != nil {
		result.Success = false
		result.Error = err
		result.Latency = time.Since(start)

		monitoring.RecordProduceRequest(req.Topic, c.ID, "error", 0, result.Latency)
		monitoring.RecordError("produce", c.ID, "send_failed")

		monitoring.Warn("Failed to produce to cluster",
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
	for _, msg := range req.Messages {
		if msg.Value != nil {
			b, _ := msg.Value.Encode()
			totalBytes += int64(len(b))
		}
	}

	monitoring.RecordProduceRequest(req.Topic, c.ID, "success", totalBytes, result.Latency)

	monitoring.Debug("Successfully produced to cluster",
		zap.String("cluster", c.ID),
		zap.String("topic", req.Topic),
		zap.Int("messages", len(req.Messages)),
		zap.Duration("latency", result.Latency),
	)

	return result
}

// sendMessages sends messages to a cluster using sync producer for reliability
func (h *Handler) sendMessages(ctx context.Context, c *cluster.Cluster, req *ProduceRequest, result *ClusterProduceResult) error {
	syncProducer, err := c.ConnectionPool.GetSyncProducer()
	if err != nil {
		return fmt.Errorf("failed to get sync producer: %w", err)
	}
	defer c.ConnectionPool.ReturnSyncProducer(syncProducer)

	var lastOffset int64

	// Send messages sequentially for guaranteed ordering and reliability
	for i, msg := range req.Messages {
		// Check context cancellation before sending each message
		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled after %d/%d messages: %w", i, len(req.Messages), ctx.Err())
		default:
		}

		// Copy message for this cluster to avoid data races
		clusterMsg := &sarama.ProducerMessage{
			Topic:     msg.Topic,
			Key:       msg.Key,
			Value:     msg.Value,
			Headers:   copyHeaders(msg.Headers),
			Metadata:  msg.Metadata,
			Partition: msg.Partition,
			Timestamp: msg.Timestamp,
		}

		// Send message synchronously
		partition, offset, err := syncProducer.SendMessage(clusterMsg)
		if err != nil {
			return fmt.Errorf("failed to send message %d/%d to cluster %s: %w", i+1, len(req.Messages), c.ID, err)
		}

		// Store last successful offset
		lastOffset = offset

		monitoring.Debug("Message sent successfully",
			zap.String("cluster", c.ID),
			zap.String("topic", msg.Topic),
			zap.Int32("partition", partition),
			zap.Int64("offset", offset),
			zap.Int("message_num", i+1),
			zap.Int("total_messages", len(req.Messages)),
		)
	}

	// Set the final offset from the last message
	result.Offset = lastOffset
	return nil
}

// copyHeaders creates a deep copy of headers to avoid data races
func copyHeaders(headers []*sarama.RecordHeader) []*sarama.RecordHeader {
	if headers == nil {
		return nil
	}

	copied := make([]*sarama.RecordHeader, len(headers))
	for i, h := range headers {
		copied[i] = &sarama.RecordHeader{
			Key:   append([]byte(nil), h.Key...),
			Value: append([]byte(nil), h.Value...),
		}
	}
	return copied
}

// evaluateResults evaluates results based on ack policy
func (h *Handler) evaluateResults(req *ProduceRequest, results []*ClusterProduceResult) (*ProduceResponse, error) {
	response := &ProduceResponse{
		Topic:          req.Topic,
		Partition:      req.Partition,
		Timestamp:      time.Now(),
		ClusterResults: make(map[string]*ClusterProduceResult),
	}

	successCount := 0
	for _, result := range results {
		response.ClusterResults[result.ClusterID] = result
		if result.Success {
			successCount++
			if response.Offset == 0 {
				response.Offset = result.Offset
			}
		}
	}

	totalClusters := len(results)

	// Evaluate based on ack policy
	switch h.config.AckPolicy {
	case "ALL_CLUSTERS":
		if successCount != totalClusters {
			return response, fmt.Errorf("not all clusters acknowledged (success: %d, total: %d)", successCount, totalClusters)
		}

	case "MAJORITY":
		majority := (totalClusters / 2) + 1
		if successCount < majority {
			return response, fmt.Errorf("majority not reached (success: %d, required: %d)", successCount, majority)
		}

	case "ANY":
		if successCount == 0 {
			return response, fmt.Errorf("no cluster acknowledged")
		}

	case "QUORUM":
		if successCount < h.config.QuorumCount {
			return response, fmt.Errorf("quorum not reached (success: %d, required: %d)", successCount, h.config.QuorumCount)
		}

	default:
		return response, fmt.Errorf("unknown ack policy: %s", h.config.AckPolicy)
	}

	return response, nil
}

// ProduceBatch produces a batch of messages
func (h *Handler) ProduceBatch(ctx context.Context, requests []*ProduceRequest) ([]*ProduceResponse, error) {
	responses := make([]*ProduceResponse, len(requests))
	var wg sync.WaitGroup
	errChan := make(chan error, len(requests))

	for i, req := range requests {
		wg.Add(1)
		go func(idx int, request *ProduceRequest) {
			defer wg.Done()

			resp, err := h.Produce(ctx, request)
			if err != nil {
				errChan <- err
				return
			}
			responses[idx] = resp
		}(i, req)
	}

	wg.Wait()
	close(errChan)

	// Collect errors
	var errors []error
	for err := range errChan {
		errors = append(errors, err)
	}

	if len(errors) > 0 {
		return responses, fmt.Errorf("batch produce errors: %v", errors)
	}

	return responses, nil
}
