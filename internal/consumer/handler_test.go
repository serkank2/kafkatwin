package consumer

import (
	"testing"
	"time"

	"github.com/IBM/sarama"
)

func TestDeduplicateMessages(t *testing.T) {
	handler := &Handler{}

	now := time.Now()

	tests := []struct {
		name     string
		messages []*Message
		expected int
	}{
		{
			name: "No duplicates",
			messages: []*Message{
				{Offset: 1, Timestamp: now, Topic: "test", Partition: 0},
				{Offset: 2, Timestamp: now.Add(time.Second), Topic: "test", Partition: 0},
				{Offset: 3, Timestamp: now.Add(2 * time.Second), Topic: "test", Partition: 0},
			},
			expected: 3,
		},
		{
			name: "Exact duplicates (same offset and timestamp)",
			messages: []*Message{
				{Offset: 1, Timestamp: now, Topic: "test", Partition: 0, ClusterID: "c1"},
				{Offset: 1, Timestamp: now, Topic: "test", Partition: 0, ClusterID: "c2"},
				{Offset: 2, Timestamp: now.Add(time.Second), Topic: "test", Partition: 0, ClusterID: "c1"},
			},
			expected: 2,
		},
		{
			name: "Same offset, different timestamp (keep both)",
			messages: []*Message{
				{Offset: 1, Timestamp: now, Topic: "test", Partition: 0, ClusterID: "c1"},
				{Offset: 1, Timestamp: now.Add(time.Millisecond), Topic: "test", Partition: 0, ClusterID: "c2"},
			},
			expected: 2,
		},
		{
			name: "Multiple duplicates",
			messages: []*Message{
				{Offset: 1, Timestamp: now, Topic: "test", Partition: 0, ClusterID: "c1"},
				{Offset: 1, Timestamp: now, Topic: "test", Partition: 0, ClusterID: "c2"},
				{Offset: 1, Timestamp: now, Topic: "test", Partition: 0, ClusterID: "c3"},
				{Offset: 2, Timestamp: now, Topic: "test", Partition: 0, ClusterID: "c1"},
			},
			expected: 2,
		},
		{
			name:     "Empty messages",
			messages: []*Message{},
			expected: 0,
		},
		{
			name:     "Nil messages",
			messages: nil,
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := handler.deduplicateMessages(tt.messages)

			if len(result) != tt.expected {
				t.Errorf("Expected %d messages after deduplication, got %d", tt.expected, len(result))
			}

			// Verify messages are in order
			for i := 1; i < len(result); i++ {
				if result[i].Offset < result[i-1].Offset {
					t.Error("Messages are not in offset order")
				}
			}
		})
	}
}

func TestMergeResults(t *testing.T) {
	handler := &Handler{
		config: FakeConsumerConfig{
			maxPollRecords: 500,
		},
	}

	now := time.Now()

	req := &FetchRequest{
		Topic:     "test-topic",
		Partition: 0,
	}

	offsetInfo := &OffsetInfo{
		ClusterOffsets: make(map[string]int64),
	}

	tests := []struct {
		name            string
		results         []*ClusterFetchResult
		expectedMsgCount int
	}{
		{
			name: "Single cluster with messages",
			results: []*ClusterFetchResult{
				{
					ClusterID:     "c1",
					Success:       true,
					HighWaterMark: 100,
					Messages: []*sarama.ConsumerMessage{
						{Topic: "test-topic", Partition: 0, Offset: 1, Timestamp: now},
						{Topic: "test-topic", Partition: 0, Offset: 2, Timestamp: now.Add(time.Second)},
					},
				},
			},
			expectedMsgCount: 2,
		},
		{
			name: "Multiple clusters with duplicates",
			results: []*ClusterFetchResult{
				{
					ClusterID:     "c1",
					Success:       true,
					HighWaterMark: 100,
					Messages: []*sarama.ConsumerMessage{
						{Topic: "test-topic", Partition: 0, Offset: 1, Timestamp: now},
						{Topic: "test-topic", Partition: 0, Offset: 2, Timestamp: now.Add(time.Second)},
					},
				},
				{
					ClusterID:     "c2",
					Success:       true,
					HighWaterMark: 100,
					Messages: []*sarama.ConsumerMessage{
						{Topic: "test-topic", Partition: 0, Offset: 1, Timestamp: now},
						{Topic: "test-topic", Partition: 0, Offset: 3, Timestamp: now.Add(2 * time.Second)},
					},
				},
			},
			expectedMsgCount: 3, // 1, 2, 3 (offset 1 is deduplicated)
		},
		{
			name: "One cluster fails",
			results: []*ClusterFetchResult{
				{
					ClusterID:     "c1",
					Success:       true,
					HighWaterMark: 100,
					Messages: []*sarama.ConsumerMessage{
						{Topic: "test-topic", Partition: 0, Offset: 1, Timestamp: now},
					},
				},
				{
					ClusterID: "c2",
					Success:   false,
					Messages:  []*sarama.ConsumerMessage{},
				},
			},
			expectedMsgCount: 1,
		},
		{
			name:            "All clusters fail",
			results:         []*ClusterFetchResult{
				{ClusterID: "c1", Success: false},
				{ClusterID: "c2", Success: false},
			},
			expectedMsgCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			response := handler.mergeResults(req, tt.results, offsetInfo)

			if response == nil {
				t.Fatal("Expected non-nil response")
			}

			if len(response.Messages) != tt.expectedMsgCount {
				t.Errorf("Expected %d messages, got %d", tt.expectedMsgCount, len(response.Messages))
			}

			// Verify messages are sorted by offset
			for i := 1; i < len(response.Messages); i++ {
				if response.Messages[i].Offset < response.Messages[i-1].Offset {
					t.Error("Messages are not sorted by offset")
				}
			}

			// Verify cluster results are stored
			if len(response.ClusterResults) != len(tt.results) {
				t.Errorf("Expected %d cluster results, got %d", len(tt.results), len(response.ClusterResults))
			}
		})
	}
}

func TestMessage_Structure(t *testing.T) {
	now := time.Now()

	msg := &Message{
		Topic:     "test-topic",
		Partition: 0,
		Offset:    123,
		Key:       []byte("key1"),
		Value:     []byte("value1"),
		Headers: []*sarama.RecordHeader{
			{Key: []byte("h1"), Value: []byte("v1")},
		},
		Timestamp: now,
		ClusterID: "cluster-1",
	}

	if msg.Topic != "test-topic" {
		t.Error("Topic mismatch")
	}

	if msg.Offset != 123 {
		t.Error("Offset mismatch")
	}

	if msg.ClusterID != "cluster-1" {
		t.Error("ClusterID mismatch")
	}

	if len(msg.Headers) != 1 {
		t.Error("Headers length mismatch")
	}
}

func TestFetchRequest_Validation(t *testing.T) {
	req := &FetchRequest{
		Topic:         "test-topic",
		Partition:     0,
		Offset:        100,
		MaxBytes:      1024000,
		MinBytes:      1,
		MaxWait:       500 * time.Millisecond,
		ConsumerGroup: "test-group",
	}

	if req.Topic == "" {
		t.Error("Topic should not be empty")
	}

	if req.Partition < 0 {
		t.Error("Partition should not be negative")
	}

	if req.MaxWait == 0 {
		t.Error("MaxWait should not be zero")
	}
}

func TestFetchResponse_Structure(t *testing.T) {
	resp := &FetchResponse{
		Topic:          "test-topic",
		Partition:      0,
		Messages:       make([]*Message, 0),
		HighWaterMark:  500,
		ClusterResults: make(map[string]*ClusterFetchResult),
	}

	resp.ClusterResults["c1"] = &ClusterFetchResult{
		ClusterID:     "c1",
		Success:       true,
		HighWaterMark: 500,
		Latency:       50 * time.Millisecond,
	}

	if resp.HighWaterMark != 500 {
		t.Error("HighWaterMark mismatch")
	}

	if len(resp.ClusterResults) != 1 {
		t.Error("ClusterResults length mismatch")
	}
}

func BenchmarkDeduplicateMessages(b *testing.B) {
	handler := &Handler{}
	now := time.Now()

	// Create a large set of messages with some duplicates
	messages := make([]*Message, 1000)
	for i := 0; i < 1000; i++ {
		messages[i] = &Message{
			Offset:    int64(i / 2), // Create duplicates
			Timestamp: now.Add(time.Duration(i) * time.Millisecond),
			Topic:     "test",
			Partition: 0,
			ClusterID: "c1",
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = handler.deduplicateMessages(messages)
	}
}

// FakeConsumerConfig for testing
type FakeConsumerConfig struct {
	maxPollRecords int
}

func (f FakeConsumerConfig) GetMaxPollRecords() int {
	return f.maxPollRecords
}
