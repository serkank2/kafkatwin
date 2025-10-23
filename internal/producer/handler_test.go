package producer

import (
	"context"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/serkank2/kafkatwin/internal/config"
)

func TestHandler_EvaluateResults(t *testing.T) {
	tests := []struct {
		name        string
		ackPolicy   string
		quorumCount int
		results     []*ClusterProduceResult
		shouldError bool
	}{
		{
			name:      "ALL_CLUSTERS success",
			ackPolicy: "ALL_CLUSTERS",
			results: []*ClusterProduceResult{
				{ClusterID: "c1", Success: true, Offset: 100},
				{ClusterID: "c2", Success: true, Offset: 200},
				{ClusterID: "c3", Success: true, Offset: 300},
			},
			shouldError: false,
		},
		{
			name:      "ALL_CLUSTERS failure",
			ackPolicy: "ALL_CLUSTERS",
			results: []*ClusterProduceResult{
				{ClusterID: "c1", Success: true, Offset: 100},
				{ClusterID: "c2", Success: false},
				{ClusterID: "c3", Success: true, Offset: 300},
			},
			shouldError: true,
		},
		{
			name:      "MAJORITY success",
			ackPolicy: "MAJORITY",
			results: []*ClusterProduceResult{
				{ClusterID: "c1", Success: true, Offset: 100},
				{ClusterID: "c2", Success: true, Offset: 200},
				{ClusterID: "c3", Success: false},
			},
			shouldError: false,
		},
		{
			name:      "MAJORITY failure",
			ackPolicy: "MAJORITY",
			results: []*ClusterProduceResult{
				{ClusterID: "c1", Success: true, Offset: 100},
				{ClusterID: "c2", Success: false},
				{ClusterID: "c3", Success: false},
			},
			shouldError: true,
		},
		{
			name:      "ANY success",
			ackPolicy: "ANY",
			results: []*ClusterProduceResult{
				{ClusterID: "c1", Success: true, Offset: 100},
				{ClusterID: "c2", Success: false},
				{ClusterID: "c3", Success: false},
			},
			shouldError: false,
		},
		{
			name:      "ANY failure",
			ackPolicy: "ANY",
			results: []*ClusterProduceResult{
				{ClusterID: "c1", Success: false},
				{ClusterID: "c2", Success: false},
				{ClusterID: "c3", Success: false},
			},
			shouldError: true,
		},
		{
			name:        "QUORUM success",
			ackPolicy:   "QUORUM",
			quorumCount: 2,
			results: []*ClusterProduceResult{
				{ClusterID: "c1", Success: true, Offset: 100},
				{ClusterID: "c2", Success: true, Offset: 200},
				{ClusterID: "c3", Success: false},
			},
			shouldError: false,
		},
		{
			name:        "QUORUM failure",
			ackPolicy:   "QUORUM",
			quorumCount: 3,
			results: []*ClusterProduceResult{
				{ClusterID: "c1", Success: true, Offset: 100},
				{ClusterID: "c2", Success: true, Offset: 200},
				{ClusterID: "c3", Success: false},
			},
			shouldError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := &Handler{
				config: config.ProducerConfig{
					AckPolicy:   tt.ackPolicy,
					QuorumCount: tt.quorumCount,
				},
			}

			req := &ProduceRequest{
				Topic:     "test-topic",
				Partition: 0,
			}

			resp, err := handler.evaluateResults(req, tt.results)

			if tt.shouldError && err == nil {
				t.Errorf("Expected error but got none")
			}

			if !tt.shouldError && err != nil {
				t.Errorf("Expected no error but got: %v", err)
			}

			if !tt.shouldError && resp == nil {
				t.Error("Expected response but got nil")
			}

			if !tt.shouldError && resp != nil {
				if len(resp.ClusterResults) != len(tt.results) {
					t.Errorf("Expected %d cluster results, got %d", len(tt.results), len(resp.ClusterResults))
				}
			}
		})
	}
}

func TestCopyHeaders(t *testing.T) {
	original := []*sarama.RecordHeader{
		{Key: []byte("key1"), Value: []byte("value1")},
		{Key: []byte("key2"), Value: []byte("value2")},
	}

	copied := copyHeaders(original)

	// Verify lengths match
	if len(copied) != len(original) {
		t.Fatalf("Expected %d headers, got %d", len(original), len(copied))
	}

	// Verify contents match
	for i := range original {
		if string(copied[i].Key) != string(original[i].Key) {
			t.Errorf("Header %d key mismatch: expected %s, got %s", i, string(original[i].Key), string(copied[i].Key))
		}
		if string(copied[i].Value) != string(original[i].Value) {
			t.Errorf("Header %d value mismatch: expected %s, got %s", i, string(original[i].Value), string(copied[i].Value))
		}
	}

	// Verify it's a deep copy (modifying copy shouldn't affect original)
	copied[0].Key[0] = 'X'
	if original[0].Key[0] == 'X' {
		t.Error("Modifying copied header affected original - not a deep copy")
	}
}

func TestCopyHeaders_Nil(t *testing.T) {
	copied := copyHeaders(nil)
	if copied != nil {
		t.Error("Expected nil for nil input")
	}
}

func TestCopyHeaders_Empty(t *testing.T) {
	original := []*sarama.RecordHeader{}
	copied := copyHeaders(original)

	if copied == nil {
		t.Error("Expected empty slice, got nil")
	}

	if len(copied) != 0 {
		t.Errorf("Expected 0 headers, got %d", len(copied))
	}
}

func TestProduceRequest_Validation(t *testing.T) {
	req := &ProduceRequest{
		Topic:     "test-topic",
		Partition: 0,
		Messages: []*sarama.ProducerMessage{
			{
				Topic: "test-topic",
				Value: sarama.StringEncoder("test message"),
			},
		},
	}

	if req.Topic == "" {
		t.Error("Topic should not be empty")
	}

	if len(req.Messages) == 0 {
		t.Error("Messages should not be empty")
	}
}

func TestProduceResponse_Structure(t *testing.T) {
	resp := &ProduceResponse{
		Topic:          "test-topic",
		Partition:      0,
		Offset:         100,
		Timestamp:      time.Now(),
		ClusterResults: make(map[string]*ClusterProduceResult),
	}

	resp.ClusterResults["cluster-1"] = &ClusterProduceResult{
		ClusterID: "cluster-1",
		Success:   true,
		Offset:    100,
		Latency:   50 * time.Millisecond,
	}

	if len(resp.ClusterResults) != 1 {
		t.Errorf("Expected 1 cluster result, got %d", len(resp.ClusterResults))
	}

	if resp.ClusterResults["cluster-1"].Success != true {
		t.Error("Expected cluster result to be successful")
	}
}

func BenchmarkCopyHeaders(b *testing.B) {
	headers := []*sarama.RecordHeader{
		{Key: []byte("key1"), Value: []byte("value1")},
		{Key: []byte("key2"), Value: []byte("value2")},
		{Key: []byte("key3"), Value: []byte("value3")},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = copyHeaders(headers)
	}
}
