package ratelimit

import (
	"context"
	"testing"
	"time"
)

func TestNewLimiter(t *testing.T) {
	config := Config{
		DefaultRequestsPerSecond: 100,
		DefaultBytesPerSecond:    1024,
		BurstSize:                200,
		QuotaCheckInterval:       100 * time.Millisecond,
	}

	limiter := NewLimiter(config)
	if limiter == nil {
		t.Fatal("Expected non-nil limiter")
	}
}

func TestTokenBucket_Take(t *testing.T) {
	bucket := NewTokenBucket(100, 50)

	// Should succeed - plenty of tokens
	if !bucket.Take(10) {
		t.Fatal("Expected to successfully take 10 tokens")
	}

	// Should succeed - still enough tokens
	if !bucket.Take(50) {
		t.Fatal("Expected to successfully take 50 tokens")
	}

	// Should fail - not enough tokens
	if bucket.Take(50) {
		t.Fatal("Expected to fail taking 50 tokens")
	}
}

func TestTokenBucket_Refill(t *testing.T) {
	bucket := NewTokenBucket(100, 100) // 100 tokens/second

	// Take all tokens
	bucket.Take(100)

	// Wait for refill (1 second should add 100 tokens)
	time.Sleep(1100 * time.Millisecond)

	// Should succeed after refill
	if !bucket.Take(90) {
		t.Fatal("Expected tokens to be refilled")
	}
}

func TestLimiter_AllowRequest(t *testing.T) {
	config := Config{
		DefaultRequestsPerSecond: 100,
		DefaultBytesPerSecond:    1024,
		BurstSize:                100,
	}

	limiter := NewLimiter(config)
	ctx := context.Background()

	// First request should succeed
	allowed, err := limiter.AllowRequest(ctx, "client1", 1)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !allowed {
		t.Fatal("Expected first request to be allowed")
	}
}

func TestQuota_AllowBytes(t *testing.T) {
	quota := &Quota{
		ClientID:        "client1",
		ProduceByteRate: 1000,
		FetchByteRate:   2000,
		Window:          1 * time.Second,
		LastReset:       time.Now(),
	}

	// Should allow within quota
	if !quota.AllowBytes(500, "produce") {
		t.Fatal("Expected to allow 500 bytes")
	}

	// Record some bytes
	quota.RecordProduce(800)

	// Should deny - would exceed quota
	if quota.AllowBytes(300, "produce") {
		t.Fatal("Expected to deny 300 bytes (would exceed quota)")
	}

	// Should still allow within quota for different operation
	if !quota.AllowBytes(1500, "fetch") {
		t.Fatal("Expected to allow fetch within quota")
	}
}

func TestQuota_CheckReset(t *testing.T) {
	quota := &Quota{
		ClientID:             "client1",
		ProduceByteRate:      1000,
		CurrentProduceRate:   500,
		CurrentFetchRate:     300,
		CurrentRequestRate:   10,
		Window:               100 * time.Millisecond,
		LastReset:            time.Now().Add(-200 * time.Millisecond),
	}

	quota.CheckReset()

	if quota.CurrentProduceRate != 0 {
		t.Fatal("Expected produce rate to be reset")
	}
	if quota.CurrentFetchRate != 0 {
		t.Fatal("Expected fetch rate to be reset")
	}
	if quota.CurrentRequestRate != 0 {
		t.Fatal("Expected request rate to be reset")
	}
}

func TestQuota_IsExceeded(t *testing.T) {
	quota := &Quota{
		ClientID:           "client1",
		ProduceByteRate:    1000,
		CurrentProduceRate: 1500,
		Window:             1 * time.Second,
		LastReset:          time.Now(),
	}

	exceeded, reason := quota.IsExceeded()
	if !exceeded {
		t.Fatal("Expected quota to be exceeded")
	}
	if reason == "" {
		t.Fatal("Expected reason for quota exceeded")
	}
}

func TestLimiter_SetGetQuota(t *testing.T) {
	config := Config{
		DefaultRequestsPerSecond: 100,
		QuotaCheckInterval:       1 * time.Second,
	}

	limiter := NewLimiter(config)

	quota := &Quota{
		ClientID:        "client1",
		ProduceByteRate: 5000,
		FetchByteRate:   10000,
		RequestRate:     200,
		Window:          1 * time.Second,
	}

	limiter.SetQuota("client1", quota)

	retrieved := limiter.GetQuota("client1")
	if retrieved == nil {
		t.Fatal("Expected to retrieve quota")
	}

	if retrieved.ProduceByteRate != 5000 {
		t.Fatalf("Expected ProduceByteRate 5000, got %d", retrieved.ProduceByteRate)
	}
}

func TestLimiter_RemoveQuota(t *testing.T) {
	config := Config{
		DefaultRequestsPerSecond: 100,
		QuotaCheckInterval:       1 * time.Second,
	}

	limiter := NewLimiter(config)

	quota := &Quota{
		ClientID:        "client1",
		ProduceByteRate: 5000,
		Window:          1 * time.Second,
	}

	limiter.SetQuota("client1", quota)
	limiter.RemoveQuota("client1")

	retrieved := limiter.GetQuota("client1")
	if retrieved != nil {
		t.Fatal("Expected quota to be removed")
	}
}

func TestQuota_GetStats(t *testing.T) {
	quota := &Quota{
		ClientID:             "client1",
		ProduceByteRate:      1000,
		FetchByteRate:        2000,
		RequestRate:          100,
		CurrentProduceRate:   500,
		CurrentFetchRate:     800,
		CurrentRequestRate:   50,
		Window:               1 * time.Second,
		LastReset:            time.Now(),
	}

	stats := quota.GetStats()

	if stats["client_id"] != "client1" {
		t.Fatal("Expected correct client_id in stats")
	}

	if stats["produce_byte_rate"] != int64(1000) {
		t.Fatal("Expected correct produce_byte_rate in stats")
	}
}

func BenchmarkTokenBucket_Take(b *testing.B) {
	bucket := NewTokenBucket(1000000, 100000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bucket.Take(1)
	}
}

func BenchmarkLimiter_AllowRequest(b *testing.B) {
	config := Config{
		DefaultRequestsPerSecond: 10000,
		BurstSize:                100000,
		QuotaCheckInterval:       1 * time.Second,
	}

	limiter := NewLimiter(config)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		limiter.AllowRequest(ctx, "client1", 1)
	}
}
