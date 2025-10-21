package ratelimit

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/serkank2/kafkatwin/internal/monitoring"
)

// Limiter manages rate limiting and quotas
type Limiter struct {
	buckets map[string]*TokenBucket // client_id or topic -> bucket
	quotas  map[string]*Quota        // client_id -> quota
	mu      sync.RWMutex
	config  Config
}

// Config contains rate limiter configuration
type Config struct {
	DefaultRequestsPerSecond int64
	DefaultBytesPerSecond    int64
	BurstSize                int64
	QuotaCheckInterval       time.Duration
}

// TokenBucket implements token bucket algorithm
type TokenBucket struct {
	capacity     int64
	tokens       int64
	refillRate   int64 // tokens per second
	lastRefill   time.Time
	mu           sync.Mutex
}

// Quota represents resource quota
type Quota struct {
	ClientID           string
	ProduceByteRate    int64 // bytes per second
	FetchByteRate      int64 // bytes per second
	RequestRate        int64 // requests per second
	CurrentProduceRate int64
	CurrentFetchRate   int64
	CurrentRequestRate int64
	Window             time.Duration
	LastReset          time.Time
	mu                 sync.Mutex
}

// NewLimiter creates a new rate limiter
func NewLimiter(config Config) *Limiter {
	if config.DefaultRequestsPerSecond == 0 {
		config.DefaultRequestsPerSecond = 1000
	}
	if config.DefaultBytesPerSecond == 0 {
		config.DefaultBytesPerSecond = 10 * 1024 * 1024 // 10MB/s
	}
	if config.BurstSize == 0 {
		config.BurstSize = config.DefaultRequestsPerSecond * 2
	}
	if config.QuotaCheckInterval == 0 {
		config.QuotaCheckInterval = 1 * time.Second
	}

	l := &Limiter{
		buckets: make(map[string]*TokenBucket),
		quotas:  make(map[string]*Quota),
		config:  config,
	}

	// Start quota checker
	go l.quotaChecker()

	return l
}

// AllowRequest checks if a request is allowed
func (l *Limiter) AllowRequest(ctx context.Context, clientID string, tokens int64) (bool, error) {
	bucket := l.getOrCreateBucket(clientID)
	return bucket.Take(tokens), nil
}

// AllowBytes checks if bytes quota allows the operation
func (l *Limiter) AllowBytes(ctx context.Context, clientID string, bytes int64, operation string) (bool, error) {
	quota := l.getOrCreateQuota(clientID)
	return quota.AllowBytes(bytes, operation), nil
}

// RecordProduceBytes records produced bytes for quota tracking
func (l *Limiter) RecordProduceBytes(clientID string, bytes int64) {
	quota := l.getOrCreateQuota(clientID)
	quota.RecordProduce(bytes)
}

// RecordFetchBytes records fetched bytes for quota tracking
func (l *Limiter) RecordFetchBytes(clientID string, bytes int64) {
	quota := l.getOrCreateQuota(clientID)
	quota.RecordFetch(bytes)
}

// RecordRequest records a request for quota tracking
func (l *Limiter) RecordRequest(clientID string) {
	quota := l.getOrCreateQuota(clientID)
	quota.RecordRequest()
}

// SetQuota sets quota for a client
func (l *Limiter) SetQuota(clientID string, quota *Quota) {
	l.mu.Lock()
	defer l.mu.Unlock()

	quota.ClientID = clientID
	quota.LastReset = time.Now()
	if quota.Window == 0 {
		quota.Window = 1 * time.Second
	}

	l.quotas[clientID] = quota

	monitoring.Info("Quota set for client",
		zap.String("client_id", clientID),
		zap.Int64("produce_byte_rate", quota.ProduceByteRate),
		zap.Int64("fetch_byte_rate", quota.FetchByteRate),
		zap.Int64("request_rate", quota.RequestRate),
	)
}

// GetQuota returns quota for a client
func (l *Limiter) GetQuota(clientID string) *Quota {
	l.mu.RLock()
	defer l.mu.RUnlock()

	quota, exists := l.quotas[clientID]
	if !exists {
		return nil
	}

	return quota
}

// RemoveQuota removes quota for a client
func (l *Limiter) RemoveQuota(clientID string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	delete(l.quotas, clientID)
	monitoring.Info("Quota removed for client", zap.String("client_id", clientID))
}

// getOrCreateBucket gets or creates a token bucket for a client
func (l *Limiter) getOrCreateBucket(clientID string) *TokenBucket {
	l.mu.RLock()
	bucket, exists := l.buckets[clientID]
	l.mu.RUnlock()

	if exists {
		return bucket
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	// Double-check after acquiring write lock
	bucket, exists = l.buckets[clientID]
	if exists {
		return bucket
	}

	bucket = NewTokenBucket(l.config.BurstSize, l.config.DefaultRequestsPerSecond)
	l.buckets[clientID] = bucket

	return bucket
}

// getOrCreateQuota gets or creates a quota for a client
func (l *Limiter) getOrCreateQuota(clientID string) *Quota {
	l.mu.RLock()
	quota, exists := l.quotas[clientID]
	l.mu.RUnlock()

	if exists {
		return quota
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	// Double-check after acquiring write lock
	quota, exists = l.quotas[clientID]
	if exists {
		return quota
	}

	quota = &Quota{
		ClientID:        clientID,
		ProduceByteRate: l.config.DefaultBytesPerSecond,
		FetchByteRate:   l.config.DefaultBytesPerSecond,
		RequestRate:     l.config.DefaultRequestsPerSecond,
		Window:          1 * time.Second,
		LastReset:       time.Now(),
	}
	l.quotas[clientID] = quota

	return quota
}

// quotaChecker periodically checks and resets quotas
func (l *Limiter) quotaChecker() {
	ticker := time.NewTicker(l.config.QuotaCheckInterval)
	defer ticker.Stop()

	for range ticker.C {
		l.mu.RLock()
		for _, quota := range l.quotas {
			quota.CheckReset()
		}
		l.mu.RUnlock()
	}
}

// NewTokenBucket creates a new token bucket
func NewTokenBucket(capacity int64, refillRate int64) *TokenBucket {
	return &TokenBucket{
		capacity:   capacity,
		tokens:     capacity,
		refillRate: refillRate,
		lastRefill: time.Now(),
	}
}

// Take attempts to take tokens from the bucket
func (tb *TokenBucket) Take(tokens int64) bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	tb.refill()

	if tb.tokens >= tokens {
		tb.tokens -= tokens
		return true
	}

	return false
}

// refill refills the bucket based on elapsed time
func (tb *TokenBucket) refill() {
	now := time.Now()
	elapsed := now.Sub(tb.lastRefill)

	tokensToAdd := int64(elapsed.Seconds() * float64(tb.refillRate))
	tb.tokens += tokensToAdd

	if tb.tokens > tb.capacity {
		tb.tokens = tb.capacity
	}

	tb.lastRefill = now
}

// AllowBytes checks if bytes are within quota
func (q *Quota) AllowBytes(bytes int64, operation string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	switch operation {
	case "produce":
		if q.ProduceByteRate <= 0 {
			return true
		}
		return q.CurrentProduceRate+bytes <= q.ProduceByteRate

	case "fetch":
		if q.FetchByteRate <= 0 {
			return true
		}
		return q.CurrentFetchRate+bytes <= q.FetchByteRate

	default:
		return true
	}
}

// RecordProduce records produced bytes
func (q *Quota) RecordProduce(bytes int64) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.CurrentProduceRate += bytes
}

// RecordFetch records fetched bytes
func (q *Quota) RecordFetch(bytes int64) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.CurrentFetchRate += bytes
}

// RecordRequest records a request
func (q *Quota) RecordRequest() {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.CurrentRequestRate++
}

// CheckReset checks if quota window has elapsed and resets counters
func (q *Quota) CheckReset() {
	q.mu.Lock()
	defer q.mu.Unlock()

	if time.Since(q.LastReset) >= q.Window {
		q.CurrentProduceRate = 0
		q.CurrentFetchRate = 0
		q.CurrentRequestRate = 0
		q.LastReset = time.Now()
	}
}

// IsExceeded checks if any quota is exceeded
func (q *Quota) IsExceeded() (bool, string) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.ProduceByteRate > 0 && q.CurrentProduceRate > q.ProduceByteRate {
		return true, fmt.Sprintf("produce byte rate exceeded: %d/%d", q.CurrentProduceRate, q.ProduceByteRate)
	}

	if q.FetchByteRate > 0 && q.CurrentFetchRate > q.FetchByteRate {
		return true, fmt.Sprintf("fetch byte rate exceeded: %d/%d", q.CurrentFetchRate, q.FetchByteRate)
	}

	if q.RequestRate > 0 && q.CurrentRequestRate > q.RequestRate {
		return true, fmt.Sprintf("request rate exceeded: %d/%d", q.CurrentRequestRate, q.RequestRate)
	}

	return false, ""
}

// GetStats returns quota statistics
func (q *Quota) GetStats() map[string]interface{} {
	q.mu.Lock()
	defer q.mu.Unlock()

	return map[string]interface{}{
		"client_id":             q.ClientID,
		"produce_byte_rate":     q.ProduceByteRate,
		"fetch_byte_rate":       q.FetchByteRate,
		"request_rate":          q.RequestRate,
		"current_produce_rate":  q.CurrentProduceRate,
		"current_fetch_rate":    q.CurrentFetchRate,
		"current_request_rate":  q.CurrentRequestRate,
		"window":                q.Window.String(),
		"time_until_reset":      q.Window - time.Since(q.LastReset),
	}
}
