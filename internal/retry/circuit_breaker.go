package retry

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/serkank2/kafkatwin/internal/monitoring"
)

var (
	// ErrCircuitOpen is returned when the circuit breaker is open
	ErrCircuitOpen = errors.New("circuit breaker is open")
)

// State represents the circuit breaker state
type State int

const (
	// StateClosed means requests are allowed
	StateClosed State = iota
	// StateOpen means requests are blocked
	StateOpen
	// StateHalfOpen means testing if service has recovered
	StateHalfOpen
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// requestResult represents a single request result in the sliding window
type requestResult struct {
	timestamp time.Time
	failed    bool
}

// CircuitBreaker implements the circuit breaker pattern with sliding window
type CircuitBreaker struct {
	mu                  sync.RWMutex
	state               State
	failureCount        int
	successCount        int
	lastFailureTime     time.Time
	lastStateChangeTime time.Time
	name                string

	// Sliding window for failure rate calculation
	window         []requestResult
	windowDuration time.Duration

	// Statistics
	totalRequests     int64
	totalFailures     int64
	totalSuccesses    int64
	consecutiveSuccesses int
	consecutiveFailures  int

	// Configuration
	maxFailures        int
	resetTimeout       time.Duration
	halfOpenSuccesses  int
}

// CircuitBreakerConfig contains circuit breaker configuration
type CircuitBreakerConfig struct {
	Name              string        // Name for logging and metrics
	MaxFailures       int           // Number of failures before opening
	ResetTimeout      time.Duration // Time to wait before transitioning from Open to Half-Open
	HalfOpenSuccesses int           // Number of successes in Half-Open before closing
	WindowDuration    time.Duration // Duration for sliding window (default: 1 minute)
}

// NewCircuitBreaker creates a new circuit breaker
func NewCircuitBreaker(config CircuitBreakerConfig) *CircuitBreaker {
	if config.MaxFailures <= 0 {
		config.MaxFailures = 5
	}
	if config.ResetTimeout <= 0 {
		config.ResetTimeout = 60 * time.Second
	}
	if config.HalfOpenSuccesses <= 0 {
		config.HalfOpenSuccesses = 2
	}
	if config.WindowDuration <= 0 {
		config.WindowDuration = 60 * time.Second
	}
	if config.Name == "" {
		config.Name = "circuit-breaker"
	}

	cb := &CircuitBreaker{
		name:                config.Name,
		state:               StateClosed,
		maxFailures:         config.MaxFailures,
		resetTimeout:        config.ResetTimeout,
		halfOpenSuccesses:   config.HalfOpenSuccesses,
		windowDuration:      config.WindowDuration,
		lastStateChangeTime: time.Now(),
		window:              make([]requestResult, 0, 100),
	}

	monitoring.Info("Circuit breaker initialized",
		zap.String("name", cb.name),
		zap.Int("max_failures", cb.maxFailures),
		zap.Duration("reset_timeout", cb.resetTimeout),
		zap.Duration("window_duration", cb.windowDuration),
	)

	return cb
}

// Execute executes a function through the circuit breaker
func (cb *CircuitBreaker) Execute(ctx context.Context, fn func() error) error {
	if err := cb.beforeRequest(); err != nil {
		monitoring.Debug("Circuit breaker rejected request",
			zap.String("name", cb.name),
			zap.String("state", cb.state.String()),
			zap.Error(err),
		)
		return err
	}

	startTime := time.Now()
	err := fn()
	duration := time.Since(startTime)

	cb.afterRequest(err)

	if err != nil {
		monitoring.Debug("Circuit breaker request failed",
			zap.String("name", cb.name),
			zap.Duration("duration", duration),
			zap.Error(err),
		)
	}

	return err
}

// ExecuteWithResult executes a function with result through the circuit breaker
func (cb *CircuitBreaker) ExecuteWithResult(ctx context.Context, fn func() (interface{}, error)) (interface{}, error) {
	if err := cb.beforeRequest(); err != nil {
		return nil, err
	}

	result, err := fn()

	cb.afterRequest(err)

	return result, err
}

// beforeRequest checks if request should be allowed
func (cb *CircuitBreaker) beforeRequest() error {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case StateClosed:
		return nil
	case StateOpen:
		// Check if we should transition to half-open
		timeSinceChange := time.Since(cb.lastStateChangeTime)
		if timeSinceChange >= cb.resetTimeout {
			monitoring.Info("Circuit breaker transitioning to HALF_OPEN",
				zap.String("name", cb.name),
				zap.Duration("time_in_open", timeSinceChange),
			)
			cb.setState(StateHalfOpen)
			return nil
		}
		return ErrCircuitOpen
	case StateHalfOpen:
		return nil
	}

	return nil
}

// afterRequest records the result of a request
func (cb *CircuitBreaker) afterRequest(err error) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	// Update statistics
	cb.totalRequests++

	// Add to sliding window
	now := time.Now()
	cb.window = append(cb.window, requestResult{
		timestamp: now,
		failed:    err != nil,
	})

	// Clean old entries from sliding window
	cb.cleanWindow(now)

	if err != nil {
		cb.totalFailures++
		cb.onFailure()
	} else {
		cb.totalSuccesses++
		cb.onSuccess()
	}
}

// cleanWindow removes expired entries from the sliding window
func (cb *CircuitBreaker) cleanWindow(now time.Time) {
	cutoff := now.Add(-cb.windowDuration)
	validStart := 0

	for i, result := range cb.window {
		if result.timestamp.After(cutoff) {
			validStart = i
			break
		}
	}

	if validStart > 0 {
		cb.window = cb.window[validStart:]
	}
}

// getWindowFailureRate calculates the failure rate in the current window
func (cb *CircuitBreaker) getWindowFailureRate() float64 {
	if len(cb.window) == 0 {
		return 0.0
	}

	failures := 0
	for _, result := range cb.window {
		if result.failed {
			failures++
		}
	}

	return float64(failures) / float64(len(cb.window))
}

// onSuccess handles successful request
func (cb *CircuitBreaker) onSuccess() {
	cb.consecutiveSuccesses++
	cb.consecutiveFailures = 0

	switch cb.state {
	case StateClosed:
		// Reset failure count on success
		if cb.failureCount > 0 {
			cb.failureCount = 0
		}
	case StateHalfOpen:
		cb.successCount++
		if cb.successCount >= cb.halfOpenSuccesses {
			monitoring.Info("Circuit breaker transitioning to CLOSED",
				zap.String("name", cb.name),
				zap.Int("consecutive_successes", cb.consecutiveSuccesses),
				zap.Float64("failure_rate", cb.getWindowFailureRate()),
			)
			cb.setState(StateClosed)
			cb.reset()
		}
	}
}

// onFailure handles failed request
func (cb *CircuitBreaker) onFailure() {
	cb.failureCount++
	cb.consecutiveFailures++
	cb.consecutiveSuccesses = 0
	cb.lastFailureTime = time.Now()

	switch cb.state {
	case StateClosed:
		if cb.failureCount >= cb.maxFailures {
			failureRate := cb.getWindowFailureRate()
			monitoring.Warn("Circuit breaker transitioning to OPEN",
				zap.String("name", cb.name),
				zap.Int("failure_count", cb.failureCount),
				zap.Int("consecutive_failures", cb.consecutiveFailures),
				zap.Float64("failure_rate", failureRate),
				zap.Time("last_failure", cb.lastFailureTime),
			)
			cb.setState(StateOpen)
		}
	case StateHalfOpen:
		monitoring.Warn("Circuit breaker returning to OPEN from HALF_OPEN",
			zap.String("name", cb.name),
			zap.Int("consecutive_failures", cb.consecutiveFailures),
		)
		cb.setState(StateOpen)
	}
}

// setState changes the circuit breaker state
func (cb *CircuitBreaker) setState(state State) {
	if cb.state != state {
		cb.state = state
		cb.lastStateChangeTime = time.Now()
	}
}

// reset resets counters
func (cb *CircuitBreaker) reset() {
	cb.failureCount = 0
	cb.successCount = 0
}

// GetState returns the current state
func (cb *CircuitBreaker) GetState() State {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state
}

// GetFailureCount returns the current failure count
func (cb *CircuitBreaker) GetFailureCount() int {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.failureCount
}

// Reset manually resets the circuit breaker to closed state
func (cb *CircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	monitoring.Info("Circuit breaker manually reset",
		zap.String("name", cb.name),
		zap.String("previous_state", cb.state.String()),
	)

	cb.setState(StateClosed)
	cb.reset()
}

// GetName returns the circuit breaker name
func (cb *CircuitBreaker) GetName() string {
	return cb.name
}

// Stats represents circuit breaker statistics
type Stats struct {
	State                State
	FailureCount         int
	SuccessCount         int
	ConsecutiveFailures  int
	ConsecutiveSuccesses int
	TotalRequests        int64
	TotalFailures        int64
	TotalSuccesses       int64
	FailureRate          float64
	LastFailureTime      time.Time
	LastStateChangeTime  time.Time
	TimeSinceStateChange time.Duration
	WindowSize           int
}

// GetStats returns detailed statistics about the circuit breaker
func (cb *CircuitBreaker) GetStats() Stats {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	return Stats{
		State:                cb.state,
		FailureCount:         cb.failureCount,
		SuccessCount:         cb.successCount,
		ConsecutiveFailures:  cb.consecutiveFailures,
		ConsecutiveSuccesses: cb.consecutiveSuccesses,
		TotalRequests:        cb.totalRequests,
		TotalFailures:        cb.totalFailures,
		TotalSuccesses:       cb.totalSuccesses,
		FailureRate:          cb.getWindowFailureRate(),
		LastFailureTime:      cb.lastFailureTime,
		LastStateChangeTime:  cb.lastStateChangeTime,
		TimeSinceStateChange: time.Since(cb.lastStateChangeTime),
		WindowSize:           len(cb.window),
	}
}
