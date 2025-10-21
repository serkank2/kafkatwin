package retry

import (
	"context"
	"errors"
	"sync"
	"time"
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

// CircuitBreaker implements the circuit breaker pattern
type CircuitBreaker struct {
	mu                  sync.RWMutex
	state               State
	failureCount        int
	successCount        int
	lastFailureTime     time.Time
	lastStateChangeTime time.Time

	// Configuration
	maxFailures        int
	resetTimeout       time.Duration
	halfOpenSuccesses  int
}

// CircuitBreakerConfig contains circuit breaker configuration
type CircuitBreakerConfig struct {
	MaxFailures       int           // Number of failures before opening
	ResetTimeout      time.Duration // Time to wait before transitioning from Open to Half-Open
	HalfOpenSuccesses int           // Number of successes in Half-Open before closing
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

	return &CircuitBreaker{
		state:              StateClosed,
		maxFailures:        config.MaxFailures,
		resetTimeout:       config.ResetTimeout,
		halfOpenSuccesses:  config.HalfOpenSuccesses,
		lastStateChangeTime: time.Now(),
	}
}

// Execute executes a function through the circuit breaker
func (cb *CircuitBreaker) Execute(ctx context.Context, fn func() error) error {
	if err := cb.beforeRequest(); err != nil {
		return err
	}

	err := fn()

	cb.afterRequest(err)

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
		if time.Since(cb.lastStateChangeTime) >= cb.resetTimeout {
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

	if err != nil {
		cb.onFailure()
	} else {
		cb.onSuccess()
	}
}

// onSuccess handles successful request
func (cb *CircuitBreaker) onSuccess() {
	switch cb.state {
	case StateClosed:
		cb.reset()
	case StateHalfOpen:
		cb.successCount++
		if cb.successCount >= cb.halfOpenSuccesses {
			cb.setState(StateClosed)
			cb.reset()
		}
	}
}

// onFailure handles failed request
func (cb *CircuitBreaker) onFailure() {
	cb.failureCount++
	cb.lastFailureTime = time.Now()

	switch cb.state {
	case StateClosed:
		if cb.failureCount >= cb.maxFailures {
			cb.setState(StateOpen)
		}
	case StateHalfOpen:
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
	cb.setState(StateClosed)
	cb.reset()
}
