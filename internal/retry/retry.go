package retry

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"time"

	"go.uber.org/zap"

	"github.com/serkank2/kafkatwin/internal/monitoring"
)

// RetryPredicate determines if an error is retryable
type RetryPredicate func(error) bool

// Policy represents a retry policy
type Policy struct {
	MaxRetries      int
	BaseDelay       time.Duration
	MaxDelay        time.Duration
	Multiplier      float64
	Jitter          bool
	RetryPredicate  RetryPredicate
	OnRetry         func(attempt int, err error, delay time.Duration)
}

// DefaultPolicy returns a default retry policy
func DefaultPolicy() *Policy {
	return &Policy{
		MaxRetries:     3,
		BaseDelay:      100 * time.Millisecond,
		MaxDelay:       30 * time.Second,
		Multiplier:     2.0,
		Jitter:         true,
		RetryPredicate: DefaultRetryPredicate,
	}
}

// DefaultRetryPredicate is the default predicate for retryable errors
func DefaultRetryPredicate(err error) bool {
	if err == nil {
		return false
	}

	// Don't retry context errors
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	// Don't retry circuit breaker open errors
	if errors.Is(err, ErrCircuitOpen) {
		return false
	}

	// By default, retry all other errors
	return true
}

// Execute executes a function with retry logic
func (p *Policy) Execute(ctx context.Context, fn func() error) error {
	var lastErr error
	startTime := time.Now()

	for attempt := 0; attempt <= p.MaxRetries; attempt++ {
		// Check context before attempting
		select {
		case <-ctx.Done():
			monitoring.Debug("Retry aborted due to context cancellation",
				zap.Int("attempt", attempt),
				zap.Duration("elapsed", time.Since(startTime)),
			)
			return ctx.Err()
		default:
		}

		// Apply delay for retries (not for first attempt)
		if attempt > 0 {
			delay := p.calculateDelay(attempt)

			// Call OnRetry callback if provided
			if p.OnRetry != nil {
				p.OnRetry(attempt, lastErr, delay)
			}

			monitoring.Debug("Retrying operation",
				zap.Int("attempt", attempt),
				zap.Int("max_retries", p.MaxRetries),
				zap.Duration("delay", delay),
				zap.Error(lastErr),
			)

			// Wait with context awareness
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}

		// Execute the function
		err := fn()

		if err == nil {
			// Success
			if attempt > 0 {
				monitoring.Info("Operation succeeded after retries",
					zap.Int("attempts", attempt+1),
					zap.Duration("total_time", time.Since(startTime)),
				)
			}
			return nil
		}

		// Check if error is retryable
		lastErr = err
		if p.RetryPredicate != nil && !p.RetryPredicate(err) {
			monitoring.Debug("Error is not retryable",
				zap.Int("attempt", attempt),
				zap.Error(err),
			)
			return fmt.Errorf("non-retryable error: %w", err)
		}
	}

	monitoring.Warn("Max retries exceeded",
		zap.Int("max_retries", p.MaxRetries),
		zap.Duration("total_time", time.Since(startTime)),
		zap.Error(lastErr),
	)

	return fmt.Errorf("max retries (%d) exceeded: %w", p.MaxRetries, lastErr)
}

// calculateDelay calculates the delay for a given attempt with exponential backoff
func (p *Policy) calculateDelay(attempt int) time.Duration {
	// Calculate exponential delay
	delay := float64(p.BaseDelay) * math.Pow(p.Multiplier, float64(attempt-1))

	// Cap at max delay
	if delay > float64(p.MaxDelay) {
		delay = float64(p.MaxDelay)
	}

	if p.Jitter {
		// Full jitter: random value between 0 and calculated delay
		// This provides better distribution and reduces thundering herd
		delay = rand.Float64() * delay
	}

	return time.Duration(delay)
}

// ExecuteWithResult executes a function with retry logic and returns a result
func ExecuteWithResult[T any](ctx context.Context, policy *Policy, fn func() (T, error)) (T, error) {
	var result T
	var lastErr error
	startTime := time.Now()

	for attempt := 0; attempt <= policy.MaxRetries; attempt++ {
		// Check context before attempting
		select {
		case <-ctx.Done():
			monitoring.Debug("Retry aborted due to context cancellation",
				zap.Int("attempt", attempt),
				zap.Duration("elapsed", time.Since(startTime)),
			)
			return result, ctx.Err()
		default:
		}

		// Apply delay for retries (not for first attempt)
		if attempt > 0 {
			delay := policy.calculateDelay(attempt)

			// Call OnRetry callback if provided
			if policy.OnRetry != nil {
				policy.OnRetry(attempt, lastErr, delay)
			}

			monitoring.Debug("Retrying operation with result",
				zap.Int("attempt", attempt),
				zap.Int("max_retries", policy.MaxRetries),
				zap.Duration("delay", delay),
				zap.Error(lastErr),
			)

			// Wait with context awareness
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return result, ctx.Err()
			case <-timer.C:
			}
		}

		// Execute the function
		var err error
		result, err = fn()

		if err == nil {
			// Success
			if attempt > 0 {
				monitoring.Info("Operation with result succeeded after retries",
					zap.Int("attempts", attempt+1),
					zap.Duration("total_time", time.Since(startTime)),
				)
			}
			return result, nil
		}

		// Check if error is retryable
		lastErr = err
		if policy.RetryPredicate != nil && !policy.RetryPredicate(err) {
			monitoring.Debug("Error is not retryable",
				zap.Int("attempt", attempt),
				zap.Error(err),
			)
			return result, fmt.Errorf("non-retryable error: %w", err)
		}
	}

	monitoring.Warn("Max retries exceeded for operation with result",
		zap.Int("max_retries", policy.MaxRetries),
		zap.Duration("total_time", time.Since(startTime)),
		zap.Error(lastErr),
	)

	return result, fmt.Errorf("max retries (%d) exceeded: %w", policy.MaxRetries, lastErr)
}

// IsRetryable checks if an error is retryable using the default predicate
func IsRetryable(err error) bool {
	return DefaultRetryPredicate(err)
}

// NewPolicy creates a new retry policy with custom configuration
func NewPolicy(maxRetries int, baseDelay, maxDelay time.Duration) *Policy {
	return &Policy{
		MaxRetries:     maxRetries,
		BaseDelay:      baseDelay,
		MaxDelay:       maxDelay,
		Multiplier:     2.0,
		Jitter:         true,
		RetryPredicate: DefaultRetryPredicate,
	}
}
