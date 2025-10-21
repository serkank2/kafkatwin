package retry

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"time"
)

// Policy represents a retry policy
type Policy struct {
	MaxRetries int
	BaseDelay  time.Duration
	MaxDelay   time.Duration
	Multiplier float64
	Jitter     bool
}

// DefaultPolicy returns a default retry policy
func DefaultPolicy() *Policy {
	return &Policy{
		MaxRetries: 3,
		BaseDelay:  100 * time.Millisecond,
		MaxDelay:   30 * time.Second,
		Multiplier: 2.0,
		Jitter:     true,
	}
}

// Execute executes a function with retry logic
func (p *Policy) Execute(ctx context.Context, fn func() error) error {
	var lastErr error

	for attempt := 0; attempt <= p.MaxRetries; attempt++ {
		if attempt > 0 {
			delay := p.calculateDelay(attempt)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
		}

		if err := fn(); err != nil {
			lastErr = err
			continue
		}

		return nil
	}

	return fmt.Errorf("max retries exceeded: %w", lastErr)
}

// calculateDelay calculates the delay for a given attempt
func (p *Policy) calculateDelay(attempt int) time.Duration {
	delay := float64(p.BaseDelay) * math.Pow(p.Multiplier, float64(attempt-1))

	if delay > float64(p.MaxDelay) {
		delay = float64(p.MaxDelay)
	}

	if p.Jitter {
		// Add jitter: ±25% of the delay
		jitter := delay * 0.25
		delay = delay - jitter + (rand.Float64() * jitter * 2)
	}

	return time.Duration(delay)
}

// ExecuteWithResult executes a function with retry logic and returns a result
func ExecuteWithResult[T any](ctx context.Context, policy *Policy, fn func() (T, error)) (T, error) {
	var result T
	var lastErr error

	for attempt := 0; attempt <= policy.MaxRetries; attempt++ {
		if attempt > 0 {
			delay := policy.calculateDelay(attempt)
			select {
			case <-ctx.Done():
				return result, ctx.Err()
			case <-time.After(delay):
			}
		}

		var err error
		result, err = fn()
		if err != nil {
			lastErr = err
			continue
		}

		return result, nil
	}

	return result, fmt.Errorf("max retries exceeded: %w", lastErr)
}
