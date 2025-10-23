package retry

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestNewCircuitBreaker(t *testing.T) {
	config := CircuitBreakerConfig{
		Name:              "test-cb",
		MaxFailures:       5,
		ResetTimeout:      30 * time.Second,
		HalfOpenSuccesses: 2,
		WindowDuration:    60 * time.Second,
	}

	cb := NewCircuitBreaker(config)

	if cb.name != "test-cb" {
		t.Errorf("Expected name 'test-cb', got %s", cb.name)
	}

	if cb.maxFailures != 5 {
		t.Errorf("Expected maxFailures=5, got %d", cb.maxFailures)
	}

	if cb.resetTimeout != 30*time.Second {
		t.Errorf("Expected resetTimeout=30s, got %v", cb.resetTimeout)
	}

	if cb.halfOpenSuccesses != 2 {
		t.Errorf("Expected halfOpenSuccesses=2, got %d", cb.halfOpenSuccesses)
	}

	if cb.windowDuration != 60*time.Second {
		t.Errorf("Expected windowDuration=60s, got %v", cb.windowDuration)
	}

	if cb.GetState() != StateClosed {
		t.Errorf("Expected initial state CLOSED, got %s", cb.GetState())
	}
}

func TestNewCircuitBreaker_Defaults(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{})

	if cb.maxFailures != 5 {
		t.Errorf("Expected default maxFailures=5, got %d", cb.maxFailures)
	}

	if cb.resetTimeout != 60*time.Second {
		t.Errorf("Expected default resetTimeout=60s, got %v", cb.resetTimeout)
	}

	if cb.halfOpenSuccesses != 2 {
		t.Errorf("Expected default halfOpenSuccesses=2, got %d", cb.halfOpenSuccesses)
	}

	if cb.windowDuration != 60*time.Second {
		t.Errorf("Expected default windowDuration=60s, got %v", cb.windowDuration)
	}

	if cb.name != "circuit-breaker" {
		t.Errorf("Expected default name 'circuit-breaker', got %s", cb.name)
	}
}

func TestCircuitBreaker_Execute_Success(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{MaxFailures: 3})
	ctx := context.Background()

	callCount := 0
	fn := func() error {
		callCount++
		return nil
	}

	err := cb.Execute(ctx, fn)

	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if callCount != 1 {
		t.Errorf("Expected 1 call, got %d", callCount)
	}

	if cb.GetState() != StateClosed {
		t.Errorf("Expected state CLOSED, got %s", cb.GetState())
	}
}

func TestCircuitBreaker_Execute_Failures(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{MaxFailures: 3})
	ctx := context.Background()

	testErr := errors.New("test error")
	fn := func() error {
		return testErr
	}

	// First 3 failures should keep circuit closed
	for i := 0; i < 3; i++ {
		err := cb.Execute(ctx, fn)
		if !errors.Is(err, testErr) {
			t.Errorf("Expected testErr, got %v", err)
		}

		if i < 2 && cb.GetState() != StateClosed {
			t.Errorf("Expected state CLOSED after %d failures, got %s", i+1, cb.GetState())
		}
	}

	// After maxFailures, circuit should be open
	if cb.GetState() != StateOpen {
		t.Errorf("Expected state OPEN after %d failures, got %s", cb.maxFailures, cb.GetState())
	}

	// Further calls should be rejected without executing the function
	callCount := 0
	rejectedFn := func() error {
		callCount++
		return nil
	}

	err := cb.Execute(ctx, rejectedFn)
	if !errors.Is(err, ErrCircuitOpen) {
		t.Errorf("Expected ErrCircuitOpen, got %v", err)
	}

	if callCount != 0 {
		t.Error("Function should not be called when circuit is open")
	}
}

func TestCircuitBreaker_OpenToHalfOpen(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		MaxFailures:  2,
		ResetTimeout: 100 * time.Millisecond,
	})
	ctx := context.Background()

	// Trigger circuit to open
	for i := 0; i < 2; i++ {
		cb.Execute(ctx, func() error {
			return errors.New("error")
		})
	}

	if cb.GetState() != StateOpen {
		t.Error("Circuit should be open after max failures")
	}

	// Wait for reset timeout
	time.Sleep(150 * time.Millisecond)

	// Next request should transition to half-open
	callCount := 0
	cb.Execute(ctx, func() error {
		callCount++
		return nil
	})

	if callCount != 1 {
		t.Error("Function should be called when transitioning to half-open")
	}

	// After success in half-open, still need halfOpenSuccesses
	if cb.GetState() != StateHalfOpen {
		t.Errorf("Expected state HALF_OPEN, got %s", cb.GetState())
	}
}

func TestCircuitBreaker_HalfOpenToClosed(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		MaxFailures:       2,
		ResetTimeout:      50 * time.Millisecond,
		HalfOpenSuccesses: 2,
	})
	ctx := context.Background()

	// Open the circuit
	for i := 0; i < 2; i++ {
		cb.Execute(ctx, func() error {
			return errors.New("error")
		})
	}

	// Wait and transition to half-open
	time.Sleep(100 * time.Millisecond)

	// Execute successful requests to close circuit
	for i := 0; i < 2; i++ {
		err := cb.Execute(ctx, func() error {
			return nil
		})
		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
	}

	// Circuit should now be closed
	if cb.GetState() != StateClosed {
		t.Errorf("Expected state CLOSED after %d successes, got %s", cb.halfOpenSuccesses, cb.GetState())
	}
}

func TestCircuitBreaker_HalfOpenToOpen(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		MaxFailures:  2,
		ResetTimeout: 50 * time.Millisecond,
	})
	ctx := context.Background()

	// Open the circuit
	for i := 0; i < 2; i++ {
		cb.Execute(ctx, func() error {
			return errors.New("error")
		})
	}

	// Wait and transition to half-open
	time.Sleep(100 * time.Millisecond)
	cb.Execute(ctx, func() error {
		return nil
	})

	if cb.GetState() != StateHalfOpen {
		t.Error("Circuit should be half-open")
	}

	// Failure in half-open should reopen circuit
	cb.Execute(ctx, func() error {
		return errors.New("error")
	})

	if cb.GetState() != StateOpen {
		t.Errorf("Expected state OPEN after failure in half-open, got %s", cb.GetState())
	}
}

func TestCircuitBreaker_Reset(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{MaxFailures: 2})
	ctx := context.Background()

	// Open the circuit
	for i := 0; i < 2; i++ {
		cb.Execute(ctx, func() error {
			return errors.New("error")
		})
	}

	if cb.GetState() != StateOpen {
		t.Error("Circuit should be open")
	}

	// Manual reset
	cb.Reset()

	if cb.GetState() != StateClosed {
		t.Errorf("Expected state CLOSED after reset, got %s", cb.GetState())
	}

	if cb.GetFailureCount() != 0 {
		t.Errorf("Expected failure count 0 after reset, got %d", cb.GetFailureCount())
	}
}

func TestCircuitBreaker_ExecuteWithResult(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{})
	ctx := context.Background()

	result, err := cb.ExecuteWithResult(ctx, func() (interface{}, error) {
		return "success", nil
	})

	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if result != "success" {
		t.Errorf("Expected 'success', got %v", result)
	}
}

func TestCircuitBreaker_GetStats(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		MaxFailures: 3,
	})
	ctx := context.Background()

	// Execute some operations
	cb.Execute(ctx, func() error { return nil })
	cb.Execute(ctx, func() error { return errors.New("error") })
	cb.Execute(ctx, func() error { return nil })

	stats := cb.GetStats()

	if stats.State != StateClosed {
		t.Errorf("Expected state CLOSED, got %s", stats.State)
	}

	if stats.TotalRequests != 3 {
		t.Errorf("Expected 3 total requests, got %d", stats.TotalRequests)
	}

	if stats.TotalSuccesses != 2 {
		t.Errorf("Expected 2 successes, got %d", stats.TotalSuccesses)
	}

	if stats.TotalFailures != 1 {
		t.Errorf("Expected 1 failure, got %d", stats.TotalFailures)
	}

	if stats.FailureCount != 1 {
		t.Errorf("Expected failure count 1, got %d", stats.FailureCount)
	}
}

func TestCircuitBreaker_FailureRate(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		MaxFailures:    5,
		WindowDuration: 1 * time.Second,
	})
	ctx := context.Background()

	// Execute 10 requests: 7 success, 3 failures
	for i := 0; i < 10; i++ {
		cb.Execute(ctx, func() error {
			if i%3 == 0 {
				return errors.New("error")
			}
			return nil
		})
	}

	stats := cb.GetStats()

	expectedFailureRate := 0.4 // 4 failures out of 10
	if stats.FailureRate < 0.3 || stats.FailureRate > 0.5 {
		t.Errorf("Expected failure rate around %.2f, got %.2f", expectedFailureRate, stats.FailureRate)
	}
}

func TestCircuitBreaker_SlidingWindow(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		MaxFailures:    5,
		WindowDuration: 100 * time.Millisecond,
	})
	ctx := context.Background()

	// Add some failures
	for i := 0; i < 3; i++ {
		cb.Execute(ctx, func() error {
			return errors.New("error")
		})
	}

	stats1 := cb.GetStats()
	window1 := stats1.WindowSize

	if window1 != 3 {
		t.Errorf("Expected window size 3, got %d", window1)
	}

	// Wait for window to expire
	time.Sleep(150 * time.Millisecond)

	// Add new request to trigger cleanup
	cb.Execute(ctx, func() error {
		return nil
	})

	stats2 := cb.GetStats()
	window2 := stats2.WindowSize

	// Old entries should be cleaned up
	if window2 != 1 {
		t.Errorf("Expected window size 1 after expiration, got %d", window2)
	}
}

func TestCircuitBreaker_ConsecutiveFailures(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{MaxFailures: 5})
	ctx := context.Background()

	// 3 consecutive failures
	for i := 0; i < 3; i++ {
		cb.Execute(ctx, func() error {
			return errors.New("error")
		})
	}

	stats := cb.GetStats()
	if stats.ConsecutiveFailures != 3 {
		t.Errorf("Expected 3 consecutive failures, got %d", stats.ConsecutiveFailures)
	}

	// Success should reset consecutive counter
	cb.Execute(ctx, func() error {
		return nil
	})

	stats = cb.GetStats()
	if stats.ConsecutiveFailures != 0 {
		t.Errorf("Expected 0 consecutive failures after success, got %d", stats.ConsecutiveFailures)
	}

	if stats.ConsecutiveSuccesses != 1 {
		t.Errorf("Expected 1 consecutive success, got %d", stats.ConsecutiveSuccesses)
	}
}

func TestCircuitBreaker_GetName(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{Name: "my-circuit"})

	if cb.GetName() != "my-circuit" {
		t.Errorf("Expected name 'my-circuit', got %s", cb.GetName())
	}
}

func TestStateString(t *testing.T) {
	if StateClosed.String() != "closed" {
		t.Errorf("Expected 'closed', got %s", StateClosed.String())
	}

	if StateOpen.String() != "open" {
		t.Errorf("Expected 'open', got %s", StateOpen.String())
	}

	if StateHalfOpen.String() != "half-open" {
		t.Errorf("Expected 'half-open', got %s", StateHalfOpen.String())
	}
}

func TestCircuitBreaker_ConcurrentAccess(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{MaxFailures: 100})
	ctx := context.Background()

	done := make(chan bool)
	numGoroutines := 10
	numRequests := 100

	// Concurrent executions
	for g := 0; g < numGoroutines; g++ {
		go func() {
			for i := 0; i < numRequests; i++ {
				cb.Execute(ctx, func() error {
					if i%3 == 0 {
						return errors.New("error")
					}
					return nil
				})
			}
			done <- true
		}()
	}

	// Wait for all goroutines
	for g := 0; g < numGoroutines; g++ {
		<-done
	}

	stats := cb.GetStats()

	expectedTotal := int64(numGoroutines * numRequests)
	if stats.TotalRequests != expectedTotal {
		t.Errorf("Expected %d total requests, got %d", expectedTotal, stats.TotalRequests)
	}

	// Verify total = successes + failures
	if stats.TotalSuccesses+stats.TotalFailures != stats.TotalRequests {
		t.Error("Total requests should equal successes + failures")
	}
}

func BenchmarkCircuitBreaker_Execute_Closed(b *testing.B) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{})
	ctx := context.Background()

	fn := func() error {
		return nil
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cb.Execute(ctx, fn)
	}
}

func BenchmarkCircuitBreaker_Execute_Open(b *testing.B) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{MaxFailures: 1})
	ctx := context.Background()

	// Open the circuit
	cb.Execute(ctx, func() error {
		return errors.New("error")
	})

	fn := func() error {
		return nil
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cb.Execute(ctx, fn)
	}
}

func BenchmarkCircuitBreaker_GetStats(b *testing.B) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cb.GetStats()
	}
}
