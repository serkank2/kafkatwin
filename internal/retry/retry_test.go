package retry

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestDefaultPolicy(t *testing.T) {
	policy := DefaultPolicy()

	if policy.MaxRetries != 3 {
		t.Errorf("Expected MaxRetries=3, got %d", policy.MaxRetries)
	}

	if policy.BaseDelay != 100*time.Millisecond {
		t.Errorf("Expected BaseDelay=100ms, got %v", policy.BaseDelay)
	}

	if policy.Multiplier != 2.0 {
		t.Errorf("Expected Multiplier=2.0, got %f", policy.Multiplier)
	}

	if !policy.Jitter {
		t.Error("Expected Jitter to be enabled")
	}

	if policy.RetryPredicate == nil {
		t.Error("Expected RetryPredicate to be set")
	}
}

func TestNewPolicy(t *testing.T) {
	policy := NewPolicy(5, 50*time.Millisecond, 10*time.Second)

	if policy.MaxRetries != 5 {
		t.Errorf("Expected MaxRetries=5, got %d", policy.MaxRetries)
	}

	if policy.BaseDelay != 50*time.Millisecond {
		t.Errorf("Expected BaseDelay=50ms, got %v", policy.BaseDelay)
	}

	if policy.MaxDelay != 10*time.Second {
		t.Errorf("Expected MaxDelay=10s, got %v", policy.MaxDelay)
	}
}

func TestExecute_Success(t *testing.T) {
	policy := DefaultPolicy()
	ctx := context.Background()

	callCount := 0
	fn := func() error {
		callCount++
		return nil
	}

	err := policy.Execute(ctx, fn)

	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if callCount != 1 {
		t.Errorf("Expected 1 call, got %d", callCount)
	}
}

func TestExecute_SuccessAfterRetries(t *testing.T) {
	policy := DefaultPolicy()
	policy.BaseDelay = 10 * time.Millisecond
	ctx := context.Background()

	callCount := 0
	fn := func() error {
		callCount++
		if callCount < 3 {
			return errors.New("temporary error")
		}
		return nil
	}

	start := time.Now()
	err := policy.Execute(ctx, fn)
	duration := time.Since(start)

	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if callCount != 3 {
		t.Errorf("Expected 3 calls, got %d", callCount)
	}

	// Should have waited at least for 2 retries (baseDelay * 2^0 + baseDelay * 2^1)
	// With jitter, we expect at least some delay
	if duration < 5*time.Millisecond {
		t.Errorf("Expected some delay, got %v", duration)
	}
}

func TestExecute_MaxRetriesExceeded(t *testing.T) {
	policy := DefaultPolicy()
	policy.BaseDelay = 1 * time.Millisecond
	ctx := context.Background()

	callCount := 0
	testErr := errors.New("persistent error")
	fn := func() error {
		callCount++
		return testErr
	}

	err := policy.Execute(ctx, fn)

	if err == nil {
		t.Error("Expected error, got nil")
	}

	if !errors.Is(err, testErr) {
		t.Errorf("Expected error to wrap testErr, got %v", err)
	}

	// Should be called MaxRetries+1 times (initial + retries)
	expectedCalls := policy.MaxRetries + 1
	if callCount != expectedCalls {
		t.Errorf("Expected %d calls, got %d", expectedCalls, callCount)
	}
}

func TestExecute_ContextCancellation(t *testing.T) {
	policy := DefaultPolicy()
	policy.BaseDelay = 100 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())

	callCount := 0
	fn := func() error {
		callCount++
		if callCount == 2 {
			cancel()
		}
		return errors.New("error")
	}

	err := policy.Execute(ctx, fn)

	if !errors.Is(err, context.Canceled) {
		t.Errorf("Expected context.Canceled, got %v", err)
	}

	// Should stop after context is canceled
	if callCount > 3 {
		t.Errorf("Expected at most 3 calls, got %d", callCount)
	}
}

func TestExecute_ContextTimeout(t *testing.T) {
	policy := DefaultPolicy()
	policy.BaseDelay = 50 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	callCount := 0
	fn := func() error {
		callCount++
		return errors.New("error")
	}

	err := policy.Execute(ctx, fn)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Expected context.DeadlineExceeded, got %v", err)
	}
}

func TestExecute_NonRetryableError(t *testing.T) {
	policy := DefaultPolicy()
	ctx := context.Background()

	callCount := 0
	circuitErr := ErrCircuitOpen
	fn := func() error {
		callCount++
		return circuitErr
	}

	err := policy.Execute(ctx, fn)

	if err == nil {
		t.Error("Expected error, got nil")
	}

	// Should only be called once for non-retryable errors
	if callCount != 1 {
		t.Errorf("Expected 1 call for non-retryable error, got %d", callCount)
	}
}

func TestExecute_OnRetryCallback(t *testing.T) {
	policy := DefaultPolicy()
	policy.BaseDelay = 1 * time.Millisecond
	ctx := context.Background()

	callbackCount := 0
	policy.OnRetry = func(attempt int, err error, delay time.Duration) {
		callbackCount++
		if attempt != callbackCount {
			t.Errorf("Expected attempt %d, got %d", callbackCount, attempt)
		}
		if err == nil {
			t.Error("Expected error in callback, got nil")
		}
		if delay <= 0 {
			t.Errorf("Expected positive delay, got %v", delay)
		}
	}

	callCount := 0
	fn := func() error {
		callCount++
		if callCount < 3 {
			return errors.New("temporary error")
		}
		return nil
	}

	err := policy.Execute(ctx, fn)

	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if callbackCount != 2 {
		t.Errorf("Expected 2 callback calls, got %d", callbackCount)
	}
}

func TestExecuteWithResult_Success(t *testing.T) {
	policy := DefaultPolicy()
	ctx := context.Background()

	result, err := ExecuteWithResult(ctx, policy, func() (string, error) {
		return "success", nil
	})

	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if result != "success" {
		t.Errorf("Expected 'success', got %s", result)
	}
}

func TestExecuteWithResult_SuccessAfterRetries(t *testing.T) {
	policy := DefaultPolicy()
	policy.BaseDelay = 1 * time.Millisecond
	ctx := context.Background()

	callCount := 0
	result, err := ExecuteWithResult(ctx, policy, func() (int, error) {
		callCount++
		if callCount < 3 {
			return 0, errors.New("temporary error")
		}
		return 42, nil
	})

	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if result != 42 {
		t.Errorf("Expected 42, got %d", result)
	}

	if callCount != 3 {
		t.Errorf("Expected 3 calls, got %d", callCount)
	}
}

func TestExecuteWithResult_MaxRetriesExceeded(t *testing.T) {
	policy := DefaultPolicy()
	policy.BaseDelay = 1 * time.Millisecond
	ctx := context.Background()

	callCount := 0
	result, err := ExecuteWithResult(ctx, policy, func() (string, error) {
		callCount++
		return "", errors.New("persistent error")
	})

	if err == nil {
		t.Error("Expected error, got nil")
	}

	if result != "" {
		t.Errorf("Expected empty string, got %s", result)
	}

	expectedCalls := policy.MaxRetries + 1
	if callCount != expectedCalls {
		t.Errorf("Expected %d calls, got %d", expectedCalls, callCount)
	}
}

func TestDefaultRetryPredicate_Nil(t *testing.T) {
	if DefaultRetryPredicate(nil) {
		t.Error("Expected false for nil error")
	}
}

func TestDefaultRetryPredicate_ContextCanceled(t *testing.T) {
	if DefaultRetryPredicate(context.Canceled) {
		t.Error("Expected false for context.Canceled")
	}
}

func TestDefaultRetryPredicate_ContextDeadlineExceeded(t *testing.T) {
	if DefaultRetryPredicate(context.DeadlineExceeded) {
		t.Error("Expected false for context.DeadlineExceeded")
	}
}

func TestDefaultRetryPredicate_CircuitBreakerOpen(t *testing.T) {
	if DefaultRetryPredicate(ErrCircuitOpen) {
		t.Error("Expected false for ErrCircuitOpen")
	}
}

func TestDefaultRetryPredicate_RetryableError(t *testing.T) {
	if !DefaultRetryPredicate(errors.New("some error")) {
		t.Error("Expected true for regular errors")
	}
}

func TestIsRetryable(t *testing.T) {
	if IsRetryable(nil) {
		t.Error("Expected false for nil error")
	}

	if !IsRetryable(errors.New("some error")) {
		t.Error("Expected true for regular errors")
	}
}

func TestCalculateDelay(t *testing.T) {
	policy := &Policy{
		BaseDelay:  100 * time.Millisecond,
		MaxDelay:   10 * time.Second,
		Multiplier: 2.0,
		Jitter:     false,
	}

	// Test exponential backoff without jitter
	delay1 := policy.calculateDelay(1)
	if delay1 != 100*time.Millisecond {
		t.Errorf("Expected 100ms for attempt 1, got %v", delay1)
	}

	delay2 := policy.calculateDelay(2)
	if delay2 != 200*time.Millisecond {
		t.Errorf("Expected 200ms for attempt 2, got %v", delay2)
	}

	delay3 := policy.calculateDelay(3)
	if delay3 != 400*time.Millisecond {
		t.Errorf("Expected 400ms for attempt 3, got %v", delay3)
	}
}

func TestCalculateDelay_MaxDelay(t *testing.T) {
	policy := &Policy{
		BaseDelay:  1 * time.Second,
		MaxDelay:   2 * time.Second,
		Multiplier: 2.0,
		Jitter:     false,
	}

	delay := policy.calculateDelay(10)
	if delay > 2*time.Second {
		t.Errorf("Expected delay to be capped at 2s, got %v", delay)
	}
}

func TestCalculateDelay_Jitter(t *testing.T) {
	policy := &Policy{
		BaseDelay:  100 * time.Millisecond,
		MaxDelay:   10 * time.Second,
		Multiplier: 2.0,
		Jitter:     true,
	}

	// With jitter, delays should vary
	delays := make(map[time.Duration]bool)
	for i := 0; i < 10; i++ {
		delay := policy.calculateDelay(2)
		// Should be between 0 and 200ms
		if delay < 0 || delay > 200*time.Millisecond {
			t.Errorf("Expected delay between 0 and 200ms, got %v", delay)
		}
		delays[delay] = true
	}

	// With 10 attempts, we should see some variation (at least 3 different values)
	if len(delays) < 3 {
		t.Errorf("Expected at least 3 different delay values with jitter, got %d", len(delays))
	}
}

func BenchmarkExecute_NoRetries(b *testing.B) {
	policy := DefaultPolicy()
	ctx := context.Background()

	fn := func() error {
		return nil
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		policy.Execute(ctx, fn)
	}
}

func BenchmarkExecute_WithRetries(b *testing.B) {
	policy := DefaultPolicy()
	policy.BaseDelay = 1 * time.Millisecond
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		callCount := 0
		policy.Execute(ctx, func() error {
			callCount++
			if callCount < 3 {
				return errors.New("error")
			}
			return nil
		})
	}
}

func BenchmarkExecuteWithResult(b *testing.B) {
	policy := DefaultPolicy()
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ExecuteWithResult(ctx, policy, func() (int, error) {
			return 42, nil
		})
	}
}
