package httputil

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRetryWithContext_SuccessOnFirst(t *testing.T) {
	calls := 0
	err := RetryWithContext(context.Background(), []time.Duration{time.Millisecond}, func() error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 1 {
		t.Errorf("calls = %d, want 1", calls)
	}
}

func TestRetryWithContext_SuccessOnRetry(t *testing.T) {
	calls := 0
	err := RetryWithContext(context.Background(), []time.Duration{time.Millisecond, time.Millisecond}, func() error {
		calls++
		if calls < 3 {
			return errors.New("transient")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 3 {
		t.Errorf("calls = %d, want 3", calls)
	}
}

func TestRetryWithContext_AllFail(t *testing.T) {
	calls := 0
	err := RetryWithContext(context.Background(), []time.Duration{time.Millisecond}, func() error {
		calls++
		return errors.New("permanent")
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if calls != 2 {
		t.Errorf("calls = %d, want 2 (initial + 1 retry)", calls)
	}
}

func TestRetryWithContext_CancelledDuringDelay(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	go func() {
		time.Sleep(5 * time.Millisecond)
		cancel()
	}()

	err := RetryWithContext(ctx, []time.Duration{time.Second}, func() error {
		calls++
		return errors.New("fail")
	})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
	if calls != 1 {
		t.Errorf("calls = %d, want 1", calls)
	}
}

func TestPollWithContext_ImmediateSuccess(t *testing.T) {
	calls := 0
	err := PollWithContext(context.Background(), time.Millisecond, 10, func() (bool, error) {
		calls++
		return true, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 1 {
		t.Errorf("calls = %d, want 1", calls)
	}
}

func TestPollWithContext_EventualSuccess(t *testing.T) {
	calls := 0
	err := PollWithContext(context.Background(), time.Millisecond, 10, func() (bool, error) {
		calls++
		return calls >= 3, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 3 {
		t.Errorf("calls = %d, want 3", calls)
	}
}

func TestPollWithContext_MaxAttemptsExhausted(t *testing.T) {
	calls := 0
	err := PollWithContext(context.Background(), time.Millisecond, 3, func() (bool, error) {
		calls++
		return false, nil
	})
	if err == nil {
		t.Fatal("expected error after max attempts")
	}
	if calls != 3 {
		t.Errorf("calls = %d, want 3", calls)
	}
}
