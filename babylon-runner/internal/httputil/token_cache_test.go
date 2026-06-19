package httputil

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestTokenCache_Get_CachesToken(t *testing.T) {
	calls := 0
	tc := NewTokenCache(func(ctx context.Context) (string, time.Duration, error) {
		calls++
		return "token-1", time.Hour, nil
	})

	tok1, err := tc.Get(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	tok2, err := tc.Get(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if tok1 != "token-1" || tok2 != "token-1" {
		t.Errorf("tokens = %q, %q, want %q", tok1, tok2, "token-1")
	}
	if calls != 1 {
		t.Errorf("refresh called %d times, want 1", calls)
	}
}

func TestTokenCache_Get_RefreshesExpired(t *testing.T) {
	calls := 0
	tc := NewTokenCache(func(ctx context.Context) (string, time.Duration, error) {
		calls++
		return fmt.Sprintf("token-%d", calls), 1 * time.Millisecond, nil
	})

	tok1, _ := tc.Get(context.Background())
	time.Sleep(5 * time.Millisecond)
	tok2, _ := tc.Get(context.Background())

	if tok1 == tok2 {
		t.Error("expected different tokens after expiry")
	}
	if calls != 2 {
		t.Errorf("refresh called %d times, want 2", calls)
	}
}

func TestTokenCache_Get_RefreshError(t *testing.T) {
	tc := NewTokenCache(func(ctx context.Context) (string, time.Duration, error) {
		return "", 0, errors.New("auth failed")
	})

	_, err := tc.Get(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestTokenCache_Get_ThreadSafe(t *testing.T) {
	calls := 0
	var mu sync.Mutex
	tc := NewTokenCache(func(ctx context.Context) (string, time.Duration, error) {
		mu.Lock()
		calls++
		mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		return "token", time.Hour, nil
	})

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := tc.Get(context.Background())
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if calls > 2 {
		t.Errorf("refresh called %d times, expected <=2 (double-check lock)", calls)
	}
}

func TestTokenCache_Close_WithCleanup(t *testing.T) {
	cleaned := ""
	tc := NewTokenCache(
		func(ctx context.Context) (string, time.Duration, error) {
			return "tok-to-clean", time.Hour, nil
		},
		WithCleanup(func(ctx context.Context, token string) error {
			cleaned = token
			return nil
		}),
	)

	tc.Get(context.Background())
	if err := tc.Close(context.Background()); err != nil {
		t.Fatalf("close error: %v", err)
	}
	if cleaned != "tok-to-clean" {
		t.Errorf("cleanup received %q, want %q", cleaned, "tok-to-clean")
	}
}

func TestTokenCache_Close_WithoutCleanup(t *testing.T) {
	tc := NewTokenCache(func(ctx context.Context) (string, time.Duration, error) {
		return "tok", time.Hour, nil
	})

	tc.Get(context.Background())
	if err := tc.Close(context.Background()); err != nil {
		t.Fatalf("close error: %v", err)
	}
}
