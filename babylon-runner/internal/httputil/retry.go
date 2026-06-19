package httputil

import (
	"context"
	"fmt"
	"time"
)

// RetryWithContext executes fn with retries, respecting context cancellation.
// It makes 1 + len(delays) attempts. Between attempt i and i+1, it waits
// delays[i]. Returns the last error if all attempts fail, or ctx.Err() if
// cancelled during a delay.
func RetryWithContext(ctx context.Context, delays []time.Duration, fn func() error) error {
	var lastErr error
	for i := 0; i <= len(delays); i++ {
		if i > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delays[i-1]):
			}
		}
		if err := fn(); err != nil {
			lastErr = err
			continue
		}
		return nil
	}
	return lastErr
}

// PollWithContext polls fn at interval until it returns done=true, a non-nil
// error, or context cancels. Returns an error if maxAttempts is exhausted
// without fn returning done=true.
func PollWithContext(ctx context.Context, interval time.Duration, maxAttempts int, fn func() (done bool, err error)) error {
	for i := 0; i < maxAttempts; i++ {
		if i > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(interval):
			}
		}
		done, err := fn()
		if err != nil {
			return err
		}
		if done {
			return nil
		}
	}
	return fmt.Errorf("poll exhausted after %d attempts", maxAttempts)
}
