package httputil

import (
	"context"
	"sync"
	"time"
)

// TokenCache provides thread-safe token caching with TTL and automatic
// refresh. Used by Tower (OAuth) and Sandbox (login) clients.
type TokenCache struct {
	mu      sync.RWMutex
	token   string
	expiry  time.Time
	refresh func(ctx context.Context) (token string, ttl time.Duration, err error)
	cleanup func(ctx context.Context, token string) error
}

// TokenCacheOption configures a TokenCache.
type TokenCacheOption func(*TokenCache)

// WithCleanup sets a cleanup function called by Close to release the token
// (e.g., Tower DELETE /api/v2/tokens/{id}). If not set, Close is a no-op.
func WithCleanup(fn func(ctx context.Context, token string) error) TokenCacheOption {
	return func(c *TokenCache) { c.cleanup = fn }
}

// NewTokenCache creates a token cache with the given refresh callback.
func NewTokenCache(refresh func(context.Context) (string, time.Duration, error), opts ...TokenCacheOption) *TokenCache {
	c := &TokenCache{refresh: refresh}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Get returns a valid token, refreshing if expired or missing.
// Thread-safe with double-check locking.
func (c *TokenCache) Get(ctx context.Context) (string, error) {
	c.mu.RLock()
	if c.token != "" && time.Now().Before(c.expiry) {
		defer c.mu.RUnlock()
		return c.token, nil
	}
	c.mu.RUnlock()

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.token != "" && time.Now().Before(c.expiry) {
		return c.token, nil
	}
	token, ttl, err := c.refresh(ctx)
	if err != nil {
		return "", err
	}
	c.token = token
	c.expiry = time.Now().Add(ttl)
	return token, nil
}

// Close cleans up the current token. No-op if no cleanup function was
// provided or if no token is cached.
func (c *TokenCache) Close(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cleanup != nil && c.token != "" {
		err := c.cleanup(ctx, c.token)
		c.token = ""
		return err
	}
	c.token = ""
	return nil
}
