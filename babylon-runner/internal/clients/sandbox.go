package clients

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/rhpds/anarchy/babylon-runner/internal/httputil"
)

// SandboxAPIClient communicates with the Sandbox API to manage
// placements (book, start, stop, release) and authentication.
// Login tokens are cached internally via httputil.TokenCache;
// callers no longer pass an accessToken to each method.
type SandboxAPIClient struct {
	baseURL          string
	client           *http.Client
	tokenCache       *httputil.TokenCache
	retryDelays      []time.Duration
	loginRetryDelays []time.Duration
}

// SandboxAPIOption configures optional SandboxAPIClient behaviour.
type SandboxAPIOption func(*SandboxAPIClient)

// WithNoRetries disables retry delays on the SandboxAPIClient.
// Intended for tests where the sandbox server responds deterministically.
func WithNoRetries() SandboxAPIOption {
	return func(c *SandboxAPIClient) {
		c.retryDelays = nil
		c.loginRetryDelays = nil
	}
}

// NewSandboxAPIClient creates a SandboxAPIClient with the given base URL
// and login token. The login token is used to obtain access tokens which
// are cached with a 1-hour TTL.
func NewSandboxAPIClient(baseURL, loginToken string, opts ...SandboxAPIOption) *SandboxAPIClient {
	// Build login retry delays: 40 attempts with 5s delay each.
	loginDelays := make([]time.Duration, 39) // 39 delays for 40 attempts
	for i := range loginDelays {
		loginDelays[i] = 5 * time.Second
	}
	c := &SandboxAPIClient{
		baseURL: baseURL,
		client: &http.Client{
			Timeout:   30 * time.Second,
			Transport: httputil.NewTransport(nil),
		},
		retryDelays: []time.Duration{
			5 * time.Second,
			10 * time.Second,
			20 * time.Second,
		},
		loginRetryDelays: loginDelays,
	}
	for _, opt := range opts {
		opt(c)
	}

	// Build token cache with login retry logic.
	c.tokenCache = httputil.NewTokenCache(func(ctx context.Context) (string, time.Duration, error) {
		return c.login(ctx, loginToken)
	})

	return c
}

// Close releases resources held by the client, including the cached
// access token.
func (c *SandboxAPIClient) Close(ctx context.Context) error {
	return c.tokenCache.Close(ctx)
}

// login authenticates with the Sandbox API using a login token and
// returns an access token with TTL. It retries on transport errors
// and non-200 responses. Decode errors and missing access_token are
// terminal (not retried).
func (c *SandboxAPIClient) login(ctx context.Context, loginToken string) (string, time.Duration, error) {
	loginURL := fmt.Sprintf("%s/api/v1/login", c.baseURL)
	headers := map[string]string{"Authorization": "Bearer " + loginToken}

	maxAttempts := len(c.loginRetryDelays) + 1

	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return "", 0, ctx.Err()
			case <-time.After(c.loginRetryDelays[attempt-1]):
			}
		}

		var result map[string]string
		status, err := httputil.DoJSON(ctx, c.client, http.MethodPost, loginURL, headers, nil, &result)
		if err != nil {
			if status >= 200 {
				// Got a response but decode failed — terminal.
				return "", 0, fmt.Errorf("decode login response: %w", err)
			}
			// Transport error — retryable.
			lastErr = fmt.Errorf("POST %s: %w", loginURL, err)
			continue
		}
		if status != http.StatusOK {
			lastErr = fmt.Errorf("POST %s: status %d", loginURL, status)
			continue
		}

		token := result["access_token"]
		if token == "" {
			// Terminal — bad response shape.
			return "", 0, fmt.Errorf("login response missing access_token")
		}
		return token, 1 * time.Hour, nil
	}
	return "", 0, fmt.Errorf("login failed after %d attempts: %w", maxAttempts, lastErr)
}

// accessToken returns the current cached access token, refreshing if needed.
func (c *SandboxAPIClient) accessToken(ctx context.Context) (string, error) {
	return c.tokenCache.Get(ctx)
}

// authHeaders returns the Authorization header map for the cached access token.
func (c *SandboxAPIClient) authHeaders(ctx context.Context) (map[string]string, error) {
	token, err := c.accessToken(ctx)
	if err != nil {
		return nil, err
	}
	return map[string]string{"Authorization": "Bearer " + token}, nil
}

// GetPlacement retrieves a placement by UUID. Returns the placement
// data, HTTP status code, and any error. A 404 status is not treated
// as an error; the caller should check statusCode.
func (c *SandboxAPIClient) GetPlacement(ctx context.Context, uuid string) (map[string]interface{}, int, error) {
	headers, err := c.authHeaders(ctx)
	if err != nil {
		return nil, 0, err
	}

	url := fmt.Sprintf("%s/api/v1/placements/%s", c.baseURL, uuid)

	var result map[string]interface{}
	status, err := httputil.DoJSON(ctx, c.client, http.MethodGet, url, headers, nil, &result)
	if err != nil {
		if status == http.StatusNotFound {
			return nil, http.StatusNotFound, nil
		}
		return nil, status, fmt.Errorf("GET %s: %w", url, err)
	}

	if status == http.StatusNotFound {
		return nil, http.StatusNotFound, nil
	}
	if status != http.StatusOK {
		return nil, status, fmt.Errorf("GET %s: status %d", url, status)
	}

	return result, status, nil
}

// BookPlacement creates a new placement. Returns the response body,
// HTTP status code, and any error. Multiple status codes (200, 202,
// 400, 401, 404, 409, 507) are accepted without error; the caller
// decides how to handle each.
func (c *SandboxAPIClient) BookPlacement(ctx context.Context, reqBody map[string]interface{}) (map[string]interface{}, int, error) {
	headers, err := c.authHeaders(ctx)
	if err != nil {
		return nil, 0, err
	}

	url := fmt.Sprintf("%s/api/v1/placements", c.baseURL)

	var result map[string]interface{}
	status, err := httputil.DoJSON(ctx, c.client, http.MethodPost, url, headers, reqBody, &result)
	if err != nil {
		return nil, status, fmt.Errorf("POST %s: %w", url, err)
	}

	// Accept these status codes without error; let the caller handle them.
	acceptedCodes := map[int]bool{
		200: true, 202: true, 400: true, 401: true,
		404: true, 409: true, 507: true,
	}

	if !acceptedCodes[status] {
		return result, status, fmt.Errorf("POST %s: unexpected status %d", url, status)
	}

	return result, status, nil
}

// ReleasePlacement deletes a placement by UUID.
func (c *SandboxAPIClient) ReleasePlacement(ctx context.Context, uuid string) error {
	headers, err := c.authHeaders(ctx)
	if err != nil {
		return err
	}

	url := fmt.Sprintf("%s/api/v1/placements/%s", c.baseURL, uuid)

	status, err := httputil.DoJSON(ctx, c.client, http.MethodDelete, url, headers, nil, nil)
	if err != nil {
		return fmt.Errorf("DELETE %s: %w", url, err)
	}
	if status != http.StatusOK {
		return fmt.Errorf("DELETE %s: status %d", url, status)
	}
	return nil
}

// StartPlacement starts a placement by UUID. It retries with backoff
// on failure.
func (c *SandboxAPIClient) StartPlacement(ctx context.Context, uuid string) (map[string]interface{}, error) {
	url := fmt.Sprintf("%s/api/v1/placements/%s/start", c.baseURL, uuid)
	return c.doPlacementAction(ctx, url)
}

// StopPlacement stops a placement by UUID. It retries with backoff
// on failure.
func (c *SandboxAPIClient) StopPlacement(ctx context.Context, uuid string) (map[string]interface{}, error) {
	url := fmt.Sprintf("%s/api/v1/placements/%s/stop", c.baseURL, uuid)
	return c.doPlacementAction(ctx, url)
}

// GetRequestStatus retrieves the status of an async request by ID.
func (c *SandboxAPIClient) GetRequestStatus(ctx context.Context, requestID string) (map[string]interface{}, error) {
	headers, err := c.authHeaders(ctx)
	if err != nil {
		return nil, err
	}

	url := fmt.Sprintf("%s/api/v1/requests/%s/status", c.baseURL, requestID)

	var result map[string]interface{}
	status, err := httputil.DoJSON(ctx, c.client, http.MethodGet, url, headers, nil, &result)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", url, err)
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("GET %s: status %d", url, status)
	}
	return result, nil
}

// doPlacementAction performs a PUT request for placement start/stop
// operations with retry and backoff. Transport errors and non-200
// responses are retried; decode errors are terminal.
func (c *SandboxAPIClient) doPlacementAction(ctx context.Context, actionURL string) (map[string]interface{}, error) {
	headers, err := c.authHeaders(ctx)
	if err != nil {
		return nil, err
	}

	maxAttempts := len(c.retryDelays) + 1

	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(c.retryDelays[attempt-1]):
			}
		}

		var result map[string]interface{}
		status, err := httputil.DoJSON(ctx, c.client, http.MethodPut, actionURL, headers, nil, &result)
		if err != nil {
			if status >= 200 {
				// Got a response but decode failed — terminal.
				return nil, fmt.Errorf("decode response: %w", err)
			}
			lastErr = fmt.Errorf("PUT %s: %w", actionURL, err)
			continue
		}
		if status != http.StatusOK {
			lastErr = fmt.Errorf("PUT %s: status %d", actionURL, status)
			continue
		}
		return result, nil
	}
	return nil, fmt.Errorf("PUT %s failed after %d attempts: %w", actionURL, maxAttempts, lastErr)
}
