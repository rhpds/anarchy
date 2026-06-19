package clients

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// DefaultSandboxAPIURL is the default base URL for the Sandbox API
// when running inside the cluster.
const DefaultSandboxAPIURL = "http://sandbox-api.babylon-sandbox-api.svc.cluster.local:8080"

// SandboxAPIClient communicates with the Sandbox API to manage
// placements (book, start, stop, release) and authentication.
type SandboxAPIClient struct {
	baseURL          string
	client           *http.Client
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

// NewSandboxAPIClient creates a SandboxAPIClient with the given base URL.
func NewSandboxAPIClient(baseURL string, opts ...SandboxAPIOption) *SandboxAPIClient {
	// Build login retry delays: 40 attempts with 5s delay each.
	loginDelays := make([]time.Duration, 39) // 39 delays for 40 attempts
	for i := range loginDelays {
		loginDelays[i] = 5 * time.Second
	}
	c := &SandboxAPIClient{
		baseURL: baseURL,
		client:  &http.Client{Timeout: 30 * time.Second},
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
	return c
}

// Login authenticates with the Sandbox API using a login token and
// returns an access token. It retries up to 40 times with a 5-second
// delay between attempts on transport errors and non-200 responses.
// Decode errors and missing access_token are terminal (not retried).
func (s *SandboxAPIClient) Login(loginToken string) (string, error) {
	loginURL := fmt.Sprintf("%s/api/v1/login", s.baseURL)

	maxAttempts := len(s.loginRetryDelays) + 1

	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			select {
			case <-time.After(s.loginRetryDelays[attempt-1]):
			}
		}

		req, err := http.NewRequest(http.MethodPost, loginURL, nil)
		if err != nil {
			return "", fmt.Errorf("create request: %w", err)
		}
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", loginToken))

		resp, err := s.client.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("POST %s: %w", loginURL, err)
			continue
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			lastErr = fmt.Errorf("read response: %w", err)
			continue
		}

		if resp.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("POST %s: status %d", loginURL, resp.StatusCode)
			continue
		}

		var result map[string]string
		if err := json.Unmarshal(body, &result); err != nil {
			return "", fmt.Errorf("decode login response: %w", err)
		}

		token, ok := result["access_token"]
		if !ok || token == "" {
			return "", fmt.Errorf("login response missing access_token")
		}
		return token, nil
	}
	return "", fmt.Errorf("login failed after %d attempts: %w", maxAttempts, lastErr)
}

// GetPlacement retrieves a placement by UUID. Returns the placement
// data, HTTP status code, and any error. A 404 status is not treated
// as an error; the caller should check statusCode.
func (s *SandboxAPIClient) GetPlacement(accessToken, uuid string) (map[string]interface{}, int, error) {
	url := fmt.Sprintf("%s/api/v1/placements/%s", s.baseURL, uuid)

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, 0, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", accessToken))

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("GET %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, http.StatusNotFound, nil
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, resp.StatusCode, fmt.Errorf("GET %s: status %d", url, resp.StatusCode)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, resp.StatusCode, fmt.Errorf("decode response: %w", err)
	}
	return result, resp.StatusCode, nil
}

// BookPlacement creates a new placement. Returns the response body,
// HTTP status code, and any error. Multiple status codes (200, 202,
// 400, 401, 404, 409, 507) are accepted without error; the caller
// decides how to handle each.
func (s *SandboxAPIClient) BookPlacement(accessToken string, reqBody map[string]interface{}) (map[string]interface{}, int, error) {
	url := fmt.Sprintf("%s/api/v1/placements", s.baseURL)

	data, err := json.Marshal(reqBody)
	if err != nil {
		return nil, 0, fmt.Errorf("marshal request body: %w", err)
	}

	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		return nil, 0, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", accessToken))
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("POST %s: %w", url, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, fmt.Errorf("read response: %w", err)
	}

	// Accept these status codes without error; let the caller handle them.
	acceptedCodes := map[int]bool{
		200: true, 202: true, 400: true, 401: true,
		404: true, 409: true, 507: true,
	}

	var result map[string]interface{}
	if len(body) > 0 {
		if err := json.Unmarshal(body, &result); err != nil {
			return nil, resp.StatusCode, fmt.Errorf("decode response: %w", err)
		}
	}

	if !acceptedCodes[resp.StatusCode] {
		return result, resp.StatusCode, fmt.Errorf("POST %s: unexpected status %d", url, resp.StatusCode)
	}

	return result, resp.StatusCode, nil
}

// ReleasePlacement deletes a placement by UUID.
func (s *SandboxAPIClient) ReleasePlacement(accessToken, uuid string) error {
	url := fmt.Sprintf("%s/api/v1/placements/%s", s.baseURL, uuid)

	req, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", accessToken))

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("DELETE %s: %w", url, err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("DELETE %s: status %d", url, resp.StatusCode)
	}
	return nil
}

// StartPlacement starts a placement by UUID. It retries with backoff
// on failure.
func (s *SandboxAPIClient) StartPlacement(accessToken, uuid string) (map[string]interface{}, error) {
	url := fmt.Sprintf("%s/api/v1/placements/%s/start", s.baseURL, uuid)
	return s.doPlacementAction(accessToken, url)
}

// StopPlacement stops a placement by UUID. It retries with backoff
// on failure.
func (s *SandboxAPIClient) StopPlacement(accessToken, uuid string) (map[string]interface{}, error) {
	url := fmt.Sprintf("%s/api/v1/placements/%s/stop", s.baseURL, uuid)
	return s.doPlacementAction(accessToken, url)
}

// GetRequestStatus retrieves the status of an async request by ID.
func (s *SandboxAPIClient) GetRequestStatus(accessToken, requestID string) (map[string]interface{}, error) {
	url := fmt.Sprintf("%s/api/v1/requests/%s/status", s.baseURL, requestID)

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", accessToken))

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", url, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: status %d", url, resp.StatusCode)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return result, nil
}

// doPlacementAction performs a PUT request for placement start/stop
// operations with retry and backoff. Transport errors and non-200
// responses are retried; decode errors are terminal.
func (s *SandboxAPIClient) doPlacementAction(accessToken, actionURL string) (map[string]interface{}, error) {
	maxAttempts := len(s.retryDelays) + 1

	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			select {
			case <-time.After(s.retryDelays[attempt-1]):
			}
		}

		req, err := http.NewRequest(http.MethodPut, actionURL, nil)
		if err != nil {
			return nil, fmt.Errorf("create request: %w", err)
		}
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", accessToken))

		resp, err := s.client.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("PUT %s: %w", actionURL, err)
			continue
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			lastErr = fmt.Errorf("read response: %w", err)
			continue
		}

		if resp.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("PUT %s: status %d", actionURL, resp.StatusCode)
			continue
		}

		var result map[string]interface{}
		if err := json.Unmarshal(body, &result); err != nil {
			return nil, fmt.Errorf("decode response: %w", err)
		}
		return result, nil
	}
	return nil, fmt.Errorf("PUT %s failed after %d attempts: %w", actionURL, maxAttempts, lastErr)
}
