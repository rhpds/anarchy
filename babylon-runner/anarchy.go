package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"
)

// retryDelays defines the wait times between retry attempts for API calls.
var retryDelays = []time.Duration{5 * time.Second, 10 * time.Second, 20 * time.Second}

// AnarchyClient communicates with the Anarchy API to update subjects
// and schedule actions.
type AnarchyClient struct {
	cfg    Config
	client *http.Client
}

// NewAnarchyClient creates an AnarchyClient with a default HTTP client.
func NewAnarchyClient(cfg Config) *AnarchyClient {
	return &AnarchyClient{
		cfg:    cfg,
		client: &http.Client{Timeout: time.Duration(cfg.RequestTimeout) * time.Second},
	}
}

// SubjectUpdate sends a PATCH request to update a subject's metadata, spec,
// or status. It retries on failure up to 3 times with increasing delays.
func (a *AnarchyClient) SubjectUpdate(ctx context.Context, subjectName string, patch SubjectPatch) error {
	url := fmt.Sprintf("%s/run/subject/%s", a.cfg.AnarchyURL, subjectName)
	return a.doWithRetry(ctx, http.MethodPatch, url, patch)
}

// ScheduleAction sends a POST request to schedule an action on a subject.
// It retries on failure up to 3 times with increasing delays.
func (a *AnarchyClient) ScheduleAction(ctx context.Context, subjectName string, req ScheduleActionRequest) error {
	url := fmt.Sprintf("%s/run/subject/%s/actions", a.cfg.AnarchyURL, subjectName)
	return a.doWithRetry(ctx, http.MethodPost, url, req)
}

// doWithRetry performs an HTTP request with JSON body, retrying up to
// len(retryDelays) times on non-200 responses or transport errors.
func (a *AnarchyClient) doWithRetry(ctx context.Context, method, url string, body interface{}) error {
	data, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal request body: %w", err)
	}

	var lastErr error
	maxAttempts := len(retryDelays) + 1
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			delay := retryDelays[attempt-1]
			slog.Warn("retrying request", "method", method, "url", url, "delay", delay, "attempt", attempt+1, "maxAttempts", maxAttempts)
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(data))
		if err != nil {
			return fmt.Errorf("create request: %w", err)
		}
		req.Header.Set("Authorization", a.cfg.AuthHeader())
		req.Header.Set("Content-Type", "application/json")

		resp, err := a.client.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			lastErr = fmt.Errorf("%s %s: %w", method, url, err)
			continue
		}
		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode == http.StatusOK {
			return nil
		}
		slog.Warn("API error response", "method", method, "url", url, "status", resp.StatusCode, "body", string(respBody))
		lastErr = fmt.Errorf("%s %s: status %d", method, url, resp.StatusCode)
	}
	return lastErr
}
