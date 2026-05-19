package main

import (
	"bytes"
	"encoding/json"
	"fmt"
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
func (a *AnarchyClient) SubjectUpdate(subjectName string, patch SubjectPatch) error {
	url := fmt.Sprintf("%s/run/subject/%s", a.cfg.AnarchyURL, subjectName)
	return a.doWithRetry(http.MethodPatch, url, patch)
}

// ScheduleAction sends a POST request to schedule an action on a subject.
// It retries on failure up to 3 times with increasing delays.
func (a *AnarchyClient) ScheduleAction(subjectName string, req ScheduleActionRequest) error {
	url := fmt.Sprintf("%s/run/subject/%s/actions", a.cfg.AnarchyURL, subjectName)
	return a.doWithRetry(http.MethodPost, url, req)
}

// doWithRetry performs an HTTP request with JSON body, retrying up to
// len(retryDelays) times on non-200 responses or transport errors.
func (a *AnarchyClient) doWithRetry(method, url string, body interface{}) error {
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
			time.Sleep(delay)
		}

		req, err := http.NewRequest(method, url, bytes.NewReader(data))
		if err != nil {
			return fmt.Errorf("create request: %w", err)
		}
		req.Header.Set("Authorization", a.cfg.AuthHeader())
		req.Header.Set("Content-Type", "application/json")

		resp, err := a.client.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("%s %s: %w", method, url, err)
			continue
		}
		resp.Body.Close()

		if resp.StatusCode == http.StatusOK {
			return nil
		}
		lastErr = fmt.Errorf("%s %s: status %d", method, url, resp.StatusCode)
	}
	return lastErr
}
