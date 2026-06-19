package clients

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/rhpds/anarchy/babylon-runner/internal/httputil"
	"github.com/rhpds/anarchy/babylon-runner/internal/types"
)

// AnarchyClientConfig holds configuration for creating an AnarchyClient.
// Defined locally to avoid circular imports with the runner package.
type AnarchyClientConfig struct {
	BaseURL    string
	AuthHeader string
	Timeout    time.Duration
}

// AnarchyClient communicates with the Anarchy API to update subjects
// and schedule actions.
type AnarchyClient struct {
	baseURL     string
	authHeader  string
	client      *http.Client
	retryDelays []time.Duration
}

// NewAnarchyClient creates an AnarchyClient with a default HTTP client.
func NewAnarchyClient(cfg AnarchyClientConfig) *AnarchyClient {
	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = 35 * time.Second
	}
	return &AnarchyClient{
		baseURL:    cfg.BaseURL,
		authHeader: cfg.AuthHeader,
		client:     &http.Client{Timeout: timeout},
		retryDelays: []time.Duration{
			5 * time.Second,
			10 * time.Second,
			20 * time.Second,
		},
	}
}

// SubjectUpdate sends a PATCH request to update a subject's metadata, spec,
// or status. It retries on failure up to 3 times with increasing delays.
func (a *AnarchyClient) SubjectUpdate(ctx context.Context, subjectName string, patch types.SubjectPatch) error {
	url := fmt.Sprintf("%s/run/subject/%s", a.baseURL, subjectName)
	return a.doWithRetry(ctx, http.MethodPatch, url, patch)
}

// ScheduleAction sends a POST request to schedule an action on a subject.
// It retries on failure up to 3 times with increasing delays.
func (a *AnarchyClient) ScheduleAction(ctx context.Context, subjectName string, req types.ScheduleActionRequest) error {
	url := fmt.Sprintf("%s/run/subject/%s/actions", a.baseURL, subjectName)
	return a.doWithRetry(ctx, http.MethodPost, url, req)
}

// doWithRetry performs an HTTP request with JSON body, retrying up to
// len(retryDelays) times on non-200 responses or transport errors.
func (a *AnarchyClient) doWithRetry(ctx context.Context, method, url string, body interface{}) error {
	return httputil.RetryWithContext(ctx, a.retryDelays, func() error {
		status, err := httputil.DoJSON(ctx, a.client, method, url,
			map[string]string{"Authorization": a.authHeader}, body, nil)
		if err != nil {
			return err
		}
		if status >= 400 {
			return fmt.Errorf("%s %s: status %d", method, url, status)
		}
		return nil
	})
}
