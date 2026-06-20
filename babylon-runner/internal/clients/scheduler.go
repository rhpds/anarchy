package clients

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"time"

	"github.com/rhpds/anarchy/babylon-runner/internal/httputil"
	"github.com/rhpds/anarchy/babylon-runner/internal/metrics"
	"github.com/rhpds/anarchy/babylon-runner/internal/types"
)

// Candidate represents a controller hostname eligible for job scheduling.
type Candidate struct {
	Domain string `json:"domain"`
}

// EvaluateRequest is the request body for the controller scheduler API.
type EvaluateRequest struct {
	Candidates    []Candidate                    `json:"candidates"`
	RequireLabels map[string]types.StringOrSlice `json:"require_labels,omitempty"`
	PreferLabels  map[string]types.StringOrSlice `json:"prefer_labels,omitempty"`
	InstanceGroup string                         `json:"instance_group,omitempty"`
}

// RankedController is a scored controller returned by the scheduler.
type RankedController struct {
	Domain   string  `json:"domain"`
	Name     string  `json:"name"`
	Score    float64 `json:"score"`
	Eligible bool    `json:"eligible"`
}

// EvaluateResponse is the response from the controller scheduler API.
type EvaluateResponse struct {
	Ranked   []RankedController `json:"ranked"`
	Strategy string             `json:"strategy"`
}

// SchedulerClient communicates with the controller-scheduler API.
type SchedulerClient struct {
	baseURL string
	apiKey  string
	client  *http.Client
}

func NewSchedulerClient(baseURL, apiKey string, tlsConfig *tls.Config) *SchedulerClient {
	return &SchedulerClient{
		baseURL: baseURL,
		apiKey:  apiKey,
		client: &http.Client{
			Timeout:   10 * time.Second,
			Transport: httputil.InstrumentedTransport(httputil.NewTransport(tlsConfig), metrics.SchedulerAPIDuration, "http"),
		},
	}
}

func (c *SchedulerClient) Evaluate(ctx context.Context, req EvaluateRequest) (*EvaluateResponse, error) {
	url := c.baseURL + "/api/v1/evaluate/controllers"
	headers := map[string]string{"X-API-Key": c.apiKey}

	var resp EvaluateResponse

	err := httputil.RetryWithContext(ctx, []time.Duration{3 * time.Second, 3 * time.Second}, func() error {
		status, err := httputil.DoJSON(ctx, c.client, http.MethodPost, url, headers, req, &resp)
		if err != nil {
			return fmt.Errorf("POST %s: %w", url, err)
		}
		if status != http.StatusOK {
			return fmt.Errorf("POST %s: status %d", url, status)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &resp, nil
}
