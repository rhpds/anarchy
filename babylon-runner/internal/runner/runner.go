package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/rhpds/anarchy/babylon-runner/internal/clients"
	"github.com/rhpds/anarchy/babylon-runner/internal/types"
	"k8s.io/client-go/kubernetes"
)

// HandlerFunc is the signature for all run handlers.
type HandlerFunc func(rc *RunContext) error

// Runner implements the main polling loop that fetches runs from the
// Anarchy API, dispatches them to handlers, and posts results.
type Runner struct {
	config         Config
	client         *http.Client
	anarchy        *clients.AnarchyClient
	clientset      kubernetes.Interface
	handlers       map[string]HandlerFunc
	postRetryDelay time.Duration // base delay between POST retries; 0 in tests
}

// New creates a Runner with an HTTP client and AnarchyClient.
func New(cfg Config, clientset kubernetes.Interface) *Runner {
	anarchyCfg := clients.AnarchyClientConfig{
		BaseURL:    cfg.AnarchyURL,
		AuthHeader: cfg.AuthHeader(),
		Timeout:    cfg.RequestTimeout,
	}
	return &Runner{
		config:         cfg,
		client:         &http.Client{Timeout: cfg.RequestTimeout},
		anarchy:        clients.NewAnarchyClient(anarchyCfg),
		clientset:      clientset,
		handlers:       make(map[string]HandlerFunc),
		postRetryDelay: 5 * time.Second,
	}
}

// SetHandlers replaces the handler map with the provided handlers.
func (r *Runner) SetHandlers(handlers map[string]HandlerFunc) {
	r.handlers = handlers
}

// Run starts the polling loop. It stops on SIGTERM or SIGINT.
func (r *Runner) Run() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	slog.Info("babylon-runner starting",
		"runner", r.config.RunnerName,
		"pod", r.config.PodName,
		"url", r.config.AnarchyURL)

	ticker := time.NewTicker(r.config.PollingInterval)
	defer ticker.Stop()

	r.pollOnce(ctx)
	for {
		select {
		case <-ctx.Done():
			slog.Info("shutting down")
			return
		case <-ticker.C:
			r.pollOnce(ctx)
		}
	}
}

// pollOnce performs a single poll cycle: fetch a run, dispatch it to
// the appropriate handler, and post the result back to Anarchy.
func (r *Runner) pollOnce(ctx context.Context) error {
	payload, err := r.getRun(ctx)
	if err != nil {
		slog.Error("poll failed", "error", err)
		return err
	}
	if payload == nil {
		return nil
	}

	rc := &RunContext{
		Payload:       *payload,
		AnarchyClient: r.anarchy,
		Clientset:     r.clientset,
		Result: types.RunResult{
			Status: "successful",
		},
	}

	slog.Info("dispatching run",
		"run", rc.RunName(),
		"handler", payload.Handler.Type+":"+payload.Handler.Name)

	if err := Dispatch(rc, r.handlers); err != nil {
		slog.Error("handler failed", "run", rc.RunName(), "error", err)
		rc.Result.Status = "failed"
		rc.Result.StatusMessage = err.Error()
	}

	if err := r.postResult(ctx, rc.RunName(), rc.Result); err != nil {
		slog.Error("post result failed", "run", rc.RunName(), "error", err)
	}
	return nil
}

// getRun fetches the next available run from the Anarchy API.
// Returns nil,nil when there is no run available (timeout or 204).
func (r *Runner) getRun(ctx context.Context) (*types.RunPayload, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		r.config.AnarchyURL+"/run", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", r.config.AuthHeader())

	resp, err := r.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		var payload types.RunPayload
		if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
			return nil, fmt.Errorf("decode run payload: %w", err)
		}
		return &payload, nil
	case http.StatusNoContent, http.StatusRequestTimeout:
		return nil, nil
	case http.StatusForbidden:
		return nil, fmt.Errorf("authentication failed (403)")
	default:
		return nil, fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}
}

// postResult posts the handler result back to Anarchy. It retries up to
// 10 times with increasing delays capped at 60s.
func (r *Runner) postResult(ctx context.Context, runName string, result types.RunResult) error {
	url := fmt.Sprintf("%s/run/%s", r.config.AnarchyURL, runName)
	maxRetries := 10
	baseDelay := r.postRetryDelay
	maxDelay := 60 * time.Second

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 && baseDelay > 0 {
			delay := baseDelay * time.Duration(attempt)
			if delay > maxDelay {
				delay = maxDelay
			}
			slog.Warn("retrying POST result", "attempt", attempt, "delay", delay)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
		}
		body, _ := json.Marshal(result)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url,
			strings.NewReader(string(body)))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", r.config.AuthHeader())

		resp, err := r.client.Do(req)
		if err != nil {
			continue
		}
		resp.Body.Close()
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return nil
		}
		slog.Warn("POST result failed", "status", resp.StatusCode)
	}
	return fmt.Errorf("POST result failed after %d retries", maxRetries)
}

// Dispatch routes a run to the appropriate handler based on handler type and name.
func Dispatch(rc *RunContext, handlers map[string]HandlerFunc) error {
	var key string
	switch rc.Payload.Handler.Type {
	case "subjectEvent":
		key = "event:" + rc.Payload.Handler.Name
	case "action":
		key = "action:" + rc.ActionName()
	case "actionCallback":
		key = "action:" + rc.ActionName() + ":" + rc.Payload.Handler.Name
	default:
		return fmt.Errorf("unknown handler type: %s", rc.Payload.Handler.Type)
	}

	handler, ok := handlers[key]
	if !ok {
		return fmt.Errorf("no handler registered for %s", key)
	}
	return handler(rc)
}
