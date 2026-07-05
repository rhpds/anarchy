package runner

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"time"

	"github.com/rhpds/anarchy/babylon-runner/internal/clients"
	"github.com/rhpds/anarchy/babylon-runner/internal/metrics"
	"github.com/rhpds/anarchy/babylon-runner/internal/secrets"
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
	towerTLSConfig *tls.Config
	towerPool      *clients.TowerClientPool
	secretCache    *secrets.Cache
	ready             atomic.Bool
	consecutiveErrors atomic.Int32
}

// New creates a Runner with an HTTP client and AnarchyClient.
func New(cfg Config, clientset kubernetes.Interface, towerTLSConfig *tls.Config) *Runner {
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
		towerTLSConfig: towerTLSConfig,
		towerPool:      clients.NewTowerClientPool(),
	}
}

// SetHandlers replaces the handler map with the provided handlers.
func (r *Runner) SetHandlers(handlers map[string]HandlerFunc) {
	r.handlers = handlers
}

// IsReady reports whether the runner has successfully contacted the Anarchy API.
func (r *Runner) IsReady() bool { return r.ready.Load() }

// TowerPool returns the shared Tower client pool for token reuse.
func (r *Runner) TowerPool() *clients.TowerClientPool { return r.towerPool }

// SetSecretCache sets the Kubernetes secret informer cache.
func (r *Runner) SetSecretCache(c *secrets.Cache) { r.secretCache = c }

// Run starts the polling loop. It stops when the context is cancelled.
func (r *Runner) Run(ctx context.Context) {
	slog.Info("babylon-runner starting",
		"runner", r.config.RunnerName,
		"pod", r.config.PodName,
		"url", r.config.AnarchyURL)

	for {
		select {
		case <-ctx.Done():
			slog.Info("shutting down")
			return
		default:
		}
		if err := r.pollOnce(ctx); err != nil {
			select {
			case <-ctx.Done():
				slog.Info("shutting down")
				return
			case <-time.After(r.config.PollingInterval):
			}
		}
	}
}

// pollOnce performs a single poll cycle: fetch a run, dispatch it to
// the appropriate handler, and post the result back to Anarchy.
func (r *Runner) pollOnce(ctx context.Context) error {
	pollStart := time.Now()
	payload, err := r.getRun(ctx)
	metrics.PollDuration.Observe(time.Since(pollStart).Seconds())
	if err != nil {
		count := r.consecutiveErrors.Add(1)
		slog.Error("poll failed", "error", err, "consecutiveErrors", count)
		if count >= int32(r.config.MaxPollFailures) {
			r.ready.Store(false)
			slog.Warn("readiness lost due to consecutive poll failures",
				"count", count, "threshold", r.config.MaxPollFailures)
		}
		return err
	}
	r.consecutiveErrors.Store(0)
	if payload == nil {
		return nil
	}

	metrics.ActiveRun.Set(1)
	defer metrics.ActiveRun.Set(0)

	rc := &RunContext{
		Ctx:                  ctx,
		Payload:              *payload,
		AnarchyClient:        r.anarchy,
		Clientset:            r.clientset,
		DefaultSandboxAPIURL: r.config.SandboxAPIURL,
		TowerTLSConfig:       r.towerTLSConfig,
		TowerClientPool:      r.towerPool,
		SecretCache:          r.secretCache,
		ActionRetryIntervals: r.config.ActionRetryIntervals,
		TowerPollIntervals:   r.config.TowerPollIntervals,
		Result: types.RunResult{
			Status: "successful",
		},
	}

	subject := rc.SubjectName()
	handlerName := payload.Handler.Name
	if handlerName == "" && payload.Action != nil {
		handlerName = payload.Action.Spec.Action
	}
	slog.Info("dispatching run",
		"run", rc.RunName(),
		"subject", subject,
		"handler", payload.Handler.Type+":"+handlerName)

	handlerType := payload.Handler.Type
	actionName := handlerName
	runStart := time.Now()

	func() {
		defer func() {
			if rec := recover(); rec != nil {
				stack := string(debug.Stack())
				slog.Error("handler panicked",
					"run", rc.RunName(),
					"subject", subject,
					"panic", rec,
					"stack", stack)
				rc.Result.Status = "failed"
				rc.Result.StatusMessage = fmt.Sprintf("panic: %v", rec)
			}
		}()

		if err := Dispatch(rc, r.handlers); err != nil {
			slog.Error("handler failed", "run", rc.RunName(), "subject", subject, "error", err)
			rc.Result.Status = "failed"
			rc.Result.StatusMessage = err.Error()
		}
	}()

	metrics.RunDuration.WithLabelValues(handlerType, actionName).Observe(time.Since(runStart).Seconds())
	metrics.RunTotal.WithLabelValues(handlerType, actionName, rc.Result.Status).Inc()

	if err := r.postResult(ctx, rc.RunName(), rc.Result); err != nil {
		slog.Warn("post result rejected (will be re-dispatched)", "run", rc.RunName(), "subject", subject, "error", err)
	} else {
		slog.Info("run complete", "run", rc.RunName(), "subject", subject, "status", rc.Result.Status)
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
		r.ready.Store(true)
		var payload types.RunPayload
		if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
			return nil, fmt.Errorf("decode run payload: %w", err)
		}
		if payload.Run.Metadata.Name == "" {
			return nil, nil
		}
		return &payload, nil
	case http.StatusNoContent, http.StatusRequestTimeout:
		r.ready.Store(true)
		return nil, nil
	case http.StatusForbidden:
		return nil, fmt.Errorf("authentication failed (403)")
	default:
		return nil, fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}
}

// postResult posts the handler result back to Anarchy. It retries up to
// maxRetries times with increasing delays capped at 60s.
func (r *Runner) postResult(ctx context.Context, runName string, result types.RunResult) error {
	url := fmt.Sprintf("%s/run/%s", r.config.AnarchyURL, runName)
	maxRetries := 10
	baseDelay := r.postRetryDelay
	maxDelay := 60 * time.Second
	var lastErr error

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
		body, _ := json.Marshal(map[string]interface{}{"result": result})
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url,
			strings.NewReader(string(body)))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", r.config.AuthHeader())

		resp, err := r.client.Do(req)
		if err != nil {
			lastErr = err
			slog.Warn("POST result transport error", "run", runName, "attempt", attempt, "error", err)
			continue
		}
		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return nil
		}
		lastErr = fmt.Errorf("status %d: %s", resp.StatusCode, string(respBody))
		slog.Warn("POST result failed", "status", resp.StatusCode, "body", string(respBody), "run", runName)
		if resp.StatusCode >= 400 && resp.StatusCode < 500 {
			return fmt.Errorf("POST result rejected: %d %s", resp.StatusCode, string(respBody))
		}
	}
	return fmt.Errorf("POST result failed after %d retries: %v", maxRetries, lastErr)
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
