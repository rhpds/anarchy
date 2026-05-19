package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os/signal"
	"syscall"
	"time"
)

// HandlerFunc is the signature for all handler functions.
type HandlerFunc func(rc *RunContext) error

// RunContext holds per-run state and provides convenience methods
// for accessing payload data and calling the Anarchy API.
type RunContext struct {
	Payload     RunPayload
	Anarchy     *AnarchyClient
	SubjectName string
	RunName     string
	ActionName  string // e.g. "provision" -- empty for events

	// TowerBaseURL allows tests to override the tower URL.
	TowerBaseURL string
	// SandboxBaseURL allows tests to override the sandbox API URL.
	SandboxBaseURL string

	finished               bool
	finishActionDirective  *FinishActionDirective
	deleteSubjectDirective *DeleteSubjectDirective
}

// FinishAction marks the action as finished with the given state.
// The directive is included in the POST /run result for the operator to process.
func (rc *RunContext) FinishAction(state string) {
	rc.finished = true
	rc.finishActionDirective = &FinishActionDirective{State: state}
	slog.Info("action finished", "action", rc.ActionName, "state", state)
}

// DeleteSubject marks the subject for deletion by the operator.
func (rc *RunContext) DeleteSubject(removeFinalizers bool) {
	rc.deleteSubjectDirective = &DeleteSubjectDirective{RemoveFinalizers: removeFinalizers}
	slog.Info("subject marked for deletion", "subject", rc.SubjectName, "removeFinalizers", removeFinalizers)
}

// ContinueAction schedules the current action to run again after the
// specified duration string (e.g. "30s", "5m").
func (rc *RunContext) ContinueAction(after string) error {
	return rc.Anarchy.ScheduleAction(rc.SubjectName, ScheduleActionRequest{
		Action: rc.ActionName,
		After:  after,
	})
}

// ContinueActionWithVars schedules the current action with additional vars
// (e.g. action_retry_count).
func (rc *RunContext) ContinueActionWithVars(after string, vars map[string]interface{}) error {
	return rc.Anarchy.ScheduleAction(rc.SubjectName, ScheduleActionRequest{
		Action: rc.ActionName,
		After:  after,
		Vars:   vars,
	})
}

// SubjectUpdate delegates to AnarchyClient.SubjectUpdate for this subject.
func (rc *RunContext) SubjectUpdate(patch SubjectPatch) error {
	return rc.Anarchy.SubjectUpdate(rc.SubjectName, patch)
}

// ScheduleAction delegates to AnarchyClient.ScheduleAction for this subject.
func (rc *RunContext) ScheduleAction(req ScheduleActionRequest) error {
	return rc.Anarchy.ScheduleAction(rc.SubjectName, req)
}

// CurrentState returns subject.spec.vars.current_state.
func (rc *RunContext) CurrentState() string {
	return getNestedString(rc.Payload.Subject, "spec", "vars", "current_state")
}

// DesiredState returns subject.spec.vars.desired_state.
func (rc *RunContext) DesiredState() string {
	return getNestedString(rc.Payload.Subject, "spec", "vars", "desired_state")
}

// JobVars returns subject.spec.vars.job_vars.
func (rc *RunContext) JobVars() map[string]interface{} {
	return getNestedMap(rc.Payload.Subject, "spec", "vars", "job_vars")
}

// GovernorJobVars returns governor.spec.vars.job_vars.
func (rc *RunContext) GovernorJobVars() map[string]interface{} {
	return getNestedMap(rc.Payload.Governor, "spec", "vars", "job_vars")
}

// Meta returns __meta__ from governor.spec.vars.job_vars.
// The Ansible runner flattens governor.spec.vars into top-level vars,
// so __meta__ is accessed as job_vars.__meta__ in Ansible. In the raw
// payload from the operator, it lives at spec.vars.job_vars.__meta__.
func (rc *RunContext) Meta() map[string]interface{} {
	return getNestedMap(rc.Payload.Governor, "spec", "vars", "job_vars", "__meta__")
}

// SandboxAPIInUse returns true if meta.aws_sandboxed is true or
// meta.sandboxes has at least one element.
func (rc *RunContext) SandboxAPIInUse() bool {
	meta := rc.Meta()
	if meta == nil {
		return false
	}
	if getNestedBool(meta, "aws_sandboxed") {
		return true
	}
	sandboxes, ok := meta["sandboxes"]
	if !ok {
		return false
	}
	list, ok := sandboxes.([]interface{})
	if !ok {
		return false
	}
	return len(list) > 0
}

// DeployerDisabled returns true if __meta__.deployer.actions.{action}.disable
// is truthy (matching Ansible's check).
func (rc *RunContext) DeployerDisabled(action string) bool {
	meta := rc.Meta()
	if meta == nil {
		return false
	}
	deployer := getNestedMap(meta, "deployer")
	if deployer == nil {
		return false
	}
	actionCfg := getNestedMap(deployer, "actions", action)
	if actionCfg == nil {
		return false
	}
	disable, ok := actionCfg["disable"].(bool)
	return ok && disable
}

// UUID returns job_vars.uuid from the subject.
func (rc *RunContext) UUID() string {
	return getNestedString(rc.Payload.Subject, "spec", "vars", "job_vars", "uuid")
}

// GUID returns job_vars.guid from the subject.
func (rc *RunContext) GUID() string {
	return getNestedString(rc.Payload.Subject, "spec", "vars", "job_vars", "guid")
}

// StatusActions returns subject.status.actions.
func (rc *RunContext) StatusActions() map[string]interface{} {
	return getNestedMap(rc.Payload.Subject, "status", "actions")
}

// StatusTowerJobs returns subject.status.towerJobs.
func (rc *RunContext) StatusTowerJobs() map[string]interface{} {
	return getNestedMap(rc.Payload.Subject, "status", "towerJobs")
}

// ActionRetryCount returns action_retry_count from the action's spec.vars (default 0).
func (rc *RunContext) ActionRetryCount() int {
	if rc.Payload.Action == nil {
		return 0
	}
	v := getNestedString(rc.Payload.Action, "spec", "vars", "action_retry_count")
	if v != "" {
		// Handle string representation.
		n := 0
		for _, c := range v {
			if c >= '0' && c <= '9' {
				n = n*10 + int(c-'0')
			}
		}
		return n
	}
	// Try float64 (JSON numbers).
	vars := getNestedMap(rc.Payload.Action, "spec", "vars")
	if vars != nil {
		if f, ok := vars["action_retry_count"].(float64); ok {
			return int(f)
		}
		if i, ok := vars["action_retry_count"].(int); ok {
			return i
		}
	}
	return 0
}

// IsBeingDeleted returns true if the subject has a deletionTimestamp.
func (rc *RunContext) IsBeingDeleted() bool {
	ts := getNestedString(rc.Payload.Subject, "metadata", "deletionTimestamp")
	return ts != ""
}

// GovernorActions returns governor.spec.actions.
func (rc *RunContext) GovernorActions() map[string]interface{} {
	return getNestedMap(rc.Payload.Governor, "spec", "actions")
}

// Runner implements the main polling loop that fetches runs from the
// Anarchy API, dispatches them to handlers, and posts results.
type Runner struct {
	cfg              Config
	client           *http.Client
	anarchy          *AnarchyClient
	handlers         map[string]HandlerFunc
	postMaxAttempts  int
	postRetryDelay   time.Duration
}

// NewRunner creates a Runner with an HTTP client and AnarchyClient.
func NewRunner(cfg Config) *Runner {
	return &Runner{
		cfg:             cfg,
		client:          &http.Client{Timeout: time.Duration(cfg.RequestTimeout) * time.Second},
		anarchy:         NewAnarchyClient(cfg),
		handlers:        make(map[string]HandlerFunc),
		postMaxAttempts: 10,
		postRetryDelay:  5 * time.Second,
	}
}

// Run starts the polling loop. It stops on SIGTERM or SIGINT.
func (r *Runner) Run() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	ticker := time.NewTicker(time.Duration(r.cfg.PollingInterval) * time.Second)
	defer ticker.Stop()

	slog.Info("runner polling loop started")

	// Run an initial poll immediately before waiting for the ticker.
	if err := r.pollOnce(ctx); err != nil && ctx.Err() == nil {
		slog.Error("poll error", "error", err)
	}

	for {
		select {
		case <-ctx.Done():
			slog.Info("received signal, shutting down")
			return
		case <-ticker.C:
			if err := r.pollOnce(ctx); err != nil && ctx.Err() == nil {
				slog.Error("poll error", "error", err)
			}
		}
	}
}

// pollOnce performs a single poll cycle: fetch a run, dispatch it to
// the appropriate handler, and post the result back to Anarchy.
func (r *Runner) pollOnce(ctx context.Context) error {
	payload, err := r.getRun(ctx)
	if err != nil {
		return fmt.Errorf("getRun: %w", err)
	}
	if payload == nil {
		return nil // no run available
	}

	subjectName := getNestedString(payload.Subject, "metadata", "name")
	runName := getNestedString(payload.Run, "metadata", "name")
	actionName := getNestedString(payload.Action, "spec", "action")

	slog.Info("received run",
		"run", runName, "subject", subjectName, "handlerType", payload.Handler.Type,
		"handlerName", payload.Handler.Name, "action", actionName)

	rc := &RunContext{
		Payload:     *payload,
		Anarchy:     r.anarchy,
		SubjectName: subjectName,
		RunName:     runName,
		ActionName:  actionName,
	}

	result := RunResult{
		Result: ResultPayload{
			RC:     0,
			Status: "successful",
		},
	}

	if err := dispatch(rc, r.handlers); err != nil {
		slog.Error("handler error", "run", runName, "error", err)
		result.Result.RC = 1
		result.Result.Status = "failed"
		result.Result.StatusMessage = err.Error()
	}

	// Include handler directives in the result.
	result.FinishAction = rc.finishActionDirective
	result.DeleteSubject = rc.deleteSubjectDirective

	if err := r.postResult(runName, result); err != nil {
		return fmt.Errorf("postResult run=%s: %w", runName, err)
	}

	return nil
}

// getRun fetches the next available run from the Anarchy API.
// Returns nil,nil when there is no run available (timeout or 204).
// Returns an error on 403 (authentication failure) or transport errors.
func (r *Runner) getRun(ctx context.Context) (*RunPayload, error) {
	url := fmt.Sprintf("%s/run", r.cfg.AnarchyURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Authorization", r.cfg.AuthHeader())

	resp, err := r.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", url, err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		// Decode the run payload.
	case http.StatusNoContent, http.StatusRequestTimeout:
		return nil, nil
	case http.StatusForbidden:
		return nil, fmt.Errorf("GET %s: authentication failed (403)", url)
	default:
		return nil, fmt.Errorf("GET %s: unexpected status %d", url, resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response body: %w", err)
	}

	// Empty body or JSON null means no run available.
	if len(body) == 0 || string(body) == "null" {
		return nil, nil
	}

	var payload RunPayload
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	// Sanity check: the operator returned 200 but the payload has no
	// handler type — treat it as no run available.
	if payload.Handler.Type == "" {
		return nil, nil
	}

	return &payload, nil
}

// postResult posts the handler result back to Anarchy. It retries up to
// 10 times with exponential backoff.
func (r *Runner) postResult(runName string, result RunResult) error {
	url := fmt.Sprintf("%s/run/%s", r.cfg.AnarchyURL, runName)
	data, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("marshal result: %w", err)
	}

	maxAttempts := r.postMaxAttempts
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			delay := time.Duration(attempt) * r.postRetryDelay
			if delay > 60*time.Second {
				delay = 60 * time.Second
			}
			slog.Warn("retrying POST", "url", url, "delay", delay, "attempt", attempt+1, "maxAttempts", maxAttempts)
			time.Sleep(delay)
		}

		req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(data))
		if err != nil {
			return fmt.Errorf("create request: %w", err)
		}
		req.Header.Set("Authorization", r.cfg.AuthHeader())
		req.Header.Set("Content-Type", "application/json")

		resp, err := r.client.Do(req)
		if err != nil {
			slog.Error("POST attempt failed", "url", url, "attempt", attempt+1, "error", err)
			continue
		}
		resp.Body.Close()

		if resp.StatusCode == http.StatusOK {
			return nil
		}
		slog.Warn("POST unexpected status", "url", url, "attempt", attempt+1, "status", resp.StatusCode)
	}

	return fmt.Errorf("POST %s: all %d attempts failed", url, maxAttempts)
}

// dispatch routes the run to the appropriate handler based on the handler
// type and action name.
func dispatch(rc *RunContext, handlers map[string]HandlerFunc) error {
	var key string

	switch rc.Payload.Handler.Type {
	case "subjectEvent":
		key = "event:" + rc.Payload.Handler.Name
	case "action":
		key = "action:" + rc.ActionName
	case "actionCallback":
		key = "action:" + rc.ActionName + ":" + rc.Payload.Handler.Name
	default:
		return fmt.Errorf("unknown handler type: %q", rc.Payload.Handler.Type)
	}

	handler, ok := handlers[key]
	if !ok {
		return fmt.Errorf("no handler registered for %q", key)
	}

	return handler(rc)
}
