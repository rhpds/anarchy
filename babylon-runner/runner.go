package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
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
	log.Printf("action %s finished with state %q", rc.ActionName, state)
}

// DeleteSubject marks the subject for deletion by the operator.
func (rc *RunContext) DeleteSubject(removeFinalizers bool) {
	rc.deleteSubjectDirective = &DeleteSubjectDirective{RemoveFinalizers: removeFinalizers}
	log.Printf("subject %s marked for deletion (removeFinalizers=%v)", rc.SubjectName, removeFinalizers)
}

// ContinueAction schedules the current action to run again after the
// specified duration string (e.g. "30s", "5m").
func (rc *RunContext) ContinueAction(after string) error {
	return rc.Anarchy.ScheduleAction(rc.SubjectName, ScheduleActionRequest{
		Action: rc.ActionName,
		After:  after,
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

// Meta returns __meta__ from governor.spec.vars.
func (rc *RunContext) Meta() map[string]interface{} {
	return getNestedMap(rc.Payload.Governor, "spec", "vars", "__meta__")
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

// DeployerDisabled returns true if meta.deployer.entry_points.{action}
// equals "disabled" or "none".
func (rc *RunContext) DeployerDisabled(action string) bool {
	meta := rc.Meta()
	if meta == nil {
		return false
	}
	val := getNestedString(meta, "deployer", "entry_points", action)
	return val == "disabled" || val == "none"
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
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

	ticker := time.NewTicker(time.Duration(r.cfg.PollingInterval) * time.Second)
	defer ticker.Stop()

	log.Println("runner polling loop started")

	// Run an initial poll immediately before waiting for the ticker.
	if err := r.pollOnce(); err != nil {
		log.Printf("poll error: %v", err)
	}

	for {
		select {
		case sig := <-sigCh:
			log.Printf("received signal %v, shutting down", sig)
			return
		case <-ticker.C:
			if err := r.pollOnce(); err != nil {
				log.Printf("poll error: %v", err)
			}
		}
	}
}

// pollOnce performs a single poll cycle: fetch a run, dispatch it to
// the appropriate handler, and post the result back to Anarchy.
func (r *Runner) pollOnce() error {
	payload, err := r.getRun()
	if err != nil {
		return fmt.Errorf("getRun: %w", err)
	}
	if payload == nil {
		return nil // no run available
	}

	subjectName := getNestedString(payload.Subject, "metadata", "name")
	runName := getNestedString(payload.Run, "metadata", "name")
	actionName := getNestedString(payload.Action, "spec", "action")

	log.Printf("received run=%s subject=%s handler=%s/%s action=%s",
		runName, subjectName, payload.Handler.Type, payload.Handler.Name, actionName)

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
		log.Printf("handler error for run=%s: %v", runName, err)
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
func (r *Runner) getRun() (*RunPayload, error) {
	url := fmt.Sprintf("%s/run", r.cfg.AnarchyURL)
	req, err := http.NewRequest(http.MethodGet, url, nil)
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

	// Empty body means no run available.
	if len(body) == 0 {
		return nil, nil
	}

	var payload RunPayload
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
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
			log.Printf("retrying POST %s in %v (attempt %d/%d)", url, delay, attempt+1, maxAttempts)
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
			log.Printf("POST %s attempt %d failed: %v", url, attempt+1, err)
			continue
		}
		resp.Body.Close()

		if resp.StatusCode == http.StatusOK {
			return nil
		}
		log.Printf("POST %s attempt %d: status %d", url, attempt+1, resp.StatusCode)
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
