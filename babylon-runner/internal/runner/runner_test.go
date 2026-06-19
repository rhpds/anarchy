package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rhpds/anarchy/babylon-runner/internal/clients"
	"github.com/rhpds/anarchy/babylon-runner/internal/types"
)

// --- Dispatch tests ---

func TestDispatchEvent(t *testing.T) {
	var called bool
	handlers := map[string]HandlerFunc{
		"event:create": func(rc *RunContext) error {
			called = true
			return nil
		},
	}

	rc := &RunContext{
		Ctx: context.Background(),
		Payload: types.RunPayload{
			Handler: types.Handler{
				Type: "subjectEvent",
				Name: "create",
			},
		},
	}

	if err := Dispatch(rc, handlers); err != nil {
		t.Fatalf("Dispatch returned error: %v", err)
	}
	if !called {
		t.Error("expected event:create handler to be called")
	}
}

func TestDispatchAction(t *testing.T) {
	var called bool
	handlers := map[string]HandlerFunc{
		"action:provision": func(rc *RunContext) error {
			called = true
			return nil
		},
	}

	rc := &RunContext{
		Ctx: context.Background(),
		Payload: types.RunPayload{
			Handler: types.Handler{
				Type: "action",
				Name: "run",
			},
			Action: &types.Action{
				Spec: types.ActionSpec{Action: "provision"},
			},
		},
	}

	if err := Dispatch(rc, handlers); err != nil {
		t.Fatalf("Dispatch returned error: %v", err)
	}
	if !called {
		t.Error("expected action:provision handler to be called")
	}
}

func TestDispatchActionCallback(t *testing.T) {
	var called bool
	handlers := map[string]HandlerFunc{
		"action:provision:complete": func(rc *RunContext) error {
			called = true
			return nil
		},
	}

	rc := &RunContext{
		Ctx: context.Background(),
		Payload: types.RunPayload{
			Handler: types.Handler{
				Type: "actionCallback",
				Name: "complete",
			},
			Action: &types.Action{
				Spec: types.ActionSpec{Action: "provision"},
			},
		},
	}

	if err := Dispatch(rc, handlers); err != nil {
		t.Fatalf("Dispatch returned error: %v", err)
	}
	if !called {
		t.Error("expected action:provision:complete handler to be called")
	}
}

func TestDispatchUnknownType(t *testing.T) {
	handlers := map[string]HandlerFunc{}
	rc := &RunContext{
		Ctx: context.Background(),
		Payload: types.RunPayload{
			Handler: types.Handler{
				Type: "unknownType",
				Name: "test",
			},
		},
	}

	err := Dispatch(rc, handlers)
	if err == nil {
		t.Fatal("expected error for unknown handler type, got nil")
	}
	if !strings.Contains(err.Error(), "unknown handler type") {
		t.Errorf("error = %q, want substring %q", err.Error(), "unknown handler type")
	}
}

func TestDispatchUnregisteredHandler(t *testing.T) {
	handlers := map[string]HandlerFunc{}
	rc := &RunContext{
		Ctx: context.Background(),
		Payload: types.RunPayload{
			Handler: types.Handler{
				Type: "subjectEvent",
				Name: "unknown",
			},
		},
	}

	err := Dispatch(rc, handlers)
	if err == nil {
		t.Fatal("expected error for unregistered handler, got nil")
	}
	if !strings.Contains(err.Error(), "no handler registered") {
		t.Errorf("error = %q, want substring %q", err.Error(), "no handler registered")
	}
}

// --- Polling tests ---

func TestPollAndPostSuccess(t *testing.T) {
	var postCalled atomic.Int32
	var postBody struct {
		Result types.RunResult `json:"result"`
	}

	runName := "test-run-abc"
	payload := types.RunPayload{
		Handler: types.Handler{
			Type: "subjectEvent",
			Name: "create",
		},
		Subject: types.Subject{
			Metadata: types.ObjectMeta{Name: "test-subject"},
		},
		Run: types.Run{
			Metadata: types.ObjectMeta{Name: runName},
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/run":
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(payload)
		case r.Method == http.MethodPost && r.URL.Path == fmt.Sprintf("/run/%s", runName):
			postCalled.Add(1)
			json.NewDecoder(r.Body).Decode(&postBody)
			w.WriteHeader(http.StatusOK)
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	cfg := Config{
		AnarchyURL:      server.URL,
		RunnerName:      "runner",
		PodName:         "pod",
		RunnerToken:     "token",
		PollingInterval: 5 * time.Second,
		RequestTimeout:  5 * time.Second,
	}

	runner := New(cfg, nil, nil)
	runner.handlers["event:create"] = func(rc *RunContext) error {
		return nil
	}

	err := runner.pollOnce(context.Background())
	if err != nil {
		t.Fatalf("pollOnce returned error: %v", err)
	}

	if postCalled.Load() != 1 {
		t.Errorf("POST /run/%s call count = %d, want 1", runName, postCalled.Load())
	}
	if postBody.Result.Status != "successful" {
		t.Errorf("result status = %q, want %q", postBody.Result.Status, "successful")
	}
}

func TestPollAndPostFailedHandler(t *testing.T) {
	var postBody struct {
		Result types.RunResult `json:"result"`
	}
	runName := "test-run-fail"

	payload := types.RunPayload{
		Handler: types.Handler{
			Type: "subjectEvent",
			Name: "create",
		},
		Subject: types.Subject{
			Metadata: types.ObjectMeta{Name: "test-subject"},
		},
		Run: types.Run{
			Metadata: types.ObjectMeta{Name: runName},
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/run":
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(payload)
		case r.Method == http.MethodPost:
			json.NewDecoder(r.Body).Decode(&postBody)
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	cfg := Config{
		AnarchyURL:      server.URL,
		RunnerName:      "runner",
		PodName:         "pod",
		RunnerToken:     "token",
		PollingInterval: 5 * time.Second,
		RequestTimeout:  5 * time.Second,
	}

	runner := New(cfg, nil, nil)
	runner.handlers["event:create"] = func(rc *RunContext) error {
		return fmt.Errorf("handler failed")
	}

	err := runner.pollOnce(context.Background())
	if err != nil {
		t.Fatalf("pollOnce returned error: %v", err)
	}

	if postBody.Result.Status != "failed" {
		t.Errorf("result status = %q, want %q", postBody.Result.Status, "failed")
	}
}

func TestPollOnceIncludesDirectives(t *testing.T) {
	var postBody struct {
		Result types.RunResult `json:"result"`
	}
	runName := "test-run-directives"

	payload := types.RunPayload{
		Handler: types.Handler{
			Type: "action",
			Name: "run",
		},
		Subject: types.Subject{
			Metadata: types.ObjectMeta{Name: "test-subject"},
		},
		Run: types.Run{
			Metadata: types.ObjectMeta{Name: runName},
		},
		Action: &types.Action{
			Spec: types.ActionSpec{Action: "provision"},
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/run":
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(payload)
		case r.Method == http.MethodPost && r.URL.Path == fmt.Sprintf("/run/%s", runName):
			json.NewDecoder(r.Body).Decode(&postBody)
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	cfg := Config{
		AnarchyURL:      server.URL,
		RunnerName:      "runner",
		PodName:         "pod",
		RunnerToken:     "token",
		PollingInterval: 5 * time.Second,
		RequestTimeout:  5 * time.Second,
	}

	runner := New(cfg, nil, nil)
	runner.handlers["action:provision"] = func(rc *RunContext) error {
		rc.FinishAction("successful")
		rc.DeleteSubject(true)
		return nil
	}

	err := runner.pollOnce(context.Background())
	if err != nil {
		t.Fatalf("pollOnce returned error: %v", err)
	}

	if postBody.Result.FinishAction == nil {
		t.Fatal("FinishAction directive should not be nil")
	}
	if postBody.Result.FinishAction.State != "successful" {
		t.Errorf("FinishAction.State = %q, want %q", postBody.Result.FinishAction.State, "successful")
	}
	if postBody.Result.DeleteSubject == nil {
		t.Fatal("DeleteSubject directive should not be nil")
	}
	if postBody.Result.DeleteSubject.RemoveFinalizers != true {
		t.Errorf("DeleteSubject.RemoveFinalizers = %v, want true", postBody.Result.DeleteSubject.RemoveFinalizers)
	}
}

// --- getRun tests ---

func TestGetRunNoContent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	r := newTestRunner(server.URL)

	payload, err := r.getRun(context.Background())
	if err != nil {
		t.Fatalf("getRun returned error: %v", err)
	}
	if payload != nil {
		t.Errorf("getRun() = %v, want nil", payload)
	}
}

func TestGetRunNullBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("null"))
	}))
	defer server.Close()

	r := newTestRunner(server.URL)

	payload, err := r.getRun(context.Background())
	if err != nil {
		t.Fatalf("getRun returned error: %v", err)
	}
	if payload != nil {
		t.Errorf("getRun() = %v, want nil for null body", payload)
	}
}

func TestGetRunEmptyObject(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("{}"))
	}))
	defer server.Close()

	r := newTestRunner(server.URL)

	payload, err := r.getRun(context.Background())
	if err != nil {
		t.Fatalf("getRun returned error: %v", err)
	}
	if payload != nil {
		t.Errorf("getRun() = %v, want nil for empty object", payload)
	}
}

func TestGetRunTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusRequestTimeout)
	}))
	defer server.Close()

	r := newTestRunner(server.URL)

	payload, err := r.getRun(context.Background())
	if err != nil {
		t.Fatalf("getRun returned error: %v", err)
	}
	if payload != nil {
		t.Errorf("getRun() = %v, want nil", payload)
	}
}

func TestGetRunForbidden(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer server.Close()

	r := newTestRunner(server.URL)

	payload, err := r.getRun(context.Background())
	if err == nil {
		t.Fatal("expected error for 403, got nil")
	}
	if payload != nil {
		t.Errorf("payload should be nil on error, got %v", payload)
	}
	if !strings.Contains(err.Error(), "authentication failed") {
		t.Errorf("error = %q, want substring %q", err.Error(), "authentication failed")
	}
}

func TestGetRunUnexpectedStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	r := newTestRunner(server.URL)

	payload, err := r.getRun(context.Background())
	if err == nil {
		t.Fatal("expected error for 500, got nil")
	}
	if payload != nil {
		t.Errorf("payload should be nil on error, got %v", payload)
	}
}

// --- postResult tests ---

func TestPostResultSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/run/test-run" {
			t.Errorf("path = %s, want /run/test-run", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	r := newTestRunner(server.URL)

	result := types.RunResult{
		Status: "successful",
	}

	err := r.postResult(context.Background(), "test-run", result)
	if err != nil {
		t.Fatalf("postResult returned error: %v", err)
	}
}

func TestPostResultRetryThenSuccess(t *testing.T) {
	var attempts atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count := attempts.Add(1)
		if count == 1 {
			w.WriteHeader(http.StatusInternalServerError)
		} else {
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()

	r := newTestRunner(server.URL)

	result := types.RunResult{
		Status: "successful",
	}

	err := r.postResult(context.Background(), "test-run", result)
	if err != nil {
		t.Fatalf("postResult returned error: %v", err)
	}
	if attempts.Load() < 2 {
		t.Errorf("expected at least 2 attempts, got %d", attempts.Load())
	}
}

// --- RunContext convenience method tests ---

func TestRunContext_ConvenienceMethods(t *testing.T) {
	rc := &RunContext{
		Ctx: context.Background(),
		Payload: types.RunPayload{
			Handler: types.Handler{Type: "action", Name: "provision"},
			Governor: types.Governor{
				Metadata: types.ObjectMeta{Name: "test-gov"},
				Spec: types.GovernorSpec{
					Vars: types.GovernorVars{
						JobVars: map[string]interface{}{"region": "us-east-1"},
						Meta: &types.Meta{
							AWSSandboxed: true,
							Deployer: &types.DeployerMeta{
								Actions: map[string]types.DeployerActionConfig{
									"status": {Disabled: true},
								},
							},
						},
					},
				},
			},
			Subject: types.Subject{
				Metadata: types.ObjectMeta{Name: "test-subj"},
				Spec: types.SubjectSpec{
					Vars: types.SubjectVars{
						CurrentState: "started",
						DesiredState: "stopped",
						JobVars:      map[string]interface{}{"uuid": "abc-123", "guid": "xyz-456"},
					},
				},
			},
			Action: &types.Action{
				Metadata: types.ObjectMeta{Name: "test-action"},
				Spec:     types.ActionSpec{Action: "provision"},
			},
			Run: types.Run{Metadata: types.ObjectMeta{Name: "test-run"}},
		},
	}

	if rc.SubjectName() != "test-subj" {
		t.Errorf("SubjectName = %q", rc.SubjectName())
	}
	if rc.RunName() != "test-run" {
		t.Errorf("RunName = %q", rc.RunName())
	}
	if rc.CurrentState() != "started" {
		t.Errorf("CurrentState = %q", rc.CurrentState())
	}
	if rc.DesiredState() != "stopped" {
		t.Errorf("DesiredState = %q", rc.DesiredState())
	}
	if rc.ActionName() != "provision" {
		t.Errorf("ActionName = %q", rc.ActionName())
	}
	if rc.UUID() != "abc-123" {
		t.Errorf("UUID = %q", rc.UUID())
	}
	if rc.GUID() != "xyz-456" {
		t.Errorf("GUID = %q", rc.GUID())
	}
	if !rc.SandboxAPIInUse() {
		t.Error("SandboxAPIInUse = false")
	}
	if !rc.DeployerDisabled("status") {
		t.Error("DeployerDisabled(status) = false")
	}
	if rc.DeployerDisabled("provision") {
		t.Error("DeployerDisabled(provision) = true")
	}
}

func TestRunContext_CurrentState(t *testing.T) {
	rc := &RunContext{
		Ctx: context.Background(),
		Payload: types.RunPayload{
			Subject: types.Subject{
				Spec: types.SubjectSpec{
					Vars: types.SubjectVars{CurrentState: "started"},
				},
			},
		},
	}
	if got := rc.CurrentState(); got != "started" {
		t.Errorf("CurrentState() = %q, want %q", got, "started")
	}
}

func TestRunContext_DesiredState(t *testing.T) {
	rc := &RunContext{
		Ctx: context.Background(),
		Payload: types.RunPayload{
			Subject: types.Subject{
				Spec: types.SubjectSpec{
					Vars: types.SubjectVars{DesiredState: "stopped"},
				},
			},
		},
	}
	if got := rc.DesiredState(); got != "stopped" {
		t.Errorf("DesiredState() = %q, want %q", got, "stopped")
	}
}

func TestRunContext_CheckStatusState(t *testing.T) {
	rc := &RunContext{
		Ctx: context.Background(),
		Payload: types.RunPayload{
			Subject: types.Subject{
				Spec: types.SubjectSpec{
					Vars: types.SubjectVars{CheckStatusState: "check-pending"},
				},
			},
		},
	}
	if got := rc.CheckStatusState(); got != "check-pending" {
		t.Errorf("CheckStatusState() = %q, want %q", got, "check-pending")
	}
}

func TestRunContext_JobVars(t *testing.T) {
	rc := &RunContext{
		Ctx: context.Background(),
		Payload: types.RunPayload{
			Subject: types.Subject{
				Spec: types.SubjectSpec{
					Vars: types.SubjectVars{
						JobVars: map[string]interface{}{
							"uuid": "abc-123",
							"guid": "xyz-789",
						},
					},
				},
			},
		},
	}
	jv := rc.JobVars()
	if jv == nil {
		t.Fatal("JobVars() returned nil")
	}
	if jv["uuid"] != "abc-123" {
		t.Errorf("JobVars()[uuid] = %v, want %q", jv["uuid"], "abc-123")
	}
}

func TestRunContext_GovernorJobVars(t *testing.T) {
	rc := &RunContext{
		Ctx: context.Background(),
		Payload: types.RunPayload{
			Governor: types.Governor{
				Spec: types.GovernorSpec{
					Vars: types.GovernorVars{
						JobVars: map[string]interface{}{
							"cloud": "aws",
						},
					},
				},
			},
		},
	}
	gjv := rc.GovernorJobVars()
	if gjv == nil {
		t.Fatal("GovernorJobVars() returned nil")
	}
	if gjv["cloud"] != "aws" {
		t.Errorf("GovernorJobVars()[cloud] = %v, want %q", gjv["cloud"], "aws")
	}
}

func TestRunContext_Meta(t *testing.T) {
	rc := &RunContext{
		Ctx: context.Background(),
		Payload: types.RunPayload{
			Governor: types.Governor{
				Spec: types.GovernorSpec{
					Vars: types.GovernorVars{
						Meta: &types.Meta{
							AWSSandboxed: true,
						},
					},
				},
			},
		},
	}
	m := rc.Meta()
	if m == nil {
		t.Fatal("Meta() returned nil")
	}
	if !m.AWSSandboxed {
		t.Error("Meta().AWSSandboxed = false, want true")
	}
}

func TestRunContext_MetaNilReturnsEmpty(t *testing.T) {
	rc := &RunContext{
		Ctx: context.Background(),
		Payload: types.RunPayload{
			Governor: types.Governor{
				Spec: types.GovernorSpec{
					Vars: types.GovernorVars{},
				},
			},
		},
	}
	m := rc.Meta()
	if m == nil {
		t.Fatal("Meta() returned nil, expected empty Meta")
	}
	if m.AWSSandboxed {
		t.Error("empty Meta().AWSSandboxed = true, want false")
	}
}

func TestRunContext_UUID(t *testing.T) {
	rc := &RunContext{
		Ctx: context.Background(),
		Payload: types.RunPayload{
			Subject: types.Subject{
				Spec: types.SubjectSpec{
					Vars: types.SubjectVars{
						JobVars: map[string]interface{}{"uuid": "abc-123"},
					},
				},
			},
		},
	}
	if got := rc.UUID(); got != "abc-123" {
		t.Errorf("UUID() = %q, want %q", got, "abc-123")
	}
}

func TestRunContext_GUID(t *testing.T) {
	rc := &RunContext{
		Ctx: context.Background(),
		Payload: types.RunPayload{
			Subject: types.Subject{
				Spec: types.SubjectSpec{
					Vars: types.SubjectVars{
						JobVars: map[string]interface{}{"guid": "xyz-789"},
					},
				},
			},
		},
	}
	if got := rc.GUID(); got != "xyz-789" {
		t.Errorf("GUID() = %q, want %q", got, "xyz-789")
	}
}

func TestRunContext_SandboxAPIInUse(t *testing.T) {
	tests := []struct {
		name string
		meta *types.Meta
		want bool
	}{
		{
			name: "aws_sandboxed true",
			meta: &types.Meta{AWSSandboxed: true},
			want: true,
		},
		{
			name: "aws_sandboxed false",
			meta: &types.Meta{AWSSandboxed: false},
			want: false,
		},
		{
			name: "nil meta",
			meta: nil,
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rc := &RunContext{
				Ctx: context.Background(),
				Payload: types.RunPayload{
					Governor: types.Governor{
						Spec: types.GovernorSpec{
							Vars: types.GovernorVars{
								Meta: tt.meta,
							},
						},
					},
				},
			}
			if got := rc.SandboxAPIInUse(); got != tt.want {
				t.Errorf("SandboxAPIInUse() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRunContext_DeployerDisabled(t *testing.T) {
	tests := []struct {
		name   string
		meta   *types.Meta
		action string
		want   bool
	}{
		{
			name: "disabled",
			meta: &types.Meta{
				Deployer: &types.DeployerMeta{
					Actions: map[string]types.DeployerActionConfig{
						"provision": {Disabled: true},
					},
				},
			},
			action: "provision",
			want:   true,
		},
		{
			name: "not disabled",
			meta: &types.Meta{
				Deployer: &types.DeployerMeta{
					Actions: map[string]types.DeployerActionConfig{
						"provision": {Disabled: false},
					},
				},
			},
			action: "provision",
			want:   false,
		},
		{
			name: "no deployer",
			meta: &types.Meta{},
			action: "provision",
			want:   false,
		},
		{
			name: "action not in map",
			meta: &types.Meta{
				Deployer: &types.DeployerMeta{
					Actions: map[string]types.DeployerActionConfig{},
				},
			},
			action: "provision",
			want:   false,
		},
		{
			name:   "nil meta",
			meta:   nil,
			action: "provision",
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rc := &RunContext{
				Ctx: context.Background(),
				Payload: types.RunPayload{
					Governor: types.Governor{
						Spec: types.GovernorSpec{
							Vars: types.GovernorVars{
								Meta: tt.meta,
							},
						},
					},
				},
			}
			if got := rc.DeployerDisabled(tt.action); got != tt.want {
				t.Errorf("DeployerDisabled(%q) = %v, want %v", tt.action, got, tt.want)
			}
		})
	}
}

func TestRunContext_StatusActions(t *testing.T) {
	rc := &RunContext{
		Ctx: context.Background(),
		Payload: types.RunPayload{
			Subject: types.Subject{
				Status: types.SubjectStatus{
					Actions: map[string]interface{}{
						"provision": map[string]interface{}{
							"state": "running",
						},
					},
				},
			},
		},
	}
	sa := rc.StatusActions()
	if sa == nil {
		t.Fatal("StatusActions() returned nil")
	}
	prov, ok := sa["provision"].(map[string]interface{})
	if !ok {
		t.Fatal("expected provision key in StatusActions()")
	}
	if prov["state"] != "running" {
		t.Errorf("StatusActions()[provision][state] = %v, want %q", prov["state"], "running")
	}
}

func TestRunContext_StatusTowerJobs(t *testing.T) {
	rc := &RunContext{
		Ctx: context.Background(),
		Payload: types.RunPayload{
			Subject: types.Subject{
				Status: types.SubjectStatus{
					TowerJobs: map[string]interface{}{
						"provision": map[string]interface{}{
							"id": "42",
						},
					},
				},
			},
		},
	}
	tj := rc.StatusTowerJobs()
	if tj == nil {
		t.Fatal("StatusTowerJobs() returned nil")
	}
}

func TestRunContext_GovernorActions(t *testing.T) {
	rc := &RunContext{
		Ctx: context.Background(),
		Payload: types.RunPayload{
			Governor: types.Governor{
				Spec: types.GovernorSpec{
					Actions: map[string]map[string]interface{}{
						"provision": {
							"roles": []interface{}{"deployer"},
						},
					},
				},
			},
		},
	}
	ga := rc.GovernorActions()
	if ga == nil {
		t.Fatal("GovernorActions() returned nil")
	}
	if _, ok := ga["provision"]; !ok {
		t.Error("expected provision key in GovernorActions()")
	}
}

func TestRunContext_ActionRetryCount(t *testing.T) {
	rc := &RunContext{
		Ctx: context.Background(),
		Payload: types.RunPayload{
			Action: &types.Action{
				Spec: types.ActionSpec{
					Vars: map[string]interface{}{
						"action_retry_count": float64(3),
					},
				},
			},
		},
	}
	if got := rc.ActionRetryCount(); got != 3 {
		t.Errorf("ActionRetryCount() = %d, want 3", got)
	}
}

func TestRunContext_ActionRetryCountNoAction(t *testing.T) {
	rc := &RunContext{
		Ctx: context.Background(),
		Payload: types.RunPayload{},
	}
	if got := rc.ActionRetryCount(); got != 0 {
		t.Errorf("ActionRetryCount() = %d, want 0", got)
	}
}

func TestRunContext_IsBeingDeleted(t *testing.T) {
	ts := "2024-01-01T00:00:00Z"
	rc := &RunContext{
		Ctx: context.Background(),
		Payload: types.RunPayload{
			Subject: types.Subject{
				Metadata: types.ObjectMeta{
					DeletionTimestamp: &ts,
				},
			},
		},
	}
	if !rc.IsBeingDeleted() {
		t.Error("IsBeingDeleted() = false, want true")
	}
}

func TestRunContext_IsBeingDeletedFalse(t *testing.T) {
	rc := &RunContext{
		Ctx: context.Background(),
		Payload: types.RunPayload{
			Subject: types.Subject{
				Metadata: types.ObjectMeta{},
			},
		},
	}
	if rc.IsBeingDeleted() {
		t.Error("IsBeingDeleted() = true, want false")
	}
}

func TestRunContext_ActionNameNoAction(t *testing.T) {
	rc := &RunContext{
		Ctx: context.Background(),
		Payload: types.RunPayload{},
	}
	if got := rc.ActionName(); got != "" {
		t.Errorf("ActionName() = %q, want empty string", got)
	}
}

func TestRunContext_ActionVars(t *testing.T) {
	rc := &RunContext{
		Ctx: context.Background(),
		Payload: types.RunPayload{
			Action: &types.Action{
				Spec: types.ActionSpec{
					Vars: map[string]interface{}{
						"key": "value",
					},
				},
			},
		},
	}
	av := rc.ActionVars()
	if av == nil {
		t.Fatal("ActionVars() returned nil")
	}
	if av["key"] != "value" {
		t.Errorf("ActionVars()[key] = %v, want %q", av["key"], "value")
	}
}

func TestRunContext_ActionVarsNoAction(t *testing.T) {
	rc := &RunContext{
		Ctx: context.Background(),
		Payload: types.RunPayload{},
	}
	if av := rc.ActionVars(); av != nil {
		t.Errorf("ActionVars() = %v, want nil", av)
	}
}

// --- Directive tests ---

func TestFinishActionDirective(t *testing.T) {
	rc := &RunContext{Ctx: context.Background()}
	rc.FinishAction("successful")

	if rc.Result.FinishAction == nil {
		t.Fatal("FinishAction should not be nil")
	}
	if rc.Result.FinishAction.State != "successful" {
		t.Errorf("FinishAction.State = %q, want %q", rc.Result.FinishAction.State, "successful")
	}
}

func TestDeleteSubjectDirective(t *testing.T) {
	rc := &RunContext{Ctx: context.Background()}
	rc.DeleteSubject(true)

	if rc.Result.DeleteSubject == nil {
		t.Fatal("DeleteSubject should not be nil")
	}
	if !rc.Result.DeleteSubject.RemoveFinalizers {
		t.Errorf("DeleteSubject.RemoveFinalizers = %v, want true", rc.Result.DeleteSubject.RemoveFinalizers)
	}
}

func TestContinueActionDirective(t *testing.T) {
	rc := &RunContext{Ctx: context.Background()}
	rc.ContinueAction("30s")

	if rc.Result.ContinueAction == nil {
		t.Fatal("ContinueAction should not be nil")
	}
	// The After field should be a timestamp (not "30s" itself)
	if rc.Result.ContinueAction.After == "" {
		t.Error("ContinueAction.After should not be empty")
	}
}

func TestContinueActionWithVarsDirective(t *testing.T) {
	rc := &RunContext{Ctx: context.Background()}
	vars := map[string]interface{}{"action_retry_count": float64(2)}
	rc.ContinueActionWithVars("1m", vars)

	if rc.Result.ContinueAction == nil {
		t.Fatal("ContinueAction should not be nil")
	}
	if rc.Result.ContinueAction.Vars == nil {
		t.Fatal("ContinueAction.Vars should not be nil")
	}
	if rc.Result.ContinueAction.Vars["action_retry_count"] != float64(2) {
		t.Errorf("ContinueAction.Vars[action_retry_count] = %v, want 2",
			rc.Result.ContinueAction.Vars["action_retry_count"])
	}
}

// --- SubjectAllVars / GovernorAllVars tests ---

func TestRunContext_SubjectAllVars(t *testing.T) {
	rc := &RunContext{
		Ctx: context.Background(),
		Payload: types.RunPayload{
			Subject: types.Subject{
				Spec: types.SubjectSpec{
					Vars: types.SubjectVars{
						CurrentState: "started",
						DesiredState: "stopped",
						JobVars: map[string]interface{}{
							"uuid": "test-uuid",
						},
					},
				},
			},
		},
	}
	allVars := rc.SubjectAllVars()
	if allVars == nil {
		t.Fatal("SubjectAllVars() returned nil")
	}
	if allVars["current_state"] != "started" {
		t.Errorf("SubjectAllVars()[current_state] = %v, want %q", allVars["current_state"], "started")
	}
	if jv, ok := allVars["job_vars"].(map[string]interface{}); !ok || jv["uuid"] != "test-uuid" {
		t.Errorf("SubjectAllVars()[job_vars][uuid] unexpected value")
	}
}

func TestRunContext_GovernorAllVars(t *testing.T) {
	rc := &RunContext{
		Ctx: context.Background(),
		Payload: types.RunPayload{
			Governor: types.Governor{
				Spec: types.GovernorSpec{
					Vars: types.GovernorVars{
						JobVars: map[string]interface{}{
							"region": "us-east-1",
						},
					},
				},
			},
		},
	}
	allVars := rc.GovernorAllVars()
	if allVars == nil {
		t.Fatal("GovernorAllVars() returned nil")
	}
	if jv, ok := allVars["job_vars"].(map[string]interface{}); !ok || jv["region"] != "us-east-1" {
		t.Errorf("GovernorAllVars()[job_vars][region] unexpected value")
	}
}

// --- SetHandlers test ---

func TestSetHandlers(t *testing.T) {
	cfg := Config{
		AnarchyURL:      "http://localhost",
		RunnerName:      "test",
		PodName:         "pod",
		RunnerToken:     "token",
		PollingInterval: 5 * time.Second,
		RequestTimeout:  5 * time.Second,
	}
	r := New(cfg, nil, nil)

	handlers := map[string]HandlerFunc{
		"event:create": func(rc *RunContext) error { return nil },
	}
	r.SetHandlers(handlers)

	rc := &RunContext{
		Ctx: context.Background(),
		Payload: types.RunPayload{
			Handler: types.Handler{Type: "subjectEvent", Name: "create"},
		},
	}
	if err := Dispatch(rc, r.handlers); err != nil {
		t.Fatalf("Dispatch returned error: %v", err)
	}
}

func TestPollOnceRecoversPanic(t *testing.T) {
	var postBody struct {
		Result types.RunResult `json:"result"`
	}
	var postCalled atomic.Int32
	runName := "test-run-panic"

	payload := types.RunPayload{
		Handler: types.Handler{
			Type: "subjectEvent",
			Name: "create",
		},
		Subject: types.Subject{
			Metadata: types.ObjectMeta{Name: "test-subject"},
		},
		Run: types.Run{
			Metadata: types.ObjectMeta{Name: runName},
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/run":
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(payload)
		case r.Method == http.MethodPost && r.URL.Path == fmt.Sprintf("/run/%s", runName):
			postCalled.Add(1)
			json.NewDecoder(r.Body).Decode(&postBody)
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	cfg := Config{
		AnarchyURL:      server.URL,
		RunnerName:      "runner",
		PodName:         "pod",
		RunnerToken:     "token",
		PollingInterval: 5 * time.Second,
		RequestTimeout:  5 * time.Second,
	}

	runner := New(cfg, nil, nil)
	runner.handlers["event:create"] = func(rc *RunContext) error {
		panic("nil pointer dereference simulation")
	}

	err := runner.pollOnce(context.Background())
	if err != nil {
		t.Fatalf("pollOnce returned error: %v", err)
	}

	if postCalled.Load() != 1 {
		t.Errorf("POST call count = %d, want 1", postCalled.Load())
	}
	if postBody.Result.Status != "failed" {
		t.Errorf("result status = %q, want %q", postBody.Result.Status, "failed")
	}
	if !strings.Contains(postBody.Result.StatusMessage, "panic") {
		t.Errorf("status message = %q, should contain 'panic'", postBody.Result.StatusMessage)
	}
}

// --- helper ---

func newTestRunner(serverURL string) *Runner {
	cfg := Config{
		AnarchyURL:      serverURL,
		RunnerName:      "r",
		PodName:         "p",
		RunnerToken:     "t",
		PollingInterval: 5 * time.Second,
		RequestTimeout:  5 * time.Second,
	}
	anarchyCfg := clients.AnarchyClientConfig{
		BaseURL:    serverURL,
		AuthHeader: cfg.AuthHeader(),
		Timeout:    cfg.RequestTimeout,
	}
	return &Runner{
		config:         cfg,
		client:         &http.Client{Timeout: 5 * time.Second},
		anarchy:        clients.NewAnarchyClient(anarchyCfg),
		handlers:       make(map[string]HandlerFunc),
		postRetryDelay: 0, // no delay in tests
	}
}
