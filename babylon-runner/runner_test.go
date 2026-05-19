package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestRunnerDispatchEvent(t *testing.T) {
	var called bool
	handlers := map[string]HandlerFunc{
		"event:create": func(rc *RunContext) error {
			called = true
			return nil
		},
	}

	rc := &RunContext{
		Payload: RunPayload{
			Handler: Handler{
				Type: "subjectEvent",
				Name: "create",
			},
		},
	}

	if err := dispatch(rc, handlers); err != nil {
		t.Fatalf("dispatch returned error: %v", err)
	}
	if !called {
		t.Error("expected event:create handler to be called")
	}
}

func TestRunnerDispatchAction(t *testing.T) {
	var called bool
	handlers := map[string]HandlerFunc{
		"action:provision": func(rc *RunContext) error {
			called = true
			return nil
		},
	}

	rc := &RunContext{
		Payload: RunPayload{
			Handler: Handler{
				Type: "action",
				Name: "run",
			},
			Action: map[string]interface{}{
				"spec": map[string]interface{}{
					"action": "provision",
				},
			},
		},
		ActionName: "provision",
	}

	if err := dispatch(rc, handlers); err != nil {
		t.Fatalf("dispatch returned error: %v", err)
	}
	if !called {
		t.Error("expected action:provision handler to be called")
	}
}

func TestRunnerDispatchActionCallback(t *testing.T) {
	var called bool
	handlers := map[string]HandlerFunc{
		"action:provision:complete": func(rc *RunContext) error {
			called = true
			return nil
		},
	}

	rc := &RunContext{
		Payload: RunPayload{
			Handler: Handler{
				Type: "actionCallback",
				Name: "complete",
			},
			Action: map[string]interface{}{
				"spec": map[string]interface{}{
					"action": "provision",
				},
			},
		},
		ActionName: "provision",
	}

	if err := dispatch(rc, handlers); err != nil {
		t.Fatalf("dispatch returned error: %v", err)
	}
	if !called {
		t.Error("expected action:provision:complete handler to be called")
	}
}

func TestRunnerDispatchUnknown(t *testing.T) {
	handlers := map[string]HandlerFunc{}
	rc := &RunContext{
		Payload: RunPayload{
			Handler: Handler{
				Type: "subjectEvent",
				Name: "unknown",
			},
		},
	}

	err := dispatch(rc, handlers)
	if err == nil {
		t.Fatal("expected error for unknown handler, got nil")
	}
}

func TestRunnerPollAndPost(t *testing.T) {
	var postCalled atomic.Int32
	var postBody RunResult

	runName := "test-run-abc"
	payload := RunPayload{
		Handler: Handler{
			Type: "subjectEvent",
			Name: "create",
		},
		Subject: map[string]interface{}{
			"metadata": map[string]interface{}{
				"name": "test-subject",
			},
		},
		Run: map[string]interface{}{
			"metadata": map[string]interface{}{
				"name": runName,
			},
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
		AnarchyURL:     server.URL,
		RunnerName:     "runner",
		PodName:        "pod",
		RunnerToken:    "token",
		RequestTimeout: 5,
	}

	runner := NewRunner(cfg)
	runner.handlers["event:create"] = func(rc *RunContext) error {
		return nil
	}

	err := runner.pollOnce()
	if err != nil {
		t.Fatalf("pollOnce returned error: %v", err)
	}

	if postCalled.Load() != 1 {
		t.Errorf("POST /run/%s call count = %d, want 1", runName, postCalled.Load())
	}
	if postBody.Result.Status != "successful" {
		t.Errorf("result status = %q, want %q", postBody.Result.Status, "successful")
	}
	if postBody.Result.RC != 0 {
		t.Errorf("result rc = %d, want 0", postBody.Result.RC)
	}
}

func TestRunnerPollAndPostFailedHandler(t *testing.T) {
	runName := "test-run-fail"
	var postBody RunResult

	payload := RunPayload{
		Handler: Handler{
			Type: "subjectEvent",
			Name: "create",
		},
		Subject: map[string]interface{}{
			"metadata": map[string]interface{}{
				"name": "test-subject",
			},
		},
		Run: map[string]interface{}{
			"metadata": map[string]interface{}{
				"name": runName,
			},
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
		AnarchyURL:     server.URL,
		RunnerName:     "runner",
		PodName:        "pod",
		RunnerToken:    "token",
		RequestTimeout: 5,
	}

	runner := NewRunner(cfg)
	runner.handlers["event:create"] = func(rc *RunContext) error {
		return fmt.Errorf("handler failed")
	}

	err := runner.pollOnce()
	if err != nil {
		t.Fatalf("pollOnce returned error: %v", err)
	}

	if postBody.Result.Status != "failed" {
		t.Errorf("result status = %q, want %q", postBody.Result.Status, "failed")
	}
	if postBody.Result.RC != 1 {
		t.Errorf("result rc = %d, want 1", postBody.Result.RC)
	}
}

func TestRunContextCurrentState(t *testing.T) {
	rc := &RunContext{
		Payload: RunPayload{
			Subject: map[string]interface{}{
				"spec": map[string]interface{}{
					"vars": map[string]interface{}{
						"current_state": "started",
					},
				},
			},
		},
	}
	if got := rc.CurrentState(); got != "started" {
		t.Errorf("CurrentState() = %q, want %q", got, "started")
	}
}

func TestRunContextDesiredState(t *testing.T) {
	rc := &RunContext{
		Payload: RunPayload{
			Subject: map[string]interface{}{
				"spec": map[string]interface{}{
					"vars": map[string]interface{}{
						"desired_state": "stopped",
					},
				},
			},
		},
	}
	if got := rc.DesiredState(); got != "stopped" {
		t.Errorf("DesiredState() = %q, want %q", got, "stopped")
	}
}

func TestRunContextJobVars(t *testing.T) {
	rc := &RunContext{
		Payload: RunPayload{
			Subject: map[string]interface{}{
				"spec": map[string]interface{}{
					"vars": map[string]interface{}{
						"job_vars": map[string]interface{}{
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

func TestRunContextUUID(t *testing.T) {
	rc := &RunContext{
		Payload: RunPayload{
			Subject: map[string]interface{}{
				"spec": map[string]interface{}{
					"vars": map[string]interface{}{
						"job_vars": map[string]interface{}{
							"uuid": "abc-123",
						},
					},
				},
			},
		},
	}
	if got := rc.UUID(); got != "abc-123" {
		t.Errorf("UUID() = %q, want %q", got, "abc-123")
	}
}

func TestRunContextGUID(t *testing.T) {
	rc := &RunContext{
		Payload: RunPayload{
			Subject: map[string]interface{}{
				"spec": map[string]interface{}{
					"vars": map[string]interface{}{
						"job_vars": map[string]interface{}{
							"guid": "xyz-789",
						},
					},
				},
			},
		},
	}
	if got := rc.GUID(); got != "xyz-789" {
		t.Errorf("GUID() = %q, want %q", got, "xyz-789")
	}
}

func TestRunContextSandboxAPIInUse(t *testing.T) {
	tests := []struct {
		name string
		meta map[string]interface{}
		want bool
	}{
		{
			name: "aws_sandboxed true",
			meta: map[string]interface{}{
				"aws_sandboxed": true,
			},
			want: true,
		},
		{
			name: "sandboxes non-empty",
			meta: map[string]interface{}{
				"sandboxes": []interface{}{"sandbox-1"},
			},
			want: true,
		},
		{
			name: "neither set",
			meta: map[string]interface{}{},
			want: false,
		},
		{
			name: "sandboxes empty",
			meta: map[string]interface{}{
				"sandboxes": []interface{}{},
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rc := &RunContext{
				Payload: RunPayload{
					Governor: map[string]interface{}{
						"spec": map[string]interface{}{
							"vars": map[string]interface{}{
								"__meta__": tt.meta,
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

func TestRunContextDeployerDisabled(t *testing.T) {
	tests := []struct {
		name   string
		meta   map[string]interface{}
		action string
		want   bool
	}{
		{
			name: "disabled",
			meta: map[string]interface{}{
				"deployer": map[string]interface{}{
					"entry_points": map[string]interface{}{
						"provision": "disabled",
					},
				},
			},
			action: "provision",
			want:   true,
		},
		{
			name: "none",
			meta: map[string]interface{}{
				"deployer": map[string]interface{}{
					"entry_points": map[string]interface{}{
						"provision": "none",
					},
				},
			},
			action: "provision",
			want:   true,
		},
		{
			name: "enabled",
			meta: map[string]interface{}{
				"deployer": map[string]interface{}{
					"entry_points": map[string]interface{}{
						"provision": "main.yml",
					},
				},
			},
			action: "provision",
			want:   false,
		},
		{
			name:   "no meta",
			meta:   map[string]interface{}{},
			action: "provision",
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rc := &RunContext{
				Payload: RunPayload{
					Governor: map[string]interface{}{
						"spec": map[string]interface{}{
							"vars": map[string]interface{}{
								"__meta__": tt.meta,
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

func TestRunContextGovernorJobVars(t *testing.T) {
	rc := &RunContext{
		Payload: RunPayload{
			Governor: map[string]interface{}{
				"spec": map[string]interface{}{
					"vars": map[string]interface{}{
						"job_vars": map[string]interface{}{
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

func TestRunContextMeta(t *testing.T) {
	rc := &RunContext{
		Payload: RunPayload{
			Governor: map[string]interface{}{
				"spec": map[string]interface{}{
					"vars": map[string]interface{}{
						"__meta__": map[string]interface{}{
							"deployer": "agnosticd",
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
	if m["deployer"] != "agnosticd" {
		t.Errorf("Meta()[deployer] = %v, want %q", m["deployer"], "agnosticd")
	}
}

func TestRunContextStatusActions(t *testing.T) {
	rc := &RunContext{
		Payload: RunPayload{
			Subject: map[string]interface{}{
				"status": map[string]interface{}{
					"actions": map[string]interface{}{
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

func TestRunContextStatusTowerJobs(t *testing.T) {
	rc := &RunContext{
		Payload: RunPayload{
			Subject: map[string]interface{}{
				"status": map[string]interface{}{
					"towerJobs": map[string]interface{}{
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

func TestRunContextGovernorActions(t *testing.T) {
	rc := &RunContext{
		Payload: RunPayload{
			Governor: map[string]interface{}{
				"spec": map[string]interface{}{
					"actions": map[string]interface{}{
						"provision": map[string]interface{}{
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
}
