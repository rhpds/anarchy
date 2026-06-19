package clients

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rhpds/anarchy/babylon-runner/internal/types"
)

func TestAnarchyClientSubjectUpdate(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify method.
		if r.Method != http.MethodPatch {
			t.Errorf("method = %s, want PATCH", r.Method)
		}
		// Verify path.
		if r.URL.Path != "/run/subject/test-subject" {
			t.Errorf("path = %s, want /run/subject/test-subject", r.URL.Path)
		}
		// Verify auth header.
		want := "Bearer runner:pod:token"
		if got := r.Header.Get("Authorization"); got != want {
			t.Errorf("Authorization = %q, want %q", got, want)
		}
		// Decode body and verify content.
		var patch types.SubjectPatch
		if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		if patch.Patch.Metadata == nil {
			t.Fatal("expected metadata in patch, got nil")
		}
		if got := patch.Patch.Metadata.Labels["env"]; got != "prod" {
			t.Errorf("label env = %q, want %q", got, "prod")
		}
		if !patch.Patch.SkipUpdateProcessing {
			t.Error("skip_update_processing = false, want true")
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := AnarchyClientConfig{
		BaseURL:    server.URL,
		AuthHeader: "Bearer runner:pod:token",
	}
	client := NewAnarchyClient(cfg)

	patch := types.SubjectPatch{
		Patch: types.PatchBody{
			Metadata: &types.PatchMetadata{
				Labels: map[string]string{"env": "prod"},
			},
			SkipUpdateProcessing: true,
		},
	}
	if err := client.SubjectUpdate(context.Background(), "test-subject", patch); err != nil {
		t.Fatalf("SubjectUpdate returned error: %v", err)
	}
}

func TestAnarchyClientScheduleAction(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify method.
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		// Verify path.
		if r.URL.Path != "/run/subject/test-subject/actions" {
			t.Errorf("path = %s, want /run/subject/test-subject/actions", r.URL.Path)
		}
		// Verify auth header.
		want := "Bearer runner:pod:token"
		if got := r.Header.Get("Authorization"); got != want {
			t.Errorf("Authorization = %q, want %q", got, want)
		}
		// Decode body and verify content.
		var req types.ScheduleActionRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		if req.Action != "stop" {
			t.Errorf("action = %q, want %q", req.Action, "stop")
		}
		if len(req.Cancel) != 2 || req.Cancel[0] != "start" || req.Cancel[1] != "restart" {
			t.Errorf("cancel = %v, want [start restart]", req.Cancel)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := AnarchyClientConfig{
		BaseURL:    server.URL,
		AuthHeader: "Bearer runner:pod:token",
	}
	client := NewAnarchyClient(cfg)

	req := types.ScheduleActionRequest{
		Action: "stop",
		Cancel: []string{"start", "restart"},
	}
	if err := client.ScheduleAction(context.Background(), "test-subject", req); err != nil {
		t.Fatalf("ScheduleAction returned error: %v", err)
	}
}

func TestAnarchyClientRetry(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := attempts.Add(1)
		if n <= 2 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := AnarchyClientConfig{
		BaseURL:    server.URL,
		AuthHeader: "Bearer runner:pod:token",
	}
	client := NewAnarchyClient(cfg)
	// Use zero delays so the test runs fast.
	client.retryDelays = []time.Duration{0, 0, 0}

	patch := types.SubjectPatch{
		Patch: types.PatchBody{
			SkipUpdateProcessing: true,
		},
	}
	if err := client.SubjectUpdate(context.Background(), "test-subject", patch); err != nil {
		t.Fatalf("SubjectUpdate should succeed after retries, got: %v", err)
	}
	if got := attempts.Load(); got != 3 {
		t.Errorf("attempts = %d, want 3", got)
	}
}

func TestAnarchyClientRetryExhausted(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	cfg := AnarchyClientConfig{
		BaseURL:    server.URL,
		AuthHeader: "Bearer runner:pod:token",
	}
	client := NewAnarchyClient(cfg)
	// Use zero delays so the test runs fast.
	client.retryDelays = []time.Duration{0, 0}

	patch := types.SubjectPatch{
		Patch: types.PatchBody{
			SkipUpdateProcessing: true,
		},
	}
	err := client.SubjectUpdate(context.Background(), "test-subject", patch)
	if err == nil {
		t.Fatal("expected error when all retries exhausted, got nil")
	}
	// 1 initial attempt + 2 retries = 3 total attempts
	if got := attempts.Load(); got != 3 {
		t.Errorf("attempts = %d, want 3", got)
	}
}
