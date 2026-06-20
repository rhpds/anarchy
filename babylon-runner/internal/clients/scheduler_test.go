package clients

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rhpds/anarchy/babylon-runner/internal/types"
)

func TestSchedulerEvaluateSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/evaluate/controllers" {
			t.Errorf("path = %s, want /api/v1/evaluate/controllers", r.URL.Path)
		}
		if r.Header.Get("X-API-Key") != "test-key" {
			t.Errorf("X-API-Key = %q, want %q", r.Header.Get("X-API-Key"), "test-key")
		}

		var req EvaluateRequest
		json.NewDecoder(r.Body).Decode(&req)
		if len(req.Candidates) != 2 {
			t.Errorf("candidates = %d, want 2", len(req.Candidates))
		}

		json.NewEncoder(w).Encode(EvaluateResponse{
			Ranked: []RankedController{
				{Domain: "host1.example.com", Name: "ctrl-1", Score: 85.0, Eligible: true},
				{Domain: "host2.example.com", Name: "ctrl-2", Score: 42.0, Eligible: true},
			},
			Strategy: "weighted",
		})
	}))
	defer server.Close()

	client := NewSchedulerClient(server.URL, "test-key", nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.Evaluate(ctx, EvaluateRequest{
		Candidates: []Candidate{
			{Domain: "host1.example.com"},
			{Domain: "host2.example.com"},
		},
		RequireLabels: map[string]types.StringOrSlice{"env": {"prod"}},
		InstanceGroup: "provision",
	})
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if len(resp.Ranked) != 2 {
		t.Fatalf("ranked = %d, want 2", len(resp.Ranked))
	}
	if resp.Ranked[0].Domain != "host1.example.com" {
		t.Errorf("ranked[0].domain = %q, want %q", resp.Ranked[0].Domain, "host1.example.com")
	}
	if resp.Strategy != "weighted" {
		t.Errorf("strategy = %q, want %q", resp.Strategy, "weighted")
	}
}

func TestSchedulerEvaluateServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	client := NewSchedulerClient(server.URL, "test-key", nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := client.Evaluate(ctx, EvaluateRequest{
		Candidates: []Candidate{{Domain: "host1"}},
	})
	if err == nil {
		t.Error("expected error for 500")
	}
}

func TestSchedulerEvaluateTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(3 * time.Second)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewSchedulerClient(server.URL, "test-key", nil)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	_, err := client.Evaluate(ctx, EvaluateRequest{
		Candidates: []Candidate{{Domain: "host1"}},
	})
	if err == nil {
		t.Error("expected error for timeout")
	}
}
