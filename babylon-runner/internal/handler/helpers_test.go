package handler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/rhpds/anarchy/babylon-runner/internal/clients"
	"github.com/rhpds/anarchy/babylon-runner/internal/runner"
	"github.com/rhpds/anarchy/babylon-runner/internal/types"
)

// assertAfterTimestamp checks that an "after" field is a valid UTC
// timestamp approximately `expected` duration from now (within 10s tolerance).
func assertAfterTimestamp(t *testing.T, got string, expected string) {
	t.Helper()
	ts, err := time.Parse("2006-01-02T15:04:05Z", got)
	if err != nil {
		t.Errorf("after: %q is not a valid timestamp: %v", got, err)
		return
	}
	d, err := time.ParseDuration(expected)
	if err != nil {
		t.Fatalf("bad expected duration %q: %v", expected, err)
	}
	diff := ts.Sub(time.Now().UTC())
	tolerance := 10 * time.Second
	if diff < d-tolerance || diff > d+tolerance {
		t.Errorf("after = %s (%.0fs from now), want ~%s", got, diff.Seconds(), expected)
	}
}

// anarchyCall records an HTTP request made to the test Anarchy server.
type anarchyCall struct {
	Method string
	Path   string
	Body   map[string]interface{}
}

// newTestAnarchyServer creates an httptest.Server that records all requests.
// It returns the server and a pointer to the call log slice.
func newTestAnarchyServer(t *testing.T) (*httptest.Server, *[]anarchyCall) {
	t.Helper()
	var (
		calls []anarchyCall
		mu    sync.Mutex
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]interface{}
		if r.Body != nil {
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				body = nil
			}
		}

		mu.Lock()
		calls = append(calls, anarchyCall{
			Method: r.Method,
			Path:   r.URL.Path,
			Body:   body,
		})
		mu.Unlock()

		w.WriteHeader(http.StatusOK)
	}))

	return server, &calls
}

// newTestRunContext creates a RunContext connected to the given test
// Anarchy server. The governor has provision, destroy, start, stop, and
// update actions. The subject is minimal (no current_state, no job_vars).
func newTestRunContext(t *testing.T, server *httptest.Server) *runner.RunContext {
	t.Helper()
	cfg := clients.AnarchyClientConfig{
		BaseURL:    server.URL,
		AuthHeader: "Bearer test-token",
		Timeout:    5 * time.Second,
	}

	return &runner.RunContext{
		Payload: types.RunPayload{
			Governor: types.Governor{
				Spec: types.GovernorSpec{
					Actions: map[string]map[string]interface{}{
						"provision": {},
						"destroy":   {},
						"start":     {},
						"stop":      {},
						"update":    {},
					},
					Vars: types.GovernorVars{
						JobVars: map[string]interface{}{
							"cloud_provider": "ec2",
						},
					},
				},
			},
			Subject: types.Subject{
				Metadata: types.ObjectMeta{
					Name: "test-subject",
				},
				Spec: types.SubjectSpec{
					Vars: types.SubjectVars{},
				},
				Status: types.SubjectStatus{},
			},
		},
		Result:        types.RunResult{Status: "successful"},
		AnarchyClient: clients.NewAnarchyClient(cfg),
	}
}

// newTestTowerServer creates a mock Tower TLS server that handles the
// full LaunchJob workflow (tokens, resources, search, job status).
// Returns an httptest.Server whose URL matches what NewTowerClient
// constructs: "https://" + host:port.
func newTestTowerServer(t *testing.T) *httptest.Server {
	t.Helper()
	idCounter := 0
	return httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "POST":
			idCounter++
			json.NewEncoder(w).Encode(map[string]interface{}{
				"id":    float64(idCounter),
				"token": "test-token",
			})
		case "DELETE":
			w.WriteHeader(http.StatusNoContent)
		case "GET":
			if r.URL.RawQuery != "" {
				// Search requests: resource not found.
				json.NewEncoder(w).Encode(map[string]interface{}{
					"count":   0,
					"results": []interface{}{},
				})
			} else {
				// GET job status.
				json.NewEncoder(w).Encode(map[string]interface{}{
					"id":     float64(42),
					"status": "successful",
				})
			}
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
}

// towerServerHost extracts the host:port from an httptest TLS server URL.
// This is used as the controller hostname so that NewTowerClient constructs
// a baseURL that matches the test server.
func towerServerHost(t *testing.T, ts *httptest.Server) string {
	t.Helper()
	u, err := url.Parse(ts.URL)
	if err != nil {
		t.Fatalf("parse tower server URL: %v", err)
	}
	return u.Host
}

// withTowerServer configures the RunContext with a mock tower server
// by setting the __meta__.ansible_controllers and deployer SCM config.
func withTowerServer(rc *runner.RunContext, towerServer *httptest.Server) {
	host := ""
	if u, err := url.Parse(towerServer.URL); err == nil {
		host = u.Host
	}

	rc.Payload.Governor.Spec.Vars.Meta = &types.Meta{
		Deployer: &types.DeployerMeta{
			SCMUrl: "https://github.com/example/repo.git",
			SCMRef: "main",
		},
		AnsibleControllers: []map[string]interface{}{
			{
				"hostname": host,
				"user":     "admin",
				"password": "secret",
			},
		},
	}
}

// withTowerServerAndMeta configures the RunContext similarly to
// withTowerServer but preserves any existing Meta fields.
func withTowerServerAndMeta(rc *runner.RunContext, towerServer *httptest.Server) {
	host := ""
	if u, err := url.Parse(towerServer.URL); err == nil {
		host = u.Host
	}

	meta := rc.Payload.Governor.Spec.Vars.Meta
	if meta == nil {
		meta = &types.Meta{}
	}
	if meta.Deployer == nil {
		meta.Deployer = &types.DeployerMeta{
			SCMUrl: "https://github.com/example/repo.git",
			SCMRef: "main",
		}
	}
	meta.AnsibleControllers = []map[string]interface{}{
		{
			"hostname": host,
			"user":     "admin",
			"password": "secret",
		},
	}
	rc.Payload.Governor.Spec.Vars.Meta = meta
}

// contains is a helper for checking substring presence.
func contains(s, substr string) bool {
	return len(s) > 0 && len(substr) > 0 && len(s) >= len(substr) && indexOf(s, substr) >= 0
}

func indexOf(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}
