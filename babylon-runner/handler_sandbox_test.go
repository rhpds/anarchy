package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// newSimpleSandboxServer creates a mock Sandbox API server with common routes.
func newSimpleSandboxServer(t *testing.T, handlers map[string]http.HandlerFunc) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for route, handler := range handlers {
			if strings.HasPrefix(r.URL.Path, route) || r.URL.Path == route {
				handler(w, r)
				return
			}
		}
		http.NotFound(w, r)
	}))
}

// withSandboxEnabled configures a RunContext for sandbox API use.
func withSandboxEnabled(rc *RunContext, sandboxServer *httptest.Server, uuid string) {
	setNested(rc.Payload.Governor, true, "spec", "vars", "__meta__", "aws_sandboxed")
	setNested(rc.Payload.Governor, "test-login-token", "spec", "vars", "__meta__", "sandbox_api_login_token")
	setNested(rc.Payload.Subject, uuid, "spec", "vars", "job_vars", "uuid")
	setNested(rc.Payload.Subject, "test-guid-123", "spec", "vars", "job_vars", "guid")
	rc.SandboxBaseURL = sandboxServer.URL
}

// --- TestGetSandboxClient ---

func TestGetSandboxClient(t *testing.T) {
	tests := []struct {
		name        string
		setupRC     func(*RunContext)
		wantBaseURL string
	}{
		{
			name: "with SandboxBaseURL set",
			setupRC: func(rc *RunContext) {
				rc.SandboxBaseURL = "http://test-sandbox.local:8080"
			},
			wantBaseURL: "http://test-sandbox.local:8080",
		},
		{
			name: "with meta sandbox_api_url set",
			setupRC: func(rc *RunContext) {
				setNested(rc.Payload.Governor, "http://meta-sandbox.local:9090", "spec", "vars", "__meta__", "sandbox_api_url")
			},
			wantBaseURL: "http://meta-sandbox.local:9090",
		},
		{
			name:        "with nothing set - uses default",
			setupRC:     func(rc *RunContext) {},
			wantBaseURL: DefaultSandboxAPIURL,
		},
		{
			name: "SandboxBaseURL overrides meta",
			setupRC: func(rc *RunContext) {
				rc.SandboxBaseURL = "http://test-override.local"
				setNested(rc.Payload.Governor, "http://meta-sandbox.local:9090", "spec", "vars", "__meta__", "sandbox_api_url")
			},
			wantBaseURL: "http://test-override.local",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server, _ := newTestAnarchyServer(t)
			defer server.Close()

			rc := newTestRunContext(t, server)
			tt.setupRC(rc)

			client := getSandboxClient(rc)
			if client.baseURL != tt.wantBaseURL {
				t.Errorf("baseURL = %s, want %s", client.baseURL, tt.wantBaseURL)
			}
		})
	}
}

// --- TestSandboxLoginToken ---

func TestSandboxLoginToken(t *testing.T) {
	tests := []struct {
		name      string
		setupRC   func(*RunContext)
		wantToken string
	}{
		{
			name: "token present in meta",
			setupRC: func(rc *RunContext) {
				setNested(rc.Payload.Governor, "my-secret-token", "spec", "vars", "__meta__", "sandbox_api_login_token")
			},
			wantToken: "my-secret-token",
		},
		{
			name:      "no meta - returns empty",
			setupRC:   func(rc *RunContext) {},
			wantToken: "",
		},
		{
			name: "meta exists but no token - returns empty",
			setupRC: func(rc *RunContext) {
				setNested(rc.Payload.Governor, "some-value", "spec", "vars", "__meta__", "other_field")
			},
			wantToken: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server, _ := newTestAnarchyServer(t)
			defer server.Close()

			rc := newTestRunContext(t, server)
			tt.setupRC(rc)

			token := sandboxLoginToken(rc)
			if token != tt.wantToken {
				t.Errorf("sandboxLoginToken() = %q, want %q", token, tt.wantToken)
			}
		})
	}
}

// --- TestSandboxLogin ---

func TestSandboxLogin(t *testing.T) {
	t.Run("success - returns access token", func(t *testing.T) {
		sandboxServer := newSimpleSandboxServer(t, map[string]http.HandlerFunc{
			"/api/v1/login": func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost {
					t.Errorf("expected POST, got %s", r.Method)
				}
				auth := r.Header.Get("Authorization")
				if auth != "Bearer test-login-token" {
					t.Errorf("Authorization = %s, want 'Bearer test-login-token'", auth)
				}
				json.NewEncoder(w).Encode(map[string]string{"access_token": "access-123"})
			},
		})
		defer sandboxServer.Close()

		anarchyServer, _ := newTestAnarchyServer(t)
		defer anarchyServer.Close()

		rc := newTestRunContext(t, anarchyServer)
		setNested(rc.Payload.Governor, "test-login-token", "spec", "vars", "__meta__", "sandbox_api_login_token")
		rc.SandboxBaseURL = sandboxServer.URL

		accessToken, err := sandboxLogin(rc)
		if err != nil {
			t.Fatalf("sandboxLogin() error = %v", err)
		}
		if accessToken != "access-123" {
			t.Errorf("accessToken = %s, want 'access-123'", accessToken)
		}
	})

	t.Run("no login token - returns error", func(t *testing.T) {
		anarchyServer, _ := newTestAnarchyServer(t)
		defer anarchyServer.Close()

		rc := newTestRunContext(t, anarchyServer)

		_, err := sandboxLogin(rc)
		if err == nil {
			t.Fatal("expected error when no login token, got nil")
		}
		if !strings.Contains(err.Error(), "no sandbox_api_login_token") {
			t.Errorf("error = %v, want 'no sandbox_api_login_token'", err)
		}
	})

	t.Run("server error - returns error", func(t *testing.T) {
		sandboxServer := newSimpleSandboxServer(t, map[string]http.HandlerFunc{
			"/api/v1/login": func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
			},
		})
		defer sandboxServer.Close()

		anarchyServer, _ := newTestAnarchyServer(t)
		defer anarchyServer.Close()

		rc := newTestRunContext(t, anarchyServer)
		setNested(rc.Payload.Governor, "test-login-token", "spec", "vars", "__meta__", "sandbox_api_login_token")
		rc.SandboxBaseURL = sandboxServer.URL

		_, err := sandboxLogin(rc)
		if err == nil {
			t.Fatal("expected error when server returns 500, got nil")
		}
		if !strings.Contains(err.Error(), "sandbox login") {
			t.Errorf("error = %v, want 'sandbox login' error", err)
		}
	})
}

// --- TestSandboxGet ---

func TestSandboxGet(t *testing.T) {
	t.Run("no UUID - returns error", func(t *testing.T) {
		anarchyServer, _ := newTestAnarchyServer(t)
		defer anarchyServer.Close()

		rc := newTestRunContext(t, anarchyServer)

		_, err := sandboxGet(rc, "provision")
		if err == nil {
			t.Fatal("expected error when no uuid, got nil")
		}
		if !strings.Contains(err.Error(), "no uuid") {
			t.Errorf("error = %v, want 'no uuid'", err)
		}
	})

	t.Run("placement found with resources - extracts vars and updates subject", func(t *testing.T) {
		sandboxServer := newSimpleSandboxServer(t, map[string]http.HandlerFunc{
			"/api/v1/login": func(w http.ResponseWriter, r *http.Request) {
				json.NewEncoder(w).Encode(map[string]string{"access_token": "access-token"})
			},
			"/api/v1/placements/": func(w http.ResponseWriter, r *http.Request) {
				placement := map[string]interface{}{
					"uuid":   "test-uuid-123",
					"status": "available",
					"resources": []interface{}{
						map[string]interface{}{
							"name":  "sandbox-aws-1",
							"kind":  "AwsSandbox",
							"cloud": "aws",
							"credentials": map[string]interface{}{
								"aws_access_key_id":     "AKIATEST",
								"aws_secret_access_key": "secret123",
								"aws_default_region":    "us-east-1",
							},
							"hosted_zone_id": "Z1234567890ABC",
							"account_id":     "123456789012",
						},
					},
				}
				json.NewEncoder(w).Encode(placement)
			},
		})
		defer sandboxServer.Close()

		anarchyServer, calls := newTestAnarchyServer(t)
		defer anarchyServer.Close()

		rc := newTestRunContext(t, anarchyServer)
		withSandboxEnabled(rc, sandboxServer, "test-uuid-123")

		result, err := sandboxGet(rc, "provision")
		if err != nil {
			t.Fatalf("sandboxGet() error = %v", err)
		}

		if result.Status != "success" {
			t.Errorf("Status = %s, want 'success'", result.Status)
		}

		// Verify extracted vars.
		if result.DynamicVars["aws_access_key_id"] != "AKIATEST" {
			t.Errorf("aws_access_key_id = %v, want 'AKIATEST'", result.DynamicVars["aws_access_key_id"])
		}
		if result.DynamicVars["hosted_zone_id"] != "Z1234567890ABC" {
			t.Errorf("hosted_zone_id = %v, want 'Z1234567890ABC'", result.DynamicVars["hosted_zone_id"])
		}

		// Verify labels.
		if result.Labels["sandbox"] != "sandbox-aws-1" {
			t.Errorf("sandbox label = %s, want 'sandbox-aws-1'", result.Labels["sandbox"])
		}

		// Verify subject update was called.
		if len(*calls) == 0 {
			t.Fatal("expected at least one API call for subject update")
		}
		lastCall := (*calls)[len(*calls)-1]
		if lastCall.Method != http.MethodPatch {
			t.Errorf("last call method = %s, want PATCH", lastCall.Method)
		}
	})

	t.Run("placement not found + action=provision - calls book", func(t *testing.T) {
		sandboxServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch {
			case r.Method == http.MethodPost && r.URL.Path == "/api/v1/login":
				json.NewEncoder(w).Encode(map[string]string{"access_token": "access-token"})
			case r.Method == http.MethodGet && r.URL.Path == "/api/v1/placements/test-uuid-123":
				w.WriteHeader(http.StatusNotFound)
			case r.Method == http.MethodPost && r.URL.Path == "/api/v1/placements":
				w.WriteHeader(http.StatusOK)
				json.NewEncoder(w).Encode(map[string]interface{}{
					"uuid":      "test-uuid-123",
					"status":    "available",
					"resources": []interface{}{},
				})
			default:
				http.NotFound(w, r)
			}
		}))
		defer sandboxServer.Close()

		anarchyServer, _ := newTestAnarchyServer(t)
		defer anarchyServer.Close()

		rc := newTestRunContext(t, anarchyServer)
		withSandboxEnabled(rc, sandboxServer, "test-uuid-123")
		setNested(rc.Payload.Governor, []interface{}{
			map[string]interface{}{"kind": "AwsSandbox"},
		}, "spec", "vars", "__meta__", "sandboxes")

		result, err := sandboxGet(rc, "provision")
		if err != nil {
			t.Fatalf("sandboxGet() error = %v", err)
		}

		if result.Status != "success" {
			t.Errorf("Status = %s, want 'success'", result.Status)
		}
	})

	t.Run("placement not found + action=destroy - returns not-found", func(t *testing.T) {
		sandboxServer := newSimpleSandboxServer(t, map[string]http.HandlerFunc{
			"/api/v1/login": func(w http.ResponseWriter, r *http.Request) {
				json.NewEncoder(w).Encode(map[string]string{"access_token": "access-token"})
			},
			"/api/v1/placements/": func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusNotFound)
			},
		})
		defer sandboxServer.Close()

		anarchyServer, _ := newTestAnarchyServer(t)
		defer anarchyServer.Close()

		rc := newTestRunContext(t, anarchyServer)
		withSandboxEnabled(rc, sandboxServer, "test-uuid-123")

		result, err := sandboxGet(rc, "destroy")
		if err != nil {
			t.Fatalf("sandboxGet() error = %v", err)
		}

		if result.Status != "not-found" {
			t.Errorf("Status = %s, want 'not-found'", result.Status)
		}
	})

	t.Run("placement status=error - returns error result", func(t *testing.T) {
		sandboxServer := newSimpleSandboxServer(t, map[string]http.HandlerFunc{
			"/api/v1/login": func(w http.ResponseWriter, r *http.Request) {
				json.NewEncoder(w).Encode(map[string]string{"access_token": "access-token"})
			},
			"/api/v1/placements/": func(w http.ResponseWriter, r *http.Request) {
				json.NewEncoder(w).Encode(map[string]interface{}{
					"uuid":   "test-uuid-123",
					"status": "error",
				})
			},
		})
		defer sandboxServer.Close()

		anarchyServer, _ := newTestAnarchyServer(t)
		defer anarchyServer.Close()

		rc := newTestRunContext(t, anarchyServer)
		withSandboxEnabled(rc, sandboxServer, "test-uuid-123")

		result, err := sandboxGet(rc, "provision")
		if err != nil {
			t.Fatalf("sandboxGet() error = %v", err)
		}

		if result.Status != "error" {
			t.Errorf("Status = %s, want 'error'", result.Status)
		}
	})

	t.Run("placement status=queued - returns queued result", func(t *testing.T) {
		sandboxServer := newSimpleSandboxServer(t, map[string]http.HandlerFunc{
			"/api/v1/login": func(w http.ResponseWriter, r *http.Request) {
				json.NewEncoder(w).Encode(map[string]string{"access_token": "access-token"})
			},
			"/api/v1/placements/": func(w http.ResponseWriter, r *http.Request) {
				json.NewEncoder(w).Encode(map[string]interface{}{
					"uuid":   "test-uuid-123",
					"status": "queued",
				})
			},
		})
		defer sandboxServer.Close()

		anarchyServer, _ := newTestAnarchyServer(t)
		defer anarchyServer.Close()

		rc := newTestRunContext(t, anarchyServer)
		withSandboxEnabled(rc, sandboxServer, "test-uuid-123")

		result, err := sandboxGet(rc, "provision")
		if err != nil {
			t.Fatalf("sandboxGet() error = %v", err)
		}

		if result.Status != "queued" {
			t.Errorf("Status = %s, want 'queued'", result.Status)
		}
	})
}

// --- TestSandboxBook ---

func TestSandboxBook(t *testing.T) {
	t.Run("status 200 - success with extracted vars", func(t *testing.T) {
		sandboxServer := newSimpleSandboxServer(t, map[string]http.HandlerFunc{
			"/api/v1/placements": func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				json.NewEncoder(w).Encode(map[string]interface{}{
					"uuid":   "test-uuid-123",
					"status": "available",
					"resources": []interface{}{
						map[string]interface{}{
							"name": "sandbox-1",
							"credentials": map[string]interface{}{
								"api_key": "key123",
							},
						},
					},
				})
			},
		})
		defer sandboxServer.Close()

		anarchyServer, _ := newTestAnarchyServer(t)
		defer anarchyServer.Close()

		rc := newTestRunContext(t, anarchyServer)
		withSandboxEnabled(rc, sandboxServer, "test-uuid-123")

		result, err := sandboxBook(rc, "access-token")
		if err != nil {
			t.Fatalf("sandboxBook() error = %v", err)
		}

		if result.Status != "success" {
			t.Errorf("Status = %s, want 'success'", result.Status)
		}
		if result.DynamicVars["api_key"] != "key123" {
			t.Errorf("api_key = %v, want 'key123'", result.DynamicVars["api_key"])
		}
	})

	t.Run("status 202 - queued", func(t *testing.T) {
		sandboxServer := newSimpleSandboxServer(t, map[string]http.HandlerFunc{
			"/api/v1/placements": func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusAccepted)
				json.NewEncoder(w).Encode(map[string]interface{}{
					"uuid":   "test-uuid-123",
					"status": "queued",
				})
			},
		})
		defer sandboxServer.Close()

		anarchyServer, _ := newTestAnarchyServer(t)
		defer anarchyServer.Close()

		rc := newTestRunContext(t, anarchyServer)
		withSandboxEnabled(rc, sandboxServer, "test-uuid-123")

		result, err := sandboxBook(rc, "access-token")
		if err != nil {
			t.Fatalf("sandboxBook() error = %v", err)
		}

		if result.Status != "queued" {
			t.Errorf("Status = %s, want 'queued'", result.Status)
		}
	})

	t.Run("status 507 - queued (no capacity)", func(t *testing.T) {
		sandboxServer := newSimpleSandboxServer(t, map[string]http.HandlerFunc{
			"/api/v1/placements": func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(507)
				json.NewEncoder(w).Encode(map[string]interface{}{
					"message": "No capacity",
				})
			},
		})
		defer sandboxServer.Close()

		anarchyServer, _ := newTestAnarchyServer(t)
		defer anarchyServer.Close()

		rc := newTestRunContext(t, anarchyServer)
		withSandboxEnabled(rc, sandboxServer, "test-uuid-123")

		result, err := sandboxBook(rc, "access-token")
		if err != nil {
			t.Fatalf("sandboxBook() error = %v", err)
		}

		if result.Status != "queued" {
			t.Errorf("Status = %s, want 'queued'", result.Status)
		}
	})

	t.Run("other status - error", func(t *testing.T) {
		sandboxServer := newSimpleSandboxServer(t, map[string]http.HandlerFunc{
			"/api/v1/placements": func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusBadRequest)
				json.NewEncoder(w).Encode(map[string]interface{}{
					"error": "Bad request",
				})
			},
		})
		defer sandboxServer.Close()

		anarchyServer, _ := newTestAnarchyServer(t)
		defer anarchyServer.Close()

		rc := newTestRunContext(t, anarchyServer)
		withSandboxEnabled(rc, sandboxServer, "test-uuid-123")

		result, err := sandboxBook(rc, "access-token")
		if err == nil {
			t.Fatal("expected error for status 400, got nil")
		}

		if result.Status != "error" {
			t.Errorf("Status = %s, want 'error'", result.Status)
		}
	})
}

// --- TestSandboxCleanup ---

func TestSandboxCleanup(t *testing.T) {
	t.Run("no UUID - skips without error", func(t *testing.T) {
		anarchyServer, _ := newTestAnarchyServer(t)
		defer anarchyServer.Close()

		rc := newTestRunContext(t, anarchyServer)

		err := sandboxCleanup(rc)
		if err != nil {
			t.Fatalf("sandboxCleanup() error = %v, want nil", err)
		}
	})

	t.Run("no login token - skips without error", func(t *testing.T) {
		anarchyServer, _ := newTestAnarchyServer(t)
		defer anarchyServer.Close()

		rc := newTestRunContext(t, anarchyServer)
		setNested(rc.Payload.Subject, "test-uuid", "spec", "vars", "job_vars", "uuid")

		err := sandboxCleanup(rc)
		if err != nil {
			t.Fatalf("sandboxCleanup() error = %v, want nil", err)
		}
	})

	t.Run("success - releases placement", func(t *testing.T) {
		var deleteCalled bool
		sandboxServer := newSimpleSandboxServer(t, map[string]http.HandlerFunc{
			"/api/v1/login": func(w http.ResponseWriter, r *http.Request) {
				json.NewEncoder(w).Encode(map[string]string{"access_token": "access-token"})
			},
			"/api/v1/placements/": func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodDelete {
					deleteCalled = true
					w.WriteHeader(http.StatusOK)
				}
			},
		})
		defer sandboxServer.Close()

		anarchyServer, _ := newTestAnarchyServer(t)
		defer anarchyServer.Close()

		rc := newTestRunContext(t, anarchyServer)
		withSandboxEnabled(rc, sandboxServer, "test-uuid-123")

		err := sandboxCleanup(rc)
		if err != nil {
			t.Fatalf("sandboxCleanup() error = %v", err)
		}

		if !deleteCalled {
			t.Error("expected DELETE to be called")
		}
	})

	t.Run("release failure - returns error", func(t *testing.T) {
		sandboxServer := newSimpleSandboxServer(t, map[string]http.HandlerFunc{
			"/api/v1/login": func(w http.ResponseWriter, r *http.Request) {
				json.NewEncoder(w).Encode(map[string]string{"access_token": "access-token"})
			},
			"/api/v1/placements/": func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodDelete {
					w.WriteHeader(http.StatusInternalServerError)
				}
			},
		})
		defer sandboxServer.Close()

		anarchyServer, _ := newTestAnarchyServer(t)
		defer anarchyServer.Close()

		rc := newTestRunContext(t, anarchyServer)
		withSandboxEnabled(rc, sandboxServer, "test-uuid-123")

		err := sandboxCleanup(rc)
		if err == nil {
			t.Fatal("expected error when release fails, got nil")
		}
		if !strings.Contains(err.Error(), "release placement") {
			t.Errorf("error = %v, want 'release placement' error", err)
		}
	})
}

// --- TestSandboxStart ---

func TestSandboxStart(t *testing.T) {
	t.Run("no UUID - returns error", func(t *testing.T) {
		anarchyServer, _ := newTestAnarchyServer(t)
		defer anarchyServer.Close()

		rc := newTestRunContext(t, anarchyServer)

		err := sandboxStart(rc)
		if err == nil {
			t.Fatal("expected error when no uuid, got nil")
		}
		if !strings.Contains(err.Error(), "no uuid") {
			t.Errorf("error = %v, want 'no uuid'", err)
		}
	})

	t.Run("success with request_id - polls and returns nil", func(t *testing.T) {
		sandboxServer := newSimpleSandboxServer(t, map[string]http.HandlerFunc{
			"/api/v1/login": func(w http.ResponseWriter, r *http.Request) {
				json.NewEncoder(w).Encode(map[string]string{"access_token": "access-token"})
			},
			"/api/v1/placements/test-uuid-123/start": func(w http.ResponseWriter, r *http.Request) {
				json.NewEncoder(w).Encode(map[string]interface{}{
					"request_id": "req-start-1",
				})
			},
			"/api/v1/requests/req-start-1/status": func(w http.ResponseWriter, r *http.Request) {
				json.NewEncoder(w).Encode(map[string]interface{}{
					"status": "success",
				})
			},
		})
		defer sandboxServer.Close()

		anarchyServer, _ := newTestAnarchyServer(t)
		defer anarchyServer.Close()

		rc := newTestRunContext(t, anarchyServer)
		withSandboxEnabled(rc, sandboxServer, "test-uuid-123")

		err := sandboxStart(rc)
		if err != nil {
			t.Fatalf("sandboxStart() error = %v", err)
		}
	})

	t.Run("no request_id in response - immediate success", func(t *testing.T) {
		sandboxServer := newSimpleSandboxServer(t, map[string]http.HandlerFunc{
			"/api/v1/login": func(w http.ResponseWriter, r *http.Request) {
				json.NewEncoder(w).Encode(map[string]string{"access_token": "access-token"})
			},
			"/api/v1/placements/test-uuid-123/start": func(w http.ResponseWriter, r *http.Request) {
				json.NewEncoder(w).Encode(map[string]interface{}{
					"status": "started",
				})
			},
		})
		defer sandboxServer.Close()

		anarchyServer, _ := newTestAnarchyServer(t)
		defer anarchyServer.Close()

		rc := newTestRunContext(t, anarchyServer)
		withSandboxEnabled(rc, sandboxServer, "test-uuid-123")

		err := sandboxStart(rc)
		if err != nil {
			t.Fatalf("sandboxStart() error = %v", err)
		}
	})

	t.Run("login failure - returns error", func(t *testing.T) {
		sandboxServer := newSimpleSandboxServer(t, map[string]http.HandlerFunc{
			"/api/v1/login": func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
			},
		})
		defer sandboxServer.Close()

		anarchyServer, _ := newTestAnarchyServer(t)
		defer anarchyServer.Close()

		rc := newTestRunContext(t, anarchyServer)
		withSandboxEnabled(rc, sandboxServer, "test-uuid-123")

		err := sandboxStart(rc)
		if err != nil && !strings.Contains(err.Error(), "sandbox login") {
			t.Errorf("error = %v, want 'sandbox login' error", err)
		}
	})
}

// --- TestSandboxStop ---

func TestSandboxStop(t *testing.T) {
	t.Run("no UUID - returns error", func(t *testing.T) {
		anarchyServer, _ := newTestAnarchyServer(t)
		defer anarchyServer.Close()

		rc := newTestRunContext(t, anarchyServer)

		err := sandboxStop(rc)
		if err == nil {
			t.Fatal("expected error when no uuid, got nil")
		}
		if !strings.Contains(err.Error(), "no uuid") {
			t.Errorf("error = %v, want 'no uuid'", err)
		}
	})

	t.Run("success with request_id - polls and returns nil", func(t *testing.T) {
		sandboxServer := newSimpleSandboxServer(t, map[string]http.HandlerFunc{
			"/api/v1/login": func(w http.ResponseWriter, r *http.Request) {
				json.NewEncoder(w).Encode(map[string]string{"access_token": "access-token"})
			},
			"/api/v1/placements/test-uuid-123/stop": func(w http.ResponseWriter, r *http.Request) {
				json.NewEncoder(w).Encode(map[string]interface{}{
					"request_id": "req-stop-1",
				})
			},
			"/api/v1/requests/req-stop-1/status": func(w http.ResponseWriter, r *http.Request) {
				json.NewEncoder(w).Encode(map[string]interface{}{
					"status": "complete",
				})
			},
		})
		defer sandboxServer.Close()

		anarchyServer, _ := newTestAnarchyServer(t)
		defer anarchyServer.Close()

		rc := newTestRunContext(t, anarchyServer)
		withSandboxEnabled(rc, sandboxServer, "test-uuid-123")

		err := sandboxStop(rc)
		if err != nil {
			t.Fatalf("sandboxStop() error = %v", err)
		}
	})

	t.Run("login failure - returns error", func(t *testing.T) {
		sandboxServer := newSimpleSandboxServer(t, map[string]http.HandlerFunc{
			"/api/v1/login": func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
			},
		})
		defer sandboxServer.Close()

		anarchyServer, _ := newTestAnarchyServer(t)
		defer anarchyServer.Close()

		rc := newTestRunContext(t, anarchyServer)
		withSandboxEnabled(rc, sandboxServer, "test-uuid-123")

		err := sandboxStop(rc)
		if err != nil && !strings.Contains(err.Error(), "sandbox login") {
			t.Errorf("error = %v, want 'sandbox login' error", err)
		}
	})
}

// --- TestPollSandboxRequest ---

func TestPollSandboxRequest(t *testing.T) {
	t.Run("status success - returns nil", func(t *testing.T) {
		sandboxServer := newSimpleSandboxServer(t, map[string]http.HandlerFunc{
			"/api/v1/requests/req-1/status": func(w http.ResponseWriter, r *http.Request) {
				json.NewEncoder(w).Encode(map[string]interface{}{
					"status": "success",
				})
			},
		})
		defer sandboxServer.Close()

		client := NewSandboxAPIClient(sandboxServer.URL)
		err := pollSandboxRequest(client, "access-token", "req-1")
		if err != nil {
			t.Fatalf("pollSandboxRequest() error = %v", err)
		}
	})

	t.Run("status complete - returns nil", func(t *testing.T) {
		sandboxServer := newSimpleSandboxServer(t, map[string]http.HandlerFunc{
			"/api/v1/requests/req-2/status": func(w http.ResponseWriter, r *http.Request) {
				json.NewEncoder(w).Encode(map[string]interface{}{
					"status": "complete",
				})
			},
		})
		defer sandboxServer.Close()

		client := NewSandboxAPIClient(sandboxServer.URL)
		err := pollSandboxRequest(client, "access-token", "req-2")
		if err != nil {
			t.Fatalf("pollSandboxRequest() error = %v", err)
		}
	})

	t.Run("status error - returns error with message", func(t *testing.T) {
		sandboxServer := newSimpleSandboxServer(t, map[string]http.HandlerFunc{
			"/api/v1/requests/req-3/status": func(w http.ResponseWriter, r *http.Request) {
				json.NewEncoder(w).Encode(map[string]interface{}{
					"status":  "error",
					"message": "Something went wrong",
				})
			},
		})
		defer sandboxServer.Close()

		client := NewSandboxAPIClient(sandboxServer.URL)
		err := pollSandboxRequest(client, "access-token", "req-3")
		if err == nil {
			t.Fatal("expected error for status 'error', got nil")
		}
		if !strings.Contains(err.Error(), "Something went wrong") {
			t.Errorf("error = %v, want 'Something went wrong'", err)
		}
	})

	t.Run("status failed - returns error", func(t *testing.T) {
		sandboxServer := newSimpleSandboxServer(t, map[string]http.HandlerFunc{
			"/api/v1/requests/req-4/status": func(w http.ResponseWriter, r *http.Request) {
				json.NewEncoder(w).Encode(map[string]interface{}{
					"status":  "failed",
					"message": "Operation failed",
				})
			},
		})
		defer sandboxServer.Close()

		client := NewSandboxAPIClient(sandboxServer.URL)
		err := pollSandboxRequest(client, "access-token", "req-4")
		if err == nil {
			t.Fatal("expected error for status 'failed', got nil")
		}
		if !strings.Contains(err.Error(), "failed") {
			t.Errorf("error = %v, want 'failed' error", err)
		}
	})
}

// --- TestExtractSandboxVars ---

func TestExtractSandboxVars(t *testing.T) {
	t.Run("empty resources - empty vars and labels", func(t *testing.T) {
		placement := map[string]interface{}{}

		vars, labels := extractSandboxVars(placement)
		if len(vars) != 0 {
			t.Errorf("expected empty vars, got %d items", len(vars))
		}
		if len(labels) != 0 {
			t.Errorf("expected empty labels, got %d items", len(labels))
		}
	})

	t.Run("non-array resources - empty", func(t *testing.T) {
		placement := map[string]interface{}{
			"resources": "not-an-array",
		}

		vars, labels := extractSandboxVars(placement)
		if len(vars) != 0 {
			t.Errorf("expected empty vars, got %d items", len(vars))
		}
		if len(labels) != 0 {
			t.Errorf("expected empty labels, got %d items", len(labels))
		}
	})

	t.Run("single resource with credentials - credentials flattened", func(t *testing.T) {
		placement := map[string]interface{}{
			"resources": []interface{}{
				map[string]interface{}{
					"name":  "my-sandbox",
					"kind":  "AwsSandbox",
					"cloud": "aws",
					"credentials": map[string]interface{}{
						"aws_access_key_id":     "AKIA123",
						"aws_secret_access_key": "secret",
						"aws_default_region":    "us-west-2",
					},
				},
			},
		}

		vars, labels := extractSandboxVars(placement)

		if vars["aws_access_key_id"] != "AKIA123" {
			t.Errorf("aws_access_key_id = %v, want 'AKIA123'", vars["aws_access_key_id"])
		}
		if vars["aws_secret_access_key"] != "secret" {
			t.Errorf("aws_secret_access_key = %v, want 'secret'", vars["aws_secret_access_key"])
		}
		if vars["aws_default_region"] != "us-west-2" {
			t.Errorf("aws_default_region = %v, want 'us-west-2'", vars["aws_default_region"])
		}
		if vars["sandbox_name_0"] != "my-sandbox" {
			t.Errorf("sandbox_name_0 = %v, want 'my-sandbox'", vars["sandbox_name_0"])
		}
		if vars["sandbox_kind_0"] != "AwsSandbox" {
			t.Errorf("sandbox_kind_0 = %v, want 'AwsSandbox'", vars["sandbox_kind_0"])
		}
		if vars["sandbox_cloud_0"] != "aws" {
			t.Errorf("sandbox_cloud_0 = %v, want 'aws'", vars["sandbox_cloud_0"])
		}

		if labels["sandbox"] != "my-sandbox" {
			t.Errorf("sandbox label = %s, want 'my-sandbox'", labels["sandbox"])
		}
	})

	t.Run("multiple resources - all extracted, first name becomes label", func(t *testing.T) {
		placement := map[string]interface{}{
			"resources": []interface{}{
				map[string]interface{}{
					"name":  "sandbox-1",
					"kind":  "AwsSandbox",
					"cloud": "aws",
				},
				map[string]interface{}{
					"name":  "sandbox-2",
					"kind":  "GcpSandbox",
					"cloud": "gcp",
				},
			},
		}

		vars, labels := extractSandboxVars(placement)

		if vars["sandbox_name_0"] != "sandbox-1" {
			t.Errorf("sandbox_name_0 = %v, want 'sandbox-1'", vars["sandbox_name_0"])
		}
		if vars["sandbox_name_1"] != "sandbox-2" {
			t.Errorf("sandbox_name_1 = %v, want 'sandbox-2'", vars["sandbox_name_1"])
		}
		if vars["sandbox_kind_0"] != "AwsSandbox" {
			t.Errorf("sandbox_kind_0 = %v, want 'AwsSandbox'", vars["sandbox_kind_0"])
		}
		if vars["sandbox_kind_1"] != "GcpSandbox" {
			t.Errorf("sandbox_kind_1 = %v, want 'GcpSandbox'", vars["sandbox_kind_1"])
		}

		if labels["sandbox"] != "sandbox-1" {
			t.Errorf("sandbox label = %s, want 'sandbox-1'", labels["sandbox"])
		}
	})

	t.Run("resource with hosted_zone_id and account_id fields", func(t *testing.T) {
		placement := map[string]interface{}{
			"resources": []interface{}{
				map[string]interface{}{
					"name":           "aws-sandbox",
					"hosted_zone_id": "Z1234567890ABC",
					"account_id":     "123456789012",
					"zone":           "us-east-1a",
					"project_id":     "my-project-123",
				},
			},
		}

		vars, labels := extractSandboxVars(placement)

		if vars["hosted_zone_id"] != "Z1234567890ABC" {
			t.Errorf("hosted_zone_id = %v, want 'Z1234567890ABC'", vars["hosted_zone_id"])
		}
		if vars["account_id"] != "123456789012" {
			t.Errorf("account_id = %v, want '123456789012'", vars["account_id"])
		}
		if vars["zone"] != "us-east-1a" {
			t.Errorf("zone = %v, want 'us-east-1a'", vars["zone"])
		}
		if vars["project_id"] != "my-project-123" {
			t.Errorf("project_id = %v, want 'my-project-123'", vars["project_id"])
		}

		if labels["sandbox"] != "aws-sandbox" {
			t.Errorf("sandbox label = %s, want 'aws-sandbox'", labels["sandbox"])
		}
	})
}
