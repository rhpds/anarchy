package clients

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSandboxAPILoginViaTokenCache(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/api/v1/login" {
			t.Errorf("path = %s, want /api/v1/login", r.URL.Path)
		}
		want := "Bearer my-login-token"
		if got := r.Header.Get("Authorization"); got != want {
			t.Errorf("Authorization = %q, want %q", got, want)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"access_token": "access-abc123",
		})
	}))
	defer server.Close()

	client := NewSandboxAPIClient(server.URL, "my-login-token", WithNoRetries())
	defer client.Close(context.Background())

	// Token is obtained implicitly on first method call.
	// Verify by calling a simple method that exercises the token cache.
	token, err := client.accessToken(context.Background())
	if err != nil {
		t.Fatalf("accessToken returned error: %v", err)
	}
	if token != "access-abc123" {
		t.Errorf("token = %q, want %q", token, "access-abc123")
	}
}

func TestSandboxAPIGetPlacement(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/login":
			json.NewEncoder(w).Encode(map[string]string{"access_token": "access-token"})
		case "/api/v1/placements/uuid-123":
			if r.Method != http.MethodGet {
				t.Errorf("method = %s, want GET", r.Method)
			}
			want := "Bearer access-token"
			if got := r.Header.Get("Authorization"); got != want {
				t.Errorf("Authorization = %q, want %q", got, want)
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{
				"uuid":   "uuid-123",
				"status": "ready",
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := NewSandboxAPIClient(server.URL, "login-token", WithNoRetries())
	defer client.Close(context.Background())

	result, statusCode, err := client.GetPlacement(context.Background(), "uuid-123")
	if err != nil {
		t.Fatalf("GetPlacement returned error: %v", err)
	}
	if statusCode != 200 {
		t.Errorf("statusCode = %d, want 200", statusCode)
	}
	if result["uuid"] != "uuid-123" {
		t.Errorf("uuid = %v, want %q", result["uuid"], "uuid-123")
	}
	if result["status"] != "ready" {
		t.Errorf("status = %v, want %q", result["status"], "ready")
	}
}

func TestSandboxAPIGetPlacement404(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/login":
			json.NewEncoder(w).Encode(map[string]string{"access_token": "access-token"})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	client := NewSandboxAPIClient(server.URL, "login-token", WithNoRetries())
	defer client.Close(context.Background())

	result, statusCode, err := client.GetPlacement(context.Background(), "missing-uuid")
	if err != nil {
		t.Fatalf("GetPlacement 404 should not return error, got: %v", err)
	}
	if statusCode != 404 {
		t.Errorf("statusCode = %d, want 404", statusCode)
	}
	if result != nil {
		t.Errorf("result = %v, want nil on 404", result)
	}
}

func TestSandboxAPIBookPlacement(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/login":
			json.NewEncoder(w).Encode(map[string]string{"access_token": "access-token"})
		case "/api/v1/placements":
			if r.Method != http.MethodPost {
				t.Errorf("method = %s, want POST", r.Method)
			}
			// Verify request body.
			var body map[string]interface{}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode body: %v", err)
			}
			if body["service_uuid"] != "svc-001" {
				t.Errorf("service_uuid = %v, want %q", body["service_uuid"], "svc-001")
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusAccepted)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"uuid":       "placement-456",
				"request_id": "req-789",
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := NewSandboxAPIClient(server.URL, "login-token", WithNoRetries())
	defer client.Close(context.Background())

	reqBody := map[string]interface{}{
		"service_uuid": "svc-001",
	}
	result, statusCode, err := client.BookPlacement(context.Background(), reqBody)
	if err != nil {
		t.Fatalf("BookPlacement returned error: %v", err)
	}
	if statusCode != 202 {
		t.Errorf("statusCode = %d, want 202", statusCode)
	}
	if result["uuid"] != "placement-456" {
		t.Errorf("uuid = %v, want %q", result["uuid"], "placement-456")
	}
}

func TestSandboxAPIStartPlacement(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/login":
			json.NewEncoder(w).Encode(map[string]string{"access_token": "access-token"})
		case "/api/v1/placements/uuid-123/start":
			if r.Method != http.MethodPut {
				t.Errorf("method = %s, want PUT", r.Method)
			}
			want := "Bearer access-token"
			if got := r.Header.Get("Authorization"); got != want {
				t.Errorf("Authorization = %q, want %q", got, want)
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{
				"uuid":   "uuid-123",
				"status": "starting",
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := NewSandboxAPIClient(server.URL, "login-token", WithNoRetries())
	defer client.Close(context.Background())

	result, err := client.StartPlacement(context.Background(), "uuid-123")
	if err != nil {
		t.Fatalf("StartPlacement returned error: %v", err)
	}
	if result["status"] != "starting" {
		t.Errorf("status = %v, want %q", result["status"], "starting")
	}
}

func TestSandboxAPIStopPlacement(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/login":
			json.NewEncoder(w).Encode(map[string]string{"access_token": "access-token"})
		case "/api/v1/placements/uuid-123/stop":
			if r.Method != http.MethodPut {
				t.Errorf("method = %s, want PUT", r.Method)
			}
			want := "Bearer access-token"
			if got := r.Header.Get("Authorization"); got != want {
				t.Errorf("Authorization = %q, want %q", got, want)
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{
				"uuid":   "uuid-123",
				"status": "stopping",
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := NewSandboxAPIClient(server.URL, "login-token", WithNoRetries())
	defer client.Close(context.Background())

	result, err := client.StopPlacement(context.Background(), "uuid-123")
	if err != nil {
		t.Fatalf("StopPlacement returned error: %v", err)
	}
	if result["status"] != "stopping" {
		t.Errorf("status = %v, want %q", result["status"], "stopping")
	}
}

func TestSandboxAPIReleasePlacement(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/login":
			json.NewEncoder(w).Encode(map[string]string{"access_token": "access-token"})
		case "/api/v1/placements/uuid-123":
			if r.Method != http.MethodDelete {
				t.Errorf("method = %s, want DELETE", r.Method)
			}
			want := "Bearer access-token"
			if got := r.Header.Get("Authorization"); got != want {
				t.Errorf("Authorization = %q, want %q", got, want)
			}
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := NewSandboxAPIClient(server.URL, "login-token", WithNoRetries())
	defer client.Close(context.Background())

	err := client.ReleasePlacement(context.Background(), "uuid-123")
	if err != nil {
		t.Fatalf("ReleasePlacement returned error: %v", err)
	}
}

func TestSandboxAPIGetRequestStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/login":
			json.NewEncoder(w).Encode(map[string]string{"access_token": "access-token"})
		case "/api/v1/requests/req-123/status":
			if r.Method != http.MethodGet {
				t.Errorf("method = %s, want GET", r.Method)
			}
			want := "Bearer access-token"
			if got := r.Header.Get("Authorization"); got != want {
				t.Errorf("Authorization = %q, want %q", got, want)
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{
				"request_id": "req-123",
				"status":     "completed",
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := NewSandboxAPIClient(server.URL, "login-token", WithNoRetries())
	defer client.Close(context.Background())

	result, err := client.GetRequestStatus(context.Background(), "req-123")
	if err != nil {
		t.Fatalf("GetRequestStatus returned error: %v", err)
	}
	if result["request_id"] != "req-123" {
		t.Errorf("request_id = %v, want %q", result["request_id"], "req-123")
	}
	if result["status"] != "completed" {
		t.Errorf("status = %v, want %q", result["status"], "completed")
	}
}

func TestSandboxAPIGetRequestStatusError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/login":
			json.NewEncoder(w).Encode(map[string]string{"access_token": "access-token"})
		default:
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer server.Close()

	client := NewSandboxAPIClient(server.URL, "login-token", WithNoRetries())
	defer client.Close(context.Background())

	result, err := client.GetRequestStatus(context.Background(), "req-123")
	if err == nil {
		t.Fatal("expected error for 500, got nil")
	}
	if result != nil {
		t.Errorf("result should be nil on error, got %v", result)
	}
}

func TestSandboxAPILoginMissingToken(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// Return response without access_token
		json.NewEncoder(w).Encode(map[string]string{
			"message": "login successful",
		})
	}))
	defer server.Close()

	client := NewSandboxAPIClient(server.URL, "my-login-token", WithNoRetries())
	defer client.Close(context.Background())

	// The login error should surface when we try to get a token.
	_, err := client.accessToken(context.Background())
	if err == nil {
		t.Fatal("expected error for missing access_token, got nil")
	}
}

func TestSandboxAPIGetPlacementError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/login":
			json.NewEncoder(w).Encode(map[string]string{"access_token": "access-token"})
		default:
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer server.Close()

	client := NewSandboxAPIClient(server.URL, "login-token", WithNoRetries())
	defer client.Close(context.Background())

	result, statusCode, err := client.GetPlacement(context.Background(), "uuid-123")
	if err == nil {
		t.Fatal("expected error for 500, got nil")
	}
	if statusCode != 500 {
		t.Errorf("statusCode = %d, want 500", statusCode)
	}
	if result != nil {
		t.Errorf("result should be nil on error, got %v", result)
	}
}

func TestSandboxAPIReleasePlacementError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/login":
			json.NewEncoder(w).Encode(map[string]string{"access_token": "access-token"})
		default:
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer server.Close()

	client := NewSandboxAPIClient(server.URL, "login-token", WithNoRetries())
	defer client.Close(context.Background())

	err := client.ReleasePlacement(context.Background(), "uuid-123")
	if err == nil {
		t.Fatal("expected error for 500, got nil")
	}
}

func TestSandboxAPIBookPlacementUnexpectedStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/login":
			json.NewEncoder(w).Encode(map[string]string{"access_token": "access-token"})
		default:
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer server.Close()

	client := NewSandboxAPIClient(server.URL, "login-token", WithNoRetries())
	defer client.Close(context.Background())

	_, _, err := client.BookPlacement(context.Background(), map[string]interface{}{"key": "val"})
	if err == nil {
		t.Fatal("expected error for unexpected status 500, got nil")
	}
}

func TestSandboxAPITokenCaching(t *testing.T) {
	var loginCount int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/login":
			loginCount++
			json.NewEncoder(w).Encode(map[string]string{"access_token": "access-token"})
		case "/api/v1/placements/uuid-1":
			json.NewEncoder(w).Encode(map[string]interface{}{"uuid": "uuid-1"})
		case "/api/v1/placements/uuid-2":
			json.NewEncoder(w).Encode(map[string]interface{}{"uuid": "uuid-2"})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := NewSandboxAPIClient(server.URL, "login-token", WithNoRetries())
	defer client.Close(context.Background())

	ctx := context.Background()

	// Two consecutive calls should reuse the same token.
	_, _, _ = client.GetPlacement(ctx, "uuid-1")
	_, _, _ = client.GetPlacement(ctx, "uuid-2")

	if loginCount != 1 {
		t.Errorf("login called %d times, want 1 (token should be cached)", loginCount)
	}
}
