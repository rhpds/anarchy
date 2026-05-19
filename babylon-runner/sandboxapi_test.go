package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSandboxAPILogin(t *testing.T) {
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

	client := NewSandboxAPIClient(server.URL)
	token, err := client.Login("my-login-token")
	if err != nil {
		t.Fatalf("Login returned error: %v", err)
	}
	if token != "access-abc123" {
		t.Errorf("token = %q, want %q", token, "access-abc123")
	}
}

func TestSandboxAPIGetPlacement(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method = %s, want GET", r.Method)
		}
		if r.URL.Path != "/api/v1/placements/uuid-123" {
			t.Errorf("path = %s, want /api/v1/placements/uuid-123", r.URL.Path)
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
	}))
	defer server.Close()

	client := NewSandboxAPIClient(server.URL)
	result, statusCode, err := client.GetPlacement("access-token", "uuid-123")
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
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	client := NewSandboxAPIClient(server.URL)
	result, statusCode, err := client.GetPlacement("access-token", "missing-uuid")
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
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/api/v1/placements" {
			t.Errorf("path = %s, want /api/v1/placements", r.URL.Path)
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
	}))
	defer server.Close()

	client := NewSandboxAPIClient(server.URL)
	reqBody := map[string]interface{}{
		"service_uuid": "svc-001",
	}
	result, statusCode, err := client.BookPlacement("access-token", reqBody)
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
		if r.Method != http.MethodPut {
			t.Errorf("method = %s, want PUT", r.Method)
		}
		if r.URL.Path != "/api/v1/placements/uuid-123/start" {
			t.Errorf("path = %s, want /api/v1/placements/uuid-123/start", r.URL.Path)
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
	}))
	defer server.Close()

	client := NewSandboxAPIClient(server.URL)
	result, err := client.StartPlacement("access-token", "uuid-123")
	if err != nil {
		t.Fatalf("StartPlacement returned error: %v", err)
	}
	if result["status"] != "starting" {
		t.Errorf("status = %v, want %q", result["status"], "starting")
	}
}

func TestSandboxAPIStopPlacement(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Errorf("method = %s, want PUT", r.Method)
		}
		if r.URL.Path != "/api/v1/placements/uuid-123/stop" {
			t.Errorf("path = %s, want /api/v1/placements/uuid-123/stop", r.URL.Path)
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
	}))
	defer server.Close()

	client := NewSandboxAPIClient(server.URL)
	result, err := client.StopPlacement("access-token", "uuid-123")
	if err != nil {
		t.Fatalf("StopPlacement returned error: %v", err)
	}
	if result["status"] != "stopping" {
		t.Errorf("status = %v, want %q", result["status"], "stopping")
	}
}

func TestSandboxAPIReleasePlacement(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("method = %s, want DELETE", r.Method)
		}
		if r.URL.Path != "/api/v1/placements/uuid-123" {
			t.Errorf("path = %s, want /api/v1/placements/uuid-123", r.URL.Path)
		}
		want := "Bearer access-token"
		if got := r.Header.Get("Authorization"); got != want {
			t.Errorf("Authorization = %q, want %q", got, want)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewSandboxAPIClient(server.URL)
	err := client.ReleasePlacement("access-token", "uuid-123")
	if err != nil {
		t.Fatalf("ReleasePlacement returned error: %v", err)
	}
}
