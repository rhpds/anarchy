package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestTowerSelectControllerRandom(t *testing.T) {
	controllers := []map[string]interface{}{
		{"hostname": "ctrl-1", "active_job_count": float64(5)},
		{"hostname": "ctrl-2", "active_job_count": float64(3)},
		{"hostname": "ctrl-3", "active_job_count": float64(1)},
	}

	picked := selectController(controllers, "random")
	if picked == nil {
		t.Fatal("selectController returned nil")
	}

	hostname, ok := picked["hostname"].(string)
	if !ok {
		t.Fatal("hostname is not a string")
	}
	found := false
	for _, c := range controllers {
		if c["hostname"] == hostname {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("picked hostname %q not in controllers", hostname)
	}
}

func TestTowerSelectControllerFirstAvailable(t *testing.T) {
	controllers := []map[string]interface{}{
		{"hostname": "ctrl-1", "active_job_count": float64(5)},
		{"hostname": "ctrl-2", "active_job_count": float64(3)},
	}

	picked := selectController(controllers, "first-available")
	if picked == nil {
		t.Fatal("selectController returned nil")
	}
	if got := picked["hostname"]; got != "ctrl-1" {
		t.Errorf("hostname = %v, want ctrl-1", got)
	}
}

func TestTowerSelectControllerBalance(t *testing.T) {
	controllers := []map[string]interface{}{
		{"hostname": "ctrl-1", "active_job_count": float64(10)},
		{"hostname": "ctrl-2", "active_job_count": float64(2)},
		{"hostname": "ctrl-3", "active_job_count": float64(5)},
	}

	picked := selectController(controllers, "balance")
	if picked == nil {
		t.Fatal("selectController returned nil")
	}
	if got := picked["hostname"]; got != "ctrl-2" {
		t.Errorf("hostname = %v, want ctrl-2 (lowest active_job_count)", got)
	}
}

func TestTowerGetJobStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method = %s, want GET", r.Method)
		}
		if r.URL.Path != "/api/v2/jobs/42/" {
			t.Errorf("path = %s, want /api/v2/jobs/42/", r.URL.Path)
		}
		wantAuth := "Bearer test-oauth-token"
		if got := r.Header.Get("Authorization"); got != wantAuth {
			t.Errorf("Authorization = %q, want %q", got, wantAuth)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"id":     float64(42),
			"status": "successful",
		})
	}))
	defer server.Close()

	tc := NewTowerClient("unused", "user", "pass")
	tc.baseURL = server.URL

	status, err := tc.GetJobStatus("test-oauth-token", 42)
	if err != nil {
		t.Fatalf("GetJobStatus returned error: %v", err)
	}
	if got := status["status"]; got != "successful" {
		t.Errorf("status = %v, want successful", got)
	}
}

func TestTowerCancelJob(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/api/v2/jobs/99/cancel/" {
			t.Errorf("path = %s, want /api/v2/jobs/99/cancel/", r.URL.Path)
		}
		wantAuth := "Bearer cancel-token"
		if got := r.Header.Get("Authorization"); got != wantAuth {
			t.Errorf("Authorization = %q, want %q", got, wantAuth)
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	tc := NewTowerClient("unused", "user", "pass")
	tc.baseURL = server.URL

	if err := tc.CancelJob("cancel-token", 99); err != nil {
		t.Fatalf("CancelJob returned error: %v", err)
	}
}

func TestTowerLaunchJob(t *testing.T) {
	// Track which endpoints were called and return appropriate responses.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch {
		// CreateOAuthToken
		case r.Method == http.MethodPost && r.URL.Path == "/api/v2/tokens/":
			user, pass, ok := r.BasicAuth()
			if !ok || user != "admin" || pass != "secret" {
				t.Errorf("BasicAuth = (%q, %q, %v), want (admin, secret, true)", user, pass, ok)
			}
			json.NewEncoder(w).Encode(map[string]interface{}{
				"token": "oauth-tok",
				"id":    float64(7),
			})

		// DeleteOAuthToken
		case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/api/v2/tokens/"):
			if r.URL.Path != "/api/v2/tokens/7/" {
				t.Errorf("delete token path = %s, want /api/v2/tokens/7/", r.URL.Path)
			}
			w.WriteHeader(http.StatusNoContent)

		// Create organization
		case r.Method == http.MethodPost && r.URL.Path == "/api/v2/organizations/":
			json.NewEncoder(w).Encode(map[string]interface{}{"id": float64(1)})

		// Create inventory
		case r.Method == http.MethodPost && r.URL.Path == "/api/v2/inventories/":
			var body map[string]interface{}
			json.NewDecoder(r.Body).Decode(&body)
			if body["organization"] != float64(1) {
				t.Errorf("inventory organization = %v, want 1", body["organization"])
			}
			json.NewEncoder(w).Encode(map[string]interface{}{"id": float64(2)})

		// Create project
		case r.Method == http.MethodPost && r.URL.Path == "/api/v2/projects/":
			var body map[string]interface{}
			json.NewDecoder(r.Body).Decode(&body)
			if body["organization"] != float64(1) {
				t.Errorf("project organization = %v, want 1", body["organization"])
			}
			json.NewEncoder(w).Encode(map[string]interface{}{"id": float64(3)})

		// Create job template
		case r.Method == http.MethodPost && r.URL.Path == "/api/v2/job_templates/":
			var body map[string]interface{}
			json.NewDecoder(r.Body).Decode(&body)
			if body["inventory"] != float64(2) {
				t.Errorf("template inventory = %v, want 2", body["inventory"])
			}
			if body["project"] != float64(3) {
				t.Errorf("template project = %v, want 3", body["project"])
			}
			json.NewEncoder(w).Encode(map[string]interface{}{"id": float64(4)})

		// Launch job template
		case r.Method == http.MethodPost && r.URL.Path == "/api/v2/job_templates/4/launch/":
			wantAuth := "Bearer oauth-tok"
			if got := r.Header.Get("Authorization"); got != wantAuth {
				t.Errorf("launch Authorization = %q, want %q", got, wantAuth)
			}
			json.NewEncoder(w).Encode(map[string]interface{}{"id": float64(555)})

		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	tc := NewTowerClient("unused", "admin", "secret")
	tc.baseURL = server.URL

	jobConfig := TowerJobConfig{
		Organization:  "test-org",
		Inventory:     "test-inv",
		ProjectSCMURL: "https://git.example.com/repo.git",
		ProjectSCMRef: "main",
		TemplateName:  "deploy",
		Playbook:      "site.yml",
		ExtraVars:     map[string]interface{}{"env": "prod"},
		Timeout:       600,
	}

	jobID, err := tc.LaunchJob(jobConfig)
	if err != nil {
		t.Fatalf("LaunchJob returned error: %v", err)
	}
	if jobID != 555 {
		t.Errorf("jobID = %d, want 555", jobID)
	}
}

func TestTowerCreateOAuthToken(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/api/v2/tokens/" {
			t.Errorf("path = %s, want /api/v2/tokens/", r.URL.Path)
		}
		user, pass, ok := r.BasicAuth()
		if !ok || user != "admin" || pass != "secret" {
			t.Errorf("BasicAuth = (%q, %q, %v), want (admin, secret, true)", user, pass, ok)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"token": "my-token",
			"id":    float64(42),
		})
	}))
	defer server.Close()

	tc := NewTowerClient("unused", "admin", "secret")
	tc.baseURL = server.URL

	token, tokenID, err := tc.CreateOAuthToken()
	if err != nil {
		t.Fatalf("CreateOAuthToken returned error: %v", err)
	}
	if token != "my-token" {
		t.Errorf("token = %q, want my-token", token)
	}
	if tokenID != 42 {
		t.Errorf("tokenID = %d, want 42", tokenID)
	}
}

func TestTowerDeleteOAuthToken(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("method = %s, want DELETE", r.Method)
		}
		wantPath := "/api/v2/tokens/42/"
		if r.URL.Path != wantPath {
			t.Errorf("path = %s, want %s", r.URL.Path, wantPath)
		}
		user, pass, ok := r.BasicAuth()
		if !ok || user != "admin" || pass != "secret" {
			t.Errorf("BasicAuth = (%q, %q, %v), want (admin, secret, true)", user, pass, ok)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	tc := NewTowerClient("unused", "admin", "secret")
	tc.baseURL = server.URL

	if err := tc.DeleteOAuthToken(42); err != nil {
		t.Fatalf("DeleteOAuthToken returned error: %v", err)
	}
}

func TestTowerEnsureResource(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/api/v2/organizations/" {
			t.Errorf("path = %s, want /api/v2/organizations/", r.URL.Path)
		}
		wantAuth := "Bearer my-token"
		if got := r.Header.Get("Authorization"); got != wantAuth {
			t.Errorf("Authorization = %q, want %q", got, wantAuth)
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"id": 10, "name": "org1"}`)
	}))
	defer server.Close()

	tc := NewTowerClient("unused", "admin", "secret")
	tc.baseURL = server.URL

	id, err := tc.ensureResource("my-token", "/api/v2/organizations/", map[string]interface{}{
		"name": "org1",
	})
	if err != nil {
		t.Fatalf("ensureResource returned error: %v", err)
	}
	if id != 10 {
		t.Errorf("id = %d, want 10", id)
	}
}
