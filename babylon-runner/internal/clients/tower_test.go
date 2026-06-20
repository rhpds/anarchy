package clients

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestInsertUnvaultStringNoVault(t *testing.T) {
	vars := map[string]interface{}{
		"env_type": "ocp4-cluster",
		"count":    float64(3),
		"nested": map[string]interface{}{
			"key": "plain-value",
		},
	}
	result := InsertUnvaultString(vars)

	if result["env_type"] != "ocp4-cluster" {
		t.Errorf("env_type = %v, want ocp4-cluster", result["env_type"])
	}
	if result["count"] != float64(3) {
		t.Errorf("count = %v, want 3", result["count"])
	}
	nested := result["nested"].(map[string]interface{})
	if nested["key"] != "plain-value" {
		t.Errorf("nested.key = %v, want plain-value", nested["key"])
	}
}

func TestInsertUnvaultStringSimple(t *testing.T) {
	vaultBlob := "$ANSIBLE_VAULT;1.1;AES256\n3836313733...\n"
	vars := map[string]interface{}{
		"env_type":   "ocp4-cluster",
		"secret_key": vaultBlob,
	}
	result := InsertUnvaultString(vars)

	// env_type should be unchanged.
	if result["env_type"] != "ocp4-cluster" {
		t.Errorf("env_type = %v, want ocp4-cluster", result["env_type"])
	}

	// secret_key should be replaced with Jinja2 lookup.
	secretVal, ok := result["secret_key"].(string)
	if !ok {
		t.Fatalf("secret_key is not a string: %T", result["secret_key"])
	}
	if !strings.HasPrefix(secretVal, "{{ lookup('unvault_string', __vaulted_value_") {
		t.Errorf("secret_key = %q, want Jinja2 lookup expression", secretVal)
	}
	if !strings.HasSuffix(secretVal, ") }}") {
		t.Errorf("secret_key = %q, missing closing ) }}", secretVal)
	}

	// Extract the variable name and verify the blob is stored there.
	varName := strings.TrimPrefix(secretVal, "{{ lookup('unvault_string', ")
	varName = strings.TrimSuffix(varName, ") }}")
	storedBlob, ok := result[varName].(string)
	if !ok {
		t.Fatalf("vaulted value key %q not found in result", varName)
	}
	if storedBlob != vaultBlob {
		t.Errorf("stored blob = %q, want original vault blob", storedBlob)
	}
}

func TestInsertUnvaultStringNested(t *testing.T) {
	vaultBlob1 := "$ANSIBLE_VAULT;1.1;AES256\naaa\n"
	vaultBlob2 := "  $ANSIBLE_VAULT;1.1;AES256\nbbb\n" // leading whitespace
	vars := map[string]interface{}{
		"top_level": "plain",
		"creds": map[string]interface{}{
			"access_key": "AKIA...",
			"secret_key": vaultBlob1,
		},
		"list_vals": []interface{}{
			"plain",
			vaultBlob2,
			float64(42),
		},
	}
	result := InsertUnvaultString(vars)

	// top_level unchanged.
	if result["top_level"] != "plain" {
		t.Errorf("top_level = %v, want plain", result["top_level"])
	}

	// Nested dict: access_key unchanged, secret_key replaced.
	creds := result["creds"].(map[string]interface{})
	if creds["access_key"] != "AKIA..." {
		t.Errorf("creds.access_key = %v, want AKIA...", creds["access_key"])
	}
	secretVal := creds["secret_key"].(string)
	if !strings.HasPrefix(secretVal, "{{ lookup('unvault_string', __vaulted_value_") {
		t.Errorf("creds.secret_key = %q, want Jinja2 lookup", secretVal)
	}

	// List: plain unchanged, vault replaced, number unchanged.
	list := result["list_vals"].([]interface{})
	if list[0] != "plain" {
		t.Errorf("list[0] = %v, want plain", list[0])
	}
	listVault := list[1].(string)
	if !strings.HasPrefix(listVault, "{{ lookup('unvault_string', __vaulted_value_") {
		t.Errorf("list[1] = %q, want Jinja2 lookup", listVault)
	}
	if list[2] != float64(42) {
		t.Errorf("list[2] = %v, want 42", list[2])
	}

	// Count __vaulted_value_ keys at top level — should be exactly 2.
	vaultedCount := 0
	for k := range result {
		if strings.HasPrefix(k, "__vaulted_value_") {
			vaultedCount++
		}
	}
	if vaultedCount != 2 {
		t.Errorf("vaulted value count = %d, want 2", vaultedCount)
	}
}

func TestInsertUnvaultStringMultipleSameValue(t *testing.T) {
	// Same vault blob used twice should get unique variable names.
	vaultBlob := "$ANSIBLE_VAULT;1.1;AES256\nsame\n"
	vars := map[string]interface{}{
		"key1": vaultBlob,
		"key2": vaultBlob,
	}
	result := InsertUnvaultString(vars)

	val1 := result["key1"].(string)
	val2 := result["key2"].(string)

	// Both should be lookups but with different variable names.
	if !strings.HasPrefix(val1, "{{ lookup('unvault_string', __vaulted_value_") {
		t.Errorf("key1 = %q, want Jinja2 lookup", val1)
	}
	if !strings.HasPrefix(val2, "{{ lookup('unvault_string', __vaulted_value_") {
		t.Errorf("key2 = %q, want Jinja2 lookup", val2)
	}
	if val1 == val2 {
		t.Errorf("key1 and key2 have the same lookup expression, should be unique")
	}
}

func TestTowerSelectControllerRandom(t *testing.T) {
	controllers := []map[string]interface{}{
		{"hostname": "ctrl-1", "active_job_count": float64(5)},
		{"hostname": "ctrl-2", "active_job_count": float64(3)},
		{"hostname": "ctrl-3", "active_job_count": float64(1)},
	}

	picked := SelectController(controllers, "random")
	if picked == nil {
		t.Fatal("SelectController returned nil")
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

	picked := SelectController(controllers, "first-available")
	if picked == nil {
		t.Fatal("SelectController returned nil")
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

	picked := SelectController(controllers, "balance")
	if picked == nil {
		t.Fatal("SelectController returned nil")
	}
	if got := picked["hostname"]; got != "ctrl-2" {
		t.Errorf("hostname = %v, want ctrl-2 (lowest active_job_count)", got)
	}
}

func TestTowerSelectControllerEmpty(t *testing.T) {
	picked := SelectController(nil, "random")
	if picked != nil {
		t.Errorf("SelectController(nil) = %v, want nil", picked)
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

	tc := NewTowerClient("unused", "user", "pass", nil)
	tc.baseURL = server.URL

	status, err := tc.GetJobStatus(context.Background(), "test-oauth-token", 42)
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

	tc := NewTowerClient("unused", "user", "pass", nil)
	tc.baseURL = server.URL

	if err := tc.CancelJob(context.Background(), "cancel-token", 99); err != nil {
		t.Fatalf("CancelJob returned error: %v", err)
	}
}

func TestTowerCancelJobAlreadyFinished(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 405 means the job already finished.
		w.WriteHeader(http.StatusMethodNotAllowed)
	}))
	defer server.Close()

	tc := NewTowerClient("unused", "user", "pass", nil)
	tc.baseURL = server.URL

	if err := tc.CancelJob(context.Background(), "token", 99); err != nil {
		t.Fatalf("CancelJob should not error on 405, got: %v", err)
	}
}

func TestTowerLaunchJob(t *testing.T) {
	// Track which endpoints were called and return appropriate responses.
	// EnsureResource does GET (search) then POST (create) for each resource.
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

		// Search requests (GET with query params) -> resource not found.
		case r.Method == http.MethodGet && r.URL.RawQuery != "":
			json.NewEncoder(w).Encode(map[string]interface{}{
				"count":   0,
				"results": []interface{}{},
			})

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
			if body["project"] != float64(3) {
				t.Errorf("template project = %v, want 3", body["project"])
			}
			json.NewEncoder(w).Encode(map[string]interface{}{"id": float64(4)})

		// SyncChildren GET: no existing associations.
		case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/job_templates/4/"):
			json.NewEncoder(w).Encode(map[string]interface{}{
				"results": []interface{}{},
			})

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

	tc := NewTowerClient("unused", "admin", "secret", nil)
	tc.baseURL = server.URL

	jobConfig := TowerJobConfig{
		Organization:          "test-org",
		Inventory:             "test-inv",
		ProjectSCMURL:         "https://git.example.com/repo.git",
		ProjectSCMRef:         "main",
		SCMUpdateOnLaunch:     true,
		SCMUpdateCacheTimeout: 30,
		SCMClean:              true,
		TemplateName:          "deploy",
		Playbook:              "site.yml",
		ExtraVars:             map[string]interface{}{"env": "prod"},
		Timeout:               600,
	}

	jobID, err := tc.LaunchJob(context.Background(), jobConfig)
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

	tc := NewTowerClient("unused", "admin", "secret", nil)
	tc.baseURL = server.URL

	token, tokenID, err := tc.CreateOAuthToken(context.Background())
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

	tc := NewTowerClient("unused", "admin", "secret", nil)
	tc.baseURL = server.URL

	if err := tc.DeleteOAuthToken(context.Background(), 42); err != nil {
		t.Fatalf("DeleteOAuthToken returned error: %v", err)
	}
}

func TestTowerEnsureResourceCreate(t *testing.T) {
	// EnsureResource: search returns nothing -> creates via POST.
	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		w.Header().Set("Content-Type", "application/json")

		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v2/organizations/":
			// Search returns no results.
			json.NewEncoder(w).Encode(map[string]interface{}{
				"count":   0,
				"results": []interface{}{},
			})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v2/organizations/":
			wantAuth := "Bearer my-token"
			if got := r.Header.Get("Authorization"); got != wantAuth {
				t.Errorf("Authorization = %q, want %q", got, wantAuth)
			}
			fmt.Fprint(w, `{"id": 10, "name": "org1"}`)
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	tc := NewTowerClient("unused", "admin", "secret", nil)
	tc.baseURL = server.URL

	id, err := tc.EnsureResource(context.Background(), "my-token", "/api/v2/organizations/", map[string]interface{}{
		"name": "org1",
	})
	if err != nil {
		t.Fatalf("EnsureResource returned error: %v", err)
	}
	if id != 10 {
		t.Errorf("id = %d, want 10", id)
	}
	if requestCount != 2 {
		t.Errorf("requestCount = %d, want 2 (GET search + POST create)", requestCount)
	}
}

func TestTowerEnsureResourceExisting(t *testing.T) {
	// EnsureResource: search finds existing resource -> returns ID without POST.
	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		w.Header().Set("Content-Type", "application/json")

		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v2/organizations/":
			// Search returns existing resource.
			json.NewEncoder(w).Encode(map[string]interface{}{
				"count": 1,
				"results": []interface{}{
					map[string]interface{}{"id": float64(42), "name": "org1"},
				},
			})
		default:
			t.Errorf("unexpected request: %s %s (should not POST when resource exists)", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	tc := NewTowerClient("unused", "admin", "secret", nil)
	tc.baseURL = server.URL

	id, err := tc.EnsureResource(context.Background(), "my-token", "/api/v2/organizations/", map[string]interface{}{
		"name": "org1",
	})
	if err != nil {
		t.Fatalf("EnsureResource returned error: %v", err)
	}
	if id != 42 {
		t.Errorf("id = %d, want 42 (existing resource)", id)
	}
	if requestCount != 1 {
		t.Errorf("requestCount = %d, want 1 (GET search only)", requestCount)
	}
}

func TestTowerLaunchJobWithEEAndCredentials(t *testing.T) {
	// Track resources created to verify EE, credentials, instance groups.
	var createdEE, associatedCreds, associatedIGs int
	var eeData map[string]interface{}
	var templateData map[string]interface{}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/v2/tokens/":
			json.NewEncoder(w).Encode(map[string]interface{}{
				"token": "oauth-tok", "id": float64(1),
			})
		case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/api/v2/tokens/"):
			w.WriteHeader(http.StatusNoContent)

		// GET list of children for SyncChildren (no existing associations).
		case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/job_templates/5/credentials/"):
			json.NewEncoder(w).Encode(map[string]interface{}{
				"results": []interface{}{},
			})
		case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/job_templates/5/instance_groups/"):
			json.NewEncoder(w).Encode(map[string]interface{}{
				"results": []interface{}{},
			})

		// Search requests -> return not found (except credentials).
		case r.Method == http.MethodGet && r.URL.RawQuery != "":
			// Return found result for static credential search.
			if strings.Contains(r.URL.Path, "credentials") && strings.Contains(r.URL.RawQuery, "my-cred") {
				json.NewEncoder(w).Encode(map[string]interface{}{
					"count": 1,
					"results": []interface{}{
						map[string]interface{}{"id": float64(50), "name": "my-cred"},
					},
				})
			} else if strings.Contains(r.URL.Path, "instance_groups") {
				json.NewEncoder(w).Encode(map[string]interface{}{
					"count": 1,
					"results": []interface{}{
						map[string]interface{}{"id": float64(60), "name": "my-ig"},
					},
				})
			} else {
				json.NewEncoder(w).Encode(map[string]interface{}{
					"count": 0, "results": []interface{}{},
				})
			}

		// Create resources (org=1, inv=2, project=3, ee=4, template=5).
		case r.Method == http.MethodPost && r.URL.Path == "/api/v2/organizations/":
			json.NewEncoder(w).Encode(map[string]interface{}{"id": float64(1)})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v2/inventories/":
			json.NewEncoder(w).Encode(map[string]interface{}{"id": float64(2)})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v2/projects/":
			json.NewEncoder(w).Encode(map[string]interface{}{"id": float64(3)})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v2/execution_environments/":
			createdEE++
			json.NewDecoder(r.Body).Decode(&eeData)
			json.NewEncoder(w).Encode(map[string]interface{}{"id": float64(4)})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v2/job_templates/":
			json.NewDecoder(r.Body).Decode(&templateData)
			json.NewEncoder(w).Encode(map[string]interface{}{"id": float64(5)})

		// Credential association (SyncChildren posts with associate: true).
		case r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/credentials/"):
			associatedCreds++
			w.WriteHeader(http.StatusNoContent)

		// Instance group association.
		case r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/instance_groups/"):
			associatedIGs++
			w.WriteHeader(http.StatusNoContent)

		// Launch.
		case r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/launch/"):
			json.NewEncoder(w).Encode(map[string]interface{}{"id": float64(999)})

		// Project update (for retry).
		case r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/update/"):
			w.WriteHeader(http.StatusAccepted)

		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	tc := NewTowerClient("unused", "admin", "secret", nil)
	tc.baseURL = server.URL

	jobConfig := TowerJobConfig{
		Organization:          "test-org",
		Inventory:             "test-inv",
		ProjectSCMURL:         "https://git.example.com/repo.git",
		ProjectSCMRef:         "main",
		SCMUpdateOnLaunch:     true,
		SCMUpdateCacheTimeout: 60,
		SCMClean:              true,
		TemplateName:          "deploy",
		Playbook:              "site.yml",
		ExtraVars:             map[string]interface{}{"env": "prod"},
		Timeout:               600,
		ExecutionEnvironment: &EEConfig{
			Name:  "test-org ee-image",
			Image: "quay.io/test/ee:latest",
			Pull:  "always",
		},
		InstanceGroups: []string{"my-ig"},
		Credentials:    []string{"my-cred"},
	}

	jobID, err := tc.LaunchJob(context.Background(), jobConfig)
	if err != nil {
		t.Fatalf("LaunchJob returned error: %v", err)
	}
	if jobID != 999 {
		t.Errorf("jobID = %d, want 999", jobID)
	}

	// Verify EE was created.
	if createdEE != 1 {
		t.Errorf("createdEE = %d, want 1", createdEE)
	}
	if eeData["image"] != "quay.io/test/ee:latest" {
		t.Errorf("ee image = %v, want quay.io/test/ee:latest", eeData["image"])
	}
	if eeData["pull"] != "always" {
		t.Errorf("ee pull = %v, want always", eeData["pull"])
	}

	// Verify template has EE set.
	if templateData["execution_environment"] != float64(4) {
		t.Errorf("template execution_environment = %v, want 4", templateData["execution_environment"])
	}

	// Verify credentials and instance groups were associated.
	if associatedCreds != 1 {
		t.Errorf("associatedCreds = %d, want 1", associatedCreds)
	}
	if associatedIGs != 1 {
		t.Errorf("associatedIGs = %d, want 1", associatedIGs)
	}
}

func TestSyncChildrenIdempotent(t *testing.T) {
	var associated, disassociated []int

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch r.Method {
		case http.MethodGet:
			// Template already has credentials 10 and 20.
			json.NewEncoder(w).Encode(map[string]interface{}{
				"results": []interface{}{
					map[string]interface{}{"id": float64(10)},
					map[string]interface{}{"id": float64(20)},
				},
			})
		case http.MethodPost:
			var body map[string]interface{}
			json.NewDecoder(r.Body).Decode(&body)
			id := int(body["id"].(float64))
			if body["disassociate"] == true {
				disassociated = append(disassociated, id)
			} else {
				associated = append(associated, id)
			}
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	defer server.Close()

	tc := NewTowerClient("unused", "admin", "secret", nil)
	tc.baseURL = server.URL

	// Desired: keep 10, drop 20, add 30.
	err := tc.SyncChildren(context.Background(), "tok", "/api/v2/job_templates/", 5, "credentials", []int{10, 30})
	if err != nil {
		t.Fatalf("SyncChildren error: %v", err)
	}

	if len(disassociated) != 1 || disassociated[0] != 20 {
		t.Errorf("disassociated = %v, want [20]", disassociated)
	}
	if len(associated) != 1 || associated[0] != 30 {
		t.Errorf("associated = %v, want [30]", associated)
	}
}

func TestSyncChildrenNoChanges(t *testing.T) {
	var postCount int

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch r.Method {
		case http.MethodGet:
			json.NewEncoder(w).Encode(map[string]interface{}{
				"results": []interface{}{
					map[string]interface{}{"id": float64(10)},
					map[string]interface{}{"id": float64(20)},
				},
			})
		case http.MethodPost:
			postCount++
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	defer server.Close()

	tc := NewTowerClient("unused", "admin", "secret", nil)
	tc.baseURL = server.URL

	// Desired matches existing — no POSTs should happen.
	err := tc.SyncChildren(context.Background(), "tok", "/api/v2/job_templates/", 5, "credentials", []int{10, 20})
	if err != nil {
		t.Fatalf("SyncChildren error: %v", err)
	}
	if postCount != 0 {
		t.Errorf("postCount = %d, want 0 (no changes needed)", postCount)
	}
}

func TestTowerLaunchJobSCMSettings(t *testing.T) {
	// Verify SCM settings are passed to the project creation.
	var projData map[string]interface{}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/v2/tokens/":
			json.NewEncoder(w).Encode(map[string]interface{}{
				"token": "tok", "id": float64(1),
			})
		case r.Method == http.MethodDelete:
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodGet && r.URL.RawQuery != "":
			json.NewEncoder(w).Encode(map[string]interface{}{
				"count": 0, "results": []interface{}{},
			})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v2/organizations/":
			json.NewEncoder(w).Encode(map[string]interface{}{"id": float64(1)})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v2/inventories/":
			json.NewEncoder(w).Encode(map[string]interface{}{"id": float64(2)})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v2/projects/":
			json.NewDecoder(r.Body).Decode(&projData)
			json.NewEncoder(w).Encode(map[string]interface{}{"id": float64(3)})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v2/job_templates/":
			json.NewEncoder(w).Encode(map[string]interface{}{"id": float64(4)})
		case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/job_templates/4/"):
			json.NewEncoder(w).Encode(map[string]interface{}{
				"results": []interface{}{},
			})
		case r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/launch/"):
			json.NewEncoder(w).Encode(map[string]interface{}{"id": float64(100)})
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()

	tc := NewTowerClient("unused", "admin", "secret", nil)
	tc.baseURL = server.URL

	jobConfig := TowerJobConfig{
		Organization:          "org",
		Inventory:             "inv",
		ProjectSCMURL:         "https://git.example.com/repo.git",
		ProjectSCMRef:         "v2.5",
		SCMUpdateOnLaunch:     false, // release version
		SCMUpdateCacheTimeout: 3600,
		SCMClean:              false,
		TemplateName:          "tmpl",
		Playbook:              "site.yml",
		Timeout:               300,
	}

	_, err := tc.LaunchJob(context.Background(), jobConfig)
	if err != nil {
		t.Fatalf("LaunchJob returned error: %v", err)
	}

	// Verify SCM settings on project.
	if projData["scm_update_on_launch"] != false {
		t.Errorf("scm_update_on_launch = %v, want false", projData["scm_update_on_launch"])
	}
	if projData["scm_update_cache_timeout"] != float64(3600) {
		t.Errorf("scm_update_cache_timeout = %v, want 3600", projData["scm_update_cache_timeout"])
	}
	if projData["scm_clean"] != false {
		t.Errorf("scm_clean = %v, want false", projData["scm_clean"])
	}
}

func TestTowerLaunchJobRetryOnTemplateFail(t *testing.T) {
	templateCreateAttempts := 0
	var projectUpdated bool

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/v2/tokens/":
			json.NewEncoder(w).Encode(map[string]interface{}{
				"token": "tok", "id": float64(1),
			})
		case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/api/v2/tokens/"):
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodGet && r.URL.RawQuery != "":
			json.NewEncoder(w).Encode(map[string]interface{}{
				"count": 0, "results": []interface{}{},
			})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v2/organizations/":
			json.NewEncoder(w).Encode(map[string]interface{}{"id": float64(1)})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v2/inventories/":
			json.NewEncoder(w).Encode(map[string]interface{}{"id": float64(2)})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v2/projects/":
			json.NewEncoder(w).Encode(map[string]interface{}{"id": float64(3)})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v2/job_templates/":
			templateCreateAttempts++
			if templateCreateAttempts == 1 {
				w.WriteHeader(http.StatusBadRequest)
				fmt.Fprint(w, `{"detail":"project sync pending"}`)
				return
			}
			json.NewEncoder(w).Encode(map[string]interface{}{"id": float64(4)})
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/update/"):
			projectUpdated = true
			w.WriteHeader(http.StatusAccepted)
		case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/job_templates/4/"):
			json.NewEncoder(w).Encode(map[string]interface{}{
				"results": []interface{}{},
			})
		case r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/launch/"):
			json.NewEncoder(w).Encode(map[string]interface{}{"id": float64(777)})
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()

	tc := NewTowerClient("unused", "admin", "secret", nil)
	tc.baseURL = server.URL

	jobID, err := tc.LaunchJob(context.Background(), TowerJobConfig{
		Organization:  "test-org",
		Inventory:     "test-inv",
		ProjectSCMURL: "https://git.example.com/repo.git",
		ProjectSCMRef: "main",
		TemplateName:  "test-template",
		Playbook:      "playbook.yml",
	})
	if err != nil {
		t.Fatalf("LaunchJob failed: %v", err)
	}
	if jobID != 777 {
		t.Errorf("jobID = %d, want 777", jobID)
	}
	if templateCreateAttempts < 2 {
		t.Errorf("templateCreateAttempts = %d, want >= 2", templateCreateAttempts)
	}
	if !projectUpdated {
		t.Error("expected project update to be triggered on retry")
	}
}

func TestGetJobCount(t *testing.T) {
	tests := []struct {
		name string
		c    map[string]interface{}
		want float64
	}{
		{"with count", map[string]interface{}{"active_job_count": float64(5)}, 5},
		{"missing key", map[string]interface{}{}, 0},
		{"wrong type", map[string]interface{}{"active_job_count": "five"}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetJobCount(tt.c); got != tt.want {
				t.Errorf("GetJobCount() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTowerClientTokenCache(t *testing.T) {
	var tokenCreateCount atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/v2/tokens/":
			tokenCreateCount.Add(1)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"token": "cached-token",
				"id":    float64(42),
			})
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()

	host := strings.TrimPrefix(server.URL, "http://")
	tc := NewTowerClient(host, "user", "pass", &tls.Config{InsecureSkipVerify: true})
	tc.baseURL = server.URL

	ctx := context.Background()
	tok1, err := tc.GetToken(ctx)
	if err != nil {
		t.Fatalf("GetToken: %v", err)
	}
	if tok1 != "cached-token" {
		t.Errorf("token = %q, want %q", tok1, "cached-token")
	}

	tok2, err := tc.GetToken(ctx)
	if err != nil {
		t.Fatalf("GetToken second call: %v", err)
	}
	if tok2 != "cached-token" {
		t.Errorf("second token = %q, want %q", tok2, "cached-token")
	}

	if tokenCreateCount.Load() != 1 {
		t.Errorf("token create count = %d, want 1 (should be cached)", tokenCreateCount.Load())
	}

	tc.Close(ctx)
}

func TestTowerClientPoolReuse(t *testing.T) {
	pool := NewTowerClientPool()
	tc1 := pool.Get("host1", "user", "pass", nil)
	tc2 := pool.Get("host1", "user", "pass", nil)

	if tc1 != tc2 {
		t.Error("pool should return the same client for the same hostname")
	}

	tc3 := pool.Get("host2", "user", "pass", nil)
	if tc1 == tc3 {
		t.Error("pool should return different clients for different hostnames")
	}
}
