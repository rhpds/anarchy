package clients

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"strings"

	"github.com/rhpds/anarchy/babylon-runner/internal/httputil"
)

const vaultVarChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

// InsertUnvaultString transforms vault-encrypted values in a map into
// Jinja2 lookup expressions so Tower can decrypt them at runtime.
// Matches the Python insert_unvault_string filter from babylon_anarchy_governor.
//
// Any string value starting with "$ANSIBLE_VAULT;" is replaced with
// {{ lookup('unvault_string', __vaulted_value_XXXX) }} and the original
// encrypted blob is stored in a sibling key at the top level.
func InsertUnvaultString(vars map[string]interface{}) map[string]interface{} {
	vaultedValues := make(map[string]string)
	result := unvaultRecurse(vars, vaultedValues).(map[string]interface{})
	for k, v := range vaultedValues {
		result[k] = v
	}
	return result
}

func unvaultRecurse(value interface{}, vaulted map[string]string) interface{} {
	switch v := value.(type) {
	case map[string]interface{}:
		out := make(map[string]interface{}, len(v))
		for k, val := range v {
			out[k] = unvaultRecurse(val, vaulted)
		}
		return out
	case []interface{}:
		out := make([]interface{}, len(v))
		for i, val := range v {
			out[i] = unvaultRecurse(val, vaulted)
		}
		return out
	case string:
		if strings.HasPrefix(strings.TrimSpace(v), "$ANSIBLE_VAULT;") {
			var varName string
			for {
				b := make([]byte, 12)
				for i := range b {
					b[i] = vaultVarChars[rand.Intn(len(vaultVarChars))]
				}
				varName = "__vaulted_value_" + string(b)
				if _, exists := vaulted[varName]; !exists {
					break
				}
			}
			vaulted[varName] = v
			return "{{ lookup('unvault_string', " + varName + ") }}"
		}
		return v
	default:
		return v
	}
}

// TowerJobConfig holds the parameters for launching a job on Tower/AAP2.
type TowerJobConfig struct {
	Organization  string
	Inventory     string
	ProjectName   string
	ProjectSCMURL string
	ProjectSCMRef string
	TemplateName  string
	Playbook      string
	ExtraVars     map[string]interface{}
	Timeout       int

	// SCM settings for the project.
	SCMUpdateOnLaunch     bool
	SCMUpdateCacheTimeout int
	SCMClean              bool

	// Execution environment (AAP2/Controller only).
	ExecutionEnvironment *EEConfig

	// Instance groups to associate with the job template.
	InstanceGroups []string

	// Credential names to associate with the job template.
	Credentials []string

	// Vault credentials to create: vault_id -> password.
	VaultCredentials map[string]string
}

// EEConfig holds execution environment configuration.
type EEConfig struct {
	Name    string
	Image   string
	Pull    string
	Private bool
}

// TowerClient communicates with a Tower/AAP2 instance via its REST API.
type TowerClient struct {
	baseURL  string
	username string
	password string
	client   *http.Client
}

// NewTowerClient creates a TowerClient for the given hostname.
// tlsConfig is optional — nil uses Go defaults (system CA, verify enabled).
func NewTowerClient(hostname, username, password string, tlsConfig *tls.Config) *TowerClient {
	return &TowerClient{
		baseURL:  "https://" + hostname,
		username: username,
		password: password,
		client: &http.Client{
			Transport: httputil.NewTransport(tlsConfig),
		},
	}
}

// SelectController picks a controller from the list based on the selection mode.
//   - "random": random pick
//   - "first-available": first element
//   - "balance": pick the one with lowest active_job_count
func SelectController(controllers []map[string]interface{}, mode string) map[string]interface{} {
	if len(controllers) == 0 {
		return nil
	}

	switch mode {
	case "first-available":
		return controllers[0]
	case "balance":
		best := controllers[0]
		bestCount := GetJobCount(best)
		for _, c := range controllers[1:] {
			count := GetJobCount(c)
			if count < bestCount {
				best = c
				bestCount = count
			}
		}
		return best
	default: // "random"
		return controllers[rand.Intn(len(controllers))]
	}
}

// GetJobCount extracts active_job_count as float64 from a controller map.
func GetJobCount(c map[string]interface{}) float64 {
	v, ok := c["active_job_count"].(float64)
	if !ok {
		return 0
	}
	return v
}

// CreateOAuthToken creates a personal access token via the Tower API
// using Basic Auth credentials.
func (tc *TowerClient) CreateOAuthToken() (token string, tokenID int, err error) {
	url := tc.baseURL + "/api/v2/tokens/"

	req, err := http.NewRequest(http.MethodPost, url, nil)
	if err != nil {
		return "", 0, fmt.Errorf("create request: %w", err)
	}
	req.SetBasicAuth(tc.username, tc.password)
	req.Header.Set("Content-Type", "application/json")

	resp, err := tc.client.Do(req)
	if err != nil {
		return "", 0, fmt.Errorf("POST %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", 0, fmt.Errorf("POST %s: status %d", url, resp.StatusCode)
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", 0, fmt.Errorf("decode response: %w", err)
	}

	tok, ok := result["token"].(string)
	if !ok {
		return "", 0, fmt.Errorf("token field missing or not a string")
	}
	id, ok := result["id"].(float64)
	if !ok {
		return "", 0, fmt.Errorf("id field missing or not a number")
	}

	return tok, int(id), nil
}

// DeleteOAuthToken deletes a personal access token by ID using Basic Auth.
func (tc *TowerClient) DeleteOAuthToken(tokenID int) error {
	url := fmt.Sprintf("%s/api/v2/tokens/%d/", tc.baseURL, tokenID)

	req, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.SetBasicAuth(tc.username, tc.password)

	resp, err := tc.client.Do(req)
	if err != nil {
		return fmt.Errorf("DELETE %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("DELETE %s: status %d", url, resp.StatusCode)
	}
	return nil
}

// GetJobStatus fetches the status of a Tower job by its ID.
func (tc *TowerClient) GetJobStatus(oauthToken string, jobID int) (map[string]interface{}, error) {
	url := fmt.Sprintf("%s/api/v2/jobs/%d/", tc.baseURL, jobID)

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+oauthToken)

	resp, err := tc.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("GET %s: status %d", url, resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return result, nil
}

// CancelJob sends a cancel request for a Tower job.
func (tc *TowerClient) CancelJob(oauthToken string, jobID int) error {
	url := fmt.Sprintf("%s/api/v2/jobs/%d/cancel/", tc.baseURL, jobID)

	req, err := http.NewRequest(http.MethodPost, url, nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+oauthToken)

	resp, err := tc.client.Do(req)
	if err != nil {
		return fmt.Errorf("POST %s: %w", url, err)
	}
	defer resp.Body.Close()

	// 405 means the job already finished — nothing to cancel, which is fine.
	if resp.StatusCode == http.StatusMethodNotAllowed {
		return nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("POST %s: status %d", url, resp.StatusCode)
	}
	return nil
}

// SearchResource searches for a resource by name via the Tower API.
// Returns the ID of the first match, or 0 if not found.
func (tc *TowerClient) SearchResource(oauthToken, path, name string) (int, error) {
	params := url.Values{"name": {name}}
	reqURL := tc.baseURL + path + "?" + params.Encode()

	req, err := http.NewRequest(http.MethodGet, reqURL, nil)
	if err != nil {
		return 0, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+oauthToken)

	resp, err := tc.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("GET %s: %w", reqURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return 0, fmt.Errorf("GET %s: status %d", reqURL, resp.StatusCode)
	}

	var result struct {
		Count   int                      `json:"count"`
		Results []map[string]interface{} `json:"results"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, fmt.Errorf("decode response: %w", err)
	}

	if result.Count > 0 && len(result.Results) > 0 {
		if id, ok := result.Results[0]["id"].(float64); ok {
			return int(id), nil
		}
	}
	return 0, nil
}

// CreateResource creates a Tower resource by POSTing to the given API path.
// It returns the "id" field from the JSON response.
func (tc *TowerClient) CreateResource(oauthToken, path string, data map[string]interface{}) (int, error) {
	reqURL := tc.baseURL + path

	body, err := json.Marshal(data)
	if err != nil {
		return 0, fmt.Errorf("marshal body: %w", err)
	}

	req, err := http.NewRequest(http.MethodPost, reqURL, bytes.NewReader(body))
	if err != nil {
		return 0, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+oauthToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err := tc.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("POST %s: %w", reqURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("POST %s: status %d: %s", reqURL, resp.StatusCode, string(respBody))
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, fmt.Errorf("decode response: %w", err)
	}

	id, ok := result["id"].(float64)
	if !ok {
		return 0, fmt.Errorf("id field missing or not a number in response from %s", path)
	}
	return int(id), nil
}

// EnsureResource finds a resource by name or creates it. Returns the ID.
// This is idempotent: if the resource already exists, returns its ID
// without modification (matching awx.awx module behavior).
func (tc *TowerClient) EnsureResource(oauthToken, path string, data map[string]interface{}) (int, error) {
	name, _ := data["name"].(string)
	if name != "" {
		id, err := tc.SearchResource(oauthToken, path, name)
		if err != nil {
			// Search failed; fall through to create.
		} else if id > 0 {
			return id, nil
		}
	}
	return tc.CreateResource(oauthToken, path, data)
}

// UpdateProject triggers a project SCM sync. This is used as a retry
// mechanism when job template creation or launch fails.
func (tc *TowerClient) UpdateProject(oauthToken string, projID int) error {
	reqURL := fmt.Sprintf("%s/api/v2/projects/%d/update/", tc.baseURL, projID)

	req, err := http.NewRequest(http.MethodPost, reqURL, nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+oauthToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err := tc.client.Do(req)
	if err != nil {
		return fmt.Errorf("POST %s: %w", reqURL, err)
	}
	defer resp.Body.Close()

	// 202 Accepted is the normal response for project update.
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("POST %s: status %d", reqURL, resp.StatusCode)
	}
	return nil
}

// listChildIDs returns the IDs of all resources currently associated with
// a parent via a sub-resource endpoint (e.g., job_templates/5/credentials/).
func (tc *TowerClient) listChildIDs(oauthToken, parentPath string, parentID int, childEndpoint string) ([]int, error) {
	reqURL := fmt.Sprintf("%s%s%d/%s/", tc.baseURL, parentPath, parentID, childEndpoint)

	req, err := http.NewRequest(http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+oauthToken)

	resp, err := tc.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", reqURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("GET %s: status %d", reqURL, resp.StatusCode)
	}

	var result struct {
		Results []struct {
			ID float64 `json:"id"`
		} `json:"results"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	ids := make([]int, len(result.Results))
	for i, r := range result.Results {
		ids[i] = int(r.ID)
	}
	return ids, nil
}

// postChild sends an associate or disassociate request to a sub-resource endpoint.
func (tc *TowerClient) postChild(oauthToken, reqURL string, payload map[string]interface{}) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal body: %w", err)
	}

	req, err := http.NewRequest(http.MethodPost, reqURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+oauthToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err := tc.client.Do(req)
	if err != nil {
		return fmt.Errorf("POST %s: %w", reqURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("POST %s: status %d: %s", reqURL, resp.StatusCode, string(respBody))
	}
	return nil
}

// SyncChildren synchronizes a parent's sub-resource associations to match
// desiredIDs exactly. It GETs the current associations, calculates the diff,
// disassociates extras, and associates missing ones. This is idempotent —
// matching the pattern used by the awx.awx Ansible collection.
func (tc *TowerClient) SyncChildren(oauthToken, parentPath string, parentID int, childEndpoint string, desiredIDs []int) error {
	existing, err := tc.listChildIDs(oauthToken, parentPath, parentID, childEndpoint)
	if err != nil {
		return fmt.Errorf("list existing %s: %w", childEndpoint, err)
	}

	existingSet := make(map[int]bool, len(existing))
	for _, id := range existing {
		existingSet[id] = true
	}
	desiredSet := make(map[int]bool, len(desiredIDs))
	for _, id := range desiredIDs {
		desiredSet[id] = true
	}

	reqURL := fmt.Sprintf("%s%s%d/%s/", tc.baseURL, parentPath, parentID, childEndpoint)

	for id := range existingSet {
		if !desiredSet[id] {
			if err := tc.postChild(oauthToken, reqURL, map[string]interface{}{
				"id": float64(id), "disassociate": true,
			}); err != nil {
				return fmt.Errorf("disassociate %s %d: %w", childEndpoint, id, err)
			}
		}
	}

	for id := range desiredSet {
		if !existingSet[id] {
			if err := tc.postChild(oauthToken, reqURL, map[string]interface{}{
				"id": float64(id), "associate": true,
			}); err != nil {
				return fmt.Errorf("associate %s %d: %w", childEndpoint, id, err)
			}
		}
	}

	return nil
}

// LaunchJob creates all required Tower resources and launches a job template.
// Full workflow matching Ansible run-tower-job.yaml:
//  1. Create token
//  2. Ensure organization
//  3. Ensure inventory
//  4. Create vault credentials (if any)
//  5. Ensure project (with SCM settings)
//  6. Ensure execution environment (if configured)
//  7. Create job template (with retry on failure via project update)
//  8. Associate credentials and instance groups with template
//  9. Launch job (with retry on failure via project update)
func (tc *TowerClient) LaunchJob(config TowerJobConfig) (int, error) {
	// Step 1: Create OAuth token.
	token, tokenID, err := tc.CreateOAuthToken()
	if err != nil {
		return 0, fmt.Errorf("create oauth token: %w", err)
	}
	defer func() { _ = tc.DeleteOAuthToken(tokenID) }()

	// Step 2: Ensure organization.
	orgID, err := tc.EnsureResource(token, "/api/v2/organizations/", map[string]interface{}{
		"name": config.Organization,
	})
	if err != nil {
		return 0, fmt.Errorf("ensure organization: %w", err)
	}

	// Step 3: Ensure inventory.
	invID, err := tc.EnsureResource(token, "/api/v2/inventories/", map[string]interface{}{
		"name":         config.Inventory,
		"organization": float64(orgID),
	})
	if err != nil {
		return 0, fmt.Errorf("ensure inventory: %w", err)
	}

	// Step 4: Create vault credentials (if any).
	// Vault credentials are AWX credentials of type "Vault" with vault_id/vault_password inputs.
	var credIDs []int
	if len(config.VaultCredentials) > 0 {
		// Find the "Vault" credential type ID.
		vaultTypeID, err := tc.SearchResource(token, "/api/v2/credential_types/", "Vault")
		if err == nil && vaultTypeID > 0 {
			for vaultID, vaultPassword := range config.VaultCredentials {
				credName := config.Organization + " " + vaultID
				credID, err := tc.EnsureResource(token, "/api/v2/credentials/", map[string]interface{}{
					"name":            credName,
					"credential_type": float64(vaultTypeID),
					"inputs": map[string]interface{}{
						"vault_id":       vaultID,
						"vault_password": vaultPassword,
					},
					"organization": float64(orgID),
				})
				if err != nil {
					return 0, fmt.Errorf("ensure vault credential %q: %w", vaultID, err)
				}
				credIDs = append(credIDs, credID)
			}
		}
	}

	// Look up static credentials by name.
	for _, credName := range config.Credentials {
		credID, err := tc.SearchResource(token, "/api/v2/credentials/", credName)
		if err != nil {
			return 0, fmt.Errorf("find credential %q: %w", credName, err)
		}
		if credID > 0 {
			credIDs = append(credIDs, credID)
		}
	}

	// Step 5: Ensure project (with SCM settings).
	projData := map[string]interface{}{
		"name":                     config.ProjectName,
		"organization":             float64(orgID),
		"scm_type":                 "git",
		"scm_url":                  config.ProjectSCMURL,
		"scm_branch":               config.ProjectSCMRef,
		"scm_update_on_launch":     config.SCMUpdateOnLaunch,
		"scm_update_cache_timeout": float64(config.SCMUpdateCacheTimeout),
		"scm_clean":                config.SCMClean,
	}
	projID, err := tc.EnsureResource(token, "/api/v2/projects/", projData)
	if err != nil {
		return 0, fmt.Errorf("ensure project: %w", err)
	}

	// Step 6: Ensure execution environment (if configured).
	var eeID int
	if config.ExecutionEnvironment != nil && config.ExecutionEnvironment.Image != "" {
		eeData := map[string]interface{}{
			"name":         config.ExecutionEnvironment.Name,
			"image":        config.ExecutionEnvironment.Image,
			"organization": float64(orgID),
		}
		if config.ExecutionEnvironment.Pull != "" {
			eeData["pull"] = config.ExecutionEnvironment.Pull
		}
		// If the EE image is private, use the registry hostname as
		// the credential name (must be pre-created on Tower).
		if config.ExecutionEnvironment.Private {
			registry := strings.SplitN(config.ExecutionEnvironment.Image, "/", 2)[0]
			regCredID, _ := tc.SearchResource(token, "/api/v2/credentials/", registry)
			if regCredID > 0 {
				eeData["credential"] = float64(regCredID)
			}
		}
		eeID, err = tc.EnsureResource(token, "/api/v2/execution_environments/", eeData)
		if err != nil {
			return 0, fmt.Errorf("ensure execution environment: %w", err)
		}
	}

	// Step 7: Create job template (with retry via project update).
	// extra_vars are set on the template (matching Ansible awx.awx.job_template),
	// NOT at launch time. The launch only passes inventory.
	templateData := map[string]interface{}{
		"name":                    config.TemplateName,
		"project":                 float64(projID),
		"playbook":                config.Playbook,
		"ask_inventory_on_launch": true,
	}
	if config.Timeout > 0 {
		templateData["timeout"] = float64(config.Timeout)
	}
	if eeID > 0 {
		templateData["execution_environment"] = float64(eeID)
	}
	if config.ExtraVars != nil {
		extraVarsJSON, err := json.Marshal(config.ExtraVars)
		if err != nil {
			return 0, fmt.Errorf("marshal extra_vars: %w", err)
		}
		templateData["extra_vars"] = string(extraVarsJSON)
	}

	tmplID, err := tc.EnsureResource(token, "/api/v2/job_templates/", templateData)
	if err != nil {
		// Retry: update project SCM, then retry template creation.
		_ = tc.UpdateProject(token, projID)
		tmplID, err = tc.EnsureResource(token, "/api/v2/job_templates/", templateData)
		if err != nil {
			return 0, fmt.Errorf("ensure job template: %w", err)
		}
	}

	// Step 8: Sync credentials with the template (idempotent diff-based).
	if err := tc.SyncChildren(token, "/api/v2/job_templates/", tmplID, "credentials", credIDs); err != nil {
		return 0, fmt.Errorf("sync credentials: %w", err)
	}

	// Sync instance groups with the template.
	var igIDs []int
	for _, igName := range config.InstanceGroups {
		igID, err := tc.SearchResource(token, "/api/v2/instance_groups/", igName)
		if err != nil || igID == 0 {
			continue
		}
		igIDs = append(igIDs, igID)
	}
	if err := tc.SyncChildren(token, "/api/v2/job_templates/", tmplID, "instance_groups", igIDs); err != nil {
		return 0, fmt.Errorf("sync instance groups: %w", err)
	}

	// Step 9: Launch the job template (with retry via project update).
	launchData := map[string]interface{}{
		"inventory": float64(invID),
	}

	launchPath := fmt.Sprintf("/api/v2/job_templates/%d/launch/", tmplID)
	jobID, err := tc.CreateResource(token, launchPath, launchData)
	if err != nil {
		// Retry: update project SCM, then retry launch.
		_ = tc.UpdateProject(token, projID)
		jobID, err = tc.CreateResource(token, launchPath, launchData)
		if err != nil {
			return 0, fmt.Errorf("launch job: %w", err)
		}
	}

	return jobID, nil
}
