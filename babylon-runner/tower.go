package main

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
)

// TowerJobConfig holds the parameters for launching a job on Tower/AAP2.
type TowerJobConfig struct {
	Organization  string
	Inventory     string
	ProjectSCMURL string
	ProjectSCMRef string
	TemplateName  string
	Playbook      string
	ExtraVars     map[string]interface{}
	Timeout       int
}

// TowerClient communicates with a Tower/AAP2 instance via its REST API.
type TowerClient struct {
	baseURL  string
	username string
	password string
	client   *http.Client
}

// NewTowerClient creates a TowerClient for the given hostname.
// TLS certificate verification is disabled because tower certs are
// self-signed in our environment.
func NewTowerClient(hostname, username, password string) *TowerClient {
	return &TowerClient{
		baseURL:  "https://" + hostname,
		username: username,
		password: password,
		client: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
		},
	}
}

// selectController picks a controller from the list based on the selection mode.
//   - "random": random pick
//   - "first-available": first element
//   - "balance": pick the one with lowest active_job_count
func selectController(controllers []map[string]interface{}, mode string) map[string]interface{} {
	if len(controllers) == 0 {
		return nil
	}

	switch mode {
	case "first-available":
		return controllers[0]
	case "balance":
		best := controllers[0]
		bestCount := getJobCount(best)
		for _, c := range controllers[1:] {
			count := getJobCount(c)
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

// getJobCount extracts active_job_count as float64 from a controller map.
func getJobCount(c map[string]interface{}) float64 {
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

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("POST %s: status %d", url, resp.StatusCode)
	}
	return nil
}

// ensureResource creates a Tower resource by POSTing to the given API path.
// It returns the "id" field from the JSON response.
func (tc *TowerClient) ensureResource(oauthToken, path string, data map[string]interface{}) (int, error) {
	url := tc.baseURL + path

	body, err := json.Marshal(data)
	if err != nil {
		return 0, fmt.Errorf("marshal body: %w", err)
	}

	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return 0, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+oauthToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err := tc.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("POST %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return 0, fmt.Errorf("POST %s: status %d", url, resp.StatusCode)
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

// LaunchJob creates all required Tower resources and launches a job template.
// It performs the full workflow: create token, org, inventory, project,
// job template, launch, then cleanup the token.
func (tc *TowerClient) LaunchJob(config TowerJobConfig) (int, error) {
	// Step 1: Create OAuth token.
	token, tokenID, err := tc.CreateOAuthToken()
	if err != nil {
		return 0, fmt.Errorf("create oauth token: %w", err)
	}

	// Ensure token cleanup happens regardless of outcome.
	defer func() {
		_ = tc.DeleteOAuthToken(tokenID)
	}()

	// Step 2: Create organization.
	orgID, err := tc.ensureResource(token, "/api/v2/organizations/", map[string]interface{}{
		"name": config.Organization,
	})
	if err != nil {
		return 0, fmt.Errorf("create organization: %w", err)
	}

	// Step 3: Create inventory.
	invID, err := tc.ensureResource(token, "/api/v2/inventories/", map[string]interface{}{
		"name":         config.Inventory,
		"organization": float64(orgID),
	})
	if err != nil {
		return 0, fmt.Errorf("create inventory: %w", err)
	}

	// Step 4: Create project.
	projID, err := tc.ensureResource(token, "/api/v2/projects/", map[string]interface{}{
		"name":         config.Organization + "-project",
		"organization": float64(orgID),
		"scm_type":     "git",
		"scm_url":      config.ProjectSCMURL,
		"scm_branch":   config.ProjectSCMRef,
	})
	if err != nil {
		return 0, fmt.Errorf("create project: %w", err)
	}

	// Step 5: Create job template.
	templateData := map[string]interface{}{
		"name":      config.TemplateName,
		"inventory": float64(invID),
		"project":   float64(projID),
		"playbook":  config.Playbook,
	}
	if config.Timeout > 0 {
		templateData["timeout"] = float64(config.Timeout)
	}
	tmplID, err := tc.ensureResource(token, "/api/v2/job_templates/", templateData)
	if err != nil {
		return 0, fmt.Errorf("create job template: %w", err)
	}

	// Step 6: Launch the job template.
	launchData := map[string]interface{}{}
	if config.ExtraVars != nil {
		extraVarsJSON, err := json.Marshal(config.ExtraVars)
		if err != nil {
			return 0, fmt.Errorf("marshal extra_vars: %w", err)
		}
		launchData["extra_vars"] = string(extraVarsJSON)
	}

	launchPath := fmt.Sprintf("/api/v2/job_templates/%d/launch/", tmplID)
	jobID, err := tc.ensureResource(token, launchPath, launchData)
	if err != nil {
		return 0, fmt.Errorf("launch job: %w", err)
	}

	return jobID, nil
}
