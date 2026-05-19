package main

import (
	"fmt"
	"log"
	"net/http"
	"time"
)

// SandboxResult holds the result of a sandbox API get or book operation.
type SandboxResult struct {
	Status      string                 // "success", "queued", "error", "not-found"
	Placement   map[string]interface{} // raw placement data
	DynamicVars map[string]interface{} // extracted job vars from resources
	Labels      map[string]string      // extracted labels
}

// getSandboxClient creates a SandboxAPIClient, using rc.SandboxBaseURL for tests.
func getSandboxClient(rc *RunContext) *SandboxAPIClient {
	baseURL := DefaultSandboxAPIURL
	if rc.SandboxBaseURL != "" {
		baseURL = rc.SandboxBaseURL
	} else {
		meta := rc.Meta()
		if meta != nil {
			if u, ok := meta["sandbox_api_url"].(string); ok && u != "" {
				baseURL = u
			}
		}
	}
	return NewSandboxAPIClient(baseURL)
}

// sandboxLoginToken returns the sandbox API login token from __meta__.
func sandboxLoginToken(rc *RunContext) string {
	meta := rc.Meta()
	if meta == nil {
		return ""
	}
	token, _ := meta["sandbox_api_login_token"].(string)
	return token
}

// sandboxLogin authenticates with the sandbox API and returns an access token.
func sandboxLogin(rc *RunContext) (string, error) {
	token := sandboxLoginToken(rc)
	if token == "" {
		return "", fmt.Errorf("no sandbox_api_login_token in __meta__")
	}
	client := getSandboxClient(rc)
	accessToken, err := client.Login(token)
	if err != nil {
		return "", fmt.Errorf("sandbox login: %w", err)
	}
	return accessToken, nil
}

// sandboxGet performs the sandbox get workflow:
//  1. Login to sandbox API
//  2. GET placement by UUID
//  3. If error status -> return error result
//  4. If found with resources -> extract vars, update subject
//  5. If action is "provision" and not found -> book new placement
func sandboxGet(rc *RunContext, action string) (*SandboxResult, error) {
	uuid := rc.UUID()
	if uuid == "" {
		return nil, fmt.Errorf("no uuid in job_vars")
	}

	accessToken, err := sandboxLogin(rc)
	if err != nil {
		return nil, err
	}

	client := getSandboxClient(rc)
	placement, statusCode, err := client.GetPlacement(accessToken, uuid)
	if err != nil {
		return nil, fmt.Errorf("get placement: %w", err)
	}

	// Not found -- book if provision action.
	if statusCode == http.StatusNotFound {
		if action == "provision" && sandboxLoginToken(rc) != "" {
			return sandboxBook(rc, accessToken)
		}
		return &SandboxResult{Status: "not-found"}, nil
	}

	// Check placement status.
	placementStatus, _ := placement["status"].(string)
	if placementStatus == "error" {
		return &SandboxResult{Status: "error", Placement: placement}, nil
	}
	if placementStatus == "queued" {
		return &SandboxResult{Status: "queued", Placement: placement}, nil
	}

	// Extract vars and update subject.
	dynamicVars, labels := extractSandboxVars(placement)
	if len(dynamicVars) > 0 || len(labels) > 0 {
		patch := PatchBody{SkipUpdateProcessing: true}
		if len(labels) > 0 {
			patch.Metadata = &PatchMetadata{Labels: labels}
		}
		if len(dynamicVars) > 0 {
			// Merge dynamic vars into existing job_vars.
			jv := rc.JobVars()
			if jv == nil {
				jv = make(map[string]interface{})
			}
			mergeMap(jv, dynamicVars)
			patch.Spec = &PatchSpec{
				Vars: map[string]interface{}{
					"job_vars": jv,
				},
			}
		}
		if err := rc.SubjectUpdate(SubjectPatch{Patch: patch}); err != nil {
			return nil, fmt.Errorf("update subject with sandbox vars: %w", err)
		}
	}

	return &SandboxResult{
		Status:      "success",
		Placement:   placement,
		DynamicVars: dynamicVars,
		Labels:      labels,
	}, nil
}

// sandboxBook books a new placement via the sandbox API.
func sandboxBook(rc *RunContext, accessToken string) (*SandboxResult, error) {
	uuid := rc.UUID()
	guid := rc.GUID()
	meta := rc.Meta()

	// Build request body.
	reqBody := map[string]interface{}{
		"service_uuid": uuid,
	}

	// Add annotations.
	annotations := map[string]interface{}{
		"guid": guid,
	}
	govJobVars := rc.GovernorJobVars()
	if govJobVars != nil {
		if envType, ok := govJobVars["env_type"].(string); ok {
			annotations["env_type"] = envType
		}
	}
	subjectJobVars := rc.JobVars()
	if subjectJobVars != nil {
		if owner, ok := subjectJobVars["owner_email"].(string); ok {
			annotations["owner"] = owner
			annotations["email"] = owner
		}
	}
	reqBody["annotations"] = annotations

	// Add sandboxes/resources from __meta__.
	if meta != nil {
		if sandboxes, ok := meta["sandboxes"].([]interface{}); ok {
			reqBody["resources"] = sandboxes
		}
	}

	client := getSandboxClient(rc)
	result, statusCode, err := client.BookPlacement(accessToken, reqBody)
	if err != nil {
		return nil, fmt.Errorf("book placement: %w", err)
	}

	switch statusCode {
	case http.StatusOK:
		dynamicVars, labels := extractSandboxVars(result)
		return &SandboxResult{
			Status:      "success",
			Placement:   result,
			DynamicVars: dynamicVars,
			Labels:      labels,
		}, nil
	case 202, 507:
		// Queued or no capacity.
		return &SandboxResult{Status: "queued", Placement: result}, nil
	default:
		return &SandboxResult{Status: "error", Placement: result},
			fmt.Errorf("book placement returned status %d", statusCode)
	}
}

// sandboxCleanup releases the sandbox placement.
func sandboxCleanup(rc *RunContext) error {
	uuid := rc.UUID()
	if uuid == "" {
		log.Printf("sandboxCleanup: no uuid, skipping")
		return nil
	}

	token := sandboxLoginToken(rc)
	if token == "" {
		log.Printf("sandboxCleanup: no login token, skipping")
		return nil
	}

	accessToken, err := sandboxLogin(rc)
	if err != nil {
		return fmt.Errorf("sandbox cleanup login: %w", err)
	}

	client := getSandboxClient(rc)
	if err := client.ReleasePlacement(accessToken, uuid); err != nil {
		return fmt.Errorf("release placement: %w", err)
	}

	log.Printf("sandboxCleanup: released placement uuid=%s", uuid)
	return nil
}

// sandboxStart starts the sandbox placement and polls for completion.
func sandboxStart(rc *RunContext) error {
	uuid := rc.UUID()
	if uuid == "" {
		return fmt.Errorf("no uuid for sandbox start")
	}

	accessToken, err := sandboxLogin(rc)
	if err != nil {
		return err
	}

	client := getSandboxClient(rc)
	result, err := client.StartPlacement(accessToken, uuid)
	if err != nil {
		return fmt.Errorf("start placement: %w", err)
	}

	// Extract request_id for polling.
	requestID, _ := result["request_id"].(string)
	if requestID == "" {
		return nil
	}

	// Update subject status with request info.
	if err := rc.SubjectUpdate(SubjectPatch{
		Patch: PatchBody{
			Metadata: &PatchMetadata{
				Labels: map[string]string{"state": "starting"},
			},
			Status: map[string]interface{}{
				"sandboxAPIJobs": map[string]interface{}{
					"start": map[string]interface{}{
						"requestID":      requestID,
						"startTimestamp": nowUTC(),
					},
				},
			},
			SkipUpdateProcessing: true,
		},
	}); err != nil {
		return err
	}

	return pollSandboxRequest(client, accessToken, requestID)
}

// sandboxStop stops the sandbox placement and polls for completion.
func sandboxStop(rc *RunContext) error {
	uuid := rc.UUID()
	if uuid == "" {
		return fmt.Errorf("no uuid for sandbox stop")
	}

	accessToken, err := sandboxLogin(rc)
	if err != nil {
		return err
	}

	client := getSandboxClient(rc)
	result, err := client.StopPlacement(accessToken, uuid)
	if err != nil {
		return fmt.Errorf("stop placement: %w", err)
	}

	// Extract request_id for polling.
	requestID, _ := result["request_id"].(string)
	if requestID == "" {
		return nil
	}

	// Update subject status with request info.
	if err := rc.SubjectUpdate(SubjectPatch{
		Patch: PatchBody{
			Metadata: &PatchMetadata{
				Labels: map[string]string{"state": "stopping"},
			},
			Status: map[string]interface{}{
				"sandboxAPIJobs": map[string]interface{}{
					"stop": map[string]interface{}{
						"requestID":      requestID,
						"startTimestamp": nowUTC(),
					},
				},
			},
			SkipUpdateProcessing: true,
		},
	}); err != nil {
		return err
	}

	return pollSandboxRequest(client, accessToken, requestID)
}

// pollSandboxRequest polls a sandbox API async request until it completes.
func pollSandboxRequest(client *SandboxAPIClient, accessToken, requestID string) error {
	maxAttempts := 120 // ~10 minutes at 5s intervals
	pollInterval := 5 * time.Second

	for i := 0; i < maxAttempts; i++ {
		if i > 0 {
			time.Sleep(pollInterval)
		}

		status, err := client.GetRequestStatus(accessToken, requestID)
		if err != nil {
			log.Printf("pollSandboxRequest: error checking request %s: %v", requestID, err)
			continue
		}

		state, _ := status["status"].(string)
		switch state {
		case "success", "complete":
			log.Printf("pollSandboxRequest: request %s completed successfully", requestID)
			return nil
		case "error", "failed":
			msg, _ := status["message"].(string)
			return fmt.Errorf("sandbox request %s failed: %s", requestID, msg)
		default:
			log.Printf("pollSandboxRequest: request %s status=%s, continuing", requestID, state)
		}
	}

	return fmt.Errorf("sandbox request %s timed out after %d attempts", requestID, maxAttempts)
}

// extractSandboxVars extracts dynamic job variables and labels from a
// placement response. Resources contain credentials and account info
// that get merged into job_vars for Tower job extra_vars.
func extractSandboxVars(placement map[string]interface{}) (vars map[string]interface{}, labels map[string]string) {
	vars = make(map[string]interface{})
	labels = make(map[string]string)

	resources, ok := placement["resources"].([]interface{})
	if !ok || len(resources) == 0 {
		return vars, labels
	}

	for i, r := range resources {
		res, ok := r.(map[string]interface{})
		if !ok {
			continue
		}

		name, _ := res["name"].(string)
		kind, _ := res["kind"].(string)
		cloud, _ := res["cloud"].(string)

		if name != "" {
			if i == 0 {
				labels["sandbox"] = name
			}
			vars[fmt.Sprintf("sandbox_name_%d", i)] = name
		}
		if kind != "" {
			vars[fmt.Sprintf("sandbox_kind_%d", i)] = kind
		}
		if cloud != "" {
			vars[fmt.Sprintf("sandbox_cloud_%d", i)] = cloud
		}

		// Extract credentials (e.g. AWS keys, region).
		creds, ok := res["credentials"].(map[string]interface{})
		if ok {
			for k, v := range creds {
				vars[k] = v
			}
		}

		// Extract additional resource metadata.
		for _, field := range []string{"hosted_zone_id", "account_id", "zone", "project_id"} {
			if v, ok := res[field]; ok {
				vars[field] = v
			}
		}
	}

	return vars, labels
}
