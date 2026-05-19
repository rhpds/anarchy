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
// When SandboxBaseURL is set (test mode), retries and delays are minimized.
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
	client := NewSandboxAPIClient(baseURL)
	// When SandboxBaseURL is set, we're in test mode — use fast retries.
	if rc.SandboxBaseURL != "" {
		client.loginRetries = 1
		client.loginDelay = 0
	}
	return client
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

	// Extract vars (without creds for subject) and labels.
	subjectVars := extractSandboxVars(placement, false)
	labels := extractSandboxLabels(placement)
	// Extract vars with creds for Tower extra_vars.
	dynamicVars := extractSandboxVars(placement, true)

	if len(subjectVars) > 0 || len(labels) > 0 {
		patch := PatchBody{SkipUpdateProcessing: true}
		if len(labels) > 0 {
			patch.Metadata = &PatchMetadata{Labels: labels}
		}
		if len(subjectVars) > 0 {
			// Merge non-secret vars into existing job_vars.
			jv := rc.JobVars()
			if jv == nil {
				jv = make(map[string]interface{})
			}
			mergeMap(jv, subjectVars)
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
		dynamicVars := extractSandboxVars(result, true)
		labels := extractSandboxLabels(result)
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
// It uses pollInterval between attempts, with up to maxAttempts tries.
// When pollInterval is 0 (e.g. in tests), it polls without sleeping.
func pollSandboxRequest(client *SandboxAPIClient, accessToken, requestID string) error {
	maxAttempts := 120 // ~10 minutes at 5s intervals
	pollInterval := 5 * time.Second
	if client.loginDelay == 0 {
		// Test mode: don't sleep between polls.
		pollInterval = 0
		maxAttempts = 5
	}

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

// getStringDefault returns the string value of a key from a map,
// returning defaultVal if missing or not a string.
func getStringDefault(m map[string]interface{}, key, defaultVal string) string {
	v, ok := m[key]
	if !ok {
		return defaultVal
	}
	s, ok := v.(string)
	if !ok {
		return defaultVal
	}
	return s
}

// getCredList returns the credentials list from a resource map.
func getCredList(res map[string]interface{}) []map[string]interface{} {
	raw, ok := res["credentials"].([]interface{})
	if !ok {
		return nil
	}
	var creds []map[string]interface{}
	for _, c := range raw {
		if cm, ok := c.(map[string]interface{}); ok {
			creds = append(creds, cm)
		}
	}
	return creds
}

// deepCopySlice creates a deep copy of a []interface{} value.
func deepCopySlice(src []interface{}) []interface{} {
	dst := make([]interface{}, len(src))
	for i, v := range src {
		switch vv := v.(type) {
		case map[string]interface{}:
			dst[i] = deepCopyMap(vv)
		case []interface{}:
			dst[i] = deepCopySlice(vv)
		default:
			dst[i] = v
		}
	}
	return dst
}

// deepCopyMap creates a deep copy of a map[string]interface{} value.
func deepCopyMap(src map[string]interface{}) map[string]interface{} {
	dst := make(map[string]interface{}, len(src))
	for k, v := range src {
		switch vv := v.(type) {
		case map[string]interface{}:
			dst[k] = deepCopyMap(vv)
		case []interface{}:
			dst[k] = deepCopySlice(vv)
		default:
			dst[k] = v
		}
	}
	return dst
}

// extractAwsSandboxVars extracts vars from an AwsSandbox resource.
func extractAwsSandboxVars(res map[string]interface{}, creds bool) map[string]interface{} {
	name := getStringDefault(res, "name", "unknown")
	hostedZoneID := getStringDefault(res, "hosted_zone_id", "unknown")
	accountID := getStringDefault(res, "account_id", "unknown")
	zone := getStringDefault(res, "zone", "unknown")

	toMerge := map[string]interface{}{
		"sandbox_name":           name,
		"sandbox_hosted_zone_id": hostedZoneID,
		"HostedZoneId":           hostedZoneID,
		"sandbox_account":        accountID,
		"sandbox_account_id":     accountID,
		"sandbox_zone":           zone,
		"subdomain_base_suffix":  "." + zone,
	}

	if creds {
		for _, cred := range getCredList(res) {
			if getStringDefault(cred, "kind", "none") == "aws_iam_key" {
				toMerge["sandbox_aws_access_key_id"] = getStringDefault(cred, "aws_access_key_id", "unknown")
				toMerge["aws_access_key_id"] = getStringDefault(cred, "aws_access_key_id", "unknown")
				toMerge["sandbox_aws_secret_access_key"] = getStringDefault(cred, "aws_secret_access_key", "unknown")
				toMerge["aws_secret_access_key"] = getStringDefault(cred, "aws_secret_access_key", "unknown")
				// Include raw credentials list.
				rawCreds, _ := res["credentials"].([]interface{})
				toMerge["sandbox_credentials"] = rawCreds
				break
			}
		}
	}

	return toMerge
}

// extractOcpSandboxVars extracts vars from an OcpSandbox resource.
func extractOcpSandboxVars(res map[string]interface{}, creds bool) map[string]interface{} {
	toMerge := map[string]interface{}{
		"sandbox_openshift_name":        getStringDefault(res, "name", "unknown"),
		"sandbox_openshift_namespace":   getStringDefault(res, "namespace", "unknown"),
		"sandbox_openshift_cluster":     getStringDefault(res, "ocp_cluster", "unknown"),
		"sandbox_openshift_api_url":     getStringDefault(res, "api_url", "unknown"),
		"sandbox_openshift_apps_domain": getStringDefault(res, "ingress_domain", "unknown"),
		"sandbox_openshift_console_url": getStringDefault(res, "console_url", "unknown"),
	}

	if creds {
		for _, cred := range getCredList(res) {
			credKind := getStringDefault(cred, "kind", "none")
			if credKind == "ServiceAccount" {
				toMerge["sandbox_openshift_api_key"] = getStringDefault(cred, "token", "unknown")
				toMerge["sandbox_openshift_api_token"] = getStringDefault(cred, "token", "unknown")
				rawCreds, _ := res["credentials"].([]interface{})
				toMerge["sandbox_openshift_credentials"] = rawCreds
			}
			if credKind == "KeycloakUser" {
				toMerge["sandbox_openshift_user"] = getStringDefault(cred, "username", "unknown")
				toMerge["sandbox_openshift_password"] = getStringDefault(cred, "password", "unknown")
			}
		}
	}

	// Merge additional vars from cluster_additional_vars or additional_vars.
	additionalVars := getNestedMap(res, "cluster_additional_vars", "deployer")
	if additionalVars == nil {
		additionalVars = getNestedMap(res, "additional_vars", "deployer")
	}
	for k, v := range additionalVars {
		toMerge[k] = v
	}

	return toMerge
}

// extractIBMSandboxVars extracts vars from an IBMResourceGroupSandbox resource.
func extractIBMSandboxVars(res map[string]interface{}, creds bool) map[string]interface{} {
	toMerge := make(map[string]interface{})

	if creds {
		credList := getCredList(res)
		for _, cred := range credList {
			if getStringDefault(cred, "apikey", "") != "" {
				// Use first credential entry's apikey and name.
				if len(credList) > 0 {
					toMerge["ibmcloud_api_key"] = getStringDefault(credList[0], "apikey", "unknown")
					toMerge["ibmcloud_resource_group_name"] = getStringDefault(credList[0], "name", "unknown")
				}
				break
			}
		}
	}

	// Merge additional vars.
	additionalVars := getNestedMap(res, "additional_vars", "deployer")
	for k, v := range additionalVars {
		toMerge[k] = v
	}

	return toMerge
}

// extractSandboxVars extracts dynamic job variables from a placement
// response, matching the Python extract_sandboxes_vars filter.
// When creds is true, credential data is included (for Tower extra_vars).
// When creds is false, only non-secret data is included (for subject job_vars).
func extractSandboxVars(placement map[string]interface{}, creds bool) map[string]interface{} {
	vars := make(map[string]interface{})

	resources, ok := placement["resources"].([]interface{})
	if !ok || len(resources) == 0 {
		return vars
	}

	for _, r := range resources {
		res, ok := r.(map[string]interface{})
		if !ok {
			continue
		}

		kind := getStringDefault(res, "kind", "none")

		var toMerge map[string]interface{}
		switch kind {
		case "AwsSandbox":
			toMerge = extractAwsSandboxVars(res, creds)
		case "OcpSandbox":
			toMerge = extractOcpSandboxVars(res, creds)
		case "IBMResourceGroupSandbox":
			toMerge = extractIBMSandboxVars(res, creds)
		default:
			// Any other kind: include raw credentials only.
			toMerge = make(map[string]interface{})
			if creds {
				rawCreds, _ := res["credentials"].([]interface{})
				if rawCreds != nil {
					toMerge["credentials"] = rawCreds
				}
			}
		}

		// Determine target var name from annotations.
		varName := "main"
		if annotations, ok := res["annotations"].(map[string]interface{}); ok {
			if v, ok := annotations["var"].(string); ok && v != "" {
				varName = v
			}
		}

		if varName == "main" {
			mergeMap(vars, toMerge)
		} else {
			vars[varName] = toMerge
		}
	}

	// Add full sandboxes response for downstream use when creds included.
	if creds {
		vars["sandboxes"] = deepCopySlice(resources)
	}

	return vars
}

// extractSandboxLabels extracts labels from a placement response,
// matching the Python extract_sandboxes_labels filter.
func extractSandboxLabels(placement map[string]interface{}) map[string]string {
	resources, ok := placement["resources"].([]interface{})
	if !ok || len(resources) == 0 {
		return map[string]string{}
	}

	if len(resources) == 1 {
		res, ok := resources[0].(map[string]interface{})
		if !ok {
			return map[string]string{}
		}
		kind := sanitizeKind(getStringDefault(res, "kind", "none"))
		name := getStringDefault(res, "name", "unknown")
		return map[string]string{
			"sandbox": name,
			kind:      name,
		}
	}

	ret := make(map[string]string)
	increment := 2
	for _, r := range resources {
		res, ok := r.(map[string]interface{})
		if !ok {
			continue
		}
		kind := sanitizeKind(getStringDefault(res, "kind", "none"))
		name := getStringDefault(res, "name", "unknown")

		if ret["sandbox"] == "" {
			ret["sandbox"] = name
		}

		if _, exists := ret[kind]; exists {
			ret[fmt.Sprintf("%s%d", kind, increment)] = name
			increment++
		} else {
			ret[kind] = name
		}
	}

	return ret
}

// sanitizeKind removes non-alphanumeric characters from a kind string.
func sanitizeKind(kind string) string {
	result := make([]byte, 0, len(kind))
	for i := 0; i < len(kind); i++ {
		c := kind[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') {
			result = append(result, c)
		}
	}
	return string(result)
}
