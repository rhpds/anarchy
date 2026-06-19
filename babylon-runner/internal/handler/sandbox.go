package handler

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/rhpds/anarchy/babylon-runner/internal/clients"
	"github.com/rhpds/anarchy/babylon-runner/internal/httputil"
	"github.com/rhpds/anarchy/babylon-runner/internal/runner"
	"github.com/rhpds/anarchy/babylon-runner/internal/template"
	"github.com/rhpds/anarchy/babylon-runner/internal/types"
)

// SandboxResult holds the result of a sandbox API get or book operation.
type SandboxResult struct {
	Status      string                 // "success", "queued", "error", "not-found"
	Placement   map[string]interface{} // raw placement data
	DynamicVars map[string]interface{} // extracted job vars (with creds) for Tower extra_vars
	SubjectVars map[string]interface{} // extracted vars (no creds) for subject/provision_data
	Labels      map[string]string      // extracted labels
}

// sandboxAPIVars returns the sandbox_api varSecret data from
// governor.spec.vars.sandbox_api. This is injected by the Anarchy operator
// from a K8s Secret configured in the governor's varSecrets.
func sandboxAPIVars(rc *runner.RunContext) map[string]interface{} {
	return rc.Payload.Governor.Spec.Vars.SandboxAPI
}

// getSandboxClient creates a SandboxAPIClient with integrated login.
// URL priority: rc.SandboxBaseURL (test) > governor varSecret > rc.DefaultSandboxAPIURL (config).
func getSandboxClient(rc *runner.RunContext) (*clients.SandboxAPIClient, error) {
	loginToken := sandboxLoginToken(rc)
	if loginToken == "" {
		return nil, fmt.Errorf("no sandbox_api_login_token in governor sandbox_api secret")
	}

	baseURL := rc.DefaultSandboxAPIURL
	if rc.SandboxBaseURL != "" {
		baseURL = rc.SandboxBaseURL
	} else {
		sbVars := sandboxAPIVars(rc)
		if sbVars != nil {
			if u, ok := sbVars["sandbox_api_url"].(string); ok && u != "" {
				baseURL = u
			}
		}
	}
	return clients.NewSandboxAPIClient(baseURL, loginToken, rc.SandboxClientOpts...), nil
}

// sandboxLoginToken returns the sandbox API login token from
// governor.spec.vars.sandbox_api.sandbox_api_login_token (varSecret).
// Matches the Ansible default: {{ sandbox_api.sandbox_api_login_token }}.
func sandboxLoginToken(rc *runner.RunContext) string {
	sbVars := sandboxAPIVars(rc)
	if sbVars == nil {
		return ""
	}
	token, _ := sbVars["sandbox_api_login_token"].(string)
	return token
}

// sandboxGet performs the sandbox get workflow:
//  1. Create sandbox API client (with integrated login)
//  2. GET placement by UUID
//  3. If error status -> return error result
//  4. If found with resources -> extract vars, update subject
//  5. If action is "provision" and not found -> book new placement
func sandboxGet(rc *runner.RunContext, action string) (*SandboxResult, error) {
	uuid := rc.UUID()
	if uuid == "" {
		return nil, fmt.Errorf("no uuid in job_vars")
	}

	client, err := getSandboxClient(rc)
	if err != nil {
		return nil, err
	}

	ctx := rc.Ctx
	placement, statusCode, err := client.GetPlacement(ctx, uuid)
	if err != nil {
		return nil, fmt.Errorf("get placement: %w", err)
	}

	// Not found -- book if provision action.
	if statusCode == http.StatusNotFound {
		if action == "provision" && sandboxLoginToken(rc) != "" {
			return sandboxBook(rc, client)
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
		patch := types.PatchBody{SkipUpdateProcessing: true}
		if len(labels) > 0 {
			patch.Metadata = &types.PatchMetadata{Labels: labels}
		}
		if len(subjectVars) > 0 {
			// Merge non-secret vars into existing job_vars.
			jv := rc.JobVars()
			if jv == nil {
				jv = make(map[string]interface{})
			}
			types.DeepMergeMap(jv, subjectVars)
			patch.Spec = &types.PatchSpec{
				Vars: map[string]interface{}{
					"job_vars": jv,
				},
			}
		}
		if err := rc.SubjectUpdate(types.SubjectPatch{Patch: patch}); err != nil {
			return nil, fmt.Errorf("update subject with sandbox vars: %w", err)
		}
	}

	return &SandboxResult{
		Status:      "success",
		Placement:   placement,
		DynamicVars: dynamicVars,
		SubjectVars: subjectVars,
		Labels:      labels,
	}, nil
}

// sandboxBook books a new placement via the sandbox API.
// The client is passed from the caller (sandboxGet) so the same
// token cache is reused.
func sandboxBook(rc *runner.RunContext, client *clients.SandboxAPIClient) (*SandboxResult, error) {
	uuid := rc.UUID()
	guid := rc.GUID()
	meta := rc.Meta()

	// Build request body.
	reqBody := map[string]interface{}{
		"service_uuid": uuid,
	}

	// Add reservation from __meta__.sandbox_api.reservation.
	if meta != nil && meta.SandboxAPI != nil {
		if reservation, ok := meta.SandboxAPI["reservation"].(string); ok && reservation != "" {
			reqBody["reservation"] = reservation
		}
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
		// owner = requester_username or student_name or "babylon"
		owner := types.FirstString(
			rc.Payload.Subject.Metadata.Annotations["poolboy.gpte.redhat.com/resource-requester-user"],
			types.StringFromMap(subjectJobVars, "requester_username"),
			types.StringFromMap(subjectJobVars, "student_name"),
			"babylon",
		)
		annotations["owner"] = owner

		// owner_email = requester_email or email or "unknown"
		email := types.FirstString(
			rc.Payload.Subject.Metadata.Annotations["poolboy.gpte.redhat.com/resource-requester-email"],
			types.StringFromMap(subjectJobVars, "requester_email"),
			types.StringFromMap(subjectJobVars, "email"),
			"unknown",
		)
		annotations["owner_email"] = email
	}
	annotations["comment"] = "sandbox-api"
	reqBody["annotations"] = annotations

	// Add sandboxes/resources from __meta__ with var annotations injected.
	// Resolve Jinja2 expressions (e.g. {{ job_vars.namespace_suffix | default('dev') }})
	// before sending to the sandbox API.
	if meta != nil && meta.Sandboxes != nil {
		vars := template.J2VarContext(rc.SubjectAllVars(), rc.GovernorAllVars())
		resolved := template.ResolveJ2(meta.Sandboxes, vars).([]interface{})
		reqBody["resources"] = injectVarAnnotations(resolved)
	}

	ctx := rc.Ctx
	result, statusCode, err := client.BookPlacement(ctx, reqBody)
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
func sandboxCleanup(rc *runner.RunContext) error {
	uuid := rc.UUID()
	if uuid == "" {
		slog.Warn("sandboxCleanup: no uuid, skipping")
		return nil
	}

	token := sandboxLoginToken(rc)
	if token == "" {
		slog.Warn("sandboxCleanup: no login token, skipping")
		return nil
	}

	client, err := getSandboxClient(rc)
	if err != nil {
		return fmt.Errorf("sandbox cleanup: %w", err)
	}

	ctx := rc.Ctx
	if err := client.ReleasePlacement(ctx, uuid); err != nil {
		return fmt.Errorf("release placement: %w", err)
	}

	slog.Info("sandboxCleanup: released placement", "uuid", uuid)
	return nil
}

// sandboxStart starts the sandbox placement and polls for completion.
func sandboxStart(rc *runner.RunContext) error {
	uuid := rc.UUID()
	if uuid == "" {
		return fmt.Errorf("no uuid for sandbox start")
	}

	client, err := getSandboxClient(rc)
	if err != nil {
		return err
	}

	ctx := rc.Ctx
	result, err := client.StartPlacement(ctx, uuid)
	if err != nil {
		return fmt.Errorf("start placement: %w", err)
	}

	// Extract request_id for polling.
	requestID, _ := result["request_id"].(string)
	if requestID == "" {
		return nil
	}

	// Update subject status with request info.
	if err := rc.SubjectUpdate(types.SubjectPatch{
		Patch: types.PatchBody{
			Metadata: &types.PatchMetadata{
				Labels: map[string]string{"state": "starting"},
			},
			Status: map[string]interface{}{
				"sandboxAPIJobs": map[string]interface{}{
					"start": map[string]interface{}{
						"requestID":      requestID,
						"startTimestamp": types.NowUTC(),
					},
				},
			},
			SkipUpdateProcessing: true,
		},
	}); err != nil {
		return err
	}

	return pollSandboxRequest(ctx, client, requestID)
}

// sandboxStop stops the sandbox placement and polls for completion.
func sandboxStop(rc *runner.RunContext) error {
	uuid := rc.UUID()
	if uuid == "" {
		return fmt.Errorf("no uuid for sandbox stop")
	}

	client, err := getSandboxClient(rc)
	if err != nil {
		return err
	}

	ctx := rc.Ctx
	result, err := client.StopPlacement(ctx, uuid)
	if err != nil {
		return fmt.Errorf("stop placement: %w", err)
	}

	// Extract request_id for polling.
	requestID, _ := result["request_id"].(string)
	if requestID == "" {
		return nil
	}

	// Update subject status with request info.
	if err := rc.SubjectUpdate(types.SubjectPatch{
		Patch: types.PatchBody{
			Metadata: &types.PatchMetadata{
				Labels: map[string]string{"state": "stopping"},
			},
			Status: map[string]interface{}{
				"sandboxAPIJobs": map[string]interface{}{
					"stop": map[string]interface{}{
						"requestID":      requestID,
						"startTimestamp": types.NowUTC(),
					},
				},
			},
			SkipUpdateProcessing: true,
		},
	}); err != nil {
		return err
	}

	return pollSandboxRequest(ctx, client, requestID)
}

// pollSandboxRequest polls a sandbox API async request until it completes.
// Uses httputil.PollWithContext for cancellation-aware polling.
func pollSandboxRequest(ctx context.Context, client *clients.SandboxAPIClient, requestID string) error {
	return httputil.PollWithContext(ctx, 5*time.Second, 120, func() (bool, error) {
		status, err := client.GetRequestStatus(ctx, requestID)
		if err != nil {
			slog.Error("pollSandboxRequest: error checking request", "requestID", requestID, "error", err)
			// Transient error — keep polling.
			return false, nil
		}

		state, _ := status["status"].(string)
		switch state {
		case "success", "complete":
			slog.Info("pollSandboxRequest: request completed successfully", "requestID", requestID)
			return true, nil
		case "error", "failed":
			msg, _ := status["message"].(string)
			return true, fmt.Errorf("sandbox request %s failed: %s", requestID, msg)
		default:
			slog.Info("pollSandboxRequest: request still in progress", "requestID", requestID, "status", state)
			return false, nil
		}
	})
}

// injectVarAnnotations copies the "var" and "namespace_suffix" fields
// from each sandbox resource into its annotations, matching the Python
// inject_var_annotations filter.
func injectVarAnnotations(sandboxes []interface{}) []interface{} {
	result := make([]interface{}, len(sandboxes))
	for i, s := range sandboxes {
		sb, ok := s.(map[string]interface{})
		if !ok {
			result[i] = s
			continue
		}
		// Shallow copy to avoid mutating the original.
		cp := make(map[string]interface{}, len(sb))
		for k, v := range sb {
			cp[k] = v
		}
		ann, _ := cp["annotations"].(map[string]interface{})
		if ann == nil {
			ann = make(map[string]interface{})
		} else {
			// Copy annotations too.
			annCopy := make(map[string]interface{}, len(ann))
			for k, v := range ann {
				annCopy[k] = v
			}
			ann = annCopy
		}
		if v, ok := cp["var"].(string); ok && v != "" {
			ann["var"] = v
		}
		if v, ok := cp["namespace_suffix"].(string); ok && v != "" {
			ann["namespace_suffix"] = v
		}
		if len(ann) > 0 {
			cp["annotations"] = ann
		}
		result[i] = cp
	}
	return result
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
	additionalVars := types.GetNestedMap(res, "cluster_additional_vars", "deployer")
	if additionalVars == nil {
		additionalVars = types.GetNestedMap(res, "additional_vars", "deployer")
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
	additionalVars := types.GetNestedMap(res, "additional_vars", "deployer")
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
			types.DeepMergeMap(vars, toMerge)
		} else {
			vars[varName] = toMerge
		}
	}

	// Add full sandboxes response for downstream use when creds included.
	if creds {
		vars["sandboxes"] = types.DeepCopySlice(resources)
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
