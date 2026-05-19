package main

import (
	"fmt"
	"log"
	"net/http"
)

// Default deployer entry points from babylon governor defaults/main.yaml.
var defaultEntryPoints = map[string]string{
	"provision": "ansible/main.yml",
	"destroy":   "ansible/destroy.yml",
	"start":     "ansible/lifecycle_entry_point.yml",
	"stop":      "ansible/lifecycle_entry_point.yml",
	"status":    "ansible/lifecycle_entry_point.yml",
	"update":    "ansible/lifecycle_entry_point.yml",
}

// getDeployerEntryPoint returns the playbook path for the given action,
// falling back to the default if not configured in __meta__.deployer.
func getDeployerEntryPoint(deployer map[string]interface{}, action string) string {
	if deployer != nil {
		ep := getNestedMap(deployer, "entry_points")
		if ep != nil {
			if v, ok := ep[action].(string); ok && v != "" && v != "disabled" && v != "none" {
				return v
			}
		}
	}
	if def, ok := defaultEntryPoints[action]; ok {
		return def
	}
	return "ansible/main.yml"
}

// getTowerClientForAction creates a TowerClient from the governor's
// __meta__.ansible_controllers configuration. Returns the client, the
// controller hostname, and any error.
func getTowerClientForAction(rc *RunContext) (*TowerClient, string, error) {
	if rc.TowerBaseURL != "" {
		return &TowerClient{
			baseURL: rc.TowerBaseURL,
			client:  &http.Client{},
		}, "test-tower", nil
	}

	meta := rc.Meta()
	if meta == nil {
		return nil, "", fmt.Errorf("no __meta__ in governor")
	}

	controllersRaw, ok := meta["ansible_controllers"].([]interface{})
	if !ok || len(controllersRaw) == 0 {
		return nil, "", fmt.Errorf("no ansible_controllers in __meta__")
	}

	var controllers []map[string]interface{}
	for _, c := range controllersRaw {
		if m, ok := c.(map[string]interface{}); ok {
			controllers = append(controllers, m)
		}
	}
	if len(controllers) == 0 {
		return nil, "", fmt.Errorf("no valid controllers in ansible_controllers")
	}

	mode := "random"
	if m, ok := meta["ansible_controller_select_mode"].(string); ok && m != "" {
		mode = m
	}
	controller := selectController(controllers, mode)
	if controller == nil {
		return nil, "", fmt.Errorf("no controller selected")
	}

	hostname, _ := controller["hostname"].(string)
	username, _ := controller["user"].(string)
	password, _ := controller["password"].(string)
	if hostname == "" {
		return nil, "", fmt.Errorf("controller has no hostname")
	}

	return NewTowerClient(hostname, username, password), hostname, nil
}

// buildJobExtraVars assembles the extra variables for a Tower job by
// merging subject job_vars, governor job_vars, and dynamic vars (sandbox
// creds), then applying action-specific overrides and callback vars.
// This matches the Ansible assembly order:
//
//	subject | combine(governor) | combine(dynamic) | remove_keys(__meta__)
//	| combine(action_extra_vars) | combine(callback_vars)
//	| combine(output_dir) | combine(scm_ref_var)
func buildJobExtraVars(rc *RunContext, action string, dynamicJobVars map[string]interface{}) map[string]interface{} {
	extraVars := make(map[string]interface{})

	// Subject first (lowest priority).
	if sjv := rc.JobVars(); sjv != nil {
		mergeMap(extraVars, sjv)
	}
	// Governor overrides subject.
	if gjv := rc.GovernorJobVars(); gjv != nil {
		mergeMap(extraVars, gjv)
	}
	// Dynamic vars (sandbox creds) override both.
	if dynamicJobVars != nil {
		mergeMap(extraVars, dynamicJobVars)
	}

	// Remove __meta__ from merged vars.
	delete(extraVars, "__meta__")

	// Action extra_vars from deployer config, defaulting to {"ACTION": action}.
	meta := rc.Meta()
	deployer := getNestedMap(meta, "deployer")
	actionExtraVars := getNestedMap(deployer, "actions", action, "extra_vars")
	if actionExtraVars != nil {
		mergeMap(extraVars, actionExtraVars)
	} else {
		extraVars["ACTION"] = action
	}

	// Callback vars with configurable var names.
	callbackURLVar := "agnosticd_callback_url"
	callbackTokenVar := "agnosticd_callback_token"
	if deployer != nil {
		if v, ok := deployer["callback_url_var"].(string); ok && v != "" {
			callbackURLVar = v
		}
		if v, ok := deployer["callback_token_var"].(string); ok && v != "" {
			callbackTokenVar = v
		}
	}
	if rc.Payload.Action != nil {
		if u := getNestedString(rc.Payload.Action, "spec", "callbackUrl"); u != "" {
			extraVars[callbackURLVar] = u
		}
		if t := getNestedString(rc.Payload.Action, "spec", "callbackToken"); t != "" {
			extraVars[callbackTokenVar] = t
		}
	}

	// output_dir if deployer_type is "agnosticd" (default).
	deployerType := "agnosticd"
	if deployer != nil {
		if dt, ok := deployer["type"].(string); ok && dt != "" {
			deployerType = dt
		}
	}
	if deployerType == "agnosticd" {
		uuid := rc.UUID()
		if uuid != "" {
			extraVars["output_dir"] = "/tmp/output-" + uuid
		}
	}

	// scm_ref_var injection.
	if deployer != nil {
		if scmRefVar, ok := deployer["scm_ref_var"].(string); ok && scmRefVar != "" {
			scmRef := getNestedString(deployer, "scm_ref")
			if scmRef != "" {
				extraVars[scmRefVar] = scmRef
			}
		}
	}

	return extraVars
}

// getTowerClientForHost creates a TowerClient for a specific controller
// hostname by finding the matching entry in __meta__.ansible_controllers.
// This is used by checkDeployerJob and cancelTowerJob to connect to the
// same controller where the job was originally launched.
func getTowerClientForHost(rc *RunContext, hostname string) (*TowerClient, error) {
	if rc.TowerBaseURL != "" {
		return &TowerClient{
			baseURL: rc.TowerBaseURL,
			client:  &http.Client{},
		}, nil
	}

	meta := rc.Meta()
	if meta == nil {
		return nil, fmt.Errorf("no __meta__ in governor")
	}

	controllersRaw, ok := meta["ansible_controllers"].([]interface{})
	if !ok || len(controllersRaw) == 0 {
		return nil, fmt.Errorf("no ansible_controllers in __meta__")
	}

	for _, c := range controllersRaw {
		m, ok := c.(map[string]interface{})
		if !ok {
			continue
		}
		h, _ := m["hostname"].(string)
		if h == hostname {
			username, _ := m["user"].(string)
			password, _ := m["password"].(string)
			return NewTowerClient(hostname, username, password), nil
		}
	}

	return nil, fmt.Errorf("controller %q not found in ansible_controllers", hostname)
}

// launchTowerJob selects a controller, builds the job config, launches
// the Tower job, and updates the subject status. If newState is non-empty,
// it also transitions labels and current_state. extraSpecVars are merged
// into the subject spec.vars patch (e.g. check_status_state for status).
// dynamicJobVars are sandbox vars with credentials, merged into Tower extra_vars.
func launchTowerJob(rc *RunContext, action, newState string, extraSpecVars, dynamicJobVars map[string]interface{}) error {
	meta := rc.Meta()
	deployer := getNestedMap(meta, "deployer")

	playbook := getDeployerEntryPoint(deployer, action)
	scmURL := getNestedString(deployer, "scm_url")
	scmRef := getNestedString(deployer, "scm_ref")

	tc, hostname, err := getTowerClientForAction(rc)
	if err != nil {
		return fmt.Errorf("get tower client: %w", err)
	}

	org := "babylon"
	if deployer != nil {
		if o, ok := deployer["organization"].(string); ok && o != "" {
			org = o
		}
	}
	timeout := 10800
	if deployer != nil {
		if t, ok := deployer["timeout"].(float64); ok && t > 0 {
			timeout = int(t)
		}
	}

	config := TowerJobConfig{
		Organization:  org,
		Inventory:     rc.SubjectName,
		ProjectSCMURL: scmURL,
		ProjectSCMRef: scmRef,
		TemplateName:  rc.SubjectName + "-" + action,
		Playbook:      playbook,
		ExtraVars:     buildJobExtraVars(rc, action, dynamicJobVars),
		Timeout:       timeout,
	}

	log.Printf("launching tower job action=%s subject=%s playbook=%s", action, rc.SubjectName, playbook)

	jobID, err := tc.LaunchJob(config)
	if err != nil {
		return fmt.Errorf("launch tower job: %w", err)
	}

	log.Printf("tower job %d launched action=%s subject=%s", jobID, action, rc.SubjectName)

	// Build the subject update patch.
	patchBody := PatchBody{
		Status: map[string]interface{}{
			"towerJobs": map[string]interface{}{
				action: map[string]interface{}{
					"deployerJob":      float64(jobID),
					"startTimestamp":    nowUTC(),
					"completeTimestamp": nil,
					"towerHost":        hostname,
				},
			},
		},
	}

	if newState != "" {
		patchBody.Metadata = &PatchMetadata{
			Labels: map[string]string{"state": newState},
		}
		patchBody.Spec = &PatchSpec{
			Vars: map[string]interface{}{
				"current_state": newState,
			},
		}
	}

	// Merge extra spec vars (e.g. check_status_state for status action).
	if len(extraSpecVars) > 0 {
		if patchBody.Spec == nil {
			patchBody.Spec = &PatchSpec{Vars: make(map[string]interface{})}
		}
		for k, v := range extraSpecVars {
			patchBody.Spec.Vars[k] = v
		}
	}

	return rc.SubjectUpdate(SubjectPatch{Patch: patchBody})
}

// cancelTowerJob cancels a running Tower job for the given action.
// Errors are logged but not returned to avoid failing the caller.
func cancelTowerJob(rc *RunContext, action string) {
	towerJobs := rc.StatusTowerJobs()
	jobInfo := getNestedMap(towerJobs, action)
	if jobInfo == nil {
		return
	}

	// Skip if already complete.
	if ct, ok := jobInfo["completeTimestamp"]; ok && ct != nil {
		return
	}

	jobIDFloat, ok := jobInfo["deployerJob"].(float64)
	if !ok || jobIDFloat == 0 {
		return
	}

	// Use towerHost from the job status to connect to the correct controller.
	towerHost, _ := jobInfo["towerHost"].(string)
	if towerHost == "" {
		log.Printf("cancelTowerJob: no towerHost in job status for action=%s", action)
		return
	}

	tc, err := getTowerClientForHost(rc, towerHost)
	if err != nil {
		log.Printf("cancelTowerJob: cannot get tower client for host=%s: %v", towerHost, err)
		return
	}

	token, tokenID, err := tc.CreateOAuthToken()
	if err != nil {
		log.Printf("cancelTowerJob: cannot create oauth token: %v", err)
		return
	}
	defer func() { _ = tc.DeleteOAuthToken(tokenID) }()

	if err := tc.CancelJob(token, int(jobIDFloat)); err != nil {
		log.Printf("cancelTowerJob: failed to cancel job %d: %v", int(jobIDFloat), err)
	} else {
		log.Printf("cancelTowerJob: canceled job %d for action=%s", int(jobIDFloat), action)
	}
}

// cancelAllIncompleteTowerJobs cancels every tower job in
// subject.status.towerJobs that has no completeTimestamp.
func cancelAllIncompleteTowerJobs(rc *RunContext) {
	towerJobs := rc.StatusTowerJobs()
	if towerJobs == nil {
		return
	}
	for action, v := range towerJobs {
		if _, ok := v.(map[string]interface{}); !ok {
			continue
		}
		cancelTowerJob(rc, action)
	}
}

// extractProvisionData extracts provision-specific data from the Tower
// job response artifacts.
func extractProvisionData(jobStatus map[string]interface{}) (data, messageBody, messages interface{}) {
	artifacts, _ := jobStatus["artifacts"].(map[string]interface{})
	if artifacts != nil {
		data = artifacts["provision_data"]
		messageBody = artifacts["provision_message_body"]
		messages = artifacts["provision_messages"]
	}
	return data, messageBody, messages
}
