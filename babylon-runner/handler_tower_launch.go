package main

import (
	"encoding/base64"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"regexp"
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

// getDeployerEntryPoint returns the playbook path for the given action.
// Lookup order (matching Ansible defaults/main.yaml):
//  1. __meta__.deployer.actions.{action}.entry_point
//  2. __meta__.deployer.entry_point (generic)
//  3. Per-action default from defaultEntryPoints
func getDeployerEntryPoint(deployer map[string]interface{}, action string) string {
	if deployer != nil {
		// 1. Per-action entry point.
		actionEP := getNestedString(deployer, "actions", action, "entry_point")
		if actionEP != "" {
			return actionEP
		}
		// 2. Generic deployer entry point.
		genericEP := getNestedString(deployer, "entry_point")
		if genericEP != "" {
			return genericEP
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
	if hostname == "" {
		return nil, "", fmt.Errorf("controller has no hostname")
	}

	username, password, err := resolveControllerCreds(rc, controller)
	if err != nil {
		return nil, "", fmt.Errorf("credentials for %s: %w", hostname, err)
	}

	return NewTowerClient(hostname, username, password), hostname, nil
}

// resolveControllerCreds returns (username, password) for a controller entry.
// It tries, in order:
//  1. user/password fields directly on the controller map
//  2. babylon_tower varSecret from governor vars
//  3. K8s Secret with label babylon.gpte.redhat.com/ansible-control-plane={hostname}
func resolveControllerCreds(rc *RunContext, controller map[string]interface{}) (string, string, error) {
	hostname, _ := controller["hostname"].(string)
	username, _ := controller["user"].(string)
	password, _ := controller["password"].(string)

	if username != "" && password != "" {
		return username, password, nil
	}

	// Fallback 1: babylon_tower varSecret (resolved by operator into governor vars).
	bt := getNestedMap(rc.Payload.Governor, "spec", "vars", "babylon_tower")
	if bt != nil {
		if u, ok := bt["user"].(string); ok && username == "" {
			username = u
		}
		if p, ok := bt["password"].(string); ok && password == "" {
			password = p
		}
		if username != "" && password != "" {
			return username, password, nil
		}
	}

	// Fallback 2: K8s Secret lookup by controller hostname label.
	// Matches the Ansible role's kubernetes.core.k8s_info label selector.
	ns := os.Getenv("ANARCHY_NAMESPACE")
	if ns == "" {
		// Derive namespace from the pod's service account.
		if nsBytes, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace"); err == nil {
			ns = string(nsBytes)
		}
	}
	if ns != "" && hostname != "" {
		label := "babylon.gpte.redhat.com/ansible-control-plane=" + hostname
		data, err := k8sSecretData(ns, label)
		if err != nil {
			slog.Warn("k8s secret lookup failed", "hostname", hostname, "error", err)
		} else {
			if u, ok := data["user"]; ok && username == "" {
				decoded, _ := base64.StdEncoding.DecodeString(u)
				username = string(decoded)
			}
			if p, ok := data["password"]; ok && password == "" {
				decoded, _ := base64.StdEncoding.DecodeString(p)
				password = string(decoded)
			}
		}
	}

	if username == "" || password == "" {
		return "", "", fmt.Errorf("no credentials found (checked controller entry, babylon_tower varSecret, k8s secret)")
	}
	return username, password, nil
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

	// Debug: log what we got from each source.
	slog.Debug("buildJobExtraVars sources",
		"subjectJobVarsNil", rc.JobVars() == nil,
		"governorJobVarsNil", rc.GovernorJobVars() == nil,
		"dynamicJobVarsNil", dynamicJobVars == nil)

	// Remove __meta__ from merged vars.
	delete(extraVars, "__meta__")

	// Debug: log final extra_vars keys.
	keys := make([]string, 0, len(extraVars))
	for k := range extraVars {
		keys = append(keys, k)
	}
	slog.Debug("buildJobExtraVars", "numVars", len(extraVars), "keys", keys)

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
			username, password, err := resolveControllerCreds(rc, m)
			if err != nil {
				return nil, fmt.Errorf("credentials for %s: %w", hostname, err)
			}
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

	// Organization from __meta__.tower.organization (default "babylon").
	tower := getNestedMap(meta, "tower")
	org := "babylon"
	if tower != nil {
		if o, ok := tower["organization"].(string); ok && o != "" {
			org = o
		}
	}
	// Timeout from __meta__.tower.timeout (default 10800).
	timeout := 10800
	if tower != nil {
		if t, ok := tower["timeout"].(float64); ok && t > 0 {
			timeout = int(t)
		}
	}
	// Inventory name: "{org} {tower.inventory|default('default')}".
	inventorySuffix := "default"
	if tower != nil {
		if inv, ok := tower["inventory"].(string); ok && inv != "" {
			inventorySuffix = inv
		}
	}
	inventoryName := org + " " + inventorySuffix
	// Template name: "{org} {anarchy_action_name} {uuid}".
	// anarchy_action_name is the K8s AnarchyAction resource name.
	actionResourceName := getNestedString(rc.Payload.Action, "metadata", "name")
	uuid := rc.UUID()
	templateName := fmt.Sprintf("%s %s %s", org, actionResourceName, uuid)

	// Project name: "{org} {scm_url} ({scm_ref})".
	projectName := fmt.Sprintf("%s %s (%s)", org, scmURL, scmRef)

	// SCM settings (matching Ansible run-tower-job.yaml):
	// scm_update_on_launch: true unless scm_ref is a release version (digits.digits).
	scmUpdateOnLaunch := true
	if regexp.MustCompile(`\d+\.\d+$`).MatchString(scmRef) {
		scmUpdateOnLaunch = false
	}
	// scm_update_cache_timeout: __meta__.deployer.scm_update_cache_timeout (default 30).
	scmUpdateCacheTimeout := 30
	if deployer != nil {
		if v, ok := deployer["scm_update_cache_timeout"].(float64); ok {
			scmUpdateCacheTimeout = int(v)
		}
	}
	// scm_clean: __meta__.deployer.scm_clean (default true).
	scmClean := true
	if deployer != nil {
		if v, ok := deployer["scm_clean"].(bool); ok {
			scmClean = v
		}
	}

	// Execution environment (AAP2/Controller only).
	// ansible_control_plane type from __meta__.ansible_control_plane.type (default "tower").
	var eeConfig *EEConfig
	controlPlaneType := "tower"
	acp := getNestedMap(meta, "ansible_control_plane")
	if acp != nil {
		if t, ok := acp["type"].(string); ok && t != "" {
			controlPlaneType = t
		}
	}
	if controlPlaneType == "controller" {
		ee := getNestedMap(deployer, "execution_environment")
		if ee != nil {
			eeImage, _ := ee["image"].(string)
			if eeImage != "" {
				eeName, _ := ee["name"].(string)
				if eeName == "" {
					eeName = org + " " + eeImage
				}
				eePull, _ := ee["pull"].(string)
				eePrivate, _ := ee["private"].(bool)
				eeConfig = &EEConfig{
					Name:    eeName,
					Image:   eeImage,
					Pull:    eePull,
					Private: eePrivate,
				}
			}
		}
	}

	// Instance groups: per-action first, then global fallback.
	// __meta__.deployer.actions.{action}.ansible_control_plane.instance_groups
	// | default(__meta__.ansible_control_plane.instance_groups)
	// | default([])
	var instanceGroups []string
	actionACP := getNestedMap(deployer, "actions", action, "ansible_control_plane")
	if actionACP != nil {
		instanceGroups = extractStringSlice(actionACP, "instance_groups")
	}
	if len(instanceGroups) == 0 && acp != nil {
		instanceGroups = extractStringSlice(acp, "instance_groups")
	}

	// Static credentials from __meta__.deployer.credentials (default []).
	var credentials []string
	if deployer != nil {
		credentials = extractStringSlice(deployer, "credentials")
	}

	// Vault credentials from governor.spec.vars.vault_credentials (default {}).
	// Injected by the operator via varSecrets; always empty if not configured.
	var vaultCredentials map[string]string
	vc := getNestedMap(rc.Payload.Governor, "spec", "vars", "vault_credentials")
	if len(vc) > 0 {
		vaultCredentials = make(map[string]string, len(vc))
		for k, v := range vc {
			if s, ok := v.(string); ok {
				vaultCredentials[k] = s
			}
		}
	}

	config := TowerJobConfig{
		Organization:          org,
		Inventory:             inventoryName,
		ProjectName:           projectName,
		ProjectSCMURL:         scmURL,
		ProjectSCMRef:         scmRef,
		SCMUpdateOnLaunch:     scmUpdateOnLaunch,
		SCMUpdateCacheTimeout: scmUpdateCacheTimeout,
		SCMClean:              scmClean,
		TemplateName:          templateName,
		Playbook:              playbook,
		ExtraVars:             insertUnvaultString(buildJobExtraVars(rc, action, dynamicJobVars)),
		Timeout:               timeout,
		ExecutionEnvironment:  eeConfig,
		InstanceGroups:        instanceGroups,
		Credentials:           credentials,
		VaultCredentials:      vaultCredentials,
	}

	slog.Info("launching tower job", "action", action, "subject", rc.SubjectName, "entryPoint", playbook)

	jobID, err := tc.LaunchJob(config)
	if err != nil {
		return fmt.Errorf("launch tower job: %w", err)
	}

	slog.Info("tower job launched", "jobID", jobID, "action", action, "subject", rc.SubjectName)

	// Build the subject update patch.
	patchBody := PatchBody{
		Status: map[string]interface{}{
			"towerJobs": map[string]interface{}{
				action: map[string]interface{}{
					"deployerJob":      float64(jobID),
					"startTimestamp":    nowUTC(),
					"completeTimestamp": nil,
					"towerHost":        hostname,
					"towerJobURL":      fmt.Sprintf("%s/#/jobs/%d/", hostname, jobID),
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
		slog.Warn("cancelTowerJob: no towerHost in job status", "action", action)
		return
	}

	tc, err := getTowerClientForHost(rc, towerHost)
	if err != nil {
		slog.Error("cancelTowerJob: cannot get tower client", "host", towerHost, "error", err)
		return
	}

	token, tokenID, err := tc.CreateOAuthToken()
	if err != nil {
		slog.Error("cancelTowerJob: cannot create oauth token", "error", err)
		return
	}
	defer func() { _ = tc.DeleteOAuthToken(tokenID) }()

	if err := tc.CancelJob(token, int(jobIDFloat)); err != nil {
		slog.Error("cancelTowerJob: failed to cancel job", "job", int(jobIDFloat), "error", err)
	} else {
		slog.Info("cancelTowerJob: canceled job", "job", int(jobIDFloat), "action", action)
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
