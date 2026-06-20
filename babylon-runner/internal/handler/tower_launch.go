package handler

import (
	"fmt"
	"log/slog"
	"regexp"

	"github.com/rhpds/anarchy/babylon-runner/internal/clients"
	"github.com/rhpds/anarchy/babylon-runner/internal/runner"
	"github.com/rhpds/anarchy/babylon-runner/internal/types"
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
func getDeployerEntryPoint(meta *types.Meta, action string) string {
	if meta != nil && meta.Deployer != nil {
		if actionCfg, ok := meta.Deployer.Actions[action]; ok && actionCfg.EntryPoint != "" {
			return actionCfg.EntryPoint
		}
		if meta.Deployer.EntryPoint != "" {
			return meta.Deployer.EntryPoint
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
func getTowerClientForAction(rc *runner.RunContext) (*clients.TowerClient, string, error) {
	if rc.TowerBaseURL != "" {
		return clients.NewTowerClient(rc.TowerBaseURL, "", "", rc.TowerTLSConfig), "test-tower", nil
	}

	meta := rc.Meta()
	if meta == nil {
		return nil, "", fmt.Errorf("no __meta__ in governor")
	}

	if meta.ControllerScheduler != nil && meta.ControllerScheduler.URL != "" {
		selected, hostname, err := trySchedulerSelection(rc, meta)
		if err != nil {
			slog.Warn("controller-scheduler failed, falling back to local selection",
				"error", err)
		} else {
			return selected, hostname, nil
		}
	}

	controllers := meta.AnsibleControllers
	if len(controllers) == 0 {
		return nil, "", fmt.Errorf("no ansible_controllers in __meta__")
	}

	mode := meta.AnsibleControllerSelectMode
	if mode == "" {
		mode = "random"
	}
	controller := clients.SelectController(controllers, mode)
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

	return rc.TowerClientPool.Get(hostname, username, password, rc.TowerTLSConfig), hostname, nil
}

func trySchedulerSelection(rc *runner.RunContext, meta *types.Meta) (*clients.TowerClient, string, error) {
	cs := meta.ControllerScheduler

	apiKey, err := resolveSchedulerAPIKey(rc)
	if err != nil {
		return nil, "", fmt.Errorf("scheduler API key: %w", err)
	}

	var candidates []clients.Candidate
	for _, c := range meta.AnsibleControllers {
		if h, ok := c["hostname"].(string); ok && h != "" {
			candidates = append(candidates, clients.Candidate{Domain: h})
		}
	}

	scheduler := clients.NewSchedulerClient(cs.URL, apiKey, rc.TowerTLSConfig)
	resp, err := scheduler.Evaluate(rc.Ctx, clients.EvaluateRequest{
		Candidates:    candidates,
		RequireLabels: cs.RequireLabels,
		PreferLabels:  cs.PreferLabels,
		InstanceGroup: rc.ActionName(),
	})
	if err != nil {
		return nil, "", err
	}
	if len(resp.Ranked) == 0 {
		return nil, "", fmt.Errorf("scheduler returned empty ranking")
	}

	selectedHost := resp.Ranked[0].Domain
	slog.Info("controller-scheduler selected",
		"hostname", selectedHost,
		"score", resp.Ranked[0].Score)

	username, password, err := resolveControllerCreds(rc, map[string]interface{}{
		"hostname": selectedHost,
	})
	if err != nil {
		return nil, "", fmt.Errorf("credentials for scheduler-selected %s: %w", selectedHost, err)
	}

	return rc.TowerClientPool.Get(selectedHost, username, password, rc.TowerTLSConfig), selectedHost, nil
}

func resolveSchedulerAPIKey(rc *runner.RunContext) (string, error) {
	creds, _ := rc.Payload.Governor.Spec.Vars.Get("controller_scheduler_credentials").(map[string]interface{})
	if creds == nil {
		return "", fmt.Errorf("controller_scheduler_credentials not found in governor vars")
	}
	key, _ := creds["cluster_scheduler_api_key_governor"].(string)
	if key == "" {
		return "", fmt.Errorf("cluster_scheduler_api_key_governor is empty in controller_scheduler_credentials")
	}
	return key, nil
}

// resolveControllerCreds returns (username, password) for a controller by
// looking up the K8s Secret labeled babylon.gpte.redhat.com/ansible-control-plane={hostname}
// from the informer cache.
func resolveControllerCreds(rc *runner.RunContext, controller map[string]interface{}) (string, string, error) {
	hostname, _ := controller["hostname"].(string)
	if hostname == "" {
		return "", "", fmt.Errorf("controller has no hostname")
	}

	if rc.SecretCache == nil {
		return "", "", fmt.Errorf("secret cache not available, cannot resolve credentials for %s", hostname)
	}

	secret, ok := rc.SecretCache.GetByLabel(
		"babylon.gpte.redhat.com/ansible-control-plane", hostname)
	if !ok {
		return "", "", fmt.Errorf("no k8s secret found for controller %s", hostname)
	}

	username := string(secret.Data["user"])
	password := string(secret.Data["password"])
	if username == "" || password == "" {
		return "", "", fmt.Errorf("k8s secret for %s missing user or password", hostname)
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
func buildJobExtraVars(rc *runner.RunContext, action string, dynamicJobVars map[string]interface{}) map[string]interface{} {
	extraVars := make(map[string]interface{})

	// Subject first (lowest priority).
	if sjv := rc.JobVars(); sjv != nil {
		types.DeepMergeMap(extraVars, sjv)
	}
	// Governor overrides subject.
	if gjv := rc.GovernorJobVars(); gjv != nil {
		types.DeepMergeMap(extraVars, gjv)
	}
	// Dynamic vars (sandbox creds) override both.
	if dynamicJobVars != nil {
		types.DeepMergeMap(extraVars, dynamicJobVars)
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
	if meta != nil && meta.Deployer != nil {
		if actionCfg, ok := meta.Deployer.Actions[action]; ok {
			// DeployerActionConfig doesn't have extra_vars; use the raw governor
			// data for action extra_vars via the generic Actions map.
			// The Actions map in GovernorSpec is map[string]map[string]interface{},
			// but DeployerMeta.Actions is typed. We need to check for extra_vars
			// in the raw deployer data.
			// Since DeployerActionConfig doesn't have extra_vars, we won't merge
			// them here -- fall through to the default.
			_ = actionCfg
		}
	}
	// Check raw deployer map for action extra_vars.
	govAllVars := rc.GovernorAllVars()
	rawMeta := types.GetNestedMap(govAllVars, "job_vars", "__meta__")
	actionExtraVars := types.GetNestedMap(rawMeta, "deployer", "actions", action, "extra_vars")
	if actionExtraVars != nil {
		types.DeepMergeMap(extraVars, actionExtraVars)
	} else {
		extraVars["ACTION"] = action
	}

	// Callback vars with configurable var names.
	callbackURLVar := "agnosticd_callback_url"
	callbackTokenVar := "agnosticd_callback_token"
	rawDeployer := types.GetNestedMap(rawMeta, "deployer")
	if rawDeployer != nil {
		if v, ok := rawDeployer["callback_url_var"].(string); ok && v != "" {
			callbackURLVar = v
		}
		if v, ok := rawDeployer["callback_token_var"].(string); ok && v != "" {
			callbackTokenVar = v
		}
	}
	if rc.Payload.Action != nil {
		if rc.Payload.Action.Spec.CallbackUrl != "" {
			extraVars[callbackURLVar] = rc.Payload.Action.Spec.CallbackUrl
		}
		if rc.Payload.Action.Spec.CallbackToken != "" {
			extraVars[callbackTokenVar] = rc.Payload.Action.Spec.CallbackToken
		}
	}

	// output_dir if deployer_type is "agnosticd" (default).
	deployerType := "agnosticd"
	if meta != nil && meta.Deployer != nil && meta.Deployer.Type != "" {
		deployerType = meta.Deployer.Type
	}
	if deployerType == "agnosticd" {
		uuid := rc.UUID()
		if uuid != "" {
			extraVars["output_dir"] = "/tmp/output-" + uuid
		}
	}

	// scm_ref_var injection.
	if rawDeployer != nil {
		if scmRefVar, ok := rawDeployer["scm_ref_var"].(string); ok && scmRefVar != "" {
			if meta != nil && meta.Deployer != nil && meta.Deployer.SCMRef != "" {
				extraVars[scmRefVar] = meta.Deployer.SCMRef
			}
		}
	}

	return extraVars
}

// getTowerClientForHost creates a TowerClient for a specific controller
// hostname by finding the matching entry in __meta__.ansible_controllers.
// This is used by checkDeployerJob and cancelTowerJob to connect to the
// same controller where the job was originally launched.
func getTowerClientForHost(rc *runner.RunContext, hostname string) (*clients.TowerClient, error) {
	if rc.TowerBaseURL != "" {
		return clients.NewTowerClient(rc.TowerBaseURL, "", "", rc.TowerTLSConfig), nil
	}

	meta := rc.Meta()
	if meta == nil {
		return nil, fmt.Errorf("no __meta__ in governor")
	}

	controllers := meta.AnsibleControllers
	if len(controllers) == 0 {
		return nil, fmt.Errorf("no ansible_controllers in __meta__")
	}

	for _, m := range controllers {
		h, _ := m["hostname"].(string)
		if h == hostname {
			username, password, err := resolveControllerCreds(rc, m)
			if err != nil {
				return nil, fmt.Errorf("credentials for %s: %w", hostname, err)
			}
			return rc.TowerClientPool.Get(hostname, username, password, rc.TowerTLSConfig), nil
		}
	}

	return nil, fmt.Errorf("controller %q not found in ansible_controllers", hostname)
}

// launchTowerJob selects a controller, builds the job config, launches
// the Tower job, and updates the subject status. If newState is non-empty,
// it also transitions labels and current_state. extraSpecVars are merged
// into the subject spec.vars patch (e.g. check_status_state for status).
// dynamicJobVars are sandbox vars with credentials, merged into Tower extra_vars.
func launchTowerJob(rc *runner.RunContext, action, newState string, extraSpecVars, dynamicJobVars map[string]interface{}) error {
	meta := rc.Meta()

	playbook := getDeployerEntryPoint(meta, action)
	scmURL := ""
	scmRef := ""
	if meta != nil && meta.Deployer != nil {
		scmURL = meta.Deployer.SCMUrl
		scmRef = meta.Deployer.SCMRef
	}

	tc, hostname, err := getTowerClientForAction(rc)
	if err != nil {
		return fmt.Errorf("get tower client: %w", err)
	}

	// Organization from __meta__.tower.organization (default "babylon").
	org := "babylon"
	if meta != nil && meta.Tower != nil && meta.Tower.Organization != "" {
		org = meta.Tower.Organization
	}
	// Timeout from __meta__.tower.timeout (default 10800).
	timeout := 10800
	if meta != nil && meta.Tower != nil && meta.Tower.Timeout > 0 {
		timeout = meta.Tower.Timeout
	}
	// Inventory name: "{org} {tower.inventory|default('default')}".
	inventorySuffix := "default"
	if meta != nil && meta.Tower != nil && meta.Tower.Inventory != "" {
		inventorySuffix = meta.Tower.Inventory
	}
	inventoryName := org + " " + inventorySuffix
	// Template name: "{org} {anarchy_action_name} {uuid}".
	// anarchy_action_name is the K8s AnarchyAction resource name.
	actionResourceName := ""
	if rc.Payload.Action != nil {
		actionResourceName = rc.Payload.Action.Metadata.Name
	}
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
	// scm_update_cache_timeout: __meta__.deployer.scm_cache_timeout (default 30).
	scmUpdateCacheTimeout := 30
	if meta != nil && meta.Deployer != nil && meta.Deployer.SCMCacheTimeout > 0 {
		scmUpdateCacheTimeout = meta.Deployer.SCMCacheTimeout
	}
	// scm_clean: __meta__.deployer.scm_clean (default true).
	scmClean := true
	if meta != nil && meta.Deployer != nil && meta.Deployer.SCMClean != nil {
		scmClean = *meta.Deployer.SCMClean
	}

	// Execution environment (AAP2/Controller only).
	// ansible_control_plane type from raw meta (not typed).
	var eeConfig *clients.EEConfig
	govAllVars := rc.GovernorAllVars()
	rawMeta := types.GetNestedMap(govAllVars, "job_vars", "__meta__")
	controlPlaneType := "tower"
	acp := types.GetNestedMap(rawMeta, "ansible_control_plane")
	if acp != nil {
		if t, ok := acp["type"].(string); ok && t != "" {
			controlPlaneType = t
		}
	}
	rawDeployer := types.GetNestedMap(rawMeta, "deployer")
	if controlPlaneType == "controller" {
		ee := types.GetNestedMap(rawDeployer, "execution_environment")
		if ee != nil {
			eeImage, _ := ee["image"].(string)
			if eeImage != "" {
				eeName, _ := ee["name"].(string)
				if eeName == "" {
					eeName = org + " " + eeImage
				}
				eePull, _ := ee["pull"].(string)
				eePrivate, _ := ee["private"].(bool)
				eeConfig = &clients.EEConfig{
					Name:    eeName,
					Image:   eeImage,
					Pull:    eePull,
					Private: eePrivate,
				}
			}
		}
	}

	// Instance groups: per-action first, then global fallback.
	var instanceGroups []string
	actionACP := types.GetNestedMap(rawDeployer, "actions", action, "ansible_control_plane")
	if actionACP != nil {
		instanceGroups = types.ExtractStringSlice(actionACP, "instance_groups")
	}
	if len(instanceGroups) == 0 && acp != nil {
		instanceGroups = types.ExtractStringSlice(acp, "instance_groups")
	}

	// Static credentials from __meta__.deployer.credentials (default []).
	var credentials []string
	if rawDeployer != nil {
		credentials = types.ExtractStringSlice(rawDeployer, "credentials")
	}

	// Vault credentials from governor.spec.vars.vault_credentials (default {}).
	// Injected by the operator via varSecrets; always empty if not configured.
	var vaultCredentials map[string]string
	vcRaw, _ := rc.Payload.Governor.Spec.Vars.Get("vault_credentials").(map[string]interface{})
	if len(vcRaw) > 0 {
		vaultCredentials = make(map[string]string, len(vcRaw))
		for k, v := range vcRaw {
			if s, ok := v.(string); ok {
				vaultCredentials[k] = s
			}
		}
	}

	config := clients.TowerJobConfig{
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
		ExtraVars:             clients.InsertUnvaultString(buildJobExtraVars(rc, action, dynamicJobVars)),
		Timeout:               timeout,
		ExecutionEnvironment:  eeConfig,
		InstanceGroups:        instanceGroups,
		Credentials:           credentials,
		VaultCredentials:      vaultCredentials,
	}

	slog.Info("launching tower job", "action", action, "subject", rc.SubjectName(), "entryPoint", playbook)

	jobID, err := tc.LaunchJob(rc.Ctx, config)
	if err != nil {
		return fmt.Errorf("launch tower job: %w", err)
	}

	slog.Info("tower job launched", "jobID", jobID, "action", action, "subject", rc.SubjectName())

	towerJobURL := fmt.Sprintf("%s/#/jobs/%d/", hostname, jobID)

	// Build the subject update patch.
	patchBody := types.PatchBody{
		Status: map[string]interface{}{
			"towerJobs": map[string]interface{}{
				action: map[string]interface{}{
					"deployerJob":      float64(jobID),
					"startTimestamp":    types.NowUTC(),
					"completeTimestamp": nil,
					"towerHost":        hostname,
					"towerJobURL":      towerJobURL,
				},
			},
		},
	}

	if newState != "" {
		patchBody.Metadata = &types.PatchMetadata{
			Labels: map[string]string{"state": newState},
		}
		patchBody.Spec = &types.PatchSpec{
			Vars: map[string]interface{}{
				"current_state": newState,
			},
		}
	}

	// Merge extra spec vars (e.g. check_status_state for status action).
	if len(extraSpecVars) > 0 {
		if patchBody.Spec == nil {
			patchBody.Spec = &types.PatchSpec{Vars: make(map[string]interface{})}
		}
		for k, v := range extraSpecVars {
			patchBody.Spec.Vars[k] = v
		}
	}

	return rc.SubjectUpdate(types.SubjectPatch{Patch: patchBody})
}

// cancelTowerJob cancels a running Tower job for the given action.
// Errors are logged but not returned to avoid failing the caller.
func cancelTowerJob(rc *runner.RunContext, action string) {
	towerJobs := rc.StatusTowerJobs()
	jobInfo := types.GetNestedMap(towerJobs, action)
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

	token, err := tc.GetToken(rc.Ctx)
	if err != nil {
		slog.Error("cancelTowerJob: cannot get oauth token", "error", err)
		return
	}

	if err := tc.CancelJob(rc.Ctx, token, int(jobIDFloat)); err != nil {
		slog.Error("cancelTowerJob: failed to cancel job", "job", int(jobIDFloat), "error", err)
	} else {
		slog.Info("cancelTowerJob: canceled job", "job", int(jobIDFloat), "action", action)
	}
}

// cancelAllIncompleteTowerJobs cancels every tower job in
// subject.status.towerJobs that has no completeTimestamp.
func cancelAllIncompleteTowerJobs(rc *runner.RunContext) {
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

