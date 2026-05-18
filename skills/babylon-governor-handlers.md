# Babylon Governor Handler Reference

Line-by-line logic for each handler in `babylon_anarchy_governor/tasks/`.
For Go reimplementation — shows exact sequence of operations, conditions, and API calls.

## Dispatch Logic (tasks/main.yaml)

```
if anarchy_action_config_name is defined:
    if anarchy_action_callback_name is defined:
        → handle-action-{config_name}-{callback_name}.yaml
    else:
        → handle-action-{config_name}.yaml
elif anarchy_event_name is defined:
    → handle-event-{event_name}.yaml
else:
    → handle-no-action-or-event.yaml (fails with error)
```

---

## EVENT HANDLERS

### handle-event-create.yaml

**Condition:** `current_state is undefined` (first time)

1. `anarchy_subject_update(skip_update_processing=true)`:
   - Labels: `state: provision-pending`
   - Spec vars:
     - `current_state: provision-pending`
     - `job_vars.cloud_provider`: from governor job_vars, default `none`
     - `job_vars.platform`: from governor job_vars, default `RHPDS`
     - `job_vars.uuid`: generated UUID4 if not present
2. `anarchy_schedule_action(action="provision")`

### handle-event-update.yaml

**Determines action from state comparison:**

```
_current_state = subject.spec.vars.current_state
_desired_state = subject.spec.vars.desired_state
_current_job_vars = subject.spec.vars.job_vars
_previous_job_vars = subject.status.previous_state.job_vars  (or subject.spec.vars.job_vars)

if _current_job_vars != _previous_job_vars:
    _action = 'update'
elif _desired_state == 'started' and _current_state == 'stopped':
    _action = 'start'
elif _desired_state == 'stopped' and _current_state == 'started':
    _action = 'stop'
else:
    _action = None
```

**If _action is valid AND exists in governor.spec.actions:**
1. `anarchy_subject_update(skip_update_processing=true)`:
   - Labels: `state: {_action}-pending`
   - Spec vars: `current_state: {_action}-pending`
2. `anarchy_schedule_action(action=_action, cancel=["start", "stop"])`

**Status check (separate condition):**
- If `check_status_state == 'pending'` OR `check_status_request_timestamp` changed
- AND `check_status_state` is empty or `'successful'`
1. `anarchy_subject_update(skip_update_processing=true)`:
   - Spec vars: `check_status_state: pending`
2. `anarchy_schedule_action(action="status")`

### handle-event-delete.yaml

1. **Cancel all incomplete tower jobs** (loop through `status.towerJobs`, cancel each that has no `completeTimestamp`)
2. **If NO provision deployer job exists OR destroy disabled OR destroy not in governor actions:**
   → `handle-event-delete-without-destroy.yaml`
3. **If provision deployer job exists AND destroy enabled AND destroy in governor actions:**
   → `handle-event-delete-with-destroy.yaml`

### handle-event-delete-with-destroy.yaml

1. `anarchy_schedule_action(action="destroy", cancel=["start", "stop", "update"])`
2. `anarchy_subject_update`:
   - Spec vars: `current_state: destroy-pending`, `desired_state: destroyed`

### handle-event-delete-without-destroy.yaml

1. **If** `sandbox_api_in_use AND sandbox_api_destroy_catch_all AND uuid exists`:
   → `sandbox_cleanup.yml`
2. `anarchy_subject_update`:
   - Spec vars: `current_state: destroy-complete`, `desired_state: destroyed`
3. `anarchy_subject_delete(remove_finalizers=true)`

---

## ACTION HANDLERS

### handle-action-provision.yaml (router)

```
if current_state == "provision-pending":
    → run-provision.yaml
elif current_state == "provision-queued":
    → check-provision-queue.yaml
elif current_state == "provisioning" AND deployer not disabled:
    job_action = "provision"
    job_info = status.towerJobs.provision
    → check-deployer-job.yaml
```

### run-provision.yaml

1. **If** `startTimestamp not in status.actions.provision`:
   - `anarchy_subject_update(skip_update_processing=true)`:
     - Status: `actions.provision.startTimestamp: {now_utc}`

2. **If** `sandbox_api_in_use`:
   → `sandbox_get.yaml`

3. **If deployer NOT disabled:**
   - Set vars:
     - `new_subject_state: provisioning`
     - `job_template_playbook: deployer_entry_points.provision` (default `ansible/main.yml`)
     - `job_extra_vars`: merge of subject_job_vars + governor_job_vars + dynamic_job_vars + deployer extra_vars + callback vars
   - → `check-run-tower-job.yaml` (launches Tower job)
   - `anarchy_continue_action(after=job_check_interval)` (default 5m)

4. **If sandbox_api_in_use AND deployer IS disabled:**
   - `anarchy_subject_update`:
     - Labels: `state: started`
     - Spec vars: `current_state: started`, `healthy: true`, `provision_data: action_provision_data`
     - Status: `actions.provision.completeTimestamp: {now}`, `actions.provision.status: successful`
   - `anarchy_finish_action(state="successful")`

### handle-action-provision-complete.yaml

1. `anarchy_subject_update`:
   - Labels: `state: started`
   - Spec vars:
     - `current_state: started`
     - `healthy: true`
     - `provision_data: action_provision_data`
     - `provision_message_body: action_provision_message_body`
     - `provision_messages: action_provision_messages`
   - Status:
     - `actions.provision.completeTimestamp: {now}`
     - `actions.provision.state: successful`
     - `towerJobs.provision.completeTimestamp: {now}`
     - `towerJobs.provision.jobStatus: successful`
2. `anarchy_finish_action(state="successful")`

### handle-action-provision-error.yaml

1. (Optional) `prevent_resource_claim_bind.yaml` if poolboy
2. `anarchy_subject_update(skip_update_processing=true)`:
   - Labels: `state: provision-error`
   - Spec vars: `current_state: provision-error`, `healthy: false`
   - Status: `actions.provision.completeTimestamp`, `state: error`, `towerJobs.provision.completeTimestamp`, `jobStatus: error`
3. `anarchy_finish_action(state="error")`

### handle-action-provision-failed.yaml

1. (Optional) `prevent_resource_claim_bind.yaml` if poolboy
2. `anarchy_subject_update(skip_update_processing=true)`:
   - Labels: `state: provision-failed`
   - Spec vars: `current_state: provision-failed`, `healthy: false`, `job_vars.agnosticd_collect_forensics: true`
   - Status: `actions.provision.completeTimestamp`, `state: failed`, `towerJobs.provision.completeTimestamp`, `jobStatus: failed`
3. `anarchy_finish_action(state="failed")`

### check-provision-queue.yaml

1. `sandbox_api_login.yaml`
2. GET `{sandbox_api_url}/api/v1/placements/{uuid}` — wait for status in [success, error, queued]
3. **If error or 404:**
   → `handle-action-provision-error.yaml` + end_play
4. **If queued:**
   - `anarchy_subject_update(skip_update_processing=true)`:
     - Status: `sandboxAPIJobs.provision.placementStatus`, `lastCheckTimestamp`
   - `anarchy_continue_action(after=sandbox_api_queue_check_interval)` (30s)
   - end_play
5. **If success:**
   - Extract sandbox vars into `dynamic_job_vars`
   - `anarchy_subject_update(skip_update_processing=true)`:
     - Labels: extracted sandbox labels + `state: provision-pending`
     - Spec vars: `current_state: provision-pending`, `job_vars: extracted vars`
     - Status: `sandboxAPIJobs.provision.placementStatus`, `dequeuedTimestamp`
   - → `run-provision.yaml`

---

### handle-action-destroy.yaml (router)

1. **If** `startTimestamp not in status.actions.destroy AND current_state == "destroy-pending"`:
   - `anarchy_subject_update(skip_update_processing=true)`:
     - Status: `actions.destroy.startTimestamp: {now}`

2. **If** `sandbox_api_in_use AND sandbox_api_destroy_catch_all AND (state in [destroy-error, destroy-failed, destroy-canceled] OR deployer disabled)`:
   - Debug message
   - → `sandbox_cleanup.yml`
   - `anarchy_subject_delete(remove_finalizers=true)`
   - **end_play** (return early)

3. **If** `current_state != "destroying" AND deployer not disabled`:
   → `run-destroy.yaml`

4. **If** `current_state == "destroying" AND deployer not disabled`:
   - `job_action = "destroy"`, `job_info = status.towerJobs.destroy`
   - → `check-deployer-job.yaml`

### run-destroy.yaml

1. **If** `sandbox_api_in_use`: → `sandbox_get.yaml`
2. Get controller access facts
3. **If provision Tower job exists AND not complete:** cancel it via `awx.awx.job_cancel`
4. Set vars:
   - `new_subject_state: destroying`
   - `job_template_playbook: deployer_entry_points.destroy` (default `ansible/destroy.yml`)
   - `job_extra_vars`: merged vars with destroy-specific extras
5. → `check-run-tower-job.yaml`
6. `anarchy_continue_action(after=job_check_interval)`

### handle-action-destroy-complete.yaml

1. **If** `sandbox_api_in_use`: → `sandbox_cleanup.yml`
2. `anarchy_subject_update(skip_update_processing=true)`:
   - Labels: `state: destroy-complete`
   - Spec vars: `current_state: destroy-complete`
   - Status: `actions.destroy.completeTimestamp`, `state: successful`, `towerJobs.destroy.completeTimestamp`, `jobStatus: successful`
3. `anarchy_subject_delete(remove_finalizers=true)`

---

### handle-action-start.yaml (router)

```
if current_state != "starting":
    → run-start.yaml
elif current_state == "starting" AND deployer not disabled:
    job_action = "start", job_info = status.towerJobs.start
    → check-deployer-job.yaml
```

### run-start.yaml

1. `anarchy_subject_update(skip_update_processing=true)`:
   - Status: `actions.start.startTimestamp: {now}`

2. **If** `sandbox_api_in_use AND __meta__.sandbox_api.actions.start.enable` (default true):
   - `sandbox_api_login.yaml`
   - `new_subject_state = 'started'` if deployer disabled, else `'starting'`
   - → `sandbox_api_start.yaml`
   - If deployer disabled: `anarchy_subject_update` with `actions.start.completeTimestamp`

3. **If deployer NOT disabled:**
   - If sandbox_api_in_use: → `sandbox_get.yaml`
   - Set vars: `new_subject_state: starting`, `job_template_playbook: deployer_entry_points.start` (default `ansible/lifecycle_entry_point.yml`)
   - → `check-run-tower-job.yaml`
   - `anarchy_continue_action(after=job_check_interval)`

### handle-action-start-complete.yaml

1. `anarchy_subject_update`:
   - Labels: `state: started`
   - Spec vars: `current_state: started`
   - Status: `actions.start.completeTimestamp`, `state: successful`, `towerJobs.start.completeTimestamp`, `jobStatus: successful`
2. `anarchy_finish_action(state="successful")`

---

### handle-action-stop.yaml (router)

```
if current_state != "stopping":
    → run-stop.yaml
elif current_state == "stopping" AND deployer not disabled:
    job_action = "stop", job_info = status.towerJobs.stop
    → check-deployer-job.yaml
```

### run-stop.yaml

1. `anarchy_subject_update(skip_update_processing=true)`:
   - Status: `actions.stop.startTimestamp: {now}`

2. **If deployer NOT disabled:**
   - If sandbox_api_in_use: → `sandbox_get.yaml`
   - Set vars: `new_subject_state: stopping`, `job_template_playbook: deployer_entry_points.stop`
   - → `check-run-tower-job.yaml`
   - `anarchy_continue_action(after=job_check_interval)`

3. **If** `sandbox_api_in_use AND __meta__.sandbox_api.actions.stop.enable AND deployer disabled`:
   - `sandbox_api_login.yaml`
   - → `sandbox_api_stop.yaml`

### handle-action-stop-complete.yaml

1. `anarchy_subject_update`:
   - Status: `actions.stop.completeTimestamp`, `state: successful`, `towerJobs.stop.completeTimestamp`, `jobStatus: successful`
2. **If** `sandbox_api_in_use AND __meta__.sandbox_api.actions.stop.enable`:
   - `sandbox_api_login.yaml` + `sandbox_api_stop.yaml`
3. `anarchy_subject_update`:
   - Labels: `state: stopped`
   - Spec vars: `current_state: stopped`
4. `anarchy_finish_action(state="successful")`

---

### handle-action-status.yaml (router)

```
if check_status_state == "pending":
    → run-status.yaml
elif check_status_state == "running" AND deployer not disabled:
    job_action = "status", job_info = status.towerJobs.status
    → check-deployer-job.yaml
```

### run-status.yaml

1. `anarchy_subject_update(skip_update_processing=true)`:
   - Status: `actions.status.startTimestamp: {now}`
2. **If deployer NOT disabled:**
   - If sandbox_api_in_use: → `sandbox_get.yaml`
   - Set vars: `new_check_status_state: running`, `job_template_playbook: deployer_entry_points.status`
   - → `check-run-tower-job.yaml`
   - `anarchy_continue_action(after=job_check_interval)`

---

### handle-action-update.yaml (router)

```
if current_state != "updating":
    → run-update.yaml
elif current_state == "updating":
    job_action = "update", job_info = status.towerJobs.update
    → check-deployer-job.yaml
```

### run-update.yaml

1. **If** `sandbox_api_in_use`: → `sandbox_get.yaml`
2. Set vars: `new_subject_state: updating`, `job_template_playbook: deployer_entry_points.update`
3. → `check-run-tower-job.yaml`
4. `anarchy_continue_action(after=job_check_interval)`

---

## TOWER JOB MANAGEMENT

### check-run-tower-job.yaml

1. Loop through `ansible_controllers`, check each via `check-ansible-controller.yaml`
2. Select controller based on `ansible_controller_select_mode`:
   - `balance` — least active jobs
   - `first-available` — first in list
   - `random` — random selection (default)
3. **If controller found:** → `run-tower-job.yaml`
4. **If no controller:** → `alert-no-controller-available.yaml`
5. **Always:** cleanup OAuth tokens for all available controllers

### run-tower-job.yaml

Creates Tower resources and launches job:
1. Create AWX Organization (`organization_name`, default `babylon`)
2. Create AWX Inventory (`inventory_name`)
3. Create AWX Credentials (loop vault_credentials)
4. Set project SCM ref from `__meta__.deployer.scm_ref` or git tag lookup
5. Create AWX Project (git SCM, with `scm_update_on_launch`)
6. Create Execution Environment (if `execution_environment.image` defined)
7. Create AWX Job Template (with rescue/retry on failure — updates project and retries)
8. Launch Job (with rescue/retry)
9. `anarchy_subject_update`:
   - Labels: `state: {new_subject_state}`
   - Spec vars: `current_state: {new_subject_state}`, `check_status_state: {new_check_status_state}` if defined
   - Status: `towerJobs.{action}`: `deployerJob`, `startTimestamp`, `completeTimestamp: null`, `towerHost`, `towerJobURL`

### check-deployer-job.yaml

1. Get controller access facts
2. GET `https://{controller.hostname}/api/v2/jobs/{job_info.deployerJob}/` with OAuth token
3. **If job status in [canceled, error, failed]:**
   → `handle-action-{action}-{status}.yaml`
4. **If job status == successful:**
   - `get_deployer_job_provision_info` — extracts `provision_data`, `provision_message_body`, `provision_messages`
   - → `handle-action-{action}-complete.yaml` with extracted data
5. **Always:** cleanup OAuth token
6. **If job still running:**
   - `anarchy_continue_action(after=job_check_interval)`

---

## SANDBOX API OPERATIONS

### sandbox_api_login.yaml

1. POST `{sandbox_api_url}/api/v1/login` with `Authorization: Bearer {sandbox_api_login_token}`
   - Retries: 40, Delay: 5s
2. Set `access_token` from response

### sandbox_get.yaml

1. → `sandbox_api_login.yaml`
2. → `sandbox_api_get.yaml`
3. **If** action is `provision` AND placement not found AND token exists:
   → `sandbox_api_book.yaml`

### sandbox_api_get.yaml

1. GET `{sandbox_api_url}/api/v1/placements/{uuid}` with access_token
   - Status codes: [200, 404], retries/delay
2. **If placement status == error:** → `handle-action-{action}-error.yaml` + end
3. **If placement found (200 with resources):**
   - Extract sandbox vars into `dynamic_job_vars`
   - `anarchy_subject_update(skip_update_processing=true)`:
     - Labels: extracted sandbox labels
     - Spec vars: `job_vars: extracted vars (no creds)`

### sandbox_api_book.yaml

1. Validate request via `validate_sandboxes_request` filter
2. Get OCP console URL from ConfigMap
3. POST `{sandbox_api_url}/api/v1/placements`:
   - Body: `service_uuid`, `reservation`, annotations (guid, env_type, owner, email), `resources`
   - Status codes: [200, 202, 400, 401, 404, 409, 507]
   - Retries: 40, Delay: 5s
4. **If response not 200/202:** → error handler + end
5. **If placement queued:**
   - `anarchy_subject_update`: Labels `state: provision-queued`, Spec vars `current_state: provision-queued`
   - `anarchy_continue_action(after=sandbox_api_queue_check_interval)` (30s)
   - end_play
6. GET placement, wait for success/error/queued
7. **If still queued:** update + continue + end
8. **If error:** → error handler + end
9. **If success:** extract sandbox vars, update subject labels and job_vars

### sandbox_cleanup.yml

1. Assert mandatory vars (governor, subject, guid, uuid)
2. **If** `sandbox_api_login_token` not empty:
   - → `sandbox_api_login.yaml`
   - → `sandbox_api_release.yaml` (DELETE placement)
3. **If placement 404:** → `aws_sandbox_cleanup_legacy.yaml` (fallback)

### sandbox_api_start.yaml

1. PUT `{sandbox_api_url}/api/v1/placements/{uuid}/start` (retries/delay)
2. `anarchy_subject_update`: Labels `state: starting`, Status: requestID, message, timestamps
3. GET `{sandbox_api_url}/api/v1/requests/{request_id}/status` — wait for success/error
4. **If success:** update state to `{new_subject_state}`, finish action if deployer disabled
5. **If error:** update state to `start-error`, finish action with error
6. **Rescue:** set `start-error`, `healthy: false`

### sandbox_api_stop.yaml

1. PUT `{sandbox_api_url}/api/v1/placements/{uuid}/stop` (retries/delay)
2. `anarchy_subject_update`: Labels `state: stopping`, Status: requestID, timestamps
3. GET `{sandbox_api_url}/api/v1/requests/{request_id}/status` — wait for success/error
4. **If success:** update state to `{new_subject_state}`, finish action if deployer disabled
5. **If error:** update state to `stop-error`, finish action with error
6. **Rescue:** set `stop-error`, `healthy: false`

---

## KEY DEFAULTS (defaults/main.yaml)

```yaml
sandbox_api_url: http://sandbox-api.babylon-sandbox-api.svc.cluster.local:8080
sandbox_api_retries: 40
sandbox_api_delay: 5
sandbox_api_queue_check_interval: 30s
job_check_interval: 5m
action_retry_intervals: [1m, 5m, 10m, 30m, 1h, 2h, 4h, 8h, 16h, 1d]
ansible_controller_select_mode: random  # or balance, first-available
organization_name: babylon
job_template_timeout: 10800  # 3 hours
maximum_job_count: 200
callback_url_var: agnosticd_callback_url
callback_token_var: agnosticd_callback_token
deployer_type: agnosticd
delete_on_failure: true
sandbox_api_destroy_catch_all: true
preserve_job_vars: [aws_region, region]

deployer_entry_points:
  provision: ansible/main.yml
  destroy: ansible/destroy.yml
  start: ansible/lifecycle_entry_point.yml
  stop: ansible/lifecycle_entry_point.yml
  status: ansible/lifecycle_entry_point.yml
  update: ansible/lifecycle_entry_point.yml
```

## KEY VARIABLE DERIVATIONS

```yaml
sandbox_api_in_use: __meta__.aws_sandboxed OR len(__meta__.sandboxes) > 0
guid: subject.spec.vars.job_vars.guid
uuid: subject.spec.vars.job_vars.uuid
env_type: governor.job_vars.env_type OR subject.job_vars.env_type
current_state: subject.spec.vars.current_state (default 'unknown')
desired_state: subject.spec.vars.desired_state (default 'unknown')
ansible_controllers: __meta__.ansible_controllers OR [babylon_tower]
deployer_entry_points.{action}: disabled if set to 'disabled' or 'none'
```
