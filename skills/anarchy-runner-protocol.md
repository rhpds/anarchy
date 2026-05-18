# Anarchy Runner Protocol Reference

Complete protocol specification for reimplementing the anarchy runner in Go.

## Authentication

Bearer token format:
```
Authorization: Bearer {runner_name}:{pod_name}:{runner_token}
```

Validated by regex in `api/anarchyrunnerpod.py:26`:
```python
match = re.match(r'Bearer ([^: ]+):([^: ]+):(.*)', auth_header)
```

## Environment Variables

**Required (no defaults):**
- `ANARCHY_URL` — Base URL of Anarchy API (e.g., `http://anarchy-api:5000`)
- `HOSTNAME` — Pod name (used as `pod_name` in auth header)
- `RUNNER_NAME` — Runner identifier
- `RUNNER_TOKEN` — Authentication token

**Optional (with defaults):**
- `ANARCHY_DOMAIN` — Default: `anarchy.gpte.redhat.com`
- `POLLING_INTERVAL` — Default: `5` seconds
- `REQUEST_TIMEOUT` — Default: `35` seconds
- `LOG_LEVEL` — Default: `INFO`

## GET /run

**Request:**
```
GET {ANARCHY_URL}/run
Authorization: Bearer {runner_name}:{pod_name}:{runner_token}
Timeout: REQUEST_TIMEOUT (35s)
```

**Response codes:**
- `200 OK` — Run available, JSON body
- `403 FORBIDDEN` — Auth failed or pod deleting/terminating
- Timeout/connection error — No run available

**Response payload (200):**

For `subjectEvent` handler type (no `action` key):
```json
{
  "handler": {
    "type": "subjectEvent",
    "name": "create|update|delete",
    "vars": {}
  },
  "governor": { /* full AnarchyGovernor K8s object */ },
  "subject": { /* full AnarchySubject K8s object with vars snapshot */ },
  "run": { /* full AnarchyRun K8s object */ }
}
```

For `action` or `actionCallback` handler type (includes `action` key):
```json
{
  "handler": {
    "type": "action|actionCallback",
    "name": "callback_name (for actionCallback)",
    "vars": {}
  },
  "governor": { /* AnarchyGovernor */ },
  "subject": { /* AnarchySubject */ },
  "action": { /* AnarchyAction with spec.action, spec.callbackToken, spec.callbackUrl */ },
  "run": { /* AnarchyRun */ }
}
```

**Handler type determination** (`api/anarchyrun.py:200-227`):
- If `handler.type` is set, use it directly
- Legacy: if `anarchy_event_name` in spec_vars → `subjectEvent`
- Legacy: if `anarchy_action_callback_name` in spec_vars → `actionCallback`
- Legacy: no handler → `action`

## POST /run/{name}

**Request:**
```
POST {ANARCHY_URL}/run/{anarchy_run_name}
Authorization: Bearer {runner_name}:{pod_name}:{runner_token}
Content-Type: application/json
Retries: 10 attempts with POLLING_INTERVAL sleep between
```

**Request body:**
```json
{
  "result": {
    "rc": 0,
    "status": "successful|failed",
    "statusMessage": "optional error message"
  }
}
```

**Response:** `{"success": true}`

**Operator processing (`api/app.py:160-197`):**
- `failed` → `set_runner_state_failed(result)` — stores result, increments failures, sets retryAfter
- `successful` → `set_runner_state_successful(result)` — stores result, marks successful

## PATCH /run/subject/{name} (anarchy_subject_update)

**Source:** `anarchy-runner/ansible-runner/project/action_plugins/anarchy_subject_update.py`

```
PATCH {ANARCHY_URL}/run/subject/{subject_name}
Authorization: Bearer {runner_name}:{pod_name}:{runner_token}
Content-Type: application/json
Retries: 3 attempts with 5, 10, 20 second delays
```

**Request body:**
```json
{
  "patch": {
    "metadata": {
      "annotations": {},
      "labels": {}
    },
    "spec": {
      "vars": {}
    },
    "status": {},
    "skip_update_processing": true
  }
}
```

All fields in `patch` are optional. `skip_update_processing` prevents the operator from triggering event handlers for this update.

**Response:** `{"success": true, "result": { /* updated AnarchySubject */ }}`

## POST /run/subject/{name}/actions (anarchy_schedule_action)

**Source:** `anarchy-runner/ansible-runner/project/action_plugins/anarchy_schedule_action.py`

```
POST {ANARCHY_URL}/run/subject/{subject_name}/actions
Authorization: Bearer {runner_name}:{pod_name}:{runner_token}
Content-Type: application/json
Retries: 3 attempts with 5, 10, 20 second delays
```

**Request body:**
```json
{
  "action": "provision|destroy|start|stop|status|update",
  "after": "2026-05-15T10:30:00Z or interval like '1h', '10m', '30s'",
  "cancel": ["action1", "action2"],
  "vars": {}
}
```

**Response:** `{"success": true, "result": { /* created AnarchyAction */ }}`

## anarchy_finish_action (file write, NOT HTTP)

**Source:** `action_plugins/anarchy_finish_action.py`

Writes to `{OUTPUT_DIR}/anarchy-result.yaml`:
```yaml
finishAction:
  state: successful|failed|error
```

The runner reads this file and includes it in the POST /run result. The operator processes it.

**For the Go runner:** Include `finishAction` in the POST /run result payload directly, or set it as a field on the result struct that gets serialized.

## anarchy_continue_action (file write, NOT HTTP)

**Source:** `action_plugins/anarchy_continue_action.py`

Writes to `{OUTPUT_DIR}/anarchy-result.yaml`:
```yaml
continueAction:
  after: "2026-05-15T10:30:00Z"
  vars: {}
```

**For the Go runner:** Include `continueAction` in the POST /run result payload.

## anarchy_subject_delete (file write, NOT HTTP)

**Source:** `action_plugins/anarchy_subject_delete.py`

Writes to `{OUTPUT_DIR}/anarchy-result.yaml`:
```yaml
deleteSubject:
  removeFinalizers: true|false
```

**For the Go runner:** Include `deleteSubject` in the POST /run result payload.

## Variable Assembly Order

From `anarchy-runner/anarchyrunner.py:248-289`, all_vars is built as:

```python
all_vars = {
    **anarchy_governor.vars,       # 1. Governor vars (includes resolved varSecrets)
    **run_config.vars,             # 2. Handler/action config vars
    **anarchy_subject.vars,        # 3. Subject vars (snapshot from run scheduling)
}
if anarchy_action:
    all_vars.update(anarchy_action.vars)  # 4. Action vars
if handler_vars:
    all_vars.update(handler_vars)         # 5. Handler-specific vars

# Then system vars are added:
all_vars.update(dict(
    anarchy_domain = ...,
    anarchy_governor = governor.export_for_inventory(),
    anarchy_governor_name = ...,
    anarchy_namespace = ...,
    anarchy_run = run.export_for_inventory(),
    anarchy_run_pod_name = ...,
    anarchy_run_timestamp = datetime.now(UTC).strftime('%FT%TZ'),
    anarchy_runner_name = ...,
    anarchy_runner_token = ...,
    anarchy_subject = subject.export_for_inventory(),
    anarchy_subject_name = ...,
    anarchy_url = ...,
))

# Action-specific vars:
if anarchy_action:
    all_vars.update(dict(
        anarchy_action = action.export_for_inventory(),
        anarchy_action_name = action.name,
        anarchy_action_callback_name_parameter = run_config.callback_name_parameter,
        anarchy_action_callback_token = action.callback_token,
        anarchy_action_callback_url = action.callback_url,
        anarchy_action_config_name = action.action,  # e.g. "provision", "destroy"
    ))

# Handler type vars:
if handler_type == 'actionCallback':
    all_vars['anarchy_action_callback_name'] = handler_name
elif handler_type == 'subjectEvent':
    all_vars['anarchy_event_name'] = handler_name  # e.g. "create", "update", "delete"
```

## Polling Loop

From `anarchy-runner/anarchyrunner.py`:

```
while True:
    run_data = GET /run (with timeout)
    if run_data:
        execute(run_data)  # runs ansible-playbook
        POST /run/{name} with result (10 retries)
    else:
        sleep(POLLING_INTERVAL)
```
