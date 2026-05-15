# Babylon Runner: Governor Rewrite from Ansible to Go

## Summary

Replace the Ansible-based AnarchyRun execution for the babylon governor with a Go binary (`babylon-runner`). The anarchy operator (Python/Kopf) remains unchanged. The state machine remains unchanged. The Go runner implements the babylon governor logic natively, line-by-line equivalent to the Ansible tasks.

## Toggle Mechanism

A field `spec.runner` on the AnarchyGovernor CR controls which runner executes runs:
- `default` (or unset) — current Ansible runner (no behavior change)
- `babylon-go` — new Go runner

The operator routes runs to the appropriate runner pool based on this field. Both runners can coexist on the same cluster for side-by-side validation.

## Architecture

```
AnarchyGovernor CR (spec.runner: babylon-go)
        │
        ▼
Anarchy Operator (unchanged, Python/Kopf)
        │
        ▼ GET /run (polling)
babylon-runner (Go binary, new)
        │
        ├──► Anarchy API (subject updates, action scheduling)
        ├──► Sandbox API (booking, lifecycle)
        └──► AAP2/Tower (deployer jobs)
```

- The Go runner lives in `anarchy/babylon-runner/`.
- It replaces the Python Ansible runner for babylon governor workloads only.
- It uses the same polling protocol: authenticates with bearer token `{runner_name}:{pod_name}:{token}`, polls `GET /run`, executes, posts results to `POST /run/{name}`.
- It is babylon-specific, not a generic Ansible replacement.

## Components

### `main` / `cmd`
Entry point. Runner name, pod name, token from env vars. Starts polling loop. Graceful shutdown on SIGTERM.

### `runner` — Polling & Dispatch
- Polls `GET /run` with bearer token
- Dispatches based on run payload:
  - `anarchy_action_config_name` set → action handler (provision, destroy, start, stop, status)
  - `anarchy_event_name` set → event handler (create, update, delete)
  - Otherwise → no-action-or-event handler
- Posts results back via `POST /run/{name}`

### `anarchy` — Anarchy API Client
Wraps the REST calls behind the Ansible modules:
- `SubjectUpdate(vars, varSecrets, labels, annotations)`
- `ScheduleAction(actionName, after, vars)`
- `FinishAction(successful bool)`
- `ContinueAction()`
- `SubjectDelete()`

### `sandboxapi` — Sandbox API Client
HTTP client for sandbox API:
- `Login() → token`
- `BookPlacement(request) → placement`
- `GetPlacement(uuid) → placement`
- `ReleasePlacement(uuid)`
- `StartPlacement(uuid)`
- `StopPlacement(uuid)`

### `tower` — AAP2/Tower Client
REST client for Ansible Automation Platform:
- `SelectController(controllers, mode)` — random/balance/first-available
- `LaunchJob(template, extraVars) → jobID`
- `GetJobStatus(jobID) → status, artifacts`
- `CancelJob(jobID)`
- `GetProvisionInfo(jobID) → provisionData, messages`
- OAuth token lifecycle (create, cleanup)

### `handlers` — Action & Event Handlers
One file per handler, mirroring Ansible task files 1:1:
- `event_create.go` — handle-event-create.yaml
- `event_update.go` — handle-event-update.yaml
- `event_delete.go` — handle-event-delete.yaml
- `action_provision.go` — handle-action-provision.yaml + run-provision.yaml + provision-complete.yaml
- `action_destroy.go` — handle-action-destroy.yaml + run-destroy.yaml
- `action_start.go` — run-start.yaml + start-complete.yaml
- `action_stop.go` — run-stop.yaml + stop-complete.yaml
- `action_status.go` — run-status.yaml + status-complete.yaml
- `check_deployer_job.go` — check-deployer-job.yaml
- `sandbox_book.go` — sandbox_api_book.yaml
- `sandbox_get.go` — sandbox_get.yaml
- `sandbox_cleanup.go` — sandbox_cleanup.yml

## State Machine

Preserved exactly as-is. No state or transition changes.

```
provision-pending → provisioning → started
                 ↘ provision-queued → provisioning (sandbox queue)

started ↔ stopped (via start/stop actions)
started → stopping → stopped
stopped → starting → started

any state → destroying → destroyed (subject deleted)
         ↘ destroy-error → destroying (retry)
                        ↘ destroyed (catch-all cleanup)
```

### State Transitions by Handler

| Handler | Sets `current_state` to | Schedules next action |
|---------|------------------------|----------------------|
| event_create | `provision-pending` | `provision` (immediate) |
| action_provision (pending) | `provisioning` | `provision` (check interval) |
| action_provision (queued) | `provisioning` when ready | `provision` (recheck) |
| provision-complete | `started` | none |
| action_stop | `stopping` → `stopped` | none / `stop` (check) |
| action_start | `starting` → `started` | none / `start` (check) |
| action_destroy | `destroying` | `destroy` (check interval) |
| destroy-complete | deletes subject | none |
| event_update | unchanged | `stop`, `start`, `update`, or `status` |
| event_delete | unchanged | cancels jobs, triggers destroy |

The `healthy` flag is set to `true` on successful provision, `false` on failures. The `job_vars` comparison in event_update uses the same diff logic (previous vs current) to decide what action to take.

## Data Flow

```
Anarchy Operator                    babylon-runner                     External Services
      │                                  │                                    │
      │  GET /run (poll)                 │                                    │
      │◄─────────────────────────────────│                                    │
      │                                  │                                    │
      │  Run payload (subject vars,      │                                    │
      │  governor vars, action/event)    │                                    │
      │─────────────────────────────────►│                                    │
      │                                  │                                    │
      │                                  │── Login ──────────────────────────►│ Sandbox API
      │                                  │◄─ Token ──────────────────────────│
      │                                  │── Book/Get Placement ────────────►│
      │                                  │◄─ Sandbox credentials ───────────│
      │                                  │                                    │
      │                                  │── Select controller ────────────►│ AAP2/Tower
      │                                  │── Launch job (extra_vars) ──────►│
      │                                  │◄─ Job ID ────────────────────────│
      │                                  │                                    │
      │  anarchy_subject_update          │                                    │
      │◄─────────────────────────────────│  (current_state, job ID, etc.)    │
      │                                  │                                    │
      │  anarchy_schedule_action         │                                    │
      │◄─────────────────────────────────│  (check in 5m)                    │
      │                                  │                                    │
      │  anarchy_finish_action           │                                    │
      │◄─────────────────────────────────│  (successful: true/false)         │
      │                                  │                                    │
      │  POST /run/{name} (results)      │                                    │
      │◄─────────────────────────────────│                                    │
```

**Variable passing:** The run payload from the operator contains all vars the Ansible runner receives — `anarchy_subject`, `anarchy_governor`, `anarchy_action`, `job_vars`, `__meta__`, etc. The Go runner unpacks these into typed structs.

**Credentials flow:** Sandbox API credentials from governor vars (`sandbox_api_user`, `sandbox_api_password`). Tower credentials from governor vars (`ansible_controller_*`). Read from the run payload.

**Result posting:** Posts back to `POST /run/{name}` with same result format — success/failure status and output.

## Error Handling

Replicates the Ansible governor's behavior exactly.

### Action retries
Exponential backoff: `[1m, 5m, 10m, 30m, 1h, 2h, 4h, 8h, 16h, 1d]`. On failure, schedules next retry via `anarchy_schedule_action`. After exhausting retries, marks action failed, sets `healthy: false`.

### Tower job failures
- `failed` or `canceled` → set `current_state` to `{action}-failed`, `healthy: false`, store error in `status.towerJobs`
- API unreachable → retry with backoff, don't change state
- Still running → reschedule check in `job_check_interval` (5m)

### Sandbox API failures
- Booking queued → `provision-queued`, reschedule check
- Booking error → retry with backoff
- HTTP errors → retry with backoff, log
- Placement not ready → reschedule check

### Destroy error handling
- `sandbox_api_destroy_catch_all` true + destroy fails → `sandbox_cleanup` + delete subject
- False → `destroy-error`, retry with backoff

### Runner-level errors
- Poll fails → log, sleep, retry
- Panic → `defer recover()` in handler, log stack trace, post failure
- Post-result fails → log, continue polling (operator times out and reschedules)

## Testing

### Unit tests
Per package, with mock HTTP clients:
- `handlers/` — verify correct API calls, state transitions, retry scheduling per handler
- `anarchy/` — request construction, response parsing
- `sandboxapi/` — login, booking, placement lifecycle
- `tower/` — controller selection, job launch, status polling, OAuth lifecycle

### Integration tests
- Mock anarchy operator API returning canned run payloads
- Full polling loop through provision → start → stop → destroy lifecycle
- Verify all API calls in correct order with correct payloads

### Validation against Ansible
- Feed same input vars the Ansible version receives, assert same API calls in same order
- Use real run payloads captured from working environment as test fixtures

### E2E validation
No dedicated e2e tests in initial rewrite. The toggle mechanism (`spec.runner: babylon-go` vs `default`) enables side-by-side comparison on a live cluster.

## Default Variables

Carried over from `babylon_anarchy_governor/defaults/main.yaml`:
- `sandbox_api_url`: `http://sandbox-api.babylon-sandbox-api.svc.cluster.local:8080`
- `sandbox_api_in_use`: derived from `__meta__.aws_sandboxed` or `__meta__.sandboxes`
- `deployer_entry_points`: provision→`ansible/main.yml`, destroy→`ansible/destroy.yml`, start/stop/status→`ansible/lifecycle_entry_point.yml`
- `action_retry_intervals`: `[1m, 5m, 10m, 30m, 1h, 2h, 4h, 8h, 16h, 1d]`
- `job_check_interval`: `5m`
- `ansible_controller_select_mode`: `random`
- `sandbox_api_destroy_catch_all`: from governor vars

## File Structure

```
anarchy/babylon-runner/
├── cmd/
│   └── main.go
├── internal/
│   ├── runner/
│   │   └── runner.go          # Polling loop + dispatch
│   ├── handlers/
│   │   ├── event_create.go
│   │   ├── event_update.go
│   │   ├── event_delete.go
│   │   ├── action_provision.go
│   │   ├── action_destroy.go
│   │   ├── action_start.go
│   │   ├── action_stop.go
│   │   ├── action_status.go
│   │   ├── check_deployer_job.go
│   │   ├── sandbox_book.go
│   │   ├── sandbox_get.go
│   │   └── sandbox_cleanup.go
│   ├── anarchy/
│   │   └── client.go          # Anarchy API client
│   ├── sandboxapi/
│   │   └── client.go          # Sandbox API client
│   └── tower/
│       └── client.go          # AAP2/Tower client
├── go.mod
├── go.sum
├── Dockerfile
└── README.md
```
