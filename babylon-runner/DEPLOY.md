# babylon-runner: Deploy & Test Guide

## How It Works

The Go `babylon-runner` is a drop-in replacement for the Python/Ansible runner.
It uses the exact same protocol: polls `GET /run`, executes, posts results to `POST /run/{name}`.
The anarchy operator doesn't care what language the runner is written in — it just needs a container that speaks the protocol.

The runner image is controlled by the `AnarchyRunner` CR's pod template.
If no image is specified in the pod template, the operator falls back to `RUNNER_IMAGE` env var, then to the operator's own image.

## Project Structure

```text
babylon-runner/
  cmd/babylon-runner/    # Entry point (main.go)
  internal/
    types/               # Shared data types (RunPayload, RunResult, etc.)
    runner/              # Core runner loop, config, dispatch, RunContext
    handler/             # Handler implementations (provision, destroy, start, stop, etc.)
    clients/             # AnarchyClient, DeployerClient HTTP clients
    httputil/            # Shared HTTP helpers and token cache
    template/            # Template rendering utilities
  Makefile               # build, test, lint, clean, docker-build targets
  Dockerfile             # Multi-stage build (builder + ubi9-micro)
```

## Enable the Go Runner

### Option A: Separate runner pool (recommended for testing)

Add a second runner pool in your Helm values alongside the existing `default` pool:

```yaml
# helm values override
runners:
  default:
    # ... existing config unchanged ...
  babylon-go:
    consecutiveFailureLimit: 10
    maxReplicas: 2
    minReplicas: 1
    runLimit: 100
    scaleUpDelay: 5m
    scaleUpThreshold: 20
    scalingCheckInterval: 1m
    resources:
      limits:
        cpu: "1"
        memory: 256Mi
      requests:
        cpu: 500m
        memory: 256Mi
```

Then set `RUNNER_IMAGE` on the operator to point to the Go runner image, **or** patch the AnarchyRunner CR directly after deploy:

```bash
oc patch anarchyrunner babylon-go -n anarchy --type merge -p '
spec:
  podTemplate:
    spec:
      containers:
      - name: runner
        image: quay.io/rhpds/babylon-runner:latest
'
```

Both pools will run side-by-side. Runs are dispatched FIFO to any available pod from any pool.

> **Future**: The design spec calls for a `spec.runner` field on `AnarchyGovernor` to route
> runs to a specific pool. Until that's implemented, both pools share the same queue.

### Option B: Replace the default runner (full switchover)

Override the runner image globally:

```bash
# Via Helm values
helm upgrade anarchy ./helm \
  --set image.repository=quay.io/rhpds/babylon-runner

# Or via env var on the operator deployment
oc set env deployment/anarchy -n anarchy RUNNER_IMAGE=quay.io/rhpds/babylon-runner:latest
```

The operator will spawn all runner pods with the Go image instead of the Python one.

### Option C: Per-namespace override

If deploying per-namespace (multi-tenant), set the image in the namespace runner config:

```yaml
namespace:
  name: anarchy-myenv
  runners:
    default:
      # standard runner config, image comes from RUNNER_IMAGE or operator image
    babylon-go:
      resources:
        limits:
          cpu: "1"
          memory: 256Mi
        requests:
          cpu: 500m
          memory: 256Mi
```

Then patch the runner CR in that namespace as shown in Option A.

## Disable the Go Runner

### If using Option A (separate pool)

Delete the extra runner pool:

```bash
oc delete anarchyrunner babylon-go -n anarchy
```

Or remove it from Helm values and re-deploy. All runs go back to the `default` (Python) pool.

### If using Option B (full switchover)

Revert the image:

```bash
# Remove the env var override — operator falls back to its own image
oc set env deployment/anarchy -n anarchy RUNNER_IMAGE-

# Or re-deploy with default values
helm upgrade anarchy ./helm
```

## Build the Image

```bash
cd anarchy/babylon-runner

# Local build
podman build -t babylon-runner:dev .

# Push to registry
podman tag babylon-runner:dev quay.io/rhpds/babylon-runner:dev
podman push quay.io/rhpds/babylon-runner:dev
```

## Dev Environment Testing

### 1. Run unit tests

```bash
cd anarchy/babylon-runner
make test

# Or run per-package:
go test ./internal/types/ -v
go test ./internal/runner/ -v
go test ./internal/handler/ -v
```

### 2. Test locally against a real cluster

Port-forward the anarchy API from your dev cluster:

```bash
oc port-forward svc/anarchy -n anarchy 5000:5000
```

Get the runner token from an existing runner pod:

```bash
# Find runner pod name and its token
oc get pods -n anarchy -l anarchy.gpte.redhat.com/runner -o yaml \
  | grep -A1 RUNNER_TOKEN
```

Build and run locally:

```bash
export ANARCHY_URL=http://localhost:5000
export RUNNER_NAME=default
export RUNNER_TOKEN=<token-from-above>
export HOSTNAME=local-dev-pod

make build
bin/babylon-runner
```

The runner will start polling for runs. Trigger an action on a subject to see it work.

### 3. Test on-cluster with a dev image

```bash
# Build and push dev image
podman build -t quay.io/rhpds/babylon-runner:dev .
podman push quay.io/rhpds/babylon-runner:dev

# Patch the runner to use your dev image
oc patch anarchyrunner default -n anarchy --type merge -p '
spec:
  podTemplate:
    spec:
      containers:
      - name: runner
        image: quay.io/rhpds/babylon-runner:dev
        imagePullPolicy: Always
'
```

Watch the runner pods restart with the new image:

```bash
oc get pods -n anarchy -l anarchy.gpte.redhat.com/runner -w
```

Check logs:

```bash
oc logs -f -l anarchy.gpte.redhat.com/runner -n anarchy
```

### 4. Revert to the Python runner

```bash
# Remove the image override — pods revert to operator image
oc patch anarchyrunner default -n anarchy --type merge -p '
spec:
  podTemplate:
    spec:
      containers:
      - name: runner
        image: ""
'
```

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `ANARCHY_URL` | yes | — | Anarchy API URL (e.g. `http://anarchy.anarchy.svc:5000`) |
| `RUNNER_NAME` | yes | — | Runner pool name (e.g. `default`) |
| `RUNNER_TOKEN` | yes | — | Auth token (auto-injected by operator) |
| `HOSTNAME` | yes | — | Pod name (auto-injected by k8s downward API) |
| `OPERATOR_NAMESPACE` | no | (from serviceaccount) | Namespace for K8s secret lookups |
| `POLLING_INTERVAL` | no | `5` | Seconds between polls |
| `REQUEST_TIMEOUT` | no | `35` | HTTP request timeout in seconds |
| `SANDBOX_API_URL` | no | `http://sandbox-api.babylon-sandbox-api.svc.cluster.local:8080` | Sandbox API base URL |
| `TOWER_TLS_VERIFY` | no | `true` | Verify Tower/Controller TLS certificates |
| `TOWER_CA_CERT` | no | — | Path to custom CA certificate for Tower |
| `ACTION_RETRY_INTERVALS` | no | `1m,5m,10m,30m,1h,2h,4h,8h,16h,1d` | Comma-separated retry delays for failed actions |
| `METRICS_PORT` | no | `9093` | Prometheus metrics and health endpoint port |
| `MAX_POLL_FAILURES` | no | `10` | Consecutive poll failures before exit |

All required vars are auto-injected by the operator when it creates runner pods.
You only need to set them manually when running locally.

## Observability

### Prometheus Metrics

The runner exposes Prometheus metrics on `:{METRICS_PORT}/metrics`:

| Metric | Type | Description |
|--------|------|-------------|
| `babylon_runner_run_duration_seconds` | histogram | Run execution duration by handler and action |
| `babylon_runner_runs_total` | counter | Total runs by handler, action, and status |
| `babylon_runner_poll_duration_seconds` | histogram | Poll request duration |
| `babylon_runner_tower_job_duration_seconds` | histogram | Tower API operation duration |
| `babylon_runner_sandbox_api_duration_seconds` | histogram | Sandbox API operation duration |
| `babylon_runner_scheduler_api_duration_seconds` | histogram | Controller-scheduler API duration |
| `babylon_runner_active_run` | gauge | 1 if processing a run, 0 if idle |

### Health Endpoints

| Path | Description |
|------|-------------|
| `:{METRICS_PORT}/healthz` | Liveness — always 200 |
| `:{METRICS_PORT}/readyz` | Readiness — 200 if polling is healthy, 503 after `MAX_POLL_FAILURES` consecutive failures |

### Controller Scheduler

When a governor configures `__meta__.controller_scheduler`, the runner calls the controller-scheduler API to select the best controller by score. The scheduler is tried first; on failure it falls back to local `ansible_controllers` selection. Credentials for the selected controller are resolved from K8s Secrets labeled `babylon.gpte.redhat.com/ansible-control-plane={hostname}`.
