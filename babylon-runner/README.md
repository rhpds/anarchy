# babylon-runner

A Go-based runner for processing AnarchyRun workloads. Drop-in replacement for the Python/Ansible runner using the same protocol: polls `GET /run` for work, executes handlers, posts results to `POST /run/{name}`.

## Architecture

```text
babylon-runner/
  cmd/babylon-runner/       Entry point (main.go)
  internal/
    clients/                AnarchyClient, DeployerClient HTTP clients
    handler/                Handler implementations (provision, destroy, start, stop, etc.)
    httputil/               Shared HTTP helpers and token cache
    metrics/                Prometheus metrics and health endpoints
    runner/                 Core runner loop, config, dispatch, RunContext
    secrets/                Kubernetes Secret cache (informer-backed)
    template/               Template rendering utilities
    types/                  Shared data types (RunPayload, RunResult, etc.)
  helm/                     Independent Helm chart (Deployment + HPA + ServiceMonitor)
  Dockerfile                Multi-stage build (golang → ubi9-micro)
  Makefile                  build, test, lint, clean, docker-build targets
  build-template.yaml       OpenShift BuildConfig template
  dev-run.sh                Local development script
```

### How It Works

1. The runner long-polls the Anarchy API (`GET /run`) waiting for work
2. When a run arrives, it dispatches to the appropriate handler based on governor configuration
3. Handlers interact with external systems (Ansible Controller, Sandbox API, controller-scheduler)
4. Results are posted back to the API (`POST /run/{name}`)
5. The operator does not care what language the runner is written in — only that it speaks the protocol

### Key Design Decisions

- **Independent Helm chart** — deployed separately from the Anarchy operator, managed by its own HPA
- **`ignore-pod-management` annotation** — the AnarchyRunner CR tells the operator to skip pod management; the Deployment + HPA handle scaling
- **Deterministic token auth** — `sha256(namespace-release-babylon-runner-salt) | trunc 32`, no Secrets needed
- **Graceful degradation** — Kubernetes client, secret cache, and controller-scheduler are optional; the runner works without them

## Prerequisites

- Go 1.26+
- `oc` CLI with cluster access
- Helm 3
- Anarchy operator running in the target namespace

## Quick Start

### Deploy with Helm

```bash
helm install babylon-runner helm/ \
  --namespace <anarchy-namespace> \
  --set runnerName=babylon
```

### Deploy with custom values

```bash
helm install babylon-runner helm/ \
  --namespace <anarchy-namespace> \
  -f values-production.yaml
```

### Upgrade

```bash
helm upgrade babylon-runner helm/ \
  --namespace <anarchy-namespace> \
  -f values-production.yaml
```

### Uninstall

```bash
helm uninstall babylon-runner --namespace <anarchy-namespace>
```

## Development

### Method 1: Local Binary with `dev-run.sh` (Recommended)

The fastest iteration loop (~2s compile). Automatically sets up port-forwarding, creates a dev pod with the right labels, and runs the binary locally:

```bash
cd babylon-runner
./dev-run.sh
```

This script:

1. Port-forwards `svc/anarchy` from the cluster
2. Creates a lightweight dev pod so the API recognizes the runner
3. Builds and runs the Go binary locally
4. Cleans up everything on exit (Ctrl+C)

Environment overrides:

```bash
OPERATOR_NAMESPACE=my-namespace RUNNER_NAME=my-runner ./dev-run.sh
```

### Method 2: BuildConfig (Full Cluster)

Build and deploy the image inside OpenShift (~1-2 min):

```bash
# Create the BuildConfig
oc process -f build-template.yaml --local \
  -p GIT_REF=$(git rev-parse --abbrev-ref HEAD) | oc apply -f -

# Build the image
oc start-build babylon-runner -n <namespace> --from-dir=. --follow

# Deploy with Helm pointing to the internal registry
helm upgrade --install babylon-runner helm/ \
  --namespace <namespace> \
  -f helm-vars-dev.yaml
```

### Running Tests

```bash
make test             # all tests
go test ./internal/handler/ -v -run TestProvision   # filtered
```

### Linting & Formatting

```bash
make lint    # golangci-lint
make fmt     # gofmt
make vet     # go vet
```

### Building

```bash
make build          # local binary → bin/babylon-runner
make docker-build   # container image via podman
```

## Helm Chart

The chart at `helm/` deploys:

| Template              | Resource                                                        |
| --------------------- | --------------------------------------------------------------- |
| `deployment.yaml`     | Deployment with configurable resources and env vars             |
| `anarchyrunner.yaml`  | AnarchyRunner CR with `ignore-pod-management` annotation        |
| `hpa.yaml`            | HorizontalPodAutoscaler (CPU-based, with stabilization windows) |
| `configmap-env.yaml`  | ConfigMap with runner configuration                             |
| `service.yaml`        | Service for metrics scraping                                    |
| `serviceaccount.yaml` | ServiceAccount for the runner pods                              |
| `rbac.yaml`           | Role + RoleBinding for Secret read access                       |
| `servicemonitor.yaml` | Prometheus ServiceMonitor (optional)                            |

### Key Values

| Value                                        | Default                        | Description                             |
| -------------------------------------------- | ------------------------------ | --------------------------------------- |
| `runnerName`                                 | `babylon`                      | Runner pool name                        |
| `image.repository`                           | `quay.io/rhpds/babylon-runner` | Container image                         |
| `autoscaling.enabled`                        | `true`                         | Enable HPA                              |
| `autoscaling.minReplicas`                    | `1`                            | Minimum pods                            |
| `autoscaling.maxReplicas`                    | `10`                           | Maximum pods                            |
| `autoscaling.targetCPUUtilizationPercentage` | `50`                           | CPU target for scaling                  |
| `auth.tokenSalt`                             | `v1`                           | Salt for deterministic token generation |
| `serviceMonitor.enabled`                     | `true`                         | Deploy Prometheus ServiceMonitor        |

See [helm/values.yaml](helm/values.yaml) for the full list.

### Token Rotation

Change the salt and upgrade — pods restart with the new token:

```bash
helm upgrade babylon-runner helm/ \
  --namespace <namespace> \
  --set auth.tokenSalt=v2
```

## Environment Variables

| Variable                 | Required | Default                                       | Description                                 |
| ------------------------ | -------- | --------------------------------------------- | ------------------------------------------- |
| `ANARCHY_URL`            | yes      | —                                             | Anarchy API URL                             |
| `RUNNER_NAME`            | yes      | —                                             | Runner pool name                            |
| `RUNNER_TOKEN`           | yes      | —                                             | Auth token (auto-injected by Helm/operator) |
| `HOSTNAME`               | yes      | —                                             | Pod name (Kubernetes downward API)          |
| `OPERATOR_NAMESPACE`     | no       | (from serviceaccount)                         | Namespace for Secret lookups                |
| `POLLING_INTERVAL`       | no       | `5`                                           | Seconds between polls                       |
| `REQUEST_TIMEOUT`        | no       | `35`                                          | HTTP request timeout (seconds)              |
| `SANDBOX_API_URL`        | no       | `http://sandbox-api...svc.cluster.local:8080` | Sandbox API base URL                        |
| `TOWER_TLS_VERIFY`       | no       | `true`                                        | Verify Tower/Controller TLS                 |
| `TOWER_CA_CERT`          | no       | —                                             | Path to custom CA certificate               |
| `ACTION_RETRY_INTERVALS` | no       | `1m,5m,10m,30m,1h,2h,4h,8h,16h,1d`            | Retry delays for failed actions             |
| `METRICS_PORT`           | no       | `9093`                                        | Prometheus/health endpoint port             |
| `MAX_POLL_FAILURES`      | no       | `10`                                          | Consecutive failures before exit            |

All required variables are auto-injected when deployed via Helm. Manual setup is only needed for local development.

## Observability

### Prometheus Metrics

Exposed on `:{METRICS_PORT}/metrics`:

| Metric                                          | Type      | Description                           |
| ----------------------------------------------- | --------- | ------------------------------------- |
| `babylon_runner_runs_total`                     | counter   | Total runs by handler, action, status |
| `babylon_runner_run_duration_seconds`           | histogram | Run execution duration                |
| `babylon_runner_poll_duration_seconds`          | histogram | Poll request duration                 |
| `babylon_runner_tower_job_duration_seconds`     | histogram | Tower API operation duration          |
| `babylon_runner_sandbox_api_duration_seconds`   | histogram | Sandbox API duration                  |
| `babylon_runner_scheduler_api_duration_seconds` | histogram | Controller-scheduler API duration     |
| `babylon_runner_active_run`                     | gauge     | 1 if processing a run, 0 if idle      |

### Health Endpoints

| Path                      | Description                                                    |
| ------------------------- | -------------------------------------------------------------- |
| `:{METRICS_PORT}/healthz` | Liveness — always 200                                          |
| `:{METRICS_PORT}/readyz`  | Readiness — 503 after `MAX_POLL_FAILURES` consecutive failures |

## Reverting to the Python Runner

Remove the Helm release — the Python runner (if still present) is untouched:

```bash
helm uninstall babylon-runner --namespace <namespace>
```
