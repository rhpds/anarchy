# Babylon Runner: Independent Helm Deployment & Integration Design

**Date:** 2026-07-04
**Status:** Draft
**Supersedes:** [2026-06-20-babylon-runner-helm-design.md](2026-06-20-babylon-runner-helm-design.md) (drop-in approach — too intrusive)
**Prerequisites:** [2026-06-24-pending-runs-metric-design.md](2026-06-24-pending-runs-metric-design.md) (`anarchy_runner_pending_runs` gauge)

## Goal

Deploy the Go-based `babylon-runner` independently from the Anarchy operator using a separate Helm chart with native Kubernetes primitives (Deployment, HPA, ServiceMonitor). The design enables a smooth, phased transition from the Python runner to the Go runner across ~31 namespaces with ~290 runner pods, with rollback possible at every phase.

## Non-Goals

- Modifying the CRD schema
- Changing the API code
- Installing cluster-level components (KEDA, prometheus-adapter)
- Replacing the existing Anarchy Helm chart
- Per-governor routing (future work)

## Design Principles

- **Minimal operator change** — ~3 lines to skip pod management when annotation is present
- **Zero API changes** — Go runner uses the same auth and polling protocol
- **Independent lifecycle** — separate chart, separate release, separate versioning
- **Safe coexistence** — Python and Go runners share the same FIFO queue during transition
- **Instant rollback** — delete the Helm release; Python runner is untouched

## Architecture

```
                    ┌──────────────────────┐
                    │    Anarchy API        │
                    │    GET /run (FIFO)    │
                    │    port 5000          │
                    └──────────┬───────────┘
                               │
                  pending_run_names (single queue)
                               │
                  ┌────────────┴────────────┐
                  ▼                         ▼
     ┌──────────────────────┐  ┌──────────────────────┐
     │ AnarchyRunner CR     │  │ AnarchyRunner CR     │
     │ name: default        │  │ name: babylon        │
     │ managed by: operator │  │ annotation:          │
     │ pods: operator-      │  │   pod-management:    │
     │   created raw Pods   │  │   external           │
     │ (Python/Ansible)     │  │ managed by: Helm     │
     └──────────────────────┘  │ pods: Deployment     │
                               │ scaling: HPA (CPU)   │
                               │ (Go binary)          │
                               └──────────────────────┘
```

Both runners poll `GET /run` with a valid `Authorization: Bearer {runner_name}:{pod_name}:{runner_token}` header. The API dispatches pending runs FIFO to any authenticated pod — no routing by runner name. Both pools drain the same queue in parallel.

## What Changes

| Component | Change |
|---|---|
| CRD | None |
| API | None |
| Operator (`operator/anarchyrunner.py`) | ~3 lines: skip `manage_pods()` if annotation `pod-management: external` |
| New chart (`babylon-runner/helm/`) | ConfigMap + Deployment + Service + HPA + ServiceMonitor + AnarchyRunner CR |
| Token | Deterministic sha256, literal env var |
| HPA | CPU-based, native `autoscaling/v2` |

## Operator Change

In `operator/anarchyrunner.py`, method `manage_pods()` (line 218), add an early return when the annotation signals external pod management:

```python
async def manage_pods(self, logger):
    if self.annotations.get(f"{Anarchy.domain}/pod-management") == "external":
        return
    if not self.pods_preloaded:
        await self.preload_pods()
    # ... rest unchanged ...
```

This is the only code change to the Anarchy codebase. The operator still creates the AnarchyRunner CR's status, watches for events, and tracks runs — it just doesn't create, scale, or terminate pods for this runner.

## Authentication

### How the API validates tokens (zero changes)

The API watches pods in its namespace via the label `anarchy.gpte.redhat.com/runner`. When a runner pod makes a request, the API:

1. Extracts `runner_name`, `pod_name`, and `runner_token` from the Bearer header
2. Looks up the `AnarchyRunner` in cache by `runner_name`
3. Looks up the pod in cache by `pod_name`
4. Reads the literal `RUNNER_TOKEN` env var from the pod's spec (`api/anarchyrunnerpod.py:21-42`)
5. Compares the token from the header with the token from the pod spec

The token must be a **literal value** in the pod spec — `secretKeyRef` or `configMapKeyRef` values resolve to `None` in the Kubernetes API and would fail validation. This is why `RUNNER_TOKEN` is set as a literal `env` entry in the Deployment, not in the ConfigMap.

### Deterministic token

The Helm chart generates a deterministic token using sha256:

```yaml
{{- $tokenInput := printf "%s-%s-babylon-runner-%s" .Release.Namespace .Release.Name .Values.auth.tokenSalt }}
{{- $token := $tokenInput | sha256sum | trunc 32 }}
```

This token is:

- **Stable across ArgoCD syncs** — same inputs produce the same hash
- **Rotatable** — change `auth.tokenSalt` in values to generate a new token
- **Unique per namespace** — namespace is part of the hash input
- **Injected as literal** — set directly in the Deployment env, readable by the API

### Required pod labels

The Deployment must include the label `anarchy.gpte.redhat.com/runner: babylon` on pods so the API's watch loop discovers them.

## Helm Chart: `babylon-runner/helm/`

### Values

```yaml
runnerName: babylon

auth:
  tokenSalt: "v1"  # change to rotate token

image:
  repository: quay.io/rhpds/babylon-runner
  tag: ""  # defaults to .Chart.AppVersion
  pullPolicy: IfNotPresent

resources:
  requests:
    cpu: 50m
    memory: 64Mi
  limits:
    cpu: 500m
    memory: 128Mi

# babylon-runner configuration
# All values map to environment variables in the Go binary.
# See babylon-runner/internal/runner/config.go for defaults.
config:
  anarchyURL: ""               # empty = http://anarchy.<namespace>.svc:5000
  anarchyDomain: "anarchy.gpte.redhat.com"  # label/annotation domain prefix
  pollingInterval: "5"         # seconds between GET /run polls
  requestTimeout: "35"         # HTTP request timeout in seconds
  sandboxAPIURL: ""            # empty = in-cluster default
  towerTLSVerify: "true"       # verify Tower/Controller TLS certs
  towerCACert: ""              # path to custom CA cert for Tower
  actionRetryIntervals: ""     # empty = default schedule (1m,5m,10m,...,1d)
  metricsPort: "9093"          # Prometheus metrics and health port
  maxPollFailures: "10"        # consecutive poll failures before exit

# Additional env vars added to the ConfigMap.
# Example:
#   extraEnvVars:
#   - name: MY_CUSTOM_VAR
#     value: "my-value"
extraEnvVars: []

autoscaling:
  enabled: true
  minReplicas: 1
  maxReplicas: 10
  targetCPUUtilizationPercentage: 70
  scaleUp:
    stabilizationWindowSeconds: 120  # 2min with sustained load before adding pods
    maxPods: 2
    periodSeconds: 60
  scaleDown:
    stabilizationWindowSeconds: 300  # 5min stable before removing pods
    maxPods: 1
    periodSeconds: 60

serviceMonitor:
  enabled: true
  interval: 30s
```

### Templates

The chart produces 6 resources:

**1. AnarchyRunner CR** — with the `pod-management: external` annotation:

```yaml
apiVersion: anarchy.gpte.redhat.com/v1
kind: AnarchyRunner
metadata:
  name: {{ .Values.runnerName }}
  annotations:
    {{ .Values.config.anarchyDomain }}/pod-management: external
```

**2. ConfigMap** (`cm-env.yaml`) — centralizes all configuration as environment variables:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ include "babylon-runner.fullname" . }}-env
  labels:
    {{- include "babylon-runner.labels" . | nindent 4 }}
data:
  ANARCHY_URL: {{ .Values.config.anarchyURL | default (printf "http://anarchy.%s.svc:5000" .Release.Namespace) | quote }}
  RUNNER_NAME: {{ .Values.runnerName | quote }}
  POLLING_INTERVAL: {{ .Values.config.pollingInterval | quote }}
  REQUEST_TIMEOUT: {{ .Values.config.requestTimeout | quote }}
  TOWER_TLS_VERIFY: {{ .Values.config.towerTLSVerify | quote }}
  METRICS_PORT: {{ .Values.config.metricsPort | quote }}
  MAX_POLL_FAILURES: {{ .Values.config.maxPollFailures | quote }}
  {{- if .Values.config.sandboxAPIURL }}
  SANDBOX_API_URL: {{ .Values.config.sandboxAPIURL | quote }}
  {{- end }}
  {{- if .Values.config.towerCACert }}
  TOWER_CA_CERT: {{ .Values.config.towerCACert | quote }}
  {{- end }}
  {{- if .Values.config.actionRetryIntervals }}
  ACTION_RETRY_INTERVALS: {{ .Values.config.actionRetryIntervals | quote }}
  {{- end }}
  {{- range .Values.extraEnvVars }}
  {{ .name | upper }}: {{ .value | quote }}
  {{- end }}
```

Variables with empty defaults (`anarchyURL`, `sandboxAPIURL`, `towerCACert`, `actionRetryIntervals`) use fallback values when not set: `anarchyURL` defaults to `http://anarchy.<namespace>.svc:5000` (matching the operator's logic in `operator/anarchyrunner.py:144`); the others fall back to the Go binary's compiled-in defaults.

**3. Deployment** — uses `envFrom` for ConfigMap and literal `env` for token:

```yaml
{{- $tokenInput := printf "%s-%s-babylon-runner-%s" .Release.Namespace .Release.Name .Values.auth.tokenSalt }}
{{- $token := $tokenInput | sha256sum | trunc 32 }}
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ include "babylon-runner.fullname" . }}
  annotations:
    reloader.stakater.com/auto: "true"
    checksum/env: {{ include (print .Template.BasePath "/cm-env.yaml") . | sha256sum }}
spec:
  selector:
    matchLabels:
      {{- include "babylon-runner.selectorLabels" . | nindent 6 }}
  template:
    metadata:
      labels:
        {{- include "babylon-runner.labels" . | nindent 8 }}
        {{ .Values.config.anarchyDomain }}/runner: {{ .Values.runnerName }}
    spec:
      serviceAccountName: {{ include "babylon-runner.fullname" . }}
      containers:
      - name: runner
        image: "{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}"
        imagePullPolicy: {{ .Values.image.pullPolicy }}
        envFrom:
        - configMapRef:
            name: {{ include "babylon-runner.fullname" . }}-env
        env:
        # RUNNER_TOKEN must be a literal env var — the API reads it
        # from the pod spec. configMapRef/secretKeyRef resolve to
        # None in the K8s API and would fail auth validation.
        - name: RUNNER_TOKEN
          value: {{ $token | quote }}
        ports:
        - name: metrics
          containerPort: 9093
        readinessProbe:
          httpGet:
            path: /readyz
            port: metrics
          initialDelaySeconds: 5
          periodSeconds: 10
        livenessProbe:
          httpGet:
            path: /healthz
            port: metrics
          initialDelaySeconds: 5
          periodSeconds: 30
        resources:
          {{- toYaml .Values.resources | nindent 10 }}
```

The Deployment uses two mechanisms for config change detection:

- `checksum/env` annotation — triggers rolling restart on ConfigMap content change (works with any Helm-based deploy)
- `reloader.stakater.com/auto: "true"` — Stakater Reloader watches for ConfigMap changes and triggers restarts (works with `oc apply`, ArgoCD, or any external change to the ConfigMap)

Both are included for defense in depth. The `checksum/env` catches changes during `helm upgrade`; the Reloader catches out-of-band changes.

**4. Service** — exposes metrics port for ServiceMonitor:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: {{ include "babylon-runner.fullname" . }}-metrics
  labels:
    {{- include "babylon-runner.labels" . | nindent 4 }}
spec:
  ports:
  - name: metrics
    port: 9093
    targetPort: metrics
  selector:
    {{- include "babylon-runner.selectorLabels" . | nindent 4 }}
```

**5. HPA** — CPU-based with stabilization windows:

```yaml
{{- if .Values.autoscaling.enabled }}
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: {{ include "babylon-runner.fullname" . }}
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: {{ include "babylon-runner.fullname" . }}
  minReplicas: {{ .Values.autoscaling.minReplicas }}
  maxReplicas: {{ .Values.autoscaling.maxReplicas }}
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: {{ .Values.autoscaling.targetCPUUtilizationPercentage }}
  behavior:
    scaleUp:
      stabilizationWindowSeconds: {{ .Values.autoscaling.scaleUp.stabilizationWindowSeconds }}
      policies:
      - type: Pods
        value: {{ .Values.autoscaling.scaleUp.maxPods }}
        periodSeconds: {{ .Values.autoscaling.scaleUp.periodSeconds }}
    scaleDown:
      stabilizationWindowSeconds: {{ .Values.autoscaling.scaleDown.stabilizationWindowSeconds }}
      policies:
      - type: Pods
        value: {{ .Values.autoscaling.scaleDown.maxPods }}
        periodSeconds: {{ .Values.autoscaling.scaleDown.periodSeconds }}
{{- end }}
```

**6. ServiceMonitor** — for Prometheus scraping (no basicAuth — Go runner `/metrics` endpoint is unauthenticated):

```yaml
{{- if .Values.serviceMonitor.enabled }}
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: {{ include "babylon-runner.fullname" . }}
  labels:
    {{- include "babylon-runner.labels" . | nindent 4 }}
spec:
  endpoints:
  - interval: {{ .Values.serviceMonitor.interval }}
    path: /metrics
    port: metrics
  namespaceSelector:
    matchNames:
    - {{ .Release.Namespace }}
  selector:
    matchLabels:
      {{- include "babylon-runner.selectorLabels" . | nindent 6 }}
{{- end }}
```

### Resource Comparison

| Resource | Python Runner | Go Runner |
|---|---|---|
| CPU request | 500m | 50m |
| CPU limit | 1 | 500m |
| Memory request | 512Mi | 64Mi |
| Memory limit | 1Gi | 128Mi |
| Image size | ~500MB+ | ~15MB |

## HPA Design

### Why CPU-based

The cluster does not have KEDA or prometheus-adapter installed. Custom metrics (`anarchy_runner_pending_runs`) would require cluster-level changes. CPU-based HPA is native, requires nothing extra, and works well for this workload — the runner consumes CPU when processing runs and is near-idle when polling.

### Stabilization windows

- **Scale up:** 120s stabilization — CPU must stay above target for 2 minutes before adding pods. Maximum 2 pods per 60s. Prevents bursts from causing pod explosion.
- **Scale down:** 300s stabilization — CPU must stay below target for 5 minutes before removing pods. Maximum 1 pod per 60s. Prevents flapping.

### Future: custom metrics

When KEDA or prometheus-adapter becomes available, add `anarchy_runner_pending_runs` as a second metric source. The HPA v2 spec supports multiple metrics and uses the one requesting the most replicas. No Deployment changes needed.

## Migration Guide

This section serves as an operational guide for the phased transition from Python to Go runner.

### Phase 0 — Prerequisites

| Action | Detail |
|---|---|
| Merge operator change | ~3 lines: skip `manage_pods()` when annotation `pod-management: external` is present |
| Build Go runner image | Push to `quay.io/rhpds/babylon-runner:v0.1.0` |
| Chart `babylon-runner/helm/` ready | Validated with `helm template` |
| `anarchy_runner_pending_runs` metric deployed | Per [pending-runs-metric-design](2026-06-24-pending-runs-metric-design.md) |

### Phase 1 — Pilot (1 low-traffic namespace)

Deploy the Go runner alongside the existing Python runner in a single namespace:

```bash
helm install babylon-runner babylon-runner/helm/ \
  --namespace babylon-anarchy-7 \
  --set runnerName=babylon \
  --set autoscaling.minReplicas=1 \
  --set autoscaling.maxReplicas=3
```

**What happens:**

- Python runner `default` continues operating normally (zero changes)
- Go runner `babylon` starts picking up runs from the shared queue
- Both process runs in parallel via FIFO dispatch

**Validation criteria before advancing:**

- Go runner logs clean for 24h (no errors, no panics)
- Runs complete with `status: successful`
- Metrics `babylon_runner_runs_total` visible in Prometheus via ServiceMonitor
- Run duration comparable to Python historical baseline
- No impact on Python runner operations

### Phase 2 — Expand (5-10 namespaces)

Deploy to namespaces with varying traffic levels using a shared values file:

```yaml
# values-production.yaml
runnerName: babylon
image:
  repository: quay.io/rhpds/babylon-runner
  tag: v0.1.0
autoscaling:
  enabled: true
  minReplicas: 1
  maxReplicas: 5
  targetCPUUtilizationPercentage: 70
```

```bash
for ns in babylon-anarchy-{5,6,7}; do
  helm install babylon-runner babylon-runner/helm/ \
    --namespace "$ns" \
    -f values-production.yaml
done
```

**Validation:** same criteria as Phase 1, monitored for 48-72h across all namespaces.

### Phase 3 — Reduce Python runners

In validated namespaces, reduce Python runner replicas to minimum (safety net):

```bash
oc patch anarchyrunner default -n babylon-anarchy-7 \
  --type merge -p '{"spec":{"minReplicas":1,"maxReplicas":1}}'
```

The Go runner with HPA handles the load. The Python runner stays as fallback — if the Go runner fails, the single Python pod processes pending runs.

### Phase 4 — Full cutover (per namespace)

After the Go runner is stable for 1+ week in a namespace:

```bash
# Remove Python runner from namespace
oc delete anarchyrunner default -n babylon-anarchy-7
```

The operator deletes Python pods. The Go runner is the sole runner in the namespace.

### Rollback

Each phase has independent, instant rollback:

| Phase | Rollback action | Impact |
|---|---|---|
| 1-2 | `helm uninstall babylon-runner -n <namespace>` | Go pods removed. Python runner untouched. Zero downtime |
| 3 | Restore original `minReplicas`/`maxReplicas` on AnarchyRunner `default` | Python scales back up |
| 4 | Re-apply original AnarchyRunner via infra values / ArgoCD | Operator recreates Python pods |

In Phases 1-3, rollback is simply deleting the Helm release — the Python runner was never modified.

### Monitoring during transition

```promql
# Go runner: successful runs rate
rate(babylon_runner_runs_total{status="successful"}[5m])

# Go runner: error rate
rate(babylon_runner_runs_total{status="failed"}[5m])

# Pending runs queue depth (growing = queue not draining)
anarchy_runner_pending_runs{namespace="<namespace>"}

# Go runner activity (0=idle, 1=processing)
babylon_runner_active_run
```

Suggested alerts:

- `anarchy_runner_pending_runs > 20` for 5+ minutes — queue growing, investigate
- `rate(babylon_runner_runs_total{status="failed"}[5m]) > 0.1` — high error rate, consider rollback

### Suggested timeline

| Week | Action |
|---|---|
| S1 | Phase 0: merge operator change, build image, finalize chart |
| S1-S2 | Phase 1: pilot in 1 namespace, monitor for 1 week |
| S3-S4 | Phase 2: expand to 5-10 namespaces |
| S5-S6 | Phase 3: reduce Python runners in validated namespaces |
| S7+ | Phase 4: full cutover, namespace by namespace |

The pace is intentionally conservative for a production-critical environment. Can be accelerated if Phase 1 goes well.

## Future Work

- **BasicAuth on Go runner metrics** — add auth middleware to `/metrics` endpoint for consistency with existing ServiceMonitors
- **KEDA / prometheus-adapter HPA** — when available on cluster, add `anarchy_runner_pending_runs` as custom metric trigger
- **Per-governor routing** — use `spec.runner` field on AnarchyGovernor to route specific workloads to specific runner pools
- **ArgoCD ApplicationSet** — automate babylon-runner deployment across namespaces via generators

## References

- [babylon-runner design](2026-05-15-babylon-runner-design.md) — original Go rewrite spec
- [babylon-runner improvements](2026-06-18-babylon-runner-improvements.md) — restructuring and code review
- [babylon-runner helm design](2026-06-20-babylon-runner-helm-design.md) — superseded drop-in approach
- [pending-runs-metric design](2026-06-24-pending-runs-metric-design.md) — `anarchy_runner_pending_runs` gauge
- `operator/anarchyrunner.py:218` — `manage_pods()` method (operator change location)
- `api/anarchyrunnerpod.py:21-42` — token validation logic
- `babylon-runner/internal/runner/config.go` — Go runner configuration and env var parsing
- `babylon-runner/internal/metrics/server.go` — Go runner metrics server (no auth)
- `babylon-runner/DEPLOY.md` — deployment and testing guide
