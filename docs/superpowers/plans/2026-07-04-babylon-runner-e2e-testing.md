# Babylon Runner Helm Chart: E2E Testing Plan

**Goal:** Validate the babylon-runner Helm chart on the dev cluster (`babylon-anarchy-test` namespace) — from image builds through runner coexistence with the existing `default` runner.

**Cluster:** `~/secrets/ocp-babydev.infra-us-east.kubeconfig`
**Namespace:** `babylon-anarchy-test`
**Branch:** `babylon-runner-restructure`

## Prerequisites

- All 12 implementation tasks from the Helm chart plan are complete
- AnarchyRunner spec fix (commit `616b121`) applied
- Kubeconfig available and cluster accessible

## Conventions

All commands assume:

```bash
export KUBECONFIG=~/secrets/ocp-babydev.infra-us-east.kubeconfig
export NS=babylon-anarchy-test
```

**Deploy method:** `helm template` + `oc apply` (matches ArgoCD pattern used in production).

**Original images to restore during cleanup:**

- Operator + API: `quay.io/rhpds/anarchy:v0.25.19`
- ImageStream registry: `image-registry.openshift-image-registry.svc:5000/babylon-anarchy-test`

---

### Task 1: Pre-flight — Validate Cluster Access and Pre-conditions

**Purpose:** Ensure the cluster is reachable and the namespace has all prerequisites before modifying anything.

- [x] **Step 1: Verify kubeconfig and namespace**

```bash
oc get ns $NS
oc -n $NS get pods
```

Expected: namespace `babylon-anarchy-test` exists and is Active. Pods running: `anarchy-*` (operator), `anarchy-api-*`, `anarchy-runner-default-*`.

- [x] **Step 2: Verify CRDs installed**

```bash
oc get crd anarchyrunners.anarchy.gpte.redhat.com -o jsonpath='{.spec.versions[0].name}'
oc get crd anarchygovernors.anarchy.gpte.redhat.com -o jsonpath='{.spec.versions[0].name}'
```

Expected: both return `v1`.

- [x] **Step 3: Verify operator and API are healthy**

```bash
oc -n $NS get deployment anarchy -o jsonpath='{.status.readyReplicas}'
oc -n $NS get deployment anarchy-api -o jsonpath='{.status.readyReplicas}'
```

Expected: both return `1` (or more).

- [x] **Step 4: Verify existing runner is functional**

```bash
oc -n $NS get anarchyrunners
oc -n $NS get pods -l anarchy.gpte.redhat.com/runner=default
```

Expected: AnarchyRunner `default` exists. At least one runner pod is Running.

- [x] **Step 5: Record current state for cleanup**

```bash
oc -n $NS get deployment anarchy -o jsonpath='{.spec.template.spec.containers[0].image}'
oc -n $NS get deployment anarchy-api -o jsonpath='{.spec.template.spec.containers[0].image}'
```

Expected: both show `quay.io/rhpds/anarchy:v0.25.19`. Save this for Task 10 restore.

---

### Task 2: Build — Rebuild Operator Anarchy from Current Branch

**Purpose:** Rebuild the Anarchy operator image from branch `babylon-runner-restructure` so it includes the `ignore-pod-management` annotation support. Add a permanent log line for operational visibility. Update operator and API deployments to use the new image.

- [x] **Step 1: Add log in** `manage_pods()` **for operational visibility**

In `operator/anarchyrunner.py`, modify the `ignore_pod_management` check in `manage_pods()` to log when it skips:

```python
async def manage_pods(self, logger):
    if self.ignore_pod_management:
        logger.info("Skipping pod management for runner %s (ignore-pod-management annotation set)", self.name)
        return
```

This is a **permanent** change — commit it. The log provides operational visibility when runners are managed externally.

- [x] **Step 2: Commit the log change**

```bash
git add operator/anarchyrunner.py
git commit -m "feat(operator): log when skipping pod management for externally managed runners"
```

- [x] **Step 3: Build the operator image from local source**

```bash
oc -n $NS start-build anarchy --from-dir=. --wait --follow
```

Expected: build completes successfully. The BuildConfig `anarchy` already exists and outputs to `istag anarchy:latest`.

- [x] **Step 4: Verify the new image exists**

```bash
oc -n $NS get istag anarchy:latest -o jsonpath='{.image.dockerImageReference}' && echo ""
```

Expected: returns a valid image reference with a new SHA.

- [x] **Step 5: Update operator deployment to use the new image**

```bash
LOCAL_IMAGE="image-registry.openshift-image-registry.svc:5000/$NS/anarchy:latest"
oc -n $NS set image deployment/anarchy manager="$LOCAL_IMAGE"
oc -n $NS rollout status deployment/anarchy --timeout=120s
```

Expected: rollout completes, operator pod restarts with new image.

- [x] **Step 6: Update API deployment to use the new image**

```bash
oc -n $NS set image deployment/anarchy-api api="$LOCAL_IMAGE"
oc -n $NS rollout status deployment/anarchy-api --timeout=120s
```

Expected: rollout completes, API pod restarts with new image.

- [x] **Step 7: Verify both deployments are healthy with new image**

```bash
oc -n $NS get deployment anarchy -o jsonpath='{.spec.template.spec.containers[0].image}'
echo ""
oc -n $NS get deployment anarchy-api -o jsonpath='{.spec.template.spec.containers[0].image}'
echo ""
oc -n $NS get pods
```

Expected: both deployments show the local ImageStream image. All pods are Running and Ready.

- [x] **Step 8: Verify existing runner still works**

```bash
oc -n $NS get anarchyrunners
oc -n $NS get pods -l anarchy.gpte.redhat.com/runner=default
```

Expected: runner `default` still exists and its pod is Running.

---

### Task 3: Build — Build Babylon Runner Image

**Purpose:** Create the ImageStream and BuildConfig for the babylon-runner, then build the Go binary image.

- [x] **Step 1: Create build resources from template**

```bash
oc -n $NS process -f babylon-runner/build-template.yaml --local | oc -n $NS apply -f -
```

Expected: ImageStream `babylon-runner` and BuildConfig `babylon-runner` created.

- [x] **Step 2: Build the babylon-runner image**

```bash
oc -n $NS start-build babylon-runner --from-dir=babylon-runner/ --wait --follow
```

Expected: multi-stage Go build completes successfully.

- [x] **Step 3: Verify the image exists**

```bash
oc -n $NS get istag babylon-runner:latest -o jsonpath='{.image.dockerImageReference}' && echo ""
```

Expected: returns a valid image reference.

---

### Task 4: Deploy — Install Babylon Runner Helm Chart

**Purpose:** Render the Helm chart and apply to the cluster using `oc apply` (ArgoCD pattern).

- [x] **Step 1: Render and review the manifests**

```bash
helm template babylon-runner babylon-runner/helm/ \
  --namespace $NS \
  --set image.repository="image-registry.openshift-image-registry.svc:5000/$NS/babylon-runner" \
  --set image.tagOverride=latest \
  --set runnerName=babylon \
  --set autoscaling.minReplicas=1 \
  --set autoscaling.maxReplicas=3 \
  > /tmp/babylon-runner-manifests.yaml

cat /tmp/babylon-runner-manifests.yaml
```

Expected: 7 YAML documents rendered. Review for correctness before applying.

- [x] **Step 2: Validate the runner label on pods**

```bash
grep -A1 'runner:' /tmp/babylon-runner-manifests.yaml
```

Expected: `anarchy.gpte.redhat.com/runner: babylon` (NOT `default`).

- [x] **Step 3: Apply the manifests**

```bash
oc -n $NS apply -f /tmp/babylon-runner-manifests.yaml
```

Expected: all 7 resources created/configured.

- [x] **Step 4: Verify resources are up**

```bash
oc -n $NS get sa,cm,deploy,svc,hpa,servicemonitor,anarchyrunner -l app.kubernetes.io/instance=babylon-runner
```

Expected: all 7 resources listed.

---

### Task 5: Verify — Check All 7 Resources Created

**Purpose:** Confirm every resource the chart produces exists and has correct configuration.

- [x] **Step 1: ServiceAccount**

```bash
oc -n $NS get sa babylon-runner
```

Expected: ServiceAccount exists.

- [x] **Step 2: ConfigMap**

```bash
oc -n $NS get cm babylon-runner-env -o yaml
```

Expected: ConfigMap exists with keys `RUNNER_NAME: babylon`, `ANARCHY_URL: http://anarchy.babylon-anarchy-test.svc:5000`, `ANARCHY_DOMAIN`, `POLLING_INTERVAL`, etc.

- [x] **Step 3: Deployment**

```bash
oc -n $NS get deployment babylon-runner -o wide
```

Expected: Deployment exists. Image is the local ImageStream image.

- [x] **Step 4: Service**

```bash
oc -n $NS get svc babylon-runner-metrics
```

Expected: Service exists, port 9093.

- [x] **Step 5: HPA**

```bash
oc -n $NS get hpa babylon-runner
```

Expected: HPA exists targeting the Deployment, min=1, max=3, target CPU 70%.

- [x] **Step 6: ServiceMonitor**

```bash
oc -n $NS get servicemonitor babylon-runner
```

Expected: ServiceMonitor exists with interval 30s, port metrics.

- [x] **Step 7: AnarchyRunner CR**

```bash
oc -n $NS get anarchyrunner babylon -o yaml
```

Expected: AnarchyRunner `babylon` exists with annotation `anarchy.gpte.redhat.com/ignore-pod-management: "true"` and `spec.minReplicas`/`spec.maxReplicas` set.

---

### Task 6: Verify — Operator Integration (ignore-pod-management)

**Purpose:** Confirm the operator recognizes the AnarchyRunner CR but does NOT create pods for it.

- [x] **Step 1: Check both runners are visible**

```bash
oc -n $NS get anarchyrunners
```

Expected: two runners listed — `default` and `babylon`.

- [x] **Step 2: Check operator logs for the ignore-pod-management message**

```bash
oc -n $NS logs deployment/anarchy --since=2m | grep -i "ignore-pod-management\|Skipping pod management"
```

Expected: log line like `Skipping pod management for runner babylon (ignore-pod-management annotation set)`.

- [x] **Step 3: Verify operator did NOT create pods for babylon runner**

The only pods with label `runner=babylon` should be owned by the Deployment (ReplicaSet), not by the AnarchyRunner CR.

```bash
oc -n $NS get pods -l anarchy.gpte.redhat.com/runner=babylon -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.metadata.ownerReferences[0].kind}{"\n"}{end}'
```

Expected: all pods show `ReplicaSet` as owner (from our Deployment), NOT `AnarchyRunner`.

- [x] **Step 4: Verify default runner is unaffected**

```bash
oc -n $NS get pods -l anarchy.gpte.redhat.com/runner=default
```

Expected: default runner pods still Running, managed by operator as before.

---

### Task 7: Verify — Runner Pod Health and API Connectivity

**Purpose:** Confirm the babylon runner pod starts, passes health checks, and authenticates with the Anarchy API.

- [x] **Step 1: Check pod is Running and Ready**

```bash
oc -n $NS get pods -l app.kubernetes.io/name=babylon-runner
```

Expected: at least 1 pod in `Running` state, `READY 1/1`.

- [x] **Step 2: Check pod logs for successful API connection**

```bash
oc -n $NS logs deployment/babylon-runner --tail=50
```

Expected: logs show the runner starting up, connecting to the API URL, and polling for runs. No errors about authentication (401/403), connection refused, or TLS issues.

- [x] **Step 3: Verify health endpoints respond**

The image is a static Go binary (distroless) — no shell tools available. Use port-forward:

```bash
POD=$(oc -n $NS get pod -l app.kubernetes.io/name=babylon-runner -o jsonpath='{.items[0].metadata.name}')
oc -n $NS port-forward $POD 9093:9093 &
sleep 2
curl -s http://localhost:9093/healthz && echo ""
curl -s http://localhost:9093/readyz && echo ""
kill %1
```

Expected: both return a healthy response (200 OK or similar).

- [x] **Step 4: Verify token auth works**

Check the API logs for the babylon runner's polling requests:

```bash
oc -n $NS logs deployment/anarchy-api --tail=50 | grep -i "babylon"
```

Expected: no authentication errors. The runner's polling requests should be accepted.

---

### Task 8: Verify — Runner Coexistence (default + babylon)

**Purpose:** The critical functional test. Create multiple AnarchySubjects via the UI and verify that both runners process runs in parallel.

**This task involves manual interaction:** the user creates AnarchySubjects through the Anarchy interface/UI while we observe the results.

- [x] **Step 1: Record baseline state**

```bash
# Count existing completed runs
oc -n $NS get anarchyruns -o jsonpath='{range .items[*]}{.metadata.labels.anarchy\.gpte\.redhat\.com/runner}{"\n"}{end}' | sort | uniq -c

# Check default runner pod run count
oc -n $NS get anarchyrunner default -o jsonpath='{.status.pods}' | python3 -m json.tool
```

Record the current run count and distribution.

- [x] **Step 2: User creates AnarchySubjects via UI**

The user creates multiple AnarchySubjects through the interface to generate AnarchyRuns.

- [x] **Step 3: Monitor run distribution between runners**

Watch runs being created and assigned:

```bash
oc -n $NS get anarchyruns -w
```

In a separate terminal, monitor which pods process runs:

```bash
oc -n $NS logs -f deployment/babylon-runner 2>&1 | grep -i "run\|processing\|complete"
```

And the default runner:

```bash
oc -n $NS logs -f -l anarchy.gpte.redhat.com/runner=default 2>&1 | grep -i "run\|processing\|complete"
```

- [x] **Step 4: Verify runs completed successfully**

After subjects are created:

```bash
oc -n $NS get anarchyruns --sort-by=.metadata.creationTimestamp | tail -20
```

Expected: runs show `runner` label with pods from BOTH runners. All runs reach `successful` state.

- [x] **Step 5: Verify run distribution across both runners**

```bash
# Check which runner pod processed each recent run
oc -n $NS get anarchyruns --sort-by=.metadata.creationTimestamp -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.metadata.labels.anarchy\.gpte\.redhat\.com/runner}{"\n"}{end}' | tail -20

# Count runs per runner pod
oc -n $NS get anarchyruns -o jsonpath='{range .items[*]}{.metadata.labels.anarchy\.gpte\.redhat\.com/runner}{"\n"}{end}' | sort | uniq -c
```

Expected: runs distributed across pods from both `default` and `babylon` runners. Both runners show increased run counts compared to baseline.

---

### Task 9: Verify — Metrics

**Purpose:** Confirm the metrics endpoint is serving data.

> **Note:** HPA and ServiceMonitor were disabled in `helm-vars-dev.yaml` for this test cycle (metrics-server instability on the dev cluster). They will be validated in production.

- [x] **Step 1: Check metrics endpoint**

The image is a static Go binary (distroless) — use port-forward:

```bash
POD=$(oc -n $NS get pod -l app.kubernetes.io/name=babylon-runner -o jsonpath='{.items[0].metadata.name}')
oc -n $NS port-forward $POD 9093:9093 &
sleep 2
curl -s http://localhost:9093/metrics | head -40
kill %1
```

Expected: Prometheus-formatted metrics output with `babylon_runner_*` metrics.

- [ ] **Step 2: Verify key metrics are present**

```bash
POD=$(oc -n $NS get pod -l app.kubernetes.io/name=babylon-runner -o jsonpath='{.items[0].metadata.name}')
oc -n $NS port-forward $POD 9093:9093 &
sleep 2
curl -s http://localhost:9093/metrics | grep -E "^babylon_runner_|^# HELP babylon_runner_" | head -20
kill %1
```

Expected: metrics related to runs processed, poll duration, errors, etc.

---

### Task 10: Cleanup — Restore Cluster to Original State

**Purpose:** Remove all test artifacts and restore the namespace to its pre-test state.

- [x] **Step 1: Remove the babylon-runner manifests**

```bash
oc -n $NS delete -f /tmp/babylon-runner-manifests.yaml
```

Expected: all 7 chart resources removed.

- [ ] **Step 2: Verify all chart resources are removed**

```bash
oc -n $NS get sa,cm,deploy,svc,hpa,servicemonitor,anarchyrunner -l app.kubernetes.io/instance=babylon-runner
```

Expected: no resources found.

- [ ] **Step 3: Remove babylon-runner build resources**

```bash
oc -n $NS delete bc babylon-runner
oc -n $NS delete is babylon-runner
```

Expected: BuildConfig and ImageStream removed.

- [ ] **Step 4: Restore operator and API to original image**

```bash
ORIGINAL_IMAGE="quay.io/rhpds/anarchy:v0.25.19"
oc -n $NS set image deployment/anarchy manager="$ORIGINAL_IMAGE"
oc -n $NS set image deployment/anarchy-api api="$ORIGINAL_IMAGE"
oc -n $NS rollout status deployment/anarchy --timeout=120s
oc -n $NS rollout status deployment/anarchy-api --timeout=120s
```

Expected: both deployments rollback to original image, pods restart and become Ready.

- [ ] **Step 5: Verify cluster is restored**

```bash
oc -n $NS get pods
oc -n $NS get anarchyrunners
oc -n $NS get deployment anarchy -o jsonpath='{.spec.template.spec.containers[0].image}'
echo ""
oc -n $NS get deployment anarchy-api -o jsonpath='{.spec.template.spec.containers[0].image}'
```

Expected: only the original pods (`anarchy-*`, `anarchy-api-*`, `anarchy-runner-default-*`), one AnarchyRunner (`default`), both deployments using `quay.io/rhpds/anarchy:v0.25.19`.
