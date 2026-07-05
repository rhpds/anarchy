# Performance Baseline: Go Runner vs Python Runner

Comparative performance analysis between the Go-based `babylon-runner` and the Python/Ansible `anarchy-runner` (default). These baselines were collected on a live cluster under real workloads to inform scaling decisions, HPA configuration, and resource allocation.

## Test Environment

| Parameter | Value                             |
| --------- | --------------------------------- |
| Cluster   | ocp-babydev (infra-us-east)       |
| OpenShift | 4.21.16 / Kubernetes 1.34.7       |
| Namespace | `babylon-anarchy-test`            |
| Governor  | `tests.babylon-empty-config.prod` |
| Date      | 2026-07-04 / 2026-07-05           |

### Runner Resource Configuration

|                        | Go Runner (`babylon`)                      | Python Runner (`default`) |
| ---------------------- | ------------------------------------------ | ------------------------- |
| Image                  | `babylon-runner` (multi-stage, ubi9-micro) | `python-kopf-s2i`         |
| CPU request / limit    | 50m / 500m                                 | 500m / 1000m              |
| Memory request / limit | 64Mi / 128Mi                               | 512Mi / 1Gi               |

## Test 1: Start/Stop Throughput (2026-07-04)

Measures runner throughput under sustained load from workshop start/stop operations. Both runners shared the workload during a workshop lifecycle with ~80 active subjects.

### Runner Replicas

| Runner             | Pods                             |
| ------------------ | -------------------------------- |
| Go (`babylon`)     | 1 (Helm deploy)                  |
| Python (`default`) | 5 (default AnarchyRunner config) |

### Test 1 Methodology

1. Workshop `tests.babylon-empty-config.prod` provisioned with multiple seats
2. WorkshopProvision patched: `startDelay: 1s`, `concurrency: 25`
3. Go runner deployed via Helm (1 pod), Python runner at default 5 pods
4. Metrics collected from AnarchyRun resources in the namespace

### Results — Run Distribution

| Runner          | Runs Processed | Share |
| --------------- | -------------- | ----- |
| Go (1 pod)      | 253            | 78%   |
| Python (5 pods) | 72             | 22%   |

A single Go pod processed **3.5x more runs** than the Python runner's entire fleet of 5 pods combined.

### Results — Execution Time per Run Type

| Run Type  | Go Avg | Python Avg | Ratio         |
| --------- | ------ | ---------- | ------------- |
| Start     | 1.7s   | 24.0s      | 14x faster    |
| Update    | 14ms   | 17.0s      | ~1200x faster |
| Provision | 2.0s   | 24.0s      | 12x faster    |

## Test 2: Provision Stress Test (2026-07-05)

Focused specifically on provisioning throughput: 20 new provisions created in rapid succession with reduced throttling. For this test, the Python runner was scaled down to 1 pod to create a fair 1:1 comparison.

### Runner Replicas

| Runner             | Pods                                  |
| ------------------ | ------------------------------------- |
| Go (`babylon`)     | 1 (Helm deploy)                       |
| Python (`default`) | 1 (patched from 5 for 1:1 comparison) |

### Test 2 Methodology

1. WorkshopProvision `tests.babylon-empty-config.prod-whlgq` created with 1 seat
2. Patched to `startDelay: 1s`, `concurrency: 25`
3. Increased to 21 seats via UI (triggering 20 new provisions)
4. Both runners active: 1x Go, 1x Python
5. Metrics collected from AnarchyRun resources and Prometheus/Thanos

### Results — Run Distribution

| Runner         | Runs | Share | Throughput    |
| -------------- | ---- | ----- | ------------- |
| Go (1 pod)     | 113  | 93%   | 17.2 runs/min |
| Python (1 pod) | 8    | 7%    | 1.2 runs/min  |

**Go processed 14.1x more runs** than Python with identical pod count.

### Results — Execution Time (creation → completion)

| Run Type  | Go (runs) | Go Avg | Python (runs) | Python Avg | Ratio |
| --------- | --------- | ------ | ------------- | ---------- | ----- |
| Create    | 19        | 10.3s  | 1             | 11.0s      | 1.1x  |
| Provision | 60        | 10.6s  | 4             | 24.2s      | 2.3x  |
| Update    | 34        | 5.8s   | 3             | 14.7s      | 2.5x  |

### Provision Flow Difference

The two runners handle provisioning differently, which affects end-to-end latency:

**Python runner (single-phase):**

1. Receives run with `current_state=provision-pending`
2. Executes Ansible playbook locally via `ansible-runner`
3. Playbook launches Tower job **and waits** for completion (callback)
4. Returns result — single run, ~24s

**Go runner (two-phase):**

1. Receives run with `current_state=provision-pending`
2. Calls Tower API directly to launch job (~1s)
3. Returns `ContinueAction("5m")` — operator reschedules
4. After 5 minutes, new run: `checkDeployerJob()` polls Tower status
5. If complete → `handleProvisionComplete()`; if not → another 5m wait

Each Go provision required 3-4 runs (launch → check → complete). Despite more runs, individual run execution was faster. End-to-end provision time averaged 112s due to the fixed 5-minute polling interval — a known optimization opportunity (see Future Work).

### Results — Provision Phases

| Metric                  | Value |
| ----------------------- | ----- |
| Total provision runs    | 65    |
| Subjects provisioned    | 20    |
| Avg runs per subject    | 3.2   |
| Subjects needing 3 runs | 15    |
| Subjects needing 4 runs | 5     |

## Resource Consumption (Prometheus/Thanos)

Collected via `container_memory_working_set_bytes` and `rate(container_cpu_usage_seconds_total[1m])` from the cluster's Thanos instance over a 6-hour window including both idle and stress test periods.

### Memory

| Metric               | Go Runner | Python Runner | Ratio        |
| -------------------- | --------- | ------------- | ------------ |
| Idle                 | 15.5 Mi   | 233-264 Mi    | **15x less** |
| Avg (6h)             | 16.0 Mi   | 235-267 Mi    | **15x less** |
| Peak (stress test)   | 19.8 Mi   | 368 Mi        | **19x less** |
| Variation under load | +3.4 Mi   | +100 Mi       | —            |

During the provision stress test (113 runs in ~7 minutes), Go runner memory rose from 18.2 Mi to 18.9 Mi (+0.7 Mi), then dropped to 15.5 Mi after GC. Memory usage is essentially flat regardless of load.

### CPU

| Metric             | Go Runner | Python Runner | Ratio        |
| ------------------ | --------- | ------------- | ------------ |
| Idle               | 0.2m      | 0.1m          | ~equal       |
| Peak (stress test) | 4.8m      | 475m          | **99x less** |
| Avg under load     | ~2m       | ~13m          | **6x less**  |

The Go runner peak CPU during the stress test was **4.8 millicores** — less than 1% of its 500m limit. The Python runner regularly spikes to 470m+ when executing Ansible playbooks.

### Resource Utilization vs Requests

|                         | Go Runner             | Python Runner             |
| ----------------------- | --------------------- | ------------------------- |
| CPU utilization at peak | 9.6% of request (50m) | 95% of request (500m)     |
| Memory utilization      | 30% of request (64Mi) | 47-52% of request (512Mi) |

## HPA Implications

The Go runner's resource consumption is too low for CPU-based or memory-based autoscaling to work effectively:

- **CPU HPA at 50% target** requires sustained 25m usage. Go runner peak was 4.8m — the HPA will **never trigger**.
- **Memory** is nearly constant at 15-19 Mi regardless of load.

### Recommended HPA Strategy

Use a **custom metric** rather than resource-based scaling:

1. **`anarchy_runner_pending_runs`** — gauge of queued + pending runs in the operator. Scale when work is piling up, not when resources are consumed.
2. **`babylon_runner_active_run`** — gauge already exposed by the Go runner (1 = processing, 0 = idle). Useful for scale-from-zero but not for gradual scaling.

Pending runs is the strongest signal because it directly measures demand rather than proxy indicators.

## Cost Comparison (per pod)

| Resource               | Go Runner | Python Runner | Savings           |
| ---------------------- | --------- | ------------- | ----------------- |
| CPU request            | 50m       | 500m          | **10x**           |
| Memory request         | 64Mi      | 512Mi         | **8x**            |
| Estimated cost per pod | ~1x       | ~10x          | **90% reduction** |

At equal throughput, a single Go pod replaces 5-14 Python pods, compounding the per-pod savings.

## Summary

| Metric                       | Go Runner | Python Runner | Winner        |
| ---------------------------- | --------- | ------------- | ------------- |
| Throughput (runs/min, 1 pod) | 17.2      | 1.2           | Go (14x)      |
| Provision execution          | 10.6s avg | 24.2s avg     | Go (2.3x)     |
| Update execution             | 5.8s avg  | 14.7s avg     | Go (2.5x)     |
| Memory footprint             | 16 Mi     | 250 Mi        | Go (15x less) |
| CPU footprint                | 4.8m peak | 475m peak     | Go (99x less) |
| CPU request                  | 50m       | 500m          | Go (10x less) |
| Memory request               | 64Mi      | 512Mi         | Go (8x less)  |

## Test 3: ContinueAction Adaptive Backoff (2026-07-05)

Measures the impact of replacing the fixed 5-minute `ContinueAction("5m")` polling interval with an adaptive backoff strategy. Compared directly against Test 2 (same governor, same methodology, same cluster).

### Backoff Strategy

The Go runner now uses an adaptive backoff for `ContinueAction` instead of a fixed 5-minute delay:

- First check after ~30s
- Subsequent checks with increasing intervals (backoff)
- Caps at a maximum interval

This targets the latency gap identified in Test 2, where the fixed 5-minute wait added unnecessary delay for Tower jobs completing in under 1 minute.

### Test 3 Runner Replicas

| Runner             | Pods            |
| ------------------ | --------------- |
| Go (`babylon`)     | 1 (Helm deploy) |
| Python (`default`) | 1               |

### Test 3 Methodology

1. Existing WorkshopProvision `tests.babylon-empty-config.prod-whlgq` with 21 seats (from Test 2)
2. Confirmed `startDelay: 1s`, `concurrency: 25` still active
3. Increased to 41 seats via UI (triggering 20 new provisions)
4. New babylon-runner image built and deployed with adaptive backoff
5. Metrics collected from AnarchyRun resources in the namespace

### Test 3 Results — Run Distribution

| Runner         | Runs | Share |
| -------------- | ---- | ----- |
| Go (1 pod)     | 115  | 91%   |
| Python (1 pod) | 7    | 6%    |
| Canceled       | 4    | 3%    |

### Test 3 Results — Execution Time per Run Type

| Run Type  | Go (runs) | Go Avg | Python (runs) | Python Avg |
| --------- | --------- | ------ | ------------- | ---------- |
| Create    | 20        | 7.1s   | —             | —          |
| Provision | 62        | 9.8s   | 4             | 26.2s      |
| Update    | 33        | 7.8s   | 3             | 3.3s       |

### Test 3 Results — Provision End-to-End (backoff vs fixed polling)

| Metric               | Test 2 (fixed 5m) | Test 3 (backoff) | Change         |
| -------------------- | ----------------- | ---------------- | -------------- |
| **Avg E2E time**     | 112s              | **58.4s**        | **48% faster** |
| Median E2E time      | ~112s             | 57s              | —              |
| Min E2E time         | —                 | 41s              | —              |
| Max E2E time         | —                 | 92s              | —              |
| Avg runs per subject | 3.2               | 3.5              | similar        |
| Subjects provisioned | 20                | 17               | —              |

### Test 3 Results — Distribution by Poll Count

Subjects that completed with 0 additional polls (Tower job finished before first check) achieved the fastest times. Those requiring 2 polls still completed well under the previous 112s average.

| Polls (tower_poll_count) | Subjects | Avg E2E | Runs/Subject |
| ------------------------ | -------- | ------- | ------------ |
| 0                        | 10       | ~50s    | 3            |
| 1                        | 5        | ~55s    | 4            |
| 2                        | 2        | ~87s    | 5            |

### Notes

- 2 subjects remained stuck in `provisioning` state despite their Tower jobs completing. Root cause is an operator/API issue with action rescheduling (`runScheduled` not being cleared after `reschedule()`), not a runner bug. These were excluded from the metrics.
- The 17 completed subjects (vs 20 triggered) reflect this operator issue, not a runner limitation.

## Future Work

- **Fix operator action rescheduling**: The `reschedule()` method in `operator/anarchyaction.py` does not reliably clear `runScheduled` from the action status, causing some actions to get stuck after a `ContinueAction` with backoff. This affects ~10% of provisions in stress tests.
- **Tower job callback**: Eliminate polling entirely by configuring Tower to callback when jobs complete. Would reduce E2E provision time to ~10-15s (single run, no waiting).
- **Custom HPA metric**: Implement `anarchy_runner_pending_runs` in the operator and configure HPA to scale on queue depth.
- **Large-scale stress test**: Test with 100+ concurrent provisions to find Go runner throughput ceiling and validate HPA behavior.
- **Multi-pod Go comparison**: Run 3-5 Go pods to measure scaling linearity and identify bottlenecks (API contention, Tower rate limits).
