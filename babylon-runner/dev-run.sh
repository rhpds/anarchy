#!/bin/bash
# Run the Go babylon-runner locally against a port-forwarded anarchy API.
#
# What it does:
#   1. Starts port-forward to the anarchy service
#   2. Creates a lightweight dev pod (sleep infinity) with the right labels
#      and a known token so the anarchy API recognizes it as a real runner.
#   3. Builds and runs the Go runner using that pod identity.
#   4. Cleans up everything on exit.
#
# Prerequisites:
#   - oc login to your dev cluster

set -euo pipefail

NAMESPACE="${ANARCHY_NAMESPACE:-babylon-anarchy-test}"
RUNNER="${RUNNER_NAME:-default}"
DEV_POD="babylon-runner-godev"
DEV_TOKEN=$(python3 -c "import random, string; print(''.join(random.choices(string.ascii_lowercase + string.digits, k=24)))")
PORT_FORWARD_PID=""

# --- Save original runner replicas ---

ORIG_MIN=$(oc get anarchyrunner "$RUNNER" -n "$NAMESPACE" -o jsonpath='{.spec.minReplicas}' 2>/dev/null || echo "1")
ORIG_MAX=$(oc get anarchyrunner "$RUNNER" -n "$NAMESPACE" -o jsonpath='{.spec.maxReplicas}' 2>/dev/null || echo "3")

# --- Cleanup on exit ---

cleanup() {
  echo ""
  echo "Cleaning up..."
  if [[ -n "$PORT_FORWARD_PID" ]]; then
    kill "$PORT_FORWARD_PID" 2>/dev/null || true
    wait "$PORT_FORWARD_PID" 2>/dev/null || true
  fi
  oc delete pod "$DEV_POD" -n "$NAMESPACE" --ignore-not-found --wait=false 2>/dev/null || true
  echo "Restoring runner replicas (min=$ORIG_MIN, max=$ORIG_MAX)..."
  oc patch anarchyrunner "$RUNNER" -n "$NAMESPACE" --type merge \
    -p "{\"spec\":{\"minReplicas\":$ORIG_MIN,\"maxReplicas\":$ORIG_MAX}}" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

# --- Start port-forward ---

echo "Starting port-forward to svc/anarchy in $NAMESPACE..."
oc port-forward svc/anarchy -n "$NAMESPACE" 5000:5000 &
PORT_FORWARD_PID=$!
sleep 2

# Check it's alive.
if ! kill -0 "$PORT_FORWARD_PID" 2>/dev/null; then
  echo "ERROR: port-forward failed to start" >&2
  exit 1
fi

# --- Scale down Python runners ---

echo "Scaling down Python runners..."
oc patch anarchyrunner "$RUNNER" -n "$NAMESPACE" --type merge \
  -p '{"spec":{"minReplicas":0,"maxReplicas":0}}'
# Delete existing runner pods immediately (operator is slow to reconcile).
oc delete pods -n "$NAMESPACE" -l "anarchy.gpte.redhat.com/runner=$RUNNER" --wait=false 2>/dev/null || true

# --- Create a dev runner pod ---

# Delete any leftover dev pod from a previous run and wait for it to be gone.
oc delete pod "$DEV_POD" -n "$NAMESPACE" --ignore-not-found --wait=true 2>/dev/null || true

echo "Creating dev runner pod $DEV_POD..."
cat <<EOF | oc create -n "$NAMESPACE" -f -
apiVersion: v1
kind: Pod
metadata:
  name: ${DEV_POD}
  labels:
    anarchy.gpte.redhat.com/runner: ${RUNNER}
spec:
  containers:
  - name: runner
    image: registry.access.redhat.com/ubi9/ubi-minimal:latest
    command: ["sleep", "infinity"]
    env:
    - name: RUNNER_TOKEN
      value: "${DEV_TOKEN}"
    - name: RUNNER_NAME
      value: "${RUNNER}"
EOF

echo "Waiting for dev pod to be ready..."
oc wait --for=condition=Ready pod/"$DEV_POD" -n "$NAMESPACE" --timeout=60s

# --- Run ---

export ANARCHY_URL="${ANARCHY_URL:-http://localhost:5000}"
export ANARCHY_NAMESPACE="$NAMESPACE"
export RUNNER_NAME="$RUNNER"
export RUNNER_TOKEN="$DEV_TOKEN"
export HOSTNAME="$DEV_POD"

echo ""
echo "ANARCHY_URL=$ANARCHY_URL"
echo "RUNNER_NAME=$RUNNER_NAME"
echo "RUNNER_TOKEN=${RUNNER_TOKEN:0:8}..."
echo "HOSTNAME=$HOSTNAME"
echo ""
echo "Building..."
go build -o "${TMPDIR:-/tmp}/babylon-runner" . || exit 1
echo "Starting babylon-runner... (Ctrl+C to stop)"
# Can't exec here — need the trap to fire for cleanup.
"${TMPDIR:-/tmp}/babylon-runner"
