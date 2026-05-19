#!/bin/bash
# Run the Go babylon-runner locally against a port-forwarded anarchy API.
#
# What it does:
#   1. Ensures at least one Python runner pod exists (scales up if needed)
#   2. Reads the pod name + token from it
#   3. Starts the Go runner impersonating that pod
#
# Both the real pod and the Go runner will poll. The Go runner may need
# a few cycles to win the race. To guarantee the Go runner handles all
# runs, scale the pool to exactly 1 before running this script:
#   oc patch anarchyrunner default -n <ns> --type merge \
#     -p '{"spec":{"minReplicas":1,"maxReplicas":1}}'
#
# Prerequisites:
#   - oc login to your dev cluster
#   - oc port-forward svc/anarchy -n <namespace> 5000:5000 (in another terminal)

set -euo pipefail

NAMESPACE="${ANARCHY_NAMESPACE:-babylon-anarchy-test}"
RUNNER="${RUNNER_NAME:-default}"

# --- Get a runner pod to read identity from ---

POD=$(oc get pods -n "$NAMESPACE" \
  -l "anarchy.gpte.redhat.com/runner=$RUNNER" \
  --field-selector=status.phase=Running \
  -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)

if [[ -z "$POD" ]]; then
  echo "No running runner pod found — scaling up one..."
  oc patch anarchyrunner "$RUNNER" -n "$NAMESPACE" --type merge \
    -p '{"spec":{"minReplicas":1,"maxReplicas":1}}'
  echo "Waiting for runner pod to start..."
  oc wait --for=condition=Ready pod \
    -l "anarchy.gpte.redhat.com/runner=$RUNNER" \
    -n "$NAMESPACE" --timeout=120s
  POD=$(oc get pods -n "$NAMESPACE" \
    -l "anarchy.gpte.redhat.com/runner=$RUNNER" \
    --field-selector=status.phase=Running \
    -o jsonpath='{.items[0].metadata.name}')
fi

if [[ -z "$POD" ]]; then
  echo "ERROR: could not find or create a runner pod" >&2
  exit 1
fi

# --- Extract the token ---

TOKEN=$(oc get pod "$POD" -n "$NAMESPACE" \
  -o jsonpath='{.spec.containers[?(@.name=="runner")].env[?(@.name=="RUNNER_TOKEN")].value}')

if [[ -z "$TOKEN" ]]; then
  echo "ERROR: could not extract RUNNER_TOKEN from pod $POD" >&2
  exit 1
fi

# --- Run ---

export ANARCHY_URL="${ANARCHY_URL:-http://localhost:5000}"
export RUNNER_NAME="$RUNNER"
export RUNNER_TOKEN="$TOKEN"
export HOSTNAME="$POD"

echo "ANARCHY_URL=$ANARCHY_URL"
echo "RUNNER_NAME=$RUNNER_NAME"
echo "RUNNER_TOKEN=${RUNNER_TOKEN:0:8}..."
echo "HOSTNAME=$HOSTNAME"
echo ""
echo "Building..."
go build -o "${TMPDIR:-/tmp}/babylon-runner" . || exit 1
echo "Starting babylon-runner... (Ctrl+C to stop)"
exec "${TMPDIR:-/tmp}/babylon-runner"
