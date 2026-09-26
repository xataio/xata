#!/usr/bin/env bash
# Run on the disposable GCP host; retrieve evidence before deleting the VM.
set -euo pipefail
root=$(cd "$(dirname "$0")/../.." && pwd)
export PATH="/opt/xata-test-tools/bin:$PATH"
export XATA_MATRIX_ARTIFACTS=${XATA_MATRIX_ARTIFACTS:-"$root/dist/verification/gcp"}
mkdir -p "$XATA_MATRIX_ARTIFACTS"
trap 'result=$?; printf "%s\n" "$result" > "$XATA_MATRIX_ARTIFACTS/result-code.txt"' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
cd "$root"
{
  date -u
  uname -m
  docker info --format 'Memory bytes: {{.MemTotal}}; CPUs: {{.NCPU}}'
  helm version --short
  kubectl version --client
  k3d version
  kind version
} > "$XATA_MATRIX_ARTIFACTS/host.txt"
python3 tests/helm/cleanup_test.py
# Multi-node coverage first; repeat singles after the schema-setup change.
if (( $# == 0 )); then set -- k3d-multi kind-multi k3d-single kind-single; fi
scripts/helm/test-matrix.sh "$@"
export XATA_EXISTING_ARTIFACTS="$XATA_MATRIX_ARTIFACTS/existing-operators"
scripts/helm/test-existing-operators.sh > "$XATA_MATRIX_ARTIFACTS/existing-operators.log" 2>&1
echo 'PASS: requested matrix and separately managed operators'
