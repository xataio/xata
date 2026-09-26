#!/usr/bin/env bash
# Run one isolated environment at a time; all names are reserved for these tests.
set -euo pipefail
root=$(cd "$(dirname "$0")/../.." && pwd)
artifacts=${XATA_MATRIX_ARTIFACTS:-"$root/dist/verification"}
mkdir -p "$artifacts"
version=$(helm show chart "$root/charts/xata" | awk '/^version:/ {print $2}')
environments=("${@}")
if (( ${#environments[@]} == 0 )); then environments=(k3d-single kind-single k3d-multi kind-multi); fi
for environment in "${environments[@]}"; do
  case "$environment" in k3d-single|kind-single|k3d-multi|kind-multi) ;; *) exit 2;; esac
  distro=${environment%-*}; topology=${environment##*-}
  output="$artifacts/$environment"
  mkdir -p "$output"
  export KUBECONFIG="$output/kubeconfig"
  export XATA_TEST_ARTIFACTS="$output" XATA_TEST_STATE="$output/api-state.json"
  export XATA_TEST_NAMESPACE=tenant-control
  export XATA_TEST_EXTENDED=true XATA_TEST_RECOVERY=true
  export XATA_PROFILE="$topology-node"
  "$root/scripts/helm/create-test-cluster.sh" "$distro" "$topology" "$KUBECONFIG" >"$output/cluster.log" 2>&1
  if [[ "$topology" == single ]]; then
    "$root/scripts/helm/install-test-storage.sh" >"$output/storage-install.log" 2>&1
    export XATA_STORAGE_CLASS=csi-hostpath-sc XATA_SNAPSHOT_CLASS=csi-hostpath-snapclass
  else
    "$root/scripts/helm/install-nfs-test-storage.sh" >"$output/storage-install.log" 2>&1
    export XATA_STORAGE_CLASS=xata-test-nfs XATA_SNAPSHOT_CLASS=xata-test-nfs
  fi
  python3 -u "$root/tests/helm/storage_test.py" >"$output/storage-test.log" 2>&1
  "$root/scripts/helm/verify.sh" >"$output/install-test.log" 2>&1
  python3 -u "$root/tests/helm/data_protection_test.py" >"$output/data-protection.log" 2>&1
  if [[ "$topology" == multi ]]; then
    python3 -u "$root/tests/helm/failover_test.py" >"$output/failover.log" 2>&1
  fi
  export XATA_TEST_VERIFY_ONLY=true XATA_TEST_EXTENDED=false XATA_TEST_RECOVERY=false
  export XATA_TEST_UPGRADE_CHART="$output/xata-$version.tgz"
  python3 -u "$root/tests/helm/api_smoke.py" >"$output/idempotent-upgrade.log" 2>&1
  unset XATA_TEST_UPGRADE_CHART
  export XATA_TEST_CHART="$output/xata-$version.tgz"
  python3 -u "$root/tests/helm/retention_test.py" >"$output/retention.log" 2>&1
  python3 -u "$root/tests/helm/lifecycle_test.py" >"$output/lifecycle.log" 2>&1
  helm test xata -n "$XATA_TEST_NAMESPACE" --timeout 5m >"$output/helm-test.log" 2>&1
  kubectl get nodes -o wide >"$output/nodes.txt"
  kubectl get pods -A >"$output/pods.txt"
  echo "PASS: $environment install, storage, APIs, branching, restore, recovery, upgrade, retention, HTTPS and configuration rollback"
  if [[ ${XATA_KEEP_TEST_CLUSTERS:-false} != true ]]; then
    "$root/scripts/helm/delete-test-cluster.sh" "$distro" "xata-chart-$environment"
  fi
  unset XATA_TEST_VERIFY_ONLY
done
