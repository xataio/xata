#!/usr/bin/env bash
set -euo pipefail
root=$(cd "$(dirname "$0")/../.." && pwd)
: "${KUBECONFIG:?Set a disposable cluster kubeconfig}"
: "${XATA_STORAGE_CLASS:?Set the test CSI storage class}"
: "${XATA_SNAPSHOT_CLASS:?Set the test snapshot class}"
profile=${XATA_PROFILE:-single-node}
namespace=${XATA_TEST_NAMESPACE:-xata}
case "$profile" in single-node|multi-node) ;; *) exit 2;; esac
context=$(kubectl config current-context)
case "$context" in *xata-chart-*) ;; *) echo 'Refusing to modify a non-test cluster' >&2; exit 1;; esac
artifacts=${XATA_TEST_ARTIFACTS:-"$root/dist"}
"$root/scripts/helm/package.sh" "$artifacts"
python3 "$root/tests/helm/render_test.py"
chart="$artifacts/xata-$(helm show chart "$root/charts/xata" | awk '/^version:/ {print $2}').tgz"
values=( -f "$root/charts/xata/examples/$profile.yaml"
  --set "metadata.storageClass=$XATA_STORAGE_CLASS"
  --set "objectStorage.storageClass=$XATA_STORAGE_CLASS"
  --set "clusters.config.clusters.storageClass=$XATA_STORAGE_CLASS"
  --set "clusters.config.clusters.volumeSnapshotClass=$XATA_SNAPSHOT_CLASS" )
if [[ "$profile" == multi-node ]]; then
  values+=( --set-string 'clusters.config.clusters.nodeSelector=xata.io/test-worker:true'
    --set-json 'metadata.nodeSelector={"xata.io/test-worker":"true"}' )
fi
helm lint --strict --kube-version 1.34.1 "$chart" "${values[@]}"
helm upgrade --install xata "$chart" -n "$namespace" --create-namespace \
  "${values[@]}" --wait --wait-for-jobs --timeout 15m
helm test xata -n "$namespace" --timeout 5m
export XATA_TEST_REPLICAS=0
if [[ "$profile" == multi-node ]]; then export XATA_TEST_REPLICAS=2; fi
python3 -u "$root/tests/helm/api_smoke.py"
