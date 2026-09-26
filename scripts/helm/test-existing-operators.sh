#!/usr/bin/env bash
# Existing-operator compatibility on a separate disposable single-node cluster.
set -euo pipefail
root=$(cd "$(dirname "$0")/../.." && pwd)
output=${XATA_EXISTING_ARTIFACTS:-"$root/dist/verification/existing-operators"}
name=${XATA_EXISTING_CLUSTER_NAME:-xata-chart-existing-operators}
case "$name" in xata-chart-*) ;; *) exit 2;; esac
mkdir -p "$output"
export KUBECONFIG="$output/kubeconfig"
export XATA_TEST_NAMESPACE=tenant-control XATA_TEST_STATE="$output/api-state.json"
kind create cluster --name "$name" --image kindest/node:v1.34.0 \
  --config "$root/tests/helm/clusters/kind-single.yaml" --kubeconfig "$KUBECONFIG" --wait 180s
chmod 600 "$KUBECONFIG"
"$root/scripts/helm/install-nfs-test-storage.sh"
export XATA_STORAGE_CLASS=xata-test-nfs XATA_SNAPSHOT_CLASS=xata-test-nfs
python3 -u "$root/tests/helm/storage_test.py"
"$root/scripts/helm/package.sh" "$output"
version=$(helm show chart "$root/charts/xata" | awk '/^version:/ {print $2}')
chart="$output/xata-$version.tgz"
bundle=$(mktemp -d)
trap 'rm -rf "$bundle"' EXIT
tar xzf "$chart" -C "$bundle"
kubectl create namespace "$XATA_TEST_NAMESPACE"
kubectl apply --server-side -f "$bundle/xata/crds"
kubectl apply --server-side -f "$bundle/xata/charts/keycloak/crds"
helm install external-cert-manager "$bundle/xata/charts/cert-manager" -n "$XATA_TEST_NAMESPACE" \
  --set crds.enabled=false --set global.leaderElection.namespace=kube-system --wait --timeout 5m
helm install external-cnpg "$bundle/xata/charts/cloudnative-pg" -n "$XATA_TEST_NAMESPACE" \
  --set crds.create=false --set nameOverride=cloudnative-pg \
  --set fullnameOverride=cnpg-controller-manager --wait --timeout 5m
helm install external-envoy "$bundle/xata/charts/gateway-helm" -n "$XATA_TEST_NAMESPACE" --wait --timeout 5m
helm template external-keycloak "$bundle/xata/charts/keycloak" -n "$XATA_TEST_NAMESPACE" \
  --show-only templates/operator.yaml | kubectl apply -f -
kubectl -n "$XATA_TEST_NAMESPACE" rollout status deployment/keycloak-operator --timeout=300s
helm install xata "$chart" -n "$XATA_TEST_NAMESPACE" \
  --set cnpg.enabled=false --set cert-manager.enabled=false --set envoy.enabled=false \
  --set keycloak.operator.enabled=false \
  --set metadata.storageClass=xata-test-nfs --set objectStorage.storageClass=xata-test-nfs \
  --set clusters.config.clusters.storageClass=xata-test-nfs \
  --set clusters.config.clusters.volumeSnapshotClass=xata-test-nfs \
  --wait --wait-for-jobs --timeout 15m
helm test xata -n "$XATA_TEST_NAMESPACE" --timeout 5m
env XATA_TEST_VERIFY_ONLY=false XATA_TEST_EXTENDED=true XATA_TEST_RECOVERY=false \
  XATA_TEST_UPGRADE_CHART='' python3 -u "$root/tests/helm/api_smoke.py"
test "$(kubectl -n "$XATA_TEST_NAMESPACE" get deploy cnpg-controller-manager -o 'jsonpath={.metadata.annotations.meta\.helm\.sh/release-name}')" = external-cnpg
echo 'PASS: packaged Xata installs with separately managed pinned operators; API, SQL and ownership verified'
if [[ ${XATA_KEEP_TEST_CLUSTERS:-false} != true ]]; then
  "$root/scripts/helm/delete-test-cluster.sh" kind "$name"
fi
