#!/usr/bin/env bash
set -euo pipefail
: "${KUBECONFIG:?Set the disposable cluster kubeconfig}"
root=$(cd "$(dirname "$0")/../.." && pwd)
context=$(kubectl config current-context)
case "$context" in *xata-chart-*) ;; *) echo 'Refusing to change a non-test cluster' >&2; exit 1;; esac
if [[ $(kubectl get nodes -o name | wc -l) -ne 1 ]]; then
  echo 'HostPath fixture supports single-node only; qualify a shared CSI backend for multi-node tests.' >&2
  exit 3
fi
snap=https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/v8.2.0
for resource in volumesnapshotclasses volumesnapshotcontents volumesnapshots; do
  kubectl apply --server-side -f "$snap/client/config/crd/snapshot.storage.k8s.io_${resource}.yaml"
done
kubectl apply -f "$snap/deploy/kubernetes/snapshot-controller/rbac-snapshot-controller.yaml"
kubectl apply -f "$snap/deploy/kubernetes/snapshot-controller/setup-snapshot-controller.yaml"
fixture=$(mktemp -d)
trap 'rm -rf "$fixture"' EXIT
git clone --quiet https://github.com/kubernetes-csi/csi-driver-host-path.git "$fixture/driver"
git -C "$fixture/driver" checkout --quiet 7a59b471bd5786ee3fe46b5829f09490a82968ca
bash "$fixture/driver/deploy/kubernetes-1.34/deploy.sh"
kubectl apply -f "$root/tests/helm/fixtures/hostpath-storage.yaml"
