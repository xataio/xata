#!/usr/bin/env bash
set -euo pipefail
: "${KUBECONFIG:?Set the disposable cluster kubeconfig}"
root=$(cd "$(dirname "$0")/../.." && pwd)
case "$(kubectl config current-context)" in *xata-chart-*) ;; *) exit 2;; esac
node=$(kubectl get nodes -l node-role.kubernetes.io/control-plane -o jsonpath='{.items[0].metadata.name}')
kubectl label node "$node" xata.io/test-storage=true --overwrite
kubectl apply -f "$root/tests/helm/fixtures/nfs-server.yaml"
kubectl -n xata-test-storage rollout status deployment/nfs-server --timeout=300s
snapshotter=true
if kubectl get crd volumesnapshots.snapshot.storage.k8s.io >/dev/null 2>&1; then snapshotter=false; fi
# Preserve numeric UID/GID during archive restore; PostgreSQL must own PGDATA.
helm upgrade --install xata-test-nfs \
  https://raw.githubusercontent.com/kubernetes-csi/csi-driver-nfs/v4.12.1/charts/v4.12.1/csi-driver-nfs-4.12.1.tgz \
  -n xata-test-storage --set "externalSnapshotter.enabled=$snapshotter" \
  --set "externalSnapshotter.customResourceDefinitions.enabled=$snapshotter" \
  --set controller.useTarCommandInSnapshot=true \
  --wait --timeout 5m
server=$(kubectl -n xata-test-storage get svc nfs-server -o jsonpath='{.spec.clusterIP}')
cat <<YAML | kubectl apply -f -
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: xata-test-nfs
provisioner: nfs.csi.k8s.io
parameters:
  server: "$server"
  share: /
  mountPermissions: '0777'
reclaimPolicy: Retain
volumeBindingMode: Immediate
mountOptions: [nfsvers=4.1, hard]
---
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshotClass
metadata:
  name: xata-test-nfs
driver: nfs.csi.k8s.io
deletionPolicy: Delete
YAML
