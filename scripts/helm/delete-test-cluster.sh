#!/usr/bin/env bash
# Stop NFS clients before their server; killing both together can block node exit.
set -euo pipefail
distro=${1:?Usage: delete-test-cluster.sh kind|k3d xata-chart-NAME}
name=${2:?Missing test cluster name}
: "${KUBECONFIG:?Set the disposable cluster kubeconfig}"
case "$distro/$name" in kind/xata-chart-*|k3d/xata-chart-*) ;; *) exit 2;; esac
case "$(kubectl config current-context)" in *"$name"*) ;; *) echo 'Wrong test context' >&2; exit 2;; esac
# An API failure is not evidence that NFS is absent. Abort instead of killing
# the server while clients may still have hard-mounted volumes.
nfs_server=$(kubectl -n xata-test-storage get deploy nfs-server --ignore-not-found -o name)
if [[ -n "$nfs_server" ]]; then
  for namespace in "${XATA_TEST_NAMESPACE:-tenant-control}" xata-clusters xata-storage-verification; do
    kubectl -n "$namespace" delete deployments,statefulsets,jobs,cronjobs --all --ignore-not-found --wait=true --timeout=180s
    kubectl -n "$namespace" delete pods --all --ignore-not-found --grace-period=10 --wait=true --timeout=180s
  done
  nodes=$(kubectl get nodes -o 'jsonpath={.items[*].metadata.name}')
  for node in $nodes; do
    case "$node" in "$name"-*|"k3d-$name"-*) ;; *) echo 'Unexpected node name' >&2; exit 2;; esac
    for attempt in {1..60}; do
      mounts=$(docker exec "$node" cat /proc/self/mountinfo)
      if [[ "$mounts" != *" - nfs"* ]]; then break; fi
      if [[ "$attempt" == 60 ]]; then echo "NFS mounts remain on $node; keeping the server alive" >&2; exit 1; fi
      sleep 2
    done
  done
fi
if [[ "$distro" == k3d ]]; then k3d cluster delete "$name"; else kind delete cluster --name "$name"; fi
