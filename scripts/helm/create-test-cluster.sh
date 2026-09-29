#!/usr/bin/env bash
set -euo pipefail
root=$(cd "$(dirname "$0")/../.." && pwd)
distro=${1:?Usage: create-test-cluster.sh k3d|kind single|multi /absolute/kubeconfig}
topology=${2:?Missing topology}
kubeconfig=${3:?Missing dedicated kubeconfig}
case "$distro/$topology" in k3d/single|k3d/multi|kind/single|kind/multi) ;; *) exit 2;; esac
case "$kubeconfig" in /*) ;; *) echo 'Use an absolute kubeconfig path' >&2; exit 2;; esac
name="xata-chart-$distro-$topology"
if [[ "$topology" == multi ]]; then
  memory=$(docker info --format '{{.MemTotal}}')
  if (( memory < 11500000000 )); then
    echo 'BLOCKED: six-node tests require Docker with at least 12 GB allocated (16 GB recommended). Existing workloads are not stopped.' >&2
    exit 3
  fi
fi
mkdir -p "$(dirname "$kubeconfig")"
if [[ "$distro" == k3d ]]; then
  servers=1; agents=0
  if [[ "$topology" == multi ]]; then servers=3; agents=3; fi
  k3d cluster create "$name" --image rancher/k3s:v1.34.1-k3s1 \
    --servers "$servers" --agents "$agents" \
    --k3s-arg '--disable=traefik@server:*' \
    --kubeconfig-update-default=false --kubeconfig-switch-context=false --wait
  k3d kubeconfig get "$name" > "$kubeconfig"
else
  kind create cluster --name "$name" --image kindest/node:v1.34.0 \
    --config "$root/tests/helm/clusters/kind-$topology.yaml" --kubeconfig "$kubeconfig" --wait 180s
fi
chmod 600 "$kubeconfig"
kubectl --kubeconfig "$kubeconfig" wait --for=condition=Ready nodes --all --timeout=180s
if [[ "$topology" == multi ]]; then
  kubectl --kubeconfig "$kubeconfig" label nodes -l '!node-role.kubernetes.io/control-plane' xata.io/test-worker=true --overwrite
fi
