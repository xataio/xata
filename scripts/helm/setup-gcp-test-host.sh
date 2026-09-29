#!/usr/bin/env bash
# Run as root on a disposable Ubuntu 24.04 VM reserved for chart verification.
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive
# Keep package-maintenance restarts out of this short-lived test VM's matrix.
systemctl disable --now apt-daily.timer apt-daily-upgrade.timer
systemctl mask apt-daily.service apt-daily-upgrade.service
export NEEDRESTART_MODE=l
apt-get update
apt-get install -y ca-certificates curl docker.io git jq openssl postgresql-client python3-venv rsync
systemctl enable --now docker
sysctl -w fs.inotify.max_user_instances=1024 fs.inotify.max_user_watches=1048576
# Ganesha 4.0.8 in the test fixture rejects Linux 6.19+ directory delegations.
# https://github.com/nfs-ganesha/nfs-ganesha/issues/1385
modprobe nfsv4
if [[ -e /sys/module/nfsv4/parameters/directory_delegations ]]; then
  printf 'N\n' > /sys/module/nfsv4/parameters/directory_delegations
  printf 'options nfsv4 directory_delegations=N\n' > /etc/modprobe.d/xata-test-nfs.conf
fi
python3 -m venv /opt/xata-test-tools
/opt/xata-test-tools/bin/pip install PyYAML==6.0.3
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
cd "$work"
curl --fail --location --retry 5 -o helm.tar.gz https://get.helm.sh/helm-v4.1.1-linux-amd64.tar.gz
curl --fail --location --retry 5 -o helm.sha256 https://get.helm.sh/helm-v4.1.1-linux-amd64.tar.gz.sha256
printf '%s  helm.tar.gz\n' "$(cat helm.sha256)" | sha256sum -c -
tar -xzf helm.tar.gz
install -m 0755 linux-amd64/helm /usr/local/bin/helm
curl --fail --location --retry 5 -o kubectl https://dl.k8s.io/release/v1.34.1/bin/linux/amd64/kubectl
curl --fail --location --retry 5 -o kubectl.sha256 https://dl.k8s.io/release/v1.34.1/bin/linux/amd64/kubectl.sha256
printf '%s  kubectl\n' "$(cat kubectl.sha256)" | sha256sum -c -
install -m 0755 kubectl /usr/local/bin/kubectl
curl --fail --location --retry 5 -o k3d https://github.com/k3d-io/k3d/releases/download/v5.9.0/k3d-linux-amd64
curl --fail --location --retry 5 -o k3d-checksums.txt https://github.com/k3d-io/k3d/releases/download/v5.9.0/checksums.txt
mv k3d k3d-linux-amd64
awk '$2 == "_dist/k3d-linux-amd64" { print $1 "  k3d-linux-amd64" }' k3d-checksums.txt | sha256sum -c -
install -m 0755 k3d-linux-amd64 /usr/local/bin/k3d
curl --fail --location --retry 5 -o kind-linux-amd64 https://github.com/kubernetes-sigs/kind/releases/download/v0.32.0/kind-linux-amd64
curl --fail --location --retry 5 -o kind.sha256 https://github.com/kubernetes-sigs/kind/releases/download/v0.32.0/kind-linux-amd64.sha256sum
sha256sum -c kind.sha256
install -m 0755 kind-linux-amd64 /usr/local/bin/kind
docker info --format 'Docker memory: {{.MemTotal}}; CPUs: {{.NCPU}}'
helm version --short
kubectl version --client
k3d version
kind version
