#!/usr/bin/env bash
# Verify the supplied package without using the checkout or an external registry.
set -euo pipefail
archive=${1:?Usage: verify-distribution.sh CHART.tgz}
work=$(mktemp -d)
registry="xata-chart-oci-$$"
cleanup() {
  docker rm -f "$registry" >/dev/null 2>&1 || true
  rm -rf "$work"
}
trap cleanup EXIT
cp "$archive" "$work/candidate.tgz"
cd "$work"
version=$(helm show chart candidate.tgz | awk '/^version:/ {print $2}')
helm template xata candidate.tgz --namespace tenant-control --kube-version 1.34.1 \
  --include-crds \
  --set clusters.config.clusters.storageClass=test-csi \
  --set clusters.config.clusters.volumeSnapshotClass=test-snapshot \
  --set 'network.apiServerCIDRs[0]=192.0.2.1/32' > rendered.yaml
python3 - <<'PY'
import yaml
with open('rendered.yaml') as stream:
    resources = [item for item in yaml.safe_load_all(stream) if item]
assert any(item['kind'] == 'CustomResourceDefinition' for item in resources)
assert any(item['kind'] == 'Job' and item['metadata']['name'] == 'projects-setup-1' for item in resources)
print(f'PASS: archive-only rendering from an empty directory; {len(resources)} resources')
PY
docker run -d --name "$registry" -p 127.0.0.1::5000 registry:2.8.3 >/dev/null
binding=$(docker port "$registry" 5000/tcp)
port=${binding##*:}
for attempt in {1..30}; do
  if curl --fail --silent "http://127.0.0.1:$port/v2/" >/dev/null; then break; fi
  sleep 1
done
curl --fail --silent "http://127.0.0.1:$port/v2/" >/dev/null
helm push candidate.tgz "oci://127.0.0.1:$port/chart-tests" --plain-http
mkdir pulled
helm pull "oci://127.0.0.1:$port/chart-tests/xata" --version "$version" --plain-http --destination pulled
cmp candidate.tgz "pulled/xata-$version.tgz"
python3 - <<'PY'
import hashlib, pathlib
print('PASS: OCI round trip preserves exact archive bytes')
print('Archive SHA-256:', hashlib.sha256(pathlib.Path('candidate.tgz').read_bytes()).hexdigest())
PY
