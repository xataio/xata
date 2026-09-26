#!/usr/bin/env bash
set -euo pipefail
root=$(cd "$(dirname "$0")/../.." && pwd)
destination=${1:-"$root/dist"}
mkdir -p "$destination"
# Chart.lock pins remote dependencies. Build once when refreshing dependencies.
if [ ! -f "$root/charts/xata/charts/cloudnative-pg-0.0.0-g139a814.tgz" ]; then
  helm dependency build "$root/charts/xata"
fi
for component in auth projects clusters branch-operator gateway keycloak api-gateway; do
  helm package "$root/charts/$component" --destination "$root/charts/xata/charts" >/dev/null
done
cp "$root/HELM-CHART-VERIFICATION.md" "$root/charts/xata/VERIFICATION.md"
helm package "$root/charts/xata" --destination "$destination"
python3 - "$destination" <<'PY'
import hashlib, pathlib, sys
for archive in pathlib.Path(sys.argv[1]).glob('xata-*.tgz'):
    archive.with_suffix('.tgz.sha256').write_text(hashlib.sha256(archive.read_bytes()).hexdigest()+'  '+archive.name+'\n')
PY
