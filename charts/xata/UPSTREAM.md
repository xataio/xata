# Provenance

Xata source and application images: commit `98ced3cc9d5038c78c7f3a00ab7d22da847a4fdd` from https://github.com/xataio/xata.
Application image digests are in `values.yaml`; the matching scale-to-zero sidecar digest is in `templates/plugin-scale-to-zero.yaml`.

The archive includes seven local component charts and three pinned external charts. `Chart.lock` records their versions. Remote dependency archives include their upstream licenses.

| Asset | Pinned source |
|---|---|
| Xata CNPG fork and CRDs | `oci://ghcr.io/xataio/xata-cnpg/charts/cloudnative-pg:0.0.0-g139a814` |
| cert-manager and CRDs | `https://charts.jetstack.io`, version `v1.18.2` |
| Envoy Gateway and Gateway API CRDs | `oci://docker.io/envoyproxy/gateway-helm:v1.2.4` |
| Barman plugin and ObjectStore CRD | https://github.com/cloudnative-pg/plugin-barman-cloud/releases/tag/v0.10.0 |
| Scale-to-zero plugin | https://github.com/xataio/cnpg-i-scale-to-zero/releases/tag/v0.1.8 |
| Keycloak and Xata CRDs | Corresponding component charts at the Xata source commit above |
| Realm configuration | `kustomize/overlays/local/auth/keycloak/realm.json` from the source checkout |

Plugin manifests have release-namespace substitution, deferred certificate creation, and the local deployment's Xata Branch RBAC and sidecar image override. CRD API approval annotations are preserved. Refresh with `scripts/helm/vendor-assets.py` after `helm dependency build charts/xata`, review the diff, and rerun the full matrix.

The Xata source and plugin manifests use Apache-2.0. The included `LICENSE` applies to the Xata chart; dependency licenses remain with their archives. RustFS is a separate optional container dependency, licensed by its upstream project; its source and licensing are at https://github.com/rustfs/rustfs. Container images are pulled at installation time, not redistributed inside the chart.
