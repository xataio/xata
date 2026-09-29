# Xata chart verification

Date: 2026-09-26. Preview: `0.1.0-dev.2`. Xata source: `98ced3cc9d5038c78c7f3a00ab7d22da847a4fdd`.

**Validation complete: all four environments and the separately managed operator check passed.** Both six-node and both single-node environments passed every required gate. This is a tested preview, qualified for the configurations below.

## Current compatibility matrix

The cloud runner is Ubuntu 24.04 AMD64 on a GCP Spot VM with 8 vCPUs and 64 GB RAM. Kubernetes nodes are containers on that VM, so these tests establish behavior under containerized node faults, not independent physical failure domains. Every cell uses an isolated kubeconfig and installs the packaged archive into `tenant-control`. Multi-node tests use NFS CSI 4.12.1; single-node tests use CSI HostPath.

| Environment | Topology | Current candidate |
|---|---|---|
| k3d / k3s v1.34.1-k3s1 | 1 server | PASS: every required gate, including complete node restart |
| kind / Kubernetes v1.34.0 | 1 control-plane | PASS: every required gate, including complete node restart |
| k3d / k3s v1.34.1-k3s1 | 3 servers + 3 workers | PASS: every required gate; resumed after host-maintenance interruption |
| kind / Kubernetes v1.34.0 | 3 control-plane + 3 workers | PASS: every required gate |

## Required gates and evidence

Each cell must pass:

- CSI provisioning, snapshot/restore contents, UID/GID and permissions. Multi-node storage is restored on another worker.
- Packaged installation, metadata SQL through `helm test`, authenticated API, unauthorized-request rejection, project and database creation.
- TLS SQL with 1,000 rows and checksum `9d8d0c4fd8532ab746d46dd9260e52c4`.
- Hibernation/wakeup and database pod replacement. Single-node cells also restart the whole node.
- Snapshot branching, object-store backup restore and independent child writes.
- PITR including a transaction before the selected timestamp and excluding a later one; SQL during a backup-store outage and WAL archiving resumption.
- Upgrade with stable credentials and data; uninstall/reinstall retaining metadata, branch data and seven tracked Secrets, including Keycloak admin identity.
- API HTTPS with certificate/hostname verification, configuration-only TLS rollback and a final Helm SQL test.

Multi-node cells additionally remove the primary database worker and a control-plane member. Before the worker fault, both asynchronous replicas must have replayed the fixture. This tests recovery of acknowledged, replayed data; it is not a zero-RPO guarantee.

SQL upgrade probes use `kubectl port-forward`. Probe failures cannot by themselves establish externally observed load-balancer downtime. Configuration rollback does not establish rollback of schema migrations or PostgreSQL major versions.

Current kind multi-node measurements: worker promotion in 81.9 seconds with the replayed fixture retained; 59 successful / 1 failed SQL probes over 65.0 seconds during idempotent upgrade. Retention, certificate/hostname verification, configuration rollback and the final Helm SQL test passed.

Current k3d multi-node worker promotion took 74.8 seconds with the replayed fixture retained; its idempotent upgrade recorded 74 successful / 1 failed SQL probes over 81.2 seconds. All retention, HTTPS, rollback and final Helm gates passed. Both multi-node clusters were deleted after their results were saved. Each single-node upgrade recorded 48 successful / 1 failed SQL probes over 53.0 seconds; both whole-node restart checks passed and both clusters were deleted.

Logs and tested archives are under `dist/verification/gcp/`. Failed attempts are retained separately. Credential-bearing values, API state, kubeconfigs and private SSH keys are excluded from downloaded evidence. The provided CI workflow has not been run successfully in GitHub Actions.

The current candidate also passed a separate fresh kind/NFS installation with independently managed CNPG, cert-manager, Envoy and Keycloak operators. Storage qualification, API authentication, SQL, snapshot branching, backup restore and isolation passed; the CNPG Deployment retained its `external-cnpg` Helm owner. The integration cluster was deleted and the cloud runner exited successfully with status 0. Evidence: `dist/verification/gcp/existing-operators.log` and `dist/verification/gcp/result-code.txt`.

## Corrections verified during development

- Auth and projects schema setup now runs in one Job per service per release revision; replicas wait for completion. This avoids concurrent setup races. Jobs remain available for replacement pods.
- Multi-node scale-to-zero uses two ready replicas on distinct nodes. Losing its former singleton had blocked CNPG database failover. The corrected candidate passed an existing-release upgrade and worker recovery in 55.1 seconds, then a fresh six-node k3d recovery in 80.9 seconds. Barman remains a singleton because the pinned server is leader elected; its eviction delay is shortened for rescheduling.
- The API test now recreates an exited port-forward. A forced transport termination passed before resuming the earlier k3d upgrade, retention and HTTPS gates. The original failing log is preserved; it did not establish an application failure.
- Bootstrap now applies plugin certificates and waits for plugin Deployments before creating metadata resources. The preceding kind run submitted metadata before Barman was reachable, accumulated reconciliation delay and exceeded the Helm wait deadline while Keycloak finished startup. The corrected fresh kind install completed in about 6 minutes 20 seconds and passed all subsequent matrix gates.
- NFS snapshots preserve ownership and permissions using `controller.useTarCommandInSnapshot=true`. The default archive mode had restored PostgreSQL files as UID 0.
- The test host disables NFSv4 directory delegations to work around [Ganesha issue 1385](https://github.com/nfs-ganesha/nfs-ganesha/issues/1385) on its Linux 7.0 kernel. The recorded failure was `GET_DIR_DELEGATION` returning `NFS4ERR_OP_ILLEGAL`, followed by recursive ownership changes failing with `EREMOTEIO`. The corrected reproduction preserved UID/GID 26 and modes 700/600. Numeric owner IDs also avoid container identity-domain mismatches.
- Cluster cleanup fails closed on Kubernetes lookup errors or remaining NFS mounts. Three negative cases pass. Only the disposable test resources are targeted.
- Ubuntu automatic package maintenance coincided with systemd restarting the long-running test service at 06:56:47 UTC. This began a duplicate kind run after its matrix had already passed. The duplicate cluster was removed and the completed kind evidence restored from a local checkpoint. Automatic maintenance timers are disabled on the disposable runner; interrupted harness runs now record a nonzero status. K3d's remaining gates passed after rechecking its retained API/SQL fixture. Chart contents were unchanged by this host correction.

The preceding six-node k3d candidate passed all required gates, including 68 successful / 1 failed SQL probes over 75.2 seconds during idempotent upgrade. Earlier macOS arm64 single-node runs passed the same application/recovery gates. These earlier runs are historical evidence; the table above records the current candidate on AMD64.

## Upstream and static tests

- Go: 50 packages passed; 11 failed because the OSS checkout lacks `gen/proto/xatastor-storage-node/v1` or `saas-charts/clusterpool-operator/crds`. **The full `make test` result is not passing.** No Go source or upstream assertions were changed.
- OPA: 35/35 passed. Keycloak extension: 82 passed, zero failures/errors/skips.
- `make lint-kube`: local overlay passed; absent `overlays/local-saas` prevents complete overlay verification.
- Strict Helm lint, profile/schema/resource-contract checks, complete repository `make lint-charts`, ShellCheck and Python syntax checks passed during development. Changed checks are rerun after corrections.
- The cloud-runner SIGTERM regression test passed: interruption returns and records status 143 before any cluster can be created. The host-maintenance script changes also pass Bash syntax validation. Source-test counts and missing-file errors are preserved in `dist/verification/upstream-summary.txt`.

## Distribution and operational limits

The archive bundles ten dependencies, CRDs, pinned plugins, bootstrap assets, examples, licenses and this report. The corrected runtime candidate passed archive-only rendering from an empty directory (235 resources) and byte-identical OCI push/pull through a temporary localhost registry. Evidence is in `dist/verification/gcp-distribution-runtime.txt`. Delivery also requires repeating this check on the exact final archive after bundling this report; its separate record is `dist/verification/distribution-final.txt`. The final archive is compared with all five tested packages; only the root README and verification report may differ. This comparison is recorded in `dist/verification/final-package-comparison.txt`. No public release has been published.

This preview is qualified only against the stated test environments. The disposable NFS server is not production HA storage, and archive snapshots do not prove atomicity under arbitrary workloads. External production CSI storage, durable off-cluster backups, identity/SMTP and public DNS/certificates remain operator-provided and unqualified here. Completed standby backups may still need WAL to arrive; the cloud tests observed the default five-minute archive delay before a restore succeeded. Verify restores, not only backup completion.

The approved GCP host uses an auto-deleted boot disk, IAP-only SSH and a four-hour native DELETE deadline. Delivery requires explicit deletion after evidence retrieval and a separate readback confirming absence of the VM, disk, firewall, subnet and network. Final operational evidence is stored outside this archive in `dist/verification/gcp-cleanup-final.log` and `dist/verification/gcp-resource-absence.json`; earlier cleanup logs refer to earlier runners. Heavy tests ran on GCP. The two leftover local envtest processes were identified by their repository working directory and stopped; see `dist/verification/local-envtest-cleanup.txt`.

## Reproduce

```sh
helm dependency build charts/xata
scripts/helm/package.sh dist
python3 tests/helm/render_test.py
scripts/helm/test-matrix.sh
scripts/helm/test-existing-operators.sh
scripts/helm/verify-distribution.sh dist/xata-0.1.0-dev.2.tgz
```

Pass `kind-single`, `k3d-single`, `kind-multi` or `k3d-multi` to select matrix cells. Successful clusters are deleted unless `XATA_KEEP_TEST_CLUSTERS=true`; failures preserve the cluster for diagnosis. The GCP scripts prepare and explicitly delete the disposable runner.

Sources: repository `Makefile`, `charts/Makefile`, `kustomize/Makefile`, `tests/helm/`, `scripts/helm/`, and `charts/xata/UPSTREAM.md`; [NFS CSI implementation](https://github.com/kubernetes-csi/csi-driver-nfs/blob/v4.12.1/pkg/nfs/controllerserver.go), [CNPG backups](https://cloudnative-pg.io/docs/1.25/backup/), [Helm CRD lifecycle](https://helm.sh/docs/chart_best_practices/custom_resource_definitions/).
