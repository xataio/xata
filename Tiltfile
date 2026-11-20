def install_snapshots_and_hostpath():
    # Clone & deploy CSI HostPath driver - used for branching
    INSTALL_SNAPSHOTS_CMD = """
        set -e

        echo "📦 Installing VolumeSnapshot CRDs"
        kubectl apply -f https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/release-6.3/client/config/crd/snapshot.storage.k8s.io_volumesnapshotclasses.yaml
        kubectl apply -f https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/release-6.3/client/config/crd/snapshot.storage.k8s.io_volumesnapshotcontents.yaml
        kubectl apply -f https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/release-6.3/client/config/crd/snapshot.storage.k8s.io_volumesnapshots.yaml

        echo "🔧 Installing snapshot-controller"
        kubectl apply -f https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/v6.3.3/deploy/kubernetes/snapshot-controller/rbac-snapshot-controller.yaml
        kubectl apply -f https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/v6.3.3/deploy/kubernetes/snapshot-controller/setup-snapshot-controller.yaml

        echo "⏳ Waiting for CRDs to be Established"
        for crd in \
        volumesnapshotclasses.snapshot.storage.k8s.io \
        volumesnapshotcontents.snapshot.storage.k8s.io \
        volumesnapshots.snapshot.storage.k8s.io
        do
        until kubectl get crd $crd &> /dev/null; do sleep 1; done
        kubectl wait --for=condition=Established crd $crd --timeout=120s
        done

        echo "📥 Cloning CSI HostPath driver"
        if [ ! -d .tilt-build/csi-driver-host-path ]; then
        git clone https://github.com/kubernetes-csi/csi-driver-host-path.git .tilt-build/csi-driver-host-path
        fi

        echo "🚀 Deploying HostPath CSI driver"
        bash .tilt-build/csi-driver-host-path/deploy/kubernetes-latest/deploy.sh
    """
    local_resource(
        name = "install-snapshots-and-hostpath",
        cmd = INSTALL_SNAPSHOTS_CMD,
        deps = ["Tiltfile"],
        labels = "infra"
    )

KUSTOMIZE_FLAGS = ['--enable-helm', '--load-restrictor', 'LoadRestrictionsNone']

def create_rustfs_resource():
    # RustFS, an S3-compatible store for backups and billing events (https://rustfs.com)
    k8s_yaml('dev/rustfs.yaml')
    k8s_resource(workload='rustfs-eu', objects=['rustfs-eu:secret'], port_forwards=9001, labels='infra')

def create_resources():
    install_snapshots_and_hostpath()

    k8s_yaml(secret_from_dict("metastore-reader-credentials", namespace = "xata", inputs = {
        'metastore_reader_user': 'metastore_reader',
        'metastore_reader_password': 'changeme',
    }))

    # Fetch subchart deps before kustomize renders the
    # overlay — kustomize --enable-helm shells out to `helm template`, which
    # requires the deps to be present in charts/.
    local('make -C ./charts build-deps')
    # Use OSS kustomize overlay
    k8s_yaml(kustomize('kustomize/overlays/local', flags=KUSTOMIZE_FLAGS))
    secret_settings(disable_scrub=True)
    update_settings(k8s_upsert_timeout_secs=600)  # 10 minutes for resource creation
    ci_settings(readiness_timeout='10m')


    # Service containers
    # By including only the relevant directories, we ensure other changes don't trigger builds.
    COMMON_DEPENDENCIES = ['./internal', './openapi', './services/auth/api/spec', './gen', './go.mod', './go.sum', './proto']

    # OSS auth service
    docker_build('xatatech/auth', '.',
                build_args={'SERVICE_NAME': 'auth'},
                only=['./services/auth'] + COMMON_DEPENDENCIES)
    docker_build('xatatech/clusters', '.',
                build_args={'SERVICE_NAME': 'clusters'},
                only=['./services/clusters', './services/branch-operator'] + COMMON_DEPENDENCIES)
    docker_build('xatatech/gateway', '.',
                build_args={'SERVICE_NAME': 'gateway'},
                only=['./services/gateway'] + COMMON_DEPENDENCIES)
    # OSS projects service
    docker_build('xatatech/projects', '.',
                build_args={'SERVICE_NAME': 'projects', 'SERVICE_PATH': './services/projects'},
                only=['./services/projects', './services/clusters', './services/branch-operator'] + COMMON_DEPENDENCIES)
    docker_build('xatatech/branch-operator', '.',
                build_args={'SERVICE_NAME': 'branch-operator'},
                only=['./services/branch-operator'] + COMMON_DEPENDENCIES)

    docker_build('xatatech/keycloak', './dev/docker/keycloak',
                build_args={'NO_THEME': 'true'})

    k8s_kind('Keycloak$', image_json_path='{.spec.image}')

    # Sidecar for the scale-to-zero CNPG-I plugin
    docker_build('xatatech/scale-to-zero-sidecar', '.',
                dockerfile='./services/scale-to-zero-sidecar/Dockerfile',
                match_in_env_vars=True,
                only=[
                  './services/scale-to-zero-sidecar',
                  './services/branch-operator/api/',
                  './go.mod',
                  './go.sum'
                  ]
                )

    # Services
    k8s_resource(workload='auth', resource_deps=['bootstrap-db', 'auth-keycloak', 'import-keycloak-realm'], labels='services')
    k8s_resource(workload='clusters', resource_deps=['rustfs-eu'], port_forwards=5002, labels='services')
    k8s_resource(workload='gateway', port_forwards=[7654, 8443], labels='services')
    k8s_resource(workload='projects', resource_deps=['bootstrap-db'], port_forwards='5003:5002', labels='services')
    k8s_resource(workload='branch-operator', labels='services')

    # Databases
    k8s_resource(workload='postgres', labels='databases')
    k8s_resource(workload='bootstrap-db', resource_deps=['postgres'], labels='databases')

    # Auth
    k8s_resource(workload='keycloak-operator', labels='auth')
    k8s_resource(workload='import-keycloak-realm', resource_deps=['auth-keycloak'], labels='auth')
    k8s_resource(workload='auth-keycloak', resource_deps=['keycloak-operator', 'bootstrap-db'], labels='auth')

    # Per-cell metrics & logs stack (VictoriaMetrics + VictoriaLogs + Vector).
    k8s_resource('victoria-metrics', port_forwards=8428, labels='monitoring')
    k8s_resource('victoria-logs', port_forwards=9428, labels='monitoring')
    k8s_resource('vmagent', labels='monitoring')
    k8s_resource('vector', labels='monitoring')

    # Networking
    k8s_resource(workload='envoy-gateway', labels='networking')
    # Expose envoy API gateway (port 5081; avoids macOS AirPlay Receiver on 5000/5001)
    local_resource('envoy-api', labels='networking', links='http://localhost:5081', serve_cmd='''
            while true; do
                attempt=0;
                while true; do
                    err=$(kubectl get svc -n envoy-gateway-system --selector=gateway.envoyproxy.io/owning-gateway-namespace=xata,gateway.envoyproxy.io/owning-gateway-name=eg -o jsonpath='{.items[0].metadata.name}' 2>&1 >/tmp/envoy-api-svc);
                    ENVOY_SERVICE=$(cat /tmp/envoy-api-svc);
                    if [ -n "$ENVOY_SERVICE" ]; then break; fi;
                    attempt=$((attempt + 1));
                    if [ $((attempt % 15)) -eq 1 ]; then
                        echo "Waiting for envoy (attempt $attempt): ${err:-selector matched no service}";
                        kubectl get svc -n envoy-gateway-system 2>&1 | sed 's/^/    /';
                    fi;
                    sleep 1;
                done;
                kubectl port-forward -n envoy-gateway-system svc/$ENVOY_SERVICE 5081:80;
            done
        ''',
        readiness_probe=probe(
            period_secs=1,
            tcp_socket=tcp_socket_action(port=5081, host='localhost'),
        ))

    # Expose envoy keycloak gateway (port 8080)
    local_resource('envoy-auth', labels='networking', links='http://localhost:8080', serve_cmd='''
            while true; do
                attempt=0;
                while true; do
                    err=$(kubectl get svc -n envoy-gateway-system --selector=gateway.envoyproxy.io/owning-gateway-namespace=xata,gateway.envoyproxy.io/owning-gateway-name=keycloak-eg -o jsonpath='{.items[0].metadata.name}' 2>&1 >/tmp/envoy-auth-svc);
                    ENVOY_SERVICE=$(cat /tmp/envoy-auth-svc);
                    if [ -n "$ENVOY_SERVICE" ]; then break; fi;
                    attempt=$((attempt + 1));
                    if [ $((attempt % 15)) -eq 1 ]; then
                        echo "Waiting for envoy (attempt $attempt): ${err:-selector matched no service}";
                        kubectl get svc -n envoy-gateway-system 2>&1 | sed 's/^/    /';
                    fi;
                    sleep 1;
                done;
                kubectl port-forward -n envoy-gateway-system svc/$ENVOY_SERVICE 8080:80;
            done
        ''',
        readiness_probe=probe(
            period_secs=1,
            tcp_socket=tcp_socket_action(port=8080, host='localhost'),
        ))

    # Infra
    k8s_resource(workload='eg-gateway-helm-certgen', labels='infra')
    k8s_resource(workload='cert-manager', labels='infra')
    k8s_resource(workload='cert-manager-cainjector', labels='infra')
    k8s_resource(workload='cert-manager-webhook', labels='infra')
    create_rustfs_resource()

    # CNPG
    k8s_resource(workload='cnpg-controller-manager', labels=['infra', 'cnpg'])
    k8s_resource(workload='scale-to-zero', labels=['infra', 'cnpg'])

    # E2E Tests
    local_resource(
        'e2e-tests',
        'make test-e2e-local',
        deps=['e2e'],
        resource_deps=['auth', 'clusters', 'gateway', 'projects', 'envoy-api', 'envoy-auth'],
        labels='e2e-tests',
    )


## Tilt up service selection

load('ext://secret', 'secret_create_generic', 'secret_from_dict')

config.define_string_list("to-run", args=True)
cfg = config.parse()

create_resources()

groups_by_label = {
    'monitoring': [
        'victoria-metrics',
        'victoria-logs',
        'vmagent',
        'vector',
    ],
    'cnpg': [
        'cnpg-controller-manager',
        'scale-to-zero',
    ],
    'infra': [
        'eg-gateway-helm-certgen',
        'cert-manager',
        'cert-manager-cainjector',
        'cert-manager-webhook',
        'rustfs-eu',
        'install-snapshots-and-hostpath'
    ],
    'networking': [
        'envoy-gateway',
        'envoy-api'
    ],
    'databases': [
        'postgres',
        'bootstrap-db'
    ],
    'services': [
        'auth',
        'clusters',
        'gateway',
        'projects',
        'branch-operator',
    ],
    'e2e-tests': [
        'e2e-tests',
    ],
}

to_run = []
to_run_args = cfg.get('to-run', [])
# Default to 'core' services in CI if no specific resources are requested
CI = os.getenv("CI", "") == "true"
if not to_run_args and CI:
    to_run_args = ['core', 'e2e-tests']

for arg in to_run_args:
    if arg == 'core':
        to_run.extend(groups_by_label.get('infra'))
        to_run.extend(groups_by_label.get('networking'))
        to_run.extend(groups_by_label.get('databases'))
        to_run.extend(groups_by_label.get('services'))
        to_run.extend(groups_by_label.get('cnpg'))
    elif arg in groups_by_label:
        # Add any resources by their predefined groups
        to_run.extend(groups_by_label.get(arg))
    else:
        # Add any resources by their individual names
        to_run.append(arg)


config.set_enabled_resources(to_run)
