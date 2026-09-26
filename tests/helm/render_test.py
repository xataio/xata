#!/usr/bin/env python3
"""Validate install contracts, including failure cases, without a Kubernetes cluster."""
import collections
import pathlib
import subprocess
import yaml

ROOT = pathlib.Path(__file__).resolve().parents[2]
CHART = ROOT / 'charts/xata'
BASE = ['helm', 'template', 'xata', str(CHART), '-n', 'tenant-control',
        '--kube-version', '1.34.1',
        '--set', 'clusters.config.clusters.storageClass=test-csi',
        '--set', 'clusters.config.clusters.volumeSnapshotClass=test-snapshot']


def render(args=(), success=True):
    result = subprocess.run(BASE + list(args), text=True, capture_output=True)
    if success:
        assert result.returncode == 0, result.stderr
        return [doc for doc in yaml.safe_load_all(result.stdout) if doc]
    assert result.returncode != 0, 'invalid configuration unexpectedly rendered'


for profile in ['single-node', 'multi-node']:
    docs = render(['-f', str(CHART / f'examples/{profile}.yaml')])
    resources = {(d['kind'], d['metadata'].get('namespace'), d['metadata']['name']): d for d in docs}
    assert len(resources) == len(docs), 'duplicate Kubernetes resource identity'
    for d in docs:
        if d['kind'] == 'Service':
            assert not any('aws-load-balancer' in key for key in (d['metadata'].get('annotations') or {})), 'inherited AWS load-balancer settings'
        if d['kind'] == 'Deployment':
            pod = d['spec']['template']['spec']
            for container in pod.get('containers', []) + pod.get('initContainers', []):
                names = [e['name'] for e in container.get('env', [])]
                assert len(names) == len(set(names)), f'duplicate env in {d["metadata"]["name"]}'
        if d['kind'] in ['Deployment', 'Job', 'Secret', 'ConfigMap']:
            assert d['metadata'].get('namespace', 'tenant-control') != 'xata', 'fixed control-plane namespace'
    bootstrap = resources['ConfigMap', 'tenant-control', 'xata-bootstrap']
    certificates = list(yaml.safe_load_all(bootstrap['data']['certificates.yaml']))
    assert all(d['kind'] in ['Certificate', 'Issuer'] for d in certificates if d)
    bootstrap_command = resources['Job', 'tenant-control', 'xata-bootstrap-1']['spec']['template']['spec']['containers'][0]['args'][0]
    assert bootstrap_command.index('/bootstrap/certificates.yaml') < bootstrap_command.index('rollout status deployment/barman-cloud')
    assert bootstrap_command.index('rollout status deployment/scale-to-zero') < bootstrap_command.index('/bootstrap/resources.yaml')
    custom = list(yaml.safe_load_all(bootstrap['data']['resources.yaml']))
    database = next(d for d in custom if d and d['kind'] == 'Cluster')
    assert database['spec']['instances'] == (1 if profile == 'single-node' else 3)
    plugin = resources['Deployment', 'tenant-control', 'scale-to-zero']
    assert plugin['spec']['replicas'] == (1 if profile == 'single-node' else 2)
    if profile == 'multi-node':
        pod = plugin['spec']['template']['spec']
        assert pod['affinity']['podAntiAffinity']['requiredDuringSchedulingIgnoredDuringExecution'][0]['topologyKey'] == 'kubernetes.io/hostname'
        assert resources['PodDisruptionBudget', 'tenant-control', 'scale-to-zero']['spec']['maxUnavailable'] == 1
        barman = resources['Deployment', 'tenant-control', 'barman-cloud']['spec']['template']['spec']
        assert all(item['tolerationSeconds'] == 10 for item in barman['tolerations'])
    assert database['spec']['superuserSecret']['name'] == 'xata-metastore-superuser'
    for name in ['auth-api-key-hmac', 'keycloak-realm-secret', 'xata-metastore-app']:
        assert resources['Secret', 'tenant-control', name]['metadata']['annotations']['helm.sh/resource-policy'] == 'keep'
    assert all(':latest' not in yaml.safe_dump(d) for d in docs), 'unpinned image'
    certmanager = resources['Deployment', 'tenant-control', 'xata-cert-manager']
    args = certmanager['spec']['template']['spec']['containers'][0]['args']
    assert '--leader-election-namespace=kube-system' in args, 'cert-manager needs a non-empty lease namespace'
    gateway = resources['Deployment', 'tenant-control', 'gateway']
    env = gateway['spec']['template']['spec']['containers'][0]['env']
    namespace = next(item for item in env if item['name']=='XATA_NAMESPACE')
    assert namespace['valueFrom']['fieldRef']['fieldPath']=='metadata.namespace'
    for name in ['auth', 'projects']:
        pod = resources['Deployment', 'tenant-control', name]['spec']['template']['spec']
        assert pod['serviceAccountName'] == name+'-setup-wait'
        assert len(pod['initContainers']) == 1
        waiter = pod['initContainers'][0]
        assert waiter['command'] == ['kubectl']
        assert 'job/'+name+'-setup-1' in waiter['args']
        job = resources['Job', 'tenant-control', name+'-setup-1']['spec']
        assert job['parallelism'] == job['completions'] == 1
        assert 'ttlSecondsAfterFinished' not in job, 'replacement pods still need setup completion'
        setup = job['template']['spec']['containers'][0]
        assert setup['command'] == ['/server'] and setup['args'] == ['setup']
        if name == 'projects':
            volumes = job['template']['spec']['volumes']
            assert any(v['name']=='scheduler-config' for v in volumes)
    print(f'PASS: {profile} rendering and resource contracts')

external = render(['--set', 'cnpg.enabled=false', '--set', 'cert-manager.enabled=false', '--set', 'envoy.enabled=false', '--set', 'keycloak.operator.enabled=false'])
assert not any(d['kind'] == 'Deployment' and d['metadata']['name'] in ['cnpg-controller-manager', 'envoy-gateway', 'keycloak-operator'] for d in external)
render(['--set', 'profile=multi-node'], success=False)
render(['--set', 'backups.bucket='], success=False)
render(['--set', 'gateway.clustersNamespace=other'], success=False)
render(['--set', 'clusters.config.clusters.storageClass='], success=False)
production = render(['-f',str(CHART/'examples/multi-node.yaml'),'-f',str(CHART/'examples/production.yaml')])
listeners = [d['spec']['listeners'][0] for d in production if d['kind']=='Gateway']
assert all(listener['protocol']=='HTTPS' and listener['port']==443 and listener['tls']['certificateRefs'] for listener in listeners)
render(['--set','api-gateway.gateway.hostname=api.example.com'],success=False)
print('PASS: existing operators and invalid-value rejection')
