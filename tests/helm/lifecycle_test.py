#!/usr/bin/env python3
"""Verify HTTPS, a configuration-only rollback, and single-node restart recovery."""
import json
import os
import pathlib
import subprocess
import time

K = ['kubectl', '--kubeconfig', os.environ['KUBECONFIG']]
NS = os.environ.get('XATA_TEST_NAMESPACE', 'xata')
H = ['helm', '--kubeconfig', os.environ['KUBECONFIG'], '-n', NS]
context = subprocess.check_output(K + ['config', 'current-context'], text=True).strip()
assert 'xata-chart-' in context, 'Refusing to operate on a non-test cluster'
chart = os.environ['XATA_TEST_CHART']
env = os.environ | {'XATA_TEST_VERIFY_ONLY': 'true', 'XATA_TEST_EXTENDED': 'false', 'XATA_TEST_RECOVERY': 'false'}
env.pop('XATA_TEST_UPGRADE_CHART', None)
smoke = ['python3', '-u', str(pathlib.Path(__file__).with_name('api_smoke.py'))]
history = json.loads(subprocess.check_output(H + ['history', 'xata', '-o', 'json']))
revision = str(history[-1]['revision'])
subprocess.run(H + ['upgrade', 'xata', chart, '--reset-then-reuse-values', '--set',
                   'api-gateway.gateway.tlsSecret=gateway-secret', '--wait', '--wait-for-jobs', '--timeout', '15m'], check=True)
subprocess.run(smoke, env=env, check=True)
print('PASS: HTTPS API with certificate and hostname verification, unauthorized rejection and retained data', flush=True)
# The binary versions and database schemas are unchanged by this TLS-only change.
subprocess.run(H + ['rollback', 'xata', revision, '--wait', '--wait-for-jobs', '--timeout', '15m'], check=True)
subprocess.run(smoke, env=env, check=True)
print('PASS: configuration-only Helm rollback retains API access, credentials and SQL data', flush=True)

if context.endswith('-single'):
    nodes = json.loads(subprocess.check_output(K + ['get', 'nodes', '-o', 'json']))['items']
    assert len(nodes) == 1
    node = nodes[0]['metadata']['name']
    assert 'xata-chart-' in node
    subprocess.run(['docker', 'restart', node], check=True)
    deadline = time.monotonic() + 600
    while time.monotonic() < deadline:
        result = subprocess.run(K + ['get', '--raw=/readyz', '--request-timeout=5s'],
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if result.returncode == 0:
            break
        time.sleep(3)
    else:
        raise RuntimeError('Kubernetes API did not recover after node restart')
    subprocess.run(K + ['wait', 'node/' + node, '--for=condition=Ready', '--timeout=300s'], check=True)
    # Controllers recover asynchronously after the API becomes healthy.
    for attempt in range(3):
        result = subprocess.run(smoke, env=env)
        if result.returncode == 0:
            break
        time.sleep(10)
    else:
        raise RuntimeError('Application/data did not recover after node restart')
    print('PASS: complete single-node restart recovers API, SQL data and stable credentials', flush=True)
