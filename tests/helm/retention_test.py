#!/usr/bin/env python3
"""Uninstall/reinstall the test release and verify stable identities and data."""
import hashlib, json, os, pathlib, subprocess, tempfile

KUBE = os.environ['KUBECONFIG']
NS = os.environ.get('XATA_TEST_NAMESPACE','xata')
K = ['kubectl','--kubeconfig',KUBE,'-n',NS]
H = ['helm','--kubeconfig',KUBE,'-n',NS]
assert 'xata-chart-' in subprocess.check_output(K+['config','current-context'],text=True)
chart = os.environ['XATA_TEST_CHART']

def fingerprints():
    result = {}
    for name in ['xata-metastore-app','xata-metastore-superuser','auth-api-key-hmac','keycloak-realm-secret','auth-keycloak-initial-admin','gateway-secret','xata-backups']:
        data = json.loads(subprocess.check_output(K+['get','secret',name,'-o','json']))['data']
        result[name] = hashlib.sha256(json.dumps(data,sort_keys=True).encode()).hexdigest()
    return result

before = fingerprints()
with tempfile.TemporaryDirectory(prefix='xata-retention-') as directory:
    values = pathlib.Path(directory)/'values.json'
    values.write_bytes(subprocess.check_output(H+['get','values','xata','-o','json']))
    values.chmod(0o600)
    subprocess.run(H+['uninstall','xata','--wait','--timeout','5m'],check=True)
    assert fingerprints() == before, 'Uninstall removed or changed retained credentials'
    subprocess.run(K+['get','cluster','xata-metastore'],check=True,stdout=subprocess.DEVNULL)
    subprocess.run(H+['install','xata',chart,'-f',str(values),'--wait','--wait-for-jobs','--timeout','15m'],check=True)
assert fingerprints() == before, 'Reinstall rotated persistent credentials'
env = os.environ | {'XATA_TEST_VERIFY_ONLY':'true','XATA_TEST_EXTENDED':'false','XATA_TEST_RECOVERY':'false'}
env.pop('XATA_TEST_UPGRADE_CHART',None)
subprocess.run(['python3','-u',str(pathlib.Path(__file__).with_name('api_smoke.py'))],env=env,check=True)
print('PASS: uninstall/reinstall retains metadata, branch data, credentials and Keycloak admin identity',flush=True)
