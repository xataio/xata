#!/usr/bin/env python3
"""Qualify CSI snapshot restore with a checksum and distinct nodes where available."""
import json, os, subprocess, time

K = ['kubectl', '--kubeconfig', os.environ['KUBECONFIG']]
assert 'xata-chart-' in subprocess.check_output(K + ['config', 'current-context'], text=True)
NS = 'xata-storage-verification'
SC = os.environ.get('XATA_STORAGE_CLASS', 'xata-test-nfs')
SNAP = os.environ.get('XATA_SNAPSHOT_CLASS', SC)

def apply(document):
    subprocess.run(K + ['apply', '-f', '-'], input=json.dumps(document), text=True, check=True, stdout=subprocess.DEVNULL)

def wait(kind, name, condition):
    subprocess.run(K + ['-n', NS, 'wait', kind+'/'+name, '--for='+condition, '--timeout=300s'], check=True)

apply({'apiVersion':'v1','kind':'Namespace','metadata':{'name':NS}})
nodes = json.loads(subprocess.check_output(K + ['get', 'nodes', '-o', 'json']))['items']
workers = [n['metadata']['name'] for n in nodes if 'node-role.kubernetes.io/control-plane' not in n['metadata'].get('labels', {})]
nodes = workers or [n['metadata']['name'] for n in nodes]
source_command = ('printf xata-snapshot-fixture > /data/fixture; '
                  'mkdir /data/owned; printf owner-fixture > /data/owned/data; '
                  'chown -R 26:26 /data/owned; chmod 700 /data/owned; chmod 600 /data/owned/data; '
                  'sync; sha256sum /data/fixture')
restore_command = ('test "$(cat /data/fixture)" = xata-snapshot-fixture; '
                   'test "$(cat /data/owned/data)" = owner-fixture; '
                   'test "$(stat -c %u:%g:%a /data/owned)" = 26:26:700; '
                   'test "$(stat -c %u:%g:%a /data/owned/data)" = 26:26:600; '
                   'sha256sum /data/fixture')
for name, source in [('source', False), ('restored', True)]:
    pvc = {'apiVersion':'v1','kind':'PersistentVolumeClaim','metadata':{'name':name,'namespace':NS},'spec':{'accessModes':['ReadWriteOnce'],'storageClassName':SC,'resources':{'requests':{'storage':'1Gi'}}}}
    if source:
        pvc['spec']['dataSource'] = {'name':'fixture','kind':'VolumeSnapshot','apiGroup':'snapshot.storage.k8s.io'}
    apply(pvc)
    pod = {'apiVersion':'v1','kind':'Pod','metadata':{'name':name,'namespace':NS},'spec':{'restartPolicy':'Never','nodeSelector':{'kubernetes.io/hostname':nodes[-1] if source else nodes[0]},'containers':[{'name':'verify','image':'busybox:1.37.0','command':['sh','-ec',restore_command if source else source_command],'volumeMounts':[{'name':'data','mountPath':'/data'}]}],'volumes':[{'name':'data','persistentVolumeClaim':{'claimName':name}}]}}
    apply(pod)
    wait('pod', name, 'jsonpath={.status.phase}=Succeeded')
    checksum = subprocess.check_output(K + ['-n',NS,'logs',name], text=True).split()[0]
    if not source:
        expected = checksum
        apply({'apiVersion':'snapshot.storage.k8s.io/v1','kind':'VolumeSnapshot','metadata':{'name':'fixture','namespace':NS},'spec':{'volumeSnapshotClassName':SNAP,'source':{'persistentVolumeClaimName':'source'}}})
        wait('volumesnapshot','fixture','jsonpath={.status.readyToUse}=true')
    else:
        assert checksum == expected, 'Snapshot checksum mismatch'
print(f'PASS: CSI snapshot checksum {expected}, UID/GID and permissions; source={nodes[0]}, restore={nodes[-1]}')
