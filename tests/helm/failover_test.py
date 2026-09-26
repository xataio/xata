#!/usr/bin/env python3
"""Kill one disposable worker, then one control-plane node; always restart them."""
import json, os, pathlib, subprocess, time

K = ['kubectl','--kubeconfig',os.environ['KUBECONFIG'],'--request-timeout=15s']
context = subprocess.check_output(K+['config','current-context'],text=True).strip()
assert 'xata-chart-' in context and 'multi' in context
DATA = 'xata-clusters'
state = json.loads(pathlib.Path(os.environ['XATA_TEST_STATE']).read_text())
branch = state['branch']
database = state.get('database','xata')

def get(kind, name='', namespace=DATA):
    args = K+['-n',namespace,'get',kind]
    if name: args.append(name)
    return json.loads(subprocess.check_output(args+['-o','json'],stderr=subprocess.PIPE,timeout=20))

def sql(query):
    primary = get('cluster',branch)['status']['currentPrimary']
    return subprocess.check_output(K+['-n',DATA,'exec',primary,'-c','postgres','--','psql','-U','postgres','-d',database,'-X','-At','-v','ON_ERROR_STOP=1','-c',query],text=True,stderr=subprocess.PIPE,timeout=20).strip()

cluster = get('cluster',branch)
assert cluster['spec']['instances'] == 3 and cluster['status']['readyInstances'] == 3
control_ns = os.environ.get('XATA_TEST_NAMESPACE','xata')
plugin = get('deployment','scale-to-zero',control_ns)
assert plugin['spec']['replicas'] >= 2 and plugin['status'].get('readyReplicas',0) >= 2
plugin_pods = [p for p in get('pods',namespace=control_ns)['items'] if p['metadata'].get('labels',{}).get('app') == 'scale-to-zero' and not p['metadata'].get('deletionTimestamp')]
assert len({p['spec']['nodeName'] for p in plugin_pods}) >= 2, 'Scale-to-zero plugin replicas must occupy distinct nodes'
pods = [p for p in get('pods')['items'] if p['metadata'].get('labels',{}).get('cnpg.io/cluster') == branch and p['metadata'].get('labels',{}).get('cnpg.io/podRole') == 'instance']
assert len({p['spec']['nodeName'] for p in pods}) == 3, 'Database instances must occupy three workers'
primary = cluster['status']['currentPrimary']
worker = next(p['spec']['nodeName'] for p in pods if p['metadata']['name'] == primary)
assert 'xata-chart-' in worker
assert get('node',worker)['metadata']['labels'].get('xata.io/test-worker') == 'true'
sql('CREATE TABLE IF NOT EXISTS helm_failover (id int PRIMARY KEY); TRUNCATE helm_failover; INSERT INTO helm_failover VALUES (1);')
lsn = sql('SELECT pg_current_wal_lsn()')
for attempt in range(60):
    # The profile uses asynchronous replication. Establish replay before faulting.
    if sql(f"SELECT count(*) FROM pg_stat_replication WHERE replay_lsn >= '{lsn}'::pg_lsn") == '2': break
    time.sleep(2)
else: raise RuntimeError('Replicas did not replay the acknowledged fixture')
started = time.monotonic()
subprocess.run(['docker','kill','--signal=KILL',worker],check=True)
try:
    for attempt in range(180):
        try:
            current = get('cluster',branch)['status']['currentPrimary']
            if current != primary and sql('SELECT count(*) FROM helm_failover') == '1':
                sql('INSERT INTO helm_failover VALUES (2) ON CONFLICT DO NOTHING')
                assert sql("SELECT md5(string_agg(id::text || ':' || payload, ',' ORDER BY id)) FROM helm_fixture") == state['checksum']
                break
        except (subprocess.SubprocessError, KeyError): pass
        time.sleep(2)
    else: raise RuntimeError('Worker loss did not produce a writable promoted primary')
    print(f'PASS: worker loss promotes a writable replica in {time.monotonic()-started:.1f}s; replayed fixture retained',flush=True)
finally:
    subprocess.run(['docker','start',worker],check=True)
subprocess.run(K+['wait','node/'+worker,'--for=condition=Ready','--timeout=300s'],check=True)

nodes = get('nodes')['items']
controls = [n['metadata']['name'] for n in nodes if 'node-role.kubernetes.io/control-plane' in n['metadata'].get('labels',{}) and n['metadata'].get('labels',{}).get('xata.io/test-storage') != 'true']
assert len(controls) >= 2
control = sorted(controls)[-1]
assert 'xata-chart-' in control
subprocess.run(['docker','kill','--signal=KILL',control],check=True)
try:
    for attempt in range(60):
        try:
            get('nodes')
            assert sql('SELECT count(*) FROM helm_failover') == '2'
            break
        except subprocess.SubprocessError: time.sleep(2)
    else: raise RuntimeError('Control-plane member loss interrupted API or SQL beyond the timeout')
    print('PASS: Kubernetes API and SQL remain available with one control-plane member stopped',flush=True)
finally:
    subprocess.run(['docker','start',control],check=True)
subprocess.run(K+['wait','node/'+control,'--for=condition=Ready','--timeout=300s'],check=True)
