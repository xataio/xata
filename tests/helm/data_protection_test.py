#!/usr/bin/env python3
"""Verify a PITR transaction boundary and WAL catch-up in disposable clusters."""
import copy, json, os, pathlib, subprocess, time

K = ['kubectl','--kubeconfig',os.environ['KUBECONFIG']]
assert 'xata-chart-' in subprocess.check_output(K+['config','current-context'],text=True)
NS = os.environ.get('XATA_TEST_NAMESPACE','xata')
DATA = 'xata-clusters'
state = json.loads(pathlib.Path(os.environ['XATA_TEST_STATE']).read_text())
branch = state['branch']
database = state.get('database','xata')

def get(kind, name, namespace=DATA):
    return json.loads(subprocess.check_output(K+['-n',namespace,'get',kind,name,'-o','json'],stderr=subprocess.PIPE))

def apply(document, namespace=DATA):
    subprocess.run(K+['-n',namespace,'apply','-f','-'],input=json.dumps(document),text=True,check=True,stdout=subprocess.DEVNULL)

def sql(query, cluster=branch):
    primary = get('cluster',cluster)['status']['currentPrimary']
    return subprocess.check_output(K+['-n',DATA,'exec',primary,'-c','postgres','--','psql','-U','postgres','-d',database,'-X','-At','-v','ON_ERROR_STOP=1','-c',query],text=True,stderr=subprocess.PIPE).strip()

sql("CREATE TABLE IF NOT EXISTS helm_pitr (marker text PRIMARY KEY); TRUNCATE helm_pitr; INSERT INTO helm_pitr VALUES ('before');")
name = 'helm-pitr-'+str(int(time.time()))
apply({'apiVersion':'postgresql.cnpg.io/v1','kind':'Backup','metadata':{'name':name},'spec':{'cluster':{'name':branch},'method':'plugin','pluginConfiguration':{'name':'barman-cloud.cloudnative-pg.io'}}})
for attempt in range(180):
    phase = get('backup',name).get('status',{}).get('phase')
    if phase == 'completed': break
    assert phase != 'failed', 'PITR base backup failed'
    time.sleep(2)
else: raise RuntimeError('PITR base backup timed out')
target = sql("SELECT to_char(clock_timestamp() AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"')")
time.sleep(2)
sql("INSERT INTO helm_pitr VALUES ('after');")
archived = int(sql('SELECT archived_count FROM pg_stat_archiver'))
sql('SELECT pg_switch_wal()')
for attempt in range(120):
    if int(sql('SELECT archived_count FROM pg_stat_archiver')) > archived: break
    time.sleep(2)
else: raise RuntimeError('WAL was not archived for the PITR target')
source = get('branch',branch)
spec = copy.deepcopy(source['spec'])
spec['cluster'].update({'name':name,'instances':1,'hibernation':'Disabled','scaleToZero':{'enabled':False,'inactivityPeriodMinutes':15}})
spec['restore'] = {'type':'ObjectStore','name':branch,'timestamp':target}
apply({'apiVersion':'xata.io/v1alpha1','kind':'Branch','metadata':{'name':name},'spec':spec})
try:
    for attempt in range(60):
        try: get('cluster',name); break
        except subprocess.CalledProcessError: time.sleep(2)
    subprocess.run(K+['-n',DATA,'wait','cluster/'+name,'--for=condition=Ready','--timeout=600s'],check=True)
    assert sql('SELECT marker FROM helm_pitr ORDER BY marker',name) == 'before', 'PITR crossed the selected transaction boundary'
    assert sql("SELECT md5(string_agg(id::text || ':' || payload, ',' ORDER BY id)) FROM helm_fixture",name) == state['checksum']
    print('PASS: PITR includes the before transaction, excludes after, and preserves the fixture checksum',flush=True)
finally:
    # Xata Branch is cluster-scoped, unlike the CNPG Cluster it creates.
    subprocess.run(K+['delete','branch',name,'--wait=true','--timeout=180s'],check=True)

# The bundled store is explicitly a disposable test dependency here.
before = int(sql('SELECT archived_count FROM pg_stat_archiver'))
failed_before = int(sql('SELECT failed_count FROM pg_stat_archiver'))
subprocess.run(K+['-n',NS,'scale','deploy/xata-objectstore','--replicas=0'],check=True)
try:
    subprocess.run(K+['-n',NS,'wait','pod','-l','app=xata-objectstore','--for=delete','--timeout=180s'],check=True)
    sql("INSERT INTO helm_pitr VALUES ('during-outage') ON CONFLICT DO NOTHING; SELECT pg_switch_wal();")
    for attempt in range(90):
        if int(sql('SELECT failed_count FROM pg_stat_archiver')) > failed_before: break
        time.sleep(2)
    else: raise RuntimeError('Backup outage did not reach the WAL archiver')
    assert sql('SELECT count(*) FROM helm_fixture') == '1000', 'SQL stopped working during backup outage'
finally:
    subprocess.run(K+['-n',NS,'scale','deploy/xata-objectstore','--replicas=1'],check=True)
subprocess.run(K+['-n',NS,'rollout','status','deploy/xata-objectstore','--timeout=300s'],check=True)
for attempt in range(150):
    if int(sql('SELECT archived_count FROM pg_stat_archiver')) > before: break
    time.sleep(2)
else: raise RuntimeError('WAL did not catch up after restoring object storage')
print('PASS: SQL survives backup outage and WAL archiving resumes',flush=True)
