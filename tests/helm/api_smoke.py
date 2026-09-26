#!/usr/bin/env python3
"""Exercise the shipped APIs in disposable xata-chart-* clusters. Never prints credentials."""
import base64, hashlib, json, os, pathlib, ssl, subprocess, time, urllib.request, urllib.error
KUBE=os.environ['KUBECONFIG']; NS=os.environ.get('XATA_TEST_NAMESPACE','xata')
K=['kubectl','--kubeconfig',KUBE,'-n',NS]
def run(*args): return subprocess.check_output(K+list(args),text=True,stderr=subprocess.PIPE)
context=run('config','current-context').strip()
assert 'xata-chart-' in context, 'Refusing to create a development identity outside a test cluster'
for deployment in ['auth','projects','gateway']:
 run('rollout','status','deployment/'+deployment,'--timeout=300s')
run('wait','--for=condition=Ready','pod/auth-keycloak-0','--timeout=300s')
run('exec','deploy/auth','-c','auth','--','/server','create_dev_user')
key=run('exec','deploy/auth','-c','auth','--','/server','create_dev_api_key').strip().splitlines()[-1]
services=json.loads(run('get','svc','-l','gateway.envoyproxy.io/owning-gateway-name=eg','-o','json'))
assert len(services['items'])==1
proxies=json.loads(run('get','deploy','-l','gateway.envoyproxy.io/owning-gateway-name=eg','-o','json'))
assert proxies['items'], 'No API Envoy deployment found'
for proxy in proxies['items']:
 run('rollout','status','deployment/'+proxy['metadata']['name'],'--timeout=300s')
listener=json.loads(run('get','gateway.gateway.networking.k8s.io','eg','-o','json'))['spec']['listeners'][0]
scheme='https' if listener['protocol']=='HTTPS' else 'http'
sslcontext=None
if scheme=='https':
 certname=listener['tls']['certificateRefs'][0]['name']
 cert=json.loads(run('get','secret',certname,'-o','json'))['data']['tls.crt']
 sslcontext=ssl.create_default_context(cadata=base64.b64decode(cert).decode())
forward_command=K+['port-forward','svc/'+services['items'][0]['metadata']['name'],'15080:'+str(listener['port'])]
forward=subprocess.Popen(forward_command,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)

def api(path,body=None,auth=True,method=None):
 global forward
 if forward.poll() is not None:
  forward.wait()
  forward=subprocess.Popen(forward_command,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
  time.sleep(1)
 headers={'Host':'localhost','Content-Type':'application/json'}
 if auth:headers['Authorization']='Bearer '+key
 req=urllib.request.Request(scheme+'://localhost:15080'+path,headers=headers,data=None if body is None else json.dumps(body).encode(),method=method)
 try:
  with urllib.request.urlopen(req,timeout=45,context=sslcontext) as resp:
   result=resp.read()
   return json.loads(result) if result else None
 except urllib.error.HTTPError as e:
  detail=e.read().decode()
  raise RuntimeError(f'HTTP {e.code} {path}: {detail[:500]}') from None
try:
 for attempt in range(30):
  try:orgs=api('/organizations');break
  except (OSError,RuntimeError):time.sleep(1)
 else:raise RuntimeError('API gateway not ready')
 if os.environ.get('XATA_TEST_RESTART_API_FORWARD')=='true':
  forward.terminate();forward.wait(timeout=10)
  for attempt in range(30):
   try:
    reconnected=api('/organizations')
    assert {item['id'] for item in reconnected['organizations']}=={item['id'] for item in orgs['organizations']}
    break
   except (OSError,RuntimeError):time.sleep(1)
  else:raise RuntimeError('API port-forward did not reconnect')
  print('PASS: API client port-forward reconnects after termination',flush=True)
 org=orgs['organizations'][0]['id']
 try:api('/organizations',auth=False)
 except RuntimeError as error:assert 'HTTP 401' in str(error), str(error)
 else:raise AssertionError('Unauthenticated request was accepted')
 print('PASS: authenticated organizations API through Envoy over '+scheme.upper()+'; unauthorized access rejected')
 types=api('/organizations/'+org+'/instanceTypes?region=local')
 assert any(t['name']=='xata.micro' for t in types['instanceTypes'])
 projects=api('/organizations/'+org+'/projects')
 existing=next((p for p in projects['projects'] if p['name']=='helm-verification'),None)
 project=existing or api('/organizations/'+org+'/projects',{'name':'helm-verification'})
 print('PASS: project API')
 project=project.get('project',project)
 path='/organizations/'+org+'/projects/'+project['id']+'/branches'
 branches=api(path)['branches']
 branch=next((b for b in branches if b['name']=='main'),None)
 if branch is None:
  branch=api(path,{'name':'main','mode':'custom','configuration':{'image':'postgres:17.11','instanceType':'xata.micro','region':'local','replicas':int(os.environ.get('XATA_TEST_REPLICAS','0')),'storage':1},'scaleToZero':{'enabled':False,'inactivityPeriodMinutes':15}})

 statefile=pathlib.Path(os.environ.get('XATA_TEST_STATE','/private/tmp/xata-chart-test/api-state.json'))
 previous=json.loads(statefile.read_text()) if statefile.exists() else {}
 state={'organization':org,'project':project['id'],'branch':branch.get('branch',branch)['id']}
 print('PASS: project and database creation accepted')
 branch_id=state['branch']
 for attempt in range(60):
  if subprocess.run(['kubectl','--kubeconfig',KUBE,'-n','xata-clusters','get','cluster/'+branch_id],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL).returncode==0:break
  time.sleep(2)
 subprocess.run(['kubectl','--kubeconfig',KUBE,'-n','xata-clusters','wait','--for=condition=Ready','cluster/'+branch_id,'--timeout=600s'],check=True)
 credentials=api(path+'/'+branch_id+'/credentials')
 credentials=credentials.get('credentials',credentials)
 sqlforward=subprocess.Popen(K+['port-forward','svc/gateway','15432:5432'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
 try:
  sqlenv=os.environ|{'PGHOST':credentials['hostname'],'PGHOSTADDR':'127.0.0.1','PGPORT':'15432','PGDATABASE':credentials['dbname'],'PGUSER':credentials['username'],'PGPASSWORD':credentials['password'],'PGSSLMODE':'require','PGCONNECT_TIMEOUT':'5'}
  def sql(query):
   global sqlforward
   if sqlforward.poll() is not None:
    sqlforward=subprocess.Popen(K+['port-forward','svc/gateway','15432:5432'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    time.sleep(1)
   return subprocess.check_output(['psql','-X','-At','-v','ON_ERROR_STOP=1','-c',query],env=sqlenv,text=True,stderr=subprocess.PIPE).strip()
  for attempt in range(30):
   try:
    assert sql('SELECT 1')=='1';break
   except subprocess.CalledProcessError:time.sleep(2)
  else:raise RuntimeError('SQL gateway did not accept the branch connection')
  fingerprint=hashlib.sha256(credentials['password'].encode()).hexdigest()
  if os.environ.get('XATA_TEST_VERIFY_ONLY')=='true':
   assert previous.get('branch')==branch_id, 'Branch identity changed'
   assert previous.get('credentialFingerprint')==fingerprint, 'Database credentials changed'
  else:
   sql('CREATE TABLE IF NOT EXISTS helm_fixture (id integer PRIMARY KEY, payload text NOT NULL); INSERT INTO helm_fixture SELECT n, md5(n::text) FROM generate_series(1,1000) n ON CONFLICT DO NOTHING;')
  checksum=sql("SELECT md5(string_agg(id::text || ':' || payload, ',' ORDER BY id)) FROM helm_fixture")
  assert sql('SELECT count(*) FROM helm_fixture')=='1000'
  assert checksum=='9d8d0c4fd8532ab746d46dd9260e52c4', 'Fixture contents changed'
  if os.environ.get('XATA_TEST_VERIFY_ONLY')=='true':assert checksum==previous['checksum']
  print('PASS: SQL gateway TLS, fixture rows and checksum',checksum)
  state['checksum']=checksum
  state['database']=credentials['dbname']
  state['credentialFingerprint']=fingerprint
  statefile.parent.mkdir(parents=True,exist_ok=True)
  statefile.write_text(json.dumps(state))
  if os.environ.get('XATA_TEST_RECOVERY')=='true':
   data_command=['kubectl','--kubeconfig',KUBE,'-n','xata-clusters']
   selector='cnpg.io/cluster='+branch_id+',cnpg.io/podRole=instance'
   api(path+'/'+branch_id,{'scaleToZero':{'enabled':True,'inactivityPeriodMinutes':15},'hibernate':True},method='PATCH')
   for attempt in range(120):
    pods=json.loads(subprocess.check_output(data_command+['get','pods','-l',selector,'-o','json']))
    if not pods['items']:break
    time.sleep(2)
   else:raise RuntimeError('Database did not hibernate')
   for attempt in range(90):
    try:
     assert sql('SELECT count(*) FROM helm_fixture')=='1000';break
    except subprocess.CalledProcessError:time.sleep(2)
   else:raise RuntimeError('SQL-triggered wakeup did not recover the database')
   print('PASS: hibernation and SQL-triggered wakeup with retained data',flush=True)
   api(path+'/'+branch_id,{'scaleToZero':{'enabled':False,'inactivityPeriodMinutes':15}},method='PATCH')
   subprocess.run(data_command+['delete','pod','-l',selector,'--wait=true','--timeout=180s'],check=True)
   for attempt in range(90):
    try:
     assert sql("SELECT md5(string_agg(id::text || ':' || payload, ',' ORDER BY id)) FROM helm_fixture")==checksum;break
    except subprocess.CalledProcessError:time.sleep(2)
   else:raise RuntimeError('Database did not recover after a pod restart')
   print('PASS: database pod restart with retained checksum',flush=True)
  if os.environ.get('XATA_TEST_UPGRADE_CHART'):
   def secret_fingerprints():
    result={}
    for name in ['xata-metastore-app','xata-metastore-superuser','auth-api-key-hmac','keycloak-realm-secret','gateway-secret','xata-backups']:
     data=json.loads(run('get','secret',name,'-o','json'))['data']
     result[name]=hashlib.sha256(json.dumps(data,sort_keys=True).encode()).hexdigest()
    return result
   before=secret_fingerprints()
   logpath=statefile.parent/'upgrade-test.log'
   with logpath.open('w') as log:
    command=['helm','--kubeconfig',KUBE,'upgrade','xata',os.environ['XATA_TEST_UPGRADE_CHART'],'-n',NS,'--reset-then-reuse-values','--wait','--wait-for-jobs','--timeout','15m']
    if os.environ.get('XATA_TEST_UPGRADE_VALUES'):
     command += ['-f', os.environ['XATA_TEST_UPGRADE_VALUES']]
    upgrade=subprocess.Popen(command,stdout=log,stderr=log)
    successful=failed=0
    started=time.monotonic()
    while upgrade.poll() is None:
     try:
      assert sql('SELECT count(*) FROM helm_fixture')=='1000'
      successful+=1
     except subprocess.CalledProcessError:failed+=1
     time.sleep(1)
    assert upgrade.returncode==0, 'Helm upgrade failed; inspect '+str(logpath)
   assert before==secret_fingerprints(), 'An upgrade changed persistent secrets'
   annotations=json.loads(run('get','service','gateway','-o','json'))['metadata'].get('annotations',{})
   assert not any('aws-load-balancer' in key for key in annotations), 'Upgrade retained obsolete AWS defaults'
   assert sql("SELECT md5(string_agg(id::text || ':' || payload, ',' ORDER BY id)) FROM helm_fixture")==checksum
   print(f'PASS: chart upgrade, stable secrets and data; {successful} successful / {failed} failed SQL probes in {time.monotonic()-started:.1f}s',flush=True)
  if os.environ.get('XATA_TEST_EXTENDED')=='true':
   original_env=sqlenv.copy()
   data_command=['kubectl','--kubeconfig',KUBE,'-n','xata-clusters']
   for restore_mode in ['snapshot','backup']:
    if restore_mode=='backup':
     backup_name='helm-verification-'+str(int(time.time()))
     backup={'apiVersion':'postgresql.cnpg.io/v1','kind':'Backup','metadata':{'name':backup_name},'spec':{'cluster':{'name':branch_id},'method':'plugin','pluginConfiguration':{'name':'barman-cloud.cloudnative-pg.io'}}}
     subprocess.run(data_command+['apply','-f','-'],input=json.dumps(backup),text=True,check=True,stdout=subprocess.DEVNULL)
     for attempt in range(120):
      result=json.loads(subprocess.check_output(data_command+['get','backup',backup_name,'-o','json']))
      phase=result.get('status',{}).get('phase')
      if phase=='completed':break
      assert phase!='failed','Backup failed; inspect its status'
      time.sleep(3)
     else:raise RuntimeError('Backup did not complete')
     print('PASS: completed object-storage backup',flush=True)
     child=api(path+'/'+branch_id+'/restore',{'name':'helm-backup-restore','scaleToZero':{'enabled':False,'inactivityPeriodMinutes':15}})
    else:
     child=api(path,{'name':'helm-snapshot-branch','mode':'inherit','parentID':branch_id,'scaleToZero':{'enabled':False,'inactivityPeriodMinutes':15}})
    child_id=child.get('branch',child)['id']
    # The API responds before its controller has created the CNPG resource.
    for attempt in range(60):
     if subprocess.run(data_command+['get','cluster/'+child_id],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL).returncode==0:break
     time.sleep(2)
    subprocess.run(data_command+['wait','--for=condition=Ready','cluster/'+child_id,'--timeout=600s'],check=True)
    child_credentials=api(path+'/'+child_id+'/credentials')
    child_credentials=child_credentials.get('credentials',child_credentials)
    sqlenv=original_env|{'PGHOST':child_credentials['hostname'],'PGDATABASE':child_credentials['dbname'],'PGUSER':child_credentials['username'],'PGPASSWORD':child_credentials['password']}
    for attempt in range(30):
     try:
      child_checksum=sql("SELECT md5(string_agg(id::text || ':' || payload, ',' ORDER BY id)) FROM helm_fixture");break
     except subprocess.CalledProcessError:time.sleep(2)
    else:raise RuntimeError('Restored branch SQL unavailable')
    assert child_checksum==checksum, 'Restored data differs from the source'
    sql("UPDATE helm_fixture SET payload='child-only' WHERE id=1")
    sqlenv=original_env
    assert sql("SELECT payload FROM helm_fixture WHERE id=1")!='child-only', 'Branch isolation failed'
    print('PASS:',restore_mode,'restore checksum and branch isolation',flush=True)
    api(path+'/'+child_id,method='DELETE')
    subprocess.run(data_command+['wait','--for=delete','cluster/'+child_id,'--timeout=180s'],check=True)
 finally:
  sqlforward.terminate();sqlforward.wait(timeout=10)



finally:
 forward.terminate();forward.wait(timeout=10)
