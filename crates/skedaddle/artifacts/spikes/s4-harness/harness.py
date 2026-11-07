"""Shared Windows/Git-Bash harness. No host file deletion, truncation, or image pruning.
All files live below artifacts. Files created by earlier attempts are preserved.
"""
import ctypes, datetime, gzip, hashlib, hmac, json, os, pathlib, re, shutil
import subprocess, sys, threading, time, uuid
sys.dont_write_bytecode = True
ROOT = pathlib.Path(__file__).resolve().parent
ART = ROOT.parents[1]
LOCK = json.loads((ROOT/'images.lock').read_text(encoding='utf8'))
IMAGES = LOCK['images']
RUN = os.environ.get('SKEDADDLE_RUN_ID', 'manual')
if not re.fullmatch(r'[a-z0-9][a-z0-9-]{0,62}', RUN): raise ValueError('run-id must be lowercase alphanumeric/hyphen, <=63 characters')
MAJOR = int(os.environ.get('SKEDADDLE_PG_MAJOR', '18'))
OUT = ART / ('s4-'+RUN)
GIB = 1024**3
VHDX = pathlib.Path('C:/Users/beach/AppData/Local/Docker/wsl/disk/docker_data.vhdx')
def command(args, check=True, env=None, timeout=180, input=None):
    p=subprocess.run([str(x) for x in args],capture_output=True,text=True,encoding='utf8',errors='replace',env=env,timeout=timeout,input=input)
    if check and p.returncode: raise RuntimeError(f'{args}: exit {p.returncode}\n{p.stdout}\n{p.stderr}')
    return {'exit':p.returncode,'stdout':p.stdout,'stderr':p.stderr}
def docker(*args, **kwargs): return command(['docker',*args],**kwargs)
def text(*args,**kwargs): return docker(*args,**kwargs)['stdout'].strip()
def save(name, data):
    # Exclusive creation prevents collisions with files not created by this run.
    with (OUT/name).open('x',encoding='utf8') as f:
        f.write(data if isinstance(data,str) else json.dumps(data,indent=2))
def event(kind,data): save(f'{kind}-{time.time_ns()}.json',data)
def environment():
    env=os.environ.copy()
    env.update(RUN_ID=RUN, PG_IMAGE=IMAGES['pg'+str(MAJOR)]['ref'], PG_VOLUME_TARGET='/var/lib/postgresql' if MAJOR>=18 else '/var/lib/postgresql/data')
    for key in ('probe','minio','gcs','toxiproxy','pgbouncer'): env[key.upper()+'_IMAGE']=IMAGES[key]['ref']
    return env
def compose(*args,**kwargs): return docker('compose','-f',str(ROOT/'compose.yaml'),'-p','skedaddle-'+RUN,*args,env=environment(),**kwargs)
def resources(run=None):
    filt='label=skedaddle.test'+('='+run if run else '')
    result={}
    for kind,args in [('containers',['ps','-aq']),('volumes',['volume','ls','-q']),('networks',['network','ls','-q'])]:
        result[kind]=text(*args,'--filter',filt).splitlines()
    return result
def remove_resources(run=None,dry=False):
    found=resources(run)
    for kind,args in [('containers',['rm','-f']),('volumes',['volume','rm']),('networks',['network','rm'])]:
        for ident in found[kind]:
            # Reinspect and revalidate immediately before each removal.
            obj=json.loads(text('inspect',ident))[0]
            labels=(obj.get('Config') or {}).get('Labels') if kind=='containers' else obj.get('Labels')
            labels=labels or {}
            if 'skedaddle.test' not in labels or (run and labels['skedaddle.test']!=run): raise RuntimeError('Label changed: refusing '+ident)
            print(('WOULD REMOVE' if dry else 'REMOVE'),kind,ident,labels['skedaddle.test'],flush=True)
            if not dry: docker(*args,ident)
    return found
def alive(pid):
    if not pid: return False
    k=ctypes.WinDLL('kernel32',use_last_error=True)
    k.OpenProcess.restype=ctypes.c_void_p
    handle=k.OpenProcess(0x1000,False,int(pid))
    if not handle: return ctypes.get_last_error()==5 # inaccessible means conservatively live
    status=ctypes.c_ulong()
    ok=k.GetExitCodeProcess(ctypes.c_void_p(handle),ctypes.byref(status))
    k.CloseHandle(ctypes.c_void_p(handle))
    return bool(ok and status.value==259)
def sweep():
    runs=set()
    for kind,ids in resources().items():
        for ident in ids:
            obj=json.loads(text('inspect',ident))[0]
            labels=(obj.get('Config') or {}).get('Labels') if kind=='containers' else obj.get('Labels')
            runs.add(labels['skedaddle.test'])
    for run in sorted(runs):
        if run==RUN: continue
        state=ART/('s4-'+run)/'owner.json'
        if not state.exists():
            print('SWEEPER preserves unknown labeled run',run,'(use explicit cleanup.ps1)'); continue
        owner=json.loads(state.read_text())
        if alive(owner.get('pid')):
            print('SWEEPER preserves active run',run); continue
        print('SWEEPER removes orphan',run,flush=True)
        orphan_env=os.environ.copy()
        orphan_env.update(SKEDADDLE_RUN_ID=run,SKEDADDLE_PG_MAJOR=str(owner['pg_major']))
        recovered=command([sys.executable,'-B',str(ROOT/'harness.py'),'export'],env=orphan_env,check=False)
        print('SWEEPER orphan evidence',recovered,flush=True)
        remove_resources(run)
def guard():
    if OUT.exists(): raise RuntimeError('Run-id already has artifacts; choose a fresh run-id: '+RUN)
    cfree=shutil.disk_usage('C:/').free
    budget_path=ROOT/'budget.json'
    base=json.loads(budget_path.read_text())['base_budget_bytes'] if budget_path.exists() else 4*GIB
    missing=[]
    for key,item in IMAGES.items():
        if docker('image','inspect',item['ref'],check=False)['exit']: missing.append(key)
    missing_bytes=sum(IMAGES[k]['size_bytes'] for k in missing)
    override=float(os.environ.get('SKEDADDLE_BUDGET_GIB','0'))
    needed=int(override*GIB) if override else int(base+3*missing_bytes)
    print(f'DISK GUARD C: needed={needed} free={cfree}; missing image allowance={3*missing_bytes}',flush=True)
    if cfree<needed: raise RuntimeError(f'DISK GUARD REFUSED C: needed {needed} bytes vs free {cfree}. Author may reclaim unused Docker data with their own docker system prune; harness never prunes.')
    probe=IMAGES['probe']['ref']
    if 'probe' in missing: docker('pull',probe)
    name='skedaddle-'+RUN+'-guard'
    # No image-declared VOLUME, no bind mounts; root overlay shares Docker's backing filesystem.
    # --rm removes this task-created, run-labeled ephemeral Docker container only.
    try:
        output=text('run','--rm','--name',name,'--label','skedaddle.test='+RUN,probe,'df','-Pk','/')
    finally:
        if text('ps','-aq','--filter','name=^/'+name+'$','--filter','label=skedaddle.test='+RUN):
            docker('rm','-f',name)
    free=int(output.splitlines()[-1].split()[3])*1024
    print(f'DISK GUARD Docker VM: needed={needed} free={free}',flush=True)
    if free<needed: raise RuntimeError(f'DISK GUARD REFUSED Docker VM: needed {needed} vs free {free} bytes; harness never prunes.')
def snapshot():
    return {'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'c_free':shutil.disk_usage('C:/').free,'vhdx':VHDX.stat().st_size,'docker_system_df':docker('system','df',check=False)}
def up():
    OUT.mkdir(exist_ok=False)
    (OUT/'temp').mkdir()
    save('owner.json',{'pid':int(os.environ.get('SKEDADDLE_OWNER_PID',os.getppid())),'msys_pid':os.environ.get('SKEDADDLE_MSYS_PID'),'run_id':RUN,'pg_major':MAJOR})
    save('before.json',snapshot())
    save('compose-up.json',compose('up','-d','--wait','--wait-timeout','150',check=False))
    ps=compose('ps','--format','json')
    save('compose-started.json',ps)
    # Compose's result was recorded even on failure.
    up_result=json.loads((OUT/'compose-up.json').read_text())
    if up_result['exit']: raise RuntimeError(up_result['stderr'])
    print('READY',RUN,'PG',MAJOR,flush=True)
def cid(service): return compose('ps','-q',service)['stdout'].strip()
def execute(service,*args,check=True,input=None): return docker('exec','-i',cid(service),*args,check=check,input=input)
def sql(query,user='postgres',host=None,port=5432,check=True):
    args=['env','PGPASSWORD='+('skedaddle' if user=='postgres' else 'app'),'psql','-X','-v','ON_ERROR_STOP=1','-U',user,'-d','skedaddle','-At']
    if host: args+=['-h',host,'-p',str(port)]
    return execute('pg',*args,check=check,input=query)
def wget(url,*args,check=True): return execute('probe','wget','-q','-O','-',*args,url,check=check)
def smoke():
    checks={}
    checks['settings']=sql("select name,setting,unit from pg_settings where name in ('wal_level','max_slot_wal_keep_size','max_wal_size','logical_decoding_work_mem','max_replication_slots','max_wal_senders','logging_collector','log_destination') order by name;")
    checks['pgbouncer']=sql('select 42;',host='pgbouncer')
    checks['toxiproxy_version']=wget('http://toxiproxy:8474/version')
    checks['toxiproxy_create']=wget('http://toxiproxy:8474/proxies','--header=Content-Type: application/json','--post-data={"name":"pg","listen":"0.0.0.0:5433","upstream":"pg:5432","enabled":true}')
    checks['toxiproxy_query']=sql('select 43;',host='toxiproxy',port=5433)
    checks['minio_health']=wget('http://minio:9000/minio/health/live')
    # AWS SigV4 list-buckets without an additional image or host network port.
    now=datetime.datetime.now(datetime.timezone.utc)
    stamp=now.strftime('%Y%m%dT%H%M%SZ'); day=stamp[:8]
    payload=hashlib.sha256(b'').hexdigest()
    headers=f'host:minio:9000\nx-amz-content-sha256:{payload}\nx-amz-date:{stamp}\n'
    signed='host;x-amz-content-sha256;x-amz-date'
    canonical='GET\n/\n\n'+headers+'\n'+signed+'\n'+payload
    scope=day+'/us-east-1/s3/aws4_request'
    message='AWS4-HMAC-SHA256\n'+stamp+'\n'+scope+'\n'+hashlib.sha256(canonical.encode()).hexdigest()
    key=b'AWS4skedaddle-secret'
    for value in (day,'us-east-1','s3','aws4_request'): key=hmac.new(key,value.encode(),hashlib.sha256).digest()
    signature=hmac.new(key,message.encode(),hashlib.sha256).hexdigest()
    auth=f'AWS4-HMAC-SHA256 Credential=skedaddle/{scope}, SignedHeaders={signed}, Signature={signature}'
    checks['minio_list']=wget('http://minio:9000/','--header=x-amz-date: '+stamp,'--header=x-amz-content-sha256: '+payload,'--header=Authorization: '+auth)
    checks['gcs_list']=wget('http://gcs:4443/storage/v1/b')
    save('smoke.json',checks)
    print('SMOKE PASS',RUN,flush=True)
def privileges():
    sql("CREATE ROLE app LOGIN PASSWORD 'app' NOSUPERUSER NOREPLICATION; GRANT USAGE,CREATE ON SCHEMA public TO app; CREATE TABLE owned_by_postgres(id int); CREATE FUNCTION public.evt() RETURNS event_trigger LANGUAGE plpgsql AS $$ BEGIN END $$;")
    results={}
    def test(name,query,expect,user='app'):
        r=sql(query,user=user,check=False)
        r['expected_success']=expect
        r['pass']=(r['exit']==0)==expect
        results[name]=r
        event('privilege-'+name,r)
    test('event_trigger',"CREATE EVENT TRIGGER app_evt ON ddl_command_end EXECUTE FUNCTION public.evt();",False)
    test('self_grant_replication','ALTER ROLE app REPLICATION;',False)
    test('publication_without_db_create','CREATE TABLE app_owned(id int); CREATE PUBLICATION pub_denied FOR TABLE app_owned;',False)
    sql('GRANT CREATE ON DATABASE skedaddle TO app;')
    test('publication_owned_table','CREATE PUBLICATION pub_owned FOR TABLE app_owned;',True)
    test('publication_foreign_table','CREATE PUBLICATION pub_foreign FOR TABLE owned_by_postgres;',False)
    test('publication_all_tables','CREATE PUBLICATION pub_all FOR ALL TABLES;',False)
    test('slot_without_replication',"SELECT * FROM pg_create_logical_replication_slot('app_denied','pgoutput');",False)
    sql('ALTER ROLE app REPLICATION;')
    test('slot_with_replication',"SELECT * FROM pg_create_logical_replication_slot('app_allowed','pgoutput');",True)
    test('origin_without_grant',"SELECT pg_replication_origin_create('app_origin');",False)
    funcs=['pg_replication_origin_create(text)','pg_replication_origin_advance(text,pg_lsn)','pg_replication_origin_session_setup(text)','pg_replication_origin_session_is_setup()','pg_replication_origin_session_progress(boolean)','pg_replication_origin_session_reset()','pg_replication_origin_xact_setup(pg_lsn,timestamp with time zone)','pg_replication_origin_xact_reset()','pg_replication_origin_progress(text,boolean)','pg_replication_origin_drop(text)']
    sql(' '.join('GRANT EXECUTE ON FUNCTION '+f+' TO app;' for f in funcs))
    test('origin_with_grants',"SELECT pg_replication_origin_create('app_origin'); SELECT pg_replication_origin_advance('app_origin','0/100'); SELECT pg_replication_origin_session_setup('app_origin'); SELECT pg_replication_origin_session_is_setup(); SELECT pg_replication_origin_session_progress(false); SELECT pg_replication_origin_xact_setup('0/200',now()); SELECT pg_replication_origin_xact_reset(); SELECT pg_replication_origin_session_reset(); SELECT pg_replication_origin_progress('app_origin',false); SELECT pg_replication_origin_drop('app_origin');",True)
    save('privileges.json',results)
    if not all(r['pass'] for r in results.values()): raise RuntimeError('Privilege expectations differed; see privileges.json')
    print('PRIVILEGES PASS',MAJOR,flush=True)
def large():
    stop=threading.Event(); samples=[]
    def sample():
        while not stop.is_set():
            samples.append(snapshot())
            stop.wait(3)
    t=threading.Thread(target=sample,daemon=True); t.start()
    try:
        sql("SELECT * FROM pg_create_logical_replication_slot('large_slot','pgoutput');")
        # 60 scales measured <1,000,000,000 bytes including indexes; avoids overshooting the cap.
        init=execute('pg','pgbench','-U','postgres','-d','skedaddle','-i','-s','60')
        save('pgbench-init.json',init)
        size=int(sql("SELECT pg_database_size('skedaddle');")['stdout'].strip())
        save('database-size-before-churn.json',{'bytes':size})
        if size>1000000000: raise RuntimeError('Database cap exceeded')
        # Update a limited hot set; each transaction checks size before the next batch.
        for i in range(6):
            sql('UPDATE pgbench_accounts SET abalance=abalance+1 WHERE aid BETWEEN 1 AND 20000; CHECKPOINT;')
            size=int(sql("SELECT pg_database_size('skedaddle');")['stdout'].strip())
            if size>1000000000: raise RuntimeError('Database cap exceeded during churn')
            time.sleep(2)
        save('large-result.json',{'database_bytes':size,'slot':sql('SELECT slot_name,wal_status,safe_wal_size FROM pg_replication_slots;'),'volume_usage':execute('pg','du','-sk','/var/lib/postgresql'),'settings':sql('SHOW max_slot_wal_keep_size; SHOW max_wal_size; SHOW logical_decoding_work_mem;')})
    finally:
        stop.set();t.join();samples.append(snapshot());save('peak-samples.json',samples)
    print('LARGE PASS',size,flush=True)
def export():
    if not OUT.exists(): return
    name='evidence-'+str(time.time_ns())+'.jsonl.gz'
    # Gzip worst-case overhead for <=47 MiB raw is <1 MiB. Leave 2 MiB for metadata.
    existing=sum(p.stat().st_size for p in OUT.rglob('*') if p.is_file())
    available=50*1024**2-existing
    if available<=1024**2: raise RuntimeError('Evidence budget exhausted; refusing new export, preserving all existing files')
    raw_limit=int(os.environ.get('SKEDADDLE_EXPORT_RAW_LIMIT',47*1024**2))
    raw_limit=max(0,min(raw_limit,47*1024**2,available-1024**2))
    remaining=raw_limit; clipped=False
    commands=[['docker','compose','-f',str(ROOT/'compose.yaml'),'-p','skedaddle-'+RUN,'ps','-a','--format','json'],['docker','system','df'],['docker','system','df','-v']]
    for ident in resources(RUN)['containers']:
        commands += [['docker','inspect',ident],['docker','logs','--timestamps','--tail','20000',ident]]
    with gzip.open(OUT/name,'xb',compresslevel=6) as f:
        def emit(b):
            nonlocal remaining,clipped
            n=min(len(b),remaining)
            if n:f.write(b[:n]);remaining-=n
            if n<len(b):clipped=True
        for args in commands:
            emit(('\nCOMMAND '+json.dumps(args)+'\n').encode())
            p=subprocess.Popen(args,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,env=environment())
            while True:
                chunk=p.stdout.read(65536)
                if not chunk:break
                emit(chunk)
            p.wait()
            emit(('\nEXIT '+str(p.returncode)+'\n').encode())
        # Small footer reserved outside raw stream allowance.
        f.write(('\nEXPORT_END clipped='+str(clipped)+' raw_limit='+str(raw_limit)+'\n').encode())
    if (OUT/name).stat().st_size>48*1024**2: raise RuntimeError('Unexpected gzip expansion exceeded evidence allowance')
    print('EXPORTED',OUT/name,'bytes',(OUT/name).stat().st_size,'clipped',clipped,flush=True)
def down():
    errors=[]
    try:
        result=compose('down','--volumes','--remove-orphans',check=False)
        if OUT.exists():event('compose-down',result)
        if result['exit']:errors.append(result)
    finally:
        remove_resources(RUN)
        if OUT.exists():
            event('after',snapshot())
            print('TEMP PRESERVED',OUT/'temp',flush=True)
    check(RUN)
    if errors: print('Compose down initially failed; label fallback completed',errors)
def check(run=None):
    found=resources(run)
    if any(found.values()):
        print('LEFTOVER TEST RESOURCES',json.dumps(found));raise RuntimeError('no_leftover_test_resources FAILED')
    print('no_leftover_test_resources: PASS',flush=True)
def inventory():
    result={}
    for kind,args in [('containers',['ps','-aq']),('volumes',['volume','ls','-q']),('networks',['network','ls','-q']),('images',['image','ls','-q','--no-trunc'])]:
        values=[]
        for ident in sorted(set(text(*args).splitlines())):
            obj=json.loads(text('inspect',ident))[0]
            labels=(obj.get('Config') or {}).get('Labels') if kind in ('containers','images') else obj.get('Labels')
            if 'skedaddle.test' not in (labels or {}): values.append(obj.get('Id',obj.get('Name')))
        result[kind]=sorted(values)
    return result
if __name__=='__main__':
    action=sys.argv[1]
    try:
        if action=='guard':guard()
        elif action=='sweep':sweep()
        elif action=='up':up()
        elif action=='smoke':smoke()
        elif action=='privileges':privileges()
        elif action=='large':large()
        elif action=='export':export()
        elif action=='down':down()
        elif action=='check':check()
        elif action=='check-run':check(RUN)
        elif action=='cleanup':remove_resources();check()
        elif action=='cleanup-dry':remove_resources(dry=True)
        elif action=='inventory':print(json.dumps(inventory(),indent=2))
        elif action=='fail':raise RuntimeError('Intentional workload failure')
        elif action=='hold':
            print('HOLD READY',RUN,flush=True)
            while True:time.sleep(1)
        else:raise ValueError(action)
    except BaseException as e:
        print(type(e).__name__+': '+str(e),file=sys.stderr,flush=True)
        sys.exit(1)
