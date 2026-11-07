"""RW0 P2 harness. Never deletes host files, prunes images or touches other runs."""
import ctypes, datetime, hashlib, hmac, json, os, pathlib, re, shutil, signal
import subprocess, sys, time, urllib.request

ROOT = pathlib.Path(__file__).resolve().parent
CRATE = ROOT.parent
TASK = pathlib.Path(sys.argv[2]).resolve()
if not TASK.as_posix().lower().startswith('j:/gigglesdb-work/'):
    raise RuntimeError('all generated output must live in J:/gigglesdb-work/<unique-task>')
RUN = sys.argv[3] if len(sys.argv) > 3 and sys.argv[3] else 'p2-' + datetime.datetime.now().strftime('%Y%m%d-%H%M%S') + '-' + str(os.getpid())
if not re.fullmatch('[a-z0-9][a-z0-9-]{0,62}', RUN): raise ValueError('invalid run id')
LOCK = json.loads((CRATE/'artifacts/spikes/s4-harness/images.lock').read_text())['images']
ENV = os.environ.copy()
ENV.update(CARGO_TARGET_DIR=str(TASK/'target'),TEMP=str(TASK/'tmp'),TMP=str(TASK/'tmp'),CARGO_BUILD_JOBS='2',PYTHONDONTWRITEBYTECODE='1',RUN_ID=RUN,SKEDADDLE_TEST_ROOT=str(TASK/'scratch'))
for service,key in [('pg','pg18'),('minio','minio'),('gcs','gcs'),('toxiproxy','toxiproxy'),('faults','probe')]: ENV[service.upper()+'_IMAGE']=LOCK[key]['ref']

def command(args, check=True, timeout=180):
    result=subprocess.run([str(a) for a in args],cwd=CRATE,env=ENV,text=True,encoding='utf8',errors='replace',capture_output=True,timeout=timeout)
    if check and result.returncode: raise RuntimeError(f'{args}: exit={result.returncode}\n{result.stdout}\n{result.stderr}')
    return result

def docker(*args, **kwargs): return command(['docker',*args],**kwargs)
def compose(*args, **kwargs): return docker('compose','-f',ROOT/'compose.yaml','-p','skedaddle-'+RUN,*args,**kwargs)
def save(name,value):
    with (TASK/name).open('x',encoding='utf8') as f: f.write(value if isinstance(value,str) else json.dumps(value,indent=2))

def resources(run=None):
    filt='label=skedaddle.test'+('='+run if run else '')
    return {kind:docker(*args,'--filter',filt).stdout.split() for kind,args in [('container',['ps','-aq']),('volume',['volume','ls','-q']),('network',['network','ls','-q'])]}

def check():
    found=resources()
    print('no_leftover_test_resources',json.dumps(found),flush=True)
    if any(found.values()): raise RuntimeError('labeled test resources remain; other runs are preserved')

def down(run=RUN):
    # Enumerate by exact task label, then validate label and compose project before each teardown.
    for kind,ids in resources(run).items():
        for ident in ids:
            obj=json.loads(docker('inspect',ident).stdout)[0]
            labels=(obj.get('Config',{}).get('Labels') if kind=='container' else obj.get('Labels')) or {}
            if labels.get('skedaddle.test')!=run or labels.get('com.docker.compose.project')!='skedaddle-'+run:
                raise RuntimeError('refusing a resource outside this labeled compose project: '+ident)
            args={'container':['rm','-f'],'volume':['volume','rm'],'network':['network','rm']}[kind]
            docker(*args,ident)

def alive(pid):
    if os.name!='nt':
        try: os.kill(pid,0);return True
        except ProcessLookupError: return False
        except PermissionError: return True
    kernel=ctypes.WinDLL('kernel32',use_last_error=True)
    kernel.OpenProcess.restype=ctypes.c_void_p
    handle=kernel.OpenProcess(0x1000,False,pid)
    if not handle:return ctypes.get_last_error()==5
    status=ctypes.c_ulong()
    try:return bool(kernel.GetExitCodeProcess(ctypes.c_void_p(handle),ctypes.byref(status)) and status.value==259)
    finally:kernel.CloseHandle(ctypes.c_void_p(handle))

def sweep_own_orphans():
    # Only owner records inside THIS task directory authorize an automatic sweep.
    for path in TASK.glob('owner-*.json'):
        owner=json.loads(path.read_text())
        run=owner['run_id']
        if owner.get('task_root')!=str(TASK) or not re.fullmatch('[a-z0-9][a-z0-9-]{0,62}',run):
            raise RuntimeError('invalid task owner record; preserve resources for inspection')
        if run!=RUN and not alive(int(owner['pid'])):down(run)

def guard():
    # Host guard precedes ANY created Docker resource and all image pulls.
    missing=[]
    for key in ['pg18','minio','gcs','toxiproxy','probe']:
        if docker('image','inspect',LOCK[key]['ref'],check=False).returncode: missing.append(key)
    needed=4*1024**3+3*sum(LOCK[k]['size_bytes'] for k in missing)
    free=shutil.disk_usage('C:/').free
    print(f'DISK GUARD C: needed={needed} free={free}',flush=True)
    if free<needed: raise RuntimeError('disk guard refused C: headroom; no pruning is performed')
    if shutil.disk_usage(TASK).free<8*1024**3: raise RuntimeError('J: lacks build/test headroom')
    if 'probe' in missing: docker('pull',LOCK['probe']['ref'])
    name='skedaddle-'+RUN+'-guard'
    try:
        probe=docker('run','--name',name,'--network','none','--label','skedaddle.test='+RUN,'--label','com.docker.compose.project=skedaddle-'+RUN,LOCK['probe']['ref'],'df','-Pk','/')
        free=int(probe.stdout.splitlines()[-1].split()[3])*1024
        print(f'DISK GUARD Docker VM: needed={needed} free={free}',flush=True)
        if free<needed: raise RuntimeError('disk guard refused Docker VM headroom')
    finally: down()

def endpoint(service,port):
    address=compose('port',service,str(port)).stdout.strip()
    return 'http://'+address

def http(url,method='GET',body=None,headers=None):
    request=urllib.request.Request(url,data=body,method=method,headers=headers or {})
    with urllib.request.urlopen(request,timeout=10) as response: return response.read()

def bucket(url):
    host=url.split('//')[1];path='/p2-bucket'
    now=datetime.datetime.now(datetime.timezone.utc);stamp=now.strftime('%Y%m%dT%H%M%SZ');day=stamp[:8]
    payload=hashlib.sha256(b'').hexdigest();signed='host;x-amz-content-sha256;x-amz-date'
    canonical=f'PUT\n{path}\n\nhost:{host}\nx-amz-content-sha256:{payload}\nx-amz-date:{stamp}\n\n{signed}\n{payload}'
    scope=day+'/us-east-1/s3/aws4_request';message='AWS4-HMAC-SHA256\n'+stamp+'\n'+scope+'\n'+hashlib.sha256(canonical.encode()).hexdigest();key=b'AWS4skedaddle-secret'
    for x in [day,'us-east-1','s3','aws4_request']:key=hmac.new(key,x.encode(),hashlib.sha256).digest()
    auth='AWS4-HMAC-SHA256 Credential=skedaddle/'+scope+', SignedHeaders='+signed+', Signature='+hmac.new(key,message.encode(),hashlib.sha256).hexdigest()
    http(url+path,'PUT',b'',{'x-amz-date':stamp,'x-amz-content-sha256':payload,'Authorization':auth})

def cargo(args,name,timeout=7200):
    started=time.monotonic();result=command(['cargo',*args],check=False,timeout=timeout)
    save(name+'.log',result.stdout+'\n'+result.stderr)
    print(name,'exit',result.returncode,'seconds',round(time.monotonic()-started,3),flush=True)
    print(result.stdout[-12000:]+result.stderr[-3000:],flush=True)
    return result.returncode

def audit_scratch():
    # Product cleanup is allowed; the harness itself retains all host evidence.
    # Only the two real subprocess-crash fixtures intentionally leave orphans.
    leaks=[];orphans=[]
    for path in (TASK/'scratch').rglob('*'):
        rel=path.relative_to(TASK/'scratch')
        suspicious=(re.search(r'#\d+$',path.name) or '.staging-' in path.name
                    or path.name.endswith(('.export','.skedaddle-scratch')))
        if not suspicious:continue
        parts=rel.parts
        object_orphan=(parts[0].startswith('p2-staging-crash-object-') and len(parts)==2
                       and parts[1]=='unfinished.json#1' and path.is_file())
        export_orphan=(parts[0].startswith('p2-staging-crash-export-') and len(parts) in (2,3)
                       and parts[1]=='db.skedaddle-scratch'
                       and ((len(parts)==2 and path.is_dir()) or
                            (len(parts)==3 and re.fullmatch(r'archive\.export\.staging-[0-9a-f]{32}',parts[2]) and path.is_file())))
        (orphans if object_orphan or export_orphan else leaks).append(str(rel))
    save('scratch-audit-'+RUN+'.json',{'unexpected':leaks,'reported_crash_orphans':orphans})
    print('scratch audit unexpected',len(leaks),'reported crash orphan paths',len(orphans),flush=True)
    if leaks:raise RuntimeError('product left unexpected staging/temp files: '+repr(leaks))

def p2(mutate=False,backend_only=False):
    guard()
    sweep_own_orphans()
    for directory in ['scratch','tmp']: (TASK/directory).mkdir(exist_ok=True)
    started=False;status=0
    def trap(signum,frame): raise KeyboardInterrupt('signal '+str(signum))
    signal.signal(signal.SIGINT,trap);signal.signal(signal.SIGTERM,trap)
    try:
        save('owner-'+RUN+'.json',{'run_id':RUN,'pid':os.getpid(),'task_root':str(TASK)})
        started=True
        result=compose('up','-d','--wait','--wait-timeout','180',check=False,timeout=240)
        save('compose-'+RUN+'.log',result.stdout+result.stderr)
        if result.returncode: raise RuntimeError('compose startup failed')
        cfg={'run_id':RUN,'minio':endpoint('minio',9000),'gcs':endpoint('gcs',4443),'toxiproxy':endpoint('toxiproxy',8474),'proxy_minio':endpoint('toxiproxy',9002),'proxy_errors':endpoint('toxiproxy',9003)}
        cfg['pg']='host=127.0.0.1 port='+endpoint('pg',5432).rsplit(':',1)[1]+' user=postgres password=skedaddle dbname=skedaddle'
        cfg['proxy_pg']='host=127.0.0.1 port='+endpoint('toxiproxy',9004).rsplit(':',1)[1]+' user=postgres password=skedaddle dbname=skedaddle'
        bucket(cfg['minio'])
        http(cfg['gcs']+'/storage/v1/b?project=p2','POST',b'{"name":"p2-bucket"}',{'Content-Type':'application/json'})
        http(cfg['toxiproxy']+'/proxies','POST',json.dumps({'name':'minio','listen':'0.0.0.0:9002','upstream':'minio:9000','enabled':True}).encode(),{'Content-Type':'application/json'})
        http(cfg['toxiproxy']+'/proxies','POST',json.dumps({'name':'http-errors','listen':'0.0.0.0:9003','upstream':'faults:8080','enabled':True}).encode(),{'Content-Type':'application/json'})
        http(cfg['toxiproxy']+'/proxies','POST',json.dumps({'name':'pg','listen':'0.0.0.0:9004','upstream':'pg:5432','enabled':True}).encode(),{'Content-Type':'application/json'})
        config_name='config-'+RUN+'.json';save(config_name,cfg);ENV['SKEDADDLE_TEST_CONFIG']=str(TASK/config_name)
        if mutate:
            status |= mutants()
        elif backend_only:
            status |= cargo(['test','--locked','--features','test-crash','-j2','--test','coordinator','--test','coordinator_crash','--','--ignored','--skip','journal_long_run','--nocapture','--test-threads=1'],'p2-backends-'+RUN)
        else:
            status |= cargo(['test','--locked','--features','test-crash','-j2'],'p2-plain-'+RUN)
            status |= cargo(['test','--locked','--features','test-crash','-j2','--test','coordinator','--test','coordinator_crash','--','--ignored','--nocapture','--test-threads=1'],'p2-docker-'+RUN)
        audit_scratch()
        # Bound Docker data before teardown; journal/scratch bytes are asserted in Rust.
        sizes={}
        for service,path in [('pg','/var/lib/postgresql'),('minio','/data'),('gcs','/storage')]:
            cid=compose('ps','-q',service).stdout.strip()
            sizes[service]=int(docker('exec',cid,'du','-sk',path).stdout.split()[0])*1024
        save('data-size-'+RUN+'.json',sizes)
        if sum(sizes.values())>1_000_000_000: raise RuntimeError('Docker data cap exceeded')
    finally:
        if started:
            try:
                for ident in resources(RUN)['container']:
                    logs=docker('logs','--tail','1000',ident,check=False)
                    save('docker-'+ident[:12]+'-'+RUN+'.log',(logs.stdout+logs.stderr)[-1024*1024:])
            finally: down()
    return status

def mutants():
    # Installed P0 tool; source scratch lives on J: and --leak-dirs preserves it.
    tool=CRATE/'artifacts/spikes/s1-replication/cargo-home/bin/cargo-mutants.exe'
    result=command([tool,'mutants','--version']);print(result.stdout,flush=True)
    if '27.1.0' not in result.stdout: raise RuntimeError('cargo-mutants 27.1.0 required')
    io_re=r'Journal::(?:create|put|read)\b'
    # Disjoint source scopes cover every mutant. Each scope runs the relevant
    # real-store/PG/crash or pure/reference-model oracles, without repeating
    # multi-second network fault setup for unrelated arithmetic mutations.
    suites=[
        ('state','mod.rs',None,None,['only_legal_transitions','every_readiness_regression','entry_hash_and_semantic','complete_projection','coordinator_errors','ownership_observers','retry_effect','local_apply_is_atomic','local_transaction_rolls_back','broken_chain_or_gap','clock_regression']),
        ('epoch','epoch.rs',None,None,['coordinator::epoch::tests','ownership_observers','retry_effect','owned_coordinator_steps','coordinator_checkpoints','effect_errors_revoke_ownership','stale_epoch_effect','crash_after_put','rebuild_on_new_host','pg_crash_effect_adoption','pg_commit_response_loss','pg_effect_namespaces','pg_authority_schema','local_two_coordinators_race']),
        ('journal-chain','journal.rs',None,io_re,['coordinator::journal::tests','entry_hash','broken_chain','checkpoint','full_scrub','malformed','local_two_coordinators_race','clock_regression']),
        ('journal-io','journal.rs',io_re,None,['two_coordinators_race_one_wins','owned_coordinator_steps','object_store_latency','checkpoints_do_not_hide']),
        ('gcs','gcs.rs',None,None,['coordinator::gcs::tests','two_coordinators_race_one_wins','gcs_json_create_paging']),
    ]
    chosen=ENV.get('SKEDADDLE_MUTATION_SUITE','all')
    if chosen!='all':suites=[s for s in suites if s[0]==chosen]
    if not suites:raise RuntimeError('unknown mutation suite')
    source_files=[CRATE/'src/coordinator'/name for name in ['mod.rs','epoch.rs','journal.rs','gcs.rs']]
    hashes={str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in source_files}
    save('mutation-source-hashes-'+RUN+'.json',hashes)
    inventory_args=[tool,'mutants','--list','--features','test-crash']
    for path in source_files:inventory_args+=['--file','src/coordinator/'+path.name]
    if ENV.get('SKEDADDLE_MUTANTS_FILTER'):inventory_args+=['--re',ENV['SKEDADDLE_MUTANTS_FILTER']]
    names=command(inventory_args).stdout.splitlines()
    inventory={name:[] for name in ['state','epoch','journal-chain','journal-io','gcs']}
    for name in names:
        path=name.split(':',1)[0].replace('\\','/')
        group={'src/coordinator/mod.rs':'state','src/coordinator/epoch.rs':'epoch','src/coordinator/gcs.rs':'gcs'}.get(path)
        if path=='src/coordinator/journal.rs':group='journal-io' if re.search(io_re,name) else 'journal-chain'
        if group:inventory[group].append(name)
    save('mutation-inventory-'+RUN+'.json',inventory)
    status=0
    for name,file,include,exclude,filters in suites:
        output=TASK/('mutations-'+RUN+'-'+name)
        if output.exists():raise RuntimeError('fresh mutation output required; never rotate/delete evidence')
        args=[tool,'mutants','--in-place','--leak-dirs','--jobserver-tasks','2','--output',output,'--file','src/coordinator/'+file,'--features','test-crash','--timeout','45',
              '--cargo-arg=--lib','--cargo-arg=--test=coordinator','--cargo-arg=--test=coordinator_crash']
        include=ENV.get('SKEDADDLE_MUTANTS_FILTER') or include
        if include:args+=['--re',include]
        if exclude:args+=['--exclude-re',exclude]
        args+=['--','--','--include-ignored','--skip','journal_long_run','--skip','paging_exceeds_1024_without_losing_rows','--test-threads=1',*filters]
        result=command(args,check=False,timeout=43200)
        save('mutations-'+RUN+'-'+name+'.log',result.stdout+result.stderr)
        print(name,result.stdout+result.stderr,flush=True)
        if any(hashlib.sha256(pathlib.Path(p).read_bytes()).hexdigest()!=digest for p,digest in hashes.items()):raise RuntimeError('mutation source restoration hash mismatch')
        mutants_path=output/'mutants.out/mutants.json'
        if mutants_path.exists() and not ENV.get('SKEDADDLE_MUTANTS_FILTER'):
            raw=json.loads(mutants_path.read_text());count=len(raw) if isinstance(raw,list) else len(raw['mutants'])
            if count!=len(inventory[name]):raise RuntimeError('mutation inventory coverage mismatch for '+name)
        status|=result.returncode
        # 2=survivors, 3=mutant timeout. Both require review, but must not
        # silently prevent later disjoint source scopes from running.
        if result.returncode not in (0,2,3):return status
    return status

if __name__=='__main__':
    action=sys.argv[1]
    code=0
    try:
        if action=='p2':code=p2()
        elif action=='p2-backends':code=p2(backend_only=True)
        elif action=='guard':guard()
        elif action=='check':check()
        elif action=='down':down()
        elif action=='mutants':code=p2(mutate=True)
        else:raise ValueError(action)
    finally:
        if action!='check':check()
    sys.exit(code)
