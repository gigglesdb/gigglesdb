"""RW0 P2 harness. Never deletes host files, prunes images or touches other runs."""
import ctypes, datetime, hashlib, hmac, json, os, pathlib, re, shutil, signal
import subprocess, sys, time, urllib.request, socket, ssl, struct
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import threading, urllib.parse

ROOT = pathlib.Path(__file__).resolve().parent
CRATE = ROOT.parent
TASK = pathlib.Path(sys.argv[2]).resolve()
if not TASK.as_posix().lower().startswith('j:/gigglesdb-work/'):
    raise RuntimeError('all generated output must live in J:/gigglesdb-work/<unique-task>')
SCRATCH = TASK/('p3-scratch' if sys.argv[1]=='p3' else 'scratch')
RUN = sys.argv[3] if len(sys.argv) > 3 and sys.argv[3] else 'p2-' + datetime.datetime.now().strftime('%Y%m%d-%H%M%S') + '-' + str(os.getpid())
if not re.fullmatch('[a-z0-9][a-z0-9-]{0,62}', RUN): raise ValueError('invalid run id')
LOCK = json.loads((CRATE/'artifacts/spikes/s4-harness/images.lock').read_text())['images']
LOCK.update(json.loads((ROOT/'images.lock').read_text())['images'])
ENV = os.environ.copy()
ENV.update(CARGO_TARGET_DIR=os.environ.get('CARGO_TARGET_DIR',str(TASK/'target')),TEMP=str(TASK/'tmp'),TMP=str(TASK/'tmp'),CARGO_BUILD_JOBS='4',PYTHONDONTWRITEBYTECODE='1',RUN_ID=RUN,SKEDADDLE_TEST_ROOT=str(SCRATCH),
           RUSTC_WRAPPER='J:/gigglesdb-work/tools/bin/sccache.exe',SCCACHE_DIR='J:/gigglesdb-work/sccache-cache',SCCACHE_CACHE_SIZE='100G',CARGO_INCREMENTAL='0')
for service,key in [('pg','pg18'),('minio','minio'),('gcs','gcs'),('toxiproxy','toxiproxy'),('faults','probe')]: ENV[service.upper()+'_IMAGE']=LOCK[key]['ref']
PG_VERSION=ENV.get('SKEDADDLE_PG_VERSION','16')
if PG_VERSION not in ('16','17','18'): raise ValueError('PG version must be 16, 17 or 18')
if sys.argv[1]=='pg-matrix':
    ENV.update(PG_MATRIX_IMAGE=LOCK['pg'+PG_VERSION]['ref'])

def command(args, check=True, timeout=180):
    result=subprocess.run([str(a) for a in args],cwd=CRATE,env=ENV,text=True,encoding='utf8',errors='replace',capture_output=True,timeout=timeout)
    if check and result.returncode: raise RuntimeError(f'{args}: exit={result.returncode}\n{result.stdout}\n{result.stderr}')
    return result

def docker(*args, **kwargs): return command(['docker',*args],**kwargs)
def compose(*args, **kwargs): return docker('compose','-f',ROOT/'compose.yaml','-p','skedaddle-'+RUN,*args,**kwargs)
def save(name,value):
    with (TASK/name).open('x',encoding='utf8',newline='\n') as f: f.write(value if isinstance(value,str) else json.dumps(value,indent=2))

def resources(run=None):
    filt='label=skedaddle.test'+('='+run if run else '')
    return {kind:docker(*args,'--filter',filt).stdout.split() for kind,args in [('container',['ps','-aq']),('volume',['volume','ls','-q']),('network',['network','ls','-q'])]}

def check(run=None):
    found=resources(run)
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
    keys = ['pg'+PG_VERSION,'probe'] if sys.argv[1]=='pg-matrix' else ['pg18','minio','gcs','toxiproxy','probe']
    if sys.argv[1] == 'linux-fs': keys.append('rust')
    for key in keys:
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
    # Exclusive across GD-0049 lanes and coordinators. Never steal a stale lock:
    # its owner must first prove that its entire process tree has stopped.
    lock=pathlib.Path('J:/gigglesdb-work/gd49-lock/heavy-cargo.lock')
    lock.parent.mkdir(parents=True,exist_ok=True)
    while True:
        try:
            with lock.open('x',encoding='utf8') as stream:
                stream.write(json.dumps({'task':str(TASK),'run':RUN,'pid':os.getpid()}))
            break
        except FileExistsError:
            print('waiting for heavy cargo lock',flush=True)
            time.sleep(5)
    started=time.monotonic()
    child=None
    release=True
    try:
        args=['-j4' if arg=='-j2' else arg for arg in args]
        child=subprocess.Popen(['cargo',*args],cwd=CRATE,env=ENV,text=True,
                               encoding='utf8',errors='replace',stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        try:
            stdout,stderr=child.communicate(timeout=timeout)
        except BaseException:
            # Stopping a wrapper alone leaves rustc/link/test descendants alive.
            if os.name=='nt':
                release=False
                snapshot=subprocess.run(['powershell','-NoProfile','-Command',
                    'Get-CimInstance Win32_Process | Select-Object ProcessId,ParentProcessId | ConvertTo-Json -Compress'],
                    env=ENV,text=True,capture_output=True,check=True)
                processes=json.loads(snapshot.stdout)
                descendants={child.pid}
                while True:
                    found={int(p['ProcessId']) for p in processes if int(p['ParentProcessId']) in descendants}
                    if found.issubset(descendants):break
                    descendants.update(found)
                for pid in descendants:
                    subprocess.run(['taskkill','/T','/F','/PID',str(pid)],env=ENV,
                                   stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,check=False)
                deadline=time.monotonic()+30
                while any(alive(pid) for pid in descendants) and time.monotonic()<deadline:time.sleep(0.1)
                release=not any(alive(pid) for pid in descendants)
                if not release:raise RuntimeError('cargo descendants remain; heavy lock retained for inspection')
            else:
                # This harness's heavy-cargo path is a Windows host lane.
                child.kill()
            child.communicate()
            raise
        result=subprocess.CompletedProcess(child.args,child.returncode,stdout,stderr)
    finally:
        if release and (child is None or child.poll() is not None):
            lock.unlink()
    save(name+'.log',result.stdout+'\n'+result.stderr)
    print(name,'exit',result.returncode,'seconds',round(time.monotonic()-started,3),flush=True)
    print(result.stdout[-12000:]+result.stderr[-3000:],flush=True)
    return result.returncode

def audit_scratch():
    # Product cleanup is allowed; the harness itself retains all host evidence.
    # Only the two real subprocess-crash fixtures intentionally leave orphans.
    leaks=[];orphans=[]
    for path in SCRATCH.rglob('*'):
        rel=path.relative_to(SCRATCH)
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

def start_encryption_capture():
    # Fault injection only: no credentials, payloads or arbitrary headers retained.
    records=[]
    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*args):pass
        def handle_request(self):
            if self.path=='/capture':
                body=json.dumps(records).encode();self.send_response(200)
            else:
                length=int(self.headers.get('Content-Length','0'))
                while length:
                    part=self.rfile.read(min(length,65536))
                    if not part:break
                    length-=len(part)
                headers={k.lower():v for k,v in self.headers.items() if k.lower().startswith('x-amz-server-side-encryption')}
                records.append({'method':self.command,'headers':headers,'query':urllib.parse.parse_qs(urllib.parse.urlsplit(self.path).query)})
                del records[:-128]
                body=b'';self.send_response(503)
            self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body)
        do_GET=do_POST=do_PUT=handle_request
    server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
    thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    return server,'http://127.0.0.1:'+str(server.server_port)

def p2(mutate=False,backend_only=False,p3=False):
    guard()
    sweep_own_orphans()
    for directory in [SCRATCH,TASK/'tmp']: directory.mkdir(exist_ok=True)
    started=False;status=0;capture=None
    def trap(signum,frame): raise KeyboardInterrupt('signal '+str(signum))
    signal.signal(signal.SIGINT,trap);signal.signal(signal.SIGTERM,trap)
    try:
        save('owner-'+RUN+'.json',{'run_id':RUN,'pid':os.getpid(),'task_root':str(TASK)})
        started=True
        result=compose('up','-d','--wait','--wait-timeout','180',check=False,timeout=240)
        save('compose-'+RUN+'.log',result.stdout+result.stderr)
        if result.returncode: raise RuntimeError('compose startup failed')
        cfg={'run_id':RUN,'minio':endpoint('minio',9000),'gcs':endpoint('gcs',4443),'toxiproxy':endpoint('toxiproxy',8474),'proxy_minio':endpoint('toxiproxy',9002),'proxy_errors':endpoint('toxiproxy',9003)}
        if p3:capture,cfg['capture']=start_encryption_capture()
        cfg['pg']='host=127.0.0.1 port='+endpoint('pg',5432).rsplit(':',1)[1]+' user=postgres password=skedaddle dbname=skedaddle'
        cfg['proxy_pg']='host=127.0.0.1 port='+endpoint('toxiproxy',9004).rsplit(':',1)[1]+' user=postgres password=skedaddle dbname=skedaddle'
        bucket(cfg['minio'])
        http(cfg['gcs']+'/storage/v1/b?project=p2','POST',b'{"name":"p2-bucket"}',{'Content-Type':'application/json'})
        http(cfg['toxiproxy']+'/proxies','POST',json.dumps({'name':'minio','listen':'0.0.0.0:9002','upstream':'minio:9000','enabled':True}).encode(),{'Content-Type':'application/json'})
        http(cfg['toxiproxy']+'/proxies','POST',json.dumps({'name':'http-errors','listen':'0.0.0.0:9003','upstream':'faults:8080','enabled':True}).encode(),{'Content-Type':'application/json'})
        http(cfg['toxiproxy']+'/proxies','POST',json.dumps({'name':'pg','listen':'0.0.0.0:9004','upstream':'pg:5432','enabled':True}).encode(),{'Content-Type':'application/json'})
        config_name='config-'+RUN+'.json';save(config_name,cfg);ENV['SKEDADDLE_TEST_CONFIG']=str(TASK/config_name)
        if p3:
            status |= cargo(['test','--locked','-j2','--test','buffer_backends','--','--include-ignored','--nocapture','--test-threads=1'],'p3-backends-'+RUN)
            status |= cargo(['test','--locked','--features','test-crash','-j2','--test','buffer_writer','--','--include-ignored','--nocapture','--test-threads=1'],'p3-writer-'+RUN)
            status |= cargo(['test','--locked','-j2','--test','buffer_reader','--test','buffer_gc','--','--ignored','--nocapture','--test-threads=1'],'p3-reader-gc-'+RUN)
        elif mutate:
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
        if capture:capture.shutdown();capture.server_close()
        if started:
            try:
                for ident in resources(RUN)['container']:
                    logs=docker('logs','--tail','1000',ident,check=False)
                    save('docker-'+ident[:12]+'-'+RUN+'.log',(logs.stdout+logs.stderr)[-1024*1024:])
            finally: down()
    return status

PG_MATRIX_SERVICES=('pg-source','pg-destination','pg-replica-wal')
PG_MATRIX_TESTS=('pg_replication','pg_source','pg_apply','pg_preflight','pg_chunk','pg_catalog',
                 'pg_ddl','pg_fence','pg_activate','pg_reverse','verify_pg','cutover','rollback',
                 'runner','runner_verify','engine','source_ack','mysql_dest_schema')


def pg_test_selections():
    raw=ENV.get('SKEDADDLE_PG_TEST_SELECTION')
    if raw is None: return [(binary,None) for binary in PG_MATRIX_TESTS]
    requested=json.loads(raw)
    if not isinstance(requested,list) or not requested:
        raise ValueError('PG test selection must be a nonempty list')
    selected=[]
    for item in requested:
        if not isinstance(item,str): raise ValueError('PG test selection must be text')
        binary,separator,selection=item.partition('::')
        if binary not in PG_MATRIX_TESTS or (separator and not selection):
            raise ValueError('invalid PG binary or test filter: '+item)
        selected.append((binary,selection if separator else None))
    return selected


def pg_matrix_config(ports, ca_paths):
    """One config contract, with env-only legacy gates also recorded in JSON.

    sslmode=require is supported by tokio-postgres's DSN parser. Its TLS
    connector must load ca_pem and validate localhost; NoTls cannot use it.
    libpq/psql additionally receives PGSSLMODE=verify-full and a public bundle.
    """
    def dsn(side, role='skedaddle_fixture', database='skedaddle'):
        password={'postgres':'skedaddle','skedaddle_fixture':'fixture-test',
                  'skedaddle_setup':'setup-test',
                  'skedaddle_capture':'capture-test','skedaddle_fence':'fence-test',
                  'skedaddle_apply':'apply-test','skedaddle_verify':'verify-test'}[role]
        return f'host=localhost hostaddr=127.0.0.1 port={ports[side]} user={role} password={password} dbname={database} sslmode=require'
    source=dsn('pg-source','skedaddle_setup');destination=dsn('pg-destination','skedaddle_setup')
    admin_source=dsn('pg-source','postgres');admin_destination=dsn('pg-destination','postgres')
    legacy={
        'SKEDADDLE_PG_DDL_URL':admin_source,
        'SKEDADDLE_PG_FENCE_DSN':admin_source,
        'SKEDADDLE_PG_ACTIVATE_DSN':admin_destination,
        'SKEDADDLE_PG_REVERSE_SOURCE_URL':admin_source,
        'SKEDADDLE_PG_REVERSE_DESTINATION_URL':admin_destination,
        'SKEDADDLE_VERIFY_SOURCE_URL':source,
        'SKEDADDLE_VERIFY_DESTINATION_URL':destination,
        'SKEDADDLE_CUTOVER_SOURCE_URL':admin_source,
        'SKEDADDLE_CUTOVER_DESTINATION_URL':admin_destination,
        'SKEDADDLE_ROLLBACK_PG_SOURCE_URL':dsn('pg-source','postgres','skedaddle_rollback'),
        'SKEDADDLE_ROLLBACK_PG_DESTINATION_URL':dsn('pg-destination','postgres','skedaddle_rollback'),
    }
    cfg={'run_id':RUN,'pg_version':int(PG_VERSION),'pg':dsn('pg-source'),
         'pg_preflight':dsn('pg-source'),'pg_replica_wal':dsn('pg-replica-wal'),
         'pg_chunk':dsn('pg-source','skedaddle_capture'),'pg_apply':admin_destination,
         'pg_ddl':admin_source,'pg_fence':admin_source,'pg_activate':admin_destination,
         'source':source,'destination':destination,'env':legacy,
         'ca_pem':str(TASK/'pg-ca-bundle.pem'),'roles':{}}
    for name in ('pg_reverse','verify_pg','cutover','rollback','engine'):
        privileged=name in ('pg_reverse','cutover','rollback')
        cfg[name]={'source':admin_source if privileged else source,
                   'destination':admin_destination if privileged else destination}
    cfg['rollback']={side:legacy['SKEDADDLE_ROLLBACK_PG_'+side.upper()+'_URL'] for side in ('source','destination')}
    for side in ('pg-source','pg-destination'):
        cfg['roles'][side]={name:dsn(side,'skedaddle_'+name)
                            for name in ('capture','fence','apply','verify')}
    for key,side in [('replication','pg-source'),('destination_replication','pg-destination')]:
        cfg[key]={'host':'127.0.0.1','port':ports[side],'server_name':'localhost',
                  'user':'skedaddle_capture','password':'capture-test',
                  'database':'skedaddle','ca_pem':str(ca_paths[side])}
    return cfg


def pg_matrix_probe(service, port, ca_path):
    # Assert genuine PostgreSQL TLS negotiation and hostname/CA validation.
    context=ssl.create_default_context(cafile=str(ca_path))
    with socket.create_connection(('127.0.0.1',port),timeout=10) as raw:
        raw.sendall(struct.pack('!II',8,80877103))
        if raw.recv(1)!=b'S': raise RuntimeError(service+' refused TLS')
        with context.wrap_socket(raw,server_hostname='localhost') as secured:
            tls_version=secured.version()
    cid=compose('ps','-q',service).stdout.strip()
    base=['exec','-e','PGPASSWORD=skedaddle',cid,'psql','-X','-v','ON_ERROR_STOP=1']
    rejected=docker(*base,'host=127.0.0.1 user=postgres dbname=skedaddle sslmode=disable','-Atqc','SELECT 1',check=False)
    if rejected.returncode==0 or 'rejects connection' not in rejected.stderr:
        raise RuntimeError(service+' did not explicitly reject plaintext')
    # Request password authentication over TCP; local trust never proves SCRAM.
    conn='host=127.0.0.1 user=postgres dbname=skedaddle sslmode=verify-full sslrootcert=/var/lib/postgresql/h1/tls/ca.pem'
    settings=docker(*base,conn,'-Atqc',
        "SELECT current_setting('server_version_num'),current_setting('wal_level'),current_setting('password_encryption'),ssl FROM pg_stat_ssl WHERE pid=pg_backend_pid()")
    version,wal,password,tls=settings.stdout.strip().split('|')
    expected='replica' if service=='pg-replica-wal' else 'logical'
    if int(version)//10000!=int(PG_VERSION) or wal!=expected or password!='scram-sha-256' or tls!='t':
        raise RuntimeError(service+' TLS/SCRAM/version/WAL settings mismatch')
    # Verify all TCP allow rules are hostssl SCRAM, and all passwords are SCRAM.
    auth=docker(*base,conn,'-Atqc',
        "SELECT NOT EXISTS(SELECT 1 FROM pg_hba_file_rules WHERE error IS NOT NULL OR (type LIKE 'host%' AND auth_method<>'reject' AND (type<>'hostssl' OR auth_method<>'scram-sha-256'))), NOT EXISTS(SELECT 1 FROM pg_authid WHERE rolcanlogin AND (rolpassword IS NULL OR rolpassword NOT LIKE 'SCRAM-SHA-256$%'))")
    if auth.stdout.strip()!='t|t': raise RuntimeError(service+' authentication policy mismatch')
    return {'server_version_num':int(version),'wal_level':wal,'tls':tls_version,
            'password_encryption':password,'plaintext_rejected':True,'scram_policy':True}


def pg_client_precheck():
    lock=json.loads((ROOT/'pg-client.lock').read_text())
    ENV['SKEDADDLE_PSQL']=lock['executable']
    try:
        result=command([ENV['SKEDADDLE_PSQL'],'--version'],timeout=15)
        if result.stdout.strip()!=lock['psql_version']:
            raise RuntimeError('unexpected client version: '+result.stdout.strip())
    except (OSError, RuntimeError, subprocess.TimeoutExpired) as error:
        raise RuntimeError('PG matrix requires pinned native psql; run harness/provision-pg-client.ps1 '
                           '-TaskRoot <fresh J:/gigglesdb-work/task> before retrying: '+str(error)) from error
    print('host client: '+result.stdout.strip(),flush=True)


def pg_matrix():
    """Future Docker lane only. Static acceptance never invokes this function."""
    selections=pg_test_selections()  # Reject bad selections before fixture startup.
    pg_client_precheck()  # Read-only, before any database mutation or fixture startup.
    guard()  # Includes C:/J:/Docker-VM headroom, before fixture startup/pulls.
    for directory in (SCRATCH,TASK/'tmp'): directory.mkdir(exist_ok=True)
    started=False;status=0
    def trap(signum,frame): raise KeyboardInterrupt('signal '+str(signum))
    signal.signal(signal.SIGINT,trap);signal.signal(signal.SIGTERM,trap)
    try:
        save('owner-'+RUN+'.json',{'run_id':RUN,'pid':os.getpid(),'task_root':str(TASK)})
        started=True
        result=compose('--profile','pg-matrix','up','-d','--wait','--wait-timeout','240',
                       *PG_MATRIX_SERVICES,check=False,timeout=300)
        save('compose-'+RUN+'.log',result.stdout+result.stderr)
        if result.returncode: raise RuntimeError('TLS PostgreSQL matrix startup failed')
        ports={side:int(endpoint(side,5432).rsplit(':',1)[1]) for side in PG_MATRIX_SERVICES}
        ca_paths={side:TASK/(side+'-ca.pem') for side in PG_MATRIX_SERVICES}
        provenance={};probes={}
        for side in PG_MATRIX_SERVICES:
            cid=compose('ps','-q',side).stdout.strip()
            # Export only the public trust anchor. Private keys never leave Docker.
            save(ca_paths[side].name,docker('exec',cid,'cat','/var/lib/postgresql/h1/tls/ca.pem').stdout)
            provenance[side]={name:docker('exec',cid,'cat','/var/lib/postgresql/h1/tools/'+name).stdout
                              for name in ('openssl-package-url.txt','openssl-index-entry.txt')}
            probes[side]=pg_matrix_probe(side,ports[side],ca_paths[side])
        save('openssl-provenance-'+RUN+'.json',provenance)
        save('tls-policy-'+RUN+'.json',probes)
        save('pg-ca-bundle.pem',''.join(path.read_text() for path in ca_paths.values()))
        cfg=pg_matrix_config(ports,ca_paths)
        config_path=pathlib.Path(ENV.get('SKEDADDLE_TEST_CONFIG',str(TASK/('config-'+RUN+'.json')))).resolve()
        if not config_path.is_relative_to(TASK): raise RuntimeError('SKEDADDLE_TEST_CONFIG must stay in this task folder')
        with config_path.open('x',encoding='utf8',newline='\n') as stream: json.dump(cfg,stream,indent=2)
        ENV.update(cfg['env'],SKEDADDLE_TEST_CONFIG=str(config_path),
                   PGSSLMODE='verify-full',PGSSLROOTCERT=cfg['ca_pem'])
        # Some gates are #[ignore], others are ordinary tests with env gates.
        # Run both sets explicitly, serially; retain every failed test binary.
        for binary,selection in selections:
            if not (CRATE/'tests'/(binary+'.rs')).is_file():
                raise RuntimeError('required gated test binary absent: '+binary)
            for ignored in (False,True):
                args=['test','--locked','--no-fail-fast','-j4','--test',binary]
                if selection: args.append(selection)
                args.append('--')
                if ignored:args+=['--ignored']
                args+=['--nocapture','--test-threads=1']
                status|=cargo(args,'pg'+PG_VERSION+'-'+binary+('-ignored' if ignored else '')+'-'+RUN)
        # Preserve source coverage and route every configuration-gated library live test.
        library_selections=('pg::source::tests::', 'pg::tls::tests::harness_connection_reports_real_encryption',
                            'runner::fence_readiness::tests::live_extra_writer_and_definer_block_then_exact_acknowledgments_hold',
                            'runner::destination_readiness::tests::live_invalid_index_not_valid_sequence_type_and_reverse_privilege')
        for selection_index,selection in enumerate(library_selections if 'SKEDADDLE_PG_TEST_SELECTION' not in ENV else ()):
            for ignored in (False,True):
                args=['test','--locked','--no-fail-fast','-j4','--lib',selection,'--']
                if ignored:args+=['--ignored']
                status|=cargo(args+['--nocapture','--test-threads=1'],
                              'pg'+PG_VERSION+'-library-'+str(selection_index)+'-'+str(ignored)+'-'+RUN)
        sizes={side:int(docker('exec',compose('ps','-q',side).stdout.strip(),
                              'du','-sk','/var/lib/postgresql').stdout.split()[0])*1024
               for side in PG_MATRIX_SERVICES}
        save('data-size-'+RUN+'.json',sizes)
        if sum(sizes.values())>1_000_000_000: raise RuntimeError('PostgreSQL aggregate data cap exceeded')
    finally:
        if started:
            try:
                for ident in resources(RUN)['container']:
                    logs=docker('logs','--tail','1000',ident,check=False)
                    save('docker-'+ident[:12]+'-'+RUN+'.log',(logs.stdout+logs.stderr)[-1024*1024:])
            finally:down()
    return status


def linux_fs():
    """Real Unix links, a pinned Rust image, read-only source and J: outputs."""
    guard()
    sweep_own_orphans()
    for directory in [TASK/'linux-tmp', TASK/'linux-scratch']:
        directory.mkdir(exist_ok=True)
    save('owner-'+RUN+'.json',{'run_id':RUN,'pid':os.getpid(),'task_root':str(TASK)})
    # Mount the repo read-only so Cargo can resolve sibling path dependencies.
    repo = CRATE.parents[1]
    source = '/src'
    name = 'skedaddle-'+RUN+'-linux-fs'
    try:
        result = docker('run', '--name', name,
            '--label', 'skedaddle.test='+RUN,
            '--label', 'com.docker.compose.project=skedaddle-'+RUN,
            '--read-only',
            '--mount', 'type=bind,source='+str(repo)+',target='+source+',readonly',
            '--mount', 'type=bind,source='+str(TASK)+',target=/work',
            '--workdir', source+'/crates/skedaddle',
            '--env', 'CARGO_TARGET_DIR=/work/linux-target',
            '--env', 'CARGO_HOME=/work/linux-cargo-home',
            '--env', 'CARGO_BUILD_JOBS=2',
            '--env', 'TEMP=/work/linux-tmp', '--env', 'TMP=/work/linux-tmp',
            '--env', 'TMPDIR=/work/linux-tmp',
            '--env', 'SKEDADDLE_TEST_ROOT=/work/linux-scratch',
            '--env', 'SKEDADDLE_TEST_WORK_ROOT=/work',
            LOCK['rust']['ref'],
            'cargo', 'test', '--locked', '--test', 'fsconfine', '--', '--nocapture',
            check=False, timeout=7200)
        save('linux-fs-'+RUN+'.log',result.stdout+'\n'+result.stderr)
        print(result.stdout+result.stderr,flush=True)
        return result.returncode
    finally:
        down()


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
        elif action=='p3':code=p2(p3=True)
        elif action=='linux-fs':code=linux_fs()
        elif action=='pg-matrix':code=pg_matrix()
        elif action=='guard':guard()
        elif action=='check':check(sys.argv[3] if len(sys.argv)>3 and sys.argv[3] else None)
        elif action=='down':down()
        elif action=='mutants':code=p2(mutate=True)
        else:raise ValueError(action)
    finally:
        # Scope the post-run check to this run: concurrent runs of other lanes are not ours to judge.
        if action!='check':check(RUN)
    sys.exit(code)
