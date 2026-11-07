"""S1 adapter of S4: guard/up/export/down/check; never sweep other runs or delete host files."""
import importlib.util, io, json, os, pathlib, subprocess, sys, time, traceback, tarfile, urllib.request, hashlib
sys.dont_write_bytecode = True
ROOT = pathlib.Path(__file__).resolve().parent
S4 = ROOT.parent / 's4-harness'
MAJOR = int(sys.argv[1])
RUN = sys.argv[2] if len(sys.argv)>2 else 's1-pg%d-%d'%(MAJOR,time.time())
os.environ.update(SKEDADDLE_RUN_ID=RUN, SKEDADDLE_PG_MAJOR=str(MAJOR), SKEDADDLE_OWNER_PID=str(os.getpid()), PYTHONDONTWRITEBYTECODE='1')
spec = importlib.util.spec_from_file_location('s4', S4/'harness.py')
h = importlib.util.module_from_spec(spec)
spec.loader.exec_module(h)
h.OUT = h.ART / ('s1-'+RUN)
def compose(*args, **kwargs):
    return h.docker('compose','-f',str(S4/'compose.yaml'),'-f',str(ROOT/'compose.override.yaml'),'-p','skedaddle-'+RUN,*args,env=h.environment(),**kwargs)
h.compose = compose
def port(service, internal):
    return int(compose('port',service,str(internal))['stdout'].strip().rsplit(':',1)[1])
status=0
started=False
try:
    h.guard()
    started=True
    h.up()
    pg=h.cid('pg')
    # Keys and certificates are generated inside the labeled PG container, never on the host.
    # Alpine PG ships libssl, not the CLI. Extract ONLY the CLI into a fresh container
    # directory with tar -k (no overwrites). No package manager or file cleanup runs.
    repo=h.execute('pg','cat','/etc/apk/repositories')['stdout'].splitlines()[0]
    arch=h.execute('pg','uname','-m')['stdout'].strip()
    index=urllib.request.urlopen(repo+'/'+arch+'/APKINDEX.tar.gz',timeout=60).read()
    with tarfile.open(fileobj=io.BytesIO(index),mode='r:gz') as archive:
        packages=archive.extractfile('APKINDEX').read().decode()
    entry=next(x for x in packages.split('\n\n') if '\nP:openssl\n' in '\n'+x+'\n')
    version=next(x[2:] for x in entry.splitlines() if x.startswith('V:'))
    url=repo+'/'+arch+'/openssl-'+version+'.apk'
    h.execute('pg','mkdir','/s1-tools')
    h.execute('pg','wget','-q','-O','/s1-tools/openssl.apk',url)
    h.execute('pg','tar','-xzkf','/s1-tools/openssl.apk','-C','/s1-tools','usr/bin/openssl')
    h.save('openssl-source.json',{'url':url,'apkindex_sha256':hashlib.sha256(index).hexdigest(),'entry':entry})
    ssl=h.execute('pg','sh','-ec', 'set -C; /s1-tools/usr/bin/openssl req -x509 -newkey rsa:2048 -nodes -days 2 -subj /CN=localhost -addext subjectAltName=DNS:localhost,IP:127.0.0.1 -addext basicConstraints=critical,CA:FALSE -addext extendedKeyUsage=serverAuth -keyout "$PGDATA/s1-server.key" -out "$PGDATA/s1-server.crt"; chown postgres:postgres "$PGDATA/s1-server.key" "$PGDATA/s1-server.crt"; chmod 600 "$PGDATA/s1-server.key"; printf "local all all trust\\nhostssl all all all scram-sha-256\\nhostssl replication all all scram-sha-256\\nhostnossl all all all reject\\nhostnossl replication all all reject\\n" > "$PGDATA/s1-hba.conf"; chown postgres:postgres "$PGDATA/s1-hba.conf"')
    h.save('tls-setup.json',ssl)
    h.sql("ALTER SYSTEM SET ssl='on'; ALTER SYSTEM SET ssl_cert_file='s1-server.crt'; ALTER SYSTEM SET ssl_key_file='s1-server.key'; ALTER SYSTEM SET hba_file='"+('/var/lib/postgresql/18/docker' if MAJOR==18 else '/var/lib/postgresql/data')+"/s1-hba.conf'; ALTER SYSTEM SET log_connections='on'; ALTER SYSTEM SET wal_sender_timeout='4s';")
    # hba_file is a postmaster setting; restart only this run's labeled PG container.
    h.docker('restart',pg)
    for _ in range(60):
        if h.sql('select 1;',check=False)['exit']==0: break
        time.sleep(0.5)
    else: raise RuntimeError('PG failed to restart')
    # Socket SQL remains local trust. All TCP including logical replication requires TLS/SCRAM.
    h.save('tls-settings.json',h.sql("show ssl; show hba_file; select type,auth_method,error from pg_hba_file_rules; select rolname, left(rolpassword,14) from pg_authid where rolname='postgres';"))
    h.save('server-cert.pem',h.execute('pg','sh','-ec','cat "$PGDATA/s1-server.crt"')['stdout'])
    h.save('toxi-create.json',h.wget('http://toxiproxy:8474/proxies','--header=Content-Type: application/json','--post-data={"name":"pg","listen":"0.0.0.0:5433","upstream":"pg:5432","enabled":true}'))
    env=os.environ.copy()
    env.update(S1_PG_PORT=str(port('pg',5432)), S1_TOXI_PORT=str(port('toxiproxy',5433)), S1_TOXI_API=str(port('toxiproxy',8474)), S1_CERT=str(h.OUT/'server-cert.pem'), S1_OUT=str(h.OUT), S1_MAJOR=str(MAJOR))
    probe=pathlib.Path('K:/Projects/gigglesdb/crates/skedaddle/target/debug/s1-replication.exe')
    result=h.command([str(probe)],env=env,timeout=360,check=False)
    h.save('probe-output.json',result)
    print(result['stdout'],flush=True)
    print(result['stderr'],flush=True)
    if result['exit']: raise RuntimeError('Probe failed: '+str(result['exit']))
    h.save('database-size.json',h.sql("select pg_database_size('skedaddle'); select slot_name,spill_txns,spill_count,spill_bytes from pg_stat_replication_slots;"))
except BaseException:
    status=1
    traceback.print_exc()
finally:
    if started:
        try: h.export()
        except BaseException: status=1; traceback.print_exc()
        try: h.down()
        except BaseException: status=1; traceback.print_exc()
        try: h.check(RUN)
        except BaseException: status=1; traceback.print_exc()
sys.exit(status)
