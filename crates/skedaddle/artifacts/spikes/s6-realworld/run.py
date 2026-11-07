"""S6: S4 PG18, three real apps, timed HTTP workload, upgrade, scoped teardown.
Never invokes S4 sweep or unscoped cleanup; no host filesystem deletion.
"""
import collections, datetime, importlib.util, json, os, pathlib, subprocess, sys, time, traceback, urllib.request
sys.dont_write_bytecode=True
ROOT=pathlib.Path(__file__).resolve().parent
RUN=sys.argv[1]; DURATION=int(sys.argv[2]) if len(sys.argv)>2 else 180
OUT=ROOT.parents[1]/('s6-'+RUN)
images=json.loads((OUT/'images.lock.json').read_text())
os.environ.update(SKEDADDLE_RUN_ID=RUN,SKEDADDLE_PG_MAJOR='18',SKEDADDLE_OWNER_PID=str(os.getpid()),PYTHONDONTWRITEBYTECODE='1')
spec=importlib.util.spec_from_file_location('s4',ROOT.parent/'s4-harness'/'harness.py');h=importlib.util.module_from_spec(spec);spec.loader.exec_module(h)
h.OUT=OUT/'runtime'
def save(name,data):
    with (OUT/name).open('x',encoding='utf8') as f:f.write(data if isinstance(data,str) else json.dumps(data,indent=2))
original_command=h.command
def command(args,**kwargs):
    result=original_command(args,**kwargs)
    with (OUT/'commands.jsonl').open('a',encoding='utf8') as f:
        f.write(json.dumps({'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'argv':[str(a) for a in args],'exit':result['exit']})+'\n')
    return result
h.command=command
env=h.environment()
for key,value in images.items():env[key.upper().replace('-','_')+'_IMAGE']=value['ref']
def compose(*args,**kwargs):return h.docker('compose','-f',str(ROOT/'compose.yaml'),'-p','skedaddle-'+RUN,*args,env=env,**kwargs)
h.compose=compose
def sql(db,query):return h.execute('pg','psql','-X','-v','ON_ERROR_STOP=1','-U','postgres','-d',db,'-At',input=query)
def base(service,port):return 'http://127.0.0.1:'+compose('port',service,str(port))['stdout'].strip().rsplit(':',1)[1]
def ready(url,timeout=180):
    deadline=time.monotonic()+timeout
    while time.monotonic()<deadline:
        try:
            with urllib.request.urlopen(url,timeout=3) as r:
                if r.status==200:return
        except Exception:pass
        time.sleep(1)
    raise RuntimeError('Readiness timeout '+url)
def schema(db,phase):
    q="""SELECT json_build_object(
    'server_version',current_setting('server_version'),
    'database_bytes',pg_database_size(current_database()),
    'tables',(SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relkind IN ('r','p')),
    'foreign_keys',(SELECT count(*) FROM pg_constraint c JOIN pg_namespace n ON n.oid=c.connamespace WHERE n.nspname='public' AND c.contype='f'),
    'sequences',(SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relkind='S'),
    'triggers',(SELECT count(*) FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND NOT t.tgisinternal),
    'partitioned_tables',(SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relkind='p'),
    'partitions',(SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relispartition),
    'types',(SELECT json_agg(x) FROM (SELECT DISTINCT t.typname FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_type t ON t.oid=a.atttypid WHERE n.nspname='public' AND c.relkind IN ('r','p') AND a.attnum>0 AND NOT a.attisdropped ORDER BY 1) x));"""
    obj=json.loads(sql(db,q)['stdout']);save('schema-'+db+'-'+phase+'.json',obj)
    dump=h.execute('pg','pg_dump','-U','postgres','-d',db,'--schema-only','--no-owner','--no-privileges')['stdout'];save('schema-'+db+'-'+phase+'.sql',dump)
    if obj['database_bytes']>1000000000:raise RuntimeError('Database exceeds 1GB')
    return obj
def migration():
    tables=sql('skedaddle',"SELECT tablename FROM pg_tables WHERE schemaname='public' AND (tablename LIKE '%version%' OR tablename LIKE '%migration%');")['stdout'].splitlines()
    return {t:sql('skedaddle','TABLE "'+t+'";')['stdout'] for t in tables}
from driver import Driver
from oracle import verify
started=False;status=0;driver=None;summary={}
try:
    h.guard()
    # The S4 guard conservatively covers every harness image; app images were resolved/pulled separately.
    started=True;h.up()
    h.docker('cp',str(ROOT/'feed.xml'),h.cid('fixture')+':/fixture/feed.xml')
    mf=base('miniflux-old',8080);ready(mf+'/healthcheck')
    save('migration-before.json',migration())
    save('pg-app-connections-initial.json',sql('skedaddle',"SELECT datname,usename,application_name,client_addr::text,state FROM pg_stat_activity WHERE client_addr IS NOT NULL;"))
    print('MINIFLUX READY',mf,flush=True)
    sql('postgres','CREATE DATABASE gitea;');sql('postgres','CREATE DATABASE keycloak;')
    save('candidates-up.json',compose('--profile','candidates','up','-d','gitea','keycloak',check=False))
    gb=base('gitea',3000);kb=base('keycloak',8080)
    ready(gb+'/api/v1/version');ready(kb+'/realms/master/.well-known/openid-configuration',240)
    save('gitea-admin-bootstrap.json',h.docker('exec','--user','git',h.cid('gitea'),'gitea','admin','user','create','--username','admin','--password','s6-local-admin-password','--email','admin@example.org','--admin','--must-change-password=false','--config','/data/gitea/conf/app.ini'))
    driver=Driver(OUT/'history.jsonl')
    driver.gitea_workload(gb);print('GITEA API WORKLOAD PASS',flush=True)
    driver.keycloak_workload(kb);print('KEYCLOAK API WORKLOAD PASS',flush=True)
    start=time.monotonic();i=0
    while time.monotonic()-start<DURATION:
        driver.miniflux_cycle(mf,i);i+=1
        if i%15==0:print('MINIFLUX PROGRESS',i,'cycles',round(time.monotonic()-start,1),'seconds',flush=True)
        time.sleep(2)
    summary=driver.close();driver=None;summary.update(miniflux_cycles=i,miniflux_elapsed_seconds=round(time.monotonic()-start,3),requested_duration_seconds=DURATION)
    save('driver-summary.json',summary)
    for db in ('skedaddle','gitea','keycloak'):schema(db,'populated')
    save('pg-app-connections-populated.json',sql('skedaddle',"SELECT datname,usename,application_name,client_addr::text,state FROM pg_stat_activity WHERE client_addr IS NOT NULL;"))
    oracle=verify(OUT/'history.jsonl',sql);save('oracle-before-upgrade.json',oracle)
    if oracle['failed']:raise RuntimeError('Oracle mismatch before upgrade')
    print('ORACLE BEFORE PASS',oracle['passed'],flush=True)
    save('miniflux-stop-old.json',compose('stop','miniflux-old'))
    save('upgrade-start.json',compose('--profile','upgrade','up','-d','miniflux-new',check=False))
    newbase=base('miniflux-new',8080);ready(newbase+'/healthcheck')
    save('migration-after.json',migration());schema('skedaddle','upgraded')
    before=json.loads((OUT/'migration-before.json').read_text());after=json.loads((OUT/'migration-after.json').read_text())
    if not before or before==after:raise RuntimeError('Expected actual schema migration version change')
    oracle=verify(OUT/'history.jsonl',sql);save('oracle-after-upgrade.json',oracle)
    if oracle['failed']:raise RuntimeError('Oracle mismatch after upgrade')
    save('upgrade-result.json',{'from':images['miniflux-old']['tag'],'to':images['miniflux-new']['tag'],'before':before,'after':after,'oracle_passed':oracle['passed'],'status':'PASS'})
    print('UPGRADE PASS',before,after,'oracle',oracle['passed'],flush=True)
except BaseException:
    status=1;save('failure.txt',traceback.format_exc());traceback.print_exc()
finally:
    if driver is not None:
        try:save('driver-summary-partial.json',driver.close())
        except BaseException:traceback.print_exc()
    if started:
        try:
            save('run-resources-before-down.json',h.resources(RUN))
            h.export()
        except BaseException:status=1;traceback.print_exc()
        try:h.down()
        except BaseException:status=1;traceback.print_exc()
        try:
            h.check(RUN);save('leftovers.json',h.resources(RUN))
        except BaseException:status=1;traceback.print_exc()
    save('result.json',{'exit':status,'run_id':RUN,'no_host_file_deletion':True,'no_sweeper':True,'no_archdocs_or_linear_writes':True})
print('S6 EXIT',status,OUT,flush=True)
sys.exit(status)
