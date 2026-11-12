#!/usr/bin/env python3
"""Source-pinned GUI catalog qualification; all runtime artifacts stay on J:."""
import sys
sys.dont_write_bytecode = True
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import argparse, base64, collections, hashlib, http.cookiejar, json, os, re, secrets
import shutil, subprocess, time, urllib.request, urllib.error
from compat_lib import docker, gigglesdb, util

HERE = Path(__file__).resolve().parent
ROOT = Path('J:/gigglesdb-work/p1-gui')

def write(path, value):
    path.write_text(value if isinstance(value, str) else json.dumps(value, indent=2)+'\n', encoding='utf-8', newline='\n')

def wait(test, seconds=90):
    deadline = time.monotonic()+seconds
    while time.monotonic() < deadline:
        try:
            if test(): return
        except (OSError, RuntimeError, urllib.error.URLError): pass
        time.sleep(.5)
    raise RuntimeError('readiness deadline exceeded')

def shape(value):
    if isinstance(value, dict): return {k:shape(v) for k,v in sorted(value.items()) if k not in ('id','_id','sid','did','scid','tid','server','server_id','icon','inode','connected','version','bgcolor','fgcolor','data_size')}
    if isinstance(value, list): return [shape(v) for v in value]
    return type(value).__name__

def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--server-bin', type=Path, required=True)
    p.add_argument('--tool', choices=['pgadmin','dbeaver','all'], default='all')
    args = p.parse_args()
    run_id = 'r0-'+time.strftime('%Y%m%dT%H%M%SZ',time.gmtime())+'-'+secrets.token_hex(3)
    run = ROOT/'runs'/run_id; run.mkdir(parents=True)
    for name in ('tmp','target','deps'): (run/name).mkdir()
    password = secrets.token_urlsafe(24)
    env = dict(os.environ, TEMP=str(run/'tmp'), TMP=str(run/'tmp'), CARGO_TARGET_DIR=str(run/'target'),
               PGPASSWORD=password, POSTGRES_PASSWORD=password, PGADMIN_DEFAULT_EMAIL='gui@example.org', PGADMIN_DEFAULT_PASSWORD=password)
    engine = docker.Docker(run_id, env)
    pins = json.loads((HERE/'pins.json').read_text())
    images = json.loads((HERE.parent/'pins.json').read_text())
    pgref = 'postgres@'+images['postgres']['digest']
    pins['pgadmin'] = 'dpage/pgadmin4@'+images['pgadmin']['digest']
    data = dict(pins=pins, results=[], run=str(run), limitations=[
        'DBeaver 24.3.0 source-derived PostgreSQL browse path, not a running DBeaver UI.',
        'PASS compares request success and result shape; raw values are retained. OIDs and system inventory differ.',
        'pgAdmin uses desktop mode on a loopback-only HTTP port; database password authentication remains enabled.'])
    data['harness_head']=util.command(['git','rev-parse','HEAD'],env)['stdout'].strip()
    server = None
    def add(tool, node, pg, gd, sql=None):
        status = 'HARNESS_ERROR' if pg.get('status') != 'OK' else 'FAIL' if gd.get('status') != 'OK' else 'PASS'
        diff = {}
        if 'columns' in pg and 'columns' in gd:
            diff = dict(columns_equal=pg['columns']==gd['columns'], pg_rows=pg['row_count'], giggles_rows=gd['row_count'])
            if not diff['columns_equal'] or (node != 'schemas' and diff['pg_rows'] != diff['giggles_rows']): status = 'FAIL'
        elif 'body' in pg and 'body' in gd:
            diff = dict(shape_equal=shape(pg['body'])==shape(gd['body']))
            if not diff['shape_equal'] and status == 'PASS': status = 'FAIL'
        item = dict(tool=tool,node=node,outcome=status,pg=pg,giggles=gd,difference=diff)
        if sql is not None: item['sql'] = sql
        if status == 'FAIL':
            item['classification'] = 'product gap'
            item['owner'] = 'giggles-core' if tool=='setup' else 'giggles-pg-catalog'
        data['results'].append(item)
        print(tool,node,status,flush=True)
        return item
    def jdbc(side, entries, simple=False):
        manifest = run/(side+'-'+secrets.token_hex(3)+'.tsv')
        write(manifest,''.join(q['id']+'\t'+base64.b64encode(q['sql'].encode()).decode()+'\t'+q.get('bind','')+'\n' for q in entries))
        url = 'jdbc:postgresql://127.0.0.1:'+str(ports[side])+'/postgres?sslmode=disable'
        if simple: url += '&preferQueryMode=simple'
        r = util.command(['java','-cp',str(run/'deps')+os.pathsep+str(jar),'Probe',url,str(manifest)],env,timeout=240)
        write(manifest.with_suffix('.stdout.jsonl'),util.redact(r['stdout'],[password]))
        write(manifest.with_suffix('.stderr.txt'),util.redact(r['stderr'],[password]))
        if r['exit'] != 0 or r['timed_out']: raise RuntimeError('JDBC process failed: '+util.redact(r['stderr'],[password]))
        return {v['id']:v for v in map(json.loads,r['stdout'].splitlines())}
    def logs():
        r = engine.call('logs', pgname)
        return r['stdout']+r['stderr']
    try:
        for image in (pgref, pins['pgadmin']) if args.tool!='dbeaver' else (pgref,):
            if not engine.image(image,True): raise RuntimeError('image unavailable: '+image)
        net = engine.prefix+'net'; engine.networks.append(net); engine.require('network','create',net)
        pgname = engine.prefix+'pg'; engine.containers.append(pgname)
        engine.require('run','-d','--name',pgname,'--network',net,'-p','127.0.0.1::5432','--tmpfs','/var/lib/postgresql:rw,size=512m',
                       '-e','POSTGRES_PASSWORD',pgref,'postgres','-c','log_statement=all','-c','log_line_prefix=GUI[%p] %a ')
        wait(lambda:engine.call('exec',pgname,'pg_isready','-h','127.0.0.1','-U','postgres')['exit']==0)
        pgport = int(engine.require('port',pgname,'5432/tcp').split(':')[-1])
        server = gigglesdb.Server(args.server_bin.resolve(),run,env,password,['--engine','v2'])
        ports = dict(pg=pgport,giggles=server.start()); data['server']=server.meta
        jar = run/'deps'/'postgresql.jar'
        jar.write_bytes(urllib.request.urlopen('https://repo.maven.apache.org/maven2/org/postgresql/postgresql/'+pins['jdbc_version']+'/postgresql-'+pins['jdbc_version']+'.jar',timeout=60).read())
        if hashlib.sha256(jar.read_bytes()).hexdigest()!=pins['jdbc_sha256']: raise RuntimeError('JDBC checksum mismatch')
        r = util.command(['javac','-d',str(run/'deps'),str(HERE/'Probe.java')],env,timeout=60)
        if r['exit']!=0: raise RuntimeError(r['stderr'])
        setup = [dict(id='ddl_'+str(i+1),sql=s.strip()+';') for i,s in enumerate((HERE/'sample.sql').read_text().split(';')) if s.strip()]
        ids = dict(id='ids',sql="SELECT (SELECT oid FROM pg_catalog.pg_database WHERE datname='postgres') AS db, (SELECT oid FROM pg_catalog.pg_namespace WHERE nspname='gui_sales') AS schema, (SELECT oid FROM pg_catalog.pg_class WHERE relname='child' AND relnamespace=(SELECT oid FROM pg_catalog.pg_namespace WHERE nspname='gui_sales')) AS tab")
        sides = {side:jdbc(side,setup+[ids]) for side in ports}
        for q in setup: add('setup',q['id'],sides['pg'][q['id']],sides['giggles'][q['id']],q['sql'])
        oids = {side:sides[side]['ids'].get('rows',[[None,None,None]])[0] for side in ports}
        objects = dict(id='objects',sql="SELECT n.nspname,c.relname,c.oid FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname IN ('gui_sales','gui_aux')")
        inventories = {side:jdbc(side,[objects])['objects'] for side in ports}
        if any(v.get('status') != 'OK' for v in inventories.values()):
            raise RuntimeError('object identity inventory failed')
        identities = {side:{tuple(row[:2]):row[2] for row in v['rows']} for side,v in inventories.items()}
        mapping = {oid:identities['giggles'][key] for key,oid in identities['pg'].items() if key in identities['giggles']}
        mapping.update({a:b for a,b in zip(oids['pg'],oids['giggles']) if a and b and int(a)>10000})
        write(run/'oid-mapping.json',dict(inventories=inventories,mapping=mapping))
        if args.tool in ('all','dbeaver'):
            queries = json.loads((HERE/'queries.json').read_text())
            queries += [dict(id='metadata_'+name,sql='META:'+name) for name in ('schemas','tables','columns','indexes','pk','fk')]
            outputs = {side:jdbc(side,queries) for side in ports}
            for q in queries:
                item = add('dbeaver',q['id'],outputs['pg'][q['id']],outputs['giggles'][q['id']],q['sql'])
                item['provenance'] = q.get('url','pgJDBC DatabaseMetaData 42.7.3')
                item['oracle_sql'] = re.findall(r'GUI\[\d+\] p1-gui:'+q['id']+r' LOG:  (?:statement:|execute [^:]*:) (.*?)(?=\nGUI\[|\Z)',logs(),re.S)
                if q['id']=='metadata_tables' and item['outcome']=='FAIL' and item['difference'].get('columns_equal'):
                    item.update(owner='giggles-core',classification='product fixture gap: unsupported CREATE VIEW',blocked_by='setup/ddl_7')
                if q['id']=='metadata_fk' and item['outcome']=='FAIL': item['owner']='giggles-server'
        if args.tool in ('all','pgadmin'):
            name=engine.prefix+'admin'; engine.containers.append(name)
            engine.require('run','-d','--name',name,'--network',net,'--add-host','host.docker.internal:host-gateway','-p','127.0.0.1::80',
                '--tmpfs','/var/lib/pgadmin:rw,size=128m,uid=5050,gid=5050','-e','PGADMIN_DEFAULT_EMAIL','-e','PGADMIN_DEFAULT_PASSWORD',
                '-e','PGADMIN_CONFIG_SERVER_MODE=False','-e','PGADMIN_CONFIG_MASTER_PASSWORD_REQUIRED=False',
                '-e','PGADMIN_CONFIG_WTF_CSRF_ENABLED=False',pins['pgadmin'])
            adminport = int(engine.require('port',name,'80/tcp').split(':')[-1])
            opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
            base = 'http://127.0.0.1:'+str(adminport)
            wait(lambda:opener.open(base+'/misc/ping',timeout=5).status==200)
            opener.open(base+'/browser/',timeout=30).read()
            def api(path,payload=None):
                req = urllib.request.Request(base+path,data=json.dumps(payload).encode() if payload is not None else None,headers={'Content-Type':'application/json','X-Requested-With':'XMLHttpRequest'})
                try:
                    with opener.open(req,timeout=45) as r: code,raw = r.status,r.read().decode()
                except urllib.error.HTTPError as e: code,raw = e.code,e.read().decode()
                try: body=json.loads(raw)
                except ValueError: body=dict(raw=raw)
                ok=code<400 and (not isinstance(body,dict) or body.get('success',1) not in (0,False))
                return dict(status='OK' if ok else 'ERR',http=code,body=body,path=path)
            captures={side:{} for side in ports}
            for side in ports:
                before=logs()
                created=api('/browser/server/obj/1/',dict(name=side,host=pgname if side=='pg' else 'host.docker.internal',port=5432 if side=='pg' else ports[side],db='postgres',username='postgres',role='',sslmode='disable',save_password=False))
                captures[side]['register']=created
                body=created['body']; sid=body.get('node',{}).get('_id') or body.get('node',{}).get('id')
                if sid is None: raise RuntimeError('pgAdmin registration failed: '+str(created))
                db,sc,tab=oids[side]
                calls=[('connect',f'/browser/server/connect/1/{sid}',dict(password=password)),
                       ('databases',f'/browser/database/nodes/1/{sid}/',None),
                       ('schemas',f'/browser/schema/nodes/1/{sid}/{db}/',None),
                       ('tables',f'/browser/table/nodes/1/{sid}/{db}/{sc}/',None)]
                calls += [(node,f'/browser/{kind}/nodes/1/{sid}/{db}/{sc}/{tab}/',None) for node,kind in [('columns','column'),('indexes','index'),('pk','primary_key'),('unique','unique_constraint'),('fk','foreign_key'),('check','check_constraint')]]
                calls += [('properties',f'/browser/table/obj/1/{sid}/{db}/{sc}/{tab}',None),('sql',f'/browser/table/sql/1/{sid}/{db}/{sc}/{tab}',None)]
                for node,path,payload in calls:
                    before=logs()
                    captures[side][node]=api(path,payload)
                    if side=='pg': captures[side][node]['oracle_sql_log']=logs()[len(before):]
                    write(run/('api-'+side+'-'+node+'.json'),captures[side][node])
            for node in captures['pg']:
                add('pgadmin',node,captures['pg'][node],captures['giggles'][node])
            replay=[]
            for node,record in captures['pg'].items():
                raw=record.get('oracle_sql_log','')
                for i,sql in enumerate(re.findall(r'GUI\[\d+\] [^\n]*?LOG:  (?:statement:|execute [^:]*:) (.*?)(?=\nGUI\[|\Z)',raw,re.S)):
                    sql=sql.strip()
                    if sql and '$1' not in sql and not sql.startswith(('BEGIN','COMMIT')):
                        replay.append(dict(id=node+'_'+str(i),sql=sql))
            write(run/'pgadmin-oracle-queries.json',replay)
            adapted=[]
            for q in replay:
                sql=re.sub(r"'(?:''|[^'])*'|\"(?:\"\"|[^\"])*\"|\b(\d+)\b",lambda m:str(mapping.get(m[1],m[0])) if m[1] else m[0],q['sql'])
                if oids['pg'][0] and oids['giggles'][0]:
                    sql=re.sub(r'(\b(?:oid|datid|datoid)\s*=\s*)'+oids['pg'][0]+r'\b',lambda m:m[1]+oids['giggles'][0],sql)
                adapted.append(dict(q,sql=sql))
            if replay:
                outputs=dict(pg=jdbc('pg',replay,True),giggles=jdbc('giggles',adapted,True))
                for q in replay:
                    item=add('pgadmin-sql',q['id'],outputs['pg'][q['id']],outputs['giggles'][q['id']],q['sql'])
                    if q['id']=='connect_0' and item['outcome']=='FAIL': item['owner']='giggles-server'
                connect=next(v for v in data['results'] if v['tool']=='pgadmin' and v['node']=='connect')
                if connect['outcome']=='FAIL':
                    match=re.search(r'query_id=([a-f0-9]{64})',str(connect['giggles']))
                    root=next((q for q in replay if match and hashlib.sha256(q['sql'].encode()).hexdigest()==match[1]),None)
                    for v in data['results']:
                        if v['tool']=='pgadmin' and v['outcome']=='FAIL':
                            v.update(owner='giggles-server',classification='product prerequisite gap: connection initialization',blocked_by='pgadmin/connect')
                            if root: v.update(sql=root['sql'],root_query='pgadmin-sql/'+root['id'],sqlstate=outputs['giggles'][root['id']].get('sqlstate'))
            write(run/'pgadmin.log',util.redact(engine.call('logs',name)['stdout']+engine.call('logs',name)['stderr'],[password]))
        write(run/'oracle.log',util.redact(logs(),[password]))
    except Exception as exc:
        data['harness_error']=util.redact(str(exc),[password]); print(data['harness_error'],flush=True)
    finally:
        if server: server.stop()
        try: engine.cleanup(); data['cleanup']='PASS'
        except Exception as exc: data['cleanup']=str(exc)
        data['summary']=dict(collections.Counter(v['outcome'] for v in data['results']))
        data=util.redact(data,[password]); write(run/'report.json',data)
        lines=['# GUI catalog qualification','',f'Run: {run}',f'Counts: {data["summary"]}',f'Cleanup: {data["cleanup"]}','', '| Tool | Node/query | Result | SQLSTATE | Owner |','|---|---|---|---|---|']
        for v in data['results']: lines.append('| '+ ' | '.join(str(x) for x in (v['tool'],v['node'],v['outcome'],v['giggles'].get('sqlstate',''),v.get('owner',''))) +' |')
        lines+=['','Limitations:']+['- '+v for v in data['limitations']]
        if data.get('harness_error'): lines+=['','HARNESS ERROR: '+data['harness_error']]
        write(run/'report.md','\n'.join(lines)+'\n')
        print('Report:',run/'report.md',flush=True)
        for name in ('bin','cwd','data','tmp','target','deps'):
            shutil.rmtree(run/name,ignore_errors=True)
    return 2 if data.get('harness_error') or data['summary'].get('HARNESS_ERROR') else 1 if data['summary'].get('FAIL') else 0

if __name__=='__main__': sys.exit(main())
