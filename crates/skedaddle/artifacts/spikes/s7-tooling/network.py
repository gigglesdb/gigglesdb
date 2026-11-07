"""S7 isolated S4 adapter: never calls sweep or unscoped cleanup."""
import importlib.util, json, os, pathlib, socket, struct, subprocess, sys, time, traceback, urllib.request
sys.dont_write_bytecode = True
ROOT = pathlib.Path(__file__).resolve().parent
BASE = ROOT.parents[2]
RUN = sys.argv[1]
os.environ.update(SKEDADDLE_RUN_ID=RUN, SKEDADDLE_PG_MAJOR='18', SKEDADDLE_OWNER_PID=str(os.getpid()))
S4 = ROOT.parent / 's4-harness'
spec = importlib.util.spec_from_file_location('s4', S4/'harness.py')
h = importlib.util.module_from_spec(spec); spec.loader.exec_module(h)
h.OUT = BASE/'artifacts'/('s7-'+RUN)/'network'
def compose(*args, **kwargs):
    return h.docker('compose','-f',str(S4/'compose.yaml'),'-f',str(ROOT/'compose.override.yaml'),'-p','skedaddle-'+RUN,*args,env=h.environment(),**kwargs)
h.compose = compose
def port(service, n): return int(compose('port',service,str(n))['stdout'].strip().rsplit(':',1)[1])
results=[]
def api(path, value=None, method=None):
    req=urllib.request.Request(API+path, data=None if value is None else json.dumps(value).encode(), headers={'Content-Type':'application/json'},method=method)
    with urllib.request.urlopen(req,timeout=10) as r:
        data=r.read()
        return json.loads(data) if data else None
def http_call(p, method, path, body=b'', content='application/json'):
    with socket.create_connection(('127.0.0.1',p),timeout=2) as s:
        s.settimeout(2)
        req=(f'{method} {path} HTTP/1.0\r\nHost: localhost\r\nContent-Type: {content}\r\nContent-Length: {len(body)}\r\nConnection: close\r\n\r\n').encode()+body
        s.sendall(req); chunks=[]
        while True:
            b=s.recv(65536)
            if not b: break
            chunks.append(b)
        data=b''.join(chunks)
        assert b' 200 ' in data.split(b'\r\n')[0],data[:200]
        return {'bytes':len(data),'reads':len(chunks),'max_read':max(map(len,chunks),default=0),'body_ok':b'x'*32768 in data}
def pg_call(p):
    with socket.create_connection(('127.0.0.1',p),timeout=2) as s:
        s.settimeout(2); sizes=[]
        def exact(n):
            b=b''
            while len(b)<n:
                v=s.recv(n-len(b))
                if not v: raise ConnectionError('PG EOF')
                sizes.append(len(v)); b+=v
            return b
        def ready():
            data=b''
            while True:
                typ=exact(1); n=struct.unpack('!I',exact(4))[0]; body=exact(n-4); data+=body
                if typ==b'E': raise RuntimeError(body.decode(errors='replace'))
                if typ==b'Z': return data
        body=struct.pack('!I',196608)+b'user\0postgres\0database\0skedaddle\0\0'
        s.sendall(struct.pack('!I',len(body)+4)+body); ready()
        query=b"select repeat('x',32768);\0"
        s.sendall(b'Q'+struct.pack('!I',len(query)+4)+query); data=ready()
        assert b'x'*32768 in data
        return {'bytes':len(data),'reads':len(sizes),'max_read':max(sizes),'body_ok':True}
def container_call():
    r=h.command(['docker','exec','-i',h.cid('pg'),'env','PGCONNECT_TIMEOUT=2','PGPASSWORD=skedaddle','psql','-X','-h','toxiproxy','-p','5433','-U','postgres','-d','skedaddle','-At','-c',"select repeat('x',32768);"],timeout=9,check=False)
    if r['exit']: raise RuntimeError(json.dumps(r))
    assert r['stdout'].strip()=='x'*32768
    return {'bytes':len(r['stdout']),'body_ok':True}
TOXICS={
 'latency':{'latency':250,'jitter':0},
 'timeout':{'timeout':400},
 'reset_peer':{'timeout':0},
 'bandwidth':{'rate':8},
 'slicer':{'average_size':128,'size_variation':0,'delay':10000},
}
def measure(route,toxic,call):
    start=time.perf_counter()
    try: detail=call(); ok=True
    except Exception as e: detail={'error':str(e)}; ok=False
    elapsed=time.perf_counter()-start
    passed=(not ok) if toxic in ('timeout','reset_peer') else ok
    if toxic=='latency': passed &= elapsed>=0.22
    if toxic=='bandwidth': passed &= elapsed>=3.0
    if toxic=='slicer': passed &= elapsed>=2.0
    result={'route':route,'toxic':toxic,'pass':passed,'seconds':round(elapsed,4),'detail':detail}
    results.append(result); print(json.dumps(result),flush=True)
    h.event('measurement',result)
started=False; status=0
try:
    h.guard(); started=True; h.up()
    API='http://127.0.0.1:'+str(port('toxiproxy',8474))
    h.save('toxi-version.json',api('/version'))
    for name,listen,upstream in [('pg',5433,'pg:5432'),('gcs',4444,'gcs:4443')]:
        h.save('proxy-'+name+'.json',api('/proxies',{'name':name,'listen':'0.0.0.0:'+str(listen),'upstream':upstream,'enabled':True}))
    PG=port('toxiproxy',5433); GCS=port('toxiproxy',4444)
    h.save('ports.json',{'api':API,'pg':PG,'gcs':GCS})
    http_call(GCS,'POST','/storage/v1/b',b'{"name":"s7-bucket"}')
    http_call(GCS,'POST','/upload/storage/v1/b/s7-bucket/o?uploadType=media&name=blob',b'x'*32768,'application/octet-stream')
    routes=[('host-pg','pg',lambda:pg_call(PG)),('host-gcs','gcs',lambda:http_call(GCS,'GET','/storage/v1/b/s7-bucket/o/blob?alt=media')),('container-pg','pg',container_call)]
    for route,proxy,call in routes:
        measure(route,'baseline',call)
        for toxic,attributes in TOXICS.items():
            h.event('toxic-create',api('/proxies/'+proxy+'/toxics',{'name':'s7-'+toxic,'type':toxic,'stream':'downstream','toxicity':1.0,'attributes':attributes}))
            try: measure(route,toxic,call)
            finally: api('/proxies/'+proxy+'/toxics/s7-'+toxic,method='DELETE')
        measure(route,'recovery',call)
    h.save('results.json',results)
    if not all(r['pass'] for r in results): status=1
except BaseException:
    status=1; traceback.print_exc()
finally:
    if started:
        try: h.export()
        except BaseException: status=1; traceback.print_exc()
        try: h.down()
        except BaseException: status=1; traceback.print_exc()
        try:
            leftover=h.resources(RUN); h.save('leftovers.json',leftover); h.check(RUN)
        except BaseException: status=1; traceback.print_exc()
sys.exit(status)
