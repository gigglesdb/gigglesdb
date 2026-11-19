from base_live import *
from render import policy
import hashlib,threading,psycopg
from wire import Portal,WireError
T=time.monotonic()
def stamp():return {'seconds':round(time.monotonic()-T,4),'utc':datetime.datetime.now(datetime.timezone.utc).isoformat()}
def ev(n,o):(D/(n+'.json')).write_text(json.dumps(o,indent=2),newline='\n')
def poll(fn,seconds=60):
 end=time.monotonic()+seconds
 while time.monotonic()<end:
  try:
   v=fn()
   if v:return v
  except Exception:pass
  time.sleep(.2)
 raise AssertionError('poll timeout')
def conn(p,s):
 port=pf(p,6432)
 try:return psycopg.connect(host='127.0.0.1',port=port,user='session:'+s,password='live-session-8a',dbname='app',sslmode='require',channel_binding='disable',connect_timeout=3,autocommit=True)
 except psycopg.Error as original:
  # libpq hides a pre-TLS PostgreSQL ErrorResponse as an SSL negotiation error.
  # Probe only after an unclassified failure, preserving NY013 as unavailable.
  if original.sqlstate is None:
   import struct
   with socket.create_connection(('127.0.0.1',port),timeout=3) as raw:
    raw.sendall(struct.pack('!II',8,80877103))
    if raw.recv(1)==b'E':
     def exact(n):
      value=b''
      while len(value)<n:
       part=raw.recv(n-len(value))
       if not part:raise WireError('EOF')
       value+=part
      return value
     payload=exact(struct.unpack('!I',exact(4))[0]-4)
     code=next((v[1:].decode() for v in payload.split(b'\0') if v.startswith(b'C')),None)
     if code=='NY013':raise WireError('NY013') from original
  raise

def query(p,s):
 with conn(p,s) as c:return c.execute('SELECT email FROM customers WHERE id=1').fetchone()[0]
def session(leader,key,agent='live-agent'):
 return must(leader,body={'route':'postgres','mode':'read_only','verifier':verifier('live-session-8a'),'secret_sha256':hashlib.sha256(b'live-session-8a').hexdigest()},key=key,path='/v1/sessions',auth=agent+'.live-agent-8a')['id']
def bootstrap(leader,agent='live-agent'):
 principal=must(leader,body={'name':agent,'kind':'agent','admin':False,'verifier':verifier('live-agent-8a')},key='create-'+agent,path='/v1/principals')
 (D/'grant.json').write_text(json.dumps({'grants':[{'route':'postgres','mode':'read_only'}]}))
 out=command(['curl.exe','--ssl-no-revoke','--noproxy','*','--cacert',str(D/'ca.crt'),'-sS','-X','PUT','-H','Authorization: Bearer nunya.operator.live-admin-8a','-H','Idempotency-Key: grant-'+agent,'-H','Content-Type: application/json','--data-binary','@'+str(D/'grant.json'),f'https://localhost:{pf(leader)}/v1/principals/{principal["id"]}/grants'],'grant-agent');assert 'error' not in json.loads(out),out
 return session(leader,'initial-session-'+agent,agent)
def portal(p,s):
 q=Portal(pf(p,6432),s);return q,q
def apply(text,key):
 l=holder();state=must(l);return must(l,'apply',{'policy_toml':text,'expected_revision':state['active_revision']['number'],'catalog_fingerprint':state['catalog_fingerprint']},key)
def active():return must(holder()).get('activation',{}).get('state')=='active'
def partition():
 p=next(p for p in names() if p!=holder());pod=next(x for x in pods() if x['metadata']['name']==p);node=pod['spec']['nodeName'];raw=command(['docker','exec',node,'crictl','pods','--name',p,'-q'],'sandbox').strip().splitlines()[0];pid=json.loads(command(['docker','exec',node,'crictl','inspectp',raw],'sandbox-inspect'))['info']['pid']
 svc=json.loads(command([K,'--kubeconfig',env['KUBECONFIG'],'get','service','kubernetes','-o','json'],'api-service'))['spec']['clusterIP'];ep=json.loads(command([K,'--kubeconfig',env['KUBECONFIG'],'get','endpoints','kubernetes','-o','json'],'api-endpoints').lstrip())['subsets'][0];dest=[(svc,443)]+[(a['ip'],v['port']) for a in ep['addresses'] for v in ep['ports']]
 def cut(heal=False):
  guard()
  for ip,port in dest:command(['docker','exec',node,'nsenter','-t',str(pid),'-n','iptables','-D' if heal else '-I','OUTPUT','-d',ip,'-p','tcp','--dport',str(port),'-j','DROP'],('heal' if heal else 'cut')+ip)
 c,q=portal(p,SURVIVE);pf(p,6480);cut();t=time.monotonic();out=[]
 try:
  while time.monotonic()-t<8:
   try:q.fetchmany(1)
   except (psycopg.Error,WireError) as e:out.append({'seconds':time.monotonic()-t,'sqlstate':e.sqlstate});break
   time.sleep(.1)
  code=command(['curl.exe','--ssl-no-revoke','--noproxy','*','-sS','-o','NUL','-w','%{http_code}',f'http://127.0.0.1:{pf(p,6480)}/ready'],'cut-readiness')
  ev('scenario6',{'pod':p,'events':out,'ready':code,'destinations':dest,'method':'pod netns OUTPUT DROP only API IP/port; pgwire and admin forwarded traffic still responds'})
  assert out and out[-1]['sqlstate']=='NY013' and out[-1]['seconds']<=7 and code=='503'
 finally:cut(True);c.close()
 poll(allready,60);return {'refusal':out,'readiness':code,'healed':True}
def network_probe(node,pid,ip,port):
 # Only this script emits the refusal marker, after recognizing bash's C-locale
 # connection error. Operational failures must never qualify as network denial.
 script=f'LC_ALL=C; export LC_ALL; error=$( {{ echo > /dev/tcp/{ip}/{port}; }} 2>&1); code=$?; if [ "$code" -eq 0 ]; then exit 0; fi; printf "%s\\n" "$error" >&2; case "$error" in *"connect: Connection refused"*) printf "NUNYA_CONNECTION_REFUSED\\n" >&2; exit 42;; *) exit 1;; esac'
 guard()
 r=subprocess.run(['docker','exec',node,'nsenter','-t',str(pid),'-n','timeout','3','bash','-c',script],env=env,capture_output=True,text=True)
 blocked=r.returncode==124 or (r.returncode==42 and 'NUNYA_CONNECTION_REFUSED' in r.stderr.splitlines())
 return {'exit':r.returncode,'reachable':r.returncode==0,'blocked':blocked,'valid':r.returncode==0 or blocked,'stdout':r.stdout,'stderr':r.stderr,'output':r.stdout+r.stderr}

def network():
 target=pods()[0];ip=target['status']['podIP'];out={}
 # Probe from the actual postgres (non-nunya) netns and a nunya netns using node binaries.
 for label,pod in [('non-nunya',data(['get','pods','-l','app=postgres'],'postgres-pods')['items'][0]),('nunya',pods()[1])]:
  n=pod['spec']['nodeName'];raw=command(['docker','exec',n,'crictl','pods','--name',pod['metadata']['name'],'-q'],'net-sandbox-'+label).strip().splitlines()[0];pid=json.loads(command(['docker','exec',n,'crictl','inspectp',raw],'net-inspect-'+label))['info']['pid']
  out[label]=network_probe(n,pid,ip,6482)
  admin=network_probe(n,pid,ip,6480);out[label].update({'admin_exit':admin['exit'],'admin_closed':admin['blocked'],'admin_probe':admin})
 ev('networkpolicy',out)
 assert all(x['valid'] and x['admin_probe']['valid'] for x in out.values()),out
 assert out['non-nunya']['blocked'] and out['nunya']['reachable'] and all(x['admin_closed'] for x in out.values()),out
 return out

def ready(p):
 return not p['metadata'].get('deletionTimestamp') and any(c['type']=='Ready' and c['status']=='True' for c in p['status'].get('conditions',[]))
def names():return [p['metadata']['name'] for p in pods() if not p['metadata'].get('deletionTimestamp')]
def allready():
 ps=pods();return len([p for p in ps if not p['metadata'].get('deletionTimestamp')])==3 and all(ready(p) for p in ps)
def stable():
 # All replicas must run before import, including followers with only file policy.
 poll(lambda:len(pods())==3 and all(p['status'].get('phase')=='Running' for p in pods()),180)
 leader=wait_leader()[0];before=[{'pod':p['metadata']['name'],'ready':ready(p)} for p in pods()]
 first=must(leader,'import',{},'bootstrap-import-once');ev('scenario0-bootstrap',{'leader':leader,'preimport':before,'import_response':first,'import_calls':1});assert first['revision']['number']==1
 poll(lambda:allready() and active(),120)
 start=time.monotonic();samples=[]
 while time.monotonic()-start<12:
  ps=pods();sample={'seconds':time.monotonic()-start,'pods':{p['metadata']['name']:ready(p) for p in ps}};samples.append(sample)
  assert len(sample['pods'])==3 and all(sample['pods'].values()),sample
  time.sleep(.5)
 return {'preimport':before,'import_calls':1,'serving_term_seconds':6,'stable_seconds':time.monotonic()-start,'samples':samples}
def shared():
 global SURVIVE,EXPECTED
 SURVIVE=bootstrap(holder(),'survive-agent');values={};observations={}
 for p in names():
  try:
   v=query(p,SURVIVE);values[p]=v;observations[p]={'success':True,'sha256':hashlib.sha256(v.encode()).hexdigest()}
  except Exception as e:observations[p]={'success':False,'sqlstate':getattr(e,'sqlstate',None),'error_type':type(e).__name__}
 ev('scenario1-pods',observations)
 assert len(values)==3 and len(set(values.values()))==1 and not next(iter(values.values())).startswith('synthetic'),'cross-pod token validation failed; see hashes'
 EXPECTED=next(iter(values.values()));return {'pods':list(values),'identical_tokens':True,'token_sha256':hashlib.sha256(EXPECTED.encode()).hexdigest()}
def revoke():
 sid=bootstrap(holder(),'revoke-agent');portals={};out={}
 try:
  for p in names():portals[p]=Portal(pf(p,6432),sid)
  t=time.monotonic();must(holder(),body={},key='revoke-session',path='/v1/sessions/'+sid+'/revoke')
  while len(out)<3 and time.monotonic()-t<6:
   for p,q in portals.items():
    if p in out:continue
    try:q.fetchmany(1)
    except (psycopg.Error,WireError) as e:out[p]={'sqlstate':e.sqlstate,'latency_seconds':time.monotonic()-t}
   time.sleep(.025)
  ev('scenario2',out);assert len(out)==3 and all(v['sqlstate']=='NY011' and v['latency_seconds']<=6 for v in out.values()),out
  return out
 finally:
  for q in portals.values():q.close()
def tokenize(p,sid,value):
 with conn(p,sid) as c:return c.execute("SELECT nunya_tokenize('email', %s)",(value,)).fetchone()[0]
def limits():
 global LIMIT_SESSIONS
 apply(policy(23),'small-limit');poll(lambda:active() and allready())
 first=bootstrap(holder(),'limit-agent');LIMIT_SESSIONS=[first]+[session(holder(),'limit-session-'+str(i),'limit-agent') for i in (1,2)]
 ps=names();attempts=[];connections=[]
 try:
  for p,sid in zip(ps,LIMIT_SESSIONS):connections.append(conn(p,sid))
  for i in range(96):
   p=ps[i%3]
   try:
    value=connections[i%3].execute("SELECT nunya_tokenize('email', %s)",(f'unique-budget-{i}@example.test',)).fetchone()[0]
    assert isinstance(value,str) and value
    attempts.append({'pod':p,'success':True})
   except (psycopg.Error,WireError) as e:
    attempts.append({'pod':p,'success':False,'sqlstate':e.sqlstate})
    if e.sqlstate!='NY007':ev('scenario3',attempts);raise
   if len(attempts)>=6 and all(x.get('sqlstate')=='NY007' for x in attempts[-3:]):break
 finally:
  for c in connections:c.close()
 count=sum(x['success'] for x in attempts);ev('scenario3',{'limit':23,'successes':count,'attempts':attempts})
 assert 0<count<=23 and all(x.get('sqlstate')=='NY007' for x in attempts[-3:])
 return {'limit':23,'successes':count,'NY007':len(attempts)-count,'other_errors':0,'same_principal_sessions':3}
def kill():
 old=holder();before={p:query(p,SURVIVE) for p in names()};assert len(set(before.values()))==1
 t=time.monotonic();kub(['delete','pod',old,'--grace-period=0','--force','--wait=false'],'leader-force-delete')
 new=poll(lambda:(h if (h:=holder()) and h!=old else None),60);take=time.monotonic()-t
 poll(allready,120);poll(lambda:query(new,SURVIVE)==EXPECTED,60)
 values={p:query(p,SURVIVE) for p in names()};assert all(v==EXPECTED for v in values.values())
 denials=[]
 for i,p in enumerate(names()):
  try:tokenize(p,LIMIT_SESSIONS[i],'after-takeover@example.test');denials.append({'pod':p,'sqlstate':None})
  except (psycopg.Error,WireError) as e:denials.append({'pod':p,'sqlstate':e.sqlstate})
 assert all(x['sqlstate']=='NY007' for x in denials),denials
 return {'old':old,'new':new,'takeover_seconds':take,'same_session_and_tokens':True,'counter_denials':denials}
def tighten():
 global EXPECTED
 follower=next(p for p in names() if p!=holder());q=Portal(pf(follower,6432),SURVIVE);events=[];out={};done=threading.Event()
 def mutate():
  try:
   response=apply('version = 2\ndefault = "show"\n[[column]]\ntable="public.customers"\nname="email"\nmasked=true\nplaceholder="[tightened]"\n','tighten')
   out['response']=response;out['response_seconds']=time.monotonic()
   if response.get('activation')=='active':out['active_seconds']=out['response_seconds'];return
   deadline=time.monotonic()+30
   while time.monotonic()<deadline:
    state=must(holder());now=time.monotonic();a=state.get('activation',{});events.append({'seconds':now,'activation':a})
    if a.get('state')=='active':out['active_seconds']=now;return
    time.sleep(.025)
   out['error']='activation timeout'
  except Exception as e:out['error']=str(e)
  finally:done.set()
 th=threading.Thread(target=mutate);th.start();refused=None
 try:
  deadline=time.monotonic()+30
  while time.monotonic()<deadline:
   try:q.fetchmany(1)
   except (psycopg.Error,WireError) as e:refused={'seconds':time.monotonic(),'sqlstate':e.sqlstate};break
   time.sleep(.025)
 finally:q.close();th.join(timeout=65)
 ev('scenario5',{'mutation':out,'polls':events,'refused':refused})
 assert done.is_set() and 'error' not in out and refused and refused['sqlstate']=='NY008' and refused['seconds']<out['active_seconds'],out
 EXPECTED='[tightened]';return {'NY008':True,'before_active_seconds':out['active_seconds']-refused['seconds'],'follower':follower}
def restart():
 expected=must(holder())['active_revision']['number'];old=next(p for p in names() if p!=holder());existing=set(names())
 kub(['delete','pod',old,'--wait=false'],'restart-delete');p=poll(lambda:next((p for p in names() if p not in existing),None));observations=[];deadline=time.monotonic()+120
 while time.monotonic()<deadline:
  try:value=query(p,SURVIVE)
  except Exception as e:
   observations.append({'served':False,'sqlstate':getattr(e,'sqlstate',None)});time.sleep(.2);continue
  # This sample is intentionally AFTER the first successful query.
  leases=data(['get','leases'],'restart-leases-after-query');entry=next((x for x in leases['items'] if x['metadata']['name']=='nunya-serving-'+p),None)
  result={'replacement':p,'expected_revision':expected,'first_value_sha256':hashlib.sha256(value.encode()).hexdigest(),'matches_current_policy':value==EXPECTED,'lease_after_query':entry,'earlier_refusals':observations};ev('scenario7',result)
  assert value==EXPECTED=='[tightened]' and entry is not None
  annotations=entry['metadata'].get('annotations',{});assert json.loads(annotations.get('nunya.gigglesdb.dev/confirmed','{}')).get('postgres')==expected,annotations
  return result
 raise AssertionError('replacement did not serve within 120 seconds')
def run_scenarios():
 import base_live as b
 funcs=[stable,shared,revoke,limits,kill,tighten,partition,restart,network]
 deps={0:[],1:[0],2:[1],3:[1],4:[1,3],5:[1],6:[1],7:[5],8:[]}
 for n,fn in enumerate(funcs):
  blocked=[i for i in deps[n] if b.results[str(i)]['status']!='PASS']
  print('SCENARIO',n,fn.__name__,flush=True)
  if blocked:r={'status':'BLOCKED','prerequisites':blocked}
  else:
   try:r={'status':'PASS','details':fn()}
   except Exception as e:
    r={'status':'FAIL','error':str(e)};(D/f'scenario{n}-failure.txt').write_text(traceback.format_exc(),newline='\n')
  b.results[str(n)]=r;ev('scenario'+str(n)+'-result',r);ev('results',b.results);print(n,r['status'],flush=True)
