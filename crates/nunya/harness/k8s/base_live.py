"""Local kind qualification. Evidence stays in this directory; run.sh owns teardown."""
from pathlib import Path
import concurrent.futures,datetime,http.client,json,os,socket,ssl,subprocess,time,traceback,urllib.parse
from render import verifier
D=Path('J:/gigglesdb-work/nunya/gd88/live-8c5'); WT=Path('J:/gigglesdb-work/wt/gd88-w1/crates/nunya')
K='C:/ProgramData/chocolatey/bin/kubectl.exe'; NS='nunya'; env=os.environ.copy(); env['KUBECONFIG']='J:/gigglesdb-work/nunya/gd88/kind/kubeconfig'
env['TEMP']=env['TMP']=str(D/'tmp'); (D/'tmp').mkdir(exist_ok=True)
results={}; forwards={}; handles=[]; counter=0
def command(args,label,check=True,input=None):
 print('COMMAND',label,flush=True)
 with (D/(label+'.log')).open('wb') as log:
  p=subprocess.run(args,input=input,text=True,encoding='utf-8',errors='replace',stdout=log,stderr=subprocess.STDOUT,env=env)
 output=(D/(label+'.log')).read_text(encoding='utf-8',errors='replace')
 if check and p.returncode:raise RuntimeError(f'{label}: exit {p.returncode}; see log')
 return output[output.index('{'):] if args[0]==K and '-o' in args and args[args.index('-o')+1]=='json' and '{' in output else output

def guard():
 context=command([K,'--kubeconfig',env['KUBECONFIG'],'config','current-context'],'context-guard').strip()
 assert context=='kind-nunya-8c5', 'unsafe context: '+context
def kub(args,label,check=True,input=None):
 if args[0] in ['apply','delete','create','replace','patch','exec']: guard()
 return command([K,'--kubeconfig',env['KUBECONFIG'],'-n',NS,*args],label,check,input)
def data(args,label): return json.loads(kub([*args,'-o','json'],label))
def pods(): return data(['get','pods','-l','app.kubernetes.io/name=nunya'],'pods')['items']
def holder(): return data(['get','lease','nunya-leader'],'lease')['spec'].get('holderIdentity')
def wait_leader(old=None):
 start=time.monotonic()
 while time.monotonic()-start<60:
  try:
   h=holder()
   if h and h!=old and any(p['metadata']['name']==h and not p['metadata'].get('deletionTimestamp') for p in pods()):
    time.sleep(3); return h,round(time.monotonic()-start,3)
  except Exception: pass
  time.sleep(.5)
 raise RuntimeError('no leader takeover within 60 seconds')
def pf(pod,remote=6481):
 key=(pod,remote)
 if key in forwards and forwards[key][1].poll() is None: return forwards[key][0]
 sock=socket.socket(); sock.bind(('127.0.0.1',0)); port=sock.getsockname()[1]; sock.close()
 f=(D/f'pf-{pod}-{remote}.log').open('w'); handles.append(f)
 p=subprocess.Popen([K,'--kubeconfig',env['KUBECONFIG'],'-n',NS,'port-forward','pod/'+pod,f'{port}:{remote}'],stdout=f,stderr=subprocess.STDOUT,env=env)
 forwards[key]=(port,p)
 start=time.monotonic()
 while time.monotonic()-start<20:
  if p.poll() is not None: raise RuntimeError('port-forward exited: '+pod)
  try:
   with socket.create_connection(('127.0.0.1',port),timeout=.2): return port
  except OSError: time.sleep(.1)
 raise RuntimeError('port-forward not ready')
def request(pod,op='',body=None,key=None,follow=True,path=None,auth='operator.live-admin-8a',label=None):
 global counter
 path=path or '/v1/routes/postgres/policy'+('/'+op if op else '')
 headers={'Authorization':'Bearer nunya.'+auth}
 if key: headers['Idempotency-Key']=key
 payload=None if body is None else json.dumps(body)
 if payload is not None: headers['Content-Type']='application/json'
 method='POST' if body is not None else 'GET'
 counter+=1; label=label or f'http-{counter}'
 port=pf(pod)
 args=['curl.exe','--ssl-no-revoke','--noproxy','*','--cacert',str(D/'ca.crt'),'-sS','--max-time','30','-X',method,'-D',str(D/(label+'.headers')),'-o',str(D/(label+'.body')),'-w','%{http_code}']
 for k,v in headers.items(): args+=['-H',k+': '+v]
 if payload is not None: args+=['--data-binary','@-']
 args+=[f'https://localhost:{port}'+path]
 response=subprocess.run(args,input=payload,text=True,capture_output=True,env=env)
 if response.returncode: raise RuntimeError(label+': curl '+response.stderr)
 status=int(response.stdout); text=(D/(label+'.body')).read_text()
 hs={k.strip().lower():v.strip() for line in (D/(label+'.headers')).read_text().splitlines() if ':' in line for k,v in [line.split(':',1)]}
 (D/(label+'.json')).write_text(json.dumps({'pod':pod,'method':method,'path':path,'status':status,'headers':hs,'body':json.loads(text) if text else None},indent=2),newline='\n')
 if status==307 and follow:
  loc=hs.get('location') or hs.get('Location'); target=urllib.parse.urlparse(loc)
  dest=next(p['metadata']['name'] for p in pods() if p['status'].get('podIP')==target.hostname)
  return request(dest,body=body,key=key,follow=False,path=target.path,auth=auth,label=label+'-redirected')
 return status,json.loads(text) if text else None,hs
def must(pod,op='',body=None,key=None,**kw):
 s,b,_=request(pod,op,body,key,**kw)
 assert 200<=s<300,(op,s,b)
 return b
