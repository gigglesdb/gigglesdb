"""One-shot private-kind proof. Run with python -I run.py run|teardown."""
import sys
sys.dont_write_bytecode=True
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parent))
import base_live as b
import render, scenarios as s
import json, os, shutil, subprocess, time, tarfile, io, urllib.request, traceback
P=Path(__file__).resolve().parent;D=b.D;KC=b.env['KUBECONFIG'];KIND='J:/gigglesdb-work/tools/bin/kind.exe';NAME='nunya-8c5';BUILDER=NAME+'-builder'
NODES=[NAME+'-control-plane',NAME+'-worker',NAME+'-worker2']
os.environ['TEMP']=os.environ['TMP']=str(D/'tmp');os.environ['PYTHONDONTWRITEBYTECODE']='1';sys.dont_write_bytecode=True

def snapshot(prefix):
 for args,label in [(['image','ls','--no-trunc'],'images'),(['ps','-a','--no-trunc'],'containers'),(['volume','ls'],'volumes'),(['network','ls'],'networks'),(['buildx','ls'],'builders'),(['image','ls','-q','--no-trunc'],'image-ids')]:b.command(['docker',*args],prefix+'-'+label)
 (D/(prefix+'-space.json')).write_text(json.dumps({'C_free_bytes':shutil.disk_usage('C:/').free}))

def image_load(image):
 label=image.replace(':','-').replace('/','-');srcpath=D/'tmp/image.tar';dstpath=D/'tmp/amd64.tar'
 b.command(['docker','image','save',image,'-o',str(srcpath)],'save-'+label)
 with tarfile.open(srcpath) as src,tarfile.open(dstpath,'w') as dst:
  manifests=json.load(src.extractfile('manifest.json'));selected=[m for m in manifests if json.load(src.extractfile(m['Config']))['architecture']=='amd64'];assert len(selected)==1
  for name in {selected[0]['Config'],*selected[0]['Layers']}:
   entry=src.getmember(name);dst.addfile(entry,src.extractfile(entry))
  raw=json.dumps(selected).encode();entry=tarfile.TarInfo('manifest.json');entry.size=len(raw);dst.addfile(entry,io.BytesIO(raw))
 for node in NODES:
  b.guard();b.command(['docker','cp',str(dstpath),node+':/task-image.tar'],'copy-'+label+'-'+node)
  b.guard();b.command(['docker','exec',node,'ctr','-n','k8s.io','images','import','--all-platforms','/task-image.tar'],'import-'+label+'-'+node)
  b.guard();b.command(['docker','exec',node,'rm','/task-image.tar'],'rm-tar-'+node)
 srcpath.unlink();dstpath.unlink()

def setup():
 if Path(KC).exists():raise RuntimeError('Private kubeconfig exists; refusing to overwrite it')
 snapshot('pre')
 # Seed only the private file so the mandatory guard also precedes kind create.
 Path(KC).parent.mkdir(parents=True,exist_ok=True)
 Path(KC).write_text('apiVersion: v1\nkind: Config\ncurrent-context: kind-nunya-8c5\ncontexts:\n- name: kind-nunya-8c5\n  context:\n    cluster: kind-nunya-8c5\n    user: kind-nunya-8c5\nclusters: []\nusers: []\n',newline='\n')
 b.guard();b.command([KIND,'create','cluster','--name',NAME,'--kubeconfig',KC,'--config',str(P/'kind.yaml'),'--wait','0s'],'cluster-create')
 b.guard()
 url='https://raw.githubusercontent.com/projectcalico/calico/v3.31.2/manifests/calico.yaml'
 raw=urllib.request.urlopen(url,timeout=60).read().decode();raw=raw.replace('# - name: CALICO_IPV4POOL_CIDR\n            #   value: \"192.168.0.0/16\"','- name: CALICO_IPV4POOL_CIDR\n              value: \"10.244.0.0/16\"');(D/'calico.yaml').write_text(raw,newline='\n')
 b.kub(['apply','--server-side','-n','kube-system','-f',str(D/'calico.yaml')],'calico-install')
 try:b.kub(['rollout','status','daemonset/calico-node','-n','kube-system','--timeout=240s'],'calico-ready')
 except Exception:
  # No redeployment of Nunya: fallback occurs before the one application deployment.
  b.kub(['delete','-n','kube-system','-f',str(D/'calico.yaml'),'--ignore-not-found'],'calico-remove')
  raise RuntimeError('Calico failed; NetworkPolicy UNPROVEN; kindnet fallback not attempted because node CNI restoration needs separate qualification')
 b.kub(['wait','--for=condition=Ready','nodes','--all','--timeout=120s'],'nodes-ready')
 b.command(['docker','buildx','create','--name',BUILDER,'--driver','docker-container'],'builder-create')
 b.command(['docker','buildx','build','--builder',BUILDER,'--load','-t','nunya:gd88-8c5','--build-arg','FEATURES=kubernetes','-f',str(b.WT/'Dockerfile'),str(b.WT.parent.parent),'--progress','plain'],'image-build')
 for image in ['nunya:gd88-8c5','postgres:18-alpine','alpine:3.21.3']:
  if not image.startswith('nunya:'):b.command(['docker','pull',image],'pull-'+image.replace(':','-'))
  image_load(image)
 for f in ['namespace.yaml','crds.yaml','rbac.yaml']:b.kub(['apply','-f',str(b.WT/'deploy/kubernetes'/f)],'apply-'+f)
 render.render(b)
 for f in ['networkpolicy.yaml','pdb.yaml']:b.kub(['apply','-f',str(b.WT/'deploy/kubernetes'/f)],'apply-'+f)
 b.kub(['rollout','status','deployment/postgres','--timeout=120s'],'postgres-ready')
 b.kub(['apply','-f',str(D/'deployment.json')],'single-deployment')

def teardown():
 for _,proc in b.forwards.values():
  if proc.poll() is None:proc.terminate();proc.wait(timeout=10)
 for f in b.handles:f.close()
 nodeimages=[]
 for node in NODES:
  r=subprocess.run(['docker','inspect',node,'--format','{{.Image}}'],capture_output=True,text=True,env=b.env)
  if r.returncode==0:nodeimages.append(r.stdout.strip())
 if Path(KC).exists():
  b.guard();b.command([KIND,'delete','cluster','--name',NAME,'--kubeconfig',KC],'cluster-delete');Path(KC).unlink()
 b.command(['docker','buildx','rm',BUILDER],'builder-delete',False)
 pre=set((D/'pre-image-ids.log').read_text().splitlines()) if (D/'pre-image-ids.log').exists() else set()
 for tag in ['nunya:gd88-8c5','postgres:18-alpine','alpine:3.21.3','rust:1-bookworm','debian:bookworm-slim','moby/buildkit:buildx-stable-1',*set(nodeimages)]:
  r=subprocess.run(['docker','image','inspect',tag,'--format','{{.Id}}'],capture_output=True,text=True,env=b.env)
  if r.returncode==0 and r.stdout.strip() not in pre:b.command(['docker','image','rm',tag],'delete-image-'+tag.replace(':','-').replace('/','-'),False)
 r=subprocess.run(['docker','network','inspect','kind'],capture_output=True,text=True,env=b.env)
 if r.returncode==0 and not any(line.split()[-1:]==['kind'] for line in (D/'pre-networks.log').read_text().splitlines()) and not json.loads(r.stdout)[0].get('Containers'):b.command(['docker','network','rm','kind'],'delete-kind-network')
 snapshot('post')
 tmp=(D/'tmp').resolve();assert tmp.parent==D.resolve()
 if tmp.exists():shutil.rmtree(tmp)
 (D/'cleanup-proof.json').write_text(json.dumps({'kubeconfig_exists':Path(KC).exists(),'nodes_absent':all(n not in (D/'post-containers.log').read_text() for n in NODES),'builder_absent':BUILDER not in (D/'post-builders.log').read_text()},indent=2))

def main():
 if sys.argv[1:] == ['teardown']:teardown();return
 if sys.argv[1:] != ['run']:raise SystemExit('usage: python -I run.py run|teardown')
 if Path(KC).exists():raise SystemExit('Private kubeconfig already exists; no mutation performed')
 try:
  setup();import importlib;importlib.reload(s);s.run_scenarios()
 except Exception:
  (D/'setup-failure.txt').write_text(traceback.format_exc(),newline='\n');print(traceback.format_exc(),flush=True)
  for n in range(9):
   if str(n) not in b.results:b.results[str(n)]={'status':'BLOCKED','reason':'setup prerequisite failed'}
 finally:
  if Path(KC).exists():
   try:
    b.kub(['get','pods','-o','wide'],'final-pods',False);b.kub(['get','leases','-o','json'],'final-leases',False)
    for p in b.pods():b.kub(['logs',p['metadata']['name'],'-c','nunya','--timestamps','--tail=250'],'final-'+p['metadata']['name'],False)
   except Exception:pass
  (D/'results.json').write_text(json.dumps(b.results,indent=2),newline='\n')
  teardown()
 print(json.dumps(b.results,indent=2),flush=True)
if __name__=='__main__':
 main()
 if sys.argv[1:]==['run'] and any(r['status']!='PASS' for r in b.results.values()):raise SystemExit(1)
