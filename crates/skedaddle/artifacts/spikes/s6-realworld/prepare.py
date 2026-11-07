"""Read registry metadata, retain official sources, pull digest-pinned bounded images.
No deletion, no host temp files, no Docker resource creation or cleanup.
"""
import datetime, hashlib, importlib.util, json, os, pathlib, shutil, subprocess, sys, urllib.request
sys.dont_write_bytecode = True
ROOT = pathlib.Path(__file__).resolve().parent
RUN = sys.argv[1]
OUT = ROOT.parents[1] / ('s6-' + RUN)
OUT.mkdir(exist_ok=False)
def save(name, obj):
    with (OUT/name).open('x', encoding='utf8') as f:
        f.write(obj if isinstance(obj, str) else json.dumps(obj, indent=2))
def fetch(url, headers=None):
    req = urllib.request.Request(url, headers={'User-Agent':'Skedaddle-S6-research', **(headers or {})})
    with urllib.request.urlopen(req, timeout=90) as r:
        data = r.read(10*1024*1024+1)
        if len(data)>10*1024*1024: raise RuntimeError('Metadata exceeded bound')
        return data, dict(r.headers)
def get(url, headers=None): return json.loads(fetch(url, headers)[0])
save('research-start.json', {'run_id':RUN,'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'c_free':shutil.disk_usage('C:/').free})
images = {}
accept = {'Accept': 'application/vnd.oci.image.index.v1+json, application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json'}
for name, registry, repo, tag in [
    ('miniflux-old','registry-1.docker.io','miniflux/miniflux','2.2.13'),
    ('miniflux-new','registry-1.docker.io','miniflux/miniflux','2.2.14'),
    ('gitea','registry-1.docker.io','gitea/gitea','1.24.6'),
    ('keycloak','quay.io','keycloak/keycloak','26.7.5')]:
    headers = dict(accept)
    if registry == 'registry-1.docker.io':
        token = get('https://auth.docker.io/token?service=registry.docker.io&scope=repository:'+repo+':pull')['token']
        headers['Authorization'] = 'Bearer '+token
    base='https://'+registry+'/v2/'+repo
    raw, response = fetch(base+'/manifests/'+tag, headers)
    index=json.loads(raw)
    digest=response.get('Docker-Content-Digest') or response.get('docker-content-digest') or 'sha256:'+hashlib.sha256(raw).hexdigest()
    if 'manifests' in index:
        child=next(m for m in index['manifests'] if m.get('platform',{}).get('os')=='linux' and m['platform'].get('architecture')=='amd64')['digest']
        manifest=get(base+'/manifests/'+child,headers)
    else: child=digest; manifest=index
    config=get(base+'/blobs/'+manifest['config']['digest'],headers)
    ref=('quay.io/' if registry=='quay.io' else '')+repo+'@'+child
    images[name]={'tag':repo+':'+tag,'index_digest':digest,'ref':ref,'compressed_bytes':sum(l['size'] for l in manifest['layers']),'manifest':manifest,'config':config}
    print(name,ref,images[name]['compressed_bytes'],flush=True)
save('images-resolved.json',images)
# Future cold runs guard both C: and the Docker filesystem before app pulls.
os.environ.update(SKEDADDLE_RUN_ID=RUN,SKEDADDLE_PG_MAJOR='18',PYTHONDONTWRITEBYTECODE='1')
spec=importlib.util.spec_from_file_location('s4_pull_guard',ROOT.parent/'s4-harness'/'harness.py')
h=importlib.util.module_from_spec(spec);spec.loader.exec_module(h)
h.OUT=OUT/'pull-guard'
for name,item in images.items():h.IMAGES['s6-'+name]={'ref':item['ref'],'size_bytes':item['compressed_bytes']}
h.guard()
sources={
 'miniflux-requirements':'https://miniflux.app/docs/requirements.html',
 'miniflux-api':'https://miniflux.app/docs/api.html',
 'miniflux-config':'https://miniflux.app/docs/configuration.html',
 'miniflux-license':'https://raw.githubusercontent.com/miniflux/v2/2.2.14/LICENSE',
 'gitea-requirements':'https://docs.gitea.com/installation/database-prep/',
 'gitea-upgrade':'https://docs.gitea.com/installation/upgrade-from-gitea/',
 'gitea-license':'https://raw.githubusercontent.com/go-gitea/gitea/v1.24.6/LICENSE',
 'keycloak-database':'https://www.keycloak.org/server/db',
 'keycloak-upgrade':'https://www.keycloak.org/docs/26.7.5/upgrading/index.html',
 'keycloak-license':'https://raw.githubusercontent.com/keycloak/keycloak/26.7.5/LICENSE.txt',
 'chinook-readme':'https://raw.githubusercontent.com/lerocha/chinook-database/master/README.md',
 'chinook-license':'https://raw.githubusercontent.com/lerocha/chinook-database/master/LICENSE.md',
 'chinook-tree':'https://api.github.com/repos/lerocha/chinook-database/git/trees/master?recursive=1',
 'pagila-readme':'https://raw.githubusercontent.com/devrimgunduz/pagila/master/README.md',
 'pagila-license':'https://raw.githubusercontent.com/devrimgunduz/pagila/master/LICENSE',
 'pagila-tree':'https://api.github.com/repos/devrimgunduz/pagila/git/trees/master?recursive=1',
 'monaco':'https://download.geofabrik.de/europe/monaco.html',
 'osm-license':'https://www.openstreetmap.org/copyright',
}
results={}
for name,url in sources.items():
    try:
        raw,headers=fetch(url)
        save('source-'+name+'.txt',raw.decode('utf8'))
        results[name]={'url':url,'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()}
    except Exception as e: results[name]={'url':url,'error':str(e)}
save('sources.json',results)
before=json.loads(subprocess.run(['docker','image','ls','--no-trunc','--format','{{json .}}'],capture_output=True,text=True,check=True).stdout.replace('\n',',').rstrip(',').join(['[',']']))
save('images-before.json',before)
total=0
for name,item in images.items():
    p=subprocess.run(['docker','pull',item['ref']],capture_output=True,text=True,timeout=300)
    save('pull-'+name+'.json',{'exit':p.returncode,'stdout':p.stdout,'stderr':p.stderr})
    if p.returncode: raise RuntimeError('pull failed '+name)
    obj=json.loads(subprocess.run(['docker','image','inspect',item['ref']],capture_output=True,text=True,check=True).stdout)[0]
    item['docker_image_inspect_size_bytes']=obj['Size'];item['image_id']=obj['Id'];total+=obj['Size']
    print('PULLED',name,obj['Size'],flush=True)
    if total>6*1024**3: raise RuntimeError('Image allowance exceeded')
save('images.lock.json',images)
save('image-footprint.json',{'sum_image_inspect_size_bytes':total,'note':'Conservative sum of four images; shared layers counted repeatedly. Compressed bytes are registry linux/amd64 layer sums.'})
print('PREPARED',OUT,flush=True)
