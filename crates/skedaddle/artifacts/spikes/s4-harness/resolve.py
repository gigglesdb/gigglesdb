"""Resolve once; all runtime references are immutable. Never removes local files."""
import datetime, json, pathlib, shutil, subprocess, sys
sys.dont_write_bytecode = True
ROOT = pathlib.Path(__file__).resolve().parent
OUT = ROOT.parents[1] / 's4-20261106-implementation'
def call(*args, check=True):
    p = subprocess.run(args, capture_output=True, text=True, encoding='utf8', errors='replace')
    if check and p.returncode: raise RuntimeError(p.stderr or p.stdout)
    return p.stdout + p.stderr
def write(name, data):
    (OUT/name).write_text(data, encoding='utf8')
if __name__ == '__main__':
    if shutil.disk_usage('C:/').free < 8*1024**3: raise RuntimeError('Initial pull guard: need 8 GiB on C:')
    write('df-before-current-pulls.txt',call('docker','system','df',check=False))
    write('host-before-current-pulls.json',json.dumps({'c_free':shutil.disk_usage('C:/').free,'vhdx':pathlib.Path('C:/Users/beach/AppData/Local/Docker/wsl/disk/docker_data.vhdx').stat().st_size}))
    lock=json.loads((ROOT/('images.lock' if (ROOT/'images.lock').exists() else 'images.prior-20261106.lock')).read_text())
    lock.update(pg19_status='RC1; GA planned 2026-11-19, NOT GA as of 2026-11-06',docker_hub_source='https://hub.docker.com/_/postgres',pgbouncer_source='https://github.com/edoburu/docker-pgbouncer/releases',pgbouncer_reason='Maintained Alpine image; upstream release activity and minimal footprint')
    tags={'probe':'busybox:1.37.0','pg16':'postgres:16-alpine','pg17':'postgres:17-alpine','pg18':'postgres:18-alpine','minio':'quay.io/thanos/minio:RELEASE.2025-09-07T16-13-09Z','gcs':'fsouza/fake-gcs-server:latest','toxiproxy':'ghcr.io/shopify/toxiproxy:latest','pgbouncer':'edoburu/pgbouncer:latest'}
    for key,tag in tags.items():
        if key not in lock['images']:
            print('RESOLVE',tag,flush=True)
            manifest=json.loads(call('docker','buildx','imagetools','inspect',tag,'--format','{{json .Manifest}}'))
            lock['images'][key]={'tag':tag,'ref':tag.rsplit(':',1)[0]+'@'+manifest['digest']}
        item=lock['images'][key]
        print('PULL',item['ref'],flush=True)
        pull=call('docker','pull',item['ref'])
        write('pull-'+key+'.txt',pull)
        obj=json.loads(call('docker','image','inspect',item['ref']))[0]
        item.update(size_bytes=obj['Size'],id=obj['Id'])
        (ROOT/'images.lock').write_text(json.dumps(lock,indent=2)+'\n',encoding='utf8')
        if key=='probe':
            vm=call('docker','run','--rm','--label','skedaddle.test=s4-initial-probe','--name','skedaddle-s4-initial-probe',item['ref'],'df','-Pk','/')
            write('vm-before-current-pulls.txt',vm)
            if int(vm.splitlines()[-1].split()[3])*1024 < 8*1024**3: raise RuntimeError('Docker VM initial guard: need 8 GiB')
    write('df-after-pulls.txt',call('docker','system','df',check=False))
    write('images.json',json.dumps(lock,indent=2))
    print('TOTAL image inspect bytes',sum(i['size_bytes'] for i in lock['images'].values()),flush=True)
