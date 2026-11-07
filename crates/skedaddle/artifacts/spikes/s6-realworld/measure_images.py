"""Measure uncompressed layer tar bytes by streaming Docker save; no disk archive.
Docker's containerd image inspect Size here is compressed, so report it separately.
"""
import gzip, json, pathlib, subprocess, sys, tarfile
ROOT=pathlib.Path(__file__).resolve().parent
OUT=ROOT.parents[1]/('s6-'+sys.argv[1])
images=json.loads((OUT/'images.lock.json').read_text())
sizes={}; measured={}
for name,item in images.items():
    wanted={layer['digest'].split(':')[1] for layer in item['manifest']['layers']}
    missing=wanted-set(sizes)
    if missing:
        proc=subprocess.Popen(['docker','image','save',item['ref']],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        with tarfile.open(fileobj=proc.stdout,mode='r|') as archive:
            for member in archive:
                digest=member.name.rsplit('/',1)[-1]
                if digest not in missing or not member.isfile():continue
                stream=archive.extractfile(member)
                # OCI gzip blobs are identified by the registry descriptor media type.
                layer=next(l for l in item['manifest']['layers'] if l['digest'].endswith(digest))
                source=gzip.GzipFile(fileobj=stream) if layer['mediaType'].endswith('+gzip') or layer['mediaType'].endswith('.gzip') else stream
                count=0
                while True:
                    data=source.read(1024*1024)
                    if not data:break
                    count+=len(data)
                sizes[digest]=count
        proc.stdout.close();err=proc.stderr.read().decode();code=proc.wait()
        if code:raise RuntimeError(err)
    if wanted-set(sizes):raise RuntimeError('Missing layer measurements '+str(wanted-set(sizes)))
    measured[name]={'ref':item['ref'],'tag':item['tag'],'compressed_bytes':item['compressed_bytes'],'uncompressed_layer_tar_bytes':sum(sizes[d] for d in wanted),'docker_image_inspect_size_bytes':item.get('docker_image_inspect_size_bytes',item.get('uncompressed_bytes'))}
    print(name,json.dumps(measured[name]),flush=True)
result={'images':measured,'unique_uncompressed_layer_tar_bytes':sum(sizes.values()),'unique_compressed_bytes':sum({l['digest']:l['size'] for i in images.values() for l in i['manifest']['layers']}.values()),'method':'Stream docker image save OCI blobs through gzip, count uncompressed tar bytes; deduplicate shared layer digests. No tar file written.'}
with (OUT/'images-measured.json').open('x',encoding='utf8') as f:json.dump(result,f,indent=2)
