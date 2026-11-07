import pathlib, os, sys, subprocess, json, datetime, hashlib
R=pathlib.Path(__file__).resolve().parent; BASE=R.parents[2]; RUN=sys.argv[1]
OUT=BASE/'artifacts'/('s3b-'+RUN); OUT.mkdir(exist_ok=False)
env=os.environ.copy(); env.update(CARGO_BUILD_JOBS='2',CARGO_TARGET_DIR=str(BASE/'target/s3'),CARGO_HOME=str(R.parent/'s1-replication/cargo-home'),TEMP=str(BASE/'target/s3'),TMP=str(BASE/'target/s3'),PYTHONDONTWRITEBYTECODE='1')
def command(name,args,timeout):
    print('RUN',args,flush=True)
    p=subprocess.run(args,env=env,cwd=BASE,capture_output=True,text=True,timeout=timeout)
    (OUT/(name+'.json')).write_text(json.dumps(dict(command=args,exit=p.returncode,stdout=p.stdout,stderr=p.stderr),indent=2))
    print(p.stdout,flush=True); print(p.stderr,flush=True)
    return p.returncode
if command('prepare',[sys.executable,'-B',str(R/'prepare.py')],30): sys.exit(1)
if command('build',['cargo','build','--locked','--offline','--manifest-path',str(R/'Cargo.toml')],300): sys.exit(1)
(OUT/'source-manifest.json').write_text(json.dumps([dict(path=str(p),bytes=p.stat().st_size,sha256=hashlib.sha256(p.read_bytes()).hexdigest()) for p in R.rglob('*') if p.is_file()],indent=2))
status=0
for major in (16,17,18):
    status |= command('pg'+str(major),[sys.executable,'-B',str(R/'run.py'),str(major),RUN+'-pg'+str(major),str(OUT/('pg'+str(major)))],340)
(OUT/'campaign.json').write_text(json.dumps(dict(run=RUN,exit=status,utc=datetime.datetime.now(datetime.timezone.utc).isoformat())))
sys.exit(status)
