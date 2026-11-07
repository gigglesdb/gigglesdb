"""Sequential final matrix; run.py guarantees export/teardown on failures."""
import json, os, pathlib, subprocess, sys
root=pathlib.Path(__file__).resolve().parent
campaign=sys.argv[1] if len(sys.argv)>1 else '20261106'
out=root.parents[1]/('s1-verification-'+campaign)
out.mkdir(exist_ok=False)
results=[]
for major in [16,17,18]:
    command=[sys.executable,'-B',str(root/'run.py'),str(major),'s1-pg%d-final-%s'%(major,campaign)]
    p=subprocess.run(command,capture_output=True,text=True,encoding='utf8',errors='replace')
    result={'command':command,'exit':p.returncode,'stdout':p.stdout,'stderr':p.stderr}
    with (out/('pg%d.json'%major)).open('x',encoding='utf8') as f:json.dump(result,f,indent=2)
    results.append(result)
    print('PG',major,'exit',p.returncode,p.stdout,p.stderr,flush=True)
with (out/'summary.json').open('x',encoding='utf8') as f:json.dump({'runs':len(results),'passed':sum(r['exit']==0 for r in results),'failed':sum(r['exit']!=0 for r in results)},f,indent=2)
sys.exit(int(any(r['exit'] for r in results)))
