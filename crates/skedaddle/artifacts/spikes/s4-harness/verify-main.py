"""Real-service verification driver; preserves every output and uses fresh run IDs."""
import json, pathlib, subprocess, sys, time
sys.dont_write_bytecode=True
ROOT=pathlib.Path(__file__).resolve().parent
OUT=ROOT.parents[1]/'s4-20261106-implementation'
def ps(name,extra):
    script=f"& '{ROOT / 'run.ps1'}' -RunId 20261106-{name} {extra}"
    with (OUT/(name+'-console.txt')).open('x',encoding='utf8') as f:
        p=subprocess.Popen(['powershell','-NoProfile','-ExecutionPolicy','Bypass','-Command',script],stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,encoding='utf8',errors='replace')
        for line in p.stdout: print(line,end='',flush=True);f.write(line);f.flush()
        rc=p.wait()
    print(name,'EXIT',rc,flush=True)
    return rc
if __name__=='__main__':
    for major in (16,17,18):
        rc=ps('b'+str(major)+'v2',f"-PgMajor {major} -Workload {{ Invoke-Engine 'smoke'; Invoke-Engine 'privileges' }}")
        if rc: sys.exit(rc)
    rc=ps('large','-PgMajor 18 -Mode large')
    sys.exit(rc)
