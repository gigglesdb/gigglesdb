"""Real failure, signal, hard-kill, guard and inventory verification. No file cleanup."""
import ctypes, json, os, pathlib, subprocess, sys, threading, time
sys.dont_write_bytecode=True
ROOT=pathlib.Path(__file__).resolve().parent
OUT=ROOT.parents[1]/'s4-20261106-implementation'
BASH=r'C:\Program Files\Git\bin\bash.exe'
RESULTS={}
def write(name,obj):
    with (OUT/name).open('x',encoding='utf8') as f:f.write(obj if isinstance(obj,str) else json.dumps(obj,indent=2))
def call(args,env=None):
    p=subprocess.run(args,env=env,stdin=subprocess.PIPE,capture_output=True,text=True,encoding='utf8',errors='replace',timeout=240)
    return {'exit':p.returncode,'stdout':p.stdout,'stderr':p.stderr}
def psargs(script):return ['powershell','-NoProfile','-ExecutionPolicy','Bypass','-Command',script]
def psrun(run,extra):return psargs(f"& '{ROOT/'run.ps1'}' -RunId 20261106-{run} {extra}")
def check():return call(['powershell','-NoProfile','-ExecutionPolicy','Bypass','-File',str(ROOT/'cleanup.ps1'),'-Check'])
def count(run):
    found={}
    for name,args in [('containers',['ps','-aq']),('volumes',['volume','ls','-q']),('networks',['network','ls','-q'])]:
        found[name]=call(['docker',*args,'--filter','label=skedaddle.test=20261106-'+run])['stdout'].splitlines()
    return found
def start(args,name,env=None,new_console=False):
    flags=0;startup=None
    if new_console:
        flags=subprocess.CREATE_NEW_CONSOLE
        startup=subprocess.STARTUPINFO();startup.dwFlags=subprocess.STARTF_USESHOWWINDOW;startup.wShowWindow=0
    p=subprocess.Popen(args,env=env,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,encoding='utf8',errors='replace',creationflags=flags,startupinfo=startup)
    ready=threading.Event();lines=[]
    f=(OUT/(name+'-console.txt')).open('x',encoding='utf8')
    def read():
        for line in p.stdout:
            lines.append(line);f.write(line);f.flush();print(name+': '+line,end='',flush=True)
            if 'HOLD READY' in line:ready.set()
        f.close()
    t=threading.Thread(target=read,daemon=True);t.start()
    deadline=time.monotonic()+200
    while not ready.wait(.25):
        if p.poll() is not None or time.monotonic()>deadline: raise RuntimeError(name+' did not become ready: '+''.join(lines))
    return p,t
def finish(p,t,timeout=150):
    rc=p.wait(timeout);t.join(timeout=10);return rc
def env(run,**kwargs):
    e=os.environ.copy();e.update(RUN_ID='20261106-'+run,**kwargs);return e
def early():
    # PowerShell callback actually throws after healthy services are up.
    r=call(psrun('efailps',"-Workload { throw 'Intentional PowerShell callback failure' }"))
    RESULTS['powershell_throw']={'run':r,'check':check()};write('efailps-result.json',RESULTS['powershell_throw'])
    print('E PS',r['exit'],RESULTS['powershell_throw']['check'],flush=True)
    # Shell callback actually exits nonzero. Small real export budget proves bounded streaming.
    e=env('efailsh',WORKLOAD='exit 23',SKEDADDLE_EXPORT_RAW_LIMIT='1024')
    r=call([BASH,'--noprofile','--norc',str(ROOT/'run.sh')],env=e)
    RESULTS['bash_failure']={'run':r,'check':check()};write('efailsh-result.json',RESULTS['bash_failure'])
    print('E SH',r,flush=True)
    for sig in ('INT','TERM'):
        run='sig'+sig.lower();e=env(run,WORKLOAD='echo HOLD READY; while true; do sleep 1; done')
        p,t=start([BASH,'--noprofile','--norc',str(ROOT/'run.sh')],run,env=e)
        owner=json.loads((ROOT.parents[1]/('s4-20261106-'+run)/'owner.json').read_text())
        signal_result=call([BASH,'--noprofile','--norc','-c','kill -s '+sig+' '+owner['msys_pid']])
        try:rc=finish(p,t)
        except subprocess.TimeoutExpired:
            # End only the owned wrapper; preserve Docker resources for explicit cleanup.
            call(['taskkill','/F','/PID',str(p.pid)]);rc=finish(p,t)
        RESULTS['bash_sig'+sig]={'signal':signal_result,'exit':rc,'check':check(),'leftovers':count(run)}
        write(run+'-result.json',RESULTS['bash_sig'+sig]);print('F',sig,RESULTS['bash_sig'+sig],flush=True)
        if RESULTS['bash_sig'+sig]['check']['exit']:
            write(run+'-recovery.json',call(['powershell','-NoProfile','-ExecutionPolicy','Bypass','-File',str(ROOT/'cleanup.ps1')]))
def ctrlc(run='ctrlc2'):
    # Dedicated, hidden Windows console: true CTRL_C_EVENT, no keyboard emulation.
    args=psrun(run,"-Workload { Write-Host 'HOLD READY'; while ($true) { Start-Sleep -Milliseconds 250 } }")
    p,t=start(args,run,new_console=True)
    k=ctypes.WinDLL('kernel32',use_last_error=True)
    k.FreeConsole()
    attached=bool(k.AttachConsole(p.pid));ignore=bool(k.SetConsoleCtrlHandler(None,True))
    ctypes.set_last_error(0)
    generated=bool(k.GenerateConsoleCtrlEvent(0,0)) if attached else False
    error=0 if generated else ctypes.get_last_error();time.sleep(.5);k.FreeConsole()
    try:rc=finish(p,t,150)
    except subprocess.TimeoutExpired:
        call(['taskkill','/F','/PID',str(p.pid)]);rc=finish(p,t)
    RESULTS['powershell_ctrl_c']={'attach':attached,'ignore_self':ignore,'generated':generated,'winerror':error,'exit':rc,'check':check(),'leftovers':count(run)}
    write(run+'-result.json',RESULTS['powershell_ctrl_c']);print('F CTRL_C',RESULTS['powershell_ctrl_c'],flush=True)
    if RESULTS['powershell_ctrl_c']['check']['exit']:
        write(run+'-recovery.json',call(['powershell','-NoProfile','-ExecutionPolicy','Bypass','-File',str(ROOT/'cleanup.ps1')]))
def tail():
    # Hard kill the owned PowerShell only, after native startup has returned.
    p,t=start(psrun('hardkill',"-Workload { Write-Host 'HOLD READY'; while ($true) { Start-Sleep -Milliseconds 250 } }"),'hardkill')
    killed=call(['taskkill','/F','/PID',str(p.pid)]);rc=finish(p,t)
    orphans=count('hardkill')
    recovery=call(psrun('recovery',"-Mode smoke"))
    RESULTS['hard_kill']={'kill':killed,'exit':rc,'orphans_before_next_start':orphans,'next_start':recovery,'check':check()}
    write('hardkill-result.json',RESULTS['hard_kill']);print('F HARDKILL',RESULTS['hard_kill'],flush=True)
    # Explicit cleanup idempotency with an additional owned labeled Docker network.
    created=call(['docker','network','create','--label','skedaddle.test=20261106-explicit','skedaddle-20261106-explicit'])
    precheck=check()
    cleanargs=['powershell','-NoProfile','-ExecutionPolicy','Bypass','-File',str(ROOT/'cleanup.ps1')]
    c1=call(cleanargs);c2=call(cleanargs)
    RESULTS['cleanup_idempotent']={'create':created,'check_detects':precheck,'first':c1,'second':c2,'check':check()}
    write('cleanup-result.json',RESULTS['cleanup_idempotent'])
    # Forced refusal, both entry points. Compare all Docker resource IDs around it.
    before=call([sys.executable,'-B',str(ROOT/'harness.py'),'inventory'])
    refuseps=call(psrun('guardps','-BudgetGiB 999999'))
    refusesh=call([BASH,'--noprofile','--norc',str(ROOT/'run.sh')],env=env('guardsh',BUDGET_GIB='999999'))
    after=call([sys.executable,'-B',str(ROOT/'harness.py'),'inventory'])
    RESULTS['guard_refusal']={'powershell':refuseps,'bash':refusesh,'inventory_identical':before['stdout']==after['stdout'],'check':check()}
    write('guard-result.json',RESULTS['guard_refusal'])
    dry=call(['powershell','-NoProfile','-ExecutionPolicy','Bypass','-File',str(ROOT/'cleanup.ps1'),'-Images','-WhatIf'])
    write('images-whatif.txt',dry['stdout']+dry['stderr'])
    baseline=json.loads((ROOT.parents[1]/'s4-verification-20261106-023701'/'inventory-before.json').read_text())
    current=json.loads(after['stdout']);manifest=json.loads((ROOT/'images.lock').read_text())
    allowed={v['id'] for v in manifest['images'].values()}
    differences={}
    for kind in baseline:
        differences[kind]={'missing':sorted(set(baseline[kind])-set(current[kind])),'added':sorted(set(current[kind])-set(baseline[kind]))}
    ok=all(not d['missing'] and (kind=='images' and set(d['added'])<=allowed or not d['added']) for kind,d in differences.items())
    RESULTS['non_interference']={'pass':ok,'differences':differences}
    write('inventory-after.json',current);write('non-interference.json',RESULTS['non_interference'])
    write('paths-results.json',RESULTS)
    print('H NONINTERFERENCE',ok,'FINAL',check(),flush=True)
if __name__=='__main__':
    if sys.argv[1:]==['tail']:
        ctrlc();tail()
    else:
        early();ctrlc();tail()
