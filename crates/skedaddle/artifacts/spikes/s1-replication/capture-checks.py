"""Capture final focused checks; all generated host files stay in the S1 evidence tree."""
import json, os, pathlib, subprocess, sys
root=pathlib.Path(__file__).resolve().parent
campaign=sys.argv[1] if len(sys.argv)>1 else '20261106'
out=root.parents[1]/('s1-verification-'+campaign)
env=os.environ.copy()
env.update(CARGO_BUILD_JOBS='2',CARGO_TARGET_DIR='K:/Projects/gigglesdb/crates/skedaddle/target',CARGO_HOME=str(root/'cargo-home'),TEMP='K:/Projects/gigglesdb/crates/skedaddle/target',TMP='K:/Projects/gigglesdb/crates/skedaddle/target')
commands=[('survey-compile',['cargo','check','--manifest-path',str(root/'survey-compile/Cargo.toml')]),('unit-tests',['cargo','test','--locked','--manifest-path',str(root/'Cargo.toml')]),('probe-dependencies',['cargo','tree','--locked','--manifest-path',str(root/'Cargo.toml'),'--edges','normal','--prefix','none']),('survey-dependencies',['cargo','tree','--locked','--manifest-path',str(root/'survey-compile/Cargo.toml'),'--edges','normal','--prefix','none'])]
failed=False
for name,command in commands:
    p=subprocess.run(command,capture_output=True,text=True,encoding='utf8',errors='replace',env=env)
    with (out/(name+'.json')).open('x',encoding='utf8') as f:json.dump({'command':command,'exit':p.returncode,'stdout':p.stdout,'stderr':p.stderr},f,indent=2)
    print(name,'exit',p.returncode,p.stdout if name=='unit-tests' else '',p.stderr[-2500:],flush=True)
    failed|=p.returncode!=0
sys.exit(int(failed))
