import pathlib,subprocess,json,sys,os
root=pathlib.Path(__file__).resolve().parents[3]
a=root/'target-dd68-json-functions';a.mkdir(exist_ok=True)
m=str(root/'crates/giggles-pg-json/Cargo.toml')
cmds=[]
if '--live' in sys.argv:cmds.append(['python',str(root/'crates/giggles-pg-json/tests/generate_operators.py')])
cmds += [['cargo','fmt','--manifest-path',m,'--','--check'],['cargo','test','--locked','--manifest-path',m,'--target-dir',str(a)],['cargo','clippy','--locked','--manifest-path',m,'--target-dir',str(a),'--all-targets','--','-D','warnings']]
if '--live' in sys.argv:cmds[-2]+= ['--','--include-ignored','--test-threads=1']
results=[]
for i,c in enumerate(cmds):
 p=subprocess.run(c,cwd=root,capture_output=True,env={**os.environ,'PG_JSON_OPERATOR_CONTAINER':'dd68-json-sol-20261106'})
 (a/f'check-{i}.log').write_bytes(p.stdout+p.stderr)
 results.append({'command':c,'exit_code':p.returncode,'log':f'check-{i}.log'});print(c[1],p.returncode)
(a/'verification.json').write_text(json.dumps(results,indent=2)+'\n')
sys.exit(int(any(r['exit_code'] for r in results)))
