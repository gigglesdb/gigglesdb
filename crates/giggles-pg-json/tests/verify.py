import pathlib, subprocess, sys, json
root=pathlib.Path(__file__).resolve().parents[3]
artifact=root/'target-dd68-json';artifact.mkdir(exist_ok=True)
manifest=root/'crates/giggles-pg-json/Cargo.toml'
commands=[['cargo','fmt','--manifest-path',str(manifest),'--','--check'], ['cargo','test','--locked','--manifest-path',str(manifest),'--target-dir',str(artifact)],['cargo','clippy','--locked','--manifest-path',str(manifest),'--target-dir',str(artifact),'--all-targets','--','-D','warnings']]
if '--live' in sys.argv: commands[1]+= ['--','--include-ignored','--test-threads=1']
results=[]
for i,cmd in enumerate(commands):
 p=subprocess.run(cmd,cwd=root,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
 (artifact/f'check-{i}.log').write_bytes(p.stdout)
 results.append({'command':cmd,'exit_code':p.returncode,'log':f'check-{i}.log'})
 print(cmd[1],p.returncode)
(artifact/'verification.json').write_text(json.dumps(results,indent=2)+'\n')
sys.exit(0 if all(r['exit_code']==0 for r in results) else 1)
