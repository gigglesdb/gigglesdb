"""Direct-exit-code qualification; no container lifecycle side effects."""
import pathlib, subprocess, json, sys, os, hashlib
root=pathlib.Path(__file__).resolve().parents[3]
crate=root/'crates/giggles-pg-json'
a=root/'target-dd68-jsonpath-predicates'; a.mkdir(exist_ok=True)
live='--live' in sys.argv
mode='live' if live else 'offline'
def hashes(paths): return {str(p.relative_to(crate)):hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}
sources=[p for p in crate.rglob('*') if p.is_file() and (p.suffix in ['.rs','.py','.toml','.lock'] or p.parent.name=='upstream')]
before=hashes(sources)
fixtures=list((crate/'tests/fixtures').glob('*.tsv'))
fixture_before=hashes(fixtures)
env={**os.environ,'PG_JSON_PATH_CONTAINER':'dd68-jsonpath-predicates-sol-20261106','PG_JSON_CONTAINER':'dd68-jsonpath-predicates-sol-20261106','PG_JSON_OPERATOR_CONTAINER':'dd68-jsonpath-predicates-sol-20261106','PG_JSON_MUTATION_CONTAINER':'dd68-jsonpath-predicates-sol-20261106'}
m=str(crate/'Cargo.toml')
cmds=[]
if live:
 cmds += [['python',str(crate/'tests/generate_predicates.py')],['python',str(crate/'tests/generate_jsonpath.py')],['python',str(crate/'tests/generate_operators.py')]]
cmds += [['cargo','fmt','--manifest-path',m,'--','--check'],['cargo','test','--offline','--locked','--manifest-path',m,'--target-dir',str(a)],['cargo','clippy','--offline','--locked','--manifest-path',m,'--target-dir',str(a),'--all-targets','--','-D','warnings']]
if live: cmds[-2] += ['--','--include-ignored','--test-threads=1']
results=[]
for i,c in enumerate(cmds):
 p=subprocess.run(c,cwd=root,capture_output=True,env=env)
 log=f'{mode}-{i}.log'; (a/log).write_bytes(p.stdout+p.stderr)
 results.append({'command':c,'exit_code':p.returncode,'log':log}); print(c[1],p.returncode,flush=True)
after=hashes(sources)
fixture_after=hashes(fixtures)
provenance=json.loads((crate/'upstream/provenance.json').read_text())
upstream_ok=all(hashlib.sha256((crate/'upstream'/pathlib.Path(s['path']).name).read_bytes()).hexdigest()==s['sha256'] for s in provenance['sources'])
record={'checks':results,'source_before':before,'source_after':after,'source_stable':before==after,'fixture_before':fixture_before,'fixture_after':fixture_after,'fixtures_stable':fixture_before==fixture_after,'upstream_provenance_verified':upstream_ok}
(a/f'{mode}-verification.json').write_text(json.dumps(record,indent=2)+'\n')
sys.exit(int(any(r['exit_code'] for r in results) or before!=after or fixture_before!=fixture_after or not upstream_ok))
