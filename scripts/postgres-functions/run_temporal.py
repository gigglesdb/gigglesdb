"""Run full existing SQL harness with immutable executable and before/after identities."""
import collections,hashlib,json,pathlib,shutil,subprocess,sys
from conformance import ROOT,load_cases
BASE=ROOT.parent.parent
from workdir import default_output,check_outside_repo
OUT=default_output('temporal')
def hashes():
    paths=list((BASE/'crates').rglob('*.rs'))+[BASE/'crates/giggles-core/examples/function_probe.rs']+list(ROOT.glob('*.py'))+list(ROOT.glob('cases*.json'))+[ROOT/'postgres-18.0.json.gz',ROOT/'reference.json',ROOT/'temporal-reference.json']
    return {str(p.relative_to(BASE)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(paths)}
def main():
    global OUT
    if len(sys.argv)>2:OUT=pathlib.Path(sys.argv[2]).resolve()
    check_outside_repo(OUT)
    OUT.mkdir(parents=True,exist_ok=True)
    probe=OUT/'function_probe.exe'
    if probe.exists():raise RuntimeError('refuse to replace frozen executable')
    shutil.copyfile(sys.argv[1],probe)
    before=hashes();(OUT/'before.json').write_text(json.dumps(before,indent=2))
    files=['cases.json','cases-strings.json','cases-binary.json','cases-numeric.json','cases-temporal.json']
    cmd=[sys.executable,str(ROOT/'conformance.py'),'run','--output',str(OUT/'full-run'),'--probe',str(probe)]
    for f in files:cmd+=['--cases',str(ROOT/f)]
    result=subprocess.run(cmd,capture_output=True,text=True,encoding='utf-8')
    (OUT/'process.json').write_text(json.dumps(dict(exit_code=result.returncode,command=cmd)))
    (OUT/'stdout.txt').write_text(result.stdout,encoding='utf-8');(OUT/'stderr.txt').write_text(result.stderr,encoding='utf-8')
    after=hashes();(OUT/'after.json').write_text(json.dumps(after,indent=2))
    summary=dict(exit_code=result.returncode,probe_sha256=hashlib.sha256(probe.read_bytes()).hexdigest(),probe_origin=str(pathlib.Path(sys.argv[1]).resolve()),build_source_correspondence='unverified existing executable; no build performed',changed_paths=[p for p in before.keys()|after.keys() if before.get(p)!=after.get(p)],full=False,verified=0)
    report=OUT/'full-run/report.json'
    if report.exists():
        r=json.loads(report.read_text(encoding='utf-8'));summary.update(total=len(r['cases']),counts=r['counts'],verdicts=dict(collections.Counter(c['verdict'] for c in r['cases'])),temporal=dict(collections.Counter(c['verdict'] for c in r['cases'] if c['id'].startswith('temporal-'))),temporal_candidates=dict(collections.Counter(json.dumps(c['candidate'],sort_keys=True) for c in r['cases'] if c['id'].startswith('temporal-'))))
    (OUT/'summary.json').write_text(json.dumps(summary,indent=2));print(json.dumps(summary,indent=2))
    return result.returncode
if __name__=='__main__':raise SystemExit(main())
