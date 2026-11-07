"""Execute an already captured probe, refusing source/executable drift.
Never changes historical observations or expectation pins.
"""
import argparse,collections,json,pathlib,sys
import run_prepared as original
import observe_prepared_parameters as parameters
from conformance import ROOT,run,load_cases
from prepared_wire import WireReference
from source_closure import inventory,sha,guard_frozen
from capture_provenance import OUT,save
from workdir import check_outside_repo

def main():
 global OUT
 parser=argparse.ArgumentParser(description=__doc__);parser.add_argument("--output",type=pathlib.Path,default=OUT)
 OUT=parser.parse_args().output
 check_outside_repo(OUT)
 receipt=json.loads((OUT/'build-receipt.json').read_text());probe=OUT/'build/debug/examples/function_probe.exe'
 frozen=json.loads((OUT/'frozen-after.json').read_text());expected=receipt['probe_sha256']
 def guard():
  guard_frozen(OUT/'source',frozen,probe,expected)
 guard();details=[]
 cases=original.fixtures()
 params=load_cases([ROOT/f for f in ['cases.json','cases-strings.json','cases-binary.json','cases-numeric.json']])
 params=[c for c in params if c.get('params')]
 with WireReference() as ref:
  for family,items in [('root',cases),('parameters',params)]:
   for i,c in enumerate(items):
    guard()
    core=c['core'] if family=='root' else [p if p['type'] in {'numeric','real','double precision'} and p['value'] is not None else p['value'] for p in c['params']]
    wire=c['wire'] if family=='root' else [parameters.text(p) for p in c['params']]
    setup=c.get('setup',[])
    refvalue,trace=ref.observe(c['query'],wire,c.get('reference_setup',setup))
    save(OUT/f'{family}-trace-{i}.json',trace)
    withheld=17 in (refvalue.get('parameterTypes') or []) and any(isinstance(v,str) for v in core)
    cmd=[str(probe),str(OUT/f'{family}-{i}.db')]
    if withheld:
     candidate={'status':'withheld','reason':'Text to bytea conversion contract absent'};code=None
    else:
     p=run(cmd,input=json.dumps(dict(query=c['query'],setup=setup,params=core,exact_numeric=True,describe_metadata=True)))
     candidate=json.loads(p.stdout) if p.returncode==0 else dict(status='probe_error',diagnostic=p.stderr);code=p.returncode
    item=dict(id=c['id'],family=family,reference=refvalue,candidate=candidate,core_params=core,native_exit=code,command=cmd,probe_sha256=expected)
    item['dimensions']=parameters.dimensions(item);details.append(item)
 save(OUT/'prepared-report.json',dict(cases=details,full=False,source_receipt_qualified=receipt['qualified'],probe_sha256=expected))
 guard()
 cmd=[sys.executable,str(ROOT/'conformance.py'),'run','--output',str(OUT/'legacy'),'--probe',str(probe)]
 for name in ['cases.json','cases-strings.json','cases-binary.json','cases-numeric.json','cases-temporal.json']:cmd+=['--cases',str(ROOT/name)]
 p=run(cmd,timeout=1800);save(OUT/'legacy-process.json',dict(command=cmd,native_exit=p.returncode));(OUT/'legacy.stdout').write_text(p.stdout);(OUT/'legacy.stderr').write_text(p.stderr)
 guard()
 save(OUT/'campaign-receipt.json',dict(full=False,probe_sha256=expected,frozen_unchanged=True,legacy_native_exit=p.returncode,prepared=len(details),source_receipt_qualified=receipt['qualified'],dimension_counts={family:{d:dict(collections.Counter(str(c['dimensions'][d]) for c in details if c['family']==family)) for d in details[0]['dimensions']} for family in ['root','parameters']}))
 return p.returncode
if __name__=='__main__':raise SystemExit(main())
