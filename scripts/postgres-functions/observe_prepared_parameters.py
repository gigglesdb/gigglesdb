"""Observe legacy parameter SQL through actual Parse0, without using its literal expectations."""
import argparse,pathlib,json,struct,sys,hashlib
from run_prepared import OUT,execute,save
from conformance import ROOT,load_cases,run
from prepared_wire import WireReference
from workdir import check_outside_repo
OIDS={16:'boolean',17:'bytea',20:'bigint',21:'smallint',23:'integer',25:'text',700:'real',701:'double precision',1700:'numeric'}
def text(p):
 v=p['value'];t=p['type']
 if v is None:return None
 if t in {'real','double precision'}:return repr(struct.unpack('!f' if t=='real' else '!d',bytes.fromhex(v))[0])
 if t=='boolean':return str(v).lower()
 return str(v)
def dimensions(c):
 r,a=c['reference'],c['candidate'];d=a.get('description') or {}
 out={'parameter_metadata':d.get('parameterTypes')==[OIDS.get(o,str(o)) for o in r.get('parameterTypes') or []], 'result_metadata':d.get('resultTypes')==[OIDS.get(o['oid'],str(o['oid'])) for o in r.get('resultTypes') or []], 'success':r['status']==a.get('status'),'error_contract':'core variant and PG SQLSTATE separately observed; no wire equivalence assertion','exact_values':'not comparable'}
 if a.get('status')=='withheld':
  out['parameter_metadata']='unobserved: bytea execution withheld'
  out['result_metadata']='unobserved: bytea execution withheld'
 if r.get('parameterTypes') is None:out['parameter_metadata']='unavailable: PG Parse/Describe failed'
 if r.get('resultTypes') is None:out['result_metadata']='unavailable: PG Parse/Describe failed'
 if 17 in (r.get('parameterTypes') or []) and any(isinstance(v,str) for v in c.get('core_params',[])):
  out['success']='withheld: no core Text to PG bytea input conversion contract'
  out['exact_values']='withheld: no core Text to PG bytea input conversion contract'
  return out
 if r['status']=='ok' and a.get('status')=='ok' and out['result_metadata'] is True:
  try:
   rows=[]
   for row in r['rows']:
    converted=[]
    for v,f in zip(row,r['resultTypes']):
     oid=f['oid']
     if v is None:converted.append(None)
     elif oid in {20,21,23}:converted.append(int(v))
     elif oid==16:converted.append(v=='t')
     elif oid==1700:converted.append({'numeric_text':v})
     elif oid in {700,701}:converted.append({('float4_bits' if oid==700 else 'float8_bits'):struct.pack('!f' if oid==700 else '!d',float(v)).hex()})
     else:converted.append(v)
    rows.append(converted)
   out['exact_values']=rows==a['rows']
  except (ValueError,OverflowError):out['exact_values']='conversion unavailable'
 return out

def main():
 global OUT
 parser=argparse.ArgumentParser(description=__doc__);parser.add_argument("--output",type=pathlib.Path,default=OUT)
 OUT=parser.parse_args().output
 check_outside_repo(OUT)
 probe=OUT/'build/debug/examples/function_probe.exe'
 cases=[c for c in load_cases([ROOT/f for f in ['cases.json','cases-strings.json','cases-binary.json','cases-numeric.json']]) if c.get('params')]
 save(OUT/'prepared-parameter-fixtures.json',cases);details=[]
 with WireReference() as ref:
  for i,c in enumerate(cases):
   r,t=ref.observe(c['query'],[text(p) for p in c['params']],c.get('reference_setup',c['setup']));save(OUT/f'parameter-trace-{i}.json',t)
   params=[p if p['type'] in {'numeric','real','double precision'} and p['value'] is not None else p['value'] for p in c['params']]
   cmd=[str(probe),str(OUT/f'parameter-{i}.db')];p=run(cmd,input=json.dumps(dict(query=c['query'],setup=c['setup'],params=params,exact_numeric=True,describe_metadata=True)))
   item=dict(id=c['id'],reference=r,candidate=json.loads(p.stdout) if p.returncode==0 else dict(status='probe_error',diagnostic=p.stderr),core_params=params,native_exit=p.returncode,command=cmd);item['dimensions']=dimensions(item);details.append(item)
 save(OUT/'prepared-parameter-report.json',dict(cases=details,full=False,verified=0,denominator=3413,legacy_expectations_used=False))
 initial=json.loads((OUT/'prepared-report.json').read_text(encoding='utf-8'))
 for c in initial['cases']:c['dimensions']=dimensions(c)
 save(OUT/'prepared-report.json',initial)
if __name__=='__main__':main()
