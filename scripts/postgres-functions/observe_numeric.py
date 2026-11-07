"""PG18 observed exact numeric and IEEE family; never derive oracle values from core."""
import argparse
from pathlib import Path
from workdir import default_output,check_outside_repo
import gzip
import json
from conformance import ROOT, IMAGE, Reference, digest, validate_pin, validate_cases, validate_inventory

NAMES=set('abs ceil ceiling floor round trunc sign div mod gcd lcm sqrt cbrt exp ln log log10 power scale min_scale trim_scale factorial degrees radians pi'.split())

def fixtures():
    inventory=json.loads(gzip.decompress((ROOT/'postgres-18.0.json.gz').read_bytes()))
    targets=[]
    for r in inventory['routines']:
        ident=r['identity']; name=ident.split('(')[0].split('.')[-1].strip('"')
        types=ident.split('(')[1].split(')')[0].split(', ')
        if name in NAMES and all(t in {'numeric','real','double precision','integer','bigint','smallint',''} for t in types): targets.append((ident,name,types if types!=[''] else []))
    cases=[]
    def add(ident,label,expr,params=None):
        c=dict(id=f'numeric-{len(cases)}-{label}',routines=[ident],kind='boundary',setup=[],query='SELECT '+expr+' AS v',exact_numeric=True,expected_core_error='Sql(Type)',core_error_rationale='Independent intended core domain/binding variant; PostgreSQL SQLSTATE is separately observed, not wire evidence.')
        if params is not None: c.update(params=params,binding_evidence='Core prepared Value parameters; reference lexical typed literals only, not extended protocol.')
        cases.append(c)
    def cast(v,t): return "CAST('"+v+"' AS "+t+")"
    for ident,name,types in targets:
        def call(args): return name+'('+','.join(args)+')'
        args=[cast('2.5000' if t in {'numeric','real','double precision'} else '2',t) for t in types]
        add(ident,'normal',call(args));cases[-1]['kind']='normal'
        for i,t in enumerate(types):
            a=args.copy();a[i]='CAST(NULL AS '+t+')';add(ident,'null-'+str(i),call(a));cases[-1]['kind']='null'
        if types:
            values=['-2.5000','-0.0000','0','0.000001','1.0050','99999999999999999999.0000100','NaN','Infinity','-Infinity'] if types[0] in {'numeric','real','double precision'} else ['-2147483648','-1','0','2147483647']
            for v in values:
                a=args.copy();a[0]=cast(v,types[0]);add(ident,'value-'+v,call(a))
            add(ident,'wrong-type',call(['true']+args[1:]))
            add(ident,'missing-arity',name+'()')
        if name in {'round','trunc'} and types==['numeric','integer']:
            for scale in ['-10','-2','0','2','8','30']:
                add(ident,'scale-'+scale,call([cast('123456789.555500','numeric'),scale]))
        if len(types)==2:
            for v in ['0','-3','2147483647']:
                add(ident,'second-'+v,call([args[0],cast(v,types[1])]))
        if types==['numeric']:
            add(ident,'typed-param',call(['CAST($1 AS numeric)']),[dict(type='numeric',value='12345678901234567890.00001000')])
        if types==['double precision'] or types==['real']:
            t=types[0]
            for bits in (['8000000000000000','3ff0000000000001','0000000000000001','7fefffffffffffff'] if t=='double precision' else ['80000000','3f800001','00000001','7f7fffff']):
                add(ident,'typed-bits-'+bits,call(['CAST($1 AS '+t+')']),[dict(type=t,value=bits)])
    ident='pg_catalog.abs(numeric)/f'
    for v in ['1.2300','-0.0000','9'*100+'.'+'0'*30+'1','1e-100','1e100','NaN','Infinity','-Infinity']:
        add(ident,'literal-cast',cast(v,'numeric'))
    for expr in ['round(abs(CAST($1 AS numeric)),2)','abs($1)','abs(CAST($1 AS numeric))','abs(CAST($1 AS numeric))+round(CAST($1 AS numeric),2)','abs(CAST($1 AS numeric))+CAST($1 AS integer)']:
        add(ident,'parameter-reuse-inference',expr,[dict(type='numeric',value='-1.2500')])
    for v,t in [('1e400','double precision'),('1e-400','double precision'),('1e40','real'),('1e-50','real'),('bogus','numeric'),('1e131073','numeric')]: add(ident,'range-cast',cast(v,t))
    return inventory,cases

def observe():
    parser=argparse.ArgumentParser()
    parser.add_argument('--output',type=Path,default=default_output('observe-numeric'),help='new output directory outside repository')
    args=parser.parse_args()
    output=args.output;check_outside_repo(output);output.mkdir(parents=True,exist_ok=False)
    inventory,cases=fixtures()
    with Reference() as ref:
        validate_pin(ref.inventory(),json.loads((ROOT/'reference.json').read_text()))
        for c in cases:
            c['expected']=ref.query(c)
            if c['expected']['status'] not in {'ok','error'}: raise RuntimeError(c)
        validate_cases(cases,validate_inventory(inventory))
    (output/'cases-numeric.json').write_text(json.dumps(cases,indent=2)+'\n')
    (output/'numeric-reference.json').write_text(json.dumps(dict(image=IMAGE,inventory_sha256=digest(inventory),corpus_sha256=digest(cases),case_count=len(cases),observation='Owned pinned PG18; numeric text retains scale; float send exact IEEE bytes; lexical parameter expansion, no wire claim.'),indent=2)+'\n')
    print(len(cases),digest(cases))
if __name__=='__main__': observe()
