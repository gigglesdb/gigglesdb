"""Observe supplemental fixtures on an owned pinned PG18.0; never guess values."""
import argparse
from pathlib import Path
from workdir import default_output,check_outside_repo
import gzip
import json
from conformance import ROOT, Reference, IMAGE, digest, validate_cases, validate_inventory, validate_pin

NAMES = set('char_length character_length octet_length bit_length starts_with strpos reverse repeat replace translate left right lpad rpad btrim ltrim rtrim split_part concat concat_ws'.split())

def fixtures():
    inventory = json.loads(gzip.decompress((ROOT / 'postgres-18.0.json.gz').read_bytes()))
    cases = []
    def add(identity, label, expression, kind='normal', params=None, core=None):
        case = dict(id='strings-' + identity.split('(')[0].split('.')[-1] + '-' + str(len(cases)) + '-' + label,
                    routines=[identity], kind=kind, setup=[], query='SELECT '+expression+' AS v')
        if params is not None: case['params'] = params
        if core:
            case['kind'] = 'error'
            case['expected_core_error'] = core
            case['core_error_rationale'] = 'Sql(Type) is the intended binding/evaluation contract; current Sql(Syntax) parser rejection remains a failure, not accepted compatibility. Core variant is independent of oracle SQLSTATE.'
        cases.append(case)
    for routine in inventory['routines']:
        identity = routine['identity']
        name, args = identity.split('(')
        name = name.split('.')[-1].strip(chr(34))
        if name not in NAMES: continue
        types = args.split(')')[0].split(', ')
        def literal(t, value):
            if value is None: return 'CAST(NULL AS '+('text' if t=='"any"' else t)+')'
            return {'text': "'"+value.replace("'", "''")+"'", 'character': "CAST('é  ' AS character(3))", 'bytea': "CAST('abc' AS bytea)", 'bit': "B'10101'", 'integer': '3', '"any"': "'é'::text"}[t]
        base = [literal(t,'é界é') for t in types]
        if name in ('strpos','starts_with'): base[1]="'界'"
        if name in ('split_part',): base=["'a::b::c'", "'::'", '2']
        if name in ('replace','translate'): base=["'é界é'", "'é'", "'X'"]
        if name.endswith('trim') and len(base)==2: base[1]="CAST('a' AS bytea)" if types[0]=='bytea' else "'é'"
        if name in ('lpad','rpad') and len(base)==3: base[2]="'界x'"
        def call(values): return name+'('+','.join(values)+')'
        add(identity,'normal',call(base))
        for i,t in enumerate(types):
            vals=base.copy(); vals[i]=literal(t,None); add(identity,'null-'+str(i),call(vals),'null')
        if types[0] in ('text','"any"'):
            vals=base.copy(); vals[0]="''"; add(identity,'empty',call(vals),'boundary')
            # Parameters are explicitly typed in SQL on both adapters.
            vals=base.copy(); vals[0]='CAST($1 AS text)'
            add(identity,'parameter',call(vals),params=[{'type':'text','value':'é界é'}])
            add(identity,'nested','char_length('+call(base)+')' if name not in ('char_length','character_length','octet_length','bit_length','strpos','starts_with') else 'abs('+call(base)+')' if name!='starts_with' else 'coalesce('+call(base)+',false)')
            cases[-1]['routines'] = [identity] # nesting is additional context, not exhaustive callee evidence
        if 'integer' in types:
            for number in [-2147483648,-3,0,1,12]:
                vals=base.copy(); vals[types.index('integer')]=str(number)
                add(identity,'integer-'+str(number),call(vals),'boundary',core='Sql(Type)' if name=='split_part' and number==0 else None)
        if types[0]=='text':
            add(identity,'wrong-type',name+'(true'+(' ,'+','.join(base[1:]) if len(base)>1 else '')+')','error',core='Sql(Type)')
    for name, expressions in {
        'concat': ["concat('?',NULL,12,true,'')", "concat(NULL,NULL)"],
        'concat_ws': ["concat_ws('?','?',NULL,12,'')", "concat_ws('',NULL,'a','b')"],
        'split_part': ["split_part('abc','',-1)", "split_part('abc','x',-2)"],
        'lpad': ["lpad('?',5,'')"],
        'rpad': ["rpad('?',5,'')"],
        'replace': ["replace('??','','x')"],
        'translate': ["translate('???','??','X')"],
    }.items():
        ident=next(r['identity'] for r in inventory['routines'] if r['identity'].startswith('pg_catalog.'+name+'(') and ('text' in r['identity'] or name=='concat') and (name not in ('lpad','rpad') or r['identity'].count(',')==2))
        for expression in expressions: add(ident,'variadic-or-empty-boundary',expression,'boundary')
    identity='pg_catalog.abs(integer)/f' 
    add(identity,'explicit-cast-binding','abs(CAST($1 AS integer))',params=[{'type':'integer','value':-7}])
    cases[-1]['binding_evidence']='Explicit SQL cast fixes overload on both adapters; untyped legacy abs($1) expands a typed reference literal but core infers its own parameter type and is not pgwire-equivalent.'
    for expr in ["substring('é界abc' FROM 2 FOR 2)","substring('é界abc' FROM -1 FOR 3)","substring('é界abc' FROM 2)","substring('abc' FROM 1 FOR -1)"]:
        # SQL syntax resolves to the quoted substring catalog identity.
        ident='pg_catalog."substring"(text, integer, integer)/f' if ' FOR ' in expr else 'pg_catalog."substring"(text, integer)/f'
        add(ident,'from-for',expr,'error' if 'FOR -1' in expr else 'boundary',core='Sql(Type)' if 'FOR -1' in expr else None)
    return inventory,cases

if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--output',type=Path,default=default_output('observe-strings'),help='new output directory outside repository')
    args=parser.parse_args()
    output=args.output;check_outside_repo(output);output.mkdir(parents=True,exist_ok=False)
    inventory,cases=fixtures()
    with Reference() as reference:
        live=reference.inventory()
        validate_pin(live,json.loads((ROOT/'reference.json').read_text()))
        for case in cases:
            case['expected']=reference.query(case)
            if case['expected']['status']=='error':
                if not case.get('expected_core_error'): raise RuntimeError(case)
                case['kind']='error'
            elif case.get('expected_core_error'): raise RuntimeError(case)
            elif case['expected']['status']!='ok': raise RuntimeError(case)
        validate_cases(cases,validate_inventory(live))
    path=output/'cases-strings.json'
    path.write_text(json.dumps(cases,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    (output/'strings-reference.json').write_text(json.dumps({'image':IMAGE,'inventory_sha256':digest(inventory),'corpus_sha256':digest(cases),'case_count':len(cases),'observation':'All expected values, column metadata and SQLSTATE observed using Reference.query; literal expansion, not pgwire.'},indent=2)+'\n')
    print(len(cases),digest(cases))
