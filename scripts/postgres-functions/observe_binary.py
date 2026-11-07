"""Independent binary fixtures; expected results must be observed on pinned PG18."""
import argparse
from pathlib import Path
from workdir import default_output,check_outside_repo
import gzip
import json
from conformance import ROOT, IMAGE, Reference, digest, validate_pin, validate_cases, validate_inventory

BINARY_NAMES = set('length octet_length bit_length reverse btrim ltrim rtrim substring substr get_byte get_bit set_byte set_bit encode sha224 sha256 sha384 sha512'.split())

def fixtures():
    inventory = json.loads(gzip.decompress((ROOT/'postgres-18.0.json.gz').read_bytes()))
    cases = []
    def add(identity, label, expr, error=False, params=None):
        case = dict(id='binary-'+str(len(cases))+'-'+label, routines=[identity], kind='error' if error else 'boundary', setup=[], query='SELECT '+expr+' AS v')
        if params is not None:
            case['params']=params
            case['binding_evidence']='Primitive parameter with explicit SQL cast to bytea; reference literal expansion, no binary wire evidence.'
        if error:
            case['expected_core_error']='Sql(Type)'
            case['core_error_rationale']='Intended binding/domain validation variant, independent of observed SQLSTATE. Parser Syntax rejection remains a failure.'
        cases.append(case)
    def b(hex): return "CAST('\\x"+hex+"' AS bytea)"
    targets=[r['identity'] for r in inventory['routines'] if (r['identity'].split('(')[0].split('.')[-1].strip('"') in BINARY_NAMES and r['identity'].split('(')[1].startswith('bytea')) or r['identity']=='pg_catalog.decode(text, text)/f']
    for ident in targets:
        name=ident.split('(')[0].split('.')[-1].strip('"')
        types=ident.split('(')[1].split(')')[0].split(', ')
        args=[b('0080ff01026162') if t=='bytea' else "CAST('UTF8' AS name)" if t=='name' else "'hex'" if t=='text' else '1' for t in types]
        if name=='length' and len(args)==2: args[0]=b('616263')
        if name.endswith('trim'): args[1]=b('0062')
        if name=='decode': args=["'0080ff'", "'hex'"]
        if name.startswith('set_'): args[2]='1'
        def call(a): return name+'('+','.join(a)+')'
        add(ident,'normal',call(args));cases[-1]['kind']='normal'
        for i,t in enumerate(types):
            vals=args.copy();vals[i]='CAST(NULL AS '+t+')';add(ident,'null-'+str(i),call(vals));cases[-1]['kind']='null'
        vals=args.copy(); vals[0]="''" if name=='decode' else b('');add(ident,'empty',call(vals),name.startswith(('get_','set_')))
        if name!='decode':
            vals=args.copy();vals[0]='CAST($1 AS bytea)';add(ident,'primitive-text-cast',call(vals),error=name=='length' and len(args)==2,params=[dict(type='text',value='\\x0080ff01026162')])
        add(ident,'arity-missing',name+'()',True)
        add(ident,'wrong-type',call(['true']+args[1:]),True)
        if name in ('get_bit','get_byte','set_bit','set_byte'):
            for n in [-1,0,7,8,55,56,2147483647]:
                vals=args.copy();vals[1]=str(n); add(ident,'index-'+str(n),call(vals),n<0 or n>=(56 if 'bit' in name else 7))
            if name.startswith('set_'):
                for n in [-1,2,255,256]:
                    vals=args.copy();vals[2]=str(n);add(ident,'value-'+str(n),call(vals),n not in (0,1) if name=='set_bit' else False)
        if name in ('substring','substr'):
            for start in [-2147483648,-2,0,1,8,2147483647]:
                vals=args.copy();vals[1]=str(start);add(ident,'start-'+str(start),call(vals))
            if len(args)==3:
                for n in [-1,0,2147483647]:
                    vals=args.copy();vals[2]=str(n);add(ident,'length-'+str(n),call(vals),n<0)
        if name in ('encode','decode'):
            for encoding in ['hex','HEX','base64','escape','bogus','']:
                vals=[b('000aff5c80'),repr(encoding)] if name=='encode' else ["'YWJj'" if encoding.lower()=='base64' else "'abc'" if encoding=='escape' else "'00ff'",repr(encoding)]
                add(ident,'encoding-'+encoding,call(vals),encoding in ('bogus',''))
    decode='pg_catalog.decode(text, text)/f'
    for encoding, values in {'hex':['0','gg','00 ff',' 00\nff\t'], 'base64':['YQ==','YWI=','YWJj','YQ','YQ=','Y===','YQ===','YW Jj\n\t\r','!','===='], 'escape':['abc',r'\000\377\\',r'\400',r'\08x', '\\']}.items():
        for value in values:
            # Error intent is specified independently; observation verifies it below.
            error = (encoding=='hex' and value in ['0','gg']) or (encoding=='base64' and value in ['YQ','YQ=','Y===','YQ===','!','====']) or (encoding=='escape' and value in [r'\400',r'\08x','\\'])
            add(decode,'decode-edge',"decode('"+value.replace("'","''")+"','"+encoding+"')",error)
    # Sized binary generation is kept under core's output cap; over-cap core
    # behavior has no PG domain error counterpart and cannot be a negative fixture.
    for n in [32768,65536]:
        add('pg_catalog.decode(text, text)/f','output-boundary-'+str(n),"octet_length(decode(repeat('00',"+str(n)+"),'hex'))")
    for index in [1, 15, 16, 23, 24, 25]:
        add('pg_catalog.get_bit(bytea, bigint)/f', 'bit-order-'+str(index), 'get_bit('+b('0180ff01')+','+str(index)+')')
    add('pg_catalog.sha256(bytea)/f','nested-digest',"encode(sha256(decode('0080ff','hex')),'hex')")
    return inventory,cases

def observe():
    parser=argparse.ArgumentParser()
    parser.add_argument('--output',type=Path,default=default_output('observe-binary'),help='new output directory outside repository')
    args=parser.parse_args()
    output=args.output;check_outside_repo(output);output.mkdir(parents=True,exist_ok=False)
    inventory,cases=fixtures()
    with Reference() as ref:
        validate_pin(ref.inventory(),json.loads((ROOT/'reference.json').read_text()))
        for case in cases:
            result=ref.query(case)
            if result['status']=='error' and 'expected_core_error' not in case: raise RuntimeError((case,result))
            if result['status']=='ok' and 'expected_core_error' in case: raise RuntimeError((case,result))
            case['expected']=result
        validate_cases(cases,validate_inventory(inventory))
    (output/'cases-binary.json').write_text(json.dumps(cases,indent=2)+'\n',encoding='utf-8')
    (output/'binary-reference.json').write_text(json.dumps(dict(image=IMAGE,inventory_sha256=digest(inventory),corpus_sha256=digest(cases),case_count=len(cases),observation='Reference.query on owned pinned PG18.0; explicit SQL casts, no binary wire evidence.'),indent=2)+'\n')
    print(len(cases),digest(cases))
if __name__=='__main__': observe()
