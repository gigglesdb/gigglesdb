#!/usr/bin/env python3
"""Capture typmod behavior from the owned PostgreSQL 18.6 Docker oracle."""
import csv, datetime, json, os, pathlib, subprocess, time
NAME = 'gd-e2-oracle-e2a0'
ROOT = pathlib.Path(__file__).resolve().parent
os.environ.update(CARGO_TARGET_DIR='J:/gigglesdb-work/e2-types/target', TEMP='J:/gigglesdb-work/e2-types/tmp/e2a-0', TMP='J:/gigglesdb-work/e2-types/tmp/e2a-0')
pathlib.Path(os.environ['TEMP']).mkdir(parents=True, exist_ok=True)
def docker(*args, input=None):
    return subprocess.run(['docker', *args], input=input, text=True, encoding='utf-8', capture_output=True, check=True).stdout.strip()
def quote(s): return "'" + str(s).replace("'", "''") + "'"
for attempt in range(60):
    try:
        docker('exec',NAME,'pg_isready','-U','postgres')
        break
    except subprocess.CalledProcessError:
        if attempt == 59: raise
        time.sleep(1)
cases=[]
def add(kind, value, args, sql): cases.append((kind,str(value),str(args),sql))
for p in list(range(1,39))+[1000]:
    for s in sorted(set([-3,-1,0,1,2,5,p,p+1])):
        # Decimal strings deliberately avoid host floating point calculations.
        vals=['0','-0.00001','1.5','-1.5','9.95','-9.95','NaN','Infinity','-Infinity']
        if not -1000 <= s <= 1000: continue
        # Boundary and neighbors are exact decimal strings, including maxdigits < 0.
        from decimal import Decimal, localcontext
        with localcontext() as ctx:
            ctx.prec=2100
            boundary=Decimal(10) ** (p-s)
            step=Decimal(10) ** (-s)
            for x in [boundary-step,boundary,boundary+step]:
                vals += [format(x,'f'),format(-x,'f')]
        for v in vals: add('numeric_apply',v,f'{p},{s}',f'SELECT ({quote(v)}::numeric({p},{s}))::text')
for args in ['(0)','(1001)','(5,1001)','(5,-1001)']:
    add('numeric_modifier','1',args,f'SELECT (1::numeric{args})::text')
for typ in ['numeric(10,2)','numeric(5)','numeric(2,-3)','numeric(3,5)','varchar(20)','char(5)','char']:
    add('typmod',typ,'',f"SELECT atttypmod::text || ',' || format_type(atttypid,atttypmod) FROM pg_attribute WHERE attrelid='tm_{len(cases)}'::regclass AND attname='v'")
    cases[-1]=(*cases[-1][:3],f'CREATE TEMP TABLE tm_{len(cases)-1}(v {typ}); '+cases[-1][3])
for typ in ['varchar','char']:
    for n in [1,2,5]:
        for value in ['', 'a','ab','abc','ab   ','abcdef','ñ','日本','😀','😀😀']:
            for mode in ['assignment','cast']:
                expr=f'SELECT ({quote(value)}::{typ}({n}))::text' if typ=='varchar' else f"SELECT encode(bpcharsend({quote(value)}::char({n})), 'hex')"
                if mode=='assignment': expr=f'CREATE TEMP TABLE assign(v {typ}({n})); INSERT INTO assign VALUES ({quote(value)}); SELECT v::text FROM assign; DROP TABLE assign' if typ=='varchar' else f"CREATE TEMP TABLE assign(v char({n})); INSERT INTO assign VALUES ({quote(value)}); SELECT encode(bpcharsend(v), 'hex') FROM assign; DROP TABLE assign"
                add(f'{typ}_{mode}',value,n,expr)
for value in ['','a','a  ','日本 ','😀  ']:
    for n in [1,2,5]: add('bpchar_lengths',value,n,f'SELECT length(v)::text || \',\' || octet_length(v)::text FROM (SELECT {quote(value)}::char({n}) v) q')
for a,b in [('a  ','a    '),('a','a '),('a ','a'),('日本','日本 '),('a','b'),('ñ','z'),('😀','日本')]:
    add('bpchar_eq',a,b,f"SELECT CASE WHEN {quote(a)}::char({max(1,len(a))}) = {quote(b)}::char({max(1,len(b))}) THEN 't' ELSE 'f' END")
    left=f'{quote(a)}::char({max(1,len(a))}) COLLATE "C"'
    right=f'{quote(b)}::char({max(1,len(b))}) COLLATE "C"'
    add('bpchar_order',a,b,f"SELECT CASE WHEN {left} = {right} THEN '=' ELSE (SELECT CASE label WHEN 0 THEN '<' ELSE '>' END FROM (VALUES ({left},0),({right},1)) q(v,label) ORDER BY v COLLATE \"C\" LIMIT 1) END")
# An exception block makes SQLSTATE and DETAIL part of the captured data.
setup='''CREATE OR REPLACE FUNCTION pg_temp.capture(q text) RETURNS json LANGUAGE plpgsql AS $$ DECLARE v text; st text; de text; BEGIN EXECUTE q INTO v; RETURN json_build_array(coalesce(v,''),'',''); EXCEPTION WHEN OTHERS THEN GET STACKED DIAGNOSTICS st=RETURNED_SQLSTATE,de=PG_EXCEPTION_DETAIL; RETURN json_build_array('ERROR',st,coalesce(de,''),SQLERRM); END $$;'''
# Multi-statement commands cannot return through EXECUTE INTO; wrap table cases in functions.
queries=[]
for i,(kind,value,args,sql) in enumerate(cases):
    if kind=='typmod':
        create,select=sql.split('; ',1)
        queries.append(create+'; SELECT pg_temp.capture('+quote(select)+');')
    elif kind.endswith('_assignment'):
        create,insert,select,drop=sql.split('; ')
        # unique table per case; exception rollback permits later cases.
        body=f'CREATE OR REPLACE FUNCTION pg_temp.case_{i}() RETURNS text LANGUAGE plpgsql AS $x$ DECLARE r text; BEGIN {create}; {insert}; {select} INTO r; {drop}; RETURN r; END $x$;'
        queries.append(body+' SELECT pg_temp.capture('+quote(f'SELECT pg_temp.case_{i}()')+');')
    else: queries.append('SELECT pg_temp.capture('+quote(sql)+');')
output=docker('exec','-i',NAME,'psql','-U','postgres','-AtX','-q',input=setup+'\n'+'\n'.join(queries))
results=[json.loads(line) for line in output.splitlines() if line.startswith('[')]
assert len(results)==len(cases),(len(results),len(cases))
with (ROOT/'typmod_pg18.tsv').open('w',encoding='utf-8',newline='') as f:
    writer=csv.writer(f,delimiter='\t',lineterminator='\n')
    writer.writerow(['kind','input','args','expected_text_or_ERROR','sqlstate','detail_or_empty'])
    for (kind,value,args,_),result in zip(cases,results):
        expected,state,detail=result[:3]
        if kind in ('char_assignment','char_cast') and not state: expected=bytes.fromhex(expected).decode('utf-8')
        if (kind=='numeric_modifier' or kind in ('char_assignment','varchar_assignment')) and state: detail=result[3]+(' DETAIL: '+detail if detail else '')
        writer.writerow([kind,value,args,expected,state,detail])
metadata=docker('exec','-i',NAME,'psql','-U','postgres','-AtX',input='SHOW server_version; SHOW server_encoding;')
image=docker('image','inspect','postgres:18.6','--format','{{.Id}}')
(ROOT/'TYPMOD-PROVENANCE.txt').write_text(f'PostgreSQL typmod oracle capture\nUTC capture: {datetime.datetime.now(datetime.timezone.utc).isoformat()}\nImage: postgres:18.6\nImage ID: {image}\nServer version and encoding:\n{metadata}\nContainer: {NAME}\nStart: docker run -d --rm --name {NAME} -e POSTGRES_PASSWORD=x postgres:18.6\nCommand: python -I crates/giggles-pg-types/tests/fixtures/generate_typmod_pg18.py\nQueries: docker exec -i {NAME} psql -U postgres -AtX -q\nRows: {len(cases)}\nNumeric apply rows: {sum(c[0]=="numeric_apply" for c in cases)}\nErrors capture RETURNED_SQLSTATE and PG_EXCEPTION_DETAIL; numeric_modifier and character assignment detail additionally capture SQLERRM.\nTSV has six raw tab-separated columns; these cases contain no tabs or newlines.\n',encoding='utf-8')
print(f'Captured {len(cases)} rows, numeric_apply={sum(c[0]=="numeric_apply" for c in cases)}')
