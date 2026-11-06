"""Regenerate durable fixtures using only the owned pinned PG18 container.
Start container per README; this script never starts/removes containers.
"""
import pathlib, subprocess, hashlib, json, os
ROOT=pathlib.Path(__file__).resolve().parent
NAME=os.environ.get('PG_JSON_CONTAINER','dd68-json-sol-20261106')
def pg(sql):
 p=subprocess.run(['docker','exec','-i',NAME,'psql','-XAtq','-v','ON_ERROR_STOP=1','-U','postgres'],input=sql.encode(),stdout=subprocess.PIPE,stderr=subprocess.PIPE)
 if p.returncode: raise RuntimeError(p.stderr.decode())
 return p.stdout.decode().splitlines()
setup=r'''
create or replace function pg_temp.probe(t text, src text, op text) returns text language plpgsql as $$
declare v text; expr text;
begin
expr := format('convert_from(decode(%L, ''hex''), ''UTF8'')::%s',src,t);
case op
when 'text' then expr:=expr||'::text';
when 'wire' then expr:=format('encode(pg_catalog.%s_send(%s),''hex'')',t,expr);
when 'type' then expr:=format('pg_catalog.%s_typeof(%s)',t,expr);
when 'length' then expr:=format('pg_catalog.%s_array_length(%s)::text',t,expr);
when 'keys' then expr:=format('(select string_agg(encode(convert_to(k,''UTF8''),''hex''),'','') from pg_catalog.%s_object_keys(%s) k)',t,expr);
when 'field' then expr:=format('pg_catalog.%s_object_field(%s,''a'')::text',t,expr);
when 'fieldtext' then expr:=format('pg_catalog.%s_object_field_text(%s,''a'')',t,expr);
when 'element' then expr:=format('pg_catalog.%s_array_element(%s,-1)::text',t,expr);
when 'elementtext' then expr:=format('pg_catalog.%s_array_element_text(%s,-1)',t,expr);
end case;
execute 'select '||expr into v;
return case when v is null then 'NULL' else 'OK:'||encode(convert_to(v,'UTF8'),'hex') end;
exception when others then return 'ERR:'||SQLSTATE;
end $$;
'''
base=['null','true','false','0','-0','1.00','1e2','1.00e-2','1234567890123456789012345678901234567890','1e1000','1e-1000','[]','{}','[null]','[1, 2, null]','{"a":1,"a":2}','{"aa":0,"b":1,"a":1.00,"a":2.00}',r'"\u0000"',r'"\ud800"',r'"\udc00"',r'"\ud800\udc00"',r'"\ud800x"',r'"\ud800\u0041"','"\u00e9\U0001f600"',r'"\n\b\f\r\t\/\\\""','"\x00"','', '01','+1','1.','1e','NaN','[1,]','{"a":1,}','[','{"a":}',r'"\x00"',r'"\uZZZZ"','[[]]','{"a":null}','{"a":"x"}','{"a":{"z":1}}']
for i in range(32):
 base.extend([f' {{ "a" : {i}.00, "zz": [{i},null,"\u00e9"], "a": {i}e-2 }} ',f'[{i},{{"a":"v{i}"}},[false]]',f'{i}e+{i}', '['*(i+1)+'0'+']'*(i+1)])
base.extend(['['+','.join('[]' for _ in range(500))+']', '{'+','.join('"k%d":{}'%i for i in range(250))+'}', '['*60+'{}'+']'*60, r'{"a":1,"\u0061":2.00}', '"'+chr(0xe9)+chr(0x1f600)+'"', '"'+chr(0x2028)+'"'])
for n in ['-0.00','0e-10','-123.4500e2','1.00e2','1.00e-2','123456789012345678901234567890.012300']:
 base.append(n)
for esc in [r'\udfff',r'\udbff\udfff',r'\ud800\ud800',r'\u0000',r'\u00e9']:
 base.extend(['"'+esc+'"','{"a":"'+esc+'"}','{"'+esc+'":0}'])
ops=['text','wire','type','length','keys','field','fieldtext','element','elementtext']
cases=[(t,s,op) for s in base for t in ['json','jsonb'] for op in ops]
queries=[f"select pg_temp.probe('{t}','{s.encode().hex()}','{op}');" for t,s,op in cases]
sql=setup+'\n'+'\n'.join(queries)+'\n'
(ROOT/'fixtures/oracle.sql').write_text(sql,encoding='utf-8')
rows=pg(sql)
assert len(rows)==len(cases),(len(rows),len(cases))
(ROOT/'fixtures/oracle.tsv').write_text(''.join(f'{t}\t{s.encode().hex()}\t{op}\t{result}\n' for (t,s,op),result in zip(cases,rows)),encoding='utf-8')
version=pg('select version(); show server_encoding; select datcollate from pg_database where datname=current_database();')
meta={'image':'postgres@sha256:41fc5342eefba6cc2ccda736aaf034bbbb7c3df0fdb81516eba1ba33f360162c','version':version,'cases':len(cases),'sha256':{str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(ROOT.rglob('*')) if p.is_file() and p.name!='manifest.json'}}
(ROOT/'fixtures/manifest.json').write_text(json.dumps(meta,indent=2)+'\n')
print(len(rows), 'PG18 cases generated')


