"""Independent PG18 operator oracle. Requires the owned container created in this task."""
import pathlib,subprocess,json,hashlib,os
root=pathlib.Path(__file__).resolve().parent
name=os.environ.get('PG_JSON_OPERATOR_CONTAINER','dd68-json-functions-sol-20261106')
def pg(sql):
 p=subprocess.run(['docker','exec','-i',name,'psql','-XAtq','-v','ON_ERROR_STOP=1','-U','postgres','-d','operator_oracle'],input=sql.encode(),capture_output=True)
 if p.returncode: raise RuntimeError(p.stderr.decode())
 return p.stdout.decode().splitlines()
def lit(s): return "convert_from(decode('%s','hex'),'UTF8')"%s.encode().hex()
values=['null','false','true','0','0.00','1','1.000','-1','123456789012345678901234567890.00','"a"','""','"\u00e9"','"??"','[]','{}','[null]','[1]','[1,1]','[1,2]','[2,1]','[[1,2]]','[[1],[2]]','[[1,2],1]','{"a":1}','{"a":1.00}','{"a":null}','{"a":[1,2]}','[{"a":1,"b":2}]','[{"a":1},{"b":2}]','{"aa":0,"b":1}','{"a":1,"a":2}','[null,{"a":null},[null,1]]']
values.extend(['"\U0001f600"', '"\u2028"', '{"\u00e9":null}', '["a","\u00e9"]', '1e1000','1e-1000','[[]]','[[[]]]','[{},[]]', '{"a":[null,1]}'])
cases=[]
for a in values:
 for b in values:
  for op,expr in [('concatwire',"encode(jsonb_send(%s || %s),'hex')"),('concat','(%s || %s)'),('less','(%s < %s)'),('eq','(%s = %s)'),('contains','(%s @> %s)'),('contained','(%s <@ %s)')]:
   cases.append((a,b,op,expr%(lit(a)+'::jsonb',lit(b)+'::jsonb')))
for a in values:
 for op,expr in [('deletepath',"(%s #- array['a','-1'])"),('nullpath',"(%s #- array[null]::text[])"),('badpath',"(%s #- array['x'])"),('strip','jsonb_strip_nulls(%s,false)'),('striparrays','jsonb_strip_nulls(%s,true)'),('deletekey',"(%s - 'a')"),('deleteindex','(%s - (-1))'),('exists',"(%s ? 'a')"),('any',"(%s ?| array['a',null,'\u00e9'])"),('all',"(%s ?& array['a',null,'\u00e9'])"),('path',"(%s #> array['a','-1'])"),('pathtext',"(%s #>> array['a','-1'])"),('emptytext',"(%s #>> array[]::text[])")]:
  cases.append((a,'',op,expr%(lit(a)+'::jsonb')))
setup="""create function pg_temp.probe(e text) returns text language plpgsql as $$ declare v text; begin execute 'select ('||e||')::text' into v; return case when v is null then 'NULL' else 'OK:'||encode(convert_to(v,'UTF8'),'hex') end; exception when others then return 'ERR:'||SQLSTATE; end $$;\n"""
sql=setup+'\n'.join("select pg_temp.probe(%s);"%lit(e) for a,b,op,e in cases)+'\n'
(root/'fixtures/operators.sql').write_text(sql,encoding='utf8')
rows=pg(sql)
assert len(rows)==len(cases)
(root/'fixtures/operators.tsv').write_text(''.join('%s\t%s\t%s\t%s\n'%(a.encode().hex(),b.encode().hex(),op,r) for (a,b,op,e),r in zip(cases,rows)),encoding='utf8')
(root/'fixtures/operators-manifest.json').write_text(json.dumps({'cases':len(cases),'version':pg('select version()'),'image':'postgres@sha256:41fc5342eefba6cc2ccda736aaf034bbbb7c3df0fdb81516eba1ba33f360162c','sha256':{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in [root/'fixtures/operators.sql',root/'fixtures/operators.tsv',pathlib.Path(__file__)]}},indent=2)+'\n')
print(len(rows))
