import pathlib,subprocess,json,hashlib,itertools,os
root=pathlib.Path(__file__).resolve().parents[1]; out=root/'tests/fixtures'
def pg(sql):
 p=subprocess.run(['docker','exec','-i',os.environ.get('PG_JSON_PATH_CONTAINER','dd68-jsonpath-sol-20261106'),'psql','-XAtq','-v','ON_ERROR_STOP=1','-U','postgres','-d','operator_oracle'],input=sql.encode(),capture_output=True)
 if p.returncode: raise RuntimeError(p.stderr.decode())
 return p.stdout.decode().splitlines()
def lit(s): return "convert_from(decode('%s','hex'),'UTF8')"%s.encode().hex()
def enc(s): return '-' if s is None else s.encode().hex()
values=['null','true','false','0','1.2300','{}','[]','[1,2]','[true,false]','[[1],[2]]','{"a":true}','{"a":null}','{"a":[1,2]}','[{"a":1},{}]','[{}, {"a":1}]','{"a":0,"a":1}','{"a.b":2}','[{"a":{"b":1}},{}]', '[{"a":{}},{"a":{"b":true}}]', None]
paths=['$','$.a','$."a.b"','$.*','$[0]','$[2]','$[last]','$[*]','$[*].a','$.a[*]','$.a[0]','$v','$v.a','$[*].a.b','$.a.b[0]']
cases=[]
for a,p,mode,v,s,op in itertools.product(values,paths,['strict','lax'],['{}','{"v":null}','{"v":{"a":true}}'],[False,True],['query','query_array','query_first','exists','match']):
 path=mode+' '+p; target='NULL::jsonb' if a is None else lit(a)+'::jsonb'
 expr='jsonb_path_'+op+'('+','.join([target,lit(path)+'::jsonpath',lit(v)+'::jsonb',str(s).lower()])+')'
 if op=='query': expr='(select jsonb_agg(x) from '+expr+' x)'
 cases.append((enc(a),enc(path),enc(v),str(int(s)),op,expr))
setup="create function pg_temp.probe(e text) returns text language plpgsql as $$ declare v text; begin execute 'select ('||e||')::text' into v; return case when v is null then 'NULL' else 'OK:'||encode(convert_to(v,'UTF8'),'hex') end; exception when others then return 'ERR:'||SQLSTATE; end $$;\n"
sql=setup+'\n'.join('select pg_temp.probe(%s);'%lit(c[-1]) for c in cases)+'\n'
(out/'jsonpath.sql').write_text(sql); rows=pg(sql)
assert len(rows)==len(cases)
(out/'jsonpath.tsv').write_text(''.join('\t'.join(c[:-1])+'\t'+r+'\n' for c,r in zip(cases,rows)))
files=[out/'jsonpath.sql',out/'jsonpath.tsv',pathlib.Path(__file__)]+[root/'upstream'/n for n in ['jsonpath.c','jsonpath_gram.y','jsonpath_exec.c']]
(out/'jsonpath-manifest.json').write_text(json.dumps(dict(cases=len(rows),image='postgres@sha256:41fc5342eefba6cc2ccda736aaf034bbbb7c3df0fdb81516eba1ba33f360162c',version=pg('select version()'),collation=pg("select datcollate from pg_database where datname=current_database()"),sha256={str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest() for p in files}),indent=2)+'\n')
print(len(rows))

