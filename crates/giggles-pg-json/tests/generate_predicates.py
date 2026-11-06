import pathlib,subprocess,json,hashlib,itertools
root=pathlib.Path(__file__).resolve().parents[1]; out=root/'tests/fixtures'
def pg(sql):
 p=subprocess.run(['docker','exec','-i','dd68-jsonpath-predicates-sol-20261106','psql','-XAtq','-v','ON_ERROR_STOP=1','-U','postgres','-d','operator_oracle'],input=sql.encode(),capture_output=True)
 if p.returncode: raise RuntimeError(p.stderr.decode())
 return p.stdout.decode().splitlines()
def lit(s): return "convert_from(decode('%s','hex'),'UTF8')"%s.encode().hex()
def enc(s): return '-' if s is None else s.encode().hex()
values=['null','true','false','0','1.000','1.00000000000000000000001','"a"','"ÃƒÂ©"','{}','[]','[1,2]','[1,"a"]','[[1],[2]]','{"a":true}','{"a":[1,2]}','[{"a":1},{}]']
paths=['$[0 to 2]','$[-1 to last]','$[last to 0]','$[2 to 1]','$[last to last]','$[0 to last]','$ == 1','$ != null','$ < null','$ >= null','$ == $','$ == $v','$[*] == $v[*]','exists($.a)','! (exists($.a))','( $.a == 1 ) is unknown','$.a == 1 || exists($.a)','$.a == 1 && exists($.a)','$ ? (@ == 1)','$[*] ? (@.a == 1)','$ ? (exists(@.a))','$ ? (@ == $v)','$ ? (@ != null)','$ ? (@ > 0 && @ < 2)','$ ? (! (exists(@.a)))']
paths += ['$ '+op+' '+literal for op in ['==','!=','<','>','<=','>='] for literal in ['null','true','false','1.00000000000000000000001','"a"','"Ã©"']]
paths += ['$.**','$.**{0}','$.**{1}','$.**{0 to 2}','$.**{last}','$.**{1 to last}','$.**{2 to 1}']
cases=[]
for a,p,mode,v,s,op in itertools.product(values,paths,['strict','lax'],['{}','{"v":null}','{"v":[1,2]}'],[False,True],['query','query_array','query_first','exists','match']):
 path=mode+' '+p; target='NULL::jsonb' if a is None else lit(a)+'::jsonb'
 expr='jsonb_path_'+op+'('+','.join([target,lit(path)+'::jsonpath',lit(v)+'::jsonb',str(s).lower()])+')'
 if op=='query': expr='(select jsonb_agg(x) from '+expr+' x)'
 cases.append((enc(a),enc(path),enc(v),str(int(s)),op,expr))
setup="create function pg_temp.probe(e text) returns text language plpgsql as $$ declare v text; begin execute 'select ('||e||')::text' into v; return case when v is null then 'NULL' else 'OK:'||encode(convert_to(v,'UTF8'),'hex') end; exception when others then return 'ERR:'||SQLSTATE; end $$;\n"
sql=setup+'\n'.join('select pg_temp.probe(%s);'%lit(c[-1]) for c in cases)+'\n'
(out/'predicates.sql').write_text(sql); rows=pg(sql)
assert len(rows)==len(cases)
(out/'predicates.tsv').write_text(''.join('\t'.join(c[:-1])+'\t'+r+'\n' for c,r in zip(cases,rows)))
files=[out/'predicates.sql',out/'predicates.tsv',pathlib.Path(__file__)]+[root/'upstream'/n for n in ['jsonpath.c','jsonpath_gram.y','jsonpath_exec.c']]
(out/'predicates-manifest.json').write_text(json.dumps(dict(cases=len(rows),image='postgres@sha256:41fc5342eefba6cc2ccda736aaf034bbbb7c3df0fdb81516eba1ba33f360162c',version=pg('select version()'),collation=pg("select datcollate from pg_database where datname=current_database()"),sha256={str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest() for p in files}),indent=2)+'\n')
print(len(rows))


