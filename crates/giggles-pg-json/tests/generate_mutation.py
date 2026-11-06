"""Independent PG18 mutation oracle, executed only by the live verifier."""
import pathlib, subprocess, json, hashlib, os, random
root=pathlib.Path(__file__).resolve().parent
name=os.environ.get('PG_JSON_MUTATION_CONTAINER','dd68-json-mutation-sol-20261106')
def pg(sql):
 p=subprocess.run(['docker','exec','-i',name,'psql','-XAtq','-v','ON_ERROR_STOP=1','-U','postgres','-d','operator_oracle'],input=sql.encode(),capture_output=True)
 if p.returncode: raise RuntimeError(p.stderr.decode())
 return p.stdout.decode().splitlines()
def lit(s): return "convert_from(decode('%s','hex'),'UTF8')"%s.encode().hex()
def enc(s): return '-' if s is None else s.encode().hex()
values=['null','1','[]','{}','[1,2]','[null,{},[1]]','{"a":1}','{"a":null}','{"a":[]}','{"a":{"b":[1,2]}}','{"a":[{"b":0}]}','{"aa":0,"b":1}','[[],{}]','{"a":{},"b":1.00}',None]
paths=[[],['a'],['b'],['a','b'],['a','b','-1'],['a','0'],['a','0','b'],['0'],['1'],['-1'],['-2'],['-99'],['99'],['2147483647'],['-2147483648'],['2147483648'],['x'],['  +1'],['1 '],[''],[None],['missing',None],['a',None],['0',None],['0','x'],['a','b',None],None]
new=['null','1.2300','{"new":[null,"Ã©",1]}',None]
treatments=['use_json_null','delete_key','return_target','raise_exception','invalid',None]
cases=[]
def add(a,p,v,flag,op,t):
 target='NULL::jsonb' if a is None else lit(a)+'::jsonb'
 path='NULL::text[]' if p is None else 'array['+','.join('NULL' if k is None else lit(k) for k in p)+']::text[]'
 value='NULL::jsonb' if v is None else lit(v)+'::jsonb'
 f='NULL::boolean' if flag is None else str(flag).lower()
 expr=op+'('+','.join([target,path,value,f]+([] if op!='jsonb_set_lax' else ['NULL::text' if t is None else lit(t)]))+')'
 cases.append((enc(a),'~' if p is None else ','.join('-' if k is None else 'x'+enc(k) for k in p),enc(v),'N' if flag is None else str(int(flag)),op,enc(t),expr))
for a in values:
 for p in paths:
  for v in new:
   for f in [False,True]:
    for op in ['jsonb_set','jsonb_insert']: add(a,p,v,f,op,None)
    for t in treatments: add(a,p,v,f,'jsonb_set_lax',t)
# deterministic independently generated nested inputs and paths
rng=random.Random(680318)
for i in range(500):
 a=json.dumps({'a':[rng.randrange(-100,100), {'b':[None,rng.randrange(10)]}]},ensure_ascii=False)
 add(a,rng.choice(paths),rng.choice(new),rng.choice([False,True,None]),rng.choice(['jsonb_set','jsonb_insert','jsonb_set_lax']),rng.choice(treatments))
setup="create function pg_temp.probe(e text) returns text language plpgsql as $$ declare v text; begin execute 'select ('||e||')::text' into v; return case when v is null then 'NULL' else 'OK:'||encode(convert_to(v,'UTF8'),'hex') end; exception when others then return 'ERR:'||SQLSTATE; end $$;\n"
sql=setup+'\n'.join('select pg_temp.probe(%s);'%lit(c[-1]) for c in cases)+'\n'
(root/'fixtures/mutation.sql').write_text(sql,encoding='utf8')
rows=pg(sql); assert len(rows)==len(cases)
(root/'fixtures/mutation.tsv').write_text(''.join('\t'.join(c[:-1])+'\t'+r+'\n' for c,r in zip(cases,rows)),encoding='utf8')
files=[root/'fixtures/mutation.sql',root/'fixtures/mutation.tsv',pathlib.Path(__file__),root.parent/'upstream/jsonfuncs.c']
(root/'fixtures/mutation-manifest.json').write_text(json.dumps({'cases':len(cases),'version':pg('select version()'),'collation':pg('select datcollate from pg_database where datname=current_database()'),'image':'postgres@sha256:41fc5342eefba6cc2ccda736aaf034bbbb7c3df0fdb81516eba1ba33f360162c','sha256':{str(p.relative_to(root.parent)):hashlib.sha256(p.read_bytes()).hexdigest() for p in files}},indent=2)+'\n')
print(len(rows))
