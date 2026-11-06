import pathlib,json,subprocess,itertools,hashlib,argparse
p=argparse.ArgumentParser();p.add_argument("--output-root",required=True);p.add_argument("--container",required=True);args=p.parse_args()
out=pathlib.Path(args.output_root).resolve();out.mkdir(parents=True,exist_ok=True)
def pg(sql):
 p=subprocess.run(['docker','exec','-i',args.container,'psql','-XAtq','-v','ON_ERROR_STOP=1','-U','postgres','-d','postgres'],input=sql.encode(),capture_output=True);(out/'oracle-stderr.log').write_bytes(p.stderr);assert p.returncode==0,p.stderr;return p.stdout.decode().splitlines()
def lit(s):return "convert_from(decode('%s','hex'),'UTF8')"%s.encode().hex()
vals=['null','1','true','{}','[]','[1,2]','[[1],[2]]','{"a":1}','[{"a":[1,2]}, {}, {"a":{"a":true}},null]','{"a":[],"b":{"a":[true,false]},"c":[{"a":1},[]]}']
paths=[d+c for d in ['$.**','$.**{0}','$.**{1}','$.**{last}','$.**{1 to last}','$.**{0 to 2}','$.**{2 to 1}'] for c in ['.a','.a.a','.*','[0]','[last]','[*]','[-1 to last]','[0 to 2]',' ? (exists(@.a))',' ? ((exists(@.a)) is unknown)',' ? (@ == $v)','.**.a']]
paths += ['$.**.a == 1','exists($.**.a)','(exists($.**.missing)) is unknown','$.** ? (@ == 1 || @ == $missing)','$.** ? (@ == 1 && @ == $missing)','$v.**.a','$.missing.**.a','$.**.missing.a','$.** ? (exists(@.missing))','$.** ? ((@.missing == 1) is unknown)']
cases=[]
for a,p,m,v,s,op in itertools.product(vals,paths,['strict','lax'],['{}','{"v":1}'],[False,True],['query','query_array','query_first','exists','match']):
 path=m+' '+p;expr='jsonb_path_'+op+'('+','.join([lit(a)+'::jsonb',lit(path)+'::jsonpath',lit(v)+'::jsonb',str(s).lower()])+')'
 if op=='query':expr='(select jsonb_agg(x) from '+expr+' x)'
 cases.append((a.encode().hex(),path.encode().hex(),v.encode().hex(),str(int(s)),op,expr))
setup="create function pg_temp.probe(e text) returns text language plpgsql as $$ declare v text; begin execute 'select ('||e||')::text' into v; return case when v is null then 'NULL' else 'OK:'||encode(convert_to(v,'UTF8'),'hex') end; exception when others then return 'ERR:'||SQLSTATE; end $$;\n"
sql=setup+'\n'.join('select pg_temp.probe(%s);'%lit(c[-1]) for c in cases)+'\n';(out/'continuations.sql').write_text(sql);rows=pg(sql);assert len(rows)==len(cases);(out/'continuations.tsv').write_text(''.join('\t'.join(c[:-1])+'\t'+row+'\n' for c,row in zip(cases,rows)))
print(len(rows));print(pg('select version(); select datcollate from pg_database where datname=current_database();'))
