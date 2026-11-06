import pathlib,json,subprocess,itertools,hashlib,argparse
p=argparse.ArgumentParser();p.add_argument("--output-root",required=True);p.add_argument("--container",required=True);args=p.parse_args()
out=pathlib.Path(args.output_root).resolve();out.mkdir(parents=True,exist_ok=True)
def pg(sql):
 p=subprocess.run(['docker','exec','-i',args.container,'psql','-XAtq','-v','ON_ERROR_STOP=1','-U','postgres','-d','postgres'],input=sql.encode(),capture_output=True);(out/'oracle-stderr.log').write_bytes(p.stderr);assert p.returncode==0,p.stderr;return p.stdout.decode().splitlines()
def lit(s):return "convert_from(decode('%s','hex'),'UTF8')"%s.encode().hex()
vals=['null','1','true','{}','[]','[1,2]','[[1],[2]]','{"a":1}','[{"a":[1,2]}, {}, {"a":{"a":true}},null]','{"a":[],"b":{"a":[true,false]},"c":[{"a":1},[]]}']
selectors=['0,0','last,0,last','-1','+0','last - 1','last+1','last-0','last - 2 to last','0 to last - 1','-2 to last+2','2,0,1','0,3,1','3,0','0 to 1,1 to last','last to 0,0','-1,0,last','last+2147483648','2147483648','-2147483649','last-2147483649','9223372036854775807','last+9223372036854775807','last-9223372036854775807','-9223372036854775808','0,2147483648,1','0 to 2147483648,0','0 to 1,-1 to 1']
paths=['$['+x+']' for x in selectors]+['$.**['+x+']' for x in selectors]
paths+=['$[0,0].a','$[0,1].a','$[1,0].a','$[0,last].type()','$[last-1,last].size()','$.**[0,last].a','$v[0,last]','$[0,last] == 1','exists($[0,2147483648])','($[0,2147483648] == 1) is unknown','$[0,last] ? (@ == 1)','$ ? (@[last-1,last] == $v)','$[0,last][last-1,last]','$.size()[0,0]','$.type()[0,0]','$.**[0,last].**[last-1,last]']
cases=[]
for a,p,m,v,s,op in itertools.product(vals,paths,['strict','lax'],['{}','{"v":[1,2]}'],[False,True],['query','query_array','query_first','exists','match']):
 path=m+' '+p;expr='jsonb_path_'+op+'('+','.join([lit(a)+'::jsonb',lit(path)+'::jsonpath',lit(v)+'::jsonb',str(s).lower()])+')'
 if op=='query':expr='(select jsonb_agg(x) from '+expr+' x)'
 cases.append((a.encode().hex(),path.encode().hex(),v.encode().hex(),str(int(s)),op,expr))
setup="create function pg_temp.probe(e text) returns text language plpgsql as $$ declare v text; begin execute 'select ('||e||')::text' into v; return case when v is null then 'NULL' else 'OK:'||encode(convert_to(v,'UTF8'),'hex') end; exception when others then return 'ERR:'||SQLSTATE; end $$;\n"
sql=setup+'\n'.join('select pg_temp.probe(%s);'%lit(c[-1]) for c in cases)+'\n';(out/'selectors.sql').write_text(sql);rows=pg(sql);assert len(rows)==len(cases);(out/'selectors.tsv').write_text(''.join('\t'.join(c[:-1])+'\t'+row+'\n' for c,row in zip(cases,rows)))
print(len(rows));print(pg('select version(); select datcollate from pg_database where datname=current_database();'))
