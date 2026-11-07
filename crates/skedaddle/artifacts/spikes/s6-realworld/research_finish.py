"""Finish bounded official-source dataset metadata; no dataset downloads."""
import hashlib,json,pathlib,sys,urllib.request
ROOT=pathlib.Path(__file__).resolve().parent;OUT=ROOT.parents[1]/('s6-'+sys.argv[1])
def fetch(name,url):
    with urllib.request.urlopen(urllib.request.Request(url,headers={'User-Agent':'Skedaddle-S6-research'}),timeout=60) as r:raw=r.read(1024*1024)
    with (OUT/('source-'+name+'.txt')).open('x',encoding='utf8') as f:f.write(raw.decode())
    return {'url':url,'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()}
pagila=json.loads((OUT/'source-pagila-tree.txt').read_text());chinook=json.loads((OUT/'source-chinook-tree.txt').read_text())
sources={}
sources['pagila-license-corrected']=fetch('pagila-license-corrected','https://raw.githubusercontent.com/devrimgunduz/pagila/'+pagila['sha']+'/LICENSE.txt')
sources['pagila-schema']=fetch('pagila-schema','https://raw.githubusercontent.com/devrimgunduz/pagila/'+pagila['sha']+'/pagila-schema.sql')
sources['gitea-version-requirements']=fetch('gitea-version-requirements','https://docs.gitea.com/1.24/installation/database-prep/')
sources['miniflux-migration-source-tree']=fetch('miniflux-migration-source-tree','https://api.github.com/repos/miniflux/v2/git/trees/2.2.14?recursive=1')
sources['miniflux-migrations']=fetch('miniflux-migrations','https://raw.githubusercontent.com/miniflux/v2/2.2.14/internal/database/migrations.go')
sources['netbox-rejection']=fetch('netbox-rejection','https://netboxlabs.com/docs/netbox/v4.5/installation/')
selected={
 'pagila':{'commit':pagila['sha'],'files':[x for x in pagila['tree'] if x['path'] in ('pagila-schema.sql','pagila-data.sql','LICENSE.txt')],'licence':'PostgreSQL License','planned_database_cap_bytes':1000000000,'expected_database_budget_bytes':100000000,'keys':'integer/smallint; composite film_actor; UUIDv7 customer/rental/payment; date-range temporal optional','types':'numeric, enum, text[], tsvector, timestamp, UUID; optional JSONB/temporal subsets'},
 'chinook':{'commit':chinook['sha'],'files':[x for x in chinook['tree'] if 'PostgreSql' in x['path'] and x['path'].endswith('.sql')],'licence':'MIT','planned_database_cap_bytes':1000000000,'expected_database_budget_bytes':50000000,'keys':'integer primary keys; composite (PlaylistId,TrackId); quoted mixed-case identifiers','types':'varchar/text, numeric, timestamp, integer'},
 'osm-monaco':{'url':'https://download.geofabrik.de/europe/monaco.html','reported_source_size':'690 KB PBF (page snapshot)','licence':'ODbL 1.0; OpenStreetMap attribution','planned_database_cap_bytes':1000000000,'expected_database_budget_bytes':100000000,'keys':'composite (element_type,bigint osm_id), text tag keys; relation memberships','types':'bigint, text, JSONB tags, numeric coordinates; PostGIS geometry optional, requires extension image outside this run'},
 'notes':'No dataset data downloaded or imported. Source byte sizes are upstream Git tree sizes/page snapshot, not measured imported DB sizes. Hard cap must be checked by importer; budgets are conservative estimates, not proof of import footprint.'}
with (OUT/'dataset-choices.json').open('x',encoding='utf8') as f:json.dump(selected,f,indent=2)
with (OUT/'sources-additional.json').open('x',encoding='utf8') as f:json.dump(sources,f,indent=2)
print(json.dumps(selected,indent=2))
