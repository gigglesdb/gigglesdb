"""Independent PG18 temporal SQL corpus. No temporal candidate transport is invented."""
import argparse
from pathlib import Path
from workdir import default_output,check_outside_repo
import gzip,json
from conformance import ROOT,IMAGE,Reference,digest,validate_pin,validate_inventory,validate_cases
TYPES={'date':'2000-02-29','time without time zone':'23:59:59.999999','time with time zone':'04:05:06+05:45','timestamp without time zone':'1969-12-31 23:59:59.999999','timestamp with time zone':'2024-11-03 01:30:00 America/New_York','interval':'1 mon -40 days 25:00:00.123456'}
SETUP=["SET TIME ZONE 'UTC'","SET DateStyle = 'ISO, YMD'","SET IntervalStyle = 'postgres'","SET lc_time = 'C'","SET extra_float_digits = 3"]
def cast(v,t): return 'CAST(NULL AS '+t+')' if v is None else "CAST('"+v.replace("'","''")+"' AS "+t+')'
def fixtures():
    inv=json.loads(gzip.decompress((ROOT/'postgres-18.0.json.gz').read_bytes()));cases=[]
    def add(ids,label,expr,kind='boundary',special=None):
        c=dict(id='temporal-%04d-%s'%(len(cases),label),routines=ids,kind=kind,setup=[],reference_setup=SETUP.copy(),query='SELECT '+expr+' AS v',exact_numeric=True,expected_core_error='Sql(Type)',expectedUnsupported=True,core_error_rationale='Temporal SQL foundation unavailable; negative SQL expectations are not implementation support.')
        if special:c['special_form']=special
        cases.append(c)
    names=set('extract date_part date_trunc date_bin make_date make_time make_timestamp make_timestamptz make_interval justify_days justify_hours justify_interval age timezone isfinite'.split())
    for r in inv['routines']:
        ident=r['identity'];name=ident.split('(')[0].split('.')[-1].strip(chr(34));ts=ident.split('(')[1].split(')')[0].split(', ')
        if name not in names or ('xid' in ts):continue
        # Session/current-date overloads are deliberately excluded from deterministic reference.
        if name=='age' and len(ts)==1 or name=='timezone' and (len(ts)==1 or 'time with time zone' in ts and ts[0]=='text'):continue
        def arg(t,i):
            if t in TYPES:return cast(TYPES[t],t)
            if t=='text':return cast('America/New_York' if name in {'timezone','make_timestamptz'} or name=='date_trunc' and i==2 else 'day',t)
            if t=='integer':return str(([2024,2,29,12,34,56][i] if name!='make_interval' else [1,-2,3,-4,5,-6][i]))
            if t=='double precision':return cast('0.9999995',t)
            raise ValueError((name,t))
        args=[arg(t,i) for i,t in enumerate(ts)]
        if name=='date_bin':args[0]=cast('15 minutes','interval')
        def call(a):return 'pg_catalog.'+('\"extract\"' if name=='extract' else name)+'('+','.join(a)+')'
        add([ident],name+'-normal',call(args),'normal')
        for i,t in enumerate(ts):
            a=args.copy();a[i]=cast(None,t);add([ident],name+'-null-'+str(i),call(a),'null')
        if name in {'extract','date_part','date_trunc'}:
            for field in 'microseconds milliseconds second minute hour day week month quarter year decade century millennium dow isodow doy isoyear epoch timezone timezone_hour timezone_minute nonsense'.split():
                a=args.copy();a[0]=cast(field,'text');add([ident],name+'-'+field,call(a))
                if name=='extract':add([ident],'special-extract-'+field,'EXTRACT('+field+' FROM '+args[1]+')',special='extract')
        if name in {'make_time','make_timestamp','make_timestamptz','make_interval'}:
            i=ts.index('double precision')
            for v in ['-0.0000005','0.0000005','0.0000015','59.9999995','60','NaN','Infinity']:
                a=args.copy();a[i]=cast(v,'double precision');add([ident],name+'-round-'+v,call(a))
        if name in {'justify_days','justify_hours','justify_interval'}:
            for v in ['-1 mon 40 days -25 hours','1 mon -1 day','-30 days 24 hours','35 days -49 hours','Infinity','-Infinity']:
                add([ident],name+'-mixed',call([cast(v,'interval')]))
        if name=='date_bin':
            for v in ['0 seconds','-1 second','1 mon','0.000001 second','1000000 days']:
                a=args.copy();a[0]=cast(v,'interval');add([ident],'bin-stride',call(a))
        if name=='timezone' and ts[0]=='text':
            for zone in ['America/New_York','Australia/Lord_Howe','Pacific/Apia','UTC','not/a_zone']:
                for dt in ['2024-03-10 02:30','2024-11-03 01:30','2024-10-06 02:15','2011-12-30 12:00']:
                    add([ident],'zone-transition',call([cast(zone,'text'),cast(dt,ts[1])]))
    for t,normal in TYPES.items():
        name={'date':'date_in','time without time zone':'time_in','time with time zone':'timetz_in','timestamp without time zone':'timestamp_in','timestamp with time zone':'timestamptz_in','interval':'interval_in'}[t]
        ident=next(r['identity'] for r in inv['routines'] if r['identity'].startswith('pg_catalog.'+name+'('))
        vals=[normal,None,'bogus','infinity','-infinity']
        if t=='date':vals+=['0001-01-01 BC','4713-11-24 BC','5874897-12-31','5874898-01-01','1900-02-29','2000-02-29','2020-12-31','2021-01-01']
        elif 'timestamp' in t:vals+=['0001-12-31 BC','294276-12-31 23:59:59.999999','294277-01-01','1970-01-01','1969-12-31 23:59:59.999999','2000-02-29 24:00','2024-01-31']
        elif 'time' in t:vals+=['24:00:00','24:00:00.000001','23:59:59.9999995','00:00:00.0000005','12:34:56-15:59']
        else:vals+=['1 mon -40 days 25 hours','-0.0000005 seconds','2147483647 months','2147483648 months','1 year 2 mons -3 days -04:05:06.123456']
        for v in vals:add([ident],'literal-'+name,cast(v,t),'null' if v is None else 'boundary',special='cast')
    for t in ['timestamp without time zone','timestamp with time zone']:
        ident='pg_catalog.age('+t+', '+t+')/f'
        for a,b in [('2024-02-29','2023-02-28'),('2024-03-31','2024-02-29'),('2023-03-01','2023-01-31'),('0001-01-01','0001-01-01 BC')]:
            add([ident],'age-calendar','age('+cast(a,t)+','+cast(b,t)+')')
    ident='pg_catalog."extract"(text, date)/f'
    for dt in ['2015-12-31','2016-01-01','2016-01-04','2020-12-31','2021-01-01','2000-02-29','1900-03-01','0001-01-01 BC']:
        for field in ['isoyear','week','isodow','epoch']:
            add([ident],'iso-calendar','EXTRACT('+field+' FROM '+cast(dt,'date')+')',special='extract')
    ident='pg_catalog.make_date(integer, integer, integer)/f'
    for args in ['2000,2,29','1900,2,29','2024,4,31','0,1,1','-1,12,31','5874897,12,31','5874898,1,1']:
        add([ident],'constructor-date','make_date('+args+')')
    seen=set();unique=[]
    for c in cases:
        if c['query'] in seen:continue
        seen.add(c['query']);unique.append(c)
    return inv,unique

def observe():
    parser=argparse.ArgumentParser()
    parser.add_argument('--output',type=Path,default=default_output('observe-temporal'),help='new output directory outside repository')
    parser.add_argument('--reuse-observed',action='store_true')
    args=parser.parse_args()
    output=args.output;check_outside_repo(output);output.mkdir(parents=True,exist_ok=False)
    inv,cases=fixtures()
    import sys
    cached={};cached_binary={}
    if args.reuse_observed and (ROOT/'cases-temporal.json').exists():
        cached={c['id']:c for c in json.loads((ROOT/'cases-temporal.json').read_text())}
        cached_binary={b['id']:b for b in json.loads((ROOT/'temporal-reference.json').read_text())['binary']}
    with Reference() as ref:
        validate_pin(ref.inventory(),json.loads((ROOT/'reference.json').read_text()))
        tz=ref.sql("SHOW server_encoding; SELECT datcollate, datctype FROM pg_database WHERE datname=current_database();");
        from conformance import require,run
        tzdata=require(run(['docker','exec',ref.container,'sh','-c','cat /usr/share/zoneinfo/tzdata.zi | head -1']))
        if '2025b' not in tzdata:raise RuntimeError('tzdata pin mismatch: '+tzdata)
        if require(tz)!='UTF8\nC|C':raise RuntimeError('locale/encoding mismatch: '+require(tz))
        for ix,c in enumerate(cases):
            if ix%100==0:print('observe',ix,len(cases),flush=True)
            old=cached.get(c['id'],{})
            c['expected']=old['expected'] if {k:v for k,v in old.items() if k!='expected'}==c else ref.query(c)
            if c['expected']['status'] not in {'ok','error'}:raise RuntimeError(c)
        validate_cases(cases,validate_inventory(inv))
        # Actual result type chooses send function; separate evidence, not fake candidate cells.
        binary=[]
        sends={'date':'date_send','time without time zone':'time_send','time with time zone':'timetz_send','timestamp without time zone':'timestamp_send','timestamp with time zone':'timestamptz_send','interval':'interval_send','numeric':'numeric_send','double precision':'float8send','boolean':'boolsend'}
        for c in cases:
            e=c['expected']
            if e['status']!='ok':continue
            t=e['columns'][0]['type'];fn=sends.get(t)
            if fn:
                q=dict(c,query="SELECT encode(pg_catalog."+fn+"(v),'hex') AS v FROM ("+c['query']+") AS x",exact_numeric=False)
                old=cached.get(c['id'])
                binary.append(cached_binary[c['id']] if old==c and c['id'] in cached_binary else dict(id=c['id'],type=t,result=ref.query(q)))
    (output/'cases-temporal.json').write_text(json.dumps(cases,indent=2)+'\n')
    (output/'temporal-reference.json').write_text(json.dumps(dict(image=IMAGE,inventory_sha256=digest(inv),corpus_sha256=digest(cases),case_count=len(cases),tzdata=tzdata,locale=require(tz),owned_container=ref.container,cleanup='Reference context exit succeeded before writing',binary=binary,full=False,verified=0),indent=2)+'\n')
    print(len(cases),digest(cases),flush=True)
if __name__=='__main__':observe()
