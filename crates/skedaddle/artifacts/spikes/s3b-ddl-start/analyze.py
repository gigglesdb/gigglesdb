"""Independent retained-evidence audit; read-only scoped Docker inventories; no deletion."""
import pathlib, sys, json, subprocess, hashlib
R=pathlib.Path(__file__).resolve().parent; B=R.parents[2]; RUN=sys.argv[1]
O=B/'artifacts'/('s3b-'+RUN)
def readlines(p): return [json.loads(x) for x in p.read_text().splitlines()]
def lsn(s):
    a,b=s.split('/'); return int(a,16)*2**32+int(b,16)
checks=[]
def check(name,yes):
    checks.append(dict(name=name,passed=bool(yes)))
    if not yes: print('FAIL',name)
versions=[]
for major in (16,17,18):
    p=O/f'pg{major}'; ev=readlines(p/'events.jsonl'); cases={e['case']:e for e in ev if 'case' in e}
    raw=[e['record'] for e in ev if 'record' in e]; live=[e for e in ev if 'check' in e]
    check(f'{major}:live',len(live)==27 and all(e['pass'] for e in live))
    check(f'{major}:complete',any('summary' in e and e['summary']['failed']==0 for e in ev) and not any('fatal' in e for e in ev))
    ids=[]; seen=[]; open_tx=None; tx_messages=[]; last_commit=0
    for rec in raw:
        k=rec['kind']
        if k=='Begin':
            check(f'{major}:begin:{rec["xid"]}',open_tx is None);open_tx=rec;tx_messages=[]
        if k=='Message':
            m=json.loads(rec['content']);tx_messages.append((m,rec));seen.append(m)
            check(f'{major}:message:{m["id"]}:{m["ddl"]}',open_tx is not None and rec['transactional'] and str(open_tx['xid'])==m['xid'] and lsn(rec['lsn'])<=lsn(open_tx['final_lsn']))
            if m['ddl']=='start': ids.append(m['id'])
        if k=='Commit':
            check(f'{major}:commit:{rec["xid"]}',open_tx is not None and rec['xid']==open_tx['xid'] and rec['commit']==open_tx['final_lsn'] and lsn(rec['commit'])>last_commit)
            open_tx=None;last_commit=lsn(rec['commit'])
    check(f'{major}:raw_complete',open_tx is None and len(seen)==30 and len(ids)==16 and len(set(ids))==16)
    check(f'{major}:helper_start_empty',all(m['start_helper_rows']==0 and m['start_helper_error'] is None for m in seen if m['ddl']=='start'))
    helper=next(e['helper_outside_event'] for e in ev if 'helper_outside_event' in e)
    check(f'{major}:helper_outside_error','39P03' in helper)
    for name,n in [('add_column',1),('create_index',1),('drop_index',1),('drop_table',1),('multistmt',3),('reattach',1),('drop_unique',1),('finalize',1),('reattach_strict',1)]:
        m=cases[name]['messages']
        check(f'{major}:{name}:pairs',len(m)==2*n and all(m[i]['ddl']=='start' and m[i+1]['ddl']=='finish' and m[i]['id']==m[i+1]['id'] and m[i]['xid']==m[i+1]['xid'] and lsn(m[i]['message_lsn'])<lsn(m[i+1]['message_lsn']) for i in range(0,len(m),2)))
        check(f'{major}:{name}:one_txn',len({v['xid'] for v in m})==1)
    check(f'{major}:multi_distinct_ids',len({m['id'] for m in cases['multistmt']['messages']})==3)
    commits={str(r['xid']):r for r in raw if r['kind']=='Commit'}
    split_summary={}
    for name in ('detach_success','cic_nonunique','cic_unique','cic_failure','detach_cancel'):
        c=cases[name];early=c['early_messages'];m=c['messages'];first=c['first_catalog'];start=m[0]
        check(f'{major}:{name}:first_xid',len(early)==1 and early[0]['ddl']=='start' and start['xid']==first['xmin'] and commits[start['xid']]['commit']==start['commit_lsn'])
        check(f'{major}:{name}:lsns',lsn(start['message_lsn'])<lsn(start['commit_lsn'])<=lsn(first['observer_lsn']))
        check(f'{major}:{name}:held',any('Lock' in row for row in c['waiting']))
        if name.startswith('cic_'):
            check(f'{major}:{name}:class_first_xid',first['class_xmin']==start['xid'] and first['valid'] is False)
        else: check(f'{major}:{name}:pending',first['pending'] is True)
        failed=name in ('cic_failure','detach_cancel')
        if failed:
            check(f'{major}:{name}:unmatched',len(m)==1 and not c['late_messages'] and c['error'] is not None)
            check(f'{major}:{name}:signal',c['after_catalog'][0][0]==('true' if name=='detach_cancel' else 'false'))
            check(f'{major}:{name}:error',('23505' if name=='cic_failure' else '57014') in c['error'])
        else:
            check(f'{major}:{name}:finish',len(m)==2 and m[1]['ddl']=='finish' and m[1]['id']==start['id'] and m[1]['xid']!=start['xid'] and lsn(m[1]['commit_lsn'])>lsn(start['commit_lsn']) and c['session_id_after'][0][0]==start['id'])
        split_summary[name]=dict(start_xid=start['xid'],start_message_lsn=start['message_lsn'],first_commit_lsn=start['commit_lsn'],finish_commit_lsn=None if failed else m[1]['commit_lsn'],catalog=first,after=c['after_catalog'])
    check(f'{major}:finalize_new_id',cases['finalize']['messages'][0]['id']!=cases['detach_cancel']['messages'][0]['id'])
    for name in ('strict_detach','strict_cic','strict_unique_cic'):
        row=next(e for e in live if e['check']==name);d=row['detail']
        check(f'{major}:{name}:strict',d['before']==d['after'] and 'P0001' in d['error'] and 'SKEDADDLE strict:' in d['error'] and not any(v['kind']=='Message' for v in d['records']))
    for name in ('no_markers_before_commit','rollback_no_markers'):
        row=next(e for e in live if e['check']==name)
        check(f'{major}:{name}:no_message',not any(r['kind']=='Message' for r in row['detail']['records']))
    sizes=json.loads((p/'database-sizes.json').read_text())
    max_size=max(int(row.split('|')[1]) for row in sizes['stdout'].splitlines())
    check(f'{major}:size',max_size<=1_000_000_000)
    check(f'{major}:leftovers',not any(json.loads((p/'leftovers.json').read_text()).values()))
    server=next(e['server'][0][0] for e in ev if 'server' in e)
    commands=readlines(p/'commands.jsonl')
    versions.append(dict(major=major,server=server,live_checks=len(live),sql_batches=len(commands),messages=len(seen),commits=sum(r['kind']=='Commit' for r in raw),max_db_bytes=max_size,split=split_summary))
leftovers={}
for run in ('start-20261106-1201',RUN):
    for major in (16,17,18):
        key=f'{run}-pg{major}';state={}
        for kind,args in [('containers',['ps','-aq']),('volumes',['volume','ls','-q']),('networks',['network','ls','-q'])]:
            cmd=['docker',*args,'--filter',f'label=skedaddle.test={key}']
            p=subprocess.run(cmd,capture_output=True,text=True,check=True)
            state[kind]=dict(command=cmd,ids=p.stdout.splitlines())
        check(f'{key}:final_inventory',not any(v['ids'] for v in state.values()))
        leftovers[key]=state
(O/'final-leftovers.json').write_text(json.dumps(leftovers,indent=2))
(O/'analysis.json').write_text(json.dumps(dict(run=RUN,passed=sum(v['passed'] for v in checks),failed=sum(not v['passed'] for v in checks),versions=versions,checks=checks),indent=2))
print(json.dumps(dict(versions=versions,checks=len(checks),failed=sum(not v['passed'] for v in checks)),indent=2))
sys.exit(int(any(not c['passed'] for c in checks)))
