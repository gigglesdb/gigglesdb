"""Replay acknowledged HTTP history into expected row projections, compare to PG.
Uses only read-only SQL; deleted API entities must be absent, including cascades.
"""
import json
def verify(history, sql):
    expected={}; entries_by_feed={}; count=0
    def put(db,table,ident,fields): expected[(db,table,ident)]=fields
    def absent(db,table,ident): expected[(db,table,ident)]=None
    for line in history.read_text(encoding='utf8').splitlines():
        r=json.loads(line);count+=1
        assert r['sequence']==count and 200<=r['response_status']<300, r
        app,a,p,b,ids=r['app'],r['action'],r['request'],r['response'],r['ids']
        if app=='miniflux':
            db='skedaddle'
            if a=='create_category': put(db,'categories',b['id'],{'title':p['title']})
            elif a=='update_category': expected[(db,'categories',ids['category_id'])]['title']=p['title']
            elif a=='delete_category': absent(db,'categories',ids['category_id'])
            elif a=='create_feed': put(db,'feeds',b['feed_id'],{'category_id':p['category_id']})
            elif a=='list_entries':
                entries_by_feed[ids['feed_id']]=[e['id'] for e in b['entries']]
                for e in b['entries']: put(db,'entries',e['id'],{'status':e['status'],'starred':e['starred']})
            elif a=='read_entries':
                for ident in p['entry_ids']: expected[(db,'entries',ident)]['status']=p['status']
            elif a=='bookmark_entry':
                e=expected[(db,'entries',ids['entry_id'])];e['starred']=not e['starred']
            elif a=='delete_feed':
                absent(db,'feeds',ids['feed_id'])
                for ident in entries_by_feed[ids['feed_id']]: absent(db,'entries',ident)
            elif a=='create_user': put(db,'users',b['id'],{'username':p['username']})
            elif a=='update_user': expected[(db,'users',ids['user_id'])].update(p)
            elif a=='delete_user': absent(db,'users',ids['user_id'])
        elif app=='gitea':
            db='gitea'
            if a=='create_repo': put(db,'repository',b['id'],{'name':p['name']})
            elif a=='create_issue': put(db,'issue',b['id'],{'name':p['title'],'content':p['body']})
            elif a=='update_issue': expected[(db,'issue',ids['issue_id'])].update(name=p['title'],content=p['body'])
            elif a=='create_comment': put(db,'comment',b['id'],{'content':p['body']})
            elif a=='update_comment': expected[(db,'comment',ids['comment_id'])]['content']=p['body']
        elif app=='keycloak':
            db='keycloak'
            if a=='create_user':
                ident=r['location'].rsplit('/',1)[1]
                put(db,'user_entity',ident,{'username':p['username']})
            elif a=='update_user': expected[(db,'user_entity',ids['user_id'])]['first_name']=p['firstName']
            elif a=='delete_user': absent(db,'user_entity',ids['user_id'])
    checks=[]; actual={}
    for db,table in sorted({(db,table) for db,table,ident in expected}):
        literals=','.join("'"+str(ident).replace("'","''")+"'" for d,t,ident in expected if d==db and t==table)
        rows=json.loads(sql(db,f'SELECT coalesce(json_agg(t),\'[]\'::json) FROM (SELECT * FROM "{table}" WHERE id IN ({literals})) t;')['stdout'])
        actual[(db,table)]={str(row['id']):row for row in rows}
    for (db,table,ident),fields in expected.items():
        found=actual[(db,table)].get(str(ident));row=[] if found is None else [found]
        ok=(not row) if fields is None else len(row)==1 and all(row[0].get(k)==v for k,v in fields.items())
        checks.append({'database':db,'table':table,'id':ident,'expected':fields,'actual':None if not row else {k:row[0].get(k) for k in (fields or {})},'pass':ok})
    return {'history_lines':count,'row_projections':len(checks),'deletion_checks':sum(x['expected'] is None for x in checks),'passed':sum(x['pass'] for x in checks),'failed':sum(not x['pass'] for x in checks),'checks':checks}
