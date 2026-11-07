"""Real HTTP workloads; append every HTTP acknowledgement before checking status.
Histories contain no credentials or bearer tokens. No filesystem deletion.
"""
import base64, collections, datetime, json, os, time, urllib.error, urllib.request
PASSWORD='s6-local-admin-password'
class Driver:
    def __init__(self, path):
        self.file=path.open('x',encoding='utf8'); self.n=0; self.counts=collections.Counter()
    def call(self, app, base, action, method, path, data=None, ids=None, token=None, form=False):
        headers={}
        if token: headers['Authorization']='Bearer '+token
        elif app in ('miniflux','gitea'): headers['Authorization']='Basic '+base64.b64encode(('admin:'+PASSWORD).encode()).decode()
        if data is not None:
            headers['Content-Type']='application/x-www-form-urlencoded' if form else 'application/json'
            payload=data.encode() if form else json.dumps(data).encode()
        else: payload=None
        started=time.monotonic()
        req=urllib.request.Request(base+path,data=payload,method=method,headers=headers)
        try:
            response=urllib.request.urlopen(req,timeout=45)
        except urllib.error.HTTPError as e: response=e
        except Exception as e:
            self.file.write(json.dumps({'sequence':self.n+1,'app':app,'action':action,'timestamp':datetime.datetime.now(datetime.timezone.utc).isoformat(),'transport_error':str(e)})+'\n'); self.file.flush();os.fsync(self.file.fileno());raise
        with response:
            raw=response.read().decode(); status=response.status
            try: body=json.loads(raw) if raw else None
            except ValueError: body=raw
            location=response.headers.get('Location')
        self.n+=1; self.counts[app+':'+action]+=1
        safe_data={k:v for k,v in data.items() if k not in ('password','credentials')} if isinstance(data,dict) else ('[redacted form]' if form else data)
        safe_body={k:v for k,v in body.items() if 'token' not in k} if action=='token' and isinstance(body,dict) else body
        entity_ids=dict(ids or {})
        if location: entity_ids['location_id']=location.rstrip('/').rsplit('/',1)[-1]
        if isinstance(body,dict):
            for key in ('id','feed_id','user_id','category_id','index'):
                if key in body: entity_ids[key]=body[key]
        record={'sequence':self.n,'app':app,'action':action,'timestamp':datetime.datetime.now(datetime.timezone.utc).isoformat(),'response_status':status,'method':method,'path':path,'ids':entity_ids,'request':safe_data,'response':safe_body,'location':location,'elapsed_ms':round((time.monotonic()-started)*1000,3)}
        self.file.write(json.dumps(record)+'\n');self.file.flush();os.fsync(self.file.fileno())
        if not 200<=status<300: raise RuntimeError(f'{app} {action}: HTTP {status}: {body}')
        return body, location
    def miniflux_cycle(self,base,i):
        def call(action,method,path,data=None,ids=None): return self.call('miniflux',base,action,method,path,data,ids)[0]
        category=call('create_category','POST','/v1/categories',{'title':f's6-category-{i}'})['id']
        call('update_category','PUT',f'/v1/categories/{category}',{'title':f's6-updated-{i}'},{'category_id':category})
        feed=call('create_feed','POST','/v1/feeds',{'feed_url':f'http://fixture:8081/feed.xml?run={i}','category_id':category})['feed_id']
        call('update_feed','PUT',f'/v1/feeds/{feed}',{'title':f's6-feed-{i}'},{'feed_id':feed})
        entries=call('list_entries','GET',f'/v1/feeds/{feed}/entries',ids={'feed_id':feed})['entries']
        entry_ids=[x['id'] for x in entries]
        if len(entry_ids)!=3: raise RuntimeError('Expected exactly three ingested fixture entries')
        call('read_entries','PUT','/v1/entries',{'entry_ids':entry_ids,'status':'read'},{'entry_ids':entry_ids,'feed_id':feed})
        call('bookmark_entry','PUT',f'/v1/entries/{entry_ids[0]}/bookmark',ids={'entry_id':entry_ids[0],'feed_id':feed})
        if i%2==0:
            call('delete_feed','DELETE',f'/v1/feeds/{feed}',ids={'feed_id':feed})
            call('delete_category','DELETE',f'/v1/categories/{category}',ids={'category_id':category})
        if i%5==0:
            user=call('create_user','POST','/v1/users',{'username':f's6-user-{i}','password':PASSWORD,'is_admin':False})['id']
            call('update_user','PUT',f'/v1/users/{user}',{'username':f's6-updated-user-{i}'},{'user_id':user})
            if i%10==0: call('delete_user','DELETE',f'/v1/users/{user}',ids={'user_id':user})
        if i%15==0: call('refresh_all','PUT','/v1/feeds/refresh')
    def gitea_workload(self,base):
        def call(action,method,path,data=None,ids=None): return self.call('gitea',base,action,method,path,data,ids)[0]
        call('create_repo','POST','/api/v1/user/repos',{'name':'s6-repo','private':False,'auto_init':False})
        for i in range(6):
            issue=call('create_issue','POST','/api/v1/repos/admin/s6-repo/issues',{'title':f's6-issue-{i}','body':'original'})
            index=issue['number']; ident=issue['id']
            call('update_issue','PATCH',f'/api/v1/repos/admin/s6-repo/issues/{index}',{'title':f's6-updated-issue-{i}','body':'updated'},{'issue_id':ident,'index':index})
            comment=call('create_comment','POST',f'/api/v1/repos/admin/s6-repo/issues/{index}/comments',{'body':f's6-comment-{i}'},{'issue_id':ident})
            call('update_comment','PATCH',f'/api/v1/repos/admin/s6-repo/issues/comments/{comment["id"]}',{'body':f's6-updated-comment-{i}'},{'comment_id':comment['id']})
        call('list_issues','GET','/api/v1/repos/admin/s6-repo/issues')
    def keycloak_workload(self,base):
        token=self.call('keycloak',base,'token','POST','/realms/master/protocol/openid-connect/token','client_id=admin-cli&username=admin&password='+PASSWORD+'&grant_type=password',form=True)[0]['access_token']
        def call(action,method,path,data=None,ids=None): return self.call('keycloak',base,action,method,path,data,ids,token=token)
        call('create_realm','POST','/admin/realms',{'realm':'s6','enabled':True})
        for i in range(6):
            body,location=call('create_user','POST','/admin/realms/s6/users',{'username':f's6-user-{i}','enabled':True})
            ident=location.rsplit('/',1)[1]
            call('update_user','PUT',f'/admin/realms/s6/users/{ident}',{'firstName':f's6-first-{i}'},{'user_id':ident})
            call('get_user','GET',f'/admin/realms/s6/users/{ident}',ids={'user_id':ident})
            if i%2==0: call('delete_user','DELETE',f'/admin/realms/s6/users/{ident}',ids={'user_id':ident})
        call('list_users','GET','/admin/realms/s6/users')
    def close(self):
        self.file.close()
        return {'acknowledged_actions':self.n,'counts':dict(self.counts)}
