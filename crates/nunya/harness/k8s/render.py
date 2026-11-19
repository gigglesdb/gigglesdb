"""Render harness-only JSON overlays; Python stdlib, OpenSSL and kubectl."""
from pathlib import Path
import base64, hashlib, hmac, os, json
D=Path('J:/gigglesdb-work/nunya/gd88/live-8c5')
def verifier(password):
 salt=b'nunya-live-8c5';salted=hashlib.pbkdf2_hmac('sha256',password.encode(),salt,4096);b64=lambda b:base64.b64encode(b).decode()
 return 'SCRAM-SHA-256$4096:'+b64(salt)+'$'+b64(hashlib.sha256(hmac.digest(salted,b'Client Key','sha256')).digest())+':'+b64(hmac.digest(salted,b'Server Key','sha256'))
def policy(limit=100000):
 return f'''version = 2
default = "show"
[domains.email]
tokenize = "on"
tokenize_limit_per_session = {limit}
tokenize_limit_per_principal = {limit}
tokenize_principal_window = "1h"
[[column]]
table = "public.customers"
name = "email"
blind = "email"
'''
def json_documents(text):
 """Accept kubectl's JSON sequence for a multi-document YAML input."""
 decoder=json.JSONDecoder();items=[]
 while text.strip():
  item,end=decoder.raw_decode(text.lstrip());text=text.lstrip()[end:]
  items.extend(item['items'] if item.get('kind')=='List' else [item])
 return {'apiVersion':'v1','kind':'List','items':items}
def render(b):
 openssl='C:/Program Files/Git/usr/bin/openssl.exe';tmp=D/'tmp'
 b.command([openssl,'req','-x509','-newkey','rsa:2048','-nodes','-keyout',str(tmp/'ca.key'),'-out',str(D/'ca.crt'),'-days','2','-subj','/CN=nunya-8c5-ca'],'ca-cert')
 # The pod network is 10.244.0.0/16; include reachable per-node pod addresses.
 sans='DNS:localhost,DNS:postgres,IP:127.0.0.1,'+','.join(f'IP:10.244.{n}.{i}' for n in range(4) for i in range(1,255))
 (tmp/'ext.cnf').write_text('subjectAltName='+sans+'\nbasicConstraints=CA:FALSE\n',newline='\n')
 b.command([openssl,'req','-newkey','rsa:2048','-nodes','-keyout',str(tmp/'tls.key'),'-out',str(tmp/'tls.csr'),'-subj','/CN=nunya-8c5-leaf'],'leaf-request')
 b.command([openssl,'x509','-req','-in',str(tmp/'tls.csr'),'-CA',str(D/'ca.crt'),'-CAkey',str(tmp/'ca.key'),'-CAcreateserial','-out',str(tmp/'tls.crt'),'-days','2','-extfile',str(tmp/'ext.cnf')],'leaf-cert')
 def obj(kind,name,**kw):return dict(apiVersion='v1',kind=kind,metadata={'name':name,'namespace':'nunya'},**kw)
 secret=obj('Secret','nunya-credentials',stringData={'root.key':os.urandom(32).hex(),'password':'live-postgres-8a','tls.crt':(tmp/'tls.crt').read_text(),'tls.key':(tmp/'tls.key').read_text(),'upstream-ca.crt':(D/'ca.crt').read_text(),'admin-verifier':verifier('live-admin-8a')})
 peer=obj('Secret','nunya-peer',data={'secret':base64.b64encode(os.urandom(32)).decode()})
 b.kub(['apply','-f','-'],'apply-secrets',input=json.dumps({'apiVersion':'v1','kind':'List','items':[secret,peer]}))
 deployment=json_documents(b.kub(['create','--dry-run=client','-f',str(b.WT/'deploy/kubernetes/deployment.yaml'),'-o','json'],'render-deployment'))
 # Explicitly retain three replicas, matching the repository manifest default.
 cm,pvc,dep,svc=deployment['items'];cm['data']['policy.toml']=policy();dep['spec']['replicas']=3
 c=dep['spec']['template']['spec']['containers'][0];c['image']='nunya:gd88-8c5';c['imagePullPolicy']='Never'
 (D/'deployment.json').write_text(json.dumps(deployment,indent=2),newline='\n')
 pginit=obj('ConfigMap','postgres-init',data={'seed.sql':"CREATE TABLE customers(id int primary key, email text); INSERT INTO customers SELECT i,'synthetic@example.test' FROM generate_series(1,10000) i;"})
 pg={'apiVersion':'apps/v1','kind':'Deployment','metadata':{'name':'postgres','namespace':'nunya'},'spec':{'replicas':1,'selector':{'matchLabels':{'app':'postgres'}},'template':{'metadata':{'labels':{'app':'postgres'}},'spec':{'securityContext':{'fsGroup':70,'runAsUser':70,'runAsGroup':70,'runAsNonRoot':True,'seccompProfile':{'type':'RuntimeDefault'}},'containers':[{'name':'postgres','securityContext':{'allowPrivilegeEscalation':False,'capabilities':{'drop':['ALL']}},'image':'postgres:18-alpine','imagePullPolicy':'Never','args':['-c','ssl=on','-c','ssl_cert_file=/tls/tls.crt','-c','ssl_key_file=/tls/tls.key'],'env':[{'name':'POSTGRES_USER','value':'nunya'},{'name':'POSTGRES_DB','value':'app'},{'name':'POSTGRES_PASSWORD','value':'live-postgres-8a'},{'name':'PGDATA','value':'/var/lib/postgresql/data/pgdata'}],'volumeMounts':[{'name':'data','mountPath':'/var/lib/postgresql/data'},{'name':'init','mountPath':'/docker-entrypoint-initdb.d'},{'name':'tls','mountPath':'/tls','readOnly':True}]}],'volumes':[{'name':'data','emptyDir':{}},{'name':'init','configMap':{'name':'postgres-init'}},{'name':'tls','secret':{'secretName':'nunya-credentials','defaultMode':416}}]}}}}
 pgsvc=obj('Service','postgres',spec={'selector':{'app':'postgres'},'ports':[{'port':5432,'targetPort':5432}]})
 b.kub(['apply','-f','-'],'apply-postgres',input=json.dumps({'apiVersion':'v1','kind':'List','items':[pginit,pg,pgsvc]}))
