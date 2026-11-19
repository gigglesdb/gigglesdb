import socket,ssl,struct,hashlib,hmac,base64,os
class WireError(Exception):
 def __init__(self,code):self.sqlstate=code;super().__init__('pgwire '+str(code))
class Portal:
 def __init__(self,port,sid):
  s=socket.create_connection(('127.0.0.1',port),timeout=5);s.sendall(struct.pack('!II',8,80877103));self.s=s
  first=s.recv(1)
  if first==b'E':
   length=struct.unpack('!I',s.recv(4))[0];v=b''
   while len(v)<length-4:v+=s.recv(length-4-len(v))
   s.close();self.error(v)
  if first!=b'S':s.close();raise WireError('TLS_UNAVAILABLE')
  self.s=ssl._create_unverified_context().wrap_socket(s,server_hostname='localhost');self.s.settimeout(5)
  payload=struct.pack('!I',196608)+b'user\0session:'+sid.encode()+b'\0database\0app\0\0';self.s.sendall(struct.pack('!I',len(payload)+4)+payload)
  nonce=base64.b64encode(os.urandom(18)).decode();bare='n=user,r='+nonce
  while True:
   k,v=self.recv()
   if k==b'E':self.error(v)
   if k==b'R':
    code=struct.unpack('!I',v[:4])[0]
    if code==10:
     first=('n,,'+bare).encode();self.send(b'p',b'SCRAM-SHA-256\0'+struct.pack('!I',len(first))+first)
    elif code==11:
     server=v[4:].decode();parts=dict(x.split('=',1) for x in server.split(','));assert parts['r'].startswith(nonce)
     salted=hashlib.pbkdf2_hmac('sha256',b'live-session-8a',base64.b64decode(parts['s']),int(parts['i']));clientkey=hmac.digest(salted,b'Client Key','sha256');final='c=biws,r='+parts['r'];auth=(bare+','+server+','+final).encode();sig=hmac.digest(hashlib.sha256(clientkey).digest(),auth,'sha256');proof=bytes(a^b for a,b in zip(clientkey,sig));self.send(b'p',(final+',p='+base64.b64encode(proof).decode()).encode())
   if k==b'Z':break
  self.send(b'Q',b'BEGIN\0');self.drain()
  self.send(b'P',b'q\0SELECT id,email FROM customers ORDER BY id\0'+struct.pack('!H',0));self.send(b'B',b'p\0q\0'+struct.pack('!HHH',0,0,0));assert self.fetchmany(1)>0
 def send(self,k,v):self.s.sendall(k+struct.pack('!I',len(v)+4)+v)
 def recv(self):
  def read(n):
   b=b''
   while len(b)<n:
    x=self.s.recv(n-len(b))
    if not x:raise WireError('EOF')
    b+=x
   return b
  k=read(1);return k,read(struct.unpack('!I',read(4))[0]-4)
 def error(self,v):raise WireError(next((x[1:].decode() for x in v.split(b'\0') if x.startswith(b'C')),None))
 def drain(self):
  count=0;error=None
  while True:
   k,v=self.recv()
   if k==b'D':count+=1
   if k==b'E':self.error(v)
   if k==b'Z':
    if error is not None:self.error(error)
    return count
 def fetchmany(self,n):self.send(b'E',b'p\0'+struct.pack('!I',n));self.send(b'S',b'');return self.drain()
 def close(self):self.s.close()
