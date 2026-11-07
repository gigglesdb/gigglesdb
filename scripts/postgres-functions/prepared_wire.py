"""Actual Parse0/Describe/Bind protocol; Docker exec is only a duplex byte transport."""
import struct, subprocess, time, threading, sys
from conformance import Reference, IMAGE, run, require

def i16(n): return struct.pack('!h', n)
def i32(n): return struct.pack('!i', n)
def message(tag, payload=b''): return tag + i32(len(payload)+4) + payload
def cstring(s): return s.encode()+b'\0'

# A container deadline survives losing the Docker client. Only this shell's cat
# jobs are signalled, and EXIT always reaps them before their parent exits.
_TRANSPORT = r"""exec timeout --foreground --signal=TERM --kill-after=2s 25s bash -c '
cleanup() {
    trap - EXIT HUP INT TERM
    exec 3<&- 3>&- 4<&-
    kill -KILL $(jobs -pr) 2>/dev/null || :
    wait || :
}
trap cleanup EXIT
trap "exit 124" HUP INT TERM
exec 4<&0
exec 3<>/dev/tcp/127.0.0.1/5432 || exit
cat <&3 &
cat <&4 >&3 &
wait -n
'"""
_OBSERVE_TIMEOUT = 30.0
_SHUTDOWN_TIMEOUT = 3.0


class TransportCleanupError(RuntimeError):
    """Failed oracle transport; keep any observed SQL result as evidence only."""
    def __init__(self, detail, observation, trace):
        super().__init__('wire transport cleanup failed: '+detail)
        self.observation = observation
        self.trace = trace


def _finish_transport(p):
    """Bound cleanup and report its failures without replacing a primary error."""
    failures = []
    try:
        if p.poll() is None:
            try:
                p.stdin.write(message(b'X'))
                p.stdin.flush()
            except (OSError, ValueError) as e:
                failures.append(str(e))
        try:
            p.stdin.close()
        except (OSError, ValueError) as e:
            failures.append(str(e))
        try:
            p.wait(timeout=_SHUTDOWN_TIMEOUT)
        except subprocess.TimeoutExpired:
            failures.append('transport shutdown timed out')
            p.kill()  # The owned Docker transport only; never a database PID.
            p.wait(timeout=_SHUTDOWN_TIMEOUT)
        if p.returncode:
            failures.append('transport exited with status '+str(p.returncode))
    except (OSError, ValueError, subprocess.TimeoutExpired) as e:
        failures.append(str(e))
    finally:
        for stream in (p.stdin, p.stdout, p.stderr):
            try:
                stream.close()
            except (OSError, ValueError) as e:
                failures.append(str(e))
    return failures

class WireReference(Reference):
    def __enter__(self):
        self.container=require(run(['docker','run','-d','--rm','--network','none','--tmpfs','/var/lib/postgresql','-e','POSTGRES_HOST_AUTH_METHOD=trust','-e','POSTGRES_INITDB_ARGS=--encoding=UTF8 --locale=C',IMAGE,'-c','listen_addresses=127.0.0.1','-c','timezone=UTC','-c','statement_timeout=10000'],timeout=120))
        try:
            for _ in range(300):
                if run(['docker','exec',self.container,'cat','/proc/1/comm']).stdout.strip()=='postgres' and run(['docker','exec',self.container,'pg_isready','-U','postgres']).returncode==0:return self
                time.sleep(.2)
            raise RuntimeError('reference startup timeout')
        except BaseException:
            self.__exit__();raise
    def observe(self, query, params, setup=()):
        p=subprocess.Popen(['docker','exec','-i',self.container,'bash','-c',_TRANSPORT],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        trace=[]
        observation=None
        expired=threading.Event()
        def expire():
            expired.set()
            try:p.kill()
            except OSError:pass
        deadline=threading.Timer(_OBSERVE_TIMEOUT, expire)
        deadline.daemon=True
        deadline.start()
        def send(b):p.stdin.write(b);p.stdin.flush();trace.append({'send_hex':b.hex()})
        def read(n):
            b=b''
            while len(b)<n:
                v=p.stdout.read(n-len(b))
                if not v:
                    if expired.is_set():raise TimeoutError('wire transport deadline exceeded')
                    raise RuntimeError('wire EOF')
                b+=v
            return b
        def cycle():
            out=[]
            while True:
                tag=read(1);length=struct.unpack('!i',read(4))[0]
                if not 4<=length<=16777216:raise RuntimeError('invalid length')
                b=read(length-4);trace.append({'receive':tag.decode(),'payload_hex':b.hex()});out.append((tag,b))
                if tag==b'Z':return out
        def error(messages):
            for t,b in messages:
                if t==b'E':
                    fields={v[:1].decode():v[1:].decode() for v in b.split(b'\0') if v}
                    return {'status':'error','sqlstate':fields.get('C'),'message':fields.get('M')}
        result={'parameterTypes':None,'resultTypes':None}
        try:
            b=i32(196608)+cstring('user')+cstring('postgres')+cstring('database')+cstring('postgres')+b'\0';send(i32(len(b)+4)+b);require_auth=cycle()
            if error(require_auth):raise RuntimeError(str(error(require_auth)))
            for sql in setup:
                send(message(b'Q',cstring(sql)));e=error(cycle())
                if e:
                    observation=dict(e,status='setup_error');return observation,trace
            send(message(b'P',b'probe\0'+cstring(query)+i16(0))+message(b'D',b'Sprobe\0')+message(b'S'))
            messages=cycle();e=error(messages)
            if e:
                observation=dict(result,**e);return observation,trace
            for t,b in messages:
                if t==b't':result['parameterTypes']=list(struct.unpack('!'+str(struct.unpack('!h',b[:2])[0])+'I',b[2:]))
                if t==b'T':
                    fields=[];off=2
                    for _ in range(struct.unpack('!h',b[:2])[0]):
                        end=b.index(0,off);name=b[off:end].decode();off=end+1
                        table,attr,oid,size,mod,fmt=struct.unpack('!IhIhih',b[off:off+18]);off+=18;fields.append({'name':name,'oid':oid,'format':fmt})
                    result['resultTypes']=fields
            payload=b'\0probe\0'+i16(0)+i16(len(params))
            for v in params:
                if v is None:payload+=i32(-1)
                else:b=v.encode();payload+=i32(len(b))+b
            send(message(b'B',payload+i16(0))+message(b'E',b'\0'+i32(0))+message(b'S'))
            messages=cycle();e=error(messages)
            if e:
                observation=dict(result,**e);return observation,trace
            rows=[]
            for t,b in messages:
                if t==b'D':
                    row=[];off=2
                    for _ in range(struct.unpack('!h',b[:2])[0]):
                        n=struct.unpack('!i',b[off:off+4])[0];off+=4;row.append(None if n==-1 else b[off:off+n].decode());off+=max(0,n)
                    rows.append(row)
            observation=dict(result,status='ok',rows=rows,format='PG text exact UTF8; numeric retained as text');return observation,trace
        finally:
            primary=sys.exc_info()[1]
            failures=_finish_transport(p)
            deadline.cancel()
            if expired.is_set():failures.append('wire transport deadline exceeded')
            if failures:
                detail='; '.join(failures)
                trace.append({'transport_cleanup_error':detail})
                if primary is not None:
                    add_note=getattr(primary,'add_note',None)
                    if callable(add_note):add_note(detail)
                else:raise TransportCleanupError(detail, observation, trace)
