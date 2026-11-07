"""S4 guard/up/export/down/check only; no cross-run sweep or host deletion."""
import pathlib, os, sys, importlib.util, traceback
sys.dont_write_bytecode=True
R=pathlib.Path(__file__).resolve().parent
MAJOR=int(sys.argv[1]); RUN=sys.argv[2]; OUT=pathlib.Path(sys.argv[3])
os.environ.update(SKEDADDLE_RUN_ID=RUN,SKEDADDLE_PG_MAJOR=str(MAJOR),SKEDADDLE_OWNER_PID=str(os.getpid()),PYTHONDONTWRITEBYTECODE='1')
spec=importlib.util.spec_from_file_location('s4',R.parent/'s4-harness'/'harness.py')
h=importlib.util.module_from_spec(spec); spec.loader.exec_module(h)
h.OUT=OUT
def compose(*args,**kw):
    return h.docker('compose','-f',str(h.ROOT/'compose.yaml'),'-f',str(R.parent/'s3-ddl'/'compose.override.yaml'),'-p','skedaddle-'+RUN,*args,env=h.environment(),**kw)
h.compose=compose
status=0; started=False
try:
    h.guard(); started=True; h.up()
    port=int(compose('port','pg','5432')['stdout'].strip().rsplit(':',1)[1])
    env=os.environ.copy(); env.update(S1_PG_PORT=str(port),S1_OUT=str(OUT),S3_MAJOR=str(MAJOR))
    result=h.command([str(R.parents[2]/'target/s3/debug/s3b-ddl-start.exe')],env=env,timeout=220,check=False)
    h.save('probe-output.json',result); print(result['stdout'],flush=True); print(result['stderr'],flush=True)
    status=int(result['exit']!=0)
    h.save('database-sizes.json',h.sql('SELECT datname,pg_database_size(oid) FROM pg_database ORDER BY datname;'))
except BaseException:
    status=1; traceback.print_exc()
finally:
    if started:
        for action in (h.export,h.down):
            try: action()
            except BaseException: status=1; traceback.print_exc()
        try: h.check(RUN); h.save('leftovers.json',h.resources(RUN))
        except BaseException: status=1; traceback.print_exc()
sys.exit(status)
