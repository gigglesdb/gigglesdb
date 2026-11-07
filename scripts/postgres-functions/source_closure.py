"""Fail-closed source evidence. Inventory is conservative; compiler inputs are separate.

No report qualifies a historical binary without a sealed before/after build receipt.
All copied files (including non-Rust embedded data) are checked, never extension filtered.
"""
import hashlib, json, pathlib, re, os

MANDATORY = ('crates/giggles-core/src/lib.rs',
 'crates/giggles-core/src/relational/query.rs',
 'crates/giggles-core/src/relational/query/functions.rs',
 'crates/giggles-pg-types/src/lib.rs', 'crates/giggles-model/src/lib.rs')

def sha(path):
 return hashlib.sha256(pathlib.Path(path).read_bytes()).hexdigest()

def key(raw):
 value=str(raw).replace('\\','/')
 if value.startswith('/') or re.match(r'^[a-zA-Z]:',value) or any(p in ('','..','.') for p in value.split('/')):
  raise ValueError('unsafe relative input: '+value)
 return value.casefold()

def normalize(mapping):
 result={}
 for raw,digest in mapping.items():
  k=key(raw)
  if k in result: raise ValueError('aliased input: '+raw)
  if not re.fullmatch('[0-9a-f]{64}',digest): raise ValueError('invalid digest: '+raw)
  result[k]=digest
 return result

def inventory(root):
 root=pathlib.Path(root).resolve(); result={}
 for directory, dirs, files in os.walk(root):
  dirs[:]=[d for d in dirs if not d.startswith('target') and d not in {'.git','artifacts'}]
  for name in dirs+files:
   if (pathlib.Path(directory)/name).is_symlink(): raise ValueError('symlink in source copy')
  for name in files:
   p=pathlib.Path(directory)/name
   p.resolve().relative_to(root)
   k=key(p.relative_to(root).as_posix())
   if k in result: raise ValueError('case alias: '+str(p))
   result[k]=sha(p)
 return result

def compare(captured, copied, live, required=MANDATORY):
 captured,copied,live=map(normalize,(captured,copied,live))
 problems=[]
 for label,values in [('captured',captured),('copy',copied),('live',live)]:
  if not values: problems.append(label+': empty closure')
  for member in required:
   if key(member) not in values: problems.append(label+': missing mandatory '+member)
 differences={k:dict(captured=captured.get(k),copy=copied.get(k),live=live.get(k))
  for k in sorted(captured.keys()|copied.keys()|live.keys())
  if not captured.get(k)==copied.get(k)==live.get(k)}
 return dict(qualified=not problems and not differences,problems=problems,differences=differences,
             captured_count=len(captured),copy_count=len(copied),live_count=len(live))

def depinfo_members(path):
 # rustc emits a phony rule per input. Drive colons are not rule separators.
 text=pathlib.Path(path).read_text(encoding='utf-8').replace('\\\n','')
 members=[]
 for line in text.splitlines():
  if line.endswith(':') and not line.startswith('#'):
   members.append(line[:-1].replace('\\ ', ' '))
 if not members: raise ValueError('no input rules in depinfo: '+str(path))
 return members

def verify_receipt(before, after, probe, native_exit, compiler_inputs, records):
 result=compare(before,after,after)
 errors=result['problems']
 if result['differences']: errors.append('source changed during build')
 compiler=normalize(compiler_inputs)
 for p in MANDATORY:
  if key(p) not in compiler: errors.append('mandatory compiler input missing: '+p)
 for p,d in compiler.items():
  if normalize(after).get(p)!=d: errors.append('compiler input absent/different: '+p)
 if native_exit!=0: errors.append('build native exit is not zero')
 for r in ('cargo_metadata','rustc_version','rustc_cfg','cargo_version','build_command','build_script_records'):
  if r not in records or records[r] is None: errors.append('missing build record: '+r)
 for r in ('cargo_metadata','rustc_version','rustc_cfg','cargo_version','build_command'):
  if not records.get(r): errors.append('empty build record: '+r)
 try:
  metadata=json.loads(records.get('cargo_metadata',''))
  if not metadata.get('packages') or not metadata.get('resolve'): errors.append('empty Cargo dependency graph')
 except (ValueError,TypeError): errors.append('invalid Cargo metadata')
 if not pathlib.Path(probe).is_file(): errors.append('missing built executable')
 return dict(qualified=not errors,problems=errors,probe_sha256=sha(probe) if pathlib.Path(probe).is_file() else None,
             source_differences=result['differences'],native_exit=native_exit)

def historical(campaign, live_root):
 campaign=pathlib.Path(campaign); source=campaign/'source'
 captured=json.loads((campaign/'actual-build-inputs.json').read_text(encoding='utf-8'))
 copied=inventory(source)
 # All retained paths are checked. Also detect newly added files within every copied crate.
 live={}
 for crate in (source/'crates').iterdir():
  if crate.is_dir():
   for k,v in inventory(pathlib.Path(live_root)/'crates'/crate.name).items():
    live['crates/'+crate.name+'/'+k]=v
 result=compare(captured,copied,live)
 compiler={}; errors=[]
 for name,folder in [('giggles_core','giggles-core'),('giggles_model','giggles-model'),('giggles_pg_types','giggles-pg-types')]:
  files=list((campaign/'build/debug/deps').glob(name+'-*.d'))
  if len(files)!=1: errors.append('ambiguous/missing depinfo: '+name);continue
  for member in depinfo_members(files[0]):
   p=source/'crates'/folder/member.replace('\\','/')
   try:
    rel=p.resolve().relative_to(source.resolve()).as_posix();compiler[key(rel)]=sha(p)
   except (ValueError,OSError): errors.append('unresolved compiler input: '+member)
 for member in MANDATORY:
  if key(member) not in compiler: errors.append('mandatory compiler input missing: '+member)
 probe=campaign/'build/debug/examples/function_probe.exe'
 result.update(compiler_source_copy=compiler,latest_source_matches=result['qualified'],
  qualified=False,full=False,probe_sha256=sha(probe),historical_receipt_verified=False,
  nonqualification_reasons=errors+['No sealed pre/post full input capture and build receipt exists for historical binary'],
  supersedes='E383 latest-source claim; empty closure cannot qualify',
  dimensions={'root_metadata':'19/22','parameter_metadata':'144/147','prepared_fixture_count':169})
 return result


def guard_frozen(source, captured, probe, expected_probe_sha256):
 if not captured or inventory(source)!=normalize(captured):
  raise RuntimeError('frozen source drift or empty capture')
 if sha(probe)!=expected_probe_sha256:
  raise RuntimeError('built executable drift')
