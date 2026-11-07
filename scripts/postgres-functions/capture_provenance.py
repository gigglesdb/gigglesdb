"""Owned build capture. Use: python capture_provenance.py build|correct-history.

Qualification is deliberately withheld for build scripts with unbounded external
inputs. Captured repo inventory is a conservative superset, never called compiler
input. Cargo metadata determines transitive package roots; dep-info determines
compiler membership. External package data is captured separately.
"""
import argparse,json,os,pathlib,shutil,subprocess,sys
from source_closure import *
from workdir import default_output,check_outside_repo
BASE=pathlib.Path(__file__).resolve().parents[2]
OUT=default_output('provenance')
def save(p,v):
 check_outside_repo(p);p.parent.mkdir(parents=True,exist_ok=True);p.write_text(json.dumps(v,indent=2),encoding='utf-8')
def command(args,cwd,stem):
 p=subprocess.run(args,cwd=cwd,capture_output=True,text=True,encoding='utf-8')
 save(OUT/(stem+'.process.json'),dict(command=args,cwd=str(cwd),native_exit=p.returncode))
 (OUT/(stem+'.stdout')).write_text(p.stdout,encoding='utf-8');(OUT/(stem+'.stderr')).write_text(p.stderr,encoding='utf-8')
 if p.returncode:raise RuntimeError(stem+' native exit '+str(p.returncode))
 return p.stdout

def correct():
 check_outside_repo(OUT);OUT.mkdir(parents=True,exist_ok=True);old=BASE/'target-dd68-prepared-oracle/latest-source-provenance.json'
 backup=OUT/'latest-source-provenance.e383-original.json'
 if not backup.exists():shutil.copyfile(old,backup)
 result=historical(old.parent,BASE)
 save(OUT/'historical-correction.json',result);save(OUT/'latest-source-provenance.json',result)
 print('Historical nonqualified; differences:',len(result['differences']))

def build():
 check_outside_repo(OUT);OUT.mkdir(parents=True,exist_ok=True);source=OUT/'source'
 if source.exists():raise RuntimeError('refuse reuse of owned source campaign')
 before=inventory(BASE/'crates');save(OUT/'live-before-copy.json',before)
 shutil.copytree(BASE/'crates',source/'crates',ignore=shutil.ignore_patterns('target*','.git','artifacts'))
 frozen=inventory(source);save(OUT/'frozen-before.json',frozen)
 records={}
 manifest=source/'crates/giggles-core/Cargo.toml'
 for name,args in [('cargo_metadata',['cargo','metadata','--locked','--format-version','1','--manifest-path',str(manifest)]),('rustc_version',['rustc','-vV']),('rustc_cfg',['rustc','--print','cfg']),('cargo_version',['cargo','-V'])]:
  records[name]=command(args,source,name)
 meta=json.loads(records['cargo_metadata'])
 external={};local=[]
 for package in meta['packages']:
  root=pathlib.Path(package['manifest_path']).parent.resolve()
  if package['source'] is None:
   root.relative_to(source.resolve());local.append((package,root))
  else:external[package['id']]=inventory(root)
 save(OUT/'external-package-inputs.json',external)
 records['build_command']=['cargo','build','--locked','--manifest-path',str(manifest),'--example','function_probe','--target-dir',str(OUT/'build'),'-j','1']
 command(records['build_command'],source,'build')
 records['build_script_records']={str(p.relative_to(OUT)):p.read_text(encoding='utf-8') for p in (OUT/'build/debug/build').glob('*/output')}
 save(OUT/'build-records.json',records)
 compiler={};problems=[]
 for package,root in local:
  name=package['name'].replace('-','_')
  files=list((OUT/'build/debug/deps').glob(name+'-*.d'))
  if package['name']=='giggles-core':files+=list((OUT/'build/debug/examples').glob('function_probe-*.d'))
  if not files:problems.append('no depinfo for '+package['name'])
  for dep in files:
   for member in depinfo_members(dep):
    p=pathlib.Path(member.replace('\\','/'))
    if not p.is_absolute():p=root/p
    try:rel=p.resolve().relative_to(source.resolve()).as_posix();compiler[key(rel)]=sha(p)
    except (OSError,ValueError):problems.append('unresolved local compiler member '+member)
 external_compiler={}
 for package in meta['packages']:
  if package['source'] is None:continue
  root=pathlib.Path(package['manifest_path']).parent.resolve()
  files=list((OUT/'build/debug/deps').glob(package['name'].replace('-','_')+'-*.d'))
  package_inputs={}
  for dep in files:
   for member in depinfo_members(dep):
    p=pathlib.Path(member.replace('\\','/'))
    if not p.is_absolute():p=root/p
    try:
     rel=p.resolve().relative_to(root).as_posix();package_inputs[key(rel)]=sha(p)
    except (ValueError,OSError):
     # Generated inputs outside package roots need their own frozen capture.
     problems.append('external compiler input outside captured package: '+member)
  if package_inputs:external_compiler[package['id']]=package_inputs
 save(OUT/'external-compiler-inputs.json',external_compiler)
 external_after={package['id']:inventory(pathlib.Path(package['manifest_path']).parent) for package in meta['packages'] if package['source'] is not None}
 if external_after!=external:problems.append('external package input drift during build')
 save(OUT/'external-package-inputs-after.json',external_after)
 after=inventory(source);save(OUT/'frozen-after.json',after);save(OUT/'compiler-inputs.json',compiler)
 probe=OUT/'build/debug/examples/function_probe.exe'
 receipt=verify_receipt(frozen,after,probe,0,compiler,records)
 # Build scripts can read arbitrary files/env/network not declared to Cargo.
 if records['build_script_records']:problems.append('build scripts present: external/unreported inputs not sandbox-proven')
 live=inventory(BASE/'crates');save(OUT/'live-after-build.json',live)
 live_report=compare(frozen,after,{'crates/'+k:v for k,v in live.items()})
 if not live_report['qualified']:problems.append('live source drift or input membership difference')
 receipt['problems']+=problems;receipt['qualified']=not receipt['problems'];receipt['latest_source_matches']=live_report['qualified'];receipt['full']=False
 receipt['live_comparison']=live_report
 save(OUT/'build-receipt.json',receipt)
 print(json.dumps(receipt,indent=2))
if __name__=='__main__':
 parser=argparse.ArgumentParser(description=__doc__)
 parser.add_argument('mode',choices=['build','correct-history'])
 parser.add_argument('--output',type=pathlib.Path,default=OUT)
 args=parser.parse_args();OUT=args.output;check_outside_repo(OUT)
 if args.mode=='correct-history':correct()
 else:build()
