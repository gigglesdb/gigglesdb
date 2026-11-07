import pathlib,tempfile,unittest
from source_closure import *
class ClosureTests(unittest.TestCase):
 def setUp(self): self.base={p:'a'*64 for p in MANDATORY}
 def test_windows(self):
  self.assertTrue(compare({p.replace('/','\\').upper():v for p,v in self.base.items()},self.base,self.base)['qualified'])
 def test_empty_e387(self): self.assertFalse(compare({}, {}, {})['qualified'])
 def test_membership_loss(self):
  b=dict(self.base);b.pop(MANDATORY[1]);self.assertFalse(compare(b,b,b)['qualified'])
 def test_union(self):
  for name in ['local/src/lib.rs','local/Cargo.toml','embedded/tzdata','fixtures/raw.bin']:
   with self.subTest(name=name):
    b=dict(self.base);b[name]='b'*64
    self.assertFalse(compare(self.base,self.base,b)['qualified'])
    self.assertFalse(compare(b,b,self.base)['qualified'])
    self.assertTrue(compare(b,b,b)['qualified'])
    c=dict(b);c[name]='c'*64;self.assertFalse(compare(b,b,c)['qualified'])
 def test_escape_alias(self):
  for name in ['../escape','C:\\escape','/escape','a/../escape']:
   with self.assertRaises(ValueError):key(name)
  with self.assertRaises(ValueError):normalize({'A':'a'*64,'a':'a'*64})
 def test_build_drift(self):
  with tempfile.TemporaryDirectory() as d:
   p=pathlib.Path(d)/'probe';p.write_bytes(b'executable')
   after=dict(self.base);after[MANDATORY[1]]='b'*64
   r=verify_receipt(self.base,after,p,0,self.base,{})
   self.assertFalse(r['qualified']);self.assertIn('source changed during build',r['problems'])
 def test_embedded_inventory(self):
  with tempfile.TemporaryDirectory() as d:
   p=pathlib.Path(d)/'zone';p.write_bytes(b'abc');self.assertEqual(inventory(d),{'zone':sha(p)})
 def test_depinfo_windows(self):
  with tempfile.TemporaryDirectory() as d:
   p=pathlib.Path(d)/'a.d';p.write_text('C:\\build\\a: src\\lib.rs\n\nsrc\\lib.rs:\n')
   self.assertEqual(depinfo_members(p),['src\\lib.rs'])
 def test_guard_replaced_executable_and_embedded_data(self):
  with tempfile.TemporaryDirectory() as d:
   root=pathlib.Path(d)/'source';root.mkdir();data=root/'tzdata';data.write_bytes(b'zone')
   probe=pathlib.Path(d)/'probe';probe.write_bytes(b'compiled')
   frozen=inventory(root);expected=sha(probe);guard_frozen(root,frozen,probe,expected)
   data.write_bytes(b'changed')
   with self.assertRaises(RuntimeError):guard_frozen(root,frozen,probe,expected)
   data.write_bytes(b'zone');probe.write_bytes(b'replaced')
   with self.assertRaises(RuntimeError):guard_frozen(root,frozen,probe,expected)
 def test_empty_compiler_and_bad_exit(self):
  with tempfile.TemporaryDirectory() as d:
   p=pathlib.Path(d)/'probe';p.write_bytes(b'compiled')
   records={k:{} for k in ['cargo_metadata','rustc_version','rustc_cfg','cargo_version','build_command','build_script_records']}
   self.assertFalse(verify_receipt(self.base,self.base,p,0,{},records)['qualified'])
   self.assertFalse(verify_receipt(self.base,self.base,p,9,self.base,records)['qualified'])
 def test_real_rustc_embedded_nonrust_dependency(self):
  import subprocess,shutil
  if not shutil.which('rustc'):self.skipTest('rustc unavailable')
  with tempfile.TemporaryDirectory() as d:
   root=pathlib.Path(d);(root/'zone.bin').write_bytes(b'tzdata');(root/'fixture.txt').write_text('fixture')
   (root/'main.rs').write_text('fn main(){println!("{} {}",include_bytes!("zone.bin").len(),include_str!("fixture.txt"));}')
   p=subprocess.run(['rustc','main.rs','--emit=link,dep-info'],cwd=root,capture_output=True,text=True)
   self.assertEqual(p.returncode,0,p.stderr)
   members=depinfo_members(root/'main.d')
   self.assertIn('zone.bin',members);self.assertIn('fixture.txt',members)
   before=inventory(root);(root/'zone.bin').write_bytes(b'changed zone')
   result=compare(before,inventory(root),inventory(root),required=())
   self.assertFalse(result['qualified']);self.assertIn('zone.bin',result['differences'])
if __name__=='__main__':unittest.main()
