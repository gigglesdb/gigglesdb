import os,unittest
from prepared_wire import WireReference,message,i16,i32
class PreparedWireTests(unittest.TestCase):
 def test_frame(self):self.assertEqual(message(b'S'),b'S\x00\x00\x00\x04')
 @unittest.skipUnless(os.environ.get('DD68_REFERENCE_TESTS')=='1','live pinned reference opt-in')
 def test_actual_parse0_bind(self):
  with WireReference() as r:
   v,t=r.observe('SELECT abs($1) AS v',['-2.5']);self.assertEqual(v['parameterTypes'],[701]);self.assertEqual(v['rows'],[['2.5']]);self.assertEqual(v['resultTypes'][0]['oid'],701)
   self.assertTrue(any('send_hex' in x and x['send_hex'].startswith('50') for x in t))
   v,_=r.observe('SELECT CAST($1 AS bytea) AS v',['abc']);self.assertEqual(v['parameterTypes'],[17]);self.assertEqual(v['rows'],[['\\x616263']])
   v,_=r.observe('SELECT abs($1) AS v',[None]);self.assertEqual(v['rows'],[[None]])
   v,_=r.observe('SELECT CAST($1 AS integer) AS v',['bad']);self.assertEqual(v['sqlstate'],'22P02')
class PreparedReportingTests(unittest.TestCase):
 def test_fixture_pin(self):
  import json
  from conformance import ROOT,digest
  cases=json.loads((ROOT/'cases-prepared.json').read_text(encoding='utf-8'));pin=json.loads((ROOT/'prepared-reference.json').read_text(encoding='utf-8'))
  self.assertEqual(len(cases),169);self.assertEqual(len({c['id'] for c in cases}),169);self.assertEqual(pin['corpus_sha256'],digest(cases))
 def test_conversion_contract(self):
  from observe_prepared_parameters import dimensions
  c=dict(reference=dict(status='ok',parameterTypes=[17],resultTypes=[dict(oid=23)],rows=[['3']]),candidate=dict(status='error',description=dict(parameterTypes=['bytea'],resultTypes=['integer'])),core_params=['abc'])
  d=dimensions(c);self.assertIs(d['parameter_metadata'],True);self.assertIsInstance(d['success'],str)
 def test_exact_mismatch(self):
  from observe_prepared_parameters import dimensions
  c=dict(reference=dict(status='ok',parameterTypes=[23],resultTypes=[dict(oid=23)],rows=[['3']]),candidate=dict(status='ok',description=dict(parameterTypes=['integer'],resultTypes=['integer']),rows=[[4]]),core_params=[3])
  self.assertIs(dimensions(c)['exact_values'],False)
 def test_withheld_metadata_is_unobserved(self):
  from observe_prepared_parameters import dimensions
  c=dict(reference=dict(status='ok',parameterTypes=[17],resultTypes=[dict(oid=23)],rows=[['3']]),candidate=dict(status='withheld'),core_params=['abc'])
  d=dimensions(c)
  self.assertEqual(d['parameter_metadata'],'unobserved: bytea execution withheld')
  self.assertTrue(d['success'].startswith('withheld:'))
if __name__=='__main__':unittest.main()

