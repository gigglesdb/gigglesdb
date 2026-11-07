import json,os,struct,unittest
from conformance import ROOT,IMAGE,Reference,digest,load_cases,coverage,parameter_literal,oracle_query
from observe_numeric import fixtures
class NumericTests(unittest.TestCase):
    def test_regeneration(self):
        inv,generated=fixtures();stored=load_cases([ROOT/'cases-numeric.json'])
        self.assertEqual([{k:v for k,v in c.items() if k!='expected'} for c in stored],generated)
        pin=json.loads((ROOT/'numeric-reference.json').read_text())
        self.assertEqual(pin['corpus_sha256'],digest(stored));self.assertEqual(pin['inventory_sha256'],digest(inv));self.assertEqual(pin['image'],IMAGE)
        self.assertGreater(len(stored),300)
        self.assertEqual([len(load_cases([ROOT/p])) for p in ['cases.json','cases-strings.json','cases-binary.json']],[66,250,255])
        ledger=coverage(inv,stored,{c['id']:'PASS' for c in stored});self.assertEqual(len(ledger['routines']),3413);self.assertFalse(ledger['complete']);self.assertEqual(ledger['counts']['verified'],0)
    def test_lossless_parameter_validation(self):
        for t,v in [('numeric',1.2),('numeric',"1');drop table x;--"),('real','000000000'),('double precision','xyz')]:
            with self.assertRaises(ValueError):parameter_literal(dict(type=t,value=v))
        self.assertEqual(parameter_literal(dict(type='numeric',value='1.2300')), "CAST('1.2300' AS numeric)")
        self.assertEqual(parameter_literal(dict(type='double precision',value='8000000000000000')), "CAST('-0.0' AS double precision)")
        self.assertEqual(oracle_query(dict(query="SELECT '$1', $1 /* $2 */",params=[dict(type='numeric',value='1.2300')])),"SELECT '$1', CAST('1.2300' AS numeric) /* $2 */")
    def test_independent_exact_results(self):
        cases=load_cases([ROOT/'cases-numeric.json'])
        self.assertTrue(any(c['expected'].get('rows')==[[{'numeric_text':'1.2300'}]] for c in cases))
        self.assertTrue(any(c['expected'].get('rows')==[[{'float8_bits':'8000000000000000'}]] for c in cases))
        self.assertTrue(any(c['expected']['status']=='error' for c in cases))
        self.assertTrue(any(c['query']=='SELECT abs($1) AS v' for c in cases))
@unittest.skipUnless(os.environ.get('DD68_REFERENCE_TESTS')=='1','enable pinned oracle')
class NumericOracleTests(unittest.TestCase):
    def test_all_observed_again(self):
        with Reference() as ref:
            for case in load_cases([ROOT/'cases-numeric.json']):
                with self.subTest(id=case['id']):self.assertEqual(ref.query(case),case['expected'])
