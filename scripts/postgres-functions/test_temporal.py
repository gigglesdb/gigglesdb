import json,unittest
from conformance import ROOT,IMAGE,digest,load_cases,coverage
from observe_temporal import fixtures
class TemporalTests(unittest.TestCase):
    def test_integrity(self):
        inv,generated=fixtures();stored=load_cases([ROOT/'cases-temporal.json']);pin=json.loads((ROOT/'temporal-reference.json').read_text())
        self.assertEqual([{k:v for k,v in c.items() if k!='expected'} for c in stored],generated)
        self.assertEqual(pin['corpus_sha256'],digest(stored));self.assertEqual(pin['inventory_sha256'],digest(inv));self.assertEqual(pin['image'],IMAGE);self.assertIn('2025b',pin['tzdata'])
        self.assertGreater(len(stored),400);self.assertEqual(len(set(c['query'] for c in stored)),len(stored))
        self.assertTrue(all('params' not in c and c['expectedUnsupported'] and c['setup']==[] and c['reference_setup'] for c in stored))
        self.assertTrue(any(c['expected']['status']=='error' for c in stored))
        self.assertTrue(any(c.get('special_form')=='extract' for c in stored))
        self.assertGreater(len(pin['binary']),100)
        for b in pin['binary']:
            self.assertEqual(b['result']['status'],'ok')
            v=b['result']['rows'][0][0]
            if v is not None:self.assertEqual(bytes.fromhex(v).hex(),v)
        ledger=coverage(inv,stored,{c['id']:'FAIL' for c in stored});self.assertEqual(len(ledger['routines']),3413);self.assertEqual(ledger['counts']['verified'],0);self.assertFalse(ledger['complete'])
        self.assertEqual([len(load_cases([ROOT/f])) for f in ['cases.json','cases-strings.json','cases-binary.json','cases-numeric.json']],[66,250,255,783])
if __name__=='__main__':unittest.main()
