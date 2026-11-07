import copy
import gzip
import json
import os
import unittest
from pathlib import Path
from conformance import ROOT, Reference, load_cases, digest, coverage, validate_cases, validate_inventory
from observe_strings import fixtures, NAMES

class SupplementalTests(unittest.TestCase):
    def test_default_preserves_seed_and_explicit_selection(self):
        seed=json.loads((ROOT/'cases.json').read_text(encoding='utf-8'))
        self.assertEqual(len(seed),66)
        self.assertEqual(digest(seed),'f460405206585757ac37482c28dc81a71409baa538415dfdb238dd5c686857a1')
        self.assertEqual(load_cases(),seed)
        supplemental=load_cases([ROOT/'cases-strings.json'])
        self.assertEqual(load_cases([ROOT/'cases.json',ROOT/'cases-strings.json']),seed+supplemental)
        with self.assertRaisesRegex(ValueError,'duplicate'):
            load_cases([ROOT/'cases.json',ROOT/'cases.json'])

    def test_signature_coverage_and_pin(self):
        inventory, generated=fixtures()
        cases=load_cases([ROOT/'cases-strings.json'])
        self.assertEqual([{k:v for k,v in c.items() if k!='expected'} for c in cases],generated)
        identities={r['identity'] for r in inventory['routines'] if r['identity'].split('(')[0].split('.')[-1].strip('"') in NAMES}
        self.assertTrue(identities <= {r for c in cases for r in c['routines']})
        self.assertEqual(digest(cases),json.loads((ROOT/'strings-reference.json').read_text())['corpus_sha256'])
        ledger=coverage(inventory,cases,{c['id']:'PASS' for c in cases})
        self.assertEqual(len(ledger['routines']),3413)
        self.assertEqual(ledger['counts']['verified'],0)
        with self.assertRaisesRegex(ValueError,'missing or extra'):
            coverage(inventory,cases,{})
        bad=copy.deepcopy(cases);bad[0]['routines']=['pg_catalog.fake(text)/f']
        with self.assertRaisesRegex(ValueError,'unknown routine'):
            validate_cases(bad,validate_inventory(inventory))

@unittest.skipUnless(os.environ.get('DD68_REFERENCE_TESTS')=='1','enable pinned oracle')
class SupplementalOracleTests(unittest.TestCase):
    def test_all_stored_expectations_observed_again(self):
        with Reference() as ref:
            for case in load_cases([ROOT/'cases-strings.json']):
                with self.subTest(id=case['id']):
                    self.assertEqual(ref.query(case),case['expected'])
