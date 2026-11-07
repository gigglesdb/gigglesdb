import copy
import json
import os
import unittest
from conformance import ROOT, IMAGE, Reference, digest, load_cases, validate_pin, validate_cases, validate_inventory, coverage
from observe_binary import fixtures

class BinaryTests(unittest.TestCase):
    def test_regeneration_integrity_and_inventory(self):
        inventory,generated=fixtures()
        cases=load_cases([ROOT/'cases-binary.json'])
        self.assertEqual([{k:v for k,v in c.items() if k!='expected'} for c in cases],generated)
        pin=json.loads((ROOT/'binary-reference.json').read_text())
        self.assertEqual(pin['corpus_sha256'],digest(cases))
        self.assertEqual(pin['inventory_sha256'],digest(inventory))
        self.assertEqual(pin['image'],IMAGE)
        self.assertEqual(pin['case_count'],len(cases))
        validate_cases(cases,validate_inventory(inventory))
        self.assertEqual(len({r for c in cases for r in c['routines']}),22)
        self.assertTrue(any(c['id'].endswith('nested-digest') for c in cases))
        bit_rows=[c['expected']['rows'][0][0] for c in cases if '-bit-order-' in c['id']]
        self.assertEqual(bit_rows,[0,1,1,1,1,0])
        ledger=coverage(inventory,cases,{c['id']:'PASS' for c in cases})
        self.assertEqual(len(ledger['routines']),3413)
        self.assertEqual(ledger['counts']['verified'],0)
        self.assertEqual(len(load_cases()),66)
        self.assertEqual(len(load_cases([ROOT/'cases-strings.json'])),250)
        self.assertEqual(len(load_cases([ROOT/'cases.json',ROOT/'cases-strings.json',ROOT/'cases-binary.json'])),316+len(cases))

    def test_exact_negatives_and_primitive_bindings(self):
        _,cases=fixtures()
        stored=load_cases([ROOT/'cases-binary.json'])
        for c in stored:
            if c['expected']['status']=='error':
                self.assertEqual(c['expected_core_error'],'Sql(Type)')
                self.assertIn('independent',c['core_error_rationale'])
            if c.get('params'):
                self.assertTrue(all(p['type']=='text' for p in c['params']))
                self.assertIn('CAST($1 AS bytea)',c['query'])
        bad=copy.deepcopy(stored)
        c=next(c for c in bad if c['expected']['status']=='error')
        del c['expected_core_error']
        with self.assertRaisesRegex(ValueError,'exact core error'):
            validate_cases(bad,validate_inventory(fixtures()[0]))

@unittest.skipUnless(os.environ.get('DD68_REFERENCE_TESTS')=='1','enable pinned oracle')
class BinaryOracleTests(unittest.TestCase):
    def test_all_binary_expectations_on_pinned_reference(self):
        with Reference() as ref:
            validate_pin(ref.inventory(),json.loads((ROOT/'reference.json').read_text()))
            for case in load_cases([ROOT/'cases-binary.json']):
                with self.subTest(id=case['id']):
                    self.assertEqual(ref.query(case),case['expected'])
