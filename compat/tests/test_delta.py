import contextlib
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from compat_lib import delta
import harness


def report(checks=(), cases=(), **extra):
    return dict(checks=list(checks), differential={'cases':list(cases)}, **extra)


def check(outcome='PASS', **extra):
    return dict(client='driver', id='one', outcome=outcome, owner='T1', **extra)


def case(outcome='COMPATIBLE', **extra):
    return dict(area='sql', id='one', outcome=outcome, owner='T2', **extra)


class DeltaTests(unittest.TestCase):
    def test_regressions_and_fixes(self):
        for old, new in [('PASS','FAIL'), ('PASS','HARNESS_ERROR')]:
            text, regressed = delta.compare(report([check(old)]), report([check(new)]))
            self.assertTrue(regressed)
            self.assertIn('check:driver/one', text.split('## Fixed')[0])
        for new in ('UNSUPPORTED','DIVERGENT','HARNESS_ERROR'):
            self.assertTrue(delta.compare(report(cases=[case()]), report(cases=[case(new)]))[1])
        for old in ('UNSUPPORTED','DIVERGENT'):
            text, regressed = delta.compare(report(cases=[case(old)]), report(cases=[case()]))
            self.assertFalse(regressed)
            self.assertIn('corpus:sql/one', text.split('## Fixed')[1].split('## Other')[0])
        self.assertIn('FAIL -> PASS', delta.compare(report([check('FAIL')]), report([check()]))[0])

    def test_nested_error_and_owner_relabel(self):
        old = check('FAIL', giggles={'status':'OK','observation':{'nested':[{'status':'ERR','observation':{'sqlstate':'42601'}}]}})
        new = dict(old, owner='T3', giggles={'observation':{'errors':[{'status':'ERR','sqlstate':'0A000'}]}})
        text, regressed = delta.compare(report([old]), report([new]))
        self.assertFalse(regressed)
        self.assertIn('owner changed T1 -> T3', text)
        self.assertIn('42601 -> 0A000', text)
        self.assertIn('check:driver/one', text.split('## Other changes')[1])
        self.assertEqual(delta.first_error(case(first_divergence={'giggles':{'sqlstate':'42883'}})), '42883')
        self.assertEqual(delta.first_error(case('UNTESTED', reason='unavailable')), 'unavailable')

    def test_expected_error_data_and_first_failed_step(self):
        expected = {'status':'OK','data':{'sqlstate':'23505'}}
        self.assertEqual(delta.first_error(check(giggles={'status':'OK','observation':{'duplicate':expected}},reason='generic')), 'N/A')
        failed = {'status':'ERR','sqlstate':None,'reason':'missing_row'}
        row = check('FAIL', giggles={'status':'OK','observation':{'duplicate':expected,'point':failed,'later':{'status':'ERR','sqlstate':'42601'}}},reason='driver observations differ')
        self.assertEqual(delta.first_error(row), 'missing_row')
        self.assertEqual(delta.first_error(row), delta.first_error(json.loads(json.dumps(row))))
        self.assertEqual(delta.first_error(check('FAIL',giggles={'status':'ERR','observation':{'sqlstate':'42883'}})), '42883')

    def test_summary_metadata_added_removed_and_rates(self):
        a = report([check()], [case()], run_id='old', build={'commit':'abc'}, started_at='start', finished_at='end', clients={'gone':{}})
        b = report([check('FAIL'),dict(check(),client='new')], [case('UNSUPPORTED')], run_id='new')
        text, _ = delta.compare(a,b)
        self.assertIn('commit abc; run old; started start; finished end', text)
        self.assertIn('commit N/A; run new; started N/A; finished N/A', text)
        self.assertIn('driver: PASS 1 -> 0 (-1)', text)
        self.assertIn('gone: PASS 0 -> 0 (+0); pass rate N/A -> N/A (N/A)', text)
        self.assertIn('new: PASS 0 -> 1 (+1); pass rate N/A -> 100.0% (N/A)', text)
        self.assertIn('Differential pass rate: 100.0% -> 0.0% (-100.0 pp)', text)
        self.assertIn('check:new/one', text.split('## Added')[1].split('## Removed')[0])
        self.assertTrue(text.splitlines()[-1].startswith('Summary: driver PASS'))
        self.assertIn('gone PASS', text.splitlines()[-1])
        self.assertIn('new PASS', text.splitlines()[-1])
        self.assertIn('-100.0 pp', text.splitlines()[-1])
        removed, _ = delta.compare(b,a)
        self.assertIn('check:new/one', removed.split('## Removed')[1])

    def test_invalid_and_duplicates(self):
        for value in ([], {'checks':{}}, report([check(),check()]), report([check('BOGUS')]), {'differential':[]}):
            with self.assertRaises(ValueError):
                delta.records(value)
        with self.assertRaises(ValueError):
            json.loads('{"checks":[],"checks":[]}', object_pairs_hook=delta._unique)

    def test_cli_output_and_refusal(self):
        with tempfile.TemporaryDirectory(dir=os.environ['TEMP']) as directory:
            root = Path(directory)
            a, b, out = root/'old.json', root/'new.json', root/'nested'/'delta.md'
            a.write_text(json.dumps(report([check()])), encoding='utf-8')
            b.write_text(json.dumps(report([check('FAIL')])), encoding='utf-8')
            with contextlib.redirect_stdout(io.StringIO()) as stdout:
                self.assertEqual(harness.main(['delta',str(a),str(b),'--out',str(out)]), 3)
            self.assertEqual(out.read_text(encoding='utf-8'), stdout.getvalue())
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(harness.main(['delta',str(a),str(a)]), 0)
            with contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(delta.run(a,b,'K:/must-not-exist/delta.md'),2)
                self.assertEqual(delta.run(root/'missing',b),2)
                with patch('compat_lib.delta.util.output_root', side_effect=ValueError('redirected to K:')):
                    self.assertEqual(delta.run(a,b,out),2)

    def test_deterministic_and_no_duplicate_classification(self):
        a, b = report([check()]), report([dict(check('FAIL'),owner='T3')])
        text, _ = delta.compare(a,b)
        self.assertEqual(text.count('`check:driver/one`'), 1)
        self.assertEqual(text, delta.compare(a,b)[0])


if __name__ == '__main__':
    unittest.main()
