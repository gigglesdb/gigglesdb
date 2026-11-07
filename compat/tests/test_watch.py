import contextlib
import datetime
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from compat_lib import watch
import harness
from tests.task_storage import ModuleStorage

_STORAGE = ModuleStorage()

def setUpModule():
    _STORAGE.start()

def tearDownModule():
    _STORAGE.finish()


class WatchTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=_STORAGE.root)
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.a, self.b = 'a'*40, 'b'*40
        self.old = self.write('old.json', self.report(self.a, 'PASS'))
        self.new = self.write('new.json', self.report(self.b, 'FAIL'))
        self.args = SimpleNamespace(state=self.root/'state.json', seed_report=self.old, seed_commit=self.a,
            min_interval_minutes=60, cargo_target=str(self.root/'target'), build_root=str(self.root/'builds'), out_root=str(self.root/'runs'))

    def report(self, commit, outcome):
        return dict(build={'commit':commit}, finished_at='2020-01-01T00:00:00Z', checks=[dict(client='psql', id='one', outcome=outcome)], differential={'cases':[]})

    def write(self, name, value):
        path = self.root/name
        path.write_text(json.dumps(value))
        return path

    def invoke(self, runner, current=None, paths='crates/giggles-core/src/lib.rs'):
        def git(argv, env):
            self.assertTrue(env['TEMP'].startswith(str(self.root)))
            return {'exit':0, 'stdout': current or self.b if 'rev-parse' in argv else paths if 'diff' in argv else 'commits', 'stderr':''}
        output = io.StringIO()
        with patch.object(watch.util, 'command', side_effect=git), contextlib.redirect_stdout(output), contextlib.redirect_stderr(io.StringIO()):
            code = watch.run(self.args, runner, self.root)
        self.console = output.getvalue()
        return code

    def test_seed_same_commit_and_scope_noop(self):
        for current, paths in [(self.a, ''), (self.b, 'compat/harness.py\ncrates/giggles-pg-conformance/x')]:
            with self.subTest(current=current):
                self.args.state.unlink(missing_ok=True)
                self.assertEqual(0, self.invoke(lambda _: self.fail('run'), current, paths))

    def test_interval_and_busy_noop(self):
        self.write('state.json', dict(last_run_commit=self.a, last_report=str(self.old), last_run_at=datetime.datetime.now(datetime.timezone.utc).isoformat()))
        self.assertEqual(0, self.invoke(lambda _: self.fail('run')))
        self.args.state.unlink()
        target = Path(self.args.cargo_target); target.mkdir(exist_ok=True)
        for name in ('.gd-r0-run.lock','.gd-r0-build.lock'):
            (target/name).touch()
            self.assertEqual(0, self.invoke(lambda _: self.fail('run')))
            (target/name).unlink()

    def test_regression_labels_and_atomic_baseline(self):
        self.args.build_worktree = 'J:/gigglesdb-work/wt/h5c-watch-test'
        for outcome, label in [('FAIL','reproduces-in-isolation'), ('PASS','state-dependent (passes alone)'), ('UNTESTED','isolation-inconclusive')]:
            self.args.state.unlink(missing_ok=True)
            isolated = self.write('isolated.json', self.report(self.b, outcome))
            calls = []
            def runner(argv):
                calls.append(argv)
                self.assertEqual(str(watch.gigglesdb.validate_build_worktree(self.args.build_worktree)), argv[argv.index('--build-worktree')+1])
                if len(calls) == 2:
                    self.assertEqual(self.b, json.loads(self.args.state.read_text())['last_run_commit'])
                    self.assertIn('--no-corpus', argv)
                return 0, self.new if len(calls) == 1 else isolated
            self.assertEqual(3, self.invoke(runner))
            self.assertIn(label, (self.root/'delta-aaaaaaa-bbbbbbb.md').read_text())
            self.assertEqual(2, len(calls))
            self.assertIn('REGRESSION aaaaaaa..bbbbbbb check:psql/one '+label, self.console)
            self.assertIn('commits', self.console)

    def test_corpus_isolated_case_and_infra_label(self):
        old = self.report(self.a, 'PASS')
        new = self.report(self.b, 'PASS')
        old['differential']['cases'] = [dict(area='sql', id='case', outcome='COMPATIBLE')]
        new['differential']['cases'] = [dict(area='sql', id='case', outcome='DIVERGENT')]
        self.write('old.json', old)
        self.write('new.json', new)
        calls = []
        def runner(argv):
            calls.append(argv)
            if len(calls) == 1:
                return 0, self.new
            self.assertEqual(['--clients','psql','--checks','__none__','--corpus','sql/case'], argv[-6:])
            return 2, None
        self.assertEqual(3, self.invoke(runner))
        self.assertIn('isolation-inconclusive', (self.root/'delta-aaaaaaa-bbbbbbb.md').read_text())

    def test_race_noop_and_infra_preserves_baseline(self):
        def busy(_):
            raise watch.LockBusy('busy')
        self.assertEqual(0, self.invoke(busy))
        self.assertEqual(2, self.invoke(lambda _: (2, self.new)))
        self.assertEqual(self.a, json.loads(self.args.state.read_text())['last_run_commit'])

    def test_invalid_state_and_interval(self):
        self.args.min_interval_minutes = float('nan')
        self.assertEqual(2, self.invoke(lambda _: self.fail('run')))
        self.args.min_interval_minutes = 60
        self.args.seed_commit = self.b
        self.assertEqual(2, self.invoke(lambda _: self.fail('run')))

    def test_invalid_build_worktree_rejected_before_runner(self):
        for path in ('K:/source', 'J:/gigglesdb-work/outside-wt', 'J:/gigglesdb-work/wt'):
            with self.subTest(path=path):
                self.args.build_worktree = path
                self.assertEqual(2, self.invoke(lambda _: self.fail('run'), self.a))

    def test_cli_build_worktree_defaults_and_override(self):
        for command in ('run', 'watch'):
            self.assertEqual('J:/gigglesdb-work/wt/r0-build', harness.parser().parse_args([command]).build_worktree)
            self.assertEqual('J:/gigglesdb-work/wt/custom', harness.parser().parse_args([command, '--build-worktree', 'J:/gigglesdb-work/wt/custom']).build_worktree)

    def test_run_validates_worktree_before_inner_run(self):
        with patch.object(harness, '_run') as inner, contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual((2, None), harness.run_once(['run', '--build-worktree', 'K:/source']))
            inner.assert_not_called()

    def test_lock_exclusion_and_ownership(self):
        with watch.RunLock(self.root):
            with self.assertRaises(watch.LockBusy):
                with watch.RunLock(self.root):
                    pass
        self.assertFalse((self.root/'.gd-r0-run.lock').exists())
        with watch.RunLock(self.root):
            (self.root/'.gd-r0-run.lock').write_text('other-owner')
        self.assertEqual('other-owner', (self.root/'.gd-r0-run.lock').read_text())

    def test_noop_removes_unique_child_temp(self):
        self.assertEqual(0, self.invoke(lambda _: self.fail('run'), self.a))
        self.assertEqual([], list(self.root.glob('.watch-tmp-*')))
        self.assertFalse((self.root/'tmp').exists())
        self.assertFalse((self.root/'target').exists())

    def test_build_guard_only_for_watcher(self):
        (self.root/'.gd-r0-build.lock').touch()
        argv = ['run','--server-bin','fake.exe','--cargo-target',str(self.root)]
        with patch.object(harness, '_run', return_value=0):
            self.assertEqual((0, None), harness.run_once(argv))
            with self.assertRaises(watch.LockBusy):
                harness.run_once(argv, fail_if_build_busy=True)
        self.assertFalse((self.root/'.gd-r0-run.lock').exists())

    def test_run_lock_applies_to_server_bin(self):
        argv = ['run','--server-bin','fake.exe','--cargo-target',str(self.root)]
        with watch.RunLock(self.root), patch.object(harness, '_run') as inner:
            with self.assertRaises(watch.LockBusy):
                harness.run_once(argv)
            inner.assert_not_called()


if __name__ == '__main__':
    unittest.main()
