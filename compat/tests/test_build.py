import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest.mock import patch, Mock

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from compat_lib import gigglesdb

SHA = 'a'*40


from tests.task_storage import ModuleStorage

_STORAGE = ModuleStorage()
TASK = _STORAGE.root

def setUpModule():
    _STORAGE.start()

def tearDownModule():
    _STORAGE.finish()

class BuildTests(unittest.TestCase):
    def test_successful_build_preserves_old_source_and_removes_tmp(self):
        with tempfile.TemporaryDirectory(dir=TASK) as directory:
            root = Path(directory)
            target = root/'target'
            (target/'debug').mkdir(parents=True)
            (target/'debug'/('giggles-server.exe' if os.name == 'nt' else 'giggles-server')).write_bytes(b'executable')
            old_source = root/'builds'/SHA/'src'
            old_source.mkdir(parents=True)
            (old_source/'keep').write_text('old evidence')
            process = Mock()
            process.wait.return_value = 0
            wrapper = root/'sccache.exe'
            wrapper.write_bytes(b'mocked wrapper')
            with patch.object(gigglesdb, 'validate_build_worktree', return_value=TASK/('wt-' + root.name)), patch.object(gigglesdb, 'SCCACHE', wrapper), patch.object(gigglesdb, 'resolve_commit', return_value=SHA), patch.object(gigglesdb, '_prepare_worktree'), patch.object(gigglesdb.subprocess, 'Popen', return_value=process) as popen, patch.object(gigglesdb, 'command', return_value=dict(exit=0, stdout='version')):
                binary, meta = gigglesdb.prepare_build('repo', 'HEAD', root/'builds', target, {})
            cargo_env = popen.call_args.kwargs['env']
            self.assertEqual(cargo_env['SCCACHE_DIR'], 'J:/gigglesdb-work/sccache-cache')
            self.assertEqual(cargo_env['SCCACHE_CACHE_SIZE'], '100G')
            self.assertEqual(cargo_env['CARGO_INCREMENTAL'], '0')
            self.assertEqual(cargo_env['RUSTC_WRAPPER'], str(wrapper))
            self.assertEqual(cargo_env['CARGO_TARGET_DIR'], str(target.resolve()))
            self.assertEqual(cargo_env['TEMP'], str(binary.parent/'tmp'))
            self.assertTrue(binary.is_file())
            self.assertTrue((binary.parent/'build.json').is_file())
            self.assertTrue((binary.parent/'build.log').is_file())
            self.assertEqual((binary.parent/'src'/'keep').read_text(), 'old evidence')
            self.assertFalse((binary.parent/'tmp').exists())
            self.assertEqual(meta['build_scratch_cleanup']['scratch_removed'], ['tmp'])

    def test_failed_build_cleans_tmp_and_missing_wrapper_is_removed(self):
        with tempfile.TemporaryDirectory(dir=TASK) as directory:
            root = Path(directory)
            process = Mock()
            process.wait.return_value = 1
            absent = root/'missing-sccache.exe'
            with patch.object(gigglesdb, 'validate_build_worktree', return_value=TASK/('wt-' + root.name)), patch.object(gigglesdb, 'resolve_commit', return_value=SHA), patch.object(gigglesdb, '_prepare_worktree'), patch.object(gigglesdb, 'SCCACHE', absent), patch.object(gigglesdb.subprocess, 'Popen', return_value=process) as popen:
                with self.assertRaises(gigglesdb.BuildError) as caught:
                    gigglesdb.prepare_build('repo', 'HEAD', root/'builds', root/'target', dict(RUSTC_WRAPPER='inherited'))
            self.assertEqual(caught.exception.metadata['sccache'], 'unavailable')
            self.assertNotIn('RUSTC_WRAPPER', popen.call_args.kwargs['env'])
            self.assertFalse((root/'builds'/SHA/'tmp').exists())

    def test_interrupted_build_stops_process_and_cleans_tmp_and_lock(self):
        with tempfile.TemporaryDirectory(dir=TASK) as directory:
            root = Path(directory)
            process = Mock()
            process.wait.side_effect = KeyboardInterrupt
            with patch.object(gigglesdb, 'validate_build_worktree', return_value=TASK/('wt-' + root.name)), patch.object(gigglesdb, 'resolve_commit', return_value=SHA), patch.object(gigglesdb, '_prepare_worktree'), patch.object(gigglesdb, '_record_sccache'), patch.object(gigglesdb, '_stop_build') as stop, patch.object(gigglesdb.subprocess, 'Popen', return_value=process):
                with self.assertRaises(KeyboardInterrupt):
                    gigglesdb.prepare_build('repo', 'HEAD', root/'builds', root/'target', {})
            stop.assert_called_once()
            self.assertFalse((root/'builds'/SHA/'tmp').exists())
            self.assertFalse((root/'target'/'.gd-r0-build.lock').exists())

    def test_worktree_path_rejects_primary_and_outside(self):
        for value in ('K:/source', 'J:/gigglesdb-work/wt', str(Path(__file__).resolve().parents[2])):
            with self.assertRaises(ValueError):
                gigglesdb.validate_build_worktree(value)
        self.assertEqual(gigglesdb.validate_build_worktree(gigglesdb.BUILD_WORKTREE), gigglesdb.BUILD_WORKTREE.absolute())

    def test_registered_worktree_checkout_then_clean(self):
        with tempfile.TemporaryDirectory(dir=TASK) as directory:
            source = Path(directory).resolve()
            listing = dict(exit=0, stdout='worktree ' + str(source) + '\nHEAD ' + SHA + '\ndetached\n')
            with patch.object(gigglesdb, 'command', side_effect=[listing, dict(exit=0), dict(exit=0)]) as run:
                gigglesdb._prepare_worktree('repo', SHA, source, {})
            self.assertEqual(run.call_args_list[1].args[0][-4:], ['checkout', '--detach', '--force', SHA])
            self.assertEqual(run.call_args_list[1].args[0][1:3], ['-c', 'core.autocrlf=false'])
            self.assertEqual(run.call_args_list[2].args[0][-2:], ['clean', '-ffdx'])

    def test_unregistered_existing_worktree_refused(self):
        with tempfile.TemporaryDirectory(dir=TASK) as directory:
            with patch.object(gigglesdb, 'command', return_value=dict(exit=0, stdout='')) as run:
                with self.assertRaises(ValueError):
                    gigglesdb._prepare_worktree('repo', SHA, Path(directory), {})
                self.assertEqual(run.call_count, 1)

    def test_missing_worktree_added_detached(self):
        with tempfile.TemporaryDirectory(dir=TASK) as directory:
            source = Path(directory)/'new'
            with patch.object(gigglesdb, 'command', return_value=dict(exit=0, stdout='')) as run:
                gigglesdb._prepare_worktree('repo', SHA, source, {})
            self.assertEqual(run.call_args.args[0][-5:], ['worktree', 'add', '--detach', str(source), SHA])
            self.assertEqual(run.call_args.args[0][1:3], ['-c', 'core.autocrlf=false'])

    def test_sccache_counts_ignore_duplicate_advanced_counts(self):
        def result(hits, misses):
            return dict(exit=0, stdout=json.dumps(dict(stats=dict(
                cache_hits=dict(counts=dict(Rust=hits), adv_counts=dict(rust=hits)),
                cache_misses=dict(counts=dict(Rust=misses), adv_counts=dict(rust=misses))))))
        metadata = dict(sccache='available')
        with patch.object(gigglesdb, 'command', side_effect=[result(10, 3), result(14, 5)]):
            gigglesdb._record_sccache(metadata, {}, 'before')
            gigglesdb._record_sccache(metadata, {}, 'after')
        self.assertEqual(metadata['sccache'], dict(hits=4, misses=2))
        self.assertEqual(metadata['sccache_hits_delta'], 4)
        self.assertEqual(metadata['sccache_misses_delta'], 2)

    def test_resolve_ref_and_bad_ref(self):
        with patch.object(gigglesdb, 'command', return_value=dict(exit=0, stdout=SHA+'\n')) as command:
            self.assertEqual(gigglesdb.resolve_commit('repo', 'HEAD', {}), SHA)
            self.assertEqual(command.call_args.args[0][-1], 'HEAD^{commit}')
        with patch.object(gigglesdb, 'command', return_value=dict(exit=1, stdout='')):
            with self.assertRaises(gigglesdb.BuildError):
                gigglesdb.resolve_commit('repo', 'missing', {})

    def test_reject_k_outputs_before_commands(self):
        with patch.object(gigglesdb, 'command') as command:
            for build, target in [('K:/bad', TASK), (TASK, 'K:/bad')]:
                with self.assertRaises(ValueError):
                    gigglesdb.prepare_build('repo', 'HEAD', build, target, {})
            command.assert_not_called()

    def test_cache_hit_checks_existing_worktree_registration_without_mutation(self):
        with tempfile.TemporaryDirectory(dir=TASK) as directory:
            root = Path(directory)
            source = root/'source'
            source.mkdir()
            cached = dict(commit=SHA)
            with patch.object(gigglesdb, 'validate_build_worktree', return_value=source), patch.object(gigglesdb, 'resolve_commit', return_value=SHA), patch.object(gigglesdb, '_cached', return_value=cached), patch.object(gigglesdb.subprocess, 'Popen') as popen:
                with patch.object(gigglesdb, 'command', return_value=dict(exit=0, stdout='')) as run:
                    with self.assertRaises(ValueError):
                        gigglesdb.prepare_build('repo', 'HEAD', root/'build', root/'target', {})
                    self.assertEqual(run.call_args.args[0][-3:], ['worktree', 'list', '--porcelain'])
                    self.assertEqual(run.call_count, 1)
                with patch.object(gigglesdb, 'command', return_value=dict(exit=0, stdout='worktree ' + str(source) + '\n')) as run:
                    _, metadata = gigglesdb.prepare_build('repo', 'HEAD', root/'build', root/'target', {})
                    self.assertEqual(metadata['build_cache'], 'hit')
                    self.assertEqual(run.call_count, 1)
                    self.assertEqual(run.call_args.args[0][-3:], ['worktree', 'list', '--porcelain'])
                popen.assert_not_called()

    def test_cache_is_verified_and_hit_does_not_build(self):
        with tempfile.TemporaryDirectory(dir=TASK) as directory:
            root = Path(directory)
            build = root/SHA
            build.mkdir()
            binary = build/'giggles-server.exe'
            binary.write_bytes(b'executable')
            metadata = dict(commit=SHA, size=binary.stat().st_size,
                            sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
                            build_root_dir='stale copied root', build_identity='stale identity')
            (build/'build.json').write_text(json.dumps(metadata))
            with patch.object(gigglesdb, 'validate_build_worktree', return_value=TASK/('wt-' + root.name)), patch.object(gigglesdb, 'resolve_commit', return_value=SHA), patch.object(gigglesdb, '_verify_cached_worktree'), patch.object(gigglesdb.subprocess, 'Popen') as popen:
                result, meta = gigglesdb.prepare_build('repo', 'HEAD', root, root/'target', {})
                self.assertEqual(result, binary)
                self.assertEqual(meta['build_cache'], 'hit')
                self.assertEqual(meta['build_root_dir'], str(build))
                self.assertEqual(meta['build_identity'], 'built-from-commit')
                self.assertEqual(meta['resolved_commit'], SHA)
                self.assertEqual(meta['binary_sha256'], metadata['sha256'])
                popen.assert_not_called()
            binary.write_bytes(b'alteration')
            self.assertIsNone(gigglesdb._cached(build, SHA))

    def test_shared_worktree_lock_blocks_different_targets(self):
        with tempfile.TemporaryDirectory(dir=TASK) as directory:
            root = Path(directory)
            first, second = root/'target1', root/'target2'
            first.mkdir(); second.mkdir()
            filename = '.gd-r0-shared-worktree.lock'
            with gigglesdb._build_lock(first, {}), gigglesdb._build_lock(root, {}, filename=filename):
                with gigglesdb._build_lock(second, {}):
                    with self.assertRaises(gigglesdb.BuildError):
                        with gigglesdb._build_lock(root, {}, timeout=0, filename=filename):
                            self.fail('shared worktree lock must serialize distinct targets')
                self.assertTrue((root/filename).exists())
            self.assertFalse((root/filename).exists())

    def test_lock_releases_only_owned_token(self):
        with tempfile.TemporaryDirectory(dir=TASK) as directory:
            target = Path(directory)
            lock = target/'.gd-r0-build.lock'
            with gigglesdb._build_lock(target, {}):
                self.assertTrue(lock.exists())
            self.assertFalse(lock.exists())
            with gigglesdb._build_lock(target, {}):
                lock.write_text('another owner')
            self.assertEqual(lock.read_text(), 'another owner')

    def test_stale_lock_recovered_and_timeout_preserves_live_lock(self):
        with tempfile.TemporaryDirectory(dir=TASK) as directory:
            target = Path(directory)
            lock = target/'.gd-r0-build.lock'
            lock.write_text('old')
            old = time.time()-7300
            os.utime(lock, (old, old))
            meta = {}
            with gigglesdb._build_lock(target, meta):
                self.assertEqual(meta['stale_build_locks'], [str(lock)])
            lock.write_text('live')
            with self.assertRaises(gigglesdb.BuildError):
                with gigglesdb._build_lock(target, {}, timeout=0):
                    self.fail('acquired live lock')
            self.assertEqual(lock.read_text(), 'live')



if __name__ == '__main__':
    unittest.main()
