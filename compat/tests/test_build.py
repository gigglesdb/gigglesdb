import hashlib
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import tarfile
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
    def test_successful_build_removes_only_owned_source_and_tmp(self):
        with tempfile.TemporaryDirectory(dir=TASK) as directory:
            root = Path(directory)
            target = root/'target'
            (target/'debug').mkdir(parents=True)
            (target/'debug'/('giggles-server.exe' if os.name == 'nt' else 'giggles-server')).write_bytes(b'executable')
            process = Mock()
            process.wait.return_value = 0
            with patch.object(gigglesdb, 'resolve_commit', return_value=SHA), patch.object(gigglesdb, '_extract_archive'), patch.object(gigglesdb.subprocess, 'Popen', return_value=process), patch.object(gigglesdb, 'command', return_value=dict(exit=0, stdout='version')):
                binary, meta = gigglesdb.prepare_build('repo', 'HEAD', root/'builds', target, {})
            self.assertTrue(binary.is_file())
            self.assertTrue((binary.parent/'build.json').is_file())
            self.assertTrue((binary.parent/'build.log').is_file())
            self.assertFalse((binary.parent/'src').exists())
            self.assertFalse((binary.parent/'tmp').exists())
            self.assertEqual(meta['build_scratch_cleanup']['scratch_removed'], ['src','tmp'])

    def test_archive_drains_large_trailing_padding(self):
        with tempfile.TemporaryDirectory(dir=TASK) as directory:
            root = Path(directory)
            archive = root/'padded.tar'
            with tarfile.open(archive, 'w') as tar:
                info = tarfile.TarInfo('file.txt')
                info.size = 2
                tar.addfile(info, io.BytesIO(b'ok'))
            with archive.open('ab') as stream:
                stream.write(b'\0' * (1024*1024))
            source = root/'src'
            source.mkdir()
            popen = gigglesdb.subprocess.Popen
            def writer(args, **kwargs):
                return popen([sys.executable, '-B', '-c',
                    'import pathlib,sys; sys.stdout.buffer.write(pathlib.Path(sys.argv[1]).read_bytes())',
                    str(archive)], **kwargs)
            with (root/'archive.log').open('wb') as log, patch.object(gigglesdb.subprocess, 'Popen', side_effect=writer):
                gigglesdb._extract_archive('repo', SHA, source, os.environ.copy(), log)
            self.assertEqual((source/'file.txt').read_bytes(), b'ok')

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
            with patch.object(gigglesdb, 'resolve_commit', return_value=SHA), patch.object(gigglesdb.subprocess, 'Popen') as popen:
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

    def test_source_cleanup_requires_ownership(self):
        with tempfile.TemporaryDirectory(dir=TASK) as directory:
            build = Path(directory)
            source = build/'src'
            source.mkdir()
            (source/'user-file').write_text('keep')
            with self.assertRaises(ValueError):
                gigglesdb._reset_source(build)
            self.assertTrue((source/'user-file').exists())
            (build/'.gd-source-owner').write_text(str(build.resolve()))
            gigglesdb._reset_source(build)
            self.assertFalse((source/'user-file').exists())


if __name__ == '__main__':
    unittest.main()
