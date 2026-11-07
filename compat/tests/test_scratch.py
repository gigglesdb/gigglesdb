import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from compat_lib import scratch
from tests.task_storage import ModuleStorage

_STORAGE = ModuleStorage()
def setUpModule():
    _STORAGE.start()
def tearDownModule():
    _STORAGE.finish()


class ScratchTests(unittest.TestCase):
    def test_keep_scratch_cli_is_independent_of_keep(self):
        import harness
        args = harness.parser().parse_args(['run', '--keep-scratch'])
        self.assertTrue(args.keep_scratch)
        self.assertFalse(args.keep)

    def test_named_children_only_and_evidence_kept(self):
        with tempfile.TemporaryDirectory(dir=_STORAGE.root) as directory:
            root = Path(directory)
            for name in scratch.RUN_CHILDREN:
                (root/name).mkdir()
                (root/name/'file').write_bytes(b'abc')
            evidence = ('report.json', 'report.md', 'server.stdout.log',
                        'server.stderr.log', 'checks/client/case/result.json',
                        'corpus/area/case/result.json')
            for name in evidence:
                path = root/name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(b'evidence')
            result = scratch.remove_children(root)
            self.assertEqual(result['scratch_removed'], list(scratch.RUN_CHILDREN))
            self.assertEqual(result['scratch_bytes_freed'], 15)
            for name in evidence:
                self.assertEqual((root/name).read_bytes(), b'evidence')

    def test_keep_and_invalid_names(self):
        with tempfile.TemporaryDirectory(dir=_STORAGE.root) as directory:
            root = Path(directory)
            (root/'data').mkdir()
            self.assertEqual(scratch.remove_children(root, keep=True)['scratch_removed'], [])
            self.assertTrue((root/'data').exists())
            for names in (('../outside',), ('checks',), ('src',), ('data','data')):
                with self.assertRaises(ValueError):
                    scratch.remove_children(root, names)

    def test_redirects_preflight_before_any_deletion(self):
        with tempfile.TemporaryDirectory(dir=_STORAGE.root) as directory:
            root = Path(directory)
            (root/'bin').mkdir()
            (root/'data').mkdir()
            nested = root/'data'/'redirect'
            nested.mkdir()
            real = scratch._redirect
            for unsafe in (root, root/'data', nested):
                with patch.object(scratch, '_redirect', side_effect=lambda path: path == unsafe or real(path)):
                    with self.assertRaises(ValueError):
                        scratch.remove_children(root)
                self.assertTrue((root/'bin').exists())

    def test_redirected_ancestor(self):
        with tempfile.TemporaryDirectory(dir=_STORAGE.root) as directory:
            root = Path(directory)
            (root/'data').mkdir()
            with patch.object(Path, 'resolve', return_value=root/'elsewhere'):
                with self.assertRaises(ValueError):
                    scratch.remove_children(root)
