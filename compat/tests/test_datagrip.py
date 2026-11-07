"""Regression checks for source-derived DataGrip cases."""
import importlib.util
import json
from pathlib import Path
import re
import shutil
import tempfile
import unittest
from unittest.mock import patch

from tests.task_storage import ModuleStorage

_STORAGE = ModuleStorage()
SPEC = importlib.util.spec_from_file_location('gen_datagrip', Path(__file__).resolve().parents[1] / 'tools/gen_datagrip_corpus.py')
gen = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(gen)


def setUpModule():
    _STORAGE.start()


def tearDownModule():
    _STORAGE.finish()


class DataGripTests(unittest.TestCase):
    def test_family_collisions_have_stable_short_hash_suffix(self):
        rows = [dict(sql=sql, sha=gen.digest(sql), family='same family', source='source', case='case')
                for sql in ('SELECT 1', 'SELECT 2')]
        rows.append(dict(rows[0], sql='SELECT 3', sha=gen.digest('SELECT 3'), family='x' * 100))
        with patch.object(gen, 'sources', return_value=iter(rows)):
            items = list(gen.cases())
        self.assertEqual(items[0]['id'], 'datagrip_same_family')
        self.assertEqual(items[1]['id'], 'datagrip_same_family_' + rows[1]['sha'][:12])
        self.assertLessEqual(len(items[2]['id']), 47)

    def test_exact_source_hashes_and_deduplication(self):
        source = list(gen.sources())
        items = list(gen.cases())
        self.assertEqual({x['sha'] for x in source}, {x['sha'] for x in items})
        self.assertEqual(len(items), len({x['sha'] for x in source}))
        self.assertLess(len(items), len(source))
        for item in items:
            self.assertEqual(item['sha'], gen.digest(item['sql']))
            self.assertRegex(item['id'], r'^datagrip_[a-z0-9_]+$')
            self.assertLessEqual(len(('r0c_' + item['id'] + '_app').encode()), 63)

    def test_metadata_fixture_and_source_preservation(self):
        for item in gen.cases():
            with self.subTest(case=item['id']):
                text = gen.render(item)
                self.assertIn('-- owner: G\n', text)
                self.assertIn('-- compare: header\n', text)
                # A CRLF checkout (core.autocrlf or git archive) may convert
                # line endings; the generator's content contract is LF-normalized.
                checked_in = (gen.OUT / (item['id'] + '.sql')).read_bytes().replace(b'\r\n', b'\n')
                self.assertEqual(checked_in, text.encode('utf-8'))
                self.assertFalse(checked_in.startswith(b'\xef\xbb\xbf'))
                self.assertNotIn(b'\r', checked_in)
                self.assertIn(f'-- source: {item["source"]}#{item["case"]}\n', text)
                self.assertIn(item['sha'][:12], text)
                self.assertEqual(text.count('\nCREATE TABLE '), 1)
                self.assertEqual(text.count('\nCREATE INDEX '), 1)
                self.assertIn('id bigint PRIMARY KEY, body text, flag boolean, small smallint, num integer, data bytea', text)
                expected = re.sub(r'(?<!:):schema_id\b|\$[0-9]+\b', '2200', item['sql'])
                tokens = list(dict.fromkeys(re.findall(r'(?<!:):schema_id\b|\$[0-9]+\b', item['sql'])))
                if tokens:
                    self.assertIn('-- parameters: ' + ', '.join(token + '=2200' for token in tokens), text)
                else:
                    self.assertNotIn('-- parameters:', text)
                self.assertTrue(text.endswith('\n'.join(gen.statements(expected)) + '\n'))

    def test_split_preserves_comments_and_quoted_semicolons(self):
        sql = "SELECT ';'; SELECT $$a;b$$; /* nested /* ; */ ; */ SELECT 3 -- end"
        parts = list(gen.statements(sql))
        self.assertEqual(len(parts), 3)
        self.assertEqual(''.join(parts), sql + '\n;')
        self.assertEqual(list(gen.statements('SELECT 1; -- trailing')), ['SELECT 1;', ' -- trailing'])

    def test_regeneration_is_identical_and_scoped(self):
        with tempfile.TemporaryDirectory(dir=_STORAGE.root) as folder:
            out = Path(folder)
            other = out / 'other.sql'
            other.write_text('SELECT 99;', encoding='utf-8')
            stale = out / 'stale.sql'
            stale.write_text(gen.MARKER + '\nold', encoding='utf-8')
            gen.generate(out)
            self.assertFalse(stale.exists())
            before = {p.name: p.read_bytes() for p in out.iterdir()}
            gen.generate(out)
            self.assertEqual(before, {p.name: p.read_bytes() for p in out.iterdir()})
            self.assertEqual(other.read_text(encoding='utf-8'), 'SELECT 99;')
            for name, data in before.items():
                if name != 'other.sql':
                    self.assertEqual(data, gen.read_source(gen.OUT / name).encode('utf-8'))

    def test_crlf_bom_source_checkout_is_byte_identical(self):
        with tempfile.TemporaryDirectory(dir=_STORAGE.root) as folder:
            root = Path(folder)
            for relative in ('crates/giggles-pg-conformance/probes/datagrip-2026.2.5',
                             'crates/giggles-pg-catalog/tests/fixtures'):
                destination = root / relative
                shutil.copytree(gen.ROOT / relative, destination)
                for path in destination.rglob('*'):
                    if not path.is_file():
                        continue
                    text = gen.read_source(path)
                    if path.name in ('latest-expanded-queries.json', 'app-expanded-queries.json'):
                        rows = json.loads(text)
                        for row in rows:
                            row['sql'] = gen.normalize_source(row['sql']).replace('\n', '\r\n')
                        text = json.dumps(rows, ensure_ascii=False, indent=2) + '\n'
                    path.write_bytes(b'\xef\xbb\xbf' + text.replace('\n', '\r\n').encode('utf-8'))
            baseline = root / 'baseline'
            converted = root / 'converted'
            self.assertEqual(gen.generate(baseline), 36)
            self.assertEqual(gen.generate(converted, root=root), 36)
            expected = {p.name: p.read_bytes() for p in baseline.iterdir()}
            self.assertEqual(expected, {p.name: p.read_bytes() for p in converted.iterdir()})
            gen.generate(converted, root=root)
            self.assertEqual(expected, {p.name: p.read_bytes() for p in converted.iterdir()})

    def test_unowned_collision_is_rejected(self):
        with tempfile.TemporaryDirectory(dir=_STORAGE.root) as folder:
            out = Path(folder)
            item = next(gen.cases())
            (out / (item['id'] + '.sql')).write_text('owned elsewhere', encoding='utf-8')
            with self.assertRaisesRegex(ValueError, 'unowned'):
                gen.generate(out)
