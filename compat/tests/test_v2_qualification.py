import hashlib
import io
from pathlib import Path
import stat
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import v2_qualification as qualification


class QualificationTests(unittest.TestCase):
    def runner(self, **overrides):
        args = dict(seed=5412, rows=12, batch=2, copy_batch=4, size_check_rows=8,
                    payload_bytes=5, target_bytes=0, max_bytes=1000)
        args.update(overrides)
        run = object.__new__(qualification.Qualification)
        run.args = SimpleNamespace(**args)
        run.root = Path('owned-run')
        run.loaded_rows = 0
        run.cells = {}
        return run

    def test_size_skips_vanishing_files_but_preserves_other_errors(self):
        vanished, regular, folder = Mock(), Mock(), Mock()
        vanished.stat.side_effect = FileNotFoundError('compacted SST')
        regular.stat.return_value = SimpleNamespace(st_mode=stat.S_IFREG, st_size=17)
        folder.stat.return_value = SimpleNamespace(st_mode=stat.S_IFDIR, st_size=4096)
        root = Mock()
        root.rglob.return_value = [vanished, regular, folder]
        self.assertEqual(qualification.size(root), 17)
        regular.stat.side_effect = PermissionError('not a compaction race')
        with self.assertRaises(PermissionError):
            qualification.size(root)

    def test_payload_has_exact_ascii_length_and_independent_deterministic_rows(self):
        for length in (0, 1, 5, 512):
            text = qualification.payload(5412, 123, length)
            self.assertEqual(len(text.encode('ascii')), length)
            self.assertEqual(text, qualification.payload(5412, 123, length))
            self.assertTrue(set(text) <= set('0123456789abcdef'))
        self.assertNotEqual(qualification.payload(1, 123, 512), qualification.payload(1, 124, 512))
        self.assertNotEqual(qualification.payload(1, 123, 512), qualification.payload(2, 123, 512))

    def test_target_stops_on_sampled_size_and_counts_acknowledged_rows(self):
        run, client = self.runner(target_bytes=80), Mock()
        with patch.object(qualification, 'size', side_effect=[120, 100]) as size, patch('builtins.print'):
            disk, stats = run.load_large(client, Path('data'))
        self.assertEqual((run.loaded_rows, disk, stats['copy_rows']), (8, 100, 8))
        self.assertEqual(size.call_count, 2)  # Root cap and database target, once per sample.
        self.assertEqual(client.copy.call_count, 2)
        for call in client.copy.call_args_list:
            table, text, count = call.args
            self.assertEqual((table, len(text.splitlines()), count), ('big', 4, 4))
            for line in text.splitlines():
                row, v, h, payload = line.split('\t')
                self.assertEqual((v, h, payload),
                                 (row, str(int(row)*17), qualification.payload(5412, int(row), 5)))

    def test_rows_remain_an_upper_bound_when_target_not_reached(self):
        run, client = self.runner(rows=10, target_bytes=900, payload_bytes=0), Mock()
        with patch.object(qualification, 'size', return_value=20), patch('builtins.print'):
            disk, stats = run.load_large(client, Path('data'))
        self.assertEqual((run.loaded_rows, disk, stats['copy_rows']), (10, 20, 10))
        self.assertEqual([c.args[2] for c in client.copy.call_args_list], [4, 4, 2])
        self.assertTrue(all(len(line.split('\t')) == 3 for c in client.copy.call_args_list
                            for line in c.args[1].splitlines()))
        self.assertEqual(run.big_values(2), '(2,2,34)')

    def test_copy_rejection_falls_back_once_with_sampled_size_checks(self):
        run, client = self.runner(), Mock()
        client.copy.side_effect = qualification.SqlError('not supported')
        client.rows.return_value = [['0']]
        client.sql.return_value = ['BEGIN', 'INSERT 0 2', 'COMMIT']
        with patch.object(qualification, 'size', return_value=20) as size, patch('builtins.print'):
            _, stats = run.load_large(client, Path('data'))
        self.assertEqual((run.loaded_rows, stats['copy_rows'], stats['insert_rows']), (12, 0, 12))
        self.assertEqual(client.copy.call_count, 1)
        self.assertEqual(client.sql.call_count, 6)
        self.assertEqual(size.call_count, 4)  # At 8 rows and final 12, never every INSERT.
        self.assertIn(run.big_values(0), client.sql.call_args_list[0].args[0])

    def test_copy_with_ambiguous_commit_is_not_retried(self):
        run, client = self.runner(), Mock()
        client.copy.side_effect = qualification.SqlError('lost reply')
        client.rows.return_value = [['4']]
        with self.assertRaisesRegex(RuntimeError, 'ambiguous'):
            run.load_large(client, Path('data'))
        client.sql.assert_not_called()
        self.assertEqual(run.loaded_rows, 0)

    def test_copy_requires_complete_statement_acknowledgement(self):
        client = object.__new__(qualification.Psql)
        client.run = SimpleNamespace(container='owned-container', env={'TEMP': 'owned-tmp'},
                                     args=SimpleNamespace(command_timeout=60))
        client.port, client.log = 1234, io.StringIO()
        result = SimpleNamespace(returncode=0, stdout='COPY 2\n', stderr='')
        with patch.object(qualification.subprocess, 'run', return_value=result) as command:
            client.copy('big', '0\t0\t0\n1\t1\t17\n', 2)
            self.assertEqual(command.call_args.kwargs['input'], '0\t0\t0\n1\t1\t17\n')
            self.assertEqual(command.call_args.args[0][-1], 'COPY big FROM STDIN')
            result.stdout = 'COPY 1\n'
            with self.assertRaisesRegex(RuntimeError, 'acknowledge'):
                client.copy('big', 'two rows', 2)
            result.returncode, result.stderr = 1, 'ERROR: unsupported'
            with self.assertRaises(qualification.SqlError):
                client.copy('big', 'two rows', 2)

    def test_insert_sampling_and_commit_acknowledgement(self):
        run, client = self.runner(rows=10), Mock()
        client.sql.return_value = ['COMMIT']
        with patch.object(qualification, 'size', return_value=20) as size, patch('builtins.print'):
            run.load_batches(client, 'ddl_t', 10, lambda i: f'({i},{i*3})')
        self.assertEqual(size.call_count, 2)
        client.sql.return_value = ['BEGIN']
        with self.assertRaisesRegex(RuntimeError, 'COMMIT'):
            run.load_batches(client, 'ddl_t', 10, lambda i: f'({i},{i*3})')

    def test_cap_failure_stops_after_acknowledged_chunk(self):
        run, client = self.runner(max_bytes=10), Mock()
        with patch.object(qualification, 'size', return_value=20), patch('builtins.print'):
            with self.assertRaisesRegex(RuntimeError, '--max-bytes=10'):
                run.load_large(client, Path('data'))
        self.assertEqual(run.loaded_rows, 8)

    def test_verification_and_sum_use_loaded_rows_including_payload_after_restore(self):
        for digest in (True, False):
            with self.subTest(md5_supported=digest):
                run, client = self.runner(rows=100), Mock()
                run.loaded_rows = 3
                def rows(sql):
                    if 'count(*)' in sql:
                        return [['3', '3', '0', '34']]
                    if 'sum(h)' in sql:
                        return [['51']]
                    if 'length(payload)' in sql:
                        i = int(sql.split('id=')[1][:-1])
                        text = qualification.payload(5412, i, 5)
                        return [['5', hashlib.md5(text.encode('ascii')).hexdigest()]]
                    if 'id,payload' in sql:
                        i = int(sql.split('id=')[1][:-1])
                        return [[str(i), qualification.payload(5412, i, 5)]]
                    if 'WHERE h=' in sql:
                        h = int(sql.split('h=')[1][:-1])
                        return [[str(h//17), str(h)]]
                    return [[str(i), str(i*17)] for i in range(3)]
                client.rows.side_effect = rows
                if digest:
                    client.sql.return_value = ['900150983cd24fb0d6963f7d28e17f72']
                else:
                    client.sql.side_effect = qualification.SqlError('42883: undefined function')
                run.verify_large(client)
                run.large_sum(client)
                self.assertEqual(run.cells['large-sum'][0], 'PASS')
