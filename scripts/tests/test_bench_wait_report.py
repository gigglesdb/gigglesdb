"""Small collector-format fixtures; run directly with Python isolated mode."""
import importlib.util
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

sys.dont_write_bytecode = True

HELPER = Path(__file__).resolve().parents[1] / 'bench-wait-report.py'
spec = importlib.util.spec_from_file_location('wait_report', HELPER)
reporter = importlib.util.module_from_spec(spec)
spec.loader.exec_module(reporter)


def fixture(root):
    files = {
        'manifest': '''server_pid=100
    TID COMMAND
    100 tokio-runtime-w
    101 tokio-runtime-w
    102 giggles-commit-1
    103 giggles-io-1
capture=offcpu status=ok duration_seconds=20 start_utc=now pid=100
capture=futex status=ok duration_seconds=10 start_utc=now pid=100
capture=wakeup status=failed duration_seconds=0 start_utc=now pid=100
''',
        'offcpu': '''tokio-runtime-w;std::thread::start;tokio::runtime::scheduler::multi_thread::worker::Context::park;-;entry_SYSCALL_64;futex_wait;schedule;finish_task_switch.isra.0 30000000
giggles-commit-1;giggles::commit;parking_lot::raw_mutex::RawMutex::lock_slow;-;entry_SYSCALL_64;futex_wait;schedule;finish_task_switch.isra.0 4000000
giggles-commit-2;giggles::commit;parking_lot::raw_mutex::RawMutex::lock_slow;-;entry_SYSCALL_64;futex_wait;schedule;finish_task_switch.isra.0 1000000
giggles-io-1;giggles::sync;-;vfs_fsync;ext4_sync_file;schedule;finish_task_switch.isra.0 1000000
giggles-io-1;giggles::read;-;folio_wait_bit_common;io_schedule;schedule 500000
''',
        'futex': '''@count[giggles-commit-1, 
        7f3a1b2c3d4e syscall+30 (/usr/lib/x86_64-linux-gnu/libc.so.6)
        55aa00112233 parking_lot::raw_mutex::RawMutex::lock_slow+0x1a2 (/home/user/giggles-server)
]: 42
@us[giggles-commit-1, 
        7f3a1b2c3d4e syscall+30 (/usr/lib/x86_64-linux-gnu/libc.so.6)
        55aa00112233 parking_lot::raw_mutex::RawMutex::lock_slow+0x1a2 (/home/user/giggles-server)
]: 2000000
''',
        'fsync': '@latency[fsync, giggles-commit]:\n[1, 2) 4 |@@@@|\n',
        'syscount': 'Tracing...\nSYSCALL COUNT LATENCY(ms)\n' + ''.join(f'call{i} {20-i} 1.00\n' for i in range(12)),
        'iostat': '''Linux test
Device r/s w/s w_await %util
nvme0n1 0 4 10.00 20.00

Device r/s w/s w_await %util
nvme0n1 0 4 30.00 40.00
''',
        'pidstat': '''12:00:00 UID TGID TID %usr %system %guest %wait %CPU CPU Command
12:00:01 1000 - 102 2 3 0 0 5 2 |__giggles-commit
12:00:02 1000 - 102 4 5 0 0 9 2 |__giggles-commit
12:00:00 UID TGID TID cswch/s nvcswch/s Command
12:00:01 1000 - 102 10 2 |__giggles-commit
12:00:02 1000 - 102 20 4 |__giggles-commit
Average: 1000 - 102 15 3 |__giggles-commit
''',
    }
    for prefix, text in files.items():
        suffix = 'folded' if prefix == 'offcpu' else 'txt'
        (root / f'{prefix}-fixture.{suffix}').write_text(text, encoding='utf-8')


class WaitReportTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=os.environ.get('TEMP') or os.environ.get('TMPDIR'))
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        fixture(self.root)

    def test_idle_ranking_and_wall(self):
        text = reporter.report(self.root, 'fixture')
        self.assertIn('idle: 30000.000 ms', text)
        self.assertIn('working waits: 6500.000 ms', text)
        self.assertIn('| 1 | giggles-commit | futex |', text)
        self.assertIn('| giggles-io | fsync |', text)
        self.assertIn('| giggles-io | io |', text)
        # 5,000,000 us / (20 s * 1e6 * 1 thread) = 25%.
        self.assertIn('| 5000.000 | 13.70% | 25.00% |', text)
        # Futex: 2,000,000 / (10 s * 1e6 * 1) = 20%.
        self.assertIn('| 2000.000 | 42 | 47619.05 | 20.00% |', text)
        self.assertIn('Server threads: 4;', text)
        self.assertIn('tokio-runtime-w \u00d72', text)
        self.assertIn('Avg \u00b5s', text)
        main_table = text.split('Idle summary:')[0].split('| Rank')[1]
        self.assertNotIn('tokio-runtime-w', main_table)

    def test_storage(self):
        text = reporter.report(self.root, 'fixture')
        self.assertIn('| nvme0n1 | 30.00 | 20.00 |', text)
        self.assertIn('| 102 | giggles-commit | 7.00 |', text)
        self.assertIn('| 102 | giggles-commit | 18.00 |', text)
        self.assertIn('@latency[fsync, giggles-commit]:', text)
        self.assertIn('call9 11 1.00', text)
        self.assertNotIn('call10 10 1.00', text)

    def test_swapped_stack_sections_and_full(self):
        rows = reporter.offcpu_rows('worker-1;entry_SYSCALL_64;futex_wait;schedule;-;app::root;app::leaf 10')
        self.assertEqual(next(iter(rows))[:3], ('worker', 'futex', 'futex <- app::leaf <- app::root'))
        text = reporter.report(self.root, 'fixture', full=True)
        self.assertIn('finish_task_switch.isra.0', text)
        self.assertIn('syscall+30', text)

    def test_idle_rules(self):
        idle = reporter.idle_stack
        self.assertTrue(idle(['tokio::runtime::park::Inner::park'], 'worker', 'futex'))
        self.assertTrue(idle(['tokio::runtime::io::Driver::park'], 'worker', 'epoll'))
        self.assertTrue(idle(['std::thread::park', 'crossbeam::Parker::park'], 'pool', 'futex'))
        self.assertFalse(idle(['app::commit', 'std::thread::park'], 'pool', 'futex'))
        self.assertFalse(idle(['parking_lot::Mutex::lock_slow'], 'worker', 'futex'))
        # An application lock wait on a tokio worker is a working wait, even
        # though the root frames are tokio::runtime::scheduler.
        worker_root = ['tokio::runtime::scheduler::multi_thread::worker::run',
                       'giggles::commit::apply', 'parking_lot::raw_mutex::RawMutex::lock_slow',
                       'parking_lot_core::parking_lot::park']
        self.assertFalse(idle(worker_root, 'tokio-runtime-w', 'futex'))
        self.assertTrue(idle(['tokio::runtime::scheduler::multi_thread::worker::run',
                              'tokio::runtime::scheduler::multi_thread::worker::Context::park_timeout'],
                             'tokio-runtime-w', 'futex'))
        blocking = ['std::sys::thread::Thread::new::thread_start',
                    'tokio::runtime::blocking::pool::Inner::run', 'std::sync::Condvar::wait_timeout']
        self.assertTrue(idle(blocking, 'tokio-runtime-w', 'futex'))
        self.assertFalse(idle(blocking[:2] + ['giggles::wal::fsync'], 'tokio-runtime-w', 'futex'))

    def test_futex_leaf_order_and_merge(self):
        text = '@us[worker-1,\n ab app::leaf+1 (server)\n cd app::root+2 (server)\n]: 100\n'
        text += text.replace('worker-1', 'worker-2')
        rows = reporter.futex_rows(text)
        self.assertEqual(rows[('worker', 'app::leaf+1 <- app::root+2')], [200, 0])

    def test_wakeup_and_unknown_threads(self):
        (self.root / 'wakeup-fixture.folded').write_text('target;root;wake;waker 900\n', encoding='utf-8')
        text = reporter.report(self.root, 'fixture')
        self.assertIn('waker <- wake <- root <- target | 900', text)
        (self.root / 'offcpu-no-manifest.folded').write_text('worker;app::lock;-;futex_wait 20\n', encoding='utf-8')
        self.assertIn('| n/a |', reporter.report(self.root, 'no-manifest'))

    def test_wall_pool_denominator_and_fallback(self):
        (self.root / 'offcpu-fixture.folded').write_text('tokio-runtime-w;app::lock;-;futex_wait 4000000\n', encoding='utf-8')
        self.assertIn('| 4000.000 | 100.00% | 10.00% |', reporter.report(self.root, 'fixture'))
        manifest = self.root / 'manifest-fixture.txt'
        manifest.write_text('TID COMMAND\n100 worker-1\n', encoding='utf-8')
        (self.root / 'offcpu-fixture.folded').write_text('worker-1;app::lock;-;futex_wait 3000000\n', encoding='utf-8')
        self.assertIn('| 3000.000 | 100.00% | 10.00% |', reporter.report(self.root, 'fixture'))

    def test_missing_empty_and_cli(self):
        text = reporter.report(self.root, 'absent')
        self.assertIn(reporter.MISSING, text)
        (self.root / 'offcpu-empty.folded').touch()
        self.assertIn(reporter.MISSING, reporter.report(self.root, 'empty'))
        result = subprocess.run([sys.executable, '-I', str(HELPER), str(self.root), '--top', '1'], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('## fixture', result.stdout)
        out = self.root / 'report.md'
        result = subprocess.run([sys.executable, '-I', str(HELPER), str(self.root), '--key', 'fixture', '--out', str(out)], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('## fixture', out.read_text(encoding='utf-8'))


if __name__ == '__main__':
    unittest.main()
