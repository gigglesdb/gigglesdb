"""Transport ownership and failure handling without starting Docker."""
import io
import struct
import subprocess
import threading
import time
import unittest
from unittest.mock import patch

import prepared_wire as wire


def frame(tag, payload=b''):
    return wire.message(tag.encode(), payload)


def error_frames(code):
    return frame('E', b'SERROR\0C' + code.encode() + b'\0Mexpected\0\0') + frame('Z', b'I')


def responses(kind):
    startup = frame('Z', b'I')
    metadata = frame('1') + frame('t', b'\0\0') + frame('Z', b'I')
    if kind == 'setup':
        return startup + error_frames('42P01')
    if kind == 'parse':
        return startup + error_frames('42883')
    if kind == 'bind':
        return startup + metadata + error_frames('22P02')
    return startup + metadata + frame('2') + frame('D', b'\0\1\0\0\0\1' + b'1') + frame('Z', b'I')


class Input(io.BytesIO):
    def __init__(self, broken=False):
        super().__init__()
        self.broken = broken
        self.writes = []

    def write(self, data):
        if self.broken:
            raise BrokenPipeError('primary broken pipe')
        self.writes.append(data)
        return super().write(data)


class Process:
    def __init__(self, body, cleanup_fail=False, broken=False, wait_hangs=False):
        self.stdin = Input(broken)
        self.stdout = io.BytesIO(body)
        self.stderr = io.BytesIO()
        self.returncode = None
        self.cleanup_fail = cleanup_fail
        self.wait_hangs = wait_hangs
        self.kills = 0
        self.waits = []

    def poll(self):
        return self.returncode

    def wait(self, timeout):
        self.waits.append(timeout)
        if self.wait_hangs and not self.kills:
            raise subprocess.TimeoutExpired('owned fake', timeout)
        if self.cleanup_fail:
            raise OSError('cleanup wait failed')
        self.returncode = 0 if not self.kills else -9
        return self.returncode

    def kill(self):
        self.kills += 1
        self.returncode = -9


class Lifecycle(unittest.TestCase):
    def observe(self, process, setup=()):
        reference = wire.WireReference()
        reference.container = 'owned_fake'
        with patch.object(wire.subprocess, 'Popen', return_value=process):
            return reference.observe('SELECT 1', [], setup)

    def test_success_sends_terminate_closes_and_reaps_without_kill(self):
        process = Process(responses('ok'))
        result, _ = self.observe(process)
        self.assertEqual(result['rows'], [['1']])
        self.assertEqual(process.stdin.writes[-1], frame('X'))
        self.assertEqual(process.kills, 0)
        self.assertEqual(len(process.waits), 1)
        self.assertTrue(all(s.closed for s in (process.stdin, process.stdout, process.stderr)))

    def test_sql_and_setup_errors_cannot_hide_cleanup_failure(self):
        for kind, code, status, setup in [
            ('setup', '42P01', 'setup_error', ['bad']),
            ('parse', '42883', 'error', ()),
            ('bind', '22P02', 'error', ()),
        ]:
            with self.subTest(kind=kind):
                process = Process(responses(kind), cleanup_fail=True)
                with self.assertRaises(wire.TransportCleanupError) as caught:
                    self.observe(process, setup)
                result = caught.exception.observation
                trace = caught.exception.trace
                self.assertEqual((result['status'], result['sqlstate']), (status, code))
                self.assertIn('transport_cleanup_error', trace[-1])

    def test_primary_read_error_survives_cleanup_error(self):
        process = Process(b'', cleanup_fail=True)
        with self.assertRaisesRegex(RuntimeError, '^wire EOF$') as caught:
            self.observe(process)
        if callable(getattr(caught.exception, 'add_note', None)):
            self.assertTrue(any('cleanup wait failed' in note for note in caught.exception.__notes__))

    def test_primary_broken_pipe_survives_cleanup_error(self):
        process = Process(b'', broken=True, cleanup_fail=True)
        with self.assertRaisesRegex(BrokenPipeError, 'primary broken pipe') as caught:
            self.observe(process)
        if callable(getattr(caught.exception, 'add_note', None)):
            self.assertTrue(caught.exception.__notes__)

    def test_primary_exception_without_add_note_remains_primary(self):
        class LegacyError(Exception):
            add_note = None

        failure = LegacyError('primary legacy error')
        process = Process(b'', cleanup_fail=True)

        def fail_read(count):
            raise failure

        process.stdout.read = fail_read
        with self.assertRaises(LegacyError) as caught:
            self.observe(process)
        self.assertIs(caught.exception, failure)
        self.assertTrue(all(s.closed for s in (process.stdin, process.stdout, process.stderr)))

    def test_invalid_frame_preserves_primary_error(self):
        process = Process(b'Z' + struct.pack('!i', 3), cleanup_fail=True)
        with self.assertRaisesRegex(RuntimeError, '^invalid length$'):
            self.observe(process)

    def test_success_cannot_hide_cleanup_failure(self):
        process = Process(responses('ok'), cleanup_fail=True)
        with self.assertRaises(wire.TransportCleanupError):
            self.observe(process)

    def test_shutdown_hang_kills_and_reaps_only_owned_process(self):
        process = Process(responses('ok'), wait_hangs=True)
        with self.assertRaisesRegex(wire.TransportCleanupError, 'transport shutdown timed out'):
            self.observe(process)
        self.assertEqual(process.kills, 1)
        self.assertEqual(process.waits, [wire._SHUTDOWN_TIMEOUT] * 2)

    def test_nonzero_exit_retains_observation_as_failed_transport_evidence(self):
        process = Process(responses('parse'))

        def wait(timeout):
            process.returncode = 7
            return 7

        process.wait = wait
        with self.assertRaises(wire.TransportCleanupError) as caught:
            self.observe(process)
        self.assertEqual(caught.exception.observation['sqlstate'], '42883')
        self.assertIn('status 7', str(caught.exception))

    def test_deadline_breaks_blocked_read_and_preserves_timeout(self):
        process = Process(b'')
        unblock = threading.Event()

        def blocked_read(count):
            unblock.wait(2)
            return b''

        def kill():
            process.kills += 1
            process.returncode = -9
            unblock.set()

        process.stdout.read = blocked_read
        process.kill = kill
        start = time.monotonic()
        with patch.object(wire, '_OBSERVE_TIMEOUT', 0.05):
            with self.assertRaisesRegex(TimeoutError, 'wire transport deadline exceeded'):
                self.observe(process)
        self.assertLess(time.monotonic() - start, 1.5)
        self.assertEqual(process.kills, 1)


if __name__ == '__main__':
    unittest.main(verbosity=2)
