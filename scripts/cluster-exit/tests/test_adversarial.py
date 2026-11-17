"""Pad entropy and bound SQL regression checks; no server processes."""
from pathlib import Path
import sys
from types import SimpleNamespace
import threading
import zlib

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from adversarial import arguments, insert_pad
import adversarial as adv


@pytest.mark.parametrize('size,account', [(16384, True), (4096, False), (32768, False),
                                         (65536, False), (1, False), (0, True)])
def test_pad_insert_is_random_and_bound(size, account):
    calls = []
    conn = SimpleNamespace(execute=lambda sql, params: calls.append((sql, params)))
    assert insert_pad(conn, 'pad_test', 41, size, account) == size
    sql, (ident, pad) = calls[0]
    assert ident == 41 and len(pad) == size
    assert len(sql.encode()) < 4096 and sql.count('%s') == 2
    assert sql == ('INSERT INTO pad_test (id, bal, pad) VALUES (%s, 100, %s)' if account else
                   'INSERT INTO pad_test (id, pad) VALUES (%s, %s)')
    if size >= 4096:
        ratio = len(zlib.compress(pad.encode())) / size
        assert .5 < ratio < 1.1  # Random hex retains entropy; repeated characters fail.
        raw = bytes.fromhex(pad)
        assert len(raw) / len(zlib.compress(raw)) < 1.1


def test_bulk_defaults(monkeypatch):
    monkeypatch.setattr(sys, 'argv', ['adversarial', '--cluster', 'c', '--out', 'o', '--checker', 'h'])
    args = arguments()
    assert (args.bulk_row_bytes, args.bulk_bytes_per_sec) == (32768, 4194304)
    assert args.split_wait == 120


@pytest.mark.parametrize('total,accounts,expected', [(19, 1, 'done'), (18, 2, 'done'), (18, 1, 'failed')])
def test_split_wait_ignores_window_end(monkeypatch, total, accounts, expected):
    counts, clock = iter([18, total]), iter([0, 0, 2])
    monkeypatch.setattr(adv, 'ranges', lambda _: range(next(counts)))
    monkeypatch.setattr(adv, 'account_ranges', lambda *_: (range(accounts), {}))
    monkeypatch.setattr(adv.time, 'monotonic', lambda: next(clock))
    monkeypatch.setattr(adv.time, 'sleep', lambda _: None)
    done, actions = threading.Event(), []
    done.set()
    adv.fault(None, None, 'split', [], actions, 6, 18, done, split_wait=1)
    assert actions[0]['status'] == expected
    assert ('first_growth_seconds' in actions[0]) == (expected == 'done')


@pytest.mark.parametrize('name', ['total', 'scan', 'filler'])
@pytest.mark.parametrize('state,broken', [('25P02', False), ('25P02', 'SELECT 1'),
                                         ('25P02', 'ROLLBACK'), ('22000', False), ('40001', False), ('python', False)])
def test_observer_connection_ring_and_recovery(monkeypatch, name, state, broken):
    calls, connections = [], []
    monkeypatch.setattr(adv.random.Random, 'choice', lambda _, nodes: nodes[-1])
    class Cursor(list):
        def fetchone(self):
            return self[0]
    def execute(sql, params=None):
        calls.append(sql)
        if len(calls) == 7:
            if state != '25P02':
                observer.stop.set()
            raise (ValueError if state == 'python' else adv.psycopg.errors.lookup(state))('injected')
        if broken and sql == broken:
            raise adv.psycopg.OperationalError('broken probe')
        if len(calls) == 10:
            observer.stop.set()
        return Cursor([(2, 200)] if 'count(*)' in sql else [(1,)] if sql == 'SELECT 1' else [])
    conn = SimpleNamespace(execute=execute, closed=False, close=lambda: None)
    target = SimpleNamespace(nodes=[1, 2, 3], connect=lambda n: connections.append(n) or conn)
    observer = adv.Observers(target, 'acct', 'fill', [1, 2], SimpleNamespace(fill_bytes=1))
    observer.stop.wait = lambda _: None
    observer.loop(name)
    result = observer.results[name]
    assert len(connections) == (2 if broken else 1) and result['retries'] == (state == '40001')
    if state == '40001':
        assert result['errors'] == []
        return
    error, = result['errors']
    assert len(error['events']) == 5
    assert [e['outcome'] for e in error['events']] == ['ok'] * 4 + ['ValueError' if state == 'python' else state]
    assert all(e['node'] == connections[0] and e['at'] and e['kind'] for e in error['events'])
    if state == '25P02':
        assert calls.count('ROLLBACK') == 1 and error['recovery']['rollback_ok'] == (broken != 'ROLLBACK')
        assert error['recovery']['next_statement_ok'] == (not broken)
        assert error['recovery']['events'][-1]['kind'] == ('ROLLBACK' if broken == 'ROLLBACK' else 'SELECT probe')
        assert result['answers'] == 6
    else:
        assert 'recovery' not in error and 'ROLLBACK' not in calls
