"""Synthetic pgbench 18 parsing and sweep boundaries; no processes."""
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from bench import Bench, parse_log, percentile, stop_reason, summarize


def test_scheduled_latency_already_includes_lag():
    values, errors = parse_log(['0 81 4621 0 1412881037 912698 3005',
                               '1 82 6173 0 1412881037 914578 4304'], rate=True)
    assert values == [4.621, 6.173] and not errors


def test_failures_never_count_as_commits():
    values, errors = parse_log(['0 0 1000 0 1 0', '0 1 serialization 0 1 1',
                               '1 0 deadlock 0 1 2', '2 0 failed 0 1 3'])
    assert values == [1] and errors == {'40001': 1, '40P01': 1, 'failed': 1}
    assert parse_log(['0 1 skipped 0 1 2 20'], True)[1] == {'skipped': 1}


@pytest.mark.parametrize('line', ['bad', '0 0 nan 0 1 2', '0 0 -1 0 1 2', '0 0 1 0 1 2 3'])
def test_reject_wrong_format(line):
    with pytest.raises(ValueError):
        parse_log([line])


def test_nearest_rank_and_pooled_percentiles():
    assert percentile([], 99) is None
    point = summarize([1] * 99 + [100], {}, 100)
    assert (point['p50_ms'], point['p95_ms'], point['p99_ms'], point['mean_ms']) == (1, 1, 1, 1.99)
    assert percentile([1, 100], 99) == 100


@pytest.mark.parametrize('changes,rate,expected', [({}, 100, None), ({}, 200, 'max_rate'),
    ({'p99_ms': 20.001}, 100, 'p99_exceeded'), ({'p99_ms': None}, 100, 'p99_exceeded'),
    ({'tps': 94.99}, 100, 'achieved_below_95_percent'), ({'errors': {'40001': 1}}, 100, 'errors'),
    ({'process_errors': 1}, 100, 'errors')])
def test_stop_boundaries(changes, rate, expected):
    point = dict(tps=200 if rate == 200 else 95, p99_ms=20, errors={}) | changes
    assert stop_reason(point, rate, 20, 200) == expected


def test_sweep_retains_last_good_point_and_warmup():
    from types import SimpleNamespace
    bench = Bench.__new__(Bench)
    bench.a = SimpleNamespace(start_rate=100, step=1.5, max_rate=200, p99_ms=20, warmup=3)
    calls = []
    def run(kind, offered, duration=None):
        calls.append((kind, offered, duration))
        return dict(tps=offered, p99_ms=10 if offered < 200 else 21, errors={})
    bench.run = run
    result = bench.sweep('read')
    assert result['highest_passing']['offered_tps'] == 150
    assert [p['offered_tps'] for p in result['curve']] == [100, 150, 200]
    assert result['curve'][-1]['stop'] == 'p99_exceeded'
    assert calls == [('read', r, d) for r in (100, 150, 200) for d in (3, None)]


def test_sweep_with_no_passing_rate():
    from types import SimpleNamespace
    bench = Bench.__new__(Bench)
    bench.a = SimpleNamespace(start_rate=100, step=1.5, max_rate=200, p99_ms=20, warmup=0)
    bench.run = lambda *a, **kw: dict(tps=80, p99_ms=1, errors={})
    result = bench.sweep('write')
    assert result['highest_passing'] is None and len(result['curve']) == 1


@pytest.mark.parametrize('rows,chunk,bounds', [(1, 10000, [(1, 1)]),
    (20000, 10000, [(1, 10000), (10001, 20000)]),
    (20001, 10000, [(1, 10000), (10001, 20000), (20001, 20001)])])
def test_fill_chunk_boundaries(rows, chunk, bounds):
    from psycopg.types.numeric import Int8
    from types import SimpleNamespace
    from unittest.mock import MagicMock
    bench = Bench.__new__(Bench)
    bench.a, bench.cluster = SimpleNamespace(rows=rows, fill_chunk=chunk), None
    conn = MagicMock()
    bench.connect = lambda: conn
    conn.__enter__.return_value = conn
    fill = bench.prepare()
    inserts = [call.args for call in conn.execute.call_args_list if call.args[0].startswith('INSERT')]
    assert [args[1] for args in inserts] == bounds * 2
    assert all(isinstance(value, Int8) for args in inserts for value in args[1])
    assert all("SELECT g, 0, repeat('x', 100) FROM generate_series(%s, %s) AS g" in args[0]
               for args in inserts)
    assert all(item['rows'] == rows and item['form'] == 'generate_series' for item in fill.values())
    conn.transaction.assert_not_called()
    conn.cursor.assert_not_called()


def test_fill_values_fallback_boundaries():
    from types import SimpleNamespace
    from unittest.mock import MagicMock
    import psycopg
    bench = Bench.__new__(Bench)
    bench.a, bench.cluster = SimpleNamespace(rows=1003, fill_chunk=1001), None
    conn = MagicMock()
    bench.connect = lambda: conn
    conn.__enter__.return_value = conn
    def execute(sql, params=None):
        if 'generate_series' in sql:
            raise psycopg.errors.FeatureNotSupported('unsupported')
    conn.execute.side_effect = execute
    fill = bench.prepare()
    inserts = [call.args for call in conn.execute.call_args_list if ' VALUES ' in call.args[0]]
    assert [len(args[1]) // 3 for args in inserts] == [500, 500, 1, 2] * 2
    assert [args[1][::3] for args in inserts] == [list(range(a, b)) for a, b in
        [(1, 501), (501, 1001), (1001, 1002), (1002, 1004)]] * 2
    assert all(item['form'] == 'values' for item in fill.values())
    conn.transaction.assert_not_called()
