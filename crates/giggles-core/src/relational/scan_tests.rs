use super::*;
use std::cell::Cell;

fn execute(state: &mut Relational, sql: &str) {
    let prepared = Prepared::parse(sql, 1, state.epoch).unwrap();
    state.execute(&prepared, &[], None).unwrap();
}
fn fixture(count: usize) -> (Relational, TableDef) {
    let mut state = Relational::default();
    execute(
        &mut state,
        "CREATE TABLE items (id BIGINT PRIMARY KEY, value BIGINT)",
    );
    let insert = Prepared::parse("INSERT INTO items VALUES ($1, $2)", 1, state.epoch).unwrap();
    for id in 0..count {
        state.execute(&insert, &[Value::Int(id as i64), Value::Int(id as i64)], None).unwrap();
    }
    let table = state.tables["items"].clone();
    (state, table)
}
fn keys(
    source: &LegacyRows<'_>,
    table: &TableDef,
    range: &KeyRange,
    dir: Direction,
) -> Vec<Vec<u8>> {
    source
        .scan(table, range, dir)
        .unwrap()
        .map(|row| {
            let row = row.unwrap();
            assert!(matches!(row.key, Cow::Borrowed(_)));
            assert!(matches!(row.row, Cow::Borrowed(_)));
            row.key.into_owned()
        })
        .collect()
}
#[test]
fn source_ranges_and_get() {
    let (state, table) = fixture(8);
    let source = LegacyRows(&state);
    let all: Vec<_> = state.rows[&table.id].keys().cloned().collect();
    assert_eq!(
        keys(&source, &table, &KeyRange::all(), Direction::Forward),
        all
    );
    assert_eq!(
        keys(&source, &table, &KeyRange::all(), Direction::Reverse),
        all.iter().rev().cloned().collect::<Vec<_>>()
    );
    for lo in [
        Bound::Unbounded,
        Bound::Included(all[2].clone()),
        Bound::Excluded(all[2].clone()),
    ] {
        for hi in [
            Bound::Unbounded,
            Bound::Included(all[5].clone()),
            Bound::Excluded(all[5].clone()),
        ] {
            let range = KeyRange {
                lo: lo.clone(),
                hi: hi.clone(),
            };
            let expected: Vec<_> = state.rows[&table.id]
                .range((lo.clone(), hi.clone()))
                .map(|(key, _)| key.clone())
                .collect();
            assert_eq!(keys(&source, &table, &range, Direction::Forward), expected);
            assert_eq!(
                keys(&source, &table, &range, Direction::Reverse),
                expected.into_iter().rev().collect::<Vec<_>>()
            );
        }
    }
    assert_eq!(
        keys(
            &source,
            &table,
            &KeyRange::point(all[3].clone()),
            Direction::Forward
        ),
        vec![all[3].clone()]
    );
    for (lo, hi) in [
        (
            Bound::Included(all[5].clone()),
            Bound::Included(all[2].clone()),
        ),
        (
            Bound::Excluded(all[2].clone()),
            Bound::Included(all[2].clone()),
        ),
        (
            Bound::Included(all[2].clone()),
            Bound::Excluded(all[2].clone()),
        ),
        (
            Bound::Excluded(all[2].clone()),
            Bound::Excluded(all[2].clone()),
        ),
    ] {
        let range = KeyRange { lo, hi };
        assert!(range.is_empty());
        assert!(keys(&source, &table, &range, Direction::Forward).is_empty());
    }
    let hit = source.get(&table, &all[0]).unwrap().unwrap();
    assert!(matches!(hit.row, Cow::Borrowed(_)));
    assert_eq!(hit.row.values[0], Value::Int(0));
    assert!(source.get(&table, b"absent").unwrap().is_none());
    let mut missing = table.clone();
    missing.id = u64::MAX;
    assert!(matches!(
        source.scan(&missing, &KeyRange::all(), Direction::Forward),
        Err(SqlError::UnknownTable)
    ));
    assert!(matches!(
        source.get(&missing, &all[0]),
        Err(SqlError::UnknownTable)
    ));
}
#[test]
fn legacy_bounds_conversion() {
    for (low, high, point) in [(vec![2], vec![1], false), (vec![2], vec![2], false)] {
        assert!(
            KeyRange::from_bounds(Bounds {
                low,
                high,
                low_inclusive: true,
                high_inclusive: true,
                point
            })
            .is_empty()
        );
    }
    let point = KeyRange::from_bounds(Bounds {
        low: vec![2],
        high: vec![2],
        low_inclusive: true,
        high_inclusive: true,
        point: true,
    });
    assert_eq!(point, KeyRange::point(vec![2]));
    assert_eq!(
        KeyRange::from_bounds(Bounds {
            low: vec![1],
            high: vec![3],
            low_inclusive: false,
            high_inclusive: true,
            point: false
        }),
        KeyRange {
            lo: Bound::Excluded(vec![1]),
            hi: Bound::Included(vec![3])
        }
    );
}
#[test]
fn cancellation_and_laziness() {
    let (state, table) = fixture(200);
    let calls = Cell::new(0);
    let mut cancel = || {
        calls.set(calls.get() + 1);
        calls.get() == 2
    };
    let mut meter = Meter::new(&mut cancel);
    let source = LegacyRows(&state);
    let mut it = filtered(
        source
            .scan(&table, &KeyRange::all(), Direction::Forward)
            .unwrap(),
        &mut meter,
        |row| Ok(Some(row.row.values[0].clone())),
    );
    for id in 0..127 {
        assert_eq!(it.next(), Some(Ok(Value::Int(id))));
    }
    assert_eq!(it.next(), Some(Err(SqlError::Aborted)));
    assert!(it.next().is_none());
    drop(it);
    assert_eq!(meter.rows(), 128);
    assert_eq!(calls.get(), 2);
    let (state, table) = fixture(5000);
    let source = LegacyRows(&state);
    let mut cancel = || false;
    let mut meter = Meter::new(&mut cancel);
    let rows = filtered(
        source
            .scan(&table, &KeyRange::all(), Direction::Forward)
            .unwrap(),
        &mut meter,
        |row| Ok(Some(row.row.values[0].clone())),
    );
    let output: Vec<_> = Page {
        offset: 2,
        limit: Some(3),
    }
    .apply(rows)
    .collect();
    assert_eq!(
        output,
        vec![Ok(Value::Int(2)), Ok(Value::Int(3)), Ok(Value::Int(4))]
    );
    assert_eq!(meter.rows(), 5);
}
#[test]
fn query_stream_cancellation_and_limit() {
    let (mut state, _) = fixture(1200);
    let query = Prepared::parse("SELECT id FROM items", 1, state.epoch).unwrap();
    let calls = Cell::new(0);
    assert_eq!(
        state.execute_with_cancel(&query, &[], None, || {
            calls.set(calls.get() + 1);
            true
        }),
        Err(SqlError::Aborted)
    );
    assert_eq!(calls.get(), 1);

    let query = Prepared::parse("SELECT id FROM items LIMIT 1", 1, state.epoch).unwrap();
    let result = state.execute_with_cancel(&query, &[], None, || {
        calls.set(calls.get() + 1);
        true
    }).unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].values, vec![Value::Int(0)]);
    assert_eq!(calls.get(), 1);

    // A non-key predicate must reach past the old intermediate row ceiling.
    let query = Prepared::parse("SELECT id FROM items WHERE value = 1199 LIMIT 1", 1, state.epoch).unwrap();
    let result = state.execute_with_cancel(&query, &[], None, || false).unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].values, vec![Value::Int(1199)]);
}
#[test]
fn filtering_and_errors_fuse() {
    let (state, table) = fixture(10);
    let source = LegacyRows(&state);
    let mut cancel = || false;
    let mut meter = Meter::new(&mut cancel);
    let mut it = filtered(
        source
            .scan(&table, &KeyRange::all(), Direction::Forward)
            .unwrap(),
        &mut meter,
        |row| {
            let Value::Int(id) = row.row.values[0] else {
                unreachable!()
            };
            if id == 6 {
                Err(SqlError::Type)
            } else {
                Ok((id % 2 == 0).then_some(id))
            }
        },
    );
    assert_eq!(it.next(), Some(Ok(0)));
    assert_eq!(it.next(), Some(Ok(2)));
    assert_eq!(it.next(), Some(Ok(4)));
    assert_eq!(it.next(), Some(Err(SqlError::Type)));
    assert_eq!(it.next(), None);
    assert_eq!(it.next(), None);
    drop(it);
    assert_eq!(meter.rows(), 7);
    let mut it = filtered(
        Box::new(std::iter::once(Err(SqlError::Version))),
        &mut meter,
        |_| Ok(Some(1)),
    );
    assert_eq!(it.next(), Some(Err(SqlError::Version)));
    assert_eq!(it.next(), None);
}
#[test]
fn stable_bounded_sort() {
    let mut seed = 123u64;
    let rows: Vec<_> = (0..2000)
        .map(|seq| {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            ((seed >> 32) % 17, seq)
        })
        .collect();
    let mut expected = rows.clone();
    expected.sort_by_key(|r| r.0);
    for keep in [
        None,
        Some(0),
        Some(1),
        Some(5),
        Some(1999),
        Some(2000),
        Some(5000),
    ] {
        let mut retention = Retention::new(MAX_RETAINED_BYTES);
        retention.charge(13).unwrap();
        let result = sort_bounded(
            rows.iter().copied().map(Ok),
            keep,
            |_| 10,
            |a, b| a.0.cmp(&b.0),
            &mut retention,
        )
        .unwrap();
        assert_eq!(
            result,
            expected[..keep.unwrap_or(expected.len()).min(expected.len())]
        );
        assert_eq!(retention.used(), 13);
    }
    let pulls = Cell::new(0);
    let mut retention = Retention::new(1);
    assert!(
        sort_bounded(
            std::iter::from_fn(|| {
                pulls.set(pulls.get() + 1);
                Some(Ok(1))
            }),
            Some(0),
            |_| 1,
            Ord::cmp,
            &mut retention
        )
        .unwrap()
        .is_empty()
    );
    assert_eq!(pulls.get(), 0);
}
#[test]
fn retention_limits_and_cleanup() {
    let mut retention = Retention::new(40);
    // A cap of 40 proves no more than four size-10 rows are charged,
    // including during replacement; the incoming candidate is the fifth row.
    let result = sort_bounded(
        (0..1000).rev().map(Ok),
        Some(4),
        |_| 10,
        Ord::cmp,
        &mut retention,
    )
    .unwrap();
    assert_eq!(result, vec![0, 1, 2, 3]);
    assert_eq!(retention.peak, 40);
    assert!(retention.peak + 10 <= 50);
    assert_eq!(retention.used(), 0);
    let mut retention = Retention::new(25);
    retention.charge(1).unwrap();
    assert_eq!(
        sort_bounded((0..3).map(Ok), None, |_| 10, Ord::cmp, &mut retention),
        Err(SqlError::Limit)
    );
    assert_eq!(retention.used(), 1);
    assert_eq!(
        sort_bounded(
            [Ok(1), Err(SqlError::Type), Ok(2)].into_iter(),
            None,
            |_| 10,
            Ord::cmp,
            &mut retention
        ),
        Err(SqlError::Type)
    );
    assert_eq!(retention.used(), 1);
    let sizes = Cell::new(0);
    assert_eq!(
        sort_bounded(
            (0..1000).map(Ok),
            Some(4),
            |_| {
                sizes.set(sizes.get() + 1);
                10
            },
            Ord::cmp,
            &mut Retention::new(40)
        )
        .unwrap(),
        vec![0, 1, 2, 3]
    );
    assert_eq!(sizes.get(), 4);
    let mut retention = Retention::new(usize::MAX);
    retention.charge(usize::MAX).unwrap();
    assert_eq!(retention.charge(1), Err(SqlError::Limit));
    assert_eq!(retention.used(), usize::MAX);
    retention.release(usize::MAX);
    retention.release(1);
    assert_eq!(retention.used(), 0);
}
#[test]
fn page_saturates_and_meter_overflow() {
    assert_eq!(Page::default().keep(), None);
    assert_eq!(
        Page::default().apply(0..3).collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    let page = Page {
        offset: 3,
        limit: Some(2),
    };
    assert_eq!(page.keep(), Some(5));
    assert_eq!(page.apply(0..10).collect::<Vec<_>>(), vec![3, 4]);
    let page = Page {
        offset: u64::MAX,
        limit: Some(u64::MAX),
    };
    assert_eq!(page.keep(), Some(usize::MAX));
    assert_eq!(page.apply(0..3).next(), None);
    let mut cancel = || false;
    let mut meter = Meter::new(&mut cancel);
    meter.rows = u64::MAX;
    assert_eq!(meter.tick(), Err(SqlError::Limit));
    assert_eq!(meter.rows(), u64::MAX);
}

#[test]
fn converted_ranges_match_map() {
    let (state, table) = fixture(8);
    let source = LegacyRows(&state);
    let map = &state.rows[&table.id];
    let all: Vec<_> = map.keys().cloned().collect();
    for low_inclusive in [false, true] {
        for high_inclusive in [false, true] {
            let range = KeyRange::from_bounds(Bounds {
                low: all[2].clone(),
                high: all[5].clone(),
                low_inclusive,
                high_inclusive,
                point: false,
            });
            let expected: Vec<_> = map
                .range((range.lo.clone(), range.hi.clone()))
                .map(|(key, _)| key.clone())
                .collect();
            assert_eq!(keys(&source, &table, &range, Direction::Forward), expected);
        }
    }
    let range = KeyRange::from_bounds(Bounds {
        low: all[3].clone(),
        high: all[3].clone(),
        low_inclusive: true,
        high_inclusive: true,
        point: true,
    });
    assert_eq!(
        keys(&source, &table, &range, Direction::Forward),
        vec![all[3].clone()]
    );
}

#[test]
fn page_zero_does_not_pull_and_large_limit_is_finite() {
    let pulls = Cell::new(0);
    let it = std::iter::from_fn(|| {
        pulls.set(pulls.get() + 1);
        Some(1)
    });
    assert_eq!(
        Page {
            offset: 123,
            limit: Some(0)
        }
        .apply(it)
        .next(),
        None
    );
    assert_eq!(pulls.get(), 0);
    assert_eq!(
        Page {
            offset: 0,
            limit: Some(u64::MAX)
        }
        .apply(0..3)
        .collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
}
