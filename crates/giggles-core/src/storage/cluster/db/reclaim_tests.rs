//! R5-101 (GD-0075 Phase 11): after DROP TABLE / DROP INDEX through a gateway, once the drop is
//! older than the GC TTL, every range leader deletes the dropped object's keys in its range.
use super::*;
use crate::ReclaimStats;
use crate::relational::split_key;
use crate::storage::cluster::db::local::engine_config;
use crate::storage::engine::codec::{self, pk_bytes};
use crate::storage::engine::kv::Keyspace;
use crate::storage::engine::txn::Isolation;
use crate::storage::range::engine::{EngineConfig, reclaim::hooks};
use crate::storage::range_api::{RangeClient, RangeError, ReadDirection, ReadRequest};
use std::ops::Bound;

const TTL: Duration = Duration::from_secs(60 * 60);
const CLOCK_OFFSET_MS: u64 = 10 * 60 * 1000;
const DDL: [&str; 6] = [
    "CREATE TABLE d(id INT PRIMARY KEY, a INT)",
    "CREATE INDEX d_a ON d(a)",
    "CREATE TABLE o(id INT PRIMARY KEY, a INT, b INT)",
    "CREATE INDEX o_a ON o(a)",
    "CREATE INDEX o_b ON o(b)",
    "CREATE TABLE r(id INT PRIMARY KEY)",
];

/// Ids of d, d_a, o, o_a, o_b, r from the committed catalog.
fn ids(admin: &Admin) -> [u64; 6] {
    let Backend::V2(db) = &admin.inner.backend else {
        panic!("expected a v2 backend")
    };
    let mut txn = db.begin(Isolation::ReadCommitted);
    let parts = db.catalog().parts(&mut txn).unwrap();
    let table = |name: &str| parts.tables.values().find(|t| t.name == name).unwrap().id;
    let index = |name: &str| {
        let found = parts.indexes.iter().find(|d| split_key(&d.name).1 == name);
        found.unwrap().id
    };
    let [d, d_a, o, o_a, o_b, r] = ["d", "d_a", "o", "o_a", "o_b", "r"];
    [
        table(d),
        index(d_a),
        table(o),
        index(o_a),
        index(o_b),
        table(r),
    ]
}

/// Splits putting d's rows in three ranges and d_a's and o_a's entries in two each: the ids
/// come from a probe cluster that ran the same DDL (ids are leased in order).
fn spread_splits() -> Vec<Vec<u8>> {
    let probe = LocalCluster::new(&[]);
    let gateway = gateway(&probe, 1);
    let mut s = gateway.admin.sql_session();
    for sql in DDL {
        run(&mut s, sql);
    }
    let [d, d_a, _, o_a, _, _] = ids(&gateway.admin);
    let at = |space, id: u64, suffix: Vec<u8>| {
        range_key(space, &[&id.to_be_bytes()[..], &suffix].concat())
    };
    let pk = |n: i64| pk_bytes(&[Value::Int(n)]).unwrap();
    let tuple =
        |n: i64| codec::index_tuple_bytes(&[Value::Int(n)], &[codec::Direction::Asc]).unwrap();
    let mut splits = vec![
        at(RangeSpace::Rows, d, vec![]),
        at(RangeSpace::Rows, d, pk(200)),
        at(RangeSpace::Rows, d, pk(400)),
        at(RangeSpace::Rows, d + 1, vec![]),
    ];
    for id in [d_a, o_a] {
        splits.push(at(RangeSpace::Index, id, vec![]));
        splits.push(at(RangeSpace::Index, id, tuple(25)));
        splits.push(at(RangeSpace::Index, id + 1, vec![]));
    }
    splits.sort();
    splits.dedup();
    splits
}

#[test]
fn dropped_tables_and_indexes_are_reclaimed_on_every_range_after_the_gc_ttl() {
    let config = EngineConfig {
        gc_ttl: TTL,
        // Passes run only when the test asks (hooks::reclaim_now).
        reclaim_every: Duration::from_secs(3600),
        ..engine_config()
    };
    let cluster = LocalCluster::with_config(&spread_splits(), config);
    let client: &dyn RangeClient = &*cluster;
    let gw = gateway(&cluster, 1);
    let mut s = gw.admin.sql_session();
    for sql in DDL {
        run(&mut s, sql);
    }
    let [d, d_a, o, o_a, o_b, r] = ids(&gw.admin);
    let rows = |f: &dyn Fn(i64) -> String| (1..=600).map(f).collect::<Vec<_>>().join(",");
    run(
        &mut s,
        &format!(
            "INSERT INTO d VALUES {}",
            rows(&|i| format!("({i}, {})", i % 50))
        ),
    );
    run(
        &mut s,
        &format!(
            "INSERT INTO o VALUES {}",
            rows(&|i| format!("({i}, {}, {i})", i % 50))
        ),
    );
    run(
        &mut s,
        &format!("INSERT INTO r VALUES {}", rows(&|i| format!("({i})"))),
    );

    let keys = |ks, id| hooks::raw_keys(client, ks, id);
    let ranges = |space, ks, id| {
        keys(ks, id)
            .iter()
            .map(|raw| cluster.range_of(space, codec::split_versioned(raw).unwrap().0))
            .collect::<BTreeSet<_>>()
    };
    assert_eq!(ranges(RangeSpace::Rows, Keyspace::Rows, d).len(), 3);
    assert_eq!(ranges(RangeSpace::Index, Keyspace::Index, d_a).len(), 2);
    assert_eq!(ranges(RangeSpace::Index, Keyspace::Index, o_a).len(), 2);
    let others = || {
        [
            (Keyspace::Rows, o),
            (Keyspace::Index, o_b),
            (Keyspace::Rows, r),
        ]
        .map(|(ks, id)| keys(ks, id))
    };
    let before = others();
    assert!(before.iter().all(|k| k.len() >= 600));
    let dropped = || {
        [
            (Keyspace::Rows, d),
            (Keyspace::Index, d_a),
            (Keyspace::Index, o_a),
        ]
        .map(|(ks, id)| keys(ks, id).len())
    };

    run(&mut s, "DROP TABLE d");
    run(&mut s, "DROP INDEX o_a");
    let after_drop = client.oracle().next(1).unwrap().0 >> 16;
    let expired = after_drop + TTL.as_millis() as u64 + CLOCK_OFFSET_MS + 1;
    // Dropped less than the TTL ago: kept.
    let held = dropped();
    assert!(held.iter().all(|n| *n >= 600), "{held:?}");
    assert_eq!(
        hooks::reclaim_at(client, after_drop).unwrap(),
        ReclaimStats::default()
    );
    assert_eq!(dropped(), held);

    // One delete per dropped object and range: d's three row ranges, two each for d_a, o_a.
    assert_eq!(
        hooks::reclaim_at(client, expired).unwrap(),
        ReclaimStats {
            tables: 3,
            indexes: 4
        }
    );
    assert_eq!(dropped(), [0, 0, 0]);
    // Other tables and indexes, and live r, are untouched.
    assert_eq!(others(), before);
    assert_eq!(
        hooks::reclaim_at(client, expired).unwrap(),
        ReclaimStats::default()
    );
    let expected: Vec<Vec<i64>> = (0..12).map(|k| vec![7 + 50 * k]).collect();
    assert_eq!(
        ints(&run(&mut s, "SELECT id FROM o WHERE a = 7 ORDER BY id")),
        expected
    );

    // A read below the TTL fails instead of returning a reclaimed (empty) table.
    let middle = range_key(
        RangeSpace::Rows,
        &[&d.to_be_bytes()[..], &pk_bytes(&[Value::Int(300)]).unwrap()].concat(),
    );
    let desc = cluster.descriptor_for(&middle).unwrap();
    let error = client
        .read(ReadRequest {
            range: desc.id,
            epoch: desc.epoch,
            space: RangeSpace::Rows,
            start: Bound::Unbounded,
            end: Bound::Unbounded,
            dir: ReadDirection::Forward,
            ts: crate::storage::range_api::Timestamp(1),
            page_bytes: 1 << 20,
        })
        .unwrap_err();
    assert!(matches!(error, RangeError::Conflict(_)), "{error:?}");
}

#[test]
fn reclaim_keeps_the_clock_offset_margin() {
    let config = EngineConfig {
        gc_ttl: TTL,
        reclaim_every: Duration::from_secs(3600),
        ..engine_config()
    };
    let cluster = LocalCluster::with_config(&[], config);
    let client: &dyn RangeClient = &*cluster;
    let gw = gateway(&cluster, 1);
    let mut s = gw.admin.sql_session();
    for sql in DDL {
        run(&mut s, sql);
    }
    let [d, ..] = ids(&gw.admin);
    run(&mut s, "INSERT INTO d VALUES (1, 1)");
    // Bound the DROP timestamp, without depending on how quickly the test machine runs.
    let before = client.oracle().next(1).unwrap().0 >> 16;
    run(&mut s, "DROP TABLE d");
    let after = client.oracle().next(1).unwrap().0 >> 16;
    let ttl_ms = TTL.as_millis() as u64;
    assert_eq!(
        hooks::reclaim_at(client, before + ttl_ms + CLOCK_OFFSET_MS - 1).unwrap(),
        ReclaimStats::default()
    );
    assert!(!hooks::raw_keys(client, Keyspace::Rows, d).is_empty());
    let reclaimed = hooks::reclaim_at(client, after + ttl_ms + CLOCK_OFFSET_MS + 1).unwrap();
    assert_eq!(reclaimed.tables, 1);
    assert!(hooks::raw_keys(client, Keyspace::Rows, d).is_empty());
    assert_eq!(hooks::error_count(client), 0);
}
