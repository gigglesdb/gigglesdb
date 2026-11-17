//! R5-102 (GD-0075 Phase 11): CREATE INDEX through a gateway, backfilled in chunk transactions
//! over rows and index entries split across several ranges, equals a single-node v2 build.
use super::*;
use crate::relational::{SqlResult, Value, split_key};
use crate::storage::engine::codec::{self, pk_bytes};
use crate::storage::engine::kv::{Direction, Keyspace};
use crate::storage::engine::txn::Isolation;
use crate::storage::v2_sql::USED_INDEX_SCAN;
use std::collections::BTreeMap;
use std::ops::Bound;

const ROWS: i64 = 1_200;
const TABLE: &str = "CREATE TABLE ib(id INT PRIMARY KEY, a INT, b TEXT, u INT)";
/// (name, statement, the leading value a split key is placed before).
type IndexSpec = (&'static str, &'static str, fn() -> Value);
const INDEXES: [IndexSpec; 3] = [
    ("ib_a", "CREATE INDEX ib_a ON ib(a)", || Value::Int(3)),
    ("ib_ba", "CREATE INDEX ib_ba ON ib(b, a)", || {
        Value::Text("k2".into())
    }),
    ("ib_u", "CREATE UNIQUE INDEX ib_u ON ib(u)", || {
        Value::Int(1_800)
    }),
];
/// Every query's leading predicate column is the first column of one index above.
const QUERIES: [&str; 4] = [
    "SELECT id, a, b, u FROM ib WHERE a = 3 ORDER BY id",
    "SELECT id, a, b FROM ib WHERE b = 'k1' ORDER BY id",
    "SELECT id, u FROM ib WHERE u >= 1500 AND u < 2400 ORDER BY id",
    "SELECT id FROM ib WHERE b >= 'k2' AND a = 5 ORDER BY id",
];

/// The same 1,200 rows for both databases: NULLs and duplicates in a and b, NULLs in u.
fn load(session: &mut crate::storage::SqlSession) {
    let opt = |null: bool, value: String| if null { "NULL".to_string() } else { value };
    for start in (1..=ROWS).step_by(200) {
        let values: Vec<String> = (start..start + 200)
            .map(|id| {
                let a = opt(id % 11 == 0, (id % 7).to_string());
                let b = opt(id % 5 == 0, format!("'k{}'", id % 4));
                let u = opt(id % 13 == 0, (id * 3).to_string());
                format!("({id}, {a}, {b}, {u})")
            })
            .collect();
        run(
            session,
            &format!("INSERT INTO ib VALUES {}", values.join(",")),
        );
    }
}

fn v2(admin: &Admin) -> &V2Db {
    match &admin.inner.backend {
        Backend::V2(db) => db,
        _ => panic!("expected a v2 backend"),
    }
}

/// The table id and the ids of the named indexes, from the committed catalog.
fn ids(admin: &Admin, names: &[&str]) -> (u64, Vec<Option<u64>>) {
    let db = v2(admin);
    let mut txn = db.begin(Isolation::ReadCommitted);
    let parts = db.catalog().parts(&mut txn).unwrap();
    let table = parts.tables.values().find(|t| t.name == "ib").unwrap().id;
    let index = |name: &str| {
        parts
            .indexes
            .iter()
            .find(|d| d.table == table && split_key(&d.name).1 == name)
            .map(|d| d.id)
    };
    (table, names.iter().map(|n| index(n)).collect())
}

/// Committed physical entries under `prefix` (an object id), with the id stripped.
fn entries(admin: &Admin, ks: Keyspace, prefix: u64) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let db = v2(admin);
    let mut txn = db.begin(Isolation::ReadCommitted);
    let (lo, hi) = (prefix.to_be_bytes(), (prefix + 1).to_be_bytes());
    txn.scan(
        ks,
        Bound::Included(&lo[..]),
        Bound::Excluded(&hi[..]),
        Direction::Forward,
        None,
    )
    .unwrap()
    .map(|pair| pair.map(|(key, value)| (key[8..].to_vec(), value)))
    .collect::<Result<_, _>>()
    .unwrap()
}

fn text(result: &SqlResult) -> Vec<String> {
    result
        .rows
        .iter()
        .map(|r| format!("{:?}", r.values))
        .collect()
}

/// Each query's rows and whether it read through a secondary index.
fn answers(session: &mut crate::storage::SqlSession) -> Vec<(Vec<String>, bool)> {
    QUERIES
        .iter()
        .map(|sql| {
            USED_INDEX_SCAN.set(false);
            let rows = text(&run(session, sql));
            (rows, USED_INDEX_SCAN.get())
        })
        .collect()
}

/// Splits that put the table's rows in three ranges and each index's entries in two: the
/// object ids come from a probe cluster that ran the same DDL (ids are leased in order).
fn spread_splits() -> Vec<Vec<u8>> {
    let probe = LocalCluster::new(&[]);
    let gateway = gateway(&probe, 1);
    let mut s = gateway.admin.sql_session();
    run(&mut s, TABLE);
    for (_, sql, _) in INDEXES {
        run(&mut s, sql);
    }
    let (table, indexes) = ids(&gateway.admin, &INDEXES.map(|(name, _, _)| name));
    let at =
        |space, id: u64, suffix: &[u8]| range_key(space, &[&id.to_be_bytes()[..], suffix].concat());
    let mut splits = vec![
        at(RangeSpace::Rows, table, &[]),
        at(RangeSpace::Rows, table + 1, &[]),
    ];
    for pk in [400, 800] {
        splits.push(at(
            RangeSpace::Rows,
            table,
            &pk_bytes(&[Value::Int(pk)]).unwrap(),
        ));
    }
    for (id, (_, _, split)) in indexes.into_iter().zip(INDEXES) {
        let id = id.unwrap();
        let tuple = codec::index_tuple_bytes(&[split()], &[codec::Direction::Asc]).unwrap();
        splits.push(at(RangeSpace::Index, id, &[]));
        splits.push(at(RangeSpace::Index, id, &tuple));
        splits.push(at(RangeSpace::Index, id + 1, &[]));
    }
    splits.sort();
    splits.dedup();
    splits
}

#[test]
fn create_index_across_ranges_equals_a_single_node_build() {
    let cluster = LocalCluster::new(&spread_splits());
    let a = gateway(&cluster, 1);
    let mut sa = a.admin.sql_session();
    let dir = std::env::temp_dir().join(format!("index-build-{:016x}", rand_id()));
    let _cleanup = Cleanup(dir.clone());
    let single = Admin::create_engine(&dir, Limits::default(), crate::Engine::V2).unwrap();
    let mut s1 = single.sql_session();
    for session in [&mut sa, &mut s1] {
        run(session, TABLE);
        load(session);
        for (_, sql, _) in INDEXES {
            run(session, sql);
        }
    }
    let names = INDEXES.map(|(name, _, _)| name);
    let (table, cluster_ids) = ids(&a.admin, &names);
    let (single_table, single_ids) = ids(&single, &names);
    // The rows really are spread: at least three row ranges.
    let rows = entries(&a.admin, Keyspace::Rows, table);
    assert_eq!(rows.len(), ROWS as usize);
    let row_ranges: BTreeSet<_> = rows
        .keys()
        .map(|k| cluster.range_of(RangeSpace::Rows, &[&table.to_be_bytes()[..], k].concat()))
        .collect();
    assert!(row_ranges.len() >= 3, "{row_ranges:?}");
    assert_eq!(
        entries(&single, Keyspace::Rows, single_table).len(),
        ROWS as usize
    );
    for (name, (id, single_id)) in names.iter().zip(cluster_ids.iter().zip(&single_ids)) {
        let (id, single_id) = (id.unwrap(), single_id.unwrap());
        let built = entries(&a.admin, Keyspace::Index, id);
        let ranges: BTreeSet<_> = built
            .keys()
            .map(|k| cluster.range_of(RangeSpace::Index, &[&id.to_be_bytes()[..], k].concat()))
            .collect();
        assert!(ranges.len() >= 2, "{name}: {ranges:?}");
        // One entry per row (NULL tuples included), equal to the single-node build.
        assert_eq!(built.len(), ROWS as usize, "{name}");
        assert!(
            built == entries(&single, Keyspace::Index, single_id),
            "{name}"
        );
    }
    // Queries read through the indexes and return the same rows on both, also on a gateway
    // that attached after the DDL.
    let b = gateway(&cluster, 2);
    let mut sb = b.admin.sql_session();
    let expected = answers(&mut s1);
    assert!(
        expected
            .iter()
            .all(|(rows, indexed)| *indexed && !rows.is_empty()),
        "{expected:?}"
    );
    assert_eq!(answers(&mut sa), expected);
    assert_eq!(answers(&mut sb), expected);

    // A UNIQUE build over duplicate values fails the same way on both and publishes nothing.
    let mut failures = Vec::new();
    for (admin, session) in [(&a.admin, &mut sa), (&single, &mut s1)] {
        let error = session
            .execute_at("CREATE UNIQUE INDEX ib_dup ON ib(a)", &[], None, &clock())
            .unwrap_err();
        failures.push(format!("{error:?}"));
        let (_, after) = ids(admin, &["ib_a", "ib_ba", "ib_u", "ib_dup"]);
        assert!(
            after[..3].iter().all(Option::is_some) && after[3].is_none(),
            "{after:?}"
        );
        // Duplicates of a are still accepted: no unique index on a is enforced.
        run(session, "INSERT INTO ib VALUES (5000, 3, 'k1', 15000)");
    }
    assert!(failures[0].contains("Duplicate"), "{failures:?}");
    assert_eq!(failures[0], failures[1]);
    assert_eq!(answers(&mut sb), answers(&mut s1));
    // The existing unique index still rejects a duplicate on both.
    for session in [&mut sb, &mut s1] {
        let error = session
            .execute_at(
                "INSERT INTO ib VALUES (5001, 1, 'k1', 15000)",
                &[],
                None,
                &clock(),
            )
            .unwrap_err();
        assert!(format!("{error:?}").contains("Duplicate"), "{error:?}");
    }
}

struct Cleanup(std::path::PathBuf);
impl Drop for Cleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn rand_id() -> u64 {
    let mut bytes = [0; 8];
    getrandom::fill(&mut bytes).unwrap();
    u64::from_be_bytes(bytes)
}
