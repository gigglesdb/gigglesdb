//! F5 SQL parity, MVCC maintenance, concurrent UNIQUE and reopen witnesses.
use super::*;
use relational::Value;
use std::{sync::mpsc, thread, time::Duration};

fn rows(f: &Fixture, sql: &str) -> Vec<Vec<Value>> {
    f.admin()
        .sql_session()
        .execute(sql, &[], None)
        .unwrap()
        .rows
        .into_iter()
        .map(|r| r.values)
        .collect()
}
fn index_entries(f: &Fixture) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut txn = f.db().begin(Isolation::ReadCommitted);
    txn.scan(
        Keyspace::Index,
        Bound::Unbounded,
        Bound::Unbounded,
        Direction::Forward,
        None,
    )
    .unwrap()
    .collect::<Result<_, _>>()
    .unwrap()
}
#[test]
fn parity_for_indexes_constraints_and_rekey() {
    let scripts = [
        "CREATE TABLE t (id bigint PRIMARY KEY, v bigint)",
        "INSERT INTO t VALUES (1,10),(2,20),(3,NULL),(4,NULL)",
        "CREATE UNIQUE INDEX uv ON t(v)",
        "CREATE INDEX iv ON t(v)",
        "SELECT id FROM t WHERE v=10",
        "SELECT id FROM t WHERE v>=10 AND v<=20 ORDER BY v",
        "UPDATE t SET v=30 WHERE id=2",
        "DELETE FROM t WHERE id=1",
        "INSERT INTO t VALUES(5,10)",
        "INSERT INTO t VALUES(6,30)",
        "ALTER TABLE t ADD CONSTRAINT uq UNIQUE(id,v)",
        "DROP INDEX iv",
        "DROP INDEX IF EXISTS missing",
        "CREATE TABLE c(id bigint PRIMARY KEY,v bigint UNIQUE,w bigint,UNIQUE(w))",
        "INSERT INTO c VALUES(1,2,3)",
        "INSERT INTO c VALUES(2,2,4)",
        "INSERT INTO c VALUES(2,4,3)",
        "CREATE TABLE h(v bigint)",
        "INSERT INTO h VALUES(7),(7)",
        "ALTER TABLE h ADD CONSTRAINT hp PRIMARY KEY(v)",
        "DELETE FROM h WHERE v=7",
        "INSERT INTO h VALUES(8),(9)",
        "CREATE INDEX hv ON h(v)",
        "ALTER TABLE h ADD CONSTRAINT hp PRIMARY KEY(v)",
        "INSERT INTO h VALUES(8)",
        "SELECT v FROM h ORDER BY v",
    ];
    let mut f = Fixture::new();
    let legacy_path = f.path.with_extension("legacy");
    let legacy = Admin::create_engine(&legacy_path, Limits::default(), Engine::Legacy).unwrap();
    for sql in scripts {
        let normalize = |r: Result<relational::SqlResult, CoreError>| {
            r.map(|r| {
                (
                    r.rows.into_iter().map(|row| row.values).collect::<Vec<_>>(),
                    r.affected,
                )
            })
        };
        let old = normalize(legacy.sql_session().execute(sql, &[], None));
        let new = normalize(f.admin().sql_session().execute(sql, &[], None));
        assert_eq!(new, old, "{sql}");
    }
    let parts = f.parts();
    let entries = index_entries(&f);
    assert!(!entries.is_empty());
    f.reopen();
    assert_eq!(f.parts(), parts);
    assert_eq!(index_entries(&f), entries);
    assert_eq!(
        rows(&f, "SELECT v FROM h ORDER BY v"),
        vec![vec![Value::Int(8)], vec![Value::Int(9)]]
    );
    assert!(matches!(
        f.admin()
            .sql_session()
            .execute("INSERT INTO t VALUES(7,30)", &[], None),
        Err(CoreError::Sql(SqlError::Duplicate))
    ));
    drop(legacy);
    fs::remove_dir_all(legacy_path).unwrap();
}
#[test]
fn index_versions_rollback_and_null_distinctness() {
    let f = Fixture::new();
    f.execute("CREATE TABLE t(id bigint PRIMARY KEY,v text UNIQUE)");
    f.execute("INSERT INTO t VALUES(1,'a'),(2,NULL),(3,NULL)");
    let initial = index_entries(&f);
    assert_eq!(initial.len(), 3);
    let mut reader = f.db().begin(Isolation::RepeatableRead);
    let snapshot = reader
        .scan(
            Keyspace::Index,
            Bound::Unbounded,
            Bound::Unbounded,
            Direction::Forward,
            None,
        )
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    f.execute("UPDATE t SET v='b' WHERE id=1");
    f.execute("DELETE FROM t WHERE id=2");
    assert_eq!(index_entries(&f).len(), 2);
    let again = reader
        .scan(
            Keyspace::Index,
            Bound::Unbounded,
            Bound::Unbounded,
            Direction::Forward,
            None,
        )
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(snapshot, again);
    let before = index_entries(&f);
    let mut session = f.admin().sql_session();
    session.execute("BEGIN", &[], None).unwrap();
    session
        .execute("INSERT INTO t VALUES(4,'c')", &[], None)
        .unwrap();
    session.execute("ROLLBACK", &[], None).unwrap();
    assert_eq!(index_entries(&f), before);
    assert!(
        f.admin()
            .sql_session()
            .execute("CREATE UNIQUE INDEX bad ON t(v)", &[], None)
            .is_ok()
    );
}
#[test]
fn native_unique_waits_then_returns_duplicate() {
    let f = Fixture::new();
    f.execute("CREATE TABLE t(id bigint PRIMARY KEY,v bigint UNIQUE)");
    let db = match &f.admin().inner.backend {
        Backend::V2(db) => db.clone(),
        _ => unreachable!(),
    };
    let parts = f.parts();
    let table = engine::catalog::TableEntry {
        def: parts.tables["t"].clone(),
        heap: false,
        columns_ext: BTreeMap::new(),
    };
    let mut first = db.begin(Isolation::ReadCommitted);
    db.rows
        .insert(
            &mut first,
            &table,
            vec![Value::Int(1), Value::Int(10)],
            None,
            &|| false,
        )
        .unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (result_tx, result_rx) = mpsc::channel();
    let other = db.clone();
    let worker = thread::spawn(move || {
        let mut second = other.begin(Isolation::ReadCommitted);
        started_tx.send(()).unwrap();
        let result = other.rows.insert(
            &mut second,
            &table,
            vec![Value::Int(2), Value::Int(10)],
            None,
            &|| false,
        );
        result_tx.send(result).unwrap();
    });
    started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(result_rx.recv_timeout(Duration::from_millis(100)).is_err());
    db.catalog().commit_ddl(first).unwrap();
    assert!(matches!(
        result_rx.recv_timeout(Duration::from_secs(10)).unwrap(),
        Err(CoreError::Sql(SqlError::Duplicate))
    ));
    worker.join().unwrap();
    assert_eq!(index_entries(&f).len(), 1);
}
#[test]
fn feature_guard_and_descriptor_wire_are_append_only() {
    let mut f = Fixture::new();
    f.execute("CREATE TABLE t(id bigint PRIMARY KEY,v bigint)");
    let before = f.catalog_bytes();
    f.execute("INSERT INTO t VALUES(1,2)");
    f.execute("CREATE INDEX iv ON t(v)");
    let descriptor = &f.parts().indexes[0];
    assert_eq!(engine::indexes::descriptor_key(descriptor.id)[0], 0x10);
    let bytes = engine::indexes::encode_descriptor(descriptor).unwrap();
    assert_eq!(bytes[0], 1);
    assert_eq!(
        engine::indexes::decode_descriptor(&bytes).unwrap(),
        *descriptor
    );
    let after = f.catalog_bytes();
    for record in before {
        assert!(after.contains(&record));
    }
    let mut batch = WriteBatch::default();
    let bits = f.db().kv.get(Keyspace::Meta, b"features").unwrap().unwrap();
    let bits = u64::from_be_bytes(bits.try_into().unwrap());
    assert_ne!(bits & 4, 0);
    batch.put(
        Keyspace::Meta,
        b"features".to_vec(),
        (bits & !4).to_be_bytes().to_vec(),
    );
    f.db().kv.write(batch, Durability::Sync).unwrap();
    f.admin.take();
    assert!(matches!(
        Admin::open(&f.path, Limits::default()),
        Err(CoreError::Corrupt(_))
    ));
}

#[test]
fn point_and_range_use_mvcc_index_entries() {
    let f = Fixture::new();
    f.execute("CREATE TABLE t(id bigint PRIMARY KEY,v bigint)");
    f.execute("INSERT INTO t VALUES(1,10),(2,20),(3,20),(4,30)");
    f.execute("CREATE INDEX iv ON t(v)");
    let parts = f.parts();
    let table = TableEntry {
        def: parts.tables["t"].clone(),
        heap: false,
        columns_ext: BTreeMap::new(),
    };
    let descriptor = &parts.indexes[0];
    let tuple = |n| codec::index_tuple_bytes(&[Value::Int(n)], &[codec::Direction::Asc]).unwrap();
    let mut txn = f.db().begin(Isolation::RepeatableRead);
    let point = engine::indexes::scan(
        &mut txn,
        &table,
        descriptor,
        Bound::Included(tuple(20)),
        Bound::Included(tuple(20)),
    )
    .unwrap();
    assert_eq!(
        point
            .iter()
            .map(|(_, r)| r.values[0].clone())
            .collect::<Vec<_>>(),
        vec![Value::Int(2), Value::Int(3)]
    );
    f.execute("UPDATE t SET v=40 WHERE id=2");
    let old = engine::indexes::scan(
        &mut txn,
        &table,
        descriptor,
        Bound::Included(tuple(20)),
        Bound::Included(tuple(20)),
    )
    .unwrap();
    assert_eq!(old, point);
    let mut fresh = f.db().begin(Isolation::ReadCommitted);
    let range = engine::indexes::scan(
        &mut fresh,
        &table,
        descriptor,
        Bound::Excluded(tuple(10)),
        Bound::Included(tuple(30)),
    )
    .unwrap();
    assert_eq!(
        range
            .iter()
            .map(|(_, r)| r.values[0].clone())
            .collect::<Vec<_>>(),
        vec![Value::Int(3), Value::Int(4)]
    );
}

#[test]
fn sql_point_and_range_read_through_index_keyspace() {
    let f = Fixture::new();
    f.execute("CREATE TABLE t(id bigint PRIMARY KEY,v bigint)");
    f.execute("INSERT INTO t VALUES(1,10),(2,20),(3,30)");
    f.execute("CREATE INDEX iv ON t(v)");
    for (sql, expected) in [
        ("SELECT id FROM t WHERE v=20", vec![vec![Value::Int(2)]]),
        (
            "SELECT id FROM t WHERE v>10 AND v<=30 ORDER BY id",
            vec![vec![Value::Int(2)], vec![Value::Int(3)]],
        ),
    ] {
        USED_INDEX_SCAN.set(false);
        assert_eq!(rows(&f, sql), expected);
        assert!(USED_INDEX_SCAN.get(), "{sql}");
    }
}

#[test]
fn native_unique_wait_preserves_row_store_deadline() {
    let f = Fixture::new();
    f.execute("CREATE TABLE t(id bigint PRIMARY KEY,v bigint UNIQUE)");
    let table = TableEntry {
        def: f.parts().tables["t"].clone(),
        heap: false,
        columns_ext: BTreeMap::new(),
    };
    let mut first = f.db().begin(Isolation::ReadCommitted);
    f.db()
        .rows
        .insert(
            &mut first,
            &table,
            vec![Value::Int(1), Value::Int(10)],
            None,
            &|| false,
        )
        .unwrap();
    let mut second = f.db().begin(Isolation::ReadCommitted);
    let result = f.db().rows.insert(
        &mut second,
        &table,
        vec![Value::Int(2), Value::Int(10)],
        Some(std::time::Instant::now() + Duration::from_millis(100)),
        &|| false,
    );
    assert!(matches!(result, Err(CoreError::Busy)));
    assert!(
        f.db()
            .rows
            .get(&mut second, &table, &[Value::Int(2)])
            .unwrap()
            .is_none()
    );
}
