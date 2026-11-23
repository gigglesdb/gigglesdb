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
        Backend::V2(_) => f.admin().test_v2(),
        _ => unreachable!(),
    };
    let parts = f.parts();
    let table = engine::catalog::TableEntry {
        primary_index_birth: None,
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
    let other = db.child(Holder::Admin).unwrap();
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
        primary_index_birth: None,
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
        primary_index_birth: None,
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

#[test]
fn partial_index_descriptor_and_feature_commit_together() {
    let mut f = Fixture::new();
    f.execute("CREATE TABLE t(id int PRIMARY KEY,v int,f boolean)");
    f.execute("INSERT INTO t VALUES (1,1,true),(2,1,false)");
    f.execute("CREATE INDEX iv ON t(v)");
    let bit = engine::meta::PARTIAL_INDEXES;
    let features = |f: &Fixture| {
        let bytes = f.db().kv.get(Keyspace::Meta, b"features").unwrap().unwrap();
        u64::from_be_bytes(bytes.try_into().unwrap())
    };
    {
        let mut session = f.admin().sql_session();
        for sql in ["BEGIN", "CREATE UNIQUE INDEX active ON t(v) WHERE f", "ROLLBACK"] {
            session.execute(sql, &[], None).unwrap();
        }
    }
    assert_eq!(features(&f) & bit, 0);
    {
        let mut session = f.admin().sql_session();
        for sql in ["BEGIN", "CREATE UNIQUE INDEX active ON t(v) WHERE f", "DROP INDEX active", "COMMIT"] {
            session.execute(sql, &[], None).unwrap();
        }
    }
    assert_eq!(features(&f) & bit, 0);
    f.execute("CREATE UNIQUE INDEX active ON t(v) WHERE f");
    assert_eq!(features(&f) & bit, bit);
    let parts = f.parts();
    let partial = parts.indexes.iter().find(|d| d.predicate.is_some()).unwrap();
    let mut ordinary = partial.clone();
    ordinary.predicate = None;
    let ordinary = engine::indexes::encode_descriptor(&ordinary).unwrap();
    let wire = engine::indexes::encode_descriptor(partial).unwrap();
    assert_eq!(&wire[..ordinary.len()], ordinary);
    let mut suffix = 1u64.to_be_bytes().to_vec();
    suffix.push(b'f');
    suffix.extend(1u64.to_be_bytes());
    suffix.extend(parts.tables["t"].columns[2].id.to_be_bytes());
    assert_eq!(&wire[ordinary.len()..], suffix);
    let mut batch = WriteBatch::default();
    batch.put(Keyspace::Meta, b"features".to_vec(), (features(&f) & !bit).to_be_bytes().to_vec());
    f.db().kv.write(batch, Durability::Sync).unwrap();
    f.admin.take();
    assert!(matches!(Admin::open(&f.path, Limits::default()), Err(CoreError::Corrupt(_))));
}

#[test]
fn temporal_and_partial_index_features_publish_independently() {
    use engine::meta::{PARTIAL_INDEXES, TEMPORAL_INDEXES};

    for (key, predicate, expected) in [
        ("g", " WHERE active", PARTIAL_INDEXES),
        ("dt", "", TEMPORAL_INDEXES),
        ("dt", " WHERE active", PARTIAL_INDEXES | TEMPORAL_INDEXES),
    ] {
        let mut f = Fixture::new();
        f.execute("CREATE TABLE t(id int PRIMARY KEY, g int, dt timestamp(3), active boolean)");
        let features = |f: &Fixture| {
            let bytes = f.db().kv.get(Keyspace::Meta, b"features").unwrap().unwrap();
            u64::from_be_bytes(bytes.try_into().unwrap()) & (PARTIAL_INDEXES | TEMPORAL_INDEXES)
        };
        assert_eq!(features(&f), 0);
        let create = format!("CREATE UNIQUE INDEX t_ix ON t({key}){predicate}");
        {
            let mut session = f.admin().sql_session();
            for statements in [
                vec!["BEGIN", &create, "ROLLBACK"],
                vec!["BEGIN", "SAVEPOINT before_index", &create, "ROLLBACK TO before_index", "COMMIT"],
                vec!["BEGIN", &create, "DROP INDEX t_ix", "COMMIT"],
            ] {
                for sql in statements {
                    session.execute(sql, &[], None).unwrap();
                }
                assert_eq!(features(&f), 0);
            }
        }
        assert_eq!(features(&f), 0);
        f.execute(&create);
        assert_eq!(features(&f), expected);
        f.reopen();
        assert_eq!(features(&f), expected);
    }
}


#[test]
fn partial_temporal_catalog_publication_matches_durable_reload() {
    let mut f = Fixture::new();
    for sql in [
        "CREATE TABLE t(id int PRIMARY KEY, spare int, live boolean, dt timestamp)",
        "CREATE UNIQUE INDEX ix ON t(dt) WHERE live",
        "CREATE TABLE unrelated(id int PRIMARY KEY)",
        "ALTER TABLE t RENAME COLUMN live TO active",
        "ALTER TABLE t DROP COLUMN spare",
    ] {
        f.execute(sql);
        f.db().catalog().assert_current_matches_reload();
    }
    f.execute("INSERT INTO t VALUES (1,true,'2026-01-01'),(2,false,'2026-01-01')");
    {
        let mut session = f.admin().sql_session();
        for sql in [
            "BEGIN",
            "SAVEPOINT replace_index",
            "DROP INDEX ix",
            "CREATE UNIQUE INDEX ix ON t(dt) WHERE NOT active",
            "ROLLBACK TO replace_index",
            "COMMIT",
        ] {
            session.execute(sql, &[], None).unwrap();
        }
    }
    f.db().catalog().assert_current_matches_reload();
    f.execute("INSERT INTO t VALUES (3,false,'2026-01-01')");
    f.execute("DELETE FROM t WHERE id=3");
    f.execute("DROP INDEX ix");
    f.execute("CREATE UNIQUE INDEX ix ON t(dt) WHERE NOT active");
    f.db().catalog().assert_current_matches_reload();
    f.reopen();
    f.execute("INSERT INTO t VALUES (4,true,'2026-01-01')");
    assert_eq!(
        rows(&f, "SELECT id FROM t WHERE dt >= '2026-01-01' ORDER BY id"),
        [vec![Value::Int(1)], vec![Value::Int(2)], vec![Value::Int(4)]],
    );
}

#[test]
fn f1_native_predicate_probe() {
    for extra in [0, 48] {
        let f = Fixture::new();
        f.execute("CREATE TABLE t(id int PRIMARY KEY, g int, active boolean)");
        for i in 0..extra {
            f.execute(&format!("CREATE TABLE extra_{i}(id int PRIMARY KEY, x int, y text)"));
        }
        f.execute("CREATE INDEX t_partial ON t(g) WHERE active");
        let parts = f.parts();
        let descriptor = &parts.indexes[0];
        let table = TableEntry {
            def: parts.tables["t"].clone(),
            heap: false,
            primary_index_birth: None,
            columns_ext: BTreeMap::new(),
        };
        let mut txn = f.db().begin(Isolation::RepeatableRead);
        let start = std::time::Instant::now();
        for i in 1..=400 {
            f.db().rows.insert(&mut txn, &table, vec![Value::Int(i), Value::Int(i), Value::Bool(true)], None, &|| false).unwrap();
        }
        let writes = start.elapsed();
        let start = std::time::Instant::now();
        let mut seen = 0;
        let mut after = None;
        loop {
            after = engine::indexes::visit_page(&mut txn, &table, descriptor, Bound::Unbounded, Bound::Unbounded,
                after, &mut |_, _| { seen += 1; Ok(()) }, 4).unwrap();
            if after.is_none() { break; }
        }
        assert_eq!(seen, 400);
        eprintln!(
            "F1 native {extra} extra tables: 400 writes {writes:?}, 101 pages {:?}",
            start.elapsed(),
        );

        // An in-transaction catalog change must invalidate the old binding.
        let savepoint = txn.savepoint();
        let mut changed = descriptor.clone();
        changed.predicate.as_mut().unwrap().sql = "not active".into();
        txn.put(
            Keyspace::Catalog,
            engine::indexes::descriptor_key(changed.id),
            engine::indexes::encode_descriptor(&changed).unwrap(),
        ).unwrap();
        f.db().rows.insert(
            &mut txn,
            &table,
            vec![Value::Int(401), Value::Int(401), Value::Bool(false)],
            None,
            &|| false,
        ).unwrap();
        txn.rollback_to(&savepoint).unwrap();
        let rows = engine::indexes::scan(
            &mut txn, &table, descriptor, Bound::Unbounded, Bound::Unbounded,
        ).unwrap();
        assert_eq!(rows.len(), 400);
    }
}
