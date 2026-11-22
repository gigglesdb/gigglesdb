//! Large-table regressions: fixtures bypass the SQL statement-image limits.
use super::*;
use relational::Value::Int;

pub(super) fn large_table(count: usize, duplicates: bool, indexed: bool) -> Fixture {
    let f = Fixture::new();
    f.execute("CREATE TABLE t (id bigint PRIMARY KEY, v bigint)");
    if indexed {
        f.execute("CREATE INDEX existing_idx ON t(v)");
    }
    let parts = f.parts();
    let entry = TableEntry {
        primary_index_birth: None,
        def: parts.tables["t"].clone(),
        heap: false,
        columns_ext: BTreeMap::new(),
    };
    // Use the real row-store transaction publisher, in bounded fixture batches.
    // This keeps MVCC timestamps, row-version reservations and index entries valid.
    for start in (0..count).step_by(200) {
        let mut txn = f.db().begin(Isolation::ReadCommitted);
        for id in start..(start + 200).min(count) {
            f.db()
                .rows
                .insert(
                    &mut txn,
                    &entry,
                    vec![Int(id as i64), Int(if duplicates { 7 } else { id as i64 })],
                    None,
                    &|| false,
                )
                .unwrap();
        }
        txn.commit().unwrap();
    }
    f
}

#[test]
fn create_index_over_20_000_sql_inserted_rows() {
    let f = Fixture::new();
    f.execute("CREATE TABLE t (id bigint PRIMARY KEY, v bigint)");
    let mut session = f.admin().sql_session();
    for start in (0..20_000).step_by(200) {
        session.execute("BEGIN", &[], None).unwrap();
        let values = (start..start + 200)
            .map(|id| format!("({id},{id})"))
            .collect::<Vec<_>>()
            .join(",");
        session
            .execute(&format!("INSERT INTO t VALUES {values}"), &[], None)
            .unwrap();
        session.execute("COMMIT", &[], None).unwrap();
    }
    session
        .execute("CREATE INDEX large_idx ON t(v)", &[], None)
        .unwrap();
}

#[test]
fn create_index_over_200_000_rows() {
    let mut f = large_table(200_000, false, false);
    f.execute("CREATE INDEX large_idx ON t(v)");
    f.reopen();
    assert_eq!(f.parts().indexes.len(), 1);
    let mut txn = f.db().begin(Isolation::ReadCommitted);
    assert_eq!(
        txn.scan(
            Keyspace::Index,
            Bound::Unbounded,
            Bound::Unbounded,
            Direction::Forward,
            None,
        )
        .unwrap()
        .try_fold(0, |count, pair| pair.map(|_| count + 1))
        .unwrap(),
        200_000
    );
}

#[test]
fn large_ddl_with_and_without_existing_index() {
    // Whole-table UPDATE/DELETE above 1,024 rows still hit legacy statement
    // image limits; that known DML gap is outside this DDL regression set.
    let cases = [
        "ALTER TABLE t ADD COLUMN d bigint DEFAULT 42",
        "ALTER TABLE t ALTER COLUMN v SET NOT NULL",
        "ALTER TABLE t ADD CONSTRAINT ck CHECK (v >= 0)",
        "DROP INDEX IF EXISTS existing_idx",
        "DROP TABLE t",
    ];
    for indexed in [false, true] {
        let mut f = large_table(200_000, false, indexed);
        for sql in cases {
            let before = f.parts();
            let mut session = f.admin().sql_session();
            session.execute("BEGIN", &[], None).unwrap();
            session
                .execute(sql, &[], None)
                .unwrap_or_else(|e| panic!("indexed={indexed}: {sql}: {e:?}"));
            session.execute("ROLLBACK", &[], None).unwrap();
            drop(session);
            assert_eq!(f.parts(), before);
            f.execute(sql);
            f.reopen();
            if sql.contains("ADD COLUMN") {
                let entry = TableEntry {
                    primary_index_birth: None,
                    def: f.parts().tables["t"].clone(),
                    heap: false,
                    columns_ext: BTreeMap::new(),
                };
                let mut txn = f.db().begin(Isolation::ReadCommitted);
                for id in [0, 199999] {
                    assert_eq!(
                        f.db()
                            .rows
                            .get(&mut txn, &entry, &[Int(id)])
                            .unwrap()
                            .unwrap()
                            .values,
                        vec![Int(id), Int(id), Int(42)]
                    );
                }
            }
            if sql == "DROP TABLE t" {
                assert!(f.parts().tables.is_empty());
                assert!(f.parts().indexes.is_empty());
            }
        }
    }
}

#[test]
fn large_unique_builds() {
    let f = large_table(200_000, false, true);
    f.execute("CREATE UNIQUE INDEX unique_idx ON t(v)");
    f.execute("ALTER TABLE t ADD CONSTRAINT uq UNIQUE(v)");
}

#[test]
fn paged_validation_rejects_bad_rows_after_the_first_page() {
    let f = large_table(1025, false, true);
    let entry = TableEntry {
        primary_index_birth: None,
        def: f.parts().tables["t"].clone(),
        heap: false,
        columns_ext: BTreeMap::new(),
    };
    for (value, sql, error) in [
        (
            relational::Value::Null,
            "ALTER TABLE t ALTER COLUMN v SET NOT NULL",
            SqlError::Null,
        ),
        (
            Int(-1),
            "ALTER TABLE t ADD CONSTRAINT ck CHECK(v >= 0)",
            SqlError::CheckViolation,
        ),
    ] {
        let mut txn = f.db().begin(Isolation::ReadCommitted);
        f.db()
            .rows
            .update(
                &mut txn,
                &entry,
                &[Int(1024)],
                &[(1, value)],
                None,
                None,
                &|| false,
            )
            .unwrap();
        txn.commit().unwrap();
        let before = f.parts();
        assert_eq!(
            f.admin().sql_session().execute(sql, &[], None),
            Err(CoreError::Sql(error))
        );
        assert_eq!(f.parts(), before);
    }
}

#[test]
fn large_foreign_key_validation() {
    let f = large_table(200_000, true, false);
    f.execute("CREATE TABLE parent (id bigint PRIMARY KEY)");
    f.execute("INSERT INTO parent VALUES (7)");
    f.execute("ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY(v) REFERENCES parent(id)");
}

#[test]
fn large_primary_key_validation() {
    let f = Fixture::new();
    f.execute("CREATE TABLE t (id bigint, v bigint)");
    let parts = f.parts();
    let entry = TableEntry {
        primary_index_birth: None,
        def: parts.tables["t"].clone(),
        heap: true,
        columns_ext: BTreeMap::new(),
    };
    for start in (0..200_000).step_by(200) {
        let mut txn = f.db().begin(Isolation::ReadCommitted);
        for id in start..start + 200 {
            let values = entry
                .def
                .columns
                .iter()
                .map(|c| {
                    if c.name == "id" || c.name == "v" {
                        Int(id)
                    } else {
                        relational::Value::Null
                    }
                })
                .collect();
            f.db()
                .rows
                .insert(&mut txn, &entry, values, None, &|| false)
                .unwrap();
        }
        txn.commit().unwrap();
    }
    f.execute("ALTER TABLE t ADD PRIMARY KEY(id)");
}

#[test]
fn large_unique_duplicate_failure_leaves_no_index_after_reopen() {
    let mut f = large_table(200_000, false, false);
    let entry = TableEntry {
        primary_index_birth: None,
        def: f.parts().tables["t"].clone(),
        heap: false,
        columns_ext: BTreeMap::new(),
    };
    let mut txn = f.db().begin(Isolation::ReadCommitted);
    f.db()
        .rows
        .update(
            &mut txn,
            &entry,
            &[Int(199999)],
            &[(1, Int(0))],
            None,
            None,
            &|| false,
        )
        .unwrap();
    txn.commit().unwrap();
    let result =
        f.admin()
            .sql_session()
            .execute("CREATE UNIQUE INDEX duplicate_idx ON t(v)", &[], None);
    f.reopen();
    assert!(f.parts().indexes.is_empty());
    assert_eq!(result, Err(CoreError::Sql(SqlError::Duplicate)));
    let mut txn = f.db().begin(Isolation::ReadCommitted);
    f.db()
        .rows
        .update(
            &mut txn,
            &entry,
            &[Int(199999)],
            &[(1, Int(199999))],
            None,
            None,
            &|| false,
        )
        .unwrap();
    txn.commit().unwrap();
    f.execute("CREATE UNIQUE INDEX duplicate_idx ON t(v)");
    f.reopen();
    assert_eq!(f.parts().indexes.len(), 1);
}

#[test]
fn orphan_crash_worker() {
    let Ok(path) = std::env::var("P1_IX_CRASH_PATH") else {
        return;
    };
    let admin = Admin::open(path, Limits::default()).unwrap();
    CRASH_AFTER_INDEX_CHUNK.set(true);
    admin
        .sql_session()
        .execute("CREATE INDEX retry_idx ON t(v)", &[], None)
        .unwrap();
    panic!("chunk crash did not fire");
}

#[test]
fn orphaned_chunks_are_invisible_after_rollback_and_mid_build_crash() {
    // Reopen reclaims orphan prefixes left by rollback or process death.
    for crash in [false, true] {
        let mut f = large_table(401, false, false);
        if crash {
            f.admin.take();
            let out = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "storage::v2_sql::tests::large_ddl::orphan_crash_worker",
                    "--nocapture",
                ])
                .env("P1_IX_CRASH_PATH", &f.path)
                .output()
                .unwrap();
            assert_eq!(out.status.code(), Some(86), "{out:?}");
        } else {
            let mut s = f.admin().sql_session();
            s.execute("BEGIN", &[], None).unwrap();
            s.execute("CREATE INDEX retry_idx ON t(v)", &[], None)
                .unwrap();
            s.execute("ROLLBACK", &[], None).unwrap();
        }
        f.reopen();
        assert!(f.parts().indexes.is_empty());
        let mut txn = f.db().begin(Isolation::ReadCommitted);
        assert!(
            txn.scan(
                Keyspace::Index,
                Bound::Unbounded,
                Bound::Unbounded,
                Direction::Forward,
                Some(1)
            )
            .unwrap()
            .next()
            .is_none()
        );
        USED_INDEX_SCAN.set(false);
        assert_eq!(
            f.admin()
                .sql_session()
                .execute("SELECT id FROM t WHERE v=1", &[], None)
                .unwrap()
                .rows
                .len(),
            1
        );
        assert!(!USED_INDEX_SCAN.get());
        drop(txn);
        f.execute("CREATE INDEX retry_idx ON t(v)");
        f.reopen();
        assert_eq!(f.parts().indexes.len(), 1);
        USED_INDEX_SCAN.set(false);
        assert_eq!(
            f.admin()
                .sql_session()
                .execute("SELECT id FROM t WHERE v=1", &[], None)
                .unwrap()
                .rows
                .len(),
            1
        );
        assert!(USED_INDEX_SCAN.get());
    }
}

#[test]
fn legacy_sidecar_boundary_does_not_limit_v2_backfill() {
    // Isolate the exact legacy-sidecar admission bound independently of SQL's
    // scan, transaction write bytes, and RocksDB index access scan limits.
    for count in [1024, 1025] {
        let f = large_table(count, false, false);
        f.execute("CREATE INDEX boundary_idx ON t(v)");
    }
}

fn physical_count(f: &Fixture, ks: Keyspace) -> usize {
    f.db()
        .kv
        .range(ks, Bound::Unbounded, Bound::Unbounded, Direction::Forward)
        .unwrap()
        .try_fold(0, |count, pair| pair.map(|_| count + 1))
        .unwrap()
}

#[test]
fn reclaim_large_drop_and_snapshot_horizon() {
    let f = large_table(200_000, false, false);
    let mut reader = f.db().begin(Isolation::RepeatableRead);
    let entry = TableEntry {
        primary_index_birth: None,
        def: f.db().catalog().parts(&mut reader).unwrap().tables["t"].clone(),
        heap: false,
        columns_ext: BTreeMap::new(),
    };
    assert!(
        f.db()
            .rows
            .get(&mut reader, &entry, &[Int(199999)])
            .unwrap()
            .is_some()
    );
    f.execute("DROP TABLE t");
    assert_eq!(f.admin().reclaim_storage().unwrap().tables, 0);
    assert_eq!(physical_count(&f, Keyspace::Rows), 200_000);
    assert!(
        f.db()
            .rows
            .get(&mut reader, &entry, &[Int(199999)])
            .unwrap()
            .is_some()
    );
    drop(reader);
    assert_eq!(f.admin().reclaim_storage().unwrap().tables, 1);
    assert_eq!(physical_count(&f, Keyspace::Rows), 0);
    assert_eq!(f.admin().reclaim_storage().unwrap().tables, 0);
}

#[test]
fn reclaim_retains_concurrent_builder_until_outer_transaction_ends() {
    let f = large_table(401, false, false);
    let admin = f.admin().clone();
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let (go_tx, go_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        let first = std::cell::Cell::new(true);
        AFTER_INDEX_CHUNK.with_borrow_mut(|hook| {
            *hook = Some(Box::new(move || {
                if first.replace(false) {
                    ready_tx.send(()).unwrap();
                    go_rx
                        .recv_timeout(std::time::Duration::from_secs(30))
                        .unwrap();
                }
            }))
        });
        let mut session = admin.sql_session();
        session.execute("BEGIN", &[], None).unwrap();
        session
            .execute("CREATE INDEX concurrent_idx ON t(v)", &[], None)
            .unwrap();
        AFTER_INDEX_CHUNK.with_borrow_mut(|hook| *hook = None);
        session
    });
    ready_rx
        .recv_timeout(std::time::Duration::from_secs(30))
        .unwrap();
    assert_eq!(physical_count(&f, Keyspace::Index), 200);
    assert_eq!(f.admin().reclaim_storage().unwrap().indexes, 0);
    go_tx.send(()).unwrap();
    let mut session = worker.join().unwrap();
    // All chunks exist, but the descriptor is still transaction-local.
    assert_eq!(physical_count(&f, Keyspace::Index), 401);
    assert_eq!(f.admin().reclaim_storage().unwrap().indexes, 0);
    session.execute("COMMIT", &[], None).unwrap();
    assert_eq!(f.admin().reclaim_storage().unwrap().indexes, 0);
    assert_eq!(physical_count(&f, Keyspace::Index), 401);
    f.execute("DROP INDEX concurrent_idx");
    assert_eq!(physical_count(&f, Keyspace::Index), 0);
}

#[test]
fn reclaim_crash_worker() {
    let Ok(path) = std::env::var("P1_GC_CRASH_PATH") else {
        return;
    };
    let admin = Admin::open(path, Limits::default()).unwrap();
    // Keep pre-DROP snapshots until both prefixes are durable retirement candidates.
    let db = match &admin.inner.backend {
        Backend::V2(_) => admin.test_v2(),
        _ => unreachable!(),
    };
    let snapshot = db.sequencer.snapshot();
    admin
        .sql_session()
        .execute("DROP TABLE t", &[], None)
        .unwrap();
    drop(snapshot);
    v2::reclaim::CRASH_AFTER_RECLAIM.set(true);
    admin.reclaim_storage().unwrap();
    panic!("reclaim crash did not fire");
}

#[test]
fn reclaim_resumes_after_crash_between_prefixes() {
    let mut f = large_table(401, false, false);
    f.execute("CREATE INDEX crash_idx ON t(v)");
    f.admin.take();
    let out = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "storage::v2_sql::tests::large_ddl::reclaim_crash_worker",
            "--nocapture",
        ])
        .env("P1_GC_CRASH_PATH", &f.path)
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(86), "{out:?}");
    f.reopen();
    assert_eq!(physical_count(&f, Keyspace::Rows), 0);
    assert_eq!(physical_count(&f, Keyspace::Index), 0);
    assert!(f.parts().tables.is_empty());
    assert_eq!(
        f.admin().reclaim_storage().unwrap(),
        ReclaimStats::default()
    );
}

#[test]
fn reclaim_schema_cascade_and_type_column_drop() {
    let f = Fixture::new();
    f.execute("CREATE SCHEMA s");
    f.execute("CREATE TABLE s.t(id bigint PRIMARY KEY, v bigint)");
    f.execute("INSERT INTO s.t VALUES(1,2)");
    f.execute("CREATE INDEX s_idx ON s.t(v)");
    f.execute("DROP SCHEMA s CASCADE");
    assert_eq!(physical_count(&f, Keyspace::Rows), 0);
    assert_eq!(physical_count(&f, Keyspace::Index), 0);
    f.execute("CREATE TYPE mood AS ENUM ('ok')");
    f.execute("CREATE TABLE t(id bigint PRIMARY KEY, m mood)");
    f.execute("INSERT INTO t VALUES(1,'ok')");
    f.execute("DROP TYPE mood CASCADE");
    assert_eq!(
        f.admin()
            .sql_session()
            .execute("SELECT * FROM t", &[], None)
            .unwrap()
            .rows[0]
            .values,
        vec![Int(1)]
    );
    assert_eq!(
        f.admin().reclaim_storage().unwrap(),
        ReclaimStats::default()
    );
}

#[test]
fn reclaim_is_best_effort_and_dml_skips_it() {
    use v2::reclaim::{RECLAIM_ERROR, RECLAIM_PASSES};
    let mut f = large_table(1, false, false);
    let passes = RECLAIM_PASSES.get();
    f.execute("INSERT INTO t VALUES(2,2)");
    let mut session = f.admin().sql_session();
    session.execute("BEGIN", &[], None).unwrap();
    session
        .execute("UPDATE t SET v=3 WHERE id=2", &[], None)
        .unwrap();
    session
        .execute("DELETE FROM t WHERE id=2", &[], None)
        .unwrap();
    session.execute("COMMIT", &[], None).unwrap();
    drop(session);
    assert_eq!(RECLAIM_PASSES.get(), passes);
    RECLAIM_ERROR.set(Some(CoreError::Storage("transient GC failure".into())));
    f.execute("DROP TABLE t");
    assert!(physical_count(&f, Keyspace::Rows) > 0);
    assert_eq!(f.db().reclaim_errors.load(Ordering::Relaxed), 1);
    RECLAIM_ERROR.set(Some(CoreError::Storage("transient open GC failure".into())));
    f.reopen();
    assert_eq!(f.db().reclaim_errors.load(Ordering::Relaxed), 1);
    f.reopen();
    assert_eq!(physical_count(&f, Keyspace::Rows), 0);
    f.admin.take();
    RECLAIM_ERROR.set(Some(CoreError::Corrupt("corrupt GC prefix".into())));
    assert!(matches!(
        Admin::open(&f.path, Limits::default()),
        Err(CoreError::Corrupt(_))
    ));
}
