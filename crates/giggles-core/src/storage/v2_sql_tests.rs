use super::*;
use engine::{
    catalog_codec, codec,
    hlc::Ts,
    kv::{Direction, Durability, Keyspace, WriteBatch},
    mvcc,
};
use std::ops::Bound;

struct Fixture {
    path: std::path::PathBuf,
    admin: Option<Admin>,
}
impl Fixture {
    fn new() -> Self {
        let mut nonce = [0; 16];
        getrandom::fill(&mut nonce).unwrap();
        let path = std::env::temp_dir().join(format!("v2-sql-{:032x}", u128::from_be_bytes(nonce)));
        let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
        Self {
            path,
            admin: Some(admin),
        }
    }
    fn admin(&self) -> &Admin {
        self.admin.as_ref().unwrap()
    }
    fn db(&self) -> &v2::V2Db {
        match &self.admin().inner.backend {
            Backend::V2(db) => db,
            _ => unreachable!(),
        }
    }
    fn reopen(&mut self) {
        self.admin.take();
        self.admin = Some(Admin::open(&self.path, Limits::default()).unwrap());
    }
    fn parts(&self) -> CatalogParts {
        let mut txn = self.db().begin(Isolation::ReadCommitted);
        self.db().catalog().parts(&mut txn).unwrap()
    }
    fn catalog_bytes(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.db()
            .kv
            .snapshot()
            .unwrap()
            .range(
                Keyspace::Catalog,
                Bound::Unbounded,
                Bound::Unbounded,
                Direction::Forward,
            )
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    }
    fn execute(&self, sql: &str) {
        self.admin()
            .sql_session()
            .execute(sql, &[], None)
            .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.admin.take();
        fs::remove_dir_all(&self.path).unwrap();
    }
}

#[test]
fn ddl_defaults_and_renames_persist() {
    let mut f = Fixture::new();
    f.execute("CREATE TABLE t (id bigint PRIMARY KEY, v text DEFAULT 'x')");
    let original = f.parts();
    let n = original.tables["t"].columns[1].id;
    assert_eq!(
        original.ext.columns[&n].default.as_deref(),
        Some("'x'::text")
    );
    f.reopen();
    assert_eq!(f.parts(), original);
    f.execute("ALTER TABLE t RENAME COLUMN v TO w");
    f.execute("ALTER TABLE t RENAME TO u");
    let renamed = f.parts();
    assert_eq!(renamed.tables["u"].columns[1].name, "w");
    let id = renamed.tables["u"].id;
    assert_eq!(renamed.ext.constraints[&id][0].name, "t_pkey");
    f.reopen();
    assert_eq!(f.parts(), renamed);
    f.execute("ALTER TABLE u ALTER COLUMN w SET DEFAULT 'y'");
    assert_eq!(
        f.parts().ext.columns[&n].default.as_deref(),
        Some("'y'::text")
    );
    f.reopen();
    assert_eq!(
        f.parts().ext.columns[&n].default.as_deref(),
        Some("'y'::text")
    );
    f.execute("ALTER TABLE u ALTER COLUMN w DROP DEFAULT");
    assert!(
        f.parts()
            .ext
            .columns
            .get(&n)
            .is_none_or(|v| v.default.is_none())
    );
    let dropped_default = f.parts();
    f.reopen();
    assert_eq!(f.parts(), dropped_default);
}

#[test]
fn serial_catalog_and_owned_counter_are_removed_on_drop() {
    let mut f = Fixture::new();
    f.execute("CREATE TABLE t (id serial PRIMARY KEY)");
    let parts = f.parts();
    let (&id, sequence) = parts.ext.sequences.iter().next().unwrap();
    assert_eq!(parts.ext.sequences.len(), 1);
    let mut txn = f.db().begin(Isolation::ReadCommitted);
    assert!(
        mvcc::read_at(
            f.db().kv.as_ref(),
            Keyspace::Catalog,
            &catalog_codec::sequence_key(id),
            txn.snapshot_ts()
        )
        .unwrap()
        .is_some()
    );
    drop(txn);
    let features = f.db().kv.get(Keyspace::Meta, b"features").unwrap().unwrap();
    assert_ne!(
        u64::from_be_bytes(features.try_into().unwrap()) & (1 << 6),
        0
    );
    let counter = catalog_codec::counter_key(id, sequence.incarnation);
    // Counters are lazy; seed an owned counter to prove DROP removes it.
    let mut batch = WriteBatch::default();
    batch.put(
        Keyspace::Seq,
        counter.clone(),
        catalog_codec::encode_counter(
            &sequences::SeqCounter {
                incarnation: sequence.incarnation,
                last_value: sequence.start,
                is_called: false,
                log_hw: sequence.start,
                stamp: 0,
            },
            sequence,
        )
        .unwrap(),
    );
    engine::meta::put_features(&mut batch, (1 << 6) | (1 << 8)).unwrap();
    f.db().kv.write(batch, Durability::Sync).unwrap();
    assert!(f.db().kv.get(Keyspace::Seq, &counter).unwrap().is_some());
    f.reopen();
    assert_eq!(f.parts(), parts);
    f.execute("DROP TABLE t");
    assert!(f.parts().tables.is_empty());
    assert!(f.parts().ext.sequences.is_empty());
    assert!(f.db().kv.get(Keyspace::Seq, &counter).unwrap().is_none());
    f.reopen();
    assert!(f.parts().tables.is_empty());
}

#[test]
fn explicit_commit_rollback_and_failure_abort() {
    let mut f = Fixture::new();
    let mut s = f.admin().sql_session();
    s.execute("BEGIN", &[], None).unwrap();
    assert!(s.in_transaction());
    s.execute("CREATE TABLE rolled (id bigint PRIMARY KEY)", &[], None)
        .unwrap();
    s.execute("ROLLBACK", &[], None).unwrap();
    assert!(f.parts().tables.is_empty());
    s.execute("BEGIN", &[], None).unwrap();
    s.execute("CREATE TABLE committed (id bigint PRIMARY KEY)", &[], None)
        .unwrap();
    s.execute("COMMIT", &[], None).unwrap();
    s.execute("BEGIN", &[], None).unwrap();
    s.execute("CREATE TABLE failed (id bigint PRIMARY KEY)", &[], None)
        .unwrap();
    assert!(matches!(
        s.execute("CREATE TABLE failed (id bigint PRIMARY KEY)", &[], None),
        Err(CoreError::Sql(_))
    ));
    assert!(s.is_aborted());
    assert!(matches!(
        s.execute("COMMIT", &[], None),
        Err(CoreError::Sql(relational::SqlError::Aborted))
    ));
    s.execute("ROLLBACK", &[], None).unwrap();
    assert_eq!(
        f.parts()
            .tables
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["committed"]
    );
    drop(s);
    f.reopen();
    assert!(f.parts().tables.contains_key("committed"));
    assert!(!f.parts().tables.contains_key("failed"));
}

#[test]
fn implicit_failure_and_refusals_leave_catalog_unchanged() {
    let f = Fixture::new();
    f.execute("CREATE TABLE t (id bigint PRIMARY KEY)");
    let before = f.parts();
    let bytes = f.catalog_bytes();
    assert!(matches!(
        f.admin()
            .sql_session()
            .execute("CREATE TABLE t (id bigint PRIMARY KEY)", &[], None),
        Err(CoreError::Sql(_))
    ));
    assert_eq!(f.parts(), before);
    assert_eq!(f.catalog_bytes(), bytes);
    for (sql, feature) in [
        ("CREATE INDEX idx ON t (id)", "secondary indexes"),
        (
            "CREATE TABLE u (id bigint PRIMARY KEY, n bigint UNIQUE)",
            "secondary indexes",
        ),
        (
            "ALTER TABLE t ADD COLUMN n bigint",
            "ALTER TABLE row rewrite",
        ),
    ] {
        assert!(
            matches!(f.admin().sql_session().execute(sql, &[], None),
            Err(CoreError::EngineUnsupported { engine: "v2", feature: got }) if got == feature),
            "{sql}"
        );
        assert_eq!(f.parts(), before, "{sql}");
        assert_eq!(f.catalog_bytes(), bytes, "{sql}");
    }
}

#[test]
fn invalid_stored_constraint_fails_image_validation_without_catalog_writes() {
    use relational::catalog_ext::{Action, Constraint, ConstraintKind, Deferral};
    let mut f = Fixture::new();
    f.execute("CREATE TABLE t (id bigint PRIMARY KEY)");
    let parts = f.parts();
    let table = &parts.tables["t"];
    let ids = f.db().catalog().reserve_ids(1).unwrap();
    let constraint = Constraint {
        id: ids.start,
        name: "broken_fk".into(),
        kind: ConstraintKind::ForeignKey {
            columns: vec![table.columns[0].id],
            ref_table: table.id,
            ref_columns: vec![u64::MAX],
            on_delete: Action::NoAction,
            on_update: Action::NoAction,
            not_valid: false,
            deferral: Deferral::NotDeferrable,
        },
    };
    let ts = engine::meta::open_checks(f.db().kv.as_ref())
        .unwrap()
        .last_commit_ts
        .unwrap();
    let mut batch = WriteBatch::default();
    batch.put(
        Keyspace::Catalog,
        codec::user_key_seek_at(
            &catalog_codec::constraint_key(table.id, constraint.id),
            Ts(ts),
        ),
        mvcc::encode_live(&catalog_codec::encode_constraint(&constraint, "t").unwrap()),
    );
    engine::meta::put_features(&mut batch, 1 << 6).unwrap();
    f.db().kv.write(batch, Durability::Sync).unwrap();
    f.reopen();
    let before = f.parts();
    let bytes = f.catalog_bytes();
    assert!(matches!(
        f.admin()
            .sql_session()
            .execute("CREATE TABLE x (id bigint PRIMARY KEY)", &[], None),
        Err(CoreError::Corrupt(_))
    ));
    assert_eq!(f.parts(), before);
    assert_eq!(f.catalog_bytes(), bytes);
}

#[test]
fn check_constraint_is_refused_without_catalog_changes() {
    let f = Fixture::new();
    f.execute("CREATE TABLE q (id bigint PRIMARY KEY, n bigint)");
    // The classifier refuses these statements before the post-image gate is
    // reachable. Direct check_post_image tests below exercise that gate; this
    // SQL witness proves the earlier refusal leaves catalog bytes untouched.
    // Every DDL form that would create an unenforced CHECK or FOREIGN KEY is
    // refused with 0A000 and leaves the catalog byte-identical (DD-0068 X3 #2).
    for sql in [
        "CREATE TABLE t (id bigint PRIMARY KEY, n bigint CHECK (n > 0))",
        "CREATE TABLE t (id bigint PRIMARY KEY, n bigint, CHECK (n > 0))",
        "CREATE TABLE t (id bigint PRIMARY KEY, p bigint REFERENCES q(id))",
        "CREATE TABLE t (id bigint PRIMARY KEY, p bigint, FOREIGN KEY (p) REFERENCES q(id))",
        "ALTER TABLE q ADD CHECK (n > 0)",
        "ALTER TABLE q ADD CONSTRAINT q_n_fkey FOREIGN KEY (n) REFERENCES q(id)",
    ] {
        let before = f.parts();
        let bytes = f.catalog_bytes();
        assert!(
            matches!(
                f.admin().sql_session().execute(sql, &[], None),
                Err(CoreError::EngineUnsupported {
                    engine: "v2",
                    feature: "SQL statement"
                })
            ),
            "{sql}"
        );
        assert_eq!(f.parts(), before, "{sql}");
        assert_eq!(f.catalog_bytes(), bytes, "{sql}");
    }
}

fn created_catalog(
    sql: &str,
) -> (
    BTreeMap<String, relational::TableDef>,
    relational::catalog_ext::CatalogExt,
    u64,
    u64,
) {
    let mut image = relational::Relational::default();
    let prepared = relational::Prepared::parse(sql, 0, 0).unwrap();
    image.execute(&prepared, &[], None).unwrap();
    image.catalog_parts()
}

#[test]
fn post_image_invalid_constraint_is_corrupt() {
    use relational::catalog_ext::ConstraintKind;
    let (tables, mut ext, next_id, _) =
        created_catalog("CREATE TABLE t (id bigint PRIMARY KEY, p bigint REFERENCES t(id))");
    let constraints = ext.constraints.get_mut(&tables["t"].id).unwrap();
    let foreign_key = constraints
        .iter_mut()
        .find_map(|constraint| {
            if let ConstraintKind::ForeignKey { columns, .. } = &mut constraint.kind {
                Some(columns)
            } else {
                None
            }
        })
        .unwrap();
    foreign_key[0] = u64::MAX;
    assert!(matches!(
        check_post_image(&tables, &ext, next_id),
        Err(CoreError::Corrupt(message)) if message.starts_with("v2 DDL produced an invalid catalog: ")
    ));
}

#[test]
fn post_image_check_is_unsupported() {
    let (tables, ext, next_id, _) =
        created_catalog("CREATE TABLE t (id bigint PRIMARY KEY, n bigint CHECK (n > 0))");
    assert!(matches!(
        check_post_image(&tables, &ext, next_id),
        Err(CoreError::EngineUnsupported {
            engine: "v2",
            feature: "SQL statement"
        })
    ));
}

#[test]
fn post_image_varchar_is_unsupported() {
    let (tables, ext, next_id, _) =
        created_catalog("CREATE TABLE t (id bigint PRIMARY KEY, v varchar(10))");
    assert!(matches!(
        check_post_image(&tables, &ext, next_id),
        Err(CoreError::EngineUnsupported {
            engine: "v2",
            feature: "SQL statement"
        })
    ));
}

#[test]
fn post_image_primary_key_is_valid() {
    let (tables, ext, next_id, _) = created_catalog("CREATE TABLE t (id bigint PRIMARY KEY)");
    check_post_image(&tables, &ext, next_id).unwrap();
}

#[test]
fn varchar_char_refusals_leave_catalog_bytes_unchanged() {
    let f = Fixture::new();
    f.execute("CREATE TABLE q (id bigint PRIMARY KEY)");
    for sql in [
        "CREATE TABLE t (id bigint PRIMARY KEY, v varchar(10))",
        "CREATE TABLE t (id bigint PRIMARY KEY, v char(3))",
        "ALTER TABLE q ADD COLUMN v varchar(10)",
    ] {
        let bytes = f.catalog_bytes();
        assert!(
            matches!(
                f.admin().sql_session().execute(sql, &[], None),
                Err(CoreError::EngineUnsupported {
                    engine: "v2",
                    feature: "varchar/char types"
                })
            ),
            "{sql}"
        );
        assert_eq!(f.catalog_bytes(), bytes, "{sql}");
    }
}

fn values(session: &mut SqlSession, sql: &str) -> Vec<Vec<relational::Value>> {
    session
        .execute(sql, &[], None)
        .unwrap()
        .rows
        .into_iter()
        .map(|r| r.values)
        .collect()
}

#[test]
fn dml_script_matches_legacy_results_and_errors() {
    let f = Fixture::new();
    let path = f.path.with_extension("legacy");
    let legacy = Admin::create(&path, Limits::default()).unwrap();
    let mut a = legacy.sql_session();
    let mut b = f.admin().sql_session();
    // Compare SQL values, column names, affected counts and exact error variants.
    // Row versions are engine-owned opaque tokens; test their behavior separately.
    let script = [
        "CREATE TABLE t (id bigint PRIMARY KEY, n bigint NOT NULL, label text DEFAULT 'default')",
        "INSERT INTO t (id,n) VALUES (1,10) RETURNING *",
        "INSERT INTO t VALUES (2,20,'two'),(3,30,'three') RETURNING id,n,label",
        "INSERT INTO t VALUES (1,99,'duplicate')",
        "INSERT INTO t VALUES (4,40,'four'),(2,22,'duplicate') RETURNING *",
        "SELECT * FROM t ORDER BY id",
        "UPDATE t SET id=8,n=n+1 WHERE id=1 RETURNING *",
        "UPDATE t SET id=3 WHERE id=2 RETURNING *",
        "UPDATE t SET n=n+5 WHERE n>=20 RETURNING id,n",
        "UPDATE t SET n=NULL WHERE id=8",
        "SELECT id,n,label FROM t ORDER BY id DESC",
        "DELETE FROM t WHERE n>30 RETURNING *",
        "DELETE FROM t WHERE id=999 RETURNING *",
        "INSERT INTO t VALUES (2,99,'ignored') ON CONFLICT (id) DO NOTHING RETURNING *",
        "INSERT INTO t VALUES (2,99,'updated') ON CONFLICT (id) DO UPDATE SET n=excluded.n RETURNING *",
        "BEGIN",
        "INSERT INTO t VALUES (10,100,'ten') RETURNING *",
        "UPDATE t SET id=11 WHERE id=10 RETURNING *",
        "SELECT * FROM t ORDER BY id",
        "ROLLBACK",
        "SELECT * FROM t ORDER BY id",
        "BEGIN",
        "INSERT INTO t VALUES (12,120,'twelve') RETURNING *",
        "DELETE FROM t WHERE id=8 RETURNING *",
        "COMMIT",
        "BEGIN",
        "INSERT INTO t VALUES (13,130,'thirteen')",
        "INSERT INTO t VALUES (14,140,'fourteen'),(12,120,'duplicate')",
        "SELECT * FROM t ORDER BY id",
        "COMMIT",
        "ROLLBACK",
        "SELECT * FROM t ORDER BY id",
        "DROP TABLE t",
        "CREATE TABLE t (id bigint PRIMARY KEY, n bigint)",
        "SELECT * FROM t ORDER BY id",
        "CREATE TABLE heap (n bigint, label text)",
        "INSERT INTO heap VALUES (10,'a'),(20,'b') RETURNING *",
        "UPDATE heap SET n=n+1 RETURNING *",
        "DELETE FROM heap WHERE n>15 RETURNING *",
        "SELECT * FROM heap ORDER BY n",
    ];
    for sql in script {
        let left = a.execute(sql, &[], None);
        let right = b.execute(sql, &[], None);
        match (left, right) {
            (Ok(left), Ok(right)) => {
                assert_eq!(left.columns, right.columns, "{sql}");
                assert_eq!(left.affected, right.affected, "{sql}");
                assert_eq!(
                    left.rows.into_iter().map(|r| r.values).collect::<Vec<_>>(),
                    right.rows.into_iter().map(|r| r.values).collect::<Vec<_>>(),
                    "{sql}"
                );
            }
            (Err(left), Err(right)) => assert_eq!(left, right, "{sql}"),
            (left, right) => panic!("{sql}: legacy={left:?}, v2={right:?}"),
        }
    }
    drop((a, b, legacy));
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn dml_versions_rekey_reopen_and_mvcc_visibility() {
    use relational::Value::Int;
    let mut f = Fixture::new();
    f.execute("CREATE TABLE t (id bigint PRIMARY KEY, n bigint)");
    let mut s = f.admin().sql_session();
    let initial = s
        .execute("INSERT INTO t VALUES (1,10) RETURNING *", &[], None)
        .unwrap()
        .rows
        .remove(0);
    let updated = s
        .execute(
            "UPDATE t SET id=2,n=20 WHERE id=1 RETURNING *",
            &[],
            Some(initial.version),
        )
        .unwrap()
        .rows
        .remove(0);
    assert!(updated.version > initial.version);
    let allocator = f.db().kv.get(Keyspace::Seq, &[0; 9]).unwrap();
    let commit = f.db().kv.get(Keyspace::Meta, b"last_commit_ts").unwrap();
    values(&mut s, "SELECT * FROM t ORDER BY id");
    assert_eq!(f.db().kv.get(Keyspace::Seq, &[0; 9]).unwrap(), allocator);
    assert_eq!(
        f.db().kv.get(Keyspace::Meta, b"last_commit_ts").unwrap(),
        commit
    );
    assert!(values(&mut s, "SELECT * FROM t WHERE id=1").is_empty());
    assert_eq!(
        s.execute("UPDATE t SET n=30 WHERE id=2", &[], Some(initial.version)),
        Err(CoreError::Sql(relational::SqlError::Version))
    );
    s.execute("BEGIN", &[], None).unwrap();
    s.execute("UPDATE t SET id=3 WHERE id=2", &[], None)
        .unwrap();
    assert_eq!(
        values(&mut s, "SELECT * FROM t ORDER BY id"),
        vec![vec![Int(3), Int(20)]]
    );
    // Readers do not wait on the SQL writer mutex, and see committed row versions.
    assert_eq!(
        values(&mut f.admin().sql_session(), "SELECT * FROM t ORDER BY id"),
        vec![vec![Int(2), Int(20)]]
    );
    s.execute("COMMIT", &[], None).unwrap();
    drop(s);
    f.reopen();
    assert_eq!(
        values(&mut f.admin().sql_session(), "SELECT * FROM t ORDER BY id"),
        vec![vec![Int(3), Int(20)]]
    );
    f.execute("DROP TABLE t");
    // DROP cascades row tombstones in the catalog transaction, not orphan rows.
    let mut txn = f.db().begin(Isolation::ReadCommitted);
    assert!(
        txn.scan(
            Keyspace::Rows,
            Bound::Unbounded,
            Bound::Unbounded,
            Direction::Forward,
            None
        )
        .unwrap()
        .next()
        .is_none()
    );
    drop(txn);
    f.reopen();
    assert!(f.parts().tables.is_empty());
}

#[test]
fn failed_statement_preserves_prior_transaction_writes() {
    use relational::Value::Int;
    let f = Fixture::new();
    f.execute("CREATE TABLE t (id bigint PRIMARY KEY,n bigint)");
    let mut s = f.admin().sql_session();
    s.execute("BEGIN", &[], None).unwrap();
    s.execute("INSERT INTO t VALUES (1,10)", &[], None).unwrap();
    assert_eq!(
        s.execute("INSERT INTO t VALUES (2,20),(1,99)", &[], None),
        Err(CoreError::Sql(relational::SqlError::Duplicate))
    );
    assert!(s.is_aborted());
    // Inspect the existing MVCC transaction directly: a failed statement keeps
    // the first successful statement but publishes none of its own rows.
    let parts = f.parts();
    let entry = engine::catalog::TableEntry {
        def: parts.tables["t"].clone(),
        heap: false,
        columns_ext: BTreeMap::new(),
    };
    let txn = s.v2_transaction.as_mut().unwrap();
    assert_eq!(
        f.db()
            .rows
            .get(txn, &entry, &[Int(1)])
            .unwrap()
            .unwrap()
            .values,
        vec![Int(1), Int(10)]
    );
    assert!(f.db().rows.get(txn, &entry, &[Int(2)]).unwrap().is_none());
    s.execute("ROLLBACK", &[], None).unwrap();
    assert!(values(&mut s, "SELECT * FROM t ORDER BY id").is_empty());
}

#[test]
fn v2_admission_and_multitable_refusals() {
    let f = Fixture::new();
    f.execute("CREATE TABLE t (id bigint PRIMARY KEY)");
    let ticket = f.admin().inner.enter_sql().unwrap();
    // Default admission allows 32 simultaneous statements.
    let mut tickets = vec![ticket];
    for _ in 1..Limits::default().admitted {
        tickets.push(f.admin().inner.enter_sql().unwrap());
    }
    assert_eq!(
        f.admin()
            .sql_session()
            .execute("INSERT INTO t VALUES (1)", &[], None),
        Err(CoreError::Busy)
    );
    drop(tickets);
    let mut sessions = (0..relational::MAX_TRANSACTIONS)
        .map(|_| {
            let mut s = f.admin().sql_session();
            s.execute("BEGIN", &[], None).unwrap();
            s
        })
        .collect::<Vec<_>>();
    assert_eq!(
        f.admin().sql_session().execute("BEGIN", &[], None),
        Err(CoreError::Busy)
    );
    sessions
        .pop()
        .unwrap()
        .execute("COMMIT", &[], None)
        .unwrap();
    let mut next = f.admin().sql_session();
    next.execute("BEGIN", &[], None).unwrap();
    next.execute("ROLLBACK", &[], None).unwrap();
    drop(sessions);
    for sql in [
        "SELECT a.id FROM t a JOIN t b ON a.id=b.id",
        "SELECT id FROM t WHERE id IN (SELECT id FROM t)",
        "UPDATE t SET id=2 WHERE id IN (SELECT id FROM t)",
        "DELETE FROM t WHERE id IN (SELECT id FROM t)",
    ] {
        assert!(
            matches!(
                f.admin().sql_session().execute(sql, &[], None),
                Err(CoreError::EngineUnsupported {
                    feature: "multi-table SQL reads",
                    ..
                })
            ),
            "{sql}"
        );
    }
    assert!(values(&mut f.admin().sql_session(), "SELECT * FROM t ORDER BY id").is_empty());
}

#[test]
fn clocked_serial_defaults_match_legacy_and_transactional_create_persists() {
    let mut f = Fixture::new();
    let path = f.path.with_extension("serial-legacy");
    let legacy = Admin::create(&path, Limits::default()).unwrap();
    let mut a = legacy.sql_session();
    let mut b = f.admin().sql_session();
    let clock = relational::StatementClock {
        unavailable: None,
        transaction_us: 1,
        statement_us: 1,
        zone: None,
        wall: None,
    };
    for sql in [
        "BEGIN",
        "CREATE TABLE t (id serial PRIMARY KEY,n bigint DEFAULT 7)",
        "INSERT INTO t DEFAULT VALUES RETURNING *",
        "INSERT INTO t DEFAULT VALUES RETURNING *",
        "COMMIT",
        "INSERT INTO t DEFAULT VALUES RETURNING *",
        "BEGIN",
        "INSERT INTO t DEFAULT VALUES RETURNING *",
        "ROLLBACK",
        "INSERT INTO t DEFAULT VALUES RETURNING *",
        "SELECT * FROM t ORDER BY id",
    ] {
        let left = a
            .execute_at(sql, &[], None, &clock)
            .unwrap_or_else(|e| panic!("legacy {sql}: {e:?}"));
        let right = b
            .execute_at(sql, &[], None, &clock)
            .unwrap_or_else(|e| panic!("v2 {sql}: {e:?}"));
        assert_eq!(left.columns, right.columns, "{sql}");
        assert_eq!(left.affected, right.affected, "{sql}");
        assert_eq!(
            left.rows.into_iter().map(|r| r.values).collect::<Vec<_>>(),
            right.rows.into_iter().map(|r| r.values).collect::<Vec<_>>(),
            "{sql}"
        );
    }
    drop((a, b, legacy));
    fs::remove_dir_all(path).unwrap();
    f.reopen();
    // The persisted reservation may skip values across restart, as on legacy.
    let next = f
        .admin()
        .sql_session()
        .execute_at(
            "INSERT INTO t DEFAULT VALUES RETURNING id",
            &[],
            None,
            &clock,
        )
        .unwrap();
    assert!(matches!(next.rows[0].values[0],relational::Value::Int(n) if n>5));
}

#[test]
fn sink_failure_rolls_back_partial_rekey_and_drop() {
    use relational::Value::Int;
    let f = Fixture::new();
    f.execute("CREATE TABLE t (id bigint PRIMARY KEY,n bigint)");
    f.execute("INSERT INTO t VALUES (1,10),(2,20)");
    let mut s = f.admin().sql_session();
    for sql in ["UPDATE t SET id=3 WHERE id=1", "DROP TABLE t"] {
        s.execute("BEGIN", &[], None).unwrap();
        s.execute("INSERT INTO t VALUES (4,40)", &[], None).unwrap();
        FAIL_AFTER_ROW_WRITE.set(true);
        assert_eq!(s.execute(sql, &[], None), Err(CoreError::Cancelled));
        assert!(!FAIL_AFTER_ROW_WRITE.get());
        let txn = s.v2_transaction.as_mut().unwrap();
        let parts = f.db().catalog().parts(txn).unwrap();
        let entry = engine::catalog::TableEntry {
            def: parts.tables["t"].clone(),
            heap: false,
            columns_ext: BTreeMap::new(),
        };
        for (id, n) in [(1, 10), (2, 20), (4, 40)] {
            assert_eq!(
                f.db()
                    .rows
                    .get(txn, &entry, &[Int(id)])
                    .unwrap()
                    .unwrap()
                    .values,
                vec![Int(id), Int(n)]
            );
        }
        assert!(f.db().rows.get(txn, &entry, &[Int(3)]).unwrap().is_none());
        s.execute("ROLLBACK", &[], None).unwrap();
    }
}

#[test]
fn mutation_limits_and_parameter_errors_match_legacy_without_rows() {
    use relational::Value::{Int, Text};
    let f = Fixture::new();
    let path = f.path.with_extension("limits-legacy");
    let legacy = Admin::create(&path, Limits::default()).unwrap();
    let mut a = legacy.sql_session();
    let mut b = f.admin().sql_session();
    for s in [&mut a, &mut b] {
        s.execute("CREATE TABLE t (id bigint PRIMARY KEY,v text)", &[], None)
            .unwrap();
    }
    let wide = [Int(1), Text("x".repeat(relational::MAX_ROW_BYTES + 1))];
    for params in [&wide[..], &[Int(1)][..]] {
        let left = a
            .execute("INSERT INTO t VALUES ($1,$2)", params, None)
            .unwrap_err();
        assert_eq!(
            b.execute("INSERT INTO t VALUES ($1,$2)", params, None),
            Err(left)
        );
    }
    let tuples = (0..=relational::MAX_ROWS)
        .map(|i| format!("({i},'x')"))
        .collect::<Vec<_>>()
        .join(",");
    let sql = format!("INSERT INTO t VALUES {tuples}");
    let left = a.execute(&sql, &[], None).unwrap_err();
    assert_eq!(b.execute(&sql, &[], None), Err(left));
    assert!(values(&mut a, "SELECT * FROM t ORDER BY id").is_empty());
    assert!(values(&mut b, "SELECT * FROM t ORDER BY id").is_empty());
    drop((a, b, legacy));
    fs::remove_dir_all(path).unwrap();
}
