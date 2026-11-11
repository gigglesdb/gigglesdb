use super::*;
use engine::{
    catalog_codec, codec,
    hlc::Ts,
    kv::{Direction, Durability, Keyspace, WriteBatch},
    mvcc,
};
use std::ops::Bound;

#[path = "v2_large_ddl_tests.rs"]
mod large_ddl;
#[path = "v2_large_dml_tests.rs"]
mod large_dml;
#[path = "v2_large_read_tests.rs"]
mod large_read;
#[path = "v2_fk_prefix_tests.rs"]
mod fk_prefix;
#[path = "v2_reduction_tests.rs"]
mod reduction;

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
fn internal_exists_scale_and_own_writes() {
    use crate::relational::Value;
    let f = Fixture::new();
    f.execute("CREATE TABLE users(id int PRIMARY KEY)");
    f.execute("CREATE TABLE orders(id int PRIMARY KEY, user_id int)");
    let parts = f.parts();
    let mut txn = f.db().begin(Isolation::ReadCommitted);
    for (name, count) in [("users", 5000), ("orders", 20000)] {
        let entry = engine::catalog::TableEntry { def: parts.tables[name].clone(),
            heap: false, columns_ext: BTreeMap::new() };
        for id in 1..=count {
            let mut values = vec![Value::Int(id)];
            if name == "orders" { values.push(Value::Int((id - 1) % 5000 + 1)); }
            f.db().rows.insert(&mut txn, &entry, values, None, &|| false).unwrap();
        }
    }
    txn.commit().unwrap();
    let mut session = f.admin().sql_session();
    let mut p = session.prepare("SELECT count(*) FROM users").unwrap();
    p.exists_test_query("SELECT count(*) FROM users u WHERE EXISTS(SELECT 1/0 FROM orders o WHERE o.user_id=u.id)");
    let started = std::time::Instant::now();
    assert_eq!(session.execute_prepared(&p, &[], None).unwrap().rows[0].values, vec![Value::Int(5000)]);
    eprintln!("C4 v2 users=5000 orders=20000 elapsed={:?}", started.elapsed());
    session.execute("BEGIN", &[], None).unwrap();
    session.execute("INSERT INTO users VALUES(5001)", &[], None).unwrap();
    session.execute("INSERT INTO orders VALUES(20001,5001)", &[], None).unwrap();
    assert_eq!(session.execute_prepared(&p, &[], None).unwrap().rows[0].values, vec![Value::Int(5001)]);
    session.execute("ROLLBACK", &[], None).unwrap();
    assert_eq!(session.execute_prepared(&p, &[], None).unwrap().rows[0].values, vec![Value::Int(5000)]);
}

#[test]
fn internal_exists_aggregate_semantics_and_combined_retention() {
    use crate::relational::{Value, scan};
    let f = Fixture::new();
    f.execute("CREATE TABLE t(id int PRIMARY KEY, v text)");
    f.execute(&format!("INSERT INTO t VALUES(1,'{}'),(2,'{}')", "x".repeat(700), "y".repeat(700)));
    let mut session = f.admin().sql_session();
    for (child, expected) in [
        ("SELECT max(id) FROM t WHERE false", Ok(2)),
        ("SELECT max(id) FROM t HAVING false", Ok(0)),
        ("SELECT sum(1/(id-id)) FROM t", Err(CoreError::Sql(SqlError::DivisionByZero))),
        ("SELECT sum(1/(id-id)) FROM t OFFSET 1", Err(CoreError::Sql(SqlError::DivisionByZero))),
        ("SELECT sum(1/(id-id)) FROM t GROUP BY id", Err(CoreError::Sql(SqlError::DivisionByZero))),
        ("SELECT sum(1/(id-id)) FROM t LIMIT 0", Ok(0)),
    ] {
        let mut p = session.prepare("SELECT id FROM t").unwrap();
        p.exists_test_query(&format!("SELECT id FROM t WHERE EXISTS({child})"));
        assert_eq!(session.execute_prepared(&p, &[], None).map(|r| r.rows.len()), expected, "{child}");
    }
    for (sql, fails) in [
        ("SELECT id FROM t ORDER BY id+0", false),
        ("SELECT max(v) FROM t", false),
        ("SELECT id FROM t WHERE EXISTS(SELECT 1 FROM t i WHERE i.id=t.id)", false),
        ("SELECT id FROM t WHERE EXISTS(SELECT 1 FROM t i WHERE i.id=t.id) ORDER BY id+0", true),
        ("SELECT max(v) FROM t WHERE EXISTS(SELECT 1 FROM t i WHERE i.id=t.id)", true),
    ] {
        let mut p = session.prepare("SELECT id FROM t").unwrap();
        p.exists_test_query(sql);
        let result = scan::with_cursor_limit(2048, || session.execute_prepared(&p, &[], None));
        if fails { assert_eq!(result, Err(CoreError::Sql(SqlError::Limit)), "{sql}"); }
        else { assert!(result.is_ok(), "{sql}: {result:?}"); }
    }
    assert_eq!(session.execute("SELECT count(*) FROM t", &[], None).unwrap().rows[0].values, vec![Value::Int(2)]);
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
fn implicit_failures_leave_catalog_unchanged() {
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
    let sql = "ALTER TABLE t ADD COLUMN id bigint";
    assert!(matches!(
        f.admin().sql_session().execute(sql, &[], None),
        Err(CoreError::Sql(SqlError::DuplicateColumn))
    ));
    assert_eq!(f.parts(), before, "{sql}");
    assert_eq!(f.catalog_bytes(), bytes, "{sql}");
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
fn check_and_foreign_key_ddl_persist() {
    for sql in [
        "CREATE TABLE t (id bigint PRIMARY KEY, n bigint CHECK (n > 0))",
        "CREATE TABLE t (id bigint PRIMARY KEY, n bigint, CHECK (n > 0))",
        "CREATE TABLE t (id bigint PRIMARY KEY, p bigint REFERENCES q(id))",
        "CREATE TABLE t (id bigint PRIMARY KEY, p bigint, FOREIGN KEY (p) REFERENCES q(id))",
        "ALTER TABLE q ADD CHECK (n > 0)",
        "ALTER TABLE q ADD CONSTRAINT q_n_fkey FOREIGN KEY (n) REFERENCES q(id)",
    ] {
        let mut f = Fixture::new();
        f.execute("CREATE TABLE q (id bigint PRIMARY KEY, n bigint)");
        f.execute(sql);
        let parts = f.parts();
        assert!(
            parts.ext.constraints.values().flatten().any(|c| matches!(
                c.kind,
                relational::catalog_ext::ConstraintKind::Check { .. }
                    | relational::catalog_ext::ConstraintKind::ForeignKey { .. }
            )),
            "{sql}"
        );
        f.reopen();
        assert_eq!(f.parts(), parts, "{sql}");
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
fn post_image_check_is_valid() {
    let (tables, ext, next_id, _) =
        created_catalog("CREATE TABLE t (id bigint PRIMARY KEY, n bigint CHECK (n > 0))");
    check_post_image(&tables, &ext, next_id).unwrap();
}

#[test]
fn post_image_text_backed_column_types_are_valid() {
    for ty in [
        "varchar(10)",
        "char(3)",
        "uuid",
        "jsonb",
        "int[]",
        "bigint[]",
        "text[]",
    ] {
        let (tables, ext, next_id, _) =
            created_catalog(&format!("CREATE TABLE t (id bigint PRIMARY KEY, v {ty})"));
        check_post_image(&tables, &ext, next_id).unwrap();
    }
}

#[test]
fn post_image_primary_key_is_valid() {
    let (tables, ext, next_id, _) = created_catalog("CREATE TABLE t (id bigint PRIMARY KEY)");
    check_post_image(&tables, &ext, next_id).unwrap();
}

#[test]
fn text_backed_columns_match_legacy_and_reopen() {
    use relational::{DataType, Value};
    for (ty, data_type, literal, typmod) in [
        ("varchar(5)", DataType::Varchar, "'hello'", Some(9)),
        ("char(3)", DataType::Bpchar, "'x'", Some(7)),
        (
            "uuid",
            DataType::Uuid,
            "'550E8400-E29B-41D4-A716-446655440000'",
            None,
        ),
        ("jsonb", DataType::Jsonb, r#"'{"b":2,"a":1}'"#, None),
        ("int[]", DataType::IntArray, "'{1,-2,NULL}'", None),
        (
            "bigint[]",
            DataType::BigIntArray,
            "'{9223372036854775807,-2,NULL}'",
            None,
        ),
        ("text[]", DataType::TextArray, "'{hello,world,NULL}'", None),
    ] {
        let mut f = Fixture::new();
        let path = f.path.with_extension("column-types-legacy");
        let legacy = Admin::create(&path, Limits::default()).unwrap();
        let expected;
        {
            let mut a = legacy.sql_session();
            let mut b = f.admin().sql_session();
            parity(
                &mut a,
                &mut b,
                &format!("CREATE TABLE t (id bigint PRIMARY KEY, v {ty})"),
                &[],
            )
            .unwrap();
            parity(
                &mut a,
                &mut b,
                &format!("INSERT INTO t VALUES (1,{literal}),(2,NULL)"),
                &[],
            )
            .unwrap();
            expected = parity(&mut a, &mut b, "SELECT * FROM t ORDER BY id", &[]).unwrap();
            assert!(matches!(expected.rows[0].values[1], Value::Text(_)), "{ty}");
        }
        let parts = f.parts();
        let column = &parts.tables["t"].columns[1];
        assert_eq!(column.data_type, data_type);
        assert_eq!(
            parts.ext.columns.get(&column.id).and_then(|ext| ext.typmod),
            typmod
        );
        let features = engine::meta::validate_meta(f.db().kv.as_ref())
            .unwrap()
            .features;
        assert_ne!(features & engine::meta::TEXT_BACKED_COLUMN_TYPES, 0);
        f.reopen();
        assert_eq!(f.parts(), parts, "{ty}");
        assert_eq!(
            values(&mut f.admin().sql_session(), "SELECT * FROM t ORDER BY id"),
            expected
                .rows
                .iter()
                .map(|row| row.values.clone())
                .collect::<Vec<_>>(),
            "{ty}"
        );
        let mut a = legacy.sql_session();
        let mut b = f.admin().sql_session();
        parity(
            &mut a,
            &mut b,
            "UPDATE t SET v=NULL WHERE id=1 RETURNING *",
            &[],
        )
        .unwrap();
        parity(&mut a, &mut b, "SELECT * FROM t ORDER BY id", &[]).unwrap();
        drop((a, b, legacy));
        fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn text_column_revision_preserves_old_catalog_and_feature_bytes() {
    let mut f = Fixture::new();
    f.execute("CREATE TABLE q (id bigint PRIMARY KEY, v text)");
    f.execute("INSERT INTO q VALUES (1,'old')");
    let bytes = f.catalog_bytes();
    let features = f.db().kv.get(Keyspace::Meta, b"features").unwrap().unwrap();
    assert_eq!(
        u64::from_be_bytes(features.clone().try_into().unwrap())
            & engine::meta::TEXT_BACKED_COLUMN_TYPES,
        0
    );
    f.reopen();
    assert_eq!(f.catalog_bytes(), bytes);
    assert_eq!(
        f.db().kv.get(Keyspace::Meta, b"features").unwrap().unwrap(),
        features
    );
    {
        let mut session = f.admin().sql_session();
        session.execute("BEGIN", &[], None).unwrap();
        session
            .execute(
                "CREATE TABLE rolled (id bigint PRIMARY KEY, v varchar(5))",
                &[],
                None,
            )
            .unwrap();
        session.execute("ROLLBACK", &[], None).unwrap();
    }
    assert_eq!(f.catalog_bytes(), bytes);
    assert_eq!(
        f.db().kv.get(Keyspace::Meta, b"features").unwrap().unwrap(),
        features
    );
    f.execute("CREATE TABLE added (id bigint PRIMARY KEY, v uuid)");
    f.execute("DROP TABLE added");
    f.reopen();
    assert_ne!(
        engine::meta::validate_meta(f.db().kv.as_ref())
            .unwrap()
            .features
            & engine::meta::TEXT_BACKED_COLUMN_TYPES,
        0
    );
}

#[test]
fn varchar_length_errors_match_legacy_after_reopen() {
    let mut f = Fixture::new();
    let path = f.path.with_extension("typmod-legacy");
    let legacy = Admin::create(&path, Limits::default()).unwrap();
    {
        let mut a = legacy.sql_session();
        let mut b = f.admin().sql_session();
        parity(
            &mut a,
            &mut b,
            "CREATE TABLE t (id bigint PRIMARY KEY, v varchar(2), c char(3))",
            &[],
        )
        .unwrap();
        parity(&mut a, &mut b, "INSERT INTO t VALUES (1,'ab','x')", &[]).unwrap();
    }
    f.reopen();
    let mut a = legacy.sql_session();
    let mut b = f.admin().sql_session();
    parity(&mut a, &mut b, "INSERT INTO t VALUES (2,'abc','x')", &[]).unwrap_err();
    parity(&mut a, &mut b, "INSERT INTO t VALUES (2,'ab','abcd')", &[]).unwrap_err();
    parity(&mut a, &mut b, "SELECT * FROM t ORDER BY id", &[]).unwrap();
    drop((a, b, legacy));
    fs::remove_dir_all(path).unwrap();
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
    // DROP unpublishes and reclaims rows once no older snapshot remains.
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
fn v2_admission_limits() {
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
fn sink_failure_rolls_back_partial_rekey_and_column_rewrite() {
    use relational::Value::Int;
    let f = Fixture::new();
    f.execute("CREATE TABLE t (id bigint PRIMARY KEY,n bigint)");
    f.execute("INSERT INTO t VALUES (1,10),(2,20)");
    let mut s = f.admin().sql_session();
    for sql in [
        "UPDATE t SET id=3 WHERE id=1",
        "ALTER TABLE t ADD COLUMN d bigint DEFAULT 42",
    ] {
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
fn mutation_limits_keep_legacy_cap_but_page_v2_values() {
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
    assert_eq!(left, CoreError::Sql(SqlError::Limit));
    assert_eq!(b.execute(&sql, &[], None).unwrap().affected, 1025);
    assert!(values(&mut a, "SELECT * FROM t ORDER BY id").is_empty());
    assert_eq!(values(&mut b, "SELECT * FROM t ORDER BY id").len(), 1025);
    drop((a, b, legacy));
    fs::remove_dir_all(path).unwrap();
}

fn parity(
    legacy: &mut SqlSession,
    v2: &mut SqlSession,
    sql: &str,
    params: &[relational::Value],
) -> Result<relational::SqlResult, CoreError> {
    let left = legacy.execute(sql, params, None);
    let right = v2.execute(sql, params, None);
    match (&left, &right) {
        (Ok(left), Ok(right)) => {
            assert_eq!(left.columns, right.columns, "{sql}");
            assert_eq!(left.affected, right.affected, "{sql}");
            assert_eq!(
                left.rows.iter().map(|r| &r.values).collect::<Vec<_>>(),
                right.rows.iter().map(|r| &r.values).collect::<Vec<_>>(),
                "{sql}"
            );
        }
        (Err(left), Err(right)) => assert_eq!(left, right, "{sql}"),
        _ => panic!("{sql}: legacy={left:?}, v2={right:?}"),
    }
    left
}

#[test]
fn multitable_reads_and_mutations_match_legacy() {
    let f = Fixture::new();
    let path = f.path.with_extension("multitable-legacy");
    let legacy = Admin::create(&path, Limits::default()).unwrap();
    let mut a = legacy.sql_session();
    let mut b = f.admin().sql_session();
    for sql in [
        "CREATE TABLE a (id bigint PRIMARY KEY,k bigint,v text)",
        "CREATE TABLE b (id bigint PRIMARY KEY,k bigint,v text)",
        "CREATE TABLE c (id bigint PRIMARY KEY,bid bigint)",
        "CREATE TABLE dst (id bigint PRIMARY KEY,k bigint,v text)",
        "CREATE TABLE heap (k bigint,v text)",
        "INSERT INTO a VALUES (1,10,'alpha'),(2,20,'beta'),(3,NULL,NULL)",
        "INSERT INTO b VALUES (11,10,'apple'),(12,10,NULL),(13,30,'berry')",
        "INSERT INTO c VALUES (21,11),(22,13)",
        "SELECT a.id,b.id FROM a INNER JOIN b ON a.k=b.k ORDER BY 1,2",
        "SELECT a.id,b.id FROM a LEFT OUTER JOIN b ON a.k=b.k ORDER BY 1,2",
        "SELECT a.id,b.id,c.id FROM a LEFT JOIN b ON a.k=b.k LEFT JOIN c ON b.id=c.bid ORDER BY 1,2,3",
        "SELECT x.id,y.id FROM a x JOIN a y ON x.k=y.k ORDER BY 1,2",
        "SELECT a.id,b.id FROM a JOIN b ON true ORDER BY 1,2 LIMIT 3",
        "SELECT a.id,b.id FROM a JOIN b ON true ORDER BY 1,2 LIMIT 3 OFFSET 1",
        "SELECT a.id,count(*),count(b.id) FROM a LEFT JOIN b ON a.k=b.k GROUP BY a.id ORDER BY 1",
        "SELECT a.k,sum(b.id),min(b.id),max(b.id) FROM a JOIN b ON a.k=b.k GROUP BY a.k HAVING count(*)>1 ORDER BY 1",
        "SELECT id FROM a WHERE k IN (SELECT k FROM b) ORDER BY id",
        "SELECT id FROM a WHERE k NOT IN (SELECT k FROM b) ORDER BY id",
        "SELECT id FROM a WHERE k NOT IN (SELECT k FROM b WHERE false) ORDER BY id",
        "SELECT id FROM a WHERE k IN (SELECT b.k FROM b JOIN c ON b.id=c.bid) ORDER BY id",
        "SELECT id FROM a WHERE k IN (SELECT k FROM b WHERE id IN (SELECT bid FROM c)) ORDER BY id",
        "SELECT a.id,b.id FROM a JOIN b ON a.k=b.k WHERE b.id IN (SELECT bid FROM c) ORDER BY 1,2",
        "INSERT INTO dst SELECT a.id,b.id,a.v FROM a JOIN b ON a.k=b.k WHERE b.id=11 RETURNING *",
        "INSERT INTO dst SELECT id,k,v FROM a WHERE id=2 RETURNING *",
        "INSERT INTO dst SELECT id,k,v FROM a WHERE id=1 ON CONFLICT (id) DO NOTHING RETURNING *",
        "INSERT INTO dst SELECT id,k,v FROM a WHERE id=1 ON CONFLICT (id) DO UPDATE SET k=excluded.k RETURNING *",
        "INSERT INTO heap SELECT k,v FROM a WHERE id IN (SELECT id FROM dst) RETURNING *",
        "SELECT * FROM heap ORDER BY k,v",
        "INSERT INTO dst SELECT id+10,k,v FROM dst RETURNING *",
        "SELECT * FROM dst ORDER BY id",
        "UPDATE dst SET k=k+1 WHERE id IN (SELECT id FROM a WHERE k IN (SELECT k FROM b)) RETURNING *",
        "DELETE FROM dst WHERE id IN (SELECT id FROM a WHERE id=2) RETURNING *",
        "SELECT * FROM dst ORDER BY id",
        "BEGIN",
        "INSERT INTO b VALUES (14,20,'new')",
        "INSERT INTO dst SELECT id+100,k,v FROM a WHERE k IN (SELECT k FROM b) RETURNING *",
        "SELECT dst.id,b.id FROM dst JOIN b ON dst.k=b.k ORDER BY 1,2",
        "ROLLBACK",
        "SELECT * FROM dst ORDER BY id",
    ] {
        parity(&mut a, &mut b, sql, &[]).unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    }
    for (sql, params) in [
        (
            "SELECT a.id,b.id FROM a LEFT JOIN b ON a.k=b.k AND b.id=$1 ORDER BY 1,2",
            vec![relational::Value::Int(12)],
        ),
        (
            "SELECT id FROM a WHERE k IN (SELECT k FROM b WHERE id=$1) ORDER BY id",
            vec![relational::Value::Int(11)],
        ),
        (
            "INSERT INTO dst SELECT id+$1,k,v FROM a WHERE id=$2 RETURNING *",
            vec![relational::Value::Int(200), relational::Value::Int(3)],
        ),
        (
            "UPDATE dst SET k=$1 WHERE id IN (SELECT id FROM a WHERE id=$2) RETURNING *",
            vec![relational::Value::Int(77), relational::Value::Int(1)],
        ),
    ] {
        parity(&mut a, &mut b, sql, &params).unwrap();
    }
    // Exact legacy errors, including statements that fail after source staging.
    for sql in [
        "SELECT missing FROM a JOIN b ON a.id=b.id",
        "SELECT a.id FROM a JOIN absent ON a.id=absent.id",
        "SELECT id FROM a WHERE k IN (SELECT v FROM b)",
        "INSERT INTO dst SELECT id,k,v FROM a",
        "UPDATE dst SET id=11 WHERE id IN (SELECT id FROM a)",
        "INSERT INTO dst SELECT id+500,1/(id-2),v FROM a",
    ] {
        parity(&mut a, &mut b, sql, &[]).unwrap_err();
    }
    parity(&mut a, &mut b, "SELECT * FROM dst ORDER BY id", &[]).unwrap();
    drop((a, b, legacy));
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn legacy_unsupported_read_forms_keep_exact_errors() {
    let f = Fixture::new();
    let path = f.path.with_extension("unsupported-legacy");
    let legacy = Admin::create(&path, Limits::default()).unwrap();
    let mut a = legacy.sql_session();
    let mut b = f.admin().sql_session();
    for sql in [
        "CREATE TABLE a (id bigint PRIMARY KEY)",
        "CREATE TABLE b (id bigint PRIMARY KEY)",
        "INSERT INTO a VALUES (1)",
        "INSERT INTO b VALUES (2)",
    ] {
        parity(&mut a, &mut b, sql, &[]).unwrap();
    }
    assert_eq!(
        parity(&mut a, &mut b, "SELECT id FROM a WHERE EXISTS (SELECT id FROM b)", &[])
            .unwrap().rows[0].values,
        vec![relational::Value::Int(1)],
    );
    for sql in [
        "SELECT id FROM a UNION SELECT id FROM b",
        "SELECT id FROM a UNION ALL SELECT id FROM b",
        "SELECT id FROM a INTERSECT SELECT id FROM b",
        "SELECT id FROM a EXCEPT SELECT id FROM b",
        "WITH q AS (SELECT id FROM a) SELECT id FROM q",
        "SELECT a.id,b.id FROM a RIGHT JOIN b ON a.id=b.id",
        "SELECT a.id,b.id FROM a FULL OUTER JOIN b ON a.id=b.id",
        "SELECT (SELECT id FROM b) FROM a",
        "SELECT id FROM a WHERE id=(SELECT id FROM b)",
        "SELECT id FROM (SELECT id FROM a) q",
        "INSERT INTO b SELECT id FROM a UNION SELECT id FROM b",
    ] {
        parity(&mut a, &mut b, sql, &[]).unwrap_err();
    }
    drop((a, b, legacy));
    fs::remove_dir_all(path).unwrap();
}

struct ScanHookGuard;
impl Drop for ScanHookGuard {
    fn drop(&mut self) {
        AFTER_TABLE_SCAN.with(|hook| hook.replace(None));
    }
}

#[test]
fn all_statement_sources_share_one_snapshot_despite_concurrent_commit() {
    use relational::Value::Int;
    use std::sync::mpsc;
    use std::time::Duration;
    // Exercise implicit and explicit SQL transactions. The second statement
    // must see the writer, while every read of the first sees the old snapshot.
    for explicit in [false, true] {
        for sql in [
            "SELECT a.n,b.n FROM a JOIN b ON a.id=b.id",
            "SELECT n FROM a WHERE n IN (SELECT n FROM b)",
            "SELECT sum(a.n::integer),sum(b.n::integer) FROM a JOIN b ON a.id=b.id",
            "INSERT INTO dst SELECT a.n,b.n FROM a JOIN b ON a.id=b.id RETURNING *",
            "UPDATE dst SET n=n+1 WHERE n IN (SELECT n FROM a WHERE n IN (SELECT n FROM b)) RETURNING *",
            "DELETE FROM dst WHERE n IN (SELECT n FROM a WHERE n IN (SELECT n FROM b)) RETURNING *",
        ] {
            let f = Fixture::new();
            for ddl in [
                "CREATE TABLE a (id bigint PRIMARY KEY,n bigint)",
                "CREATE TABLE b (id bigint PRIMARY KEY,n bigint)",
                "CREATE TABLE dst (id bigint PRIMARY KEY,n bigint)",
                "INSERT INTO a VALUES (1,10)",
                "INSERT INTO b VALUES (1,10)",
            ] {
                f.execute(ddl);
            }
            if sql.starts_with("UPDATE") || sql.starts_with("DELETE") {
                f.execute("INSERT INTO dst VALUES (10,10)");
            }
            let (start, started) = mpsc::sync_channel(0);
            let (done, finished) = mpsc::sync_channel(0);
            let admin = f.admin().clone();
            let writer = std::thread::spawn(move || {
                started.recv_timeout(Duration::from_secs(30)).unwrap();
                let mut s = admin.sql_session();
                for sql in ["BEGIN", "UPDATE a SET n=20", "UPDATE b SET n=20", "COMMIT"] {
                    s.execute(sql, &[], None).unwrap();
                }
                done.send(()).unwrap();
            });
            let mut session = f.admin().sql_session();
            if explicit {
                session.execute("BEGIN", &[], None).unwrap();
            }
            let mut scans = Vec::new();
            let observed = Arc::new(std::sync::Mutex::new(Vec::new()));
            let record = observed.clone();
            let mut first = true;
            AFTER_TABLE_SCAN.with(|hook| {
                hook.replace(Some(Box::new(move |name| {
                    record.lock().unwrap().push(name.to_owned());
                    if first {
                        first = false;
                        start.send(()).unwrap();
                        finished.recv_timeout(Duration::from_secs(30)).unwrap();
                    }
                })))
            });
            let guard = ScanHookGuard;
            let result = values(&mut session, sql);
            drop(guard);
            writer.join().unwrap();
            scans.extend(observed.lock().unwrap().iter().cloned());
            assert_eq!(&scans[..2], &["a", "b"], "{sql}");
            let expected = if sql.starts_with("UPDATE") {
                vec![vec![Int(10), Int(11)]]
            } else if sql.starts_with("SELECT n") {
                vec![vec![Int(10)]]
            } else {
                vec![vec![Int(10), Int(10)]]
            };
            assert_eq!(result, expected, "explicit={explicit}: {sql}");
            assert_eq!(
                values(&mut session, "SELECT a.n,b.n FROM a JOIN b ON a.id=b.id"),
                vec![vec![Int(20), Int(20)]]
            );
            if sql.starts_with("INSERT") {
                assert_eq!(
                    values(&mut session, "SELECT * FROM dst"),
                    vec![vec![Int(10), Int(10)]]
                );
            }
            if explicit {
                session.execute("COMMIT", &[], None).unwrap();
            }
        }
    }
}

#[test]
fn schemas_and_user_types_match_legacy_and_persist() {
    use relational::Value;
    let mut f = Fixture::new();
    let path = f.path.with_extension("schema-types-legacy");
    let legacy = Admin::create(&path, Limits::default()).unwrap();
    let expected;
    {
        let mut a = legacy.sql_session();
        let mut b = f.admin().sql_session();
        for sql in [
            "CREATE SCHEMA app",
            "CREATE SCHEMA other",
            "SELECT 1",
            "CREATE TYPE app.mood AS ENUM ('sad','ok','happy')",
            "SELECT 1",
            "CREATE DOMAIN app.positive AS integer CHECK (VALUE > 0)",
            "CREATE DOMAIN app.short_text AS varchar(3)",
            "CREATE TYPE app.pair AS (n integer, label varchar(3))",
            "CREATE TABLE public.t (id integer PRIMARY KEY, v text)",
            "CREATE TABLE app.t (id integer PRIMARY KEY, m app.mood, n app.positive, s app.short_text)",
            "CREATE TABLE other.t (id integer PRIMARY KEY, v text)",
            "INSERT INTO public.t VALUES (1,'public')",
            "INSERT INTO app.t VALUES (1,'happy',1,'abc'),(2,'sad',2,'x'),(3,'ok',3,NULL)",
            "INSERT INTO other.t VALUES (1,'other')",
            "SELECT id,m,n,s FROM app.t ORDER BY m",
            "SELECT ROW(7,'abc')::app.pair",
        ] {
            parity(&mut a, &mut b, sql, &[]).unwrap();
        }
        assert_eq!(
            parity(&mut a, &mut b, "CREATE SCHEMA app", &[]).unwrap_err(),
            CoreError::DuplicateSchema
        );
        parity(&mut a, &mut b, "CREATE SCHEMA IF NOT EXISTS app", &[]).unwrap();
        let ordered = values(&mut b, "SELECT m FROM app.t ORDER BY m");
        assert_eq!(
            ordered,
            vec![
                vec![Value::Text("sad".into())],
                vec![Value::Text("ok".into())],
                vec![Value::Text("happy".into())]
            ]
        );
        let error = parity(
            &mut a,
            &mut b,
            "INSERT INTO app.t VALUES (4,'ok',0,'a')",
            &[],
        )
        .unwrap_err();
        assert!(
            matches!(error, CoreError::Sql(relational::SqlError::CheckViolation)),
            "{error:?}"
        );
        parity(
            &mut a,
            &mut b,
            "INSERT INTO app.t VALUES (4,'bad',1,'a')",
            &[],
        )
        .unwrap_err();
        parity(
            &mut a,
            &mut b,
            "INSERT INTO app.t VALUES (4,'ok',1,'long')",
            &[],
        )
        .unwrap_err();
        a.set_search_path("app,public", "postgres");
        b.set_search_path("app,public", "postgres");
        assert_eq!(
            b.prepare("INSERT INTO t VALUES (4,'ok',1,'a')")
                .unwrap()
                .table(),
            Some("app\0t")
        );
        parity(
            &mut a,
            &mut b,
            "UPDATE public.t SET v='public2' WHERE id=1 RETURNING *",
            &[],
        )
        .unwrap();
        expected = parity(&mut a, &mut b, "SELECT id,m,n,s FROM t ORDER BY m", &[]).unwrap();
        parity(
            &mut a,
            &mut b,
            "UPDATE t SET n=n+1 WHERE id=1 RETURNING id,n",
            &[],
        )
        .unwrap();
        a.set_search_path("other,public", "postgres");
        b.set_search_path("other,public", "postgres");
        parity(&mut a, &mut b, "SELECT * FROM t", &[]).unwrap();
        parity(
            &mut a,
            &mut b,
            "CREATE TABLE chosen (id integer PRIMARY KEY)",
            &[],
        )
        .unwrap();
    }
    let parts = f.parts();
    assert!(parts.tables.contains_key("app\0t"));
    assert!(parts.tables.contains_key("other\0chosen"));
    let features = engine::meta::validate_meta(f.db().kv.as_ref())
        .unwrap()
        .features;
    assert_ne!(features & engine::meta::SCHEMAS_AND_USER_TYPES, 0);
    f.reopen();
    assert_eq!(f.parts(), parts);
    {
        let mut a = legacy.sql_session();
        let mut b = f.admin().sql_session();
        a.set_search_path("app,public", "postgres");
        b.set_search_path("app,public", "postgres");
        let reopened = parity(&mut a, &mut b, "SELECT id,m,n,s FROM t ORDER BY m", &[]).unwrap();
        assert_eq!(reopened.rows.len(), expected.rows.len());
        parity(&mut a, &mut b, "SELECT ROW(7,'abc')::app.pair", &[]).unwrap();
        parity(&mut a, &mut b, "DROP TYPE app.mood", &[]).unwrap_err();
        parity(&mut a, &mut b, "DROP SCHEMA app", &[]).unwrap_err();
        for sql in [
            "DROP TABLE app.t",
            "DROP TYPE app.mood",
            "DROP DOMAIN app.positive",
            "DROP DOMAIN app.short_text",
            "DROP TYPE app.pair",
            "DROP SCHEMA app",
            "DROP SCHEMA other CASCADE",
        ] {
            parity(&mut a, &mut b, sql, &[]).unwrap();
        }
    }
    f.reopen();
    assert!(f.parts().ext.schemas.is_empty());
    assert!(f.parts().ext.types.is_empty());
    assert_ne!(
        engine::meta::validate_meta(f.db().kv.as_ref())
            .unwrap()
            .features
            & engine::meta::SCHEMAS_AND_USER_TYPES,
        0
    );
    drop(legacy);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn schema_type_feature_is_transactional_and_old_catalog_stays_identical() {
    let mut f = Fixture::new();
    f.execute("CREATE TABLE t (id integer PRIMARY KEY)");
    let bytes = f.catalog_bytes();
    let features = f.db().kv.get(Keyspace::Meta, b"features").unwrap();
    {
        let mut s = f.admin().sql_session();
        for sql in [
            "BEGIN",
            "CREATE SCHEMA rolled",
            "CREATE TYPE e AS ENUM ('a','b')",
            "ROLLBACK",
        ] {
            s.execute(sql, &[], None).unwrap();
        }
    }
    assert_eq!(f.catalog_bytes(), bytes);
    f.reopen();
    assert_eq!(f.catalog_bytes(), bytes);
    assert_eq!(
        f.db().kv.get(Keyspace::Meta, b"features").unwrap(),
        features
    );
}

#[test]
fn sql_savepoints_match_legacy_including_aborted_recovery() {
    let f = Fixture::new();
    let legacy_path = f.path.with_extension("legacy");
    let legacy = Admin::create(&legacy_path, Limits::default()).unwrap();
    for admin in [&legacy, f.admin()] {
        let mut sql = admin.sql_session();
        assert!(matches!(
            sql.execute("SAVEPOINT s", &[], None),
            Err(CoreError::Sql(SqlError::NoActiveTransaction))
        ));
        for statement in [
            "CREATE TABLE marks (id bigint PRIMARY KEY)",
            "BEGIN",
            "INSERT INTO marks VALUES (1)",
            "SAVEPOINT s",
            "INSERT INTO marks VALUES (2)",
            "SAVEPOINT s",
            "INSERT INTO marks VALUES (3)",
            "SAVEPOINT child",
        ] {
            sql.execute(statement, &[], None).unwrap();
        }
        assert!(matches!(
            sql.execute("INSERT INTO marks VALUES (1)", &[], None),
            Err(CoreError::Sql(SqlError::Duplicate))
        ));
        assert!(matches!(
            sql.execute("RELEASE s", &[], None),
            Err(CoreError::Sql(SqlError::Aborted))
        ));
        sql.execute("ROLLBACK TO SAVEPOINT s", &[], None).unwrap();
        assert!(!sql.is_aborted());
        assert!(matches!(
            sql.execute("ROLLBACK TO child", &[], None),
            Err(CoreError::Sql(SqlError::InvalidSavepoint))
        ));
        sql.execute("ROLLBACK TO s", &[], None).unwrap();
        sql.execute("RELEASE SAVEPOINT s", &[], None).unwrap();
        sql.execute("ROLLBACK TO s", &[], None).unwrap();
        sql.execute("INSERT INTO marks VALUES (4)", &[], None)
            .unwrap();
        sql.execute("ROLLBACK TO s", &[], None).unwrap();
        sql.execute("RELEASE s", &[], None).unwrap();
        sql.execute("COMMIT", &[], None).unwrap();
        let result = sql
            .execute("SELECT id FROM marks ORDER BY id", &[], None)
            .unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].values, [relational::Value::Int(1)]);
        sql.execute("BEGIN", &[], None).unwrap();
        assert!(matches!(
            sql.execute("ROLLBACK TO s", &[], None),
            Err(CoreError::Sql(SqlError::InvalidSavepoint))
        ));
        sql.rollback();
    }
    drop(legacy);
    fs::remove_dir_all(legacy_path).unwrap();
}

#[test]
fn normalized_copy_defaults_atomicity_and_savepoint_match_legacy() {
    let f = Fixture::new();
    let legacy_path = f.path.with_extension("legacy");
    let legacy = Admin::create(&legacy_path, Limits::default()).unwrap();
    for admin in [&legacy, f.admin()] {
        use relational::Value as V;
        let mut sql = admin.sql_session();
        sql.execute("CREATE SEQUENCE copy_seq", &[], None).unwrap();
        sql.execute(
            "CREATE TABLE copied (id bigint PRIMARY KEY, v bigint DEFAULT nextval('copy_seq'))",
            &[],
            None,
        )
        .unwrap();
        let clock = relational::StatementClock {
            unavailable: None,
            transaction_us: 0,
            statement_us: 0,
            zone: None,
            wall: None,
        };
        sql.execute("BEGIN", &[], None).unwrap();
        sql.execute("SAVEPOINT before_copy", &[], None).unwrap();
        let (copy, p) = sql.prepare_copy("COPY copied(id) FROM STDIN").unwrap();
        assert_eq!(sql.copy_columns(&copy).unwrap().len(), 1);
        let (normalized, rows) = sql
            .normalize_copy_rows_at(
                &p,
                vec![vec![V::Text("1".into())], vec![V::Text("2".into())]],
                &clock,
            )
            .unwrap();
        assert_eq!(
            sql.ingest_copy_at(&normalized, rows, &clock)
                .unwrap()
                .affected,
            2
        );
        sql.execute("ROLLBACK TO before_copy", &[], None).unwrap();
        assert!(
            sql.execute("SELECT * FROM copied", &[], None)
                .unwrap()
                .rows
                .is_empty()
        );
        let (_, p) = sql.prepare_copy("COPY copied(id) FROM STDIN").unwrap();
        let (normalized, rows) = sql
            .normalize_copy_rows_at(&p, vec![vec![V::Int(3)]], &clock)
            .unwrap();
        assert_eq!(rows[0], [V::Int(3), V::Int(3)]);
        sql.ingest_copy_at(&normalized, rows, &clock).unwrap();
        let (_, p) = sql.prepare_copy("COPY copied FROM STDIN").unwrap();
        assert!(matches!(
            sql.ingest_copy(
                &p,
                vec![vec![V::Int(4), V::Int(4)], vec![V::Int(3), V::Int(5)]]
            ),
            Err(CoreError::Sql(SqlError::Duplicate))
        ));
        sql.execute("ROLLBACK TO before_copy", &[], None).unwrap();
        sql.execute("COMMIT", &[], None).unwrap();
        assert!(
            sql.execute("SELECT * FROM copied", &[], None)
                .unwrap()
                .rows
                .is_empty()
        );
    }
    drop(legacy);
    fs::remove_dir_all(legacy_path).unwrap();
}

#[path = "v2_sql_index_tests.rs"]
mod index_tests;

#[path = "v2_sql_alter_tests.rs"]
mod alter_tests;

#[path = "v2_sql_constraint_tests.rs"]
mod constraint_tests;

#[path = "v2_sql_table_free_tests.rs"]
mod table_free_tests;

#[path = "v2_sql_ident_tests.rs"]
mod ident_tests;

#[test]
fn fk_validation_rejects_non_read_committed_snapshot() {
    for isolation in [Isolation::RepeatableRead, Isolation::Serializable] {
        let f = Fixture::new();
        f.execute("CREATE TABLE p(id int PRIMARY KEY)");
        f.execute("CREATE TABLE c(id int PRIMARY KEY, p int REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)");
        f.execute("INSERT INTO p VALUES(1)");
        let mut s = f.admin().sql_session();
        s.execute("BEGIN", &[], None).unwrap();
        s.v2_transaction = Some(f.db().begin(isolation));
        assert_eq!(
            s.execute("INSERT INTO c VALUES(1,1)", &[], None),
            Err(CoreError::Sql(SqlError::Conflict))
        );
        s.execute("ROLLBACK", &[], None).unwrap();
        s.execute("BEGIN", &[], None).unwrap();
        s.execute("INSERT INTO c VALUES(1,1)", &[], None).unwrap();
        let mut pending = s.deferral.clone();
        let mut txn = f.db().begin(isolation);
        assert_eq!(
            validate_pending(f.db(), &mut txn, &mut pending),
            Err(CoreError::Sql(SqlError::Conflict))
        );
        s.execute("ROLLBACK", &[], None).unwrap();
    }
}

#[test]
fn copy_input_snapshot_savepoint_drop_and_forward_fk() {
    use relational::Value as V;
    let f = Fixture::new();
    let mut sql = f.admin().sql_session();
    sql.execute("CREATE TABLE copied (id INT PRIMARY KEY)", &[], None).unwrap();
    let clock = relational::StatementClock { unavailable: None, transaction_us: 0,
        statement_us: 0, zone: None, wall: None };
    sql.execute("BEGIN", &[], None).unwrap();
    sql.execute("INSERT INTO copied VALUES (1)", &[], None).unwrap();
    sql.execute("SAVEPOINT before_copy", &[], None).unwrap();
    let (_, p) = sql.prepare_copy("COPY copied FROM STDIN").unwrap();
    assert!(sql.begin_copy_input().unwrap());
    sql.feed_copy_input_at(&p, vec![vec![V::Int(2)]], &clock).unwrap();
    let mut observer = f.admin().sql_session();
    assert!(observer.execute("SELECT * FROM copied", &[], None).unwrap().rows.is_empty());
    assert!(sql.feed_copy_input_at(&p, vec![vec![V::Int(1)]], &clock).is_err());
    sql.execute("ROLLBACK TO before_copy", &[], None).unwrap();
    assert_eq!(sql.execute("SELECT * FROM copied", &[], None).unwrap().rows.len(), 1);
    sql.execute("COMMIT", &[], None).unwrap();
    let (_, p) = sql.prepare_copy("COPY copied FROM STDIN").unwrap();
    sql.begin_copy_input().unwrap();
    sql.feed_copy_input_at(&p, vec![vec![V::Int(2)]], &clock).unwrap();
    drop(sql);
    assert_eq!(observer.execute("SELECT * FROM copied", &[], None).unwrap().rows.len(), 1);
    observer.execute("CREATE TABLE tree (id INT PRIMARY KEY, parent INT REFERENCES tree(id))", &[], None).unwrap();
    let (_, p) = observer.prepare_copy("COPY tree FROM STDIN").unwrap();
    observer.begin_copy_input().unwrap();
    observer.feed_copy_input_at(&p, vec![vec![V::Int(1), V::Int(2)]], &clock).unwrap();
    observer.feed_copy_input_at(&p, vec![vec![V::Int(2), V::Null]], &clock).unwrap();
    assert_eq!(observer.finish_copy_input_at(&p, &clock).unwrap().affected, 2);
}
