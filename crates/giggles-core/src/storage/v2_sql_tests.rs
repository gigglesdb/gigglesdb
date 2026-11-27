#[path = "v2_cascade_point_tests.rs"]
mod v2_cascade_point_tests;
#[path = "v2_mutation_point_tests.rs"]
mod mutation_point;
#[path = "v2_view_ddl_tests.rs"]
mod view_ddl;

use super::*;
use engine::{
    catalog_codec, codec,
    hlc::Ts,
    kv::{Direction, Durability, Keyspace, WriteBatch},
    mvcc,
};
use std::ops::Bound;

#[test]
fn prepared_dml_uses_current_search_path_defaults_and_values_error_order() {
    use relational::Value;

    let f = Fixture::new();
    for sql in [
        "CREATE SCHEMA alternate",
        "CREATE TABLE public.t(id int PRIMARY KEY, v int NOT NULL DEFAULT 10)",
        "CREATE TABLE alternate.t(id int PRIMARY KEY, v int NOT NULL DEFAULT 20)",
    ] {
        f.execute(sql);
    }
    let mut session = f.admin().sql_session();
    session.set_search_path("public", "postgres");
    let insert = session.prepare("INSERT INTO t(id) VALUES ($1) RETURNING id,v").unwrap();
    let update = session.prepare("UPDATE t SET v=v+1 WHERE id=$1 RETURNING id,v").unwrap();
    let delete = session.prepare("DELETE FROM t WHERE id=$1 RETURNING id,v").unwrap();
    let id = [Value::Int(1)];
    let execute = |session: &mut SqlSession, statement: &relational::Prepared, v| {
        let result = session.execute_prepared(statement, &id, None).unwrap();
        assert_eq!(result.affected, 1);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].values, [Value::Int(1), Value::Int(v)]);
    };
    execute(&mut session, &insert, 10);
    execute(&mut session, &update, 11);
    execute(&mut session, &delete, 11);

    session.set_search_path("alternate,public", "postgres");
    execute(&mut session, &insert, 20);
    execute(&mut session, &update, 21);
    execute(&mut session, &delete, 21);

    f.execute("ALTER TABLE alternate.t ALTER COLUMN v SET DEFAULT 30");
    f.execute("ALTER TABLE alternate.t ADD COLUMN added int DEFAULT 40");
    execute(&mut session, &insert, 30);
    assert_eq!(values(&mut session, "SELECT added FROM t"), [vec![Value::Int(40)]]);
    execute(&mut session, &update, 31);
    execute(&mut session, &delete, 31);

    // Fail in the second and last input rows, on opposite sides of a page boundary.
    for (first, last, state) in [
        ("NULL", "$1::text::integer", "23502"),
        ("$1::text::integer", "NULL", "22P02"),
    ] {
        let rows = (0..=200)
            .map(|id| {
                let value = match id {
                    1 => first,
                    200 => last,
                    _ => "DEFAULT",
                };
                format!("({id},{value})")
            })
            .collect::<Vec<_>>()
            .join(",");
        let statement = session.prepare(&format!("INSERT INTO t(id,v) VALUES {rows} RETURNING id,v")).unwrap();
        let error = session.execute_prepared(&statement, &[Value::Text("bad".into())], None).unwrap_err();
        assert_eq!(crate::sqlstate(&error), state, "{error:?}");
        assert!(values(&mut session, "SELECT id FROM t ORDER BY id").is_empty());
    }
    assert!(values(&mut session, "SELECT id FROM public.t ORDER BY id").is_empty());
}

#[test]
fn sink_binding_falls_back_when_the_final_catalog_changes_relation_resolution() {
    let f = Fixture::new();
    f.execute("CREATE SCHEMA alternate");
    f.execute("CREATE TABLE public.first_rows(id int PRIMARY KEY)");
    f.execute("CREATE TABLE alternate.second_rows(id int PRIMARY KEY)");
    f.execute("CREATE INDEX t ON public.first_rows(id)");
    let mut session = f.admin().sql_session();
    session.set_search_path("alternate,public", "postgres");
    let prepared = session.prepare("DROP INDEX t").unwrap();
    let db = f.db();
    let image = || db.catalog().snapshot(&mut db.begin(Isolation::ReadCommitted))
        .unwrap().image().unwrap();
    let preliminary = image();
    let memo = relational::SchemaBindingMemo::default();
    let bound = memo.at(&preliminary).bind(&prepared).unwrap();
    assert_eq!(bound.table(), Some("first_rows"));

    f.execute("CREATE INDEX t ON alternate.second_rows(id)");
    let final_catalog = image();
    let mut fork = preliminary.catalog_image_for_statement(0, 0);
    fork.catalog_indexes(final_catalog.secondary_descriptors_engine()).unwrap();
    let selected = prepared.bind_schemas_with(&fork, Some(memo.at(&preliminary))).unwrap();
    assert_eq!(selected.table(), Some("alternate\0second_rows"));
}

#[test]
fn schema_memo_rebinds_after_savepoint_restores_the_catalog() {
    use relational::Value;

    let f = Fixture::new();
    f.execute("CREATE SCHEMA alternate");
    f.execute("CREATE TABLE t(id int PRIMARY KEY, v int DEFAULT 10)");
    let mut session = f.admin().sql_session();
    session.set_search_path("alternate,public", "postgres");
    let prepared = session.prepare("INSERT INTO t(id) VALUES ($1) RETURNING v").unwrap();

    for sql in [
        "BEGIN",
        "SAVEPOINT before_ddl",
        "CREATE TABLE alternate.t(id int PRIMARY KEY, v int DEFAULT 20)",
    ] {
        session.execute(sql, &[], None).unwrap();
    }
    let run = |session: &mut SqlSession, expected| {
        let result = session.execute_prepared(&prepared, &[Value::Int(1)], None).unwrap();
        assert_eq!(result.rows[0].values, [Value::Int(expected)]);
    };
    let private = session.bind_relation_context(&prepared).unwrap();
    assert_eq!(private.table(), Some("alternate\0t"));
    run(&mut session, 20);
    session.bind_relation_context(&prepared).unwrap();
    session.execute("ROLLBACK TO SAVEPOINT before_ddl", &[], None).unwrap();

    let restored = session.bind_relation_context(&prepared).unwrap();
    assert_eq!(restored.table(), Some("t"));
    run(&mut session, 10);
    session.execute("COMMIT", &[], None).unwrap();
}

#[test]
fn occupancy_uses_the_statement_catalog_image_including_private_unique_ddl() {
    let f = Fixture::new();
    f.execute("CREATE TABLE t(id int PRIMARY KEY,k text)");
    let db = f.db();
    let mut txn = db.begin(Isolation::ReadCommitted);
    let committed = db.catalog().snapshot(&mut txn).unwrap().image().unwrap();
    drop(txn);
    let mut session = f.admin().sql_session();
    let insert_case_variant = "INSERT INTO t VALUES (2,'ABC')";

    let shared_check = |committed: Arc<relational::Relational>| {
        OCCUPANCY_IMAGE.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move |snapshot, image| {
                assert!(Arc::ptr_eq(image, &snapshot.image().unwrap()));
                assert!(Arc::ptr_eq(image, &committed));
                assert!(snapshot.parts.indexes.is_empty());
            }));
        });
    };
    let checked = |statement| {
        OCCUPANCY_IMAGE.with(|hook| assert!(hook.borrow().is_none(), "{statement}"))
    };
    // A primary-key move uses the occupancy publisher on a deterministic table.
    session
        .execute("INSERT INTO t VALUES (10,'abc')", &[], None)
        .unwrap();
    shared_check(committed.clone());
    session
        .execute("UPDATE t SET id=1 WHERE id=10", &[], None)
        .unwrap();
    checked("committed catalog before private DDL");

    for sql in [
        "BEGIN",
        "CREATE COLLATION ci(provider=icu,locale='und-u-ks-level2',deterministic=false)",
        "CREATE UNIQUE INDEX t_ci ON t(k COLLATE ci)",
    ] {
        session.execute(sql, &[], None).unwrap();
    }
    let before_ddl = committed.clone();
    OCCUPANCY_IMAGE.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move |snapshot, image| {
            assert!(Arc::ptr_eq(image, &snapshot.image().unwrap()));
            assert!(!Arc::ptr_eq(image, &before_ddl));
            assert_eq!(snapshot.parts.indexes[0].name, "t_ci");
            assert!(image.has_nondeterministic_keys(&snapshot.parts.tables["t"]));
        }));
    });
    let error = session.execute(insert_case_variant, &[], None).unwrap_err();
    checked("private catalog with a nondeterministic index");
    assert_eq!(error, CoreError::Sql(SqlError::Duplicate));
    assert_eq!(crate::sqlstate(&error), "23505");
    assert_eq!(
        relational::take_sql_error_message().as_deref(),
        Some("duplicate key value violates unique constraint \"t_ci\"")
    );

    session.execute("ROLLBACK", &[], None).unwrap();
    assert_eq!(
        session
            .execute(insert_case_variant, &[], None)
            .unwrap()
            .affected,
        1
    );
    shared_check(committed);
    session
        .execute("UPDATE t SET id=3 WHERE id=2", &[], None)
        .unwrap();
    checked("committed catalog after rollback");
}

#[test]
fn native_coercion_uses_transaction_private_type_metadata() {
    use giggles_pg_types::{Numeric, temporal::{ParserSettings, Timestamp}};
    use relational::Value;

    let f = Fixture::new();
    let mut session = f.admin().sql_session();
    for sql in [
        "BEGIN",
        "CREATE TYPE mood AS ENUM ('ok')",
        "CREATE TABLE inputs (id bigint PRIMARY KEY, m mood, c char(3), u uuid, n numeric(4,2), t timestamp(3))",
    ] {
        session.execute(sql, &[], None).unwrap();
    }

    let table = session.native_table_engine("inputs").unwrap();
    let outside = f.admin().sql_session();
    assert!(matches!(
        outside.native_table_engine("inputs"),
        Err(CoreError::Sql(SqlError::UnknownTable))
    ));

    let text = |s: &str| Value::Text(s.into());
    let numeric = |s: &str| Value::Numeric(Numeric::parse(s).unwrap());
    let timestamp = |s: &str| Value::Timestamp(Timestamp::parse(s, &ParserSettings::default()).unwrap());
    for (column, input, expected) in [
        (1, text("ok"), Ok(text("ok"))),
        (1, text("other"), Err(SqlError::InvalidTextRepresentation)),
        (2, text("x"), Ok(text("x  "))),
        (2, text("long"), Err(SqlError::StringDataRightTruncation)),
        (
            3,
            text("A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11"),
            Ok(text("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")),
        ),
        (3, text("bad uuid"), Err(SqlError::InvalidTextRepresentation)),
        (4, numeric("12.345"), Ok(numeric("12.35"))),
        (4, numeric("99.995"), Err(SqlError::Overflow)),
        (
            5,
            timestamp("2024-02-29 12:34:56.7896"),
            Ok(timestamp("2024-02-29 12:34:56.790")),
        ),
    ] {
        let column = &table.columns[column];
        assert_native_coercion(&f, &session, column, input, expected.map_err(CoreError::Sql));
    }
    session.execute("ROLLBACK", &[], None).unwrap();
}

fn assert_native_coercion(
    f: &Fixture,
    session: &SqlSession,
    column: &relational::ColumnDef,
    input: relational::Value,
    expected: Result<relational::Value, CoreError>,
) {
    let actual = session.coerce_column_engine(column, input.clone());
    let reference = f.db().metadata_state(session.v2_transaction.as_ref()).and_then(|owned| {
        relational::coerce_to_column(&owned.sql, column, input.clone()).map_err(CoreError::Sql)
    });
    assert_eq!(actual, reference, "column={} input={input:?}", column.name);
    assert_eq!(reference, expected, "column={} input={input:?}", column.name);
}

#[test]
fn native_coercion_respects_isolation_after_committed_typmod_change() {
    use giggles_pg_types::character::{CharKind, CharTypmod};
    use relational::Value;

    let f = Fixture::new();
    f.execute("CREATE TABLE inputs (id bigint PRIMARY KEY, v varchar(3))");
    let mut rr = f.admin().sql_session();
    rr.set_isolation(crate::SqlIsolation::RepeatableRead);
    rr.begin_native().unwrap();
    let mut rc = f.admin().sql_session();
    rc.begin_native().unwrap();
    // BEGIN and metadata inspection alone do not pin the RR snapshot.
    for reader in [&mut rr, &mut rc] {
        reader.execute("SELECT 1", &[], None).unwrap();
        assert!(reader.v2_transaction.as_ref().unwrap().current_snapshot_ts().is_some());
    }
    let table = rr.native_table_engine("inputs").unwrap();
    let column = &table.columns[1];
    let value = Value::Text("four".into());
    let too_long = Err(CoreError::Sql(SqlError::StringDataRightTruncation));
    for session in [&rr, &rc] {
        assert_native_coercion(&f, session, column, value.clone(), too_long.clone());
    }

    // Change only the typmod through the catalog DDL transaction so both
    // sessions can coerce against the same column ID across the commit.
    let db = f.db();
    let mut writer = db.begin(Isolation::ReadCommitted);
    let before = db.catalog().parts(&mut writer).unwrap();
    let mut after = before.clone();
    after.ext.columns.get_mut(&column.id).unwrap().typmod =
        Some(CharTypmod::new(CharKind::Varchar, 5).unwrap().to_typmod());
    db.catalog()
        .apply_diff(&mut writer, &before, &after, &(0..0), None, &|| false)
        .unwrap();
    db.catalog().commit_ddl(writer).unwrap();

    rr.begin_native_step().unwrap();
    assert_native_coercion(&f, &rr, column, value.clone(), too_long.clone());
    assert_native_coercion(&f, &rc, column, value.clone(), too_long);
    rc.begin_native_step().unwrap();
    assert_native_coercion(&f, &rc, column, value.clone(), Ok(value));
    rc.rollback_native().unwrap();
    rr.rollback_native().unwrap();
}

#[test]
fn native_coercion_preserves_kernel_metadata_provenance_error() {
    use crate::storage::{Metadata, encode};
    use relational::Value;

    let f = Fixture::new();
    f.execute("CREATE TABLE inputs (id bigint PRIMARY KEY)");
    let session = f.admin().sql_session();
    let table = session.native_table_engine("inputs").unwrap();
    let column = &table.columns[0];
    let value = Value::Int(1);
    assert_native_coercion(&f, &session, column, value.clone(), Ok(value.clone()));

    let db = f.db();
    let metadata = Metadata {
        format: 4,
        domain: u128::from_be_bytes(db.meta.domain) ^ 1,
        schema: 0,
        last_time: 0,
        next_approval: 0,
        origin: Some(77),
    };
    let mut writer = db.begin(Isolation::ReadCommitted);
    writer.put(Keyspace::Kernel, vec![0], encode(&metadata).unwrap()).unwrap();
    writer.commit().unwrap();

    assert_native_coercion(
        &f, &session, column, value,
        Err(CoreError::Corrupt("kernel metadata domain mismatch".into())),
    );
}

#[test]
fn with_dml_locks_hold_plain_dml_sentinel_until_commit_or_rollback() {
    use crate::relational::Value;
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    for mutation in [
        "DELETE FROM t WHERE n>=0 RETURNING id",
        "UPDATE t SET n=n+1 WHERE n>=0 RETURNING id",
    ] {
        for with in [false, true] {
            for commit in [false, true] {
                let f = Fixture::new();
                f.execute("CREATE TABLE t(id bigint PRIMARY KEY,n bigint)");
                f.execute("INSERT INTO t VALUES(1,0)");
                let mut holder = f.admin().sql_session();
                holder.execute("BEGIN", &[], None).unwrap();
                assert_eq!(
                    holder.v2_transaction.as_ref().unwrap().isolation(),
                    Isolation::ReadCommitted
                );
                let sql = if with {
                    format!("WITH d AS ({mutation}) SELECT * FROM d")
                } else {
                    mutation.to_owned()
                };
                holder.execute(&sql, &[], None).unwrap();

                // A fresh primary key cannot conflict on a row lock. It must
                // wait on the scan writer's sentinel, including for WITH DML.
                let admin = f.admin().clone();
                let (send, done) = mpsc::channel();
                let worker = std::thread::spawn(move || {
                    let result = admin
                        .sql_session()
                        .execute("INSERT INTO t VALUES(2,0)", &[], None);
                    send.send(result).unwrap();
                });
                let until = Instant::now() + Duration::from_secs(10);
                loop {
                    assert!(
                        matches!(done.try_recv(), Err(mpsc::TryRecvError::Empty)),
                        "writer completed before {sql} ended"
                    );
                    if holder.v2_transaction.as_ref().unwrap().sql_waiting_count() > 0 {
                        break;
                    }
                    assert!(Instant::now() < until, "writer never waited for {sql}");
                    std::thread::sleep(Duration::from_millis(1));
                }
                holder
                    .execute(if commit { "COMMIT" } else { "ROLLBACK" }, &[], None)
                    .unwrap();
                assert_eq!(
                    done.recv_timeout(Duration::from_secs(20))
                        .unwrap()
                        .unwrap()
                        .affected,
                    1
                );
                worker.join().unwrap();
                let rows = f
                    .admin()
                    .sql_session()
                    .execute("SELECT id,n FROM t ORDER BY id", &[], None)
                    .unwrap()
                    .rows;
                let mut expected = Vec::new();
                if !commit || mutation.starts_with("UPDATE") {
                    expected.push(vec![Value::Int(1), Value::Int(i64::from(commit))]);
                }
                expected.push(vec![Value::Int(2), Value::Int(0)]);
                assert_eq!(
                    rows.into_iter().map(|r| r.values).collect::<Vec<_>>(),
                    expected
                );
            }
        }
    }
}

#[test]
fn with_dml_locks_authorization_marker_is_not_a_storage_target() {
    for sql in [
        "WITH d AS (DELETE FROM t RETURNING id) SELECT * FROM d",
        "WITH d AS (UPDATE t SET n=n+1 RETURNING id) SELECT * FROM d",
        "WITH q AS (SELECT * FROM t) SELECT * FROM q",
    ] {
        let prepared = relational::Prepared::parse(sql, 1, 0).unwrap();
        assert_eq!(prepared.table(), Some("giggles_join"));
        assert_eq!(prepared.storage_table(), None);
        assert!(!prepared.read_tables().iter().any(|name| name == "giggles_join"));
        assert!(!prepared.write_tables().iter().any(|(name, _)| name == "giggles_join"));
    }
    let plain = relational::Prepared::parse("DELETE FROM t RETURNING id", 1, 0).unwrap();
    assert_eq!(plain.storage_table(), Some("t"));
}

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
#[path = "v2_join_tests.rs"]
mod indexed_join;
#[path = "v2_ordered_range_tests.rs"]
mod ordered_range;
#[path = "v2_q40_prefix_key_tests.rs"]
mod q40_exact_key_boundary;

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
    fn db(&self) -> crate::storage::EngineLease {
        match &self.admin().inner.backend {
            Backend::V2(_) => self.admin().test_v2(),
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
        let entry = engine::catalog::TableEntry { primary_index_birth: None, def: parts.tables[name].clone(),
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
            target: crate::relational::catalog_ext::ReferencedKey::Unknown,
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
        check_post_image(Arc::new(tables), ext, next_id),
        Err(CoreError::Corrupt(message)) if message.starts_with("v2 DDL produced an invalid catalog: ")
    ));
}

#[test]
fn post_image_check_is_valid() {
    let (tables, ext, next_id, _) =
        created_catalog("CREATE TABLE t (id bigint PRIMARY KEY, n bigint CHECK (n > 0))");
    check_post_image(Arc::new(tables), ext, next_id).unwrap();
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
        check_post_image(Arc::new(tables), ext, next_id).unwrap();
    }
}

#[test]
fn post_image_primary_key_is_valid() {
    let (tables, ext, next_id, _) = created_catalog("CREATE TABLE t (id bigint PRIMARY KEY)");
    check_post_image(Arc::new(tables), ext, next_id).unwrap();
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
fn prefixed_point_versions_sql_drop_primary_key() {
    use relational::Value::Int;

    let f = Fixture::new();
    f.execute("CREATE TABLE t (a int, b int, PRIMARY KEY (a,b))");
    f.execute("INSERT INTO t VALUES (-1,7)");
    f.execute("ALTER TABLE t DROP CONSTRAINT t_pkey");

    let mut session = f.admin().sql_session();
    assert_eq!(
        values(&mut session, "SELECT a,b FROM t FOR UPDATE"),
        vec![vec![Int(-1), Int(7)]]
    );
}

#[test]
fn prefixed_point_versions_sql_add_primary_key() {
    use relational::Value::Int;

    let f = Fixture::new();
    f.execute("CREATE TABLE t (a int, b int, PRIMARY KEY (a,b))");
    f.execute("INSERT INTO t VALUES (1,7)");
    f.execute("ALTER TABLE t DROP CONSTRAINT t_pkey");
    f.execute("ALTER TABLE t ADD PRIMARY KEY (a)");

    let mut session = f.admin().sql_session();
    assert_eq!(
        values(&mut session, "SELECT a,b FROM t WHERE a=1 FOR UPDATE"),
        vec![vec![Int(1), Int(7)]]
    );
}

#[test]
fn prefixed_point_versions_sql_rebuilt_index() {
    use relational::Value::Int;

    let f = Fixture::new();
    f.execute("CREATE TABLE t (a int, b int, c int, PRIMARY KEY (a,b))");
    f.execute("CREATE INDEX ix ON t(c)");
    f.execute("INSERT INTO t VALUES (-1,7,10)");
    f.execute("ALTER TABLE t DROP CONSTRAINT t_pkey");

    let mut key = f.parts().indexes[0].id.to_be_bytes().to_vec();
    codec::append_index_tuple(&mut key, [(&Int(10), codec::Direction::Asc)].into_iter()).unwrap();
    let pk = codec::pk_bytes(&[Int(-1)]).unwrap();
    key.extend_from_slice(&pk);
    let db = f.db();
    let mut txn = db.begin(Isolation::ReadCommitted);
    let target = txn
        .lock_for_write(Keyspace::Index, &key, None, &|| false)
        .unwrap();
    assert_eq!(target.current, Some(pk));
}

fn check_prefixed_versions_scans(add_primary_key: bool) {
    use relational::Value::Int;

    let f = Fixture::new();
    f.execute("CREATE TABLE t (a int, b int, PRIMARY KEY (a,b))");
    f.execute("INSERT INTO t VALUES (-1,7),(1,8),(2,9)");
    f.execute("ALTER TABLE t DROP CONSTRAINT t_pkey");
    if add_primary_key {
        f.execute("ALTER TABLE t ADD PRIMARY KEY (a)");
    }
    let expected = vec![vec![Int(-1), Int(7)], vec![Int(1), Int(8)], vec![Int(2), Int(9)]];
    let sql = values(&mut f.admin().sql_session(), "SELECT a,b FROM t ORDER BY a,b");
    let db = f.db();
    let mut txn = db.begin(Isolation::ReadCommitted);
    let scan = txn.scan(Keyspace::Rows, Bound::Unbounded, Bound::Unbounded, Direction::Forward, None)
        .unwrap()
        .map(|item| {
            let (key, value) = item.unwrap();
            let values = codec::decode_row(&key, &value).unwrap().values.into_iter().take(2).collect::<Vec<_>>();
            (key, values)
        })
        .collect::<Vec<_>>();
    assert!(scan.windows(2).all(|pair| pair[0].0 < pair[1].0), "full scan keys must increase strictly: {scan:?}");
    // DROP assigns -1,-2,-3 in the old PK order, so heap-key order is reversed.
    let expected_scan = if add_primary_key { expected.clone() } else { expected.iter().rev().cloned().collect() };
    let scan = scan.into_iter().map(|(_, values)| values).collect::<Vec<_>>();
    assert_eq!((sql, scan), (expected, expected_scan));
}

#[test]
fn prefixed_versions_scans_sql_drop_primary_key() {
    check_prefixed_versions_scans(false);
}

#[test]
fn prefixed_versions_scans_sql_add_primary_key() {
    check_prefixed_versions_scans(true);
}

#[test]
fn prefixed_point_versions_sql_catalog_name() {
    let f = Fixture::new();
    f.execute("CREATE TABLE t (id int PRIMARY KEY)");
    f.execute("CREATE SCHEMA t");
    f.execute("CREATE TABLE t.x (id int PRIMARY KEY)");

    let id = f.parts().tables["t"].id;
    let key = catalog_codec::name_key("t");
    assert_eq!(
        mvcc::latest_version(f.db().kv.as_ref(), Keyspace::Catalog, &key)
            .unwrap()
            .and_then(|(_, value)| value),
        Some(id.to_be_bytes().to_vec())
    );
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
        primary_index_birth: None,
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
            primary_index_birth: None,
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
    b.set_max_value_bytes(relational::MAX_ROW_BYTES);
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
    assert_eq!(
        parity(&mut a, &mut b, "SELECT id FROM (SELECT id FROM a) q", &[])
            .unwrap().rows[0].values,
        vec![relational::Value::Int(1)],
    );
    for (sql, expected) in [
        ("SELECT id FROM a UNION SELECT id FROM b", vec![1, 2]),
        ("SELECT id FROM a UNION ALL SELECT id FROM b", vec![1, 2]),
        ("SELECT id FROM a INTERSECT SELECT id FROM b", vec![]),
        ("SELECT id FROM a EXCEPT SELECT id FROM b", vec![1]),
        ("WITH q AS (SELECT id FROM a) SELECT id FROM q", vec![1]),
    ] {
        for session in [&mut a, &mut b] {
            let result = session.execute(sql, &[], None).unwrap();
            let mut rows: Vec<_> = result.rows.into_iter().map(|r| {
                let [relational::Value::Int(id)] = r.values.as_slice() else { panic!("{sql}: {r:?}") };
                *id
            }).collect();
            rows.sort_unstable();
            assert_eq!(rows, expected, "{sql}");
        }
    }
    for (sql, expected) in [
        ("SELECT a.id,b.id FROM a RIGHT JOIN b ON a.id=b.id",
         vec![vec![relational::Value::Null, relational::Value::Int(2)]]),
        ("SELECT a.id,b.id FROM a FULL OUTER JOIN b ON a.id=b.id ORDER BY a.id NULLS LAST",
         vec![vec![relational::Value::Int(1), relational::Value::Null],
              vec![relational::Value::Null, relational::Value::Int(2)]]),
    ] {
        let result = parity(&mut a, &mut b, sql, &[]).unwrap();
        assert_eq!(result.rows.into_iter().map(|r| r.values).collect::<Vec<_>>(), expected, "{sql}");
    }
    for (sql, expected) in [
        ("SELECT (SELECT id FROM b) FROM a", vec![vec![relational::Value::Int(2)]]),
        ("SELECT id FROM a WHERE id=(SELECT id FROM b)", vec![]),
    ] {
        let result = parity(&mut a, &mut b, sql, &[]).unwrap();
        assert_eq!(result.rows.into_iter().map(|r| r.values).collect::<Vec<_>>(), expected, "{sql}");
    }
    assert_eq!(
        parity(&mut a, &mut b, "INSERT INTO b SELECT id FROM a UNION SELECT id FROM b WHERE false", &[])
            .unwrap().affected,
        1,
    );
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
fn fk_validation_at_fixed_snapshots_crosschecks_witnesses() {
    // GD-0075 R5-76: FK checks at REPEATABLE READ/SERIALIZABLE run at the
    // transaction snapshot and fail 40001 only when a witness changed since it.
    for isolation in [Isolation::RepeatableRead, Isolation::Serializable] {
        let f = Fixture::new();
        f.execute("CREATE TABLE p(id int PRIMARY KEY)");
        f.execute("CREATE TABLE c(id int PRIMARY KEY, p int REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED)");
        f.execute("CREATE TABLE u(id int PRIMARY KEY)");
        f.execute("INSERT INTO p VALUES(1),(2)");
        let mut s = f.admin().sql_session();
        s.execute("BEGIN", &[], None).unwrap();
        s.v2_transaction = Some(f.db().begin(isolation));
        s.execute("INSERT INTO u VALUES(1)", &[], None).unwrap();
        s.execute("INSERT INTO c VALUES(1,1)", &[], None).unwrap();
        let mut pending = s.deferral.clone();
        let mut txn = f.db().begin(isolation);
        validate_pending(&f.db(), &mut txn, &mut pending).unwrap();
        drop(txn);
        s.execute("COMMIT", &[], None).unwrap();
        // A parent deleted after the snapshot. PostgreSQL 18.6 accepts the
        // INSERT (INSERT 0 1) and fails the deferred check at COMMIT with
        // 40001 "could not serialize access due to concurrent update". The
        // deferred FK is no longer probed at the INSERT (GD-0092), so the
        // former GD-0075 R5-77 divergence (40001 at the INSERT) is gone.
        s.execute("BEGIN", &[], None).unwrap();
        s.v2_transaction = Some(f.db().begin(isolation));
        s.execute("SELECT count(*) FROM u", &[], None).unwrap();
        f.execute("DELETE FROM p WHERE id = 2");
        assert_eq!(
            s.execute("INSERT INTO c VALUES(2,2)", &[], None).map(|r| r.affected),
            Ok(1)
        );
        assert_eq!(
            s.execute("COMMIT", &[], None),
            Err(CoreError::Sql(SqlError::Conflict))
        );
        assert!(s.execute("SELECT * FROM c WHERE id = 2", &[], None).unwrap().rows.is_empty());
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

#[test]
fn unique_constraint_origin_survives_v2_reopen_and_owned_index_drop() {
    let mut f = Fixture::new();
    f.execute("CREATE TABLE uq_origin (label text)");
    f.execute("CREATE UNIQUE INDEX plain ON uq_origin(label)");
    f.reopen(); // old record versions, with no origin metadata
    assert!(f.admin().catalog_presentation().unwrap().constraints.is_empty());
    assert_eq!(f.parts().ext.ext_version, 1);
    f.execute("ALTER TABLE uq_origin ADD CONSTRAINT uq UNIQUE(label)");
    let bit = engine::meta::UNIQUE_CONSTRAINT_IDENTITY;
    let bits = u64::from_be_bytes(f.db().kv.get(Keyspace::Meta, b"features").unwrap().unwrap().try_into().unwrap());
    assert_ne!(bits & bit, 0);
    assert!(engine::content_check::check(f.db().kv.as_ref(), bits & !bit).is_err());
    f.reopen();
    assert_eq!(f.parts().ext.ext_version, 7);
    let metadata = f.admin().catalog_presentation().unwrap();
    let unique: Vec<_> = metadata.constraints.iter().filter(|c| c.kind == 'u').collect();
    assert_eq!(unique.len(), 1); assert_eq!(unique[0].name, "uq");
    assert!(f.parts().indexes.iter().any(|d| d.id == unique[0].index && d.unique));
    f.execute("ALTER TABLE uq_origin DROP CONSTRAINT uq");
    f.reopen();
    assert!(f.admin().catalog_presentation().unwrap().constraints.is_empty());
    assert_eq!(f.parts().indexes.len(), 1);
}

#[cfg(feature = "engine-rocksdb")]
#[path = "v2_select_points_storage_tests.rs"]
mod select_points;

#[test]
fn wide_value_budget_and_durable_feature() {
    use relational::Value::{Int, Text};
    let mut f = Fixture::new();
    let bits = |f: &Fixture| engine::meta::open_checks(f.db().kv.snapshot().unwrap().as_ref()).unwrap().features;
    let mut s = f.admin().sql_session();
    assert_eq!(s.max_value_bytes, 67_108_864);
    s.set_max_value_bytes(relational::MAX_VALUE_BYTES);
    assert_eq!(s.max_value_bytes, 1_073_741_819);
    s.set_max_value_bytes(relational::DEFAULT_MAX_VALUE_BYTES);
    s.execute("CREATE TABLE wide_values(id bigint PRIMARY KEY, v text)", &[], None).unwrap();
    s.execute("INSERT INTO wide_values VALUES(1,'small')", &[], None).unwrap();
    assert_eq!(bits(&f) & engine::meta::WIDE_ROWS, 0);
    s.execute("BEGIN", &[], None).unwrap();
    s.execute("UPDATE wide_values SET v=repeat('x',204800) WHERE id=1", &[], None).unwrap();
    s.execute("ROLLBACK", &[], None).unwrap();
    assert_eq!(bits(&f) & engine::meta::WIDE_ROWS, 0);
    for sql in ["BEGIN", "SAVEPOINT wide"] { s.execute(sql, &[], None).unwrap(); }
    s.execute("UPDATE wide_values SET v=repeat('x',204800) WHERE id=1", &[], None).unwrap();
    for sql in ["ROLLBACK TO wide", "COMMIT"] { s.execute(sql, &[], None).unwrap(); }
    assert_eq!(bits(&f) & engine::meta::WIDE_ROWS, 0);
    for size in [204800, 1048577] {
        let inserted = s.execute("INSERT INTO wide_values VALUES($1,$2) RETURNING v", &[Int(size as i64), Text("x".repeat(size))], None).unwrap_or_else(|e| panic!("insert size {size}: {e:?}"));
        assert_eq!(inserted.rows[0].values, vec![Text("x".repeat(size))]);
        assert_ne!(bits(&f) & engine::meta::WIDE_ROWS, 0);
        let sql = format!("UPDATE wide_values SET v=repeat('x',{size}) WHERE id=1 RETURNING v");
        assert_eq!(s.execute(&sql, &[], None).unwrap().rows[0].values, vec![Text("x".repeat(size))]);
        assert_eq!(s.execute("SELECT v FROM wide_values WHERE id=1", &[], None).unwrap().rows[0].values, vec![Text("x".repeat(size))]);
    }
    assert_ne!(bits(&f) & engine::meta::WIDE_ROWS, 0);
    assert_eq!(s.execute("SELECT length(repeat('x',100000) || repeat('y',100000))", &[], None).unwrap().rows[0].values, vec![Int(200000)]);
    for sql in ["SELECT repeat('x',1073741823)", "SELECT lpad('x',1073741824)"] {
        assert_eq!(s.execute(sql, &[], None), Err(CoreError::Sql(SqlError::Limit)));
        assert_eq!(relational::take_sql_error_message().as_deref(), Some("requested length too large"));
    }
    for literal in [format!("'{}'", "l".repeat(204800)), format!("$${}$$", "l".repeat(204800))] {
        assert_eq!(s.execute(&format!("SELECT length({literal})"), &[], None).unwrap().rows[0].values, vec![Int(204800)]);
    }
    let described = s.prepare("SELECT repeat('x',1048577)").unwrap();
    s.describe(&described).unwrap();
    s.validate_immutable_constants(&described).unwrap();
    s.set_max_value_bytes(65536);
    assert_eq!(s.execute("SELECT repeat('x',70000)", &[], None), Err(CoreError::Sql(SqlError::Limit)));
    drop(s);
    f.reopen();
    assert_eq!(f.admin().sql_session().execute("SELECT length(v) FROM wide_values WHERE id=1", &[], None).unwrap().rows[0].values, vec![Int(1048577)]);
}

#[test]
fn statement_token_budget() {
    use relational::Value::{Int, Bool, Null};
    let f = Fixture::new();
    let mut s = f.admin().sql_session();
    assert_eq!(s.max_statement_tokens, 1_000_000);
    s.execute("CREATE TABLE token_budget(id int)", &[], None).unwrap();
    s.execute("INSERT INTO token_budget VALUES(1),(20000),(100001)", &[], None).unwrap();
    for count in [20000, 100000] {
        let list = (1..=count).map(|n| n.to_string()).collect::<Vec<_>>().join(",");
        let sql = format!("SELECT id FROM token_budget WHERE id IN ({list}) ORDER BY id");
        let start = std::time::Instant::now();
        let result = s.execute(&sql, &[], None).unwrap();
        eprintln!("{count} IN: {:?}", start.elapsed());
        assert_eq!(result.rows.iter().map(|r| r.values.clone()).collect::<Vec<_>>(), vec![vec![Int(1)],vec![Int(20000)]]);
        if count == 20000 {
            s.set_max_statement_tokens(4096);
            assert_eq!(s.execute(&sql, &[], None), Err(CoreError::Sql(SqlError::Limit)));
            s.set_max_statement_tokens(relational::DEFAULT_MAX_STATEMENT_TOKENS);
        }
    }
    let r = s.execute("SELECT 1 IN (1,NULL), 2 IN (1,NULL), 2 NOT IN (1,NULL), 1 NOT IN (1,NULL), NULL IN (1,2), 2 NOT IN (1,3)", &[], None).unwrap();
    assert_eq!(r.rows[0].values, vec![Bool(true), Null, Null, Bool(false), Null, Bool(true)]);
    let rows = (1..=20000).map(|n| format!("({n})")).collect::<Vec<_>>().join(",");
    let start = std::time::Instant::now();
    let r = s.execute(&format!("SELECT count(*) FROM (VALUES {rows}) AS v(x)"), &[], None).unwrap();
    eprintln!("20000 VALUES: {:?}", start.elapsed());
    assert_eq!(r.rows[0].values, vec![Int(20000)]);
}


#[test]
fn uuid_array_feature_bit_tracks_persisted_types() {
    let mut f = Fixture::new();
    let bit = engine::meta::UUID_ARRAY_COLUMNS;
    let features = |f: &Fixture| engine::meta::validate_meta(f.db().kv.as_ref()).unwrap().features;

    f.execute("CREATE TABLE t (id bigint PRIMARY KEY, v text[])");
    f.execute("INSERT INTO t VALUES (1,'{old}')");
    let bytes = f.catalog_bytes();
    let old_bits = features(&f);
    assert_eq!(old_bits & bit, 0);
    f.execute("SELECT ARRAY[]::uuid[]");
    f.reopen();
    assert_eq!(f.catalog_bytes(), bytes);
    assert_eq!(features(&f), old_bits);

    f.execute("CREATE TABLE copy AS SELECT id, ARRAY[]::uuid[] AS ids FROM t");
    assert_ne!(features(&f) & bit, 0);
    assert!(engine::content_check::check(f.db().kv.as_ref(), features(&f) & !bit).is_err());
    f.execute("CREATE VIEW copied_ids AS SELECT ids FROM copy");
    f.reopen();
    assert_eq!(
        values(&mut f.admin().sql_session(), "SELECT ids FROM copied_ids"),
        vec![vec![relational::Value::Text("{}".into())]],
    );
    f.execute("DROP VIEW copied_ids");
    f.execute("DROP TABLE copy");
    f.reopen();
    assert_ne!(features(&f) & bit, 0);
}

#[test]
fn text_search_v2_feature_bit_and_ordinary_catalog_bytes() {
    let mut f = Fixture::new();
    let bit = engine::meta::TEXT_SEARCH_TYPES;
    let features = |f: &Fixture| engine::meta::validate_meta(f.db().kv.as_ref()).unwrap().features;
    f.execute("CREATE TABLE t (id bigint PRIMARY KEY, v text[])");
    f.execute("INSERT INTO t VALUES (1,'{old}')");
    let bytes = f.catalog_bytes();
    let old_bits = features(&f);
    assert_eq!(old_bits & bit, 0);
    f.execute("SELECT to_tsvector('a'), to_tsquery('a')");
    f.reopen();
    assert_eq!(f.catalog_bytes(), bytes);
    assert_eq!(features(&f), old_bits);

    f.execute("ALTER TABLE t ADD COLUMN q tsquery");
    f.execute("UPDATE t SET q = ''");
    assert_ne!(features(&f) & bit, 0);
    assert!(engine::content_check::check(f.db().kv.as_ref(), features(&f) & !bit).is_err());
    f.reopen();
    assert_eq!(
        values(&mut f.admin().sql_session(), "SELECT q::text FROM t"),
        vec![vec![relational::Value::Text(String::new())]],
    );
}

#[test]
fn catalog_scale_feature_is_committed_with_the_first_wide_catalog() {
    let f = Fixture::new();
    let db = f.db();
    let wide = || {
        let bits = engine::meta::validate_meta(db.kv.snapshot().unwrap().as_ref()).unwrap().features;
        bits & engine::meta::WIDE_CATALOG != 0
    };
    f.execute("CREATE TABLE wide_catalog_table(id integer PRIMARY KEY, k integer)");
    for i in 1..=8 {
        f.execute(&format!("CREATE INDEX wc_{i} ON wide_catalog_table(k)"));
    }
    assert!(!wide());

    let mut session = f.admin().sql_session();
    for sql in [
        "BEGIN",
        "CREATE INDEX wc_9 ON wide_catalog_table(k)",
        "ROLLBACK",
    ] {
        session.execute(sql, &[], None).unwrap();
    }
    assert!(!wide());

    f.execute("CREATE INDEX wc_9 ON wide_catalog_table(k)");
    assert!(wide());
    f.execute("DROP INDEX wc_9");
    assert!(wide());
}

#[test]
fn uuid_array_feature_uses_final_committed_overlay() {
    let f = Fixture::new();
    let mut session = f.admin().sql_session();
    let features = || engine::meta::validate_meta(f.db().kv.as_ref()).unwrap().features;
    let bit = engine::meta::UUID_ARRAY_COLUMNS;
    for transaction in [
        &["BEGIN", "CREATE TABLE dropped (id int PRIMARY KEY, a uuid[])", "DROP TABLE dropped", "COMMIT"][..],
        &["BEGIN", "SAVEPOINT before_array", "CREATE TABLE restored (id int PRIMARY KEY, a uuid[])",
            "ROLLBACK TO SAVEPOINT before_array", "CREATE TABLE kept (id int PRIMARY KEY)", "COMMIT"],
    ] {
        for sql in transaction {
            session.execute(sql, &[], None).unwrap();
        }
        assert_eq!(features() & bit, 0);
    }
    f.execute("INSERT INTO kept VALUES (1)");
    f.execute("ALTER TABLE kept ADD COLUMN a uuid[]");
    assert_ne!(features() & bit, 0);
}

#[test]
fn uuid_array_and_catalog_scale_bits_open_without_catalog_witnesses() {
    let mut f = Fixture::new();
    let features = |f: &Fixture| engine::meta::validate_meta(f.db().kv.as_ref()).unwrap().features;
    f.execute("CREATE TABLE ordinary (id int PRIMARY KEY)");
    let bits = engine::meta::UUID_ARRAY_COLUMNS | engine::meta::WIDE_CATALOG;
    let old = features(&f);
    assert_eq!(old & bits, 0);
    let mut batch = WriteBatch::default();
    engine::meta::put_features(&mut batch, old | bits).unwrap();
    f.db().kv.write(batch, Durability::Sync).unwrap();
    f.reopen();
    f.execute("INSERT INTO ordinary VALUES (1)");
    assert_eq!(
        values(&mut f.admin().sql_session(), "SELECT id FROM ordinary"),
        vec![vec![relational::Value::Int(1)]],
    );
    assert_eq!(features(&f), old | bits);
}

#[test]
fn catalog_scale_main_shaped_sequences_preserve_feature_word() {
    let mut f = Fixture::new();
    let features = |f: &Fixture| engine::meta::validate_meta(f.db().kv.as_ref()).unwrap().features;
    f.execute("CREATE TABLE ordinary (id int PRIMARY KEY)");
    for i in 0..129 {
        f.execute(&format!("CREATE SEQUENCE s{i}"));
    }
    let old = features(&f);
    assert_eq!(old & engine::meta::WIDE_CATALOG, 0);
    f.reopen();
    f.execute("INSERT INTO ordinary VALUES (1)");
    assert_eq!(features(&f), old);
}

#[test]
fn ddl_rejects_duplicate_index_and_missing_fk_column() {
    let f = Fixture::new();
    f.execute("CREATE TABLE parent(id int PRIMARY KEY)");
    f.execute("CREATE TABLE child(id int PRIMARY KEY, parent_id int)");
    f.execute("CREATE INDEX child_parent_idx ON child(parent_id)");
    let mut session = f.admin().sql_session();
    let before = f.parts();
    assert_eq!(before.indexes.len(), 1);
    session
        .execute("CREATE INDEX child_parent_idx ON child(id)", &[], None)
        .unwrap_err();
    f.db().catalog().assert_current_matches_reload();
    assert_eq!(f.parts(), before);

    let statement = "ALTER TABLE child ADD CONSTRAINT child_parent_fk FOREIGN KEY(parent_id) REFERENCES parent(missing)";
    let error = session.execute(statement, &[], None).unwrap_err();
    assert_eq!(crate::sqlstate(&error), "42703", "{statement}: {error:?}");
    f.db().catalog().assert_current_matches_reload();
    assert_eq!(f.parts(), before);
}

#[test]
fn ddl_rejects_a_post_image_with_a_dangling_column_collation() {
    let f = Fixture::new();
    f.execute("CREATE SCHEMA collation_schema");
    f.execute("CREATE COLLATION collation_schema.local_c FROM \"C\"");
    f.execute("CREATE TABLE t(id int PRIMARY KEY, label text COLLATE collation_schema.local_c)");
    let before = f.parts();
    let error = f
        .admin()
        .sql_session()
        .execute("DROP SCHEMA collation_schema CASCADE", &[], None)
        .unwrap_err();
    assert!(matches!(
        error,
        CoreError::Corrupt(ref message) if message.starts_with("v2 DDL produced an invalid catalog: ")
    ), "{error:?}");
    f.db().catalog().assert_current_matches_reload();
    assert_eq!(f.parts(), before);
}

#[test]
fn gin_indexes_catalog_publication_matches_durable_reload() {
    let mut f = Fixture::new();
    let features = |f: &Fixture| engine::meta::validate_meta(f.db().kv.as_ref()).unwrap().features;
    f.execute("CREATE TABLE f2_gin (id int PRIMARY KEY, tags text[])");
    assert_eq!(features(&f) & engine::meta::GIN_INDEXES, 0);
    f.execute("CREATE INDEX f2_gin_idx ON f2_gin USING gin (tags)");
    f.db().catalog().assert_current_matches_reload();
    assert_ne!(features(&f) & engine::meta::GIN_INDEXES, 0);

    f.reopen();
    assert_eq!(f.parts().ext.gin_indexes.len(), 1);
    assert_ne!(features(&f) & engine::meta::GIN_INDEXES, 0);
    f.execute("CREATE TABLE f2_gin_seed (id int PRIMARY KEY)");
    f.execute("DROP INDEX f2_gin_idx");
    f.db().catalog().assert_current_matches_reload();

    f.reopen();
    assert!(f.parts().ext.gin_indexes.is_empty());
    assert_ne!(features(&f) & engine::meta::GIN_INDEXES, 0);
}

#[test]
fn generated_columns_catalog_publication_matches_durable_reload() {
    let mut f = Fixture::new();
    let features = |f: &Fixture| engine::meta::validate_meta(f.db().kv.as_ref()).unwrap().features;
    f.execute("CREATE TABLE f2_generation (id int PRIMARY KEY, n int, s int GENERATED ALWAYS AS (n * 2) STORED)");
    assert!(f.parts().ext.columns.is_empty());
    assert_ne!(features(&f) & engine::meta::GENERATED_COLUMNS, 0);

    f.execute("INSERT INTO f2_generation VALUES (1, 5)");
    f.execute("ALTER TABLE f2_generation ALTER COLUMN s SET EXPRESSION AS (n * 3)");
    f.db().catalog().assert_current_matches_reload();

    f.reopen();
    assert_ne!(features(&f) & engine::meta::GENERATED_COLUMNS, 0);
    f.execute("INSERT INTO f2_generation VALUES (2, 7)");
    assert_eq!(
        values(&mut f.admin().sql_session(), "SELECT s FROM f2_generation ORDER BY id"),
        vec![vec![relational::Value::Int(15)], vec![relational::Value::Int(21)]],
    );
    f.execute("CREATE TABLE f2_generation_seed (id int PRIMARY KEY)");
    f.execute("ALTER TABLE f2_generation ALTER COLUMN s DROP EXPRESSION");
    f.db().catalog().assert_current_matches_reload();

    f.reopen();
    assert!(f.parts().ext.generated_columns.is_empty());
    assert_ne!(features(&f) & engine::meta::GENERATED_COLUMNS, 0);
    f.execute("INSERT INTO f2_generation VALUES (3, 8, 100)");
    assert_eq!(
        values(&mut f.admin().sql_session(), "SELECT s FROM f2_generation WHERE id = 3"),
        vec![vec![relational::Value::Int(100)]],
    );
}

#[test]
fn mixed_sql_ddl_catalog_publication_matches_durable_reload() {
    let f = Fixture::new();
    let mut session = f.admin().sql_session();
    let run = |session: &mut SqlSession, sql| {
        session.execute(sql, &[], None).unwrap_or_else(|e| panic!("{sql}: {e:?}"));
        f.db().catalog().assert_current_matches_reload();
    };
    for sql in [
        "CREATE SCHEMA ddl_test",
        "CREATE TABLE ddl_test.parent(id serial PRIMARY KEY)",
        "CREATE TABLE ddl_test.child(id int PRIMARY KEY, parent_id int)",
        "COMMENT ON TABLE ddl_test.child IS 'child table'",
        "COMMENT ON COLUMN ddl_test.child.parent_id IS 'parent reference'",
        "CREATE INDEX parent_lookup ON ddl_test.child(parent_id)",
        "ALTER TABLE ddl_test.child ADD CONSTRAINT child_parent_fk FOREIGN KEY(parent_id) REFERENCES ddl_test.parent(id)",
        "ALTER TABLE ddl_test.child ADD COLUMN extra int",
        "ALTER TABLE ddl_test.child RENAME COLUMN extra TO renamed",
        "BEGIN",
        "ALTER TABLE ddl_test.child DROP CONSTRAINT child_parent_fk",
        "DROP INDEX ddl_test.parent_lookup",
        "ALTER TABLE ddl_test.child DROP COLUMN parent_id",
        "COMMIT",
        "ALTER TABLE ddl_test.child RENAME TO renamed_child",
        "BEGIN",
        "DROP TABLE ddl_test.renamed_child",
        "CREATE TABLE ddl_test.renamed_child(id int PRIMARY KEY)",
        "COMMIT",
    ] {
        run(&mut session, sql);
    }

    let before = f.parts();
    run(&mut session, "BEGIN");
    run(&mut session, "ALTER TABLE ddl_test.renamed_child ADD COLUMN rolled_back int");
    session
        .execute("ALTER TABLE ddl_test.renamed_child DROP COLUMN missing", &[], None)
        .unwrap_err();
    f.db().catalog().assert_current_matches_reload();
    run(&mut session, "ROLLBACK");
    assert_eq!(f.parts(), before);

    for sql in [
        "BEGIN",
        "SAVEPOINT before_ddl",
        "ALTER TABLE ddl_test.renamed_child ADD COLUMN savepoint_column int",
        "ROLLBACK TO SAVEPOINT before_ddl",
        "COMMIT",
    ] {
        run(&mut session, sql);
    }
    assert_eq!(f.parts(), before);
    for sql in [
        "DROP TABLE ddl_test.renamed_child",
        "DROP TABLE ddl_test.parent",
        "DROP SCHEMA ddl_test",
    ] {
        run(&mut session, sql);
    }
}

#[test]
fn waiting_sql_ddl_catalog_publication_matches_durable_reload() {
    use std::time::{Duration, Instant};

    let f = Fixture::new();
    f.execute("CREATE TABLE a(id int PRIMARY KEY)");
    f.db().catalog().assert_current_matches_reload();
    f.execute("CREATE TABLE b(id int PRIMARY KEY)");
    f.db().catalog().assert_current_matches_reload();
    std::thread::scope(|scope| {
        let mut first = f.admin().sql_session();
        for sql in ["BEGIN", "ALTER TABLE a ADD COLUMN first_change int"] {
            first.execute(sql, &[], None).unwrap();
            f.db().catalog().assert_current_matches_reload();
        }

        // Even on another table, the second DDL waits for the catalog barrier.
        let other = scope.spawn(|| {
            f.execute("ALTER TABLE b ADD COLUMN second_change int");
            f.db().catalog().assert_current_matches_reload();
        });
        let until = Instant::now() + Duration::from_secs(10);
        while first.v2_transaction.as_ref().unwrap().sql_waiting_count() == 0 {
            assert!(!other.is_finished(), "second DDL did not wait");
            assert!(Instant::now() < until, "second DDL never reached the barrier");
            std::thread::sleep(Duration::from_millis(1));
        }
        f.db().catalog().assert_current_matches_reload();
        first.execute("COMMIT", &[], None).unwrap();
        f.db().catalog().assert_current_matches_reload();
        other.join().unwrap();
    });

    let parts = f.parts();
    assert_eq!(parts.tables["a"].columns[1].name, "first_change");
    assert_eq!(parts.tables["b"].columns[1].name, "second_change");
}

#[test]
fn key_types_v2_feature_tracks_live_keys() {
    let mut f = Fixture::new();
    let bit = engine::meta::KEY_TYPES;
    let features = |f: &Fixture| engine::meta::validate_meta(f.db().kv.as_ref()).unwrap().features;
    f.execute("CREATE TABLE ordinary (id int PRIMARY KEY, keep text)");
    f.execute("INSERT INTO ordinary VALUES (1, 'unindexed ')");
    let key = engine::catalog_codec::table_key(f.parts().tables["ordinary"].id);
    let ordinary = |f: &Fixture| f.catalog_bytes().into_iter()
        .filter(|(physical, _)| physical.starts_with(&key))
        .collect::<Vec<_>>();
    let bytes = ordinary(&f);
    assert_eq!(bytes.len(), 1);
    assert_eq!(features(&f) & bit, 0);

    let mut s = f.admin().sql_session();
    for sql in ["BEGIN", "CREATE TABLE scratch (k char(3) PRIMARY KEY)", "DROP TABLE scratch", "COMMIT"] {
        s.execute(sql, &[], None).unwrap();
    }
    drop(s);
    assert_eq!(features(&f) & bit, 0);

    f.execute("CREATE TABLE calendar (day date PRIMARY KEY)");
    f.execute("INSERT INTO calendar VALUES ('2024-01-02')");
    let bits = features(&f);
    assert_ne!(bits & bit, 0);
    let old_mask = engine::meta::FEATURE_TABLE[..39].iter().fold(0, |mask, (bit, _)| mask | bit);
    assert!(engine::meta::check_features_allowed(bits, old_mask).is_err());
    assert!(engine::content_check::check(f.db().kv.as_ref(), features(&f) & !bit).is_err());
    f.execute("CREATE TABLE codes (id int PRIMARY KEY, code bpchar UNIQUE)");
    f.execute("DROP TABLE calendar");
    assert_ne!(features(&f) & bit, 0);
    assert!(engine::content_check::check(f.db().kv.as_ref(), features(&f) & !bit).is_err());
    f.reopen();
    f.execute("DROP TABLE codes");
    assert_eq!(features(&f) & bit, 0);
    f.reopen();
    assert_eq!(features(&f) & bit, 0);
    assert_eq!(ordinary(&f), bytes);
}
