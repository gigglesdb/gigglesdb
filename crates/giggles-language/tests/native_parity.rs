//! Legacy differential skeleton, not R2/v2 qualification. Only the keyed catalog
//! subset below is observed; row versions and the wider catalog remain gaps.
use giggles_core::{
    Admin, CoreError, Engine as StorageEngine, Limits,
    relational::{DataType, SqlError, with_sql_parser_forbidden},
};
use giggles_language::{Bindings, Engine, Error, LanguageVersion, SqlRow, Value};
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
struct Database(PathBuf);
impl Database {
    fn fresh(engine: StorageEngine) -> (Self, Admin) {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "native-parity-{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
        (Self(path), admin)
    }
}
impl Drop for Database {
    fn drop(&mut self) {
        if self.0.is_dir() {
            std::fs::remove_dir_all(&self.0).unwrap();
        } else {
            std::fs::remove_file(&self.0).unwrap();
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
struct Table {
    name: String,
    columns: Vec<(String, DataType, bool)>, // bool = nullable
    primary_key: Vec<String>,
    rows: Vec<Vec<Value>>,
    indexes: Vec<Index>,
}
// Logical references retain table and column names; internal IDs never enter equality.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Index {
    name: String,
    table: String,
    keys: Vec<String>,
    unique: bool,
}
fn values(rows: Vec<SqlRow>) -> Vec<Vec<Value>> {
    rows.into_iter().map(|row| row.values).collect()
}
fn snapshot(admin: &Admin) -> Vec<Table> {
    let metadata = admin.sql_session().catalog_metadata().unwrap();
    let mut tables = metadata.tables;
    let indexes = metadata.indexes;
    tables.sort_by(|a, b| a.name.cmp(&b.name));
    tables
        .into_iter()
        .map(|table| {
            let primary_key: Vec<_> = table
                .primary_key
                .iter()
                .map(|&i| table.columns[i].name.clone())
                .collect();
            // Fixture identifiers are fixed canonical names, never user input.
            let query = format!(
                "select * from {} order by {} limit 1024",
                table.name,
                primary_key.join(",")
            );
            let rows = admin.sql_session().execute(&query, &[], None).unwrap().rows;
            let mut logical_indexes: Vec<_> = indexes
                .iter()
                .filter(|index| index.table == table.id)
                .map(|index| Index {
                    name: index.name.clone(),
                    table: table.name.clone(),
                    // IndexInfo exposes ordered column IDs, but no direction.
                    // Resolve against this table only; a dangling/wrong-table ID fails.
                    keys: index
                        .columns
                        .iter()
                        .map(|id| {
                            table
                                .columns
                                .iter()
                                .find(|column| column.id == *id)
                                .expect("index key must belong to its referenced table")
                                .name
                                .clone()
                        })
                        .collect(),
                    unique: index.unique,
                })
                .collect();
            logical_indexes.sort_by(|a, b| a.name.cmp(&b.name));
            Table {
                indexes: logical_indexes,
                name: table.name,
                columns: table
                    .columns
                    .into_iter()
                    .map(|c| (c.name, c.data_type, !c.not_null))
                    .collect(),
                primary_key,
                rows: values(rows),
            }
        })
        .collect()
}
#[derive(Clone, Debug, PartialEq, Eq)]
enum Step {
    Success(Vec<Vec<Value>>),
    Sql(SqlError),
    Native(Error),
}
fn sql(admin: &Admin, text: &str) -> Step {
    match admin.sql_session().execute(text, &[], None) {
        Ok(result) => Step::Success(values(result.rows)),
        Err(CoreError::Sql(kind)) => Step::Sql(kind),
        Err(error) => panic!("unexpected SQL infrastructure error: {error:?}"),
    }
}
fn native(admin: &Admin, text: &str, catalog: bool) -> Step {
    with_sql_parser_forbidden(|| {
        let engine = Engine::with_version(admin.clone(), LanguageVersion::Embedded07);
        let result = if catalog {
            engine.execute_catalog_text(text).map(|_| vec![])
        } else {
            engine
                .execute_text(text, &Bindings::new())
                .map(|out| values(out.rows))
        };
        match result {
            Ok(rows) => Step::Success(rows),
            Err(error) => Step::Native(error),
        }
    })
}
#[derive(Clone, Debug, PartialEq, Eq)]
struct Observation {
    state: Vec<Table>,
    step: Step,
}
fn parity(sql: &Observation, native: &Observation) -> bool {
    sql == native
        || (sql.state == native.state
            && matches!(
                (&sql.step, &native.step),
                (
                    Step::Sql(SqlError::Duplicate),
                    Step::Native(Error::Duplicate)
                )
            )
            && Error::Duplicate.sqlstate() == Some("23505"))
}
#[test]
fn legacy_create_insert_update_select_and_duplicate_parity() {
    let (sql_db, sql_admin) = Database::fresh(StorageEngine::Legacy);
    let (native_db, native_admin) = Database::fresh(StorageEngine::Legacy);
    let sql_texts = [
        "create table items(id bigint primary key, label text)",
        "insert into items values(2,null)",
        "insert into items values(1,'ready')",
        "update items set label='done' where id=1",
        "select * from items order by id limit 1024",
        "insert into items values(1,'duplicate')",
    ];
    let native_texts = [
        "create table items(id int64,label nullable text,primary key(id))",
        "insert items set id=2,label=null",
        "insert items set id=1,label=\"ready\"",
        "update items where id=1 set label=\"done\"",
        "items order by id limit 1024",
        "insert items set id=1,label=\"duplicate\"",
    ];
    let expected_rows = vec![
        vec![Value::Int(1), Value::Text("done".into())],
        vec![Value::Int(2), Value::Null],
    ];
    for (i, (sql_text, native_text)) in sql_texts.iter().zip(native_texts).enumerate() {
        let left = Observation {
            step: sql(&sql_admin, sql_text),
            state: snapshot(&sql_admin),
        };
        let right = Observation {
            step: native(&native_admin, native_text, i == 0),
            state: snapshot(&native_admin),
        };
        assert_eq!(left.state, right.state, "step {i}");
        assert_eq!(left.state[0].rows.len(), [0, 1, 2, 2, 2, 2][i]);
        if i == 5 {
            assert_eq!(left.step, Step::Sql(SqlError::Duplicate));
            assert_eq!(right.step, Step::Native(Error::Duplicate));
            assert!(parity(&left, &right), "step {i}: SQLSTATE parity");
            assert_eq!(Error::Duplicate.sqlstate(), Some("23505"));
            assert_eq!(left.state[0].rows, expected_rows, "failed insert is atomic");
        } else {
            assert!(parity(&left, &right), "step {i}: {left:?} != {right:?}");
            let expected = if i == 4 {
                expected_rows.clone()
            } else {
                vec![]
            };
            assert_eq!(left.step, Step::Success(expected));
        }
    }
    let before = snapshot(&sql_admin);
    drop(sql_admin);
    drop(native_admin);
    let reopened_sql = Admin::open(&sql_db.0, Limits::default()).unwrap();
    let reopened_native = Admin::open(&native_db.0, Limits::default()).unwrap();
    assert_eq!(snapshot(&reopened_sql), before);
    assert_eq!(snapshot(&reopened_native), before);
    assert_eq!(
        before,
        vec![Table {
            name: "items".into(),
            columns: vec![
                ("id".into(), DataType::BigInt, false),
                ("label".into(), DataType::Text, true)
            ],
            primary_key: vec!["id".into()],
            rows: expected_rows,
            indexes: vec![],
        }]
    );
}
#[test]
fn comparator_rejects_perturbed_rows_and_errors() {
    let baseline = Observation {
        state: vec![Table {
            name: "items".into(),
            columns: vec![],
            primary_key: vec![],
            rows: vec![vec![Value::Int(1)]],
            indexes: vec![],
        }],
        step: Step::Sql(SqlError::Duplicate),
    };
    assert!(parity(&baseline, &baseline));
    let mut changed = baseline.clone();
    changed.state[0].rows[0][0] = Value::Int(2);
    assert!(!parity(&baseline, &changed));
    changed = baseline.clone();
    changed.step = Step::Sql(SqlError::Null);
    assert!(!parity(&baseline, &changed));
}
#[test]
fn unsupported_native_construct_cannot_pass() {
    let (_db, admin) = Database::fresh(StorageEngine::Legacy);
    let text = "create table items(id numeric,primary key(id))";
    let step = native(&admin, text, true);
    assert!(matches!(step, Step::Native(Error::Unsupported)));
    assert!(!parity(
        &Observation {
            state: vec![],
            step: Step::Success(vec![])
        },
        &Observation {
            state: snapshot(&admin),
            step
        },
    ));
}
#[cfg(feature = "engine-rocksdb")]
#[test]
fn v2_native_catalog_matches_sql_on_populated_database() {
    let (left_db, left) = Database::fresh(StorageEngine::V2);
    let (right_db, right) = Database::fresh(StorageEngine::V2);
    let engine = Engine::with_version(right.clone(), LanguageVersion::Embedded016);
    for (statement, text) in [
        ("create table items(id bigint primary key, a bigint not null)",
         "create table items(id int64,a int64,primary key(id))"),
        ("insert into items values(1,10)", "insert items set id=1,a=10"),
        ("create index items_a on items(a)", "create index items_a on items(a)"),
        // SQL v2 permits CREATE TABLE after business rows already exist.
        ("create table second(id bigint primary key)", "create table second(id int64,primary key(id))"),
    ] {
        let expected = left.sql_session().execute(statement, &[], None).unwrap();
        with_sql_parser_forbidden(|| {
            if text.starts_with("create") {
                engine.execute_catalog_text(text).unwrap();
                assert_eq!(expected.affected, 0);
            } else {
                let actual = engine.execute_text(text, &Bindings::new()).unwrap();
                assert_eq!(actual.affected as u64, expected.affected);
            }
        });
        assert_eq!(snapshot(&left), snapshot(&right));
    }
    assert_eq!(native(&right, "items order by id limit 1024", false),
        sql(&left, "select * from items order by id"));
    // Both SQL reads use the newly backfilled secondary index, including after reopen.
    let indexed_read = "select * from items where a=10";
    let expected = Step::Success(vec![vec![Value::Int(1), Value::Int(10)]]);
    for admin in [&left, &right] {
        assert_eq!(sql(admin, indexed_read), expected);
    }
    assert_eq!(native(&right, "items where a=10 order by id limit 1024", false), expected);
    for (statement, text, kind, code) in [
        ("create table items(id bigint primary key)", "create table items(id int64,primary key(id))", SqlError::DuplicateTable, "42P07"),
        ("create index items_a on items(a)", "create index items_a on items(a)", SqlError::Duplicate, "23505"),
        ("create index missing on items(absent)", "create index missing on items(absent)", SqlError::UnknownColumn, "42703"),
    ] {
        assert_eq!(sql(&left, statement), Step::Sql(kind.clone()));
        let actual = with_sql_parser_forbidden(|| engine.execute_catalog_text(text)).unwrap_err();
        if kind == SqlError::Duplicate {
            assert_eq!(actual, Error::Duplicate);
        } else {
            assert_eq!(actual, Error::Sql { sqlstate: code.into() });
        }
        assert_eq!(actual.sqlstate(), Some(code));
        assert_eq!(snapshot(&left), snapshot(&right));
    }
    let before = snapshot(&left);
    drop(engine); drop(left); drop(right);
    let left = Admin::open(&left_db.0, Limits::default()).unwrap();
    let right = Admin::open(&right_db.0, Limits::default()).unwrap();
    assert_eq!(snapshot(&left), before);
    assert_eq!(snapshot(&right), before);
    for admin in [&left, &right] {
        assert_eq!(sql(admin, indexed_read), expected);
    }
    assert_eq!(native(&right, "items where a=10 order by id limit 1024", false), expected);
}
#[test]
fn parser_trap_positive_control_covers_text_and_prepared_entry() {
    let (_db, admin) = Database::fresh(StorageEngine::Legacy);
    let mut session = admin.sql_session();
    let prepared = session.prepare("select 1").unwrap();
    session.execute("begin", &[], None).unwrap();
    let trapped = |work: &mut dyn FnMut()| {
        assert!(catch_unwind(AssertUnwindSafe(|| with_sql_parser_forbidden(work))).is_err());
    };
    trapped(&mut || {
        let _ = session.execute("select 1", &[], None);
    });
    trapped(&mut || {
        let _ = session.execute_prepared(&prepared, &[], None);
    });
    session.rollback();
    assert_eq!(
        sql(&admin, "select 1"),
        Step::Success(vec![vec![Value::Int(1)]])
    );
}

fn provision_index(admin: &Admin, native_side: bool, name: &str, keys: &[&str], unique: bool) {
    if native_side && !unique {
        with_sql_parser_forbidden(|| {
            Engine::with_version(admin.clone(), LanguageVersion::Embedded014)
                .execute_catalog_text(&format!("create index {name} on items({})", keys.join(",")))
                .unwrap();
        });
    } else {
        assert_eq!(
            sql(
                admin,
                &format!(
                    "create {}index {name} on items({})",
                    if unique { "unique " } else { "" },
                    keys.join(",")
                )
            ),
            Step::Success(vec![])
        );
    }
}
fn indexed_pair() -> (Database, Admin, Database, Admin) {
    let (left_db, left) = Database::fresh(StorageEngine::Legacy);
    let (right_db, right) = Database::fresh(StorageEngine::Legacy);
    assert_eq!(
        sql(
            &left,
            "create table items(id bigint primary key, a bigint not null, b bigint not null)"
        ),
        Step::Success(vec![])
    );
    assert_eq!(
        native(
            &right,
            "create table items(id int64,a int64,b int64,primary key(id))",
            true
        ),
        Step::Success(vec![])
    );
    (left_db, left, right_db, right)
}
fn reopen_observation(db: &Database) -> Observation {
    let admin = Admin::open(&db.0, Limits::default()).unwrap();
    Observation {
        state: snapshot(&admin),
        step: Step::Success(vec![]),
    }
}
#[test]
fn secondary_indexes_match_after_reopen() {
    let (left_db, left, right_db, right) = indexed_pair();
    for (name, keys, unique) in [
        ("items_ab", vec!["a", "b"], false),
        ("items_ba_unique", vec!["b", "a"], true),
    ] {
        provision_index(&left, false, name, &keys, unique);
        provision_index(&right, true, name, &keys, unique);
        assert_eq!(snapshot(&left), snapshot(&right));
    }
    // Native index creation has an empty-state policy: insert only after DDL.
    assert_eq!(
        sql(&left, "insert into items values(1,10,20)"),
        Step::Success(vec![])
    );
    assert_eq!(
        native(&right, "insert items set id=1,a=10,b=20", false),
        Step::Success(vec![])
    );
    let before = snapshot(&left);
    assert_eq!(
        before[0].indexes,
        vec![
            Index {
                name: "items_ab".into(),
                table: "items".into(),
                keys: vec!["a".into(), "b".into()],
                unique: false
            },
            Index {
                name: "items_ba_unique".into(),
                table: "items".into(),
                keys: vec!["b".into(), "a".into()],
                unique: true
            },
        ]
    );
    drop(left);
    drop(right);
    let left = reopen_observation(&left_db);
    let right = reopen_observation(&right_db);
    assert_eq!(left.state, before);
    assert_eq!(right.state, before);
    assert!(parity(&left, &right));
}
#[test]
fn secondary_index_controls_fail_after_reopen() {
    for control in ["missing", "reversed", "uniqueness"] {
        let (left_db, left, right_db, right) = indexed_pair();
        provision_index(&left, false, "items_ab", &["a", "b"], false);
        match control {
            "missing" => {}
            "reversed" => provision_index(&right, true, "items_ab", &["b", "a"], false),
            "uniqueness" => provision_index(&right, true, "items_ab", &["a", "b"], true),
            _ => unreachable!(),
        }
        drop(left);
        drop(right);
        let left = reopen_observation(&left_db);
        let right = reopen_observation(&right_db);
        // Isolate the index difference: identical rows/schema and success result.
        let mut without_indexes = right.clone();
        without_indexes.state[0].indexes = left.state[0].indexes.clone();
        assert!(parity(&left, &without_indexes), "{control}");
        assert!(!parity(&left, &right), "{control}");
    }
}

#[test]
fn text_index_matches_typed_catalog_and_errors() {
    let (_left_db, left) = Database::fresh(StorageEngine::Legacy);
    let (_right_db, right) = Database::fresh(StorageEngine::Legacy);
    for admin in [&left, &right] {
        Engine::with_version(admin.clone(), LanguageVersion::Embedded014)
            .execute_catalog_text("create table items(id int64,a int64,primary key(id))")
            .unwrap();
    }
    let engine = Engine::with_version(left.clone(), LanguageVersion::Embedded014);
    let typed = giggles_core::operations::Engine::new(right.clone());
    let request = |columns: Vec<String>| giggles_core::operations::CatalogRequest::CreateIndex {
        table: "items".into(),
        name: "items_a".into(),
        columns,
    };
    let native_tree = giggles_language::CatalogStatement::CreateIndex {
        table: "items".into(),
        name: "items_a".into(),
        columns: vec!["a".into()],
    };
    for (keys, columns) in [
        ("missing", vec!["missing".into()]),
        ("a,a", vec!["a".into(), "a".into()]),
    ] {
        let text_error = with_sql_parser_forbidden(|| {
            engine.execute_catalog_text(&format!("create index items_a on items({keys})"))
        })
        .unwrap_err();
        let typed_error = typed.execute_catalog(&request(columns)).unwrap_err();
        assert_eq!(format!("{text_error:?}"), format!("{typed_error:?}"));
    }
    let text_outcome = with_sql_parser_forbidden(|| {
        engine.execute_catalog_text("create index items_a on items(a)")
    })
    .unwrap();
    let tree_outcome = with_sql_parser_forbidden(|| {
        Engine::with_version(right.clone(), LanguageVersion::Embedded014)
            .execute_catalog(&native_tree)
    })
    .unwrap();
    assert_eq!(text_outcome.table, tree_outcome.table);
    assert_eq!(snapshot(&left), snapshot(&right));
    for admin in [&left, &right] {
        assert_eq!(
            sql(admin, "insert into items values(1,2)"),
            Step::Success(vec![])
        );
    }
    assert!(matches!(
        engine.execute_catalog_text("create index another on items(a)"),
        Err(Error::PlanRequired)
    ));
    assert!(matches!(
        typed.execute_catalog(&giggles_core::operations::CatalogRequest::CreateIndex {
            table: "items".into(),
            name: "another".into(),
            columns: vec!["a".into()],
        }),
        Err(giggles_core::operations::Error::PlanRequired)
    ));
}

#[test]
fn index_text_is_opt_in_and_rejects_extended_forms() {
    use giggles_language::{
        CatalogStatement, parse_catalog_with_version, parse_input_with_version,
    };
    let text = "create index items_a on items(a)";
    assert!(matches!(
        parse_catalog_with_version(text, LanguageVersion::Embedded013),
        Err(Error::Unsupported)
    ));
    assert!(matches!(
        parse_catalog_with_version(text, LanguageVersion::Embedded014),
        Ok(CatalogStatement::CreateIndex { .. })
    ));
    assert!(parse_input_with_version(text, LanguageVersion::Embedded014).is_ok());
    for text in [
        "create unique index x on items(a)",
        "create index x on items(a desc)",
        "create index x on items(a asc)",
        "create index x on items(a) where a=1",
        "create index x on public.items(a)",
        "create index x on items()",
        "create index x on items(a+1)",
        "drop index x",
    ] {
        assert!(
            parse_catalog_with_version(text, LanguageVersion::Embedded014).is_err(),
            "{text}"
        );
    }
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn v2_native_read_matches_sql() {
    let (_db, admin) = Database::fresh(StorageEngine::V2);
    sql(&admin, "create table items(id bigint primary key, note text)");
    sql(&admin, "insert into items values(1,'one'),(2,NULL)");
    assert_eq!(native(&admin, "items order by id limit 1024", false),
        sql(&admin, "select * from items order by id"));
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn native_large_reads_keep_v2_parity_and_legacy_cap() {
    for storage in [StorageEngine::Legacy, StorageEngine::V2] {
        let (_db, admin) = Database::fresh(storage);
        let mut writer = admin.sql_session();
        writer.execute("create table items(tenant bigint,id bigint,n bigint,primary key(tenant,id))", &[], None).unwrap();
        writer.execute("create table tenants(tenant bigint primary key,note bigint)", &[], None).unwrap();
        writer.execute("insert into tenants values(7,42)", &[], None).unwrap();
        if storage == StorageEngine::V2 {
            writer.execute("create index items_n on items(n)", &[], None).unwrap();
        }
        let engine = Engine::with_version(admin.clone(), LanguageVersion::Embedded020);
        let simple = Engine::with_version(admin.clone(), LanguageVersion::Embedded07);
        let mut seeded = 0;
        for count in [1024, 1025, 5000] {
            writer.execute("BEGIN", &[], None).unwrap();
            while seeded < count {
                let end = (seeded + 500).min(count);
                let tuples = (seeded..end).map(|id| format!("(7,{id},1)")).collect::<Vec<_>>().join(",");
                writer.execute(&format!("insert into items values{tuples}"), &[], None).unwrap();
                seeded = end;
            }
            writer.execute("COMMIT", &[], None).unwrap();
            let read = |text: &str| with_sql_parser_forbidden(|| simple.execute_text(text, &Bindings::new()));
            if storage == StorageEngine::Legacy && count > 1024 {
                assert_eq!(read("items order by tenant,id limit 1024"), Err(Error::Limit));
                assert_eq!(read(&format!("items limit {count}")), Err(Error::Type));
                continue;
            }
            for (native, sql) in [
                (format!("items order by tenant,id limit {count}"), "select * from items order by tenant,id"),
                (format!("items where n=1 order by id desc limit {count}"), "select * from items where n=1 order by id desc"),
                (format!("items where id>=1023 order by id desc limit {count} select id+1 as next"), "select id+1 as next from items where id>=1023 order by id desc"),
            ] {
                let actual = read(&native).unwrap();
                let sql = if storage == StorageEngine::Legacy { format!("{sql} limit {count}") } else { sql.into() };
                let expected = writer.execute(&sql, &[], None).unwrap();
                assert_eq!(values(actual.rows), values(expected.rows), "{storage:?}, {count}: {native}");
                assert!(actual.complete);
            }
            if storage == StorageEngine::V2 {
                let default_page = read("items order by tenant,id").unwrap();
                assert_eq!((default_page.rows.len(), default_page.matched, default_page.complete), (100, count, false));
                for (native, sql) in [
                    ("items select count(*) as n,sum(id) as total", "select count(*) as n,sum(id) as total from items"),
                    ("items a join tenants b on a.tenant=b.tenant order by a.id select a.id,b.note", "select a.id,b.note from items a join tenants b on a.tenant=b.tenant order by a.id"),
                ] {
                    let actual = with_sql_parser_forbidden(|| engine.execute_text(native, &Bindings::new())).unwrap();
                    let expected = writer.execute(sql, &[], None).unwrap();
                    assert_eq!(values(actual.rows), values(expected.rows), "{count}: {native}");
                }
                let prepared = simple.prepare_bound_text(&format!("items order by tenant,id limit {count}"), &Bindings::new()).unwrap();
                assert_eq!(with_sql_parser_forbidden(|| simple.execute_bound(&prepared)).unwrap().rows.len(), count);
                let page = with_sql_parser_forbidden(|| engine.read_page_text(
                    &format!("page items order by tenant,id limit {count}"), &Bindings::new(),
                )).unwrap();
                assert_eq!(page.outcome.rows.len(), count);
                assert!(page.next.is_none());
                let block = with_sql_parser_forbidden(|| simple.execute_transaction_text(
                    &format!("transaction {{ let r=(items order by tenant,id limit {count}); emit @r.rows }}"), &Bindings::new(),
                )).unwrap();
                assert!(matches!(&block.emitted[0], giggles_language::EmittedValue::Rows { rows, complete: true, .. } if rows.len() == count));
                let explain = giggles_core::operations::Engine::new(admin.clone()).explain_read(
                    &giggles_core::operations::Request::Read { table: "items".into(), filter: None, order: vec![], limit: None, projection: vec![] },
                    &Bindings::new(),
                ).unwrap();
                assert_eq!(explain.limits.input_rows, usize::MAX);
                assert!(explain.stages.contains(&giggles_core::operations::ReadExplainStage::StreamingCompleteTableMaterialization));
            }
        }
    }
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn v2_native_input_streams_past_result_byte_budget() {
    let (_db, admin) = Database::fresh(StorageEngine::V2);
    let mut writer = admin.sql_session();
    writer.execute("create table items(id bigint primary key,note text)", &[], None).unwrap();
    writer.execute("BEGIN", &[], None).unwrap();
    for start in (0..1100).step_by(500) {
        let tuples = (start..(start + 500).min(1100)).map(|id| format!("({id},$1)")).collect::<Vec<_>>().join(",");
        writer.execute(&format!("insert into items values{tuples}"), &[Value::Text("x".repeat(1000))], None).unwrap();
    }
    writer.execute("COMMIT", &[], None).unwrap();
    let engine = Engine::with_version(admin, LanguageVersion::Embedded07);
    with_sql_parser_forbidden(|| {
        assert_eq!(engine.execute_text("items order by id limit 1100 select id", &Bindings::new()).unwrap().rows.len(), 1100);
        assert_eq!(engine.execute_text("items order by id", &Bindings::new()).unwrap().rows.len(), 100);
        assert_eq!(engine.execute_text("items limit 1100", &Bindings::new()), Err(Error::Limit));
    });
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn v2_native_insert_matches_sql() {
    let (left_db, left) = Database::fresh(StorageEngine::V2);
    let (right_db, right) = Database::fresh(StorageEngine::V2);
    for admin in [&left, &right] {
        sql(admin, "create table items(id bigint primary key, label text)");
    }
    assert_eq!(native(&left, "insert items set id=1,label=\"ready\"", false),
        sql(&right, "insert into items values(1,'ready')"));
    assert_eq!(snapshot(&left), snapshot(&right));
    drop(left); drop(right);
    assert!(parity(&reopen_observation(&left_db), &reopen_observation(&right_db)));
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn v2_native_update_matches_sql() {
    let (_left_db, left) = Database::fresh(StorageEngine::V2);
    let (_right_db, right) = Database::fresh(StorageEngine::V2);
    for admin in [&left, &right] {
        sql(admin, "create table items(id bigint primary key, label text)");
        sql(admin, "insert into items values(1,'ready')");
    }
    let result = with_sql_parser_forbidden(|| Engine::with_version(left.clone(), LanguageVersion::Embedded07)
        .execute_text("update items where id=1 set label=\"done\"", &Bindings::new())).unwrap();
    let expected = right.sql_session().execute("update items set label='done' where id=1", &[], None).unwrap();
    assert_eq!(u64::try_from(result.affected).unwrap(), expected.affected);
    assert_eq!(Step::Success(values(result.rows)), Step::Success(values(expected.rows)));
    assert_eq!(snapshot(&left), snapshot(&right));
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn v2_native_delete_matches_sql() {
    let (_left_db, left) = Database::fresh(StorageEngine::V2);
    let (_right_db, right) = Database::fresh(StorageEngine::V2);
    for admin in [&left, &right] {
        sql(admin, "create table items(id bigint primary key, label text)");
        sql(admin, "insert into items values(1,'ready'),(2,'done'),(3,'done')");
    }
    for (text, statement) in [
        ("delete items where id=1", "delete from items where id=1"),
        ("delete many items where true at most 2", "delete from items"),
    ] {
        let result = with_sql_parser_forbidden(|| Engine::with_version(left.clone(), LanguageVersion::Embedded07)
            .execute_text(text, &Bindings::new())).unwrap();
        let expected = right.sql_session().execute(statement, &[], None).unwrap();
        assert_eq!(u64::try_from(result.affected).unwrap(), expected.affected);
        assert_eq!(Step::Success(values(result.rows)), Step::Success(values(expected.rows)));
        assert_eq!(snapshot(&left), snapshot(&right));
    }
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn v2_native_block_matches_sql_transaction() {
    let (_left_db, left) = Database::fresh(StorageEngine::V2);
    let (_right_db, right) = Database::fresh(StorageEngine::V2);
    for admin in [&left, &right] {
        sql(admin, "create table items(id bigint primary key, label text)");
    }
    let result = with_sql_parser_forbidden(|| Engine::with_version(left.clone(), LanguageVersion::Embedded07)
        .execute_transaction_text("transaction { insert items set id=1,label=\"ready\"; update items where id=1 set label=\"done\"; }", &Bindings::new())).unwrap();
    let mut session = right.sql_session();
    session.execute("BEGIN", &[], None).unwrap();
    for (summary, statement) in result.summaries.iter().zip([
        "insert into items values(1,'ready')", "update items set label='done' where id=1",
    ]) {
        assert_eq!(u64::try_from(summary.affected).unwrap(), session.execute(statement, &[], None).unwrap().affected);
    }
    session.execute("COMMIT", &[], None).unwrap();
    assert_eq!(result.summaries.len(), 2);
    assert_eq!(snapshot(&left), snapshot(&right));
}

#[test]
fn numeric_parameters_match_sql() {
    use giggles_core::relational::Numeric;
    for storage in [
        StorageEngine::Legacy,
        #[cfg(feature = "engine-rocksdb")]
        StorageEngine::V2,
    ] {
    let (_left_db, left) = Database::fresh(storage);
    let (_right_db, right) = Database::fresh(storage);
    for admin in [&left, &right] {
        admin.sql_session().execute("create table amounts(id bigint primary key, n numeric(12,4), label text)", &[], None).unwrap();
    }
    let engine = Engine::with_version(left.clone(), LanguageVersion::Embedded015);
    for (id, text) in [(1, Some("12345.6789")), (2, None), (3, Some("1.23456"))] {
        let value = text.map(|s| Value::Numeric(Numeric::parse(s).unwrap())).unwrap_or(Value::Null);
        let bindings = Bindings::from([("n".into(), value.clone())]);
        let result = with_sql_parser_forbidden(|| engine.execute_text(&format!("insert amounts set id={id},n=$n"), &bindings)).unwrap();
        let expected = right.sql_session().execute(&format!("insert into amounts(id,n) values({id},$1)"), &[value], None).unwrap();
        assert_eq!(result.affected as u64, expected.affected);
        assert_eq!(snapshot(&left), snapshot(&right));
    }
    let rows = left.sql_session().execute("select n from amounts where id=1", &[], None).unwrap().rows;
    assert_eq!(rows[0].values, vec![Value::Numeric(Numeric::parse("12345.6789").unwrap())]);
    for (statement, sql_text) in [
        ("update amounts where id=1 set n=$n", "update amounts set n=$1 where id=1"),
        ("insert amounts set id=4,n=$n", "insert into amounts(id,n) values(4,$1)"),
    ] {
        let value = Value::Numeric(Numeric::parse("999999999.99999").unwrap());
        let actual = engine.execute_text(statement, &Bindings::from([("n".into(), value.clone())])).unwrap_err();
        let expected = right.sql_session().execute(sql_text, &[value], None).unwrap_err();
        let CoreError::Sql(expected) = expected else { panic!("unexpected SQL error") };
        assert_eq!(actual.sqlstate(), Some("22003"));
        let expected_code = match expected { SqlError::Overflow => Some("22003"), other => panic!("unexpected SQL error: {other:?}") };
        assert_eq!(actual.sqlstate(), expected_code);
        assert_eq!(snapshot(&left), snapshot(&right));
    }
    let bindings = Bindings::from([("n".into(), Value::Numeric(Numeric::parse("2.34565").unwrap()))]);
    let bound = engine.prepare_bound_text("update amounts where id=1 set n=$n", &bindings).unwrap();
    engine.execute_bound(&bound).unwrap();
    right.sql_session().execute("update amounts set n=$1 where id=1", &[bindings["n"].clone()], None).unwrap();
    assert_eq!(snapshot(&left), snapshot(&right));
    for minor in 1..=14 {
        let version = serde_json::from_str(&format!("\"0.{minor}\"")).unwrap();
        assert_eq!(Engine::with_version(left.clone(), version).execute_text("update amounts where id=1 set n=$n", &bindings), Err(Error::Unsupported));
    }
    for text in ["update amounts where id=1 set label=$n", "update amounts where id=1 set n=$n+1", "amounts select $n"] {
        assert!(engine.execute_text(text, &bindings).is_err());
    }
}
}


#[test]
fn numeric_native_catalog_matches_sql() {
    use giggles_core::relational::Numeric;
    use giggles_language::{CatalogStatement, CatalogType, ColumnSpec, parse_catalog_with_version};
    let text = "create table amounts(id int64,n numeric,opt nullable numeric,primary key(id))";
    let tree = CatalogStatement::CreateTable {
        table: "amounts".into(),
        columns: vec![
            ColumnSpec { name: "id".into(), data_type: CatalogType::Int64, nullable: false },
            ColumnSpec { name: "n".into(), data_type: CatalogType::Numeric, nullable: false },
            ColumnSpec { name: "opt".into(), data_type: CatalogType::Numeric, nullable: true },
        ],
        primary_key: vec!["id".into()],
    };
    let v = LanguageVersion::Embedded016;
    assert_eq!(parse_catalog_with_version(text, v).unwrap(), tree);
    let (left_db, left) = Database::fresh(StorageEngine::Legacy);
    let (right_db, right) = Database::fresh(StorageEngine::Legacy);
    let (tree_db, typed) = Database::fresh(StorageEngine::Legacy);
    for minor in 1..=15 {
        let old = serde_json::from_str(&format!("\"0.{minor}\"")).unwrap();
        assert!(parse_catalog_with_version(text, old).is_err());
        assert_eq!(Engine::with_version(left.clone(), old).execute_catalog(&tree), Err(Error::Unsupported));
    }
    for unsupported in ["numeric(10,2)", "float", "date"] {
        assert!(parse_catalog_with_version(&text.replace("n numeric", &format!("n {unsupported}")), v).is_err());
    }
    with_sql_parser_forbidden(|| {
        Engine::with_version(left.clone(), v).execute_catalog_text(text).unwrap();
        Engine::with_version(typed.clone(), v).execute_catalog(&tree).unwrap();
    });
    right.sql_session().execute("create table amounts(id bigint primary key,n numeric not null,opt numeric)", &[], None).unwrap();
    assert_eq!(snapshot(&left), snapshot(&right));
    assert_eq!(snapshot(&typed), snapshot(&right));
    let catalog = snapshot(&left);
    assert_eq!(catalog[0].columns[1], ("n".into(), DataType::Numeric, false));
    assert_eq!(catalog[0].columns[2], ("opt".into(), DataType::Numeric, true));
    for (id, number) in [(1, "9007199254740993.00100"), (2, "-0.00000000001")] {
        let value = Value::Numeric(Numeric::parse(number).unwrap());
        let bindings = Bindings::from([("n".into(), value.clone())]);
        for admin in [&left, &typed] {
            let result = with_sql_parser_forbidden(|| Engine::with_version(admin.clone(), LanguageVersion::Embedded015)
                .execute_text(&format!("insert amounts set id={id},n=$n"), &bindings)).unwrap();
            assert_eq!(result.affected, 1);
        }
        right.sql_session().execute(&format!("insert into amounts(id,n) values({id},$1)"), &[value], None).unwrap();
    }
    // The new profile retains the prior numeric parameter admission.
    for admin in [&left, &typed] {
        with_sql_parser_forbidden(|| Engine::with_version(admin.clone(), v).execute_text(
            "update amounts where id=1 set opt=$n",
            &Bindings::from([("n".into(), Value::Numeric(Numeric::parse("1.25").unwrap()))]),
        )).unwrap();
    }
    right.sql_session().execute("update amounts set opt=1.25 where id=1", &[], None).unwrap();
    assert_eq!(snapshot(&left), snapshot(&right));
    assert_eq!(snapshot(&typed), snapshot(&right));
    drop(left); drop(right); drop(typed);
    let expected = reopen_observation(&right_db);
    assert!(parity(&reopen_observation(&left_db), &expected));
    assert!(parity(&reopen_observation(&tree_db), &expected));
    #[cfg(feature = "engine-rocksdb")]
    {
        let (_db, v2) = Database::fresh(StorageEngine::V2);
        let engine = Engine::with_version(v2.clone(), v);
        with_sql_parser_forbidden(|| {
            engine.execute_catalog_text(text).unwrap();
            assert_eq!(engine.execute_catalog(&tree), Err(Error::Sql { sqlstate: "42P07".into() }));
        });
        assert_eq!(snapshot(&v2)[0].columns, expected.state[0].columns);
    }
}

#[test]
fn arithmetic_zero_divisor_sqlstate_matches_on_both_engines() {
    // PG 18.6: both bigint division and remainder by zero report 22012.
    for storage in [
        StorageEngine::Legacy,
        #[cfg(feature = "engine-rocksdb")]
        StorageEngine::V2,
    ] {
        let (_left_db, left) = Database::fresh(storage);
        let (_right_db, right) = Database::fresh(storage);
        for admin in [&left, &right] {
            admin.sql_session().execute(
                "create table items(id bigint primary key,n bigint not null)", &[], None,
            ).unwrap();
            admin.sql_session().execute("insert into items values(1,7)", &[], None).unwrap();
        }
        let engine = Engine::with_version(left.clone(), LanguageVersion::Embedded016);
        let before = snapshot(&left);
        for operator in ["/", "%"] {
            let native_text = format!("update items where id=1 set n=n{operator}$zero");
            let sql_text = format!("update items set n=n{operator}$1 where id=1");
            let bindings = Bindings::from([("zero".into(), Value::Int(0))]);
            let bound = with_sql_parser_forbidden(|| {
                engine.prepare_bound_text(&native_text, &bindings)
            }).unwrap();
            for prepared in [false, true] {
                let actual = with_sql_parser_forbidden(|| {
                    if prepared {
                        engine.execute_bound(&bound)
                    } else {
                        engine.execute_text(&native_text, &bindings)
                    }
                }).unwrap_err();
                let expected = right.sql_session().execute(
                    &sql_text, &[Value::Int(0)], None,
                ).unwrap_err();
                assert_eq!(actual, Error::DivisionByZero, "{storage:?}: {native_text}");
                let expected_code = match expected {
                    // SQL wire mapping, independently verified against PG 18.6.
                    CoreError::Sql(SqlError::DivisionByZero) => "22012",
                    other => panic!("unexpected SQL error: {other:?}"),
                };
                assert_eq!(actual.sqlstate(), Some(expected_code));
                assert_eq!(snapshot(&left), before, "failed native update is atomic");
                assert_eq!(snapshot(&right), before, "failed SQL update is atomic");
            }
        }
    }
}

#[cfg(feature = "engine-rocksdb")]
fn seed(admin: &Admin, table: &str, ids: std::ops::Range<usize>) {
    let mut writer = admin.sql_session();
    writer.execute("BEGIN", &[], None).unwrap();
    for start in ids.clone().step_by(500) {
        let tuples = (start..(start + 500).min(ids.end)).map(|id| format!("({id},0,{id})")).collect::<Vec<_>>().join(",");
        writer.execute(&format!("insert into {table} values{tuples}"), &[], None).unwrap();
    }
    writer.execute("COMMIT", &[], None).unwrap();
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn native_large_writes_keep_v2_parity_and_legacy_cap() {
    let (_left_db, left) = Database::fresh(StorageEngine::V2);
    let (_right_db, right) = Database::fresh(StorageEngine::V2);
    for admin in [&left, &right] {
        for table in ["items", "dst", "edge"] {
            sql(admin, &format!("create table {table}(id bigint primary key, n bigint, u bigint)"));
        }
        sql(admin, "alter table items add constraint uq unique(u)");
        seed(admin, "items", 0..5000);
        seed(admin, "edge", 0..1024);
    }
    let engine = Engine::with_version(left.clone(), LanguageVersion::Embedded021);
    let dump = |admin: &Admin, table: &str| values(admin.sql_session()
        .execute(&format!("select * from {table} order by id"), &[], None).unwrap().rows);
    for (native, statement, affected) in [
        ("insert into dst from items select id,n,u", "insert into dst select id,n,u from items", 5000),
        ("update items where id=4999 set n=1", "update items set n=1 where id=4999", 1),
        ("delete items where id=4998", "delete from items where id=4998", 1),
        ("insert items set id=8000,n=0,u=8000", "insert into items values(8000,0,8000)", 1),
        ("insert many into items(id,n,u) values(7000,0,7000),(7001,0,7001) at most 2",
            "insert into items values(7000,0,7000),(7001,0,7001)", 2),
        ("insert into items(id,n,u) values(10,5,10) on conflict(id) do update set n=incoming.n",
            "insert into items values(10,5,10) on conflict(id) do update set n=excluded.n", 1),
        ("insert into items(id,n,u) values(6000,1,6000) on conflict(id) do nothing",
            "insert into items values(6000,1,6000) on conflict(id) do nothing", 1),
        ("update many items where true set n=n+1 at most 5003", "update items set n=n+1", 5003),
        ("update many edge where true set n=1 at most 1024", "update edge set n=1", 1024),
        ("insert edge set id=1024,n=0,u=1024", "insert into edge values(1024,0,1024)", 1),
        ("update many edge where true set n=2 at most 1025", "update edge set n=2", 1025),
        ("delete many dst where true at most 5000", "delete from dst", 5000),
    ] {
        let result = with_sql_parser_forbidden(|| engine.execute_text(native, &Bindings::new()))
            .unwrap_or_else(|error| panic!("{native}: {error:?}"));
        let expected = right.sql_session().execute(statement, &[], None).unwrap();
        assert_eq!((result.affected, expected.affected), (affected, affected as u64), "{native}");
        for table in ["items", "dst", "edge"] {
            assert_eq!(dump(&left, table), dump(&right, table), "{native}: {table}");
        }
    }
    assert_eq!(dump(&left, "items").len(), 5003);
    // Cardinality-explicit semantics remain, now beyond 1,024.
    for (native, error) in [
        ("update many edge where true set n=3 at most 1024", Error::TooManyMatches),
        ("delete many items where true at most 5002", Error::TooManyMatches),
        ("insert items set id=9000,n=0,u=10", Error::Duplicate),
        ("update items where id=11 set u=12", Error::Duplicate),
    ] {
        assert_eq!(with_sql_parser_forbidden(|| engine.execute_text(native, &Bindings::new())), Err(error), "{native}");
    }
    for statement in ["insert into items values(9000,0,10)", "update items set u=12 where id=11"] {
        assert_eq!(sql(&right, statement), Step::Sql(SqlError::Duplicate), "{statement}");
        assert_eq!(giggles_core::sqlstate(&CoreError::Sql(SqlError::Duplicate)), "23505");
    }
    let result = with_sql_parser_forbidden(|| engine.execute_text("delete many items where true at most 5003", &Bindings::new())).unwrap();
    assert_eq!(result.affected, 5003);
    assert_eq!(right.sql_session().execute("delete from items", &[], None).unwrap().affected, 5003);
    assert_eq!(dump(&left, "items"), dump(&right, "items"));
    let many = |base: i64, count: i64| {
        use giggles_core::operations::{Expr, Request, ValueExpr};
        let literal = |n: i64| Expr::Value(ValueExpr::Literal(Value::Int(n)));
        Request::InsertMany { table: "dst".into(), columns: vec!["id".into()],
            rows: (base..base + count).map(|id| vec![literal(id)]).collect(),
            cap: ValueExpr::Literal(Value::Int(count)) }
    };
    let core = giggles_core::operations::Engine::new(left.clone());
    for (base, count) in [(0, 1024), (2000, 1025)] {
        assert_eq!(core.execute(&many(base, count), &Bindings::new()).unwrap().affected, count as usize);
    }
    assert_eq!(dump(&left, "dst").len(), 2049);

    // Legacy keeps its table and count capacity.
    let (_legacy_db, legacy) = Database::fresh(StorageEngine::Legacy);
    sql(&legacy, "create table edge(id bigint primary key, n bigint, u bigint)");
    seed(&legacy, "edge", 0..1024);
    let engine = Engine::with_version(legacy.clone(), LanguageVersion::Embedded021);
    for (native, outcome) in [
        ("update many edge where true set n=1 at most 1024", Ok(1024)),
        ("update many edge where true set n=1 at most 1025", Err(Error::Type)),
        ("insert edge set id=1024,n=0,u=1024", Err(Error::Limit)),
    ] {
        let actual = with_sql_parser_forbidden(|| engine.execute_text(native, &Bindings::new())).map(|out| out.affected);
        assert_eq!(actual, outcome, "{native}");
    }
    sql(&legacy, "create table dst(id bigint primary key)");
    let core = giggles_core::operations::Engine::new(legacy);
    assert_eq!(core.execute(&many(0, 1025), &Bindings::new()), Err(giggles_core::operations::Error::Limit));
    assert_eq!(core.execute(&many(0, 1024), &Bindings::new()).unwrap().affected, 1024);
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn v2_literal_insert_many_and_engine_limits() {
    let text = |table: &str, ids: std::ops::Range<usize>| format!(
        "insert many into {table}(id,n,u) values {} at most {}",
        ids.clone().map(|id| format!("({id},{},{id})", id % 7)).collect::<Vec<_>>().join(","), ids.len());
    let (_left_db, left) = Database::fresh(StorageEngine::V2);
    let (_right_db, right) = Database::fresh(StorageEngine::V2);
    for admin in [&left, &right] {
        sql(admin, "create table items(id bigint primary key, n bigint, u bigint)");
    }
    let engine = Engine::with_version(left.clone(), LanguageVersion::Embedded021);
    let native = text("items", 0..5000);
    assert!(native.len() > giggles_language::MAX_INPUT);
    let result = with_sql_parser_forbidden(|| engine.execute_text(&native, &Bindings::new())).unwrap();
    let sql_text = native.replacen("insert many into items(id,n,u) values", "insert into items values", 1)
        .replacen(" at most 5000", "", 1);
    assert_eq!((result.affected, right.sql_session().execute(&sql_text, &[], None).unwrap().affected), (5000, 5000));
    let dump = |admin: &Admin| values(admin.sql_session().execute("select * from items order by id", &[], None).unwrap().rows);
    assert_eq!(dump(&left), dump(&right));
    assert_eq!(dump(&left).len(), 5000);
    // Non-insert text keeps the 64 KiB / 4,096-token budget.
    assert_eq!(engine.execute_text(&format!("items where {}true", "true and ".repeat(2100)), &Bindings::new()), Err(Error::Limit));

    // Legacy keeps the 1,024-tuple cap, now admitted at bind rather than by text tokens.
    let (_legacy_db, legacy) = Database::fresh(StorageEngine::Legacy);
    sql(&legacy, "create table items(id bigint primary key, n bigint, u bigint)");
    let engine = Engine::with_version(legacy.clone(), LanguageVersion::Embedded021);
    assert_eq!(engine.execute_text(&text("items", 0..1025), &Bindings::new()), Err(Error::Limit));
    assert_eq!(engine.execute_text(&text("items", 0..1024), &Bindings::new()).unwrap().affected, 1024);

    // Advertised limits: existing fields stay legacy; engine_rows separates engines.
    let manifest = serde_json::to_value(giggles_language::capability_manifest_for(LanguageVersion::Embedded021)).unwrap();
    let limits = &manifest["limits"];
    assert_eq!(limits["native_table_rows"], 1024);
    assert_eq!(limits["insert_input_rows"], 1024);
    assert_eq!(limits["insert_text_bytes"], giggles_core::relational::MAX_STATEMENT_SQL);
    for field in ["native_table_rows", "result_page_rows", "mutation_rows", "insert_input_rows"] {
        assert_eq!(limits["engine_rows"]["legacy"][field], 1024, "{field}");
        assert!(limits["engine_rows"]["v2"][field].is_null(), "{field}");
    }
    let profile = serde_json::to_value(giggles_language::profile_for(LanguageVersion::Embedded021)).unwrap();
    assert_eq!((profile["max_rows"].clone(), profile["v2_max_rows"].clone()), (1024.into(), serde_json::Value::Null));
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn v2_point_writes_read_one_key_and_filters_stream() {
    use giggles_core::operations::native_input_rows;
    let (_left_db, left) = Database::fresh(StorageEngine::V2);
    let (_right_db, right) = Database::fresh(StorageEngine::V2);
    for admin in [&left, &right] {
        sql(admin, "create table items(id bigint primary key, n bigint, u bigint)");
        sql(admin, "create table pair(a bigint, b text, n bigint, primary key(a,b))");
        sql(admin, "insert into pair values(1,'x',0),(1,'y',0)");
        seed(admin, "items", 0..20_000);
        sql(admin, "update items set n=id%1000");
    }
    let engine = Engine::with_version(left.clone(), LanguageVersion::Embedded021);
    let dump = |admin: &Admin, table: &str| values(admin.sql_session()
        .execute(&format!("select * from {table} order by 1,2"), &[], None).unwrap().rows);
    // (native, SQL, affected or rows, rows decoded, rows retained)
    for (native, statement, count, decoded, kept) in [
        ("update items where id=12345 set u=0", "update items set u=0 where id=12345", 1, 1, 1),
        ("items where id=777", "select * from items where id=777", 1, 1, 1),
        ("items where 778=id and n=778", "select * from items where id=778 and n=778", 1, 1, 1),
        ("items where id=778 and n=1", "select * from items where id=778 and n=1", 0, 1, 0),
        ("delete items where id=19999", "delete from items where id=19999", 1, 1, 1),
        ("items where id=19999", "select * from items where id=19999", 0, 0, 0),
        ("update pair where a=1 and b=\"y\" set n=5", "update pair set n=5 where a=1 and b='y'", 1, 1, 1),
        ("update many items where n=7 set u=1 at most 20", "update items set u=1 where n=7", 20, 19_999, 20),
        ("items where n=7 order by id desc limit 3", "select * from items where n=7 order by id desc limit 3", 3, 19_999, 20),
        ("delete many items where n=8 or id=5 at most 21", "delete from items where n=8 or id=5", 21, 19_999, 21),
    ] {
        let before = native_input_rows();
        let result = with_sql_parser_forbidden(|| engine.execute_text(native, &Bindings::new()))
            .unwrap_or_else(|error| panic!("{native}: {error:?}"));
        let after = native_input_rows();
        assert_eq!((after.0 - before.0, after.1 - before.1), (decoded, kept), "{native}");
        let expected = right.sql_session().execute(statement, &[], None).unwrap();
        if statement.starts_with("select") {
            assert_eq!(values(result.rows), values(expected.rows), "{native}");
        } else {
            assert_eq!((result.affected, expected.affected), (count, count as u64), "{native}");
        }
        for table in ["items", "pair"] {
            assert_eq!(dump(&left, table), dump(&right, table), "{native}: {table}");
        }
    }
    // Legacy keeps its bounded complete-table input and never takes this path.
    let (_legacy_db, legacy) = Database::fresh(StorageEngine::Legacy);
    sql(&legacy, "create table items(id bigint primary key, n bigint, u bigint)");
    seed(&legacy, "items", 0..10);
    let engine = Engine::with_version(legacy, LanguageVersion::Embedded021);
    let before = native_input_rows();
    assert_eq!(engine.execute_text("update items where id=3 set u=0", &Bindings::new()).unwrap().affected, 1);
    assert_eq!(native_input_rows(), before);
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn v2_inserts_upserts_and_typed_keys_read_only_their_keys() {
    use giggles_core::{operations::{ReadExplainStage as Stage, native_input_rows}, relational::Numeric};
    let (_left_db, left) = Database::fresh(StorageEngine::V2);
    let (_right_db, right) = Database::fresh(StorageEngine::V2);
    let uuid = |n: usize| format!("00000000-0000-0000-0000-{n:012x}");
    for admin in [&left, &right] {
        sql(admin, "create table items(id bigint primary key, n bigint, u bigint)");
        sql(admin, "create table dst(id bigint primary key, n bigint, u bigint)");
        sql(admin, "create table nk(k numeric primary key, n bigint)");
        sql(admin, "create table uk(k uuid primary key, n bigint)");
        seed(admin, "items", 0..20_000);
        let nk = (0..2000).map(|i| format!("({i}.5,0),({i},0)")).collect::<Vec<_>>().join(",");
        sql(admin, &format!("insert into nk values{nk}"));
        let uk = (0..2000).map(|i| format!("('{}',0)", uuid(i))).collect::<Vec<_>>().join(",");
        sql(admin, &format!("insert into uk values{uk}"));
    }
    let engine = Engine::with_version(left.clone(), LanguageVersion::Embedded021);
    let dump = |admin: &Admin, table: &str| values(admin.sql_session()
        .execute(&format!("select * from {table} order by 1"), &[], None).unwrap().rows);
    let key = |text: &str| Bindings::from_iter([("uuid".to_string(), Value::Text(uuid(10).to_uppercase()))]
        .into_iter().filter(|_| text.contains("$uuid")));
    // (native, SQL, affected, rows decoded by native input): O(written), not O(table).
    for (native, statement, count, decoded) in [
        ("insert items set id=30000,n=0,u=30000", "insert into items values(30000,0,30000)", 1, 0),
        ("insert many into items(id,n,u) values(30001,0,30001),(30002,0,30002) at most 2",
            "insert into items values(30001,0,30001),(30002,0,30002)", 2, 0),
        ("insert into items(id,n,u) values(10,5,10) on conflict(id) do update set n=incoming.n",
            "insert into items values(10,5,10) on conflict(id) do update set n=excluded.n", 1, 1),
        ("insert many into items(id,n,u) values(11,1,11),(30003,1,30003) on conflict(id) do nothing at most 2",
            "insert into items values(11,1,11),(30003,1,30003) on conflict(id) do nothing", 1, 1),
        ("insert into dst from items select id,n,u", "insert into dst select id,n,u from items", 20_004, 0),
        ("update uk where k=$uuid set n=1", "update uk set n=1 where k='00000000-0000-0000-0000-00000000000a'", 1, 1),
    ] {
        let before = native_input_rows();
        let result = with_sql_parser_forbidden(|| engine.execute_text(native, &key(native)))
            .unwrap_or_else(|error| panic!("{native}: {error:?}"));
        assert_eq!(native_input_rows().0 - before.0, decoded, "{native}");
        let expected = right.sql_session().execute(statement, &[], None).unwrap();
        assert_eq!((result.affected, expected.affected), (count, count as u64), "{native}");
        for table in ["items", "dst", "nk", "uk"] {
            assert_eq!(dump(&left, table), dump(&right, table), "{native}: {table}");
        }
    }
    // The language admits Numeric values only in assignments (a numeric filter is
    // Type or Unsupported); the core API takes a numeric key literal.
    use giggles_core::operations::{Compare, Expr, Intent, Request, ValueExpr};
    let half = || Expr::Compare(Box::new(Expr::Field("k".into())), Compare::Eq,
        Box::new(Expr::Value(ValueExpr::Literal(Value::Numeric(Numeric::parse("12.50").unwrap())))));
    let core = giggles_core::operations::Engine::new(left.clone());
    let before = native_input_rows();
    let update = Request::Update { table: "nk".into(), filter: half(), guard: None,
        values: vec![("n".into(), ValueExpr::Literal(Value::Int(1)))], intent: Intent::One };
    assert_eq!(core.execute(&update, &Bindings::new()).unwrap().affected, 1);
    assert_eq!(native_input_rows().0 - before.0, 1);
    assert_eq!(right.sql_session().execute("update nk set n=1 where k=12.5", &[], None).unwrap().affected, 1);
    assert_eq!(dump(&left, "nk"), dump(&right, "nk"));
    let read = Request::Read { table: "nk".into(), filter: Some(half()), order: vec![], limit: None, projection: vec![] };
    assert_eq!(core.explain_read(&read, &Bindings::new()).unwrap().stages[0], Stage::PrimaryKeyLookup);
    // Existing keys are found by lookup: 23505 as in SQL, nothing staged.
    for (native, statement, decoded) in [
        ("insert items set id=5,n=0,u=5", "insert into items values(5,0,5)", 0),
        ("insert many into items(id,n,u) values(40000,0,40000),(6,0,6) at most 2",
            "insert into items values(40000,0,40000),(6,0,6)", 1),
    ] {
        let before = native_input_rows();
        assert_eq!(with_sql_parser_forbidden(|| engine.execute_text(native, &Bindings::new())), Err(Error::Duplicate), "{native}");
        assert_eq!(native_input_rows().0 - before.0, decoded, "{native}");
        assert_eq!(sql(&right, statement), Step::Sql(SqlError::Duplicate), "{statement}");
        assert_eq!(dump(&left, "items"), dump(&right, "items"), "{native}");
    }
    // A transaction block carrying a large INSERT MANY has the INSERT text bound.
    let tuples = (50_000..55_000).map(|id| format!("({id},1,{id})")).collect::<Vec<_>>().join(",");
    let block = format!("transaction {{ insert many into items(id,n,u) values{tuples} at most 5000; }}");
    assert!(block.len() > 65_536);
    let before = native_input_rows();
    with_sql_parser_forbidden(|| engine.execute_transaction_text(&block, &Bindings::new())).unwrap();
    assert_eq!(native_input_rows().0 - before.0, 0);
    sql(&right, &format!("insert into items values{tuples}"));
    assert_eq!(dump(&left, "items"), dump(&right, "items"));
    let read = format!("transaction {{ items where {}; }}", vec!["id=1"; 20_000].join(" or "));
    assert_eq!(engine.execute_transaction_text(&read, &Bindings::new()).map(|_| ()), Err(Error::Limit));
    // Temporal values have no key encoding in SQL either, so no temporal key exists to look up.
    sql(&right, "create table tk(k timestamp primary key)");
    assert!(!matches!(sql(&right, "insert into tk values('2020-01-01 00:00:00')"), Step::Success(_)));
    // Explain names the V2 access path; legacy keeps its stages.
    for (text, stage) in [
        ("explain items where id=7", Stage::PrimaryKeyLookup),
        ("explain uk where k=$uuid", Stage::PrimaryKeyLookup),
        ("explain items where n=7 order by id", Stage::StreamingFilter),
        ("explain items where id=7 or id=8", Stage::StreamingFilter),
        ("explain items order by id", Stage::StreamingCompleteTableMaterialization),
    ] {
        let explain = engine.explain_text(text, &key(text)).unwrap_or_else(|error| panic!("{text}: {error:?}"));
        assert_eq!(explain.stages[0], stage, "{text}");
    }
    let (_legacy_db, legacy) = Database::fresh(StorageEngine::Legacy);
    sql(&legacy, "create table items(id bigint primary key, n bigint, u bigint)");
    seed(&legacy, "items", 0..10);
    let engine = Engine::with_version(legacy, LanguageVersion::Embedded021);
    let before = native_input_rows();
    assert_eq!(engine.execute_text("insert into items(id,n,u) values(3,1,3) on conflict(id) do update set n=incoming.n",
        &Bindings::new()).unwrap().affected, 1);
    assert_eq!(engine.execute_text("insert items set id=3,n=0,u=3", &Bindings::new()), Err(Error::Duplicate));
    assert_eq!(native_input_rows(), before);
    let explain = engine.explain_text("explain items where id=7", &Bindings::new()).unwrap();
    assert_eq!(explain.stages[..2], [Stage::WholeStateSnapshot, Stage::BoundedCompleteTableMaterialization]);
}
