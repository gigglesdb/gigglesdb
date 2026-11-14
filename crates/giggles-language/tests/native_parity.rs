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
#[test]
fn native_on_v2_allows_only_transaction_controls() {
    // giggles-language's core dependency enables engine-rocksdb even when this
    // crate is tested with --no-default-features.
    let (_db, admin) = Database::fresh(StorageEngine::V2);
    // R2 item 2: typed BEGIN/ROLLBACK run on v2 without the SQL parser.
    with_sql_parser_forbidden(|| {
        let mut session = admin.sql_session();
        session.begin_native().unwrap();
        session.rollback_native().unwrap();
    });
    // Every other native operation is still refused on v2.
    assert!(matches!(
        native(&admin, "create table items(id int64,primary key(id))", true),
        Step::Native(Error::Core(_))
    ));
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

#[test]
fn v2_native_read_matches_sql() {
    let (_db, admin) = Database::fresh(StorageEngine::V2);
    sql(&admin, "create table items(id bigint primary key, note text)");
    sql(&admin, "insert into items values(1,'one'),(2,NULL)");
    assert_eq!(native(&admin, "items order by id limit 1024", false),
        sql(&admin, "select * from items order by id"));
}
