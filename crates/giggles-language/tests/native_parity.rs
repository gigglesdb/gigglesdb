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
}
fn values(rows: Vec<SqlRow>) -> Vec<Vec<Value>> {
    rows.into_iter().map(|row| row.values).collect()
}
fn snapshot(admin: &Admin) -> Vec<Table> {
    let mut tables = admin.catalog().unwrap().1;
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
            Table {
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
    sql == native // No unsupported/error wildcard can produce a pass.
}
#[test]
fn legacy_create_insert_update_select_and_duplicate_gap() {
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
            // TODO(R2 plan item 9): preserve structured core errors in native.
            // Assert the opaque Core variant; never derive a SQLSTATE from text.
            assert_eq!(left.step, Step::Sql(SqlError::Duplicate));
            assert!(matches!(right.step, Step::Native(Error::Core(_))));
            assert!(!parity(&left, &right), "known error gap is not parity");
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
