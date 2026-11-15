mod common;
use giggles_core::{Admin, CoreError, Engine, Limits, SqlSession, relational::{SqlError, Value}};
use giggles_core::operations::{Bindings, Request, ValueExpr};

fn engines() -> Vec<Engine> {
    #[allow(unused_mut)]
    let mut engines = vec![Engine::Legacy];
    #[cfg(feature = "engine-rocksdb")]
    engines.push(Engine::V2);
    engines
}
fn trap<T>(work: impl FnOnce() -> T) -> T {
    #[cfg(feature = "test-sql-parser-trap")]
    { giggles_core::relational::with_sql_parser_forbidden(work) }
    #[cfg(not(feature = "test-sql-parser-trap"))]
    { work() }
}
fn control(s: &mut SqlSession, native: bool, verb: &str, name: &str) -> Result<(), CoreError> {
    if native {
        trap(|| match verb {
            "SAVEPOINT" => s.savepoint(name),
            "ROLLBACK TO" => s.rollback_to(name),
            "RELEASE" => s.release(name),
            _ => unreachable!(),
        })
    } else {
        s.execute(&format!("{verb} \"{}\"", name.replace('"', "\"\"")), &[], None).map(|_| ())
    }
}
fn insert(s: &mut SqlSession, native: bool, id: i64) -> Result<(), String> {
    if native {
        trap(|| s.execute_native(&Request::Insert {
            table: "t".into(),
            values: vec![("id".into(), ValueExpr::Literal(Value::Int(id)))],
        }, &Bindings::new())).map(|_| ()).map_err(|e| e.sqlstate().unwrap().to_owned())
    } else {
        s.execute(&format!("INSERT INTO t VALUES({id})"), &[], None)
            .map(|_| ()).map_err(|e| giggles_core::sqlstate(&e).to_owned())
    }
}
fn rows(s: &mut SqlSession) -> Vec<Vec<Value>> {
    s.execute("SELECT id FROM t ORDER BY id", &[], None).unwrap()
        .rows.into_iter().map(|row| row.values).collect()
}
fn fixture(engine: Engine, test: impl FnOnce(&Admin)) {
    let path = common::fresh("native-savepoints");
    let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
    admin.sql_session().execute("CREATE TABLE t(id BIGINT PRIMARY KEY)", &[], None).unwrap();
    test(&admin);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn nested_duplicate_marks_release_and_native_writes_match_sql() {
    for engine in engines() {
        fixture(engine, |admin| {
            let mut outcomes = Vec::new();
            for native in [false, true] {
                let mut s = admin.sql_session();
                trap(|| s.begin_native()).unwrap();
                insert(&mut s, native, 1).unwrap();
                control(&mut s, native, "SAVEPOINT", "outer").unwrap();
                insert(&mut s, native, 2).unwrap();
                control(&mut s, native, "SAVEPOINT", "inner").unwrap();
                insert(&mut s, native, 3).unwrap();
                control(&mut s, native, "SAVEPOINT", "inner").unwrap();
                insert(&mut s, native, 4).unwrap();
                control(&mut s, native, "ROLLBACK TO", "inner").unwrap();
                let youngest = rows(&mut s);
                control(&mut s, native, "RELEASE", "inner").unwrap();
                control(&mut s, native, "ROLLBACK TO", "inner").unwrap();
                let older = rows(&mut s);
                control(&mut s, native, "ROLLBACK TO", "outer").unwrap();
                let outer = rows(&mut s);
                // The target survives rollback, and releasing it removes younger marks.
                control(&mut s, native, "SAVEPOINT", "younger").unwrap();
                control(&mut s, native, "RELEASE", "outer").unwrap();
                assert_eq!(control(&mut s, native, "ROLLBACK TO", "younger"),
                    Err(CoreError::Sql(SqlError::InvalidSavepoint)));
                trap(|| s.rollback_native()).unwrap();
                outcomes.push((youngest, older, outer));
            }
            assert_eq!(outcomes[0], outcomes[1]);
            assert_eq!(outcomes[1], (
                vec![vec![Value::Int(1)], vec![Value::Int(2)], vec![Value::Int(3)]],
                vec![vec![Value::Int(1)], vec![Value::Int(2)]],
                vec![vec![Value::Int(1)]],
            ));
        });
    }
}

#[test]
fn rollback_to_recovers_native_statement_errors_and_preserves_committable_work() {
    for engine in engines() {
        fixture(engine, |admin| {
            for native in [false, true] {
                let mut s = admin.sql_session();
                s.begin_native().unwrap();
                insert(&mut s, native, 1).unwrap();
                control(&mut s, native, "SAVEPOINT", "healthy").unwrap();
                assert_eq!(insert(&mut s, native, 1).unwrap_err(), "23505");
                assert_eq!(insert(&mut s, native, 2).unwrap_err(), "25P02");
                for verb in ["SAVEPOINT", "RELEASE"] {
                    assert_eq!(control(&mut s, native, verb, "healthy"),
                        Err(CoreError::Sql(SqlError::Aborted)));
                }
                assert_eq!(control(&mut s, native, "ROLLBACK TO", "missing"),
                    Err(CoreError::Sql(SqlError::InvalidSavepoint)));
                assert!(s.is_aborted());
                control(&mut s, native, "ROLLBACK TO", "healthy").unwrap();
                assert!(!s.is_aborted());
                insert(&mut s, native, 2).unwrap();
                control(&mut s, native, "RELEASE", "healthy").unwrap();
                trap(|| s.commit_native()).unwrap();
                assert_eq!(rows(&mut s), vec![vec![Value::Int(1)], vec![Value::Int(2)]]);
                s.execute("DELETE FROM t", &[], None).unwrap();
            }
        });
    }
}

#[test]
fn identifier_values_truncate_at_utf8_boundaries_and_control_errors_match_sql() {
    for engine in engines() {
        fixture(engine, |admin| {
            for native in [false, true] {
                let mut s = admin.sql_session();
                for verb in ["SAVEPOINT", "ROLLBACK TO", "RELEASE"] {
                    assert_eq!(control(&mut s, native, verb, "idle"),
                        Err(CoreError::Sql(SqlError::NoActiveTransaction)));
                }
                s.begin_native().unwrap();
                for name in ["MiXeD", "a\"b", "pg_catalog", "savepoint"] {
                    control(&mut s, native, "SAVEPOINT", name).unwrap();
                    control(&mut s, native, "RELEASE", name).unwrap();
                }
                for length in [61, 62] {
                    let prefix = format!("{}\u{e9}", "a".repeat(length));
                    control(&mut s, native, "SAVEPOINT", &format!("{prefix}X")).unwrap();
                    control(&mut s, native, "RELEASE", &format!("{prefix}Y")).unwrap();
                }
                control(&mut s, native, "SAVEPOINT", "MiXeD").unwrap();
                assert_eq!(control(&mut s, native, "RELEASE", "mixed"),
                    Err(CoreError::Sql(SqlError::InvalidSavepoint)));
                control(&mut s, native, "ROLLBACK TO", "MiXeD").unwrap();
                trap(|| s.rollback_native()).unwrap();
            }
            let mut s = admin.sql_session();
            for invalid in ["", "bad\0name"] {
                s.begin_native().unwrap();
                assert_eq!(trap(|| s.savepoint(invalid)), Err(CoreError::Sql(SqlError::Syntax)));
                assert!(s.is_aborted());
                s.rollback_native().unwrap();
            }
        });
    }
}
