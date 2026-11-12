//! PostgreSQL 18: comparison with NULL is unknown, and WHERE selects only true.
//! NOT NULL constrains stored values, not the other operand of a comparison.
mod common;
use giggles_core::{Admin, Engine, Limits, SqlSession, relational::Value};

fn fixture(engine: Engine, test: impl FnOnce(&mut SqlSession)) {
    let path = common::fresh("null-key-predicate");
    let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
    let mut session = admin.sql_session();
    for sql in [
        "CREATE TABLE t(id bigint PRIMARY KEY,v bigint,n bigint NOT NULL)",
        "INSERT INTO t VALUES (1,10,20),(2,30,40)",
        "CREATE TABLE c(a bigint,b bigint,v bigint,PRIMARY KEY(a,b))",
        "INSERT INTO c VALUES (1,1,10),(1,2,30)",
    ] {
        session.execute(sql, &[], None).unwrap();
    }
    test(&mut session);
    drop(session);
    drop(admin);
    if path.is_dir() {
        std::fs::remove_dir_all(path).unwrap();
    } else {
        std::fs::remove_file(path).unwrap();
    }
}

fn mutation(
    engine: Engine,
    delete: bool,
    table: &str,
    predicate: &str,
    params: &[Value],
    count: u64,
) {
    for returning in [false, true] {
        fixture(engine, |s| {
            let command = if delete {
                format!("DELETE FROM {table}")
            } else {
                format!("UPDATE {table} SET v=v+1")
            };
            let suffix = if returning { " RETURNING v" } else { "" };
            let sql = format!("{command} WHERE {predicate}{suffix}");
            let result = s
                .execute(&sql, params, None)
                .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
            // PG18: NULL alone matches nothing; IN/OR can still match their true arm.
            assert_eq!(result.affected, count, "{sql}");
            assert_eq!(
                result.rows.len(),
                if returning { count as usize } else { 0 },
                "{sql}"
            );
            if returning && count == 1 {
                assert_eq!(
                    result.rows[0].values,
                    [Value::Int(if delete { 10 } else { 11 })]
                );
            }
            let actual = s
                .execute(&format!("SELECT v FROM {table} ORDER BY v"), &[], None)
                .unwrap()
                .rows
                .into_iter()
                .map(|r| r.values)
                .collect::<Vec<_>>();
            let expected = match (delete, count) {
                (_, 0) => vec![vec![Value::Int(10)], vec![Value::Int(30)]],
                (true, _) => vec![vec![Value::Int(30)]],
                (false, _) => vec![vec![Value::Int(11)], vec![Value::Int(30)]],
            };
            assert_eq!(actual, expected, "{sql}");
        });
    }
}

fn reused(engine: Engine, delete: bool) {
    fixture(engine, |s| {
        let sql = if delete {
            "DELETE FROM t WHERE id=$1 RETURNING id"
        } else {
            "UPDATE t SET v=v+1 WHERE id=$1 RETURNING id"
        };
        let prepared = s.prepare(sql).unwrap();
        // PG18: the same prepared plan can bind NULL, then an ordinary key.
        let first = s.execute_prepared(&prepared, &[Value::Null], None).unwrap();
        assert_eq!(first.affected, 0);
        assert!(first.rows.is_empty());
        let second = s
            .execute_prepared(&prepared, &[Value::Int(1)], None)
            .unwrap();
        assert_eq!(second.affected, 1);
        assert_eq!(second.rows[0].values, [Value::Int(1)]);
    });
}

macro_rules! cases {
    ($engine:expr) => {
        macro_rules! predicate {
            ($name:ident, $table:expr, $predicate:expr, $params:expr, $count:expr) => {
                mod $name {
                    use super::*;
                    #[test]
                    fn update() {
                        mutation($engine, false, $table, $predicate, $params, $count);
                    }
                    #[test]
                    fn delete() {
                        mutation($engine, true, $table, $predicate, $params, $count);
                    }
                }
            };
        }
        predicate!(literal, "t", "id=NULL", &[], 0);
        predicate!(parameter, "t", "id=$1", &[Value::Null], 0);
        predicate!(composite_literal, "c", "a=1 AND b=NULL", &[], 0);
        predicate!(
            composite_parameter,
            "c",
            "a=$1 AND b=$2",
            &[Value::Int(1), Value::Null],
            0
        );
        predicate!(membership_mixed, "t", "id IN (1,NULL)", &[], 1);
        predicate!(membership_null, "t", "id IN (NULL)", &[], 0);
        predicate!(conjunction, "t", "id=NULL AND v=10", &[], 0);
        predicate!(disjunction, "t", "id=NULL OR id=1", &[], 1);
        predicate!(non_key, "t", "v=NULL", &[], 0);
        predicate!(not_null_non_key, "t", "n=NULL", &[], 0);
        predicate!(reversed, "t", "NULL=id", &[], 0);
        predicate!(range, "t", "id>NULL", &[], 0);
        #[test]
        fn reused_update() {
            reused($engine, false);
        }
        #[test]
        fn reused_delete() {
            reused($engine, true);
        }
        #[test]
        fn select() {
            fixture($engine, |s| {
                // PG18 SELECT already observes the same three-valued logic.
                for (sql, params) in [
                    ("SELECT id FROM t WHERE id=NULL", vec![]),
                    ("SELECT id FROM t WHERE id=$1", vec![Value::Null]),
                ] {
                    assert!(
                        s.execute(sql, &params, None).unwrap().rows.is_empty(),
                        "{sql}"
                    );
                }
            });
        }
    };
}

mod legacy {
    use super::*;
    cases!(Engine::Legacy);
}
#[cfg(feature = "engine-rocksdb")]
mod v2 {
    use super::*;
    cases!(Engine::V2);
}
