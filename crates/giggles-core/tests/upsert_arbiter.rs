//! PostgreSQL 18.6 oracle: p1-sf/ua-pg.log (2 and 1,101 input rows).
mod common;
use giggles_core::{Admin, CoreError, Engine, Limits, SqlSession, relational::{SqlError, Value}};

fn run(s: &mut SqlSession, sql: &str) -> giggles_core::relational::SqlResult {
    s.execute(sql, &[], None).unwrap_or_else(|e| panic!("{sql}: {e:?}"))
}

fn cases(engine: Engine, count: usize) {
    let path = common::fresh("upsert-arbiter");
    let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
    let mut s = admin.sql_session();
    for (target, arbiter_id) in [("(id)", true), ("(email)", false),
        ("ON CONSTRAINT ua_pkey", true), ("ON CONSTRAINT ua_email_key", false), ("", true)] {
        for repeat_id in [true, false] {
            for update in [true, false] {
                run(&mut s, "CREATE TABLE ua(id int CONSTRAINT ua_pkey PRIMARY KEY, email text CONSTRAINT ua_email_key UNIQUE)");
                run(&mut s, "INSERT INTO ua VALUES(0,'keep')");
                let before = run(&mut s, "SELECT * FROM ua").rows;
                let values = (1..=count).map(|i| format!("({},'e{}')",
                    if repeat_id && i == count { 1 } else { i },
                    if !repeat_id && i == count { 1 } else { i }))
                    .collect::<Vec<_>>().join(",");
                let action = if update { "UPDATE SET email=excluded.email" } else { "NOTHING" };
                let sql = format!("INSERT INTO ua VALUES{values} ON CONFLICT {target} DO {action}");
                let error = if update && target.is_empty() { Some(SqlError::Syntax) }
                    else if update && repeat_id == arbiter_id { Some(SqlError::CardinalityViolation) }
                    else if !update && (target.is_empty() || repeat_id == arbiter_id) { None }
                    else { Some(SqlError::Duplicate) };
                let result = s.execute(&sql, &[], None);
                if let Some(error) = error {
                    assert_eq!(result, Err(CoreError::Sql(error)), "count={count} target={target} repeat_id={repeat_id} update={update}");
                    assert_eq!(run(&mut s, "SELECT * FROM ua").rows, before, "failed statement must roll back every page");
                } else {
                    assert_eq!(result.unwrap().affected, (count - 1) as u64);
                    assert_eq!(run(&mut s, "SELECT count(*) FROM ua").rows[0].values, vec![Value::Int(count as i64)]);
                    assert_eq!(run(&mut s, "SELECT * FROM ua WHERE id=0").rows, before);
                }
                run(&mut s, "DROP TABLE ua");
            }
        }
    }
    // A non-arbiter clash in EXCLUDED is harmless if the update discards it.
    for target in ["(id)", "ON CONSTRAINT ua_pkey"] {
        run(&mut s, "CREATE TABLE ua(id int CONSTRAINT ua_pkey PRIMARY KEY, email text CONSTRAINT ua_email_key UNIQUE)");
        run(&mut s, &format!("INSERT INTO ua VALUES(0,'keep'),({count},'old')"));
        let values = (1..=count).map(|i| format!("({i},'e{}')", if i == count { 1 } else { i }))
            .collect::<Vec<_>>().join(",");
        assert_eq!(run(&mut s, &format!("INSERT INTO ua VALUES{values} ON CONFLICT {target} DO UPDATE SET email=ua.email")).affected, count as u64);
        assert_eq!(run(&mut s, "SELECT count(*) FROM ua").rows[0].values, vec![Value::Int(count as i64 + 1)]);
        assert_eq!(run(&mut s, &format!("SELECT email FROM ua WHERE id={count}")).rows[0].values, vec![Value::Text("old".into())]);
        run(&mut s, "DROP TABLE ua");
    }
    drop(s);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn small_legacy_upserts_distinguish_arbiters() {
    cases(Engine::Legacy, 2);
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn small_v2_upserts_distinguish_arbiters() {
    cases(Engine::V2, 2);
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn paged_v2_upserts_distinguish_arbiters() {
    cases(Engine::V2, 1101);
}
