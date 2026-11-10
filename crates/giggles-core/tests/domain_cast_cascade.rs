mod common;
use giggles_core::{
    Admin, CoreError, Engine, Limits, SqlSession,
    relational::{SqlError, Value, take_sql_error_message, take_sql_notices},
};
fn run(s: &mut SqlSession, sql: &str) {
    s.execute(sql, &[], None)
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
}
fn err(s: &mut SqlSession, sql: &str, error: SqlError) {
    assert_eq!(
        s.execute(sql, &[], None),
        Err(CoreError::Sql(error)),
        "{sql}"
    );
}
fn domains(engine: Engine) {
    let path = common::fresh("domain-casts");
    {
        let a = Admin::create_engine(&path, Limits::default(), engine).unwrap();
        let mut s = a.sql_session();
        run(
            &mut s,
            "CREATE DOMAIN d AS integer NOT NULL CONSTRAINT positive CHECK (VALUE > 0)",
        );
        for sql in [
            "SELECT '2'::d",
            "SELECT CAST('2' AS d)",
            "SELECT 1 WHERE 2::d = 2",
        ] {
            run(&mut s, sql);
        }
        assert_eq!(
            s.execute("SELECT $1::d", &[Value::Int(3)], None)
                .unwrap()
                .rows[0]
                .values,
            vec![Value::Int(3)]
        );
        run(&mut s, "CREATE TABLE t (id int PRIMARY KEY, n int)");
        run(&mut s, "INSERT INTO t VALUES (1, '2'::d)");
        for sql in [
            "SELECT NULL::d",
            "SELECT CAST(NULL AS d)",
            "SELECT 1 WHERE NULL::d = 1",
            "INSERT INTO t VALUES (2, NULL::d)",
        ] {
            err(&mut s, sql, SqlError::Null);
            assert_eq!(
                take_sql_error_message().as_deref(),
                Some("domain d does not allow null values")
            );
        }
        for sql in [
            "SELECT 0::d",
            "SELECT CAST(-1 AS d)",
            "SELECT 1 WHERE 0::d = 1",
            "INSERT INTO t VALUES (2, 0::d)",
        ] {
            err(&mut s, sql, SqlError::CheckViolation);
            assert_eq!(
                take_sql_error_message().as_deref(),
                Some("value for domain d violates check constraint \"positive\"")
            );
        }
        assert_eq!(
            s.execute("SELECT $1::d", &[Value::Null], None),
            Err(CoreError::Sql(SqlError::Null))
        );
        run(&mut s, "CREATE DOMAIN short AS varchar(2)");
        run(&mut s, "SELECT 'abcd'::short");
    }
    std::fs::remove_dir_all(path).unwrap();
}
fn cascade(engine: Engine) {
    let path = common::fresh("type-cascade");
    {
        let a = Admin::create_engine(&path, Limits::default(), engine).unwrap();
        let mut s = a.sql_session();
        for sql in [
            "CREATE TYPE e AS ENUM ('a')",
            "CREATE DOMAIN d AS int",
            "CREATE TYPE c AS (x int)",
            "CREATE TABLE t (id int PRIMARY KEY, v e CHECK(v = 'a'))",
            "CREATE INDEX vi ON t(v)",
            "CREATE TABLE u (id int PRIMARY KEY, v e)",
            "INSERT INTO t VALUES (1, 'a')",
            "INSERT INTO u VALUES (1, 'a')",
        ] {
            run(&mut s, sql);
        }
        err(&mut s, "DROP TYPE e", SqlError::DependentObjects);
        err(
            &mut s,
            "DROP TYPE e, absent CASCADE",
            SqlError::UndefinedObject,
        );
        run(&mut s, "SELECT v FROM t");
        run(&mut s, "SELECT v FROM u");
        run(&mut s, "DROP TYPE e, d, c CASCADE");
        let notices = take_sql_notices();
        assert_eq!(notices.len(), 1);
        assert_eq!(notices[0].0, "drop cascades to 2 other objects");
        assert_eq!(
            notices[0].1.as_deref(),
            Some("drop cascades to column v of table t\ndrop cascades to column v of table u")
        );
        for table in ["t", "u"] {
            assert_eq!(
                s.execute(&format!("SELECT * FROM {table}"), &[], None)
                    .unwrap()
                    .rows[0]
                    .values,
                vec![Value::Int(1)]
            );
        }
        run(&mut s, "CREATE INDEX vi ON t(id)");
        run(&mut s, "INSERT INTO t VALUES (2)");
        run(&mut s, "DROP TYPE IF EXISTS absent CASCADE");
        assert_eq!(
            take_sql_notices(),
            vec![("type \"absent\" does not exist, skipping".into(), None)]
        );
        run(&mut s, "CREATE DOMAIN only_type AS int");
        run(&mut s, "CREATE TABLE only_table (v only_type)");
        err(&mut s, "DROP TYPE only_type CASCADE", SqlError::Unsupported);
        run(&mut s, "INSERT INTO only_table VALUES (7)");
    }
    std::fs::remove_dir_all(path).unwrap();
}
#[test]
fn legacy_domains() {
    domains(Engine::Legacy);
}
#[test]
fn legacy_cascade() {
    cascade(Engine::Legacy);
}
#[cfg(feature = "engine-rocksdb")]
#[test]
fn v2_domains() {
    domains(Engine::V2);
}
#[cfg(feature = "engine-rocksdb")]
#[test]
fn v2_cascade() {
    cascade(Engine::V2);
}
