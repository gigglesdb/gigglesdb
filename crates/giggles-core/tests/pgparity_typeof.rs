mod common;
use giggles_core::{
    Admin, Limits, SqlSession,
    relational::{DataType, Value},
};

fn run(s: &mut SqlSession, sql: &str) -> Vec<Vec<Value>> {
    s.execute(sql, &[], None)
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
        .rows
        .into_iter()
        .map(|r| r.values)
        .collect()
}
fn texts(names: &[&str]) -> Vec<Value> {
    names.iter().map(|s| Value::Text((*s).into())).collect()
}
#[test]
fn table_expression_matrix() {
    let a = Admin::create(common::fresh("pgparity-typeof-matrix"), Limits::default()).unwrap();
    let mut s = a.sql_session();
    run(
        &mut s,
        "CREATE TABLE a(id int PRIMARY KEY, v text[], i int[], j jsonb, t text)",
    );
    run(
        &mut s,
        r#"INSERT INTO a VALUES (1,'{a,b}','{1,2}','{"a":1}','x')"#,
    );
    for expr in ["v || '{c}'", "j || '1'", "t || 1", "'a'::varchar(3)", "now()", "1.5", "NULL", "'x'", "true", "current_date", "1::smallint", "1::real", "1::float8", "'a'::char(2)", "'1 day'::interval", "'00:00'::time", "localtimestamp", "gen_random_uuid()", "'\\x00'::bytea"] {
        run(&mut s, &format!("SELECT pg_typeof({expr}) FROM a WHERE id = 1"));
    }
    assert_eq!(
        run(
            &mut s,
            r#"SELECT pg_typeof(v || '{c}'), pg_typeof(j || '1'), pg_typeof(t || 1), pg_typeof(v), pg_typeof(i), pg_typeof(t), pg_typeof(id), pg_typeof(id::bigint), pg_typeof('a'::varchar(3)), pg_typeof(now()), pg_typeof(1.5), pg_typeof(NULL), pg_typeof('x'), pg_typeof(true), pg_typeof(current_date), pg_typeof(1::smallint), pg_typeof(1::real), pg_typeof(1::float8), pg_typeof('a'::char(2)), pg_typeof('1 day'::interval), pg_typeof('00:00'::time), pg_typeof(localtimestamp), pg_typeof(gen_random_uuid()), pg_typeof('\x00'::bytea) FROM a WHERE id = 1"#
        ),
        vec![texts(&[
            "text[]",
            "jsonb",
            "text",
            "text[]",
            "integer[]",
            "text",
            "integer",
            "bigint",
            "character varying",
            "timestamp with time zone",
            "numeric",
            "unknown",
            "unknown",
            "boolean",
            "date",
            "smallint",
            "real",
            "double precision",
            "character",
            "interval",
            "time without time zone",
            "timestamp without time zone",
            "uuid",
            "bytea"
        ])]
    );
    for (expr, name) in [
        ("v || '{c}'", "text[]"),
        ("v || '{c}'::text[]", "text[]"),
        (r#"j || '{"b":2}'"#, "jsonb"),
        (r#"j || '{"b":2}'::jsonb"#, "jsonb"),
    ] {
        let result = s
            .execute(
                &format!("SELECT id, pg_typeof({expr}) AS result_type FROM a ORDER BY id"),
                &[],
                None,
            )
            .unwrap();
        assert_eq!(result.columns[1], "result_type");
        assert_eq!(
            result.rows[0].values,
            vec![Value::Int(1), Value::Text(name.into())]
        );
    }
}
#[test]
fn table_free_regtype_and_enum() {
    let a = Admin::create(common::fresh("pgparity-typeof-free"), Limits::default()).unwrap();
    let mut s = a.sql_session();
    assert_eq!(
        run(&mut s, "SELECT pg_typeof(1), pg_typeof('{a}'::text[])"),
        vec![texts(&["integer", "text[]"])]
    );
    assert_eq!(
        run(
            &mut s,
            "SELECT pg_typeof(pg_typeof(1)), pg_typeof(1)::text, pg_typeof(1) = 'integer'::regtype, pg_typeof(1) = 'int4'::regtype"
        ),
        vec![vec![
            Value::Text("regtype".into()),
            Value::Text("integer".into()),
            Value::Bool(true),
            Value::Bool(true)
        ]]
    );
    let result = s.execute("SELECT pg_typeof(1)", &[], None).unwrap();
    assert_eq!(result.columns[0], "pg_typeof");
    let prepared = s.prepare("SELECT pg_typeof(1)").unwrap();
    assert_eq!(
        s.describe(&prepared).unwrap().columns[0].data_type,
        DataType::RegType
    );
    run(&mut s, "CREATE TYPE r0c_e AS ENUM ('a','b')");
    run(&mut s, "CREATE TABLE e(id int PRIMARY KEY, v r0c_e)");
    run(&mut s, "INSERT INTO e VALUES (1,'a')");
    assert_eq!(
        run(&mut s, "SELECT pg_typeof(v) FROM e"),
        vec![texts(&["r0c_e"])]
    );
    assert!(
        s.execute(
            "CREATE TABLE forbidden(id int PRIMARY KEY, v regtype)",
            &[],
            None
        )
        .is_err()
    );
}
#[test]
fn regtype_serializes_as_legacy_text() {
    assert_eq!(
        bincode::serialize(&DataType::RegType).unwrap(),
        bincode::serialize(&DataType::Text).unwrap()
    );
    assert_eq!(
        serde_json::to_string(&DataType::RegType).unwrap(),
        serde_json::to_string(&DataType::Text).unwrap()
    );
}
