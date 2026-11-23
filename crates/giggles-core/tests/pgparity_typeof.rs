mod common;
use giggles_core::{
    Admin, Limits, SqlSession,
    relational::{DataType, StatementClock, Value},
};
use giggles_pg_types::temporal::SessionZone;

const CLOCK: StatementClock = StatementClock {
    unavailable: None,
    transaction_us: 86_400_000_100,
    statement_us: 86_400_000_100,
    zone: Some(SessionZone::Fixed(0)),
    wall: None,
};

fn run(s: &mut SqlSession, sql: &str) -> Vec<Vec<Value>> {
    s.execute_at(sql, &[], None, &CLOCK)
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
fn regtype_cast_validates_qualified_and_signed_type_modifiers() {
    use giggles_core::{CoreError, relational::{SqlError, take_sql_error_message}};

    let a = Admin::create(common::fresh("pgparity-typeof-regtype-modifiers"), Limits::default())
        .unwrap();
    let mut s = a.sql_session();
    assert_eq!(
        run(&mut s, r#"SELECT 'numeric(3,-2)'::regtype, '"pg_catalog"."varchar"(8)'::regtype"#),
        vec![texts(&["numeric", "character varying"])]
    );
    for (ty, error, message) in [
        ("pg_catalog.numeric(0)", SqlError::InvalidParameterValue, "NUMERIC precision 0 must be between 1 and 1000"),
        ("pg_catalog.varchar(0)", SqlError::InvalidParameterValue, "length for type varchar must be at least 1"),
        ("numeric(1,-1001)", SqlError::InvalidParameterValue, "NUMERIC scale -1001 must be between -1000 and 1000"),
        ("pg_catalog.float4(1)", SqlError::Syntax, r#"type modifier is not allowed for type "pg_catalog.float4""#),
    ] {
        let sql = format!("SELECT '{ty}'::regtype");
        assert_eq!(s.execute(&sql, &[], None), Err(CoreError::Sql(error)), "{ty}");
        assert_eq!(take_sql_error_message().as_deref(), Some(message), "{ty}");
    }
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

fn assert_sqlstate(s: &mut SqlSession, sql: &str, expected: &str) {
    let error = s.execute_at(sql, &[], None, &CLOCK).unwrap_err();
    assert_eq!(giggles_core::sqlstate(&error), expected, "{sql}: {error:?}");
}

#[test]
fn evaluates_constants_and_aborts_a_transaction_on_error() {
    let a = Admin::create(
        common::fresh("pgparity-typeof-constants"),
        Limits::default(),
    )
    .unwrap();
    let mut s = a.sql_session();
    let divide = "SELECT pg_typeof(1/0)";
    assert_sqlstate(&mut s, divide, "22012");
    run(&mut s, "BEGIN");
    assert_sqlstate(&mut s, divide, "22012");
    assert_sqlstate(&mut s, "SELECT 1", "25P02");
    run(&mut s, "ROLLBACK");
    assert_eq!(run(&mut s, "SELECT 1"), vec![vec![Value::Int(1)]]);
    assert_eq!(
        run(&mut s, "SELECT 'today'::text::date = current_date"),
        vec![vec![Value::Bool(true)]]
    );

    assert_eq!(
        run(
            &mut s,
            "SELECT pg_typeof('today'::text::date), pg_typeof('now'::timestamptz), pg_typeof(current_date), pg_typeof('abc'), pg_typeof(NULL), pg_typeof(gen_random_uuid())",
        ),
        vec![texts(&[
            "date",
            "timestamp with time zone",
            "date",
            "unknown",
            "unknown",
            "uuid"
        ])]
    );
    for (sql, state) in [
        ("SELECT pg_typeof(100::numeric(3,1))", "22003"),
        ("SELECT pg_typeof(extract(hour from current_date))", "0A000"),
        ("SELECT pg_typeof('x'::integer)", "22P02"),
        ("SELECT pg_typeof(2147483647 + 1)", "22003"),
        ("SELECT pg_typeof(pg_typeof(1/0))", "22012"),
        ("SELECT pg_typeof(1/0) IS NULL", "22012"),
    ] {
        assert_sqlstate(&mut s, sql, state);
    }
}

fn create_typeof_rows(s: &mut SqlSession) {
    run(s, "CREATE TABLE tt (id int primary key, x int, d date)");
    run(
        s,
        "INSERT INTO tt VALUES (1, 0, '2024-01-01'), (2, 5, NULL)",
    );
}

#[test]
fn evaluates_row_arguments_only_for_selected_rows_and_case_arms() {
    let a = Admin::create(common::fresh("pgparity-typeof-rows"), Limits::default()).unwrap();
    let mut s = a.sql_session();
    create_typeof_rows(&mut s);
    assert_sqlstate(
        &mut s,
        "SELECT id, pg_typeof(10 / x) FROM tt ORDER BY id",
        "22012",
    );
    let integer = Value::Text("integer".into());
    assert_eq!(
        run(
            &mut s,
            "SELECT id, pg_typeof(10 / x) FROM tt WHERE x <> 0 ORDER BY id"
        ),
        vec![vec![Value::Int(2), integer.clone()]]
    );
    let constant = "SELECT pg_typeof(1/0) FROM tt WHERE false";
    let prepared = s.prepare(constant).unwrap();
    let error = s.validate_immutable_constants(&prepared).unwrap_err();
    assert_eq!(giggles_core::sqlstate(&error), "22012", "{error:?}");
    // Direct execution follows the same folding rules as the bare expression.
    let mut execute = |sql| {
        s.execute(sql, &[], None)
            .map(|result| result.rows)
            .map_err(|error| giggles_core::sqlstate(&error))
    };
    assert_eq!(execute(constant), execute("SELECT 1/0 FROM tt WHERE false"));
    let row_dependent = "SELECT pg_typeof(10/x) FROM tt WHERE false";
    let prepared = s.prepare(row_dependent).unwrap();
    s.validate_immutable_constants(&prepared).unwrap();
    assert!(run(&mut s, row_dependent).is_empty());
    assert_eq!(
        run(
            &mut s,
            "SELECT CASE WHEN x = 0 THEN 'zero' ELSE pg_typeof(10/x)::text END FROM tt ORDER BY id"
        ),
        vec![texts(&["zero"]), vec![integer]]
    );
    assert_eq!(
        run(
            &mut s,
            "SELECT id, pg_typeof(d), pg_typeof(x::bigint), pg_typeof(x || 'a') FROM tt ORDER BY id"
        ),
        [1, 2].map(|id| {
            let mut row = vec![Value::Int(id)];
            row.extend(texts(&["date", "bigint", "text"]));
            row
        })
    );
}

#[test]
fn evaluates_prepared_arguments_with_supplied_parameters() {
    let a = Admin::create(
        common::fresh("pgparity-typeof-parameter"),
        Limits::default(),
    )
    .unwrap();
    let mut s = a.sql_session();
    let prepared = s
        .prepare("SELECT pg_typeof(10 / $1)")
        .unwrap()
        .with_parameter_types(&[Some(DataType::Integer)])
        .unwrap();
    let result = s
        .execute_prepared(&prepared, &[Value::Int(2)], None)
        .unwrap();
    assert_eq!(result.rows[0].values, texts(&["integer"]));
    let error = s
        .execute_prepared(&prepared, &[Value::Int(0)], None)
        .unwrap_err();
    assert_eq!(giggles_core::sqlstate(&error), "22012", "{error:?}");
}

#[test]
fn evaluates_sequences_as_often_as_the_bare_expression() {
    let a = Admin::create(
        common::fresh("pgparity-typeof-sequences"),
        Limits::default(),
    )
    .unwrap();
    let mut s = a.sql_session();
    create_typeof_rows(&mut s);
    run(&mut s, "CREATE SEQUENCE ts");
    assert_eq!(
        run(&mut s, "SELECT pg_typeof(nextval('ts'))"),
        vec![texts(&["bigint"])]
    );
    assert_eq!(
        run(&mut s, "SELECT currval('ts')"),
        vec![vec![Value::Int(1)]]
    );

    // A sort key can evaluate a volatile expression separately from projection.
    // The wrapper must advance the sequence exactly as often as the bare call.
    run(&mut s, "CREATE SEQUENCE bare");
    run(&mut s, "SELECT nextval('bare')");
    run(&mut s, "SELECT nextval('bare') FROM tt ORDER BY 1");
    assert_eq!(
        run(&mut s, "SELECT pg_typeof(nextval('ts')) FROM tt ORDER BY 1"),
        vec![texts(&["bigint"]); 2]
    );
    assert_eq!(
        run(&mut s, "SELECT currval('ts')"),
        run(&mut s, "SELECT currval('bare')")
    );

    run(
        &mut s,
        "SELECT id, nextval('bare') FROM tt ORDER BY nextval('bare')",
    );
    assert_eq!(
        run(
            &mut s,
            "SELECT id, pg_typeof(nextval('ts')) FROM tt ORDER BY pg_typeof(nextval('ts'))"
        ),
        [1, 2].map(|id| vec![Value::Int(id), Value::Text("bigint".into())])
    );
    assert_eq!(
        run(&mut s, "SELECT currval('ts')"),
        run(&mut s, "SELECT currval('bare')")
    );
}
