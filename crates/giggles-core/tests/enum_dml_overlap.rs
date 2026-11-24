mod common;

use giggles_core::{
    Admin, Engine, Limits, SqlSession,
    relational::{SqlResult, Value, take_sql_diagnostic_hint, take_sql_error_message},
    sqlstate,
};

fn run(session: &mut SqlSession, sql: &str) -> SqlResult {
    session
        .execute(sql, &[], None)
        .unwrap_or_else(|error| panic!("{sql}: {error:?} ({:?})", take_sql_error_message()))
}

fn rows(session: &mut SqlSession, sql: &str) -> Vec<Vec<Value>> {
    run(session, sql)
        .rows
        .into_iter()
        .map(|row| row.values)
        .collect()
}

fn text(value: &str) -> Value {
    Value::Text(value.into())
}

fn reject(session: &mut SqlSession, sql: &str, column: &str, target: &str, source: &str) {
    let error = session.execute(sql, &[], None).expect_err(sql);
    assert_eq!(sqlstate(&error), "42804", "{sql}");
    assert_eq!(
        take_sql_error_message().unwrap(),
        format!("column \"{column}\" is of type {target} but expression is of type {source}"),
        "{sql}",
    );
    assert_eq!(
        take_sql_diagnostic_hint().as_deref(),
        Some("You will need to rewrite or cast the expression."),
        "{sql}",
    );
}

fn enum_dml_overlap(engine: Engine) {
    let path = common::fresh("enum-dml-overlap");
    let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
    let mut session = admin.sql_session();
    for sql in [
        "CREATE TYPE mood AS ENUM ('z', 'a', 'm')",
        "CREATE TABLE enum_overlap (id integer PRIMARY KEY, feeling mood, feelings mood[])",
        "CREATE TABLE enum_source (id integer PRIMARY KEY, target_id integer, priority integer, feeling mood)",
    ] {
        run(&mut session, sql);
    }

    assert_eq!(
        run(
            &mut session,
            "
        INSERT INTO enum_overlap
        SELECT 1, v.feeling, v.feelings
        FROM (VALUES ('a'::mood, ARRAY['a']::mood[])) AS v(feeling, feelings)"
        )
        .affected,
        1
    );
    for (column, literal, destination_type) in
        [("feeling", "a", "mood"), ("feelings", "{a}", "mood[]")]
    {
        let sql = format!(
            "INSERT INTO enum_overlap (id, {column}) SELECT 2, v.x FROM (VALUES ('{literal}')) AS v(x)"
        );
        reject(&mut session, &sql, column, destination_type, "text");
    }
    run(
        &mut session,
        "INSERT INTO enum_overlap VALUES (2, 'a', '{a}'), (3, 'a', '{a}')",
    );
    assert_eq!(
        rows(
            &mut session,
            "SELECT id, feeling, feelings FROM enum_overlap ORDER BY id"
        ),
        [1, 2, 3].map(|id| vec![Value::Int(id), text("a"), text("{a}")])
    );

    run(
        &mut session,
        "INSERT INTO enum_source VALUES (1, 1, 2, 'm'), (2, 1, 1, 'z'), (3, 2, 1, 'a')",
    );
    assert_eq!(
        run(
            &mut session,
            "
        UPDATE enum_overlap AS t SET feeling = (
            SELECT s.feeling FROM enum_source AS s
            WHERE s.target_id = t.id ORDER BY s.feeling, s.priority LIMIT 1
        ) WHERE t.id <= 2"
        )
        .affected,
        2
    );
    assert_eq!(
        rows(
            &mut session,
            "SELECT id, feeling FROM enum_overlap ORDER BY id"
        ),
        [(1, "z"), (2, "a"), (3, "a")].map(|(id, value)| vec![Value::Int(id), text(value)])
    );

    assert_eq!(
        run(
            &mut session,
            "
        WITH chosen AS (SELECT feeling FROM enum_source WHERE target_id = 1 AND priority = 2)
        UPDATE enum_overlap SET feeling = (SELECT feeling FROM chosen) WHERE id = 3"
        )
        .affected,
        1
    );
    assert_eq!(
        rows(
            &mut session,
            "SELECT id, feeling FROM enum_overlap ORDER BY id"
        ),
        [(1, "z"), (2, "a"), (3, "m")].map(|(id, value)| vec![Value::Int(id), text(value)])
    );

    assert_eq!(
        rows(
            &mut session,
            "
        SELECT json_agg(feeling ORDER BY feeling), jsonb_agg(feeling ORDER BY feeling)
        FROM enum_overlap"
        ),
        [vec![text(r#"["z", "a", "m"]"#), text(r#"["z", "a", "m"]"#)]]
    );
    assert_eq!(
        rows(
            &mut session,
            "
        SELECT json_agg(feeling ORDER BY feeling DESC), jsonb_agg(feeling ORDER BY feeling DESC)
        FROM enum_overlap GROUP BY (id > 0)"
        ),
        [vec![text(r#"["m", "a", "z"]"#), text(r#"["m", "a", "z"]"#)]]
    );
    assert_eq!(
        rows(
            &mut session,
            "SELECT pg_typeof(feeling), pg_typeof(feelings) FROM enum_overlap WHERE id = 1"
        ),
        [vec![text("mood"), text("mood[]")]]
    );

    assert_eq!(
        rows(
            &mut session,
            "
        SELECT pg_typeof(item), item FROM (VALUES ('z'::mood), ('a')) v(item) ORDER BY item"
        ),
        [vec![text("mood"), text("z")], vec![text("mood"), text("a")]]
    );
    assert_eq!(rows(&mut session, "
        SELECT pg_typeof(item), item FROM (VALUES (ARRAY['z']::mood[]), ('{a}')) v(item) ORDER BY item"),
        [vec![text("mood[]"), text("{z}")], vec![text("mood[]"), text("{a}")]]);
    assert_eq!(
        rows(
            &mut session,
            "
        SELECT pg_typeof(json_agg(feeling ORDER BY feeling)),
            pg_typeof(jsonb_agg(feeling ORDER BY feeling)) FROM enum_overlap"
        ),
        [vec![text("json"), text("jsonb")]]
    );
    assert_eq!(
        rows(
            &mut session,
            "
        SELECT t.id, pg_typeof((SELECT s.feeling FROM enum_source s
            WHERE s.target_id = t.id ORDER BY s.feeling LIMIT 1))
        FROM enum_overlap t ORDER BY t.id"
        ),
        [1, 2, 3].map(|id| vec![Value::Int(id), text("mood")])
    );
    assert_eq!(
        rows(
            &mut session,
            "
        SELECT t.id, (SELECT max(s.priority) FROM enum_source s WHERE s.feeling = t.feeling)
        FROM enum_overlap t ORDER BY t.id"
        ),
        [(1, 1), (2, 1), (3, 2)].map(|(id, priority)| vec![Value::Int(id), Value::Int(priority)])
    );
    assert_eq!(
        rows(
            &mut session,
            r#"
        SELECT jsonb_set(jsonb_agg(feeling ORDER BY feeling), '{0}', '"other"'::jsonb)
        FROM enum_overlap"#
        ),
        [vec![text(r#"["other", "a", "m"]"#)]]
    );
    run(
        &mut session,
        "CREATE TYPE other_mood AS ENUM ('z', 'a', 'm')",
    );
    reject(
        &mut session,
        "
        INSERT INTO enum_overlap (id, feeling)
        SELECT 4, v.feeling FROM (VALUES ('a'::other_mood)) v(feeling)",
        "feeling",
        "mood",
        "other_mood",
    );
    reject(
        &mut session,
        "UPDATE enum_overlap SET feeling = (SELECT 'a'::text) WHERE id = 3",
        "feeling",
        "mood",
        "text",
    );

    let insert = session
        .prepare(
            "
        INSERT INTO enum_overlap (id, feeling)
        SELECT 4, $1 FROM (VALUES (1)) v(x)",
        )
        .unwrap();
    let (_, parameter_types) = session.describe_with_enum_parameters(&insert).unwrap();
    let sample = session.prepare("SELECT 'a'::mood").unwrap();
    assert_eq!(
        parameter_types[&0].oid(),
        session.describe(&sample).unwrap().type_oids[0]
    );

    let update = "UPDATE enum_overlap SET feeling = (SELECT $1) WHERE id = 4";
    let error = session
        .prepare(update)
        .and_then(|plan| session.describe(&plan))
        .expect_err(update);
    assert_eq!(sqlstate(&error), "42804");
    assert_eq!(
        take_sql_error_message().as_deref(),
        Some(r#"column "feeling" is of type mood but expression is of type text"#)
    );
}

#[test]
fn legacy_enum_dml_overlap() {
    enum_dml_overlap(Engine::Legacy);
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn v2_enum_dml_overlap() {
    enum_dml_overlap(Engine::V2);
}
