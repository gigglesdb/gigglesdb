mod common;
use common::fresh;
use giggles_core::{
    Admin, CoreError, Limits, SqlSession,
    relational::{SqlError, Value},
};

fn run(session: &mut SqlSession, sql: &str) -> Vec<Vec<Value>> {
    session
        .execute(sql, &[], None)
        .expect(sql)
        .rows
        .into_iter()
        .map(|row| row.values)
        .collect()
}

#[test]
fn numeric_assignment_uses_text_before_character_typmods() {
    let admin = Admin::create(fresh("e2-assignment-numeric-char"), Limits::default()).unwrap();
    let mut session = admin.sql_session();
    run(
        &mut session,
        "CREATE TABLE t (id int PRIMARY KEY, v varchar(12), c char(12))",
    );
    // PostgreSQL 18.6 output: numeric retains scale; float4 uses its own shortest text.
    for (id, expression, text) in [
        (1, "-12::smallint", "-12"),
        (2, "12345::int", "12345"),
        (3, "2147483648::bigint", "2147483648"),
        (4, "1.50::numeric", "1.50"),
        (5, "0.1::real", "0.1"),
        (6, "0.1::double precision", "0.1"),
    ] {
        run(
            &mut session,
            &format!("INSERT INTO t VALUES ({id}, {expression}, {expression})"),
        );
        assert_eq!(
            run(&mut session, &format!("SELECT v,c FROM t WHERE id={id}")),
            vec![vec![
                Value::Text(text.into()),
                Value::Text(format!("{text:<12}"))
            ]]
        );
    }
    run(
        &mut session,
        "CREATE TABLE narrow (id int PRIMARY KEY, v varchar(3), c char(3))",
    );
    for sql in [
        "INSERT INTO narrow (id,v) VALUES (1,1234::int)",
        "INSERT INTO narrow (id,c) VALUES (2,1.50::numeric)",
    ] {
        assert_eq!(
            session.execute(sql, &[], None),
            Err(CoreError::Sql(SqlError::StringDataRightTruncation)),
            "{sql}"
        );
    }
}
