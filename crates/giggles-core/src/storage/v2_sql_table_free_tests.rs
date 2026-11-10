use super::*;
use relational::Value as V;

fn check(sql: &str, expected: Result<Vec<V>, SqlError>) {
    check_catalog(sql, expected, "CREATE SCHEMA cursor_context");
}

fn check_catalog(sql: &str, expected: Result<Vec<V>, SqlError>, ddl: &str) {
    let f = Fixture::new();
    let mut session = f.admin().sql_session();
    session.execute(ddl, &[], None).unwrap();
    let p = session.prepare(sql).unwrap();
    session.describe(&p).unwrap();
    let clock = relational::StatementClock {
        unavailable: None,
        transaction_us: 0,
        statement_us: 0,
        zone: None,
        wall: None,
    };
    let result = session
        .open_cursor_at(&p, &[], &clock)
        .and_then(|mut cursor| {
            let mut rows = Vec::new();
            while !cursor.is_exhausted() {
                rows.extend(cursor.fetch(16, 4096)?);
            }
            Ok(rows)
        });
    match expected {
        Ok(values) => {
            let result = result.unwrap_or_else(|error| panic!("{sql}: {error:?}"));
            assert_eq!(result.len(), 1, "{sql}");
            assert_eq!(result[0].values, values, "{sql}");
        }
        Err(error) => assert_eq!(result, Err(CoreError::Sql(error)), "{sql}"),
    }
}

#[test]
fn table_free_scalars() {
    check(
        "SELECT lower('ABC') AS l, abs(-7) AS a",
        Ok(vec![V::Text("abc".into()), V::Int(7)]),
    );
}

#[test]
fn no_from_aggregate() {
    check("SELECT count(*) AS c", Ok(vec![V::Int(1)]));
}

#[test]
fn public_schema_routine() {
    check(
        "SELECT pg_catalog.quote_ident('a b') AS q",
        Ok(vec![V::Text("\"a b\"".into())]),
    );
}

#[test]
fn explicit_bytea_cast() {
    check(
        r"SELECT position('\x02'::bytea IN '\x010203'::bytea) AS p",
        Ok(vec![V::Int(2)]),
    );
}

#[test]
fn immutable_overflow_error() {
    check("SELECT abs(-2147483648)", Err(SqlError::Overflow));
}

#[test]
fn division_by_zero() {
    check("SELECT 1/0", Err(SqlError::DivisionByZero));
}

#[test]
fn select_literals() {
    check(
        "SELECT 1 AS a, 'x' AS b",
        Ok(vec![V::Int(1), V::Text("x".into())]),
    );
    check("SELECT NULL::int AS n", Ok(vec![V::Null]));
}

#[test]
fn enum_labels_do_not_restrict_cursor_catalog() {
    check_catalog(
        "SELECT 1 AS a",
        Ok(vec![V::Int(1)]),
        "CREATE TYPE cursor_enum AS ENUM ('a', 'b')",
    );
}
