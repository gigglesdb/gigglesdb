mod common;
use common::*;
use giggles_core::{
    relational::{Prepared, SqlError as E, Value as V},
    *,
};
fn db() -> (Admin, SqlSession) {
    let a = Admin::create(fresh("sql-undefined-routine-codes"), Limits::default()).unwrap();
    let mut s = a.sql_session();
    s.execute(
        "CREATE TABLE f (id BIGINT PRIMARY KEY, txt TEXT)",
        &[],
        None,
    )
    .unwrap();
    (a, s)
}
fn cases() -> Vec<(&'static str, E)> {
    vec![
        ("SELECT nosuchfn('a')", E::UndefinedFunction),
        ("SELECT nosuchfn()", E::UndefinedFunction),
        ("SELECT frobnicate(1,2)", E::UndefinedFunction),
        ("SELECT abort(1)", E::UndefinedFunction),
        ("SELECT \"LOWER\"('a')", E::UndefinedFunction),
        ("SELECT \"Lower\"('a')", E::UndefinedFunction),
        ("SELECT \"coalesce\"(1,2)", E::UndefinedFunction),
        ("SELECT \"nullif\"(1,2)", E::UndefinedFunction),
        ("SELECT \"row\"(1,2)", E::UndefinedFunction),
        ("SELECT \"trim\"('a')", E::UndefinedFunction),
        ("SELECT \"a.b\"(1)", E::UndefinedFunction),
        ("SELECT nosuchfn(*)", E::UndefinedFunction),
        ("SELECT pg_catalog.nosuchfn('a')", E::UndefinedFunction),
        ("SELECT pg_catalog.\"LOWER\"('a')", E::UndefinedFunction),
        ("SELECT pg_catalog.coalesce(1,2)", E::UndefinedFunction),
        ("SELECT pg_catalog.nullif(1,2)", E::UndefinedFunction),
        ("SELECT pg_catalog.trim('a')", E::UndefinedFunction),
        ("SELECT \"pg_catalog\".nosuchfn(1)", E::UndefinedFunction),
        (
            "SELECT information_schema.nosuchfn('a')",
            E::UndefinedFunction,
        ),
        ("SELECT information_schema.lower('a')", E::UndefinedFunction),
        ("SELECT pg_toast.lower('a')", E::UndefinedFunction),
        ("SELECT public.nosuchfn('a')", E::UndefinedFunction),
        ("SELECT Public.nosuchfn(1)", E::UndefinedFunction),
        (
            "SELECT aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa(1)",
            E::UndefinedFunction,
        ),
        ("SELECT 1 WHERE nosuchfn(1)=1", E::UndefinedFunction),
        ("SELECT nosuchfn(id) FROM f", E::UndefinedFunction),
        ("SELECT nosuchschema.lower('a')", E::InvalidSchema),
        ("SELECT nosuchschema.nosuchfn('a')", E::InvalidSchema),
        ("SELECT nosuchschema.lower(1/0)", E::InvalidSchema),
        ("SELECT nosuchschema.count(*)", E::InvalidSchema),
        ("SELECT \"PG_CATALOG\".lower('a')", E::InvalidSchema),
        ("SELECT \"PUBLIC\".nosuchfn(1)", E::InvalidSchema),
        (
            "SELECT \"INFORMATION_SCHEMA\".nosuchfn(1)",
            E::InvalidSchema,
        ),
        ("SELECT pg_temp.lower('a')", E::InvalidSchema),
        ("SELECT nosuchschema.f(id) FROM f", E::InvalidSchema),
        (
            "SELECT nosuchschema.lower(nosuchcol) FROM f",
            E::UnknownColumn,
        ),
        ("SELECT nosuchfn(nosuchcol) FROM f", E::UnknownColumn),
        (
            "SELECT nosuchschema.lower(a) FROM nosuchtable",
            E::UnknownTable,
        ),
    ]
}
fn error(s: &mut SqlSession, sql: &str, expected: E) {
    let p = s
        .prepare(sql)
        .unwrap_or_else(|e| panic!("{sql}: prepare {e:?}"));
    assert_eq!(
        s.describe(&p).unwrap_err(),
        CoreError::Sql(expected.clone()),
        "{sql}: describe"
    );
    assert_eq!(
        s.execute_prepared(&p, &[], None),
        Err(CoreError::Sql(expected.clone())),
        "{sql}: prepared"
    );
    assert_eq!(
        s.execute(sql, &[], None),
        Err(CoreError::Sql(expected.clone())),
        "{sql}: plain"
    );
    assert!(
        !Prepared::rejects_unsupported_table_free_scalar(sql),
        "{sql}: admission"
    );
}
#[test]
fn unresolved_routines_preserve_error_codes_and_analysis_order() {
    let (_a, mut s) = db();
    for (sql, expected) in cases() {
        error(&mut s, sql, expected);
    }
}
#[test]
fn catalog_and_syntax_names_keep_existing_dispatch() {
    let (_a, mut s) = db();
    for sql in [
        "SELECT LOWER('a')",
        "SELECT \"lower\"('a')",
        "SELECT PG_CATALOG.lower('a')",
        "SELECT format('%s','a')",
    ] {
        let p = s.prepare(sql).unwrap();
        assert!(s.describe(&p).is_ok(), "{sql}");
        for result in [s.execute(sql, &[], None), s.execute_prepared(&p, &[], None)] {
            assert_eq!(
                result.unwrap().rows[0].values,
                vec![V::Text("a".into())],
                "{sql}"
            );
        }
    }
    for sql in [
        "SELECT to_char(1,'9')",
        "SELECT jsonb_path_query_first('{}','$')",
        "SELECT row(1,2)",
        "SELECT information_schema._pg_expandarray(1)",
    ] {
        assert_eq!(
            s.execute(sql, &[], None),
            Err(CoreError::Sql(E::Unsupported)),
            "{sql}"
        );
    }
    assert_eq!(
        s.execute("SELECT nullif(1,2)", &[], None).unwrap().rows[0].values,
        vec![V::Int(1)]
    );
    // PostgreSQL 18.6: pg_typeof(1) is integer (GD-0079 1A; DD-0068 notice).
    assert_eq!(
        s.execute("SELECT pg_typeof(1)", &[], None).unwrap().rows[0].values,
        vec![V::Text("integer".into())]
    );
}
