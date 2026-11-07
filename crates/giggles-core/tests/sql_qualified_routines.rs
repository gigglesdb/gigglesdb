//! E939: PostgreSQL 18 reports 42883 for public calls for every role. GigglesDB
//! server authorization failures report 42501 to avoid revealing routine existence;
//! authorized calls report 42883. Core has no authorization layer and always
//! reports UndefinedFunction at binding for these calls.
mod common;
use common::*;
use giggles_core::{
    relational::{Prepared, SqlError as E, Value as V},
    *,
};

fn db() -> (Admin, SqlSession) {
    let a = Admin::create(fresh("sql-qualified-routines"), Limits::default()).unwrap();
    let mut s = a.sql_session();
    s.execute(
        "CREATE TABLE f (id BIGINT PRIMARY KEY, txt TEXT)",
        &[],
        None,
    )
    .unwrap();
    (a, s)
}

fn error(s: &mut SqlSession, sql: &str, expected: E) {
    assert_eq!(
        s.execute(sql, &[], None),
        Err(CoreError::Sql(expected)),
        "{sql}"
    );
}

#[test]
fn public_calls_parse_then_fail_analysis() {
    let (_a, mut s) = db();
    for call in [
        "public.quote_ident('a')",
        "public.abs(1)",
        "public.lower('a')",
        "public.now()",
        "public.count(1)",
        "public.nosuchfn(1)",
        "\"public\".quote_ident('a')",
        "PUBLIC.QUOTE_IDENT('a')",
        "public.\"QUOTE_IDENT\"('a')",
        "public.quote_ident()",
        "public.quote_ident('a', 'b')",
        "\"public.abs\"(1)",
        "\"pg_catalog.abs\"(1)",
    ] {
        let sql = format!("SELECT {call}");
        let p = s.prepare(&sql).unwrap_or_else(|e| panic!("{sql}: {e:?}"));
        assert_eq!(
            s.describe(&p).unwrap_err(),
            CoreError::Sql(E::UndefinedFunction),
            "{sql}"
        );
        assert_eq!(
            s.execute_prepared(&p, &[], None),
            Err(CoreError::Sql(E::UndefinedFunction)),
            "{sql}"
        );
        // Diagnostic admission must not turn the binding error into Unsupported.
        assert!(
            !Prepared::rejects_unsupported_table_free_scalar(&sql),
            "{sql}"
        );
        assert!(p.table().is_some(), "{sql}");
    }
}

#[test]
fn targeted_public_calls_reach_analysis_without_catalog_rejection() {
    let (_a, mut s) = db();
    for sql in [
        "SELECT upper(public.lower('a'))",
        "SELECT CAST(public.abs(1) AS TEXT)",
        "SELECT 1 WHERE public.abs(1) = 1",
        "SELECT 1 ORDER BY public.abs(1)",
        "SELECT count(public.abs(1))",
        "SELECT sum(public.count(*))",
        "SELECT public.count(sum(1))",
    ] {
        let p = s.prepare(sql).unwrap();
        assert!(p.table().is_some(), "{sql}");
        assert!(
            !Prepared::rejects_unsupported_table_free_scalar(sql),
            "{sql}"
        );
        error(&mut s, sql, E::UndefinedFunction);
    }
    // PG18.6: built-in aggregates evaluate the implicit row (DD-0068 F11).
    for sql in ["SELECT count(1)", "SELECT sum(1)"] {
        assert!(s.prepare(sql).unwrap().table().is_none(), "{sql}");
    }
    assert!(
        s.prepare("SELECT nosuchfn(1)")
            .unwrap()
            .table()
            .is_some()
    );
    assert!(!Prepared::rejects_unsupported_table_free_scalar(
        "SELECT nosuchfn(1)"
    ));
}

#[test]
fn public_calls_resolve_arguments_before_routine_lookup() {
    let (_a, mut s) = db();
    let builtin = "SELECT upper(nosuchcolumn)";
    let public = "SELECT public.abs(nosuchcolumn)";
    assert!(s.prepare(builtin).unwrap().table().is_some());
    assert!(s.prepare(public).unwrap().table().is_some());
    let expected = s.execute(builtin, &[], None).unwrap_err();
    assert_eq!(s.execute(public, &[], None).unwrap_err(), expected);

    for (builtin, public) in [
        (
            "SELECT id FROM f WHERE upper(nosuchcol) = 'a'",
            "SELECT id FROM f WHERE public.abs(nosuchcol) = 1",
        ),
        (
            "SELECT upper(upper(nosuchcol)) FROM f",
            "SELECT public.abs(upper(nosuchcol)) FROM f",
        ),
        (
            "SELECT count(upper(nosuchcol)) FROM f",
            "SELECT count(public.abs(nosuchcol)) FROM f",
        ),
        (
            "SELECT abs(CAST('bad' AS INTEGER))",
            "SELECT public.abs(CAST('bad' AS INTEGER))",
        ),
    ] {
        let expected = s.execute(builtin, &[], None).unwrap_err();
        assert_eq!(
            s.execute(public, &[], None).unwrap_err(),
            expected,
            "{public}"
        );
    }

    // Argument inference must leave $1 unresolved until failed routine lookup.
    let sql = "SELECT public.abs($1)";
    let p = s.prepare(sql).unwrap();
    assert!(p.table().is_some());
    assert_eq!(
        s.describe(&p).unwrap_err(),
        CoreError::Sql(E::UndefinedFunction)
    );
    assert_eq!(
        s.execute_prepared(&p, &[V::Int(1)], None),
        Err(CoreError::Sql(E::UndefinedFunction))
    );

    // An argument aggregate must not be bound in an artificial scalar context.
    for sql in [
        "SELECT public.count(sum(1))",
        "SELECT public.count(sum(1)) FROM f",
    ] {
        let p = s.prepare(sql).unwrap();
        assert_eq!(
            s.describe(&p).unwrap_err(),
            CoreError::Sql(E::UndefinedFunction)
        );
        error(&mut s, sql, E::UndefinedFunction);
    }
}

#[test]
fn public_aggregate_and_syntax_names_are_ordinary_calls() {
    let (_a, mut s) = db();
    for name in [
        "count",
        "sum",
        "min",
        "max",
        "avg",
        "coalesce",
        "position",
        "trim",
        "extract",
        "overlay",
        "substring",
    ] {
        for args in ["", "1", "1, 2", "*"] {
            let sql = format!("SELECT public.{name}({args})");
            s.prepare(&sql).unwrap_or_else(|e| panic!("{sql}: {e:?}"));
            error(&mut s, &sql, E::UndefinedFunction);
        }
    }
}

#[test]
fn public_calls_in_scalar_and_aggregate_contexts() {
    let (_a, mut s) = db();
    for sql in [
        "SELECT upper(public.lower('a'))",
        "SELECT CAST(public.abs(1) AS TEXT)",
        "SELECT public.abs(1)::TEXT",
        "SELECT count(public.abs(1)) FROM f",
        "SELECT sum(public.count(*)) FROM f",
        "SELECT public.count(sum(1)) FROM f",
        "SELECT id FROM f WHERE public.quote_ident(txt) = 'a'",
        "SELECT id FROM f WHERE public.abs(id) IS NULL",
        "SELECT id FROM f ORDER BY public.quote_ident(txt)",
        "SELECT public.quote_ident(txt) FROM f LIMIT 0",
        "SELECT 1 WHERE public.abs(1) = 1",
        "SELECT 1 ORDER BY public.abs(1)",
    ] {
        s.prepare(sql).unwrap_or_else(|e| panic!("{sql}: {e:?}"));
        error(&mut s, sql, E::UndefinedFunction);
    }
}

#[test]
fn public_calls_defer_to_syntax_and_source_resolution() {
    let (_a, mut s) = db();
    for sql in [
        "SELECT public.quote_ident('a' FROM",
        "SELECT public.quote_ident('a'), public.abs(1 FROM f",
        "SELECT public.quote_ident('a') FROM f WHERE (",
        "SELECT public.count(*, 1)",
        "SELECT public.abs(1,)",
        "SELECT public.abs(,1)",
        "SELECT public.abs(1,,2)",
    ] {
        error(&mut s, sql, E::Syntax);
    }
    error(
        &mut s,
        "SELECT public.quote_ident('a') FROM nosuchtable",
        E::UnknownTable,
    );
}

#[test]
fn other_routine_resolution_is_unchanged() {
    let (_a, mut s) = db();
    assert_eq!(
        s.execute("SELECT pg_catalog.quote_ident('a')", &[], None)
            .unwrap()
            .rows[0]
            .values,
        vec![V::Text("a".into())]
    );
    for (call, expected) in [
        ("pg_catalog.nosuchfn(1)", E::UndefinedFunction),
        ("nosuchfn(1)", E::UndefinedFunction),
        ("information_schema.quote_ident('a')", E::UndefinedFunction),
        ("\"PUBLIC\".quote_ident('a')", E::InvalidSchema),
        ("nosuchschema.abs(1)", E::InvalidSchema),
        ("pg_catalog.\"public.abs\"(1)", E::UndefinedFunction),
        ("pg_catalog.coalesce(1, 2)", E::UndefinedFunction),
    ] {
        error(&mut s, &format!("SELECT {call}"), expected);
    }
}
