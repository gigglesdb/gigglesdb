//! Flat-array codec rejects unsupported shapes without unwinding.
mod common;
use common::fresh;
use giggles_core::{Admin, Limits, relational::*};
#[test]
fn malformed_array_sql_is_a_clean_error() {
    let admin = Admin::create(fresh("array-validation"), Limits::default()).unwrap();
    let mut s = admin.sql_session();
    let error = s.execute(r#"SELECT '{a,b'::text[] COLLATE "und-x-icu" < '{a}'::text[]"#, &[], None).unwrap_err();
    assert_eq!(giggles_core::sqlstate(&error), "22P02");
    assert_eq!(take_sql_error_message().as_deref(), Some("malformed array literal: \"{a,b\""));
    // PG accepts these shapes; our flat one-based codec currently rejects them.
    for sql in [
        r#"SELECT '[2:3]={a,b}'::text[] COLLATE "und-x-icu" < '{a,b}'::text[]"#,
        r#"SELECT ARRAY[['a','b'],['c','d']] COLLATE "und-x-icu" < ARRAY[['a','c'],['c','d']]"#,
    ] {
        let error = s.execute(sql, &[], None).unwrap_err();
        assert!(matches!(error, giggles_core::CoreError::Sql(SqlError::InvalidTextRepresentation | SqlError::Unsupported)), "{sql}: {error:?}");
    }
}

#[test]
fn swallowed_bind_probe_does_not_leak_array_diagnostic() {
    let admin = Admin::create(fresh("array-probe-diagnostic"), Limits::default()).unwrap();
    let mut s = admin.sql_session();
    let error = s.execute("SELECT strpos('{a,b'::text[], decode('00','hex'))", &[], None).unwrap_err();
    assert_eq!(giggles_core::sqlstate(&error), "22P02");
    assert_eq!(take_sql_error_message().as_deref(), Some("malformed array literal: \"{a,b\""));
    let error = s.execute("SELECT 1/0", &[], None).unwrap_err();
    assert_eq!(giggles_core::sqlstate(&error), "22012");
    // Field-less errors use their canonical primary message when no override is set.
    assert_eq!(take_sql_error_message().as_deref().unwrap_or("division by zero"), "division by zero");
}
