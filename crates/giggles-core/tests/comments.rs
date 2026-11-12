mod common;
use giggles_core::{Admin, CoreError, Limits, relational::SqlError};
#[cfg(feature = "engine-rocksdb")]
use giggles_core::Engine;

#[test]
fn legacy_refuses_comments_without_mutation() {
    let admin = Admin::create(common::fresh("comments-legacy"), Limits::default()).unwrap();
    let mut s = admin.sql_session();
    s.execute("CREATE TABLE t(id int)", &[], None).unwrap();
    for sql in ["COMMENT ON TABLE t IS 'x'", "COMMENT ON COLUMN t.id IS NULL", "COMMENT ON VIEW t IS 'x'"] {
        assert_eq!(s.execute(sql, &[], None), Err(CoreError::Sql(SqlError::Unsupported)));
    }
}
#[cfg(feature = "engine-rocksdb")]
#[test]
fn v2_comment_forms_errors_and_reopen() {
    let path = common::fresh("comments-v2");
    let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
    let mut s = admin.sql_session();
    for sql in ["CREATE SCHEMA s", "CREATE TABLE s.t(id int, \"C\" text)",
        "COMMENT ON TABLE s.t IS 'é''quoted'", "COMMENT ON COLUMN s.t.\"C\" IS 'column'",
        "BEGIN", "COMMENT ON TABLE s.t IS NULL", "ROLLBACK", "COMMENT ON COLUMN s.t.id IS ''"] {
        s.execute(sql, &[], None).unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    }
    for (sql, error) in [("COMMENT ON TABLE absent IS 'x'".into(), SqlError::UnknownTable),
        ("COMMENT ON COLUMN s.t.absent IS 'x'".into(), SqlError::UnknownColumn),
        (format!("COMMENT ON TABLE s.t IS '{}'", "é".repeat(32769)), SqlError::Limit)] {
        assert_eq!(s.execute(&sql, &[], None), Err(CoreError::Sql(error)));
    }
    drop(s); drop(admin);
    let admin = Admin::open(&path, Limits::default()).unwrap();
    admin.sql_session().execute("DROP TABLE s.t", &[], None).unwrap();
}
