mod common;
use giggles_core::{Admin, CoreError, Limits, relational::SqlError};

#[test]
fn drop_options_keep_if_exists_separate() {
    let admin = Admin::create(common::fresh("alter-drop-options"), Limits::default()).unwrap();
    let mut s = admin.sql_session();
    for sql in ["CREATE TABLE t(id int PRIMARY KEY, a int, b int)",
        "ALTER TABLE t DROP COLUMN IF EXISTS missing RESTRICT",
        "ALTER TABLE t DROP COLUMN a RESTRICT", "ALTER TABLE t DROP COLUMN b CASCADE"] {
        s.execute(sql, &[], None).unwrap();
    }
    assert_eq!(s.execute("ALTER TABLE t DROP COLUMN missing CASCADE", &[], None),
        Err(CoreError::Sql(SqlError::UnknownColumn)));
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn column_views_restrict_cascade_rollback_and_reopen() {
    let path = common::fresh("alter-column-views");
    let admin = Admin::create_engine(&path, Limits::default(), giggles_core::Engine::V2).unwrap();
    let mut s = admin.sql_session();
    for sql in ["CREATE TABLE t(id int PRIMARY KEY, c int, keep int, spare int)",
        "INSERT INTO t VALUES(1,2,3,4)", "CREATE VIEW v AS SELECT c AS alias, keep FROM t",
        "CREATE VIEW vv AS SELECT keep FROM v", "CREATE VIEW safe AS SELECT keep FROM t",
        "COMMENT ON VIEW v IS 'dependent'", "COMMENT ON COLUMN vv.keep IS 'nested'"] {
        s.execute(sql, &[], None).unwrap();
    }
    let before = format!("{:?}", admin.catalog_presentation().unwrap());
    for suffix in ["", " RESTRICT"] {
        assert_eq!(s.execute(&format!("ALTER TABLE t DROP COLUMN c{suffix}"), &[], None),
            Err(CoreError::Sql(SqlError::DependentObjects)));
        assert_eq!(format!("{:?}", admin.catalog_presentation().unwrap()), before);
    }
    for sql in ["BEGIN", "ALTER TABLE t DROP COLUMN c CASCADE", "ROLLBACK"] {
        s.execute(sql, &[], None).unwrap();
    }
    assert_eq!(format!("{:?}", admin.catalog_presentation().unwrap()), before);
    assert_eq!(s.execute("SELECT * FROM vv", &[], None).unwrap().rows.len(), 1);
    // A failed later action rolls back the column and dependent-view cleanup.
    assert_eq!(s.execute("ALTER TABLE t DROP COLUMN c CASCADE, DROP COLUMN missing", &[], None),
        Err(CoreError::Sql(SqlError::UnknownColumn)));
    assert_eq!(format!("{:?}", admin.catalog_presentation().unwrap()), before);
    assert_eq!(s.execute("ALTER TABLE t DROP COLUMN spare CASCADE, DROP COLUMN c RESTRICT", &[], None),
        Err(CoreError::Sql(SqlError::DependentObjects)));
    assert_eq!(format!("{:?}", admin.catalog_presentation().unwrap()), before);
    s.execute("ALTER TABLE t DROP COLUMN c CASCADE", &[], None).unwrap();
    let expected = admin.catalog_presentation().unwrap();
    assert_eq!(expected.views.len(), 1);
    assert!(expected.comments.is_empty());
    drop(s); drop(admin);
    let admin = Admin::open(path, Limits::default()).unwrap();
    assert_eq!(format!("{:?}", admin.catalog_presentation().unwrap()), format!("{expected:?}"));
    let mut s = admin.sql_session();
    assert_eq!(s.execute("SELECT * FROM safe", &[], None).unwrap().rows.len(), 1);
    for view in ["v", "vv"] {
        assert_eq!(s.execute(&format!("SELECT * FROM {view}"), &[], None),
            Err(CoreError::Sql(SqlError::UnknownTable)));
    }
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn earlier_cascade_retires_nested_view_before_later_restrict() {
    let admin = Admin::create_engine(common::fresh("alter-mixed-drop"), Limits::default(), giggles_core::Engine::V2).unwrap();
    let mut s = admin.sql_session();
    for sql in ["CREATE TABLE t(id int PRIMARY KEY, c int, keep int)",
        "CREATE VIEW v AS SELECT c AS alias, keep FROM t", "CREATE VIEW vv AS SELECT keep FROM v",
        "ALTER TABLE t DROP COLUMN c CASCADE, DROP COLUMN keep RESTRICT"] {
        s.execute(sql, &[], None).unwrap();
    }
    assert!(admin.catalog_presentation().unwrap().views.is_empty());
}
