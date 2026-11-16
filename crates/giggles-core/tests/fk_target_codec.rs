mod common;
use giggles_core::{Admin, CoreError, Engine, Limits, relational::{SqlError, Value}};
fn reopen_legacy_fk(engine: Engine) {
    let path = common::fresh("fk-target-old");
    let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
    let mut session = admin.sql_session();
    for sql in ["CREATE TABLE p(a bigint,b bigint,PRIMARY KEY(a,b))",
        "CREATE TABLE c(a bigint,b bigint,FOREIGN KEY(a,b) REFERENCES p(b,a))",
        "INSERT INTO p VALUES(1,2)", "INSERT INTO c VALUES(2,1)"] {
        session.execute(sql, &[], None).unwrap();
    }
    drop(session); drop(admin);
    let admin = Admin::open(&path, Limits::default()).unwrap();
    let mut session = admin.sql_session();
    assert_eq!(session.execute("SELECT a,b FROM c", &[], None).unwrap().rows[0].values,
        vec![Value::Int(2), Value::Int(1)]);
    assert_eq!(session.execute("INSERT INTO c VALUES(3,4)", &[], None), Err(CoreError::Sql(SqlError::ForeignKeyViolation)));
    session.execute("CREATE UNIQUE INDEX equivalent ON p(b,a)", &[], None).unwrap();
    drop(session); drop(admin);
    let admin = Admin::open(&path, Limits::default()).unwrap();
    assert_eq!(admin.sql_session().execute("DELETE FROM p", &[], None), Err(CoreError::Sql(SqlError::ForeignKeyViolation)));
    drop(admin); std::fs::remove_dir_all(path).unwrap();
}
#[test]
fn legacy_fk_old_metadata_survives_rewrite_and_reopen() { reopen_legacy_fk(Engine::Legacy); }
#[cfg(feature = "engine-rocksdb")]
#[test]
fn v2_fk_old_metadata_survives_rewrite_and_reopen() { reopen_legacy_fk(Engine::V2); }
