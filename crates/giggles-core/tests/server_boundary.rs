mod common;
use giggles_core::{
    relational::{DataType, SqlError, Value},
    *,
};
fn admin() -> Admin {
    let path = common::fresh("p2-core");
    std::fs::create_dir_all(&path).unwrap();
    Admin::create(path.join("db.redb"), Default::default()).unwrap()
}
#[test]
fn descriptor_has_no_write_or_allocator_side_effects() {
    let a = admin();
    let mut s = a.sql_session();
    s.execute(
        "CREATE TABLE app (id BIGINT PRIMARY KEY, body TEXT)",
        &[],
        None,
    )
    .unwrap();
    let before = a.catalog().unwrap();
    let p = s.prepare("INSERT INTO app VALUES ($1,$2)").unwrap();
    for _ in 0..20 {
        let d = s.describe(&p).unwrap();
        assert_eq!(d.parameters, vec![DataType::BigInt, DataType::Text]);
        assert!(d.columns.is_empty());
    }
    assert_eq!(a.catalog().unwrap(), before);
    assert!(
        s.execute("SELECT * FROM app LIMIT 1", &[], None)
            .unwrap()
            .rows
            .is_empty()
    );
    s.execute_prepared(&p, &[Value::Int(1), Value::Text("ok".into())], None)
        .unwrap();
    assert_eq!(
        s.execute("SELECT * FROM app LIMIT 1", &[], None)
            .unwrap()
            .rows[0]
            .version,
        1
    );
}
#[test]
fn descriptor_domain_schema_and_rollback_branch_fences() {
    let a = admin();
    let b = admin();
    let mut s = a.sql_session();
    s.execute("BEGIN", &[], None).unwrap();
    s.execute("CREATE TABLE abandoned (id BIGINT PRIMARY KEY)", &[], None)
        .unwrap();
    let p = s.prepare("SELECT id FROM abandoned WHERE id=$1").unwrap();
    s.execute("ROLLBACK", &[], None).unwrap();
    s.execute("START TRANSACTION", &[], None).unwrap();
    s.execute(
        "CREATE TABLE replacement (id BIGINT PRIMARY KEY)",
        &[],
        None,
    )
    .unwrap();
    assert!(matches!(
        s.describe(&p),
        Err(CoreError::Sql(SqlError::StaleSchema))
    ));
    assert!(s.is_aborted());
    s.rollback();
    let p = s.prepare("BEGIN").unwrap();
    assert!(matches!(
        b.sql_session().describe(&p),
        Err(CoreError::Sql(SqlError::StaleSchema))
    ));
}
#[test]
fn descriptor_rejects_parameter_type_conflicts_and_gateway_abort() {
    let a = admin();
    let mut s = a.sql_session();
    s.execute(
        "CREATE TABLE app (id BIGINT PRIMARY KEY, body TEXT)",
        &[],
        None,
    )
    .unwrap();
    s.execute("BEGIN", &[], None).unwrap();
    let p = s.prepare("UPDATE app SET body=$1 WHERE id=$1").unwrap();
    assert!(matches!(
        s.describe(&p),
        Err(CoreError::Sql(SqlError::Type))
    ));
    assert!(s.is_aborted());
    assert!(matches!(
        s.execute("COMMIT", &[], None),
        Err(CoreError::Sql(SqlError::Aborted))
    ));
    s.execute("ROLLBACK", &[], None).unwrap();
    s.execute("BEGIN", &[], None).unwrap();
    s.abort();
    assert!(s.is_aborted());
    s.rollback();
    assert!(!s.is_aborted());
}




#[test]
fn prepared_rollback_can_recover_abort_but_keeps_transaction_fence() {
    let a = admin();
    let mut s = a.sql_session();
    s.execute("BEGIN", &[], None).unwrap();
    let old = s.prepare("ROLLBACK").unwrap();
    s.abort();
    let rollback = s.prepare("ROLLBACK").unwrap();
    assert!(s.describe(&rollback).unwrap().columns.is_empty());
    s.execute_prepared(&rollback, &[], None).unwrap();
    assert!(!s.is_aborted());
    s.execute("BEGIN", &[], None).unwrap();
    s.abort();
    assert!(matches!(
        s.describe(&old),
        Err(CoreError::Sql(SqlError::StaleSchema))
    ));
    assert!(matches!(
        s.execute_prepared(&old, &[], None),
        Err(CoreError::Sql(SqlError::StaleSchema))
    ));
    s.execute("ROLLBACK", &[], None).unwrap();
}
