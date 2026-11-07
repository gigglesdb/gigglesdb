mod common;
use common::*;
use giggles_core::{Admin, Limits, relational::*};
#[test]
fn temporal_empty_descriptor_payload_reopen_export_restore() {
    let path = fresh("temporal-storage");
    let a = Admin::create(&path, Limits::default()).unwrap();
    let mut s = a.sql_session();
    s.execute("CREATE TABLE temporals(id BIGINT PRIMARY KEY,d DATE,t TIME,z TIMETZ,ts TIMESTAMP,tz TIMESTAMPTZ,i INTERVAL)",&[],None).unwrap();
    let empty_export = fresh("temporal-empty-export");
    a.export(&empty_export).unwrap();
    let bytes = std::fs::read(&empty_export).unwrap();
    assert_eq!(u32::from_be_bytes(bytes[8..12].try_into().unwrap()), 15);
    s.execute("INSERT INTO temporals VALUES(1,'2000-01-01','24:00:00','12:00:00+02','2000-01-01 12:34:56','2000-01-01 12:34:56+02','1 mon')",&[],None).unwrap();
    let sql = "SELECT d,t,z,ts,tz,i FROM temporals ORDER BY id LIMIT 10";
    let expected = s.execute(sql, &[], None).unwrap();
    let export = fresh("temporal-export");
    a.export(&export).unwrap();
    let restored = Admin::restore(&export, fresh("temporal-restored"), Limits::default()).unwrap();
    assert_eq!(
        restored.sql_session().execute(sql, &[], None).unwrap(),
        expected
    );
    drop(s);
    drop(a);
    let reopened = Admin::open(&path, Limits::default()).unwrap();
    assert_eq!(
        reopened.sql_session().execute(sql, &[], None).unwrap(),
        expected
    );
}
#[test]
fn temporal_primary_key_rejects_empty_definition_before_catalog_change() {
    let a = Admin::create(fresh("temporal-key"), Limits::default()).unwrap();
    let mut s = a.sql_session();
    for t in [
        "date",
        "time",
        "timetz",
        "timestamp",
        "timestamptz",
        "interval",
    ] {
        assert_eq!(
            s.execute(&format!("CREATE TABLE bad(id {t} PRIMARY KEY)"), &[], None),
            Err(giggles_core::CoreError::Sql(SqlError::Unsupported))
        );
    }
    s.execute("CREATE TABLE bad(id BIGINT PRIMARY KEY)", &[], None)
        .unwrap();
}
