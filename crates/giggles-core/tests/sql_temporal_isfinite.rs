mod common;
use giggles_core::{Admin, Limits, relational::*};
#[test]
fn finite_purity_namespaces_types_and_dependency_walk() {
    let a = Admin::create(common::fresh("isfinite-purity"), Limits::default()).unwrap();
    let mut s = a.sql_session();
    for name in [
        "isfinite",
        "ISFINITE",
        "pg_catalog.isfinite",
        "pg_catalog.\"isfinite\"",
    ] {
        let sql = format!("SELECT {name}(date '2000-01-01')");
        let p = s.prepare(&sql).unwrap();
        assert_eq!(p.table(), None, "{sql}");
        assert_eq!(
            s.execute_prepared(&p, &[], None).unwrap().rows[0].values,
            vec![Value::Bool(true)]
        );
    }
    for sql in [
        "SELECT isfinite()",
        "SELECT isfinite(1)",
        "SELECT isfinite(time '12:00:00')",
        "SELECT isfinite(date '2000-01-01', date '2000-01-02')",
        "SELECT \"ISFINITE\"(date '2000-01-01')",
        "SELECT public.isfinite(date '2000-01-01')",
    ] {
        if let Ok(p) = s.prepare(sql) {
            assert!(s.describe(&p).is_err(), "{sql}");
        }
    }
    for ty in ["date", "timestamp", "timestamptz", "interval"] {
        for (value, expected) in [("infinity", false), ("-infinity", false)] {
            let r = s
                .execute(&format!("SELECT isfinite('{value}'::{ty})"), &[], None)
                .unwrap();
            assert_eq!(r.rows[0].values, vec![Value::Bool(expected)]);
        }
        let p = s.prepare(&format!("SELECT isfinite($1::{ty})")).unwrap();
        assert_eq!(p.table(), None);
        assert_eq!(
            s.execute_prepared(&p, &[Value::Null], None).unwrap().rows[0].values,
            vec![Value::Null]
        );
    }
    s.execute(
        "CREATE TABLE a(id BIGINT PRIMARY KEY,t timestamp)",
        &[],
        None,
    )
    .unwrap();
    assert_eq!(
        s.prepare("SELECT isfinite(t) FROM a").unwrap().table(),
        Some("a")
    );
    for sql in [
        "SELECT isfinite((SELECT t FROM a))",
        "SELECT isfinite(unknown_function())",
        "SELECT public.isfinite(NULL::date)",
    ] {
        if let Ok(p) = s.prepare(sql) {
            assert!(p.table().is_some(), "{sql}");
        }
    }
}
