use giggles_pg_catalog::{CatalogSnapshot, PgType, Value, execute, plan, plan_gateway};

// pg_dump asks `SELECT pg_catalog.pg_is_in_recovery()` before dumping. A
// standalone GigglesDB server is a primary, so it answers false as PG18 does.
#[test]
fn pg_is_in_recovery_reports_a_primary() {
    for sql in [
        "SELECT pg_catalog.pg_is_in_recovery()",
        "select pg_is_in_recovery()",
        "SELECT pg_is_in_recovery() AS standby",
    ] {
        for p in [
            plan(sql).unwrap().expect(sql),
            plan_gateway(sql).unwrap().expect(sql),
        ] {
            let r = execute(&p, &CatalogSnapshot::default(), &[]).unwrap();
            assert_eq!(r.fields.len(), 1, "{sql}");
            assert_eq!(r.fields[0].data_type, PgType::Bool, "{sql}");
            assert_eq!(r.rows, vec![vec![Value::Bool(false)]], "{sql}");
        }
    }
    assert_eq!(
        execute(
            &plan("SELECT pg_is_in_recovery()").unwrap().unwrap(),
            &CatalogSnapshot::default(),
            &[]
        )
        .unwrap()
        .fields[0]
            .name,
        "pg_is_in_recovery"
    );
    for sql in [
        "SELECT pg_is_in_recovery(1)",
        "SELECT other.pg_is_in_recovery()",
    ] {
        assert!(plan(sql).is_err(), "{sql}");
    }
}
