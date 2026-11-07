use giggles_pg_catalog::{CatalogSnapshot, Error, execute, plan};

#[test]
fn table_free_keepalive_selects_are_real_typed_queries() {
    use giggles_pg_catalog::{PgType, Value};
    for sql in [
        "SELECT 1",
        "/* keep alive */ select 1;",
        "SELECT 1 AS alive",
    ] {
        let p = plan(sql)
            .unwrap()
            .expect("table-free SELECT must route to evaluator");
        let r = execute(&p, &CatalogSnapshot::default(), &[]).unwrap();
        assert_eq!(r.fields[0].data_type, PgType::Int4);
        assert_eq!(r.rows, vec![vec![Value::Int(1)]]);
    }
    let p = plan("SELECT 1 WHERE false").unwrap().unwrap();
    assert!(
        execute(&p, &CatalogSnapshot::default(), &[])
            .unwrap()
            .rows
            .is_empty()
    );
    assert!(plan("SELECT 1; SELECT 2").is_err());
    assert!(plan("SELECT 1; DELETE FROM app").is_err());
    assert!(plan("SELECT pg_sleep(1)").is_err());
    assert!(plan("SELECT id FROM app").unwrap().is_none());
}

#[test]
fn zero_limit_never_returns_a_row_even_for_identity_or_derived_sources() {
    for sql in [
        "SELECT current_database() LIMIT 0",
        "SELECT relname FROM pg_catalog.pg_class LIMIT 0",
        "SELECT x.relname FROM (SELECT relname FROM pg_catalog.pg_class LIMIT 0) x",
    ] {
        let p = plan(sql).unwrap().unwrap();
        assert!(
            execute(&p, &CatalogSnapshot::default(), &[])
                .unwrap()
                .rows
                .is_empty(),
            "{sql}"
        );
    }
}

#[test]
fn unsupported_distinct_on_is_not_silently_treated_as_full_row_distinct() {
    assert!(matches!(
        plan(
            "SELECT DISTINCT ON (relkind) relkind, relname FROM pg_catalog.pg_class ORDER BY relkind, relname"
        ),
        Err(Error::Unsupported)
    ));
}

#[test]
fn detected_identity_and_startup_are_truthful_and_execution_scoped() {
    use giggles_pg_catalog::{PgType, Value, execute_with_startup};
    let snapshot = CatalogSnapshot::default();
    for sql in [
        "select version()",
        "SELECT pg_catalog.version() AS identity",
    ] {
        let p = plan(sql).unwrap().unwrap();
        let r = execute(&p, &snapshot, &[]).unwrap();
        assert_eq!(r.fields[0].data_type, PgType::Text);
        let Value::Text(v) = &r.rows[0][0] else {
            panic!()
        };
        assert!(v.starts_with("GigglesDB "));
    }
    let sql = "select round(extract(epoch from pg_postmaster_start_time() at time zone 'UTC')) as startup_time";
    let p = plan(sql).unwrap().unwrap();
    assert_eq!(execute(&p, &snapshot, &[]), Err(Error::Unsupported));
    for start in [123, 456] {
        let r = execute_with_startup(&p, &snapshot, &[], Some(start)).unwrap();
        assert_eq!(r.fields[0].data_type, PgType::Int8);
        assert_eq!(r.rows, vec![vec![Value::Int(start)]]);
    }
    assert_eq!(
        execute_with_startup(&p, &snapshot, &[], Some(-1)),
        Err(Error::Unsupported)
    );
    for sql in [
        "select version(1)",
        "select pg_postmaster_start_time()",
        "select round(extract(epoch from pg_postmaster_start_time() at time zone 'PST'))",
        "select txid_current()",
    ] {
        assert!(plan(sql).is_err(), "{sql}");
    }
}

#[test]
fn jdbc_secondary_type_lookup_reports_xid_and_array_identity() {
    use giggles_pg_catalog::Value;
    let p = plan(include_str!("fixtures/jdbc-column-type-lookup.sql"))
        .unwrap()
        .unwrap();
    for (oid, name, array) in [
        (28, "xid", false),
        (23, "int4", false),
        (1007, "_int4", true),
    ] {
        let r = execute(&p, &CatalogSnapshot::default(), &[Value::Int(oid)]).unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][0], Value::Bool(array));
        assert_eq!(r.rows[0][2], Value::Text(name.into()));
    }
}

#[test]
fn numeric_wire_override_cannot_change_projected_parameter_types() {
    for sql in [
        "select $1",
        "select $1::int8",
        "select x from (select $1 as x from pg_type) t",
        "select coalesce($1, 1)",
    ] {
        assert!(
            !plan(sql).unwrap().unwrap().allows_numeric_input_override(),
            "{sql}"
        );
    }
    assert!(
        plan("select oid from pg_type where oid=$1")
            .unwrap()
            .unwrap()
            .allows_numeric_input_override()
    );
}

#[test]
fn temporal_table_free_casts_route_to_core_but_catalog_relations_remain_owned() {
    for ty in [
        "date",
        "time",
        "timetz",
        "timestamp",
        "timestamptz",
        "interval",
    ] {
        assert!(
            giggles_pg_catalog::plan_gateway(&format!("SELECT $1::{ty}"))
                .unwrap()
                .is_none()
        );
        assert!(
            giggles_pg_catalog::plan_gateway(&format!("SELECT $1::{ty} FROM pg_catalog.pg_type"))
                .is_err()
        );
    }
}

#[test]
fn gateway_routes_scalar_structure_without_a_core_function_whitelist() {
    use giggles_pg_catalog::plan_gateway;
    for expression in [
        "abs(-7)",
        "sqrt(4)",
        "round(1.25::numeric,1)",
        "lower(upper('MiXeD'))",
        "length('abc')",
        "substring('abc',2,1)",
        "encode(decode('00ff','hex'),'hex')",
        "reverse('\\x00ff'::bytea)",
        "date_part('year', DATE '2000-01-01')",
        "make_time(12,34,56)",
        "NULL::interval",
        "NULL::numeric",
        "NULL::bytea",
        "pg_catalog.abs(-7)",
        "pg_catalog.lower('ABC')",
        "unknown_function()",
        "public.abs(-7)",
    ] {
        assert!(
            plan_gateway(&format!("SELECT {expression}"))
                .unwrap()
                .is_none(),
            "{expression}"
        );
    }
    for sql in [
        "SELECT 1",
        "/* SELECT sqrt(4) */ SELECT 'pg_class'",
        "SELECT version()",
        "SELECT current_database()",
        "SELECT oid FROM pg_catalog.pg_type",
    ] {
        assert!(plan_gateway(sql).unwrap().is_some(), "{sql}");
    }
    for sql in [
        "SELECT abs(-7) FROM pg_catalog.pg_type",
        "SELECT c.oid FROM pg_type c JOIN app a ON true",
        "SELECT current_database(), (SELECT id FROM app)",
    ] {
        assert!(plan_gateway(sql).is_err(), "{sql}");
    }
}
