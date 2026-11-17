mod common;
use giggles_core::{Admin, Limits};
#[test]
fn only_proven_table_free_scalar_queries_have_no_gateway_target() {
    let a = Admin::create(common::fresh("table-free-target"), Limits::default()).unwrap();
    let mut s = a.sql_session();
    for sql in [
        "SELECT 1",
        "SELECT format('%s-%s',1,'a')",
        "SELECT greatest(1,2)",
        "SELECT least('a','b')",
        "SELECT greatest(abs(-2),least(3,format('%s',4)::int))",
        "SELECT format('%s',greatest(1,2))",
        "SELECT char_length(concat('a','b'))",
        "SELECT get_byte(sha256(decode('00','hex')),0)",
        "SELECT encode(set_byte(decode('00','hex'),0,1),'hex')",
        "SELECT round(1.234::numeric,2)",
        "SELECT sin(0::double precision)",
        "SELECT gcd(12,8)",
        "SELECT substring('abc',1,2)",
        "SELECT abs(-2)",
        "SELECT pg_catalog.abs(-2)",
        "SELECT make_time(12,34,56)",
        "SELECT date_part('year', timestamp '2000-01-01')",
        "SELECT NULL::date",
        "SELECT $1::timestamp",
        "SELECT abs(-2) WHERE true ORDER BY abs(-3) LIMIT 1",
    ] {
        assert_eq!(s.prepare(sql).unwrap().table(), None, "{sql}");
    }
    s.execute("CREATE TABLE a(id BIGINT PRIMARY KEY)", &[], None)
        .unwrap();
    s.execute("CREATE TABLE b(id BIGINT PRIMARY KEY)", &[], None)
        .unwrap();
    assert_eq!(
        s.prepare("SELECT abs(id) FROM a").unwrap().table(),
        Some("a")
    );
    assert_eq!(
        s.prepare("SELECT a.id FROM a JOIN b ON a.id=b.id")
            .unwrap()
            .table(),
        Some("giggles_join")
    );
    // Unknown calls, user namespaces and nested table reads never get a table-free proof.
    for sql in [
        "SELECT format('%s',(SELECT id FROM a))",
        "SELECT greatest(1,(SELECT id FROM a))",
        "SELECT unknown_function()",
        "SELECT public.abs(-2)",
        "SELECT (SELECT id FROM a)",
    ] {
        if let Ok(p) = s.prepare(sql) {
            assert!(p.table().is_some(), "{sql}");
            if let Some(data_type) = match sql {
                "SELECT format('%s',(SELECT id FROM a))" => Some(giggles_core::relational::DataType::Text),
                "SELECT greatest(1,(SELECT id FROM a))" | "SELECT (SELECT id FROM a)" => Some(giggles_core::relational::DataType::BigInt),
                _ => None,
            } {
                let description = s.describe(&p).unwrap();
                assert_eq!(description.columns.len(), 1, "{sql}");
                assert_eq!(description.columns[0].data_type, data_type, "{sql}");
            } else {
                assert!(s.describe(&p).is_err(), "{sql}");
            }
        }
    }
    let p = s.prepare("WITH x AS (SELECT id FROM a) SELECT id FROM x").unwrap();
    assert!(p.table().is_some());
    let description = s.describe(&p).unwrap();
    assert_eq!(description.columns.len(), 1);
    assert_eq!(description.columns[0].name, "id");
}

#[test]
fn scalar_calls_without_from_reject_unbound_columns() {
    let a = Admin::create(
        common::fresh("table-free-unbound-column"),
        Limits::default(),
    )
    .unwrap();
    let mut s = a.sql_session();
    for sql in [
        "SELECT abs(id)",
        "SELECT format('%s',id)",
        "SELECT greatest(id,1)",
    ] {
        let prepared = s.prepare(sql).unwrap();
        assert!(prepared.table().is_some(), "{sql}");
        assert!(
            matches!(
                s.describe(&prepared),
                Err(giggles_core::CoreError::Sql(
                    giggles_core::relational::SqlError::UnknownColumn
                ))
            ),
            "{sql}",
        );
    }
}
