use giggles_pg_catalog::{Error, plan, plan_gateway};

#[test]
fn non_catalog_overflow_is_declined() {
    let list = (1..=20000)
        .map(|n| n.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let nodes = vec!["1"; 5000].join(",");
    for sql in [
        format!("SELECT octet_length('{}')", "x".repeat(70000)),
        format!("SELECT id FROM t WHERE id IN ({list})"),
        format!("SELECT id FROM t WHERE id IN ({nodes})"),
        format!("SELECT {} FROM t", vec!["id"; 4100].join(",")),
        format!("SELECT {} FROM t", vec!["-id"; 2100].join(",")),
        format!("SELECT {}id{} FROM t", "(".repeat(100), ")".repeat(100)),
        format!("SELECT {} FROM t", "id+".repeat(300) + "id"),
        format!(
            "SELECT octet_length($tag${}$tag$) /* pg_class */",
            "pg_catalog".repeat(7000)
        ),
        format!(
            "SELECT octet_length('{}') -- pg_class",
            "pg_class".repeat(9000)
        ),
    ] {
        assert!(plan(&sql).unwrap().is_none());
        assert!(plan_gateway(&sql).unwrap().is_none());
    }
}

#[test]
fn oversized_catalog_keeps_its_guard_even_with_late_or_quoted_references() {
    let padding = " ".repeat(70000);
    for tail in [
        "pg_class",
        "pg_catalog.pg_class",
        "\"pg_class\"",
        "information_schema.tables",
    ] {
        let sql = format!("SELECT oid FROM {padding}{tail}");
        assert_eq!(plan_gateway(&sql).unwrap_err(), Error::Limit);
    }
    for expr in [
        "version()",
        "current_user",
        "pg_get_viewdef(1)",
        "format_type(23,null)",
    ] {
        assert_eq!(
            plan_gateway(&format!("SELECT {padding}{expr}")).unwrap_err(),
            Error::Limit
        );
    }
}
