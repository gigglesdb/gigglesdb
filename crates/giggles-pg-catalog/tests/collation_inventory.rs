use giggles_pg_catalog::*;
#[test]
fn exact_postgres_inventory() {
    let sql = "SELECT oid,collname,collnamespace,collowner,collprovider,collisdeterministic,collencoding,collcollate,collctype,colllocale,collicurules,collversion FROM pg_catalog.pg_collation ORDER BY oid";
    let p = plan(sql).unwrap().unwrap();
    let result = execute(&p, &CatalogSnapshot::default(), &[]).unwrap();
    let expected: Vec<Vec<Value>> = include_str!("../../giggles-icu/src/pg_collation.txt")
        .lines()
        .map(|l| {
            l.split('|')
                .enumerate()
                .map(|(i, v)| match i {
                    0 | 6 => Value::Int(v.parse().unwrap()),
                    2 => Value::Int(11),
                    3 => Value::Int(10),
                    5 => Value::Bool(v == "t"),
                    _ if v.is_empty() => Value::Null,
                    _ => Value::Text(v.into()),
                })
                .collect()
        })
        .collect();
    assert_eq!(result.rows.len(), 880);
    assert_eq!(result.rows, expected);
}
#[test]
fn catalog_declines_collation_expressions() {
    for sql in [
        r#"SELECT '?' COLLATE "und-x-icu" < 'z'"#,
        r#"SELECT 'a' COLLATE "C" < 'B'"#,
        r#"SELECT collname FROM pg_collation ORDER BY collname COLLATE "C""#,
    ] {
        assert!(plan_gateway(sql).unwrap().is_none(), "{sql}");
    }
}
