use giggles_pg_catalog::*;

// PostgreSQL 18 pg_type golden rows: oid, typname, typlen, typtype,
// typcategory, typispreferred, typelem, typarray.
type GoldenTypeRow = (
    i64,
    &'static str,
    i64,
    &'static str,
    &'static str,
    bool,
    i64,
    i64,
);

const PG18_TYPE_ROWS: &[GoldenTypeRow] = &[
    (16, "bool", 1, "b", "B", true, 0, 1000),
    (17, "bytea", -1, "b", "U", false, 0, 1001),
    (18, "char", 1, "b", "Z", false, 0, 1002),
    (19, "name", 64, "b", "S", false, 18, 1003),
    (20, "int8", 8, "b", "N", false, 0, 1016),
    (21, "int2", 2, "b", "N", false, 0, 1005),
    (22, "int2vector", -1, "b", "A", false, 21, 1006),
    (23, "int4", 4, "b", "N", false, 0, 1007),
    (24, "regproc", 4, "b", "N", false, 0, 1008),
    (25, "text", -1, "b", "S", true, 0, 1009),
    (26, "oid", 4, "b", "N", true, 0, 1028),
    (28, "xid", 4, "b", "U", false, 0, 1011),
    (30, "oidvector", -1, "b", "A", false, 26, 1013),
    (194, "pg_node_tree", -1, "b", "Z", false, 0, 0),
    (700, "float4", 4, "b", "N", false, 0, 1021),
    (701, "float8", 8, "b", "N", true, 0, 1022),
    (1000, "_bool", -1, "b", "A", false, 16, 0),
    (1001, "_bytea", -1, "b", "A", false, 17, 0),
    (1002, "_char", -1, "b", "A", false, 18, 0),
    (1003, "_name", -1, "b", "A", false, 19, 0),
    (1005, "_int2", -1, "b", "A", false, 21, 0),
    (1006, "_int2vector", -1, "b", "A", false, 22, 0),
    (1007, "_int4", -1, "b", "A", false, 23, 0),
    (1008, "_regproc", -1, "b", "A", false, 24, 0),
    (1009, "_text", -1, "b", "A", false, 25, 0),
    (1011, "_xid", -1, "b", "A", false, 28, 0),
    (1013, "_oidvector", -1, "b", "A", false, 30, 0),
    (1016, "_int8", -1, "b", "A", false, 20, 0),
    (1021, "_float4", -1, "b", "A", false, 700, 0),
    (1022, "_float8", -1, "b", "A", false, 701, 0),
    (1028, "_oid", -1, "b", "A", false, 26, 0),
    (1033, "aclitem", 16, "b", "U", false, 0, 1034),
    (1034, "_aclitem", -1, "b", "A", false, 1033, 0),
    (1082, "date", 4, "b", "D", false, 0, 1182),
    (1083, "time", 8, "b", "D", false, 0, 1183),
    (1114, "timestamp", 8, "b", "D", false, 0, 1115),
    (1115, "_timestamp", -1, "b", "A", false, 1114, 0),
    (1182, "_date", -1, "b", "A", false, 1082, 0),
    (1183, "_time", -1, "b", "A", false, 1083, 0),
    (1184, "timestamptz", 8, "b", "D", true, 0, 1185),
    (1185, "_timestamptz", -1, "b", "A", false, 1184, 0),
    (1186, "interval", 16, "b", "T", true, 0, 1187),
    (1187, "_interval", -1, "b", "A", false, 1186, 0),
    (1231, "_numeric", -1, "b", "A", false, 1700, 0),
    (1266, "timetz", 12, "b", "D", false, 0, 1270),
    (1270, "_timetz", -1, "b", "A", false, 1266, 0),
    (1700, "numeric", -1, "b", "N", false, 0, 1231),
];

fn run(sql: &str) -> ResultSet {
    let plan = plan_gateway(sql).expect("catalog plan").expect("catalog");
    let described = describe(&plan).expect("describe");
    let result = execute(&plan, &CatalogSnapshot::default(), &[]).expect("execute");
    assert_eq!(result.fields, described);
    result
}

#[test]
fn pg18_type_rows_match_golden() {
    assert_eq!(PG18_TYPE_ROWS.len(), 47);
    let oids = PG18_TYPE_ROWS
        .iter()
        .map(|row| row.0.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let result = run(&format!(
        "SELECT oid, typname, typlen, typtype, typcategory, typispreferred, typelem, typarray FROM pg_catalog.pg_type WHERE oid IN ({oids}) ORDER BY oid"
    ));
    let expected: Vec<Vec<Value>> = PG18_TYPE_ROWS
        .iter()
        .map(
            |&(oid, name, len, kind, category, preferred, elem, array)| {
                vec![
                    Value::Int(oid),
                    Value::Text(name.into()),
                    Value::Int(len),
                    Value::Text(kind.into()),
                    Value::Text(category.into()),
                    Value::Bool(preferred),
                    Value::Int(elem),
                    Value::Int(array),
                ]
            },
        )
        .collect();
    assert_eq!(result.rows, expected);
}

#[test]
fn preferred_type_oids_match_pg18_modeled_types() {
    let result = run("SELECT oid FROM pg_catalog.pg_type WHERE typispreferred ORDER BY oid");
    let expected: Vec<Vec<Value>> = [16, 25, 26, 701, 1184, 1186]
        .into_iter()
        .map(|oid| vec![Value::Int(oid)])
        .collect();
    assert_eq!(result.rows, expected);
}
