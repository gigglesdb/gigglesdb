use giggles_pg_catalog::*;
use std::collections::BTreeSet;

const BATCH: &str = include_str!("fixtures/npgsql_9_0_4_startup.sql");
// PostgreSQL 18 reference subset; the leading number is the original batch position.
const EXPECTED_TYPES: &str = r#"nspname|oid|typname|typtype|typnotnull|elemtypoid
1|pg_catalog|700|float4|b|f|
2|pg_catalog|23|int4|b|f|
3|pg_catalog|24|regproc|b|f|
4|pg_catalog|25|text|b|f|
5|pg_catalog|26|oid|b|f|
6|pg_catalog|27|tid|b|f|
7|pg_catalog|28|xid|b|f|
8|pg_catalog|29|cid|b|f|
9|pg_catalog|30|oidvector|b|f|
12|pg_catalog|194|pg_node_tree|b|f|
23|pg_catalog|22|int2vector|b|f|
24|pg_catalog|701|float8|b|f|
32|pg_catalog|1033|aclitem|b|f|
33|pg_catalog|1042|bpchar|b|f|
34|pg_catalog|1043|varchar|b|f|
35|pg_catalog|1082|date|b|f|
36|pg_catalog|1083|time|b|f|
37|pg_catalog|1114|timestamp|b|f|
38|pg_catalog|1184|timestamptz|b|f|
39|pg_catalog|1186|interval|b|f|
40|pg_catalog|1266|timetz|b|f|
43|pg_catalog|1700|numeric|b|f|
65|pg_catalog|16|bool|b|f|
66|pg_catalog|17|bytea|b|f|
70|pg_catalog|18|char|b|f|
71|pg_catalog|19|name|b|f|
72|pg_catalog|20|int8|b|f|
73|pg_catalog|21|int2|b|f|
93|pg_catalog|1000|_bool|a|f|16
94|pg_catalog|1001|_bytea|a|f|17
95|pg_catalog|1002|_char|a|f|18
96|pg_catalog|1003|_name|a|f|19
97|pg_catalog|1016|_int8|a|f|20
98|pg_catalog|1005|_int2|a|f|21
99|pg_catalog|1006|_int2vector|a|f|22
100|pg_catalog|1007|_int4|a|f|23
101|pg_catalog|1008|_regproc|a|f|24
102|pg_catalog|1009|_text|a|f|25
103|pg_catalog|1028|_oid|a|f|26
104|pg_catalog|1010|_tid|a|f|27
105|pg_catalog|1011|_xid|a|f|28
106|pg_catalog|1012|_cid|a|f|29
107|pg_catalog|1013|_oidvector|a|f|30
117|pg_catalog|1021|_float4|a|f|700
118|pg_catalog|1022|_float8|a|f|701
125|pg_catalog|1034|_aclitem|a|f|1033
126|pg_catalog|1014|_bpchar|a|f|1042
127|pg_catalog|1015|_varchar|a|f|1043
128|pg_catalog|1182|_date|a|f|1082
129|pg_catalog|1183|_time|a|f|1083
130|pg_catalog|1115|_timestamp|a|f|1114
131|pg_catalog|1185|_timestamptz|a|f|1184
132|pg_catalog|1187|_interval|a|f|1186
133|pg_catalog|1270|_timetz|a|f|1266
136|pg_catalog|1231|_numeric|a|f|1700
0|pg_catalog|3220|pg_lsn|b|f|
0|pg_catalog|3221|_pg_lsn|a|f|3220
0|pg_catalog|2950|uuid|b|f|
0|pg_catalog|2951|_uuid|a|f|2950
0|pg_catalog|3802|jsonb|b|f|
0|pg_catalog|3807|_jsonb|a|f|3802
"#;

fn snapshot() -> CatalogSnapshot {
    CatalogSnapshot {
        tables: vec![TableDef {
            id: 7,
            name: "t".into(),
            columns: vec![
                ColumnDef {
                    id: 1,
                    name: "id".into(),
                    data_type: ScalarType::Integer,
                    not_null: true,
                },
                ColumnDef {
                    id: 2,
                    name: "name".into(),
                    data_type: ScalarType::Text,
                    not_null: false,
                },
            ],
            primary_key: vec![0],
        }],
        ..CatalogSnapshot::default()
    }
}
fn run(sql: &str) -> ResultSet {
    let p = plan_gateway(sql)
        .unwrap_or_else(|e| panic!("plan {e:?}: {sql}"))
        .expect("catalog statement");
    let result = execute(&p, &snapshot(), &[]).unwrap_or_else(|e| panic!("execute {e:?}: {sql}"));
    assert_eq!(describe(&p).unwrap(), result.fields);
    result
}
fn s(value: &str) -> Value {
    Value::Text(value.into())
}
fn fields(result: &ResultSet) -> Vec<(&str, u32)> {
    result
        .fields
        .iter()
        .map(|f| (f.name.as_str(), f.data_type.oid()))
        .collect()
}

#[test]
fn verbatim_npgsql_9_0_4_startup_batch() {
    // This fixture has four statements and no semicolons in literals or comments.
    // Preserve every statement's SQL instead of rewriting the driver's query.
    let statements: Vec<_> = BATCH
        .split_inclusive(';')
        .filter(|s| !s.trim().is_empty())
        .collect();
    assert_eq!(statements.len(), 4);
    let version = run(statements[0]);
    assert_eq!(fields(&version), vec![("version", 25)]);
    assert_eq!(version.rows.len(), 1);
    assert_eq!(version.rows[0].len(), 1);
    assert!(matches!(&version.rows[0][0], Value::Text(_)));

    let types = run(statements[1]);
    assert_eq!(
        fields(&types),
        vec![
            ("nspname", 19),
            ("oid", 26),
            ("typname", 19),
            ("typtype", 18),
            ("typnotnull", 16),
            ("elemtypoid", 26)
        ]
    );
    // G1-a2 tracks the known PG18 divergence: its 30 arrays report class a
    // and an element OID. With pg_proc empty, G1-a reports b and NULL instead.
    // The 31 base-type rows retain the exact PostgreSQL 18 reference values.
    assert_eq!(
        EXPECTED_TYPES
            .lines()
            .filter(|line| line.contains("|b|"))
            .count(),
        31
    );
    assert_eq!(
        EXPECTED_TYPES
            .lines()
            .filter(|line| line.contains("|a|"))
            .count(),
        30
    );
    let expected: BTreeSet<Vec<Value>> = EXPECTED_TYPES
        .lines()
        .skip(1)
        .filter(|l| !l.is_empty())
        .map(|line| {
            let columns: Vec<_> = line.split('|').collect();
            assert_eq!(columns.len(), 7);
            vec![
                s(columns[1]),
                Value::Int(columns[2].parse().unwrap()),
                s(columns[3]),
                s(if columns[4] == "a" { "b" } else { columns[4] }),
                Value::Bool(columns[5] == "t"),
                if columns[4] == "a" || columns[6].is_empty() {
                    Value::Null
                } else {
                    Value::Int(columns[6].parse().unwrap())
                },
            ]
        })
        .collect();
    // E2a′: + bpchar, varchar, _bpchar, _varchar (ranks from the PG18.6 batch output).
    assert_eq!(expected.len(), 61);
    assert_eq!(types.rows.len(), 61);
    assert_eq!(
        types.rows.iter().cloned().collect::<BTreeSet<_>>(),
        expected
    );
    assert!(types.rows.iter().all(|row| row[3] == s("b")));
    let composites = run(statements[2]);
    assert_eq!(
        fields(&composites),
        vec![("oid", 26), ("attname", 19), ("atttypid", 26)]
    );
    assert!(composites.rows.is_empty());
    let enums = run(statements[3]);
    assert_eq!(fields(&enums), vec![("oid", 26), ("enumlabel", 19)]);
    assert!(enums.rows.is_empty());
}

#[test]
fn empty_range_and_enum_relations_have_postgres_18_shapes() {
    let ranges = run("SELECT * FROM pg_catalog.pg_range");
    assert!(ranges.rows.is_empty());
    assert_eq!(
        fields(&ranges),
        vec![
            ("rngtypid", 26),
            ("rngsubtype", 26),
            ("rngmultitypid", 26),
            ("rngcollation", 26),
            ("rngsubopc", 26),
            ("rngcanonical", 24),
            ("rngsubdiff", 24)
        ]
    );
    let enums = run("SELECT * FROM pg_catalog.pg_enum");
    assert!(enums.rows.is_empty());
    assert_eq!(
        fields(&enums),
        vec![
            ("oid", 26),
            ("enumtypid", 26),
            ("enumsortorder", 700),
            ("enumlabel", 19)
        ]
    );
}

#[test]
fn procedure_registry_remains_empty_in_g1a() {
    assert!(run("SELECT * FROM pg_catalog.pg_proc").rows.is_empty());
    let composite = run(
        "SELECT t.typreceive FROM pg_type t JOIN pg_class c ON c.reltype=t.oid WHERE c.relname='t'",
    );
    assert_eq!(composite.rows, vec![vec![Value::Int(2403)]]);
}
#[test]
fn range_and_enum_are_registered_with_postgres_catalog_identities() {
    assert_eq!(
        run(
            "SELECT oid,relname FROM pg_class WHERE relname IN ('pg_range','pg_enum') ORDER BY oid"
        )
        .rows,
        vec![
            vec![Value::Int(3501), s("pg_enum")],
            vec![Value::Int(3541), s("pg_range")]
        ]
    );
    let attrs = run(
        "SELECT c.relname,a.attname,a.atttypid,a.attnum FROM pg_class c JOIN pg_attribute a ON a.attrelid=c.oid WHERE c.relname IN ('pg_enum','pg_range') AND a.attnum>0 ORDER BY c.relname,a.attnum",
    );
    let expected = [
        ("pg_enum", "oid", 26, 1),
        ("pg_enum", "enumtypid", 26, 2),
        ("pg_enum", "enumsortorder", 700, 3),
        ("pg_enum", "enumlabel", 19, 4),
        ("pg_range", "rngtypid", 26, 1),
        ("pg_range", "rngsubtype", 26, 2),
        ("pg_range", "rngmultitypid", 26, 3),
        ("pg_range", "rngcollation", 26, 4),
        ("pg_range", "rngsubopc", 26, 5),
        ("pg_range", "rngcanonical", 24, 6),
        ("pg_range", "rngsubdiff", 24, 7),
    ]
    .into_iter()
    .map(|(r, n, t, a)| vec![s(r), s(n), Value::Int(t), Value::Int(a)])
    .collect::<Vec<_>>();
    assert_eq!(attrs.rows, expected);
}

const EXPECTED_RECEIVES: &str = r#"16|bool|2436|boolrecv|0|f|0|b
17|bytea|2412|bytearecv|0|f|0|b
18|char|2434|charrecv|0|f|0|b
19|name|2422|namerecv|0|f|0|b
20|int8|2408|int8recv|0|f|0|b
21|int2|2404|int2recv|0|f|0|b
22|int2vector|2410|int2vectorrecv|0|f|0|b
23|int4|2406|int4recv|0|f|0|b
24|regproc|2444|regprocrecv|0|f|0|b
25|text|2414|textrecv|0|f|0|b
26|oid|2418|oidrecv|0|f|0|b
28|xid|2440|xidrecv|0|f|0|b
30|oidvector|2420|oidvectorrecv|0|f|0|b
194|pg_node_tree|197|pg_node_tree_recv|0|f|0|b
700|float4|2424|float4recv|0|f|0|b
701|float8|2426|float8recv|0|f|0|b
1000|_bool|2400|array_recv|0|f|0|b
1001|_bytea|2400|array_recv|0|f|0|b
1002|_char|2400|array_recv|0|f|0|b
1003|_name|2400|array_recv|0|f|0|b
1005|_int2|2400|array_recv|0|f|0|b
1006|_int2vector|2400|array_recv|0|f|0|b
1007|_int4|2400|array_recv|0|f|0|b
1008|_regproc|2400|array_recv|0|f|0|b
1009|_text|2400|array_recv|0|f|0|b
1011|_xid|2400|array_recv|0|f|0|b
1013|_oidvector|2400|array_recv|0|f|0|b
1016|_int8|2400|array_recv|0|f|0|b
1021|_float4|2400|array_recv|0|f|0|b
1022|_float8|2400|array_recv|0|f|0|b
1028|_oid|2400|array_recv|0|f|0|b
1033|aclitem|0|-|0|f|0|b
1034|_aclitem|2400|array_recv|0|f|0|b
1082|date|2468|date_recv|0|f|0|b
1083|time|2470|time_recv|0|f|0|b
1114|timestamp|2474|timestamp_recv|0|f|0|b
1115|_timestamp|2400|array_recv|0|f|0|b
1182|_date|2400|array_recv|0|f|0|b
1183|_time|2400|array_recv|0|f|0|b
1184|timestamptz|2476|timestamptz_recv|0|f|0|b
1185|_timestamptz|2400|array_recv|0|f|0|b
1186|interval|2478|interval_recv|0|f|0|b
1187|_interval|2400|array_recv|0|f|0|b
1231|_numeric|2400|array_recv|0|f|0|b
1266|timetz|2472|timetz_recv|0|f|0|b
1270|_timetz|2400|array_recv|0|f|0|b
1700|numeric|2460|numeric_recv|0|f|0|b"#;

#[test]
fn every_builtin_receive_mapping_matches_postgres_18_reference() {
    let reference: Vec<Vec<&str>> = EXPECTED_RECEIVES
        .lines()
        .map(|line| line.split('|').collect())
        .collect();
    assert_eq!(reference.len(), 47);
    let oids = reference.iter().map(|c| c[0]).collect::<Vec<_>>().join(",");
    let result = run(&format!(
        "SELECT oid,typreceive,typrelid,typnotnull,typbasetype,typtype FROM pg_type WHERE oid IN ({oids})"
    ));
    assert_eq!(
        fields(&result),
        vec![
            ("oid", 26),
            ("typreceive", 26),
            ("typrelid", 26),
            ("typnotnull", 16),
            ("typbasetype", 26),
            ("typtype", 18)
        ]
    );
    let expected = reference
        .iter()
        .map(|c| {
            vec![
                Value::Int(c[0].parse().unwrap()),
                Value::Int(c[2].parse().unwrap()),
                Value::Int(c[4].parse().unwrap()),
                Value::Bool(c[5] == "t"),
                Value::Int(c[6].parse().unwrap()),
                s(c[7]),
            ]
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(expected.len(), 47);
    assert_eq!(result.rows.len(), 47);
    assert_eq!(result.rows.into_iter().collect::<BTreeSet<_>>(), expected);
}
