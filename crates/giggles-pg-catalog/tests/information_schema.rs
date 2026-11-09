use giggles_pg_catalog::*;

const PG18_SCHEMATA: &str = r#"catalog_name|schema_name|schema_owner|default_character_set_catalog|default_character_set_schema|default_character_set_name|sql_path
postgres|information_schema|postgres||||
postgres|pg_catalog|postgres||||
postgres|pg_toast|postgres||||
postgres|public|pg_database_owner||||
(4 rows)"#;

const PG18_TABLES: &str = r#"table_catalog|table_schema|table_name|table_type|self_referencing_column_name|reference_generation|user_defined_type_catalog|user_defined_type_schema|user_defined_type_name|is_insertable_into|is_typed|commit_action
postgres|public|g3_items|BASE TABLE||||||YES|NO|
postgres|public|g3_pair|BASE TABLE||||||YES|NO|
(2 rows)"#;

const PG18_COLUMNS: &str = r#"table_catalog|table_schema|table_name|column_name|ordinal_position|column_default|is_nullable|data_type|character_maximum_length|character_octet_length|numeric_precision|numeric_precision_radix|numeric_scale|datetime_precision|interval_type|interval_precision|character_set_catalog|character_set_schema|character_set_name|collation_catalog|collation_schema|collation_name|domain_catalog|domain_schema|domain_name|udt_catalog|udt_schema|udt_name|scope_catalog|scope_schema|scope_name|maximum_cardinality|dtd_identifier|is_self_referencing|is_identity|identity_generation|identity_start|identity_increment|identity_maximum|identity_minimum|identity_cycle|is_generated|generation_expression|is_updatable
postgres|public|g3_items|id|1||NO|integer|||32|2|0|||||||||||||postgres|pg_catalog|int4|||||1|NO|NO||||||NO|NEVER||YES
postgres|public|g3_items|name|2||NO|text||1073741824||||||||||||||||postgres|pg_catalog|text|||||2|NO|NO||||||NO|NEVER||YES
postgres|public|g3_items|qty|3||YES|bigint|||64|2|0|||||||||||||postgres|pg_catalog|int8|||||3|NO|NO||||||NO|NEVER||YES
postgres|public|g3_items|price|4||YES|numeric||||10||||||||||||||postgres|pg_catalog|numeric|||||4|NO|NO||||||NO|NEVER||YES
postgres|public|g3_items|created|5||YES|timestamp with time zone||||||6||||||||||||postgres|pg_catalog|timestamptz|||||5|NO|NO||||||NO|NEVER||YES
postgres|public|g3_items|flag|6||YES|boolean||||||||||||||||||postgres|pg_catalog|bool|||||6|NO|NO||||||NO|NEVER||YES
postgres|public|g3_items|data|7||YES|bytea||||||||||||||||||postgres|pg_catalog|bytea|||||7|NO|NO||||||NO|NEVER||YES
postgres|public|g3_items|d|8||YES|date||||||0||||||||||||postgres|pg_catalog|date|||||8|NO|NO||||||NO|NEVER||YES
postgres|public|g3_items|r|9||YES|real|||24|2||||||||||||||postgres|pg_catalog|float4|||||9|NO|NO||||||NO|NEVER||YES
postgres|public|g3_items|dp|10||YES|double precision|||53|2||||||||||||||postgres|pg_catalog|float8|||||10|NO|NO||||||NO|NEVER||YES
postgres|public|g3_items|sm|11||YES|smallint|||16|2|0|||||||||||||postgres|pg_catalog|int2|||||11|NO|NO||||||NO|NEVER||YES
postgres|public|g3_items|t|12||YES|time without time zone||||||6||||||||||||postgres|pg_catalog|time|||||12|NO|NO||||||NO|NEVER||YES
postgres|public|g3_items|tz|13||YES|time with time zone||||||6||||||||||||postgres|pg_catalog|timetz|||||13|NO|NO||||||NO|NEVER||YES
postgres|public|g3_items|ts|14||YES|timestamp without time zone||||||6||||||||||||postgres|pg_catalog|timestamp|||||14|NO|NO||||||NO|NEVER||YES
postgres|public|g3_items|iv|15||YES|interval||||||6||||||||||||postgres|pg_catalog|interval|||||15|NO|NO||||||NO|NEVER||YES
postgres|public|g3_pair|a|1||NO|integer|||32|2|0|||||||||||||postgres|pg_catalog|int4|||||1|NO|NO||||||NO|NEVER||YES
postgres|public|g3_pair|b|2||NO|text||1073741824||||||||||||||||postgres|pg_catalog|text|||||2|NO|NO||||||NO|NEVER||YES
postgres|public|g3_pair|note|3||YES|text||1073741824||||||||||||||||postgres|pg_catalog|text|||||3|NO|NO||||||NO|NEVER||YES
(18 rows)"#;

const PG18_TABLE_CONSTRAINTS: &str = r#"constraint_catalog|constraint_schema|constraint_name|table_catalog|table_schema|table_name|constraint_type|is_deferrable|initially_deferred|enforced|nulls_distinct
postgres|public|g3_items_id_not_null|postgres|public|g3_items|CHECK|NO|NO|YES|
postgres|public|g3_items_name_not_null|postgres|public|g3_items|CHECK|NO|NO|YES|
postgres|public|g3_items_pkey|postgres|public|g3_items|PRIMARY KEY|NO|NO|YES|
postgres|public|g3_pair_a_not_null|postgres|public|g3_pair|CHECK|NO|NO|YES|
postgres|public|g3_pair_b_not_null|postgres|public|g3_pair|CHECK|NO|NO|YES|
postgres|public|g3_pair_pkey|postgres|public|g3_pair|PRIMARY KEY|NO|NO|YES|
(6 rows)"#;

const PG18_KEY_COLUMN_USAGE: &str = r#"constraint_catalog|constraint_schema|constraint_name|table_catalog|table_schema|table_name|column_name|ordinal_position|position_in_unique_constraint
postgres|public|g3_items_pkey|postgres|public|g3_items|id|1|
postgres|public|g3_pair_pkey|postgres|public|g3_pair|a|1|
postgres|public|g3_pair_pkey|postgres|public|g3_pair|b|2|
(3 rows)"#;

const PG18_VIEW_COLUMNS: &str = r#"== tables columns
attname|atttypid|atttypid|attnum
table_catalog|information_schema.sql_identifier|13312|1
table_schema|information_schema.sql_identifier|13312|2
table_name|information_schema.sql_identifier|13312|3
table_type|information_schema.character_data|13310|4
self_referencing_column_name|information_schema.sql_identifier|13312|5
reference_generation|information_schema.character_data|13310|6
user_defined_type_catalog|information_schema.sql_identifier|13312|7
user_defined_type_schema|information_schema.sql_identifier|13312|8
user_defined_type_name|information_schema.sql_identifier|13312|9
is_insertable_into|information_schema.yes_or_no|13320|10
is_typed|information_schema.yes_or_no|13320|11
commit_action|information_schema.character_data|13310|12
(12 rows)
== columns columns
attname|atttypid|atttypid|attnum
table_catalog|information_schema.sql_identifier|13312|1
table_schema|information_schema.sql_identifier|13312|2
table_name|information_schema.sql_identifier|13312|3
column_name|information_schema.sql_identifier|13312|4
ordinal_position|information_schema.cardinal_number|13307|5
column_default|information_schema.character_data|13310|6
is_nullable|information_schema.yes_or_no|13320|7
data_type|information_schema.character_data|13310|8
character_maximum_length|information_schema.cardinal_number|13307|9
character_octet_length|information_schema.cardinal_number|13307|10
numeric_precision|information_schema.cardinal_number|13307|11
numeric_precision_radix|information_schema.cardinal_number|13307|12
numeric_scale|information_schema.cardinal_number|13307|13
datetime_precision|information_schema.cardinal_number|13307|14
interval_type|information_schema.character_data|13310|15
interval_precision|information_schema.cardinal_number|13307|16
character_set_catalog|information_schema.sql_identifier|13312|17
character_set_schema|information_schema.sql_identifier|13312|18
character_set_name|information_schema.sql_identifier|13312|19
collation_catalog|information_schema.sql_identifier|13312|20
collation_schema|information_schema.sql_identifier|13312|21
collation_name|information_schema.sql_identifier|13312|22
domain_catalog|information_schema.sql_identifier|13312|23
domain_schema|information_schema.sql_identifier|13312|24
domain_name|information_schema.sql_identifier|13312|25
udt_catalog|information_schema.sql_identifier|13312|26
udt_schema|information_schema.sql_identifier|13312|27
udt_name|information_schema.sql_identifier|13312|28
scope_catalog|information_schema.sql_identifier|13312|29
scope_schema|information_schema.sql_identifier|13312|30
scope_name|information_schema.sql_identifier|13312|31
maximum_cardinality|information_schema.cardinal_number|13307|32
dtd_identifier|information_schema.sql_identifier|13312|33
is_self_referencing|information_schema.yes_or_no|13320|34
is_identity|information_schema.yes_or_no|13320|35
identity_generation|information_schema.character_data|13310|36
identity_start|information_schema.character_data|13310|37
identity_increment|information_schema.character_data|13310|38
identity_maximum|information_schema.character_data|13310|39
identity_minimum|information_schema.character_data|13310|40
identity_cycle|information_schema.yes_or_no|13320|41
is_generated|information_schema.character_data|13310|42
generation_expression|information_schema.character_data|13310|43
is_updatable|information_schema.yes_or_no|13320|44
(44 rows)
== table_constraints columns
attname|atttypid|atttypid|attnum
constraint_catalog|information_schema.sql_identifier|13312|1
constraint_schema|information_schema.sql_identifier|13312|2
constraint_name|information_schema.sql_identifier|13312|3
table_catalog|information_schema.sql_identifier|13312|4
table_schema|information_schema.sql_identifier|13312|5
table_name|information_schema.sql_identifier|13312|6
constraint_type|information_schema.character_data|13310|7
is_deferrable|information_schema.yes_or_no|13320|8
initially_deferred|information_schema.yes_or_no|13320|9
enforced|information_schema.yes_or_no|13320|10
nulls_distinct|information_schema.yes_or_no|13320|11
(11 rows)
== key_column_usage columns
attname|atttypid|atttypid|attnum
constraint_catalog|information_schema.sql_identifier|13312|1
constraint_schema|information_schema.sql_identifier|13312|2
constraint_name|information_schema.sql_identifier|13312|3
table_catalog|information_schema.sql_identifier|13312|4
table_schema|information_schema.sql_identifier|13312|5
table_name|information_schema.sql_identifier|13312|6
column_name|information_schema.sql_identifier|13312|7
ordinal_position|information_schema.cardinal_number|13307|8
position_in_unique_constraint|information_schema.cardinal_number|13307|9
(9 rows)
== schemata columns
attname|atttypid|atttypid|attnum
catalog_name|information_schema.sql_identifier|13312|1
schema_name|information_schema.sql_identifier|13312|2
schema_owner|information_schema.sql_identifier|13312|3
default_character_set_catalog|information_schema.sql_identifier|13312|4
default_character_set_schema|information_schema.sql_identifier|13312|5
default_character_set_name|information_schema.sql_identifier|13312|6
sql_path|information_schema.character_data|13310|7
(7 rows)"#;

fn snapshot() -> CatalogSnapshot {
    use ScalarType::*;
    let mut snapshot = CatalogSnapshot {
        user: "postgres".into(),
        database: "postgres".into(),
        ..CatalogSnapshot::default()
    };
    snapshot.roles[0].name = "postgres".into();
    let columns = [
        ("id", Integer),
        ("name", Text),
        ("qty", BigInt),
        ("price", Numeric),
        ("created", TimestampTz),
        ("flag", Boolean),
        ("data", Bytes),
        ("d", Date),
        ("r", Real),
        ("dp", DoublePrecision),
        ("sm", SmallInt),
        ("t", Time),
        ("tz", TimeTz),
        ("ts", Timestamp),
        ("iv", Interval),
    ];
    snapshot.tables = vec![
        TableDef {
            id: 1,
            name: "g3_items".into(),
            columns: columns
                .into_iter()
                .enumerate()
                .map(|(i, (name, data_type))| ColumnDef {
                    id: i as u64 + 1,
                    name: name.into(),
                    data_type,
                    not_null: i < 2,
                })
                .collect(),
            primary_key: vec![0],
        },
        TableDef {
            id: 2,
            name: "g3_pair".into(),
            columns: [("a", Integer), ("b", Text), ("note", Text)]
                .into_iter()
                .enumerate()
                .map(|(i, (name, data_type))| ColumnDef {
                    id: i as u64 + 1,
                    name: name.into(),
                    data_type,
                    not_null: i < 2,
                })
                .collect(),
            primary_key: vec![0, 1],
        },
    ];
    snapshot
}

fn run(snapshot: &CatalogSnapshot, sql: &str) -> ResultSet {
    let plan = plan(sql).expect(sql).expect("catalog query");
    let result = execute(&plan, snapshot, &[]).expect(sql);
    assert_eq!(describe(&plan).expect("describe"), result.fields);
    result
}

fn golden_rows(text: &str, fields: &[Field]) -> Vec<Vec<Value>> {
    text.lines()
        .skip(1)
        .filter(|line| !line.starts_with('('))
        .filter(|line| !line.starts_with("postgres|pg_toast|"))
        .map(|line| {
            let cells: Vec<_> = line.split('|').collect();
            assert_eq!(cells.len(), fields.len());
            cells
                .into_iter()
                .zip(fields)
                .map(|(cell, field)| {
                    if cell.is_empty() {
                        Value::Null
                    } else if field.data_type == PgType::Int4 {
                        Value::Int(cell.parse().expect("cardinal number"))
                    } else {
                        Value::Text(cell.into())
                    }
                })
                .collect()
        })
        .collect()
}

#[test]
fn public_rows_match_postgresql_18() {
    let snapshot = snapshot();
    for (view, golden, filter, order) in [
        ("schemata", PG18_SCHEMATA, "", "schema_name"),
        (
            "tables",
            PG18_TABLES,
            " WHERE table_schema = 'public'",
            "table_name",
        ),
        (
            "columns",
            PG18_COLUMNS,
            " WHERE table_schema = 'public'",
            "table_name, ordinal_position",
        ),
        (
            "table_constraints",
            PG18_TABLE_CONSTRAINTS,
            " WHERE table_schema = 'public'",
            "table_name, constraint_name",
        ),
        (
            "key_column_usage",
            PG18_KEY_COLUMN_USAGE,
            " WHERE table_schema = 'public'",
            "table_name, constraint_name, ordinal_position",
        ),
    ] {
        let result = run(
            &snapshot,
            &format!("SELECT * FROM information_schema.{view}{filter} ORDER BY {order}"),
        );
        assert_eq!(result.rows, golden_rows(golden, &result.fields), "{view}");
    }
}

#[test]
fn all_view_columns_match_pg18_order_and_mapped_domain_types() {
    for section in PG18_VIEW_COLUMNS.split("== ").skip(1) {
        let mut lines = section.lines();
        let view = lines.next().unwrap().strip_suffix(" columns").unwrap();
        let expected: Vec<Field> = lines
            .skip(1)
            .filter(|line| !line.starts_with('('))
            .map(|line| {
                let cells: Vec<_> = line.split('|').collect();
                Field {
                    name: cells[0].into(),
                    data_type: match cells[1] {
                        "information_schema.sql_identifier" => PgType::Name,
                        "information_schema.cardinal_number" => PgType::Int4,
                        "information_schema.character_data" | "information_schema.yes_or_no" => {
                            PgType::Varchar
                        }
                        other => panic!("unknown domain {other}"),
                    },
                }
            })
            .collect();
        let result = run(
            &snapshot(),
            &format!("SELECT * FROM information_schema.{view} LIMIT 0"),
        );
        assert_eq!(result.fields, expected, "{view}");
    }
}

#[test]
fn schema_qualification_routes_only_information_schema_views() {
    for view in [
        "schemata",
        "tables",
        "columns",
        "table_constraints",
        "key_column_usage",
    ] {
        let qualified = run(
            &snapshot(),
            &format!("SELECT * FROM information_schema.{view} LIMIT 0"),
        );
        assert!(!qualified.fields.is_empty(), "{view}");
        for name in [view.to_string(), format!("\"{view}\"")] {
            let sql = format!("SELECT * FROM {name} LIMIT 0");
            assert!(matches!(plan(&sql), Ok(None)), "{sql}");
            assert!(matches!(plan_gateway(&sql), Ok(None)), "{sql}");
        }
        for schema in ["public", "pg_catalog", "other"] {
            let planned = plan(&format!("SELECT * FROM {schema}.{view}"));
            assert!(
                !matches!(planned, Ok(Some(_))),
                "wrong schema {schema}.{view} routed as information_schema"
            );
        }
    }
}

#[test]
fn namespace_and_view_class_attribute_rowtype_registration_are_coherent() {
    let snapshot = snapshot();
    assert_eq!(
        run(
            &snapshot,
            "SELECT oid FROM pg_namespace WHERE nspname = 'information_schema'"
        )
        .rows,
        vec![vec![Value::Int(13293)]]
    );
    for (view, oid, rowtype, count) in [
        ("columns", 13381, 13383, 44),
        ("key_column_usage", 13414, 13416, 9),
        ("schemata", 13467, 13469, 7),
        ("table_constraints", 13496, 13498, 11),
        ("tables", 13510, 13512, 12),
    ] {
        assert_eq!(run(&snapshot, &format!("SELECT oid, relnamespace, reltype, relkind, relnatts FROM pg_class WHERE relname = '{view}'")).rows, vec![vec![Value::Int(oid), Value::Int(13293), Value::Int(rowtype), Value::Text("v".into()), Value::Int(count)]]);
        assert_eq!(
            run(
                &snapshot,
                &format!(
                    "SELECT oid, typnamespace, typrelid, typtype FROM pg_type WHERE oid = {rowtype}"
                )
            )
            .rows,
            vec![vec![
                Value::Int(rowtype),
                Value::Int(13293),
                Value::Int(oid),
                Value::Text("c".into())
            ]]
        );
        let attrs = run(
            &snapshot,
            &format!(
                "SELECT attname, atttypid, attnum FROM pg_attribute WHERE attrelid = {oid} AND attnum > 0 ORDER BY attnum"
            ),
        );
        let fields = run(
            &snapshot,
            &format!("SELECT * FROM information_schema.{view} LIMIT 0"),
        )
        .fields;
        assert_eq!(attrs.rows.len(), count as usize);
        for (i, (row, field)) in attrs.rows.iter().zip(fields).enumerate() {
            let type_oid = match field.data_type {
                PgType::Name => 19,
                PgType::Int4 => 23,
                PgType::Varchar => 1043,
                other => panic!("unexpected type {other:?}"),
            };
            assert_eq!(
                *row,
                vec![
                    Value::Text(field.name),
                    Value::Int(type_oid),
                    Value::Int(i as i64 + 1)
                ]
            );
        }
    }
}

const CLIENT_01_G3_ITEMS_SQL: &str = r#"SELECT column_name,data_type,is_nullable FROM information_schema.columns WHERE table_name='g3_items' ORDER BY ordinal_position;"#;

const CLIENT_01_G3_ITEMS_PSV: &str = r#"column_name|data_type|is_nullable
id|integer|NO
name|text|NO
qty|bigint|YES
price|numeric|YES
created|timestamp with time zone|YES
flag|boolean|YES
data|bytea|YES
d|date|YES
r|real|YES
dp|double precision|YES
sm|smallint|YES
t|time without time zone|YES
tz|time with time zone|YES
ts|timestamp without time zone|YES
iv|interval|YES
(15 rows)"#;

const CLIENT_01_G3_PAIR_SQL: &str = r#"SELECT column_name,data_type,is_nullable FROM information_schema.columns WHERE table_name='g3_pair' ORDER BY ordinal_position;"#;

const CLIENT_01_G3_PAIR_PSV: &str = r#"column_name|data_type|is_nullable
a|integer|NO
b|text|NO
note|text|YES
(3 rows)"#;

const CLIENT_02_G3_ITEMS_SQL: &str = r#"SELECT table_catalog, table_schema, table_name, table_type
	FROM information_schema.tables
	WHERE
	    table_type IN ('BASE TABLE', 'FOREIGN', 'FOREIGN TABLE') AND
	    table_schema NOT IN ('pg_catalog', 'information_schema') AND table_schema = $1 AND table_name = $2 AND table_type = $3;"#;

const CLIENT_02_G3_ITEMS_PSV: &str = r#"table_catalog|table_schema|table_name|table_type
postgres|public|g3_items|BASE TABLE
(1 row)"#;

const CLIENT_02_G3_PAIR_SQL: &str = r#"SELECT table_catalog, table_schema, table_name, table_type
	FROM information_schema.tables
	WHERE
	    table_type IN ('BASE TABLE', 'FOREIGN', 'FOREIGN TABLE') AND
	    table_schema NOT IN ('pg_catalog', 'information_schema') AND table_schema = $1 AND table_name = $2 AND table_type = $3;"#;

const CLIENT_02_G3_PAIR_PSV: &str = r#"table_catalog|table_schema|table_name|table_type
postgres|public|g3_pair|BASE TABLE
(1 row)"#;

const CLIENT_03_G3_ITEMS_SQL: &str = r#"SELECT
	    table_catalog,
	    table_schema,
	    table_name,
	    column_name,
	    ordinal_position,
	    column_default,
	    is_nullable,
	    CASE WHEN udt_schema is NULL THEN udt_name ELSE format_type(typ.oid, NULL) END,
	    character_maximum_length,
	    character_octet_length,
	    numeric_precision,
	    numeric_precision_radix,
	    numeric_scale,
	    datetime_precision,
	    character_set_catalog,
	    character_set_schema,
	    character_set_name,
	    collation_catalog
	FROM information_schema.columns
	JOIN pg_namespace AS ns ON ns.nspname = udt_schema
	JOIN pg_type AS typ ON typnamespace = ns.oid AND typname = udt_name WHERE table_schema = $1 AND table_name = $2;"#;

const CLIENT_03_G3_ITEMS_PSV: &str = r#"table_catalog|table_schema|table_name|column_name|ordinal_position|column_default|is_nullable|format_type|character_maximum_length|character_octet_length|numeric_precision|numeric_precision_radix|numeric_scale|datetime_precision|character_set_catalog|character_set_schema|character_set_name|collation_catalog
postgres|public|g3_items|id|1|<NULL>|NO|integer|<NULL>|<NULL>|32|2|0|<NULL>|<NULL>|<NULL>|<NULL>|<NULL>
postgres|public|g3_items|name|2|<NULL>|NO|text|<NULL>|1073741824|<NULL>|<NULL>|<NULL>|<NULL>|<NULL>|<NULL>|<NULL>|<NULL>
postgres|public|g3_items|qty|3|<NULL>|YES|bigint|<NULL>|<NULL>|64|2|0|<NULL>|<NULL>|<NULL>|<NULL>|<NULL>
postgres|public|g3_items|price|4|<NULL>|YES|numeric|<NULL>|<NULL>|<NULL>|10|<NULL>|<NULL>|<NULL>|<NULL>|<NULL>|<NULL>
postgres|public|g3_items|created|5|<NULL>|YES|timestamp with time zone|<NULL>|<NULL>|<NULL>|<NULL>|<NULL>|6|<NULL>|<NULL>|<NULL>|<NULL>
postgres|public|g3_items|flag|6|<NULL>|YES|boolean|<NULL>|<NULL>|<NULL>|<NULL>|<NULL>|<NULL>|<NULL>|<NULL>|<NULL>|<NULL>
postgres|public|g3_items|data|7|<NULL>|YES|bytea|<NULL>|<NULL>|<NULL>|<NULL>|<NULL>|<NULL>|<NULL>|<NULL>|<NULL>|<NULL>
postgres|public|g3_items|d|8|<NULL>|YES|date|<NULL>|<NULL>|<NULL>|<NULL>|<NULL>|0|<NULL>|<NULL>|<NULL>|<NULL>
postgres|public|g3_items|r|9|<NULL>|YES|real|<NULL>|<NULL>|24|2|<NULL>|<NULL>|<NULL>|<NULL>|<NULL>|<NULL>
postgres|public|g3_items|dp|10|<NULL>|YES|double precision|<NULL>|<NULL>|53|2|<NULL>|<NULL>|<NULL>|<NULL>|<NULL>|<NULL>
postgres|public|g3_items|sm|11|<NULL>|YES|smallint|<NULL>|<NULL>|16|2|0|<NULL>|<NULL>|<NULL>|<NULL>|<NULL>
postgres|public|g3_items|t|12|<NULL>|YES|time without time zone|<NULL>|<NULL>|<NULL>|<NULL>|<NULL>|6|<NULL>|<NULL>|<NULL>|<NULL>
postgres|public|g3_items|tz|13|<NULL>|YES|time with time zone|<NULL>|<NULL>|<NULL>|<NULL>|<NULL>|6|<NULL>|<NULL>|<NULL>|<NULL>
postgres|public|g3_items|ts|14|<NULL>|YES|timestamp without time zone|<NULL>|<NULL>|<NULL>|<NULL>|<NULL>|6|<NULL>|<NULL>|<NULL>|<NULL>
postgres|public|g3_items|iv|15|<NULL>|YES|interval|<NULL>|<NULL>|<NULL>|<NULL>|<NULL>|6|<NULL>|<NULL>|<NULL>|<NULL>
(15 rows)"#;

const CLIENT_03_G3_PAIR_SQL: &str = r#"SELECT
	    table_catalog,
	    table_schema,
	    table_name,
	    column_name,
	    ordinal_position,
	    column_default,
	    is_nullable,
	    CASE WHEN udt_schema is NULL THEN udt_name ELSE format_type(typ.oid, NULL) END,
	    character_maximum_length,
	    character_octet_length,
	    numeric_precision,
	    numeric_precision_radix,
	    numeric_scale,
	    datetime_precision,
	    character_set_catalog,
	    character_set_schema,
	    character_set_name,
	    collation_catalog
	FROM information_schema.columns
	JOIN pg_namespace AS ns ON ns.nspname = udt_schema
	JOIN pg_type AS typ ON typnamespace = ns.oid AND typname = udt_name WHERE table_schema = $1 AND table_name = $2;"#;

const CLIENT_03_G3_PAIR_PSV: &str = r#"table_catalog|table_schema|table_name|column_name|ordinal_position|column_default|is_nullable|format_type|character_maximum_length|character_octet_length|numeric_precision|numeric_precision_radix|numeric_scale|datetime_precision|character_set_catalog|character_set_schema|character_set_name|collation_catalog
postgres|public|g3_pair|a|1|<NULL>|NO|integer|<NULL>|<NULL>|32|2|0|<NULL>|<NULL>|<NULL>|<NULL>|<NULL>
postgres|public|g3_pair|b|2|<NULL>|NO|text|<NULL>|1073741824|<NULL>|<NULL>|<NULL>|<NULL>|<NULL>|<NULL>|<NULL>|<NULL>
postgres|public|g3_pair|note|3|<NULL>|YES|text|<NULL>|1073741824|<NULL>|<NULL>|<NULL>|<NULL>|<NULL>|<NULL>|<NULL>|<NULL>
(3 rows)"#;

const CLIENT_04_G3_ITEMS_SQL: &str = r#"SELECT NULL AS TABLE_CAT, n.nspname AS TABLE_SCHEM, c.relname AS TABLE_NAME,  CASE n.nspname ~ '^pg_' OR n.nspname = 'information_schema'  WHEN true THEN CASE  WHEN n.nspname = 'pg_catalog' OR n.nspname = 'information_schema' THEN CASE c.relkind   WHEN 'r' THEN 'SYSTEM TABLE'   WHEN 'v' THEN 'SYSTEM VIEW'   WHEN 'i' THEN 'SYSTEM INDEX'   ELSE NULL   END  WHEN n.nspname = 'pg_toast' THEN CASE c.relkind   WHEN 'r' THEN 'SYSTEM TOAST TABLE'   WHEN 'i' THEN 'SYSTEM TOAST INDEX'   ELSE NULL   END  ELSE CASE c.relkind   WHEN 'r' THEN 'TEMPORARY TABLE'   WHEN 'p' THEN 'TEMPORARY TABLE'   WHEN 'i' THEN 'TEMPORARY INDEX'   WHEN 'S' THEN 'TEMPORARY SEQUENCE'   WHEN 'v' THEN 'TEMPORARY VIEW'   ELSE NULL   END  END  WHEN false THEN CASE c.relkind  WHEN 'r' THEN 'TABLE'  WHEN 'p' THEN 'PARTITIONED TABLE'  WHEN 'i' THEN 'INDEX'  WHEN 'P' then 'PARTITIONED INDEX'  WHEN 'S' THEN 'SEQUENCE'  WHEN 'v' THEN 'VIEW'  WHEN 'c' THEN 'TYPE'  WHEN 'f' THEN 'FOREIGN TABLE'  WHEN 'm' THEN 'MATERIALIZED VIEW'  ELSE NULL  END  ELSE NULL  END  AS TABLE_TYPE, d.description AS REMARKS,  '' as TYPE_CAT, '' as TYPE_SCHEM, '' as TYPE_NAME, '' AS SELF_REFERENCING_COL_NAME, '' AS REF_GENERATION  FROM pg_catalog.pg_namespace n, pg_catalog.pg_class c  LEFT JOIN pg_catalog.pg_description d ON (c.oid = d.objoid AND d.objsubid = 0  and d.classoid = 'pg_class'::regclass)  WHERE c.relnamespace = n.oid  AND n.nspname LIKE 'public' AND c.relname LIKE 'g3_items' AND (false  OR ( c.relkind = 'r' AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' ) )  ORDER BY TABLE_TYPE,TABLE_SCHEM,TABLE_NAME;"#;

const CLIENT_04_G3_ITEMS_PSV: &str = r#"table_cat|table_schem|table_name|table_type|remarks|type_cat|type_schem|type_name|self_referencing_col_name|ref_generation
<NULL>|public|g3_items|TABLE|<NULL>|||||
(1 row)"#;

const CLIENT_04_G3_PAIR_SQL: &str = r#"SELECT NULL AS TABLE_CAT, n.nspname AS TABLE_SCHEM, c.relname AS TABLE_NAME,  CASE n.nspname ~ '^pg_' OR n.nspname = 'information_schema'  WHEN true THEN CASE  WHEN n.nspname = 'pg_catalog' OR n.nspname = 'information_schema' THEN CASE c.relkind   WHEN 'r' THEN 'SYSTEM TABLE'   WHEN 'v' THEN 'SYSTEM VIEW'   WHEN 'i' THEN 'SYSTEM INDEX'   ELSE NULL   END  WHEN n.nspname = 'pg_toast' THEN CASE c.relkind   WHEN 'r' THEN 'SYSTEM TOAST TABLE'   WHEN 'i' THEN 'SYSTEM TOAST INDEX'   ELSE NULL   END  ELSE CASE c.relkind   WHEN 'r' THEN 'TEMPORARY TABLE'   WHEN 'p' THEN 'TEMPORARY TABLE'   WHEN 'i' THEN 'TEMPORARY INDEX'   WHEN 'S' THEN 'TEMPORARY SEQUENCE'   WHEN 'v' THEN 'TEMPORARY VIEW'   ELSE NULL   END  END  WHEN false THEN CASE c.relkind  WHEN 'r' THEN 'TABLE'  WHEN 'p' THEN 'PARTITIONED TABLE'  WHEN 'i' THEN 'INDEX'  WHEN 'P' then 'PARTITIONED INDEX'  WHEN 'S' THEN 'SEQUENCE'  WHEN 'v' THEN 'VIEW'  WHEN 'c' THEN 'TYPE'  WHEN 'f' THEN 'FOREIGN TABLE'  WHEN 'm' THEN 'MATERIALIZED VIEW'  ELSE NULL  END  ELSE NULL  END  AS TABLE_TYPE, d.description AS REMARKS,  '' as TYPE_CAT, '' as TYPE_SCHEM, '' as TYPE_NAME, '' AS SELF_REFERENCING_COL_NAME, '' AS REF_GENERATION  FROM pg_catalog.pg_namespace n, pg_catalog.pg_class c  LEFT JOIN pg_catalog.pg_description d ON (c.oid = d.objoid AND d.objsubid = 0  and d.classoid = 'pg_class'::regclass)  WHERE c.relnamespace = n.oid  AND n.nspname LIKE 'public' AND c.relname LIKE 'g3_pair' AND (false  OR ( c.relkind = 'r' AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' ) )  ORDER BY TABLE_TYPE,TABLE_SCHEM,TABLE_NAME;"#;

const CLIENT_04_G3_PAIR_PSV: &str = r#"table_cat|table_schem|table_name|table_type|remarks|type_cat|type_schem|type_name|self_referencing_col_name|ref_generation
<NULL>|public|g3_pair|TABLE|<NULL>|||||
(1 row)"#;

const CLIENT_05_G3_ITEMS_SQL: &str = r#"SELECT        result.TABLE_CAT,        result.TABLE_SCHEM,        result.TABLE_NAME,        result.COLUMN_NAME,        result.KEY_SEQ,        result.PK_NAME FROM      (SELECT NULL AS TABLE_CAT, n.nspname AS TABLE_SCHEM,   ct.relname AS TABLE_NAME, a.attname AS COLUMN_NAME,   (information_schema._pg_expandarray(i.indkey)).n AS KEY_SEQ, ci.relname AS PK_NAME,   information_schema._pg_expandarray(i.indkey) AS KEYS, a.attnum AS A_ATTNUM FROM pg_catalog.pg_class ct   JOIN pg_catalog.pg_attribute a ON (ct.oid = a.attrelid)   JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid)   JOIN pg_catalog.pg_index i ON ( a.attrelid = i.indrelid)   JOIN pg_catalog.pg_class ci ON (ci.oid = i.indexrelid) WHERE true  AND n.nspname = 'public' AND ct.relname = 'g3_items' AND i.indisprimary  ) result where  result.A_ATTNUM = (result.KEYS).x  ORDER BY result.table_name, result.pk_name, result.key_seq;"#;

const CLIENT_05_G3_ITEMS_PSV: &str = r#"table_cat|table_schem|table_name|column_name|key_seq|pk_name
<NULL>|public|g3_items|id|1|g3_items_pkey
(1 row)"#;

const CLIENT_05_G3_PAIR_SQL: &str = r#"SELECT        result.TABLE_CAT,        result.TABLE_SCHEM,        result.TABLE_NAME,        result.COLUMN_NAME,        result.KEY_SEQ,        result.PK_NAME FROM      (SELECT NULL AS TABLE_CAT, n.nspname AS TABLE_SCHEM,   ct.relname AS TABLE_NAME, a.attname AS COLUMN_NAME,   (information_schema._pg_expandarray(i.indkey)).n AS KEY_SEQ, ci.relname AS PK_NAME,   information_schema._pg_expandarray(i.indkey) AS KEYS, a.attnum AS A_ATTNUM FROM pg_catalog.pg_class ct   JOIN pg_catalog.pg_attribute a ON (ct.oid = a.attrelid)   JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid)   JOIN pg_catalog.pg_index i ON ( a.attrelid = i.indrelid)   JOIN pg_catalog.pg_class ci ON (ci.oid = i.indexrelid) WHERE true  AND n.nspname = 'public' AND ct.relname = 'g3_pair' AND i.indisprimary  ) result where  result.A_ATTNUM = (result.KEYS).x  ORDER BY result.table_name, result.pk_name, result.key_seq;"#;

const CLIENT_05_G3_PAIR_PSV: &str = r#"table_cat|table_schem|table_name|column_name|key_seq|pk_name
<NULL>|public|g3_pair|a|1|g3_pair_pkey
<NULL>|public|g3_pair|b|2|g3_pair_pkey
(2 rows)"#;

const CLIENT_07_G3_ITEMS_SQL: &str = r#"SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = 'g3_items' ORDER BY ordinal_position;"#;

const CLIENT_07_G3_ITEMS_PSV: &str = r#"column_name|data_type|is_nullable
id|integer|NO
name|text|NO
qty|bigint|YES
price|numeric|YES
created|timestamp with time zone|YES
flag|boolean|YES
data|bytea|YES
d|date|YES
r|real|YES
dp|double precision|YES
sm|smallint|YES
t|time without time zone|YES
tz|time with time zone|YES
ts|timestamp without time zone|YES
iv|interval|YES
(15 rows)"#;

const CLIENT_07_G3_PAIR_SQL: &str = r#"SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = 'g3_pair' ORDER BY ordinal_position;"#;

const CLIENT_07_G3_PAIR_PSV: &str = r#"column_name|data_type|is_nullable
a|integer|NO
b|text|NO
note|text|YES
(3 rows)"#;

const CLIENT_06_SQL: &str =
    r#"CREATE TABLE r0c_information_schema_columns_t (id int PRIMARY KEY, label text);"#;

fn client_rows(text: &str, fields: &[Field]) -> Vec<Vec<Value>> {
    let header: Vec<_> = text.lines().next().unwrap().split('|').collect();
    assert_eq!(
        fields.iter().map(|f| f.name.as_str()).collect::<Vec<_>>(),
        header
    );
    text.lines()
        .skip(1)
        .filter(|line| !line.starts_with('('))
        .map(|line| {
            let cells: Vec<_> = line.split('|').collect();
            assert_eq!(cells.len(), fields.len());
            cells
                .into_iter()
                .zip(fields)
                .map(|(cell, field)| {
                    if cell == "<NULL>" {
                        Value::Null
                    } else {
                        match field.data_type {
                            PgType::Int2 | PgType::Int4 | PgType::Int8 | PgType::Oid => {
                                Value::Int(cell.parse().unwrap())
                            }
                            PgType::Bool => Value::Bool(cell == "t"),
                            _ => Value::Text(cell.into()),
                        }
                    }
                })
                .collect()
        })
        .collect()
}

fn assert_client_outcomes(snapshot: &CatalogSnapshot) {
    for (number, table, sql, golden) in [
        (
            1,
            "g3_items",
            CLIENT_01_G3_ITEMS_SQL,
            CLIENT_01_G3_ITEMS_PSV,
        ),
        (1, "g3_pair", CLIENT_01_G3_PAIR_SQL, CLIENT_01_G3_PAIR_PSV),
        (
            2,
            "g3_items",
            CLIENT_02_G3_ITEMS_SQL,
            CLIENT_02_G3_ITEMS_PSV,
        ),
        (2, "g3_pair", CLIENT_02_G3_PAIR_SQL, CLIENT_02_G3_PAIR_PSV),
        (
            3,
            "g3_items",
            CLIENT_03_G3_ITEMS_SQL,
            CLIENT_03_G3_ITEMS_PSV,
        ),
        (3, "g3_pair", CLIENT_03_G3_PAIR_SQL, CLIENT_03_G3_PAIR_PSV),
        (
            4,
            "g3_items",
            CLIENT_04_G3_ITEMS_SQL,
            CLIENT_04_G3_ITEMS_PSV,
        ),
        (4, "g3_pair", CLIENT_04_G3_PAIR_SQL, CLIENT_04_G3_PAIR_PSV),
        (
            5,
            "g3_items",
            CLIENT_05_G3_ITEMS_SQL,
            CLIENT_05_G3_ITEMS_PSV,
        ),
        (5, "g3_pair", CLIENT_05_G3_PAIR_SQL, CLIENT_05_G3_PAIR_PSV),
        (
            7,
            "g3_items",
            CLIENT_07_G3_ITEMS_SQL,
            CLIENT_07_G3_ITEMS_PSV,
        ),
        (7, "g3_pair", CLIENT_07_G3_PAIR_SQL, CLIENT_07_G3_PAIR_PSV),
    ] {
        let params = match number {
            2 => vec![
                Value::Text("public".into()),
                Value::Text(table.into()),
                Value::Text("BASE TABLE".into()),
            ],
            3 => vec![Value::Text("public".into()), Value::Text(table.into())],
            _ => vec![],
        };
        let plan = plan(sql).expect(sql).expect("client catalog query");
        let result = execute(&plan, snapshot, &params).expect(sql);
        assert_eq!(describe(&plan).unwrap(), result.fields);
        assert_eq!(
            result.rows,
            client_rows(golden, &result.fields),
            "client {number}, {table}"
        );
    }
    // PostgreSQL's sixth outcome is CREATE TABLE. Ordinary application DDL
    // falls back to the caller through both metadata planning entry points.
    assert!(matches!(plan(CLIENT_06_SQL), Ok(None)));
    assert!(matches!(plan_gateway(CLIENT_06_SQL), Ok(None)));
}

#[test]
fn seven_client_outcomes_match_pg18_metadata_and_preserve_ordinary_ddl_fallback() {
    assert_client_outcomes(&snapshot());
}

#[test]
fn seven_client_outcomes_remain_bounded_at_128_tables() {
    let mut snapshot = snapshot();
    for id in 3..=128 {
        snapshot.tables.push(TableDef {
            id,
            name: format!("scale_{id}"),
            columns: vec![
                ColumnDef {
                    id: 1,
                    name: "id".into(),
                    data_type: ScalarType::Integer,
                    not_null: true,
                },
                ColumnDef {
                    id: 2,
                    name: "value".into(),
                    data_type: ScalarType::Text,
                    not_null: false,
                },
            ],
            primary_key: vec![0],
        });
    }
    assert_eq!(snapshot.tables.len(), 128);
    assert_client_outcomes(&snapshot);
}

#[test]
fn qualified_regclass_respects_catalog_namespaces() {
    assert_eq!(
        run(
            &snapshot(),
            "SELECT 'information_schema.columns'::regclass::oid"
        )
        .rows,
        vec![vec![Value::Int(13381)]]
    );
    for (view, oid) in [
        ("schemata", 13467),
        ("tables", 13510),
        ("columns", 13381),
        ("table_constraints", 13496),
        ("key_column_usage", 13414),
    ] {
        for name in [
            format!("information_schema.{view}"),
            format!(" INFORMATION_SCHEMA . {} ", view.to_ascii_uppercase()),
            format!("\"information_schema\".\"{view}\""),
            format!("information_schema.\"{view}\""),
            format!("\"information_schema\".{view}"),
        ] {
            assert_eq!(
                run(&snapshot(), &format!("SELECT '{name}'::regclass::oid")).rows,
                vec![vec![Value::Int(oid)]],
                "{name}"
            );
        }
        for name in [
            view.to_string(),
            format!("\"{view}\""),
            format!("pg_catalog.{view}"),
            format!("public.{view}"),
            format!("\"INFORMATION_SCHEMA\".{view}"),
            format!("information_schema.\"{}\"", view.to_ascii_uppercase()),
        ] {
            let sql = format!("SELECT '{name}'::regclass");
            let result = plan(&sql).and_then(|p| execute(&p.unwrap(), &snapshot(), &[]));
            assert!(result.is_err(), "{sql}");
        }
    }
    for sql in [
        "SELECT * FROM pg_catalog.columns",
        "SELECT * FROM information_schema.pg_class",
    ] {
        assert!(matches!(plan(sql), Err(Error::Unsupported)), "{sql}");
    }
    for sql in [
        "SELECT 'pg_catalog.columns'::regclass",
        "SELECT 'information_schema.pg_class'::regclass",
        "SELECT 'information_schema.''columns'''::regclass",
        "SELECT 'information_schema.`columns`'::regclass",
        "SELECT 'information_schema.[columns]'::regclass",
    ] {
        let result = plan(sql).and_then(|p| execute(&p.unwrap(), &snapshot(), &[]));
        assert!(result.is_err(), "{sql}");
    }
}

#[test]
fn internal_name_and_char_columns_have_no_character_length_metadata() {
    // PostgreSQL 18 reports NULL lengths for internal name and "char" types.
    let result = run(
        &snapshot(),
        "SELECT column_name, character_maximum_length, character_octet_length FROM information_schema.columns WHERE table_schema = 'pg_catalog' AND table_name = 'pg_class' AND column_name IN ('relname', 'relkind') ORDER BY column_name",
    );
    assert_eq!(
        result.rows,
        vec![
            vec![Value::Text("relkind".into()), Value::Null, Value::Null],
            vec![Value::Text("relname".into()), Value::Null, Value::Null],
        ]
    );
}

#[test]
fn gateway_routes_only_qualified_information_schema_views() {
    for view in [
        "schemata",
        "tables",
        "columns",
        "table_constraints",
        "key_column_usage",
    ] {
        for name in [
            format!("information_schema.{view}"),
            format!("INFORMATION_SCHEMA.{}", view.to_ascii_uppercase()),
            format!("\"information_schema\".\"{view}\""),
            format!("information_schema.\"{view}\""),
            format!("\"information_schema\".{view}"),
        ] {
            let sql = format!("SELECT * FROM {name} LIMIT 0");
            assert!(matches!(plan_gateway(&sql), Ok(Some(_))), "{sql}");
            assert!(matches!(plan(&sql), Ok(Some(_))), "{sql}");
        }
    }
    assert!(matches!(
        plan_gateway("SELECT * FROM pg_class LIMIT 0"),
        Ok(Some(_))
    ));
    assert!(matches!(
        plan_gateway("SELECT * FROM pg_catalog.columns"),
        Err(Error::Unsupported)
    ));
}

#[test]
fn unqualified_application_names_mix_with_catalog_relations() {
    for name in ["tables", "columns"] {
        for sql in [
            format!("SELECT * FROM information_schema.columns c JOIN {name} a ON true"),
            format!("SELECT * FROM information_schema.columns WHERE EXISTS (SELECT 1 FROM {name})"),
            format!(
                "SELECT 1 FROM information_schema.columns UNION ALL SELECT 1 FROM {name} UNION ALL SELECT 1 FROM information_schema.tables"
            ),
        ] {
            assert!(matches!(plan(&sql), Err(Error::MixedRelations)), "{sql}");
            assert!(
                matches!(plan_gateway(&sql), Err(Error::MixedRelations)),
                "{sql}"
            );
        }
        let projection = std::iter::repeat_n("1 + 1", 100)
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT {projection} FROM information_schema.columns UNION ALL SELECT {projection} FROM {name} UNION ALL SELECT {projection} FROM information_schema.tables"
        );
        assert!(matches!(plan(&sql), Err(Error::MixedRelations)), "{sql}");
        assert!(
            matches!(plan_gateway(&sql), Err(Error::MixedRelations)),
            "{sql}"
        );
        let sql = format!("WITH {name} AS (SELECT oid FROM pg_class) SELECT * FROM {name}");
        assert!(matches!(plan(&sql), Ok(Some(_))), "{sql}");
        assert!(matches!(plan_gateway(&sql), Ok(Some(_))), "{sql}");
    }
}

#[test]
fn inventory_and_columns_cover_every_modeled_table_and_view() {
    let snapshot = snapshot();
    let expected = run(
        &snapshot,
        "SELECT n.nspname, c.relname, CASE WHEN c.relkind = 'v' THEN 'VIEW' ELSE 'BASE TABLE' END AS table_type FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid WHERE c.relkind IN ('r', 'v') ORDER BY n.nspname, c.relname",
    );
    let actual = run(
        &snapshot,
        "SELECT table_schema, table_name, table_type FROM information_schema.tables ORDER BY table_schema, table_name",
    );
    assert_eq!(actual.rows, expected.rows);

    let expected = run(
        &snapshot,
        "SELECT n.nspname, c.relname, a.attname, a.attnum FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid JOIN pg_attribute a ON a.attrelid = c.oid WHERE c.relkind IN ('r', 'v') AND a.attnum > 0 ORDER BY n.nspname, c.relname, a.attnum",
    );
    let actual = run(
        &snapshot,
        "SELECT table_schema, table_name, column_name, ordinal_position FROM information_schema.columns ORDER BY table_schema, table_name, ordinal_position",
    );
    assert_eq!(actual.rows, expected.rows);

    let view_columns = run(
        &snapshot,
        "SELECT is_updatable FROM information_schema.columns WHERE table_schema = 'information_schema'",
    );
    assert_eq!(view_columns.rows.len(), 83);
    assert!(
        view_columns
            .rows
            .iter()
            .all(|row| row == &vec![Value::Text("NO".into())])
    );
}

#[test]
fn effective_primary_key_not_null_is_reported_without_pg_constraint_check_rows() {
    let mut snapshot = snapshot();
    snapshot.tables[0].columns[0].not_null = false;
    assert_eq!(run(&snapshot, "SELECT is_nullable FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'g3_items' AND column_name = 'id'").rows, vec![vec![Value::Text("NO".into())]]);
    assert_eq!(run(&snapshot, "SELECT constraint_type FROM information_schema.table_constraints WHERE constraint_schema = 'public' AND constraint_name = 'g3_items_id_not_null'").rows, vec![vec![Value::Text("CHECK".into())]]);
    // Synthetic information_schema CHECK rows are presentation metadata only.
    assert_eq!(run(&snapshot, "SELECT c.conname, c.contype FROM pg_constraint c JOIN pg_namespace n ON n.oid = c.connamespace WHERE n.nspname = 'public' ORDER BY c.conname").rows, vec![
        vec![Value::Text("g3_items_pkey".into()), Value::Text("p".into())],
        vec![Value::Text("g3_pair_pkey".into()), Value::Text("p".into())],
    ]);
}

#[test]
fn modeled_internal_type_metadata_matches_pg18_vector_and_builtin_names() {
    // PG18 vectors are ARRAY despite their space-separated catalog encoding;
    // builtin regproc and pg_node_tree retain their PostgreSQL type names.
    let result = run(
        &snapshot(),
        "SELECT table_name, column_name, data_type, udt_name FROM information_schema.columns WHERE table_schema = 'pg_catalog' AND ((table_name = 'pg_index' AND column_name IN ('indkey', 'indclass')) OR (table_name = 'pg_am' AND column_name = 'amhandler') OR (table_name = 'pg_rewrite' AND column_name = 'ev_action')) ORDER BY table_name, column_name",
    );
    let expected = [
        ["pg_am", "amhandler", "regproc", "regproc"],
        ["pg_index", "indclass", "ARRAY", "oidvector"],
        ["pg_index", "indkey", "ARRAY", "int2vector"],
        ["pg_rewrite", "ev_action", "pg_node_tree", "pg_node_tree"],
    ]
    .into_iter()
    .map(|row| {
        row.into_iter()
            .map(|cell| Value::Text(cell.into()))
            .collect::<Vec<_>>()
    })
    .collect::<Vec<_>>();
    assert_eq!(result.rows, expected);
}
