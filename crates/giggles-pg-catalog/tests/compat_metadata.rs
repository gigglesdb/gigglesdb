use giggles_pg_catalog::*;
fn run(sql: &str) -> ResultSet {
    execute(
        &plan(sql).unwrap().unwrap(),
        &CatalogSnapshot::default(),
        &[],
    )
    .unwrap()
}
#[test]
fn catalog_count_empty_filtered_and_grouped() {
    for (sql, expected) in [
        (
            "SELECT count(*) FROM pg_catalog.pg_class WHERE relname = 'r0_drop_table'",
            0,
        ),
        (
            "SELECT count(*) FROM pg_catalog.pg_namespace WHERE oid IN (11,2200)",
            2,
        ),
        ("SELECT count(NULL::text) FROM pg_catalog.pg_namespace", 0),
        (
            "SELECT count(oid) FROM pg_catalog.pg_namespace WHERE oid=11",
            1,
        ),
    ] {
        let result = run(sql);
        assert_eq!(result.fields[0].data_type, PgType::Int8);
        assert_eq!(result.rows, vec![vec![Value::Int(expected)]], "{sql}");
    }
    assert!(
        run("SELECT oid,count(*) FROM pg_catalog.pg_namespace WHERE false GROUP BY oid")
            .rows
            .is_empty()
    );
    assert_eq!(
        run("SELECT oid,count(*) FROM pg_catalog.pg_namespace WHERE oid=11 GROUP BY oid").rows,
        vec![vec![Value::Int(11), Value::Int(1)]]
    );
    for sql in [
        "SELECT count(count(*)) FROM pg_catalog.pg_class",
        "SELECT count(DISTINCT oid) FROM pg_catalog.pg_class",
        "SELECT count(*) OVER () FROM pg_catalog.pg_class",
    ] {
        assert!(plan(sql).is_err(), "{sql}");
    }
}
// Captured from PostgreSQL 18.6 pg_attribute joined to pg_type, resolving typbasetype.
const PG18: &str = r#"columns|table_catalog|19
columns|table_schema|19
columns|table_name|19
columns|column_name|19
columns|ordinal_position|23
columns|column_default|1043
columns|is_nullable|1043
columns|data_type|1043
columns|character_maximum_length|23
columns|character_octet_length|23
columns|numeric_precision|23
columns|numeric_precision_radix|23
columns|numeric_scale|23
columns|datetime_precision|23
columns|interval_type|1043
columns|interval_precision|23
columns|character_set_catalog|19
columns|character_set_schema|19
columns|character_set_name|19
columns|collation_catalog|19
columns|collation_schema|19
columns|collation_name|19
columns|domain_catalog|19
columns|domain_schema|19
columns|domain_name|19
columns|udt_catalog|19
columns|udt_schema|19
columns|udt_name|19
columns|scope_catalog|19
columns|scope_schema|19
columns|scope_name|19
columns|maximum_cardinality|23
columns|dtd_identifier|19
columns|is_self_referencing|1043
columns|is_identity|1043
columns|identity_generation|1043
columns|identity_start|1043
columns|identity_increment|1043
columns|identity_maximum|1043
columns|identity_minimum|1043
columns|identity_cycle|1043
columns|is_generated|1043
columns|generation_expression|1043
columns|is_updatable|1043
key_column_usage|constraint_catalog|19
key_column_usage|constraint_schema|19
key_column_usage|constraint_name|19
key_column_usage|table_catalog|19
key_column_usage|table_schema|19
key_column_usage|table_name|19
key_column_usage|column_name|19
key_column_usage|ordinal_position|23
key_column_usage|position_in_unique_constraint|23
schemata|catalog_name|19
schemata|schema_name|19
schemata|schema_owner|19
schemata|default_character_set_catalog|19
schemata|default_character_set_schema|19
schemata|default_character_set_name|19
schemata|sql_path|1043
table_constraints|constraint_catalog|19
table_constraints|constraint_schema|19
table_constraints|constraint_name|19
table_constraints|table_catalog|19
table_constraints|table_schema|19
table_constraints|table_name|19
table_constraints|constraint_type|1043
table_constraints|is_deferrable|1043
table_constraints|initially_deferred|1043
table_constraints|enforced|1043
table_constraints|nulls_distinct|1043
tables|table_catalog|19
tables|table_schema|19
tables|table_name|19
tables|table_type|1043
tables|self_referencing_column_name|19
tables|reference_generation|1043
tables|user_defined_type_catalog|19
tables|user_defined_type_schema|19
tables|user_defined_type_name|19
tables|is_insertable_into|1043
tables|is_typed|1043
tables|commit_action|1043
"#;
#[test]
fn information_schema_domain_base_descriptors() {
    for relation in [
        "columns",
        "tables",
        "table_constraints",
        "key_column_usage",
        "schemata",
    ] {
        let result = run(&format!(
            "SELECT * FROM information_schema.{relation} WHERE false"
        ));
        let expected: Vec<_> = PG18
            .lines()
            .filter_map(|line| {
                let parts: Vec<_> = line.split('|').collect();
                (parts[0] == relation).then(|| (parts[1], parts[2].parse::<u32>().unwrap()))
            })
            .collect();
        let actual: Vec<_> = result
            .fields
            .iter()
            .map(|f| (f.name.as_str(), f.data_type.oid()))
            .collect();
        assert_eq!(actual, expected, "{relation}");
    }
}
