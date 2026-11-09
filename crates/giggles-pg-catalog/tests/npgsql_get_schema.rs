// Verbatim Npgsql 9.0.4 NpgsqlSchema GetTables/GetColumns SQL (restrictions
// rewritten from `:name` to `$n`), as sent by GetSchema("Tables"/"Columns").
use giggles_pg_catalog::*;

const TABLES_SQL: &str = "SELECT table_catalog, table_schema, table_name, table_type\nFROM information_schema.tables\nWHERE\n    table_type IN ('BASE TABLE', 'FOREIGN', 'FOREIGN TABLE') AND\n    table_schema NOT IN ('pg_catalog', 'information_schema') AND table_schema = $1 AND table_name = $2 AND table_type = $3";

const COLUMNS_SQL: &str = "SELECT\n    table_catalog,\n    table_schema,\n    table_name,\n    column_name,\n    ordinal_position,\n    column_default,\n    is_nullable,\n    CASE WHEN udt_schema is NULL THEN udt_name ELSE format_type(typ.oid, NULL) END,\n    character_maximum_length,\n    character_octet_length,\n    numeric_precision,\n    numeric_precision_radix,\n    numeric_scale,\n    datetime_precision,\n    character_set_catalog,\n    character_set_schema,\n    character_set_name,\n    collation_catalog\nFROM information_schema.columns\nJOIN pg_namespace AS ns ON ns.nspname = udt_schema\nJOIN pg_type AS typ ON typnamespace = ns.oid AND typname = udt_name\n WHERE table_schema = $1 AND table_name = $2";

fn snapshot() -> CatalogSnapshot {
    CatalogSnapshot {
        tables: vec![TableDef {
            id: 7,
            name: "items".into(),
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
                ColumnDef {
                    id: 3,
                    name: "score".into(),
                    data_type: ScalarType::BigInt,
                    not_null: false,
                },
            ],
            primary_key: vec![0],
        }],
        ..CatalogSnapshot::default()
    }
}

fn run(sql: &str, params: &[&str]) -> ResultSet {
    let p = plan_gateway(sql)
        .unwrap_or_else(|e| panic!("plan {e:?}"))
        .expect("catalog statement");
    let params: Vec<Value> = params.iter().map(|v| Value::Text((*v).into())).collect();
    execute(&p, &snapshot(), &params).unwrap_or_else(|e| panic!("execute {e:?}"))
}

fn t(v: &str) -> Value {
    Value::Text(v.into())
}

/// The catalog infers `name` for these restrictions; the server accepts
/// Npgsql's declared text OIDs over them (see giggles-server pgwire).
#[test]
fn npgsql_9_0_4_parameters_infer_name() {
    let p = plan_gateway(COLUMNS_SQL).unwrap().unwrap();
    assert!(p.allows_numeric_input_override());
    assert_eq!(
        p.parameter_types()
            .iter()
            .map(|t| t.oid())
            .collect::<Vec<_>>(),
        [19, 19]
    );
}

#[test]
fn npgsql_9_0_4_get_schema_tables() {
    let r = run(TABLES_SQL, &["public", "items", "BASE TABLE"]);
    assert_eq!(
        r.rows,
        [vec![t("giggles"), t("public"), t("items"), t("BASE TABLE")]]
    );
}

#[test]
fn npgsql_9_0_4_get_schema_columns() {
    let r = run(COLUMNS_SQL, &["public", "items"]);
    let observed: Vec<_> = r
        .rows
        .iter()
        .map(|row| {
            (
                row[3].clone(),
                row[4].clone(),
                row[6].clone(),
                row[7].clone(),
            )
        })
        .collect();
    assert_eq!(
        observed,
        [
            (t("id"), Value::Int(1), t("NO"), t("integer")),
            (t("name"), Value::Int(2), t("YES"), t("text")),
            (t("score"), Value::Int(3), t("YES"), t("bigint")),
        ]
    );
}
