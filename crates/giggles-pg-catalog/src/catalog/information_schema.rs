use super::*;

// PostgreSQL 18.6 pg_namespace/pg_class capture: namespace 13293, owner 10;
// each pair is the view relation OID and its actual composite rowtype OID.
pub(super) fn identity(name: &str) -> Option<(u32, u32)> {
    Some(match name {
        "columns" => (13381, 13383),
        "key_column_usage" => (13414, 13416),
        "schemata" => (13467, 13469),
        "table_constraints" => (13496, 13498),
        "tables" => (13510, 13512),
        _ => return None,
    })
}

/// Domain descriptors use Name for sql_identifier and Int4 for cardinal_number.
/// PostgreSQL character_data/yes_or_no have varchar bases; without a varchar
/// PgType, these use Text. Domain OIDs are not advertised because domain
/// coercion is not implemented.
pub(super) fn schema(name: &str) -> Option<Vec<Field>> {
    Some(fields(match name {
        "tables" => &[
            ("table_catalog", T::Name),
            ("table_schema", T::Name),
            ("table_name", T::Name),
            ("table_type", T::Text),
            ("self_referencing_column_name", T::Name),
            ("reference_generation", T::Text),
            ("user_defined_type_catalog", T::Name),
            ("user_defined_type_schema", T::Name),
            ("user_defined_type_name", T::Name),
            ("is_insertable_into", T::Text),
            ("is_typed", T::Text),
            ("commit_action", T::Text),
        ],
        "columns" => &[
            ("table_catalog", T::Name),
            ("table_schema", T::Name),
            ("table_name", T::Name),
            ("column_name", T::Name),
            ("ordinal_position", T::Int4),
            ("column_default", T::Text),
            ("is_nullable", T::Text),
            ("data_type", T::Text),
            ("character_maximum_length", T::Int4),
            ("character_octet_length", T::Int4),
            ("numeric_precision", T::Int4),
            ("numeric_precision_radix", T::Int4),
            ("numeric_scale", T::Int4),
            ("datetime_precision", T::Int4),
            ("interval_type", T::Text),
            ("interval_precision", T::Int4),
            ("character_set_catalog", T::Name),
            ("character_set_schema", T::Name),
            ("character_set_name", T::Name),
            ("collation_catalog", T::Name),
            ("collation_schema", T::Name),
            ("collation_name", T::Name),
            ("domain_catalog", T::Name),
            ("domain_schema", T::Name),
            ("domain_name", T::Name),
            ("udt_catalog", T::Name),
            ("udt_schema", T::Name),
            ("udt_name", T::Name),
            ("scope_catalog", T::Name),
            ("scope_schema", T::Name),
            ("scope_name", T::Name),
            ("maximum_cardinality", T::Int4),
            ("dtd_identifier", T::Name),
            ("is_self_referencing", T::Text),
            ("is_identity", T::Text),
            ("identity_generation", T::Text),
            ("identity_start", T::Text),
            ("identity_increment", T::Text),
            ("identity_maximum", T::Text),
            ("identity_minimum", T::Text),
            ("identity_cycle", T::Text),
            ("is_generated", T::Text),
            ("generation_expression", T::Text),
            ("is_updatable", T::Text),
        ],
        "table_constraints" => &[
            ("constraint_catalog", T::Name),
            ("constraint_schema", T::Name),
            ("constraint_name", T::Name),
            ("table_catalog", T::Name),
            ("table_schema", T::Name),
            ("table_name", T::Name),
            ("constraint_type", T::Text),
            ("is_deferrable", T::Text),
            ("initially_deferred", T::Text),
            ("enforced", T::Text),
            ("nulls_distinct", T::Text),
        ],
        "key_column_usage" => &[
            ("constraint_catalog", T::Name),
            ("constraint_schema", T::Name),
            ("constraint_name", T::Name),
            ("table_catalog", T::Name),
            ("table_schema", T::Name),
            ("table_name", T::Name),
            ("column_name", T::Name),
            ("ordinal_position", T::Int4),
            ("position_in_unique_constraint", T::Int4),
        ],
        "schemata" => &[
            ("catalog_name", T::Name),
            ("schema_name", T::Name),
            ("schema_owner", T::Name),
            ("default_character_set_catalog", T::Name),
            ("default_character_set_schema", T::Name),
            ("default_character_set_name", T::Name),
            ("sql_path", T::Text),
        ],
        _ => return None,
    }))
}

pub(super) fn populate(
    snapshot: &CatalogSnapshot,
    key_names: &BTreeMap<u64, String>,
    all: &mut BTreeMap<String, Relation>,
) -> R<()> {
    // The finite catalog presents bootstrap-superuser visibility; there is no
    // privilege filtering until an authorization model exists.
    let mut generated: BTreeMap<&str, Vec<Vec<Value>>> = [
        ("schemata", vec![]),
        ("tables", vec![]),
        ("columns", vec![]),
        ("table_constraints", vec![]),
        ("key_column_usage", vec![]),
    ]
    .into_iter()
    .collect();
    for row in &all["pg_namespace"].rows {
        let I(owner) = row[2] else {
            return Err(Error::InvalidSnapshot);
        };
        generated.get_mut("schemata").unwrap().push(vec![
            s(&snapshot.database),
            row[1].clone(),
            s(role_name(snapshot, owner).unwrap()),
            N,
            N,
            N,
            N,
        ]);
    }
    for relation in &all["pg_class"].rows {
        if !matches!(&relation[3], Value::Text(kind) if kind == "r" || kind == "v") {
            continue;
        }
        let namespace = if relation[2] == I(13293) {
            "information_schema"
        } else if relation[2] == I(2200) {
            "public"
        } else {
            "pg_catalog"
        };
        let view = relation[3] == s("v");
        generated.get_mut("tables").unwrap().push(vec![
            s(&snapshot.database),
            s(namespace),
            relation[1].clone(),
            s(if view { "VIEW" } else { "BASE TABLE" }),
            N,
            N,
            N,
            N,
            N,
            s(if view { "NO" } else { "YES" }),
            s("NO"),
            N,
        ]);
        for attribute in &all["pg_attribute"].rows {
            if attribute[0] != relation[0] {
                continue;
            }
            let (I(oid), I(position)) = (&attribute[2], &attribute[3]) else {
                return Err(Error::InvalidSnapshot);
            };
            if *position <= 0 {
                continue;
            }
            generated.get_mut("columns").unwrap().push(column_row(
                snapshot,
                namespace,
                &relation[1],
                &attribute[1],
                *position,
                *oid as u32,
                attribute[4] == B(true),
                !view,
                all,
            ));
        }
    }
    for table in &snapshot.tables {
        let mut constraint = |name: &str, kind: &str| {
            generated.get_mut("table_constraints").unwrap().push(vec![
                s(&snapshot.database),
                s("public"),
                s(name),
                s(&snapshot.database),
                s("public"),
                s(&table.name),
                s(kind),
                s("NO"),
                s("NO"),
                s("YES"),
                N,
            ]);
        };
        for (index, column) in table.columns.iter().enumerate() {
            if column.not_null || table.primary_key.contains(&index) {
                constraint(&format!("{}_{}_not_null", table.name, column.name), "CHECK");
            }
        }
        if !table.primary_key.is_empty() {
            constraint(&key_names[&table.id], "PRIMARY KEY");
            for (position, index) in table.primary_key.iter().enumerate() {
                generated.get_mut("key_column_usage").unwrap().push(vec![
                    s(&snapshot.database),
                    s("public"),
                    s(&key_names[&table.id]),
                    s(&snapshot.database),
                    s("public"),
                    s(&table.name),
                    s(&table.columns[*index].name),
                    I(position as i64 + 1),
                    N,
                ]);
            }
        }
    }
    for (name, rows) in generated {
        all.get_mut(name).unwrap().rows = rows;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn column_row(
    snapshot: &CatalogSnapshot,
    namespace: &str,
    table: &Value,
    column: &Value,
    position: i64,
    oid: u32,
    not_null: bool,
    updatable: bool,
    all: &BTreeMap<String, Relation>,
) -> Vec<Value> {
    let mut row = vec![N; 44];
    row[0] = s(&snapshot.database);
    row[1] = s(namespace);
    row[2] = table.clone();
    row[3] = column.clone();
    row[4] = I(position);
    row[6] = s(if not_null { "NO" } else { "YES" });
    let type_row = all["pg_type"]
        .rows
        .iter()
        .find(|r| r[0] == I(i64::from(oid)));
    let udt = type_row
        .map(|r| r[1].clone())
        .unwrap_or_else(|| s("record"));
    // PG18 uses element presence and variable length, including vector types.
    let array = type_row.is_some_and(|r| r[10] != I(0) && r[3] == I(-1));
    let builtin = type_row.is_some_and(|r| r[2] == I(11));
    let data_type = match oid {
        16 => "boolean",
        17 => "bytea",
        18 => "\"char\"",
        19 => "name",
        20 => "bigint",
        21 => "smallint",
        23 => "integer",
        25 => "text",
        26 => "oid",
        700 => "real",
        701 => "double precision",
        1700 => "numeric",
        1082 => "date",
        1083 => "time without time zone",
        1266 => "time with time zone",
        1114 => "timestamp without time zone",
        1184 => "timestamp with time zone",
        1186 => "interval",
        _ if array => "ARRAY",
        _ if builtin => match &udt {
            Value::Text(name) => name.as_str(),
            _ => "USER-DEFINED",
        },
        _ => "USER-DEFINED",
    };
    row[7] = s(data_type);
    if oid == 25 {
        row[9] = I(1073741824);
    }
    match oid {
        21 | 23 | 20 => {
            row[10] = I(match oid {
                21 => 16,
                23 => 32,
                _ => 64,
            });
            row[11] = I(2);
            row[12] = I(0);
        }
        700 | 701 => {
            row[10] = I(if oid == 700 { 24 } else { 53 });
            row[11] = I(2);
        }
        1700 => row[11] = I(10),
        1082 => row[13] = I(0),
        1083 | 1266 | 1114 | 1184 | 1186 => row[13] = I(6),
        _ => {}
    }
    row[25] = s(&snapshot.database);
    row[26] = s("pg_catalog");
    row[27] = udt;
    row[32] = s(&position.to_string());
    row[33] = s("NO");
    row[34] = s("NO");
    row[40] = s("NO");
    row[41] = s("NEVER");
    row[43] = s(if updatable { "YES" } else { "NO" });
    row
}
