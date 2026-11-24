use super::*;

// PostgreSQL 18.6 pg_namespace/pg_class capture: namespace 13293, owner 10;
// each pair is the view relation OID and its actual composite rowtype OID.
pub(super) fn identity(name: &str) -> Option<(u32, u32)> {
    Some(match name {
        "views" => (13568, 13570),
        "columns" => (13381, 13383),
        "key_column_usage" => (13414, 13416),
        "schemata" => (13467, 13469),
        "table_constraints" => (13496, 13498),
        "tables" => (13510, 13512),
        "triggers" => (13525, 13527),
        "routines" => (13462, 13464),
        _ => return None,
    })
}

/// RowDescription exposes the PG18 domain base types: name for sql_identifier,
/// int4 for cardinal_number, and varchar for character_data/yes_or_no.
pub(super) fn schema(name: &str) -> Option<Vec<Field>> {
    if let Some(fields) = super::routines::information_schema(name) { return Some(fields); }
    Some(fields(match name {
        "views" => &[("table_catalog", T::Name), ("table_schema", T::Name), ("table_name", T::Name),
            ("view_definition", T::Varchar), ("check_option", T::Varchar), ("is_updatable", T::Varchar),
            ("is_insertable_into", T::Varchar), ("is_trigger_updatable", T::Varchar),
            ("is_trigger_deletable", T::Varchar), ("is_trigger_insertable_into", T::Varchar)],
        "tables" => &[
            ("table_catalog", T::Name),
            ("table_schema", T::Name),
            ("table_name", T::Name),
            ("table_type", T::Varchar),
            ("self_referencing_column_name", T::Name),
            ("reference_generation", T::Varchar),
            ("user_defined_type_catalog", T::Name),
            ("user_defined_type_schema", T::Name),
            ("user_defined_type_name", T::Name),
            ("is_insertable_into", T::Varchar),
            ("is_typed", T::Varchar),
            ("commit_action", T::Varchar),
        ],
        "columns" => &[
            ("table_catalog", T::Name),
            ("table_schema", T::Name),
            ("table_name", T::Name),
            ("column_name", T::Name),
            ("ordinal_position", T::Int4),
            ("column_default", T::Varchar),
            ("is_nullable", T::Varchar),
            ("data_type", T::Varchar),
            ("character_maximum_length", T::Int4),
            ("character_octet_length", T::Int4),
            ("numeric_precision", T::Int4),
            ("numeric_precision_radix", T::Int4),
            ("numeric_scale", T::Int4),
            ("datetime_precision", T::Int4),
            ("interval_type", T::Varchar),
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
            ("is_self_referencing", T::Varchar),
            ("is_identity", T::Varchar),
            ("identity_generation", T::Varchar),
            ("identity_start", T::Varchar),
            ("identity_increment", T::Varchar),
            ("identity_maximum", T::Varchar),
            ("identity_minimum", T::Varchar),
            ("identity_cycle", T::Varchar),
            ("is_generated", T::Varchar),
            ("generation_expression", T::Varchar),
            ("is_updatable", T::Varchar),
        ],
        "table_constraints" => &[
            ("constraint_catalog", T::Name),
            ("constraint_schema", T::Name),
            ("constraint_name", T::Name),
            ("table_catalog", T::Name),
            ("table_schema", T::Name),
            ("table_name", T::Name),
            ("constraint_type", T::Varchar),
            ("is_deferrable", T::Varchar),
            ("initially_deferred", T::Varchar),
            ("enforced", T::Varchar),
            ("nulls_distinct", T::Varchar),
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
            ("sql_path", T::Varchar),
        ],
        _ => return None,
    }))
}

pub(super) fn populate(
    snapshot: &CatalogSnapshot,
    lookup: &Lookup<'_>,
    key_names: &BTreeMap<u64, String>,
    budget: &mut Budget,
    all: &mut BTreeMap<String, Relation>,
) -> R<()> {
    // The finite catalog presents bootstrap-superuser visibility; there is no
    // privilege filtering until an authorization model exists.
    let mut generated = BTreeMap::<&str, Vec<Vec<Value>>>::new();
    macro_rules! add {
        ($name:literal, $row:expr) => {
            if all.contains_key($name) {
                budget.add(generated.entry($name).or_default(), $row)?;
            }
        };
    }
    let user = snapshot.roles.iter().find(|role| role.name == snapshot.user);
    for (&id, definition) in &snapshot.views {
        let table = lookup.tables[&id];
        let owner = snapshot.table_owners.get(&id).copied().unwrap_or(10);
        let can_define = user.is_some_and(|role| role.oid == owner || role.superuser);
        add!("views", vec![s(&snapshot.database), s(snapshot.table_schema(id).0),
            s(&table.name), if can_define { s(definition) } else { N }, s("NONE"),
            s("NO"), s("NO"), s("NO"), s("NO"), s("NO")]);
    }
    for row in rows(all, "pg_namespace").iter().filter(|_| all.contains_key("schemata")) {
        let I(owner) = row[2] else {
            return Err(Error::InvalidSnapshot);
        };
        add!("schemata", vec![
            s(&snapshot.database),
            row[1].clone(),
            s(lookup.roles[&owner]),
            N,
            N,
            N,
            N,
        ]);
    }
    let mut attributes = HashMap::<i64, Vec<&[Value]>>::new();
    if all.contains_key("columns") {
        for row in rows(all, "pg_attribute") {
            budget.visit(1)?;
            if row_oid(&row[4]) > 0 {
                attributes.entry(row_oid(&row[0])).or_default().push(row);
            }
        }
    }
    let column_source = |name| {
        if all.contains_key("columns") { rows(all, name) } else { &[] }
    };
    budget.visit(column_source("pg_type").len() + column_source("pg_collation").len()
        + column_source("pg_attrdef").len())?;
    let types = materialize::by_oid(column_source("pg_type"));
    let collations = materialize::by_oid(column_source("pg_collation"));
    let defaults = column_source("pg_attrdef").iter().map(|row| {
        let (I(relation), I(position)) = (&row[1], &row[2]) else {
            return Err(Error::InvalidSnapshot);
        };
        Ok(((*relation, *position), &row[3]))
    }).collect::<R<BTreeMap<_, _>>>()?;
    for relation in rows(all, "pg_class").iter().filter(|_| all.contains_key("tables") || all.contains_key("columns")) {
        if !matches!(&relation[3], Value::Text(kind) if kind == "r" || kind == "v") {
            continue;
        }
        let namespace = lookup.namespaces.get(&row_oid(&relation[2])).copied().ok_or(Error::InvalidSnapshot)?;
        let view = relation[3] == s("v");
        add!("tables", vec![
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
        for &attribute in attributes.get(&row_oid(&relation[0])).into_iter().flatten() {
            let (I(relation_id), I(oid), I(position)) = (&relation[0], &attribute[2], &attribute[4]) else {
                return Err(Error::InvalidSnapshot);
            };
            let mut column = column_row(
                snapshot,
                namespace,
                &relation[1],
                &attribute[1],
                *position,
                *oid as u32,
                &attribute[5],
                attribute[11] == B(true),
                !view,
                types.get(oid).copied(),
            );
            let generated = lookup.relations.get(relation_id)
                .and_then(|table| table.columns.get((*position - 1) as usize))
                .and_then(|column| snapshot.generated_columns.get(&column.id));
            if let Some(generated) = generated {
                column[41] = s("ALWAYS");
                column[42] = s(&generated.expression);
            } else if let Some(default) = defaults.get(&(*relation_id, *position)) {
                column[5] = (*default).clone();
            }
            let collation = row_oid(&attribute[19]);
            if collation != 0 && collation != 100
                && let Some(collation) = collations.get(&collation)
            {
                column[19] = s(&snapshot.database);
                column[20] = match collation[2] {
                    I(oid) => lookup.namespaces.get(&oid).map_or(N, |name| s(name)),
                    _ => N,
                };
                column[21] = collation[1].clone();
            }
            add!("columns", column);
        }
    }
    if all.contains_key("table_constraints") || all.contains_key("key_column_usage") {
        for table in &snapshot.tables {
            let (schema, _) = snapshot.table_schema(table.id);
            let mut constraint = |name: &str, kind: &str| -> R<()> {
                add!("table_constraints", vec![
                    s(&snapshot.database),
                    s(schema),
                    s(name),
                    s(&snapshot.database),
                    s(schema),
                    s(&table.name),
                    s(kind),
                    s("NO"),
                    s("NO"),
                    s("YES"),
                    N,
                ]);
                Ok(())
            };
            for (index, column) in table.columns.iter().enumerate() {
                if column.not_null || table.primary_key.contains(&index) {
                    constraint(&format!("{}_{}_not_null", table.name, column.name), "CHECK")?;
                }
            }
            if !table.primary_key.is_empty() {
                constraint(&key_names[&table.id], "PRIMARY KEY")?;
                for (position, index) in table.primary_key.iter().enumerate() {
                    add!("key_column_usage", vec![
                        s(&snapshot.database),
                        s(schema),
                        s(&key_names[&table.id]),
                        s(&snapshot.database),
                        s(schema),
                        s(&table.name),
                        s(&table.columns[*index].name),
                        I(position as i64 + 1),
                        N,
                    ]);
                }
            }
        }
        for c in snapshot.constraints.iter().filter(|c| c.kind == 'u') {
            let table = lookup.tables[&c.table];
            let schema = snapshot.table_schema(c.table).0;
            add!("table_constraints", vec![s(&snapshot.database), s(schema), s(&c.name),
                s(&snapshot.database), s(schema), s(&table.name), s("UNIQUE"), s("NO"), s("NO"), s("YES"), s("YES")]);
            for (position, id) in c.columns.iter().enumerate() {
                let col = lookup.column(table, *id).ok_or(Error::InvalidSnapshot)?;
                add!("key_column_usage", vec![s(&snapshot.database), s(schema), s(&c.name),
                    s(&snapshot.database), s(schema), s(&table.name), s(&col.name), I(position as i64 + 1), N]);
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
    typmod: &Value,
    not_null: bool,
    updatable: bool,
    type_row: Option<&[Value]>,
) -> Vec<Value> {
    let mut row = vec![N; 44];
    row[0] = s(&snapshot.database);
    row[1] = s(namespace);
    row[2] = table.clone();
    row[3] = column.clone();
    row[4] = I(position);
    row[6] = s(if not_null { "NO" } else { "YES" });
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
        1043 => "character varying",
        1042 => "character",
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
    if matches!(oid, 1042 | 1043)
        && let I(n) = typmod
        && *n >= 4
    {
        row[8] = I(n - 4);
        row[9] = I((n - 4) * 4);
    }
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
        1700 => {
            row[11] = I(10);
            if let I(raw) = typmod
                && let Ok(raw) = i32::try_from(*raw)
                && let Some(modifier) = giggles_pg_types::typmod::NumericTypmod::from_typmod(raw)
            {
                row[10] = I(i64::from(modifier.precision()));
                // PG18 information_schema exposes the unsigned packed scale,
                // unlike format_type's signed interpretation.
                row[12] = I(i64::from(modifier.scale() & 0x7ff));
            }
        }
        1082 => row[13] = I(0),
        1083 | 1266 | 1114 | 1184 => row[13] = match typmod {
            I(n) if *n >= 0 => I(*n),
            _ => I(6),
        },
        1186 => row[13] = I(6),
        _ => {}
    }
    row[25] = s(&snapshot.database);
    row[26] = s(snapshot.user_types.iter()
        .find(|ty| ty.kind == 'e' && ty.oid.checked_add(1) == Some(oid))
        .map_or("pg_catalog", |ty| ty.schema.as_str()));
    row[27] = udt;
    row[32] = s(&position.to_string());
    row[33] = s("NO");
    row[34] = s("NO");
    row[40] = s("NO");
    row[41] = s("NEVER");
    row[43] = s(if updatable { "YES" } else { "NO" });
    row
}
