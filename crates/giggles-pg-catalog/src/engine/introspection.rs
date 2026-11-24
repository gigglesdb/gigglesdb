use super::*;
use Value::{Bool as B, Int as I, Null as N, Text as S};
use std::collections::HashMap;

/// Catalog reads performed by scalar functions, including calls made by the
/// executor itself (for example, the visibility check in pg_get_indexdef).
pub(super) fn dependencies(name: &str, args: usize) -> &'static [&'static str] {
    match name {
        "pg_get_constraintdef" => &["pg_constraint", "pg_attribute"],
        "pg_get_indexdef" if args == 1 => &["pg_class", "pg_namespace", "pg_indexes"],
        "pg_get_indexdef" => &[
            "pg_index",
            "pg_attribute",
            "pg_class",
            "pg_namespace",
            "pg_indexes",
        ],
        "pg_get_expr" => &["pg_attrdef", "pg_constraint", "pg_index"],
        "_regcollation_display" => &["pg_collation", "pg_namespace"],
        "pg_collation_actual_version" => &["pg_collation"],
        "_regclass_display" | "pg_table_is_visible" => &["pg_class", "pg_namespace"],
        "_regnamespace" => &["pg_namespace"],
        "pg_relation_is_publishable"
        | "pg_relation_size"
        | "pg_total_relation_size"
        | "pg_table_size"
        | "pg_indexes_size" => &["pg_class"],
        "to_regtype" | "_regtype" => &["pg_type", "pg_namespace"],
        "_regtype_display" | "format_type" | "pg_type_is_visible" => &["pg_type"],
        "obj_description" | "col_description" => &["pg_description"],
        "shobj_description" => &["pg_shdescription"],
        "_aclexplode" | "pg_has_role" => &["pg_authid"],
        "has_schema_privilege" => &["pg_authid", "pg_namespace"],
        "has_database_privilege" => &["pg_authid", "pg_database"],
        "has_type_privilege" => &["pg_authid", "pg_type", "pg_namespace"],
        "has_sequence_privilege" => &["pg_authid", "pg_class"],
        "has_table_privilege" | "has_column_privilege" | "has_any_column_privilege" => {
            &["pg_authid", "pg_class", "pg_attribute"]
        }
        _ => &[],
    }
}

type OidRows<'a> = HashMap<i64, &'a [Value]>;
type AttributeNames<'a> = HashMap<(i64, i64), &'a Value>;

struct IndexDefinitions<'a> {
    indexes: OidRows<'a>,
    classes: OidRows<'a>,
    namespaces: OidRows<'a>,
    by_name: HashMap<(&'a str, &'a str), &'a [Value]>,
}

struct Visibility<'a> {
    relations: OidRows<'a>,
    namespaces: HashMap<&'a str, i64>,
    names: HashMap<(i64, &'a str), i64>,
}

fn oid_rows<'a>(rows: &'a [Vec<Value>], include: impl Fn(&[Value]) -> bool) -> OidRows<'a> {
    let mut by_oid = HashMap::new();
    for row in rows.iter().filter(|row| include(row)) {
        if let I(oid) = row[0] {
            by_oid.entry(oid).or_insert(row.as_slice());
        }
    }
    by_oid
}

#[derive(Default)]
pub(super) struct Lookups<'a> {
    attributes: Option<AttributeNames<'a>>,
    constraints: Option<(catalog::Definitions<'a>, OidRows<'a>)>,
    indexes: Option<IndexDefinitions<'a>>,
    expressions: Option<HashMap<(&'a str, i64), Option<&'a str>>>,
    visibility: Option<Visibility<'a>>,
}

impl<'a> Runtime<'a> {
    fn attribute_names(&mut self) -> R<&AttributeNames<'a>> {
        if self.introspection.attributes.is_none() {
            let rows = &self.catalogs["pg_attribute"].rows;
            self.charge(rows.len())?;
            let mut attributes = HashMap::new();
            for row in rows {
                if let (I(relation), I(position)) = (&row[0], &row[4]) {
                    attributes.entry((*relation, *position)).or_insert(&row[1]);
                }
            }
            self.introspection.attributes = Some(attributes);
        }
        Ok(self.introspection.attributes.as_ref().unwrap())
    }

    pub(super) fn expression_definition(&mut self, args: &[Value]) -> R<Value> {
        let (S(expression), I(relation)) = (&args[0], &args[1]) else {
            return Err(Error::Unsupported);
        };
        if self.introspection.expressions.is_none() {
            let mut expressions = HashMap::new();
            let generated_tables: HashMap<_, _> = if self.snapshot.generated_columns.is_empty() {
                HashMap::new()
            } else {
                self.snapshot.tables.iter().filter_map(|table| {
                    Some((i64::from(catalog::table_oid(table.id).ok()?), table))
                }).collect()
            };
            for (name, expression, relation) in [
                ("pg_attrdef", "adbin", "adrelid"),
                ("pg_constraint", "conbin", "conrelid"),
                ("pg_index", "indpred", "indrelid"),
            ] {
                let catalog = &self.catalogs[name];
                let field = |name| catalog.fields.iter().position(|f| f.name == name);
                let (Some(expression), Some(relation)) = (field(expression), field(relation)) else {
                    continue;
                };
                self.charge(catalog.rows.len())?;
                let position = (name == "pg_attrdef" && !generated_tables.is_empty())
                    .then(|| field("adnum")).flatten();
                for row in &catalog.rows {
                    if let (S(expr), I(oid)) = (&row[expression], &row[relation]) {
                        let pretty = position.and_then(|position| {
                            let I(position) = row[position] else { return None; };
                            let column = generated_tables.get(oid)?.columns
                                .get(usize::try_from(position.checked_sub(1)?).ok()?)?;
                            self.snapshot.generated_columns.get(&column.id)
                                .map(|column| column.pretty_expression.as_str())
                        });
                        expressions.entry((expr.as_str(), *oid)).or_insert(pretty);
                    }
                }
            }
            self.introspection.expressions = Some(expressions);
        }
        let pretty = self.introspection.expressions.as_ref().unwrap()
            .get(&(expression.as_str(), *relation)).ok_or(Error::Unsupported)?;
        let result = if args.get(2) == Some(&B(true)) {
            pretty.unwrap_or_else(|| {
                expression.strip_prefix('(').and_then(|e| e.strip_suffix(')')).unwrap_or(expression)
            })
        } else {
            expression
        };
        Ok(S(result.to_owned()))
    }

    pub(super) fn constraint_definition(&mut self, args: &[Value]) -> R<Value> {
        let oid = integer(args[0].clone())?;
        if self.introspection.constraints.is_none() {
            let rows = &self.catalogs["pg_constraint"].rows;
            self.charge(rows.len() + self.snapshot.constraints.len() + self.snapshot.tables.len())?;
            self.introspection.constraints = Some((
                catalog::Definitions::new(self.snapshot),
                oid_rows(
                    rows,
                    |row| matches!(&row[3], S(kind) if kind == "p" || kind == "n"),
                ),
            ));
        }
        let (definitions, fallback) = self.introspection.constraints.as_ref().unwrap();
        if let Some(definition) =
            definitions.definition(self.snapshot, oid, args.get(1) == Some(&B(true)))
        {
            return Ok(S(definition));
        }
        let Some(&row) = fallback.get(&oid) else {
            return Ok(N);
        };
        let (I(relation), Value::Array(keys)) = (&row[8], &row[20]) else {
            return Ok(N);
        };
        self.charge(keys.len())?;
        let attributes = self.attribute_names()?;
        let columns = keys
            .iter()
            .map(|key| {
                let I(position) = key else { return None };
                match attributes.get(&(*relation, *position))? {
                    S(name) => Some(catalog::quote_identifier(name)),
                    _ => None,
                }
            })
            .collect::<Option<Vec<_>>>();
        Ok(columns.map_or(N, |columns| {
            let columns = columns.join(", ");
            if matches!(&row[3], S(kind) if kind == "n") {
                S(format!("NOT NULL {columns}"))
            } else {
                S(format!("PRIMARY KEY ({columns})"))
            }
        }))
    }

    pub(super) fn table_is_visible(&mut self, value: &Value) -> R<Value> {
        let I(oid) = value else { return Ok(N) };
        if self.introspection.visibility.is_none() {
            let classes = &self.catalogs["pg_class"].rows;
            let schemas = &self.catalogs["pg_namespace"].rows;
            self.charge(classes.len() + schemas.len())?;
            let mut namespaces = HashMap::new();
            for row in schemas {
                if let (I(oid), S(name)) = (&row[0], &row[1]) {
                    namespaces.entry(name.as_str()).or_insert(*oid);
                }
            }
            let mut names = HashMap::new();
            for row in classes {
                if let (I(oid), S(name), I(schema)) = (&row[0], &row[1], &row[2]) {
                    names.entry((*schema, name.as_str())).or_insert(*oid);
                }
            }
            self.introspection.visibility = Some(Visibility {
                relations: oid_rows(classes, |_| true),
                namespaces,
                names,
            });
        }
        self.charge(self.snapshot.search_path.len() + 1)?;
        let lookup = self.introspection.visibility.as_ref().unwrap();
        let Some(row) = lookup.relations.get(oid) else {
            return Ok(N);
        };
        let S(name) = &row[1] else {
            return Ok(B(false));
        };
        let path = &self.snapshot.search_path;
        let implicit = (!path.iter().any(|schema| schema == "pg_catalog")).then_some("pg_catalog");
        let first = implicit
            .into_iter()
            .chain(path.iter().map(String::as_str))
            .filter_map(|schema| lookup.namespaces.get(schema))
            .find_map(|schema| lookup.names.get(&(*schema, name.as_str())));
        Ok(B(first == Some(oid)))
    }

    pub(super) fn index_definition(&mut self, args: &[Value]) -> R<Value> {
        let oid = integer(args[0].clone())?;
        let column = args.get(1).map_or(Ok(0), |value| integer(value.clone()))?;
        if self.introspection.indexes.is_none() {
            let indexes = self
                .catalogs
                .get("pg_index")
                .map_or(&[][..], |r| r.rows.as_slice());
            let classes = &self.catalogs["pg_class"].rows;
            let namespaces = &self.catalogs["pg_namespace"].rows;
            let definitions = &self.catalogs["pg_indexes"].rows;
            self.charge(indexes.len() + classes.len() + namespaces.len() + definitions.len())?;
            let mut by_name = HashMap::new();
            for row in definitions {
                if let (S(schema), S(index)) = (&row[0], &row[2]) {
                    by_name
                        .entry((schema.as_str(), index.as_str()))
                        .or_insert(row.as_slice());
                }
            }
            self.introspection.indexes = Some(IndexDefinitions {
                indexes: oid_rows(indexes, |_| true),
                classes: oid_rows(classes, |row| matches!(&row[17], S(kind) if kind == "i")),
                namespaces: oid_rows(namespaces, |_| true),
                by_name,
            });
        }
        let lookup = self.introspection.indexes.as_ref().unwrap();
        let index = lookup.indexes.get(&oid);
        if column != 0 {
            if index.is_none() {
                return Ok(N);
            }
            return Ok(match self.attribute_names()?.get(&(oid, column)) {
                Some(S(name)) => S(catalog::quote_identifier(name)),
                Some(_) => N,
                None => S(String::new()),
            });
        }
        let relation = index.map_or(N, |row| row[1].clone());
        let definition = lookup.classes.get(&oid).and_then(|row| {
            let (S(name), I(namespace)) = (&row[1], &row[2]) else {
                return None;
            };
            let S(schema) = &lookup.namespaces.get(namespace)?[1] else {
                return None;
            };
            lookup
                .by_name
                .get(&(schema.as_str(), name.as_str()))
                .copied()
        });
        let visible = if args.get(2) == Some(&B(true)) {
            self.table_is_visible(&relation)? == B(true)
        } else {
            false
        };
        let Some(row) = definition else { return Ok(N) };
        Ok(match &row[4] {
            S(definition) if visible => {
                let index_name = catalog::quote_identifier(&text(row[2].clone())?);
                let schema = catalog::quote_identifier(&text(row[0].clone())?);
                let prefix = if definition.starts_with("CREATE UNIQUE INDEX ") {
                    "CREATE UNIQUE INDEX "
                } else {
                    "CREATE INDEX "
                };
                // Remove qualification only at the table position; the index
                // identifier may itself contain " ON public.".
                let offset = prefix.len() + index_name.len() + " ON ".len();
                S(format!(
                    "{}{}",
                    &definition[..offset],
                    &definition[offset + schema.len() + 1..]
                ))
            }
            value => value.clone(),
        })
    }
}
