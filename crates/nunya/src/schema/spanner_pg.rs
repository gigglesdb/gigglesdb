//! Spanner PostgreSQL-dialect catalog snapshot loading through PGAdapter.
use super::{
    Column, ColumnId, ColumnRef, DependentKind, DependentObject, Relation, RelationKind, Snapshot,
};
use crate::{
    error::{NyCode, Refusal},
    value::ColumnType,
};
use std::collections::BTreeMap;
use tokio_postgres::{Client, types::ToSql};

const TABLES: &str = "SELECT table_schema,table_name,table_type FROM information_schema.tables WHERE table_type IN ('BASE TABLE','VIEW') AND ((COALESCE(array_length($1::text[],1),0)>0 AND table_schema=ANY($1)) OR (COALESCE(array_length($1::text[],1),0)=0 AND table_schema NOT IN ('information_schema','pg_catalog','spanner_sys'))) ORDER BY table_schema,table_name";
const COLUMNS: &str = "SELECT table_schema,table_name,column_name,COALESCE(spanner_type,data_type),is_nullable,is_generated,generation_expression,column_default FROM information_schema.columns WHERE ((COALESCE(array_length($1::text[],1),0)>0 AND table_schema=ANY($1)) OR (COALESCE(array_length($1::text[],1),0)=0 AND table_schema NOT IN ('information_schema','pg_catalog','spanner_sys'))) ORDER BY table_schema,table_name,ordinal_position";
const VIEWS: &str = "SELECT table_schema,table_name,view_definition FROM information_schema.views WHERE ((COALESCE(array_length($1::text[],1),0)>0 AND table_schema=ANY($1)) OR (COALESCE(array_length($1::text[],1),0)=0 AND table_schema NOT IN ('information_schema','pg_catalog','spanner_sys'))) ORDER BY table_schema,table_name";
const PRIMARY_KEY: &str = "SELECT table_schema,table_name,column_name FROM information_schema.index_columns WHERE index_type='PRIMARY_KEY' AND ((COALESCE(array_length($1::text[],1),0)>0 AND table_schema=ANY($1)) OR (COALESCE(array_length($1::text[],1),0)=0 AND table_schema NOT IN ('information_schema','pg_catalog','spanner_sys'))) ORDER BY table_schema,table_name,ordinal_position";
const STREAM_CATALOGS: &str = "SELECT table_name FROM information_schema.tables WHERE table_schema='information_schema' AND table_name IN ('change_streams','change_stream_tables','change_stream_columns')";
const SCHEMAS: &str = "SELECT schema_name FROM information_schema.schemata WHERE ((COALESCE(array_length($1::text[],1),0)>0 AND schema_name=ANY($1)) OR (COALESCE(array_length($1::text[],1),0)=0 AND schema_name NOT IN ('information_schema','pg_catalog','spanner_sys'))) ORDER BY schema_name";
const STREAMS: &str =
    "SELECT change_stream_name,CAST(\"all\" AS text) FROM information_schema.change_streams";
const STREAM_TABLES: &str = "SELECT change_stream_name,table_schema,table_name FROM information_schema.change_stream_tables";
const STREAM_COLUMNS: &str = "SELECT change_stream_name,table_schema,table_name FROM information_schema.change_stream_columns";

fn unavailable() -> Refusal {
    Refusal::new(NyCode::SchemaStale, "catalog snapshot unavailable")
}
type CatalogRow = Vec<Option<String>>;

async fn query_catalog(
    client: &Client,
    sql: &str,
    params: &[&(dyn ToSql + Sync)],
) -> Result<Vec<CatalogRow>, Refusal> {
    client
        .query(sql, params)
        .await
        .map_err(|_| unavailable())?
        .iter()
        .map(|row| {
            (0..row.len())
                .map(|index| row.try_get(index).map_err(|_| unavailable()))
                .collect()
        })
        .collect()
}
fn required(row: &[Option<String>], index: usize) -> Result<&str, Refusal> {
    row.get(index)
        .and_then(Option::as_deref)
        .ok_or_else(unavailable)
}
fn optional(row: &[Option<String>], index: usize) -> Result<Option<String>, Refusal> {
    row.get(index).cloned().ok_or_else(unavailable)
}
fn relation_key(row: &[Option<String>]) -> Result<(String, String), Refusal> {
    Ok((required(row, 0)?.into(), required(row, 1)?.into()))
}
fn column_identity(
    schema: &str,
    table: &str,
    column: &str,
    raw_type: &str,
) -> (ColumnRef, ColumnId) {
    (
        ColumnRef {
            schema: schema.into(),
            table: table.into(),
            column: column.into(),
        },
        ColumnId::Spanner {
            schema: schema.into(),
            table: table.into(),
            column: column.into(),
            spanner_type: raw_type.into(),
        },
    )
}
fn reads(relation: &Relation) -> Vec<ColumnRef> {
    relation
        .columns
        .iter()
        .map(|column| ColumnRef {
            schema: relation.schema.clone(),
            table: relation.name.clone(),
            column: column.name.clone(),
        })
        .collect()
}
fn generated(is_generated: &str, expression: Option<String>) -> Result<Option<String>, Refusal> {
    match is_generated {
        "NEVER" => Ok(None),
        "ALWAYS" => expression
            .filter(|value| !value.trim().is_empty())
            .map(Some)
            .ok_or_else(unavailable),
        _ => Err(unavailable()),
    }
}
fn view_reads(definition: &str, relations: &[Relation]) -> Vec<ColumnRef> {
    let folded = definition.to_lowercase().replace("\"\"", "\"");
    relations
        .iter()
        .filter(|relation| folded.contains(&relation.name.to_lowercase()))
        .flat_map(reads)
        .collect()
}
fn add_stream(dependents: &mut Vec<DependentObject>, name: &str, relation: &Relation) {
    let dependent = DependentObject {
        kind: DependentKind::Rule,
        name: format!("change_stream:{name}"),
        reads: reads(relation),
        writes_table: None,
    };
    if !dependents.contains(&dependent) {
        dependents.push(dependent);
    }
}

/// Capture Spanner's PostgreSQL information schema using plain parameterized queries.
pub async fn load_spanner_pg_snapshot(
    client: &Client,
    schemas: &[String],
    search_path: &[String],
    now_unix_ms: u64,
) -> Result<Snapshot, Refusal> {
    load_snapshot(
        move |sql, filtered| async move {
            let selected = [&schemas as &(dyn ToSql + Sync)];
            let params: &[_] = if filtered { &selected } else { &[] };
            query_catalog(client, sql, params).await
        },
        search_path,
        now_unix_ms,
    )
    .await
}

async fn load_snapshot<F>(
    query: impl Fn(&'static str, bool) -> F,
    search_path: &[String],
    now_unix_ms: u64,
) -> Result<Snapshot, Refusal>
where
    F: Future<Output = Result<Vec<CatalogRow>, Refusal>>,
{
    let captured_schemas = query(SCHEMAS, true)
        .await?
        .iter()
        .map(|row| required(row, 0).map(str::to_owned))
        .collect::<Result<Vec<_>, _>>()?;
    let mut column_ids = BTreeMap::new();
    let mut relations = Vec::new();
    let mut relation_indexes = BTreeMap::new();
    for row in query(TABLES, true).await? {
        let (schema, name) = relation_key(&row)?;
        let kind = match required(&row, 2)? {
            "BASE TABLE" => RelationKind::Table,
            "VIEW" => RelationKind::View,
            _ => return Err(unavailable()),
        };
        relation_indexes.insert((schema.clone(), name.clone()), relations.len());
        relations.push(Relation {
            schema,
            name,
            kind,
            columns: vec![],
            view_definition: None,
            primary_key: vec![],
        });
    }
    for row in query(COLUMNS, true).await? {
        let Some(&index) = relation_indexes.get(&relation_key(&row)?) else {
            continue;
        };
        let relation = &mut relations[index];
        let nullable = match required(&row, 4)? {
            "YES" => true,
            "NO" => false,
            _ => return Err(unavailable()),
        };
        let column_name = required(&row, 2)?;
        let raw_type = required(&row, 3)?;
        let (column_ref, id) =
            column_identity(&relation.schema, &relation.name, column_name, raw_type);
        column_ids.insert(column_ref, id);
        relation.columns.push(Column {
            name: column_name.into(),
            ty: map_spanner_pg_type(raw_type),
            nullable,
            generated: generated(required(&row, 5)?, optional(&row, 6)?)?,
            default: optional(&row, 7)?,
        });
    }
    for row in query(VIEWS, true).await? {
        let Some(&index) = relation_indexes.get(&relation_key(&row)?) else {
            continue;
        };
        let relation = &mut relations[index];
        if relation.kind == RelationKind::View {
            let definition = required(&row, 2)?;
            if relation.view_definition.is_some() || definition.trim().is_empty() {
                return Err(unavailable());
            }
            relation.view_definition = Some(definition.into());
        }
    }
    for row in query(PRIMARY_KEY, true).await? {
        if let Some(&index) = relation_indexes.get(&relation_key(&row)?) {
            relations[index].primary_key.push(required(&row, 2)?.into());
        }
    }
    let mut dependents = Vec::new();
    for relation in &relations {
        if relation.kind == RelationKind::View && relation.view_definition.is_none() {
            return Err(unavailable());
        }
        for column in &relation.columns {
            if column.generated.is_some() {
                dependents.push(DependentObject {
                    kind: DependentKind::GeneratedColumn,
                    name: format!("{}.{}.{}", relation.schema, relation.name, column.name),
                    reads: reads(relation),
                    writes_table: None,
                });
            }
        }
        if let Some(definition) = &relation.view_definition {
            dependents.push(DependentObject {
                kind: DependentKind::View,
                name: format!("{}.{}", relation.schema, relation.name),
                reads: view_reads(definition, &relations),
                writes_table: None,
            });
        }
    }
    let catalog_rows = query(STREAM_CATALOGS, false).await?;
    let catalogs = catalog_rows
        .iter()
        .map(|row| required(row, 0))
        .collect::<Result<Vec<_>, _>>()?;
    // Partial catalogs fail closed to avoid silently missing change-stream dependencies.
    if !catalogs.is_empty() {
        if ![
            "change_streams",
            "change_stream_tables",
            "change_stream_columns",
        ]
        .iter()
        .all(|name| catalogs.contains(name))
        {
            return Err(unavailable());
        }
        let mut streams = Vec::new();
        let stream_rows = query(STREAMS, false).await?;
        for row in &stream_rows {
            let name = required(row, 0)?;
            let all = match required(row, 1)? {
                "true" | "YES" => true,
                "false" | "NO" => false,
                _ => return Err(unavailable()),
            };
            if all {
                for relation in &relations {
                    if relation.kind == RelationKind::Table {
                        add_stream(&mut dependents, name, relation);
                    }
                }
            }
            streams.push(name);
        }
        for sql in [STREAM_TABLES, STREAM_COLUMNS] {
            for row in query(sql, false).await? {
                let stream = required(&row, 0)?;
                if !streams.contains(&stream) {
                    return Err(unavailable());
                }
                let key = (required(&row, 1)?.into(), required(&row, 2)?.into());
                if let Some(&index) = relation_indexes.get(&key) {
                    add_stream(&mut dependents, stream, &relations[index]);
                }
            }
        }
    }
    Ok(
        Snapshot::new(relations, dependents, search_path.to_vec(), now_unix_ms)
            .with_column_ids(column_ids)
            .with_captured_schemas(captured_schemas),
    )
}

/// Map Spanner PostgreSQL type names while preserving unsupported types.
pub fn map_spanner_pg_type(spanner_type: &str) -> ColumnType {
    let normalized = spanner_type.trim().to_ascii_lowercase();
    if let Some(element) = normalized.strip_suffix("[]") {
        return ColumnType::Array(Box::new(map_spanner_pg_type(element)));
    }
    let (base, length) = if let Some((base, modifier)) = normalized.split_once('(') {
        let Some(modifier) = modifier.strip_suffix(')') else {
            return ColumnType::Other(spanner_type.into());
        };
        (
            base.trim(),
            modifier
                .trim()
                .parse::<i32>()
                .ok()
                .filter(|length| *length > 0),
        )
    } else {
        (normalized.as_str(), None)
    };
    match base {
        "character varying" | "varchar" | "text" => {
            if normalized.contains('(') && length.is_none() {
                ColumnType::Other(spanner_type.into())
            } else if length.is_some() || base != "text" {
                ColumnType::Varchar(length)
            } else {
                ColumnType::Text
            }
        }
        "bigint" | "int8" if !normalized.contains('(') => ColumnType::Int8,
        "boolean" if !normalized.contains('(') => ColumnType::Bool,
        "bytea" if !normalized.contains('(') => ColumnType::Bytea,
        "date" if !normalized.contains('(') => ColumnType::Date,
        "timestamp with time zone" if !normalized.contains('(') => ColumnType::Timestamptz,
        "numeric" => ColumnType::Numeric,
        "jsonb" if !normalized.contains('(') => ColumnType::Jsonb,
        _ => ColumnType::Other(spanner_type.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn cells<const N: usize>(cells: [Option<&str>; N]) -> CatalogRow {
        cells.map(|cell| cell.map(str::to_owned)).into()
    }

    fn text<const N: usize>(row: [&str; N]) -> CatalogRow {
        cells(row.map(Some))
    }

    fn column(fields: [&str; 6], generated: Option<&str>, default: Option<&str>) -> CatalogRow {
        let mut row = text(fields);
        row.extend([generated.map(str::to_owned), default.map(str::to_owned)]);
        row
    }

    #[tokio::test]
    async fn fake_column_query_preserves_exact_identity() {
        let snapshot = load_snapshot(
            |sql, _| async move {
                match sql {
                    SCHEMAS => Ok(vec![text(["public"])]),
                    TABLES => Ok(vec![text(["public", "items", "BASE TABLE"])]),
                    COLUMNS => Ok(vec![
                        column(
                            ["public", "items", "id", "bigint", "NO", "NEVER"],
                            None,
                            None,
                        ),
                        column(
                            [
                                "public",
                                "items",
                                "Label",
                                "character varying(42)",
                                "YES",
                                "NEVER",
                            ],
                            None,
                            None,
                        ),
                    ]),
                    VIEWS | PRIMARY_KEY | STREAM_CATALOGS => Ok(vec![]),
                    _ => panic!("unexpected catalog query"),
                }
            },
            &[],
            0,
        )
        .await
        .unwrap();
        assert_eq!(snapshot.column_ids.len(), 2);
        for (column, spanner_type) in [("id", "bigint"), ("Label", "character varying(42)")] {
            assert_eq!(
                snapshot.column_id(&ColumnRef {
                    schema: "public".into(),
                    table: "items".into(),
                    column: column.into(),
                }),
                Some(&ColumnId::Spanner {
                    schema: "public".into(),
                    table: "items".into(),
                    column: column.into(),
                    spanner_type: spanner_type.into(),
                })
            );
        }
    }

    #[tokio::test]
    async fn batched_catalog_matches_per_relation_snapshot() {
        let definition = "SELECT id FROM public.items";
        let queries = std::cell::RefCell::new(Vec::new());
        let calls = &queries;
        let search_path = vec!["public".into(), "archive".into()];
        let snapshot = load_snapshot(
            |sql, filtered| async move {
                calls.borrow_mut().push((sql, filtered));
                Ok(match sql {
                    SCHEMAS => vec![text(["archive"]), text(["public"])],
                    TABLES => vec![
                        text(["archive", "items", "BASE TABLE"]),
                        text(["public", "empty", "BASE TABLE"]),
                        text(["public", "item_view", "VIEW"]),
                        text(["public", "items", "BASE TABLE"]),
                    ],
                    COLUMNS => vec![
                        column(
                            ["archive", "items", "id", "text", "YES", "NEVER"],
                            None,
                            None,
                        ),
                        column(
                            ["public", "item_view", "id", "bigint", "YES", "NEVER"],
                            None,
                            None,
                        ),
                        column(
                            ["public", "items", "id", "bigint", "NO", "NEVER"],
                            None,
                            Some("0"),
                        ),
                        column(
                            ["public", "items", "derived", "bigint", "NO", "ALWAYS"],
                            Some("id + 1"),
                            None,
                        ),
                        cells([
                            Some("public"),
                            Some("orphan"),
                            Some("ignored"),
                            None,
                            Some("UNKNOWN"),
                            None,
                            None,
                            None,
                        ]),
                    ],
                    VIEWS => vec![text(["public", "item_view", definition])],
                    PRIMARY_KEY => vec![
                        text(["archive", "items", "id"]),
                        text(["public", "items", "derived"]),
                        text(["public", "items", "id"]),
                    ],
                    STREAM_CATALOGS => vec![
                        text(["change_streams"]),
                        text(["change_stream_tables"]),
                        text(["change_stream_columns"]),
                    ],
                    STREAMS => vec![text(["audit", "false"])],
                    STREAM_TABLES => vec![text(["audit", "public", "items"])],
                    STREAM_COLUMNS => vec![text(["audit", "public", "items"])],
                    _ => panic!("unexpected catalog query"),
                })
            },
            &search_path,
            123,
        )
        .await
        .unwrap();
        assert_eq!(
            queries.into_inner(),
            [
                (SCHEMAS, true),
                (TABLES, true),
                (COLUMNS, true),
                (VIEWS, true),
                (PRIMARY_KEY, true),
                (STREAM_CATALOGS, false),
                (STREAMS, false),
                (STREAM_TABLES, false),
                (STREAM_COLUMNS, false),
            ]
        );

        let expected_relations = vec![
            Relation {
                schema: "archive".into(),
                name: "items".into(),
                kind: RelationKind::Table,
                columns: vec![Column {
                    name: "id".into(),
                    ty: ColumnType::Text,
                    nullable: true,
                    generated: None,
                    default: None,
                }],
                view_definition: None,
                primary_key: vec!["id".into()],
            },
            Relation {
                schema: "public".into(),
                name: "empty".into(),
                kind: RelationKind::Table,
                columns: vec![],
                view_definition: None,
                primary_key: vec![],
            },
            Relation {
                schema: "public".into(),
                name: "item_view".into(),
                kind: RelationKind::View,
                columns: vec![Column {
                    name: "id".into(),
                    ty: ColumnType::Int8,
                    nullable: true,
                    generated: None,
                    default: None,
                }],
                view_definition: Some(definition.into()),
                primary_key: vec![],
            },
            Relation {
                schema: "public".into(),
                name: "items".into(),
                kind: RelationKind::Table,
                columns: vec![
                    Column {
                        name: "id".into(),
                        ty: ColumnType::Int8,
                        nullable: false,
                        generated: None,
                        default: Some("0".into()),
                    },
                    Column {
                        name: "derived".into(),
                        ty: ColumnType::Int8,
                        nullable: false,
                        generated: Some("id + 1".into()),
                        default: None,
                    },
                ],
                view_definition: None,
                primary_key: vec!["derived".into(), "id".into()],
            },
        ];
        let column_ref = |schema: &str, table: &str, column: &str| ColumnRef {
            schema: schema.into(),
            table: table.into(),
            column: column.into(),
        };
        let item_columns = vec![
            column_ref("public", "items", "id"),
            column_ref("public", "items", "derived"),
        ];
        let expected_dependents = vec![
            DependentObject {
                kind: DependentKind::View,
                name: "public.item_view".into(),
                reads: [
                    vec![column_ref("archive", "items", "id")],
                    item_columns.clone(),
                ]
                .concat(),
                writes_table: None,
            },
            DependentObject {
                kind: DependentKind::GeneratedColumn,
                name: "public.items.derived".into(),
                reads: item_columns.clone(),
                writes_table: None,
            },
            DependentObject {
                kind: DependentKind::Rule,
                name: "change_stream:audit".into(),
                reads: item_columns,
                writes_table: None,
            },
        ];
        let expected_ids = [
            ("archive", "items", "id", "text"),
            ("public", "item_view", "id", "bigint"),
            ("public", "items", "id", "bigint"),
            ("public", "items", "derived", "bigint"),
        ]
        .into_iter()
        .map(|(schema, table, column, raw_type)| {
            (
                column_ref(schema, table, column),
                ColumnId::Spanner {
                    schema: schema.into(),
                    table: table.into(),
                    column: column.into(),
                    spanner_type: raw_type.into(),
                },
            )
        })
        .collect();
        let expected = Snapshot::new(expected_relations, expected_dependents, search_path, 123)
            .with_column_ids(expected_ids)
            .with_captured_schemas(vec!["archive".into(), "public".into()]);
        assert_eq!(snapshot.fingerprint, expected.fingerprint);
        assert_eq!(snapshot, expected);
    }

    #[tokio::test]
    async fn batched_catalog_rejects_unknown_table_or_nullability_metadata() {
        for (table_type, is_nullable) in [("UNKNOWN", "YES"), ("BASE TABLE", "UNKNOWN")] {
            let error = load_snapshot(
                |sql, _| async move {
                    match sql {
                        SCHEMAS => Ok(vec![]),
                        TABLES => Ok(vec![text(["public", "items", table_type])]),
                        COLUMNS => Ok(vec![column(
                            ["public", "items", "id", "bigint", is_nullable, "NEVER"],
                            None,
                            None,
                        )]),
                        _ => panic!("invalid metadata must end the capture"),
                    }
                },
                &[],
                0,
            )
            .await
            .unwrap_err();
            assert_eq!(error.code, NyCode::SchemaStale);
        }
    }

    #[test]
    fn type_mapping() {
        for (name, expected) in [
            ("text", ColumnType::Text),
            ("varchar", ColumnType::Varchar(None)),
            ("character varying(42)", ColumnType::Varchar(Some(42))),
            ("text(8)", ColumnType::Varchar(Some(8))),
            ("bigint", ColumnType::Int8),
            ("int8", ColumnType::Int8),
            ("boolean", ColumnType::Bool),
            ("bytea", ColumnType::Bytea),
            ("date", ColumnType::Date),
            ("timestamp with time zone", ColumnType::Timestamptz),
            ("numeric", ColumnType::Numeric),
            ("numeric(38,9)", ColumnType::Numeric),
            ("jsonb", ColumnType::Jsonb),
        ] {
            assert_eq!(map_spanner_pg_type(name), expected, "{name}");
        }
        for name in [
            "double precision",
            "real",
            "integer",
            "timestamp",
            "varchar(0)",
            "text(-1)",
            "varchar(no)",
            "jsonb(1)",
        ] {
            assert_eq!(map_spanner_pg_type(name), ColumnType::Other(name.into()));
        }
        assert_eq!(
            map_spanner_pg_type(" BIGINT [] "),
            ColumnType::Array(Box::new(ColumnType::Int8))
        );
        assert_eq!(
            map_spanner_pg_type("varchar(12)[][]"),
            ColumnType::Array(Box::new(ColumnType::Array(Box::new(ColumnType::Varchar(
                Some(12)
            )))))
        );
    }
    fn relation(name: &str) -> Relation {
        Relation {
            schema: "public".into(),
            name: name.into(),
            kind: RelationKind::Table,
            columns: vec![Column {
                name: "secret".into(),
                ty: ColumnType::Text,
                nullable: false,
                generated: None,
                default: None,
            }],
            view_definition: None,
            primary_key: vec![],
        }
    }
    #[test]
    fn dependency_reads_are_conservative() {
        let relations = vec![relation("items"), relation("item"), relation("unrelated")];
        let found = view_reads("SELECT * FROM public.ITEMS", &relations);
        assert_eq!(found, [reads(&relations[0]), reads(&relations[1])].concat());
        let mut dependents = Vec::new();
        add_stream(&mut dependents, "audit", &relations[0]);
        add_stream(&mut dependents, "audit", &relations[0]);
        add_stream(&mut dependents, "audit", &relations[1]);
        assert_eq!(dependents.len(), 2);
        assert!(
            dependents
                .iter()
                .all(|dependent| dependent.kind == DependentKind::Rule
                    && dependent.name == "change_stream:audit")
        );
    }
    #[test]
    fn generated_metadata_fails_closed() {
        assert_eq!(generated("NEVER", None).unwrap(), None);
        assert_eq!(
            generated("ALWAYS", Some("x + 1".into())).unwrap(),
            Some("x + 1".into())
        );
        assert!(generated("ALWAYS", None).is_err());
        assert!(generated("ALWAYS", Some(" ".into())).is_err());
        assert!(generated("UNKNOWN", None).is_err());
    }
}
