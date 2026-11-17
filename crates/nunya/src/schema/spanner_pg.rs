//! Spanner PostgreSQL-dialect catalog snapshot loading through PGAdapter.
use super::{Column, ColumnRef, DependentKind, DependentObject, Relation, RelationKind, Snapshot};
use crate::{
    error::{NyCode, Refusal},
    value::ColumnType,
};
use tokio_postgres::{Client, Row, types::FromSqlOwned};

const TABLES: &str = "SELECT table_schema,table_name,table_type FROM information_schema.tables WHERE table_type IN ('BASE TABLE','VIEW') AND ((COALESCE(array_length($1::text[],1),0)>0 AND table_schema=ANY($1)) OR (COALESCE(array_length($1::text[],1),0)=0 AND table_schema NOT IN ('information_schema','pg_catalog','spanner_sys'))) ORDER BY table_schema,table_name";
const COLUMNS: &str = "SELECT column_name,COALESCE(spanner_type,data_type),is_nullable,is_generated,generation_expression,column_default FROM information_schema.columns WHERE table_schema=$1 AND table_name=$2 ORDER BY ordinal_position";
const VIEW: &str =
    "SELECT view_definition FROM information_schema.views WHERE table_schema=$1 AND table_name=$2";
const PRIMARY_KEY: &str = "SELECT column_name FROM information_schema.index_columns WHERE table_schema=$1 AND table_name=$2 AND index_type='PRIMARY_KEY' ORDER BY ordinal_position";
const STREAM_CATALOGS: &str = "SELECT table_name FROM information_schema.tables WHERE table_schema='information_schema' AND table_name IN ('change_streams','change_stream_tables','change_stream_columns')";
const SCHEMAS: &str = "SELECT schema_name FROM information_schema.schemata WHERE ((COALESCE(array_length($1::text[],1),0)>0 AND schema_name=ANY($1)) OR (COALESCE(array_length($1::text[],1),0)=0 AND schema_name NOT IN ('information_schema','pg_catalog','spanner_sys'))) ORDER BY schema_name";
const STREAMS: &str =
    "SELECT change_stream_name,CAST(\"all\" AS text) FROM information_schema.change_streams";
const STREAM_TABLES: &str = "SELECT change_stream_name,table_schema,table_name FROM information_schema.change_stream_tables";
const STREAM_COLUMNS: &str = "SELECT change_stream_name,table_schema,table_name FROM information_schema.change_stream_columns";

fn unavailable() -> Refusal {
    Refusal::new(NyCode::SchemaStale, "catalog snapshot unavailable")
}
fn get<T: FromSqlOwned>(row: &Row, index: usize) -> Result<T, Refusal> {
    row.try_get(index).map_err(|_| unavailable())
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
    let selected = schemas.to_vec();
    let captured_schemas = client
        .query(SCHEMAS, &[&selected])
        .await
        .map_err(|_| unavailable())?
        .iter()
        .map(|row| get::<String>(row, 0))
        .collect::<Result<Vec<_>, _>>()?;
    let mut relations = Vec::new();
    for row in client
        .query(TABLES, &[&selected])
        .await
        .map_err(|_| unavailable())?
    {
        let schema: String = get(&row, 0)?;
        let name: String = get(&row, 1)?;
        let kind = match get::<String>(&row, 2)?.as_str() {
            "BASE TABLE" => RelationKind::Table,
            "VIEW" => RelationKind::View,
            _ => return Err(unavailable()),
        };
        let mut columns = Vec::new();
        for row in client
            .query(COLUMNS, &[&schema, &name])
            .await
            .map_err(|_| unavailable())?
        {
            let nullable = match get::<String>(&row, 2)?.as_str() {
                "YES" => true,
                "NO" => false,
                _ => return Err(unavailable()),
            };
            columns.push(Column {
                name: get(&row, 0)?,
                ty: map_spanner_pg_type(&get::<String>(&row, 1)?),
                nullable,
                generated: generated(&get::<String>(&row, 3)?, get(&row, 4)?)?,
                default: get(&row, 5)?,
            });
        }
        let view_definition = if kind == RelationKind::View {
            let rows = client
                .query(VIEW, &[&schema, &name])
                .await
                .map_err(|_| unavailable())?;
            if rows.len() != 1 {
                return Err(unavailable());
            }
            Some(
                get::<Option<String>>(&rows[0], 0)?
                    .filter(|value| !value.trim().is_empty())
                    .ok_or_else(unavailable)?,
            )
        } else {
            None
        };
        let primary_key = client
            .query(PRIMARY_KEY, &[&schema, &name])
            .await
            .map_err(|_| unavailable())?
            .iter()
            .map(|row| get(row, 0))
            .collect::<Result<Vec<String>, _>>()?;
        relations.push(Relation {
            schema,
            name,
            kind,
            columns,
            view_definition,
            primary_key,
        });
    }
    let mut dependents = Vec::new();
    for relation in &relations {
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
    let catalogs = client
        .query(STREAM_CATALOGS, &[])
        .await
        .map_err(|_| unavailable())?
        .iter()
        .map(|row| get::<String>(row, 0))
        .collect::<Result<Vec<_>, _>>()?;
    // Partial catalogs fail closed to avoid silently missing change-stream dependencies.
    if !catalogs.is_empty() {
        if ![
            "change_streams",
            "change_stream_tables",
            "change_stream_columns",
        ]
        .iter()
        .all(|name| catalogs.iter().any(|found| found == name))
        {
            return Err(unavailable());
        }
        let mut streams = Vec::new();
        for row in client
            .query(STREAMS, &[])
            .await
            .map_err(|_| unavailable())?
        {
            let name: String = get(&row, 0)?;
            let all = match get::<String>(&row, 1)?.as_str() {
                "true" | "YES" => true,
                "false" | "NO" => false,
                _ => return Err(unavailable()),
            };
            if all {
                for relation in &relations {
                    if relation.kind == RelationKind::Table {
                        add_stream(&mut dependents, &name, relation);
                    }
                }
            }
            streams.push(name);
        }
        for sql in [STREAM_TABLES, STREAM_COLUMNS] {
            for row in client.query(sql, &[]).await.map_err(|_| unavailable())? {
                let stream: String = get(&row, 0)?;
                let schema: String = get(&row, 1)?;
                let table: String = get(&row, 2)?;
                if !streams.contains(&stream) {
                    return Err(unavailable());
                }
                if let Some(relation) = relations
                    .iter()
                    .find(|relation| relation.schema == schema && relation.name == table)
                {
                    add_stream(&mut dependents, &stream, relation);
                }
            }
        }
    }
    Ok(Snapshot::new(
        relations,
        dependents,
        search_path.to_vec(),
        now_unix_ms,
    ).with_captured_schemas(captured_schemas))
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
