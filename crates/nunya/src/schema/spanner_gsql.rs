//! GoogleSQL information-schema snapshots through a text-cell runner.
use super::{
    Column, ColumnId, ColumnRef, DependentKind, DependentObject, Relation, RelationKind, Snapshot,
};
use crate::{
    error::{NyCode, Refusal},
    value::ColumnType,
};
use std::{future::Future, pin::Pin};
/// Asynchronous catalog-query result.
pub type QueryResult<'a> =
    Pin<Box<dyn Future<Output = Result<Vec<Vec<Option<String>>>, Refusal>> + Send + 'a>>;
/// Executes constant catalog SQL and converts cells to text.
pub trait SqlRunner: Send + Sync {
    /// Execute a catalog query.
    fn query<'a>(&'a self, sql: &'a str) -> QueryResult<'a>;
}
const TABLES: &str = "SELECT TABLE_SCHEMA,TABLE_NAME,TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE IN ('BASE TABLE','VIEW') AND TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA','SPANNER_SYS') ORDER BY TABLE_SCHEMA,TABLE_NAME";
const COLUMNS: &str = "SELECT TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,ORDINAL_POSITION,IS_NULLABLE,SPANNER_TYPE,IS_GENERATED,GENERATION_EXPRESSION,COLUMN_DEFAULT FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA','SPANNER_SYS') ORDER BY TABLE_SCHEMA,TABLE_NAME,ORDINAL_POSITION";
const PRIMARY_KEY: &str = "SELECT TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,ORDINAL_POSITION FROM INFORMATION_SCHEMA.INDEX_COLUMNS WHERE INDEX_TYPE = 'PRIMARY_KEY' AND TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA','SPANNER_SYS') ORDER BY TABLE_SCHEMA,TABLE_NAME,ORDINAL_POSITION";
const VIEWS: &str = "SELECT TABLE_SCHEMA,TABLE_NAME,VIEW_DEFINITION FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA','SPANNER_SYS')";
const STREAMS: &str = "SELECT CHANGE_STREAM_NAME,TABLE_SCHEMA,TABLE_NAME FROM INFORMATION_SCHEMA.CHANGE_STREAM_TABLES";
fn unavailable() -> Refusal {
    Refusal::new(NyCode::SchemaStale, "catalog snapshot unavailable")
}
fn cell(row: &[Option<String>], i: usize) -> Result<&str, Refusal> {
    row.get(i)
        .and_then(Option::as_deref)
        .ok_or_else(unavailable)
}
fn optional(row: &[Option<String>], i: usize) -> Result<Option<String>, Refusal> {
    row.get(i).cloned().ok_or_else(unavailable)
}
fn relation<'a>(
    relations: &'a mut [Relation],
    row: &[Option<String>],
    offset: usize,
) -> Result<&'a mut Relation, Refusal> {
    let schema = cell(row, offset)?;
    let name = cell(row, offset + 1)?;
    relations
        .iter_mut()
        .find(|r| r.schema == schema && r.name == name)
        .ok_or_else(unavailable)
}
fn reads(r: &Relation) -> Vec<ColumnRef> {
    r.columns
        .iter()
        .map(|c| ColumnRef {
            schema: r.schema.clone(),
            table: r.name.clone(),
            column: c.name.clone(),
        })
        .collect()
}
/// Capture the GoogleSQL catalog, conservatively recording dependencies.
pub async fn load_gsql_snapshot(
    runner: &dyn SqlRunner,
    now_unix_ms: u64,
) -> Result<Snapshot, Refusal> {
    let mut column_ids = std::collections::BTreeMap::new();
    let mut relations = Vec::new();
    for row in runner.query(TABLES).await.map_err(|_| unavailable())? {
        let kind = match cell(&row, 2)? {
            "BASE TABLE" => RelationKind::Table,
            "VIEW" => RelationKind::View,
            _ => return Err(unavailable()),
        };
        let schema = cell(&row, 0)?.to_owned();
        let name = cell(&row, 1)?.to_owned();
        if relations.iter().any(|r: &Relation| {
            r.schema.eq_ignore_ascii_case(&schema) && r.name.eq_ignore_ascii_case(&name)
        }) {
            return Err(unavailable());
        }
        relations.push(Relation {
            schema,
            name,
            kind,
            columns: vec![],
            primary_key: vec![],
            view_definition: None,
        });
    }
    let mut columns = runner.query(COLUMNS).await.map_err(|_| unavailable())?;
    columns.sort_by_key(|row| {
        row.get(3)
            .and_then(Option::as_deref)
            .and_then(|s| s.parse::<u64>().ok())
    });
    for row in columns {
        cell(&row, 3)?.parse::<u64>().map_err(|_| unavailable())?;
        let nullable = match cell(&row, 4)? {
            "YES" => true,
            "NO" => false,
            _ => return Err(unavailable()),
        };
        let generated = match cell(&row, 6)? {
            "NEVER" => None,
            "ALWAYS" => Some(
                optional(&row, 7)?
                    .filter(|s| !s.trim().is_empty())
                    .ok_or_else(unavailable)?,
            ),
            _ => return Err(unavailable()),
        };
        let r = relation(&mut relations, &row, 0)?;
        let name = cell(&row, 2)?.to_owned();
        if r.columns.iter().any(|c| c.name.eq_ignore_ascii_case(&name)) {
            return Err(unavailable());
        }
        column_ids.insert(
            ColumnRef {
                schema: r.schema.clone(),
                table: r.name.clone(),
                column: name.clone(),
            },
            ColumnId::Spanner {
                schema: r.schema.clone(),
                table: r.name.clone(),
                column: name.clone(),
                spanner_type: cell(&row, 5)?.to_owned(),
            },
        );
        r.columns.push(Column {
            name,
            ty: map_spanner_type(cell(&row, 5)?),
            nullable,
            generated,
            default: optional(&row, 8)?,
        });
    }
    let mut keys = runner.query(PRIMARY_KEY).await.map_err(|_| unavailable())?;
    keys.sort_by_key(|row| {
        row.get(3)
            .and_then(Option::as_deref)
            .and_then(|s| s.parse::<u64>().ok())
    });
    for row in keys {
        cell(&row, 3)?.parse::<u64>().map_err(|_| unavailable())?;
        let r = relation(&mut relations, &row, 0)?;
        let name = cell(&row, 2)?;
        let col = r
            .columns
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(name))
            .ok_or_else(unavailable)?;
        if r.primary_key.contains(&col.name) {
            return Err(unavailable());
        }
        r.primary_key.push(col.name.clone());
    }
    for row in runner.query(VIEWS).await.map_err(|_| unavailable())? {
        let r = relation(&mut relations, &row, 0)?;
        if r.kind != RelationKind::View || r.view_definition.is_some() {
            return Err(unavailable());
        }
        let definition = cell(&row, 2)?;
        if definition.trim().is_empty() {
            return Err(unavailable());
        }
        r.view_definition = Some(definition.to_owned());
    }
    let mut dependents = Vec::new();
    for r in &relations {
        if r.kind == RelationKind::View && r.view_definition.is_none() {
            return Err(unavailable());
        }
        for c in &r.columns {
            if c.generated.is_some() {
                dependents.push(DependentObject {
                    kind: DependentKind::GeneratedColumn,
                    name: format!("{}.{}.{}", r.schema, r.name, c.name),
                    reads: reads(r),
                    writes_table: None,
                });
            }
        }
        if let Some(definition) = &r.view_definition {
            let definition = definition.to_ascii_lowercase();
            dependents.push(DependentObject {
                kind: DependentKind::View,
                name: format!("{}.{}", r.schema, r.name),
                reads: relations
                    .iter()
                    .filter(|t| definition.contains(&t.name.to_ascii_lowercase()))
                    .flat_map(reads)
                    .collect(),
                writes_table: None,
            });
        }
    }
    if let Ok(rows) = runner.query(STREAMS).await {
        for row in rows {
            let name = cell(&row, 0)?;
            let r = relation(&mut relations, &row, 1)?;
            let dependent = DependentObject {
                kind: DependentKind::Rule,
                name: format!("change_stream:{name}"),
                reads: reads(r),
                writes_table: None,
            };
            if !dependents.contains(&dependent) {
                dependents.push(dependent);
            }
        }
    }
    Ok(
        Snapshot::new(relations, dependents, vec![String::new()], now_unix_ms)
            .with_column_ids(column_ids),
    )
}
/// Map GoogleSQL types, retaining unsupported names for fail-closed validation.
pub fn map_spanner_type(t: &str) -> ColumnType {
    let upper = t.trim().to_ascii_uppercase();
    if let Some(inner) = upper
        .strip_prefix("ARRAY<")
        .and_then(|s| s.strip_suffix('>'))
    {
        return ColumnType::Array(Box::new(map_spanner_type(inner)));
    }
    if ["STRING(", "BYTES("]
        .iter()
        .any(|prefix| upper.starts_with(prefix))
        && upper.ends_with(')')
    {
        let (base, size) = upper.split_once('(').unwrap_or(("", ""));
        let size = size.strip_suffix(')').unwrap_or("");
        if size == "MAX" || size.parse::<u64>().is_ok_and(|n| n > 0) {
            return if base == "STRING" {
                ColumnType::Text
            } else {
                ColumnType::Bytea
            };
        }
    }
    match upper.as_str() {
        "INT64" => ColumnType::Int8,
        "BOOL" => ColumnType::Bool,
        "DATE" => ColumnType::Date,
        "TIMESTAMP" => ColumnType::Timestamptz,
        "NUMERIC" => ColumnType::Numeric,
        "JSON" => ColumnType::Json,
        _ => ColumnType::Other(t.into()),
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    struct Fake {
        fail: Option<&'static str>,
    }
    fn row(values: &[&str]) -> Vec<Option<String>> {
        values
            .iter()
            .map(|s| {
                if *s == "NULL" {
                    None
                } else {
                    Some((*s).into())
                }
            })
            .collect()
    }
    impl SqlRunner for Fake {
        fn query<'a>(&'a self, sql: &'a str) -> QueryResult<'a> {
            Box::pin(async move {
                if self.fail == Some(sql) {
                    return Err(Refusal::new(NyCode::NotAnalysable, "fake"));
                }
                Ok(match sql {
                    TABLES => vec![
                        row(&["", "Users", "BASE TABLE"]),
                        row(&["", "Names", "VIEW"]),
                    ],
                    COLUMNS => vec![
                        row(&[
                            "",
                            "Users",
                            "Generated",
                            "2",
                            "YES",
                            "INT64",
                            "ALWAYS",
                            "1",
                            "NULL",
                        ]),
                        row(&[
                            "",
                            "Users",
                            "Email",
                            "1",
                            "NO",
                            "STRING(MAX)",
                            "NEVER",
                            "NULL",
                            "'default'",
                        ]),
                        row(&[
                            "",
                            "Names",
                            "Email",
                            "1",
                            "YES",
                            "STRING(MAX)",
                            "NEVER",
                            "NULL",
                            "NULL",
                        ]),
                    ],
                    PRIMARY_KEY => vec![row(&["", "Users", "Email", "1"])],
                    VIEWS => vec![row(&["", "Names", "SELECT Email FROM USERS"])],
                    STREAMS => vec![row(&["audit", "", "Users"])],
                    _ => return Err(unavailable()),
                })
            })
        }
    }
    #[tokio::test]
    async fn snapshot_and_conservative_dependents() {
        let s = load_gsql_snapshot(&Fake { fail: None }, 42).await.unwrap();
        assert_eq!(s.column_ids.len(), 3);
        for (column, spanner_type) in [("Email", "STRING(MAX)"), ("Generated", "INT64")] {
            assert_eq!(
                s.column_id(&ColumnRef {
                    schema: "".into(),
                    table: "Users".into(),
                    column: column.into()
                }),
                Some(&ColumnId::Spanner {
                    schema: "".into(),
                    table: "Users".into(),
                    column: column.into(),
                    spanner_type: spanner_type.into()
                })
            );
        }
        assert_eq!(s.taken_at_unix_ms, 42);
        assert_eq!(s.search_path, vec![""]);
        assert!(s.user_routines.is_empty());
        let r = &s.relations[0];
        assert_eq!(
            r.columns
                .iter()
                .map(|c| c.name.as_str())
                .collect::<Vec<_>>(),
            vec!["Email", "Generated"]
        );
        assert_eq!(r.primary_key, vec!["Email"]);
        assert_eq!(r.columns[0].default.as_deref(), Some("'default'"));
        assert!(!r.columns[0].nullable);
        for kind in [
            DependentKind::GeneratedColumn,
            DependentKind::View,
            DependentKind::Rule,
        ] {
            let d = s.dependents.iter().find(|d| d.kind == kind).unwrap();
            assert_eq!(d.reads.len(), 2);
        }
        assert!(s.dependents.iter().any(|d| d.name == "change_stream:audit"));
        assert_eq!(
            load_gsql_snapshot(
                &Fake {
                    fail: Some(STREAMS)
                },
                0
            )
            .await
            .unwrap()
            .dependents
            .len(),
            2
        );
        for sql in [TABLES, COLUMNS, PRIMARY_KEY, VIEWS] {
            assert_eq!(
                load_gsql_snapshot(&Fake { fail: Some(sql) }, 0)
                    .await
                    .unwrap_err()
                    .code,
                NyCode::SchemaStale
            );
        }
    }
    #[test]
    fn every_type_branch() {
        for (t, expected) in [
            ("STRING(12)", ColumnType::Text),
            ("STRING(MAX)", ColumnType::Text),
            ("INT64", ColumnType::Int8),
            ("BOOL", ColumnType::Bool),
            ("BYTES(MAX)", ColumnType::Bytea),
            ("BYTES(4)", ColumnType::Bytea),
            ("DATE", ColumnType::Date),
            ("TIMESTAMP", ColumnType::Timestamptz),
            ("NUMERIC", ColumnType::Numeric),
            ("JSON", ColumnType::Json),
            (
                "ARRAY<INT64>",
                ColumnType::Array(Box::new(ColumnType::Int8)),
            ),
            (
                "ARRAY<ARRAY<BOOL>>",
                ColumnType::Array(Box::new(ColumnType::Array(Box::new(ColumnType::Bool)))),
            ),
        ] {
            assert_eq!(map_spanner_type(t), expected);
        }
        for t in [
            "FLOAT64",
            "FLOAT32",
            "STRUCT<X INT64>",
            "unknown",
            "STRING(0)",
            "STRING(no)",
            "ARRAY<INT64",
        ] {
            assert_eq!(map_spanner_type(t), ColumnType::Other(t.into()));
        }
    }
}
