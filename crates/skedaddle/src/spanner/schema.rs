//! Canonical selected-table metadata. Requires complete INFORMATION_SCHEMA visibility.
//! References: https://docs.cloud.google.com/spanner/docs/information-schema
//! and https://docs.cloud.google.com/spanner/docs/information-schema-pg.
use super::client::{Client, Error};
use crate::definition::{ObjectKind, ObjectSelector, SpannerDialect};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, sync::Arc};

pub type Row = BTreeMap<String, Option<String>>;
pub type Metadata = BTreeMap<String, Vec<Row>>;
pub fn text<'a>(row: &'a Row, name: &str) -> &'a str {
    row.get(name).and_then(|v| v.as_deref()).unwrap_or("")
}
pub fn yes(s: &str) -> bool {
    s.eq_ignore_ascii_case("true") || s == "YES"
}
pub fn key(schema: &str, name: &str) -> String {
    serde_json::to_string(&(schema, name)).expect("strings")
}
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Table {
    pub descriptor: Row,
    pub columns: BTreeMap<String, Row>,
    pub indexes: BTreeMap<String, Row>,
    pub index_columns: BTreeMap<String, Row>,
}
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaSnapshot {
    pub tables: BTreeMap<String, Table>,
}
impl SchemaSnapshot {
    pub fn digest(&self) -> [u8; 32] {
        Sha256::digest(serde_json::to_vec(self).expect("metadata strings")).into()
    }
    pub fn from_metadata(metadata: &Metadata, selected: &[ObjectSelector]) -> Result<Self, Error> {
        let mut out = Self::default();
        for o in selected {
            if o.kind != ObjectKind::Table {
                return Err("Spanner selection must contain tables".into());
            }
            let id = key(&o.schema, &o.name);
            if out.tables.contains_key(&id) {
                return Err("duplicate selected Spanner table".into());
            }
            let matches =
                |r: &&Row| text(r, "table_schema") == o.schema && text(r, "table_name") == o.name;
            let table_rows: Vec<_> = metadata
                .get("tables")
                .into_iter()
                .flatten()
                .filter(matches)
                .collect();
            if table_rows.len() != 1 {
                return Err(format!("selected table missing or invisible: {id}").into());
            }
            let mut table = Table {
                descriptor: table_rows[0].clone(),
                ..Default::default()
            };
            for (view, target, fields) in [
                ("columns", &mut table.columns, &["column_name"][..]),
                ("indexes", &mut table.indexes, &["index_name"][..]),
                (
                    "index_columns",
                    &mut table.index_columns,
                    &["index_name", "column_name"][..],
                ),
            ] {
                for row in metadata.get(view).into_iter().flatten().filter(matches) {
                    let id = serde_json::to_string(
                        &fields.iter().map(|f| text(row, f)).collect::<Vec<_>>(),
                    )?;
                    if target.insert(id, row.clone()).is_some() {
                        return Err("duplicate metadata row".into());
                    }
                }
            }
            if table.columns.is_empty()
                || !table
                    .indexes
                    .values()
                    .any(|r| text(r, "index_type") == "PRIMARY_KEY")
                || !table
                    .index_columns
                    .values()
                    .any(|r| text(r, "index_name") == "PRIMARY_KEY")
            {
                return Err("incomplete selected-table column/key metadata".into());
            }
            out.tables.insert(id, table);
        }
        Ok(out)
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChangeKind {
    Added,
    Removed,
    Columns,
    Indexes,
    Table,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Change {
    pub table: String,
    pub kind: ChangeKind,
}
pub fn diff(start: &SchemaSnapshot, now: &SchemaSnapshot) -> Vec<Change> {
    let mut changes = Vec::new();
    for (id, a) in &start.tables {
        let Some(b) = now.tables.get(id) else {
            changes.push(Change {
                table: id.clone(),
                kind: ChangeKind::Removed,
            });
            continue;
        };
        for (changed, kind) in [
            (a.columns != b.columns, ChangeKind::Columns),
            (
                a.indexes != b.indexes || a.index_columns != b.index_columns,
                ChangeKind::Indexes,
            ),
            (a.descriptor != b.descriptor, ChangeKind::Table),
        ] {
            if changed {
                changes.push(Change {
                    table: id.clone(),
                    kind,
                });
            }
        }
    }
    for id in now
        .tables
        .keys()
        .filter(|id| !start.tables.contains_key(*id))
    {
        changes.push(Change {
            table: id.clone(),
            kind: ChangeKind::Added,
        });
    }
    changes
}

/// Every query in one strong read-only transaction; no mixed catalog timestamps.
/// Casts normalize PG YES/NO and GoogleSQL BOOL without changing identifiers.
pub async fn collect(client: &Client, stream: bool) -> Result<Metadata, Error> {
    let (db, endpoint, _) = client.snapshot_connection();
    let tx = db.read_only_transaction().build().await?;
    let cast = if endpoint.dialect == SpannerDialect::GoogleSql {
        "STRING"
    } else {
        "varchar"
    };
    let quote = if endpoint.dialect == SpannerDialect::GoogleSql {
        '`'
    } else {
        '"'
    };
    let views = [
        (
            "tables",
            "table_schema,table_name,parent_table_name,on_delete_action,table_type",
        ),
        (
            "columns",
            "table_schema,table_name,column_name,ordinal_position,spanner_type,is_nullable,is_generated,generation_expression,is_stored,column_default,spanner_state",
        ),
        (
            "indexes",
            "table_schema,table_name,index_name,index_type,parent_table_name,is_unique,is_null_filtered,index_state",
        ),
        (
            "index_columns",
            "table_schema,table_name,index_name,column_name,ordinal_position,column_ordering",
        ),
        (
            "change_streams",
            "change_stream_schema,change_stream_name,all",
        ),
        (
            "change_stream_tables",
            "change_stream_schema,change_stream_name,table_schema,table_name,all_columns",
        ),
        (
            "change_stream_columns",
            "change_stream_schema,change_stream_name,table_schema,table_name,column_name",
        ),
        (
            "change_stream_options",
            "change_stream_schema,change_stream_name,option_name,option_value",
        ),
        ("database_options", "option_name,option_value"),
    ];
    let mut out = Metadata::new();
    for (view, fields) in views.into_iter().take(if stream { 9 } else { 4 }) {
        let fields: Vec<_> = fields.split(',').collect();
        let projection = fields
            .iter()
            .map(|f| {
                let f = if quote == '`' {
                    f.to_uppercase()
                } else {
                    (*f).into()
                };
                format!("CAST({quote}{f}{quote} AS {cast})")
            })
            .collect::<Vec<_>>()
            .join(",");
        let mut rows = tx
            .execute_query(format!(
                "SELECT {projection} FROM information_schema.{view}"
            ))
            .await?;
        let mut values = Vec::new();
        while let Some(row) = rows.next().await {
            let row = row?;
            let mut value = Row::new();
            for (i, f) in fields.iter().enumerate() {
                value.insert((*f).into(), row.get::<Option<String>>(i)?);
            }
            values.push(value);
        }
        out.insert(view.into(), values);
    }
    Ok(out)
}

/// Install the preflight snapshot BEFORE capture/snapshot copying. Each boundary
/// is checked by default; larger intervals still check after at most 60 seconds
/// when a resolved boundary is available. A failed check remains retryable.
pub struct Guard {
    client: Arc<Client>,
    selected: Vec<ObjectSelector>,
    start: SchemaSnapshot,
    every: u64,
    boundaries: u64,
    checked: std::time::Instant,
}
impl Guard {
    pub fn new(
        client: Arc<Client>,
        selected: Vec<ObjectSelector>,
        start: SchemaSnapshot,
        every: u64,
    ) -> Result<Self, Error> {
        if every == 0 || selected.is_empty() {
            return Err("schema guard requires selection and positive interval".into());
        }
        let ids: std::collections::BTreeSet<_> =
            selected.iter().map(|o| key(&o.schema, &o.name)).collect();
        if ids.len() != selected.len() || ids != start.tables.keys().cloned().collect() {
            return Err("schema guard baseline selection mismatch".into());
        }
        Ok(Self {
            client,
            selected,
            start,
            every,
            boundaries: 0,
            checked: std::time::Instant::now(),
        })
    }
    pub async fn check(&mut self) -> Result<(), Error> {
        self.boundaries = self.boundaries.saturating_add(1);
        if self.boundaries < self.every && self.checked.elapsed().as_secs() < 60 {
            return Ok(());
        }
        let now =
            SchemaSnapshot::from_metadata(&collect(&self.client, false).await?, &self.selected)?;
        let changes = diff(&self.start, &now);
        if !changes.is_empty() {
            return Err(format!("selected Spanner schema drift: {changes:?}").into());
        }
        self.boundaries = 0;
        self.checked = std::time::Instant::now();
        Ok(())
    }
}
