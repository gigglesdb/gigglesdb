//! Qualified table adapters from source AND destination preflight evidence.
//!
//! Built-in types are qualified by namespace, name, kind and built-in wire
//! descriptor, never by a source OID reused on the destination. Enum/domain
//! definitions need evidence absent from Snapshot and therefore block. Callers
//! must also block floats (including float arrays): pgoutput text capture cannot
//! prove preservation of arbitrary NaN payload bits. Binary capture or qualified
//! value-domain evidence is needed before those adapters can be enabled.
//! Callers
//! must read back both server_encoding and client_encoding as UTF8 at both ends
//! and enforce the remaining mapping session settings. Wire encoding alone
//! does not establish catalog encoding identity.
//! ServerPlanner qualifies text/collation, numeric, interval and timetz ordering
//! on a dedicated source connection. The compatible synchronous classifier uses
//! typed PostgreSQL order for its narrower supported key set.
//!
//! This is adapter qualification, not a migration readiness assessment. Before
//! using these adapters callers must pass the complete preflight assessment,
//! qualify writable columns (including generated/identity behavior), privileges,
//! replica identity, DDL drift handling and the mapping session settings. Partial
//! projections must use TableCatalog::chunk_query rather than legacy SELECT *.

mod decode;
mod order;
mod server;
pub use order::{RangeClassifier, RangeStatus};
pub use server::{CollationEvidence, CollationQualification, ServerPlanner, ServerTable};

use std::{collections::BTreeSet, fmt};

use serde_json::Value as Json;
use sha2::{Digest, Sha256};
use tokio_postgres::types::{Kind, Type};

use super::{
    apply,
    chunk::{self, Direction, KeyColumn, QualifiedName, TableKey},
    pgoutput::{Relation, RelationColumn},
    preflight::{Check, Observation, Snapshot},
    source::{self, ColumnMapping, TypeResolver},
};
use crate::{
    definition::{ObjectKind, ObjectSelector},
    event::{OpaqueValue, Row},
    mapping::PgType,
};

/// A failure to qualify is actionable evidence, never an inferred text mapping.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Blocker {
    pub column: String,
    pub sql_type: String,
    pub reason: String,
    pub next_step: &'static str,
}
impl fmt::Display for Blocker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} ({}): {}; {}",
            self.column, self.sql_type, self.reason, self.next_step
        )
    }
}
impl std::error::Error for Blocker {}
type Result<T> = std::result::Result<T, Blocker>;
fn blocked(
    column: &str,
    sql_type: &str,
    reason: impl Into<String>,
    next_step: &'static str,
) -> Blocker {
    Blocker {
        column: column.into(),
        sql_type: sql_type.into(),
        reason: reason.into(),
        next_step,
    }
}

#[derive(Clone, Debug)]
pub struct TableSelection {
    pub schema: String,
    pub name: String,
    /// All in-scope columns, including every operational/replica-identity key.
    pub columns: Vec<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NullOrder {
    First,
    Last,
}
#[derive(Clone, Debug)]
pub struct IndexColumn {
    pub key: KeyColumn,
    /// Preserved even though operational keys must be NOT NULL. chunk::TableKey
    /// cannot represent custom null order and rejects NULL parameters.
    pub null_order: NullOrder,
    pub opclass: QualifiedName,
    pub opfamily: QualifiedName,
}
#[derive(Clone, Debug)]
pub struct QualifiedColumn {
    pub name: String,
    pub pg_type: PgType,
    pub source_wire: Type,
    pub destination_wire: Type,
    pub typmod: i32,
    pub source_type_id: String,
}

#[derive(Clone, Debug)]
pub struct TableCatalog {
    pub selection: TableSelection,
    pub columns: Vec<QualifiedColumn>,
    pub index_columns: Vec<IndexColumn>,
    pub key: TableKey,
    pub destination: apply::Table,
}
impl TableCatalog {
    /// Select only qualified columns; legacy TableKey::read_query selects *.
    pub fn chunk_query(&self, range: &chunk::ChunkRange) -> chunk::Result<chunk::ChunkQuery> {
        self.key.read_query_columns(range, &self.selection.columns)
    }
    pub async fn read_chunk(
        &self,
        client: &mut tokio_postgres::Client,
        range: &chunk::ChunkRange,
    ) -> chunk::Result<Vec<Row>> {
        self.chunk_query(range)?
            .execute(client)
            .await?
            .into_iter()
            .map(self.decoder())
            .collect()
    }
    pub fn resolver(&self) -> QualifiedResolver {
        QualifiedResolver(self.clone())
    }
    /// Directly usable as PgChunks.decode, using checked binary reads.
    pub fn decoder(&self) -> impl FnMut(tokio_postgres::Row) -> chunk::Result<Row> + use<> {
        let columns = self.columns.clone();
        move |row| {
            if row.len() != columns.len() {
                return Err(chunk::ChunkError::Invalid(
                    "catalog row column count changed",
                ));
            }
            let mut result = Row::new();
            for column in &columns {
                let i = row
                    .columns()
                    .iter()
                    .position(|c| c.name() == column.name)
                    .ok_or(chunk::ChunkError::Invalid("catalog row column missing"))?;
                if row.columns()[i].type_() != &column.source_wire {
                    return Err(chunk::ChunkError::Invalid("catalog row wire type changed"));
                }
                let raw: Option<decode::Raw> = row.try_get(i)?;
                let value = match raw {
                    None => crate::mapping::Value::Null,
                    Some(raw) => decode::binary(&column.pg_type, &column.source_wire, &raw.0)?,
                };
                result.insert(
                    column.name.clone(),
                    OpaqueValue {
                        type_id: column.source_type_id.clone(),
                        bytes: if value == crate::mapping::Value::Null {
                            None
                        } else {
                            Some(value.canonical_bytes()?)
                        },
                    },
                );
            }
            Ok(result)
        }
    }
    pub fn range_classifier(
        &self,
        plan: &crate::coordinator::chunk::ChunkPlan,
        order_domain: &str,
    ) -> Result<RangeClassifier> {
        RangeClassifier::new(self, plan, order_domain)
    }
}

pub struct QualifiedResolver(TableCatalog);
impl TypeResolver for QualifiedResolver {
    fn included_columns(
        &mut self,
        relation: &Relation<'_>,
    ) -> source::Result<Option<BTreeSet<String>>> {
        if relation.namespace != self.0.selection.schema || relation.name != self.0.selection.name {
            return Err(source::Error::RecoveryRequired("unqualified relation"));
        }
        Ok(Some(self.0.selection.columns.iter().cloned().collect()))
    }
    fn resolve(
        &mut self,
        relation: &Relation<'_>,
        column: &RelationColumn<'_>,
    ) -> source::Result<ColumnMapping> {
        if relation.namespace.is_empty()
            || relation.namespace != self.0.selection.schema
            || relation.name != self.0.selection.name
        {
            return Err(source::Error::RecoveryRequired("unqualified relation"));
        }
        let c = self
            .0
            .columns
            .iter()
            .find(|c| c.name == column.name)
            .ok_or(source::Error::RecoveryRequired(
                "unqualified relation column",
            ))?;
        if c.source_wire.oid() != column.type_oid || c.typmod != column.type_modifier {
            return Err(source::Error::RecoveryRequired(
                "qualified column type/typmod changed",
            ));
        }
        Ok(ColumnMapping::identity(c.pg_type.clone()))
    }
}

/// Assemble selected tables. `utf8_encoding_verified` means BOTH server_encoding
/// and client_encoding were read back as UTF8 on EACH endpoint. Snapshot lacks
/// encoding evidence: client wire encoding alone cannot qualify catalog types.
pub fn build(
    source: &Snapshot,
    destination: &Snapshot,
    selections: &[TableSelection],
    schema_version: u64,
    utf8_encoding_verified: bool,
) -> Result<Vec<TableCatalog>> {
    if !utf8_encoding_verified {
        return Err(blocked(
            "<session>",
            "server_encoding/client_encoding",
            "UTF8 server and client encoding readbacks absent at one or both endpoints",
            "Read back server_encoding and client_encoding as UTF8 on both endpoints; apply and verify source::REQUIRED_SETTINGS_SQL too.",
        ));
    }
    let mut seen = BTreeSet::new();
    selections
        .iter()
        .map(|selection| {
            if !seen.insert((&selection.schema, &selection.name)) {
                return Err(blocked(
                    &selection.name,
                    "table",
                    "duplicate selection",
                    "Select each table once.",
                ));
            }
            assemble(source, destination, selection, schema_version)
        })
        .collect()
}
fn observation<'a>(
    snapshot: &'a Snapshot,
    selection: &TableSelection,
    check: Check,
) -> Result<&'a Observation> {
    let matches: Vec<_> = snapshot
        .observations
        .iter()
        .filter(|o| {
            o.check == check
                && o.object.as_ref()
                    == Some(&ObjectSelector {
                        schema: selection.schema.clone(),
                        name: selection.name.clone(),
                        kind: ObjectKind::Table,
                        exclude_columns: Vec::new(),
                    })
        })
        .collect();
    if matches.len() != 1 || matches[0].error.is_some() {
        return Err(blocked(
            &selection.name,
            "catalog",
            format!("missing, duplicate or failed {} observation", check.id()),
            "Recollect preflight on this endpoint.",
        ));
    }
    Ok(matches[0])
}
fn text<'a>(row: &'a Json, field: &str, column: &str) -> Result<&'a str> {
    row[field]
        .as_str()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            blocked(
                column,
                "catalog",
                format!("missing {field}"),
                "Recollect complete catalog evidence.",
            )
        })
}
struct ColumnBehavior {
    not_null: bool,
    has_default: bool,
    generated: bool,
    identity: bool,
}
fn column_behavior(row: &Json, column: &str) -> Result<ColumnBehavior> {
    let flag = |field: &str| {
        row[field].as_bool().ok_or_else(|| {
            blocked(
                column,
                "column behavior",
                "column lacks checked not_null/has_default/generated/identity evidence",
                "Recollect attnotnull, atthasdef, attgenerated and attidentity for every column.",
            )
        })
    };
    Ok(ColumnBehavior {
        not_null: flag("not_null")?,
        has_default: flag("has_default")?,
        generated: flag("generated")?,
        identity: flag("identity")?,
    })
}
fn assemble(
    source: &Snapshot,
    destination: &Snapshot,
    selection: &TableSelection,
    schema_version: u64,
) -> Result<TableCatalog> {
    let src = &observation(source, selection, Check::ColumnTypes)?.rows;
    let dst = &observation(destination, selection, Check::ColumnTypes)?.rows;
    let names = |rows: &[Json]| -> Result<BTreeSet<String>> {
        let mut names = BTreeSet::new();
        for row in rows {
            let name = text(row, "column", &selection.name)?;
            if !names.insert(name.into()) {
                return Err(blocked(
                    name,
                    "catalog",
                    "duplicate column",
                    "Recollect column evidence.",
                ));
            }
        }
        Ok(names)
    };
    let selected: BTreeSet<_> = selection.columns.iter().cloned().collect();
    if selected.is_empty()
        || selected.len() != selection.columns.len()
        || !selected.is_subset(&names(src)?)
        || !selected.is_subset(&names(dst)?)
    {
        return Err(blocked(
            &selection.name,
            "table projection",
            "selection must name existing source and destination columns exactly once",
            "Select existing columns without duplicates, including every key column.",
        ));
    }
    for row in dst {
        let name = text(row, "column", &selection.name)?;
        let behavior = column_behavior(row, name)?;
        if !selected.contains(name) {
            // Identity uses its owned sequence without setting atthasdef.
            // Generated columns are recomputed by the destination.
            let safe = !behavior.not_null
                || behavior.has_default
                || behavior.generated
                || behavior.identity;
            if !safe {
                return Err(blocked(
                    name,
                    "destination column",
                    "excluded destination column is NOT NULL without a default",
                    "Include this column or add a qualified destination default.",
                ));
            }
        } else if behavior.generated {
            return Err(blocked(
                name,
                "destination generated column",
                "selected generated column cannot appear in apply writes",
                "Exclude this column and qualify destination recomputation before readiness.",
            ));
        }
    }
    let mut columns = Vec::new();
    for row in src {
        let name = text(row, "column", &selection.name)?;
        if !selected.contains(name) {
            continue;
        }
        if column_behavior(row, name)?.generated {
            return Err(blocked(
                name,
                "source generated column",
                "selected generated column cannot appear in apply writes",
                "Exclude this column and qualify destination recomputation before readiness.",
            ));
        }
        let (pg_type, source_wire, typmod) = qualify(row, name)?;
        let dest = dst
            .iter()
            .find(|r| r["column"] == name)
            .expect("column sets checked");
        let (dest_type, destination_wire, dest_typmod) = qualify(dest, name)?;
        if pg_type != dest_type || typmod != dest_typmod {
            return Err(blocked(
                name,
                &source_wire.to_string(),
                "destination type or typmod differs",
                "Qualify an explicit reversible mapping or align destination DDL.",
            ));
        }
        let source_type_id = format!(
            "skva.v1:{:x}",
            Sha256::digest(
                serde_json::to_vec(&(&pg_type, typmod)).expect("type descriptor serializes")
            )
        );
        columns.push(QualifiedColumn {
            name: name.into(),
            pg_type,
            source_wire,
            destination_wire,
            typmod,
            source_type_id,
        });
    }
    let eligible = |r: &&Json| {
        ["unique", "valid", "ready", "immediate", "not_null"]
            .iter()
            .all(|k| r[*k] == true)
            && r["partial"] == false
            && r["expression"] == false
    };
    let rows = &observation(source, selection, Check::Key)?.rows;
    let identity = rows
        .first()
        .and_then(|r| r["replica_identity"].as_str())
        .ok_or_else(|| {
            blocked(
                &selection.name,
                "replica identity",
                "missing replica identity evidence",
                "Recollect source key observations.",
            )
        })?;
    if !matches!(identity, "d" | "i" | "f")
        || rows.iter().any(|r| r["replica_identity"] != identity)
    {
        return Err(blocked(
            &selection.name,
            "replica identity",
            "replica identity is unavailable or inconsistent",
            "Choose DEFAULT primary key, USING INDEX, or FULL and recollect preflight.",
        ));
    }
    if identity == "f" && names(src)? != selected {
        return Err(blocked(
            &selection.name,
            "replica identity FULL",
            "excluded column is required by replica identity FULL",
            "Include every source column or choose a qualified replica identity index containing only included columns.",
        ));
    }
    let index = rows.iter().filter(eligible).filter(|r| match identity { "d" => r["primary"] == true, "i" => r["identity_index"] == true, "f" => true, _ => false }).min_by_key(|r| (r["primary"] != true, r["name"].as_str().unwrap_or("")))
        .ok_or_else(|| blocked(&selection.name, "index", "no valid immediate non-null unique key available in replica-identity old tuples; NULL boundaries are unsupported downstream", "Choose a non-null primary/identity key or FULL with a qualified key; NULLS FIRST/LAST metadata alone cannot enable nullable chunks."))?;
    if index["access_method"] != "btree" {
        return Err(blocked(
            &selection.name,
            "index",
            "only btree ordering is qualified",
            "Choose a btree default-opclass key.",
        ));
    }
    let keys = index["key_columns"]
        .as_array()
        .filter(|a| !a.is_empty())
        .ok_or_else(|| {
            blocked(
                &selection.name,
                "index",
                "missing ordered key metadata",
                "Recollect preflight with indoption, indcollation and opclass/opfamily evidence.",
            )
        })?;
    let mut index_columns = Vec::new();
    let mut key_names = BTreeSet::new();
    for k in keys {
        let name = text(k, "column", &selection.name)?;
        let column = columns.iter().find(|c| c.name == name).ok_or_else(|| {
            blocked(
                name,
                "key",
                "key column is not selected",
                "Select every key column.",
            )
        })?;
        if !key_names.insert(name) {
            return Err(blocked(
                name,
                "key",
                "duplicate key column",
                "Recollect key evidence.",
            ));
        }
        let qualified = |prefix: &str| -> Result<QualifiedName> {
            Ok(QualifiedName {
                schema: text(k, &format!("{prefix}_schema"), name)?.into(),
                name: text(k, &format!("{prefix}_name"), name)?.into(),
            })
        };
        let opclass = qualified("opclass")?;
        let opfamily = qualified("opfamily")?;
        // varchar is binary-compatible with text's default opclass. Built-in
        // arrays use pg_catalog.anyarray (2277), not the concrete array OID.
        let input_oid = match column.pg_type {
            PgType::Varchar { .. } => Type::TEXT.oid(),
            PgType::Array(_) => 2277,
            _ => column.source_wire.oid(),
        };
        if k["opclass_default"] != true
            || opclass.schema != "pg_catalog"
            || opfamily.schema != "pg_catalog"
            || k["opclass_input_oid"].as_u64() != Some(u64::from(input_oid))
        {
            return Err(blocked(
                name,
                &column.source_wire.to_string(),
                "non-default or non-catalog operator class/family is unqualified",
                "Select the built-in default btree opclass, or qualify a documented ordering allowlist.",
            ));
        }
        let flag = |field: &str| {
            k[field].as_bool().ok_or_else(|| {
                blocked(
                    name,
                    "index",
                    format!("missing {field}"),
                    "Recollect index options.",
                )
            })
        };
        let collation = match (&k["collation_schema"], &k["collation_name"]) {
            (Json::Null, Json::Null) => None,
            _ => Some(qualified("collation")?),
        };
        index_columns.push(IndexColumn {
            key: KeyColumn {
                name: name.into(),
                pg_type: column.pg_type.clone(),
                sql_type: QualifiedName {
                    schema: column.source_wire.schema().into(),
                    name: column.source_wire.name().into(),
                },
                collation,
                direction: if flag("desc")? {
                    Direction::Desc
                } else {
                    Direction::Asc
                },
            },
            null_order: if flag("nulls_first")? {
                NullOrder::First
            } else {
                NullOrder::Last
            },
            opclass,
            opfamily,
        });
    }
    let key = TableKey {
        table: QualifiedName {
            schema: selection.schema.clone(),
            name: selection.name.clone(),
        }
        .sql()
        .map_err(|_| {
            blocked(
                &selection.name,
                "table",
                "invalid SQL identifier",
                "Use valid nonempty identifiers.",
            )
        })?,
        relation: QualifiedName {
            schema: selection.schema.clone(),
            name: selection.name.clone(),
        },
        columns: index_columns.iter().map(|k| k.key.clone()).collect(),
    };
    key.validate().map_err(|_| {
        blocked(
            &selection.name,
            "key",
            "invalid key metadata",
            "Recollect key evidence.",
        )
    })?;
    let expected: Vec<_> = index_columns
        .iter()
        .map(|k| Json::String(k.key.name.clone()))
        .collect();
    if index["columns"].as_array() != Some(&expected) {
        return Err(blocked(
            &selection.name,
            "index",
            "key column observations disagree",
            "Recollect coherent key evidence.",
        ));
    }
    let destination = apply::Table {
        event_table: key.table.clone(),
        schema: selection.schema.clone(),
        name: selection.name.clone(),
        schema_version,
        columns: columns
            .iter()
            .map(|c| apply::Column {
                name: c.name.clone(),
                type_id: c.source_type_id.clone(),
                pg_type: c.pg_type.clone(),
                wire_type: c.destination_wire.clone(),
            })
            .collect(),
        key: index_columns.iter().map(|k| k.key.name.clone()).collect(),
    };
    Ok(TableCatalog {
        selection: selection.clone(),
        columns,
        index_columns,
        key,
        destination,
    })
}

fn qualify(row: &Json, column: &str) -> Result<(PgType, Type, i32)> {
    let schema = text(row, "schema", column)?;
    let name = text(row, "name", column)?;
    let sql_type = format!("{schema}.{name}");
    let fail = || {
        blocked(
            column,
            &sql_type,
            "type is not fully qualified by built-in catalog evidence",
            "Supply enum/domain/extension definitions and constraints or an explicit qualified mapping; no raw-text fallback.",
        )
    };
    let oid = row["oid"]
        .as_u64()
        .and_then(|v| u32::try_from(v).ok())
        .ok_or_else(fail)?;
    let wire = Type::from_oid(oid).ok_or_else(fail)?;
    let typmod = row["typmod"]
        .as_i64()
        .and_then(|v| i32::try_from(v).ok())
        .ok_or_else(fail)?;
    if schema != "pg_catalog"
        || wire.schema() != schema
        || wire.name() != name
        || row["kind"] != "b"
        || row["base_oid"] != 0
    {
        return Err(fail());
    }
    let pg_type = builtin(&wire).ok_or_else(fail)?;
    if contains_float(&pg_type) {
        return Err(blocked(
            column,
            &sql_type,
            "pgoutput text capture cannot qualify arbitrary float NaN payload preservation",
            "Qualify binary capture or a proven float value domain before enabling float columns/arrays.",
        ));
    }
    let expected_element = match wire.kind() {
        Kind::Array(t) => t.oid(),
        _ => 0,
    };
    if row["element_oid"].as_u64() != Some(u64::from(expected_element)) {
        return Err(fail());
    }
    Ok((pg_type, wire, typmod))
}
fn contains_float(ty: &PgType) -> bool {
    match ty {
        PgType::Float4 | PgType::Float8 => true,
        PgType::Array(element) => contains_float(element),
        _ => false,
    }
}
fn builtin(wire: &Type) -> Option<PgType> {
    Some(match *wire {
        Type::BOOL => PgType::Bool,
        Type::INT2 => PgType::Int2,
        Type::INT4 => PgType::Int4,
        Type::INT8 => PgType::Int8,
        Type::NUMERIC => PgType::Numeric,
        Type::FLOAT4 => PgType::Float4,
        Type::FLOAT8 => PgType::Float8,
        Type::TEXT => PgType::Text {
            encoding: "UTF8".into(),
        },
        Type::VARCHAR => PgType::Varchar {
            encoding: "UTF8".into(),
        },
        Type::BPCHAR => PgType::Char {
            encoding: "UTF8".into(),
        },
        Type::BYTEA => PgType::Bytea,
        Type::UUID => PgType::Uuid,
        Type::DATE => PgType::Date,
        Type::TIME => PgType::Time,
        Type::TIMETZ => PgType::Timetz,
        Type::TIMESTAMP => PgType::Timestamp,
        Type::TIMESTAMPTZ => PgType::Timestamptz,
        Type::INTERVAL => PgType::Interval,
        Type::JSON => PgType::Json,
        Type::JSONB => PgType::Jsonb,
        _ => match wire.kind() {
            Kind::Array(element) => PgType::Array(Box::new(builtin(element)?)),
            _ => return None,
        },
    })
}
