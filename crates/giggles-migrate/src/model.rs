use crate::{Result, require};
use serde::{Deserialize, Serialize};
use std::{cmp::Ordering, collections::BTreeMap};

/// Exact scalar representation. Null is a value; missing and deleted are separate.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum Scalar {
    Null,
    Bool(bool),
    Int(i64),
    Text(String),
    Bytes(Vec<u8>),
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScalarType {
    Bool,
    Int,
    Text,
    Bytes,
}
impl ScalarType {
    pub fn accepts(&self, v: &Scalar) -> bool {
        matches!(
            (self, v),
            (_, Scalar::Null)
                | (Self::Bool, Scalar::Bool(_))
                | (Self::Int, Scalar::Int(_))
                | (Self::Text, Scalar::Text(_))
                | (Self::Bytes, Scalar::Bytes(_))
        )
    }
}
pub type Key = Vec<Scalar>;
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    Asc,
    Desc,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Stream {
    pub id: String,
    pub start: u64,
    pub end: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Bounds {
    pub max_events: usize,
    pub max_chunk_bytes: usize,
    pub max_state_bytes: usize,
    pub max_rows: usize,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimestampUnit {
    MicrosecondsI64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExpiryContract {
    AbsoluteMicrosecondsExclusive,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TombstoneRetention {
    PermanentForRehearsal,
}
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TombstoneScope {
    Cell,
    Row,
    Partition,
    Range,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CellSemantics {
    pub timestamp_unit: TimestampUnit,
    pub expiry: ExpiryContract,
    pub tombstone_retention: TombstoneRetention,
    pub tombstone_scopes: Vec<TombstoneScope>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    pub version: u32,
    pub source: String,
    pub table: String,
    pub schema_epoch: u64,
    pub topology_epoch: u64,
    pub partition_types: Vec<ScalarType>,
    pub clustering_types: Vec<ScalarType>,
    pub clustering_order: Vec<Direction>,
    pub columns: BTreeMap<String, ScalarType>,
    pub cell_semantics: CellSemantics,
    pub streams: Vec<Stream>,
    pub horizon_micros: i64,
    /// Only reject_ambiguous is supported. No release-specific tie behavior is inferred.
    pub tie_policy: String,
    pub unsupported_features: Vec<String>,
    pub bounds: Bounds,
}
impl Manifest {
    pub fn validate(&self) -> Result<()> {
        require(self.version == 1, "unsupported normalized contract version")?;
        require(
            !self.source.is_empty() && !self.table.is_empty(),
            "empty source/table",
        )?;
        require(
            self.tie_policy == "reject_ambiguous",
            "unvalidated source tie rule",
        )?;
        require(
            self.unsupported_features.is_empty(),
            "unsupported source features",
        )?;
        let scopes: std::collections::BTreeSet<_> =
            self.cell_semantics.tombstone_scopes.iter().collect();
        require(
            !scopes.is_empty() && scopes.len() == self.cell_semantics.tombstone_scopes.len(),
            "empty/duplicate tombstone scope inventory",
        )?;
        require(
            !self.partition_types.is_empty()
                && self.partition_types.len() <= 8
                && self.clustering_types.len() <= 8,
            "key arity",
        )?;
        require(
            self.clustering_types.len() == self.clustering_order.len(),
            "clustering directions",
        )?;
        require(
            !self.columns.is_empty()
                && self.columns.len() <= 32
                && self.columns.keys().all(|c| !c.is_empty()),
            "column schema",
        )?;
        require(
            !self.streams.is_empty() && self.streams.len() <= 32,
            "stream inventory",
        )?;
        let mut ids = std::collections::BTreeSet::new();
        for s in &self.streams {
            require(
                !s.id.is_empty() && ids.insert(&s.id) && s.start <= s.end && s.end - s.start < 4096,
                "invalid stream bounds/identity",
            )?;
        }
        let b = &self.bounds;
        require(
            b.max_events > 0
                && b.max_events <= 4096
                && b.max_chunk_bytes > 0
                && b.max_chunk_bytes <= 48 * 1024
                && b.max_state_bytes > 0
                && b.max_state_bytes <= 48 * 1024
                && b.max_rows > 0
                && b.max_rows <= 256,
            "unsafe bounds",
        )
    }
    pub fn validate_key(&self, key: &Key, partition: bool) -> Result<()> {
        let types = if partition {
            &self.partition_types
        } else {
            &self.clustering_types
        };
        require(
            key.len() == types.len()
                && types
                    .iter()
                    .zip(key)
                    .all(|(t, v)| !matches!(v, Scalar::Null) && t.accepts(v)),
            "key type/arity/null",
        )
    }
    pub fn compare_clustering(&self, a: &Key, b: &Key) -> Ordering {
        a.iter()
            .zip(b)
            .zip(&self.clustering_order)
            .map(|((a, b), d)| match d {
                Direction::Asc => a.cmp(b),
                Direction::Desc => b.cmp(a),
            })
            .find(|o| *o != Ordering::Equal)
            .unwrap_or_else(|| a.len().cmp(&b.len()))
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Bound {
    pub key: Key,
    pub inclusive: bool,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "scope", rename_all = "snake_case", deny_unknown_fields)]
pub enum Scope {
    Cell {
        key: Key,
        column: String,
    },
    Row {
        key: Key,
    },
    Partition,
    Range {
        lower: Option<Bound>,
        upper: Option<Bound>,
    },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Mutation {
    Put {
        key: Key,
        column: String,
        value: Scalar,
        timestamp: i64,
        expires_at: Option<i64>,
    },
    Tombstone {
        target: Scope,
        timestamp: i64,
    },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Event {
    pub id: String,
    pub source: String,
    pub stream: String,
    /// Original opaque source token, retained verbatim, never interpreted as order.
    pub source_position: String,
    /// Adapter-supplied contiguous normalized index for this declared stream cut.
    pub position: u64,
    pub schema_epoch: u64,
    pub topology_epoch: u64,
    pub partition: Key,
    pub mutation: Mutation,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Chunk {
    pub id: String,
    pub events: Vec<Event>,
}
pub fn expiry_from_ttl(timestamp: i64, ttl_seconds: i64) -> Result<i64> {
    require(ttl_seconds >= 0, "negative TTL")?;
    ttl_seconds
        .checked_mul(1_000_000)
        .and_then(|ttl| timestamp.checked_add(ttl))
        .ok_or_else(|| crate::Error("TTL overflow".into()))
}
pub fn validate_event(m: &Manifest, e: &Event) -> Result<()> {
    require(
        !e.id.is_empty() && !e.source_position.is_empty() && e.source == m.source,
        "event identity/source",
    )?;
    require(
        e.schema_epoch == m.schema_epoch && e.topology_epoch == m.topology_epoch,
        "schema/topology epoch mismatch",
    )?;
    require(
        m.streams
            .iter()
            .any(|s| s.id == e.stream && e.position >= s.start && e.position <= s.end),
        "unknown stream or out-of-cut position",
    )?;
    m.validate_key(&e.partition, true)?;
    let column = |c: &str| require(m.columns.contains_key(c), "unknown cell column");
    match &e.mutation {
        Mutation::Put {
            key,
            column: c,
            value,
            ..
        } => {
            m.validate_key(key, false)?;
            column(c)?;
            require(m.columns[c].accepts(value), "cell type")?;
        }
        Mutation::Tombstone { target, .. } => {
            let kind = match target {
                Scope::Cell { .. } => TombstoneScope::Cell,
                Scope::Row { .. } => TombstoneScope::Row,
                Scope::Partition => TombstoneScope::Partition,
                Scope::Range { .. } => TombstoneScope::Range,
            };
            require(
                m.cell_semantics.tombstone_scopes.contains(&kind),
                "undeclared tombstone scope",
            )?;
            match target {
                Scope::Cell { key, column: c } => {
                    m.validate_key(key, false)?;
                    column(c)?;
                }
                Scope::Row { key } => m.validate_key(key, false)?,
                Scope::Partition => (),
                Scope::Range { lower, upper } => {
                    for b in [lower, upper].into_iter().flatten() {
                        m.validate_key(&b.key, false)?;
                    }
                    if let (Some(l), Some(u)) = (lower, upper) {
                        require(
                            m.compare_clustering(&l.key, &u.key) != Ordering::Greater,
                            "reversed range",
                        )?;
                    }
                }
            }
        }
    }
    Ok(())
}
