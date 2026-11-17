//! Journal-bound startup truth. Full-table descriptors include unselected columns.
use super::{
    MysqlPosition,
    capture::CaptureStopReason,
    client::MysqlVersion,
    dest_schema,
    resume::ResumePlan,
    schema::{TableDescriptor, TableMapChange},
};
use crate::{
    event::SourceIdentity,
    mapping::{Classification, mysql::plan_column},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

#[derive(Debug)]
pub enum StartupError {
    Json(serde_json::Error),
    Invalid(&'static str),
    Position(crate::event::EventError),
    Destination(dest_schema::Blocker),
    Mapping(crate::mapping::MappingError),
}
impl std::fmt::Display for StartupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for StartupError {}
macro_rules! conversion {
    ($ty:ty, $variant:ident) => {
        impl From<$ty> for StartupError {
            fn from(e: $ty) -> Self {
                Self::$variant(e)
            }
        }
    };
}
conversion!(serde_json::Error, Json);
conversion!(crate::event::EventError, Position);
conversion!(dest_schema::Blocker, Destination);
conversion!(crate::mapping::MappingError, Mapping);
type Result<T> = std::result::Result<T, StartupError>;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum Version {
    #[serde(rename = "skedaddle-mysql-startup-v1")]
    V1,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum StopVersion {
    #[serde(rename = "skedaddle-mysql-schema-stop-v1")]
    V1,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum GtidMode {
    ON,
    OFF,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum VersionFamily {
    #[serde(rename = "8.0")]
    V80,
    #[serde(rename = "8.4")]
    V84,
}
impl From<MysqlVersion> for VersionFamily {
    fn from(v: MysqlVersion) -> Self {
        match v {
            MysqlVersion::V80 => Self::V80,
            MysqlVersion::V84 => Self::V84,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SelectedTable {
    pub schema: String,
    pub table: String,
    pub columns: Vec<String>,
    pub primary_key: Vec<String>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TableRecord {
    selection: SelectedTable,
    descriptor_digest: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MysqlStartupRecord {
    version: Version,
    // engine, stable cluster ID, incarnation, database/set, configured stream.
    source: [String; 5],
    gtid_mode: GtidMode,
    version_family: VersionFamily,
    start_position: Vec<u8>,
    tables: Vec<TableRecord>,
    mapping_versions: Vec<String>,
    mapping_digest: String,
    destination_schema_digest: String,
}
/// Build a fresh live observation with `MysqlStartupRecord::build`. The optional
/// plan MUST be the successful result of resume::check against the durable
/// checkpoint (which may be newer than this startup position) and live server,
/// not a plan invented by the caller. It authorizes
/// server UUID failover only; stable capture identity must never change.
pub struct ObservedStartup {
    pub record: MysqlStartupRecord,
    pub resume_plan: Option<ResumePlan>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StartupDrift {
    IdentityChanged,
    GtidModeChanged,
    VersionFamilyChanged,
    TablesChanged,
    ProjectionChanged { table: String },
    DescriptorChanged { table: String },
    MappingChanged,
    DestinationSchemaChanged,
}
impl std::fmt::Display for StartupDrift {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for StartupDrift {}
fn digest(value: &impl Serialize) -> Result<String> {
    Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(value)?)))
}
fn valid_name(s: &str) -> bool {
    !s.is_empty() && !s.contains('\0')
}
fn valid_digest(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
}
fn position(bytes: &[u8]) -> Result<MysqlPosition> {
    let p = MysqlPosition::decode(bytes)?;
    let offset = match &p {
        MysqlPosition::Gtid { offset, .. } | MysqlPosition::File { offset, .. } => *offset,
    };
    if !(4..=u64::from(u32::MAX)).contains(&offset) {
        return Err(StartupError::Invalid("position offset"));
    }
    if let MysqlPosition::Gtid { file, .. } = &p
        && !valid_name(file)
    {
        return Err(StartupError::Invalid("position file"));
    }
    Ok(p)
}
fn server(p: &MysqlPosition) -> &str {
    match p {
        MysqlPosition::Gtid { server_uuid, .. } | MysqlPosition::File { server_uuid, .. } => {
            server_uuid
        }
    }
}
impl MysqlStartupRecord {
    /// Table order and descriptor input order are irrelevant. Column ordinal,
    /// projection order and primary-key order are significant. The descriptor
    /// digest includes ALL columns of each selected table, not just projection.
    /// Mapping module versions are supplied by the installer (none exist as
    /// exported constants today); they must be stable, nonempty identifiers.
    pub fn build(
        source: &SourceIdentity,
        gtid_mode: GtidMode,
        family: MysqlVersion,
        start: &MysqlPosition,
        selected: &[SelectedTable],
        descriptors: &[TableDescriptor],
        mapping_versions: Vec<String>,
    ) -> Result<Self> {
        let mut selected = selected.to_vec();
        selected.sort_by(|a, b| (&a.schema, &a.table).cmp(&(&b.schema, &b.table)));
        let mut tables = Vec::new();
        let mut whole = Vec::new();
        let mut projected = Vec::new();
        let mut plans = Vec::new();
        for selection in selected {
            let matches: Vec<_> = descriptors
                .iter()
                .filter(|d| d.schema == selection.schema && d.table == selection.table)
                .collect();
            if matches.len() != 1 {
                return Err(StartupError::Invalid("missing/duplicate descriptor"));
            }
            let mut d = matches[0].clone();
            d.columns.sort_by_key(|c| c.ordinal);
            if selection.primary_key != d.primary_key {
                return Err(StartupError::Invalid("selected primary key"));
            }
            let mut projection = d.clone();
            projection.columns.clear();
            for name in &selection.columns {
                let c = d
                    .columns
                    .iter()
                    .find(|c| &c.name == name)
                    .ok_or(StartupError::Invalid("projection column"))?;
                let p = plan_column(c)?;
                let class = match p.classification {
                    Classification::Supported => "supported",
                    Classification::Unverified => "unverified",
                    Classification::Unsupported => "unsupported",
                };
                plans.push(serde_json::json!([
                    selection.schema,
                    selection.table,
                    c.name,
                    p.source,
                    p.pg_type,
                    p.ddl_type,
                    class,
                    p.supported_with_transformation,
                    p.collation.as_ref().map(|x| (
                        &x.name,
                        &x.locale,
                        x.deterministic,
                        x.pattern_operations_restricted
                    )),
                    p.check,
                    p.type_modifier,
                    p.type_id,
                    p.generated,
                    p.args,
                    p.labels
                ]));
                projection.columns.push(c.clone());
            }
            tables.push(TableRecord {
                selection,
                descriptor_digest: digest(&d)?,
            });
            whole.push(d);
            projected.push(projection);
        }
        // Validate whole descriptors too, including columns outside projection.
        dest_schema::build(&whole)?;
        let ddl = dest_schema::build(&projected)?;
        let r = Self {
            version: Version::V1,
            source: [
                source.engine.clone(),
                source.system_id.clone(),
                source.lineage.clone(),
                source.db_name.clone(),
                source.stream.clone(),
            ],
            gtid_mode,
            version_family: family.into(),
            start_position: start.encode()?,
            tables,
            mapping_versions,
            mapping_digest: digest(&plans)?,
            destination_schema_digest: digest(&ddl.statements())?,
        };
        r.validate()?;
        Ok(r)
    }
    fn validate(&self) -> Result<()> {
        if self.source[0] != "mysql" || self.source.iter().any(|s| !valid_name(s)) {
            return Err(StartupError::Invalid("source identity"));
        }
        let p = position(&self.start_position)?;
        if matches!(p, MysqlPosition::Gtid { .. }) && self.gtid_mode != GtidMode::ON {
            return Err(StartupError::Invalid("GTID position with mode OFF"));
        }
        if self.mapping_versions.is_empty()
            || self.mapping_versions.iter().any(|s| !valid_name(s))
            || !valid_digest(&self.mapping_digest)
            || !valid_digest(&self.destination_schema_digest)
        {
            return Err(StartupError::Invalid("mapping/schema identity"));
        }
        let mut last = None;
        if self.tables.is_empty() {
            return Err(StartupError::Invalid("empty selection"));
        }
        for t in &self.tables {
            let s = &t.selection;
            let key = (&s.schema, &s.table);
            if !valid_name(&s.schema)
                || !valid_name(&s.table)
                || last.is_some_and(|old| old >= key)
                || !valid_digest(&t.descriptor_digest)
            {
                return Err(StartupError::Invalid("table identity"));
            }
            last = Some(key);
            for names in [&s.columns, &s.primary_key] {
                if names.is_empty()
                    || names.iter().any(|n| !valid_name(n))
                    || names.iter().collect::<BTreeSet<_>>().len() != names.len()
                {
                    return Err(StartupError::Invalid("projection/key"));
                }
            }
            if s.primary_key.iter().any(|k| !s.columns.contains(k)) {
                return Err(StartupError::Invalid("key outside projection"));
            }
        }
        Ok(())
    }
    pub fn start_position(&self) -> Result<MysqlPosition> {
        position(&self.start_position)
    }
    pub fn encode(&self) -> Result<String> {
        self.validate()?;
        Ok(serde_json::to_string(self)?)
    }
    pub fn decode(text: &str) -> Result<Self> {
        let r: Self = serde_json::from_str(text)?;
        r.validate()?;
        Ok(r)
    }
    pub fn check_restart(
        &self,
        observed: &ObservedStartup,
    ) -> std::result::Result<(), StartupDrift> {
        use StartupDrift::*;
        let o = &observed.record;
        if self.source != o.source {
            return Err(IdentityChanged);
        }
        if self.gtid_mode != o.gtid_mode {
            return Err(GtidModeChanged);
        }
        if self.version_family != o.version_family {
            return Err(VersionFamilyChanged);
        }
        let old = self.start_position().map_err(|_| IdentityChanged)?;
        let new = o.start_position().map_err(|_| IdentityChanged)?;
        let eligible = matches!((&old,&observed.resume_plan), (MysqlPosition::Gtid{set,..},Some(ResumePlan::Gtid{set:accepted,..})) if !accepted.to_string().is_empty() && set.is_subset(accepted));
        if server(&old) != server(&new) && !eligible {
            return Err(IdentityChanged);
        }
        if self
            .tables
            .iter()
            .map(|t| (&t.selection.schema, &t.selection.table))
            .ne(o
                .tables
                .iter()
                .map(|t| (&t.selection.schema, &t.selection.table)))
        {
            return Err(TablesChanged);
        }
        for (a, b) in self.tables.iter().zip(&o.tables) {
            let table = format!("{}.{}", a.selection.schema, a.selection.table);
            if a.selection != b.selection {
                return Err(ProjectionChanged { table });
            }
            if a.descriptor_digest != b.descriptor_digest {
                return Err(DescriptorChanged { table });
            }
        }
        if self.mapping_versions != o.mapping_versions || self.mapping_digest != o.mapping_digest {
            return Err(MappingChanged);
        }
        if self.destination_schema_digest != o.destination_schema_digest {
            return Err(DestinationSchemaChanged);
        }
        Ok(())
    }
}

// Wire mirror avoids changing CaptureStopReason or deserializing &'static str.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
enum StopReason {
    Ddl {
        tables: Vec<(String, String)>,
        statement: String,
    },
    UnknownDdl {
        statement: String,
        reason: String,
    },
    ColumnCount {
        schema: String,
        table: String,
        expected: usize,
        actual: usize,
    },
    Column {
        schema: String,
        table: String,
        ordinal: u64,
        name: String,
        field: String,
    },
}
const MAP_FIELDS: &[&str] = &[
    "unsupported descriptor metadata",
    "column type",
    "type metadata",
    "column name",
    "nullability",
    "collation",
    "signedness",
    "enum/set labels",
];
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MysqlSchemaStopRecord {
    version: StopVersion,
    boundary: Vec<u8>,
    reason: StopReason,
}
impl MysqlSchemaStopRecord {
    pub fn new(reason: &CaptureStopReason, boundary: &MysqlPosition) -> Result<Self> {
        let reason = match reason {
            CaptureStopReason::Ddl { tables, statement } => StopReason::Ddl {
                tables: tables.clone(),
                statement: statement.clone(),
            },
            CaptureStopReason::UnknownDdl { statement, reason } => StopReason::UnknownDdl {
                statement: statement.clone(),
                reason: reason.clone(),
            },
            CaptureStopReason::TableMap {
                schema,
                table,
                change,
            } => match change {
                TableMapChange::ColumnCount { expected, actual } => StopReason::ColumnCount {
                    schema: schema.clone(),
                    table: table.clone(),
                    expected: *expected,
                    actual: *actual,
                },
                TableMapChange::Column {
                    ordinal,
                    name,
                    field,
                } => StopReason::Column {
                    schema: schema.clone(),
                    table: table.clone(),
                    ordinal: *ordinal,
                    name: name.clone(),
                    field: (*field).into(),
                },
            },
        };
        let r = Self {
            version: StopVersion::V1,
            boundary: boundary.encode()?,
            reason,
        };
        r.validate()?;
        Ok(r)
    }
    fn validate(&self) -> Result<()> {
        position(&self.boundary)?;
        match &self.reason {
            StopReason::Ddl { tables, statement }
                if tables.is_empty()
                    || !valid_name(statement)
                    || tables.iter().any(|(s, t)| !valid_name(s) || !valid_name(t)) =>
            {
                return Err(StartupError::Invalid("DDL stop"));
            }
            StopReason::UnknownDdl { statement, reason }
                if !valid_name(statement) || !valid_name(reason) =>
            {
                return Err(StartupError::Invalid("unknown DDL stop"));
            }
            StopReason::ColumnCount { schema, table, .. }
            | StopReason::Column { schema, table, .. }
                if !valid_name(schema) || !valid_name(table) =>
            {
                return Err(StartupError::Invalid("table-map stop"));
            }
            _ => {}
        }
        if let StopReason::Column {
            ordinal,
            name,
            field,
            ..
        } = &self.reason
            && (*ordinal == 0 || !valid_name(name) || !MAP_FIELDS.contains(&field.as_str()))
        {
            return Err(StartupError::Invalid("table-map column"));
        }
        Ok(())
    }
    pub fn boundary(&self) -> Result<MysqlPosition> {
        position(&self.boundary)
    }
    pub fn reason(&self) -> CaptureStopReason {
        match &self.reason {
            StopReason::Ddl { tables, statement } => CaptureStopReason::Ddl {
                tables: tables.clone(),
                statement: statement.clone(),
            },
            StopReason::UnknownDdl { statement, reason } => CaptureStopReason::UnknownDdl {
                statement: statement.clone(),
                reason: reason.clone(),
            },
            StopReason::ColumnCount {
                schema,
                table,
                expected,
                actual,
            } => CaptureStopReason::TableMap {
                schema: schema.clone(),
                table: table.clone(),
                change: TableMapChange::ColumnCount {
                    expected: *expected,
                    actual: *actual,
                },
            },
            StopReason::Column {
                schema,
                table,
                ordinal,
                name,
                field,
            } => CaptureStopReason::TableMap {
                schema: schema.clone(),
                table: table.clone(),
                change: TableMapChange::Column {
                    ordinal: *ordinal,
                    name: name.clone(),
                    field: MAP_FIELDS
                        .iter()
                        .copied()
                        .find(|f| *f == field)
                        .expect("validated stop field"),
                },
            },
        }
    }
    pub fn encode(&self) -> Result<String> {
        self.validate()?;
        Ok(serde_json::to_string(self)?)
    }
    pub fn decode(text: &str) -> Result<Self> {
        let r: Self = serde_json::from_str(text)?;
        r.validate()?;
        Ok(r)
    }
}
#[cfg(test)]
#[path = "startup_tests.rs"]
mod tests;
