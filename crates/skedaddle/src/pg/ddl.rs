//! Transactional DDL capture and conservative transaction preflight (D12).
//!
//! Callers must buffer the entire transaction, preflight all its records, and
//! only then apply its DML. Capture must continue after a boundary stop. Message
//! LSNs are not commit positions. An end trigger cannot guard intermediate
//! commits of concurrent/split-transaction DDL: prohibit those commands while
//! migrating. Milestone 1 detects hazards at preflight, double-samples a scoped
//! catalog fingerprint around slot creation, and persists sticky drift/hazard
//! blockers at readiness watermarks and the final cutover marker. Any scoped
//! drift (including captured transactional DDL, F10) requires quiescence and a
//! fresh snapshot. Polls can miss transient reverted changes; a start-time
//! ddl_command_start guard remains a planned follow-up (S3b).

mod operational;
pub use operational::*;

mod classify;
mod sql;

pub use classify::*;
pub use sql::*;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;

pub const MESSAGE_PREFIX: &str = "skedaddle";
pub const WIRE_FORMAT: u32 = 1;
pub const MAX_MESSAGE_BYTES: usize = 8 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DdlError(pub String);

impl fmt::Display for DdlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for DdlError {}

/// Catalog data belongs to the statement's event, never a later live query.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DdlRecord {
    pub format: u32,
    pub installation: String,
    /// Administrative origin is an integrity convenience, not a security boundary.
    /// `_skedaddle`-only DDL is safe because MigrationDefinition::validate and
    /// runner::selection exclude that exact reserved schema from mappings.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin: Option<String>,
    pub server_major: u32,
    pub version: u64,
    pub tag: String,
    pub xid: String,
    pub query: String,
    pub commands: Vec<DdlCommand>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DdlCommand {
    /// Absent only for PostgreSQL's identity-less privilege commands.
    #[serde(deserialize_with = "Deserialize::deserialize")]
    pub classid: Option<u32>,
    #[serde(deserialize_with = "Deserialize::deserialize")]
    pub objid: Option<u32>,
    #[serde(deserialize_with = "Deserialize::deserialize")]
    pub objsubid: Option<i32>,
    pub command_tag: String,
    pub object_type: String,
    pub schema_name: Option<String>,
    #[serde(deserialize_with = "Deserialize::deserialize")]
    pub object_identity: Option<String>,
    pub in_extension: bool,
    pub columns: Option<Vec<Column>>,
    pub relation: Option<Value>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Column {
    pub attnum: i32,
    pub name: String,
    #[serde(rename = "type")]
    pub type_name: String,
    pub notnull: bool,
    pub dropped: bool,
    pub identity: String,
    pub generated: String,
    pub missing: bool,
    pub missing_value: Option<Value>,
    pub default: Option<String>,
    /// Type OID, typmod, collation, ACL, storage, compression and options.
    pub attributes: Value,
}

impl DdlRecord {
    fn validate(&self, installation: &str) -> Result<(), DdlError> {
        validate_id(installation)?;
        if self.format != WIRE_FORMAT
            || self.installation != installation
            || !(16..=18).contains(&self.server_major)
            || self.version == 0
            || self.version > i64::MAX as u64
            || self.tag.is_empty()
            || self.query.is_empty()
            || !self.xid.bytes().all(|b| b.is_ascii_digit())
            || !self.xid.parse::<u64>().is_ok_and(|xid| xid > 0)
        {
            return Err(DdlError("foreign or malformed DDL envelope".into()));
        }
        for command in &self.commands {
            // SCT_Grant and SCT_AlterDefaultPrivileges have no object address.
            // Permit only the complete identity-less shape with matching tags.
            let identity_less = matches!(
                self.tag.as_str(),
                "GRANT" | "REVOKE" | "ALTER DEFAULT PRIVILEGES"
            ) && command.command_tag == self.tag
                && command.classid.is_none()
                && command.objid.is_none()
                && command.objsubid.is_none()
                && command.schema_name.is_none()
                && command.object_identity.is_none()
                && command.columns.is_none()
                && command.relation.is_none();
            let addressed = command.classid.is_some_and(|oid| oid > 0)
                && command.objid.is_some_and(|oid| oid > 0)
                && command.objsubid.is_some_and(|subid| subid >= 0)
                && command
                    .object_identity
                    .as_ref()
                    .is_some_and(|s| !s.is_empty());
            if (!identity_less && !addressed)
                || command.command_tag.is_empty()
                || command.object_type.is_empty()
                || command.relation.as_ref().is_some_and(|v| !v.is_object())
            {
                return Err(DdlError("malformed DDL command".into()));
            }
            if let Some(columns) = &command.columns {
                let mut previous = 0;
                for column in columns {
                    if column.attnum <= previous
                        || column.name.is_empty()
                        || column.type_name.is_empty()
                        || !column.attributes.is_object()
                        || column.missing != column.missing_value.is_some()
                    {
                        return Err(DdlError("malformed DDL column snapshot".into()));
                    }
                    previous = column.attnum;
                }
            }
        }
        Ok(())
    }
}

pub fn encode(record: &DdlRecord) -> Result<Vec<u8>, DdlError> {
    record.validate(&record.installation)?;
    let bytes = serde_json::to_vec(record).map_err(|e| DdlError(e.to_string()))?;
    if bytes.len() > MAX_MESSAGE_BYTES {
        return Err(DdlError("DDL message exceeds size limit".into()));
    }
    Ok(bytes)
}

/// Reject unrelated messages rather than interpreting them as schema changes.
pub fn decode(
    message: &super::pgoutput::Message<'_>,
    installation: &str,
) -> Result<DdlRecord, DdlError> {
    let super::pgoutput::Message::LogicalMessage {
        transactional: true,
        prefix: MESSAGE_PREFIX,
        content,
        ..
    } = message
    else {
        return Err(DdlError("not a transactional Skedaddle DDL message".into()));
    };
    if content.len() > MAX_MESSAGE_BYTES {
        return Err(DdlError("DDL message exceeds size limit".into()));
    }
    let record: DdlRecord = serde_json::from_slice(content).map_err(|e| DdlError(e.to_string()))?;
    record.validate(installation)?;
    Ok(record)
}

/// Broad S3 catalog fingerprint. It detects persistent drift, not TRUNCATE,
/// reverted drift, or a first affected commit. Recovery requires quiescence and
/// a fresh consistent snapshot; polling alone does not establish apply safety.
pub const FINGERPRINT_SQL: &str = include_str!("../../artifacts/spikes/s3-ddl/fingerprint.sql");

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PollPoint {
    Watermark(u64),
    FinalMarker(u64),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DriftBlocker {
    pub point: PollPoint,
    pub expected: String,
    pub observed: String,
    pub remediation: &'static str,
}

/// No-event-trigger profile: every watermark AND final marker must be polled.
/// Once blocked it remains blocked, even if a later poll returns to baseline.
#[derive(Clone, Debug)]
pub struct FingerprintProfile {
    baseline: String,
    blocker: Option<DriftBlocker>,
}

impl FingerprintProfile {
    pub fn new(baseline: String) -> Result<Self, DdlError> {
        validate_fingerprint(&baseline)?;
        Ok(Self {
            baseline,
            blocker: None,
        })
    }

    pub fn poll(&mut self, point: PollPoint, observed: String) -> Result<(), DriftBlocker> {
        if let Some(blocker) = &self.blocker {
            return Err(blocker.clone());
        }
        if validate_fingerprint(&observed).is_err() || observed != self.baseline {
            let blocker = DriftBlocker {
                point,
                expected: self.baseline.clone(),
                observed,
                remediation: "Keep capture buffering; stop apply and readiness. Quiesce schema changes and restart from a fresh consistent snapshot; a fingerprint cannot identify the first affected commit.",
            };
            self.blocker = Some(blocker.clone());
            return Err(blocker);
        }
        Ok(())
    }

    pub fn blocker(&self) -> Option<&DriftBlocker> {
        self.blocker.as_ref()
    }
}

fn validate_fingerprint(value: &str) -> Result<(), DdlError> {
    if value.len() != 32 || !value.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(DdlError("expected a catalog MD5 fingerprint".into()));
    }
    Ok(())
}

pub(crate) const ADMIN_INSTALLATION_SQL: &str = "SELECT substr(e.evtname,length('skedaddle_ddl_')+1) FROM pg_event_trigger e JOIN pg_proc p ON p.oid=e.evtfoid JOIN pg_namespace n ON n.oid=p.pronamespace WHERE starts_with(e.evtname,'skedaddle_ddl_') AND e.evtname=p.proname AND n.nspname='_skedaddle' AND e.evtevent='ddl_command_end' AND e.evtenabled='O' AND e.evttags IS NULL AND p.prosecdef";

/// Resolve the endpoint's installed identity, never the migration name.
/// No installation needs no tag; ambiguous installations fail closed.
/// Origin is an integrity convenience: reserved-schema selection guards, not
/// this caller-controlled setting, keep metadata outside mapped objects.
pub(crate) fn administrative_origin(
    connection: &mut impl super::fence::Connection,
    timeout: std::time::Duration,
) -> Result<String, DdlError> {
    let rows = connection
        .query(ADMIN_INSTALLATION_SQL, timeout)
        .map_err(DdlError)?;
    administrative_origin_sql(&rows)
}

pub(crate) fn administrative_origin_sql(rows: &super::fence::Rows) -> Result<String, DdlError> {
    match rows.as_slice() {
        [] => Ok(String::new()),
        [row] if row.len() == 1 => {
            validate_id(&row[0])?;
            Ok(format!("SET LOCAL skedaddle.admin_origin = '{}'; ", row[0]))
        }
        _ => Err(DdlError(
            "ambiguous or malformed DDL capture installation lookup".into(),
        )),
    }
}
