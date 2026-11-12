//! Destination planning and ordered execution (see [`executor`]).
//!
//! Execute `session_setup` once on a fresh, dedicated origin session, then run
//! every selected statement in ONE transaction. Check every returned row count
//! before continuing; any fault requires rollback, including the progress write.
//! A fallback is selected only when its immediately preceding UPDATE returned
//! zero rows. Do not advance the ordered BufferReader checkpoint before commit.
//! Range lookup is coordinator-owned: absence of a durable ChunkDone is Copying.
//! Preflight supplies writable columns and matching catalog/wire descriptors,
//! verifies domain constraints, and configures the connection's client_encoding
//! to match the mapping's text encoding. This planner does not qualify a schema.

use std::{collections::BTreeMap, fmt, io};

use crate::{
    event::{AssembledTxn, ContentDigest, Event, EventKind, OpaqueValue, Row, SourceOrigin, TxnId},
    mapping::{MappingError, PgType, Value},
};
use tokio_postgres::types::{Format, IsNull, Kind, ToSql, Type, private::BytesMut};

pub mod executor;

type Result<T> = std::result::Result<T, ApplyError>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ApplyError {
    Invalid(&'static str),
    Mapping(MappingError),
    InvalidTransaction,
    MissingRow { table: String, ordinal: u64 },
    UnexpectedRowCount(u64),
    DigestMismatch,
    InconsistentProgress,
    MissingDigestEvidence,
    DdlBoundary { ordinal: u64 },
}
impl fmt::Display for ApplyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "destination apply: {self:?}")
    }
}
impl std::error::Error for ApplyError {}
impl From<MappingError> for ApplyError {
    fn from(value: MappingError) -> Self {
        Self::Mapping(value)
    }
}

#[derive(Clone, Debug)]
pub struct Column {
    pub name: String,
    /// Canonical source/catalog identity, compared even for NULL values.
    pub type_id: String,
    pub pg_type: PgType,
    /// Actual destination wire type (including destination-local array OIDs).
    pub wire_type: Type,
}

#[derive(Clone, Debug)]
pub struct Table {
    /// Matches Event.table; not parsed as SQL.
    pub event_table: String,
    pub schema: String,
    pub name: String,
    pub schema_version: u64,
    pub columns: Vec<Column>,
    /// Non-null unique/primary key, in catalog order.
    pub key: Vec<String>,
}
impl Table {
    fn sql_name(&self) -> Result<String> {
        Ok(format!("{}.{}", ident(&self.schema)?, ident(&self.name)?))
    }
    fn column(&self, name: &str) -> Result<&Column> {
        self.columns
            .iter()
            .find(|c| c.name == name)
            .ok_or(ApplyError::Invalid("unknown column"))
    }
    fn validate(&self) -> Result<()> {
        self.sql_name()?;
        let mut names = std::collections::BTreeSet::new();
        for column in &self.columns {
            ident(&column.name)?;
            check_wire_type(&column.pg_type, &column.wire_type)?;
            if column.type_id.is_empty() || !names.insert(&column.name) {
                return Err(ApplyError::Invalid("column descriptor"));
            }
        }
        let mut keys = std::collections::BTreeSet::new();
        if self.key.is_empty() || self.event_table.is_empty() {
            return Err(ApplyError::Invalid("table identity/key"));
        }
        for name in &self.key {
            self.column(name)?;
            if !keys.insert(name) {
                return Err(ApplyError::Invalid("duplicate key column"));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RangeState {
    Copying,
    Completed,
}

#[derive(Clone, Debug)]
pub struct PlanContext {
    pub stream: String,
    /// Manifest containing this record; mseq alone is NOT a transaction ID.
    pub mseq: u64,
    /// Must already exist, and be dedicated to this stream.
    pub origin: String,
    /// Explicit timestamp for synthetic ChunkRows/control records only.
    pub synthetic_commit_time: Option<i64>,
}

/// A checked PG Bind parameter. Text bytes are produced by the mapping codec,
/// not passed through String's binary ToSql implementation. Float scalars and
/// float arrays use binary to preserve all NaN payload/sign bits and -0.
#[derive(Clone, Debug)]
pub struct Parameter {
    wire_type: Type,
    format: Format,
    bytes: Option<Vec<u8>>,
}
impl Parameter {
    pub fn wire_type(&self) -> &Type {
        &self.wire_type
    }
    pub fn format(&self) -> Format {
        self.format
    }
    pub fn bytes(&self) -> Option<&[u8]> {
        self.bytes.as_deref()
    }
    fn text(ty: Type, bytes: impl Into<Vec<u8>>) -> Self {
        Self {
            wire_type: ty,
            format: Format::Text,
            bytes: Some(bytes.into()),
        }
    }
}
impl ToSql for Parameter {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> std::result::Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        if ty != &self.wire_type {
            return Err(Box::new(io::Error::new(
                io::ErrorKind::InvalidInput,
                "destination parameter type mismatch",
            )));
        }
        match &self.bytes {
            Some(bytes) => {
                out.extend_from_slice(bytes);
                Ok(IsNull::No)
            }
            None => Ok(IsNull::Yes),
        }
    }
    fn accepts(_: &Type) -> bool {
        true
    }
    fn encode_format(&self, _: &Type) -> Format {
        self.format
    }
    tokio_postgres::types::to_sql_checked!();
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RowCount {
    Any,
    AtMostOne,
    ExactlyOne,
    /// Zero is a verifier-visible fault, never a request to upsert.
    Existing {
        table: String,
        ordinal: u64,
    },
}

#[derive(Clone, Debug)]
pub struct Statement {
    pub sql: String,
    pub parameters: Vec<Parameter>,
    pub row_count: RowCount,
    /// Execute only if the immediately previous statement returned zero rows.
    pub if_previous_missing: bool,
}
impl Statement {
    pub fn should_execute(&self, previous_rows: Option<u64>) -> Result<bool> {
        if self.if_previous_missing {
            Ok(previous_rows.ok_or(ApplyError::Invalid("fallback has no previous result"))? == 0)
        } else {
            Ok(true)
        }
    }
    /// For SELECT ... FOR UPDATE use returned rows, otherwise affected rows.
    pub fn check_rows(&self, rows: u64) -> Result<()> {
        match &self.row_count {
            RowCount::Any => Ok(()),
            RowCount::AtMostOne if rows <= 1 => Ok(()),
            RowCount::ExactlyOne if rows == 1 => Ok(()),
            RowCount::Existing { .. } if rows == 1 => Ok(()),
            RowCount::Existing { table, ordinal } if rows == 0 => Err(ApplyError::MissingRow {
                table: table.clone(),
                ordinal: *ordinal,
            }),
            _ => Err(ApplyError::UnexpectedRowCount(rows)),
        }
    }
}

/// Record identity authenticated by the ordered BufferReader. Legacy progress
/// has no evidence and cannot authorize the equal-H exception.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecordKind {
    Transaction = 1,
    ChunkRows = 2,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordEvidence {
    kind: RecordKind,
    manifest_digest: ContentDigest,
    previous_manifest_digest: ContentDigest,
    // Bind a cloned proof to the exact verified record, not mutable Progress.
    identity: (String, u64, TxnId, u64, ContentDigest),
}
impl RecordEvidence {
    fn new(
        kind: RecordKind,
        manifest_digest: ContentDigest,
        previous_manifest_digest: ContentDigest,
        p: &Progress,
    ) -> Self {
        Self {
            kind,
            manifest_digest,
            previous_manifest_digest,
            identity: (p.stream.clone(), p.mseq, p.txn.clone(), p.lsn, p.digest),
        }
    }
    fn verify_identity(&self, p: &Progress) -> Result<()> {
        if self.identity.0 != p.stream
            || self.identity.1 != p.mseq
            || self.identity.2 != p.txn
            || self.identity.3 != p.lsn
        {
            return Err(ApplyError::InconsistentProgress);
        }
        if self.identity.4 != p.digest {
            return Err(ApplyError::DigestMismatch);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Progress {
    pub stream: String,
    pub mseq: u64,
    pub txn: TxnId,
    pub lsn: u64,
    pub digest: ContentDigest,
    pub evidence: Option<RecordEvidence>,
}

#[derive(Clone, Debug)]
pub struct TransactionPlan {
    /// Preserved publisher metadata; destination origin setup still uses the
    /// dedicated stream origin and connector commit LSN for durable progress.
    pub source_origin: Option<SourceOrigin>,
    pub session_setup: Statement,
    /// BEGIN/COMMIT are executor responsibilities; a row-count fault rolls back.
    pub statements: Vec<Statement>,
    pub progress: Progress,
}

fn statement(sql: String, parameters: Vec<Parameter>, row_count: RowCount) -> Statement {
    Statement {
        sql,
        parameters,
        row_count,
        if_previous_missing: false,
    }
}
fn ident(name: &str) -> Result<String> {
    if name.is_empty() || name.contains('\0') {
        return Err(ApplyError::Invalid("SQL identifier"));
    }
    Ok(format!("\"{}\"", name.replace('"', "\"\"")))
}
fn lsn(txn: &AssembledTxn) -> Result<u64> {
    Ok(u64::from_be_bytes(
        txn.commit_position
            .bytes()
            .try_into()
            .map_err(|_| ApplyError::Invalid("PG commit position must be an eight-byte LSN"))?,
    ))
}

/// Event.key must be the OLD key for updates/key changes and the current key
/// otherwise. `range` classifies it using durable ChunkDone state. ChunkRows
/// always overwrite and bypass the range lookup.
/// DDL blocks the complete transaction; markers have no row effects.
pub fn plan_transaction(
    txn: &AssembledTxn,
    tables: &[Table],
    context: &PlanContext,
    range: impl FnMut(&Table, &Row) -> Result<RangeState>,
) -> Result<TransactionPlan> {
    plan_transaction_inner(txn, tables, context, None, range)
}

/// Permit only D7/D8 tool-internal DDL bound to the caller's configured
/// installation. Canonical events, ordinals, digest and receipt stay untouched.
/// Capture authenticates global continuity; apply also checks continuity between
/// DDL records within this transaction, its PG XID and the exact control shape.
pub fn plan_transaction_with_ddl(
    txn: &AssembledTxn,
    tables: &[Table],
    context: &PlanContext,
    installation: &str,
    range: impl FnMut(&Table, &Row) -> Result<RangeState>,
) -> Result<TransactionPlan> {
    plan_transaction_inner(txn, tables, context, Some(installation), range)
}

fn plan_transaction_inner(
    txn: &AssembledTxn,
    tables: &[Table],
    context: &PlanContext,
    installation: Option<&str>,
    mut range: impl FnMut(&Table, &Row) -> Result<RangeState>,
) -> Result<TransactionPlan> {
    txn.write_canonical(&mut io::sink())
        .map_err(|_| ApplyError::InvalidTransaction)?;
    // Preflight the entire transaction before invoking any row classifier.
    let mut previous_version: Option<u64> = None;
    let mut previous_xid: Option<String> = None;
    for event in &txn.events {
        if event.kind != EventKind::DdlBoundary {
            continue;
        }
        let rejected = || ApplyError::DdlBoundary {
            ordinal: event.ordinal_in_txn,
        };
        let installation = installation.ok_or_else(rejected)?;
        let row = event.after.as_ref().ok_or_else(rejected)?;
        let bytes = row
            .get("control")
            .filter(|v| v.type_id == "skedaddle.ddl.v1")
            .and_then(|v| v.bytes.as_deref())
            .ok_or_else(rejected)?;
        if row.len() != 1
            || event.table.is_some()
            || !event.key.is_empty()
            || event.before.is_some()
            || !event.unchanged_cols.is_empty()
        {
            return Err(rejected());
        }
        let record = super::ddl::decode(
            &super::pgoutput::Message::LogicalMessage {
                transactional: true,
                lsn: 0,
                prefix: super::ddl::MESSAGE_PREFIX,
                content: bytes,
            },
            installation,
        )
        .map_err(|_| rejected())?;
        // pgoutput carries the low 32 bits of PostgreSQL's full transaction ID.
        let xid = record.xid.parse::<u64>().map_err(|_| rejected())? as u32;
        if txn.txn.0.len() != 12
            || txn.txn.0[..4] != xid.to_be_bytes()
            || event.schema_version != record.version
            || previous_version.is_some_and(|v| v.checked_add(1) != Some(record.version))
            || previous_xid.as_ref().is_some_and(|xid| xid != &record.xid)
            || !super::ddl::accepted_tool_internal(&record, installation)
        {
            return Err(rejected());
        }
        previous_version = Some(record.version);
        previous_xid = Some(record.xid);
    }
    if context.stream.is_empty()
        || context.stream.contains('\0')
        || context.origin.is_empty()
        || context.origin.contains('\0')
    {
        return Err(ApplyError::Invalid("stream/origin"));
    }
    let mut table_map = BTreeMap::new();
    for table in tables {
        table.validate()?;
        if table_map.insert(&table.event_table, table).is_some() {
            return Err(ApplyError::Invalid("duplicate table"));
        }
    }
    let lsn = lsn(txn)?;
    let time = txn
        .commit_time
        .or_else(|| {
            txn.events
                .iter()
                .all(|e| {
                    matches!(
                        e.kind,
                        EventKind::ChunkRows | EventKind::Watermark | EventKind::FinalMarker
                    )
                })
                .then_some(context.synthetic_commit_time)
                .flatten()
        })
        .ok_or(ApplyError::Invalid("missing origin commit time"))?;
    let pg_time = time
        .checked_sub(946_684_800_000_000)
        .ok_or(ApplyError::Invalid("origin timestamp overflow"))?;
    let session_setup = statement(
        "SELECT pg_catalog.pg_replication_origin_session_setup($1)".into(),
        vec![Parameter::text(Type::TEXT, context.origin.as_bytes())],
        RowCount::ExactlyOne,
    );
    let mut statements = vec![
        statement(
            "SET LOCAL session_replication_role = replica".into(),
            vec![],
            RowCount::Any,
        ),
        statement(
            "SELECT pg_catalog.pg_replication_origin_xact_setup($1, $2)".into(),
            vec![
                Parameter::text(Type::PG_LSN, format!("{:X}/{:X}", lsn >> 32, lsn as u32)),
                Parameter {
                    wire_type: Type::TIMESTAMPTZ,
                    format: Format::Binary,
                    bytes: Some(pg_time.to_be_bytes().to_vec()),
                },
            ],
            RowCount::ExactlyOne,
        ),
    ];
    for event in &txn.events {
        match event.kind {
            EventKind::DdlBoundary | EventKind::Watermark | EventKind::FinalMarker => continue,
            _ => {}
        }
        if let EventKind::Truncate {
            relations,
            cascade,
            restart_identity,
        } = &event.kind
        {
            let mut names = Vec::with_capacity(relations.len());
            for relation in relations {
                let table = table_map
                    .get(relation)
                    .ok_or(ApplyError::Invalid("unknown truncate table"))?;
                if event.schema_version != table.schema_version {
                    return Err(ApplyError::Invalid("schema version mismatch"));
                }
                names.push(table.sql_name()?);
            }
            let mut sql = format!("TRUNCATE TABLE {}", names.join(", "));
            if *restart_identity {
                sql.push_str(" RESTART IDENTITY");
            }
            if *cascade {
                sql.push_str(" CASCADE");
            }
            statements.push(statement(sql, vec![], RowCount::Any));
            continue;
        }
        let table = table_map
            .get(
                &event
                    .table
                    .as_ref()
                    .ok_or(ApplyError::Invalid("row event without table"))?,
            )
            .ok_or(ApplyError::Invalid("unknown table"))?;
        if event.schema_version != table.schema_version {
            return Err(ApplyError::Invalid("schema version mismatch"));
        }
        for name in &event.unchanged_cols {
            table.column(name)?;
            if table.key.contains(name) {
                return Err(ApplyError::Invalid("unchanged key"));
            }
        }
        for row in [
            &event.key,
            event.before.as_ref().unwrap_or(&event.key),
            event.after.as_ref().unwrap_or(&event.key),
        ] {
            for name in row.keys() {
                table.column(name)?;
            }
        }
        let old_key = key(table, &event.key)?;
        let state = if event.kind == EventKind::ChunkRows {
            RangeState::Copying
        } else {
            range(table, &old_key)?
        };
        let existing = if state == RangeState::Completed {
            RowCount::Existing {
                table: table.event_table.clone(),
                ordinal: event.ordinal_in_txn,
            }
        } else {
            RowCount::AtMostOne
        };
        match event.kind {
            EventKind::Insert | EventKind::ChunkRows => {
                let row = present_row(table, event, &old_key)?;
                if event.kind == EventKind::ChunkRows
                    && (row.len() != table.columns.len() || !event.unchanged_cols.is_empty())
                {
                    return Err(ApplyError::Invalid("ChunkRows requires a complete row"));
                }
                statements.push(insert(
                    table,
                    &row,
                    state == RangeState::Copying || event.kind == EventKind::ChunkRows,
                )?);
            }
            EventKind::Update => {
                let row = present_row(table, event, &old_key)?;
                let changed = event
                    .after
                    .as_ref()
                    .ok_or(ApplyError::Invalid("update without after"))?;
                let mut params = Vec::new();
                let mut assignments = Vec::new();
                for column in &table.columns {
                    if let Some(value) = changed
                        .get(&column.name)
                        .filter(|_| !event.unchanged_cols.contains(&column.name))
                    {
                        params.push(parameter(column, value)?);
                        assignments.push(format!("{} = ${}", ident(&column.name)?, params.len()));
                    }
                }
                let predicate = predicate(table, &old_key, &mut params)?;
                let sql = if assignments.is_empty() {
                    format!(
                        "SELECT 1 FROM {} WHERE {predicate} FOR UPDATE",
                        table.sql_name()?
                    )
                } else {
                    format!(
                        "UPDATE {} SET {} WHERE {predicate}",
                        table.sql_name()?,
                        assignments.join(", ")
                    )
                };
                statements.push(statement(sql, params, existing));
                if state == RangeState::Copying {
                    let mut fallback = insert(table, &row, true)?;
                    fallback.if_previous_missing = true;
                    statements.push(fallback);
                }
            }
            EventKind::Delete => {
                let mut params = Vec::new();
                let predicate = predicate(table, &old_key, &mut params)?;
                statements.push(statement(
                    format!("DELETE FROM {} WHERE {predicate}", table.sql_name()?),
                    params,
                    existing,
                ));
            }
            _ => unreachable!("handled above"),
        }
    }
    let progress = Progress {
        stream: context.stream.clone(),
        mseq: context.mseq,
        txn: txn.txn.clone(),
        lsn,
        digest: txn.digest,
        evidence: None,
    };
    statements.push(progress_statement(&progress));
    Ok(TransactionPlan {
        source_origin: txn.source_origin.clone(),
        session_setup,
        statements,
        progress,
    })
}

fn progress_statement(p: &Progress) -> Statement {
    let (kind, manifest, previous) = evidence_parameters(p);
    statement(
        "INSERT INTO _skedaddle.apply_progress (stream, last_mseq, last_txn, last_position, last_digest, record_kind, manifest_digest, previous_manifest_digest) VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT (stream) DO UPDATE SET last_mseq = EXCLUDED.last_mseq, last_txn = EXCLUDED.last_txn, last_position = EXCLUDED.last_position, last_digest = EXCLUDED.last_digest, record_kind = EXCLUDED.record_kind, manifest_digest = EXCLUDED.manifest_digest, previous_manifest_digest = EXCLUDED.previous_manifest_digest".into(),
        vec![Parameter::text(Type::TEXT, p.stream.as_bytes()), Parameter::text(Type::NUMERIC, p.mseq.to_string()), binary(Type::BYTEA, p.txn.0.clone()), binary(Type::BYTEA, p.lsn.to_be_bytes().to_vec()), binary(Type::BYTEA, p.digest.to_vec()), kind, manifest, previous], RowCount::ExactlyOne,
    )
}
fn evidence_parameters(p: &Progress) -> (Parameter, Parameter, Parameter) {
    let parameter = |wire_type, bytes| Parameter {
        wire_type,
        format: Format::Binary,
        bytes,
    };
    (
        parameter(
            Type::INT2,
            p.evidence
                .as_ref()
                .map(|e| (e.kind as i16).to_be_bytes().to_vec()),
        ),
        parameter(
            Type::BYTEA,
            p.evidence.as_ref().map(|e| e.manifest_digest.to_vec()),
        ),
        parameter(
            Type::BYTEA,
            p.evidence
                .as_ref()
                .map(|e| e.previous_manifest_digest.to_vec()),
        ),
    )
}

fn key(table: &Table, row: &Row) -> Result<Row> {
    table
        .key
        .iter()
        .map(|name| {
            let value = row.get(name).ok_or(ApplyError::Invalid("incomplete key"))?;
            let param = parameter(table.column(name)?, value)?;
            if param.bytes.is_none() {
                return Err(ApplyError::Invalid("NULL key"));
            }
            Ok((name.clone(), value.clone()))
        })
        .collect()
}
fn present_row(table: &Table, event: &Event, old_key: &Row) -> Result<Row> {
    let mut row = event
        .after
        .clone()
        .ok_or(ApplyError::Invalid("missing after row"))?;
    row.retain(|name, _| !event.unchanged_cols.contains(name));
    for (name, value) in old_key {
        row.entry(name.clone()).or_insert_with(|| value.clone());
    }
    key(table, &row)?;
    Ok(row)
}
fn predicate(table: &Table, key: &Row, params: &mut Vec<Parameter>) -> Result<String> {
    table
        .key
        .iter()
        .map(|name| {
            params.push(parameter(table.column(name)?, &key[name])?);
            Ok(format!("{} = ${}", ident(name)?, params.len()))
        })
        .collect::<Result<Vec<_>>>()
        .map(|v| v.join(" AND "))
}
fn insert(table: &Table, row: &Row, upsert: bool) -> Result<Statement> {
    let mut params = Vec::new();
    let mut columns = Vec::new();
    let mut placeholders = Vec::new();
    let mut assignments = Vec::new();
    for column in &table.columns {
        if let Some(value) = row.get(&column.name) {
            params.push(parameter(column, value)?);
            let name = ident(&column.name)?;
            columns.push(name.clone());
            placeholders.push(format!("${}", params.len()));
            if !table.key.contains(&column.name) {
                assignments.push(format!("{name} = EXCLUDED.{name}"));
            }
        }
    }
    let mut sql = format!(
        "INSERT INTO {} ({}) VALUES ({})",
        table.sql_name()?,
        columns.join(", "),
        placeholders.join(", ")
    );
    let row_count = if upsert {
        let keys = table
            .key
            .iter()
            .map(|n| ident(n))
            .collect::<Result<Vec<_>>>()?
            .join(", ");
        sql.push_str(&format!(" ON CONFLICT ({keys}) "));
        if assignments.is_empty() {
            sql.push_str("DO NOTHING");
            RowCount::AtMostOne
        } else {
            sql.push_str(&format!("DO UPDATE SET {}", assignments.join(", ")));
            RowCount::ExactlyOne
        }
    } else {
        RowCount::ExactlyOne
    };
    Ok(statement(sql, params, row_count))
}
fn binary(ty: Type, bytes: Vec<u8>) -> Parameter {
    Parameter {
        wire_type: ty,
        format: Format::Binary,
        bytes: Some(bytes),
    }
}

fn check_wire_type(pg_type: &PgType, wire: &Type) -> Result<()> {
    let expected = match pg_type {
        PgType::Bool => Type::BOOL,
        PgType::Int2 => Type::INT2,
        PgType::Int4 => Type::INT4,
        PgType::Int8 => Type::INT8,
        PgType::Numeric => Type::NUMERIC,
        PgType::Float4 => Type::FLOAT4,
        PgType::Float8 => Type::FLOAT8,
        PgType::Text { .. } => Type::TEXT,
        PgType::Varchar { .. } => Type::VARCHAR,
        PgType::Char { .. } => Type::BPCHAR,
        PgType::Bytea => Type::BYTEA,
        PgType::Uuid => Type::UUID,
        PgType::Date => Type::DATE,
        PgType::Time => Type::TIME,
        PgType::Timetz => Type::TIMETZ,
        PgType::Timestamp => Type::TIMESTAMP,
        PgType::Timestamptz => Type::TIMESTAMPTZ,
        PgType::Interval => Type::INTERVAL,
        PgType::Json => Type::JSON,
        PgType::Jsonb => Type::JSONB,
        PgType::Array(base) => {
            let Kind::Array(element) = wire.kind() else {
                return Err(ApplyError::Invalid("array wire descriptor"));
            };
            return check_wire_type(base, element);
        }
        PgType::Domain { type_id, base, .. } => {
            let Kind::Domain(element) = wire.kind() else {
                return Err(ApplyError::Invalid("domain wire descriptor"));
            };
            if type_id.schema != wire.schema() || type_id.name != wire.name() {
                return Err(ApplyError::Invalid("domain wire identity"));
            }
            return check_wire_type(base, element);
        }
        PgType::Enum {
            type_id, labels, ..
        } => {
            let Kind::Enum(wire_labels) = wire.kind() else {
                return Err(ApplyError::Invalid("enum wire descriptor"));
            };
            return if type_id.schema == wire.schema()
                && type_id.name == wire.name()
                && labels
                    .iter()
                    .map(Vec::as_slice)
                    .eq(wire_labels.iter().map(|s| s.as_bytes()))
            {
                Ok(())
            } else {
                Err(ApplyError::Invalid("enum wire identity"))
            };
        }
        PgType::Other { .. } => return Err(ApplyError::Invalid("unqualified wire type")),
    };
    if &expected == wire {
        Ok(())
    } else {
        Err(ApplyError::Invalid("builtin wire descriptor"))
    }
}

fn parameter(column: &Column, opaque: &OpaqueValue) -> Result<Parameter> {
    if opaque.type_id != column.type_id {
        return Err(ApplyError::Invalid("canonical type identity mismatch"));
    }
    let value = match &opaque.bytes {
        Some(bytes) => Value::from_canonical_bytes(bytes)?,
        None => Value::Null,
    };
    // Canonical Value::Null is not the OpaqueValue SQL-NULL representation.
    if opaque.bytes.is_some() && value == Value::Null {
        return Err(ApplyError::Invalid("noncanonical opaque NULL"));
    }
    let formatted = value.format_pg_text(&column.pg_type);
    if float_type(&column.pg_type) {
        // The text codec performs structural validation before reporting loss.
        if let Err(error) = &formatted
            && !matches!(error, MappingError::LossyText(_))
        {
            return Err(error.clone().into());
        }
        let bytes = float_binary(&value, &column.wire_type)?;
        return Ok(Parameter {
            wire_type: column.wire_type.clone(),
            format: Format::Binary,
            bytes,
        });
    }
    Ok(Parameter {
        wire_type: column.wire_type.clone(),
        format: Format::Text,
        bytes: formatted?,
    })
}
fn float_type(ty: &PgType) -> bool {
    match ty {
        PgType::Float4 | PgType::Float8 => true,
        PgType::Array(base) | PgType::Domain { base, .. } => float_type(base),
        _ => false,
    }
}
fn float_binary(value: &Value, ty: &Type) -> Result<Option<Vec<u8>>> {
    if let Kind::Domain(base) = ty.kind() {
        return float_binary(
            match value {
                Value::Domain { base, .. } => base,
                other => other,
            },
            base,
        );
    }
    Ok(Some(match value {
        Value::Null => return Ok(None),
        Value::Float4(bits) if ty == &Type::FLOAT4 => bits.to_be_bytes().to_vec(),
        Value::Float8(bits) if ty == &Type::FLOAT8 => bits.to_be_bytes().to_vec(),
        Value::Array(array) => {
            let Kind::Array(element) = ty.kind() else {
                return Err(ApplyError::Invalid("array wire type"));
            };
            let mut out = Vec::new();
            out.extend_from_slice(&(array.dimensions.len() as i32).to_be_bytes());
            out.extend_from_slice(&1_i32.to_be_bytes());
            out.extend_from_slice(&element.oid().to_be_bytes());
            for dimension in &array.dimensions {
                let length = i32::try_from(dimension.length)
                    .map_err(|_| ApplyError::Invalid("array dimension overflow"))?;
                out.extend_from_slice(&length.to_be_bytes());
                out.extend_from_slice(&dimension.lower_bound.to_be_bytes());
            }
            for value in &array.elements {
                if let Some(bytes) = float_binary(value, element)? {
                    out.extend_from_slice(
                        &i32::try_from(bytes.len())
                            .map_err(|_| ApplyError::Invalid("array element size"))?
                            .to_be_bytes(),
                    );
                    out.extend_from_slice(&bytes);
                } else {
                    out.extend_from_slice(&(-1_i32).to_be_bytes());
                }
            }
            out
        }
        _ => return Err(ApplyError::Invalid("float wire type")),
    }))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommitDecision {
    Applied,
    NotApplied,
}

/// Reconnect on the SAME origin/stream and read durable origin progress and
/// apply_progress consistently (no concurrent applier). `receipt` is an exact
/// historical transaction receipt when replaying BEFORE the latest row. A lone
/// latest digest cannot prove an older transaction's digest: fail closed.
pub fn resolve_commit(
    target: &Progress,
    latest: Option<&Progress>,
    origin_lsn: u64,
    receipt: Option<&Progress>,
) -> Result<CommitDecision> {
    for progress in [Some(target), latest, receipt].into_iter().flatten() {
        if let Some(evidence) = &progress.evidence {
            evidence.verify_identity(progress)?;
        }
    }
    if let Some(receipt) = receipt {
        verify_receipt(target, receipt)?;
    }
    let chunk = |p: &Progress| {
        p.evidence
            .as_ref()
            .is_some_and(|e| e.kind == RecordKind::ChunkRows)
    };
    let Some(latest) = latest else {
        return if origin_lsn == 0 && receipt.is_none() && !chunk(target) {
            Ok(CommitDecision::NotApplied)
        } else {
            Err(ApplyError::InconsistentProgress)
        };
    };
    if latest.stream != target.stream || latest.lsn != origin_lsn {
        return Err(ApplyError::InconsistentProgress);
    }
    match latest.lsn.cmp(&target.lsn) {
        std::cmp::Ordering::Less => {
            if latest.mseq > target.mseq || receipt.is_some() || chunk(target) {
                return Err(ApplyError::InconsistentProgress);
            }
            Ok(CommitDecision::NotApplied)
        }
        std::cmp::Ordering::Equal => {
            if latest.mseq == target.mseq {
                verify_receipt(target, latest)?;
                return Ok(CommitDecision::Applied);
            }
            if ordered_h_pair(latest, target) && receipt.is_none() {
                return Ok(CommitDecision::NotApplied);
            }
            if ordered_h_pair(target, latest) {
                let receipt = receipt.ok_or(ApplyError::MissingDigestEvidence)?;
                if receipt.evidence.is_none() {
                    return Err(ApplyError::MissingDigestEvidence);
                }
                verify_receipt(target, receipt)?;
                return Ok(CommitDecision::Applied);
            }
            Err(ApplyError::InconsistentProgress)
        }
        std::cmp::Ordering::Greater => {
            if latest.mseq < target.mseq {
                return Err(ApplyError::InconsistentProgress);
            }
            verify_receipt(target, receipt.ok_or(ApplyError::MissingDigestEvidence)?)?;
            Ok(CommitDecision::Applied)
        }
    }
}
/// Both records must be authenticated, adjacent manifests in the same verified
/// chain. Only Transaction(H) -> ChunkRows(H) consumes the equal-position slot.
fn ordered_h_pair(transaction: &Progress, rows: &Progress) -> bool {
    let (Some(t), Some(r)) = (&transaction.evidence, &rows.evidence) else {
        return false;
    };
    transaction.stream == rows.stream
        && transaction.lsn == rows.lsn
        && transaction.mseq.checked_add(1) == Some(rows.mseq)
        && t.kind == RecordKind::Transaction
        && r.kind == RecordKind::ChunkRows
        && t.manifest_digest == r.previous_manifest_digest
}

fn verify_receipt(target: &Progress, receipt: &Progress) -> Result<()> {
    if target.stream != receipt.stream
        || target.mseq != receipt.mseq
        || target.lsn != receipt.lsn
        || target.txn != receipt.txn
    {
        return Err(ApplyError::InconsistentProgress);
    }
    if target.digest != receipt.digest {
        return Err(ApplyError::DigestMismatch);
    }
    // NULL evidence denotes a pre-v2 row, sufficient only for the original
    // exact identity/digest rule. It never authorizes ordered_h_pair.
    if let (Some(target), Some(receipt)) = (&target.evidence, &receipt.evidence)
        && target != receipt
    {
        return Err(ApplyError::InconsistentProgress);
    }
    Ok(())
}

#[cfg(test)]
mod tests;
