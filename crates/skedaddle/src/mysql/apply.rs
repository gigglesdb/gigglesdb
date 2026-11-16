//! MySQL rollback destination. Every committed transaction writes a receipt in
//! the same InnoDB transaction as its effects. Reverse capture MUST exclude a
//! transaction containing an `_skedaddle.apply_receipts` row event (M11).
//! Binlogging stays enabled, including in file-position mode. Apply reads
//! @@session.sql_log_bin and refuses 0; it never writes that restricted variable.
//! Install separately as admin (CREATE on `_skedaddle`.*). The apply account
//! needs SELECT, INSERT, UPDATE, DELETE on each target table and on
//! `_skedaddle`.apply_progress and `_skedaddle`.apply_receipts. No global or
//! dynamic privileges, CREATE, SUPER or session-variable administration needed.
//! Receipt collection removes only exact identities from completed manifests;
//! after a crash, older uncollected receipts may remain (safe conservative GC).
use super::{
    binlog::MysqlValue,
    client::{MysqlConfig, MysqlError},
    schema::TableDescriptor,
};
use crate::{
    event::{AssembledTxn, Event, EventKind, OpaqueValue, SourceIdentity},
    mapping::{
        self, Value,
        mysql::{self, MysqlPgColumn},
    },
};
use mysql_async::{Conn, Params, Value as Param, prelude::Queryable};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

type Result<T> = std::result::Result<T, ApplyError>;
#[derive(Debug)]
pub enum ApplyError {
    Mysql(MysqlError),
    Mapping(mapping::MappingError),
    Event(crate::event::EventError),
    Invalid(&'static str),
    TableShape(String),
    IdentityConflict,
    DigestConflict,
    RowCount {
        table: String,
        actual: u64,
    },
    /// COMMIT may have succeeded; retry this exact transaction, never skip it.
    UnknownOutcome(MysqlError),
    RetryExhausted,
    BinlogDisabled,
    DestinationCopyUnsupported,
    Engine(crate::coordinator::Error),
    Buffer(crate::buffer::reader::Error),
    Blocked,
}
impl std::fmt::Display for ApplyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DestinationCopyUnsupported => {
                f.write_str("MySQL destination copy not supported yet")
            }
            Self::BinlogDisabled => f.write_str("MySQL apply requires @@session.sql_log_bin=1"),
            _ => write!(f, "{self:?}"),
        }
    }
}
impl std::error::Error for ApplyError {}
impl From<MysqlError> for ApplyError {
    fn from(e: MysqlError) -> Self {
        Self::Mysql(e)
    }
}
impl From<mysql_async::Error> for ApplyError {
    fn from(e: mysql_async::Error) -> Self {
        Self::Mysql(e.into())
    }
}
impl From<mapping::MappingError> for ApplyError {
    fn from(e: mapping::MappingError) -> Self {
        Self::Mapping(e)
    }
}
impl From<crate::event::EventError> for ApplyError {
    fn from(e: crate::event::EventError) -> Self {
        Self::Event(e)
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApplyResult {
    Applied,
    AlreadyApplied,
    ResolvedCommitted,
}

const PROGRESS: &str = "CREATE TABLE IF NOT EXISTS _skedaddle.apply_progress (stream VARBINARY(32) NOT NULL, position_domain VARBINARY(1024) NOT NULL, last_position LONGBLOB NOT NULL, PRIMARY KEY(stream)) ENGINE=InnoDB";
const RECEIPTS: &str = "CREATE TABLE IF NOT EXISTS _skedaddle.apply_receipts (stream VARBINARY(32) NOT NULL, position_hash BINARY(32) NOT NULL, position_domain VARBINARY(1024) NOT NULL, position LONGBLOB NOT NULL, txn_id LONGBLOB NOT NULL, digest BINARY(32) NOT NULL, committed_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6), PRIMARY KEY(stream,position_hash)) ENGINE=InnoDB";

/// Explicit installation, never an implicit side effect of apply. Unknown table
/// shapes fail closed; there is no legacy-format fallback or automatic ALTER.
pub async fn install_apply_tables(config: &MysqlConfig) -> Result<()> {
    let mut client = config.connect().await?;
    let c = &mut client.conn;
    c.query_drop("CREATE DATABASE IF NOT EXISTS _skedaddle")
        .await?;
    c.query_drop(PROGRESS).await?;
    c.query_drop(RECEIPTS).await?;
    for (table, expected, keys) in [
        (
            "apply_progress",
            vec![
                ("stream", "varbinary(32)"),
                ("position_domain", "varbinary(1024)"),
                ("last_position", "longblob"),
            ],
            vec!["stream"],
        ),
        (
            "apply_receipts",
            vec![
                ("stream", "varbinary(32)"),
                ("position_hash", "binary(32)"),
                ("position_domain", "varbinary(1024)"),
                ("position", "longblob"),
                ("txn_id", "longblob"),
                ("digest", "binary(32)"),
                ("committed_at", "timestamp(6)"),
            ],
            vec!["stream", "position_hash"],
        ),
    ] {
        let engine: Option<String> = c.exec_first("SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA='_skedaddle' AND TABLE_NAME=?", (table,)).await?;
        let columns: Vec<(String,String,String,String,Option<String>)> = c.exec("SELECT COLUMN_NAME,COLUMN_TYPE,IS_NULLABLE,EXTRA,COLUMN_DEFAULT FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='_skedaddle' AND TABLE_NAME=? ORDER BY ORDINAL_POSITION", (table,)).await?;
        let pk: Vec<String> = c.exec("SELECT COLUMN_NAME FROM information_schema.STATISTICS WHERE TABLE_SCHEMA='_skedaddle' AND TABLE_NAME=? AND INDEX_NAME='PRIMARY' ORDER BY SEQ_IN_INDEX", (table,)).await?;
        let index_count: Option<u64> = c.exec_first("SELECT COUNT(DISTINCT INDEX_NAME) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA='_skedaddle' AND TABLE_NAME=?", (table,)).await?;
        if engine.as_deref() != Some("InnoDB")
            || pk != keys
            || index_count != Some(1)
            || columns.len() != expected.len()
            || columns
                .iter()
                .zip(expected)
                .any(|((name, ty, null, extra, default), (en, et))| {
                    name != en
                        || ty != et
                        || null != "NO"
                        || if en == "committed_at" {
                            extra != "DEFAULT_GENERATED"
                                || default.as_deref() != Some("CURRENT_TIMESTAMP(6)")
                        } else {
                            !extra.is_empty() || default.is_some()
                        }
                })
        {
            return Err(ApplyError::TableShape(table.into()));
        }
    }
    client.conn.disconnect().await?;
    Ok(())
}

struct Table {
    descriptor: TableDescriptor,
    columns: BTreeMap<String, MysqlPgColumn>,
}
/// Explicit lost-reply fault seam, run after the real server COMMIT but before
/// the applier accepts its acknowledgment. Production callers use `apply`.
#[async_trait::async_trait]
pub trait CommitObserver: Send {
    async fn after_commit(&mut self, connection_id: u32) -> std::result::Result<(), MysqlError>;
}
struct NoObserver;
#[async_trait::async_trait]
impl CommitObserver for NoObserver {
    async fn after_commit(&mut self, _: u32) -> std::result::Result<(), MysqlError> {
        Ok(())
    }
}
pub struct MysqlApplier {
    config: MysqlConfig,
    source: SourceIdentity,
    stream: Vec<u8>,
    tables: BTreeMap<String, Table>,
    reader: Option<crate::buffer::reader::BufferReader>,
    pending_receipts: Vec<(u64, [u8; 32])>,
    blocked: bool,
}
impl MysqlApplier {
    /// Descriptor map keys are the table identities carried by destination-shaped
    /// events; descriptors retain the actual MySQL schema/table/PK names.
    pub fn new(
        config: MysqlConfig,
        source: SourceIdentity,
        descriptors: BTreeMap<String, TableDescriptor>,
    ) -> Result<Self> {
        let mut identity = Vec::new();
        for part in [
            &source.engine,
            &source.system_id,
            &source.lineage,
            &source.db_name,
            &source.stream,
        ] {
            identity.extend_from_slice(&(part.len() as u64).to_be_bytes());
            identity.extend_from_slice(part.as_bytes());
        }
        let mut tables = BTreeMap::new();
        for (name, descriptor) in descriptors {
            if descriptor.schema == "_skedaddle" || descriptor.primary_key.is_empty() {
                return Err(ApplyError::Invalid(
                    "reserved schema or missing primary key",
                ));
            }
            let columns: BTreeMap<_, _> = descriptor
                .columns
                .iter()
                .map(|c| Ok((c.name.clone(), mysql::plan_column(c)?)))
                .collect::<Result<_>>()?;
            if columns.len() != descriptor.columns.len()
                || descriptor
                    .primary_key
                    .iter()
                    .any(|k| !columns.contains_key(k))
            {
                return Err(ApplyError::Invalid("descriptor columns/primary key"));
            }
            tables.insert(
                name,
                Table {
                    descriptor,
                    columns,
                },
            );
        }
        Ok(Self {
            config,
            source,
            stream: Sha256::digest(identity).to_vec(),
            tables,
            reader: None,
            pending_receipts: Vec::new(),
            blocked: false,
        })
    }
    /// Bind a reader reconstructed from the durable Runtime applied checkpoint.
    /// Never reuse a failed or externally cancelled adapter.
    pub fn with_reader(mut self, reader: crate::buffer::reader::BufferReader) -> Self {
        self.reader = Some(reader);
        self
    }
    pub async fn buffer_step(&mut self, runtime: &crate::engine::Runtime) -> Result<bool> {
        if self.blocked {
            return Err(ApplyError::Blocked);
        }
        runtime
            .state()
            .await
            .operational_schema
            .require_clear()
            .map_err(|_| ApplyError::Invalid("operational schema blocked"))?;
        self.blocked = true;
        let reader = self
            .reader
            .as_mut()
            .ok_or(ApplyError::Invalid("missing buffer reader"))?;
        let before = reader.checkpoint().clone();
        let Some(txn) = reader.next_txn().await.map_err(ApplyError::Buffer)? else {
            self.blocked = false;
            return Ok(false);
        };
        let mseq = reader
            .last_verified()
            .ok_or(ApplyError::Invalid("missing verified envelope"))?
            .entry()
            .mseq;
        let checkpoint = reader.checkpoint().clone();
        // The reader authenticates the entire manifest before yielding records.
        // Empty ChunkRows still carry envelope evidence at their boundary.
        let chunk = txn.events.iter().any(|e| e.kind == EventKind::ChunkRows)
            || (checkpoint.next_mseq > mseq
                && checkpoint
                    .last_record
                    .as_ref()
                    .is_some_and(|r| r.chunk_rows))
            || (before.next_mseq == mseq
                && before.last_position.as_ref() == Some(&txn.commit_position)
                && before.last_record.as_ref().is_some_and(|r| !r.chunk_rows));
        if chunk {
            return Err(ApplyError::DestinationCopyUnsupported);
        }
        for event in &txn.events {
            if matches!(event.kind, EventKind::Watermark | EventKind::FinalMarker) {
                crate::engine::classify_captured_control(event).map_err(|e| {
                    ApplyError::Engine(crate::coordinator::Error::RecoveryRequired(e.to_string()))
                })?;
            }
        }
        self.apply(&txn).await?;
        self.pending_receipts.push((mseq, position_hash(&txn)));
        complete_boundary(
            async {
                runtime
                    .record_applied(&txn, mseq, &checkpoint)
                    .await
                    .map_err(ApplyError::Engine)
            },
            self.collect_receipts(checkpoint.next_mseq),
        )
        .await?;
        self.blocked = false;
        Ok(true)
    }
    async fn collect_receipts(&mut self, next_mseq: u64) -> Result<()> {
        if !self
            .pending_receipts
            .iter()
            .any(|(mseq, _)| *mseq < next_mseq)
        {
            return Ok(());
        }
        let mut c = self.connect().await?;
        for (mseq, hash) in &self.pending_receipts {
            if *mseq < next_mseq {
                c.exec_drop(
                    "DELETE FROM _skedaddle.apply_receipts WHERE stream=? AND position_hash=?",
                    (&self.stream, hash.as_slice()),
                )
                .await?;
            }
        }
        c.disconnect().await?;
        self.pending_receipts.retain(|(mseq, _)| *mseq >= next_mseq);
        Ok(())
    }
    pub async fn install_apply_tables(&self) -> Result<()> {
        install_apply_tables(&self.config).await
    }
    async fn connect(&self) -> Result<Conn> {
        let mut c = self.config.connect().await?.conn;
        require_binlogging(&mut c).await?;
        c.query_drop("SET NAMES utf8mb4").await?;
        c.query_drop("SET time_zone='+00:00'").await?;
        c.query_drop("SET SESSION sql_mode='STRICT_ALL_TABLES,NO_ZERO_DATE,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'").await?;
        Ok(c)
    }
    pub async fn apply(&mut self, txn: &AssembledTxn) -> Result<ApplyResult> {
        self.apply_with_observer(txn, &mut NoObserver).await
    }
    pub async fn apply_with_observer(
        &mut self,
        txn: &AssembledTxn,
        observer: &mut dyn CommitObserver,
    ) -> Result<ApplyResult> {
        if txn.source != self.source || txn.source_origin.is_some() {
            return Err(ApplyError::Invalid("source identity/origin"));
        }
        txn.write_canonical(&mut std::io::sink())?;
        if txn.commit_position.order_domain().is_empty()
            || txn.commit_position.order_domain().len() > 1024
        {
            return Err(ApplyError::Invalid("position domain"));
        }
        let statements = txn
            .events
            .iter()
            .map(|e| self.statement(e))
            .collect::<Result<Vec<_>>>()?;
        let mut recovering = false;
        for _ in 0..3 {
            let mut c = self.connect().await.map_err(|e| {
                if recovering {
                    match e {
                        ApplyError::Mysql(e) => ApplyError::UnknownOutcome(e),
                        other => other,
                    }
                } else {
                    e
                }
            })?;
            c.query_drop("START TRANSACTION").await?;
            let result = self.effects(&mut c, txn, &statements).await;
            match result {
                Ok(false) => {
                    c.query_drop("ROLLBACK").await?;
                    c.disconnect().await?;
                    return Ok(if recovering {
                        ApplyResult::ResolvedCommitted
                    } else {
                        ApplyResult::AlreadyApplied
                    });
                }
                Err(e) => {
                    let _ = c.query_drop("ROLLBACK").await;
                    let _ = c.disconnect().await;
                    return Err(e);
                }
                Ok(true) => {}
            }
            let commit = match c.query_drop("COMMIT").await {
                Ok(()) => observer.after_commit(c.id()).await,
                Err(e) => Err(MysqlError::from(e)),
            };
            match commit {
                Ok(()) => {
                    let _ = c.disconnect().await;
                    return Ok(ApplyResult::Applied);
                }
                Err(e) => {
                    if !e.is_transient() {
                        let _ = c.query_drop("ROLLBACK").await;
                        let _ = c.disconnect().await;
                        return Err(e.into());
                    }
                    // Drop the dead session. The next iteration locks progress
                    // before reading receipts, waiting out the previous COMMIT.
                    recovering = true;
                }
            }
        }
        Err(ApplyError::RetryExhausted)
    }
    async fn effects(
        &self,
        c: &mut Conn,
        txn: &AssembledTxn,
        statements: &[Option<Statement>],
    ) -> Result<bool> {
        let domain = txn.commit_position.order_domain().as_bytes();
        let position = txn.commit_position.bytes();
        c.exec_drop("INSERT INTO _skedaddle.apply_progress(stream,position_domain,last_position) VALUES(?,?,?) ON DUPLICATE KEY UPDATE stream=stream", (&self.stream,domain,position)).await?;
        let saved: Option<Vec<u8>> = c
            .exec_first(
                "SELECT position_domain FROM _skedaddle.apply_progress WHERE stream=? FOR UPDATE",
                (&self.stream,),
            )
            .await?;
        if saved.as_deref() != Some(domain) {
            return Err(ApplyError::IdentityConflict);
        }
        let hash = position_hash(txn);
        let receipt: Option<Receipt> = c.exec_first("SELECT position_domain,position,txn_id,digest FROM _skedaddle.apply_receipts WHERE stream=? AND position_hash=?", (&self.stream,hash.as_slice())).await?;
        if receipt_decision(txn, receipt.as_ref())? {
            return Ok(false);
        }
        // Descriptor types are preflight input; require transactional storage
        // on the actual destination before issuing any row effect.
        for t in self.tables.values() {
            let engine: Option<String> = c.exec_first("SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?", (&t.descriptor.schema,&t.descriptor.table)).await?;
            if engine.as_deref() != Some("InnoDB") {
                return Err(ApplyError::TableShape(format!(
                    "{}.{} must be InnoDB",
                    t.descriptor.schema, t.descriptor.table
                )));
            }
        }
        for s in statements.iter().flatten() {
            // MySQL affected_rows excludes unchanged UPDATEs. Lock/count the PK
            // match first so legitimate no-op updates still have PG's semantics.
            if let Some((sql, params)) = &s.match_sql {
                let matches: Vec<u8> = c.exec(sql, Params::Positional(params.clone())).await?;
                if matches.len() != 1 {
                    return Err(ApplyError::RowCount {
                        table: s.table.clone(),
                        actual: matches.len() as u64,
                    });
                }
            }
            c.exec_drop(&s.sql, Params::Positional(s.params.clone()))
                .await?;
            if c.affected_rows() > 1 {
                return Err(ApplyError::RowCount {
                    table: s.table.clone(),
                    actual: c.affected_rows(),
                });
            }
        }
        c.exec_drop("INSERT INTO _skedaddle.apply_receipts(stream,position_hash,position_domain,position,txn_id,digest) VALUES(?,?,?,?,?,?)", (&self.stream,hash.as_slice(),domain,position,&txn.txn.0,txn.digest.as_slice())).await?;
        c.exec_drop(
            "UPDATE _skedaddle.apply_progress SET last_position=? WHERE stream=?",
            (position, &self.stream),
        )
        .await?;
        Ok(true)
    }
    fn statement(&self, e: &Event) -> Result<Option<Statement>> {
        if matches!(e.kind, EventKind::Watermark | EventKind::FinalMarker) {
            return Ok(None);
        }
        let name = e
            .table
            .as_ref()
            .ok_or(ApplyError::Invalid("missing table"))?;
        let t = self
            .tables
            .get(name)
            .ok_or(ApplyError::Invalid("unknown table"))?;
        let table = format!(
            "{}.{}",
            quote(&t.descriptor.schema)?,
            quote(&t.descriptor.table)?
        );
        let mut params = Vec::new();
        let mut columns = Vec::new();
        if matches!(e.kind, EventKind::Insert | EventKind::Update) {
            for (name, v) in e
                .after
                .as_ref()
                .ok_or(ApplyError::Invalid("missing after row"))?
            {
                let p = t
                    .columns
                    .get(name)
                    .ok_or(ApplyError::Invalid("unknown column"))?;
                if p.generated || e.unchanged_cols.contains(name) {
                    continue;
                }
                columns.push(quote(name)?);
                params.push(parameter(p, v)?);
            }
            if columns.is_empty() {
                return Err(ApplyError::Invalid("empty writable row"));
            }
        }
        let (sql, match_sql) = match e.kind {
            EventKind::Insert => (
                format!(
                    "INSERT INTO {table} ({}) VALUES ({})",
                    columns.join(","),
                    vec!["?"; columns.len()].join(",")
                ),
                None,
            ),
            EventKind::Update | EventKind::Delete => {
                if e.key.len() != t.descriptor.primary_key.len() {
                    return Err(ApplyError::Invalid("incomplete primary key"));
                }
                let mut predicates = Vec::new();
                let mut keys = Vec::new();
                for key in &t.descriptor.primary_key {
                    let v = e
                        .key
                        .get(key)
                        .ok_or(ApplyError::Invalid("missing primary key"))?;
                    if v.bytes.is_none() {
                        return Err(ApplyError::Invalid("NULL primary key"));
                    }
                    keys.push(parameter(&t.columns[key], v)?);
                    predicates.push(format!("{}=?", quote(key)?));
                }
                let predicate = predicates.join(" AND ");
                let check = Some((
                    format!("SELECT 1 FROM {table} WHERE {predicate} FOR UPDATE"),
                    keys.clone(),
                ));
                params.extend(keys);
                let sql = if e.kind == EventKind::Delete {
                    format!("DELETE FROM {table} WHERE {predicate}")
                } else {
                    format!(
                        "UPDATE {table} SET {} WHERE {predicate}",
                        columns
                            .iter()
                            .map(|c| format!("{c}=?"))
                            .collect::<Vec<_>>()
                            .join(",")
                    )
                };
                (sql, check)
            }
            _ => return Err(ApplyError::Invalid("unsupported CDC event")),
        };
        Ok(Some(Statement {
            sql,
            params,
            match_sql,
            table,
        }))
    }
}
struct Statement {
    sql: String,
    params: Vec<Param>,
    match_sql: Option<(String, Vec<Param>)>,
    table: String,
}
fn quote(s: &str) -> Result<String> {
    if s.is_empty() || s.contains('\0') {
        return Err(ApplyError::Invalid("identifier"));
    }
    Ok(format!("`{}`", s.replace('`', "``")))
}
fn position_hash(txn: &AssembledTxn) -> [u8; 32] {
    let mut h = Sha256::new();
    let d = txn.commit_position.order_domain().as_bytes();
    h.update((d.len() as u64).to_be_bytes());
    h.update(d);
    h.update(txn.commit_position.bytes());
    h.finalize().into()
}
type Receipt = (Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>);
fn receipt_decision(txn: &AssembledTxn, r: Option<&Receipt>) -> Result<bool> {
    let Some((domain, position, id, digest)) = r else {
        return Ok(false);
    };
    if domain != txn.commit_position.order_domain().as_bytes()
        || position != txn.commit_position.bytes()
        || id != &txn.txn.0
    {
        return Err(ApplyError::IdentityConflict);
    }
    if digest != &txn.digest {
        return Err(ApplyError::DigestConflict);
    }
    Ok(true)
}
fn parameter(p: &MysqlPgColumn, opaque: &OpaqueValue) -> Result<Param> {
    if opaque.type_id != p.type_id {
        return Err(ApplyError::Invalid("canonical type identity"));
    }
    let value = match &opaque.bytes {
        None => Value::Null,
        Some(b) => Value::from_canonical_bytes(b)?,
    };
    if opaque.bytes.is_some() && value == Value::Null {
        return Err(ApplyError::Invalid("noncanonical NULL"));
    }
    let v = mysql::inverse(p, &value)?;
    Ok(match v {
        MysqlValue::Null => Param::NULL,
        MysqlValue::Signed(n) => Param::Int(n),
        MysqlValue::Unsigned(n) => Param::UInt(n),
        MysqlValue::Float(n) => Param::Float(n),
        MysqlValue::Double(n) => Param::Double(n),
        MysqlValue::DateTime(y, m, d, h, i, s, u) => Param::Date(y, m, d, h, i, s, u),
        MysqlValue::Time {
            negative,
            days,
            hours,
            minutes,
            seconds,
            micros,
        } => Param::Time(negative, days, hours, minutes, seconds, micros),
        MysqlValue::Bit(b) => Param::UInt(b.iter().fold(0u64, |n, b| (n << 8) | u64::from(*b))),
        // The opaque codec supplies calendar UTC TIMESTAMP, exact DECIMAL,
        // enum/set labels, UTF-8, binary and JSON bytes without SQL interpolation.
        _ => Param::Bytes(
            super::value::to_opaque(&p.source, &v)?
                .bytes
                .ok_or(ApplyError::Invalid("parameter bytes"))?,
        ),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{Position, TxnId};
    fn source() -> SourceIdentity {
        SourceIdentity {
            engine: "pg".into(),
            system_id: "sys".into(),
            lineage: "lineage".into(),
            db_name: "db".into(),
            stream: "stream".into(),
        }
    }
    fn txn() -> AssembledTxn {
        AssembledTxn::new(
            source(),
            TxnId(vec![1]),
            Position::new("pg", vec![2]),
            None,
            vec![],
        )
        .unwrap()
    }
    #[test]
    fn receipt_decisions_verify_every_identity_component() {
        let t = txn();
        assert!(!receipt_decision(&t, None).unwrap());
        let r = (b"pg".to_vec(), vec![2], vec![1], t.digest.to_vec());
        assert!(receipt_decision(&t, Some(&r)).unwrap());
        for i in 0..4 {
            let mut bad = r.clone();
            match i {
                0 => bad.0.push(0),
                1 => bad.1.push(0),
                2 => bad.2.push(0),
                _ => bad.3[0] ^= 1,
            }
            assert!(
                matches!(
                    receipt_decision(&t, Some(&bad)),
                    Err(ApplyError::IdentityConflict)
                ) && i < 3
                    || matches!(
                        receipt_decision(&t, Some(&bad)),
                        Err(ApplyError::DigestConflict)
                    ) && i == 3
            );
        }
    }
    #[test]
    fn position_hash_includes_domain_and_unambiguous_boundary() {
        let a = txn();
        let mut b = a.clone();
        b.commit_position = Position::new("p", b"g\x02".to_vec());
        assert_ne!(position_hash(&a), position_hash(&b));
    }
    #[test]
    fn quote_identifiers_without_interpolating_values() {
        assert_eq!(quote("a`b").unwrap(), "`a``b`");
        assert!(quote("").is_err());
        assert!(quote("a\0b").is_err());
    }
    #[test]
    fn typed_integer_and_null_parameters() {
        let c = super::super::schema::ColumnDescriptor {
            name: "id".into(),
            ordinal: 1,
            data_type: "bigint".into(),
            column_type: "bigint unsigned".into(),
            character_set: None,
            collation: None,
            nullable: true,
            generated: false,
            column_default: None,
            default_generated: false,
            generation_expression: None,
        };
        let p = mysql::plan_column(&c).unwrap();
        let value = mysql::forward(&p, &MysqlValue::Unsigned(u64::MAX)).unwrap();
        let opaque = OpaqueValue {
            type_id: p.type_id.clone(),
            bytes: Some(value.canonical_bytes().unwrap()),
        };
        assert_eq!(parameter(&p, &opaque).unwrap(), Param::UInt(u64::MAX));
        assert_eq!(
            parameter(
                &p,
                &OpaqueValue {
                    type_id: p.type_id.clone(),
                    bytes: None
                }
            )
            .unwrap(),
            Param::NULL
        );
        assert!(
            parameter(
                &p,
                &OpaqueValue {
                    type_id: p.type_id.clone(),
                    bytes: Some(Value::Null.canonical_bytes().unwrap())
                }
            )
            .is_err()
        );
        assert!(
            parameter(
                &p,
                &OpaqueValue {
                    type_id: "unknown".into(),
                    bytes: None
                }
            )
            .is_err()
        );
    }
    #[test]
    fn statement_sql_and_parameter_order() {
        let c = super::super::schema::ColumnDescriptor {
            name: "id".into(),
            ordinal: 1,
            data_type: "bigint".into(),
            column_type: "bigint".into(),
            character_set: None,
            collation: None,
            nullable: false,
            generated: false,
            column_default: None,
            default_generated: false,
            generation_expression: None,
        };
        let p = mysql::plan_column(&c).unwrap();
        let config = MysqlConfig {
            host: String::new(),
            port: 3306,
            ca: Default::default(),
            user: String::new(),
            password: String::new(),
            profile: "mysql84".into(),
            gtid: "ON".into(),
            container_id: String::new(),
            max_decompressed_bytes: 1024,
        };
        let a = MysqlApplier::new(
            config,
            source(),
            BTreeMap::from([(
                "table".into(),
                TableDescriptor {
                    schema: "s`q".into(),
                    table: "target".into(),
                    columns: vec![c],
                    primary_key: vec!["id".into()],
                },
            )]),
        )
        .unwrap();
        let opaque = |n| OpaqueValue {
            type_id: p.type_id.clone(),
            bytes: Some(Value::Int8(n).canonical_bytes().unwrap()),
        };
        let mut e = Event {
            source: source(),
            txn: TxnId(vec![1]),
            ordinal_in_txn: 0,
            position: Position::new("pg", vec![2]),
            schema_version: 1,
            kind: EventKind::Insert,
            table: Some("table".into()),
            key: BTreeMap::from([("id".into(), opaque(1))]),
            before: None,
            after: Some(BTreeMap::from([("id".into(), opaque(2))])),
            unchanged_cols: Default::default(),
        };
        let s = a.statement(&e).unwrap().unwrap();
        assert_eq!(s.sql, "INSERT INTO `s``q`.`target` (`id`) VALUES (?)");
        assert_eq!(s.params, vec![Param::Int(2)]);
        e.kind = EventKind::Update;
        let s = a.statement(&e).unwrap().unwrap();
        assert_eq!(s.sql, "UPDATE `s``q`.`target` SET `id`=? WHERE `id`=?");
        assert_eq!(s.params, vec![Param::Int(2), Param::Int(1)]);
        e.kind = EventKind::Delete;
        let s = a.statement(&e).unwrap().unwrap();
        assert_eq!(s.sql, "DELETE FROM `s``q`.`target` WHERE `id`=?");
        assert_eq!(s.params, vec![Param::Int(1)]);
        e.key.clear();
        assert!(a.statement(&e).is_err());
        e.kind = EventKind::DdlBoundary;
        assert!(a.statement(&e).is_err());
    }
}

/// A read-only check, also usable to qualify an explicitly configured session.
pub async fn require_binlogging(c: &mut Conn) -> Result<()> {
    let enabled: Option<u8> = c.query_first("SELECT @@session.sql_log_bin").await?;
    if enabled != Some(1) {
        return Err(ApplyError::BinlogDisabled);
    }
    Ok(())
}
impl crate::engine::Applier for MysqlApplier {
    async fn step(&mut self, runtime: &crate::engine::Runtime) -> crate::engine::Result<bool> {
        self.buffer_step(runtime).await.map_err(|e| match e {
            ApplyError::Engine(e) => e,
            other => crate::coordinator::Error::RecoveryRequired(other.to_string()),
        })
    }
    async fn controlled_step(
        &mut self,
        runtime: &crate::engine::Runtime,
        stop: tokio::sync::watch::Receiver<bool>,
    ) -> crate::engine::Result<bool> {
        admitted_step(stop, crate::engine::Applier::step(self, runtime)).await
    }
}

async fn complete_boundary(
    save: impl std::future::Future<Output = Result<()>>,
    collect: impl std::future::Future<Output = Result<()>>,
) -> Result<()> {
    save.await?;
    collect.await
}

async fn admitted_step(
    stop: tokio::sync::watch::Receiver<bool>,
    step: impl std::future::Future<Output = crate::engine::Result<bool>>,
) -> crate::engine::Result<bool> {
    if *stop.borrow() {
        return Ok(false);
    }
    step.await
}
#[cfg(test)]
mod adapter_tests {
    use super::*;
    #[tokio::test]
    async fn checkpoint_precedes_collection_and_failure_prevents_collection() {
        let trace = std::sync::Mutex::new(Vec::new());
        complete_boundary(
            async {
                trace.lock().unwrap().push("durable");
                Ok(())
            },
            async {
                trace.lock().unwrap().push("gc");
                Ok(())
            },
        )
        .await
        .unwrap();
        assert_eq!(*trace.lock().unwrap(), ["durable", "gc"]);
        let result = complete_boundary(async { Err(ApplyError::Blocked) }, async {
            panic!("GC after failed checkpoint")
        })
        .await;
        assert!(matches!(result, Err(ApplyError::Blocked)));
    }
    #[tokio::test]
    async fn shutdown_mid_step_completes() {
        let (stop, stopped) = tokio::sync::watch::channel(false);
        let (entered, wait_entered) = tokio::sync::oneshot::channel();
        let (release, wait_release) = tokio::sync::oneshot::channel();
        let step = admitted_step(stopped, async {
            entered.send(()).unwrap();
            wait_release.await.unwrap();
            Ok(true)
        });
        let shutdown = async {
            wait_entered.await.unwrap();
            stop.send(true).unwrap();
            release.send(()).unwrap();
        };
        let (result, ()) = tokio::join!(step, shutdown);
        assert!(result.unwrap());
    }
}
