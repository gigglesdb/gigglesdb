use crate::*;
#[cfg(not(feature = "legacy-task"))]
use crate::accounting::TaskAccounting;
#[cfg(all(test, not(feature = "legacy-task")))]
use crate::accounting::TaskResources;
mod catalog_inspection;
mod cursor;
mod drop_table;
mod write_fence_admin;
pub use write_fence_admin::WriteFenceStatus;
pub(crate) mod engine;
mod once_session;
#[cfg(feature = "engine-rocksdb")]
mod directory_identity;
mod engine_slot;
pub(crate) use engine_slot::{EngineLease, Holder};
pub(crate) use engine_slot::{GenerationTag, PreparedGeneration};
use engine_slot::{EngineSlot, StorageStatus};
/// Task preview `schema_fp` input (GD-0061 R3-701): the catalog encodings of
/// `table` and each of `constraints`, in the order given.
pub(crate) fn task_schema_bytes(
    table: &crate::relational::TableDef,
    constraints: &[crate::relational::catalog_ext::Constraint],
) -> Result<Vec<Vec<u8>>, CoreError> {
    let entry = engine::catalog::TableEntry {
        def: table.clone(),
        heap: false,
        primary_index_birth: None,
        columns_ext: std::collections::BTreeMap::new(),
    };
    let mut out = vec![engine::catalog_codec::encode_table(&entry, &entry.columns_ext)?];
    for c in constraints {
        out.push(engine::catalog_codec::encode_constraint(c, &table.name)?);
    }
    Ok(out)
}
/// The row payload encoding `codec::encode_row` writes, without the record header.
pub(crate) fn task_row_payload(values: &[crate::relational::Value]) -> Result<Vec<u8>, CoreError> {
    encode(&values)
}
/// The inverse of [`task_row_payload`] (GD-0061 R3-703: stored preview after-values).
pub(crate) fn task_row_values(payload: &[u8]) -> Result<Vec<crate::relational::Value>, CoreError> {
    decode(payload)
}
pub(crate) use engine::catalog_label_codec::UNTRUSTED_TEXT as UNTRUSTED_TEXT_LABEL;
pub(crate) use engine::{catalog::CatalogParts, catalog_codec::sql_views::{SqlViewDef, SqlViewColumn, SqlViewDependency}};
mod feature_envelope;
mod format_policy;
#[cfg(test)]
mod retired_application_tests;
mod legacy_open;
pub(crate) mod native_once;
pub mod replica;
#[cfg(feature = "engine-rocksdb")]
pub mod replicated;
pub mod range_api;
#[cfg(feature = "engine-rocksdb")]
mod range;
#[cfg(feature = "engine-rocksdb")]
mod cluster;
#[cfg(feature = "engine-rocksdb")]
pub mod cluster_node;
pub mod roles;
#[cfg(all(test, feature = "engine-rocksdb"))]
mod roles_task_only_tests;
mod secondary_index;
mod secondary_index_catalog;
mod secondary_index_public;
mod column_labels_public;
mod sequence_counters;
pub(crate) mod sequences;
mod upgrade;
pub(crate) mod v2;
// GD-0061 R3-612: the task ledger reads and stages Kernel records.
pub(crate) use engine::{kv::Keyspace, meta::PROTECTED_RECORDS, txn::{Isolation, Transaction}};
pub(crate) use v2::V2Db;
pub use upgrade::{UpgradePoint, UpgradeReport};
mod copy_input;
#[cfg(feature = "engine-rocksdb")]
mod v2_export;
mod v2_sql;
#[cfg(all(test, feature = "engine-rocksdb"))]
mod v2_fk_row_lock_tests;
use copy_input::CopyInput;
pub use cursor::Cursor;
pub use secondary_index_public::IndexInfo;
mod unique_batch_bridge;
use bincode::Options;
use redb::{Database, Durability, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::{self, OpenOptions},
    io::{Read, Write},
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};
const RECORDS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("giggles_v1");
const FORMAT: u32 = 3;
#[cfg(test)]
const NATIVE_FORMAT: u32 = 4;
const MAX_BYTES: u64 = 16 * 1024 * 1024;
const MAGIC: &[u8; 8] = b"GDEXP001";
/// PostgreSQL table lock modes, held until the SQL transaction ends.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SqlTableLockMode {
    AccessShare,
    RowShare,
    RowExclusive,
    ShareUpdateExclusive,
    Share,
    ShareRowExclusive,
    ExclusiveTable,
    AccessExclusive,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CoreError {
    Storage(String),
    Corrupt(String),
    Unsupported,
    EngineUnsupported {
        engine: &'static str,
        feature: &'static str,
    },
    Exists,
    Busy,
    Deadlock,
    LockNotAvailable,
    Fenced,
    WriteFenced {
        epoch: u64,
    },
    Cancelled,
    Limit,
    Policy(Error),
    /// Execution-time refusal to use a sequence.
    SequencePrivilege,
    Sql(relational::SqlError),
    /// Schema DDL error outside the frozen, persisted scalar-error enum.
    DuplicateSchema,
    /// Execution-only privilege inquiry error; never persisted.
    PrivilegeInquiry(&'static str),
    /// A gateway could not encode the reply while an autocommit write was staged.
    SqlReplyEncoding(&'static str),
    /// Task-session error (GD-0061). Never stored in `SqlError`, which is persisted.
    Task(crate::task::TaskError),
    /// A query temp (spill) file failed: 53100 when the device is full, else 58030.
    TempFile { disk_full: bool, detail: String },
}
impl std::fmt::Display for CoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Self::WriteFenced { epoch } = self {
            return write!(f, "database is write-fenced (epoch {epoch})");
        }
        write!(f, "{self:?}")
    }
}
impl std::error::Error for CoreError {}

/// Checks an untasked autocommit write's result set while its effects are still
/// private. A spill is borrowed for the check and rewound for delivery.
pub type SqlReplyCheck<'a> = dyn FnMut(
    crate::task::Released<&relational::SqlResult>,
    Option<&mut relational::spill::SpilledRows>,
) -> Result<(), CoreError> + 'a;

fn check_autocommit_reply(
    check: Option<&mut SqlReplyCheck<'_>>,
    result: &relational::SqlResult,
) -> Result<(), CoreError> {
    if let Some(check) = check
        && (!result.columns.is_empty() || !result.rows.is_empty())
    {
        // The statement kernel passes the check only outside the task release
        // path. Releasing this plain result is just a wrapper around the borrow.
        relational::spill::check_pending(|spill| check(crate::task::Released::synthetic(result), spill))?;
    }
    Ok(())
}
fn io(e: impl std::fmt::Display) -> CoreError {
    CoreError::Storage(e.to_string())
}
// Keep unwind containment inside the legacy redb boundary, below callers' locks.
// Decode/validation and user callbacks must retain their ordinary panic behavior.
fn legacy_redb<T>(
    operation: &'static str,
    f: impl FnOnce() -> Result<T, CoreError>,
) -> Result<T, CoreError> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(f))
        .map_err(|_| CoreError::Corrupt(format!("legacy redb panicked while {operation}")))?
}
fn codec() -> impl Options {
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_big_endian()
        .with_limit(MAX_BYTES)
        .reject_trailing_bytes()
}
fn encode<T: Serialize>(v: &T) -> Result<Vec<u8>, CoreError> {
    codec()
        .serialize(v)
        .map_err(|e| CoreError::Corrupt(e.to_string()))
}
fn decode<T: DeserializeOwned>(b: &[u8]) -> Result<T, CoreError> {
    codec()
        .deserialize(b)
        .map_err(|e| CoreError::Corrupt(e.to_string()))
}
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub admitted: usize,
    pub selected_keys: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            admitted: 32,
            selected_keys: 1024,
        }
    }
}
/// Explicit process-crash test seams. No environment variable controls production.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CrashPoint {
    None,
    BeforeCommit,
    AfterCommit,
    BeforeAck,
    ErrorBeforeCommit,
    ErrorAfterCommit,
    /// Skip intermediate read admissions; fail after the business/release decision.
    ErrorAfterDecisionCommit,
}
/// Trusted administrative recovery test seams; never controlled by environment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecoveryPoint {
    None,
    ExportWrite,
    ExportSync,
    ExportVerify,
    ExportPublish,
    CrashExportWrite,
    CrashExportSync,
    CrashExportVerify,
    /// Crash after the verified export has been published.
    CrashExportPublish,
    InstallCreated,
    InstallStaged,
    InstallCommitted,
    CrashInstallCreated,
    CrashInstallStaged,
    CrashInstallCommitted,
}
fn recovery(
    point: RecoveryPoint,
    at: RecoveryPoint,
    crash: RecoveryPoint,
) -> Result<(), CoreError> {
    if crash != RecoveryPoint::None && point == crash {
        crash_now();
    }
    if point == at {
        return Err(CoreError::Storage(format!("injected {at:?}")));
    }
    Ok(())
}
#[derive(Clone, Serialize, serde::Deserialize)]
pub(crate) struct State {
    #[serde(skip)]
    pub(crate) seq: BTreeMap<u64, sequences::SeqCounter>,
    // Legacy SQL stages these with its catalog, including across savepoints.
    #[serde(skip)]
    sequence_owner_updates: BTreeMap<u64, Option<String>>,
    pub accounting: BTreeMap<TaskId, TaskAccounting>,
    pub native_outcomes: Option<native_once::NativeOutcomes>,
    pub sql: relational::Relational,
    pub view: Snapshot,
    pub last_time: u64,
    pub next_approval: u64,
    pub origin: Option<u128>,
}

/// Borrowed metadata facade. The catalog image's version and a metadata read's
/// public epoch are different: data commits advance the latter without changing
/// the former. Keep that per-read stamp outside the shared immutable image.
pub(super) struct MetadataState<'a> {
    sql: MetadataSql<'a>,
    view: MetadataDomain,
    catalog: Option<&'a engine::catalog::CatalogSnapshot>,
}
struct MetadataDomain {
    domain: u128,
}
struct MetadataSql<'a> {
    image: &'a relational::Relational,
    epoch: u64,
    comments: Option<&'a BTreeMap<(u64, u64), String>>,
    views: Option<&'a BTreeMap<u64, SqlViewDef>>,
}
impl std::ops::Deref for MetadataSql<'_> {
    type Target = relational::Relational;
    fn deref(&self) -> &Self::Target {
        self.image
    }
}
impl MetadataSql<'_> {
    fn schema_metadata(&self) -> relational::SchemaMetadata {
        let mut metadata = self.image.schema_metadata();
        metadata.epoch = self.epoch;
        for (&id, view) in self.views.into_iter().flatten() {
            if let Some((name, _)) = metadata.schemas.iter().find(|(_, (id, _))| *id == view.schema) {
                metadata.tables.insert(id, name.clone());
            }
        }
        metadata
    }
    fn user_type_catalog(&self) -> relational::UserTypeCatalog {
        let mut metadata = self.image.user_type_catalog();
        metadata.epoch = self.epoch;
        metadata
    }
    // Both shadow the image's methods so session reads keep the V2 epoch, comments and
    // views; without the 0-arg form, Deref would reach the raw image.
    fn catalog_presentation(&self) -> Result<relational::CatalogPresentation, relational::SqlError> {
        self.catalog_presentation_in_path(&["public".into()])
    }
    fn catalog_presentation_in_path(
        &self,
        path: &[String],
    ) -> Result<relational::CatalogPresentation, relational::SqlError> {
        let mut metadata = self.image.catalog_presentation_in_path(path)?;
        metadata.epoch = self.epoch;
        metadata.comments = self.comments.cloned().unwrap_or_default();
        metadata.views = self.views.into_iter().flatten().map(|(&id, v)| (id, relational::CatalogView {
            table: view_table(id, v),
            definition: format!("{};", v.select),
            type_oids: v.columns.iter().map(|c| c.pg_type_oid).collect(),
            typmods: v.columns.iter().map(|c| c.typmod).collect(),
        })).collect();
        Ok(metadata)
    }
}
// A view's relation descriptor, as SQL presents it; it has no primary key.
fn view_table(id: u64, v: &SqlViewDef) -> relational::TableDef {
    relational::TableDef { id, name: v.name.clone(), primary_key: vec![],
        columns: v.columns.iter().map(|c| relational::ColumnDef { id: c.id, name: c.name.clone(),
            data_type: c.storage_type.clone(), not_null: false }).collect() }
}
impl<'a> MetadataState<'a> {
    fn new(image: &'a relational::Relational, epoch: u64, domain: u128) -> Self {
        Self {
            sql: MetadataSql { image, epoch, comments: None, views: None },
            view: MetadataDomain { domain },
            catalog: None,
        }
    }
    fn borrow(state: &'a State) -> Self {
        Self::new(&state.sql, state.sql.epoch, state.view.domain)
    }
}
fn relation_names(state: &State) -> BTreeMap<(bool, u64), String> {
    state
        .sql
        .tables
        .values()
        .map(|table| ((false, table.id), state.sql.table_key(table)))
        .chain(
            state
                .sql
                .catalog_ext()
                .sequences
                .iter()
                .map(|(&id, sequence)| ((true, id), sequence.name.clone())),
        )
        .collect()
}
#[derive(Serialize, serde::Deserialize)]
struct Metadata {
    format: u32,
    domain: u128,
    schema: u64,
    last_time: u64,
    next_approval: u64,
    origin: Option<u128>,
}
pub(crate) fn empty(domain: u128) -> State {
    State {
        seq: BTreeMap::new(),
        sequence_owner_updates: BTreeMap::new(),
        accounting: BTreeMap::new(),
        native_outcomes: None,
        sql: Default::default(),
        view: Snapshot {
            domain,
            schema: 0,
            rows: BTreeMap::new(),
            tasks: BTreeMap::new(),
            operations: BTreeMap::new(),
            approvals: BTreeMap::new(),
        },
        last_time: 0,
        next_approval: 0,
        origin: None,
    }
}
fn key(ns: u8, nums: &[u64]) -> Vec<u8> {
    let mut k = vec![ns];
    for n in nums {
        k.extend(n.to_be_bytes());
    }
    k
}
fn records(s: &State) -> Result<BTreeMap<Vec<u8>, Vec<u8>>, CoreError> {
    s.sql.validate_unique_indexes().map_err(secondary_index::sql_error)?;
    let format = state_format(s)?;
    let mut out = BTreeMap::new();
    let bits = feature_envelope::feature_bits_present(s)?;
    if format == 53 {
        out.insert(vec![10], bits.to_be_bytes().to_vec());
    }
    if let Some(ledger) = &s.native_outcomes {
        out.insert(vec![7], ledger.to_bytes()?);
    }
    sequence_counters::write(s, &mut out)?;
    for (task, account) in &s.accounting {
        out.insert(key(6, &[task.0]), encode(account)?);
    }
    if s.sql.has_secondary_indexes_engine() {
        let (base, payload) = secondary_index::persist(&s.sql)?;
        out.insert(vec![5], base);
        out.insert(vec![8], payload);
    } else {
        out.insert(vec![5], encode(&s.sql)?);
    }
    out.insert(
        vec![0],
        encode(&Metadata {
            format,
            domain: s.view.domain,
            schema: s.view.schema,
            last_time: s.last_time,
            next_approval: s.next_approval,
            origin: s.origin,
        })?,
    );
    for (k, v) in &s.view.rows {
        out.insert(key(1, &[k.tenant.0, k.row.0]), encode(v)?);
    }
    for (k, v) in &s.view.tasks {
        out.insert(key(2, &[k.0]), encode(v)?);
    }
    for ((t, o), v) in &s.view.operations {
        out.insert(key(3, &[t.0, o.0]), encode(v)?);
    }
    for (k, v) in &s.view.approvals {
        out.insert(key(4, &[k.0]), encode(v)?);
    }
    envelope_bound(&out)?;
    Ok(out)
}
fn state_format(s: &State) -> Result<u32, CoreError> {
    let bits = feature_envelope::feature_bits_present(s)?;
    let legacy = format_policy::bits_to_legacy_version(bits & 63).ok_or(CoreError::Unsupported)?;
    Ok(if bits & feature_envelope::NEW_BITS != 0 {
        53
    } else {
        legacy
    })
}
fn contains_temporal(s: &State) -> Result<bool, CoreError> {
    Ok(s.sql.contains_temporal()
        || match &s.native_outcomes {
            Some(n) => n.contains_temporal()?,
            None => false,
        })
}
fn envelope_family(
    version: u32,
) -> Result<(format_policy::Features, bool, bool, bool, bool), CoreError> {
    format_policy::legacy_version_to_bits(version).ok_or(CoreError::Unsupported)?;
    let (base, heap) = format_policy::heap_base(version).ok_or(CoreError::Unsupported)?;
    let (base, temporal) = format_policy::temporal_base(base).ok_or(CoreError::Unsupported)?;
    let (features, indexed, applications) = secondary_index::family(base)?;
    Ok((features, indexed, applications, temporal, heap))
}
fn envelope_bound(values: &BTreeMap<Vec<u8>, Vec<u8>>) -> Result<(), CoreError> {
    // Fixed-int map encoding: 8-byte count and two 8-byte lengths per entry.
    // Reserve the full export payload, not merely raw key/value bytes.
    let size = values
        .iter()
        .try_fold(8u64, |n, (k, v)| {
            n.checked_add(16)?
                .checked_add(k.len() as u64)?
                .checked_add(v.len() as u64)
        })
        .ok_or(CoreError::Limit)?;
    if size > MAX_BYTES {
        return Err(CoreError::Limit);
    }
    Ok(())
}
fn from_records(records: BTreeMap<Vec<u8>, Vec<u8>>) -> Result<State, CoreError> {
    from_records_with_cancel(records, &mut || crate::cancellation::check().is_err())
}
// Keep canonical validation in one body. Trusted pure callers supply their own
// cancellation policy; ordinary storage callers retain the ambient wrapper.
fn from_records_with_cancel(
    records: BTreeMap<Vec<u8>, Vec<u8>>,
    cancel: &mut impl FnMut() -> bool,
) -> Result<State, CoreError> {
    from_records_mode(records, cancel, true)
}
fn from_records_mode(
    records: BTreeMap<Vec<u8>, Vec<u8>>,
    cancel: &mut impl FnMut() -> bool,
    import: bool,
) -> Result<State, CoreError> {
    envelope_bound(&records)?;
    let m: Metadata = decode(
        records
            .get(&vec![0])
            .ok_or_else(|| CoreError::Corrupt("missing metadata".into()))?,
    )?;
    let bits = feature_envelope::decode_bits(&records, m.format, import)?;
    let legacy = match bits {
        Some(bits) => {
            format_policy::bits_to_legacy_version(bits & 63).ok_or(CoreError::Unsupported)?
        }
        None => m.format,
    };
    let (features, indexed, applications, temporal, heap) = envelope_family(legacy)?;
    if applications {
        return Err(CoreError::EngineUnsupported {
            engine: "legacy records",
            feature: RETIRED_APPLICATION_LEDGER,
        });
    }
    let (base_version, _) = format_policy::heap_base(legacy).ok_or(CoreError::Unsupported)?;
    secondary_index::gate(&records, indexed)?;
    let mut s = empty(m.domain);
    if records.contains_key(&vec![9]) {
        return Err(CoreError::Corrupt(
            "application format/namespace mismatch".into(),
        ));
    }
    match records.get(&vec![7]) {
        Some(v) if features.native_outcomes => {
            s.native_outcomes = Some(native_once::NativeOutcomes::from_bytes(v)?);
        }
        None if !features.native_outcomes => {}
        _ => {
            return Err(CoreError::Corrupt(
                "native format/namespace mismatch".into(),
            ));
        }
    }
    match records.get(&vec![5]) {
        Some(v) if base_version >= 2 => s.sql = feature_envelope::decode_relational(v)?,
        None if base_version == 1 => {}
        _ => {
            return Err(CoreError::Corrupt(
                "relational format/namespace mismatch".into(),
            ));
        }
    }
    if s.sql.contains_heap() != heap {
        return Err(CoreError::Corrupt("heap format mismatch".into()));
    }
    if !features.typed_scalars
        && (s.sql.contains_typed()
            || s.native_outcomes
                .as_ref()
                .is_some_and(|n| n.contains_typed().unwrap_or(true)))
    {
        return Err(CoreError::Corrupt("typed format mismatch".into()));
    }
    if contains_temporal(&s)? != temporal {
        return Err(CoreError::Corrupt("temporal format mismatch".into()));
    }
    s.view.schema = m.schema;
    s.last_time = m.last_time;
    s.next_approval = m.next_approval;
    s.origin = m.origin;
    let mut index_payload = None;
    for (k, v) in records {
        let num = |at: usize| -> Result<u64, CoreError> {
            let b = k
                .get(at..at + 8)
                .ok_or_else(|| CoreError::Corrupt("short key".into()))?;
            Ok(u64::from_be_bytes(b.try_into().expect("length")))
        };
        match k.first() {
            Some(6) if k.len() == 9 && base_version >= FORMAT => {
                s.accounting.insert(TaskId(num(1)?), decode(&v)?);
            }
            Some(10) if k.len() == 1 && m.format == 53 => {}
            Some(11) if bits.is_some_and(|b| b & 256 != 0) => {
                sequence_counters::read(&mut s, &k, &v)?;
            }
            Some(0) if k.len() == 1 => {}
            Some(5) if k.len() == 1 => {}
            Some(7) if k.len() == 1 && features.native_outcomes => {}
            Some(8) if k.len() == 1 && indexed => index_payload = Some(v),
            Some(1) if k.len() == 17 => {
                s.view.rows.insert(
                    Key {
                        tenant: TenantId(num(1)?),
                        row: RowId(num(9)?),
                    },
                    decode(&v)?,
                );
            }
            Some(2) if k.len() == 9 => {
                s.view.tasks.insert(TaskId(num(1)?), decode(&v)?);
            }
            Some(3) if k.len() == 17 => {
                s.view
                    .operations
                    .insert((TaskId(num(1)?), OperationId(num(9)?)), decode(&v)?);
            }
            Some(4) if k.len() == 9 => {
                s.view.approvals.insert(ApprovalId(num(1)?), decode(&v)?);
            }
            _ => {
                return Err(CoreError::Corrupt(
                    "unknown record namespace or key length".into(),
                ));
            }
        }
    }
    validate(&s)?;
    if let Some(payload) = index_payload {
        let pending = secondary_index::pending(&payload, cancel)?;
        s.sql
            .secondary_restore_engine(pending, &mut *cancel)
            .map_err(|error| secondary_index::sql_error_with_cancel(error, cancel))?;
    }
    s.sql.validate_unique_indexes().map_err(|e| CoreError::Corrupt(format!("relational invariant: {e:?}")))?;
    if let Some(bits) = bits
        && feature_envelope::feature_bits_present(&s)? != bits
    {
        return Err(CoreError::Corrupt("feature content mismatch".into()));
    }
    if bits.is_none()
        && feature_envelope::feature_bits_present(&s)? & feature_envelope::NEW_BITS != 0
    {
        return Err(CoreError::Corrupt("feature content mismatch".into()));
    }
    if cancel() {
        return Err(CoreError::Cancelled);
    }
    Ok(s)
}
fn validate(s: &State) -> Result<(), CoreError> {
    state_format(s)?;
    sequence_counters::validate(s)?;
    if let Some(ledger) = &s.native_outcomes {
        ledger.validate()?;
    }
    for (task, account) in &s.accounting {
        if !s.view.tasks.contains_key(task) || !account.used.fits(account.limits) {
            return Err(CoreError::Corrupt("task accounting invariant".into()));
        }
    }
    s.sql
        .validate()
        .map_err(|e| CoreError::Corrupt(format!("relational invariant: {e:?}")))?;
    let bad = || CoreError::Corrupt("authority invariant".into());
    for (id, t) in &s.view.tasks {
        if *id != t.grant.task
            || !policy::grant_valid(&t.grant)
            || t.transitions > t.grant.policy.transition_limit
            || t.touched.len() as u64 > t.grant.policy.distinct_limit
            || t.touched.iter().any(|k| k.tenant != t.grant.policy.tenant)
        {
            return Err(bad());
        }
    }
    for (id, a) in &s.view.approvals {
        if *id != a.id || id.0 >= s.next_approval || !s.view.tasks.contains_key(&a.preview.task) {
            return Err(bad());
        }
    }
    for ((t, o), b) in &s.view.operations {
        if !s.view.tasks.contains_key(t) {
            return Err(bad());
        }
        if let Ok(r) = &b.decision
            && (r.task != *t
                || r.operation != *o
                || r.payload != b.payload
                || r.decided_at > s.last_time
                || r.charged_transitions != r.effects.len() as u64)
        {
            return Err(bad());
        }
    }
    Ok(())
}
/// Trusted deterministic synchronization seam, never set by network inputs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DecisionPoint {
    WaitingLock,
    PreparedBindAuthorized,
    BeforeDecision,
    Decided,
    Committed,
}
type DecisionHook = Arc<dyn Fn(DecisionPoint) + Send + Sync>;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Engine {
    Legacy,
    V2,
}
/// Prepared-plan cache policy, separate from the generation used by descriptions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PreparedCatalogState {
    Version(Option<u64>),
    PrivateDdl,
    UnknownBackend,
}
/// The isolation level of a SQL transaction on the v2 engine (GD-0075 R5-76).
/// PostgreSQL's READ UNCOMMITTED runs as READ COMMITTED. The legacy engine
/// runs every SQL transaction serializable and ignores the level.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SqlIsolation {
    #[default]
    ReadCommitted,
    /// One snapshot per transaction; a concurrent update of a row it writes fails with 40001.
    RepeatableRead,
    /// REPEATABLE READ plus commit-time validation of every row and range it read (40001).
    Serializable,
}
impl SqlIsolation {
    pub(crate) fn engine(self) -> engine::txn::Isolation {
        match self {
            Self::ReadCommitted => engine::txn::Isolation::ReadCommitted,
            Self::RepeatableRead => engine::txn::Isolation::RepeatableRead,
            Self::Serializable => engine::txn::Isolation::Serializable,
        }
    }
}
#[cfg(feature = "legacy-task")]
const TASK_EXECUTORS: &str = "task executors";
#[cfg(feature = "legacy-task")]
const PROTECTED_OPERATIONS: &str = "protected operations";
const KERNEL_RECORDS: &str = "kernel records";
const SQL_SESSIONS: &str = "SQL sessions";
const LOGICAL_EXPORT: &str = "logical export";
const ROLES: &str = "roles";
#[allow(dead_code)]
const REPLICA: &str = "replica";
const LEGACY_STORAGE: &str = "legacy storage";
#[cfg(not(feature = "engine-rocksdb"))]
const ROCKSDB_NOT_COMPILED: &str = "engine-rocksdb not compiled in";

/// GD-0074 E142: the GD-0052 application reducer was removed; stores holding its
/// ledger are refused, never migrated or ignored.
pub(crate) const RETIRED_APPLICATION_LEDGER: &str = "GD-0052 application ledger (legacy format family \"application\", feature bit 3, record namespace 9) was removed; open this store with a GigglesDB build at or before commit 429cbd2c";
enum Backend {
    Legacy(Database),
    V2(EngineSlot),
}
struct Inner {
    backend: Backend,
    gate: crate::cancellation::WaitMutex<()>,
    admitted: AtomicUsize,
    sql_transactions: AtomicUsize,
    sql_operations: AtomicUsize,
    /// Statement slots of task sessions (GD-0061 R3-401).
    task_load: Arc<crate::task::limits::TaskLoad>,
    fenced: Arc<AtomicBool>,
    legacy_fence: engine::kv::FenceState,
    limits: Limits,
    crash: Mutex<CrashPoint>,
    /// The armed protected-commit crash seam (GD-0061 R3-619, D-I).
    protected_point: Mutex<Option<crate::task::commit::ProtectedPoint>>,
    /// The protected-commit pause seam (GD-0061 R3-902).
    protected_hook: Mutex<Option<crate::task::commit::ProtectedHook>>,
    decision_hook: Mutex<Option<DecisionHook>>,
    sequences: Mutex<sequences::Live>,
    native_once_owners: native_once::OnceOwners,
}
enum GuardedPersistError {
    Core(CoreError),
    ClockExpired,
}
impl From<CoreError> for GuardedPersistError {
    fn from(error: CoreError) -> Self {
        Self::Core(error)
    }
}
struct Ticket<'a>(Option<&'a AtomicUsize>);
impl Ticket<'_> {
    fn release(&mut self) {
        if let Some(counter) = self.0.take() {
            counter.fetch_sub(1, Ordering::AcqRel);
        }
    }
}
impl Drop for Ticket<'_> {
    fn drop(&mut self) {
        self.release();
    }
}
impl Inner {
    fn lease(&self, kind: Holder) -> Result<Option<EngineLease>, CoreError> {
        match &self.backend {
            Backend::Legacy(_) => Ok(None),
            Backend::V2(slot) => slot.fresh(kind).map(Some),
        }
    }

    fn fence_error(&self, site: engine::kv::FenceSite, error: &CoreError) {
        self.legacy_fence.record(engine::kv::FenceCause::for_core(site, error));
        self.fenced.store(true, Ordering::Release);
    }

    fn db(&self, feature: &'static str) -> Result<&Database, CoreError> {
        match &self.backend {
            Backend::Legacy(db) => Ok(db),
            Backend::V2(_) => Err(CoreError::EngineUnsupported {
                engine: "v2",
                feature,
            }),
        }
    }

    fn max_transactions(&self) -> usize {
        match &self.backend {
            Backend::Legacy(_) => relational::LEGACY_MAX_TRANSACTIONS,
            Backend::V2(_) => relational::MAX_TRANSACTIONS,
        }
    }

    /// Inspect metadata only. Native/task operations must keep using `db(feature)`.
    fn inspect_metadata<T>(
        &self,
        f: impl FnOnce(&MetadataState<'_>) -> Result<T, CoreError>,
    ) -> Result<T, CoreError> {
        match &self.backend {
            Backend::Legacy(_) => self.inspect(|state| f(&MetadataState::borrow(state))),
            Backend::V2(slot) => {
                let db = slot.fresh(Holder::Admin)?;
                let _ticket = self.enter()?;
                db.inspect_metadata(None, f)
            }
        }
    }

    fn decision_point(&self, point: DecisionPoint) {
        let hook = self
            .decision_hook
            .lock()
            .expect("decision hook lock")
            .clone();
        if let Some(hook) = hook {
            hook(point);
        }
    }
    fn enter_sql(&self) -> Result<Ticket<'_>, CoreError> {
        crate::cancellation::check()?;
        if self.fenced.load(Ordering::Acquire) {
            return Err(CoreError::Fenced);
        }
        self.sql_operations
            .try_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                n.checked_add(1).filter(|_| n < self.limits.admitted)
            })
            .map_err(|_| CoreError::Busy)?;
        Ok(Ticket(Some(&self.sql_operations)))
    }
    fn enter(&self) -> Result<Ticket<'_>, CoreError> {
        crate::cancellation::check()?;
        if self.fenced.load(Ordering::Acquire) {
            return Err(CoreError::Fenced);
        }
        self.admitted
            .try_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                n.checked_add(1).filter(|_| n < self.limits.admitted)
            })
            .map_err(|_| CoreError::Busy)?;
        Ok(Ticket(Some(&self.admitted)))
    }
    fn read(&self) -> Result<State, CoreError> {
        self.read_at(engine::kv::FenceSite::Read)
    }

    fn legacy_io(
        &self,
        site: engine::kv::FenceSite,
        error: impl Into<redb::Error> + std::fmt::Display,
    ) -> CoreError {
        let original = io(&error);
        if matches!(error.into(), redb::Error::Corrupted(_)) {
            use engine::kv::{FenceCause, FenceClass};
            self.legacy_fence
                .record(FenceCause::new(site, FenceClass::Data, &original));
        }
        original
    }

    fn read_at(&self, site: engine::kv::FenceSite) -> Result<State, CoreError> {
        let db = self.db(LEGACY_STORAGE)?;
        let storage = |error| self.legacy_io(site, error);
        let out = legacy_redb("reading records", || {
            let tx = db
                .begin_read()
                .map_err(|error| self.legacy_io(site, error))?;
            let t = tx
                .open_table(RECORDS)
                .map_err(|error| self.legacy_io(site, error))?;
            let mut out = BTreeMap::new();
            let mut total = 0u64;
            for item in t.iter().map_err(storage)? {
                let (k, v) = item.map_err(storage)?;
                total += k.value().len() as u64 + v.value().len() as u64;
                if total > MAX_BYTES {
                    return Err(CoreError::Limit);
                }
                out.insert(k.value().to_vec(), v.value().to_vec());
            }
            Ok(out)
        })?;
        let mut state =
            from_records_mode(out, &mut || crate::cancellation::check().is_err(), false)?;
        self.merge_live_sequences(&mut state)?;
        Ok(state)
    }
    fn inspect<T>(&self, f: impl FnOnce(&State) -> Result<T, CoreError>) -> Result<T, CoreError> {
        let _ticket = self.enter()?;
        let _guard = self.gate.lock_notified(|| {
            self.decision_point(DecisionPoint::WaitingLock)
        })?;
        if self.fenced.load(Ordering::Acquire) {
            return Err(CoreError::Fenced);
        }
        let s = self.read().inspect_err(|error| self.fence_error(engine::kv::FenceSite::Read, error))?;
        f(&s)
    }
    fn legacy_statement_snapshot(&self) -> Result<State, CoreError> {
        self.inspect(|committed| {
            let mut snapshot = committed.clone();
            snapshot.sequence_owner_updates.clear();
            Ok(snapshot)
        })
    }
    fn change<T>(
        &self,
        f: impl FnOnce(&mut State) -> Result<T, CoreError>,
    ) -> Result<T, CoreError> {
        self.change_inner(false, false, f)
    }
    #[cfg(any(test, feature = "legacy-task"))]
    fn change_decision<T>(
        &self,
        f: impl FnOnce(&mut State) -> Result<T, CoreError>,
    ) -> Result<T, CoreError> {
        self.change_inner(true, false, f)
    }
    fn change_inner<T>(
        &self,
        decision: bool,
        authority_managed: bool,
        f: impl FnOnce(&mut State) -> Result<T, CoreError>,
    ) -> Result<T, CoreError> {
        let _ticket = self.enter()?;
        let _guard = self.gate.lock_notified(|| {
            self.decision_point(DecisionPoint::WaitingLock)
        })?;
        if self.fenced.load(Ordering::Acquire) {
            return Err(CoreError::Fenced);
        }
        let mut s = self.read().inspect_err(|error| self.fence_error(engine::kv::FenceSite::Read, error))?;
        let previous_relations = relation_names(&s);
        // Logical errors can still advance the trusted monotonic decision clock.
        let result = f(&mut s);
        self.persist_checked_mode(
            &mut s,
            Some(&previous_relations),
            decision,
            true,
            &mut || true,
            authority_managed,
        )
        .map_err(|error| match error {
            GuardedPersistError::Core(error) => error,
            GuardedPersistError::ClockExpired => {
                unreachable!("ordinary persistence has no clock lease")
            }
        })?;
        result
    }
    /// Hold authority serialization across durable read admission and evaluation.
    #[cfg(feature = "legacy-task")]
    fn change_accounted<T>(
        &self,
        decision: bool,
        admission: impl FnOnce(&mut State) -> Result<(), CoreError>,
        work: impl FnOnce(&mut State) -> Result<T, CoreError>,
    ) -> Result<T, CoreError> {
        let _ticket = self.enter()?;
        let _guard = self.gate.lock_cancellable()?;
        if self.fenced.load(Ordering::Acquire) {
            return Err(CoreError::Fenced);
        }
        let mut s = self.read().inspect_err(|error| self.fence_error(engine::kv::FenceSite::Read, error))?;
        let previous_relations = relation_names(&s);
        let admitted = admission(&mut s);
        // This write does not decide the business operation. A cancelled or lost
        // invocation can consume read work but cannot publish an unreserved read.
        self.persist(&mut s, Some(&previous_relations), false)?;
        admitted?;
        let previous_relations = relation_names(&s);
        let result = work(&mut s);
        self.persist(&mut s, Some(&previous_relations), decision)?;
        result
    }
    #[cfg(feature = "legacy-task")]
    fn persist(
        &self,
        s: &mut State,
        previous_relations: Option<&BTreeMap<(bool, u64), String>>,
        decision: bool,
    ) -> Result<(), CoreError> {
        self.persist_checked(s, previous_relations, decision, &mut || true)
            .map_err(|error| match error {
                GuardedPersistError::Core(error) => error,
                GuardedPersistError::ClockExpired => {
                    unreachable!("ordinary persistence has no clock lease")
                }
            })
    }
    fn persist_checked(
        &self,
        s: &mut State,
        previous_relations: Option<&BTreeMap<(bool, u64), String>>,
        decision: bool,
        valid_lease: &mut dyn FnMut() -> bool,
    ) -> Result<(), GuardedPersistError> {
        self.persist_checked_mode(s, previous_relations, decision, true, valid_lease, false)
    }
    // Caller holds gate; counter reservations share crash/cancellation handling
    // with SQL publication without advancing the SQL conflict generation.
    fn persist_counters(&self, s: &mut State) -> Result<(), CoreError> {
        // Counter reservations do not change relation identities or names.
        self.persist_checked_mode(s, None, false, false, &mut || true, false)
            .map_err(|error| match error {
                GuardedPersistError::Core(error) => error,
                GuardedPersistError::ClockExpired => unreachable!("counter write has no lease"),
            })
    }
    fn persist_checked_mode(
        &self,
        s: &mut State,
        previous_relations: Option<&BTreeMap<(bool, u64), String>>,
        decision: bool,
        bump_generation: bool,
        valid_lease: &mut dyn FnMut() -> bool,
        authority_managed: bool,
    ) -> Result<(), GuardedPersistError> {
        // Callers capture stable relation ids under the gate before mutation.
        let (renames, forgotten) = if let Some(old) = previous_relations {
            let new = relation_names(s);
            let renames: Vec<_> = new
                .iter()
                .filter_map(|(id, name)| {
                    old.get(id)
                        .filter(|old_name| *old_name != name)
                        .map(|old_name| (old_name.clone(), name.clone()))
                })
                .collect();
            let mut forgotten: std::collections::BTreeSet<_> = old
                .iter()
                .filter(|(id, _)| !new.contains_key(id))
                .map(|(_, name)| name.clone())
                .collect();
            let created: std::collections::BTreeSet<_> = new
                .iter()
                .filter(|(id, _)| !old.contains_key(id))
                .map(|(_, name)| name.clone())
                .collect();
            if authority_managed {
                // The caller has installed authority for the replacement identity.
                forgotten.retain(|name| !created.contains(name));
            } else {
                forgotten.extend(created);
            }
            // A rename into a dropped name owns that destination after publication.
            // Conversely a newly created relation reusing a rename source is cleared.
            for (_, destination) in &renames {
                forgotten.remove(destination);
            }
            (renames, forgotten)
        } else {
            (Vec::new(), std::collections::BTreeSet::new())
        };
        self.merge_live_sequences(s)?;
        if bump_generation {
            s.sql.generation = s
                .sql
                .generation
                .checked_add(1)
                .ok_or(CoreError::Sql(relational::SqlError::Overflow))?;
        }
        let values = records(s)?;
        let point = *self.crash.lock().map_err(|_| CoreError::Fenced)?;
        let persisted = (|| {
            let site = engine::kv::FenceSite::LegacyPersist;
            let storage = |error| self.legacy_io(site, error);
            let mut tx = self
                .db(LEGACY_STORAGE)?
                .begin_write()
                .map_err(|error| self.legacy_io(site, error))?;
            tx.set_durability(Durability::Immediate).map_err(io)?;
            {
                let mut t = tx
                    .open_table(RECORDS)
                    .map_err(|error| self.legacy_io(site, error))?;
                if !values.contains_key(&vec![8]) {
                    t.remove(&[8][..]).map_err(storage)?;
                }
                if !values.contains_key(&vec![10]) {
                    t.remove(&[10][..]).map_err(storage)?;
                }
                let stale: Vec<Vec<u8>> = t
                    .iter()
                    .map_err(storage)?
                    .map(|item| item.map(|(key, _)| key.value().to_vec()).map_err(storage))
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter()
                    .filter(|key| key.first() == Some(&11) && !values.contains_key(key))
                    .collect();
                for key in stale {
                    t.remove(key.as_slice()).map_err(storage)?;
                }
                for (k, v) in &values {
                    t.insert(k.as_slice(), v.as_slice()).map_err(storage)?;
                }
            }
            roles::rename_tables(&tx, &renames)?;
            roles::forget_relations(&tx, &forgotten)?;
            roles::write_sequence_owners(&tx, s)?;
            if point == CrashPoint::BeforeCommit {
                crash_now();
            }
            if point == CrashPoint::ErrorBeforeCommit {
                return Err(CoreError::Storage("injected before commit".into()).into());
            }
            if decision {
                self.decision_point(DecisionPoint::BeforeDecision);
                if !valid_lease() {
                    return Err(GuardedPersistError::ClockExpired);
                }
                crate::cancellation::decide()?;
                if !valid_lease() {
                    return Err(GuardedPersistError::ClockExpired);
                }
                self.decision_point(DecisionPoint::Decided);
            } else {
                crate::cancellation::check()?;
            }
            tx.commit().map_err(|error| self.legacy_io(site, error))?;
            s.sequence_owner_updates.clear();
            self.accept_sequences(s)?;
            if decision {
                self.decision_point(DecisionPoint::Committed);
            }
            if point == CrashPoint::AfterCommit {
                crash_now();
            }
            if point == CrashPoint::ErrorAfterCommit
                || (decision && point == CrashPoint::ErrorAfterDecisionCommit)
            {
                return Err(CoreError::Storage("injected lost commit response".into()).into());
            }
            if point == CrashPoint::BeforeAck {
                crash_now();
            }
            Ok(())
        })();
        if let Err(e) = persisted {
            if let GuardedPersistError::Core(error) = &e
                && *error != CoreError::Cancelled
            {
                self.fence_error(engine::kv::FenceSite::LegacyPersist, error);
            }
            return Err(e);
        }
        Ok(())
    }
    #[cfg(feature = "legacy-task")]
    fn request(&self, r: &Request) -> Result<(), CoreError> {
        if let Selection::Keys(k) = &r.selection
            && k.len() > self.limits.selected_keys
        {
            return Err(CoreError::Limit);
        }
        Ok(())
    }
    #[cfg(feature = "legacy-task")]
    fn selection_bound(&self, s: &State, task: TaskId, r: &Request) -> Result<(), CoreError> {
        if let Selection::Predicate {
            tenant,
            status,
            from,
            until,
        } = &r.selection
            && s.view
                .tasks
                .get(&task)
                .is_some_and(|t| t.grant.policy.tenant == *tenant)
        {
            let count = s
                .view
                .rows
                .iter()
                .filter(|(k, v)| {
                    k.tenant == *tenant
                        && v.status == *status
                        && (*from..*until).contains(&v.failed_at)
                })
                .take(self.limits.selected_keys.saturating_add(1))
                .count();
            if count > self.limits.selected_keys {
                return Err(CoreError::Limit);
            }
        }
        Ok(())
    }
}
/// Trusted local administrator. Never hand this to a task executor or RPC caller.
#[derive(Clone)]
pub struct Admin {
    data_directory: Option<std::path::PathBuf>,
    inner: Arc<Inner>,
}
#[derive(Clone)]
#[cfg(feature = "legacy-task")]
pub struct Executor {
    inner: Arc<Inner>,
    credential: Credential,
}
pub use v2::maintenance::MaintenanceStats;
pub use v2::reclaim::ReclaimStats;

impl Admin {
    #[allow(dead_code, reason = "Used by the later recovery supervisor")]
    pub(crate) fn fence_cause(&self) -> Option<engine::kv::FenceCause> {
        match &self.inner.backend {
            Backend::Legacy(_) => self.inner.legacy_fence.cause(),
            Backend::V2(slot) => slot.fence_cause(),
        }
    }

    /// Stop local commit decisions during service shutdown. Idempotent.
    pub fn stop_submissions(&self) {
        if let Backend::V2(slot) = &self.inner.backend { slot.stop_submissions(); }
    }
    #[cfg(any(test, feature = "test-commit-sync"))]
    pub fn set_commit_sync_hook(&self, hook: Option<Arc<crate::CommitSyncHook>>) {
        if let Backend::V2(slot) = &self.inner.backend { slot.set_sync_hook(hook); }
    }

    /// Test seam for expiry, receipt, preview and retention timestamps.
    /// None restores the system clock.
    #[doc(hidden)]
    pub fn set_task_wall_clock(&self, ms: Option<u64>) -> Result<(), CoreError> {
        let Backend::V2(slot) = &self.inner.backend else { return Err(CoreError::Unsupported); };
        slot.set_task_wall_clock(ms)
    }

    pub(crate) fn lease(&self, kind: Holder) -> Result<Option<EngineLease>, CoreError> {
        self.inner.lease(kind)
    }

    #[allow(dead_code, reason = "Used by the recovery supervisor")]
    pub(crate) fn storage_status(&self) -> StorageStatus {
        match &self.inner.backend {
            Backend::Legacy(_) => StorageStatus::Legacy,
            Backend::V2(slot) => slot.status(),
        }
    }

    #[cfg(test)]
    pub(crate) fn test_v2(&self) -> EngineLease {
        self.lease(Holder::Admin).unwrap().expect("v2 engine")
    }

    /// SQL admission count for commit-Sync qualification.
    #[cfg(feature = "test-commit-sync")]
    pub fn sql_operations_live(&self) -> usize {
        self.inner.sql_operations.load(Ordering::Acquire)
    }

    /// Local database directory, when this administrator is backed by local storage.
    pub fn data_directory(&self) -> Option<&Path> { self.data_directory.as_deref() }
    fn with_data_directory(mut self, path: &Path) -> Self {
        self.data_directory = Some(std::path::absolute(path).unwrap_or_else(|_| path.to_owned()));
        self
    }

    /// Statement slots of task sessions (GD-0061 R3-401).
    pub(crate) fn task_load(&self) -> &Arc<crate::task::limits::TaskLoad> {
        &self.inner.task_load
    }

    /// Runs one deterministic MVCC GC round on a v2 database (Kernel, then
    /// Catalog, Rows and Index), as the background thread does over several
    /// ticks. Legacy storage and replicated databases run no GC.
    pub fn maintenance_step(&self) -> Result<MaintenanceStats, CoreError> {
        let _ticket = self.inner.enter_sql()?;
        match self.lease(Holder::Admin)? {
            Some(db) => db.maintenance_step(),
            _ => Ok(MaintenanceStats::default()),
        }
    }

    /// Sets the v2 GC retention window (default 60 s). Shortening it lets the
    /// next round collect sooner; lengthening it never rewinds the durable
    /// watermark. No effect on legacy storage.
    pub fn set_gc_retention(&self, retention: std::time::Duration) -> Result<(), CoreError> {
        let _ticket = self.inner.enter_sql()?;
        if let Backend::V2(slot) = &self.inner.backend {
            slot.set_gc_retention(retention)?;
        }
        Ok(())
    }

    /// Sets the task retention sweeper's settings (GD-0061 D5): read-log,
    /// before-values and outcome windows, the pass interval and the batch
    /// size. The next background pass uses them. No effect on legacy storage.
    pub fn set_task_retention(&self, config: crate::task::RetentionConfig) -> Result<(), CoreError> {
        let _ticket = self.inner.enter_sql()?;
        if let Backend::V2(slot) = &self.inner.backend {
            slot.set_task_retention(config)?;
        }
        Ok(())
    }
}

impl Admin {
    /// One-time trusted provisioning. A task's counters and caps cannot be reset.
    /// Missing provisioning (including legacy formats) denies remote disclosure.
    #[cfg(feature = "legacy-task")]
    pub fn provision_task_resources(
        &self,
        task: TaskId,
        limits: TaskResources,
    ) -> Result<(), CoreError> {
        self.inner.db(TASK_EXECUTORS)?;
        self.inner.change(|s| {
            if !s.view.tasks.contains_key(&task) {
                return Err(CoreError::Policy(Error::UnknownTask));
            }
            if s.accounting.contains_key(&task) {
                return Err(CoreError::Policy(Error::TaskExists));
            }
            s.accounting.insert(
                task,
                TaskAccounting {
                    limits,
                    used: TaskResources::default(),
                },
            );
            Ok(())
        })
    }
    #[cfg(feature = "legacy-task")]
    pub fn task_resources(&self, task: TaskId) -> Result<Option<TaskAccounting>, CoreError> {
        self.inner.db(TASK_EXECUTORS)?;
        self.inner.inspect(|s| Ok(s.accounting.get(&task).copied()))
    }
    /// Trusted test inspection: number of requests actually queued on v2 locks.
    pub fn waiting_lock_count(&self) -> usize {
        self.lease(Holder::Admin).ok().flatten().map_or(0, |db| db.waiting_lock_count())
    }

    /// Trusted test seam for deterministic local race verification. Not a wire API.
    pub fn set_decision_hook(&self, hook: Option<Arc<dyn Fn(DecisionPoint) + Send + Sync>>) {
        *self.inner.decision_hook.lock().expect("decision hook lock") = hook;
    }
    /// Trusted gateway clock high-watermark, in the existing atomic metadata.
    /// This is not a wall-clock oracle; callers must establish time certainty.
    pub fn certify_server_time(&self, lower: u64, upper: u64) -> Result<(), CoreError> {
        self.inner.change(|s| {
            if lower > upper || upper < s.last_time {
                return Err(CoreError::Policy(Error::TimeRegression));
            }
            s.last_time = upper;
            Ok(())
        })
    }
    fn attach(db: Database, limits: Limits) -> Result<Self, CoreError> {
        let a = Self::attach_backend(Backend::Legacy(db), limits)?;
        a.inner.read()?;
        Ok(a)
    }
    fn attach_backend(backend: Backend, limits: Limits) -> Result<Self, CoreError> {
        if limits.admitted == 0 || limits.selected_keys == 0 {
            return Err(CoreError::Limit);
        }
        let fenced = match &backend {
            Backend::Legacy(_) => Arc::default(),
            Backend::V2(slot) => Arc::clone(&slot.native_fenced),
        };
        Ok(Self {
            data_directory: None,
            inner: Arc::new(Inner {
                backend,
                gate: crate::cancellation::WaitMutex::new(()),
                admitted: AtomicUsize::new(0),
                sql_transactions: AtomicUsize::new(0),
                sql_operations: AtomicUsize::new(0),
                task_load: Arc::new(crate::task::limits::TaskLoad::new(limits.admitted)),
                fenced,
                legacy_fence: Default::default(),
                limits,
                crash: Mutex::new(CrashPoint::None),
                protected_point: Mutex::new(None),
                protected_hook: Mutex::new(None),
                decision_hook: Mutex::new(None),
                sequences: Mutex::new(sequences::Live::default()),
                native_once_owners: crate::cancellation::WaitMutex::new(BTreeMap::new()),
            }),
        })
    }
    #[cfg(feature = "engine-rocksdb")]
    pub(crate) fn attach_v2_backend(
        backend: Arc<dyn engine::kv::KvBackend>,
        limits: Limits,
        write_set_bytes: usize,
    ) -> Result<Admin, CoreError> {
        Self::attach_backend(
            Backend::V2(EngineSlot::new(v2::V2Db::from_backend(backend, write_set_bytes)?)),
            limits,
        )
    }
    /// Explicit create-new directory for the chosen engine. Legacy delegates to `create`.
    pub fn create_engine(
        path: impl AsRef<Path>,
        limits: Limits,
        engine: Engine,
    ) -> Result<Self, CoreError> {
        upgrade::check_startup(path.as_ref())?;
        match engine {
            Engine::Legacy => Self::create(path, limits),
            Engine::V2 => {
                #[cfg(not(feature = "engine-rocksdb"))]
                {
                    let _ = (path, limits);
                    Err(CoreError::EngineUnsupported {
                        engine: "v2",
                        feature: ROCKSDB_NOT_COMPILED,
                    })
                }
                #[cfg(feature = "engine-rocksdb")]
                {
                    if limits.admitted == 0 || limits.selected_keys == 0 {
                        return Err(CoreError::Limit);
                    }
                    let db = v2::V2Db::create(path.as_ref())?;
                    let slot = EngineSlot::for_directory(db, path.as_ref(), None);
                    Self::attach_backend(Backend::V2(slot), limits)
                        .map(|admin| admin.with_data_directory(path.as_ref()))
                }
            }
        }
    }
    /// Explicit create-new legacy directory. Existing paths, including empty ones, reject.
    /// The CLI and development server default to v2 when `engine-rocksdb` is enabled;
    /// embedders keep the legacy default here and can opt in with `Self::create_engine`.
    pub fn create(path: impl AsRef<Path>, limits: Limits) -> Result<Self, CoreError> {
        let mut bytes = [0u8; 16];
        getrandom::fill(&mut bytes).map_err(io)?;
        Self::create_identity(path, limits, u128::from_be_bytes(bytes))
    }
    fn create_identity(
        path: impl AsRef<Path>,
        limits: Limits,
        domain: u128,
    ) -> Result<Self, CoreError> {
        Self::create_state(path, limits, empty(domain), RecoveryPoint::None)
    }
    fn create_state(
        path: impl AsRef<Path>,
        limits: Limits,
        state: State,
        point: RecoveryPoint,
    ) -> Result<Self, CoreError> {
        upgrade::check_startup(path.as_ref())?;
        if limits.admitted == 0 || limits.selected_keys == 0 {
            return Err(CoreError::Limit);
        }
        validate(&state)?;
        let values = records(&state)?;
        fs::create_dir(path.as_ref()).map_err(|e| {
            if e.kind() == std::io::ErrorKind::AlreadyExists {
                CoreError::Exists
            } else {
                io(e)
            }
        })?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path.as_ref().join("data.redb"))
            .map_err(io)?;
        let db = Database::builder()
            .set_cache_size(8 * 1024 * 1024)
            .create_file(file)
            .map_err(io)?;
        recovery(
            point,
            RecoveryPoint::InstallCreated,
            RecoveryPoint::CrashInstallCreated,
        )?;
        let mut tx = db.begin_write().map_err(io)?;
        tx.set_durability(Durability::Immediate).map_err(io)?;
        {
            let mut t = tx.open_table(RECORDS).map_err(io)?;
            for (k, v) in values {
                t.insert(k.as_slice(), v.as_slice()).map_err(io)?;
            }
        }
        recovery(
            point,
            RecoveryPoint::InstallStaged,
            RecoveryPoint::CrashInstallStaged,
        )?;
        tx.commit().map_err(io)?;
        recovery(
            point,
            RecoveryPoint::InstallCommitted,
            RecoveryPoint::CrashInstallCommitted,
        )?;
        Self::attach(db, limits).map(|admin| admin.with_data_directory(path.as_ref()))
    }
    /// Existing database only: no initialization or replacement on damage.
    pub fn open(path: impl AsRef<Path>, limits: Limits) -> Result<Self, CoreError> {
        Self::open_with_holder_token(path.as_ref(), limits, None).map(|admin| admin.with_data_directory(path.as_ref()))
    }
    fn open_with_holder_token(
        path: &Path,
        limits: Limits,
        token: Option<&[u8]>,
    ) -> Result<Self, CoreError> {
        upgrade::check_startup(path)?;
        if v2::has_traces(path) {
            match engine::meta::classify_dir(path).map_err(v2::format_error)? {
                engine::meta::DirClass::V2 => {
                    let db = match token {
                        Some(token) => v2::V2Db::open_with_holder(path, Some(token))?,
                        None => v2::V2Db::open(path)?,
                    };
                    #[cfg(feature = "engine-rocksdb")]
                    let slot = EngineSlot::for_directory(db, path, token);
                    #[cfg(not(feature = "engine-rocksdb"))]
                    let slot = EngineSlot::new(db);
                    return Self::attach_backend(Backend::V2(slot), limits);
                }
                engine::meta::DirClass::Legacy => {}
                engine::meta::DirClass::Empty => {
                    return Err(CoreError::Corrupt("incomplete v2 directory".into()));
                }
            }
        }
        Self::open_legacy(path, limits, true)
    }

    /// Read legacy data without accepting admission. Buffered redb writes,
    /// including close-time maintenance, are discarded on drop. This handle
    /// must not be used for durable writes.
    #[cfg(feature = "engine-rocksdb")]
    pub(super) fn open_legacy_unaccepted(path: &Path, limits: Limits) -> Result<Self, CoreError> {
        upgrade::check_startup(path)?;
        if engine::meta::classify_dir(path).map_err(v2::format_error)?
            != engine::meta::DirClass::Legacy
        {
            return Err(io("upgrade requires a legacy v1 database"));
        }
        Self::open_legacy(path, limits, false)
    }

    fn open_legacy(path: &Path, limits: Limits, accept: bool) -> Result<Self, CoreError> {
        if limits.admitted == 0 || limits.selected_keys == 0 {
            return Err(CoreError::Limit);
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path.join("data.redb"))
            .map_err(io)?;
        let admission = legacy_open::Admission::new(file).map_err(io)?;
        let db = legacy_redb("opening database", || {
            Database::builder()
                .set_cache_size(8 * 1024 * 1024)
                .create_with_backend(admission.clone())
                .map_err(io)
        })?;
        // An opener delayed after the first check must not acquire the legacy
        // database in the Windows upgrade's close-before-rename interval.
        upgrade::check_startup(path)?;
        let admin = Self::attach(db, limits)?;
        if accept {
            admission.accept().map_err(io)?;
        }
        Ok(admin)
    }

    pub fn snapshot(&self) -> Result<Snapshot, CoreError> {
        self.inner.db(KERNEL_RECORDS)?;
        self.inner.inspect(|s| Ok(s.view.clone()))
    }
    /// The storage engine behind this handle.
    pub fn engine(&self) -> Engine {
        match &self.inner.backend {
            Backend::Legacy(_) => Engine::Legacy,
            Backend::V2(_) => Engine::V2,
        }
    }
    /// Concurrent explicit SQL/native transactions this database admits before
    /// `BEGIN` returns `Busy`.
    pub fn max_transactions(&self) -> usize {
        self.inner.max_transactions()
    }
    pub fn authority_metadata(&self) -> Result<(u128, u64, u64, Option<u128>), CoreError> {
        // V2 has a durable SQL authority domain, but no native approval clock/ledger.
        if let Some(db) = self.lease(Holder::Admin)? {
            let _ticket = self.inner.enter()?;
            return Ok((
                u128::from_be_bytes(db.meta.domain),
                0,
                0,
                db.origin_at(db.visible_ts()?)?,
            ));
        }
        self.inner
            .inspect(|s| Ok((s.view.domain, s.last_time, s.next_approval, s.origin)))
    }
    pub fn put(&self, k: Key, mut v: Row) -> Result<(), CoreError> {
        self.inner.db(KERNEL_RECORDS)?;
        self.inner.change(|s| {
            v.version = s.view.rows.get(&k).map_or(Ok(0), |old| {
                old.version
                    .checked_add(1)
                    .ok_or(CoreError::Policy(Error::Overflow))
            })?;
            s.view.rows.insert(k, v);
            Ok(())
        })
    }
    #[cfg(feature = "legacy-task")]
    pub fn grant(&self, g: Grant) -> Result<Executor, CoreError> {
        self.inner.db(TASK_EXECUTORS)?;
        let credential = self.inner.change(|s| {
            if s.view.tasks.contains_key(&g.task) {
                return Err(CoreError::Policy(Error::TaskExists));
            }
            if !policy::grant_valid(&g) {
                return Err(CoreError::Policy(Error::InvalidGrant));
            }
            let c = Credential {
                domain: s.view.domain,
                task: g.task,
                executor: g.executor,
            };
            s.view.tasks.insert(
                g.task,
                TaskState {
                    grant: g,
                    revoked: false,
                    touched: Default::default(),
                    transitions: 0,
                },
            );
            Ok(c)
        })?;
        Ok(Executor {
            inner: self.inner.clone(),
            credential,
        })
    }
    /// Trusted rebind after restart; grants no task-broadening authority.
    #[cfg(feature = "legacy-task")]
    pub fn executor(&self, task: TaskId, principal: Principal) -> Result<Executor, CoreError> {
        self.inner.db(TASK_EXECUTORS)?;
        self.inner.inspect(|s| {
            let t = s
                .view
                .tasks
                .get(&task)
                .ok_or(CoreError::Policy(Error::Authentication))?;
            if t.grant.executor != principal {
                return Err(CoreError::Policy(Error::Authentication));
            }
            Ok(Executor {
                inner: self.inner.clone(),
                credential: Credential {
                    domain: s.view.domain,
                    task,
                    executor: principal,
                },
            })
        })
    }
    #[cfg(feature = "legacy-task")]
    pub fn revoke(&self, task: TaskId) -> Result<(), CoreError> {
        self.inner.db(TASK_EXECUTORS)?;
        self.inner.change(|s| {
            s.view
                .tasks
                .get_mut(&task)
                .ok_or(CoreError::Policy(Error::UnknownTask))?
                .revoked = true;
            Ok(())
        })
    }
    pub fn bump_schema(&self) -> Result<(), CoreError> {
        self.inner.db(KERNEL_RECORDS)?;
        self.inner.change(|s| {
            s.view.schema = s
                .view
                .schema
                .checked_add(1)
                .ok_or(CoreError::Policy(Error::Overflow))?;
            Ok(())
        })
    }
    #[cfg(feature = "legacy-task")]
    pub fn approve(&self, who: Principal, p: Preview, time: Time) -> Result<ApprovalId, CoreError> {
        self.inner.db(PROTECTED_OPERATIONS)?;
        self.inner.request(&p.request)?;
        self.inner.change(|s| {
            let t = s
                .view
                .tasks
                .get(&p.task)
                .ok_or(CoreError::Policy(Error::InvalidApproval))?;
            if t.grant.approver != who || who == t.grant.executor {
                return Err(CoreError::Policy(Error::InvalidApproval));
            }
            self.inner.selection_bound(s, p.task, &p.request)?;
            let current =
                policy::preview(s, p.task, &p.request, time).map_err(CoreError::Policy)?;
            if current != p {
                return Err(CoreError::Policy(Error::StalePreview));
            }
            let id = ApprovalId(s.next_approval);
            let next = s
                .next_approval
                .checked_add(1)
                .ok_or(CoreError::Policy(Error::Overflow))?;
            s.view.approvals.insert(
                id,
                Approval {
                    id,
                    approver: who,
                    preview: p,
                    revoked: false,
                    consumed: false,
                },
            );
            s.next_approval = next;
            Ok(id)
        })
    }
    #[cfg(feature = "legacy-task")]
    pub fn revoke_approval(&self, id: ApprovalId) -> Result<(), CoreError> {
        self.inner.db(PROTECTED_OPERATIONS)?;
        self.inner.change(|s| {
            s.view
                .approvals
                .get_mut(&id)
                .ok_or(CoreError::Policy(Error::InvalidApproval))?
                .revoked = true;
            Ok(())
        })
    }
    /// Fault seams require the trusted admin handle; never exposed by Executor.
    pub fn set_crash_point(&self, point: CrashPoint) -> Result<(), CoreError> {
        *self.inner.crash.lock().map_err(|_| CoreError::Fenced)? = point;
        Ok(())
    }
    /// Arms (or, with `None`, disarms) a protected-commit crash seam (GD-0061
    /// R3-619, D-I). Like [`Admin::set_crash_point`] it needs the trusted admin
    /// handle and is never driven by environment; nothing arms it by default.
    #[doc(hidden)]
    pub fn set_protected_point(
        &self,
        point: Option<crate::task::commit::ProtectedPoint>,
    ) -> Result<(), CoreError> {
        *self.inner.protected_point.lock().map_err(|_| CoreError::Fenced)? = point;
        Ok(())
    }
    /// Installs (or, with `None`, removes) the protected-commit pause seam
    /// (GD-0061 R3-902): `hook` runs on the committing thread at every
    /// [`crate::task::ProtectedPoint`] of the calls that start after this.
    /// Trusted admin handle only; nothing installs one by default.
    #[doc(hidden)]
    pub fn set_protected_hook(
        &self,
        hook: Option<crate::task::commit::ProtectedHook>,
    ) -> Result<(), CoreError> {
        *self.inner.protected_hook.lock().map_err(|_| CoreError::Fenced)? = hook;
        Ok(())
    }
    pub fn export(&self, path: impl AsRef<Path>) -> Result<[u8; 32], CoreError> {
        self.export_with_fault(path, RecoveryPoint::None)
    }
    /// Retains a unique sibling staging file even after successful publication.
    /// Hard-link creation is atomic/no-clobber on qualified local filesystems.
    /// File contents are synced; parent-directory power-loss durability is unqualified.
    pub fn export_with_fault(
        &self,
        path: impl AsRef<Path>,
        point: RecoveryPoint,
    ) -> Result<[u8; 32], CoreError> {
        #[cfg(feature = "engine-rocksdb")]
        if let Some(db) = self.lease(Holder::Export)? {
            let _ticket = self.inner.enter()?;
            return v2_export::export(&db, path.as_ref(), point);
        }
        self.inner.db(LOGICAL_EXPORT)?;
        let path = path.as_ref();
        let (format, payload) = self.inner.inspect(|s| {
            feature_envelope::export_allowed(feature_envelope::feature_bits_present(s)?)?;
            Ok((state_format(s)?, encode(&records(s)?)?))
        })?;
        let digest: [u8; 32] = Sha256::digest(&payload).into();
        let mut nonce = [0u8; 16];
        getrandom::fill(&mut nonce).map_err(io)?;
        let mut name = path
            .file_name()
            .ok_or(CoreError::Unsupported)?
            .to_os_string();
        name.push(format!(".staging-{:032x}", u128::from_be_bytes(nonce)));
        let stage = path.with_file_name(name);
        let mut f = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&stage)
            .map_err(io)?;
        f.write_all(MAGIC).map_err(io)?;
        recovery(
            point,
            RecoveryPoint::ExportWrite,
            RecoveryPoint::CrashExportWrite,
        )?;
        f.write_all(&format.to_be_bytes()).map_err(io)?;
        f.write_all(&(payload.len() as u64).to_be_bytes())
            .map_err(io)?;
        f.write_all(&digest).map_err(io)?;
        f.write_all(&payload).map_err(io)?;
        recovery(
            point,
            RecoveryPoint::ExportSync,
            RecoveryPoint::CrashExportSync,
        )?;
        f.sync_all().map_err(io)?;
        recovery(
            point,
            RecoveryPoint::ExportVerify,
            RecoveryPoint::CrashExportVerify,
        )?;
        Self::read_export(&stage)?;
        recovery(point, RecoveryPoint::ExportPublish, RecoveryPoint::None)?;
        fs::hard_link(&stage, path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::AlreadyExists {
                CoreError::Exists
            } else {
                io(e)
            }
        })?;
        if point == RecoveryPoint::CrashExportPublish {
            crash_now();
        }
        Ok(digest)
    }
    fn read_export(path: impl AsRef<Path>) -> Result<State, CoreError> {
        let mut f = OpenOptions::new().read(true).open(path).map_err(io)?;
        if f.metadata().map_err(io)?.len() > MAX_BYTES + 52 {
            return Err(CoreError::Limit);
        }
        let mut header = [0u8; 52];
        f.read_exact(&mut header).map_err(io)?;
        if &header[..8] != MAGIC
            || (u32::from_be_bytes(header[8..12].try_into().expect("length")) != 53
                && envelope_family(u32::from_be_bytes(
                    header[8..12].try_into().expect("length"),
                ))
                .is_err())
        {
            return Err(CoreError::Unsupported);
        }
        let len = u64::from_be_bytes(header[12..20].try_into().expect("length"));
        if len > MAX_BYTES || f.metadata().map_err(io)?.len() != len + 52 {
            return Err(CoreError::Corrupt("export length".into()));
        }
        let mut payload = vec![0u8; len as usize];
        f.read_exact(&mut payload).map_err(io)?;
        if Sha256::digest(&payload)[..] != header[20..52] {
            return Err(CoreError::Corrupt("export checksum".into()));
        }
        let values: BTreeMap<Vec<u8>, Vec<u8>> = decode(&payload)?;
        let metadata: Metadata = decode(
            values
                .get(&vec![0])
                .ok_or_else(|| CoreError::Corrupt("missing metadata".into()))?,
        )?;
        if metadata.format != u32::from_be_bytes(header[8..12].try_into().expect("length")) {
            return Err(CoreError::Corrupt("export format mismatch".into()));
        }
        if encode(&values)? != payload {
            return Err(CoreError::Corrupt("noncanonical export".into()));
        }
        from_records(values)
    }
    pub fn verify_export(path: impl AsRef<Path>) -> Result<Snapshot, CoreError> {
        #[cfg(feature = "engine-rocksdb")]
        if v2_export::is_export(path.as_ref())? {
            return v2_export::verify(path.as_ref());
        }
        Ok(Self::read_export(path)?.view)
    }
    /// Fresh identity; task and approval authority is revoked, forensic outcomes
    /// retain their original domains. Existing destination paths always reject.
    pub fn restore(
        export: impl AsRef<Path>,
        path: impl AsRef<Path>,
        limits: Limits,
    ) -> Result<Self, CoreError> {
        Self::restore_with_fault(export, path, limits, RecoveryPoint::None)
    }
    pub fn restore_with_fault(
        export: impl AsRef<Path>,
        path: impl AsRef<Path>,
        limits: Limits,
        point: RecoveryPoint,
    ) -> Result<Self, CoreError> {
        #[cfg(feature = "engine-rocksdb")]
        if v2_export::is_export(export.as_ref())? {
            return v2_export::restore(export.as_ref(), path.as_ref(), limits, point);
        }
        let mut s = Self::read_export(export)?;
        let old = s.view.domain;
        let mut bytes = [0u8; 16];
        getrandom::fill(&mut bytes).map_err(io)?;
        s.view.domain = u128::from_be_bytes(bytes);
        s.origin = Some(old);
        for t in s.view.tasks.values_mut() {
            t.revoked = true;
        }
        for p in s.view.approvals.values_mut() {
            p.revoked = true;
        }
        Self::create_state(path, limits, s, point)
    }
}
#[cfg(feature = "legacy-task")]
impl Executor {
    fn debit(s: &mut State, task: TaskId, debit: TaskResources) -> Result<(), CoreError> {
        let a = s
            .accounting
            .get_mut(&task)
            .ok_or(CoreError::Policy(Error::Budget))?;
        a.used = a.used.add(debit, a.limits)?;
        Ok(())
    }
    fn read_debit(s: &mut State, task: TaskId, r: &Request) -> Result<(), CoreError> {
        // Covers both selection_bound and enumerate. Charge even on rejection.
        let selected = match &r.selection {
            Selection::Keys(keys) => u64::try_from(keys.len()).map_err(|_| CoreError::Limit)?,
            Selection::Predicate { .. } => u64::try_from(s.view.rows.len())
                .ok()
                .and_then(|n| n.checked_mul(3))
                .and_then(|n| n.checked_add(1))
                .ok_or(CoreError::Limit)?,
        };
        let cached = s
            .view
            .operations
            .get(&(task, r.operation))
            .and_then(|b| b.decision.as_ref().ok())
            .map_or(0, |r| r.effects.len());
        let rows = selected.max(
            u64::try_from(cached)
                .ok()
                .and_then(|n| n.checked_add(1))
                .ok_or(CoreError::Limit)?,
        );
        Self::debit(
            s,
            task,
            TaskResources {
                read_rows: rows.max(1),
                ..Default::default()
            },
        )
    }
    /// Trusted gateway release primitive: reserve immutable encoded content before
    /// publishing it. Successful reservations are never refunded or deduplicated.
    /// The caller must provide a fresh trusted interval and a safe encoded bound.
    pub fn reserve_disclosure(
        &self,
        encoded_bytes: u64,
        lower: u64,
        upper: u64,
    ) -> Result<(), CoreError> {
        self.inner.change_decision(|s| {
            self.auth(s)?;
            Self::current(s, self.credential.task, lower, upper)?;
            Self::debit(
                s,
                self.credential.task,
                TaskResources {
                    disclosed_rows: 1,
                    encoded_bytes,
                    ..Default::default()
                },
            )
        })
    }
    fn current(s: &mut State, task: TaskId, lower: u64, upper: u64) -> Result<(), CoreError> {
        if lower > upper {
            return Err(CoreError::Policy(Error::UncertainTime));
        }
        policy::clock(s, Time::Certified(upper)).map_err(CoreError::Policy)?;
        let t = s
            .view
            .tasks
            .get(&task)
            .ok_or(CoreError::Policy(Error::Authentication))?;
        policy::active(t, lower)
            .and_then(|_| policy::active(t, upper))
            .map_err(CoreError::Policy)
    }
    /// Remote decision: revalidate current task and the full trusted time interval
    /// under the very same serialization gate as dedupe and commit.
    pub fn execute_current(
        &self,
        r: Request,
        lower: u64,
        upper: u64,
        approval: Option<ApprovalId>,
    ) -> Result<Decision, CoreError> {
        self.inner.request(&r)?;
        let admission_request = r.clone();
        self.inner.change_accounted(
            true,
            |s| {
                self.auth(s)?;
                Self::current(s, self.credential.task, lower, upper)?;
                Self::read_debit(s, self.credential.task, &admission_request)?;
                Ok(())
            },
            |s| {
                self.auth(s)?;
                Self::current(s, self.credential.task, lower, upper)?;
                if !s
                    .view
                    .operations
                    .contains_key(&(self.credential.task, r.operation))
                {
                    self.inner.selection_bound(s, self.credential.task, &r)?;
                }
                Ok(policy::execute(
                    s,
                    self.credential.task,
                    r,
                    Time::Certified(upper),
                    approval,
                ))
            },
        )
    }
    pub fn preview_current(
        &self,
        r: &Request,
        lower: u64,
        upper: u64,
    ) -> Result<Preview, CoreError> {
        self.inner.request(r)?;
        self.inner.change_accounted(
            false,
            |s| {
                self.auth(s)?;
                Self::current(s, self.credential.task, lower, upper)?;
                Self::read_debit(s, self.credential.task, r)?;
                Ok(())
            },
            |s| {
                self.auth(s)?;
                Self::current(s, self.credential.task, lower, upper)?;
                self.inner.selection_bound(s, self.credential.task, r)?;
                policy::preview(s, self.credential.task, r, Time::Certified(upper))
                    .map_err(CoreError::Policy)
            },
        )
    }
    /// Remote disclosure gate. Local forensic status intentionally remains usable
    /// after revoke; network callers must use this current-authority variant.
    pub fn status_current(
        &self,
        id: OperationId,
        lower: u64,
        upper: u64,
    ) -> Result<OperationStatus, CoreError> {
        self.inner.change_accounted(
            false,
            |s| {
                self.auth(s)?;
                Self::current(s, self.credential.task, lower, upper)?;
                let rows = s
                    .view
                    .operations
                    .get(&(self.credential.task, id))
                    .and_then(|b| b.decision.as_ref().ok())
                    .map_or(0, |r| r.effects.len());
                let read_rows = u64::try_from(rows)
                    .ok()
                    .and_then(|n| n.checked_add(1))
                    .ok_or(CoreError::Limit)?;
                Self::debit(
                    s,
                    self.credential.task,
                    TaskResources {
                        read_rows,
                        ..Default::default()
                    },
                )?;
                Ok(())
            },
            |s| {
                self.auth(s)?;
                Self::current(s, self.credential.task, lower, upper)?;
                Ok(s.view
                    .operations
                    .get(&(self.credential.task, id))
                    .map_or(OperationStatus::Absent, |b| {
                        OperationStatus::Decided(b.decision.clone())
                    }))
            },
        )
    }
    /// Current authorization without returning task metadata or decisions.
    pub fn validate_current(&self, time: Time) -> Result<(), CoreError> {
        self.inner.change(|s| {
            self.auth(s)?;
            let now = policy::clock(s, time).map_err(CoreError::Policy)?;
            policy::active(
                s.view
                    .tasks
                    .get(&self.credential.task)
                    .ok_or(CoreError::Policy(Error::Authentication))?,
                now,
            )
            .map_err(CoreError::Policy)
        })
    }
    fn auth(&self, s: &State) -> Result<(), CoreError> {
        let c = &self.credential;
        if c.domain != s.view.domain
            || s.view
                .tasks
                .get(&c.task)
                .is_none_or(|t| t.grant.executor != c.executor)
        {
            return Err(CoreError::Policy(Error::Authentication));
        }
        Ok(())
    }
    pub fn preview(&self, r: &Request, time: Time) -> Result<Preview, CoreError> {
        self.inner.request(r)?;
        self.inner.change(|s| {
            self.auth(s)?;
            self.inner.selection_bound(s, self.credential.task, r)?;
            policy::preview(s, self.credential.task, r, time).map_err(CoreError::Policy)
        })
    }
    /// Err(Storage) means outcome unknown; the handle is fenced. Drop all handles,
    /// reopen via Admin and resolve status using the original operation ID.
    pub fn execute(
        &self,
        r: Request,
        time: Time,
        approval: Option<ApprovalId>,
    ) -> Result<Decision, CoreError> {
        self.inner.request(&r)?;
        self.inner.change_decision(|s| {
            self.auth(s)?;
            if !s
                .view
                .operations
                .contains_key(&(self.credential.task, r.operation))
            {
                self.inner.selection_bound(s, self.credential.task, &r)?;
            }
            Ok(policy::execute(s, self.credential.task, r, time, approval))
        })
    }
    pub fn status(&self, id: OperationId) -> Result<OperationStatus, CoreError> {
        self.inner.inspect(|s| {
            self.auth(s)?;
            Ok(s.view
                .operations
                .get(&(self.credential.task, id))
                .map_or(OperationStatus::Absent, |b| {
                    OperationStatus::Decided(b.decision.clone())
                }))
        })
    }
}
// Legacy transactions own whole State snapshots. This named capture is the
// session counterpart of v2 F3c's live write-set mark; future transactional
// session state (including a deferred-work queue) belongs in this capture.
#[derive(Clone)]
struct SqlSavepointState {
    staged: State,
    deferral: relational::DeferralState,
}
struct SqlSavepoint {
    name: String,
    state: SqlSavepointState,
    /// Encoded size of the captured snapshot, charged against the session budget.
    bytes: u64,
}
/// Per-session cap on captured savepoint snapshot bytes, in addition to the 64-mark cap.
const SAVEPOINT_BYTES: u64 = 64 * 1024 * 1024;
/// Snapshot size by the same fixed-int record accounting as envelope_bound.
fn snapshot_bytes(s: &State) -> Result<u64, CoreError> {
    records(s)?
        .iter()
        .try_fold(8u64, |n, (k, v)| {
            n.checked_add(16)?
                .checked_add(k.len() as u64)?
                .checked_add(v.len() as u64)
        })
        .ok_or(CoreError::Limit)
}
fn deferral_bytes(state: &relational::DeferralState) -> Result<u64, CoreError> {
    let modes = u64::try_from(state.modes.len()).map_err(|_| CoreError::Limit)?;
    let mut bytes = modes.checked_mul(9).ok_or(CoreError::Limit)?;
    for keys in state.pending.values() {
        bytes = bytes.checked_add(24).ok_or(CoreError::Limit)?;
        for key in keys {
            bytes = bytes
                .checked_add(8)
                .and_then(|n| n.checked_add(key.len() as u64))
                .ok_or(CoreError::Limit)?;
        }
    }
    Ok(bytes)
}
pub(crate) type XactSlot = Arc<Mutex<Option<engine::xact::XactGuard>>>;

/// Sequence names and optional owning table keys, in catalog iteration order.
pub type SequenceOwners = [(String, Option<String>)];

/// Trusted session only. Dropping a session discards its staged transaction.
/// No lock or admission ticket lives across client calls.
pub struct SqlSession {
    privilege_callback: Option<PrivilegeCallback>,
    admin: Admin,
    staged: Option<State>,
    v2_transaction: Option<engine::txn::Transaction>,
    /// A typed operation reuses its checked transaction during binding and execution.
    pub(crate) native_generation: Option<Arc<engine::txn::TxnLifetime>>,
    /// A parked protected run (R3-606, C1); set only inside
    /// `execute_protected_v2`, never counted as an SQL transaction slot.
    protected: Option<ProtectedRun>,
    copy_input: Option<CopyInput>,
    savepoints: Vec<SqlSavepoint>,
    v2_savepoints: Vec<(String, engine::txn::Savepoint, relational::DeferralState)>,
    copy_freeze: Vec<std::collections::BTreeSet<u64>>,
    savepoint_budget: u64,
    aborted: bool,
    transaction_id: Option<u128>,
    transaction_clock_us: Option<i64>,
    extra_float_digits: i32,
    date_style: giggles_pg_types::temporal::DateStyleSettings,
    bytea_output: giggles_pg_types::bytea::ByteaOutput,
    max_value_bytes: usize,
    max_statement_tokens: usize,
    search_path: Vec<String>,
    schema_user: String,
    schema_context_revision: Option<u64>,
    search_path_input: Option<(Arc<str>, (usize, usize, bool))>,
    unnamed_plans: Vec<(UnnamedPlanKey, relational::Prepared)>,
    authorization_bindings: bool,
    authority_managed: bool,
    ddl_owner: Option<String>,
    staged_revision: u64,
    sequence_values: sequences::SessionValues,
    sequence_generation: Option<GenerationTag>,
    sequence_unsettled: v2_sql::sql_sequences::Unsettled,
    pending_sequences: sequences::Pending,
    xact: XactSlot,
    sequence_baseline: BTreeMap<u64, u128>,
    sequence_tables: Option<std::collections::BTreeSet<String>>,
    sequence_authority: SequenceAuthority,
    deferral: relational::DeferralState,
    isolation: SqlIsolation,
    /// The task this session is bound to (GD-0061 R3-202); set once by `bind_task`.
    pub(crate) task: Option<crate::task::context::TaskContext>,
    pub(crate) task_generation: Option<GenerationTag>,
    /// The release state of a task session (GD-0061 R3-308): reservation,
    /// pending `rows_read` and the current statement. Set with `task` by
    /// `bind_task`; the protected commit's temporary binding leaves it alone.
    pub(crate) task_state: Option<Arc<crate::task::release::TaskSessionState>>,
}
mod complete_relation;
pub(crate) use complete_relation::CompleteRelation;
pub use complete_relation::native_input_rows;

#[derive(PartialEq)]
struct UnnamedPlanKey {
    sql: String,
    declared_types: Vec<Option<relational::DataType>>,
    catalog_version: u64,
    generation: u64,
    search_path: Vec<String>,
    schema_user: String,
    date_order: giggles_pg_types::temporal::DateOrder,
}

impl Admin {
    pub fn sql_session(&self) -> SqlSession {
        SqlSession {
            privilege_callback: None,
            admin: self.clone(),
            staged: None,
            v2_transaction: None,
            native_generation: None,
            sequence_generation: None,
            protected: None,
            copy_input: None,
            savepoints: Vec::new(),
            v2_savepoints: Vec::new(),
            copy_freeze: vec![Default::default()],
            savepoint_budget: SAVEPOINT_BYTES,
            aborted: false,
            transaction_id: None,
            transaction_clock_us: None,
            extra_float_digits: 1,
            date_style: Default::default(),
            bytea_output: Default::default(),
            max_value_bytes: relational::DEFAULT_MAX_VALUE_BYTES,
            max_statement_tokens: relational::DEFAULT_MAX_STATEMENT_TOKENS,
            search_path: vec!["public".into()],
            schema_user: "giggles".into(),
            schema_context_revision: Some(0),
            search_path_input: None,
            unnamed_plans: Vec::new(),
            authorization_bindings: false,
            authority_managed: false,
            ddl_owner: None,
            staged_revision: 0,
            sequence_values: Default::default(),
            sequence_unsettled: Default::default(),
            pending_sequences: Default::default(),
            xact: Default::default(),
            sequence_baseline: BTreeMap::new(),
            sequence_tables: None,
            sequence_authority: SequenceAuthority::Unrestricted,
            deferral: Default::default(),
            isolation: SqlIsolation::ReadCommitted,
            task: None,
            task_generation: None,
            task_state: None,
        }
    }
    /// Catalog preflight for the lossless logical archive profile. Refuse catalog
    /// extensions (typmods, DEFAULTs, identity/serial, CHECK/FK/UNIQUE constraints,
    /// sequences, and varchar/char identity) and secondary indexes because the
    /// archive cannot preserve them.
    pub fn logical_export_catalog(&self) -> Result<(u64, Vec<relational::TableDef>), CoreError> {
        self.inner.db(LOGICAL_EXPORT)?;
        self.inner.inspect(|s| {
            if !s.sql.catalog_ext().is_empty() || s.sql.has_secondary_indexes_engine() {
                return Err(CoreError::Unsupported);
            }
            Ok((s.sql.epoch, s.sql.tables.values().cloned().collect()))
        })
    }
    /// Committed catalog epoch and catalog xid, read together. Legacy uses its
    /// durable publication counter; V2 uses whole physical HLC seconds, avoiding
    /// the raw timestamp's low-32-bit wrap every ~65 seconds. Recovered commit
    /// timestamps keep this nondecreasing across restarts and wall-clock rollback.
    /// V2 seconds fit u32 until 2106; differences stay below 2^31 for ~68 years.
    /// Neither backend reads private DDL.
    pub fn catalog_generation(&self) -> Result<(u64, u64), CoreError> {
        self.inner.inspect_metadata(|s| {
            let generation = match &self.inner.backend {
                Backend::Legacy(_) => s.sql.generation,
                Backend::V2(_) => catalog_xid_from_hlc(s.sql.epoch),
            };
            Ok((s.sql.epoch, generation))
        })
    }
    pub fn collation_oids(&self)->Result<BTreeMap<u64,Vec<u32>>,CoreError> { self.inner.inspect_metadata(|s|s.sql.collation_oids().map_err(CoreError::Sql)) }
    pub fn user_collations(&self) -> Result<Vec<(u32, relational::UserCollation)>, CoreError> {
        self.inner.inspect_metadata(|s| s.sql.catalog_ext().collations.iter().map(|(id,c)| Ok((relational::type_oid(*id).map_err(CoreError::Sql)?, c.clone()))).collect())
    }
    pub fn catalog_presentation(&self) -> Result<relational::CatalogPresentation, CoreError> {
        self.inner.inspect_metadata(|s| s.sql.catalog_presentation().map_err(CoreError::Sql))
    }
    pub fn catalog_schema_metadata(&self) -> Result<relational::SchemaMetadata, CoreError> {
        self.inner.inspect_metadata(|s| Ok(s.sql.schema_metadata()))
    }
    pub fn catalog(&self) -> Result<(u64, Vec<relational::TableDef>), CoreError> {
        self.inner
            .inspect_metadata(|s| Ok((s.sql.epoch, s.sql.tables.values().cloned().collect())))
    }
    /// Read column typmods and their catalog epoch from one committed snapshot.
    pub fn catalog_typmods(&self) -> Result<(u64, BTreeMap<u64, i32>), CoreError> {
        self.inner.inspect_metadata(|s| {
            let typmods = s
                .sql
                .catalog_ext()
                .columns
                .iter()
                .filter_map(|(&id, column)| column.typmod.map(|typmod| (id, typmod)))
                .collect();
            Ok((s.sql.epoch, typmods))
        })
    }
}
/// One coherent presentation read for the wire catalog adapter.
pub struct CatalogMetadata {
    pub sequences: Vec<(u32, String)>,
    pub presentation: relational::CatalogPresentation,
    pub tables: Vec<relational::TableDef>,
    pub schemas: relational::SchemaMetadata,
    pub types: relational::UserTypeCatalog,
    pub collations: Vec<(u32, relational::UserCollation)>,
    pub collation_oids: BTreeMap<u64,Vec<u32>>,
    pub generation: u64,
    pub typmods: BTreeMap<u64, i32>,
    pub indexes: Vec<IndexInfo>,
}
fn catalog_metadata_from(s: &MetadataState<'_>, path: &[String], generation: u64) -> Result<CatalogMetadata, CoreError> {

            let presentation = s.sql.catalog_presentation_in_path(path).map_err(CoreError::Sql)?;
            let tables = s.sql.tables.values().cloned().chain(presentation.views.values().map(|v| v.table.clone())).collect();
            Ok(CatalogMetadata {
                presentation, tables,
                sequences: s.sql.catalog_ext().sequences.iter().map(|(id,d)| Ok((relational::type_oid(*id).map_err(CoreError::Sql)?,d.name.clone()))).collect::<Result<_,CoreError>>()?,
                schemas: s.sql.schema_metadata(), types: s.sql.user_type_catalog(),
                collation_oids:s.sql.collation_oids().map_err(CoreError::Sql)?,
                collations: s.sql.catalog_ext().collations.iter().map(|(id,c)| Ok((relational::type_oid(*id).map_err(CoreError::Sql)?,c.clone()))).collect::<Result<_,CoreError>>()?,
                generation,
                typmods: s.sql.catalog_ext().columns.iter().filter_map(|(&id, c)| c.typmod.map(|m| (id, m)))
                    .chain(s.sql.views.into_iter().flatten().flat_map(|(_, v)| v.columns.iter().map(|c| (c.id, c.typmod)))).collect(),
                indexes: s.sql.secondary_descriptors_engine().iter().map(|d| IndexInfo {
                    id: d.id, table: d.table, name: relational::split_key(&d.name).1.into(),
                    columns: d.columns.clone(), unique: d.unique,
                }).collect(),
            })
    }

/// Owned execution metadata. Projection is deferred until the first inquiry.
pub struct PrivilegeMetadata {
    image: PrivilegeImage,
    path: Vec<String>,
    ownership: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    roles: roles::RoleCatalog,
    roles_generation: Option<GenerationTag>,
    generation: u64,
}
impl PrivilegeMetadata {
    pub fn search_path(&self) -> &[String] { &self.path }
    pub(crate) fn set_search_path(&mut self, path: Vec<String>) { self.path = path; }
    pub fn catalog(&self) -> Result<CatalogMetadata, CoreError> {
        let image = self.image.get();
        catalog_metadata_from(&MetadataState::new(image, image.epoch, 0), &self.path, self.generation)
    }
    /// Read committed role and ACL records under the execution's generation.
    pub fn role_image(&self) -> Result<roles::RoleCatalogImage, roles::RoleError> {
        match &self.roles_generation {
            Some(tag) => self.roles.image_leased(Some(&tag.lease(Holder::Role)?)),
            None => self.roles.image_leased(None),
        }
    }

    pub fn table_owner(&self, registry: &roles::RoleCatalog, name: &str) -> Result<Option<String>, CoreError> {
        self.table_owner_with(name, || match &self.roles_generation {
            Some(_) => self.role_image()?.table_owner(name),
            None => registry.table_owner(name),
        })
    }

    /// A failed committed image read follows the same fence path as a failed
    /// committed owner lookup. Transaction-local owner writes can still be read.
    pub fn table_owner_in_image<'a>(
        &self, registry: impl Into<Option<&'a roles::RoleCatalogImage>>, name: &str,
    ) -> Result<Option<String>, CoreError> {
        self.table_owner_with(name, || registry.into().ok_or(CoreError::Fenced)?.table_owner(name))
    }

    fn table_owner_with(
        &self, name: &str, committed: impl FnOnce() -> Result<Option<String>, roles::RoleError>,
    ) -> Result<Option<String>, CoreError> {
        let key = roles::engine::encode_key(&[b"owner/".as_slice(), name.as_bytes()].concat())?;
        if let Some(value) = self.ownership.get(&key) {
            return value.as_ref().map(|v| String::from_utf8(v[1..].to_vec())
                .map_err(|e| CoreError::Corrupt(e.to_string()))).transpose();
        }
        committed().map_err(|_| CoreError::Fenced)
    }
}
/// Who answers, at V2 execution, for a sequence function call (GD-0084 Q24).
///
/// GigglesDB has no sequence ACLs: the owner-analogue of a sequence (a
/// superuser, or a member through inheritance of the owner of its owning table,
/// or of the standalone sequence itself) holds USAGE, SELECT and UPDATE
/// together, and table grants confer nothing. Roles, memberships and owners are
/// read from the latest committed role catalog at each call.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum SequenceAuthority {
    /// No execution check: embedded and internal sessions.
    #[default]
    Unrestricted,
    /// An ordinary database role (the session's effective role).
    Role(String),
    /// An application identity: owning tables by authority key; standalone
    /// sequences are never in scope.
    Tables(std::collections::BTreeSet<String>),
    /// No sequence authority at all.
    Denied,
}
/// One statement's privilege evaluator, installed only after authorization.
pub type PrivilegeCallback = Arc<dyn Fn(&str, &[relational::Value], &PrivilegeMetadata)
    -> Result<Option<bool>, CoreError> + Send + Sync>;
impl SqlSession {
    pub fn with_privilege_callback<T>(&mut self, callback: PrivilegeCallback,
        execute: impl FnOnce(&mut Self) -> T) -> T {
        let previous = self.privilege_callback.replace(callback);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| execute(self)));
        self.privilege_callback = previous;
        match result { Ok(result) => result, Err(panic) => std::panic::resume_unwind(panic) }
    }
}
enum PrivilegeImage { Legacy(Arc<State>), Catalog(Arc<relational::Relational>) }
impl PrivilegeImage {
    fn get(&self) -> &relational::Relational { match self { Self::Legacy(s) => &s.sql, Self::Catalog(s) => s } }
}
#[cfg(test)]
thread_local! { static PRIVILEGE_CAPTURES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) }; }
pub(crate) type PrivilegeProvider = Arc<dyn Fn() -> PrivilegeMetadata + Send + Sync>;
fn privilege_provider(
    image: PrivilegeImage, path: Vec<String>, txn: Option<&engine::txn::Transaction>,
    roles: roles::RoleCatalog, db: Option<&EngineLease>,
) -> PrivilegeProvider {
    let roles_generation = db.map(EngineLease::tag);
    let writes = txn.map(|txn| txn.privilege_writes());
    Arc::new(move || {
        #[cfg(test)]
        PRIVILEGE_CAPTURES.set(PRIVILEGE_CAPTURES.get() + 1);
        let generation = if roles_generation.is_some() { catalog_xid_from_hlc(image.get().epoch) } else { image.get().generation };
        PrivilegeMetadata { image: match &image { PrivilegeImage::Legacy(s) => PrivilegeImage::Legacy(s.clone()),
            PrivilegeImage::Catalog(s) => PrivilegeImage::Catalog(s.clone()) }, path: path.clone(), generation,
            roles: roles.clone(), roles_generation: roles_generation.clone(),
            ownership: writes.iter().flat_map(|writes| writes.iter())
                .filter(|((space,key),_)| *space == engine::kv::Keyspace::Catalog && key.first() == Some(&10))
                .map(|((_,key),value)| (key.clone(),value.clone())).collect() }
    })
}

/// One conversion point for the V2 catalog xid (physical milliseconds -> seconds).
fn catalog_xid_from_hlc(epoch: u64) -> u64 {
    (epoch >> 16) / 1000
}
impl SqlSession {
    /// Test inspection of PK ordering, allocation and catalog extension version.
    #[doc(hidden)]
    pub fn primary_key_birth_metadata(&self) -> Result<(BTreeMap<u64, u64>, u64, u32), CoreError> {
        self.inspect_metadata(|s| Ok((
            s.sql.tables.values().filter_map(|t| {
                let birth = s.sql.primary_key_order(t.id);
                (birth != 0).then_some((t.id, birth))
            }).collect(), s.sql.catalog_parts().2, s.sql.catalog_ext().wire_version(),
        )))
    }
    pub fn collation_authority(&self, p: &relational::Prepared) -> Result<Vec<(String, Option<String>)>, CoreError> {
        self.inspect_metadata(|s| p.collation_authority(&s.sql).map_err(CoreError::Sql))
    }
    pub fn collation_oids(&self)->Result<BTreeMap<u64,Vec<u32>>,CoreError> { self.inspect_metadata(|s|s.sql.collation_oids().map_err(CoreError::Sql)) }
    pub fn user_collations(&self) -> Result<Vec<(u32, relational::UserCollation)>, CoreError> {
        self.inspect_metadata(|s| s.sql.catalog_ext().collations.iter().map(|(id,c)| Ok((relational::type_oid(*id).map_err(CoreError::Sql)?, c.clone()))).collect())
    }
    /// Logical FK binding names for tests (constraint names should be unique in the fixture).
    #[doc(hidden)]
    pub fn foreign_key_targets(&self) -> Result<BTreeMap<String, String>, CoreError> {
        use relational::catalog_ext::{ConstraintKind, ReferencedKey};
        self.inspect_metadata(|s| Ok(s.sql.catalog_ext().constraints.values().flatten()
            .filter_map(|c| {
                let ConstraintKind::ForeignKey { ref_table, target, .. } = c.kind else { return None };
                let name = match target {
                    ReferencedKey::Unknown => "unknown".into(),
                    ReferencedKey::PrimaryKey => {
                        let table = s.sql.tables.values().find(|t| t.id == ref_table).unwrap();
                        let pk = s.sql.table_constraints(ref_table).iter()
                            .find(|c| matches!(c.kind, ConstraintKind::PrimaryKey))
                            .map(|c| c.name.clone()).unwrap_or_else(|| format!("{}_pkey", table.name));
                        format!("pk:{pk}")
                    }
                    ReferencedKey::Secondary(id) => {
                        let index = s.sql.secondary_descriptors_engine().iter().find(|d| d.id == id).unwrap();
                        format!("index:{}", relational::split_key(&index.name).1)
                    }
                };
                Some((c.name.clone(), name))
            }).collect()))
    }
    /// Coherent catalog metadata, including V2 transaction-private DDL.
    pub fn catalog_metadata(&self) -> Result<CatalogMetadata, CoreError> {
        let read = |s: &MetadataState<'_>| {
            catalog_metadata_from(s, &self.search_path, match &self.admin.inner.backend {
                Backend::Legacy(_) => s.sql.generation,
                Backend::V2(_) => catalog_xid_from_hlc(s.sql.epoch),
            })
        };
        // Legacy wire catalogs retain their established committed-only behavior.
        match &self.admin.inner.backend {
            Backend::Legacy(_) => self.admin.inner.inspect_metadata(read),
            Backend::V2(_) => self.inspect_metadata(read),
        }
    }
    fn inspect_metadata<T>(
        &self,
        f: impl FnOnce(&MetadataState<'_>) -> Result<T, CoreError>,
    ) -> Result<T, CoreError> {
        let lease = self.statement_lease()?;
        self.inspect_metadata_leased(lease.as_deref(), f)
    }

    fn inspect_metadata_leased<T>(
        &self,
        db: Option<&EngineLease>,
        f: impl FnOnce(&MetadataState<'_>) -> Result<T, CoreError>,
    ) -> Result<T, CoreError> {
        match &self.admin.inner.backend {
            Backend::Legacy(_) => match &self.staged {
                Some(state) => f(&MetadataState::borrow(state)),
                None => self.admin.inner.inspect(|state| f(&MetadataState::borrow(state))),
            },
            Backend::V2(_) => {
                let _ticket = self.admin.inner.enter()?;
                db.ok_or(CoreError::Fenced)?.inspect_metadata(self.v2_transaction.as_ref(), f)
            }
        }
    }

    pub(crate) fn require_ddl_schema_engine(&self, name: &str) -> Result<(), CoreError> {
        self.inspect_metadata(|s| s.sql.require_relation_schema(name).map_err(CoreError::Sql))
    }

    /// Resolve a native dotted relation using this session's SQL search path.
    pub fn native_relation_reference(&self, name: &str) -> Result<String, CoreError> {
        self.resolve_relation_reference(&crate::operations::native_relation_key(name))
    }

    /// Resolve the schema selected for a native CREATE TABLE request.
    pub fn native_creation_schema(&self, table: &str) -> Result<String, CoreError> {
        let key = self.native_creation_key_engine(&crate::operations::native_relation_key(table))?;
        Ok(relational::split_key(&key).0.to_owned())
    }

    /// Resolve a native index to its owning table's internal relation key.
    pub fn native_index_table(&self, name: &str) -> Result<Option<String>, CoreError> {
        let name = crate::operations::native_relation_key(name);
        self.inspect_metadata(|s| {
            let key = s.sql.resolve_index_key(&name, &self.search_path);
            Ok(s.sql.index_table_key(&key))
        })
    }

    pub fn resolve_relation_reference(&self, name: &str) -> Result<String, CoreError> {
        let resolve = |s: &MetadataState<'_>| Ok(s.sql.resolve_relation_key(name, &self.search_path));
        match &self.staged {
            Some(s) => resolve(&MetadataState::borrow(s)),
            None => self.inspect_metadata(resolve),
        }
    }
    /// Retain exact binding identities for a gateway that memoizes SQL authorization.
    pub fn enable_authorization_bindings(&mut self) {
        self.authorization_bindings = true;
    }

    /// Bind dependency identities to this session without surfacing semantic errors
    /// before the caller has checked authority.
    pub fn bind_relation_context(
        &self,
        prepared: &relational::Prepared,
    ) -> Result<relational::Prepared, CoreError> {
        let lease = self.statement_lease()?;
        self.bind_relation_context_leased(prepared, lease.as_deref())
    }

    fn bind_relation_context_leased(
        &self,
        prepared: &relational::Prepared,
        db: Option<&EngineLease>,
    ) -> Result<relational::Prepared, CoreError> {
        Self::check_prepared_generation(prepared, db)?;
        let mut prepared = prepared.clone();
        if let Some(db) = db { prepared.engine_generation = Some(db.prepared_generation()); }
        prepared.set_schema_context(self.search_path.clone(), self.schema_user.clone());
        prepared.clear_authorization_binding_version();
        let version = if prepared.has_sql_authorization_binding() { self.statement_catalog_version() } else { None };
        self.inspect_metadata_leased(db, |s| {
            let Ok(mut bound) = prepared.bind_schemas(&s.sql) else { return Ok(prepared.clone()); };
            // Outside a fixed snapshot the binder can observe a newer catalog
            // than the first version read. Monotonic versions bracket the
            // actual metadata read, so failed binding or drift leaves no proof.
            if let Some(version) = version
                && self.statement_catalog_version() == Some(version)
            {
                bound.set_authorization_binding_version(version);
            }
            Ok(bound)
        })
    }
    pub fn schema_in_search_path(&self, schema: &str) -> bool {
        self.search_path.iter().any(|s| s == schema)
    }
    /// Identity of the last parsed input, renewed on a path, user or lexer
    /// context change. Callers can use pointer equality to reuse a rendering.
    pub fn search_path_input(&self) -> Option<&Arc<str>> {
        self.search_path_input.as_ref().map(|(path, _)| path)
    }
    pub fn set_search_path(&mut self, path: &str, user: &str) {
        // The path uses the SQL lexer, so a caller with a different ambient
        // token or value budget must still reparse the same text.
        let budgets = (relational::max_statement_tokens(), relational::max_value_bytes(), relational::wide_values());
        if self.schema_user == user && self.search_path_input.as_ref()
            .is_some_and(|(raw, applied)| **raw == *path && *applied == budgets)
        {
            return;
        }
        let search_path = relational::schema_search_path(path, user);
        if self.schema_user != user || self.search_path != search_path {
            self.schema_context_revision = self.schema_context_revision.and_then(|n| n.checked_add(1));
        }
        self.schema_user = user.into();
        self.search_path = search_path;
        self.search_path_input = Some((Arc::from(path), budgets));
    }
    pub fn schema_metadata(&self) -> Result<relational::SchemaMetadata, CoreError> {
        let read = |s: &MetadataState<'_>| Ok(s.sql.schema_metadata());
        match &self.staged {
            Some(s) => read(&MetadataState::borrow(s)),
            None => self.inspect_metadata(read),
        }
    }
    pub fn relation_schema(&self, name: &str) -> Result<String, CoreError> {
        let read = |s: &MetadataState<'_>| {
            Ok(s.sql
                .tables
                .get(&s.sql.resolve_relation_key(name, &self.search_path))
                .map_or("public", |t| s.sql.table_schema(t.id))
                .to_owned())
        };
        match &self.staged {
            Some(s) => read(&MetadataState::borrow(s)),
            None => self.inspect_metadata(read),
        }
    }
    pub fn creation_schema(&self, p: &relational::Prepared) -> Result<String, CoreError> {
        let read = |s: &MetadataState<'_>| p.creation_schema(&s.sql).map_err(CoreError::Sql);
        match &self.staged {
            Some(s) => read(&MetadataState::borrow(s)),
            None => self.inspect_metadata(read),
        }
    }
    pub fn user_type_catalog(&self) -> Result<relational::UserTypeCatalog, CoreError> {
        if self.aborted {
            return Err(CoreError::Sql(relational::SqlError::Aborted));
        }
        if let Some(s) = &self.staged {
            return Ok(s.sql.user_type_catalog());
        }
        self.inspect_metadata(|s| Ok(s.sql.user_type_catalog()))
    }

    /// Mark publications whose creator authority is maintained by the caller.
    /// Dropped names lose their authority unless the same managed publication
    /// creates a replacement whose authority the caller has installed.
    pub fn set_authority_managed(&mut self, managed: bool) {
        self.authority_managed = managed;
    }

    /// Set the creator for SQL DDL. V2 maintains all ownership records itself.
    /// Legacy maintains explicit sequence owners and leaves table bookkeeping to callers.
    pub fn set_ddl_owner(&mut self, owner: Option<&str>) -> bool {
        self.ddl_owner = owner.map(str::to_owned);
        matches!(self.admin.inner.backend, Backend::V2(_))
    }

    fn staged_sequence_owner(&self, name: &str) -> Option<&Option<String>> {
        let staged = self.staged.as_ref()?;
        staged.sequence_owner_updates.iter().find_map(|(id, owner)| {
            staged.sql.catalog_ext().sequences.get(id)
                .filter(|sequence| sequence.name == name)
                .map(|_| owner)
        })
    }

    /// Read authority including this session's uncommitted DDL changes.
    pub fn table_owner(&self, table: &str) -> Result<Option<String>, roles::RoleError> {
        if let Some(owner) = self.staged_sequence_owner(table) {
            return Ok(owner.clone());
        }
        if let Some(txn) = &self.v2_transaction
            && let Some(owner) = roles::engine::own_owner(txn, table)?
        {
            return Ok(owner);
        }
        let db = self.statement_lease()?;
        self.admin.roles().table_owner_leased(table, db.as_deref())
    }

    /// Read authority from a captured image, including this session's own DDL.
    pub fn table_owner_in(
        &self,
        image: &roles::RoleCatalogImage,
        table: &str,
    ) -> Result<Option<String>, roles::RoleError> {
        if let Some(owner) = self.staged_sequence_owner(table) {
            return Ok(owner.clone());
        }
        if let Some(txn) = &self.v2_transaction
            && let Some(owner) = roles::engine::own_owner(txn, table)?
        {
            return Ok(owner);
        }
        image.table_owner(table)
    }

    /// Read grants including tombstones staged by v2 DROP/name reuse.
    pub fn table_privileges(
        &self,
        table: &str,
        role: &str,
    ) -> Result<std::collections::BTreeSet<roles::TablePrivilege>, roles::RoleError> {
        if let Some(txn) = &self.v2_transaction
            && let Some(grants) = roles::engine::own_grants(txn, table, role)?
        {
            return Ok(grants);
        }
        let db = self.statement_lease()?;
        self.admin.roles().table_privileges_leased(table, role, db.as_deref())
    }

    /// The level of every SQL transaction this session begins from now on:
    /// BEGIN, and a statement outside a transaction block (GD-0075 R5-76).
    pub fn set_isolation(&mut self, level: SqlIsolation) {
        self.isolation = level;
    }
    /// Re-levels the open transaction (SET TRANSACTION ISOLATION LEVEL). It
    /// must not have run a statement yet; callers reject that case first
    /// (25001). Without an open v2 transaction there is nothing to change.
    pub fn set_transaction_isolation(&mut self, level: SqlIsolation) -> Result<(), CoreError> {
        match &mut self.v2_transaction {
            Some(txn) if txn.isolation() != level.engine() => txn
                .set_isolation(level.engine())
                .map_err(|_| CoreError::Sql(relational::SqlError::Transaction)),
            _ => Ok(()),
        }
    }
    /// Set the v2 value and row budget; legacy retains its fixed limits.
    pub fn set_max_value_bytes(&mut self, bytes: usize) {
        let bytes = bytes.clamp(relational::MAX_ROW_BYTES, relational::MAX_VALUE_BYTES);
        if self.max_value_bytes != bytes { self.unnamed_plans.clear(); }
        self.max_value_bytes = bytes;
    }
    /// Preserve both SQL budgets in an alternate catalog preparation session.
    pub fn copy_statement_budgets_to(&self, target: &mut Self) {
        target.set_max_value_bytes(self.max_value_bytes);
        target.set_max_statement_tokens(self.max_statement_tokens);
    }
    /// Set the v2 statement syntax budget; zero is unlimited.
    pub fn set_max_statement_tokens(&mut self, tokens: usize) {
        let tokens = tokens.min(i32::MAX as usize);
        if self.max_statement_tokens != tokens { self.unnamed_plans.clear(); }
        self.max_statement_tokens = tokens;
    }
    /// Apply this session's lexer budgets to synchronous preparation prechecks.
    /// Dropping the guard restores the caller's budgets, including on errors.
    /// Keep the guard on the current thread for the duration of preparation.
    #[must_use]
    pub fn statement_budgets(&self) -> impl Sized + use<> {
        let v2 = matches!(self.admin.inner.backend, Backend::V2(_));
        (
            relational::ValueBudget::enter(self.value_budget(), v2),
            relational::StatementBudget::enter(self.max_statement_tokens, v2),
            // Thread-local budgets must be restored on the entering thread.
            std::marker::PhantomData::<std::rc::Rc<()>>,
        )
    }
    fn value_budget(&self) -> usize {
        #[cfg(feature = "engine-rocksdb")]
        if matches!(self.admin.inner.backend, Backend::V2(_)) { return self.max_value_bytes; }
        relational::MAX_ROW_BYTES
    }
    /// Set the precision used by SQL floating-point text conversions.
    pub fn set_extra_float_digits(&mut self, value: i32) {
        self.extra_float_digits = value;
    }
    pub fn set_text_settings(
        &mut self,
        date_style: giggles_pg_types::temporal::DateStyleSettings,
        bytea_output: giggles_pg_types::bytea::ByteaOutput,
    ) {
        self.date_style = date_style;
        self.bytea_output = bytea_output;
    }

    /// Tables cleared by TRUNCATE, including its transitive CASCADE closure.
    pub fn truncate_tables(&self, p: &relational::Prepared) -> Result<Vec<String>, CoreError> {
        let targets = |s: &MetadataState<'_>| {
            p.bind_schemas(&s.sql)
                .and_then(|p| p.truncate_authority_tables(&s.sql))
                .map_err(CoreError::Sql)
        };
        match &self.staged {
            Some(s) => targets(&MetadataState::borrow(s)),
            None => self.inspect_metadata(targets),
        }
    }
    /// Report default and identity sequence dependencies without resolving names.
    pub fn implicit_sequence_dependencies(
        &self,
        p: &relational::Prepared,
    ) -> Result<Vec<Option<String>>, CoreError> {
        let dependencies = |s: &MetadataState<'_>| p.implicit_sequences(&s.sql).map_err(CoreError::Sql);
        match &self.staged {
            Some(s) => dependencies(&MetadataState::borrow(s)),
            None => self.inspect_metadata(dependencies),
        }
    }
    pub fn set_sequence_tables(&mut self, tables: Option<std::collections::BTreeSet<String>>) {
        self.sequence_tables = tables;
    }
    pub fn sequence_tables(&self) -> &Option<std::collections::BTreeSet<String>> {
        &self.sequence_tables
    }
    /// The principal V2 execution checks nextval/currval/setval against
    /// (GD-0084 Q24). The server sets it from the effective role before each
    /// authorized request; a session nobody configures stays unrestricted.
    pub fn set_sequence_authority(&mut self, authority: SequenceAuthority) {
        self.sequence_authority = authority;
    }
    pub fn sequence_authority(&self) -> &SequenceAuthority {
        &self.sequence_authority
    }
    pub fn normalize_sequence_reference(&self, text: &str) -> Result<String, CoreError> {
        let name = sequences::sequence_name(text).map_err(CoreError::Sql)?;
        let resolve = |s: &MetadataState<'_>| Ok(s.sql.resolve_relation_key(&name, &self.search_path));
        match &self.staged {
            Some(s) => resolve(&MetadataState::borrow(s)),
            None => self.inspect_metadata(resolve),
        }
    }
    /// Sequence names and owning tables for authorization, without evaluating SQL.
    pub fn sequence_owners(&self) -> Result<Vec<(String, Option<String>)>, CoreError> {
        Ok(self.sequence_owners_shared()?.to_vec())
    }

    /// Ordered ownership metadata shared by all sessions reading the same committed
    /// V2 catalog snapshot. Private/unsupported contexts always build fresh.
    pub fn sequence_owners_shared(&self) -> Result<Arc<SequenceOwners>, CoreError> {
        let cacheable = self.staged.is_none() && !self.aborted && self.task.is_none()
            && matches!(&self.admin.inner.backend, Backend::V2(slot) if !slot.is_cluster())
            && self.v2_transaction.as_ref().is_none_or(|txn|
                txn.catalog_writes().next().is_none() && txn.snapshot_error().is_none());
        let owners = |s: &MetadataState<'_>| {
            let build = || {
                let mut tables = std::collections::HashMap::with_capacity(s.sql.tables.len());
                for table in s.sql.tables.values() {
                    // Preserve the first match used by the previous linear search.
                    tables.entry(table.id).or_insert(table);
                }
                s.sql
                    .catalog_ext()
                    .sequences
                    .values()
                    .map(|d| {
                        let owner = d.owned_by.and_then(|(id, _)| {
                            tables.get(&id).map(|t| s.sql.table_key(t))
                        });
                        (d.name.clone(), owner)
                    })
                    .collect::<Vec<_>>().into()
            };
            // The snapshot is supplied by the SAME metadata read as the image.
            // No separate current-version read can race with concurrent DDL.
            // table_key uses only image schema bindings, not comments/views overlays.
            Ok(if cacheable && let Some(catalog) = s.catalog {
                catalog.sequence_owners.get_or_init(build).clone()
            } else {
                build()
            })
        };
        match &self.staged {
            Some(s) => owners(&MetadataState::borrow(s)),
            None => self.inspect_metadata(owners),
        }
    }
    /// Resolve metadata only. Never executes, stages, charges or publishes SQL.
    pub fn describe(
        &mut self,
        p: &relational::Prepared,
    ) -> Result<relational::Description, CoreError> {
        relational::assert_sql_frontend_allowed();
        let lease = self.statement_lease()?;
        self.describe_bound_engine(p, lease.as_deref())
    }
    fn describe_bound_engine(
        &mut self,
        p: &relational::Prepared,
        db: Option<&EngineLease>,
    ) -> Result<relational::Description, CoreError> {
        Self::check_prepared_generation(p, db)?;
        let _budget = relational::ValueBudget::enter(self.value_budget(), matches!(self.admin.inner.backend, Backend::V2(_)));
        let _statement_budget = relational::StatementBudget::enter(self.max_statement_tokens, matches!(self.admin.inner.backend, Backend::V2(_)));
        let result = (|| {
            if self.aborted && !p.allows_aborted_transaction() {
                return Err(CoreError::Sql(relational::SqlError::Aborted));
            }
            if p.transaction.is_some() && p.transaction != self.transaction_id {
                return Err(CoreError::Sql(relational::SqlError::StaleSchema));
            }
            let describe = |s: &MetadataState<'_>| {
                if p.domain != s.view.domain {
                    return Err(CoreError::Sql(relational::SqlError::StaleSchema));
                }
                let mut metadata_plan = p.clone();
                if matches!(self.admin.inner.backend, Backend::V2(_)) {
                    metadata_plan.epoch = s.sql.image.epoch;
                }
                metadata_plan.describe(&s.sql).map_err(CoreError::Sql)
            };
            if let Some(s) = &self.staged {
                describe(&MetadataState::borrow(s))
            } else {
                self.inspect_metadata_leased(db, describe)
            }
        })();
        if result.is_err() {
            self.abort();
        }
        result
    }
    /// Inputs to successful immutable-constant validation, excluding the Prepared
    /// itself. A private catalog or unsupported backend never has a reusable key.
    pub fn constants_validation_key(&self) -> Option<(u64, usize, usize, u64)> {
        if self.staged.is_some() { return None; }
        Some((self.statement_catalog_version()?, self.value_budget(), self.max_statement_tokens, self.schema_context_revision?))
    }

    /// The latest role catalog, the statement's catalog, and the schema context.
    /// A catalog publication or private metadata requires ordinary authorization.
    pub fn authorization_catalog_key(&self, role_admin: &Admin) -> Option<(u64, u64, u64)> {
        if !Arc::ptr_eq(&self.admin.inner, &role_admin.inner)
            || self.admin.inner.fenced.load(Ordering::Acquire)
            || self.staged.is_some() || self.protected.is_some() || self.copy_input.is_some()
        {
            return None;
        }
        let Backend::V2(slot) = &self.admin.inner.backend else { return None; };
        let (latest, statement) = slot.authorization_versions(|stamp, catalog| {
            self.statement_catalog_version_with(|lease| {
                if lease.is_some_and(|db| db.stamp() != stamp) {
                    return None;
                }
                Some(catalog.read())
            })
        })?;
        Some((latest, statement, self.schema_context_revision?))
    }

    /// Recheck metadata admission, provenance and snapshot guards. Stored
    /// defaults may also need parsing on a different pgwire worker thread;
    /// a prior success may have used that thread's cached syntax.
    pub fn authorization_memo_preflight(&self, p: &relational::Prepared) -> Result<bool, CoreError> {
        self.inspect_metadata(|s| {
            // On an error in this later check the caller runs the original
            // sequence and relation checks before reaching the error again.
            Ok(!p.authorization_may_parse_defaults() || p.implicit_sequences(&s.sql).is_ok())
        })
    }

    /// Recheck the full validation path's lifetime/domain guards on a memo hit.
    /// Inspecting metadata also preserves admission and snapshot failures.
    pub fn constants_validated_preflight(&mut self, p: &relational::Prepared) -> Result<(), CoreError> {
        let _budgets = self.statement_budgets();
        relational::assert_sql_frontend_allowed();
        let result = (|| {
            if self.aborted && !p.allows_aborted_transaction() {
                return Err(CoreError::Sql(relational::SqlError::Aborted));
            }
            if !self.prepared_is_current(p) {
                return Err(CoreError::Sql(relational::SqlError::StaleSchema));
            }
            let check = |s: &MetadataState<'_>| {
                if p.domain != s.view.domain {
                    return Err(CoreError::Sql(relational::SqlError::StaleSchema));
                }
                Ok(())
            };
            if let Some(s) = &self.staged { check(&MetadataState::borrow(s)) }
            else { self.inspect_metadata(check) }
        })();
        if result.is_err() { self.abort(); }
        result
    }
    /// Validate dependency-free immutable projection/order constants without executing SQL.
    pub fn validate_immutable_constants(
        &mut self,
        p: &relational::Prepared,
    ) -> Result<(), CoreError> {
        let _budget = relational::ValueBudget::enter(self.value_budget(), matches!(self.admin.inner.backend, Backend::V2(_)));
        let _statement_budget = relational::StatementBudget::enter(self.max_statement_tokens, matches!(self.admin.inner.backend, Backend::V2(_)));
        relational::assert_sql_frontend_allowed();
        let result = (|| {
            if self.aborted && !p.allows_aborted_transaction() {
                return Err(CoreError::Sql(relational::SqlError::Aborted));
            }
            if p.transaction.is_some() && p.transaction != self.transaction_id {
                return Err(CoreError::Sql(relational::SqlError::StaleSchema));
            }
            let describe = |s: &MetadataState<'_>| {
                if p.domain != s.view.domain {
                    return Err(CoreError::Sql(relational::SqlError::StaleSchema));
                }
                let epoch = if matches!(self.admin.inner.backend, Backend::V2(_)) {
                    s.sql.image.epoch
                } else {
                    p.epoch
                };
                p.validate_immutable_constants_at_epoch(&s.sql, epoch)
                    .map_err(CoreError::Sql)
            };
            if let Some(s) = &self.staged {
                describe(&MetadataState::borrow(s))
            } else {
                self.inspect_metadata(describe)
            }
        })();
        if result.is_err() {
            self.abort();
        }
        result
    }
    pub(crate) fn settle_sequences(&self) -> Result<(), CoreError> {
        if self.sequence_unsettled.lock().map_err(|_| CoreError::Fenced)?.is_empty() {
            return Ok(());
        }
        // Settlement uses the generation that produced the obligations.
        let generation = self.sequence_generation.as_ref().ok_or(CoreError::Fenced)?;
        let db = generation.lease(Holder::Sequence)?;
        v2_sql::sql_sequences::settle(&db, &self.sequence_unsettled)?;
        Ok(())
    }

    /// A gateway parse/bind/authorization failure poisons an explicit transaction.
    pub fn abort(&mut self) {
        // Void cleanup retains obligations on failure; fallible exits retry settlement.
        let _ = self.settle_sequences();
        self.abort_transaction(true);
    }

    fn abort_transaction(&mut self, settle_on_copy_rollback: bool) {
        self.abort_copy_input(settle_on_copy_rollback);
        if self.in_transaction() {
            self.aborted = true;
        }
    }
    /// A row exit with no twin: refused in a task session (GD-0061 R3-301).
    pub fn scan(&mut self, scan: relational::Scan) -> Result<relational::SqlResult, CoreError> {
        self.refuse_task_session()?;
        self.scan_engine(scan)
    }
    /// `scan` for crate-internal steps (complete native inputs, claims), whose
    /// own exits carry the release tag.
    pub(crate) fn scan_engine(&mut self, scan: relational::Scan) -> Result<relational::SqlResult, CoreError> {
        #[cfg(feature = "engine-rocksdb")]
        if self.is_v2_engine() {
            let db = self.v2_lease(Holder::Sql)?;
            let result = self.scan_v2_engine(&db, scan);
            if result.is_err() { self.abort(); }
            return result;
        }
        self.admin.inner.db(SQL_SESSIONS)?;
        self.execute_engine(
            |domain, state| relational::Prepared::scan(scan, domain, state),
            None,
        )
    }
    /// Typed engine operations share the SQL admission and transaction path.
    fn execute_engine(
        &mut self,
        make: impl FnOnce(
            u128,
            &relational::Relational,
        ) -> Result<relational::Prepared, relational::SqlError>,
        expected_version: Option<u64>,
    ) -> Result<relational::SqlResult, CoreError> {
        let _budget = relational::ValueBudget::enter(self.value_budget(), matches!(self.admin.inner.backend, Backend::V2(_)));
        let _statement_budget = relational::StatementBudget::enter(self.max_statement_tokens, matches!(self.admin.inner.backend, Backend::V2(_)));
        self.admin.inner.db(SQL_SESSIONS)?;
        if self.aborted {
            return Err(CoreError::Sql(relational::SqlError::Aborted));
        }
        let result = (|| {
            let mut prepared = if let Some(s) = &self.staged {
                make(s.view.domain, &s.sql).map_err(CoreError::Sql)?
            } else {
                self.admin
                    .inner
                    .inspect(|s| make(s.view.domain, &s.sql).map_err(CoreError::Sql))?
            };
            prepared.transaction = self.transaction_id;
            self.execute_bound_engine(&prepared, &[], expected_version)
        })();
        if result.is_err() {
            self.abort();
        }
        result
    }
    #[cfg(all(test, feature = "engine-rocksdb"))]
    pub(crate) fn v2_max_commit_group_size(&self) -> usize {
        match &self.admin.inner.backend {
            Backend::V2(_) => self.admin.test_v2().sequencer.max_group_size(),
            _ => panic!("v2 sequencer required"),
        }
    }

    pub(crate) fn begin_native_implicit(&mut self) -> Result<(), CoreError> {
        self.native_implicit_control(true, Holder::Sql)
    }

    pub(crate) fn commit_native_implicit(&mut self) -> Result<(), CoreError> {
        self.native_implicit_control(false, Holder::Sql)
    }

    fn native_implicit_control(&mut self, begin: bool, kind: Holder) -> Result<(), CoreError> {
        let inner = self.admin.inner.clone();
        if !self.is_v2_engine() {
            return if begin { self.begin_engine() } else { self.commit_engine() };
        }
        let db = self.v2_lease(kind)?;
        self.bind_sequence_generation(&db);
        // These are the Control checks from execute_v2. No external Prepared,
        // parameters, version or schema context can be stale on this path.
        if self.copy_input.as_ref().is_some_and(|input| !input.executing) {
            self.abort();
            return Err(CoreError::Sql(relational::SqlError::Transaction));
        }
        if self.aborted { return Err(CoreError::Sql(relational::SqlError::Aborted)); }
        let mut ticket = match inner.enter_sql() {
            Ok(ticket) => ticket,
            Err(error) => { self.abort(); return Err(error); }
        };
        let result = crate::cancellation::check().and_then(|()| {
            if begin {
                if self.v2_transaction.is_some() { return Err(CoreError::Sql(relational::SqlError::Transaction)); }
                self.begin_v2_transaction_owned(db.into_holder(kind)?)?;
                let db = self.v2_transaction.as_ref().and_then(Transaction::engine_lifetime)
                    .and_then(|lifetime| lifetime.lease.as_ref()).expect("newly leased transaction");
                v2_sql::sql_sequences::settle(db, &self.sequence_unsettled)?;
                self.staged_revision = 0;
                Ok(())
            } else {
                self.commit_v2_transaction(&db, &mut ticket)
            }
        });

        if result.is_err() { self.abort(); }
        if self.v2_transaction.is_none() {
            self.xact.lock().unwrap_or_else(|e| e.into_inner()).take();
            self.transaction_clock_us = None;
        }
        result
    }

    pub(crate) fn begin_engine(&mut self) -> Result<(), CoreError> {
        if self.is_v2_engine() {
            let db = self.v2_lease(Holder::Sql)?;
            let prepared =
                relational::Prepared::begin_engine(u128::from_be_bytes(db.meta.domain), 0);
            return self.execute_bound_kernel(&prepared, &[], None, None, Some(&db), None).map(|_| ());
        }
        self.admin.inner.db(SQL_SESSIONS)?;
        self.execute_engine(
            |domain, state| Ok(relational::Prepared::begin_engine(domain, state.epoch)),
            None,
        )
        .map(|_| ())
    }
    pub(crate) fn commit_engine(&mut self) -> Result<(), CoreError> {
        if self.is_v2_engine() {
            let db = self.v2_lease(Holder::Sql)?;
            let prepared =
                relational::Prepared::commit_engine(u128::from_be_bytes(db.meta.domain), 0);
            return self.execute_bound_kernel(&prepared, &[], None, None, Some(&db), None).map(|_| ());
        }
        self.admin.inner.db(SQL_SESSIONS)?;
        self.execute_engine(
            |domain, state| Ok(relational::Prepared::commit_engine(domain, state.epoch)),
            None,
        )
        .map(|_| ())
    }
    pub(crate) fn is_v2_engine(&self) -> bool {
        matches!(self.admin.inner.backend, Backend::V2(_))
    }
    pub(crate) fn rollback_v2_engine(&mut self) -> Result<(), CoreError> {
        let db = match self.v2_lease(Holder::Sql) {
            Ok(db) => db,
            Err(CoreError::Fenced) => { self.rollback(); return Ok(()); }
            Err(error) => return Err(error),
        };
        let prepared = relational::Prepared::rollback_engine(u128::from_be_bytes(db.meta.domain), 0);
        self.execute_bound_kernel(&prepared, &[], None, None, Some(&db), None).map(|_| ())
    }
    pub(crate) fn named_control_engine(
        &mut self,
        name: &str,
        make: impl FnOnce(&str, u128, u64) -> Result<relational::Prepared, relational::SqlError>,
    ) -> Result<(), CoreError> {
        // Do not use execute_engine: its aborted-state guard blocks recovery.
        let lease = self.statement_lease()?;
        let (domain, epoch) = if let Some(db) = lease.as_deref() {
            (u128::from_be_bytes(db.meta.domain), 0)
        } else {
            self.admin.inner.db(SQL_SESSIONS)?;
            match &self.staged {
                Some(s) => (s.view.domain, s.sql.epoch),
                None => self.admin.inner.inspect(|s| Ok((s.view.domain, s.sql.epoch)))?,
            }
        };
        let mut prepared = make(name, domain, epoch).map_err(CoreError::Sql)?;
        prepared.transaction = self.transaction_id;
        self.execute_bound_kernel(&prepared, &[], None, None, lease.as_deref(), None).map(|_| ())
    }
    /// Native block steps are separate RC statements; legacy keeps its block snapshot.
    pub(crate) fn begin_native_step(&mut self) -> Result<(), CoreError> {
        crate::cancellation::check()?;
        #[cfg(feature = "engine-rocksdb")]
        if self.is_v2_engine() {
            let _lease = self.v2_lease(Holder::Sql)?;
            self.v2_transaction.as_mut()
                .ok_or(CoreError::Sql(relational::SqlError::Transaction))?
                .begin_statement();
        }
        Ok(())
    }
    pub(crate) fn catalog_engine(&mut self) -> Result<(u64, Vec<relational::TableDef>), CoreError> {
        self.inspect_catalog_engine(|fence, tables| (fence, tables.values().cloned().collect()))
    }

    /// Catalog binding sees exactly the staged snapshot used for typed reads/writes.
    pub(crate) fn inspect_catalog_engine<T>(
        &mut self,
        inspect: impl FnOnce(u64, &BTreeMap<String, relational::TableDef>) -> T,
    ) -> Result<T, CoreError> {
        let result = (|| {
            if self.aborted {
                return Err(CoreError::Sql(relational::SqlError::Aborted));
            }
            crate::cancellation::check()?;
            if self.is_v2_engine() {
                let db = self.v2_lease(Holder::Sql)?;
                let _ticket = self.admin.inner.enter()?;
                return db.inspect_binding(self.v2_transaction.as_ref(), |_, fence, tables| {
                    inspect(fence, tables)
                });
            }
            let catalog = |s: &State| Ok(inspect(s.sql.epoch, &s.sql.tables));
            if let Some(s) = &self.staged {
                catalog(s)
            } else {
                self.admin.inner.inspect(catalog)
            }
        })();
        if result.is_err() {
            self.abort();
        }
        result
    }
    /// Resolve native requests in the same private snapshot/search path as SQL.
    pub(crate) fn native_table_engine(&self, name: &str) -> Result<relational::TableDef, CoreError> {
        self.inspect_metadata(|s| {
            let key = s.sql.resolve_relation_key(name, &self.search_path);
            s.sql.tables.get(&key).cloned()
                .ok_or(CoreError::Sql(relational::SqlError::UnknownTable))
        })
    }
    /// A native read target that names a view rather than a table, as SQL
    /// resolves FROM names: its descriptor, or None when `name` is no view.
    pub(crate) fn native_view_engine(&self, name: &str) -> Result<Option<relational::TableDef>, CoreError> {
        self.inspect_metadata(|s| {
            let key = s.sql.resolve_relation_key(name, &self.search_path);
            if s.sql.tables.contains_key(&key) { return Ok(None); }
            let Some(&(id, _)) = s.sql.view_names.get(&key) else { return Ok(None) };
            Ok(s.sql.views.and_then(|views| views.get(&id)).map(|v| view_table(id, v)))
        })
    }
    /// Rows of a current view through SQL's own expansion (`SELECT * FROM`),
    /// in this transaction's snapshot. None when `view` is not a current view.
    /// Legacy input keeps its bounded materialization and row ceiling.
    pub(crate) fn native_view_rows_engine(&mut self, view: &relational::TableDef)
        -> Result<Option<Vec<relational::SqlRow>>, CoreError> {
        let Some(key) = self.native_view_key(view)? else { return Ok(None) };
        let lease = self.statement_lease()?;
        let p = self.prepare_typed_read(lease.as_deref(), vec![key.clone()], |domain, epoch|
            relational::Prepared::from_relation_read(domain, epoch, key))?;
        let rows = relational::spill::bounded(|| self.execute_bound_kernel(&p, &[], None, None, lease.as_deref(), None))?.rows;
        if rows.len() > relational::MAX_ROWS { return Err(CoreError::Limit); }
        Ok(Some(rows))
    }
    /// V2 view input captures its SQL spill locally, then filters and retains
    /// only the native read's top bound. No raw view spill escapes to an API caller.
    pub(crate) fn native_view_rows_where<E, C>(
        &mut self,
        view: &relational::TableDef,
        keep: &mut impl FnMut(&relational::SqlRow) -> Result<bool, E>,
        bound: (usize, Option<C>),
        map: fn(CoreError) -> E,
    ) -> Result<Option<Vec<relational::SqlRow>>, E>
    where
        C: Fn(&relational::SqlRow, &relational::SqlRow) -> std::cmp::Ordering + Copy,
    {
        let result = (|| {
            if !self.is_v2_engine() {
                return Err(map(CoreError::Sql(relational::SqlError::Unsupported)));
            }
            let Some(key) = self.native_view_key(view).map_err(map)? else { return Ok(None) };
            let lease = self.statement_lease().map_err(map)?;
            let p = self.prepare_typed_read(lease.as_deref(), vec![key.clone()], |domain, epoch|
                relational::Prepared::from_relation_read(domain, epoch, key)).map_err(map)?;
            let (result, spilled) = relational::spill::streaming(|| self.execute_bound_kernel(&p, &[], None, None, lease.as_deref(), None));
            let result = result.map_err(map)?;
            complete_relation::retain_native_rows(keep, bound, map, |visit| {
                if let Some(spilled) = spilled {
                    for row in spilled {
                        visit(&row.map_err(map)?)?;
                    }
                } else {
                    for row in &result.rows { visit(row)?; }
                }
                Ok(())
            }).map(Some)
        })();
        if result.is_err() { self.abort(); }
        result
    }
    /// Whether `view` still describes a current view of this snapshot.
    pub(crate) fn native_view_engine_current(&self, view: &relational::TableDef) -> Result<bool, CoreError> {
        Ok(self.native_view_key(view)?.is_some())
    }
    // The schema-qualified key of the current view `view` describes exactly.
    fn native_view_key(&self, view: &relational::TableDef) -> Result<Option<String>, CoreError> {
        self.inspect_metadata(|s| {
            let current = s.sql.views.and_then(|views| views.get(&view.id)).map(|v| view_table(view.id, v));
            if current.as_ref() != Some(view) { return Ok(None); }
            Ok(s.sql.view_names.iter().find(|(_, (id, _))| *id == view.id).map(|(key, _)| {
                let (schema, name) = relational::split_key(key);
                format!("{schema}\0{name}")
            }))
        })
    }
    pub(crate) fn native_creation_key_engine(&self, name: &str) -> Result<String, CoreError> {
        self.inspect_metadata(|s| {
            let schemas = s.sql.schema_metadata();
            let (schema, table) = relational::split_key(name);
            let schema = if name.contains('\0') { Some(schema) } else {
                self.search_path.iter().map(String::as_str)
                    .find(|s| *s == "public" || schemas.schemas.contains_key(*s))
            }.ok_or(CoreError::Sql(relational::SqlError::InvalidSchema))?;
            if schema != "public" && !schemas.schemas.contains_key(schema) {
                return Err(CoreError::Sql(relational::SqlError::InvalidSchema));
            }
            Ok(relational::relation_key(schema, table))
        })
    }
    /// Held v2 table locks as (transaction identity, table ID, PostgreSQL mode, fastpath).
    /// Legacy sessions have no lock manager and return no rows.
    pub fn held_table_locks(&self) -> Vec<(u64, u64, &'static str, bool)> {
        self.v2_transaction.as_ref().map_or_else(Vec::new, |txn| {
            txn.held_table_locks().into_iter().map(|(table, mode, fastpath)| (txn.id().0, table, mode, fastpath)).collect()
        })
    }
    /// Committed authority/catalog observation to take before Bind authorization.
    pub fn prepared_bind_observation(&self) -> Result<Option<u128>, CoreError> {
        match &self.admin.inner.backend {
            Backend::V2(_) => self.v2_lease(Holder::Sql)?.catalog().committed_observation().map(Some),
            _ => Ok(None),
        }
    }
    /// Hold prepared dependencies (including views' physical sources) through transaction end.
    /// The caller must observe committed state before authorizing each attempt.
    /// `false` requires re-authorization before expanding or locking dependencies.
    /// Errors do not abort here, so the caller can preserve error ordering.
    pub fn lock_prepared_relations(&mut self, p: &relational::Prepared, observation: Option<u128>) -> Result<bool, CoreError> {
        if !self.is_v2_engine() { return Ok(true); }
        let db = self.v2_lease(Holder::Sql)?;
        Self::check_prepared_generation(p, Some(&db))?;
        let mut scoped = p.clone();
        scoped.set_schema_context(self.search_path.clone(), self.schema_user.clone());
        let txn = self.v2_transaction.as_mut()
            .ok_or(CoreError::Sql(relational::SqlError::Transaction))?;
        let snapshot = txn.snapshot_ts();
        if let Some(error) = txn.snapshot_error() { return Err(error.clone().into()); }
        let pinned = txn.isolation() != engine::txn::Isolation::ReadCommitted;
        self.admin.inner.decision_point(DecisionPoint::PreparedBindAuthorized);
        // Bind and expand from one image of the portal's statement snapshot.
        // Runtime view projections already contain nested physical dependencies.
        let locks = self.inspect_metadata_leased(Some(&db), |state| {
            let bound = scoped.bind_schemas(&state.sql).map_err(CoreError::Sql)?;
            let mut pending = bound.authorization_tables().map_err(CoreError::Sql)?;
            pending.extend(bound.write_tables().into_iter().map(|(name, _)| name));
            let mut seen = std::collections::BTreeSet::new();
            let mut locks = BTreeMap::new();
            while let Some(name) = pending.pop() {
                if !seen.insert(name.clone()) { continue; }
                if let Some(table) = state.sql.tables.get(&name) {
                    locks.insert(table.id, (name, false));
                } else if let Some((id, projection)) = state.sql.view_names.get(&name) {
                    let projection = projection.as_ref().map_err(|error| CoreError::Sql(error.clone()))?;
                    locks.insert(*id, (name, true));
                    pending.extend(projection.reads.iter().cloned());
                } else {
                    return Err(CoreError::Sql(relational::SqlError::UnknownTable));
                }
            }
            Ok(locks)
        });
        let txn = self.v2_transaction.as_mut().expect("open transaction");
        if observation != Some(db.catalog().committed_observation()?) {
            if !pinned { txn.begin_statement(); }
            return Ok(false);
        }
        let locks = locks?;
        // One id order includes views and tables, matching ordinary SQL locking.
        for (id, (name, view)) in locks {
            if txn.current_snapshot_ts() != Some(snapshot)
                || observation != Some(db.catalog().committed_observation()?) {
                if !pinned { txn.begin_statement(); }
                return Ok(false);
            }
            let result = if view {
                txn.acquire_lock(engine::locks::LockKey::Table(id),
                    engine::locks::LockMode::AccessShare, None,
                    &|| crate::cancellation::check().is_err()).map(|()| true).map_err(CoreError::from)
            } else {
                match db.catalog().resolve_for_bind(txn, &name,
                    &|| crate::cancellation::check().is_err()) {
                    Ok(entry) => Ok(entry.is_some()),
                    Err(engine::catalog::CatalogError::Core(error)) => Err(error),
                    Err(engine::catalog::CatalogError::Txn(error)) => Err(error.into()),
                }
            };
            // A wait can change grants/identities even when locking failed.
            // Re-authorize before surfacing that error or locking another dependency.
            if txn.current_snapshot_ts() != Some(snapshot)
                || observation != Some(db.catalog().committed_observation()?) {
                if !pinned { txn.begin_statement(); }
                return Ok(false);
            }
            if !result? { return Ok(false); }
        }
        if txn.current_snapshot_ts() == Some(snapshot)
            && observation == Some(db.catalog().committed_observation()?) {
            return Ok(true);
        }
        // A READ COMMITTED wait may have observed new view definitions or
        // table identities. Expand again before the portal can be published.
        if !pinned { txn.begin_statement(); }
        Ok(false)
    }

    /// Acquire explicit table locks in list order on the open v2 transaction.
    /// Legacy sessions retain their historical validated no-op behaviour.
    pub fn lock_tables(
        &mut self,
        tables: &[String],
        mode: SqlTableLockMode,
        nowait: bool,
    ) -> Result<(), CoreError> {
        if !self.is_v2_engine() {
            return Ok(());
        }
        let result = (|| {
            if self.aborted {
                return Err(CoreError::Sql(relational::SqlError::Aborted));
            }
            let db = self.v2_lease(Holder::Sql)?;
            let names = tables
                .iter()
                .map(|name| {
                    let prepared = self.prepare_leased(&format!("SELECT * FROM {name} LIMIT 0"), Some(&db))?;
                    prepared
                        .table()
                        .map(str::to_owned)
                        .ok_or(CoreError::Sql(relational::SqlError::UnknownTable))
                })
                .collect::<Result<Vec<_>, CoreError>>()?;
            let txn = self
                .v2_transaction
                .as_mut()
                .ok_or(CoreError::Sql(relational::SqlError::Transaction))?;
            txn.begin_statement();
            // PostgreSQL's LOCK takes no snapshot: a REPEATABLE READ or
            // SERIALIZABLE transaction snapshots at its first query after the
            // lock is granted (GD-0075 R5-76).
            let first = txn.current_snapshot_ts().is_none();
            let mode = match mode {
                SqlTableLockMode::AccessShare => engine::locks::LockMode::AccessShare,
                SqlTableLockMode::RowShare => engine::locks::LockMode::RowShare,
                SqlTableLockMode::RowExclusive => engine::locks::LockMode::RowExclusive,
                SqlTableLockMode::ShareUpdateExclusive => {
                    engine::locks::LockMode::ShareUpdateExclusive
                }
                SqlTableLockMode::Share => engine::locks::LockMode::Share,
                SqlTableLockMode::ShareRowExclusive => engine::locks::LockMode::ShareRowExclusive,
                SqlTableLockMode::ExclusiveTable => engine::locks::LockMode::ExclusiveTable,
                SqlTableLockMode::AccessExclusive => engine::locks::LockMode::AccessExclusive,
            };
            let deadline = nowait.then(std::time::Instant::now);
            for name in names {
                db.catalog()
                    .resolve(txn, &name, mode, deadline, &|| {
                        crate::cancellation::check().is_err() || db.check().is_err()
                    })
                    .map_err(|error| match error {
                        engine::catalog::CatalogError::Core(error) => error,
                        engine::catalog::CatalogError::Txn(engine::txn::TxnError::LockTimeout) => {
                            CoreError::LockNotAvailable
                        }
                        engine::catalog::CatalogError::Txn(error) => error.into(),
                    })?;
            }
            if first {
                txn.forget_unused_snapshot();
            }
            Ok(())
        })();
        if result.is_err() {
            self.abort();
        }
        result
    }

    /// Eligibility from the active definition snapshot. Legacy requires empty
    /// business/task state; v2 uses SQL's synchronous populated-database DDL.
    pub(crate) fn catalog_empty_engine(&mut self) -> Result<bool, CoreError> {
        let result = (|| {
            if self.aborted {
                return Err(CoreError::Sql(relational::SqlError::Aborted));
            }
            if self.is_v2_engine() {
                // SQL v2 DDL is synchronous even on populated databases. Hold its
                // catalog barrier through the native commit, and reject drift
                // while waiting rather than silently rebinding a frozen request.
                let before = self.snapshot_identity_engine()?;
                let txn = self.v2_transaction.as_mut()
                    .ok_or(CoreError::Sql(relational::SqlError::Transaction))?;
                txn.acquire_lock(
                    engine::locks::LockKey::Table(0),
                    engine::locks::LockMode::AccessExclusive,
                    None,
                    &|| crate::cancellation::check().is_err(),
                )?;
                txn.begin_statement();
                if self.snapshot_identity_engine()? != before {
                    return Err(CoreError::Sql(relational::SqlError::StaleSchema));
                }
                return Ok(true);
            }
            self.admin.inner.db(SQL_SESSIONS)?;
            let staged = self
                .staged
                .as_ref()
                .ok_or(CoreError::Sql(relational::SqlError::Transaction))?;
            crate::cancellation::check()?;
            Ok(staged.sql.rows_empty_engine()
                && staged.native_outcomes.is_none()
                && staged.view.rows.is_empty()
                && staged.view.tasks.is_empty()
                && staged.view.operations.is_empty()
                && staged.view.approvals.is_empty()
                && staged.accounting.is_empty())
        })();
        if result.is_err() {
            self.abort();
        }
        result
    }
    #[cfg(all(test, feature = "engine-rocksdb"))]
    pub(crate) fn create_table_engine(
        &mut self,
        name: String,
        columns: Vec<relational::ColumnDef>,
        keys: Vec<String>,
    ) -> Result<(), CoreError> {
        let len = columns.len();
        self.create_table_defaults_engine(name, columns, keys, vec![None; len], vec![None; len])
    }
    pub(crate) fn create_table_defaults_engine(
        &mut self,
        name: String,
        columns: Vec<relational::ColumnDef>,
        keys: Vec<String>,
        defaults: Vec<Option<String>>,
        sequences: Vec<Option<relational::defaults::SequenceSpec>>,
    ) -> Result<(), CoreError> {
        if self.is_v2_engine() {
            let db = self.v2_lease(Holder::Sql)?;
            let domain = u128::from_be_bytes(db.meta.domain);
            let result = (|| {
                self.catalog_empty_engine()?;
                let mut prepared = relational::Prepared::create_defaults_engine(
                    name, columns, keys, defaults, sequences, domain, &relational::Relational::default(),
                ).map_err(CoreError::Sql)?;
                prepared.transaction = self.transaction_id;
                self.execute_bound_engine(&prepared, &[], None).map(|_| ())
            })();
            if result.is_err() { self.abort(); }
            return result;
        }
        self.admin.inner.db(SQL_SESSIONS)?;
        // Do not permit this entry to accidentally autocommit outside the
        // caller's eligibility/definition snapshot and final decision fence.
        self.snapshot_identity_engine()?;
        self.execute_engine(
            |domain, state| relational::Prepared::create_defaults_engine(name, columns, keys, defaults, sequences, domain, state),
            None,
        )
        .map(|_| ())
    }
    /// Typed constraints use SQL's DDL executor and the caller's transaction decision.
    pub(crate) fn constraint_catalog_engine(&mut self, request: &crate::operations::CatalogRequest) -> Result<relational::TableDef, CoreError> {
        let result = (|| {
            let (domain, epoch) = self.snapshot_identity_engine()?;
            let mut prepared = relational::Prepared::constraint_engine(request, domain, if self.is_v2_engine() { 0 } else { epoch }).map_err(CoreError::Sql)?;
            prepared.transaction = self.transaction_id;
            self.execute_bound_engine(&prepared, &[], None)?;
            let (crate::operations::CatalogRequest::AddConstraint { table, .. } | crate::operations::CatalogRequest::DropConstraint { table, .. }) = request else { unreachable!() };
            self.inspect_metadata(|s| {
                let key = s.sql.resolve_relation_key(table, &self.search_path);
                s.sql.tables.get(&key).cloned().ok_or(CoreError::Sql(relational::SqlError::UnknownTable))
            })
        })();
        if result.is_err() { self.abort(); }
        result
    }
    /// Typed drops use SQL's DDL executor and the caller's transaction decision.
    pub(crate) fn drop_catalog_engine(&mut self, name: String, cascade: bool, index: bool) -> Result<(relational::TableDef, Option<u64>), CoreError> {
        let result = (|| {
            let (domain, epoch) = self.snapshot_identity_engine()?;
            // Resolve against the same private catalog/search path as SQL, not
            // presentation metadata (which strips index schema names).
            let (before, index_id, key, table_key) = self.inspect_metadata(|s| {
                let key = if index { s.sql.resolve_index_key(&name, &self.search_path) }
                    else { s.sql.resolve_relation_key(&name, &self.search_path) };
                let descriptor = s.sql.secondary_descriptors_engine().iter().find(|d| index && d.name == key);
                let table = if index {
                    descriptor.and_then(|d| s.sql.tables.values().find(|t| t.id == d.table))
                } else { s.sql.tables.get(&key) };
                let table_key = if index { s.sql.index_table_key(&key) }
                    else { table.map(|t| s.sql.table_key(t)) };
                Ok((table.cloned(), descriptor.map(|d| d.id), key, table_key))
            })?;
            // V2 Prepared uses epoch zero; the outer catalog intent checks the
            // actual snapshot fence before entering this shared SQL path.
            let epoch = if self.is_v2_engine() { 0 } else { epoch };
            let mut prepared = if index {
                Ok(relational::Prepared::drop_index_engine(key, table_key, cascade, domain, epoch))
            } else {
                relational::Prepared::drop_table_engine(name, cascade, domain, epoch)
            }.map_err(CoreError::Sql)?;
            prepared.transaction = self.transaction_id;
            self.execute_bound_engine(&prepared, &[], None)?;
            let mut table = before.ok_or(CoreError::Sql(relational::SqlError::UnknownTable))?;
            if index {
                table = self.catalog_engine()?.1.into_iter().find(|t| t.id == table.id)
                    .ok_or(CoreError::Sql(relational::SqlError::UnknownTable))?;
            }
            Ok((table, index_id))
        })();
        if result.is_err() { self.abort(); }
        result
    }
    /// Typed ALTER shares SQL's execution, rollback and publication path.
    pub(crate) fn alter_table_engine(&mut self, name: String, action: crate::operations::AlterTableAction) -> Result<relational::TableDef, CoreError> {
        let result = (|| {
            let (domain, epoch) = self.snapshot_identity_engine()?;
            let id = self.inspect_metadata(|s| {
                let key = s.sql.resolve_relation_key(&name, &self.search_path);
                Ok(s.sql.tables.get(&key).map(|t| t.id))
            })?;
            let epoch = if self.is_v2_engine() { 0 } else { epoch };
            let mut prepared = relational::Prepared::alter_table_engine(name, action, domain, epoch)
                .map_err(CoreError::Sql)?;
            prepared.transaction = self.transaction_id;
            self.execute_bound_engine(&prepared, &[], None)?;
            self.catalog_engine()?.1.into_iter().find(|t| Some(t.id) == id)
                .ok_or(CoreError::Sql(relational::SqlError::UnknownTable))
        })();
        if result.is_err() { self.abort(); }
        result
    }
    /// Trusted native view DDL; callers authorize the target and read sources first.
    /// Returns a view postimage on create and preimage on drop (no primary key).
    pub fn execute_typed_view(&mut self, name: String, read: Option<relational::ViewRead>, cascade: bool) -> Result<relational::TableDef, CoreError> {
        let result = (|| {
            let lease = self.statement_lease()?;
            let db = lease.as_deref();
            let dropping = read.is_none();
            let search_path = self.search_path.clone();
            let definition = |s: &MetadataState<'_>| {
                let key = s.sql.resolve_relation_key(&name, &search_path);
                let id = s.sql.view_names.get(&key).map(|v| v.0);
                Ok(s.sql.catalog_presentation().map_err(CoreError::Sql)?.views.into_iter()
                    .find(|(view, _)| Some(*view) == id).map(|(_, v)| v.table))
            };
            let (domain, epoch, before) = self.inspect_metadata_leased(db, |s| {
                let epoch = if db.is_some() { 0 } else { s.sql.epoch };
                Ok((s.view.domain, epoch, if dropping { definition(s)? } else { None }))
            })?;
            let mut p = relational::Prepared::from_view_read(domain, epoch, name.clone(), read, cascade).map_err(CoreError::Sql)?;
            p.engine_generation = db.map(EngineLease::prepared_generation);
            p.transaction = self.transaction_id;
            p.set_schema_context(self.search_path.clone(), self.schema_user.clone());
            self.execute_bound_kernel(&p, &[], None, None, db, None)?;
            let table = if dropping { before } else { self.inspect_metadata_leased(db, definition)? };
            table.ok_or(CoreError::Sql(relational::SqlError::UnknownTable))
        })();
        if result.is_err() { self.abort(); }
        result
    }
    /// Identity for binding against an active transaction's catalog snapshot.
    pub(crate) fn snapshot_identity_engine(&mut self) -> Result<(u128, u64), CoreError> {
        let result = (|| {
            if self.aborted {
                return Err(CoreError::Sql(relational::SqlError::Aborted));
            }
            if self.is_v2_engine() {
                let db = self.v2_lease(Holder::Sql)?;
                let _ticket = self.admin.inner.enter()?;
                let txn = self
                    .v2_transaction
                    .as_ref()
                    .ok_or(CoreError::Sql(relational::SqlError::Transaction))?;
                return db.inspect_binding(Some(txn), |domain, fence, _| (domain, fence));
            }
            let staged = self
                .staged
                .as_ref()
                .ok_or(CoreError::Sql(relational::SqlError::Transaction))?;
            crate::cancellation::check()?;
            Ok((staged.view.domain, staged.sql.epoch))
        })();
        if result.is_err() {
            self.abort();
        }
        result
    }
    /// Fetch an actual staged version without cloning the row or transaction state.
    pub(crate) fn row_version_engine(
        &mut self,
        table: &relational::TableDef,
        key: &[relational::Value],
    ) -> Result<u64, CoreError> {

        let result = (|| {
            if self.aborted {
                return Err(CoreError::Sql(relational::SqlError::Aborted));
            }
            if self.is_v2_engine() {
                let db = self.v2_lease(Holder::Sql)?;
                let _ticket = self.admin.inner.enter_sql()?;
                crate::cancellation::check()?;
                let txn = self.v2_transaction.as_mut()
                    .ok_or(CoreError::Sql(relational::SqlError::Transaction))?;
                let catalog = db.catalog().snapshot(txn)?;
                let image = catalog.image().map_err(CoreError::Sql)?;
                // Reuse the legacy definition/key admission before touching rows.
                match image.row_version_engine(table, key) {
                    Err(relational::SqlError::Conflict) => {}
                    Err(error) => return Err(CoreError::Sql(error)),
                    Ok(_) => return Err(CoreError::Corrupt("metadata contains rows".into())),
                }
                let entry = engine::catalog::TableEntry {
                    primary_index_birth: catalog.parts.ext.primary_index_birth.get(&table.id).copied(),
                    def: table.clone(), heap: catalog.parts.heap.contains(&table.id),
                    columns_ext: BTreeMap::new(),
                };
                return db.rows.get(txn, &entry, key)?
                    .map(|row| row.version)
                    .ok_or(CoreError::Sql(relational::SqlError::Conflict));
            }
            self.admin.inner.db(SQL_SESSIONS)?;
            let staged = self
                .staged
                .as_ref()
                .ok_or(CoreError::Sql(relational::SqlError::Transaction))?;
            crate::cancellation::check()?;
            staged
                .sql
                .row_version_engine(table, key)
                .map_err(CoreError::Sql)
        })();
        if result.is_err() {
            self.abort();
        }
        result
    }
    pub(crate) fn coerce_column_engine(
        &self,
        column: &relational::ColumnDef,
        value: relational::Value,
    ) -> Result<relational::Value, CoreError> {
        if self.is_v2_engine() {
            let db = self.v2_lease(Holder::Sql)?;
            let (_, image) = db.native_metadata_image(self.v2_transaction.as_ref())?;
            return relational::coerce_to_column(&image, column, value).map_err(CoreError::Sql);
        }
        let state = self
            .staged
            .as_ref()
            .ok_or(CoreError::Sql(relational::SqlError::Transaction))?;
        relational::coerce_to_column(&state.sql, column, value).map_err(CoreError::Sql)
    }
    fn native_sequence_lease(&mut self) -> Result<Option<SessionLease>, CoreError> {
        let lease = self.statement_lease()?;
        if let Some(db) = lease.as_deref() {
            self.bind_sequence_generation(db);
        }
        Ok(lease)
    }

    /// Share SQL sequence state and callbacks across native entry points.
    fn native_sequence_callback(
        &self, db: Option<&EngineLease>,
    ) -> Result<(Arc<relational::Relational>, relational::clock::SequenceCallback), CoreError> {
        if let Some(db) = db {
            let (_, image) = db.native_metadata_image(self.v2_transaction.as_ref())?;
            let callback = self.insert_sequence_callback(db, image.clone())?;
            return Ok((image, callback));
        }
        let mut state = match &self.staged {
            Some(state) => state.clone(),
            None => self.admin.inner.legacy_statement_snapshot()?,
        };
        self.admin.inner.merge_live_sequences(&mut state)?;
        sequences::merge(&mut state, &*self.pending_sequences.lock().map_err(|_| CoreError::Fenced)?);
        let baseline = if self.staged.is_some() {
            self.sequence_baseline.clone()
        } else {
            state.sql.catalog_ext().sequences.iter().map(|(&id, d)| (id, d.incarnation)).collect()
        };
        let callback = self.admin.inner.sequence_callback(
            Arc::new(state.clone()), baseline, self.pending_sequences.clone(),
            self.sequence_values.clone(), self.sequence_tables.clone(), self.search_path.clone(),
        );
        Ok((Arc::new(state.sql), callback))
    }
    pub(crate) fn sequence_call_engine(&mut self, operation: &str, name: &str, value: Option<i64>, called: bool) -> Result<i64, CoreError> {
        let lease = self.native_sequence_lease()?;
        let _ticket = self.admin.inner.enter_sql()?;
        crate::cancellation::check()?;
        let (state, callback) = self.native_sequence_callback(lease.as_deref())?;
        let quoted = |key: &str| key.split('\0')
            .map(|part| format!("\"{}\"", part.replace('"', "\"\""))).collect::<Vec<_>>().join(".");
        let qualified = if self.is_v2_engine() {
            let key = state.resolve_relation_key(name, &self.search_path);
            if !state.catalog_ext().sequences.values().any(|def| def.name == key) {
                return Err(CoreError::Sql(relational::SqlError::UnknownTable));
            }
            let key = if key.contains('\0') { key } else { format!("public\0{key}") };
            quoted(&key)
        } else { quoted(name) };
        let result = callback(operation, &qualified, value, called, relational::clock::SequenceOrigin::Call);
        // Like a callback error here, a settle error does not abort the transaction.
        self.settle_sequences()?;
        match result? {
            relational::Value::Int(value) => Ok(value),
            _ => Err(CoreError::Sql(relational::SqlError::Type)),
        }
    }
    pub(crate) fn sequence_catalog_engine(&mut self, ddl: relational::defaults::SequenceDdl) -> Result<u64, CoreError> {
        let (domain, epoch) = self.snapshot_identity_engine()?;
        let epoch = if self.is_v2_engine() { 0 } else { epoch };
        let mut prepared = relational::Prepared::sequence_engine(ddl, domain, epoch).map_err(CoreError::Sql)?;
        prepared.transaction = self.transaction_id;
        self.execute_bound_engine(&prepared, &[], None)?;
        Ok(self.catalog_engine()?.0)
    }
    /// Normalize native input using SQL defaults and durable sequence callbacks.
    pub(crate) fn normalize_insert_engine(
        &mut self,
        table: &relational::TableDef,
        supplied: &[usize],
        values: Vec<relational::Value>,
    ) -> Result<Vec<relational::Value>, CoreError> {
        self.normalize_insert_engine_settling(table, supplied, values, true)
    }

    /// Native INSERT MANY only: the caller must settle on every request exit.
    /// Failures still abort and attempt settlement here.
    pub(crate) fn normalize_insert_engine_deferred(
        &mut self,
        table: &relational::TableDef,
        supplied: &[usize],
        values: Vec<relational::Value>,
    ) -> Result<Vec<relational::Value>, CoreError> {
        self.normalize_insert_engine_settling(table, supplied, values, false)
    }

    fn normalize_insert_engine_settling(
        &mut self,
        table: &relational::TableDef,
        supplied: &[usize],
        values: Vec<relational::Value>,
        settle: bool,
    ) -> Result<Vec<relational::Value>, CoreError> {
        let result = (|| {
            let lease = self.native_sequence_lease()?;
            let _ticket = self.admin.inner.enter_sql()?;
            crate::cancellation::check()?;
            if self.aborted {
                return Err(CoreError::Sql(relational::SqlError::Aborted));
            }
            let (state, callback) = self.native_sequence_callback(lease.as_deref())?;
            let clock = relational::clock::ExecutionClock::new(&relational::StatementClock {
                unavailable: Some(CoreError::Sql(relational::SqlError::Unsupported)),
                transaction_us: 0, statement_us: 0, zone: None, wall: None,
            }).with_extra_float_digits(self.extra_float_digits).with_sequences(callback);
            let output = state.normalize_insert_engine(table, supplied, values, &clock);
            if let Some(error) = clock.take_error() { return Err(error); }
            output.map_err(CoreError::Sql)
        })();
        let result = result.and_then(|value| {
            if settle { self.settle_sequences()?; }
            Ok(value)
        });
        if result.is_err() { self.abort(); }
        result
    }
    pub(crate) fn insert_engine(
        &mut self,
        table: &relational::TableDef,
        values: Vec<relational::Value>,
    ) -> Result<relational::SqlResult, CoreError> {
        self.insert_engine_settling(table, values, true)
    }

    /// Native INSERT MANY only: the caller must settle on every request exit.
    /// Failures still abort and attempt settlement here.
    pub(crate) fn insert_many_engine_deferred(
        &mut self, table: &relational::TableDef, rows: Vec<Vec<relational::Value>>,
    ) -> Result<relational::SqlResult, CoreError> {
        self.execute_native_many(|domain, state|
            relational::Prepared::insert_many_normalized_engine(table, rows, domain, state))
    }
    pub(crate) fn delete_many_engine(
        &mut self, table: &relational::TableDef, rows: Vec<(Vec<relational::Value>, u64)>,
    ) -> Result<relational::SqlResult, CoreError> {
        self.execute_native_many(|domain, state|
            relational::Prepared::delete_many_engine(table, rows, domain, state))
    }
    fn execute_native_many(
        &mut self, build: impl FnOnce(u128, &relational::Relational) -> Result<relational::Prepared, relational::SqlError>,
    ) -> Result<relational::SqlResult, CoreError> {
        if self.is_v2_engine() {
            let db = self.v2_lease(Holder::Sql)?;
            let result = (|| {
                if self.aborted { return Err(CoreError::Sql(relational::SqlError::Aborted)); }
                let (domain, image) = db.native_metadata_image(self.v2_transaction.as_ref())?;
                let mut prepared = build(domain, &image).map_err(CoreError::Sql)?;
                #[cfg(all(test, feature = "engine-rocksdb"))]
                native_many_tests::INSERT_PREPARED.with_borrow_mut(|hook| {
                    if let Some(hook) = hook.as_mut() { hook(&image); }
                });
                prepared.epoch = 0;
                prepared.transaction = self.transaction_id;
                self.execute_bound_kernel(&prepared, &[], None, None, Some(&db), None)
            })();
            if result.is_err() { self.abort(); }
            return result;
        }
        self.admin.inner.db(SQL_SESSIONS)?;
        self.execute_engine(|domain, state| {
            let prepared = build(domain, state)?;
            prepared.validate_native_delete_image(state)?;
            Ok(prepared)
        }, None)
    }

    fn insert_engine_settling(
        &mut self,
        table: &relational::TableDef,
        values: Vec<relational::Value>,
        settle: bool,
    ) -> Result<relational::SqlResult, CoreError> {
        if self.is_v2_engine() {
            let db = self.v2_lease(Holder::Sql)?;
            let result = (|| {
                if self.aborted {
                    return Err(CoreError::Sql(relational::SqlError::Aborted));
                }
                let (domain, image) = db.native_metadata_image(self.v2_transaction.as_ref())?;
                // V2 binds durable metadata at execution; prepared epochs use zero.
                let mut prepared = relational::Prepared::insert_normalized_engine(
                    table, values, &[], domain, &image).map_err(CoreError::Sql)?;
                #[cfg(all(test, feature = "engine-rocksdb"))]
                native_many_tests::INSERT_PREPARED.with_borrow_mut(|hook| {
                    if let Some(hook) = hook.as_mut() { hook(&image); }
                });
                prepared.epoch = 0;
                prepared.transaction = self.transaction_id;
                self.execute_bound_kernel(&prepared, &[], None, None, Some(&db), None)
            })();
            let result = result.and_then(|value| {
                if settle { self.settle_sequences()?; }
                Ok(value)
            });
            if result.is_err() { self.abort(); }
            return result;
        }
        self.admin.inner.db(SQL_SESSIONS)?;
        self.execute_engine(
            |domain, state| relational::Prepared::insert_normalized_engine(table, values, &[], domain, state),
            None,
        )
    }
    pub(crate) fn update_engine(
        &mut self,
        table: &relational::TableDef,
        key: Vec<relational::Value>,
        sets: Vec<(usize, relational::Value)>,
        expected_version: u64,
    ) -> Result<relational::SqlResult, CoreError> {
        if self.is_v2_engine() {
            let db = self.v2_lease(Holder::Sql)?;
            let result = (|| {
                if self.aborted {
                    return Err(CoreError::Sql(relational::SqlError::Aborted));
                }
                let (domain, image) = db.native_metadata_image(self.v2_transaction.as_ref())?;
                let mut prepared = relational::Prepared::update_engine(
                    table, key, sets, domain, &image).map_err(CoreError::Sql)?;
                prepared.epoch = 0;
                prepared.transaction = self.transaction_id;
                self.execute_bound_kernel(&prepared, &[], Some(expected_version), None, Some(&db), None)
            })();
            if result.is_err() { self.abort(); }
            return result;
        }
        self.admin.inner.db(SQL_SESSIONS)?;
        self.execute_engine(
            |domain, state| relational::Prepared::update_engine(table, key, sets, domain, state),
            Some(expected_version),
        )
    }
    pub(crate) fn delete_engine(
        &mut self,
        table: &relational::TableDef,
        key: Vec<relational::Value>,
        expected_version: u64,
    ) -> Result<relational::SqlResult, CoreError> {
        if self.is_v2_engine() {
            let db = self.v2_lease(Holder::Sql)?;
            let result = (|| {
                if self.aborted {
                    return Err(CoreError::Sql(relational::SqlError::Aborted));
                }
                let (domain, image) = db.native_metadata_image(self.v2_transaction.as_ref())?;
                let mut prepared = relational::Prepared::delete_engine(
                    table, key, domain, &image).map_err(CoreError::Sql)?;
                prepared.epoch = 0;
                prepared.transaction = self.transaction_id;
                self.execute_bound_kernel(&prepared, &[], Some(expected_version), None, Some(&db), None)
            })();
            if result.is_err() { self.abort(); }
            return result;
        }
        self.admin.inner.db(SQL_SESSIONS)?;
        self.execute_engine(
            |domain, state| relational::Prepared::delete_engine(table, key, domain, state),
            Some(expected_version),
        )
    }
    /// Set FK trigger suppression for the active transaction. Call before each
    /// statement after BEGIN (including an implicit server transaction).
    /// Existing deferred checks still run at commit; commit/rollback resets it.
    pub fn set_replication_role_replica(&mut self, on: bool) -> Result<(), CoreError> {
        if !self.in_transaction() {
            return Err(CoreError::Sql(relational::SqlError::NoActiveTransaction));
        }
        if self.aborted {
            return Err(CoreError::Sql(relational::SqlError::Aborted));
        }
        self.deferral.replica = on;
        Ok(())
    }

    pub fn in_transaction(&self) -> bool {
        self.staged.is_some() || self.v2_transaction.is_some()
    }
    pub fn is_aborted(&self) -> bool {
        self.aborted
    }
    /// Start a protocol preparation boundary against the transaction's visible catalog.
    /// READ COMMITTED advances; repeatable snapshots and private DDL stay intact.
    pub fn begin_prepared_statement(&mut self) {
        if let Some(txn) = &mut self.v2_transaction
            && txn.engine_lifetime().is_none_or(|life| life.lease.as_ref().unwrap().check().is_ok())
        {
            txn.begin_statement();
        }
    }

    /// Whether a prepared statement belongs to the current transaction lifetime.
    pub fn prepared_is_current(&self, p: &relational::Prepared) -> bool {
        (p.transaction.is_none() || p.transaction == self.transaction_id)
            && p.engine_generation.is_none_or(|generation| {
                matches!(&self.admin.inner.backend, Backend::V2(slot) if slot.serves_stamp(generation.stamp))
            })
    }

    /// Catalog generation used by metadata planning at the current statement snapshot.
    /// Private DDL, failed snapshots and unsupported backends cannot be cached.
    pub fn statement_catalog_version(&self) -> Option<u64> {
        let Backend::V2(slot) = &self.admin.inner.backend else { return None; };
        if slot.is_cluster() { return None; }
        self.statement_catalog_version_with(|lease| match lease {
            Some(db) => {
                db.check().ok()?;
                Some(db.catalog().current_version())
            }
            None => Some(slot.catalog_version()),
        })
    }

    // The outer option validates the observation. An unknown current version
    // still permits the transaction's historical snapshot lookup.
    fn statement_catalog_version_with(
        &self,
        current_version: impl FnOnce(Option<&EngineLease>) -> Option<Option<u64>>,
    ) -> Option<u64> {
        if self.aborted || self.task.is_some() { return None; }
        let Some(txn) = &self.v2_transaction else { return current_version(None)?; };
        if txn.catalog_writes().next().is_some() || txn.snapshot_error().is_some() { return None; }
        let db = txn.engine_lifetime()?.lease.as_ref()?;
        let current = current_version(Some(db))?;
        match txn.current_snapshot_ts() {
            Some(ts) => current.filter(|version| *version <= ts.0)
                .or_else(|| db.catalog().snapshot_version(txn).map(|version| version.0)),
            None => current,
        }
    }

    /// Unknown backends retain their historical lifetime-only revalidation.
    /// Private catalog writes must never use that fallback, including on gateways.
    pub fn prepared_catalog_state(&self) -> PreparedCatalogState {
        if self.v2_transaction.as_ref().is_some_and(|txn| txn.catalog_writes().next().is_some()) {
            return PreparedCatalogState::PrivateDdl;
        }
        match &self.admin.inner.backend {
            Backend::V2(slot) if !slot.is_cluster() =>
                PreparedCatalogState::Version(self.statement_catalog_version()),
            _ => PreparedCatalogState::UnknownBackend,
        }
    }

    /// Committed local catalog version. Transactions and cluster gateways
    /// deliberately use the normal snapshot path instead.
    pub fn committed_catalog_version(&self) -> Option<u64> {
        if self.in_transaction() || self.aborted || self.task.is_some() { return None; }
        match &self.admin.inner.backend {
            Backend::V2(slot) if !slot.is_cluster() => slot.catalog_version(),
            _ => None,
        }
    }

    fn committed_catalog_key(&self, db: &EngineLease) -> Option<(u64, u64)> {
        if self.in_transaction() || self.aborted || self.task.is_some() || db.cluster_services().is_some() {
            return None;
        }
        Some((db.catalog().current_version()?, db.id()))
    }

    /// Reuse only SELECT preparation in an unnamed, ordinary SQL session.
    /// Authorization belongs to the caller and must run on every invocation.
    pub fn prepare_unnamed(
        &mut self, sql: &str, declared_types: &[Option<relational::DataType>],
    ) -> Result<relational::Prepared, CoreError> {
        relational::assert_sql_frontend_allowed();
        let lease = match self.statement_lease() {
            Ok(lease) => lease,
            Err(CoreError::Fenced) => return self.prepare_fenced_rollback(sql),
            Err(error) => return Err(error),
        };
        if let Some(db) = lease.as_deref() { self.bind_sequence_generation(db); }
        let key = lease.as_deref().filter(|_| {
            sql.trim_start().get(..7).is_some_and(|head| head.eq_ignore_ascii_case("SELECT "))
        }).and_then(|db| self.committed_catalog_key(db)).map(|(catalog_version, generation)| UnnamedPlanKey {
            sql: sql.into(), declared_types: declared_types.into(), catalog_version, generation,
            search_path: self.search_path.clone(), schema_user: self.schema_user.clone(),
            date_order: self.date_style.order,
        });
        if let Some(key) = &key
            && let Some(index) = self.unnamed_plans.iter().position(|(saved, _)| saved == key)
        {
            relational::take_sql_warnings();
            let entry = self.unnamed_plans.remove(index);
            let p = entry.1.clone();
            self.unnamed_plans.push(entry);
            return Ok(p);
        }
        let p = self.prepare_leased(sql, lease.as_deref())?;
        if let Some(key) = key
            && lease.as_deref().and_then(|db| self.committed_catalog_key(db)) == Some((key.catalog_version, key.generation))
            && !p.is_ddl()
        {
            if self.unnamed_plans.len() == 64 { self.unnamed_plans.remove(0); }
            self.unnamed_plans.push((key, p.clone()));
        }
        Ok(p)
    }

    /// Parses and binds a statement. In a task session the task's column
    /// rules (R3-303) run here too, before anything describes or binds the
    /// statement's columns: a hidden column and a nonexistent one get the same
    /// rule error, never a binder error (42703, an unknown function, a type
    /// error) that tells them apart (design-3 section 2, call site 1). A
    /// session without a task does no extra work.
    pub fn prepare(&mut self, sql: &str) -> Result<relational::Prepared, CoreError> {
        match self.statement_lease() {
            Ok(lease) => self.prepare_leased(sql, lease.as_deref()),
            Err(CoreError::Fenced) => self.prepare_fenced_rollback(sql),
            Err(error) => Err(error),
        }
    }

    pub(crate) fn prepare_leased(&mut self, sql: &str, db: Option<&EngineLease>) -> Result<relational::Prepared, CoreError> {
        let p = self.prepare_unruled(sql, db)?;
        if self.task.is_none() {
            return Ok(p);
        }
        // A refusal here is the statement's outcome in the read log
        // (R3-309), when the caller set its text first.
        self.task_column_rules_leased(&p, db).inspect_err(|error| {
            self.log_unstarted_task_failure(p.task_class(), p.parameter_count(), error);
            self.abort();
        })
    }
    fn prepare_fenced_rollback(&self, sql: &str) -> Result<relational::Prepared, CoreError> {
        // Full rollback remains local after a flip. Keep the former size and
        // token budgets even though this parse needs no engine metadata.
        let _value_budget = relational::ValueBudget::enter(self.value_budget(), true);
        let _statement_budget = relational::StatementBudget::enter(self.max_statement_tokens, true);
        let mut p = relational::Prepared::parse(sql, 0, 0)
            .ok().filter(|p| p.control() == Some("rollback"))
            .ok_or(CoreError::Fenced)?;
        p.transaction = self.transaction_id;
        p.engine_generation = self.sequence_generation.as_ref().map(GenerationTag::prepared_generation);
        p.set_schema_context(self.search_path.clone(), self.schema_user.clone());
        Ok(p)
    }

    fn prepare_unruled(&mut self, sql: &str, db: Option<&EngineLease>) -> Result<relational::Prepared, CoreError> {
        let _date_order = relational::clock::LiteralDateOrder::enter(self.date_style.order);
        let _value_budget = relational::ValueBudget::enter(self.value_budget(), self.is_v2_engine());
        let _statement_budget = relational::StatementBudget::enter(self.max_statement_tokens, self.is_v2_engine());
        if self.is_v2_engine() {
            let db = db.ok_or(CoreError::Fenced)?;
            self.bind_sequence_generation(db);
            return self.prepare_v2(sql, db);
        }
        if sql.len() > relational::MAX_SQL {
            self.abort();
            return Err(CoreError::Sql(relational::SqlError::Limit));
        }
        self.admin.inner.db(SQL_SESSIONS)?;
        relational::assert_sql_frontend_allowed();
        let mut result = if let Some(s) = &self.staged {
            if self.aborted {
                let p = relational::Prepared::parse(sql, s.view.domain, s.sql.epoch)
                    .map_err(CoreError::Sql)?;
                if !p.allows_aborted_transaction() {
                    return Err(CoreError::Sql(relational::SqlError::Aborted));
                }
                Ok(p)
            } else {
                relational::Prepared::parse(sql, s.view.domain, s.sql.epoch).map_err(CoreError::Sql)
            }
        } else {
            self.admin.inner.inspect(|s| {
                relational::Prepared::parse(sql, s.view.domain, s.sql.epoch).map_err(CoreError::Sql)
            })
        };
        if let Ok(p) = &mut result {
            p.set_schema_context(self.search_path.clone(), self.schema_user.clone());
            p.transaction = self.transaction_id;
            let bind = |s: &State| Ok(p.bind_schemas(&s.sql).unwrap_or_else(|_| p.clone()));
            let bound = match &self.staged {
                Some(s) => bind(s),
                None => self.admin.inner.inspect(bind),
            };
            if let Ok(bound) = bound {
                *p = bound;
            }
        }
        if result.is_err() && self.staged.is_some() {
            self.aborted = true;
        }
        result
    }
    /// Parse COPY and bind an INSERT-equivalent authority/dependency descriptor.
    pub fn prepare_copy(
        &mut self,
        sql: &str,
    ) -> Result<(relational::Copy, relational::Prepared), CoreError> {
        let lease = self.statement_lease()?;
        let db = lease.as_deref();
        let inner = self.admin.inner.clone();
        let _ticket = inner.enter_sql()?;
        relational::assert_sql_frontend_allowed();
        let result = (|| {
            if self.aborted {
                return Err(CoreError::Sql(relational::SqlError::Aborted));
            }
            let mut copy = relational::Copy::parse(sql).map_err(CoreError::Sql)?;
            let resolve = |s: &MetadataState<'_>| {
                let key = copy.schema_key(&s.sql, &self.search_path);
                let view = s.sql.view_names.contains_key(&key);
                Ok((key, view))
            };
            (copy.table, copy.target_is_view) = match &self.staged {
                Some(s) => resolve(&MetadataState::borrow(s))?,
                None => self.inspect_metadata_leased(db, resolve)?,
            };
            let prepare = |s: &MetadataState<'_>| {
                let image = self.copy_target_image(&copy, s);
                let mut p = copy
                    .prepared(image.as_ref().unwrap_or(&s.sql), s.view.domain)
                    .map_err(CoreError::Sql)?;
                p.set_schema_context(self.search_path.clone(), self.schema_user.clone());
                p.validate_schemas(&s.sql).map_err(CoreError::Sql)?;
                Ok(p.bind_schemas(&s.sql).unwrap_or(p))
            };
            let mut prepared = match &self.staged {
                Some(s) => prepare(&MetadataState::borrow(s))?,
                None => self.inspect_metadata_leased(db, prepare)?,
            };
            if matches!(self.admin.inner.backend, Backend::V2(_)) {
                prepared.epoch = 0;
                prepared.engine_generation = db.map(EngineLease::prepared_generation);
            }
            prepared.transaction = self.transaction_id;
            prepared.set_schema_context(self.search_path.clone(), self.schema_user.clone());
            Ok((copy, prepared))
        })();
        if result.is_err() {
            self.abort();
        }
        result
    }
    fn copy_target_image(&self, copy: &relational::Copy, s: &MetadataState<'_>) -> Option<relational::Relational> {
        if !copy.target_is_view { return None; }
        let (id, _) = s.sql.view_names.get(&copy.table)?;
        let view = s.sql.views?.get(id)?;
        let mut image = s.sql.image.clone();
        std::sync::Arc::make_mut(&mut image.tables).insert(copy.table.clone(), view_table(*id, view));
        Some(image)
    }
    fn release_copy_freeze(&mut self, index: usize) {
        let released = self.copy_freeze.split_off(index + 1);
        self.copy_freeze[index].extend(released.into_iter().flatten());
    }
    fn rollback_copy_freeze(&mut self, index: usize) {
        self.copy_freeze.truncate(index + 2);
        self.copy_freeze[index + 1].clear();
    }
    /// Runtime precondition, called after COPY input response and before reading rows.
    pub fn copy_freeze_allowed(&mut self, copy: &relational::Copy) -> Result<bool, CoreError> {
        let id = |s: &MetadataState<'_>| Ok(s.sql.tables.get(&copy.table).map(|t| t.id));
        let id = match &self.staged {
            Some(s) => id(&MetadataState::borrow(s))?,
            None => self.inspect_metadata(id)?,
        };
        Ok(self.in_transaction() && id.is_some_and(|id| self.copy_freeze.last().unwrap().contains(&id)))
    }
    pub fn copy_columns(
        &mut self,
        copy: &relational::Copy,
    ) -> Result<Vec<relational::ColumnDef>, CoreError> {
        let inner = self.admin.inner.clone();
        let _ticket = inner.enter_sql()?;
        relational::assert_sql_frontend_allowed();
        let columns = |s: &MetadataState<'_>| {
            let image = self.copy_target_image(copy, s);
            copy.columns_in(image.as_ref().unwrap_or(&s.sql)).map_err(CoreError::Sql)
        };
        let result = match &self.staged {
            Some(s) => columns(&MetadataState::borrow(s)),
            None => self.inspect_metadata(columns),
        };
        if result.is_err() {
            self.abort();
        }
        result
    }
    /// Resolve each COPY row once, including defaults, and check row-local constraints.
    /// The returned descriptor supplies every logical column so final ingestion
    /// neither reevaluates defaults nor reserves a sequence a second time.
    pub fn normalize_copy_rows_at(
        &mut self, prepared: &relational::Prepared, rows: Vec<Vec<relational::Value>>,
        clock: &relational::StatementClock,
    ) -> Result<(relational::Prepared, Vec<Vec<relational::Value>>), CoreError> {
        let input = relational::CopyInput { rows: rows.into_iter().map(|row| row.into_iter().map(Some).collect()).collect(), options: relational::CopyOptions::default(), first_line: 1, skipped: 0 };
        self.normalize_copy_cells_at(prepared, input, clock).map(|(p, rows, _)| (p, rows))
    }
    pub fn normalize_copy_cells_at(
        &mut self, prepared: &relational::Prepared, input: relational::CopyInput,
        clock: &relational::StatementClock,
    ) -> Result<(relational::Prepared, Vec<Vec<relational::Value>>, u64), CoreError> {
        let _budget = relational::ValueBudget::enter(self.value_budget(), matches!(self.admin.inner.backend, Backend::V2(_)));
        let _statement_budget = relational::StatementBudget::enter(self.max_statement_tokens, matches!(self.admin.inner.backend, Backend::V2(_)));
        relational::assert_sql_frontend_allowed();
        if self.is_v2_engine() {
            let db = self.v2_lease(Holder::Sql)?;
            return self.normalize_copy_v2(&db, prepared, input, clock);
        }
        self.admin.inner.db(SQL_SESSIONS)?;
        let result = (|| {
            self.guard_clock(clock)?;
            crate::cancellation::check()?;
            if self.aborted {
                return Err(CoreError::Sql(relational::SqlError::Aborted));
            }
            if prepared.transaction.is_some() && prepared.transaction != self.transaction_id {
                return Err(CoreError::Sql(relational::SqlError::StaleSchema));
            }
            let mut state = match &self.staged {
                Some(s) => s.clone(),
                None => self.admin.inner.legacy_statement_snapshot()?,
            };
            if prepared.domain != state.view.domain {
                return Err(CoreError::Sql(relational::SqlError::StaleSchema));
            }
            self.admin.inner.merge_live_sequences(&mut state)?;
            sequences::merge(
                &mut state,
                &*self
                    .pending_sequences
                    .lock()
                    .map_err(|_| CoreError::Fenced)?,
            );
            let baseline = if self.staged.is_some() {
                self.sequence_baseline.clone()
            } else {
                state
                    .sql
                    .catalog_ext()
                    .sequences
                    .iter()
                    .map(|(&id, d)| (id, d.incarnation))
                    .collect()
            };
            let context = relational::clock::ExecutionClock::new(clock)
                .with_text_settings(self.date_style, self.bytea_output)
                .with_extra_float_digits(self.extra_float_digits)
                .with_sequences(self.admin.inner.sequence_callback(
                    Arc::new(state.clone()),
                    baseline,
                    self.pending_sequences.clone(),
                    self.sequence_values.clone(),
                    self.sequence_tables.clone(),
                    self.search_path.clone(),
                ));
            let output = state.sql.normalize_copy(prepared, input, Some(&context));
            crate::cancellation::check()?;
            if let Some(error) = context.take_error() {
                return Err(error);
            }
            output.map_err(CoreError::Sql)
        })();
        if result.is_err() {
            self.abort();
        }
        result
    }
    pub fn ingest_copy(
        &mut self,
        prepared: &relational::Prepared,
        rows: Vec<Vec<relational::Value>>,
    ) -> Result<relational::SqlResult, CoreError> {
        let _value_budget = relational::ValueBudget::enter(self.value_budget(), matches!(self.admin.inner.backend, Backend::V2(_)));
        let _statement_budget = relational::StatementBudget::enter(self.max_statement_tokens, matches!(self.admin.inner.backend, Backend::V2(_)));
        self.refuse_task_session()?;
        let prepared = match prepared.copy_rows(rows, None) {
            Ok(p) => p,
            Err(e) => {
                self.abort();
                return Err(CoreError::Sql(e));
            }
        };
        self.execute_prepared(&prepared, &[], None)
    }
    pub fn ingest_copy_at(
        &mut self,
        prepared: &relational::Prepared,
        rows: Vec<Vec<relational::Value>>,
        clock: &relational::StatementClock,
    ) -> Result<relational::SqlResult, CoreError> {
        let _value_budget = relational::ValueBudget::enter(self.value_budget(), matches!(self.admin.inner.backend, Backend::V2(_)));
        let _statement_budget = relational::StatementBudget::enter(self.max_statement_tokens, matches!(self.admin.inner.backend, Backend::V2(_)));
        self.refuse_task_session()?;
        let context = relational::clock::ExecutionClock::new(clock)
            .with_text_settings(self.date_style, self.bytea_output);
        let prepared = match prepared.copy_rows(rows, Some(&context)) {
            Ok(p) => p,
            Err(e) => {
                self.abort();
                return Err(CoreError::Sql(e));
            }
        };
        self.execute_prepared_at(&prepared, &[], None, clock)
    }
    /// Plain exit for trusted local callers. A task session is refused before
    /// anything runs (GD-0061 R3-301); task callers use `execute_unreleased`.
    pub fn execute(
        &mut self,
        sql: &str,
        params: &[relational::Value],
        expected_version: Option<u64>,
    ) -> Result<relational::SqlResult, CoreError> {
        self.refuse_task_session()?;
        self.execute_unreleased(sql, params, expected_version)?.untasked()
    }
    pub fn execute_unreleased(
        &mut self,
        sql: &str,
        params: &[relational::Value],
        expected_version: Option<u64>,
    ) -> Result<crate::task::release::Unreleased<relational::SqlResult>, CoreError> {
        // GD-0061 R3-309: a task statement is logged with its text, and one
        // that fails before its read-log entry starts (parse, abort) is
        // logged with its error.
        self.set_task_statement_text(sql);
        let result = self.execute_text_unreleased(sql, params, expected_version);
        if let Err(error) = &result {
            self.log_text_failure(sql, params.len(), error);
        }
        result
    }
    /// Logs a SQL-text statement that failed before its read-log entry
    /// started (GD-0061 R3-309); no-op without a task.
    fn log_text_failure(&self, sql: &str, param_count: usize, error: &CoreError) {
        if self.task.is_some() {
            let class = relational::Prepared::task_class_of_sql(sql)
                .unwrap_or(crate::task::StatementClass::Other);
            self.log_unstarted_task_failure(class, param_count, error);
        }
    }
    fn execute_text_unreleased(
        &mut self,
        sql: &str,
        params: &[relational::Value],
        expected_version: Option<u64>,
    ) -> Result<crate::task::release::Unreleased<relational::SqlResult>, CoreError> {
        relational::assert_sql_frontend_allowed();
        // ROLLBACK remains available after any parse/bind/statement/commit error.
        if self.aborted {
            let p = relational::Prepared::parse(sql, 0, 0).map_err(CoreError::Sql)?;
            if !p.allows_aborted_transaction() {
                return Err(CoreError::Sql(relational::SqlError::Aborted));
            }
            if !params.is_empty() || expected_version.is_some() {
                return Err(CoreError::Sql(relational::SqlError::Parameters));
            }
            if p.control() == Some("rollback") {
                let tag = self.release_tag_with(
                    crate::task::StatementClass::TransactionControl,
                    0,
                    || Ok(Default::default()),
                );
                self.rollback();
                return Ok(crate::task::release::Unreleased::new(Default::default(), tag));
            }
        }
        let (p, lease) = self.prepare_for_execution(sql)?;
        self.execute_bound_clocked_leased(&p, params, expected_version, None, lease.as_deref(), None)
    }
    /// Execute using caller-supplied transaction, statement and wall time.
    pub fn execute_at(
        &mut self,
        sql: &str,
        params: &[relational::Value],
        expected_version: Option<u64>,
        clock: &relational::StatementClock,
    ) -> Result<relational::SqlResult, CoreError> {
        self.refuse_task_session()?;
        self.execute_at_unreleased(sql, params, expected_version, clock)?.untasked()
    }
    pub fn execute_at_unreleased(
        &mut self,
        sql: &str,
        params: &[relational::Value],
        expected_version: Option<u64>,
        clock: &relational::StatementClock,
    ) -> Result<crate::task::release::Unreleased<relational::SqlResult>, CoreError> {
        self.set_task_statement_text(sql);
        let result = (|| {
            relational::assert_sql_frontend_allowed();
            self.guard_clock(clock)?;
            let (p, lease) = self.prepare_for_execution(sql)?;
            self.execute_prepared_at_leased(&p, params, expected_version, clock, lease.as_deref(), None)
        })();
        if let Err(error) = &result {
            self.log_text_failure(sql, params.len(), error);
        }
        result
    }
    fn prepare_for_execution(&mut self, sql: &str) -> Result<(relational::Prepared, Option<SessionLease>), CoreError> {
        match self.statement_lease() {
            Ok(lease) => Ok((self.prepare_leased(sql, lease.as_deref())?, lease)),
            Err(CoreError::Fenced) => Ok((self.prepare_fenced_rollback(sql)?, None)),
            Err(error) => Err(error),
        }
    }

    pub fn execute_prepared_at(
        &mut self,
        p: &relational::Prepared,
        params: &[relational::Value],
        expected_version: Option<u64>,
        clock: &relational::StatementClock,
    ) -> Result<relational::SqlResult, CoreError> {
        self.refuse_task_session()?;
        self.execute_prepared_at_unreleased(p, params, expected_version, clock)?.untasked()
    }
    pub fn execute_prepared_at_unreleased(
        &mut self,
        p: &relational::Prepared,
        params: &[relational::Value],
        expected_version: Option<u64>,
        clock: &relational::StatementClock,
    ) -> Result<crate::task::release::Unreleased<relational::SqlResult>, CoreError> {
        self.execute_prepared_at_checked_unreleased(p, params, expected_version, clock, None)
    }
    /// Runs a gateway reply check before publishing an untasked autocommit
    /// write with a result set. A failed check discards the staged write.
    pub fn execute_prepared_at_checked_unreleased(
        &mut self,
        p: &relational::Prepared,
        params: &[relational::Value],
        expected_version: Option<u64>,
        clock: &relational::StatementClock,
        check_reply: Option<&mut SqlReplyCheck<'_>>,
    ) -> Result<crate::task::release::Unreleased<relational::SqlResult>, CoreError> {
        relational::assert_sql_frontend_allowed();
        self.guard_clock(clock)?;
        let lease = self.execution_lease(p)?;
        self.execute_prepared_at_leased(p, params, expected_version, clock, lease.as_deref(), check_reply)
    }

    fn execute_prepared_at_leased(
        &mut self,
        p: &relational::Prepared,
        params: &[relational::Value],
        expected_version: Option<u64>,
        clock: &relational::StatementClock,
        db: Option<&EngineLease>,
        check_reply: Option<&mut SqlReplyCheck<'_>>,
    ) -> Result<crate::task::release::Unreleased<relational::SqlResult>, CoreError> {
        let context = relational::clock::ExecutionClock::new(clock)
            .with_text_settings(self.date_style, self.bytea_output)
            .with_extra_float_digits(self.extra_float_digits);
        let result = self.execute_bound_clocked_leased(p, params, expected_version, Some(&context), db, check_reply);
        // A BEGIN under an unavailable clock stays unpinned, so the first available clocked statement
        // becomes the pin. That is acceptable: the server clock stays latched until restart.
        if result.is_ok() && p.control() == Some("begin") && clock.unavailable.is_none() {
            self.transaction_clock_us = Some(clock.transaction_us);
        }
        result
    }
    fn guard_clock(&mut self, clock: &relational::StatementClock) -> Result<(), CoreError> {
        // Unavailable instants cannot be validated or establish a transaction pin.
        if clock.unavailable.is_some() {
            return Ok(());
        }
        use relational::SqlError as E;
        let error = if clock.statement_us < clock.transaction_us {
            Some(E::InvalidParameterValue)
        } else if self.in_transaction()
            && self
                .transaction_clock_us
                .is_some_and(|us| us != clock.transaction_us)
        {
            // A caller (server) invariant violation, not a user type error: the server must pass
            // the same transaction_us for every statement of an open transaction. SqlError has no
            // dedicated internal variant, so Type is used; it aborts the block before any effect.
            Some(E::Type)
        } else {
            None
        };
        if let Some(error) = error {
            if self.in_transaction() {
                self.aborted = true;
            }
            return Err(CoreError::Sql(error));
        }
        if self.in_transaction() && self.transaction_clock_us.is_none() {
            self.transaction_clock_us = Some(clock.transaction_us);
        }
        Ok(())
    }
    pub fn execute_prepared(
        &mut self,
        p: &relational::Prepared,
        params: &[relational::Value],
        expected_version: Option<u64>,
    ) -> Result<relational::SqlResult, CoreError> {
        self.refuse_task_session()?;
        self.execute_prepared_unreleased(p, params, expected_version)?.untasked()
    }
    pub fn execute_prepared_unreleased(
        &mut self,
        p: &relational::Prepared,
        params: &[relational::Value],
        expected_version: Option<u64>,
    ) -> Result<crate::task::release::Unreleased<relational::SqlResult>, CoreError> {
        relational::assert_sql_frontend_allowed();
        self.execute_bound_clocked(p, params, expected_version, None, None)
    }
    /// Trusted Admin entry point. Gateways must authorize the typed source first.
    pub fn describe_typed_read(&mut self, read: relational::SingleSourceRead) -> Result<relational::Description, CoreError> {
        let _budget = relational::ValueBudget::enter(self.value_budget(), matches!(self.admin.inner.backend, Backend::V2(_)));
        let _statement_budget = relational::StatementBudget::enter(self.max_statement_tokens, matches!(self.admin.inner.backend, Backend::V2(_)));
        let lease = self.statement_lease()?;
        let p = self.prepare_typed_read(lease.as_deref(), vec![read.table.replace('.', "\0")], |domain, epoch|
            relational::Prepared::from_single_source_read(domain, epoch, read))?;
        self.describe_bound_engine(&p, lease.as_deref())
    }
    /// Typed leaf reads use the same transaction/snapshot kernel as prepared SQL.
    pub fn execute_typed_read(&mut self, read: relational::SingleSourceRead, expected_version: Option<u64>) -> Result<relational::SqlResult, CoreError> {
        self.refuse_task_session()?;
        self.execute_typed_read_unreleased(read, expected_version)?.untasked()
    }
    pub fn execute_typed_read_unreleased(&mut self, read: relational::SingleSourceRead, expected_version: Option<u64>)
        -> Result<crate::task::release::Unreleased<relational::SqlResult>, CoreError> {
        let _budget = relational::ValueBudget::enter(self.value_budget(), matches!(self.admin.inner.backend, Backend::V2(_)));
        let _statement_budget = relational::StatementBudget::enter(self.max_statement_tokens, matches!(self.admin.inner.backend, Backend::V2(_)));
        let lease = self.statement_lease()?;
        let p = self.prepare_typed_read(lease.as_deref(), vec![read.table.replace('.', "\0")], |domain, epoch|
            relational::Prepared::from_single_source_read(domain, epoch, read))?;
        // Native reads materialise their whole result: keep the result budget.
        relational::spill::bounded(|| self.execute_bound_clocked_leased(&p, &[], expected_version, None, lease.as_deref(), None))
    }
    /// Trusted Admin entry point; gateways must authorize both source tables first.
    pub fn describe_typed_join(&mut self, read: relational::TwoSourceRead) -> Result<relational::Description, CoreError> {
        let _budget = relational::ValueBudget::enter(self.value_budget(), matches!(self.admin.inner.backend, Backend::V2(_)));
        let _statement_budget = relational::StatementBudget::enter(self.max_statement_tokens, matches!(self.admin.inner.backend, Backend::V2(_)));
        let lease = self.statement_lease()?;
        let p = self.prepare_typed_read(lease.as_deref(), vec![read.left.table.replace('.', "\0"), read.right.table.replace('.', "\0")], |domain, epoch|
            relational::Prepared::from_two_source_read(domain, epoch, read))?;
        self.describe_bound_engine(&p, lease.as_deref())
    }
    pub fn execute_typed_join(&mut self, read: relational::TwoSourceRead, expected_version: Option<u64>) -> Result<relational::SqlResult, CoreError> {
        self.refuse_task_session()?;
        self.execute_typed_join_unreleased(read, expected_version)?.untasked()
    }
    pub fn execute_typed_join_unreleased(&mut self, read: relational::TwoSourceRead, expected_version: Option<u64>)
        -> Result<crate::task::release::Unreleased<relational::SqlResult>, CoreError> {
        let _budget = relational::ValueBudget::enter(self.value_budget(), matches!(self.admin.inner.backend, Backend::V2(_)));
        let _statement_budget = relational::StatementBudget::enter(self.max_statement_tokens, matches!(self.admin.inner.backend, Backend::V2(_)));
        let lease = self.statement_lease()?;
        let p = self.prepare_typed_read(lease.as_deref(), vec![read.left.table.replace('.', "\0"), read.right.table.replace('.', "\0")], |domain, epoch|
            relational::Prepared::from_two_source_read(domain, epoch, read))?;
        relational::spill::bounded(|| self.execute_bound_clocked_leased(&p, &[], expected_version, None, lease.as_deref(), None))
    }
    /// Trusted Admin entry point; gateways authorize the target and every source first.
    pub fn execute_typed_insert_select(&mut self, insert: relational::TypedInsertSelect,
        expected_version: Option<u64>) -> Result<relational::SqlResult, CoreError> {
        let _budget = relational::ValueBudget::enter(self.value_budget(), matches!(self.admin.inner.backend, Backend::V2(_)));
        let _statement_budget = relational::StatementBudget::enter(self.max_statement_tokens, matches!(self.admin.inner.backend, Backend::V2(_)));
        // Counts only, and refused by `admit()`: the guard, no twin (R3-301).
        self.refuse_task_session()?;
        let target = insert.table.replace('.', "\0");
        let lease = self.statement_lease()?;
        let p = self.prepare_typed_read(lease.as_deref(), vec![target], |domain, epoch|
            relational::Prepared::from_typed_insert_select(domain, epoch, insert))?;
        self.execute_bound_kernel(&p, &[], expected_version, None, lease.as_deref(), None)
    }
    fn prepare_typed_read(&mut self, db: Option<&EngineLease>, tables: Vec<String>,
        prepare: impl FnOnce(u128, u64) -> Result<relational::Prepared, relational::SqlError>) -> Result<relational::Prepared, CoreError> {
        let make = |s: &MetadataState<'_>| {
            if self.aborted { return Err(CoreError::Sql(relational::SqlError::Aborted)); }
            let epoch = if matches!(self.admin.inner.backend, Backend::V2(_)) { 0 } else { s.sql.epoch };
            let mut p = prepare(s.view.domain, epoch).map_err(CoreError::Sql)?;
            p.transaction = self.transaction_id;
            p.engine_generation = db.map(EngineLease::prepared_generation);
            p.set_schema_context(self.search_path.clone(), self.schema_user.clone());
            // Views can introduce dependencies beyond the authorized sources.
            for table in tables.into_iter().chain(p.authorization_tables().map_err(CoreError::Sql)?) {
                let key = s.sql.resolve_relation_key(&table, &self.search_path);
                if !s.sql.tables.contains_key(&key) && !s.sql.view_names.contains_key(&key) { return Err(CoreError::Sql(relational::SqlError::UnknownTable)); }
            }
            p.bind_schemas(&s.sql).map_err(CoreError::Sql)
        };
        let result = match &self.staged {
            Some(s) => make(&MetadataState::borrow(s)),
            None => self.inspect_metadata_leased(db, make),
        };
        if result.is_err() { self.abort(); }
        result
    }
    // Both frontends enter this kernel after constructing typed operations.
    // Keep SQL's public prepared-call boundary separate so native execution
    // cannot acquire a dependency on SQL routine dispatch or frontend policy.
    // Crate-internal steps only (transaction control, typed DDL, native
    // steps and views): they run inside an exit that carries the release tag
    // or refuses a task session, so their intermediate rows never leave core.
    fn execute_bound_engine(
        &mut self,
        p: &relational::Prepared,
        params: &[relational::Value],
        expected_version: Option<u64>,
    ) -> Result<relational::SqlResult, CoreError> {
        let lease = self.execution_lease(p)?;
        self.execute_bound_kernel(p, params, expected_version, None, lease.as_deref(), None)
    }
    /// The statement kernel. Its result carries the task's release tag
    /// (GD-0061 R3-301); only `release` or `untasked` take the value out.
    fn execute_bound_clocked(
        &mut self,
        p: &relational::Prepared,
        params: &[relational::Value],
        expected_version: Option<u64>,
        clock: Option<&relational::clock::ExecutionClock>,
        check_reply: Option<&mut SqlReplyCheck<'_>>,
    ) -> Result<crate::task::release::Unreleased<relational::SqlResult>, CoreError> {
        let lease = self.execution_lease(p)?;
        self.execute_bound_clocked_leased(p, params, expected_version, clock, lease.as_deref(), check_reply)
    }

    fn execute_bound_clocked_leased(
        &mut self,
        p: &relational::Prepared,
        params: &[relational::Value],
        expected_version: Option<u64>,
        clock: Option<&relational::clock::ExecutionClock>,
        db: Option<&EngineLease>,
        check_reply: Option<&mut SqlReplyCheck<'_>>,
    ) -> Result<crate::task::release::Unreleased<relational::SqlResult>, CoreError> {
        Self::check_prepared_generation(p, db)?;
        let (tag, expanded) = self.statement_tag_leased(p, db)?;
        // GD-0061 R3-308: in a task session a write runs only inside the
        // protected commit; any other write is refused before it runs (and
        // logged, R3-309).
        let p = expanded.as_ref().unwrap_or(p);
        // A task statement's spill reaches only a task cursor, whose pages
        // carry this tag; any other caller gets it materialised behind the tag.
        let result = self.refuse_task_write(p).and_then(|()| {
            // GD-0061 R3-401: the task statement's slots and time limit.
            let guard = crate::task::limits::StatementGuard::enter_leased(self, db)?;
            let _active = guard.as_ref().map(crate::task::limits::StatementGuard::activate);
            match tag {
                Some(_) => relational::spill::task_statement(|| {
                    self.execute_bound_kernel(p, params, expected_version, clock, db, None)
                }),
                None => self.execute_bound_kernel(
                    p, params, expected_version, clock, db, check_reply.filter(|_| self.task.is_none()),
                ),
            }
        });
        match result {
            Ok(result) => Ok(crate::task::release::Unreleased::new(result, tag)),
            Err(error) => {
                if let Some(tag) = &tag {
                    tag.fail(&error);
                }
                Err(error)
            }
        }
    }
    /// Statement slots of task sessions on this engine (GD-0061 R3-401).
    pub(crate) fn task_load(&self) -> &Arc<crate::task::limits::TaskLoad> {
        self.admin.task_load()
    }
    /// Whether this session runs inside the protected commit.
    pub(crate) fn in_protected_run(&self) -> bool {
        self.protected.is_some()
    }
    /// Refuses a statement that writes (DML or DDL) in a task session outside
    /// the protected commit (GD-0061 R3-308).
    pub(crate) fn refuse_task_write(&self, p: &relational::Prepared) -> Result<(), CoreError> {
        if self.task.is_none() || self.protected.is_some() || !crate::task::release::task_writes_refused() {
            return Ok(());
        }
        match p.v2_class() {
            relational::V2Class::Read | relational::V2Class::Control(_) => Ok(()),
            _ => Err(crate::task::release::write_refusal(p.task_class())),
        }
    }
    /// The release tag of a statement starting now, and the statement to run
    /// in its place (`SELECT *` expanded to the task's columns). A read
    /// carries the output plan of the task's column rules (R3-303's
    /// `apply_in_catalog`, on the session's catalog view). A denial refuses
    /// the statement here, before it runs: no row is read, nothing is
    /// debited, and no execution error (22012, casts, overflow) or timing
    /// can tell the caller anything about a column it may not read. Other
    /// statements return no task rows and get an empty plan, so any row
    /// would fail closed.
    #[allow(clippy::type_complexity)]
    fn statement_tag_leased(
        &self,
        p: &relational::Prepared,
        db: Option<&EngineLease>,
    ) -> Result<(Option<Arc<crate::task::release::ReleaseTag>>, Option<relational::Prepared>), CoreError> {
        let mut expanded = None;
        let class = if self.task.is_some() { p.task_class() } else { crate::task::StatementClass::Other };
        let tag = self.release_tag_with_leased(class, p.parameter_count(), db, || {
            if !p.is_read() {
                return Ok(Default::default());
            }
            let (statement, plan) = self.task_output_plan(p, db)?;
            expanded = Some(statement);
            Ok(plan)
        });
        if let Some(tag) = &tag {
            // A denial is logged as the statement's outcome (R3-309).
            tag.refuse_denied().inspect_err(|error| tag.fail(error))?;
        }
        Ok((tag, expanded))
    }
    /// The task's column rules (R3-303) on a statement just prepared in a task
    /// session, before anything describes or binds it, so a hidden column and
    /// a nonexistent one get the same rule error and no bind error (42703)
    /// precedes it. Returns the statement with its stars expanded (design-3
    /// section 2, call site 1). Outside a task session, or for a class the
    /// rules do not judge, the statement comes back unchanged. The protected
    /// commit applies the same rules again before it runs the statement.
    pub fn task_column_rules(
        &self,
        p: &relational::Prepared,
    ) -> Result<relational::Prepared, CoreError> {
        let lease = if self.task.is_some() { self.statement_lease()? } else { None };
        self.task_column_rules_leased(p, lease.as_deref())
    }

    fn task_column_rules_leased(&self, p: &relational::Prepared, db: Option<&EngineLease>) -> Result<relational::Prepared, CoreError> {
        use crate::task::StatementClass;
        if self.task.is_none()
            || !matches!(p.task_class(), StatementClass::Select | StatementClass::Update)
        {
            return Ok(p.clone());
        }
        let (expanded, _) = self.task_output_plan(p, db).map_err(CoreError::Task)?;
        Ok(expanded)
    }
    pub(crate) fn task_search_path(&self) -> &[String] {
        &self.search_path
    }
    fn task_output_plan(
        &self,
        p: &relational::Prepared,
        db: Option<&EngineLease>,
    ) -> Result<(relational::Prepared, crate::task::columns::OutputPlan), crate::task::TaskError> {
        use crate::task::TaskError;
        let core = |e: CoreError| TaskError::Core(Box::new(e));
        let ctx = self.task.as_ref().ok_or(TaskError::Revoked)?;
        let def = crate::task::catalog::lookup_leased(db, ctx.name.as_str())
            .map_err(core)?
            .filter(|def| def.id == ctx.task && !def.revoked)
            .ok_or(TaskError::Revoked)?;
        let labels = self.task_column_labels_leased(db).map_err(core)?;
        let plan = self.inspect_metadata_leased(db, |s| {
            Ok(crate::task::columns::apply_in_catalog_with_path(
                &def,
                ctx.bound_def_version,
                &labels,
                &s.sql,
                &self.search_path,
                p,
            ))
        });
        plan.map_err(core)?
    }
    /// Adds rows a task statement's scope accepted to the session's pending
    /// `rows_read` (R3-308). The protected commit's reads are not disclosure.
    pub(crate) fn note_task_rows_read(&self, meter: &crate::task::rows::StatementMeter) {
        match (&self.protected, &self.task_state) {
            (None, Some(state)) => state.add_meter(meter),
            _ => {
                meter.take_read_delta();
                meter.take_scanned_delta();
            }
        }
    }
    fn execute_bound_kernel(
        &mut self,
        p: &relational::Prepared,
        params: &[relational::Value],
        expected_version: Option<u64>,
        clock: Option<&relational::clock::ExecutionClock>,
        db: Option<&EngineLease>,
        check_reply: Option<&mut SqlReplyCheck<'_>>,
    ) -> Result<relational::SqlResult, CoreError> {
        // A spilled v2 RETURNING or SELECT result is materialised here unless the
        // caller streams it (`relational::spill::streaming`); errors and unwinds drop it.
        relational::spill::statement(|| {
            self.execute_bound_clocked_once(p, params, expected_version, clock, db, check_reply)
        })
    }
    fn execute_bound_clocked_once(
        &mut self,
        p: &relational::Prepared,
        params: &[relational::Value],
        expected_version: Option<u64>,
        clock: Option<&relational::clock::ExecutionClock>,
        db: Option<&EngineLease>,
        check_reply: Option<&mut SqlReplyCheck<'_>>,
    ) -> Result<relational::SqlResult, CoreError> {
        let _date_order = relational::clock::LiteralDateOrder::enter(p.literal_date_order);
        let _value_budget = relational::ValueBudget::enter(self.value_budget(), matches!(self.admin.inner.backend, Backend::V2(_)));
        let _statement_budget = relational::StatementBudget::enter(self.max_statement_tokens, matches!(self.admin.inner.backend, Backend::V2(_)));
        if p.schema_context_differs(&self.search_path, &self.schema_user) {
            let mut bound = p.clone();
            bound.set_schema_context(self.search_path.clone(), self.schema_user.clone());
            return self.execute_bound_clocked_once(&bound, params, expected_version, clock, db, check_reply);
        }
        if self.is_v2_engine() {
            let Some(db) = db else {
                if p.control() != Some("rollback") { return Err(CoreError::Fenced); }
                if !params.is_empty() || expected_version.is_some() {
                    return Err(CoreError::Sql(relational::SqlError::Parameters));
                }
                self.rollback();
                return Ok(relational::SqlResult::default());
            };
            Self::check_prepared_generation(p, Some(db))?;
            self.bind_sequence_generation(db);
            // Complete native materializations expire after any staged write,
            // including writes issued through SQL on the same v2 transaction.
            let revision = if self.v2_transaction.is_some() && !p.is_read() && p.control().is_none() {
                let Some(revision) = self.staged_revision.checked_add(1) else {
                    self.abort();
                    return Err(CoreError::Sql(relational::SqlError::Overflow));
                };
                Some(revision)
            } else { None };
            let result = self.execute_v2(db, p, params, expected_version, clock, check_reply);
            if result.is_ok() {
                if let Some(revision) = revision { self.staged_revision = revision; }
                if p.control() == Some("begin") { self.staged_revision = 0; }
            }
            return result;
        }
        self.admin.inner.db(SQL_SESSIONS)?;
        if self.aborted && !p.allows_aborted_transaction() {
            return Err(CoreError::Sql(relational::SqlError::Aborted));
        }
        let inner = self.admin.inner.clone();
        let _ticket = match inner.enter_sql() {
            Ok(ticket) => ticket,
            Err(e) => {
                if self.staged.is_some() {
                    self.aborted = true;
                }
                return Err(e);
            }
        };
        let result = crate::cancellation::check()
            .and_then(|()| self.run(p, params, expected_version, clock, check_reply))
            .map_err(|e| {
                if p.schema_ddl().is_some_and(|(_, _, create)| create)
                    && e == CoreError::Sql(relational::SqlError::DuplicateObject)
                {
                    CoreError::DuplicateSchema
                } else {
                    e
                }
            });
        if result.is_err() && self.staged.is_some() {
            self.aborted = true;
        }
        if self.staged.is_none() {
            // No open transaction (implicit statement, COMMIT or ROLLBACK): drop the pin so
            // a later BEGIN, clocked or not, starts with a fresh transaction_us.
            self.transaction_clock_us = None;
        }
        result
    }
    fn run(
        &mut self,
        p: &relational::Prepared,
        params: &[relational::Value],
        expected_version: Option<u64>,
        clock: Option<&relational::clock::ExecutionClock>,
        check_reply: Option<&mut SqlReplyCheck<'_>>,
    ) -> Result<relational::SqlResult, CoreError> {
        self.admin.inner.db(SQL_SESSIONS)?;
        use relational::SqlError as E;
        if p.transaction.is_some() && p.transaction != self.transaction_id {
            return Err(CoreError::Sql(E::StaleSchema));
        }
        if let Some(control) = p.control() {
            let (domain, epoch) = match &self.staged {
                Some(s) => (s.view.domain, s.sql.epoch),
                None => self
                    .admin
                    .inner
                    .inspect(|s| Ok((s.view.domain, s.sql.epoch)))?,
            };
            if p.domain != domain || p.epoch != epoch {
                return Err(CoreError::Sql(E::StaleSchema));
            }
            if !params.is_empty() || expected_version.is_some() {
                return Err(CoreError::Sql(E::Parameters));
            }
            match control {
                "begin" => {
                    if self.staged.is_some() {
                        return Err(CoreError::Sql(E::Transaction));
                    }
                    let mut token = [0u8; 16];
                    getrandom::fill(&mut token).map_err(io)?;
                    self.admin
                        .inner
                        .sql_transactions
                        .try_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                            if n < relational::LEGACY_MAX_TRANSACTIONS {
                                Some(n + 1)
                            } else {
                                None
                            }
                        })
                        .map_err(|_| CoreError::Busy)?;
                    match self.admin.inner.legacy_statement_snapshot() {
                        Ok(s) => {
                            self.sequence_baseline = s
                                .sql
                                .catalog_ext()
                                .sequences
                                .iter()
                                .map(|(&id, d)| (id, d.incarnation))
                                .collect();
                            self.staged = Some(s);
                            self.deferral = Default::default();
                            self.transaction_id = Some(u128::from_be_bytes(token));
                            self.staged_revision = 0;
                        }
                        Err(e) => {
                            self.admin
                                .inner
                                .sql_transactions
                                .fetch_sub(1, Ordering::AcqRel);
                            return Err(e);
                        }
                    }
                }
                "savepoint" | "release" | "rollback_to" => {
                    let staged = self
                        .staged
                        .as_ref()
                        .ok_or(CoreError::Sql(E::NoActiveTransaction))?;
                    let name = p.savepoint_name().expect("savepoint control has a name");
                    if control == "savepoint" {
                        // Bound whole snapshots independently of the statement limits.
                        if self.savepoints.len() >= 64 {
                            return Err(CoreError::Sql(E::Limit));
                        }
                        // Byte budget across all live captures (54000 beyond it; nothing captured).
                        let bytes = snapshot_bytes(staged)?
                            .checked_add(deferral_bytes(&self.deferral)?)
                            .ok_or(CoreError::Limit)?;
                        let used = self.savepoints.iter().map(|mark| mark.bytes).sum::<u64>();
                        if used
                            .checked_add(bytes)
                            .is_none_or(|total| total > self.savepoint_budget)
                        {
                            return Err(CoreError::Sql(E::Limit));
                        }
                        self.copy_freeze.push(Default::default());
                        self.savepoints.push(SqlSavepoint {
                            name: name.to_owned(),
                            state: SqlSavepointState {
                                staged: staged.clone(),
                                deferral: self.deferral.clone(),
                            },
                            bytes,
                        });
                    } else {
                        let index = self
                            .savepoints
                            .iter()
                            .rposition(|mark| mark.name == name)
                            .ok_or(CoreError::Sql(E::InvalidSavepoint))?;
                        if control == "release" {
                            self.release_copy_freeze(index);
                            self.savepoints.truncate(index);
                        } else {
                            let SqlSavepointState {
                                staged: mut restored,
                                deferral,
                            } = self.savepoints[index].state.clone();
                            // Allocation and currval are nontransactional. Merge by
                            // incarnation/stamp, rather than restoring older counters.
                            sequences::merge(&mut restored, &staged.seq);
                            self.admin.inner.merge_live_sequences(&mut restored)?;
                            sequences::merge(
                                &mut restored,
                                &*self
                                    .pending_sequences
                                    .lock()
                                    .map_err(|_| CoreError::Fenced)?,
                            );
                            let revision = self
                                .staged_revision
                                .checked_add(1)
                                .ok_or(CoreError::Sql(E::Overflow))?;
                            self.staged = Some(restored);
                            self.deferral = deferral;
                            self.staged_revision = revision;
                            self.rollback_copy_freeze(index);
                            self.savepoints.truncate(index + 1);
                            self.aborted = false;
                        }
                    }
                }
                "rollback" => {
                    if self.staged.is_none() {
                        return Err(CoreError::Sql(E::Transaction));
                    }
                    self.rollback();
                }
                "commit" => {
                    let staged = self
                        .staged
                        .as_ref()
                        .ok_or(CoreError::Sql(E::Transaction))?
                        .clone();
                    let mut staged = staged;
                    sequences::merge(
                        &mut staged,
                        &*self
                            .pending_sequences
                            .lock()
                            .map_err(|_| CoreError::Fenced)?,
                    );
                    if let Err(error) = staged.sql.validate_deferred(&mut self.deferral, || {
                        crate::cancellation::check().is_err()
                    }) {
                        // A deferred constraint rejection ends the transaction.
                        self.rollback();
                        return Err(CoreError::Sql(error));
                    }
                    self.publish(staged)?;
                    self.rollback();
                }
                _ => unreachable!(),
            }
            return Ok(Default::default());
        }
        let mut s = match &self.staged {
            Some(s) => s.clone(),
            None => self.admin.inner.legacy_statement_snapshot()?,
        };
        if p.domain != s.view.domain {
            return Err(CoreError::Sql(E::StaleSchema));
        }
        if p.is_sequence_ddl() || p.truncate_restarts_identity() {
            self.admin.inner.merge_live_sequences(&mut s)?;
        }
        sequences::merge(
            &mut s,
            &*self
                .pending_sequences
                .lock()
                .map_err(|_| CoreError::Fenced)?,
        );
        let baseline = if self.staged.is_some() {
            self.sequence_baseline.clone()
        } else {
            s.sql
                .catalog_ext()
                .sequences
                .iter()
                .map(|(&id, d)| (id, d.incarnation))
                .collect()
        };
        let context = clock.map(|clock| {
            let snapshot = Arc::new(s.clone());
            let context = clock.with_sequences(self.admin.inner.sequence_callback(
                snapshot.clone(), baseline, self.pending_sequences.clone(),
                self.sequence_values.clone(), self.sequence_tables.clone(), self.search_path.clone()));
            match self.privilege_callback.as_ref().filter(|_| p.has_privilege_inquiry()) {
                Some(callback) => context.with_privileges(callback.clone(), privilege_provider(
                    PrivilegeImage::Legacy(snapshot), self.search_path.clone(), None, self.admin.roles(), None)),
                None => context,
            }
        });
        let clock = context.as_ref();
        let sequence_links = p.is_sequence_ddl().then(|| {
            s.sql.catalog_ext().sequences.iter()
                .map(|(&id, def)| (id, def.owned_by))
                .collect::<BTreeMap<_, _>>()
        });
        let sequence_before = if p.is_sequence_ddl() || p.truncate_restarts_identity() {
            Some(
                s.sql
                    .catalog_ext()
                    .sequences
                    .iter()
                    .map(|(&id, definition)| {
                        let counter = s.seq.get(&id).cloned().unwrap_or(sequences::SeqCounter {
                            incarnation: definition.incarnation,
                            last_value: definition.start,
                            is_called: false,
                            log_hw: definition.start,
                            stamp: 0,
                        });
                        (id, counter)
                    })
                    .collect::<BTreeMap<_, _>>(),
            )
        } else {
            None
        };
        let freeze_before = p.copy_freeze_created(&s.sql);
        let mut deferral = self.deferral.clone();
        let result = if let Some((names, deferred)) = p.set_constraints() {
            // Outside a block PostgreSQL still resolves named constraints;
            // ALL succeeds while the ephemeral mode change is discarded.
            if !params.is_empty() || expected_version.is_some() {
                return Err(CoreError::Sql(E::Parameters));
            }
            s.sql
                .set_constraints(&mut deferral, names, deferred, || {
                    crate::cancellation::check().is_err()
                })
                .map(|()| Default::default())
        } else if self.staged.is_some() {
            s.sql.execute_deferred(
                p,
                params,
                expected_version,
                || crate::cancellation::check().is_err(),
                clock,
                &mut deferral,
            )
        } else {
            s.sql.execute_clocked(
                p,
                params,
                expected_version,
                || crate::cancellation::check().is_err(),
                clock,
            )
        };
        // Refresh only counters in the accepted transaction snapshot, including
        // failed statements. Failed row/catalog mutations remain in local s.
        self.admin.inner.merge_live_sequences(&mut s)?;
        let pending = self
            .pending_sequences
            .lock()
            .map_err(|_| CoreError::Fenced)?;
        sequences::merge(&mut s, &pending);
        if let Some(staged) = self.staged.as_mut() {
            self.admin.inner.merge_live_sequences(staged)?;
            sequences::merge(staged, &pending);
        }
        drop(pending);
        if let Some(error) = clock.and_then(|clock| clock.take_error()) {
            return Err(error);
        }
        let result = result.map_err(CoreError::Sql)?;
        p.validate_sequence_current(&s.sql, |id| {
            sequence_before
                .as_ref()
                .and_then(|counters| counters.get(&id))
                .map(|counter| counter.last_value)
        })
        .map_err(CoreError::Sql)?;
        if p.sequence_copies_counter() {
            for (&id, original_counter) in sequence_before.iter().flatten() {
                let mut counter = original_counter.clone();
                if let Some(definition) = s.sql.catalog_ext().sequences.get(&id)
                    && definition.incarnation != counter.incarnation
                {
                    counter.incarnation = definition.incarnation;
                    counter.log_hw = counter.last_value;
                    counter.stamp = 0;
                    s.seq.insert(id, counter);
                }
            }
        }
        if let Some((id, value)) = p.sequence_restart_seed(&s.sql) {
            let definition = &s.sql.catalog_ext().sequences[&id];
            s.seq.insert(
                id,
                sequences::SeqCounter {
                    incarnation: definition.incarnation,
                    last_value: value,
                    is_called: false,
                    log_hw: value,
                    stamp: 0,
                },
            );
        }
        if p.truncate_restarts_identity() {
            for (&id, definition) in &s.sql.catalog_ext().sequences {
                if s.seq
                    .get(&id)
                    .is_some_and(|counter| counter.incarnation != definition.incarnation)
                {
                    s.seq.insert(
                        id,
                        sequences::SeqCounter {
                            incarnation: definition.incarnation,
                            last_value: definition.start,
                            is_called: false,
                            log_hw: definition.start,
                            stamp: 0,
                        },
                    );
                }
            }
        }
        // ALTER may narrow bounds while retaining the current position. A prior
        // recovery reservation is expendable; keep it inside the new bounds.
        if p.is_sequence_ddl() {
            s.seq.retain(|id, counter| {
                s.sql
                    .catalog_ext()
                    .sequences
                    .get(id)
                    .is_some_and(|definition| definition.incarnation == counter.incarnation)
            });
            for (id, counter) in &mut s.seq {
                let definition = &s.sql.catalog_ext().sequences[id];
                counter.log_hw = counter.log_hw.clamp(definition.min, definition.max);
            }
        }
        if let Some(links) = sequence_links {
            for (&id, sequence) in &s.sql.catalog_ext().sequences {
                let previous = links.get(&id);
                if previous == Some(&sequence.owned_by) {
                    continue;
                }
                let owner = match sequence.owned_by.or_else(|| previous.copied().flatten()) {
                    Some((table, _)) => {
                        let key = s.sql.tables.values()
                            .find(|def| def.id == table)
                            .map(|def| s.sql.table_key(def))
                            .ok_or(CoreError::Sql(E::UnknownTable))?;
                        let key = self.table_authority_name(&key)?;
                        self.table_owner(&key).map_err(|error| match error {
                            roles::RoleError::Core(error) => error,
                            _ => CoreError::Policy(Error::Scope),
                        })?
                    }
                    None => self.ddl_owner.clone(),
                };
                s.sequence_owner_updates.insert(id, owner);
            }
        }
        // Reserve the complete durable/export envelope before accepting a stage.
        records(&s)?;
        if let Some(before) = sequence_before {
            let mut values = self.sequence_values.lock().map_err(|_| CoreError::Fenced)?;
            for (id, counter) in before {
                if let Some(definition) = s.sql.catalog_ext().sequences.get(&id)
                    && definition.incarnation != counter.incarnation
                    && let Some(value) = values.get(&(id, counter.incarnation)).copied()
                {
                    values.insert((id, definition.incarnation), value);
                }
            }
        }
        if self.staged.is_some() {
            crate::cancellation::check()?;
            if !p.is_read() {
                self.staged_revision = self
                    .staged_revision
                    .checked_add(1)
                    .ok_or(CoreError::Sql(E::Overflow))?;
            }
            self.copy_freeze.last_mut().unwrap().extend(p.copy_freeze_marks(&s.sql, freeze_before));
            self.staged = Some(s);
            self.deferral = deferral;
        } else if !p.is_read() {
            check_autocommit_reply(check_reply, &result)?;
            self.publish(s)?;
        }
        Ok(result)
    }
    /// Check the session's staged relation namespace, or the committed catalog.
    pub fn relation_exists(&self, name: &str) -> Result<bool, CoreError> {
        let exists = |state: &MetadataState<'_>| {
            state.sql.tables.contains_key(name)
                || state.sql.view_names.contains_key(name)
                || state
                    .sql
                    .catalog_ext()
                    .sequences
                    .values()
                    .any(|sequence| sequence.name == name)
        };
        match &self.staged {
            Some(state) => Ok(exists(&MetadataState::borrow(state))),
            None => self.inspect_metadata(|state| Ok(exists(state))),
        }
    }

    /// Direct view sources in the current catalog; None denotes an ordinary relation.
    pub fn view_authorization_sources(&self, name: &str) -> Result<Option<Vec<String>>, CoreError> {
        self.inspect_metadata(|s| {
            let key = s.sql.resolve_relation_key(name, &self.search_path);
            let Some((id, _)) = s.sql.view_names.get(&key) else { return Ok(None) };
            let view = s.sql.views.and_then(|views| views.get(id)).ok_or(CoreError::Sql(relational::SqlError::UnknownTable))?;
            let mut sources = Vec::new();
            for dependency in &view.dependencies {
                if let Some(table) = s.sql.tables.values().find(|t| t.id == dependency.relation) {
                    sources.push(s.sql.table_key(table));
                } else if let Some((name, _)) = s.sql.view_names.iter().find(|(_, (id, _))| *id == dependency.relation) {
                    sources.push(name.clone());
                }
            }
            Ok(Some(sources))
        })
    }

    /// A V2 transaction's own table tombstone, not an older snapshot's missing table.
    pub fn table_dropped_in_transaction(&self, id: u64) -> bool {
        self.v2_transaction.as_ref().is_some_and(|txn|
            matches!(txn.catalog_own(&engine::catalog::table_key(id)), Some(None)))
    }

    /// Resolve a staged table name to its durable owner key by stable table id.
    pub fn table_authority_name(&self, name: &str) -> Result<String, CoreError> {
        // V2 reconciles authority keys inside each DDL statement, so staged
        // names already are the authority names (including renames).
        if matches!(self.admin.inner.backend, Backend::V2(_)) {
            return Ok(name.to_owned());
        }
        let table_id = self
            .staged
            .as_ref()
            .and_then(|s| s.sql.tables.get(name))
            .map(|table| table.id);
        let Some(table_id) = table_id else {
            return Ok(name.to_owned());
        };
        self.admin.inner.inspect_metadata(|s| {
            Ok(s.sql
                .tables
                .values()
                .find(|candidate| candidate.id == table_id)
                .map(|candidate| s.sql.table_key(candidate))
                .unwrap_or_else(|| name.to_owned()))
        })
    }
    fn publish(&self, staged: State) -> Result<(), CoreError> {
        self.admin.inner.db(SQL_SESSIONS)?;
        self.admin
            .inner
            .change_inner(true, self.authority_managed, |s| {
                if s.view.domain != staged.view.domain || s.sql.generation != staged.sql.generation
                {
                    return Err(CoreError::Sql(relational::SqlError::Conflict));
                }
                let mut staged = staged;
                sequences::merge(&mut staged, &s.seq);
                self.admin.inner.merge_live_sequences(&mut staged)?;
                *s = staged;
                Ok(())
            })
    }
    pub fn rollback(&mut self) {
        let _ = self.settle_sequences();
        self.rollback_without_settling();
    }

    fn rollback_without_settling(&mut self) {
        self.xact.lock().unwrap_or_else(|e| e.into_inner()).take();
        self.copy_input = None;
        // A parked protected run is not an SQL transaction slot.
        self.protected = None;
        let v2 = self.v2_transaction.take().is_some();
        if self.staged.take().is_some() || v2 {
            self.admin
                .inner
                .sql_transactions
                .fetch_sub(1, Ordering::AcqRel);
        }
        self.pending_sequences
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
        self.sequence_baseline.clear();
        self.deferral = Default::default();
        self.copy_freeze = vec![Default::default()];
        self.savepoints.clear();
        self.v2_savepoints.clear();
        self.aborted = false;
        self.transaction_id = None;
        self.transaction_clock_us = None;
        self.staged_revision = 0;
    }
}
impl Drop for SqlSession {
    fn drop(&mut self) {
        self.rollback();
    }
}
#[cfg(test)]
mod typed_session_tests {
    use super::*;
    use relational::{Scan, SqlError as E, TableDef, Value as V, with_sql_parser_forbidden};

    fn fixture() -> (Admin, SqlSession, TableDef) {
        let root = crate::test_artifact_root();
        fs::create_dir_all(&root).unwrap();
        let mut token = [0u8; 16];
        getrandom::fill(&mut token).unwrap();
        let path = root.join(format!("typed-session-{:032x}", u128::from_be_bytes(token)));
        let admin = Admin::create(
            path,
            Limits {
                admitted: 1,
                ..Limits::default()
            },
        )
        .unwrap();
        let mut session = admin.sql_session();
        session
            .execute(
                "CREATE TABLE native_items (tenant BIGINT, id BIGINT, title TEXT, count INTEGER NOT NULL, PRIMARY KEY (tenant,id))",
                &[],
                None,
            )
            .unwrap();
        let table = admin.catalog().unwrap().1.remove(0);
        (admin, session, table)
    }
    fn row(id: i64) -> Vec<V> {
        vec![V::Int(7), V::Int(id), V::Text("before".into()), V::Int(0)]
    }
    fn scan(session: &mut SqlSession) -> relational::SqlResult {
        session
            .scan(Scan {
                table: "native_items".into(),
                projection: vec![],
                prefix: vec![],
                lower: None,
                upper: None,
                reverse: false,
                limit: relational::MAX_ROWS,
            })
            .unwrap()
    }

    #[test]
    fn rollback_to_restores_whole_session_capture_after_statement_failure() {
        let (_admin, mut session, table) = fixture();
        session.execute("BEGIN", &[], None).unwrap();
        session.insert_engine(&table, row(1)).unwrap();
        session.execute("SAVEPOINT captured", &[], None).unwrap();
        let before = records(&session.savepoints[0].state.staged).unwrap();
        let transaction_id = session.transaction_id;
        session.transaction_clock_us = Some(123);
        session.insert_engine(&table, row(2)).unwrap();
        session
            .execute("CREATE TABLE later (id BIGINT PRIMARY KEY)", &[], None)
            .unwrap();
        assert!(
            session
                .execute("INSERT INTO later VALUES (1), (1)", &[], None)
                .is_err()
        );
        assert!(session.aborted);
        let failed_revision = session.staged_revision;
        session.execute("ROLLBACK TO captured", &[], None).unwrap();
        assert!(session.in_transaction());
        assert!(!session.aborted);
        assert_eq!(session.transaction_id, transaction_id);
        assert_eq!(session.transaction_clock_us, Some(123));
        assert!(session.staged_revision > failed_revision);
        assert_eq!(records(session.staged.as_ref().unwrap()).unwrap(), before);
        assert_eq!(session.savepoints.len(), 1);
        // Target remains live and can restore the same complete capture again.
        session.insert_engine(&table, row(3)).unwrap();
        session.execute("ROLLBACK TO captured", &[], None).unwrap();
        assert_eq!(records(session.staged.as_ref().unwrap()).unwrap(), before);
        session.rollback();
        assert!(session.savepoints.is_empty());
    }

    #[test]
    fn savepoint_byte_budget_rejects_captures_without_changing_the_stack() {
        let (_admin, mut session, table) = fixture();
        session.execute("BEGIN", &[], None).unwrap();
        session.insert_engine(&table, row(1)).unwrap();
        let one = snapshot_bytes(session.staged.as_ref().unwrap()).unwrap();
        // Room for exactly one capture of this size.
        session.savepoint_budget = one + one / 2;
        session.execute("SAVEPOINT first", &[], None).unwrap();
        assert_eq!(session.savepoints.len(), 1);
        assert!(matches!(
            session.execute("SAVEPOINT second", &[], None),
            Err(CoreError::Sql(relational::SqlError::Limit))
        ));
        assert_eq!(session.savepoints.len(), 1);
        assert_eq!(session.savepoints[0].name, "first");
        // The failed SAVEPOINT aborts the block, as any error does; ROLLBACK TO recovers it.
        session.execute("ROLLBACK TO first", &[], None).unwrap();
        session.execute("RELEASE first", &[], None).unwrap();
        assert!(session.savepoints.is_empty());
        // Releasing frees the budget for a new capture.
        session.execute("SAVEPOINT again", &[], None).unwrap();
        assert_eq!(session.savepoints.len(), 1);
        session.rollback();
    }

    #[test]
    fn typed_crud_and_controls_never_parse_sql_and_publish_to_sql_readers() {
        let (admin, mut session, table) = fixture();
        with_sql_parser_forbidden(|| {
            session.begin_engine().unwrap();
            assert_eq!(session.catalog_engine().unwrap().1, vec![table.clone()]);
            assert_eq!(session.insert_engine(&table, row(1)).unwrap().affected, 1);
            let initial = scan(&mut session).rows.remove(0);
            assert_eq!(
                session
                    .update_engine(
                        &table,
                        vec![V::Int(7), V::Int(1)],
                        vec![(2, V::Text("after".into()))],
                        initial.version,
                    )
                    .unwrap()
                    .affected,
                1
            );
            let updated = scan(&mut session).rows.remove(0);
            assert!(updated.version > initial.version);
            session.insert_engine(&table, row(2)).unwrap();
            let second = scan(&mut session).rows.remove(1);
            assert_eq!(
                session
                    .delete_engine(&table, vec![V::Int(7), V::Int(2)], second.version)
                    .unwrap()
                    .affected,
                1
            );
            // No intermediate mutation is externally visible before commit.
            assert!(scan(&mut admin.sql_session()).rows.is_empty());
            session.commit_engine().unwrap();
            assert!(!session.in_transaction());
        });
        let rows = admin
            .sql_session()
            .execute("SELECT title FROM native_items LIMIT 10", &[], None)
            .unwrap()
            .rows;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values, vec![V::Text("after".into())]);
    }

    #[test]
    fn deferred_foreign_key_savepoint_restores_checks_and_modes() {
        let (_admin, mut session, _) = fixture();
        for sql in [
            "CREATE TABLE deferred_parent (id BIGINT PRIMARY KEY)",
            "CREATE TABLE deferred_child (id BIGINT PRIMARY KEY, parent_id BIGINT, CONSTRAINT deferred_fk FOREIGN KEY (parent_id) REFERENCES deferred_parent (id) DEFERRABLE INITIALLY DEFERRED)",
            "BEGIN",
            "SAVEPOINT clean",
            "INSERT INTO deferred_child VALUES (1, 99)",
            "ROLLBACK TO clean",
            "SET CONSTRAINTS deferred_fk IMMEDIATE",
            "SAVEPOINT immediate",
            "SET CONSTRAINTS deferred_fk DEFERRED",
            "ROLLBACK TO immediate",
        ] {
            session.execute(sql, &[], None).unwrap();
        }
        assert_eq!(
            session.execute("INSERT INTO deferred_child VALUES (2, 99)", &[], None),
            Err(CoreError::Sql(relational::SqlError::ForeignKeyViolation))
        );
        session.execute("ROLLBACK TO immediate", &[], None).unwrap();
        session.execute("COMMIT", &[], None).unwrap();
        session.execute("BEGIN", &[], None).unwrap();
        session
            .execute("INSERT INTO deferred_child VALUES (3, 99)", &[], None)
            .unwrap();
        assert_eq!(
            session.execute("COMMIT", &[], None),
            Err(CoreError::Sql(relational::SqlError::ForeignKeyViolation))
        );
        assert!(!session.in_transaction());
        assert!(!session.is_aborted());
        assert!(
            session
                .execute("SELECT id FROM deferred_child LIMIT 10", &[], None)
                .unwrap()
                .rows
                .is_empty()
        );
    }

    #[test]
    fn typed_descriptor_key_and_version_errors_abort_the_whole_transaction() {
        let (admin, mut session, table) = fixture();
        with_sql_parser_forbidden(|| {
            for case in 0..7 {
                session.begin_engine().unwrap();
                session.insert_engine(&table, row(1)).unwrap();
                let version = scan(&mut session).rows[0].version;
                let (result, expected) = match case {
                    0 => {
                        let mut malformed = table.clone();
                        malformed.primary_key = vec![usize::MAX];
                        (session.insert_engine(&malformed, row(2)), E::StaleSchema)
                    }
                    1 => (
                        session.delete_engine(&table, vec![V::Int(7)], version),
                        E::Unbounded,
                    ),
                    2 => (
                        session.update_engine(
                            &table,
                            vec![V::Int(7), V::Int(1)],
                            vec![(usize::MAX, V::Int(2))],
                            version,
                        ),
                        E::UnknownColumn,
                    ),
                    3 => (
                        session.update_engine(
                            &table,
                            vec![V::Int(7), V::Int(1)],
                            vec![(1, V::Int(2))],
                            version,
                        ),
                        E::Unsupported,
                    ),
                    4 => (
                        session.update_engine(
                            &table,
                            vec![V::Int(7), V::Int(1)],
                            vec![(3, V::Int(1)), (3, V::Int(2))],
                            version,
                        ),
                        E::Duplicate,
                    ),
                    5 => (
                        session.update_engine(
                            &table,
                            vec![V::Int(7), V::Int(1)],
                            vec![(3, V::Int(1))],
                            version + 1,
                        ),
                        E::Version,
                    ),
                    _ => (
                        // Validate the complete input even if no row matches.
                        session.update_engine(
                            &table,
                            vec![V::Int(7), V::Int(99)],
                            vec![(2, V::Text("x".repeat(relational::MAX_ROW_BYTES)))],
                            version,
                        ),
                        E::Limit,
                    ),
                };
                assert_eq!(result, Err(CoreError::Sql(expected)), "case {case}");
                assert!(session.is_aborted());
                assert_eq!(session.commit_engine(), Err(CoreError::Sql(E::Aborted)));
                session.rollback();
                assert!(scan(&mut admin.sql_session()).rows.is_empty());
            }
        });
    }

    #[test]
    fn typed_catalog_tracks_staged_schema_and_commit_rejects_concurrent_changes() {
        let (admin, mut session, table) = fixture();
        session.begin_engine().unwrap();
        let original_epoch = session.catalog_engine().unwrap().0;
        admin
            .sql_session()
            .execute("CREATE TABLE outside (id BIGINT PRIMARY KEY)", &[], None)
            .unwrap();
        assert_eq!(
            session.catalog_engine().unwrap(),
            (original_epoch, vec![table.clone()])
        );
        session
            .execute("CREATE TABLE inside (id BIGINT PRIMARY KEY)", &[], None)
            .unwrap();
        with_sql_parser_forbidden(|| {
            let (epoch, tables) = session.catalog_engine().unwrap();
            assert_eq!(epoch, original_epoch + 1);
            assert!(tables.iter().any(|t| t.name == "inside"));
            assert!(!tables.iter().any(|t| t.name == "outside"));
            session.insert_engine(&table, row(1)).unwrap();
            assert_eq!(session.commit_engine(), Err(CoreError::Sql(E::Conflict)));
            assert!(session.is_aborted());
            session.rollback();
        });
        assert!(
            !admin
                .catalog()
                .unwrap()
                .1
                .iter()
                .any(|t| t.name == "inside")
        );
        assert!(scan(&mut session).rows.is_empty());
    }

    #[test]
    fn staged_row_versions_match_stored_rows_without_mutating_the_snapshot() {
        let (admin, mut session, table) = fixture();
        with_sql_parser_forbidden(|| {
            let key = [V::Int(7), V::Int(1)];
            assert_eq!(
                session.row_version_engine(&table, &key),
                Err(CoreError::Sql(E::Transaction))
            );
            session.begin_engine().unwrap();
            session.insert_engine(&table, row(1)).unwrap();
            let inserted = session.row_version_engine(&table, &key).unwrap();
            assert_eq!(inserted, scan(&mut session).rows[0].version);
            // An intervening allocation means the next version cannot be guessed
            // by incrementing this row's previous version.
            session.insert_engine(&table, row(2)).unwrap();
            session
                .update_engine(&table, key.to_vec(), vec![(3, V::Int(1))], inserted)
                .unwrap();
            let snapshot = session.staged.as_ref().unwrap().sql.clone();
            let updated = session.row_version_engine(&table, &key).unwrap();
            assert!(updated > inserted + 1);
            assert_eq!(session.staged.as_ref().unwrap().sql, snapshot);
            assert_eq!(updated, scan(&mut session).rows[0].version);
            assert!(scan(&mut admin.sql_session()).rows.is_empty());
            session.commit_engine().unwrap();
            assert_eq!(updated, scan(&mut admin.sql_session()).rows[0].version);
            assert_eq!(
                session.row_version_engine(&table, &key),
                Err(CoreError::Sql(E::Transaction))
            );
        });
    }

    #[test]
    fn staged_row_version_errors_abort_and_discard_all_pending_changes() {
        let (admin, mut session, table) = fixture();
        let before = admin.inner.inspect(|state| Ok(state.sql.clone())).unwrap();
        with_sql_parser_forbidden(|| {
            for case in 0..8 {
                session.begin_engine().unwrap();
                session.insert_engine(&table, row(1)).unwrap();
                let mut descriptor = table.clone();
                let mut key = vec![V::Int(7), V::Int(1)];
                let expected = match case {
                    0 => {
                        descriptor.primary_key = vec![usize::MAX];
                        CoreError::Sql(E::StaleSchema)
                    }
                    1 => {
                        key.pop();
                        CoreError::Sql(E::Unbounded)
                    }
                    2 => {
                        key[1] = V::Text("wrong".into());
                        CoreError::Sql(E::Type)
                    }
                    3 => {
                        key[1] = V::Null;
                        CoreError::Sql(E::Null)
                    }
                    4 => {
                        key[1] = V::Text("x".repeat(relational::MAX_ROW_BYTES + 1));
                        CoreError::Sql(E::Limit)
                    }
                    5 => {
                        key[1] = V::Int(99);
                        CoreError::Sql(E::Conflict)
                    }
                    6 => {
                        session.abort();
                        CoreError::Sql(E::Aborted)
                    }
                    _ => CoreError::Cancelled,
                };
                let result = if case == 7 {
                    let cancelled = crate::Cancellation::default();
                    assert!(cancelled.cancel());
                    cancelled.scope(|| session.row_version_engine(&descriptor, &key))
                } else {
                    session.row_version_engine(&descriptor, &key)
                };
                assert_eq!(result, Err(expected), "case {case}");
                assert!(session.is_aborted());
                assert_eq!(session.commit_engine(), Err(CoreError::Sql(E::Aborted)));
                session.rollback();
                assert_eq!(
                    admin.inner.inspect(|state| Ok(state.sql.clone())).unwrap(),
                    before
                );
            }
        });
    }

    #[test]
    fn snapshot_identity_requires_active_nonaborted_transaction_and_checks_cancellation() {
        let (admin, mut session, table) = fixture();
        let before = admin.inner.inspect(|state| Ok(state.sql.clone())).unwrap();
        with_sql_parser_forbidden(|| {
            assert_eq!(
                session.snapshot_identity_engine(),
                Err(CoreError::Sql(E::Transaction))
            );
            assert!(!session.is_aborted());

            session.begin_engine().unwrap();
            session.abort();
            assert_eq!(
                session.snapshot_identity_engine(),
                Err(CoreError::Sql(E::Aborted))
            );
            session.rollback();

            session.begin_engine().unwrap();
            session.insert_engine(&table, row(1)).unwrap();
            let cancelled = crate::Cancellation::default();
            assert!(cancelled.cancel());
            assert_eq!(
                cancelled.scope(|| session.snapshot_identity_engine()),
                Err(CoreError::Cancelled)
            );
            assert!(session.is_aborted());
            assert_eq!(session.commit_engine(), Err(CoreError::Sql(E::Aborted)));
            session.rollback();
        });
        assert_eq!(
            admin.inner.inspect(|state| Ok(state.sql.clone())).unwrap(),
            before
        );
    }

    #[test]
    fn snapshot_identity_uses_staged_domain_and_epoch_despite_external_ddl() {
        let (admin, mut session, _) = fixture();
        let identity = admin
            .inner
            .inspect(|state| Ok((state.view.domain, state.sql.epoch)))
            .unwrap();
        session.begin_engine().unwrap();
        assert_eq!(session.snapshot_identity_engine().unwrap(), identity);
        let mut outside = admin.sql_session();
        for statement in [
            "CREATE TABLE outside_one (id BIGINT PRIMARY KEY)",
            "CREATE TABLE outside_two (id BIGINT PRIMARY KEY)",
        ] {
            outside.execute(statement, &[], None).unwrap();
        }
        with_sql_parser_forbidden(|| {
            assert_eq!(session.snapshot_identity_engine().unwrap(), identity);
        });
        session
            .execute("CREATE TABLE inside (id BIGINT PRIMARY KEY)", &[], None)
            .unwrap();
        with_sql_parser_forbidden(|| {
            let staged_identity = session.snapshot_identity_engine().unwrap();
            assert_eq!(staged_identity, (identity.0, identity.1 + 1));
            assert_eq!(staged_identity.1, session.catalog_engine().unwrap().0);
            assert_eq!(admin.catalog().unwrap().0, identity.1 + 2);
            session.rollback();
            session.begin_engine().unwrap();
            assert_eq!(
                session.snapshot_identity_engine().unwrap(),
                (identity.0, identity.1 + 2)
            );
            session.rollback();
        });
    }

    #[test]
    fn typed_statements_preserve_admission_cancellation_and_transaction_limits() {
        let (admin, mut session, table) = fixture();
        with_sql_parser_forbidden(|| {
            session.begin_engine().unwrap();
            let ticket = admin.inner.enter_sql().unwrap();
            assert_eq!(session.insert_engine(&table, row(1)), Err(CoreError::Busy));
            assert!(session.is_aborted());
            drop(ticket);
            session.rollback();

            session.begin_engine().unwrap();
            session.insert_engine(&table, row(1)).unwrap();
            let cancelled = crate::Cancellation::default();
            assert!(cancelled.cancel());
            assert_eq!(
                cancelled.scope(|| session.commit_engine()),
                Err(CoreError::Cancelled)
            );
            assert!(session.is_aborted());
            session.rollback();
            assert!(scan(&mut session).rows.is_empty());

            let mut sessions: Vec<_> = (0..admin.max_transactions())
                .map(|_| admin.sql_session())
                .collect();
            for active in &mut sessions {
                active.begin_engine().unwrap();
            }
            assert_eq!(session.begin_engine(), Err(CoreError::Busy));
            assert!(!session.in_transaction());
            sessions[0].rollback();
            session.begin_engine().unwrap();
            session.rollback();
        });
    }

    #[test]
    fn typed_controls_preserve_sql_prepared_transaction_lifetimes() {
        let (_, mut session, table) = fixture();
        session.begin_engine().unwrap();
        let prepared = session
            .prepare("INSERT INTO native_items VALUES (7,1,'old',0)")
            .unwrap();
        assert!(session.prepared_is_current(&prepared));
        session.commit_engine().unwrap();
        assert!(!session.prepared_is_current(&prepared));
        session.begin_engine().unwrap();
        assert!(!session.prepared_is_current(&prepared));
        assert_eq!(
            session.execute_prepared(&prepared, &[], None),
            Err(CoreError::Sql(E::StaleSchema))
        );
        assert!(session.is_aborted());
        session.rollback();
        with_sql_parser_forbidden(|| session.insert_engine(&table, row(1))).unwrap();
    }
}
#[cfg(windows)]
pub(crate) fn crash_now() -> ! {
    unsafe extern "system" {
        fn GetCurrentProcess() -> *mut core::ffi::c_void;
        fn TerminateProcess(process: *mut core::ffi::c_void, exit_code: u32) -> i32;
    }
    // SAFETY: GetCurrentProcess returns the current process pseudo-handle. This
    // trusted fault hook intentionally terminates it without Rust destructors.
    unsafe {
        TerminateProcess(GetCurrentProcess(), 86);
    }
    std::process::exit(87)
}
#[cfg(not(windows))]
pub(crate) fn crash_now() -> ! {
    std::process::exit(86)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn preparation_precheck_budgets_restore_and_preserve_legacy() {
        use relational::{Prepared, SqlError};
        let long_value = format!("SELECT '{}'", "x".repeat(70_000));
        let many_tokens = format!("SELECT 1 IN ({})", vec!["1"; 20_000].join(","));
        let check = |sql: &str, limited: bool| {
            let targets = Prepared::sequence_ddl_targets(sql);
            let references = Prepared::constraint_references(sql);
            if limited {
                assert_eq!(targets, Err(SqlError::Limit));
                assert_eq!(references, Err(SqlError::Limit));
            } else {
                assert_eq!(targets, Ok(vec![]));
                assert_eq!(references, Ok(vec![]));
            }
        };
        let mut engines = vec![Engine::Legacy];
        if cfg!(feature = "engine-rocksdb") { engines.push(Engine::V2); }
        for engine in engines {
            let path = fresh("preparation-precheck-budgets");
            let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
            let mut session = admin.sql_session();
            session.set_max_value_bytes(100_000);
            session.set_max_statement_tokens(0);
            for sql in [&long_value, &many_tokens] {
                check(sql, true);
                {
                    let _budgets = session.statement_budgets();
                    check(sql, engine == Engine::Legacy);
                    // A nested session scope restores the outer session's limits.
                    let legacy_path = fresh("preparation-precheck-nested");
                    let legacy = Admin::create_engine(&legacy_path, Limits::default(), Engine::Legacy).unwrap();
                    {
                        let _nested = legacy.sql_session().statement_budgets();
                        check(sql, true);
                    }
                    check(sql, engine == Engine::Legacy);
                    drop(legacy);
                    fs::remove_dir_all(legacy_path).unwrap();
                }
                check(sql, true);
            }
            drop(session);
            drop(admin);
            fs::remove_dir_all(path).unwrap();
        }
    }
    #[test]
    fn legacy_statement_token_budget() {
        let path = fresh("legacy-statement-token-budget");
        let admin = Admin::create_engine(&path, Limits::default(), Engine::Legacy).unwrap();
        let mut s = admin.sql_session();
        s.set_max_statement_tokens(0);
        let list = (1..=3000).map(|n| n.to_string()).collect::<Vec<_>>().join(",");
        assert_eq!(s.execute(&format!("SELECT 1 IN ({list})"), &[], None), Err(CoreError::Sql(relational::SqlError::Limit)));
        drop(s); drop(admin); fs::remove_dir_all(path).unwrap();
    }
    #[test]
    fn v2_catalog_xid_uses_seconds_across_low_bit_wrap_and_clock_restart() {
        use super::engine::hlc::{HlcClock, Ts};
        let now_ms = 1_800_000_000_000;
        let mut clock = HlcClock::default();
        let first = clock.next(now_ms).unwrap();
        let second = clock.next(now_ms + 70_000).unwrap();
        assert!(second.0 - first.0 > u64::from(u32::MAX));
        let previous = catalog_xid_from_hlc(first.0) as u32;
        let current = catalog_xid_from_hlc(second.0) as u32;
        assert_eq!(current - previous, 70);
        assert!((current.wrapping_sub(previous) as i32) >= 0);
        // The sequencer restores its clock from the durable last_commit_ts.
        let mut reopened = HlcClock::new(Ts(second.0));
        assert_eq!(catalog_xid_from_hlc(reopened.last.0), u64::from(current));
        let after = reopened.next(now_ms).unwrap();
        assert!(catalog_xid_from_hlc(after.0) >= u64::from(current));
        assert_eq!(
            catalog_xid_from_hlc((now_ms << 16) | 65535),
            u64::from(previous)
        );
    }

    #[cfg(feature = "engine-rocksdb")]
    #[test]
    fn v2_catalog_xid_survives_database_reopen() {
        let path = fresh("v2-catalog-xid-reopen");
        let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
        let before = admin.catalog_generation().unwrap().1;
        admin
            .sql_session()
            .execute("CREATE TABLE xid_before (id BIGINT PRIMARY KEY)", &[], None)
            .unwrap();
        let committed = admin.catalog_generation().unwrap().1;
        assert!(committed >= before);
        drop(admin);
        let admin = Admin::open(&path, Limits::default()).unwrap();
        assert_eq!(admin.catalog_generation().unwrap().1, committed);
        admin
            .sql_session()
            .execute("CREATE TABLE xid_after (id BIGINT PRIMARY KEY)", &[], None)
            .unwrap();
        assert!(admin.catalog_generation().unwrap().1 >= committed);
        drop(admin);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn zero_row_heap_metadata_is_persisted_and_reopened() {
        struct Cleanup(std::path::PathBuf);
        impl Drop for Cleanup {
            fn drop(&mut self) {
                let _ = fs::remove_dir_all(&self.0);
            }
        }
        for (ddl, heap, format) in [
            ("CREATE TABLE t(id BIGINT)", true, 29),
            ("CREATE TABLE t(id BIGINT PRIMARY KEY)", false, 3),
        ] {
            let path = fresh("heap-zero-row-format");
            let _cleanup = Cleanup(path.clone());
            {
                let admin = Admin::create(&path, Limits::default()).unwrap();
                admin.sql_session().execute(ddl, &[], None).unwrap();
            }
            let admin = Admin::open(&path, Limits::default()).unwrap();
            let tx = admin
                .inner
                .db(LEGACY_STORAGE)
                .unwrap()
                .begin_read()
                .unwrap();
            let table = tx.open_table(RECORDS).unwrap();
            let metadata: Metadata =
                decode(table.get(&[0u8][..]).unwrap().unwrap().value()).unwrap();
            assert_eq!(metadata.format, format);
            let sql: relational::Relational =
                decode(table.get(&[5u8][..]).unwrap().unwrap().value()).unwrap();
            assert_eq!(sql.tables.len(), 1);
            assert_eq!(sql.contains_heap(), heap);
            assert!(sql.rows_empty_engine());
        }
    }

    #[test]
    fn accounting_format_migration_future_versions_and_corruption_fail_closed() {
        let mut state = empty(9);
        let g = Grant {
            task: TaskId(1),
            issuer: Principal(1),
            executor: Principal(2),
            approver: Principal(3),
            policy: Policy {
                tenant: TenantId(42),
                columns: std::collections::BTreeSet::from([Column::Status]),
                transitions: std::collections::BTreeSet::from([(Status::Failed, Status::Queued)]),
                incident: (100, 107),
                distinct_limit: 1,
                transition_limit: 1,
                approval_above: 1,
                valid_from: 1,
                expires: 1000,
            },
        };
        state.view.tasks.insert(
            TaskId(1),
            TaskState {
                grant: g,
                revoked: false,
                touched: Default::default(),
                transitions: 0,
            },
        );
        let legacy = records(&state).unwrap();
        for version in [1, 2, 3, 4, u32::MAX] {
            let mut r = legacy.clone();
            if version == 1 {
                r.remove(&vec![5]);
            } // In-memory record fixture, no filesystem cleanup.
            r.insert(
                vec![0],
                encode(&Metadata {
                    format: version,
                    domain: 9,
                    schema: 0,
                    last_time: 0,
                    next_approval: 0,
                    origin: None,
                })
                .unwrap(),
            );
            if version <= 3 {
                let parsed = from_records(r.clone()).unwrap();
                assert!(parsed.accounting.is_empty());
                let path = fresh("accounting-legacy");
                fs::create_dir(&path).unwrap();
                let db = Database::create(path.join("data.redb")).unwrap();
                let tx = db.begin_write().unwrap();
                {
                    let mut table = tx.open_table(RECORDS).unwrap();
                    for (key, value) in &r {
                        table.insert(key.as_slice(), value.as_slice()).unwrap();
                    }
                }
                tx.commit().unwrap();
                drop(db);
                fs::copy(path.join("data.redb"), path.join("legacy-original.redb")).unwrap();
                let a = Admin::open(&path, Limits::default()).unwrap();
                #[cfg(feature = "legacy-task")]
                {
                let e = a.executor(TaskId(1), Principal(2)).unwrap();
                assert_eq!(
                    e.reserve_disclosure(1, 10, 10),
                    Err(CoreError::Policy(Error::Budget))
                );
                a.provision_task_resources(
                    TaskId(1),
                    TaskResources {
                        read_rows: 1,
                        disclosed_rows: 1,
                        encoded_bytes: 1,
                    },
                )
                .unwrap();
                e.reserve_disclosure(1, 10, 10).unwrap();
                }
                let export = fresh("accounting-format-export");
                a.export(&export).unwrap();
                let mut bytes = fs::read(&export).unwrap();
                bytes[8..12].copy_from_slice(&2u32.to_be_bytes());
                let mismatch = fresh("accounting-header-mismatch");
                fs::write(&mismatch, bytes).unwrap();
                assert!(matches!(
                    Admin::verify_export(mismatch),
                    Err(CoreError::Corrupt(_))
                ));
            } else if version == NATIVE_FORMAT {
                assert!(matches!(
                    from_records(r.clone()),
                    Err(CoreError::Corrupt(_))
                ));
            } else {
                assert!(matches!(
                    from_records(r.clone()),
                    Err(CoreError::Unsupported)
                ));
            }
            r.insert(
                key(6, &[1]),
                encode(&TaskAccounting {
                    limits: TaskResources::default(),
                    used: TaskResources {
                        read_rows: 1,
                        ..Default::default()
                    },
                })
                .unwrap(),
            );
            assert!(from_records(r).is_err());
        }
        for value in [
            vec![],
            vec![255],
            encode(&TaskAccounting {
                limits: TaskResources::default(),
                used: TaskResources::default(),
            })
            .unwrap(),
        ] {
            let mut r = legacy.clone();
            r.insert(key(6, &[999]), value);
            assert!(from_records(r).is_err());
        }
    }
    fn fresh(name: &str) -> std::path::PathBuf {
        let root = crate::test_artifact_root();
        fs::create_dir_all(&root).unwrap();
        root.join(format!(
            "unit-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }
    #[test]
    fn legacy_format_read_migrate_and_preserve_original_artifacts() {
        let mut state = empty(777);
        state.view.rows.insert(
            Key {
                tenant: TenantId(42),
                row: RowId(1),
            },
            Row {
                status: Status::Failed,
                failed_at: 103,
                attempt_count: 0,
                version: 3,
            },
        );
        let mut values: BTreeMap<_, _> = records(&state)
            .unwrap()
            .into_iter()
            .filter(|(k, _)| *k != vec![5])
            .collect();
        values.insert(
            vec![0],
            encode(&Metadata {
                format: 1,
                domain: 777,
                schema: 0,
                last_time: 0,
                next_approval: 0,
                origin: None,
            })
            .unwrap(),
        );
        let path = fresh("legacy-source");
        fs::create_dir(&path).unwrap();
        let db = Database::create(path.join("data.redb")).unwrap();
        let tx = db.begin_write().unwrap();
        {
            let mut t = tx.open_table(RECORDS).unwrap();
            for (k, v) in &values {
                t.insert(k.as_slice(), v.as_slice()).unwrap();
            }
        }
        tx.commit().unwrap();
        drop(db);
        let original = fs::read(path.join("data.redb")).unwrap();
        let legacy_export = fresh("legacy-export");
        let payload = encode(&values).unwrap();
        let mut bytes = MAGIC.to_vec();
        bytes.extend(1u32.to_be_bytes());
        bytes.extend((payload.len() as u64).to_be_bytes());
        bytes.extend(Sha256::digest(&payload));
        bytes.extend(payload);
        fs::write(&legacy_export, &bytes).unwrap();
        assert_eq!(Admin::verify_export(&legacy_export).unwrap(), state.view);
        let a = Admin::open(&path, Limits::default()).unwrap();
        assert_eq!(a.snapshot().unwrap(), state.view);
        assert_eq!(a.catalog().unwrap(), (0, vec![]));
        // Read-only attach has not changed any logical record; raw redb open may maintain its own header.
        let t = a.inner.db(LEGACY_STORAGE).unwrap().begin_read().unwrap();
        let table = t.open_table(RECORDS).unwrap();
        let m: Metadata = decode(table.get(&[0u8][..]).unwrap().unwrap().value()).unwrap();
        assert_eq!(m.format, 1);
        drop(table);
        drop(t);
        let preserved = fresh("legacy-original-redb");
        fs::write(&preserved, &original).unwrap();
        let mut s = a.sql_session();
        s.execute(
            "CREATE TABLE application (id BIGINT PRIMARY KEY)",
            &[],
            None,
        )
        .unwrap();
        s.execute("INSERT INTO application VALUES(-1)", &[], None)
            .unwrap();
        let t = a.inner.db(LEGACY_STORAGE).unwrap().begin_read().unwrap();
        let table = t.open_table(RECORDS).unwrap();
        let m: Metadata = decode(table.get(&[0u8][..]).unwrap().unwrap().value()).unwrap();
        assert_eq!(m.format, 3);
        assert_ne!(m.format, 1, "v1 reader rejects before decoding namespace5");
        drop(table);
        drop(t);
        assert_eq!(a.snapshot().unwrap(), state.view);
        drop(s);
        drop(a);
        let a = Admin::open(path, Limits::default()).unwrap();
        assert_eq!(
            a.sql_session()
                .execute("SELECT * FROM application WHERE id=-1", &[], None)
                .unwrap()
                .rows[0]
                .values,
            vec![relational::Value::Int(-1)]
        );
        assert_eq!(fs::read(legacy_export).unwrap(), bytes);
        assert_eq!(fs::read(preserved).unwrap(), original);
    }
    #[test]
    #[cfg(feature = "legacy-task")]
    fn full_envelope_is_reserved_and_every_accepted_boundary_state_exports() {
        let a = Admin::create(fresh("size-source"), Limits::default()).unwrap();
        let g = Grant {
            task: TaskId(1),
            issuer: Principal(1),
            executor: Principal(2),
            approver: Principal(3),
            policy: Policy {
                tenant: TenantId(42),
                columns: std::collections::BTreeSet::from([Column::Status]),
                transitions: std::collections::BTreeSet::from([(Status::Failed, Status::Queued)]),
                incident: (100, 107),
                distinct_limit: 25,
                transition_limit: 25,
                approval_above: 5,
                valid_from: 1,
                expires: 1000,
            },
        };
        a.grant(g).unwrap();
        let mut state = a.inner.read().unwrap();
        state.view.operations.insert(
            (TaskId(1), OperationId(1)),
            BoundDecision {
                payload: vec![],
                decision: Err(Error::Scope),
            },
        );
        let base = encode(&records(&state).unwrap()).unwrap().len();
        state
            .view
            .operations
            .get_mut(&(TaskId(1), OperationId(1)))
            .unwrap()
            .payload = vec![7; MAX_BYTES as usize - base];
        let values = records(&state).unwrap();
        assert_eq!(encode(&values).unwrap().len() as u64, MAX_BYTES);
        let db = Admin::create_state(
            fresh("size-boundary"),
            Limits::default(),
            state.clone(),
            RecoveryPoint::None,
        )
        .unwrap();
        let file = fresh("size-export");
        db.export(&file).unwrap();
        assert_eq!(Admin::verify_export(file).unwrap(), state.view);
        state
            .view
            .operations
            .get_mut(&(TaskId(1), OperationId(1)))
            .unwrap()
            .payload
            .push(0);
        assert_eq!(records(&state).unwrap_err(), CoreError::Limit);
        let mut too_large = values;
        too_large.insert(
            key(3, &[1, 1]),
            encode(&state.view.operations[&(TaskId(1), OperationId(1))]).unwrap(),
        );
        assert!(matches!(from_records(too_large), Err(CoreError::Limit)));
    }
    #[test]
    fn admission_counter_at_maximum_rejects_without_overflow_or_poison() {
        let a = Admin::create(
            fresh("max-admission"),
            Limits {
                admitted: usize::MAX,
                selected_keys: 1,
            },
        )
        .unwrap();
        a.inner.admitted.store(usize::MAX, Ordering::Release);
        assert_eq!(a.snapshot(), Err(CoreError::Busy));
        a.inner.admitted.store(0, Ordering::Release);
        assert!(a.snapshot().is_ok());
        a.inner.sql_operations.store(usize::MAX, Ordering::Release);
        assert_eq!(
            a.sql_session().execute("BEGIN", &[], None),
            Err(CoreError::Busy)
        );
        a.inner.sql_operations.store(0, Ordering::Release);
        let mut session = a.sql_session();
        session.execute("BEGIN", &[], None).unwrap();
        session.execute("ROLLBACK", &[], None).unwrap();
    }
    #[test]
    fn ordered_namespace_tuple_encoding() {
        assert!(key(1, &[42, 1]) < key(1, &[42, 256]));
        assert!(key(1, &[42, u64::MAX]) < key(1, &[43, 0]));
        assert!(key(1, &[u64::MAX, u64::MAX]) < key(2, &[0]));
        assert_eq!(key(1, &[42, 9]).len(), 17);
    }
    #[test]
    fn malformed_version_namespace_record_and_metadata_reject() {
        let base = records(&empty(123)).unwrap();
        for mode in 0..5 {
            let mut r = base.clone();
            match mode {
                0 => {
                    r.insert(
                        vec![0],
                        encode(&Metadata {
                            format: 99,
                            domain: 123,
                            schema: 0,
                            last_time: 0,
                            next_approval: 0,
                            origin: None,
                        })
                        .unwrap(),
                    );
                }
                1 => {
                    r.insert(vec![99], vec![0]);
                }
                2 => {
                    r.insert(vec![1], vec![0]);
                }
                3 => {
                    r.insert(key(1, &[42, 0]), vec![255]);
                }
                4 => {
                    r.insert(vec![0], vec![]);
                }
                _ => unreachable!(),
            }
            assert!(from_records(r).is_err());
        }
        assert!(decode::<Vec<u64>>(&u64::MAX.to_be_bytes()).is_err());
    }
    #[test]
    fn canonical_export_and_authority_invariants_reject_even_with_valid_digest() {
        let mut values = records(&empty(5)).unwrap();
        values.insert(vec![99], vec![1]);
        let payload = encode(&values).unwrap();
        let path = fresh("forged-export");
        let mut bytes = MAGIC.to_vec();
        bytes.extend(FORMAT.to_be_bytes());
        bytes.extend((payload.len() as u64).to_be_bytes());
        bytes.extend(Sha256::digest(&payload));
        bytes.extend(payload);
        fs::write(&path, bytes).unwrap();
        assert!(matches!(
            Admin::verify_export(path),
            Err(CoreError::Corrupt(_))
        ));
    }
    #[test]
    fn admission_is_bounded_and_does_not_queue_unlimited_work() {
        let a = Admin::create(
            fresh("admission"),
            Limits {
                admitted: 1,
                selected_keys: 1,
            },
        )
        .unwrap();
        let ticket = a.inner.enter().unwrap();
        assert_eq!(a.snapshot(), Err(CoreError::Busy));
        drop(ticket);
        assert!(a.snapshot().is_ok());
    }
    #[test]
    fn invalid_limits_leave_destination_uncreated() {
        let path = fresh("invalid-limits");
        assert!(matches!(
            Admin::create(
                &path,
                Limits {
                    admitted: 0,
                    selected_keys: 1
                }
            ),
            Err(CoreError::Limit)
        ));
        assert!(!path.exists());
    }
    #[test]
    fn record_corruption_fences_live_handle_and_open_rejects() {
        let path = fresh("corrupt-record");
        let a = Admin::create(&path, Limits::default()).unwrap();
        let tx = a.inner.db(LEGACY_STORAGE).unwrap().begin_write().unwrap();
        {
            let mut table = tx.open_table(RECORDS).unwrap();
            table.insert(&[0u8][..], &[0u8][..]).unwrap();
        }
        tx.commit().unwrap();
        assert!(matches!(a.snapshot(), Err(CoreError::Corrupt(_))));
        assert_eq!(a.snapshot(), Err(CoreError::Fenced));
        drop(a);
        assert!(Admin::open(path, Limits::default()).is_err());
    }
}

#[cfg(all(test, feature = "engine-rocksdb"))]
mod fk_target_plumbing_tests;

/// Reuse the transaction's existing savepoint lifetime allocation and
/// registration. An explicit statement does one acquire load to validate it.
pub(crate) enum SessionLease {
    Fresh(EngineLease),
    Transaction(Arc<engine::txn::TxnLifetime>),
}

impl SessionLease {
    pub(crate) fn into_holder(self, kind: Holder) -> Result<EngineLease, CoreError> {
        match self {
            Self::Fresh(lease) => Ok(lease),
            Self::Transaction(lifetime) => lifetime.lease.as_ref().expect("leased transaction").child(kind),
        }
    }
}

impl From<EngineLease> for SessionLease {
    fn from(lease: EngineLease) -> Self {
        Self::Fresh(lease)
    }
}

impl std::ops::Deref for SessionLease {
    type Target = EngineLease;
    fn deref(&self) -> &EngineLease {
        match self {
            Self::Fresh(lease) => lease,
            Self::Transaction(lifetime) => lifetime.lease.as_ref().expect("leased transaction"),
        }
    }
}

/// A callback uses the transaction pin when one exists. A callback in a
/// statement without a transaction registers only if it is actually invoked.
/// The tag makes a retained callback permanently local to its original engine.
enum CallbackLease {
    Pinned(SessionLease),
    Tagged(GenerationTag),
}

impl CallbackLease {
    fn new(db: &EngineLease, txn: Option<&Transaction>) -> Self {
        match txn.and_then(Transaction::engine_lifetime) {
            Some(lifetime) => Self::Pinned(SessionLease::Transaction(Arc::clone(lifetime))),
            None => Self::Tagged(db.tag()),
        }
    }

    fn lease(&self, kind: Holder) -> Result<CallbackEngine<'_>, CoreError> {
        match self {
            Self::Pinned(db) => {
                db.check()?;
                Ok(CallbackEngine::Borrowed(db))
            }
            Self::Tagged(tag) => Ok(CallbackEngine::Owned(tag.lease(kind)?)),
        }
    }
}

#[cfg(test)]
impl From<EngineLease> for CallbackLease {
    fn from(db: EngineLease) -> Self {
        Self::Pinned(db.into())
    }
}

enum CallbackEngine<'a> {
    Borrowed(&'a EngineLease),
    Owned(EngineLease),
}

impl std::ops::Deref for CallbackEngine<'_> {
    type Target = EngineLease;
    fn deref(&self) -> &EngineLease {
        match self {
            Self::Borrowed(db) => db,
            Self::Owned(db) => db,
        }
    }
}

/// A protected run: the caller-owned transaction plus the row effects its
/// statements produced (design section 6, D-C).
// Constructed by `protected_commit` (R3-610); until it lands only tests do.
#[cfg_attr(not(all(test, feature = "engine-rocksdb")), allow(dead_code))]
pub(crate) struct ProtectedRun {
    pub(crate) txn: engine::txn::Transaction,
    pub(crate) effects: crate::task::effects::Effects,
}

pub(crate) use v2_sql::{cross_check_effects, task_restore_statement};

impl SqlSession {
    pub(crate) fn statement_lease(&self) -> Result<Option<SessionLease>, CoreError> {
        if self.is_v2_engine() { self.v2_lease(Holder::Sql).map(Some) } else { Ok(None) }
    }

    fn execution_lease(&self, p: &relational::Prepared) -> Result<Option<SessionLease>, CoreError> {
        match self.statement_lease() {
            Err(CoreError::Fenced) if p.control() == Some("rollback") => Ok(None),
            result => result,
        }
    }

    fn check_prepared_generation(p: &relational::Prepared, db: Option<&EngineLease>) -> Result<(), CoreError> {
        // Other domains follow the normal validation and statement error cleanup.
        if let Some(db) = db
            && p.engine_generation.is_some_and(|prepared| {
                let current = db.prepared_generation();
                prepared.slot == current.slot && prepared.stamp != current.stamp
            })
            && p.domain == u128::from_be_bytes(db.meta.domain)
            && p.control().is_none()
        {
            return Err(CoreError::Fenced);
        }
        Ok(())
    }

    pub(crate) fn v2_lease(&self, kind: Holder) -> Result<SessionLease, CoreError> {
        if let Some(lifetime) = self.v2_transaction.as_ref()
            .or_else(|| self.protected.as_ref().map(|run| &run.txn))
            .and_then(Transaction::engine_lifetime)
        {
            if self.native_generation.as_ref().is_none_or(|active| !Arc::ptr_eq(active, lifetime)) {
                lifetime.lease.as_ref().expect("leased transaction").check()?;
            }
            return Ok(SessionLease::Transaction(Arc::clone(lifetime)));
        }
        if let Some(generation) = &self.task_generation {
            return Ok(SessionLease::Fresh(generation.lease(kind)?));
        }
        self.admin.lease(kind)?.map(SessionLease::Fresh).ok_or(CoreError::Unsupported)
    }

    pub(crate) fn pin_native_generation(&mut self, already_admitted: bool) -> Result<(), CoreError> {
        let Some(lifetime) = self.v2_transaction.as_ref().and_then(Transaction::engine_lifetime) else {
            return Ok(());
        };
        if self.native_generation.as_ref().is_some_and(|active| Arc::ptr_eq(active, lifetime)) {
            return Ok(());
        }
        if !already_admitted {
            lifetime.lease.as_ref().expect("leased transaction").check()?;
        }
        self.native_generation = Some(Arc::clone(lifetime));
        Ok(())
    }

    fn bind_sequence_generation(&mut self, db: &EngineLease) {
        if self.sequence_generation.as_ref().is_none_or(|tag| tag.stamp() != db.stamp()) {
            // Old records can retain their own Arcs. Give the new generation
            // separate session storage so a late callback cannot mutate it.
            if self.sequence_generation.is_some() {
                self.sequence_values = Default::default();
                self.sequence_unsettled = Default::default();
                self.pending_sequences = Default::default();
                self.xact = Default::default();
                self.unnamed_plans.clear();
            }
            self.sequence_generation = Some(db.tag());
        }
    }

    /// The armed commit crash point and protected-commit seam (R3-619).
    pub(crate) fn fault_points(
        &self,
    ) -> Result<
        (
            CrashPoint,
            Option<crate::task::commit::ProtectedPoint>,
            Option<crate::task::commit::ProtectedHook>,
        ),
        CoreError,
    > {
        let inner = &self.admin.inner;
        let crash = *inner.crash.lock().map_err(|_| CoreError::Fenced)?;
        let point = *inner.protected_point.lock().map_err(|_| CoreError::Fenced)?;
        let hook = inner.protected_hook.lock().map_err(|_| CoreError::Fenced)?.clone();
        Ok((crash, point, hook))
    }
}

#[cfg(test)]
#[path = "storage/column_collation_format_tests.rs"]
mod column_collation_format_tests;

#[cfg(all(test, feature = "engine-rocksdb"))]
mod native_many_tests;

#[cfg(test)]
mod w1e_laziness {
    use super::*;
    #[test]
    fn no_inquiry_and_null_only_capture_nothing() {
        for engine in [crate::Engine::Legacy, #[cfg(feature = "engine-rocksdb")] crate::Engine::V2] {
        let path = std::path::PathBuf::from(std::env::var_os("TEMP").unwrap()).join(format!("w1e-lazy-{engine:?}-{}", std::process::id()));
        let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
        let mut session = admin.sql_session();
        session.execute("CREATE TABLE t(id INT PRIMARY KEY)", &[], None).unwrap();
        session.execute("INSERT INTO t VALUES (1),(2)", &[], None).unwrap();
        let clock = relational::StatementClock { unavailable: None, transaction_us: 0, statement_us: 0, zone: None, wall: None };
        for sql in ["SELECT id FROM t", "SELECT pg_has_role(NULL,'MEMBER') FROM t"] {
            let before = PRIVILEGE_CAPTURES.get();
            session.with_privilege_callback(Arc::new(|_,_,_| Ok(Some(true))), |s| s.execute_at(sql, &[], None, &clock)).unwrap();
            assert_eq!(PRIVILEGE_CAPTURES.get(), before, "{sql}");
        }
        let before = PRIVILEGE_CAPTURES.get();
        session.with_privilege_callback(Arc::new(|_,_,_| Ok(Some(true))), |s| s.execute_at("SELECT pg_has_role('giggles','MEMBER') FROM t", &[], None, &clock)).unwrap();
        assert_eq!(PRIVILEGE_CAPTURES.get(), before + 1);
        drop(session); drop(admin); std::fs::remove_dir_all(path).unwrap();
        }
    }
}

#[cfg(test)]
#[path = "storage/copy_freeze_tests.rs"]
mod copy_freeze_tests;

#[cfg(test)]
mod sequence_owners_tests;
