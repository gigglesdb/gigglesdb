use crate::*;
mod catalog_inspection;
mod cursor;
mod drop_table;
mod engine;
mod feature_envelope;
mod format_policy;
mod legacy_open;
pub(crate) mod native_once;
pub(crate) mod operation_adapter;
pub mod replica;
pub mod roles;
mod secondary_index;
mod secondary_index_catalog;
mod secondary_index_public;
mod sequence_counters;
pub(crate) mod sequences;
mod upgrade;
mod v2;
pub use upgrade::{UpgradePoint, UpgradeReport};
mod copy_input;
#[cfg(feature = "engine-rocksdb")]
mod v2_export;
mod v2_sql;
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
    Fenced,
    Cancelled,
    Limit,
    Policy(Error),
    Sql(relational::SqlError),
    /// Schema DDL error outside the frozen, persisted scalar-error enum.
    DuplicateSchema,
}
impl std::fmt::Display for CoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for CoreError {}
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
    pub applications: Option<replica::application::Ledger>,
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
}
struct MetadataDomain {
    domain: u128,
}
struct MetadataSql<'a> {
    image: &'a relational::Relational,
    epoch: u64,
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
        metadata
    }
    fn user_type_catalog(&self) -> relational::UserTypeCatalog {
        let mut metadata = self.image.user_type_catalog();
        metadata.epoch = self.epoch;
        metadata
    }
    fn catalog_presentation(
        &self,
    ) -> Result<relational::CatalogPresentation, relational::SqlError> {
        let mut metadata = self.image.catalog_presentation()?;
        metadata.epoch = self.epoch;
        Ok(metadata)
    }
}
impl<'a> MetadataState<'a> {
    fn new(image: &'a relational::Relational, epoch: u64, domain: u128) -> Self {
        Self {
            sql: MetadataSql { image, epoch },
            view: MetadataDomain { domain },
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
fn empty(domain: u128) -> State {
    State {
        seq: BTreeMap::new(),
        applications: None,
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
    if let Some(ledger) = &s.applications {
        out.insert(vec![9], ledger.bytes()?);
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
        || s.applications
            .as_ref()
            .is_some_and(|a| a.contains_temporal())
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
    let (base_version, _) = format_policy::heap_base(legacy).ok_or(CoreError::Unsupported)?;
    secondary_index::gate(&records, indexed, applications)?;
    let mut s = empty(m.domain);
    match records.get(&vec![9]) {
        Some(v) if applications => {
            s.applications = Some(replica::application::Ledger::from_bytes(v)?)
        }
        None if !applications => {}
        _ => {
            return Err(CoreError::Corrupt(
                "application format/namespace mismatch".into(),
            ));
        }
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
            || s.applications.as_ref().is_some_and(|a| a.contains_typed())
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
            Some(9) if k.len() == 1 && applications => {}
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
    if let Some(ledger) = &s.applications {
        ledger.validate()?;
    }
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
const TASK_EXECUTORS: &str = "task executors";
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
enum Backend {
    Legacy(Database),
    // F8 executes SQL through this handle; until then only tests read it.
    V2(#[allow(dead_code)] Arc<v2::V2Db>),
}
struct Inner {
    backend: Backend,
    gate: Mutex<()>,
    admitted: AtomicUsize,
    sql_transactions: AtomicUsize,
    sql_operations: AtomicUsize,
    fenced: AtomicBool,
    limits: Limits,
    crash: Mutex<CrashPoint>,
    decision_hook: Mutex<Option<DecisionHook>>,
    sequences: Mutex<sequences::Live>,
    native_once_owners: Mutex<BTreeMap<native_once::NativeOnceKey, native_once::InFlight>>,
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
struct Ticket<'a>(&'a AtomicUsize);
impl Drop for Ticket<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}
impl Inner {
    fn db(&self, feature: &'static str) -> Result<&Database, CoreError> {
        match &self.backend {
            Backend::Legacy(db) => Ok(db),
            Backend::V2(_) => Err(CoreError::EngineUnsupported {
                engine: "v2",
                feature,
            }),
        }
    }

    /// Inspect metadata only. Native/task operations must keep using `db(feature)`.
    fn inspect_metadata<T>(
        &self,
        f: impl FnOnce(&MetadataState<'_>) -> Result<T, CoreError>,
    ) -> Result<T, CoreError> {
        match &self.backend {
            Backend::Legacy(_) => self.inspect(|state| f(&MetadataState::borrow(state))),
            Backend::V2(db) => {
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
        Ok(Ticket(&self.sql_operations))
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
        Ok(Ticket(&self.admitted))
    }
    fn read(&self) -> Result<State, CoreError> {
        let db = self.db(LEGACY_STORAGE)?;
        let out = legacy_redb("reading records", || {
            let tx = db.begin_read().map_err(io)?;
            let t = tx.open_table(RECORDS).map_err(io)?;
            let mut out = BTreeMap::new();
            let mut total = 0u64;
            for item in t.iter().map_err(io)? {
                let (k, v) = item.map_err(io)?;
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
        let _guard = crate::cancellation::lock_notified(&self.gate, || {
            self.decision_point(DecisionPoint::WaitingLock)
        })?;
        if self.fenced.load(Ordering::Acquire) {
            return Err(CoreError::Fenced);
        }
        let s = self.read().inspect_err(|_| {
            self.fenced.store(true, Ordering::Release);
        })?;
        f(&s)
    }
    fn change<T>(
        &self,
        f: impl FnOnce(&mut State) -> Result<T, CoreError>,
    ) -> Result<T, CoreError> {
        self.change_inner(false, false, f)
    }
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
        let _guard = crate::cancellation::lock_notified(&self.gate, || {
            self.decision_point(DecisionPoint::WaitingLock)
        })?;
        if self.fenced.load(Ordering::Acquire) {
            return Err(CoreError::Fenced);
        }
        let mut s = self.read().inspect_err(|_| {
            self.fenced.store(true, Ordering::Release);
        })?;
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
    fn change_accounted<T>(
        &self,
        decision: bool,
        admission: impl FnOnce(&mut State) -> Result<(), CoreError>,
        work: impl FnOnce(&mut State) -> Result<T, CoreError>,
    ) -> Result<T, CoreError> {
        let _ticket = self.enter()?;
        let _guard = crate::cancellation::lock(&self.gate)?;
        if self.fenced.load(Ordering::Acquire) {
            return Err(CoreError::Fenced);
        }
        let mut s = self.read().inspect_err(|_| {
            self.fenced.store(true, Ordering::Release);
        })?;
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
            let mut tx = self.db(LEGACY_STORAGE)?.begin_write().map_err(io)?;
            tx.set_durability(Durability::Immediate).map_err(io)?;
            {
                let mut t = tx.open_table(RECORDS).map_err(io)?;
                if !values.contains_key(&vec![8]) {
                    t.remove(&[8][..]).map_err(io)?;
                }
                if !values.contains_key(&vec![10]) {
                    t.remove(&[10][..]).map_err(io)?;
                }
                let stale: Vec<Vec<u8>> = t
                    .iter()
                    .map_err(io)?
                    .map(|item| item.map(|(key, _)| key.value().to_vec()).map_err(io))
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter()
                    .filter(|key| key.first() == Some(&11) && !values.contains_key(key))
                    .collect();
                for key in stale {
                    t.remove(key.as_slice()).map_err(io)?;
                }
                for (k, v) in &values {
                    t.insert(k.as_slice(), v.as_slice()).map_err(io)?;
                }
            }
            roles::rename_tables(&tx, &renames)?;
            roles::forget_relations(&tx, &forgotten)?;
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
            tx.commit().map_err(io)?;
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
            if matches!(&e, GuardedPersistError::Core(error) if *error != CoreError::Cancelled) {
                self.fenced.store(true, Ordering::Release);
            }
            return Err(e);
        }
        Ok(())
    }
    fn request(&self, r: &Request) -> Result<(), CoreError> {
        if let Selection::Keys(k) = &r.selection
            && k.len() > self.limits.selected_keys
        {
            return Err(CoreError::Limit);
        }
        Ok(())
    }
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
    inner: Arc<Inner>,
}
#[derive(Clone)]
pub struct Executor {
    inner: Arc<Inner>,
    credential: Credential,
}
pub use v2::reclaim::ReclaimStats;

impl Admin {
    /// One-time trusted provisioning. A task's counters and caps cannot be reset.
    /// Missing provisioning (including legacy formats) denies remote disclosure.
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
    pub fn task_resources(&self, task: TaskId) -> Result<Option<TaskAccounting>, CoreError> {
        self.inner.db(TASK_EXECUTORS)?;
        self.inner.inspect(|s| Ok(s.accounting.get(&task).copied()))
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
        Ok(Self {
            inner: Arc::new(Inner {
                backend,
                gate: Mutex::new(()),
                admitted: AtomicUsize::new(0),
                sql_transactions: AtomicUsize::new(0),
                sql_operations: AtomicUsize::new(0),
                fenced: AtomicBool::new(false),
                limits,
                crash: Mutex::new(CrashPoint::None),
                decision_hook: Mutex::new(None),
                sequences: Mutex::new(sequences::Live::default()),
                native_once_owners: Mutex::new(BTreeMap::new()),
            }),
        })
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
                    Self::attach_backend(
                        Backend::V2(Arc::new(v2::V2Db::create(path.as_ref())?)),
                        limits,
                    )
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
        Self::attach(db, limits)
    }
    /// Existing database only: no initialization or replacement on damage.
    pub fn open(path: impl AsRef<Path>, limits: Limits) -> Result<Self, CoreError> {
        upgrade::check_startup(path.as_ref())?;
        if v2::has_traces(path.as_ref()) {
            match engine::meta::classify_dir(path.as_ref()).map_err(v2::format_error)? {
                engine::meta::DirClass::V2 => {
                    return Self::attach_backend(
                        Backend::V2(Arc::new(v2::V2Db::open(path.as_ref())?)),
                        limits,
                    );
                }
                engine::meta::DirClass::Legacy => {}
                engine::meta::DirClass::Empty => {
                    return Err(CoreError::Corrupt("incomplete v2 directory".into()));
                }
            }
        }
        Self::open_legacy(path.as_ref(), limits, true)
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
    pub(crate) fn engine(&self) -> Engine {
        match &self.inner.backend {
            Backend::Legacy(_) => Engine::Legacy,
            Backend::V2(_) => Engine::V2,
        }
    }
    pub fn authority_metadata(&self) -> Result<(u128, u64, u64, Option<u128>), CoreError> {
        // V2 has a durable SQL authority domain, but no native approval clock/ledger.
        if let Backend::V2(db) = &self.inner.backend {
            let _ticket = self.inner.enter()?;
            return Ok((
                u128::from_be_bytes(db.meta.domain),
                0,
                0,
                db.origin_at(db.sequencer.visible_ts())?,
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
        if let Backend::V2(db) = &self.inner.backend {
            let _ticket = self.inner.enter()?;
            return v2_export::export(db, path.as_ref(), point);
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
        if let Some(ledger) = &mut s.applications {
            ledger.revoke_all();
        }
        for t in s.view.tasks.values_mut() {
            t.revoked = true;
        }
        for p in s.view.approvals.values_mut() {
            p.revoked = true;
        }
        Self::create_state(path, limits, s, point)
    }
}
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
/// Trusted session only. Dropping a session discards its staged transaction.
/// No lock or admission ticket lives across client calls.
pub struct SqlSession {
    admin: Admin,
    staged: Option<State>,
    v2_transaction: Option<engine::txn::Transaction>,
    copy_input: Option<CopyInput>,
    savepoints: Vec<SqlSavepoint>,
    v2_savepoints: Vec<(String, engine::txn::Savepoint, relational::DeferralState)>,
    savepoint_budget: u64,
    aborted: bool,
    transaction_id: Option<u128>,
    transaction_clock_us: Option<i64>,
    extra_float_digits: i32,
    search_path: Vec<String>,
    schema_user: String,
    authority_managed: bool,
    ddl_owner: Option<String>,
    staged_revision: u64,
    sequence_values: sequences::SessionValues,
    pending_sequences: sequences::Pending,
    sequence_baseline: BTreeMap<u64, u128>,
    sequence_tables: Option<std::collections::BTreeSet<String>>,
    deferral: relational::DeferralState,
}
mod complete_relation;
pub(crate) use complete_relation::CompleteRelation;

impl Admin {
    pub fn sql_session(&self) -> SqlSession {
        SqlSession {
            admin: self.clone(),
            staged: None,
            v2_transaction: None,
            copy_input: None,
            savepoints: Vec::new(),
            v2_savepoints: Vec::new(),
            savepoint_budget: SAVEPOINT_BYTES,
            aborted: false,
            transaction_id: None,
            transaction_clock_us: None,
            extra_float_digits: 1,
            search_path: vec!["public".into()],
            schema_user: "giggles".into(),
            authority_managed: false,
            ddl_owner: None,
            staged_revision: 0,
            sequence_values: Default::default(),
            pending_sequences: Default::default(),
            sequence_baseline: BTreeMap::new(),
            sequence_tables: None,
            deferral: Default::default(),
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
/// One conversion point for the V2 catalog xid (physical milliseconds -> seconds).
fn catalog_xid_from_hlc(epoch: u64) -> u64 {
    (epoch >> 16) / 1000
}
impl SqlSession {
    fn inspect_metadata<T>(
        &self,
        f: impl FnOnce(&MetadataState<'_>) -> Result<T, CoreError>,
    ) -> Result<T, CoreError> {
        match &self.admin.inner.backend {
            Backend::Legacy(_) => match &self.staged {
                Some(state) => f(&MetadataState::borrow(state)),
                None => self.admin.inner.inspect(|state| f(&MetadataState::borrow(state))),
            },
            Backend::V2(db) => {
                let _ticket = self.admin.inner.enter()?;
                db.inspect_metadata(self.v2_transaction.as_ref(), f)
            }
        }
    }

    pub fn resolve_relation_reference(&self, name: &str) -> Result<String, CoreError> {
        let resolve = |s: &MetadataState<'_>| Ok(s.sql.resolve_relation_key(name, &self.search_path));
        match &self.staged {
            Some(s) => resolve(&MetadataState::borrow(s)),
            None => self.inspect_metadata(resolve),
        }
    }
    /// Bind dependency identities to this session without surfacing semantic errors
    /// before the caller has checked authority.
    pub fn bind_relation_context(
        &self,
        prepared: &relational::Prepared,
    ) -> Result<relational::Prepared, CoreError> {
        let mut prepared = prepared.clone();
        prepared.set_schema_context(self.search_path.clone(), self.schema_user.clone());
        let bind = |s: &MetadataState<'_>| {
            Ok(prepared
                .bind_schemas(&s.sql)
                .unwrap_or_else(|_| prepared.clone()))
        };
        match &self.staged {
            Some(s) => bind(&MetadataState::borrow(s)),
            None => self.inspect_metadata(bind),
        }
    }
    pub fn schema_in_search_path(&self, schema: &str) -> bool {
        self.search_path.iter().any(|s| s == schema)
    }
    pub fn set_search_path(&mut self, path: &str, user: &str) {
        self.schema_user = user.into();
        self.search_path = relational::schema_search_path(path, user);
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

    /// Configure the creator for atomic v2 DDL authority. Returns whether this
    /// engine maintains DDL authority itself; legacy callers retain bookkeeping.
    pub fn set_ddl_owner(&mut self, owner: Option<&str>) -> bool {
        self.ddl_owner = owner.map(str::to_owned);
        matches!(self.admin.inner.backend, Backend::V2(_))
    }

    /// Read authority including this session's uncommitted v2 DDL changes.
    pub fn table_owner(&self, table: &str) -> Result<Option<String>, roles::RoleError> {
        if let Some(txn) = &self.v2_transaction
            && let Some(owner) = roles::engine::own_owner(txn, table)?
        {
            return Ok(owner);
        }
        self.admin.roles().table_owner(table)
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
        self.admin.roles().table_privileges(table, role)
    }

    /// Set the precision used by SQL floating-point text conversions.
    pub fn set_extra_float_digits(&mut self, value: i32) {
        self.extra_float_digits = value;
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
        let owners = |s: &MetadataState<'_>| {
            Ok(s.sql
                .catalog_ext()
                .sequences
                .values()
                .map(|d| {
                    let owner = d.owned_by.and_then(|(id, _)| {
                        s.sql
                            .tables
                            .values()
                            .find(|t| t.id == id)
                            .map(|t| s.sql.table_key(t))
                    });
                    (d.name.clone(), owner)
                })
                .collect())
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
                self.inspect_metadata(describe)
            }
        })();
        if result.is_err() {
            self.abort();
        }
        result
    }
    /// Validate dependency-free immutable projection/order constants without executing SQL.
    pub fn validate_immutable_constants(
        &mut self,
        p: &relational::Prepared,
    ) -> Result<(), CoreError> {
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
                let mut metadata_plan = p.clone();
                if matches!(self.admin.inner.backend, Backend::V2(_)) {
                    metadata_plan.epoch = s.sql.image.epoch;
                }
                metadata_plan
                    .validate_immutable_constants(&s.sql)
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
    /// A gateway parse/bind/authorization failure poisons an explicit transaction.
    pub fn abort(&mut self) {
        self.abort_copy_input();
        if self.in_transaction() {
            self.aborted = true;
        }
    }
    pub fn scan(&mut self, scan: relational::Scan) -> Result<relational::SqlResult, CoreError> {
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
    pub(crate) fn begin_engine(&mut self) -> Result<(), CoreError> {
        self.admin.inner.db(SQL_SESSIONS)?;
        self.execute_engine(
            |domain, state| Ok(relational::Prepared::begin_engine(domain, state.epoch)),
            None,
        )
        .map(|_| ())
    }
    pub(crate) fn commit_engine(&mut self) -> Result<(), CoreError> {
        self.admin.inner.db(SQL_SESSIONS)?;
        self.execute_engine(
            |domain, state| Ok(relational::Prepared::commit_engine(domain, state.epoch)),
            None,
        )
        .map(|_| ())
    }
    /// Catalog binding sees exactly the staged snapshot used for typed reads/writes.
    pub(crate) fn catalog_engine(&mut self) -> Result<(u64, Vec<relational::TableDef>), CoreError> {
        self.admin.inner.db(SQL_SESSIONS)?;
        let result = (|| {
            if self.aborted {
                return Err(CoreError::Sql(relational::SqlError::Aborted));
            }
            crate::cancellation::check()?;
            let catalog = |s: &State| Ok((s.sql.epoch, s.sql.tables.values().cloned().collect()));
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
    /// Eligibility for synchronous safe catalog creates, from the exact active
    /// snapshot that will stage the definition. Empty existing tables are fine;
    /// business and retained task/decision state require the migration path.
    pub(crate) fn catalog_empty_engine(&mut self) -> Result<bool, CoreError> {
        self.admin.inner.db(SQL_SESSIONS)?;
        let result = (|| {
            if self.aborted {
                return Err(CoreError::Sql(relational::SqlError::Aborted));
            }
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
    pub(crate) fn create_table_engine(
        &mut self,
        name: String,
        columns: Vec<relational::ColumnDef>,
        keys: Vec<String>,
    ) -> Result<(), CoreError> {
        self.admin.inner.db(SQL_SESSIONS)?;
        // Do not permit this entry to accidentally autocommit outside the
        // caller's eligibility/definition snapshot and final decision fence.
        self.snapshot_identity_engine()?;
        self.execute_engine(
            |domain, state| relational::Prepared::create_engine(name, columns, keys, domain, state),
            None,
        )
        .map(|_| ())
    }
    /// Identity for binding against an active transaction's catalog snapshot.
    pub(crate) fn snapshot_identity_engine(&mut self) -> Result<(u128, u64), CoreError> {
        self.admin.inner.db(SQL_SESSIONS)?;
        let result = (|| {
            if self.aborted {
                return Err(CoreError::Sql(relational::SqlError::Aborted));
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
        self.admin.inner.db(SQL_SESSIONS)?;
        let result = (|| {
            if self.aborted {
                return Err(CoreError::Sql(relational::SqlError::Aborted));
            }
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
        let state = self
            .staged
            .as_ref()
            .ok_or(CoreError::Sql(relational::SqlError::Transaction))?;
        relational::coerce_to_column(&state.sql, column, value).map_err(CoreError::Sql)
    }
    pub(crate) fn insert_engine(
        &mut self,
        table: &relational::TableDef,
        values: Vec<relational::Value>,
    ) -> Result<relational::SqlResult, CoreError> {
        self.admin.inner.db(SQL_SESSIONS)?;
        self.execute_engine(
            |domain, state| relational::Prepared::insert_engine(table, values, domain, state),
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
        self.admin.inner.db(SQL_SESSIONS)?;
        self.execute_engine(
            |domain, state| relational::Prepared::delete_engine(table, key, domain, state),
            Some(expected_version),
        )
    }
    pub fn in_transaction(&self) -> bool {
        self.staged.is_some() || self.v2_transaction.is_some()
    }
    pub fn is_aborted(&self) -> bool {
        self.aborted
    }
    pub fn prepare(&mut self, sql: &str) -> Result<relational::Prepared, CoreError> {
        if let Backend::V2(db) = &self.admin.inner.backend {
            return self.prepare_v2(sql, u128::from_be_bytes(db.meta.domain));
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
        let inner = self.admin.inner.clone();
        let _ticket = inner.enter_sql()?;
        relational::assert_sql_frontend_allowed();
        let result = (|| {
            if self.aborted {
                return Err(CoreError::Sql(relational::SqlError::Aborted));
            }
            let mut copy = relational::Copy::parse(sql).map_err(CoreError::Sql)?;
            let resolve = |s: &MetadataState<'_>| Ok(copy.schema_key(&s.sql, &self.search_path));
            copy.table = match &self.staged {
                Some(s) => resolve(&MetadataState::borrow(s))?,
                None => self.inspect_metadata(resolve)?,
            };
            let prepare = |s: &MetadataState<'_>| {
                let mut p = copy
                    .prepared(&s.sql, s.view.domain)
                    .map_err(CoreError::Sql)?;
                p.set_schema_context(self.search_path.clone(), self.schema_user.clone());
                p.validate_schemas(&s.sql).map_err(CoreError::Sql)?;
                Ok(p.bind_schemas(&s.sql).unwrap_or(p))
            };
            let mut prepared = match &self.staged {
                Some(s) => prepare(&MetadataState::borrow(s))?,
                None => self.inspect_metadata(prepare)?,
            };
            if matches!(self.admin.inner.backend, Backend::V2(_)) {
                prepared.epoch = 0;
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
    pub fn copy_columns(
        &mut self,
        copy: &relational::Copy,
    ) -> Result<Vec<relational::ColumnDef>, CoreError> {
        let inner = self.admin.inner.clone();
        let _ticket = inner.enter_sql()?;
        relational::assert_sql_frontend_allowed();
        let columns = |s: &MetadataState<'_>| copy.columns_in(&s.sql).map_err(CoreError::Sql);
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
        &mut self,
        prepared: &relational::Prepared,
        rows: Vec<Vec<relational::Value>>,
        clock: &relational::StatementClock,
    ) -> Result<(relational::Prepared, Vec<Vec<relational::Value>>), CoreError> {
        relational::assert_sql_frontend_allowed();
        if let Backend::V2(db) = &self.admin.inner.backend {
            let db = db.clone();
            return self.normalize_copy_v2(&db, prepared, rows, clock);
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
                None => self.admin.inner.inspect(|s| Ok(s.clone()))?,
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
                .with_extra_float_digits(self.extra_float_digits)
                .with_sequences(self.admin.inner.sequence_callback(
                    state.clone(),
                    baseline,
                    self.pending_sequences.clone(),
                    self.sequence_values.clone(),
                    self.sequence_tables.clone(),
                    self.search_path.clone(),
                ));
            let output = state.sql.normalize_copy(prepared, rows, Some(&context));
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
        let context = relational::clock::ExecutionClock::new(clock);
        let prepared = match prepared.copy_rows(rows, Some(&context)) {
            Ok(p) => p,
            Err(e) => {
                self.abort();
                return Err(CoreError::Sql(e));
            }
        };
        self.execute_prepared_at(&prepared, &[], None, clock)
    }
    pub fn execute(
        &mut self,
        sql: &str,
        params: &[relational::Value],
        expected_version: Option<u64>,
    ) -> Result<relational::SqlResult, CoreError> {
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
                self.rollback();
                return Ok(Default::default());
            }
        }
        let p = self.prepare(sql)?;
        self.execute_prepared(&p, params, expected_version)
    }
    /// Execute using caller-supplied transaction, statement and wall time.
    pub fn execute_at(
        &mut self,
        sql: &str,
        params: &[relational::Value],
        expected_version: Option<u64>,
        clock: &relational::StatementClock,
    ) -> Result<relational::SqlResult, CoreError> {
        relational::assert_sql_frontend_allowed();
        self.guard_clock(clock)?;
        let p = self.prepare(sql)?;
        self.execute_prepared_at(&p, params, expected_version, clock)
    }
    pub fn execute_prepared_at(
        &mut self,
        p: &relational::Prepared,
        params: &[relational::Value],
        expected_version: Option<u64>,
        clock: &relational::StatementClock,
    ) -> Result<relational::SqlResult, CoreError> {
        relational::assert_sql_frontend_allowed();
        self.guard_clock(clock)?;
        let context = relational::clock::ExecutionClock::new(clock)
            .with_extra_float_digits(self.extra_float_digits);
        let result = self.execute_bound_clocked(p, params, expected_version, Some(&context));
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
        relational::assert_sql_frontend_allowed();
        self.execute_bound_engine(p, params, expected_version)
    }
    // Both frontends enter this kernel after constructing typed operations.
    // Keep SQL's public prepared-call boundary separate so native execution
    // cannot acquire a dependency on SQL routine dispatch or frontend policy.
    fn execute_bound_engine(
        &mut self,
        p: &relational::Prepared,
        params: &[relational::Value],
        expected_version: Option<u64>,
    ) -> Result<relational::SqlResult, CoreError> {
        self.execute_bound_clocked(p, params, expected_version, None)
    }
    fn execute_bound_clocked(
        &mut self,
        p: &relational::Prepared,
        params: &[relational::Value],
        expected_version: Option<u64>,
        clock: Option<&relational::clock::ExecutionClock>,
    ) -> Result<relational::SqlResult, CoreError> {
        if p.schema_context_differs(&self.search_path, &self.schema_user) {
            let mut bound = p.clone();
            bound.set_schema_context(self.search_path.clone(), self.schema_user.clone());
            return self.execute_bound_clocked(&bound, params, expected_version, clock);
        }
        if let Backend::V2(db) = &self.admin.inner.backend {
            let db = db.clone();
            return self.execute_v2(&db, p, params, expected_version, clock);
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
            .and_then(|()| self.run(p, params, expected_version, clock))
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
                            if n < relational::MAX_TRANSACTIONS {
                                Some(n + 1)
                            } else {
                                None
                            }
                        })
                        .map_err(|_| CoreError::Busy)?;
                    match self.admin.inner.inspect(|s| Ok(s.clone())) {
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
            None => self.admin.inner.inspect(|s| Ok(s.clone()))?,
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
            clock.with_sequences(self.admin.inner.sequence_callback(
                s.clone(),
                baseline,
                self.pending_sequences.clone(),
                self.sequence_values.clone(),
                self.sequence_tables.clone(),
                self.search_path.clone(),
            ))
        });
        let clock = context.as_ref();
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
            self.staged = Some(s);
            self.deferral = deferral;
        } else if !p.is_read() {
            self.publish(s)?;
        }
        Ok(result)
    }
    /// Check the session's staged relation namespace, or the committed catalog.
    pub fn relation_exists(&self, name: &str) -> Result<bool, CoreError> {
        let exists = |state: &MetadataState<'_>| {
            state.sql.tables.contains_key(name)
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
        self.copy_input = None;
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

            let mut sessions: Vec<_> = (0..relational::MAX_TRANSACTIONS)
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
        session.commit_engine().unwrap();
        session.begin_engine().unwrap();
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
