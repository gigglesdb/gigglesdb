use crate::*;
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
const FORMAT: u32 = 1;
const MAX_BYTES: u64 = 16 * 1024 * 1024;
const MAGIC: &[u8; 8] = b"GDEXP001";
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CoreError {
    Storage(String),
    Corrupt(String),
    Unsupported,
    Exists,
    Busy,
    Fenced,
    Limit,
    Policy(Error),
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
}
/// Trusted administrative recovery test seams; never controlled by environment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecoveryPoint {
    None,
    ExportWrite,
    ExportSync,
    ExportVerify,
    ExportPublish,
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
    pub view: Snapshot,
    pub last_time: u64,
    pub next_approval: u64,
    pub origin: Option<u128>,
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
    let mut out = BTreeMap::new();
    out.insert(
        vec![0],
        encode(&Metadata {
            format: FORMAT,
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
    envelope_bound(&records)?;
    let m: Metadata = decode(
        records
            .get(&vec![0])
            .ok_or_else(|| CoreError::Corrupt("missing metadata".into()))?,
    )?;
    if m.format != FORMAT {
        return Err(CoreError::Unsupported);
    }
    let mut s = empty(m.domain);
    s.view.schema = m.schema;
    s.last_time = m.last_time;
    s.next_approval = m.next_approval;
    s.origin = m.origin;
    for (k, v) in records {
        let num = |at: usize| -> Result<u64, CoreError> {
            let b = k
                .get(at..at + 8)
                .ok_or_else(|| CoreError::Corrupt("short key".into()))?;
            Ok(u64::from_be_bytes(b.try_into().expect("length")))
        };
        match k.first() {
            Some(0) if k.len() == 1 => {}
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
    Ok(s)
}
fn validate(s: &State) -> Result<(), CoreError> {
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
struct Inner {
    db: Database,
    gate: Mutex<()>,
    admitted: AtomicUsize,
    fenced: AtomicBool,
    limits: Limits,
    crash: Mutex<CrashPoint>,
}
struct Ticket<'a>(&'a AtomicUsize);
impl Drop for Ticket<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}
impl Inner {
    fn enter(&self) -> Result<Ticket<'_>, CoreError> {
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
        let tx = self.db.begin_read().map_err(io)?;
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
        from_records(out)
    }
    fn inspect<T>(&self, f: impl FnOnce(&State) -> Result<T, CoreError>) -> Result<T, CoreError> {
        let _ticket = self.enter()?;
        let _guard = self.gate.lock().map_err(|_| CoreError::Fenced)?;
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
        let _ticket = self.enter()?;
        let _guard = self.gate.lock().map_err(|_| CoreError::Fenced)?;
        if self.fenced.load(Ordering::Acquire) {
            return Err(CoreError::Fenced);
        }
        let mut s = self.read().inspect_err(|_| {
            self.fenced.store(true, Ordering::Release);
        })?;
        // Logical errors can still advance the trusted monotonic decision clock.
        let result = f(&mut s);
        let values = records(&s)?;
        let point = *self.crash.lock().map_err(|_| CoreError::Fenced)?;
        let persisted = (|| {
            let mut tx = self.db.begin_write().map_err(io)?;
            tx.set_durability(Durability::Immediate).map_err(io)?;
            {
                let mut t = tx.open_table(RECORDS).map_err(io)?;
                for (k, v) in &values {
                    t.insert(k.as_slice(), v.as_slice()).map_err(io)?;
                }
            }
            if point == CrashPoint::BeforeCommit {
                crash_now();
            }
            if point == CrashPoint::ErrorBeforeCommit {
                return Err(CoreError::Storage("injected before commit".into()));
            }
            tx.commit().map_err(io)?;
            if point == CrashPoint::AfterCommit {
                crash_now();
            }
            if point == CrashPoint::ErrorAfterCommit {
                return Err(CoreError::Storage("injected lost commit response".into()));
            }
            if point == CrashPoint::BeforeAck {
                crash_now();
            }
            Ok(())
        })();
        if let Err(e) = persisted {
            self.fenced.store(true, Ordering::Release);
            return Err(e);
        }
        result
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
impl Admin {
    fn attach(db: Database, limits: Limits) -> Result<Self, CoreError> {
        if limits.admitted == 0 || limits.selected_keys == 0 {
            return Err(CoreError::Limit);
        }
        let a = Self {
            inner: Arc::new(Inner {
                db,
                gate: Mutex::new(()),
                admitted: AtomicUsize::new(0),
                fenced: AtomicBool::new(false),
                limits,
                crash: Mutex::new(CrashPoint::None),
            }),
        };
        a.inner.read()?;
        Ok(a)
    }
    /// Explicit create-new directory. Existing paths, including empty ones, reject.
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
        Self::attach(
            Database::builder()
                .set_cache_size(8 * 1024 * 1024)
                .open(path.as_ref().join("data.redb"))
                .map_err(io)?,
            limits,
        )
    }
    pub fn snapshot(&self) -> Result<Snapshot, CoreError> {
        self.inner.inspect(|s| Ok(s.view.clone()))
    }
    pub fn authority_metadata(&self) -> Result<(u128, u64, u64, Option<u128>), CoreError> {
        self.inner
            .inspect(|s| Ok((s.view.domain, s.last_time, s.next_approval, s.origin)))
    }
    pub fn put(&self, k: Key, mut v: Row) -> Result<(), CoreError> {
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
        let path = path.as_ref();
        let payload = self.inner.inspect(|s| encode(&records(s)?))?;
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
        recovery(point, RecoveryPoint::ExportWrite, RecoveryPoint::None)?;
        f.write_all(&FORMAT.to_be_bytes()).map_err(io)?;
        f.write_all(&(payload.len() as u64).to_be_bytes())
            .map_err(io)?;
        f.write_all(&digest).map_err(io)?;
        f.write_all(&payload).map_err(io)?;
        recovery(point, RecoveryPoint::ExportSync, RecoveryPoint::None)?;
        f.sync_all().map_err(io)?;
        recovery(point, RecoveryPoint::ExportVerify, RecoveryPoint::None)?;
        Self::read_export(&stage)?;
        recovery(point, RecoveryPoint::ExportPublish, RecoveryPoint::None)?;
        fs::hard_link(&stage, path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::AlreadyExists {
                CoreError::Exists
            } else {
                io(e)
            }
        })?;
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
            || u32::from_be_bytes(header[8..12].try_into().expect("length")) != FORMAT
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
        if encode(&values)? != payload {
            return Err(CoreError::Corrupt("noncanonical export".into()));
        }
        from_records(values)
    }
    pub fn verify_export(path: impl AsRef<Path>) -> Result<Snapshot, CoreError> {
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
impl Executor {
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
        self.inner.change(|s| {
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
    fn fresh(name: &str) -> std::path::PathBuf {
        let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("artifacts");
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
                            format: 2,
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
        let tx = a.inner.db.begin_write().unwrap();
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
