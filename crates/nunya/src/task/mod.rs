//! Atomic file-backed task scopes and persistent tokenization limits.
use crate::{
    error::{NyCode, Refusal},
    pgwire::auth::ScramVerifier,
    policy::DomainName,
    token::TaskId,
};
use data_encoding::{BASE32_NOPAD, HEXLOWER};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fmt,
    fs::{self, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard},
    time::{SystemTime, UNIX_EPOCH},
};

/// Persistent task records serialized under one store-local mutation lock.
#[derive(Clone)]
pub struct TaskStore {
    dir: PathBuf,
    clock: Arc<dyn Fn() -> u64 + Send + Sync>,
    lock: Arc<Mutex<()>>,
}
/// A newly opened task and its one-time returned authentication secret.
pub struct OpenedTask {
    /// Non-secret task identifier.
    pub id: TaskId,
    /// Authentication secret, returned only when the task is opened.
    pub secret: String,
    /// Absolute expiry in milliseconds since the Unix epoch.
    pub expires_unix_ms: u64,
}
impl fmt::Debug for OpenedTask {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpenedTask")
            .field("id", &self.id)
            .field("secret", &"[REDACTED]")
            .field("expires_unix_ms", &self.expires_unix_ms)
            .finish()
    }
}
/// An authenticated task scope; debugging never exposes its salt.
pub struct TaskRecord {
    /// Non-secret task identifier.
    pub id: TaskId,
    /// Random salt used to derive per-task token scope keys.
    pub salt: [u8; 32],
    /// Absolute expiry in milliseconds since the Unix epoch.
    pub expires_unix_ms: u64,
    /// Optional operator label.
    pub label: Option<String>,
    /// Persisted explicit tokenizations by domain.
    pub tokenize_counts: BTreeMap<DomainName, u32>,
}
impl fmt::Debug for TaskRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TaskRecord")
            .field("id", &self.id)
            .field("salt", &"[REDACTED]")
            .field("expires_unix_ms", &self.expires_unix_ms)
            .field("label", &self.label)
            .field("tokenize_counts", &self.tokenize_counts)
            .finish()
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredTask {
    id: String,
    secret_sha256: String,
    scram_verifier: String,
    salt: String,
    expires_unix_ms: u64,
    label: Option<String>,
    tokenize_counts: BTreeMap<DomainName, u32>,
}
impl StoredTask {
    fn record(&self, id: TaskId) -> io::Result<TaskRecord> {
        if parse_task_id(&self.id) != Some(id) {
            return Err(invalid_record());
        }
        Ok(TaskRecord {
            id,
            salt: decode_hex(&self.salt)?,
            expires_unix_ms: self.expires_unix_ms,
            label: self.label.clone(),
            tokenize_counts: self.tokenize_counts.clone(),
        })
    }
}
fn invalid_record() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "invalid task record")
}
fn invalid_task() -> Refusal {
    Refusal::new(NyCode::TokenNotValid, "task authentication failed")
}
fn decode_hex<const N: usize>(text: &str) -> io::Result<[u8; N]> {
    if text.len() != N * 2
        || !text
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(invalid_record());
    }
    HEXLOWER
        .decode(text.as_bytes())
        .map_err(|_| invalid_record())?
        .try_into()
        .map_err(|_| invalid_record())
}
fn random<const N: usize>() -> io::Result<[u8; N]> {
    let mut bytes = [0; N];
    getrandom::fill(&mut bytes).map_err(|_| io::Error::other("task randomness unavailable"))?;
    Ok(bytes)
}
fn remove_if_present(path: &Path) -> io::Result<bool> {
    match fs::remove_file(path) {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}
/// Non-secret task listing entry: identifier, expiry, and optional label.
pub type TaskSummary = (TaskId, u64, Option<String>);

impl TaskStore {
    /// Open or create the task directory using the system clock.
    pub fn open_dir(dir: &Path) -> io::Result<Self> {
        Self::with_clock(
            dir,
            Arc::new(|| {
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
                    .unwrap_or(0)
            }),
        )
    }
    /// Open or create the task directory with an injectable millisecond clock.
    pub fn with_clock(dir: &Path, clock: Arc<dyn Fn() -> u64 + Send + Sync>) -> io::Result<Self> {
        fs::create_dir_all(dir)?;
        Ok(Self {
            dir: dir.to_owned(),
            clock,
            lock: Arc::new(Mutex::new(())),
        })
    }
    fn guard(&self) -> io::Result<MutexGuard<'_, ()>> {
        self.lock
            .lock()
            .map_err(|_| io::Error::other("task store lock unavailable"))
    }
    fn path(&self, id: &TaskId) -> PathBuf {
        self.dir.join(format!("{}.json", format_task_id(id)))
    }
    fn read(&self, id: &TaskId) -> io::Result<StoredTask> {
        let bytes = fs::read(self.path(id))?;
        let task: StoredTask = serde_json::from_slice(&bytes).map_err(|_| invalid_record())?;
        task.record(*id)?;
        let _: [u8; 32] = decode_hex(&task.secret_sha256)?;
        Ok(task)
    }
    fn live(&self, id: &TaskId) -> io::Result<StoredTask> {
        let task = self.read(id)?;
        if task.expires_unix_ms <= (self.clock)() {
            remove_if_present(&self.path(id))?;
            return Err(io::Error::new(io::ErrorKind::NotFound, "task unavailable"));
        }
        Ok(task)
    }
    fn write(&self, id: &TaskId, task: &StoredTask) -> io::Result<()> {
        let path = self.path(id);
        let temporary = path.with_extension("json.tmp");
        let bytes = serde_json::to_vec(task).map_err(|_| invalid_record())?;
        let result = (|| {
            let mut file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&temporary)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            drop(file);
            fs::rename(&temporary, &path)
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        result
    }
    fn ids(&self) -> io::Result<Vec<TaskId>> {
        let mut ids = vec![];
        for entry in fs::read_dir(&self.dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            if let Some(id) = name.strip_suffix(".json").and_then(parse_task_id) {
                ids.push(id);
            }
        }
        ids.sort_by_key(format_task_id);
        Ok(ids)
    }
    /// Open a fresh task and persist its authentication hash and SCRAM verifier.
    pub fn open(&self, ttl_ms: u64, label: Option<String>) -> io::Result<OpenedTask> {
        let _guard = self.guard()?;
        let expires_unix_ms = (self.clock)()
            .checked_add(ttl_ms)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "task expiry overflow"))?;
        let mut id = None;
        for _ in 0..16 {
            let candidate = TaskId(random()?);
            if !self.path(&candidate).try_exists()? {
                id = Some(candidate);
                break;
            }
        }
        let id = id.ok_or_else(|| io::Error::other("task identifier unavailable"))?;
        let secret = format!(
            "nys_{}",
            BASE32_NOPAD.encode(&random::<32>()?).to_ascii_lowercase()
        );
        let task = StoredTask {
            id: format_task_id(&id),
            secret_sha256: HEXLOWER.encode(&Sha256::digest(secret.as_bytes())),
            scram_verifier: ScramVerifier::from_password(&secret, &random::<16>()?, 4096)
                .to_pg_string(),
            salt: HEXLOWER.encode(&random::<32>()?),
            expires_unix_ms,
            label,
            tokenize_counts: BTreeMap::new(),
        };
        self.write(&id, &task)?;
        Ok(OpenedTask {
            id,
            secret,
            expires_unix_ms,
        })
    }
    /// Authenticate without distinguishing unknown, closed, expired, and wrong-secret tasks.
    pub fn authenticate(&self, id: &TaskId, secret: &str) -> Result<TaskRecord, Refusal> {
        let _guard = self.guard().map_err(|_| invalid_task())?;
        let task = self.live(id).map_err(|_| invalid_task())?;
        let expected: [u8; 32] = decode_hex(&task.secret_sha256).map_err(|_| invalid_task())?;
        let actual = Sha256::digest(secret.as_bytes());
        let different = expected
            .iter()
            .zip(actual.iter())
            .fold(0u8, |different, (a, b)| different | (a ^ b));
        if different != 0 {
            return Err(invalid_task());
        }
        task.record(*id).map_err(|_| invalid_task())
    }
    /// Retrieve a live task and its persisted SCRAM verifier without exposing its secret.
    pub fn scram_verifier(&self, id: &TaskId) -> Result<(TaskRecord, ScramVerifier), Refusal> {
        let _guard = self.guard().map_err(|_| invalid_task())?;
        let task = self.live(id).map_err(|_| invalid_task())?;
        let verifier = ScramVerifier::parse(&task.scram_verifier).ok_or_else(invalid_task)?;
        Ok((task.record(*id).map_err(|_| invalid_task())?, verifier))
    }
    /// Close a task by deleting its persisted record and salt.
    pub fn close(&self, id: &TaskId) -> io::Result<bool> {
        let _guard = self.guard()?;
        remove_if_present(&self.path(id))
    }
    /// List live task identifiers, expiries, and labels, removing touched expired tasks.
    pub fn list(&self) -> io::Result<Vec<TaskSummary>> {
        let _guard = self.guard()?;
        let mut tasks = vec![];
        for id in self.ids()? {
            match self.live(&id) {
                Ok(task) => tasks.push((id, task.expires_unix_ms, task.label)),
                Err(error) if error.kind() == io::ErrorKind::NotFound => (),
                Err(error) => return Err(error),
            }
        }
        Ok(tasks)
    }
    /// Persist one explicit tokenization unless it would exceed the domain limit.
    pub fn count_tokenize(
        &self,
        id: &TaskId,
        domain: &DomainName,
        limit: u32,
    ) -> Result<u32, Refusal> {
        let _guard = self.guard().map_err(|_| invalid_task())?;
        let mut task = self.live(id).map_err(|_| invalid_task())?;
        let count = task.tokenize_counts.get(domain).copied().unwrap_or(0);
        let next = count
            .checked_add(1)
            .filter(|next| *next <= limit)
            .ok_or_else(|| Refusal::new(NyCode::TokenizeLimit, "task tokenize limit reached"))?;
        task.tokenize_counts.insert(domain.clone(), next);
        self.write(id, &task).map_err(|_| invalid_task())?;
        Ok(next)
    }
    /// Delete expired task files and return how many records were removed.
    pub fn purge_expired(&self) -> io::Result<usize> {
        let _guard = self.guard()?;
        let now = (self.clock)();
        let mut removed = 0;
        for id in self.ids()? {
            let task = self.read(&id)?;
            if task.expires_unix_ms <= now && remove_if_present(&self.path(&id))? {
                removed += 1;
            }
        }
        Ok(removed)
    }
}

/// Parse exactly 32 lowercase hexadecimal identifier characters.
pub fn parse_task_id(s: &str) -> Option<TaskId> {
    decode_hex(s).ok().map(TaskId)
}
/// Format a task identifier as 32 lowercase hexadecimal characters.
pub fn format_task_id(id: &TaskId) -> String {
    HEXLOWER.encode(&id.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    struct Fixture {
        dir: PathBuf,
        now: Arc<AtomicU64>,
        store: TaskStore,
    }
    impl Fixture {
        fn new() -> Self {
            let root = std::env::temp_dir();
            let suffix = HEXLOWER.encode(&random::<16>().unwrap());
            let dir = root.join(format!("task-test-{suffix}"));
            let now = Arc::new(AtomicU64::new(100));
            let clock = now.clone();
            let store = TaskStore::with_clock(&dir, Arc::new(move || clock.load(Ordering::SeqCst)))
                .unwrap();
            Self { dir, now, store }
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            fs::remove_dir_all(&self.dir).unwrap();
        }
    }
    #[test]
    fn scram_verifier_persists_and_expires() {
        let fixture = Fixture::new();
        let opened = fixture.store.open(10, None).unwrap();
        let (record, verifier) = fixture.store.scram_verifier(&opened.id).unwrap();
        let expected = ScramVerifier::from_password(&opened.secret, &verifier.salt, 4096);
        assert_eq!(expected.to_pg_string(), verifier.to_pg_string());
        let reopened = TaskStore::with_clock(&fixture.dir, Arc::new(|| 100)).unwrap();
        let (again, saved) = reopened.scram_verifier(&opened.id).unwrap();
        assert_eq!(record.salt, again.salt);
        assert_eq!(saved.to_pg_string(), verifier.to_pg_string());
        fixture.now.store(110, Ordering::SeqCst);
        assert_eq!(
            fixture.store.scram_verifier(&opened.id).unwrap_err().code,
            NyCode::TokenNotValid
        );
        assert_eq!(
            fixture
                .store
                .scram_verifier(&TaskId([0; 16]))
                .unwrap_err()
                .code,
            NyCode::TokenNotValid
        );
    }
    #[test]
    fn open_authenticate_and_redacted_debug() {
        let fixture = Fixture::new();
        let opened = fixture.store.open(1000, Some("label".into())).unwrap();
        assert!(opened.secret.starts_with("nys_"));
        assert_eq!(opened.secret.len(), 56);
        assert!(
            opened.secret[4..]
                .bytes()
                .all(|b| b.is_ascii_lowercase() || (b'2'..=b'7').contains(&b))
        );
        let record = fixture
            .store
            .authenticate(&opened.id, &opened.secret)
            .unwrap();
        assert_eq!(record.id, opened.id);
        assert_eq!(record.label.as_deref(), Some("label"));
        assert_eq!(record.expires_unix_ms, 1100);
        assert!(!format!("{opened:?}").contains(&opened.secret));
        assert!(!format!("{record:?}").contains(&HEXLOWER.encode(&record.salt)));
        let disk = fs::read_to_string(fixture.store.path(&opened.id)).unwrap();
        assert!(!disk.contains(&opened.secret));
        assert!(
            !fixture
                .store
                .path(&opened.id)
                .with_extension("json.tmp")
                .exists()
        );
    }
    #[test]
    fn all_authentication_failures_are_identical() {
        let fixture = Fixture::new();
        let opened = fixture.store.open(10, None).unwrap();
        let wrong = fixture.store.authenticate(&opened.id, "wrong").unwrap_err();
        let unknown = fixture
            .store
            .authenticate(&TaskId([0; 16]), &opened.secret)
            .unwrap_err();
        fixture.now.store(110, Ordering::SeqCst);
        let expired = fixture
            .store
            .authenticate(&opened.id, &opened.secret)
            .unwrap_err();
        assert!(!fixture.store.path(&opened.id).exists());
        let other = fixture.store.open(10, None).unwrap();
        assert!(fixture.store.close(&other.id).unwrap());
        assert!(!fixture.store.close(&other.id).unwrap());
        let closed = fixture
            .store
            .authenticate(&other.id, &other.secret)
            .unwrap_err();
        assert_eq!(wrong.code.sqlstate(), "NY003");
        assert_eq!(wrong, unknown);
        assert_eq!(wrong, expired);
        assert_eq!(wrong, closed);
    }
    #[test]
    fn counts_persist_and_refuse_limit() {
        let fixture = Fixture::new();
        let opened = fixture.store.open(1000, None).unwrap();
        let domain = DomainName("email".into());
        assert_eq!(
            fixture
                .store
                .count_tokenize(&opened.id, &domain, 2)
                .unwrap(),
            1
        );
        assert_eq!(
            fixture
                .store
                .count_tokenize(&opened.id, &domain, 2)
                .unwrap(),
            2
        );
        assert_eq!(
            fixture
                .store
                .count_tokenize(&opened.id, &domain, 2)
                .unwrap_err()
                .code
                .sqlstate(),
            "NY007"
        );
        let again = TaskStore::with_clock(&fixture.dir, Arc::new(|| 100)).unwrap();
        let record = again.authenticate(&opened.id, &opened.secret).unwrap();
        assert_eq!(record.tokenize_counts.get(&domain), Some(&2));
        assert_eq!(again.count_tokenize(&opened.id, &domain, 3).unwrap(), 3);
    }
    #[test]
    fn count_expiry_and_closed_task() {
        let fixture = Fixture::new();
        let opened = fixture.store.open(1, None).unwrap();
        fixture.now.store(101, Ordering::SeqCst);
        let domain = DomainName("email".into());
        assert_eq!(
            fixture
                .store
                .count_tokenize(&opened.id, &domain, 2)
                .unwrap_err()
                .code
                .sqlstate(),
            "NY003"
        );
        assert!(!fixture.store.path(&opened.id).exists());
        assert_eq!(
            fixture
                .store
                .count_tokenize(&opened.id, &domain, 2)
                .unwrap_err()
                .code
                .sqlstate(),
            "NY003"
        );
    }
    #[test]
    fn list_skips_expired_purge_removes_and_ids_differ() {
        let fixture = Fixture::new();
        let expired = fixture.store.open(1, None).unwrap();
        let live = fixture.store.open(100, Some("live".into())).unwrap();
        assert_ne!(expired.id, live.id);
        fixture.now.store(101, Ordering::SeqCst);
        assert_eq!(
            fixture.store.list().unwrap(),
            vec![(live.id, 200, Some("live".into()))]
        );
        assert!(!fixture.store.path(&expired.id).exists());
        fixture.now.store(200, Ordering::SeqCst);
        assert_eq!(fixture.store.purge_expired().unwrap(), 1);
        assert_eq!(fixture.store.purge_expired().unwrap(), 0);
    }
    #[test]
    fn task_id_format_is_strict() {
        let id = TaskId([0xab; 16]);
        let formatted = format_task_id(&id);
        assert_eq!(formatted.len(), 32);
        assert_eq!(parse_task_id(&formatted), Some(id));
        for bad in [
            formatted.to_uppercase(),
            format!(" {formatted}"),
            formatted[..31].into(),
            "é".repeat(16),
        ] {
            assert!(parse_task_id(&bad).is_none());
        }
    }
    #[test]
    fn concurrent_count_updates_do_not_get_lost() {
        let fixture = Fixture::new();
        let opened = fixture.store.open(100, None).unwrap();
        std::thread::scope(|scope| {
            for _ in 0..8 {
                let store = fixture.store.clone();
                let id = opened.id;
                scope.spawn(move || {
                    store
                        .count_tokenize(&id, &DomainName("email".into()), 8)
                        .unwrap();
                });
            }
        });
        let record = fixture
            .store
            .authenticate(&opened.id, &opened.secret)
            .unwrap();
        assert_eq!(record.tokenize_counts[&DomainName("email".into())], 8);
    }
}
