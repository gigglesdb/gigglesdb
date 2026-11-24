//! Backend-independent KV seam. Keys are byte-lexicographic within a keyspace.
use std::{
    cell::Cell,
    ops::Bound,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::SystemTime,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) enum Keyspace {
    Meta,
    Catalog,
    Rows,
    Index,
    Kernel,
    Seq,
    Gc,
}

impl Keyspace {
    pub(crate) const ALL: [Self; 7] = [
        Self::Meta,
        Self::Catalog,
        Self::Rows,
        Self::Index,
        Self::Kernel,
        Self::Seq,
        Self::Gc,
    ];

    pub(crate) fn name(&self) -> &'static str {
        match self {
            Self::Meta => "meta",
            Self::Catalog => "catalog",
            Self::Rows => "rows",
            Self::Index => "index",
            Self::Kernel => "kernel",
            Self::Seq => "seq",
            Self::Gc => "gc",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Op {
    Put {
        ks: Keyspace,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        ks: Keyspace,
        key: Vec<u8>,
    },
    /// Delete exactly `[start, end)`. Empty or reversed intervals do nothing.
    DeleteRange {
        ks: Keyspace,
        start: Vec<u8>,
        end: Vec<u8>,
    },
}

/// Operations execute in insertion order as one atomic batch.
#[derive(Clone, Default, Debug, PartialEq, Eq)]
pub(crate) struct WriteBatch {
    ops: Vec<Op>,
}

/// Local commit pipeline cursor, encoded as a big-endian u64 in Meta.
pub(crate) const COMMIT_GROUP_KEY: &[u8] = b"commit_group";

const BATCH_WIRE_VERSION: u8 = 1;

impl WriteBatch {
    pub(crate) fn ops(&self) -> &[Op] {
        &self.ops
    }

    /// Versioned replication payload; does not change the storage format.
    pub(crate) fn encode(&self) -> Vec<u8> {
        fn field(out: &mut Vec<u8>, bytes: &[u8]) {
            let len = u32::try_from(bytes.len()).expect("batch field exceeds u32 length");
            out.extend_from_slice(&len.to_be_bytes());
            out.extend_from_slice(bytes);
        }
        let mut out = vec![BATCH_WIRE_VERSION];
        let count = u32::try_from(self.ops.len()).expect("batch exceeds u32 operation count");
        out.extend_from_slice(&count.to_be_bytes());
        for op in self.ops() {
            let (tag, ks, first, second) = match op {
                Op::Put { ks, key, value } => (1, ks, key, Some(value)),
                Op::Delete { ks, key } => (2, ks, key, None),
                Op::DeleteRange { ks, start, end } => (3, ks, start, Some(end)),
            };
            out.push(tag);
            out.push(Keyspace::ALL.iter().position(|item| item == ks).unwrap() as u8);
            field(&mut out, first);
            if let Some(bytes) = second {
                field(&mut out, bytes);
            }
        }
        let checksum = super::crc32c::crc32c(&out);
        out.extend_from_slice(&checksum.to_be_bytes());
        out
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, KvError> {
        fn take<'a>(input: &mut &'a [u8], len: usize) -> Result<&'a [u8], KvError> {
            if len > input.len() {
                return Err(KvError::Corrupt("truncated batch".into()));
            }
            let (value, rest) = input.split_at(len);
            *input = rest;
            Ok(value)
        }
        fn number(input: &mut &[u8]) -> Result<u32, KvError> {
            Ok(u32::from_be_bytes(take(input, 4)?.try_into().unwrap()))
        }
        fn field(input: &mut &[u8]) -> Result<Vec<u8>, KvError> {
            let len = number(input)? as usize;
            // Validate the slice before allocating from an untrusted length.
            Ok(take(input, len)?.to_vec())
        }
        let Some(&version) = bytes.first() else {
            return Err(KvError::Corrupt("empty batch".into()));
        };
        if version != BATCH_WIRE_VERSION {
            return Err(KvError::Corrupt(format!("unknown batch version {version}")));
        }
        if bytes.len() < 9 {
            return Err(KvError::Corrupt("truncated batch".into()));
        }
        let (payload, mut trailer) = bytes.split_at(bytes.len() - 4);
        if super::crc32c::crc32c(payload) != number(&mut trailer)? {
            return Err(KvError::Corrupt("batch checksum mismatch".into()));
        }
        let mut input = &payload[1..];
        let count = number(&mut input)? as usize;
        // Even an empty-key Delete needs six bytes; never reserve by count.
        if count > input.len() / 6 {
            return Err(KvError::Corrupt("batch operation count mismatch".into()));
        }
        let mut batch = Self::default();
        for _ in 0..count {
            let tag = take(&mut input, 1)?[0];
            let index = take(&mut input, 1)?[0] as usize;
            let ks = *Keyspace::ALL
                .get(index)
                .ok_or_else(|| KvError::Corrupt("unknown batch keyspace".into()))?;
            match tag {
                1 => batch.put(ks, field(&mut input)?, field(&mut input)?),
                2 => batch.delete(ks, field(&mut input)?),
                3 => batch.delete_range(ks, field(&mut input)?, field(&mut input)?),
                _ => return Err(KvError::Corrupt("unknown batch operation tag".into())),
            }
        }
        if !input.is_empty() {
            return Err(KvError::Corrupt(
                "trailing batch bytes or operation count mismatch".into(),
            ));
        }
        Ok(batch)
    }

    pub(crate) fn put(&mut self, ks: Keyspace, key: Vec<u8>, value: Vec<u8>) {
        self.ops.push(Op::Put { ks, key, value });
    }
    pub(crate) fn put_commit_group(&mut self, group: u64) {
        self.put(Keyspace::Meta, COMMIT_GROUP_KEY.to_vec(), group.to_be_bytes().to_vec());
    }

    pub(crate) fn delete(&mut self, ks: Keyspace, key: Vec<u8>) {
        self.ops.push(Op::Delete { ks, key });
    }
    pub(crate) fn delete_range(&mut self, ks: Keyspace, start: Vec<u8>, end: Vec<u8>) {
        self.ops.push(Op::DeleteRange { ks, start, end });
    }
    pub(crate) fn len(&self) -> usize {
        self.ops.len()
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }
    /// Payload bytes only, excluding operation/keyspace tags and allocation overhead.
    pub(crate) fn byte_size(&self) -> usize {
        self.ops
            .iter()
            .map(|op| match op {
                Op::Put { key, value, .. } => key.len() + value.len(),
                Op::Delete { key, .. } => key.len(),
                Op::DeleteRange { start, end, .. } => start.len() + end.len(),
            })
            .sum()
    }
    pub(crate) fn into_ops(self) -> Vec<Op> {
        self.ops
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Durability {
    /// Commit this batch and every preceding NoSync batch durably before returning.
    Sync,
    /// Atomically visible on success; durability waits for Sync or flush_barrier.
    NoSync,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Direction {
    Forward,
    Reverse,
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) enum KvError {
    Io(String),
    #[cfg(feature = "engine-rocksdb")]
    OpenRefused(String),
    #[cfg_attr(not(any(test, feature = "engine-rocksdb")), allow(dead_code))]
    RocksIo { message: String, status: RocksStatus, site: FenceSite },
    Corrupt(String),
    Fenced,
    Closed,
}

impl std::fmt::Debug for KvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(message) | Self::RocksIo { message, .. } => f.debug_tuple("Io").field(message).finish(),
            #[cfg(feature = "engine-rocksdb")]
            Self::OpenRefused(message) => f.debug_tuple("OpenRefused").field(message).finish(),
            Self::Corrupt(message) => f.debug_tuple("Corrupt").field(message).finish(),
            Self::Fenced => f.write_str("Fenced"),
            Self::Closed => f.write_str("Closed"),
        }
    }
}

impl std::fmt::Display for KvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(message) | Self::RocksIo { message, .. } => write!(f, "KV I/O: {message}"),
            #[cfg(feature = "engine-rocksdb")]
            Self::OpenRefused(message) => write!(f, "KV open refused: {message}"),
            Self::Corrupt(message) => write!(f, "KV corruption: {message}"),
            Self::Fenced => f.write_str("KV fenced"),
            Self::Closed => f.write_str("KV closed"),
        }
    }
}
impl std::error::Error for KvError {}
impl From<KvError> for crate::CoreError {
    fn from(error: KvError) -> Self {
        match error {
            KvError::Io(message) | KvError::RocksIo { message, .. } => Self::Storage(message),
            #[cfg(feature = "engine-rocksdb")]
            KvError::OpenRefused(message) => Self::Storage(message),
            KvError::Corrupt(message) => Self::Corrupt(message),
            KvError::Fenced | KvError::Closed => Self::Fenced,
        }
    }
}

pub(crate) type KvPair = (Vec<u8>, Vec<u8>);
pub(crate) struct KvIter<'a> {
    inner: Box<dyn Iterator<Item = Result<KvPair, KvError>> + Send + 'a>,
    causes: Option<&'a FenceState>,
}
impl<'a> KvIter<'a> {
    pub(crate) fn new(iter: impl Iterator<Item = Result<KvPair, KvError>> + Send + 'a) -> Self {
        Self { inner: Box::new(iter), causes: None }
    }
}
impl Iterator for KvIter<'_> {
    type Item = Result<KvPair, KvError>;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.inner.next();
        if let Some(Err(error)) = &item
            && let Some(causes) = self.causes
        {
            causes.read_error(error);
        }
        item
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(dead_code, reason = "Some sites are specific to the RocksDB backend")]
pub enum FenceSite {
    Write,
    WalWrite,
    FlushBarrier,
    WalFlush,
    CommitAppend,
    CommitSync,
    BackgroundError,
    Read,
    Open,
    WriterLock,
    CatalogReload,
    LegacyPersist,
    NativeOnceCommit,
    NativeOnceLedger,
    Other,
}

/// Status fields from RocksDB 11.8.1. The C API returns Status::ToString()
/// rather than Status itself, so severity is absent when it was not exported.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RocksStatus {
    pub code: u8,
    pub subcode: u8,
    pub severity: Option<u8>,
}

impl RocksStatus {
    const NOT_SUPPORTED: u8 = 3;
    pub(crate) const IO_ERROR: u8 = 5;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FenceClass {
    Recoverable,
    Data,
    NotRecoverable,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FenceCause {
    pub site: FenceSite,
    pub class: FenceClass,
    pub status: Option<RocksStatus>,
    pub error_text: String,
    pub at: SystemTime,
    pub generation: u64,
}

impl FenceCause {
    pub(crate) fn new(site: FenceSite, class: FenceClass, error: &impl std::fmt::Display) -> Self {
        Self {
            site,
            class,
            status: None,
            error_text: super::diagnostics::error_text(error),
            at: SystemTime::now(),
            generation: 0,
        }
    }

    pub(crate) fn for_core(site: FenceSite, error: &crate::CoreError) -> Self {
        let class = match (site, error) {
            (FenceSite::NativeOnceLedger, _) | (_, crate::CoreError::Corrupt(_)) => FenceClass::Data,
            (FenceSite::CatalogReload, crate::CoreError::Storage(_) | crate::CoreError::Fenced) => {
                FenceClass::NotRecoverable
            }
            (FenceSite::CatalogReload, _) => FenceClass::Data,
            _ => FenceClass::NotRecoverable,
        };
        Self::new(site, class, error)
    }

    pub(crate) fn for_kv(site: FenceSite, error: &KvError) -> Self {
        let (site, status, io_error) = match error {
            KvError::RocksIo { status, site: actual, .. } => {
                let site = match (site, *actual) {
                    (FenceSite::Open, _)
                    | (
                        FenceSite::Write | FenceSite::FlushBarrier,
                        FenceSite::WalWrite | FenceSite::WalFlush | FenceSite::BackgroundError,
                    ) => *actual,
                    _ => site,
                };
                (site, Some(*status), status.code == RocksStatus::IO_ERROR)
            }
            _ => (site, None, false),
        };
        let class = if matches!(error, KvError::Corrupt(_)) || site == FenceSite::NativeOnceLedger {
            FenceClass::Data
        } else if io_error && matches!(
            site,
            FenceSite::WalWrite | FenceSite::WalFlush | FenceSite::Read
                | FenceSite::Open | FenceSite::WriterLock | FenceSite::CatalogReload
        ) {
            FenceClass::Recoverable
        } else {
            FenceClass::NotRecoverable
        };
        Self { status, ..Self::new(site, class, error) }
    }
}

#[derive(Default)]
pub(crate) struct FenceState(Mutex<RecordedFence>);

#[derive(Default)]
struct RecordedFence {
    cause: Option<FenceCause>,
    commit_pipeline: bool,
    #[cfg(feature = "engine-rocksdb")]
    notifier: Option<(u64, std::sync::Weak<super::super::fence_supervisor::Wake>)>,
}

thread_local! {
    static READ_ERROR: Cell<Option<ReadErrorCapture>> = const { Cell::new(None) };
}

struct ReadErrorCapture {
    state: usize,
    error: Option<KvError>,
}

impl FenceState {
    fn enable_commit_pipeline(&self) {
        let mut recorded = self.0.lock().unwrap_or_else(|p| p.into_inner());
        recorded.commit_pipeline = true;
        if let Some(cause) = &mut recorded.cause {
            cause.class = FenceClass::NotRecoverable;
        }
    }

    fn record_explicit_pipeline_fence(&self, backend: &dyn KvBackend) {
        let mut recorded = self.0.lock().unwrap_or_else(|p| p.into_inner());
        // A durability failure can already be draining background work. Its
        // caller will report the cause when the drain completes.
        if recorded.commit_pipeline && recorded.cause.is_none() && !backend.sync_failure_latched() {
            #[cfg_attr(not(feature = "engine-rocksdb"), allow(unused_mut))]
            let mut cause = FenceCause::new(FenceSite::Other, FenceClass::NotRecoverable, &KvError::Fenced);
            #[cfg(feature = "engine-rocksdb")]
            if let Some((generation, _)) = &recorded.notifier {
                cause.generation = *generation;
            }
            recorded.cause = Some(cause);
        }
    }

    pub(crate) fn cause(&self) -> Option<FenceCause> {
        self.0.lock().unwrap_or_else(|p| p.into_inner()).cause.clone()
    }

    #[cfg(feature = "engine-rocksdb")]
    pub(in crate::storage) fn with_cause<T>(&self, read: impl FnOnce(Option<&FenceCause>) -> T) -> T {
        let recorded = self.0.lock().unwrap_or_else(|p| p.into_inner());
        read(recorded.cause.as_ref())
    }

    #[cfg(feature = "engine-rocksdb")]
    pub(in crate::storage) fn notify_recovery(
        &self,
        generation: u64,
        wake: &Arc<super::super::fence_supervisor::Wake>,
    ) {
        let mut recorded = self.0.lock().unwrap_or_else(|p| p.into_inner());
        recorded.notifier = Some((generation, Arc::downgrade(wake)));
        if let Some(cause) = &mut recorded.cause {
            cause.generation = generation;
            wake.fenced(generation);
        }
    }

    pub(crate) fn record(&self, cause: FenceCause) {
        self.record_cause(cause, false, None);
    }

    #[cfg(test)]
    pub(crate) fn record_if_empty(&self, cause: FenceCause) {
        self.record_cause(cause, true, None);
    }

    pub(crate) fn record_fence(
        &self,
        cause: FenceCause,
        flag: &AtomicBool,
        only_if_empty: bool,
    ) -> bool {
        self.record_cause(cause, only_if_empty, Some(flag))
    }

    #[cfg(feature = "engine-rocksdb")]
    pub(super) fn signal_recovery(&self) {
        let recorded = self.0.lock().unwrap_or_else(|p| p.into_inner());
        if let Some((generation, wake)) = &recorded.notifier
            && recorded.cause.is_some()
            && let Some(wake) = wake.upgrade()
        {
            wake.fenced(*generation);
        }
    }

    fn record_cause(
        &self,
        cause: FenceCause,
        only_if_empty: bool,
        fence: Option<&AtomicBool>,
    ) -> bool {
        let mut recorded = self.0.lock().unwrap_or_else(|p| p.into_inner());
        let mut cause = cause;
        if recorded.commit_pipeline {
            cause.class = FenceClass::NotRecoverable;
        }
        #[cfg(feature = "engine-rocksdb")]
        let notifier = if let Some((generation, wake)) = &recorded.notifier {
            if cause.generation != 0 && cause.generation != *generation {
                return false;
            }
            cause.generation = *generation;
            wake.upgrade()
        } else {
            None
        };
        // Admission reads both under this mutex. A new fence must wake the
        // supervisor even if an earlier read error retains cause precedence.
        let newly_fenced = fence.is_some_and(|flag| !flag.swap(true, Ordering::AcqRel));
        let keep_previous = recorded.cause.as_ref().is_some_and(|previous| {
            only_if_empty
                || cause.generation < previous.generation
                || (cause.generation == previous.generation
                    && (previous.class != FenceClass::Recoverable
                        || cause.class == FenceClass::Recoverable))
        });
        if keep_previous && !newly_fenced {
            return false;
        }
        #[cfg(feature = "engine-rocksdb")]
        let generation = cause.generation;
        let text = (!keep_previous).then(|| {
            #[cfg(feature = "engine-rocksdb")]
            let message = if notifier.is_some() { "storage fence cause" } else { "engine fence cause" };
            #[cfg(not(feature = "engine-rocksdb"))]
            let message = "engine fence cause";
            let text = format!(
                "{message} site={:?} class={:?} status={:?} generation={}: {}",
                cause.site, cause.class, cause.status, cause.generation, cause.error_text
            );
            recorded.cause = Some(cause);
            text
        });
        drop(recorded);
        #[cfg(feature = "engine-rocksdb")]
        if let Some(wake) = notifier {
            wake.fenced(generation);
        }
        if let Some(text) = text {
            super::diagnostics::error(text);
        }
        newly_fenced
    }

    /// Catalog reload runs on the caller thread. Carry the backend error across
    /// conversion to CoreError without borrowing another thread's diagnosis.
    pub(crate) fn capture_read_error<T>(&self, load: impl FnOnce() -> T) -> (T, Option<KvError>) {
        struct Restore(Option<ReadErrorCapture>);
        impl Drop for Restore {
            fn drop(&mut self) {
                READ_ERROR.set(self.0.take());
            }
        }
        let _restore = Restore(READ_ERROR.replace(Some(ReadErrorCapture {
            state: std::ptr::from_ref(self).addr(),
            error: None,
        })));
        let result = load();
        (result, READ_ERROR.take().and_then(|capture| capture.error))
    }

    fn read_error(&self, error: &KvError) {
        if matches!(error, KvError::Fenced) {
            return;
        }
        let captured = READ_ERROR.with(|slot| {
            let mut capture = slot.take();
            let matched = capture.as_mut().is_some_and(|capture| {
                if capture.state != std::ptr::from_ref(self).addr() {
                    return false;
                }
                capture.error = Some(error.clone());
                true
            });
            slot.set(capture);
            matched
        });
        // Report corruption even when catalog reload also sees the typed error.
        if !captured || matches!(error, KvError::Corrupt(_)) {
            self.record(FenceCause::for_kv(FenceSite::Read, error));
        }
    }

    fn read<T>(&self, result: Result<T, KvError>) -> Result<T, KvError> {
        result.inspect_err(|error| self.read_error(error))
    }

    fn range<'a>(&'a self, result: Result<KvIter<'a>, KvError>) -> Result<KvIter<'a>, KvError> {
        self.read(result).map(|mut iter| {
            iter.causes = Some(self);
            iter
        })
    }
}

struct ReportingSnapshot {
    snapshot: Arc<dyn KvRead>,
    causes: Arc<FenceState>,
}

impl KvRead for ReportingSnapshot {
    fn get(&self, ks: Keyspace, key: &[u8]) -> Result<Option<Vec<u8>>, KvError> {
        self.causes.read(self.snapshot.get(ks, key))
    }

    fn range(
        &self,
        ks: Keyspace,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        dir: Direction,
    ) -> Result<KvIter<'_>, KvError> {
        self.causes.range(self.snapshot.range(ks, start, end, dir))
    }

    fn first_in(&self, ks: Keyspace, start: &[u8], end: &[u8]) -> Result<Option<KvPair>, KvError> {
        self.causes.read(self.snapshot.first_in(ks, start, end))
    }

    fn seek_ge(&self, ks: Keyspace, key: &[u8]) -> Result<Option<KvPair>, KvError> {
        self.causes.read(self.snapshot.seek_ge(ks, key))
    }
}

pub(crate) trait KvRead: Send + Sync {
    fn get(&self, ks: Keyspace, key: &[u8]) -> Result<Option<Vec<u8>>, KvError>;
    /// Lazy: implementations must not materialize the whole result before iteration.
    /// Bounds apply in byte order for both directions; reversed/empty bounds are empty.
    fn range(
        &self,
        ks: Keyspace,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        dir: Direction,
    ) -> Result<KvIter<'_>, KvError>;
    /// First pair in the bytewise half-open interval `[start, end)`.
    /// Empty or reversed intervals return no pair.
    fn first_in(&self, ks: Keyspace, start: &[u8], end: &[u8]) -> Result<Option<KvPair>, KvError> {
        self.range(
            ks,
            Bound::Included(start),
            Bound::Excluded(end),
            Direction::Forward,
        )?
        .next()
        .transpose()
    }
    fn seek_ge(&self, ks: Keyspace, key: &[u8]) -> Result<Option<KvPair>, KvError> {
        self.range(
            ks,
            Bound::Included(key),
            Bound::Unbounded,
            Direction::Forward,
        )?
        .next()
        .transpose()
    }
}

pub(crate) trait KvBackend: KvRead {
    #[cfg(feature = "engine-rocksdb")]
    fn close_latch(&self) -> Option<super::kv_rocksdb::CloseLatch> {
        None
    }


    /// An error can have an ambiguous outcome, including a fully durable commit.
    /// Callers must fence rather than retry the batch on the same handle.
    fn write(&self, batch: WriteBatch, durability: Durability) -> Result<(), KvError>;
    /// Makes every preceding successful write durable. Errors are ambiguous too.
    fn flush_barrier(&self) -> Result<(), KvError>;
    /// Whether WAL sync can overlap a write on this backend.
    /// Wrappers use the false default. An opt-in must explicitly implement
    /// both commit operations and preserve their ordering and failure semantics.
    fn pipelined_sync(&self) -> bool {
        false
    }
    fn append_commit(&self, _batch: WriteBatch) -> Result<(), KvError> {
        Err(KvError::Io("commit append requires a pipeline-capable backend".into()))
    }
    fn sync_commits(&self) -> Result<(), KvError> {
        self.flush_barrier()
    }
    /// A backend with a latched sync failure rejects concurrent calls while
    /// the call that failed is still reporting the original error.
    fn sync_failure_latched(&self) -> bool {
        false
    }
    fn snapshot(&self) -> Result<Arc<dyn KvRead>, KvError>;
    fn engine_name(&self) -> &'static str;
}

/// Shared fence for ambiguous write/flush outcomes. Once fenced, subsequent calls
/// fail without consulting the backend. Already-returned snapshots/iterators retain
/// their own read view; calls in flight when another thread fences may finish.
pub(crate) struct Kv {
    backend: Arc<dyn KvBackend>,
    fenced: Arc<AtomicBool>,
    single_writer: bool,
    pub(crate) causes: Arc<FenceState>,
}

impl Kv {
    pub(crate) fn new(backend: Arc<dyn KvBackend>) -> Self {
        Self {
            backend,
            fenced: Arc::new(AtomicBool::new(false)),
            single_writer: false,
            causes: Arc::default(),
        }
    }
    /// Only for a backend whose versioned writes all pass through this engine's
    /// sequencer and locks. Attached/shared backends must use `new` instead.
    #[cfg(any(test, feature = "engine-rocksdb"))]
    pub(crate) fn new_single_writer(backend: Arc<dyn KvBackend>) -> Self {
        Self {
            single_writer: true,
            ..Self::new(backend)
        }
    }
    #[cfg(feature = "engine-rocksdb")]
    pub(crate) fn close_latch(&self) -> Option<super::kv_rocksdb::CloseLatch> {
        self.backend.close_latch()
    }

    pub(crate) fn is_single_writer(&self) -> bool {
        self.single_writer
    }
    pub(crate) fn pipelined_sync(&self) -> bool {
        self.is_single_writer() && self.backend.pipelined_sync()
    }
    /// Probe SyncWAL before admitting any appends. Unsupported WAL files use
    /// serial Sync writes. Other probe errors fail the open.
    pub(super) fn enable_commit_pipeline(&self) -> Result<bool, KvError> {
        self.check()?;
        let result = self.backend.sync_commits();
        if let Err(KvError::RocksIo { status, .. }) = &result
            && status.code == RocksStatus::NOT_SUPPORTED
        {
            return Ok(false);
        }
        result?;
        self.causes.enable_commit_pipeline();
        Ok(true)
    }

    /// Fences the handle: every later call fails without reaching the backend.
    pub(crate) fn fence(&self) {
        if !self.fenced.swap(true, Ordering::AcqRel) {
            self.causes.record_explicit_pipeline_fence(self.backend.as_ref());
            #[cfg(feature = "engine-rocksdb")]
            self.causes.signal_recovery();
            super::diagnostics::error("KV fenced".into());
        }
    }
    pub(crate) fn is_fenced(&self) -> bool {
        self.fenced.load(Ordering::Acquire)
    }
    /// Shared view of the fence flag for lease-free observers (engine slot).
    pub(crate) fn fence_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.fenced)
    }
    fn check(&self) -> Result<(), KvError> {
        if self.is_fenced() {
            Err(KvError::Fenced)
        } else {
            Ok(())
        }
    }
    pub(crate) fn get(&self, ks: Keyspace, key: &[u8]) -> Result<Option<Vec<u8>>, KvError> {
        self.check()?;
        self.causes.read(self.backend.get(ks, key))
    }
    pub(crate) fn range(
        &self,
        ks: Keyspace,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        dir: Direction,
    ) -> Result<KvIter<'_>, KvError> {
        self.check()?;
        self.causes.range(self.backend.range(ks, start, end, dir))
    }
    pub(crate) fn seek_ge(&self, ks: Keyspace, key: &[u8]) -> Result<Option<KvPair>, KvError> {
        self.check()?;
        self.causes.read(self.backend.seek_ge(ks, key))
    }
    pub(crate) fn write(&self, batch: WriteBatch, durability: Durability) -> Result<(), KvError> {
        self.check()?;
        #[cfg(test)]
        if self.single_writer && durability == Durability::NoSync {
            assert_local_no_sync(&batch);
        }
        let result = self.backend.write(batch, durability);
        self.fence_on_error(FenceSite::Write, result)
    }
    /// Appends a commit batch to the WAL without waiting for durability.
    pub(crate) fn append_commit(&self, batch: WriteBatch) -> Result<(), KvError> {
        self.check()?;
        if !self.pipelined_sync() {
            return Err(KvError::Io("commit append requires a pipeline-capable single writer".into()));
        }
        // This entry point admits the sequencer's commit batch to local NoSync.
        let result = self.backend.append_commit(batch);
        self.fence_on_error(FenceSite::CommitAppend, result)
    }
    /// Makes preceding successful commit appends durable.
    pub(crate) fn sync_commits(&self) -> Result<(), KvError> {
        self.check()?;
        let result = self.backend.sync_commits();
        self.fence_on_error(FenceSite::CommitSync, result)
    }

    #[cfg(all(test, feature = "engine-rocksdb"))]
    pub(super) fn test_commit_wal_error(&self, site: FenceSite) -> Result<(), KvError> {
        self.fence_on_error(site, Err(KvError::RocksIo {
            message: "injected commit WAL error".into(),
            status: RocksStatus { code: RocksStatus::IO_ERROR, subcode: 0, severity: None },
            site,
        }))
    }

    #[cfg(all(test, feature = "engine-rocksdb"))]
    pub(super) fn test_sync_wal_error(
        &self,
        batch: WriteBatch,
        after_apply: bool,
    ) -> Result<(), KvError> {
        self.check()?;
        if after_apply {
            self.write(batch, Durability::Sync)?;
        }
        // Inject the backend result before it is classified and reported.
        // AfterApply makes the native batch durable before reporting failure.
        self.fence_on_error(FenceSite::Write, Err(KvError::RocksIo {
            message: "injected commit WAL error".into(),
            status: RocksStatus { code: RocksStatus::IO_ERROR, subcode: 0, severity: None },
            site: FenceSite::WalWrite,
        }))
    }

    #[cfg(all(test, feature = "engine-rocksdb"))]
    pub(in crate::storage) fn test_report_write_error(&self, error: KvError) {
        let _ = self.fence_on_error(FenceSite::Write, Err(error));
    }

    #[cfg(all(test, feature = "engine-rocksdb"))]
    pub(in crate::storage) fn test_report_read_error(&self, error: &KvError) {
        self.causes.read_error(error);
    }

    pub(crate) fn flush_barrier(&self) -> Result<(), KvError> {
        self.check()?;
        let result = self.backend.flush_barrier();
        self.fence_on_error(FenceSite::FlushBarrier, result)
    }
    fn fence_on_error(&self, site: FenceSite, result: Result<(), KvError>) -> Result<(), KvError> {
        if let Err(error) = &result
            // A backend already stopping after an error can reject concurrent
            // callers. Leave the fence cause to the call that failed.
            && !(*error == KvError::Fenced && self.backend.sync_failure_latched())
            && self.causes.record_fence(FenceCause::for_kv(site, error), &self.fenced, false)
        {
            super::diagnostics::error(format!(
                "KV fenced after backend write/flush failure: {}",
                super::diagnostics::error_text(error),
            ));
        }
        result
    }
    pub(crate) fn snapshot(&self) -> Result<Arc<dyn KvRead>, KvError> {
        self.check()?;
        let snapshot = self.causes.read(self.backend.snapshot())?;
        Ok(Arc::new(ReportingSnapshot {
            snapshot,
            causes: self.causes.clone(),
        }))
    }
}

// Design note 6.2a(4). Check every local NoSync batch passed to write(),
// including GC delete chunks that leave the cursor unchanged. Commit batches
// enter through append_commit().
#[cfg(test)]
fn assert_local_no_sync(batch: &WriteBatch) {
    let safe = match batch.ops() {
        [
            Op::DeleteRange {
                ks: Keyspace::Rows,
                start,
                end,
            },
        ] => <[u8; 8]>::try_from(start.as_slice()).is_ok_and(|prefix| {
            match u64::from_be_bytes(prefix).checked_add(1) {
                Some(next) => end.as_slice() == next.to_be_bytes(),
                None => end.starts_with(&prefix) && end.last() == Some(&0),
            }
        }),
        [
            Op::DeleteRange {
                ks: Keyspace::Seq,
                start,
                end,
            },
        ] => {
            start == &super::xact::marker_key(0)
                && end.len() == start.len()
                && end.first() == start.first()
                && start < end
        }
        ops => [
            Keyspace::Catalog,
            Keyspace::Rows,
            Keyspace::Index,
            Keyspace::Kernel,
        ]
        .into_iter()
        .any(|ks| {
            let deletes = |op: &Op| {
                matches!(op, Op::Delete { ks: target, key }
                    if *target == ks && super::codec::split_versioned(key).is_ok())
            };
            match ops.split_last() {
                Some((
                    Op::Put {
                        ks: Keyspace::Gc,
                        key,
                        ..
                    },
                    rest,
                )) => key.as_slice() == [ks as u8] && rest.iter().all(deletes),
                Some(_) => ops.iter().all(deletes),
                None => false,
            }
        }),
    };
    assert!(safe, "unrecognized local NoSync batch: {batch:?}");
}

impl KvRead for Kv {
    fn get(&self, ks: Keyspace, key: &[u8]) -> Result<Option<Vec<u8>>, KvError> {
        Kv::get(self, ks, key)
    }
    fn range(
        &self,
        ks: Keyspace,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        dir: Direction,
    ) -> Result<KvIter<'_>, KvError> {
        Kv::range(self, ks, start, end, dir)
    }
    fn first_in(&self, ks: Keyspace, start: &[u8], end: &[u8]) -> Result<Option<KvPair>, KvError> {
        self.check()?;
        self.causes.read(self.backend.first_in(ks, start, end))
    }
    fn seek_ge(&self, ks: Keyspace, key: &[u8]) -> Result<Option<KvPair>, KvError> {
        Kv::seek_ge(self, ks, key)
    }
}

#[cfg(test)]
#[path = "kv_codec_tests.rs"]
mod codec_tests;
