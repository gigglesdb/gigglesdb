//! Transaction assembly and immutable segment publication.
//!
//! Spilled transactions and segments stay file-backed through manifest durability.
//! Admission encodes directly into the bounded spool; recovery reads and validates
//! one event at a time. Complete transaction digests provide redelivery history.
//! Call `seal_if_due` on the capture loop's timer, including while input is idle.

use super::{
    backends::{self, BufferStore},
    format::{self, Limits, ManifestEntry, SegmentDecoder, SegmentEncoder, SegmentHeader},
};
use crate::event::{
    AssembledTxn, ContentDigest, Event, EventError, Position, PositionOrder, Redelivery,
    RedeliveryCheck, SourceIdentity, StreamingTxn, TransactionMetadata, TxnId,
};
use crate::fsconfine::Dir;
use std::{
    cmp::Ordering,
    collections::HashMap,
    fmt,
    fs::File,
    io::{self, Cursor, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{Arc, mpsc::Sender},
    time::{Duration, Instant},
};

pub const MAX_TRANSACTION_BYTES: u64 = 1_000_000_000;
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Invalid(&'static str),
    RecoveryRequired(&'static str),
    Event(EventError),
    Format(format::FormatError),
    Store(backends::Error),
    Io(io::Error),
    OversizedTransaction {
        bytes: u64,
        maximum: u64,
    },
    SteppedDown {
        writer_epoch: u64,
        manifest_epoch: u64,
    },
    ManifestConflict,
    Blocked,
    #[cfg(feature = "test-crash")]
    InjectedCrash(CrashPoint),
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RecoveryRequired(reason) => {
                write!(f, "buffer writer recovery-required: {reason}")
            }
            _ => write!(f, "buffer writer: {self:?}"),
        }
    }
}
impl std::error::Error for Error {}
impl From<EventError> for Error {
    fn from(value: EventError) -> Self {
        Self::Event(value)
    }
}
impl From<format::FormatError> for Error {
    fn from(value: format::FormatError) -> Self {
        Self::Format(value)
    }
}
impl From<backends::Error> for Error {
    fn from(value: backends::Error) -> Self {
        Self::Store(value)
    }
}
impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

/// Retained scratch directory capability and a path used only for diagnostics.
struct ScratchRoot {
    dir: Dir,
    display: PathBuf,
}

/// A product-owned file. Drop removes only the unique file created here; the
/// configured scratch root must be private to the product, like the backend root.
struct Scratch {
    file: Option<File>,
    root: Dir,
    name: String,
    path: PathBuf,
}
impl Scratch {
    fn new(root: &ScratchRoot) -> Result<Self> {
        let name = format!("txn-{}.skedaddle-scratch", uuid::Uuid::new_v4());
        let file = root.dir.create_file(&name)?;
        let scratch = Self {
            file: Some(file),
            root: root.dir.clone(),
            path: root.display.join(&name),
            name,
        };
        scratch
            .file
            .as_ref()
            .expect("live scratch file")
            .try_lock()
            .map_err(io::Error::other)?;
        Ok(scratch)
    }
    fn file(&mut self) -> &mut File {
        self.file.as_mut().expect("live scratch file")
    }
}
impl Drop for Scratch {
    fn drop(&mut self) {
        // Scratch is replayable input, never the durability/ack point. Cleanup
        // failure is left for product startup cleanup; never delete other files.
        if let Some(file) = &self.file {
            let _ = self.root.remove_open_file(&self.name, file);
        }
        drop(self.file.take());
    }
}

impl Read for Scratch {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        self.file().read(bytes)
    }
}
impl Write for Scratch {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.file().write(bytes)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.file().flush()
    }
}
impl Seek for Scratch {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        self.file().seek(position)
    }
}

/// Validated canonical transaction whose scratch owner lives until publication.
/// Construct with TransactionAssembler::finish; replay never materializes it.
pub struct PreparedTxn {
    inner: StreamingTxn<Scratch>,
    chunk_rows: bool,
}
impl PreparedTxn {
    pub fn metadata(&self) -> &TransactionMetadata {
        self.inner.metadata()
    }
    pub fn digest(&self) -> ContentDigest {
        self.inner.digest()
    }
    pub fn canonical_len(&self) -> u64 {
        self.inner.canonical_len()
    }
    pub fn event_count(&self) -> u32 {
        self.inner.event_count()
    }
    pub fn canonical_reader(&mut self) -> Result<impl Read + '_> {
        Ok(self.inner.canonical_reader()?)
    }
}

enum Spool {
    Memory(Vec<u8>),
    Disk(Scratch),
}

struct CountBytes<'a>(&'a mut u64);
impl Write for CountBytes<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        *self.0 += bytes.len() as u64;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub struct TransactionAssembler {
    source: SourceIdentity,
    txn: TxnId,
    position: Position,
    commit_time: Option<i64>,
    budget: usize,
    maximum: u64,
    scratch_root: ScratchRoot,
    spool: Spool,
    count: u32,
    last_ordinal: Option<u64>,
    canonical_bytes: u64,
    blocked: bool,
    peak_spool_bytes: usize,
    reclamation: backends::ScratchReclamation,
}
impl TransactionAssembler {
    pub fn new(
        source: SourceIdentity,
        txn: TxnId,
        commit_position: Position,
        commit_time: Option<i64>,
        memory_budget: usize,
        max_transaction_bytes: u64,
        scratch_root: impl Into<PathBuf>,
    ) -> Result<Self> {
        validate_max(max_transaction_bytes)?;
        if memory_budget == 0 {
            return Err(Error::Invalid("zero assembler budget"));
        }
        // The configured scratch root must already exist and be private, just
        // like the local backend root. Unknown paths are audited, never removed.
        let scratch_root = scratch_root.into();
        let dir = Dir::open_root(&scratch_root)?;
        let reclamation = backends::reclaim_scratch_in(&dir, &scratch_root)?;
        let scratch_root = ScratchRoot {
            dir,
            display: scratch_root,
        };
        let empty = AssembledTxn::new(
            source.clone(),
            txn.clone(),
            commit_position.clone(),
            commit_time,
            vec![],
        )?;
        let mut canonical_bytes = 0;
        empty.write_canonical(&mut CountBytes(&mut canonical_bytes))?;
        if canonical_bytes > max_transaction_bytes {
            return Err(Error::OversizedTransaction {
                bytes: canonical_bytes,
                maximum: max_transaction_bytes,
            });
        }
        Ok(Self {
            source,
            txn,
            position: commit_position,
            commit_time,
            budget: memory_budget,
            maximum: max_transaction_bytes,
            scratch_root,
            spool: Spool::Memory(Vec::new()),
            count: 0,
            last_ordinal: None,
            canonical_bytes,
            blocked: false,
            peak_spool_bytes: 0,
            reclamation,
        })
    }

    pub fn spilled_path(&self) -> Option<&Path> {
        match &self.spool {
            Spool::Memory(_) => None,
            Spool::Disk(s) => Some(&s.path),
        }
    }
    pub fn startup_reclamation(&self) -> &backends::ScratchReclamation {
        &self.reclamation
    }
    /// Retained spool capacity, excluding the caller's Event and its temporary
    /// canonical encoding. Qualification also measures real live allocations.
    pub fn peak_spool_bytes(&self) -> usize {
        self.peak_spool_bytes
    }
    pub fn canonical_bytes(&self) -> u64 {
        self.canonical_bytes
    }

    pub fn push(&mut self, event: Event) -> Result<Redelivery> {
        if self.blocked {
            return Err(Error::Blocked);
        }
        if event.source != self.source || event.txn != self.txn {
            return Err(EventError::InvalidTransaction("assembler membership").into());
        }
        if event.position.order_domain() != self.position.order_domain() {
            return Err(EventError::PositionDomainMismatch.into());
        }
        // Repeats are exceptional: scan the bounded spool rather than retaining
        // an unbounded ordinal/digest index alongside a large transaction.
        if self.last_ordinal.is_some_and(|n| event.ordinal_in_txn <= n) {
            let mut reader = self.reader()?;
            for _ in 0..self.count {
                let old = read_event(&mut reader, self.maximum)?;
                if old.ordinal_in_txn == event.ordinal_in_txn {
                    let mut check = RedeliveryCheck::default();
                    check.check_event(&old)?;
                    return Ok(check.check_event(&event)?);
                }
            }
            return Err(EventError::InvalidTransaction("assembler ordinal order").into());
        }
        let mut bytes = 0;
        event.write_canonical(&mut CountBytes(&mut bytes))?;
        // P3a v1 embeds event fields without their 7-byte envelope and 32-byte
        // digest. This size accounting is pinned against P3a in integration tests.
        let total = self
            .canonical_bytes
            .checked_add(bytes - 39)
            .ok_or(Error::Invalid("transaction size overflow"))?;
        if total > self.maximum {
            self.blocked = true;
            return Err(Error::OversizedTransaction {
                bytes: total,
                maximum: self.maximum,
            });
        }
        let count = self
            .count
            .checked_add(1)
            .ok_or(Error::Invalid("event count overflow"))?;
        self.blocked = true; // An I/O error must never allow a partial spool commit.
        if let Spool::Memory(memory) = &self.spool
            && memory.len().saturating_add(8).saturating_add(
                usize::try_from(bytes).map_err(|_| Error::Invalid("event size overflow"))?,
            ) > self.budget
        {
            let mut scratch = Scratch::new(&self.scratch_root)?;
            scratch.file().write_all(memory)?;
            self.spool = Spool::Disk(scratch);
        }
        match &mut self.spool {
            Spool::Memory(memory) => {
                memory.reserve_exact(8 + bytes as usize);
                memory.extend(bytes.to_be_bytes());
                event.write_canonical(memory)?;
                self.peak_spool_bytes = self.peak_spool_bytes.max(memory.capacity());
            }
            Spool::Disk(scratch) => {
                scratch.file().seek(SeekFrom::End(0))?;
                scratch.file().write_all(&bytes.to_be_bytes())?;
                event.write_canonical(scratch.file())?;
            }
        }
        self.count = count;
        self.last_ordinal = Some(event.ordinal_in_txn);
        self.canonical_bytes = total;
        self.blocked = false;
        Ok(Redelivery::New)
    }

    fn reader(&self) -> Result<Box<dyn Read + '_>> {
        Ok(match &self.spool {
            Spool::Memory(memory) => Box::new(Cursor::new(memory.as_slice())),
            Spool::Disk(scratch) => {
                let mut file = scratch
                    .file
                    .as_ref()
                    .expect("live scratch file")
                    .try_clone()?;
                file.rewind()?;
                Box::new(file)
            }
        })
    }

    /// Replays one spooled event at a time into a canonical file. Neither the
    /// transaction nor its eventual segment is materialized in memory.
    pub fn finish(self) -> Result<PreparedTxn> {
        if self.blocked {
            return Err(Error::Blocked);
        }
        let storage = Scratch::new(&self.scratch_root)?;
        let mut reader = self.reader()?;
        let events = (0..self.count).map(|_| {
            read_event(&mut reader, self.maximum).map_err(|e| match e {
                Error::Event(e) => e,
                Error::Io(e) => e.into(),
                _ => EventError::InvalidEncoding("scratch event"),
            })
        });
        let inner = StreamingTxn::from_events(
            TransactionMetadata {
                source: self.source.clone(),
                txn: self.txn.clone(),
                commit_position: self.position.clone(),
                commit_time: self.commit_time,
            },
            self.count,
            events,
            storage,
        )?;
        Ok(PreparedTxn {
            inner,
            chunk_rows: false,
        })
    }
}

fn read_event(reader: &mut impl Read, maximum: u64) -> Result<Event> {
    let mut length = [0; 8];
    reader.read_exact(&mut length)?;
    let length = u64::from_be_bytes(length);
    if length > maximum.saturating_add(39) {
        return Err(Error::Invalid("scratch frame length"));
    }
    Ok(Event::read_canonical(reader, length)?)
}
fn validate_max(maximum: u64) -> Result<()> {
    if maximum == 0 || maximum > MAX_TRANSACTION_BYTES {
        return Err(Error::Invalid("transaction maximum must be in 1..=1 GB"));
    }
    Ok(())
}

#[derive(Clone, Debug)]
pub struct WriterConfig {
    pub prefix: String,
    pub migration_id: String,
    pub stream: String,
    pub epoch: u64,
    /// A seal trigger, not a transaction splitting limit. One transaction can
    /// exceed this threshold, up to max_transaction_bytes.
    pub segment_bytes: u64,
    pub segment_transactions: u64,
    pub segment_age: Duration,
    pub max_transaction_bytes: u64,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BufferedProgress {
    pub position: Position,
    pub mseq: u64,
    pub epoch: u64,
    pub manifest_digest: ContentDigest,
}

/// Evidence returned only after the containing manifest is durable.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DurableManifestRef {
    pub mseq: u64,
    pub manifest_digest: ContentDigest,
}

/// Trusted coordinator state for one configured stream, at a durable manifest
/// boundary. Persist this before authorizing GC of any preceding manifests or
/// segments. Never GC the suffix starting at `next_mseq` until a newer writer
/// checkpoint has been persisted. As with ReaderCheckpoint, the coordinator
/// owns storage, stream binding and ownership fencing for this value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriterCheckpoint {
    /// Reject older schemas rather than guessing whether the H slot was used.
    pub version: u16,
    pub next_mseq: u64,
    pub prev_digest: ContentDigest,
    /// Epoch of the preceding durable manifest (zero at genesis).
    pub last_epoch: u64,
    pub last_position: Option<Position>,
    /// Retained independently of redelivery history, including after GC.
    pub last_record: Option<format::RecordBoundary>,
    /// Inclusive lower bound of retained redelivery coverage. None means the
    /// full history from genesis; positions below Some(bound) require recovery.
    pub redelivery_from: Option<Position>,
    pub redelivery: Vec<RetainedRedelivery>,
}

impl Default for WriterCheckpoint {
    fn default() -> Self {
        Self {
            version: format::CHECKPOINT_VERSION,
            next_mseq: 0,
            prev_digest: [0; 32],
            last_epoch: 0,
            last_position: None,
            last_record: None,
            redelivery_from: None,
            redelivery: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetainedRedelivery {
    pub source: SourceIdentity,
    pub txn: TxnId,
    pub position: Position,
    pub digest: ContentDigest,
    pub chunk_rows: bool,
    /// None only while admitted but not yet durable.
    pub manifest: Option<DurableManifestRef>,
}

fn validate_checkpoint(checkpoint: &WriterCheckpoint, order: &dyn PositionOrder) -> Result<()> {
    if checkpoint.version != format::CHECKPOINT_VERSION {
        return Err(format::FormatError::UnsupportedFormatVersion {
            format: "writer-checkpoint",
            found: Some(checkpoint.version),
            expected: format::CHECKPOINT_VERSION,
        }
        .into());
    }
    if checkpoint.next_mseq == 0 {
        return if checkpoint == &WriterCheckpoint::default() {
            Ok(())
        } else {
            Err(Error::Invalid("nonempty genesis writer checkpoint"))
        };
    }
    let last = checkpoint
        .last_position
        .as_ref()
        .ok_or(Error::Invalid("writer checkpoint missing last position"))?;
    last.compare(last, order)?;
    let boundary = checkpoint
        .last_record
        .as_ref()
        .ok_or(Error::Invalid("writer checkpoint missing last record"))?;
    if let Some(first) = &checkpoint.redelivery_from
        && first.compare(last, order)? == Ordering::Greater
    {
        return Err(Error::Invalid("writer checkpoint coverage beyond progress"));
    }
    for retained in &checkpoint.redelivery {
        if retained
            .manifest
            .as_ref()
            .is_some_and(|reference| reference.mseq >= checkpoint.next_mseq)
            || (retained.chunk_rows && retained.manifest.is_none())
        {
            return Err(Error::Invalid("writer checkpoint redelivery manifest"));
        }
        if retained.position.compare(last, order)? == Ordering::Greater {
            return Err(Error::Invalid(
                "writer checkpoint contains uncommitted history",
            ));
        }
        if retained.position == *last
            && (retained.source != boundary.source || (retained.chunk_rows && !boundary.chunk_rows))
        {
            return Err(Error::Invalid(
                "writer checkpoint boundary disagrees with history",
            ));
        }
        if let Some(first) = &checkpoint.redelivery_from
            && retained.position.compare(first, order)? == Ordering::Less
        {
            return Err(Error::Invalid("writer checkpoint history below coverage"));
        }
    }
    Ok(())
}

#[cfg(feature = "test-crash")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CrashPoint {
    BeforeSegmentPut,
    AfterSegmentPut,
    BeforeManifestPut,
    AfterManifestPut,
}

pub struct BufferWriter {
    store: Arc<dyn BufferStore>,
    config: WriterConfig,
    order: Arc<dyn PositionOrder + Send + Sync>,
    progress: Sender<BufferedProgress>,
    next_mseq: u64,
    prev_digest: ContentDigest,
    last_epoch: u64,
    last_position: Option<Position>,
    last_record: Option<format::RecordBoundary>,
    redelivery_from: Option<Position>,
    redelivery: HashMap<(SourceIdentity, TxnId), RetainedRedelivery>,
    scratch_root: ScratchRoot,
    pending: Vec<PreparedTxn>,
    segment: Option<(Scratch, format::SegmentSummary)>,
    upload: Option<tokio::task::JoinHandle<backends::Result<backends::PutOutcome>>>,
    segment_published: bool,
    manifest_upload: Option<tokio::task::JoinHandle<backends::Result<backends::PutOutcome>>>,
    pending_bytes: u64,
    oldest: Option<Instant>,
    publishing: bool,
    blocked: bool,
    #[cfg(feature = "test-crash")]
    crash: Option<CrashPoint>,
}
impl BufferWriter {
    /// Rebuilds full history from genesis by exact keys. After retention GC,
    /// resume with open_from_checkpoint instead.
    pub async fn open(
        store: Arc<dyn BufferStore>,
        config: WriterConfig,
        order: Arc<dyn PositionOrder + Send + Sync>,
        progress: Sender<BufferedProgress>,
    ) -> Result<Self> {
        // Compatibility entry point. Deployments may supply a reusable private
        // root with open_with_scratch; TEMP/TMP determine this default location.
        let parent = std::env::temp_dir();
        let name = format!("skedaddle-writer-{}", uuid::Uuid::new_v4());
        let dir = Dir::open_root(&parent)?.create_dir(&name)?;
        Self::open_confined(
            store,
            config,
            order,
            progress,
            WriterCheckpoint::default(),
            ScratchRoot {
                dir,
                display: parent.join(name),
            },
        )
        .await
    }

    pub async fn open_with_scratch(
        store: Arc<dyn BufferStore>,
        config: WriterConfig,
        order: Arc<dyn PositionOrder + Send + Sync>,
        progress: Sender<BufferedProgress>,
        scratch_root: impl Into<PathBuf>,
    ) -> Result<Self> {
        let scratch_root = scratch_root.into();
        let dir = Dir::open_root(&scratch_root)?;
        Self::open_confined(
            store,
            config,
            order,
            progress,
            WriterCheckpoint::default(),
            ScratchRoot {
                dir,
                display: scratch_root,
            },
        )
        .await
    }

    /// Resumes by exact GET from the checkpoint forward, without LIST or reads
    /// of retired objects. The supplied scratch root must exist and be private.
    /// An older checkpoint also recovers manifests published before a crash but
    /// after the last checkpoint persistence. Coordinator fencing remains
    /// required: a checkpoint alone cannot discover an unpublished newer owner.
    pub async fn open_from_checkpoint(
        store: Arc<dyn BufferStore>,
        config: WriterConfig,
        checkpoint: WriterCheckpoint,
        order: Arc<dyn PositionOrder + Send + Sync>,
        progress: Sender<BufferedProgress>,
        scratch_root: impl Into<PathBuf>,
    ) -> Result<Self> {
        let scratch_root = scratch_root.into();
        let dir = Dir::open_root(&scratch_root)?;
        Self::open_confined(
            store,
            config,
            order,
            progress,
            checkpoint,
            ScratchRoot {
                dir,
                display: scratch_root,
            },
        )
        .await
    }

    async fn open_confined(
        store: Arc<dyn BufferStore>,
        config: WriterConfig,
        order: Arc<dyn PositionOrder + Send + Sync>,
        progress: Sender<BufferedProgress>,
        checkpoint: WriterCheckpoint,
        scratch_root: ScratchRoot,
    ) -> Result<Self> {
        backends::reclaim_scratch_in(&scratch_root.dir, &scratch_root.display)?;
        validate_max(config.max_transaction_bytes)?;
        if config.segment_bytes == 0
            || config.segment_transactions == 0
            || config.segment_age.is_zero()
        {
            return Err(Error::Invalid("zero seal threshold"));
        }
        if config.epoch < checkpoint.last_epoch {
            return Err(Error::SteppedDown {
                writer_epoch: config.epoch,
                manifest_epoch: checkpoint.last_epoch,
            });
        }
        validate_checkpoint(&checkpoint, order.as_ref())?;
        let mut redelivery = HashMap::new();
        for retained in checkpoint.redelivery {
            if redelivery
                .insert((retained.source.clone(), retained.txn.clone()), retained)
                .is_some()
            {
                return Err(Error::Invalid("duplicate checkpoint redelivery identity"));
            }
        }
        let mut recovered = checkpoint
            .last_position
            .clone()
            .map(|position| BufferedProgress {
                position,
                mseq: checkpoint.next_mseq - 1,
                epoch: checkpoint.last_epoch,
                manifest_digest: checkpoint.prev_digest,
            });
        let mut writer = Self {
            store,
            config,
            order,
            progress,
            next_mseq: checkpoint.next_mseq,
            prev_digest: checkpoint.prev_digest,
            last_epoch: checkpoint.last_epoch,
            last_position: checkpoint.last_position,
            last_record: checkpoint.last_record,
            redelivery_from: checkpoint.redelivery_from,
            redelivery,
            scratch_root,
            segment: None,
            upload: None,
            segment_published: false,
            manifest_upload: None,
            pending: vec![],
            pending_bytes: 0,
            oldest: None,
            publishing: false,
            blocked: false,
            #[cfg(feature = "test-crash")]
            crash: None,
        };
        while let Some(bytes) = writer.store.get(&writer.manifest_key()).await? {
            let entry = ManifestEntry::from_canonical_json(&bytes)?;
            if entry.mseq != writer.next_mseq
                || entry.prev_digest != writer.prev_digest
                || entry.epoch < writer.last_epoch
            {
                return Err(Error::Invalid("manifest chain"));
            }
            if entry.epoch > writer.config.epoch {
                return Err(Error::SteppedDown {
                    writer_epoch: writer.config.epoch,
                    manifest_epoch: entry.epoch,
                });
            }
            let expected_key = format::boundary_segment_key(
                format::segment_key(
                    &writer.config.prefix,
                    &writer.config.migration_id,
                    &writer.config.stream,
                    &entry.first_pos,
                    &entry.last_pos,
                    entry.epoch,
                ),
                &entry.first_pos,
                writer.last_position.as_ref(),
            );
            if entry.segment_key != expected_key {
                return Err(Error::Invalid("manifest segment key"));
            }
            let mut segment = Scratch::new(&writer.scratch_root)?;
            writer
                .store
                .get_into(&entry.segment_key, &mut segment)
                .await?
                .ok_or(Error::Invalid("manifest segment missing"))?;
            segment.rewind()?;
            let mut decoder = SegmentDecoder::new(segment, writer.limits())?;
            let header = decoder.header();
            if header.migration_id != writer.config.migration_id
                || header.stream != writer.config.stream
                || header.epoch != entry.epoch
                || header.first_pos != entry.first_pos
                || header.last_pos != entry.last_pos
                || header.record_count != entry.txn_count
            {
                return Err(Error::Invalid("manifest/header mismatch"));
            }
            let manifest_digest = entry.digest()?;
            while let Some((chunk_rows, txn)) =
                decoder.next_transaction_with_kind(Scratch::new(&writer.scratch_root)?)?
            {
                writer.check_position(txn.metadata(), chunk_rows)?;
                if writer.check_redelivery(txn.metadata(), txn.digest(), chunk_rows)?
                    != Redelivery::New
                {
                    return Err(Error::Invalid("duplicate transaction in manifest chain"));
                }
                writer
                    .redelivery
                    .get_mut(&(txn.metadata().source.clone(), txn.metadata().txn.clone()))
                    .expect("recovered redelivery")
                    .manifest = Some(DurableManifestRef {
                    mseq: entry.mseq,
                    manifest_digest,
                });
                writer.last_position = Some(txn.metadata().commit_position.clone());
                writer.last_record = Some(format::RecordBoundary {
                    source: txn.metadata().source.clone(),
                    chunk_rows,
                });
            }
            let (_, summary) = decoder.finish()?;
            if summary.digest != entry.segment_digest || summary.bytes != entry.bytes {
                return Err(Error::Invalid("manifest/segment digest or size"));
            }
            writer.prev_digest = manifest_digest;
            writer.next_mseq = writer
                .next_mseq
                .checked_add(1)
                .ok_or(Error::Invalid("manifest sequence exhausted"))?;
            writer.last_epoch = entry.epoch;
            recovered = Some(BufferedProgress {
                position: entry.last_pos,
                mseq: entry.mseq,
                epoch: entry.epoch,
                manifest_digest: writer.prev_digest,
            });
        }
        if let Some(progress) = recovered {
            let _ = writer.progress.send(progress);
        }
        Ok(writer)
    }
    fn limits(&self) -> Limits {
        Limits {
            max_record_bytes: self.config.max_transaction_bytes,
            ..Limits::default()
        }
    }
    fn manifest_key(&self) -> String {
        format::manifest_key(
            &self.config.prefix,
            &self.config.migration_id,
            &self.config.stream,
            self.next_mseq,
        )
    }
    fn check_position(&self, metadata: &TransactionMetadata, chunk_rows: bool) -> Result<()> {
        let pending_boundary = self.pending.last().map(|txn| format::RecordBoundary {
            source: txn.metadata().source.clone(),
            chunk_rows: txn.chunk_rows,
        });
        let previous = match (self.pending.last(), pending_boundary.as_ref()) {
            (Some(txn), Some(boundary)) => Some((&txn.metadata().commit_position, boundary)),
            _ => self.last_position.as_ref().zip(self.last_record.as_ref()),
        };
        if !format::record_follows(
            &metadata.commit_position,
            &metadata.source,
            chunk_rows,
            previous,
            self.order.as_ref(),
        )? {
            return Err(Error::Invalid("commit positions must strictly increase"));
        }
        Ok(())
    }
    pub fn is_blocked(&self) -> bool {
        self.blocked
    }
    pub fn pending_transactions(&self) -> usize {
        self.pending.len()
    }
    pub fn next_manifest_sequence(&self) -> u64 {
        self.next_mseq
    }

    /// Only durable transactions are included, even if a segment is pending.
    /// Persist the returned value atomically under coordinator ownership before
    /// acknowledging a manifest GC intent. No persistence is performed here.
    pub fn checkpoint(&self) -> Result<WriterCheckpoint> {
        if self.blocked || self.publishing {
            return Err(Error::Blocked);
        }
        let mut redelivery = Vec::new();
        if let Some(last) = &self.last_position {
            for retained in self.redelivery.values() {
                if retained.manifest.is_some()
                    && retained.position.compare(last, self.order.as_ref())? != Ordering::Greater
                {
                    redelivery.push(retained.clone());
                }
            }
        }
        // Canonical export only: stored checkpoints retain their original byte
        // digests and readers continue accepting every prior vector order.
        redelivery.sort_by(|a, b| {
            let key = |r: &RetainedRedelivery| {
                (
                    r.source.engine.clone(),
                    r.source.system_id.clone(),
                    r.source.lineage.clone(),
                    r.source.db_name.clone(),
                    r.source.stream.clone(),
                    r.txn.0.clone(),
                )
            };
            key(a).cmp(&key(b))
        });
        Ok(WriterCheckpoint {
            version: format::CHECKPOINT_VERSION,
            next_mseq: self.next_mseq,
            prev_digest: self.prev_digest,
            last_epoch: self.last_epoch,
            last_position: self.last_position.clone(),
            last_record: self.last_record.clone(),
            redelivery_from: self.redelivery_from.clone(),
            redelivery,
        })
    }

    /// Explicitly retires redelivery history strictly before an inclusive
    /// connector position. The boundary must advance monotonically and cannot
    /// exceed durable progress. Caller must ensure its source restart contract
    /// accepts recovery-required for any delivery below this boundary.
    pub fn retain_redelivery_from(&mut self, position: Position) -> Result<()> {
        if self.blocked || self.publishing {
            return Err(Error::Blocked);
        }
        position.compare(&position, self.order.as_ref())?;
        let Some(last) = &self.last_position else {
            return Err(Error::Invalid("cannot compact before durable progress"));
        };
        if position.compare(last, self.order.as_ref())? == Ordering::Greater {
            return Err(Error::Invalid(
                "redelivery coverage must advance within durable progress",
            ));
        }
        if let Some(old) = &self.redelivery_from
            && position.compare(old, self.order.as_ref())? == Ordering::Less
        {
            return Err(Error::Invalid("redelivery coverage cannot move backward"));
        }
        // Validate comparisons before mutating, so a connector error cannot
        // leave a partially compacted history claiming its previous coverage.
        let mut remove = Vec::new();
        for (key, retained) in &self.redelivery {
            if retained.position.compare(&position, self.order.as_ref())? == Ordering::Less {
                remove.push(key.clone());
            }
        }
        for key in remove {
            self.redelivery.remove(&key);
        }
        self.redelivery_from = Some(position);
        Ok(())
    }

    fn header(&self) -> Option<SegmentHeader> {
        Some(SegmentHeader {
            migration_id: self.config.migration_id.clone(),
            stream: self.config.stream.clone(),
            epoch: self.config.epoch,
            first_pos: self.pending.first()?.metadata().commit_position.clone(),
            last_pos: self.pending.last()?.metadata().commit_position.clone(),
            record_count: self.pending.len() as u64,
        })
    }
    fn pending_segment_bytes(&self) -> Result<u64> {
        let Some(header) = self.header() else {
            return Ok(0);
        };
        let mut header_bytes = 0;
        // Ask P3b to encode/count its own header. No duplicate header codec.
        drop(SegmentEncoder::new(
            CountBytes(&mut header_bytes),
            header,
            self.limits(),
        )?);
        // P3b v1 trailer: zero length, end magic, count, SHA-256.
        Ok(header_bytes
            .saturating_add(self.pending_bytes)
            .saturating_add(56))
    }

    fn check_redelivery(
        &mut self,
        metadata: &TransactionMetadata,
        digest: ContentDigest,
        chunk_rows: bool,
    ) -> Result<Redelivery> {
        if let Some(first) = &self.redelivery_from
            && metadata
                .commit_position
                .compare(first, self.order.as_ref())?
                == Ordering::Less
        {
            self.blocked = true;
            return Err(Error::RecoveryRequired(
                "redelivery below retained coverage",
            ));
        }
        let key = (metadata.source.clone(), metadata.txn.clone());
        if let Some(old) = self.redelivery.get(&key) {
            return if old.digest == digest && old.chunk_rows == chunk_rows {
                Ok(Redelivery::Noop)
            } else {
                Err(EventError::RedeliveryConflict {
                    txn: metadata.txn.clone(),
                    ordinal: None,
                }
                .into())
            };
        }
        if self.redelivery_from.is_some()
            && let Some(last) = &self.last_position
            && metadata
                .commit_position
                .compare(last, self.order.as_ref())?
                != Ordering::Greater
            && self.check_position(metadata, chunk_rows).is_err()
        {
            self.blocked = true;
            return Err(Error::RecoveryRequired(
                "old position has no retained redelivery proof",
            ));
        }
        self.redelivery.insert(
            key,
            RetainedRedelivery {
                source: metadata.source.clone(),
                txn: metadata.txn.clone(),
                position: metadata.commit_position.clone(),
                digest,
                chunk_rows,
                manifest: None,
            },
        );
        Ok(Redelivery::New)
    }

    /// Compatibility for already-owned callers. Encoding is still file-backed.
    /// Cancellation retains admitted records and any in-flight publication.
    /// Retry with the identical transaction (checked redelivery), or call seal;
    /// a later append first finishes the frozen publication before admission.
    pub async fn append(&mut self, txn: AssembledTxn, now: Instant) -> Result<Redelivery> {
        if self.blocked {
            return Err(Error::Blocked);
        }
        let mut size = 0;
        txn.write_canonical(&mut CountBytes(&mut size))?;
        if size > self.config.max_transaction_bytes {
            self.blocked = true;
            return Err(Error::OversizedTransaction {
                bytes: size,
                maximum: self.config.max_transaction_bytes,
            });
        }
        let mut storage = Scratch::new(&self.scratch_root)?;
        txn.write_canonical(&mut storage)?;
        drop(txn);
        let inner = StreamingTxn::from_storage(storage)?;
        self.append_streaming(
            PreparedTxn {
                inner,
                chunk_rows: false,
            },
            now,
        )
        .await
    }

    /// Publishes one typed snapshot batch and returns its durable manifest
    /// evidence. Unlike ordinary append, this seals even below batch thresholds.
    /// A cancelled or failed call retains admission/publication state; identical
    /// redelivery resolves it and returns the original durable reference.
    pub async fn append_chunk_rows(
        &mut self,
        txn: AssembledTxn,
        now: Instant,
    ) -> Result<(Redelivery, DurableManifestRef)> {
        if self.blocked {
            return Err(Error::Blocked);
        }
        // Use the format codec for all ChunkRows semantics. Admission remains
        // file-backed after this already-owned input is validated.
        format::validate_chunk(&txn)?;
        let mut size = 38;
        txn.write_canonical(&mut CountBytes(&mut size))?;
        if size > self.config.max_transaction_bytes {
            self.blocked = true;
            return Err(Error::OversizedTransaction {
                bytes: size,
                maximum: self.config.max_transaction_bytes,
            });
        }
        let key = (txn.source.clone(), txn.txn.clone());
        let mut storage = Scratch::new(&self.scratch_root)?;
        txn.write_canonical(&mut storage)?;
        drop(txn);
        let inner = StreamingTxn::from_storage(storage)?;
        let outcome = self
            .append_streaming(
                PreparedTxn {
                    inner,
                    chunk_rows: true,
                },
                now,
            )
            .await?;
        if self
            .redelivery
            .get(&key)
            .is_some_and(|retained| retained.manifest.is_none())
        {
            self.seal().await?;
        }
        let reference = self
            .redelivery
            .get(&key)
            .and_then(|retained| retained.manifest.clone())
            .ok_or(Error::Invalid("missing durable ChunkRows reference"))?;
        Ok((outcome, reference))
    }

    pub async fn append_streaming(&mut self, txn: PreparedTxn, now: Instant) -> Result<Redelivery> {
        if self.blocked {
            return Err(Error::Blocked);
        }
        // Freeze a seal across failures and cancellation: never change the
        // content attempted at mseq until its uncertain outcome is resolved.
        if self.publishing {
            self.seal().await?;
        }
        let size = txn.canonical_len() + if txn.chunk_rows { 38 } else { 0 };
        if size > self.config.max_transaction_bytes {
            self.blocked = true;
            return Err(Error::OversizedTransaction {
                bytes: size,
                maximum: self.config.max_transaction_bytes,
            });
        }
        // PreparedTxn validates every event and the complete transaction digest.
        let outcome = self.check_redelivery(txn.metadata(), txn.digest(), txn.chunk_rows)?;
        if outcome == Redelivery::Noop {
            return Ok(outcome);
        }
        if let Err(error) = self.check_position(txn.metadata(), txn.chunk_rows) {
            self.blocked = true;
            return Err(error);
        }
        // Admit before awaiting a put: cancellation/errors leave the complete
        // transaction pending, never a dedup key with its transaction lost.
        self.oldest.get_or_insert(now);
        self.pending_bytes = self
            .pending_bytes
            .checked_add(8 + size)
            .ok_or(Error::Invalid("segment size overflow"))?;
        self.pending.push(txn);
        if self.pending_segment_bytes()? >= self.config.segment_bytes
            || self.pending.len() as u64 >= self.config.segment_transactions
            || self.oldest.is_some_and(|start| {
                now.saturating_duration_since(start) >= self.config.segment_age
            })
        {
            self.seal().await?;
        }
        Ok(outcome)
    }
    pub async fn seal_if_due(&mut self, now: Instant) -> Result<bool> {
        if self.blocked {
            return Err(Error::Blocked);
        }
        if self
            .oldest
            .is_some_and(|start| now.saturating_duration_since(start) >= self.config.segment_age)
        {
            return self.seal().await;
        }
        Ok(false)
    }
    /// Retryable on backend errors: pending records and the exact manifest key
    /// stay unchanged. Unknown outcomes are resolved by BufferStore rereads.
    /// Cancellation retains successful segment publication and the owned
    /// manifest PUT, including its exact key and bytes. A later call awaits the
    /// same operation; only its durable result advances progress, exactly once.
    pub async fn seal(&mut self) -> Result<bool> {
        if self.blocked {
            return Err(Error::Blocked);
        }
        let Some(first) = self.pending.first() else {
            return Ok(false);
        };
        // Reserve the next sequence before any durable writes.
        let next = self
            .next_mseq
            .checked_add(1)
            .ok_or(Error::Invalid("manifest sequence exhausted"))?;
        let first_pos = first.metadata().commit_position.clone();
        let last_pos = self
            .pending
            .last()
            .expect("nonempty segment")
            .metadata()
            .commit_position
            .clone();
        // Keep the exact file across errors and cancellation, through manifest
        // durability. A retry never rebuilds or mutates the attempted segment.
        if self.segment.is_none() {
            let header = self.header().expect("nonempty segment");
            let mut encoder =
                SegmentEncoder::new(Scratch::new(&self.scratch_root)?, header, self.limits())?;
            for txn in &mut self.pending {
                if txn.chunk_rows {
                    encoder.write_chunk_rows(&mut txn.inner)?;
                } else {
                    encoder.write_streaming_transaction(&mut txn.inner)?;
                }
            }
            self.segment = Some(encoder.finish()?);
        }
        let (_, summary) = self.segment.as_ref().expect("encoded segment");
        let key = format::boundary_segment_key(
            format::segment_key(
                &self.config.prefix,
                &self.config.migration_id,
                &self.config.stream,
                &first_pos,
                &last_pos,
                self.config.epoch,
            ),
            &first_pos,
            self.last_position.as_ref(),
        );
        let manifest = ManifestEntry {
            mseq: self.next_mseq,
            epoch: self.config.epoch,
            prev_digest: self.prev_digest,
            segment_key: key.clone(),
            segment_digest: summary.digest,
            first_pos,
            last_pos: last_pos.clone(),
            txn_count: self.pending.len() as u64,
            bytes: summary.bytes,
        };
        let manifest_bytes = manifest.canonical_json()?;
        let manifest_digest = manifest.digest()?;
        self.publishing = true;
        #[cfg(feature = "test-crash")]
        self.hit(CrashPoint::BeforeSegmentPut)?;
        if !self.segment_published && self.upload.is_none() {
            let file = self
                .segment
                .as_ref()
                .expect("encoded segment")
                .0
                .file
                .as_ref()
                .expect("live segment")
                .try_clone()?;
            let store = self.store.clone();
            let key = key.clone();
            // Keep the upload handle across caller cancellation. Duplicate OS
            // handles share a seek cursor: never start a second file replay
            // while a detached backend task is still reading the first one.
            self.upload = Some(tokio::spawn(async move {
                store.put_file_create_only(&key, file).await
            }));
        }
        if !self.segment_published {
            let uploaded = self.upload.as_mut().expect("live upload").await;
            self.upload = None;
            uploaded.map_err(|_| backends::Error::Store)??;
            self.segment_published = true;
        }
        #[cfg(feature = "test-crash")]
        self.hit(CrashPoint::AfterSegmentPut)?;
        #[cfg(feature = "test-crash")]
        self.hit(CrashPoint::BeforeManifestPut)?;
        if self.manifest_upload.is_none() {
            let store = self.store.clone();
            let key = self.manifest_key();
            self.manifest_upload = Some(tokio::spawn(async move {
                store.put_create_only(&key, &manifest_bytes).await
            }));
        }
        let published = self
            .manifest_upload
            .as_mut()
            .expect("live manifest PUT")
            .await;
        self.manifest_upload = None;
        if let Err(error) = published.map_err(|_| backends::Error::Store)? {
            if error == backends::Error::Conflict {
                // Fail closed immediately, even if reading the winner fails.
                self.blocked = true;
                let winner = self.store.get(&self.manifest_key()).await?;
                if let Some(winner) = winner {
                    let winner = ManifestEntry::from_canonical_json(&winner)?;
                    if winner.epoch != self.config.epoch {
                        return Err(Error::SteppedDown {
                            writer_epoch: self.config.epoch,
                            manifest_epoch: winner.epoch,
                        });
                    }
                }
                return Err(Error::ManifestConflict);
            }
            return Err(error.into());
        }
        #[cfg(feature = "test-crash")]
        self.hit(CrashPoint::AfterManifestPut)?;
        // No await between committed local state and the progress publication.
        self.prev_digest = manifest_digest;
        self.last_epoch = self.config.epoch;
        self.next_mseq = next;
        self.last_position = Some(last_pos.clone());
        let last = self.pending.last().expect("nonempty segment");
        self.last_record = Some(format::RecordBoundary {
            source: last.metadata().source.clone(),
            chunk_rows: last.chunk_rows,
        });
        for txn in &self.pending {
            self.redelivery
                .get_mut(&(txn.metadata().source.clone(), txn.metadata().txn.clone()))
                .expect("admitted redelivery")
                .manifest = Some(DurableManifestRef {
                mseq: manifest.mseq,
                manifest_digest,
            });
        }
        self.pending.clear();
        self.segment = None;
        self.segment_published = false;
        self.pending_bytes = 0;
        self.oldest = None;
        self.publishing = false;
        let _ = self.progress.send(BufferedProgress {
            position: last_pos,
            mseq: manifest.mseq,
            epoch: self.config.epoch,
            manifest_digest,
        });
        Ok(true)
    }
    /// Available only in explicitly opted-in test builds; never reads env vars.
    #[cfg(feature = "test-crash")]
    pub fn inject_crash(&mut self, point: CrashPoint) {
        self.crash = Some(point);
    }
    #[cfg(feature = "test-crash")]
    fn hit(&mut self, point: CrashPoint) -> Result<()> {
        if self.crash == Some(point) {
            self.blocked = true;
            return Err(Error::InjectedCrash(point));
        }
        Ok(())
    }
}
