//! Manifest-only replay, bounded orphan discovery, and capture capacity signals.
//!
//! Checkpoints are trusted coordinator state: persist them only after applying
//! every transaction through that segment. A missing key is an idle tail unless
//! `required_next_mseq` says it must exist; a known durable gap is recovery-required.
use super::{
    backends::BufferStore,
    format::{self, Limits, ManifestEntry, SegmentDecoder, SegmentRecord},
};
use crate::event::{AssembledTxn, ContentDigest, Position, PositionOrder};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    io::Cursor,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::watch;

#[derive(Debug)]
pub enum Error {
    Store(super::backends::Error),
    RecoveryRequired(&'static str),
    Invalid(&'static str),
    Format(format::FormatError),
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Store(e) => e.fmt(f),
            Self::Format(e) => e.fmt(f),
            Self::RecoveryRequired(s) => write!(f, "buffer recovery-required: {s}"),
            Self::Invalid(s) => write!(f, "invalid buffer reader configuration: {s}"),
        }
    }
}
impl std::error::Error for Error {}
fn format_recovery(error: format::FormatError, reason: &'static str) -> Error {
    match error {
        format::FormatError::UnsupportedFormatVersion { .. } => Error::Format(error),
        _ => Error::RecoveryRequired(reason),
    }
}
impl From<super::backends::Error> for Error {
    fn from(e: super::backends::Error) -> Self {
        Self::Store(e)
    }
}
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug)]
pub struct ReaderConfig {
    pub prefix: String,
    pub migration_id: String,
    pub stream: String,
    pub limits: Limits,
    /// Highest next-mseq known durable in coordinator state (exclusive).
    pub required_next_mseq: u64,
}
impl ReaderConfig {
    pub fn manifest_key(&self, mseq: u64) -> String {
        format::manifest_key(&self.prefix, &self.migration_id, &self.stream, mseq)
    }
    pub fn segment_prefix(&self) -> String {
        let key = self.manifest_key(0);
        format!(
            "{}/seg/",
            key.rsplit_once("/manifest/")
                .expect("format-owned layout")
                .0
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReaderCheckpoint {
    /// Version 3 preserves the kind/source boundary needed for an equal-H pair.
    pub version: u16,
    pub next_mseq: u64,
    pub prev_digest: ContentDigest,
    pub last_epoch: u64,
    pub last_position: Option<Position>,
    /// The last fully applied record, updated only at a segment boundary.
    pub last_record: Option<format::RecordBoundary>,
}
impl Default for ReaderCheckpoint {
    fn default() -> Self {
        Self {
            version: format::CHECKPOINT_VERSION,
            next_mseq: 0,
            prev_digest: [0; 32],
            last_epoch: 0,
            last_position: None,
            last_record: None,
        }
    }
}

/// Cannot be constructed without validating the complete referenced segment.
#[derive(Clone, Debug)]
pub struct VerifiedManifest {
    pub(crate) config: ReaderConfig,
    pub(crate) entry: ManifestEntry,
    pub(crate) digest: ContentDigest,
}
impl VerifiedManifest {
    pub fn entry(&self) -> &ManifestEntry {
        &self.entry
    }
    pub fn digest(&self) -> ContentDigest {
        self.digest
    }
    pub fn manifest_key(&self) -> String {
        self.config.manifest_key(self.entry.mseq)
    }
}

type Decoder = SegmentDecoder<Cursor<Vec<u8>>>;
struct Active {
    decoder: Decoder,
    remaining: u64,
    checkpoint: ReaderCheckpoint,
}
pub struct BufferReader {
    store: Arc<dyn BufferStore>,
    config: ReaderConfig,
    order: Arc<dyn PositionOrder + Send + Sync>,
    checkpoint: ReaderCheckpoint,
    active: Option<Active>,
    verified: Option<VerifiedManifest>,
    broken: bool,
}
impl BufferReader {
    pub fn new(
        store: Arc<dyn BufferStore>,
        config: ReaderConfig,
        checkpoint: ReaderCheckpoint,
        order: Arc<dyn PositionOrder + Send + Sync>,
    ) -> Result<Self> {
        if checkpoint.version != format::CHECKPOINT_VERSION {
            return Err(Error::Format(
                format::FormatError::UnsupportedFormatVersion {
                    format: "reader-checkpoint",
                    found: Some(checkpoint.version),
                    expected: format::CHECKPOINT_VERSION,
                },
            ));
        }
        if config.migration_id.is_empty()
            || config.stream.is_empty()
            || (checkpoint.next_mseq == 0 && checkpoint != ReaderCheckpoint::default())
            || (checkpoint.next_mseq != 0
                && (checkpoint.last_position.is_none() || checkpoint.last_record.is_none()))
        {
            return Err(Error::Invalid("stream identity or genesis checkpoint"));
        }
        Ok(Self {
            store,
            config,
            order,
            checkpoint,
            active: None,
            verified: None,
            broken: false,
        })
    }
    /// Segment boundary only; while a segment is partly consumed this stays at
    /// its beginning, making restart replay safe. Caller still owns apply progress.
    pub fn checkpoint(&self) -> &ReaderCheckpoint {
        &self.checkpoint
    }
    pub fn last_verified(&self) -> Option<&VerifiedManifest> {
        self.verified.as_ref()
    }
    /// Feed durable writer progress here. Without a known durable high-water
    /// mark, an absent exact key is indistinguishable from an idle live tail.
    pub fn require_through(&mut self, next_mseq: u64) {
        self.config.required_next_mseq = self.config.required_next_mseq.max(next_mseq);
    }
    pub async fn next_txn(&mut self) -> Result<Option<AssembledTxn>> {
        if self.broken {
            return Err(Error::RecoveryRequired("reader is blocked"));
        }
        let result = self.next_inner().await;
        if matches!(result, Err(Error::RecoveryRequired(_))) {
            self.broken = true;
        }
        result
    }
    async fn next_inner(&mut self) -> Result<Option<AssembledTxn>> {
        if self.active.is_none() {
            let key = self.config.manifest_key(self.checkpoint.next_mseq);
            let Some(bytes) = self.store.get(&key).await? else {
                if self.checkpoint.next_mseq < self.config.required_next_mseq {
                    return Err(Error::RecoveryRequired("missing durable manifest"));
                }
                return Ok(None);
            };
            let entry = parse_manifest(&self.config, &self.checkpoint, &bytes)?;
            let (decoder, (last_position, last_record)) = verify_segment(
                self.store.as_ref(),
                &self.config,
                &entry,
                self.checkpoint
                    .last_position
                    .as_ref()
                    .zip(self.checkpoint.last_record.as_ref()),
                self.order.as_ref(),
            )
            .await?;
            let digest = entry
                .digest()
                .map_err(|_| Error::RecoveryRequired("manifest digest"))?;
            let checkpoint = ReaderCheckpoint {
                version: format::CHECKPOINT_VERSION,
                next_mseq: entry
                    .mseq
                    .checked_add(1)
                    .ok_or(Error::RecoveryRequired("mseq exhausted"))?,
                prev_digest: digest,
                last_epoch: entry.epoch,
                last_position: Some(last_position),
                last_record: Some(last_record),
            };
            self.active = Some(Active {
                decoder,
                remaining: entry.txn_count,
                checkpoint,
            });
            self.verified = Some(VerifiedManifest {
                config: self.config.clone(),
                entry,
                digest,
            });
        }
        let active = self.active.as_mut().expect("loaded segment");
        let record = active
            .decoder
            .next_record()
            .map_err(|error| format_recovery(error, "segment changed after validation"))?
            .ok_or(Error::RecoveryRequired("empty segment"))?;
        active.remaining -= 1;
        if active.remaining == 0 {
            self.checkpoint = active.checkpoint.clone();
            self.active = None;
        }
        Ok(Some(match record {
            SegmentRecord::Transaction(t) | SegmentRecord::ChunkRows(t) => t,
        }))
    }
}

pub(crate) fn parse_manifest(
    config: &ReaderConfig,
    checkpoint: &ReaderCheckpoint,
    bytes: &[u8],
) -> Result<ManifestEntry> {
    let entry = ManifestEntry::from_canonical_json(bytes)
        .map_err(|error| format_recovery(error, "invalid manifest"))?;
    if entry.mseq != checkpoint.next_mseq
        || entry.prev_digest != checkpoint.prev_digest
        || entry.epoch < checkpoint.last_epoch
        || entry.txn_count == 0
        || entry.segment_key
            != format::boundary_segment_key(
                format::segment_key(
                    &config.prefix,
                    &config.migration_id,
                    &config.stream,
                    &entry.first_pos,
                    &entry.last_pos,
                    entry.epoch,
                ),
                &entry.first_pos,
                checkpoint.last_position.as_ref(),
            )
    {
        return Err(Error::RecoveryRequired(
            "manifest chain or segment identity",
        ));
    }
    Ok(entry)
}

/// Two decoding passes over the same immutable bytes: first verify everything,
/// then yield one transaction at a time. No collection of decoded txns is kept.
/// BufferStore::get currently owns one full segment; a streaming GET would remove
/// that remaining segment-sized allocation without changing the replay contract.
pub(crate) async fn verify_segment(
    store: &dyn BufferStore,
    config: &ReaderConfig,
    entry: &ManifestEntry,
    previous: Option<(&Position, &format::RecordBoundary)>,
    order: &(dyn PositionOrder + Send + Sync),
) -> Result<(Decoder, (Position, format::RecordBoundary))> {
    let bytes = store
        .get(&entry.segment_key)
        .await?
        .ok_or(Error::RecoveryRequired("missing segment"))?;
    let mut decoder = SegmentDecoder::new(Cursor::new(bytes), config.limits)
        .map_err(|error| format_recovery(error, "segment header"))?;
    let h = decoder.header();
    if h.migration_id != config.migration_id
        || h.stream != config.stream
        || h.epoch != entry.epoch
        || h.first_pos != entry.first_pos
        || h.last_pos != entry.last_pos
        || h.record_count != entry.txn_count
    {
        return Err(Error::RecoveryRequired("manifest/header mismatch"));
    }
    let mut last = previous.map(|(position, boundary)| (position.clone(), boundary.clone()));
    while let Some(record) = decoder
        .next_record()
        .map_err(|error| format_recovery(error, "segment record or trailer"))?
    {
        let pos = record.position();
        let (source, chunk_rows) = match &record {
            SegmentRecord::Transaction(txn) => (&txn.source, false),
            SegmentRecord::ChunkRows(txn) => (&txn.source, true),
        };
        let valid = format::record_follows(
            pos,
            source,
            chunk_rows,
            last.as_ref().map(|(pos, boundary)| (pos, boundary)),
            order,
        )
        .map_err(|_| Error::RecoveryRequired("position order domain"))?;
        if !valid {
            return Err(Error::RecoveryRequired("positions not strictly increasing"));
        }
        last = Some((
            pos.clone(),
            format::RecordBoundary {
                source: source.clone(),
                chunk_rows,
            },
        ));
    }
    let (mut cursor, summary) = decoder
        .finish()
        .map_err(|error| format_recovery(error, "segment trailer"))?;
    if summary.digest != entry.segment_digest
        || summary.bytes != entry.bytes
        || entry.txn_count == 0
    {
        return Err(Error::RecoveryRequired("manifest/segment digest or size"));
    }
    cursor.set_position(0);
    let (last_position, last_record) = last.ok_or(Error::RecoveryRequired("empty segment"))?;
    Ok((
        SegmentDecoder::new(cursor, config.limits)
            .map_err(|error| format_recovery(error, "segment header"))?,
        (last_position, last_record),
    ))
}

/// The coordinator must durably fence writers in epochs below this value and
/// keep them fenced through journal acknowledgment and deletion. Grace alone
/// is insufficient: a slow live writer can still be publishing its manifest.
#[derive(Clone, Copy, Debug)]
pub struct OrphanFence {
    pub before_epoch: u64,
}
/// Complete retained manifest range, including every reference to any segment
/// in this stream. Checkpoint compaction must delete each segment before its
/// manifest, or retain its reference in the checkpoint used for this scan.
#[derive(Clone, Debug)]
pub struct ManifestRange {
    pub start: ReaderCheckpoint,
    pub end_mseq: u64,
    pub end_digest: ContentDigest,
}
#[derive(Clone, Debug)]
pub struct OrphanSegment {
    pub(crate) key: String,
    pub(crate) bytes: u64,
    pub(crate) digest: ContentDigest,
    pub(crate) epoch: u64,
    pub(crate) observed_at_ms: u64,
    pub(crate) config: ReaderConfig,
    pub(crate) range: ManifestRange,
    pub(crate) fence: OrphanFence,
}
impl OrphanSegment {
    pub fn key(&self) -> &str {
        &self.key
    }
    pub fn bytes(&self) -> u64 {
        self.bytes
    }
}
#[derive(Debug)]
pub struct OrphanPage {
    pub orphans: Vec<OrphanSegment>,
    /// Unreadable/corrupt/foreign segments are reported but never GC candidates.
    pub unverified: Vec<String>,
    pub next: Option<String>,
}

pub(crate) async fn referenced_keys(
    store: &dyn BufferStore,
    config: &ReaderConfig,
    range: &ManifestRange,
    candidates: &BTreeSet<String>,
) -> Result<BTreeSet<String>> {
    if range.start.next_mseq > range.end_mseq {
        return Err(Error::Invalid("manifest range"));
    }
    let mut checkpoint = range.start.clone();
    let mut referenced = BTreeSet::new();
    while checkpoint.next_mseq < range.end_mseq {
        let bytes = store
            .get(&config.manifest_key(checkpoint.next_mseq))
            .await?
            .ok_or(Error::RecoveryRequired("orphan scan manifest gap"))?;
        let entry = parse_manifest(config, &checkpoint, &bytes)?;
        if candidates.contains(&entry.segment_key) {
            referenced.insert(entry.segment_key.clone());
        }
        checkpoint.next_mseq += 1;
        checkpoint.prev_digest = entry
            .digest()
            .map_err(|_| Error::RecoveryRequired("manifest digest"))?;
        checkpoint.last_epoch = entry.epoch;
        checkpoint.last_position = Some(entry.last_pos);
    }
    if checkpoint.prev_digest != range.end_digest {
        return Err(Error::RecoveryRequired("orphan scan end anchor"));
    }
    Ok(referenced)
}

/// At most one page of candidates is retained; manifest references are checked
/// by exact GET across the checkpoint-bounded retained range, never a full LIST.
pub async fn list_orphan_segments(
    store: &dyn BufferStore,
    config: &ReaderConfig,
    range: &ManifestRange,
    fence: OrphanFence,
    token: Option<&str>,
    page_size: usize,
    now_ms: u64,
) -> Result<OrphanPage> {
    let page = store
        .list_prefix(&config.segment_prefix(), token, page_size)
        .await?;
    let candidates = page.objects.iter().map(|o| o.key.clone()).collect();
    let referenced = referenced_keys(store, config, range, &candidates).await?;
    let mut result = OrphanPage {
        orphans: vec![],
        unverified: vec![],
        next: page.next,
    };
    for object in page.objects {
        if referenced.contains(&object.key) {
            continue;
        }
        let Some(bytes) = store.get(&object.key).await? else {
            continue;
        };
        let checked = (|| {
            let decoder = SegmentDecoder::new(Cursor::new(bytes), config.limits).ok()?;
            let h = decoder.header().clone();
            if h.migration_id != config.migration_id
                || h.stream != config.stream
                || object.key
                    != format::segment_key(
                        &config.prefix,
                        &h.migration_id,
                        &h.stream,
                        &h.first_pos,
                        &h.last_pos,
                        h.epoch,
                    )
            {
                return None;
            }
            let (_, summary) = decoder.finish().ok()?;
            Some((h.epoch, summary))
        })();
        match checked {
            Some((epoch, summary)) if epoch < fence.before_epoch => {
                result.orphans.push(OrphanSegment {
                    key: object.key,
                    bytes: summary.bytes,
                    digest: summary.digest,
                    epoch,
                    observed_at_ms: now_ms,
                    config: config.clone(),
                    range: range.clone(),
                    fence,
                })
            }
            Some(_) => {} // Current/unfenced epochs cannot safely be called orphans.
            None => result.unverified.push(object.key),
        }
    }
    Ok(result)
}

#[derive(Clone, Copy, Debug)]
pub struct BufferBounds {
    pub max_buffered_bytes: u64,
    pub max_unapplied_txns: u64,
    pub max_oldest_unapplied_age: Duration,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Headroom {
    pub bytes: u64,
    pub transactions: u64,
    pub oldest_unapplied_age: Duration,
    pub age: Duration,
    pub paused: bool,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Admission(u64);
struct Usage {
    bytes: u64,
    txns: u64,
    captured: Instant,
    applied: bool,
}
struct Capacity {
    next: u64,
    entries: BTreeMap<u64, Usage>,
    blocked_request: Option<(u64, u64)>,
}
/// Shared with source and writer through subscribe(). Admission must precede
/// accepting ownership of input; on Err the caller keeps/retries the transaction.
/// Tick refresh() on the capture timer so idle age exhaustion publishes a pause.
/// Applied bytes remain charged until the corresponding journaled GC succeeds.
pub struct Backpressure {
    bounds: BufferBounds,
    capacity: Mutex<Capacity>,
    signal: watch::Sender<Headroom>,
}
impl Backpressure {
    pub fn new(bounds: BufferBounds) -> Result<Self> {
        if bounds.max_buffered_bytes == 0
            || bounds.max_unapplied_txns == 0
            || bounds.max_oldest_unapplied_age.is_zero()
        {
            return Err(Error::Invalid("zero buffer bound"));
        }
        let (signal, _) = watch::channel(Headroom {
            bytes: bounds.max_buffered_bytes,
            transactions: bounds.max_unapplied_txns,
            oldest_unapplied_age: Duration::ZERO,
            age: bounds.max_oldest_unapplied_age,
            paused: false,
        });
        Ok(Self {
            bounds,
            capacity: Mutex::new(Capacity {
                next: 0,
                entries: BTreeMap::new(),
                blocked_request: None,
            }),
            signal,
        })
    }
    pub fn subscribe(&self) -> watch::Receiver<Headroom> {
        self.signal.subscribe()
    }
    fn headroom(&self, state: &Capacity, now: Instant) -> Headroom {
        let bytes = state.entries.values().map(|u| u.bytes).sum::<u64>();
        let txns = state
            .entries
            .values()
            .filter(|u| !u.applied)
            .map(|u| u.txns)
            .sum::<u64>();
        let oldest = state
            .entries
            .values()
            .filter(|u| !u.applied)
            .map(|u| now.saturating_duration_since(u.captured))
            .max()
            .unwrap_or_default();
        let bytes = self.bounds.max_buffered_bytes.saturating_sub(bytes);
        let transactions = self.bounds.max_unapplied_txns.saturating_sub(txns);
        let age = self.bounds.max_oldest_unapplied_age.saturating_sub(oldest);
        Headroom {
            bytes,
            transactions,
            oldest_unapplied_age: oldest,
            age,
            paused: bytes == 0
                || transactions == 0
                || age.is_zero()
                || state
                    .blocked_request
                    .is_some_and(|(b, t)| b > bytes || t > transactions),
        }
    }
    pub fn refresh(&self, now: Instant) -> Headroom {
        let state = self.capacity.lock().unwrap_or_else(|e| e.into_inner());
        let headroom = self.headroom(&state, now);
        self.signal.send_replace(headroom.clone());
        headroom
    }
    pub fn try_admit(
        &self,
        bytes: u64,
        txns: u64,
        now: Instant,
    ) -> std::result::Result<Admission, Headroom> {
        let mut state = self.capacity.lock().unwrap_or_else(|e| e.into_inner());
        let h = self.headroom(&state, now);
        if h.paused
            || bytes == 0
            || txns == 0
            || bytes > h.bytes
            || txns > h.transactions
            || state.next == u64::MAX
        {
            if bytes > 0 && txns > 0 {
                state.blocked_request = Some((bytes, txns));
            }
            let h = self.headroom(&state, now);
            self.signal.send_replace(h.clone());
            return Err(h);
        }
        state.blocked_request = None;
        let id = state.next;
        state.next += 1;
        state.entries.insert(
            id,
            Usage {
                bytes,
                txns,
                captured: now,
                applied: false,
            },
        );
        self.signal.send_replace(self.headroom(&state, now));
        Ok(Admission(id))
    }
    pub fn mark_applied(&self, admission: Admission, now: Instant) -> Result<()> {
        let mut state = self.capacity.lock().unwrap_or_else(|e| e.into_inner());
        state
            .entries
            .get_mut(&admission.0)
            .ok_or(Error::Invalid("unknown admission"))?
            .applied = true;
        self.signal.send_replace(self.headroom(&state, now));
        Ok(())
    }
    pub fn mark_deleted(&self, admission: Admission, now: Instant) -> Result<()> {
        let mut state = self.capacity.lock().unwrap_or_else(|e| e.into_inner());
        if !state.entries.get(&admission.0).is_some_and(|u| u.applied) {
            return Err(Error::Invalid("delete before apply"));
        }
        state.entries.remove(&admission.0);
        self.signal.send_replace(self.headroom(&state, now));
        Ok(())
    }
}
