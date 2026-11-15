//! Durable, stream-bound authority for applied-buffer retention.
//!
//! The apply executor records a contiguous applied prefix. Merely reading or
//! verifying a segment never advances it. Checkpoints are immutable journal
//! objects; only their current references enter D3's bounded progress table.
//! Intent history stays in the journal and is searched in bounded pages on retry.
use std::{cmp::Ordering, sync::Arc};

use async_trait::async_trait;
use object_store::path::Path;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

use super::{Error, Result, Transition, epoch::Coordinator};
use crate::{
    buffer::{
        backends::{self, BufferStore},
        format::{CHECKPOINT_VERSION, FormatError, ManifestEntry, RecordBoundary},
        gc::{GcIntent, GcJournal, GcReason},
        reader::{self, ReaderCheckpoint, ReaderConfig},
        writer::{DurableManifestRef, RetainedRedelivery, WriterCheckpoint},
    },
    event::{ContentDigest, Position, PositionOrder, SourceIdentity, TxnId},
};

const CHECKPOINT_BYTES: usize = 4 * 1024 * 1024;
type WirePosition = (String, Vec<u8>);
type WireSource = [String; 5];

fn position(p: &Position) -> WirePosition {
    (p.order_domain().into(), p.bytes().into())
}
fn restore_position(p: WirePosition) -> Position {
    Position::new(p.0, p.1)
}
fn source(s: &SourceIdentity) -> WireSource {
    [
        s.engine.clone(),
        s.system_id.clone(),
        s.lineage.clone(),
        s.db_name.clone(),
        s.stream.clone(),
    ]
}
fn restore_source(s: WireSource) -> SourceIdentity {
    let [engine, system_id, lineage, db_name, stream] = s;
    SourceIdentity {
        engine,
        system_id,
        lineage,
        db_name,
        stream,
    }
}
fn rejected(message: &str) -> Error {
    Error::Rejected(format!("buffer GC: {message}"))
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Boundary {
    version: u16,
    next_mseq: u64,
    prev_digest: ContentDigest,
    last_epoch: u64,
    last_position: Option<WirePosition>,
    last_record: Option<(WireSource, bool)>,
}
impl From<&ReaderCheckpoint> for Boundary {
    fn from(c: &ReaderCheckpoint) -> Self {
        Self {
            version: c.version,
            next_mseq: c.next_mseq,
            prev_digest: c.prev_digest,
            last_epoch: c.last_epoch,
            last_position: c.last_position.as_ref().map(position),
            last_record: c
                .last_record
                .as_ref()
                .map(|r| (source(&r.source), r.chunk_rows)),
        }
    }
}
impl Boundary {
    fn reader(self) -> ReaderCheckpoint {
        ReaderCheckpoint {
            version: self.version,
            next_mseq: self.next_mseq,
            prev_digest: self.prev_digest,
            last_epoch: self.last_epoch,
            last_position: self.last_position.map(restore_position),
            last_record: self.last_record.map(|(s, chunk_rows)| RecordBoundary {
                source: restore_source(s),
                chunk_rows,
            }),
        }
    }
    fn validate(&self, epoch: u64, order: &dyn PositionOrder) -> Result<()> {
        if self.version != CHECKPOINT_VERSION || self.last_epoch > epoch {
            return Err(rejected("checkpoint version or epoch"));
        }
        if self.next_mseq == 0 {
            if self != &Self::from(&ReaderCheckpoint::default()) {
                return Err(rejected("nonempty genesis checkpoint"));
            }
        } else {
            let p = self
                .last_position
                .clone()
                .map(restore_position)
                .ok_or_else(|| rejected("checkpoint has no last position"))?;
            p.compare(&p, order)
                .map_err(|_| rejected("checkpoint position domain"))?;
            if self.last_record.is_none() || self.last_epoch == 0 {
                return Err(rejected("checkpoint has no record boundary"));
            }
        }
        Ok(())
    }
    fn follows(&self, previous: &Self, order: &dyn PositionOrder) -> Result<()> {
        if self.next_mseq < previous.next_mseq
            || self.last_epoch < previous.last_epoch
            || (self.next_mseq == previous.next_mseq && self != previous)
        {
            return Err(rejected("checkpoint regression or boundary conflict"));
        }
        if let (Some(p), Some(old)) = (&self.last_position, &previous.last_position)
            && restore_position(p.clone())
                .compare(&restore_position(old.clone()), order)
                .map_err(|_| rejected("checkpoint position domain"))?
                == Ordering::Less
        {
            return Err(rejected("checkpoint position regression"));
        }
        Ok(())
    }
    fn covers(
        &self,
        entry: &ManifestEntry,
        digest: ContentDigest,
        order: &dyn PositionOrder,
    ) -> Result<()> {
        if self.next_mseq <= entry.mseq || self.last_epoch < entry.epoch {
            return Err(rejected("durable checkpoint does not cover segment"));
        }
        let p = self
            .last_position
            .clone()
            .map(restore_position)
            .ok_or_else(|| rejected("checkpoint has no last position"))?;
        let comparison = p
            .compare(&entry.last_pos, order)
            .map_err(|_| rejected("checkpoint position domain"))?;
        if comparison == Ordering::Less
            || (self.next_mseq == entry.mseq + 1
                && (self.prev_digest != digest
                    || self.last_epoch != entry.epoch
                    || comparison != Ordering::Equal))
        {
            return Err(rejected("checkpoint does not match segment boundary"));
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Retained {
    source: WireSource,
    txn: Vec<u8>,
    position: WirePosition,
    digest: ContentDigest,
    chunk_rows: bool,
    manifest: Option<(u64, ContentDigest)>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Writer {
    boundary: Boundary,
    redelivery_from: Option<WirePosition>,
    redelivery: Vec<Retained>,
}
impl From<&WriterCheckpoint> for Writer {
    fn from(c: &WriterCheckpoint) -> Self {
        Self {
            boundary: Boundary::from(&ReaderCheckpoint {
                version: c.version,
                next_mseq: c.next_mseq,
                prev_digest: c.prev_digest,
                last_epoch: c.last_epoch,
                last_position: c.last_position.clone(),
                last_record: c.last_record.clone(),
            }),
            redelivery_from: c.redelivery_from.as_ref().map(position),
            redelivery: c
                .redelivery
                .iter()
                .map(|r| Retained {
                    source: source(&r.source),
                    txn: r.txn.0.clone(),
                    position: position(&r.position),
                    digest: r.digest,
                    chunk_rows: r.chunk_rows,
                    manifest: r.manifest.as_ref().map(|m| (m.mseq, m.manifest_digest)),
                })
                .collect(),
        }
    }
}
impl Writer {
    fn validate(&self, epoch: u64, order: &dyn PositionOrder) -> Result<()> {
        self.boundary.validate(epoch, order)?;
        if self.boundary.next_mseq == 0 {
            return if self.redelivery_from.is_none() && self.redelivery.is_empty() {
                Ok(())
            } else {
                Err(rejected("nonempty genesis writer history"))
            };
        }
        let last = restore_position(
            self.boundary
                .last_position
                .clone()
                .expect("validated boundary"),
        );
        let compare = |a: &Position, b: &Position| {
            a.compare(b, order)
                .map_err(|_| rejected("writer history position domain"))
        };
        let first = self.redelivery_from.clone().map(restore_position);
        if let Some(first) = &first
            && compare(first, &last)? == Ordering::Greater
        {
            return Err(rejected("writer coverage beyond durable progress"));
        }
        let boundary = self
            .boundary
            .last_record
            .as_ref()
            .expect("validated boundary");
        for retained in &self.redelivery {
            let p = restore_position(retained.position.clone());
            if let Some(first) = &first
                && compare(&p, first)? == Ordering::Less
            {
                return Err(rejected("writer history below retained coverage"));
            }
            if retained
                .manifest
                .as_ref()
                .is_some_and(|(mseq, _)| *mseq >= self.boundary.next_mseq)
                || (retained.chunk_rows && retained.manifest.is_none())
                || compare(&p, &last)? == Ordering::Greater
                || (p == last
                    && (retained.source != boundary.0 || (retained.chunk_rows && !boundary.1)))
            {
                return Err(rejected(
                    "writer redelivery history does not match checkpoint",
                ));
            }
        }
        Ok(())
    }
    fn checkpoint(self) -> WriterCheckpoint {
        let c = self.boundary.reader();
        WriterCheckpoint {
            version: c.version,
            next_mseq: c.next_mseq,
            prev_digest: c.prev_digest,
            last_epoch: c.last_epoch,
            last_position: c.last_position,
            last_record: c.last_record,
            redelivery_from: self.redelivery_from.map(restore_position),
            redelivery: self
                .redelivery
                .into_iter()
                .map(|r| RetainedRedelivery {
                    source: restore_source(r.source),
                    txn: TxnId(r.txn),
                    position: restore_position(r.position),
                    digest: r.digest,
                    chunk_rows: r.chunk_rows,
                    manifest: r
                        .manifest
                        .map(|(mseq, manifest_digest)| DurableManifestRef {
                            mseq,
                            manifest_digest,
                        }),
                })
                .collect(),
        }
    }
}

/// Construct for the current owner/epoch and exactly one buffer stream. A new
/// owner must construct a new authority, while durable facts survive failover.
pub struct CoordinatorGcJournal {
    coordinator: Arc<Mutex<Coordinator>>,
    store: Arc<dyn BufferStore>,
    config: ReaderConfig,
    order: Arc<dyn PositionOrder + Send + Sync>,
    epoch: u64,
}
impl CoordinatorGcJournal {
    pub async fn new(
        coordinator: Arc<Mutex<Coordinator>>,
        store: Arc<dyn BufferStore>,
        config: ReaderConfig,
        order: Arc<dyn PositionOrder + Send + Sync>,
    ) -> Result<Self> {
        let epoch = {
            let mut c = coordinator.lock().await;
            c.refresh().await?;
            if !c.is_owner() {
                return Err(Error::LostOwnership);
            }
            if config.stream.is_empty()
                || config.migration_id.is_empty()
                || c.gc_journal().namespace().rsplit('/').next()
                    != Some(config.migration_id.as_str())
            {
                return Err(rejected("migration/stream binding"));
            }
            c.state().epoch
        };
        Ok(Self {
            coordinator,
            store,
            config,
            order,
            epoch,
        })
    }
    fn key(&self, kind: &str) -> String {
        format!(
            "buffer-gc:{}",
            serde_json::to_string(&(
                &self.config.prefix,
                &self.config.migration_id,
                &self.config.stream,
                kind
            ))
            .expect("strings")
        )
    }
    fn object_key(c: &Coordinator, digest: &str) -> String {
        // Journal recovery/scrub owns every key below namespace(); checkpoint
        // payloads belong in a disjoint namespace, not its entry listing.
        format!(
            "gc-checkpoints/{:x}/{digest}.json",
            Sha256::digest(c.gc_journal().namespace().as_bytes())
        )
    }
    async fn fence(&self, c: &mut Coordinator) -> Result<()> {
        c.refresh().await?;
        if !c.is_owner() || c.state().epoch != self.epoch {
            return Err(Error::LostOwnership);
        }
        Ok(())
    }
    async fn load<T: DeserializeOwned>(&self, c: &Coordinator, kind: &str) -> Result<Option<T>> {
        let Some((digest, key)) = c.state().progress.get(&self.key(kind)) else {
            return Ok(None);
        };
        let expected_key = Self::object_key(c, digest);
        if key != &expected_key {
            return Err(rejected("checkpoint reference binding"));
        }
        let bytes = c
            .gc_journal()
            .read_bounded(&Path::from(key.as_str()), CHECKPOINT_BYTES)
            .await?
            .ok_or_else(|| rejected("durable checkpoint object missing"))?;
        if format!("{:x}", Sha256::digest(&bytes)) != *digest {
            return Err(rejected("checkpoint digest"));
        }
        Ok(Some(serde_json::from_slice(&bytes)?))
    }
    async fn persist(&self, c: &mut Coordinator, kind: &str, value: &impl Serialize) -> Result<()> {
        let bytes = serde_json::to_vec(value)?;
        if bytes.len() > CHECKPOINT_BYTES {
            return Err(rejected(
                "checkpoint exceeds 4 MiB; reduce retained redelivery history",
            ));
        }
        let digest = format!("{:x}", Sha256::digest(&bytes));
        let key = Self::object_key(c, &digest);
        c.gc_journal()
            .create(&Path::from(key.as_str()), bytes)
            .await?;
        if c.state().progress.get(&self.key(kind)) == Some(&(digest.clone(), key.clone())) {
            return self.fence(c).await;
        }
        c.commit_inner(Transition::Progress {
            stream: self.key(kind),
            marker: digest,
            position: key,
        })
        .await?;
        self.fence(c).await
    }
    /// Call only after the destination has durably applied the contiguous prefix.
    /// A position equal to a segment's last record deliberately does not suffice.
    pub async fn persist_applied_through(&self, applied: &Position) -> Result<()> {
        let mut c = self.coordinator.lock().await;
        self.fence(&mut c).await?;
        applied
            .compare(applied, self.order.as_ref())
            .map_err(|_| rejected("apply position domain"))?;
        if let Some(old) = self.load::<WirePosition>(&c, "applied").await?
            && applied
                .compare(&restore_position(old), self.order.as_ref())
                .map_err(|_| rejected("apply position domain"))?
                == Ordering::Less
        {
            return Err(rejected("applied prefix regression"));
        }
        self.persist(&mut c, "applied", &position(applied)).await
    }
    pub async fn persist_reader_checkpoint(&self, checkpoint: &ReaderCheckpoint) -> Result<()> {
        let mut c = self.coordinator.lock().await;
        self.fence(&mut c).await?;
        let next = Boundary::from(checkpoint);
        next.validate(self.epoch, self.order.as_ref())?;
        if let Some(old) = self.load::<Boundary>(&c, "reader").await? {
            next.follows(&old, self.order.as_ref())?;
        }
        self.persist(&mut c, "reader", &next).await
    }
    pub async fn persist_writer_checkpoint(&self, checkpoint: &WriterCheckpoint) -> Result<()> {
        let mut c = self.coordinator.lock().await;
        self.fence(&mut c).await?;
        let next = Writer::from(checkpoint);
        next.validate(self.epoch, self.order.as_ref())?;
        if let Some(old) = self.load::<Writer>(&c, "writer").await? {
            next.boundary.follows(&old.boundary, self.order.as_ref())?;
        }
        self.persist(&mut c, "writer", &next).await
    }
    pub async fn reader_checkpoint(&self) -> Result<Option<ReaderCheckpoint>> {
        let mut c = self.coordinator.lock().await;
        self.fence(&mut c).await?;
        Ok(self
            .load::<Boundary>(&c, "reader")
            .await?
            .map(Boundary::reader))
    }
    pub async fn writer_checkpoint(&self) -> Result<Option<WriterCheckpoint>> {
        let mut c = self.coordinator.lock().await;
        self.fence(&mut c).await?;
        Ok(self
            .load::<Writer>(&c, "writer")
            .await?
            .map(Writer::checkpoint))
    }
    async fn recorded(&self, c: &Coordinator, intent: &GcIntent) -> Result<bool> {
        let stream = self.key("intent");
        let mut after = 0;
        while after < c.state().applied_seq {
            let page = c
                .gc_journal()
                .history_page_through(after, 128, c.state().applied_seq)
                .await?;
            if page.is_empty() {
                return Err(rejected("incomplete GC intent history"));
            }
            for entry in page {
                after = entry.seq;
                if let Transition::Progress {
                    stream: s,
                    marker,
                    position,
                } = entry.transition
                    && s == stream
                    && marker == "gc-intent-v1"
                {
                    let prior: GcIntent = serde_json::from_str(&position)?;
                    if prior.key == intent.key {
                        if prior != *intent {
                            return Err(rejected("GC intent key reused with different content"));
                        }
                        return Ok(true);
                    }
                }
            }
        }
        Ok(false)
    }
    async fn authorize(&self, intent: &GcIntent) -> std::result::Result<(), AuthorizationError> {
        let mut c = self.coordinator.lock().await;
        self.fence(&mut c).await?;
        if self.recorded(&c, intent).await? {
            return self.fence(&mut c).await.map_err(Into::into);
        }
        let GcReason::AppliedAndVerified {
            mseq,
            manifest_digest,
            ..
        } = intent.reason
        else {
            return Err(rejected("orphan deletion requires a separate orphan authority").into());
        };
        let key = self.config.manifest_key(mseq);
        let bytes = self
            .store
            .get(&key)
            .await
            .map_err(AuthorizationError::Manifest)?
            .ok_or_else(|| rejected("retained manifest missing"))?;
        let entry = ManifestEntry::from_canonical_json(&bytes).map_err(|error| match error {
            FormatError::UnsupportedFormatVersion {
                format,
                found,
                expected,
            } => Error::UnsupportedFormatVersion {
                format,
                found,
                expected,
            },
            _ => rejected("invalid manifest"),
        })?;
        if entry.mseq != mseq
            || entry.epoch > self.epoch
            || entry.digest().map_err(|_| rejected("manifest digest"))? != manifest_digest
            || !((intent.key == key
                && intent.content_digest == manifest_digest
                && intent.bytes == bytes.len() as u64)
                || (intent.key == entry.segment_key
                    && intent.content_digest == entry.segment_digest
                    && intent.bytes == entry.bytes))
        {
            return Err(rejected("intent does not match retained manifest").into());
        }
        let applied = self
            .load::<WirePosition>(&c, "applied")
            .await?
            .ok_or_else(|| rejected("no durable applied prefix"))?;
        if restore_position(applied)
            .compare(&entry.last_pos, self.order.as_ref())
            .map_err(|_| rejected("apply position domain"))?
            != Ordering::Greater
        {
            return Err(
                rejected("applied prefix is not strictly beyond segment's last record").into(),
            );
        }
        let reader = self
            .load::<Boundary>(&c, "reader")
            .await?
            .ok_or_else(|| rejected("no durable reader checkpoint"))?;
        let writer = self
            .load::<Writer>(&c, "writer")
            .await?
            .ok_or_else(|| rejected("no durable writer checkpoint"))?;
        reader.validate(self.epoch, self.order.as_ref())?;
        writer.validate(self.epoch, self.order.as_ref())?;
        reader.covers(&entry, manifest_digest, self.order.as_ref())?;
        writer
            .boundary
            .covers(&entry, manifest_digest, self.order.as_ref())?;
        // commit_inner uses the exact checked head: a competing owner wins the
        // create-only next sequence or loses it; no refresh can skip this CAS.
        c.commit_inner(Transition::Progress {
            stream: self.key("intent"),
            marker: "gc-intent-v1".into(),
            position: serde_json::to_string(intent).map_err(Error::from)?,
        })
        .await?;
        self.fence(&mut c).await.map_err(Into::into)
    }
}
#[async_trait]
impl GcJournal for CoordinatorGcJournal {
    async fn record(&self, intent: &GcIntent) -> reader::Result<()> {
        self.authorize(intent)
            .await
            .map_err(AuthorizationError::into_reader)
    }
}

/// Manifest access retains its backend classification rather than becoming a
/// rejected proof. Coordinator diagnostics are redacted at the reader boundary.
enum AuthorizationError {
    Coordinator(Error),
    Manifest(backends::Error),
}
impl From<Error> for AuthorizationError {
    fn from(error: Error) -> Self {
        Self::Coordinator(error)
    }
}
impl AuthorizationError {
    fn into_reader(self) -> reader::Error {
        match self {
            Self::Manifest(error) => reader::Error::Store(error),
            Self::Coordinator(error) => coordinator_gc_error(error),
        }
    }
}
/// Classify coordinator failures for retention: storage/append uncertainty blocks
/// GC; invalid authority or durable facts require migration recovery.
pub fn coordinator_gc_error(error: Error) -> reader::Error {
    match error {
        Error::Store(_) => reader::Error::Store(backends::Error::Store),
        Error::Io(_) | Error::Core(giggles_core::CoreError::Storage(_)) => {
            reader::Error::Store(backends::Error::LocalIo)
        }
        // An ambiguous append never permits deletion: only confirmed durable
        // intent does. Exact retries search and reuse the durable intent history.
        Error::UnknownOutcome(_) => reader::Error::Store(backends::Error::UnknownPutOutcome),
        // Admission and bounded-read failures block GC without invalidating authority.
        Error::BudgetExceeded(_) | Error::TruncatedRead(_) => {
            reader::Error::Store(backends::Error::Store)
        }
        // Other local-core failures include corrupt/fenced state. Postgres is
        // not used by this authority; fail closed for unexpected variants.
        Error::RecoveryRequired(_)
        | Error::UnsupportedFormatVersion { .. }
        | Error::Rejected(_)
        | Error::Lifecycle(_)
        | Error::ReverseProof(_)
        | Error::LostOwnership
        | Error::Observer
        | Error::Core(_)
        | Error::Json(_)
        | Error::Postgres(_) => reader::Error::RecoveryRequired(
            "coordinator refused GC authority; inspect durable apply/checkpoint facts and ownership",
        ),
    }
}

#[cfg(test)]
mod classification_tests {
    use super::*;
    use crate::coordinator::{
        State, TransitionRejection, TransitionRejectionCode,
        budget::{Blocker, Budget},
    };

    #[tokio::test]
    async fn every_coordinator_error_class_has_a_gc_disposition() {
        let operational = [
            Error::Store(object_store::Error::Generic {
                store: "injected",
                source: Box::new(std::io::Error::other("injected")),
            }),
            Error::Io(std::io::Error::other("injected")),
            Error::Core(giggles_core::CoreError::Storage("injected".into())),
            Error::UnknownOutcome("injected".into()),
            Error::TruncatedRead("injected".into()),
            Error::BudgetExceeded(Blocker {
                budget: Budget::Progress,
                used: 2,
                limit: 1,
                remediation: "injected".into(),
            }),
        ];
        for error in operational {
            assert!(matches!(
                coordinator_gc_error(error),
                reader::Error::Store(_)
            ));
        }
        let pg = tokio_postgres::connect("port=invalid", tokio_postgres::NoTls)
            .await
            .err()
            .expect("invalid configuration");
        let fatal = [
            Error::RecoveryRequired("injected".into()),
            Error::Rejected("injected".into()),
            Error::LostOwnership,
            Error::Observer,
            Error::Core(giggles_core::CoreError::Corrupt("injected".into())),
            Error::Json(serde_json::from_str::<u64>("invalid").unwrap_err()),
            Error::Postgres(pg),
            Error::Lifecycle(TransitionRejection {
                from: State::Defined,
                to: None,
                code: TransitionRejectionCode::NotAllowed,
            }),
        ];
        for error in fatal {
            assert!(matches!(
                coordinator_gc_error(error),
                reader::Error::RecoveryRequired(_)
            ));
        }
    }
}
