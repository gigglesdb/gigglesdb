//! Retention deletes are effects: the coordinator must journal each typed intent
//! durably, under its ownership fence, before returning success from GcJournal.
//! No delete API accepts arbitrary object keys or caller-asserted verification.
use super::{
    backends::BufferStore,
    format::{ManifestEntry, SegmentDecoder},
    reader::{self, Error, OrphanSegment, VerifiedManifest},
};
use crate::event::{ContentDigest, PositionOrder};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, io::Cursor, sync::Arc};

#[derive(Clone, Copy, Debug)]
pub struct RetentionPolicy {
    pub retained_for_ms: u64,
    pub orphan_grace_ms: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum GcReason {
    AppliedAndVerified {
        mseq: u64,
        manifest_digest: ContentDigest,
        applied_at_ms: u64,
        verified_at_ms: u64,
    },
    Orphan {
        epoch: u64,
        fenced_before_epoch: u64,
        observed_at_ms: u64,
    },
}
/// One object per intent. A segment intent always precedes its manifest intent,
/// preserving the orphan detector's complete-retained-reference invariant.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GcIntent {
    pub key: String,
    pub content_digest: ContentDigest,
    pub bytes: u64,
    pub reason: GcReason,
}
#[async_trait]
pub trait GcJournal: Send + Sync {
    /// Idempotent durable append. Returning an error forbids the deletion.
    /// Applied receipts/checkpoint retention and epoch ownership are coordinator
    /// facts; validate them here before acknowledging an external effect.
    /// Before retiring a manifest, durably advance every required reader/writer
    /// restart checkpoint past it. A genesis-only writer must not be restarted
    /// after retention; checkpoint-aware writer recovery is an integration seam.
    async fn record(&self, intent: &GcIntent) -> reader::Result<()>;
}
/// Apply facts belong to the coordinator; verification proof belongs to reader.
#[derive(Clone, Debug)]
pub struct AppliedSegment {
    verified: VerifiedManifest,
    applied_at_ms: u64,
    verified_at_ms: u64,
}
impl AppliedSegment {
    /// Call only after applying the complete segment and durably recording that
    /// fact. Timestamps are wall-clock policy times, never source positions.
    pub fn new(verified: VerifiedManifest, applied_at_ms: u64, verified_at_ms: u64) -> Self {
        Self {
            verified,
            applied_at_ms,
            verified_at_ms,
        }
    }
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct GcOutcome {
    pub deleted_objects: u64,
    pub deleted_bytes: u64,
}
pub struct RetentionGc {
    store: Arc<dyn BufferStore>,
    order: Arc<dyn PositionOrder + Send + Sync>,
    policy: RetentionPolicy,
}
impl RetentionGc {
    pub fn new(
        store: Arc<dyn BufferStore>,
        order: Arc<dyn PositionOrder + Send + Sync>,
        policy: RetentionPolicy,
    ) -> Self {
        Self {
            store,
            order,
            policy,
        }
    }
    pub async fn collect_applied(
        &self,
        candidate: &AppliedSegment,
        now_ms: u64,
        journal: &dyn GcJournal,
    ) -> reader::Result<GcOutcome> {
        // Conservatively start retention at the later of apply and verification.
        let since = candidate.applied_at_ms.max(candidate.verified_at_ms);
        if now_ms < since || now_ms - since < self.policy.retained_for_ms {
            return Ok(GcOutcome::default());
        }
        let verified = &candidate.verified;
        let entry = &verified.entry;
        let key = verified.manifest_key();
        let current = self.store.get(&key).await?;
        if let Some(bytes) = &current {
            let actual = ManifestEntry::from_canonical_json(bytes)
                .map_err(|_| Error::RecoveryRequired("GC manifest corruption"))?;
            if actual
                .digest()
                .map_err(|_| Error::RecoveryRequired("GC manifest digest"))?
                != verified.digest
            {
                return Err(Error::RecoveryRequired("GC manifest changed"));
            }
        }
        let segment_present = self.store.get(&entry.segment_key).await?.is_some();
        if segment_present {
            if current.is_none() {
                return Err(Error::RecoveryRequired("segment without retained manifest"));
            }
            // Re-read/verify before any intent; a stale proof cannot erase corruption.
            reader::verify_segment(
                self.store.as_ref(),
                &verified.config,
                entry,
                None,
                self.order.as_ref(),
            )
            .await?;
        }
        let reason = GcReason::AppliedAndVerified {
            mseq: entry.mseq,
            manifest_digest: verified.digest,
            applied_at_ms: candidate.applied_at_ms,
            verified_at_ms: candidate.verified_at_ms,
        };
        let mut outcome = GcOutcome::default();
        if segment_present {
            let intent = GcIntent {
                key: entry.segment_key.clone(),
                content_digest: entry.segment_digest,
                bytes: entry.bytes,
                reason: reason.clone(),
            };
            journal.record(&intent).await?;
            self.store.delete(&intent.key).await?;
            outcome.deleted_objects += 1;
            outcome.deleted_bytes += intent.bytes;
        }
        if let Some(bytes) = current {
            let intent = GcIntent {
                key,
                content_digest: verified.digest,
                bytes: bytes.len() as u64,
                reason,
            };
            journal.record(&intent).await?;
            self.store.delete(&intent.key).await?;
            outcome.deleted_objects += 1;
            outcome.deleted_bytes += intent.bytes;
        }
        Ok(outcome)
    }
    pub async fn collect_orphan(
        &self,
        candidate: &OrphanSegment,
        now_ms: u64,
        journal: &dyn GcJournal,
    ) -> reader::Result<GcOutcome> {
        if now_ms < candidate.observed_at_ms
            || now_ms - candidate.observed_at_ms < self.policy.orphan_grace_ms
        {
            return Ok(GcOutcome::default());
        }
        let keys = BTreeSet::from([candidate.key.clone()]);
        if !reader::referenced_keys(
            self.store.as_ref(),
            &candidate.config,
            &candidate.range,
            &keys,
        )
        .await?
        .is_empty()
        {
            return Err(Error::RecoveryRequired("orphan now referenced"));
        }
        let Some(bytes) = self.store.get(&candidate.key).await? else {
            return Ok(GcOutcome::default());
        };
        let decoder = SegmentDecoder::new(Cursor::new(bytes), candidate.config.limits)
            .map_err(|_| Error::RecoveryRequired("orphan header"))?;
        if decoder.header().epoch != candidate.epoch
            || candidate.epoch >= candidate.fence.before_epoch
        {
            return Err(Error::RecoveryRequired("orphan fence"));
        }
        let (_, summary) = decoder
            .finish()
            .map_err(|_| Error::RecoveryRequired("orphan corruption"))?;
        if summary.digest != candidate.digest || summary.bytes != candidate.bytes {
            return Err(Error::RecoveryRequired("orphan changed"));
        }
        let intent = GcIntent {
            key: candidate.key.clone(),
            content_digest: candidate.digest,
            bytes: candidate.bytes,
            reason: GcReason::Orphan {
                epoch: candidate.epoch,
                fenced_before_epoch: candidate.fence.before_epoch,
                observed_at_ms: candidate.observed_at_ms,
            },
        };
        journal.record(&intent).await?;
        self.store.delete(&intent.key).await?;
        Ok(GcOutcome {
            deleted_objects: 1,
            deleted_bytes: intent.bytes,
        })
    }
}
