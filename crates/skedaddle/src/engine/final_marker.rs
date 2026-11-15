//! Exact final-marker receipts in the existing authenticated progress journal.
//! The marker's message LSN is distinct from its transaction's COMMIT LSN.
use super::{Captured, Runtime, recovery};
use crate::{
    buffer::{
        reader::ReaderCheckpoint,
        writer::{DurableManifestRef, WriterCheckpoint},
    },
    coordinator::{Materialized, Transition},
    cutover::{self, Key, Marker},
    event::{AssembledTxn, ContentDigest, EventKind, SourceIdentity},
    pg::{chunk::Watermark, pgoutput::Message},
};
use serde::{Deserialize, Serialize};
use std::{
    fmt,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::Notify;

const BUFFERED: &str = "engine-final-marker-buffered-v1";
const APPLIED: &str = "engine-final-marker-applied-v1";
const MAX_PAYLOAD: usize = 65536;
const MAX_RECEIPT: usize = 131072;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MarkerProgressError {
    UnknownPayload,
    InvalidEvidence,
    IdentityMismatch,
    EpochChanged,
    Rebound,
    CaptureGap,
    RunExited,
    Timeout,
}
impl fmt::Display for MarkerProgressError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "final marker: {self:?}")
    }
}
impl std::error::Error for MarkerProgressError {}
type Result<T> = std::result::Result<T, MarkerProgressError>;

/// DDL and other event kinds stay on their existing control paths. Only the
/// skedaddle watermark envelope is decoded as a chunk or final-marker payload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CapturedControl {
    Chunk(Watermark),
    Final(Marker),
    Other(EventKind),
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FinalPayload {
    r#final: String,
    epoch: i64,
}

pub fn decode_marker_payload(content: &[u8], message_lsn: u64) -> Result<CapturedControl> {
    if content.len() > MAX_PAYLOAD {
        return Err(MarkerProgressError::UnknownPayload);
    }
    if let Ok(payload) = serde_json::from_slice::<FinalPayload>(content) {
        if payload.r#final.is_empty()
            || payload.r#final.len() > 4096
            || payload.epoch <= 0
            || message_lsn == 0
        {
            return Err(MarkerProgressError::UnknownPayload);
        }
        return Ok(CapturedControl::Final(Marker {
            cutover: payload.r#final,
            epoch: payload.epoch,
            lsn: message_lsn,
        }));
    }
    Watermark::parse(&Message::LogicalMessage {
        transactional: true,
        lsn: message_lsn,
        prefix: crate::pg::chunk::MESSAGE_PREFIX,
        content,
    })
    .map(CapturedControl::Chunk)
    .map_err(|_| MarkerProgressError::UnknownPayload)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredWatermark {
    message_lsn: u64,
    content: Vec<u8>,
}

pub fn classify_captured_control(event: &crate::event::Event) -> Result<CapturedControl> {
    if event.kind != EventKind::Watermark {
        return Ok(CapturedControl::Other(event.kind.clone()));
    }
    let value = event
        .after
        .as_ref()
        .and_then(|row| row.get("control"))
        .filter(|value| value.type_id == "skedaddle.watermark.v1")
        .and_then(|value| value.bytes.as_deref())
        .ok_or(MarkerProgressError::UnknownPayload)?;
    if value.len() > MAX_RECEIPT {
        return Err(MarkerProgressError::UnknownPayload);
    }
    let stored: StoredWatermark =
        serde_json::from_slice(value).map_err(|_| MarkerProgressError::UnknownPayload)?;
    decode_marker_payload(&stored.content, stored.message_lsn)
}

pub(super) fn transaction_marker(txn: &AssembledTxn) -> Result<Option<Marker>> {
    let mut marker = None;
    for event in &txn.events {
        if let CapturedControl::Final(found) = classify_captured_control(event)?
            && marker.replace(found).is_some()
        {
            return Err(MarkerProgressError::InvalidEvidence);
        }
    }
    Ok(marker)
}

type SourceBinding = (String, String, String, String, String);
fn source_binding(source: &SourceIdentity) -> SourceBinding {
    (
        source.engine.clone(),
        source.system_id.clone(),
        source.lineage.clone(),
        source.db_name.clone(),
        source.slot.clone(),
    )
}

/// A sealed receipt: callers cannot construct one from an LSN high-water.
/// `checkpoint` is present only in the durable applied attestation itself.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinalMarkerReceipt {
    migration: String,
    source: SourceBinding,
    domain: String,
    marker: Marker,
    txn: Vec<u8>,
    txn_digest: ContentDigest,
    commit_lsn: u64,
    manifest_mseq: u64,
    manifest_digest: ContentDigest,
    checkpoint: Option<String>,
}
impl FinalMarkerReceipt {
    pub fn marker(&self) -> &Marker {
        &self.marker
    }
    pub fn migration(&self) -> &str {
        &self.migration
    }
    pub fn manifest(&self) -> DurableManifestRef {
        DurableManifestRef {
            mseq: self.manifest_mseq,
            manifest_digest: self.manifest_digest,
        }
    }
    fn buffered(&self) -> Self {
        let mut value = self.clone();
        value.checkpoint = None;
        value
    }
    fn validate(
        &self,
        migration: &str,
        source: &SourceIdentity,
        domain: &str,
        epoch: u64,
    ) -> Result<()> {
        if self.migration != migration
            || self.source != source_binding(source)
            || self.domain != domain
        {
            return Err(MarkerProgressError::Rebound);
        }
        if self.marker.epoch <= 0
            || u64::try_from(self.marker.epoch)
                .ok()
                .is_none_or(|marker_epoch| marker_epoch > epoch)
        {
            return Err(MarkerProgressError::EpochChanged);
        }
        if self.marker.cutover.is_empty()
            || self.marker.cutover.len() > 4096
            || self.marker.lsn == 0
            || self.commit_lsn < self.marker.lsn
            || self.txn.is_empty()
            || self.manifest_digest == [0; 32]
        {
            return Err(MarkerProgressError::InvalidEvidence);
        }
        if let Some(encoded) = &self.checkpoint {
            let checkpoint = super::decode_reader_checkpoint(encoded)
                .map_err(|_| MarkerProgressError::InvalidEvidence)?;
            self.check_checkpoint(&checkpoint)?;
        }
        Ok(())
    }
    fn check_checkpoint(&self, checkpoint: &ReaderCheckpoint) -> Result<()> {
        if checkpoint.next_mseq
            != self
                .manifest_mseq
                .checked_add(1)
                .ok_or(MarkerProgressError::InvalidEvidence)?
            || checkpoint.prev_digest != self.manifest_digest
            || checkpoint.last_position.as_ref().is_none_or(|p| {
                p.order_domain() != self.domain || p.bytes() != self.commit_lsn.to_be_bytes()
            })
            || checkpoint
                .last_record
                .as_ref()
                .is_none_or(|r| r.chunk_rows || source_binding(&r.source) != self.source)
        {
            return Err(MarkerProgressError::InvalidEvidence);
        }
        Ok(())
    }
}

#[derive(Clone)]
struct Binding {
    migration: String,
    source: SourceIdentity,
    domain: String,
    owner: String,
    epoch: u64,
    generation: u64,
}
#[derive(Default)]
struct State {
    binding: Option<Binding>,
    generation: u64,
    running: bool,
    invalid: Option<MarkerProgressError>,
    buffered: Option<FinalMarkerReceipt>,
    applied: Option<FinalMarkerReceipt>,
}
#[derive(Default)]
pub(super) struct Hub {
    state: Mutex<State>,
    changed: Notify,
}
impl Hub {
    pub(super) fn begin(
        &self,
        migration: &str,
        source: SourceIdentity,
        domain: String,
        snapshot: &Materialized,
    ) -> Result<()> {
        let mut state = self.state.lock().expect("final marker state poisoned");
        state.generation = state
            .generation
            .checked_add(1)
            .ok_or(MarkerProgressError::InvalidEvidence)?;
        state.running = false;
        state.invalid = None;
        state.buffered = None;
        state.applied = None;
        let binding = Binding {
            migration: migration.into(),
            source,
            domain,
            owner: snapshot.owner.clone(),
            epoch: snapshot.epoch,
            generation: state.generation,
        };
        for (stream, applied) in [(BUFFERED, false), (APPLIED, true)] {
            if let Some((encoded, position)) = snapshot.progress.get(stream) {
                if encoded.len() > MAX_RECEIPT {
                    return Err(MarkerProgressError::InvalidEvidence);
                }
                let receipt: FinalMarkerReceipt = serde_json::from_str(encoded)
                    .map_err(|_| MarkerProgressError::InvalidEvidence)?;
                // Authenticated receipts remain in journal history. A new
                // lineage starts empty. A new owner authenticates the original
                // evidence with a new run/authority binding; source markers
                // retain their original epochs across promotion.
                if receipt.migration != migration
                    || receipt.source != source_binding(&binding.source)
                    || receipt.domain != binding.domain
                {
                    continue;
                }
                receipt.validate(migration, &binding.source, &binding.domain, binding.epoch)?;
                if receipt.marker.lsn.to_string() != *position
                    || receipt.checkpoint.is_some() != applied
                {
                    return Err(MarkerProgressError::InvalidEvidence);
                }
                if applied {
                    state.applied = Some(receipt);
                } else {
                    state.buffered = Some(receipt);
                }
            }
        }
        if let Some(applied) = &state.applied
            && state.buffered.as_ref() != Some(&applied.buffered())
        {
            return Err(MarkerProgressError::InvalidEvidence);
        }
        state.binding = Some(binding);
        state.running = true;
        self.changed.notify_waiters();
        Ok(())
    }
    pub(super) fn finish(&self) {
        self.invalidate(MarkerProgressError::RunExited);
    }
    fn invalidate(&self, error: MarkerProgressError) {
        let mut state = self.state.lock().expect("final marker state poisoned");
        state.invalid = Some(error);
        state.running = false;
        self.changed.notify_waiters();
    }
    fn binding(&self) -> Result<Binding> {
        let state = self.state.lock().expect("final marker state poisoned");
        if let Some(error) = &state.invalid {
            return Err(error.clone());
        }
        if !state.running {
            return Err(MarkerProgressError::RunExited);
        }
        state.binding.clone().ok_or(MarkerProgressError::RunExited)
    }
    fn query(
        &self,
        binding: &Binding,
        marker: &Marker,
        applied: bool,
    ) -> Result<Option<FinalMarkerReceipt>> {
        let state = self.state.lock().expect("final marker state poisoned");
        if let Some(error) = &state.invalid {
            return Err(error.clone());
        }
        if !state.running {
            return Err(MarkerProgressError::RunExited);
        }
        let current = state
            .binding
            .as_ref()
            .ok_or(MarkerProgressError::RunExited)?;
        if current.generation != binding.generation
            || current.source != binding.source
            || current.domain != binding.domain
            || current.migration != binding.migration
        {
            return Err(MarkerProgressError::Rebound);
        }
        if current.epoch != binding.epoch {
            return Err(MarkerProgressError::EpochChanged);
        }
        let receipt = if applied {
            &state.applied
        } else {
            &state.buffered
        };
        if receipt.as_ref().is_some_and(|r| &r.marker != marker) {
            return Err(MarkerProgressError::IdentityMismatch);
        }
        Ok(receipt.clone())
    }
}

pub(super) struct RunGuard(pub Arc<Hub>);
impl Drop for RunGuard {
    fn drop(&mut self) {
        self.0.finish();
    }
}

/// Bound to this owner run, migration, source lineage and stable cutover key.
#[derive(Clone)]
pub struct FinalMarkerProgress {
    runtime: Runtime,
    binding: Binding,
    cutover: String,
}
impl FinalMarkerProgress {
    /// Synchronous snapshot for the existing cutover driver, which holds its
    /// exclusive Authority guard across this call. Independent owner clients
    /// should use `query` or `wait`, which refresh journal ownership first.
    pub fn receipt(&self, marker: &Marker, applied: bool) -> Result<Option<FinalMarkerReceipt>> {
        if marker.cutover != self.cutover {
            return Err(MarkerProgressError::IdentityMismatch);
        }
        if marker.epoch <= 0
            || u64::try_from(marker.epoch)
                .ok()
                .is_none_or(|epoch| epoch > self.binding.epoch)
        {
            return Err(MarkerProgressError::EpochChanged);
        }
        let receipt = self
            .runtime
            .6
            .markers
            .query(&self.binding, marker, applied)?;
        let snapshot = self.runtime.snapshot();
        if snapshot.epoch != self.binding.epoch || snapshot.owner != self.binding.owner {
            let mut state = self
                .runtime
                .6
                .markers
                .state
                .lock()
                .expect("final marker state poisoned");
            if state
                .binding
                .as_ref()
                .is_some_and(|b| b.generation == self.binding.generation)
            {
                state.invalid = Some(MarkerProgressError::EpochChanged);
                self.runtime.6.markers.changed.notify_waiters();
            }
            return Err(MarkerProgressError::EpochChanged);
        }
        Ok(receipt)
    }
    pub async fn query(
        &self,
        marker: &Marker,
        applied: bool,
    ) -> Result<Option<FinalMarkerReceipt>> {
        self.runtime.refresh_final_marker_authority().await?;
        self.receipt(marker, applied)
    }
    pub async fn wait(
        &self,
        marker: &Marker,
        applied: bool,
        timeout: Duration,
    ) -> Result<FinalMarkerReceipt> {
        if timeout.is_zero() {
            return Err(MarkerProgressError::Timeout);
        }
        tokio::time::timeout(timeout, async {
            loop {
                let notified = self.runtime.6.markers.changed.notified();
                tokio::pin!(notified); notified.as_mut().enable();
                if let Some(receipt) = self.query(marker, applied).await? { return Ok(receipt); }
                tokio::select! { _ = notified => {}, _ = tokio::time::sleep(Duration::from_millis(25)) => {} }
            }
        }).await.map_err(|_| MarkerProgressError::Timeout)?
    }
}
impl cutover::Progress for FinalMarkerProgress {
    fn buffered(&mut self, marker: &Marker) -> cutover::Result<Option<Marker>> {
        self.receipt(marker, false)
            .map(|r| r.map(|r| r.marker))
            .map_err(|e| cutover::Error::Component(e.to_string()))
    }
    fn applied(&mut self, marker: &Marker) -> cutover::Result<Option<Marker>> {
        self.receipt(marker, true)
            .map(|r| r.map(|r| r.marker))
            .map_err(|e| cutover::Error::Component(e.to_string()))
    }
}

impl Runtime {
    async fn refresh_final_marker_authority(&self) -> Result<()> {
        let mut coordinator = self.0.lock().await;
        let result = Box::pin(coordinator.refresh()).await;
        *self.1.write().expect("engine snapshot poisoned") = coordinator.state().clone();
        if result.is_err() || !coordinator.is_owner() {
            self.6.markers.invalidate(MarkerProgressError::EpochChanged);
            return Err(MarkerProgressError::EpochChanged);
        }
        Ok(())
    }
    pub fn final_marker_progress(
        &self,
        key: &Key,
        source: &SourceIdentity,
    ) -> Result<FinalMarkerProgress> {
        let binding = self.6.markers.binding()?;
        if key.cutover.is_empty()
            || key.cutover.len() > 4096
            || key.migration != binding.migration
            || source != &binding.source
            || key.coordinator != binding.owner
        {
            return Err(MarkerProgressError::Rebound);
        }
        if u64::try_from(key.epoch).ok() != Some(binding.epoch) {
            return Err(MarkerProgressError::EpochChanged);
        }
        Ok(FinalMarkerProgress {
            runtime: self.clone(),
            binding,
            cutover: key.cutover.clone(),
        })
    }
    pub fn invalidate_final_marker_capture_gap(&self) {
        self.6.markers.invalidate(MarkerProgressError::CaptureGap);
    }

    fn make_final_receipt(
        &self,
        txn: &AssembledTxn,
        marker: Marker,
        manifest: DurableManifestRef,
        checkpoint: Option<String>,
    ) -> Result<FinalMarkerReceipt> {
        let binding = self.6.markers.binding()?;
        let receipt = FinalMarkerReceipt {
            migration: binding.migration.clone(),
            source: source_binding(&txn.source),
            domain: txn.commit_position.order_domain().into(),
            marker,
            txn: txn.txn.0.clone(),
            txn_digest: txn.digest,
            commit_lsn: u64::from_be_bytes(
                txn.commit_position
                    .bytes()
                    .try_into()
                    .map_err(|_| MarkerProgressError::InvalidEvidence)?,
            ),
            manifest_mseq: manifest.mseq,
            manifest_digest: manifest.manifest_digest,
            checkpoint,
        };
        receipt.validate(
            &binding.migration,
            &binding.source,
            &binding.domain,
            binding.epoch,
        )?;
        Ok(receipt)
    }
    async fn persist_final_receipt(&self, receipt: FinalMarkerReceipt) -> super::Result<()> {
        self.flush_progress().await?;
        let applied = receipt.checkpoint.is_some();
        let mut coordinator = self.0.lock().await;
        let binding = self.6.markers.binding().map_err(recovery)?;
        if coordinator.state().epoch != binding.epoch || coordinator.state().owner != binding.owner
        {
            self.6.markers.invalidate(MarkerProgressError::EpochChanged);
            return Err(recovery(MarkerProgressError::EpochChanged));
        }
        receipt
            .validate(
                &binding.migration,
                &binding.source,
                &binding.domain,
                binding.epoch,
            )
            .map_err(recovery)?;
        if let Some(existing) = self
            .6
            .markers
            .query(&binding, &receipt.marker, applied)
            .map_err(recovery)?
        {
            if existing != receipt {
                return Err(recovery(MarkerProgressError::InvalidEvidence));
            }
            return Ok(());
        }
        let encoded = serde_json::to_string(&receipt)?;
        if encoded.len() > MAX_RECEIPT {
            return Err(recovery(MarkerProgressError::InvalidEvidence));
        }
        let result = coordinator
            .commit(Transition::Progress {
                stream: if applied { APPLIED } else { BUFFERED }.into(),
                marker: encoded,
                position: receipt.marker.lsn.to_string(),
            })
            .await;
        self.observe_journal_error(&coordinator, &result);
        *self.1.write().expect("engine snapshot poisoned") = coordinator.state().clone();
        if result.is_err() {
            self.6.markers.invalidate(MarkerProgressError::CaptureGap);
        }
        result?;
        let mut state = self
            .6
            .markers
            .state
            .lock()
            .expect("final marker state poisoned");
        if state.invalid.is_some()
            || state
                .binding
                .as_ref()
                .is_none_or(|b| b.generation != binding.generation)
        {
            return Err(recovery(MarkerProgressError::Rebound));
        }
        if applied {
            state.applied = Some(receipt);
        } else {
            state.buffered = Some(receipt);
        }
        self.6.markers.changed.notify_waiters();
        Ok(())
    }
    pub(super) async fn capture_final_marker(
        &self,
        batch: &Captured,
        checkpoint: Option<&WriterCheckpoint>,
    ) -> super::Result<()> {
        for watermark in &batch.watermarks {
            decode_marker_payload(&watermark.content, watermark.message_lsn).map_err(recovery)?;
        }
        let Some(txn) = &batch.transaction else {
            return Ok(());
        };
        let Some(marker) = transaction_marker(txn).map_err(recovery)? else {
            return Ok(());
        };
        let retained = checkpoint
            .and_then(|c| {
                c.redelivery.iter().find(|r| {
                    r.source == txn.source
                        && r.txn == txn.txn
                        && r.position == txn.commit_position
                        && r.digest == txn.digest
                })
            })
            .and_then(|r| r.manifest.clone())
            .ok_or_else(|| recovery(MarkerProgressError::InvalidEvidence))?;
        let receipt = self
            .make_final_receipt(txn, marker, retained, None)
            .map_err(recovery)?;
        self.persist_final_receipt(receipt).await
    }
    pub(super) async fn applied_final_marker(
        &self,
        txn: &AssembledTxn,
        mseq: u64,
        checkpoint: &ReaderCheckpoint,
    ) -> super::Result<()> {
        let Some(marker) = transaction_marker(txn).map_err(recovery)? else {
            return Ok(());
        };
        // Exact segment end, exact canonical transaction and exact marker bytes.
        // A larger checkpoint/LSN is deliberately insufficient.
        let receipt = self
            .make_final_receipt(
                txn,
                marker,
                DurableManifestRef {
                    mseq,
                    manifest_digest: checkpoint.prev_digest,
                },
                Some(super::encode_reader_checkpoint(checkpoint)?),
            )
            .map_err(recovery)?;
        self.persist_final_receipt(receipt.buffered()).await?;
        // This payload atomically journals the position attestation before it
        // becomes observable, and before the ordinary reader checkpoint moves.
        self.persist_final_receipt(receipt).await
    }
}

#[cfg(test)]
#[path = "final_marker_tests.rs"]
mod tests;
