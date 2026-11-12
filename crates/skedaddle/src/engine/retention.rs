//! Current-owner retention, bounded to one manifest per cadence. Pending facts
//! coalesce in memory; only confirmed durable facts can authorize deletion.
use super::*;
use crate::{
    buffer::{
        backends::BufferStore,
        format::ManifestEntry,
        gc::{AppliedSegment, RetentionGc, RetentionPolicy},
        reader::{self, ReaderCheckpoint, ReaderConfig, VerifiedManifest},
        writer::WriterCheckpoint,
    },
    coordinator::gc_authority::{CoordinatorGcJournal, coordinator_gc_error},
    event::{Position, PositionOrder},
};
use std::{
    cmp::Ordering,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

/// A resumable engine stop, preserved independently of secondary worker errors.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EnginePauseReason {
    CoordinatorJournalUnavailable,
}
impl std::fmt::Display for EnginePauseReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CoordinatorJournalUnavailable => {
                f.write_str("coordinator journal unavailable; resume when the store is healthy")
            }
        }
    }
}
impl std::error::Error for EnginePauseReason {}
impl EnginePauseReason {
    pub(super) fn error(self) -> coordinator::Error {
        coordinator::Error::Io(std::io::Error::other(self))
    }
}
fn operational(error: &coordinator::Error) -> bool {
    matches!(
        error,
        coordinator::Error::Store(_)
            | coordinator::Error::Io(_)
            | coordinator::Error::UnknownOutcome(_)
            | coordinator::Error::BudgetExceeded(_)
            | coordinator::Error::TruncatedRead(_)
            | coordinator::Error::Core(giggles_core::CoreError::Storage(_))
    )
}

impl Runtime {
    pub fn pause_reason(&self) -> Option<EnginePauseReason> {
        *self.4.read().expect("engine stop reason poisoned")
    }
    fn latch_journal_pause(&self) -> coordinator::Error {
        let reason = EnginePauseReason::CoordinatorJournalUnavailable;
        *self.4.write().expect("engine stop reason poisoned") = Some(reason);
        reason.error()
    }
    pub(super) fn observe_journal_error<T>(&self, owner: &Coordinator, result: &Result<T>) {
        if !owner.is_owner() && result.as_ref().is_err_and(operational) {
            self.latch_journal_pause();
        }
    }
    pub(super) fn local_recovery(&self, reason: impl std::fmt::Display) -> coordinator::Error {
        let error = recovery(reason);
        let mut state = self.1.write().expect("engine snapshot poisoned");
        state.lifecycle.state = State::RecoveryRequired;
        state.lifecycle.blockers.push(error.to_string());
        *self.4.write().expect("engine stop reason poisoned") = None;
        error
    }
}
impl Engine {
    // Called only after borrowed workers have joined, or before resume starts.
    pub(super) async fn reacquire_after_journal_pause(&self) -> Result<()> {
        let expected = self
            .runtime
            .5
            .read()
            .expect("engine authority poisoned")
            .clone();
        let mut owner = self.runtime.0.lock().await;
        if let Err(error) = owner.refresh().await {
            return Err(if operational(&error) {
                self.runtime.latch_journal_pause()
            } else {
                self.runtime.local_recovery(error)
            });
        }
        if owner.state().owner != expected.0
            || (owner.state().epoch != expected.1 && Some(owner.state().epoch) != expected.2)
        {
            return Err(self
                .runtime
                .local_recovery("authority changed during journal outage"));
        }
        // Record the exact next epoch before an ambiguous create. Only this
        // instance's own attempted acquire may be reconciled on the next resume.
        let attempted_epoch = owner.state().epoch.checked_add(1);
        self.runtime.5.write().expect("engine authority poisoned").2 = attempted_epoch;
        if let Err(error) = owner.acquire().await {
            return Err(if operational(&error) {
                self.runtime.latch_journal_pause()
            } else {
                self.runtime.local_recovery(error)
            });
        }
        if Some(owner.state().epoch) != attempted_epoch {
            return Err(self
                .runtime
                .local_recovery("authority raced journal-pause reacquire"));
        }
        *self.runtime.5.write().expect("engine authority poisoned") =
            (owner.state().owner.clone(), owner.state().epoch, None);
        *self.runtime.1.write().expect("engine snapshot poisoned") = owner.state().clone();
        Ok(())
    }
    pub(super) async fn finish_journal_pause(&self, stop_at: StopAt) -> Result<Reached> {
        let reason = EnginePauseReason::CoordinatorJournalUnavailable;
        if let Err(error) = self.reacquire_after_journal_pause().await {
            if self.runtime.pause_reason().is_some() {
                let mut state = self.runtime.1.write().expect("engine snapshot poisoned");
                state.lifecycle.state = State::Paused;
                state.lifecycle.blockers.push(reason.to_string());
            }
            return Err(error);
        }
        // VerifiedReady must first be demoted by a fresh post-join observation.
        if self.runtime.snapshot().lifecycle.state == State::VerifiedReady
            && let Err(error) = self.evaluate_readiness().await
        {
            return Err(if operational(&error) {
                reason.error()
            } else {
                self.runtime.local_recovery(error)
            });
        }
        if self.runtime.snapshot().lifecycle.state != State::Paused
            && let Err(error) = self
                .runtime
                .lifecycle(Event::Interrupt {
                    to: State::Paused,
                    reason: reason.to_string(),
                })
                .await
        {
            return Err(if operational(&error) {
                let mut state = self.runtime.1.write().expect("engine snapshot poisoned");
                state.lifecycle.state = State::Paused;
                state.lifecycle.blockers.push(reason.to_string());
                reason.error()
            } else {
                self.runtime.local_recovery(error)
            });
        }
        // No further journal reads: an outage after the durable pause must not
        // obscure the joined, typed stop result.
        Ok(Reached {
            state: State::Paused,
            blockers: vec![reason.to_string()],
            target_reached: stop_at == StopAt::UntilPaused,
        })
    }
    pub(super) async fn resume_after_journal_pause(&self) -> Result<()> {
        let journal_pause = self.runtime.pause_reason().is_some()
            || self
                .runtime
                .snapshot()
                .lifecycle
                .blockers
                .iter()
                .any(|b| b == &EnginePauseReason::CoordinatorJournalUnavailable.to_string());
        if !journal_pause {
            return self.runtime.lifecycle(Event::Resume).await;
        }
        if self.running.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(coordinator::Error::Rejected(
                "join engine workers before resume".into(),
            ));
        }
        self.reacquire_after_journal_pause().await?;
        if self.runtime.snapshot().lifecycle.state == State::Paused
            && let Err(error) = self.runtime.lifecycle(Event::Resume).await
        {
            return Err(if operational(&error) {
                EnginePauseReason::CoordinatorJournalUnavailable.error()
            } else {
                self.runtime.local_recovery(error)
            });
        }
        if let Some(retention) = self.runtime.3.lock().await.as_mut() {
            retention.authority = None;
            retention.expected_epoch = self.runtime.snapshot().epoch;
            retention.status.blocked_reason = None;
            retention.status.recovery_required = false;
            retention.next_tick = Instant::now();
        }
        *self.runtime.4.write().expect("engine stop reason poisoned") = None;
        Ok(())
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RetentionStatus {
    pub blocked_reason: Option<String>,
    pub recovery_required: bool,
    pub deleted_objects: u64,
    pub deleted_bytes: u64,
}
pub(super) struct Retention {
    authority: Option<CoordinatorGcJournal>,
    expected_epoch: u64,
    store: Arc<dyn BufferStore>,
    config: ReaderConfig,
    order: Arc<dyn PositionOrder + Send + Sync>,
    collector: RetentionGc,
    policy: RetentionPolicy,
    cadence: Duration,
    next_tick: Instant,
    cursor: u64,
    eligible_since: Option<Instant>,
    applied: Option<Position>,
    applied_pending: bool,
    reader: Option<ReaderCheckpoint>,
    writer: Option<WriterCheckpoint>,
    status: RetentionStatus,
}
impl Engine {
    /// Bind retention to this engine's current ownership epoch. Recreate after
    /// failover. Call before run; cadence bounds work to one manifest per tick.
    pub async fn enable_retention(
        &self,
        store: Arc<dyn BufferStore>,
        config: ReaderConfig,
        order: Arc<dyn PositionOrder + Send + Sync>,
        policy: RetentionPolicy,
        cadence: Duration,
    ) -> Result<()> {
        if cadence.is_zero() || self.running.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(recovery("retention requires a positive cadence before run"));
        }
        let expected_epoch = self.runtime.snapshot().epoch;
        *self.runtime.3.lock().await = Some(Retention {
            authority: None,
            expected_epoch,
            collector: RetentionGc::new(store.clone(), order.clone(), policy),
            store,
            config,
            order,
            policy,
            cadence,
            next_tick: Instant::now(),
            cursor: 0,
            eligible_since: None,
            applied: None,
            applied_pending: false,
            reader: None,
            writer: None,
            status: RetentionStatus::default(),
        });
        self.runtime.retention_tick().await
    }
}
impl Runtime {
    pub async fn retention_status(&self) -> Option<RetentionStatus> {
        self.3.lock().await.as_ref().map(|r| r.status.clone())
    }
    pub(super) async fn queue_writer(&self, checkpoint: Option<WriterCheckpoint>) {
        if let (Some(retention), Some(checkpoint)) = (self.3.lock().await.as_mut(), checkpoint) {
            retention.writer = Some(checkpoint);
        }
    }
    pub(super) async fn queue_applied(&self, position: Position, checkpoint: ReaderCheckpoint) {
        if let Some(retention) = self.3.lock().await.as_mut() {
            retention.applied = Some(position);
            retention.applied_pending = true;
            retention.reader = Some(checkpoint);
        }
    }
    async fn sync_authority(&self) {
        let c = self.0.lock().await;
        *self.1.write().expect("engine snapshot poisoned") = c.state().clone();
    }
    /// One bounded owner-requested pass. It uses the same checkpoint, fence and
    /// retention interval as automatic GC; a request never bypasses policy.
    pub async fn collect_buffer_once(&self) -> Result<serde_json::Value> {
        let mut guard = self.3.lock().await;
        let retention = guard
            .as_mut()
            .ok_or_else(|| recovery("retention unavailable"))?;
        {
            let mut owner = self.0.lock().await;
            owner.refresh().await?;
            if !owner.is_owner() || owner.state().epoch != retention.expected_epoch {
                return Err(coordinator::Error::LostOwnership);
            }
        }
        let mseq = retention.cursor;
        let objects = retention.status.deleted_objects;
        let bytes = retention.status.deleted_bytes;
        let result = self.retention_step(retention).await;
        {
            let owner = self.0.lock().await;
            if owner.state().epoch != retention.expected_epoch
                || (!owner.is_owner() && self.pause_reason().is_none())
            {
                return Err(coordinator::Error::LostOwnership);
            }
        }
        result?;
        if retention.status.blocked_reason.is_some() {
            return Err(recovery("buffer retention blocked"));
        }
        let deleted = retention.status.deleted_objects - objects;
        let retained = retention
            .store
            .get(&retention.config.manifest_key(mseq))
            .await
            .map_err(recovery)?
            .is_some();
        Ok(serde_json::json!({
            "examined_mseq": mseq, "next_mseq": retention.cursor,
            "deleted_objects": deleted, "deleted_bytes": retention.status.deleted_bytes - bytes,
            "retained_mseq": retained.then_some(mseq),
            "scope": "One manifest cursor step; durable checkpoints, chunk pins and retention policy remain authoritative."
        }))
    }
    pub(super) async fn retention_tick(&self) -> Result<()> {
        let mut guard = self.3.lock().await;
        let Some(retention) = guard.as_mut() else {
            return Ok(());
        };
        if Instant::now() < retention.next_tick {
            return Ok(());
        }
        self.retention_step(retention).await
    }
    async fn retention_step(&self, retention: &mut Retention) -> Result<()> {
        retention.next_tick = Instant::now() + retention.cadence;
        let result = retention.tick(self).await;
        self.sync_authority().await;
        match result {
            Ok(()) => {
                retention.status.blocked_reason = None;
                Ok(())
            }
            Err(reader::Error::Store(error)) => {
                retention.status.blocked_reason =
                    Some(format!("buffer-retention-blocked: {error}"));
                if !self.0.lock().await.is_owner() {
                    return Err(self.latch_journal_pause());
                }
                Ok(())
            }
            Err(error) => {
                if self.pause_reason().is_some() {
                    return Err(EnginePauseReason::CoordinatorJournalUnavailable.error());
                }
                let reason = format!("buffer-retention-recovery-required: {error}");
                retention.status.blocked_reason = Some(reason.clone());
                retention.status.recovery_required = true;
                // An epoch loser cannot append a lifecycle transition. Expose
                // recovery locally while leaving the new owner's journal intact.
                let mut state = self.1.write().expect("engine snapshot poisoned");
                state.lifecycle.state = State::RecoveryRequired;
                state.lifecycle.blockers.push(reason);
                Err(recovery(error))
            }
        }
    }
}
impl Retention {
    async fn initialize(&mut self, runtime: &Runtime) -> reader::Result<()> {
        if self.authority.is_some() {
            return Ok(());
        }
        let authority = CoordinatorGcJournal::new(
            runtime.0.clone(),
            self.store.clone(),
            self.config.clone(),
            self.order.clone(),
        )
        .await
        .map_err(coordinator_gc_error)?;
        // Initialization may be retried after an outage, but must never adopt a
        // replacement owner's epoch while capture/apply still use the old one.
        if runtime.0.lock().await.state().epoch != self.expected_epoch {
            return Err(reader::Error::RecoveryRequired(
                "retention initialization epoch changed",
            ));
        }
        let reader = authority
            .reader_checkpoint()
            .await
            .map_err(coordinator_gc_error)?;
        let writer = authority
            .writer_checkpoint()
            .await
            .map_err(coordinator_gc_error)?;
        if self.reader.is_none() {
            self.reader = reader;
        }
        if self.writer.is_none() {
            self.writer = writer;
        }
        // A saved complete boundary is a conservative prefix, used only for
        // eligibility; never overwrite a newer durable partial receipt with it.
        if self.applied.is_none() {
            self.applied = self.reader.as_ref().and_then(|c| c.last_position.clone());
        }
        self.authority = Some(authority);
        Ok(())
    }
    async fn tick(&mut self, runtime: &Runtime) -> reader::Result<()> {
        self.initialize(runtime).await?;
        let authority = self
            .authority
            .as_ref()
            .expect("initialized retention authority");
        // Always fence, even with no new receipt. An old owner cannot silently
        // keep running once retention detects a migration epoch change.
        authority
            .reader_checkpoint()
            .await
            .map_err(coordinator_gc_error)?;
        if self.applied_pending
            && let Some(applied) = &self.applied
        {
            authority
                .persist_applied_through(applied)
                .await
                .map_err(coordinator_gc_error)?;
            self.applied_pending = false;
        }
        if let Some(reader) = &self.reader {
            authority
                .persist_reader_checkpoint(reader)
                .await
                .map_err(coordinator_gc_error)?;
        }
        if let Some(writer) = &self.writer {
            authority
                .persist_writer_checkpoint(writer)
                .await
                .map_err(coordinator_gc_error)?;
        }
        let (Some(reader), Some(writer), Some(applied)) =
            (&self.reader, &self.writer, &self.applied)
        else {
            return Ok(());
        };
        if self.cursor >= reader.next_mseq.min(writer.next_mseq) {
            return Ok(());
        }
        // Journal replay still authenticates ChunkDone against its retained
        // ChunkRows object. Pin those exact references while allowing later WAL
        // to retire; independent durable row evidence is a separate protocol.
        if runtime.snapshot().chunk_attempts.values().any(|attempt| {
            attempt
                .done
                .as_ref()
                .is_some_and(|done| done.chunk_rows.mseq == self.cursor)
        }) {
            self.cursor += 1;
            self.eligible_since = None;
            return Ok(());
        }
        let Some(bytes) = self
            .store
            .get(&self.config.manifest_key(self.cursor))
            .await?
        else {
            // Completed prior retention. At most one absent key per cadence.
            self.cursor += 1;
            self.eligible_since = None;
            return Ok(());
        };
        let entry = ManifestEntry::from_canonical_json(&bytes)
            .map_err(|_| reader::Error::RecoveryRequired("retention manifest corruption"))?;
        if entry.mseq != self.cursor {
            return Err(reader::Error::RecoveryRequired(
                "retention manifest sequence",
            ));
        }
        if applied
            .compare(&entry.last_pos, self.order.as_ref())
            .map_err(|_| reader::Error::RecoveryRequired("retention position domain"))?
            != Ordering::Greater
        {
            return Ok(());
        }
        // Conservatively wait a full retention interval after discovering an
        // eligible candidate, including on restart. Intent timestamps stay zero
        // so exact retry identity survives crashes and ambiguous durable appends.
        let since = self.eligible_since.get_or_insert_with(Instant::now);
        if since.elapsed() < Duration::from_millis(self.policy.retained_for_ms) {
            return Ok(());
        }
        let proof_key = format!("engine-retention-proof:{}", self.config.stream);
        let canonical = String::from_utf8(bytes)
            .map_err(|_| reader::Error::RecoveryRequired("retention manifest encoding"))?;
        if self.store.get(&entry.segment_key).await?.is_some() {
            reader::verify_segment(
                self.store.as_ref(),
                &self.config,
                &entry,
                None,
                self.order.as_ref(),
            )
            .await?;
            // One bounded durable proof survives a crash between object deletes.
            if runtime.snapshot().progress.get(&proof_key)
                != Some(&(canonical.clone(), self.cursor.to_string()))
            {
                runtime
                    .marker(&proof_key, canonical.clone(), self.cursor.to_string())
                    .await
                    .map_err(coordinator_gc_error)?;
            }
        } else if runtime.snapshot().progress.get(&proof_key)
            != Some(&(canonical, self.cursor.to_string()))
        {
            return Err(reader::Error::RecoveryRequired(
                "missing segment without durable verified retention proof",
            ));
        }
        // A missing segment is the segment-delete/manifest-delete crash window.
        // The authority still verifies full durable apply/checkpoint coverage
        // and the manifest identity before acknowledging its retirement intent.
        let digest = entry
            .digest()
            .map_err(|_| reader::Error::RecoveryRequired("retention manifest digest"))?;
        let proof = VerifiedManifest {
            config: self.config.clone(),
            entry,
            digest,
        };
        let candidate = AppliedSegment::new(proof, 0, 0);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .min(u64::MAX as u128) as u64;
        let outcome = self
            .collector
            .collect_applied(&candidate, now, authority)
            .await?;
        self.status.deleted_objects += outcome.deleted_objects;
        self.status.deleted_bytes += outcome.deleted_bytes;
        self.cursor += 1;
        self.eligible_since = None;
        Ok(())
    }
}
