//! Live verification observations are process-local and never restore readiness.
//! Only manifest-durable capture and journaled, authenticated Applied receipts
//! enter this state. A new run invalidates every old observer and wait adapter.
use super::{Captured, Runtime};
use crate::{
    buffer::reader::ReaderCheckpoint,
    event::{Event, EventKind, Position, Row, SourceIdentity},
    mapping::Value,
    pg::chunk::{ChunkId, TableKey, Watermark, WatermarkKind, key_bytes},
    verify::{
        self, ApplierTerminalCause, ChangeEvidence, EvidenceInvalidation, EvidenceLimit, Key,
        KeyOrder, Lsn, RangePlan, VerifyError, sql::SqlChangeEvidence,
    },
};
use async_trait::async_trait;
use sha2::{Digest, Sha256};
use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::Notify;
use tokio_postgres::Client;
use uuid::Uuid;

const MAX_OBSERVERS: usize = 64;

#[cfg(test)]
#[path = "verification_tests.rs"]
mod tests;

/// Hard bounds on each armed interval, including keys outside the range in the
/// same table. max_bytes bounds canonical key payloads/identities plus a fixed
/// per-entry charge; it is not a measurement of allocator-resident heap bytes.
/// Decoded Values are bounded by their canonical input size and max_keys.
#[derive(Clone, Copy, Debug)]
pub struct VerificationLimits {
    pub max_keys: usize,
    pub max_bytes: usize,
    /// Includes marker emission and capture observation, or an applied wait.
    pub timeout: Duration,
}

impl VerificationLimits {
    fn validate(self) -> verify::Result<()> {
        if self.max_keys == 0 || self.max_bytes == 0 || self.timeout.is_zero() {
            return Err(VerifyError::Invalid("zero engine verification limit"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Binding {
    source: SourceIdentity,
    domain: String,
    generation: u64,
}

#[derive(Default)]
struct State {
    binding: Option<Binding>,
    running: bool,
    terminal: Option<ApplierTerminalCause>,
    invalid: Option<EvidenceInvalidation>,
    applied: Option<Lsn>,
    arms: HashMap<Uuid, Box<Armed>>,
}

struct Armed {
    range: RangePlan,
    table: Arc<TableKey>,
    limits: VerificationLimits,
    keys: BTreeMap<Vec<u8>, Key>,
    bytes: usize,
    failure: Option<ArmFailure>,
    marker: Watermark,
    through: Option<Lsn>,
}

#[derive(Clone, Copy)]
enum ArmFailure {
    Overflow(EvidenceLimit),
    Invalid(EvidenceInvalidation),
}

impl ArmFailure {
    fn error(self, range: &RangePlan) -> VerifyError {
        match self {
            Self::Overflow(limit) => VerifyError::EvidenceOverflow {
                range: range.id.clone(),
                limit,
            },
            Self::Invalid(reason) => VerifyError::EvidenceInvalidated { reason },
        }
    }
}

#[derive(Default)]
pub(super) struct Hub {
    pub(super) markers: Arc<super::final_marker::Hub>,
    state: Mutex<State>,
    changed: Notify,
}

fn invalid(reason: EvidenceInvalidation) -> VerifyError {
    VerifyError::EvidenceInvalidated { reason }
}

pub(super) fn verification_marker(token: Uuid) -> Watermark {
    Watermark {
        wm: WatermarkKind::High,
        chunk: ChunkId {
            table: format!("skedaddle:verification:{token}"),
            plan: format!("{:x}", Sha256::digest(token.as_bytes())),
            ordinal: 0,
        },
        attempt: 0,
    }
}

fn source_change(expected: &SourceIdentity, actual: &SourceIdentity) -> EvidenceInvalidation {
    if expected.stream != actual.stream {
        EvidenceInvalidation::SlotChanged
    } else {
        EvidenceInvalidation::LineageChanged
    }
}

impl State {
    fn check_binding(&self, expected: &Binding) -> verify::Result<()> {
        let actual = self
            .binding
            .as_ref()
            .ok_or_else(|| invalid(EvidenceInvalidation::CaptureGap))?;
        if actual.source != expected.source {
            return Err(invalid(source_change(&expected.source, &actual.source)));
        }
        if actual.domain != expected.domain {
            return Err(invalid(EvidenceInvalidation::LineageChanged));
        }
        if actual.generation != expected.generation {
            return Err(invalid(EvidenceInvalidation::Reconnected));
        }
        if let Some(reason) = self.invalid {
            return Err(invalid(reason));
        }
        Ok(())
    }

    fn invalidate(&mut self, reason: EvidenceInvalidation) {
        self.invalid = Some(reason);
        self.applied = None;
        for arm in self.arms.values_mut() {
            arm.failure = Some(ArmFailure::Invalid(reason));
            arm.keys.clear();
        }
    }

    fn applier_timeout(
        &self,
        binding: &Binding,
        waited_lsn: Lsn,
        elapsed: Duration,
    ) -> VerifyError {
        // Marker SQL may remain pending after the engine has already stopped.
        // A known infrastructure failure must never turn into busy coverage.
        if let Some(cause) = self.terminal {
            return VerifyError::ApplierTerminal { cause };
        }
        if let Err(error) = self.check_binding(binding) {
            return error;
        }
        if !self.running {
            return VerifyError::ApplierTerminal {
                cause: ApplierTerminalCause::ApplyStopped,
            };
        }
        VerifyError::ApplierTimeout {
            waited_lsn,
            elapsed,
        }
    }

    fn evidence_timeout(
        &self,
        binding: &Binding,
        token: Uuid,
        range: &RangePlan,
        waited_lsn: Lsn,
        elapsed: Duration,
    ) -> VerifyError {
        if let Err(error) = self.check_binding(binding) {
            return error;
        }
        if !self.running {
            return invalid(EvidenceInvalidation::CaptureGap);
        }
        if let Some(cause) = self.terminal {
            return VerifyError::ApplierTerminal { cause };
        }
        let Some(arm) = self.arms.get(&token) else {
            return invalid(EvidenceInvalidation::CaptureGap);
        };
        if let Some(failure) = arm.failure {
            return failure.error(range);
        }
        VerifyError::ApplierTimeout {
            waited_lsn,
            elapsed,
        }
    }
}

impl Hub {
    pub(super) fn begin(&self, source: SourceIdentity, domain: String) {
        let mut state = self.state.lock().expect("verification state poisoned");
        let generation = state
            .binding
            .as_ref()
            .map_or(1, |b| b.generation.saturating_add(1));
        state.invalidate(EvidenceInvalidation::Reconnected);
        state.binding = Some(Binding {
            source,
            domain,
            generation,
        });
        state.running = true;
        state.terminal = None;
        state.invalid = None;
        state.applied = None;
        drop(state);
        self.changed.notify_waiters();
    }

    pub(super) fn finish(&self, cause: ApplierTerminalCause) {
        let mut state = self.state.lock().expect("verification state poisoned");
        state.running = false;
        state.terminal.get_or_insert(cause);
        state.invalidate(EvidenceInvalidation::CaptureGap);
        drop(state);
        self.changed.notify_waiters();
    }

    pub(super) fn observe(&self, batch: &Captured) {
        let mut state = self.state.lock().expect("verification state poisoned");
        let Some(binding) = state.binding.clone() else {
            return;
        };
        if !state.running || state.invalid.is_some() {
            return;
        }
        if let Some(txn) = &batch.transaction {
            if txn.source != binding.source {
                state.invalidate(source_change(&binding.source, &txn.source));
            } else if txn.commit_position.order_domain() != binding.domain
                || txn.commit_position.bytes().len() != 8
                || txn.events.iter().any(|event| {
                    event.source != binding.source
                        || event.position.order_domain() != binding.domain
                })
            {
                state.invalidate(EvidenceInvalidation::LineageChanged);
            } else {
                for arm in state.arms.values_mut() {
                    if arm.failure.is_some() || arm.through.is_some() {
                        continue;
                    }
                    // Never filter by a sampled snapshot LSN: this includes a
                    // transaction already in flight when the observer was armed.
                    for event in &txn.events {
                        arm.observe_event(event);
                        if arm.failure.is_some() {
                            break;
                        }
                    }
                }
            }
        }
        for watermark in &batch.watermarks {
            for arm in state.arms.values_mut() {
                if arm.failure.is_some() || arm.through.is_some() {
                    continue;
                }
                if Watermark::parse(&watermark.message()).is_ok_and(|marker| marker == arm.marker) {
                    let authenticated = batch.transaction.as_ref().is_some_and(|txn| {
                        txn.source == binding.source
                            && txn.commit_position.order_domain() == binding.domain
                            && txn.commit_position.bytes() == watermark.commit_lsn.to_be_bytes()
                    });
                    if !authenticated || !watermark.transactional {
                        arm.failure = Some(ArmFailure::Invalid(EvidenceInvalidation::CaptureGap));
                    } else {
                        arm.through = Some(watermark.commit_lsn);
                    }
                }
            }
        }
        if batch.stop {
            state.invalidate(EvidenceInvalidation::CaptureGap);
        }
        drop(state);
        self.changed.notify_waiters();
    }

    /// Called exclusively after ReaderLoop's durable Applied receipt AND the
    /// engine's applied journal marker. Retention being disabled is irrelevant.
    pub(super) fn applied(&self, position: &Position, checkpoint: &ReaderCheckpoint) {
        let mut state = self.state.lock().expect("verification state poisoned");
        let Some(binding) = state.binding.clone() else {
            return;
        };
        if !state.running || state.invalid.is_some() {
            return;
        }
        let Some(record) = &checkpoint.last_record else {
            state.terminal = Some(ApplierTerminalCause::CheckpointRejected);
            drop(state);
            self.changed.notify_waiters();
            return;
        };
        if record.source != binding.source {
            state.invalidate(source_change(&binding.source, &record.source));
        } else if position.order_domain() != binding.domain
            || checkpoint.last_position.as_ref() != Some(position)
        {
            state.invalidate(EvidenceInvalidation::LineageChanged);
        } else if !record.chunk_rows {
            if let Ok(bytes) = <[u8; 8]>::try_from(position.bytes()) {
                let lsn = u64::from_be_bytes(bytes);
                if state.applied.is_some_and(|previous| previous > lsn) || checkpoint.next_mseq == 0
                {
                    state.terminal = Some(ApplierTerminalCause::CheckpointRejected);
                } else {
                    state.applied = Some(lsn);
                }
            } else {
                state.terminal = Some(ApplierTerminalCause::CheckpointRejected);
            }
        }
        drop(state);
        self.changed.notify_waiters();
    }

    fn disarm(&self, token: Uuid) {
        self.state
            .lock()
            .expect("verification state poisoned")
            .arms
            .remove(&token);
    }
}

impl Armed {
    fn observe_event(&mut self, event: &Event) {
        if let EventKind::Truncate { relations, .. } = &event.kind {
            if relations.contains(&self.table.table) {
                self.failure = Some(ArmFailure::Invalid(EvidenceInvalidation::CaptureGap));
                self.keys.clear();
            }
            return;
        }
        if event.table.as_deref() != Some(&self.table.table)
            || !matches!(
                event.kind,
                EventKind::Insert | EventKind::Update | EventKind::Delete
            )
        {
            return;
        }
        // Match the capture/window interpretation: Event.key is the old key;
        // before/after may replace key columns. Retain every visible event,
        // including updates that revert earlier writes in the same transaction.
        self.touch(&event.key, None);
        if let Some(before) = &event.before {
            self.touch(&event.key, Some(before));
        }
        if matches!(event.kind, EventKind::Insert | EventKind::Update) {
            if let Some(after) = &event.after {
                self.touch(&event.key, Some(after));
            } else {
                self.failure = Some(ArmFailure::Invalid(EvidenceInvalidation::CaptureGap));
            }
        }
    }

    fn touch(&mut self, base: &Row, overlay: Option<&Row>) {
        if self.failure.is_some() {
            return;
        }
        let mut key = Vec::with_capacity(self.table.columns.len());
        let mut bytes_used = 0_usize;
        for column in &self.table.columns {
            let Some(value) = overlay
                .and_then(|row| row.get(&column.name))
                .or_else(|| base.get(&column.name))
            else {
                self.failure = Some(ArmFailure::Invalid(EvidenceInvalidation::CaptureGap));
                return;
            };
            let Some(bytes) = value.bytes.as_deref().filter(|_| !value.type_id.is_empty()) else {
                self.failure = Some(ArmFailure::Invalid(EvidenceInvalidation::CaptureGap));
                return;
            };
            bytes_used = bytes_used
                .saturating_add(bytes.len())
                .saturating_add(std::mem::size_of::<Value>());
            if bytes_used > self.limits.max_bytes {
                self.failure = Some(ArmFailure::Overflow(EvidenceLimit::EncodedBytes(
                    self.limits.max_bytes,
                )));
                return;
            }
            let Ok(value) = Value::from_canonical_bytes(bytes) else {
                self.failure = Some(ArmFailure::Invalid(EvidenceInvalidation::CaptureGap));
                return;
            };
            key.push(value);
        }
        if self.table.validate_key(&key).is_err() {
            self.failure = Some(ArmFailure::Invalid(EvidenceInvalidation::CaptureGap));
            return;
        }
        let Ok(encoded) = key_bytes(&key) else {
            self.failure = Some(ArmFailure::Invalid(EvidenceInvalidation::CaptureGap));
            return;
        };
        if self.keys.contains_key(&encoded) {
            return;
        }
        // Charge both stored representations and per-entry allocation overhead.
        let charge = bytes_used.saturating_add(encoded.len()).saturating_add(128);
        if self.keys.len() >= self.limits.max_keys {
            self.failure = Some(ArmFailure::Overflow(EvidenceLimit::Keys(
                self.limits.max_keys,
            )));
        } else if self.bytes.saturating_add(charge) > self.limits.max_bytes {
            self.failure = Some(ArmFailure::Overflow(EvidenceLimit::EncodedBytes(
                self.limits.max_bytes,
            )));
        } else {
            self.bytes += charge;
            self.keys.insert(encoded, key);
        }
        if self.failure.is_some() {
            self.keys.clear();
        }
    }
}

/// Dedicated idle SOURCE admin session; never share it with snapshot reads or
/// an explicit transaction. Its marker must be observed in this engine's real
/// capture lineage. Dropping/rearming removes the old bounded observer.
pub struct PgChangeEvidence<O> {
    hub: Arc<Hub>,
    binding: Binding,
    admin: Arc<Client>,
    table: Arc<TableKey>,
    order: O,
    limits: VerificationLimits,
    armed: Option<Uuid>,
}

impl<O> Drop for PgChangeEvidence<O> {
    fn drop(&mut self) {
        self.disarm();
    }
}

impl<O> PgChangeEvidence<O> {
    /// Idempotently abandon the current interval. A later `changes` fails
    /// closed until a new `arm`, whose unique barrier cannot reuse old work.
    pub fn disarm(&mut self) {
        if let Some(token) = self.armed.take() {
            self.hub.disarm(token);
        }
    }
}

struct DisarmOnExit {
    hub: Arc<Hub>,
    token: Uuid,
}
impl Drop for DisarmOnExit {
    fn drop(&mut self) {
        self.hub.disarm(self.token);
    }
}

/// An applied SOURCE position waiter for one run generation and lineage.
pub struct AppliedSourceWaiter {
    hub: Arc<Hub>,
    binding: Binding,
    timeout: Duration,
    admin: Arc<Client>,
}

impl Runtime {
    /// Fail closed when an external capture adapter reports an interval it
    /// cannot replay. Also provides the explicit fault-injection seam used by
    /// the real-engine verification qualification; it can only remove trust.
    pub fn invalidate_verification_capture_gap(&self) {
        self.invalidate_final_marker_capture_gap();
        self.6
            .state
            .lock()
            .expect("verification state poisoned")
            .invalidate(EvidenceInvalidation::CaptureGap);
        self.6.changed.notify_waiters();
    }

    /// Construct only while Engine::run/run_until is active. `table` and `order`
    /// must be the same catalog-qualified key/index ordering used by both SQL
    /// endpoints. `admin` is a dedicated idle connection to that SOURCE, created
    /// through pg::tls; a wrong-source marker cannot satisfy the capture barrier.
    /// These adapters carry no durable verification/readiness certification.
    pub fn pg_verification<O: KeyOrder + Send>(
        &self,
        admin: Client,
        table: TableKey,
        order: O,
        limits: VerificationLimits,
    ) -> verify::Result<(PgChangeEvidence<O>, AppliedSourceWaiter)> {
        limits.validate()?;
        table
            .validate()
            .map_err(|_| VerifyError::Invalid("unqualified verification key"))?;
        let state = self.6.state.lock().expect("verification state poisoned");
        let binding = state.binding.clone().filter(|_| state.running).ok_or(
            VerifyError::ApplierTerminal {
                cause: ApplierTerminalCause::CaptureStopped,
            },
        )?;
        state.check_binding(&binding)?;
        drop(state);
        let admin = Arc::new(admin);
        Ok((
            PgChangeEvidence {
                hub: self.6.clone(),
                binding: binding.clone(),
                admin: admin.clone(),
                table: Arc::new(table),
                order,
                limits,
                armed: None,
            },
            AppliedSourceWaiter {
                hub: self.6.clone(),
                binding,
                timeout: limits.timeout,
                admin,
            },
        ))
    }
}

#[async_trait]
impl<O: KeyOrder + Send> SqlChangeEvidence for PgChangeEvidence<O> {
    async fn arm(&mut self, range: &RangePlan) -> verify::Result<()> {
        self.disarm();
        if range.id.table != self.table.table {
            return Err(VerifyError::Invalid("verification evidence table mismatch"));
        }
        for key in [&range.lo, &range.hi].into_iter().flatten() {
            self.table
                .validate_key(key)
                .map_err(|_| VerifyError::Invalid("verification evidence bounds"))?;
        }
        if let (Some(lo), Some(hi)) = (&range.lo, &range.hi)
            && self.order.compare_keys(lo, hi)? != Ordering::Less
        {
            return Err(VerifyError::Invalid(
                "verification evidence inverted bounds",
            ));
        }
        let token = Uuid::new_v4();
        // Existing watermark codec ignores unrelated chunk/attempt identities.
        // UUID-based reserved table/plan identities cannot collide with a real
        // qualified table plan or another verifier, including other processes.
        let marker = verification_marker(token);
        let mut state = self.hub.state.lock().expect("verification state poisoned");
        state.check_binding(&self.binding)?;
        if !state.running {
            return Err(invalid(EvidenceInvalidation::CaptureGap));
        }
        if state.arms.len() >= MAX_OBSERVERS {
            return Err(VerifyError::EvidenceOverflow {
                range: range.id.clone(),
                limit: EvidenceLimit::Observers(MAX_OBSERVERS),
            });
        }
        state.arms.insert(
            token,
            Box::new(Armed {
                range: range.clone(),
                table: self.table.clone(),
                limits: self.limits,
                keys: BTreeMap::new(),
                bytes: 0,
                failure: None,
                marker,
                through: None,
            }),
        );
        self.armed = Some(token);
        Ok(())
    }

    async fn changes(
        &mut self,
        range: &RangePlan,
        source_lsn: Lsn,
    ) -> verify::Result<ChangeEvidence> {
        Box::pin(self.collect(range, source_lsn)).await
    }
}

impl<O: KeyOrder + Send> PgChangeEvidence<O> {
    async fn collect(
        &mut self,
        range: &RangePlan,
        source_lsn: Lsn,
    ) -> verify::Result<ChangeEvidence> {
        let token = self
            .armed
            .take()
            .ok_or_else(|| invalid(EvidenceInvalidation::CaptureGap))?;
        let _disarm = DisarmOnExit {
            hub: self.hub.clone(),
            token,
        };
        let started = tokio::time::Instant::now();
        let marker = {
            let state = self.hub.state.lock().expect("verification state poisoned");
            state.check_binding(&self.binding)?;
            let arm = state
                .arms
                .get(&token)
                .ok_or_else(|| invalid(EvidenceInvalidation::CaptureGap))?;
            if arm.range != *range {
                return Err(VerifyError::Invalid("verification evidence range mismatch"));
            }
            if let Some(failure) = arm.failure {
                return Err(failure.error(range));
            }
            arm.marker.clone()
        };
        let hub = self.hub.clone();
        let binding = self.binding.clone();
        let admin = self.admin.clone();
        let order = &mut self.order;
        let result = tokio::time::timeout(
            self.limits.timeout,
            Box::pin(async move {
                marker
                    .emit(&admin)
                    .await
                    .map_err(|_| VerifyError::Read("verification source barrier failed".into()))?;
                loop {
                    let notified = hub.changed.notified();
                    let complete = {
                        let mut state = hub.state.lock().expect("verification state poisoned");
                        state.check_binding(&binding)?;
                        if !state.running {
                            return Err(invalid(EvidenceInvalidation::CaptureGap));
                        }
                        let arm = state
                            .arms
                            .get_mut(&token)
                            .ok_or_else(|| invalid(EvidenceInvalidation::CaptureGap))?;
                        if let Some(failure) = arm.failure {
                            return Err(failure.error(range));
                        }
                        arm.through
                            .map(|through| (through, std::mem::take(&mut arm.keys)))
                    };
                    if let Some((through_lsn, keys)) = complete {
                        if through_lsn < source_lsn {
                            return Err(invalid(EvidenceInvalidation::CaptureGap));
                        }
                        return Ok(ChangeEvidence {
                            through_lsn,
                            keys: ordered_range_keys(&*order, range, keys.into_values().collect())?,
                        });
                    }
                    notified.await;
                }
            }),
        )
        .await;
        result.unwrap_or_else(|_| {
            Err(self
                .hub
                .state
                .lock()
                .expect("verification state poisoned")
                .evidence_timeout(&self.binding, token, range, source_lsn, started.elapsed()))
        })
    }
}

fn ordered_range_keys(
    order: &impl KeyOrder,
    range: &RangePlan,
    mut keys: Vec<Key>,
) -> verify::Result<Vec<Key>> {
    let mut failure = None;
    keys.sort_by(|a, b| match order.compare_keys(a, b) {
        Ok(order) => order,
        Err(error) => {
            failure = Some(error);
            Ordering::Equal
        }
    });
    if let Some(error) = failure {
        return Err(error);
    }
    let mut result = Vec::new();
    for key in keys {
        if range
            .lo
            .as_ref()
            .map(|lo| order.compare_keys(&key, lo))
            .transpose()?
            .is_some_and(|o| o == Ordering::Less)
            || range
                .hi
                .as_ref()
                .map(|hi| order.compare_keys(&key, hi))
                .transpose()?
                .is_some_and(|o| o != Ordering::Less)
        {
            continue;
        }
        if result
            .last()
            .map(|previous| order.compare_keys(previous, &key))
            .transpose()?
            .is_some_and(|o| o != Ordering::Less)
        {
            return Err(VerifyError::Invalid(
                "verification key order equates distinct identities",
            ));
        }
        result.push(key);
    }
    Ok(result)
}

#[async_trait]
impl verify::Applier for AppliedSourceWaiter {
    async fn wait_until(&mut self, source_lsn: Lsn) -> verify::Result<Lsn> {
        let started = tokio::time::Instant::now();
        let result = tokio::time::timeout(
            self.timeout,
            Box::pin(async {
                let mut emitted = false;
                loop {
                    let notified = self.hub.changed.notified();
                    {
                        let state = self.hub.state.lock().expect("verification state poisoned");
                        // Stop is terminal even if the last progress was sufficient.
                        if let Some(cause) = state.terminal {
                            return Err(VerifyError::ApplierTerminal { cause });
                        }
                        state.check_binding(&self.binding)?;
                        if !state.running {
                            return Err(VerifyError::ApplierTerminal {
                                cause: ApplierTerminalCause::ApplyStopped,
                            });
                        }
                        if let Some(lsn) = state.applied.filter(|lsn| *lsn >= source_lsn) {
                            return Ok(lsn);
                        }
                    }
                    // A snapshot may sample the WAL end after the latest commit
                    // position. An idle stream still needs a later committed
                    // SOURCE transaction. This marker advances nothing until
                    // its real Applied receipt is authenticated and journaled.
                    if !emitted {
                        verification_marker(Uuid::new_v4())
                            .emit(&self.admin)
                            .await
                            .map_err(|_| {
                                VerifyError::Read("verification applied barrier failed".into())
                            })?;
                        emitted = true;
                        continue;
                    }
                    notified.await;
                }
            }),
        )
        .await;
        result.unwrap_or_else(|_| {
            Err(self
                .hub
                .state
                .lock()
                .expect("verification state poisoned")
                .applier_timeout(&self.binding, source_lsn, started.elapsed()))
        })
    }
}
