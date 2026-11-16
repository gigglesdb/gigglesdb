//! Pure MySQL verification providers. Capture/marker and v3 checkpoint wiring
//! must authenticate the inputs; status samples are never coverage evidence.
use std::{cmp::Ordering, collections::BTreeSet, sync::Arc, time::Duration};

use async_trait::async_trait;
use tokio::time::{Instant, sleep, timeout};

use crate::verify::{
    Applier, ChangeEvidence, EvidenceInvalidation, EvidenceLimit, Key, KeyOrder, RangePlan, Result,
    SourcePositionOrder, VerifyError,
    compare::{compare_key, validate_range},
    compare_positions,
    sql::SqlChangeEvidence,
};
use crate::{
    event::Position,
    mysql::{MysqlOrder, MysqlPosition},
};

/// Mapped key images, never inferred by comparing current row values. `table`
/// uses the exact verification RangePlan table identity. Inserts have only
/// `after`, deletes only `before`; updates must retain BOTH even when equal.
#[derive(Clone, Debug)]
pub struct KeyChange {
    pub table: String,
    pub before: Option<Key>,
    pub after: Option<Key>,
}

#[derive(Clone, Copy, Debug)]
pub enum ContinuityEvent {
    Reconnect,
    Gap,
    Purge,
    CaptureStop,
}

#[derive(Clone, Debug)]
pub enum EvidenceEvent {
    /// Complete transaction in commit order, including changes to rows that
    /// were reverted within that transaction. Empty transactions/markers count.
    Commit {
        position: Position,
        changes: Vec<KeyChange>,
    },
    Continuity(ContinuityEvent),
}

/// VM2-2 implements this seam from MysqlCapture. Arm must atomically subscribe
/// BEFORE returning its baseline and retain every later commit, including the
/// full changes of transactions already in flight. Never drop/overwrite queued
/// events: overflow, reconnect, purge, stop and gaps must be reported as errors
/// or Continuity events. Re-arming replaces the subscription atomically.
#[async_trait]
pub trait MysqlEvidenceInput: Send {
    async fn arm(&mut self, range: &RangePlan) -> Result<Position>;
    /// Called after destination read. Emit a fresh, attempt-bound marker and
    /// return its authenticated captured commit position (not a status sample).
    /// Its Commit event must also occur in next(), after all preceding events.
    /// This must not block delivery to the subscription while observing it.
    async fn boundary(&mut self) -> Result<Position>;
    async fn next(&mut self) -> Result<EvidenceEvent>;
}

pub struct MysqlChangeEvidence<I, O> {
    input: I,
    order: Arc<MysqlOrder>,
    keys: O,
    timeout: Duration,
    max_keys: usize,
    armed: Option<(RangePlan, Position)>,
}
impl<I, O> MysqlChangeEvidence<I, O> {
    pub(super) fn input(&self) -> &I {
        &self.input
    }
    pub(super) fn armed_position(&self) -> Option<&Position> {
        self.armed.as_ref().map(|(_, p)| p)
    }
    pub fn new(
        input: I,
        order: MysqlOrder,
        keys: O,
        timeout: Duration,
        max_keys: usize,
    ) -> Result<Self> {
        if timeout.is_zero() || max_keys == 0 {
            return Err(VerifyError::Invalid("MySQL evidence limits"));
        }
        Ok(Self {
            input,
            order: Arc::new(order),
            keys,
            timeout,
            max_keys,
            armed: None,
        })
    }
}

fn gap() -> VerifyError {
    VerifyError::EvidenceInvalidated {
        reason: EvidenceInvalidation::CaptureGap,
    }
}

/// MysqlOrder allows GTID subset ordering across source UUID sets. Verification
/// deliberately pins that set as well: a new source UUID needs a fresh session.
pub(super) fn comparable(
    order: &MysqlOrder,
    left: &Position,
    right: &Position,
) -> Result<Ordering> {
    let result = compare_positions(order, left, right)?;
    if let (MysqlPosition::Gtid { set: a, .. }, MysqlPosition::Gtid { set: b, .. }) = (
        MysqlPosition::decode(left.bytes()).map_err(VerifyError::Position)?,
        MysqlPosition::decode(right.bytes()).map_err(VerifyError::Position)?,
    ) {
        let sources = |text: String| {
            text.split(',')
                .filter(|s| !s.is_empty())
                .map(|s| s.split(':').next().unwrap().to_owned())
                .collect::<BTreeSet<_>>()
        };
        if sources(a.to_string()) != sources(b.to_string()) {
            return Err(VerifyError::Position(
                crate::event::EventError::PositionDomainMismatch,
            ));
        }
    }
    Ok(result)
}

#[async_trait]
impl<I: MysqlEvidenceInput, O: KeyOrder + Send> SqlChangeEvidence for MysqlChangeEvidence<I, O> {
    fn position_order(&self) -> SourcePositionOrder {
        self.order.clone()
    }

    async fn arm(&mut self, range: &RangePlan) -> Result<()> {
        self.armed = None;
        validate_range(&self.keys, range)?;
        let start = timeout(self.timeout, self.input.arm(range))
            .await
            .map_err(|_| gap())??;
        comparable(&self.order, &start, &start)?;
        self.armed = Some((range.clone(), start));
        Ok(())
    }

    async fn changes(&mut self, range: &RangePlan, snapshot: Position) -> Result<ChangeEvidence> {
        // Consume before awaiting: cancellation/error cannot reuse partial evidence.
        let (armed_range, mut previous) = self.armed.take().ok_or_else(gap)?;
        if armed_range != *range {
            return Err(VerifyError::Invalid("MySQL evidence range mismatch"));
        }
        comparable(&self.order, &snapshot, &previous)?;
        let input = &mut self.input;
        let key_order = &mut self.keys;
        let order = &self.order;
        let max_keys = self.max_keys;
        timeout(self.timeout, async move {
            let boundary = input.boundary().await?;
            if comparable(order, &boundary, &snapshot)? == Ordering::Less
                || comparable(order, &boundary, &previous)? != Ordering::Greater
            {
                return Err(gap());
            }
            let mut keys: Vec<Key> = Vec::new();
            loop {
                let (position, changes) = match input.next().await? {
                    EvidenceEvent::Continuity(ContinuityEvent::Reconnect) => {
                        return Err(VerifyError::EvidenceInvalidated {
                            reason: EvidenceInvalidation::Reconnected,
                        });
                    }
                    EvidenceEvent::Continuity(_) => return Err(gap()),
                    EvidenceEvent::Commit { position, changes } => (position, changes),
                };
                if comparable(order, &position, &previous)? != Ordering::Greater {
                    return Err(gap());
                }
                let end = comparable(order, &position, &boundary)?;
                // The authenticated marker itself must appear; jumping over it
                // is not proof of complete capture.
                if end == Ordering::Greater {
                    return Err(gap());
                }
                for change in changes {
                    if change.before.is_none() && change.after.is_none() {
                        return Err(gap());
                    }
                    if change.table != range.id.table {
                        continue;
                    }
                    for key in [change.before, change.after].into_iter().flatten() {
                        // Validate even an otherwise out-of-range key image.
                        compare_key(&*key_order, &key, &key)?;
                        if let Some(lo) = &range.lo
                            && compare_key(&*key_order, &key, lo)? == Ordering::Less
                        {
                            continue;
                        }
                        if let Some(hi) = &range.hi
                            && compare_key(&*key_order, &key, hi)? != Ordering::Less
                        {
                            continue;
                        }
                        let (mut index, mut hi) = (0, keys.len());
                        while index < hi {
                            let mid = index + (hi - index) / 2;
                            if compare_key(&*key_order, &keys[mid], &key)? == Ordering::Less {
                                index = mid + 1;
                            } else {
                                hi = mid;
                            }
                        }
                        if index < keys.len()
                            && compare_key(&*key_order, &keys[index], &key)? == Ordering::Equal
                        {
                            continue;
                        }
                        if keys.len() == max_keys {
                            return Err(VerifyError::EvidenceOverflow {
                                range: range.id.clone(),
                                limit: EvidenceLimit::Keys(max_keys),
                            });
                        }
                        keys.insert(index, key);
                    }
                }
                if end == Ordering::Equal {
                    return Ok(ChangeEvidence {
                        through_position: position,
                        keys,
                    });
                }
                previous = position;
                // A buffered input may be immediately ready indefinitely.
                // Cooperate so the outer deadline and cancellation still run.
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|_| gap())?
    }
}

/// Return ONLY authenticated, journaled Opaque-v3 applied native source progress.
/// None means no applied checkpoint yet. A destination WAL or capture read
/// position cannot satisfy this contract. Decode failures must return an error.
#[async_trait]
pub trait MysqlAppliedCheckpoint: Send {
    async fn applied_position(&mut self) -> Result<Option<Position>>;
}

pub struct MysqlAppliedWaiter<C> {
    checkpoint: C,
    order: MysqlOrder,
    timeout: Duration,
    poll_interval: Duration,
}
impl<C> MysqlAppliedWaiter<C> {
    pub fn new(
        checkpoint: C,
        order: MysqlOrder,
        timeout: Duration,
        poll_interval: Duration,
    ) -> Result<Self> {
        if timeout.is_zero() || poll_interval.is_zero() {
            return Err(VerifyError::Invalid("MySQL applied wait limits"));
        }
        Ok(Self {
            checkpoint,
            order,
            timeout,
            poll_interval,
        })
    }
}
#[async_trait]
impl<C: MysqlAppliedCheckpoint> Applier for MysqlAppliedWaiter<C> {
    async fn wait_until(&mut self, boundary: Position) -> Result<Position> {
        comparable(&self.order, &boundary, &boundary)?;
        let started = Instant::now();
        timeout(self.timeout, async {
            let mut previous = None;
            loop {
                if let Some(applied) = self.checkpoint.applied_position().await? {
                    if let Some(previous) = &previous
                        && comparable(&self.order, &applied, previous)? == Ordering::Less
                    {
                        return Err(VerifyError::Invalid("MySQL applied checkpoint regressed"));
                    }
                    if comparable(&self.order, &applied, &boundary)? != Ordering::Less {
                        return Ok(applied);
                    }
                    previous = Some(applied);
                }
                sleep(self.poll_interval).await;
            }
        })
        .await
        .map_err(|_| VerifyError::ApplierTimeout {
            waited_position: boundary,
            elapsed: started.elapsed(),
        })?
    }
}

#[cfg(test)]
#[path = "evidence_tests.rs"]
mod tests;
