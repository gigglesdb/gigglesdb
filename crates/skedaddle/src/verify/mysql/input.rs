//! Bounded subscription to complete mapped capture transactions. Drive capture
//! concurrently with boundary/next; marker observation never consumes the queue.
use super::evidence::{ContinuityEvent, EvidenceEvent, KeyChange, MysqlEvidenceInput};
use crate::{
    event::{Event, Position, Row},
    mysql::{
        MysqlOrder,
        client::MysqlConfig,
        control::{self, VerifyMarker},
    },
    verify::{EvidenceInvalidation, EvidenceLimit, RangePlan, Result, VerifyError},
};

pub(crate) struct CaptureEvidenceGuard(pub Arc<CaptureEvidence>);
impl std::ops::Deref for CaptureEvidenceGuard {
    type Target = CaptureEvidence;
    fn deref(&self) -> &CaptureEvidence {
        &self.0
    }
}
impl Drop for CaptureEvidenceGuard {
    fn drop(&mut self) {
        self.invalidate(ContinuityEvent::CaptureStop);
    }
}
use async_trait::async_trait;
use std::{
    cmp::Ordering,
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex},
};
use tokio::sync::Notify;

fn gap() -> VerifyError {
    VerifyError::EvidenceInvalidated {
        reason: EvidenceInvalidation::CaptureGap,
    }
}

struct Armed {
    range: RangePlan,
    marker: VerifyMarker,
    emitted: bool,
    boundary: Option<Position>,
    queue: VecDeque<EvidenceEvent>,
    bytes: usize,
    max_bytes: usize,
    max_commits: usize,
    failure: Option<Failure>,
}
#[derive(Clone, Copy)]
enum Failure {
    Continuity(ContinuityEvent),
    Overflow,
}
struct State {
    last: Position,
    trusted: bool,
    armed: Option<Armed>,
}
pub(crate) struct CaptureEvidence {
    state: Mutex<State>,
    notify: Notify,
    order: Arc<MysqlOrder>,
    keys: BTreeMap<String, Vec<String>>,
}
impl CaptureEvidence {
    pub(crate) fn new(
        last: Position,
        order: Arc<MysqlOrder>,
        keys: BTreeMap<String, Vec<String>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                last,
                trusted: true,
                armed: None,
            }),
            notify: Notify::new(),
            order,
            keys,
        })
    }
    pub(crate) fn invalidate(&self, reason: ContinuityEvent) {
        let mut state = self.state.lock().unwrap();
        state.trusted = false;
        if let Some(armed) = &mut state.armed {
            armed.failure.get_or_insert(Failure::Continuity(reason));
        }
        self.notify.notify_one();
    }
    pub(crate) fn commit(&self, position: &Position, events: &[Event], markers: &[VerifyMarker]) {
        let mut state = self.state.lock().unwrap();
        if super::evidence::comparable(&self.order, position, &state.last).ok()
            != Some(Ordering::Greater)
        {
            drop(state);
            self.invalidate(ContinuityEvent::Gap);
            return;
        }
        state.last = position.clone();
        let Some(a) = &mut state.armed else {
            return;
        };
        if a.failure.is_some() {
            return;
        }
        let changes: Result<Vec<KeyChange>> = events
            .iter()
            .map(|e| {
                let table = e.table.as_ref().ok_or_else(gap)?;
                let columns = self.keys.get(table).ok_or_else(gap)?;
                let key = |row: &Row| {
                    columns
                        .iter()
                        .map(|c| {
                            let v = row.get(c).ok_or_else(gap)?;
                            v.bytes
                                .as_deref()
                                .map(crate::mapping::Value::from_canonical_bytes)
                                .transpose()
                                .map(|v| v.unwrap_or(crate::mapping::Value::Null))
                                .map_err(VerifyError::Mapping)
                        })
                        .collect()
                };
                let before = e.before.as_ref().map(key).transpose()?;
                let after = e.after.as_ref().map(key).transpose()?;
                if before.is_none() && after.is_none() {
                    return Err(gap());
                }
                Ok(KeyChange {
                    table: table.clone(),
                    before,
                    after,
                })
            })
            .collect();
        let Ok(changes) = changes else {
            a.failure = Some(Failure::Continuity(ContinuityEvent::Gap));
            self.notify.notify_one();
            return;
        };
        let size = changes.iter().try_fold(position.bytes().len(), |total, c| {
            c.before
                .iter()
                .chain(c.after.iter())
                .flatten()
                .try_fold(total.saturating_add(c.table.len()), |n, v| {
                    v.canonical_bytes().map(|b| n.saturating_add(b.len()))
                })
        });
        match size {
            Ok(size)
                if a.queue.len() < a.max_commits && size <= a.max_bytes.saturating_sub(a.bytes) =>
            {
                a.bytes += size;
                a.queue.push_back(EvidenceEvent::Commit {
                    position: position.clone(),
                    changes,
                });
                for marker in markers {
                    if marker.verify == a.marker.verify {
                        if marker != &a.marker || a.boundary.is_some() {
                            a.failure = Some(Failure::Continuity(ContinuityEvent::Gap));
                        } else {
                            a.boundary = Some(position.clone());
                        }
                    }
                }
            }
            _ => a.failure = Some(Failure::Overflow),
        }
        self.notify.notify_one();
    }
}

/// One input per capture. Creating another input is rejected. A continuity loss
/// requires a fresh capture session; re-arm cannot certify replayed coverage.
pub struct MysqlCaptureInput {
    pub(crate) source: crate::event::SourceIdentity,
    pub(crate) capture: Arc<CaptureEvidence>,
    pub(crate) config: MysqlConfig,
    pub(crate) id: String,
    pub(crate) attempt: u64,
    pub(crate) max_bytes: usize,
    pub(crate) max_commits: usize,
}
fn check(a: &Armed) -> Result<()> {
    match a.failure {
        Some(Failure::Overflow) => Err(VerifyError::EvidenceOverflow {
            range: a.range.id.clone(),
            limit: EvidenceLimit::EncodedBytes(a.max_bytes),
        }),
        Some(Failure::Continuity(ContinuityEvent::Reconnect)) => {
            Err(VerifyError::EvidenceInvalidated {
                reason: EvidenceInvalidation::Reconnected,
            })
        }
        Some(Failure::Continuity(_)) => Err(gap()),
        None => Ok(()),
    }
}
#[async_trait]
impl MysqlEvidenceInput for MysqlCaptureInput {
    async fn arm(&mut self, range: &RangePlan) -> Result<Position> {
        self.attempt = self.attempt.checked_add(1).ok_or_else(gap)?;
        let mut s = self.capture.state.lock().unwrap();
        s.armed = None;
        if !s.trusted || !self.capture.keys.contains_key(&range.id.table) {
            return Err(gap());
        }
        s.armed = Some(Armed {
            range: range.clone(),
            marker: VerifyMarker::new(self.id.clone(), self.attempt).map_err(|_| gap())?,
            emitted: false,
            boundary: None,
            queue: VecDeque::new(),
            bytes: 0,
            max_bytes: self.max_bytes,
            max_commits: self.max_commits,
            failure: None,
        });
        Ok(s.last.clone())
    }
    async fn boundary(&mut self) -> Result<Position> {
        let marker = {
            let mut s = self.capture.state.lock().unwrap();
            let a = s.armed.as_mut().ok_or_else(gap)?;
            check(a)?;
            if a.emitted {
                return Err(gap());
            }
            a.emitted = true;
            a.marker.clone()
        };
        // A new dedicated connection cannot have an active user transaction.
        let mut client = self.config.connect().await.map_err(|_| gap())?;
        control::emit_verify_marker(&mut client.conn, &marker)
            .await
            .map_err(|_| gap())?;
        loop {
            let notified = self.capture.notify.notified();
            {
                let s = self.capture.state.lock().unwrap();
                let a = s.armed.as_ref().ok_or_else(gap)?;
                check(a)?;
                if let Some(p) = &a.boundary {
                    return Ok(p.clone());
                }
            }
            notified.await;
        }
    }
    async fn next(&mut self) -> Result<EvidenceEvent> {
        loop {
            let notified = self.capture.notify.notified();
            {
                let mut s = self.capture.state.lock().unwrap();
                let a = s.armed.as_mut().ok_or_else(gap)?;
                check(a)?;
                if let Some(event) = a.queue.pop_front() {
                    // bytes is cumulative over the armed interval, deliberately
                    // conservative even when the consumer drains the queue.
                    return Ok(event);
                }
            }
            notified.await;
        }
    }
}

#[cfg(test)]
mod tests;
