//! Live, bounded observations; none of this state restores readiness on restart.
use super::*;
use crate::{
    buffer::{reader::ReaderCheckpoint, writer::WriterCheckpoint},
    event::Position,
    pg::{chunk::Watermark, replication::Lineage},
};
use std::{sync::Mutex as StdMutex, time::Instant};

pub const MAX_OBSERVATION_AGE: Duration = Duration::from_secs(1);
const MAX_PENDING: usize = 65_536;

/// Positions are bytes; the readiness comparison uses age in milliseconds.
#[derive(Clone, Debug)]
pub struct ApplyLagObservation {
    pub source: SourceIdentity,
    pub order_domain: String,
    pub source_tip_lsn: u64,
    pub applied_source_lsn: u64,
    pub lag_bytes: u64,
    /// Captured PostgreSQL commit timestamp of the oldest unapplied transaction,
    /// measured against the sampled source clock plus monotonic elapsed time.
    pub oldest_unapplied_age_ms: u64,
    pub observed_at: Instant,
}
impl ApplyLagObservation {
    pub fn fresh(&self) -> bool {
        Instant::now()
            .checked_duration_since(self.observed_at)
            .is_some_and(|age| age <= MAX_OBSERVATION_AGE)
    }
    /// Comparable milliseconds for the coordinator. The budget input is
    /// explicitly seconds; stale observations always return a failing pair.
    pub fn compare_budget(&self, budget_seconds: u64) -> (u64, u64) {
        if self.fresh() {
            (
                self.oldest_unapplied_age_ms,
                budget_seconds.saturating_mul(1000),
            )
        } else {
            (1, 0)
        }
    }
    pub fn within_budget(&self, definition: &crate::MigrationDefinition) -> bool {
        let (age, budget) = self.compare_budget(
            definition
                .readiness_policy
                .max_lag_seconds
                .min(definition.budgets.max_replication_lag_seconds),
        );
        age <= budget
    }
    pub(super) fn matches(&self, checkpoint: &crate::pg::replication::Checkpoint) -> bool {
        source_matches(&self.source, &checkpoint.lineage)
            && self.source.slot == checkpoint.slot
            && !self.order_domain.is_empty()
            && self.source_tip_lsn > 0
            && self.applied_source_lsn > 0
            && self.lag_bytes == self.source_tip_lsn.saturating_sub(self.applied_source_lsn)
    }
}

#[derive(Clone, Debug)]
pub struct LiveReadinessObservation {
    pub owner_generation: Option<String>,
    pub capture: Option<CaptureObservation>,
    pub lag: Option<ApplyLagObservation>,
    pub schema: Option<source::SchemaState>,
    pub running: bool,
    pub schema_fresh: bool,
}

struct Tip {
    lineage: Lineage,
    lsn: u64,
    source_time_us: i64,
    started: Instant,
    marker: Watermark,
}
#[derive(Default)]
struct Live {
    owner_generation: Option<String>,
    source: Option<SourceIdentity>,
    domain: String,
    running: bool,
    valid: bool,
    baseline: Option<u64>,
    applied: Option<u64>,
    last_captured: Option<u64>,
    pending: VecDeque<(u64, i64)>,
    tip: Option<Tip>,
    awaiting: Option<Tip>,
    schema: Option<source::SchemaState>,
    schema_at: Option<Instant>,
}
#[derive(Default)]
pub(super) struct Hub(StdMutex<Live>);

fn lsn(position: &Position, domain: &str) -> Option<u64> {
    (position.order_domain() == domain).then(|| {
        <[u8; 8]>::try_from(position.bytes())
            .ok()
            .map(u64::from_be_bytes)
    })?
}
fn source_matches(source: &SourceIdentity, lineage: &Lineage) -> bool {
    source.engine == "postgres"
        && source.system_id == lineage.system_id.to_string()
        && source.lineage == lineage.timeline.to_string()
        && source.db_name == lineage.database
}
impl Hub {
    pub(super) fn owner_generation(&self, generation: Option<String>) {
        self.0
            .lock()
            .expect("live readiness poisoned")
            .owner_generation = generation;
    }
    pub(super) fn begin(
        &self,
        source: SourceIdentity,
        domain: String,
        writer: Option<WriterCheckpoint>,
        applied: Option<ReaderCheckpoint>,
    ) {
        let baseline = writer
            .as_ref()
            .and_then(|w| w.last_position.as_ref())
            .and_then(|p| lsn(p, &domain));
        let initial = applied
            .as_ref()
            .filter(|c| {
                c.last_record
                    .as_ref()
                    .is_some_and(|r| r.source == source && !r.chunk_rows)
                    && c.next_mseq > 0
            })
            .and_then(|c| c.last_position.as_ref())
            .and_then(|p| lsn(p, &domain));
        let valid = !domain.is_empty()
            && writer.as_ref().is_some_and(|w| {
                if w.next_mseq == 0 {
                    w.last_position.is_none() && w.last_record.is_none()
                } else {
                    baseline.is_some() && w.last_record.as_ref().is_some_and(|r| r.source == source)
                }
            });
        *self.0.lock().expect("live readiness poisoned") = Live {
            source: Some(source),
            domain,
            running: true,
            valid,
            baseline,
            applied: initial,
            ..Live::default()
        };
    }
    pub(super) fn finish(&self) {
        let mut state = self.0.lock().expect("live readiness poisoned");
        state.running = false;
        state.tip = None;
        state.awaiting = None;
    }
    pub(super) fn schema(&self, schema: Option<source::SchemaState>) {
        let mut state = self.0.lock().expect("live readiness poisoned");
        state.schema = schema;
        state.schema_at = Some(Instant::now());
    }
    pub(super) fn observe(&self, batch: &Captured) {
        let mut state = self.0.lock().expect("live readiness poisoned");
        let Some(txn) = &batch.transaction else {
            return;
        };
        let position = lsn(&txn.commit_position, &state.domain);
        if !state.running
            || state.source.as_ref() != Some(&txn.source)
            || position.is_none()
            || txn.commit_time.is_none()
            || batch
                .durable_lsn
                .is_none_or(|end| position.is_none_or(|position| end < position))
            || position < state.last_captured
        {
            state.valid = false;
            return;
        }
        let position = position.expect("validated position");
        state.last_captured = Some(position);
        if state.applied.is_none_or(|applied| position > applied) {
            if state.pending.len() == MAX_PENDING {
                state.valid = false;
                return;
            }
            if state
                .pending
                .back()
                .is_none_or(|(last, _)| *last != position)
            {
                state
                    .pending
                    .push_back((position, txn.commit_time.expect("validated timestamp")));
            }
        }
        if let Some(tip) = state.awaiting.as_ref()
            && position >= tip.lsn
            && batch.watermarks.iter().any(|w| {
                w.transactional
                    && w.commit_lsn == position
                    && Watermark::parse(&w.message()).is_ok_and(|m| m == tip.marker)
            })
        {
            state.tip = state.awaiting.take();
        }
    }
    pub(super) fn applied(&self, position: &Position, checkpoint: &ReaderCheckpoint) {
        let mut state = self.0.lock().expect("live readiness poisoned");
        let Some(record) = &checkpoint.last_record else {
            state.valid = false;
            return;
        };
        let next = lsn(position, &state.domain);
        if state.source.as_ref() != Some(&record.source)
            || checkpoint.last_position.as_ref() != Some(position)
            || checkpoint.next_mseq == 0
            || next.is_none()
        {
            state.valid = false;
            return;
        }
        if record.chunk_rows {
            return;
        }
        let next = next.expect("validated applied position");
        if state.applied.is_some_and(|old| next < old) {
            state.valid = false;
            return;
        }
        state.applied = Some(next);
        while state
            .pending
            .front()
            .is_some_and(|(position, _)| *position <= next)
        {
            state.pending.pop_front();
        }
    }
    pub(super) fn source_tip(
        &self,
        lineage: &Lineage,
        lsn: u64,
        source_time_us: i64,
        started: Instant,
        marker: Watermark,
    ) -> Result<()> {
        let mut state = self.0.lock().expect("live readiness poisoned");
        if !state.running
            || !state.valid
            || started > Instant::now()
            || lsn == 0
            || !state
                .source
                .as_ref()
                .is_some_and(|s| source_matches(s, lineage))
        {
            state.tip = None;
            state.awaiting = None;
            return Err(recovery("live source-tip lineage or lifecycle mismatch"));
        }
        state.awaiting = Some(Tip {
            lineage: lineage.clone(),
            lsn,
            source_time_us,
            started,
            marker,
        });
        Ok(())
    }
    fn revoke_tip(&self) {
        let mut state = self.0.lock().expect("live readiness poisoned");
        state.tip = None;
        state.awaiting = None;
    }
    fn sample(&self) -> LiveReadinessObservation {
        let state = self.0.lock().expect("live readiness poisoned");
        let lag = (|| {
            let tip = state.tip.as_ref()?;
            let applied = state.applied?;
            if !state.running
                || !state.valid
                || tip.started.elapsed() > MAX_OBSERVATION_AGE
                || state.baseline.is_some_and(|baseline| applied < baseline)
            {
                return None;
            }
            let elapsed_us = i64::try_from(tip.started.elapsed().as_micros()).ok()?;
            let source_now = tip.source_time_us.checked_add(elapsed_us)?;
            let age_ms = match state.pending.front() {
                Some((_, committed)) => u64::try_from(source_now.checked_sub(*committed)?)
                    .ok()?
                    .div_ceil(1000),
                None => 0,
            };
            Some(ApplyLagObservation {
                source: state.source.clone()?,
                order_domain: state.domain.clone(),
                source_tip_lsn: tip.lsn,
                applied_source_lsn: applied,
                lag_bytes: tip.lsn.saturating_sub(applied),
                oldest_unapplied_age_ms: age_ms,
                observed_at: tip.started,
            })
        })();
        LiveReadinessObservation {
            owner_generation: state.owner_generation.clone(),
            capture: state
                .tip
                .as_ref()
                .filter(|tip| {
                    state.running && state.valid && tip.started.elapsed() <= MAX_OBSERVATION_AGE
                })
                .and_then(|tip| {
                    Some(CaptureObservation {
                        healthy: true,
                        checkpoint: crate::pg::replication::Checkpoint {
                            lineage: tip.lineage.clone(),
                            slot: state.source.as_ref()?.slot.clone(),
                            durable_lsn: tip.lsn,
                        },
                    })
                }),
            lag,
            schema: state.schema.clone(),
            running: state.running,
            schema_fresh: state
                .schema_at
                .is_some_and(|at| at.elapsed() <= MAX_OBSERVATION_AGE),
        }
    }
}
impl Runtime {
    pub fn live_readiness(&self) -> LiveReadinessObservation {
        self.7.sample()
    }

    /// Dedicated source admin session. The live IDENTIFY_SYSTEM lineage must
    /// come from the same configured endpoint. A captured transactional marker
    /// proves coverage of this sampled tip, including otherwise idle sources.
    /// Caller starts the freshness clock before connection/query awaits.
    pub async fn probe_readiness(
        &self,
        admin: &tokio_postgres::Client,
        lineage: &Lineage,
        started: Instant,
    ) -> Result<()> {
        let result = async {
            let row = admin.query_one("SELECT pg_current_wal_lsn()::text, (extract(epoch FROM clock_timestamp()) * 1000000)::bigint", &[]).await.map_err(recovery)?;
            let tip = crate::pg::replication::parse_lsn(&row.get::<_, String>(0)).map_err(recovery)?;
            let marker = super::verification::verification_marker(uuid::Uuid::new_v4());
            self.7.source_tip(lineage, tip, row.get(1), started, marker.clone())?;
            marker.emit(admin).await.map_err(recovery)?;
            Ok(())
        }.await;
        if result.is_err() {
            self.7.revoke_tip();
        }
        result
    }
    pub fn revoke_readiness_probe(&self) {
        self.7.revoke_tip();
    }
}

#[cfg(test)]
mod bounds {
    use super::*;
    #[test]
    fn bounded_queue_overflow_and_unknown_writer_fail_closed() {
        let source = SourceIdentity {
            engine: "postgres".into(),
            system_id: "1".into(),
            lineage: "1".into(),
            db_name: "db".into(),
            slot: "slot".into(),
        };
        let hub = Hub::default();
        hub.begin(source.clone(), "pg".into(), None, None);
        assert!(!hub.0.lock().unwrap().valid);
        hub.begin(
            source.clone(),
            "pg".into(),
            Some(WriterCheckpoint::default()),
            None,
        );
        hub.0.lock().unwrap().pending.resize(MAX_PENDING, (1, 0));
        let txn = AssembledTxn::new(
            source,
            crate::event::TxnId(vec![1]),
            Position::new("pg", 2u64.to_be_bytes()),
            Some(0),
            vec![],
        )
        .unwrap();
        hub.observe(&Captured {
            transaction: Some(txn),
            durable_lsn: Some(3),
            ..Captured::default()
        });
        let state = hub.0.lock().unwrap();
        assert!(!state.valid);
        assert_eq!(state.pending.len(), MAX_PENDING);
        assert!(std::mem::size_of::<Runtime>() <= 8 * std::mem::size_of::<usize>());
    }
}
