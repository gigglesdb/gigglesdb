//! Live, bounded observations; none of this state restores readiness on restart.
use super::*;
use crate::{
    buffer::{reader::ReaderCheckpoint, writer::WriterCheckpoint},
    event::{EventError, Position},
    pg::{chunk::Watermark, replication::Lineage},
};
use std::{sync::Mutex as StdMutex, time::Instant};

pub const MAX_OBSERVATION_AGE: Duration = Duration::from_secs(1);
const MAX_PENDING: usize = 65_536;

/// Units are connector supplied; byte/transaction distances never imply age.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LagMeasure {
    pub value: u64,
    pub unit: &'static str,
}
#[derive(Clone, Debug)]
pub struct RetentionMargin {
    pub position: Position,
    pub remaining: LagMeasure,
    pub history_available: bool,
    /// Connector projection in time units; None cannot establish safe retention.
    pub time_to_exhaustion: Option<Duration>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReadinessBlocked {
    MissingProbe,
    UnknownLag,
    UnknownRetention,
    RetentionLost,
    RetentionRisk(WalLevel),
    Stale,
    Unavailable,
    Position(EventError),
}
/// A connector supplies an authenticated source tip/clock and its native units.
/// The tip is admitted only after its transactional marker is durably captured.
/// None means unknown, including a pending transaction without age evidence.
pub trait ReadinessProbe: Send + Sync {
    fn source(&self) -> &SourceIdentity;
    fn tip(&self) -> &Position;
    fn observed_at(&self) -> Instant;
    fn sample(
        &self,
        applied: &Position,
        oldest_commit: Option<Option<i64>>,
    ) -> std::result::Result<ReadinessProbeSample, EventError>;
}
pub struct ReadinessProbeSample {
    pub lag: Option<LagMeasure>,
    pub oldest_unapplied_age_ms: Option<u64>,
    pub retention: Option<RetentionMargin>,
    pub schema: Option<source::SchemaState>,
    pub observed_at: Instant,
}
#[derive(Clone)]
pub struct ApplyLagObservation {
    pub source: SourceIdentity,
    pub order: Arc<dyn PositionOrder + Send + Sync>,
    pub source_tip: Position,
    pub applied_source: Position,
    pub lag: LagMeasure,
    pub oldest_unapplied_age_ms: u64,
    pub observed_at: Instant,
}
impl std::fmt::Debug for ApplyLagObservation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApplyLagObservation")
            .field("source", &self.source)
            .field("source_tip", &self.source_tip)
            .field("applied_source", &self.applied_source)
            .field("lag", &self.lag)
            .field("oldest_unapplied_age_ms", &self.oldest_unapplied_age_ms)
            .field("observed_at", &self.observed_at)
            .finish()
    }
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
    pub(super) fn matches(&self, capture: &CaptureObservation) -> bool {
        self.source == capture.source
            && !self.order.domain().is_empty()
            && (self.source.engine != "postgres" || PgReadinessProbe::matches_lag(self))
            && self
                .source_tip
                .compare(&capture.position, self.order.as_ref())
                .is_ok()
            && self
                .applied_source
                .compare(&self.source_tip, self.order.as_ref())
                .is_ok()
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
    pub blocked: Option<ReadinessBlocked>,
    pub retention: Option<RetentionMargin>,
}

struct Tip {
    probe: Arc<dyn ReadinessProbe>,
    marker: Watermark,
}
#[derive(Default)]
struct Live {
    owner_generation: Option<String>,
    source: Option<SourceIdentity>,
    order: Option<Arc<dyn PositionOrder + Send + Sync>>,
    running: bool,
    valid: bool,
    invalid_position: Option<EventError>,
    baseline: Option<Position>,
    applied: Option<Position>,
    last_captured: Option<Position>,
    pending: VecDeque<(Position, Option<i64>)>,
    tip: Option<Tip>,
    awaiting: Option<Tip>,
    schema: Option<source::SchemaState>,
    schema_at: Option<Instant>,
}
#[derive(Default)]
pub(super) struct Hub(StdMutex<Live>);

pub(super) fn source_matches(source: &SourceIdentity, lineage: &Lineage) -> bool {
    source.engine == "postgres"
        && source.system_id == lineage.system_id.to_string()
        && source.lineage == lineage.timeline.to_string()
        && source.db_name == lineage.database
}
fn compare(
    state: &Live,
    left: &Position,
    right: &Position,
) -> std::result::Result<std::cmp::Ordering, EventError> {
    left.compare(
        right,
        state
            .order
            .as_ref()
            .ok_or(EventError::PositionDomainMismatch)?
            .as_ref(),
    )
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
        order: Arc<dyn PositionOrder + Send + Sync>,
        writer: Option<WriterCheckpoint>,
        applied: Option<ReaderCheckpoint>,
    ) {
        let baseline = writer
            .as_ref()
            .and_then(|w| w.last_position.as_ref())
            .filter(|p| p.compare(p, order.as_ref()).is_ok())
            .cloned();
        let initial = applied
            .as_ref()
            .filter(|c| {
                c.last_record
                    .as_ref()
                    .is_some_and(|r| r.source == source && !r.chunk_rows)
                    && c.next_mseq > 0
            })
            .and_then(|c| c.last_position.as_ref())
            .filter(|p| p.compare(p, order.as_ref()).is_ok())
            .cloned();
        let valid = !order.domain().is_empty()
            && writer.as_ref().is_some_and(|w| {
                if w.next_mseq == 0 {
                    w.last_position.is_none() && w.last_record.is_none()
                } else {
                    baseline.is_some() && w.last_record.as_ref().is_some_and(|r| r.source == source)
                }
            });
        *self.0.lock().expect("live readiness poisoned") = Live {
            source: Some(source),
            order: Some(order),
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
        let position = &txn.commit_position;
        let positions = compare(&state, position, position)
            .and_then(|_| match &batch.durable_position {
                Some(end) => compare(&state, end, position),
                None => Ok(std::cmp::Ordering::Equal),
            })
            .and_then(|_| match &state.last_captured {
                Some(last) => compare(&state, position, last),
                None => Ok(std::cmp::Ordering::Equal),
            });
        if let Err(error) = positions {
            state.invalid_position = Some(error);
            state.valid = false;
            return;
        }
        if !state.running
            || state.source.as_ref() != Some(&txn.source)
            || compare(&state, position, position).is_err()
            || (txn.source.engine == "postgres" && txn.commit_time.is_none())
            || batch
                .durable_position
                .as_ref()
                .is_none_or(|end| compare(&state, end, position).map_or(true, |o| o.is_lt()))
            || state
                .last_captured
                .as_ref()
                .is_some_and(|last| compare(&state, position, last).map_or(true, |o| o.is_lt()))
        {
            state.valid = false;
            return;
        }
        state.last_captured = Some(position.clone());
        if state
            .applied
            .as_ref()
            .is_none_or(|applied| compare(&state, position, applied).is_ok_and(|o| o.is_gt()))
        {
            if state.pending.len() == MAX_PENDING {
                state.valid = false;
                return;
            }
            if state
                .pending
                .back()
                .is_none_or(|(last, _)| last != position)
            {
                state.pending.push_back((position.clone(), txn.commit_time));
            }
        }
        if let Some(tip) = state.awaiting.as_ref()
            && compare(&state, position, tip.probe.tip()).is_ok_and(|o| !o.is_lt())
            && batch.watermarks.iter().any(|w| match w {
                super::CapturedMarker::Pg(w) => {
                    w.transactional
                        && pg_position_lsn(position, position.order_domain())
                            .is_ok_and(|p| p == w.commit_lsn)
                        && Watermark::parse(&w.message()).is_ok_and(|m| m == tip.marker)
                }
                super::CapturedMarker::Chunk { marker, commit, .. } => {
                    marker == &tip.marker && commit == position
                }
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
        let next = position;
        if let Err(error) = compare(&state, next, next).and_then(|_| match &state.applied {
            Some(old) => compare(&state, next, old),
            None => Ok(std::cmp::Ordering::Equal),
        }) {
            state.invalid_position = Some(error);
            state.valid = false;
            return;
        }
        if state.source.as_ref() != Some(&record.source)
            || checkpoint.last_position.as_ref() != Some(position)
            || checkpoint.next_mseq == 0
            || compare(&state, next, next).is_err()
        {
            state.valid = false;
            return;
        }
        if record.chunk_rows {
            return;
        }
        if state
            .applied
            .as_ref()
            .is_some_and(|old| compare(&state, next, old).map_or(true, |o| o.is_lt()))
        {
            state.valid = false;
            return;
        }
        state.applied = Some(next.clone());
        while state
            .pending
            .front()
            .is_some_and(|(position, _)| compare(&state, position, next).is_ok_and(|o| !o.is_gt()))
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
        if started > Instant::now() || lsn == 0 {
            self.revoke_tip();
            return Err(recovery("invalid live source tip"));
        }
        let binding = {
            let state = self.0.lock().expect("live readiness poisoned");
            state
                .source
                .clone()
                .filter(|s| source_matches(s, lineage))
                .zip(state.order.as_ref().map(|order| order.domain().to_owned()))
        };
        let (source, domain) = binding.ok_or_else(|| {
            self.revoke_tip();
            recovery("live source-tip lineage/order mismatch")
        })?;
        self.probe(
            Arc::new(PgReadinessProbe::new(
                source,
                domain,
                lsn,
                source_time_us,
                started,
            )),
            marker,
        )
        .map_err(recovery)
    }
    pub(super) fn bound(&self, source: &SourceIdentity, position: &Position) -> bool {
        let live = self.sample();
        live.capture
            .as_ref()
            .is_some_and(|capture| &capture.source == source && &capture.position == position)
    }
    fn probe(
        &self,
        probe: Arc<dyn ReadinessProbe>,
        marker: Watermark,
    ) -> std::result::Result<(), EventError> {
        let mut state = self.0.lock().expect("live readiness poisoned");
        state.awaiting = None;
        if !state.running || !state.valid || state.source.as_ref() != Some(probe.source()) {
            state.tip = None;
            return Err(EventError::InvalidTransaction(
                "readiness probe source/lifecycle mismatch",
            ));
        }
        if let Err(error) = compare(&state, probe.tip(), probe.tip()) {
            state.tip = None;
            return Err(error);
        }
        // The previous manifest-durable tip retains its original freshness
        // deadline while the new marker is pending, as in the PG producer.
        state.awaiting = Some(Tip { probe, marker });
        Ok(())
    }
    fn revoke_tip(&self) {
        let mut state = self.0.lock().expect("live readiness poisoned");
        state.tip = None;
        state.awaiting = None;
    }
    fn sample(&self) -> LiveReadinessObservation {
        let state = self.0.lock().expect("live readiness poisoned");
        let mut retention = None;
        let mut probe_schema = None;
        let blocked;
        let lag = (|| -> std::result::Result<_, ReadinessBlocked> {
            if let Some(error) = &state.invalid_position {
                return Err(ReadinessBlocked::Position(error.clone()));
            }
            if !state.running || !state.valid {
                return Err(ReadinessBlocked::Unavailable);
            }
            let tip = state.tip.as_ref().ok_or(
                if state
                    .source
                    .as_ref()
                    .is_some_and(|s| s.engine != "postgres")
                {
                    ReadinessBlocked::MissingProbe
                } else {
                    ReadinessBlocked::Unavailable
                },
            )?;
            let applied = state.applied.as_ref().ok_or(ReadinessBlocked::UnknownLag)?;
            let sample = tip
                .probe
                .sample(applied, state.pending.front().map(|(_, time)| *time))
                .map_err(ReadinessBlocked::Position)?;
            if Instant::now()
                .checked_duration_since(tip.probe.observed_at())
                .is_none_or(|age| age > MAX_OBSERVATION_AGE)
                || Instant::now()
                    .checked_duration_since(sample.observed_at)
                    .is_none_or(|age| age > MAX_OBSERVATION_AGE)
            {
                return Err(ReadinessBlocked::Stale);
            }
            probe_schema = sample.schema;
            if let Some(margin) = &sample.retention {
                compare(&state, &margin.position, tip.probe.tip())
                    .map_err(ReadinessBlocked::Position)?;
                retention = Some(margin.clone());
                if !margin.history_available {
                    return Err(ReadinessBlocked::RetentionLost);
                }
                let seconds = margin
                    .time_to_exhaustion
                    .ok_or(ReadinessBlocked::UnknownRetention)?;
                let level = readiness::retention_level(
                    true,
                    Some(seconds.as_secs_f64()),
                    &crate::status::RetentionPolicy::default(),
                );
                if matches!(level, WalLevel::Blocker | WalLevel::Abort) {
                    return Err(ReadinessBlocked::RetentionRisk(level));
                }
            } else if tip.probe.source().engine != "postgres" {
                return Err(ReadinessBlocked::UnknownRetention);
            }
            retention = sample.retention;
            if state.baseline.as_ref().is_some_and(|baseline| {
                compare(&state, applied, baseline).map_or(true, |o| o.is_lt())
            }) {
                return Err(ReadinessBlocked::UnknownLag);
            }
            Ok(ApplyLagObservation {
                source: state.source.clone().ok_or(ReadinessBlocked::Unavailable)?,
                order: state.order.clone().ok_or(ReadinessBlocked::Unavailable)?,
                source_tip: tip.probe.tip().clone(),
                applied_source: applied.clone(),
                lag: sample.lag.ok_or(ReadinessBlocked::UnknownLag)?,
                oldest_unapplied_age_ms: sample
                    .oldest_unapplied_age_ms
                    .ok_or(ReadinessBlocked::UnknownLag)?,
                observed_at: sample.observed_at,
            })
        })();
        let lag = match lag {
            Ok(lag) => {
                blocked = None;
                Some(lag)
            }
            Err(reason) => {
                blocked = Some(reason);
                None
            }
        };
        LiveReadinessObservation {
            owner_generation: state.owner_generation.clone(),
            capture: state
                .tip
                .as_ref()
                .filter(|tip| {
                    state.running
                        && state.valid
                        && Instant::now()
                            .checked_duration_since(tip.probe.observed_at())
                            .is_some_and(|age| age <= MAX_OBSERVATION_AGE)
                })
                .map(|tip| CaptureObservation {
                    healthy: true,
                    source: tip.probe.source().clone(),
                    position: tip.probe.tip().clone(),
                }),
            lag,
            blocked,
            retention,
            schema: state.schema.clone().or_else(|| probe_schema.clone()),
            running: state.running,
            schema_fresh: if state.schema.is_some() {
                state
                    .schema_at
                    .is_some_and(|at| at.elapsed() <= MAX_OBSERVATION_AGE)
            } else {
                probe_schema.is_some()
            },
        }
    }
}
// A failed observation has no evidence of schema drift and must never produce
// a durable marker. Successful observations alone can create a sticky blocker.
fn evaluate_schema_poll<E: std::fmt::Display>(
    mut schema: crate::pg::ddl::OperationalSchema,
    point: crate::pg::ddl::PollPoint,
    observation: std::result::Result<crate::pg::ddl::SchemaObservation, E>,
) -> Result<crate::pg::ddl::OperationalSchema> {
    schema.require_clear().map_err(recovery)?;
    let observed = observation.map_err(recovery)?;
    // poll records the blocker in schema; the caller persists it before failing.
    let _ = schema.poll(point, observed);
    Ok(schema)
}

impl Runtime {
    pub async fn scoped_schema_tables(&self) -> Result<Vec<crate::pg::chunk::QualifiedName>> {
        self.state()
            .await
            .definition
            .as_ref()
            .map(crate::pg::ddl::scoped_schema_tables)
            .ok_or_else(|| recovery("missing migration definition for schema scope"))
    }

    /// Register a connector probe before emitting its transactional marker.
    /// Capture must deliver that exact marker before this probe can supply facts.
    pub fn observe_readiness_probe(
        &self,
        probe: Arc<dyn ReadinessProbe>,
        marker: Watermark,
    ) -> std::result::Result<(), EventError> {
        self.7.probe(probe, marker)
    }

    pub fn live_readiness(&self) -> LiveReadinessObservation {
        self.7.sample()
    }

    /// Dedicated source admin session. The live IDENTIFY_SYSTEM lineage must
    /// come from the same configured endpoint. A captured transactional marker
    /// proves coverage of this sampled tip, including otherwise idle sources.
    /// Caller starts the freshness clock before connection/query awaits.
    pub async fn poll_schema(
        &self,
        admin: &tokio_postgres::Client,
        tables: &[crate::pg::chunk::QualifiedName],
        point: crate::pg::ddl::PollPoint,
    ) -> Result<()> {
        let schema = self.state().await.operational_schema.clone();
        schema.require_clear().map_err(recovery)?;
        let schema =
            evaluate_schema_poll(schema, point, crate::pg::ddl::observe(admin, tables).await)?;
        if let Some(blocker) = &schema.blocker {
            self.marker(
                crate::pg::ddl::BLOCKER_STREAM,
                serde_json::to_string(blocker)?,
                "sticky".into(),
            )
            .await?;
        }
        schema.require_clear().map_err(recovery)
    }
    pub async fn probe_readiness(
        &self,
        admin: &tokio_postgres::Client,
        lineage: &Lineage,
        started: Instant,
    ) -> Result<()> {
        let result = async {
            let row = admin.query_one("SELECT pg_current_wal_lsn()::text, (extract(epoch FROM clock_timestamp()) * 1000000)::bigint", &[]).await.map_err(recovery)?;
            let tip = crate::pg::replication::parse_lsn(&row.get::<_, String>(0)).map_err(recovery)?;
            let tables = self.scoped_schema_tables().await?;
            self.poll_schema(admin, &tables, crate::pg::ddl::PollPoint::Watermark(tip)).await?;
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
    fn failed_schema_observation_never_produces_a_blocker() {
        use crate::pg::ddl::{OperationalSchema, PollPoint, SchemaObservation};
        let baseline = "0123456789abcdef0123456789abcdef".to_owned();
        let schema = OperationalSchema {
            baseline: Some(baseline.clone()),
            blocker: None,
        };
        for point in [PollPoint::Watermark(10), PollPoint::FinalMarker(20)] {
            assert!(
                evaluate_schema_poll(
                    schema.clone(),
                    point.clone(),
                    Err::<SchemaObservation, _>("connection lost")
                )
                .is_err()
            );
            let clean = evaluate_schema_poll(
                schema.clone(),
                point.clone(),
                Ok::<_, &str>(SchemaObservation {
                    fingerprint: baseline.clone(),
                    hazards: vec![],
                }),
            )
            .unwrap();
            assert!(clean.blocker.is_none());
            for observed in [
                SchemaObservation {
                    fingerprint: "invalid".into(),
                    hazards: vec![],
                },
                SchemaObservation {
                    fingerprint: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
                    hazards: vec![],
                },
                SchemaObservation {
                    fingerprint: baseline.clone(),
                    hazards: vec![crate::pg::ddl::Hazard {
                        kind: "invalid-or-unready-index".into(),
                        relation: "public.t".into(),
                        object: "t_idx".into(),
                    }],
                },
            ] {
                let blocked =
                    evaluate_schema_poll(schema.clone(), point.clone(), Ok::<_, &str>(observed))
                        .unwrap();
                assert!(blocked.blocker.is_some());
                assert!(blocked.require_clear().is_err());
            }
        }
    }

    #[test]
    fn bounded_queue_overflow_and_unknown_writer_fail_closed() {
        let source = SourceIdentity {
            engine: "postgres".into(),
            system_id: "1".into(),
            lineage: "1".into(),
            db_name: "db".into(),
            stream: "slot".into(),
        };
        let hub = Hub::default();
        hub.begin(
            source.clone(),
            Arc::new(crate::pg::apply::NativePgOrder("pg".into())),
            None,
            None,
        );
        assert!(!hub.0.lock().unwrap().valid);
        hub.begin(
            source.clone(),
            Arc::new(crate::pg::apply::NativePgOrder("pg".into())),
            Some(WriterCheckpoint::default()),
            None,
        );
        hub.0.lock().unwrap().pending.resize(
            MAX_PENDING,
            (Position::new("pg", 1u64.to_be_bytes()), Some(0)),
        );
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
            durable_position: Some(Position::new("pg", 3u64.to_be_bytes())),
            ..Captured::default()
        });
        let state = hub.0.lock().unwrap();
        assert!(!state.valid);
        assert_eq!(state.pending.len(), MAX_PENDING);
        assert!(std::mem::size_of::<Runtime>() <= 8 * std::mem::size_of::<usize>());
    }
}
