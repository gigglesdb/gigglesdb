//! Live readiness assembly and source-retention policy. No readiness bit is kept.
use super::*;
use crate::{
    coordinator::readiness::ReadinessFacts,
    status::{RetentionPolicy, SlotStatus},
    verify::{ResultSlot, VerificationReport},
};
use serde::{Deserialize, Serialize};

pub(super) const COVERAGE_LOSS: &str = "engine-coverage-loss-v1";

/// A producer is sampled on every evaluation. Missing collectors are explicit;
/// supplying a report never substitutes for the engine's journal/chunk checks.
pub trait ReadinessSource: Send + Sync {
    fn sample(&self) -> ComponentSnapshot;
}
#[derive(Default)]
pub struct ComponentSnapshot {
    pub capture: Option<CaptureObservation>,
    /// Fresh paired source/applied positions and oldest-unapplied age.
    pub apply_lag: Option<ApplyLagObservation>,
    /// (blocked DDL, unknown schema job). Absence blocks schema readiness.
    pub schema_jobs: Option<(bool, bool)>,
    pub verification: Option<VerificationReport>,
    pub busy_rows_past_budget: Option<u64>,
    pub dead_letters: Option<u64>,
    /// Current synchronization capability/privileges, independent of equality
    /// in a sequence comparison report.
    pub sequences_syncable: Option<bool>,
    pub reverse_prerequisites: Option<bool>,
    pub fence_risks_acknowledged: Option<bool>,
    pub buffer_contract_matches: Option<bool>,
    pub wal: Option<WalObservation>,
}
#[derive(Clone, Debug)]
pub struct CaptureObservation {
    pub healthy: bool,
    /// Must be observed from the connected source, not copied from the saved marker.
    pub source: SourceIdentity,
    pub position: Position,
}
pub struct WalObservation {
    pub slot: SlotStatus,
    /// pg_current_wal_lsn, not retained bytes: restart_lsn can advance.
    pub current_lsn: u64,
}
pub(super) fn retention_level(
    valid: bool,
    seconds: Option<f64>,
    policy: &RetentionPolicy,
) -> WalLevel {
    if !valid || seconds.is_some_and(|s| s <= policy.abort_seconds as f64) {
        WalLevel::Abort
    } else if seconds.is_some_and(|s| s <= policy.blocker_seconds as f64) {
        WalLevel::Blocker
    } else if seconds.is_some_and(|s| s <= policy.warning_seconds as f64) {
        WalLevel::Warning
    } else {
        WalLevel::Healthy
    }
}
fn clean<T>(section: &ResultSlot<T>) -> bool {
    matches!(section, ResultSlot::Collected { findings, .. } if findings.is_empty())
}

fn binding_matches(binding: &coordinator::SourceBinding, capture: &CaptureObservation) -> bool {
    let connector = match binding.connector {
        coordinator::SourceConnector::Postgres => "postgres",
        coordinator::SourceConnector::Mysql => "mysql",
        coordinator::SourceConnector::Spanner => "spanner",
    };
    binding.validate().is_ok()
        && capture.source.engine == connector
        && capture.position.order_domain() == binding.order_domain
        && capture.source.stream == binding.stream
}

pub(super) async fn assemble(
    runtime: &Runtime,
    journal: &Journal,
    sample: &ComponentSnapshot,
) -> Result<ReadinessFacts> {
    let state = runtime.state().await;
    let mut chunks_done = !state.chunk_plans.is_empty();
    for reference in state.chunk_plans.values() {
        let plan = journal.chunk_plan(reference).await?;
        chunks_done &= state.unfinished_chunks(&plan)?.is_empty();
    }
    let lineage_matches = sample.capture.as_ref().is_some_and(|capture| {
        if state
            .source_binding
            .as_ref()
            .is_some_and(|binding| !binding_matches(binding, capture))
        {
            return false;
        }
        if capture.source.engine == "postgres" {
            state
                .progress
                .get("source-slot")
                .is_some_and(|(marker, _)| {
                    decode_source_checkpoint(marker).is_ok_and(|saved| {
                        live_readiness::source_matches(&capture.source, &saved.lineage)
                            && saved.slot == capture.source.stream
                    })
                })
        } else {
            runtime.7.bound(&capture.source, &capture.position)
        }
    });
    // ReadinessFacts carries comparable integers. Both values are milliseconds;
    // byte distance is diagnostic only, never compared with a seconds budget.
    let lag_budget = state.definition.as_ref().map_or(0, |d| {
        d.readiness_policy
            .max_lag_seconds
            .min(d.budgets.max_replication_lag_seconds)
    });
    let lag = sample.apply_lag.as_ref().filter(|lag| {
        lag.fresh()
            && sample
                .capture
                .as_ref()
                .is_some_and(|capture| lag.matches(capture))
            && state.progress.get("applied").is_some_and(|(marker, _)| {
                decode_reader_checkpoint(marker).is_ok_and(|checkpoint| {
                    checkpoint
                        .last_record
                        .as_ref()
                        .is_some_and(|record| record.source == lag.source)
                        && checkpoint.last_position.as_ref().is_some_and(|p| {
                            lag.applied_source
                                .compare(p, lag.order.as_ref())
                                .is_ok_and(|order| !order.is_gt())
                        })
                })
            })
    });
    // An explicit failing pair also fails closed when the configured budget
    // saturates at u64::MAX; MAX + 1 cannot represent absence safely.
    let (apply_lag, lag_budget) = lag.map_or((1, 0), |lag| lag.compare_budget(lag_budget));
    let (blocked_ddl, unknown_schema_job) = sample.schema_jobs.unwrap_or((true, true));
    // An absent report deliberately differs from an empty, collected table.
    let (mut verified_rows, mut in_scope_rows) = (0u64, 1u64);
    if let Some(report) = &sample.verification {
        let selected: std::collections::BTreeSet<_> = state.chunk_plans.keys().collect();
        let reported: std::collections::BTreeSet<_> =
            report.tables.iter().map(|t| &t.table).collect();
        if selected == reported && reported.len() == report.tables.len() {
            verified_rows = 0;
            in_scope_rows = 0;
            let mut complete = true;
            for table in &report.tables {
                in_scope_rows = in_scope_rows
                    .checked_add(table.rows.total)
                    .ok_or_else(|| recovery("verification coverage count overflow"))?;
                verified_rows = verified_rows
                    .checked_add(table.rows.verified)
                    .ok_or_else(|| recovery("verification coverage count overflow"))?;
                if !table.rows.is_complete() || table.exclusion_reason.is_some() {
                    complete = false;
                }
            }
            if !complete && verified_rows == in_scope_rows {
                // Malformed busy/mismatch/exclusion counts cannot pass 0 == 0.
                (verified_rows, in_scope_rows) = (0, 1);
            }
        }
    }
    Ok(ReadinessFacts {
        all_chunks_done: chunks_done,
        capture_healthy: sample.capture.as_ref().is_some_and(|c| c.healthy)
            && *runtime.2.read().expect("engine health poisoned"),
        lineage_matches,
        apply_lag,
        lag_budget,
        blocked_ddl,
        unknown_schema_job,
        verified_rows,
        in_scope_rows,
        busy_rows_past_budget: sample.busy_rows_past_budget.unwrap_or(1),
        dead_letters: sample
            .dead_letters
            .unwrap_or(1)
            .max(state.verifier_exceptions.len() as u64),
        indexes_constraints_valid: sample.verification.as_ref().is_some_and(|r| {
            clean(&r.schema) && clean(&r.indexes) && clean(&r.constraints) && clean(&r.foreign_keys)
        }),
        sequences_syncable: sample.sequences_syncable == Some(true)
            && sample
                .verification
                .as_ref()
                .is_some_and(|r| clean(&r.sequences)),
        reverse_prerequisites: sample.reverse_prerequisites.unwrap_or(false),
        fence_risks_acknowledged: sample.fence_risks_acknowledged.unwrap_or(false),
        buffer_contract_matches: sample.buffer_contract_matches.unwrap_or(false),
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WalLevel {
    Healthy,
    Warning,
    Blocker,
    Abort,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WalDecision {
    pub level: WalLevel,
    pub wal_bytes_per_second: Option<f64>,
    pub seconds_to_exhaustion: Option<f64>,
    pub slot_headroom_bytes: Option<u64>,
    pub disk_free_bytes: u64,
    pub coverage_loss: bool,
    pub resumable: bool,
    pub source_untouched: bool,
}
/// Monotonic clock and configurable defaults shared with the status policy:
/// warning 3600s, blocker 900s, abort 120s. A first sample cannot infer a rate.
pub struct WalMonitor {
    clock: Arc<dyn budget::Clock>,
    policy: RetentionPolicy,
    previous: Option<(Duration, u64)>,
}
impl WalMonitor {
    pub fn new(clock: Arc<dyn budget::Clock>, policy: RetentionPolicy) -> Result<Self> {
        if policy.abort_seconds == 0
            || policy.warning_seconds < policy.blocker_seconds
            || policy.blocker_seconds < policy.abort_seconds
        {
            return Err(recovery("invalid WAL retention thresholds"));
        }
        Ok(Self {
            clock,
            policy,
            previous: None,
        })
    }
    pub fn sample(&mut self, observation: &WalObservation) -> Result<WalDecision> {
        let now = self.clock.now();
        let rate = if let Some((before, lsn)) = self.previous {
            let elapsed = now
                .checked_sub(before)
                .ok_or_else(|| recovery("WAL clock regressed"))?;
            let growth = observation
                .current_lsn
                .checked_sub(lsn)
                .ok_or_else(|| recovery("source WAL LSN regressed"))?;
            (!elapsed.is_zero()).then(|| growth as f64 / elapsed.as_secs_f64())
        } else {
            None
        };
        // Do not discard the last nonzero interval when the clock is unchanged.
        if self.previous.is_none_or(|(before, _)| now > before) {
            self.previous = Some((now, observation.current_lsn));
        }
        let slot = &observation.slot;
        let headroom = slot
            .max_slot_wal_keep_size_bytes
            .map(|max| max.saturating_sub(slot.retained_wal_bytes));
        let bytes = headroom.map_or(slot.disk_free_bytes, |h| h.min(slot.disk_free_bytes));
        let seconds = if bytes == 0 {
            Some(0.0)
        } else {
            rate.filter(|r| *r > 0.0).map(|r| bytes as f64 / r)
        };
        let level = retention_level(slot.valid, seconds, &self.policy);
        Ok(WalDecision {
            level,
            wal_bytes_per_second: rate,
            seconds_to_exhaustion: seconds,
            slot_headroom_bytes: headroom,
            disk_free_bytes: slot.disk_free_bytes,
            coverage_loss: level == WalLevel::Abort,
            resumable: level != WalLevel::Abort,
            source_untouched: true,
        })
    }
    /// Native connectors supply their own time projection, never a fabricated LSN.
    pub fn sample_native(&self, margin: &RetentionMargin) -> Option<WalDecision> {
        let seconds = margin.time_to_exhaustion.map(|time| time.as_secs_f64());
        if margin.history_available && seconds.is_none() {
            return None;
        }
        let level = retention_level(margin.history_available, seconds, &self.policy);
        Some(WalDecision {
            level,
            wal_bytes_per_second: None,
            seconds_to_exhaustion: seconds,
            slot_headroom_bytes: None,
            disk_free_bytes: 0,
            coverage_loss: level == WalLevel::Abort,
            resumable: level != WalLevel::Abort,
            source_untouched: true,
        })
    }
    pub(super) fn sample_components(
        &mut self,
        runtime: &Runtime,
        sample: &ComponentSnapshot,
    ) -> Result<Option<WalDecision>> {
        let live = runtime.live_readiness();
        if live
            .capture
            .as_ref()
            .is_some_and(|capture| capture.source.engine != "postgres")
        {
            return Ok(live
                .retention
                .as_ref()
                .and_then(|margin| self.sample_native(margin)));
        }
        sample
            .wal
            .as_ref()
            .map(|observation| self.sample(observation))
            .transpose()
    }
}
pub(super) enum SupervisedStop {
    Cancelled,
    Target(WalMonitor),
    Wal(WalDecision),
}

impl Engine {
    /// Optional live producers. Unconnected components continue to block readiness.
    pub fn with_readiness_source(mut self, source: Arc<dyn ReadinessSource>) -> Self {
        self.readiness_source = Some(source);
        self
    }
    /// Whether durable capture coverage still permits resumption. Lifecycle and
    /// operator-recovery checks apply separately in the coordinator.
    pub fn resumable(&self) -> bool {
        !self.runtime.snapshot().progress.contains_key(COVERAGE_LOSS)
    }
    pub(super) fn require_coverage(&self) -> Result<()> {
        self.runtime
            .snapshot()
            .operational_schema
            .require_clear()
            .map_err(recovery)?;
        if self.resumable() {
            Ok(())
        } else {
            Err(recovery(
                "abort-with-coverage-loss: resumability invalidated; resnapshot into a new migration; source untouched",
            ))
        }
    }
    /// Assemble from current journal/component state, then journal the decision.
    pub async fn evaluate_readiness(&self) -> Result<ReadinessFacts> {
        let sample = self
            .readiness_source
            .as_ref()
            .map_or_else(ComponentSnapshot::default, |s| s.sample());
        self.evaluate_sample(&sample).await
    }
    pub(super) async fn evaluate_sample(
        &self,
        sample: &ComponentSnapshot,
    ) -> Result<ReadinessFacts> {
        self.require_coverage()?;
        // Suppress duplicate writes, never the old per-evaluation authority
        // refresh (including its periodic authentication deadline).
        Box::pin(self.runtime.refresh_owner()).await?;
        // Serialize journal state and the readiness commit with copy/apply.
        let mut coordinator = self.runtime.0.lock().await;
        self.runtime.flush_progress_locked(&mut coordinator).await?;
        let facts = assemble(&self.runtime, &self.journal, sample).await?;
        if matches!(
            coordinator.state().lifecycle.state,
            State::CatchingUp | State::VerifiedReady
        ) && self
            .runtime
            .2
            .persistence
            .lock()
            .expect("persistence poisoned")
            .readiness
            .as_ref()
            .is_none_or(|(saved, at)| {
                saved != &facts
                    || at.elapsed() >= Duration::from_secs(1)
                    || coordinator.state().lifecycle.blockers != facts.blockers()
                    || (facts.blockers().is_empty()
                        && coordinator.state().lifecycle.state != State::VerifiedReady)
            })
        {
            let result = coordinator
                .commit(Transition::Lifecycle {
                    event: Event::Readiness {
                        facts: facts.clone(),
                    },
                })
                .await;
            self.runtime.observe_journal_error(&coordinator, &result);
            result?;
            self.runtime
                .2
                .persistence
                .lock()
                .expect("persistence poisoned")
                .readiness = Some((facts.clone(), std::time::Instant::now()));
            *self.runtime.1.write().expect("engine snapshot poisoned") =
                coordinator.state().clone();
        }
        Ok(facts)
    }
    /// Journal before stopping. No source-side action or buffer deletion occurs.
    pub(super) async fn enforce_wal(&self, decision: &WalDecision) -> Result<()> {
        self.require_coverage()?;
        let encoded = serde_json::to_string(decision)?;
        if decision.level == WalLevel::Abort {
            self.runtime
                .marker(COVERAGE_LOSS, encoded, "abort-with-coverage-loss".into())
                .await?;
        } else {
            self.runtime
                .marker(
                    "engine-wal-risk-v1",
                    encoded,
                    format!("{:?}", decision.level),
                )
                .await?;
        }
        if matches!(decision.level, WalLevel::Blocker | WalLevel::Abort) {
            if self.runtime.snapshot().lifecycle.state == State::VerifiedReady {
                // Use a fresh failed readiness snapshot before a pre-fence interrupt.
                let sample = self
                    .readiness_source
                    .as_ref()
                    .map_or_else(ComponentSnapshot::default, |s| s.sample());
                let mut facts = assemble(&self.runtime, &self.journal, &sample).await?;
                facts.capture_healthy = false;
                self.runtime.lifecycle(Event::Readiness { facts }).await?;
            }
            if self.runtime.snapshot().lifecycle.state == State::Blocked {
                // Blocked has no Failed edge. The coverage marker is the durable
                // engine-level terminal boundary and survives coordinator replay.
                return Ok(());
            }
            self.runtime.lifecycle(Event::Interrupt {
                to: if decision.level == WalLevel::Abort { State::Failed } else { State::Blocked },
                reason: if decision.level == WalLevel::Abort {
                    "abort-with-coverage-loss: projected WAL exhaustion; resumability invalidated; source untouched; resnapshot required".into()
                } else { "wal-retention-exhaustion: projected source WAL/disk headroom below blocker threshold".into() },
            }).await?;
        }
        Ok(())
    }
    pub fn coverage_loss_report(&self) -> Option<String> {
        self.runtime
            .snapshot()
            .progress
            .get(COVERAGE_LOSS)
            .map(|(report, _)| report.clone())
    }
    pub async fn monitor_wal(
        &self,
        monitor: &mut WalMonitor,
        observation: &WalObservation,
    ) -> Result<WalDecision> {
        if self.running.swap(true, std::sync::atomic::Ordering::SeqCst) {
            return Err(recovery(
                "active engine owns WAL enforcement; workers must join before abort",
            ));
        }
        let _running = RunGuard(&self.running);
        let decision = monitor.sample(observation)?;
        self.enforce_wal(&decision).await?;
        Ok(decision)
    }
    pub(super) async fn supervision_loop(
        &self,
        mut cancel: watch::Receiver<bool>,
        stop: watch::Sender<bool>,
        stop_at: StopAt,
    ) -> Result<SupervisedStop> {
        let mut wal = WalMonitor::new(
            Arc::new(budget::SystemClock::default()),
            RetentionPolicy::default(),
        )?;
        loop {
            if *cancel.borrow() {
                return Ok(SupervisedStop::Cancelled);
            }
            let sample = self
                .readiness_source
                .as_ref()
                .map_or_else(ComponentSnapshot::default, |s| s.sample());
            self.evaluate_sample(&sample).await?;
            if let Some(decision) = wal.sample_components(&self.runtime, &sample)? {
                if matches!(decision.level, WalLevel::Blocker | WalLevel::Abort) {
                    // Stop and join workers before recording the terminal/blocking event.
                    stop.send_replace(true);
                    return Ok(SupervisedStop::Wal(decision));
                }
                if decision.level != WalLevel::Healthy {
                    self.enforce_wal(&decision).await?;
                }
            }
            if stop_at != StopAt::UntilPaused
                && stop_at.matches(self.runtime.snapshot().lifecycle.state)
            {
                stop.send_replace(true);
                return Ok(SupervisedStop::Target(wal));
            }
            tokio::select! { biased;
                _ = cancelled(&mut cancel) => return Ok(SupervisedStop::Cancelled),
                _ = tokio::time::sleep(Duration::from_millis(100)) => {},
            }
        }
    }
}

#[cfg(test)]
mod binding_tests {
    use super::*;

    #[test]
    fn native_readiness_requires_the_saved_connector_domain_stream_and_version() {
        let binding = coordinator::SourceBinding {
            version: 1,
            connector: coordinator::SourceConnector::Mysql,
            order_domain: "mysql:source/gtid".into(),
            stream: "binlog".into(),
        };
        let capture = CaptureObservation {
            healthy: true,
            source: SourceIdentity {
                engine: "mysql".into(),
                system_id: "source".into(),
                lineage: "cluster".into(),
                db_name: "database".into(),
                stream: "binlog".into(),
            },
            position: Position::new("mysql:source/gtid", b"native"),
        };
        assert!(binding_matches(&binding, &capture));
        for field in ["connector", "domain", "stream", "version"] {
            let mut wrong = binding.clone();
            match field {
                "connector" => wrong.connector = coordinator::SourceConnector::Postgres,
                "domain" => wrong.order_domain.push_str("-wrong"),
                "stream" => wrong.stream.push_str("-wrong"),
                _ => wrong.version = 2,
            }
            assert!(!binding_matches(&wrong, &capture), "{field}");
        }
    }
}
