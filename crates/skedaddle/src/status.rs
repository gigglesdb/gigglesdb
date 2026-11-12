//! Observed status and conservative projections, not lifecycle commands.
//!
//! Building status has no database, slot, authority, or journal side effects.
//! The coordinator must execute/journal any declared abort and invalidate
//! resumability before treating coverage loss as handled.

use std::{collections::BTreeMap, fmt, sync::Arc};

use serde::{Deserialize, Serialize};

use crate::{
    budget::Clock,
    coordinator::{Authority, ReadinessFacts, State},
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Error(pub &'static str);

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for Error {}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Counts {
    pub rows: u64,
    pub bytes: u64,
    pub transactions: u64,
}

impl Counts {
    fn delta(self, previous: Self) -> Result<Self, Error> {
        Ok(Self {
            rows: self
                .rows
                .checked_sub(previous.rows)
                .ok_or(Error("row counter reset"))?,
            bytes: self
                .bytes
                .checked_sub(previous.bytes)
                .ok_or(Error("byte counter reset"))?,
            transactions: self
                .transactions
                .checked_sub(previous.transactions)
                .ok_or(Error("transaction counter reset"))?,
        })
    }

    fn rate(self, window_ms: u64) -> Rate {
        let scale = 1000.0 / window_ms as f64;
        Rate {
            rows_per_second: self.rows as f64 * scale,
            bytes_per_second: self.bytes as f64 * scale,
            transactions_per_second: self.transactions as f64 * scale,
        }
    }
}

/// Cumulative counters from the same migration/epoch. Buffer counts advance
/// only at durable manifest publication; apply counts only after commit.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Counters {
    pub capture: Counts,
    pub buffer: Counts,
    pub apply: Counts,
    pub wal_bytes: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Rate {
    pub rows_per_second: f64,
    pub bytes_per_second: f64,
    pub transactions_per_second: f64,
}

impl Rate {
    fn valid(self) -> bool {
        [
            self.rows_per_second,
            self.bytes_per_second,
            self.transactions_per_second,
        ]
        .into_iter()
        .all(|v| v.is_finite() && v >= 0.0)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Measurement {
    pub window_ms: u64,
    pub capture: Rate,
    pub buffer: Rate,
    pub apply: Rate,
    /// WAL generated on the source, not captured logical payload bytes.
    pub wal_bytes_per_second: f64,
}

impl Measurement {
    fn validate(&self) -> Result<(), Error> {
        if self.window_ms == 0
            || !self.capture.valid()
            || !self.buffer.valid()
            || !self.apply.valid()
            || !self.wal_bytes_per_second.is_finite()
            || self.wal_bytes_per_second < 0.0
        {
            return Err(Error(
                "rates require a positive window and finite nonnegative values",
            ));
        }
        Ok(())
    }
}

pub struct RateMeter {
    clock: Arc<dyn Clock>,
    previous_ms: u64,
    previous: Counters,
}

impl RateMeter {
    pub fn new(clock: Arc<dyn Clock>, initial: Counters) -> Result<Self, Error> {
        Ok(Self {
            previous_ms: clock_ms(clock.as_ref())?,
            clock,
            previous: initial,
        })
    }

    /// Failed/zero-length samples do not move the baseline. Recreate the meter
    /// on epoch/counter reset rather than mixing unrelated histories.
    pub fn sample(&mut self, counters: Counters) -> Result<Measurement, Error> {
        let now = clock_ms(self.clock.as_ref())?;
        let window_ms = now
            .checked_sub(self.previous_ms)
            .filter(|ms| *ms > 0)
            .ok_or(Error(
                "measurement clock must advance by at least one millisecond",
            ))?;
        let measurement = Measurement {
            window_ms,
            capture: counters
                .capture
                .delta(self.previous.capture)?
                .rate(window_ms),
            buffer: counters.buffer.delta(self.previous.buffer)?.rate(window_ms),
            apply: counters.apply.delta(self.previous.apply)?.rate(window_ms),
            wal_bytes_per_second: counters
                .wal_bytes
                .checked_sub(self.previous.wal_bytes)
                .ok_or(Error("WAL counter reset"))? as f64
                * 1000.0
                / window_ms as f64,
        };
        self.previous_ms = now;
        self.previous = counters;
        Ok(measurement)
    }
}

fn clock_ms(clock: &dyn Clock) -> Result<u64, Error> {
    u64::try_from(clock.now().as_millis()).map_err(|_| Error("clock exceeds millisecond range"))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ChunkState {
    Pending,
    Copying,
    Done,
    Blocked,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChunkProgress {
    pub range_lo: String,
    pub range_hi: String,
    pub state: ChunkState,
    pub copied_rows: u64,
    pub copied_bytes: u64,
    pub estimated_total_rows: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SlotStatus {
    pub restart_lsn: Option<String>,
    pub confirmed_flush_lsn: Option<String>,
    pub valid: bool,
    pub invalidation_reason: Option<String>,
    pub retained_wal_bytes: u64,
    /// None denotes PostgreSQL's unlimited setting, not zero headroom.
    pub max_slot_wal_keep_size_bytes: Option<u64>,
    pub disk_free_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BufferStatus {
    pub pending_bytes: u64,
    pub pending_transactions: u64,
    pub oldest_unapplied_transaction_age_ms: u64,
    pub capture_paused_by_backpressure: bool,
    pub resident_segments: u32,
    pub max_resident_segments: u32,
    /// Declare the actual configured backend, encryption and recovery contract.
    pub backend: String,
    pub encryption: String,
    pub failure_domain_contract: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableCoverage {
    /// Includes explicitly excluded rows. Busy rows remain in-scope.
    pub total_rows: u64,
    pub verified_rows: u64,
    pub busy_rows: u64,
    pub mismatched_rows: u64,
    pub excluded_rows: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerificationCoverage {
    pub tables: BTreeMap<String, TableCoverage>,
    pub schema_verified: bool,
    pub sequences_verified: bool,
    pub constraints_verified: bool,
    pub indexes_verified: bool,
    pub foreign_keys_verified: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LatencyProbe {
    pub query: String,
    pub window_ms: u64,
    pub samples: u64,
    pub p50_ms: Option<f64>,
    pub p99_ms: Option<f64>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceImpact {
    pub replication_connection_lag_ms: Option<u64>,
    pub latency_probe: LatencyProbe,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Blocker {
    pub code: String,
    pub reason: String,
    pub remediation: String,
}

fn blocker(code: &str, reason: &str, remediation: &str) -> Blocker {
    Blocker {
        code: code.into(),
        reason: reason.into(),
        remediation: remediation.into(),
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(tag = "catch_up", rename_all = "kebab-case")]
pub enum CatchUp {
    NotCurrentlyAchievable,
    Estimated { seconds: f64 },
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Eta {
    #[serde(flatten)]
    pub catch_up: CatchUp,
    pub window_ms: u64,
    pub assumptions: Vec<String>,
}

impl Eta {
    fn project(buffer: &BufferStatus, rates: &Measurement) -> Self {
        let txn_spare = rates.apply.transactions_per_second - rates.capture.transactions_per_second;
        let byte_spare = rates.apply.bytes_per_second - rates.capture.bytes_per_second;
        let catch_up = if txn_spare <= 0.0 || (buffer.pending_bytes > 0 && byte_spare <= 0.0) {
            CatchUp::NotCurrentlyAchievable
        } else {
            let txns = buffer.pending_transactions as f64 / txn_spare;
            let bytes = if buffer.pending_bytes == 0 {
                0.0
            } else {
                buffer.pending_bytes as f64 / byte_spare
            };
            let seconds = txns.max(bytes);
            if seconds.is_finite() {
                CatchUp::Estimated { seconds }
            } else {
                CatchUp::NotCurrentlyAchievable
            }
        };
        Self {
            catch_up,
            window_ms: rates.window_ms,
            assumptions: vec![
                "Capture and committed apply rates remain equal to the measured window averages.".into(),
                "Pending work includes all captured work; no remaining copy work, pauses, retries or schema changes are added.".into(),
                "The slower of transaction and byte backlog drainage bounds catch-up; this is not a cutover ETA.".into(),
            ],
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AbortPolicy {
    AbortWithCoverageLoss,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetentionPolicy {
    pub warning_seconds: u64,
    pub blocker_seconds: u64,
    pub abort_seconds: u64,
    pub abort_policy: AbortPolicy,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            warning_seconds: 3600,
            blocker_seconds: 900,
            abort_seconds: 120,
            abort_policy: AbortPolicy::AbortWithCoverageLoss,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum RiskLevel {
    Healthy,
    Warning,
    Blocker,
    Abort,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum RetentionLimit {
    SlotWal,
    Disk,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct AbortDirective {
    pub policy: AbortPolicy,
    pub leave_source_untouched: bool,
    pub invalidate_resumability: bool,
    pub declare_coverage_loss: bool,
    pub remediation: String,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct RetentionRisk {
    pub level: RiskLevel,
    pub slot_headroom_bytes: Option<u64>,
    pub seconds_to_slot_limit: Option<f64>,
    pub seconds_to_disk_exhaustion: Option<f64>,
    pub limiting_resource: Option<RetentionLimit>,
    pub policy: RetentionPolicy,
    pub window_ms: u64,
    pub assumptions: Vec<String>,
    pub remediation: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub abort: Option<AbortDirective>,
}

fn seconds_to_limit(headroom: u64, rate: f64) -> Option<f64> {
    if headroom == 0 {
        return Some(0.0);
    }
    if rate == 0.0 {
        return None;
    }
    let seconds = headroom as f64 / rate;
    seconds.is_finite().then_some(seconds)
}

impl RetentionRisk {
    fn project(
        slot: &SlotStatus,
        rates: &Measurement,
        policy: RetentionPolicy,
    ) -> Result<Self, Error> {
        if policy.abort_seconds == 0
            || policy.warning_seconds < policy.blocker_seconds
            || policy.blocker_seconds < policy.abort_seconds
        {
            return Err(Error(
                "retention thresholds must be warning >= blocker >= abort > 0",
            ));
        }
        let slot_headroom_bytes = slot
            .max_slot_wal_keep_size_bytes
            .map(|max| max.saturating_sub(slot.retained_wal_bytes));
        let seconds_to_slot_limit = slot_headroom_bytes
            .and_then(|bytes| seconds_to_limit(bytes, rates.wal_bytes_per_second));
        let seconds_to_disk_exhaustion =
            seconds_to_limit(slot.disk_free_bytes, rates.wal_bytes_per_second);
        let earliest = [
            seconds_to_slot_limit.map(|s| (s, RetentionLimit::SlotWal)),
            seconds_to_disk_exhaustion.map(|s| (s, RetentionLimit::Disk)),
        ]
        .into_iter()
        .flatten()
        .min_by(|a, b| a.0.total_cmp(&b.0));
        let level = match earliest.map(|(seconds, _)| seconds) {
            Some(s) if s <= policy.abort_seconds as f64 => RiskLevel::Abort,
            Some(s) if s <= policy.blocker_seconds as f64 => RiskLevel::Blocker,
            Some(s) if s <= policy.warning_seconds as f64 => RiskLevel::Warning,
            _ => RiskLevel::Healthy,
        };
        Ok(Self {
            level,
            slot_headroom_bytes,
            seconds_to_slot_limit,
            seconds_to_disk_exhaustion,
            limiting_resource: earliest.map(|(_, limit)| limit),
            policy,
            window_ms: rates.window_ms,
            assumptions: vec![
                "Source WAL generation remains equal to the measured window average.".into(),
                "No retained WAL is released; all new WAL consumes disk headroom; other disk consumers are excluded.".into(),
                "max_slot_wal_keep_size is a conservative planning boundary, not an exact slot invalidation deadline.".into(),
            ],
            remediation: (level != RiskLevel::Healthy).then(||
                "Restore capture/apply throughput or safely increase WAL/disk headroom before the projected limit; never advance the slot past durable coverage.".into()),
            abort: (level == RiskLevel::Abort).then(|| AbortDirective {
                policy: policy.abort_policy,
                leave_source_untouched: true,
                invalidate_resumability: true,
                declare_coverage_loss: true,
                remediation: "Stop the migration safely, journal coverage loss and invalidate resume; resnapshot into a new migration after restoring headroom.".into(),
            }),
        })
    }
}

/// Live observations. `readiness` supplies the independent coordinator facts;
/// chunk, coverage and slot observations additionally constrain those facts.
pub struct Observations {
    pub state: State,
    pub epoch: u64,
    pub authority: Authority,
    pub chunks: BTreeMap<String, Vec<ChunkProgress>>,
    pub slot: SlotStatus,
    pub rates: Measurement,
    pub buffer: BufferStatus,
    pub verification: VerificationCoverage,
    pub blockers: Vec<Blocker>,
    pub source_impact: SourceImpact,
    pub readiness: ReadinessFacts,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Status {
    pub schema_version: u32,
    pub state: State,
    pub epoch: u64,
    pub authority: Authority,
    pub chunks: BTreeMap<String, Vec<ChunkProgress>>,
    pub slot: SlotStatus,
    pub rates: Measurement,
    pub buffer: BufferStatus,
    pub verification: VerificationCoverage,
    pub blockers: Vec<Blocker>,
    pub source_impact: SourceImpact,
    pub ready_for_cutover: bool,
    pub eta: Eta,
    pub retention_risk: RetentionRisk,
}

impl Status {
    /// Pure evaluation: a displayed regression is not a journal transition.
    /// All maps and range/blocker ordering are deterministic for JSON consumers.
    pub fn evaluate(mut input: Observations, policy: RetentionPolicy) -> Result<Self, Error> {
        input.rates.validate()?;
        let probe = &input.source_impact.latency_probe;
        if [probe.p50_ms, probe.p99_ms]
            .into_iter()
            .flatten()
            .any(|v| !v.is_finite() || v < 0.0)
        {
            return Err(Error("probe latencies must be finite and nonnegative"));
        }
        if probe.samples > 0 && probe.window_ms == 0 {
            return Err(Error("probe samples require a positive measurement window"));
        }
        if input.buffer.max_resident_segments == 0
            || input.buffer.resident_segments > input.buffer.max_resident_segments
        {
            return Err(Error(
                "resident segment count exceeds configured positive cap",
            ));
        }
        for ranges in input.chunks.values_mut() {
            ranges.sort_by(|a, b| (&a.range_lo, &a.range_hi).cmp(&(&b.range_lo, &b.range_hi)));
            if ranges
                .windows(2)
                .any(|r| r[0].range_lo == r[1].range_lo && r[0].range_hi == r[1].range_hi)
            {
                return Err(Error("duplicate chunk range"));
            }
        }
        let mut total = 0u64;
        let mut verified = 0u64;
        for coverage in input.verification.tables.values() {
            let classified = [
                coverage.verified_rows,
                coverage.busy_rows,
                coverage.mismatched_rows,
                coverage.excluded_rows,
            ]
            .into_iter()
            .try_fold(0u64, |sum, n| sum.checked_add(n))
            .ok_or(Error("coverage overflow"))?;
            if classified > coverage.total_rows {
                return Err(Error("coverage exceeds total rows"));
            }
            total = total
                .checked_add(coverage.total_rows - coverage.excluded_rows)
                .ok_or(Error("coverage overflow"))?;
            verified = verified
                .checked_add(coverage.verified_rows)
                .ok_or(Error("coverage overflow"))?;
        }
        let facts = &mut input.readiness;
        facts.all_chunks_done &= input
            .chunks
            .values()
            .flatten()
            .all(|c| c.state == ChunkState::Done);
        facts.capture_healthy &= input.slot.valid
            && input.slot.invalidation_reason.is_none()
            && !input.buffer.capture_paused_by_backpressure;
        if facts.in_scope_rows != total
            || facts.verified_rows != verified
            || input
                .chunks
                .keys()
                .any(|table| !input.verification.tables.contains_key(table))
        {
            input.blockers.push(blocker("verification-evidence-mismatch", "Table coverage does not match the independently declared in-scope and verified row counts or selected tables.", "Publish complete per-table coverage with the same denominator as the coordinator readiness facts."));
        }
        // Never let a smaller/incomplete status denominator promote readiness.
        facts.verified_rows = facts.verified_rows.min(verified);
        for code in facts.blockers() {
            input.blockers.push(blocker(
                &code,
                "A required readiness fact is not satisfied.",
                readiness_remediation(&code),
            ));
        }
        let coverage = &input.verification;
        if !(coverage.schema_verified
            && coverage.sequences_verified
            && coverage.constraints_verified
            && coverage.indexes_verified
            && coverage.foreign_keys_verified)
        {
            input.blockers.push(blocker(
                "verification-sections-incomplete",
                "Schema, sequence, constraint, index and FK coverage must all hold.",
                "Complete the independent verification sections and publish their evidence.",
            ));
        }
        if !input.slot.valid || input.slot.invalidation_reason.is_some() {
            input.blockers.push(blocker("slot-invalidated", "The replication slot no longer proves uninterrupted change coverage.", "Stop success/cutover; invalidate resumability and resnapshot into a new migration. Healthy throughput cannot repair lost coverage."));
        }
        let retention_risk = RetentionRisk::project(&input.slot, &input.rates, policy)?;
        if matches!(retention_risk.level, RiskLevel::Blocker | RiskLevel::Abort) {
            input.blockers.push(blocker(
                "retention-headroom",
                "Projected WAL retention or disk headroom is below the safety threshold.",
                retention_risk
                    .remediation
                    .as_deref()
                    .expect("risk remediation"),
            ));
        }
        if let Some(abort) = &retention_risk.abort {
            input.blockers.push(blocker(
                "abort-with-coverage-loss",
                "The declared retention abort threshold has been reached.",
                &abort.remediation,
            ));
        }
        input.blockers.sort_by(|a, b| {
            (&a.code, &a.reason, &a.remediation).cmp(&(&b.code, &b.reason, &b.remediation))
        });
        input.blockers.dedup();
        let ready_for_cutover = input.blockers.is_empty();
        let state = if input.state == State::VerifiedReady && !ready_for_cutover {
            State::CatchingUp
        } else {
            input.state
        };
        let eta = Eta::project(&input.buffer, &input.rates);
        Ok(Self {
            schema_version: 1,
            state,
            epoch: input.epoch,
            authority: input.authority,
            chunks: input.chunks,
            slot: input.slot,
            rates: input.rates,
            buffer: input.buffer,
            verification: input.verification,
            blockers: input.blockers,
            source_impact: input.source_impact,
            ready_for_cutover,
            eta,
            retention_risk,
        })
    }

    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }
}

fn readiness_remediation(code: &str) -> &'static str {
    match code {
        "chunks-incomplete" => {
            "Finish every in-scope table/range chunk and publish its completion."
        }
        "capture-unhealthy" => "Restore capture and check slot validity before attempting cutover.",
        "lineage-mismatch" => {
            "Stop resume and reconcile source lineage; resnapshot if continuity cannot be proved."
        }
        "apply-lag-over-budget" => {
            "Drain the applier backlog until lag is within the configured readiness budget."
        }
        "schema-boundary-pending" => {
            "Resolve the blocked DDL boundary or unknown schema job, then reverify."
        }
        "verification-incomplete" => {
            "Verify every in-scope row, including busy and mismatched rows, and publish coverage."
        }
        "busy-row-budget-exhausted" => {
            "Resolve busy rows past their retry budget and rerun verification."
        }
        "dead-letters-present" => "Resolve every dead-letter entry and reverify affected ranges.",
        "destination-schema-invalid" => "Validate destination indexes and constraints.",
        "sequences-not-syncable" => {
            "Make sequence synchronization safe and rerun its prerequisite checks."
        }
        "reverse-prerequisites-missing" => {
            "Provision and verify reverse-replication prerequisites."
        }
        "fence-risk-unacknowledged" => "Review and acknowledge all residual fence-plan risks.",
        "buffer-contract-mismatch" => {
            "Restore the configured buffer backend and failure-domain contract."
        }
        _ => "Reassess the independent readiness facts before cutover.",
    }
}
