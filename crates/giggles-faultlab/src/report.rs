//! Independent bounded transcript checks; unknown execution is never an abort.
use crate::{
    Result,
    proxy::{Direction, Fault, Trace},
};
use giggles_client::Outcome;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

pub const LOCAL_SCOPE: &str = "LOCAL simulation only; synthetic domains; no cloud/AZ, production, security or performance qualification";
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Phase {
    Healthy,
    DomainLoss,
    NoQuorum,
    Restored,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Action {
    Execute,
    Status,
    Replay,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum Observation {
    Recorded {
        sdk_validated_correlation_id: u64,
        reply: Value,
    },
    Unknown {
        business_id: u64,
        reason: String,
    },
    Denied {
        reason: String,
    },
    Unavailable,
    TimeoutBeforeSend,
    TlsRejected,
    Protocol,
}
impl Observation {
    pub fn from_outcome(o: Outcome, correlation_id: u64) -> Self {
        match o {
            Outcome::Recorded(r) => Self::Recorded {
                sdk_validated_correlation_id: correlation_id,
                reply: serde_json::to_value(r.into_reply()).expect("reply serializes"),
            },
            Outcome::Unknown {
                operation_id,
                reason,
            } => Self::Unknown {
                business_id: operation_id.0,
                reason: format!("{reason:?}"),
            },
            Outcome::Denied(d) => Self::Denied {
                reason: format!("{d:?}"),
            },
            Outcome::Unavailable => Self::Unavailable,
            Outcome::TimeoutBeforeSend => Self::TimeoutBeforeSend,
            Outcome::TlsRejected => Self::TlsRejected,
            Outcome::Protocol => Self::Protocol,
        }
    }
    pub fn committed(&self) -> bool {
        matches!(self,Self::Recorded{reply,..} if reply["code"]==0 && reply["data"]["code"]=="committed")
    }
    pub fn data(&self) -> Option<&Value> {
        match self {
            Self::Recorded { reply, .. } => Some(&reply["data"]),
            _ => None,
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Invocation {
    pub phase: Phase,
    pub action: Action,
    pub correlation_id: u64,
    pub business_id: u64,
    pub start_micros: u64,
    pub end_micros: u64,
    pub request: Value,
    pub outcome: Observation,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Distribution {
    pub samples: usize,
    pub min: u64,
    pub p50: u64,
    pub p95: u64,
    pub p99: u64,
    pub max: u64,
}
pub fn distribution(samples: &[u64]) -> Option<Distribution> {
    if samples.is_empty() {
        return None;
    }
    let mut s = samples.to_vec();
    s.sort_unstable();
    let rank = |p: usize| s[(s.len() * p).div_ceil(100) - 1];
    Some(Distribution {
        samples: s.len(),
        min: s[0],
        p50: rank(50),
        p95: rank(95),
        p99: rank(99),
        max: *s.last().expect("nonempty"),
    })
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Summary {
    pub attempts: usize,
    pub committed_responses: usize,
    pub unknown_responses: usize,
    pub other_responses: usize,
    pub latency_micros: BTreeMap<Phase, Distribution>,
}
pub fn summarize(invocations: &[Invocation]) -> Summary {
    let mut grouped = BTreeMap::<Phase, Vec<u64>>::new();
    for i in invocations {
        grouped
            .entry(i.phase)
            .or_default()
            .push(i.end_micros.saturating_sub(i.start_micros));
    }
    let committed_responses = invocations.iter().filter(|i| i.outcome.committed()).count();
    let unknown_responses = invocations
        .iter()
        .filter(|i| matches!(i.outcome, Observation::Unknown { .. }))
        .count();
    Summary {
        attempts: invocations.len(),
        committed_responses,
        unknown_responses,
        other_responses: invocations.len() - committed_responses - unknown_responses,
        latency_micros: grouped
            .into_iter()
            .map(|(k, v)| (k, distribution(&v).expect("nonempty")))
            .collect(),
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CatchUp {
    pub node: u64,
    pub applied: u64,
    pub diagnostics: Value,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Report {
    pub version: u32,
    pub scope: String,
    pub node_binary_sha256: String,
    pub host_os: String,
    pub host_arch: String,
    pub samples_per_phase: usize,
    pub domains: BTreeMap<u64, String>,
    pub isolated_node: u64,
    pub initial_leader: u64,
    pub phase_nodes: BTreeMap<Phase, u64>,
    pub invocations: Vec<Invocation>,
    pub summary: Summary,
    pub traces: BTreeMap<String, Trace>,
    pub final_inspection: Value,
    pub catch_up: Vec<CatchUp>,
    pub barrier: u64,
}
impl Report {
    pub fn validate(&self) -> Result<()> {
        if self.version != 1
            || self.scope != LOCAL_SCOPE
            || !(1..=8).contains(&self.samples_per_phase)
            || self.domains.len() != 3
            || self.domains.keys().copied().collect::<Vec<_>>() != [1, 2, 3]
            || !self.domains.contains_key(&self.isolated_node)
            || self.node_binary_sha256.len() != 64
            || !self
                .node_binary_sha256
                .bytes()
                .all(|b| b.is_ascii_hexdigit())
            || self.host_os.is_empty()
            || self.host_arch.is_empty()
        {
            return Err("invalid local report envelope".into());
        }
        if self.invocations.is_empty()
            || self.invocations.len() > 256
            || self.summary != summarize(&self.invocations)
        {
            return Err("summary does not match all attempts".into());
        }
        if self.phase_nodes.len() != 4
            || self.phase_nodes.get(&Phase::Healthy) != Some(&self.initial_leader)
            || self
                .phase_nodes
                .values()
                .any(|n| !self.domains.contains_key(n))
            || self.phase_nodes.get(&Phase::DomainLoss) == Some(&self.isolated_node)
            || self.phase_nodes.get(&Phase::NoQuorum) == Some(&self.isolated_node)
        {
            return Err("invalid phase routing evidence".into());
        }
        let mut ids = BTreeSet::new();
        let mut executed = BTreeMap::<u64, &Invocation>::new();
        let mut results = BTreeMap::<u64, &Value>::new();
        for i in &self.invocations {
            if !ids.insert(i.correlation_id)
                || i.end_micros < i.start_micros
                || i.request["id"] != i.correlation_id
            {
                return Err("invalid invocation identity/timing".into());
            }
            let request: giggles_consensus::native::Request =
                serde_json::from_value(i.request.clone())?;
            let correct_action = match &request.action {
                giggles_consensus::TaskAction::Execute(..) => i.action != Action::Status,
                giggles_consensus::TaskAction::Status(_) => i.action == Action::Status,
                _ => false,
            };
            let op = giggles_client::Operation::from_request(request)?;
            if op.operation_id().0 != i.business_id {
                return Err("request business ID mismatch".into());
            }
            if !correct_action {
                return Err("action mismatch".into());
            }
            if let Observation::Unknown { business_id, .. } = &i.outcome
                && *business_id != i.business_id
            {
                return Err("unknown ID mismatch".into());
            }
            if let Observation::Recorded {
                reply,
                sdk_validated_correlation_id,
            } = &i.outcome
            {
                if *sdk_validated_correlation_id != i.correlation_id {
                    return Err("response correlation mismatch".into());
                }
                let data: giggles_core::replica::NativeResponse =
                    serde_json::from_value(reply["data"].clone())?;
                if data.version != 1 || data.operation.is_some_and(|v| v.0 != i.business_id) {
                    return Err("response business ID mismatch".into());
                }
                if i.outcome.committed() {
                    if data.operation.map(|o| o.0) != Some(i.business_id)
                        || data.count != Some(1)
                        || data.digest.as_ref().is_none_or(|d| {
                            d.len() != 64 || !d.bytes().all(|b| b.is_ascii_hexdigit())
                        })
                    {
                        return Err("invalid committed effect evidence".into());
                    }
                    if let Some(previous) = results.insert(i.business_id, &reply["data"])
                        && previous != &reply["data"]
                    {
                        return Err("replay/status changed recorded result".into());
                    }
                }
            }
            if i.action == Action::Execute && executed.insert(i.business_id, i).is_some() {
                return Err("duplicate logical execute".into());
            }
            if i.phase == Phase::NoQuorum && i.outcome.committed() {
                return Err("no-quorum committed response".into());
            }
        }
        for phase in [
            Phase::Healthy,
            Phase::DomainLoss,
            Phase::NoQuorum,
            Phase::Restored,
        ] {
            let samples = self
                .invocations
                .iter()
                .filter(|i| i.phase == phase && i.action == Action::Execute)
                .collect::<Vec<_>>();
            if samples.len() != self.samples_per_phase {
                return Err("missing phase execute samples".into());
            }
            if phase != Phase::NoQuorum && samples.iter().any(|i| !i.outcome.committed()) {
                return Err("available phase did not commit every sample".into());
            }
        }
        for (business, execute) in &executed {
            let statuses = self
                .invocations
                .iter()
                .filter(|i| {
                    i.action == Action::Status
                        && i.business_id == *business
                        && i.phase == Phase::Restored
                        && i.start_micros >= execute.end_micros
                })
                .collect::<Vec<_>>();
            if statuses.is_empty() {
                return Err("missing restored status reconciliation".into());
            }
            if execute.outcome.committed() && !statuses.iter().any(|i| i.outcome.committed()) {
                return Err("acknowledged commit not recovered".into());
            }
            if matches!(execute.outcome, Observation::Unknown { .. })
                && !statuses.iter().any(|i| {
                    i.outcome.committed() || i.outcome.data().is_some_and(|v| v["code"] == "absent")
                })
            {
                return Err("ambiguous execute unresolved".into());
            }
        }
        // No invented status-only successes. Replay must preserve exact action payload.
        for i in &self.invocations {
            let original = executed.get(&i.business_id).ok_or("orphan status/replay")?;
            if i.action == Action::Replay
                && (i.request["action"] != original.request["action"] || !i.outcome.committed())
            {
                return Err("replay differs or did not commit".into());
            }
        }
        let transitions = self.final_inspection["reply"]["data"]["tasks"][0][1]["transitions"]
            .as_u64()
            .ok_or("missing actual transitions")?;
        if transitions != results.len() as u64 {
            return Err("actual effect count differs from logical commits".into());
        }
        let records = self.final_inspection["reply"]["data"]["operations"]
            .as_array()
            .ok_or("missing actual receipts")?;
        if records.len() != results.len() {
            return Err("receipt count differs from logical commits".into());
        }
        let mut receipt_ids = BTreeSet::new();
        for record in records {
            let receipt: giggles_core::Receipt =
                serde_json::from_value(record[1]["decision"]["Ok"].clone())?;
            if record[0] != serde_json::json!([1, receipt.operation.0])
                || !receipt_ids.insert(receipt.operation.0)
                || receipt.domain != 42
                || receipt.task.0 != 1
                || receipt.executor.0 != 2
                || receipt.charged_transitions != 1
                || receipt.effects.len() != 1
            {
                return Err("invalid actual receipt binding/count".into());
            }
            let original = executed.get(&receipt.operation.0).ok_or("orphan receipt")?;
            let request: giggles_consensus::native::Request =
                serde_json::from_value(original.request.clone())?;
            let giggles_consensus::TaskAction::Execute(request, _) = request.action else {
                return Err("receipt has non-execute request".into());
            };
            if receipt.payload != request.canonical() {
                return Err("receipt payload differs from original request".into());
            }
            let effect = &receipt.effects[0];
            if request.selection != giggles_core::Selection::Keys(vec![effect.key])
                || effect.before.status != giggles_core::Status::Failed
                || effect.after.status != giggles_core::Status::Queued
                || effect.before.version.checked_add(1) != Some(effect.after.version)
            {
                return Err("receipt effects differ from requested single-row transition".into());
            }
            let digest = Sha256::digest(serde_json::to_vec(&receipt.effects)?)
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>();
            if results
                .get(&receipt.operation.0)
                .is_none_or(|v| v["digest"] != digest)
            {
                return Err("actual receipt digest differs from SDK result".into());
            }
        }
        if self.barrier == 0
            || self.catch_up.len() != 3
            || self
                .catch_up
                .iter()
                .map(|c| c.node)
                .collect::<BTreeSet<_>>()
                != BTreeSet::from([1, 2, 3])
            || self.catch_up.iter().any(|c| c.applied < self.barrier)
        {
            return Err("catch-up incomplete".into());
        }
        if self.traces.len() != 9 {
            return Err("missing proxy traces".into());
        }
        let expected = (1..=3)
            .flat_map(|from| {
                (1..=3)
                    .filter(move |to| *to != from)
                    .map(move |to| format!("peer-{from}-{to}"))
            })
            .chain((1..=3).map(|id| format!("sdk-{id}")))
            .collect::<BTreeSet<_>>();
        if self.traces.keys().cloned().collect::<BTreeSet<_>>() != expected {
            return Err("incorrect proxy trace identities".into());
        }
        if !self.traces.values().any(|t| {
            t.events
                .iter()
                .any(|e| matches!(e.fault, Some(Fault::Delay { millis: 15 })))
        }) {
            return Err("missing delay injection record".into());
        }
        for (name, trace) in &self.traces {
            if trace.overflow || trace.events.is_empty() || trace.events.len() > 65536 {
                return Err("proxy trace incomplete".into());
            }
            if trace.events.last().is_none_or(|e| e.kind != "stopped") {
                return Err("proxy trace not finalized".into());
            }
            for direction in [Direction::Upstream, Direction::Downstream] {
                let has = |fault| {
                    trace.events.iter().any(|e| {
                        e.kind == "control"
                            && e.direction == Some(direction)
                            && e.fault == Some(fault)
                    })
                };
                if !has(Fault::Pass)
                    || ((name.starts_with("peer-")
                        || *name == format!("sdk-{}", self.isolated_node))
                        && !has(Fault::Disconnect))
                {
                    return Err("missing injected fault/restore evidence".into());
                }
            }
            let mut time = 0;
            let mut live = BTreeSet::new();
            let mut seen = BTreeSet::new();
            let mut read = BTreeMap::<(u64, u8), usize>::new();
            let mut forwarded = BTreeMap::<(u64, u8), usize>::new();
            for (n, e) in trace.events.iter().enumerate() {
                if e.sequence != n as u64 || e.micros < time || e.bytes > 65536 {
                    return Err("invalid proxy event sequence".into());
                }
                match e.kind.as_str() {
                    "accepted" => {
                        if e.connection == 0 || !seen.insert(e.connection) {
                            return Err("duplicate proxy connection".into());
                        }
                        live.insert(e.connection);
                    }
                    "rejected" => {
                        if e.connection == 0 || !seen.insert(e.connection) {
                            return Err("duplicate rejected connection".into());
                        }
                    }
                    "closed" | "io_error" | "lifetime" | "disconnected" => {
                        if !live.remove(&e.connection) {
                            return Err("orphan proxy terminal event".into());
                        }
                    }
                    "read" | "forwarded" | "eof" => {
                        if !live.contains(&e.connection) {
                            return Err("orphan proxy byte event".into());
                        }
                        let d = match e.direction {
                            Some(Direction::Upstream) => 0,
                            Some(Direction::Downstream) => 1,
                            None => return Err("byte event missing direction".into()),
                        };
                        let k = (e.connection, d);
                        if e.kind == "read" {
                            *read.entry(k).or_default() += e.bytes;
                        } else if e.kind == "forwarded" {
                            *forwarded.entry(k).or_default() += e.bytes;
                            if forwarded[&k] > *read.get(&k).unwrap_or(&0) {
                                return Err("forwarded more than read".into());
                            }
                        }
                    }
                    "control" => {
                        if e.connection != 0 || e.direction.is_none() || e.fault.is_none() {
                            return Err("invalid control event".into());
                        }
                    }
                    "stopped" => {
                        if e.connection != 0 || n + 1 != trace.events.len() {
                            return Err("invalid stopped event".into());
                        }
                    }
                    _ => return Err("unknown proxy event".into()),
                }
                time = e.micros;
            }
            if !live.is_empty() {
                return Err("unfinished proxy connections".into());
            }
        }
        Ok(())
    }
}
