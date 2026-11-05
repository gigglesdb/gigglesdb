#![doc = include_str!("../README.md")]

mod fixture;
mod peer;

use giggles_client::{Client, Operation, OperationId, Outcome};
pub use peer::Counters;
use serde::{Deserialize, Serialize};
use std::{
    fmt,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{net::TcpListener, sync::Notify, time::timeout};

macro_rules! scenarios {
    ($($variant:ident => $name:literal),+ $(,)?) => {
        /// Closed local-only scripts. No arbitrary target can be supplied.
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
        pub enum Scenario { $(#[serde(rename=$name)] $variant),+ }
        impl Scenario {
            pub const ALL: &'static [Self] = &[$(Self::$variant),+];
            pub fn name(self) -> &'static str { match self { $(Self::$variant => $name),+ } }
        }
        impl std::str::FromStr for Scenario {
            type Err = HarnessError;
            fn from_str(s: &str) -> Result<Self, Self::Err> { match s { $($name => Ok(Self::$variant)),+, _ => Err(HarnessError::Arguments) } }
        }
    }
}
scenarios! {
    Baseline => "baseline", Prefix1 => "prefix-1", Prefix2 => "prefix-2", Prefix3 => "prefix-3",
    BodyClose => "body-close", CompleteClose => "complete-close",
    ReplyPrefix1 => "reply-prefix-1", ReplyPrefix2 => "reply-prefix-2", ReplyPrefix3 => "reply-prefix-3", ReplyBody => "reply-body",
    OuterId => "outer-id", OuterVersion => "outer-version", ReplyVersion => "reply-version", InnerId => "inner-id", InnerVersion => "inner-version",
    ZeroLength => "zero-length", OversizeLength => "oversize-length", InvalidJson => "invalid-json", Both => "both", Neither => "neither",
    WrongName => "wrong-name", Untrusted => "untrusted", WrongAlpn => "wrong-alpn", NoAlpn => "no-alpn",
    TlsDeadline => "tls-deadline", ReadDeadline => "read-deadline", WritePressure => "write-pressure",
    AdmissionDeadline => "admission-deadline", CancelActive => "cancel-active", CancelAdmission => "cancel-admission",
    PrewriteFailover => "prewrite-failover", Unavailable => "unavailable", Indeterminate => "indeterminate", AbsentThenCommitted => "absent-then-committed",
    StatusInnerId => "status-inner-id", StatusInnerVersion => "status-inner-version", LostReplyStatus => "lost-reply-status"
}

/// Fixed error classes deliberately omit peer bytes and credential paths.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HarnessError {
    Arguments,
    Fixture,
    Io,
    Frame,
    Client,
    Peer,
    Invariant,
    Deadline,
}
impl fmt::Display for HarnessError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for HarnessError {}
impl From<std::io::Error> for HarnessError {
    fn from(_: std::io::Error) -> Self {
        Self::Io
    }
}

/// A safe outcome description; no request, response payload, credential or raw error.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Observation {
    pub class: String,
    pub reason: Option<String>,
    pub operation_id: Option<u64>,
    pub status_is_rollback: bool,
}
fn observe(outcome: &Outcome) -> Observation {
    let (class, reason, operation_id) = match outcome {
        Outcome::Recorded(r) => {
            let result = r.task_result();
            (
                if r.is_committed() {
                    "committed"
                } else if result.code == "absent" {
                    "absent"
                } else {
                    "recorded"
                },
                None,
                result.operation.map(|id| id.0),
            )
        }
        Outcome::Unknown {
            operation_id,
            reason,
        } => ("unknown", Some(format!("{reason:?}")), Some(operation_id.0)),
        Outcome::Denied(_) => ("denied", None, None),
        Outcome::Unavailable => ("unavailable", None, None),
        Outcome::TimeoutBeforeSend => ("timeout-before-send", None, None),
        Outcome::TlsRejected => ("tls-rejected", None, None),
        Outcome::Protocol => ("protocol", None, None),
    };
    Observation {
        class: class.into(),
        reason,
        operation_id,
        status_is_rollback: false,
    }
}

/// Versioned transcript with peer-observed counts and explicit limits.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Report {
    pub schema_version: u32,
    pub scenario: Scenario,
    pub seed: u64,
    pub operation_id: u64,
    pub observations: Vec<Observation>,
    pub counters: Counters,
    pub limits: Vec<String>,
    pub retained_fixture: PathBuf,
}
impl Report {
    /// Falsifiable shared invariants, also applicable to deserialized reports.
    pub fn validate(&self) -> Result<(), HarnessError> {
        let executes = self
            .counters
            .complete_execute
            .values()
            .try_fold(0u64, |a, n| a.checked_add(*n))
            .ok_or(HarnessError::Invariant)?;
        let statuses = self
            .counters
            .complete_status
            .values()
            .try_fold(0u64, |a, n| a.checked_add(*n))
            .ok_or(HarnessError::Invariant)?;
        let complete = executes
            .checked_add(statuses)
            .ok_or(HarnessError::Invariant)?;
        if self.schema_version != 1
            || self.operation_id != operation_id(self.seed)
            || self.observations.is_empty()
            || self.limits.is_empty()
            || executes > 1
            || statuses > 2
            || complete > self.counters.request_starts
            || self.counters.request_starts > self.counters.authenticated
            || self.counters.authenticated > self.counters.connections
            || self.counters.connections > 3
            || self.counters.request_starts > 1 + statuses
            || self
                .counters
                .complete_execute
                .keys()
                .chain(self.counters.complete_status.keys())
                .any(|id| *id != self.operation_id)
            || self.observations.iter().any(|o| {
                o.operation_id.is_some_and(|id| id != self.operation_id)
                    || o.status_is_rollback
                    || (matches!(o.class.as_str(), "unknown" | "committed" | "absent")
                        && o.operation_id != Some(self.operation_id))
            })
        {
            return Err(HarnessError::Invariant);
        }
        let classes: Vec<_> = self.observations.iter().map(|o| o.class.as_str()).collect();
        let expected: &[&str] = match self.scenario {
            Scenario::Baseline
            | Scenario::PrewriteFailover
            | Scenario::CancelActive
            | Scenario::CancelAdmission => &["committed"],
            Scenario::AbsentThenCommitted => &["absent", "committed"],
            Scenario::LostReplyStatus => &["unknown", "committed"],
            Scenario::AdmissionDeadline => &["timeout-before-send", "committed"],
            Scenario::WrongName | Scenario::Untrusted | Scenario::WrongAlpn | Scenario::NoAlpn => {
                &["tls-rejected"]
            }
            Scenario::TlsDeadline => &["timeout-before-send"],
            Scenario::StatusInnerId | Scenario::StatusInnerVersion => &["protocol"],
            _ => &["unknown"],
        };
        if classes != expected {
            return Err(HarnessError::Invariant);
        }
        let expected_reason = match self.scenario {
            Scenario::OuterId
            | Scenario::OuterVersion
            | Scenario::ReplyVersion
            | Scenario::InnerId
            | Scenario::InnerVersion
            | Scenario::ZeroLength
            | Scenario::OversizeLength
            | Scenario::InvalidJson
            | Scenario::Both
            | Scenario::Neither => Some("Protocol"),
            Scenario::Unavailable | Scenario::Indeterminate => Some("ServerIndeterminate"),
            Scenario::ReadDeadline | Scenario::WritePressure => Some("Timeout"),
            Scenario::Prefix1
            | Scenario::Prefix2
            | Scenario::Prefix3
            | Scenario::BodyClose
            | Scenario::CompleteClose
            | Scenario::ReplyPrefix1
            | Scenario::ReplyPrefix2
            | Scenario::ReplyPrefix3
            | Scenario::ReplyBody
            | Scenario::LostReplyStatus => Some("Transport"),
            _ => None,
        };
        if self
            .observations
            .iter()
            .enumerate()
            .any(|(i, o)| o.reason.as_deref() != if i == 0 { expected_reason } else { None })
            || self
                .counters
                .events
                .iter()
                .any(|e| e == "peer_write_failed")
        {
            return Err(HarnessError::Invariant);
        }
        let boundary = match self.scenario {
            Scenario::Prefix1 => "request_prefix_consumed_1",
            Scenario::Prefix2 => "request_prefix_consumed_2",
            Scenario::Prefix3 => "request_prefix_consumed_3",
            Scenario::BodyClose => "",
            Scenario::CompleteClose => "complete_execute",
            Scenario::ReplyPrefix1 => "reply_prefix_written_1",
            Scenario::ReplyPrefix2 => "reply_prefix_written_2",
            Scenario::ReplyPrefix3 => "reply_prefix_written_3",
            Scenario::ReplyBody => "reply_partial_body_written",
            Scenario::TlsDeadline => "tcp_accepted_tls_not_started",
            Scenario::ReadDeadline => "waiting_after_complete_execute",
            Scenario::WritePressure => "no_application_read",
            // A rejected handshake can finish before the peer records rejection.
            Scenario::WrongName | Scenario::Untrusted | Scenario::WrongAlpn | Scenario::NoAlpn => {
                ""
            }
            _ => "reply_script_boundary_reached",
        };
        if !boundary.is_empty() && !self.counters.events.iter().any(|e| e == boundary) {
            return Err(HarnessError::Invariant);
        }
        if self.scenario == Scenario::BodyClose {
            let prefix = format!("request_body_consumed_{}_of_", 1 + self.seed % 11);
            if !self.counters.events.iter().any(|e| {
                e.strip_prefix(&prefix)
                    .and_then(|s| s.parse::<u64>().ok())
                    .is_some_and(|size| size > 1 + self.seed % 11)
            }) {
                return Err(HarnessError::Invariant);
            }
        }
        let expected_counts = match self.scenario {
            Scenario::AbsentThenCommitted => (0, 2),
            Scenario::StatusInnerId | Scenario::StatusInnerVersion => (0, 1),
            Scenario::CancelActive
            | Scenario::CancelAdmission
            | Scenario::AdmissionDeadline
            | Scenario::LostReplyStatus => (1, 1),
            Scenario::WrongName
            | Scenario::Untrusted
            | Scenario::WrongAlpn
            | Scenario::NoAlpn
            | Scenario::TlsDeadline
            | Scenario::WritePressure
            | Scenario::Prefix1
            | Scenario::Prefix2
            | Scenario::Prefix3
            | Scenario::BodyClose => (0, 0),
            _ => (1, 0),
        };
        if (executes, statuses) != expected_counts {
            return Err(HarnessError::Invariant);
        }
        let partial = matches!(
            self.scenario,
            Scenario::Prefix1 | Scenario::Prefix2 | Scenario::Prefix3 | Scenario::BodyClose
        );
        let starts = complete + u64::from(partial);
        if self.counters.request_starts != starts || self.counters.connections != starts.max(1) {
            return Err(HarnessError::Invariant);
        }
        Ok(())
    }
    /// Persist JSON exclusively; previous reports and failed fixtures remain intact.
    pub fn retain(&self) -> Result<PathBuf, HarnessError> {
        let path = self.retained_fixture.join("report.json");
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;
        serde_json::to_writer_pretty(file, self).map_err(|_| HarnessError::Io)?;
        Ok(path)
    }
}

/// Deterministic identity, including u64 extremes without arithmetic overflow.
pub fn operation_id(seed: u64) -> u64 {
    let id = seed.rotate_left(17) ^ 0x4753_434f_4e46_0001;
    if id == 0 { 1 } else { id }
}

fn execute(id: u64, large: bool) -> Result<Operation, HarnessError> {
    let keys = if large {
        (0..1800)
            .map(|n| giggles_core::Key {
                tenant: giggles_core::TenantId(1),
                row: giggles_core::RowId(n),
            })
            .collect()
    } else {
        vec![]
    };
    Operation::execute(
        11,
        giggles_client::Request {
            operation: OperationId(id),
            selection: giggles_core::Selection::Keys(keys),
            column: giggles_core::Column::Status,
            after: giggles_core::Status::Queued,
        },
        None,
        1000,
    )
    .map_err(|_| HarnessError::Client)
}

struct PeerTask(Option<tokio::task::JoinHandle<Result<(), HarnessError>>>);
impl Drop for PeerTask {
    fn drop(&mut self) {
        if let Some(task) = &self.0 {
            task.abort();
        }
    }
}
impl PeerTask {
    async fn stop(mut self) -> Result<(), HarnessError> {
        let task = self.0.take().ok_or(HarnessError::Peer)?;
        task.abort();
        match task.await {
            Ok(Err(e)) => Err(e),
            Err(e) if !e.is_cancelled() => Err(HarnessError::Peer),
            _ => Ok(()),
        }
    }
}

/// Run one script against freshly generated loopback mTLS peers.
/// No argument controls a network target. The outer deadline is a harness bound,
/// not a performance qualification. Peer tasks are aborted and joined on return.
pub async fn run(scenario: Scenario, seed: u64) -> Result<Report, HarnessError> {
    let fixture = fixture::Fixture::new(
        scenario == Scenario::WrongName,
        scenario == Scenario::Untrusted,
        match scenario {
            Scenario::WrongAlpn => Some(b"other/1"),
            Scenario::NoAlpn => None,
            _ => Some(b"giggles-native/1"),
        },
    )?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let mut config = fixture.config;
    config.endpoints = vec![listener.local_addr()?];
    let _refused = if scenario == Scenario::PrewriteFailover {
        let socket = socket2::Socket::new(
            socket2::Domain::IPV4,
            socket2::Type::STREAM,
            Some(socket2::Protocol::TCP),
        )?;
        socket.bind(&std::net::SocketAddr::from(([127, 0, 0, 1], 0)).into())?;
        config.endpoints.insert(
            0,
            socket
                .local_addr()?
                .as_socket()
                .ok_or(HarnessError::Fixture)?,
        );
        Some(socket)
    } else {
        None
    };
    if matches!(
        scenario,
        Scenario::TlsDeadline | Scenario::ReadDeadline | Scenario::WritePressure
    ) {
        config.limits.max_attempts = 1;
        config.limits.request = Duration::from_millis(200);
        config.limits.connect = Duration::from_millis(150);
    }
    if matches!(
        scenario,
        Scenario::AdmissionDeadline | Scenario::CancelActive | Scenario::CancelAdmission
    ) {
        config.limits.operation = Duration::from_millis(700);
        config.limits.request = Duration::from_secs(2);
    }
    let client = Client::new(config).map_err(|_| HarnessError::Client)?;
    let id = operation_id(seed);
    let op = execute(id, scenario == Scenario::WritePressure)?;
    let state = Arc::new(Mutex::new(Counters::default()));
    let reached = Arc::new(Notify::new());
    let task = PeerTask(Some(tokio::spawn(peer::serve(
        listener,
        fixture.server,
        scenario,
        seed,
        state.clone(),
        reached.clone(),
    ))));
    let result = timeout(Duration::from_secs(8), async {
        let mut observations = Vec::new();
        if matches!(scenario, Scenario::StatusInnerId | Scenario::StatusInnerVersion) {
            let status = Operation::status(12, OperationId(id), 1000).map_err(|_| HarnessError::Client)?;
            observations.push(observe(&client.send(&status).await));
        } else if scenario == Scenario::AbsentThenCommitted {
            let status = Operation::status(12, OperationId(id), 1000).map_err(|_| HarnessError::Client)?;
            for _ in 0..2 { observations.push(observe(&client.send(&status).await)); }
        } else if matches!(scenario, Scenario::AdmissionDeadline | Scenario::CancelActive | Scenario::CancelAdmission) {
            let mut active = Box::pin(client.send(&op));
            tokio::select! {
                _ = reached.notified() => {},
                _ = &mut active => return Err(HarnessError::Peer),
            }
            let status = Operation::status(12, OperationId(id), 1000).map_err(|_| HarnessError::Client)?;
            if scenario == Scenario::AdmissionDeadline {
                // Active future is deliberately suspended while retaining its permit.
                observations.push(observe(&client.send(&status).await));
                drop(active);
            } else if scenario == Scenario::CancelAdmission {
                {
                    let waiter = client.send(&status);
                    tokio::pin!(waiter);
                    tokio::select! { _ = &mut waiter => return Err(HarnessError::Peer), _ = tokio::task::yield_now() => {} }
                }
                peer::event(&state, "caller_cancelled_admission_waiter");
                drop(active);
            } else { drop(active); }
            peer::event(&state, "caller_dropped_active_call");
            observations.push(observe(&client.send(&status).await));
        } else {
            let outcome = client.send(&op).await;
            if let Outcome::Unknown { .. } = &outcome {
                let resolution = outcome.resolution(12, 1000).map_err(|_| HarnessError::Client)?.ok_or(HarnessError::Invariant)?;
                if resolution.operation_id() != op.operation_id() { return Err(HarnessError::Invariant); }
                if scenario == Scenario::LostReplyStatus {
                    observations.push(observe(&outcome));
                    observations.push(observe(&client.send(&resolution).await));
                    return Ok(observations);
                }
            }
            observations.push(observe(&outcome));
        }
        Ok(observations)
    }).await;
    task.stop().await?;
    let observations = result.map_err(|_| HarnessError::Deadline)??;
    let counters = state.lock().map_err(|_| HarnessError::Peer)?.clone();
    let report = Report { schema_version: 1, scenario, seed, operation_id: id, observations, counters,
        retained_fixture: fixture.root,
        limits: vec!["Loopback scripted peer; no durable consensus commit evidence or production qualification".into(), "Peer consumed bytes do not identify client write/flush completion; no deterministic flush fault claimed".into(), "Seed fixes identity/script offsets; certificates, ephemeral ports and scheduling vary".into()] };
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn scenario_parse_rejects_unknown() {
        assert!("https://example.com".parse::<Scenario>().is_err());
        for s in Scenario::ALL {
            assert_eq!(s.name().parse::<Scenario>().unwrap(), *s);
        }
    }
    #[test]
    fn every_seed_is_repeatable() {
        for seed in [0, 1, 2, 42, u64::MAX] {
            assert_eq!(operation_id(seed), operation_id(seed));
            assert_ne!(operation_id(seed), operation_id(seed.wrapping_add(1)));
        }
        assert_ne!(operation_id(0x4753_434f_4e46_0001u64.rotate_right(17)), 0);
    }
    #[tokio::test]
    async fn report_rejects_replay_and_changed_id() {
        let valid = run(Scenario::Baseline, 17).await.unwrap();
        valid.validate().unwrap();
        let mut replay = valid.clone();
        *replay
            .counters
            .complete_execute
            .entry(valid.operation_id)
            .or_default() += 1;
        assert_eq!(replay.validate(), Err(HarnessError::Invariant));
        let mut changed = valid.clone();
        changed.observations[0].operation_id = Some(99);
        assert_eq!(changed.validate(), Err(HarnessError::Invariant));
        let mut overflow = valid.clone();
        overflow.counters.complete_status.insert(1, u64::MAX);
        overflow.counters.complete_status.insert(2, 1);
        assert_eq!(overflow.validate(), Err(HarnessError::Invariant));
        let mut false_success = valid.clone();
        false_success.observations[0].class = "unknown".into();
        assert_eq!(false_success.validate(), Err(HarnessError::Invariant));
        let mut rollback = valid;
        rollback.observations[0].status_is_rollback = true;
        assert_eq!(rollback.validate(), Err(HarnessError::Invariant));
    }

    #[tokio::test]
    async fn report_rejects_wrong_failure_reason_and_missing_script_boundary() {
        let valid = run(Scenario::OuterId, 42).await.unwrap();
        valid.validate().unwrap();
        let mut wrong = valid.clone();
        wrong.observations[0].reason = Some("Transport".into());
        assert_eq!(wrong.validate(), Err(HarnessError::Invariant));
        let mut missing = valid.clone();
        missing.counters.events.clear();
        assert_eq!(missing.validate(), Err(HarnessError::Invariant));
        let mut failed = valid;
        failed.counters.events.push("peer_write_failed".into());
        assert_eq!(failed.validate(), Err(HarnessError::Invariant));
    }

    #[tokio::test]
    async fn dropping_harness_peer_owner_aborts_pending_task() {
        struct Signal(Arc<Notify>);
        impl Drop for Signal {
            fn drop(&mut self) {
                self.0.notify_one();
            }
        }
        let stopped = Arc::new(Notify::new());
        let ready = Arc::new(Notify::new());
        let signal = stopped.clone();
        let started = ready.clone();
        let task = PeerTask(Some(tokio::spawn(async move {
            let _signal = Signal(signal);
            started.notify_one();
            std::future::pending::<()>().await;
            Ok(())
        })));
        ready.notified().await;
        drop(task);
        timeout(Duration::from_secs(1), stopped.notified())
            .await
            .unwrap();
    }
}
