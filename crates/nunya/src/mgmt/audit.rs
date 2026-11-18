//! Lifecycle audit boundary and fail-closed read-log adapter.
use serde::Serialize;

/// Fixed lifecycle event kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleKind {
    /// A new session was issued.
    Issued,
    /// File policy imported.
    PolicyImported,
    /// Policy draft applied.
    PolicyApplied,
    /// Policy revision restored.
    PolicyRolledBack,
    /// A session was renewed.
    Renewed,
    /// A session was revoked.
    Revoked,
    /// A principal was disabled.
    PrincipalDisabled,
    /// An ended session could not resume.
    ResumeRefused,
}

/// Non-secret identity of the mutation caller.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Actor {
    /// Session or principal owner.
    Owner,
    /// Store administrator principal in hex.
    Admin(String),
    /// Configuration administrator, without its name.
    ConfigAdmin,
    /// Trusted local socket.
    Socket,
}

/// Data-minimal session lifecycle evidence.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct LifecycleRecord {
    /// Optional policy mutation evidence.
    #[serde(skip_serializing_if="Option::is_none")]
    pub policy: Option<PolicyAudit>,
    /// Fixed event kind.
    pub kind: LifecycleKind,
    /// Authority clock timestamp.
    pub at_ms: u64,
    /// Affected principal identifier in hex.
    pub principal: String,
    /// Affected session identifier in hex, if any.
    pub session: Option<String>,
    /// Affected route, if any.
    pub route: Option<String>,
    /// Mutation caller category.
    pub actor: Actor,
    /// Fixed, data-free reason.
    pub reason: &'static str,
}

/// The lifecycle sink could not accept a record.
#[derive(Debug, PartialEq, Eq)]
pub struct AuditUnavailable;

/// Small adapter boundary for durable lifecycle recording.
pub trait LifecycleSink: Send + Sync {
    /// Accept one record or refuse without exposing sink details.
    fn record(&self, record: &LifecycleRecord) -> Result<(), AuditUnavailable>;
}

/// Forward management lifecycle records to the service read log.
pub struct ReadLogSink<L>(pub L);
impl<L: crate::readlog::ReadLog> LifecycleSink for ReadLogSink<L> {
    fn record(&self, record: &LifecycleRecord) -> Result<(), AuditUnavailable> {
        use crate::readlog::{LifecycleActor as A, LifecycleKind as K};
        let kind = match record.kind {
            LifecycleKind::PolicyImported => K::PolicyImported,
            LifecycleKind::PolicyApplied => K::PolicyApplied,
            LifecycleKind::PolicyRolledBack => K::PolicyRolledBack,
            LifecycleKind::Issued => K::Issued,
            LifecycleKind::Renewed => K::Renewed,
            LifecycleKind::Revoked => K::Revoked,
            LifecycleKind::PrincipalDisabled => K::PrincipalDisabled,
            LifecycleKind::ResumeRefused => K::ResumeRefused,
        };
        let actor = match &record.actor {
            Actor::Owner => A::Owner,
            Actor::Admin(id) => A::Admin(id.clone()),
            Actor::ConfigAdmin => A::ConfigAdmin,
            Actor::Socket => A::Socket,
        };
        self.0.write_lifecycle(&crate::readlog::LifecycleRecord {
            kind, policy: record.policy.clone(), actor, at_ms: record.at_ms, principal: record.principal.clone(),
            session: record.session.clone(), route: record.route.clone(),
            connection: None, reason: record.reason,
        }).map_err(|_| AuditUnavailable)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::readlog::{self, ReadLog};
    use std::sync::Mutex;
    #[derive(Default)]
    struct Log { records: Mutex<Vec<readlog::LifecycleRecord>>, fail: bool }
    impl ReadLog for Log {
        fn write(&self, _: &readlog::ReadLogRecord) -> Result<(), crate::error::Refusal> {
            unreachable!("only lifecycle writes expected")
        }
        fn write_lifecycle(&self, record: &readlog::LifecycleRecord) -> Result<(), crate::error::Refusal> {
            if self.fail {
                return Err(crate::error::Refusal::new(crate::error::NyCode::ReadLogUnavailable, "private failure"));
            }
            self.records.lock().unwrap().push(record.clone());
            Ok(())
        }
    }
    fn record() -> LifecycleRecord {
        LifecycleRecord { kind: LifecycleKind::Issued, actor: Actor::Owner, at_ms: 17,
            principal: "principal".into(), session: Some("session".into()),
            route: Some("postgres".into()), policy: None, reason: "requested" }
    }
    #[test]
    fn maps_all_kinds_and_actors_and_preserves_metadata() {
        use readlog::{LifecycleActor as A, LifecycleKind as K};
        let sink = ReadLogSink(Log::default());
        for (kind, expected_kind) in [
            (LifecycleKind::Issued, K::Issued), (LifecycleKind::Renewed, K::Renewed),
            (LifecycleKind::Revoked, K::Revoked), (LifecycleKind::PrincipalDisabled, K::PrincipalDisabled),
            (LifecycleKind::ResumeRefused, K::ResumeRefused),
        ] {
            for (actor, expected_actor) in [(Actor::Owner, A::Owner),
                (Actor::Admin("admin".into()), A::Admin("admin".into())),
                (Actor::ConfigAdmin, A::ConfigAdmin), (Actor::Socket, A::Socket)] {
                let record = LifecycleRecord {kind, actor, ..record()};
                sink.record(&record).unwrap();
                let records = sink.0.records.lock().unwrap();
                let saved = records.last().unwrap();
                assert_eq!(saved.kind, expected_kind);
                assert_eq!(saved.actor, expected_actor);
                assert_eq!(saved.at_ms, record.at_ms);
                assert_eq!(saved.principal, record.principal);
                assert_eq!(saved.session, record.session);
                assert_eq!(saved.route, record.route);
                assert_eq!(saved.reason, record.reason);
                assert_eq!(saved.connection, None);
            }
        }
        assert_eq!(sink.0.records.lock().unwrap().len(), 20);
    }
    #[test]
    fn policy_payload_reaches_readlog_without_reason_overloading() {
        let sink = ReadLogSink(Log::default());
        for kind in [LifecycleKind::PolicyImported, LifecycleKind::PolicyApplied, LifecycleKind::PolicyRolledBack] {
            let mut rec = record(); rec.kind = kind;
            rec.policy = Some(PolicyAudit { revision: Some(4), outcome: "active", summary: Default::default(), newly_visible: vec![], newly_masked: vec![] });
            sink.record(&rec).unwrap();
            let records = sink.0.records.lock().unwrap();
            assert_eq!(records.last().unwrap().policy, rec.policy);
            assert_eq!(records.last().unwrap().reason, "requested");
        }
    }
    #[test]
    fn every_write_failure_is_unavailable() {
        let sink = ReadLogSink(Log { fail: true, ..Log::default() });
        assert_eq!(sink.record(&record()), Err(AuditUnavailable));
    }
}

/// Value-free policy mutation audit payload.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct PolicyAudit {
    /// Newly active revision, when publication succeeded.
    pub revision: Option<u64>,
    /// Fixed operation outcome.
    pub outcome: &'static str,
    /// Diff list counts.
    pub summary: crate::policy::diff::Summary,
    /// Column names newly made visible.
    pub newly_visible: Vec<serde_json::Value>,
    /// Column names newly masked.
    pub newly_masked: Vec<serde_json::Value>,
}
