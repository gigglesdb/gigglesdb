//! Runner facts with explicit producer gaps. Nothing here caches readiness.
use crate::{
    MigrationDefinition,
    buffer::backends::{BackendReport, BufferStore, HostLoss},
    definition::BufferBackend,
    status::Blocker,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct Fact {
    pub ready: bool,
    pub source: String,
    pub staleness: String,
    pub blocker: Option<Blocker>,
}
fn missing(code: &str, source: &str, reason: &str, next: &str) -> Fact {
    Fact {
        ready: false,
        source: source.into(),
        staleness: "No current authoritative observation; fail closed on every evaluation.".into(),
        blocker: Some(Blocker {
            code: code.into(),
            reason: reason.into(),
            remediation: next.into(),
        }),
    }
}
fn contract_matches(definition: &MigrationDefinition, actual: &BackendReport) -> bool {
    let (backend, host_loss) = match definition.buffer.backend {
        BufferBackend::LocalFs(_) => ("local-fs", HostLoss::RecoveryRequiredThenResnapshot),
        BufferBackend::S3Compatible(_) => ("s3-compatible", HostLoss::SurvivedByService),
        BufferBackend::Gcs(_) => ("gcs", HostLoss::SurvivedByService),
    };
    actual.backend == backend
        && actual.failure_domain.process_crash_survived
        && actual.failure_domain.host_loss == host_loss
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct Facts {
    #[serde(default)]
    pub destination_expires_at_ms: Option<u64>,
    pub destination_validity: Fact,
    pub sequences_syncable: Fact,
    pub reverse_prerequisites: Fact,
    pub apply_lag: Fact,
    pub schema_jobs: Fact,
    pub fence_risks: Fact,
    pub buffer_contract: Fact,
    pub buffer_report: Option<BackendReport>,
    /// Diagnostic only, retained across owner exit; never readiness authority.
    #[serde(default)]
    pub schema_boundary: Option<Blocker>,
}
impl Facts {
    pub(super) fn compose_live(
        &mut self,
        definition: Option<&MigrationDefinition>,
        live: &crate::engine::LiveReadinessObservation,
    ) {
        self.apply_lag.ready = false;
        self.schema_jobs.ready = false;
        self.schema_boundary = None;
        self.apply_lag.source = "Fresh native source tip and clock, covered by a manifest-durable transactional heartbeat; authenticated journaled Applied source receipt; captured source commit timestamps.".into();
        self.apply_lag.staleness = "Expires 1 second from probe start; capture/apply exit revokes immediately; startup backlog remains unknown until its buffered baseline is applied.".into();
        self.apply_lag.blocker = Some(Blocker {
            code: "apply-lag-over-budget".into(),
            reason: "Paired source/apply lag age is unknown, stale, has a different lineage, or startup backlog has not drained.".into(),
            remediation: "Restore live capture and apply, let the startup buffer baseline drain, and obtain a fresh captured source heartbeat.".into(),
        });
        if let Some(reason) = &live.blocked {
            self.apply_lag.blocker.as_mut().expect("blocked lag").reason =
                format!("Native readiness observation blocked: {reason:?}.");
        }
        if let (Some(lag), Some(definition)) = (&live.lag, definition) {
            let budget_ms = definition
                .readiness_policy
                .max_lag_seconds
                .min(definition.budgets.max_replication_lag_seconds)
                .saturating_mul(1000);
            self.apply_lag.source = format!(
                "{} source_tip={:?}, applied_source={:?}, lag={} {}, oldest_unapplied_age_ms={}, budget_ms={budget_ms}.",
                self.apply_lag.source,
                lag.source_tip,
                lag.applied_source,
                lag.lag.value,
                lag.lag.unit,
                lag.oldest_unapplied_age_ms
            );
            self.apply_lag.ready =
                live.running && live.blocked.is_none() && lag.within_budget(definition);
            if self.apply_lag.ready {
                self.apply_lag.blocker = None;
            } else if let Some(blocker) = &mut self.apply_lag.blocker {
                blocker.reason = format!(
                    "Apply age {} ms exceeds budget {budget_ms} ms ({} {} behind), or its observation has expired.",
                    lag.oldest_unapplied_age_ms, lag.lag.value, lag.lag.unit
                );
                blocker.remediation = "Allow destination apply to catch up, reduce source write load, and resample fresh source/apply observations.".into();
            }
        }
        self.schema_jobs.source = "Running source::Capture::schema_state: SchemaVersions::blocker and unclassified/failed DDL state.".into();
        self.schema_jobs.staleness = "Sampled every 50 ms; expires after 1 second; capture/apply exit revokes immediately. Last blocked boundary is retained for diagnosis.".into();
        self.schema_jobs.blocker = Some(Blocker {
            code: "schema-boundary-pending".into(),
            reason: "Capture schema state is unavailable, stale, or its owner has exited.".into(),
            remediation: "Restore capture from a qualified schema baseline, then resample its live DDL state.".into(),
        });
        if let Some(schema) = &live.schema {
            self.schema_jobs.ready = live.running
                && live.schema_fresh
                && schema.blocked_boundary.is_none()
                && !schema.unknown_schema_job;
            if self.schema_jobs.ready {
                self.schema_jobs.blocker = None;
            } else if let Some(boundary) = &schema.blocked_boundary {
                self.schema_jobs.blocker = Some(Blocker {
                    code: "schema-boundary-pending".into(),
                    reason: format!(
                        "DDL boundary at xid {}, schema version {}: {} (capture/apply running: {}).",
                        boundary.xid, boundary.schema_version, boundary.reason, live.running
                    ),
                    remediation: boundary.remediation.into(),
                });
                self.schema_boundary = self.schema_jobs.blocker.clone();
            } else if schema.unknown_schema_job {
                self.schema_jobs.blocker.as_mut().expect("blocked schema").reason = "Capture has failed or a DDL transaction has not yet been classified; schema-job state is unknown.".into();
            }
        }
    }
    /// Start with unavailable facts; compose_live supplies only fresh,
    /// lifecycle-bound observations independently of verification completion.
    pub(super) fn sample(
        definition: Option<&MigrationDefinition>,
        buffer: Option<&dyn BufferStore>,
    ) -> Self {
        let report = buffer.map(BufferStore::report);
        let matches = definition
            .zip(report.as_ref())
            .is_some_and(|(def, actual)| contract_matches(def, actual));
        let mut buffer_contract = missing(
            "buffer-contract-mismatch",
            "Actual BufferStore::report versus the current definition backend and its documented failure domain.",
            "No current buffer report matches the definition backend and process-crash/host-loss contract.",
            "Restore a live buffer matching the definition: local-fs survives process crash and requires recovery/resnapshot after host loss; S3/GCS must also survive host loss.",
        );
        if report.is_some() && definition.is_some() {
            buffer_contract.staleness = "Read directly on every readiness evaluation; the published owner status expires after 250 ms.".into();
        }
        if matches {
            buffer_contract.ready = true;
            buffer_contract.blocker = None;
        }
        Self {
            destination_expires_at_ms: None,
            destination_validity: super::destination_readiness::Checks::missing().validity,
            sequences_syncable: super::destination_readiness::Checks::missing().sequences,
            reverse_prerequisites: super::destination_readiness::Checks::missing().reverse,
            apply_lag: missing(
                "apply-lag-over-budget",
                "Fresh paired current source WAL tip, committed applied source LSN and oldest-unapplied transaction age are unavailable.",
                "Apply lag age is unknown; LSN distance in bytes is diagnostic and cannot satisfy a seconds budget.",
                "Restore live capture/apply and a fresh captured source heartbeat, then allow the initial buffered baseline to drain.",
            ),
            schema_jobs: missing(
                "schema-boundary-pending",
                "Running source::Capture::schema_state observation is unavailable.",
                "No fresh running capture DDL boundary/schema-job observation is available.",
                "Restore capture from a qualified schema baseline and resample its live classifier state.",
            ),
            fence_risks: missing(
                "fence-risk-unacknowledged",
                "Read-only source catalog inventory is unavailable, expired, or its owner/definition/scope no longer matches.",
                "No fresh source writer inventory is available; existing acknowledgment strings cannot establish readiness.",
                "Restore the live source owner and refresh its catalog inventory, then review every named risk and acknowledge its exact stable id or add revokable direct writers to application_roles.",
            ),
            buffer_contract,
            buffer_report: report,
            schema_boundary: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{buffer::backends::ObjectBufferStore, definition::LocalFsOptions};
    use std::{path::PathBuf, time::Duration};

    fn local() -> (MigrationDefinition, ObjectBufferStore) {
        let root = PathBuf::from(
            std::env::var_os("SKEDADDLE_TEST_ROOT").expect("set SKEDADDLE_TEST_ROOT"),
        )
        .join(format!("runner-buffer-readiness-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let mut definition = MigrationDefinition::from_json(
            include_str!("../../README.md")
                .split("```json")
                .nth(1)
                .unwrap()
                .split("```")
                .next()
                .unwrap(),
        )
        .unwrap();
        definition.buffer.backend = BufferBackend::LocalFs(LocalFsOptions {
            path: root.to_str().unwrap().into(),
        });
        let store = ObjectBufferStore::local(root, Duration::from_secs(3)).unwrap();
        (definition, store)
    }
    #[test]
    fn actual_local_buffer_matches_and_declared_cloud_backend_blocks() {
        let (mut definition, store) = local();
        assert!(
            Facts::sample(Some(&definition), Some(&store))
                .buffer_contract
                .ready
        );
        // Change only the declared backend, preserving an actual local store.
        definition.buffer.backend = serde_json::from_value(serde_json::json!({
            "type": "gcs",
            "options": {"bucket": "readiness-test", "prefix": "readiness", "credentials_ref": "env:READINESS_TEST_GCS"}
        }))
        .unwrap();
        let facts = Facts::sample(Some(&definition), Some(&store));
        assert!(!facts.buffer_contract.ready);
        assert_eq!(facts.buffer_report.unwrap().backend, "local-fs");
        assert_eq!(
            facts.buffer_contract.blocker.unwrap().code,
            "buffer-contract-mismatch"
        );
    }
    #[test]
    fn failure_domain_checked_independently_of_backend_name() {
        let (definition, store) = local();
        let actual = store.report();
        assert!(contract_matches(&definition, &actual));
        // Pure contract comparison: corrupt the real report's metadata, not
        // object-store behavior or the production report producer.
        let mut wrong = actual.clone();
        wrong.failure_domain.host_loss = HostLoss::SurvivedByService;
        assert!(!contract_matches(&definition, &wrong));
        wrong = actual;
        wrong.failure_domain.process_crash_survived = false;
        assert!(!contract_matches(&definition, &wrong));
    }
    #[test]
    fn missing_producers_and_acknowledgments_never_supply_positive_facts() {
        let (mut definition, store) = local();
        definition.fence_plan.acknowledged_residual_risks = vec!["operator assertion".into()];
        let facts = Facts::sample(Some(&definition), Some(&store));
        assert!(facts.buffer_contract.ready);
        for fact in [&facts.apply_lag, &facts.schema_jobs, &facts.fence_risks] {
            assert!(!fact.ready);
            let blocker = fact.blocker.as_ref().unwrap();
            assert!(!blocker.reason.is_empty());
            assert!(!blocker.remediation.is_empty());
        }
        assert!(facts.apply_lag.source.contains("age"));
        assert!(facts.apply_lag.blocker.unwrap().reason.contains("bytes"));
        assert!(!Facts::sample(Some(&definition), None).buffer_contract.ready);
        assert!(!Facts::sample(None, Some(&store)).buffer_contract.ready);
    }
}
