//! A source inventory is independent from a completed verification report.
use super::readiness::Fact;
use crate::{
    coordinator::Materialized,
    pg::fence::inventory::{self, Kind, Risk},
    status::Blocker,
};
use std::time::{Duration, Instant};

pub(super) struct Request<'a> {
    pub state: &'a Materialized,
    pub owner: String,
    pub scope: Vec<String>,
    pub started: Instant,
}
pub(super) async fn refresh(
    retained: &std::sync::Mutex<Option<Observation>>,
    client: &tokio_postgres::Client,
    request: Request<'_>,
) -> Result<(), String> {
    *retained.lock().expect("fence inventory poisoned") = None;
    let definition = request
        .state
        .definition
        .as_ref()
        .ok_or("missing inventory definition")?;
    let risks = inventory::collect(
        client,
        &request.scope,
        &definition.fence_plan.application_roles,
    )
    .await?;
    *retained.lock().expect("fence inventory poisoned") = Observation::new(
        request.state,
        request.owner,
        request.scope,
        risks,
        request.started,
    );
    Ok(())
}

pub(super) struct Observation {
    pub owner: String,
    started: Instant,
    epoch: u64,
    generation: u64,
    digest: String,
    plans: Vec<(String, String)>,
    scope: Vec<String>,
    risks: Vec<Risk>,
}
impl Observation {
    pub fn new(
        state: &Materialized,
        owner: String,
        scope: Vec<String>,
        risks: Vec<Risk>,
        started: Instant,
    ) -> Option<Self> {
        Some(Self {
            owner,
            started,
            epoch: state.epoch,
            generation: state.verification.generation,
            digest: state.definition.as_ref()?.canonical_digest().ok()?,
            plans: state
                .chunk_plans
                .iter()
                .map(|(k, v)| (k.clone(), v.digest.clone()))
                .collect(),
            scope,
            risks,
        })
    }
    pub fn fact(&self, state: &Materialized, owner: Option<&str>, running: bool) -> Option<Fact> {
        let definition = state.definition.as_ref()?;
        if !running
            || owner != Some(self.owner.as_str())
            || self.scope.is_empty()
            || self.started.elapsed() > Duration::from_secs(1)
            || state.epoch != self.epoch
            || state.verification.generation != self.generation
            || definition.canonical_digest().ok()? != self.digest
            || state
                .chunk_plans
                .iter()
                .map(|(k, v)| (k.clone(), v.digest.clone()))
                .collect::<Vec<_>>()
                != self.plans
        {
            return None;
        }
        let missing = inventory::missing(
            &self.risks,
            &definition.fence_plan.acknowledged_residual_risks,
        );
        let reason = missing.iter().map(|risk| format!(
            "{} principal={} object={}: {}; next: {}",
            risk.id, risk.principal, risk.object, risk.evidence,
            if risk.kind == Kind::OutOfScopeWriter {
                "acknowledge this exact id or add the principal to fence_plan.application_roles and refresh"
            } else {
                "review the evidence and acknowledge this exact id in fence_plan.acknowledged_residual_risks"
            }
        )).collect::<Vec<_>>().join("; ");
        Some(Fact {
            ready: missing.is_empty(),
            source: format!("Read-only source catalog inventory: {} risks across {} selected tables; migration session/current roles exempt only from direct principal risks.", self.risks.len(), self.scope.len()),
            staleness: "Expires 1 second from refresh start; failed refresh, changed owner/epoch/definition/scope, or capture/apply exit revokes authority.".into(),
            blocker: (!missing.is_empty()).then(|| Blocker {
                code: "fence-risk-unacknowledged".into(), reason,
                remediation: "Review every named risk; use exact stable ids, then refresh. Adding application_roles only resolves revokable direct writers, not owners, superusers or indirect writers.".into(),
            }),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn state() -> Materialized {
        let json = include_str!("../../README.md")
            .split("```json")
            .nth(1)
            .unwrap()
            .split("```")
            .next()
            .unwrap();
        Materialized {
            definition: Some(crate::MigrationDefinition::from_json(json).unwrap()),
            epoch: 1,
            ..Default::default()
        }
    }
    #[test]
    fn all_missing_ids_are_diagnosed_and_expiry_or_binding_changes_revoke() {
        let mut state = state();
        let risks = inventory::from_rows(
            vec![
                vec![
                    "out-of-scope-writer",
                    "extra",
                    "s.t",
                    "column UPDATE via membership",
                ],
                vec![
                    "unscanned-security-definer",
                    "owner",
                    "s.f()",
                    "body unknown",
                ],
            ]
            .into_iter()
            .map(|r| r.into_iter().map(str::to_owned).collect())
            .collect(),
        )
        .unwrap();
        let ids = risks.iter().map(|r| r.id.clone()).collect::<Vec<_>>();
        let make = |s: &Materialized| {
            Observation::new(
                s,
                "owner".into(),
                vec!["s.t".into()],
                risks.clone(),
                Instant::now(),
            )
            .unwrap()
        };
        let observed = make(&state);
        let fact = observed.fact(&state, Some("owner"), true).unwrap();
        assert!(!fact.ready);
        for id in &ids {
            assert!(fact.blocker.as_ref().unwrap().reason.contains(id));
        }
        assert!(observed.fact(&state, Some("other-owner"), true).is_none());
        assert!(observed.fact(&state, Some("owner"), false).is_none());
        state.verification.generation += 1;
        assert!(observed.fact(&state, Some("owner"), true).is_none());
        state.verification.generation -= 1;
        state.epoch += 1;
        assert!(observed.fact(&state, Some("owner"), true).is_none());
        state.epoch -= 1;
        state
            .definition
            .as_mut()
            .unwrap()
            .fence_plan
            .acknowledged_residual_risks = ids;
        assert!(observed.fact(&state, Some("owner"), true).is_none());
        let mut observed = make(&state);
        assert!(observed.fact(&state, Some("owner"), true).unwrap().ready);
        // A retained plan from a different resolved scope cannot authorize the
        // current state even with an unchanged definition and owner token.
        observed.plans.push(("old-table".into(), "old-plan".into()));
        assert!(observed.fact(&state, Some("owner"), true).is_none());
        observed.plans.clear();
        observed.started = Instant::now() - Duration::from_millis(1001);
        assert!(observed.fact(&state, Some("owner"), true).is_none());
    }

    #[tokio::test]
    async fn live_extra_writer_and_definer_block_then_exact_acknowledgments_hold() {
        let Some(path) = std::env::var_os("SKEDADDLE_TEST_CONFIG") else {
            use std::io::Write;
            writeln!(std::io::stderr(), "LOUD SKIP F7 source fence inventory/readiness condition9: SKEDADDLE_TEST_CONFIG absent; real PostgreSQL qualification NOT RUN; no Docker started").unwrap();
            return;
        };
        let config: serde_json::Value =
            serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        let dsn = config["pg_fence"]
            .as_str()
            .expect("harness pg_fence administrative DSN required for disposable role fixture");
        let ca = config["ca_pem"].as_str().expect("harness ca_pem");
        let config = crate::pg::tls::Config::new(dsn.parse().unwrap(), ca);
        let (client, driver, _) =
            crate::pg::tls::connect(&config, &crate::security::TlsPolicy::Required)
                .await
                .unwrap();
        let driver = tokio::spawn(driver);
        let schema = format!("f7_{}", uuid::Uuid::new_v4().simple());
        let role = format!("{schema}_writer");
        // DDL, including roles, stays uncommitted. Panic/client disconnect also
        // rolls it back, so failed assertions cannot leak fixture objects.
        client.batch_execute(&format!("BEGIN; CREATE SCHEMA {schema}; CREATE TABLE {schema}.rows(id bigint); CREATE ROLE {role} NOLOGIN; GRANT USAGE ON SCHEMA {schema} TO {role}; GRANT UPDATE(id),SELECT(id) ON {schema}.rows TO {role}; CREATE FUNCTION {schema}.writer() RETURNS void LANGUAGE SQL SECURITY DEFINER AS 'UPDATE {schema}.rows SET id=id'; ALTER FUNCTION {schema}.writer() OWNER TO {role};")).await.unwrap();
        let result = async {
            let mut state = state();
            let scope = vec![format!("{schema}.rows")];
            let retained = std::sync::Mutex::new(None);
            fn request<'a>(state: &'a Materialized, scope: &[String]) -> Request<'a> {
                Request {
                    state,
                    owner: "live-owner".into(),
                    scope: scope.to_vec(),
                    started: Instant::now(),
                }
            }
            refresh(&retained, &client, request(&state, &scope)).await?;
            let (ids, extra_ids) = {
                let guard = retained.lock().unwrap();
                let observed = guard.as_ref().unwrap();
                assert!(
                    observed
                        .risks
                        .iter()
                        .any(|r| r.kind == Kind::OutOfScopeWriter && r.principal == role)
                );
                assert!(
                    observed
                        .risks
                        .iter()
                        .any(|r| r.kind == Kind::UnscannedSecurityDefiner
                            && r.object.contains(&format!("{schema}.writer(")))
                );
                let fact = observed.fact(&state, Some("live-owner"), true).unwrap();
                assert!(!fact.ready);
                for risk in &observed.risks {
                    assert!(fact.blocker.as_ref().unwrap().reason.contains(&risk.id));
                }
                let ids = observed
                    .risks
                    .iter()
                    .map(|r| r.id.clone())
                    .collect::<Vec<_>>();
                let extras = observed
                    .risks
                    .iter()
                    .filter(|r| {
                        (r.kind == Kind::OutOfScopeWriter && r.principal == role)
                            || r.object.contains(&format!("{schema}.writer("))
                    })
                    .map(|r| r.id.clone())
                    .collect::<Vec<_>>();
                assert_eq!(extras.len(), 2);
                (ids, extras)
            };
            // Pre-acknowledge baseline risks, leaving exactly our new writer
            // and definer unacknowledged. Both must be named in the blocker.
            state
                .definition
                .as_mut()
                .unwrap()
                .fence_plan
                .acknowledged_residual_risks = ids
                .iter()
                .filter(|id| !extra_ids.contains(id))
                .cloned()
                .collect();
            refresh(&retained, &client, request(&state, &scope)).await?;
            {
                let guard = retained.lock().unwrap();
                let observed = guard.as_ref().unwrap();
                assert_eq!(
                    inventory::missing(
                        &observed.risks,
                        &state
                            .definition
                            .as_ref()
                            .unwrap()
                            .fence_plan
                            .acknowledged_residual_risks
                    )
                    .len(),
                    2
                );
                let fact = observed.fact(&state, Some("live-owner"), true).unwrap();
                assert!(!fact.ready);
                for id in &extra_ids {
                    assert!(fact.blocker.as_ref().unwrap().reason.contains(id));
                }
            }
            state
                .definition
                .as_mut()
                .unwrap()
                .fence_plan
                .acknowledged_residual_risks = ids;
            refresh(&retained, &client, request(&state, &scope)).await?;
            assert!(
                retained
                    .lock()
                    .unwrap()
                    .as_ref()
                    .unwrap()
                    .fact(&state, Some("live-owner"), true)
                    .unwrap()
                    .ready
            );
            // A real failed source query must revoke the previous authority.
            let mut invalid = request(&state, &scope);
            invalid.scope = vec![format!("{schema}.missing")];
            assert!(refresh(&retained, &client, invalid).await.is_err());
            assert!(retained.lock().unwrap().is_none());
            Ok::<(), String>(())
        }
        .await;
        client.batch_execute("ROLLBACK").await.unwrap();
        drop(client);
        driver.abort();
        result.unwrap();
    }
}
