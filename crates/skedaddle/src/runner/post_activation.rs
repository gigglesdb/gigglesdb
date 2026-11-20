//! Post-activation ownership is separate from the joined forward workers.
use super::verification::{VerifyResponse, active, write_json};
use super::*;
use crate::{
    coordinator::{
        self, Event, State, Transition, epoch::Coordinator, exclusion::MigrationExclusion,
        local::Local, reverse::EligibilityEnd,
    },
    engine::{Engine, PauseControl, Reached},
    pg::reverse::Violation,
};
use serde::Deserialize;
use serde_json::json;
use std::{collections::BTreeSet, path::Path};
use tokio::sync::watch;

pub(super) fn owns(state: State) -> bool {
    matches!(
        state,
        State::DestinationActive | State::RollbackWindow | State::RollingBack
    )
}

pub async fn request_finalize(root: &Path, id: &str) -> VerifyResponse {
    super::gc::request(root, id, "finalize").await
}

pub async fn request_finalize_objects(root: &Path, id: &str, drop_objects: bool) -> VerifyResponse {
    super::gc::request_options(root, id, "finalize", drop_objects).await
}

#[derive(Deserialize)]
struct Request {
    generation: String,
    #[serde(alias = "run")]
    request: String,
    #[serde(default)]
    drop_objects: bool,
    #[serde(default)]
    deadline_ms: Option<u64>,
}

pub(super) async fn resume(
    root: &Path,
    id: &str,
    definition: &MigrationDefinition,
    pause: watch::Receiver<bool>,
) -> Result<Reached, RunError> {
    // Do not run forward preparation against the now writable destination.
    let mut redactor = Redactor::default();
    let source_config = config(&definition.source, &mut redactor).map_err(RunError::Preparation)?;
    let journal = open_journal(root, id).await?;
    let path = root.join(format!("{id}.local"));
    let local = if path.exists() {
        Local::open_journal(&path, &journal).await?
    } else {
        let mut local = Local::create(&path)?;
        local.replay_journal(&journal).await?;
        local
    };
    let mut coordinator = Coordinator::observer(
        journal.clone(),
        local,
        format!("runner-post-{}", std::process::id()),
    )?
    .with_exclusion(MigrationExclusion::postgres(
        source_config,
        definition.source.tls,
    ));
    coordinator.acquire().await?;
    let engine = Engine::new(coordinator, journal.clone());
    Box::pin(serve(
        engine.runtime(),
        root,
        id,
        pause,
        Some((journal, definition.clone())),
    ))
    .await
    .map_err(RunError::Engine)
}

async fn serve(
    runtime: crate::engine::Runtime,
    root: &Path,
    id: &str,
    pause: watch::Receiver<bool>,
    context: Option<(coordinator::journal::Journal, MigrationDefinition)>,
) -> coordinator::Result<Reached> {
    serve_with_driver(
        runtime,
        root,
        id,
        pause,
        context,
        &mut super::rollback::Live::default(),
    )
    .await
}

async fn serve_with_driver(
    runtime: crate::engine::Runtime,
    root: &Path,
    id: &str,
    mut pause: watch::Receiver<bool>,
    context: Option<(coordinator::journal::Journal, MigrationDefinition)>,
    driver: &mut impl super::rollback::OwnerDriver,
) -> coordinator::Result<Reached> {
    let control = root.join(format!("{id}.control"));
    let mut owner = PauseControl::new(&control)?.activate()?;
    let generation = owner.generation().to_string();
    let mut handled = BTreeSet::new();
    let mut retry_at = tokio::time::Instant::now();
    let mut backoff = Duration::from_secs(1);
    let mut workers: Option<Box<super::reverse_workers::Workers>> = None;
    let mut worker_retry = tokio::time::Instant::now();
    let mut worker_backoff = Duration::from_secs(1);
    let mut status_at = tokio::time::Instant::now();
    loop {
        if let Err(error) = runtime.refresh_owner().await {
            let state = runtime.state().await;
            if let Some(cp) = &state.rollback {
                let path = control.join(format!("rollback-{generation}-{}.request", cp.request));
                respond_failure(&path, &error)?;
            }
            return Err(error);
        }
        let state = runtime.state().await;
        if state.lifecycle.state.is_terminal() {
            break;
        }
        if !owns(state.lifecycle.state) {
            return Err(coordinator::Error::Rejected(
                "post-activation owner cannot run forward work".into(),
            ));
        }
        if *pause.borrow() || owner.requested()? {
            break;
        }
        if state.lifecycle.state == State::RollingBack && tokio::time::Instant::now() >= retry_at {
            let path = control.join(format!(
                "rollback-{generation}-{}.request",
                state
                    .rollback
                    .as_ref()
                    .map(|cp| cp.request.as_str())
                    .unwrap_or("missing")
            ));
            let mut delay = Duration::from_millis(25);
            match super::rollback::run_with_driver(
                &runtime,
                root,
                id,
                context.as_ref(),
                driver,
                &mut workers,
            )
            .await
            {
                Ok(Some(response)) => write_json(&path.with_extension("response"), &response)?,
                Ok(None) => {}
                Err(coordinator::Error::LostOwnership | coordinator::Error::Observer) => {
                    respond_failure(&path, &coordinator::Error::LostOwnership)?;
                    return Err(coordinator::Error::LostOwnership);
                }
                Err(error) => {
                    respond_failure(&path, &error)?;
                    delay = Duration::from_secs(1);
                }
            }
            retry_at = tokio::time::Instant::now() + delay;
            continue;
        }
        if state.lifecycle.state != State::RollingBack
            && let Some((saved, _)) = state.progress.get(super::finalize::REQUEST)
        {
            let drop_objects = serde_json::from_str::<bool>(saved)?;
            workers = None; // These workers own sockets directly, with no spawned capture task.
            match super::finalize::run(&runtime, id, drop_objects, context.as_ref().map(|(_, d)| d))
                .await
            {
                Ok(()) => {
                    finalize(&runtime).await?;
                    continue;
                }
                Err(coordinator::Error::LostOwnership | coordinator::Error::Observer) => {
                    return Err(coordinator::Error::LostOwnership);
                }
                Err(_) => {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
            }
        }
        if state.lifecycle.state != State::RollingBack
            && state.rollback_eligibility_ended.is_none()
            && tokio::time::Instant::now() >= retry_at
        {
            #[cfg(test)]
            let simulated_healthy = driver.simulated_healthy();
            #[cfg(not(test))]
            let simulated_healthy = false;
            let result = if simulated_healthy {
                Ok(None)
            } else if let Some((journal, definition)) = &context {
                tokio::time::timeout(
                    Duration::from_secs(10),
                    probe(&runtime, journal, definition, workers.is_some()),
                )
                .await
                .unwrap_or(Err(()))
            } else {
                Ok(Some(Violation::ActivationBaselineUnavailable))
            };
            match result {
                Ok(Some(violation)) => {
                    runtime
                        .commit(Transition::RollbackEligibilityEnded {
                            reason: EligibilityEnd::Domain { violation },
                        })
                        .await?;
                }
                Ok(None) => {
                    if runtime.state().await.lifecycle.state == State::DestinationActive {
                        admit(&runtime, true).await?;
                    }
                    backoff = Duration::from_secs(1);
                }
                Err(_) => {
                    runtime
                        .marker(
                            "reverse-capture-health",
                            "unhealthy".into(),
                            "retrying".into(),
                        )
                        .await?;
                    backoff = (backoff * 2).min(Duration::from_secs(30));
                }
            }
            retry_at = tokio::time::Instant::now() + backoff;
        }
        let current = runtime.state().await;
        if current.rollback_eligibility_ended.is_some() {
            workers = None;
        } else if current.lifecycle.state == State::RollbackWindow
            && workers.is_none()
            && tokio::time::Instant::now() >= worker_retry
            && let Some((journal, definition)) = &context
        {
            let result = tokio::time::timeout(
                Duration::from_secs(10),
                Box::pin(super::reverse_workers::Workers::open(
                    &runtime, journal, definition, root, id,
                )),
            )
            .await
            .unwrap_or(Err(super::reverse_workers::Failure::Retry));
            match result {
                Ok(value) => {
                    workers = Some(value);
                    worker_backoff = Duration::from_secs(1);
                }
                Err(super::reverse_workers::Failure::End(reason)) => {
                    end_workers(&runtime, reason).await?;
                }
                Err(super::reverse_workers::Failure::Retry) => {
                    runtime
                        .marker(
                            "reverse-capture-health",
                            "unhealthy".into(),
                            "retrying".into(),
                        )
                        .await?;
                    worker_retry = tokio::time::Instant::now() + worker_backoff;
                    worker_backoff = (worker_backoff * 2).min(Duration::from_secs(30));
                }
            }
        }
        if let Some(worker) = &mut workers
            && tokio::time::Instant::now() >= status_at
        {
            worker.status(&runtime).await?;
            status_at = tokio::time::Instant::now() + Duration::from_secs(1);
        }
        if active(&control)?.as_deref() != Some(&generation) {
            return Err(coordinator::Error::LostOwnership);
        }
        for entry in std::fs::read_dir(&control)? {
            let path = entry?.path();
            let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
                continue;
            };
            if !name.ends_with(".request") || handled.contains(name) {
                continue;
            }
            let action = if name.starts_with("finalize-") {
                "finalize"
            } else if name.starts_with("rollback-") {
                "rollback"
            } else if name.starts_with("verify-") {
                "verify"
            } else if name.starts_with("gc-") {
                "gc"
            } else {
                continue;
            };
            let Ok(request) = serde_json::from_slice::<Request>(&std::fs::read(&path)?) else {
                continue;
            };
            handled.insert(name.to_owned());
            let response = if request.generation != generation {
                VerifyResponse {
                    exit_code: 8,
                    data: json!({"error":{"kind":"stale_generation"}}),
                }
            } else if uuid::Uuid::parse_str(&request.request).is_err()
                || name != format!("{action}-{generation}-{}.request", request.request)
            {
                VerifyResponse {
                    exit_code: 3,
                    data: json!({"error":{"kind":"invalid_request"}}),
                }
            } else if action == "rollback" {
                match super::rollback::admit(&runtime, &request.request, request.deadline_ms).await
                {
                    Ok(Some(response)) => response,
                    Ok(None) => {
                        // Defer the response until the admitted driver reaches an outcome.
                        retry_at = tokio::time::Instant::now();
                        continue;
                    }
                    Err(error) => {
                        respond_failure(&path, &error)?;
                        return Err(error);
                    }
                }
            } else if action == "finalize" {
                let state = runtime.state().await;
                if !super::finalize::allowed(&state) {
                    VerifyResponse {
                        exit_code: 3,
                        data: json!({"error":{"kind":"invalid_state"}}),
                    }
                } else {
                    // Durable admission disables capture restart and health probes on recovery.
                    if let Err(error) = runtime
                        .marker(
                            super::finalize::REQUEST,
                            serde_json::to_string(&request.drop_objects)?,
                            "intent".into(),
                        )
                        .await
                    {
                        respond_failure(&path, &error)?;
                        return Err(error);
                    }
                    workers = None;
                    match super::finalize::run(
                        &runtime,
                        id,
                        request.drop_objects,
                        context.as_ref().map(|(_, d)| d),
                    )
                    .await
                    {
                        Ok(()) => {
                            if let Err(error) = finalize(&runtime).await {
                                respond_failure(&path, &error)?;
                                return Err(error);
                            }
                            VerifyResponse {
                                exit_code: 0,
                                data: json!({"final_state":State::Finalized}),
                            }
                        }
                        Err(coordinator::Error::LostOwnership | coordinator::Error::Observer) => {
                            write_json(
                                &path.with_extension("response"),
                                &VerifyResponse {
                                    exit_code: 8,
                                    data: json!({"error":{"kind":"lost_ownership"}}),
                                },
                            )?;
                            return Err(coordinator::Error::LostOwnership);
                        }
                        Err(_) => VerifyResponse {
                            exit_code: 6,
                            data: json!({"error":{"kind":"infrastructure","retrying":true}}),
                        },
                    }
                }
            } else {
                // Forward verification and forward GC have no reverse authority.
                VerifyResponse {
                    exit_code: 3,
                    data: json!({"complete":false,"error":{"kind":"post_activation_unavailable"}}),
                }
            };
            write_json(&path.with_extension("response"), &response)?;
            if runtime.state().await.lifecycle.state.is_terminal() {
                break;
            }
        }
        if runtime.state().await.lifecycle.state.is_terminal()
            || *pause.borrow()
            || owner.requested()?
        {
            break;
        }
        tokio::select! {
            result = async {
                match &mut workers {
                    Some(worker) if state.lifecycle.state != State::RollingBack => Box::pin(worker.step()).await,
                    _ => std::future::pending().await,
                }
            } => {
                match result {
                    Ok(durable) => {
                        if durable && let Some(worker) = &mut workers {
                            // Outside the cancellable worker future: checkpoint
                            // publication must finish before the next WAL ack.
                            worker.status(&runtime).await?;
                        }
                    }
                    Err(super::reverse_workers::Failure::End(reason)) => {
                        end_workers(&runtime, reason).await?;
                        workers = None;
                    }
                    Err(super::reverse_workers::Failure::Retry) => {
                        workers = None;
                        runtime.marker("reverse-capture-health", "unhealthy".into(), "retrying".into()).await?;
                        worker_retry = tokio::time::Instant::now() + worker_backoff;
                        worker_backoff = (worker_backoff * 2).min(Duration::from_secs(30));
                    }
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(25)) => {},
            changed = pause.changed() => if changed.is_err() { break; },
        }
    }
    owner.retire()?;
    Ok(Reached {
        state: runtime.state().await.lifecycle.state,
        blockers: vec![],
        target_reached: false,
    })
}

fn respond_failure(path: &Path, error: &coordinator::Error) -> coordinator::Result<()> {
    let (exit_code, kind) = if matches!(
        error,
        coordinator::Error::LostOwnership | coordinator::Error::Observer
    ) {
        (8, "lost_ownership")
    } else {
        (6, "infrastructure")
    };
    write_json(
        &path.with_extension("response"),
        &VerifyResponse {
            exit_code,
            data: json!({"error":{"kind":kind}}),
        },
    )
}

pub(super) async fn end_workers(
    runtime: &crate::engine::Runtime,
    reason: EligibilityEnd,
) -> coordinator::Result<()> {
    if runtime.state().await.rollback_eligibility_ended.is_none() {
        runtime
            .commit(Transition::RollbackEligibilityEnded { reason })
            .await?;
    }
    Ok(())
}

async fn finalize(runtime: &crate::engine::Runtime) -> coordinator::Result<()> {
    if !super::finalize::allowed(&runtime.state().await) {
        return Err(coordinator::Error::Rejected(
            "finalize requires rollback window or ended eligibility".into(),
        ));
    }
    if runtime.state().await.rollback_eligibility_ended.is_none() {
        runtime
            .commit(Transition::RollbackEligibilityEnded {
                reason: EligibilityEnd::OperatorFinalized,
            })
            .await?;
    }
    runtime
        .commit(Transition::Lifecycle {
            event: Event::Advance {
                to: State::Finalized,
            },
        })
        .await
}

async fn admit(runtime: &crate::engine::Runtime, healthy: bool) -> coordinator::Result<()> {
    let state = runtime.state().await;
    if state.rollback_eligibility_ended.is_some() {
        return Ok(());
    }
    runtime
        .marker(
            "reverse-capture-health",
            if healthy { "healthy" } else { "unhealthy" }.into(),
            "admission".into(),
        )
        .await?;
    if healthy && state.lifecycle.state == State::DestinationActive {
        runtime
            .commit(Transition::Lifecycle {
                event: Event::Advance {
                    to: State::RollbackWindow,
                },
            })
            .await?;
    }
    Ok(())
}
fn baseline_failure(error: super::activation_baseline::LoadError) -> Result<Option<Violation>, ()> {
    match error {
        super::activation_baseline::LoadError::Invalid => {
            Ok(Some(Violation::DestinationCatalogDrift))
        }
        super::activation_baseline::LoadError::Unavailable(_) => Err(()),
    }
}

async fn probe(
    runtime: &crate::engine::Runtime,
    journal: &coordinator::journal::Journal,
    definition: &MigrationDefinition,
    streaming: bool,
) -> Result<Option<Violation>, ()> {
    let baseline = match super::activation_baseline::load(journal).await {
        Ok(Some(value)) => value,
        Ok(None) => return Ok(Some(Violation::ActivationBaselineUnavailable)),
        Err(error) => return baseline_failure(error),
    };
    let mut redactor = Redactor::default();
    let destination_config = config(&definition.destination, &mut redactor).map_err(|_| ())?;
    let mut destination = session(&destination_config, &definition.destination)
        .await
        .map_err(|_| ())?;
    settings(&destination.client).await.map_err(|_| ())?;
    let objects = selection::catalog_objects(&definition.selected_objects);
    let live = preflight::collect(&mut destination.client, &objects)
        .await
        .map_err(|_| ())?;
    match baseline.matches(&live) {
        Ok(true) => {}
        Ok(false) => return Ok(Some(Violation::DestinationCatalogDrift)),
        Err(_) => return Err(()), // unavailable catalog evidence is retryable
    }
    let state = runtime.state().await;
    let proof = state
        .verification
        .reverse
        .as_ref()
        .and_then(|r| r.proof.as_ref())
        .ok_or(())?;
    let identity = destination
        .client
        .query_one(
            "SELECT system_identifier::text,current_database() FROM pg_control_system()",
            &[],
        )
        .await
        .map_err(|_| ())?;
    if identity.get::<_, String>(0) != proof.binding.destination.system_identifier
        || identity.get::<_, String>(1) != proof.binding.destination.database
    {
        return Ok(Some(Violation::DestinationCatalogDrift));
    }
    let version: String = destination
        .client
        .query_one("SHOW server_version_num", &[])
        .await
        .map_err(|_| ())?
        .get(0);
    let reverse = crate::pg::reverse::Config::new(
        &proof.binding.migration,
        version.parse().map_err(|_| ())?,
        objects
            .iter()
            .map(|o| (o.schema.clone(), o.name.clone()))
            .collect(),
    )
    .map_err(|_| ())?;
    let durable_lsn = state
        .progress
        .get("reverse-slot-v1")
        .map(|(saved, _)| crate::engine::decode_source_checkpoint(saved).map(|c| c.durable_lsn))
        .transpose()
        .map_err(|_| ())?
        .unwrap_or(proof.slot.consistent_point);
    let sql = reverse
        .capture_health_sql(
            &proof.readiness,
            proof.activation_lsn,
            durable_lsn,
            streaming,
        )
        .map_err(|_| ())?;
    if !destination
        .client
        .query_one(&sql, &[])
        .await
        .map_err(|_| ())?
        .get::<_, bool>(0)
    {
        return Err(());
    }
    let source_config = config(&definition.source, &mut redactor).map_err(|_| ())?;
    let source = session(&source_config, &definition.source)
        .await
        .map_err(|_| ())?;
    let healthy: bool = source.client.query_one("SELECT EXISTS (SELECT 1 FROM pg_replication_origin WHERE roname=$1) AND to_regclass('_skedaddle.apply_progress') IS NOT NULL AND to_regclass('_skedaddle.apply_receipts') IS NOT NULL", &[&reverse.origin()]).await.map_err(|_| ())?.get(0);
    if !healthy {
        return Err(());
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordinator::{
        Materialized, ReadinessFacts,
        journal::{Entry, Journal},
        reverse::{DestinationIdentity, FenceBinding, ReverseConsistentPoint, SetupRecord},
    };
    use crate::pg::{activate, fence, reverse as pg};
    use std::{path::PathBuf, sync::Arc};
    fn task_root() -> PathBuf {
        let root = crate::test_root::scratch_root().join(format!("post-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        root
    }
    pub fn ready() -> ReadinessFacts {
        ReadinessFacts {
            all_chunks_done: true,
            capture_healthy: true,
            lineage_matches: true,
            apply_lag: 5,
            lag_budget: 5,
            blocked_ddl: false,
            unknown_schema_job: false,
            verified_rows: 100,
            in_scope_rows: 100,
            busy_rows_past_budget: 0,
            dead_letters: 0,
            indexes_constraints_valid: true,
            sequences_syncable: true,
            reverse_prerequisites: true,
            fence_risks_acknowledged: true,
            buffer_contract_matches: true,
        }
    }
    async fn append(journal: &Journal, state: &mut Materialized, transition: Transition) {
        let entry = Entry::new(state, transition, "owner").unwrap();
        journal.put(&entry).await.unwrap();
        state.apply(&entry).unwrap();
    }
    async fn fenced() -> (Journal, Materialized, FenceBinding) {
        {
            let root = task_root();
            std::fs::create_dir_all(root.join("journal")).unwrap();
            fenced_on(
                Journal::new(
                    Arc::new(coordinator::journal::local_store(root.join("journal")).unwrap()),
                    "post",
                    "migration",
                    Duration::from_secs(5),
                )
                .unwrap(),
            )
            .await
        }
    }
    async fn fenced_on(journal: Journal) -> (Journal, Materialized, FenceBinding) {
        let mut state = Materialized::default();
        append(
            &journal,
            &mut state,
            Transition::AcquireOwnership { epoch: 1 },
        )
        .await;
        for to in [State::Assessed, State::Copying, State::CatchingUp] {
            append(
                &journal,
                &mut state,
                Transition::Lifecycle {
                    event: Event::Advance { to },
                },
            )
            .await;
        }
        append(
            &journal,
            &mut state,
            Transition::Lifecycle {
                event: Event::Readiness { facts: ready() },
            },
        )
        .await;
        for to in [State::SourceFenced, State::FinalBoundaryApplied] {
            append(
                &journal,
                &mut state,
                Transition::Lifecycle {
                    event: Event::Advance { to },
                },
            )
            .await;
        }
        let table = fence::Table {
            schema: "public".into(),
            name: "rows".into(),
            owner: "admin".into(),
        };
        let catalog = fence::Catalog {
            roles: vec![fence::Role {
                name: "app".into(),
                login: false,
                superuser: false,
                bypass_rls: false,
            }],
            tables: vec![table.clone()],
            ..Default::default()
        };
        let mut destination = activate::DestinationFence::new(activate::Config {
            epoch: 1,
            applications: BTreeSet::from(["app".into()]),
            tables: vec![table],
            timeout_ms: 1000,
            post_activate: None,
        })
        .unwrap();
        destination.next = activate::Step::Fenced;
        destination.saved = Some(fence::SavedState {
            catalog: catalog.clone(),
            authority: None,
        });
        destination.evidence.push(activate::FenceEvidence {
            ordinal: 0,
            observed_at: "retained-observation".into(),
            catalog,
        });
        let binding = FenceBinding {
            migration: "migration".into(),
            epoch: 1,
            owner: "owner".into(),
            destination: DestinationIdentity {
                system_identifier: "123".into(),
                database: "destination".into(),
            },
            fence: Box::new(destination),
        };
        (journal, state, binding)
    }
    fn readiness(slot: &pg::SlotEvidence) -> pg::Readiness {
        // Exercise the actual serialized P11a Ready wire shape, including its private
        // fields. This is a contract fixture, not a database transport substitute.
        serde_json::from_value(serde_json::json!({"installation":"contract", "slot":slot})).unwrap()
    }
    async fn setup_records(
        journal: &Journal,
        state: &mut Materialized,
        binding: &FenceBinding,
    ) -> ReverseConsistentPoint {
        let slot = pg::SlotEvidence {
            slot: "skedaddle_contract_rev".into(),
            consistent_point: 100,
        };
        let readiness = readiness(&slot);
        for intent in [
            pg::Intent::Publication,
            pg::Intent::SlotReady(slot.clone()),
            pg::Intent::Ready(readiness.clone()),
        ] {
            append(
                journal,
                state,
                Transition::ReverseSetup {
                    record: Box::new(SetupRecord {
                        binding: binding.clone(),
                        endpoint: pg::Endpoint::Destination,
                        intent,
                    }),
                },
            )
            .await;
        }
        ReverseConsistentPoint {
            binding: binding.clone(),
            slot,
            readiness,
            activation_lsn: 101,
        }
    }

    async fn activated() -> (Journal, Materialized) {
        let (journal, mut state, binding) = fenced().await;
        let proof = setup_records(&journal, &mut state, &binding).await;
        append(
            &journal,
            &mut state,
            Transition::ReverseConsistentPoint {
                proof: Box::new(proof),
            },
        )
        .await;
        append(
            &journal,
            &mut state,
            Transition::Lifecycle {
                event: Event::Advance {
                    to: State::DestinationActive,
                },
            },
        )
        .await;
        (journal, state)
    }

    #[tokio::test]
    async fn owners_stay_alive_across_active_window_and_ineligible_restarts_until_finalize() {
        for window in [false, true] {
            for ended in [false, true] {
                let (journal, mut state) = activated().await;
                if window {
                    append(
                        &journal,
                        &mut state,
                        Transition::Lifecycle {
                            event: Event::Advance {
                                to: State::RollbackWindow,
                            },
                        },
                    )
                    .await;
                }
                if ended {
                    append(
                        &journal,
                        &mut state,
                        Transition::RollbackEligibilityEnded {
                            reason: EligibilityEnd::SourceFenceBreach,
                        },
                    )
                    .await;
                }
                let root = task_root();
                let local = Local::recover(root.join("owner"), &journal).await.unwrap();
                let mut coordinator =
                    Coordinator::observer(journal.clone(), local, "restart").unwrap();
                coordinator.acquire().await.unwrap();
                let engine = Engine::new(coordinator, journal.clone());
                let runtime = engine.runtime();
                let (pause, receive) = watch::channel(false);
                let owner = serve(runtime.clone(), &root, "migration", receive, None);
                tokio::pin!(owner);
                // Drive the owner until its admission decision is observable.
                // A fixed sleep can cancel it halfway through durable journal I/O
                // when the full library suite is running concurrently.
                tokio::time::timeout(Duration::from_secs(5), async {
                    tokio::select! {
                        _ = &mut owner => panic!("post-activation owner exited before finalize"),
                        _ = async {
                            loop {
                                if runtime.state().await.rollback_eligibility_ended.is_some()
                                    && active(&root.join("migration.control")).unwrap().is_some()
                                {
                                    break;
                                }
                                tokio::time::sleep(Duration::from_millis(10)).await;
                            }
                        } => {}
                    }
                })
                .await
                .unwrap();
                assert!(active(&root.join("migration.control")).unwrap().is_some());
                let replayed = journal.load(true).await.unwrap();
                assert_eq!(
                    replayed.lifecycle.state,
                    if window {
                        State::RollbackWindow
                    } else {
                        State::DestinationActive
                    }
                );
                assert_eq!(
                    replayed.rollback_eligibility_ended,
                    Some(if ended {
                        EligibilityEnd::SourceFenceBreach
                    } else {
                        EligibilityEnd::Domain {
                            violation: Violation::ActivationBaselineUnavailable,
                        }
                    })
                );
                let (reached, response) = tokio::time::timeout(Duration::from_secs(5), async {
                    tokio::join!(&mut owner, request_finalize(&root, "migration"))
                })
                .await
                .unwrap();
                assert_eq!(response.exit_code, 0);
                assert_eq!(reached.unwrap().state, State::Finalized);
                assert!(active(&root.join("migration.control")).unwrap().is_none());
                drop(pause);
                std::fs::remove_dir_all(&root).unwrap();
            }
        }
    }

    fn snapshot(name: &str) -> preflight::Snapshot {
        preflight::Snapshot {
            observations: vec![preflight::Observation {
                check: preflight::Check::SchemaDescriptor,
                object: None,
                sql: "catalog fixture".into(),
                rows: vec![json!({"column":name})],
                error: None,
            }],
        }
    }
    #[tokio::test]
    async fn activation_baseline_survives_takeover_and_buffer_gc_and_authenticates_content() {
        use super::super::activation_baseline::{Baseline, load};
        use object_store::{ObjectStoreExt, path::Path as ObjectPath};
        let store = Arc::new(object_store::memory::InMemory::new());
        let journal = Journal::new(
            store.clone(),
            "baseline",
            "migration",
            Duration::from_secs(5),
        )
        .unwrap();
        let (journal, mut state, binding) = fenced_on(journal).await;
        let proof = setup_records(&journal, &mut state, &binding).await;
        append(
            &journal,
            &mut state,
            Transition::ReverseConsistentPoint {
                proof: Box::new(proof),
            },
        )
        .await;
        let root = task_root();
        let local = Local::recover(root.join("first"), &journal).await.unwrap();
        let coordinator = Coordinator::observer(journal.clone(), local, "owner").unwrap();
        // Recovering with the retained owner still needs explicit acquisition.
        let mut coordinator = coordinator;
        coordinator.acquire().await.unwrap();
        let engine = Engine::new(coordinator, journal.clone());
        let baseline = Baseline::new(&journal, snapshot("source"), snapshot("destination"));
        baseline.persist(&journal, &engine.runtime()).await.unwrap();
        let buffer = ObjectPath::from("buffer/migration/forward/segment");
        store.put(&buffer, vec![1, 2, 3].into()).await.unwrap();
        store.delete(&buffer).await.unwrap();
        drop(engine);
        let local = Local::recover(root.join("takeover"), &journal)
            .await
            .unwrap();
        let mut coordinator = Coordinator::observer(journal.clone(), local, "takeover").unwrap();
        coordinator.acquire().await.unwrap();
        let restored = load(&journal).await.unwrap().unwrap();
        assert!(restored.matches(&snapshot("destination")).unwrap());
        assert!(!restored.matches(&snapshot("changed")).unwrap());
        let state = journal.load(true).await.unwrap();
        let (_, key) = &state.progress[super::super::activation_baseline::STREAM];
        store
            .put(&ObjectPath::from(key.as_str()), b"{}".to_vec().into())
            .await
            .unwrap();
        let failure = load(&journal).await.err().unwrap();
        assert_eq!(
            baseline_failure(failure),
            Ok(Some(Violation::DestinationCatalogDrift))
        );
        store.delete(&ObjectPath::from(key.as_str())).await.unwrap();
        let failure = load(&journal).await.err().unwrap();
        assert_eq!(baseline_failure(failure), Err(()));

        // A successfully fetched, content-addressed object can still have an
        // invalid format. These are permanent failures, unlike a failed read.
        use sha2::{Digest, Sha256};
        let engine = Engine::new(coordinator, journal.clone());
        let valid = serde_json::to_value(&restored).unwrap();
        let mut version = valid.clone();
        version["version"] = json!(2);
        let mut namespace = valid.clone();
        namespace["namespace"] = json!("another migration");
        let mut descriptors = valid.clone();
        descriptors["destination"]["observations"] = json!([]);
        for bytes in [
            b"not json".to_vec(),
            serde_json::to_vec(&version).unwrap(),
            serde_json::to_vec(&namespace).unwrap(),
            serde_json::to_vec(&descriptors).unwrap(),
            vec![b' '; 4 * 1024 * 1024 + 1],
        ] {
            let digest = format!("{:x}", Sha256::digest(&bytes));
            let key = format!(
                "activation-catalog/{:x}/{digest}.json",
                Sha256::digest(journal.namespace().as_bytes())
            );
            store
                .put(&ObjectPath::from(key.as_str()), bytes.into())
                .await
                .unwrap();
            engine
                .runtime()
                .marker(super::super::activation_baseline::STREAM, digest, key)
                .await
                .unwrap();
            let failure = load(&journal).await.err().unwrap();
            assert_eq!(
                baseline_failure(failure),
                Ok(Some(Violation::DestinationCatalogDrift))
            );
        }
        for (digest, key) in [("invalid", "untrusted"), (&"0".repeat(64), "wrong-path")] {
            engine
                .runtime()
                .marker(
                    super::super::activation_baseline::STREAM,
                    digest.into(),
                    key.into(),
                )
                .await
                .unwrap();
            let failure = load(&journal).await.err().unwrap();
            assert_eq!(
                baseline_failure(failure),
                Ok(Some(Violation::DestinationCatalogDrift))
            );
        }
    }
    #[test]
    fn baseline_infrastructure_errors_remain_retryable() {
        use super::super::activation_baseline::LoadError;
        for error in [
            coordinator::Error::Io(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "baseline read timeout",
            )),
            coordinator::Error::Io(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "journal unavailable",
            )),
            coordinator::Error::UnknownOutcome("object-store unavailable".into()),
        ] {
            assert_eq!(baseline_failure(LoadError::from(error)), Err(()));
        }
        assert_eq!(
            baseline_failure(LoadError::Invalid),
            Ok(Some(Violation::DestinationCatalogDrift))
        );
    }

    #[tokio::test]
    async fn source_breach_is_journaled_once_and_replayed_as_a_critical_alert() {
        let (journal, _) = activated().await;
        let root = task_root();
        let local = Local::recover(root.join("breach"), &journal).await.unwrap();
        let mut coordinator =
            Coordinator::observer(journal.clone(), local, "breach-owner").unwrap();
        coordinator.acquire().await.unwrap();
        let engine = Engine::new(coordinator, journal.clone());
        admit(&engine.runtime(), true).await.unwrap();
        end_workers(&engine.runtime(), EligibilityEnd::SourceFenceBreach)
            .await
            .unwrap();
        let seq = journal.load(true).await.unwrap().applied_seq;
        end_workers(&engine.runtime(), EligibilityEnd::SourceFenceBreach)
            .await
            .unwrap();
        let state = journal.load(true).await.unwrap();
        assert_eq!(state.applied_seq, seq);
        assert_eq!(state.lifecycle.state, State::RollbackWindow);
        let status = super::super::reverse_workers::status(&state);
        assert_eq!(status["critical_alert"], "source-fence-breach");
        assert_eq!(status["eligible"], false);
        finalize(&engine.runtime()).await.unwrap();
        drop(engine);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn unhealthy_admission_retries_then_enters_window_only_when_healthy() {
        let (journal, _) = activated().await;
        let root = task_root();
        let local = Local::recover(root.join("admission"), &journal)
            .await
            .unwrap();
        let mut coordinator = Coordinator::observer(journal.clone(), local, "retry-owner").unwrap();
        coordinator.acquire().await.unwrap();
        let engine = Engine::new(coordinator, journal.clone());
        for _ in 0..3 {
            admit(&engine.runtime(), false).await.unwrap();
            let state = journal.load(true).await.unwrap();
            assert_eq!(state.lifecycle.state, State::DestinationActive);
            assert!(state.rollback_eligibility_ended.is_none());
        }
        admit(&engine.runtime(), true).await.unwrap();
        assert_eq!(
            journal.load(true).await.unwrap().lifecycle.state,
            State::RollbackWindow
        );
        engine
            .runtime()
            .commit(Transition::RollbackEligibilityEnded {
                reason: EligibilityEnd::Domain {
                    violation: Violation::DestinationCatalogDrift,
                },
            })
            .await
            .unwrap();
        let seq = journal.load(true).await.unwrap().applied_seq;
        admit(&engine.runtime(), true).await.unwrap();
        assert_eq!(journal.load(true).await.unwrap().applied_seq, seq);
    }

    #[tokio::test]
    async fn eligible_but_unhealthy_destination_active_refuses_finalize() {
        let (journal, _) = activated().await;
        let root = task_root();
        let local = Local::recover(root.join("finalize"), &journal)
            .await
            .unwrap();
        let mut coordinator = Coordinator::observer(journal.clone(), local, "finalizer").unwrap();
        coordinator.acquire().await.unwrap();
        let engine = Engine::new(coordinator, journal.clone());
        admit(&engine.runtime(), false).await.unwrap();
        assert!(finalize(&engine.runtime()).await.is_err());
        let state = journal.load(true).await.unwrap();
        assert_eq!(state.lifecycle.state, State::DestinationActive);
        assert_eq!(state.rollback_eligibility_ended, None);
    }

    #[tokio::test]
    async fn admitted_finalize_resumes_before_probes_and_records_intent_before_lifecycle() {
        let (journal, _) = activated().await;
        let root = task_root();
        let local = Local::recover(root.join("resume-finalize"), &journal)
            .await
            .unwrap();
        let mut coordinator = Coordinator::observer(journal.clone(), local, "finalizer").unwrap();
        coordinator.acquire().await.unwrap();
        let engine = Engine::new(coordinator, journal.clone());
        let runtime = engine.runtime();
        admit(&runtime, true).await.unwrap();
        runtime
            .marker(
                super::super::finalize::REQUEST,
                "false".into(),
                "intent".into(),
            )
            .await
            .unwrap();
        let admitted = journal.load(true).await.unwrap();
        assert_eq!(admitted.lifecycle.state, State::RollbackWindow);
        assert!(admitted.rollback_eligibility_ended.is_none());
        let (_pause, receive) = watch::channel(false);
        let reached = serve(runtime, &root, "migration", receive, None)
            .await
            .unwrap();
        assert_eq!(reached.state, State::Finalized);
        let done = journal.load(true).await.unwrap();
        assert_eq!(
            done.rollback_eligibility_ended,
            Some(EligibilityEnd::OperatorFinalized)
        );
        assert!(done.applied_seq > admitted.applied_seq);
    }

    #[tokio::test]
    async fn rollback_requests_reject_stale_generation_and_ineligible_window() {
        let (journal, _) = activated().await;
        let root = task_root();
        let local = Local::recover(root.join("rollback-request"), &journal)
            .await
            .unwrap();
        let mut coordinator =
            Coordinator::observer(journal.clone(), local, "rollback-owner").unwrap();
        coordinator.acquire().await.unwrap();
        let engine = Engine::new(coordinator, journal.clone());
        let runtime = engine.runtime();
        admit(&runtime, true).await.unwrap();
        end_workers(&runtime, EligibilityEnd::SourceFenceBreach)
            .await
            .unwrap();
        let before = journal.load(true).await.unwrap().applied_seq;
        let (pause, receive) = watch::channel(false);
        let owner = serve(runtime.clone(), &root, "migration", receive, None);
        let requests = async {
            let control = root.join("migration.control");
            let generation = loop {
                if let Some(value) = active(&control).unwrap() {
                    break value;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            };
            let stale = uuid::Uuid::new_v4().to_string();
            for (generation, expected, kind) in [
                (stale, 8, "stale_generation"),
                (generation, 3, "rollback_ineligible"),
            ] {
                let request = uuid::Uuid::new_v4().to_string();
                let path = control.join(format!("rollback-{generation}-{request}.request"));
                write_json(
                    &path,
                    &json!({"generation":generation,"request":request,"deadline_ms":30_000}),
                )
                .unwrap();
                let response = loop {
                    if let Ok(bytes) = std::fs::read(path.with_extension("response")) {
                        break serde_json::from_slice::<VerifyResponse>(&bytes).unwrap();
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                };
                assert_eq!(response.exit_code, expected);
                assert_eq!(response.data["error"]["kind"], kind);
            }
            assert_eq!(journal.load(true).await.unwrap().applied_seq, before);
            pause.send(true).unwrap();
        };
        let (reached, ()) = tokio::time::timeout(Duration::from_secs(10), async {
            tokio::join!(owner, requests)
        })
        .await
        .unwrap();
        assert_eq!(reached.unwrap().state, State::RollbackWindow);
        drop(engine);
        std::fs::remove_dir_all(root).unwrap();
    }

    struct FakeRollback {
        model: crate::rollback::driver::tests::OwnerModel,
        stop: Option<coordinator::rollback::Step>,
        expired: bool,
    }
    impl super::super::rollback::OwnerDriver for FakeRollback {
        fn simulated_healthy(&self) -> bool {
            true
        }
        async fn slice(
            &mut self,
            runtime: &crate::engine::Runtime,
            _: &Path,
            id: &str,
            _: Option<&(coordinator::journal::Journal, MigrationDefinition)>,
        ) -> coordinator::Result<crate::cutover::Result<crate::rollback::driver::Outcome>> {
            if self.stop == runtime.state().await.rollback.as_ref().map(|c| c.next) {
                self.stop = None;
                return Err(coordinator::Error::LostOwnership);
            }
            if self.expired {
                tokio::time::sleep(Duration::from_millis(150)).await;
            }
            Ok(self.model.tick(runtime.clone(), id.into()).await)
        }
    }
    async fn rollback_engine(
        journal: &coordinator::journal::Journal,
        root: &Path,
        name: &str,
    ) -> Engine {
        let local = Local::recover(root.join(name), journal).await.unwrap();
        let mut coordinator = Coordinator::observer(journal.clone(), local, name).unwrap();
        let exclusion = MigrationExclusion::file(root.join("rollback-locks")).unwrap();
        coordinator = coordinator.with_exclusion(exclusion.clone());
        coordinator.acquire().await.unwrap();
        Engine::new_with_exclusion(coordinator, journal.clone(), exclusion)
    }
    #[tokio::test]
    async fn owner_rollback_resumes_each_driver_step_after_loss_of_ownership() {
        use coordinator::rollback::{Record, Step};
        for step in [
            Step::PreHook,
            Step::FenceDestination,
            Step::Drain,
            Step::FinalMarker,
            Step::ReverseApply,
            Step::Verify,
            Step::Sequences,
            Step::RestoreSource,
            Step::PostHook,
        ] {
            let (journal, _) = activated().await;
            let root = task_root();
            let engine = rollback_engine(&journal, &root, "first").await;
            let runtime = engine.runtime();
            admit(&runtime, true).await.unwrap();
            runtime
                .commit(Transition::Rollback {
                    record: Box::new(Record::Admit {
                        request: "r".into(),
                        deadline_at_ms: u64::MAX,
                    }),
                })
                .await
                .unwrap();
            let mut fake = FakeRollback {
                model: Default::default(),
                stop: Some(step),
                expired: false,
            };
            let (_send, receive) = watch::channel(false);
            let result = serve_with_driver(
                runtime.clone(),
                &root,
                "migration",
                receive,
                None,
                &mut fake,
            )
            .await;
            assert!(
                matches!(result, Err(coordinator::Error::LostOwnership)),
                "{step:?}"
            );
            let control = root.join("migration.control");
            let generation = active(&control).unwrap();
            // Dropping the owner retires its generation, but its response is retained.
            let responses = std::fs::read_dir(&control)
                .unwrap()
                .filter_map(Result::ok)
                .filter(|e| e.path().extension().is_some_and(|s| s == "response"))
                .collect::<Vec<_>>();
            assert_eq!(responses.len(), 1);
            let response: VerifyResponse =
                serde_json::from_slice(&std::fs::read(responses[0].path()).unwrap()).unwrap();
            assert_eq!(response.exit_code, 8);
            assert!(generation.is_none());
            drop(runtime);
            drop(engine);
            let engine = rollback_engine(&journal, &root, "second").await;
            let (_send, receive) = watch::channel(false);
            assert_eq!(
                serve_with_driver(
                    engine.runtime(),
                    &root,
                    "migration",
                    receive,
                    None,
                    &mut fake
                )
                .await
                .unwrap()
                .state,
                State::SourceActiveAfterRollback
            );
            fake.model.assert_complete();
            drop(engine);
            std::fs::remove_dir_all(root).unwrap();
        }
    }
    #[tokio::test]
    async fn owner_takeover_resumes_deadline_destination_restoration() {
        use coordinator::rollback::{Record, Step};
        let (journal, _) = activated().await;
        let root = task_root();
        let engine = rollback_engine(&journal, &root, "abort-first").await;
        let runtime = engine.runtime();
        admit(&runtime, true).await.unwrap();
        let deadline = u64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis(),
        )
        .unwrap()
            + 2000;
        runtime
            .commit(Transition::Rollback {
                record: Box::new(Record::Admit {
                    request: "r".into(),
                    deadline_at_ms: deadline,
                }),
            })
            .await
            .unwrap();
        let mut fake = FakeRollback {
            model: Default::default(),
            stop: Some(Step::RestoreDestination),
            expired: false,
        };
        // Establish the destination fence, then simulate a crash after the durable
        // abort decision but before its compensating effect.
        fake.model
            .tick(runtime.clone(), "migration".into())
            .await
            .unwrap();
        fake.model
            .tick(runtime.clone(), "migration".into())
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_secs(2)).await;
        runtime
            .commit(Transition::Rollback {
                record: Box::new(Record::DeadlineAbort),
            })
            .await
            .unwrap();
        let (_send, receive) = watch::channel(false);
        assert!(matches!(
            serve_with_driver(
                runtime.clone(),
                &root,
                "migration",
                receive,
                None,
                &mut fake
            )
            .await,
            Err(coordinator::Error::LostOwnership)
        ));
        drop(runtime);
        drop(engine);
        let engine = rollback_engine(&journal, &root, "abort-second").await;
        let (send, receive) = watch::channel(false);
        let stop = async {
            loop {
                if journal.load(true).await.unwrap().lifecycle.state == State::RollbackWindow {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            send.send(true).unwrap();
        };
        let (result, ()) = tokio::time::timeout(Duration::from_secs(15), async {
            tokio::join!(
                serve_with_driver(
                    engine.runtime(),
                    &root,
                    "migration",
                    receive,
                    None,
                    &mut fake
                ),
                stop
            )
        })
        .await
        .unwrap();
        assert_eq!(result.unwrap().state, State::RollbackWindow);
        assert_eq!(
            journal
                .load(true)
                .await
                .unwrap()
                .rollback
                .as_ref()
                .unwrap()
                .next,
            Step::Aborted
        );
        fake.model.assert_aborted();
        drop(engine);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn owner_deadline_abort_restarts_capture_and_returns_to_window() {
        let (journal, _) = activated().await;
        let root = task_root();
        let engine = rollback_engine(&journal, &root, "deadline-owner").await;
        admit(&engine.runtime(), true).await.unwrap();
        let mut fake = FakeRollback {
            model: Default::default(),
            stop: None,
            expired: true,
        };
        let (send, receive) = watch::channel(false);
        let request = async {
            loop {
                if active(&root.join("migration.control")).unwrap().is_some() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            let response = super::super::rollback::request_rollback(
                &root,
                "migration",
                Duration::from_millis(100),
            )
            .await;
            send.send(true).unwrap();
            response
        };
        let (reached, response) = tokio::time::timeout(Duration::from_secs(15), async {
            tokio::join!(
                serve_with_driver(
                    engine.runtime(),
                    &root,
                    "migration",
                    receive,
                    None,
                    &mut fake
                ),
                request
            )
        })
        .await
        .unwrap();
        assert_eq!(reached.unwrap().state, State::RollbackWindow);
        assert_eq!(response.exit_code, 2);
        assert_eq!(response.data["rollback"]["outcome"], "deadline-aborted");
        fake.model.assert_aborted();
        drop(engine);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn eligible_rollback_request_is_admitted_and_completed_through_owner() {
        let (journal, _) = activated().await;
        let root = task_root();
        let engine = rollback_engine(&journal, &root, "request-owner").await;
        admit(&engine.runtime(), true).await.unwrap();
        let mut fake = FakeRollback {
            model: Default::default(),
            stop: None,
            expired: false,
        };
        let (send, receive) = watch::channel(false);
        let request = async {
            loop {
                if active(&root.join("migration.control")).unwrap().is_some() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            let response = super::super::rollback::request_rollback(
                &root,
                "migration",
                Duration::from_secs(30),
            )
            .await;
            if response.exit_code != 0 {
                send.send(true).unwrap();
            }
            response
        };
        let (reached, response) = tokio::time::timeout(Duration::from_secs(30), async {
            tokio::join!(
                serve_with_driver(
                    engine.runtime(),
                    &root,
                    "migration",
                    receive,
                    None,
                    &mut fake
                ),
                request
            )
        })
        .await
        .unwrap();
        assert_eq!(response.exit_code, 0, "{}", response.data);
        assert_eq!(reached.unwrap().state, State::SourceActiveAfterRollback);
        assert_eq!(response.data["complete"], true);
        assert_eq!(response.data["rollback"]["outcome"], "completed");
        fake.model.assert_complete();
        drop(engine);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn admitted_rollback_survives_takeover_without_probes_or_finalize() {
        let (journal, _) = activated().await;
        let root = task_root();
        let local = Local::recover(root.join("rollback-admit"), &journal)
            .await
            .unwrap();
        let mut coordinator =
            Coordinator::observer(journal.clone(), local, "rollback-first").unwrap();
        coordinator.acquire().await.unwrap();
        let engine = Engine::new(coordinator, journal.clone());
        let runtime = engine.runtime();
        admit(&runtime, true).await.unwrap();
        // Missing live endpoint context fails closed and durably latches operator
        // action, preserving the admitted identity and deadline across takeover.
        runtime
            .commit(Transition::Rollback {
                record: Box::new(coordinator::rollback::Record::Admit {
                    request: "rollback-request".into(),
                    deadline_at_ms: u64::try_from(
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_millis(),
                    )
                    .unwrap()
                        + 30_000,
                }),
            })
            .await
            .unwrap();
        let admitted = journal.load(true).await.unwrap();
        assert_eq!(admitted.lifecycle.state, State::RollingBack);
        let checkpoint = admitted.rollback.clone().unwrap();
        assert_eq!(checkpoint.request, "rollback-request");
        assert!(checkpoint.intent.is_none());
        assert_eq!(
            super::super::rollback::run_with_driver(
                &runtime,
                &root,
                "migration",
                None,
                &mut super::super::rollback::Live::default(),
                &mut None
            )
            .await
            .unwrap()
            .unwrap()
            .exit_code,
            5
        );
        assert!(journal.load(true).await.unwrap().applied_seq > admitted.applied_seq);
        drop(runtime);
        drop(engine);
        let local = Local::recover(root.join("rollback-takeover"), &journal)
            .await
            .unwrap();
        let mut coordinator =
            Coordinator::observer(journal.clone(), local, "rollback-second").unwrap();
        coordinator.acquire().await.unwrap();
        let engine = Engine::new(coordinator, journal.clone());
        let recovered = journal.load(true).await.unwrap();
        assert!(recovered.epoch > admitted.epoch);
        assert_eq!(recovered.rollback.as_ref(), Some(&checkpoint));
        let (pause, receive) = watch::channel(false);
        let owner = serve(engine.runtime(), &root, "migration", receive, None);
        let stop = async {
            tokio::time::sleep(Duration::from_millis(100)).await;
            pause.send(true).unwrap();
        };
        let (reached, ()) =
            tokio::time::timeout(Duration::from_secs(5), async { tokio::join!(owner, stop) })
                .await
                .unwrap();
        assert_eq!(reached.unwrap().state, State::RollingBack);
        let after = journal.load(true).await.unwrap();
        assert_eq!(after.applied_seq, recovered.applied_seq);
        assert!(after.rollback_eligibility_ended.is_none());
        assert_eq!(after.rollback.as_ref(), Some(&checkpoint));
        drop(engine);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn terminal_states_return_without_starting_workers() {
        for terminal in [
            State::Finalized,
            State::SourceActiveAfterRollback,
            State::AbortedToSource,
        ] {
            let (journal, mut state) = if terminal == State::AbortedToSource {
                let (journal, state, _) = fenced().await;
                (journal, state)
            } else {
                activated().await
            };
            if terminal == State::Finalized {
                append(
                    &journal,
                    &mut state,
                    Transition::RollbackEligibilityEnded {
                        reason: EligibilityEnd::SourceFenceBreach,
                    },
                )
                .await;
            } else if terminal == State::SourceActiveAfterRollback {
                for to in [State::RollbackWindow, State::RollingBack] {
                    append(
                        &journal,
                        &mut state,
                        Transition::Lifecycle {
                            event: Event::Advance { to },
                        },
                    )
                    .await;
                }
            }
            append(
                &journal,
                &mut state,
                Transition::Lifecycle {
                    event: Event::Advance { to: terminal },
                },
            )
            .await;
            let root = task_root();
            let local = Local::recover(root.join("owner"), &journal).await.unwrap();
            let mut coordinator =
                Coordinator::observer(journal.clone(), local, "terminal").unwrap();
            coordinator.acquire().await.unwrap();
            let engine = Engine::new(coordinator, journal.clone());
            let (_send, receive) = watch::channel(false);
            let reached = tokio::time::timeout(
                Duration::from_secs(2),
                serve(engine.runtime(), &root, "migration", receive, None),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(reached.state, terminal);
            assert!(active(&root.join("migration.control")).unwrap().is_none());
            std::fs::remove_dir_all(root).unwrap();
        }
    }
    #[test]
    fn rollback_runtime_journal_callbacks_work_inside_async_apply_and_recover() {
        use crate::coordinator::rollback::{Record, Step};
        use crate::rollback::{apply::Journal as _, driver::RuntimeJournal};
        let executor = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let (journal, _) = executor.block_on(activated());
        let root = task_root();
        let local = executor
            .block_on(Local::recover(root.join("rollback-bridge"), &journal))
            .unwrap();
        let mut coordinator =
            Coordinator::observer(journal.clone(), local, "bridge-owner").unwrap();
        executor.block_on(coordinator.acquire()).unwrap();
        let engine = Engine::new(coordinator, journal.clone());
        let runtime = engine.runtime();
        executor.block_on(admit(&runtime, true)).unwrap();
        // Exercise authenticated recovery of an admission; the CLI gate stays off.
        executor
            .block_on(runtime.commit(Transition::Rollback {
                record: Box::new(Record::Admit {
                    request: "bridge-request".into(),
                    deadline_at_ms: u64::MAX,
                }),
            }))
            .unwrap();
        let mut adapter = RuntimeJournal::new(&runtime, Default::default()).unwrap();
        executor.block_on(async {
            crate::rollback::destination::Journal::publish(
                &mut adapter,
                Record::Intent {
                    step: Step::PreHook,
                },
            )
            .unwrap();
            crate::rollback::destination::Journal::native(
                &mut adapter,
                &crate::cutover::Record::Drain(Default::default()),
            )
            .unwrap();
            crate::rollback::destination::Journal::publish(
                &mut adapter,
                Record::Checkpoint {
                    step: Step::PreHook,
                    final_marker: None,
                },
            )
            .unwrap();
            adapter
                .save("bridge-request", Default::default(), true)
                .unwrap();
        });
        let recovered = RuntimeJournal::new(&runtime, Default::default()).unwrap();
        assert_eq!(
            crate::rollback::destination::Journal::state(&recovered)
                .rollback
                .as_ref()
                .unwrap()
                .next,
            Step::FenceDestination
        );
        assert_eq!(recovered.destination_records().unwrap().len(), 1);
        assert!(recovered.progress("bridge-request").unwrap().1);
        assert!(recovered.progress("other-request").is_err());
        drop(adapter);
        drop(recovered);
        drop(runtime);
        drop(engine);
        std::fs::remove_dir_all(root).unwrap();
    }
}
