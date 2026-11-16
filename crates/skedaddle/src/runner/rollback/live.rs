//! One owner slice. All endpoint adapters are reconstructed after an error.
use super::*;
use crate::runner::{self, reverse_workers::Workers};
use crate::{
    cutover,
    pg::{activate, fence},
    rollback::{apply, destination, driver, source},
    verify::{self, RowSource},
};
use std::sync::Arc;

fn retry<T>(_: T) -> cutover::Error {
    cutover::Error::Component("rollback endpoint unavailable (details redacted)".into())
}
fn invalid<T>(_: T) -> cutover::Error {
    cutover::Error::Invalid("rollback durable evidence unavailable")
}
#[derive(Clone)]
struct Order(crate::pg::catalog::RangeClassifier);
impl verify::KeyOrder for Order {
    fn compare_keys(&self, a: &verify::Key, b: &verify::Key) -> verify::Result<std::cmp::Ordering> {
        self.0
            .compare_keys(a, b)
            .map_err(|_| verify::VerifyError::Invalid("rollback key order"))
    }
}
struct Verification {
    source: crate::pg::tls::Config,
    destination: crate::pg::tls::Config,
    tables: Vec<crate::pg::catalog::TableCatalog>,
    journal: coordinator::journal::Journal,
    state: Materialized,
    id: String,
    max_bytes: u64,
}
impl apply::Verification for Verification {
    async fn verify(&mut self) -> apply::Result<()> {
        if self.tables.is_empty() {
            return Err(apply::Error::OperatorAction(
                "rollback table inventory missing",
            ));
        }
        for table in &self.tables {
            let reference = self
                .state
                .chunk_plans
                .get(&table.key.table)
                .ok_or(apply::Error::OperatorAction("rollback plan missing"))?;
            let plan = self
                .journal
                .chunk_plan(reference)
                .await
                .map_err(|_| apply::Error::Retry)?;
            let order = Order(
                table
                    .range_classifier(&plan, &format!("skedaddle-postgres:{}", self.id))
                    .map_err(|_| apply::Error::OperatorAction("rollback plan differs"))?,
            );
            let columns = table
                .columns
                .iter()
                .map(|c| verify::sql::Column {
                    name: c.name.clone(),
                    pg_type: c.pg_type.clone(),
                })
                .collect::<Vec<_>>();
            let cap = verify::sql::RangeBudget {
                max_rows: 100_000,
                max_bytes: self.max_bytes,
            };
            let mut source = verify::sql::PgRowSource::connect(
                &self.source,
                verify::sql::Side::Source,
                table.key.clone(),
                columns.clone(),
                order.clone(),
                cap,
            )
            .await
            .map_err(|_| apply::Error::Retry)?;
            let mut destination = verify::sql::PgRowSource::connect(
                &self.destination,
                verify::sql::Side::Destination,
                table.key.clone(),
                columns,
                order.clone(),
                cap,
            )
            .await
            .map_err(|_| apply::Error::Retry)?;
            let projection = verify::Projection::new(table.selection.columns.clone())
                .map_err(|_| apply::Error::OperatorAction("rollback projection invalid"))?;
            // Both endpoints are fenced: compare every saved range, including extras.
            for range in &plan.ranges {
                let left = destination
                    .fetch(range, &projection)
                    .await
                    .map_err(|_| apply::Error::Retry)?;
                let right = source
                    .fetch(range, &projection)
                    .await
                    .map_err(|_| apply::Error::Retry)?;
                let result = verify::compare_rows(&order, range, &projection, &left, &right)
                    .map_err(|_| apply::Error::OperatorAction("rollback comparison invalid"))?;
                if !result.differences.is_empty() {
                    return Err(apply::Error::OperatorAction(
                        "source differs from destination",
                    ));
                }
            }
        }
        Ok(())
    }
}
struct Hooks<'a> {
    runtime: &'a Runtime,
    executor: tokio::runtime::Handle,
    definition: &'a crate::MigrationDefinition,
    request: &'a str,
}
impl Hooks<'_> {
    fn run(&mut self, phase: crate::definition::HookPhase) -> cutover::Result<()> {
        for (index, hook) in self
            .definition
            .hooks
            .iter()
            .enumerate()
            .filter(|(_, h)| h.phase == phase)
        {
            let stream = serde_json::json!(["rollback-hook-v1", self.request, index]).to_string();
            let state = self.executor.block_on(self.runtime.state());
            if state
                .progress
                .get(&stream)
                .is_some_and(|(value, _)| value == "succeeded")
            {
                continue;
            }
            let hook = crate::hooks::OperatorHook::try_from(hook).map_err(invalid)?;
            self.executor
                .block_on(
                    self.runtime
                        .marker(&stream, "intent".into(), "rollback".into()),
                )
                .map_err(retry)?;
            crate::hooks::run_phase_then(
                hook.phase,
                &[hook],
                &crate::security::Redactor::default(),
                |_| {},
                || (),
            )
            .map_err(|error| match error.outcome {
                crate::hooks::HookOutcome::Failed
                | crate::hooks::HookOutcome::InvalidConfiguration => {
                    cutover::Error::Invalid("rollback hook failed")
                }
                _ => retry(()),
            })?;
            self.executor
                .block_on(
                    self.runtime
                        .marker(&stream, "succeeded".into(), "rollback".into()),
                )
                .map_err(retry)?;
        }
        Ok(())
    }
}
impl destination::PreHook for Hooks<'_> {
    fn run(&mut self, _: &cutover::Key) -> cutover::Result<()> {
        self.run(crate::definition::HookPhase::PreRollback)
    }
}
impl source::PostHook for Hooks<'_> {
    fn run(&mut self, _: &cutover::Key) -> apply::Result<()> {
        self.run(crate::definition::HookPhase::PostRollback)
            .map_err(|e| match e {
                cutover::Error::Invalid(s) => apply::Error::OperatorAction(s),
                _ => apply::Error::Retry,
            })
    }
}
/// A dedicated thread retains non-Send transaction assembly between slices.
/// Dropping the sender stops the idle thread and closes its directly owned sockets.
type Reply = tokio::sync::oneshot::Sender<cutover::Result<driver::Outcome>>;
#[derive(Default)]
pub(super) struct Thread {
    send: Option<std::sync::mpsc::Sender<Reply>>,
}
impl Thread {
    pub(super) async fn slice(
        &mut self,
        runtime: &Runtime,
        id: &str,
        journal: &coordinator::journal::Journal,
        definition: &crate::MigrationDefinition,
        root: &Path,
    ) -> coordinator::Result<cutover::Result<driver::Outcome>> {
        if self.send.is_none() {
            let (send, receive) = std::sync::mpsc::channel::<
                tokio::sync::oneshot::Sender<cutover::Result<driver::Outcome>>,
            >();
            let runtime = runtime.clone();
            let id = id.to_owned();
            let journal = journal.clone();
            let definition = definition.clone();
            let root = root.to_owned();
            std::thread::Builder::new()
                .name("rollback-capture-owner".into())
                .spawn(move || {
                    let Ok(rt) = tokio::runtime::Builder::new_multi_thread()
                        .worker_threads(1)
                        .enable_all()
                        .build()
                    else {
                        return;
                    };
                    let rt = Arc::new(rt);
                    let handle = rt.handle().clone();
                    let mut retained = None;
                    while let Ok(reply) = receive.recv() {
                        let result = (|| {
                            handle
                                .block_on(runtime.refresh_owner())
                                .map_err(|e| match e {
                                    coordinator::Error::LostOwnership
                                    | coordinator::Error::Observer => cutover::Error::LostOwnership,
                                    _ => retry(e),
                                })?;
                            if retained.is_none() {
                                retained = Some(
                                    rt.block_on(Workers::open(
                                        &runtime,
                                        &journal,
                                        &definition,
                                        &root,
                                        &id,
                                    ))
                                    .map_err(|e| match e {
                                        runner::reverse_workers::Failure::Retry => retry(()),
                                        runner::reverse_workers::Failure::End(_) => {
                                            cutover::Error::Invalid(
                                                "reverse capture evidence differs",
                                            )
                                        }
                                    })?,
                                );
                            }
                            let worker = retained.as_mut().expect("opened capture");
                            run_slice(&runtime, &id, &journal, &definition, &rt, &handle, worker)
                        })();
                        if !matches!(
                            result,
                            Ok(driver::Outcome::Progress) | Err(cutover::Error::Pending)
                        ) {
                            retained = None;
                        }
                        if reply.send(result).is_err() {
                            break;
                        }
                    }
                })
                .map_err(coordinator::Error::Io)?;
            self.send = Some(send);
        }
        let (reply, response) = tokio::sync::oneshot::channel();
        if self
            .send
            .as_ref()
            .expect("started thread")
            .send(reply)
            .is_err()
        {
            self.send = None;
            return Err(coordinator::Error::RecoveryRequired(
                "rollback worker stopped; retrying".into(),
            ));
        }
        response.await.map_err(|_| {
            coordinator::Error::RecoveryRequired("rollback worker failed; retrying".into())
        })
    }
}
fn run_slice(
    runtime: &Runtime,
    id: &str,
    journal: &coordinator::journal::Journal,
    definition: &crate::MigrationDefinition,
    rt: &Arc<tokio::runtime::Runtime>,
    handle: &tokio::runtime::Handle,
    worker: &mut Workers,
) -> cutover::Result<driver::Outcome> {
    let state = handle.block_on(runtime.state());
    let cp = state
        .rollback
        .as_ref()
        .ok_or(cutover::Error::Invalid("rollback admission missing"))?;
    let key = cutover::Key {
        migration: id.into(),
        cutover: cp.request.clone(),
        epoch: state.epoch.try_into().map_err(invalid)?,
        coordinator: state.owner.clone(),
    };
    let (original, destination_config) =
        runner::cutover::rollback_fences(&state).map_err(invalid)?;
    if original.saved.is_none() || original.next != fence::Step::Complete {
        return Err(cutover::Error::Invalid("original source fence incomplete"));
    }
    let mut redactor = crate::security::Redactor::default();
    let sc = runner::config(&definition.source, &mut redactor).map_err(retry)?;
    let dc = runner::config(&definition.destination, &mut redactor).map_err(retry)?;
    let open_source = || runner::cutover::Pg::open(rt.clone(), &sc, &definition.source);
    let open_destination = || runner::cutover::Pg::open(rt.clone(), &dc, &definition.destination);
    let mut log = driver::RuntimeJournal::new(runtime, Default::default())?;
    let native_key = destination::destination_key(&key, &cp.request);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(invalid)?
        .as_millis()
        .try_into()
        .map_err(invalid)?;
    let records = log.destination_records()?;
    let connection = open_destination()?;
    // Once native intents exist, replay their authenticated config. Revalidating
    // after revoking grants would mistake our own fence for stale inventory.
    let acknowledged = if records.is_empty()
        && cp.next == coordinator::rollback::Step::FenceDestination
        && now < cp.deadline_at_ms
    {
        connection.destination_acknowledgments(
            &destination_config.tables,
            &destination_config.applications,
            &definition
                .fence_plan
                .acknowledged_destination_residual_risks,
        )?
    } else {
        Default::default()
    };
    let mut destination = cutover::pg::Source {
        connection,
        state: fence::Fence::new(fence::FenceConfig {
            epoch: key.epoch,
            applications: destination_config.applications,
            tables: destination_config.tables,
            acknowledged,
            deadline_ms: 30_000,
        })
        .map_err(invalid)?,
        drained: Default::default(),
        pre_fence: || Ok(()),
    };
    destination::replay_native(&mut destination, &native_key, &records)?;
    let mut source = source::Pg {
        source: open_source()?,
        destination: open_destination()?,
        scope: activate::SequenceScope {
            tables: original.config.tables.clone(),
            sequences: vec![],
        },
        original,
        timeout: Duration::from_secs(5),
    };
    let replay = apply::Replay {
        store: runner::operational::buffer(definition, true).map_err(retry)?,
        order: Arc::new(runner::operational::Order(format!("{id}_rev"))),
        required_next_mseq: cp.capture_boundary.as_ref().map_or(0, |b| b.next_mseq),
        tables: worker
            .tables
            .iter()
            .map(|t| t.destination.clone())
            .collect(),
    };
    let mut target = crate::pg::apply::executor::Executor::new(
        runner::operational::Connect {
            config: sc.clone(),
            connection: definition.source.clone(),
            drivers: vec![],
        },
        format!("{id}_rev"),
        format!("skedaddle_{id}_rev"),
        1000,
        crate::pg::apply::SourcePositions::native_pg(format!("{id}_rev")),
    )
    .map_err(invalid)?;
    // Verification uses the original source-side key inventory and saved plans.
    let mut verification = Verification {
        source: sc,
        destination: dc,
        tables: worker.tables.clone(),
        journal: journal.clone(),
        state: state.clone(),
        id: id.into(),
        max_bytes: definition.budgets.max_snapshot_bytes,
    };
    let mut pre = Hooks {
        runtime,
        executor: handle.clone(),
        definition,
        request: &cp.request,
    };
    let mut post = Hooks {
        runtime,
        executor: handle.clone(),
        definition,
        request: &cp.request,
    };
    driver::execute_next(
        &key,
        now,
        &mut runtime.cutover_authority(),
        rt,
        &mut driver::Components {
            destination: &mut destination,
            capture: worker,
            pre_hook: &mut pre,
            replay: &replay,
            target: &mut target,
            verification: &mut verification,
            source: &mut source,
            post_hook: &mut post,
            journal: &mut log,
        },
    )
}
