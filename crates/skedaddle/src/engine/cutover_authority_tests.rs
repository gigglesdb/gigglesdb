use super::*;
use crate::{
    coordinator::{
        Event, MigrationExclusion, ReadinessFacts, State, Transition,
        epoch::Coordinator,
        journal::{self, Journal},
        local::Local,
    },
    engine::Engine,
    pg::{ddl, fence::Connection},
};
use std::{io::Write, path::PathBuf, sync::Arc, time::Duration};

struct Fixture {
    engine: Engine,
    journal: Journal,
    exclusion: Arc<MigrationExclusion>,
    root: PathBuf,
}
impl Fixture {
    async fn new(configured: bool) -> Self {
        let root = PathBuf::from(std::env::var_os("SKEDADDLE_TEST_ROOT").unwrap())
            .join(format!("cutover-authority-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(root.join("journal")).unwrap();
        let journal = Journal::new(
            Arc::new(journal::local_store(root.join("journal")).unwrap()),
            "engine",
            "migration",
            Duration::from_secs(5),
        )
        .unwrap();
        let exclusion = MigrationExclusion::file(root.join("locks")).unwrap();
        let mut coordinator = Coordinator::observer(
            journal.clone(),
            Local::create(root.join("owner")).unwrap(),
            "owner",
        )
        .unwrap();
        if configured {
            coordinator = coordinator.with_exclusion(exclusion.clone());
        }
        coordinator.acquire().await.unwrap();
        let engine = if configured {
            Engine::new_with_exclusion(coordinator, journal.clone(), exclusion.clone())
        } else {
            Engine::new(coordinator, journal.clone())
        };
        Self {
            engine,
            journal,
            exclusion,
            root,
        }
    }
    fn key(&self) -> Key {
        let state = self.engine.runtime().snapshot();
        Key {
            migration: self.journal.migration_id().into(),
            cutover: "cutover".into(),
            epoch: state.epoch.try_into().unwrap(),
            coordinator: state.owner,
        }
    }
    fn competitor(&self, same_arc: bool) -> Coordinator {
        let exclusion = if same_arc {
            self.exclusion.clone()
        } else {
            MigrationExclusion::file(self.root.join("locks")).unwrap()
        };
        Coordinator::observer(
            self.journal.clone(),
            Local::create(self.root.join(format!("other-{}", uuid::Uuid::new_v4()))).unwrap(),
            "other",
        )
        .unwrap()
        .with_exclusion(exclusion)
    }
}
fn executor() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

#[test]
fn run_excludes_independent_promotion_and_allows_runtime_calls() {
    let async_runtime = executor();
    let f = async_runtime.block_on(Fixture::new(true));
    let runtime = f.engine.runtime();
    let mut other = f.competitor(false);
    async_runtime
        .block_on(runtime.progress_marker("buffered", "manifest-durable".into(), "123".into()))
        .unwrap();
    runtime
        .cutover_authority()
        .run(&f.key(), || {
            assert_eq!(
                async_runtime
                    .block_on(f.journal.load(false))
                    .unwrap()
                    .progress["buffered"]
                    .1,
                "123"
            );
            async_runtime
                .block_on(runtime.marker("in-closure", "yes".into(), "1".into()))
                .unwrap();
            assert!(
                async_runtime
                    .block_on(async {
                        tokio::time::timeout(Duration::from_millis(50), other.acquire()).await
                    })
                    .is_err()
            );
            assert_eq!(runtime.snapshot().progress["in-closure"].0, "yes");
            Ok(())
        })
        .unwrap();
    assert_eq!(async_runtime.block_on(other.acquire()).unwrap(), 2);
}

#[test]
fn competing_runtime_authority_waits_without_blocking_active_runtime_calls() {
    let async_runtime = executor();
    let f = async_runtime.block_on(Fixture::new(true));
    let runtime = f.engine.runtime();
    let key = f.key();
    let worker = runtime
        .cutover_authority()
        .run(&key, || {
            let other = runtime.clone();
            let key = key.clone();
            let (started, start) = std::sync::mpsc::channel();
            let (entered, enter) = std::sync::mpsc::channel();
            let worker = std::thread::spawn(move || {
                started.send(()).unwrap();
                other.cutover_authority().run(&key, || {
                    entered.send(()).unwrap();
                    Ok(())
                })
            });
            start.recv_timeout(Duration::from_secs(5)).unwrap();
            assert!(enter.recv_timeout(Duration::from_millis(50)).is_err());
            async_runtime.block_on(async {
                tokio::time::timeout(
                    Duration::from_secs(5),
                    runtime.marker("concurrent", "available".into(), "1".into()),
                )
                .await
                .unwrap()
                .unwrap();
            });
            Ok((worker, enter))
        })
        .unwrap();
    worker.0.join().unwrap().unwrap();
    worker.1.recv_timeout(Duration::from_secs(5)).unwrap();
}

#[test]
fn stale_foreign_and_wrong_migration_keys_are_rejected() {
    let async_runtime = executor();
    let f = async_runtime.block_on(Fixture::new(true));
    let valid = f.key();
    for key in [
        Key {
            epoch: 0,
            ..valid.clone()
        },
        Key {
            epoch: 2,
            ..valid.clone()
        },
        Key {
            epoch: -1,
            ..valid.clone()
        },
        Key {
            coordinator: "foreign".into(),
            ..valid.clone()
        },
        Key {
            migration: "foreign".into(),
            ..valid.clone()
        },
    ] {
        assert_eq!(
            f.engine
                .runtime()
                .cutover_authority()
                .run(&key, || { panic!("invalid ownership reached closure") }),
            Err::<(), _>(cutover::Error::LostOwnership)
        );
    }
    f.engine
        .runtime()
        .cutover_authority()
        .run(&valid, || Ok(()))
        .unwrap();
}

#[test]
fn real_journal_takeover_before_completion_fails_closed() {
    let async_runtime = executor();
    let f = async_runtime.block_on(Fixture::new(true));
    let runtime = f.engine.runtime();
    let mut other = f.competitor(true);
    let result = runtime.cutover_authority().run_with(&f.key(), |held| {
        // Deliberately misuse the real nested-promotion API. It accepts this
        // backend capability, but must invalidate the original owner at exit.
        assert_eq!(
            async_runtime
                .block_on(other.acquire_with_guard(held.guard))
                .unwrap(),
            2
        );
        Ok(())
    });
    assert_eq!(result, Err(cutover::Error::LostOwnership));
    assert_eq!(runtime.snapshot().owner, other.instance_id());
    assert_eq!(runtime.snapshot().epoch, 2);
    assert!(!async_runtime.block_on(runtime.0.lock()).is_owner());
    assert_eq!(
        async_runtime
            .block_on(f.competitor(false).acquire())
            .unwrap(),
        3
    );
}

#[test]
fn closure_error_releases_exclusion_and_preserves_error() {
    let async_runtime = executor();
    let f = async_runtime.block_on(Fixture::new(true));
    assert_eq!(
        f.engine
            .runtime()
            .cutover_authority()
            .run(&f.key(), || { Err::<(), _>(cutover::Error::Pending) }),
        Err(cutover::Error::Pending)
    );
    assert_eq!(
        async_runtime
            .block_on(f.competitor(false).acquire())
            .unwrap(),
        2
    );
}

#[test]
fn panic_releases_exclusion_and_refreshes_snapshot() {
    let async_runtime = executor();
    let f = async_runtime.block_on(Fixture::new(true));
    let runtime = f.engine.runtime();
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        runtime
            .cutover_authority()
            .run(&f.key(), || -> cutover::Result<()> {
                // Commit on the real coordinator without updating Runtime's cache.
                async_runtime.block_on(async {
                    runtime
                        .0
                        .lock()
                        .await
                        .commit(Transition::Progress {
                            stream: "panic".into(),
                            marker: "retained".into(),
                            position: "1".into(),
                        })
                        .await
                        .unwrap();
                });
                panic!("closure panic")
            })
    }));
    assert!(panic.is_err());
    assert_eq!(runtime.snapshot().progress["panic"].0, "retained");
    assert_eq!(
        async_runtime
            .block_on(f.competitor(false).acquire())
            .unwrap(),
        2
    );
}

#[tokio::test]
async fn sync_bridge_inside_current_thread_tokio_keeps_non_send_closure() {
    let f = Fixture::new(true).await;
    let borrowed = std::rc::Rc::new(std::cell::Cell::new(0));
    let result = f
        .engine
        .runtime()
        .cutover_authority()
        .run(&f.key(), || {
            borrowed.set(1);
            Ok(borrowed.clone())
        })
        .unwrap();
    assert_eq!(result.get(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sync_bridge_inside_multithread_tokio() {
    let f = Fixture::new(true).await;
    f.engine
        .runtime()
        .cutover_authority()
        .run(&f.key(), || Ok(()))
        .unwrap();
}

#[test]
fn nested_authority_borrows_outer_capability_without_reacquiring() {
    let async_runtime = executor();
    let f = async_runtime.block_on(Fixture::new(true));
    let key = f.key();
    f.engine
        .runtime()
        .cutover_authority()
        .run_with(&key, |held| {
            let mut nested = held;
            nested.run(&key, || Ok(()))?;
            assert_eq!(
                nested.run(
                    &Key {
                        epoch: 2,
                        ..key.clone()
                    },
                    || Ok(())
                ),
                Err(cutover::Error::LostOwnership)
            );
            assert_eq!(
                nested.run(&key, || Err::<(), _>(cutover::Error::Pending)),
                Err(cutover::Error::Pending)
            );
            Ok(())
        })
        .unwrap();
}

#[test]
fn no_exclusion_is_a_typed_configuration_error() {
    let async_runtime = executor();
    let f = async_runtime.block_on(Fixture::new(false));
    assert_eq!(
        f.engine
            .runtime()
            .cutover_authority()
            .run(&f.key(), || Ok(())),
        Err(cutover::Error::Invalid(
            "migration exclusion is not configured"
        ))
    );
}

#[test]
fn observer_cannot_enter_cutover_and_runtime_stays_eight_pointers() {
    let async_runtime = executor();
    let f = async_runtime.block_on(Fixture::new(true));
    let observer = Engine::new(f.competitor(false), f.journal.clone());
    assert_eq!(
        observer
            .runtime()
            .cutover_authority()
            .run(&f.key(), || Ok(())),
        Err(cutover::Error::LostOwnership)
    );
    assert!(std::mem::size_of::<Runtime>() <= 8 * std::mem::size_of::<usize>());
}

// A fault transport, never a fake database. The guarded rejection below must
// happen before any transport call. The real transport path is qualified by
// reverse_setup_refreshes_snapshot_on_partial_failure_and_returns_real_proof.
struct Unreachable;
impl Connection for Unreachable {
    fn query(&mut self, _: &str, _: Duration) -> Result<fence::Rows, String> {
        panic!("unauthorized reverse setup reached a database transport")
    }
}
#[test]
fn reverse_setup_requires_capability_from_the_same_runtime() {
    let async_runtime = executor();
    let f = async_runtime.block_on(Fixture::new(true));
    let other = async_runtime.block_on(Fixture::new(true));
    let config =
        reverse::Config::new("authority", 160000, vec![("public".into(), "rows".into())]).unwrap();
    let mut destination_fence = activate::DestinationFence::new(activate::Config {
        epoch: 1,
        applications: ["app".into()].into(),
        tables: vec![fence::Table {
            schema: "public".into(),
            name: "rows".into(),
            owner: "owner".into(),
        }],
        timeout_ms: 1000,
        post_activate: None,
    })
    .unwrap();
    f.engine
        .runtime()
        .cutover_authority()
        .run_with(&f.key(), |held| {
            // Compile-time property: callers cannot construct HeldCutover (private
            // fields), and Runtime::setup_reverse has a mandatory &HeldCutover arg.
            // The capability's lifetime cannot escape run_with's outer guard.
            assert_eq!(
                other
                    .engine
                    .runtime()
                    .setup_reverse(
                        held,
                        &config,
                        &mut Unreachable,
                        &mut Unreachable,
                        &mut destination_fence
                    )
                    .err(),
                Some(cutover::Error::LostOwnership)
            );
            Ok(())
        })
        .unwrap();
}

struct Pg {
    runtime: Arc<tokio::runtime::Runtime>,
    client: tokio_postgres::Client,
}
impl Pg {
    fn connect(runtime: Arc<tokio::runtime::Runtime>, dsn: &str, ca: &str) -> Self {
        let config = crate::pg::tls::Config::new(dsn.parse().unwrap(), ca);
        let client = runtime.block_on(async {
            let (client, driver, _) =
                crate::pg::tls::connect(&config, &crate::security::TlsPolicy::Required)
                    .await
                    .unwrap();
            tokio::spawn(async move {
                let _ = driver.await;
            });
            client
        });
        Self { runtime, client }
    }
    fn sql(&mut self, sql: &str) -> fence::Rows {
        self.query(sql, Duration::from_secs(30)).unwrap()
    }
}
impl Connection for Pg {
    fn query(&mut self, sql: &str, timeout: Duration) -> Result<fence::Rows, String> {
        self.runtime.block_on(async {
            let result = tokio::time::timeout(timeout, self.client.simple_query(sql)).await;
            match result {
                Ok(Ok(rows)) => Ok(rows
                    .into_iter()
                    .filter_map(|message| match message {
                        tokio_postgres::SimpleQueryMessage::Row(row) => Some(
                            (0..row.len())
                                .map(|index| row.get(index).unwrap_or("").to_string())
                                .collect(),
                        ),
                        _ => None,
                    })
                    .collect()),
                error => {
                    let _ = self.client.simple_query("ROLLBACK").await;
                    Err(format!("real PG query failed: {error:?}"))
                }
            }
        })
    }
}
struct FailPublication<'a>(&'a mut Pg);
impl Connection for FailPublication<'_> {
    fn query(&mut self, sql: &str, timeout: Duration) -> Result<fence::Rows, String> {
        if sql.contains("CREATE PUBLICATION") {
            Err("injected loss after durable publication intent".into())
        } else {
            self.0.query(sql, timeout)
        }
    }
}

#[test]
fn reverse_setup_refreshes_snapshot_on_partial_failure_and_returns_real_proof() {
    let Some(path) = std::env::var_os("SKEDADDLE_TEST_CONFIG") else {
        writeln!(std::io::stderr(), "SKIP Runtime owner-scoped reverse setup: SKEDADDLE_TEST_CONFIG absent; real-PG partial-failure/snapshot/proof qualification NOT RUN").unwrap();
        return;
    };
    let config: serde_json::Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    let (Ok(source), Ok(destination)) = (
        std::env::var("SKEDADDLE_PG_REVERSE_SOURCE_URL"),
        std::env::var("SKEDADDLE_PG_REVERSE_DESTINATION_URL"),
    ) else {
        writeln!(std::io::stderr(), "SKIP Runtime owner-scoped reverse setup: SKEDADDLE_PG_REVERSE_SOURCE_URL and SKEDADDLE_PG_REVERSE_DESTINATION_URL required; real-PG partial-failure/snapshot/proof qualification NOT RUN").unwrap();
        return;
    };
    let ca = config["ca_pem"]
        .as_str()
        .or_else(|| config["replication"]["ca_pem"].as_str())
        .unwrap();
    let async_runtime = Arc::new(tokio::runtime::Runtime::new().unwrap());
    let f = async_runtime.block_on(Fixture::new(true));
    let runtime = f.engine.runtime();
    async_runtime.block_on(async {
        for to in [State::Assessed, State::Copying, State::CatchingUp] {
            runtime.lifecycle(Event::Advance { to }).await.unwrap();
        }
        runtime
            .lifecycle(Event::Readiness {
                facts: ReadinessFacts {
                    all_chunks_done: true,
                    capture_healthy: true,
                    lineage_matches: true,
                    apply_lag: 0,
                    lag_budget: 1,
                    blocked_ddl: false,
                    unknown_schema_job: false,
                    verified_rows: 1,
                    in_scope_rows: 1,
                    busy_rows_past_budget: 0,
                    dead_letters: 0,
                    indexes_constraints_valid: true,
                    sequences_syncable: true,
                    reverse_prerequisites: true,
                    fence_risks_acknowledged: true,
                    buffer_contract_matches: true,
                },
            })
            .await
            .unwrap();
        for to in [State::SourceFenced, State::FinalBoundaryApplied] {
            runtime.lifecycle(Event::Advance { to }).await.unwrap();
        }
    });
    let mut source = Pg::connect(async_runtime.clone(), &source, ca);
    let mut destination = Pg::connect(async_runtime.clone(), &destination, ca);
    let id = format!("e6b2_{}", &uuid::Uuid::new_v4().simple().to_string()[..16]);
    for pg in [&mut source, &mut destination] {
        pg.sql(&format!(
            "CREATE SCHEMA {id}; CREATE TABLE {id}.rows (id int PRIMARY KEY)"
        ));
    }
    destination.sql(&format!("CREATE ROLE {id}_app NOLOGIN; GRANT USAGE ON SCHEMA {id} TO {id}_app; GRANT INSERT ON {id}.rows TO {id}_app"));
    let owner = destination.sql("SELECT current_user")[0][0].clone();
    let version = destination.sql("SHOW server_version_num")[0][0]
        .parse()
        .unwrap();
    let reverse = reverse::Config::new(&id, version, vec![(id.clone(), "rows".into())]).unwrap();
    let mut destination_fence = activate::DestinationFence::new(activate::Config {
        epoch: 1,
        applications: [format!("{id}_app")].into(),
        tables: vec![fence::Table {
            schema: id.clone(),
            name: "rows".into(),
            owner,
        }],
        timeout_ms: 30_000,
        post_activate: None,
    })
    .unwrap();
    while destination_fence.next != activate::Step::Fenced {
        destination_fence
            .execute_next(&mut destination, &mut |_| Ok(()))
            .unwrap();
    }
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        runtime
            .cutover_authority()
            .run_with(&f.key(), |held| {
                let before = runtime.snapshot().applied_seq;
                assert!(
                    runtime
                        .setup_reverse(
                            held,
                            &reverse,
                            &mut FailPublication(&mut destination),
                            &mut source,
                            &mut destination_fence
                        )
                        .is_err()
                );
                assert!(runtime.snapshot().applied_seq > before);
                assert_eq!(
                    runtime.snapshot(),
                    async_runtime.block_on(f.journal.load(true)).unwrap()
                );
                let proof = runtime.setup_reverse(
                    held,
                    &reverse,
                    &mut destination,
                    &mut source,
                    &mut destination_fence,
                )?;
                let state = runtime.snapshot();
                assert!(state.verification.reverse.as_ref().unwrap().proof.is_some());
                assert_eq!(
                    runtime.snapshot(),
                    async_runtime.block_on(f.journal.load(true)).unwrap()
                );
                let (identity, barrier) = coordinator::reverse::destination_barrier(
                    &mut destination,
                    Duration::from_secs(30),
                )
                .unwrap();
                let mut activation = destination_fence.clone();
                activation.next = activate::Step::CheckActivation;
                proof
                    .check(&f.key(), &activation, &identity, barrier)
                    .unwrap();
                Ok(())
            })
            .unwrap();
    }));
    // Remove only this test's named installation, on assertion failures too.
    let trigger = ddl::TriggerSql::new(&id, version).unwrap();
    destination.sql(&format!("{} SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name='{}'; DROP PUBLICATION IF EXISTS {}; DROP SCHEMA {id} CASCADE; DROP ROLE {id}_app;", trigger.remove, reverse.slot(), reverse.publication()));
    source.sql(&format!("SELECT pg_replication_origin_drop(roname) FROM pg_replication_origin WHERE roname='{}'; DROP SCHEMA {id} CASCADE;", reverse.origin()));
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}
