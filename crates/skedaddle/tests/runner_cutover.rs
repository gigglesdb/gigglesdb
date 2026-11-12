//! Real filesystem request protocol and config-gated owner/PG qualifications.
//! This binary starts no Docker. All scratch is beneath the task test root.
mod support;
use serde_json::{Value, json};
use skedaddle::{
    MigrationDefinition,
    coordinator::{Event, State, Transition, epoch::Coordinator, journal::Journal, local::Local},
    runner::{CutoverErrorKind, CutoverOutcome, request_cutover},
};
use std::{
    fs,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::Arc,
    time::Duration,
};

fn root() -> PathBuf {
    let path = PathBuf::from(std::env::var_os("SKEDADDLE_TEST_ROOT").expect("task test root"))
        .join(format!("runner-cutover-{}", uuid::Uuid::new_v4()));
    assert!(path.starts_with("J:/gigglesdb-work"));
    fs::create_dir_all(&path).unwrap();
    path
}
fn definition(root: &Path) -> Value {
    let mut value: Value = serde_json::from_str(
        include_str!("../README.md")
            .split("```json")
            .nth(1)
            .unwrap()
            .split("```")
            .next()
            .unwrap(),
    )
    .unwrap();
    value["buffer"]["backend"] = json!({"type":"local-fs","options":{"path":root.join("buffer")}});
    value["hooks"] = json!([]);
    value
}
#[tokio::test]
async fn absent_owner_and_invalid_request_are_typed_without_creating_coordinator() {
    let root = root();
    let no_owner = request_cutover(&root, "migration", Duration::from_secs(60))
        .await
        .unwrap();
    let invalid = request_cutover(&root, "../escape", Duration::from_secs(60))
        .await
        .unwrap();
    let zero = request_cutover(&root, "migration", Duration::ZERO)
        .await
        .unwrap();
    for (outcome, expected) in [
        (no_owner, CutoverErrorKind::NoOwner),
        (invalid, CutoverErrorKind::InvalidRequest),
        (zero, CutoverErrorKind::InvalidRequest),
    ] {
        let json = serde_json::to_value(&outcome).unwrap();
        assert_eq!(json["outcome"], "failed");
        assert!(!json["next_step"].as_str().unwrap().is_empty());
        assert!(matches!(outcome, CutoverOutcome::Failed { error, .. } if error.kind == expected));
    }
    assert!(fs::read_dir(root).unwrap().next().is_none());
}

struct Owner(Option<Child>, PathBuf);
fn terminate_tree(child: &mut Child, root: &Path) {
    #[cfg(windows)]
    {
        if child.try_wait().unwrap().is_none() {
            let status = Command::new("taskkill")
                .args(["/T", "/F", "/PID", &child.id().to_string()])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .unwrap();
            assert!(
                status.success() || child.try_wait().unwrap().is_some(),
                "owner tree termination failed"
            );
        }
        // The barrier is the owner's only child. Join it as well as the owner;
        // taskkill /T synchronously terminates all descendants, never just root.
        if let Ok(pid) = fs::read_to_string(root.join("hook-pid")) {
            let pid: u32 = pid.parse().unwrap();
            let status = Command::new("powershell.exe").args(["-NoProfile", "-NonInteractive", "-Command",
                &format!("$ownedProcess = Get-Process -Id {pid} -ErrorAction SilentlyContinue; if ($ownedProcess) {{ $ownedProcess.WaitForExit() }}; exit 0")])
                .stdout(Stdio::null()).stderr(Stdio::null()).status().unwrap();
            assert!(status.success(), "hook tree join failed");
        }
    }
    #[cfg(not(windows))]
    {
        let _ = root;
        let _ = Command::new("kill")
            .args(["-KILL", "--", &format!("-{}", child.id())])
            .status();
    }
    child.wait().unwrap();
}
struct Background<T>(tokio::task::JoinHandle<T>);
impl<T> Drop for Background<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}
impl Owner {
    fn spawn(root: &Path, id: &str, resume: bool) -> Self {
        let mut command = Command::new(std::env::current_exe().unwrap());
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            command.process_group(0);
        }
        Self(
            Some(
                command
                    .args(["--exact", "operational_owner_process", "--nocapture"])
                    .env("SKEDADDLE_CUTOVER_OWNER_ROOT", root)
                    .env("SKEDADDLE_CUTOVER_OWNER_ID", id)
                    .env(
                        "SKEDADDLE_CUTOVER_OWNER_RESUME",
                        if resume { "1" } else { "0" },
                    )
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .spawn()
                    .unwrap(),
            ),
            root.to_owned(),
        )
    }
    fn kill(&mut self) {
        if let Some(child) = self.0.as_mut() {
            terminate_tree(child, &self.1);
        }
        self.0.take();
    }
    async fn join(&mut self) {
        tokio::time::timeout(Duration::from_secs(180), async {
            loop {
                if let Some(status) = self.0.as_mut().unwrap().try_wait().unwrap() {
                    assert!(
                        status.success(),
                        "operational owner failed (endpoint details withheld)"
                    );
                    self.0.take();
                    return;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("owner did not join");
    }
}
impl Drop for Owner {
    fn drop(&mut self) {
        if let Some(child) = self.0.as_mut() {
            terminate_tree(child, &self.1);
        }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn operational_owner_process() {
    let Some(root) = std::env::var_os("SKEDADDLE_CUTOVER_OWNER_ROOT") else {
        return;
    };
    let root = PathBuf::from(root);
    let id = std::env::var("SKEDADDLE_CUTOVER_OWNER_ID").unwrap();
    let definition =
        MigrationDefinition::from_json(&fs::read_to_string(root.join("definition.json")).unwrap())
            .unwrap();
    let (_pause, requested) = tokio::sync::watch::channel(false);
    let result = skedaddle::runner::run(
        &root,
        &id,
        &definition,
        std::env::var("SKEDADDLE_CUTOVER_OWNER_RESUME").unwrap() == "1",
        skedaddle::engine::StopAt::UntilPaused,
        requested,
    )
    .await;
    assert!(
        result.is_ok(),
        "operational owner failed (endpoint details withheld)"
    );
}

// Exercise the same whole-tree stop/join independently of PostgreSQL.
#[test]
fn owner_tree_process() {
    let Some(root) = std::env::var_os("SKEDADDLE_CUTOVER_TREE_ROOT") else {
        return;
    };
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "cutover_hook_barrier_process", "--nocapture"])
        .env("SKEDADDLE_CUTOVER_HOOK_ROOT", root)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    assert!(child.wait().unwrap().success());
}
#[tokio::test]
async fn owner_kill_terminates_and_joins_its_hook_process_tree_without_pg() {
    let root = root();
    let mut command = Command::new(std::env::current_exe().unwrap());
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }
    let child = command
        .args(["--exact", "owner_tree_process", "--nocapture"])
        .env("SKEDADDLE_CUTOVER_TREE_ROOT", &root)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut owner = Owner(Some(child), root.clone());
    tokio::time::timeout(Duration::from_secs(60), async {
        while !root.join("hook-entered").is_file() {
            assert!(owner.0.as_mut().unwrap().try_wait().unwrap().is_none());
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("owned child tree never reached its barrier");
    owner.kill();
    assert!(owner.0.is_none());
    assert!(
        !root.join("hook-release").exists(),
        "tree stopped without releasing the child barrier"
    );
}

// The hook blocks on a file barrier rather than a narrow timing race. The
// owning test can expire the *destination* deadline or kill the actual owner.
#[test]
fn cutover_hook_barrier_process() {
    let Some(root) = std::env::var_os("SKEDADDLE_CUTOVER_HOOK_ROOT") else {
        return;
    };
    let root = PathBuf::from(root);
    fs::write(root.join("hook-pid"), std::process::id().to_string()).unwrap();
    fs::write(root.join("hook-entered"), []).unwrap();
    let start = std::time::Instant::now();
    while !root.join("hook-release").is_file() {
        assert!(
            start.elapsed() < Duration::from_secs(180),
            "hook barrier not released"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

static LIVE: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
async fn connect(dsn: &str, ca: &str) -> (tokio_postgres::Client, tokio::task::JoinHandle<()>) {
    let (client, driver, _) = skedaddle::pg::tls::connect(
        &skedaddle::pg::tls::Config::new(dsn.parse().unwrap(), ca),
        &skedaddle::security::TlsPolicy::Required,
    )
    .await
    .expect("fixture TLS connection failed");
    (
        client,
        tokio::spawn(async move {
            let _ = driver.await;
        }),
    )
}
fn ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}
struct Fixture {
    root: PathBuf,
    id: String,
    schema: String,
    role: String,
    definition: MigrationDefinition,
    source: tokio_postgres::Client,
    destination: tokio_postgres::Client,
    source_dsn: String,
    ca: String,
    drivers: Vec<tokio::task::JoinHandle<()>>,
}
impl Fixture {
    async fn new(name: &str, barrier: bool) -> Option<Self> {
        let Some(path) = std::env::var_os("SKEDADDLE_TEST_CONFIG") else {
            eprintln!(
                "SKIP runner cutover {name}: SKEDADDLE_TEST_CONFIG absent; real PG cutover/timings/deadline/owner-kill qualification NOT RUN"
            );
            return None;
        };
        let config: Value = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
        let source_dsn = config["pg_ddl"]
            .as_str()
            .expect("pg_ddl disposable admin DSN");
        let destination_dsn = config["pg_apply"]
            .as_str()
            .expect("pg_apply disposable admin DSN");
        let ca = config["ca_pem"].as_str().expect("explicit fixture CA");
        let (source, source_driver) = connect(source_dsn, ca).await;
        let (destination, destination_driver) = connect(destination_dsn, ca).await;
        let identity = "SELECT system_identifier::text,current_database() FROM pg_control_system()";
        let left = source.query_one(identity, &[]).await.unwrap();
        let right = destination.query_one(identity, &[]).await.unwrap();
        assert_ne!(
            left.get::<_, String>(0),
            right.get::<_, String>(0),
            "distinct disposable clusters required: role NOLOGIN is cluster-global"
        );
        let root = root();
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let schema = format!("rcut_{suffix}");
        let role = format!("rcut_app_{suffix}");
        let setup = format!(
            "CREATE ROLE {role} LOGIN; CREATE SCHEMA {schema}; CREATE TABLE {schema}.items(id bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,body text NOT NULL); GRANT USAGE ON SCHEMA {schema} TO {role}; GRANT SELECT,INSERT,UPDATE,DELETE ON {schema}.items TO {role}; GRANT USAGE ON SEQUENCE {schema}.items_id_seq TO {role};"
        );
        for client in [&source, &destination] {
            client.batch_execute(&setup).await.unwrap();
        }
        source
            .batch_execute(&format!(
                "INSERT INTO {schema}.items(body) SELECT 'seed' FROM generate_series(1,20)"
            ))
            .await
            .unwrap();
        let mut value = definition(&root);
        for (side, dsn) in [("source", source_dsn), ("destination", destination_dsn)] {
            let secret = root.join(format!("{side}.secret"));
            fs::write(&secret, dsn).unwrap();
            let ca_ref = root.join(format!("{side}-ca.secret"));
            fs::write(&ca_ref, ca).unwrap();
            value[side]["connection_ref"] = json!(format!("file:{}", secret.display()));
            value[side]["tls_ca_ref"] = json!(format!("file:{}", ca_ref.display()));
            value[side]["tls"] = json!("require");
        }
        value["selected_objects"] = json!([{"schema":schema,"name":"items","kind":"table"}]);
        value["fence_plan"]["application_roles"] = json!([role]);
        value["readiness_policy"]["max_lag_seconds"] = json!(60);
        let selected = format!("{}.{}", ident(&schema), ident("items"));
        let risks = skedaddle::pg::fence::inventory::collect(
            &source,
            std::slice::from_ref(&selected),
            std::slice::from_ref(&role),
        )
        .await
        .unwrap();
        let administrator: String = source
            .query_one("SELECT current_user", &[])
            .await
            .unwrap()
            .get(0);
        let mut rows = vec![];
        for principal in [&administrator, &role] {
            for kind in [
                "superuser",
                "table-owner",
                "bypass-rls",
                "out-of-scope-writer",
                "active-writer-session",
            ] {
                rows.push(vec![
                    kind.into(),
                    principal.clone(),
                    selected.clone(),
                    "disposable test administrator/writer".into(),
                ]);
            }
        }
        let extra = skedaddle::pg::fence::inventory::from_rows(rows).unwrap();
        value["fence_plan"]["acknowledged_residual_risks"] = json!(
            risks
                .into_iter()
                .chain(extra)
                .map(|r| r.id)
                .collect::<Vec<_>>()
        );
        if barrier {
            let hook_ref = root.join("hook-root.secret");
            fs::write(&hook_ref, root.to_str().unwrap()).unwrap();
            value["hooks"] = json!([{"name":"cutover-barrier","phase":"pre-fence","timeout_seconds":180,
                "command":[std::env::current_exe().unwrap(),"--exact","cutover_hook_barrier_process","--nocapture"],
                "secret_env":{"SKEDADDLE_CUTOVER_HOOK_ROOT":format!("file:{}",hook_ref.display())}}]);
        }
        let definition = MigrationDefinition::from_json(&value.to_string()).unwrap();
        let id = definition.canonical_digest().unwrap()[..24].to_owned();
        fs::write(
            root.join("definition.json"),
            serde_json::to_vec(&definition).unwrap(),
        )
        .unwrap();
        let journal = Journal::new(
            Arc::new(skedaddle::coordinator::journal::local_store(&root).unwrap()),
            "skedaddle",
            &id,
            Duration::from_secs(30),
        )
        .unwrap();
        let mut owner = Coordinator::observer(
            journal,
            Local::create(root.join(format!("{id}.local"))).unwrap(),
            "fixture-initialize",
        )
        .unwrap();
        owner.acquire().await.unwrap();
        owner
            .commit(Transition::Define {
                definition: Box::new(definition.clone()),
            })
            .await
            .unwrap();
        owner
            .commit(Transition::Lifecycle {
                event: Event::Advance {
                    to: State::Assessed,
                },
            })
            .await
            .unwrap();
        drop(owner);
        Some(Self {
            root,
            id,
            schema,
            role,
            definition,
            source,
            destination,
            source_dsn: source_dsn.into(),
            ca: ca.into(),
            drivers: vec![source_driver, destination_driver],
        })
    }
    async fn saved(&self) -> Value {
        let journal = skedaddle::runner::open_journal(&self.root, &self.id)
            .await
            .unwrap();
        let state = journal.load(true).await.unwrap();
        state
            .progress
            .get("runner-cutover-v1")
            .map_or(Value::Null, |(s, _)| serde_json::from_str(s).unwrap())
    }
    async fn ready(&self, owner: &mut Owner) {
        tokio::time::timeout(Duration::from_secs(180), async {
            loop {
                assert!(
                    owner.0.as_mut().unwrap().try_wait().unwrap().is_none(),
                    "owner exited before readiness"
                );
                let response = skedaddle::runner::request_verification(
                    &self.root,
                    &self.id,
                    &self.definition,
                    &[],
                )
                .await;
                if response.is_ok_and(|r| r.data["verification"] == true) {
                    let journal = skedaddle::runner::open_journal(&self.root, &self.id)
                        .await
                        .unwrap();
                    let state = journal.load(true).await.unwrap();
                    // The service re-evaluates this observation itself; status
                    // merely tells this test when to issue an admitted request.
                    if state.lifecycle.state == State::VerifiedReady {
                        return;
                    }
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("real readiness did not become available");
    }
    async fn barrier(&self) {
        tokio::time::timeout(Duration::from_secs(180), async {
            while !self.root.join("hook-entered").is_file() {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("cutover did not reach hook barrier");
    }
    async fn destination_ms(&self) -> u64 {
        let now: i64 = self
            .destination
            .query_one(
                "SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        now.try_into().unwrap()
    }
    async fn cleanup(self) {
        let forward = format!("skedaddle_{}", self.id);
        let reverse = format!("{forward}_rev");
        let version: String = self
            .source
            .query_one("SHOW server_version_num", &[])
            .await
            .unwrap()
            .get(0);
        // Remove only this fixture's migration-scoped control rows.
        let migration = format!("'{}'", self.id.replace('\'', "''"));
        let metadata = format!(
            "DO $cleanup$ BEGIN \
             IF to_regclass('_skedaddle.fence_state') IS NOT NULL THEN \
             DELETE FROM _skedaddle.fence_state WHERE migration={migration}; END IF; \
             IF to_regclass('_skedaddle.cutover_marker') IS NOT NULL THEN \
             DELETE FROM _skedaddle.cutover_marker WHERE migration={migration}; END IF; \
             IF to_regclass('_skedaddle.cutover_activation') IS NOT NULL THEN \
             DELETE FROM _skedaddle.cutover_activation WHERE migration={migration}; END IF; \
             IF to_regclass('_skedaddle.authority') IS NOT NULL THEN \
             DELETE FROM _skedaddle.authority WHERE migration={migration}; END IF; \
             IF to_regclass('_skedaddle.effects') IS NOT NULL THEN \
             DELETE FROM _skedaddle.effects WHERE migration={migration}; END IF; \
             END $cleanup$"
        );
        for client in [&self.source, &self.destination] {
            client.batch_execute(&metadata).await.unwrap();
        }
        let trigger =
            skedaddle::pg::ddl::TriggerSql::new(&self.id, version.parse().unwrap()).unwrap();
        self.source.batch_execute(&trigger.remove).await.unwrap();
        let trigger =
            skedaddle::pg::ddl::TriggerSql::new(&self.id, version.parse().unwrap()).unwrap();
        self.destination
            .batch_execute(&trigger.remove)
            .await
            .unwrap();
        for (client, slot) in [(&self.source, &forward), (&self.destination, &reverse)] {
            client.query("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name=$1", &[slot]).await.unwrap();
            client
                .batch_execute(&format!(
                    "DROP PUBLICATION IF EXISTS {slot}; DROP SCHEMA {} CASCADE; DROP ROLE {}",
                    self.schema, self.role
                ))
                .await
                .unwrap();
        }
        for (client, origin) in [(&self.destination, &forward), (&self.source, &reverse)] {
            client.query("SELECT pg_replication_origin_drop(roname) FROM pg_replication_origin WHERE roname=$1", &[origin]).await.unwrap();
        }
        for handle in self.drivers {
            handle.abort();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pg_full_cutover_under_concurrent_writes_reports_timings_and_pause() {
    use futures::FutureExt;
    let _serial = LIVE.lock().await;
    let Some(fixture) = Fixture::new("full", false).await else {
        return;
    };
    let proof = std::panic::AssertUnwindSafe(async {
        let mut owner = Owner::spawn(&fixture.root, &fixture.id, false);
        fixture.ready(&mut owner).await;
        let (writer, driver) = connect(&fixture.source_dsn, &fixture.ca).await;
        let _driver = Background(driver);
        writer
            .batch_execute(&format!("SET ROLE {}", fixture.role))
            .await
            .unwrap();
        let (stop, mut stopped) = tokio::sync::watch::channel(false);
        let sql = format!(
            "INSERT INTO {}.items(body) VALUES('concurrent')",
            fixture.schema
        );
        let mut writes = Background(tokio::spawn(async move {
            let mut committed = 0;
            while stopped.has_changed().is_ok() && !*stopped.borrow_and_update() {
                if writer.execute(&sql, &[]).await.is_ok() {
                    committed += 1;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            committed
        }));
        let outcome = tokio::time::timeout(
            Duration::from_secs(180),
            request_cutover(&fixture.root, &fixture.id, Duration::from_secs(120)),
        )
        .await
        .unwrap()
        .unwrap();
        stop.send_replace(true);
        let committed = (&mut writes.0).await.unwrap();
        assert!(committed > 0, "concurrent writer did not commit");
        let CutoverOutcome::Completed {
            step_timings_ms,
            write_pause_ms,
            ..
        } = outcome
        else {
            panic!("full cutover did not complete: {outcome:?}")
        };
        assert!(
            step_timings_ms
                .iter()
                .any(|s| s.step == skedaddle::cutover::Step::Fence)
        );
        assert!(
            step_timings_ms
                .iter()
                .any(|s| s.step == skedaddle::cutover::Step::Activate)
        );
        assert!(write_pause_ms <= 120_000);
        assert!(
            owner.0.as_mut().unwrap().try_wait().unwrap().is_none(),
            "post-activation owner exited"
        );
        let finalized = tokio::time::timeout(
            Duration::from_secs(10),
            skedaddle::runner::request_finalize(&fixture.root, &fixture.id),
        )
        .await
        .unwrap();
        assert_eq!(finalized.exit_code, 0, "{finalized:?}");
        owner.join().await;
        let terminal = request_cutover(&fixture.root, &fixture.id, Duration::from_secs(60))
            .await
            .unwrap();
        assert!(matches!(terminal, CutoverOutcome::Completed { .. }));
        let journal = skedaddle::runner::open_journal(&fixture.root, &fixture.id)
            .await
            .unwrap();
        let before = journal.load(true).await.unwrap().applied_seq;
        let mut terminal_owner = Owner::spawn(&fixture.root, &fixture.id, true);
        terminal_owner.join().await;
        assert_eq!(
            journal.load(true).await.unwrap().applied_seq,
            before,
            "terminal restart must start no workers or acquire ownership"
        );
        assert_eq!(
            request_cutover(&fixture.root, &fixture.id, Duration::from_secs(60))
                .await
                .unwrap(),
            terminal
        );
        let sql = format!("SELECT count(*) FROM {}.items", fixture.schema);
        assert_eq!(
            fixture
                .source
                .query_one(&sql, &[])
                .await
                .unwrap()
                .get::<_, i64>(0),
            fixture
                .destination
                .query_one(&sql, &[])
                .await
                .unwrap()
                .get::<_, i64>(0)
        );
        fixture
            .destination
            .batch_execute(&format!(
                "SET ROLE {}; INSERT INTO {}.items(body) VALUES('destination-active'); RESET ROLE",
                fixture.role, fixture.schema
            ))
            .await
            .unwrap();
    })
    .catch_unwind()
    .await;
    fixture.cleanup().await;
    if let Err(panic) = proof {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pg_deadline_abort_restores_source_writes() {
    use futures::FutureExt;
    let _serial = LIVE.lock().await;
    let Some(fixture) = Fixture::new("deadline", true).await else {
        return;
    };
    let proof = std::panic::AssertUnwindSafe(async {
        let mut owner = Owner::spawn(&fixture.root, &fixture.id, false);
        fixture.ready(&mut owner).await;
        let request = request_cutover(&fixture.root, &fixture.id, Duration::from_secs(3));
        let release = async {
            fixture.barrier().await;
            let deadline = fixture.saved().await["deadline_ms"].as_u64().unwrap();
            while fixture.destination_ms().await <= deadline {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            fs::write(fixture.root.join("hook-release"), []).unwrap();
        };
        let (outcome, ()) = tokio::time::timeout(Duration::from_secs(180), async {
            tokio::join!(request, release)
        })
        .await
        .unwrap();
        let CutoverOutcome::DeadlineAborted { write_pause_ms, .. } = outcome.unwrap() else {
            panic!("deadline must abort before activation")
        };
        assert!(write_pause_ms.is_some());
        owner.join().await;
        fixture
            .source
            .batch_execute(&format!(
                "SET ROLE {}; INSERT INTO {}.items(body) VALUES('source-restored'); RESET ROLE",
                fixture.role, fixture.schema
            ))
            .await
            .unwrap();
    })
    .catch_unwind()
    .await;
    fixture.cleanup().await;
    if let Err(panic) = proof {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pg_owner_kill_resumes_stable_cutover_and_original_deadline() {
    use futures::FutureExt;
    let _serial = LIVE.lock().await;
    let Some(fixture) = Fixture::new("owner-kill", true).await else {
        return;
    };
    let proof = std::panic::AssertUnwindSafe(async {
        let mut owner = Owner::spawn(&fixture.root, &fixture.id, false);
        fixture.ready(&mut owner).await;
        let request = request_cutover(&fixture.root, &fixture.id, Duration::from_secs(120));
        let kill = async { fixture.barrier().await; owner.kill(); };
        let (outcome, ()) = tokio::time::timeout(Duration::from_secs(180), async { tokio::join!(request, kill) }).await.unwrap();
        assert!(matches!(outcome.unwrap(), CutoverOutcome::Failed { error, .. } if error.kind == CutoverErrorKind::OwnerStopped));
        let before = fixture.saved().await;
        fs::write(fixture.root.join("hook-release"), []).unwrap();
        let mut successor = Owner::spawn(&fixture.root, &fixture.id, true);
        successor.join().await;
        let after = fixture.saved().await;
        assert_eq!(before["state"]["key"]["cutover"], after["state"]["key"]["cutover"]);
        assert_eq!(before["deadline_ms"], after["deadline_ms"]);
        assert_eq!(after["state"]["next"], "Complete");
        assert!(after["state"]["key"]["epoch"].as_u64().unwrap() > before["state"]["key"]["epoch"].as_u64().unwrap());
    }).catch_unwind().await;
    fixture.cleanup().await;
    if let Err(panic) = proof {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test]
async fn authenticated_terminal_replay_returns_saved_outcome_without_copy_or_endpoints() {
    use skedaddle::{
        cutover,
        pg::{activate, fence},
    };
    let root = root();
    let id = "terminal";
    let definition = MigrationDefinition::from_json(&definition(&root).to_string()).unwrap();
    let journal = Journal::new(
        Arc::new(skedaddle::coordinator::journal::local_store(&root).unwrap()),
        "skedaddle",
        id,
        Duration::from_secs(30),
    )
    .unwrap();
    let exclusion =
        skedaddle::coordinator::MigrationExclusion::file(root.join("exclusion")).unwrap();
    let mut owner = Coordinator::observer(
        journal.clone(),
        Local::create(root.join("terminal.local")).unwrap(),
        "terminal-owner",
    )
    .unwrap()
    .with_exclusion(exclusion);
    owner.acquire().await.unwrap();
    owner
        .commit(Transition::Define {
            definition: Box::new(definition.clone()),
        })
        .await
        .unwrap();
    for to in [State::Assessed, State::Copying, State::CatchingUp] {
        owner
            .commit(Transition::Lifecycle {
                event: Event::Advance { to },
            })
            .await
            .unwrap();
    }
    owner
        .commit(Transition::Lifecycle {
            event: Event::Readiness {
                facts: support::ready(),
            },
        })
        .await
        .unwrap();
    owner
        .commit(Transition::Lifecycle {
            event: Event::Advance {
                to: State::SourceFenced,
            },
        })
        .await
        .unwrap();
    let key = cutover::Key {
        migration: id.into(),
        cutover: "stable-cutover".into(),
        epoch: 1,
        coordinator: owner.instance_id().into(),
    };
    let mut state = cutover::State::new(key.clone()).unwrap();
    state.next = cutover::Step::Aborted;
    let table = fence::Table {
        schema: "public".into(),
        name: "items".into(),
        owner: "owner".into(),
    };
    let source = fence::Fence::new(fence::FenceConfig {
        epoch: 1,
        applications: ["app".into()].into(),
        tables: vec![table.clone()],
        acknowledged: Default::default(),
        deadline_ms: 1000,
    })
    .unwrap();
    let destination = activate::DestinationFence::new(activate::Config {
        epoch: 1,
        applications: ["app".into()].into(),
        tables: vec![table],
        timeout_ms: 1000,
        post_activate: None,
    })
    .unwrap();
    let outcome = CutoverOutcome::DeadlineAborted {
        cutover_id: key.cutover.clone(),
        step_timings_ms: vec![],
        write_pause_ms: Some(12),
        next_step: "source restored".into(),
    };
    let saved = json!({"state":state,"deadline_ms":100,"source":source,"destination":destination,"drained":cutover::DrainCounts::default(),"timings":[],"aborted_at_ms":112,"last_clock_ms":112,"outcome":outcome});
    owner
        .commit(Transition::Progress {
            stream: "runner-cutover-v1".into(),
            marker: saved.to_string(),
            position: key.cutover,
        })
        .await
        .unwrap();
    let terminal = State::AbortedToSource;
    owner
        .commit(Transition::Lifecycle {
            event: Event::Advance { to: terminal },
        })
        .await
        .unwrap();
    let before = journal.load(true).await.unwrap().applied_seq;
    drop(owner);
    assert_eq!(
        request_cutover(&root, id, Duration::from_secs(60))
            .await
            .unwrap(),
        outcome
    );
    let (_pause, requested) = tokio::sync::watch::channel(false);
    // README secret references cannot connect. Success proves the early
    // authenticated terminal path precedes endpoint prep and worker start.
    let reached = skedaddle::runner::run(
        &root,
        id,
        &definition,
        true,
        skedaddle::engine::StopAt::UntilPaused,
        requested,
    )
    .await;
    assert!(
        reached.is_ok(),
        "terminal replay attempted endpoint preparation"
    );
    assert_eq!(reached.ok().unwrap().state, terminal);
    assert_eq!(journal.load(true).await.unwrap().applied_seq, before);
}
