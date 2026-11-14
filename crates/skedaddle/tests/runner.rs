//! Real local control/CLI tests and explicitly gated PostgreSQL lifecycle proof.
use serde_json::{Value, json};
use skedaddle::{
    MigrationDefinition,
    coordinator::{Event, State, Transition, epoch::Coordinator, journal::Journal, local::Local},
};
use std::{fs, path::PathBuf, process::Command, sync::Arc, time::Duration};
const EXCLUDED_SENTINEL: &str = "D6_PRIVATE_SENTINEL_f4792cb08a954cfa";
static LIVE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[path = "support/child_capture.rs"]
mod child_capture;
use child_capture::CapturedChild as ChildGuard;
async fn child_result(child: &mut ChildGuard) -> (i32, Value) {
    tokio::time::timeout(Duration::from_secs(60), async {
        while child.0.as_mut().unwrap().try_wait().unwrap().is_none() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("runner child did not join within 60s");
    let output = child.output();
    assert_no_sentinel(&output.stdout);
    assert_no_sentinel(&output.stderr);
    let envelope = serde_json::from_slice(&output.stdout).expect("runner child envelope absent");
    (output.status.code().unwrap(), envelope)
}

#[test]
fn pause_control_owner_process_fixture() {
    let Some(root) = std::env::var_os("SKEDADDLE_PAUSE_OWNER_FIXTURE") else {
        return;
    };
    let root = PathBuf::from(root);
    let owner = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .read(true)
        .open(root.join("owner.lock"))
        .unwrap();
    owner.lock().unwrap();
    let token = uuid::Uuid::new_v4();
    fs::write(root.join("owner-generation"), token.to_string()).unwrap();
    fs::write(root.join("fixture-ready"), []).unwrap();
    let start = std::time::Instant::now();
    while !root.join(format!("pause-{token}")).exists() {
        assert!(
            start.elapsed() < Duration::from_secs(30),
            "pause request absent"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[tokio::test]
async fn cli_pause_requests_real_locked_owner_generation_in_second_process() {
    let root = root();
    let id = initialize(&root, definition(&root), State::Copying).await;
    let control = root.join(format!("{id}.control"));
    fs::create_dir_all(&control).unwrap();
    let mut child = ChildGuard::new(Some(
        Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "pause_control_owner_process_fixture",
                "--nocapture",
            ])
            .env("SKEDADDLE_PAUSE_OWNER_FIXTURE", &control)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap(),
    ));
    tokio::time::timeout(Duration::from_secs(10), async {
        while !control.join("fixture-ready").exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    struct JournalOutage {
        original: PathBuf,
        displaced: PathBuf,
    }
    impl Drop for JournalOutage {
        fn drop(&mut self) {
            let _ = fs::rename(&self.displaced, &self.original);
        }
    }
    let outage = JournalOutage {
        original: root.join("skedaddle/journal").join(&id),
        displaced: root.join("journal-unavailable"),
    };
    fs::rename(&outage.original, &outage.displaced).unwrap();
    let (code, result) = invoke(&root, "pause", &id);
    assert_eq!(code, 0, "{result}");
    assert_eq!(result["data"]["pause_requested"], true);
    assert!(child.0.as_mut().unwrap().wait().unwrap().success());
    child.0.take();
    drop(outage);
    assert_eq!(
        invoke(&root, "status", &id).1["data"]["state"]["lifecycle"]["state"],
        "copying"
    );
    assert_eq!(
        invoke(&root, "pause", &id).1["data"]["pause_requested"],
        false
    );
}

#[tokio::test]
async fn until_accepts_exact_boundaries_and_rejects_unknown_values() {
    let root = root();
    let id = initialize(&root, definition(&root), State::Assessed).await;
    for until in ["catching-up", "verified-ready", "paused"] {
        let output = Command::new(env!("CARGO_BIN_EXE_skedaddle"))
            .arg("--state-root")
            .arg(&root)
            .args(["start", &id, "--until", until])
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(3)); // valid parse reaches missing CA
        let result: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(result["command"], "start");
    }
    let output = Command::new(env!("CARGO_BIN_EXE_skedaddle"))
        .args(["start", &id, "--until", "ready"])
        .output()
        .unwrap();
    let result: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["exit_code"], 3);
    assert_eq!(result["command"], "parse");
}

#[tokio::test]
async fn live_cli_excluded_sentinel_catching_up_pause_resume_and_equal_row_counts() {
    use futures::FutureExt;
    use std::{io::Write, panic::AssertUnwindSafe, process::Stdio};
    let _live = LIVE_LOCK.lock().await;
    let Some(path) = std::env::var_os("SKEDADDLE_TEST_CONFIG") else {
        let _ = writeln!(
            std::io::stderr(),
            "SKIP runner excluded-column sentinel operational CLI: SKEDADDLE_TEST_CONFIG absent; real PostgreSQL copy/concurrent-write/pause/resume/count/sentinel-isolation qualification NOT RUN"
        );
        return;
    };
    let cfg: Value = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
    let source_dsn = cfg["pg_ddl"]
        .as_str()
        .expect("H1 pg_ddl admin DSN required");
    let destination_dsn = cfg["pg_apply"]
        .as_str()
        .expect("H1 pg_apply admin DSN required");
    let ca = cfg["ca_pem"].as_str().expect("H1 ca_pem required");
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
    fn spawn(root: &std::path::Path, id: &str, command: &str, until: &str) -> ChildGuard {
        ChildGuard::new(Some(
            Command::new(env!("CARGO_BIN_EXE_skedaddle"))
                .arg("--state-root")
                .arg(root)
                .args([command, id, "--until", until])
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap(),
        ))
    }
    async fn count(client: &tokio_postgres::Client, schema: &str) -> i64 {
        client
            .query_one(&format!("SELECT count(*) FROM {schema}.rows"), &[])
            .await
            .unwrap()
            .get(0)
    }
    let (source, source_driver) = connect(source_dsn, ca).await;
    let (destination, destination_driver) = connect(destination_dsn, ca).await;
    let root = root();
    let schema = format!("rc{}", uuid::Uuid::new_v4().simple());
    let ddl = format!(
        "CREATE SCHEMA {schema};CREATE TABLE {schema}.rows(id bigint NOT NULL, token uuid NOT NULL, body text, private_payload text, PRIMARY KEY(id,token))"
    );
    source.batch_execute(&ddl).await.unwrap();
    destination.batch_execute(&ddl).await.unwrap();
    source.batch_execute(&format!("INSERT INTO {schema}.rows SELECT i,md5(i::text)::uuid,'seed','{EXCLUDED_SENTINEL}' FROM generate_series(1,1200) i")).await.unwrap();
    let mut value = definition(&root);
    value["selected_objects"] = json!([{"schema":schema,"name":"rows","kind":"table","exclude_columns":["private_payload"]}]);
    fs::write(root.join("source-connection.secret"), source_dsn).unwrap();
    fs::write(root.join("destination-connection.secret"), destination_dsn).unwrap();
    for side in ["source", "destination"] {
        fs::write(root.join(format!("{side}-ca.secret")), ca).unwrap();
    }
    // assess is still an observer. This explicit journal fixture supplies the
    // legitimate Assessed prerequisite; init/assess reachability is not claimed.
    let id = initialize(&root, value, State::Assessed).await;
    let proof = AssertUnwindSafe(async {
        let mut first = spawn(&root, &id, "start", "catching-up");
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let state = invoke(&root, "status", &id).1;
                if state["data"]["state"]["chunk_attempts"].as_object().is_some_and(|attempts| !attempts.is_empty()) { break; }
                first.assert_running("start exited before copy window");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.unwrap();
        for i in 1201..1231 {
            source.execute(&format!("INSERT INTO {schema}.rows VALUES($1,md5($1::text)::uuid,'concurrent','{EXCLUDED_SENTINEL}')"), &[&(i as i64)]).await.unwrap();
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        let (code, reached) = child_result(&mut first).await;
        assert_eq!(code, 0, "{reached}");
        assert_eq!(reached["data"]["reached_state"], "catching-up");
        assert!(assert_buffer_has_no_sentinel(&root.join("buffer")) > 0);
        let mut previous = Value::Null;
        for cycle in 0..2 {
            let mut running = spawn(&root, &id, "resume", "paused");
            if cycle == 1 {
                source.execute(&format!("INSERT INTO {schema}.rows VALUES(2000,md5('2000')::uuid,'after-resume','{EXCLUDED_SENTINEL}')"), &[]).await.unwrap();
            }
            tokio::time::timeout(Duration::from_secs(30), async {
                loop {
                    let status = invoke(&root, "status", &id).1;
                    let applied = &status["data"]["state"]["progress"]["applied"];
                    if count(&source, &schema).await == count(&destination, &schema).await && !applied.is_null() && applied != &previous {
                        previous = applied.clone();
                        break;
                    }
                    running.assert_running("resume exited before catchup");
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }).await.unwrap();
            let (code, requested) = invoke(&root, "pause", &id);
            assert_eq!(code, 0, "{requested}");
            assert_eq!(requested["data"]["pause_requested"], true);
            let (code, paused) = child_result(&mut running).await;
            assert_eq!(code, 0, "{paused}");
            assert_eq!(paused["data"]["reached_state"], "paused");
            assert!(assert_buffer_has_no_sentinel(&root.join("buffer")) > 0);
            assert_eq!(invoke(&root, "status", &id).1["data"]["state"]["lifecycle"]["state"], "paused");
        }
        assert_eq!(count(&source, &schema).await, count(&destination, &schema).await);
        let source_secrets: i64 = source.query_one(&format!("SELECT count(*) FROM {schema}.rows WHERE private_payload=$1"), &[&EXCLUDED_SENTINEL]).await.unwrap().get(0);
        assert_eq!(source_secrets, count(&source, &schema).await);
        let destination_secrets: i64 = destination.query_one(&format!("SELECT count(*) FROM {schema}.rows WHERE private_payload IS NOT NULL"), &[]).await.unwrap().get(0);
        assert_eq!(destination_secrets, 0, "excluded values reached destination");
        assert!(assert_buffer_has_no_sentinel(&root.join("buffer")) > 0, "buffer scan must inspect persisted files");
        let stream = format!("skedaddle-{id}-wal");
        let progress: i64 = destination.query_one("SELECT count(*) FROM _skedaddle.apply_progress WHERE stream=$1", &[&stream]).await.unwrap().get(0);
        assert_eq!(progress, 1, "destination receipts must belong to this migration stream");
    }).catch_unwind().await;
    let trigger = skedaddle::pg::ddl::TriggerSql::new(
        &id,
        cfg["pg_version"].as_u64().unwrap() as u32 * 10000,
    )
    .unwrap();
    source.batch_execute(&trigger.remove).await.unwrap();
    let slot = format!("skedaddle_{id}");
    source.query("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name=$1", &[&slot]).await.unwrap();
    source
        .batch_execute(&format!(
            "DROP PUBLICATION IF EXISTS skedaddle_{id};DROP SCHEMA {schema} CASCADE"
        ))
        .await
        .unwrap();
    destination
        .query(
            "SELECT pg_replication_origin_drop(roname) FROM pg_replication_origin WHERE roname=$1",
            &[&slot],
        )
        .await
        .unwrap();
    destination
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .await
        .unwrap();
    source_driver.abort();
    destination_driver.abort();
    if let Err(panic) = proof {
        std::panic::resume_unwind(panic);
    }
}

fn root() -> PathBuf {
    let root =
        PathBuf::from(std::env::var_os("SKEDADDLE_TEST_ROOT").expect("set SKEDADDLE_TEST_ROOT"))
            .join(format!("runner-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&root).unwrap();
    root
}

fn definition(root: &std::path::Path) -> Value {
    let readme = include_str!("../README.md");
    let example = readme
        .split("```json")
        .nth(1)
        .unwrap()
        .split("```")
        .next()
        .unwrap();
    let mut value: Value = serde_json::from_str(example).unwrap();
    value["buffer"]["backend"] = json!({"type":"local-fs","options":{"path":root.join("buffer")}});
    for side in ["source", "destination"] {
        let dsn = root.join(format!("{side}-connection.secret"));
        let ca = root.join(format!("{side}-ca.secret"));
        fs::write(&dsn, "host=127.0.0.1 port=1 user=runner_private_user password=runner_private_password dbname=postgres").unwrap();
        fs::write(&ca, root.join("private-missing-ca.pem").to_str().unwrap()).unwrap();
        value[side]["connection_ref"] = json!(format!("file:{}", dsn.display()));
        value[side]["tls_ca_ref"] = json!(format!("file:{}", ca.display()));
        value[side]["tls"] = json!("require");
    }
    value
}

fn invoke(root: &std::path::Path, command: &str, id: &str) -> (i32, Value) {
    let output = Command::new(env!("CARGO_BIN_EXE_skedaddle"))
        .arg("--state-root")
        .arg(root)
        .args([command, id])
        .output()
        .unwrap();
    assert_no_sentinel(&output.stdout);
    assert_no_sentinel(&output.stderr);
    let text = String::from_utf8(output.stdout).unwrap();
    for secret in [
        "runner_private_password",
        "runner_private_user",
        "private-missing-ca.pem",
    ] {
        assert!(!text.contains(secret), "secret leaked: {text}");
        assert!(!String::from_utf8_lossy(&output.stderr).contains(secret));
    }
    let envelope: Value = serde_json::from_str(&text).unwrap();
    let code = output.status.code().unwrap();
    assert_eq!(envelope["exit_code"], code);
    if code != 0 {
        assert!(
            !envelope["problem"]["next_step"]
                .as_str()
                .unwrap()
                .is_empty()
        );
    }
    (code, envelope)
}

async fn initialize(root: &std::path::Path, value: Value, state: State) -> String {
    let definition = MigrationDefinition::from_json(&value.to_string()).unwrap();
    let id = definition.canonical_digest().unwrap()[..24].to_owned();
    let store = object_store::local::LocalFileSystem::new_with_prefix(root).unwrap();
    let journal = Journal::new(Arc::new(store), "skedaddle", &id, Duration::from_secs(5)).unwrap();
    let mut coordinator = Coordinator::observer(
        journal,
        Local::create(root.join(format!("{id}.local"))).unwrap(),
        "runner-local-qualification",
    )
    .unwrap();
    coordinator.acquire().await.unwrap();
    coordinator
        .commit(Transition::Define {
            definition: Box::new(definition),
        })
        .await
        .unwrap();
    coordinator
        .commit(Transition::Lifecycle {
            event: Event::Advance {
                to: State::Assessed,
            },
        })
        .await
        .unwrap();
    if state != State::Assessed {
        coordinator
            .commit(Transition::Lifecycle {
                event: Event::Advance { to: State::Copying },
            })
            .await
            .unwrap();
        if state == State::Paused {
            coordinator
                .commit(Transition::Lifecycle {
                    event: Event::Interrupt {
                        to: State::Paused,
                        reason: "local runner fixture pause".into(),
                    },
                })
                .await
                .unwrap();
        }
    }
    id
}

#[tokio::test]
async fn missing_environment_secret_has_redacted_actionable_envelope() {
    let root = root();
    let mut value = definition(&root);
    let name = format!("RUNNER_ABSENT_{}", uuid::Uuid::new_v4().simple());
    value["source"]["connection_ref"] = json!(format!("env:{name}"));
    let id = initialize(&root, value, State::Assessed).await;
    let (code, envelope) = invoke(&root, "start", &id);
    assert_eq!(code, 6, "{envelope}");
    assert!(!envelope.to_string().contains(&name));
    assert_eq!(envelope["data"]["final_state"]["state"], "assessed");
    assert_eq!(envelope["data"]["external_effects_performed"], false);
}

#[tokio::test]
async fn unreachable_secret_file_does_not_expose_path() {
    let root = root();
    let mut value = definition(&root);
    value["source"]["connection_ref"] = json!(format!(
        "file:{}",
        root.join("private-unavailable-secret").display()
    ));
    let id = initialize(&root, value, State::Assessed).await;
    let (code, envelope) = invoke(&root, "start", &id);
    assert_eq!(code, 6, "{envelope}");
    assert!(!envelope.to_string().contains("private-unavailable-secret"));
}

#[tokio::test]
async fn missing_ca_reference_has_actionable_envelope_on_resume() {
    let root = root();
    let mut value = definition(&root);
    value["source"]["tls_ca_ref"] = json!(format!(
        "file:{}",
        root.join("private-unavailable-ca-reference").display()
    ));
    let id = initialize(&root, value, State::Paused).await;
    let (code, envelope) = invoke(&root, "resume", &id);
    assert_eq!(code, 6, "{envelope}");
    assert!(
        !envelope
            .to_string()
            .contains("private-unavailable-ca-reference")
    );
    assert_eq!(envelope["data"]["final_state"]["state"], "paused");
}

#[tokio::test]
async fn missing_ca_bundle_is_invalid_input_without_path_disclosure() {
    let root = root();
    let value = definition(&root);
    let id = initialize(&root, value, State::Assessed).await;
    let (code, envelope) = invoke(&root, "start", &id);
    assert_eq!(code, 3, "{envelope}");
    assert!(
        envelope["problem"]["what"]
            .as_str()
            .unwrap()
            .contains("CA bundle")
    );
}

#[tokio::test]
async fn unreachable_host_has_redacted_infrastructure_envelope() {
    let root = root();
    let mut value = definition(&root);
    // A real closed local TCP port exercises the connector without fake DBs.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    fs::write(root.join("source-connection.secret"), format!("host=127.0.0.1 port={port} user=runner_private_user password=runner_private_password dbname=postgres")).unwrap();
    for side in ["source", "destination"] {
        value[side]["tls"] = json!("disable-for-local-test");
        value[side].as_object_mut().unwrap().remove("tls_ca_ref");
    }
    let id = initialize(&root, value, State::Assessed).await;
    let (code, envelope) = invoke(&root, "start", &id);
    assert_eq!(code, 6, "{envelope}");
    assert_eq!(envelope["data"]["final_state"]["state"], "assessed");
}

#[tokio::test]
async fn local_pause_without_owner_is_success_without_journal_mutation() {
    let root = root();
    let value = definition(&root);
    let id = initialize(&root, value, State::Copying).await;
    let (code, envelope) = invoke(&root, "pause", &id);
    assert_eq!(code, 0, "{envelope}");
    assert_eq!(envelope["data"]["pause_requested"], false);
    let (code, envelope) = invoke(&root, "status", &id);
    assert_eq!(code, 0);
    assert_eq!(envelope["data"]["state"]["lifecycle"]["state"], "copying");
}

#[tokio::test]
async fn status_reports_all_readiness_gaps_and_next_steps_without_live_owner() {
    let root = root();
    let id = initialize(&root, definition(&root), State::Copying).await;
    let journal = skedaddle::runner::open_journal(&root, &id).await.unwrap();
    let state = journal.load(true).await.unwrap();
    let status = skedaddle::runner::verification_status(&root, &id, &state);
    let facts = &status["readiness"];
    for key in [
        "apply_lag",
        "schema_jobs",
        "fence_risks",
        "buffer_contract",
        "destination_validity",
        "sequences_syncable",
        "reverse_prerequisites",
    ] {
        assert_eq!(facts[key]["ready"], false, "{status}");
        for field in ["reason", "remediation", "code"] {
            assert!(
                !facts[key]["blocker"][field].as_str().unwrap().is_empty(),
                "{status}"
            );
        }
        assert!(!facts[key]["source"].as_str().unwrap().is_empty());
        assert!(!facts[key]["staleness"].as_str().unwrap().is_empty());
    }
    assert_eq!(
        facts["schema_jobs"]["blocker"]["code"],
        "schema-boundary-pending"
    );
    // This public runner projection is consumed by the existing CLI status path.
    let (code, cli) = invoke(&root, "status", &id);
    assert_eq!(code, 0);
    assert!(cli.to_string().contains("readiness"));
    assert!(cli.to_string().contains("oldest-unapplied"));
}

#[tokio::test]
async fn cli_retains_stopped_capture_boundary_only_as_bound_historical_diagnostic() {
    use skedaddle::pg::ddl::{Classification, DdlRecord, SchemaVersions};
    let root = root();
    let id = initialize(&root, definition(&root), State::Copying).await;
    let state = skedaddle::runner::open_journal(&root, &id)
        .await
        .unwrap()
        .load(true)
        .await
        .unwrap();
    let control = root.join(format!("{id}.control"));
    fs::create_dir_all(&control).unwrap();
    let generation = uuid::Uuid::new_v4().to_string();
    fs::write(control.join("owner-generation"), &generation).unwrap();
    let mut schemas = SchemaVersions::new("e5status".into(), 0, []).unwrap();
    let Classification::SafeBoundaryStop(boundary) = schemas.classify_transaction(&[DdlRecord {
        origin: None,
        format: 1,
        installation: "e5status".into(),
        server_major: 18,
        version: 1,
        tag: "ALTER TABLE".into(),
        xid: "73".into(),
        query: "ALTER TABLE unknown ADD COLUMN x int".into(),
        commands: vec![],
    }]) else {
        panic!("policy-hitting DDL must produce a boundary");
    };
    let mut facts = skedaddle::runner::verification_status(&root, &id, &state)["readiness"].clone();
    facts["schema_boundary"] = json!({ "code":"schema-boundary-pending", "reason":boundary.reason, "remediation":boundary.remediation });
    // Old positives must never survive owner exit, even if the record contains
    // them. Only an actual boundary diagnostic can survive its freshness bound.
    for key in [
        "apply_lag",
        "schema_jobs",
        "fence_risks",
        "buffer_contract",
        "destination_validity",
        "sequences_syncable",
        "reverse_prerequisites",
    ] {
        facts[key]["ready"] = json!(true);
    }
    let mut saved = json!({
        "terminal":true,
        "generation":generation, "epoch":state.epoch,
        "invalidation_generation":state.verification.generation,
        "definition_digest":state.definition.as_ref().unwrap().canonical_digest().unwrap(),
        "observed_at_ms":0, "facts":facts,
    });
    let path = control.join("readiness-status.json");
    fs::write(&path, serde_json::to_vec(&saved).unwrap()).unwrap();
    fs::remove_file(control.join("owner-generation")).unwrap();
    let status = skedaddle::runner::verification_status(&root, &id, &state);
    for key in [
        "apply_lag",
        "schema_jobs",
        "fence_risks",
        "buffer_contract",
        "destination_validity",
        "sequences_syncable",
        "reverse_prerequisites",
    ] {
        assert_eq!(status["readiness"][key]["ready"], false);
    }
    assert!(
        status["readiness"]["schema_jobs"]["blocker"]["reason"]
            .as_str()
            .unwrap()
            .contains(&boundary.reason)
    );
    assert_eq!(
        status["readiness"]["schema_jobs"]["blocker"]["remediation"],
        boundary.remediation
    );
    let (code, cli) = invoke(&root, "status", &id);
    assert_eq!(code, 0);
    assert!(cli.to_string().contains("Last capture boundary"));
    assert!(cli.to_string().contains(boundary.remediation));
    saved["epoch"] = json!(state.epoch + 1);
    fs::write(&path, serde_json::to_vec(&saved).unwrap()).unwrap();
    assert!(
        !skedaddle::runner::verification_status(&root, &id, &state)
            .to_string()
            .contains("Last capture boundary")
    );
    saved["epoch"] = json!(state.epoch);
    fs::write(&path, serde_json::to_vec(&saved).unwrap()).unwrap();
    let owner = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(control.join("owner.lock"))
        .unwrap();
    owner.lock().unwrap();
    fs::write(
        control.join("owner-generation"),
        uuid::Uuid::new_v4().to_string(),
    )
    .unwrap();
    assert!(
        !skedaddle::runner::verification_status(&root, &id, &state)
            .to_string()
            .contains("Last capture boundary")
    );
}

#[tokio::test]
async fn live_preparation_qualifies_bigint_uuid_and_blocks_text_key() {
    let _live = LIVE_LOCK.lock().await;
    fn skip(reason: &str) {
        use std::io::Write;
        let _ = writeln!(
            std::io::stderr(),
            "SKIP runner preparation: {reason}; live catalog/TLS qualification NOT RUN; operational copy/pause/resume has its own explicitly gated test"
        );
    }
    let Some(path) = std::env::var_os("SKEDADDLE_TEST_CONFIG") else {
        skip("SKEDADDLE_TEST_CONFIG absent");
        return;
    };
    let config: Value = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
    let Some(source_dsn) = config["engine"]["source"].as_str() else {
        skip("H1 config lacks engine.source operational section");
        return;
    };
    let destination_dsn = config["engine"]["destination"]
        .as_str()
        .expect("H1 config engine.destination absent");
    let ca = config["ca_pem"].as_str().expect("H1 config ca_pem absent");
    let connect = |dsn: String, ca: String| async move {
        let config = skedaddle::pg::tls::Config::new(dsn.parse().unwrap(), ca);
        let (client, driver, _) =
            skedaddle::pg::tls::connect(&config, &skedaddle::security::TlsPolicy::Required)
                .await
                .unwrap_or_else(|_| panic!("live preparation fixture TLS connection failed"));
        let handle = tokio::spawn(async move {
            let _ = driver.await;
        });
        (client, handle)
    };
    let (source, source_driver) = connect(source_dsn.into(), ca.into()).await;
    let (destination, destination_driver) = connect(destination_dsn.into(), ca.into()).await;
    let schema = format!("rp{}", uuid::Uuid::new_v4().simple());
    let setup = format!(
        "CREATE SCHEMA {schema}; CREATE TABLE {schema}.qualified_rows(id bigint NOT NULL, token uuid NOT NULL, body text, PRIMARY KEY(id,token)); CREATE TABLE {schema}.text_rows(id text PRIMARY KEY, body text)"
    );
    let setup_source = source.batch_execute(&setup).await;
    let setup_destination = destination.batch_execute(&setup).await;
    let root = root();
    let qualification = if setup_source.is_ok() && setup_destination.is_ok() {
        let mut value = definition(&root);
        fs::write(root.join("source-connection.secret"), source_dsn).unwrap();
        fs::write(root.join("destination-connection.secret"), destination_dsn).unwrap();
        for side in ["source", "destination"] {
            fs::write(root.join(format!("{side}-ca.secret")), ca).unwrap();
        }
        value["selected_objects"] =
            json!([{"schema":schema,"name":"qualified_rows","kind":"table"}]);
        let definition = MigrationDefinition::from_json(&value.to_string()).unwrap();
        let qualified = skedaddle::runner::qualify(&definition).await;
        let first_passed = qualified.is_ok_and(|result| {
            result.qualified_tables == 1
                && result.utf8_readbacks_verified
                && !result.external_effects_performed
        });
        value["selected_objects"] = json!([{"schema":schema,"name":"text_rows","kind":"table"}]);
        let id = initialize(&root, value, State::Assessed).await;
        let (code, envelope) = invoke(&root, "start", &id);
        let blocked = code == 2
            && envelope["data"]["blocker"]["table"] == format!("{schema}.text_rows")
            && envelope["data"]["blocker"]["column"] == "id"
            && envelope["data"]["blocker"]["next_step"]
                .as_str()
                .is_some_and(|step| !step.is_empty());
        first_passed && blocked
    } else {
        false
    };
    // Only this test's uniquely named SQL fixtures are removed.
    let source_cleanup = source
        .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .await;
    let destination_cleanup = destination
        .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .await;
    source_driver.abort();
    destination_driver.abort();
    assert!(
        source_cleanup.is_ok() && destination_cleanup.is_ok(),
        "live preparation fixture cleanup failed"
    );
    assert!(
        qualification,
        "live preparation did not qualify bigint/uuid and block text keys; database details withheld"
    );
}

fn assert_no_sentinel(bytes: &[u8]) {
    assert!(
        !bytes
            .windows(EXCLUDED_SENTINEL.len())
            .any(|window| window == EXCLUDED_SENTINEL.as_bytes()),
        "excluded sentinel leaked"
    );
}

fn assert_buffer_has_no_sentinel(root: &std::path::Path) -> usize {
    let mut files = 0;
    for entry in fs::read_dir(root).unwrap() {
        let entry = entry.unwrap();
        if entry.file_type().unwrap().is_dir() {
            files += assert_buffer_has_no_sentinel(&entry.path());
        } else {
            assert_no_sentinel(&fs::read(entry.path()).unwrap());
            files += 1;
        }
    }
    files
}
