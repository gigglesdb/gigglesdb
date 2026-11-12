//! Real CLI failure/control paths; the online qualifier is loudly config-gated.
use serde_json::{Value, json};
use skedaddle::{
    MigrationDefinition,
    coordinator::{Event, State, Transition, epoch::Coordinator, journal::Journal, local::Local},
};
use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::Arc,
    time::Duration,
};

fn root() -> PathBuf {
    let path =
        PathBuf::from(std::env::var_os("SKEDADDLE_TEST_ROOT").expect("set SKEDADDLE_TEST_ROOT"))
            .join(format!("runner-verify-{}", uuid::Uuid::new_v4()));
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
    for side in ["source", "destination"] {
        let path = root.join(format!("{side}.secret"));
        fs::write(&path,"host=127.0.0.1 port=1 user=verify_private_user password=verify_private_password dbname=postgres").unwrap();
        value[side]["connection_ref"] = json!(format!("file:{}", path.display()));
        value[side]["tls"] = json!("disable-for-local-test");
        value[side].as_object_mut().unwrap().remove("tls_ca_ref");
    }
    value
}
async fn initialize(root: &Path, value: Value) -> String {
    let definition = MigrationDefinition::from_json(&value.to_string()).unwrap();
    let id = definition.canonical_digest().unwrap()[..24].to_owned();
    let journal = Journal::new(
        Arc::new(object_store::local::LocalFileSystem::new_with_prefix(root).unwrap()),
        "skedaddle",
        &id,
        Duration::from_secs(5),
    )
    .unwrap();
    let mut coordinator = Coordinator::observer(
        journal,
        Local::create(root.join(format!("{id}.local"))).unwrap(),
        "verification-fixture",
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
    id
}
fn invoke(root: &Path, args: &[&str]) -> (i32, Value) {
    let output = Command::new(env!("CARGO_BIN_EXE_skedaddle"))
        .arg("--state-root")
        .arg(root)
        .args(args)
        .output()
        .unwrap();
    for secret in ["verify_private_user", "verify_private_password"] {
        assert!(!String::from_utf8_lossy(&output.stdout).contains(secret));
        assert!(!String::from_utf8_lossy(&output.stderr).contains(secret));
    }
    let value = serde_json::from_slice(&output.stdout).expect("verification CLI envelope absent");
    (output.status.code().unwrap(), value)
}
#[tokio::test]
async fn verify_real_unreachable_endpoint_returns_sanitized_infrastructure_six() {
    let root = root();
    let mut value = definition(&root);
    let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = socket.local_addr().unwrap().port();
    drop(socket);
    fs::write(root.join("source.secret"),format!("host=127.0.0.1 port={port} user=verify_private_user password=verify_private_password dbname=postgres")).unwrap();
    value["selected_objects"] = json!([{"schema":"public","name":"accounts","kind":"table"}]);
    let id = initialize(&root, value).await;
    let (code, envelope) = invoke(&root, &["verify", &id]);
    assert_eq!(code, 6, "{envelope}");
    assert_eq!(envelope["command"], "verify");
    let status = invoke(&root, &["status", &id]).1;
    assert_eq!(status["data"]["verification"]["verification"], false);
    assert!(status["data"]["state"]["verification"].is_null());
}

struct Child(Option<std::process::Child>);
impl Drop for Child {
    fn drop(&mut self) {
        if let Some(child) = self.0.as_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}
#[test]
fn control_owner_process_fixture() {
    let Some(path) = std::env::var_os("SKEDADDLE_VERIFY_CONTROL_FIXTURE") else {
        return;
    };
    let control = PathBuf::from(path);
    let open = |path| {
        fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .unwrap()
    };
    let owner = open(control.join("owner.lock"));
    owner.lock().unwrap();
    let migration = open(PathBuf::from(
        std::env::var_os("SKEDADDLE_VERIFY_MIGRATION_LOCK").unwrap(),
    ));
    migration.lock().unwrap();
    let generation = uuid::Uuid::new_v4();
    fs::write(control.join("owner-generation"), generation.to_string()).unwrap();
    fs::write(control.join("ready"), []).unwrap();
    let started = std::time::Instant::now();
    let mut answered = false;
    loop {
        assert!(
            started.elapsed() < Duration::from_secs(30),
            "control request/pause missing"
        );
        for entry in fs::read_dir(&control).unwrap() {
            let path = entry.unwrap().path();
            let name = path.file_name().unwrap().to_string_lossy();
            if !answered
                && name.starts_with(&format!("verify-{generation}-"))
                && name.ends_with(".request")
            {
                let request: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
                assert_eq!(request["generation"], generation.to_string());
                assert_eq!(request["tables"], json!(["public.accounts"]));
                let staging = path.with_extension("staging");
                fs::write(&staging,serde_json::to_vec(&json!({"exit_code":2,"data":{"complete":false,"verification":false,"control_fixture_acknowledged":true}})).unwrap()).unwrap();
                fs::rename(staging, path.with_extension("response")).unwrap();
                answered = true;
            }
        }
        if answered && control.join(format!("pause-{generation}")).is_file() {
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}
#[tokio::test]
async fn real_owner_lock_verify_request_and_pause_round_trip_preserves_journal() {
    let root = root();
    let id = initialize(&root, definition(&root)).await;
    let control = root.join(format!("{id}.control"));
    fs::create_dir_all(&control).unwrap();
    let mut child = Child(Some(
        Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "control_owner_process_fixture", "--nocapture"])
            .env("SKEDADDLE_VERIFY_CONTROL_FIXTURE", &control)
            .env(
                "SKEDADDLE_VERIFY_MIGRATION_LOCK",
                root.join(format!("{id}.owner")),
            )
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    ));
    tokio::time::timeout(Duration::from_secs(10), async {
        while !control.join("ready").exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let before = invoke(&root, &["status", &id]).1["data"]["state"].clone();
    let (code, response) = invoke(&root, &["verify", &id, "--tables", "public.accounts"]);
    assert_eq!(code, 2, "{response}");
    assert_eq!(response["data"]["control_fixture_acknowledged"], true);
    assert_eq!(response["data"]["complete"], false);
    let (code, response) = invoke(&root, &["pause", &id]);
    assert_eq!(code, 0, "{response}");
    assert_eq!(response["data"]["pause_requested"], true);
    assert!(child.0.as_mut().unwrap().wait().unwrap().success());
    child.0.take();
    assert_eq!(invoke(&root, &["status", &id]).1["data"]["state"], before);
}
async fn connect(dsn: &str, ca: &str) -> (tokio_postgres::Client, tokio::task::JoinHandle<()>) {
    let (client, driver, _) = skedaddle::pg::tls::connect(
        &skedaddle::pg::tls::Config::new(dsn.parse().unwrap(), ca),
        &skedaddle::security::TlsPolicy::Required,
    )
    .await
    .expect("live verification fixture TLS failed");
    (
        client,
        tokio::spawn(async move {
            let _ = driver.await;
        }),
    )
}
#[tokio::test]
async fn live_owner_verifies_then_reports_direct_destination_corruption_and_pauses() {
    use futures::FutureExt;
    use std::{io::Write, panic::AssertUnwindSafe};
    let Some(path) = std::env::var_os("SKEDADDLE_TEST_CONFIG") else {
        let _ = writeln!(
            std::io::stderr(),
            "SKIP runner_verify: SKEDADDLE_TEST_CONFIG absent; live owner CatchingUp/full coverage/exit0/status readiness/direct SQL corruption/exit2/drilldown/pause qualification NOT RUN"
        );
        return;
    };
    let config: Value = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
    let src = config["pg_ddl"]
        .as_str()
        .expect("admin pg_ddl DSN required");
    let dst = config["pg_apply"]
        .as_str()
        .expect("admin pg_apply DSN required");
    let ca = config["ca_pem"].as_str().expect("CA bundle required");
    let (source, source_driver) = connect(src, ca).await;
    let (destination, destination_driver) = connect(dst, ca).await;
    let root = root();
    let schema = format!("rv{}", uuid::Uuid::new_v4().simple());
    let ddl = format!(
        "CREATE SCHEMA {schema};CREATE TABLE {schema}.rows(id bigint PRIMARY KEY,body text)"
    );
    source.batch_execute(&ddl).await.unwrap();
    destination.batch_execute(&ddl).await.unwrap();
    source
        .batch_execute(&format!(
            "INSERT INTO {schema}.rows SELECT i,'source-'||i FROM generate_series(1,5) i"
        ))
        .await
        .unwrap();
    let mut value = definition(&root);
    value["selected_objects"] = json!([{"schema":schema,"name":"rows","kind":"table"}]);
    for (side, dsn) in [("source", src), ("destination", dst)] {
        fs::write(root.join(format!("{side}.secret")), dsn).unwrap();
        let ca_ref = root.join(format!("{side}-ca.secret"));
        fs::write(&ca_ref, ca).unwrap();
        value[side]["tls"] = json!("require");
        value[side]["tls_ca_ref"] = json!(format!("file:{}", ca_ref.display()));
    }
    let id = initialize(&root, value).await;
    let proof = AssertUnwindSafe(async {
        let mut owner = Child(Some(
            Command::new(env!("CARGO_BIN_EXE_skedaddle"))
                .arg("--state-root")
                .arg(&root)
                .args(["start", &id, "--until", "paused"])
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap(),
        ));
        tokio::time::timeout(Duration::from_secs(60), async {
            loop {
                let status = invoke(&root, &["status", &id]).1;
                if status["data"]["state"]["lifecycle"]["state"] == "catching-up" {
                    break;
                }
                assert!(
                    owner.0.as_mut().unwrap().try_wait().unwrap().is_none(),
                    "owner exited before CatchingUp"
                );
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("owner never reached CatchingUp");
        let (code, verified) = invoke(&root, &["verify", &id]);
        assert_eq!(code, 0, "{verified}");
        assert_eq!(verified["data"]["complete"], true);
        assert_eq!(
            verified["data"]["report"]["tables"][0]["counts"]["verified"],
            5
        );
        assert_eq!(
            verified["data"]["readiness_verification"], true,
            "{verified}"
        );
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if invoke(&root, &["status", &id]).1["data"]["verification"]["verification"] == true
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("status never showed current verification readiness");
        destination
            .batch_execute(&format!(
                "UPDATE {schema}.rows SET body='corrupted-destination' WHERE id=3"
            ))
            .await
            .unwrap();
        let (code, mismatch) = invoke(&root, &["verify", &id]);
        assert_eq!(code, 2, "{mismatch}");
        assert_eq!(mismatch["data"]["readiness_verification"], false);
        assert_eq!(
            mismatch["data"]["report"]["tables"][0]["counts"]["mismatched"],
            1
        );
        assert!(mismatch["data"]["ranges"].to_string().contains("Changed"));
        assert_eq!(invoke(&root, &["pause", &id]).0, 0);
        tokio::time::timeout(Duration::from_secs(30), async {
            while owner.0.as_mut().unwrap().try_wait().unwrap().is_none() {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("pause did not join owner");
        let output = owner.0.take().unwrap().wait_with_output().unwrap();
        assert!(output.status.success());
    })
    .catch_unwind()
    .await;
    let trigger = skedaddle::pg::ddl::TriggerSql::new(
        &id,
        config["pg_version"].as_u64().unwrap() as u32 * 10000,
    )
    .unwrap();
    source.batch_execute(&trigger.remove).await.unwrap();
    let slot = format!("skedaddle_{id}");
    source.query("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name=$1",&[&slot]).await.unwrap();
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
