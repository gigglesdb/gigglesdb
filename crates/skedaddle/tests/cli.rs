use serde_json::{Value, json};
use skedaddle::{
    MigrationDefinition,
    coordinator::{Event, State, Transition, epoch::Coordinator, journal::Journal, local::Local},
};
use std::{
    fs::{self, OpenOptions},
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
    time::Duration,
};
fn root() -> PathBuf {
    let root =
        PathBuf::from(std::env::var_os("SKEDADDLE_TEST_ROOT").expect("set SKEDADDLE_TEST_ROOT"))
            .join(format!("cli-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&root).unwrap();
    root
}
fn invoke(root: &Path, args: &[&str]) -> (i32, Value) {
    let output = Command::new(env!("CARGO_BIN_EXE_skedaddle"))
        .arg("--state-root")
        .arg(root)
        .args(args)
        .output()
        .unwrap();
    let code = output.status.code().unwrap();
    let value: Value = serde_json::from_slice(&output.stdout).unwrap_or_else(|e| {
        panic!(
            "{e}: stdout {:?}, stderr {:?}",
            output.stdout, output.stderr
        )
    });
    assert_eq!(value["schema_version"], 1);
    assert_eq!(value["exit_code"], code);
    assert_eq!(
        value["outcome"],
        match code {
            0 => "success",
            2 => "blocked",
            3 => "invalid-input",
            4 => "illegal-transition",
            5 => "recovery-required",
            6 => "infrastructure",
            7 => "internal",
            8 => "non-owner",
            9 => "not-wired",
            _ => panic!("unexpected code"),
        }
    );
    if code == 0 {
        assert!(value["problem"].is_null());
    }
    if code != 0 {
        let p = &value["problem"];
        for key in ["what", "why", "next_step"] {
            assert!(!p[key].as_str().unwrap().is_empty());
        }
    }
    (code, value)
}
fn definition(root: &Path) -> PathBuf {
    let path = root.join("definition.json");
    let value = json!({"definition_version":1,"source":{"connection_ref":"env:SOURCE","tls_ca_ref":"env:PRIVATE_SOURCE_CA_IDENTIFIER","engine":"postgresql","version":"17"},"destination":{"connection_ref":"env:DESTINATION","tls_ca_ref":"file:/private/destination-ca-identifier","engine":"postgresql","version":"17"},"selected_objects":[{"schema":"public","name":"items","kind":"table"}],"excluded_objects":[],"mapping_version":"1","connector_versions":{"source":"1","destination":"1"},"buffer":{"backend":{"type":"local-fs","options":{"path":root.join("buffer").to_str().unwrap()}},"manifest_put":"create-only"},"budgets":{"max_snapshot_bytes":1000000,"max_buffer_bytes":1000000,"max_downtime_seconds":30,"max_replication_lag_seconds":30,"max_parallel_tables":1},"readiness_policy":{"max_lag_seconds":1,"required_consecutive_checks":2,"require_full_validation":true},"buffer_retention":{"min_seconds":60,"max_seconds":3600,"retain_on_failure":true},"fence_plan":{"application_roles":["app"],"acknowledged_residual_risks":[]},"hooks":[],"rollback_contract":{"strategy":"restart-from-source","source_retention_seconds":3600,"procedure":"Restart from source","acknowledged_data_loss_window_seconds":0}});
    fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
    MigrationDefinition::from_path(&path).unwrap();
    path
}
fn init(root: &Path) -> String {
    let path = definition(root);
    let (code, v) = invoke(root, &["init", path.to_str().unwrap()]);
    assert_eq!(code, 0, "{v}");
    let text = v.to_string();
    assert!(!text.contains("PRIVATE_SOURCE_CA_IDENTIFIER"), "{text}");
    assert!(
        !text.contains("/private/destination-ca-identifier"),
        "{text}"
    );
    v["data"]["id"].as_str().unwrap().into()
}
async fn advance(root: &Path, id: &str, states: &[State]) {
    let store = object_store::local::LocalFileSystem::new_with_prefix(root).unwrap();
    let journal = Journal::new(Arc::new(store), "skedaddle", id, Duration::from_secs(30)).unwrap();
    let local = Local::open_journal(root.join(format!("{id}.local")), &journal)
        .await
        .unwrap();
    let mut c = Coordinator::observer(journal, local, "cli-test").unwrap();
    c.acquire().await.unwrap();
    for state in states {
        c.commit(Transition::Lifecycle {
            event: if *state == State::VerifiedReady {
                Event::Readiness {
                    facts: skedaddle::coordinator::ReadinessFacts {
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
                }
            } else {
                Event::Advance { to: *state }
            },
        })
        .await
        .unwrap();
    }
}
#[test]
fn exit_code_table_is_pinned() {
    use skedaddle::cli::Outcome::*;
    assert_eq!(
        [
            Success as u8,
            Blocked as u8,
            InvalidInput as u8,
            IllegalTransition as u8,
            RecoveryRequired as u8,
            Infrastructure as u8,
            Internal as u8,
            NonOwner as u8,
            NotWired as u8
        ],
        [0, 2, 3, 4, 5, 6, 7, 8, 9]
    );
}
#[test]
fn parsing_help_definition_and_missing_state() {
    let r = root();
    assert_eq!(invoke(&r, &["unknown"]).0, 3);
    assert_eq!(invoke(&r, &["--help"]).0, 0);
    assert_eq!(invoke(&r, &["--version"]).0, 0);
    let bad = r.join("bad.json");
    fs::write(&bad, b"{}").unwrap();
    assert_eq!(invoke(&r, &["init", bad.to_str().unwrap()]).0, 3);
    assert_eq!(invoke(&r, &["status", "missing"]).0, 3);
    let missing = r.join("absent");
    assert_eq!(invoke(&missing, &["status", "missing"]).0, 6);
}
#[test]
fn initialized_status_report_assessment_and_not_wired_commands() {
    let r = root();
    let id = init(&r);
    let (code, status) = invoke(&r, &["status", &id]);
    assert_eq!(code, 0);
    assert_eq!(status["data"]["state"]["lifecycle"]["state"], "defined");
    assert_eq!(status["data"]["observer"], true);
    for command in ["status", "report", "assess"] {
        let output = invoke(&r, &[command, &id]).1;
        let text = output.to_string();
        assert!(!text.contains("PRIVATE_SOURCE_CA_IDENTIFIER"), "{text}");
        assert!(
            !text.contains("/private/destination-ca-identifier"),
            "{text}"
        );
        if command != "assess" {
            assert_eq!(output["data"]["tls_trust"]["source"]["ca_configured"], true);
            assert_eq!(
                output["data"]["tls_trust"]["source"]["reference_kind"],
                "env"
            );
            assert_eq!(
                output["data"]["tls_trust"]["destination"]["reference_kind"],
                "file"
            );
            assert_eq!(
                output["data"]["state"]["definition"]["source"]["connection_ref"],
                "env:SOURCE"
            );
        }
    }

    assert_eq!(invoke(&r, &["report", &id]).0, 0);
    assert_eq!(invoke(&r, &["assess", &id]).0, 2);
    assert_eq!(invoke(&r, &["assess", &id, "--sample", "10"]).0, 9);
    for command in ["start", "resume"] {
        assert_eq!(invoke(&r, &[command, &id]).0, 9);
    }
    assert_eq!(invoke(&r, &["rollback", &id]).0, 3);
    for deadline in ["0s", "soon", "5d", "-1s"] {
        assert_eq!(invoke(&r, &["rollback", &id, "--deadline", deadline]).0, 3);
    }
    assert_eq!(invoke(&r, &["finalize", &id]).0, 3);
    let (code, verification) = invoke(&r, &["verify", &id]);
    assert_eq!(code, 6, "{verification}");
    assert_eq!(verification["outcome"], "infrastructure");
    assert!(
        !verification["problem"]["next_step"]
            .as_str()
            .unwrap()
            .is_empty()
    );
    let (code, gc) = invoke(&r, &["buffer", "gc", &id]);
    assert_eq!(code, 6, "{gc}");
    assert_eq!(gc["data"]["error"]["kind"], "no_owner");
    for args in [
        vec!["buffer", "gc"],
        vec!["buffer", "gc", "../bad"],
        vec!["buffer", "gc", &id, "--unknown"],
    ] {
        assert_eq!(invoke(&r, &args).0, 3);
    }
    let (code, idle) = invoke(&r, &["pause", &id]);
    assert_eq!(code, 0);
    assert_eq!(idle["data"]["pause_requested"], false);
    assert_eq!(idle["data"]["final_state"], "defined");
    assert_eq!(invoke(&r, &["abort", &id]).0, 4);
}
#[tokio::test]
async fn legal_pause_and_second_process_observer() {
    let r = root();
    let id = init(&r);
    advance(&r, &id, &[State::Assessed, State::Copying]).await;
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(r.join(format!("{id}.owner")))
        .unwrap();
    file.try_lock().unwrap();
    let before = invoke(&r, &["status", &id]).1;
    assert_eq!(invoke(&r, &["pause", &id]).0, 0);
    let after = invoke(&r, &["report", &id]).1;
    assert_eq!(before["data"]["state"], after["data"]["state"]);
    drop(file);
    let (code, paused) = invoke(&r, &["pause", &id]);
    assert_eq!(code, 0, "{paused}");
    assert_eq!(paused["data"]["pause_requested"], false);
    assert_eq!(paused["data"]["final_state"], "copying");
    assert_eq!(invoke(&r, &["pause", &id]).0, 0);
}
#[tokio::test]
async fn coordinator_only_abort_and_corrupt_journal_requires_recovery() {
    let r = root();
    let id = init(&r);
    advance(
        &r,
        &id,
        &[
            State::Assessed,
            State::Copying,
            State::CatchingUp,
            State::VerifiedReady,
            State::SourceFenced,
        ],
    )
    .await;
    let (code, result) = invoke(&r, &["abort", &id]);
    assert_eq!(code, 0);
    assert_eq!(result["data"]["external_effects_performed"], false);
    assert_eq!(
        invoke(&r, &["status", &id]).1["data"]["state"]["lifecycle"]["state"],
        "aborted-to-source"
    );
    let first = r
        .join("skedaddle")
        .join("journal")
        .join(&id)
        .join("00000000000000000001.json");
    fs::write(first, b"{broken").unwrap();
    assert_eq!(invoke(&r, &["status", &id]).0, 5);
}
#[test]
fn definition_override_and_operational_options() {
    let r = root();
    let id = init(&r);
    let path = r.join("definition.json");
    let path = path.to_str().unwrap();
    assert_eq!(invoke(&r, &["--definition", path, "status", &id]).0, 0);
    assert_eq!(invoke(&r, &["--definition", path, "check", path]).0, 2);
    assert_eq!(invoke(&r, &["--definition", path, "init", path]).0, 4);
    let mut different: Value = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
    different["mapping_version"] = json!("different");
    let other = r.join("other.json");
    fs::write(&other, serde_json::to_vec(&different).unwrap()).unwrap();
    let other = other.to_str().unwrap();
    for args in [
        vec!["--definition", other, "status", &id],
        vec!["--definition", other, "check", path],
        vec!["--definition", other, "init", path],
    ] {
        assert_eq!(invoke(&r, &args).0, 3);
    }
    let (code, verification) = invoke(
        &r,
        &["verify", &id, "--tables", "public.items", "public.other"],
    );
    assert_eq!(code, 6, "{verification}");
    assert_eq!(verification["outcome"], "infrastructure");
    assert!(
        !verification["problem"]["next_step"]
            .as_str()
            .unwrap()
            .is_empty()
    );
    // No owner process is running: the request reaches no owner, which is an
    // infrastructure/precondition failure carrying the runner's envelope.
    let (code, cutover) = invoke(&r, &["cutover", &id, "--deadline", "30s"]);
    assert_eq!(code, 6, "{cutover}");
    assert_eq!(cutover["outcome"], "infrastructure");
    assert_eq!(cutover["data"]["outcome"], "failed", "{cutover}");
    assert_eq!(cutover["data"]["error"]["kind"], "no_owner", "{cutover}");
    for deadline in [None, Some("0s"), Some("soon"), Some("5d"), Some("-1s")] {
        let mut args = vec!["cutover", id.as_str()];
        if let Some(deadline) = deadline {
            args.extend(["--deadline", deadline]);
        }
        assert_eq!(invoke(&r, &args).0, 3, "{deadline:?}");
    }
    assert_eq!(invoke(&r, &["finalize", &id, "--drop-objects"]).0, 3);
    for id in ["C:bad", "bad:name", "../bad", "bad?name"] {
        assert_eq!(invoke(&r, &["status", id]).0, 3);
    }
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(r.join(format!("{id}.owner")))
        .unwrap();
    file.try_lock().unwrap();
    assert_eq!(invoke(&r, &["start", &id]).0, 8);
}
#[test]
fn watch_streams_flushed_observer_envelopes() {
    use std::io::{BufRead, BufReader};
    let r = root();
    let id = init(&r);
    let mut child = Command::new(env!("CARGO_BIN_EXE_skedaddle"))
        .arg("--state-root")
        .arg(&r)
        .args(["status", &id, "--watch"])
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let mut lines = BufReader::new(stdout).lines();
    for _ in 0..2 {
        let value: Value = serde_json::from_str(&lines.next().unwrap().unwrap()).unwrap();
        assert_eq!(value["outcome"], "success");
        assert_eq!(value["data"]["observer"], true);
    }
    child.kill().unwrap();
    child.wait().unwrap();
}
#[tokio::test]
async fn abort_with_recorded_fence_requires_operational_cleanup() {
    let r = root();
    let id = init(&r);
    advance(
        &r,
        &id,
        &[
            State::Assessed,
            State::Copying,
            State::CatchingUp,
            State::VerifiedReady,
            State::SourceFenced,
        ],
    )
    .await;
    let store = object_store::local::LocalFileSystem::new_with_prefix(&r).unwrap();
    let journal = Journal::new(Arc::new(store), "skedaddle", &id, Duration::from_secs(30)).unwrap();
    let local = Local::open_journal(r.join(format!("{id}.local")), &journal)
        .await
        .unwrap();
    let mut c = Coordinator::observer(journal, local, "fence-test").unwrap();
    c.acquire().await.unwrap();
    c.commit(Transition::FenceState {
        db: "source".into(),
        role: "app".into(),
        saved_privileges_json: "{}".into(),
    })
    .await
    .unwrap();
    drop(c);
    let before = invoke(&r, &["status", &id]).1;
    assert_eq!(invoke(&r, &["abort", &id]).0, 9);
    assert_eq!(
        before["data"]["state"],
        invoke(&r, &["status", &id]).1["data"]["state"]
    );
}
