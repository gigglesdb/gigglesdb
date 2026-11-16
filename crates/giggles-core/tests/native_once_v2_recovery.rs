use giggles_core::{
    Admin, CrashPoint, DecisionPoint, Engine as StorageEngine, Limits,
    operations::{
        Bindings, Engine, Error, Request, ValueExpr,
        once::{
            ONCE_CONTRACT_VERSION, OnceBody, OnceClockUnavailable, OnceError, OnceIdentity, OnceRequest, OnceStatus,
            OnceTime,
        },
    },
    relational::Value as V,
};
use std::{
    io::{BufRead, BufReader, Write},
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::{Arc, mpsc},
    time::{Duration, Instant},
};

struct Dir(PathBuf);
impl Drop for Dir {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}
struct Worker(Child);
impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn fixture() -> (Dir, Admin, Engine) {
    let mut nonce = [0; 16];
    getrandom::fill(&mut nonce).unwrap();
    let dir = Dir(std::env::temp_dir().join(format!("native-once-v2-recovery-{:032x}", u128::from_be_bytes(nonce))));
    let admin = Admin::create_engine(&dir.0, Limits::default(), StorageEngine::V2).unwrap();
    for sql in [
        "CREATE TABLE items(id BIGINT PRIMARY KEY, n BIGINT)",
        "CREATE INDEX items_n ON items(n)",
    ] {
        admin.sql_session().execute(sql, &[], None).unwrap();
    }
    let engine = Engine::new(admin.clone());
    (dir, admin, engine)
}
fn request(admin: &Admin) -> OnceRequest {
    OnceRequest {
        contract_version: ONCE_CONTRACT_VERSION,
        identity: OnceIdentity {
            database_domain: admin.authority_metadata().unwrap().0,
            tenant: 42,
            namespace: "t13.recovery".into(),
            key: b"original".to_vec(),
        },
        retention_seconds: 60,
        body: OnceBody::Operation(Request::Insert {
            table: "items".into(),
            values: vec![
                ("id".into(), ValueExpr::Literal(V::Int(1))),
                ("n".into(), ValueExpr::Literal(V::Int(10))),
            ],
        }),
    }
}
fn clock() -> Result<OnceTime, OnceClockUnavailable> {
    Ok(OnceTime {
        lower_unix_seconds: 1000,
        upper_unix_seconds: 1001,
        valid_until: Instant::now() + Duration::from_secs(10),
    })
}
fn assert_rows(admin: &Admin, committed: bool) {
    let expected = if committed {
        vec![vec![V::Int(1), V::Int(10)]]
    } else {
        vec![]
    };
    // The equality predicate exercises the non-primary secondary index as well as the full scan.
    for filter in ["", "WHERE n=10"] {
        let rows = admin
            .sql_session()
            .execute(&format!("SELECT id,n FROM items {filter}"), &[], None)
            .unwrap()
            .rows;
        assert_eq!(rows.into_iter().map(|row| row.values).collect::<Vec<_>>(), expected);
    }
}
fn reopen_and_retry(path: &std::path::Path, original: &OnceRequest, committed: bool) {
    let admin = Admin::open(path, Limits::default()).unwrap();
    let engine = Engine::new(admin.clone());
    assert_rows(&admin, committed);
    let retained = match engine.outcome(&original.identity).unwrap() {
        OnceStatus::Retained(receipt) if committed => Some(receipt),
        OnceStatus::Absent if !committed => None,
        other => panic!("unexpected recovered identity: {other:?}"),
    };
    let receipt = engine.execute_once(original, &Bindings::new(), &clock).unwrap();
    assert_eq!(receipt.replayed, committed);
    assert!(receipt.outcome.is_ok());
    if let Some(retained) = retained {
        assert_eq!(receipt, retained);
    }
    assert_rows(&admin, true);
    assert!(
        engine
            .execute_once(original, &Bindings::new(), &|| panic!("replay clock"))
            .unwrap()
            .replayed
    );
    assert_rows(&admin, true);
}
fn child(path: &std::path::Path, mode: &str) -> Command {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", "once_v2_process_worker", "--ignored", "--nocapture"])
        .env("GIGGLES_ONCE_V2_PATH", path)
        .env("GIGGLES_ONCE_V2_MODE", mode)
        .current_dir(path)
        .stdin(Stdio::null());
    command
}
fn crash_case(mode: &str, committed: bool, crash_code: Option<i32>) {
    let (dir, admin, engine) = fixture();
    let original = request(&admin);
    drop(engine);
    drop(admin);
    let output = child(&dir.0, mode).output().unwrap();
    assert!(!output.status.success(), "child did not crash: {output:?}");
    if let Some(code) = crash_code {
        assert_eq!(output.status.code(), Some(code), "{output:?}");
    }
    if mode == "decision" {
        assert!(
            String::from_utf8_lossy(&output.stdout).contains("T13_DECISION_ABORT"),
            "{output:?}"
        );
    }
    reopen_and_retry(&dir.0, &original, committed);
}

#[test]
fn before_commit_process_crash_leaves_no_row_or_receipt_and_retry_executes() {
    crash_case("before", false, Some(86));
}
#[test]
fn error_before_commit_is_unknown_then_absent_and_retryable() {
    // Same contract as legacy lost_commit_ack_is_resolved_by_the_original_key_after_reopen:
    // the caller sees Unknown; the original key resolves to Absent and executes once.
    let (dir, admin, engine) = fixture();
    let original = request(&admin);
    admin.set_crash_point(CrashPoint::ErrorBeforeCommit).unwrap();
    let error = engine.execute_once(&original, &Bindings::new(), &clock).unwrap_err();
    assert_eq!(error, OnceError::Unknown);
    // The armed crash point also applies to SQL on this handle; verify after reopen.
    drop(engine);
    drop(admin);
    reopen_and_retry(&dir.0, &original, false);
}
#[test]
fn after_commit_process_crash_retains_row_index_and_replay_without_duplicates() {
    crash_case("after", true, Some(86));
}
#[test]
fn before_ack_process_crash_retains_row_index_and_replay_without_duplicates() {
    crash_case("ack", true, Some(86));
}
#[test]
fn error_after_commit_is_unknown_and_fences_once_calls_until_reopen() {
    let (dir, admin, engine) = fixture();
    let original = request(&admin);
    admin.set_crash_point(CrashPoint::ErrorAfterCommit).unwrap();
    assert_eq!(
        engine.execute_once(&original, &Bindings::new(), &clock),
        Err(OnceError::Unknown)
    );
    let fenced = OnceError::Operation(Error::Core(giggles_core::CoreError::Fenced.to_string()));
    assert_eq!(
        engine.execute_once(&original, &Bindings::new(), &clock),
        Err(fenced.clone())
    );
    assert_eq!(engine.outcome(&original.identity), Err(fenced));
    drop(engine);
    drop(admin);
    reopen_and_retry(&dir.0, &original, true);
}
#[test]
fn before_decision_abort_leaves_no_row_or_receipt() {
    crash_case("decision", false, None);
}
#[test]
fn killed_admitted_owner_is_absent_after_restart_and_retry_executes() {
    let (dir, admin, engine) = fixture();
    let original = request(&admin);
    drop(engine);
    drop(admin);
    let mut worker = Worker(
        child(&dir.0, "owner")
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let stdout = worker.0.stdout.take().unwrap();
    let (ready, received) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            if line.unwrap().contains("T13_OWNER_READY") {
                let _ = ready.send(());
                break;
            }
        }
    });
    // Timeout only bounds a broken child; the hook's flushed signal establishes readiness.
    received
        .recv_timeout(Duration::from_secs(60))
        .expect("admitted owner readiness");
    worker.0.kill().unwrap();
    assert!(!worker.0.wait().unwrap().success());
    reader.join().unwrap();
    drop(worker);
    reopen_and_retry(&dir.0, &original, false);
}
#[test]
#[ignore = "subprocess crash worker"]
fn once_v2_process_worker() {
    let path = std::env::var("GIGGLES_ONCE_V2_PATH").unwrap();
    let mode = std::env::var("GIGGLES_ONCE_V2_MODE").unwrap();
    let admin = Admin::open(path, Limits::default()).unwrap();
    let original = request(&admin);
    match mode.as_str() {
        "before" => admin.set_crash_point(CrashPoint::BeforeCommit).unwrap(),
        "after" => admin.set_crash_point(CrashPoint::AfterCommit).unwrap(),
        "ack" => admin.set_crash_point(CrashPoint::BeforeAck).unwrap(),
        "decision" | "owner" => {
            let owner = mode == "owner";
            admin.set_decision_hook(Some(Arc::new(move |point| {
                if point == DecisionPoint::BeforeDecision {
                    if !owner {
                        println!("T13_DECISION_ABORT");
                        std::io::stdout().flush().unwrap();
                        std::process::abort();
                    }
                    println!("T13_OWNER_READY");
                    std::io::stdout().flush().unwrap();
                    loop {
                        std::thread::park();
                    }
                }
            })));
        }
        _ => panic!("unknown crash mode"),
    }
    Engine::new(admin)
        .execute_once(&original, &Bindings::new(), &clock)
        .unwrap();
    panic!("crash seam did not terminate")
}
