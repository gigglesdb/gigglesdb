mod common;
use common::*;
use giggles_outbox::*;
use serde_json::{Value as J, json};
use std::{
    io::Write,
    path::Path,
    process::{Command, Stdio},
};
fn cli(path: &Path, input: &[u8]) -> std::process::Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_giggles-outbox"))
        .arg(path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(input).unwrap();
    child.wait_with_output().unwrap()
}
fn ok(path: &Path, req: J) -> J {
    let r = cli(path, &serde_json::to_vec(&req).unwrap());
    assert_eq!(
        r.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&r.stdout)
    );
    serde_json::from_slice(&r.stdout).unwrap()
}
#[test]
fn cli_persisted_json_workflow() {
    let p = fresh("cli");
    ok(&p, json!({"command":"create"}));
    let r = ok(
        &p,
        json!({"command":"enqueue","stream":"tenant","effect_id":"a","destination_kind":"notification","payload":"hello",
        "application":[{"sql":"CREATE TABLE orders (id BIGINT PRIMARY KEY, status TEXT NOT NULL)"},{"sql":"INSERT INTO orders VALUES (1,'accepted')"}]}),
    );
    assert_eq!(r["ok"]["commit"], "Committed");
    let r = ok(
        &p,
        json!({"command":"status","stream":"tenant","effect_id":"a"}),
    );
    assert_eq!(r["ok"]["status"]["phase"], "Ready");
    assert!(!r.to_string().contains("hello"));
    let r = ok(
        &p,
        json!({"command":"claim","stream":"tenant","owner":"worker","now":{"lower":0,"upper":0},"ttl":10,"limit":1}),
    );
    let token = r["ok"]["claim"]["Committed"][0]["token"].clone();
    assert_eq!(
        r["ok"]["claim"]["Committed"][0]["payload"],
        json!([104, 101, 108, 108, 111])
    );
    ok(
        &p,
        json!({"command":"ack","token":token,"now":{"lower":1,"upper":1}}),
    );
    let r = ok(&p, json!({"command":"reconcile"}));
    assert_eq!(r["ok"]["reconciliation"]["acked"], 1);
    let o = Outbox::open(&p).unwrap();
    assert_eq!(o.scan_application(scan()).unwrap().rows.len(), 1);
}
#[test]
fn cli_bad_large_input_and_sanitized_errors() {
    let p = fresh("cli-errors");
    for data in [
        b"not json".to_vec(),
        b"{\"command\":\"create\",\"unexpected\":1}".to_vec(),
        vec![b' '; 65537],
        br#"{"command":"status","stream":"tenant","effect_id":"a"}"#.to_vec(),
    ] {
        let r = cli(&p, &data);
        assert_eq!(r.status.code(), Some(2));
        let j: J = serde_json::from_slice(&r.stdout).unwrap();
        assert!(j.get("error").is_some());
        assert!(!String::from_utf8_lossy(&r.stdout).contains(&p.to_string_lossy().to_string()));
        assert!(r.stderr.is_empty());
    }
}
#[test]
fn cli_file_input() {
    let p = fresh("file-db");
    let input = fresh("request.json");
    std::fs::write(&input, b"{\"command\":\"create\"}").unwrap();
    let r = Command::new(env!("CARGO_BIN_EXE_giggles-outbox"))
        .arg(&p)
        .arg(input)
        .output()
        .unwrap();
    assert_eq!(r.status.code(), Some(0));
    assert!(Outbox::open(&p).is_ok());
}

// A local durable mock receiver, deliberately independent of the outbox store.
// All records are retained, and no network is involved.
fn receiver(path: &Path, effect_id: &str, idempotent: bool) -> usize {
    let previous = std::fs::read_to_string(path).unwrap_or_default();
    let mut ids: Vec<_> = previous.lines().map(str::to_owned).collect();
    if !idempotent || !ids.iter().any(|id| id == effect_id) {
        ids.push(effect_id.into());
    }
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)
        .unwrap();
    for id in &ids {
        writeln!(f, "{id}").unwrap();
    }
    f.sync_all().unwrap();
    ids.len()
}
#[test]
fn receiver_crash_child() {
    let Ok(p) = std::env::var("OUTBOX_RECEIVER_DB") else {
        return;
    };
    let receiver_path = std::env::var("OUTBOX_RECEIVER_FILE").unwrap();
    let o = Outbox::open(p).unwrap();
    let d = claim(&o, "first", 0).remove(0);
    assert_eq!(
        receiver(
            Path::new(&receiver_path),
            &d.token.effect_id,
            std::env::var("OUTBOX_RECEIVER_MODE").unwrap() == "idempotent"
        ),
        1
    );
    // Abrupt process termination after durable receiver success, before ack.
    std::process::exit(87);
}
#[test]
fn crash_after_receiver_success_redelivers_stable_id_and_negative_control() {
    for idempotent in [true, false] {
        let (p, o) = db();
        seed(&o, "stable-effect");
        drop(o);
        let receiver_path = fresh("receiver");
        let r = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "receiver_crash_child", "--nocapture"])
            .env("OUTBOX_RECEIVER_DB", &p)
            .env("OUTBOX_RECEIVER_FILE", &receiver_path)
            .env(
                "OUTBOX_RECEIVER_MODE",
                if idempotent {
                    "idempotent"
                } else {
                    "non-idempotent"
                },
            )
            .output()
            .unwrap();
        assert_eq!(
            r.status.code(),
            Some(87),
            "{}",
            String::from_utf8_lossy(&r.stdout)
        );
        let o = Outbox::open(&p).unwrap();
        let d = claim(&o, "second", 10).remove(0);
        assert_eq!(d.token.effect_id, "stable-effect");
        assert_eq!(d.attempt, 2);
        assert_eq!(
            receiver(&receiver_path, &d.token.effect_id, idempotent),
            if idempotent { 1 } else { 2 }
        );
        o.ack(&d.token, now(11)).unwrap();
        assert_eq!(o.reconcile().unwrap().acked, 1);
    }
}
