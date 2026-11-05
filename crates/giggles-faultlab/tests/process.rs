use giggles_faultlab::process::*;
use serde_json::json;
use std::{
    io::{BufRead, Write},
    process::Command,
    time::{Duration, Instant},
};
#[test]
fn protocol_child() {
    let Ok(mode) = std::env::var("FAULTLAB_TEST_CHILD") else {
        return;
    };
    if mode == "stderr" {
        let _ = std::io::stderr().write_all(&vec![b'x'; MAX_LOG as usize + 2]);
    }
    if mode == "oversize" {
        println!("{}", "x".repeat(MAX_LINE + 1));
        return;
    }
    println!("{{\"ready\":true}}");
    std::io::stdout().flush().unwrap();
    for line in std::io::stdin().lock().lines() {
        let line = line.unwrap();
        let v: serde_json::Value = serde_json::from_str(&line).unwrap();
        if mode == "respond" {
            println!("{}", json!({"id":v["id"],"reply":{"code":0}}));
            std::io::stdout().flush().unwrap();
        }
    }
}
fn command(mode: &str) -> Command {
    let mut c = Command::new(std::env::current_exe().unwrap());
    c.args(["--exact", "protocol_child", "--nocapture"])
        .env("FAULTLAB_TEST_CHILD", mode);
    c
}
#[test]
fn fixture_names_unique() {
    let handles = (0..24)
        .map(|_| std::thread::spawn(|| fresh_directory("unique").unwrap()))
        .collect::<Vec<_>>();
    let set = handles
        .into_iter()
        .map(|h| h.join().unwrap())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(set.len(), 24);
    for path in set {
        assert!(path.exists());
    }
}
#[test]
fn rejects_unsafe_fixture_label() {
    for label in ["", "../escape", "x/y", "a b"] {
        assert!(fresh_directory(label).is_err());
    }
}
#[test]
fn missing_binary_returns_error() {
    let root = fresh_directory("missing").unwrap();
    assert!(
        OwnedNode::start(
            &root.join("nonexistent.exe"),
            &root.join("config"),
            &root,
            1
        )
        .is_err()
    );
}
#[test]
fn process_deadline_bounded() {
    let root = fresh_directory("deadline").unwrap();
    let mut n = OwnedNode::start_command(command("silent"), &root, 1).unwrap();
    n.send(json!({"id":1})).unwrap();
    let start = Instant::now();
    assert!(
        n.until(|v| v["id"] == 1, Duration::from_millis(50))
            .is_err()
    );
    assert!(start.elapsed() < Duration::from_secs(1));
}
#[test]
fn owned_child_teardown() {
    let root = fresh_directory("owned").unwrap();
    let mut a = OwnedNode::start_command(command("respond"), &root, 1).unwrap();
    let mut b = OwnedNode::start_command(command("respond"), &root, 2).unwrap();
    assert_ne!(a.pid(), b.pid());
    a.terminate().unwrap();
    assert_eq!(
        b.control(1, json!({"op":"test"})).unwrap()["reply"]["code"],
        0
    );
    b.terminate().unwrap();
    b.terminate().unwrap();
}
#[test]
fn oversized_output_rejected() {
    let root = fresh_directory("oversize").unwrap();
    assert!(OwnedNode::start_command(command("oversize"), &root, 1).is_err());
}
#[test]
fn write_new_never_overwrites() {
    let root = fresh_directory("exclusive").unwrap();
    let path = root.join("kept");
    write_new(&path, b"original").unwrap();
    assert!(write_new(&path, b"replacement").is_err());
    assert_eq!(std::fs::read(path).unwrap(), b"original");
}

#[test]
fn oversized_input_rejected_before_send() {
    let root = fresh_directory("input-cap").unwrap();
    let mut n = OwnedNode::start_command(command("respond"), &root, 1).unwrap();
    assert!(n.send(json!({"large":"x".repeat(MAX_LINE+1)})).is_err());
    assert_eq!(
        n.control(1, json!({"op":"test"})).unwrap()["reply"]["code"],
        0
    );
}
#[test]
fn oversized_stderr_is_explicit_failure() {
    let root = fresh_directory("stderr-cap").unwrap();
    if let Ok(mut n) = OwnedNode::start_command(command("stderr"), &root, 1) {
        assert!(n.terminate().is_err());
    }
    assert!(
        std::fs::metadata(root.join("node-1.stderr.log"))
            .unwrap()
            .len()
            <= MAX_LOG + 1
    );
}
#[test]
fn sha256_matches_known_vector() {
    let root = fresh_directory("hash").unwrap();
    let p = root.join("abc");
    write_new(&p, b"abc").unwrap();
    assert_eq!(
        file_sha256(&p).unwrap(),
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );
}
