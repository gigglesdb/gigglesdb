use giggles_history::{Limits, Verdict, check, parse_jsonl};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    fs::File,
    io::BufReader,
    path::{Path, PathBuf},
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

fn root() -> PathBuf {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("artifacts")
        .join(format!(
            "test-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
    std::fs::create_dir_all(&root).unwrap();
    root
}

fn source_hashes() -> Value {
    fn visit(path: &Path, files: &mut Vec<PathBuf>) {
        for item in std::fs::read_dir(path).unwrap() {
            let path = item.unwrap().path();
            if path.is_dir() {
                visit(&path, files);
            } else {
                files.push(path);
            }
        }
    }
    let crates = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let mut files = Vec::new();
    for name in [
        "giggles-consensus",
        "giggles-server",
        "giggles-core",
        "giggles-model",
        "giggles-history",
    ] {
        let base = crates.join(name);
        visit(&base.join("src"), &mut files);
        files.push(base.join("Cargo.toml"));
        files.push(base.join("Cargo.lock"));
    }
    files.sort();
    let map: serde_json::Map<String, Value> = files
        .into_iter()
        .filter(|p| p.exists())
        .map(|p| {
            let hash = format!("{:x}", Sha256::digest(std::fs::read(&p).unwrap()));
            (
                p.strip_prefix(crates)
                    .unwrap()
                    .to_string_lossy()
                    .replace('\\', "/"),
                json!(hash),
            )
        })
        .collect();
    Value::Object(map)
}

#[test]
fn actual_three_process_campaigns_and_cli() {
    let root = root();
    let before = source_hashes();
    std::fs::write(
        root.join("owner-sources-before-build.json"),
        serde_json::to_vec_pretty(&before).unwrap(),
    )
    .unwrap();
    // --locked prevents modifying the owned consensus manifest/lockfile.
    // All build outputs, including dependency build scripts, stay in our crate.
    let target = Path::new(env!("CARGO_MANIFEST_DIR")).join("target/owner-node");
    let profile = if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    };
    let mut build = Command::new(env!("CARGO"));
    build
        .args(["build", "--locked", "--manifest-path"])
        .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join("../giggles-consensus/Cargo.toml"))
        .args(["--bin", "giggles-node", "-j", "2", "--target-dir"])
        .arg(&target);
    if profile == "release" {
        build.arg("--release");
    }
    let output = build.output().unwrap();
    std::fs::write(root.join("node-build.stdout.log"), &output.stdout).unwrap();
    std::fs::write(root.join("node-build.stderr.log"), &output.stderr).unwrap();
    assert!(
        output.status.success(),
        "node build failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let after = source_hashes();
    std::fs::write(
        root.join("owner-sources-after-build.json"),
        serde_json::to_vec_pretty(&after).unwrap(),
    )
    .unwrap();
    assert_eq!(
        before, after,
        "owner sources changed during node build; binary attribution needs another run"
    );
    let node = target
        .join(profile)
        .join(format!("giggles-node{}", std::env::consts::EXE_SUFFIX));
    for seed in [42, 43] {
        let output = Command::new(env!("CARGO_BIN_EXE_giggles-history-runtime"))
            .arg(&node)
            .arg(&root)
            .arg(seed.to_string())
            .output()
            .unwrap();
        std::fs::write(root.join(format!("cli-{seed}.stdout.json")), &output.stdout).unwrap();
        std::fs::write(root.join(format!("cli-{seed}.stderr.log")), &output.stderr).unwrap();
        assert!(
            output.status.success(),
            "campaign failed: {} artifacts {}",
            String::from_utf8_lossy(&output.stderr),
            root.display()
        );
        let result: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(result["verdict"], "PASS");
        let artifacts = PathBuf::from(result["artifacts"].as_str().unwrap());
        let history = parse_jsonl(BufReader::new(
            File::open(artifacts.join("history.jsonl")).unwrap(),
        ))
        .unwrap();
        assert_eq!(
            check(&history, Limits::default()).unwrap().verdict,
            Verdict::Pass
        );
        assert!(history.attempts.iter().all(|a| a.durable.is_none()));
        assert_eq!(
            history
                .attempts
                .iter()
                .filter(|a| a.logical_id == "task-1-op-5")
                .count(),
            3
        );
        for name in [
            "stale_read",
            "partial_acknowledged_write",
            "duplicate_logical_effect",
        ] {
            let history = parse_jsonl(BufReader::new(
                File::open(artifacts.join(format!("negative-{name}.jsonl"))).unwrap(),
            ))
            .unwrap();
            assert_eq!(
                check(&history, Limits::default()).unwrap().verdict,
                Verdict::Fail
            );
        }
        let events = std::fs::read_to_string(artifacts.join("events.jsonl")).unwrap();
        let values: Vec<Value> = events
            .lines()
            .map(|s| serde_json::from_str(s).unwrap())
            .collect();
        for (index, v) in values.iter().enumerate() {
            assert_eq!(v["tick"], index as u64 + 1);
        }
        assert!(values.iter().filter(|v| v["kind"] == "spawn").count() >= 8);
        assert!(values.iter().any(|v| v["kind"] == "proxy_discard"));
        assert!(values.iter().any(|v| v["kind"] == "minority_window_closed"));
    }
    std::fs::write(
        root.join("test-result.json"),
        serde_json::to_vec_pretty(&json!({"seeds":[42,43],"result":"PASS","node":node})).unwrap(),
    )
    .unwrap();
}

#[test]
fn cli_rejects_missing_binary_and_invalid_arguments() {
    for args in [
        vec![],
        vec!["missing", "artifacts", "not-a-seed"],
        vec!["missing", "artifacts", "42"],
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_giggles-history-runtime"))
            .args(args)
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(64));
    }
}
