use giggles_topology::{Topology, Validated};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicU64, Ordering};

const INPUT: &str = include_str!("fixtures/three-providers.json");
static COUNTER: AtomicU64 = AtomicU64::new(0);

fn cli(command: &str, path: &Path, scenario: Option<&str>) -> Output {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_giggles-topology"));
    cmd.arg(command).arg(path);
    if let Some(s) = scenario {
        cmd.arg(s);
    }
    cmd.output().unwrap()
}
fn fixture() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/three-providers.json")
}

// Preserve each malformed/modified fixture uniquely; no teardown removes artifacts.
fn artifact(input: &[u8]) -> PathBuf {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("artifacts")
        .join(format!(
            "cli-{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
    std::fs::create_dir_all(&dir).unwrap();
    let p = dir.join("input.json");
    std::fs::write(&p, input).unwrap();
    p
}

#[test]
fn real_cli_golden_json_and_exit_codes() {
    let f = fixture();
    let valid = cli("validate", &f, None);
    assert_eq!(valid.status.code(), Some(0));
    let body: serde_json::Value = serde_json::from_slice(&valid.stdout).unwrap();
    assert_eq!(body["valid"], true);
    for cmd in ["inspect", "export"] {
        let output = cli(cmd, &f, None);
        assert_eq!(output.status.code(), Some(0));
        assert_eq!(
            String::from_utf8(output.stdout).unwrap().trim(),
            Validated::from_json(INPUT.as_bytes())
                .unwrap()
                .export_json()
        );
    }
    let baseline = cli("analyze", &f, Some("baseline"));
    assert_eq!(baseline.status.code(), Some(0));
    let loss = cli("analyze", &f, Some("loss:provider:a"));
    assert_eq!(loss.status.code(), Some(0));
    let actual: serde_json::Value = serde_json::from_slice(&loss.stdout).unwrap();
    let golden: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/provider-a-report.json")).unwrap();
    assert_eq!(actual, golden);
    assert_eq!(
        actual["scenarios"][0]["groups"][0]["surviving_voters"],
        serde_json::json!(["nb", "nc"])
    );
    assert_eq!(
        actual["scenarios"][0]["groups"][0]["capacity"]["surviving_available"],
        2
    );
    assert_eq!(
        cli("analyze", &f, Some("two-providers")).status.code(),
        Some(3)
    );
    assert_eq!(cli("analyze", &f, None).status.code(), Some(3));
    let mut t: Topology = serde_json::from_str(INPUT).unwrap();
    t.groups[0].capacity = None;
    let missing = artifact(&serde_json::to_vec(&t).unwrap());
    assert_eq!(
        cli("analyze", &missing, Some("baseline")).status.code(),
        Some(4)
    );
    t.groups[0].requirements.clear();
    assert_eq!(
        cli(
            "analyze",
            &artifact(&serde_json::to_vec(&t).unwrap()),
            Some("baseline")
        )
        .status
        .code(),
        Some(4)
    );
}

#[test]
fn real_cli_invalid_input_usage_io_limits_and_unknown_scenario() {
    let f = fixture();
    for (cmd, s) in [
        ("bogus", None),
        ("analyze", Some("absent")),
        ("export", Some("extra")),
    ] {
        assert_eq!(cli(cmd, &f, s).status.code(), Some(2));
    }
    assert_eq!(
        Command::new(env!("CARGO_BIN_EXE_giggles-topology"))
            .output()
            .unwrap()
            .status
            .code(),
        Some(2)
    );
    let malformed = artifact(b"{bad");
    let result = cli("validate", &malformed, None);
    assert_eq!(result.status.code(), Some(2));
    assert!(result.stdout.is_empty());
    assert!(
        serde_json::from_slice::<serde_json::Value>(&result.stderr).unwrap()["error"].is_string()
    );
    assert_eq!(
        cli(
            "validate",
            &malformed.with_file_name("never-created.json"),
            None
        )
        .status
        .code(),
        Some(2)
    );
    assert_eq!(
        cli(
            "validate",
            &artifact(&vec![b' '; giggles_topology::MAX_BYTES + 1]),
            None
        )
        .status
        .code(),
        Some(2)
    );
}
