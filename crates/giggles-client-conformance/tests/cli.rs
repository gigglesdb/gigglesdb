use giggles_client_conformance::Report;
use std::process::Command;

#[test]
fn cli_retains_machine_readable_report() {
    let output = Command::new(env!("CARGO_BIN_EXE_giggles-client-conformance"))
        .args(["inner-version", "42"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let path = String::from_utf8(output.stdout).unwrap();
    let report: Report = serde_json::from_slice(&std::fs::read(path.trim()).unwrap()).unwrap();
    report.validate().unwrap();
    assert_eq!(report.observations[0].class, "unknown");
    assert_eq!(report.observations[0].reason.as_deref(), Some("Protocol"));
}

#[test]
fn cli_rejects_unknown_seed_and_external_target() {
    for args in [
        vec![],
        vec!["baseline"],
        vec!["baseline", "-1"],
        vec!["baseline", "18446744073709551616"],
        vec!["unknown", "1"],
        vec!["https://example.com", "1"],
        vec!["baseline", "1", "127.0.0.1:80"],
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_giggles-client-conformance"))
            .args(args)
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
        assert_eq!(
            String::from_utf8(output.stderr).unwrap().trim(),
            "conformance: Arguments"
        );
    }
}

#[test]
fn cli_lists_only_closed_scripts() {
    let output = Command::new(env!("CARGO_BIN_EXE_giggles-client-conformance"))
        .arg("--list")
        .output()
        .unwrap();
    assert!(output.status.success());
    let text = String::from_utf8(output.stdout).unwrap();
    assert_eq!(
        text.lines().count(),
        giggles_client_conformance::Scenario::ALL.len()
    );
    assert!(
        text.lines()
            .all(|s| s.parse::<giggles_client_conformance::Scenario>().is_ok())
    );
}
