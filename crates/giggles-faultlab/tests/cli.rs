use std::{path::PathBuf, process::Command};
fn cli(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_giggles-faultlab"))
        .args(args)
        .output()
        .unwrap()
}
#[test]
fn help() {
    let o = cli(&["help"]);
    assert!(o.status.success());
    assert!(String::from_utf8(o.stdout).unwrap().contains("LOCAL"));
}
#[test]
fn bad_args() {
    for args in [vec![], vec!["wat"], vec!["run"], vec!["help", "extra"]] {
        assert!(!cli(&args).status.success());
    }
}
#[test]
fn sample_limit() {
    for samples in ["0", "9", "-1", "abc", "99999999999999999999"] {
        assert!(!cli(&["run", "unused", samples]).status.success());
    }
}
#[test]
fn missing_node() {
    assert!(
        !cli(&["run", "Z:/missing-giggles-node.exe", "1"])
            .status
            .success()
    );
}
#[test]
fn malformed_report() {
    let root = giggles_faultlab::process::fresh_directory("cli").unwrap();
    let path = root.join("bad.json");
    giggles_faultlab::process::write_new(&path, b"{\"summary\":\"success\"}").unwrap();
    assert!(!cli(&["check", path.to_str().unwrap()]).status.success());
}
#[test]
fn absent_report() {
    assert!(!cli(&["check", "Z:/missing-report.json"]).status.success());
}
#[test]
fn successful_real_run() {
    let node = std::env::var_os("GIGGLES_NODE_EXE")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../target-faultlab/debug/giggles-node.exe")
        });
    let output = cli(&["run", node.to_str().unwrap(), "1"]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let path = String::from_utf8(output.stdout).unwrap();
    eprintln!("CLI report: {}", path.trim());
    assert!(cli(&["check", path.trim()]).status.success());
    let mut report = giggles_faultlab::lab::check(std::path::Path::new(path.trim())).unwrap();
    report.summary.committed_responses += 1;
    let forged = std::path::Path::new(path.trim()).with_file_name("forged-report.json");
    giggles_faultlab::process::write_new(&forged, &serde_json::to_vec(&report).unwrap()).unwrap();
    assert!(!cli(&["check", forged.to_str().unwrap()]).status.success());
}
