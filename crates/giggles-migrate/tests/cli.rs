use std::{
    path::{Path, PathBuf},
    process::{Command, Output},
};
fn fresh() -> PathBuf {
    let p = std::env::temp_dir().join(format!(
        "giggles-migrate-cli-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&p).unwrap();
    p
}
fn fixture(name: &str) -> String {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("examples")
        .join(name)
        .to_string_lossy()
        .into_owned()
}
fn run(root: &Path, label: &str, args: &[&str]) -> Output {
    let out = Command::new(env!("CARGO_BIN_EXE_giggles-migrate"))
        .args(args)
        .output()
        .unwrap();
    std::fs::write(root.join(format!("{label}.stdout")), &out.stdout).unwrap();
    std::fs::write(root.join(format!("{label}.stderr")), &out.stderr).unwrap();
    std::fs::write(
        root.join(format!("{label}.command")),
        format!("{args:?}\nstatus={}\n", out.status),
    )
    .unwrap();
    out
}
#[test]
fn end_to_end_fixture() {
    let root = fresh();
    let db = root.join("db").to_string_lossy().into_owned();
    let m = fixture("manifest.json");
    let c = fixture("chunks.json");
    for (label, args) in [
        ("validate", vec!["manifest", "validate", &m]),
        ("reconcile", vec!["reconcile", &m, &c]),
        ("import", vec!["import", &m, &c, &db]),
        ("retry", vec!["import", &m, &c, &db]),
        ("verify", vec!["verify", &m, &c, &db]),
        ("status", vec!["status", &m, &db]),
    ] {
        let out = run(&root, label, &args);
        assert!(out.status.success(), "{label}: {out:?}");
        if label == "reconcile" {
            let rows: Vec<giggles_migrate::MaterializedRow> =
                serde_json::from_slice(&out.stdout).unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(
                rows[0].cells["body"].value,
                giggles_migrate::Scalar::Text("edited".into())
            );
            assert_eq!(
                rows[0].cells["attachment"].value,
                giggles_migrate::Scalar::Bytes(vec![])
            );
        }
        if label == "status" {
            let status: giggles_migrate::Status = serde_json::from_slice(&out.stdout).unwrap();
            assert_eq!(status.phase, "canary-ready");
            assert_eq!(status.chunks, 2);
            assert!(!status.source_completeness_proven);
        }
    }
}
#[test]
fn cli_rejects_invalid_manifest_and_corruption() {
    let root = fresh();
    let m = fixture("manifest.json");
    let c = fixture("chunks.json");
    let db = root.join("db").to_string_lossy().into_owned();
    assert!(
        run(&root, "import", &["import", &m, &c, &db])
            .status
            .success()
    );
    let mut chunks: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&c).unwrap()).unwrap();
    chunks[0]["events"][1]["mutation"]["value"]["value"] = "changed".into();
    let bad = root.join("changed.json").to_string_lossy().into_owned();
    std::fs::write(&bad, serde_json::to_vec(&chunks).unwrap()).unwrap();
    assert!(
        !run(&root, "mismatch", &["import", &m, &bad, &db])
            .status
            .success()
    );
    assert!(
        !run(&root, "verify-mismatch", &["verify", &m, &bad, &db])
            .status
            .success()
    );
    let mut manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&m).unwrap()).unwrap();
    manifest["unsupported_features"] = serde_json::json!(["lwt"]);
    let invalid = root.join("invalid.json").to_string_lossy().into_owned();
    std::fs::write(&invalid, serde_json::to_vec(&manifest).unwrap()).unwrap();
    assert!(
        !run(&root, "invalid", &["manifest", "validate", &invalid])
            .status
            .success()
    );
    assert!(
        !run(&root, "authority", &["cutover", &m, &db])
            .status
            .success()
    );
}
#[test]
fn cli_bounds_and_unknown_fields() {
    let root = fresh();
    let giant = root.join("giant.json").to_string_lossy().into_owned();
    std::fs::write(&giant, vec![b' '; 49153]).unwrap();
    let out = run(&root, "giant", &["manifest", "validate", &giant]);
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("byte bound"));
    assert!(!run(&root, "missing", &[]).status.success());
}
