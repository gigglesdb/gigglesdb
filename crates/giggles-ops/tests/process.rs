mod common;
use common::*;
use giggles_core::{Admin, Limits};
use giggles_ops::*;
use std::{
    path::Path,
    process::{Command, Output},
};
fn cli(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_giggles-ops"))
        .args(args)
        .output()
        .unwrap()
}
fn json(o: &Output) -> serde_json::Value {
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    serde_json::from_slice(&o.stdout).unwrap()
}
fn s(p: &Path) -> &str {
    p.to_str().unwrap()
}
#[test]
fn subprocess_cli_lifecycle_and_no_clobber() {
    let root = fresh("cli-root");
    let source = fresh("cli-source").join("db");
    let a = Admin::create(&source, Limits::default()).unwrap();
    a.sql_session()
        .execute("CREATE TABLE data (id BIGINT PRIMARY KEY)", &[], None)
        .unwrap();
    a.sql_session()
        .execute("INSERT INTO data VALUES(17)", &[], None)
        .unwrap();
    drop(a);
    let backup = [
        "backup",
        "--root",
        s(&root),
        "--source",
        s(&source),
        "--id",
        "cli-1",
    ];
    assert_eq!(json(&cli(&backup))["state"], "complete");
    assert!(!cli(&backup).status.success());
    assert_eq!(
        json(&cli(&["inspect", "--root", s(&root), "--id", "cli-1"]))["backup_id"],
        "cli-1"
    );
    assert_eq!(
        json(&cli(&["verify", "--root", s(&root), "--id", "cli-1"]))["manifest"]["state"],
        "complete"
    );
    assert_eq!(
        json(&cli(&["list", "--root", s(&root)]))
            .as_array()
            .unwrap()
            .len(),
        2
    );
    let target = fresh("cli-restore").join("db");
    let args = [
        "restore",
        "--root",
        s(&root),
        "--id",
        "cli-1",
        "--target",
        s(&target),
    ];
    assert_eq!(json(&cli(&args))["reopened"], true);
    assert!(!cli(&args).status.success());
    let r = Admin::open(target, Limits::default()).unwrap();
    assert_eq!(
        r.sql_session()
            .execute("SELECT * FROM data LIMIT 10", &[], None)
            .unwrap()
            .rows
            .len(),
        1
    );
}
#[test]
fn cli_invalid_arguments_and_sanitized_errors() {
    for args in [
        vec![],
        vec!["help"],
        vec!["list"],
        vec!["list", "--root", "private-secret-123"],
        vec!["verify", "--root", "x", "--id", "../x"],
        vec!["list", "--root", "x", "--root", "y"],
        vec!["backup", "--root", "x", "--id", "a", "--what", "y"],
    ] {
        let o = cli(&args);
        assert!(!o.status.success());
        assert!(o.stdout.is_empty());
        let v: serde_json::Value = serde_json::from_slice(&o.stderr).unwrap();
        assert!(v["error"].is_string());
        assert!(!String::from_utf8_lossy(&o.stderr).contains("private-secret"));
    }
}
#[test]
fn process_backup_crash_matrix() {
    for point in ["pre-export", "post-snapshot", "pre-publish", "post-publish"] {
        let root = fresh("crash-catalog");
        let source = fresh("crash-source").join("db");
        drop(Admin::create(&source, Limits::default()).unwrap());
        let o = Command::new(env!("CARGO_BIN_EXE_ops-fault-driver"))
            .args(["backup", point, s(&root), "crash-1", s(&source)])
            .output()
            .unwrap();
        assert!(!o.status.success());
        assert_ne!(
            o.status.code(),
            Some(101),
            "fault must not be a setup panic: {o:?}"
        );
        let c = Catalog::open(&root, OpsLimits::default()).unwrap();
        let id = BackupId::new("crash-1").unwrap();
        assert_eq!(
            c.verify(&id).is_ok(),
            point == "post-publish",
            "{point}: {:?}",
            o
        );
        let entries = c.list().unwrap();
        assert!(!entries.is_empty());
        if point == "post-publish" {
            assert!(matches!(
                c.backup(&Admin::open(&source, Limits::default()).unwrap(), &id),
                Err(OpsError::Exists)
            ));
        }
    }
}
#[test]
fn process_restore_crash_matrix() {
    for point in [
        "pre-restore",
        "created",
        "staged",
        "committed",
        "post-install",
    ] {
        let (root, c, a) = fixture();
        let _protected = protected(&a);
        let original = a.snapshot().unwrap();
        c.backup(&a, &id()).unwrap();
        let target = fresh("crash-restore").join("db");
        let o = Command::new(env!("CARGO_BIN_EXE_ops-fault-driver"))
            .args(["restore", point, s(&root), id().as_str(), s(&target)])
            .output()
            .unwrap();
        assert!(!o.status.success());
        assert_ne!(
            o.status.code(),
            Some(101),
            "fault must not be a setup panic: {o:?}"
        );
        if matches!(point, "created" | "staged" | "committed") {
            assert_eq!(o.status.code(), Some(86), "core crash seam: {point}");
        }
        assert_eq!(target.exists(), point != "pre-restore");
        assert!(!target.join("ops-restore-report.json").exists());
        let reopened = Admin::open(&target, Limits::default());
        assert_eq!(
            reopened.is_ok(),
            matches!(point, "committed" | "post-install"),
            "{point}"
        );
        if let Ok(r) = reopened {
            let restored = r.snapshot().unwrap();
            assert_ne!(restored.domain, original.domain);
            assert_eq!(restored.operations, original.operations);
            assert!(restored.tasks.values().all(|t| t.revoked));
        }
        if target.exists() {
            assert!(matches!(c.restore(&id(), &target), Err(OpsError::Exists)));
        }
        c.verify(&id()).unwrap();
    }
}
