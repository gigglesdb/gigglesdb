//! Legacy durable-boundary registry. A future engine adapter can reuse the protocol.
mod common;
use common::*;
use giggles_core::{relational::Value, *};
use serde_json::{Value as Json, json};
use std::{fs, io::Write, path::Path, process::Command};

#[derive(Clone, Copy, Debug)]
enum Boundary {
    Commit(CrashPoint, bool), // bool: SQL transaction rather than task receipt
    Recovery(RecoveryPoint),
}

// Exhaustive matches deliberately force registry review when either enum grows.
fn commit_name(point: CrashPoint) -> &'static str {
    match point {
        CrashPoint::None => "None",
        CrashPoint::BeforeCommit => "BeforeCommit",
        CrashPoint::AfterCommit => "AfterCommit",
        CrashPoint::BeforeAck => "BeforeAck",
        CrashPoint::ErrorBeforeCommit => "ErrorBeforeCommit",
        CrashPoint::ErrorAfterCommit => "ErrorAfterCommit",
        CrashPoint::ErrorAfterDecisionCommit => "ErrorAfterDecisionCommit",
    }
}
fn recovery_step(point: RecoveryPoint) -> &'static str {
    match point {
        RecoveryPoint::None => "install-control",
        RecoveryPoint::ExportWrite
        | RecoveryPoint::ExportSync
        | RecoveryPoint::ExportVerify
        | RecoveryPoint::ExportPublish => "export-error",
        RecoveryPoint::InstallCreated
        | RecoveryPoint::InstallStaged
        | RecoveryPoint::InstallCommitted => "install-error",
        RecoveryPoint::CrashInstallCreated
        | RecoveryPoint::CrashInstallStaged
        | RecoveryPoint::CrashInstallCommitted => "install-crash",
    }
}
fn registry() -> Vec<Boundary> {
    let mut all = Vec::new();
    for point in [
        CrashPoint::None,
        CrashPoint::BeforeCommit,
        CrashPoint::AfterCommit,
        CrashPoint::BeforeAck,
        CrashPoint::ErrorBeforeCommit,
        CrashPoint::ErrorAfterCommit,
        CrashPoint::ErrorAfterDecisionCommit,
    ] {
        commit_name(point);
        all.push(Boundary::Commit(point, false));
        all.push(Boundary::Commit(point, true));
    }
    for point in [
        RecoveryPoint::None,
        RecoveryPoint::ExportWrite,
        RecoveryPoint::ExportSync,
        RecoveryPoint::ExportVerify,
        RecoveryPoint::ExportPublish,
        RecoveryPoint::InstallCreated,
        RecoveryPoint::InstallStaged,
        RecoveryPoint::InstallCommitted,
        RecoveryPoint::CrashInstallCreated,
        RecoveryPoint::CrashInstallStaged,
        RecoveryPoint::CrashInstallCommitted,
    ] {
        recovery_step(point);
        all.push(Boundary::Recovery(point));
    }
    all
}
fn ack(path: &Path, kind: &str, value: String) {
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path.join("acks.jsonl"))
        .unwrap();
    writeln!(file, "{}", json!({"kind": kind, "value": value})).unwrap();
    file.sync_all().unwrap();
}
fn rows(a: &Admin) -> Vec<Vec<Value>> {
    a.sql_session()
        .execute("SELECT id, body FROM boundary_rows ORDER BY id", &[], None)
        .unwrap()
        .rows
        .into_iter()
        .map(|r| r.values)
        .collect()
}
fn receipt(a: &Admin, op: u64) -> OperationStatus {
    a.executor(TaskId(1), Principal(2))
        .unwrap()
        .status(OperationId(op))
        .unwrap()
}
fn child(path: &Path, mode: &str) -> std::process::Output {
    Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "boundary_worker", "--nocapture"])
        .env("GD_DURABLE_PATH", path)
        .env("GD_DURABLE_MODE", mode)
        .output()
        .unwrap()
}
#[test]
fn boundary_worker() {
    let Ok(mode) = std::env::var("GD_DURABLE_MODE") else {
        return;
    };
    let path = std::path::PathBuf::from(std::env::var_os("GD_DURABLE_PATH").unwrap());
    if mode == "lock" {
        assert!(Admin::open(&path, Limits::default()).is_err());
        return;
    }
    let boundary = registry()[mode.parse::<usize>().unwrap()];
    let a = Admin::open(&path, Limits::default()).unwrap();
    a.sql_session()
        .execute(
            "CREATE TABLE boundary_rows (id BIGINT PRIMARY KEY, body TEXT)",
            &[],
            None,
        )
        .unwrap();
    ack(&path, "ddl", format!("{:?}", a.catalog().unwrap().1));
    let mut session = a.sql_session();
    session.execute("BEGIN", &[], None).unwrap();
    session
        .execute(
            "INSERT INTO boundary_rows VALUES (1,'one'),(2,'two')",
            &[],
            None,
        )
        .unwrap();
    session.execute("COMMIT", &[], None).unwrap();
    ack(&path, "sql", format!("{:?}", rows(&a)));
    let e = a.executor(TaskId(1), Principal(2)).unwrap();
    let decision = e
        .execute(request(1, &[0]), Time::Certified(10), None)
        .unwrap()
        .unwrap();
    ack(
        &path,
        "receipt",
        format!("{:?}", OperationStatus::Decided(Ok(decision))),
    );
    match boundary {
        Boundary::Commit(point, is_sql) => {
            if is_sql {
                session.execute("BEGIN", &[], None).unwrap();
                session
                    .execute(
                        "INSERT INTO boundary_rows VALUES (3,'three'),(4,'four')",
                        &[],
                        None,
                    )
                    .unwrap();
            }
            a.set_crash_point(point).unwrap();
            if is_sql {
                let result = session.execute("COMMIT", &[], None);
                if point == CrashPoint::None {
                    result.unwrap();
                    ack(&path, "final-sql", format!("{:?}", rows(&a)));
                } else {
                    assert!(result.is_err());
                }
            } else {
                let result = e.execute(request(2, &[1, 2]), Time::Certified(11), None);
                if point == CrashPoint::None {
                    let decision = result.unwrap().unwrap();
                    ack(
                        &path,
                        "final-receipt",
                        format!("{:?}", OperationStatus::Decided(Ok(decision))),
                    );
                } else {
                    assert!(result.is_err());
                }
            }
        }
        Boundary::Recovery(point) => {
            let export = path.join("source.export");
            if recovery_step(point) == "export-error" {
                assert!(a.export_with_fault(&export, point).is_err());
            } else {
                a.export(&export).unwrap();
                ack(
                    &path,
                    "export",
                    format!("{:?}", Admin::verify_export(&export).unwrap()),
                );
                let result = Admin::restore_with_fault(
                    &export,
                    path.join("installed"),
                    Limits::default(),
                    point,
                );
                if point == RecoveryPoint::None {
                    let installed = result.unwrap();
                    ack(
                        &path,
                        "install",
                        format!("{:?}", installed.snapshot().unwrap()),
                    );
                } else {
                    assert!(result.is_err());
                }
            }
        }
    }
    // Returned-error seams and controls have no abrupt seam: exit without destructors.
    std::process::exit(86);
}

#[test]
fn every_legacy_durable_boundary() {
    let mut report = Vec::new();
    for (index, boundary) in registry().into_iter().enumerate() {
        let (path, a, e) = fixture(3);
        drop(e);
        drop(a);
        let output = child(&path, &index.to_string());
        fs::write(path.join("child.stdout"), &output.stdout).unwrap();
        fs::write(path.join("child.stderr"), &output.stderr).unwrap();
        assert_eq!(output.status.code(), Some(86), "{boundary:?}: {output:?}");
        let a = Admin::open(&path, Limits::default()).unwrap();
        let acknowledgements: Vec<Json> = fs::read_to_string(path.join("acks.jsonl"))
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        let expected_receipt = acknowledgements
            .iter()
            .find(|v| v["kind"] == "receipt")
            .unwrap();
        assert_eq!(expected_receipt["value"], format!("{:?}", receipt(&a, 1)));
        assert_eq!(a.snapshot().unwrap().rows[&key(0)].status, Status::Queued);
        assert_eq!(
            acknowledgements
                .iter()
                .find(|v| v["kind"] == "ddl")
                .unwrap()["value"],
            format!("{:?}", a.catalog().unwrap().1)
        );
        let actual = rows(&a);
        let base = vec![
            vec![Value::Int(1), Value::Text("one".into())],
            vec![Value::Int(2), Value::Text("two".into())],
        ];
        assert_eq!(&actual[..2], base.as_slice());
        assert_eq!(
            acknowledgements
                .iter()
                .find(|v| v["kind"] == "sql")
                .unwrap()["value"],
            format!("{base:?}")
        );
        let committed = match boundary {
            Boundary::Commit(point, _) => !matches!(
                point,
                CrashPoint::BeforeCommit | CrashPoint::ErrorBeforeCommit
            ),
            _ => false,
        };
        let sql_commit = matches!(boundary, Boundary::Commit(_, true)) && committed;
        let task_commit = matches!(boundary, Boundary::Commit(_, false)) && committed;
        let mut expected = base;
        if sql_commit {
            expected.extend([
                vec![Value::Int(3), Value::Text("three".into())],
                vec![Value::Int(4), Value::Text("four".into())],
            ]);
        }
        assert_eq!(actual, expected, "{boundary:?}: partial SQL transaction");
        if let Some(ack) = acknowledgements.iter().find(|v| v["kind"] == "final-sql") {
            assert_eq!(ack["value"], format!("{actual:?}"));
        }
        let snapshot = a.snapshot().unwrap();
        for id in [1, 2] {
            assert_eq!(
                snapshot.rows[&key(id)].status,
                if task_commit {
                    Status::Queued
                } else {
                    Status::Failed
                }
            );
        }
        assert_eq!(
            matches!(receipt(&a, 2), OperationStatus::Decided(Ok(_))),
            task_commit
        );
        assert_eq!(snapshot.operations.len(), if task_commit { 2 } else { 1 });
        if let Some(ack) = acknowledgements
            .iter()
            .find(|v| v["kind"] == "final-receipt")
        {
            assert_eq!(ack["value"], format!("{:?}", receipt(&a, 2)));
        }
        // Export's existing verifier validates the recovered logical state.
        let proof = path.join("verified.export");
        a.export(&proof).unwrap();
        assert_eq!(Admin::verify_export(proof).unwrap(), snapshot);
        assert!(child(&path, "lock").status.success());
        let mut install_checks = 0;
        if let Boundary::Recovery(point) = boundary {
            let export = path.join("source.export");
            if recovery_step(point) == "export-error" {
                assert!(!export.exists());
                let stages: Vec<_> = fs::read_dir(&path)
                    .unwrap()
                    .map(|e| e.unwrap().path())
                    .filter(|p| p.to_string_lossy().contains("source.export.staging-"))
                    .collect();
                assert_eq!(stages.len(), 1);
                assert_eq!(
                    Admin::verify_export(&stages[0]).is_ok(),
                    point != RecoveryPoint::ExportWrite
                );
                install_checks = 3;
            } else {
                assert_eq!(Admin::verify_export(&export).unwrap(), snapshot);
                assert_eq!(
                    acknowledgements
                        .iter()
                        .find(|v| v["kind"] == "export")
                        .unwrap()["value"],
                    format!("{snapshot:?}")
                );
                let result = Admin::open(path.join("installed"), Limits::default());
                if matches!(
                    point,
                    RecoveryPoint::None
                        | RecoveryPoint::InstallCommitted
                        | RecoveryPoint::CrashInstallCommitted
                ) {
                    let restored = result.unwrap();
                    let restored_snapshot = restored.snapshot().unwrap();
                    assert_eq!(restored_snapshot.rows, snapshot.rows);
                    assert_eq!(restored_snapshot.operations, snapshot.operations);
                    assert_eq!(rows(&restored), actual);
                    assert!(restored_snapshot.tasks.values().all(|t| t.revoked));
                    if let Some(ack) = acknowledgements.iter().find(|v| v["kind"] == "install") {
                        assert_eq!(ack["value"], format!("{restored_snapshot:?}"));
                    }
                    assert!(child(&path.join("installed"), "lock").status.success());
                    let verify = path.join("restored.export");
                    restored.export(&verify).unwrap();
                    assert_eq!(Admin::verify_export(verify).unwrap(), restored_snapshot);
                    install_checks = if point == RecoveryPoint::None { 9 } else { 8 };
                } else {
                    // Incomplete installs must reject open, never expose valid empty state.
                    assert!(result.is_err());
                    install_checks = 2;
                }
            }
        }
        report.push(json!({"boundary": format!("{boundary:?}"), "engine": "legacy",
            "outcome": "pass", "termination": match boundary {
                Boundary::Commit(CrashPoint::BeforeCommit | CrashPoint::AfterCommit | CrashPoint::BeforeAck, _) |
                Boundary::Recovery(RecoveryPoint::CrashInstallCreated | RecoveryPoint::CrashInstallStaged | RecoveryPoint::CrashInstallCommitted) => "existing-abrupt-crash-seam",
                Boundary::Commit(CrashPoint::None, _) | Boundary::Recovery(RecoveryPoint::None) => "exit-after-ack-control",
                _ => "exit-after-returned-error",
            }, "step": match boundary { Boundary::Commit(_, true) => "sql-commit", Boundary::Commit(_, false) => "task-receipt", Boundary::Recovery(p) => recovery_step(p) },
            "assertion_counts": {"acknowledged_results": acknowledgements.len(), "atomicity": 6,
                "verify": 1, "writer_exclusion": 1, "recovery": install_checks}}));
    }
    let report_path = std::path::PathBuf::from(std::env::var_os("TEMP").unwrap())
        .join("durable_boundaries_report.json");
    fs::write(&report_path, serde_json::to_vec_pretty(&json!({"boundaries": report,
        "missing_fault_points": ["ExportWrite/Sync/Verify/Publish have returned-error seams only; abrupt export crashes need new seams"]})).unwrap()).unwrap();
    println!(
        "legacy durable boundaries: {} passed; report={}",
        report.len(),
        report_path.display()
    );
}
