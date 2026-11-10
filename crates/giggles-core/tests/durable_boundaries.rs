//! Legacy and v2 durable SQL commit and logical recovery boundaries.
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
        RecoveryPoint::CrashExportWrite
        | RecoveryPoint::CrashExportSync
        | RecoveryPoint::CrashExportVerify
        | RecoveryPoint::CrashExportPublish => "export-crash",
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
        RecoveryPoint::CrashExportWrite,
        RecoveryPoint::CrashExportSync,
        RecoveryPoint::CrashExportVerify,
        RecoveryPoint::CrashExportPublish,
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
            if recovery_step(point).starts_with("export-") {
                let result = a.export_with_fault(&export, point);
                assert_eq!(recovery_step(point), "export-error", "crash seam returned");
                assert!(result.is_err());
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
            } else if recovery_step(point) == "export-crash" {
                let stages: Vec<_> = fs::read_dir(&path)
                    .unwrap()
                    .map(|entry| entry.unwrap().path())
                    .filter(|p| p.to_string_lossy().contains("source.export.staging-"))
                    .collect();
                assert_eq!(stages.len(), 1);
                if point == RecoveryPoint::CrashExportWrite {
                    assert!(Admin::verify_export(&stages[0]).is_err());
                } else {
                    assert_eq!(Admin::verify_export(&stages[0]).unwrap(), snapshot);
                }
                if point == RecoveryPoint::CrashExportPublish {
                    assert_eq!(Admin::verify_export(&export).unwrap(), snapshot);
                } else {
                    assert!(!export.exists());
                }
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
                Boundary::Recovery(RecoveryPoint::CrashExportWrite | RecoveryPoint::CrashExportSync | RecoveryPoint::CrashExportVerify | RecoveryPoint::CrashExportPublish | RecoveryPoint::CrashInstallCreated | RecoveryPoint::CrashInstallStaged | RecoveryPoint::CrashInstallCommitted) => "existing-abrupt-crash-seam",
                Boundary::Commit(CrashPoint::None, _) | Boundary::Recovery(RecoveryPoint::None) => "exit-after-ack-control",
                _ => "exit-after-returned-error",
            }, "step": match boundary { Boundary::Commit(_, true) => "sql-commit", Boundary::Commit(_, false) => "task-receipt", Boundary::Recovery(p) => recovery_step(p) },
            "assertion_counts": {"acknowledged_results": acknowledgements.len(), "atomicity": 6,
                "verify": 1, "writer_exclusion": 1, "recovery": install_checks}}));
    }
    let report_path = std::path::PathBuf::from(std::env::var_os("TEMP").unwrap())
        .join("durable_boundaries_report.json");
    fs::write(
        &report_path,
        serde_json::to_vec_pretty(&json!({"boundaries": report,
        "missing_fault_points": []}))
        .unwrap(),
    )
    .unwrap();
    println!(
        "legacy durable boundaries: {} passed; report={}",
        report.len(),
        report_path.display()
    );
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn v2_recovery_worker() {
    let Ok(mode) = std::env::var("GD_V2_RECOVERY_MODE") else {
        return;
    };
    let root = std::path::PathBuf::from(std::env::var_os("GD_V2_RECOVERY_PATH").unwrap());
    let Boundary::Recovery(point) = registry()[mode.parse::<usize>().unwrap()] else {
        panic!("recovery only");
    };
    let a = Admin::open(root.join("db"), Limits::default()).unwrap();
    let mut sql = a.sql_session();
    sql.execute(
        "CREATE TABLE boundary_rows (id BIGINT PRIMARY KEY, body TEXT)",
        &[],
        None,
    )
    .unwrap();
    sql.execute(
        "INSERT INTO boundary_rows VALUES (1,'one'),(2,'two')",
        &[],
        None,
    )
    .unwrap();
    let export = root.join("source.export");
    if recovery_step(point).starts_with("export-") {
        assert!(a.export_with_fault(export, point).is_err());
        assert_eq!(recovery_step(point), "export-error", "crash seam returned");
    } else {
        a.export(&export).unwrap();
        let result =
            Admin::restore_with_fault(export, root.join("installed"), Limits::default(), point);
        if point == RecoveryPoint::None {
            assert_eq!(rows(&result.unwrap()), rows(&a));
        } else {
            assert!(result.is_err());
        }
    }
    std::process::exit(86);
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn every_v2_recovery_boundary() {
    let mut count = 0;
    for (index, boundary) in registry().into_iter().enumerate() {
        let Boundary::Recovery(point) = boundary else {
            continue;
        };
        let root = fresh("v2-recovery-boundary");
        fs::create_dir(&root).unwrap();
        drop(Admin::create_engine(root.join("db"), Limits::default(), Engine::V2).unwrap());
        let output = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "v2_recovery_worker", "--nocapture"])
            .env("GD_V2_RECOVERY_MODE", index.to_string())
            .env("GD_V2_RECOVERY_PATH", &root)
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(86), "{point:?}: {output:?}");
        let a = Admin::open(root.join("db"), Limits::default()).unwrap();
        assert_eq!(rows(&a).len(), 2, "source changed: {point:?}");
        let export = root.join("source.export");
        if recovery_step(point).starts_with("export-") {
            let stages: Vec<_> = fs::read_dir(&root)
                .unwrap()
                .map(|e| e.unwrap().path())
                .filter(|p| p.to_string_lossy().contains("source.export.staging-"))
                .collect();
            assert_eq!(stages.len(), 1);
            assert_eq!(
                Admin::verify_export(&stages[0]).is_ok(),
                !matches!(
                    point,
                    RecoveryPoint::ExportWrite | RecoveryPoint::CrashExportWrite
                )
            );
            assert_eq!(export.exists(), point == RecoveryPoint::CrashExportPublish);
            if export.exists() {
                let restored =
                    Admin::restore(&export, root.join("checked"), Limits::default()).unwrap();
                assert_eq!(rows(&restored), rows(&a));
            }
        } else {
            Admin::verify_export(export).unwrap();
            let installed = Admin::open(root.join("installed"), Limits::default());
            if matches!(
                point,
                RecoveryPoint::None
                    | RecoveryPoint::InstallCommitted
                    | RecoveryPoint::CrashInstallCommitted
            ) {
                assert_eq!(rows(&installed.unwrap()), rows(&a));
            } else {
                assert!(installed.is_err(), "incomplete install opened: {point:?}");
            }
        }
        drop(a);
        fs::remove_dir_all(root).unwrap();
        count += 1;
    }
    assert_eq!(count, 15);
    assert_eq!(count + 2 * v2_commit_points().len(), 27);
    println!("v2 recovery boundaries: {count} passed");
}

#[cfg(feature = "engine-rocksdb")]
fn v2_commit_points() -> Vec<CrashPoint> {
    registry()
        .into_iter()
        .filter_map(|boundary| match boundary {
            Boundary::Commit(point, true) => match point {
                CrashPoint::None
                | CrashPoint::BeforeCommit
                | CrashPoint::AfterCommit
                | CrashPoint::BeforeAck
                | CrashPoint::ErrorBeforeCommit
                | CrashPoint::ErrorAfterCommit => Some(point),
                // v2 does not implement kernel decisions; this is not a SQL seam.
                CrashPoint::ErrorAfterDecisionCommit => None,
            },
            _ => None,
        })
        .collect()
}

#[cfg(feature = "engine-rocksdb")]
fn v2_execute(sql: &mut SqlSession, statement: &str) -> Result<relational::SqlResult, CoreError> {
    // Sequence evaluation uses the same clocked entry point as the server.
    sql.execute_at(
        statement,
        &[],
        None,
        &relational::StatementClock {
            unavailable: None,
            transaction_us: 0,
            statement_us: 0,
            zone: None,
            wall: None,
        },
    )
}

#[cfg(feature = "engine-rocksdb")]
fn v2_query(sql: &mut SqlSession, query: &str) -> Vec<Vec<Value>> {
    v2_execute(sql, query)
        .unwrap_or_else(|error| panic!("{query}: {error:?}"))
        .rows
        .into_iter()
        .map(|row| row.values)
        .collect()
}

#[cfg(feature = "engine-rocksdb")]
fn v2_parent(sql: &mut SqlSession) -> Vec<Vec<Value>> {
    v2_query(sql, "SELECT id,tag,code,qty FROM cb_parent ORDER BY id")
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn v2_commit_worker() {
    let Ok(mode) = std::env::var("GD_V2_COMMIT_MODE") else {
        return;
    };
    let root = std::path::PathBuf::from(std::env::var_os("GD_V2_COMMIT_PATH").unwrap());
    let mode = mode.parse::<usize>().unwrap();
    let point = v2_commit_points()[mode / 2];
    let ddl = mode % 2 == 1;
    let a = Admin::open(root.join("db"), Limits::default()).unwrap();
    let mut sql = a.sql_session();
    for statement in [
        "CREATE SEQUENCE cb_ids",
        "CREATE TABLE cb_parent (id BIGINT PRIMARY KEY DEFAULT nextval('cb_ids'), tag BIGINT NOT NULL, code TEXT NOT NULL, qty BIGINT CHECK (qty > 0))",
        "CREATE INDEX cb_tag ON cb_parent(tag)",
        "CREATE UNIQUE INDEX cb_code ON cb_parent(code)",
        "CREATE TABLE cb_child (id BIGINT PRIMARY KEY, parent_id BIGINT REFERENCES cb_parent(id))",
        "BEGIN",
        "INSERT INTO cb_parent(tag,code,qty) VALUES (10,'one',1),(20,'two',2),(30,'three',3)",
        "INSERT INTO cb_child VALUES (1,1)",
        "COMMIT",
    ] {
        v2_execute(&mut sql, statement).unwrap_or_else(|error| panic!("{statement}: {error:?}"));
    }
    ack(&root, "base", format!("{:?}", v2_parent(&mut sql)));
    v2_execute(
        &mut sql,
        "INSERT INTO cb_parent(tag,code,qty) VALUES (40,'four',4)",
    )
    .unwrap();
    ack(&root, "stable", format!("{:?}", v2_parent(&mut sql)));
    if !ddl {
        for statement in [
            "BEGIN",
            "INSERT INTO cb_parent(tag,code,qty) VALUES (50,'five',5)",
            "INSERT INTO cb_child VALUES (2,5)",
            "UPDATE cb_parent SET tag=25,code='changed' WHERE id=2",
            "DELETE FROM cb_parent WHERE id=3",
        ] {
            v2_execute(&mut sql, statement)
                .unwrap_or_else(|error| panic!("{statement}: {error:?}"));
        }
        ack(&root, "sequence", "5".into()); // default id returned/used before COMMIT
    }
    let seen = v2_query(&mut sql, "SELECT nextval('cb_ids')");
    let Value::Int(seen) = seen[0][0] else {
        panic!("sequence value")
    };
    ack(&root, "sequence", seen.to_string());
    a.set_crash_point(point).unwrap();
    let result = v2_execute(
        &mut sql,
        if ddl {
            "CREATE INDEX cb_qty ON cb_parent(qty)"
        } else {
            "COMMIT"
        },
    );
    match point {
        CrashPoint::None => {
            result.unwrap();
            ack(&root, "final", format!("{:?}", v2_parent(&mut sql)));
        }
        CrashPoint::ErrorBeforeCommit | CrashPoint::ErrorAfterCommit => {
            assert_eq!(
                result.unwrap_err(),
                CoreError::Storage(
                    if point == CrashPoint::ErrorBeforeCommit {
                        "injected before commit"
                    } else {
                        "injected lost commit response"
                    }
                    .into()
                )
            );
        }
        _ => panic!("abrupt commit seam returned: {point:?}: {result:?}"),
    }
    std::process::exit(86);
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn every_v2_commit_boundary() {
    use relational::SqlError;
    let mut report = Vec::new();
    assert_eq!(v2_commit_points().len(), 6);
    for (index, point) in v2_commit_points().into_iter().enumerate() {
        for ddl in [false, true] {
            let root = fresh("v2-commit-boundary");
            fs::create_dir(&root).unwrap();
            drop(Admin::create_engine(root.join("db"), Limits::default(), Engine::V2).unwrap());
            let output = Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "v2_commit_worker", "--nocapture"])
                .env(
                    "GD_V2_COMMIT_MODE",
                    (index * 2 + usize::from(ddl)).to_string(),
                )
                .env("GD_V2_COMMIT_PATH", &root)
                .output()
                .unwrap();
            assert_eq!(
                output.status.code(),
                Some(86),
                "{point:?}, ddl={ddl}: {output:?}"
            );
            let acks: Vec<Json> = fs::read_to_string(root.join("acks.jsonl"))
                .unwrap()
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect();
            // Admin::open -> meta::open_checks runs engine/content_check.rs.
            let a = Admin::open(root.join("db"), Limits::default()).unwrap();
            let mut sql = a.sql_session();
            let row = |id, tag, code: &str, qty| {
                vec![
                    Value::Int(id),
                    Value::Int(tag),
                    Value::Text(code.into()),
                    Value::Int(qty),
                ]
            };
            let baseline = vec![
                row(1, 10, "one", 1),
                row(2, 20, "two", 2),
                row(3, 30, "three", 3),
                row(4, 40, "four", 4),
            ];
            assert_eq!(acks[0]["kind"], "base");
            assert_eq!(acks[0]["value"], format!("{:?}", &baseline[..3]));
            assert_eq!(acks[1]["kind"], "stable");
            assert_eq!(acks[1]["value"], format!("{baseline:?}"));
            let committed = !matches!(
                point,
                CrashPoint::BeforeCommit | CrashPoint::ErrorBeforeCommit
            );
            let expected = if committed && !ddl {
                vec![
                    baseline[0].clone(),
                    row(2, 25, "changed", 2),
                    baseline[3].clone(),
                    row(5, 50, "five", 5),
                ]
            } else {
                baseline.clone()
            };
            let actual = v2_parent(&mut sql);
            assert_eq!(
                actual, expected,
                "partial transaction: {point:?}, ddl={ddl}"
            );
            // Both acknowledged commits' untouched rows survive, including lost replies.
            for stable in [&baseline[0], &baseline[3]] {
                assert!(actual.contains(stable));
            }
            let mut children = vec![vec![Value::Int(1), Value::Int(1)]];
            if committed && !ddl {
                children.push(vec![Value::Int(2), Value::Int(5)]);
            }
            assert_eq!(
                v2_query(&mut sql, "SELECT id,parent_id FROM cb_child ORDER BY id"),
                children
            );
            let final_ack = acks.iter().find(|ack| ack["kind"] == "final");
            assert_eq!(final_ack.is_some(), point == CrashPoint::None);
            if let Some(ack) = final_ack {
                assert_eq!(ack["value"], format!("{actual:?}"));
            }
            // Check new catalog publication before creating a missing control index.
            if ddl {
                let probe = v2_execute(&mut sql, "CREATE INDEX cb_qty ON cb_parent(qty)");
                if committed {
                    assert_eq!(probe.unwrap_err(), CoreError::Sql(SqlError::Duplicate));
                } else {
                    probe.unwrap();
                }
            }
            // The unfiltered query uses the heap; tag/code equality uses their
            // indexes (qty too for DDL). Include old/deleted keys to catch ghosts.
            for (column, position) in [("tag", 1), ("code", 2), ("qty", 3)] {
                for value in baseline.iter().chain(&actual).map(|row| &row[position]) {
                    let literal = match value {
                        Value::Int(n) => n.to_string(),
                        Value::Text(s) => format!("'{s}'"),
                        _ => unreachable!(),
                    };
                    let indexed = v2_query(
                        &mut sql,
                        &format!(
                            "SELECT id,tag,code,qty FROM cb_parent WHERE {column}={literal} ORDER BY id"
                        ),
                    );
                    let full: Vec<_> = actual
                        .iter()
                        .filter(|row| &row[position] == value)
                        .cloned()
                        .collect();
                    assert_eq!(
                        indexed, full,
                        "index/heap mismatch: {point:?}, ddl={ddl}, {column}={literal}"
                    );
                }
            }
            for (statement, error) in [
                (
                    "INSERT INTO cb_parent VALUES (99,99,'one',1)",
                    SqlError::Duplicate,
                ),
                (
                    "INSERT INTO cb_parent VALUES (99,99,'new',0)",
                    SqlError::CheckViolation,
                ),
                (
                    "INSERT INTO cb_child VALUES (99,99)",
                    SqlError::ForeignKeyViolation,
                ),
                (
                    "DELETE FROM cb_parent WHERE id=1",
                    SqlError::ForeignKeyViolation,
                ),
            ] {
                assert_eq!(
                    v2_execute(&mut sql, statement).unwrap_err(),
                    CoreError::Sql(error),
                    "{statement}"
                );
            }
            assert!(
                actual
                    .iter()
                    .all(|row| matches!(row[3], Value::Int(n) if n > 0))
            );
            for child in &children {
                assert!(actual.iter().any(|row| row[0] == child[1]));
            }
            let mut maximum = acks
                .iter()
                .filter(|ack| ack["kind"] == "sequence")
                .map(|ack| ack["value"].as_str().unwrap().parse::<i64>().unwrap())
                .max()
                .unwrap();
            assert_eq!(maximum, if ddl { 5 } else { 6 });
            for _ in 0..2 {
                let next = v2_query(&mut sql, "SELECT nextval('cb_ids')");
                let Value::Int(next) = next[0][0] else {
                    panic!("sequence value")
                };
                assert!(
                    next > maximum,
                    "sequence reused {next} after {maximum}: {point:?}"
                );
                maximum = next;
            }
            assert_eq!(
                v2_parent(&mut sql),
                actual,
                "failed constraint probe changed data"
            );
            assert_eq!(
                v2_query(&mut sql, "SELECT id,parent_id FROM cb_child ORDER BY id"),
                children
            );
            report.push(json!({"boundary": commit_name(point), "engine": "v2",
                "step": if ddl { "autocommit-create-index" } else { "sql-commit" },
                "outcome": "pass", "committed": committed, "acknowledged": final_ack.is_some(),
                "termination": match point {
                    CrashPoint::None => "exit-after-ack-control",
                    CrashPoint::ErrorBeforeCommit | CrashPoint::ErrorAfterCommit => "exit-after-returned-error",
                    _ => "existing-abrupt-crash-seam",
                }, "checks": ["atomicity", "earlier-acks", "index-heap", "unique", "fk", "check", "sequence", "open-content-check"]}));
            drop(sql);
            drop(a);
            fs::remove_dir_all(root).unwrap();
        }
    }
    assert_eq!(report.len(), 12);
    let report_path = std::path::PathBuf::from(std::env::var_os("TEMP").unwrap())
        .join("v2_commit_boundaries_report.json");
    fs::write(&report_path, serde_json::to_vec_pretty(&json!({"boundaries": report,
        "registered_v2_boundaries": 27, "missing_fault_points": [],
        "not_applicable": [{"boundary": "ErrorAfterDecisionCommit", "reason": "v2 has no kernel decisions"}]
    })).unwrap()).unwrap();
    println!(
        "v2 commit boundaries: {} passed; report={}",
        report.len(),
        report_path.display()
    );
}
