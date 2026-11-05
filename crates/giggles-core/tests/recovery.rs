mod common;
use common::*;
use giggles_core::*;
use std::{fs, process::Command};
#[test]
fn all_export_failures_retain_stage_and_never_publish() {
    for point in [
        RecoveryPoint::ExportWrite,
        RecoveryPoint::ExportSync,
        RecoveryPoint::ExportVerify,
        RecoveryPoint::ExportPublish,
    ] {
        let (p, a, e) = fixture(1);
        let before = a.snapshot().unwrap();
        let dest = p.join("out.export");
        assert!(a.export_with_fault(&dest, point).is_err());
        assert!(!dest.exists());
        let stages = fs::read_dir(&p)
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.to_string_lossy().contains(".staging-"))
            .collect::<Vec<_>>();
        assert_eq!(stages.len(), 1);
        assert_eq!(
            Admin::verify_export(&stages[0]).is_ok(),
            point != RecoveryPoint::ExportWrite
        );
        assert_eq!(before, a.snapshot().unwrap());
        assert_eq!(e.status(OperationId(1)).unwrap(), OperationStatus::Absent);
        a.export(&dest).unwrap();
        assert_eq!(before, Admin::verify_export(&dest).unwrap());
        assert!(stages[0].exists());
    }
}
#[test]
fn no_clobber_existing_files_directories_and_missing_parent() {
    let (p, a, _) = fixture(1);
    let file = p.join("prior");
    fs::write(&file, b"sentinel").unwrap();
    assert!(a.export(&file).is_err());
    assert_eq!(fs::read(&file).unwrap(), b"sentinel");
    let dir = p.join("prior-dir");
    fs::create_dir(&dir).unwrap();
    assert!(a.export(&dir).is_err());
    assert!(dir.is_dir());
    let absent = p.join("missing-parent").join("out");
    assert!(a.export(absent).is_err());
    assert!(a.snapshot().is_ok());
}
#[test]
fn publication_race_has_one_verified_winner() {
    let (p, a, _) = fixture(2);
    let out = p.join("raced");
    let threads = (0..8)
        .map(|_| {
            let a = a.clone();
            let out = out.clone();
            std::thread::spawn(move || a.export(out))
        })
        .collect::<Vec<_>>();
    let results = threads
        .into_iter()
        .map(|t| t.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    assert_eq!(Admin::verify_export(out).unwrap(), a.snapshot().unwrap());
    assert_eq!(
        fs::read_dir(p)
            .unwrap()
            .filter(|e| e
                .as_ref()
                .unwrap()
                .file_name()
                .to_string_lossy()
                .contains(".staging-"))
            .count(),
        8
    );
}
fn assert_restored(a: &Admin, original: &Snapshot) {
    let s = a.snapshot().unwrap();
    assert_ne!(s.domain, original.domain);
    assert_eq!(s.operations, original.operations);
    assert_eq!(s.rows, original.rows);
    assert!(s.tasks.values().all(|t| t.revoked));
    assert!(s.approvals.values().all(|p| p.revoked));
    assert_eq!(a.authority_metadata().unwrap().3, Some(original.domain));
    let e = a.executor(TaskId(1), Principal(2)).unwrap();
    assert_eq!(
        e.status(OperationId(1)).unwrap(),
        OperationStatus::Decided(
            original.operations[&(TaskId(1), OperationId(1))]
                .decision
                .clone()
        )
    );
    assert_eq!(
        e.execute(request(9, &[1]), Time::Certified(11), None)
            .unwrap(),
        Err(Error::Revoked)
    );
}
fn source() -> (std::path::PathBuf, Admin, Snapshot) {
    let (p, a, e) = fixture(2);
    e.execute(request(1, &[0]), Time::Certified(10), None)
        .unwrap()
        .unwrap();
    let preview = e.preview(&request(2, &[1]), Time::Certified(10)).unwrap();
    a.approve(Principal(3), preview, Time::Certified(10))
        .unwrap();
    let s = a.snapshot().unwrap();
    let file = p.join("export");
    a.export(&file).unwrap();
    (file, a, s)
}
#[test]
fn returned_install_errors_leave_invalid_partial_or_complete_revoked_state() {
    for point in [
        RecoveryPoint::InstallCreated,
        RecoveryPoint::InstallStaged,
        RecoveryPoint::InstallCommitted,
    ] {
        let (file, source, s) = source();
        let dest = fresh("install-fault");
        assert!(Admin::restore_with_fault(&file, &dest, Limits::default(), point).is_err());
        assert!(dest.join("data.redb").exists());
        if point == RecoveryPoint::InstallCommitted {
            assert_restored(&Admin::open(&dest, Limits::default()).unwrap(), &s);
        } else {
            assert!(Admin::open(&dest, Limits::default()).is_err());
        }
        assert!(Admin::restore(&file, &dest, Limits::default()).is_err());
        assert_eq!(source.snapshot().unwrap(), s);
        assert_restored(
            &Admin::restore(file, fresh("install-retry"), Limits::default()).unwrap(),
            &s,
        );
    }
}
#[test]
fn restore_crash_worker() {
    let Ok(mode) = std::env::var("GD_RESTORE_MODE") else {
        return;
    };
    let point = match mode.as_str() {
        "created" => RecoveryPoint::CrashInstallCreated,
        "staged" => RecoveryPoint::CrashInstallStaged,
        "committed" => RecoveryPoint::CrashInstallCommitted,
        _ => panic!("bad mode"),
    };
    Admin::restore_with_fault(
        std::env::var("GD_EXPORT").unwrap(),
        std::env::var("GD_DEST").unwrap(),
        Limits::default(),
        point,
    )
    .unwrap();
    panic!("crash seam did not terminate");
}
#[test]
fn subprocess_install_crashes_never_open_valid_empty_database() {
    for mode in ["created", "staged", "committed"] {
        let (file, a, s) = source();
        let dest = fresh("crash-restore");
        let out = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "restore_crash_worker", "--nocapture"])
            .env("GD_RESTORE_MODE", mode)
            .env("GD_EXPORT", &file)
            .env("GD_DEST", &dest)
            .output()
            .unwrap();
        fs::write(dest.join("child.stdout"), &out.stdout).unwrap();
        fs::write(dest.join("child.stderr"), &out.stderr).unwrap();
        fs::write(dest.join("child.command"),format!("exe={} --exact restore_crash_worker --nocapture\nmode={mode}\nexport={}\ndestination={}\nstatus={}\n",std::env::current_exe().unwrap().display(),file.display(),dest.display(),out.status)).unwrap();
        assert_eq!(out.status.code(), Some(86), "{out:?}");
        if mode == "committed" {
            assert_restored(&Admin::open(&dest, Limits::default()).unwrap(), &s);
        } else {
            assert!(Admin::open(&dest, Limits::default()).is_err());
        }
        assert!(Admin::restore(&file, &dest, Limits::default()).is_err());
        assert_eq!(a.snapshot().unwrap(), s);
    }
}
