mod common;
use common::*;
use giggles_core::*;
use std::{io::Write, process::Command};
#[test]
fn subprocess_worker() {
    let Ok(mode) = std::env::var("GIGGLES_CHILD_MODE") else {
        return;
    };
    let p = std::env::var("GIGGLES_CHILD_PATH").unwrap();
    if mode == "second" {
        assert!(Admin::open(p, Limits::default()).is_err());
        return;
    }
    let a = Admin::open(&p, Limits::default()).unwrap();
    let e = a.executor(TaskId(1), Principal(2)).unwrap();
    let point = match mode.as_str() {
        "before" => CrashPoint::BeforeCommit,
        "after" => CrashPoint::AfterCommit,
        "ack" => CrashPoint::BeforeAck,
        "acknowledged" => CrashPoint::None,
        _ => panic!("bad mode"),
    };
    a.set_crash_point(point).unwrap();
    let d = e
        .execute(request(2, &[1]), Time::Certified(11), Some(ApprovalId(0)))
        .unwrap()
        .unwrap();
    let mut ack = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(std::path::Path::new(&p).join("ack.txt"))
        .unwrap();
    writeln!(ack, "{d:?}").unwrap();
    ack.sync_all().unwrap();
    std::process::exit(86);
}
fn child(mode: &str, path: &std::path::Path) -> std::process::Output {
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "subprocess_worker", "--nocapture"])
        .env("GIGGLES_CHILD_MODE", mode)
        .env("GIGGLES_CHILD_PATH", path)
        .output()
        .unwrap();
    std::fs::write(
        path.join(format!("child-{mode}.stdout.txt")),
        &output.stdout,
    )
    .unwrap();
    std::fs::write(
        path.join(format!("child-{mode}.stderr.txt")),
        &output.stderr,
    )
    .unwrap();
    std::fs::write(
        path.join(format!("child-{mode}.command.txt")),
        format!(
            "{} --exact subprocess_worker --nocapture\nmode={mode}\npath={}\nstatus={}\n",
            std::env::current_exe().unwrap().display(),
            path.display(),
            output.status
        ),
    )
    .unwrap();
    output
}
#[test]
fn independent_process_rejects_second_writer() {
    let (p, _a, _e) = fixture(1);
    let output = child("second", &p);
    assert!(output.status.success(), "{:?}", output);
}
#[test]
fn subprocess_abrupt_termination_at_all_commit_boundaries() {
    for mode in ["before", "after", "ack", "acknowledged"] {
        let (path, a, e) = fixture(2);
        let acknowledged = e
            .execute(request(1, &[0]), Time::Certified(10), None)
            .unwrap()
            .unwrap();
        let preview = e.preview(&request(2, &[1]), Time::Certified(10)).unwrap();
        assert_eq!(
            a.approve(Principal(3), preview, Time::Certified(10))
                .unwrap(),
            ApprovalId(0)
        );
        drop(e);
        drop(a);
        let out = child(mode, &path);
        assert_eq!(out.status.code(), Some(86), "{out:?}");
        let a = Admin::open(&path, Limits::default()).unwrap();
        let e = a.executor(TaskId(1), Principal(2)).unwrap();
        assert_eq!(
            e.status(OperationId(1)).unwrap(),
            OperationStatus::Decided(Ok(acknowledged))
        );
        let s = a.snapshot().unwrap();
        let committed = mode != "before";
        assert_eq!(s.approvals[&ApprovalId(0)].consumed, committed);
        assert_eq!(
            a.authority_metadata().unwrap().1,
            if committed { 11 } else { 10 }
        );
        assert_eq!(s.operations.len(), if committed { 2 } else { 1 });
        assert_eq!(
            s.tasks[&TaskId(1)].transitions,
            if committed { 2 } else { 1 }
        );
        assert_eq!(
            s.tasks[&TaskId(1)].touched.len(),
            if committed { 2 } else { 1 }
        );
        assert_eq!(
            s.rows[&key(1)].status,
            if committed {
                Status::Queued
            } else {
                Status::Failed
            }
        );
        let status = e.status(OperationId(2)).unwrap();
        assert_eq!(matches!(status, OperationStatus::Decided(Ok(_))), committed);
        if mode == "acknowledged" {
            assert!(path.join("ack.txt").exists());
        }
        // Stable retry commits once on either side of the crash boundary.
        assert!(
            e.execute(request(2, &[1]), Time::Certified(11), Some(ApprovalId(0)))
                .unwrap()
                .is_ok()
        );
        assert_eq!(a.snapshot().unwrap().tasks[&TaskId(1)].transitions, 2);
    }
}
