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

#[cfg(feature = "engine-rocksdb")]
mod v2_single_writer {
    use giggles_core::{Admin, CoreError, Engine, Limits};
    use std::{
        fs,
        path::{Path, PathBuf},
        process::{Child, Command, Stdio},
        thread,
        time::{Duration, Instant},
    };

    struct TestDir(PathBuf);
    impl TestDir {
        fn new() -> Self {
            let mut nonce = [0; 16];
            getrandom::fill(&mut nonce).unwrap();
            let path =
                std::env::temp_dir().join(format!("v2-writer-{:032x}", u128::from_be_bytes(nonce)));
            fs::create_dir(&path).unwrap();
            Self(path)
        }
        fn db(&self) -> PathBuf {
            self.0.join("db")
        }
        fn ready(&self) -> PathBuf {
            self.0.join("ready")
        }
    }
    impl Drop for TestDir {
        fn drop(&mut self) {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }

    fn assert_writer_rejected(result: Result<Admin, CoreError>) {
        match result {
            Err(CoreError::Storage(message)) => {
                assert_eq!(message, "database already open by another writer");
            }
            Err(error) => panic!("wrong error kind: {error:?}"),
            Ok(_) => panic!("second writer opened the database"),
        }
    }

    fn create(path: &Path) -> Admin {
        Admin::create_engine(path, Limits::default(), Engine::V2).unwrap()
    }

    fn command(mode: &str, dir: &TestDir) -> Command {
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "v2_single_writer::subprocess_worker",
                "--nocapture",
            ])
            .env("GIGGLES_V2_WRITER_MODE", mode)
            .env("GIGGLES_V2_WRITER_PATH", dir.db())
            .env("GIGGLES_V2_WRITER_READY", dir.ready());
        command
    }

    #[test]
    fn subprocess_worker() {
        let Ok(mode) = std::env::var("GIGGLES_V2_WRITER_MODE") else {
            return;
        };
        let path = PathBuf::from(std::env::var_os("GIGGLES_V2_WRITER_PATH").unwrap());
        if mode == "second" {
            assert_writer_rejected(Admin::open(&path, Limits::default()));
            assert_writer_rejected(Admin::create_engine(&path, Limits::default(), Engine::V2));
            return;
        }
        assert_eq!(mode, "hold");
        let _admin = Admin::open(&path, Limits::default()).unwrap();
        fs::write(
            std::env::var_os("GIGGLES_V2_WRITER_READY").unwrap(),
            b"ready",
        )
        .unwrap();
        loop {
            thread::park();
        }
    }

    #[test]
    fn same_process_second_open_rejected_and_drop_releases_lock() {
        let dir = TestDir::new();
        let admin = create(&dir.db());
        assert_writer_rejected(Admin::open(dir.db(), Limits::default()));
        assert_writer_rejected(Admin::create_engine(
            dir.db(),
            Limits::default(),
            Engine::V2,
        ));
        drop(admin);
        let identity = fs::read_to_string(dir.db().join("giggles.lock")).unwrap();
        assert!(identity.contains(&format!("pid={}\n", std::process::id())));
        let lines: Vec<_> = identity.lines().collect();
        assert_eq!(lines.len(), 3);
        assert!(
            lines[1]
                .strip_prefix("started_unix_nanos=")
                .unwrap()
                .parse::<u128>()
                .unwrap()
                > 0
        );
        assert_eq!(lines[2].strip_prefix("nonce=").unwrap().len(), 32);
        drop(Admin::open(dir.db(), Limits::default()).unwrap());
        assert_ne!(
            fs::read_to_string(dir.db().join("giggles.lock")).unwrap(),
            identity
        );
    }

    #[test]
    fn independent_process_rejects_second_writer() {
        let dir = TestDir::new();
        let _admin = create(&dir.db());
        let output = command("second", &dir).output().unwrap();
        assert!(output.status.success(), "{output:?}");
    }

    struct KillOnDrop(Child);
    impl Drop for KillOnDrop {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    #[test]
    fn killed_writer_releases_lock() {
        let dir = TestDir::new();
        drop(create(&dir.db()));
        let mut child = KillOnDrop(command("hold", &dir).stdout(Stdio::null()).spawn().unwrap());
        let deadline = Instant::now() + Duration::from_secs(30);
        while !dir.ready().exists() {
            assert!(
                child.0.try_wait().unwrap().is_none(),
                "child exited before acquiring lock"
            );
            assert!(Instant::now() < deadline, "child did not acquire lock");
            thread::sleep(Duration::from_millis(10));
        }
        assert_writer_rejected(Admin::open(dir.db(), Limits::default()));
        child.0.kill().unwrap();
        assert!(!child.0.wait().unwrap().success());
        drop(Admin::open(dir.db(), Limits::default()).unwrap());
    }

    #[test]
    fn stale_lock_file_does_not_block_create_or_open() {
        let dir = TestDir::new();
        fs::create_dir(dir.db()).unwrap();
        let lock = dir.db().join("giggles.lock");
        // Even a stale record naming this live process must not deny access.
        let stale = format!(
            "pid={}\nstarted_unix_nanos=1\nnonce=stale\n",
            std::process::id()
        );
        fs::write(&lock, &stale).unwrap();
        drop(create(&dir.db()));
        fs::write(&lock, &stale).unwrap();
        drop(Admin::open(dir.db(), Limits::default()).unwrap());
        assert_ne!(fs::read_to_string(lock).unwrap(), stale);
    }
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
