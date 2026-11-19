mod common;
use common::*;
use giggles_core::*;
use std::process::Command;
#[test]
fn ordinary_second_writer_child() {
    let Ok(path) = std::env::var("GIGGLES_ORDINARY_SECOND_PATH") else { return; };
    assert!(Admin::open(path, Limits::default()).is_err());
}
#[test]
fn ordinary_independent_process_rejects_second_writer() {
    let p = fresh("ordinary-second-writer");
    let _a = Admin::create(&p, Limits::default()).unwrap();
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "ordinary_second_writer_child", "--nocapture"])
        .env("GIGGLES_ORDINARY_SECOND_PATH", &p).output().unwrap();
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

fn ordinary_row_database(n: u64) -> (std::path::PathBuf, Admin) {
    let path = fresh("ordinary-row-database");
    let admin = Admin::create(&path, Limits::default()).unwrap();
    for i in 0..n { admin.put(key(i), row()).unwrap(); }
    (path, admin)
}

#[test]
fn ordinary_reject_existing_missing_corrupt_and_second_open() {
    let (path, a) = ordinary_row_database(0);
    assert!(Admin::create(&path, Limits::default()).is_err());
    assert!(Admin::open(&path, Limits::default()).is_err());
    assert!(Admin::open(fresh("absent"), Limits::default()).is_err());
    let bad = fresh("bad");
    std::fs::create_dir(&bad).unwrap();
    std::fs::write(bad.join("data.redb"), b"corrupt").unwrap();
    assert!(Admin::open(&bad, Limits::default()).is_err());
    assert_eq!(std::fs::read(bad.join("data.redb")).unwrap(), b"corrupt");
    drop(a);
}

#[test]
fn ordinary_process_lock_follows_hardlink_alias() {
    let (p, _a) = ordinary_row_database(0);
    let alias = fresh("hardlink-alias");
    std::fs::create_dir(&alias).unwrap();
    std::fs::hard_link(p.join("data.redb"), alias.join("data.redb")).unwrap();
    assert!(Admin::open(alias, Limits::default()).is_err());
}

#[test]
fn ordinary_corrupt_engine_header_is_rejected_without_replacement() {
    use std::io::{Seek, Write};
    let (p, a) = ordinary_row_database(1);
    drop(a);
    let dest = fresh("corrupt-engine");
    std::fs::create_dir(&dest).unwrap();
    std::fs::copy(p.join("data.redb"), dest.join("data.redb")).unwrap();
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .open(dest.join("data.redb"))
        .unwrap();
    f.seek(std::io::SeekFrom::Start(0)).unwrap();
    f.write_all(&[0xff; 512]).unwrap();
    f.sync_all().unwrap();
    drop(f);
    assert!(Admin::open(&dest, Limits::default()).is_err());
    assert_eq!(
        &std::fs::read(dest.join("data.redb")).unwrap()[..512],
        &[0xff; 512]
    );
    assert!(Admin::open(p, Limits::default()).is_ok());
}

mod ordinary_cli {
use super::common::fresh;
use serde_json::{Value, json};
#[test]
fn ordinary_sql_cli_batches_typed_rows_and_error_rollback() {
    let mut c = Cli::new();
    c.create();
    c.admin(json!({"command":"sql_batch","statements":[{"sql":"BEGIN"},{"sql":"CREATE TABLE cli_rows (id BIGINT PRIMARY KEY, name TEXT)"},{"sql":"INSERT INTO cli_rows VALUES($1,$2)","params":[{"Int":-5},{"Text":"雪"}]},{"sql":"COMMIT"}]}),0,0);
    let r = c.admin(
        json!({"command":"sql","sql":"SELECT * FROM cli_rows LIMIT 5","params":[]}),
        0,
        0,
    );
    assert_eq!(r["rows"][0]["values"], json!([{"Int":-5},{"Text":"雪"}]));
    c.admin(json!({"command":"sql_batch","statements":[{"sql":"BEGIN"},{"sql":"DELETE FROM cli_rows WHERE id=-5"},{"sql":"INSERT INTO cli_rows VALUES(NULL,'bad')"},{"sql":"COMMIT"}]}),3,207);
    let r = c.admin(
        json!({"command":"sql","sql":"SELECT id FROM cli_rows LIMIT 5","params":[]}),
        0,
        0,
    );
    assert_eq!(r["rows"][0]["values"], json!([{"Int":-5}]));
    c.admin(json!({"command":"sql_batch","statements":[{"sql":"BEGIN"},{"sql":"DELETE FROM cli_rows WHERE id=-5"}]}),3,214);
    let r = c.admin(
        json!({"command":"sql","sql":"SELECT id FROM cli_rows LIMIT 5","params":[]}),
        0,
        0,
    );
    assert_eq!(r["rows"].as_array().unwrap().len(), 1);
}
struct Cli {
    root: std::path::PathBuf,
    db: std::path::PathBuf,
    seq: usize,
}
impl Cli {
    fn new() -> Self {
        let root = fresh("workflow");
        std::fs::create_dir(&root).unwrap();
        let db = root.join("db");
        Self { root, db, seq: 0 }
    }
    fn call(&mut self, args: Vec<String>, exit: i32, code: u32) -> Value {
        self.seq += 1;
        let out = std::process::Command::new(env!("CARGO_BIN_EXE_giggles"))
            .args(&args)
            .output()
            .unwrap();
        let base = self.root.join(format!("{:03}", self.seq));
        std::fs::write(base.with_extension("stdout"), &out.stdout).unwrap();
        std::fs::write(base.with_extension("stderr"), &out.stderr).unwrap();
        std::fs::write(
            base.with_extension("command"),
            format!(
                "{} {args:?}\nstatus={}\n",
                env!("CARGO_BIN_EXE_giggles"),
                out.status
            ),
        )
        .unwrap();
        assert_eq!(out.status.code(), Some(exit), "{out:?}");
        let v: Value = serde_json::from_slice(&out.stdout).unwrap();
        assert_eq!(v["version"], 1);
        assert_eq!(v["code"], code);
        v["data"].clone()
    }
    fn create(&mut self) {
        self.call(
            vec![
                "create".into(),
                "--engine".into(),
                "legacy".into(),
                self.db.display().to_string(),
            ],
            0,
            0,
        );
    }
    fn admin(&mut self, v: Value, exit: i32, code: u32) -> Value {
        self.call(
            vec!["admin".into(), self.db.display().to_string(), v.to_string()],
            exit,
            code,
        )
    }
}
}

#[test]
fn ordinary_cli_export_verify_restore() {
    let p = fresh("cli");
    let run = |args: &[&std::ffi::OsStr]| {
        let out = std::process::Command::new(env!("CARGO_BIN_EXE_giggles"))
            .args(args)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        out
    };
    run(&["create".as_ref(), "--engine".as_ref(), "legacy".as_ref(), p.as_os_str()]);
    let file = fresh("cli-export");
    run(&["export".as_ref(), p.as_os_str(), file.as_os_str()]);
    run(&["verify".as_ref(), file.as_os_str()]);
    let target = fresh("cli-restore");
    run(&["restore".as_ref(), file.as_os_str(), target.as_os_str()]);
}
