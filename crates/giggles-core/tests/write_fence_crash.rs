#![cfg(feature = "engine-rocksdb")]
use giggles_core::{Admin, CoreError, Engine, Limits};

#[test]
fn fence_crash_worker() {
    let Some(path) = std::env::var_os("GD_WRITE_FENCE_CRASH_PATH") else {
        return;
    };
    let a = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
    a.sql_session()
        .execute("CREATE TABLE items(id BIGINT PRIMARY KEY)", &[], None)
        .unwrap();
    assert_eq!(a.install_write_fence(b"holder", "crash").unwrap(), 1);
    // A marker distinguishes the intended abort from an unrelated startup failure.
    let marker = std::path::PathBuf::from(&path).join("fence-installed");
    std::fs::File::create(marker).unwrap().sync_all().unwrap();
    std::process::abort();
}
#[test]
fn fence_survives_abort_and_restart() {
    let mut nonce = [0; 16];
    getrandom::fill(&mut nonce).unwrap();
    let path =
        std::env::temp_dir().join(format!("fence-crash-{:032x}", u128::from_be_bytes(nonce)));
    let child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "fence_crash_worker", "--nocapture"])
        .env("GD_WRITE_FENCE_CRASH_PATH", &path)
        .output()
        .unwrap();
    assert!(!child.status.success(), "child must abort");
    assert!(
        path.join("fence-installed").exists(),
        "{}",
        String::from_utf8_lossy(&child.stderr)
    );
    let a = Admin::open(&path, Limits::default()).unwrap();
    let status = a.write_fence_status().unwrap().unwrap();
    assert!(status.fenced);
    assert_eq!((status.epoch, status.label.as_str()), (1, "crash"));
    let error = a
        .sql_session()
        .execute("INSERT INTO items VALUES (1)", &[], None)
        .unwrap_err();
    assert_eq!(error, CoreError::WriteFenced { epoch: 1 });
    assert_eq!(giggles_core::sqlstate(&error), "25006");
    assert!(
        a.sql_session()
            .execute("SELECT * FROM items", &[], None)
            .unwrap()
            .rows
            .is_empty()
    );
    drop(a);
    let a = Admin::open_as_fence_holder(&path, Limits::default(), b"holder").unwrap();
    a.sql_session()
        .execute("INSERT INTO items VALUES (1)", &[], None)
        .unwrap();
    drop(a);
    std::fs::remove_dir_all(path).unwrap();
}
