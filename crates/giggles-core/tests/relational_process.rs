mod common;
use common::*;
use giggles_core::{relational::Value as V, *};
use std::{io::Write, process::Command};
#[test]
fn sql_crash_worker() {
    let Ok(mode) = std::env::var("GD_SQL_CHILD") else {
        return;
    };
    let path = std::env::var("GD_SQL_PATH").unwrap();
    let a = Admin::open(&path, Limits::default()).unwrap();
    let mut s = a.sql_session();
    s.execute("BEGIN", &[], None).unwrap();
    s.execute(
        "CREATE TABLE crash_rows (tenant BIGINT, id BIGINT, body TEXT, PRIMARY KEY(tenant,id))",
        &[],
        None,
    )
    .unwrap();
    s.execute("INSERT INTO crash_rows VALUES(42,-1,'first')", &[], None)
        .unwrap();
    s.execute("INSERT INTO crash_rows VALUES(42,2,'second')", &[], None)
        .unwrap();
    if mode == "staged" {
        std::process::exit(86);
    }
    a.set_crash_point(match mode.as_str() {
        "before" => CrashPoint::BeforeCommit,
        "after" => CrashPoint::AfterCommit,
        "ack" => CrashPoint::BeforeAck,
        "acknowledged" => CrashPoint::None,
        _ => panic!("mode"),
    })
    .unwrap();
    s.execute("COMMIT", &[], None).unwrap();
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(std::path::Path::new(&path).join("sql-ack"))
        .unwrap();
    writeln!(f, "committed catalog and two rows").unwrap();
    f.sync_all().unwrap();
    std::process::exit(86);
}
#[test]
fn catalog_rows_and_versions_atomic_at_abrupt_process_boundaries() {
    for mode in ["staged", "before", "after", "ack", "acknowledged"] {
        let path = fresh("sql-process");
        let a = Admin::create(&path, Limits::default()).unwrap();
        a.put(key(0), row()).unwrap();
        drop(a);
        let exe = std::env::current_exe().unwrap();
        let out = Command::new(&exe)
            .args(["--exact", "sql_crash_worker", "--nocapture"])
            .env("GD_SQL_CHILD", mode)
            .env("GD_SQL_PATH", &path)
            .output()
            .unwrap();
        std::fs::write(path.join("child.stdout"), &out.stdout).unwrap();
        std::fs::write(path.join("child.stderr"), &out.stderr).unwrap();
        std::fs::write(path.join("child.command"),format!("{} --exact sql_crash_worker --nocapture\nGD_SQL_CHILD={mode}\nGD_SQL_PATH={}\nstatus={}\n",exe.display(),path.display(),out.status)).unwrap();
        assert_eq!(out.status.code(), Some(86), "{out:?}");
        let a = Admin::open(&path, Limits::default()).unwrap();
        assert_eq!(a.snapshot().unwrap().rows[&key(0)].status, Status::Failed);
        let committed = ["after", "ack", "acknowledged"].contains(&mode);
        assert_eq!(a.catalog().unwrap().1.len(), usize::from(committed));
        if committed {
            let mut s = a.sql_session();
            let r = s
                .execute(
                    "SELECT id,body FROM crash_rows WHERE tenant=42 ORDER BY id LIMIT 5",
                    &[],
                    None,
                )
                .unwrap();
            assert_eq!(
                r.rows.iter().map(|r| r.values.clone()).collect::<Vec<_>>(),
                vec![
                    vec![V::Int(-1), V::Text("first".into())],
                    vec![V::Int(2), V::Text("second".into())]
                ]
            );
            assert!(r.rows[1].version > r.rows[0].version);
        }
        assert_eq!(path.join("sql-ack").exists(), mode == "acknowledged");
    }
}
