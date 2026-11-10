//! Abrupt process termination, including interruptions of the recovery command.
use giggles_core::{
    relational::{SqlResult, Value},
    roles::{NewRole, TablePrivilege},
    *,
};
use serde_json::{Value as Json, json};
use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
    process::Command,
};

fn run(s: &mut SqlSession, sql: &str) -> SqlResult {
    let clock = relational::StatementClock {
        unavailable: None,
        transaction_us: 1,
        statement_us: 1,
        zone: None,
        wall: None,
    };
    s.execute_at(sql, &[], None, &clock)
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
}
fn fixture(path: &Path) {
    let a = Admin::create(path, Limits::default()).unwrap();
    let mut s = a.sql_session();
    run(&mut s, "CREATE TABLE heap (n BIGINT)");
    run(&mut s, "INSERT INTO heap VALUES (1), (2), (3)");
    run(&mut s, "DELETE FROM heap WHERE n = 3");
    {
        for sql in [
            "CREATE SCHEMA app",
            "CREATE TYPE app.mood AS ENUM ('happy', 'sad')",
            "CREATE SEQUENCE ids START 10",
            "SELECT setval('ids', 800)",
            "CREATE TABLE app.parent (id BIGINT PRIMARY KEY, mood app.mood)",
            "CREATE TABLE child (id BIGINT PRIMARY KEY, pid BIGINT REFERENCES app.parent(id), code TEXT, n BIGINT CHECK (n >= 0))",
            "CREATE INDEX child_pid ON child(pid)",
            "CREATE UNIQUE INDEX child_code ON child(code)",
        ] {
            run(&mut s, sql);
        }
        let parents = (1..=120)
            .map(|i| format!("({i}, 'happy')"))
            .collect::<Vec<_>>()
            .join(",");
        let children = (1..=120)
            .map(|i| format!("({i}, {i}, 'c{i}', {i})"))
            .collect::<Vec<_>>()
            .join(",");
        let heaps = (10..=130)
            .map(|i| format!("({i})"))
            .collect::<Vec<_>>()
            .join(",");
        run(&mut s, &format!("INSERT INTO app.parent VALUES {parents}"));
        run(&mut s, &format!("INSERT INTO child VALUES {children}"));
        run(&mut s, &format!("INSERT INTO heap VALUES {heaps}"));
        run(&mut s, "UPDATE child SET n = 999 WHERE id = 12");
        run(&mut s, "DELETE FROM child WHERE id = 120");
        run(&mut s, "DELETE FROM app.parent WHERE id = 120");
        let roles = a.roles();
        for name in ["owner", "reader"] {
            roles
                .create(NewRole {
                    name: name.into(),
                    login: true,
                    ..Default::default()
                })
                .unwrap();
        }
        roles.grant_membership("reader", "owner").unwrap();
        roles.set_table_owner("child", "owner").unwrap();
        roles
            .grant_table("child", "reader", &[TablePrivilege::Select])
            .unwrap();
    }
}

fn image(a: &Admin) -> Json {
    let mut s = a.sql_session();
    let schemas = s.schema_metadata().unwrap();
    let types = s.user_type_catalog().unwrap();
    json!({
        "domain": a.authority_metadata().unwrap().0.to_string(),
        "tables": format!("{:?}", a.catalog().unwrap().1),
        "schemas": format!("{:?}", (schemas.schemas, schemas.tables)),
        "types": format!("{:?}", (types.types, types.column_types)),
        "roles": format!("{:?}", a.roles().list().unwrap()),
        "membership": format!("{:?}", a.roles().memberships("owner").unwrap()),
        "owner": s.table_owner("child").unwrap(),
        "grants": format!("{:?}", s.table_privileges("child", "reader").unwrap()),
        "rows": (["SELECT * FROM app.parent ORDER BY id", "SELECT * FROM child ORDER BY id",
            "SELECT n FROM heap ORDER BY n"].map(|q| format!("{:?}", run(&mut s, q)))),
    })
}
fn copy_dir(from: &Path, to: &Path) {
    fs::create_dir(to).unwrap();
    for entry in fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &to.join(entry.file_name()));
        } else {
            fs::copy(entry.path(), to.join(entry.file_name())).unwrap();
        }
    }
}
fn check(path: &Path, expected: &Json) {
    let a = Admin::open(path, Limits::default()).unwrap();
    assert_eq!(
        &image(&a),
        expected,
        "FINDING: content changed at {}",
        path.display()
    );
    drop(a);
    // Probe nextval on a disposable copy: checking must not advance the original.
    let probe = path.with_file_name("sequence-probe");
    copy_dir(path, &probe);
    let a = Admin::open(&probe, Limits::default()).unwrap();
    assert_eq!(
        run(&mut a.sql_session(), "SELECT nextval('ids')").rows[0].values,
        vec![Value::Int(801)],
        "FINDING: sequence changed"
    );
    drop(a);
    fs::remove_dir_all(probe).unwrap();
}
fn post_upgrade(path: &Path, expected: &Json) {
    check(path, expected);
    let a = Admin::open(path, Limits::default()).unwrap();
    let mut s = a.sql_session();
    run(&mut s, "INSERT INTO heap VALUES (500)");
    run(&mut s, "INSERT INTO child VALUES (999, 1, 'new', 0)");
    assert!(matches!(
        s.execute("INSERT INTO child VALUES (998, 1, 'new', 0)", &[], None),
        Err(CoreError::Sql(relational::SqlError::Duplicate))
    ));
    assert!(matches!(
        s.execute("INSERT INTO child VALUES (998, 9999, 'fk', 0)", &[], None),
        Err(CoreError::Sql(relational::SqlError::ForeignKeyViolation))
    ));
    assert!(
        s.execute("INSERT INTO child VALUES (998, 1, 'check', -1)", &[], None)
            .is_err()
    );
    assert_eq!(
        s.table_privileges("child", "reader").unwrap(),
        BTreeSet::from([TablePrivilege::Select])
    );
    drop(s);
    drop(a);
    let a = Admin::open(path, Limits::default()).unwrap();
    assert_eq!(
        run(&mut a.sql_session(), "SELECT count(*) FROM child").rows[0].values,
        vec![Value::Int(120)]
    );
}
fn child(path: &Path, mode: &str, seam: &str) {
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "crash_worker", "--nocapture"])
        .env("GD_UPGRADE_PATH", path)
        .env("GD_UPGRADE_MODE", mode)
        .env("GD_UPGRADE_CRASH", seam)
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(86), "{mode}/{seam}: {output:?}");
}
// Same no-destructor Windows primitive as storage::crash_now.
#[cfg(windows)]
fn abrupt() -> ! {
    unsafe extern "system" {
        fn GetCurrentProcess() -> *mut core::ffi::c_void;
        fn TerminateProcess(process: *mut core::ffi::c_void, code: u32) -> i32;
    }
    unsafe {
        TerminateProcess(GetCurrentProcess(), 86);
    }
    std::process::exit(87)
}
#[cfg(not(windows))]
fn abrupt() -> ! {
    std::process::exit(86)
}
#[test]
fn crash_worker() {
    let Ok(mode) = std::env::var("GD_UPGRADE_MODE") else {
        return;
    };
    let path = PathBuf::from(std::env::var_os("GD_UPGRADE_PATH").unwrap());
    if mode == "recover" {
        Admin::recover_upgrade(path).unwrap();
    } else {
        Admin::upgrade_to_v2_with_hook(path, Limits::default(), |at| {
            if format!("{at:?}") == mode {
                abrupt();
            }
            Ok(())
        })
        .unwrap();
    }
    panic!("crash boundary was not reached");
}
fn backups(path: &Path) -> Vec<PathBuf> {
    fs::read_dir(path.parent().unwrap())
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "gdexp"))
        .collect()
}
fn retained(path: &Path) -> PathBuf {
    let found: Vec<_> = fs::read_dir(path.parent().unwrap())
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| {
            p.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("db.v1-pre-upgrade-")
        })
        .collect();
    assert_eq!(found.len(), 1);
    found[0].clone()
}
#[test]
fn every_upgrade_and_recovery_crash() {
    let root = PathBuf::from(std::env::var_os("TEMP").unwrap()).join(format!(
        "upgrade-crash-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir(&root).unwrap();
    let points = [
        "ExportWritten",
        "ExportVerified",
        "V2Built",
        "Verified",
        "FirstRename",
        "SecondRename",
        "BuildBatch",
        "BeforeMarkerRename",
    ];
    let recoveries = [
        ("ExportWritten", "RecoveryDelete"),
        ("FirstRename", "RecoveryRename"),
        ("FirstRename", "RecoveryDelete"),
        ("SecondRename", "RecoveryDelete"),
    ];
    let mut report = Vec::new();
    for (index, (point, recovery)) in points
        .into_iter()
        .map(|p| (p, ""))
        .chain(recoveries)
        .enumerate()
    {
        let case = root.join(index.to_string());
        fs::create_dir(&case).unwrap();
        let path = case.join("db");
        fixture(&path);
        let a = Admin::open(&path, Limits::default()).unwrap();
        let expected = image(&a);
        drop(a);
        child(&path, point, point);
        let completed = point == "SecondRename";
        if completed {
            // The second rename publishes v2 and consumes tmp: this is already complete.
            check(&path, &expected);
            check(&retained(&path), &expected);
        } else {
            let error = Admin::open(&path, Limits::default())
                .err()
                .unwrap()
                .to_string();
            assert!(error.contains("upgrade in progress"), "{error}");
        }
        // Inspect v1 independently before recovery; tmp still blocks its original name.
        let proof = case.join("v1-proof");
        let legacy = if completed || point == "FirstRename" {
            retained(&path)
        } else {
            path.clone()
        };
        copy_dir(&legacy, &proof);
        check(&proof, &expected);
        fs::remove_dir_all(proof).unwrap();
        let exports = backups(&path);
        assert_eq!(exports.len(), 1);
        Admin::verify_export(&exports[0]).unwrap();
        if !recovery.is_empty() {
            if completed {
                // Completed cleanup state: valid v2 with a leftover tmp sibling.
                fs::create_dir(case.join("db.v2-upgrade-tmp")).unwrap();
                assert!(Admin::open(&path, Limits::default()).is_err());
            }
            child(&path, "recover", recovery);
        }
        let outcome = Admin::recover_upgrade(&path).unwrap();
        assert_eq!(
            Admin::recover_upgrade(&path).unwrap(),
            "no upgrade in progress"
        );
        check(&path, &expected);
        let retry = if completed {
            assert!(Admin::upgrade_to_v2(&path, Limits::default()).is_err());
            // A completed v2 cannot be upgraded again; its retained v1 can.
            let retry = case.join("retry");
            copy_dir(&retained(&path), &retry);
            retry
        } else {
            path.clone()
        };
        let upgraded = Admin::upgrade_to_v2(&retry, Limits::default()).unwrap();
        assert!(upgraded.legacy.join("data.redb").exists());
        check(&upgraded.legacy, &expected);
        post_upgrade(&retry, &expected);
        for export in exports.into_iter().chain([upgraded.export]) {
            Admin::verify_export(export).unwrap();
        }
        if completed {
            assert!(path.join("GIGGLES_FORMAT").exists());
        }
        report.push(
            json!({"boundary": point, "recovery_crash": recovery, "outcome": "pass",
            "resolution": outcome, "v1_intact": true, "backup_verified": true}),
        );
        fs::remove_dir_all(case).unwrap();
    }
    let report_path = root.parent().unwrap().join("upgrade_crash_report.json");
    fs::write(
        &report_path,
        serde_json::to_vec_pretty(&json!({"boundaries": report})).unwrap(),
    )
    .unwrap();
    fs::remove_dir(root).unwrap();
    println!(
        "upgrade crash boundaries: {} passed; report={}",
        report.len(),
        report_path.display()
    );
}
#[test]
fn recovery_refuses_ambiguous_or_invalid_states() {
    let root = PathBuf::from(std::env::var_os("TEMP").unwrap())
        .join(format!("upgrade-refuse-{}", std::process::id()));
    fs::create_dir(&root).unwrap();
    let path = root.join("db");
    assert_eq!(
        Admin::recover_upgrade(&path).unwrap(),
        "no upgrade in progress"
    );
    let cli = Command::new(env!("CARGO_BIN_EXE_giggles"))
        .args(["upgrade", "--recover"])
        .arg(&path)
        .output()
        .unwrap();
    assert!(cli.status.success(), "{cli:?}");
    assert!(
        String::from_utf8(cli.stdout)
            .unwrap()
            .contains("no upgrade in progress")
    );
    for name in [
        "db.v1-pre-upgrade-1",
        "db.v1-pre-upgrade-2",
        "db.v2-upgrade-tmp",
    ] {
        fs::create_dir(root.join(name)).unwrap();
    }
    let error = Admin::recover_upgrade(&path).unwrap_err().to_string();
    assert!(error.contains("multiple v1-pre-upgrade"), "{error}");
    fs::create_dir(&path).unwrap();
    fs::write(path.join("GIGGLES_FORMAT"), b"bad").unwrap();
    assert!(Admin::recover_upgrade(&path).is_err());
    assert!(root.join("db.v2-upgrade-tmp").exists());
    fs::remove_dir_all(root).unwrap();
}
