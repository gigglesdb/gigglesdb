use std::{
    path::PathBuf,
    process::{Command, Output},
    sync::atomic::{AtomicU64, Ordering},
};
static NEXT: AtomicU64 = AtomicU64::new(0);
fn path() -> PathBuf {
    std::env::temp_dir().join(format!(
        "giggles-native-cli-{}-{}-{}.db",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ))
}
fn invoke(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_giggles-language"))
        .args(args)
        .output()
        .unwrap()
}
fn init(p: &str) {
    let out = invoke(&[
        "init",
        p,
        "create table items (id bigint primary key, state text not null)",
    ]);
    assert!(out.status.success(), "{:?}", out);
}
fn json(out: &Output) -> serde_json::Value {
    assert!(out.status.success(), "{:?}", out);
    serde_json::from_slice(&out.stdout).unwrap()
}
#[test]
fn persistent_init_run_reopen() {
    let p = path();
    let p = p.to_str().unwrap();
    init(p);
    assert_eq!(
        json(&invoke(&[
            "run",
            p,
            "insert items set id=$id,state=$state",
            r#"{"id":{"Int":42},"state":{"Text":"ready"}}"#
        ]))["changed"],
        1
    );
    let first = json(&invoke(&["run", p, "items where id=42 select state"]));
    assert_eq!(first["rows"][0]["values"][0]["Text"], "ready");
    json(&invoke(&[
        "run",
        p,
        "claim items where state=\"ready\" order by id set state=\"taken\"",
    ]));
    assert_eq!(
        json(&invoke(&["run", p, "items where state=\"taken\""]))["matched"],
        1
    );
}
#[test]
fn malformed_parameters_exit() {
    let p = path();
    let p = p.to_str().unwrap();
    init(p);
    for args in [
        vec![
            "run",
            p,
            "insert items set id=1,state=\"ready\"",
            "{}garbage",
        ],
        vec!["run", p, "items where id=$id", r#"{"id":"1"}"#],
        vec!["run", p, "items where id=$id", "{}"],
        vec!["run", p, "insert items set id=1,state=\"ready\";items"],
    ] {
        let out = invoke(&args);
        assert!(!out.status.success());
        let v: serde_json::Value = serde_json::from_slice(&out.stderr).unwrap();
        assert!(v.get("error").is_some());
    }
    assert_eq!(json(&invoke(&["run", p, "items"]))["matched"], 0);
}
#[test]
fn capabilities_honest() {
    let v = json(&invoke(&["capabilities"]));
    assert_eq!(v["name"], "embedded-trusted-native-0.1");
    assert_eq!(v["automatic_retries"], false);
    assert_eq!(v["trusted_admin"], true);
    let omitted = v["unsupported"].as_array().unwrap();
    for s in [
        "once",
        "receipts",
        "task authorization",
        "decimal",
        "uuid",
        "server",
    ] {
        assert!(omitted.contains(&serde_json::Value::String(s.into())))
    }
    assert!(!invoke(&["capabilities", "extra"]).status.success());
}
#[test]
fn init_requires_a_create_table_statement() {
    for schema in ["begin", "commit", "rollback", "select * from missing"] {
        let p = path();
        let out = invoke(&["init", p.to_str().unwrap(), schema]);
        assert!(!out.status.success(), "accepted schema {schema}: {out:?}");
        assert!(out.stdout.is_empty());
    }
    let p = path();
    init(p.to_str().unwrap());
    assert_eq!(
        json(&invoke(&["run", p.to_str().unwrap(), "items"]))["matched"],
        0
    );
}
#[test]
fn subprocess_failure_reopen_controls() {
    let p = path();
    let p = p.to_str().unwrap();
    init(p);
    json(&invoke(&[
        "run",
        p,
        "insert items set id=1,state=\"ready\"",
    ]));
    json(&invoke(&["run", p, "insert items set id=2,state=\"done\""]));
    for text in [
        "update items where true when state=\"ready\" set state=\"x\"",
        "delete many items where true at most 1",
        "claim items where state=\"ready\" order by id set state=\"ready\"",
    ] {
        assert!(!invoke(&["run", p, text]).status.success());
        assert_eq!(
            json(&invoke(&["run", p, "items where state=\"ready\""]))["matched"],
            1
        )
    }
    let duplicate = invoke(&["init", p, "create table other (id bigint primary key)"]);
    assert!(!duplicate.status.success());
    assert_eq!(json(&invoke(&["run", p, "items"]))["matched"], 2);
}

// Runs only when explicitly spawned by abrupt_process_bulk_atomicity. The
// environment variables are test transport, never a production failure hook.
#[test]
#[ignore = "test-only crash subprocess entrypoint"]
fn crash_worker() {
    let path = std::env::var_os("GIGGLES_NATIVE_CRASH_DB").expect("child database");
    let point = match std::env::var("GIGGLES_NATIVE_CRASH_POINT")
        .unwrap()
        .as_str()
    {
        "before" => giggles_core::CrashPoint::BeforeCommit,
        "after" => giggles_core::CrashPoint::AfterCommit,
        _ => panic!("invalid crash point"),
    };
    let a = giggles_core::Admin::open(path, giggles_core::Limits::default()).unwrap();
    a.set_crash_point(point).unwrap();
    let result = giggles_language::Engine::new(a).execute_text(
        "update many items where state=\"ready\" set state=\"done\" at most 2",
        &giggles_language::Bindings::new(),
    );
    panic!("crash seam did not terminate: {result:?}");
}

#[test]
fn abrupt_process_bulk_atomicity() {
    for (point, expected) in [("before", "ready"), ("after", "done")] {
        let p = path();
        let pstr = p.to_str().unwrap();
        init(pstr);
        for id in 1..=2 {
            json(&invoke(&[
                "run",
                pstr,
                &format!("insert items set id={id},state=\"ready\""),
            ]));
        }
        let output = Command::new(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "crash_worker",
                "--nocapture",
                "--test-threads=1",
            ])
            .env("GIGGLES_NATIVE_CRASH_DB", &p)
            .env("GIGGLES_NATIVE_CRASH_POINT", point)
            .output()
            .unwrap();
        std::fs::write(
            p.with_extension(format!("{point}.stdout.log")),
            &output.stdout,
        )
        .unwrap();
        std::fs::write(
            p.with_extension(format!("{point}.stderr.log")),
            &output.stderr,
        )
        .unwrap();
        assert!(!output.status.success(), "expected abrupt process exit");
        // A missing child test or an ordinary panic cannot satisfy the control:
        // core's crash seam uses exit 86, while libtest panics use exit 101.
        assert_eq!(output.status.code(), Some(86), "{output:?}");
        let rows = json(&invoke(&["run", pstr, "items order by id select state"]));
        assert_eq!(rows["matched"], 2);
        for row in rows["rows"].as_array().unwrap() {
            assert_eq!(row["values"][0]["Text"], expected);
        }
    }
}

#[test]
fn explicit_engine_init_run_reopen() {
    for engine in ["legacy", "v2"] {
        let p = path();
        let pstr = p.to_str().unwrap();
        let out = invoke(&[
            "init",
            "--engine",
            engine,
            pstr,
            "create table items (id bigint primary key, state text not null)",
        ]);
        // Also exercises the no-RocksDB build when core defaults are disabled.
        if engine == "v2" && !out.status.success() {
            let error = String::from_utf8(out.stderr).unwrap();
            assert!(error.contains("engine-rocksdb"), "{error}");
            assert!(!p.exists());
            continue;
        }
        json(&out);
        assert_eq!(
            json(&invoke(&[
                "run",
                pstr,
                "insert items set id=1,state=\"ready\""
            ]))["changed"],
            1
        );
        assert_eq!(json(&invoke(&["run", pstr, "items"]))["matched"], 1);
        // Init is create-new, including when a different engine is requested.
        let other = if engine == "legacy" { "v2" } else { "legacy" };
        assert!(!invoke(&["init", "--engine", other, pstr]).status.success());
        assert_eq!(json(&invoke(&["run", pstr, "items"]))["matched"], 1);
        // Engine selection is init-only: reopening always detects stored format.
        assert!(
            !invoke(&["run", "--engine", other, pstr, "items"])
                .status
                .success()
        );
    }
}

#[test]
fn invalid_cli_options_have_no_filesystem_effects() {
    let parent = path();
    let db = parent.join("db");
    let p = db.to_str().unwrap();
    for args in [
        vec!["init", "--engine", "unknown", p],
        vec![
            "init",
            "--engine",
            "unknown",
            p,
            "create table t(id int primary key)",
        ],
        vec!["init", "--language-version", "999", p],
        vec!["init", "--engine", "legacy", "--language-version", "999", p],
        vec!["run", "--language-version", "999", p, "items"],
        vec!["capabilities", "--language-version", "999"],
        vec!["capability-manifest", "--language-version", "999"],
    ] {
        assert!(!invoke(&args).status.success(), "{args:?}");
        assert!(!parent.exists(), "{args:?} created a directory");
    }
    let out = invoke(&["init", "--engine", "unknown", p]);
    assert!(
        String::from_utf8(out.stderr)
            .unwrap()
            .contains("unknown engine")
    );
}
