use base64::Engine;
use giggles_pg_conformance::*;
use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};
use tokio::process::Command;
const REQUIRED: &[(&str, &str)] = &[("rows", "exact rows"), ("metadata", "real catalog")];
#[test]
fn executed_embedded_probe_identity_does_not_follow_observed_source_edits() {
    let root = dir();
    let observed = root.join("observed.java");
    std::fs::write(&observed, b"edited checkout source, not embedded probe").unwrap();
    let actual = root.join("JdbcProbe.java");
    let recorded = write_embedded_jdbc_probe(&actual).unwrap();
    assert_ne!(recorded.sha256, identity(&observed).unwrap().sha256);
    assert_eq!(
        std::fs::read(&actual).unwrap(),
        include_bytes!("../probes/JdbcProbe.java")
    );
    std::fs::write(&observed, b"second edit after executable build").unwrap();
    assert_eq!(recorded.sha256, identity(&actual).unwrap().sha256);
    assert!(write_embedded_jdbc_probe(&actual).is_err());
}

#[test]
fn compiled_probe_inventory_requires_main_and_records_inner_class_bytes() {
    let root = dir();
    assert!(jdbc_class_identities(&root).is_err());
    std::fs::write(root.join("JdbcProbe.class"), b"main class bytes").unwrap();
    std::fs::write(root.join("JdbcProbe$TLS.class"), b"TLS class bytes").unwrap();
    std::fs::write(root.join("unrelated.class"), b"not on probe inventory").unwrap();
    let first = jdbc_class_identities(&root).unwrap();
    assert_eq!(first.len(), 2);
    let before = first
        .iter()
        .find(|i| i.path.ends_with("JdbcProbe$TLS.class"))
        .unwrap();
    std::fs::write(root.join("JdbcProbe$TLS.class"), b"changed TLS class").unwrap();
    let second = jdbc_class_identities(&root).unwrap();
    let after = second
        .iter()
        .find(|i| i.path.ends_with("JdbcProbe$TLS.class"))
        .unwrap();
    assert_ne!(before.sha256, after.sha256);
}

fn record(id: &str, status: &str, text: &str) -> String {
    format!(
        "CASE\t{id}\t{status}\t{}\n",
        base64::engine::general_purpose::STANDARD.encode(text)
    )
}
fn capture() -> Capture {
    Capture {
        code: Some(0),
        stdout: format!(
            "VERSION\t42.7.3\n{}{}",
            record("rows", "PASS", "assertions satisfied"),
            record("metadata", "PASS", "assertions satisfied")
        )
        .into_bytes(),
        ..Default::default()
    }
}
fn verdict(c: &Capture) -> Status {
    evaluate("test", "default", "42.7.3", REQUIRED, c).verdict
}
#[test]
fn complete_asserting_probe_passes() {
    assert_eq!(verdict(&capture()), Status::Pass);
}
#[test]
fn missing_required_case_fails() {
    let mut c = capture();
    c.stdout = b"VERSION\t42.7.3\n".to_vec();
    assert_eq!(verdict(&c), Status::Fail);
}
#[test]
fn nonzero_exit_cannot_pass() {
    let mut c = capture();
    c.code = Some(3);
    assert_eq!(verdict(&c), Status::Fail);
}
#[test]
fn signal_exit_cannot_pass() {
    let mut c = capture();
    c.code = None;
    assert_eq!(verdict(&c), Status::Fail);
}
#[test]
fn timeout_cannot_pass() {
    let mut c = capture();
    c.timed_out = true;
    assert_eq!(verdict(&c), Status::Fail);
}
#[test]
fn truncated_capture_cannot_pass() {
    let mut c = capture();
    c.truncated = true;
    assert_eq!(verdict(&c), Status::Fail);
}
#[test]
fn wrong_rows_fail() {
    let mut c = capture();
    c.stdout = format!(
        "VERSION\t42.7.3\n{}{}",
        record("rows", "FAIL", "wrong row 9, expected 8"),
        record("metadata", "PASS", "assertions satisfied")
    )
    .into_bytes();
    assert_eq!(verdict(&c), Status::Fail);
}

#[test]
fn directory_snapshot_has_exact_content_and_independent_writes() {
    let d = dir();
    let src = d.join("source");
    std::fs::create_dir(&src).unwrap();
    std::fs::create_dir(src.join("nested")).unwrap();
    std::fs::write(src.join("nested/data.redb"), b"source").unwrap();
    let dest = d.join("dest");
    let ids = copy_fixture_tree(&src, &dest, 6).unwrap();
    assert_eq!(ids.len(), 1);
    assert_eq!(
        std::fs::read(dest.join("nested/data.redb")).unwrap(),
        b"source"
    );
    std::fs::write(dest.join("nested/data.redb"), b"owned").unwrap();
    assert_eq!(
        std::fs::read(src.join("nested/data.redb")).unwrap(),
        b"source"
    );
    assert!(
        copy_fixture_tree(&src, &d.join("too-large"), 5)
            .unwrap_err()
            .to_string()
            .contains("size limit")
    );
}
#[test]
fn directory_depth_and_entry_bounds_are_enforced() {
    let d = dir();
    let src = d.join("deep");
    std::fs::create_dir(&src).unwrap();
    let mut deep = src.clone();
    for _ in 0..9 {
        deep = deep.join("d");
        std::fs::create_dir(&deep).unwrap();
    }
    assert!(
        copy_fixture_tree(&src, &d.join("deep-out"), 100)
            .unwrap_err()
            .to_string()
            .contains("depth limit")
    );
    let src = d.join("many");
    std::fs::create_dir(&src).unwrap();
    for i in 0..1025 {
        std::fs::write(src.join(i.to_string()), b"").unwrap();
    }
    assert!(
        copy_fixture_tree(&src, &d.join("many-out"), 100)
            .unwrap_err()
            .to_string()
            .contains("entry limit")
    );
}
#[test]
fn metadata_exception_fails() {
    let mut c = capture();
    c.stdout = format!(
        "VERSION\t42.7.3\n{}{}",
        record("rows", "PASS", "assertions satisfied"),
        record("metadata", "FAIL", "SQLSTATE=0A000")
    )
    .into_bytes();
    let p = evaluate("test", "default", "42.7.3", REQUIRED, &c);
    assert_eq!(p.verdict, Status::Fail);
    assert_eq!(p.cases[1].actual, "SQLSTATE=0A000");
}
#[test]
fn unexecuted_metadata_prevents_pass() {
    let mut c = capture();
    c.stdout = format!(
        "VERSION\t42.7.3\n{}{}",
        record("rows", "PASS", "assertions satisfied"),
        record("metadata", "UNTESTED", "no catalog support")
    )
    .into_bytes();
    assert_eq!(verdict(&c), Status::Untested);
}
#[test]
fn stale_version_fails() {
    let mut c = capture();
    c.stdout = String::from_utf8(c.stdout)
        .unwrap()
        .replace("42.7.3", "42.7.4")
        .into_bytes();
    assert_eq!(verdict(&c), Status::Fail);
}
#[test]
fn absent_version_fails() {
    let mut c = capture();
    c.stdout = record("rows", "PASS", "assertions satisfied").into_bytes();
    assert_eq!(verdict(&c), Status::Fail);
}
#[test]
fn duplicate_version_fails() {
    let mut c = capture();
    c.stdout.extend(b"VERSION\t42.7.3\n");
    assert_eq!(verdict(&c), Status::Fail);
}
#[test]
fn duplicate_case_fails() {
    let mut c = capture();
    c.stdout
        .extend(record("rows", "PASS", "assertions satisfied").bytes());
    assert_eq!(verdict(&c), Status::Fail);
}
#[test]
fn unknown_case_fails() {
    let mut c = capture();
    c.stdout
        .extend(record("fake", "PASS", "assertions satisfied").bytes());
    assert_eq!(verdict(&c), Status::Fail);
}
#[test]
fn unknown_status_fails() {
    let mut c = capture();
    c.stdout.extend(record("rows", "SKIP", "anything").bytes());
    assert_eq!(verdict(&c), Status::Fail);
}
#[test]
fn malformed_base64_fails() {
    let mut c = capture();
    c.stdout.extend(b"CASE\trows\tPASS\t!\n");
    assert_eq!(verdict(&c), Status::Fail);
}
#[test]
fn non_utf8_output_fails() {
    let mut c = capture();
    c.stdout.push(255);
    assert_eq!(verdict(&c), Status::Fail);
}
#[test]
fn unsolicited_output_fails() {
    let mut c = capture();
    c.stdout.extend(b"success!\n");
    assert_eq!(verdict(&c), Status::Fail);
}
#[test]
fn pass_with_wrong_actual_fails() {
    let mut c = capture();
    c.stdout = String::from_utf8(c.stdout)
        .unwrap()
        .replace(
            &base64::engine::general_purpose::STANDARD.encode("assertions satisfied"),
            &base64::engine::general_purpose::STANDARD.encode("wrong rows"),
        )
        .into_bytes();
    assert_eq!(verdict(&c), Status::Fail);
}
#[test]
fn oversized_actual_fails() {
    let mut c = capture();
    c.stdout
        .extend(record("rows", "FAIL", &"x".repeat(4097)).bytes());
    assert_eq!(verdict(&c), Status::Fail);
}
#[test]
fn oversized_record_fails() {
    let mut c = capture();
    c.stdout.extend("x".repeat(8193).bytes());
    assert_eq!(verdict(&c), Status::Fail);
}
#[test]
fn empty_actual_fails() {
    let mut c = capture();
    c.stdout.extend(record("rows", "PASS", "").bytes());
    assert_eq!(verdict(&c), Status::Fail);
}
#[test]
fn explicit_missing_tool_is_untested() {
    let p = untested("psql", "inventory", REQUIRED, "executable not installed");
    assert_eq!(p.verdict, Status::Untested);
    assert!(p.cases.iter().all(|c| c.status == Status::Untested));
}
#[test]
fn startup_failure_keeps_all_expected_cases_and_fails() {
    let p = failed("pgJDBC", "default", REQUIRED, "readiness timeout");
    assert_eq!(p.verdict, Status::Fail);
    assert_eq!(p.cases.len(), REQUIRED.len());
    assert!(
        p.cases
            .iter()
            .all(|c| c.status == Status::Fail && c.actual == "readiness timeout")
    );
    assert_eq!(p.integrity, vec!["readiness timeout"]);
}
#[test]
fn readiness_only_loopback_nonzero_bounded() {
    assert_eq!(
        parse_port(b"native=127.0.0.1:100 postgres=127.0.0.1:123 preview=single-host").unwrap(),
        123
    );
    for s in [
        "native=127.0.0.1:1 postgres=0.0.0.0:2 preview=single-host",
        "native=127.0.0.1:1 postgres=127.0.0.1:0 preview=single-host",
        "postgres=127.0.0.1:2",
        "native=127.0.0.1:1 postgres=127.0.0.1:65536 preview=single-host",
    ] {
        assert!(parse_port(s.as_bytes()).is_err());
    }
    assert!(parse_port(&[b'a'; 1025]).is_err());
}
static SERIAL: AtomicU64 = AtomicU64::new(0);
fn dir() -> PathBuf {
    let d = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("artifacts")
        .join(format!(
            "honesty-{}-{}",
            std::process::id(),
            SERIAL.fetch_add(1, Ordering::Relaxed)
        ));
    std::fs::create_dir_all(&d).unwrap();
    d
}
#[test]
fn bounded_file_and_identity_change() {
    let d = dir();
    let p = d.join("input");
    std::fs::write(&p, b"12345").unwrap();
    assert!(bounded(&p, 4).is_err());
    assert_eq!(bounded(&p, 5).unwrap(), b"12345");
    let before = identity(&p).unwrap();
    std::fs::write(&p, b"12346").unwrap();
    assert_ne!(before.sha256, identity(&p).unwrap().sha256);
}
#[test]
fn manifest_rejects_unknown_fields_and_unbounded_timeout() {
    let d = dir();
    let p = d.join("manifest.json");
    let mut v = serde_json::json!({"server":d.join("server"),"fixture":d,"server_config":d.join("config"),"config_base":d,"output":d.join("out"),"timeout_seconds":301});
    std::fs::write(&p, v.to_string()).unwrap();
    assert!(Manifest::read(&p).is_err());
    v["timeout_seconds"] = 1.into();
    v["unexpected"] = true.into();
    std::fs::write(&p, v.to_string()).unwrap();
    assert!(Manifest::read(&p).is_err());
}
#[test]
fn manifest_rejects_relative_paths_and_oversize() {
    let d = dir();
    let p = d.join("manifest.json");
    std::fs::write(&p,serde_json::json!({"server":"relative","fixture":d,"server_config":d,"config_base":d,"output":d}).to_string()).unwrap();
    assert!(Manifest::read(&p).is_err());
    std::fs::write(&p, vec![b' '; 65537]).unwrap();
    assert!(Manifest::read(&p).is_err());
}
#[test]
fn isolation_rejects_paths_outside_fixture() {
    let d = dir();
    let fixture = d.join("fixture");
    std::fs::create_dir(&fixture).unwrap();
    let db = d.join("db.redb");
    std::fs::write(&db, b"fake").unwrap();
    let cfg = d.join("config.json");
    std::fs::write(&cfg, serde_json::json!({"database":db}).to_string()).unwrap();
    let m = Manifest {
        server: db,
        fixture,
        server_config: cfg,
        config_base: d.clone(),
        output: d.clone(),
        java: None,
        javac: None,
        jdbc_jar: None,
        expected_server_sha256: None,
        expected_jar_sha256: None,
        timeout_seconds: 1,
    };
    assert!(
        isolate(&m, &d)
            .unwrap_err()
            .to_string()
            .contains("outside explicit fixture")
    );
}
#[test]
fn isolation_copies_database_and_rewrites_listeners() {
    let d = dir();
    let fixture = d.join("fixture");
    std::fs::create_dir(&fixture).unwrap();
    let out = d.join("owned");
    std::fs::create_dir(&out).unwrap();
    for name in [
        "db.redb",
        "server.pem",
        "server.key.TEST-ONLY",
        "ca.pem",
        "app.pem",
        "app.key.TEST-ONLY",
    ] {
        std::fs::write(fixture.join(name), name.as_bytes()).unwrap();
    }
    let cfg = d.join("config.json");
    std::fs::write(&cfg,serde_json::json!({"database":fixture.join("db.redb"),"certificate":fixture.join("server.pem"),"private_key":fixture.join("server.key.TEST-ONLY"),"client_ca":fixture.join("ca.pem"),"native":"0.0.0.0:12","postgres":"0.0.0.0:13","bindings":[]}).to_string()).unwrap();
    let m = Manifest {
        server: cfg.clone(),
        fixture: fixture.clone(),
        server_config: cfg,
        config_base: d,
        output: out.clone(),
        java: None,
        javac: None,
        jdbc_jar: None,
        expected_server_sha256: None,
        expected_jar_sha256: None,
        timeout_seconds: 1,
    };
    assert_eq!(isolate(&m, &out).unwrap().len(), 6);
    let c: serde_json::Value =
        serde_json::from_slice(&std::fs::read(out.join("server.json")).unwrap()).unwrap();
    assert_eq!(c["postgres"], "127.0.0.1:0");
    assert_eq!(
        c["database"],
        out.join("db.redb").to_string_lossy().as_ref()
    );
    std::fs::write(out.join("db.redb"), b"mutated").unwrap();
    assert_eq!(std::fs::read(fixture.join("db.redb")).unwrap(), b"db.redb");
}

// The test executable is its own deterministic subprocess fixture; no shell or external tool dependency.
#[test]
fn child_fixture() {
    match std::env::var("GD48_TEST_CHILD").as_deref() {
        Ok("timeout") => std::thread::sleep(Duration::from_secs(20)),
        Ok("overflow") => {
            use std::io::Write;
            std::io::stdout().write_all(&vec![b'x'; CAP + 100]).unwrap();
        }
        Ok("nonzero") => std::process::exit(7),
        _ => {}
    }
}
#[tokio::test]
async fn process_timeout_is_bounded_and_reaped() {
    let now = std::time::Instant::now();
    let c = run(
        Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "child_fixture", "--nocapture"])
            .env("GD48_TEST_CHILD", "timeout"),
        Duration::from_millis(150),
    )
    .await
    .unwrap();
    assert!(c.timed_out);
    assert!(now.elapsed() < Duration::from_secs(5));
}
#[tokio::test]
async fn process_output_is_bounded_and_flagged() {
    let c = run(
        Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "child_fixture", "--nocapture"])
            .env("GD48_TEST_CHILD", "overflow"),
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    assert_eq!(c.stdout.len(), CAP);
    assert!(c.truncated);
    assert_eq!(c.code, Some(0));
}
#[tokio::test]
async fn process_nonzero_is_preserved() {
    let c = run(
        Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "child_fixture", "--nocapture"])
            .env("GD48_TEST_CHILD", "nonzero"),
        Duration::from_secs(5),
    )
    .await
    .unwrap();
    assert_eq!(c.code, Some(7));
    assert!(!c.timed_out);
}

#[test]
fn discovery_missing_output_fails_every_required_case() {
    let c = Capture {
        code: Some(0),
        stdout: b"VERSION\t42.7.3\n".to_vec(),
        ..Default::default()
    };
    let p = evaluate(
        "pgJDBC",
        "datagrip-discovery",
        "42.7.3",
        DISCOVERY_CASES,
        &c,
    );
    assert_eq!(p.verdict, Status::Fail);
    assert!(p.cases.iter().all(|c| c.status == Status::Fail));
}
#[test]
fn discovery_incorrect_rows_cannot_pass() {
    let mut c = capture();
    c.stdout = format!(
        "VERSION\t42.7.3\n{}{}{}{}{}{}{}",
        record("dg_IsSuperUser", "FAIL", "wrong superuser row"),
        record("dg_ListDatabases", "FAIL", "invented database"),
        record("dg_ListSchemas", "PASS", "assertions satisfied"),
        record("dg_ListExistentLanguages", "PASS", "assertions satisfied"),
        record("dg_ListLanguages", "PASS", "assertions satisfied"),
        record("dg_RetrieveEventTriggers", "PASS", "assertions satisfied"),
        record("dg_remaining_templates", "UNTESTED", "templates pending")
    )
    .into_bytes();
    let p = evaluate(
        "pgJDBC",
        "datagrip-discovery",
        "42.7.3",
        DISCOVERY_CASES,
        &c,
    );
    assert_eq!(p.verdict, Status::Fail);
    assert!(p.integrity.is_empty());
}
#[test]
fn discovery_untested_templates_prevent_support() {
    let mut c = capture();
    c.stdout = b"VERSION\t42.7.3\n".to_vec();
    for (name, _) in DISCOVERY_CASES {
        c.stdout.extend(
            record(
                name,
                if *name == "dg_remaining_templates" {
                    "UNTESTED"
                } else {
                    "PASS"
                },
                "assertions satisfied",
            )
            .bytes(),
        );
    }
    assert_eq!(
        evaluate(
            "pgJDBC",
            "datagrip-discovery",
            "42.7.3",
            DISCOVERY_CASES,
            &c
        )
        .verdict,
        Status::Untested
    );
    c.stdout
        .extend(record("dg_IsSuperUser", "PASS", "assertions satisfied").bytes());
    assert_eq!(
        evaluate(
            "pgJDBC",
            "datagrip-discovery",
            "42.7.3",
            DISCOVERY_CASES,
            &c
        )
        .verdict,
        Status::Fail
    );
}
#[test]
fn discovery_cannot_substitute_for_main_required_cases() {
    let c = Capture {
        code: Some(0),
        stdout: format!(
            "VERSION\t42.7.3\n{}",
            record("dg_IsSuperUser", "PASS", "assertions satisfied")
        )
        .into_bytes(),
        ..Default::default()
    };
    let p = evaluate("pgJDBC", "default", "42.7.3", JDBC_CASES, &c);
    assert_eq!(p.verdict, Status::Fail);
    assert!(p.cases.iter().all(|c| c.status == Status::Fail));
    assert!(!p.integrity.is_empty());
}

#[test]
fn discovery_resource_identity_and_executable_queries_are_pinned() {
    use sha2::{Digest, Sha256};
    let resource = include_bytes!("../probes/datagrip-2026.2.5/PgIntroQueries.sql");
    let inventory: serde_json::Value =
        serde_json::from_str(include_str!("../probes/datagrip-2026.2.5/inventory.json")).unwrap();
    assert_eq!(
        format!("{:x}", Sha256::digest(resource)),
        inventory["resource_sha256"].as_str().unwrap()
    );
    assert_eq!(inventory["version"], "2026.2.5");
    assert_eq!(inventory["build"], "262.10315.132");
    assert_eq!(inventory["blocks"].as_array().unwrap().len(), 98);
    let sql = std::str::from_utf8(resource).unwrap();
    let java = include_str!("../probes/JdbcProbe.java");
    for name in ["IsSuperUser", "ListDatabases"] {
        let marker = format!("---- {name} ----");
        let section = sql
            .split_once(&marker)
            .unwrap()
            .1
            .split("----")
            .next()
            .unwrap()
            .trim();
        assert!(
            java.contains(&serde_json::to_string(section).unwrap()),
            "captured executable SQL drift: {name}"
        );
    }
}
#[test]
fn required_registries_have_unique_cases_and_preserve_main_contract() {
    for cases in [JDBC_CASES, DISCOVERY_CASES] {
        let ids = cases
            .iter()
            .map(|(id, _)| id)
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(ids.len(), cases.len());
    }
    for id in [
        "metadata_tables",
        "metadata_columns",
        "metadata_pk",
        "metadata_types",
        "metadata_hidden",
        "schema_fence",
        "cancel",
        "metadata_type_names",
        "identity",
    ] {
        assert!(JDBC_CASES.iter().any(|(key, _)| *key == id));
    }
}

#[test]
fn expanded_batch_sql_is_exact_and_every_case_is_required() {
    use sha2::{Digest, Sha256};
    let captured: Vec<serde_json::Value> = serde_json::from_str(include_str!(
        "../probes/datagrip-2026.2.5/latest-expanded-queries.json"
    ))
    .unwrap();
    assert_eq!(captured.len(), 14);
    let java = include_str!("../probes/JdbcProbe.java");
    for row in captured {
        let sql = row["sql"].as_str().unwrap();
        assert_eq!(
            format!("{:x}", Sha256::digest(sql.as_bytes())),
            row["hash"].as_str().unwrap()
        );
        assert_eq!(row["sha_match_raw"], true);
        let id = row["case"].as_str().unwrap();
        assert!(
            DISCOVERY_CASES.iter().any(|(name, _)| *name == id),
            "unrequired captured case {id}"
        );
        assert!(
            java.contains(&format!(
                "batchQuery({},{},",
                serde_json::to_string(id).unwrap(),
                serde_json::to_string(sql).unwrap()
            )),
            "unexecuted/drifting captured SQL {id}"
        );
    }
}
#[test]
fn unsupported_mvcc_never_becomes_client_support() {
    let mut c = capture();
    c.stdout = b"VERSION\t42.7.3\n".to_vec();
    for (id, _) in DISCOVERY_CASES {
        let unsupported = matches!(*id, "dg_StateNumber" | "dg_CurrentXid");
        c.stdout.extend(
            record(
                id,
                if unsupported { "UNTESTED" } else { "PASS" },
                if unsupported {
                    "UNSUPPORTED PostgreSQL MVCC; asserted SQLSTATE=0A000; rejection is not support"
                } else {
                    "assertions satisfied"
                },
            )
            .bytes(),
        );
    }
    let result = evaluate(
        "pgJDBC",
        "datagrip-discovery",
        "42.7.3",
        DISCOVERY_CASES,
        &c,
    );
    assert_eq!(result.verdict, Status::Untested);
    assert!(result.integrity.is_empty());
    assert_eq!(
        result
            .cases
            .iter()
            .filter(|c| c.status == Status::Untested)
            .count(),
        2
    );
}

#[test]
fn installed_app_templates_and_fixture_contract_are_independently_pinned() {
    use sha2::{Digest, Sha256};
    let resource = include_str!("../probes/datagrip-2026.2.5/PgIntroQueries.sql");
    let java = include_str!("../probes/JdbcProbe.java");
    let rows: Vec<serde_json::Value> = serde_json::from_str(include_str!(
        "../probes/datagrip-2026.2.5/app-expanded-queries.json"
    ))
    .unwrap();
    assert_eq!(rows.len(), 7);
    for row in rows {
        let name = row["name"].as_str().unwrap();
        let template = row["template"].as_str().unwrap();
        let sql = row["sql"].as_str().unwrap();
        let marker = format!("---- {name} ----");
        let installed = resource
            .split_once(&marker)
            .unwrap()
            .1
            .split("----")
            .next()
            .unwrap()
            .trim();
        assert_eq!(
            installed,
            template.trim(),
            "installed source template drift: {name}"
        );
        assert_eq!(
            format!("{:x}", Sha256::digest(template.as_bytes())),
            row["template_sha256"].as_str().unwrap()
        );
        assert_eq!(
            format!("{:x}", Sha256::digest(sql.as_bytes())),
            row["expanded_sha256"].as_str().unwrap()
        );
        assert!(!sql.contains('#'), "unexpanded template macro");
        assert!(
            sql.contains(":schema_id::oid"),
            "schema OID must be discovered at runtime"
        );
        let id = row["case"].as_str().unwrap();
        assert!(DISCOVERY_CASES.iter().any(|(required, _)| *required == id));
        assert!(
            java.contains(&format!(
                "appQuery({},{},",
                serde_json::to_string(id).unwrap(),
                serde_json::to_string(sql).unwrap()
            )),
            "unexecuted app template {name}"
        );
    }
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../probes/datagrip-2026.2.5/fixture-contract.json"
    ))
    .unwrap();
    let names: Vec<_> = fixture["columns"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["id", "body", "flag", "small", "num", "data"]);
    assert_eq!(fixture["primary_key"], serde_json::json!(["id"]));
    assert_eq!(fixture["hidden_table"], "other");
    assert!(java.contains("hidden other catalog disclosed"));
}
