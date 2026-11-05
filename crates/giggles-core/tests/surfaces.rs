mod common;
use common::*;
use giggles_core::*;
#[test]
fn narrow_sql_matches_native_and_denies_escape_grammar() {
    let (_, a, e) = fixture(3);
    let sql = "UPDATE deliveries SET status = 'queued' WHERE tenant = 42 AND id = 0";
    assert_eq!(parse_repair(OperationId(1), sql).unwrap(), request(1, &[0]));
    assert!(
        e.execute_sql(OperationId(1), sql, Time::Certified(10), None)
            .unwrap()
            .is_ok()
    );
    assert_eq!(
        e.execute_sql(OperationId(1), sql, Time::Certified(11), None)
            .unwrap(),
        e.execute(request(1, &[0]), Time::Uncertain, None).unwrap()
    );
    let before = a.snapshot().unwrap();
    for bad in [
        "DELETE FROM deliveries",
        "UPDATE deliveries SET status = 'queued'",
        "UPDATE deliveries SET status = 'queued' WHERE tenant = 42 AND id = 0; DROP TABLE task",
        "UPDATE deliveries SET status = 'queued' WHERE tenant = 42 OR id = 1",
        "UPDATE deliveries SET status = 'queued' WHERE tenant = 42 AND id = +1",
        "UPDATE deliveries SET status = 'queued' WHERE tenant = 42 AND id = 1 --comment",
        "SELECT * FROM receipts",
        "UPDATE deliveries SET status = lower('queued') WHERE tenant = 42 AND id = 1",
    ] {
        assert_eq!(
            e.execute_sql(OperationId(20), bad, Time::Certified(10), None),
            Err(CoreError::Unsupported)
        );
    }
    assert_eq!(a.snapshot().unwrap(), before);
}
#[test]
fn sql_predicate_uses_exact_approval_and_same_policy() {
    let (_, a, e) = fixture(6);
    let sql = "UPDATE deliveries SET status = 'queued' WHERE tenant = 42 AND status = 'failed' AND failed_at >= 100 AND failed_at < 107";
    let r = parse_repair(OperationId(1), sql).unwrap();
    let p = e.preview(&r, Time::Certified(10)).unwrap();
    let id = a.approve(Principal(3), p, Time::Certified(10)).unwrap();
    assert!(
        e.execute_sql(OperationId(1), sql, Time::Certified(10), Some(id))
            .unwrap()
            .is_ok()
    );
    assert_eq!(
        e.execute_sql(
            OperationId(2),
            "UPDATE deliveries SET status = 'queued' WHERE tenant = 43 AND id = 0",
            Time::Certified(10),
            None
        )
        .unwrap(),
        Err(Error::Scope)
    );
}
#[test]
fn sql_size_numeric_and_operator_bounds() {
    assert_eq!(
        parse_repair(OperationId(1), &"x".repeat(4097)),
        Err(CoreError::Limit)
    );
    for token in ["18446744073709551616", "1e5", "-1", "0x10", "1;", "'1'"] {
        assert_eq!(
            parse_repair(
                OperationId(1),
                &format!(
                    "UPDATE deliveries SET status = 'queued' WHERE tenant = 42 AND id = {token}"
                )
            ),
            Err(CoreError::Unsupported)
        );
    }
}
#[test]
fn cli_demo_export_verify_restore_and_status() {
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
    let demo = run(&["demo".as_ref(), p.as_os_str()]);
    let text = String::from_utf8(demo.stdout).unwrap();
    let demo: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(demo["data"]["permitted"]["code"], 0);
    assert_eq!(demo["data"]["forbidden"]["code"], 108);
    let file = fresh("cli-export");
    run(&["export".as_ref(), p.as_os_str(), file.as_os_str()]);
    run(&["verify".as_ref(), file.as_os_str()]);
    let target = fresh("cli-restore");
    run(&["restore".as_ref(), file.as_os_str(), target.as_os_str()]);
    let status = run(&[
        "status".as_ref(),
        p.as_os_str(),
        "1".as_ref(),
        "2".as_ref(),
        "1".as_ref(),
    ]);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&status.stdout).unwrap()["code"],
        0
    );
    std::fs::write(p.join("demo.stdout.txt"), text).unwrap();
}
