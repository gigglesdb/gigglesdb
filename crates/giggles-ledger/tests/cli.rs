use giggles_ledger::*;
use serde_json::{Value, json};
use std::{
    io::Write,
    path::PathBuf,
    process::{Command, Stdio},
    sync::atomic::{AtomicU64, Ordering},
};
static NEXT: AtomicU64 = AtomicU64::new(0);
fn path(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "giggles-ledger-cli-{label}-{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ))
}
fn invoke(v: Value) -> (bool, Option<Value>) {
    let mut child = Command::new(env!("CARGO_BIN_EXE_giggles-ledger"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(v.to_string().as_bytes())
        .unwrap();
    let out = child.wait_with_output().unwrap();
    (
        out.status.success(),
        serde_json::from_slice(&out.stdout).ok(),
    )
}
fn ok(v: Value) -> Value {
    let (success, out) = invoke(v);
    assert!(success, "{out:?}");
    out.unwrap()["ok"].clone()
}
fn intent(id: &str, op: Operation, now: u64) -> Intent {
    Intent {
        id: id.into(),
        clock: Clock {
            lower: now,
            upper: now,
        },
        operation: op,
    }
}
fn transfer(n: u128) -> Transfer {
    Transfer {
        ledger: "main".into(),
        asset: "USD".into(),
        debit: "mint".into(),
        credit: "a".into(),
        amount: Amount(n),
    }
}
fn setup() -> PathBuf {
    let p = path("db");
    ok(json!({"command":"create","path":p,"definition":{"ledger":"main","asset":"USD","scale":2}}));
    for (id, rule) in [
        ("mint", BalanceRule::Unrestricted),
        ("a", BalanceRule::DebitsMayNotExceedCredits),
    ] {
        ok(
            json!({"command":"account","path":p,"definition":AccountDefinition {id:id.into(),rule,max_credit:None}}),
        );
    }
    p
}
#[test]
fn workflow_json_real_database() {
    let p = setup();
    let i = intent("fund", Operation::Post(transfer(u128::MAX - 100)), 0);
    let r = ok(json!({"command":"execute","path":p,"intent":i}));
    assert_eq!(ok(json!({"command":"status","path":p,"id":"fund"})), r);
    let reserve = intent(
        "hold",
        Operation::Reserve {
            transfer: transfer(2),
            deadline: 100,
        },
        1,
    );
    ok(json!({"command":"execute","path":p,"intent":reserve}));
    ok(
        json!({"command":"execute","path":p,"intent":intent("capture",Operation::Capture {reserve:"hold".into()},2)}),
    );
    let report = ok(json!({"command":"reconcile","path":p}));
    assert_eq!(report["receipts"], 3);
    assert_eq!(report["pending"], 0);
    assert_eq!(
        ok(json!({"command":"get_account","path":p,"id":"a"}))["totals"]["posted_credits"],
        serde_json::to_value(Amount(u128::MAX - 98)).unwrap()
    );
}
#[test]
fn malformed_cli_rejected() {
    for v in [
        json!({"command":"unknown"}),
        json!({"command":"create","path":"unused","definition":{},"surprise":1}),
        json!({"command":"execute","path":"unused","intent":{"id":"x","amount":1.5}}),
    ] {
        let (success, out) = invoke(v);
        assert!(!success);
        assert!(out.unwrap().get("error").is_some());
    }
}
#[test]
fn subprocess_crash_edges_reopen_and_retry() {
    for (fault, committed) in [(Fault::BeforeCommit, false), (Fault::AfterCommit, true)] {
        let p = setup();
        let i = intent("stable", Operation::Post(transfer(7)), 0);
        let (success, _) = invoke(json!({"command":"inject","path":p,"intent":i,"fault":fault}));
        assert!(!success);
        let status = ok(json!({"command":"status","path":p,"id":"stable"}));
        assert_eq!(!status.is_null(), committed);
        let r = ok(json!({"command":"execute","path":p,"intent":i}));
        assert_eq!(r["ordinal"], 1);
        let report = ok(json!({"command":"reconcile","path":p}));
        assert_eq!(report["receipts"], 1);
    }
}
#[test]
fn export_restore_duplicate_cli() {
    let p = setup();
    let image = path("export");
    let restored = path("restore");
    let i = intent("stable", Operation::Post(transfer(7)), 0);
    let r = ok(json!({"command":"execute","path":p,"intent":i}));
    ok(json!({"command":"export","path":p,"destination":image}));
    ok(json!({"command":"restore","source":image,"path":restored}));
    assert_eq!(
        ok(json!({"command":"execute","path":restored,"intent":i})),
        r
    );
}
