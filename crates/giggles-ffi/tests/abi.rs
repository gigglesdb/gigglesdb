#[path = "../../giggles-core/tests/common.rs"]
mod common;
use common::*;
use giggles_core::*;
use giggles_ffi::*;
use serde_json::{Value, json};
use std::ptr::{null, null_mut};
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());
fn open(mode: &str, path: &std::path::Path) -> u64 {
    let data = json!({"mode":mode,"path":path}).to_string();
    let mut h = 0;
    assert_eq!(
        unsafe { gd_v1_admin_open(data.as_ptr(), data.len(), &mut h) },
        0
    );
    assert_ne!(h, 0);
    h
}
fn copy(b: u64) -> Value {
    unsafe {
        let mut n = 0;
        assert_eq!(gd_v1_buffer_copy(b, null_mut(), 0, &mut n), 20);
        assert!(n > 0);
        let mut out = vec![0; n];
        assert_eq!(gd_v1_buffer_copy(b, out.as_mut_ptr(), out.len(), &mut n), 0);
        serde_json::from_slice(&out).unwrap()
    }
}
fn call(h: u64, v: Value, code: u32) -> Value {
    let s = v.to_string();
    let mut b = 0;
    assert_eq!(unsafe { gd_v1_call(h, s.as_ptr(), s.len(), &mut b) }, code);
    assert_ne!(b, 0);
    let v = copy(b);
    assert_eq!(v["code"], code);
    assert_eq!(gd_v1_close(b), 0);
    assert_eq!(gd_v1_close(b), 18);
    v["data"].clone()
}
fn setup(n: u64, threshold: u64) -> (std::path::PathBuf, u64, u64) {
    let p = fresh("ffi");
    let a = open("create", &p);
    for i in 0..n {
        call(a, json!({"command":"seed","key":key(i),"row":row()}), 0);
    }
    let mut g = grant();
    g.policy.approval_above = threshold;
    call(a, json!({"command":"grant","grant":g}), 0);
    let mut e = 0;
    assert_eq!(unsafe { gd_v1_executor(a, 1, 2, &mut e) }, 0);
    (p, a, e)
}
fn execute(e: u64, r: Request, id: Option<u64>, code: u32) -> Value {
    call(
        e,
        json!({"command":"execute","request":r,"time":{"Certified":10},"approval":id}),
        code,
    )
}
#[test]
fn version_null_lengths_utf8_and_invalid_json() {
    let _serial = SERIAL.lock().unwrap();
    unsafe {
        assert_eq!(gd_v1_abi_version(), 1);
        let mut out = 42;
        assert_eq!(gd_v1_admin_open(null(), 1, &mut out), 18);
        assert_eq!(out, 0);
        assert_eq!(gd_v1_admin_open(null(), usize::MAX, &mut out), 16);
        for b in [
            b"null".to_vec(),
            vec![0xff],
            b"{}".to_vec(),
            br#"{"mode":"create","path":"x","extra":1}"#.to_vec(),
        ] {
            assert_eq!(gd_v1_admin_open(b.as_ptr(), b.len(), &mut out), 17);
            assert_eq!(out, 0);
        }
        assert_eq!(gd_v1_call(999, null(), 0, &mut out), 18);
        assert_eq!(gd_v1_close(u64::MAX), 18);
    }
}
#[test]
fn wrong_kind_stale_and_executor_cannot_admin() {
    let _serial = SERIAL.lock().unwrap();
    unsafe {
        let (_, a, e) = setup(1, 5);
        call(e, json!({"command":"grant","grant":grant()}), 17);
        call(e, json!({"command":"inspect"}), 17);
        let mut o = 99;
        assert_eq!(gd_v1_executor(e, 1, 2, &mut o), 18);
        assert_eq!(o, 0);
        assert_eq!(gd_v1_executor(a, 1, 999, &mut o), 100);
        let s = br#"{"command":"metadata"}"#;
        let mut b = 0;
        assert_eq!(gd_v1_call(a, s.as_ptr(), s.len(), &mut b), 0);
        assert_eq!(gd_v1_call(b, s.as_ptr(), s.len(), &mut o), 18);
        assert_eq!(gd_v1_executor(b, 1, 2, &mut o), 18);
        let mut n = 10;
        assert_eq!(gd_v1_buffer_copy(a, null_mut(), 0, &mut n), 18);
        assert_eq!(n, 0);
        assert_eq!(gd_v1_close(b), 0);
        assert_eq!(gd_v1_close(a), 0);
        assert_eq!(gd_v1_executor(a, 1, 2, &mut o), 18);
        execute(e, request(1, &[0]), None, 0);
        assert_eq!(gd_v1_close(e), 0);
        assert_eq!(gd_v1_close(e), 18);
    }
}
#[test]
fn copied_buffers_are_owned_no_partial_copy_and_survive_source_close() {
    let _serial = SERIAL.lock().unwrap();
    unsafe {
        let (_, a, e) = setup(1, 5);
        let s = br#"{"command":"metadata"}"#;
        let mut b = 0;
        assert_eq!(gd_v1_call(a, s.as_ptr(), s.len(), &mut b), 0);
        assert_eq!(gd_v1_close(a), 0);
        assert_eq!(gd_v1_close(e), 0);
        let mut n = 0;
        assert_eq!(gd_v1_buffer_copy(b, null_mut(), 0, &mut n), 20);
        let mut sentinel = vec![0xAA; n];
        let mut needed = 0;
        assert_eq!(
            gd_v1_buffer_copy(b, sentinel.as_mut_ptr(), n - 1, &mut needed),
            20
        );
        assert_eq!(needed, n);
        assert!(sentinel.iter().all(|b| *b == 0xAA));
        assert_eq!(gd_v1_buffer_copy(b, null_mut(), n, &mut needed), 18);
        assert_eq!(
            gd_v1_buffer_copy(b, sentinel.as_mut_ptr(), usize::MAX, &mut needed),
            16
        );
        assert_eq!(
            gd_v1_buffer_copy(b, sentinel.as_mut_ptr(), n, &mut needed),
            0
        );
        assert_eq!(gd_v1_close(b), 0);
        assert_eq!(
            serde_json::from_slice::<Value>(&sentinel).unwrap()["version"],
            1
        );
    }
}
#[test]
fn exact_approval_typed_sql_status_retry_and_reopen() {
    let _serial = SERIAL.lock().unwrap();
    let (p, a, e) = setup(6, 5);
    let r = predicate(1);
    let preview = call(
        e,
        json!({"command":"preview","request":r,"time":{"Certified":10}}),
        0,
    );
    let id=call(a,json!({"command":"approve","principal":3,"preview":preview["preview"],"time":{"Certified":10}}),0).as_u64().unwrap();
    let receipt = execute(e, r.clone(), Some(id), 0);
    assert_eq!(receipt["effects"].as_array().unwrap().len(), 6);
    assert_eq!(
        call(
            e,
            json!({"command":"sql","operation":1,"sql":"UPDATE deliveries SET status = 'queued' WHERE tenant = 42 AND status = 'failed' AND failed_at >= 100 AND failed_at < 107","time":"Uncertain","approval":null}),
            0
        ),
        receipt
    );
    assert_eq!(execute(e, r, None, 0), receipt);
    assert_eq!(
        call(e, json!({"command":"status","operation":1}), 0),
        receipt
    );
    assert_eq!(gd_v1_close(e), 0);
    assert_eq!(gd_v1_close(a), 0);
    let a = open("open", &p);
    let mut e = 0;
    assert_eq!(unsafe { gd_v1_executor(a, 1, 2, &mut e) }, 0);
    assert_eq!(
        call(e, json!({"command":"status","operation":1}), 0),
        receipt
    );
    execute(e, request(1, &[0]), None, 116);
    call(e, json!({"command":"status","operation":99}), 1);
    assert_eq!(gd_v1_close(e), 0);
    assert_eq!(gd_v1_close(a), 0);
}
#[test]
fn preview_stale_revoke_and_denials_are_stable() {
    let _serial = SERIAL.lock().unwrap();
    let (_, a, e) = setup(1, 0);
    let r = predicate(1);
    let p = call(
        e,
        json!({"command":"preview","request":r,"time":{"Certified":10}}),
        0,
    );
    call(a, json!({"command":"seed","key":key(1),"row":row()}), 0);
    call(
        a,
        json!({"command":"approve","principal":3,"preview":p["preview"],"time":{"Certified":10}}),
        115,
    );
    execute(e, request(2, &[0]), None, 113);
    call(e, json!({"command":"status","operation":2}), 113);
    call(a, json!({"command":"revoke","task":1}), 0);
    execute(e, request(3, &[0]), None, 104);
    assert_eq!(gd_v1_close(e), 0);
    assert_eq!(gd_v1_close(a), 0);
}
#[test]
fn concurrent_same_operation_returns_identical_receipt_once() {
    let _serial = SERIAL.lock().unwrap();
    let (p, a, e) = setup(1, 5);
    let threads = (0..8)
        .map(|_| std::thread::spawn(move || execute(e, request(1, &[0]), None, 0)))
        .collect::<Vec<_>>();
    let results = threads
        .into_iter()
        .map(|t| t.join().unwrap())
        .collect::<Vec<_>>();
    assert!(results.iter().all(|r| *r == results[0]));
    assert_eq!(gd_v1_close(e), 0);
    assert_eq!(gd_v1_close(a), 0);
    assert_eq!(
        Admin::open(p, Limits::default())
            .unwrap()
            .snapshot()
            .unwrap()
            .tasks[&TaskId(1)]
            .transitions,
        1
    );
}
#[test]
fn buffer_resource_bound_rejects_before_action_and_recovers() {
    let _serial = SERIAL.lock().unwrap();
    unsafe {
        let (_, a, e) = setup(1, 5);
        let bytes = br#"{"command":"metadata"}"#;
        let mut held = Vec::new();
        loop {
            let mut b = 99;
            let code = gd_v1_call(a, bytes.as_ptr(), bytes.len(), &mut b);
            if code == 16 {
                assert_eq!(b, 0);
                break;
            }
            assert_eq!(code, 0);
            held.push(b);
            assert!(held.len() <= 1024);
        }
        assert!(!held.is_empty());
        for b in held {
            assert_eq!(gd_v1_close(b), 0);
        }
        execute(e, request(1, &[0]), None, 0);
        assert_eq!(gd_v1_close(e), 0);
        assert_eq!(gd_v1_close(a), 0);
    }
}
