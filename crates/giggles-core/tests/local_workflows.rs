mod common;
use common::*;
use giggles_core::*;
use serde_json::{Value, json};
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
        self.call(vec!["create".into(), self.db.display().to_string()], 0, 0);
    }
    fn admin(&mut self, v: Value, exit: i32, code: u32) -> Value {
        self.call(
            vec!["admin".into(), self.db.display().to_string(), v.to_string()],
            exit,
            code,
        )
    }
    fn exec(&mut self, v: Value, exit: i32, code: u32) -> Value {
        self.call(
            vec![
                "executor".into(),
                self.db.display().to_string(),
                "1".into(),
                "2".into(),
                v.to_string(),
            ],
            exit,
            code,
        )
    }
    fn setup(&mut self, n: u64, approval: u64, limit: u64) {
        self.create();
        for i in 0..n {
            self.admin(json!({"command":"seed","key":key(i),"row":row()}), 0, 0);
        }
        let mut g = grant();
        g.policy.approval_above = approval;
        g.policy.distinct_limit = limit;
        g.policy.transition_limit = limit;
        self.admin(json!({"command":"grant","grant":g}), 0, 0);
    }
    fn preview(&mut self, r: Request) -> String {
        self.exec(
            json!({"command":"preview","request":r,"time":{"Certified":10}}),
            0,
            0,
        )["preview"]
            .as_str()
            .unwrap()
            .into()
    }
    fn approve(&mut self, p: String, exit: i32, code: u32) -> Value {
        self.admin(
            json!({"command":"approve","principal":3,"preview":p,"time":{"Certified":10}}),
            exit,
            code,
        )
    }
    fn execute(&mut self, r: Request, a: Option<u64>, exit: i32, code: u32) -> Value {
        self.exec(
            json!({"command":"execute","request":r,"time":{"Certified":10},"approval":a}),
            exit,
            code,
        )
    }
}
#[test]
fn complete_exact_approval_restart_retry_export_restore() {
    let mut c = Cli::new();
    c.setup(6, 5, 25);
    let r = predicate(1);
    let p = c.preview(r.clone());
    let id = c.approve(p, 0, 0);
    let d = c.execute(r.clone(), Some(id.as_u64().unwrap()), 0, 0);
    assert_eq!(d["effects"].as_array().unwrap().len(), 6);
    assert_eq!(c.execute(r.clone(), None, 0, 0), d);
    assert_eq!(c.exec(json!({"command":"status","operation":1}), 0, 0), d);
    let file = c.root.join("export");
    c.call(
        vec![
            "export".into(),
            c.db.display().to_string(),
            file.display().to_string(),
        ],
        0,
        0,
    );
    c.call(vec!["verify".into(), file.display().to_string()], 0, 0);
    let restored = c.root.join("restored");
    c.call(
        vec![
            "restore".into(),
            file.display().to_string(),
            restored.display().to_string(),
        ],
        0,
        0,
    );
    c.call(
        vec![
            "restore".into(),
            file.display().to_string(),
            restored.display().to_string(),
        ],
        3,
        13,
    );
    c.db = restored;
    assert_eq!(c.exec(json!({"command":"status","operation":1}), 0, 0), d);
    c.execute(request(2, &[0]), None, 2, 104);
}
#[test]
fn stale_preview_then_repreview_and_approval() {
    let mut c = Cli::new();
    c.setup(2, 0, 25);
    let r = predicate(1);
    let p = c.preview(r.clone());
    c.admin(json!({"command":"seed","key":key(2),"row":row()}), 0, 0);
    c.approve(p, 2, 115);
    let p = c.preview(r.clone());
    let id = c.approve(p, 0, 0).as_u64().unwrap();
    c.execute(r, Some(id), 0, 0);
}
#[test]
fn revoked_task_and_approval_deny_and_persist() {
    let mut c = Cli::new();
    c.setup(1, 0, 25);
    let p = c.preview(request(1, &[0]));
    let id = c.approve(p, 0, 0).as_u64().unwrap();
    c.admin(json!({"command":"revoke_approval","approval":id}), 0, 0);
    c.execute(request(1, &[0]), Some(id), 2, 114);
    c.exec(json!({"command":"status","operation":1}), 2, 114);
    c.admin(json!({"command":"revoke","task":1}), 0, 0);
    c.execute(request(2, &[0]), None, 2, 104);
}
#[test]
fn scope_budget_and_missing_approval_exit_nonzero() {
    let mut c = Cli::new();
    c.setup(2, 5, 1);
    let mut r = request(1, &[0]);
    r.selection = Selection::Keys(vec![Key {
        tenant: TenantId(43),
        row: RowId(0),
    }]);
    c.execute(r, None, 2, 108);
    c.execute(request(2, &[0]), None, 0, 0);
    c.execute(request(3, &[1]), None, 2, 112);
    let mut c = Cli::new();
    c.setup(6, 5, 25);
    c.execute(predicate(1), None, 2, 113);
}
#[test]
fn repair_denial_regression_and_status_absent() {
    let mut c = Cli::new();
    c.setup(1, 5, 25);
    c.call(
        vec![
            "repair".into(),
            c.db.display().to_string(),
            "1".into(),
            "2".into(),
            "1".into(),
            "10".into(),
            "UPDATE deliveries SET status = 'queued' WHERE tenant = 43 AND id = 0".into(),
        ],
        2,
        108,
    );
    c.exec(json!({"command":"status","operation":1}), 2, 108);
    c.exec(json!({"command":"status","operation":999}), 3, 1);
}
#[test]
fn sql_and_typed_retry_share_binding() {
    let mut c = Cli::new();
    c.setup(2, 5, 25);
    let d = c.execute(request(1, &[0]), None, 0, 0);
    assert_eq!(c.exec(json!({"command":"sql","operation":1,"sql":"UPDATE deliveries SET status = 'queued' WHERE tenant = 42 AND id = 0","time":"Uncertain","approval":null}),0,0),d);
    c.execute(request(1, &[1]), None, 2, 116);
}
#[test]
fn lost_commit_response_resolves_through_cli_status_and_retry() {
    for point in [CrashPoint::ErrorBeforeCommit, CrashPoint::ErrorAfterCommit] {
        let mut c = Cli::new();
        c.setup(1, 5, 25);
        let a = Admin::open(&c.db, Limits::default()).unwrap();
        let e = a.executor(TaskId(1), Principal(2)).unwrap();
        a.set_crash_point(point).unwrap();
        assert!(matches!(
            e.execute(request(1, &[0]), Time::Certified(10), None),
            Err(CoreError::Storage(_))
        ));
        assert_eq!(a.snapshot(), Err(CoreError::Fenced));
        drop(e);
        drop(a);
        if point == CrashPoint::ErrorBeforeCommit {
            c.exec(json!({"command":"status","operation":1}), 3, 1);
        } else {
            c.exec(json!({"command":"status","operation":1}), 0, 0);
        }
        c.execute(request(1, &[0]), None, 0, 0);
        c.execute(request(1, &[0]), None, 0, 0);
        let a = Admin::open(&c.db, Limits::default()).unwrap();
        assert_eq!(a.snapshot().unwrap().tasks[&TaskId(1)].transitions, 1);
    }
}
#[test]
fn malformed_unknown_fields_time_and_unauthorized_approver_deny() {
    let mut c = Cli::new();
    c.setup(1, 5, 25);
    c.admin(json!({"command":"grant","grant":grant(),"extra":1}), 3, 17);
    c.exec(json!({"command":"revoke","task":1}), 3, 17);
    c.exec(
        json!({"command":"execute","request":request(1,&[0]),"time":"Uncertain","approval":null}),
        2,
        106,
    );
    let p = c.preview(request(2, &[0]));
    c.admin(
        json!({"command":"approve","principal":2,"preview":p,"time":{"Certified":10}}),
        2,
        114,
    );
    c.admin(
        json!({"command":"approve","principal":3,"preview":"not hex","time":{"Certified":10}}),
        3,
        16,
    );
}
#[test]
fn bounded_invalid_inputs_do_not_mutate_database() {
    let (_, a, e) = fixture(1);
    let before = a.snapshot().unwrap();
    for input in [
        vec![b'x'; local::MAX_INPUT + 1],
        vec![0xff],
        b"null".to_vec(),
        b"{}".to_vec(),
        b"{\"command\":\"seed\"}".to_vec(),
    ] {
        assert_ne!(local::admin(&a, &input).code, 0);
        assert_ne!(local::executor(&e, &input).code, 0);
    }
    assert_eq!(a.snapshot().unwrap(), before);
}
