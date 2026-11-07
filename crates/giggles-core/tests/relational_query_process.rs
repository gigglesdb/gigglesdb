use serde_json::{Value, json};
use std::{path::PathBuf, process::Command};
struct Cli {
    root: PathBuf,
    seq: usize,
}
impl Cli {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "gd-query-process-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).unwrap();
        Self { root, seq: 0 }
    }
    fn call(&mut self, request: Option<Value>, exit: i32, code: u32) -> Value {
        self.seq += 1;
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_giggles"));
        if let Some(v) = request {
            cmd.arg("admin")
                .arg(self.root.join("db"))
                .arg(v.to_string());
        } else {
            cmd.arg("create").arg(self.root.join("db"));
        }
        let out = cmd.output().unwrap();
        std::fs::write(self.root.join(format!("{}.stdout", self.seq)), &out.stdout).unwrap();
        std::fs::write(self.root.join(format!("{}.stderr", self.seq)), &out.stderr).unwrap();
        assert_eq!(out.status.code(), Some(exit), "{out:?}");
        let value: Value = serde_json::from_slice(&out.stdout).unwrap();
        assert_eq!(value["code"], code);
        value["data"].clone()
    }
}
#[test]
fn persistent_reopen_join_aggregate_json() {
    let mut c = Cli::new();
    c.call(None, 0, 0);
    c.call(Some(json!({"command":"sql_batch","statements":[{"sql":"BEGIN"},{"sql":"CREATE TABLE owners (id BIGINT PRIMARY KEY, label TEXT)"},{"sql":"CREATE TABLE entries (id BIGINT PRIMARY KEY, owner BIGINT, amount BIGINT)"},{"sql":"INSERT INTO owners VALUES(1,'persisted')"},{"sql":"INSERT INTO entries VALUES(1,1,12)"},{"sql":"INSERT INTO entries VALUES(2,1,-3)"},{"sql":"COMMIT"}]})),0,0);
    let r=c.call(Some(json!({"command":"sql","sql":"SELECT e.id,o.label FROM entries e JOIN owners o ON o.id=e.owner ORDER BY e.id LIMIT 10","params":[]})),0,0);
    assert_eq!(
        r["rows"][0]["values"],
        json!([{"Int":1},{"Text":"persisted"}])
    );
    assert_eq!(r["rows"][0]["version"], 0);
    let r=c.call(Some(json!({"command":"sql","sql":"SELECT COUNT(*),SUM(e.amount) FROM entries e JOIN owners o ON o.id=e.owner","params":[]})),0,0);
    assert_eq!(r["rows"][0]["values"], json!([{"Int":2},{"Numeric":"9"}]));
    let r=c.call(Some(json!({"command":"sql","sql":"EXPLAIN SELECT e.id FROM entries e JOIN owners o ON o.id=e.owner","params":[]})),0,0);
    assert!(
        r["rows"][0]["values"][0]["Text"]
            .as_str()
            .unwrap()
            .contains("InnerPrimaryKeyNestedLoop")
    );
}
#[test]
fn unsupported_query_never_writes() {
    let mut c = Cli::new();
    c.call(None, 0, 0);
    c.call(
        Some(
            json!({"command":"sql","sql":"CREATE TABLE kept (id BIGINT PRIMARY KEY)","params":[]}),
        ),
        0,
        0,
    );
    c.call(Some(json!({"command":"sql_batch","statements":[{"sql":"BEGIN"},{"sql":"INSERT INTO kept VALUES(1)"},{"sql":"EXPLAIN DELETE FROM kept WHERE id=1"},{"sql":"COMMIT"}]})),3,201);
    let r = c.call(
        Some(json!({"command":"sql","sql":"SELECT COUNT(*) FROM kept","params":[]})),
        0,
        0,
    );
    assert_eq!(r["rows"][0]["values"], json!([{"Int":0}]));
}
