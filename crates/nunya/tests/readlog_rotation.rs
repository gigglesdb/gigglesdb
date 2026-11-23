use nunya::{
    readlog::{FileLog, Outcome, ReadLog, ReadLogRecord, statement_fingerprint},
    token::{PrincipalId, ScopeId, SessionId},
};
use std::{fs, path::Path, time::{SystemTime, UNIX_EPOCH}};

fn statement_at(path: &Path) -> String {
    let line = fs::read_to_string(path).unwrap();
    let record: serde_json::Value = serde_json::from_str(&line).unwrap();
    record["statement"].as_str().unwrap().to_owned()
}

#[test]
fn file_log_keeps_three_ordered_predecessors() {
    let directory = std::env::temp_dir().join(format!(
        "nunya-readlog-rotation-{}-{}",
        std::process::id(),
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
    ));
    fs::create_dir(&directory).unwrap();
    let path = directory.join("read.log");
    let log = FileLog::open(&path, 1, 3).unwrap();
    for timestamp in 1..=6 {
        let statement = format!("SELECT {timestamp}");
        log.write(&ReadLogRecord {
            ts_unix_ms: timestamp,
            principal: PrincipalId([1; 16]),
            session: SessionId([2; 16]),
            scope: ScopeId([3; 16]),
            connection: None,
            credential_id: None,
            route: "postgres".into(),
            policy_revision: None,
            config_revision: None,
            statement_fingerprint: statement_fingerprint(&statement),
            statement,
            outcome: Outcome::Allowed,
            rows: 1,
            tokens_released: vec![],
        }).unwrap();
    }
    assert_eq!(statement_at(&path), "SELECT 6");
    for number in 1..=3 {
        let backup = directory.join(format!("read.log.{number}"));
        assert_eq!(statement_at(&backup), format!("SELECT {}", 6 - number));
    }
    assert_eq!(fs::read_dir(&directory).unwrap().count(), 4);
    drop(log);
    fs::remove_dir_all(directory).unwrap();
}
