use super::*;
use relational::{StatementClock, Value as V};

#[test]
fn clocked_execute_and_prepared_status() {
    check(false);
}

#[test]
fn clocked_cursor_status() {
    check(true);
}

fn check(cursor: bool) {
    let path = std::env::temp_dir().join(format!("v2-xact-clock-{}-{cursor}", std::process::id()));
    let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
    let mut s = admin.sql_session();
    let clock = StatementClock {
        unavailable: None,
        transaction_us: 100,
        statement_us: 100,
        zone: None,
        wall: None,
    };
    s.execute("CREATE TABLE xc(id bigint)", &[], None).unwrap();
    s.execute("BEGIN", &[], None).unwrap();
    s.execute("INSERT INTO xc VALUES(1)", &[], None).unwrap();
    let result = s
        .execute_at("SELECT pg_current_xact_id()", &[], None, &clock)
        .unwrap();
    let V::Int(xid) = result.rows[0].values[0] else {
        panic!("expected xid")
    };
    let p = s.prepare("SELECT pg_xact_status($1)").unwrap();
    let result = s
        .execute_prepared_at(&p, &[V::Int(xid)], None, &clock)
        .unwrap();
    assert_eq!(result.rows[0].values, vec![V::Text("in progress".into())]);
    s.execute("COMMIT", &[], None).unwrap();
    let p = s.prepare("SELECT pg_xact_status($1)").unwrap();
    let rows = if cursor {
        s.open_cursor_at(&p, &[V::Int(xid)], &clock)
            .unwrap()
            .fetch(16, 4096)
            .unwrap()
    } else {
        s.execute_prepared_at(&p, &[V::Int(xid)], None, &clock)
            .unwrap()
            .rows
    };
    assert_eq!(rows[0].values, vec![V::Text("committed".into())]);
    // A read-only transaction uses the streaming cursor rather than the write-overlay fallback.
    s.execute("BEGIN", &[], None).unwrap();
    let p = s.prepare("SELECT pg_current_xact_id()").unwrap();
    let rows = if cursor {
        s.open_cursor_at(&p, &[], &clock)
            .unwrap()
            .fetch(16, 4096)
            .unwrap()
    } else {
        s.execute_prepared_at(&p, &[], None, &clock).unwrap().rows
    };
    assert!(matches!(rows[0].values[0], V::Int(id) if id > xid));
    s.execute("ROLLBACK", &[], None).unwrap();
    drop(s);
    drop(admin);
    fs::remove_dir_all(path).unwrap();
}
