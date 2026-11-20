mod common;
use giggles_core::{Admin, Engine, Limits, SqlSession};

fn query(s: &mut SqlSession, sql: &str) -> giggles_core::relational::SqlResult {
    let clock = giggles_core::relational::StatementClock {
        unavailable: None,
        transaction_us: 100,
        statement_us: 100,
        zone: None,
        wall: None,
    };
    s.execute_at(sql, &[], None, &clock)
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
}
fn run(s: &mut SqlSession, sql: &str) {
    query(s, sql);
}
fn owners(s: &SqlSession) -> Vec<(String, Option<String>)> {
    s.sequence_owners().unwrap()
}

#[test]
fn sequence_owners_follow_rename_reassignment_detach_drop_and_private_ddl() {
    for engine in [Engine::Legacy, Engine::V2] {
        if engine == Engine::V2 && !cfg!(feature = "engine-rocksdb") {
            continue;
        }
        let path = common::fresh("sequence-owner-fast-path");
        let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
        let mut s = admin.sql_session();
        run(&mut s, "CREATE TABLE a (id BIGINT PRIMARY KEY)");
        run(&mut s, "CREATE TABLE b (id BIGINT PRIMARY KEY)");
        run(&mut s, "CREATE SEQUENCE ids OWNED BY a.id");
        assert_eq!(owners(&s), vec![("ids".into(), Some("a".into()))]);
        run(&mut s, "ALTER TABLE a RENAME TO renamed");
        assert_eq!(owners(&s), vec![("ids".into(), Some("renamed".into()))]);
        run(&mut s, "BEGIN");
        run(&mut s, "ALTER SEQUENCE ids OWNED BY b.id");
        assert_eq!(owners(&s), vec![("ids".into(), Some("b".into()))]);
        assert_eq!(
            owners(&admin.sql_session()),
            vec![("ids".into(), Some("renamed".into()))]
        );
        run(&mut s, "ALTER SEQUENCE ids OWNED BY NONE");
        assert_eq!(owners(&s), vec![("ids".into(), None)]);
        run(&mut s, "COMMIT");
        run(&mut s, "DROP TABLE b");
        assert_eq!(owners(&s), vec![("ids".into(), None)]);
        run(&mut s, "ALTER SEQUENCE ids OWNED BY renamed.id");
        run(&mut s, "DROP TABLE renamed");
        assert!(owners(&s).is_empty());
    }
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn empty_pending_read_only_commit_and_nonempty_commit_reopen() {
    use giggles_core::relational::Value;
    let path = common::fresh("sequence-pending-fast-path");
    {
        let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
        let mut s = admin.sql_session();
        run(&mut s, "BEGIN");
        run(&mut s, "SELECT 1");
        run(&mut s, "COMMIT");
        // A private sequence uses pending counters, unlike already committed
        // nextval reservations (which are deliberately nontransactional).
        run(&mut s, "BEGIN");
        run(&mut s, "CREATE SEQUENCE ids CACHE 1");
        assert_eq!(
            query(&mut s, "SELECT nextval('ids')").rows[0].values[0],
            Value::Int(1)
        );
        run(&mut s, "COMMIT");
    }
    let admin = Admin::open(&path, Limits::default()).unwrap();
    // Reopen may skip reserved values; the committed counter must never rewind.
    assert!(matches!(
        query(&mut admin.sql_session(), "SELECT nextval('ids')").rows[0].values[0],
        Value::Int(value) if value > 1
    ));
}
