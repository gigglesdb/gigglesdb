use super::*;
use relational::{Value, take_sql_error_message, take_sql_notices};

#[test]
fn view_and_comment_ddl_clear_previous_statement_diagnostics() {
    let f = Fixture::new(); let mut s = f.admin().sql_session();
    for sql in ["CREATE TABLE t(a int)", "CREATE VIEW v AS SELECT a FROM t", "CREATE VIEW w AS SELECT a FROM v"] { s.execute(sql, &[], None).unwrap(); }
    for sql in ["CREATE VIEW extra AS SELECT a FROM t", "DROP VIEW IF EXISTS extra", "COMMENT ON VIEW v IS 'clean'"] {
        assert_eq!(s.execute("DROP VIEW IF EXISTS missing,v", &[], None), Err(CoreError::Sql(SqlError::DependentObjects)));
        s.execute(sql, &[], None).unwrap();
        assert!(take_sql_notices().is_empty(), "{sql}");
    }
    s.execute("DROP VIEW IF EXISTS missing", &[], None).unwrap();
    assert_eq!(take_sql_notices().len(), 1);
}

#[test]
fn all_drop_view_targets_and_cascade_descendants_hold_statement_locks() {
    use engine::{locks::{LockKey, LockMode}, txn::TxnError};
    use std::{sync::mpsc, time::{Duration, Instant}};
    for drop in ["DROP VIEW v,w", "DROP VIEW v CASCADE"] {
        let f = Fixture::new(); let mut s = f.admin().sql_session();
        for sql in ["CREATE TABLE t(a int)", "CREATE VIEW v AS SELECT a FROM t", "CREATE VIEW w AS SELECT a FROM v", "BEGIN"] { s.execute(sql, &[], None).unwrap(); }
        let ids = f.parts().sql_views.keys().copied().collect::<Vec<_>>();
        s.execute(drop, &[], None).unwrap();
        // Immediate probes verify actual relation locks, independently of worker scheduling
        // and of the catalog-wide DDL barrier (which also blocks CREATE VIEW).
        for id in ids {
            let probe = f.db().begin(Isolation::ReadCommitted);
            assert!(matches!(probe.acquire_lock(LockKey::Table(id), LockMode::AccessShare, Some(Instant::now()), &|| false), Err(TxnError::LockTimeout)));
        }
        let admin = f.admin().clone(); let (tx, rx) = mpsc::channel();
        let worker = std::thread::spawn(move || tx.send(admin.sql_session().execute("CREATE VIEW late AS SELECT a FROM w", &[], None)).unwrap());
        assert!(rx.recv_timeout(Duration::from_millis(100)).is_err());
        s.execute("ROLLBACK", &[], None).unwrap();
        rx.recv_timeout(Duration::from_secs(10)).unwrap().unwrap(); worker.join().unwrap();
    }
}

#[test]
fn replace_view_retains_identity_authority_comments_and_dependent_bindings() {
    let mut f = Fixture::new(); let mut s = f.admin().sql_session();
    for sql in ["CREATE TABLE t(a int,b int,label varchar(8),wide varchar(9),big bigint)", "CREATE TABLE u(a int,b int)",
        "INSERT INTO t VALUES(1,2,'one','wide',3)", "INSERT INTO u VALUES(4,5)", "CREATE VIEW v(x) AS SELECT a FROM t",
        "CREATE VIEW w AS SELECT x FROM v", "COMMENT ON VIEW v IS 'view'", "COMMENT ON COLUMN v.x IS 'column'"] { s.execute(sql, &[], None).unwrap(); }
    f.admin().roles().create(roles::NewRole { name: "reader".into(), ..Default::default() }).unwrap();
    f.admin().roles().set_table_owner("v", "reader").unwrap();
    f.admin().roles().grant_table("v", "reader", &[roles::TablePrivilege::Select]).unwrap();
    let before = f.parts(); let (id, original) = before.sql_views.iter().find(|(_, v)| v.name == "v").unwrap();
    s.execute("CREATE OR REPLACE VIEW v(x,y) AS SELECT b,a FROM t", &[], None).unwrap();
    let after = f.parts();
    assert_eq!(after.sql_views[id].columns[0].id, original.columns[0].id);
    assert_ne!(after.sql_views[id].columns[0].source, original.columns[0].source);
    assert_eq!(after.comments, before.comments);
    assert_eq!(s.execute("SELECT * FROM w", &[], None).unwrap().rows[0].values, [Value::Int(2)]);
    assert_eq!(f.admin().roles().table_owner("v").unwrap().as_deref(), Some("reader"));
    assert!(s.table_privileges("v", "reader").unwrap().contains(&roles::TablePrivilege::Select));
    for (sql, message) in [("CREATE OR REPLACE VIEW v AS SELECT a,b FROM t", "cannot change name"),
        ("CREATE OR REPLACE VIEW v(x,y) AS SELECT big,a FROM t", "cannot change data type"),
        ("CREATE OR REPLACE VIEW v(x) AS SELECT a FROM t", "cannot drop columns")] {
        assert_eq!(s.execute(sql, &[], None), Err(CoreError::Sql(SqlError::InvalidTableDefinition)));
        assert!(take_sql_error_message().unwrap().starts_with(message));
        assert_eq!(f.parts().sql_views, after.sql_views);
    }
    s.execute("CREATE VIEW vm AS SELECT label FROM t", &[], None).unwrap();
    assert_eq!(s.execute("CREATE OR REPLACE VIEW vm(label) AS SELECT wide FROM t", &[], None), Err(CoreError::Sql(SqlError::InvalidTableDefinition)));
    assert_eq!(s.execute("CREATE OR REPLACE VIEW t AS SELECT * FROM u", &[], None), Err(CoreError::Sql(SqlError::WrongObjectType)));
    for sql in ["CREATE OR REPLACE VIEW absent AS SELECT * FROM u", "CREATE OR REPLACE VIEW v(x,y) AS SELECT b,a FROM u", "ALTER TABLE t DROP COLUMN b"] { s.execute(sql, &[], None).unwrap(); }
    assert_eq!(f.parts().sql_views[id].columns.iter().map(|c| c.id).collect::<Vec<_>>(), after.sql_views[id].columns.iter().map(|c| c.id).collect::<Vec<_>>());
    drop(s); f.reopen(); let mut s = f.admin().sql_session();
    assert_eq!(s.execute("SELECT * FROM w", &[], None).unwrap().rows[0].values, [Value::Int(5)]);
    assert_eq!(f.parts().comments, before.comments);
}
