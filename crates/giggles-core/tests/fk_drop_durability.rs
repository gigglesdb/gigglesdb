mod common;
use giggles_core::{Admin, Cancellation, CrashPoint, DecisionPoint, Engine, Limits, SqlSession, sqlstate};
use std::sync::Arc;

fn run(s: &mut SqlSession, sql: &str) {
    s.execute(sql, &[], None).unwrap_or_else(|e| panic!("{sql}: {e:?}"));
}
fn fixture(a: &Admin, constraint: bool) {
    let mut s = a.sql_session();
    run(&mut s, "create table p(id int not null)");
    run(&mut s, if constraint { "alter table p add constraint u unique(id)" }
        else { "create unique index s on p(id)" });
    for sql in ["alter table p add constraint pk primary key(id)",
        "create table c1(id int constraint f1 references p(id))",
        "create table c2(id int constraint f2 references p)",
        "insert into p values(1)", "insert into c1 values(1)", "insert into c2 values(1)"] {
        run(&mut s, sql);
    }
}
fn snapshot(a: &Admin) -> String {
    let s = a.sql_session();
    let m = s.catalog_metadata().unwrap();
    format!("{:?}", (m.tables, m.indexes, m.presentation, s.foreign_key_targets().unwrap()))
}
fn verify(a: &Admin, constraint: bool, removed: bool) {
    let mut s = a.sql_session();
    let targets = s.foreign_key_targets().unwrap();
    assert_eq!(targets.len(), if removed { 1 } else { 2 });
    assert_eq!(targets["f2"], "pk:pk");
    if !removed { assert_eq!(targets["f1"], if constraint { "index:u" } else { "index:s" }); }
    let m = s.catalog_metadata().unwrap();
    assert_eq!(m.indexes.iter().any(|i| i.name == if constraint { "u" } else { "s" }), !removed);
    if removed { run(&mut s, "insert into c1 values(999)"); }
    else { assert_eq!(sqlstate(&s.execute("insert into c1 values(999)", &[], None).unwrap_err()), "23503"); }
    assert_eq!(sqlstate(&s.execute("insert into c2 values(999)", &[], None).unwrap_err()), "23503");
}
fn exercise(engine: Engine) {
    for constraint in [false, true] {
        let path = common::fresh("fk-drop-durability");
        let a = Admin::create_engine(&path, Limits::default(), engine).unwrap();
        fixture(&a, constraint);
        let stmt = if constraint { "alter table p drop constraint u" } else { "drop index s" };
        let before = snapshot(&a);
        assert_eq!(sqlstate(&a.sql_session().execute(stmt, &[], None).unwrap_err()), "2BP01");
        drop(a);
        let a = Admin::open(&path, Limits::default()).unwrap();
        assert_eq!(snapshot(&a), before);
        verify(&a, constraint, false);
        a.set_decision_hook(Some(Arc::new(|point| {
            if point == DecisionPoint::BeforeDecision {
                assert!(Cancellation::current().unwrap().cancel());
            }
        })));
        let cascade = format!("{stmt} cascade");
        let cancel = Cancellation::default();
        // V2 has no kernel decision callback; its public token tests admission.
        // Legacy cancels after candidate mutation, immediately before publication.
        if engine == Engine::V2 { assert!(cancel.cancel()); }
        let error = cancel.scope(|| a.sql_session().execute(&cascade, &[], None)).unwrap_err();
        a.set_decision_hook(None);
        assert_eq!(sqlstate(&error), "57014");
        assert_eq!(snapshot(&a), before);
        verify(&a, constraint, false);
        if engine == Engine::V2 && !constraint {
            let mut s = a.sql_session();
            run(&mut s, "begin");
            run(&mut s, "drop index s cascade");
            run(&mut s, "rollback");
            assert_eq!(snapshot(&a), before);
            verify(&a, constraint, false);
        }
        run(&mut a.sql_session(), &cascade);
        drop(a);
        let a = Admin::open(&path, Limits::default()).unwrap();
        verify(&a, constraint, true);
        drop(a);
        std::fs::remove_dir_all(path).unwrap();
    }
}
#[test]
fn legacy_cancel_and_reopen() { exercise(Engine::Legacy); }
#[cfg(feature = "engine-rocksdb")]
#[test]
fn v2_cancel_and_reopen() { exercise(Engine::V2); }

// Default-feature seams abort the process on either side of the catalog commit.
#[test]
fn crash_child() {
    let Some(path) = std::env::var_os("FK_DROP_CRASH_PATH") else { return; };
    let a = Admin::open(path, Limits::default()).unwrap();
    let point = if std::env::var("FK_DROP_CRASH_AFTER").unwrap() == "true" {
        CrashPoint::AfterCommit
    } else { CrashPoint::BeforeCommit };
    a.set_crash_point(point).unwrap();
    run(&mut a.sql_session(), "drop index s cascade");
    panic!("crash hook did not fire");
}
#[test]
fn crash_recovers_whole_catalog() {
    for engine in [Engine::Legacy, #[cfg(feature = "engine-rocksdb")] Engine::V2] {
    for after in [false, true] {
        let path = common::fresh("fk-drop-crash");
        let a = Admin::create_engine(&path, Limits::default(), engine).unwrap();
        fixture(&a, false);
        drop(a);
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "fk_drop_durability::crash_child", "--nocapture"])
            .env("FK_DROP_CRASH_PATH", &path).env("FK_DROP_CRASH_AFTER", after.to_string())
            .output().unwrap();
        assert_eq!(output.status.code(), Some(86), "{output:?}");
        let a = Admin::open(&path, Limits::default()).unwrap();
        verify(&a, false, after);
        drop(a);
        std::fs::remove_dir_all(path).unwrap();
    }
    }
}
