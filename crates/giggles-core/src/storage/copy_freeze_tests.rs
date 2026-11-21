//! Runtime marks follow SQL and native controls on both engines.
use super::*;
fn exercise(engine: Engine, native: bool) {
    let path = std::env::temp_dir().join(format!("freeze-{}-{}-{:?}", std::process::id(), native, engine));
    let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
    let mut s = admin.sql_session();
    let run = |s: &mut SqlSession, sql: &str| { s.execute(sql, &[], None).unwrap(); };
    let allowed = |s: &mut SqlSession, name: &str| {
        let (copy, _) = s.prepare_copy(&format!("COPY {name} FROM STDIN (FREEZE)")).unwrap();
        s.copy_freeze_allowed(&copy).unwrap()
    };
    let control = |s: &mut SqlSession, verb: &str, name: &str| {
        if native {
            match verb {
                "SAVEPOINT" => s.savepoint(name),
                "RELEASE" => s.release(name),
                "ROLLBACK TO" => s.rollback_to(name),
                _ => unreachable!(),
            }.unwrap();
        } else { run(s, &format!("{verb} {name}")); }
    };
    s.begin_native().unwrap();
    run(&mut s, "CREATE TABLE f(i int)");
    assert!(allowed(&mut s, "f"));
    control(&mut s, "SAVEPOINT", "a");
    assert!(!allowed(&mut s, "f"));
    run(&mut s, "TRUNCATE f");
    assert!(allowed(&mut s, "f"));
    control(&mut s, "ROLLBACK TO", "a");
    assert!(!allowed(&mut s, "f"));
    control(&mut s, "SAVEPOINT", "b");
    run(&mut s, "CREATE TABLE g(i int)");
    control(&mut s, "RELEASE", "a");
    assert!(allowed(&mut s, "f"));
    assert!(allowed(&mut s, "g"));
    run(&mut s, "CREATE TABLE ca AS SELECT i FROM f");
    assert!(allowed(&mut s, "ca"));
    run(&mut s, "SELECT i INTO si FROM f");
    assert!(allowed(&mut s, "si"));
    s.commit_native().unwrap();
    assert!(!allowed(&mut s, "f"));
    s.begin_native().unwrap();
    run(&mut s, "TRUNCATE f");
    assert!(allowed(&mut s, "f"));
    s.rollback_native().unwrap();
    s.begin_native().unwrap();
    assert!(!allowed(&mut s, "f"));
    s.rollback_native().unwrap();
    drop(s);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}
#[test]
fn legacy_sql_and_native() { for native in [false, true] { exercise(Engine::Legacy, native); } }
#[cfg(feature = "engine-rocksdb")]
#[test]
fn v2_sql_and_native() { for native in [false, true] { exercise(Engine::V2, native); } }
