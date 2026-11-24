mod common;

use giggles_core::relational::Value;
use giggles_core::{Admin, CoreError, Engine, Limits, SqlSession, sqlstate};

fn refuse(s: &mut SqlSession, sql: &str, command: &str) {
    let error = s.execute(sql, &[], None).unwrap_err();
    assert_eq!(sqlstate(&error), "0A000", "{sql}: {error:?}");
    let CoreError::RoutineDdl(diagnostic) = error else {
        panic!("{sql}: {error:?}");
    };
    assert_eq!(
        diagnostic.message,
        format!("{command} on a table with triggers is not supported"),
    );
}

#[test]
fn unimplemented_trigger_write_leaves_target_and_cascaded_rows_intact() {
    let path = common::fresh("trigger-write-refusal");
    let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
    let mut session = admin.sql_session();
    let mut run = |sql| session.execute(sql, &[], None).unwrap();
    run("CREATE TABLE parent (id int PRIMARY KEY)");
    run(
        "CREATE TABLE child (id int PRIMARY KEY, p int REFERENCES parent(id) ON DELETE CASCADE, v int)",
    );
    run("INSERT INTO parent VALUES (1)");
    run("INSERT INTO child VALUES (2, 1, 0)");
    run("CREATE FUNCTION f() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$");
    run(
        "CREATE TRIGGER before_child BEFORE INSERT OR UPDATE OF v OR DELETE ON child FOR EACH ROW EXECUTE FUNCTION f()",
    );
    assert_eq!(run("UPDATE child SET p = p").affected, 1);
    drop(session);
    drop(admin);

    let admin = Admin::open(&path, Limits::default()).unwrap();
    let mut session = admin.sql_session();
    refuse(&mut session, "INSERT INTO child VALUES (3, 1, 0)", "INSERT");
    refuse(&mut session, "UPDATE child SET v = v", "UPDATE");
    refuse(&mut session, "DELETE FROM child WHERE id = 2", "DELETE");
    refuse(&mut session, "DELETE FROM parent WHERE id = 1", "DELETE");
    let mut run = |sql| session.execute(sql, &[], None).unwrap();
    assert_eq!(
        run("SELECT count(*) FROM parent").rows[0].values,
        vec![Value::Int(1)]
    );
    assert_eq!(
        run("SELECT count(*) FROM child").rows[0].values,
        vec![Value::Int(1)]
    );
    run("ALTER TABLE child DISABLE TRIGGER before_child");
    assert_eq!(run("DELETE FROM parent WHERE id = 1").affected, 1);
    assert_eq!(
        run("SELECT count(*) FROM child").rows[0].values,
        vec![Value::Int(0)]
    );
    drop(session);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}
