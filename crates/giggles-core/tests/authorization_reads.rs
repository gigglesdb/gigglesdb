mod common;
use giggles_core::{Admin, Limits};

#[test]
fn mutation_authorization_reads_are_syntactic_and_ignore_synthetic_stars() {
    let admin = Admin::create(common::fresh("mutation-authorization-reads"), Limits::default()).unwrap();
    let mut session = admin.sql_session();
    session.execute("CREATE TABLE app(id BIGINT PRIMARY KEY, body BIGINT)", &[], None).unwrap();
    session.execute("CREATE TABLE child(id BIGINT PRIMARY KEY)", &[], None).unwrap();
    for (sql, target_read, conflict_update) in [
        ("UPDATE app SET body=1", false, false),
        ("UPDATE app SET body=1+1 WHERE true", false, false),
        ("UPDATE app SET body=body+1", true, false),
        ("UPDATE app SET body=CASE WHEN id=1 THEN 2 ELSE 3 END", true, false),
        ("UPDATE app SET body=1 WHERE id=1", true, false),
        ("DELETE FROM app WHERE true", false, false),
        ("DELETE FROM app WHERE id=1", true, false),
        ("INSERT INTO app VALUES(2,1) RETURNING 1", false, false),
        ("UPDATE app SET body=1 RETURNING 1", false, false),
        ("DELETE FROM app RETURNING 1", false, false),
        ("INSERT INTO app VALUES(2,1) RETURNING id", true, false),
        ("UPDATE app SET body=1 RETURNING *", true, false),
        ("DELETE FROM app RETURNING abs(body)", true, false),
        ("INSERT INTO app VALUES(2,1) ON CONFLICT DO NOTHING", false, false),
        ("INSERT INTO app VALUES(2,1) ON CONFLICT(id) DO NOTHING", true, false),
        ("INSERT INTO app VALUES(2,1) ON CONFLICT ON CONSTRAINT app_pkey DO NOTHING", true, false),
        ("INSERT INTO app VALUES(2,1) ON CONFLICT(id) DO UPDATE SET body=1", true, true),
        ("INSERT INTO app VALUES(2,1) ON CONFLICT ON CONSTRAINT app_pkey DO UPDATE SET body=excluded.body", true, true),
        ("DELETE FROM app WHERE EXISTS(SELECT id FROM child)", false, false),
        ("DELETE FROM app WHERE id IN(SELECT id FROM child)", true, false),
    ] {
        let prepared = session.prepare(sql).unwrap();
        let tables = prepared.authorization_tables().unwrap();
        assert_eq!(tables.iter().any(|table| table == "app"), target_read, "{sql}: {tables:?}");
        assert_eq!(tables.iter().any(|table| table == "child"), sql.contains("FROM child"), "{sql}");
        // Execution dependencies remain separate from syntactic privilege reads.
        assert_eq!(prepared.read_tables(), if sql.contains("FROM child") { vec!["child"] } else { vec![] });
        assert_eq!(prepared.authorization_update_table(), conflict_update.then_some("app"), "{sql}");
    }
}
