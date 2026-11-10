use super::*;
use relational::Value;

fn error(s: &mut SqlSession, sql: &str, expected: SqlError) {
    assert_eq!(
        s.execute(sql, &[], None),
        Err(CoreError::Sql(expected)),
        "{sql}"
    );
}

#[test]
fn check_fk_validation_reopen_and_drop() {
    let mut f = Fixture::new();
    f.execute("CREATE TABLE p(id int PRIMARY KEY, u int UNIQUE)");
    f.execute("CREATE TABLE c(id int PRIMARY KEY, p int CONSTRAINT fk REFERENCES p(u), n int CONSTRAINT positive CHECK(n>0))");
    f.execute("INSERT INTO p VALUES(1,10)");
    f.execute("INSERT INTO c VALUES(1,10,1)");
    f.reopen();
    let mut s = f.admin().sql_session();
    error(
        &mut s,
        "INSERT INTO c VALUES(2,10,0)",
        SqlError::CheckViolation,
    );
    error(
        &mut s,
        "INSERT INTO c VALUES(2,99,1)",
        SqlError::ForeignKeyViolation,
    );
    error(&mut s, "DELETE FROM p", SqlError::ForeignKeyViolation);
    s.execute(
        "ALTER TABLE c ADD CONSTRAINT too_large CHECK(n>2) NOT VALID",
        &[],
        None,
    )
    .unwrap();
    error(
        &mut s,
        "ALTER TABLE c VALIDATE CONSTRAINT too_large",
        SqlError::CheckViolation,
    );
    s.execute("ALTER TABLE c DROP CONSTRAINT too_large", &[], None)
        .unwrap();
    error(
        &mut s,
        "ALTER TABLE c ADD CONSTRAINT too_large CHECK(n>2)",
        SqlError::CheckViolation,
    );
    s.execute("CREATE TABLE bad(id int PRIMARY KEY, p int)", &[], None)
        .unwrap();
    s.execute("INSERT INTO bad VALUES(1,99)", &[], None)
        .unwrap();
    error(
        &mut s,
        "ALTER TABLE bad ADD FOREIGN KEY(p) REFERENCES p(u)",
        SqlError::ForeignKeyViolation,
    );
    s.execute(
        "ALTER TABLE bad ADD CONSTRAINT bad_fk FOREIGN KEY(p) REFERENCES p(u) NOT VALID",
        &[],
        None,
    )
    .unwrap();
    error(
        &mut s,
        "ALTER TABLE bad VALIDATE CONSTRAINT bad_fk",
        SqlError::ForeignKeyViolation,
    );
    s.execute("UPDATE bad SET p=10", &[], None).unwrap();
    s.execute("ALTER TABLE bad VALIDATE CONSTRAINT bad_fk", &[], None)
        .unwrap();
}

#[test]
fn cascade_restrict_savepoints_and_deferral() {
    let f = Fixture::new();
    f.execute("CREATE TABLE p(id int PRIMARY KEY)");
    f.execute("CREATE TABLE c(id int PRIMARY KEY, p int REFERENCES p(id) ON DELETE CASCADE ON UPDATE CASCADE DEFERRABLE INITIALLY DEFERRED)");
    f.execute("CREATE INDEX c_p ON c(p)");
    let mut s = f.admin().sql_session();
    for sql in ["BEGIN", "INSERT INTO c VALUES(1,1)", "SAVEPOINT a"] {
        s.execute(sql, &[], None).unwrap();
    }
    error(
        &mut s,
        "SET CONSTRAINTS ALL IMMEDIATE",
        SqlError::ForeignKeyViolation,
    );
    for sql in [
        "ROLLBACK TO a",
        "INSERT INTO p VALUES(1)",
        "COMMIT",
        "BEGIN",
        "SAVEPOINT a",
        "UPDATE p SET id=2",
        "ROLLBACK TO a",
        "COMMIT",
    ] {
        s.execute(sql, &[], None).unwrap();
    }
    assert_eq!(
        s.execute("SELECT p FROM c", &[], None).unwrap().rows[0].values[0],
        Value::Int(1)
    );
    s.execute("UPDATE p SET id=2", &[], None).unwrap();
    assert_eq!(
        s.execute("SELECT p FROM c", &[], None).unwrap().rows[0].values[0],
        Value::Int(2)
    );
    for sql in ["BEGIN", "DELETE FROM p", "ROLLBACK"] {
        s.execute(sql, &[], None).unwrap();
    }
    assert_eq!(
        s.execute("SELECT p FROM c", &[], None).unwrap().rows.len(),
        1
    );
    s.execute("DELETE FROM p", &[], None).unwrap();
    assert!(
        s.execute("SELECT * FROM c", &[], None)
            .unwrap()
            .rows
            .is_empty()
    );
    for sql in ["BEGIN", "INSERT INTO c VALUES(2,9)"] {
        s.execute(sql, &[], None).unwrap();
    }
    error(&mut s, "COMMIT", SqlError::ForeignKeyViolation);
    assert!(
        s.execute("SELECT * FROM c", &[], None)
            .unwrap()
            .rows
            .is_empty()
    );
    s.execute(
        "CREATE TABLE r(id int PRIMARY KEY, p int REFERENCES p(id) ON DELETE RESTRICT)",
        &[],
        None,
    )
    .unwrap();
    s.execute("INSERT INTO p VALUES(1)", &[], None).unwrap();
    s.execute("INSERT INTO r VALUES(1,1)", &[], None).unwrap();
    error(&mut s, "DELETE FROM p", SqlError::RestrictViolation);
}

#[test]
fn concurrent_parent_delete_child_insert_cannot_orphan() {
    for delete_first in [true, false] {
        let f = Fixture::new();
        f.execute("CREATE TABLE p(id int PRIMARY KEY)");
        f.execute("CREATE TABLE c(id int PRIMARY KEY, p int REFERENCES p(id))");
        f.execute("INSERT INTO p VALUES(1)");
        let mut first = f.admin().sql_session();
        first.execute("BEGIN", &[], None).unwrap();
        first
            .execute(
                if delete_first {
                    "DELETE FROM p"
                } else {
                    "INSERT INTO c VALUES(1,1)"
                },
                &[],
                None,
            )
            .unwrap();
        let admin = f.admin().clone();
        let (tx, rx) = std::sync::mpsc::channel();
        let (done, result) = std::sync::mpsc::channel();
        let join = std::thread::spawn(move || {
            tx.send(()).unwrap();
            done.send(admin.sql_session().execute(
                if delete_first {
                    "INSERT INTO c VALUES(1,1)"
                } else {
                    "DELETE FROM p"
                },
                &[],
                None,
            ))
            .unwrap();
        });
        rx.recv_timeout(std::time::Duration::from_secs(10)).unwrap();
        assert!(matches!(
            result.recv_timeout(std::time::Duration::from_millis(100)),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout)
        ));
        first.execute("COMMIT", &[], None).unwrap();
        assert_eq!(
            result
                .recv_timeout(std::time::Duration::from_secs(10))
                .unwrap(),
            Err(CoreError::Sql(SqlError::ForeignKeyViolation))
        );
        join.join().unwrap();
    }
}

#[test]
fn cascade_default_loads_other_parent_without_replaying_sequence() {
    let f = Fixture::new();
    let mut s = f.admin().sql_session();
    let clock = relational::StatementClock {
        unavailable: None,
        transaction_us: 1,
        statement_us: 1,
        zone: None,
        wall: None,
    };
    for sql in [
        "CREATE SEQUENCE defaults_seq",
        "CREATE TABLE p(id int PRIMARY KEY)",
        "CREATE TABLE q(id int PRIMARY KEY)",
        "INSERT INTO p VALUES(1),(9)",
        "INSERT INTO q VALUES(1),(9)",
        "CREATE TABLE c(id int PRIMARY KEY, p int DEFAULT nextval('defaults_seq') REFERENCES p(id) ON DELETE SET DEFAULT, FOREIGN KEY(p) REFERENCES q(id))",
        "INSERT INTO c VALUES(1,9)",
        "DELETE FROM p WHERE id=9",
    ] {
        s.execute_at(sql, &[], None, &clock)
            .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    }
    assert_eq!(
        s.execute_at("SELECT p FROM c", &[], None, &clock)
            .unwrap()
            .rows[0]
            .values,
        [Value::Int(1)]
    );
    assert_eq!(
        s.execute_at("SELECT nextval('defaults_seq')", &[], None, &clock)
            .unwrap()
            .rows[0]
            .values,
        [Value::Int(2)]
    );
}
