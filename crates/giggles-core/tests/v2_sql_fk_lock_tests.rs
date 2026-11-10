#![cfg(feature = "engine-rocksdb")]
mod common;
use giggles_core::{Admin, CoreError, Engine, Limits, relational::SqlError};
use std::{path::PathBuf, sync::mpsc, thread, time::Duration};

struct Fixture {
    admin: Option<Admin>,
    path: PathBuf,
}
impl Fixture {
    fn new(fk: bool) -> Self {
        let path = common::fresh("v2-fk-lock");
        let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
        let mut s = admin.sql_session();
        s.execute("CREATE TABLE p(id int PRIMARY KEY)", &[], None)
            .unwrap();
        s.execute(
            if fk {
                "CREATE TABLE c(id int PRIMARY KEY, p int REFERENCES p(id))"
            } else {
                "CREATE TABLE c(id int PRIMARY KEY, p int)"
            },
            &[],
            None,
        )
        .unwrap();
        s.execute("INSERT INTO p VALUES(1)", &[], None).unwrap();
        drop(s);
        Self {
            admin: Some(admin),
            path,
        }
    }
    fn admin(&self) -> &Admin {
        self.admin.as_ref().unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.admin.take();
        std::fs::remove_dir_all(&self.path).unwrap();
    }
}

#[test]
fn unrelated_writers_do_not_wait_for_fk_transactions() {
    for first in ["INSERT INTO u VALUES(1)", "INSERT INTO c VALUES(1,1)"] {
        let f = Fixture::new(true);
        let mut s = f.admin().sql_session();
        s.execute("CREATE TABLE u(id int PRIMARY KEY)", &[], None)
            .unwrap();
        s.execute("CREATE TABLE v(id int PRIMARY KEY)", &[], None)
            .unwrap();
        s.execute("BEGIN", &[], None).unwrap();
        s.execute(first, &[], None).unwrap();
        let admin = f.admin().clone();
        let (tx, rx) = mpsc::channel();
        let worker = thread::spawn(move || {
            tx.send(
                admin
                    .sql_session()
                    .execute("INSERT INTO v VALUES(1)", &[], None),
            )
            .unwrap();
        });
        let result = rx.recv_timeout(Duration::from_secs(10));
        s.execute("ROLLBACK", &[], None).unwrap();
        worker.join().unwrap();
        result.expect("unrelated writer blocked").unwrap();
    }
}

fn conflicting(first: &'static str, second: &'static str, fk: bool) {
    let f = Fixture::new(fk);
    let mut s = f.admin().sql_session();
    s.execute("BEGIN", &[], None).unwrap();
    s.execute(first, &[], None).unwrap();
    let admin = f.admin().clone();
    let (started, ready) = mpsc::channel();
    let (tx, rx) = mpsc::channel();
    let worker = thread::spawn(move || {
        let mut other = admin.sql_session();
        started.send(()).unwrap();
        tx.send(other.execute(second, &[], None)).unwrap();
    });
    ready.recv_timeout(Duration::from_secs(10)).unwrap();
    let waiting = rx.recv_timeout(Duration::from_millis(100));
    s.execute("COMMIT", &[], None).unwrap();
    let result = match waiting {
        Ok(result) => result,
        Err(mpsc::RecvTimeoutError::Timeout) => rx.recv_timeout(Duration::from_secs(10)).unwrap(),
        Err(error) => panic!("worker disconnected: {error}"),
    };
    worker.join().unwrap();
    assert_eq!(
        result,
        Err(CoreError::Sql(SqlError::ForeignKeyViolation)),
        "{first}; {second}"
    );
    let children = s.execute("SELECT p FROM c", &[], None).unwrap().rows;
    let parents = s.execute("SELECT id FROM p", &[], None).unwrap().rows;
    if fk || first.starts_with("ALTER") {
        for child in children {
            assert!(
                parents.iter().any(|parent| parent.values == child.values),
                "orphan"
            );
        }
    }
}

#[test]
fn parent_delete_and_child_insert_cannot_orphan() {
    conflicting(
        "DELETE FROM p WHERE id=1",
        "INSERT INTO c VALUES(1,1)",
        true,
    );
    conflicting(
        "INSERT INTO c VALUES(1,1)",
        "DELETE FROM p WHERE id=1",
        true,
    );
}
#[test]
fn parent_key_update_and_child_insert_cannot_orphan() {
    conflicting(
        "UPDATE p SET id=2 WHERE id=1",
        "INSERT INTO c VALUES(1,1)",
        true,
    );
    conflicting(
        "INSERT INTO c VALUES(1,1)",
        "UPDATE p SET id=2 WHERE id=1",
        true,
    );
}
#[test]
fn adding_fk_and_child_writes_validate_after_waiting() {
    const ADD: &str = "ALTER TABLE c ADD FOREIGN KEY(p) REFERENCES p(id)";
    conflicting(ADD, "INSERT INTO c VALUES(1,99)", false);
    conflicting("INSERT INTO c VALUES(1,99)", ADD, false);
}
