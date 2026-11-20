//! Native request metadata visibility and cascade equivalence regressions.
use super::*;
use crate::operations::{self, Bindings, Expr, Request, ValueExpr};
use relational::Value::{self, Int};
use std::cell::RefCell;
use std::path::PathBuf;
use std::rc::Rc;

struct Fixture {
    admin: Option<Admin>,
    path: PathBuf,
}
impl Fixture {
    fn new() -> Self {
        let mut token = [0; 16];
        getrandom::fill(&mut token).unwrap();
        let path = std::env::temp_dir().join(format!("f2a-{:032x}", u128::from_be_bytes(token)));
        let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
        Self {
            admin: Some(admin),
            path,
        }
    }
    fn admin(&self) -> &Admin {
        self.admin.as_ref().unwrap()
    }
    fn sql(&self, sql: &str) -> relational::SqlResult {
        self.admin()
            .sql_session()
            .execute(sql, &[], None)
            .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
    }
    fn contents(&self, tables: &[&str]) -> Vec<Vec<Vec<Value>>> {
        tables
            .iter()
            .map(|table| {
                self.sql(&format!("SELECT * FROM {table} ORDER BY id"))
                    .rows
                    .into_iter()
                    .map(|r| r.values)
                    .collect()
            })
            .collect()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.admin.take();
        let _ = fs::remove_dir_all(&self.path);
    }
}
fn literal(value: Value) -> Expr {
    Expr::Value(ValueExpr::Literal(value))
}
fn delete(table: &str) -> Request {
    Request::Delete {
        table: table.into(),
        filter: literal(Value::Bool(true)),
        guard: None,
        cap: Some(ValueExpr::Literal(Int(100))),
    }
}
fn many() -> Request {
    Request::InsertMany {
        table: "p".into(),
        columns: vec!["id".into()],
        rows: vec![vec![literal(Int(1))], vec![literal(Int(2))]],
        cap: ValueExpr::Literal(Int(2)),
    }
}
type PreparedHook = Box<dyn FnMut(&relational::Relational)>;
thread_local! {
    pub(super) static INSERT_PREPARED: RefCell<Option<PreparedHook>> = const { RefCell::new(None) };
}
struct Hooks;
impl Drop for Hooks {
    fn drop(&mut self) {
        operations::NATIVE_MANY_AFTER_NORMALIZE.with_borrow_mut(|h| *h = None);
        operations::NATIVE_MANY_BEFORE_WRITE.with_borrow_mut(|h| *h = None);
        INSERT_PREPARED.with_borrow_mut(|h| *h = None);
    }
}

#[test]
fn metadata_revalidates_ddl_committed_between_normalized_rows() {
    for isolation in [
        None,
        Some(SqlIsolation::ReadCommitted),
        Some(SqlIsolation::RepeatableRead),
        Some(SqlIsolation::Serializable),
    ] {
        let fixed = isolation.is_some_and(|level| level != SqlIsolation::ReadCommitted);
        let f = Fixture::new();
        let _hooks = Hooks;
        f.sql("CREATE TABLE p(id bigint PRIMARY KEY)");
        let admin = f.admin().clone();
        operations::NATIVE_MANY_AFTER_NORMALIZE.with_borrow_mut(|h| {
            *h = Some(Box::new(move |_, row| {
                if row == 1 {
                    admin
                        .sql_session()
                        .execute("CREATE TABLE marker(id bigint PRIMARY KEY)", &[], None)
                        .unwrap();
                }
            }))
        });
        let observed = Rc::new(RefCell::new(Vec::new()));
        let observations = observed.clone();
        INSERT_PREPARED.with_borrow_mut(|h| {
            *h = Some(Box::new(move |image| {
                // Observe the exact image supplied to insert_normalized_engine.
                observations
                    .borrow_mut()
                    .push(image.tables.contains_key("marker"));
            }))
        });
        let mut session = f.admin().sql_session();
        if let Some(level) = isolation {
            session.set_isolation(level);
            session.execute("BEGIN", &[], None).unwrap();
        }
        session.execute_native(&many(), &Bindings::new()).unwrap();
        if isolation.is_some() {
            session.execute("COMMIT", &[], None).unwrap();
        }
        assert_eq!(
            *observed.borrow(),
            if fixed {
                vec![false, false]
            } else {
                vec![false, true]
            }
        );
        drop(session);
        drop(_hooks);
    }
}

#[test]
fn private_ddl_and_native_requests_share_the_overlay() {
    let f = Fixture::new();
    let mut session = f.admin().sql_session();
    session.execute("BEGIN", &[], None).unwrap();
    session
        .execute("CREATE TABLE p(id bigint PRIMARY KEY)", &[], None)
        .unwrap();
    session.execute_native(&many(), &Bindings::new()).unwrap();
    assert_eq!(
        session
            .execute("SELECT * FROM p", &[], None)
            .unwrap()
            .rows
            .len(),
        2
    );
    session.execute("ROLLBACK", &[], None).unwrap();
}

#[test]
fn cascades_and_errors_match_the_per_row_publisher() {
    for action in [
        "CASCADE",
        "SET NULL",
        "RESTRICT",
        "NO ACTION",
        "NO ACTION DEFERRABLE INITIALLY DEFERRED",
    ] {
        for grandchildren in [false, true] {
            let mut results = Vec::new();
            for baseline in [true, false] {
                let _hooks = Hooks;
                let f = Fixture::new();
                f.sql("CREATE TABLE p(id bigint PRIMARY KEY)");
                f.sql(&format!("CREATE TABLE c(id bigint PRIMARY KEY,p bigint REFERENCES p(id) ON DELETE {action})"));
                f.sql("CREATE TABLE other(id bigint PRIMARY KEY)");
                f.sql("INSERT INTO p VALUES(1),(2)");
                f.sql("INSERT INTO c VALUES(10,1),(20,2)");
                f.sql("INSERT INTO other VALUES(99)");
                let mut tables = vec!["p", "c", "other"];
                if grandchildren {
                    f.sql("CREATE TABLE g(id bigint PRIMARY KEY,c bigint REFERENCES c(id) ON DELETE CASCADE)");
                    f.sql("INSERT INTO g VALUES(100,10),(200,20)");
                    tables.push("g");
                }
                let summary = if baseline {
                    let mut session = f.admin().sql_session();
                    session.execute("BEGIN", &[], None).unwrap();
                    let mut totals = (0, 0, 0);
                    let result = (1..=2).try_for_each(|id| {
                        let request = Request::Delete {
                            table: "p".into(),
                            filter: Expr::Compare(
                                Box::new(Expr::Field("id".into())),
                                operations::Compare::Eq,
                                Box::new(literal(Int(id))),
                            ),
                            guard: None,
                            cap: Some(ValueExpr::Literal(Int(1))),
                        };
                        session.execute_native(&request, &Bindings::new()).map(|o| {
                            totals.0 += o.matched;
                            totals.1 += o.affected;
                            totals.2 += o.changed;
                        })
                    });
                    if result.is_ok() {
                        session
                            .execute("COMMIT", &[], None)
                            .map(|_| totals)
                            .map_err(|error| operations::Error::Sql {
                                sqlstate: crate::sqlstate(&error).into(),
                            })
                    } else {
                        session.execute("ROLLBACK", &[], None).unwrap();
                        result.map(|()| totals)
                    }
                } else {
                    operations::Engine::new(f.admin().clone())
                        .execute(&delete("p"), &Bindings::new())
                        .map(|o| (o.matched, o.affected, o.changed))
                }
                .map_err(|e| e.sqlstate().map(str::to_owned));
                results.push((summary, f.contents(&tables)));
            }
            assert_eq!(
                results[0], results[1],
                "{action}, grandchildren={grandchildren}"
            );
        }
    }
}

#[test]
fn self_reference_preserves_preselected_child_conflict_and_atomicity() {
    let _hooks = Hooks;
    let f = Fixture::new();
    f.sql("CREATE TABLE p(id bigint PRIMARY KEY,parent bigint REFERENCES p(id) ON DELETE CASCADE)");
    f.sql("INSERT INTO p VALUES(1,NULL),(2,1),(3,2)");
    let before = f.contents(&["p"]);
    let result = operations::Engine::new(f.admin().clone()).execute(&delete("p"), &Bindings::new());
    assert!(result.is_err());
    assert_eq!(f.contents(&["p"]), before);
}

#[test]
fn locked_changed_child_is_reread_before_cascade_hydration() {
    let f = Fixture::new();
    let _hooks = Hooks;
    f.sql("CREATE TABLE p(id bigint PRIMARY KEY)");
    f.sql("CREATE TABLE c(id bigint PRIMARY KEY,p bigint REFERENCES p(id) ON DELETE CASCADE)");
    f.sql("INSERT INTO p VALUES(1),(2)");
    f.sql("INSERT INTO c VALUES(10,1)");
    let mut holder = f.admin().sql_session();
    holder.execute("BEGIN", &[], None).unwrap();
    holder
        .execute("UPDATE c SET p=2 WHERE id=10", &[], None)
        .unwrap();
    let admin = f.admin().clone();
    let (send, recv) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        send.send(operations::Engine::new(admin).execute(&delete("p"), &Bindings::new()))
            .unwrap();
    });
    let until = std::time::Instant::now() + std::time::Duration::from_secs(20);
    while holder.v2_transaction.as_ref().unwrap().sql_waiting_count() == 0 {
        assert!(matches!(
            recv.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        ));
        assert!(std::time::Instant::now() < until);
        std::thread::yield_now();
    }
    holder.execute("COMMIT", &[], None).unwrap();
    assert_eq!(
        recv.recv_timeout(std::time::Duration::from_secs(20))
            .unwrap()
            .unwrap()
            .affected,
        2
    );
    worker.join().unwrap();
    assert_eq!(
        f.contents(&["p", "c"]),
        vec![Vec::<Vec<Value>>::new(), Vec::new()]
    );
}

#[test]
fn concurrent_dml_commit_between_insert_rows_keeps_fk_error_and_rollback() {
    let f = Fixture::new();
    let _hooks = Hooks;
    f.sql("CREATE TABLE parent(id bigint PRIMARY KEY)");
    f.sql("CREATE TABLE p(id bigint PRIMARY KEY REFERENCES parent(id))");
    f.sql("INSERT INTO parent VALUES(1),(2)");
    let admin = f.admin().clone();
    operations::NATIVE_MANY_BEFORE_WRITE.with_borrow_mut(|h| {
        *h = Some(Box::new(move |_, row| {
            if row == 1 {
                admin
                    .sql_session()
                    .execute("DELETE FROM parent WHERE id=2", &[], None)
                    .unwrap();
            }
        }))
    });
    let result = operations::Engine::new(f.admin().clone()).execute(&many(), &Bindings::new());
    assert_eq!(result.unwrap_err().sqlstate(), Some("23503"));
    assert!(f.contents(&["p"])[0].is_empty());
    assert_eq!(f.contents(&["parent"])[0], vec![vec![Int(1)]]);
    drop(_hooks);
}

#[test]
fn ddl_cannot_commit_between_successful_row_writes() {
    let f = Fixture::new();
    let _hooks = Hooks;
    f.sql("CREATE TABLE p(id bigint PRIMARY KEY)");
    let admin = f.admin().clone();
    let (send, recv) = std::sync::mpsc::channel();
    let recv = Rc::new(RefCell::new(recv));
    let done = recv.clone();
    let job = Rc::new(RefCell::new(None));
    let thread = job.clone();
    operations::NATIVE_MANY_BEFORE_WRITE.with_borrow_mut(|h| {
        *h = Some(Box::new(move |session, row| {
            if row != 1 {
                return;
            }
            let admin = admin.clone();
            let send = send.clone();
            *thread.borrow_mut() = Some(std::thread::spawn(move || {
                send.send(admin.sql_session().execute(
                    "CREATE TABLE marker(id bigint PRIMARY KEY)",
                    &[],
                    None,
                ))
                .unwrap();
            }));
            let until = std::time::Instant::now() + std::time::Duration::from_secs(20);
            while session.v2_transaction.as_ref().unwrap().sql_waiting_count() == 0 {
                assert!(matches!(
                    done.borrow().try_recv(),
                    Err(std::sync::mpsc::TryRecvError::Empty)
                ));
                assert!(std::time::Instant::now() < until);
                std::thread::yield_now();
            }
            assert!(matches!(
                done.borrow().try_recv(),
                Err(std::sync::mpsc::TryRecvError::Empty)
            ));
        }))
    });
    operations::Engine::new(f.admin().clone())
        .execute(&many(), &Bindings::new())
        .unwrap();
    recv.borrow()
        .recv_timeout(std::time::Duration::from_secs(20))
        .unwrap()
        .unwrap();
    job.borrow_mut().take().unwrap().join().unwrap();
    drop(_hooks);
    assert_eq!(f.sql("SELECT * FROM p").rows.len(), 2);
}

#[test]
fn set_null_not_null_error_and_competing_fk_errors_keep_precedence() {
    let f = Fixture::new();
    let _hooks = Hooks;
    f.sql("CREATE TABLE p(id bigint PRIMARY KEY)");
    f.sql("CREATE TABLE c(id bigint PRIMARY KEY,p bigint NOT NULL REFERENCES p(id) ON DELETE SET NULL)");
    f.sql("CREATE TABLE r(id bigint PRIMARY KEY,p bigint REFERENCES p(id) ON DELETE RESTRICT)");
    f.sql("INSERT INTO p VALUES(1),(2)");
    f.sql("INSERT INTO c VALUES(10,1),(20,2)");
    f.sql("INSERT INTO r VALUES(30,2)");
    let before = f.contents(&["p", "c", "r"]);
    let result = operations::Engine::new(f.admin().clone()).execute(&delete("p"), &Bindings::new());
    assert!(result.is_err());
    assert_eq!(f.contents(&["p", "c", "r"]), before);
    assert_eq!(result.unwrap_err().sqlstate(), Some("23502"));
}
