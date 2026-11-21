//! Native request metadata visibility and cascade equivalence regressions.
use super::*;
use crate::operations::{self, Bindings, Expr, Request, ValueExpr};
use relational::Value::{self, Int};
use std::cell::RefCell;
use std::path::PathBuf;
use std::rc::Rc;

fn test_clock() -> relational::StatementClock {
    relational::StatementClock { unavailable: None, transaction_us: 1, statement_us: 1, zone: None, wall: None }
}

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
            .execute_at(sql, &[], None, &test_clock())
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
        operations::NATIVE_MANY_ON_EXIT.with(|h| h.set(None));
        operations::NATIVE_DELETE_BEFORE_WRITE.with_borrow_mut(|h| *h = None);
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
        assert_eq!(*observed.borrow(), vec![false]);
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
fn cascades_and_errors_match_sql_statement() {
    for action in [
        "CASCADE",
        "SET NULL",
        "SET DEFAULT",
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
                    f.admin().sql_session().execute("DELETE FROM p WHERE id IN (1,2)", &[], None)
                        .map(|r| (r.affected as usize,r.affected as usize,r.affected as usize))
                        .map_err(|error| operations::Error::Sql { sqlstate: crate::sqlstate(&error).into() })
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
fn self_reference_delete_matches_sql_statement() {
    let mut results=Vec::new();
    for native in [false,true] {
        let f = Fixture::new();
        f.sql("CREATE TABLE p(id bigint PRIMARY KEY,parent bigint REFERENCES p(id) ON DELETE CASCADE)");
        f.sql("INSERT INTO p VALUES(1,NULL),(2,1),(3,2)");
        let affected=if native {
            operations::Engine::new(f.admin().clone()).execute(&delete("p"), &Bindings::new()).unwrap().affected as u64
        } else { f.sql("DELETE FROM p WHERE id IN(1,2,3)").affected };
        results.push((affected,f.contents(&["p"])));
    }
    assert_eq!(results[0],results[1]);
    assert_eq!(results[0].0,3);
    assert!(results[0].1[0].is_empty());
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
fn concurrent_dml_commit_before_batch_insert_keeps_fk_error_and_rollback() {
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

fn q23_native_batches_equal_sql(case: usize) {
    {
        let mut results = Vec::new();
        for native in [false, true] {
            let f = Fixture::new();
            let (request, sql, tables) = if case == 0 {
                f.sql("CREATE TABLE p(id bigint PRIMARY KEY,parent bigint REFERENCES p(id))");
                (Request::InsertMany { table: "p".into(), columns: vec!["id".into(), "parent".into()],
                    rows: vec![vec![literal(Int(3)),literal(Int(4))], vec![literal(Int(4)),literal(Value::Null)]],
                    cap: ValueExpr::Literal(Int(2)) }, "INSERT INTO p VALUES(3,4),(4,NULL)", vec!["p"])
            } else if case == 1 {
                f.sql("CREATE TABLE p(id bigint PRIMARY KEY,parent bigint REFERENCES p(id))");
                f.sql("INSERT INTO p VALUES(1,NULL),(2,1)");
                (delete("p"), "DELETE FROM p WHERE id IN (1,2)", vec!["p"])
            } else {
                f.sql("CREATE TABLE p(id bigint PRIMARY KEY)");
                f.sql("CREATE TABLE c(id bigint PRIMARY KEY,pid bigint REFERENCES p(id) ON DELETE CASCADE)");
                f.sql("CREATE TABLE g(id bigint PRIMARY KEY,cid bigint REFERENCES c(id),pid bigint REFERENCES p(id) ON DELETE CASCADE)");
                f.sql("INSERT INTO p VALUES(1),(2)");
                f.sql("INSERT INTO c VALUES(10,1)");
                f.sql("INSERT INTO g VALUES(100,10,2)");
                (delete("p"), "DELETE FROM p WHERE id IN (1,2)", vec!["p","c","g"])
            };
            let affected = if native {
                operations::Engine::new(f.admin().clone()).execute(&request, &Bindings::new()).unwrap().affected as u64
            } else { f.sql(sql).affected };
            results.push((affected, f.contents(&tables)));
        }
        assert_eq!(results[0], results[1], "Q23 case {case}");
        assert_eq!(results[0].0, 2);
    }
}

#[test]
fn q23_insert_self_reference() { q23_native_batches_equal_sql(0); }
#[test]
fn q23_delete_self_reference() { q23_native_batches_equal_sql(1); }
#[test]
fn q23_delete_cascade_valid_end_state() { q23_native_batches_equal_sql(2); }

#[test]
fn insert_equivalence_unique_identity_and_invalid_foreign_keys() {
    for (ddl, seed, columns, rows, sql) in [
        ("CREATE TABLE p(id bigint PRIMARY KEY,u bigint CHECK(u>0))", "", vec!["id","u"],
            vec![vec![Int(2),Int(-2)],vec![Int(1),Int(-1)]], "INSERT INTO p VALUES(1,-1),(2,-2)"),
        ("CREATE TABLE p(id bigint PRIMARY KEY,u bigint NOT NULL)", "", vec!["id","u"],
            vec![vec![Int(2),Value::Null],vec![Int(1),Value::Null]], "INSERT INTO p VALUES(1,NULL),(2,NULL)"),
        ("CREATE TABLE p(id bigint PRIMARY KEY,u bigint UNIQUE)", "", vec!["id","u"],
            vec![vec![Int(2),Int(8)],vec![Int(1),Int(8)]], "INSERT INTO p VALUES(1,8),(2,8)"),
        ("CREATE TABLE p(id bigint PRIMARY KEY)", "INSERT INTO p VALUES(1)", vec!["id"],
            vec![vec![Int(2)],vec![Int(1)]], "INSERT INTO p VALUES(1),(2)"),
        ("CREATE TABLE p(id bigint PRIMARY KEY,parent bigint REFERENCES p(id))", "", vec!["id","parent"],
            vec![vec![Int(1),Int(99)],vec![Int(2),Value::Null]], "INSERT INTO p VALUES(1,99),(2,NULL)"),
        ("CREATE TABLE p(id bigserial PRIMARY KEY,u bigint)", "", vec!["u"],
            vec![vec![Int(8)],vec![Int(9)]], "INSERT INTO p(u) VALUES(8),(9)"),
        ("CREATE TABLE p(id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,u bigint)", "", vec!["u"],
            vec![vec![Int(8)],vec![Int(9)]], "INSERT INTO p(u) VALUES(8),(9)"),
    ] {
        let mut results = Vec::new();
        for native in [false,true] {
            let f = Fixture::new(); f.sql(ddl); if !seed.is_empty() { f.sql(seed); }
            let result = if native {
                let request = Request::InsertMany { table: "p".into(),
                    columns: columns.iter().map(|c| (*c).into()).collect(),
                    rows: rows.iter().map(|r| r.iter().cloned().map(literal).collect()).collect(),
                    cap: ValueExpr::Literal(Int(2)) };
                operations::Engine::new(f.admin().clone()).execute(&request,&Bindings::new())
                    .map(|o| o.affected as u64).map_err(|e| e.sqlstate().unwrap().to_owned())
            } else {
                f.admin().sql_session().execute_at(sql,&[],None,&test_clock())
                    .map(|o| o.affected).map_err(|e| crate::sqlstate(&e).to_owned())
            };
            results.push((result,f.contents(&["p"])));
        }
        assert_eq!(results[0],results[1],"{ddl}");
    }
}

#[test]
fn duplicate_target_keeps_native_admission_error() {
    let f = Fixture::new(); f.sql("CREATE TABLE p(id bigint PRIMARY KEY)");
    let mut request = many();
    if let Request::InsertMany { rows, .. } = &mut request { rows[1] = rows[0].clone(); }
    assert_eq!(operations::Engine::new(f.admin().clone()).execute(&request,&Bindings::new()),
        Err(operations::Error::DuplicateTarget));
    let error = f.admin().sql_session().execute("INSERT INTO p VALUES(1),(1)",&[],None).unwrap_err();
    assert_eq!(crate::sqlstate(&error),"23505");
    assert!(f.contents(&["p"])[0].is_empty());
}

#[test]
fn return_rows_versions_and_generated_values_match_sql() {
    use crate::operations::Projection;
    for ddl in ["CREATE TABLE p(id bigserial PRIMARY KEY,u bigint)",
        "CREATE TABLE p(id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,u bigint)"] {
        let mut results = Vec::new();
        for native in [false,true] {
            let f=Fixture::new(); f.sql(ddl);
            let request = Request::Returning { request: Box::new(Request::InsertMany {
                table:"p".into(),columns:vec!["u".into()],rows:vec![vec![literal(Int(8))],vec![literal(Int(9))]],
                cap:ValueExpr::Literal(Int(2)) }), projection: ["id","u"].into_iter().map(|c|
                    Projection { expression:Expr::Field(c.into()),alias:None }).collect() };
            let (affected,returned) = if native {
                let r=operations::Engine::new(f.admin().clone()).execute(&request,&Bindings::new()).unwrap();
                (r.affected as u64,r.rows)
            } else { let r=f.sql("INSERT INTO p(u) VALUES(8),(9) RETURNING id,u"); (r.affected,r.rows) };
            let stored=f.sql("SELECT * FROM p ORDER BY id").rows;
            assert_eq!(returned,stored);
            results.push((affected,returned.into_iter().map(|r|r.values).collect::<Vec<_>>(),f.contents(&["p"])));
        }
        assert_eq!(results[0],results[1]);
    }
}

#[test]
fn ddl_waits_for_native_batch_transaction() {
    let f=Fixture::new(); f.sql("CREATE TABLE p(id bigint PRIMARY KEY)");
    let mut session=f.admin().sql_session(); session.execute("BEGIN",&[],None).unwrap();
    session.execute_native(&many(),&Bindings::new()).unwrap();
    let admin=f.admin().clone(); let (send,recv)=std::sync::mpsc::channel();
    let worker=std::thread::spawn(move || { send.send(admin.sql_session().execute(
        "CREATE TABLE marker(id bigint PRIMARY KEY)",&[],None)).unwrap(); });
    let deadline=std::time::Instant::now()+std::time::Duration::from_secs(20);
    while session.v2_transaction.as_ref().unwrap().sql_waiting_count()==0 {
        assert!(matches!(recv.try_recv(),Err(std::sync::mpsc::TryRecvError::Empty)));
        assert!(std::time::Instant::now()<deadline); std::thread::yield_now();
    }
    session.execute("COMMIT",&[],None).unwrap();
    recv.recv_timeout(std::time::Duration::from_secs(20)).unwrap().unwrap(); worker.join().unwrap();
    assert_eq!(f.contents(&["p"])[0].len(),2);
}

#[test]
fn delete_many_return_preimages_and_versions_equal_sql() {
    use crate::operations::Projection;
    let mut outcomes=Vec::new();
    for native in [false,true] {
        let f=Fixture::new(); f.sql("CREATE TABLE p(id bigint PRIMARY KEY,u bigint)");
        f.sql("INSERT INTO p VALUES(3,30),(1,10),(2,20)");
        let before=f.sql("SELECT * FROM p ORDER BY id").rows;
        let rows=if native {
            operations::Engine::new(f.admin().clone()).execute(&Request::Returning {
                request:Box::new(delete("p")),projection:["id","u"].into_iter().map(|c|
                    Projection {expression:Expr::Field(c.into()),alias:None}).collect()
            },&Bindings::new()).unwrap().rows
        } else { f.sql("DELETE FROM p WHERE id IN(3,1,2) RETURNING id,u").rows };
        assert_eq!(rows,before);
        outcomes.push((rows.into_iter().map(|r|r.values).collect::<Vec<_>>(),f.contents(&["p"])));
    }
    assert_eq!(outcomes[0],outcomes[1]);
}

#[test]
fn mixed_restrict_and_no_action_matches_sql_immediate_error() {
    let mut outcomes=Vec::new();
    for native in [false,true] {
        let f=Fixture::new();
        f.sql("CREATE TABLE p(id bigint PRIMARY KEY)");
        f.sql("CREATE TABLE a(id bigint PRIMARY KEY,p bigint REFERENCES p(id) ON DELETE NO ACTION)");
        f.sql("CREATE TABLE z(id bigint PRIMARY KEY,p bigint REFERENCES p(id) ON DELETE RESTRICT)");
        f.sql("INSERT INTO p VALUES(1),(2)");
        f.sql("INSERT INTO a VALUES(10,1)"); f.sql("INSERT INTO z VALUES(20,2)");
        let result=if native {
            operations::Engine::new(f.admin().clone()).execute(&delete("p"),&Bindings::new())
                .map(|r|r.affected as u64).map_err(|e|e.sqlstate().unwrap().to_owned())
        } else { f.admin().sql_session().execute("DELETE FROM p WHERE id IN(1,2)",&[],None)
            .map(|r|r.affected).map_err(|e|crate::sqlstate(&e).to_owned()) };
        assert_eq!(result,Err("23503".into()));
        outcomes.push((result,f.contents(&["p","a","z"])));
    }
    assert_eq!(outcomes[0],outcomes[1]);
}

#[test]
fn autocommit_ddl_waits_through_native_batch_exit() {
    fn on_exit(session: &SqlSession) {
        let admin=session.admin.clone();
        let (send,recv)=std::sync::mpsc::channel();
        let worker=std::thread::spawn(move || {
            send.send(admin.sql_session().execute("CREATE TABLE marker(id bigint PRIMARY KEY)",&[],None)).unwrap();
        });
        let deadline=std::time::Instant::now()+std::time::Duration::from_secs(20);
        while session.v2_transaction.as_ref().unwrap().sql_waiting_count()==0 {
            assert!(matches!(recv.try_recv(),Err(std::sync::mpsc::TryRecvError::Empty)));
            assert!(std::time::Instant::now()<deadline); std::thread::yield_now();
        }
        EXIT_DDL.with_borrow_mut(|h| *h=Some((recv,worker)));
    }
    let f=Fixture::new(); let _hooks=Hooks;
    f.sql("CREATE TABLE p(id bigint PRIMARY KEY)");
    operations::NATIVE_MANY_ON_EXIT.with(|h|h.set(Some(on_exit)));
    f.admin().sql_session().execute_native(&many(),&Bindings::new()).unwrap();
    let (recv,worker)=EXIT_DDL.with_borrow_mut(|h|h.take().unwrap());
    recv.recv_timeout(std::time::Duration::from_secs(20)).unwrap().unwrap(); worker.join().unwrap();
    assert_eq!(f.contents(&["p"])[0].len(),2);
    f.sql("SELECT * FROM marker");
}
type DdlPending = (std::sync::mpsc::Receiver<Result<relational::SqlResult,CoreError>>,std::thread::JoinHandle<()>);
thread_local! {
    static EXIT_DDL: RefCell<Option<DdlPending>> = const { RefCell::new(None) };
}


#[test]
fn cascade_delete_many_point_candidates_and_returning_equal_sql() {
    use crate::operations::{Compare, Projection};
    for count in [1, 201] {
        let mut outcomes = Vec::new();
        for native in [false, true] {
            let f = Fixture::new();
            let _hooks = Hooks;
            f.sql("CREATE TABLE p(id bigint PRIMARY KEY)");
            f.sql("CREATE TABLE c(id bigint PRIMARY KEY,p bigint REFERENCES p(id) ON DELETE CASCADE)");
            f.sql("CREATE INDEX c_p ON c(p)");
            f.sql(&format!("INSERT INTO p VALUES {}", (1..=1000)
                .map(|id| format!("({id})")).collect::<Vec<_>>().join(",")));
            f.sql("INSERT INTO c VALUES(10,1),(20,201),(30,999)");
            let before = f.sql(&format!("SELECT * FROM p WHERE id<={count} ORDER BY id")).rows;
            let (affected, rows) = if native {
                operations::NATIVE_DELETE_BEFORE_WRITE.with_borrow_mut(|h| *h = Some(Box::new(|| {
                    v2_sql::MUTATION_PAGE_READS.set(0);
                    v2_sql::MUTATION_POINT_READS.set(0);
                })));
                let result = operations::Engine::new(f.admin().clone()).execute(&Request::Returning {
                    request: Box::new(Request::Delete { table: "p".into(),
                        filter: Expr::Compare(Box::new(Expr::Field("id".into())), Compare::Le,
                            Box::new(literal(Int(count)))), guard: None,
                        cap: Some(ValueExpr::Literal(Int(count))) }),
                    projection: vec![Projection { expression: Expr::Field("id".into()), alias: None }],
                }, &Bindings::new()).unwrap();
                assert_eq!(v2_sql::MUTATION_PAGE_READS.get(), 0);
                assert_eq!(v2_sql::MUTATION_POINT_READS.get(), count as usize);
                (result.affected as u64, result.rows)
            } else {
                let keys = (1..=count).map(|id| id.to_string()).collect::<Vec<_>>().join(",");
                let result = f.sql(&format!("DELETE FROM p WHERE id IN({keys}) RETURNING id"));
                (result.affected, result.rows)
            };
            assert_eq!(affected, count as u64);
            assert_eq!(rows, before);
            outcomes.push((affected, rows.into_iter().map(|r| r.values).collect::<Vec<_>>(),
                f.contents(&["p", "c"])));
        }
        assert_eq!(outcomes[0], outcomes[1]);
    }
}

#[test]
fn cascade_native_delete_duplicate_keys_are_one_candidate() {
    let f = Fixture::new();
    f.sql("CREATE TABLE p(id bigint PRIMARY KEY)");
    f.sql("CREATE TABLE c(id bigint PRIMARY KEY,p bigint REFERENCES p(id) ON DELETE CASCADE)");
    f.sql("CREATE INDEX c_p ON c(p)");
    f.sql("INSERT INTO p VALUES(1),(2),(3)");
    f.sql("INSERT INTO c VALUES(10,1),(20,2),(30,3)");
    let mut session = f.admin().sql_session();
    let table = session.catalog_engine().unwrap().1.into_iter().find(|t| t.name == "p").unwrap();
    let rows = f.sql("SELECT * FROM p ORDER BY id").rows;
    let target = |i: usize| (vec![rows[i].values[0].clone()], rows[i].version);
    v2_sql::MUTATION_PAGE_READS.set(0);
    v2_sql::MUTATION_POINT_READS.set(0);
    assert_eq!(session.delete_many_engine(&table, vec![target(1), target(0), target(1)]).unwrap().affected, 2);
    assert_eq!(v2_sql::MUTATION_PAGE_READS.get(), 0);
    assert_eq!(v2_sql::MUTATION_POINT_READS.get(), 2);
    assert_eq!(f.contents(&["p", "c"]), vec![vec![vec![Int(3)]], vec![vec![Int(30), Int(3)]]]);
}

#[test]
fn cascade_native_delete_missing_and_stale_witnesses_fail_before_writes() {
    for missing in [false, true] {
        let f = Fixture::new();
        let _hooks = Hooks;
        f.sql("CREATE TABLE p(id bigint PRIMARY KEY,n bigint)");
        f.sql("CREATE TABLE c(id bigint PRIMARY KEY,p bigint REFERENCES p(id) ON DELETE CASCADE)");
        f.sql("INSERT INTO p VALUES(1,0),(2,0)");
        f.sql("INSERT INTO c VALUES(10,1),(20,2)");
        let admin = f.admin().clone();
        operations::NATIVE_DELETE_BEFORE_WRITE.with_borrow_mut(|h| *h = Some(Box::new(move || {
            admin.sql_session().execute(if missing { "DELETE FROM p WHERE id=2" }
                else { "UPDATE p SET n=1 WHERE id=2" }, &[], None).unwrap();
        })));
        let error = operations::Engine::new(f.admin().clone()).execute(&delete("p"), &Bindings::new()).unwrap_err();
        assert_eq!(error.sqlstate(), Some("40001"));
        let expected = if missing { vec![vec![vec![Int(1), Int(0)]], vec![vec![Int(10), Int(1)]]] }
            else { vec![vec![vec![Int(1), Int(0)], vec![Int(2), Int(1)]],
                vec![vec![Int(10), Int(1)], vec![Int(20), Int(2)]]] };
        assert_eq!(f.contents(&["p", "c"]), expected);
    }
}
