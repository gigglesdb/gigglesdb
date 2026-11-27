//! F6: transaction-held writer locks and READ COMMITTED wait/re-read behavior.
use super::*;
use relational::Value::Int;
use std::sync::mpsc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

struct Fixture { admin: Option<Admin>, path: std::path::PathBuf }
impl Fixture {
    fn new() -> Self {
        let mut nonce = [0; 16];
        getrandom::fill(&mut nonce).unwrap();
        let path = std::env::temp_dir().join(format!("f6-{:032x}", u128::from_be_bytes(nonce)));
        let admin = Some(Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap());
        Self { admin, path }
    }
    fn admin(&self) -> &Admin { self.admin.as_ref().unwrap() }
    fn sql(&self) -> SqlSession { self.admin().sql_session() }
    fn exec(&self, sql: &str) { run(&mut self.sql(), sql); }
}
impl Drop for Fixture {
    fn drop(&mut self) { self.admin.take(); fs::remove_dir_all(&self.path).unwrap(); }
}
fn run(s: &mut SqlSession, sql: &str) -> relational::SqlResult {
    s.execute_at(sql, &[], None, &clock()).unwrap_or_else(|e| panic!("{sql}: {e:?}"))
}
fn clock() -> relational::StatementClock {
    relational::StatementClock { unavailable: None, transaction_us: 1, statement_us: 1, zone: None, wall: None }
}

fn batch_rows(result: relational::SqlResult) -> Vec<Vec<relational::Value>> {
    result.rows.into_iter().map(|row| row.values).collect()
}

type BatchOutcome = Result<(u64, Vec<Vec<relational::Value>>), (&'static str, String)>;

fn batch_outcome(result: Result<relational::SqlResult, CoreError>) -> BatchOutcome {
    result.map(|result| (result.affected, batch_rows(result)))
        .map_err(|error| (crate::sqlstate(&error), error.to_string()))
}

#[test]
fn batch_matches_point_probes_for_delete_and_primary_key_update() {
    for update in [false, true] {
        for action in ["CASCADE", "SET NULL", "SET DEFAULT", "RESTRICT", "NO ACTION", "NO ACTION DEFERRABLE INITIALLY DEFERRED"] {
            let run_case = |disabled| {
                let f = Fixture::new();
                f.exec("CREATE TABLE p(id int PRIMARY KEY)");
                f.exec(&format!("CREATE TABLE c(id int PRIMARY KEY, a int DEFAULT 10,
                    b int, FOREIGN KEY(a) REFERENCES p(id) ON {} {action},
                    FOREIGN KEY(b) REFERENCES p(id) ON DELETE CASCADE ON UPDATE CASCADE)",
                    if update { "UPDATE" } else { "DELETE" }));
                f.exec("CREATE INDEX ca ON c(a)");
                f.exec("CREATE INDEX cb ON c(b,id)");
                f.exec("CREATE TABLE g(id int, c int REFERENCES c(id) ON DELETE CASCADE ON UPDATE CASCADE,
                    PRIMARY KEY(c,id))");
                f.exec("INSERT INTO p VALUES(1),(2),(3),(4),(10)");
                f.exec("INSERT INTO c VALUES(1,2,4),(2,4,3)");
                f.exec("INSERT INTO g VALUES(1,1),(2,2)");
                let mut s = f.sql();
                run(&mut s, "BEGIN");
                let batches = FK_CHILD_BATCHES.get();
                DISABLE_FK_CHILD_BATCH.set(disabled);
                let sql = if update { "UPDATE p SET id=id+20 WHERE id IN (1,2,3) RETURNING id" }
                    else { "DELETE FROM p WHERE id IN (1,2,3) RETURNING id" };
                let statement = batch_outcome(s.execute(sql, &[], None));
                DISABLE_FK_CHILD_BATCH.set(false);
                if !disabled && (update || !action.starts_with("NO ACTION")) {
                    assert!(FK_CHILD_BATCHES.get() > batches, "batch never ran for {sql}, {action}");
                }
                let end = batch_outcome(s.execute(if statement.is_ok() { "COMMIT" } else { "ROLLBACK" }, &[], None));
                if s.is_aborted() { run(&mut s, "ROLLBACK"); }
                let tables = ["SELECT * FROM p ORDER BY id", "SELECT * FROM c ORDER BY id", "SELECT * FROM g ORDER BY id"]
                    .map(|sql| batch_rows(run(&mut s, sql)));
                (statement, end, tables)
            };
            assert_eq!(run_case(false), run_case(true), "update={update}, action={action}");
        }
    }
}

#[test]
fn batch_invalidates_empty_keys_after_set_default() {
    let f = Fixture::new();
    f.exec("CREATE TABLE p(id int PRIMARY KEY)");
    f.exec("CREATE TABLE c(id int PRIMARY KEY, a int DEFAULT 2 REFERENCES p(id) ON DELETE SET DEFAULT,
        b int REFERENCES p(id) ON DELETE CASCADE)");
    f.exec("CREATE INDEX ca ON c(a)");
    f.exec("CREATE INDEX cb ON c(b)");
    f.exec("INSERT INTO p VALUES (1),(2),(3)");
    f.exec("INSERT INTO c VALUES (1,1,3)");
    // Parent 1 moves c.a to the absent-at-first parent key 2. Parent 2's
    // SET DEFAULT must fail before parent 3 gets a chance to delete the child.
    let batches = FK_CHILD_BATCHES.get();
    let error = f.sql().execute("DELETE FROM p WHERE id IN (1,2,3)", &[], None).unwrap_err();
    assert_eq!(crate::sqlstate(&error), "23503", "{error:?}");
    assert!(FK_CHILD_BATCHES.get() > batches);
    assert_eq!(batch_rows(run(&mut f.sql(), "SELECT * FROM c")), [vec![Int(1), Int(1), Int(3)]]);
}

#[test]
fn batch_replaces_all_empty_probes_for_a_hundred_parents() {
    let f = Fixture::new();
    f.exec("CREATE TABLE p(id int PRIMARY KEY)");
    f.exec("CREATE TABLE c(id int PRIMARY KEY, p int REFERENCES p(id) ON DELETE CASCADE)");
    f.exec("CREATE INDEX cp ON c(p)");
    f.exec("CREATE TABLE d(p int REFERENCES p(id) ON DELETE CASCADE, id int, PRIMARY KEY(p,id))");
    let keys = (1..=100).map(|key| key.to_string()).collect::<Vec<_>>();
    let values = keys.iter().map(|key| format!("({key})")).collect::<Vec<_>>().join(",");
    f.exec(&format!("INSERT INTO p VALUES {values}"));
    let probes = FK_CHILD_PROBES.get();
    let checks = FK_CHILD_BATCHES.get();
    let result = run(&mut f.sql(), &format!("DELETE FROM p WHERE id IN ({}) RETURNING id", keys.join(",")));
    assert_eq!(result.affected, 100);
    assert_eq!(FK_CHILD_BATCHES.get() - checks, 2);
    assert_eq!(FK_CHILD_PROBES.get() - probes, 0);
}

const BATCH_DELETE: &str = "DELETE FROM p WHERE id IN (1,2,3) RETURNING id";
const BATCH_CHILD: &str = "INSERT INTO c VALUES(1,2)";

fn batch_cascade_fixture() -> Fixture {
    let f = Fixture::new();
    f.exec("CREATE TABLE p(id int PRIMARY KEY)");
    f.exec("CREATE TABLE c(id int PRIMARY KEY, p int REFERENCES p(id) ON DELETE CASCADE)");
    f.exec("CREATE INDEX cp ON c(p)");
    f.exec("INSERT INTO p VALUES (1),(2),(3)");
    f
}

fn batch_assert_final(f: &Fixture, deleted: bool) {
    let mut s = f.sql();
    assert!(run(&mut s, "SELECT c.id FROM c LEFT JOIN p ON p.id=c.p WHERE p.id IS NULL").rows.is_empty());
    assert_eq!(batch_rows(run(&mut s, "SELECT * FROM p ORDER BY id")),
        if deleted { vec![] } else { vec![vec![Int(1)], vec![Int(2)], vec![Int(3)]] });
    assert_eq!(batch_rows(run(&mut s, "SELECT * FROM c ORDER BY id")),
        if deleted { vec![] } else { vec![vec![Int(1), Int(2)]] });
}

#[test]
fn batch_cascade_lock_waits_match_postgres() {
    for level in [SqlIsolation::ReadCommitted, SqlIsolation::RepeatableRead, SqlIsolation::Serializable] {
        for child_first in [true, false] {
            let f = batch_cascade_fixture();
            let mut holder = f.sql();
            holder.set_isolation(level);
            run(&mut holder, "BEGIN");
            let (first, second) = if child_first { (BATCH_CHILD, BATCH_DELETE) } else { (BATCH_DELETE, BATCH_CHILD) };
            let checks = FK_CHILD_BATCHES.get();
            assert_eq!(run(&mut holder, first).affected, if child_first { 1 } else { 3 });
            assert_eq!(FK_CHILD_BATCHES.get() - checks, usize::from(!child_first));

            let admin = f.admin().clone();
            let (tx, rx) = mpsc::channel();
            let worker = thread::spawn(move || {
                let mut waiter = admin.sql_session();
                waiter.set_isolation(level);
                run(&mut waiter, "BEGIN");
                let checks = FK_CHILD_BATCHES.get();
                let result = batch_outcome(waiter.execute(second, &[], None));
                run(&mut waiter, if result.is_ok() { "COMMIT" } else { "ROLLBACK" });
                tx.send((result, FK_CHILD_BATCHES.get() - checks)).unwrap();
            });
            let deadline = Instant::now() + Duration::from_secs(10);
            while holder.v2_transaction.as_ref().unwrap().sql_waiting_count() == 0 {
                assert!(Instant::now() < deadline, "no lock wait for {second} at {level:?}");
                thread::sleep(Duration::from_millis(1));
            }
            run(&mut holder, "COMMIT");
            let (result, checks) = rx.recv_timeout(Duration::from_secs(10)).unwrap();
            worker.join().unwrap();
            assert_eq!(checks, usize::from(child_first));
            match (child_first, level) {
                (true, SqlIsolation::ReadCommitted) => assert_eq!(result.unwrap(),
                    (3, vec![vec![Int(1)], vec![Int(2)], vec![Int(3)]])),
                (false, SqlIsolation::ReadCommitted) => assert_eq!(result.unwrap_err().0, "23503"),
                _ => assert_eq!(result.unwrap_err().0, "40001"),
            }
            batch_assert_final(&f, !child_first || level == SqlIsolation::ReadCommitted);
        }
    }
}

#[test]
fn batch_cascade_finds_a_child_committed_after_the_fixed_snapshot() {
    for level in [SqlIsolation::RepeatableRead, SqlIsolation::Serializable] {
        let f = batch_cascade_fixture();
        let mut a = f.sql();
        a.set_isolation(level);
        run(&mut a, "BEGIN");
        run(&mut a, "SELECT count(*) FROM p");
        let mut b = f.sql();
        b.set_isolation(level);
        run(&mut b, "BEGIN");
        run(&mut b, BATCH_CHILD);
        run(&mut b, "COMMIT");
        let checks = FK_CHILD_BATCHES.get();
        let outcome = batch_outcome(a.execute(BATCH_DELETE, &[], None));
        run(&mut a, "ROLLBACK");
        assert_eq!(FK_CHILD_BATCHES.get() - checks, 1);
        assert_eq!(outcome.unwrap_err().0, "40001");
        batch_assert_final(&f, false);
    }
}

struct Pending { done: mpsc::Receiver<Result<relational::SqlResult, CoreError>>, worker: JoinHandle<()> }
impl Pending {
    fn start(f: &Fixture, sql: &str) -> Self {
        let disabled = DISABLE_MUTATION_POINTS.get();
        let admin = f.admin().clone();
        let sql = sql.to_owned();
        let (send, done) = mpsc::channel();
        let worker = thread::spawn(move || { DISABLE_MUTATION_POINTS.set(disabled); let result = admin.sql_session().execute_at(&sql, &[], None, &clock()); send.send(result).unwrap(); });
        Self { done, worker }
    }
    fn blocked(&self, holder: &SqlSession) {
        let until = Instant::now() + Duration::from_secs(10);
        loop {
            assert!(matches!(self.done.try_recv(), Err(mpsc::TryRecvError::Empty)), "writer completed before holder ended");
            if holder.v2_transaction.as_ref().unwrap().sql_waiting_count() > 0 { break; }
            assert!(Instant::now() < until, "writer never entered a lock wait");
            thread::sleep(Duration::from_millis(1));
        }
    }
    fn finish(self) -> Result<relational::SqlResult, CoreError> {
        let result = self.done.recv_timeout(Duration::from_secs(20)).unwrap();
        self.worker.join().unwrap();
        result
    }
}

#[test]
fn disjoint_parent_deletes_and_update_proceed() {
    let f = Fixture::new();
    f.exec("CREATE TABLE p(id bigint PRIMARY KEY,n bigint)");
    f.exec("CREATE TABLE c(id bigint PRIMARY KEY,p bigint REFERENCES p(id))");
    f.exec("INSERT INTO p VALUES(1,0),(2,0),(3,0)");
    let mut a=f.sql(); run(&mut a,"BEGIN");
    let before=FK_DELETE_SHARED.get();
    run(&mut a,"DELETE FROM p WHERE id IN (1)");
    assert_eq!(FK_DELETE_SHARED.get(),before+1);
    Pending::start(&f,"DELETE FROM p WHERE id IN (2)").finish().unwrap();
    Pending::start(&f,"UPDATE p SET n=1 WHERE id=3").finish().unwrap();
    run(&mut a,"ROLLBACK");
}
#[test]
fn overlapping_parent_delete_rereads_after_wait() {
    for commit in [false,true] {
        let f=Fixture::new();
        f.exec("CREATE TABLE p(id bigint PRIMARY KEY,n bigint)");
        f.exec("CREATE TABLE c(id bigint PRIMARY KEY,p bigint REFERENCES p(id))");
        f.exec("INSERT INTO p VALUES(1,0),(2,0),(3,0)");
        let mut a=f.sql(); run(&mut a,"BEGIN");
        run(&mut a,"DELETE FROM p WHERE id IN (1,2)");
        let b=Pending::start(&f,"DELETE FROM p WHERE id IN (3,2) RETURNING id");
        b.blocked(&a); run(&mut a,if commit {"COMMIT"}else{"ROLLBACK"});
        let result=b.finish().unwrap();
        assert_eq!(result.rows.len(),if commit {1}else{2});
        assert!(result.rows.iter().any(|r|r.values==vec![Int(3)]));
    }
}

#[test]
fn retention_upgrade_preserves_delete() {
    let f=Fixture::new();
    f.exec("CREATE TABLE p(id bigint PRIMARY KEY,n text)");
    f.exec("CREATE TABLE c(id bigint PRIMARY KEY,p bigint REFERENCES p(id))");
    f.exec(&format!("INSERT INTO p VALUES(1,'{}')", "x".repeat(4096)));
    let before=FK_DELETE_UPGRADE.get();
    FK_DELETE_RETENTION.set(Some(1024));
    let result=f.sql().execute_at("DELETE FROM p WHERE id IN (1)",&[],None,&clock());
    FK_DELETE_RETENTION.set(None);
    result.unwrap();
    assert_eq!(FK_DELETE_UPGRADE.get(),before+1);
    assert!(run(&mut f.sql(),"SELECT * FROM p").rows.is_empty());
}

#[test]
fn conservative_fallbacks_do_not_take_shared_delete_route() {
    for ddl in [
        "CREATE TABLE c(id bigint PRIMARY KEY,p bigint REFERENCES p(id) DEFERRABLE)",
        "CREATE TABLE c(id bigint PRIMARY KEY,p bigint DEFAULT NULL REFERENCES p(id) ON DELETE SET DEFAULT)",
        "ALTER TABLE p ADD CONSTRAINT self_ref FOREIGN KEY(n) REFERENCES p(id)",
    ] {
        let f=Fixture::new();
        f.exec("CREATE TABLE p(id bigint PRIMARY KEY,n bigint)");
        f.exec(ddl);
        let before=FK_DELETE_SHARED.get();
        run(&mut f.sql(),"DELETE FROM p WHERE id IN (1)");
        assert_eq!(FK_DELETE_SHARED.get(),before);
    }
    let f=Fixture::new();
    f.exec("CREATE TABLE p(id bigint PRIMARY KEY,n bigint)");
    f.exec("CREATE TABLE c(id bigint PRIMARY KEY,p bigint REFERENCES p(id))");
    let before=FK_DELETE_SHARED.get();
    let ids=(1..=201).map(|i|i.to_string()).collect::<Vec<_>>().join(",");
    run(&mut f.sql(),&format!("DELETE FROM p WHERE id IN ({ids})"));
    assert_eq!(FK_DELETE_SHARED.get(),before);
}

#[test]
fn same_parent_update_and_delete_wait_both_orders() {
    for delete_first in [false,true] {
        for commit in [false,true] {
            let f=Fixture::new();
            f.exec("CREATE TABLE p(id bigint PRIMARY KEY,n bigint)");
            f.exec("CREATE TABLE c(id bigint PRIMARY KEY,p bigint REFERENCES p(id))");
            f.exec("INSERT INTO p VALUES(1,0)");
            let mut a=f.sql(); run(&mut a,"BEGIN");
            let delete="DELETE FROM p WHERE id IN (1) RETURNING id";
            let update="UPDATE p SET n=n+1 WHERE id=1 RETURNING id";
            run(&mut a,if delete_first {delete}else{update});
            let b=Pending::start(&f,if delete_first {update}else{delete});
            b.blocked(&a); run(&mut a,if commit {"COMMIT"}else{"ROLLBACK"});
            assert_eq!(b.finish().unwrap().rows.len(),usize::from(!(delete_first && commit)));
        }
    }
}

#[test]
fn fk_races_match_exclusive_route() {
    fn scenario(action: &str, child_first: bool, commit: bool, update: bool, disabled: bool) -> String {
        DISABLE_MUTATION_POINTS.set(disabled);
        let f=Fixture::new();
        f.exec("CREATE TABLE p(id bigint PRIMARY KEY,n bigint)");
        f.exec(&format!("CREATE TABLE c(id bigint PRIMARY KEY,p bigint REFERENCES p(id) ON DELETE {action})"));
        f.exec("INSERT INTO p VALUES(1,0),(2,0)");
        if update {f.exec("INSERT INTO c VALUES(1,2)");}
        let child=if update {"UPDATE c SET p=1 WHERE id=1"}else{"INSERT INTO c VALUES(1,1)"};
        let delete="DELETE FROM p WHERE id IN (1)";
        let mut a=f.sql();run(&mut a,"BEGIN");
        run(&mut a,if child_first {child}else{delete});
        let b=Pending::start(&f,if child_first {delete}else{child});
        b.blocked(&a);run(&mut a,if commit {"COMMIT"}else{"ROLLBACK"});
        let outcome=match b.finish(){Ok(_)=>"ok".to_owned(),Err(e)=>format!("{e:?}")};
        let parents=run(&mut f.sql(),"SELECT * FROM p ORDER BY id");
        let children=run(&mut f.sql(),"SELECT * FROM c ORDER BY id");
        DISABLE_MUTATION_POINTS.set(false);
        format!("{outcome};{:?};{:?}",parents.rows.iter().map(|r|&r.values).collect::<Vec<_>>(),children.rows.iter().map(|r|&r.values).collect::<Vec<_>>())
    }
    for action in ["RESTRICT","NO ACTION","CASCADE","SET NULL"] {
        for child_first in [false,true] {for commit in [false,true] {for update in [false,true] {
            assert_eq!(scenario(action,child_first,commit,update,false),scenario(action,child_first,commit,update,true),"{action} child_first={child_first} commit={commit} update={update}");
        }}}
    }
}

// Compare the complete observable result while proving the selected RI path ran.
fn batch_differential(setup: &[&str], statement: &str, expected_batches: bool, expected_probes: bool) {
    let exercise = |disabled| {
        let f = Fixture::new();
        for sql in setup { f.exec(sql); }
        let checks = FK_CHILD_BATCHES.get();
        let probes = FK_CHILD_PROBES.get();
        DISABLE_FK_CHILD_BATCH.set(disabled);
        let result = batch_outcome(f.sql().execute(statement, &[], None));
        DISABLE_FK_CHILD_BATCH.set(false);
        assert_eq!(FK_CHILD_BATCHES.get() > checks, !disabled && expected_batches, "{statement}: {result:?}");
        if !disabled { assert_eq!(FK_CHILD_PROBES.get() > probes, expected_probes, "{statement}"); }
        let rows = ["SELECT * FROM p ORDER BY 1", "SELECT * FROM c ORDER BY 1"]
            .map(|sql| batch_rows(run(&mut f.sql(), sql)));
        (result, rows)
    };
    assert_eq!(exercise(false), exercise(true), "{statement}");
}

#[test]
fn batch_cross_type_converted_keys_and_whole_page_fallback() {
    for indexed in [false, true] {
        for parent in ["numeric(10,2)", "float8"] {
            let ddl = format!("CREATE TABLE p(id {parent} PRIMARY KEY)");
            let mut setup = vec![ddl.as_str(),
                "CREATE TABLE c(id int PRIMARY KEY, p int REFERENCES p ON DELETE CASCADE ON UPDATE CASCADE)",
                "INSERT INTO p VALUES(1.00),(2.00),(3.00)", "INSERT INTO c VALUES(1,1)"];
            if indexed { setup.push("CREATE INDEX cp ON c(p)"); }
            batch_differential(&setup, "DELETE FROM p WHERE id IN (1.00,2.00,3.00) RETURNING id", false, false);
            batch_differential(&setup, "UPDATE p SET id=id+10 WHERE id IN (1.00,2.00,3.00) RETURNING id", false, false);
        }
    }
}

#[test]
fn batch_composite_reordered_index_and_text_primary_prefix() {
    batch_differential(&[
        "CREATE TABLE p(a int,b int,PRIMARY KEY(a,b))",
        "CREATE TABLE c(id int PRIMARY KEY,a int,b int,FOREIGN KEY(a,b) REFERENCES p ON DELETE CASCADE)",
        "CREATE INDEX cb ON c(b,a)", "INSERT INTO p VALUES(1,2),(2,1),(3,4)",
        "INSERT INTO c VALUES(1,1,2)"], "DELETE FROM p RETURNING a,b", true, true);
    batch_differential(&[
        "CREATE TABLE p(id text PRIMARY KEY)",
        "CREATE TABLE c(p text REFERENCES p ON DELETE CASCADE,id int,PRIMARY KEY(p,id))",
        "INSERT INTO p VALUES('a'),('ab'),('abc')", "INSERT INTO c VALUES('ab',1)"],
        "DELETE FROM p WHERE id IN ('a','ab') RETURNING id", true, true);
}

#[test]
fn batch_scan_cap_falls_back_to_later_point_probes() {
    let exercise = |disabled| {
        let f = Fixture::new();
        f.exec("CREATE TABLE p(id int PRIMARY KEY)");
        f.exec("CREATE TABLE c(id int PRIMARY KEY,p int REFERENCES p ON DELETE CASCADE)");
        f.exec("CREATE INDEX cp ON c(p)");
        f.exec("INSERT INTO p VALUES(1),(2),(100)");
        let children = (1..=30).map(|i| format!("({i},2)")).collect::<Vec<_>>().join(",");
        f.exec(&format!("INSERT INTO c VALUES {children}"));
        let checks = FK_CHILD_BATCHES.get(); let probes = FK_CHILD_PROBES.get();
        DISABLE_FK_CHILD_BATCH.set(disabled);
        let result = batch_outcome(f.sql().execute("DELETE FROM p WHERE id IN (1,100) RETURNING id", &[], None));
        DISABLE_FK_CHILD_BATCH.set(false);
        assert_eq!(FK_CHILD_BATCHES.get()-checks, usize::from(!disabled));
        assert_eq!(FK_CHILD_PROBES.get()-probes, if disabled { 2 } else { 1 });
        (result, batch_rows(run(&mut f.sql(), "SELECT * FROM p ORDER BY id")),
            batch_rows(run(&mut f.sql(), "SELECT * FROM c ORDER BY id")))
    };
    assert_eq!(exercise(false), exercise(true));
}

#[test]
fn batch_upsert_drain_matches_point_probes() {
    batch_differential(&[
        "CREATE TABLE p(id int PRIMARY KEY, k int UNIQUE)",
        "CREATE TABLE c(id int PRIMARY KEY,p int REFERENCES p(id) ON UPDATE CASCADE)",
        "CREATE INDEX cp ON c(p)", "INSERT INTO p VALUES(1,1),(2,2),(3,3)",
        "INSERT INTO c VALUES(1,2)"],
        "INSERT INTO p SELECT id+10,k FROM p ON CONFLICT(k) DO UPDATE SET id=excluded.id RETURNING id", true, true);
}

#[test]
fn batch_mixed_delete_update_actions_in_cascade_queue() {
    batch_differential(&[
        "CREATE TABLE r(id int PRIMARY KEY)",
        "CREATE TABLE p(id int PRIMARY KEY,a int REFERENCES r ON DELETE CASCADE,b int UNIQUE REFERENCES r ON DELETE SET NULL)",
        "CREATE INDEX pa ON p(a)", "CREATE INDEX pb ON p(b)",
        "CREATE TABLE c(id int PRIMARY KEY,p int REFERENCES p(b) ON DELETE CASCADE ON UPDATE SET NULL)",
        "CREATE INDEX cp ON c(p)", "INSERT INTO r VALUES(1),(2)",
        "INSERT INTO p VALUES(1,1,1),(2,NULL,2)", "INSERT INTO c VALUES(1,1),(2,2)"],
        "DELETE FROM r RETURNING id", true, true);
}


#[test]
fn batch_dangling_index_entry_preserves_normal_probe_error() {
    let exercise = |disabled| {
        let f = batch_cascade_fixture();
        f.exec("INSERT INTO c VALUES(1,2)");
        let db = f.admin().test_v2();
        let mut txn = db.begin(engine::txn::Isolation::ReadCommitted);
        let parts = db.catalog().parts(&mut txn).unwrap();
        let child = parts.tables.get("c").unwrap();
        let mut key = child.id.to_be_bytes().to_vec();
        key.extend(codec::pk_bytes(&[Int(1)]).unwrap());
        txn.lock_for_write(Keyspace::Rows, &key, None, &|| false).unwrap();
        txn.delete(Keyspace::Rows, key).unwrap();
        txn.commit().unwrap();
        let checks = FK_CHILD_BATCHES.get(); let probes = FK_CHILD_PROBES.get();
        DISABLE_FK_CHILD_BATCH.set(disabled);
        let result = batch_outcome(f.sql().execute(BATCH_DELETE, &[], None));
        DISABLE_FK_CHILD_BATCH.set(false);
        assert!(result.is_err(), "dangling entry must remain an error");
        assert_eq!(FK_CHILD_BATCHES.get()-checks, usize::from(!disabled));
        assert!(FK_CHILD_PROBES.get() > probes);
        (result, batch_rows(run(&mut f.sql(), "SELECT * FROM p ORDER BY id")),
            batch_rows(run(&mut f.sql(), "SELECT * FROM c ORDER BY id")))
    };
    assert_eq!(exercise(false), exercise(true));
}

#[test]
fn batch_cross_type_upsert_exact_and_fallback() {
    for parent in ["numeric(10,2)", "float8"] {
        for indexed in [false,true] {
            let ddl = format!("CREATE TABLE p(id {parent} PRIMARY KEY)");
            let mut setup = vec![ddl.as_str(),
                "CREATE TABLE c(id int PRIMARY KEY,p int REFERENCES p ON UPDATE CASCADE)",
                "INSERT INTO p VALUES(1.00),(2.00),(3.00)", "INSERT INTO c VALUES(1,1)"];
            if indexed { setup.push("CREATE INDEX cp ON c(p)"); }
            batch_differential(&setup,
                "INSERT INTO p SELECT id FROM p ON CONFLICT(id) DO UPDATE SET id=excluded.id+10 RETURNING id",
                indexed && parent != "float8", true);
        }
    }
}
