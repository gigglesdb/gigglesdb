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
