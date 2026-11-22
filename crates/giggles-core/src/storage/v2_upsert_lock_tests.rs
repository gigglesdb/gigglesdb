//! F18b: bounded upsert sentinel admission and wait/re-read protocol.
use super::*;
use relational::Value::{Int, Null, Text};
use std::sync::mpsc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

struct Fixture { admin: Option<Admin>, path: std::path::PathBuf }
impl Fixture {
    fn new(ddl: &str) -> Self {
        let mut nonce = [0; 16];
        getrandom::fill(&mut nonce).unwrap();
        let path = std::env::temp_dir().join(format!("f18b-{:032x}", u128::from_be_bytes(nonce)));
        let admin = Some(Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap());
        let fixture = Self { admin, path };
        run(&mut fixture.sql(), ddl);
        fixture
    }
    fn users() -> Self {
        Self::new("CREATE TABLE users(id bigserial PRIMARY KEY,email text UNIQUE,name text)")
    }
    fn sql(&self) -> SqlSession { self.admin.as_ref().unwrap().sql_session() }
    fn rows(&self, sql: &str) -> Vec<Vec<relational::Value>> {
        run(&mut self.sql(), sql).rows.into_iter().map(|r| r.values).collect()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) { self.admin.take(); fs::remove_dir_all(&self.path).unwrap(); }
}
fn clock() -> relational::StatementClock {
    relational::StatementClock { unavailable: None, transaction_us: 1, statement_us: 1, zone: None, wall: None }
}
fn run(s: &mut SqlSession, sql: &str) -> relational::SqlResult {
    s.execute_at(sql, &[], None, &clock()).unwrap_or_else(|e| panic!("{sql}: {e:?}"))
}
fn upsert(email: &str, name: &str, nothing: bool) -> String {
    format!("INSERT INTO users(email,name) VALUES('{email}','{name}') ON CONFLICT(email) {}",
        if nothing { "DO NOTHING" } else { "DO UPDATE SET name=excluded.name" })
}
struct Outcome { result: relational::SqlResult, shared: usize, upgrades: usize }
struct Pending { done: mpsc::Receiver<Outcome>, worker: JoinHandle<()> }
impl Pending {
    fn start(f: &Fixture, sql: String) -> Self {
        Self::start_params(f, sql, vec![])
    }
    fn start_params(f: &Fixture, sql: String, params: Vec<relational::Value>) -> Self {
        let admin = f.admin.as_ref().unwrap().clone();
        let (send, done) = mpsc::channel();
        let worker = thread::spawn(move || {
            let mut s = admin.sql_session();
            run(&mut s, "BEGIN");
            let shared = UPSERT_SHARED.get();
            let upgrades = UPSERT_UPGRADE.get();
            let result = s.execute_at(&sql, &params, None, &clock())
                .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
            let outcome = Outcome { result, shared: UPSERT_SHARED.get() - shared,
                upgrades: UPSERT_UPGRADE.get() - upgrades };
            run(&mut s, "COMMIT");
            send.send(outcome).unwrap();
        });
        Self { done, worker }
    }
    fn blocked(&self, holder: &SqlSession) {
        let until = Instant::now() + Duration::from_secs(10);
        loop {
            assert!(matches!(self.done.try_recv(), Err(mpsc::TryRecvError::Empty)), "upsert completed before holder ended");
            if holder.v2_transaction.as_ref().unwrap().sql_waiting_count() > 0 { break; }
            assert!(Instant::now() < until, "upsert never entered a lock wait");
            thread::sleep(Duration::from_millis(1));
        }
    }
    fn finish(self) -> Outcome {
        let outcome = self.done.recv_timeout(Duration::from_secs(20)).expect("upsert hung");
        self.worker.join().unwrap();
        outcome
    }
    fn without_wait(self, holder: &SqlSession) -> Outcome {
        let until = Instant::now() + Duration::from_secs(20);
        let outcome = loop {
            assert_eq!(holder.v2_transaction.as_ref().unwrap().sql_waiting_count(), 0);
            match self.done.try_recv() {
                Ok(outcome) => break outcome,
                Err(mpsc::TryRecvError::Empty) => {}
                Err(mpsc::TryRecvError::Disconnected) => panic!("upsert worker exited"),
            }
            assert!(Instant::now() < until, "distinct upsert hung");
            thread::sleep(Duration::from_millis(1));
        };
        self.worker.join().unwrap();
        outcome
    }
}

#[test]
fn distinct_keys_update_and_do_nothing_do_not_wait() {
    for nothing in [false, true] {
        let f = Fixture::users();
        let mut a = f.sql(); run(&mut a, "BEGIN");
        let shared = UPSERT_SHARED.get();
        run(&mut a, &upsert("a", "A", nothing));
        assert_eq!(UPSERT_SHARED.get() - shared, 1);
        let second = Pending::start(&f, upsert("b", "B", nothing)).without_wait(&a);
        assert_eq!((second.shared, second.upgrades, second.result.affected), (1, 0, 1));
        run(&mut a, "COMMIT");
        assert_eq!(f.rows("SELECT email,name FROM users ORDER BY email"),
            vec![vec![Text("a".into()), Text("A".into())], vec![Text("b".into()), Text("B".into())]]);
    }
}
#[test]
fn same_key_waits_then_updates_or_inserts_after_commit_or_rollback() {
    for commit in [false, true] {
        let f = Fixture::users();
        let mut a = f.sql(); run(&mut a, "BEGIN");
        run(&mut a, &upsert("a", "A", false));
        let second = Pending::start(&f, upsert("a", "B", false));
        second.blocked(&a);
        run(&mut a, if commit { "COMMIT" } else { "ROLLBACK" });
        let second = second.finish();
        assert_eq!((second.shared, second.upgrades, second.result.affected), (1, usize::from(commit), 1));
        assert_eq!(f.rows("SELECT email,name FROM users"), vec![vec![Text("a".into()), Text("B".into())]]);
    }
}
#[test]
fn same_key_do_nothing_waits_then_skips_or_inserts() {
    for commit in [false, true] {
        let f = Fixture::users();
        let mut a = f.sql(); run(&mut a, "BEGIN");
        run(&mut a, &upsert("a", "A", true));
        let second = Pending::start(&f, upsert("a", "B", true));
        second.blocked(&a);
        run(&mut a, if commit { "COMMIT" } else { "ROLLBACK" });
        let second = second.finish();
        assert_eq!((second.shared, second.upgrades, second.result.affected),
            (1, usize::from(commit), u64::from(!commit)));
        assert_eq!(f.rows("SELECT name FROM users"), vec![vec![Text(if commit { "A" } else { "B" }.into())]]);
    }
}
#[test]
fn expression_values_do_nothing_waits_then_skips() {
    let f = Fixture::users();
    let sql = "INSERT INTO users(email) VALUES(lower($1)) ON CONFLICT(email) DO NOTHING";
    let mut a = f.sql();
    run(&mut a, "BEGIN");
    let before = UPSERT_SHARED.get();
    let first = a.execute_at(sql, &[Text("A".into())], None, &clock()).unwrap();
    assert_eq!(first.affected, 1);
    assert_eq!(UPSERT_SHARED.get(), before);
    let second = Pending::start_params(&f, sql.into(), vec![Text("A".into())]);
    second.blocked(&a);
    run(&mut a, "COMMIT");
    let second = second.finish();
    assert_eq!((second.shared, second.upgrades, second.result.affected), (0, 0, 0));
    assert_eq!(f.rows("SELECT email FROM users"), vec![vec![Text("a".into())]]);
}
#[test]
fn explicit_primary_target_locks_rows_and_refreshes() {
    for commit in [false, true] {
        let f = Fixture::new("CREATE TABLE users(id bigint PRIMARY KEY,name text)");
        let mut a = f.sql(); run(&mut a, "BEGIN");
        let before = UPSERT_SHARED.get();
        run(&mut a, "INSERT INTO users VALUES(1,'A') ON CONFLICT(id) DO UPDATE SET name=excluded.name");
        assert_eq!(UPSERT_SHARED.get() - before, 1);
        let second = Pending::start(&f, "INSERT INTO users VALUES(1,'B') ON CONFLICT(id) DO UPDATE SET name=excluded.name".into());
        second.blocked(&a);
        run(&mut a, if commit { "COMMIT" } else { "ROLLBACK" });
        let second = second.finish();
        assert_eq!((second.shared, second.upgrades), (1, 0));
        assert_eq!(f.rows("SELECT * FROM users"), vec![vec![Int(1), Text("B".into())]]);
    }
}
#[test]
fn parameters_casts_and_integer_to_bigint_are_shared() {
    let f = Fixture::users();
    let mut s = f.sql();
    let before = UPSERT_SHARED.get();
    s.execute_at("INSERT INTO users(id,email,name) VALUES(10,$1,'A') ON CONFLICT(email) DO UPDATE SET name=excluded.name",
        &[Text("a".into())], None, &clock()).unwrap();
    s.execute_at("INSERT INTO users(id,email,name) VALUES(11::bigint,$1::text,'B') ON CONFLICT(email) DO UPDATE SET name=excluded.name",
        &[Text("a".into())], None, &clock()).unwrap();
    assert_eq!(UPSERT_SHARED.get() - before, 2);
    assert_eq!(f.rows("SELECT * FROM users"), vec![vec![Int(10), Text("a".into()), Text("B".into())]]);
}
#[test]
fn committed_owner_is_prelocked_without_upgrade() {
    let f = Fixture::users();
    run(&mut f.sql(), &upsert("a", "A", false));
    let before = (UPSERT_SHARED.get(), UPSERT_UPGRADE.get());
    run(&mut f.sql(), &upsert("a", "B", false));
    assert_eq!((UPSERT_SHARED.get() - before.0, UPSERT_UPGRADE.get() - before.1), (1, 0));
    assert_eq!(f.rows("SELECT name FROM users"), vec![vec![Text("B".into())]]);
}
#[test]
fn null_and_omitted_targets_have_no_unique_witness() {
    let f = Fixture::users();
    let before = UPSERT_SHARED.get();
    run(&mut f.sql(), "INSERT INTO users(email,name) VALUES(NULL,'A'),(NULL,'B') ON CONFLICT(email) DO NOTHING");
    run(&mut f.sql(), "INSERT INTO users(name) VALUES('C') ON CONFLICT(email) DO NOTHING");
    assert_eq!(UPSERT_SHARED.get() - before, 2);
    assert_eq!(f.rows("SELECT email FROM users"), vec![vec![Null]; 3]);
}
#[test]
fn unsupported_values_row_count_set_and_isolation_fall_back() {
    let f = Fixture::users();
    let mut s = f.sql();
    let before = UPSERT_SHARED.get();
    // General VALUES expressions use the exclusive sentinel fallback, not UPSERT_SHARED.
    let sql = "INSERT INTO users(email) VALUES(lower($1)) ON CONFLICT(email) DO NOTHING";
    for affected in [1, 0] {
        let expression = s.execute_at(sql, &[Text("A".into())], None, &clock()).unwrap();
        assert_eq!(expression.affected, affected);
    }
    assert_eq!(f.rows("SELECT email FROM users"), vec![vec![Text("a".into())]]);
    assert_eq!(UPSERT_SHARED.get(), before);
    let rows = (0..201).map(|i| format!("('e{i}')")).collect::<Vec<_>>().join(",");
    assert_eq!(run(&mut s, &format!("INSERT INTO users(email) VALUES {rows} ON CONFLICT(email) DO NOTHING")).affected, 201);
    run(&mut s, "INSERT INTO users(email) VALUES('a') ON CONFLICT(email) DO UPDATE SET email=excluded.email");
    run(&mut s, "BEGIN");
    let db = f.admin.as_ref().unwrap().test_v2();
    s.v2_transaction = Some(db.begin(Isolation::RepeatableRead));
    run(&mut s, &upsert("b", "B", false));
    run(&mut s, "COMMIT");
    assert_eq!(UPSERT_SHARED.get(), before);
    assert_eq!(f.rows("SELECT count(*) FROM users"), vec![vec![Int(203)]]);
}
#[test]
fn defaulted_target_and_failed_coercion_fall_back() {
    let f = Fixture::new("CREATE TABLE users(id bigserial PRIMARY KEY,email varchar(1) DEFAULT 'x' UNIQUE,name text)");
    let mut s = f.sql();
    let before = UPSERT_SHARED.get();
    run(&mut s, "INSERT INTO users(name) VALUES('A') ON CONFLICT(email) DO NOTHING");
    let error = s.execute_at("INSERT INTO users(email) VALUES('long') ON CONFLICT(email) DO NOTHING", &[], None, &clock());
    assert_eq!(error, Err(CoreError::Sql(SqlError::StringDataRightTruncation)));
    assert_eq!(UPSERT_SHARED.get(), before);
    assert_eq!(f.rows("SELECT email,name FROM users"), vec![vec![Text("x".into()), Text("A".into())]]);
}
#[test]
fn sequence_defaults_are_consumed_once_per_proposed_row() {
    let f = Fixture::users();
    let mut s = f.sql();
    run(&mut s, "INSERT INTO users(email) VALUES('seed')");
    assert_eq!(run(&mut s, "SELECT currval('users_id_seq')").rows[0].values, vec![Int(1)]);
    run(&mut s, "INSERT INTO users(email) VALUES('a'),('b') ON CONFLICT(email) DO NOTHING");
    assert_eq!(run(&mut s, "SELECT currval('users_id_seq')").rows[0].values, vec![Int(3)]);
    run(&mut s, "INSERT INTO users(email) VALUES('a'),('c') ON CONFLICT(email) DO UPDATE SET name='B'");
    assert_eq!(run(&mut s, "SELECT currval('users_id_seq')").rows[0].values, vec![Int(5)]);
    assert_eq!(run(&mut s, "INSERT INTO users(email) VALUES('next') RETURNING id").rows[0].values, vec![Int(6)]);
}
