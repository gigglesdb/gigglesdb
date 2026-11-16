use super::common::net_cluster::{NetCluster, until};
use giggles_core::{
    CoreError, SqlSession,
    relational::Value,
    replicated::{BatchSink, ReplicaStore, ReplicateError},
};
use giggles_history::{
    History, Limits, Observation, Op, Outcome, State, Verdict, check,
    record::{Recorder, TransferWorkload, controls, window},
    write_jsonl,
};
use giggles_replication::{core_store::ReplicatedEngine, sink::SnapshotPolicy};
use std::{
    fs,
    path::PathBuf,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

/// v2 binds pg_current_xact_id()/pg_xact_status() only for clocked execution
/// (the pgwire path always supplies a StatementClock; plain `execute` has none).
fn clocked(
    s: &mut SqlSession,
    sql: &str,
    params: &[Value],
    expected: Option<u64>,
) -> Result<giggles_core::relational::SqlResult, CoreError> {
    s.execute_at(
        sql,
        params,
        expected,
        &giggles_core::relational::StatementClock {
            unavailable: None,
            // Fixed instants, as in giggles-core's v2_xact_tests; no SQL here reads the clock.
            transaction_us: 100,
            statement_us: 100,
            zone: None,
            wall: None,
        },
    )
}
const SCAN: &str = "SELECT id, bal FROM acct ORDER BY id";
fn scan(s: &mut SqlSession) -> Result<Vec<(i64, i64)>, CoreError> {
    Ok(clocked(s, SCAN, &[], None)?
        .rows
        .into_iter()
        .map(|r| {
            let [Value::Int(id), Value::Int(bal)] = r.values.as_slice() else {
                panic!("integer account row")
            };
            (*id, *bal)
        })
        .collect())
}
fn cluster() -> NetCluster {
    let c = NetCluster::new(
        "sql-history",
        [0x42; 16],
        SnapshotPolicy {
            every_entries: 20,
            margin: 5,
        },
    );
    c.execute("CREATE TABLE acct(id bigint PRIMARY KEY, bal bigint NOT NULL)");
    for id in 1..=8 {
        c.execute(&format!(
            "INSERT INTO acct VALUES ({id},1000) ON CONFLICT DO NOTHING"
        ));
    }
    c.converged();
    c
}
fn initial() -> State {
    (1..=8).map(|id| (id, 1000)).collect()
}
fn attempt(
    engine: &ReplicatedEngine,
    rec: &Recorder,
    logical: &str,
    ops: Vec<Op>,
) -> Option<(String, i64)> {
    let mut s = engine.admin().sql_session();
    let p = rec.invoke(logical, ops.clone());
    let before_commit = (|| -> Result<_, CoreError> {
        clocked(&mut s, "BEGIN", &[], None)?;
        if let Op::Transfer { from, to, amount } = ops[0] {
            if clocked(
                &mut s,
                &format!(
                    "UPDATE acct SET bal = bal - {amount} WHERE id = {from} AND bal >= {amount}"
                ),
                &[],
                None,
            )?
            .affected
                == 0
            {
                return Ok(None);
            }
            clocked(
                &mut s,
                &format!("UPDATE acct SET bal = bal + {amount} WHERE id = {to}"),
                &[],
                None,
            )?;
            let x = clocked(&mut s, "SELECT pg_current_xact_id()", &[], None)?;
            let Value::Int(xid) = x.rows[0].values[0] else {
                panic!("integer xid")
            };
            // Read back only the transfer's own rows (read set == write set).
            let own = clocked(
                &mut s,
                &format!("SELECT id, bal FROM acct WHERE id = {from} OR id = {to}"),
                &[],
                None,
            )?;
            let value = |key: i64| {
                own.rows.iter().find_map(|r| match r.values.as_slice() {
                    [Value::Int(id), Value::Int(bal)] if *id == key => Some(*bal),
                    _ => None,
                })
            };
            let observations = vec![
                Observation::Read { value: value(from) },
                Observation::Read { value: value(to) },
            ];
            return Ok(Some((Some(xid), observations)));
        }
        Ok(Some((
            None,
            vec![Observation::Scan {
                rows: scan(&mut s)?,
            }],
        )))
    })();
    let Ok(Some((xid, observations))) = before_commit else {
        let _ = clocked(&mut s, "ROLLBACK", &[], None);
        rec.complete(p, Outcome::Aborted, vec![]);
        return None;
    };
    let id = p.id.clone();
    let outcome = if clocked(&mut s, "COMMIT", &[], None).is_ok() {
        Outcome::Committed
    } else {
        Outcome::Indeterminate
    };
    rec.complete(p, outcome, observations);
    // A read-only attempt has no effect to resolve; it stays open to the horizon.
    (outcome == Outcome::Indeterminate)
        .then_some(())
        .and(xid.map(|xid| (id, xid)))
}
fn resolve(c: &NetCluster, rec: &Recorder, unknown: Vec<(String, i64)>) {
    for (id, xid) in unknown {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let (_, e) = c.leader();
            let mut s = e.admin().sql_session();
            let r = clocked(&mut s, &format!("SELECT pg_xact_status({xid})"), &[], None).unwrap();
            match &r.rows[0].values[0] {
                Value::Text(status) if status == "committed" => {
                    rec.resolve(&id, Outcome::Committed).unwrap();
                    break;
                }
                Value::Text(status) if status == "aborted" => {
                    rec.resolve(&id, Outcome::Aborted).unwrap();
                    break;
                }
                Value::Text(status) if status == "in progress" => {
                    assert!(Instant::now() < deadline, "unresolved {id} xid={xid}")
                }
                other => panic!("unexpected xid status: {other:?}"),
            }
            thread::sleep(Duration::from_millis(10));
        }
    }
}
fn final_scan(c: &NetCluster, rec: &Recorder) -> State {
    let (_, e) = c.leader();
    let p = rec.invoke("final", vec![Op::Scan { start: 1, end: 9 }]);
    let rows = scan(&mut e.admin().sql_session()).unwrap();
    rec.complete(
        p,
        Outcome::Committed,
        vec![Observation::Scan { rows: rows.clone() }],
    );
    rows.into_iter().collect()
}
fn run_window(c: &mut NetCluster, seed: u64, w: usize, state: State) -> (History, State) {
    let rec = Recorder::new();
    let publisher = RwLock::new(Some(c.leader().1));
    let next = AtomicUsize::new(0);
    let limit = AtomicUsize::new(50);
    let active = AtomicUsize::new(0);
    let unknown = Mutex::new(vec![]);
    thread::scope(|scope| {
        for client in 0..4 {
            let (rec, publisher, next, limit, active, unknown) =
                (&rec, &publisher, &next, &limit, &active, &unknown);
            scope.spawn(move || {
                let mut workload = TransferWorkload::new(seed * 16 + client, (1..=8).collect())
                    .read_committed_safe();
                loop {
                    let n = next.try_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
                        (n < limit.load(Ordering::SeqCst)).then_some(n + 1)
                    });
                    let Ok(n) = n else {
                        if next.load(Ordering::SeqCst) >= 100 {
                            break;
                        }
                        thread::sleep(Duration::from_millis(1));
                        continue;
                    };
                    let engine = loop {
                        let guard = publisher.read().unwrap();
                        if let Some(e) = guard.as_ref() {
                            active.fetch_add(1, Ordering::SeqCst);
                            break e.clone();
                        }
                        drop(guard);
                        thread::sleep(Duration::from_millis(1));
                    };
                    if let Some(u) = attempt(
                        &engine,
                        rec,
                        &format!("w{w}-c{client}-{n}"),
                        workload.next_ops(),
                    ) {
                        unknown.lock().unwrap().push(u);
                    }
                    drop(engine);
                    active.fetch_sub(1, Ordering::SeqCst);
                }
            });
        }
        until(|| rec.len() >= 40);
        if w != 0 {
            let leader = c.leader().0;
            let victim = if w == 1 { leader } else { (leader + 1) % 3 };
            *publisher.write().unwrap() = None;
            if w == 3 {
                until(|| active.load(Ordering::SeqCst) == 0);
                c.wipe(victim);
            } else {
                c.stop(victim);
                until(|| active.load(Ordering::SeqCst) == 0);
                c.start(victim);
            }
            *publisher.write().unwrap() = Some(c.leader().1);
        }
        limit.store(100, Ordering::SeqCst);
    });
    drop(publisher);
    c.converged();
    resolve(c, &rec, unknown.into_inner().unwrap());
    let final_state = final_scan(c, &rec);
    let h = window(state, &(1..=8).collect::<Vec<_>>(), rec.take()).unwrap();
    (h, final_state)
}
fn assert_pass_and_controls(h: &History) -> (u64, String) {
    let result = check(h, Limits::default()).unwrap();
    assert_eq!(
        result.verdict,
        Verdict::Pass,
        "explored={} reason={}",
        result.explored,
        result.reason
    );
    let mut names = vec![];
    for (name, control) in [
        (
            "stale",
            controls::stale_read as fn(&History) -> Option<History>,
        ),
        ("aborted", controls::aborted_but_visible),
        ("duplicate", controls::duplicate_apply),
    ] {
        let mutated = control(h).unwrap_or_else(|| panic!("missing {name} control"));
        let r = check(&mutated, Limits::default()).unwrap();
        assert_eq!(
            r.verdict,
            Verdict::Fail,
            "{name}: explored={} reason={}",
            r.explored,
            r.reason
        );
        names.push(format!("{name}=FAIL"));
    }
    (result.explored, names.join(","))
}
#[test]
fn replicated_sql_transfer_histories() {
    let started = Instant::now();
    let seeds = std::env::var("R4_HISTORY_SEEDS").unwrap_or_else(|_| "11,12,13".into());
    let out = std::env::var_os("R4_HISTORY_OUT").map(PathBuf::from);
    if let Some(dir) = &out {
        fs::create_dir_all(dir).unwrap();
    }
    let mut summary = String::new();
    for seed in seeds.split(',').map(|s| s.trim().parse::<u64>().unwrap()) {
        let mut c = cluster();
        let mut state = initial();
        for w in 0..4 {
            let (h, next) = run_window(&mut c, seed, w, state);
            state = next;
            if let Some(dir) = &out {
                write_jsonl(
                    &h,
                    fs::File::create(dir.join(format!("seed{seed}-w{w}.jsonl"))).unwrap(),
                )
                .unwrap();
            }
            let (explored, controls) = assert_pass_and_controls(&h);
            let count = |o| h.attempts.iter().filter(|a| a.outcome == o).count();
            let line = format!(
                "seed={seed} window={w} attempts={} committed={} aborted={} indeterminate?resolved={} verdict=PASS explored={explored} controls={controls}\n",
                h.attempts.len(),
                count(Outcome::Committed),
                count(Outcome::Aborted),
                count(Outcome::Indeterminate)
            );
            eprint!("{line}");
            summary.push_str(&line);
            if let Some(dir) = &out {
                fs::write(dir.join("summary.txt"), &summary).unwrap();
            }
        }
    }
    eprintln!("SQL history runtime: {:?}", started.elapsed());
}
struct ReadOnly;
impl BatchSink for ReadOnly {
    fn replicate(&self, _: Vec<u8>) -> Result<(), ReplicateError> {
        Err(ReplicateError::NotLeader)
    }
}
#[test]
fn stale_follower_read_is_rejected() {
    let mut c = cluster();
    let (clean, state) = run_window(&mut c, 11, 0, initial());
    assert_pass_and_controls(&clean);
    c.converged();
    let follower = (c.leader().0 + 1) % 3;
    c.stop(follower);
    let rec = Recorder::new();
    for n in 0..5 {
        let (_, e) = c.leader();
        assert!(
            attempt(
                &e,
                &rec,
                &format!("transfer-{n}"),
                vec![
                    Op::Transfer {
                        from: 1,
                        to: 2,
                        amount: 1
                    },
                    Op::Read { key: 1 },
                    Op::Read { key: 2 },
                ]
            )
            .is_none()
        );
    }
    assert_eq!(rec.len(), 5);
    {
        let store = ReplicaStore::open(&c.configs[follower].data_dir).unwrap();
        let admin = store
            .open_admin(
                Arc::new(ReadOnly),
                giggles_core::Limits::default(),
                64 << 20,
            )
            .unwrap();
        let p = rec.invoke("stale-follower", vec![Op::Scan { start: 1, end: 9 }]);
        rec.complete(
            p,
            Outcome::Committed,
            vec![Observation::Scan {
                rows: scan(&mut admin.sql_session()).unwrap(),
            }],
        );
    }
    let h = window(state, &(1..=8).collect::<Vec<_>>(), rec.take()).unwrap();
    assert!(
        h.attempts[..5]
            .iter()
            .all(|a| a.outcome == Outcome::Committed)
    );
    let result = check(&h, Limits::default()).unwrap();
    c.start(follower);
    c.converged();
    assert_eq!(result.verdict, Verdict::Fail, "{result:?}");
}
