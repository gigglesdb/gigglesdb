#![cfg(feature = "engine-rocksdb")]
//! R4-42 at process level: concurrent SQL transfer histories against three real
//! server processes, closed into bounded windows and checked for strict
//! serializability by the independent giggles-history checker.

use crate::replication_harness::Cluster;
use giggles_history::{
    Check, History, Limits, Observation, Op, Outcome, State, Verdict, check,
    record::{Recorder, TransferWorkload, controls, window},
    write_jsonl,
};
use std::{
    io::Write,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::watch;
use tokio_postgres::{Client, Config, NoTls, error::SqlState};

const ACCOUNTS: std::ops::RangeInclusive<i64> = 1..=8;
const CLIENTS: u64 = 4;
const WINDOWS: usize = 4;
const STATEMENT: Duration = Duration::from_secs(20);
const RECONNECT: Duration = Duration::from_secs(60);
const RESOLVE: Duration = Duration::from_secs(10);

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Fault {
    None,
    LeaderKill,
    FollowerKill,
}
impl Fault {
    fn of(window: usize) -> Self {
        match window {
            1 | 3 => Self::LeaderKill,
            2 => Self::FollowerKill,
            _ => Self::None,
        }
    }
    fn name(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::LeaderKill => "leader-kill",
            Self::FollowerKill => "follower-kill",
        }
    }
}

/// An attempt whose COMMIT reply was lost; `xid` is None for read-only attempts
/// and in the explicit R4_HISTORY_NO_XID mode.
struct Unknown {
    attempt: String,
    transfer: bool,
    xid: Option<i64>,
}

struct Shared {
    rec: Recorder,
    started: AtomicUsize,
    unknown: Mutex<Vec<Unknown>>,
    no_xid: bool,
}

fn env_list(name: &str, default: &[u64]) -> Vec<u64> {
    match std::env::var(name) {
        Ok(v) if !v.trim().is_empty() => v
            .split(',')
            .map(|s| {
                s.trim()
                    .parse()
                    .unwrap_or_else(|e| panic!("{name}: {s:?}: {e}"))
            })
            .collect(),
        _ => default.to_vec(),
    }
}

async fn stmt<T>(
    fut: impl std::future::Future<Output = Result<T, tokio_postgres::Error>>,
) -> Result<T, String> {
    match tokio::time::timeout(STATEMENT, fut).await {
        Ok(Ok(v)) => Ok(v),
        Ok(Err(e)) => Err(e.to_string()),
        Err(_) => Err("statement timed out".into()),
    }
}

/// Connect through the multi-host config; followers refuse with 57P03, so a
/// successful startup lands on the current leader.
async fn reconnect(config: &Config) -> Client {
    let deadline = Instant::now() + RECONNECT;
    loop {
        if let Ok(Ok((client, connection))) =
            tokio::time::timeout(Duration::from_secs(5), config.connect(NoTls)).await
        {
            tokio::spawn(async move {
                let _ = connection.await;
            });
            return client;
        }
        assert!(
            Instant::now() < deadline,
            "no leader accepted a session within {RECONNECT:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn scan_rows(rows: &[tokio_postgres::Row]) -> Vec<(i64, i64)> {
    rows.iter()
        .map(|r| (r.get::<_, i64>(0), r.get::<_, i64>(1)))
        .collect()
}

async fn rollback(client: &Client) {
    let _ = tokio::time::timeout(Duration::from_secs(5), client.batch_execute("ROLLBACK")).await;
}

/// Execute one attempt. Errors before COMMIT is sent abort it; an error or a
/// lost connection on COMMIT leaves it indeterminate with the scan as prefix.
async fn attempt(shared: &Shared, client: &Client, logical: &str, ops: Vec<Op>) {
    let transfer = match ops.first() {
        Some(Op::Transfer { from, to, amount }) => Some((*from, *to, *amount)),
        _ => None,
    };
    let scans = ops.iter().any(|op| matches!(op, Op::Scan { .. }));
    let p = shared.rec.invoke(logical, ops);
    let attempt_id = p.id.clone();
    let body = async {
        stmt(client.batch_execute("BEGIN")).await?;
        let mut xid = None;
        if let Some((from, to, amount)) = transfer {
            let debited = stmt(client.execute(
                "UPDATE acct SET bal = bal - $1 WHERE id = $2 AND bal >= $1",
                &[&amount, &from],
            ))
            .await?;
            if debited == 0 {
                return Ok(None);
            }
            stmt(client.execute(
                "UPDATE acct SET bal = bal + $1 WHERE id = $2",
                &[&amount, &to],
            ))
            .await?;
            if !shared.no_xid {
                match tokio::time::timeout(
                    STATEMENT,
                    client.query_one("SELECT pg_current_xact_id()", &[]),
                )
                .await
                {
                    Ok(Ok(row)) => xid = Some(row.get::<_, i64>(0)),
                    Ok(Err(e)) => {
                        // Unsupported is a harness gap, never a silent abort.
                        assert!(
                            !matches!(
                                e.code(),
                                Some(&SqlState::FEATURE_NOT_SUPPORTED)
                                    | Some(&SqlState::UNDEFINED_FUNCTION)
                            ),
                            "pg_current_xact_id() unsupported over pgwire ({e}); \
                             set R4_HISTORY_NO_XID=1 to run without durable resolution"
                        );
                        return Err(e.to_string());
                    }
                    Err(_) => return Err("statement timed out".into()),
                }
            }
        }
        let observations = if scans {
            let rows = stmt(client.query("SELECT id, bal FROM acct ORDER BY id", &[])).await?;
            vec![Observation::Scan {
                rows: scan_rows(&rows),
            }]
        } else if let Some((from, to, _)) = transfer {
            // Read back the transfer's own two rows (read set == write set).
            let rows = stmt(client.query(
                "SELECT id, bal FROM acct WHERE id = $1 OR id = $2",
                &[&from, &to],
            ))
            .await?;
            let value = |id: i64| {
                rows.iter()
                    .find(|row| row.get::<_, i64>(0) == id)
                    .map(|row| row.get::<_, i64>(1))
            };
            vec![
                Observation::Read { value: value(from) },
                Observation::Read { value: value(to) },
            ]
        } else {
            vec![]
        };
        Ok::<_, String>(Some((observations, xid)))
    };
    match body.await {
        Ok(None) | Err(_) => {
            rollback(client).await;
            shared.rec.complete(p, Outcome::Aborted, vec![]);
        }
        Ok(Some((observations, xid))) => match stmt(client.batch_execute("COMMIT")).await {
            Ok(()) => shared.rec.complete(p, Outcome::Committed, observations),
            Err(_) => {
                shared.rec.complete(p, Outcome::Indeterminate, observations);
                shared.unknown.lock().unwrap().push(Unknown {
                    attempt: attempt_id,
                    transfer: transfer.is_some(),
                    xid,
                });
            }
        },
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_client(
    shared: Arc<Shared>,
    config: Config,
    workload: Arc<Mutex<TransferWorkload>>,
    seed: u64,
    win: usize,
    c: u64,
    limit: usize,
    gate: Option<(usize, watch::Receiver<bool>)>,
) {
    let mut client: Option<Client> = None;
    for n in 0.. {
        let index = shared.started.fetch_add(1, Ordering::SeqCst);
        if index >= limit {
            return;
        }
        if let Some((mid, rx)) = &gate
            && index >= *mid
        {
            // Later attempts run once the fault is underway, earlier ones race it.
            let mut rx = rx.clone();
            let _ = rx.wait_for(|started| *started).await;
        }
        if client.as_ref().is_none_or(Client::is_closed) {
            client = Some(reconnect(&config).await);
        }
        let ops = workload.lock().unwrap().next_ops();
        let logical = format!("s{seed}-w{win}-c{c}-{n}");
        let live = client.as_ref().unwrap();
        attempt(&shared, live, &logical, ops).await;
    }
}

async fn inject(cluster: &mut Cluster, fault: Fault) {
    let deadline = Instant::now() + Duration::from_secs(30);
    match fault {
        Fault::None => {}
        Fault::LeaderKill => {
            let leader = cluster.leader().await;
            cluster.kill(leader);
            let replacement = cluster.leader().await;
            assert_ne!(replacement, leader);
            cluster.start(leader);
            cluster.follower_until(leader, deadline).await;
        }
        Fault::FollowerKill => {
            let leader = cluster.leader().await;
            let follower = (0..3).find(|&i| i != leader).unwrap();
            cluster.kill(follower);
            tokio::time::sleep(Duration::from_millis(300)).await;
            cluster.start(follower);
            cluster.follower_until(follower, deadline).await;
        }
    }
}

async fn xact_status(client: &Client, xid: i64) -> Outcome {
    let deadline = Instant::now() + RESOLVE;
    loop {
        let status: Option<String> = client
            .query_one("SELECT pg_xact_status($1)", &[&xid])
            .await
            .unwrap_or_else(|e| panic!("pg_xact_status({xid}): {e}"))
            .get(0);
        match status.as_deref() {
            Some("committed") => return Outcome::Committed,
            Some("aborted") => return Outcome::Aborted,
            Some("in progress") if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            other => panic!("pg_xact_status({xid}) did not resolve within {RESOLVE:?}: {other:?}"),
        }
    }
}

fn write_window(dir: &Path, seed: u64, win: usize, h: &History) -> PathBuf {
    std::fs::create_dir_all(dir).unwrap();
    let path = dir.join(format!("proc-seed{seed}-w{win}.jsonl"));
    let file = std::fs::File::create(&path).unwrap();
    write_jsonl(h, std::io::BufWriter::new(file)).unwrap();
    path
}

fn verdict(c: &Check) -> &'static str {
    match c.verdict {
        Verdict::Pass => "PASS",
        Verdict::Fail => "FAIL",
        Verdict::Inconclusive => "INCONCLUSIVE",
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sql_transfer_histories_under_leader_kills() {
    let seeds = env_list("R4_HISTORY_SEEDS", &[21, 22]);
    let attempts = env_list("R4_HISTORY_ATTEMPTS", &[60])[0] as usize;
    assert!(
        (2..=120).contains(&attempts),
        "R4_HISTORY_ATTEMPTS must be in 2..=120"
    );
    let out = std::env::var_os("R4_HISTORY_OUT").map(PathBuf::from);
    let no_xid = std::env::var("R4_HISTORY_NO_XID").is_ok_and(|v| v == "1");
    if no_xid {
        println!("R4_HISTORY_NO_XID=1: indeterminate transfers stay unresolved");
    }
    let accounts: Vec<i64> = ACCOUNTS.collect();
    let started = Instant::now();
    for seed in seeds {
        let mut cluster = Cluster::new();
        let (_, admin) = cluster.leader_client().await;
        admin
            .batch_execute("CREATE TABLE acct (id bigint PRIMARY KEY, bal bigint NOT NULL)")
            .await
            .unwrap();
        for id in ACCOUNTS {
            admin
                .execute("INSERT INTO acct (id, bal) VALUES ($1, 1000)", &[&id])
                .await
                .unwrap();
        }
        drop(admin);
        let config = cluster.config(&[0, 1, 2]);
        let workloads: Vec<_> = (0..CLIENTS)
            .map(|c| {
                // v2 SQL transactions run at engine read committed (storage/v2_sql.rs
                // BEGIN), so a full scan inside a transfer can show write skew that
                // no replication property explains (GD-0074 E105). Default to the
                // workload that read committed keeps serializable; set
                // R4_HISTORY_TRANSFER_SCANS=1 to reproduce the isolation gap.
                let workload = TransferWorkload::new(seed * 16 + c, accounts.clone());
                let scans = std::env::var("R4_HISTORY_TRANSFER_SCANS").is_ok_and(|v| v == "1");
                Arc::new(Mutex::new(if scans {
                    workload
                } else {
                    workload.read_committed_safe()
                }))
            })
            .collect();
        let mut initial: State = ACCOUNTS.map(|id| (id, 1000)).collect();
        for win in 0..WINDOWS {
            let fault = Fault::of(win);
            let window_start = Instant::now();
            let shared = Arc::new(Shared {
                rec: Recorder::new(),
                started: AtomicUsize::new(0),
                unknown: Mutex::new(vec![]),
                no_xid,
            });
            let mid = attempts / 2;
            let (fault_tx, fault_rx) = watch::channel(false);
            let gate = (fault != Fault::None).then_some((mid, fault_rx));
            let tasks: Vec<_> = (0..CLIENTS)
                .map(|c| {
                    tokio::spawn(run_client(
                        shared.clone(),
                        config.clone(),
                        workloads[c as usize].clone(),
                        seed,
                        win,
                        c,
                        attempts,
                        gate.clone(),
                    ))
                })
                .collect();
            if fault != Fault::None {
                while shared.started.load(Ordering::SeqCst) < mid {
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
                fault_tx.send(true).unwrap();
                inject(&mut cluster, fault).await;
            }
            for task in tasks {
                task.await.unwrap();
            }

            // Close the window: resolve lost replies through durable xid status.
            let (_, leader) = cluster.leader_client().await;
            let unknown = std::mem::take(&mut *shared.unknown.lock().unwrap());
            let (mut resolved_c, mut resolved_a, mut unresolved_w, mut unresolved_r) = (0, 0, 0, 0);
            for u in &unknown {
                match u.xid {
                    Some(xid) => {
                        let durable = xact_status(&leader, xid).await;
                        if durable == Outcome::Committed {
                            resolved_c += 1;
                        } else {
                            resolved_a += 1;
                        }
                        shared.rec.resolve(&u.attempt, durable).unwrap();
                    }
                    None if u.transfer => unresolved_w += 1,
                    None => unresolved_r += 1,
                }
            }
            // One quiescent committed scan closes the window.
            let scan = workloads[0].lock().unwrap().scan();
            let p = shared
                .rec
                .invoke(&format!("s{seed}-w{win}-final"), vec![scan]);
            leader.batch_execute("BEGIN").await.unwrap();
            let rows = scan_rows(
                &leader
                    .query("SELECT id, bal FROM acct ORDER BY id", &[])
                    .await
                    .unwrap(),
            );
            leader.batch_execute("COMMIT").await.unwrap();
            shared.rec.complete(
                p,
                Outcome::Committed,
                vec![Observation::Scan { rows: rows.clone() }],
            );
            drop(leader);

            let h = window(initial.clone(), &accounts, shared.rec.take()).unwrap();
            let count = |o: Outcome| h.attempts.iter().filter(|a| a.outcome == o).count();
            let (committed, aborted, indeterminate) = (
                count(Outcome::Committed),
                count(Outcome::Aborted),
                count(Outcome::Indeterminate),
            );
            let path = out.as_ref().map(|dir| write_window(dir, seed, win, &h));
            let result = check(&h, Limits::default()).unwrap();
            let mut control_verdicts = vec![];
            for (name, control) in [
                (
                    "stale_read",
                    controls::stale_read as fn(&History) -> Option<History>,
                ),
                ("aborted_but_visible", controls::aborted_but_visible),
                ("duplicate_apply", controls::duplicate_apply),
            ] {
                let v = control(&h).map(|m| verdict(&check(&m, Limits::default()).unwrap()));
                control_verdicts.push(format!("{name}:{}", v.unwrap_or("NONE")));
            }
            let status = if unresolved_w > 0 {
                " INDETERMINATE-UNRESOLVED"
            } else {
                ""
            };
            let line = format!(
                "seed={seed} window={win} fault={} attempts={} committed={committed} \
                 aborted={aborted} indeterminate={indeterminate} (resolved_committed={resolved_c} \
                 resolved_aborted={resolved_a} unresolved_readonly={unresolved_r} \
                 unresolved_no_xid={unresolved_w}) verdict={}{status} explored={} controls={} \
                 elapsed_ms={}",
                fault.name(),
                h.attempts.len(),
                verdict(&result),
                result.explored,
                control_verdicts.join(","),
                window_start.elapsed().as_millis()
            );
            println!("{line}");
            if let Some(dir) = &out {
                let mut summary = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(dir.join("proc-summary.txt"))
                    .unwrap();
                writeln!(summary, "{line}").unwrap();
            }
            if result.verdict != Verdict::Pass {
                let path = path.unwrap_or_else(|| {
                    write_window(
                        &std::env::temp_dir().join("r4-history-failures"),
                        seed,
                        win,
                        &h,
                    )
                });
                assert!(
                    unresolved_w > 0 && result.verdict == Verdict::Inconclusive,
                    "seed {seed} window {win}: {} explored={} reason={} diagnostics={:?} history={}",
                    verdict(&result),
                    result.explored,
                    result.reason,
                    result.diagnostics,
                    path.display()
                );
            }
            for v in &control_verdicts {
                assert!(
                    v.ends_with(":FAIL"),
                    "seed {seed} window {win}: control {v}, expected FAIL"
                );
            }
            initial = rows.into_iter().collect();
        }
    }
    println!("total elapsed {:?}", started.elapsed());
}
