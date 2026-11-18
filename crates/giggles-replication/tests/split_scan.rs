//! SQL scans across splits (GD-0075 R5-84): three ClusterNodes with real engines and transport;
//! a gateway scans a table page by page (forward and reverse, count, sum) while `Splitter`
//! splits the table's range again and again; a second gateway commits insert+update batches
//! meanwhile. Every scan sees each row exactly once and a prefix of the committed batches,
//! each whole (a statement reads at one timestamp, across StaleEpoch resumes).
use super::common::certs::{config_for, make_pki};
use giggles_core::{
    CoreError, Limits, SqlSession,
    cluster_node::{ClusterGateway, EngineSettings, GatewaySettings},
    range_api::{
        RangeClient, RangeError, RangeId, RangeSpace, ReadDirection, ReadRequest, SYSTEM_RANGE,
        range_key,
    },
    relational::{StatementClock, Value},
};
use giggles_replication::{
    bootstrap::presplit,
    client::{LocalRanges, RangeTransport, RoutedRangeClient},
    cluster_node::{ClusterNode, ClusterNodeConfig},
    split::Splitter,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    net::TcpListener,
    ops::Bound,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const CLUSTER: [u8; 16] = [84; 16];
const NODES: [u64; 3] = [1, 2, 3];
const DEADLINE: Duration = Duration::from_secs(60);
/// Rows loaded before the splits (ids 1..=N, payload -1).
const N: i64 = 2_000;
/// Rows each writer batch inserts (ids above N).
const BATCH: i64 = 4;
/// A small read page, so one scan of the table takes hundreds of pages.
const PAGE_BYTES: usize = 256;

struct Root(PathBuf);
impl Drop for Root {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Three nodes over TLS with the legacy layout: range 1 = system, range 2 = every user key.
fn start() -> (Root, Vec<ClusterNode>) {
    start_layout(true)
}

fn start_layout(legacy: bool) -> (Root, Vec<ClusterNode>) {
    // Tests of this file run in parallel: a counter keeps their roots apart even when
    // two start within one clock tick.
    static STARTED: AtomicUsize = AtomicUsize::new(0);
    let root = Root(std::env::temp_dir().join(format!(
        "split-scan-{}-{}-{}",
        std::process::id(),
        STARTED.fetch_add(1, Ordering::Relaxed),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )));
    let mut layout = presplit(&[], &NODES, 3).unwrap();
    if legacy {
        // Existing clusters have no separate reserved range. Preserve that layout
        // so membership probes must follow every split of the last user range.
        layout.pop();
        layout.last_mut().unwrap().end = None;
    }
    let pki = make_pki(&root.0.join("pki"), &NODES);
    let listeners: Vec<_> = NODES
        .iter()
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let addresses: BTreeMap<_, _> = NODES
        .iter()
        .zip(&listeners)
        .map(|(id, l)| (*id, l.local_addr().unwrap().to_string()))
        .collect();
    drop(listeners);
    let nodes: Vec<ClusterNode> = NODES
        .iter()
        .map(|&node| {
            ClusterNode::start(ClusterNodeConfig {
                transport: config_for(&pki, node, CLUSTER, &addresses),
                data_dir: root.0.join(format!("n{node}/data")),
                log_dir: root.0.join(format!("n{node}/log")),
                snap_dir: root.0.join(format!("n{node}/snap")),
                tick: Duration::from_millis(20),
                layout: layout.clone(),
                engine: EngineSettings {
                    lease: Duration::from_secs(3),
                    ..EngineSettings::default()
                },
                liveness: None,
                policy: None,
                call_limits: None,
            })
            .unwrap()
        })
        .collect();
    assert!(nodes[0].bootstrap_directory(DEADLINE).unwrap());
    for node in &nodes {
        node.wait_ready(DEADLINE).unwrap();
    }
    (root, nodes)
}

fn gateway(node: &ClusterNode) -> ClusterGateway {
    node.attach_gateway(
        GatewaySettings {
            node: node.node_id() as u16,
            heartbeat: Duration::from_millis(500),
            gateway_lease: Duration::from_secs(1),
            page_bytes: PAGE_BYTES,
            ..GatewaySettings::default()
        },
        Limits::default(),
    )
    .unwrap()
}

fn clock() -> StatementClock {
    StatementClock {
        unavailable: None,
        transaction_us: 100,
        statement_us: 100,
        zone: None,
        wall: None,
    }
}

fn sql(s: &mut SqlSession, statement: &str) -> Result<Vec<Vec<i64>>, CoreError> {
    Ok(s.execute_at(statement, &[], None, &clock())?
        .rows
        .into_iter()
        .map(|row| {
            row.values
                .into_iter()
                .map(|v| match v {
                    Value::Int(i) => i,
                    other => panic!("not an int: {other:?}"),
                })
                .collect()
        })
        .collect())
}

/// A routed client on `node` with an empty route cache (reads the directory).
fn fresh_client(node: &ClusterNode) -> RoutedRangeClient {
    let system = node
        .layout()
        .iter()
        .find(|d| d.id == SYSTEM_RANGE)
        .unwrap()
        .clone();
    RoutedRangeClient::new(
        node.node_id(),
        system,
        node.transport().calls().clone() as Arc<dyn RangeTransport>,
        node.host().clone() as Arc<dyn LocalRanges>,
    )
}

/// Retries `call` while a leader or route is moving.
fn retry<T>(mut call: impl FnMut() -> Result<T, RangeError>) -> T {
    let deadline = Instant::now() + DEADLINE;
    loop {
        match call() {
            Ok(value) => return value,
            Err(
                RangeError::NotLeader { .. }
                | RangeError::Unavailable(_)
                | RangeError::StaleEpoch { .. },
            ) if Instant::now() < deadline => thread::sleep(Duration::from_millis(20)),
            Err(e) => panic!("{e}"),
        }
    }
}

/// Every Rows user key, in key order (all of them are table t's: it is the only table).
fn row_keys(client: &RoutedRangeClient) -> Vec<Vec<u8>> {
    let mut keys = Vec::new();
    let mut start = Bound::Included(Vec::new());
    loop {
        let page = retry(|| {
            let desc = client.descriptor_for(&range_key(RangeSpace::Rows, &[]))?;
            client.read(ReadRequest {
                range: desc.id,
                epoch: desc.epoch,
                space: RangeSpace::Rows,
                start: start.clone(),
                end: Bound::Unbounded,
                dir: ReadDirection::Forward,
                ts: client.oracle().next(1)?,
                page_bytes: 1 << 20,
            })
        });
        // Versioned pairs: user key ++ 8-byte version.
        keys.extend(page.pairs.iter().map(|(k, _)| k[..k.len() - 8].to_vec()));
        match page.resume {
            Some(resume) => start = resume,
            None => return keys,
        }
    }
}

/// Splits the range holding user key `key` (Rows) on its leader; returns the right child's id.
fn split_at(nodes: &[ClusterNode], key: &[u8]) -> RangeId {
    let at = range_key(RangeSpace::Rows, key);
    let deadline = Instant::now() + DEADLINE;
    loop {
        let desc = retry(|| fresh_client(&nodes[0]).descriptor_for(&at));
        let attempt = nodes
            .iter()
            .find(|n| n.host().service(desc.id).is_some())
            .map(|leader| {
                Splitter::new(
                    leader.host().clone(),
                    leader.client().clone() as Arc<dyn RangeClient>,
                )
                .split(desc.id, &at)
            });
        match attempt {
            Some(Ok((_, right))) => return right.id,
            Some(Err(e)) => eprintln!("split of range {} at {at:02x?}: {e}", desc.id.0),
            None => {}
        }
        assert!(Instant::now() < deadline, "split at {at:02x?} timed out");
        thread::sleep(Duration::from_millis(20));
    }
}

/// Split progress: `started` splits began, `done` splits were applied and published;
/// `statements` scan statements began.
#[derive(Default)]
struct Splits {
    started: AtomicUsize,
    done: AtomicUsize,
    statements: AtomicUsize,
}

/// Committed writer batches: batch b inserts ids N+B*b+1..=N+B*(b+1) with payload b and sets
/// row b+1's payload to b, in one transaction.
#[derive(Default)]
struct Batches {
    committed: AtomicUsize,
}

fn batch_ids(b: i64) -> std::ops::RangeInclusive<i64> {
    N + BATCH * b + 1..=N + BATCH * (b + 1)
}

/// Checks one scan's (id, payload) rows; returns how many batches it saw.
fn check_rows(what: &str, rows: &[Vec<i64>], before: usize, after: usize) -> usize {
    let ids: BTreeSet<i64> = rows.iter().map(|r| r[0]).collect();
    assert_eq!(ids.len(), rows.len(), "{what}: duplicate ids");
    let seen = (rows.len() as i64 - N) / BATCH;
    let want: BTreeSet<i64> = (1..=N + BATCH * seen).collect();
    assert_eq!(
        ids,
        want,
        "{what}: missing {:?}, extra {:?}",
        want.difference(&ids).take(10).collect::<Vec<_>>(),
        ids.difference(&want).take(10).collect::<Vec<_>>()
    );
    let seen = seen as usize;
    assert!(
        (before..=after + 1).contains(&seen),
        "{what}: {seen} batches, {before} committed before and {after} after"
    );
    for row in rows {
        let (id, p) = (row[0], row[1]);
        let want = if id > N {
            (id - N - 1) / BATCH
        } else if id - 1 < seen as i64 {
            id - 1
        } else {
            -1
        };
        assert_eq!(p, want, "{what}: row {id} (saw {seen} batches)");
    }
    seen
}

/// The scan statements, run in this order: statement number n is `SCANS[n % 3]`.
const SCANS: [&str; 3] = [
    "SELECT id, p FROM t",
    "SELECT id, p FROM t ORDER BY id DESC",
    "SELECT count(*), sum(id) FROM t",
];

/// Scans through `s` until `stop`; returns the statements run and, per `SCANS` entry, how
/// many a whole split ran inside.
fn scan_loop(
    s: &mut SqlSession,
    stop: &AtomicBool,
    splits: &Splits,
    batches: &Batches,
) -> (usize, [usize; 3]) {
    let (mut scans, mut met) = (0, [0; 3]);
    while !stop.load(Ordering::Acquire) || scans < 4 {
        for (i, query) in SCANS.into_iter().enumerate() {
            let before = batches.committed.load(Ordering::Acquire);
            let started = splits.started.load(Ordering::Acquire);
            splits.statements.fetch_add(1, Ordering::AcqRel);
            let rows = sql(s, query).unwrap_or_else(|e| panic!("scan {scans} {query}: {e}"));
            // Split number `started` began after this statement did and was applied before
            // it ended.
            if splits.done.load(Ordering::Acquire) > started {
                met[i] += 1;
            }
            let after = batches.committed.load(Ordering::Acquire);
            let what = format!("scan {scans} {query}");
            match i {
                0 => {
                    check_rows(&what, &rows, before, after);
                }
                1 => {
                    check_rows(&what, &rows, before, after);
                    assert!(rows.windows(2).all(|w| w[0][0] > w[1][0]), "{what}: order");
                }
                _ => {
                    let (count, sum) = (rows[0][0], rows[0][1]);
                    let seen = (count - N) / BATCH;
                    assert_eq!(count, N + BATCH * seen, "{what}");
                    let top = N + BATCH * seen;
                    assert_eq!(sum, top * (top + 1) / 2, "{what}");
                    assert!(
                        (before as i64..=after as i64 + 1).contains(&seen),
                        "{what}: {seen} batches"
                    );
                }
            }
        }
        scans += 1;
    }
    (3 * scans, met)
}

/// Commits batches through `s` until `stop`; an ambiguous outcome is settled by reading.
/// Returns how many batches were retried, and how many of those failed with "no leader"
/// (a write reaching a split child before its first election, GD-0075 P9-9).
fn write_loop(s: &mut SqlSession, stop: &AtomicBool, batches: &Batches) -> (usize, usize) {
    let (mut retried, mut no_leader) = (0, 0);
    let mut b = 0_i64;
    while !stop.load(Ordering::Acquire) {
        let values: Vec<String> = batch_ids(b).map(|id| format!("({id}, {b})")).collect();
        let statements = [
            "BEGIN".to_owned(),
            format!("INSERT INTO t VALUES {}", values.join(", ")),
            format!("UPDATE t SET p = {b} WHERE id = {}", b + 1),
            "COMMIT".to_owned(),
        ];
        let outcome = statements
            .iter()
            .try_for_each(|statement| sql(s, statement).map(drop));
        if let Err(e) = outcome {
            eprintln!("batch {b}: {e}");
            no_leader += usize::from(e.to_string().contains("no leader"));
            let _ = sql(s, "ROLLBACK");
            let ids = batch_ids(b);
            let query = format!(
                "SELECT count(*) FROM t WHERE id >= {} AND id <= {}",
                ids.start(),
                ids.end()
            );
            let deadline = Instant::now() + DEADLINE;
            let found = loop {
                match sql(s, &query) {
                    Ok(rows) => break rows[0][0],
                    Err(e) => {
                        assert!(Instant::now() < deadline, "batch {b}: {e}");
                        thread::sleep(Duration::from_millis(20));
                    }
                }
            };
            retried += 1;
            match found {
                0 => continue,
                BATCH => {}
                other => panic!("batch {b}: {other} of {BATCH} rows committed"),
            }
        }
        b += 1;
        batches.committed.store(b as usize, Ordering::Release);
        thread::sleep(Duration::from_millis(10));
    }
    (retried, no_leader)
}

/// Runs scans on `sa` (and batches on `sb`) while the table splits at each of `keys`.
fn splits_during_scans(
    nodes: &[ClusterNode],
    keys: &[&[u8]],
    sa: &mut SqlSession,
    sb: Option<&mut SqlSession>,
    splits: &Splits,
    batches: &Batches,
) -> (usize, [usize; 3], (usize, usize)) {
    let stop = &AtomicBool::new(false);
    thread::scope(|scope| {
        let scanner = scope.spawn(move || scan_loop(sa, stop, splits, batches));
        let writer = sb.map(|sb| scope.spawn(move || write_loop(sb, stop, batches)));
        for key in keys {
            // Land the split just after a scan statement starts: forward and reverse scans
            // in turn.
            let target = splits.started.load(Ordering::Acquire) % 2;
            let seen = splits.statements.load(Ordering::Acquire);
            loop {
                let now = splits.statements.load(Ordering::Acquire);
                // A failed scanner stops counting: go on to join it.
                if (now != seen && (now - 1) % 3 == target) || scanner.is_finished() {
                    break;
                }
                thread::sleep(Duration::from_millis(1));
            }
            thread::sleep(Duration::from_millis(20));
            splits.started.fetch_add(1, Ordering::AcqRel);
            let t = Instant::now();
            let right = split_at(nodes, key);
            splits.done.fetch_add(1, Ordering::AcqRel);
            eprintln!("split -> range {} in {:?}", right.0, t.elapsed());
        }
        thread::sleep(Duration::from_millis(300));
        stop.store(true, Ordering::Release);
        let (scans, met) = scanner.join().unwrap();
        let retried = writer.map_or((0, 0), |w| w.join().unwrap());
        (scans, met, retried)
    })
}

#[test]
fn sql_scans_see_each_row_once_while_the_table_splits() {
    let began = Instant::now();
    let (_root, nodes) = start();
    let a = gateway(&nodes[0]);
    let mut sa = a.admin.sql_session();
    sql(&mut sa, "CREATE TABLE t(id INT PRIMARY KEY, p INT)").unwrap();
    for chunk in (1..=N).collect::<Vec<_>>().chunks(250) {
        let values: Vec<String> = chunk.iter().map(|id| format!("({id}, -1)")).collect();
        sql(
            &mut sa,
            &format!("INSERT INTO t VALUES {}", values.join(", ")),
        )
        .unwrap();
    }
    let keys = row_keys(&fresh_client(&nodes[0]));
    assert_eq!(keys.len(), N as usize);
    let table = keys[0][..8].to_vec();
    assert!(keys.iter().all(|k| k.starts_with(&table)));
    let at = |i: usize| keys[i].as_slice();
    let (splits, batches) = (Splits::default(), Batches::default());

    // 1. A static table: splits at the middle, then inside the right child (twice) and the
    // left one, while gateway A scans.
    let (scans, met, _) = splits_during_scans(
        &nodes,
        &[at(1000), at(1500), at(1250), at(500)],
        &mut sa,
        None,
        &splits,
        &batches,
    );
    eprintln!("static: {scans} statements, a whole split inside {met:?} of them");
    let mut total_met = met;

    // 2. Gateway B on another node commits batches while the scans and splits go on.
    let b = gateway(&nodes[1]);
    let mut sb = b.admin.sql_session();
    let (scans, met, (retried, no_leader)) = splits_during_scans(
        &nodes,
        &[at(250), at(1750), at(750)],
        &mut sa,
        Some(&mut sb),
        &splits,
        &batches,
    );
    let committed = batches.committed.load(Ordering::Acquire);
    eprintln!(
        "writes: {scans} statements, a whole split inside {met:?} of them; \
         {committed} batches ({retried} retried, {no_leader} on \"no leader\")"
    );
    // Gateway writes wait out a new child's first election (txn.rs retry, lock_in_range).
    assert_eq!(no_leader, 0, "batches failed with \"no leader\"");
    for (total, met) in total_met.iter_mut().zip(met) {
        *total += met;
    }
    assert!(committed > 0, "no batch committed");
    // Forward and reverse scans both met splits mid-scan.
    assert!(
        total_met[0] >= 1 && total_met[1] >= 1,
        "splits inside scans: {total_met:?}"
    );

    // 3. A fresh gateway on the third node sees every row; the table spans several ranges.
    let c = gateway(&nodes[2]);
    let mut sc = c.admin.sql_session();
    let rows = sql(&mut sc, "SELECT id, p FROM t").unwrap();
    assert_eq!(
        check_rows("gateway C", &rows, committed, committed),
        committed
    );
    let client = fresh_client(&nodes[2]);
    let end = range_key(
        RangeSpace::Rows,
        &(u64::from_be_bytes(table.clone().try_into().unwrap()) + 1).to_be_bytes(),
    );
    let mut key = range_key(RangeSpace::Rows, &table);
    let mut ranges = BTreeSet::new();
    loop {
        let desc = retry(|| client.descriptor_for(&key));
        ranges.insert(desc.id);
        match desc.end {
            Some(next) if next < end => key = next,
            _ => break,
        }
    }
    assert!(ranges.len() >= 4, "table span in {} ranges", ranges.len());
    assert_eq!(ranges.len(), 8, "{ranges:?}");
    eprintln!("split scan test took {:?}", began.elapsed());
    drop((sa, sb, sc, a, b, c));
    for node in nodes {
        node.stop();
    }
}

/// Keep splitting the range containing the highest Rows key. On the legacy layout
/// every split moves membership locks; new layouts leave them in their own range.
#[test]
fn max_range_splits_preserve_membership_and_exact_scans() {
    for legacy in [true, false] {
        let (_root, nodes) = start_layout(legacy);
        let a = gateway(&nodes[0]);
        let mut session = a.admin.sql_session();
        sql(&mut session, "CREATE TABLE t(id INT PRIMARY KEY, p INT)").unwrap();
        let values = (1..=N).map(|id| format!("({id}, -1)")).collect::<Vec<_>>();
        for chunk in values.chunks(250) {
            sql(
                &mut session,
                &format!("INSERT INTO t VALUES {}", chunk.join(", ")),
            )
            .unwrap();
        }
        let keys = row_keys(&fresh_client(&nodes[0]));
        let at = (1..=12)
            .map(|i| keys[i * 150].as_slice())
            .collect::<Vec<_>>();
        let (splits, batches) = (Splits::default(), Batches::default());
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            splits_during_scans(&nodes, &at, &mut session, None, &splits, &batches)
        }));
        let fences = a.membership_fences();
        eprintln!("max-range legacy={legacy}: fences={fences}");
        let (scans, _, _) = result.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
        assert_eq!(splits.done.load(Ordering::Acquire), 12);
        assert!(scans >= 12, "legacy={legacy}: {scans} scans");
        eprintln!(
            "max-range legacy={legacy}: 12 splits, {scans} exact scan statements, fences={fences}"
        );
        if !legacy {
            assert_eq!(
                fences, 0,
                "reserved layout must not fence during user splits"
            );
        }
        drop((session, a));
        for node in nodes {
            node.stop();
        }
    }
}
