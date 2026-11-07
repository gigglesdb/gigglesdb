//! Independent concurrent-history oracle. No checker consults MVCC helpers or the
//! production read set; only the optional diagnostic fallback probe uses that set.
use super::{
    codec,
    commit::{CommitRequest, ManualClock, Sequencer, SequencerConfig},
    hlc::Ts,
    kv::{Direction, Keyspace, Kv},
    kv_mem::MemKv,
    locks::{LockConfig, LockManager, TxnId},
    ssi_validate::{ValidationConfig, Validator},
    txn::{Isolation, Transaction, TxnError},
};
use crate::relational::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Bound,
    sync::{Arc, Barrier},
    thread,
    time::{Duration, Instant},
};
type Payload = Vec<u8>;
type Overlay = BTreeMap<usize, Option<Payload>>;
#[derive(Clone, Debug)]
enum Observation {
    Get {
        snapshot: Ts,
        key: usize,
        value: Option<Payload>,
    },
    Scan {
        snapshot: Ts,
        a: usize,
        b: usize,
        dir: Direction,
        limit: Option<usize>,
        rows: Vec<(usize, Payload)>,
        limit_cut: bool,
    },
    Write {
        snapshot: Ts,
        key: usize,
        value: Option<Payload>,
        target: Option<Payload>,
        observed: Ts,
    },
}
impl Observation {
    fn snapshot(&self) -> Ts {
        match self {
            Self::Get { snapshot, .. }
            | Self::Scan { snapshot, .. }
            | Self::Write { snapshot, .. } => *snapshot,
        }
    }
}
#[derive(Clone, Debug)]
enum Outcome {
    Committed(Ts),
    ReadOnlyCommitted(Ts),
    Aborted(String),
}
#[derive(Clone, Debug)]
struct FailedOperation {
    index: usize,
    snapshot: Option<Ts>,
    planned: PlannedOp,
    reason: TxnError,
}
#[derive(Clone, Debug)]
struct HistoryTxn {
    failed: Option<FailedOperation>,
    id: u64,
    isolation: Isolation,
    ops: Vec<Observation>,
    outcome: Outcome,
}
impl HistoryTxn {
    fn attempted_ops(&self) -> usize {
        self.ops.len() + usize::from(self.failed.is_some())
    }
    fn writes(&self) -> Overlay {
        let mut out = Overlay::new();
        for op in &self.ops {
            if let Observation::Write { key, value, .. } = op {
                out.insert(*key, value.clone());
            }
        }
        out
    }
    fn committed(&self) -> Option<Ts> {
        match self.outcome {
            Outcome::Committed(ts) | Outcome::ReadOnlyCommitted(ts) => Some(ts),
            Outcome::Aborted(_) => None,
        }
    }
}
#[derive(Clone, Debug)]
struct Version {
    ts: Ts,
    writer: u64,
    value: Option<Payload>,
}
struct Model {
    versions: Vec<Vec<Version>>,
}
impl Model {
    fn new(history: &[HistoryTxn], keys: usize) -> Self {
        let mut versions = vec![vec![]; keys];
        for t in history {
            if let Some(ts) = t.committed() {
                for (key, value) in t.writes() {
                    versions[key].push(Version {
                        ts,
                        writer: t.id,
                        value,
                    });
                }
            }
        }
        for versions in &mut versions {
            versions.sort_by_key(|v| v.ts);
            assert!(versions.windows(2).all(|w| w[0].ts < w[1].ts));
        }
        Self { versions }
    }
    fn value(&self, key: usize, ts: Ts) -> Option<Payload> {
        let versions = &self.versions[key];
        let end = versions.partition_point(|v| v.ts <= ts);
        versions[..end].last().and_then(|v| v.value.clone())
    }
    fn rows(
        &self,
        ts: Ts,
        own: &Overlay,
        a: usize,
        b: usize,
        dir: Direction,
        limit: Option<usize>,
    ) -> Vec<(usize, Payload)> {
        let mut out: Vec<_> = (a..b)
            .filter_map(|key| {
                own.get(&key)
                    .cloned()
                    .unwrap_or_else(|| self.value(key, ts))
                    .map(|v| (key, v))
            })
            .collect();
        if dir == Direction::Reverse {
            out.reverse();
        }
        if let Some(n) = limit {
            out.truncate(n);
        }
        out
    }
}
fn user_key(n: usize) -> Vec<u8> {
    let mut key = 1u64.to_be_bytes().to_vec();
    key.extend(codec::pk_bytes(&[Value::Int(n as i64)]).unwrap());
    key
}
fn payload(writer: u64, seq: u64) -> Payload {
    let mut v = writer.to_be_bytes().to_vec();
    v.extend(seq.to_be_bytes());
    v
}
fn decode_key(key: &[u8], keys: usize) -> usize {
    (0..keys)
        .find(|n| user_key(*n) == key)
        .unwrap_or_else(|| panic!("unknown user key {key:?}"))
}
fn next(rng: &mut u64) -> u64 {
    *rng ^= *rng << 13;
    *rng ^= *rng >> 7;
    *rng ^= *rng << 17;
    *rng
}
fn seed_for(seed: u64, worker: usize) -> u64 {
    seed.wrapping_add((worker as u64 + 1).wrapping_mul(0x9e3779b97f4a7c15))
        .max(1)
}
fn legal_abort(e: &TxnError) -> bool {
    matches!(
        e,
        TxnError::Deadlock | TxnError::LockTimeout | TxnError::Serialization(_)
    )
}
fn check_c1(history: &[HistoryTxn]) -> Result<(), String> {
    let committed: BTreeSet<_> = history
        .iter()
        .filter(|t| t.committed().is_some())
        .flat_map(|t| {
            t.ops.iter().filter_map(|op| match op {
                Observation::Write {
                    value: Some(value), ..
                } => Some(value.clone()),
                _ => None,
            })
        })
        .collect();
    for t in history {
        let mut own = Overlay::new();
        for (i, op) in t.ops.iter().enumerate() {
            let values: Vec<&Payload> = match op {
                Observation::Get { value, .. } => value.iter().collect(),
                Observation::Scan { rows, .. } => rows.iter().map(|(_, v)| v).collect(),
                Observation::Write { target, .. } => target.iter().collect(),
            };
            for value in values {
                if !committed.contains(value) && !own.values().any(|v| v.as_ref() == Some(value)) {
                    return Err(format!(
                        "C1 txn={} op={i} dirty value={value:?} history={t:?}",
                        t.id
                    ));
                }
            }
            if let Observation::Write { key, value, .. } = op {
                own.insert(*key, value.clone());
            }
        }
    }
    Ok(())
}
fn check_c2(history: &[HistoryTxn], model: &Model) -> Result<(), String> {
    for t in history {
        if let Some(failed) = &t.failed {
            if failed.index != t.ops.len() || !legal_abort(&failed.reason) {
                return Err(format!(
                    "C2 failed-operation metadata txn={} index={} planned={:?} reason={:?} history={t:?}",
                    t.id, failed.index, failed.planned, failed.reason
                ));
            }
            if t.isolation != Isolation::ReadCommitted
                && let Some(first) = t.ops.first()
                && failed.snapshot != Some(first.snapshot())
            {
                return Err(format!(
                    "C2 failed-operation repeatability txn={} op={} planned={:?} expected={:?} actual={:?} history={t:?}",
                    t.id,
                    failed.index,
                    failed.planned,
                    first.snapshot(),
                    failed.snapshot
                ));
            }
        }
        let mut own = Overlay::new();
        for (i, op) in t.ops.iter().enumerate() {
            if t.isolation != Isolation::ReadCommitted {
                let expected = t.ops[0].snapshot();
                let actual = op.snapshot();
                if actual != expected {
                    return Err(format!(
                        "C2 repeatability txn={} op={i} expected={expected:?} actual={actual:?} history={t:?}",
                        t.id
                    ));
                }
            }
            match op {
                Observation::Get {
                    snapshot,
                    key,
                    value,
                } => {
                    let expected = own
                        .get(key)
                        .cloned()
                        .unwrap_or_else(|| model.value(*key, *snapshot));
                    if *value != expected {
                        return Err(format!(
                            "C2 txn={} op={i} S={snapshot:?} expected={expected:?} actual={value:?} history={t:?}",
                            t.id
                        ));
                    }
                }
                Observation::Scan {
                    snapshot,
                    a,
                    b,
                    dir,
                    limit,
                    rows,
                    ..
                } => {
                    let expected = model.rows(*snapshot, &own, *a, *b, *dir, *limit);
                    if *rows != expected {
                        return Err(format!(
                            "C2 txn={} op={i} S={snapshot:?} expected={expected:?} actual={rows:?} history={t:?}",
                            t.id
                        ));
                    }
                }
                Observation::Write { key, value, .. } => {
                    own.insert(*key, value.clone());
                }
            }
        }
    }
    Ok(())
}
fn intervening(versions: &[Version], snapshot: Ts, commit: Ts) -> &[Version] {
    let start = versions.partition_point(|v| v.ts <= snapshot);
    let end = versions.partition_point(|v| v.ts < commit);
    &versions[start.min(end)..end]
}
fn check_c3(history: &[HistoryTxn], model: &Model) -> Result<(), String> {
    for t in history {
        if t.isolation == Isolation::ReadCommitted {
            continue;
        }
        if let Some(commit) = t.committed()
            && let Some(first) = t.ops.first()
        {
            let snapshot = first.snapshot();
            for key in t.writes().keys() {
                if let Some(v) = intervening(&model.versions[*key], snapshot, commit)
                    .iter()
                    .find(|v| v.writer != t.id)
                {
                    return Err(format!(
                        "C3 txn={} key={key} S={snapshot:?} commit={commit:?} intervening={v:?} history={t:?}",
                        t.id
                    ));
                }
            }
        }
    }
    Ok(())
}
fn check_c4(history: &[HistoryTxn], model: &Model) -> Result<(), String> {
    for t in history {
        let mut own = Overlay::new();
        for (i, op) in t.ops.iter().enumerate() {
            if let Observation::Write {
                key,
                value,
                target,
                observed,
                ..
            } = op
            {
                if t.isolation == Isolation::ReadCommitted {
                    let expected = own
                        .get(key)
                        .cloned()
                        .unwrap_or_else(|| model.value(*key, *observed));
                    if *target != expected {
                        return Err(format!(
                            "C4 txn={} op={i} observed={observed:?} expected={expected:?} actual={target:?} history={t:?}",
                            t.id
                        ));
                    }
                }
                own.insert(*key, value.clone());
            }
        }
    }
    Ok(())
}
/// Inclusive consumed endpoint when a limit cuts a scan; otherwise its original
/// half-open bounds. This uses returned history, never production read_set.
fn covered(op: &Observation, key: usize) -> bool {
    match op {
        Observation::Get { key: point, .. } => key == *point,
        Observation::Write { .. } => false,
        Observation::Scan {
            a,
            b,
            dir,
            limit,
            rows,
            limit_cut,
            ..
        } => {
            if key < *a || key >= *b {
                return false;
            }
            if limit == &Some(0) {
                return false;
            }
            if *limit_cut && let Some((last, _)) = rows.last() {
                return match dir {
                    Direction::Forward => key <= *last,
                    Direction::Reverse => key >= *last,
                };
            }
            true
        }
    }
}
fn check_c5(history: &[HistoryTxn], model: &Model) -> Result<(), String> {
    for t in history {
        if t.isolation != Isolation::Serializable {
            continue;
        }
        let Some(commit) = t.committed() else {
            continue;
        };
        let writes = t.writes();
        let point = if writes.is_empty() {
            commit
        } else {
            Ts(commit.0 - 1)
        };
        for (i, op) in t.ops.iter().enumerate() {
            if matches!(op, Observation::Write { .. }) {
                continue;
            }
            let snapshot = op.snapshot();
            for (key, versions) in model.versions.iter().enumerate() {
                if covered(op, key)
                    && let Some(v) = intervening(versions, snapshot, commit)
                        .iter()
                        .find(|v| v.writer != t.id)
                {
                    return Err(format!(
                        "C5 txn={} op={i} key={key} S={snapshot:?} commit={commit:?} intervening={v:?} history={t:?}",
                        t.id
                    ));
                }
            }
            match op {
                Observation::Get { key, value, .. } => {
                    if !writes.contains_key(key) {
                        let expected = model.value(*key, point);
                        if *value != expected {
                            return Err(format!(
                                "C5 txn={} op={i} point={point:?} expected={expected:?} actual={value:?} history={t:?}",
                                t.id
                            ));
                        }
                    }
                }
                Observation::Scan { rows, dir, .. } => {
                    let mut expected: Vec<_> = (0..model.versions.len())
                        .filter(|key| !writes.contains_key(key) && covered(op, *key))
                        .filter_map(|key| model.value(key, point).map(|v| (key, v)))
                        .collect();
                    if *dir == Direction::Reverse {
                        expected.reverse();
                    }
                    let actual: Vec<_> = rows
                        .iter()
                        .filter(|(key, _)| !writes.contains_key(key))
                        .cloned()
                        .collect();
                    if actual != expected {
                        return Err(format!(
                            "C5 txn={} op={i} point={point:?} expected={expected:?} actual={actual:?} history={t:?}",
                            t.id
                        ));
                    }
                }
                Observation::Write { .. } => unreachable!(),
            }
        }
    }
    Ok(())
}
fn check_all(seed: u64, history: &[HistoryTxn], keys: usize) -> Model {
    let mut writer_timestamps = BTreeMap::new();
    for t in history {
        if let Outcome::Committed(ts) = t.outcome
            && let Some(prior) = writer_timestamps.insert(ts, t)
        {
            panic!(
                "history seed={seed:#x}: duplicate writer timestamp {ts:?}, prior={prior:?}, actual={t:?}"
            );
        }
    }
    let model = Model::new(history, keys);
    for result in [
        check_c1(history),
        check_c2(history, &model),
        check_c3(history, &model),
        check_c4(history, &model),
        check_c5(history, &model),
    ] {
        if let Err(error) = result {
            panic!("history seed={seed:#x}: {error}");
        }
    }
    model
}
#[derive(Clone, Copy, Debug)]
enum PlannedOp {
    Get(usize),
    Scan {
        a: usize,
        b: usize,
        dir: Direction,
        limit: Option<usize>,
    },
    Put(usize),
    Delete(usize),
}
#[derive(Clone, Debug)]
struct Plan {
    isolation: Isolation,
    ops: Vec<PlannedOp>,
    rollback: bool,
}
fn plan(rng: &mut u64, keys: usize, isolation: Option<Isolation>) -> Plan {
    let isolation = isolation.unwrap_or_else(|| match next(rng) % 3 {
        0 => Isolation::ReadCommitted,
        1 => Isolation::RepeatableRead,
        _ => Isolation::Serializable,
    });
    let count = next(rng) % 6 + 1;
    let mut ops = Vec::new();
    for _ in 0..count {
        ops.push(match next(rng) % 4 {
            0 => PlannedOp::Get((next(rng) % keys as u64) as usize),
            1 => {
                let x = (next(rng) % (keys + 1) as u64) as usize;
                let y = (next(rng) % (keys + 1) as u64) as usize;
                let dir = if next(rng).is_multiple_of(2) {
                    Direction::Forward
                } else {
                    Direction::Reverse
                };
                let limit = if next(rng).is_multiple_of(2) {
                    None
                } else {
                    Some((next(rng) % 4 + 1) as usize)
                };
                PlannedOp::Scan {
                    a: x.min(y),
                    b: x.max(y),
                    dir,
                    limit,
                }
            }
            2 => PlannedOp::Put((next(rng) % keys as u64) as usize),
            _ => PlannedOp::Delete((next(rng) % keys as u64) as usize),
        });
    }
    Plan {
        isolation,
        ops,
        rollback: next(rng).is_multiple_of(10),
    }
}
fn apply(
    t: &mut Transaction,
    seq: &Sequencer,
    id: u64,
    index: usize,
    op: PlannedOp,
    keys: usize,
) -> Result<Observation, TxnError> {
    match op {
        PlannedOp::Get(key) => {
            let value = t.get(Keyspace::Rows, &user_key(key))?;
            Ok(Observation::Get {
                snapshot: t.current_snapshot_ts().unwrap(),
                key,
                value,
            })
        }
        PlannedOp::Scan { a, b, dir, limit } => {
            let rows = t
                .scan(
                    Keyspace::Rows,
                    Bound::Included(&user_key(a)),
                    Bound::Excluded(&user_key(b)),
                    dir,
                    limit,
                )?
                .map(|r| r.map(|(k, v)| (decode_key(&k, keys), v)))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Observation::Scan {
                snapshot: t.current_snapshot_ts().unwrap(),
                a,
                b,
                dir,
                limit,
                limit_cut: limit.is_some_and(|n| rows.len() == n),
                rows,
            })
        }
        PlannedOp::Put(key) | PlannedOp::Delete(key) => {
            let target = t.lock_for_write(
                Keyspace::Rows,
                &user_key(key),
                Some(Instant::now() + Duration::from_secs(2)),
                &|| false,
            )?;
            let observed = seq.visible_ts();
            let value = if matches!(op, PlannedOp::Put(_)) {
                Some(payload(id, index as u64))
            } else {
                None
            };
            if let Some(v) = &value {
                t.put(Keyspace::Rows, user_key(key), v.clone())?;
            } else {
                t.delete(Keyspace::Rows, user_key(key))?;
            }
            Ok(Observation::Write {
                snapshot: t.current_snapshot_ts().unwrap(),
                key,
                value,
                target: target.current,
                observed,
            })
        }
    }
}
fn fixture(keys: usize, tiny: bool) -> (Arc<MemKv>, Arc<Sequencer>, Arc<LockManager>, HistoryTxn) {
    let mem = Arc::new(MemKv::new());
    let seq = Sequencer::open(
        Arc::new(Kv::new(mem.clone())),
        Arc::new(ManualClock::new(0)),
        SequencerConfig {
            recent_log_bytes: if tiny {
                0
            } else {
                SequencerConfig::default().recent_log_bytes
            },
            ..SequencerConfig::default()
        },
    )
    .unwrap();
    let locks = Arc::new(LockManager::new(LockConfig {
        deadlock_recheck: Duration::from_millis(2),
        ..LockConfig::default()
    }));
    let mut t = Transaction::begin(&seq, &locks, TxnId(0), Isolation::ReadCommitted);
    let mut ops = Vec::new();
    for key in 0..keys {
        t.begin_statement();
        ops.push(apply(&mut t, &seq, 0, key, PlannedOp::Put(key), keys).unwrap());
    }
    let ts = t.commit().unwrap();
    (
        mem,
        seq,
        locks,
        HistoryTxn {
            failed: None,
            id: 0,
            isolation: Isolation::ReadCommitted,
            ops,
            outcome: Outcome::Committed(ts),
        },
    )
}
fn probe(t: &Transaction, seq: &Sequencer) -> usize {
    let Some(snapshot) = t.current_snapshot_ts() else {
        return 0;
    };
    let validator = Arc::new(Validator::new(
        t.read_set().clone(),
        ValidationConfig::default(),
    ));
    let hook = validator.clone();
    let result = seq.submit(CommitRequest {
        snapshot,
        writes: BTreeMap::new(),
        validation: Some(Box::new(move |log, kv, snapshot, _| {
            hook.validate(log, kv, snapshot)
        })),
    });
    if let Err(e) = result {
        assert!(legal_abort(&e), "unexpected diagnostic error {e:?}");
    }
    validator.fallback_count()
}
fn execute(
    seq: &Arc<Sequencer>,
    locks: &Arc<LockManager>,
    id: u64,
    plan: &Plan,
    keys: usize,
    tiny: bool,
    before_commit: impl FnOnce(),
) -> (HistoryTxn, usize) {
    let mut t = Transaction::begin(seq, locks, TxnId(id), plan.isolation);
    let mut ops = Vec::new();
    let mut abort = None;
    let mut failed = None;
    for (index, op) in plan.ops.iter().enumerate() {
        if plan.isolation == Isolation::ReadCommitted {
            t.begin_statement();
        }
        match apply(&mut t, seq, id, index, *op, keys) {
            Ok(op) => ops.push(op),
            Err(e) => {
                assert!(
                    legal_abort(&e),
                    "unexpected error txn={id} op={index} plan={plan:?}: {e:?}"
                );
                failed = Some(FailedOperation {
                    index,
                    snapshot: t.current_snapshot_ts(),
                    planned: *op,
                    reason: e.clone(),
                });
                abort = Some(format!("{e:?}"));
                break;
            }
        }
    }
    before_commit();
    let (outcome, fallbacks) = if let Some(reason) = abort {
        t.rollback();
        (Outcome::Aborted(reason), 0)
    } else if plan.rollback {
        t.rollback();
        (Outcome::Aborted("Rollback".into()), 0)
    } else {
        // Probe only: real Transaction::commit below always remains authoritative.
        // A counted overflow and monotonic horizon establish that the same active
        // snapshot's subsequent real hook also requires fallback (or sees a conflict).
        let fallbacks = if tiny && plan.isolation == Isolation::Serializable {
            probe(&t, seq)
        } else {
            0
        };
        let writing = ops.iter().any(|op| matches!(op, Observation::Write { .. }));
        let outcome = match t.commit() {
            Ok(ts) => {
                if writing {
                    Outcome::Committed(ts)
                } else {
                    Outcome::ReadOnlyCommitted(ts)
                }
            }
            Err(e) => {
                assert!(
                    legal_abort(&e),
                    "unexpected commit error txn={id} plan={plan:?}: {e:?}"
                );
                Outcome::Aborted(format!("{e:?}"))
            }
        };
        (outcome, fallbacks)
    };
    (
        HistoryTxn {
            failed,
            id,
            isolation: plan.isolation,
            ops,
            outcome,
        },
        fallbacks,
    )
}
fn run_history(
    seed: u64,
    isolation: Option<Isolation>,
    tiny: bool,
    workers: usize,
    transactions: usize,
    keys: usize,
) {
    let started = Instant::now();
    let (mem, seq, locks, setup) = fixture(keys, tiny);
    let plans: Vec<Vec<Plan>> = (0..workers)
        .map(|worker| {
            let mut rng = seed_for(seed, worker);
            (0..transactions)
                .map(|_| plan(&mut rng, keys, isolation))
                .collect()
        })
        .collect();
    if tiny {
        assert!(
            plans[0][0]
                .ops
                .iter()
                .all(|op| matches!(op, PlannedOp::Get(_) | PlannedOp::Scan { .. }))
                && !plans[0][0].rollback
        );
        assert!(
            plans[1][0]
                .ops
                .iter()
                .any(|op| matches!(op, PlannedOp::Put(_) | PlannedOp::Delete(_)))
                && !plans[1][0].rollback
        );
    }
    let start = Arc::new(Barrier::new(workers));
    let prefix = Arc::new(Barrier::new(workers));
    let (reader_ready_s, reader_ready_r) = std::sync::mpsc::channel();
    let (writer_done_s, writer_done_r) = std::sync::mpsc::channel();
    let mut reader_ready = Some(reader_ready_r);
    let mut writer_done = Some(writer_done_r);
    let threads: Vec<_> = plans
        .into_iter()
        .enumerate()
        .map(|(worker, plans)| {
            let seq = seq.clone();
            let locks = locks.clone();
            let start = start.clone();
            let prefix = prefix.clone();
            let ready_s = reader_ready_s.clone();
            let done_s = writer_done_s.clone();
            let ready_r = if worker == 1 {
                reader_ready.take()
            } else {
                None
            };
            let done_r = if worker == 0 {
                writer_done.take()
            } else {
                None
            };
            thread::spawn(move || {
                start.wait();
                let mut history = Vec::new();
                let mut fallbacks = 0;
                for (round, plan) in plans.iter().enumerate() {
                    if tiny && round == 0 {
                        if worker == 1 {
                            ready_r.as_ref().unwrap().recv().unwrap();
                        } else if worker > 1 {
                            prefix.wait();
                        }
                    }
                    let id = 1 + (worker * transactions + round) as u64;
                    let (record, count) = execute(&seq, &locks, id, plan, keys, tiny, || {
                        if tiny && round == 0 && worker == 0 {
                            ready_s.send(()).unwrap();
                            done_r.as_ref().unwrap().recv().unwrap();
                        }
                    });
                    if tiny && round == 0 && worker == 1 {
                        assert!(
                            record.committed().is_some(),
                            "scheduled writer must commit: {record:?}"
                        );
                        done_s.send(()).unwrap();
                    }
                    history.push(record);
                    fallbacks += count;
                    if tiny && round == 0 && worker < 2 {
                        prefix.wait();
                    }
                }
                (history, fallbacks)
            })
        })
        .collect();
    let mut history = vec![setup];
    let mut fallbacks = 0;
    for (worker_id, worker) in threads.into_iter().enumerate() {
        let (records, count) = worker.join().unwrap_or_else(|error| {
            eprintln!("history worker panic seed={seed:#x} worker={worker_id}");
            std::panic::resume_unwind(error)
        });
        history.extend(records);
        fallbacks += count;
    }
    if tiny {
        assert!(
            fallbacks > 0,
            "seed={seed}: scheduled probe must exercise fallback"
        );
    }
    let model = check_all(seed, &history, keys);
    // C6: derive final state and timestamp solely from acknowledged history.
    // An engine-visible clock that lags acknowledgments cannot truncate this oracle.
    let model_final_ts = history
        .iter()
        .filter_map(|t| match t.outcome {
            Outcome::Committed(ts) => Some(ts),
            _ => None,
        })
        .max()
        .expect("setup commits a writer");
    assert_eq!(
        seq.visible_ts(),
        model_final_ts,
        "C6 seed={seed:#x} live visible expected={model_final_ts:?}"
    );
    let reopened = Sequencer::open(
        Arc::new(Kv::new(Arc::new(mem.crash()))),
        Arc::new(ManualClock::new(0)),
        SequencerConfig::default(),
    )
    .unwrap_or_else(|error| panic!("C6 seed={seed:#x} reopen expected=success actual={error:?}"));
    let mut read = Transaction::begin(&reopened, &locks, TxnId(u64::MAX), Isolation::ReadCommitted);
    for key in 0..keys {
        let expected = model.value(key, Ts(u64::MAX));
        let actual = read
            .get(Keyspace::Rows, &user_key(key))
            .unwrap_or_else(|error| {
                panic!("C6 seed={seed:#x} key={key} expected={expected:?} actual=error({error:?})")
            });
        assert_eq!(
            actual, expected,
            "C6 seed={seed:#x} key={key} final={model_final_ts:?}"
        );
    }
    read.rollback();
    assert_eq!(
        reopened.visible_ts(),
        model_final_ts,
        "C6 seed={seed:#x} recovered meta timestamp expected={model_final_ts:?}"
    );
    let mut committed = 0;
    let mut readonly = 0;
    let mut aborts = BTreeMap::<String, usize>::new();
    let mut operations = 0;
    let mut successful_ops = 0;
    for t in history.iter().filter(|t| t.id != 0) {
        operations += t.attempted_ops();
        successful_ops += t.ops.len();
        match &t.outcome {
            Outcome::Committed(_) => committed += 1,
            Outcome::ReadOnlyCommitted(_) => readonly += 1,
            Outcome::Aborted(reason) => *aborts.entry(reason.clone()).or_default() += 1,
        }
    }
    eprintln!(
        "history seed={seed:#x} isolation={isolation:?} tiny={tiny} committed={committed} read_only_committed={readonly} aborts={aborts:?} ops={operations} successful_ops={successful_ops} diagnostic_fallbacks={fallbacks} elapsed={:?}",
        started.elapsed()
    );
}
const SEEDS: [u64; 3] = [1, 3, 4];
#[test]
fn history_rc() {
    for seed in SEEDS {
        run_history(seed, Some(Isolation::ReadCommitted), false, 8, 150, 8);
    }
}
#[test]
fn history_rr() {
    for seed in SEEDS {
        run_history(seed, Some(Isolation::RepeatableRead), false, 8, 150, 8);
    }
}
#[test]
fn history_ser() {
    for seed in SEEDS {
        run_history(seed, Some(Isolation::Serializable), false, 8, 150, 8);
    }
}
#[test]
fn history_mixed() {
    for seed in SEEDS {
        run_history(seed, None, false, 8, 150, 8);
    }
}
#[test]
fn history_ser_tiny_log() {
    for seed in SEEDS {
        run_history(seed, Some(Isolation::Serializable), true, 8, 150, 8);
    }
}
#[test]
#[ignore = "manual long history: 16 workers x 2000 transactions, 32 keys"]
fn history_long() {
    run_history(0x1badd00d, None, false, 16, 2000, 32);
}

#[test]
fn n1_rr_write_skew_detected() {
    let started = Instant::now();
    let mut committed = 0;
    let mut operations = 0;
    let mut violations = 0;
    for repeat in 0..24 {
        let (_, seq, locks, setup) = fixture(2, false);
        let mut a = Transaction::begin(&seq, &locks, TxnId(1), Isolation::RepeatableRead);
        let mut b = Transaction::begin(&seq, &locks, TxnId(2), Isolation::RepeatableRead);
        let mut a_ops = Vec::new();
        let mut b_ops = Vec::new();
        for key in 0..2 {
            a_ops.push(apply(&mut a, &seq, 1, key, PlannedOp::Get(key), 2).unwrap());
            b_ops.push(apply(&mut b, &seq, 2, key, PlannedOp::Get(key), 2).unwrap());
        }
        a_ops.push(apply(&mut a, &seq, 1, 2, PlannedOp::Put(0), 2).unwrap());
        b_ops.push(apply(&mut b, &seq, 2, 2, PlannedOp::Put(1), 2).unwrap());
        let mut history = vec![
            setup,
            HistoryTxn {
                failed: None,
                id: 1,
                isolation: Isolation::RepeatableRead,
                ops: a_ops,
                outcome: Outcome::Committed(a.commit().unwrap()),
            },
            HistoryTxn {
                failed: None,
                id: 2,
                isolation: Isolation::RepeatableRead,
                ops: b_ops,
                outcome: Outcome::Committed(b.commit().unwrap()),
            },
        ];
        let model = Model::new(&history, 2);
        assert!(check_c1(&history).is_ok());
        assert!(check_c2(&history, &model).is_ok());
        assert!(check_c3(&history, &model).is_ok());
        assert!(check_c4(&history, &model).is_ok());
        for t in history.iter_mut().skip(1) {
            t.isolation = Isolation::Serializable;
        }
        assert!(
            check_c5(&history, &model).is_err(),
            "negative RR skew escaped C5 repeat={repeat} history={history:?}"
        );
        violations += 1;
        committed += history
            .iter()
            .skip(1)
            .filter(|t| t.committed().is_some())
            .count();
        operations += history.iter().skip(1).map(|t| t.ops.len()).sum::<usize>();
    }
    eprintln!(
        "n1 repeats={violations} committed={committed} read_only_committed=0 aborts=0 ops={operations} C5_violations={violations} diagnostic_fallbacks=0 elapsed={:?}",
        started.elapsed()
    );
}
#[test]
fn n2_corrupted_history_detected() {
    let started = Instant::now();
    let (_, seq, locks, setup) = fixture(2, false);
    let mut t = Transaction::begin(&seq, &locks, TxnId(1), Isolation::Serializable);
    let op = apply(&mut t, &seq, 1, 0, PlannedOp::Get(0), 2).unwrap();
    let mut history = vec![
        setup,
        HistoryTxn {
            failed: None,
            id: 1,
            isolation: Isolation::Serializable,
            ops: vec![op],
            outcome: Outcome::ReadOnlyCommitted(t.commit().unwrap()),
        },
    ];
    let model = Model::new(&history, 2);
    assert!(check_c2(&history, &model).is_ok());
    assert!(check_c5(&history, &model).is_ok());
    if let Observation::Get { value, .. } = &mut history[1].ops[0] {
        *value = Some(payload(0, 1));
    } else {
        unreachable!()
    };
    assert!(
        check_c1(&history).is_ok(),
        "corruption deliberately uses another committed ID"
    );
    let c2_detected = check_c2(&history, &model).is_err();
    let c5_detected = check_c5(&history, &model).is_err();
    assert!(c2_detected);
    assert!(c5_detected);
    let readonly = history
        .iter()
        .skip(1)
        .filter(|t| matches!(t.outcome, Outcome::ReadOnlyCommitted(_)))
        .count();
    let operations: usize = history.iter().skip(1).map(|t| t.ops.len()).sum();
    eprintln!(
        "n2 committed=0 read_only_committed={readonly} aborts=0 ops={operations} C1_pass=true C2_detected={c2_detected} C5_detected={c5_detected} diagnostic_fallbacks=0 elapsed={:?}",
        started.elapsed()
    );
}
