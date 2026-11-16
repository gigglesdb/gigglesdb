//! Gateway transactions against real range engines (GD-0075 R5-42, R5-51/R5-53 wiring).
use super::*;
use crate::range_api::{
    Epoch, HomeOp, HomeReply, LockGrant, Outcome, ReadPage, ReadRequest, SYSTEM_RANGE,
    SequenceReply, TsOracle,
};
use crate::storage::cluster::db::local::{LocalCluster, engine_config};
use crate::storage::engine::txn::{Transaction, TxnServices};
use std::{ops::Bound, sync::mpsc};

fn row(table: u64, pk: &[u8]) -> Vec<u8> {
    let mut key = table.to_be_bytes().to_vec();
    key.extend_from_slice(pk);
    key
}

/// Ranges: system | rows of tables < 10 | table 10 below pk "m" | table 10 from "m" up to
/// table 20 | rows of tables >= 20 | index entries.
fn splits() -> Vec<Vec<u8>> {
    vec![
        range_key(RangeSpace::Rows, &10u64.to_be_bytes()),
        range_key(RangeSpace::Rows, &row(10, b"m")),
        range_key(RangeSpace::Rows, &20u64.to_be_bytes()),
        range_key(RangeSpace::Index, &[]),
    ]
}

fn cluster() -> Arc<LocalCluster> {
    LocalCluster::new(&splits())
}

fn services(cluster: &Arc<LocalCluster>, config: ClusterConfig) -> Arc<ClusterTxnServices> {
    ClusterTxnServices::new(cluster.clone(), config)
}

fn begin(services: &Arc<ClusterTxnServices>, isolation: Isolation) -> Transaction {
    Transaction::begin_with(
        TxnServices::Cluster(services.clone()),
        services.next_txn_id(),
        isolation,
    )
}

fn never() -> bool {
    false
}

fn put_row(txn: &mut Transaction, table: u64, pk: &[u8], value: &[u8]) {
    let key = row(table, pk);
    txn.lock_for_write(Keyspace::Rows, &key, None, &never)
        .unwrap();
    txn.put(Keyspace::Rows, key, value.to_vec()).unwrap();
}

/// The committed value of a row, read by a fresh statement.
fn read(gw: &Arc<ClusterTxnServices>, table: u64, pk: &[u8]) -> Option<Vec<u8>> {
    let mut txn = begin(gw, Isolation::ReadCommitted);
    txn.get(Keyspace::Rows, &row(table, pk)).unwrap()
}

#[test]
fn single_range_commit_is_visible_to_later_statements_only() {
    let cluster = cluster();
    let gw = services(&cluster, ClusterConfig::default());
    let mut reader = begin(&gw, Isolation::ReadCommitted);
    assert_eq!(reader.get(Keyspace::Rows, &row(10, b"a")).unwrap(), None);

    let mut txn = begin(&gw, Isolation::ReadCommitted);
    put_row(&mut txn, 10, b"a", b"one");
    put_row(&mut txn, 10, b"b", b"two");
    let ts = txn.commit().unwrap();

    // The old statement keeps its timestamp; a new statement sees the commit.
    assert_eq!(reader.get(Keyspace::Rows, &row(10, b"a")).unwrap(), None);
    reader.begin_statement();
    assert!(reader.current_snapshot_ts().unwrap() > ts);
    assert_eq!(
        reader.get(Keyspace::Rows, &row(10, b"a")).unwrap(),
        Some(b"one".to_vec())
    );
    let scanned: Vec<_> = reader
        .scan(
            Keyspace::Rows,
            Bound::Included(&row(10, b"")),
            Bound::Excluded(&row(11, b"")),
            crate::storage::engine::kv::Direction::Forward,
            None,
        )
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert_eq!(scanned.len(), 2);
}

#[test]
fn read_your_writes_and_latest_value_through_the_lock() {
    let cluster = cluster();
    let gw = services(&cluster, ClusterConfig::default());
    let mut first = begin(&gw, Isolation::ReadCommitted);
    put_row(&mut first, 10, b"a", b"v1");
    first.commit().unwrap();
    let mut txn = begin(&gw, Isolation::ReadCommitted);
    let target = txn
        .lock_for_write(Keyspace::Rows, &row(10, b"a"), None, &never)
        .unwrap();
    assert_eq!(target.current, Some(b"v1".to_vec()));
    txn.put(Keyspace::Rows, row(10, b"a"), b"v2".to_vec())
        .unwrap();
    assert_eq!(
        txn.get(Keyspace::Rows, &row(10, b"a")).unwrap(),
        Some(b"v2".to_vec())
    );
}

#[test]
fn a_commit_across_ranges_is_atomic_and_frees_every_lock() {
    let cluster = cluster();
    let gw = services(&cluster, ClusterConfig::default());
    let left = cluster.range_of(RangeSpace::Rows, &row(10, b"a"));
    let right = cluster.range_of(RangeSpace::Rows, &row(10, b"z"));
    assert_ne!(left, right);
    let mut txn = begin(&gw, Isolation::ReadCommitted);
    put_row(&mut txn, 10, b"a", b"x");
    put_row(&mut txn, 10, b"z", b"y");
    // A range locked but not written is released too.
    txn.lock_for_write(Keyspace::Rows, &row(30, b"k"), None, &never)
        .unwrap();
    txn.commit().unwrap();
    assert_eq!(read(&gw, 10, b"a"), Some(b"x".to_vec()));
    assert_eq!(read(&gw, 10, b"z"), Some(b"y".to_vec()));
    // Every key is free again.
    let mut other = begin(&gw, Isolation::ReadCommitted);
    let soon = || Some(Instant::now() + Duration::from_millis(500));
    for key in [row(10, b"a"), row(10, b"z"), row(30, b"k")] {
        other
            .lock_for_write(Keyspace::Rows, &key, soon(), &never)
            .unwrap();
    }
}

#[test]
fn a_cross_range_commit_with_a_lost_participant_aborts_everywhere() {
    let cluster = cluster();
    let gw = services(
        &cluster,
        ClusterConfig {
            retries: 2,
            ..ClusterConfig::default()
        },
    );
    let right = cluster.range_of(RangeSpace::Rows, &row(10, b"z"));
    let mut txn = begin(&gw, Isolation::ReadCommitted);
    put_row(&mut txn, 10, b"a", b"x");
    put_row(&mut txn, 10, b"z", b"y");
    // A new leader for the right range: the transaction's locks there are gone.
    cluster.restart(right);
    let error = txn.commit().unwrap_err();
    assert_eq!(error.sqlstate(), "40001", "{error:?}");
    assert_eq!(read(&gw, 10, b"a"), None);
    assert_eq!(read(&gw, 10, b"z"), None);
}

#[test]
fn exclusive_sentinel_blocks_shared_writers_in_every_range_of_the_table() {
    let cluster = cluster();
    let gw = services(&cluster, ClusterConfig::default());
    let scanner = begin(&gw, Isolation::ReadCommitted);
    scanner
        .lock_sql_sentinel(10, LockMode::Exclusive, &never)
        .unwrap();
    for pk in [b"a", b"z"] {
        let gw = gw.clone();
        let (done, wait) = mpsc::channel();
        let handle = thread::spawn(move || {
            let mut writer = begin(&gw, Isolation::ReadCommitted);
            writer
                .lock_sql_sentinel(10, LockMode::Shared, &never)
                .unwrap();
            let deadline = Some(Instant::now() + Duration::from_millis(300));
            let result = writer.lock_for_write(Keyspace::Rows, &row(10, pk), deadline, &never);
            done.send(result.err()).unwrap();
        });
        assert_eq!(
            wait.recv_timeout(Duration::from_secs(10)).unwrap(),
            Some(TxnError::LockTimeout)
        );
        handle.join().unwrap();
    }
    // Another table's writers are not blocked.
    let mut other = begin(&gw, Isolation::ReadCommitted);
    other
        .lock_sql_sentinel(30, LockMode::Shared, &never)
        .unwrap();
    put_row(&mut other, 30, b"a", b"x");
    drop(scanner);
    let mut writer = begin(&gw, Isolation::ReadCommitted);
    writer
        .lock_sql_sentinel(10, LockMode::Shared, &never)
        .unwrap();
    put_row(&mut writer, 10, b"z", b"y");
}

#[test]
fn shared_sentinel_is_taken_only_in_ranges_written() {
    let cluster = cluster();
    let gw = services(&cluster, ClusterConfig::default());
    let mut writer = begin(&gw, Isolation::ReadCommitted);
    writer
        .lock_sql_sentinel(10, LockMode::Shared, &never)
        .unwrap();
    put_row(&mut writer, 10, b"a", b"x");
    // A scanning writer can still lock the right half, then waits on the left one.
    let gw2 = gw.clone();
    let (done, wait) = mpsc::channel();
    let handle = thread::spawn(move || {
        let scanner = begin(&gw2, Isolation::ReadCommitted);
        let result = scanner.lock_sql_sentinel(10, LockMode::Exclusive, &never);
        done.send(result.err()).unwrap();
    });
    assert!(wait.recv_timeout(Duration::from_millis(300)).is_err());
    writer.commit().unwrap();
    assert_eq!(wait.recv_timeout(Duration::from_secs(10)).unwrap(), None);
    handle.join().unwrap();
}

#[test]
fn a_blocked_lock_observes_cancellation() {
    let cluster = cluster();
    let gw = services(
        &cluster,
        ClusterConfig {
            lock_slice: Duration::from_millis(20),
            ..ClusterConfig::default()
        },
    );
    let mut holder = begin(&gw, Isolation::ReadCommitted);
    put_row(&mut holder, 10, b"a", b"x");
    let mut waiter = begin(&gw, Isolation::ReadCommitted);
    let start = Instant::now();
    let cancel = move || start.elapsed() > Duration::from_millis(100);
    let error = waiter
        .lock_for_write(Keyspace::Rows, &row(10, b"a"), None, &cancel)
        .unwrap_err();
    assert_eq!(error, TxnError::Cancelled);
}

/// Range leases of 300 ms: a transaction holding locks longer survives only by heartbeats.
fn short_lease_cluster() -> Arc<LocalCluster> {
    LocalCluster::with_config(
        &splits(),
        crate::storage::range::engine::EngineConfig {
            lease: Duration::from_millis(300),
            sweep: Duration::from_millis(20),
            ..engine_config()
        },
    )
}

#[test]
fn heartbeats_keep_range_locks_past_the_lease() {
    let cluster = short_lease_cluster();
    let gw = services(
        &cluster,
        ClusterConfig {
            heartbeat: Duration::from_millis(50),
            ..ClusterConfig::default()
        },
    );
    let mut txn = begin(&gw, Isolation::ReadCommitted);
    put_row(&mut txn, 10, b"a", b"x");
    thread::sleep(Duration::from_millis(1000));
    txn.commit().unwrap();
    assert_eq!(read(&gw, 10, b"a"), Some(b"x".to_vec()));
}

#[test]
fn an_expired_lease_fails_the_transaction_with_a_retryable_error() {
    let cluster = short_lease_cluster();
    let gw = services(
        &cluster,
        ClusterConfig {
            heartbeat: Duration::from_secs(60),
            ..ClusterConfig::default()
        },
    );
    let mut txn = begin(&gw, Isolation::ReadCommitted);
    put_row(&mut txn, 10, b"a", b"x");
    thread::sleep(Duration::from_millis(1000));
    assert_eq!(txn.commit().unwrap_err().sqlstate(), "40001");
    assert_eq!(read(&gw, 10, b"a"), None);
}

#[test]
fn a_new_leader_fails_the_transaction_with_a_retryable_error() {
    let cluster = cluster();
    let gw = services(&cluster, ClusterConfig::default());
    let range = cluster.range_of(RangeSpace::Rows, &row(10, b"a"));
    let mut txn = begin(&gw, Isolation::ReadCommitted);
    put_row(&mut txn, 10, b"a", b"x");
    cluster.restart(range);
    assert_eq!(
        txn.commit().unwrap_err().sqlstate(),
        "40001",
        "lost locks abort with serialization_failure"
    );
    assert_eq!(read(&gw, 10, b"a"), None);
}

#[test]
fn a_grant_from_a_new_lock_table_aborts_before_commit() {
    let cluster = cluster();
    let gw = services(&cluster, ClusterConfig::default());
    let range = cluster.range_of(RangeSpace::Rows, &row(10, b"a"));
    let mut txn = begin(&gw, Isolation::ReadCommitted);
    // A Shared sentinel only: commit's row-lock check could not see it go.
    txn.lock_sql_sentinel(10, LockMode::Shared, &never).unwrap();
    put_row(&mut txn, 10, b"a", b"x");
    cluster.restart(range);
    let error = txn
        .lock_for_write(Keyspace::Rows, &row(10, b"b"), None, &never)
        .unwrap_err();
    assert_eq!(error.sqlstate(), "40001");
    assert_eq!(txn.commit().unwrap_err().sqlstate(), "40001");
    assert_eq!(read(&gw, 10, b"a"), None);
}

#[test]
fn serializable_and_transactional_counters_are_unsupported_in_a_cluster() {
    let cluster = cluster();
    let gw = services(&cluster, ClusterConfig::default());
    let mut txn = begin(&gw, Isolation::Serializable);
    put_row(&mut txn, 10, b"a", b"x");
    assert_eq!(txn.commit().unwrap_err(), TxnError::Unsupported);
    let mut txn = begin(&gw, Isolation::ReadCommitted);
    let mut key = vec![2];
    key.extend_from_slice(&[0; 24]);
    txn.put_counter(key, vec![0; 25]).unwrap();
    assert_eq!(txn.commit().unwrap_err(), TxnError::Unsupported);
}

#[test]
fn repeatable_read_reports_a_write_conflict_from_the_grant() {
    let cluster = cluster();
    let gw = services(&cluster, ClusterConfig::default());
    let mut rr = begin(&gw, Isolation::RepeatableRead);
    assert_eq!(rr.get(Keyspace::Rows, &row(10, b"a")).unwrap(), None);
    let mut other = begin(&gw, Isolation::ReadCommitted);
    put_row(&mut other, 10, b"a", b"x");
    other.commit().unwrap();
    let error = rr
        .lock_for_write(Keyspace::Rows, &row(10, b"a"), None, &never)
        .unwrap_err();
    assert_eq!(error.sqlstate(), "40001");
}

#[test]
fn read_only_commit_uses_no_range_and_ids_carry_the_node() {
    let cluster = cluster();
    let gw = services(
        &cluster,
        ClusterConfig {
            node: 7,
            ..ClusterConfig::default()
        },
    );
    let id = gw.next_txn_id();
    assert_eq!(ClusterTxnId(id.0).node(), 7);
    let mut txn = begin(&gw, Isolation::ReadCommitted);
    let snapshot = txn.snapshot_ts();
    assert_eq!(txn.commit().unwrap(), snapshot);
}

#[test]
fn an_oracle_failure_fails_reads_instead_of_reading_old_data() {
    let cluster = cluster();
    let gw = services(
        &cluster,
        ClusterConfig {
            retries: 2,
            ..ClusterConfig::default()
        },
    );
    let mut txn = begin(&gw, Isolation::ReadCommitted);
    put_row(&mut txn, 10, b"a", b"x");
    txn.commit().unwrap();
    let mut reader = begin(&gw, Isolation::ReadCommitted);
    assert!(
        reader
            .get(Keyspace::Rows, &row(10, b"a"))
            .unwrap()
            .is_some()
    );
    cluster.stop(SYSTEM_RANGE);
    reader.begin_statement();
    assert!(reader.snapshot_error().is_some());
    assert!(reader.get(Keyspace::Rows, &row(10, b"a")).is_err());
}

/// The cluster as a coordinator that dies after `budget` calls sees it: later calls fail
/// without reaching the cluster (GD-0075 R5-52/R5-53 against real engines).
struct Dying {
    cluster: Arc<LocalCluster>,
    budget: std::sync::atomic::AtomicUsize,
}
impl Dying {
    fn call(&self) -> Result<(), RangeError> {
        self.budget
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |b| b.checked_sub(1))
            .map(|_| ())
            .map_err(|_| RangeError::Unavailable("coordinator died".into()))
    }
}
impl RangeClient for Dying {
    fn descriptor_for(&self, key: &[u8]) -> Result<RangeDescriptor, RangeError> {
        self.cluster.descriptor_for(key)
    }
    fn invalidate(&self, stale: &RangeDescriptor) {
        self.cluster.invalidate(stale);
    }
    fn read(&self, req: ReadRequest) -> Result<ReadPage, RangeError> {
        self.call()?;
        self.cluster.read(req)
    }
    fn lock(&self, req: LockRequest) -> Result<LockGrant, RangeError> {
        self.call()?;
        self.cluster.lock(req)
    }
    fn blockers(&self, range: RangeId, txn: ClusterTxnId) -> Result<Vec<ClusterTxnId>, RangeError> {
        RangeClient::blockers(self.cluster.as_ref(), range, txn)
    }
    fn cancel_wait(&self, range: RangeId, txn: ClusterTxnId) -> Result<(), RangeError> {
        RangeClient::cancel_wait(self.cluster.as_ref(), range, txn)
    }
    fn release(&self, range: RangeId, txn: ClusterTxnId) -> Result<(), RangeError> {
        self.call()?;
        self.cluster.release(range, txn)
    }
    fn heartbeat(&self, range: RangeId, txn: ClusterTxnId) -> Result<(), RangeError> {
        self.call()?;
        self.cluster.heartbeat(range, txn)
    }
    fn commit(
        &self,
        range: RangeId,
        epoch: Epoch,
        txn: ClusterTxnId,
        writes: Vec<RangeWrite>,
        marker_xid: Option<u64>,
        lock_generation: Option<u64>,
    ) -> Result<Timestamp, RangeError> {
        self.call()?;
        self.cluster
            .commit(range, epoch, txn, writes, marker_xid, lock_generation)
    }
    fn prepare(
        &self,
        range: RangeId,
        epoch: Epoch,
        txn: ClusterTxnId,
        home: RangeId,
        writes: Vec<RangeWrite>,
        lock_generation: Option<u64>,
    ) -> Result<Timestamp, RangeError> {
        self.call()?;
        self.cluster
            .prepare(range, epoch, txn, home, writes, lock_generation)
    }
    fn finalize(
        &self,
        range: RangeId,
        txn: ClusterTxnId,
        outcome: Outcome,
    ) -> Result<(), RangeError> {
        self.call()?;
        self.cluster.finalize(range, txn, outcome)
    }
    fn home(&self, range: RangeId, epoch: Epoch, op: HomeOp) -> Result<HomeReply, RangeError> {
        self.call()?;
        self.cluster.home(range, epoch, op)
    }
    fn oracle(&self) -> &dyn TsOracle {
        self
    }
    fn lease(
        &self,
        kind: LeaseKind,
        id: u64,
        count: u64,
    ) -> Result<std::ops::Range<u64>, RangeError> {
        self.cluster.lease(kind, id, count)
    }
    fn sequence(
        &self,
        id: u64,
        incarnation: u128,
        op: SequenceOp,
    ) -> Result<SequenceReply, RangeError> {
        self.cluster.sequence(id, incarnation, op)
    }
}
impl TsOracle for Dying {
    fn next(&self, n: u32) -> Result<Timestamp, RangeError> {
        self.call()?;
        self.cluster.oracle().next(n)
    }
}

/// A coordinator killed at every step of a two-range commit leaves one outcome, which readers
/// on another gateway see without waiting for anyone: prepared records are resolved through the
/// home record (R5-53) and idle home records are recovered (R5-52) by the range engines.
#[test]
fn a_coordinator_killed_at_every_step_leaves_one_outcome_readers_resolve() {
    let cluster = LocalCluster::with_config(
        &splits(),
        crate::storage::range::engine::EngineConfig {
            lease: Duration::from_millis(500),
            sweep: Duration::from_millis(20),
            resolve_after: Duration::from_millis(50),
            read_wait: Duration::from_secs(10),
            recovery: crate::storage::range::home::RecoveryConfig {
                recover_after: Duration::from_millis(300),
                finish_after: Duration::from_millis(100),
            },
            recovery_tick: Duration::from_millis(50),
        },
    );
    let reader = services(&cluster, ClusterConfig::default());
    let mut outcomes = BTreeSet::new();
    for budget in 0..40 {
        let dying = Arc::new(Dying {
            cluster: cluster.clone(),
            budget: std::sync::atomic::AtomicUsize::new(usize::MAX),
        });
        let gw = ClusterTxnServices::new(
            dying.clone(),
            ClusterConfig {
                node: 2,
                retries: 1,
                heartbeat: Duration::from_secs(60),
                ..ClusterConfig::default()
            },
        );
        let (a, z) = (format!("a{budget}"), format!("z{budget}"));
        let mut txn = begin(&gw, Isolation::ReadCommitted);
        let id = ClusterTxnId(txn.id().0);
        put_row(&mut txn, 10, a.as_bytes(), b"x");
        put_row(&mut txn, 10, z.as_bytes(), b"y");
        // From here the coordinator survives `budget` more calls.
        dying.budget.store(budget, Ordering::SeqCst);
        let result = txn.commit();
        let started = Instant::now();
        let try_read = |pk: &[u8]| {
            let mut txn = begin(&reader, Isolation::ReadCommitted);
            txn.get(Keyspace::Rows, &row(10, pk))
        };
        let (left, right) = match (try_read(a.as_bytes()), try_read(z.as_bytes())) {
            (Ok(left), Ok(right)) => (left, right),
            other => {
                let home = cluster.range_of(RangeSpace::Rows, &row(10, a.as_bytes()));
                let status = cluster.home(home, Epoch(1), HomeOp::Status { txn: id });
                let right = cluster.range_of(RangeSpace::Rows, &row(10, z.as_bytes()));
                let finals: Vec<_> = match &status {
                    Ok(HomeReply::Status(crate::range_api::HomeStatus::Decided(o))) => {
                        [home, right]
                            .iter()
                            .map(|r| cluster.finalize(*r, id, o.clone()))
                            .collect()
                    }
                    _ => Vec::new(),
                };
                panic!(
                    "budget {budget}: {other:?}; commit {result:?}; home {status:?}; finalize {finals:?}"
                );
            }
        };
        assert!(
            started.elapsed() < Duration::from_secs(8),
            "readers waited {:?}",
            started.elapsed()
        );
        assert_eq!(
            left.is_some(),
            right.is_some(),
            "budget {budget}: torn commit"
        );
        if result.is_ok() {
            assert!(
                left.is_some(),
                "budget {budget}: acknowledged but not visible"
            );
        }
        outcomes.insert(left.is_some());
        drop(gw);
    }
    assert_eq!(outcomes.len(), 2, "both outcomes were exercised");
}
