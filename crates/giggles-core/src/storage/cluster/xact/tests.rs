//! R5-55: xid records and `pg_xact_status` across gateways of one in-process cluster.
use super::*;
use crate::range_api::{
    Epoch, GatewayPeer, HomeOp, HomeReply, LeaseKind, LockGrant, LockRequest, Outcome, RangeDescriptor,
    RangeId, ReadPage, SequenceOp, SequenceReply, Timestamp, TsOracle, cluster_record_key,
};
use crate::relational::{StatementClock, Value};
use crate::storage::cluster::coordinator::deadlock::DeadlockConfig;
use crate::storage::cluster::db::{Gateway, attach, local::LocalCluster};
use crate::storage::cluster::txn::ClusterConfig;
use crate::storage::{Limits, SqlSession};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

#[test]
fn records_have_a_fixed_layout_and_decode_strictly() {
    assert_eq!(xact_key(0x0102), [0xF3, 0, 0, 0, 0, 0, 0, 0x01, 0x02]);
    let owner = XactRecord::Owner {
        node: 2,
        txn: ClusterTxnId::new(2, 5),
    };
    assert_eq!(
        owner.encode(),
        [1, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 2, 0, 0, 0, 0, 0, 5]
    );
    assert_eq!(XactRecord::Committed.encode(), [1, 1]);
    for record in [owner, XactRecord::Committed] {
        assert_eq!(XactRecord::decode(&record.encode()).unwrap(), record);
    }
    let mut long = owner.encode();
    long.push(0);
    for bad in [
        &[][..],
        &[1],
        &[2, 1],
        &[1, 2],
        &[1, 1, 0],
        &owner.encode()[..17],
        &long,
    ] {
        assert!(
            matches!(XactRecord::decode(bad), Err(RangeError::Corrupt(_))),
            "{bad:?}"
        );
    }
    let write = XactRecord::Committed.write(7);
    assert_eq!(write.space, RangeSpace::Catalog);
    assert!(cluster_record_key(&write.key));
}

fn clock() -> StatementClock {
    StatementClock {
        unavailable: None,
        transaction_us: 100,
        statement_us: 100,
        zone: Some(giggles_pg_types::temporal::SessionZone::Fixed(0)),
        wall: None,
    }
}

/// Table 1's rows in their own range, apart from the system range.
fn cluster() -> Arc<LocalCluster> {
    let mut splits = Vec::new();
    for space in [RangeSpace::Rows, RangeSpace::Index] {
        for id in 1..=4_u64 {
            splits.push(range_key(space, &id.to_be_bytes()));
        }
    }
    splits.sort();
    LocalCluster::new(&splits)
}

fn gateway(cluster: &Arc<LocalCluster>, client: Arc<dyn RangeClient>, node: u16) -> Gateway {
    let config = ClusterConfig {
        node,
        heartbeat: Duration::from_millis(200),
        ..ClusterConfig::default()
    };
    let deadlock = DeadlockConfig {
        deadlock_timeout: Duration::from_millis(200),
        lock_wait_backstop: Duration::from_secs(20),
        tick: Duration::from_millis(50),
    };
    let peer: Arc<dyn GatewayPeer> = cluster.clone();
    let gateway = attach(
        client,
        config,
        Some((peer, deadlock)),
        [9; 16],
        Limits::default(),
    )
    .unwrap();
    cluster.register_gateway(u64::from(node), gateway.services.gateway_service());
    gateway
}

fn run(s: &mut SqlSession, sql: &str) -> crate::relational::SqlResult {
    s.execute_at(sql, &[], None, &clock())
        .unwrap_or_else(|e| panic!("{sql}: {e:?}"))
}

fn scalar(s: &mut SqlSession, sql: &str) -> Value {
    run(s, sql).rows[0].values[0].clone()
}

fn xid(s: &mut SqlSession) -> i64 {
    match scalar(s, "SELECT pg_current_xact_id()") {
        Value::Int(id) => id,
        other => panic!("expected an xid, got {other:?}"),
    }
}

fn status(s: &mut SqlSession, xid: i64) -> Value {
    scalar(s, &format!("SELECT pg_xact_status({xid})"))
}

fn text(s: &str) -> Value {
    Value::Text(s.into())
}

#[test]
fn another_gateway_reports_committed_aborted_in_progress_and_unknown() {
    let cluster = cluster();
    let a = gateway(&cluster, cluster.clone(), 1);
    let mut sa = a.admin.sql_session();
    run(&mut sa, "CREATE TABLE t(id INT PRIMARY KEY)");
    let b = gateway(&cluster, cluster.clone(), 2);
    let mut sb = b.admin.sql_session();

    run(&mut sa, "BEGIN");
    let x = xid(&mut sa);
    run(&mut sa, "INSERT INTO t VALUES (1)");
    assert_eq!(status(&mut sb, x), text("in progress"));
    run(&mut sa, "COMMIT");
    assert_eq!(status(&mut sb, x), text("committed"));
    // The owner itself, once the xid is no longer live there, reads the record too.
    assert_eq!(status(&mut sa, x), text("committed"));

    run(&mut sa, "BEGIN");
    let y = xid(&mut sa);
    run(&mut sa, "INSERT INTO t VALUES (2)");
    assert_eq!(status(&mut sb, y), text("in progress"));
    run(&mut sa, "ROLLBACK");
    assert_eq!(status(&mut sb, y), text("aborted"));

    // An xid in an autocommit statement with no other write commits on the system range.
    let z = xid(&mut sa);
    assert_eq!(status(&mut sb, z), text("committed"));

    // Never assigned anywhere.
    assert_eq!(status(&mut sb, x.max(y).max(z) + 1_000_000), Value::Null);
    assert_eq!(
        scalar(&mut sb, "SELECT count(*) FROM t"),
        Value::Int(1),
        "only the committed insert is visible"
    );
}

#[test]
fn an_ended_or_unreachable_owner_gives_aborted_or_in_progress() {
    let cluster = cluster();
    let a = gateway(&cluster, cluster.clone(), 1);
    let mut sa = a.admin.sql_session();
    run(&mut sa, "CREATE TABLE t(id INT PRIMARY KEY)");
    let b = gateway(&cluster, cluster.clone(), 2);
    let mut sb = b.admin.sql_session();
    run(&mut sa, "BEGIN");
    let x = xid(&mut sa);
    run(&mut sa, "INSERT INTO t VALUES (1)");
    // A's gateway goes away without committing; nobody answers for node 1.
    drop(sa);
    drop(a);
    assert_eq!(status(&mut sb, x), text("in progress"));
    // Node 1 restarts: its new gateway never ran the owner, and attaches although XACT
    // records exist in the system range (catalog loads skip them).
    let a2 = gateway(&cluster, cluster.clone(), 1);
    assert_eq!(status(&mut sb, x), text("aborted"));
    let mut sa2 = a2.admin.sql_session();
    assert_eq!(scalar(&mut sa2, "SELECT count(*) FROM t"), Value::Int(0));
    assert_eq!(status(&mut sa2, x), text("aborted"));
}

/// A's client: every call fails once a Commit decision has been accepted at a home, as if A's
/// gateway died right after Decide(Commit) (nothing is finalized or forgotten). With
/// `lose_reply`, the Decide's own reply is lost too; with `before_decide`, the gateway dies
/// instead just before the Decide(Commit) reaches the home (the transaction stays prepared and
/// undecided until recovery).
struct DiesAfterDecide {
    inner: Arc<LocalCluster>,
    dead: AtomicBool,
    lose_reply: bool,
    before_decide: bool,
}
impl DiesAfterDecide {
    fn alive(&self) -> Result<(), RangeError> {
        if self.dead.load(Ordering::SeqCst) {
            return Err(RangeError::Unavailable("gateway dead".into()));
        }
        Ok(())
    }
}
impl RangeClient for DiesAfterDecide {
    fn descriptor_for(&self, range_key: &[u8]) -> Result<RangeDescriptor, RangeError> {
        self.inner.descriptor_for(range_key)
    }
    fn invalidate(&self, stale: &RangeDescriptor) {
        self.inner.invalidate(stale);
    }
    fn read(&self, req: ReadRequest) -> Result<ReadPage, RangeError> {
        self.alive()?;
        self.inner.read(req)
    }
    fn lock(&self, req: LockRequest) -> Result<LockGrant, RangeError> {
        self.alive()?;
        self.inner.lock(req)
    }
    fn blockers(&self, range: RangeId, txn: ClusterTxnId) -> Result<Vec<ClusterTxnId>, RangeError> {
        self.alive()?;
        RangeClient::blockers(self.inner.as_ref(), range, txn)
    }
    fn cancel_wait(&self, range: RangeId, txn: ClusterTxnId) -> Result<(), RangeError> {
        self.alive()?;
        RangeClient::cancel_wait(self.inner.as_ref(), range, txn)
    }
    fn release(&self, range: RangeId, txn: ClusterTxnId) -> Result<(), RangeError> {
        self.alive()?;
        self.inner.release(range, txn)
    }
    fn heartbeat(&self, range: RangeId, txn: ClusterTxnId) -> Result<(), RangeError> {
        self.alive()?;
        self.inner.heartbeat(range, txn)
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
        self.alive()?;
        self.inner
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
        self.alive()?;
        self.inner
            .prepare(range, epoch, txn, home, writes, lock_generation)
    }
    fn finalize(&self, range: RangeId, txn: ClusterTxnId, outcome: Outcome) -> Result<(), RangeError> {
        self.alive()?;
        self.inner.finalize(range, txn, outcome)
    }
    fn home(&self, range: RangeId, epoch: Epoch, op: HomeOp) -> Result<HomeReply, RangeError> {
        self.alive()?;
        let commit = matches!(
            op,
            HomeOp::Decide {
                outcome: Outcome::Commit(_),
                ..
            }
        );
        if commit && self.before_decide {
            self.dead.store(true, Ordering::SeqCst);
            return Err(RangeError::Unavailable("gateway dead".into()));
        }
        let reply = self.inner.home(range, epoch, op)?;
        if commit {
            self.dead.store(true, Ordering::SeqCst);
            if self.lose_reply {
                return Err(RangeError::Unavailable("reply lost".into()));
            }
        }
        Ok(reply)
    }
    fn oracle(&self) -> &dyn TsOracle {
        self.inner.oracle()
    }
    fn lease(&self, kind: LeaseKind, id: u64, count: u64) -> Result<std::ops::Range<u64>, RangeError> {
        self.alive()?;
        self.inner.lease(kind, id, count)
    }
    fn sequence(&self, id: u64, incarnation: u128, op: SequenceOp) -> Result<SequenceReply, RangeError> {
        self.alive()?;
        self.inner.sequence(id, incarnation, op)
    }
}

#[test]
fn a_commit_decided_by_a_coordinator_that_died_reports_committed() {
    let cluster = cluster();
    let client = Arc::new(DiesAfterDecide {
        inner: cluster.clone(),
        dead: AtomicBool::new(false),
        lose_reply: false,
        before_decide: false,
    });
    let a = gateway(&cluster, client.clone(), 1);
    let mut sa = a.admin.sql_session();
    run(&mut sa, "CREATE TABLE t(id INT PRIMARY KEY)");
    let b = gateway(&cluster, cluster.clone(), 2);
    let mut sb = b.admin.sql_session();
    run(&mut sa, "BEGIN");
    let x = xid(&mut sa);
    run(&mut sa, "INSERT INTO t VALUES (1)");
    // Two ranges written (the system range for XACT/x, home; the row's range): Track D's
    // protocol. The decision is durable, so COMMIT succeeds; then the gateway is gone.
    run(&mut sa, "COMMIT");
    assert!(client.dead.load(Ordering::SeqCst), "a cross-range commit");
    drop(sa);
    drop(a);
    // B's read of XACT/x waits on the prepared write and resolves it through its home.
    let started = Instant::now();
    assert_eq!(status(&mut sb, x), text("committed"));
    assert!(started.elapsed() < Duration::from_secs(30));
    assert_eq!(scalar(&mut sb, "SELECT count(*) FROM t"), Value::Int(1));
}

/// Review S1: an xid transaction that writes only the system range commits through prepare
/// and decide as well. Its Decide(Commit) is applied, but the reply is lost and A's gateway is
/// cut off from then on: COMMIT reports an unknown outcome, the owner has ended, and another
/// gateway reports the true outcome, committed (the prepared Committed record resolves through
/// its home), never "aborted" first.
#[test]
fn a_lost_commit_reply_of_a_system_range_only_xid_is_ambiguous_and_reads_committed() {
    let cluster = cluster();
    let client = Arc::new(DiesAfterDecide {
        inner: cluster.clone(),
        dead: AtomicBool::new(false),
        lose_reply: true,
        before_decide: false,
    });
    let a = gateway(&cluster, client.clone(), 1);
    let mut sa = a.admin.sql_session();
    let b = gateway(&cluster, cluster.clone(), 2);
    let mut sb = b.admin.sql_session();
    run(&mut sa, "BEGIN");
    let x = xid(&mut sa);
    let error = sa.execute_at("COMMIT", &[], None, &clock()).unwrap_err();
    assert!(
        client.dead.load(Ordering::SeqCst),
        "the commit went through a Decide"
    );
    assert!(format!("{error:?}").contains("outcome unknown"), "{error:?}");
    assert_eq!(status(&mut sb, x), text("committed"));
    // Node 1 restarts; the answer stays.
    drop(sa);
    drop(a);
    let _a2 = gateway(&cluster, cluster.clone(), 1);
    assert_eq!(status(&mut sb, x), text("committed"));
}

/// Review S4: a gateway's catalog load stops below the cluster records, so an `XACT/` record
/// left prepared by a coordinator that died before deciding (undecided until recovery) does not
/// make another gateway's attach wait on it and fail. (Catalog point reads still scan from the
/// key to the end of the system range, so DDL in that window still waits: see the P7-3 report.)
#[test]
fn a_prepared_xact_record_does_not_block_a_catalog_load() {
    let cluster = cluster();
    let b = gateway(&cluster, cluster.clone(), 2);
    let mut sb = b.admin.sql_session();
    run(&mut sb, "CREATE TABLE u(id INT PRIMARY KEY)");
    run(&mut sb, "INSERT INTO u VALUES (1)");
    let client = Arc::new(DiesAfterDecide {
        inner: cluster.clone(),
        dead: AtomicBool::new(false),
        lose_reply: false,
        before_decide: true,
    });
    let a = gateway(&cluster, client.clone(), 1);
    let mut sa = a.admin.sql_session();
    run(&mut sa, "BEGIN");
    let x = xid(&mut sa);
    assert!(sa.execute_at("COMMIT", &[], None, &clock()).is_err());
    assert!(client.dead.load(Ordering::SeqCst), "prepared, never decided");
    // Attach loads the whole catalog at a timestamp above the prepared record.
    let c = gateway(&cluster, cluster.clone(), 3);
    let mut sc = c.admin.sql_session();
    assert_eq!(scalar(&mut sc, "SELECT count(*) FROM u"), Value::Int(1));
    // The record itself is still undecided (rule 4).
    assert_eq!(status(&mut sc, x), text("in progress"));
}
