//! Blocking client seam over a single Raft driver thread.
use crate::{
    barrier::ReadBarrier,
    error::Error,
    membership::{AdminCommand, Membership},
    node::{Replica, RoleInfo},
};
use raft::{
    StateRole, Storage,
    eraftpb::{ConfChangeV2, ConfState, Message},
};
use std::{
    collections::{BTreeSet, HashMap},
    fmt,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicateError {
    NotLeader,
    Refused(String),
    Unknown(String),
    Stopped,
}
impl fmt::Display for ReplicateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Refused(s) => write!(f, "refused: {s}"),
            Self::NotLeader => f.write_str("not leader"),
            Self::Unknown(s) => write!(f, "outcome unknown: {s}"),
            Self::Stopped => f.write_str("replica stopped"),
        }
    }
}
impl std::error::Error for ReplicateError {}
pub trait Outbox: Send {
    fn send(&mut self, m: Message);
    fn apply_membership(&mut self, _m: &Membership) {}
    fn set_peers(
        &mut self,
        _peers: &std::collections::BTreeMap<u64, crate::transport::PeerConfig>,
    ) {
    }
}
type Reply<T> = mpsc::Sender<Result<T, ReplicateError>>;
pub(crate) type Receiver<T> = mpsc::Receiver<Result<T, ReplicateError>>;
#[derive(Clone, Debug)]
pub struct MembershipStatus {
    pub membership: Membership,
    pub conf: ConfState,
    pub role: RoleInfo,
    pub reachable: BTreeSet<u64>,
    pub learner_matched: Option<u64>,
    pub last_index: u64,
    pub applied: u64,
    pub snapshot_index: u64,
}
fn snapshot_index(replica: &Replica) -> u64 {
    replica
        .log()
        .snapshots()
        .and_then(|s| s.latest())
        .map_or(0, |s| s.index)
}
type SnapshotBuild = thread::JoinHandle<crate::error::Result<crate::snapshot::Manifest>>;

fn start_snapshot(replica: &mut Replica) -> crate::error::Result<Option<SnapshotBuild>> {
    let Some(snapshots) = replica.log().snapshots().cloned() else {
        return Ok(None);
    };
    let index = replica.applied_index();
    let term = replica.log().term(index).map_err(Error::Raft)?;
    let stage = snapshots.staging_dir(index, term);
    match replica.prepare_snapshot(&stage) {
        Ok(Some((index, term, conf, membership))) => Ok(Some(thread::spawn(move || {
            let result = snapshots.build(index, term, &conf, &membership, |dir| {
                std::fs::rename(&stage, dir)?;
                Ok(())
            });
            if stage.exists() {
                let _ = std::fs::remove_dir_all(stage);
            }
            result
        }))),
        other => {
            if stage.exists() {
                let _ = std::fs::remove_dir_all(stage);
            }
            other.map(|_| None)
        }
    }
}

enum Command {
    Admin(AdminCommand, Reply<()>),
    Conf(ConfChangeV2, Reply<()>),
    Status(Reply<MembershipStatus>),
    BuildSnapshot(Reply<u64>),
    Propose(Vec<u8>, Reply<()>),
    Read(Reply<u64>),
    LastIndex(Reply<u64>),
    Step(u64, Message),
    ReportSnapshot(u64, bool),
    Liveness(u64, bool),
    Shutdown,
}
#[derive(Clone)]
pub struct ReplicaHandle {
    tx: mpsc::Sender<Command>,
    role: Arc<Mutex<RoleInfo>>,
    rounds: Arc<AtomicU64>,
    stopped: Arc<AtomicBool>,
    quiesced: Arc<AtomicBool>,
}
impl ReplicaHandle {
    pub fn replicate(&self, encoded_batch: Vec<u8>) -> Result<(), ReplicateError> {
        self.submit(encoded_batch)?
            .recv()
            .unwrap_or(Err(ReplicateError::Stopped))
    }
    /// Queue a proposal on the driver thread and return its reply channel.
    ///
    /// Once this returns, any later [`ReplicaHandle::last_index`] covers the
    /// proposal: the command channel is FIFO.
    pub(crate) fn submit(&self, encoded_batch: Vec<u8>) -> Result<Receiver<()>, ReplicateError> {
        let (tx, rx) = mpsc::channel();
        self.tx
            .send(Command::Propose(encoded_batch, tx))
            .map_err(|_| ReplicateError::Stopped)?;
        Ok(rx)
    }
    /// The last index in this replica's Raft log, committed or not.
    ///
    /// The request is answered by the driver thread in command order, so the
    /// result includes every proposal submitted through this handle before the
    /// call (or that proposal was refused before reaching the log).
    pub fn last_index(&self) -> Result<u64, ReplicateError> {
        self.request(Command::LastIndex)
    }
    pub fn propose_admin(&self, cmd: AdminCommand) -> Result<(), ReplicateError> {
        self.request(|tx| Command::Admin(cmd, tx))
    }
    pub fn propose_conf_change(&self, cc: ConfChangeV2) -> Result<(), ReplicateError> {
        self.request(|tx| Command::Conf(cc, tx))
    }
    pub fn membership_status(&self) -> Result<MembershipStatus, ReplicateError> {
        self.request(Command::Status)
    }
    pub fn build_snapshot(&self) -> Result<u64, ReplicateError> {
        self.request(Command::BuildSnapshot)
    }
    fn request<T>(&self, command: impl FnOnce(Reply<T>) -> Command) -> Result<T, ReplicateError> {
        let (tx, rx) = mpsc::channel();
        self.tx
            .send(command(tx))
            .map_err(|_| ReplicateError::Stopped)?;
        rx.recv().unwrap_or(Err(ReplicateError::Stopped))
    }
    /// Confirm leadership through a ReadIndex round and wait until that read
    /// index is applied here; `Ok` carries the read index (so applied >= it).
    pub fn read_barrier(&self) -> Result<u64, ReplicateError> {
        let (tx, rx) = mpsc::channel();
        self.tx
            .send(Command::Read(tx))
            .map_err(|_| ReplicateError::Stopped)?;
        rx.recv().unwrap_or(Err(ReplicateError::Stopped))
    }
    pub fn step(&self, from: u64, m: Message) {
        let _ = self.tx.send(Command::Step(from, m));
    }
    pub fn report_snapshot(&self, to: u64, ok: bool) {
        let _ = self.tx.send(Command::ReportSnapshot(to, ok));
    }
    /// Reports a peer node's liveness change to a quiescing group (GD-0075 R5-17b).
    pub fn node_liveness(&self, node: u64, alive: bool) {
        let _ = self.tx.send(Command::Liveness(node, alive));
    }
    pub fn role(&self) -> RoleInfo {
        *self.role.lock().expect("role mutex poisoned")
    }
    pub fn read_rounds(&self) -> u64 {
        self.rounds.load(Ordering::Relaxed)
    }
    pub fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }
    /// Whether the replica was quiesced after its last driver round (R5-17b).
    pub fn is_quiesced(&self) -> bool {
        self.quiesced.load(Ordering::Acquire)
    }
    pub fn shutdown(&self) {
        let _ = self.tx.send(Command::Shutdown);
    }
}

/// Snapshot cadence and retained log entries for followers close behind.
#[derive(Clone, Copy, Debug)]
pub struct SnapshotPolicy {
    pub every_entries: u64,
    pub margin: u64,
}
impl Default for SnapshotPolicy {
    fn default() -> Self {
        Self {
            every_entries: 10_000,
            margin: 1_000,
        }
    }
}

pub fn spawn(
    replica: Replica,
    outbox: Box<dyn Outbox>,
    tick: Duration,
) -> (ReplicaHandle, thread::JoinHandle<()>) {
    spawn_with_policy(replica, outbox, tick, None)
}

pub fn spawn_with_policy(
    mut replica: Replica,
    mut outbox: Box<dyn Outbox>,
    tick: Duration,
    policy: Option<SnapshotPolicy>,
) -> (ReplicaHandle, thread::JoinHandle<()>) {
    assert!(!tick.is_zero(), "tick interval must be positive");
    let (tx, rx) = mpsc::channel();
    let handle = ReplicaHandle {
        tx,
        role: Arc::new(Mutex::new(replica.role())),
        rounds: Arc::new(AtomicU64::new(0)),
        stopped: Arc::new(AtomicBool::new(false)),
        quiesced: Arc::new(AtomicBool::new(false)),
    };
    let role = handle.role.clone();
    let rounds = handle.rounds.clone();
    let stopped = handle.stopped.clone();
    let quiesced_flag = handle.quiesced.clone();
    let join = thread::spawn(move || {
        let mut build: Option<thread::JoinHandle<crate::error::Result<crate::snapshot::Manifest>>> =
            None;
        let mut compacted_snapshot = 0;
        let mut barrier = ReadBarrier::new(replica.node_id(), 5);
        let mut pending: HashMap<u64, Reply<()>> = HashMap::new();
        let mut read_indexes = BTreeSet::new();
        let mut seq = 0u64;
        let mut published = replica.role();
        let mut next_tick = Instant::now() + tick;
        let mut was_quiesced = false;
        let issue = |replica: &mut Replica, ctx| {
            if replica.read_index(ctx).is_ok() {
                rounds.fetch_add(1, Ordering::Relaxed);
            }
        };
        outbox.apply_membership(replica.membership());
        loop {
            if build.as_ref().is_some_and(|job| job.is_finished()) {
                match build.take().unwrap().join() {
                    Ok(Ok(_)) => {}
                    Ok(Err(e)) => eprintln!("snapshot build failed: {e}"),
                    Err(_) => eprintln!("snapshot build thread panicked"),
                }
            }
            replica.hold_quiescence(!barrier.is_idle() || !pending.is_empty());
            // A quiesced group does not tick: block until a command arrives (R5-17b).
            let quiesced = replica.is_quiesced();
            if was_quiesced && !quiesced {
                next_tick = Instant::now() + tick;
            }
            was_quiesced = quiesced;
            // Checking the deadline before receiving prevents busy peer traffic from starving ticks.
            if !quiesced && Instant::now() >= next_tick {
                replica.tick();
                if let Some(ctx) = barrier.tick() {
                    issue(&mut replica, ctx);
                }
                next_tick = Instant::now() + tick;
            } else {
                let received = if quiesced {
                    rx.recv()
                        .map_err(|_| mpsc::RecvTimeoutError::Disconnected)
                } else {
                    rx.recv_timeout(next_tick.saturating_duration_since(Instant::now()))
                };
                match received {
                    Ok(Command::Shutdown) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
                    Err(mpsc::RecvTimeoutError::Timeout) => continue,
                    Ok(Command::Step(from, m)) => {
                        let _ = replica.step(from, m);
                    }
                    Ok(Command::ReportSnapshot(to, ok)) => replica.report_snapshot(to, ok),
                    Ok(Command::Liveness(node, alive)) => {
                        let _ = replica.node_liveness(node, alive);
                    }
                    Ok(Command::LastIndex(reply)) => {
                        let _ = reply.send(Ok(replica.last_index()));
                    }
                    Ok(Command::Status(reply)) => {
                        let membership = replica.membership().clone();
                        let learner_matched = membership
                            .active
                            .as_ref()
                            .and_then(|a| replica.peer_matched(a.new.id));
                        let _ = reply.send(Ok(MembershipStatus {
                            membership,
                            conf: replica.conf_state(),
                            role: replica.role(),
                            reachable: replica.reachable_voters(),
                            learner_matched,
                            last_index: replica.last_index(),
                            applied: replica.applied_index(),
                            snapshot_index: snapshot_index(&replica),
                        }));
                    }
                    Ok(Command::BuildSnapshot(reply)) => {
                        let current = snapshot_index(&replica);
                        let result = if current < replica.applied_index() && build.is_none() {
                            start_snapshot(&mut replica)
                                .map(|job| {
                                    build = job;
                                    current
                                })
                                .map_err(|e| ReplicateError::Unknown(e.to_string()))
                        } else {
                            Ok(current)
                        };
                        let _ = reply.send(result);
                    }
                    Ok(
                        command @ (Command::Propose(..) | Command::Admin(..) | Command::Conf(..)),
                    ) => {
                        seq = seq.checked_add(1).expect("proposal sequence exhausted");
                        let (result, reply) = match command {
                            Command::Propose(batch, reply) => (replica.propose(&batch, seq), reply),
                            Command::Admin(cmd, reply) => (replica.propose_admin(&cmd, seq), reply),
                            Command::Conf(cc, reply) => {
                                (replica.propose_conf_change(cc, seq), reply)
                            }
                            _ => unreachable!(),
                        };
                        match result {
                            Ok(()) => {
                                pending.insert(seq, reply);
                            }
                            Err(e) => {
                                let _ = reply.send(Err(match e {
                                    Error::NotLeader => ReplicateError::NotLeader,
                                    e => ReplicateError::Unknown(e.to_string()),
                                }));
                            }
                        }
                    }
                    Ok(Command::Read(reply)) => {
                        if replica.role().state != StateRole::Leader {
                            let _ = reply.send(Err(ReplicateError::NotLeader));
                        } else if let Some(ctx) = barrier.request(reply) {
                            issue(&mut replica, ctx);
                        }
                    }
                }
            }
            let out = match replica.drive() {
                Ok(out) => out,
                Err(e) => {
                    eprintln!("replica {} stopped: {e}", replica.node_id());
                    break;
                }
            };
            if out.membership_changed {
                outbox.apply_membership(replica.membership());
            }
            for (waiter, reason) in out.refused {
                if let Some(reply) = pending.remove(&waiter) {
                    let _ = reply.send(Err(ReplicateError::Refused(reason)));
                }
            }
            for m in out.messages {
                outbox.send(m);
            }
            for (waiter, _) in out.applied {
                if let Some(reply) = pending.remove(&waiter) {
                    let _ = reply.send(Ok(()));
                }
            }
            for waiter in out.failed {
                if let Some(reply) = pending.remove(&waiter) {
                    let _ = reply.send(Err(ReplicateError::Unknown(
                        "leadership or proposal lost".into(),
                    )));
                }
            }
            if out.role.state != StateRole::Leader || out.role.term != published.term {
                for reply in barrier.fail_all() {
                    let _ = reply.send(Err(ReplicateError::NotLeader));
                }
                read_indexes.clear();
            } else {
                for (ctx, index) in out.read_states {
                    if let Some(ctx) = barrier.on_read_state(&ctx, index) {
                        issue(&mut replica, ctx);
                    }
                    read_indexes.insert(index);
                }
                // Release in index order so each reply carries its confirmed read index.
                while let Some(index) = read_indexes.first().copied() {
                    if index > replica.applied_index() {
                        break;
                    }
                    read_indexes.remove(&index);
                    for reply in barrier.on_applied(index) {
                        let _ = reply.send(Ok(index));
                    }
                }
            }
            if let Some(policy) = policy
                && let Some(snapshots) = replica.log().snapshots().cloned()
            {
                let last = snapshots.latest().map_or(0, |m| m.index);
                if last > compacted_snapshot {
                    match replica.compact_log(policy.margin) {
                        Ok(_) => compacted_snapshot = last,
                        Err(e) => eprintln!("snapshot compaction failed: {e}"),
                    }
                }
                if build.is_none()
                    && replica.applied_index() != 0
                    && replica.applied_index().saturating_sub(last) >= policy.every_entries
                {
                    let result = start_snapshot(&mut replica);
                    match result {
                        Ok(job) => build = job,
                        Err(e) => eprintln!("snapshot preparation failed: {e}"),
                    }
                }
            }
            published = out.role;
            *role.lock().expect("role mutex poisoned") = published;
            quiesced_flag.store(replica.is_quiesced(), Ordering::Release);
        }
        *role.lock().expect("role mutex poisoned") = RoleInfo {
            state: StateRole::Follower,
            term: replica.role().term,
            leader: 0,
        };
        stopped.store(true, Ordering::Release);
        for (_, reply) in pending {
            let _ = reply.send(Err(ReplicateError::Stopped));
        }
        for reply in barrier.fail_all() {
            let _ = reply.send(Err(ReplicateError::Stopped));
        }
        if let Some(job) = build {
            match job.join() {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => eprintln!("snapshot build failed: {e}"),
                Err(_) => eprintln!("snapshot build thread panicked"),
            }
        }
        // Dropping rx also drops replies on commands still queued at shutdown.
    });
    (handle, join)
}

impl crate::transport::PeerSink for ReplicaHandle {
    fn admin(&self, _from: u64, req: crate::admin::AdminRequest) -> crate::admin::AdminResponse {
        crate::admin::serve(self, req)
    }

    fn deliver(&self, from: u64, message: Message) {
        self.step(from, message);
    }

    fn snapshot_status(&self, to: u64, ok: bool) {
        self.report_snapshot(to, ok);
    }
}
