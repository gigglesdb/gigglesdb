//! Synchronous Raft driver. A drive error is fatal; the caller must stop the replica.
use crate::{
    entry::{self, EntryKind, ProposalContext},
    error::{Error, Result},
    log_store::LogStore,
    snapshot::Manifest,
    state::{AppliedPosition, StateMachine},
};
use raft::{
    Config, RawNode, ReadOnlyOption, StateRole,
    eraftpb::{Entry, EntryType, Message, MessageType},
};
use std::{path::Path, sync::Arc};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RoleInfo {
    pub state: StateRole,
    pub term: u64,
    pub leader: u64,
}
pub struct Output {
    pub messages: Vec<Message>,
    pub applied: Vec<(u64, u64)>,
    pub failed: Vec<u64>,
    pub read_states: Vec<(Vec<u8>, u64)>,
    pub role: RoleInfo,
}
struct Pending {
    waiter: u64,
    index: u64,
    term: u64,
}
pub struct Replica {
    raw: RawNode<LogStore>,
    state: Arc<dyn StateMachine>,
    node_id: u64,
    cluster: [u8; 16],
    pending: Vec<Pending>,
}
impl Replica {
    pub fn new(log: LogStore, state: Arc<dyn StateMachine>, voters: &[u64]) -> Result<Self> {
        let (cluster, node_id) = log.identity();
        log.initialize_voters(voters)?;
        if let Some(p) = state.applied().map_err(|e| Error::Apply(e.to_string()))?
            && p.index > log.hard_state()?.commit
        {
            log.install_snapshot_meta(p.index, p.term, &log.conf_state()?)?;
        }
        let config = Config {
            id: node_id,
            election_tick: 10,
            heartbeat_tick: 2,
            check_quorum: true,
            pre_vote: true,
            read_only_option: ReadOnlyOption::Safe,
            max_size_per_msg: 1 << 20,
            max_inflight_msgs: 256,
            applied: state
                .applied()
                .map_err(|e| Error::Apply(e.to_string()))?
                .map_or(0, |p| p.index),
            ..Default::default()
        };
        let logger = slog::Logger::root(slog::Discard, slog::o!());
        let raw = RawNode::new(&config, log, &logger).map_err(Error::Raft)?;
        Ok(Self {
            raw,
            state,
            node_id,
            cluster,
            pending: Vec::new(),
        })
    }
    pub fn report_snapshot(&mut self, to: u64, ok: bool) {
        self.raw.report_snapshot(
            to,
            if ok {
                raft::SnapshotStatus::Finish
            } else {
                raft::SnapshotStatus::Failure
            },
        );
    }
    /// Capture a checkpoint on the applying thread; hashing and publication can run elsewhere.
    pub fn prepare_snapshot(&mut self, staging: &Path) -> Result<Option<(u64, u64, Vec<u64>)>> {
        if self.log().snapshots().is_none() {
            return Ok(None);
        }
        let Some(p) = self
            .state
            .applied()
            .map_err(|e| Error::Apply(e.to_string()))?
            .filter(|p| p.index != 0)
        else {
            return Ok(None);
        };
        let voters = self.log().conf_state()?.voters;
        let q = self
            .state
            .checkpoint(staging)
            .map_err(|e| Error::Apply(e.to_string()))?;
        if q != p {
            return Err(Error::Corrupt("checkpoint position changed".into()));
        }
        Ok(Some((p.index, p.term, voters)))
    }

    pub fn build_snapshot(&mut self) -> Result<Option<Manifest>> {
        let Some(snapshots) = self.log().snapshots().cloned() else {
            return Ok(None);
        };
        let Some(p) = self
            .state
            .applied()
            .map_err(|e| Error::Apply(e.to_string()))?
            .filter(|p| p.index != 0)
        else {
            return Ok(None);
        };
        let stage = snapshots.staging_dir(p.index, p.term);
        let result = (|| {
            let Some((index, term, voters)) = self.prepare_snapshot(&stage)? else {
                return Ok(None);
            };
            snapshots
                .build(index, term, &voters, |dir| {
                    std::fs::rename(&stage, dir)?;
                    Ok(())
                })
                .map(Some)
        })();
        if stage.exists() {
            let _ = std::fs::remove_dir_all(stage);
        }
        result
    }
    pub fn compact_log(&mut self, margin: u64) -> Result<Option<u64>> {
        self.state
            .flush()
            .map_err(|e| Error::Apply(e.to_string()))?;
        let durable = self
            .state
            .applied()
            .map_err(|e| Error::Apply(e.to_string()))?
            .map_or(0, |p| p.index);
        let Some(latest) = self.log().snapshots().and_then(|s| s.latest()) else {
            return Ok(None);
        };
        let to = latest.index.min(durable).saturating_sub(margin);
        if to > self.log().snapshot_meta()?.0 {
            self.log().compact(to)?;
            Ok(Some(to))
        } else {
            Ok(None)
        }
    }
    pub fn tick(&mut self) {
        self.raw.tick();
    }
    pub fn campaign(&mut self) -> Result<()> {
        self.raw.campaign().map_err(Error::Raft)
    }
    pub fn set_election_timeout(&mut self, ticks: usize) {
        self.raw.raft.set_randomized_election_timeout(ticks);
    }
    pub fn role(&self) -> RoleInfo {
        RoleInfo {
            state: self.raw.raft.state,
            term: self.raw.raft.term,
            leader: self.raw.raft.leader_id,
        }
    }
    pub fn applied_index(&self) -> u64 {
        self.raw.raft.raft_log.applied
    }
    pub fn node_id(&self) -> u64 {
        self.node_id
    }
    pub fn log(&self) -> &LogStore {
        self.raw.raft.raft_log.store()
    }
    pub fn step(&mut self, from: u64, mut m: Message) -> Result<()> {
        let cs = self.log().conf_state()?;
        if !cs.voters.contains(&from) && !cs.voters_outgoing.contains(&from)
            || m.from != from
            || m.to != self.node_id
            || raft::raw_node::is_local_msg(m.get_msg_type())
            || matches!(
                m.get_msg_type(),
                MessageType::MsgPropose | MessageType::MsgReadIndex
            )
            || m.entries
                .iter()
                .any(|e| e.get_entry_type() != EntryType::EntryNormal)
            || (m.get_msg_type() == MessageType::MsgSnapshot && !self.state.can_install())
            || (!m.get_snapshot().is_empty()
                && (m.get_msg_type() != MessageType::MsgSnapshot
                    || !self.log().snapshots().is_some_and(|s| {
                        Manifest::from_raft(self.cluster, m.get_snapshot()).is_ok_and(|manifest| {
                            s.take_incoming(manifest.index, manifest.term).is_some()
                        })
                    })))
        {
            return Err(Error::Corrupt("unbound or unsupported peer message".into()));
        }
        // A same-term rejection can reveal that an authenticated voter lost its
        // previously acknowledged log. Forget that obsolete match and probe the
        // reported tail, allowing the normal append/snapshot path to recover.
        // Old-term replies and replies superseded by a snapshot cannot rewind it.
        if m.get_msg_type() == MessageType::MsgAppendResponse
            && m.reject
            && m.term == self.raw.raft.term
            && self.raw.raft.state == StateRole::Leader
            && let Some(peer) = self.raw.raft.mut_prs().get_mut(from)
            && peer.state != raft::ProgressState::Snapshot
            && m.index <= peer.matched
            && m.reject_hint < peer.matched
        {
            // A rejection hint is a probe position, not proof of a match.
            peer.matched = 0;
            peer.become_probe();
            peer.next_idx = m.reject_hint + 1;
        }
        // The leader may still remember this peer's acknowledged position after
        // its disks were wiped. A heartbeat can arrive before snapshot recovery;
        // raft-rs otherwise panics when that remembered commit exceeds our log.
        // Lowering the commit hint never commits entries we do not possess.
        if m.get_msg_type() == MessageType::MsgHeartbeat {
            m.commit = m.commit.min(self.raw.raft.raft_log.last_index());
        }
        self.raw.step(m).map_err(Error::Raft)
    }
    pub fn propose(&mut self, payload: &[u8], waiter: u64) -> Result<()> {
        if self.role().state != StateRole::Leader {
            return Err(Error::NotLeader);
        }
        let data = entry::encode(self.cluster, EntryKind::Batch, payload)
            .map_err(|e| Error::Corrupt(e.to_string()))?;
        let context = ProposalContext {
            proposer: self.node_id,
            seq: waiter,
        }
        .encode();
        self.raw.propose(context, data).map_err(Error::Raft)?;
        self.pending.push(Pending {
            waiter,
            index: self.raw.raft.raft_log.last_index(),
            term: self.raw.raft.term,
        });
        Ok(())
    }
    pub fn read_index(&mut self, ctx: Vec<u8>) -> Result<()> {
        if self.role().state != StateRole::Leader {
            return Err(Error::NotLeader);
        }
        self.raw.read_index(ctx);
        Ok(())
    }
    fn apply(&mut self, entries: &[Entry], out: &mut Output) -> Result<()> {
        for e in entries {
            let at = AppliedPosition {
                index: e.index,
                term: e.term,
            };
            let result = if e.get_entry_type() != EntryType::EntryNormal || e.data.is_empty() {
                self.state.apply_noop(at)
            } else {
                let (kind, payload) = entry::decode(self.cluster, &e.data)
                    .map_err(|e| Error::Corrupt(e.to_string()))?;
                match kind {
                    EntryKind::Batch => self.state.apply(payload, at),
                    EntryKind::Admin => self.state.apply_noop(at),
                }
            };
            result.map_err(|e| Error::Apply(e.to_string()))?;
            let context = ProposalContext::decode(&e.context);
            self.pending.retain(|p| {
                if p.index != e.index {
                    return true;
                }
                if context
                    == Some(ProposalContext {
                        proposer: self.node_id,
                        seq: p.waiter,
                    })
                {
                    out.applied.push((p.waiter, e.index));
                } else {
                    out.failed.push(p.waiter);
                }
                false
            });
        }
        Ok(())
    }
    fn fail_deposed(&mut self, out: &mut Output) {
        let role = self.role();
        self.pending.retain(|p| {
            if role.state == StateRole::Leader && p.term == role.term {
                return true;
            }
            out.failed.push(p.waiter);
            false
        });
    }
    pub fn drive(&mut self) -> Result<Output> {
        let mut out = Output {
            messages: Vec::new(),
            applied: Vec::new(),
            failed: Vec::new(),
            read_states: Vec::new(),
            role: self.role(),
        };
        while self.raw.has_ready() {
            let mut ready = self.raw.ready();
            out.messages.extend(ready.take_messages());
            if !ready.snapshot().is_empty() {
                let snap = ready.snapshot();
                let manifest = Manifest::from_raft(self.cluster, snap)?;
                let snapshots = self
                    .log()
                    .snapshots()
                    .cloned()
                    .ok_or_else(|| Error::Corrupt("snapshot store not attached".into()))?;
                let dir = snapshots
                    .take_incoming(manifest.index, manifest.term)
                    .ok_or_else(|| Error::Corrupt("snapshot data missing".into()))?;
                let pos = self
                    .state
                    .install(&dir)
                    .map_err(|e| Error::Apply(e.to_string()))?;
                if pos
                    != (AppliedPosition {
                        index: manifest.index,
                        term: manifest.term,
                    })
                {
                    return Err(Error::Corrupt("installed snapshot position differs".into()));
                }
                self.log().install_snapshot_meta(
                    pos.index,
                    pos.term,
                    snap.get_metadata().get_conf_state(),
                )?;
                snapshots.discard_incoming_through(pos.index);
            }
            self.log().persist(ready.entries(), ready.hs())?;
            out.messages.extend(ready.take_persisted_messages());
            self.apply(ready.committed_entries(), &mut out)?;
            out.read_states.extend(
                ready
                    .take_read_states()
                    .into_iter()
                    .map(|r| (r.request_ctx, r.index)),
            );
            let mut light = self.raw.advance(ready);
            if let Some(commit) = light.commit_index() {
                let mut hs = self.log().hard_state()?;
                hs.commit = commit;
                self.log().persist(&[], Some(&hs))?;
            }
            out.messages.extend(light.take_messages());
            self.apply(light.committed_entries(), &mut out)?;
            self.raw.advance_apply();
            self.fail_deposed(&mut out);
        }
        self.fail_deposed(&mut out);
        out.role = self.role();
        Ok(out)
    }
}

#[cfg(test)]
mod wiped_peer_tests {
    use super::*;
    use crate::state::MemStateMachine;
    use raft::eraftpb::HardState;
    use std::{
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
    };

    struct Case {
        replica: Option<Replica>,
        dir: PathBuf,
    }
    impl Case {
        fn new() -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let dir = std::env::temp_dir().join(format!(
                "wiped-peer-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            let log = LogStore::open(&dir, [74; 16], 1).unwrap();
            log.initialize_voters(&[1, 2, 3]).unwrap();
            let entries: Vec<_> = (1..=65)
                .map(|index| Entry {
                    index,
                    term: 1,
                    ..Default::default()
                })
                .collect();
            log.persist(
                &entries,
                Some(&HardState {
                    term: 1,
                    commit: 65,
                    ..Default::default()
                }),
            )
            .unwrap();
            let mut replica =
                Replica::new(log, Arc::new(MemStateMachine::new()), &[1, 2, 3]).unwrap();
            replica.raw.raft.become_candidate();
            replica.raw.raft.become_leader();
            let peer = replica.raw.raft.mut_prs().get_mut(2).unwrap();
            peer.matched = 65;
            peer.become_replicate();
            Self {
                replica: Some(replica),
                dir,
            }
        }
        fn reject(&mut self, old_term: bool, reject_hint: u64) {
            let replica = self.replica.as_mut().unwrap();
            replica
                .step(
                    2,
                    Message {
                        from: 2,
                        to: 1,
                        term: replica.role().term - u64::from(old_term),
                        msg_type: MessageType::MsgAppendResponse,
                        reject: true,
                        index: 65,
                        reject_hint,
                        ..Default::default()
                    },
                )
                .unwrap();
        }
    }
    impl Drop for Case {
        fn drop(&mut self) {
            drop(self.replica.take());
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    #[test]
    fn lost_acknowledged_log_restarts_peer_probe() {
        let mut case = Case::new();
        case.reject(false, 0);
        let peer = case
            .replica
            .as_ref()
            .unwrap()
            .raw
            .raft
            .prs()
            .get(2)
            .unwrap();
        assert_eq!(peer.matched, 0, "the acknowledged log was lost");
        assert_eq!(peer.next_idx, 1, "probe from the surviving prefix");
    }

    #[test]
    fn old_term_rejection_does_not_rewind_peer() {
        let mut case = Case::new();
        case.reject(true, 0);
        let peer = case
            .replica
            .as_ref()
            .unwrap()
            .raw
            .raft
            .prs()
            .get(2)
            .unwrap();
        assert_eq!(peer.matched, 65);
    }
    #[test]
    fn pending_snapshot_survives_superseded_append_rejection() {
        let mut case = Case::new();
        case.replica
            .as_mut()
            .unwrap()
            .raw
            .raft
            .mut_prs()
            .get_mut(2)
            .unwrap()
            .become_snapshot(66);
        case.reject(false, 0);
        let peer = case
            .replica
            .as_ref()
            .unwrap()
            .raw
            .raft
            .prs()
            .get(2)
            .unwrap();
        assert_eq!(peer.pending_snapshot, 66);
        assert_eq!(peer.matched, 65);
    }
    #[test]
    fn rejection_hint_is_probed_before_being_counted_as_a_match() {
        let mut case = Case::new();
        case.reject(false, 50);
        let peer = case
            .replica
            .as_ref()
            .unwrap()
            .raw
            .raft
            .prs()
            .get(2)
            .unwrap();
        assert_eq!(peer.matched, 0);
        assert_eq!(peer.next_idx, 51);
    }
}
