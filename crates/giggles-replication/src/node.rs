//! Synchronous Raft driver. A drive error is fatal; the caller must stop the replica.
use crate::{
    entry::{self, EntryKind, ProposalContext},
    error::{Error, Result},
    log_store::{LogStore, PendingInstall},
    membership::{AdminCommand, Membership},
    snapshot::Manifest,
    state::{AppliedPosition, StateMachine},
};
use protobuf::Message as _;
use raft::{
    Config, RawNode, ReadOnlyOption, StateRole,
    eraftpb::{ConfChangeV2, ConfState, Entry, EntryType, Message, MessageType},
};
use std::{collections::BTreeSet, path::Path, sync::Arc};

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
    pub refused: Vec<(u64, String)>,
    pub membership_changed: bool,
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
    membership: Membership,
}
impl Replica {
    pub fn new(log: LogStore, state: Arc<dyn StateMachine>, voters: &[u64]) -> Result<Self> {
        let (cluster, node_id) = log.identity();
        log.initialize_voters(voters)?;
        if let Some(p) = log.pending_install()? {
            if state.applied().map_err(|e| Error::Apply(e.to_string()))?
                == Some(AppliedPosition {
                    index: p.index,
                    term: p.term,
                })
            {
                log.install_snapshot_meta(p.index, p.term, &p.conf, &p.membership)?;
            } else {
                log.clear_pending_install()?;
            }
        }
        if let Some(p) = state.applied().map_err(|e| Error::Apply(e.to_string()))?
            && p.index > log.hard_state()?.commit
        {
            log.install_snapshot_meta(
                p.index,
                p.term,
                &log.conf_state()?,
                &log.membership()?.unwrap_or_default(),
            )?;
        }
        let membership = Self::decode_membership(log.membership()?.as_deref())?;
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
            membership,
        })
    }
    fn decode_membership(bytes: Option<&[u8]>) -> Result<Membership> {
        bytes.map_or(Ok(Membership::default()), |b| {
            Membership::decode(b).map_err(|e| Error::Corrupt(e.to_string()))
        })
    }
    pub fn membership(&self) -> &Membership {
        &self.membership
    }
    pub fn conf_state(&self) -> ConfState {
        self.raw.raft.prs().conf().to_conf_state()
    }
    pub fn last_index(&self) -> u64 {
        self.raw.raft.raft_log.last_index()
    }
    pub fn peer_matched(&self, id: u64) -> Option<u64> {
        (self.role().state == StateRole::Leader)
            .then(|| self.raw.raft.prs().get(id).map(|p| p.matched))
            .flatten()
    }
    pub fn reachable_voters(&self) -> BTreeSet<u64> {
        if self.role().state != StateRole::Leader {
            return BTreeSet::new();
        }
        let cs = self.conf_state();
        cs.voters
            .iter()
            .chain(&cs.voters_outgoing)
            .chain(std::iter::once(&self.node_id))
            .copied()
            .filter(|&id| {
                id == self.node_id || self.raw.raft.prs().get(id).is_some_and(|p| p.recent_active)
            })
            .collect()
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
    // Keep the checkpoint position, full configuration, and membership together.
    #[allow(clippy::type_complexity)]
    pub fn prepare_snapshot(
        &mut self,
        staging: &Path,
    ) -> Result<Option<(u64, u64, ConfState, Vec<u8>)>> {
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
        if self.log().conf_index()? > p.index {
            return Ok(None);
        }
        let conf = self.log().conf_state()?;
        let membership = self.log().membership()?.unwrap_or_default();
        let q = self
            .state
            .checkpoint(staging)
            .map_err(|e| Error::Apply(e.to_string()))?;
        if q != p {
            return Err(Error::Corrupt("checkpoint position changed".into()));
        }
        Ok(Some((p.index, p.term, conf, membership)))
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
            let Some((index, term, conf, membership)) = self.prepare_snapshot(&stage)? else {
                return Ok(None);
            };
            snapshots
                .build(index, term, &conf, &membership, |dir| {
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
        let cs = self.conf_state();
        let empty = cs.voters.is_empty()
            && cs.voters_outgoing.is_empty()
            && cs.learners.is_empty()
            && cs.learners_next.is_empty();
        let known = cs
            .voters
            .iter()
            .chain(&cs.voters_outgoing)
            .chain(&cs.learners)
            .chain(&cs.learners_next)
            .any(|&id| id == from);
        if self.membership.is_retired(from)
            || (!empty && !known)
            || m.from != from
            || m.to != self.node_id
            || raft::raw_node::is_local_msg(m.get_msg_type())
            || matches!(
                m.get_msg_type(),
                MessageType::MsgPropose | MessageType::MsgReadIndex
            )
            || m.entries
                .iter()
                .any(|e| e.get_entry_type() == EntryType::EntryConfChange)
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
    fn propose_with(
        &mut self,
        waiter: u64,
        submit: impl FnOnce(&mut RawNode<LogStore>, Vec<u8>) -> raft::Result<()>,
    ) -> Result<()> {
        if self.role().state != StateRole::Leader {
            return Err(Error::NotLeader);
        }
        let context = ProposalContext {
            proposer: self.node_id,
            seq: waiter,
        }
        .encode();
        submit(&mut self.raw, context).map_err(Error::Raft)?;
        self.pending.push(Pending {
            waiter,
            index: self.last_index(),
            term: self.raw.raft.term,
        });
        Ok(())
    }
    pub fn propose(&mut self, payload: &[u8], waiter: u64) -> Result<()> {
        self.propose_payload(EntryKind::Batch, payload, waiter)
    }
    fn propose_payload(&mut self, kind: EntryKind, payload: &[u8], waiter: u64) -> Result<()> {
        if self.role().state != StateRole::Leader {
            return Err(Error::NotLeader);
        }
        let data = entry::encode(self.cluster, kind, payload)
            .map_err(|e| Error::Corrupt(e.to_string()))?;
        self.propose_with(waiter, |raw, ctx| raw.propose(ctx, data))
    }
    pub fn propose_admin(&mut self, cmd: &AdminCommand, waiter: u64) -> Result<()> {
        self.propose_payload(EntryKind::Admin, &cmd.encode(), waiter)
    }
    pub fn propose_conf_change(&mut self, cc: ConfChangeV2, waiter: u64) -> Result<()> {
        self.propose_with(waiter, |raw, ctx| raw.propose_conf_change(ctx, cc))
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
            let mut refused = None;
            let result = match e.get_entry_type() {
                EntryType::EntryConfChangeV2 => {
                    let cc = ConfChangeV2::parse_from_bytes(&e.data)
                        .map_err(|e| Error::Corrupt(e.to_string()))?;
                    if e.index > self.log().conf_index()? {
                        if self.membership.accept_conf_change(&cc, e.index) {
                            let cs = self.raw.apply_conf_change(&cc).map_err(Error::Raft)?;
                            crate::failpoint::hit(
                                &self.cluster,
                                Some(self.node_id),
                                "node.conf.before",
                            );
                            self.log()
                                .apply_conf(&cs, e.index, &self.membership.encode())?;
                            crate::failpoint::hit(
                                &self.cluster,
                                Some(self.node_id),
                                "node.conf.after",
                            );
                            out.membership_changed = true;
                        } else {
                            refused = Some("unexpected membership configuration change".to_owned());
                        }
                    }
                    self.state.apply_noop(at)
                }
                EntryType::EntryConfChange => self.state.apply_noop(at),
                EntryType::EntryNormal if e.data.is_empty() => self.state.apply_noop(at),
                EntryType::EntryNormal => {
                    let (kind, payload) = entry::decode(self.cluster, &e.data)
                        .map_err(|e| Error::Corrupt(e.to_string()))?;
                    match kind {
                        EntryKind::Batch => {
                            crate::failpoint::hit(
                                &self.cluster,
                                Some(self.node_id),
                                "node.apply.before",
                            );
                            let result = self.state.apply(payload, at);
                            if result.is_ok() {
                                crate::failpoint::hit(
                                    &self.cluster,
                                    Some(self.node_id),
                                    "node.apply.after",
                                );
                            }
                            result
                        }
                        EntryKind::Admin => {
                            if e.index > self.log().conf_index()? {
                                let cs = self.log().conf_state()?;
                                match AdminCommand::decode(payload)
                                    .and_then(|cmd| self.membership.apply_admin(&cmd, &cs, e.index))
                                {
                                    Ok(()) => {
                                        crate::failpoint::hit(
                                            &self.cluster,
                                            Some(self.node_id),
                                            "node.admin.before",
                                        );
                                        self.log().apply_conf(
                                            &cs,
                                            e.index,
                                            &self.membership.encode(),
                                        )?;
                                        crate::failpoint::hit(
                                            &self.cluster,
                                            Some(self.node_id),
                                            "node.admin.after",
                                        );
                                        out.membership_changed = true;
                                    }
                                    Err(e) => refused = Some(e.to_string()),
                                }
                            }
                            self.state.apply_noop(at)
                        }
                    }
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
                    if let Some(reason) = &refused {
                        out.refused.push((p.waiter, reason.clone()));
                    } else {
                        out.applied.push((p.waiter, e.index));
                    }
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
            refused: Vec::new(),
            membership_changed: false,
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
                self.log().set_pending_install(&PendingInstall {
                    index: manifest.index,
                    term: manifest.term,
                    conf: snap.get_metadata().get_conf_state().clone(),
                    membership: manifest.membership.clone(),
                })?;
                crate::failpoint::hit(
                    &self.cluster,
                    Some(self.node_id),
                    "snapshot.install.pending",
                );
                let pos = self
                    .state
                    .install(&dir)
                    .map_err(|e| Error::Apply(e.to_string()))?;
                crate::failpoint::hit(&self.cluster, Some(self.node_id), "snapshot.install.state");
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
                    &manifest.membership,
                )?;
                self.membership = Self::decode_membership(self.log().membership()?.as_deref())?;
                out.membership_changed = true;
                snapshots.discard_incoming_through(pos.index);
            }
            self.log().persist(ready.entries(), ready.hs())?;
            if ready.hs().is_some() {
                crate::failpoint::hit(&self.cluster, Some(self.node_id), "node.commit.persisted");
            }
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
                crate::failpoint::hit(&self.cluster, Some(self.node_id), "node.commit.persisted");
            }
            out.messages.extend(light.take_messages());
            self.apply(light.committed_entries(), &mut out)?;
            self.raw.advance_apply();
            self.fail_deposed(&mut out);
        }
        self.fail_deposed(&mut out);
        if self.role().state == StateRole::Leader {
            let cs = self.conf_state();
            out.messages.retain_mut(|m| {
                if m.get_msg_type() != MessageType::MsgAppend
                    || m.index != 0
                    || (!cs.learners.contains(&m.to) && !cs.learners_next.contains(&m.to))
                {
                    return true;
                }
                let Some(snapshot) = self.log().snapshots().and_then(|s| s.latest_snapshot())
                else {
                    return false;
                };
                let conf = snapshot.get_metadata().get_conf_state();
                if !conf
                    .voters
                    .iter()
                    .chain(&conf.learners)
                    .chain(&conf.voters_outgoing)
                    .any(|&id| id == m.to)
                {
                    return false;
                }
                let index = snapshot.get_metadata().index;
                *m = Message {
                    msg_type: MessageType::MsgSnapshot,
                    to: m.to,
                    from: self.node_id,
                    term: m.term,
                    snapshot: Some(snapshot).into(),
                    ..Default::default()
                };
                if let Some(peer) = self.raw.raft.mut_prs().get_mut(m.to) {
                    peer.become_snapshot(index);
                }
                true
            });
        }
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
