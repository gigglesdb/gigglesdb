//! Synchronous Raft driver. A drive error is fatal; the caller must stop the replica.
use crate::{
    entry::{self, EntryKind, ProposalContext},
    error::{Error, Result},
    log_store::LogStore,
    state::{AppliedPosition, StateMachine},
};
use raft::{
    Config, RawNode, ReadOnlyOption, StateRole,
    eraftpb::{Entry, EntryType, Message, MessageType},
};
use std::sync::Arc;

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
    pub fn step(&mut self, from: u64, m: Message) -> Result<()> {
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
            || !m.get_snapshot().is_empty()
        {
            return Err(Error::Corrupt("unbound or unsupported peer message".into()));
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
                return Err(Error::Corrupt("snapshots are not supported yet".into()));
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
