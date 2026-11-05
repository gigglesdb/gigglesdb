use crate::{Command, MAX_COMMAND, MAX_FRAME, Result, Store, VOTERS};
use giggles_core::local::Reply;
use protobuf::Message as Pb;
use raft::{Config, RawNode, StateRole, eraftpb::*};
pub struct Applied {
    pub context: Vec<u8>,
    pub index: u64,
    pub reply: Reply,
}
#[derive(Default)]
pub struct Output {
    pub messages: Vec<Message>,
    pub applied: Vec<Applied>,
    pub reads: Vec<(Vec<u8>, u64)>,
}
pub struct Node {
    pub(crate) raw: RawNode<Store>,
    pub store: Store,
}
impl Node {
    pub fn new(store: Store) -> Result<Self> {
        let s = store.state()?;
        let c = Config {
            id: s.node,
            election_tick: 10,
            heartbeat_tick: 2,
            applied: s.applied,
            max_size_per_msg: MAX_COMMAND as u64,
            max_inflight_msgs: 8,
            max_uncommitted_size: 8 * MAX_COMMAND as u64,
            check_quorum: true,
            pre_vote: true,
            read_only_option: raft::ReadOnlyOption::Safe,
            ..Default::default()
        };
        c.validate()?;
        let logger = slog::Logger::root(slog::Discard, slog::o!());
        let raw = RawNode::new(&c, store.clone(), &logger)?;
        Ok(Self { raw, store })
    }
    pub fn leader(&self) -> u64 {
        self.raw.raft.leader_id
    }
    pub fn is_leader(&self) -> bool {
        self.raw.raft.state == StateRole::Leader
    }
    pub fn diagnostics(&self) -> String {
        format!(
            "id={} term={} vote={} leader={} role={:?} commit={} applied={}",
            self.raw.raft.id,
            self.raw.raft.term,
            self.raw.raft.vote,
            self.leader(),
            self.raw.raft.state,
            self.raw.raft.raft_log.committed,
            self.raw.raft.raft_log.applied
        )
    }
    pub fn tick(&mut self) {
        self.raw.tick();
    }
    /// Deterministic scheduler seam; sets library timeout before every test tick.
    pub fn deterministic_tick(&mut self) {
        self.raw
            .raft
            .set_randomized_election_timeout(10 + self.raw.raft.id as usize);
        self.raw.tick();
    }
    /// Trusted deterministic test/runtime input. Elections still run the library.
    pub fn campaign(&mut self) -> Result<()> {
        self.raw.campaign()?;
        Ok(())
    }
    pub fn transfer(&mut self, to: u64) -> Result<()> {
        if !self.is_leader() || !VOTERS.contains(&to) {
            return Err("leader/target".into());
        }
        self.raw.transfer_leader(to);
        Ok(())
    }
    pub fn report_snapshot(&mut self, to: u64, success: bool) {
        self.raw.report_snapshot(
            to,
            if success {
                raft::SnapshotStatus::Finish
            } else {
                raft::SnapshotStatus::Failure
            },
        );
    }
    pub fn step(&mut self, authenticated_node: u64, message: Message) -> Result<()> {
        if !VOTERS.contains(&authenticated_node)
            || message.from != authenticated_node
            || message.to != self.raw.raft.id
            || message.compute_size() as usize > MAX_FRAME
            || raft::raw_node::is_local_msg(message.get_msg_type())
            || matches!(
                message.get_msg_type(),
                MessageType::MsgPropose | MessageType::MsgReadIndex
            )
        {
            return Err("unbound/unsupported node message".into());
        }
        if message
            .get_entries()
            .iter()
            .any(|e| e.get_entry_type() != EntryType::EntryNormal || e.data.len() > MAX_COMMAND)
        {
            return Err("unsupported membership/entry".into());
        }
        let identity = self.store.image()?;
        for entry in message.get_entries().iter().filter(|e| !e.data.is_empty()) {
            let command: Command = serde_json::from_slice(&entry.data)?;
            if command.domain != identity.domain()? || command.group != identity.group() {
                return Err("cross-group log entry".into());
            }
        }
        if message.has_snapshot() && !message.get_snapshot().is_empty() {
            self.store.verify_snapshot(message.get_snapshot())?;
        }
        self.raw.step(message)?;
        Ok(())
    }
    pub fn propose(&mut self, context: Vec<u8>, command: &Command) -> Result<()> {
        if !self.is_leader() {
            return Err("not leader".into());
        }
        let identity = self.store.image()?;
        if command.domain != identity.domain()? || command.group != identity.group() {
            return Err("proposal domain/group".into());
        }

        let bytes = serde_json::to_vec(command)?;
        if bytes.len() > MAX_COMMAND || context.len() > 64 {
            return Err("proposal limit".into());
        }
        self.raw.propose(context, bytes)?;
        Ok(())
    }
    pub fn read_barrier(&mut self, context: Vec<u8>) -> Result<()> {
        if !self.is_leader() || context.is_empty() || context.len() > 64 {
            return Err("leader/read context".into());
        }
        self.raw.read_index(context);
        Ok(())
    }
    fn apply(s: &mut crate::store::State, entries: &[Entry]) -> Result<Vec<Applied>> {
        let mut out = Vec::new();
        for e in entries {
            if e.index <= s.applied {
                continue;
            }
            if e.index != s.applied + 1 || e.get_entry_type() != EntryType::EntryNormal {
                return Err("apply gap/membership".into());
            }
            if !e.data.is_empty() {
                let command: Command = serde_json::from_slice(&e.data)?;
                let reply = s.image.apply(&command, e.index);
                out.push(Applied {
                    context: e.context.to_vec(),
                    index: e.index,
                    reply,
                });
            }
            s.applied = e.index;
            s.applied_term = e.term;
        }
        Ok(out)
    }
    /// No outbound message or application response escapes before durable save.
    pub fn drain(&mut self) -> Result<Output> {
        let mut out = Output::default();
        while self.raw.has_ready() {
            let mut ready = self.raw.ready();
            let results = self.store.update(|s| {
                if !ready.snapshot().is_empty() {
                    s.install(ready.snapshot())?;
                }
                let entries = ready.entries();
                if let Some(first) = entries.first() {
                    if first.index <= s.hard()?.commit {
                        return Err("overwrite committed log".into());
                    }
                    s.log = s
                        .log
                        .range(..first.index)
                        .map(|(i, b)| (*i, b.clone()))
                        .collect();
                    for e in entries {
                        s.log.insert(e.index, e.write_to_bytes()?);
                    }
                }
                if let Some(h) = ready.hs() {
                    s.hard = h.write_to_bytes()?;
                }
                Self::apply(s, ready.committed_entries())
            })?;
            out.applied.extend(results);
            out.messages.extend(ready.take_messages());
            out.messages.extend(ready.take_persisted_messages());
            let reads = ready.take_read_states();
            let mut light = self.raw.advance(ready);
            let results = self.store.update(|s| {
                if let Some(commit) = light.commit_index() {
                    let mut h = s.hard()?;
                    h.commit = commit;
                    s.hard = h.write_to_bytes()?;
                }
                Self::apply(s, light.committed_entries())
            })?;
            out.applied.extend(results);
            out.messages.extend(light.take_messages());
            self.raw.advance_apply();
            let applied = self.store.applied()?;
            for r in reads {
                if applied < r.index {
                    return Err("read barrier not applied".into());
                }
                out.reads.push((r.request_ctx, r.index));
            }
        }
        Ok(out)
    }
}
