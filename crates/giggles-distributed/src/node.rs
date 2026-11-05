use crate::{Command, MAX_COMMAND, MAX_FRAME, Result, Store};
use giggles_membership::{
    Change, Command as MembershipCommand, Intent, Operation, Readiness, Step,
};
use serde::Serialize;
#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
enum LogCommand {
    Membership(MembershipCommand),
}
enum Decoded {
    Application(Box<Command>),
    Membership(MembershipCommand),
}
fn decode(bytes: &[u8]) -> Result<Decoded> {
    let value: serde_json::Value = serde_json::from_slice(bytes)?;
    if let Some(m) = value.get("membership") {
        if value.as_object().is_none_or(|v| v.len() != 1) {
            return Err("membership envelope".into());
        }
        Ok(Decoded::Membership(serde_json::from_value(m.clone())?))
    } else {
        Ok(Decoded::Application(Box::new(serde_json::from_value(
            value,
        )?)))
    }
}
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
    fenced: bool,
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
        Ok(Self {
            raw,
            store,
            fenced: false,
        })
    }
    pub fn leader(&self) -> u64 {
        self.raw.raft.leader_id
    }
    pub fn is_leader(&self) -> bool {
        !self.fenced && self.raw.raft.state == StateRole::Leader && self.raw.raft.promotable()
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
        if self.fenced || !self.raw.raft.promotable() {
            return Err("not eligible voter".into());
        }
        self.raw.campaign()?;
        Ok(())
    }
    pub fn transfer(&mut self, to: u64) -> Result<()> {
        if !self.is_leader() || !self.store.configuration()?.voters.contains(&to) {
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
        if self.fenced
            || !(self
                .store
                .configuration()?
                .voters
                .contains(&authenticated_node)
                || self
                    .store
                    .configuration()?
                    .voters_outgoing
                    .contains(&authenticated_node)
                || self
                    .store
                    .configuration()?
                    .learners
                    .contains(&authenticated_node))
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
            .any(|e| e.get_entry_type() == EntryType::EntryConfChange || e.data.len() > MAX_COMMAND)
        {
            return Err("unsupported membership/entry".into());
        }
        let identity = self.store.image()?;
        for entry in message.get_entries().iter().filter(|e| !e.data.is_empty()) {
            match entry.get_entry_type() {
                EntryType::EntryNormal => match decode(&entry.data)? {
                    Decoded::Application(command)
                        if command.domain == identity.domain()?
                            && command.group == identity.group() => {}
                    Decoded::Membership(command) if command.identity.group == identity.group() => {}
                    _ => return Err("cross-group log entry".into()),
                },
                EntryType::EntryConfChangeV2 => {
                    let cc = ConfChangeV2::parse_from_bytes(&entry.data)?;
                    let change: Change = serde_json::from_slice(&cc.context)?;
                    if change.identity.group != identity.group() {
                        return Err("cross-group configuration".into());
                    }
                }
                _ => return Err("unsupported configuration".into()),
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
    fn membership_command(&mut self, context: Vec<u8>, operation: Operation) -> Result<()> {
        if context.is_empty() || context.len() > 64 {
            return Err("membership context bounds".into());
        }
        if !self.is_leader() {
            return Err("not active leader".into());
        }
        let m = self.store.membership()?;
        let command = MembershipCommand {
            identity: m.identity,
            operation,
        };
        let mut trial = m.clone();
        trial.apply(
            &command,
            self.store
                .applied()?
                .checked_add(1)
                .ok_or("index exhausted")?,
            self.raw.raft.term,
        )?;
        self.raw.propose(
            context,
            serde_json::to_vec(&LogCommand::Membership(command))?,
        )?;
        Ok(())
    }
    pub fn membership_begin(&mut self, context: Vec<u8>, intent: Intent) -> Result<()> {
        let state = self.store.state()?;
        if !state.roster.contains_key(&intent.learner) {
            return Err("learner not provisioned".into());
        }
        if !matches!(&state.image, crate::Machine::Range {data,..} if data.active && data.fence.is_none())
        {
            return Err("replacement requires active unfenced range".into());
        }
        self.membership_command(context, Operation::Begin(intent))
    }
    pub fn membership_change(&mut self, context: Vec<u8>, change: Change) -> Result<bool> {
        if context.is_empty() || context.len() > 64 {
            return Err("membership context bounds".into());
        }
        if !self.is_leader() {
            return Err("not active leader".into());
        }
        let m = self.store.membership()?;
        if m.change_applied(&change)? {
            return Ok(false);
        }
        let p = m.progress()?;
        if self.raw.raft.pending_conf_index > self.raw.raft.raft_log.applied {
            return Err("configuration pending; retry".into());
        }
        if change.step == Step::EnterJoint
            && p.readiness.as_ref().ok_or("not durable ready")?.term != self.raw.raft.term
        {
            return Err("old-term readiness".into());
        }
        self.raw
            .propose_conf_change(context, m.configuration(&change)?)?;
        Ok(true)
    }
    pub(crate) fn accept_readiness(
        &mut self,
        context: Vec<u8>,
        from: u64,
        r: Readiness,
    ) -> Result<()> {
        let m = self.store.membership()?;
        let p = m.progress()?;
        if !self.is_leader()
            || from != p.intent.learner
            || r.term != self.raw.raft.term
            || self
                .raw
                .raft
                .prs()
                .get(from)
                .is_none_or(|p| p.matched < r.barrier)
        {
            return Err("unauthenticated/stale replication readiness".into());
        }
        self.membership_command(context, Operation::Ready(r))
    }
    pub(crate) fn durable_probe(
        &self,
        from: u64,
        request: &crate::transport::Probe,
    ) -> Result<Readiness> {
        let s = self.store.state()?;
        let p = s.membership.progress()?;
        if self.fenced
            || from != self.leader()
            || request.term != self.raw.raft.term
            || request.intent != p.intent.id
            || request.epoch != p.intent.epoch
            || p.intent.learner != s.node
            || s.applied < request.barrier
            || request.barrier < p.added_at
        {
            return Err("probe identity/term/application fence".into());
        }
        Ok(Readiness {
            intent: request.intent,
            epoch: request.epoch,
            term: request.term,
            barrier: request.barrier,
            durable_applied: request.barrier,
        })
    }
    pub fn read_barrier(&mut self, context: Vec<u8>) -> Result<()> {
        if !self.is_leader() || context.is_empty() || context.len() > 64 {
            return Err("leader/read context".into());
        }
        self.raw.read_index(context);
        Ok(())
    }
    fn apply(&mut self, s: &mut crate::store::State, entries: &[Entry]) -> Result<Vec<Applied>> {
        let mut out = Vec::new();
        for e in entries {
            if e.index <= s.applied {
                continue;
            }
            if e.index != s.applied + 1 {
                return Err("apply gap/membership".into());
            }
            let reply = match e.get_entry_type() {
                EntryType::EntryNormal if !e.data.is_empty() => match decode(&e.data)? {
                    Decoded::Application(command) => {
                        if (s.membership.active.is_some() || s.membership.epoch != 1)
                            && matches!(
                                command.action,
                                crate::Action::Fence(_) | crate::Action::Bootstrap(_)
                            )
                        {
                            Reply::error(1, "membership transition blocks split")
                        } else {
                            s.image.apply(&command, e.index)
                        }
                    }
                    Decoded::Membership(command) => {
                        let result = if matches!(command.operation, Operation::Begin(_))
                            && !matches!(&s.image, crate::Machine::Range { data, .. } if data.active && data.fence.is_none())
                        {
                            Err("replacement requires active unfenced range".into())
                        } else {
                            s.membership.apply(&command, e.index, e.term)
                        };
                        match result {
                            Ok(()) => Reply::ok(serde_json::json!({"membership":s.membership})),
                            Err(error) => Reply::error(1, error.to_string()),
                        }
                    }
                },
                EntryType::EntryNormal => {
                    s.applied = e.index;
                    s.applied_term = e.term;
                    continue;
                }
                EntryType::EntryConfChangeV2 => {
                    let cc = ConfChangeV2::parse_from_bytes(&e.data)?;
                    let change: Change = serde_json::from_slice(&cc.context)?;
                    if s.membership.configuration(&change)? != cc {
                        return Err("configuration payload mismatch".into());
                    }
                    let conf = self.raw.apply_conf_change(&cc)?;
                    s.membership
                        .applied_configuration(&change, e.index, &conf)?;
                    s.conf = conf.write_to_bytes()?;
                    Reply::ok(serde_json::json!({"membership":s.membership}))
                }
                _ => return Err("unsupported configuration apply".into()),
            };
            out.push(Applied {
                context: e.context.to_vec(),
                index: e.index,
                reply,
            });
            s.applied = e.index;
            s.applied_term = e.term;
        }
        Ok(out)
    }
    fn apply_committed(&mut self, entries: &[Entry]) -> Result<Vec<Applied>> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }
        // RawNode may consult Storage while applying a configuration. Do not
        // hold the durable store mutex across that call.
        let original = self.store.state()?;
        let mut next = original.clone();
        let out = self.apply(&mut next, entries)?;
        self.store.update(|s| {
            if *s != original {
                return Err("concurrent store modification; reopen".into());
            }
            *s = next;
            Ok(())
        })?;
        Ok(out)
    }
    /// No outbound message or application response escapes before durable save.
    pub fn drain(&mut self) -> Result<Output> {
        if self.fenced {
            return Err("node fenced; reopen durable store".into());
        }
        let result = self.drain_inner();
        if result.is_err() {
            self.fenced = true;
        }
        result
    }
    fn drain_inner(&mut self) -> Result<Output> {
        let mut out = Output::default();
        while self.raw.has_ready() {
            let mut ready = self.raw.ready();
            let store = self.store.clone();
            store.update(|s| {
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
                Ok(())
            })?;
            let results = self.apply_committed(ready.committed_entries())?;
            out.applied.extend(results);
            out.messages.extend(ready.take_messages());
            out.messages.extend(ready.take_persisted_messages());
            let reads = ready.take_read_states();
            let mut light = self.raw.advance(ready);
            let store = self.store.clone();
            store.update(|s| {
                if let Some(commit) = light.commit_index() {
                    let mut h = s.hard()?;
                    h.commit = commit;
                    s.hard = h.write_to_bytes()?;
                }
                Ok(())
            })?;
            let results = self.apply_committed(light.committed_entries())?;
            out.applied.extend(results);
            out.messages.extend(light.take_messages());
            self.raw.advance_apply();
            if self.raw.raft.state == StateRole::Leader && !self.raw.raft.promotable() {
                self.raw.raft.become_follower(self.raw.raft.term, 0);
            }
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

#[cfg(test)]
#[path = "membership_tests.rs"]
mod membership_tests;
