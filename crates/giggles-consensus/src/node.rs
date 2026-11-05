use crate::authority::{
    self, Binding, ClockSource, Entry as AuthorityEntry, Fence, LeaderClock, Session,
};
use crate::{Command, MAX_COMMAND, MAX_FRAME, Result, Store, VOTERS};
use protobuf::Message as Pb;
use raft::{Config, RawNode, StateRole, eraftpb::*};

/// Immutable already-charged response, held only by its proposing process.
pub struct HeldOutput {
    fence: Fence,
    index: u64,
    reply: giggles_core::local::Reply,
}
/// One-use release proof correlated to the exact committed release entry.
pub struct ReleaseProof {
    fence: Fence,
    decision: u64,
}

pub struct Applied {
    pub context: Vec<u8>,
    pub index: u64,
    pub reply: giggles_core::local::Reply,
    fence: Option<Fence>,
    release: Option<u64>,
}
impl Applied {
    pub fn is_authenticated(&self) -> bool {
        self.fence.is_some()
    }
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
    pub fn term(&self) -> u64 {
        self.raw.raft.term
    }
    /// Called only after verifying a TLS client's leaf certificate. No remote
    /// principal/domain/task input is accepted; identity comes from committed state.
    pub fn authenticate(&self, verified_fingerprint: &str) -> Result<Session> {
        self.store.authority()?.session(verified_fingerprint)
    }
    fn propose_authority(&mut self, context: Vec<u8>, entry: AuthorityEntry) -> Result<()> {
        if !self.is_leader() {
            return Err("not leader".into());
        }
        let mut bytes = authority::PREFIX.to_vec();
        bytes.extend(serde_json::to_vec(&entry)?);
        if bytes.len() > MAX_COMMAND || context.len() > 64 {
            return Err("proposal limit".into());
        }
        self.raw.propose(context, bytes)?;
        Ok(())
    }
    /// Trusted process-owner provisioning only. Each accepted reload fences every
    /// old session, including unchanged overlapping certificates; reconnect them.
    pub fn reload_credentials(
        &mut self,
        context: Vec<u8>,
        expected: u64,
        bindings: Vec<Binding>,
    ) -> Result<()> {
        authority::Authority::validated(self.store.state()?.domain, &bindings)?;
        expected.checked_add(1).ok_or("epoch overflow")?;
        self.propose_authority(context, AuthorityEntry::Reload { expected, bindings })
    }
    pub fn establish_time<C: ClockSource>(
        &mut self,
        context: Vec<u8>,
        clock: &mut LeaderClock<C>,
    ) -> Result<()> {
        if !self.is_leader() {
            return Err("not leader".into());
        }
        let a = self.store.authority()?;
        let (lower, upper) = clock.interval(a.watermark, 0)?;
        self.propose_authority(
            context,
            AuthorityEntry::Time {
                term: self.term(),
                lower,
                upper,
            },
        )
    }
    fn fresh_fence<C: ClockSource>(
        &self,
        session: &Session,
        clock: &mut LeaderClock<C>,
        horizon: u64,
    ) -> Result<Fence> {
        if !self.is_leader() {
            return Err("not leader".into());
        }
        let a = self.store.authority()?;
        if a.term != self.term() {
            return Err("leader time not established".into());
        }
        let (lower, upper) = clock.interval(a.watermark, horizon)?;
        let f = Fence {
            binding: session.binding.clone(),
            epoch: session.epoch,
            term: self.term(),
            lower,
            upper,
        };
        a.check(&f, self.term())?;
        Ok(f)
    }
    pub fn decide<C: ClockSource>(
        &mut self,
        context: Vec<u8>,
        session: &Session,
        action: crate::TaskAction,
        clock: &mut LeaderClock<C>,
    ) -> Result<()> {
        let fence = self.fresh_fence(session, clock, authority::DECISION_HORIZON)?;
        self.propose_authority(context, AuthorityEntry::Decide { fence, action })
    }
    /// Consume a decision from this node's trusted output stream. The runtime must
    /// correlate its locally submitted context; follower apply events are not ACKs.
    pub fn hold(&self, applied: Applied) -> Result<HeldOutput> {
        let fence = applied.fence.ok_or("not authenticated decision")?;
        if applied.release.is_some() || !self.is_leader() || fence.term != self.term() {
            return Err("stale decision".into());
        }
        Ok(HeldOutput {
            fence,
            index: applied.index,
            reply: applied.reply,
        })
    }
    pub fn request_release<C: ClockSource>(
        &mut self,
        context: Vec<u8>,
        held: &HeldOutput,
        clock: &mut LeaderClock<C>,
    ) -> Result<()> {
        if held.fence.term != self.term() {
            return Err("stale held term".into());
        }
        let session = Session {
            binding: held.fence.binding.clone(),
            epoch: held.fence.epoch,
        };
        let fence = self.fresh_fence(&session, clock, authority::DECISION_HORIZON)?;
        self.propose_authority(
            context,
            AuthorityEntry::Release {
                fence,
                decision: held.index,
            },
        )
    }
    pub fn release_proof(&self, applied: Applied) -> Result<ReleaseProof> {
        if applied.reply.code != 0 {
            return Err("release denied".into());
        }
        Ok(ReleaseProof {
            fence: applied.fence.ok_or("not release")?,
            decision: applied.release.ok_or("not release")?,
        })
    }
    /// Consumes held bytes and proof once; rechecks local applied state and fresh
    /// clock just before handing bytes to the listener. A pause after this check
    /// and before actual socket delivery cannot be ruled out by this API.
    pub fn finish_release<C: ClockSource>(
        &self,
        held: HeldOutput,
        proof: ReleaseProof,
        clock: &mut LeaderClock<C>,
    ) -> Result<giggles_core::local::Reply> {
        if !self.is_leader()
            || self.term() != proof.fence.term
            || proof.decision != held.index
            || proof.fence.binding != held.fence.binding
            || proof.fence.epoch != held.fence.epoch
            || proof.fence.term != held.fence.term
        {
            return Err("release correlation".into());
        }
        let session = Session {
            binding: held.fence.binding,
            epoch: held.fence.epoch,
        };
        let f = self.fresh_fence(&session, clock, 0)?;
        if f.upper > proof.fence.upper {
            return Err("release proof expired".into());
        }
        let (_, check) = self.store.image()?.evaluate(&Command::ValidateRelease {
            task: f.binding.task,
            principal: f.binding.principal,
            lower: f.lower,
            upper: proof.fence.upper,
        })?;
        if check.code != 0 {
            return Err("current task denied release".into());
        }
        Ok(held.reply)
    }
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
        command.validate()?;
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
                let (reply, fence, release) =
                    if let Some(bytes) = e.data.strip_prefix(authority::PREFIX) {
                        let entry: AuthorityEntry = serde_json::from_slice(bytes)?;
                        Self::apply_authority(s, e.term, entry)?
                    } else {
                        let command: Command = serde_json::from_slice(&e.data)?;
                        let (image, reply) = s.image.evaluate(&command)?;
                        s.image = image;
                        (reply, None, None)
                    };
                out.push(Applied {
                    context: e.context.to_vec(),
                    index: e.index,
                    reply,
                    fence,
                    release,
                });
            }
            s.applied = e.index;
            s.applied_term = e.term;
        }
        Ok(out)
    }
    fn apply_authority(
        s: &mut crate::store::State,
        term: u64,
        entry: AuthorityEntry,
    ) -> Result<(giggles_core::local::Reply, Option<Fence>, Option<u64>)> {
        use giggles_core::local::Reply;
        let denied = || Reply::error(100, "authority denied");
        match entry {
            AuthorityEntry::Reload { expected, bindings } => {
                let map = authority::Authority::validated(s.domain, &bindings);
                if expected != s.authority.epoch || expected == u64::MAX || map.is_err() {
                    return Ok((denied(), None, None));
                }
                s.authority.bindings = map?;
                s.authority.epoch = expected + 1;
                Ok((
                    Reply::ok(serde_json::json!({"epoch":expected+1})),
                    None,
                    None,
                ))
            }
            AuthorityEntry::Time {
                term: expected,
                lower,
                upper,
            } => {
                if expected != term
                    || lower > upper
                    || upper - lower > authority::MAX_INTERVAL
                    || upper < s.authority.watermark
                    || expected < s.authority.term
                {
                    return Ok((denied(), None, None));
                }
                s.authority.term = expected;
                s.authority.watermark = s.authority.watermark.max(lower);
                Ok((
                    Reply::ok(
                        serde_json::json!({"time_term":expected,"watermark":s.authority.watermark}),
                    ),
                    None,
                    None,
                ))
            }
            AuthorityEntry::Decide { fence, action } => {
                if s.authority.check(&fence, term).is_err() {
                    return Ok((denied(), Some(fence), None));
                }
                let (image, reply) = s.image.evaluate(&Command::NativeTask {
                    task: fence.binding.task,
                    principal: fence.binding.principal,
                    lower: fence.lower,
                    upper: fence.upper,
                    action,
                })?;
                s.image = image;
                s.authority.watermark = s.authority.watermark.max(fence.lower);
                Ok((reply, Some(fence), None))
            }
            AuthorityEntry::Release { fence, decision } => {
                if decision == 0 || decision > s.applied || s.authority.check(&fence, term).is_err()
                {
                    return Ok((denied(), Some(fence), Some(decision)));
                }
                let (image, reply) = s.image.evaluate(&Command::ValidateRelease {
                    task: fence.binding.task,
                    principal: fence.binding.principal,
                    lower: fence.lower,
                    upper: fence.upper,
                })?;
                s.image = image;
                s.authority.watermark = s.authority.watermark.max(fence.lower);
                Ok((reply, Some(fence), Some(decision)))
            }
        }
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
