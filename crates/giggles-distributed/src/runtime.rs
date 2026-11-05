use crate::{
    Command, MAX_GROUPS, Machine, Node, Result, Store, machine::*, node::Output, transport,
};
use giggles_core::{Key, local::Reply};
use serde::{Deserialize, Serialize};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::{collections::BTreeMap, path::PathBuf, time::Duration};
use tokio::sync::{mpsc, oneshot};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GroupConfig {
    pub peer: transport::Configuration,
    pub initial: Initial,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum Initial {
    Directory(Descriptor),
    Data(Descriptor),
    Vacant,
}
impl Initial {
    fn build(&self, domain: u128, group: u64) -> Result<Machine> {
        let image = match self {
            Self::Directory(d) => Machine::root(domain, d.clone())?,
            Self::Data(d) => Machine::range(domain, d.clone())?,
            Self::Vacant => Machine::Vacant { domain, group },
        };
        image.validate(domain, group)?;
        Ok(image)
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub domain: u128,
    pub node: u64,
    pub groups: Vec<GroupConfig>,
}
type Answer = oneshot::Sender<std::result::Result<Observation, String>>;
enum Input {
    Inspect(Answer),
    Peer(transport::Event),
    Tick,
    Propose(Box<Command>, Answer),
    Read(Answer),
    Campaign(Answer),
    Checkpoint(Answer),
    Transfer(u64, Answer),
    Block(std::collections::BTreeSet<u64>, Answer),
    Arm(bool, Answer),
}
#[derive(Clone)]
pub struct Handle {
    tx: mpsc::Sender<Input>,
    stop: Arc<AtomicBool>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Observation {
    pub is_leader: bool,
    pub quorum_checked: bool,
    pub group: u64,
    pub leader: u64,
    pub applied: u64,
    pub machine: Machine,
    pub reply: Option<ReplyValue>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReplyValue {
    pub code: u32,
    pub data: serde_json::Value,
}
impl From<Reply> for ReplyValue {
    fn from(r: Reply) -> Self {
        Self {
            code: r.code,
            data: r.data,
        }
    }
}
impl Handle {
    async fn ask(&self, make: impl FnOnce(Answer) -> Input) -> Result<Observation> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .try_send(make(tx))
            .map_err(|_| "group admission full/closed")?;
        tokio::time::timeout(Duration::from_secs(5), rx)
            .await
            .map_err(|_| "indeterminate timeout; inspect/resume, never rollback")??
            .map_err(Into::into)
    }
    pub async fn propose(&self, c: Command) -> Result<Observation> {
        self.ask(|tx| Input::Propose(Box::new(c), tx)).await
    }
    pub async fn current(&self) -> Result<Observation> {
        self.ask(Input::Read).await
    }
    /// Local diagnostics only; never use this to authorize routed work.
    pub async fn inspect(&self) -> Result<Observation> {
        self.ask(Input::Inspect).await
    }
    pub async fn campaign(&self) -> Result<Observation> {
        self.ask(Input::Campaign).await
    }
    pub async fn arm_crash(&self, after: bool) -> Result<Observation> {
        self.ask(|tx| Input::Arm(after, tx)).await
    }
    pub async fn transfer(&self, to: u64) -> Result<Observation> {
        self.ask(|tx| Input::Transfer(to, tx)).await
    }
    pub async fn block(&self, peers: std::collections::BTreeSet<u64>) -> Result<Observation> {
        self.ask(|tx| Input::Block(peers, tx)).await
    }
    pub async fn checkpoint(&self) -> Result<Observation> {
        self.ask(Input::Checkpoint).await
    }
}
fn observation(node: &Node, reply: Option<Reply>) -> Result<Observation> {
    Ok(Observation {
        is_leader: node.is_leader(),
        quorum_checked: false,
        group: node.store.image()?.group(),
        leader: node.leader(),
        applied: node.store.applied()?,
        machine: node.store.image()?,
        reply: reply.map(Into::into),
    })
}
fn complete(
    node: &mut Node,
    out: Output,
    pending: &mut BTreeMap<Vec<u8>, Answer>,
    reads: &mut BTreeMap<Vec<u8>, Answer>,
    send: &mpsc::Sender<raft::eraftpb::Message>,
    blocked: &std::collections::BTreeSet<u64>,
) -> Result<()> {
    for message in out.messages {
        let to = message.to;
        let snapshot = message.has_snapshot();
        if blocked.contains(&to) {
            if snapshot {
                node.report_snapshot(to, false);
            }
            continue;
        }
        if send.try_send(message).is_err() && snapshot {
            node.report_snapshot(to, false);
        }
        // A dropped snapshot must be retried by the Raft progress tracker.
        let _ = to;
    }
    for a in out.applied {
        if let Some(tx) = pending.remove(&a.context) {
            let _ = tx.send(observation(node, Some(a.reply)).map_err(|e| e.to_string()));
        }
    }
    for (context, _) in out.reads {
        if let Some(tx) = reads.remove(&context) {
            let _ = tx.send(
                observation(node, None)
                    .map(|mut o| {
                        o.quorum_checked = true;
                        o
                    })
                    .map_err(|e| e.to_string()),
            );
        }
    }
    Ok(())
}
pub struct Runtime {
    pub groups: BTreeMap<u64, Handle>,
    pub domain: u128,
}
impl Drop for Runtime {
    fn drop(&mut self) {
        for h in self.groups.values() {
            h.stop.store(true, Ordering::Release);
            let _ = h.tx.try_send(Input::Tick);
        }
    }
}
impl Runtime {
    pub async fn start(config: Config) -> Result<Self> {
        if config.groups.is_empty() || config.groups.len() > MAX_GROUPS {
            return Err("group limit".into());
        }
        let mut groups = BTreeMap::<u64, ()>::new();
        let mut paths = std::collections::BTreeSet::<PathBuf>::new();
        // Validate the complete startup before opening any store/listener.
        for g in &config.groups {
            if g.peer.id != config.node
                || g.peer.domain != config.domain
                || groups.contains_key(&g.peer.group)
                || !paths.insert(g.peer.database.clone())
            {
                return Err("configuration identity/path collision".into());
            }
            g.initial.build(config.domain, g.peer.group)?;
            g.peer.validate()?;
            groups.insert(g.peer.group, ());
        }
        let mut runtime = Self {
            groups: BTreeMap::new(),
            domain: config.domain,
        };
        for g in config.groups {
            let store = if g.peer.database.exists() {
                Store::open(&g.peer.database, config.node, config.domain, g.peer.group)?
            } else {
                Store::create(
                    &g.peer.database,
                    config.node,
                    config.domain,
                    g.peer.group,
                    g.initial.build(config.domain, g.peer.group)?,
                )?
            };
            let mut node = Node::new(store)?;
            let stop = Arc::new(AtomicBool::new(false));
            let actor_stop = stop.clone();
            let (tx, mut input) = mpsc::channel(128);
            let (events, mut peer_events) = mpsc::channel(128);
            let outbound = transport::Transport::bind(g.peer.clone())
                .await?
                .run(events);
            let peer_tx = tx.clone();
            tokio::spawn(async move {
                loop {
                    let event = tokio::select! {
                        _ = peer_tx.closed() => break,
                        event = peer_events.recv() => match event {
                            Some(event) => event,
                            None => break,
                        },
                    };
                    if peer_tx.send(Input::Peer(event)).await.is_err() {
                        break;
                    }
                }
            });
            let ticks = tx.clone();
            let tick_stop = stop.clone();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_millis(100));
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    interval.tick().await;
                    if tick_stop.load(Ordering::Acquire) || ticks.is_closed() {
                        break;
                    }
                    let _ = ticks.try_send(Input::Tick);
                }
            });
            // Durable old entries survive actor restarts. Their apply contexts
            // must never match a new caller's pending slot, even when its local
            // counter repeats. This nonce is correlation only, not authority or
            // nondeterminism in state-machine evaluation.
            let mut incarnation = [0u8; 16];
            rustls::crypto::ring::default_provider()
                .secure_random
                .fill(&mut incarnation)
                .map_err(|_| "request correlation entropy unavailable")?;
            tokio::task::spawn_blocking(move || {
                let mut serial = 0u64;
                let mut pending = BTreeMap::<Vec<u8>, Answer>::new();
                let mut reads = BTreeMap::<Vec<u8>, Answer>::new();
                let mut term = 0;
                let mut blocked = std::collections::BTreeSet::new();
                while let Some(event) = input.blocking_recv() {
                    if actor_stop.load(Ordering::Acquire) {
                        break;
                    }
                    let result = (|| -> Result<()> {
                        match event {
                            Input::Inspect(tx) => {
                                let _ =
                                    tx.send(observation(&node, None).map_err(|e| e.to_string()));
                            }
                            Input::Tick => node.tick(),
                            Input::Peer(transport::Event::Message(from, m)) => {
                                if !blocked.contains(&from) {
                                    // A malformed or misrouted peer frame has no
                                    // authority to stop another ordering group.
                                    if let Err(error) = node.step(from, m) {
                                        eprintln!("rejected peer input: {error}");
                                    }
                                }
                            }
                            Input::Peer(transport::Event::SnapshotResult(to, ok)) => {
                                node.report_snapshot(to, ok)
                            }
                            Input::Arm(after, tx) => {
                                let r = node
                                    .store
                                    .arm_apply_crash(if after {
                                        crate::store::ApplyCrash::AfterCommit
                                    } else {
                                        crate::store::ApplyCrash::BeforeCommit
                                    })
                                    .and_then(|_| observation(&node, None));
                                let _ = tx.send(r.map_err(|e| e.to_string()));
                            }
                            Input::Transfer(to, tx) => {
                                let r = node.transfer(to).and_then(|_| observation(&node, None));
                                let _ = tx.send(r.map_err(|e| e.to_string()));
                            }
                            Input::Block(peers, tx) => {
                                blocked = peers;
                                let _ =
                                    tx.send(observation(&node, None).map_err(|e| e.to_string()));
                            }
                            Input::Campaign(tx) => {
                                let r = node.campaign().and_then(|_| observation(&node, None));
                                let _ = tx.send(r.map_err(|e| e.to_string()));
                            }
                            Input::Checkpoint(tx) => {
                                let r = node
                                    .store
                                    .checkpoint()
                                    .and_then(|_| observation(&node, None));
                                let _ = tx.send(r.map_err(|e| e.to_string()));
                            }
                            Input::Propose(c, tx) => {
                                if pending.len() + reads.len() >= 32 {
                                    let _ = tx.send(Err("pending admission full".into()));
                                } else {
                                    serial = serial.checked_add(1).ok_or("context exhausted")?;
                                    let mut context = incarnation.to_vec();
                                    context.extend(serial.to_be_bytes());
                                    match node.propose(context.clone(), &c) {
                                        Ok(()) => {
                                            pending.insert(context, tx);
                                        }
                                        Err(e) => {
                                            let _ = tx
                                                .send(Err(format!("leader={} {e}", node.leader())));
                                        }
                                    }
                                }
                            }
                            Input::Read(tx) => {
                                if pending.len() + reads.len() >= 32 {
                                    let _ = tx.send(Err("pending admission full".into()));
                                } else {
                                    serial = serial.checked_add(1).ok_or("context exhausted")?;
                                    let mut context = incarnation.to_vec();
                                    context.extend(serial.to_be_bytes());
                                    match node.read_barrier(context.clone()) {
                                        Ok(()) => {
                                            reads.insert(context, tx);
                                        }
                                        Err(e) => {
                                            let _ = tx
                                                .send(Err(format!("leader={} {e}", node.leader())));
                                        }
                                    }
                                }
                            }
                        }
                        let output = node.drain()?;
                        complete(
                            &mut node,
                            output,
                            &mut pending,
                            &mut reads,
                            &outbound,
                            &blocked,
                        )?;
                        let new_term = node.store.state()?.hard()?.term;
                        if new_term != term || !node.is_leader() {
                            for (_, tx) in std::mem::take(&mut pending)
                                .into_iter()
                                .chain(std::mem::take(&mut reads))
                            {
                                let _ = tx.send(Err(
                                    "indeterminate leadership change; inspect/resume".into(),
                                ));
                            }
                            term = new_term;
                        }
                        Ok(())
                    })();
                    if let Err(e) = result {
                        eprintln!("group {} stopped: {e}", g.peer.group);
                        break;
                    }
                }
            });
            runtime.groups.insert(g.peer.group, Handle { tx, stop });
        }
        Ok(runtime)
    }
    pub fn group(&self, group: u64) -> Result<&Handle> {
        self.groups
            .get(&group)
            .ok_or_else(|| "group not hosted".into())
    }
    pub async fn apply(&self, group: u64, action: Action) -> Result<Observation> {
        let o = self
            .group(group)?
            .propose(Command {
                domain: self.domain,
                group,
                action,
            })
            .await?;
        if let Some(r) = &o.reply
            && r.code != 0
        {
            return Err(format!("state machine rejected: {}", r.data).into());
        }
        Ok(o)
    }
    pub async fn concurrent(&self, group: u64, actions: Vec<Action>) -> Result<Vec<Observation>> {
        if actions.is_empty() || actions.len() > 16 {
            return Err("concurrent batch limit".into());
        }
        let mut jobs = tokio::task::JoinSet::new();
        let mut results = Vec::new();
        for action in actions {
            let h = self.group(group)?.clone();
            let domain = self.domain;
            jobs.spawn(async move {
                h.propose(Command {
                    domain,
                    group,
                    action,
                })
                .await
            });
        }
        while let Some(result) = jobs.join_next().await {
            results.push(result??);
        }
        Ok(results)
    }
    pub async fn directory(&self) -> Result<Directory> {
        match self.group(DIRECTORY)?.current().await?.machine {
            Machine::Root { directory, .. } => Ok(directory),
            _ => Err("not directory".into()),
        }
    }
    /// Recovery is forward-only. Every state transition is independently durable.
    /// All groups must currently be led here; otherwise use the typed steps on
    /// their reported leaders, or retry once leadership is colocated.
    pub async fn resume(&self, id: u64) -> Result<Directory> {
        let root = self.directory().await?;
        let intent = root.intents.get(&id).ok_or("unknown lifecycle id")?;
        if intent.published {
            for proof in intent.children.as_ref().ok_or("published without proofs")? {
                self.apply(
                    proof.child.group,
                    Action::Activate {
                        proof: proof.clone(),
                        revision: root.revision,
                    },
                )
                .await?;
            }
            return Ok(root);
        }
        let split = &intent.split;
        self.apply(split.parent.group, Action::Fence(split.clone()))
            .await?;
        let parent = self.group(split.parent.group)?.current().await?;
        let Machine::Range { data, .. } = parent.machine else {
            return Err("parent missing".into());
        };
        let children = data.children()?;
        let mut proofs = Vec::new();
        for child in children {
            let expected = child.provenance.clone().ok_or("missing child provenance")?;
            self.apply(child.descriptor.group, Action::Bootstrap(Box::new(child)))
                .await?;
            let observed = self.group(expected.child.group)?.current().await?;
            let Machine::Range { data, .. } = observed.machine else {
                return Err("child not ready".into());
            };
            if data.provenance.as_ref() != Some(&expected)
                || data.fence.is_some()
                || digest(&(&data.image, &data.retries))? != expected.digest
            {
                return Err("child changed before publication".into());
            }
            proofs.push(expected);
        }
        let ready: [Ready; 2] = proofs.try_into().map_err(|_| "child count")?;
        self.apply(
            DIRECTORY,
            Action::Publish {
                id,
                ready: Box::new(ready),
            },
        )
        .await?;
        let published = self.directory().await?;
        for proof in published.intents[&id]
            .children
            .as_ref()
            .ok_or("missing proofs")?
        {
            self.apply(
                proof.child.group,
                Action::Activate {
                    proof: proof.clone(),
                    revision: published.revision,
                },
            )
            .await?;
        }
        Ok(published)
    }
    pub async fn put(
        &self,
        key: Key,
        operation: u64,
        expected: Option<u64>,
        row: giggles_core::Row,
    ) -> Result<Observation> {
        let d = self.directory().await?;
        let route = d.route(key)?;
        self.apply(
            route.group,
            Action::Put {
                route,
                operation,
                key,
                expected,
                row,
            },
        )
        .await
    }
    /// Bounded ownership-coherent observation, NOT a strict multi-range SQL
    /// transaction: concurrent data writes require next scope's predicate locks.
    pub async fn observe(&self, bounds: Bounds) -> Result<Vec<(Key, giggles_core::Row)>> {
        self.observe_interleaved(bounds, None).await
    }
    /// Trusted deterministic scheduler seam: finish a pending lifecycle after
    /// collecting rows and before the mandatory current-directory recheck.
    pub async fn observe_interleaved(
        &self,
        bounds: Bounds,
        resume: Option<u64>,
    ) -> Result<Vec<(Key, giggles_core::Row)>> {
        if !bounds.valid() {
            return Err("invalid predicate".into());
        }
        let before = self.directory().await?;
        let mut rows = Vec::new();
        for route in &before.routes {
            if route.bounds.end.is_some_and(|end| end <= bounds.start)
                || bounds.end.is_some_and(|end| end <= route.bounds.start)
            {
                continue;
            }
            let o = self.group(route.group)?.current().await?;
            let Machine::Range { data, .. } = o.machine else {
                return Err("range missing; retry whole observation".into());
            };
            data.check(route, &route.bounds)?;
            for (key, row) in data.image.snapshot()?.rows {
                if bounds.contains(key) {
                    rows.push((key, row));
                    if rows.len() > LIMIT {
                        return Err("observation limit".into());
                    }
                }
            }
        }
        if let Some(id) = resume {
            self.resume(id).await?;
        }
        if self.directory().await?.revision != before.revision {
            return Err("ownership changed; retry entire observation".into());
        }
        Ok(rows)
    }
}
