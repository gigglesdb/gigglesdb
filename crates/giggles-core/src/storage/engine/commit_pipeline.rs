//! Ordered append, durability and replies for a marked local store.
use super::*;
use std::{
    cell::Cell,
    sync::{MutexGuard, Once, PoisonError},
};

const MAX_GROUPS: usize = 8;
const MAX_BYTES: usize = 4 * 1024 * 1024;

thread_local! {
    static PIPELINE_WORKER: Cell<bool> = const { Cell::new(false) };
}

fn install_panic_hook() {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            previous(info);
            if PIPELINE_WORKER.get() {
                std::process::abort();
            }
        }));
    });
}

fn abort_on_poison<T>(_: PoisonError<T>) -> T {
    eprintln!("commit pipeline state poisoned");
    std::process::abort()
}

pub(super) fn read_group_number(kv: &Kv) -> Result<u64, KvError> {
    let Some(bytes) = kv.get(Keyspace::Meta, super::super::kv::COMMIT_GROUP_KEY)? else {
        return Ok(0);
    };
    bytes
        .try_into()
        .map(u64::from_be_bytes)
        .map_err(|_| KvError::Corrupt("invalid commit_group".into()))
}

pub(super) struct GroupNumber(u64);

impl GroupNumber {
    pub(super) fn stamp(&mut self, batch: &mut WriteBatch) -> Result<(), TxnError> {
        debug_assert!(!batch.is_empty());
        let next = self.0.checked_add(1).ok_or(TxnError::WriteSetLimit)?;
        batch.put_commit_group(next);
        self.0 = next;
        Ok(())
    }
}

pub(super) enum Reply {
    Commit(mpsc::Sender<Result<Ts, TxnError>>, Result<Ts, TxnError>),
    Unit(CounterReply, Result<(), TxnError>),
    Fence(
        mpsc::Sender<Result<Option<WriteFenceRecord>, TxnError>>,
        Result<Option<WriteFenceRecord>, TxnError>,
    ),
    Snapshot(
        mpsc::Sender<Result<PhysicalSnapshot, TxnError>>,
        Result<PhysicalSnapshot, TxnError>,
    ),
}

impl Reply {
    pub(super) fn rejected(message: Message, error: TxnError) -> Self {
        match message {
            Message::Commit(pending) => Self::Commit(pending.reply, Err(error)),
            Message::Counter { reply, .. } | Message::Highwater { reply, .. } => {
                Self::Unit(reply, Err(error))
            }
            Message::Fence { reply, .. } => Self::Fence(reply, Err(error)),
            Message::DurableSnapshot { reply, .. } => Self::Snapshot(reply, Err(error)),
            Message::Wake | Message::Shutdown => unreachable!("control message without reply"),
        }
    }

    fn send(self, override_error: Option<&TxnError>) {
        fn send<T>(
            reply: mpsc::Sender<Result<T, TxnError>>,
            result: Result<T, TxnError>,
            override_error: Option<&TxnError>,
        ) {
            let _ = reply.send(override_error.map_or(result, |error| Err(error.clone())));
        }
        match self {
            Self::Commit(reply, result) => send(reply, result, override_error),
            Self::Unit(reply, result) => { let _ = reply.send(override_error.map_or(result, |error| Err(error.clone()))); },
            Self::Fence(reply, result) => send(reply, result, override_error),
            Self::Snapshot(reply, result) => send(reply, result, override_error),
        }
    }
}

#[derive(Default)]
enum Completion {
    #[default]
    Empty,
    Appended(Option<Ts>),
    Synced(Option<Ts>),
    Failed(TxnError),
}

#[derive(Default)]
struct Item {
    bytes: usize,
    completion: Completion,
    accepted: Vec<Reply>,
    rejected: Vec<Reply>,
}

impl Item {
    fn max(&self) -> Option<Ts> {
        match self.completion {
            Completion::Appended(ts) | Completion::Synced(ts) => ts,
            _ => None,
        }
    }

    fn resolve(self, failed_barrier: bool, earlier_ambiguous: &mut bool) {
        let error = match self.completion {
            Completion::Failed(error) => Some(error),
            Completion::Appended(_) | Completion::Empty => {
                (failed_barrier || *earlier_ambiguous).then_some(TxnError::CommitAmbiguous)
            }
            Completion::Synced(_) => None,
        };
        *earlier_ambiguous |= error == Some(TxnError::CommitAmbiguous);
        let rejection = error.as_ref().map(|_| &TxnError::Fenced);
        for reply in self.rejected {
            reply.send(rejection);
        }
        for reply in self.accepted {
            reply.send(error.as_ref());
        }
    }
}

#[derive(Default)]
struct PipeState {
    queue: VecDeque<Item>,
    terminal: bool,
    appender_active: bool,
    // Includes reservations and detached cohorts through their last reply.
    groups: usize,
    bytes: usize,
    // Stage A's Stopped marker follows the last enqueued item.
    stopped: bool,
}

impl PipeState {
    fn fits(&self, bytes: usize) -> bool {
        self.groups < MAX_GROUPS
            && self
                .bytes
                .checked_add(bytes)
                .is_some_and(|n| n <= MAX_BYTES)
    }
}

pub(super) struct Pipe {
    state: Mutex<PipeState>,
    changed: Condvar,
    trim_bound: AtomicU64,
}

impl Pipe {
    fn lock(&self) -> MutexGuard<'_, PipeState> {
        self.state.lock().unwrap_or_else(abort_on_poison)
    }

    fn wait_while(
        &self,
        predicate: impl FnMut(&mut PipeState) -> bool,
    ) -> MutexGuard<'_, PipeState> {
        self.changed
            .wait_while(self.lock(), predicate)
            .unwrap_or_else(abort_on_poison)
    }

    fn reserve(&self, bytes: usize, exit_on_terminal: bool) -> bool {
        let mut state =
            self.wait_while(|state| !(state.fits(bytes) || exit_on_terminal && state.terminal));
        if exit_on_terminal && state.terminal {
            return false;
        }
        state.groups += 1;
        state.bytes += bytes;
        true
    }

    fn start_append(&self) -> bool {
        let mut state = self.lock();
        if state.terminal {
            return false;
        }
        debug_assert!(!state.appender_active);
        state.appender_active = true;
        true
    }

    fn enqueue(&self, item: Item, reserved: bool, terminal: bool) {
        if !reserved {
            self.reserve(item.bytes, false);
        }
        let mut state = self.lock();
        state.queue.push_back(item);
        state.appender_active = false;
        state.terminal |= terminal;
        self.changed.notify_all();
    }

    fn terminal(&self) -> bool {
        self.lock().terminal
    }

    fn wait_drained(&self) -> bool {
        // A is the only producer. Zero reservations here is equivalent to
        // every item preceding this barrier having sent its replies.
        !self
            .wait_while(|state| state.groups != 0 && !state.terminal)
            .terminal
    }

    fn stop(&self) {
        // Resolve the tail even on terminal shutdown. Once the receiver goes
        // away, a late producer can observe the closed channel.
        let mut state = self.wait_while(|state| state.groups != 0);
        debug_assert!(!state.appender_active);
        state.stopped = true;
        self.changed.notify_all();
    }
}

pub(super) fn start_syncer(
    kv: Arc<Kv>,
    shared: Arc<Shared>,
    intake: mpsc::Sender<Message>,
    seed: Ts,
) -> (Arc<Pipe>, JoinHandle<()>) {
    install_panic_hook();
    let pipe = Arc::new(Pipe {
        state: Mutex::default(),
        changed: Condvar::new(),
        trim_bound: AtomicU64::new(seed.0),
    });
    let worker_pipe = pipe.clone();
    let worker = thread::spawn(move || {
        PIPELINE_WORKER.set(true);
        syncer(&worker_pipe, &kv, &shared, &intake);
    });
    (pipe, worker)
}

fn syncer(pipe: &Pipe, kv: &Kv, shared: &Shared, intake: &mpsc::Sender<Message>) {
    let mut earlier_ambiguous = false;
    loop {
        let (cohort, terminal) = {
            let mut state = pipe.wait_while(|state| state.queue.is_empty() && !state.stopped);
            if state.stopped && state.queue.is_empty() {
                return;
            }
            (std::mem::take(&mut state.queue), state.terminal)
        };
        let appended = cohort
            .iter()
            .any(|item| matches!(item.completion, Completion::Appended(_)));
        let failed = appended && (terminal || sync_cohort(kv, shared).is_err());
        if failed {
            {
                let mut state = pipe.lock();
                state.terminal = true;
                pipe.changed.notify_all();
            }
            kv.fence();
            publish_sequencer_fence(shared, kv);
            let _ = intake.send(Message::Wake);
        } else if let Some(max) = cohort.iter().filter_map(Item::max).max() {
            let trim_bound = publish_visible::<true>(shared, max);
            pipe.trim_bound.store(trim_bound.0, Ordering::Release);
        }
        for item in cohort {
            let bytes = item.bytes;
            item.resolve(failed, &mut earlier_ambiguous);
            let mut state = pipe.lock();
            state.groups -= 1;
            state.bytes -= bytes;
            pipe.changed.notify_all();
        }
    }
}

fn sync_cohort(kv: &Kv, _shared: &Shared) -> Result<(), KvError> {
    #[cfg(all(test, feature = "engine-rocksdb"))]
    _shared.pipeline_barriers.fetch_add(1, Ordering::Relaxed);
    #[cfg(any(test, feature = "test-commit-sync"))]
    let hook = _shared.sync_hook.lock().unwrap().clone();
    #[cfg(any(test, feature = "test-commit-sync"))]
    match hook.as_ref().map(|hook| hook.before_sync()) {
        Some(crate::commit_sync_hook::SyncOutcome::Fail) => {
            return Err(KvError::Io("injected commit barrier error".into()));
        }
        #[cfg(all(test, feature = "engine-rocksdb"))]
        Some(crate::commit_sync_hook::SyncOutcome::WalError { after_apply }) => {
            if after_apply {
                kv.sync_commits()?;
            }
            return kv.test_commit_wal_error(super::super::kv::FenceSite::CommitSync);
        }
        _ => {}
    }
    kv.sync_commits()?;
    #[cfg(test)]
    if let Some(hook) = hook {
        hook.after_sync();
    }
    Ok(())
}

fn append(kv: &Kv, _shared: &Shared, batch: WriteBatch) -> Result<(), KvError> {
    #[cfg(any(test, feature = "test-commit-sync"))]
    let hook = _shared.sync_hook.lock().unwrap().clone();
    #[cfg(any(test, feature = "test-commit-sync"))]
    match hook.as_ref().map(|hook| hook.on_before_append()) {
        Some(crate::commit_sync_hook::SyncOutcome::Fail) => {
            return Err(KvError::Io("injected commit append error".into()));
        }
        #[cfg(all(test, feature = "engine-rocksdb"))]
        Some(crate::commit_sync_hook::SyncOutcome::WalError { after_apply }) => {
            if after_apply {
                kv.append_commit(batch)?;
            }
            return kv.test_commit_wal_error(super::super::kv::FenceSite::CommitAppend);
        }
        _ => {}
    }
    kv.append_commit(batch)?;
    #[cfg(any(test, feature = "test-commit-sync"))]
    if let Some(hook) = hook {
        hook.on_after_append();
    }
    Ok(())
}

struct Watermarks {
    mvcc_max: Ts,
    physical_floor: Ts,
}

pub(super) struct Group<'a> {
    pipe: &'a Pipe,
    number: &'a mut GroupNumber,
    watermarks: &'a mut Watermarks,
    item: Item,
    reserved: bool,
    barrier: bool,
}

impl Group<'_> {
    pub(super) fn mvcc_max(&self) -> Ts {
        self.watermarks.mvcc_max
    }
    pub(super) fn physical_floor(&self) -> Ts {
        self.watermarks.physical_floor
    }

    pub(super) fn reject(&mut self, reply: Reply) {
        self.item.rejected.push(reply);
    }

    pub(super) fn refused(&mut self) {
        self.item.completion = Completion::Failed(TxnError::Fenced);
    }

    pub(super) fn admit(&mut self, batch: &WriteBatch) -> bool {
        let bytes = if batch.is_empty() {
            0
        } else {
            // Include the chain stamp and a conservative allowance for
            // RocksDB's batch and operation headers.
            batch.byte_size()
                + super::super::kv::COMMIT_GROUP_KEY.len()
                + 8
                + 12
                + (batch.len() + 1) * 16
        };
        self.barrier |= bytes > MAX_BYTES;
        if self.barrier && !self.pipe.wait_drained() {
            return false;
        }
        self.item.bytes = if self.barrier { 0 } else { bytes };
        self.reserved = self.pipe.reserve(self.item.bytes, true);
        if !self.reserved {
            self.item.bytes = 0;
        }
        self.reserved
    }

    #[allow(clippy::type_complexity)]
    pub(super) fn dispatch(
        &mut self,
        mut batch: WriteBatch,
        max: Ts,
        commits: Vec<(mpsc::Sender<Result<Ts, TxnError>>, Ts)>,
        counters: Vec<CounterReply>,
        kv: &Kv,
        shared: &Shared,
    ) -> bool {
        self.item
            .accepted
            .extend(counters.into_iter().map(|reply| Reply::Unit(reply, Ok(()))));
        self.item.accepted.extend(
            commits
                .into_iter()
                .map(|(reply, ts)| Reply::Commit(reply, Ok(ts))),
        );
        if batch.is_empty() {
            return false;
        }

        let result = if !self.barrier && !self.pipe.start_append() {
            Err(TxnError::Fenced)
        } else if let Err(error) = self.number.stamp(&mut batch) {
            Err(error)
        } else {
            let written = if self.barrier {
                sync_batch(kv, shared, batch)
            } else {
                append(kv, shared, batch)
            };
            written.map_err(|error| write_error(&error))
        };
        match result {
            Ok(()) => {
                let advanced = (max > self.watermarks.mvcc_max).then_some(max);
                if let Some(ts) = advanced {
                    self.watermarks.mvcc_max = ts;
                    self.watermarks.physical_floor = ts;
                }
                self.item.completion = if self.barrier {
                    Completion::Synced(advanced)
                } else {
                    Completion::Appended(advanced)
                };
                false
            }
            Err(error) => {
                self.item.completion = Completion::Failed(error);
                true
            }
        }
    }

    fn finish(self, terminal: bool) {
        let pipe = self.pipe;
        pipe.enqueue(self.item, self.reserved, terminal);
        if self.barrier {
            pipe.wait_drained();
        }
    }
}

fn submit_barrier(
    pipe: &Pipe,
    kv: &Kv,
    shared: &Shared,
    message: Message,
    execute: impl FnOnce(Message) -> (Reply, bool),
) {
    if pipe.wait_drained() && !kv.is_fenced() && !shared.stopped.load(Ordering::Acquire) {
        let (reply, failed) = execute(message);
        if failed {
            kv.fence();
            publish_sequencer_fence(shared, kv);
        }
        pipe.enqueue(
            Item {
                completion: Completion::Synced(None),
                accepted: vec![reply],
                ..Item::default()
            },
            false,
            failed,
        );
        pipe.wait_drained();
    } else {
        pipe.enqueue(
            Item {
                rejected: vec![Reply::rejected(message, TxnError::Fenced)],
                ..Item::default()
            },
            false,
            false,
        );
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn run(
    receiver: mpsc::Receiver<Message>,
    kv: Arc<Kv>,
    clock: Arc<dyn Clock>,
    config: SequencerConfig,
    shared: Arc<Shared>,
    served: Arc<ServedGate>,
    seed: Ts,
    mut features: u64,
    mut subversion: u32,
    mut fence: FenceAccess,
    pipe: Arc<Pipe>,
    group_number: u64,
) {
    PIPELINE_WORKER.set(true);
    let mut number = GroupNumber(group_number);
    let mut watermarks = Watermarks {
        mvcc_max: seed,
        physical_floor: seed,
    };
    let mut hlc = HlcClock::new(seed);
    let mut log = RecentLog::new(config.recent_log_bytes);
    let mut deferred = None;
    while !pipe.terminal() {
        let Some(message) = deferred.take().or_else(|| receiver.recv().ok()) else {
            break;
        };
        let first = match message {
            message @ (Message::Fence { .. } | Message::DurableSnapshot { .. }) => {
                submit_barrier(&pipe, &kv, &shared, message, |message| match message {
                    Message::Fence { action, reply } => {
                        let result = process_fence::<true>(
                            &kv,
                            action,
                            &mut fence,
                            &mut features,
                            &mut hlc,
                            clock.as_ref(),
                            Some(&mut number),
                        );
                        watermarks.physical_floor = hlc.last;
                        let failed =
                            matches!(result, Err(TxnError::CommitAmbiguous | TxnError::Fenced));
                        (Reply::Fence(reply, result), failed)
                    }
                    Message::DurableSnapshot { pin, reply } => {
                        let result = kv
                            .snapshot()
                            .map(|read| (pin, read))
                            .map_err(TxnError::from);
                        (Reply::Snapshot(reply, result), false)
                    }
                    _ => unreachable!("not a barrier"),
                });
                continue;
            }
            Message::Shutdown => break,
            Message::Wake => continue,
            item => item,
        };
        let (group, next, shutdown) = gather(first, &receiver, &config);
        deferred = next;
        let guarded = matches!(&group[0], Message::Commit(Pending { guard: Some(_), .. }));
        let mut output = Group {
            pipe: &pipe,
            number: &mut number,
            watermarks: &mut watermarks,
            item: Item::default(),
            reserved: false,
            barrier: guarded,
        };
        let failed = if shutdown
            || kv.is_fenced()
            || shared.stopped.load(Ordering::Acquire)
            || guarded && !pipe.wait_drained()
        {
            for message in group {
                output.reject(Reply::rejected(message, TxnError::Fenced));
            }
            false
        } else {
            log.trim(Some(Ts(pipe.trim_bound.load(Ordering::Acquire))));
            process_group::<true>(
                group,
                &kv,
                clock.as_ref(),
                &shared,
                &mut hlc,
                &mut log,
                &mut features,
                &mut subversion,
                &config.ts_source,
                &served,
                &fence,
                Some(&mut output),
            )
        };
        let terminal = failed
            && (!shared.stopped.load(Ordering::Acquire)
                || kv.is_fenced()
                || matches!(&output.item.completion, Completion::Failed(error) if *error != TxnError::Fenced));
        if terminal {
            kv.fence();
            publish_sequencer_fence(&shared, &kv);
        }
        output.finish(terminal);
        if shutdown {
            break;
        }
    }
    for message in deferred.into_iter().chain(receiver.try_iter()) {
        if !matches!(message, Message::Wake | Message::Shutdown) {
            pipe.enqueue(
                Item {
                    rejected: vec![Reply::rejected(message, TxnError::Fenced)],
                    ..Item::default()
                },
                false,
                false,
            );
        }
    }
    pipe.stop();
}

#[cfg(all(test, feature = "engine-rocksdb"))]
#[path = "commit_pipeline_tests.rs"]
mod tests;
