//! PostgreSQL advisory locks, isolated by database and key space.
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::{Condvar, Mutex, atomic::Ordering},
    time::Duration,
};

type LockId = (String, bool, i64);

#[derive(Default)]
pub(crate) struct Locks(Mutex<BTreeMap<LockId, Entry>>, Condvar);

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Mode {
    Shared,
    Exclusive,
}

impl Mode {
    fn conflicts(self, other: Self) -> bool {
        self == Self::Exclusive || other == Self::Exclusive
    }

    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Shared => "ShareLock",
            Self::Exclusive => "ExclusiveLock",
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) enum Action {
    Acquire {
        mode: Mode,
        transaction: bool,
        try_only: bool,
    },
    Unlock(Mode),
    UnlockAll,
}

#[derive(Default)]
struct Hold {
    session: usize,
    transaction: bool,
}

#[derive(Clone, Copy)]
struct Waiter {
    pid: i32,
    mode: Mode,
}

#[derive(Default)]
struct Entry {
    holds: BTreeMap<(i32, Mode), Hold>,
    queue: VecDeque<Waiter>,
}

impl Entry {
    fn retain(&mut self) -> bool {
        self.holds
            .retain(|_, hold| hold.session != 0 || hold.transaction);
        !self.holds.is_empty() || !self.queue.is_empty()
    }

    fn blockers(&self, request: Waiter) -> impl Iterator<Item = i32> + '_ {
        let holders = self.holds.keys().filter_map(move |&(pid, mode)| {
            (pid != request.pid && request.mode.conflicts(mode)).then_some(pid)
        });
        // Reentrant acquisitions bypass the queue. A backend changing modes
        // can also pass a waiter that is already blocked by its existing hold.
        let prior_waiters = self
            .queue
            .iter()
            .take_while(move |w| w.pid != request.pid)
            .filter_map(move |w| {
                let blocked_by_requester =
                    [Mode::Shared, Mode::Exclusive].into_iter().any(|held| {
                        w.mode.conflicts(held) && self.holds.contains_key(&(request.pid, held))
                    });
                (request.mode.conflicts(w.mode) && !blocked_by_requester).then_some(w.pid)
            });
        holders.chain(prior_waiters)
    }
}

impl Locks {
    pub(crate) fn release(&self, pid: i32, transaction: bool) {
        self.0.lock().expect("advisory locks").retain(|_, entry| {
            for ((owner, _), hold) in &mut entry.holds {
                if *owner == pid {
                    hold.transaction = false;
                    if !transaction {
                        hold.session = 0;
                    }
                }
            }
            if !transaction {
                entry.queue.retain(|waiter| waiter.pid != pid);
            }
            entry.retain()
        });
        self.1.notify_all();
    }

    pub(crate) fn unlock_all(&self, pid: i32) -> Result<(), &'static str> {
        self.0.lock().map_err(|_| "XX000")?.retain(|_, entry| {
            for ((owner, _), hold) in &mut entry.holds {
                if *owner == pid {
                    hold.session = 0;
                }
            }
            entry.retain()
        });
        self.1.notify_all();
        Ok(())
    }

    pub(crate) fn call(
        &self,
        session: &crate::Session,
        action: Action,
        key: (bool, i64),
    ) -> Result<bool, &'static str> {
        let pid = session.backend_pid;
        let id = (session.database.clone().unwrap_or_default(), key.0, key.1);
        let mut locks = self.0.lock().map_err(|_| "XX000")?;
        let (request, transaction, try_only) = match action {
            Action::UnlockAll => {
                drop(locks);
                self.unlock_all(pid)?;
                return Ok(true);
            }
            Action::Unlock(mode) => {
                let success = locks
                    .get_mut(&id)
                    .and_then(|entry| entry.holds.get_mut(&(pid, mode)))
                    .is_some_and(|hold| {
                        if hold.session == 0 {
                            return false;
                        }
                        hold.session -= 1;
                        true
                    });
                locks.retain(|_, entry| entry.retain());
                self.1.notify_all();
                return Ok(success);
            }
            Action::Acquire {
                mode,
                transaction,
                try_only,
            } => (Waiter { pid, mode }, transaction, try_only),
        };
        let result = loop {
            if session.connection_closed.load(Ordering::Acquire)
                || giggles_core::cancellation::check().is_err()
            {
                break Err("57014");
            }
            let entry = locks.entry(id.clone()).or_default();
            if entry.holds.contains_key(&(pid, request.mode))
                || entry.blockers(request).next().is_none()
            {
                let hold = entry.holds.entry((pid, request.mode)).or_default();
                if transaction {
                    hold.transaction = true;
                } else {
                    hold.session += 1;
                }
                break Ok(true);
            }
            if try_only {
                break Ok(false);
            }
            if !entry.queue.iter().any(|w| w.pid == pid) {
                entry.queue.push_back(request);
            }
            if deadlocked(&locks, pid) {
                break Err("40P01");
            }
            locks = self
                .1
                .wait_timeout(locks, Duration::from_millis(10))
                .map_err(|_| "XX000")?
                .0;
        };
        if let Some(entry) = locks.get_mut(&id) {
            entry.queue.retain(|w| w.pid != pid);
        }
        locks.retain(|_, entry| entry.retain());
        self.1.notify_all();
        result
    }
}

fn deadlocked(locks: &BTreeMap<LockId, Entry>, pid: i32) -> bool {
    let mut seen = BTreeSet::new();
    let mut pending = vec![pid];
    while let Some(next) = pending.pop() {
        if !seen.insert(next) {
            continue;
        }
        if let Some((entry, waiter)) = locks.values().find_map(|entry| {
            entry
                .queue
                .iter()
                .find(|w| w.pid == next)
                .map(|w| (entry, *w))
        }) {
            for blocker in entry.blockers(waiter) {
                if blocker == pid {
                    return true;
                }
                pending.push(blocker);
            }
        }
    }
    false
}
