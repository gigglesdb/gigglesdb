//! Exclusive advisory locks, isolated by database and the two PostgreSQL key spaces.
use std::{collections::{BTreeMap, VecDeque}, sync::{Condvar, Mutex}, time::Duration};
#[derive(Default)]
pub(crate) struct Locks(Mutex<BTreeMap<(String, bool, i64), Entry>>, Condvar);
#[derive(Default)]
struct Entry { owner: i32, session: usize, transaction: bool, queue: VecDeque<i32> }
impl Entry { fn held(&self) -> bool { self.session > 0 || self.transaction } }
impl Locks {
    pub(crate) fn release(&self, pid: i32, transaction: bool) {
        self.0.lock().expect("advisory locks").retain(|_, e| {
            if e.owner == pid { e.transaction = false; if !transaction { e.session = 0; } }
            if !transaction { e.queue.retain(|p| *p != pid); }
            e.held() || !e.queue.is_empty()
        });
        self.1.notify_all();
    }
    pub(crate) fn call(&self, session: &crate::Session, name: &str, key: (bool, i64)) -> Result<bool, &'static str> {
        let pid = session.backend_pid;
        if name == "pg_advisory_unlock_all" {
            self.0.lock().map_err(|_| "XX000")?.retain(|_, e| {
                if e.owner == pid { e.session = 0; } e.held() || !e.queue.is_empty()
            });
            self.1.notify_all(); return Ok(true);
        }
        let id = (session.database.clone().unwrap_or_default(), key.0, key.1);
        let mut locks = self.0.lock().map_err(|_| "XX000")?;
        if name == "pg_advisory_unlock" {
            let success = locks.get_mut(&id).is_some_and(|e| {
                if e.owner != pid || e.session == 0 { return false; }
                e.session -= 1; true
            });
            locks.retain(|_, e| e.held() || !e.queue.is_empty());
            self.1.notify_all();
            return Ok(success);
        }
        let trying = name.starts_with("pg_try_");
        let result = loop {
            if session.connection_closed.load(std::sync::atomic::Ordering::Acquire) || giggles_core::cancellation::check().is_err() { break Err("57014"); }
            let e = locks.entry(id.clone()).or_default();
            if (e.held() && e.owner == pid) || (!e.held() && e.queue.front().is_none_or(|p| *p == pid)) {
                e.owner = pid;
                if name.contains("xact") { e.transaction = true; } else { e.session += 1; }
                break Ok(true);
            }
            if trying { break Ok(false); }
            if !e.queue.contains(&pid) { e.queue.push_back(pid); }
            // Follow exclusive owners and FIFO predecessors; reject a wait cycle.
            let mut next = pid;
            let mut seen = std::collections::BTreeSet::new();
            while seen.insert(next) {
                let blocker = locks.values().find_map(|e| {
                    let at = e.queue.iter().position(|p| *p == next)?;
                    if at > 0 { Some(e.queue[at - 1]) } else if e.held() { Some(e.owner) } else { None }
                });
                let Some(blocker) = blocker else { break; }; next = blocker;
            }
            if next == pid { break Err("40P01"); }
            locks = self.1.wait_timeout(locks, Duration::from_millis(10)).map_err(|_| "XX000")?.0;
        };
        if let Some(e) = locks.get_mut(&id) { e.queue.retain(|p| *p != pid); }
        locks.retain(|_, e| e.held() || !e.queue.is_empty());
        self.1.notify_all();
        result
    }
}
