//! Open connection tracking and cancel-safe end signals.

use std::collections::{BTreeSet, HashMap};
use std::hash::Hash;
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::SystemTime;
use tokio::sync::watch;
use tokio::time::Instant;

/// A unique, monotonically increasing connection identifier.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnectionId(u64);

impl ConnectionId {
    /// Return the numeric identifier.
    pub fn get(self) -> u64 {
        self.0
    }
}

/// The first reason a connection must end.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EndReason {
    /// The connection's authority was revoked.
    Revoked,
    /// The connection's deadline elapsed.
    Expired,
}

#[derive(Clone, Copy)]
struct SignalState {
    ended: Option<EndReason>,
    deadline: Option<Instant>,
}

fn expire(state: &mut SignalState) -> bool {
    if state.ended.is_none() && state.deadline.is_some_and(|d| d <= Instant::now()) {
        state.ended = Some(EndReason::Expired);
        true
    } else {
        false
    }
}

fn current(sender: &watch::Sender<SignalState>) -> SignalState {
    sender.send_if_modified(expire);
    *sender.borrow()
}

/// A cancel-safe receiver for a connection's persistent end reason.
pub struct EndSignal {
    sender: watch::Sender<SignalState>,
    receiver: watch::Receiver<SignalState>,
}

impl EndSignal {
    /// Wait for the first end reason; registry removal alone leaves this pending until its deadline, or forever without one.
    pub async fn ended(&mut self) -> EndReason {
        loop {
            let state = current(&self.sender);
            if let Some(reason) = state.ended {
                return reason;
            }
            match state.deadline {
                Some(deadline) => {
                    tokio::select! {
                        _ = tokio::time::sleep_until(deadline) => {},
                        _ = self.receiver.changed() => {},
                    }
                }
                None => {
                    // This signal retains a sender, so removal cannot close the channel.
                    let _ = self.receiver.changed().await;
                }
            }
        }
    }

    /// Return the first end reason, including an elapsed deadline, without waiting.
    pub fn try_ended(&self) -> Option<EndReason> {
        current(&self.sender).ended
    }
}

/// The ownership and end state of an open connection.
#[derive(Clone, Debug)]
pub struct ConnectionSnapshot<S, P> {
    /// The connection's unique identifier.
    pub id: ConnectionId,
    /// The access session owning the connection.
    pub session: S,
    /// The principal owning the connection.
    pub principal: P,
    /// The connection's route.
    pub route: String,
    /// The wall-clock time of registration.
    pub opened_at: SystemTime,
    /// The connection's first end reason, if any.
    pub ended: Option<EndReason>,
}

struct Entry<S, P> {
    snapshot: ConnectionSnapshot<S, P>,
    sender: watch::Sender<SignalState>,
}

struct Inner<S, P> {
    next_id: u64,
    entries: HashMap<ConnectionId, Entry<S, P>>,
    sessions: HashMap<S, BTreeSet<ConnectionId>>,
    principals: HashMap<P, BTreeSet<ConnectionId>>,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poison| poison.into_inner())
}

/// Open connections indexed by session and principal, shared across clones.
pub struct ConnectionRegistry<S, P> {
    inner: Arc<Mutex<Inner<S, P>>>,
}

impl<S, P> Clone for ConnectionRegistry<S, P> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

/// A registration that disappears automatically when this guard is dropped.
pub struct ConnectionGuard<S: Eq + Hash, P: Eq + Hash> {
    id: ConnectionId,
    inner: Weak<Mutex<Inner<S, P>>>,
}

impl<S: Eq + Hash, P: Eq + Hash> ConnectionGuard<S, P> {
    /// Return this registration's identifier.
    pub fn id(&self) -> ConnectionId {
        self.id
    }
}

impl<S: Eq + Hash, P: Eq + Hash> Drop for ConnectionGuard<S, P> {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.upgrade() {
            let mut inner = lock(&inner);
            if let Some(entry) = inner.entries.remove(&self.id) {
                remove_index(&mut inner.sessions, &entry.snapshot.session, self.id);
                remove_index(&mut inner.principals, &entry.snapshot.principal, self.id);
            }
        }
    }
}

fn remove_index<K: Eq + Hash>(
    index: &mut HashMap<K, BTreeSet<ConnectionId>>,
    key: &K,
    id: ConnectionId,
) {
    if let Some(ids) = index.get_mut(key) {
        ids.remove(&id);
        if ids.is_empty() {
            index.remove(key);
        }
    }
}

impl<S, P> Default for ConnectionRegistry<S, P>
where
    S: Clone + Eq + Hash + Send + 'static,
    P: Clone + Eq + Hash + Send + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<S, P> ConnectionRegistry<S, P>
where
    S: Clone + Eq + Hash + Send + 'static,
    P: Clone + Eq + Hash + Send + 'static,
{
    /// Create an empty registry whose identifiers start at one.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                next_id: 1,
                entries: HashMap::new(),
                sessions: HashMap::new(),
                principals: HashMap::new(),
            })),
        }
    }

    /// Register an open connection and return its lifetime guard and end receiver.
    pub fn register(
        &self,
        session: S,
        principal: P,
        route: impl Into<String>,
        deadline: Option<Instant>,
    ) -> (ConnectionGuard<S, P>, EndSignal) {
        let route = route.into();
        let mut inner = lock(&self.inner);
        let id = ConnectionId(inner.next_id);
        // Exhaustion must stop allocation rather than ever reuse an identifier.
        inner.next_id = inner
            .next_id
            .checked_add(1)
            .expect("connection identifier space exhausted");
        let (sender, receiver) = watch::channel(SignalState {
            ended: None,
            deadline,
        });
        inner
            .sessions
            .entry(session.clone())
            .or_default()
            .insert(id);
        inner
            .principals
            .entry(principal.clone())
            .or_default()
            .insert(id);
        inner.entries.insert(
            id,
            Entry {
                snapshot: ConnectionSnapshot {
                    id,
                    session,
                    principal,
                    route,
                    opened_at: SystemTime::now(),
                    ended: None,
                },
                sender: sender.clone(),
            },
        );
        (
            ConnectionGuard {
                id,
                inner: Arc::downgrade(&self.inner),
            },
            EndSignal { sender, receiver },
        )
    }

    /// Signal every connection of a session, returning the number newly signalled.
    pub fn signal_session(&self, session: &S, reason: EndReason) -> usize {
        let inner = lock(&self.inner);
        signal(&inner, inner.sessions.get(session), reason)
    }

    /// Signal every connection of a principal, returning the number newly signalled.
    pub fn signal_principal(&self, principal: &P, reason: EndReason) -> usize {
        let inner = lock(&self.inner);
        signal(&inner, inner.principals.get(principal), reason)
    }

    /// Update every registered connection's session deadline and return the count.
    pub fn set_session_deadline(&self, session: &S, deadline: Option<Instant>) -> usize {
        let inner = lock(&self.inner);
        let Some(ids) = inner.sessions.get(session) else {
            return 0;
        };
        for id in ids {
            if let Some(entry) = inner.entries.get(id) {
                entry.sender.send_modify(|state| {
                    expire(state);
                    state.deadline = deadline;
                });
            }
        }
        ids.len()
    }

    /// List a session's connections sorted by identifier.
    pub fn list_session(&self, session: &S) -> Vec<ConnectionSnapshot<S, P>> {
        let inner = lock(&self.inner);
        snapshots(
            &inner,
            inner.sessions.get(session).into_iter().flatten().copied(),
        )
    }

    /// List a principal's connections sorted by identifier.
    pub fn list_principal(&self, principal: &P) -> Vec<ConnectionSnapshot<S, P>> {
        let inner = lock(&self.inner);
        snapshots(
            &inner,
            inner
                .principals
                .get(principal)
                .into_iter()
                .flatten()
                .copied(),
        )
    }

    /// List all connections sorted by identifier.
    pub fn list_all(&self) -> Vec<ConnectionSnapshot<S, P>> {
        let inner = lock(&self.inner);
        snapshots(&inner, inner.entries.keys().copied())
    }

    /// Return the number of registered connections, including signalled connections.
    pub fn len(&self) -> usize {
        lock(&self.inner).entries.len()
    }

    /// Return whether there are no registered connections.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

fn signal<S, P>(
    inner: &Inner<S, P>,
    ids: Option<&BTreeSet<ConnectionId>>,
    reason: EndReason,
) -> usize {
    let mut count = 0;
    for id in ids.into_iter().flatten() {
        if let Some(entry) = inner.entries.get(id) {
            entry.sender.send_if_modified(|state| {
                let expired = expire(state);
                if state.ended.is_none() {
                    state.ended = Some(reason);
                    count += 1;
                    true
                } else {
                    expired
                }
            });
        }
    }
    count
}

fn snapshots<S: Clone, P: Clone>(
    inner: &Inner<S, P>,
    ids: impl Iterator<Item = ConnectionId>,
) -> Vec<ConnectionSnapshot<S, P>> {
    let mut result: Vec<_> = ids
        .filter_map(|id| inner.entries.get(&id))
        .map(|entry| {
            let mut snapshot = entry.snapshot.clone();
            snapshot.ended = current(&entry.sender).ended;
            snapshot
        })
        .collect();
    result.sort_by_key(|snapshot| snapshot.id);
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn signal_reaches_every_connection_of_a_session_and_no_other() {
        let registry = ConnectionRegistry::new();
        let mut connections: Vec<_> = (0..3)
            .map(|_| registry.register(1, "x", "pg", None))
            .collect();
        let (_b, b) = registry.register(2, "x", "pg", None);
        assert_eq!(registry.signal_session(&1, EndReason::Revoked), 3);
        for (_, sig) in &mut connections {
            assert_eq!(sig.ended().await, EndReason::Revoked);
        }
        assert_eq!(b.try_ended(), None);
        assert_eq!(registry.len(), 4);
    }

    #[tokio::test]
    async fn signal_by_principal() {
        let registry = ConnectionRegistry::new();
        let (_a, mut a) = registry.register(1, "x", "pg", None);
        let (_b, mut b) = registry.register(2, "x", "spanner", None);
        let (_c, c) = registry.register(3, "y", "pg", None);
        assert_eq!(registry.signal_principal(&"x", EndReason::Revoked), 2);
        assert_eq!(a.ended().await, EndReason::Revoked);
        assert_eq!(b.ended().await, EndReason::Revoked);
        assert_eq!(c.try_ended(), None);
    }

    #[tokio::test]
    async fn dropped_guard_disappears_from_listing() {
        let registry = ConnectionRegistry::new();
        let (a, _) = registry.register(1, "x", "pg", None);
        let (b, _) = registry.register(1, "x", "spanner", None);
        drop(a);
        for listing in [
            registry.list_session(&1),
            registry.list_principal(&"x"),
            registry.list_all(),
        ] {
            assert_eq!(listing.len(), 1);
            assert_eq!(listing[0].id, b.id());
            assert_eq!(listing[0].route, "spanner");
        }
        assert_eq!(registry.len(), 1);
        drop(b);
        assert_eq!(registry.signal_session(&1, EndReason::Revoked), 0);
        assert!(registry.list_session(&1).is_empty());
        assert!(registry.list_principal(&"x").is_empty());
        assert!(registry.list_all().is_empty());
        assert!(registry.is_empty());
        let inner = lock(&registry.inner);
        assert!(inner.sessions.is_empty());
        assert!(inner.principals.is_empty());
    }

    #[tokio::test]
    async fn guard_outlives_registry() {
        let registry = ConnectionRegistry::new();
        let (guard, mut sig) = registry.register(1, "x", "pg", None);
        drop(registry);
        drop(guard);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), sig.ended())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn deadline_expires() {
        let registry = ConnectionRegistry::new();
        let (_guard, mut sig) = registry.register(
            1,
            "x",
            "pg",
            Some(Instant::now() + Duration::from_millis(50)),
        );
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), sig.ended())
                .await
                .unwrap(),
            EndReason::Expired
        );
        assert_eq!(sig.try_ended(), Some(EndReason::Expired));
        assert_eq!(registry.list_all()[0].ended, Some(EndReason::Expired));
        assert_eq!(sig.ended().await, EndReason::Expired);
    }

    #[tokio::test]
    async fn extended_deadline_postpones_expiry() {
        let registry = ConnectionRegistry::new();
        let (_guard, mut sig) = registry.register(
            1,
            "x",
            "pg",
            Some(Instant::now() + Duration::from_millis(50)),
        );
        assert_eq!(
            registry.set_session_deadline(&1, Some(Instant::now() + Duration::from_millis(600))),
            1
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(sig.try_ended(), None);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), sig.ended())
                .await
                .unwrap(),
            EndReason::Expired
        );
    }

    #[tokio::test]
    async fn first_reason_wins() {
        let registry = ConnectionRegistry::new();
        let (_guard, mut sig) = registry.register(1, "x", "pg", None);
        assert_eq!(registry.signal_session(&1, EndReason::Revoked), 1);
        assert_eq!(registry.signal_session(&1, EndReason::Expired), 0);
        assert_eq!(sig.ended().await, EndReason::Revoked);
        assert_eq!(sig.ended().await, EndReason::Revoked);
        assert_eq!(registry.list_all()[0].ended, Some(EndReason::Revoked));
    }

    #[tokio::test]
    async fn usable_in_select() {
        let registry = ConnectionRegistry::new();
        let (_guard, mut sig) = registry.register(1, "x", "pg", None);
        let other = registry.clone();
        let signaler = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            other.signal_session(&1, EndReason::Revoked);
        });
        let result = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                tokio::select! {
                    r = sig.ended() => break r,
                    _ = tokio::time::sleep(Duration::from_millis(10)) => continue,
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(result, EndReason::Revoked);
        signaler.await.unwrap();
    }

    #[tokio::test]
    async fn ids_are_unique_and_increasing() {
        let registry = ConnectionRegistry::default();
        let (a, _) = registry.register(1, "x", "pg", None);
        assert_eq!(a.id().get(), 1);
        drop(a);
        let (b, _) = registry.register(1, "x", "pg", None);
        let (c, _) = registry.register(1, "x", "pg", None);
        assert_eq!(b.id().get(), 2);
        assert!(b.id() < c.id());
        let ids: Vec<_> = registry.list_all().into_iter().map(|s| s.id).collect();
        assert_eq!(ids, vec![b.id(), c.id()]);
    }

    #[tokio::test]
    async fn signal_unknown_key_returns_zero() {
        let registry = ConnectionRegistry::<u32, u32>::new();
        assert_eq!(registry.signal_session(&99, EndReason::Revoked), 0);
        assert_eq!(registry.signal_principal(&99, EndReason::Revoked), 0);
        assert_eq!(registry.set_session_deadline(&99, None), 0);
    }

    #[tokio::test]
    async fn pending_wait_honors_deadline_updates() {
        let registry = ConnectionRegistry::new();
        let (_guard, mut sig) = registry.register(
            1,
            "x",
            "pg",
            Some(Instant::now() + Duration::from_millis(80)),
        );
        let mut waiting = Box::pin(sig.ended());
        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut waiting)
                .await
                .is_err()
        );
        registry.set_session_deadline(&1, Some(Instant::now() + Duration::from_secs(2)));
        assert!(
            tokio::time::timeout(Duration::from_millis(150), &mut waiting)
                .await
                .is_err()
        );
        registry.set_session_deadline(&1, Some(Instant::now() + Duration::from_millis(20)));
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), waiting)
                .await
                .unwrap(),
            EndReason::Expired
        );
    }

    #[tokio::test]
    async fn elapsed_deadline_cannot_be_renewed_or_overridden() {
        let registry = ConnectionRegistry::new();
        let (_guard, sig) = registry.register(1, "x", "pg", Some(Instant::now()));
        registry.set_session_deadline(&1, None);
        assert_eq!(registry.signal_session(&1, EndReason::Revoked), 0);
        assert_eq!(sig.try_ended(), Some(EndReason::Expired));
    }

    #[tokio::test]
    async fn guard_and_signal_are_send() {
        fn assert_send<T: Send>(_: &T) {}
        let registry = ConnectionRegistry::new();
        let (guard, sig) = registry.register(1, "x", "pg", None);
        assert_send(&guard);
        assert_send(&sig);
    }
}
