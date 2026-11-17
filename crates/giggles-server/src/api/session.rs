use super::{error::ApiError, identity::ConnectionIdentity, limits::*};
use crate::{Service, Session};
use giggles_core::Cancellation;
use std::{
    collections::{HashMap, VecDeque},
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

pub(crate) trait Clock: Send + Sync {
    fn now(&self) -> Instant;
}
struct SystemClock;
impl Clock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}
pub(crate) trait Tokens: Send + Sync {
    fn next(&self) -> Result<[u8; 32], ApiError>;
}
struct RandomTokens;
impl Tokens for RandomTokens {
    fn next(&self) -> Result<[u8; 32], ApiError> {
        let mut token = [0; 32];
        getrandom::fill(&mut token).map_err(|_| ApiError::Internal)?;
        Ok(token)
    }
}

pub(crate) struct Registry {
    service: Arc<Service>,
    entries: Mutex<HashMap<String, Arc<Entry>>>,
    // Locked only under entries, so removing a live token and recording its
    // revocation is atomic to lookups. Markers own no sessions or permits.
    revoked: Mutex<VecDeque<Revoked>>,
    capacity: usize,
    slots: Arc<Semaphore>,
    clock: Arc<dyn Clock>,
    tokens: Arc<dyn Tokens>,
    stopping: AtomicBool,
}
struct Revoked {
    token: String,
    owner: ConnectionIdentity,
    at: Instant,
}
struct Entry {
    owner: ConnectionIdentity,
    created: Instant,
    _slot: OwnedSemaphorePermit,
    state: Mutex<State>,
}
struct State {
    session: Option<Session>,
    busy: bool,
    closing: bool,
    last_completed: Instant,
    transaction_start: Option<Instant>,
    cancellation: Option<Cancellation>,
}

impl Registry {
    pub(crate) fn new(service: Arc<Service>, limits: ApiLimits) -> Arc<Self> {
        Self::with_sources(
            service,
            limits,
            Arc::new(SystemClock),
            Arc::new(RandomTokens),
        )
    }

    pub(crate) fn with_sources(
        service: Arc<Service>,
        limits: ApiLimits,
        clock: Arc<dyn Clock>,
        tokens: Arc<dyn Tokens>,
    ) -> Arc<Self> {
        Arc::new(Self {
            service,
            entries: Mutex::new(HashMap::new()),
            revoked: Mutex::new(VecDeque::new()),
            capacity: limits.sessions,
            slots: Arc::new(Semaphore::new(limits.sessions)),
            clock,
            tokens,
            stopping: AtomicBool::new(false),
        })
    }

    pub(crate) fn now(&self) -> Instant {
        self.clock.now()
    }

    /// Test shorthand for an open without a task.
    #[cfg(test)]
    pub(crate) fn open(&self, owner: &ConnectionIdentity) -> Result<String, ApiError> {
        self.open_task(owner, None)
    }

    /// `task` selects the task of an agent session (GD-0061 R3-208); an
    /// ordinary identity must not send one.
    pub(crate) fn open_task(
        &self,
        owner: &ConnectionIdentity,
        task: Option<&str>,
    ) -> Result<String, ApiError> {
        if self.stopping.load(Ordering::Acquire) {
            return Err(ApiError::Unavailable);
        }
        if task.is_some() && !owner.agent {
            return Err(ApiError::InvalidArgument);
        }
        if let Some(task) = task {
            giggles_core::task::definition::TaskName::new(task)
                .map_err(|_| ApiError::InvalidArgument)?;
        }
        owner.check(&self.service)?;
        let slot = self
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| ApiError::ResourceExhausted)?;
        let session = if owner.agent {
            self.service.agent_session(&owner.fingerprint, task)
        } else {
            self.service.session(&owner.fingerprint)
        }
        .map_err(|error| {
            if owner.agent {
                eprintln!("protocol=agent_api rejected stage=open reason={error:?}");
            }
            ApiError::Unauthenticated
        })?;
        if session.epoch != owner.epoch || session.role != owner.role {
            return Err(ApiError::Unauthenticated);
        }
        let now = self.now();
        let entry = Arc::new(Entry {
            owner: owner.clone(),
            created: now,
            _slot: slot,
            state: Mutex::new(State {
                session: Some(session),
                busy: false,
                closing: false,
                last_completed: now,
                transaction_start: None,
                cancellation: None,
            }),
        });
        for _ in 0..3 {
            let token = hex::encode(self.tokens.next()?);
            {
                let mut map = self.entries.lock().map_err(|_| ApiError::Internal)?;
                if self.stopping.load(Ordering::Acquire) {
                    return Err(ApiError::Unavailable);
                }
                let mut revoked = self.revoked.lock().map_err(|_| ApiError::Internal)?;
                revoked.retain(|marker| now < marker.at + IDLE_TIMEOUT);
                if map.contains_key(&token) || revoked.iter().any(|marker| marker.token == token) {
                    continue;
                }
                map.insert(token.clone(), entry.clone());
            }
            // Reload may race construction/insertion. Never publish a stale handle.
            if let Err(error) = owner.check(&self.service) {
                self.invalidate(&token, &entry, true);
                return Err(error);
            }
            return Ok(token);
        }
        Err(ApiError::Internal)
    }

    pub(crate) fn valid_token(token: &str) -> bool {
        token.len() == 64
            && token
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    }

    fn lookup(&self, token: &str, owner: &ConnectionIdentity) -> Result<Arc<Entry>, ApiError> {
        if !Self::valid_token(token) {
            return Err(ApiError::InvalidArgument);
        }
        owner.check(&self.service)?;
        let entry = {
            let map = self.entries.lock().map_err(|_| ApiError::Internal)?;
            let mut revoked = self.revoked.lock().map_err(|_| ApiError::Internal)?;
            let now = self.now();
            revoked.retain(|marker| now < marker.at + IDLE_TIMEOUT);
            if revoked
                .iter()
                .any(|marker| marker.token == token && &marker.owner == owner)
            {
                return Err(ApiError::Unauthenticated);
            }
            map.get(token).cloned().ok_or(ApiError::SessionNotFound)?
        };
        if &entry.owner != owner {
            return Err(ApiError::SessionNotFound);
        }
        if entry.owner.check(&self.service).is_err() {
            let state = entry.state.lock().map_err(|_| ApiError::Internal)?;
            let expired = expired(&entry, &state, self.now());
            drop(state);
            self.invalidate(token, &entry, !expired);
            return Err(if expired {
                ApiError::SessionNotFound
            } else {
                ApiError::Unauthenticated
            });
        }
        Ok(entry)
    }

    pub(crate) fn lease(
        self: &Arc<Self>,
        token: &str,
        owner: &ConnectionIdentity,
        cancellation: Cancellation,
    ) -> Result<SessionLease, ApiError> {
        if self.stopping.load(Ordering::Acquire) {
            return Err(ApiError::Unavailable);
        }
        let entry = self.lookup(token, owner)?;
        let now = self.now();
        let mut state = entry.state.lock().map_err(|_| ApiError::Internal)?;
        if state.closing || expired(&entry, &state, now) {
            drop(state);
            self.invalidate(token, &entry, false);
            return Err(ApiError::SessionNotFound);
        }
        if state.busy {
            return Err(ApiError::SessionBusy);
        }
        let session = state.session.take().ok_or(ApiError::Internal)?;
        let deadline = state
            .transaction_start
            .map(|start| start + TRANSACTION_TIMEOUT)
            .map_or(entry.created + MAX_AGE, |transaction| {
                transaction.min(entry.created + MAX_AGE)
            });
        cancellation.deadline(deadline);
        state.busy = true;
        state.cancellation = Some(cancellation);
        drop(state);
        Ok(SessionLease {
            registry: Arc::downgrade(self),
            token: token.into(),
            entry,
            session: Some(session),
            completed: None,
            invalidate_on_drop: false,
        })
    }

    pub(crate) fn close(&self, token: &str, owner: &ConnectionIdentity) -> Result<(), ApiError> {
        let entry = self.lookup(token, owner)?;
        let state = entry.state.lock().map_err(|_| ApiError::Internal)?;
        let found = !state.closing && !expired(&entry, &state, self.now());
        drop(state);
        let removed = self.invalidate(token, &entry, false);
        if found && removed {
            Ok(())
        } else {
            Err(ApiError::SessionNotFound)
        }
    }

    /// Mark closed before removing the map key; lookups that already copied the Arc
    /// observe Closing. No work, authority lock, or destructor runs under map locks.
    fn invalidate(&self, token: &str, entry: &Arc<Entry>, revoked: bool) -> bool {
        let (session, cancellation) = {
            let mut state = entry.state.lock().unwrap_or_else(|p| p.into_inner());
            state.closing = true;
            (state.session.take(), state.cancellation.take())
        };
        let removed = {
            let mut map = self.entries.lock().unwrap_or_else(|p| p.into_inner());
            if map
                .get(token)
                .is_some_and(|found| Arc::ptr_eq(found, entry))
            {
                map.remove(token);
                if revoked {
                    let now = self.now();
                    let mut markers = self.revoked.lock().unwrap_or_else(|p| p.into_inner());
                    markers.retain(|marker| now < marker.at + IDLE_TIMEOUT);
                    if markers.len() == self.capacity {
                        markers.pop_front();
                    }
                    markers.push_back(Revoked {
                        token: token.into(),
                        owner: entry.owner.clone(),
                        at: now,
                    });
                }
                true
            } else {
                false
            }
        };
        if let Some(cancellation) = cancellation {
            cancellation.cancel();
        }
        drop(session);
        removed
    }

    pub(crate) fn sweep(&self) {
        let entries: Vec<_> = self
            .entries
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .map(|(token, entry)| (token.clone(), entry.clone()))
            .collect();
        let now = self.now();
        {
            let _map = self.entries.lock().unwrap_or_else(|p| p.into_inner());
            self.revoked
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .retain(|marker| now < marker.at + IDLE_TIMEOUT);
        }
        for (token, entry) in entries {
            let invalid_identity = entry.owner.check(&self.service).is_err();
            let (invalid_state, expired) = entry.state.lock().map_or((true, false), |state| {
                let expired = expired(&entry, &state, now);
                (state.closing || expired, expired)
            });
            if invalid_identity || invalid_state {
                self.invalidate(&token, &entry, invalid_identity && !expired);
            }
        }
    }

    pub(crate) fn stop(&self) {
        self.stopping.store(true, Ordering::Release);
        let entries: Vec<_> = self
            .entries
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .map(|(token, entry)| (token.clone(), entry.clone()))
            .collect();
        for (token, entry) in entries {
            self.invalidate(&token, &entry, false);
        }
    }
}

fn expired(entry: &Entry, state: &State, now: Instant) -> bool {
    now >= entry.created + MAX_AGE
        || state
            .transaction_start
            .is_some_and(|at| now >= at + TRANSACTION_TIMEOUT)
        || (!state.busy && now >= state.last_completed + IDLE_TIMEOUT)
}

/// Owns the unique live Session while a call is queued, running, or being sent.
/// Dropping a response can restore it; unwinding/Closing can only discard it.
pub(crate) struct SessionLease {
    registry: Weak<Registry>,
    token: String,
    entry: Arc<Entry>,
    session: Option<Session>,
    completed: Option<Instant>,
    invalidate_on_drop: bool,
}

impl SessionLease {
    pub(crate) fn session(&mut self) -> &mut Session {
        self.session
            .as_mut()
            .expect("exclusive SessionLease owns its session")
    }

    pub(crate) fn completed(&mut self) {
        if let Some(registry) = self.registry.upgrade() {
            let now = registry.now();
            self.completed = Some(now);
            let transaction = self.session.as_ref().is_some_and(Session::in_transaction);
            let mut state = self.entry.state.lock().unwrap_or_else(|p| p.into_inner());
            if transaction {
                state.transaction_start.get_or_insert(now);
            } else {
                state.transaction_start = None;
            }
        }
    }

    pub(crate) fn check_release(&self) -> Result<(), ApiError> {
        let registry = self.registry.upgrade().ok_or(ApiError::Unavailable)?;
        self.entry.owner.check(&registry.service)?;
        let state = self.entry.state.lock().map_err(|_| ApiError::Internal)?;
        if state.closing || expired(&self.entry, &state, registry.now()) {
            Err(ApiError::SessionNotFound)
        } else {
            Ok(())
        }
    }

    pub(crate) fn invalidate(&mut self) {
        self.invalidate_on_drop = true;
    }
}

impl Drop for SessionLease {
    fn drop(&mut self) {
        let Some(mut session) = self.session.take() else {
            return;
        };
        session.connection = None;
        let Some(registry) = self.registry.upgrade() else {
            return;
        };
        let identity_valid = self.entry.owner.check(&registry.service).is_ok();
        let now = registry.now();
        let mut state = self.entry.state.lock().unwrap_or_else(|p| p.into_inner());
        let close = state.closing
            || self.entry.state.is_poisoned()
            || self.invalidate_on_drop
            || std::thread::panicking()
            || !identity_valid
            || expired(&self.entry, &state, now);
        if !close {
            state.session = Some(session);
            state.busy = false;
            state.cancellation = None;
            if let Some(completed) = self.completed {
                state.last_completed = completed;
            }
            return;
        }
        state.closing = true;
        drop(state);
        registry.invalidate(&self.token, &self.entry, !identity_valid);
        drop(session);
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod revoked_tests {
    use super::*;
    use crate::{Binding, Role, clock::Clock as ServiceClock};
    use giggles_core::{Admin, Limits};

    struct TestClock(Mutex<Instant>);
    impl Clock for TestClock {
        fn now(&self) -> Instant {
            *self.0.lock().unwrap()
        }
    }

    #[test]
    fn markers_are_owner_scoped_bounded_and_expire_without_holding_slots() {
        let path = crate::test_support::fixture_root().join(format!(
            "api-revoked-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&path).unwrap();
        let admin = Admin::create(path.join("db.redb"), Limits::default()).unwrap();
        admin
            .sql_session()
            .execute("CREATE TABLE app (id BIGINT PRIMARY KEY)", &[], None)
            .unwrap();
        let role = Role::Application {
            domain: admin.authority_metadata().unwrap().0.to_string(),
            tables: ["app".into()].into_iter().collect(),
        };
        let bindings: Vec<_> = ["11", "22"]
            .into_iter()
            .map(|hex| Binding {
                fingerprint: hex.repeat(32),
                role: role.clone(),
            })
            .collect();
        let service =
            Service::new(admin, bindings.clone(), ServiceClock::new(0, 3600).unwrap()).unwrap();
        let owner = ConnectionIdentity::verified(&service, &bindings[0].fingerprint).unwrap();
        let other = ConnectionIdentity::verified(&service, &bindings[1].fingerprint).unwrap();
        let clock = Arc::new(TestClock(Mutex::new(Instant::now())));
        let registry = Registry::with_sources(
            service.clone(),
            ApiLimits { sessions: 2 },
            clock.clone(),
            Arc::new(RandomTokens),
        );
        let survivor = registry.open(&other).unwrap();
        let first = registry.open(&owner).unwrap();
        service.reload_bindings(vec![bindings[1].clone()]).unwrap();
        registry.sweep();
        assert_eq!(registry.slots.available_permits(), 1);
        service.reload_bindings(bindings.clone()).unwrap();
        // A replaced binding has a new epoch and cannot inspect another owner's marker.
        let replacement = ConnectionIdentity::verified(&service, &bindings[0].fingerprint).unwrap();
        assert!(matches!(
            registry.lookup(&first, &replacement),
            Err(ApiError::SessionNotFound)
        ));
        assert!(matches!(
            registry.lookup(&first, &other),
            Err(ApiError::SessionNotFound)
        ));
        assert!(registry.lookup(&survivor, &other).is_ok());

        // Drive two more genuine binding failures, including one busy lease.
        let second = registry.open(&replacement).unwrap();
        let cancellation = Cancellation::default();
        let lease = registry
            .lease(&second, &replacement, cancellation.clone())
            .unwrap();
        service.reload_bindings(vec![bindings[1].clone()]).unwrap();
        registry.sweep();
        assert!(cancellation.check().is_err());
        assert_eq!(registry.slots.available_permits(), 0);
        drop(lease);
        assert_eq!(registry.slots.available_permits(), 1);
        service.reload_bindings(bindings.clone()).unwrap();
        let replacement = ConnectionIdentity::verified(&service, &bindings[0].fingerprint).unwrap();
        let third = registry.open(&replacement).unwrap();
        service.reload_bindings(vec![bindings[1].clone()]).unwrap();
        registry.sweep();
        let markers = registry.revoked.lock().unwrap();
        assert_eq!(markers.len(), 2);
        assert_eq!(markers[0].token, second);
        assert_eq!(markers[1].token, third);
        drop(markers);
        assert!(registry.lookup(&survivor, &other).is_ok());
        // Expiry is measured from eviction and queries never refresh it.
        *clock.0.lock().unwrap() += IDLE_TIMEOUT;
        registry.sweep();
        assert!(registry.revoked.lock().unwrap().is_empty());
        assert_eq!(registry.slots.available_permits(), 2);
        assert!(matches!(
            registry.lookup(&third, &other),
            Err(ApiError::SessionNotFound)
        ));
    }
}
