//! Lease-bounded pod state and conservative durable counter reservations.
use super::*;
use crate::{
    policy::{
        DomainName,
        kube_store::RuntimeBridge,
        serving::{ServingGate, now_nanos},
    },
    state::{
        authority::{AdmitError, Admitted, Operation, SessionLogin, TokenizeError, TokenizeLimits},
        model::*,
        runtime::Registry,
        serving_state::ServingState,
    },
    token::ScopeId,
};
use hmac::{Hmac, KeyInit, Mac};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};
const TIMEOUT: Duration = Duration::from_secs(2);
struct Entry {
    session: AccessSession,
    credentials: Vec<SessionCredential>,
    verifiers: Vec<(CredentialId, Verifier)>,
}
struct Batch {
    id: String,
    left: u64,
    expires: u64,
}
#[derive(Default)]
struct View {
    epoch: u64,
    touched: HashMap<SessionId, u64>,
    activity: u64,
    sessions: HashMap<SessionId, Entry>,
    revalidate: HashSet<SessionId>,
    scopes: HashMap<ScopeId, TokenScope>,
    batches: HashMap<(SessionId, String), Batch>,
    since: u64,
    until: u64,
    term: u64,
}
/// Per-pod serving state, valid only within both the serving lease and last successful sync term.
pub struct PeerStateCache {
    client: Arc<PeerClient>,
    bridge: RuntimeBridge,
    runtime: Option<tokio::runtime::Handle>,
    gate: Arc<ServingGate>,
    registry: Registry,
    term: Duration,
    batch: u64,
    view: Arc<Mutex<View>>,
    connections: Mutex<HashMap<SessionId, u64>>,
    mono: Arc<dyn Fn() -> u64 + Send + Sync>,
    wall: Arc<dyn Fn() -> u64 + Send + Sync>,
}
impl PeerStateCache {
    /// Build an initially closed cache; no local Authority is used.
    pub fn new(
        client: Arc<PeerClient>,
        gate: Arc<ServingGate>,
        registry: Registry,
        term: Duration,
        counter_batch: u64,
    ) -> Self {
        let bridge = RuntimeBridge::with_timeout(TIMEOUT);
        let runtime = bridge.run(async { tokio::runtime::Handle::current() });
        Self {
            client,
            bridge,
            runtime,
            gate,
            registry,
            term,
            batch: counter_batch.clamp(1, 64),
            view: Arc::default(),
            connections: Mutex::default(),
            mono: Arc::new(now_nanos),
            wall: Arc::new(auth::now),
        }
    }
    #[cfg(test)]
    pub(super) fn with_clocks(
        mut self,
        mono: Arc<dyn Fn() -> u64 + Send + Sync>,
        wall: Arc<dyn Fn() -> u64 + Send + Sync>,
    ) -> Self {
        self.mono = mono;
        self.wall = wall;
        self
    }
    #[cfg(test)]
    pub(super) fn close_gate_for_test(&self) {
        self.gate.close();
    }
    #[cfg(test)]
    pub(super) fn registry_for_test(&self) -> &Registry {
        &self.registry
    }
    #[cfg(test)]
    pub(super) fn cached(&self) -> (usize, usize) {
        let v = self.lock();
        (v.sessions.len(), v.batches.len())
    }
    fn lock(&self) -> MutexGuard<'_, View> {
        self.view
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    // Enqueue rather than block on best-effort returns and connection activity.
    fn background(&self, future: impl std::future::Future<Output = ()> + Send + 'static) {
        if let Some(runtime) = &self.runtime {
            runtime.spawn(future);
        }
    }
    fn give_back(&self, batch: Batch) {
        if batch.left == 0 {
            return;
        }
        let client = self.client.clone();
        self.background(async move {
            let _ = client
                .counter_return(
                    Return {
                        batch_id: batch.id,
                        unused: batch.left,
                    },
                    TIMEOUT,
                )
                .await;
        });
    }
    fn signal(&self, id: SessionId, reason: EndReason) {
        let reason = match reason {
            EndReason::Expired | EndReason::IdleTimeout => {
                crate::state::registry::EndReason::Expired
            }
            EndReason::Requested
            | EndReason::AdminRevoked
            | EndReason::PrincipalDisabled
            | EndReason::ConfigRemoved
            | EndReason::GrantRemoved
            | EndReason::PolicyReset => crate::state::registry::EndReason::Revoked,
        };
        self.registry.signal_session(&id, reason);
    }
    fn clear(&self, v: &mut View) {
        v.revalidate.extend(v.sessions.keys().copied());
        if v.until != 0 || !v.sessions.is_empty() || !v.scopes.is_empty() || !v.batches.is_empty() {
            v.epoch = v.epoch.wrapping_add(1);
        }
        v.sessions.clear();
        v.scopes.clear();
        for (_, b) in v.batches.drain() {
            self.give_back(b);
        }
        v.until = 0;
    }
    fn reset(&self, v: &mut View) {
        v.epoch = v.epoch.wrapping_add(1);
        v.revalidate.extend(v.sessions.keys().copied());
        v.sessions.clear();
        v.scopes.clear();
        for (_, batch) in v.batches.drain() {
            self.give_back(batch);
        }
        v.until = 0;
    }
    fn usable(&self, v: &mut View) -> bool {
        if !self.gate.allow() || (self.mono)() >= v.until {
            self.clear(v);
            return false;
        }
        if self.client.term() != v.term {
            self.reset(v);
            return false;
        }
        true
    }
    fn evict(&self, v: &mut View, id: SessionId, reason: Option<EndReason>) {
        v.sessions.remove(&id);
        let keys: Vec<_> = v
            .batches
            .keys()
            .filter(|(s, _)| *s == id)
            .cloned()
            .collect();
        for k in keys {
            if let Some(b) = v.batches.remove(&k) {
                self.give_back(b);
            }
        }
        v.epoch = v.epoch.wrapping_add(1);
        if let Some(reason) = reason { self.signal(id, reason); }
    }
    /// Apply leader invalidations after a successful serving renewal; failures never extend validity.
    pub fn sync(&self) -> Result<(), PeerError> {
        let mut v = self.lock();
        // Discard expired state before attempting a fresh sync, even if the call fails.
        self.usable(&mut v);
        let start = (self.mono)();
        let client = self.client.clone();
        let since = v.since;
        let epoch = v.epoch;
        let sent = v.touched.iter().map(|(id, seq)| (*id, *seq)).take(1024).collect::<Vec<_>>();
        let touched = sent.iter().map(|(id, _)| *id).collect();
        drop(v);
        let changes = self
            .bridge
            .run(async move { client.changes(ChangesRequest { since, touched }, TIMEOUT).await })
            .ok_or(PeerError::Timeout)??;
        let mut v = self.lock();
        if v.epoch != epoch { return Err(PeerError::Stale); }
        for (id, seq) in sent { if v.touched.get(&id) == Some(&seq) { v.touched.remove(&id); } }
        if changes.generation != self.client.term() {
            return Err(PeerError::Stale);
        }
        if changes.reset || v.term != self.client.term() {
            self.reset(&mut v);
        }
        // The registry has end signals, not a revalidation signal. Revalidate live
        // connections here before reopening the cache; only ended/unknown sessions
        // are signalled. A transport failure keeps the cache closed until retry.
        for c in changes.changes {
            match c.kind {
                ChangeKind::Refresh {} => { self.evict(&mut v, c.session_id, None); v.revalidate.insert(c.session_id); }
                ChangeKind::End { reason } => self.evict(&mut v, c.session_id, Some(reason)),
            }
        }
        let epoch = v.epoch;
        let pending: Vec<_> = v.revalidate.iter().copied().collect();
        drop(v);
        for id in pending {
            let client = self.client.clone();
            let result = self
                .bridge
                .run(async move {
                    client
                        .session_lookup(Lookup { session_id: id }, TIMEOUT)
                        .await
                })
                .ok_or(PeerError::Timeout)??;
            if self.lock().epoch != epoch || self.client.term() != changes.generation {
                return Err(PeerError::Stale);
            }
            match result {
                LookupResult::Active { session, .. } if session.id == id => {}
                LookupResult::Active { .. } => return Err(PeerError::Protocol),
                LookupResult::Unknown => self.signal(id, EndReason::AdminRevoked),
                LookupResult::Ended { reason } => {
                    self.signal(id, reason.unwrap_or(EndReason::AdminRevoked))
                }
            }
        }
        let mut v = self.lock();
        if v.epoch != epoch || self.client.term() != changes.generation { return Err(PeerError::Stale); }
        v.revalidate.clear();
        v.term = self.client.term();
        v.since = changes.next;
        if !self.gate.allow() {
            self.clear(&mut v);
            return Err(PeerError::Timeout);
        }
        v.until = start.saturating_add(self.term.as_nanos().try_into().unwrap_or(u64::MAX));
        if !self.usable(&mut v) {
            return Err(PeerError::Timeout);
        }
        // Return batches before the safety deadline, including idle sessions.
        let now = (self.wall)();
        let keys: Vec<_> = v
            .batches
            .iter()
            .filter(|(_, b)| now >= b.expires.saturating_sub(1000))
            .map(|(k, _)| k.clone())
            .collect();
        for k in keys {
            if let Some(b) = v.batches.remove(&k) {
                self.give_back(b);
            }
        }
        Ok(())
    }
    fn load(&self, id: SessionId) -> Result<MutexGuard<'_, View>, AdmitError> {
        let mut v = self.lock();
        if !self.usable(&mut v) {
            return Err(AdmitError::Unavailable);
        }
        if !v.sessions.contains_key(&id) {
            let epoch = v.epoch;
            let term = v.term;
            drop(v);
            let client = self.client.clone();
            let result = self
                .bridge
                .run(async move {
                    client
                        .session_lookup(Lookup { session_id: id }, TIMEOUT)
                        .await
                })
                .and_then(Result::ok)
                .ok_or(AdmitError::Unavailable)?;
            v = self.lock();
            if v.epoch != epoch || v.term != term || !self.usable(&mut v) {
                return Err(AdmitError::Unavailable);
            }
            match result {
                LookupResult::Active {
                    session,
                    credentials,
                    verifiers,
                } => {
                    if session.id != id {
                        return Err(AdmitError::Ended);
                    }
                    v.sessions.insert(
                        id,
                        Entry {
                            session: *session,
                            credentials,
                            verifiers,
                        },
                    );
                }
                LookupResult::Unknown => { self.signal(id, EndReason::AdminRevoked); return Err(AdmitError::Unknown); },
                LookupResult::Ended { reason } => {
                    self.signal(id, reason.unwrap_or(EndReason::AdminRevoked));
                    return Err(AdmitError::Ended);
                }
            }
        }
        let now = (self.wall)();
        let s = &v.sessions.get(&id).ok_or(AdmitError::Unknown)?.session;
        let reason = if s.state != SessionState::Active {
            Some(s.revoke_reason.unwrap_or(EndReason::AdminRevoked))
        } else if now >= s.expires_ms {
            Some(EndReason::Expired)
        } else if s
            .idle_timeout_ms
            .is_some_and(|idle| now.saturating_sub(s.last_used_ms) > idle)
        {
            Some(EndReason::IdleTimeout)
        } else {
            None
        };
        if let Some(reason) = reason {
            self.evict(&mut v, id, Some(reason));
            return Err(AdmitError::Ended);
        }
        Ok(v)
    }
    fn scope_cached(&self, id: ScopeId) -> Option<TokenScope> {
        let mut v = self.lock();
        if !self.usable(&mut v) {
            return None;
        }
        if let Some(s) = v.scopes.get(&id) {
            return Some(s.clone());
        }
        let epoch = v.epoch;
        let term = v.term;
        drop(v);
        let client = self.client.clone();
        let result = self
            .bridge
            .run(async move { client.scope_get(ScopeGet { scope_id: id }, TIMEOUT).await })?
            .ok()?
            .scope?;
        let mut v = self.lock();
        if v.epoch != epoch || v.term != term || !self.usable(&mut v) || result.id != id {
            return None;
        }
        v.scopes.insert(id, result.clone());
        Some(result)
    }
}
impl ServingState for PeerStateCache {
    fn available(&self) -> bool {
        self.usable(&mut self.lock())
    }
    fn session_login(&self, id: SessionId, route: &RouteId) -> Option<SessionLogin> {
        let v = self.load(id).ok()?;
        let e = v.sessions.get(&id)?;
        if e.session.route != *route {
            return None;
        }
        let principal = e.session.principal;
        let scope_id = e.session.scope;
        let now = (self.wall)();
        let verifiers = e
            .verifiers
            .iter()
            .filter(|(id, _)| {
                e.credentials.iter().any(|c| {
                    c.id == *id && c.state == CredentialState::Active && now < c.expires_ms
                })
            })
            .cloned()
            .collect::<Vec<_>>();
        if verifiers.is_empty() {
            return None;
        }
        let epoch = v.epoch;
        let term = v.term;
        drop(v);
        let scope = self.scope_cached(scope_id)?;
        let mut v = self.lock();
        if v.epoch != epoch || v.term != term || !self.usable(&mut v) { return None; }
        if scope.route != *route || scope.derivation_version != 2 {
            return None;
        }
        Some(SessionLogin {
            principal,
            scope,
            verifiers,
        })
    }
    fn session_secret_credential(&self, id: SessionId, secret: &[u8]) -> Option<CredentialId> {
        let digest: [u8; 32] = Sha256::digest(secret).into();
        let mut v = self.load(id).ok()?;
        let now = (self.wall)();
        let e = v.sessions.get(&id)?;
        let mut matched = false;
        let mut selected = [0; 16];
        for c in e
            .credentials
            .iter()
            .filter(|c| c.state == CredentialState::Active && now < c.expires_ms)
        {
            // Same constant-time digest verification and credential selection as Authority login.
            let mut candidate = Hmac::<Sha256>::new_from_slice(&c.secret_hash).ok()?;
            candidate.update(&digest);
            let tag = candidate.finalize().into_bytes();
            let mut stored = Hmac::<Sha256>::new_from_slice(&c.secret_hash).ok()?;
            stored.update(&c.secret_hash);
            let valid = stored.verify_slice(&tag).is_ok();
            let mask = 0u8.wrapping_sub(u8::from(valid));
            for (out, byte) in selected.iter_mut().zip(c.id.0) {
                *out = (*out & !mask) | (byte & mask);
            }
            matched |= valid;
        }
        if matched && let Some(e) = v.sessions.get_mut(&id) {
            e.session.last_used_ms = e.session.last_used_ms.max(now);
        }
        matched.then_some(CredentialId(selected))
    }
    fn admit(&self, id: SessionId, op: Operation) -> Result<Admitted, AdmitError> {
        let mut v = self.load(id)?;
        if v.touched.len() >= 1024 && !v.touched.contains_key(&id) { return Err(AdmitError::Unavailable); }
        let e = v.sessions.get_mut(&id).ok_or(AdmitError::Unknown)?;
        let s = &mut e.session;
        if matches!(op, Operation::Write) && !s.mode.permits_write() {
            return Err(AdmitError::ReadOnly);
        }
        s.last_used_ms = s.last_used_ms.max((self.wall)());
        let admitted = Admitted {
            principal: s.principal,
            route: s.route.clone(),
            mode: s.mode,
            scope: s.scope,
        };
        v.activity = v.activity.wrapping_add(1);
        let activity = v.activity;
        v.touched.insert(id, activity);
        Ok(admitted)
    }
    fn session(&self, id: SessionId) -> Option<AccessSession> {
        let v = self.load(id).ok()?;
        Some(v.sessions.get(&id)?.session.clone())
    }
    fn scope(&self, id: ScopeId) -> Option<TokenScope> {
        self.scope_cached(id)
    }
    fn record_tokenization(
        &self,
        id: SessionId,
        domain: &DomainName,
        _limits: &TokenizeLimits,
    ) -> Result<(), TokenizeError> {
        let mut v = self.load(id).map_err(|e| match e { AdmitError::Unavailable => TokenizeError::Store, AdmitError::Ended | AdmitError::Unknown | AdmitError::ReadOnly => TokenizeError::Ended })?;
        let key = (id, domain.0.clone());
        let now = (self.wall)();
        if v.batches
            .get(&key)
            .is_some_and(|b| b.left == 0 || now >= b.expires.saturating_sub(1000))
            && let Some(b) = v.batches.remove(&key)
        {
            self.give_back(b);
        }
        if !v.batches.contains_key(&key) {
            let epoch = v.epoch;
            let term = v.term;
            drop(v);
            let client = self.client.clone();
            let d = domain.0.clone();
            let want = self.batch;
            let result = self
                .bridge
                .run(async move {
                    client
                        .counter_reserve(
                            Reserve {
                                session_id: id,
                                domain: d,
                                want,
                            },
                            TIMEOUT,
                        )
                        .await
                })
                .and_then(Result::ok)
                .ok_or(TokenizeError::Store)?;
            let r = match result {
                ReserveResult::Granted(r) => r,
                ReserveResult::Ended(_) => return Err(TokenizeError::Ended),
            };
            if r.granted == 0 {
                return Err(TokenizeError::LimitReached);
            }
            let b = Batch {
                id: r.batch_id,
                left: r.granted,
                expires: r.expires_ms,
            };
            v = self.lock();
            if v.epoch != epoch || v.term != term || !self.usable(&mut v) || (self.wall)() >= b.expires.saturating_sub(1000) {
                self.give_back(b);
                return Err(TokenizeError::Store);
            }
            // A separate timer refunds idle batches before expiry, without waiting for renewal.
            let view = self.view.clone();
            let client = self.client.clone();
            let timer_key = key.clone();
            let batch_id = b.id.clone();
            let delay =
                Duration::from_millis(b.expires.saturating_sub((self.wall)()).saturating_sub(1000));
            self.background(async move {
                tokio::time::sleep(delay).await;
                let batch = {
                    let mut v = view
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    if v.batches.get(&timer_key).is_some_and(|b| b.id == batch_id) {
                        v.batches.remove(&timer_key)
                    } else {
                        None
                    }
                };
                if let Some(b) = batch {
                    let _ = client
                        .counter_return(
                            Return {
                                batch_id: b.id,
                                unused: b.left,
                            },
                            TIMEOUT,
                        )
                        .await;
                }
            });
            if let Some(previous) = v.batches.insert(key.clone(), b) { self.give_back(previous); }
        }
        if !self.usable(&mut v) {
            return Err(TokenizeError::Store);
        }
        let b = v.batches.get_mut(&key).ok_or(TokenizeError::Store)?;
        if (self.wall)() >= b.expires.saturating_sub(1000) {
            return Err(TokenizeError::Store);
        }
        b.left = b.left.checked_sub(1).ok_or(TokenizeError::LimitReached)?;
        Ok(())
    }
    fn report(&self, id: SessionId, start: bool) {
        let last = {
            let mut connections = self
                .connections
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if start {
                *connections.entry(id).or_default() += 1;
                false
            } else {
                if let Some(n) = connections.get_mut(&id) {
                    *n = n.saturating_sub(1);
                    if *n == 0 {
                        connections.remove(&id);
                    }
                }
                !connections.contains_key(&id)
            }
        };
        let client = self.client.clone();
        let view = self.view.clone();
        self.background(async move {
            if last {
                let batches = tokio::task::spawn_blocking(move || {
                    let mut v = view
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    let keys: Vec<_> = v
                        .batches
                        .keys()
                        .filter(|(session, _)| *session == id)
                        .cloned()
                        .collect();
                    keys.into_iter()
                        .filter_map(|key| v.batches.remove(&key))
                        .collect::<Vec<_>>()
                })
                .await
                .unwrap_or_default();
                for b in batches {
                    let _ = client
                        .counter_return(
                            Return {
                                batch_id: b.id,
                                unused: b.left,
                            },
                            TIMEOUT,
                        )
                        .await;
                }
            }
            let _ = client
                .session_report(
                    Report {
                        session_id: id,
                        event: if start { Event::Start } else { Event::End },
                        connections: None,
                    },
                    TIMEOUT,
                )
                .await;
        });
    }
}

impl Drop for PeerStateCache {
    fn drop(&mut self) {
        let mut v = self.lock();
        self.clear(&mut v);
    }
}
