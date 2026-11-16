//! In-memory authority with atomic durable mutations and fail-closed admission.
use super::{
    model::*,
    schema::Table,
    store::{Store, StoreError},
};
use crate::token::{PrincipalId, ScopeId, SessionId};
use hmac::{Hmac, KeyInit, Mac};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet},
    fmt,
    sync::{Arc, Mutex, RwLock},
};

mod config_sync;
mod counters;
mod login;
pub use login::SessionLogin;
pub use config_sync::{ConfigPrincipal, ConfigSync};
pub use counters::{TokenizeError, TokenizeLimits};

/// Service-wide upper bounds for session lifetimes.
#[derive(Clone, Copy, Debug)]
pub struct SessionLimits {
    /// Maximum absolute session lifetime.
    pub max_lifetime_ms: u64,
    /// Optional idle timeout.
    pub idle_timeout_ms: Option<u64>,
}
impl Default for SessionLimits {
    fn default() -> Self {
        Self {
            max_lifetime_ms: 8 * 60 * 60 * 1000,
            idle_timeout_ms: Some(60 * 60 * 1000),
        }
    }
}
/// Post-commit notification for connection termination.
pub trait RevocationHook: Send + Sync {
    /// Signal a durably ended session outside authority locks.
    fn session_ended(&self, session: SessionId, principal: PrincipalId, reason: EndReason);
}
/// Requested execution access.
#[derive(Clone, Copy, Debug)]
pub enum Operation {
    /// Read-only execution.
    Read,
    /// Writing execution.
    Write,
}
/// Effective authorization for one execution.
#[derive(Debug)]
pub struct Admitted {
    /// Owning principal.
    pub principal: PrincipalId,
    /// Authorized route.
    pub route: RouteId,
    /// Restrictive intersection of the grant and session modes.
    pub mode: Mode,
    /// Session-owned token scope.
    pub scope: ScopeId,
}
/// Data-free admission failure.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum AdmitError {
    /// Unknown session.
    #[error("unknown session")]
    Unknown,
    /// Session is no longer authorized.
    #[error("session ended")]
    Ended,
    /// Effective mode refuses writes.
    #[error("session is read only")]
    ReadOnly,
}
/// Data-free authority operation failure.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum AuthorityError {
    /// Target does not exist.
    #[error("not found")]
    NotFound,
    /// Invalid principal name.
    #[error("invalid principal name")]
    NameInvalid,
    /// Principal name already exists.
    #[error("principal name taken")]
    NameTaken,
    /// Configuration owns the principal.
    #[error("principal is config managed")]
    ConfigManaged,
    /// Principal is disabled.
    #[error("principal disabled")]
    PrincipalDisabled,
    /// No grant for the route.
    #[error("no route grant")]
    NoGrant,
    /// Requested mode exceeds grant.
    #[error("mode exceeds grant")]
    ModeExceedsGrant,
    /// Requested lifetime exceeds limit.
    #[error("lifetime too long")]
    LifetimeTooLong,
    /// Idempotency key was used for different input.
    #[error("idempotency conflict")]
    IdempotencyConflict,
    /// Session ended.
    #[error("session ended")]
    SessionEnded,
    /// Storage failed or contained malformed records.
    #[error("state store unavailable")]
    Store,
}
impl From<StoreError> for AuthorityError {
    fn from(_: StoreError) -> Self {
        Self::Store
    }
}
/// Client-supplied acquisition fields; authentication material is redacted.
#[derive(Clone)]
pub struct AcquireRequest {
    /// Requested route.
    pub route: RouteId,
    /// Requested session mode, defaulting to the current grant.
    pub mode: Option<Mode>,
    /// Requested absolute lifetime.
    pub lifetime_ms: Option<u64>,
    /// Audit-only isolation flag.
    pub isolated: bool,
    /// Audit-only label.
    pub label: Option<String>,
    /// Principal-local retry key.
    pub idempotency_key: String,
    /// Client-generated SCRAM verifier.
    pub verifier: Verifier,
    /// Client-generated secret digest.
    pub secret_sha256: [u8; 32],
}
impl fmt::Debug for AcquireRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Labels and retry keys are client input too; never echo them in diagnostics.
        f.debug_struct("AcquireRequest")
            .field("route", &self.route)
            .field("mode", &self.mode)
            .field("lifetime_ms", &self.lifetime_ms)
            .field("isolated", &self.isolated)
            .field("label", &"[REDACTED]")
            .field("idempotency_key", &"[REDACTED]")
            .field("verifier", &"[REDACTED]")
            .field("secret_sha256", &"[REDACTED]")
            .finish()
    }
}
#[derive(Default)]
struct View {
    principals: HashMap<PrincipalId, Principal>,
    credentials: HashMap<CredentialId, PrincipalCredential>,
    grants: HashMap<(PrincipalId, RouteId), Mode>,
    sessions: HashMap<SessionId, AccessSession>,
    session_credentials: HashMap<CredentialId, SessionCredential>,
    scopes: HashMap<ScopeId, TokenScope>,
    idempotency: HashMap<(PrincipalId, String), IdempotencyRecord>,
    pending: HashSet<SessionId>,
    flushed: HashMap<SessionId, u64>,
}
/// Authoritative memory view with serialized write-through mutations.
pub struct Authority {
    store: Arc<dyn Store>,
    limits: SessionLimits,
    view: RwLock<View>,
    // This mutex coordinates writers only; admission never takes it.
    writer: Mutex<()>,
    hook: RwLock<Option<Arc<dyn RevocationHook>>>,
}
fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}
fn hash<T: serde::Serialize>(value: &T) -> [u8; 32] {
    Sha256::digest(encode(value)).into()
}
fn end(view: &mut View, id: SessionId, reason: EndReason, now: u64) {
    if let Some(s) = view.sessions.get_mut(&id)
        && s.state == SessionState::Active
    {
        s.state = if matches!(reason, EndReason::Expired | EndReason::IdleTimeout) {
            SessionState::Expired
        } else {
            SessionState::Revoked
        };
        s.revoke_reason = Some(reason);
        s.ended_ms = Some(now);
        view.pending.insert(id);
    }
}
fn end_principal(view: &mut View, id: PrincipalId, reason: EndReason, now: u64) {
    let ids: Vec<_> = view
        .sessions
        .values()
        .filter(|s| s.principal == id)
        .map(|s| s.id)
        .collect();
    for sid in ids {
        end(view, sid, reason, now);
    }
}
fn secret_matches(key: &[u8; 32], digest: &[u8; 32]) -> bool {
    let Ok(mut candidate) = Hmac::<Sha256>::new_from_slice(key) else {
        return false;
    };
    candidate.update(digest);
    let tag = candidate.finalize().into_bytes();
    let Ok(mut stored) = Hmac::<Sha256>::new_from_slice(key) else {
        return false;
    };
    stored.update(key);
    stored.verify_slice(&tag).is_ok()
}
impl Authority {
    /// Load and validate every application table before accepting requests.
    pub fn load(store: Arc<dyn Store>, limits: SessionLimits) -> Result<Arc<Self>, AuthorityError> {
        let mut v = View::default();
        {
            let read = store.read()?;
            for table in Table::ALL {
                for (key, bytes) in read.scan_prefix(table, b"")? {
                    let bad = || AuthorityError::Store;
                    match table {
                        Table::Principals => {
                            let r: Principal = decode(&bytes).map_err(|_| bad())?;
                            if key != keys::principal(r.id)
                                || !valid_name(&r.name)
                                || v.principals.values().any(|p| p.name == r.name)
                            {
                                return Err(bad());
                            }
                            v.principals.insert(r.id, r);
                        }
                        Table::PrincipalCredentials => {
                            let r: PrincipalCredential = decode(&bytes).map_err(|_| bad())?;
                            if key != keys::principal_credential(r.principal, r.id)
                                || v.credentials.insert(r.id, r).is_some()
                            {
                                return Err(bad());
                            }
                        }
                        Table::Grants => {
                            let r: Grant = decode(&bytes).map_err(|_| bad())?;
                            if key != keys::grant(r.principal, &r.route) {
                                return Err(bad());
                            }
                            v.grants.insert((r.principal, r.route), r.mode);
                        }
                        Table::Sessions => {
                            let r: AccessSession = decode(&bytes).map_err(|_| bad())?;
                            if key != keys::session(r.id)
                                || r.created_ms > r.expires_ms
                                || r.last_used_ms < r.created_ms
                                || (r.state == SessionState::Active)
                                    != (r.revoke_reason.is_none() && r.ended_ms.is_none())
                                || (r.state != SessionState::Active
                                    && (r.revoke_reason.is_none() || r.ended_ms.is_none()))
                            {
                                return Err(bad());
                            }
                            v.flushed.insert(r.id, r.last_used_ms);
                            v.sessions.insert(r.id, r);
                        }
                        Table::SessionCredentials => {
                            let r: SessionCredential = decode(&bytes).map_err(|_| bad())?;
                            if key != keys::session_credential(r.session, r.id)
                                || v.session_credentials.insert(r.id, r).is_some()
                            {
                                return Err(bad());
                            }
                        }
                        Table::Scopes => {
                            let r: TokenScope = decode(&bytes).map_err(|_| bad())?;
                            if key != keys::scope(r.id)
                                || r.derivation_version != 2
                                || r.codec_version != 1
                                || r.deployment_id != store.meta().deployment_id
                            {
                                return Err(bad());
                            }
                            v.scopes.insert(r.id, r);
                        }
                        Table::Idempotency => {
                            let r: IdempotencyRecord = decode(&bytes).map_err(|_| bad())?;
                            if key != keys::idempotency(r.principal, &r.key)
                                || SessionId::from_hex(&r.result).is_none()
                            {
                                return Err(bad());
                            }
                            v.idempotency.insert((r.principal, r.key.clone()), r);
                        }
                        Table::SessionCounters | Table::PrincipalCounters => {
                            if table == Table::PrincipalCounters {
                                counters::decode_principal_count(&bytes).map_err(|_| bad())?;
                            } else {
                                decode_count(&bytes).map_err(|_| bad())?;
                            }
                            let suffix = if table == Table::PrincipalCounters {
                                8
                            } else {
                                0
                            };
                            if key.len() < 18 + suffix {
                                return Err(bad());
                            }
                            let len = u16::from_be_bytes([key[16], key[17]]) as usize;
                            if key.len() != 18 + len + suffix
                                || std::str::from_utf8(&key[18..18 + len]).is_err()
                            {
                                return Err(bad());
                            }
                        }
                    }
                }
            }
        }
        if v.credentials
            .values()
            .any(|c| !v.principals.contains_key(&c.principal))
            || v.grants.keys().any(|(p, _)| !v.principals.contains_key(p))
            || v.sessions.values().any(|s| {
                !v.principals.contains_key(&s.principal)
                    || v.scopes
                        .get(&s.scope)
                        .is_none_or(|scope| scope.route != s.route)
            })
            || v.session_credentials.values().any(|c| {
                v.sessions
                    .get(&c.session)
                    .is_none_or(|s| c.expires_ms > s.expires_ms)
            })
        {
            return Err(AuthorityError::Store);
        }
        Ok(Arc::new(Self {
            store,
            limits,
            view: RwLock::new(v),
            writer: Mutex::new(()),
            hook: RwLock::new(None),
        }))
    }
    /// Install the post-commit termination hook.
    pub fn set_hook(&self, hook: Arc<dyn RevocationHook>) {
        *self.hook.write().unwrap() = Some(hook);
    }
    fn notify(&self, rows: &[AccessSession]) {
        let hook = self.hook.read().unwrap().clone();
        if let Some(hook) = hook {
            for s in rows {
                if let Some(reason) = s.revoke_reason {
                    hook.session_ended(s.id, s.principal, reason);
                }
            }
        }
    }
    fn api_principal(&self, id: PrincipalId) -> Result<Principal, AuthorityError> {
        let p = self.principal(id).ok_or(AuthorityError::NotFound)?;
        if p.source == PrincipalSource::Config {
            return Err(AuthorityError::ConfigManaged);
        }
        Ok(p)
    }
    /// Create an API principal, retrying by a global creation key.
    pub fn create_principal(
        &self,
        name: &str,
        kind: PrincipalKind,
        admin: bool,
        idem_key: &str,
    ) -> Result<Principal, AuthorityError> {
        let _writer = self.writer.lock().unwrap();
        if !valid_name(name) {
            return Err(AuthorityError::NameInvalid);
        }
        let key = format!("\0principal:{idem_key}");
        let owner = PrincipalId([0; 16]);
        let request_hash = hash(&("create_principal", name, kind, admin));
        {
            let v = self.view.read().unwrap();
            if let Some(r) = v.idempotency.get(&(owner, key.clone())) {
                if r.request_hash != request_hash {
                    return Err(AuthorityError::IdempotencyConflict);
                }
                return PrincipalId::from_hex(&r.result)
                    .and_then(|id| v.principals.get(&id).cloned())
                    .ok_or(AuthorityError::Store);
            }
            if v.principals.values().any(|p| p.name == name) {
                return Err(AuthorityError::NameTaken);
            }
        }
        let p = Principal {
            id: PrincipalId::random(),
            name: name.into(),
            kind,
            state: PrincipalState::Enabled,
            source: PrincipalSource::Api,
            administrator: admin,
            revision: 1,
        };
        let r = IdempotencyRecord {
            principal: owner,
            key: key.clone(),
            request_hash,
            result: p.id.to_hex(),
        };
        let mut tx = self.store.write()?;
        tx.put(Table::Principals, &keys::principal(p.id), &encode(&p))?;
        tx.put(
            Table::Idempotency,
            &keys::idempotency(owner, &key),
            &encode(&r),
        )?;
        tx.commit()?;
        let mut v = self.view.write().unwrap();
        v.principals.insert(p.id, p.clone());
        v.idempotency.insert((owner, key), r);
        Ok(p)
    }
    /// Enable or disable a principal, atomically persisting all affected sessions.
    pub fn set_principal_enabled(
        &self,
        id: PrincipalId,
        enabled: bool,
    ) -> Result<(), AuthorityError> {
        let writer = self.writer.lock().unwrap();
        let mut p = self.api_principal(id)?;
        p.state = if enabled {
            PrincipalState::Enabled
        } else {
            PrincipalState::Disabled
        };
        p.revision = p.revision.saturating_add(1);
        let rows = {
            let mut v = self.view.write().unwrap();
            if !enabled {
                v.principals.insert(id, p.clone());
                end_principal(
                    &mut v,
                    id,
                    EndReason::PrincipalDisabled,
                    self.store.now_ms(),
                );
            }
            v.pending
                .iter()
                .filter_map(|sid| v.sessions.get(sid))
                .filter(|s| s.principal == id)
                .cloned()
                .collect::<Vec<_>>()
        };
        let mut tx = self.store.write()?;
        tx.put(Table::Principals, &keys::principal(id), &encode(&p))?;
        for s in &rows {
            let _: AccessSession = decode(
                &tx.get(Table::Sessions, &keys::session(s.id))?
                    .ok_or(AuthorityError::Store)?,
            )
            .map_err(|_| AuthorityError::Store)?;
            tx.put(Table::Sessions, &keys::session(s.id), &encode(s))?;
        }
        tx.commit()?;
        {
            let mut v = self.view.write().unwrap();
            v.principals.insert(id, p);
            for s in &rows {
                v.pending.remove(&s.id);
            }
        }
        drop(writer);
        self.notify(&rows);
        Ok(())
    }
    /// Add an authentication credential to an API principal.
    pub fn add_principal_credential(
        &self,
        id: PrincipalId,
        verifier: Verifier,
        expires_unix_ms: Option<u64>,
    ) -> Result<CredentialId, AuthorityError> {
        let _writer = self.writer.lock().unwrap();
        self.api_principal(id)?;
        let c = PrincipalCredential {
            id: CredentialId::random(),
            principal: id,
            verifier,
            state: CredentialState::Active,
            expires_ms: expires_unix_ms,
        };
        let mut tx = self.store.write()?;
        tx.put(
            Table::PrincipalCredentials,
            &keys::principal_credential(id, c.id),
            &encode(&c),
        )?;
        tx.commit()?;
        let cid = c.id;
        self.view.write().unwrap().credentials.insert(cid, c);
        Ok(cid)
    }
    /// Revoke an API principal's credential.
    pub fn revoke_principal_credential(
        &self,
        id: PrincipalId,
        cid: CredentialId,
    ) -> Result<(), AuthorityError> {
        let _writer = self.writer.lock().unwrap();
        self.api_principal(id)?;
        let mut c = self
            .view
            .read()
            .unwrap()
            .credentials
            .get(&cid)
            .filter(|c| c.principal == id)
            .cloned()
            .ok_or(AuthorityError::NotFound)?;
        c.state = CredentialState::Revoked;
        // Deny authentication before the durable write, including on failure.
        self.view
            .write()
            .unwrap()
            .credentials
            .insert(cid, c.clone());
        let mut tx = self.store.write()?;
        tx.put(
            Table::PrincipalCredentials,
            &keys::principal_credential(id, cid),
            &encode(&c),
        )?;
        tx.commit()?;
        Ok(())
    }
    /// Replace all route grants, including for configuration principals.
    pub fn set_grants(
        &self,
        id: PrincipalId,
        grants: Vec<(RouteId, Mode)>,
    ) -> Result<(), AuthorityError> {
        let writer = self.writer.lock().unwrap();
        self.principal(id).ok_or(AuthorityError::NotFound)?;
        let grants: HashMap<_, _> = grants.into_iter().collect();
        let rows = {
            let mut v = self.view.write().unwrap();
            // Restrict existing grants now; additions and widenings require durability.
            v.grants.retain(|(p, route), mode| {
                if *p != id {
                    return true;
                }
                if let Some(new) = grants.get(route) {
                    *mode = mode.min(*new);
                    true
                } else {
                    false
                }
            });
            let active: Vec<_> = v
                .sessions
                .values()
                .filter(|s| s.principal == id && s.state == SessionState::Active)
                .map(|s| s.id)
                .collect();
            for sid in active {
                let route = v.sessions[&sid].route.clone();
                if let Some(mode) = grants.get(&route) {
                    let session = v.sessions.get_mut(&sid).ok_or(AuthorityError::Store)?;
                    session.mode = session.mode.min(*mode);
                } else {
                    end(&mut v, sid, EndReason::GrantRemoved, self.store.now_ms());
                }
            }
            // Include active rows on retries: a failed reduction already narrowed memory.
            v.sessions
                .values()
                .filter(|s| {
                    s.principal == id
                        && (s.state == SessionState::Active || v.pending.contains(&s.id))
                })
                .cloned()
                .collect::<Vec<_>>()
        };
        let mut tx = self.store.write()?;
        // Durable rows, rather than memory, retain routes removed by a failed attempt.
        for (key, _) in tx.scan_prefix(Table::Grants, &keys::principal(id))? {
            tx.delete(Table::Grants, &key)?;
        }
        for (route, mode) in &grants {
            tx.put(
                Table::Grants,
                &keys::grant(id, route),
                &encode(&Grant {
                    principal: id,
                    route: route.clone(),
                    mode: *mode,
                }),
            )?;
        }
        let mut written = Vec::new();
        for s in &rows {
            let stored: AccessSession = decode(
                &tx.get(Table::Sessions, &keys::session(s.id))?
                    .ok_or(AuthorityError::Store)?,
            )
            .map_err(|_| AuthorityError::Store)?;
            if s.state == SessionState::Active
                && (stored.state != SessionState::Active || stored.mode == s.mode)
            {
                continue;
            }
            tx.put(Table::Sessions, &keys::session(s.id), &encode(s))?;
            written.push(s.clone());
        }
        tx.commit()?;
        {
            let mut v = self.view.write().unwrap();
            v.grants.retain(|(p, _), _| *p != id);
            for (route, mode) in grants {
                v.grants.insert((id, route), mode);
            }
            for s in &written {
                // Admission may have ended an active snapshot during the transaction.
                if s.state != SessionState::Active {
                    v.pending.remove(&s.id);
                }
                v.flushed.insert(s.id, s.last_used_ms);
            }
        }
        let mut ended: Vec<_> = written
            .into_iter()
            .filter(|s| s.state != SessionState::Active)
            .collect();
        ended.extend(self.drain_pending_best_effort());
        drop(writer);
        self.notify(&ended);
        Ok(())
    }
    /// Look up a principal by opaque identifier.
    pub fn principal(&self, id: PrincipalId) -> Option<Principal> {
        self.view.read().unwrap().principals.get(&id).cloned()
    }
    /// Look up a principal by its unique name.
    pub fn principal_by_name(&self, name: &str) -> Option<Principal> {
        self.view
            .read()
            .unwrap()
            .principals
            .values()
            .find(|p| p.name == name)
            .cloned()
    }
    /// Return active unexpired verifiers for an enabled principal.
    pub fn principal_credential_verifiers(&self, id: PrincipalId) -> Vec<Verifier> {
        let v = self.view.read().unwrap();
        let now = self.store.now_ms();
        if v.principals
            .get(&id)
            .is_none_or(|p| p.state != PrincipalState::Enabled)
        {
            return vec![];
        }
        v.credentials
            .values()
            .filter(|c| {
                c.principal == id
                    && c.state == CredentialState::Active
                    && c.expires_ms.is_none_or(|e| now < e)
            })
            .map(|c| c.verifier.clone())
            .collect()
    }
    fn grant(&self, principal: PrincipalId, route: &RouteId) -> Result<Mode, AuthorityError> {
        let v = self.view.read().unwrap();
        let p = v
            .principals
            .get(&principal)
            .ok_or(AuthorityError::NotFound)?;
        if p.state != PrincipalState::Enabled {
            return Err(AuthorityError::PrincipalDisabled);
        }
        v.grants
            .get(&(principal, route.clone()))
            .copied()
            .ok_or(AuthorityError::NoGrant)
    }
    /// Acquire a new session and scope or return the identical acquisition retry.
    pub fn acquire_session(
        &self,
        principal: PrincipalId,
        req: AcquireRequest,
    ) -> Result<AccessSession, AuthorityError> {
        let _writer = self.writer.lock().unwrap();
        let owner = self.principal(principal).ok_or(AuthorityError::NotFound)?;
        if owner.state != PrincipalState::Enabled {
            return Err(AuthorityError::PrincipalDisabled);
        }
        let request_hash = hash(&(
            "acquire_session",
            &req.route,
            req.mode,
            req.lifetime_ms,
            req.isolated,
            &req.label,
            &req.idempotency_key,
            &req.verifier,
            req.secret_sha256,
        ));
        {
            let v = self.view.read().unwrap();
            if let Some(r) = v.idempotency.get(&(principal, req.idempotency_key.clone())) {
                if r.request_hash != request_hash {
                    return Err(AuthorityError::IdempotencyConflict);
                }
                return SessionId::from_hex(&r.result)
                    .and_then(|id| v.sessions.get(&id).cloned())
                    .filter(|s| {
                        s.state == SessionState::Active && self.store.now_ms() < s.expires_ms
                    })
                    .ok_or(AuthorityError::SessionEnded);
            }
        }
        let grant = self.grant(principal, &req.route)?;
        let mode = req.mode.unwrap_or(grant);
        if mode.min(grant) != mode {
            return Err(AuthorityError::ModeExceedsGrant);
        }
        let lifetime = req.lifetime_ms.unwrap_or(self.limits.max_lifetime_ms);
        if lifetime > self.limits.max_lifetime_ms {
            return Err(AuthorityError::LifetimeTooLong);
        }
        let now = self.store.now_ms();
        let expires = now
            .checked_add(lifetime)
            .ok_or(AuthorityError::LifetimeTooLong)?;
        let mut salt = [0; 32];
        getrandom::fill(&mut salt).map_err(|_| AuthorityError::Store)?;
        let scope = TokenScope {
            id: ScopeId::random(),
            salt,
            derivation_version: 2,
            codec_version: 1,
            deployment_id: self.store.meta().deployment_id,
            route: req.route.clone(),
        };
        let s = AccessSession {
            id: SessionId::random(),
            principal,
            route: req.route,
            mode,
            scope: scope.id,
            created_ms: now,
            expires_ms: expires,
            idle_timeout_ms: self.limits.idle_timeout_ms,
            last_used_ms: now,
            state: SessionState::Active,
            revoke_reason: None,
            ended_ms: None,
            label: req.label,
            isolated: req.isolated,
        };
        let c = SessionCredential {
            id: CredentialId::random(),
            session: s.id,
            verifier: req.verifier,
            secret_hash: req.secret_sha256,
            expires_ms: expires,
            state: CredentialState::Active,
        };
        let r = IdempotencyRecord {
            principal,
            key: req.idempotency_key,
            request_hash,
            result: s.id.to_hex(),
        };
        let mut tx = self.store.write()?;
        tx.put(Table::Scopes, &keys::scope(scope.id), &encode(&scope))?;
        tx.put(Table::Sessions, &keys::session(s.id), &encode(&s))?;
        tx.put(
            Table::SessionCredentials,
            &keys::session_credential(s.id, c.id),
            &encode(&c),
        )?;
        tx.put(
            Table::Idempotency,
            &keys::idempotency(principal, &r.key),
            &encode(&r),
        )?;
        tx.commit()?;
        let mut v = self.view.write().unwrap();
        v.scopes.insert(scope.id, scope);
        v.session_credentials.insert(c.id, c);
        v.idempotency.insert((principal, r.key.clone()), r);
        v.flushed.insert(s.id, now);
        v.sessions.insert(s.id, s.clone());
        Ok(s)
    }
    fn owned_session(
        &self,
        principal: PrincipalId,
        id: SessionId,
    ) -> Result<AccessSession, AuthorityError> {
        self.session(id)
            .filter(|s| s.principal == principal)
            .ok_or(AuthorityError::NotFound)
    }
    /// Resume an existing admissible session without creating records.
    pub fn resume(
        &self,
        principal: PrincipalId,
        id: SessionId,
    ) -> Result<AccessSession, AuthorityError> {
        self.owned_session(principal, id)?;
        self.admit(id, Operation::Read)
            .map_err(|_| AuthorityError::SessionEnded)?;
        self.owned_session(principal, id)
    }
    /// Explicitly renew mode or credentials without extending absolute expiry.
    pub fn renew(
        &self,
        principal: PrincipalId,
        id: SessionId,
        mode: Option<Mode>,
        add_credential: Option<(Verifier, [u8; 32])>,
    ) -> Result<AccessSession, AuthorityError> {
        let writer = self.writer.lock().unwrap();
        let mut ended = Vec::new();
        let result = (|| {
            let mut s = self.owned_session(principal, id)?;
            self.admit(id, Operation::Read)
                .map_err(|_| AuthorityError::SessionEnded)?;
            let grant = self.grant(principal, &s.route)?;
            if let Some(mode) = mode {
                if mode.min(grant) != mode {
                    return Err(AuthorityError::ModeExceedsGrant);
                }
                s.mode = mode;
            }
            s.last_used_ms = self.store.now_ms().max(s.last_used_ms);
            let credential = add_credential.map(|(verifier, secret_hash)| SessionCredential {
                id: CredentialId::random(),
                session: id,
                verifier,
                secret_hash,
                expires_ms: s.expires_ms,
                state: CredentialState::Active,
            });
            // Keep admission from ending the session while the transaction is built.
            let v = self.view.read().unwrap();
            let current = v.sessions.get(&id).ok_or(AuthorityError::NotFound)?;
            if current.state != SessionState::Active {
                return Err(AuthorityError::SessionEnded);
            }
            s.last_used_ms = s.last_used_ms.max(current.last_used_ms);
            let pending: Vec<_> = v
                .pending
                .iter()
                .filter_map(|id| v.sessions.get(id).cloned())
                .collect();
            let persisted_last_used = s.last_used_ms;
            let mut tx = self.store.write()?;
            let stored: AccessSession = tx
                .get(Table::Sessions, &keys::session(id))?
                .ok_or(AuthorityError::SessionEnded)
                .and_then(|b| decode(&b).map_err(|_| AuthorityError::Store))?;
            if stored.state != SessionState::Active || self.store.now_ms() >= stored.expires_ms {
                return Err(AuthorityError::SessionEnded);
            }
            tx.put(Table::Sessions, &keys::session(id), &encode(&s))?;
            for row in &pending {
                let _: AccessSession = decode(
                    &tx.get(Table::Sessions, &keys::session(row.id))?
                        .ok_or(AuthorityError::Store)?,
                )
                .map_err(|_| AuthorityError::Store)?;
                tx.put(Table::Sessions, &keys::session(row.id), &encode(row))?;
            }
            if let Some(c) = &credential {
                tx.put(
                    Table::SessionCredentials,
                    &keys::session_credential(id, c.id),
                    &encode(c),
                )?;
            }
            drop(v);
            tx.commit()?;
            let mut v = self.view.write().unwrap();
            for row in &pending {
                v.pending.remove(&row.id);
                v.flushed.insert(row.id, row.last_used_ms);
            }
            ended.extend(pending);
            // Admission can end the session or advance last_used during I/O. Never resurrect it.
            let current = v.sessions.get_mut(&id).ok_or(AuthorityError::NotFound)?;
            if current.state != SessionState::Active {
                return Err(AuthorityError::SessionEnded);
            }
            current.mode = s.mode;
            current.last_used_ms = current.last_used_ms.max(s.last_used_ms);
            s = current.clone();
            v.flushed.insert(id, persisted_last_used);
            if let Some(c) = credential {
                v.session_credentials.insert(c.id, c);
            }
            Ok(s)
        })();
        ended.extend(self.drain_pending_best_effort());
        drop(writer);
        self.notify(&ended);
        result
    }
    /// Deny first, persist second, and notify only after a successful commit.
    pub fn revoke(&self, id: SessionId, reason: EndReason) -> Result<(), AuthorityError> {
        let writer = self.writer.lock().unwrap();
        let rows = {
            let mut v = self.view.write().unwrap();
            if !v.sessions.contains_key(&id) {
                return Err(AuthorityError::NotFound);
            }
            end(&mut v, id, reason, self.store.now_ms());
            if !v.pending.contains(&id) {
                return Ok(());
            }
            vec![v.sessions[&id].clone()]
        };
        self.persist_ended(&rows)?;
        drop(writer);
        self.notify(&rows);
        Ok(())
    }
    fn persist_ended(&self, rows: &[AccessSession]) -> Result<(), AuthorityError> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut tx = self.store.write()?;
        for s in rows {
            let _: AccessSession = decode(
                &tx.get(Table::Sessions, &keys::session(s.id))?
                    .ok_or(AuthorityError::Store)?,
            )
            .map_err(|_| AuthorityError::Store)?;
            tx.put(Table::Sessions, &keys::session(s.id), &encode(s))?;
        }
        tx.commit()?;
        let mut v = self.view.write().unwrap();
        for s in rows {
            v.pending.remove(&s.id);
            v.flushed.insert(s.id, s.last_used_ms);
        }
        Ok(())
    }
    fn persist_pending(&self) -> Result<Vec<AccessSession>, AuthorityError> {
        let mut persisted = Vec::new();
        loop {
            let rows: Vec<_> = {
                let v = self.view.read().unwrap();
                v.pending
                    .iter()
                    .filter_map(|id| v.sessions.get(id).cloned())
                    .collect()
            };
            if rows.is_empty() {
                return Ok(persisted);
            }
            self.persist_ended(&rows)?;
            persisted.extend(rows);
        }
    }
    // Preserve the caller's result and all notifications even if a later batch fails.
    fn drain_pending_best_effort(&self) -> Vec<AccessSession> {
        let mut persisted = Vec::new();
        loop {
            let rows: Vec<_> = {
                let v = self.view.read().unwrap();
                v.pending
                    .iter()
                    .filter_map(|id| v.sessions.get(id).cloned())
                    .collect()
            };
            if rows.is_empty() || self.persist_ended(&rows).is_err() {
                return persisted;
            }
            persisted.extend(rows);
        }
    }
    /// Admit using only memory and the injected clock, queuing expiry persistence.
    pub fn admit(&self, id: SessionId, op: Operation) -> Result<Admitted, AdmitError> {
        let now = self.store.now_ms();
        let mut v = self.view.write().unwrap();
        Self::admit_view(&mut v, id, op, now)
    }
    fn admit_view(
        v: &mut View,
        id: SessionId,
        op: Operation,
        now: u64,
    ) -> Result<Admitted, AdmitError> {
        let s = v.sessions.get(&id).cloned().ok_or(AdmitError::Unknown)?;
        if s.state != SessionState::Active {
            return Err(AdmitError::Ended);
        }
        let reason = if now >= s.expires_ms {
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
            end(v, id, reason, now);
            return Err(AdmitError::Ended);
        }
        if v.principals
            .get(&s.principal)
            .is_none_or(|p| p.state != PrincipalState::Enabled)
        {
            return Err(AdmitError::Ended);
        }
        let grant = v
            .grants
            .get(&(s.principal, s.route.clone()))
            .ok_or(AdmitError::Ended)?;
        let mode = s.mode.min(*grant);
        if matches!(op, Operation::Write) && !mode.permits_write() {
            return Err(AdmitError::ReadOnly);
        }
        if let Some(current) = v.sessions.get_mut(&id) {
            current.last_used_ms = current.last_used_ms.max(now);
        }
        Ok(Admitted {
            principal: s.principal,
            route: s.route,
            mode,
            scope: s.scope,
        })
    }
    /// Verify a secret digest in constant time for an admissible session.
    pub fn check_session_secret(&self, id: SessionId, secret: &[u8]) -> bool {
        let now = self.store.now_ms();
        let digest: [u8; 32] = Sha256::digest(secret).into();
        let mut v = self.view.write().unwrap();
        // Refused authentication must not refresh an idle session.
        let previous = v.sessions.get(&id).map(|s| s.last_used_ms);
        if Self::admit_view(&mut v, id, Operation::Read, now).is_err() {
            let _ = secret_matches(&[0; 32], &digest);
            return false;
        }
        let mut matched = false;
        for c in v
            .session_credentials
            .values()
            .filter(|c| c.session == id && c.state == CredentialState::Active && now < c.expires_ms)
        {
            matched |= secret_matches(&c.secret_hash, &digest);
        }
        if !matched && let (Some(last), Some(s)) = (previous, v.sessions.get_mut(&id)) {
            s.last_used_ms = last;
        }
        matched
    }
    /// Read a session snapshot.
    pub fn session(&self, id: SessionId) -> Option<AccessSession> {
        self.view.read().unwrap().sessions.get(&id).cloned()
    }
    /// Read a token scope snapshot.
    pub fn scope(&self, id: ScopeId) -> Option<TokenScope> {
        self.view.read().unwrap().scopes.get(&id).cloned()
    }
    /// List a principal's sessions, including tombstones.
    pub fn sessions_of(&self, id: PrincipalId) -> Vec<AccessSession> {
        self.view
            .read()
            .unwrap()
            .sessions
            .values()
            .filter(|s| s.principal == id)
            .cloned()
            .collect()
    }
    /// Persist pending endings and detect absolute or idle expiry.
    pub fn sweep(&self) -> Result<usize, AuthorityError> {
        let writer = self.writer.lock().unwrap();
        let now = self.store.now_ms();
        let rows = {
            let mut v = self.view.write().unwrap();
            let ids: Vec<_> = v
                .sessions
                .values()
                .filter(|s| s.state == SessionState::Active)
                .filter_map(|s| {
                    if now >= s.expires_ms {
                        Some((s.id, EndReason::Expired))
                    } else if s
                        .idle_timeout_ms
                        .is_some_and(|idle| now.saturating_sub(s.last_used_ms) > idle)
                    {
                        Some((s.id, EndReason::IdleTimeout))
                    } else {
                        None
                    }
                })
                .collect();
            for (id, reason) in ids {
                end(&mut v, id, reason, now);
            }
            v.pending
                .iter()
                .filter_map(|id| v.sessions.get(id).cloned())
                .collect::<Vec<_>>()
        };
        self.persist_ended(&rows)?;
        drop(writer);
        self.notify(&rows);
        Ok(rows.len())
    }
    /// Persist activity timestamps after at least one minute of movement.
    pub fn flush_last_used(&self) -> Result<(), AuthorityError> {
        let writer = self.writer.lock().unwrap();
        let rows = {
            let v = self.view.read().unwrap();
            v.sessions
                .values()
                .filter(|s| {
                    s.state == SessionState::Active
                        && s.last_used_ms
                            .saturating_sub(*v.flushed.get(&s.id).unwrap_or(&s.created_ms))
                            >= 60_000
                })
                .cloned()
                .collect::<Vec<_>>()
        };
        if !rows.is_empty() {
            let mut tx = self.store.write()?;
            for s in &rows {
                let stored: AccessSession = decode(
                    &tx.get(Table::Sessions, &keys::session(s.id))?
                        .ok_or(AuthorityError::Store)?,
                )
                .map_err(|_| AuthorityError::Store)?;
                if stored.state == SessionState::Active && self.store.now_ms() < stored.expires_ms {
                    tx.put(Table::Sessions, &keys::session(s.id), &encode(s))?;
                }
            }
            tx.commit()?;
            let mut v = self.view.write().unwrap();
            for s in rows {
                v.flushed.insert(s.id, s.last_used_ms);
            }
        }
        let ended = self.persist_pending()?;
        drop(writer);
        self.notify(&ended);
        Ok(())
    }
    /// Delete retained tombstones, their credentials and session counters.
    pub fn purge_tombstones(&self, retention_ms: u64) -> Result<usize, AuthorityError> {
        let _writer = self.writer.lock().unwrap();
        let now = self.store.now_ms();
        let ids = {
            let v = self.view.read().unwrap();
            v.sessions
                .values()
                .filter(|s| {
                    s.state != SessionState::Active
                        && !v.pending.contains(&s.id)
                        && s.expires_ms
                            .checked_add(retention_ms)
                            .is_some_and(|e| e < now)
                })
                .map(|s| s.id)
                .collect::<Vec<_>>()
        };
        if ids.is_empty() {
            return Ok(0);
        }
        let mut tx = self.store.write()?;
        for id in &ids {
            tx.delete(Table::Sessions, &keys::session(*id))?;
            for table in [Table::SessionCredentials, Table::SessionCounters] {
                for (key, _) in tx.scan_prefix(table, &id.0)? {
                    tx.delete(table, &key)?;
                }
            }
        }
        tx.commit()?;
        let mut v = self.view.write().unwrap();
        for id in &ids {
            v.sessions.remove(id);
            v.flushed.remove(id);
            v.session_credentials.retain(|_, c| c.session != *id);
        }
        Ok(ids.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{pgwire::auth::ScramVerifier, state::store::RedbStore, token::keys::RootKey};
    use std::{
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
    };

    pub(super) struct TestDir(pub(super) PathBuf);
    impl TestDir {
        fn new() -> Self {
            let path = PathBuf::from(std::env::var_os("TEMP").unwrap())
                .join(format!("authority-{}", SessionId::random().to_hex()));
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }
    impl Drop for TestDir {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.0).unwrap();
        }
    }
    #[derive(Default)]
    pub(super) struct Hook(pub(super) Mutex<Vec<(SessionId, PrincipalId, EndReason)>>);
    impl RevocationHook for Hook {
        fn session_ended(&self, s: SessionId, p: PrincipalId, r: EndReason) {
            self.0.lock().unwrap().push((s, p, r));
        }
    }
    pub(super) struct Fixture {
        pub(super) a: Arc<Authority>,
        pub(super) store: Arc<RedbStore>,
        pub(super) clock: Arc<AtomicU64>,
        pub(super) hook: Arc<Hook>,
        pub(super) dir: TestDir,
    }
    pub(super) fn root() -> RootKey {
        RootKey::from_hex(&"01".repeat(32)).unwrap()
    }
    pub(super) fn verifier() -> Verifier {
        Verifier(ScramVerifier::from_password(
            "test-only-password",
            b"test-only-salt",
            1,
        ))
    }
    pub(super) fn route() -> RouteId {
        RouteId::new("postgres").unwrap()
    }
    pub(super) fn request(key: &str) -> AcquireRequest {
        AcquireRequest {
            route: route(),
            mode: None,
            lifetime_ms: None,
            isolated: false,
            label: Some("audit label".into()),
            idempotency_key: key.into(),
            verifier: verifier(),
            secret_sha256: Sha256::digest(b"session-secret").into(),
        }
    }
    impl Fixture {
        pub(super) fn new() -> Self {
            Self::limits(SessionLimits::default())
        }
        pub(super) fn limits(limits: SessionLimits) -> Self {
            let dir = TestDir::new();
            let clock = Arc::new(AtomicU64::new(1000));
            let c = clock.clone();
            let store = Arc::new(
                RedbStore::with_clock(&dir.0, &root(), Arc::new(move || c.load(Ordering::SeqCst)))
                    .unwrap(),
            );
            let a = Authority::load(store.clone(), limits).unwrap();
            let hook = Arc::new(Hook::default());
            a.set_hook(hook.clone());
            Self {
                a,
                store,
                clock,
                hook,
                dir,
            }
        }
        pub(super) fn principal(&self, name: &str, mode: Mode) -> Principal {
            let p = self
                .a
                .create_principal(name, PrincipalKind::Agent, false, name)
                .unwrap();
            self.a.set_grants(p.id, vec![(route(), mode)]).unwrap();
            p
        }
        pub(super) fn acquire(&self, p: PrincipalId, key: &str) -> AccessSession {
            self.a.acquire_session(p, request(key)).unwrap()
        }
        pub(super) fn rows(&self) -> usize {
            self.store
                .read()
                .unwrap()
                .scan_prefix(Table::Sessions, b"")
                .unwrap()
                .len()
        }
        pub(super) fn stored(&self, id: SessionId) -> AccessSession {
            decode(
                &self
                    .store
                    .read()
                    .unwrap()
                    .get(Table::Sessions, &keys::session(id))
                    .unwrap()
                    .unwrap(),
            )
            .unwrap()
        }
    }
    // Inject actions during commit and failures in the following pending drain.
    struct CommitStore {
        inner: Arc<RedbStore>,
        after_commit: Mutex<Option<Box<dyn FnOnce() + Send>>>,
        fail_writes: std::sync::atomic::AtomicBool,
    }
    impl Store for CommitStore {
        fn meta(&self) -> &super::super::store::StoreMeta {
            self.inner.meta()
        }
        fn now_ms(&self) -> u64 {
            self.inner.now_ms()
        }
        fn read(&self) -> Result<Box<dyn super::super::store::ReadTxn + '_>, StoreError> {
            self.inner.read()
        }
        fn write(&self) -> Result<Box<dyn super::super::store::WriteTxn + '_>, StoreError> {
            if self.fail_writes.load(Ordering::SeqCst) {
                return Err(StoreError::Io);
            }
            Ok(Box::new(CommitTxn {
                inner: self.inner.write()?,
                store: self,
            }))
        }
    }
    struct CommitTxn<'a> {
        inner: Box<dyn super::super::store::WriteTxn + 'a>,
        store: &'a CommitStore,
    }
    impl super::super::store::ReadTxn for CommitTxn<'_> {
        fn get(&self, t: Table, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
            self.inner.get(t, key)
        }
        fn scan_prefix(
            &self,
            t: Table,
            prefix: &[u8],
        ) -> Result<super::super::store::ScanRows, StoreError> {
            self.inner.scan_prefix(t, prefix)
        }
    }
    impl super::super::store::WriteTxn for CommitTxn<'_> {
        fn put(&mut self, t: Table, key: &[u8], value: &[u8]) -> Result<(), StoreError> {
            self.inner.put(t, key, value)
        }
        fn delete(&mut self, t: Table, key: &[u8]) -> Result<bool, StoreError> {
            self.inner.delete(t, key)
        }
        fn commit(self: Box<Self>) -> Result<(), StoreError> {
            self.inner.commit()?;
            let action = self.store.after_commit.lock().unwrap().take();
            if let Some(action) = action {
                action();
            }
            Ok(())
        }
    }
    fn commit_store(f: &mut Fixture) -> Arc<CommitStore> {
        let store = Arc::new(CommitStore {
            inner: f.store.clone(),
            after_commit: Mutex::new(None),
            fail_writes: std::sync::atomic::AtomicBool::new(false),
        });
        f.a = Authority::load(store.clone(), SessionLimits::default()).unwrap();
        f.a.set_hook(f.hook.clone());
        store
    }
    #[test]
    fn ended_acquisition_replays_are_refused() {
        let f = Fixture::new();
        let p = f.principal("agent", Mode::ReadWrite);
        let s = f.acquire(p.id, "revoked");
        f.a.revoke(s.id, EndReason::Requested).unwrap();
        assert_eq!(
            f.a.acquire_session(p.id, request("revoked")).unwrap_err(),
            AuthorityError::SessionEnded
        );
        let mut req = request("expired");
        req.lifetime_ms = Some(1);
        f.a.acquire_session(p.id, req.clone()).unwrap();
        f.clock.store(1001, Ordering::SeqCst);
        assert_eq!(
            f.a.acquire_session(p.id, req).unwrap_err(),
            AuthorityError::SessionEnded
        );
    }
    #[test]
    fn renew_refuses_and_persists_memory_ended_session() {
        let f = Fixture::limits(SessionLimits {
            max_lifetime_ms: 1000,
            idle_timeout_ms: Some(10),
        });
        let p = f.principal("agent", Mode::ReadWrite);
        let s = f.acquire(p.id, "idle");
        f.clock.store(1011, Ordering::SeqCst);
        assert_eq!(
            f.a.admit(s.id, Operation::Read).unwrap_err(),
            AdmitError::Ended
        );
        assert_eq!(
            f.a.renew(p.id, s.id, None, None).unwrap_err(),
            AuthorityError::SessionEnded
        );
        assert_eq!(f.stored(s.id).revoke_reason, Some(EndReason::IdleTimeout));
        assert_eq!(f.hook.0.lock().unwrap().len(), 1);
    }
    #[test]
    fn renew_transaction_persists_other_pending_ending_and_failure_stays_closed() {
        for fail in [false, true] {
            let f = Fixture::limits(SessionLimits {
                max_lifetime_ms: 1000,
                idle_timeout_ms: Some(10),
            });
            let p = f.principal("agent", Mode::ReadWrite);
            let idle = f.acquire(p.id, "idle");
            f.clock.store(1005, Ordering::SeqCst);
            let active = f.acquire(p.id, "active");
            f.clock.store(1011, Ordering::SeqCst);
            assert_eq!(
                f.a.admit(idle.id, Operation::Read).unwrap_err(),
                AdmitError::Ended
            );
            f.store.fail_commits(fail);
            let result = f.a.renew(p.id, active.id, Some(Mode::ReadOnly), None);
            if fail {
                assert_eq!(result.unwrap_err(), AuthorityError::Store);
                assert_eq!(f.a.session(active.id).unwrap().mode, Mode::ReadWrite);
                assert_eq!(f.stored(idle.id).state, SessionState::Active);
                assert!(f.hook.0.lock().unwrap().is_empty());
            } else {
                assert_eq!(result.unwrap().mode, Mode::ReadOnly);
                assert_eq!(
                    f.stored(idle.id).revoke_reason,
                    Some(EndReason::IdleTimeout)
                );
                assert_eq!(f.hook.0.lock().unwrap().len(), 1);
                assert_eq!(f.a.sweep().unwrap(), 0);
            }
            assert_eq!(
                f.a.admit(idle.id, Operation::Read).unwrap_err(),
                AdmitError::Ended
            );
        }
    }
    #[test]
    fn idle_sweep_ends_without_admission_and_notifies_once() {
        let f = Fixture::limits(SessionLimits {
            max_lifetime_ms: 1000,
            idle_timeout_ms: Some(10),
        });
        let p = f.principal("agent", Mode::ReadWrite);
        let s = f.acquire(p.id, "idle");
        f.clock.store(1011, Ordering::SeqCst);
        assert_eq!(f.a.sweep().unwrap(), 1);
        assert_eq!(f.stored(s.id).state, SessionState::Expired);
        assert_eq!(f.stored(s.id).revoke_reason, Some(EndReason::IdleTimeout));
        assert_eq!(
            *f.hook.0.lock().unwrap(),
            vec![(s.id, p.id, EndReason::IdleTimeout)]
        );
        assert_eq!(f.a.sweep().unwrap(), 0);
        assert_eq!(f.hook.0.lock().unwrap().len(), 1);
    }
    #[test]
    fn renew_persists_session_ended_during_commit_before_returning() {
        let mut f = Fixture::new();
        let p = f.principal("agent", Mode::ReadWrite);
        let s = f.acquire(p.id, "active");
        let store = commit_store(&mut f);
        let a = f.a.clone();
        *store.after_commit.lock().unwrap() = Some(Box::new(move || {
            end(&mut a.view.write().unwrap(), s.id, EndReason::Expired, 1001);
        }));
        assert_eq!(
            f.a.renew(p.id, s.id, None, None).unwrap_err(),
            AuthorityError::SessionEnded
        );
        assert_eq!(f.stored(s.id).state, SessionState::Expired);
        assert_eq!(f.hook.0.lock().unwrap().len(), 1);
    }
    #[test]
    fn successful_renew_and_grants_keep_result_when_other_pending_drain_fails() {
        for grants in [false, true] {
            let mut f = Fixture::new();
            let p = f.principal("agent", Mode::ReadWrite);
            let s = f.acquire(p.id, "active");
            let other = f.acquire(p.id, "other");
            let store = commit_store(&mut f);
            let a = f.a.clone();
            let failing = store.clone();
            *store.after_commit.lock().unwrap() = Some(Box::new(move || {
                end(
                    &mut a.view.write().unwrap(),
                    other.id,
                    EndReason::Expired,
                    1001,
                );
                failing.fail_writes.store(true, Ordering::SeqCst);
            }));
            if grants {
                assert_eq!(
                    f.a.set_grants(p.id, vec![(route(), Mode::ReadOnly)]),
                    Ok(())
                );
            } else {
                assert!(f.a.renew(p.id, s.id, Some(Mode::ReadOnly), None).is_ok());
            }
            assert_eq!(f.stored(s.id).mode, Mode::ReadOnly);
            assert_eq!(f.stored(other.id).state, SessionState::Active);
            assert_eq!(
                f.a.admit(other.id, Operation::Read).unwrap_err(),
                AdmitError::Ended
            );
            assert!(f.hook.0.lock().unwrap().is_empty());
            store.fail_writes.store(false, Ordering::SeqCst);
            assert_eq!(f.a.sweep().unwrap(), 1);
            assert_eq!(f.stored(other.id).state, SessionState::Expired);
            assert_eq!(f.hook.0.lock().unwrap().len(), 1);
        }
    }
    #[test]
    fn acquisition_admission_and_limits() {
        let f = Fixture::new();
        let p = f.principal("agent", Mode::ReadWrite);
        let s = f.acquire(p.id, "rw");
        assert_eq!(s.mode, Mode::ReadWrite);
        let a = f.a.admit(s.id, Operation::Read).unwrap();
        assert_eq!(a.principal, p.id);
        assert_eq!(a.route, route());
        assert_eq!(a.scope, s.scope);
        assert_eq!(a.mode, Mode::ReadWrite);
        let mut r = request("ro");
        r.mode = Some(Mode::ReadOnly);
        let ro = f.a.acquire_session(p.id, r).unwrap();
        assert_eq!(
            f.a.admit(ro.id, Operation::Write).unwrap_err(),
            AdmitError::ReadOnly
        );
        assert_ne!(s.scope, ro.scope);
        assert_eq!(s.label, ro.label);
        f.a.set_grants(p.id, vec![(route(), Mode::ReadOnly)])
            .unwrap();
        let mut r = request("above");
        r.mode = Some(Mode::ReadWrite);
        assert_eq!(
            f.a.acquire_session(p.id, r).unwrap_err(),
            AuthorityError::ModeExceedsGrant
        );
        let mut r = request("long");
        r.lifetime_ms = Some(SessionLimits::default().max_lifetime_ms + 1);
        assert_eq!(
            f.a.acquire_session(p.id, r).unwrap_err(),
            AuthorityError::LifetimeTooLong
        );
        let ungranted =
            f.a.create_principal("ungranted", PrincipalKind::Person, false, "ungranted")
                .unwrap();
        assert_eq!(
            f.a.acquire_session(ungranted.id, request("none"))
                .unwrap_err(),
            AuthorityError::NoGrant
        );
    }
    #[test]
    fn failed_resume_never_creates_rows() {
        let f = Fixture::new();
        let p = f.principal("owner", Mode::ReadOnly);
        let other = f.principal("other", Mode::ReadWrite);
        let revoked = f.acquire(p.id, "revoked");
        f.a.revoke(revoked.id, EndReason::Requested).unwrap();
        let mut req = request("expired");
        req.lifetime_ms = Some(1);
        let expired = f.a.acquire_session(p.id, req).unwrap();
        let active = f.acquire(p.id, "active");
        let before = f.rows();
        assert_eq!(
            f.a.resume(p.id, SessionId::random()).unwrap_err(),
            AuthorityError::NotFound
        );
        assert_eq!(
            f.a.resume(other.id, active.id).unwrap_err(),
            AuthorityError::NotFound
        );
        assert_eq!(
            f.a.resume(p.id, revoked.id).unwrap_err(),
            AuthorityError::SessionEnded
        );
        f.clock.store(1001, Ordering::SeqCst);
        assert_eq!(
            f.a.resume(p.id, expired.id).unwrap_err(),
            AuthorityError::SessionEnded
        );
        assert_eq!(f.rows(), before);
        assert_eq!(f.a.sessions_of(p.id).len(), 3);
    }
    #[test]
    fn grant_reduction_removal_and_explicit_widening() {
        let f = Fixture::new();
        let p = f.principal("agent", Mode::ReadWrite);
        let rw = f.acquire(p.id, "rw");
        f.a.set_grants(p.id, vec![(route(), Mode::ReadOnly)])
            .unwrap();
        assert_eq!(
            f.a.admit(rw.id, Operation::Write).unwrap_err(),
            AdmitError::ReadOnly
        );
        let ro = f.acquire(p.id, "ro");
        f.a.set_grants(p.id, vec![(route(), Mode::ReadWrite)])
            .unwrap();
        assert_eq!(
            f.a.admit(ro.id, Operation::Write).unwrap_err(),
            AdmitError::ReadOnly
        );
        let renewed = f.a.renew(p.id, ro.id, Some(Mode::ReadWrite), None).unwrap();
        assert_eq!(renewed.expires_ms, ro.expires_ms);
        f.a.admit(ro.id, Operation::Write).unwrap();
        f.a.set_grants(p.id, vec![]).unwrap();
        assert_eq!(
            f.a.admit(ro.id, Operation::Read).unwrap_err(),
            AdmitError::Ended
        );
    }
    #[test]
    fn removed_grant_ends_durably_and_readding_never_revives() {
        let f = Fixture::new();
        let p = f.principal("agent", Mode::ReadWrite);
        let s = f.acquire(p.id, "rw");
        f.a.set_grants(p.id, vec![]).unwrap();
        for row in [f.a.session(s.id).unwrap(), f.stored(s.id)] {
            assert_eq!(row.state, SessionState::Revoked);
            assert_eq!(row.revoke_reason, Some(EndReason::GrantRemoved));
            assert_eq!(row.ended_ms, Some(1000));
        }
        assert_eq!(
            *f.hook.0.lock().unwrap(),
            vec![(s.id, p.id, EndReason::GrantRemoved)]
        );
        f.a.set_grants(p.id, vec![(route(), Mode::ReadWrite)])
            .unwrap();
        assert_eq!(
            f.a.admit(s.id, Operation::Read).unwrap_err(),
            AdmitError::Ended
        );
        assert_eq!(f.a.session(s.id).unwrap().state, SessionState::Revoked);
        assert_eq!(f.hook.0.lock().unwrap().len(), 1);
    }
    #[test]
    fn grant_reduction_is_sticky_and_other_route_is_untouched() {
        let f = Fixture::new();
        let p = f.principal("agent", Mode::ReadWrite);
        let other = RouteId::new("other").unwrap();
        f.a.set_grants(
            p.id,
            vec![(route(), Mode::ReadWrite), (other.clone(), Mode::ReadWrite)],
        )
        .unwrap();
        let s = f.acquire(p.id, "rw");
        let mut req = request("other");
        req.route = other.clone();
        let untouched = f.a.acquire_session(p.id, req).unwrap();
        for mode in [Mode::ReadOnly, Mode::ReadWrite] {
            f.a.set_grants(
                p.id,
                vec![(route(), mode), (other.clone(), Mode::ReadWrite)],
            )
            .unwrap();
            assert_eq!(
                f.a.admit(s.id, Operation::Write).unwrap_err(),
                AdmitError::ReadOnly
            );
            for row in [f.a.session(s.id).unwrap(), f.stored(s.id)] {
                assert_eq!(row.mode, Mode::ReadOnly);
                assert_eq!(row.expires_ms, s.expires_ms);
                assert_eq!(row.created_ms, s.created_ms);
            }
            assert_eq!(
                encode(&f.a.session(untouched.id).unwrap()),
                encode(&untouched)
            );
            assert_eq!(encode(&f.stored(untouched.id)), encode(&untouched));
        }
        f.a.admit(untouched.id, Operation::Write).unwrap();
        f.a.renew(p.id, s.id, Some(Mode::ReadWrite), None).unwrap();
        f.a.admit(s.id, Operation::Write).unwrap();
    }
    #[test]
    fn failed_grant_changes_remain_denied_and_retry_notifies_once() {
        let f = Fixture::new();
        let p = f.principal("agent", Mode::ReadWrite);
        let s = f.acquire(p.id, "rw");
        f.store.fail_commits(true);
        assert_eq!(
            f.a.set_grants(p.id, vec![(route(), Mode::ReadOnly)]),
            Err(AuthorityError::Store)
        );
        assert_eq!(
            f.a.admit(s.id, Operation::Write).unwrap_err(),
            AdmitError::ReadOnly
        );
        assert_eq!(f.a.session(s.id).unwrap().mode, Mode::ReadOnly);
        assert_eq!(f.stored(s.id).mode, Mode::ReadWrite);
        assert_eq!(
            f.a.set_grants(p.id, vec![(route(), Mode::ReadWrite)]),
            Err(AuthorityError::Store)
        );
        assert_eq!(f.a.grant(p.id, &route()).unwrap(), Mode::ReadOnly);
        assert_eq!(f.a.set_grants(p.id, vec![]), Err(AuthorityError::Store));
        assert_eq!(
            f.a.admit(s.id, Operation::Read).unwrap_err(),
            AdmitError::Ended
        );
        assert_eq!(f.stored(s.id).state, SessionState::Active);
        assert!(f.hook.0.lock().unwrap().is_empty());
        assert_eq!(
            f.a.set_grants(p.id, vec![(route(), Mode::ReadWrite)]),
            Err(AuthorityError::Store)
        );
        assert_eq!(f.a.grant(p.id, &route()), Err(AuthorityError::NoGrant));
        f.store.fail_commits(false);
        f.a.set_grants(p.id, vec![]).unwrap();
        let stored = f.stored(s.id);
        assert_eq!(stored.state, SessionState::Revoked);
        assert_eq!(stored.revoke_reason, Some(EndReason::GrantRemoved));
        assert_eq!(f.hook.0.lock().unwrap().len(), 1);
        f.a.set_grants(p.id, vec![]).unwrap();
        assert_eq!(f.hook.0.lock().unwrap().len(), 1);
        let loaded = Authority::load(f.store.clone(), SessionLimits::default()).unwrap();
        assert_eq!(loaded.grant(p.id, &route()), Err(AuthorityError::NoGrant));
        assert_eq!(
            loaded.admit(s.id, Operation::Read).unwrap_err(),
            AdmitError::Ended
        );
    }
    #[test]
    fn failed_grant_reduction_retry_persists_narrowed_session() {
        let f = Fixture::new();
        let p = f.principal("agent", Mode::ReadWrite);
        let s = f.acquire(p.id, "rw");
        f.store.fail_commits(true);
        assert_eq!(
            f.a.set_grants(p.id, vec![(route(), Mode::ReadOnly)]),
            Err(AuthorityError::Store)
        );
        f.store.fail_commits(false);
        f.a.set_grants(p.id, vec![(route(), Mode::ReadWrite)])
            .unwrap();
        assert_eq!(f.stored(s.id).mode, Mode::ReadOnly);
        assert_eq!(
            f.a.admit(s.id, Operation::Write).unwrap_err(),
            AdmitError::ReadOnly
        );
    }
    #[test]
    fn principal_disabling_ends_all_owned_sessions_once() {
        let f = Fixture::new();
        let p = f.principal("agent", Mode::ReadWrite);
        let other = f.principal("other", Mode::ReadWrite);
        let sessions = [f.acquire(p.id, "a"), f.acquire(p.id, "b")];
        let unaffected = f.acquire(other.id, "a");
        f.a.set_principal_enabled(p.id, false).unwrap();
        for s in &sessions {
            assert_eq!(
                f.a.admit(s.id, Operation::Read).unwrap_err(),
                AdmitError::Ended
            );
            let stored = f.stored(s.id);
            assert_eq!(stored.state, SessionState::Revoked);
            assert_eq!(stored.revoke_reason, Some(EndReason::PrincipalDisabled));
            assert_eq!(stored.ended_ms, Some(1000));
        }
        assert_eq!(f.hook.0.lock().unwrap().len(), 2);
        f.a.admit(unaffected.id, Operation::Write).unwrap();
        f.a.set_principal_enabled(p.id, false).unwrap();
        assert_eq!(f.hook.0.lock().unwrap().len(), 2);
        assert_eq!(
            f.a.acquire_session(p.id, request("disabled")).unwrap_err(),
            AuthorityError::PrincipalDisabled
        );
        f.a.set_principal_enabled(p.id, true).unwrap();
        assert_eq!(
            f.a.admit(sessions[0].id, Operation::Read).unwrap_err(),
            AdmitError::Ended
        );
    }
    #[test]
    fn acquisition_and_principal_idempotency() {
        let f = Fixture::new();
        let p = f.principal("agent", Mode::ReadWrite);
        let retry =
            f.a.create_principal("agent", PrincipalKind::Agent, false, "agent")
                .unwrap();
        assert_eq!(retry.id, p.id);
        assert_eq!(
            f.a.create_principal("changed", PrincipalKind::Agent, false, "agent")
                .unwrap_err(),
            AuthorityError::IdempotencyConflict
        );
        assert_eq!(
            f.a.create_principal("agent", PrincipalKind::Agent, false, "new")
                .unwrap_err(),
            AuthorityError::NameTaken
        );
        for name in ["", "a b", "?", &"a".repeat(65)] {
            assert_eq!(
                f.a.create_principal(name, PrincipalKind::Agent, false, "bad")
                    .unwrap_err(),
                AuthorityError::NameInvalid
            );
        }
        let s = f.acquire(p.id, "same");
        assert_eq!(f.acquire(p.id, "same").id, s.id);
        assert_eq!(f.rows(), 1);
        let mut r = request("same");
        r.verifier = Verifier(ScramVerifier::from_password("different", b"salt", 1));
        assert_eq!(
            f.a.acquire_session(p.id, r).unwrap_err(),
            AuthorityError::IdempotencyConflict
        );
        let variants = [
            {
                let mut r = request("same");
                r.secret_sha256 = [3; 32];
                r
            },
            {
                let mut r = request("same");
                r.label = Some("other".into());
                r
            },
            {
                let mut r = request("same");
                r.isolated = true;
                r
            },
            {
                let mut r = request("same");
                r.mode = Some(Mode::ReadOnly);
                r
            },
            {
                let mut r = request("same");
                r.lifetime_ms = Some(1);
                r
            },
            {
                let mut r = request("same");
                r.route = RouteId::new("other").unwrap();
                r
            },
        ];
        for r in variants {
            assert_eq!(
                f.a.acquire_session(p.id, r).unwrap_err(),
                AuthorityError::IdempotencyConflict
            );
        }
        let other = f.principal("other", Mode::ReadWrite);
        assert_ne!(f.acquire(other.id, "same").id, s.id);
    }
    #[test]
    fn failed_revocation_remains_denied_until_durable_retry() {
        let f = Fixture::new();
        let p = f.principal("agent", Mode::ReadWrite);
        let s = f.acquire(p.id, "s");
        f.store.fail_commits(true);
        assert_eq!(
            f.a.revoke(s.id, EndReason::AdminRevoked),
            Err(AuthorityError::Store)
        );
        assert_eq!(
            f.a.admit(s.id, Operation::Read).unwrap_err(),
            AdmitError::Ended
        );
        assert_eq!(f.stored(s.id).state, SessionState::Active);
        assert!(f.hook.0.lock().unwrap().is_empty());
        f.store.fail_commits(false);
        f.a.revoke(s.id, EndReason::Requested).unwrap();
        assert_eq!(f.stored(s.id).revoke_reason, Some(EndReason::AdminRevoked));
        assert_eq!(f.hook.0.lock().unwrap().len(), 1);
        f.a.revoke(s.id, EndReason::Requested).unwrap();
        assert_eq!(f.hook.0.lock().unwrap().len(), 1);
    }
    #[test]
    fn clock_expiry_and_idle_queue_for_sweep() {
        let f = Fixture::limits(SessionLimits {
            max_lifetime_ms: 1000,
            idle_timeout_ms: Some(10),
        });
        let p = f.principal("agent", Mode::ReadWrite);
        let mut r = request("expires");
        r.lifetime_ms = Some(5);
        let expires = f.a.acquire_session(p.id, r).unwrap();
        let idle = f.acquire(p.id, "idle");
        f.clock.store(1005, Ordering::SeqCst);
        assert_eq!(
            f.a.admit(expires.id, Operation::Read).unwrap_err(),
            AdmitError::Ended
        );
        f.clock.store(1010, Ordering::SeqCst);
        f.a.admit(idle.id, Operation::Read).unwrap();
        f.clock.store(1021, Ordering::SeqCst);
        assert_eq!(
            f.a.admit(idle.id, Operation::Read).unwrap_err(),
            AdmitError::Ended
        );
        assert!(f.hook.0.lock().unwrap().is_empty());
        f.store.fail_commits(true);
        assert_eq!(f.a.sweep(), Err(AuthorityError::Store));
        f.store.fail_commits(false);
        assert_eq!(f.a.sweep().unwrap(), 2);
        assert_eq!(f.stored(expires.id).revoke_reason, Some(EndReason::Expired));
        assert_eq!(
            f.stored(idle.id).revoke_reason,
            Some(EndReason::IdleTimeout)
        );
        assert_eq!(f.hook.0.lock().unwrap().len(), 2);
        assert_eq!(f.a.sweep().unwrap(), 0);
        let s = f.acquire(p.id, "sweep-only");
        f.clock.store(s.expires_ms, Ordering::SeqCst);
        assert_eq!(f.a.sweep().unwrap(), 1);
        assert_eq!(f.stored(s.id).revoke_reason, Some(EndReason::Expired));
    }
    #[test]
    fn reopening_restores_authoritative_records_and_tombstones() {
        let f = Fixture::new();
        let p = f.principal("agent", Mode::ReadWrite);
        let active = f.acquire(p.id, "active");
        let ended = f.acquire(p.id, "ended");
        f.a.add_principal_credential(p.id, verifier(), None)
            .unwrap();
        f.a.revoke(ended.id, EndReason::AdminRevoked).unwrap();
        let salt = f.a.scope(active.scope).unwrap().salt;
        let Fixture {
            a,
            store,
            clock,
            hook,
            dir,
        } = f;
        drop(a);
        drop(store);
        drop(hook);
        let c = clock.clone();
        let store = Arc::new(
            RedbStore::with_clock(&dir.0, &root(), Arc::new(move || c.load(Ordering::SeqCst)))
                .unwrap(),
        );
        let a = Authority::load(store.clone(), SessionLimits::default()).unwrap();
        assert_eq!(a.sessions_of(p.id).len(), 2);
        assert_eq!(a.scope(active.scope).unwrap().salt, salt);
        assert_eq!(a.principal_by_name("agent").unwrap().id, p.id);
        assert_eq!(a.principal_credential_verifiers(p.id).len(), 1);
        a.admit(active.id, Operation::Write).unwrap();
        assert!(a.check_session_secret(active.id, b"session-secret"));
        assert_eq!(
            a.admit(ended.id, Operation::Read).unwrap_err(),
            AdmitError::Ended
        );
        assert_eq!(
            a.session(ended.id).unwrap().revoke_reason,
            Some(EndReason::AdminRevoked)
        );
        assert_eq!(
            a.acquire_session(p.id, request("active")).unwrap().id,
            active.id
        );
        assert_eq!(
            a.create_principal("agent", PrincipalKind::Agent, false, "agent")
                .unwrap()
                .id,
            p.id
        );
        drop(a);
        drop(store);
    }
    #[test]
    fn session_secret_verification_and_credential_renewal() {
        let f = Fixture::new();
        let p = f.principal("agent", Mode::ReadWrite);
        let s = f.acquire(p.id, "s");
        assert!(f.a.check_session_secret(s.id, b"session-secret"));
        assert!(!f.a.check_session_secret(s.id, b"wrong"));
        assert!(!f.a.check_session_secret(SessionId::random(), b"session-secret"));
        f.a.renew(
            p.id,
            s.id,
            None,
            Some((verifier(), Sha256::digest(b"new-secret").into())),
        )
        .unwrap();
        assert!(f.a.check_session_secret(s.id, b"new-secret"));
        assert!(f.a.check_session_secret(s.id, b"session-secret"));
        f.a.revoke(s.id, EndReason::Requested).unwrap();
        assert!(!f.a.check_session_secret(s.id, b"session-secret"));
        assert!(!f.a.check_session_secret(s.id, b"new-secret"));
    }
    #[test]
    fn acquisition_debug_redacts_all_client_secrets() {
        let mut r = request("key");
        r.secret_sha256 = [0xab; 32];
        let debug = format!("{r:?}");
        assert!(!debug.contains(&r.verifier.0.to_pg_string()));
        assert!(!debug.contains(&"ab".repeat(32)));
        assert!(!debug.contains("session-secret"));
    }
    #[test]
    fn coarse_activity_flush_and_tombstone_purge() {
        let f = Fixture::new();
        let p = f.principal("agent", Mode::ReadWrite);
        let s = f.acquire(p.id, "s");
        f.clock.store(60_999, Ordering::SeqCst);
        f.a.admit(s.id, Operation::Read).unwrap();
        f.a.flush_last_used().unwrap();
        assert_eq!(f.stored(s.id).last_used_ms, 1000);
        f.clock.store(61_000, Ordering::SeqCst);
        f.a.admit(s.id, Operation::Read).unwrap();
        f.a.flush_last_used().unwrap();
        assert_eq!(f.stored(s.id).last_used_ms, 61_000);
        let domain = crate::policy::DomainName("email".into());
        let mut tx = f.store.write().unwrap();
        tx.put(
            Table::SessionCounters,
            &keys::session_counter(s.id, &domain).unwrap(),
            &encode_count(2),
        )
        .unwrap();
        tx.commit().unwrap();
        f.a.revoke(s.id, EndReason::Requested).unwrap();
        assert_eq!(f.a.purge_tombstones(10).unwrap(), 0);
        f.clock.store(s.expires_ms + 10, Ordering::SeqCst);
        assert_eq!(f.a.purge_tombstones(10).unwrap(), 0);
        f.clock.store(s.expires_ms + 11, Ordering::SeqCst);
        assert_eq!(f.a.purge_tombstones(10).unwrap(), 1);
        assert!(f.a.session(s.id).is_none());
        assert!(f.a.scope(s.scope).is_some());
        let read = f.store.read().unwrap();
        for table in [
            Table::Sessions,
            Table::SessionCredentials,
            Table::SessionCounters,
        ] {
            assert!(read.scan_prefix(table, &s.id.0).unwrap().is_empty());
        }
    }
    #[test]
    fn principal_credentials_and_config_management() {
        let f = Fixture::new();
        let p = f.principal("agent", Mode::ReadOnly);
        let cid =
            f.a.add_principal_credential(p.id, verifier(), Some(1001))
                .unwrap();
        assert_eq!(f.a.principal_credential_verifiers(p.id).len(), 1);
        f.clock.store(1001, Ordering::SeqCst);
        assert!(f.a.principal_credential_verifiers(p.id).is_empty());
        let other = f.principal("other", Mode::ReadOnly);
        assert_eq!(
            f.a.revoke_principal_credential(other.id, cid),
            Err(AuthorityError::NotFound)
        );
        f.a.revoke_principal_credential(p.id, cid).unwrap();
        let permanent =
            f.a.add_principal_credential(p.id, verifier(), None)
                .unwrap();
        assert_eq!(f.a.principal_credential_verifiers(p.id).len(), 1);
        f.a.revoke_principal_credential(p.id, permanent).unwrap();
        assert!(f.a.principal_credential_verifiers(p.id).is_empty());
        let mut config = p.clone();
        config.source = PrincipalSource::Config;
        let mut tx = f.store.write().unwrap();
        tx.put(Table::Principals, &keys::principal(p.id), &encode(&config))
            .unwrap();
        tx.commit().unwrap();
        let a = Authority::load(f.store.clone(), SessionLimits::default()).unwrap();
        assert_eq!(
            a.set_principal_enabled(p.id, false),
            Err(AuthorityError::ConfigManaged)
        );
        assert_eq!(
            a.add_principal_credential(p.id, verifier(), None),
            Err(AuthorityError::ConfigManaged)
        );
        assert_eq!(
            a.revoke_principal_credential(p.id, cid),
            Err(AuthorityError::ConfigManaged)
        );
        a.set_grants(p.id, vec![(route(), Mode::ReadWrite)])
            .unwrap();
    }
    #[test]
    fn failed_acquisition_and_disabling_are_fail_closed() {
        let f = Fixture::new();
        let p = f.principal("agent", Mode::ReadWrite);
        let s = f.acquire(p.id, "s");
        f.store.fail_commits(true);
        assert_eq!(
            f.a.acquire_session(p.id, request("failed")).unwrap_err(),
            AuthorityError::Store
        );
        assert_eq!(f.a.sessions_of(p.id).len(), 1);
        assert_eq!(
            f.a.set_principal_enabled(p.id, false),
            Err(AuthorityError::Store)
        );
        assert_eq!(
            f.a.admit(s.id, Operation::Read).unwrap_err(),
            AdmitError::Ended
        );
        assert!(f.hook.0.lock().unwrap().is_empty());
        f.store.fail_commits(false);
        f.a.set_principal_enabled(p.id, false).unwrap();
        assert_eq!(f.hook.0.lock().unwrap().len(), 1);
        assert_eq!(
            f.stored(s.id).revoke_reason,
            Some(EndReason::PrincipalDisabled)
        );
    }
    #[test]
    fn malformed_rows_fail_closed() {
        let f = Fixture::new();
        for table in Table::ALL {
            let mut tx = f.store.write().unwrap();
            tx.put(table, b"malformed", b"{}").unwrap();
            tx.commit().unwrap();
            assert!(matches!(
                Authority::load(f.store.clone(), SessionLimits::default()),
                Err(AuthorityError::Store)
            ));
            let mut tx = f.store.write().unwrap();
            tx.delete(table, b"malformed").unwrap();
            tx.commit().unwrap();
        }
    }
    #[test]
    fn wrong_secrets_do_not_keep_an_idle_session_alive() {
        let f = Fixture::limits(SessionLimits {
            max_lifetime_ms: 1000,
            idle_timeout_ms: Some(10),
        });
        let p = f.principal("agent", Mode::ReadWrite);
        let s = f.acquire(p.id, "s");
        f.clock.store(1009, Ordering::SeqCst);
        assert!(!f.a.check_session_secret(s.id, b"wrong"));
        f.clock.store(1011, Ordering::SeqCst);
        assert!(!f.a.check_session_secret(s.id, b"session-secret"));
        assert_eq!(
            f.a.session(s.id).unwrap().revoke_reason,
            Some(EndReason::IdleTimeout)
        );
    }
}
