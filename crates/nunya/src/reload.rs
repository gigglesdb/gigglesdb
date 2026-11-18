//! Shared policy reload coordination for every serving route.
use crate::{
    policy::{Policy, PolicyDocument, PolicyVersion, RouteDefault, bindings::{self, RouteBindings, MembershipChange}},
    schema::Snapshot,
    serve::{ReloadableLog, now, parse_policy_bytes, readlog_key},
    state::model::RouteId,
};
use sha2::{Digest, Sha256};
use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex, RwLock, atomic::AtomicBool},
    time::Duration,
};

type PolicyLock = RwLock<Arc<Policy>>;
type SnapshotLock = RwLock<Arc<Snapshot>>;

/// A serving route and its policy/catalog locks.
pub(crate) enum Route {
    Postgres(Arc<crate::pgwire::context::GatewayContext>),
    Spanner(Arc<crate::spanner::proxy::SpannerContext>),
    #[cfg(test)]
    Test(RouteId, PolicyLock, SnapshotLock),
}
impl Route {
    fn id(&self) -> &str {
        match self {
            Self::Postgres(c) => c.route.as_str(),
            Self::Spanner(c) => c.route.as_str(),
            #[cfg(test)]
            Self::Test(id, _, _) => id.as_str(),
        }
    }

    fn locks(&self) -> (&PolicyLock, &SnapshotLock) {
        match self {
            Self::Postgres(c) => (&c.policy, &c.snapshot),
            Self::Spanner(c) => (&c.policy, &c.snapshot),
            #[cfg(test)]
            Self::Test(_, p, s) => (p, s),
        }
    }
}
struct PublishedRoute {
    route: Route,
    policy: Arc<Policy>,
    refresh_failed: bool,
    membership_failed: bool,
}
impl std::ops::Deref for PublishedRoute {
    type Target = Route;
    fn deref(&self) -> &Route { &self.route }
}
struct Unit {
    identity: PathBuf,
    path: PathBuf,
    revisions: Arc<RwLock<crate::serve::Revisions>>,
    policy: Arc<Policy>,
    doc: Arc<PolicyDocument>,
    routes: Vec<PublishedRoute>,
    hash: String,
    applied: u64,
    last: &'static str,
    last_time: u64,
    attempts: u64,
    failures: u64,
}
struct State {
    units: Vec<Unit>,
    configured: Vec<(RouteId, usize)>,
}
impl State {
    fn unit(&self, id: &str) -> Result<usize, String> {
        self.configured
            .iter()
            .find(|(route, _)| route.as_str() == id)
            .map(|(_, unit)| *unit)
            .ok_or_else(|| "route policy unavailable".into())
    }
}
/// Policy units and their shared audit sink.
pub(crate) struct Hub {
    state: Mutex<State>,
    /// Service lifecycle and management audit log.
    pub(crate) log: ReloadableLog,
    store: RwLock<Option<Arc<dyn crate::state::store::Store>>>,
    authority: RwLock<std::sync::Weak<crate::state::authority::Authority>>,
    pending_membership: Mutex<std::collections::BTreeMap<String, MembershipChange>>,
    #[cfg(test)]
    reset_callback: RwLock<Option<ResetCallback>>,
    watch_ms: Option<u64>,
    service: crate::policy::ServicePolicySettings,
}
#[cfg(test)]
type ResetCallback = Arc<dyn Fn(&MembershipChange, &Policy) -> Result<usize, String> + Send + Sync>;
#[derive(Debug)]
struct CompileError { message: String, membership: bool }
impl CompileError {
    fn binding(error: impl std::fmt::Display) -> Self {
        Self { message: error.to_string(), membership: true }
    }
}
impl From<String> for CompileError {
    fn from(message: String) -> Self { Self { message, membership: false } }
}
impl From<&str> for CompileError {
    fn from(message: &str) -> Self { message.to_owned().into() }
}
fn hash(bytes: &[u8]) -> String {
    data_encoding::HEXLOWER.encode(&Sha256::digest(bytes))
}
fn parse(path: &Path, bytes: &[u8], service: &crate::policy::ServicePolicySettings) -> Result<PolicyDocument, String> {
    parse_policy_bytes(path, bytes, service).map_err(crate::serve::errors)
}
impl Hub {
    /// Report data-free readiness for every configured route.
    pub(crate) fn readiness(&self, audit_ok: bool, store_ok: bool) -> (bool, String) {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let policy_available = !self.state.is_poisoned();
        let mut ready = policy_available && !state.configured.is_empty();
        let mut body = String::new();
        let at = now();
        for (id, unit) in &state.configured {
            let unit = &state.units[*unit];
            let route = unit.routes.iter().find(|r| r.id() == id.as_str());
            let mut policy_ok = policy_available;
            let catalog = match route {
                Some(route) => {
                    let (policy, snapshot) = route.locks();
                    match (policy.read(), snapshot.read()) {
                        (Ok(policy), Ok(snapshot)) => {
                            policy_ok &= Arc::ptr_eq(&policy, &route.policy) && !route.refresh_failed && !route.membership_failed;
                            if at.saturating_sub(snapshot.taken_at_unix_ms)
                                > policy.snapshot_max_age_ms
                            {
                                "stale"
                            } else {
                                "ok"
                            }
                        }
                        (Err(_), Ok(_)) => {
                            policy_ok = false;
                            "unavailable"
                        }
                        (Ok(_), Err(_)) => "unavailable",
                        (Err(_), Err(_)) => {
                            policy_ok = false;
                            "unavailable"
                        }
                    }
                }
                None => "unavailable",
            };
            ready &= policy_ok && catalog == "ok" && audit_ok && store_ok;
            use std::fmt::Write;
            let available = |ok| if ok { "ok" } else { "unavailable" };
            let _ = writeln!(
                body,
                "route={} policy={} catalog={} audit={} store={}",
                id.as_str(),
                available(policy_ok),
                catalog,
                available(audit_ok),
                available(store_ok)
            );
        }
        (ready, body)
    }

    /// Load one unit for each distinct canonical policy file and configure its routes.
    pub(crate) fn new(
        routes: &[(RouteId, PathBuf)],
        watch_ms: Option<u64>,
        service: crate::policy::ServicePolicySettings,
    ) -> Result<Arc<Self>, String> {
        let mut units: Vec<Unit> = Vec::new();
        let mut configured = Vec::new();
        for (route, path) in routes {
            if configured.iter().any(|(id, _)| id == route) {
                return Err("duplicate policy route".into());
            }
            let identity = std::fs::canonicalize(path).map_err(|_| "policy file unavailable")?;
            let index = if let Some(index) = units.iter().position(|unit| unit.identity == identity) {
                index
            } else {
                let bytes = std::fs::read(path).map_err(|_| "policy file unavailable")?;
                let doc = Arc::new(parse(path, &bytes, &service)?);
                let policy = Arc::new(doc.policy.clone());
                if units
                    .first()
                    .is_some_and(|unit| readlog_key(&unit.policy.readlog) != readlog_key(&policy.readlog))
                {
                    return Err("policy files name different read logs".into());
                }
                let digest = hash(&bytes);
                units.push(Unit {
                    identity,
                    path: path.clone(),
                    policy,
                    doc,
                    routes: vec![],
                    revisions: Arc::new(RwLock::new(crate::serve::Revisions {
                        policy: digest.clone(),
                        config: String::new(),
                    })),
                    hash: digest,
                    applied: now(),
                    last: "none",
                    last_time: 0,
                    attempts: 0,
                    failures: 0,
                });
                units.len() - 1
            };
            configured.push((route.clone(), index));
        }
        let first = units.first().ok_or("route policy unavailable")?;
        let sink =
            crate::readlog::open_sink(&first.policy.readlog).map_err(|_| "read log unavailable")?;
        Ok(Arc::new(Self {
            log: ReloadableLog(
                Arc::new(RwLock::new(sink)),
                Arc::new(RwLock::new(crate::serve::Revisions::default())),
                Arc::new(AtomicBool::new(true)),
            ),
            state: Mutex::new(State { units, configured }),
            store: RwLock::new(None),
            authority: RwLock::new(std::sync::Weak::new()),
            pending_membership: Mutex::new(Default::default()),
            #[cfg(test)]
            reset_callback: RwLock::new(Some(Arc::new(|_, _| Ok(0)))) ,
            watch_ms,
            service,
        }))
    }
    /// Attach durable bindings before any route is registered.
    pub(crate) fn set_store(&self, store: Arc<dyn crate::state::store::Store>) -> Result<(), String> {
        let state = self.state.lock().map_err(|_| "policy lock unavailable")?;
        if state.units.iter().any(|unit| !unit.routes.is_empty()) {
            return Err("policy store must be set before registration".into());
        }
        *self.store.write().map_err(|_| "policy bindings unavailable")? = Some(store);
        Ok(())
    }

    /// Attach the service authority before publishing shared-domain changes.
    pub(crate) fn set_authority(&self, authority: std::sync::Weak<crate::state::authority::Authority>) -> Result<(), String> {
        *self.authority.write().map_err(|_| "session authority unavailable")? = authority;
        Ok(())
    }

    fn compile(&self, doc: &PolicyDocument, route: &RouteId, snapshot: &Snapshot) -> Result<(Arc<Policy>, Option<MembershipChange>), CompileError> {
        let store = self.store.read().map_err(|_| CompileError::binding("policy bindings unavailable"))?;
        let needs_bindings = (doc.version == PolicyVersion::V2 && doc.route_default == RouteDefault::Tokenize)
            || !doc.links.is_empty();
        if needs_bindings && store.is_none() {
            return Err(CompileError::binding("policy bindings unavailable"));
        }
        let mut bindings = match store.as_ref() {
            Some(store) => bindings::load(store.as_ref(), route).map_err(CompileError::binding)?,
            None => RouteBindings::default(),
        };
        let warnings = crate::policy::validate::validate_document(doc, snapshot, &bindings)
            .map_err(crate::serve::errors)?;
        // A final removed link still has persisted linked records to reconcile.
        if needs_bindings || bindings.iter().any(|(_, record)| record.linked) {
            let (next, change) = bindings::refresh_membership(
                store.as_ref().ok_or_else(|| CompileError::binding("policy bindings unavailable"))?.as_ref(), route, doc, snapshot)
                .map_err(CompileError::binding)?;
            bindings = next;
            if change.requires_reset() {
                let mut pending = self.pending_membership.lock().map_err(|_| CompileError::binding("policy bindings unavailable"))?;
                pending.entry(route.as_str().into()).and_modify(|old| {
                    for domain in &change.retired { if !old.retired.contains(domain) { old.retired.push(domain.clone()); } }
                    old.joined.extend(change.joined.clone());
                    old.left.extend(change.left.clone());
                }).or_insert(change);
            }
        }
        let mut policy = crate::policy::effective::served_policy(doc, snapshot, &bindings);
        if let Some(store) = store.as_ref() {
            policy.retired_domains = bindings::load_retired(store.as_ref(), route).map_err(CompileError::binding)?;
        }
        crate::policy::validate::validate_dependents(&policy, snapshot).map_err(crate::serve::errors)?;
        for warning in warnings {
            eprintln!("serve: policy warning: {}", warning.message.replace(['\r', '\n'], " "));
        }
        if let Some(store) = store.as_ref()
            && let Some(owed) = bindings::load_reset(store.as_ref(), route).map_err(CompileError::binding)? {
            self.pending_membership.lock().map_err(|_| CompileError::binding("policy bindings unavailable"))?
                .insert(route.as_str().into(), owed);
        }
        let change = self.pending_membership.lock().map_err(|_| CompileError::binding("policy bindings unavailable"))?
            .get(route.as_str()).cloned();
        Ok((Arc::new(policy), change))
    }

    fn reset_membership(&self, change: Option<&MembershipChange>, old: &Policy) -> Result<(), String> {
        let Some(change) = change.filter(|change| change.requires_reset()) else { return Ok(()); };
        let authority = self.authority.read().map_err(|_| "session authority unavailable")?.upgrade();
        let count = if let Some(authority) = authority {
            authority.end_route_sessions(&change.route, crate::state::model::EndReason::PolicyReset)
                .map_err(|_| "route session reset unavailable")?
        } else {
            #[cfg(test)]
            { self.reset_callback.read().map_err(|_| "session authority unavailable")?.as_ref()
                .ok_or("session authority unavailable")?(change, old)? }
            #[cfg(not(test))]
            { let _ = old; return Err("session authority unavailable".into()); }
        };
        use crate::readlog::ReadLog;
        self.log.write_lifecycle(&crate::readlog::LifecycleRecord {
            kind: crate::readlog::LifecycleKind::MembershipReset {
                retired_domains: change.retired.iter().map(|d| d.0.clone()).collect(), sessions: count,
            },
            at_ms: now(), principal: String::new(), session: None,
            route: Some(change.route.as_str().into()), connection: None,
            actor: crate::readlog::LifecycleActor::System, reason: "policy_reset",
        }).map_err(|_| "read log unavailable".to_string())
    }

    fn membership_pending(&self, route: &str) -> bool {
        self.pending_membership.lock().map_or(true, |pending| pending.contains_key(route))
            || self.store.read().map_or(true, |store| store.as_ref().is_some_and(|store| {
                RouteId::new(route).is_none_or(|route| bindings::load_reset(store.as_ref(), &route).map_or(true, |owed| owed.is_some()))
            }))
    }

    fn membership_published(&self, route: &str) {
        // A failed cleanup retains the durable marker, making restart repeat the safe reset.
        if let Ok(store) = self.store.read() && let Some(store) = store.as_ref() {
            let _ = bindings::clear_reset(store.as_ref(), route);
        }
        self.pending_membership.lock().unwrap_or_else(std::sync::PoisonError::into_inner).remove(route);
    }

    /// Set the service config revision on every unit and the lifecycle log.
    pub(crate) fn set_config_revision(&self, config: &str) -> Result<(), String> {
        self.commit_config_revision(config, || Ok(()))
    }
    /// Check publication availability before applying configuration, then publish infallibly.
    pub(crate) fn commit_config_revision(
        &self, config: &str, apply: impl FnOnce() -> Result<(), String>,
    ) -> Result<(), String> {
        self.commit_config_refresh(config, None, apply)
    }
    /// Commit configuration and an optional recompiled catalog under one publication lock.
    pub(crate) fn commit_config_refresh(
        &self, config: &str, refresh: Option<(&Route, Snapshot)>, apply: impl FnOnce() -> Result<(), String>,
    ) -> Result<(), String> {
        let mut state = self.state.lock().map_err(|_| "policy lock unavailable")?;
        // Validate all locks before the only fallible applier. Release revision guards
        // during apply: principal revocation emits lifecycle records using these locks.
        {
            let _revisions = state.units.iter().map(|unit| {
                unit.revisions.write().map_err(|_| "revision lock unavailable")
            }).collect::<Result<Vec<_>, _>>()?;
            let _service = self.log.1.write().map_err(|_| "revision lock unavailable")?;
        }
        if let Some((route, snapshot)) = refresh {
            let index = state.unit(route.id())?;
            self.publish_refresh(&mut state.units[index], route, snapshot, apply)?;
        } else {
            apply()?;
        }
        let mut revisions: Vec<_> = state.units.iter().map(|unit| {
            unit.revisions.write().unwrap_or_else(std::sync::PoisonError::into_inner)
        }).collect();
        let mut service = self.log.1.write().unwrap_or_else(std::sync::PoisonError::into_inner);
        for revision in &mut revisions { revision.config = config.into(); }
        service.config = config.into();
        Ok(())
    }
    /// Return the configured route's current policy.
    pub(crate) fn policy_for(&self, route: &RouteId) -> Result<Arc<Policy>, String> {
        let state = self.state.lock().map_err(|_| "policy lock unavailable")?;
        Ok(state.units[state.unit(route.as_str())?].policy.clone())
    }
    /// Return a route's current revision, refusing unavailable locks or routes.
    pub(crate) fn policy_revision_for(&self, route: &RouteId) -> Option<String> {
        let state = self.state.lock().ok()?;
        state.units[state.unit(route.as_str()).ok()?]
            .revisions
            .read()
            .ok()
            .map(|r| r.policy.clone())
    }
    /// Share the audit sink while stamping this route's unit revisions.
    pub(crate) fn route_log(&self, route: &RouteId) -> Result<ReloadableLog, String> {
        let state = self.state.lock().map_err(|_| "policy lock unavailable")?;
        Ok(ReloadableLog(
            self.log.0.clone(),
            state.units[state.unit(route.as_str())?].revisions.clone(),
            self.log.2.clone(),
        ))
    }
    /// Validate and install the configured unit policy before publishing a route.
    pub(crate) fn register(&self, route: Route) -> Result<(), String> {
        let mut state = self.state.lock().map_err(|_| "policy lock unavailable")?;
        let index = state.unit(route.id())?;
        let s = &mut state.units[index];
        let (policy, snapshot) = route.locks();
        let mut current = policy.write().map_err(|_| "policy lock unavailable")?;
        let snapshot = snapshot.read().map_err(|_| "snapshot lock unavailable")?;
        let revision = match &route {
            Route::Postgres(ctx) => Some(&ctx.policy_revision),
            Route::Spanner(ctx) => Some(&ctx.policy_revision),
            #[cfg(test)]
            Route::Test(..) => None,
        };
        let mut revision = revision.map(|revision| revision.write().map_err(|_| "revision lock unavailable")).transpose()?;
        let (compiled, change) = self.compile(&s.doc, &RouteId::new(route.id()).ok_or("route policy unavailable")?, &snapshot)
            .map_err(|error| {
                if error.membership || self.membership_pending(route.id()) {
                    for registered in &mut s.routes {
                        if registered.id() == route.id() { registered.membership_failed = true; }
                    }
                }
                error.message
            })?;
        self.reset_membership(change.as_ref(), &current).inspect_err(|_| {
            for registered in &mut s.routes {
                if registered.id() == route.id() && self.membership_pending(route.id()) { registered.membership_failed = true; }
            }
        })?;
        {
            if let Some(revision) = revision.as_mut() { **revision = Some(s.hash.clone()); }
            *current = compiled.clone();
        }
        drop(snapshot);
        drop(current);
        drop(revision);
        self.membership_published(route.id());
        s.routes.push(PublishedRoute { route, policy: compiled, refresh_failed: false, membership_failed: false });
        Ok(())
    }
    /// Validate a fresh catalog against this route's policy unit.
    pub(crate) fn refresh(&self, route: &Route, snapshot: Snapshot) -> Result<(), String> {
        self.refresh_if(route, snapshot, || true)
    }
    /// Publish a catalog only if its settings are still current under the publication lock.
    pub(crate) fn refresh_if(&self, route: &Route, snapshot: Snapshot, current: impl FnOnce() -> bool) -> Result<(), String> {
        let mut state = self.state.lock().map_err(|_| "policy lock unavailable")?;
        if !current() { return Ok(()); }
        let index = state.unit(route.id())?;
        let s = &mut state.units[index];
        self.publish_refresh(s, route, snapshot, || Ok(()))
    }

    fn publish_refresh(&self, unit: &mut Unit, route: &Route, snapshot: Snapshot, apply: impl FnOnce() -> Result<(), String>) -> Result<(), String> {
        if unit.routes.iter().any(|registered| registered.id() == route.id() && registered.membership_failed) {
            return Err("policy membership requires successful reload".into());
        }
        let result = (|| {
            let (policy, catalog) = route.locks();
            let mut policy = policy.write().map_err(|_| "policy lock unavailable")?;
            let mut catalog = catalog.write().map_err(|_| "snapshot lock unavailable")?;
            let (compiled, change) = self.compile(&unit.doc, &RouteId::new(route.id()).ok_or("route policy unavailable")?, &snapshot)
                .map_err(|error| error.message)?;
            self.reset_membership(change.as_ref(), &policy)?;
            apply()?;
            *catalog = Arc::new(snapshot);
            *policy = compiled.clone();
            Ok(compiled)
        })();
        for registered in &mut unit.routes {
            if std::ptr::eq(registered.locks().0, route.locks().0) {
                match &result {
                    Ok(compiled) => { registered.policy = compiled.clone(); registered.refresh_failed = false; registered.membership_failed = false; self.membership_published(route.id()); }
                    Err(_) => {
                        registered.refresh_failed = true;
                        if self.membership_pending(route.id()) { registered.membership_failed = true; }
                    },
                }
            }
        }
        result.map(|_| ())
    }

    fn apply(
        &self,
        unit: usize,
        candidate: Result<PolicyDocument, String>,
        digest: Option<String>,
    ) -> Result<(), String> {
        let mut state = self.state.lock().map_err(|_| "policy lock unavailable")?;
        let independent = state.units.len() > 1;
        let s = &mut state.units[unit];
        s.attempts += 1;
        s.last_time = now();
        let mut membership_failed = Vec::new();
        let result = (|| {
            let doc = Arc::new(candidate?);
            let policy = Arc::new(doc.policy.clone());
            if independent && readlog_key(&policy.readlog) != readlog_key(&s.policy.readlog) {
                return Err("policy files name different read logs".into());
            }
            let replacement = if readlog_key(&policy.readlog) != readlog_key(&s.policy.readlog) {
                Some(
                    crate::readlog::open_sink(&policy.readlog)
                        .map_err(|_| "read log unavailable")?,
                )
            } else {
                None
            };
            // Hold every policy and snapshot lock through validation and publication.
            let mut policies = Vec::new();
            let mut snapshots = Vec::new();
            for route in &s.routes {
                let (p, snap) = route.locks();
                policies.push(p.write().map_err(|_| "policy lock unavailable")?);
                snapshots.push(snap.read().map_err(|_| "snapshot lock unavailable")?);
            }
            let compiled = s.routes.iter().zip(&snapshots).map(|(route, snapshot)| {
                self.compile(&doc, &RouteId::new(route.id()).ok_or("route policy unavailable")?, snapshot)
                    .map_err(|error| { if error.membership { membership_failed.push(route.id().to_owned()); } error.message })
            }).collect::<Result<Vec<_>, String>>()?;
            for ((_, change), old) in compiled.iter().zip(&policies) {
                self.reset_membership(change.as_ref(), old)?;
            }
            let mut route_revisions = Vec::new();
            for route in &s.routes {
                let revision = match &route.route {
                    Route::Postgres(ctx) => Some(&ctx.policy_revision),
                    Route::Spanner(ctx) => Some(&ctx.policy_revision),
                    #[cfg(test)]
                    Route::Test(..) => None,
                };
                if let Some(revision) = revision {
                    route_revisions.push(revision.write().map_err(|_| "revision lock unavailable")?);
                }
            }
            let mut log = self
                .log
                .0
                .write()
                .map_err(|_| "read log lock unavailable")?;
            let mut revisions = s
                .revisions
                .write()
                .map_err(|_| "revision lock unavailable")?;
            let revision = digest.as_ref().ok_or("policy digest unavailable")?;
            if let Some(replacement) = replacement {
                *log = replacement;
            }
            for (current, compiled) in policies.iter_mut().zip(&compiled) {
                **current = compiled.0.clone();
            }
            for current in &mut route_revisions {
                **current = Some(revision.clone());
            }
            revisions.policy = revision.clone();
            Ok::<_, String>((doc, policy, compiled))
        })();
        for route in &mut s.routes {
            if result.is_err() && (membership_failed.iter().any(|id| id == route.id()) || self.membership_pending(route.id())) { route.membership_failed = true; }
        }
        match result {
            Ok((doc, policy, compiled)) => {
                s.doc = doc;
                s.policy = policy;
                for (route, compiled) in s.routes.iter_mut().zip(compiled) {
                    route.policy = compiled.0;
                    route.membership_failed = false;
                    self.membership_published(route.id());
                }
                s.hash = digest.ok_or("policy digest unavailable")?;
                s.applied = now();
                s.last = "applied";
                Ok(())
            }
            Err(e) => {
                s.failures += 1;
                s.last = "rejected";
                Err(e)
            }
        }
    }
    fn attempt(&self, unit: usize, path: &Path, bytes: Result<Vec<u8>, std::io::Error>) {
        let digest = bytes.as_ref().ok().map(|b| hash(b));
        let candidate = bytes
            .map_err(|_| "policy file unavailable".to_owned())
            .and_then(|b| parse(path, &b, &self.service));
        match self.apply(unit, candidate, digest) {
            Ok(()) => eprintln!("serve: policy reloaded"),
            Err(e) => eprintln!(
                "serve: policy reload failed: {}",
                e.replace(['\r', '\n'], " ")
            ),
        }
    }
    /// Report each configured route in configuration order.
    pub(crate) fn status(&self) -> String {
        let s = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        s.configured.iter().map(|(id, index)| {
            let unit = &s.units[*index];
            format!("route={} policy_sha256={} policy_applied_unix_ms={} last_reload={} last_reload_unix_ms={} policy_watch={}\n",
                id.as_str(), unit.hash, unit.applied, unit.last, unit.last_time,
                self.watch_ms.map_or_else(|| "off".into(), |v| v.to_string()))
        }).collect()
    }
    /// Report per-route policy counters and existing PostgreSQL connection counters.
    pub(crate) fn metrics(&self) -> Vec<(String, u64)> {
        let s = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut values = Vec::new();
        for (id, index) in &s.configured {
            let unit = &s.units[*index];
            values.extend([
                (
                    format!("nunya_policy_reloads_total{{route=\"{}\"}}", id.as_str()),
                    unit.attempts,
                ),
                (
                    format!(
                        "nunya_policy_reload_failures_total{{route=\"{}\"}}",
                        id.as_str()
                    ),
                    unit.failures,
                ),
                (
                    format!("nunya_policy_applied_unix_ms{{route=\"{}\"}}", id.as_str()),
                    unit.applied,
                ),
            ]);
        }
        for route in s.units.iter().flat_map(|unit| &unit.routes) {
            if let Route::Postgres(ctx) = &route.route {
                let m = ctx.metrics.snapshot();
                values.extend([
                    ("nunya_connections_total".into(), m.connections),
                    (
                        "nunya_statements_allowed_total".into(),
                        m.statements_allowed,
                    ),
                    ("nunya_bytes_total".into(), m.bytes),
                ]);
                values.extend(
                    crate::error::NyCode::ALL
                        .iter()
                        .zip(m.statements_refused)
                        .map(|(code, n)| (format!("nunya_refused_{}_total", code.sqlstate()), n)),
                );
            }
        }
        values
    }
    /// Reload every unit manually, or just changed units after watcher debounce.
    pub(crate) async fn run(&self, mut reload: tokio::sync::mpsc::Receiver<()>, config: Option<Arc<crate::config_reload::ConfigReloader>>) {
        let mut config_watcher = config.as_ref().map(|c| Watcher {
            attempted: c.revision(),
            previous: None, readable: true,
        });
        let mut files: Vec<_> = {
            let state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state
                .units
                .iter()
                .map(|unit| {
                    (
                        unit.path.clone(),
                        Watcher {
                            attempted: unit.hash.clone(),
                            previous: None,
                            readable: true,
                        },
                    )
                })
                .collect()
        };
        let mut interval =
            tokio::time::interval(Duration::from_millis(self.watch_ms.unwrap_or(3600000)));
        interval.tick().await;
        let hup = crate::serve::hangup();
        tokio::pin!(hup);
        let mut reload_open = true;
        loop {
            let manual = tokio::select! {
                message = reload.recv(), if reload_open => {
                    if message.is_none() { reload_open = false; continue; }
                    true
                }
                _ = &mut hup => { hup.set(crate::serve::hangup()); true }
                _ = interval.tick(), if self.watch_ms.is_some() => false,
            };
            let mut reload_config = false;
            if let (Some(config), Some(watcher)) = (&config, &mut config_watcher) {
                let bytes = std::fs::read(config.path());
                if manual {
                    if let Ok(bytes) = &bytes { watcher.attempted = hash(bytes); }
                    watcher.previous = None;
                }
                reload_config = manual || watcher.poll(bytes.as_deref().ok());
            }
            let mut attempts = Vec::new();
            for (index, (path, watcher)) in files.iter_mut().enumerate() {
                let bytes = std::fs::read(&*path);
                if manual {
                    if let Ok(bytes) = &bytes { watcher.attempted = hash(bytes); }
                    watcher.previous = None;
                }
                if manual || watcher.poll(bytes.as_deref().ok()) {
                    attempts.push((index, path.clone(), bytes));
                }
            }
            // A policy watcher trigger also rereads config, even when its bytes
            // have not changed. A config-only trigger leaves policy units alone.
            reload_config |= !attempts.is_empty();
            config_before_policy(config.as_deref().filter(|_| reload_config), || {
                for (index, path, bytes) in attempts { self.attempt(index, &path, bytes); }
            }).await;
        }
    }
}

// The callback keeps policy attempts independent while making trigger order explicit.
async fn config_before_policy(config: Option<&crate::config_reload::ConfigReloader>, attempt: impl FnOnce()) {
    if let Some(config) = config { config.reload().await; }
    attempt();
}

struct Watcher {
    attempted: String,
    previous: Option<String>,
    readable: bool,
}
impl Watcher {
    fn poll(&mut self, bytes: Option<&[u8]>) -> bool {
        let Some(bytes) = bytes else {
            if self.readable {
                eprintln!("serve: policy watch: file unavailable");
            }
            self.readable = false;
            self.previous = None;
            return false;
        };
        self.readable = true;
        let digest = hash(bytes);
        let trigger = digest != self.attempted && self.previous.as_ref() == Some(&digest);
        self.previous = Some(digest.clone());
        if trigger {
            self.attempted = digest;
        }
        trigger
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        policy::{ReadLogSink, Treatment},
        schema::ColumnRef,
    };
    use std::collections::BTreeMap;
    fn policy() -> Policy {
        Policy {
            retired_domains: Default::default(),
            json_filters: Default::default(),
            columns: BTreeMap::new(),
            domains: BTreeMap::new(),
            snapshot_max_age_ms: 60000,
            readlog: ReadLogSink::Stdout,
        }
    }
    fn document(policy: Policy) -> PolicyDocument {
        PolicyDocument { policy, version: PolicyVersion::Legacy, route_default: RouteDefault::Show,
            links: vec![], json_paths: vec![], file_readlog: None, file_snapshot_max_age_ms: None }
    }
    fn id(name: &str) -> RouteId {
        RouteId::new(name).unwrap()
    }
    fn hub() -> Hub {
        let p = policy();
        Hub {
            log: ReloadableLog(
                Arc::new(RwLock::new(crate::readlog::open_sink(&p.readlog).unwrap())),
                Arc::new(RwLock::new(crate::serve::Revisions::default())),
                Arc::new(AtomicBool::new(true)),
            ),
            state: Mutex::new(State {
                units: vec![Unit {
                    identity: PathBuf::new(),
                    path: PathBuf::new(),
                    revisions: Arc::new(RwLock::new(crate::serve::Revisions {
                        policy: hash(b"initial"),
                        config: "config-test".into(),
                    })),
                    doc: Arc::new(document(p.clone())),
                    policy: Arc::new(p),
                    routes: vec![],
                    hash: hash(b"initial"),
                    applied: 1,
                    last: "none",
                    last_time: 0,
                    attempts: 0,
                    failures: 0,
                }],
                configured: vec![(id("postgres"), 0)],
            }),
            store: RwLock::new(None),
            authority: RwLock::new(std::sync::Weak::new()),
            pending_membership: Mutex::new(Default::default()),
            #[cfg(test)]
            reset_callback: RwLock::new(Some(Arc::new(|_, _| Ok(0)))) ,
            watch_ms: None,
            service: crate::policy::ServicePolicySettings::default(),
        }
    }
    struct Files {
        dir: PathBuf,
        a: PathBuf,
        b: PathBuf,
    }
    impl Files {
        fn new() -> Self {
            let mut nonce = [0; 16];
            getrandom::fill(&mut nonce).unwrap();
            let dir = std::env::temp_dir().join(format!(
                "nunya-units-{}",
                data_encoding::HEXLOWER.encode(&nonce)
            ));
            std::fs::create_dir_all(&dir).unwrap();
            let files = Self {
                a: dir.join("a.toml"),
                b: dir.join("b.toml"),
                dir,
            };
            files.write(&files.a, 60000);
            files.write(&files.b, 60001);
            files
        }
        fn write(&self, path: &Path, age: u64) {
            std::fs::write(
                path,
                format!("snapshot_max_age_ms={age}\n[readlog]\nsink='stdout'\n"),
            )
            .unwrap();
        }
        fn hub(&self) -> Arc<Hub> {
            Hub::new(
                &[(id("a"), self.a.clone()), (id("b"), self.b.clone())],
                None, crate::policy::ServicePolicySettings::default()
            )
            .unwrap()
        }
    }
    impl Drop for Files {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.dir).unwrap();
        }
    }
    fn test_route(name: &str, snapshot: Snapshot) -> Route {
        Route::Test(
            id(name),
            RwLock::new(Arc::new(policy())),
            RwLock::new(Arc::new(snapshot)),
        )
    }
    fn empty_snapshot() -> Snapshot {
        Snapshot::new(vec![], vec![], vec![], now())
    }
    async fn manual_reload(h: &Arc<Hub>) {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let worker = h.clone();
        let task = tokio::spawn(async move { worker.run(rx, None).await });
        tx.send(()).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if h.state
                    .lock()
                    .unwrap()
                    .units
                    .iter()
                    .all(|u| u.attempts == 1)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
    }
    #[test]
    fn separate_files_validate_only_their_own_catalog() {
        let files = Files::new();
        let bytes =
            "[readlog]\nsink='stdout'\n[[column]]\ntable='public.t'\nname='secret'\nmasked=true\n";
        std::fs::write(&files.a, bytes).unwrap();
        let snapshot = Snapshot::new(
            vec![crate::schema::Relation {
                schema: "public".into(),
                name: "t".into(),
                kind: crate::schema::RelationKind::Table,
                columns: vec![crate::schema::Column {
                    name: "secret".into(),
                    ty: crate::value::ColumnType::Text,
                    nullable: true,
                    generated: None,
                    default: None,
                }],
                view_definition: None,
                primary_key: vec![],
            }],
            vec![],
            vec![],
            now(),
        );
        let h = files.hub();
        h.register(test_route("a", snapshot.clone())).unwrap();
        h.register(test_route("b", empty_snapshot())).unwrap();
        assert!(h.readiness(true, true).0);
        let b = test_route("b", empty_snapshot());
        h.refresh(&b, empty_snapshot()).unwrap();
        let shared = Hub::new(
            &[
                (id("a"), files.a.clone()),
                (id("b"), files.dir.join(".").join("a.toml")),
            ],
            None, crate::policy::ServicePolicySettings::default()
        )
        .unwrap();
        assert_eq!(shared.state.lock().unwrap().units.len(), 1);
        shared.register(test_route("a", snapshot)).unwrap();
        assert!(shared.register(test_route("b", empty_snapshot())).is_err());
        assert!(
            shared
                .refresh(&test_route("b", empty_snapshot()), empty_snapshot())
                .is_err()
        );
        assert!(h.register(test_route("unknown", empty_snapshot())).is_err());
        assert!(
            h.refresh(&test_route("unknown", empty_snapshot()), empty_snapshot())
                .is_err()
        );
        assert!(h.policy_for(&id("unknown")).is_err());
        assert!(h.route_log(&id("unknown")).is_err());
        assert_eq!(h.policy_revision_for(&id("unknown")), None);
    }
    #[tokio::test]
    async fn independent_reload_per_unit_manual_also_applies_unchanged_bytes() {
        let files = Files::new();
        let h = files.hub();
        h.register(test_route("a", empty_snapshot())).unwrap();
        h.register(test_route("b", empty_snapshot())).unwrap();
        let a = h.policy_for(&id("a")).unwrap();
        let b = h.policy_for(&id("b")).unwrap();
        let old_a = h.policy_revision_for(&id("a"));
        let old_b = h.policy_revision_for(&id("b"));
        files.write(&files.a, 70000);
        manual_reload(&h).await;
        assert_ne!(h.policy_revision_for(&id("a")), old_a);
        assert_eq!(h.policy_revision_for(&id("b")), old_b);
        assert_eq!(a.snapshot_max_age_ms, 60000);
        assert_eq!(h.policy_for(&id("a")).unwrap().snapshot_max_age_ms, 70000);
        assert_eq!(
            h.policy_for(&id("b")).unwrap().snapshot_max_age_ms,
            b.snapshot_max_age_ms
        );
        // A manual reload republishes B's identical bytes, so its Arc and attempt
        // counter change, while its effective policy and revision stay unchanged.
        let metrics = h.metrics();
        for name in ["a", "b"] {
            assert!(
                metrics.contains(&(format!("nunya_policy_reloads_total{{route=\"{name}\"}}"), 1))
            );
        }
        assert!(h.readiness(true, true).0);
        let state = h.state.lock().unwrap();
        for unit in &state.units {
            assert!(Arc::ptr_eq(
                &unit.routes[0].policy,
                &unit.routes[0].locks().0.read().unwrap()
            ));
        }
    }
    #[tokio::test]
    async fn bad_edit_on_one_route_keeps_last_good_and_other_unaffected() {
        let files = Files::new();
        let h = files.hub();
        let old = h.policy_for(&id("a")).unwrap();
        let revision = h.policy_revision_for(&id("a"));
        std::fs::write(&files.a, [255]).unwrap();
        files.write(&files.b, 70000);
        manual_reload(&h).await;
        assert!(Arc::ptr_eq(&old, &h.policy_for(&id("a")).unwrap()));
        assert_eq!(h.policy_revision_for(&id("a")), revision);
        assert_eq!(h.policy_for(&id("b")).unwrap().snapshot_max_age_ms, 70000);
        assert!(
            h.status()
                .lines()
                .next()
                .unwrap()
                .contains("last_reload=rejected ")
        );
        assert!(
            h.status()
                .lines()
                .nth(1)
                .unwrap()
                .contains("last_reload=applied ")
        );
        assert!(
            h.metrics()
                .contains(&("nunya_policy_reload_failures_total{route=\"a\"}".into(), 1))
        );
        assert!(
            h.metrics()
                .contains(&("nunya_policy_reload_failures_total{route=\"b\"}".into(), 0))
        );
    }
    #[tokio::test]
    async fn readlog_spellings_agree_at_startup_and_reload() {
        let files = Files::new();
        let policy = |path: &str| format!("[readlog]\nsink='file'\npath='{path}'\n");
        std::fs::write(&files.a, policy("read.log")).unwrap();
        std::fs::write(
            &files.b,
            policy(&files.dir.join("read.log").to_string_lossy()),
        )
        .unwrap();
        let hub = files.hub();
        std::fs::write(&files.a, policy("./read.log")).unwrap();
        manual_reload(&hub).await;
        assert!(
            hub.state
                .lock()
                .unwrap()
                .units
                .iter()
                .all(|u| u.last == "applied")
        );
        let changed = parse(
            &files.a,
            b"[readlog]\nsink='file'\npath='read.log'\nmax_bytes=1\n", &crate::policy::ServicePolicySettings::default()
        )
        .unwrap();
        assert!(hub.apply(0, Ok(changed), Some(hash(b"changed"))).is_err());
    }

    #[tokio::test]
    async fn unit_retains_named_path_for_reload() {
        let files = Files::new();
        let named = files.dir.join("./a.toml");
        let hub = Hub::new(&[(id("a"), named.clone())], None, crate::policy::ServicePolicySettings::default()).unwrap();
        assert_eq!(hub.state.lock().unwrap().units[0].path, named);
        assert_eq!(hub.state.lock().unwrap().units[0].identity, std::fs::canonicalize(&named).unwrap());
        files.write(&files.a, 70000);
        manual_reload(&hub).await;
        assert_eq!(hub.policy_for(&id("a")).unwrap().snapshot_max_age_ms, 70000);
    }

    #[tokio::test]
    async fn symlink_policy_uses_named_directory_at_startup_and_reload() {
        let files = Files::new();
        let target_dir = files.dir.join("target");
        std::fs::create_dir(&target_dir).unwrap();
        let target = target_dir.join("policy.toml");
        let named = files.dir.join("named.toml");
        std::fs::write(&target, "[readlog]\nsink='file'\npath='read.log'\n").unwrap();
        #[cfg(windows)]
        if let Err(error) = std::os::windows::fs::symlink_file(&target, &named) {
            if error.raw_os_error() == Some(1314) {
                eprintln!("symlink regression requires Windows symlink privilege (1314)");
                return;
            }
            panic!("cannot create policy symlink: {error}");
        }
        #[cfg(unix)]
        std::os::unix::fs::symlink(&target, &named).unwrap();
        let hub = Hub::new(&[(id("a"), named.clone()), (id("b"), target.clone())], None, crate::policy::ServicePolicySettings::default()).unwrap();
        assert_eq!(hub.state.lock().unwrap().units.len(), 1);
        assert!(files.dir.join("read.log").exists());
        assert!(!target_dir.join("read.log").exists());
        std::fs::write(
            &target,
            "snapshot_max_age_ms=70000\n[readlog]\nsink='file'\npath='./read.log'\n",
        )
        .unwrap();
        manual_reload(&hub).await;
        assert_eq!(hub.policy_for(&id("a")).unwrap().snapshot_max_age_ms, 70000);
        assert_eq!(hub.state.lock().unwrap().units[0].path, named);
        assert!(!target_dir.join("read.log").exists());
    }
    #[test]
    fn readlog_must_agree_across_units() {
        let files = Files::new();
        let h = files.hub();
        let revision = h.policy_revision_for(&id("a"));
        let bytes = b"[readlog]\nsink='file'\npath='audit.log'\n";
        std::fs::write(&files.a, bytes).unwrap();
        assert_eq!(files.hub_error(), "policy files name different read logs");
        let candidate = parse(&files.a, bytes, &crate::policy::ServicePolicySettings::default()).unwrap();
        assert_eq!(
            h.apply(0, Ok(candidate), Some(hash(bytes))).unwrap_err(),
            "policy files name different read logs"
        );
        assert_eq!(h.policy_revision_for(&id("a")), revision);
        assert_eq!(h.state.lock().unwrap().units[0].failures, 1);
        // Distinct files with the same resolved relative audit path agree.
        std::fs::write(&files.b, bytes).unwrap();
        let shared_sink = files.hub();
        drop(shared_sink);
        // A single canonical unit (two routes) may still replace the sink.
        files.write(&files.a, 60000);
        let single = Hub::new(
            &[(id("a"), files.a.clone()), (id("b"), files.a.clone())],
            None, crate::policy::ServicePolicySettings::default()
        )
        .unwrap();
        single
            .apply(0, Ok(parse(&files.a, bytes, &crate::policy::ServicePolicySettings::default()).unwrap()), Some(hash(bytes)))
            .unwrap();
        assert_eq!(
            single.policy_revision_for(&id("a")),
            single.policy_revision_for(&id("b"))
        );
        assert!(
            single
                .metrics()
                .contains(&("nunya_policy_reloads_total{route=\"b\"}".into(), 1))
        );
    }
    impl Files {
        fn hub_error(&self) -> String {
            match Hub::new(
                &[(id("a"), self.a.clone()), (id("b"), self.b.clone())],
                None, crate::policy::ServicePolicySettings::default()
            ) {
                Ok(_) => panic!("expected audit sink mismatch"),
                Err(e) => e,
            }
        }
    }
    #[test]
    fn route_log_stamps_its_own_route_revision() {
        use crate::readlog::ReadLog;
        let files = Files::new();
        let log = "[readlog]\nsink='file'\npath='audit.log'\n";
        std::fs::write(&files.a, format!("snapshot_max_age_ms=60000\n{log}")).unwrap();
        std::fs::write(&files.b, format!("snapshot_max_age_ms=60001\n{log}")).unwrap();
        let h = files.hub();
        h.set_config_revision("config").unwrap();
        assert_ne!(
            h.policy_revision_for(&id("a")),
            h.policy_revision_for(&id("b"))
        );
        for name in ["a", "b"] {
            let route_log = h.route_log(&id(name)).unwrap();
            assert!(Arc::ptr_eq(&route_log.0, &h.log.0));
            assert!(Arc::ptr_eq(&route_log.2, &h.log.2));
            route_log
                .write(&crate::readlog::ReadLogRecord {
                    ts_unix_ms: 1,
                    principal: crate::token::PrincipalId([1; 16]),
                    session: crate::token::SessionId([2; 16]),
                    scope: crate::token::ScopeId([3; 16]),
                    connection: None,
                    credential_id: None,
                    route: name.into(),
                    policy_revision: None,
                    config_revision: None,
                    statement_fingerprint: "test".into(),
                    statement: "test".into(),
                    outcome: crate::readlog::Outcome::Allowed,
                    rows: 0,
                    tokens_released: vec![],
                })
                .unwrap();
        }
        let bytes = std::fs::read_to_string(files.dir.join("audit.log")).unwrap();
        let records: Vec<serde_json::Value> = bytes
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(records.len(), 2);
        for (record, route) in records.iter().zip(["a", "b"]) {
            assert_eq!(
                record["policy_revision"],
                h.policy_revision_for(&id(route)).unwrap()
            );
            assert_eq!(record["config_revision"], "config");
        }
    }
    #[tokio::test]
    async fn watcher_debounces_each_unit_without_attempting_unchanged_routes() {
        let files = Files::new();
        let h = Hub::new(
            &[(id("a"), files.a.clone()), (id("b"), files.b.clone())],
            Some(5), crate::policy::ServicePolicySettings::default()
        )
        .unwrap();
        let b = h.policy_for(&id("b")).unwrap();
        files.write(&files.a, 70000);
        let worker = h.clone();
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let task = tokio::spawn(async move { worker.run(rx, None).await });
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if h.state.lock().unwrap().units[0].attempts == 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(h.policy_for(&id("a")).unwrap().snapshot_max_age_ms, 70000);
        assert!(Arc::ptr_eq(&b, &h.policy_for(&id("b")).unwrap()));
        let state = h.state.lock().unwrap();
        assert_eq!(state.units[0].attempts, 1);
        assert_eq!(state.units[1].attempts, 0);
    }
    #[test]
    fn two_route_status_and_metrics_format() {
        let files = Files::new();
        let h = files.hub();
        let state = h.state.lock().unwrap();
        let expected: String = state.configured.iter().map(|(route, index)| {
            let unit = &state.units[*index];
            format!("route={} policy_sha256={} policy_applied_unix_ms={} last_reload=none last_reload_unix_ms=0 policy_watch=off\n", route.as_str(), unit.hash, unit.applied)
        }).collect();
        drop(state);
        assert_eq!(h.status(), expected);
        assert_eq!(h.metrics().len(), 6);
        for name in ["a", "b"] {
            assert!(
                h.metrics()
                    .contains(&(format!("nunya_policy_reloads_total{{route=\"{name}\"}}"), 0))
            );
            assert!(h.metrics().contains(&(
                format!("nunya_policy_reload_failures_total{{route=\"{name}\"}}"),
                0
            )));
        }
    }
    #[test]
    fn postgres_readiness_tracks_loading_staleness_and_dependencies() {
        let h = hub();
        assert_eq!(
            h.readiness(true, true),
            (
                false,
                "route=postgres policy=ok catalog=unavailable audit=ok store=ok\n".into()
            )
        );
        h.register(Route::Test(
            id("postgres"),
            RwLock::new(Arc::new(policy())),
            RwLock::new(Arc::new(Snapshot::new(vec![], vec![], vec![], now()))),
        ))
        .unwrap();
        assert_eq!(
            h.readiness(true, true),
            (
                true,
                "route=postgres policy=ok catalog=ok audit=ok store=ok\n".into()
            )
        );
        assert!(!h.readiness(false, true).0);
        assert!(h.readiness(false, true).1.contains("audit=unavailable"));
        assert!(!h.readiness(true, false).0);
        assert!(h.readiness(true, false).1.contains("store=unavailable"));
        *h.state.lock().unwrap().units[0].routes[0]
            .locks()
            .1
            .write()
            .unwrap() = Arc::new(Snapshot::new(vec![], vec![], vec![], 0));
        assert_eq!(
            h.readiness(true, true),
            (
                false,
                "route=postgres policy=ok catalog=stale audit=ok store=ok\n".into()
            )
        );
    }

    #[tokio::test]
    async fn config_poll_is_independent_and_policy_triggers_reread_config() {
        let files = crate::config_reload::tests::Fixture::new();
        let route = id("pg");
        let hub = Hub::new(&[(route, files.dir.join("policy"))], Some(5), crate::policy::ServicePolicySettings::default()).unwrap();
        let startup = crate::config::Config::load(&files.path).unwrap();
        let config = Arc::new(crate::config_reload::ConfigReloader::new(startup, Arc::downgrade(&hub)));
        files.load(&files.text.replace("max_connections=256", "max_connections=257"));
        let worker = hub.clone(); let worker_config = config.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let task = tokio::spawn(async move { worker.run(rx, Some(worker_config)).await });
        tokio::time::timeout(Duration::from_secs(5), async {
            while config.last_reload() != "applied" { tokio::time::sleep(Duration::from_millis(1)).await; }
        }).await.unwrap();
        assert_eq!(hub.state.lock().unwrap().units[0].attempts, 0);
        assert!(config.metrics().contains(&("nunya_config_reloads_total".into(), 1)));
        std::fs::write(files.dir.join("policy"), "snapshot_max_age_ms=60001\n[readlog]\nsink='stdout'\n").unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while hub.state.lock().unwrap().units[0].attempts < 1 { tokio::time::sleep(Duration::from_millis(1)).await; }
        }).await.unwrap();
        assert!(config.metrics().contains(&("nunya_config_reloads_total".into(), 2)));
        std::fs::write(&files.path, "invalid=[").unwrap();
        tx.send(()).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while hub.state.lock().unwrap().units[0].attempts < 2 { tokio::time::sleep(Duration::from_millis(1)).await; }
        }).await.unwrap();
        assert_eq!(config.last_reload(), "rejected");
        task.abort(); assert!(task.await.unwrap_err().is_cancelled());
    }

    #[tokio::test]
    async fn manual_trigger_reloads_config_before_policy_attempt() {
        let files = crate::config_reload::tests::Fixture::new();
        let hub = files.hub();
        let startup = crate::config::Config::load(&files.path).unwrap();
        let config = crate::config_reload::ConfigReloader::new(startup, Arc::downgrade(&hub));
        let new = files.load(&files.text.replace("max_connections=256", "max_connections=257"));
        config_before_policy(Some(&config), || {
            assert_eq!(config.last_reload(), "applied");
            assert_eq!(hub.log.1.read().unwrap().config, new.revision);
            assert_eq!(hub.state.lock().unwrap().units[0].attempts, 0);
            hub.attempt(0, &files.dir.join("policy"), std::fs::read(files.dir.join("policy")));
        }).await;
        assert_eq!(hub.state.lock().unwrap().units[0].attempts, 1);
    }

    #[test]
    fn watcher_debounces_rejections_and_recovers_readability() {
        let mut w = Watcher {
            attempted: hash(b"initial"),
            previous: None,
            readable: true,
        };
        assert!(!w.poll(Some(b"initial")));
        assert!(!w.poll(Some(b"partial")));
        assert!(!w.poll(Some(b"new")));
        assert!(w.poll(Some(b"new")));
        // A rejection is still an attempt; this hash must not trigger again.
        assert!(!w.poll(Some(b"new")));
        assert!(!w.poll(None));
        assert!(!w.readable);
        assert!(!w.poll(None));
        assert!(!w.poll(Some(b"next")));
        assert!(w.readable);
        assert!(w.poll(Some(b"next")));
    }
    #[test]
    fn reload_is_all_or_nothing_and_late_registration_uses_latest() {
        let h = hub();
        // Distinct catalogs: a policy referring to a column found only in the first
        // route must fail without publishing to either route.
        let column = ColumnRef {
            schema: "public".into(),
            table: "t".into(),
            column: "secret".into(),
        };
        let mut candidate = policy();
        candidate
            .columns
            .insert(column, Treatment::Masked { placeholder: None });
        let first = Snapshot::new(
            vec![crate::schema::Relation {
                schema: "public".into(),
                name: "t".into(),
                kind: crate::schema::RelationKind::Table,
                columns: vec![crate::schema::Column {
                    name: "secret".into(),
                    ty: crate::value::ColumnType::Text,
                    nullable: true,
                    generated: None,
                    default: None,
                }],
                view_definition: None,
                primary_key: vec![],
            }],
            vec![],
            vec![],
            0,
        );
        let second = Snapshot::new(vec![], vec![], vec![], 0);
        assert!(crate::policy::validate::validate(&candidate, &first).is_ok());
        assert!(crate::policy::validate::validate(&candidate, &second).is_err());
        for snapshot in [first, second] {
            h.register(Route::Test(
                id("postgres"),
                RwLock::new(Arc::new(policy())),
                RwLock::new(Arc::new(snapshot)),
            ))
            .unwrap();
        }
        let mut valid = policy();
        valid.snapshot_max_age_ms = 12345;
        h.apply(0, Ok(document(valid.clone())), Some(hash(b"valid"))).unwrap();
        assert!(h.apply(0, Ok(document(candidate)), Some(hash(b"invalid"))).is_err());
        let mut nonce = [0; 16];
        getrandom::fill(&mut nonce).unwrap();
        let missing_parent = std::env::temp_dir().join(format!(
            "nunya-reload-missing-{}",
            data_encoding::HEXLOWER.encode(&nonce)
        ));
        let mut bad_log = policy();
        bad_log.readlog = ReadLogSink::File {
            path: missing_parent.join("readlog"),
            max_bytes: 1024,
        };
        assert!(h.apply(0, Ok(document(bad_log)), Some(hash(b"bad-log"))).is_err());
        h.register(Route::Test(
            id("postgres"),
            RwLock::new(Arc::new(policy())),
            RwLock::new(Arc::new(Snapshot::new(vec![], vec![], vec![], 0))),
        ))
        .unwrap();
        let guard = h.state.lock().unwrap();
        let state = &guard.units[0];
        for route in &state.routes {
            assert_eq!(route.locks().0.read().unwrap().snapshot_max_age_ms, 12345);
        }
        assert_eq!(state.hash, hash(b"valid"));
        assert_eq!(state.failures, 2);
    }
    #[test]
    fn native_spanner_context_observes_reload_and_swappable_log() {
        use crate::{
            spanner::proxy::{SpannerContext, SpannerUpstream},
            token::{StandardCodec, keys::RootKey},
        };
        let mut nonce = [0; 16];
        getrandom::fill(&mut nonce).unwrap();
        let dir = std::env::temp_dir().join(format!(
            "nunya-reload-{}",
            data_encoding::HEXLOWER.encode(&nonce)
        ));
        let policy_path = dir.join("policy.toml");
        std::fs::create_dir_all(&dir).unwrap();
        let bytes = b"snapshot_max_age_ms = 60000\n[readlog]\nsink = 'stdout'\n";
        std::fs::write(&policy_path, bytes).unwrap();
        let h = Hub::new(
            &[
                (id("postgres"), policy_path.clone()),
                (id("spanner-native"), policy_path.clone()),
            ],
            Some(100), crate::policy::ServicePolicySettings::default()
        )
        .unwrap();
        h.set_config_revision("config-test").unwrap();
        assert_eq!(h.state.lock().unwrap().units[0].hash, hash(bytes));
        assert!(h.status().ends_with("policy_watch=100\n"));
        // Manual reloads apply unchanged bytes, and parse exactly the supplied
        // capture even if the path has already been replaced by another editor.
        std::fs::write(&policy_path, "invalid = [").unwrap();
        h.attempt(0, &policy_path, Ok(bytes.to_vec()));
        h.attempt(0, &policy_path, Ok(bytes.to_vec()));
        assert_eq!(h.state.lock().unwrap().units[0].attempts, 2);
        assert_eq!(h.state.lock().unwrap().units[0].hash, hash(bytes));
        let runtime = crate::state::runtime::StateRuntime::open(
            &dir.join("state"),
            &RootKey::from_hex(&"ab".repeat(32)).unwrap(),
            &crate::config::SessionSettings::default(),
            &[],
        )
        .unwrap();
        let ctx = Arc::new(SpannerContext {
            authority: runtime.authority.clone(),
            registry: runtime.registry.clone(),
            route: crate::state::model::RouteId::new("spanner-native").unwrap(),
            policy: RwLock::new(Arc::new(policy())),
            policy_revision: RwLock::new(None),
            snapshot: RwLock::new(Arc::new(Snapshot::new(vec![], vec![], vec![], 0))),
            root_key: RootKey::from_hex(&"ab".repeat(32)).unwrap(),
            readlog: Box::new(h.route_log(&id("spanner-native")).unwrap()),
            codec: StandardCodec,
            database: "test".into(),
            clock: Arc::new(now),
            upstream: SpannerUpstream::Emulator {
                endpoint: "http://127.0.0.1:1".into(),
            },
        });
        assert!(!h.readiness(true, true).0);
        *ctx.snapshot.write().unwrap() = Arc::new(Snapshot::new(vec![], vec![], vec![], now()));
        h.register(Route::Spanner(ctx.clone())).unwrap();
        assert_eq!(ctx.policy_revision.read().unwrap().as_deref(), Some(hash(bytes).as_str()));
        assert!(!h.readiness(true, true).0);
        h.register(Route::Test(
            id("postgres"),
            RwLock::new(Arc::new(policy())),
            RwLock::new(Arc::new(Snapshot::new(vec![], vec![], vec![], now()))),
        ))
        .unwrap();
        let (ready, body) = h.readiness(true, true);
        assert!(ready);
        assert_eq!(body.lines().count(), 2);
        assert!(body.starts_with("route=postgres "));
        assert!(body.contains("route=spanner-native "));
        let mut new = policy();
        new.snapshot_max_age_ms = 123;
        new.readlog = ReadLogSink::File {
            path: dir.join("audit.log"),
            max_bytes: 1024,
        };
        h.apply(0, Ok(document(new)), Some("R2".into())).unwrap();
        assert_eq!(ctx.policy_revision.read().unwrap().as_deref(), Some("R2"));
        assert_eq!(ctx.policy.read().unwrap().snapshot_max_age_ms, 123);
        ctx.readlog
            .write(&crate::readlog::ReadLogRecord {
                ts_unix_ms: 1,
                principal: crate::token::PrincipalId([1; 16]),
                session: crate::token::SessionId([2; 16]),
                scope: crate::token::ScopeId([3; 16]),
                connection: None,
                credential_id: None,
                route: ctx.route.as_str().into(),
                policy_revision: None,
                config_revision: None,
                statement_fingerprint: "test".into(),
                statement: "test".into(),
                outcome: crate::readlog::Outcome::Allowed,
                rows: 0,
                tokens_released: vec![],
            })
            .unwrap();
        let logged: serde_json::Value =
            serde_json::from_slice(&std::fs::read(dir.join("audit.log")).unwrap()).unwrap();
        assert_eq!(logged["policy_revision"], "R2");
        assert_eq!(logged["config_revision"], "config-test");
        let mut next = (*h.policy_for(&id("spanner-native")).unwrap()).clone();
        next.snapshot_max_age_ms = 124;
        h.apply(0, Ok(document(next)), Some(hash(b"next"))).unwrap();
        ctx.readlog
            .write(&crate::readlog::ReadLogRecord {
                ts_unix_ms: 2,
                principal: crate::token::PrincipalId([1; 16]),
                session: crate::token::SessionId([2; 16]),
                scope: crate::token::ScopeId([3; 16]),
                connection: None,
                credential_id: None,
                route: ctx.route.as_str().into(),
                policy_revision: None,
                config_revision: None,
                statement_fingerprint: "test".into(),
                statement: "test".into(),
                outcome: crate::readlog::Outcome::Allowed,
                rows: 0,
                tokens_released: vec![],
            })
            .unwrap();
        let bytes = std::fs::read_to_string(dir.join("audit.log")).unwrap();
        let logged: serde_json::Value =
            serde_json::from_str(bytes.lines().last().unwrap()).unwrap();
        assert_eq!(logged["policy_revision"], hash(b"next"));
        assert_eq!(logged["config_revision"], "config-test");
        drop(h);
        drop(ctx);
        drop(runtime);
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn startup_status_and_reload_metrics() {
        let h = hub();
        assert_eq!(
            h.status(),
            format!(
                "route=postgres policy_sha256={} policy_applied_unix_ms=1 last_reload=none last_reload_unix_ms=0 policy_watch=off\n",
                hash(b"initial")
            )
        );
        h.apply(0, Err("invalid".into()), None).unwrap_err();
        assert!(h.status().contains("last_reload=rejected "));
        let metrics = h.metrics();
        assert!(metrics.contains(&("nunya_policy_reloads_total{route=\"postgres\"}".into(), 1)));
        assert!(metrics.contains(&(
            "nunya_policy_reload_failures_total{route=\"postgres\"}".into(),
            1
        )));
        assert!(metrics.contains(&("nunya_policy_applied_unix_ms{route=\"postgres\"}".into(), 1)));
    }
    #[test]
    fn postgres_register_and_apply_publish_matching_policy_revision() {
        use crate::pgwire::{context::{GatewayContext, Metrics}, upstream::UpstreamConfig};
        let files = Files::new();
        let runtime = crate::state::runtime::StateRuntime::open(
            &files.dir.join("state"),
            &crate::token::keys::RootKey::from_hex(&"00".repeat(32)).unwrap(),
            &crate::config::SessionSettings::default(), &[],
        ).unwrap();
        let h = hub();
        let ctx = Arc::new(GatewayContext {
            authority: runtime.authority.clone(), registry: runtime.registry.clone(), route: id("postgres"),
            policy: RwLock::new(Arc::new(policy())), policy_revision: RwLock::new(None),
            snapshot: RwLock::new(Arc::new(Snapshot::new(vec![], vec![], vec!["public".into()], 0))),
            root_key: crate::token::keys::RootKey::from_hex(&"00".repeat(32)).unwrap(),
            readlog: Box::new(h.log.clone()),
            upstream: UpstreamConfig {
                host: "unused".into(), port: 5432, user: "unused".into(),
                password: zeroize::Zeroizing::new(String::new()), dbname: "unused".into(),
                tls: crate::pgwire::upstream::UpstreamTls::Disable, application_name: "nunya".into(), connect_timeout_ms: 100,
            },
            lowering: crate::dialect::pg::PgLowering,
            codec: crate::token::StandardCodec, clock: Arc::new(|| 0), cancel_map: Mutex::new(Default::default()),
            metrics: Metrics::default(), tls: Arc::new(std::sync::RwLock::new(None)), allow_plaintext: true,
        });
        h.register(Route::Postgres(ctx.clone())).unwrap();
        let (initial, revision) = ctx.policy_snapshot().unwrap();
        assert_eq!(revision, Some(hash(b"initial")));
        let mut next = policy();
        next.snapshot_max_age_ms = 42;
        h.apply(0, Ok(document(next)), Some("R2".into())).unwrap();
        let (current, revision) = ctx.policy_snapshot().unwrap();
        assert_eq!(revision.as_deref(), Some("R2"));
        assert_eq!(current.snapshot_max_age_ms, 42);
        assert_eq!(initial.snapshot_max_age_ms, 60000);
    }


    #[tokio::test]
    async fn service_settings_survive_policy_reload() {
        let files = Files::new();
        std::fs::write(&files.a, "").unwrap();
        let service = crate::policy::ServicePolicySettings { readlog: Some(ReadLogSink::Stdout), snapshot_max_age_ms: Some(123) };
        let h = Hub::new(&[(id("a"), files.a.clone())], None, service).unwrap();
        assert_eq!(h.policy_for(&id("a")).unwrap().snapshot_max_age_ms, 123);
        std::fs::write(&files.a, "# updated policy\n").unwrap();
        manual_reload(&h).await;
        assert_eq!(h.policy_for(&id("a")).unwrap().snapshot_max_age_ms, 123);
        assert_eq!(h.policy_for(&id("a")).unwrap().readlog, ReadLogSink::Stdout);
        assert_eq!(h.state.lock().unwrap().units[0].last, "applied");
    }

    fn customer_column(name: &str) -> ColumnRef {
        ColumnRef { schema: "public".into(), table: "customers".into(), column: name.into() }
    }
    fn customer_id(attnum: i16) -> crate::schema::ColumnId {
        crate::schema::ColumnId::Pg { relation_oid: 123, attnum }
    }
    fn customers(columns: &[(&str, i16)]) -> Snapshot {
        use crate::{schema::{Column, Relation, RelationKind}, value::ColumnType};
        Snapshot::new(vec![Relation {
            schema: "public".into(), name: "customers".into(), kind: RelationKind::Table,
            columns: columns.iter().map(|(name, _)| Column {
                name: (*name).into(), ty: ColumnType::Text, nullable: true, generated: None, default: None,
            }).collect(), view_definition: None, primary_key: vec![],
        }], vec![], vec!["public".into()], now()).with_column_ids(columns.iter().map(|(name, id)| {
            (customer_column(name), customer_id(*id))
        }).collect())
    }
    fn tokenize_hub(files: &Files, routes: &[&str]) -> Arc<Hub> {
        std::fs::write(&files.a, "version=2\ndefault='tokenize'\n").unwrap();
        Hub::new(&routes.iter().map(|name| (id(name), files.a.clone())).collect::<Vec<_>>(), None,
            crate::policy::ServicePolicySettings { readlog: Some(ReadLogSink::Stdout), ..Default::default() }).unwrap()
    }
    fn binding_store(files: &Files) -> Arc<crate::state::store::RedbStore> {
        Arc::new(crate::state::store::RedbStore::open(&files.dir.join("state"),
            &crate::token::keys::RootKey::from_hex(&"01".repeat(32)).unwrap()).unwrap())
    }
    #[test]
    fn link_files_register_and_publish_retired_domains_on_reload() {
        for mode in ["show", "tokenize"] {
            let files = Files::new();
            let initial = format!("version=2\ndefault='{mode}'\n");
            std::fs::write(&files.a, &initial).unwrap();
            let linked = format!("{initial}[[link]]\nfields=['public.customers.email','public.outbox.to_address']\n");
            let mut snapshot = customers(&[("email", 1)]);
            let mut outbox = snapshot.relations[0].clone();
            outbox.name = "outbox".into();
            outbox.columns[0].name = "to_address".into();
            snapshot.relations.push(outbox);
            let other = ColumnRef { schema: "public".into(), table: "outbox".into(), column: "to_address".into() };
            let snapshot = snapshot.with_column_ids([
                (customer_column("email"), customer_id(1)),
                (other.clone(), crate::schema::ColumnId::Pg { relation_oid: 124, attnum: 1 }),
            ].into());
            let settings = crate::policy::ServicePolicySettings { readlog: Some(ReadLogSink::Stdout), ..Default::default() };
            let hub = Hub::new(&[(id("a"), files.a.clone())], None, settings.clone()).unwrap();
            hub.set_store(binding_store(&files)).unwrap();
            hub.register(test_route("a", snapshot.clone())).unwrap();
            let before = hub.state.lock().unwrap().units[0].routes[0].policy.clone();
            let old: Vec<_> = [customer_column("email"), other.clone()].iter()
                .filter_map(|c| before.domain_of(c).cloned()).collect();
            let doc = parse(&files.a, linked.as_bytes(), &settings).unwrap();
            hub.apply(0, Ok(doc), Some(hash(linked.as_bytes()))).unwrap();
            let policy = hub.state.lock().unwrap().units[0].routes[0].policy.clone();
            assert!(policy.domain_of(&other).is_some());
            assert_eq!(policy.domain_of(&other), policy.domain_of(&customer_column("email")));
            for domain in old { assert!(policy.retired_domains.contains(&domain)); }
            // Also register directly from a linked file, for both route defaults.
            std::fs::write(&files.a, &linked).unwrap();
            let direct_files = Files::new();
            let direct = Hub::new(&[(id("b"), files.a.clone())], None, settings).unwrap();
            direct.set_store(binding_store(&direct_files)).unwrap();
            direct.register(test_route("b", snapshot)).unwrap();
            let policy = direct.state.lock().unwrap().units[0].routes[0].policy.clone();
            assert!(policy.domain_of(&other).is_some());
            assert_eq!(policy.domain_of(&other), policy.domain_of(&customer_column("email")));
        }
    }

    #[test]
    fn membership_reload_resets_before_publication_and_unlinks_last_show_link() {
        let files = Files::new();
        std::fs::write(&files.a, "version=2\ndefault='show'\n").unwrap();
        let settings = crate::policy::ServicePolicySettings { readlog: Some(ReadLogSink::Stdout), ..Default::default() };
        let hub = Hub::new(&[(id("a"), files.a.clone()), (id("b"), files.b.clone())], None, settings).unwrap();
        let store = binding_store(&files);
        hub.set_store(store.clone()).unwrap();
        hub.register(test_route("a", customers(&[("email", 1), ("other", 2)]))).unwrap();
        hub.register(test_route("b", customers(&[("email", 1), ("other", 2)]))).unwrap();
        let resets = Arc::new(Mutex::new(Vec::new()));
        let seen = resets.clone();
        *hub.reset_callback.write().unwrap() = Some(Arc::new(move |change, old| {
            // These are the actual publication guards, still containing the old policy.
            assert_eq!(change.route, id("a"));
            let old_linked = old.domain_of(&customer_column("email")) == old.domain_of(&customer_column("other"))
                && old.domain_of(&customer_column("email")).is_some();
            seen.lock().unwrap().push(old_linked);
            Ok(3)
        }));
        let doc = (*hub.state.lock().unwrap().units[0].doc).clone();
        let mut linked = doc.clone();
        linked.links.push(crate::policy::LinkRule {
            fields: vec![customer_column("email"), customer_column("other")], domain: None,
        });
        hub.apply(0, Ok(linked.clone()), Some(hash(b"linked"))).unwrap();
        assert_eq!(*resets.lock().unwrap(), vec![false]);
        hub.apply(0, Ok(linked), Some(hash(b"unchanged"))).unwrap();
        assert_eq!(resets.lock().unwrap().len(), 1);
        hub.apply(0, Ok(doc), Some(hash(b"unlinked"))).unwrap();
        assert_eq!(*resets.lock().unwrap(), vec![false, true]);
        let records = bindings::load(store.as_ref(), &id("a")).unwrap();
        assert!(records.iter().all(|(_, record)| !record.linked));
        assert!(hub.readiness(true, true).0);
    }

    #[test]
    fn later_route_failure_blocks_stale_refresh_until_reload() {
        let files = Files::new();
        let hub = tokenize_hub(&files, &["a", "b"]);
        let store = binding_store(&files);
        hub.set_store(store.clone()).unwrap();
        let a = test_route("a", customers(&[("email", 1), ("other", 2)]));
        let b = test_route("b", customers(&[("email", 1)]));
        hub.register(a).unwrap(); hub.register(b).unwrap();
        let old = hub.state.lock().unwrap().units[0].routes[0].locks().0.read().unwrap().clone();
        let mut doc = (*hub.state.lock().unwrap().units[0].doc).clone();
        doc.links.push(crate::policy::LinkRule { fields: vec![customer_column("email"), customer_column("other")], domain: None });
        assert!(hub.apply(0, Ok(doc.clone()), Some(hash(b"linked"))).is_err());
        assert!(!hub.readiness(true, true).0);
        assert!(hub.state.lock().unwrap().units[0].routes[0].membership_failed);
        assert!(Arc::ptr_eq(&old, &hub.state.lock().unwrap().units[0].routes[0].locks().0.read().unwrap()));
        let records = bindings::load(store.as_ref(), &id("a")).unwrap();
        let retired = bindings::load_retired(store.as_ref(), &id("a")).unwrap();
        {
            let mut state = hub.state.lock().unwrap();
            // Exercise the production refresh with the registered route's identity;
            // refusal happens before any route lock is acquired.
            let route = test_route("a", customers(&[("email", 1), ("other", 2)]));
            assert!(hub.publish_refresh(&mut state.units[0], &route, customers(&[("email", 1), ("other", 2)]), || Ok(())).is_err());
        }
        assert_eq!(records, bindings::load(store.as_ref(), &id("a")).unwrap());
        assert_eq!(retired, bindings::load_retired(store.as_ref(), &id("a")).unwrap());
        *hub.state.lock().unwrap().units[0].routes[1].locks().1.write().unwrap() = Arc::new(customers(&[("email", 1), ("other", 2)]));
        hub.apply(0, Ok(doc), Some(hash(b"recovered"))).unwrap();
        assert!(hub.readiness(true, true).0);
        let state = hub.state.lock().unwrap();
        let policy = state.units[0].routes[0].locks().0.read().unwrap();
        assert_eq!(policy.domain_of(&customer_column("email")), policy.domain_of(&customer_column("other")));
    }

    #[test]
    fn durable_membership_reset_survives_hub_restart() {
        let files = Files::new();
        let hub = tokenize_hub(&files, &["a"]);
        let store = binding_store(&files);
        hub.set_store(store.clone()).unwrap();
        hub.register(test_route("a", customers(&[("email", 1), ("other", 2)]))).unwrap();
        use crate::state::{authority::{AcquireRequest, Authority, SessionLimits}, model::{EndReason, Mode, PrincipalKind, SessionState, Verifier}};
        let authority = Authority::load(store.clone(), SessionLimits::default()).unwrap();
        let principal = authority.create_principal("restart-agent", PrincipalKind::Agent, false, "principal").unwrap();
        authority.set_grants(principal.id, vec![(id("a"), Mode::ReadOnly)]).unwrap();
        let session = authority.acquire_session(principal.id, AcquireRequest {
            route: id("a"), mode: None, lifetime_ms: None, isolated: false, label: None,
            idempotency_key: "session".into(),
            verifier: Verifier(crate::pgwire::auth::ScramVerifier::from_password("test", b"salt", 2)),
            secret_sha256: Sha256::digest(b"test").into(),
        }).unwrap();
        let mut doc = (*hub.state.lock().unwrap().units[0].doc).clone();
        doc.links.push(crate::policy::LinkRule { fields: vec![customer_column("email"), customer_column("other")], domain: None });
        *hub.reset_callback.write().unwrap() = None;
        assert!(hub.apply(0, Ok(doc), Some(hash(b"linked"))).is_err());
        let owed = bindings::load_reset(store.as_ref(), &id("a")).unwrap().unwrap();
        assert!(!owed.retired.is_empty());
        drop(hub); drop(authority); drop(store);
        std::fs::write(&files.a, "version=2\ndefault='tokenize'\n[[link]]\nfields=['public.customers.email','public.customers.other']\n").unwrap();
        let audit = files.dir.join("restart-audit.jsonl");
        let hub = Hub::new(&[(id("a"), files.a.clone())], None,
            crate::policy::ServicePolicySettings { readlog: Some(ReadLogSink::File { path: audit.clone(), max_bytes: 1000000 }), ..Default::default() }).unwrap();
        let store = binding_store(&files); hub.set_store(store.clone()).unwrap();
        let authority = Authority::load(store.clone(), SessionLimits::default()).unwrap();
        assert_eq!(authority.session(session.id).unwrap().state, SessionState::Active);
        hub.set_authority(Arc::downgrade(&authority)).unwrap();
        hub.register(test_route("a", customers(&[("email", 1), ("other", 2)]))).unwrap();
        assert_eq!(authority.session(session.id).unwrap().state, SessionState::Revoked);
        assert_eq!(authority.session(session.id).unwrap().revoke_reason, Some(EndReason::PolicyReset));
        assert!(bindings::load_reset(store.as_ref(), &id("a")).unwrap().is_none());
        let log = std::fs::read_to_string(audit).unwrap();
        assert!(log.contains("membership_reset"));
        assert!(log.contains("\"sessions\":1"));
        for domain in owed.retired { assert!(log.contains(&domain.0)); }
    }

    #[test]
    fn binding_failure_marks_readiness_but_invalid_policy_does_not() {
        let files = Files::new();
        let hub = tokenize_hub(&files, &["a"]);
        let store = binding_store(&files);
        hub.set_store(store.clone()).unwrap();
        hub.register(test_route("a", customers(&[("email", 1), ("other", 2)]))).unwrap();
        let mut doc = (*hub.state.lock().unwrap().units[0].doc).clone();
        doc.links.push(crate::policy::LinkRule { fields: vec![customer_column("email"), customer_column("other")], domain: None });
        let before = hub.state.lock().unwrap().units[0].routes[0].policy.clone();
        store.fail_commits(true);
        assert!(hub.apply(0, Ok(doc.clone()), Some(hash(b"failed"))).is_err());
        assert!(!hub.readiness(true, true).0);
        assert!(Arc::ptr_eq(&before, &hub.state.lock().unwrap().units[0].routes[0].policy));
        store.fail_commits(false);
        hub.apply(0, Ok(doc), Some(hash(b"recovered"))).unwrap();
        assert!(hub.readiness(true, true).0);
        assert!(hub.apply(0, Err("invalid policy file".into()), None).is_err());
        assert!(hub.readiness(true, true).0);
    }

    #[test]
    fn refused_reset_is_retained_for_retry_after_bindings_persist() {
        let files = Files::new();
        let hub = tokenize_hub(&files, &["a"]);
        hub.set_store(binding_store(&files)).unwrap();
        *hub.reset_callback.write().unwrap() = None;
        assert!(hub.register(test_route("a", customers(&[("email", 1)]))).is_err());
        let calls = Arc::new(Mutex::new(0)); let count = calls.clone();
        *hub.reset_callback.write().unwrap() = Some(Arc::new(move |_, _| { *count.lock().unwrap() += 1; Ok(0) }));
        hub.register(test_route("a", customers(&[("email", 1)]))).unwrap();
        assert_eq!(*calls.lock().unwrap(), 1);
    }

    #[test]
    fn tokenize_registers_index_and_view_with_base_column_rewrites() {
        use crate::{dialect::{Lowering, pg::PgLowering}, ir::{Label, OutputRewrite}, schema::RelationKind};
        let files = Files::new();
        let hub = tokenize_hub(&files, &["a"]);
        hub.set_store(binding_store(&files)).unwrap();
        let mut doc = (*hub.state.lock().unwrap().units[0].doc).clone();
        doc.policy.columns.insert(customer_column("masked"), Treatment::Masked { placeholder: None });
        doc.policy.columns.insert(customer_column("visible"), Treatment::Show);
        hub.apply(0, Ok(doc), Some(hash(b"view rules"))).unwrap();
        let mut snapshot = customers(&[("email", 1), ("masked", 2), ("visible", 3)]);
        let mut index = snapshot.relations[0].clone();
        index.name = "customers_pkey".into();
        index.kind = RelationKind::Other;
        index.columns.truncate(1);
        let mut view = snapshot.relations[0].clone();
        view.name = "customer_view".into();
        view.kind = RelationKind::View;
        view.view_definition = Some("SELECT email, masked, visible FROM public.customers".into());
        snapshot.relations.extend([index, view]);
        hub.register(test_route("a", snapshot.clone())).unwrap();
        let state = hub.state.lock().unwrap();
        let served = state.units[0].routes[0].locks().0.read().unwrap();
        let statement = PgLowering.lower("SELECT email, masked, visible FROM public.customer_view", &snapshot, &served, &[]).unwrap().remove(0);
        assert_eq!(statement.outputs[0].source, Some(customer_column("email")));
        assert!(matches!(&statement.outputs[0].label, Label::Blind(domain) if Some(domain) == served.domain_of(&customer_column("email"))));
        assert_eq!(statement.outputs[1].label, Label::Masked);
        assert_eq!(statement.outputs[2].label, Label::Plain);
        let plan = crate::ir::check::check(&statement, &served, &snapshot).unwrap();
        assert!(matches!(&plan.outputs[0], OutputRewrite::Tokenize { domain, .. } if Some(domain) == served.domain_of(&customer_column("email"))));
        assert_eq!(plan.outputs[1], OutputRewrite::Mask { placeholder: None });
        assert_eq!(plan.outputs[2], OutputRewrite::Pass);
    }

    #[test]
    fn tokenize_compilation_still_refuses_dependents_reading_default_blind_columns() {
        use crate::schema::{DependentKind, DependentObject};
        let files = Files::new();
        let hub = tokenize_hub(&files, &["a"]);
        hub.set_store(binding_store(&files)).unwrap();
        let mut snapshot = customers(&[("email", 1)]);
        snapshot.dependents.push(DependentObject {
            name: "copy_email".into(),
            kind: DependentKind::Trigger,
            reads: vec![customer_column("email")],
            writes_table: None,
        });
        let error = hub.register(test_route("a", snapshot)).unwrap_err();
        assert!(error.contains("trigger copy_email reads blind column public.customers.email"));
        assert!(hub.state.lock().unwrap().units[0].routes.is_empty());
    }

    #[test]
    fn tokenize_registration_requires_store_and_publishes_persisted_generated_domain() {
        let files = Files::new();
        let hub = tokenize_hub(&files, &["a"]);
        assert_eq!(hub.register(test_route("a", customers(&[("email", 1)]))).unwrap_err(), "policy bindings unavailable");
        assert!(hub.state.lock().unwrap().units[0].routes.is_empty());
        let store = binding_store(&files);
        hub.set_store(store.clone()).unwrap();
        hub.register(test_route("a", customers(&[("email", 1)]))).unwrap();
        let persisted = bindings::load(store.as_ref(), &id("a")).unwrap();
        let domain = persisted.get(&customer_id(1)).unwrap().domain.as_ref().unwrap();
        let state = hub.state.lock().unwrap();
        let served = state.units[0].routes[0].locks().0.read().unwrap();
        assert_eq!(served.domain_of(&customer_column("email")), Some(domain));
        assert!(served.domains.contains_key(domain));
        drop(served); drop(state);
        assert!(hub.readiness(true, true).0);
        assert!(hub.set_store(store).is_err());
    }
    #[test]
    fn refresh_persists_before_pair_publication_and_recovers_readiness() {
        let files = Files::new();
        let hub = tokenize_hub(&files, &["postgres"]);
        let store = binding_store(&files);
        hub.set_store(store.clone()).unwrap();
        let fixture = crate::pgwire::session::tests::Fixture::new();
        let ctx = &fixture.ctx;
        *ctx.snapshot.write().unwrap() = Arc::new(customers(&[("email", 1)]));
        hub.register(Route::Postgres(ctx.clone())).unwrap();
        let original_policy = ctx.policy.read().unwrap().clone();
        let original_snapshot = ctx.snapshot.read().unwrap().clone();
        let next = customers(&[("email", 1), ("recovery_email", 2)]);
        store.fail_commits(true);
        assert!(hub.refresh_if(&Route::Postgres(ctx.clone()), next.clone(), || true).is_err());
        assert!(Arc::ptr_eq(&original_policy, &ctx.policy.read().unwrap()));
        assert!(Arc::ptr_eq(&original_snapshot, &ctx.snapshot.read().unwrap()));
        assert!(bindings::load(store.as_ref(), &ctx.route).unwrap().get(&customer_id(2)).is_none());
        assert!(!hub.readiness(true, true).0);
        assert!(hub.readiness(true, true).1.contains("policy=unavailable"));
        store.fail_commits(false);
        // A stale capture neither publishes nor clears the failed-refresh readiness.
        hub.refresh_if(&Route::Postgres(ctx.clone()), next.clone(), || false).unwrap();
        assert!(!hub.readiness(true, true).0);
        hub.refresh_if(&Route::Postgres(ctx.clone()), next, || true).unwrap();
        let persisted = bindings::load(store.as_ref(), &ctx.route).unwrap();
        let policy = ctx.policy.read().unwrap();
        let snapshot = ctx.snapshot.read().unwrap();
        let added = customer_column("recovery_email");
        assert!(snapshot.column(&added).is_some());
        assert_eq!(policy.domain_of(&added), persisted.get(&customer_id(2)).unwrap().domain.as_ref());
        assert_ne!(policy.domain_of(&added), policy.domain_of(&customer_column("email")));
        drop(snapshot); drop(policy);
        assert!(hub.readiness(true, true).0);
        // A policy reload cannot recover a snapshot refresh failure.
        store.fail_commits(true);
        assert!(hub.refresh(&Route::Postgres(ctx.clone()), customers(&[("email", 1), ("recovery_email", 2), ("extra", 3)])).is_err());
        store.fail_commits(false);
        let doc = (*hub.state.lock().unwrap().units[0].doc).clone();
        hub.apply(0, Ok(doc), Some(hash(b"reapplied"))).unwrap();
        assert!(!hub.readiness(true, true).0);
        hub.commit_config_revision("unchanged", || Ok(())).unwrap();
        assert!(!hub.readiness(true, true).0);
        hub.refresh(&Route::Postgres(ctx.clone()), customers(&[("email", 1), ("recovery_email", 2), ("extra", 3)])).unwrap();
        assert!(hub.readiness(true, true).0);
        // Readiness detects a foreign policy replacement even when its values agree.
        let foreign = Arc::new((**ctx.policy.read().unwrap()).clone());
        *ctx.policy.write().unwrap() = foreign;
        assert!(!hub.readiness(true, true).0);
    }
    #[test]
    fn legacy_registration_compiles_declared_and_unlisted_columns_without_store() {
        let files = Files::new();
        std::fs::write(&files.a, "[readlog]\nsink='stdout'\n[[column]]\ntable='public.customers'\nname='email'\nmasked=true\n").unwrap();
        let hub = Hub::new(&[(id("a"), files.a.clone())], None, Default::default()).unwrap();
        hub.register(test_route("a", customers(&[("email", 1), ("other", 2)]))).unwrap();
        let state = hub.state.lock().unwrap();
        let policy = state.units[0].routes[0].locks().0.read().unwrap();
        assert_eq!(policy.treatment(&customer_column("email")), Some(&Treatment::Masked { placeholder: None }));
        assert_eq!(policy.treatment(&customer_column("other")), Some(&Treatment::Show));
        assert_eq!(policy.columns.len(), 2);
    }
    #[test]
    fn renamed_show_rule_validates_warns_and_compiles_only_with_active_anchor() {
        let files = Files::new();
        let hub = tokenize_hub(&files, &["a"]);
        let store = binding_store(&files);
        hub.set_store(store.clone()).unwrap();
        let mut doc = (*hub.state.lock().unwrap().units[0].doc).clone();
        doc.policy.columns.insert(customer_column("email"), Treatment::Show);
        hub.apply(0, Ok(doc.clone()), Some(hash(b"show"))).unwrap();
        hub.register(test_route("a", customers(&[("email", 1)]))).unwrap();
        let renamed = customers(&[("contact", 1)]);
        let bindings = bindings::load(store.as_ref(), &id("a")).unwrap();
        let warnings = crate::policy::validate::validate_document(&doc, &renamed, &bindings).unwrap();
        assert_eq!(warnings[0].message, "show rule names public.customers.email, renamed to public.customers.contact");
        let (compiled, _) = hub.compile(&doc, &id("a"), &renamed).unwrap();
        assert_eq!(compiled.treatment(&customer_column("contact")), Some(&Treatment::Show));
        let empty = RouteBindings::default();
        assert!(crate::policy::validate::validate_document(&doc, &renamed, &empty).is_err());
        let unanchored = crate::policy::effective::served_policy(&doc, &renamed, &empty);
        assert_eq!(unanchored.treatment(&customer_column("contact")), Some(&Treatment::Masked { placeholder: None }));
        let mut inactive = bindings.clone();
        inactive.get_mut(&customer_id(1)).unwrap().status = crate::policy::bindings::BindingStatus::NeedsRepair;
        assert!(crate::policy::validate::validate_document(&doc, &renamed, &inactive).is_err());
        assert!(crate::policy::validate::validate_document(&doc, &customers(&[("contact", 2)]), &bindings).is_err());
        for treatment in [Treatment::Masked { placeholder: None }, Treatment::Blind { domain: crate::policy::DomainName("secret".into()) }] {
            doc.policy.columns.insert(customer_column("email"), treatment);
            assert!(crate::policy::validate::validate_document(&doc, &renamed, &bindings).is_err());
        }
    }

    #[test]
    fn policy_reload_recompiles_each_route_and_rejects_failed_binding_writes_atomically() {
        let files = Files::new();
        let hub = Hub::new(&[(id("a"), files.a.clone()), (id("b"), files.a.clone())], None, Default::default()).unwrap();
        let store = binding_store(&files);
        hub.set_store(store.clone()).unwrap();
        hub.register(test_route("a", customers(&[("email", 1)]))).unwrap();
        hub.register(test_route("b", customers(&[("other", 2)]))).unwrap();
        let before: Vec<_> = hub.state.lock().unwrap().units[0].routes.iter()
            .map(|r| r.locks().0.read().unwrap().clone()).collect();
        let bytes = b"version=2\ndefault='tokenize'\n";
        let doc = parse(&files.a, bytes, &crate::policy::ServicePolicySettings {
            readlog: Some(ReadLogSink::Stdout), ..Default::default()
        }).unwrap();
        store.fail_commits(true);
        assert!(hub.apply(0, Ok(doc.clone()), Some(hash(bytes))).is_err());
        {
            let state = hub.state.lock().unwrap();
            for (route, old) in state.units[0].routes.iter().zip(&before) {
                assert!(Arc::ptr_eq(&route.locks().0.read().unwrap(), old));
            }
            assert_eq!(state.units[0].doc.version, PolicyVersion::Legacy);
        }
        store.fail_commits(false);
        hub.apply(0, Ok(doc), Some(hash(bytes))).unwrap();
        let a = bindings::load(store.as_ref(), &id("a")).unwrap();
        let b = bindings::load(store.as_ref(), &id("b")).unwrap();
        let state = hub.state.lock().unwrap();
        let policies: Vec<_> = state.units[0].routes.iter().map(|r| r.locks().0.read().unwrap()).collect();
        assert_eq!(policies[0].domain_of(&customer_column("email")), a.get(&customer_id(1)).unwrap().domain.as_ref());
        assert_eq!(policies[1].domain_of(&customer_column("other")), b.get(&customer_id(2)).unwrap().domain.as_ref());
        assert_ne!(a.get(&customer_id(1)).unwrap().domain, b.get(&customer_id(2)).unwrap().domain);
        assert!(policies[0].treatment(&customer_column("other")).is_none());
        assert!(policies[1].treatment(&customer_column("email")).is_none());
        drop(policies); drop(state);
        assert!(hub.readiness(true, true).0);
    }

}
