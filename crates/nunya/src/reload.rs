//! Shared policy reload coordination for every serving route.
use crate::{
    policy::{Policy, PolicyDocument, PolicyVersion, RouteDefault, bindings::{self, RouteBindings, MembershipChange}},
    schema::Snapshot,
    serve::{ReloadableLog, now, parse_policy_bytes, readlog_key},
    state::model::RouteId,
};
use sha2::{Digest, Sha256};
use crate::policy::store::{RevisionStore, NewRevision, Owner, PolicyRevision, RevisionKind, RevisionSummary, RevisionStoreError, WriteOutcome};
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

    fn fence(&self) -> Option<&Arc<crate::policy::RevisionFence>> {
        match self {
            Self::Postgres(c) => Some(&c.fence),
            Self::Spanner(c) => Some(&c.fence),
            #[cfg(test)]
            Self::Test(..) => None,
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
    source: String,
    service_failed: bool,
    ignored_logged: bool,
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
    /// Optional process serving gate, set before routes are registered.
    pub(crate) serving_gate: Option<Arc<crate::policy::serving::ServingGate>>,
    #[cfg(feature = "kubernetes")]
    pub(crate) activation_tracker: RwLock<Option<Arc<crate::policy::kube_activation::ActivationTracker>>>,
    #[cfg(feature = "kubernetes")]
    peer_bindings: RwLock<Option<PeerBindings>>,
    state: Mutex<State>,
    /// Service lifecycle and management audit log.
    pub(crate) log: ReloadableLog,
    store: RwLock<Option<Arc<dyn crate::state::store::Store>>>,
    revision_store: RwLock<Option<Arc<dyn RevisionStore>>>,
    authority: RwLock<std::sync::Weak<crate::state::authority::Authority>>,
    file_owners: RwLock<Vec<String>>,
    route_owners: RwLock<std::collections::BTreeMap<String, Owner>>,
    pending_membership: Mutex<std::collections::BTreeMap<String, MembershipChange>>,
    #[cfg(test)]
    reset_callback: RwLock<Option<ResetCallback>>,
    watch_ms: Option<u64>,
    service: crate::policy::ServicePolicySettings,
}
#[cfg(feature = "kubernetes")]
type BindingsProvider = Arc<dyn Fn() -> Option<Arc<bindings::BindingsSnapshot>> + Send + Sync>;
#[cfg(feature = "kubernetes")]
struct PeerBindings {
    leader: Arc<dyn Fn() -> bool + Send + Sync>,
    snapshot: BindingsProvider,
}
#[cfg(feature = "kubernetes")]
fn stamp_number(stamp: &str) -> Option<u64> {
    stamp.strip_prefix("rev-")?.split('-').next()?.parse().ok()
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
/// Data-free management policy operation failures.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
#[allow(dead_code, clippy::large_enum_variant)]
pub(crate) enum PolicyOpError {
    /// A publication or runtime resource is unavailable.
    #[error("policy unavailable")]
    Unavailable,
    /// The route is not configured.
    #[error("unknown route")]
    UnknownRoute,
    /// The route shares its source file with another route.
    #[error("shared policy file")]
    SharedPolicyFile,
    /// The route was already imported.
    #[error("already service-owned")]
    AlreadyServiceOwned,
    /// Explicit file ownership remains configured.
    #[error("file owner configured")]
    FileOwnerConfigured,
    /// The route still belongs to its file.
    #[error("not service-owned")]
    NotServiceOwned,
    /// The requested revision does not exist.
    #[error("revision unavailable")]
    MissingRevision,
    /// The expected revision differs from the active revision.
    #[error("stale policy revision")]
    Stale { current: RevisionSummary },
    /// The policy failed parsing or validation with sanitized messages.
    #[error("invalid policy")]
    Invalid(Vec<String>),
    /// The policy changes service infrastructure settings.
    #[error("infrastructure setting change")]
    InfrastructureSetting,
    /// The captured catalog changed since the request.
    #[error("catalog changed")]
    CatalogChanged { current: String },
    /// The durable revision backend failed.
    #[error("policy revision store failure")]
    Store(#[from] RevisionStoreError),
}
fn revision_stamp(r: &PolicyRevision) -> String {
    format!("rev-{}-{}", r.number, r.policy_sha256.chars().take(12).collect::<String>())
}
// Include the full request and its preconditions, with unambiguous field framing.
fn request_digest(actor: &str, toml: &str, expected: Option<u64>, fingerprint: &str, kind: &RevisionKind) -> String {
    let fingerprint = if matches!(kind, RevisionKind::Rollback { .. }) { "" } else { fingerprint };
    hash(&crate::state::model::encode(&(actor, toml, expected, fingerprint, kind)))
}
#[allow(dead_code, clippy::result_large_err)] // Management endpoints are wired in the next stack.
impl Hub {
    /// Attach revision storage and load service documents before registering routes.
    pub(crate) fn set_revision_store(&self, store: Arc<dyn RevisionStore>) -> Result<(), String> {
        let mut state = self.state.lock().map_err(|_| "policy lock unavailable")?;
        if state.units.iter().any(|u| !u.routes.is_empty()) { return Err("policy store must be set before registration".into()); }
        *self.revision_store.write().map_err(|_| "policy revision store unavailable")? = Some(store.clone());
        let configured = state.configured.clone();
        for (route, index) in &configured {
            state.units[*index].service_failed = true;
            let owner = store.owner(route).map_err(|_| "service policy unavailable")?;
            self.route_owners.write().map_err(|_| "policy ownership unavailable")?.insert(route.as_str().into(), owner);
            if owner == Owner::Service && configured.iter().filter(|(_, i)| i == index).count() != 1 {
                state.units[*index].service_failed = true;
                return Err("service policy cannot share a policy file".into());
            }
        }
        for (route, index) in configured {
            let unit = &mut state.units[index];
            unit.service_failed = true;
            let owner = store.owner(&route).map_err(|_| "service policy unavailable")?;
            if owner == Owner::File { unit.service_failed = false; continue; }
            let revision = store.active(&route).map_err(|_| "service policy unavailable")?.ok_or("service policy unavailable")?;
            let doc = parse(&unit.path, revision.policy_toml.as_bytes(), &self.service).map_err(|_| "service policy unavailable")?;
            unit.policy = Arc::new(doc.policy.clone());
            unit.doc = Arc::new(doc);
            unit.source = revision.policy_toml.clone();
            unit.hash = revision.policy_sha256.clone();
            unit.applied = revision.created_unix_ms;
            unit.revisions.write().map_err(|_| "revision lock unavailable")?.policy = revision_stamp(&revision);
            unit.service_failed = false;
        }
        Ok(())
    }
    fn revision_backend(&self) -> Result<Arc<dyn RevisionStore>, PolicyOpError> {
        self.revision_store.read().map_err(|_| PolicyOpError::Unavailable)?.clone().ok_or(PolicyOpError::Unavailable)
    }
    /// Read durable route ownership.
    pub(crate) fn owner(&self, route: &RouteId) -> Result<Owner, PolicyOpError> {
        match self.revision_store.read().map_err(|_| PolicyOpError::Unavailable)?.as_ref() {
            Some(s) => Ok(s.owner(route)?), None => Ok(Owner::File),
        }
    }
    /// Read the active durable revision.
    pub(crate) fn active_revision(&self, route: &RouteId) -> Result<Option<PolicyRevision>, PolicyOpError> { Ok(self.revision_backend()?.active(route)?) }
    /// List durable revision summaries.
    pub(crate) fn revisions(&self, route: &RouteId) -> Result<Vec<RevisionSummary>, PolicyOpError> { Ok(self.revision_backend()?.history(route)?) }
    /// Read one durable revision.
    pub(crate) fn revision(&self, route: &RouteId, n: u64) -> Result<Option<PolicyRevision>, PolicyOpError> { Ok(self.revision_backend()?.get(route, n)?) }
    /// Return the currently published document and catalog.
    pub(crate) fn effective_document(&self, route: &RouteId) -> Result<(Arc<PolicyDocument>, Arc<Snapshot>), PolicyOpError> {
        let state = self.state.lock().map_err(|_| PolicyOpError::Unavailable)?;
        let unit = &state.units[state.unit(route.as_str()).map_err(|_| PolicyOpError::UnknownRoute)?];
        let published = unit.routes.iter().find(|r| r.id() == route.as_str()).ok_or(PolicyOpError::Unavailable)?;
        let snapshot = published.locks().1.read().map_err(|_| PolicyOpError::Unavailable)?.clone();
        Ok((unit.doc.clone(), snapshot))
    }
    /// Read the published source without reopening a file.
    pub(crate) fn policy_source(&self, route: &RouteId) -> Result<String, PolicyOpError> {
        let state = self.state.lock().map_err(|_| PolicyOpError::Unavailable)?;
        Ok(state.units[state.unit(route.as_str()).map_err(|_| PolicyOpError::UnknownRoute)?].source.clone())
    }
    /// Fingerprint the route's currently published catalog.
    pub(crate) fn current_catalog_fingerprint(&self, route: &RouteId) -> Result<String, PolicyOpError> {
        Ok(crate::schema::catalog_fingerprint(&self.effective_document(route)?.1))
    }
    /// Remember explicit file ownership requested in service configuration.
    pub(crate) fn set_file_owners(&self, routes: &[String]) -> Result<(), String> {
        *self.file_owners.write().map_err(|_| "policy lock unavailable")? = routes.to_vec();
        Ok(())
    }
    /// Return the revision actually enforced by this route.
    pub(crate) fn published_revision(&self, route: &RouteId) -> Result<Option<u64>, PolicyOpError> {
        let stamp = self.policy_revision_for(route);
        Ok(self.revisions(route)?.into_iter().rev().find(|r| stamp.as_deref() == Some(format!("rev-{}-{}", r.number, r.policy_sha256.chars().take(12).collect::<String>()).as_str())).map(|r| r.number))
    }
    /// Validate a draft without assigning durable bindings.
    pub(crate) fn validate_draft(&self, route: &RouteId, toml: &str) -> Result<PolicyDocument, PolicyOpError> {
        let state = self.state.lock().map_err(|_| PolicyOpError::Unavailable)?;
        let unit = &state.units[state.unit(route.as_str()).map_err(|_| PolicyOpError::UnknownRoute)?];
        let published = unit.routes.iter().find(|r| r.id() == route.as_str()).ok_or(PolicyOpError::Unavailable)?;
        let snapshot = published.locks().1.read().map_err(|_| PolicyOpError::Unavailable)?;
        self.check_draft(unit, route, toml, &snapshot, self.is_follower())
    }
    fn check_draft(&self, unit: &Unit, route: &RouteId, toml: &str, snapshot: &Snapshot, follower: bool) -> Result<PolicyDocument, PolicyOpError> {
        let doc = parse_policy_bytes(&unit.path, toml.as_bytes(), &self.service).map_err(|errors| {
            if !errors.is_empty() && errors.iter().all(|e| (e.message.starts_with("readlog:") || e.message.starts_with("snapshot_max_age_ms:")) && (e.message.ends_with("moved to the service config") || e.message.ends_with("policy file and service config disagree"))) { PolicyOpError::InfrastructureSetting }
            else { PolicyOpError::Invalid(errors.into_iter().map(|e| e.message).collect()) }
        })?;
        if readlog_key(&doc.policy.readlog) != readlog_key(&unit.doc.policy.readlog) || doc.policy.snapshot_max_age_ms != unit.doc.policy.snapshot_max_age_ms { return Err(PolicyOpError::InfrastructureSetting); }
        #[cfg(feature = "kubernetes")]
        let follower = if follower {
            let snapshot = self.binding_snapshot().ok_or(PolicyOpError::Unavailable)?;
            Some(snapshot.routes.get(route.as_str()).map(bindings::route_bindings).transpose().map_err(|_| PolicyOpError::Unavailable)?.unwrap_or_default())
        } else { None };
        #[cfg(not(feature = "kubernetes"))]
        let follower: Option<RouteBindings> = { let _ = follower; None };
        let bindings = if let Some(bindings) = follower { bindings } else {
            let store = self.store.read().map_err(|_| PolicyOpError::Unavailable)?;
            match store.as_ref() { Some(s) => bindings::load(s.as_ref(), route).map_err(|_| PolicyOpError::Unavailable)?, None => RouteBindings::default() }
        };
        crate::policy::validate::validate_document(&doc, snapshot, &bindings).map_err(|e| PolicyOpError::Invalid(e.into_iter().map(|e| e.message).collect()))?;
        let policy = crate::policy::effective::served_policy(&doc, snapshot, &bindings);
        crate::policy::validate::validate_dependents(&policy, snapshot).map_err(|e| PolicyOpError::Invalid(e.into_iter().map(|e| e.message).collect()))?;
        Ok(doc)
    }
    fn replay(store: &dyn RevisionStore, route: &RouteId, key: &Option<String>, digest: &str) -> Result<Option<PolicyRevision>, PolicyOpError> {
        if let Some(key) = key {
            for r in store.history(route)? {
                if r.idempotency_key.as_ref() == Some(key) {
                    if r.request_digest.as_deref() != Some(digest) { return Err(RevisionStoreError::IdempotencyConflict.into()); }
                    return Ok(Some(store.get(route, r.number)?.ok_or(RevisionStoreError::Corrupt)?));
                }
            }
        }
        Ok(None)
    }
    /// Transfer a single-route file policy to durable service ownership.
    pub(crate) fn import_policy(&self, route: &RouteId, actor: &str, idempotency_key: Option<String>) -> Result<PolicyRevision, PolicyOpError> {
        let follower = self.is_follower();
        if follower { return Err(PolicyOpError::Unavailable); }
        let mut state = self.state.lock().map_err(|_| PolicyOpError::Unavailable)?;
        let index = state.unit(route.as_str()).map_err(|_| PolicyOpError::UnknownRoute)?;
        if state.configured.iter().filter(|(_, i)| *i == index).count() != 1 { return Err(PolicyOpError::SharedPolicyFile); }
        let store = self.revision_backend()?;
        if self.file_owners.read().map_err(|_| PolicyOpError::Unavailable)?.iter().any(|r| r == route.as_str()) { return Err(PolicyOpError::FileOwnerConfigured); }
        let digest = request_digest(actor, "", None, "", &RevisionKind::Import);
        if let Some(r) = Self::replay(store.as_ref(), route, &idempotency_key, &digest)? {
            if store.owner(route)? != Owner::Service { return Err(RevisionStoreError::IdempotencyConflict.into()); }
            self.route_owners.write().map_err(|_| PolicyOpError::Unavailable)?.insert(route.as_str().into(), Owner::Service);
            self.finish_replay(&mut state, route, &r, follower)?;
            return Ok(r);
        }
        let unit = &mut state.units[index];
        if store.owner(route)? == Owner::Service { return Err(PolicyOpError::AlreadyServiceOwned); }
        let registered = unit.routes.iter().find(|r| r.id() == route.as_str()).ok_or(PolicyOpError::Unavailable)?;
        let snapshot = registered.locks().1.read().map_err(|_| PolicyOpError::Unavailable)?;
        let mut revisions = unit.revisions.write().map_err(|_| PolicyOpError::Unavailable)?;
        let ctx_revision = match &registered.route {
            Route::Postgres(c) => Some(&c.policy_revision), Route::Spanner(c) => Some(&c.policy_revision),
            #[cfg(test)] Route::Test(..) => None,
        };
        let mut ctx_revision = ctx_revision.map(|r| r.write().map_err(|_| PolicyOpError::Unavailable)).transpose()?;
        let expected = store.active(route)?.map(|r| r.number);
        let r = match store.write(route, expected, NewRevision {
            policy_toml: unit.source.clone(), policy_sha256: hash(unit.source.as_bytes()),
            catalog_fingerprint: crate::schema::catalog_fingerprint(&snapshot), actor: actor.into(), kind: RevisionKind::Import,
            idempotency_key, request_digest: Some(digest),
        })? {
            WriteOutcome::Written(r) | WriteOutcome::Replayed(r) => r,
            WriteOutcome::Stale { .. } => return Err(PolicyOpError::Unavailable),
        };
        self.route_owners.write().map_err(|_| PolicyOpError::Unavailable)?.insert(route.as_str().into(), Owner::Service);
        #[cfg(feature = "kubernetes")]
        self.record_bindings_revision(route.as_str(), &revision_stamp(&r), follower);
        let stamp = revision_stamp(&r);
        revisions.policy = stamp.clone();
        if let Some(ctx) = &mut ctx_revision { **ctx = Some(stamp); }
        unit.hash = r.policy_sha256.clone();
        unit.applied = r.created_unix_ms;
        Ok(r)
    }
    /// Publish a durable head locally without writing revision storage.
    pub(crate) fn adopt_revision(&self, route: &RouteId, r: &PolicyRevision) -> Result<(), PolicyOpError> {
        if &r.route != route { return Err(PolicyOpError::Unavailable); }
        let mut state = self.state.lock().map_err(|_| PolicyOpError::Unavailable)?;
        if self.route_owners.read().map_err(|_| PolicyOpError::Unavailable)?.get(route.as_str()) != Some(&Owner::Service) { return Err(PolicyOpError::NotServiceOwned); }
        self.publish_revision(&mut state, route, r, self.is_follower())
    }
    #[allow(clippy::too_many_arguments)]
    fn publish_compiled(&self, route: &RouteId, published: &PublishedRoute, policy: &mut Arc<Policy>, compiled: &Arc<Policy>, change: Option<&MembershipChange>, r: &PolicyRevision, predecessor: &str, follower: bool) -> Result<(), PolicyOpError> {
        self.reset_membership(change, policy, follower).map_err(|_| PolicyOpError::Unavailable)?;
        let old = policy.clone();
        *policy = compiled.clone();
        let tightened = published.fence().is_some_and(|f| f.publish(&old, compiled).is_some());
        #[cfg(feature = "kubernetes")]
        self.record_bindings_revision(route.as_str(), &revision_stamp(r), follower);
        #[cfg(feature = "kubernetes")]
        if let Some(tracker) = self.activation_tracker.read().map_err(|_| PolicyOpError::Unavailable)?.as_ref() { let previous = predecessor.strip_prefix("rev-").and_then(|s| s.split('-').next()).and_then(|s| s.parse::<u64>().ok());
            tracker.published(route.as_str(), r.number, tightened || previous != r.number.checked_sub(1)); }
        #[cfg(not(feature = "kubernetes"))]
        let _ = (route, r, tightened, predecessor);
        Ok(())
    }
    fn publish_revision(&self, state: &mut State, route: &RouteId, r: &PolicyRevision, follower: bool) -> Result<(), PolicyOpError> {
        let index = state.unit(route.as_str()).map_err(|_| PolicyOpError::UnknownRoute)?;
        let stamp = state.units[index].revisions.read().map_err(|_| PolicyOpError::Unavailable)?.policy.clone();
        let loaded = stamp.strip_prefix("rev-").and_then(|s| s.split('-').next()).and_then(|n| n.parse::<u64>().ok()).unwrap_or(0);
        if loaded == r.number {
            #[cfg(feature = "kubernetes")]
            self.record_bindings_revision(route.as_str(), &stamp, follower);
            return Ok(());
        }
        if loaded > r.number { return Err(PolicyOpError::Unavailable); }
        self.finish_publication(state, route, r, follower)
    }
    fn finish_replay(&self, state: &mut State, route: &RouteId, r: &PolicyRevision, follower: bool) -> Result<(), PolicyOpError> {
        if self.revision_backend()?.active(route)?.is_some_and(|a| a.number == r.number) { self.publish_revision(state, route, r, follower)?; }
        Ok(())
    }
    fn finish_publication(&self, state: &mut State, route: &RouteId, r: &PolicyRevision, follower: bool) -> Result<(), PolicyOpError> {
        let index = state.unit(route.as_str()).map_err(|_| PolicyOpError::UnknownRoute)?;
        {
            let unit = &mut state.units[index];
            let published = unit.routes.iter().find(|p| p.id() == route.as_str()).ok_or(PolicyOpError::Unavailable)?;
            let snapshot = published.locks().1.read().map_err(|_| PolicyOpError::Unavailable)?;
            let doc = self.check_draft(unit, route, &r.policy_toml, &snapshot, follower).map_err(|_| PolicyOpError::Unavailable)?;
            let (compiled, change) = self.compile(&doc, route, &snapshot, Some(&revision_stamp(r)), follower).map_err(|_| PolicyOpError::Unavailable)?;
            let mut policy = published.locks().0.write().map_err(|_| PolicyOpError::Unavailable)?;
            let predecessor = unit.revisions.read().map_err(|_| PolicyOpError::Unavailable)?.policy.clone();
            self.publish_compiled(route, published, &mut policy, &compiled, change.as_ref(), r, &predecessor, follower)?;
            match &published.route {
                Route::Postgres(c) => *c.policy_revision.write().map_err(|_| PolicyOpError::Unavailable)? = Some(revision_stamp(r)),
                Route::Spanner(c) => *c.policy_revision.write().map_err(|_| PolicyOpError::Unavailable)? = Some(revision_stamp(r)),
                #[cfg(test)] Route::Test(..) => {},
            }
            drop(policy); drop(snapshot);
            unit.revisions.write().map_err(|_| PolicyOpError::Unavailable)?.policy = revision_stamp(r);
            unit.doc = Arc::new(doc); unit.policy = Arc::new(unit.doc.policy.clone());
            unit.source = r.policy_toml.clone(); unit.hash = r.policy_sha256.clone(); unit.applied = r.created_unix_ms;
            for p in &mut unit.routes { p.policy = compiled.clone(); p.membership_failed = false; }
            self.membership_published(route.as_str(), follower);
        }
        Ok(())
    }
    /// Validate, persist and publish a new service-owned policy.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn apply_policy(&self, route: &RouteId, actor: &str, toml: &str, expected_revision: u64, catalog_fingerprint: &str, idempotency_key: Option<String>) -> Result<PolicyRevision, PolicyOpError> {
        let mut state = self.state.lock().map_err(|_| PolicyOpError::Unavailable)?;
        self.service_apply(&mut state, route, actor, toml, expected_revision, catalog_fingerprint, idempotency_key, RevisionKind::Apply)
    }
    /// Restore a historical document as a new service-owned revision.
    pub(crate) fn rollback_policy(&self, route: &RouteId, actor: &str, to: u64, expected_revision: u64, idempotency_key: Option<String>) -> Result<PolicyRevision, PolicyOpError> {
        let mut state = self.state.lock().map_err(|_| PolicyOpError::Unavailable)?;
        let revision = self.revision_backend()?.get(route, to)?.ok_or(PolicyOpError::MissingRevision)?;
        let index = state.unit(route.as_str()).map_err(|_| PolicyOpError::UnknownRoute)?;
        let registered = state.units[index].routes.iter().find(|r| r.id() == route.as_str()).ok_or(PolicyOpError::Unavailable)?;
        let fingerprint = crate::schema::catalog_fingerprint(&**registered.locks().1.read().map_err(|_| PolicyOpError::Unavailable)?);
        self.service_apply(&mut state, route, actor, &revision.policy_toml, expected_revision, &fingerprint, idempotency_key, RevisionKind::Rollback { from: to })
    }
    #[allow(clippy::too_many_arguments)]
    fn service_apply(&self, state: &mut State, route: &RouteId, actor: &str, toml: &str, expected: u64, fingerprint: &str, idempotency_key: Option<String>, kind: RevisionKind) -> Result<PolicyRevision, PolicyOpError> {
        let follower = self.is_follower();
        if follower { return Err(PolicyOpError::Unavailable); }
        let index = state.unit(route.as_str()).map_err(|_| PolicyOpError::UnknownRoute)?;
        let store = self.revision_backend()?;
        if store.owner(route)? != Owner::Service { return Err(PolicyOpError::NotServiceOwned); }
        let digest = request_digest(actor, toml, Some(expected), fingerprint, &kind);
        if let Some(r) = Self::replay(store.as_ref(), route, &idempotency_key, &digest)? {
            self.finish_replay(state, route, &r, follower).map_err(|_| PolicyOpError::Unavailable)?;
            return Ok(r);
        }
        let active = store.active(route)?.ok_or(PolicyOpError::MissingRevision)?;
        if active.number != expected { return Err(PolicyOpError::Stale { current: (&active).into() }); }
        let unit = &mut state.units[index];
        let published = unit.routes.iter().find(|r| r.id() == route.as_str()).ok_or(PolicyOpError::Unavailable)?;
        let snapshot = published.locks().1.read().map_err(|_| PolicyOpError::Unavailable)?;
        let doc = self.check_draft(unit, route, toml, &snapshot, follower)?;
        drop(snapshot);
        let result = (|| {
            let mut policy = published.locks().0.write().map_err(|_| PolicyOpError::Unavailable)?;
            let snapshot = published.locks().1.read().map_err(|_| PolicyOpError::Unavailable)?;
            let current = crate::schema::catalog_fingerprint(&snapshot);
            if current != fingerprint { return Err(PolicyOpError::CatalogChanged { current }); }
            let (compiled, change) = self.compile(&doc, route, &snapshot, Some(&format!("rev-{}-preview", expected.checked_add(1).ok_or(PolicyOpError::Unavailable)?)), follower).map_err(|e| if e.membership { PolicyOpError::Unavailable } else { PolicyOpError::Invalid(vec![e.message]) })?;
            let mut revisions = unit.revisions.write().map_err(|_| PolicyOpError::Unavailable)?;
            let ctx = match &published.route {
                Route::Postgres(c) => Some(&c.policy_revision), Route::Spanner(c) => Some(&c.policy_revision),
                #[cfg(test)] Route::Test(..) => None,
            };
            let mut ctx = ctx.map(|r| r.write().map_err(|_| PolicyOpError::Unavailable)).transpose()?;
            let r = match store.write(route, Some(expected), NewRevision { policy_toml: toml.into(), policy_sha256: hash(toml.as_bytes()), catalog_fingerprint: current, actor: actor.into(), kind, idempotency_key, request_digest: Some(digest) })? {
                WriteOutcome::Written(r) => r,
                WriteOutcome::Replayed(r) => r,
                WriteOutcome::Stale { current: Some(r) } => return Err(PolicyOpError::Stale { current: (&r).into() }),
                WriteOutcome::Stale { current: None } => return Err(PolicyOpError::MissingRevision),
            };
            self.publish_compiled(route, published, &mut policy, &compiled, change.as_ref(), &r, &revisions.policy, follower)?;
            let stamp = revision_stamp(&r);
            revisions.policy = stamp.clone();
            if let Some(ctx) = &mut ctx { **ctx = Some(stamp); }
            Ok((r, Some(compiled)))
        })();
        match result {
            Ok((r, Some(compiled))) => {
                unit.doc = Arc::new(doc); unit.policy = Arc::new(unit.doc.policy.clone());
                unit.source = toml.into(); unit.hash = r.policy_sha256.clone(); unit.applied = r.created_unix_ms;
                for p in &mut unit.routes { p.policy = compiled.clone(); p.membership_failed = false; }
                self.membership_published(route.as_str(), follower);
                Ok(r)
            }
            Ok((r, None)) => Ok(r),
            Err(e) => {
                // Durable bindings or a committed revision may now be ahead of publication.
                let ahead = store.active(route).map_or(true, |r| r.is_none_or(|r| r.number != expected));
                if ahead || self.membership_pending(route.as_str(), follower) { for p in &mut unit.routes { p.membership_failed = true; } }
                Err(e)
            }
        }
    }
}
impl Hub {
    /// Report data-free readiness for every configured route.
    pub(crate) fn readiness(&self, audit_ok: bool, store_ok: bool) -> (bool, String) {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let policy_available = !self.state.is_poisoned();
        let mut ready = policy_available && !state.configured.is_empty()
            && self.serving_gate.as_ref().is_none_or(|g| g.allow());
        let mut body = String::new();
        if self.serving_gate.as_ref().is_some_and(|g| !g.allow()) {
            body.push_str("serving=unavailable\n");
        }
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
    #[allow(dead_code)] // The file-only constructor is also used by test and embedding callers.
    pub(crate) fn new(
        routes: &[(RouteId, PathBuf)],
        watch_ms: Option<u64>,
        service: crate::policy::ServicePolicySettings,
    ) -> Result<Arc<Self>, String> {
        Self::new_with_revision_store(routes, watch_ms, service, None)
    }
    /// Release explicit startup ownership requests before reading any policy source.
    pub(crate) fn release_file_owners(store: &dyn RevisionStore, routes: &[String]) -> Result<(), String> {
        for route in routes {
            let route = RouteId::new(route).ok_or("invalid policy route")?;
            if store.owner(&route).map_err(|_| "service policy unavailable")? == Owner::Service {
                store.release_to_file(&route).map_err(|_| "service policy unavailable")?;
                eprintln!("serve: route {} returned to file ownership", route.as_str());
            }
        }
        Ok(())
    }
    /// Load service-owned source documents without depending on their former files.
    pub(crate) fn new_with_revision_store(
        routes: &[(RouteId, PathBuf)], watch_ms: Option<u64>,
        service: crate::policy::ServicePolicySettings, revisions: Option<&dyn RevisionStore>,
    ) -> Result<Arc<Self>, String> {
        let mut units: Vec<Unit> = Vec::new();
        let mut configured = Vec::new();
        for (route, path) in routes {
            if configured.iter().any(|(id, _)| id == route) {
                return Err("duplicate policy route".into());
            }
            let stored = match revisions {
                Some(store) => match store.owner(route).map_err(|_| "service policy unavailable")? {
                    Owner::Service => Some(store.active(route).map_err(|_| "service policy unavailable")?.ok_or("service policy unavailable")?),
                    Owner::File => None,
                },
                None => None,
            };
            let identity = std::fs::canonicalize(path).or_else(|e| {
                if stored.is_some() { std::path::absolute(path) } else { Err(e) }
            }).map_err(|_| "policy file unavailable")?;
            let index = if let Some(index) = units.iter().position(|unit| unit.identity == identity) {
                index
            } else {
                let bytes = match stored {
                    Some(revision) => revision.policy_toml.into_bytes(),
                    None => std::fs::read(path).map_err(|_| "policy file unavailable")?,
                };
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
                    source: String::from_utf8(bytes).map_err(|_| "policy file unavailable")?,
                    service_failed: false,
                    ignored_logged: false,
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
            serving_gate: None,
            #[cfg(feature = "kubernetes")]
            activation_tracker: RwLock::new(None),
            #[cfg(feature = "kubernetes")]
            peer_bindings: RwLock::new(None),
            log: ReloadableLog(
                Arc::new(RwLock::new(sink)),
                Arc::new(RwLock::new(crate::serve::Revisions::default())),
                Arc::new(AtomicBool::new(true)),
            ),
            state: Mutex::new(State { units, configured }),
            store: RwLock::new(None),
            revision_store: RwLock::new(None),
            authority: RwLock::new(std::sync::Weak::new()),
            route_owners: RwLock::new(Default::default()),
            file_owners: RwLock::new(Vec::new()),
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

    /// Attach peer binding sources; tests can inject a snapshot and a leader flag directly.
    #[cfg(feature = "kubernetes")]
    pub(crate) fn set_peer_bindings(&self, leader: Arc<dyn Fn() -> bool + Send + Sync>, snapshot: BindingsProvider) {
        *self.peer_bindings.write().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(PeerBindings { leader, snapshot });
    }
    #[cfg(feature = "kubernetes")]
    fn is_follower(&self) -> bool {
        self.peer_bindings.read().map_or(true, |p| p.as_ref().is_some_and(|p| !(p.leader)()))
    }
    #[cfg(not(feature = "kubernetes"))]
    fn is_follower(&self) -> bool { false }
    #[cfg(feature = "kubernetes")]
    fn binding_snapshot(&self) -> Option<Arc<bindings::BindingsSnapshot>> {
        (self.peer_bindings.read().ok()?.as_ref()?.snapshot)()
    }
    #[cfg(feature = "kubernetes")]
    fn record_bindings_revision(&self, route: &str, stamp: &str, follower: bool) {
        if follower { return; }
        let result = (|| {
            let peer = self.peer_bindings.read().map_err(|_| ())?;
            if peer.is_none() { return Ok(()); }
            let store = self.store.read().map_err(|_| ())?;
            bindings::record_route_stamp(store.as_ref().ok_or(())?.as_ref(), &RouteId::new(route).ok_or(())?, stamp).map_err(|_| ())
        })();
        if result.is_err() { eprintln!("serve: binding stamp record unavailable; retry on next publication"); }
    }
    /// Refuse follower serving renewal while any registered route lacks compiled bindings.
    #[cfg(feature = "kubernetes")]
    pub(crate) fn peer_bindings_ready(&self) -> bool {
        !self.is_follower() || (self.binding_snapshot().is_some() && self.state.lock().is_ok_and(|s| s.units.iter().all(|u| u.routes.iter().all(|r| !r.membership_failed))))
    }
    /// Recompile current documents after a binding snapshot or leadership change.
    #[cfg(feature = "kubernetes")]
    pub(crate) fn recompile_bindings(&self) {
        self.recompile_bindings_with_role(self.is_follower());
    }
    /// Compile the opened leader store before publishing holder readiness.
    #[cfg(feature = "kubernetes")]
    pub(crate) fn recompile_leader_bindings(&self) -> Result<(), String> {
        let mut state = self.state.lock().map_err(|_| "policy publication unavailable")?;
        let independent = state.units.len() > 1;
        for unit in &mut state.units {
            let stamp = unit.revisions.read().map_err(|_| "policy revision unavailable")?.policy.clone();
            let hash = unit.hash.clone();
            let result = self.apply_unit_with_role(unit, independent, Ok((*unit.doc).clone()), Some(stamp.clone()), false);
            unit.hash = hash;
            result?;
            if let Some(store) = self.store.read().map_err(|_| "policy bindings unavailable")?.as_ref() {
                for route in &unit.routes {
                    let id = RouteId::new(route.id()).ok_or("route unavailable")?;
                    if bindings::load_reset(store.as_ref(), &id).map_err(|_| "membership reset unavailable")?.is_some() {
                        return Err("membership reset publication unavailable".into());
                    }
                    bindings::record_route_stamp(store.as_ref(), &id, &stamp).map_err(|_| "binding stamp unavailable")?;
                }
            }
        }
        Ok(())
    }
    /// Drop transient reset work at a leadership boundary under the publication lock.
    #[cfg(feature = "kubernetes")]
    pub(crate) fn discard_pending_membership(&self) {
        let _state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        self.pending_membership.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clear();
    }
    /// Read every current head before reconciling any route as the initializing leader.
    #[cfg(feature = "kubernetes")]
    pub(crate) fn adopt_leader_heads(&self) -> Result<(), String> {
        let routes: Vec<_> = self.state.lock().map_err(|_| "policy publication unavailable")?
            .configured.iter().map(|(route, _)| route.clone()).collect();
        let heads = routes.iter().map(|route| {
            let owner = self.owner(route).map_err(|_| "policy ownership unavailable")?;
            let head = self.active_revision(route).map_err(|_| "policy head unavailable")?;
            if owner == Owner::Service && head.is_none() { return Err("policy head unavailable"); }
            Ok((route.clone(), owner, head))
        }).collect::<Result<Vec<_>, _>>()?;
        let mut state = self.state.lock().map_err(|_| "policy publication unavailable")?;
        for (route, owner, head) in heads {
            self.route_owners.write().map_err(|_| "policy ownership unavailable")?.insert(route.as_str().into(), owner);
            if owner == Owner::Service && let Some(head) = head {
                self.publish_revision(&mut state, &route, &head, false).map_err(|_| "policy adoption unavailable")?;
            }
        }
        Ok(())
    }
    #[cfg(feature = "kubernetes")]
    fn recompile_bindings_with_role(&self, follower: bool) {
        let Ok(mut state) = self.state.lock() else { return; };
        let independent = state.units.len() > 1;
        for unit in &mut state.units {
            let stamp = match unit.revisions.read() { Ok(r) => r.policy.clone(), Err(_) => continue };
            let hash = unit.hash.clone();
            let doc = (*unit.doc).clone();
            if self.apply_unit_with_role(unit, independent, Ok(doc), Some(stamp), follower).is_err() && follower
                && let Some(gate) = &self.serving_gate { gate.close(); }
            unit.hash = hash;
        }
    }

    fn compile(&self, doc: &PolicyDocument, route: &RouteId, snapshot: &Snapshot, stamp: Option<&str>, follower: bool) -> Result<(Arc<Policy>, Option<MembershipChange>), CompileError> {
        #[cfg(not(feature = "kubernetes"))]
        let _ = (stamp, follower);
        #[cfg(feature = "kubernetes")]
        if follower {
            let snapshot_bindings = self.binding_snapshot().ok_or_else(|| CompileError::binding("leader bindings unavailable"))?;
            let stamp = stamp.filter(|s| !s.is_empty() && !s.ends_with("-preview")).ok_or_else(|| CompileError::binding("policy stamp unavailable"))?;
            let route_snapshot = snapshot_bindings.routes.get(route.as_str());
            let ready = route_snapshot.is_some_and(|r| !r.reset_owed && !r.stamp.is_empty() && match (stamp_number(stamp), stamp_number(&r.stamp)) {
                (Some(current), Some(published)) => published >= current,
                _ => r.stamp == stamp,
            });
            if !ready { return Err(CompileError::binding("leader route bindings not ready")); }
            let records = route_snapshot.map(bindings::route_bindings).transpose().map_err(CompileError::binding)?.unwrap_or_default();
            let warnings = crate::policy::validate::validate_document(doc, snapshot, &records).map_err(crate::serve::errors)?;
            let mut policy = crate::policy::effective::served_policy(doc, snapshot, &records);
            policy.retired_domains = route_snapshot.map(|r| r.retired.iter().cloned().collect()).unwrap_or_default();
            crate::policy::validate::validate_dependents(&policy, snapshot).map_err(crate::serve::errors)?;
            for warning in warnings { eprintln!("serve: policy warning: {}", warning.message.replace(['\r', '\n'], " ")); }
            return Ok((Arc::new(policy), None));
        }
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

    fn reset_membership(&self, change: Option<&MembershipChange>, old: &Policy, follower: bool) -> Result<(), String> {
        let Some(change) = change.filter(|change| change.requires_reset()) else { return Ok(()); };
        if follower { return Err("follower cannot reset membership".into()); }
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
        self.log.write_lifecycle(&crate::readlog::LifecycleRecord { policy: None,
            kind: crate::readlog::LifecycleKind::MembershipReset {
                retired_domains: change.retired.iter().map(|d| d.0.clone()).collect(), sessions: count,
            },
            at_ms: now(), principal: String::new(), session: None,
            route: Some(change.route.as_str().into()), connection: None,
            actor: crate::readlog::LifecycleActor::System, reason: "policy_reset",
        }).map_err(|_| "read log unavailable".to_string())
    }

    fn membership_pending(&self, route: &str, follower: bool) -> bool {
        #[cfg(not(feature = "kubernetes"))]
        let _ = follower;
        #[cfg(feature = "kubernetes")]
        if follower { return self.binding_snapshot().is_none_or(|s| s.routes.get(route).is_none_or(|r| r.reset_owed)); }
        self.pending_membership.lock().map_or(true, |pending| pending.contains_key(route))
            || self.store.read().map_or(true, |store| store.as_ref().is_some_and(|store| {
                RouteId::new(route).is_none_or(|route| bindings::load_reset(store.as_ref(), &route).map_or(true, |owed| owed.is_some()))
            }))
    }

    fn membership_published(&self, route: &str, follower: bool) {
        if follower { return; }
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
    /// Return the serving route's fence for management release waiting.
    #[allow(dead_code)] // Consumed by the management release-wait task.
    pub(crate) fn fence_for(&self, route: &RouteId) -> Option<Arc<crate::policy::RevisionFence>> {
        let state = self.state.lock().ok()?;
        let unit = &state.units[state.unit(route.as_str()).ok()?];
        unit.routes.iter().find(|r| r.id() == route.as_str())?.fence().cloned()
    }
    /// Validate and install the configured unit policy before publishing a route.
    pub(crate) fn register(&self, route: Route) -> Result<(), String> {
        let follower = self.is_follower();
        let mut state = self.state.lock().map_err(|_| "policy lock unavailable")?;
        let index = state.unit(route.id())?;
        let s = &mut state.units[index];
        if s.service_failed { return Err("service policy unavailable".into()); }
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
        let compiled = self.compile(&s.doc, &RouteId::new(route.id()).ok_or("route policy unavailable")?, &snapshot, Some(s.revisions.read().map_err(|_| "revision unavailable")?.policy.as_str()), follower);
        #[cfg(feature = "kubernetes")]
        if compiled.as_ref().is_err_and(|e| e.membership) && follower {
            let pending = current.clone();
            drop(snapshot);
            drop(current);
            drop(revision);
            s.routes.push(PublishedRoute { route, policy: pending, refresh_failed: false, membership_failed: true });
            if let Some(gate) = &self.serving_gate { gate.close(); }
            return Ok(());
        }
        let (compiled, change) = compiled.map_err(|error| {
                if error.membership || self.membership_pending(route.id(), follower) {
                    for registered in &mut s.routes {
                        if registered.id() == route.id() { registered.membership_failed = true; }
                    }
                }
                error.message
            })?;
        self.reset_membership(change.as_ref(), &current, follower).inspect_err(|_| {
            for registered in &mut s.routes {
                if registered.id() == route.id() && self.membership_pending(route.id(), follower) { registered.membership_failed = true; }
            }
        })?;
        {
            if let Some(revision) = revision.as_mut() { **revision = Some(s.revisions.read().map_err(|_| "revision lock unavailable")?.policy.clone()); }
            let old = current.clone();
            *current = compiled.clone();
            if let Some(fence) = route.fence() { fence.publish(&old, &compiled); }
        }
        drop(snapshot);
        drop(current);
        drop(revision);
        #[cfg(feature = "kubernetes")]
        self.record_bindings_revision(route.id(), &s.revisions.read().map_err(|_| "revision unavailable")?.policy, follower);
        self.membership_published(route.id(), follower);
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
        let follower = self.is_follower();
        if unit.routes.iter().any(|registered| registered.id() == route.id() && registered.membership_failed) {
            return Err("policy membership requires successful reload".into());
        }
        let result = (|| {
            let (policy, catalog) = route.locks();
            let mut policy = policy.write().map_err(|_| "policy lock unavailable")?;
            let mut catalog = catalog.write().map_err(|_| "snapshot lock unavailable")?;
            let (compiled, change) = self.compile(&unit.doc, &RouteId::new(route.id()).ok_or("route policy unavailable")?, &snapshot, Some(unit.revisions.read().map_err(|_| "revision unavailable")?.policy.as_str()), follower)
                .map_err(|error| error.message)?;
            self.reset_membership(change.as_ref(), &policy, follower)?;
            apply()?;
            *catalog = Arc::new(snapshot);
            let old = policy.clone();
            *policy = compiled.clone();
            if let Some(fence) = route.fence() { fence.publish(&old, &compiled); }
            #[cfg(feature = "kubernetes")]
            self.record_bindings_revision(route.id(), &unit.revisions.read().map_err(|_| "revision unavailable")?.policy, follower);
            Ok(compiled)
        })();
        for registered in &mut unit.routes {
            if std::ptr::eq(registered.locks().0, route.locks().0) {
                match &result {
                    Ok(compiled) => { registered.policy = compiled.clone(); registered.refresh_failed = false; registered.membership_failed = false; self.membership_published(route.id(), follower); }
                    Err(_) => {
                        registered.refresh_failed = true;
                        if self.membership_pending(route.id(), follower) { registered.membership_failed = true; }
                    },
                }
            }
        }
        result.map(|_| ())
    }

    #[cfg(test)]
    fn apply(
        &self,
        unit: usize,
        candidate: Result<PolicyDocument, String>,
        digest: Option<String>,
    ) -> Result<(), String> {
        let mut state = self.state.lock().map_err(|_| "policy lock unavailable")?;
        let independent = state.units.len() > 1;
        self.apply_unit(&mut state.units[unit], independent, candidate, digest)
    }

    fn apply_unit(&self, s: &mut Unit, independent: bool, candidate: Result<PolicyDocument, String>, digest: Option<String>) -> Result<(), String> {
        self.apply_unit_with_role(s, independent, candidate, digest, self.is_follower())
    }
    fn apply_unit_with_role(&self, s: &mut Unit, independent: bool, candidate: Result<PolicyDocument, String>, digest: Option<String>, follower: bool) -> Result<(), String> {
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
                self.compile(&doc, &RouteId::new(route.id()).ok_or("route policy unavailable")?, snapshot, digest.as_deref(), follower)
                    .map_err(|error| { if error.membership { membership_failed.push(route.id().to_owned()); } error.message })
            }).collect::<Result<Vec<_>, String>>()?;
            for ((_, change), old) in compiled.iter().zip(&policies) {
                self.reset_membership(change.as_ref(), old, follower)?;
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
            for ((current, compiled), route) in policies.iter_mut().zip(&compiled).zip(&s.routes) {
                let old = (*current).clone();
                **current = compiled.0.clone();
                if let Some(fence) = route.fence() { fence.publish(&old, &compiled.0); }
            }
            for current in &mut route_revisions {
                **current = Some(revision.clone());
            }
            revisions.policy = revision.clone();
            #[cfg(feature = "kubernetes")]
            for route in &s.routes { self.record_bindings_revision(route.id(), revision, follower); }
            Ok::<_, String>((doc, policy, compiled))
        })();
        for route in &mut s.routes {
            if result.is_err() && (membership_failed.iter().any(|id| id == route.id()) || self.membership_pending(route.id(), follower)) { route.membership_failed = true; }
        }
        match result {
            Ok((doc, policy, compiled)) => {
                s.doc = doc;
                s.policy = policy;
                for (route, compiled) in s.routes.iter_mut().zip(compiled) {
                    route.policy = compiled.0;
                    route.membership_failed = false;
                    self.membership_published(route.id(), follower);
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
    fn ignore_file_unit(&self, state: &mut State, unit: usize) -> bool {
        let routes: Vec<_> = state.configured.iter().filter(|(_, i)| *i == unit).map(|(r, _)| r.clone()).collect();
        let skip = self.route_owners.read().map_or(true, |owners| routes.iter().any(|r| owners.get(r.as_str()) == Some(&Owner::Service)));
        if skip && !state.units[unit].ignored_logged {
            for route in routes { eprintln!("serve: policy file ignored for route {}: service-owned", route.as_str()); }
            state.units[unit].ignored_logged = true;
        }
        skip
    }
    fn attempt(&self, unit: usize, path: &Path, bytes: Result<Vec<u8>, std::io::Error>) {
        // Ownership and publication are serialized with management operations.
        let Ok(mut state) = self.state.lock() else { return; };
        if self.ignore_file_unit(&mut state, unit) { return; }
        let source = bytes.as_ref().ok().and_then(|b| String::from_utf8(b.clone()).ok());
        let digest = bytes.as_ref().ok().map(|b| hash(b));
        let candidate = bytes
            .map_err(|_| "policy file unavailable".to_owned())
            .and_then(|b| parse(path, &b, &self.service));
        let independent = state.units.len() > 1;
        match self.apply_unit(&mut state.units[unit], independent, candidate, digest) {
            Ok(()) => { if let Some(source) = source { state.units[unit].source = source; } eprintln!("serve: policy reloaded"); },
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
                {
                    let Ok(mut state) = self.state.lock() else { continue; };
                    if self.ignore_file_unit(&mut state, index) { continue; }
                }
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
pub(crate) mod tests {
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
            serving_gate: None,
            #[cfg(feature = "kubernetes")]
            activation_tracker: RwLock::new(None),
            #[cfg(feature = "kubernetes")]
            peer_bindings: RwLock::new(None),
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
                    source: String::new(),
                    service_failed: false,
                    ignored_logged: false,
                    applied: 1,
                    last: "none",
                    last_time: 0,
                    attempts: 0,
                    failures: 0,
                }],
                configured: vec![(id("postgres"), 0)],
            }),
            store: RwLock::new(None),
            revision_store: RwLock::new(None),
            authority: RwLock::new(std::sync::Weak::new()),
            route_owners: RwLock::new(Default::default()),
            file_owners: RwLock::new(Vec::new()),
            pending_membership: Mutex::new(Default::default()),
            #[cfg(test)]
            reset_callback: RwLock::new(Some(Arc::new(|_, _| Ok(0)))) ,
            watch_ms: None,
            service: crate::policy::ServicePolicySettings::default(),
        }
    }
    #[test]
    fn serving_gate_controls_readiness() {
        let mut hub = hub();
        hub.register(test_route("postgres", empty_snapshot())).unwrap();
        assert!(hub.readiness(true, true).0);
        let gate = Arc::new(crate::policy::serving::ServingGate::default());
        hub.serving_gate = Some(gate.clone());
        assert!(!hub.readiness(true, true).0);
        assert!(hub.readiness(true, true).1.contains("serving=unavailable"));
        gate.extend_to(u64::MAX);
        assert!(hub.readiness(true, true).0);
        gate.close();
        assert!(!hub.readiness(true, true).0);
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
            fence: Default::default(),
                serving_gate: None,
            authority: runtime.authority.clone(),
                #[cfg(feature = "kubernetes")]
                peer_state: None,
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
            fence: Default::default(),
                serving_gate: None,
            authority: runtime.authority.clone(),
                #[cfg(feature = "kubernetes")]
                peer_state: None, registry: runtime.registry.clone(), route: id("postgres"),
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
        let (compiled, _) = hub.compile(&doc, &id("a"), &renamed, None, hub.is_follower()).unwrap();
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

    fn revision_hub(files: &Files) -> (Arc<Hub>, Arc<crate::state::store::RedbStore>) {
        let hub = Hub::new(&[(id("a"), files.a.clone())], None, Default::default()).unwrap();
        let store = binding_store(files);
        hub.set_store(store.clone()).unwrap();
        hub.set_revision_store(Arc::new(crate::policy::LocalRevisionStore::new(store.clone()))).unwrap();
        hub.register(test_route("a", customers(&[("email", 1)]))).unwrap();
        (hub, store)
    }
    fn masked_toml() -> &'static str { "snapshot_max_age_ms=60000\n[readlog]\nsink='stdout'\n[[column]]\ntable='public.customers'\nname='email'\nmasked=true\n" }
    struct NoOwnerReads;
    impl RevisionStore for NoOwnerReads {
        fn owner(&self, _: &RouteId) -> Result<Owner, RevisionStoreError> { panic!("watcher must use cached ownership") }
        fn active(&self, _: &RouteId) -> Result<Option<PolicyRevision>, RevisionStoreError> { Err(RevisionStoreError::Unavailable) }
        fn write(&self, _: &RouteId, _: Option<u64>, _: NewRevision) -> Result<WriteOutcome, RevisionStoreError> { Err(RevisionStoreError::Unavailable) }
        fn history(&self, _: &RouteId) -> Result<Vec<RevisionSummary>, RevisionStoreError> { Err(RevisionStoreError::Unavailable) }
        fn get(&self, _: &RouteId, _: u64) -> Result<Option<PolicyRevision>, RevisionStoreError> { Err(RevisionStoreError::Unavailable) }
        fn release_to_file(&self, _: &RouteId) -> Result<(), RevisionStoreError> { Err(RevisionStoreError::Unavailable) }
    }
    #[test]
    fn watcher_uses_cached_ownership_without_backend_reads() {
        let files = Files::new();
        let (hub, _store) = revision_hub(&files);
        hub.import_policy(&id("a"), "admin", None).unwrap();
        *hub.revision_store.write().unwrap() = Some(Arc::new(NoOwnerReads));
        let mut state = hub.state.lock().unwrap();
        assert!(hub.ignore_file_unit(&mut state, 0));
        hub.route_owners.write().unwrap().insert("a".into(), Owner::File);
        assert!(!hub.ignore_file_unit(&mut state, 0));
    }
    #[test]
    fn import_apply_replay_stale_invalid_and_rollback() {
        let files = Files::new(); let (hub, _store) = revision_hub(&files); let route = id("a");
        let before = hub.state.lock().unwrap().units[0].routes[0].policy.clone();
        let imported = hub.import_policy(&route, "admin", Some("import".into())).unwrap();
        assert_eq!(hub.owner(&route).unwrap(), Owner::Service); assert_eq!(imported.number, 1);
        assert!(Arc::ptr_eq(&before, &hub.state.lock().unwrap().units[0].routes[0].policy));
        assert_eq!(hub.import_policy(&route, "admin", Some("import".into())).unwrap(), imported);
        assert_eq!(hub.import_policy(&route, "admin", None), Err(PolicyOpError::AlreadyServiceOwned));
        let fingerprint = hub.current_catalog_fingerprint(&route).unwrap();
        let applied = hub.apply_policy(&route, "admin", masked_toml(), 1, &fingerprint, Some("apply".into())).unwrap();
        assert_eq!(applied.number, 2); assert_eq!(hub.policy_revision_for(&route), Some(revision_stamp(&applied)));
        let current = hub.state.lock().unwrap().units[0].routes[0].policy.clone();
        assert_eq!(current.treatment(&customer_column("email")), Some(&Treatment::Masked { placeholder: None }));
        assert_eq!(hub.apply_policy(&route, "admin", masked_toml(), 1, &fingerprint, Some("apply".into())).unwrap(), applied);
        assert!(Arc::ptr_eq(&current, &hub.state.lock().unwrap().units[0].routes[0].policy));
        assert!(matches!(hub.apply_policy(&route, "admin", "invalid", 1, "wrong", None), Err(PolicyOpError::Stale { .. })));
        assert!(matches!(hub.apply_policy(&route, "admin", "invalid", 2, &fingerprint, None), Err(PolicyOpError::Invalid(_))));
        assert_eq!(hub.active_revision(&route).unwrap().unwrap().number, 2);
        assert!(Arc::ptr_eq(&current, &hub.state.lock().unwrap().units[0].routes[0].policy));
        assert_eq!(hub.apply_policy(&route, "admin", &masked_toml().replace("60000", "60001"), 2, &fingerprint, None), Err(PolicyOpError::InfrastructureSetting));
        assert!(matches!(hub.apply_policy(&route, "admin", masked_toml(), 2, "wrong", None), Err(PolicyOpError::CatalogChanged { .. })));
        let rolled = hub.rollback_policy(&route, "admin", 1, 2, Some("rollback".into())).unwrap();
        assert_eq!(rolled.number, 3); assert_eq!(rolled.kind, RevisionKind::Rollback { from: 1 });
        assert_eq!(hub.revisions(&route).unwrap().len(), 3);
        assert_eq!(hub.revision(&route, 2).unwrap(), Some(applied));
        assert!(hub.readiness(true, true).0);
    }
    #[test]
    fn explicit_file_ownership_loads_file_at_startup() {
        let files = Files::new(); let (hub, store) = revision_hub(&files); let route = id("a");
        hub.import_policy(&route, "admin", None).unwrap();
        let fp = hub.current_catalog_fingerprint(&route).unwrap();
        hub.apply_policy(&route, "admin", masked_toml(), 1, &fp, None).unwrap();
        drop(hub);
        files.write(&files.a, 12345);
        let backend = crate::policy::LocalRevisionStore::new(store.clone());
        Hub::release_file_owners(&backend, &["a".into()]).unwrap();
        let hub = Hub::new_with_revision_store(&[(route.clone(), files.a.clone())], None, Default::default(), Some(&backend)).unwrap();
        hub.set_store(store.clone()).unwrap();
        hub.set_revision_store(Arc::new(backend)).unwrap();
        hub.register(test_route("a", customers(&[("email", 1)]))).unwrap();
        assert_eq!(hub.policy_for(&route).unwrap().snapshot_max_age_ms, 12345);
        assert_eq!(hub.owner(&route).unwrap(), Owner::File);
        assert_eq!(hub.revisions(&route).unwrap().len(), 2);
        assert_eq!(hub.import_policy(&route, "admin", None).unwrap().number, 3);
    }
    #[test]
    fn service_ownership_ignores_files_once_and_restores_on_restart() {
        let files = Files::new(); let (hub, store) = revision_hub(&files); let route = id("a");
        hub.import_policy(&route, "admin", None).unwrap();
        let fp = hub.current_catalog_fingerprint(&route).unwrap();
        let applied = hub.apply_policy(&route, "admin", masked_toml(), 1, &fp, None).unwrap();
        files.write(&files.a, 12345);
        for _ in 0..2 { hub.attempt(0, &files.a, std::fs::read(&files.a)); }
        assert!(hub.state.lock().unwrap().units[0].ignored_logged);
        assert_eq!(hub.policy_revision_for(&route), Some(revision_stamp(&applied)));
        assert_eq!(hub.state.lock().unwrap().units[0].routes[0].policy.snapshot_max_age_ms, 60000);
        drop(hub);
        let hub = Hub::new(&[(route.clone(), files.a.clone())], None, Default::default()).unwrap();
        hub.set_store(store.clone()).unwrap();
        hub.set_revision_store(Arc::new(crate::policy::LocalRevisionStore::new(store))).unwrap();
        hub.register(test_route("a", customers(&[("email", 1)]))).unwrap();
        assert_eq!(hub.policy_revision_for(&route), Some(revision_stamp(&applied)));
        assert_eq!(hub.state.lock().unwrap().units[0].routes[0].policy.treatment(&customer_column("email")), Some(&Treatment::Masked { placeholder: None }));
        assert!(hub.status().contains(&format!("policy_sha256={}", applied.policy_sha256)));
    }
    #[test]
    fn shared_file_import_and_file_owned_apply_are_refused() {
        let files = Files::new();
        let hub = Hub::new(&[(id("a"), files.a.clone()), (id("b"), files.a.clone())], None, Default::default()).unwrap();
        let store = binding_store(&files); hub.set_store(store.clone()).unwrap();
        hub.set_revision_store(Arc::new(crate::policy::LocalRevisionStore::new(store))).unwrap();
        hub.register(test_route("a", customers(&[("email", 1)]))).unwrap();
        assert_eq!(hub.import_policy(&id("a"), "admin", None), Err(PolicyOpError::SharedPolicyFile));
        assert_eq!(hub.apply_policy(&id("a"), "admin", masked_toml(), 1, "", None), Err(PolicyOpError::NotServiceOwned));
    }
    #[test]
    fn missing_or_unparsable_service_revision_never_registers_file_policy() {
        for corrupt in [false, true] {
            let files = Files::new(); let (hub, store) = revision_hub(&files);
            let revision = hub.import_policy(&id("a"), "admin", None).unwrap(); drop(hub);
            use crate::state::{store::Store, schema::Table};
            let mut key = b"a\0".to_vec(); key.extend(1u64.to_be_bytes());
            let mut tx = store.write().unwrap();
            if corrupt {
                let mut revision = revision.clone(); revision.policy_toml = "not valid TOML".into();
                tx.put(Table::PolicyRevisions, &key, &crate::state::model::encode(&revision)).unwrap();
            } else { tx.delete(Table::PolicyRevisions, &key).unwrap(); }
            tx.commit().unwrap();
            let hub = Hub::new(&[(id("a"), files.a.clone())], None, Default::default()).unwrap();
            hub.set_store(store.clone()).unwrap();
            assert!(hub.set_revision_store(Arc::new(crate::policy::LocalRevisionStore::new(store))).is_err());
            assert!(hub.register(test_route("a", customers(&[("email", 1)]))).is_err());
            assert!(!hub.readiness(true, true).0);
        }
    }
    #[test]
    fn revision_commit_failure_does_not_publish() {
        let files = Files::new(); let (hub, store) = revision_hub(&files); let route = id("a");
        hub.import_policy(&route, "admin", None).unwrap();
        let fp = hub.current_catalog_fingerprint(&route).unwrap();
        let old = hub.state.lock().unwrap().units[0].routes[0].policy.clone();
        store.fail_commits(true);
        assert!(hub.apply_policy(&route, "admin", masked_toml(), 1, &fp, None).is_err());
        store.fail_commits(false);
        assert_eq!(hub.active_revision(&route).unwrap().unwrap().number, 1);
        assert!(Arc::ptr_eq(&old, &hub.state.lock().unwrap().units[0].routes[0].policy));
    }

    #[test]
    fn stored_policy_can_start_without_a_readable_former_file() {
        let files = Files::new(); let (hub, store) = revision_hub(&files);
        hub.import_policy(&id("a"), "admin", None).unwrap(); drop(hub);
        std::fs::remove_file(&files.a).unwrap();
        let backend = crate::policy::LocalRevisionStore::new(store.clone());
        let hub = Hub::new_with_revision_store(&[(id("a"), files.a.clone())], None, Default::default(), Some(&backend)).unwrap();
        hub.set_store(store.clone()).unwrap(); hub.set_revision_store(Arc::new(backend)).unwrap();
        hub.register(test_route("a", customers(&[("email", 1)]))).unwrap();
        assert!(hub.readiness(true, true).0);
    }
    #[test]
    fn configured_file_owner_and_released_import_keys_are_refused() {
        let files = Files::new(); let hub = tokenize_hub(&files, &["a"]); let store = binding_store(&files);
        hub.set_store(store.clone()).unwrap();
        let backend = Arc::new(crate::policy::LocalRevisionStore::new(store));
        hub.set_revision_store(backend.clone()).unwrap();
        hub.register(test_route("a", customers(&[("email", 1)]))).unwrap();
        hub.set_file_owners(&["a".into()]).unwrap();
        assert_eq!(hub.import_policy(&id("a"), "admin", None), Err(PolicyOpError::FileOwnerConfigured));
        hub.set_file_owners(&[]).unwrap();
        hub.import_policy(&id("a"), "admin", Some("old".into())).unwrap();
        backend.release_to_file(&id("a")).unwrap();
        assert_eq!(hub.import_policy(&id("a"), "admin", Some("old".into())), Err(PolicyOpError::Store(RevisionStoreError::IdempotencyConflict)));
        assert_eq!(hub.import_policy(&id("a"), "admin", Some("new".into())).unwrap().number, 2);
    }

    #[test]
    fn reset_failure_after_revision_write_marks_route_unready() {
        let files = Files::new(); let hub = tokenize_hub(&files, &["a"]); let store = binding_store(&files);
        hub.set_store(store.clone()).unwrap(); hub.set_revision_store(Arc::new(crate::policy::LocalRevisionStore::new(store))).unwrap();
        hub.register(test_route("a", customers(&[("email", 1), ("other", 2)]))).unwrap();
        hub.import_policy(&id("a"), "admin", None).unwrap();
        let fp = hub.current_catalog_fingerprint(&id("a")).unwrap();
        let old = hub.state.lock().unwrap().units[0].routes[0].policy.clone();
        *hub.reset_callback.write().unwrap() = None;
        let linked = "version=2\ndefault='tokenize'\n[[link]]\nfields=['public.customers.email','public.customers.other']\n";
        assert!(hub.apply_policy(&id("a"), "admin", linked, 1, &fp, Some("reset".into())).is_err());
        assert_eq!(hub.active_revision(&id("a")).unwrap().unwrap().number, 2);
        assert!(!hub.readiness(true, true).0);
        assert!(Arc::ptr_eq(&old, &hub.state.lock().unwrap().units[0].routes[0].policy));
        // A retry must finish publication or continue refusing.
        assert_eq!(hub.apply_policy(&id("a"), "admin", linked, 1, &fp, Some("reset".into())), Err(PolicyOpError::Unavailable));
        assert_eq!(hub.published_revision(&id("a")).unwrap(), Some(1));
        *hub.reset_callback.write().unwrap() = Some(Arc::new(|_, _| Ok(0)));
        assert_eq!(hub.apply_policy(&id("a"), "admin", linked, 1, &fp, Some("reset".into())).unwrap().number, 2);
        assert_eq!(hub.published_revision(&id("a")).unwrap(), Some(2));
    }

    #[test]
    fn service_infrastructure_overrides_are_refused() {
        let files = Files::new(); let hub = tokenize_hub(&files, &["a"]); let store = binding_store(&files);
        hub.set_store(store.clone()).unwrap(); hub.set_revision_store(Arc::new(crate::policy::LocalRevisionStore::new(store))).unwrap();
        hub.register(test_route("a", customers(&[("email", 1)]))).unwrap(); hub.import_policy(&id("a"), "admin", None).unwrap();
        let fp = hub.current_catalog_fingerprint(&id("a")).unwrap();
        assert_eq!(hub.apply_policy(&id("a"), "admin", "version=2\ndefault='tokenize'\nsnapshot_max_age_ms=42\n", 1, &fp, None), Err(PolicyOpError::InfrastructureSetting));
        assert_eq!(hub.active_revision(&id("a")).unwrap().unwrap().number, 1);
    }


    #[test]
    fn fence_file_apply_tightens_loosens_and_republishes() {
        let h = hub();
        let fixture = crate::pgwire::session::tests::Fixture::new();
        let ctx = fixture.ctx.clone();
        *ctx.snapshot.write().unwrap() = Arc::new(customers(&[("email", 1)]));
        h.register(Route::Postgres(ctx.clone())).unwrap();
        assert!(Arc::ptr_eq(&ctx.fence, &h.fence_for(&id("postgres")).unwrap()));
        assert!(h.fence_for(&id("unknown")).is_none());
        let initial = ctx.fence.epoch();
        let mut blind = policy();
        let domain = crate::policy::DomainName("email".into());
        blind.columns.insert(customer_column("email"), Treatment::Blind { domain: domain.clone() });
        blind.domains.insert(domain, crate::policy::DomainSettings {
            tokenize: crate::policy::TokenizeSetting::Off, range: None,
        });
        h.apply(0, Ok(document(blind.clone())), Some("blind".into())).unwrap();
        assert_eq!(ctx.fence.epoch(), initial + 1);
        h.apply(0, Ok(document(blind.clone())), Some("same".into())).unwrap();
        assert_eq!(ctx.fence.epoch(), initial + 1);
        // Blind->Show that also drops the column's domain is a loosening.
        h.apply(0, Ok(document(policy())), Some("show".into())).unwrap();
        assert_eq!(ctx.policy.read().unwrap().treatment(&customer_column("email")), Some(&Treatment::Show));
        assert_eq!(ctx.fence.epoch(), initial + 1);
    }

    #[test]
    fn fence_service_apply_and_replay_publish_under_lock() {
        let files = Files::new();
        let h = Hub::new(&[(id("postgres"), files.a.clone())], None, Default::default()).unwrap();
        let store = binding_store(&files);
        h.set_store(store.clone()).unwrap();
        h.set_revision_store(Arc::new(crate::policy::LocalRevisionStore::new(store))).unwrap();
        let fixture = crate::pgwire::session::tests::Fixture::new();
        let ctx = fixture.ctx.clone();
        *ctx.snapshot.write().unwrap() = Arc::new(customers(&[("email", 1)]));
        h.register(Route::Postgres(ctx.clone())).unwrap();
        let old = ctx.policy.read().unwrap().clone();
        let initial = ctx.fence.epoch();
        let route = id("postgres");
        h.import_policy(&route, "admin", None).unwrap();
        let fingerprint = h.current_catalog_fingerprint(&route).unwrap();
        let applied = h.apply_policy(&route, "admin", masked_toml(), 1, &fingerprint, Some("apply".into())).unwrap();
        assert_eq!(ctx.fence.epoch(), initial + 1);
        h.apply_policy(&route, "admin", masked_toml(), 1, &fingerprint, Some("apply".into())).unwrap();
        assert_eq!(ctx.fence.epoch(), initial + 1);
        // Simulate a durable commit whose in-memory publication was interrupted.
        *ctx.policy.write().unwrap() = old;
        h.state.lock().unwrap().units[0].revisions.write().unwrap().policy = "interrupted".into();
        assert_eq!(h.apply_policy(&route, "admin", masked_toml(), 1, &fingerprint, Some("apply".into())).unwrap(), applied);
        assert_eq!(ctx.fence.epoch(), initial + 2);
    }

    #[test]
    fn fence_catalog_refresh_withholds_needs_repair_column() {
        let files = Files::new();
        let h = tokenize_hub(&files, &["postgres"]);
        let store = binding_store(&files);
        h.set_store(store.clone()).unwrap();
        let fixture = crate::pgwire::session::tests::Fixture::new();
        let ctx = fixture.ctx.clone();
        let mut snapshot = customers(&[("email", 1)]);
        *ctx.snapshot.write().unwrap() = Arc::new(snapshot.clone());
        h.register(Route::Postgres(ctx.clone())).unwrap();
        let initial = ctx.fence.epoch();
        h.refresh(&Route::Postgres(ctx.clone()), snapshot.clone()).unwrap();
        assert_eq!(ctx.fence.epoch(), initial);
        assert!(matches!(ctx.policy.read().unwrap().treatment(&customer_column("email")), Some(Treatment::Blind { .. })));
        snapshot.relations[0].columns[0].ty = crate::value::ColumnType::Int8;
        h.refresh(&Route::Postgres(ctx.clone()), snapshot).unwrap();
        assert_eq!(bindings::load(store.as_ref(), &id("postgres")).unwrap().get(&customer_id(1)).unwrap().status,
            bindings::BindingStatus::NeedsRepair);
        assert_eq!(ctx.policy.read().unwrap().treatment(&customer_column("email")), Some(&Treatment::Masked { placeholder: None }));
        assert_eq!(ctx.fence.epoch(), initial + 1);
    }

    #[test]
    fn follower_adoption_refuses_regression_and_file_ownership_without_revision_write() {
        let files = Files::new();
        let h = Hub::new(&[(id("postgres"), files.a.clone())], None, Default::default()).unwrap();
        let store = binding_store(&files); h.set_store(store.clone()).unwrap();
        h.set_revision_store(Arc::new(crate::policy::LocalRevisionStore::new(store))).unwrap();
        let fixture = crate::pgwire::session::tests::Fixture::new(); let ctx = fixture.ctx.clone();
        *ctx.snapshot.write().unwrap() = Arc::new(customers(&[("email",1)]));
        h.register(Route::Postgres(ctx.clone())).unwrap(); let route = id("postgres");
        let original = h.import_policy(&route,"admin",None).unwrap();
        let initial = ctx.fence.epoch(); let slot = ctx.fence.register(); slot.begin(&ctx.fence);
        let mut revision = original.clone(); revision.number += 1; revision.policy_toml = masked_toml().into(); revision.policy_sha256 = hash(revision.policy_toml.as_bytes());
        h.adopt_revision(&route,&revision).unwrap();
        assert_eq!(ctx.fence.epoch(),initial+1); assert_eq!(ctx.fence.lagging(initial+1),1);
        assert_eq!(h.active_revision(&route).unwrap(),Some(original.clone()));
        slot.end(); assert_eq!(ctx.fence.wait_released(initial+1,Duration::ZERO),0);
        let stale = original.clone();
        assert!(h.adopt_revision(&route, &stale).is_err());
        assert_eq!(h.policy_revision_for(&route), Some(revision_stamp(&revision)));
        h.adopt_revision(&route, &revision).unwrap();
        revision.number += 1; revision.policy_toml = original.policy_toml; revision.policy_sha256 = hash(revision.policy_toml.as_bytes());
        h.adopt_revision(&route,&revision).unwrap(); assert_eq!(ctx.fence.epoch(),initial+1);
        h.route_owners.write().unwrap().insert(route.as_str().into(), Owner::File);
        let file_policy = ctx.policy.read().unwrap().clone();
        let mut ahead = revision.clone(); ahead.number += 1; ahead.policy_toml = masked_toml().into();
        assert!(matches!(h.adopt_revision(&route, &ahead), Err(PolicyOpError::NotServiceOwned)));
        assert!(Arc::ptr_eq(&file_policy, &ctx.policy.read().unwrap()));
        h.route_owners.write().unwrap().insert(route.as_str().into(), Owner::Service);
        revision.number += 1; revision.policy_toml = "invalid policy".into();
        assert!(h.adopt_revision(&route,&revision).is_err()); assert_eq!(ctx.fence.epoch(),initial+1);
    }
    #[cfg(feature = "kubernetes")]
    #[test]
    fn peer_follower_uses_only_published_bindings_and_refuses_unready_snapshots() {
        let files = Files::new();
        let settings = crate::policy::ServicePolicySettings { readlog: Some(ReadLogSink::Stdout), ..Default::default() };
        let doc = parse(&files.a, b"version=2\ndefault='tokenize'\n", &settings).unwrap();
        let route = id("a");
        let catalog = customers(&[("email", 1)]);
        let leader = hub();
        let leader_store = binding_store(&files);
        leader.set_store(leader_store.clone()).unwrap();
        leader.set_peer_bindings(Arc::new(|| true), Arc::new(|| None));
        let (expected, change) = leader.compile(&doc, &route, &catalog, Some("rev-7-test"), leader.is_follower()).unwrap();
        leader.reset_membership(change.as_ref(), &expected, leader.is_follower()).unwrap();
        leader.membership_published("a", leader.is_follower());
        leader.record_bindings_revision("a", "rev-7-test", false);
        let published = bindings::snapshot(leader_store.as_ref()).unwrap();
        assert_eq!(published.routes["a"].revision, 7);
        let follower_files = Files::new();
        let follower = hub();
        let local = binding_store(&follower_files);
        follower.set_store(local.clone()).unwrap();
        let before = bindings::snapshot(local.as_ref()).unwrap();
        let provider = Arc::new(RwLock::new(Some(Arc::new(published.clone()))));
        let source = provider.clone();
        follower.set_peer_bindings(Arc::new(|| false), Arc::new(move || source.read().unwrap().clone()));
        let (actual, change) = follower.compile(&doc, &route, &catalog, Some("rev-7-test"), follower.is_follower()).unwrap();
        assert_eq!(actual, expected);
        assert!(change.is_none());
        let expanded = customers(&[("email", 1), ("new", 2)]);
        let (actual, _) = follower.compile(&doc, &route, &expanded, Some("rev-7-test"), follower.is_follower()).unwrap();
        assert!(actual.domain_of(&customer_column("new")).is_none());
        assert_eq!(actual.treatment(&customer_column("new")), Some(&Treatment::Masked { placeholder: None }));
        assert!(follower.compile(&doc, &route, &catalog, Some("rev-8-test"), follower.is_follower()).unwrap_err().membership);
        assert!(follower.compile(&doc, &route, &catalog, None, follower.is_follower()).unwrap_err().membership);
        let mut reset = published.clone(); reset.routes.get_mut("a").unwrap().reset_owed = true;
        *provider.write().unwrap() = Some(Arc::new(reset));
        assert!(follower.membership_pending("a", follower.is_follower()));
        assert!(follower.compile(&doc, &route, &catalog, Some("rev-7-test"), follower.is_follower()).unwrap_err().membership);
        follower.membership_published("a", follower.is_follower());
        *provider.write().unwrap() = Some(Arc::new(bindings::BindingsSnapshot { version: 1, routes: Default::default() }));
        assert!(follower.compile(&doc, &route, &catalog, Some("rev-7-test"), follower.is_follower()).unwrap_err().membership);
        *provider.write().unwrap() = None;
        assert!(follower.membership_pending("a", follower.is_follower()));
        assert!(follower.compile(&doc, &route, &catalog, Some("rev-7-test"), follower.is_follower()).unwrap_err().membership);
        assert_eq!(bindings::snapshot(local.as_ref()).unwrap(), before);
        assert!(follower.pending_membership.lock().unwrap().is_empty());
    }

    #[cfg(feature = "kubernetes")]
    #[test]
    fn peer_follower_cold_registration_recovers_after_bindings_arrive() {
        let files = Files::new();
        let mut hub = tokenize_hub(&files, &["postgres"]);
        let gate = Arc::new(crate::policy::serving::ServingGate::default());
        Arc::get_mut(&mut hub).unwrap().serving_gate = Some(gate.clone());
        let store = binding_store(&files);
        let leader = tokenize_hub(&files, &["postgres"]);
        leader.set_store(store.clone()).unwrap();
        let doc = hub.state.lock().unwrap().units[0].doc.as_ref().clone();
        let catalog = customers(&[("email", 1)]);
        let (expected, change) = leader.compile(&doc, &id("postgres"), &catalog, Some("rev-3-test"), leader.is_follower()).unwrap();
        leader.reset_membership(change.as_ref(), &expected, leader.is_follower()).unwrap();
        leader.membership_published("postgres", leader.is_follower());
        bindings::record_route_stamp(store.as_ref(), &id("postgres"), "rev-3-test").unwrap();
        let provider = Arc::new(RwLock::new(None));
        let source = provider.clone();
        hub.set_peer_bindings(Arc::new(|| false), Arc::new(move || source.read().unwrap().clone()));
        hub.state.lock().unwrap().units[0].revisions.write().unwrap().policy = "rev-3-abc".into();
        let mut fixture = crate::pgwire::session::tests::Fixture::new();
        Arc::get_mut(&mut fixture.ctx).unwrap().serving_gate = Some(gate.clone());
        let ctx = fixture.ctx.clone();
        *ctx.snapshot.write().unwrap() = Arc::new(catalog);
        hub.register(Route::Postgres(ctx.clone())).unwrap();
        assert!(hub.state.lock().unwrap().units[0].routes[0].membership_failed);
        assert!(!hub.peer_bindings_ready());
        assert!(!gate.allow());
        assert!(!hub.readiness(true, true).0);
        assert!(hub.refresh(&Route::Postgres(ctx.clone()), customers(&[("email", 1)])).is_err());
        *provider.write().unwrap() = Some(Arc::new(bindings::snapshot(store.as_ref()).unwrap()));
        hub.recompile_bindings();
        assert!(!hub.state.lock().unwrap().units[0].routes[0].membership_failed);
        assert!(hub.peer_bindings_ready());
        assert_eq!(*ctx.policy.read().unwrap().as_ref(), *expected);
        assert_eq!(ctx.policy_revision.read().unwrap().as_deref(), Some("rev-3-abc"));
        gate.extend_to(u64::MAX);
        assert!(hub.readiness(true, true).0);
        *provider.write().unwrap() = None;
        hub.recompile_bindings();
        assert!(!gate.allow());
        assert!(!hub.peer_bindings_ready());
    }

    #[cfg(feature = "kubernetes")]
    #[test]
    fn peer_registration_only_defers_follower_membership_errors() {
        let files = Files::new();
        for peer_leader in [None, Some(true)] {
            let hub = tokenize_hub(&files, &["a"]);
            if let Some(leader) = peer_leader { hub.set_peer_bindings(Arc::new(move || leader), Arc::new(|| None)); }
            assert_eq!(hub.register(test_route("a", customers(&[("email", 1)]))).unwrap_err(), "policy bindings unavailable");
            assert!(hub.state.lock().unwrap().units[0].routes.is_empty());
        }
        let hub = tokenize_hub(&files, &["a"]);
        let store = binding_store(&files);
        let doc = hub.state.lock().unwrap().units[0].doc.as_ref().clone();
        hub.set_store(store.clone()).unwrap();
        let catalog = customers(&[("email", 1)]);
        let (policy, change) = hub.compile(&doc, &id("a"), &catalog, Some("rev-3-test"), hub.is_follower()).unwrap();
        hub.reset_membership(change.as_ref(), &policy, hub.is_follower()).unwrap();
        hub.membership_published("a", hub.is_follower());
        bindings::record_route_stamp(store.as_ref(), &id("a"), "rev-3-test").unwrap();
        let snapshot = Arc::new(bindings::snapshot(store.as_ref()).unwrap());
        hub.set_peer_bindings(Arc::new(|| false), Arc::new(move || Some(snapshot.clone())));
        hub.state.lock().unwrap().units[0].revisions.write().unwrap().policy = "rev-3-abc".into();
        let mut catalog = catalog;
        catalog.dependents.push(crate::schema::DependentObject {
            name: "copy_email".into(), kind: crate::schema::DependentKind::Trigger,
            reads: vec![customer_column("email")], writes_table: None,
        });
        assert!(hub.register(test_route("a", catalog)).unwrap_err().contains("trigger copy_email reads blind column"));
        assert!(hub.state.lock().unwrap().units[0].routes.is_empty());
    }

    #[cfg(feature = "kubernetes")]
    #[test]
    fn peer_leader_publication_records_revision_and_recompiles_on_flip() {
        use std::sync::atomic::Ordering::SeqCst;
        let files = Files::new();
        let hub = hub();
        let store = binding_store(&files);
        hub.set_store(store.clone()).unwrap();
        let leader = Arc::new(AtomicBool::new(true));
        let flag = leader.clone();
        let provider = Arc::new(RwLock::new(None));
        let source = provider.clone();
        hub.set_peer_bindings(Arc::new(move || flag.load(SeqCst)), Arc::new(move || source.read().unwrap().clone()));
        hub.state.lock().unwrap().units[0].revisions.write().unwrap().policy = "rev-3-abc".into();
        hub.register(test_route("postgres", customers(&[("email", 1)]))).unwrap();
        assert_eq!(bindings::snapshot(store.as_ref()).unwrap().routes["postgres"].revision, 3);
        let doc = hub.state.lock().unwrap().units[0].doc.as_ref().clone();
        hub.apply(0, Ok(doc), Some("rev-4-def".into())).unwrap();
        assert_eq!(bindings::snapshot(store.as_ref()).unwrap().routes["postgres"].revision, 4);
        leader.store(false, SeqCst);
        hub.recompile_bindings();
        assert!(hub.state.lock().unwrap().units[0].routes[0].membership_failed);
        *provider.write().unwrap() = Some(Arc::new(bindings::snapshot(store.as_ref()).unwrap()));
        hub.recompile_bindings();
        assert!(!hub.state.lock().unwrap().units[0].routes[0].membership_failed);
        leader.store(true, SeqCst);
        hub.recompile_bindings();
        assert!(!hub.state.lock().unwrap().units[0].routes[0].membership_failed);
    }

    #[cfg(feature = "kubernetes")]
    #[test]
    fn promotion_adopts_head_before_reconciling_generated_link() {
        let files = Files::new();
        let a = tokenize_hub(&files, &["postgres"]);
        let b = tokenize_hub(&files, &["postgres"]);
        let store = binding_store(&files);
        let backend = Arc::new(crate::policy::LocalRevisionStore::new(store.clone()));
        a.set_store(store.clone()).unwrap(); a.set_revision_store(backend.clone()).unwrap();
        let catalog = customers(&[("email", 1), ("other", 2)]);
        a.register(test_route("postgres", catalog.clone())).unwrap();
        let route = id("postgres");
        let n1 = a.import_policy(&route, "admin", None).unwrap();
        b.set_store(store.clone()).unwrap(); b.set_revision_store(backend).unwrap();
        b.register(test_route("postgres", catalog)).unwrap();
        assert_eq!(b.policy_revision_for(&route), Some(revision_stamp(&n1)));
        let linked = "version=2\ndefault='tokenize'\n[[link]]\nfields=['public.customers.email','public.customers.other']\n";
        let n = a.apply_policy(&route, "admin", linked, n1.number, &a.current_catalog_fingerprint(&route).unwrap(), None).unwrap();
        let expected = a.state.lock().unwrap().units[0].routes[0].policy.clone();
        // Holder readiness remains false during initialization: adoption must force the leader role.
        b.set_peer_bindings(Arc::new(|| false), Arc::new(|| None));
        b.discard_pending_membership();
        b.adopt_leader_heads().unwrap();
        b.recompile_leader_bindings().unwrap();
        let actual = b.state.lock().unwrap().units[0].routes[0].policy.clone();
        peer_pods::tokens_equal(&expected, &actual, &["email", "other"]);
        let snapshot = bindings::snapshot(store.as_ref()).unwrap();
        assert_eq!(snapshot.routes["postgres"].revision, n.number);
        assert_eq!(snapshot.routes["postgres"].stamp, revision_stamp(&n));
        assert_eq!(b.policy_revision_for(&route), Some(revision_stamp(&n)));
        *b.revision_store.write().unwrap() = None;
        assert!(b.adopt_leader_heads().is_err());
    }

    #[cfg(feature = "kubernetes")]
    #[test]
    fn promotion_discards_stale_pending_reset_after_other_leader_completed_it() {
        use crate::state::{authority::{AcquireRequest, Authority, SessionLimits}, model::{Mode, PrincipalKind, SessionState, Verifier}};
        let files = Files::new();
        let a = tokenize_hub(&files, &["a"]);
        let b = tokenize_hub(&files, &["a"]);
        let store = binding_store(&files);
        let catalog = customers(&[("email", 1), ("other", 2)]);
        for hub in [&a, &b] {
            hub.set_store(store.clone()).unwrap();
            hub.register(test_route("a", catalog.clone())).unwrap();
        }
        let mut linked = (*a.state.lock().unwrap().units[0].doc).clone();
        linked.links.push(crate::policy::LinkRule { fields: vec![customer_column("email"), customer_column("other")], domain: None });
        *a.reset_callback.write().unwrap() = Some(Arc::new(|_, _| Err("deposed".into())));
        assert!(a.apply(0, Ok(linked.clone()), Some("rev-2-linked".into())).is_err());
        assert!(!a.pending_membership.lock().unwrap().is_empty());
        assert!(bindings::load_reset(store.as_ref(), &id("a")).unwrap().is_some());
        b.apply(0, Ok(linked.clone()), Some("rev-2-linked".into())).unwrap();
        assert!(bindings::load_reset(store.as_ref(), &id("a")).unwrap().is_none());
        let authority = Authority::load(store.clone(), SessionLimits::default()).unwrap();
        let principal = authority.create_principal("agent-b", PrincipalKind::Agent, false, "b").unwrap();
        authority.set_grants(principal.id, vec![(id("a"), Mode::ReadOnly)]).unwrap();
        let session = authority.acquire_session(principal.id, AcquireRequest {
            route: id("a"), mode: None, lifetime_ms: None, isolated: false, label: None,
            idempotency_key: "b-session".into(),
            verifier: Verifier(crate::pgwire::auth::ScramVerifier::from_password("test", b"salt", 2)), secret_sha256: [0; 32],
        }).unwrap();
        a.set_authority(Arc::downgrade(&authority)).unwrap();
        // A receives the current policy as a follower; its old transient reset still exists.
        a.state.lock().unwrap().units[0].doc = Arc::new(linked);
        a.discard_pending_membership();
        a.recompile_leader_bindings().unwrap();
        assert_eq!(authority.session(session.id).unwrap().state, SessionState::Active);
        assert!(a.pending_membership.lock().unwrap().is_empty());
    }

    #[cfg(feature = "kubernetes")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn binding_initialization_failure_fences_peer_until_retry() {
        use crate::state::{authority::{Authority, SessionLimits}, leader_store::{LeaderStore, StateHolder}, peer::{Lookup, PeerAuth, PeerClient, PeerError, PeerServer}, store::{RedbStore, StoreError}};
        let files = Files::new();
        let hub = tokenize_hub(&files, &["postgres"]);
        let bindings_store = binding_store(&files);
        hub.set_store(bindings_store.clone()).unwrap();
        hub.register(test_route("postgres", customers(&[("email", 1)]))).unwrap();
        *hub.state.lock().unwrap().units[0].routes[0].locks().1.write().unwrap() = Arc::new(customers(&[("email", 1), ("other", 2)]));
        let leadership = Arc::new(|| Some(1));
        let proxy = Arc::new(LeaderStore::new(leadership.clone(), Arc::new(|| 0)));
        let path = files.dir.join("holder");
        let holder = Arc::new(StateHolder::new(proxy.clone(), leadership, Arc::new(move || RedbStore::open(&path, &crate::token::keys::RootKey::from_hex(&"01".repeat(32)).unwrap()))));
        let authority = Authority::empty(proxy, SessionLimits::default());
        let a = authority.clone(); let h = hub.clone();
        holder.on_ready(Arc::new(move || {
            a.reload().map_err(|_| StoreError::Io)?;
            a.sync_config_principals(&[]).map_err(|_| StoreError::Io)?;
            h.recompile_leader_bindings().map_err(|_| StoreError::Io)
        }));
        bindings_store.fail_commits(true);
        holder.tick();
        assert_eq!(holder.is_ready(), None);
        let ready = holder.clone();
        let server = PeerServer::new(authority, crate::policy::kube_leader::LeaderState::live_for_test(), Arc::new(PeerAuth::new(&[7; 32]).unwrap()), Default::default())
            .with_ready(Arc::new(move || ready.is_ready())).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = PeerClient::with_url(format!("http://{}", listener.local_addr().unwrap()), "b".into(), Arc::new(PeerAuth::new(&[7; 32]).unwrap()));
        let gate = Arc::new(crate::policy::serving::ServingGate::default());
        gate.extend_to(u64::MAX);
        let server = server.with_serving_gate(gate).unwrap();
        let task = tokio::spawn(server.serve(listener));
        assert!(matches!(client.session_lookup(Lookup { session_id: crate::token::SessionId::random() }, Duration::from_secs(5)).await, Err(PeerError::NotLeader)));
        bindings_store.fail_commits(false);
        holder.tick();
        assert_eq!(holder.is_ready(), Some(1));
        holder.close(); task.abort(); let _ = task.await;
    }

    #[cfg(all(test, feature = "kubernetes"))]
    pub(crate) mod peer_pods { include!("reload/peer_pods_tests.rs"); }
}
