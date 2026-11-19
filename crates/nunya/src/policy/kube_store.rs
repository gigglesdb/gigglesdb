//! Kubernetes heads are CAS commit points; history copies are immutable and repairable.
use super::kube_leader::LeaderState;
use super::store::*;
use crate::state::model::RouteId;
use kube::{
    Api, Client,
    api::{ApiResource, DynamicObject, ListParams, PostParams},
    core::GroupVersionKind,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::runtime::Handle;

const GROUP: &str = "nunya.gigglesdb.dev";
const VERSION: &str = "v1alpha1";
const ROUTE_LABEL: &str = "nunya.gigglesdb.dev/route";
const RETRY_LABEL: &str = "nunya.gigglesdb.dev/retry";
const LIMIT: usize = 1024 * 1024;

#[derive(Clone, Serialize, Deserialize)]
struct Head {
    route: RouteId,
    owner: String,
    active: u64,
    revision: Option<PolicyRevision>,
}
#[derive(Clone)]
struct VersionedHead {
    spec: Head,
    rv: String,
}
#[derive(Clone, Copy)]
enum ApiFailure {
    Conflict,
    Unavailable,
    Corrupt,
}
type ApiResult<T> = Result<T, ApiFailure>;
impl From<ApiFailure> for RevisionStoreError {
    fn from(e: ApiFailure) -> Self {
        match e {
            ApiFailure::Corrupt => Self::Corrupt,
            ApiFailure::Conflict | ApiFailure::Unavailable => Self::Unavailable,
        }
    }
}
trait Backend: Send + Sync {
    fn head(&self, name: &str) -> ApiResult<Option<VersionedHead>>;
    fn head_timeout(&self, name: &str, _timeout: std::time::Duration) -> ApiResult<Option<VersionedHead>> { self.head(name) }
    fn replace(&self, name: &str, head: &Head, rv: Option<&str>) -> ApiResult<()>;
    fn copy(&self, name: &str) -> ApiResult<Option<PolicyRevision>>;
    fn create_copy(&self, name: &str, revision: &PolicyRevision) -> ApiResult<()>;
    fn copies(&self, selector: &str) -> ApiResult<Vec<PolicyRevision>>;
}
fn digest(key: &str) -> String {
    data_encoding::HEXLOWER.encode(&Sha256::digest(key.as_bytes()))
}
fn name(route: &RouteId) -> Result<String, RevisionStoreError> {
    let n = format!("route-{}", route.as_str());
    if n.len() > 63
        || !n.as_bytes().first().is_some_and(u8::is_ascii_alphanumeric)
        || !n.as_bytes().last().is_some_and(u8::is_ascii_alphanumeric)
        || !n
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
    {
        return Err(RevisionStoreError::InvalidRoute);
    }
    Ok(n)
}
fn valid_revision(r: &PolicyRevision) -> bool {
    r.number != 0
}
fn validate(head: &Head, route: &RouteId) -> Result<(), RevisionStoreError> {
    if head.route != *route
        || !matches!(head.owner.as_str(), "file" | "service")
        || match &head.revision {
            Some(r) => r.route != *route || !valid_revision(r) || r.number != head.active,
            None => head.active != 0,
        }
    {
        return Err(RevisionStoreError::Corrupt);
    }
    Ok(())
}

/// Kubernetes revision backend with a dedicated API runtime.
pub struct KubernetesRevisionStore {
    backend: Arc<dyn Backend>,
    leader: Option<LeaderState>,
}
impl KubernetesRevisionStore {
    /// Build a namespaced store; the legacy handle argument is unused.
    pub fn new(client: Client, namespace: &str, _handle: Handle) -> Self {
        Self {
            backend: Arc::new(KubeBackend {
                heads: Api::namespaced_with(
                    client.clone(),
                    namespace,
                    &resource("NunyaPolicy", "nunyapolicies"),
                ),
                copies: Api::namespaced_with(
                    client,
                    namespace,
                    &resource("NunyaPolicyRevision", "nunyapolicyrevisions"),
                ),
                bridge: RuntimeBridge::new(),
            }),
            leader: None,
        }
    }
    /// Construct a serving-loop reader with a bounded per-call deadline.
    pub fn serving_reader(client: Client, namespace: &str, timeout: std::time::Duration) -> Self {
        Self { backend: Arc::new(KubeBackend {
            heads: Api::namespaced_with(client.clone(), namespace, &resource("NunyaPolicy", "nunyapolicies")),
            copies: Api::namespaced_with(client, namespace, &resource("NunyaPolicyRevision", "nunyapolicyrevisions")),
            bridge: RuntimeBridge::with_timeout(timeout),
        }), leader: None }
    }
    /// Quorum GET of the head only, without history repair.
    pub fn head_number(&self, route: &RouteId) -> Result<u64, RevisionStoreError> {
        Ok(self.serving_head(route, API_TIMEOUT)?.map_or(0, |r| r.number))
    }
    /// Fetch only the active service head within the serving cycle budget.
    pub fn serving_head(&self, route: &RouteId, timeout: std::time::Duration) -> Result<Option<PolicyRevision>, RevisionStoreError> {
        let h = self.backend.head_timeout(&name(route)?, timeout)?;
        if let Some(h) = h {
            validate(&h.spec, route)?;
            if h.spec.owner == "service" { return Ok(h.spec.revision); }
        }
        Ok(None)
    }
    /// Fence commits using the supplied election state.
    pub fn with_leader(mut self, leader: LeaderState) -> Self {
        self.leader = Some(leader);
        self
    }
    fn fence(&self) -> Result<(), RevisionStoreError> {
        if self.leader.as_ref().is_some_and(|l| !l.is_leader()) {
            Err(RevisionStoreError::Unavailable)
        } else {
            Ok(())
        }
    }
    fn replay(
        &self,
        route: &RouteId,
        current: &Option<PolicyRevision>,
        key: &str,
        request: &Option<String>,
    ) -> Result<Option<PolicyRevision>, RevisionStoreError> {
        let d = digest(key);
        let mut candidates = current.iter().cloned().collect::<Vec<_>>();
        candidates.extend(self.backend.copies(&format!(
            "{ROUTE_LABEL}={},{RETRY_LABEL}={}",
            route.as_str(),
            &d[..40]
        ))?);
        for r in candidates {
            if r.route != *route || !valid_revision(&r) {
                return Err(RevisionStoreError::Corrupt);
            }
            if r.idempotency_key.as_deref() == Some(key) {
                if r.request_digest != *request {
                    return Err(RevisionStoreError::IdempotencyConflict);
                }
                return Ok(Some(r));
            }
        }
        Ok(None)
    }
    fn read(&self, route: &RouteId) -> Result<Option<VersionedHead>, RevisionStoreError> {
        let h = self.backend.head(&name(route)?)?;
        if let Some(h) = &h {
            validate(&h.spec, route)?;
        }
        Ok(h)
    }
    fn backfill(&self, r: &PolicyRevision) -> Result<(), RevisionStoreError> {
        let n = format!("{}-r{}", name(&r.route)?, r.number);
        match self.backend.copy(&n)? {
            Some(old) if old == *r => Ok(()),
            Some(_) => Err(RevisionStoreError::Corrupt),
            None => match self.backend.create_copy(&n, r) {
                Ok(()) => Ok(()),
                Err(ApiFailure::Conflict) => {
                    if self.backend.copy(&n)?.as_ref() == Some(r) {
                        Ok(())
                    } else {
                        Err(RevisionStoreError::Corrupt)
                    }
                }
                Err(e) => Err(e.into()),
            },
        }
    }
    fn revisions(&self, route: &RouteId) -> Result<Vec<PolicyRevision>, RevisionStoreError> {
        let h = self.read(route)?;
        let mut rows = BTreeMap::new();
        for r in self
            .backend
            .copies(&format!("{ROUTE_LABEL}={}", route.as_str()))?
        {
            if r.route != *route || !valid_revision(&r) {
                return Err(RevisionStoreError::Corrupt);
            }
            if rows.insert(r.number, r).is_some() {
                return Err(RevisionStoreError::Corrupt);
            }
        }
        if let Some(r) = h.and_then(|h| h.spec.revision) {
            match self.backfill(&r) {
                Ok(()) | Err(RevisionStoreError::Unavailable) => {}
                Err(e) => return Err(e),
            }
            if rows.get(&r.number).is_some_and(|old| old != &r) {
                return Err(RevisionStoreError::Corrupt);
            }
            rows.insert(r.number, r);
        }
        Ok(rows.into_values().collect())
    }
}
impl RevisionStore for KubernetesRevisionStore {
    fn serving_snapshot(&self, route: &RouteId, timeout: std::time::Duration) -> Result<(Owner, Option<PolicyRevision>), RevisionStoreError> {
        if timeout.is_zero() { return Err(RevisionStoreError::Unavailable); }
        let Some(head) = self.backend.head_timeout(&name(route)?, timeout)? else { return Ok((Owner::File, None)); };
        validate(&head.spec, route)?;
        if head.spec.owner == "service" { Ok((Owner::Service, head.spec.revision)) }
        else { Ok((Owner::File, None)) }
    }
    fn active(&self, route: &RouteId) -> Result<Option<PolicyRevision>, RevisionStoreError> {
        let r = self.read(route)?.and_then(|h| h.spec.revision);
        if let Some(r) = &r {
            match self.backfill(r) {
                Ok(()) | Err(RevisionStoreError::Unavailable) => {}
                Err(e) => return Err(e),
            }
        }
        Ok(r)
    }
    fn owner(&self, route: &RouteId) -> Result<Owner, RevisionStoreError> {
        Ok(
            if self.read(route)?.is_some_and(|h| h.spec.owner == "service") {
                Owner::Service
            } else {
                Owner::File
            },
        )
    }
    fn release_to_file(&self, route: &RouteId) -> Result<(), RevisionStoreError> {
        if let Some(mut h) = self.read(route)? {
            if h.spec.owner == "file" {
                return Ok(());
            }
            self.fence()?;
            if let Some(r) = &h.spec.revision {
                self.backfill(r)?;
            }
            h.spec.owner = "file".into();
            self.fence()?;
            self.backend.replace(&name(route)?, &h.spec, Some(&h.rv))?;
        }
        Ok(())
    }
    fn get(
        &self,
        route: &RouteId,
        number: u64,
    ) -> Result<Option<PolicyRevision>, RevisionStoreError> {
        Ok(self
            .revisions(route)?
            .into_iter()
            .find(|r| r.number == number))
    }
    fn history(&self, route: &RouteId) -> Result<Vec<RevisionSummary>, RevisionStoreError> {
        Ok(self
            .revisions(route)?
            .iter()
            .map(RevisionSummary::from)
            .collect())
    }
    fn write(
        &self,
        route: &RouteId,
        expected: Option<u64>,
        draft: NewRevision,
    ) -> Result<WriteOutcome, RevisionStoreError> {
        self.fence()?;
        let n = name(route)?;
        let mut h = self.read(route)?;
        if h.is_none() && !self.backend.copies(&format!("{ROUTE_LABEL}={}", route.as_str()))?.is_empty() {
            // A concurrent first writer can commit its head and copy between our
            // GET and LIST. Re-read the commit point before declaring an orphan.
            h = self.read(route)?;
            if h.is_none() {
                return Err(RevisionStoreError::Corrupt);
            }
        }
        let current = h.as_ref().and_then(|h| h.spec.revision.clone());
        if let Some(r) = &current {
            self.backfill(r)?;
        }
        if let Some(key) = &draft.idempotency_key
            && let Some(r) = self.replay(route, &current, key, &draft.request_digest)?
        {
            return Ok(WriteOutcome::Replayed(r));
        }
        if current.as_ref().map(|r| r.number) != expected {
            return Ok(WriteOutcome::Stale { current });
        }
        let number = current
            .as_ref()
            .map_or(0, |r| r.number)
            .checked_add(1)
            .ok_or(RevisionStoreError::Corrupt)?;
        let revision = PolicyRevision {
            number,
            route: route.clone(),
            created_unix_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| RevisionStoreError::Unavailable)?
                .as_millis()
                .try_into()
                .map_err(|_| RevisionStoreError::Corrupt)?,
            policy_toml: draft.policy_toml,
            policy_sha256: draft.policy_sha256,
            catalog_fingerprint: draft.catalog_fingerprint,
            actor: draft.actor,
            kind: draft.kind,
            idempotency_key: draft.idempotency_key,
            request_digest: draft.request_digest,
        };
        let head = Head {
            route: route.clone(),
            owner: if revision.kind == RevisionKind::Import {
                "service".into()
            } else {
                h.as_ref()
                    .map_or_else(|| "file".into(), |h| h.spec.owner.clone())
            },
            active: number,
            revision: Some(revision.clone()),
        };
        let object = head_object(&n, &head, h.as_ref().map(|h| h.rv.as_str()))?;
        if serde_json::to_vec(&object)
            .map_err(|_| RevisionStoreError::Corrupt)?
            .len()
            > LIMIT
        {
            return Err(RevisionStoreError::Oversized);
        }
        self.fence()?;
        match self
            .backend
            .replace(&n, &head, h.as_ref().map(|h| h.rv.as_str()))
        {
            Ok(()) => {}
            Err(ApiFailure::Conflict) => {
                let current = self.read(route)?.and_then(|h| h.spec.revision);
                if let Some(key) = &revision.idempotency_key
                    && let Some(r) = self.replay(route, &current, key, &revision.request_digest)?
                {
                    return Ok(WriteOutcome::Replayed(r));
                }
                return Ok(WriteOutcome::Stale { current });
            }
            Err(e) => return Err(e.into()),
        }
        // Only a successful head CAS permits publishing a history copy.
        match self.backfill(&revision) {
            Ok(()) | Err(RevisionStoreError::Unavailable) => {}
            Err(e) => return Err(e),
        }
        Ok(WriteOutcome::Written(revision))
    }
}
fn resource(kind: &str, plural: &str) -> ApiResource {
    let mut r = ApiResource::from_gvk(&GroupVersionKind::gvk(GROUP, VERSION, kind));
    r.plural = plural.into();
    r
}
fn head_object(
    name: &str,
    head: &Head,
    rv: Option<&str>,
) -> Result<DynamicObject, RevisionStoreError> {
    let mut object = DynamicObject::new(name, &resource("NunyaPolicy", "nunyapolicies"));
    object.metadata.resource_version = rv.map(str::to_owned);
    object.data = serde_json::json!({"spec": head});
    Ok(object)
}
struct KubeBackend {
    heads: Api<DynamicObject>,
    copies: Api<DynamicObject>,
    bridge: RuntimeBridge,
}
fn api_error(e: kube::Error) -> ApiFailure {
    match e {
        kube::Error::Api(e) if e.code == 409 => ApiFailure::Conflict,
        _ => ApiFailure::Unavailable,
    }
}
/// Maximum elapsed time for one Kubernetes API operation, including compound release.
pub(crate) const API_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// Dedicated runtime owned by an ordinary thread, so dropping inside Tokio is safe.
pub(crate) struct RuntimeBridge {
    handle: Option<Handle>,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    timeout: std::time::Duration,
}
impl RuntimeBridge {
    pub(crate) fn new() -> Self { Self::with_timeout(API_TIMEOUT) }
    pub(crate) fn with_timeout(timeout: std::time::Duration) -> Self {
        let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
        let started = std::thread::Builder::new()
            .name("nunya-kube-owner".into())
            .spawn(move || {
                if let Ok(runtime) = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(1)
                    .thread_name("nunya-kube")
                    .enable_all()
                    .build()
                {
                    let _ = ready_tx.send(runtime.handle().clone());
                    runtime.block_on(async {
                        let _ = stop_rx.await;
                    });
                    runtime.shutdown_timeout(std::time::Duration::from_secs(1));
                }
            })
            .is_ok();
        Self {
            handle: if started { ready_rx.recv().ok() } else { None },
            stop: Some(stop_tx),
            timeout,
        }
    }
    // std receive also works on entered spawn_blocking threads. Worker callers may block;
    // API execution remains on the dedicated runtime, so no nested block_on can panic.
    pub(crate) fn run<T: Send + 'static>(
        &self,
        future: impl std::future::Future<Output = T> + Send + 'static,
    ) -> Option<T> {
        self.run_timeout(self.timeout, future)
    }
    pub(crate) fn run_timeout<T: Send + 'static>(&self, timeout: std::time::Duration,
        future: impl std::future::Future<Output = T> + Send + 'static) -> Option<T> {
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        self.handle.as_ref()?.spawn(async move {
            let _ = tx.send(tokio::time::timeout(timeout, future).await.ok());
        });
        rx.recv_timeout(timeout).ok().flatten()
    }
}
impl Drop for RuntimeBridge {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }
}
impl KubeBackend {
    fn run<T: Send + 'static>(
        &self,
        future: impl std::future::Future<Output = Result<T, kube::Error>> + Send + 'static,
    ) -> ApiResult<T> {
        self.bridge
            .run(future)
            .ok_or(ApiFailure::Unavailable)?
            .map_err(api_error)
    }
}
fn decode_copy(object: DynamicObject) -> ApiResult<PolicyRevision> {
    let r: PolicyRevision = serde_json::from_value(
        object
            .data
            .get("spec")
            .cloned()
            .ok_or(ApiFailure::Corrupt)?,
    )
    .map_err(|_| ApiFailure::Corrupt)?;
    if object.metadata.name.as_deref() != Some(&format!("route-{}-r{}", r.route.as_str(), r.number))
        || object
            .metadata
            .labels
            .as_ref()
            .and_then(|l| l.get(ROUTE_LABEL))
            .map(String::as_str)
            != Some(r.route.as_str())
        || r.number == 0
    {
        return Err(ApiFailure::Corrupt);
    }
    if let Some(key) = &r.idempotency_key
        && object
            .metadata
            .labels
            .as_ref()
            .and_then(|l| l.get(RETRY_LABEL))
            .map(String::as_str)
            != Some(&digest(key)[..40])
    {
        return Err(ApiFailure::Corrupt);
    }
    Ok(r)
}
impl Backend for KubeBackend {
    fn head(&self, name: &str) -> ApiResult<Option<VersionedHead>> { self.head_timeout(name, API_TIMEOUT) }
    fn head_timeout(&self, name: &str, timeout: std::time::Duration) -> ApiResult<Option<VersionedHead>> {
        self.bridge.run_timeout(timeout, {
            let api = self.heads.clone();
            let name = name.to_owned();
            async move { api.get_opt(&name).await }
        }).ok_or(ApiFailure::Unavailable)?.map_err(api_error)?
        .map(|o| {
            Ok(VersionedHead {
                rv: o.metadata.resource_version.ok_or(ApiFailure::Corrupt)?,
                spec: serde_json::from_value(
                    o.data.get("spec").cloned().ok_or(ApiFailure::Corrupt)?,
                )
                .map_err(|_| ApiFailure::Corrupt)?,
            })
        })
        .transpose()
    }
    fn replace(&self, name: &str, head: &Head, rv: Option<&str>) -> ApiResult<()> {
        let object = head_object(name, head, rv).map_err(|_| ApiFailure::Corrupt)?;
        if rv.is_some() {
            self.run({
                let api = self.heads.clone();
                let name = name.to_owned();
                async move { api.replace(&name, &PostParams::default(), &object).await }
            })?;
        } else {
            self.run({
                let api = self.heads.clone();
                async move { api.create(&PostParams::default(), &object).await }
            })?;
        }
        Ok(())
    }
    fn copy(&self, name: &str) -> ApiResult<Option<PolicyRevision>> {
        self.run({
            let api = self.copies.clone();
            let name = name.to_owned();
            async move { api.get_opt(&name).await }
        })?
        .map(decode_copy)
        .transpose()
    }
    fn create_copy(&self, name: &str, revision: &PolicyRevision) -> ApiResult<()> {
        let mut o = DynamicObject::new(
            name,
            &resource("NunyaPolicyRevision", "nunyapolicyrevisions"),
        );
        let mut labels = BTreeMap::from([(ROUTE_LABEL.into(), revision.route.as_str().into())]);
        if let Some(d) = &revision.idempotency_key {
            labels.insert(RETRY_LABEL.into(), digest(d)[..40].into());
        }
        o.metadata.labels = Some(labels);
        o.data = serde_json::json!({"spec": revision});
        self.run({
            let api = self.copies.clone();
            async move { api.create(&PostParams::default(), &o).await }
        })?;
        Ok(())
    }
    fn copies(&self, selector: &str) -> ApiResult<Vec<PolicyRevision>> {
        let mut result = Vec::new();
        let mut params = ListParams::default().labels(selector).limit(500);
        loop {
            let page = self.run({
                let api = self.copies.clone();
                let params = params.clone();
                async move { api.list(&params).await }
            })?;
            for object in page.items {
                result.push(decode_copy(object)?);
            }
            match page.metadata.continue_.filter(|s| !s.is_empty()) {
                Some(token) => params = params.continue_token(&token),
                None => break,
            }
        }
        Ok(result)
    }
}

fn revision_schema() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "required": ["number", "route", "created_unix_ms", "policy_toml", "policy_sha256", "catalog_fingerprint", "actor", "kind"],
        "properties": {
            "number": {"type": "integer", "minimum": 1},
            "route": {"type": "string"},
            "created_unix_ms": {"type": "integer", "minimum": 0},
            "policy_toml": {"type": "string"},
            "policy_sha256": {"type": "string"},
            "catalog_fingerprint": {"type": "string"},
            "actor": {"type": "string"},
            "kind": {"x-kubernetes-preserve-unknown-fields": true},
            "idempotency_key": {"type": "string", "nullable": true},
            "request_digest": {"type": "string", "nullable": true}
        }
    })
}
/// Return both namespaced CRDs as JSON documents (valid YAML), separated by YAML markers.
pub fn crd_yaml() -> String {
    [("NunyaPolicy", "nunyapolicies", "nunyapolicy"), ("NunyaPolicyRevision", "nunyapolicyrevisions", "nunyapolicyrevision")].into_iter().map(|(kind, plural, singular)| {
        let mut revision = revision_schema();
        // RevisionKind is an externally tagged serde enum (string or Rollback object).
        // Preserve the embedded representation; the Rust decoder rejects unknown variants.
        revision["properties"]["kind"] = serde_json::json!({"x-kubernetes-preserve-unknown-fields": true});
        let spec = if kind == "NunyaPolicy" {
            revision["nullable"] = true.into();
            serde_json::json!({"type": "object", "required": ["route", "owner", "active", "revision"], "properties": {
                "route": {"type": "string"}, "owner": {"type": "string", "enum": ["file", "service"]},
                "active": {"type": "integer", "minimum": 0}, "revision": revision
            }})
        } else {
            revision["x-kubernetes-validations"] = serde_json::json!([{ "rule": "self == oldSelf", "message": "Policy revision spec is immutable" }]);
            revision
        };
        serde_json::json!({
            "apiVersion": "apiextensions.k8s.io/v1", "kind": "CustomResourceDefinition",
            "metadata": {"name": format!("{plural}.{GROUP}")},
            "spec": {"group": GROUP, "scope": "Namespaced", "names": {"kind": kind, "plural": plural, "singular": singular},
                "versions": [{"name": VERSION, "served": true, "storage": true, "schema": {"openAPIV3Schema": {
                    "type": "object", "required": ["spec"], "properties": {"spec": spec}
                }}}]}
        }).to_string()
    }).collect::<Vec<_>>().join("\n---\n")
}

#[cfg(all(test, feature = "kubernetes"))]
mod tests {
    use super::*;
    use std::sync::{
        Barrier, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    #[derive(Default)]
    struct State {
        heads: BTreeMap<String, VersionedHead>,
        copies: BTreeMap<String, PolicyRevision>,
        rv: usize,
    }
    #[derive(Default)]
    struct Fake {
        state: Mutex<State>,
        fail_copy: AtomicBool,
        ignore_retry_label: AtomicBool,
        barrier: Option<Barrier>,
        reads: AtomicUsize,
        pause_head: Mutex<Option<(std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>)>>,
    }
    impl Backend for Fake {
        fn head(&self, name: &str) -> ApiResult<Option<VersionedHead>> {
            let head = self.state.lock().unwrap().heads.get(name).cloned();
            let pause = self.pause_head.lock().unwrap().take();
            if let Some((ready, resume)) = pause {
                ready.send(()).unwrap();
                resume.recv().unwrap();
            }
            if self.reads.fetch_add(1, Ordering::SeqCst) < 2
                && let Some(b) = &self.barrier
            {
                b.wait();
            }
            Ok(head)
        }
        fn replace(&self, name: &str, head: &Head, rv: Option<&str>) -> ApiResult<()> {
            let mut s = self.state.lock().unwrap();
            if s.heads.get(name).map(|h| h.rv.as_str()) != rv {
                return Err(ApiFailure::Conflict);
            }
            s.rv += 1;
            let h = VersionedHead {
                spec: head.clone(),
                rv: s.rv.to_string(),
            };
            s.heads.insert(name.into(), h);
            Ok(())
        }
        fn copy(&self, name: &str) -> ApiResult<Option<PolicyRevision>> {
            Ok(self.state.lock().unwrap().copies.get(name).cloned())
        }
        fn create_copy(&self, name: &str, revision: &PolicyRevision) -> ApiResult<()> {
            if self.fail_copy.load(Ordering::SeqCst) {
                return Err(ApiFailure::Unavailable);
            }
            let mut s = self.state.lock().unwrap();
            if s.copies.contains_key(name) {
                return Err(ApiFailure::Conflict);
            }
            s.copies.insert(name.into(), revision.clone());
            Ok(())
        }
        fn copies(&self, selector: &str) -> ApiResult<Vec<PolicyRevision>> {
            Ok(self
                .state
                .lock()
                .unwrap()
                .copies
                .values()
                .filter(|r| {
                    selector.split(',').all(|clause| {
                        let (key, value) = clause.split_once('=').unwrap();
                        match key {
                            ROUTE_LABEL => r.route.as_str() == value,
                            RETRY_LABEL => {
                                self.ignore_retry_label.load(Ordering::SeqCst)
                                    || r.idempotency_key
                                        .as_deref()
                                        .is_some_and(|k| digest(k).starts_with(value))
                            }
                            _ => false,
                        }
                    })
                })
                .cloned()
                .collect())
        }
    }
    fn route() -> RouteId {
        RouteId::new("a").unwrap()
    }
    fn draft(key: Option<&str>, request: &str) -> NewRevision {
        NewRevision {
            policy_toml: "private policy body".into(),
            policy_sha256: "sha".into(),
            catalog_fingerprint: "catalog".into(),
            actor: "test".into(),
            kind: RevisionKind::Import,
            idempotency_key: key.map(str::to_owned),
            request_digest: Some(request.into()),
        }
    }
    fn setup() -> (Arc<Fake>, KubernetesRevisionStore) {
        let f = Arc::new(Fake::default());
        let s = KubernetesRevisionStore {
            backend: f.clone(),
            leader: None,
        };
        (f, s)
    }
    fn written(outcome: WriteOutcome) -> PolicyRevision {
        match outcome {
            WriteOutcome::Written(r) => r,
            _ => panic!("expected write"),
        }
    }
    fn first_key(f: &Fake) -> Option<String> {
        f.state.lock().unwrap().copies["route-a-r1"]
            .idempotency_key
            .clone()
    }
    #[test]
    fn missing_head_with_copies_refuses_before_commit() {
        let (f, s) = setup();
        s.write(&route(), None, draft(None, "one")).unwrap();
        f.state.lock().unwrap().heads.clear();
        assert_eq!(s.write(&route(), None, draft(None, "two")), Err(RevisionStoreError::Corrupt));
        let state = f.state.lock().unwrap();
        assert!(state.heads.is_empty());
        assert_eq!(state.copies.len(), 1);
    }
    #[test]
    fn runtime_bridge_pending_future_times_out() {
        let bridge = RuntimeBridge::new();
        let started = std::time::Instant::now();
        assert_eq!(bridge.run(std::future::pending::<()>()), None);
        assert!(started.elapsed() >= API_TIMEOUT);
        assert!(started.elapsed() < API_TIMEOUT + std::time::Duration::from_secs(2));
        assert_eq!(bridge.run(async { 7 }), Some(7));
    }
    #[test]
    fn round_trip_numbering_rollback_and_owner() {
        let (_, s) = setup();
        let route = route();
        assert_eq!(s.owner(&route).unwrap(), Owner::File);
        assert!(s.active(&route).unwrap().is_none());
        let r = written(s.write(&route, None, draft(Some("key"), "one")).unwrap());
        assert_eq!(r.number, 1);
        assert_eq!(s.active(&route).unwrap(), Some(r.clone()));
        assert_eq!(s.get(&route, 1).unwrap(), Some(r.clone()));
        assert_eq!(s.owner(&route).unwrap(), Owner::Service);
        let mut d = draft(None, "two");
        d.kind = RevisionKind::Rollback { from: 1 };
        let second = written(s.write(&route, Some(1), d).unwrap());
        assert_eq!(second.number, 2);
        assert_eq!(second.kind, RevisionKind::Rollback { from: 1 });
        assert_eq!(
            s.history(&route)
                .unwrap()
                .iter()
                .map(|r| r.number)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        s.release_to_file(&route).unwrap();
        assert_eq!(s.owner(&route).unwrap(), Owner::File);
        assert_eq!(s.active(&route).unwrap(), Some(second));
        assert_eq!(s.get(&route, 1).unwrap(), Some(r));
        assert!(s.get(&route, 3).unwrap().is_none());
        assert!(!format!("{:?}", s.active(&route)).contains("private policy body"));
    }
    #[test]
    fn file_owned_head_with_active_number_is_not_a_serving_revision() {
        let (_, s) = setup();
        let route = route();
        let r = written(s.write(&route, None, draft(None, "one")).unwrap());
        assert_eq!(s.head_number(&route).unwrap(), r.number);
        s.release_to_file(&route).unwrap();
        assert_eq!(s.active(&route).unwrap(), Some(r));
        assert_eq!(s.head_number(&route).unwrap(), 0);
        assert!(s.serving_head(&route, API_TIMEOUT).unwrap().is_none());
    }
    #[test]
    fn retry_survives_leader_change_and_checks_full_digest() {
        let (f, s) = setup();
        let route = route();
        let first = written(s.write(&route, None, draft(Some("key"), "one")).unwrap());
        let other = KubernetesRevisionStore {
            backend: f.clone(),
            leader: None,
        };
        assert_eq!(
            other
                .write(&route, None, draft(Some("key"), "one"))
                .unwrap(),
            WriteOutcome::Replayed(first.clone())
        );
        assert_eq!(
            other.write(&route, Some(1), draft(Some("key"), "different")),
            Err(RevisionStoreError::IdempotencyConflict)
        );
        s.write(&route, Some(1), draft(None, "two")).unwrap();
        assert_eq!(
            other
                .write(&route, None, draft(Some("key"), "one"))
                .unwrap(),
            WriteOutcome::Replayed(first)
        );
        assert_eq!(first_key(&f), Some("key".into()));
    }
    #[test]
    fn absent_head_read_before_concurrent_commit_is_not_corruption() {
        let (f, store) = setup();
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let (resume_tx, resume_rx) = std::sync::mpsc::channel();
        *f.pause_head.lock().unwrap() = Some((ready_tx, resume_rx));
        let paused = KubernetesRevisionStore { backend: f.clone(), leader: None };
        let writer = std::thread::spawn(move || {
            paused.write(&route(), None, draft(None, "paused"))
        });
        ready_rx.recv().unwrap();
        let winner = written(store.write(&route(), None, draft(None, "winner")).unwrap());
        resume_tx.send(()).unwrap();
        assert_eq!(writer.join().unwrap(), Ok(WriteOutcome::Stale { current: Some(winner.clone()) }));
        assert_eq!(store.active(&route()).unwrap(), Some(winner.clone()));
        assert_eq!(store.get(&route(), 1).unwrap(), Some(winner));
        assert_eq!(store.history(&route()).unwrap().len(), 1);
    }

    #[test]
    #[ignore]
    fn stale_and_simultaneous_cas_stress() {
        for _ in 0..1000 {
            stale_and_simultaneous_cas_preserve_winner();
        }
    }

    #[test]
    fn stale_and_simultaneous_cas_preserve_winner() {
        let f = Arc::new(Fake {
            barrier: Some(Barrier::new(2)),
            ..Fake::default()
        });
        let threads = (0..2)
            .map(|i| {
                let f = f.clone();
                std::thread::spawn(move || {
                    KubernetesRevisionStore {
                        backend: f,
                        leader: None,
                    }
                    .write(&route(), None, draft(None, &i.to_string()))
                    .unwrap()
                })
            })
            .collect::<Vec<_>>();
        let results = threads
            .into_iter()
            .map(|t| t.join().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            results
                .iter()
                .filter(|r| matches!(r, WriteOutcome::Written(_)))
                .count(),
            1
        );
        assert_eq!(
            results
                .iter()
                .filter(|r| matches!(r, WriteOutcome::Stale { .. }))
                .count(),
            1
        );
        let s = KubernetesRevisionStore {
            backend: f,
            leader: None,
        };
        let winner = results
            .into_iter()
            .find_map(|r| {
                if let WriteOutcome::Written(r) = r {
                    Some(r)
                } else {
                    None
                }
            })
            .unwrap();
        assert_eq!(s.active(&route()).unwrap(), Some(winner.clone()));
        assert_eq!(s.get(&route(), 1).unwrap(), Some(winner));
        assert_eq!(s.history(&route()).unwrap().len(), 1);
        assert!(matches!(
            s.write(&route(), None, draft(None, "stale")).unwrap(),
            WriteOutcome::Stale { .. }
        ));
    }
    #[test]
    fn existing_resource_version_has_exactly_one_winner() {
        let (initial, store) = setup();
        store.write(&route(), None, draft(None, "initial")).unwrap();
        drop(store);
        let state = Arc::try_unwrap(initial)
            .ok()
            .unwrap()
            .state
            .into_inner()
            .unwrap();
        let f = Arc::new(Fake {
            state: Mutex::new(state),
            barrier: Some(Barrier::new(2)),
            ..Fake::default()
        });
        let threads = (0..2)
            .map(|i| {
                let f = f.clone();
                std::thread::spawn(move || {
                    KubernetesRevisionStore {
                        backend: f,
                        leader: None,
                    }
                    .write(&route(), Some(1), draft(None, &i.to_string()))
                    .unwrap()
                })
            })
            .collect::<Vec<_>>();
        let results = threads
            .into_iter()
            .map(|t| t.join().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            results
                .iter()
                .filter(|r| matches!(r, WriteOutcome::Written(_)))
                .count(),
            1
        );
        assert_eq!(
            results
                .iter()
                .filter(|r| matches!(r, WriteOutcome::Stale { .. }))
                .count(),
            1
        );
        let winner = results
            .into_iter()
            .find_map(|r| {
                if let WriteOutcome::Written(r) = r {
                    Some(r)
                } else {
                    None
                }
            })
            .unwrap();
        let store = KubernetesRevisionStore {
            backend: f,
            leader: None,
        };
        assert_eq!(winner.number, 2);
        assert_eq!(store.active(&route()).unwrap(), Some(winner.clone()));
        assert_eq!(store.get(&route(), 2).unwrap(), Some(winner));
        assert_eq!(store.history(&route()).unwrap().len(), 2);
    }
    #[test]
    fn retry_label_prefix_is_not_sufficient_for_replay() {
        let (f, store) = setup();
        store
            .write(&route(), None, draft(Some("key"), "one"))
            .unwrap();
        store.write(&route(), Some(1), draft(None, "two")).unwrap();
        let mut state = f.state.lock().unwrap();
        let key = state
            .copies
            .get_mut("route-a-r1")
            .unwrap()
            .idempotency_key
            .as_mut()
            .unwrap();
        *key = "different raw key".into();
        drop(state);
        // Simulate a label hash-prefix collision: lookup must still compare the raw key.
        f.ignore_retry_label.store(true, Ordering::SeqCst);
        assert!(matches!(
            store
                .write(&route(), None, draft(Some("key"), "one"))
                .unwrap(),
            WriteOutcome::Stale { .. }
        ));
    }
    #[test]
    fn missing_copies_backfill_on_history_and_next_write() {
        for history_first in [true, false] {
            let (f, s) = setup();
            f.fail_copy.store(true, Ordering::SeqCst);
            let first = written(
                s.write(&route(), None, draft(Some("retry"), "one"))
                    .unwrap(),
            );
            assert!(f.state.lock().unwrap().copies.is_empty());
            f.fail_copy.store(false, Ordering::SeqCst);
            if history_first {
                assert_eq!(s.history(&route()).unwrap().len(), 1);
            } else {
                s.write(&route(), Some(1), draft(None, "two")).unwrap();
            }
            assert_eq!(f.state.lock().unwrap().copies["route-a-r1"], first);
        }
    }
    #[test]
    fn committed_head_is_readable_while_copy_creation_is_unavailable() {
        let (f, store) = setup();
        f.fail_copy.store(true, Ordering::SeqCst);
        let first = written(store.write(&route(), None, draft(None, "one")).unwrap());
        assert_eq!(store.active(&route()).unwrap(), Some(first.clone()));
        assert_eq!(store.get(&route(), 1).unwrap(), Some(first));
        assert_eq!(store.history(&route()).unwrap().len(), 1);
        assert_eq!(
            store.write(&route(), Some(1), draft(None, "two")),
            Err(RevisionStoreError::Unavailable)
        );
    }
    #[test]
    fn conflicting_copy_is_corruption() {
        let (f, s) = setup();
        s.write(&route(), None, draft(None, "one")).unwrap();
        f.state
            .lock()
            .unwrap()
            .copies
            .get_mut("route-a-r1")
            .unwrap()
            .actor = "different".into();
        assert_eq!(s.history(&route()), Err(RevisionStoreError::Corrupt));
    }
    #[test]
    fn oversize_and_bad_dns_refused_without_commit() {
        let (_, s) = setup();
        let mut d = draft(None, "one");
        d.policy_toml = "x".repeat(LIMIT);
        assert_eq!(
            s.write(&route(), None, d),
            Err(RevisionStoreError::Oversized)
        );
        assert!(s.active(&route()).unwrap().is_none());
        assert_eq!(
            s.owner(&RouteId::new("a-").unwrap()),
            Err(RevisionStoreError::InvalidRoute)
        );
    }
    #[tokio::test]
    async fn runtime_bridge_from_spawn_blocking_and_worker() {
        let bridge = Arc::new(RuntimeBridge::new());
        let b = bridge.clone();
        assert_eq!(
            tokio::task::spawn_blocking(move || b.run(async { 42 }))
                .await
                .unwrap(),
            Some(42)
        );
        assert_eq!(bridge.run(async { 7 }), Some(7));
    }
    #[test]
    fn retry_after_cas_race_replays_winner() {
        let f = Arc::new(Fake {
            barrier: Some(Barrier::new(2)),
            ..Fake::default()
        });
        let threads = (0..2)
            .map(|_| {
                let f = f.clone();
                std::thread::spawn(move || {
                    KubernetesRevisionStore {
                        backend: f,
                        leader: None,
                    }
                    .write(&route(), None, draft(Some("same"), "same"))
                    .unwrap()
                })
            })
            .collect::<Vec<_>>();
        let results = threads
            .into_iter()
            .map(|t| t.join().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            results
                .iter()
                .filter(|r| matches!(r, WriteOutcome::Written(_)))
                .count(),
            1
        );
        assert_eq!(
            results
                .iter()
                .filter(|r| matches!(r, WriteOutcome::Replayed(_)))
                .count(),
            1
        );
    }
    #[test]
    fn follower_release_noop_preserves_head_and_skips_backfill() {
        let (f, store) = setup();
        let follower = KubernetesRevisionStore {
            backend: f.clone(),
            leader: Some(LeaderState::default()),
        };
        assert_eq!(follower.release_to_file(&route()), Ok(()));
        store.write(&route(), None, draft(None, "one")).unwrap();
        store.release_to_file(&route()).unwrap();
        let rv = f.state.lock().unwrap().heads["route-a"].rv.clone();
        f.state.lock().unwrap().copies.clear();
        f.fail_copy.store(true, Ordering::SeqCst);
        assert_eq!(follower.release_to_file(&route()), Ok(()));
        let state = f.state.lock().unwrap();
        assert_eq!(state.heads["route-a"].rv, rv);
        assert_eq!(state.heads["route-a"].spec.owner, "file");
        assert_eq!(state.heads["route-a"].spec.active, 1);
        assert!(state.copies.is_empty());
    }
    #[test]
    fn nonleader_cannot_commit_or_release() {
        let (f, s) = setup();
        s.write(&route(), None, draft(None, "one")).unwrap();
        let s = s.with_leader(LeaderState::default());
        assert_eq!(
            s.write(&route(), Some(1), draft(None, "two")),
            Err(RevisionStoreError::Unavailable)
        );
        assert_eq!(
            s.release_to_file(&route()),
            Err(RevisionStoreError::Unavailable)
        );
        assert_eq!(f.state.lock().unwrap().heads["route-a"].spec.active, 1);
        assert_eq!(s.owner(&route()).unwrap(), Owner::Service);
    }
    #[test]
    fn crds_are_namespaced_and_separate() {
        assert_eq!(include_str!("../../deploy/kubernetes/crds.yaml").replace("\r\n", "\n"), crd_yaml());
        let yaml = crd_yaml();
        let docs = yaml
            .split("\n---\n")
            .map(|s| serde_json::from_str::<serde_json::Value>(s).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(docs.len(), 2);
        assert_eq!(docs[1]["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"]["x-kubernetes-validations"][0]["rule"], "self == oldSelf");
        for d in docs {
            assert_eq!(d["spec"]["scope"], "Namespaced");
            assert_eq!(d["spec"]["group"], GROUP);
        }
    }

    #[test]
    fn resumed_old_writer_stale_resource_version_cannot_replace_successor() {
        let (fake, s) = setup();
        written(s.write(&route(), None, draft(None,"old")).unwrap());
        let stale = fake.head(&name(&route()).unwrap()).ok().flatten().unwrap();
        let successor = written(s.write(&route(), Some(1), draft(None,"successor")).unwrap());
        struct PausedReader { backend: Arc<Fake>, cached: Mutex<Option<VersionedHead>> }
        impl Backend for PausedReader {
            fn head(&self, name: &str) -> ApiResult<Option<VersionedHead>> {
                if let Some(cached) = self.cached.lock().unwrap().take() { Ok(Some(cached)) } else { self.backend.head(name) }
            }
            fn replace(&self, name: &str, head: &Head, rv: Option<&str>) -> ApiResult<()> { self.backend.replace(name,head,rv) }
            fn copy(&self, name: &str) -> ApiResult<Option<PolicyRevision>> { self.backend.copy(name) }
            fn create_copy(&self, name: &str, r: &PolicyRevision) -> ApiResult<()> { self.backend.create_copy(name,r) }
            fn copies(&self, selector: &str) -> ApiResult<Vec<PolicyRevision>> { self.backend.copies(selector) }
        }
        // Resume a write paused after its head read; bypass the old leadership fence.
        let resumed = KubernetesRevisionStore { backend: Arc::new(PausedReader { backend: fake, cached: Mutex::new(Some(stale)) }), leader: None };
        assert!(matches!(resumed.write(&route(),Some(1),draft(None,"resumed")).unwrap(), WriteOutcome::Stale { current: Some(ref r) } if r == &successor));
        assert_eq!(s.active(&route()).unwrap(), Some(successor));
        let next = written(s.write(&route(), Some(2), draft(None,"next")).unwrap());
        assert_eq!(next.number,3);
    }
}
