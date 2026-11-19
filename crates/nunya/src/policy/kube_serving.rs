//! Per-pod serving confirmation with monotonic expiration.
//!
//! A leader listing leases after writing revision R either sees this pod confirming
//! an older revision and waits, or lists before this pod's Lease write. In the latter
//! case the post-write head GET observes R and prevents renewal under the old policy.
//! renewTime is diagnostic only; the local deadline always starts before the first GET.
use super::{
    kube_store::{API_TIMEOUT, KubernetesRevisionStore, RuntimeBridge},
    serving::{ServingGate, now_nanos},
};
use crate::{config::KubernetesPolicyStore, state::model::RouteId};
use k8s_openapi::{
    api::coordination::v1::{Lease, LeaseSpec},
    apimachinery::pkg::apis::meta::v1::{MicroTime, OwnerReference},
};
use kube::{Api, Client, api::{PostParams, ListParams}};
use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
pub(crate) const CONFIRMED: &str = "nunya.gigglesdb.dev/confirmed";
/// Sanitized serving-cycle failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServingError {
    /// Invalid configuration or unavailable loaded policy.
    Invalid,
    /// API unavailable or deadline exceeded.
    Unavailable,
    /// Lease CAS conflict.
    Conflict,
}
trait Clock: Send + Sync {
    fn now(&self) -> u64;
}
struct RealClock;
impl Clock for RealClock {
    fn now(&self) -> u64 {
        now_nanos()
    }
}
trait Backend: Send + Sync {
    fn head(&self, route: &RouteId, timeout: Duration) -> Result<u64, ServingError>;
    fn lease(&self, timeout: Duration) -> Result<Option<Lease>, ServingError>;
    fn list(&self, _timeout: Duration) -> Result<Vec<Lease>, ServingError> { Ok(Vec::new()) }
    fn revision(&self, _route: &RouteId, _timeout: Duration) -> Result<crate::policy::PolicyRevision, ServingError> { Err(ServingError::Unavailable) }
    fn save(&self, lease: Lease, create: bool, timeout: Duration) -> Result<(), ServingError>;
}
struct KubeBackend {
    heads: KubernetesRevisionStore,
    leases: Api<Lease>,
    name: String,
    bridge: RuntimeBridge,
}
fn api_error(e: kube::Error) -> ServingError {
    match e {
        kube::Error::Api(e) if e.code == 409 => ServingError::Conflict,
        _ => ServingError::Unavailable,
    }
}
impl Backend for KubeBackend {
    fn revision(&self, route: &RouteId, timeout: Duration) -> Result<crate::policy::PolicyRevision, ServingError> {
        self.heads.serving_head(route, timeout).map_err(|_| ServingError::Unavailable)?.ok_or(ServingError::Invalid)
    }
    fn list(&self, timeout: Duration) -> Result<Vec<Lease>, ServingError> {
        let api = self.leases.clone();
        self.bridge.run_timeout(timeout, async move { api.list(&ListParams::default().labels("nunya.gigglesdb.dev/serving=true")).await })
            .ok_or(ServingError::Unavailable)?.map(|l| l.items).map_err(api_error)
    }
    fn head(&self, route: &RouteId, timeout: Duration) -> Result<u64, ServingError> {
        self.heads
            .serving_head(route, timeout)
            .map(|r| r.map_or(0, |r| r.number)).map_err(|_| ServingError::Unavailable)
    }
    fn lease(&self, timeout: Duration) -> Result<Option<Lease>, ServingError> {
        let api = self.leases.clone();
        let name = self.name.clone();
        self.bridge
            .run_timeout(timeout, async move { api.get_opt(&name).await })
            .ok_or(ServingError::Unavailable)?
            .map_err(api_error)
    }
    fn save(&self, lease: Lease, create: bool, timeout: Duration) -> Result<(), ServingError> {
        let api = self.leases.clone();
        let name = self.name.clone();
        self.bridge
            .run_timeout(timeout, async move {
                if create {
                    api.create(&PostParams::default(), &lease).await
                } else {
                    api.replace(&name, &PostParams::default(), &lease).await
                }
            })
            .ok_or(ServingError::Unavailable)?
            .map(|_| ())
            .map_err(api_error)
    }
}
/// Caller-supplied view of loaded revisions and the future adoption hook.
pub trait LoadedPolicies: Send + Sync {
    /// Revision actually loaded by the route, or fail closed.
    fn loaded(&self, route: &RouteId) -> Result<u64, ServingError>;
    /// Wait for older local releases before confirming a loaded revision.
    fn released(&self, _route: &RouteId, _timeout: Duration) -> bool { true }
    /// Publish the fetched head locally, without further revision-store API calls.
    fn adopt(&self, route: &RouteId, revision: &crate::policy::PolicyRevision) -> Result<(), ServingError>;
}
/// Per-pod Lease renewer; it never deletes leases on shutdown.
pub struct ServingRenewer {
    stopped: std::sync::Mutex<bool>,
    confirmed: std::sync::Mutex<BTreeMap<String, u64>>,
    tracker: Option<Arc<super::kube_activation::ActivationTracker>>,
    backend: Arc<dyn Backend>,
    clock: Arc<dyn Clock>,
    gate: Arc<ServingGate>,
    loaded: Arc<dyn LoadedPolicies>,
    routes: Vec<RouteId>,
    config: KubernetesPolicyStore,
}
impl ServingRenewer {
    /// Build a bounded Kubernetes serving loop with a closed release gate.
    pub fn new(
        client: Client,
        config: KubernetesPolicyStore,
        routes: Vec<RouteId>,
        loaded: Arc<dyn LoadedPolicies>,
        gate: Arc<ServingGate>,
        tracker: Arc<super::kube_activation::ActivationTracker>,
    ) -> Result<Self, ServingError> {
        let name = format!("nunya-serving-{}", config.pod_name);
        let term = Duration::from_secs(config.serving_lease_secs);
        let margin = Duration::from_millis(config.serving_margin_ms);
        let interval = Duration::from_millis(config.serving_renew_interval_ms);
        let usable = term.checked_sub(margin).ok_or(ServingError::Invalid)?;
        if !super::serving::valid_lease_name(&name)
            || interval.is_zero()
            || interval.saturating_mul(2) >= usable
            || i32::try_from(config.serving_lease_secs).is_err()
        {
            return Err(ServingError::Invalid);
        }
        let timeout = if API_TIMEOUT > usable {
            usable - interval
        } else {
            API_TIMEOUT
        };
        gate.close();
        Ok(Self {
            stopped: std::sync::Mutex::new(false),
            confirmed: std::sync::Mutex::new(BTreeMap::new()),
            tracker: Some(tracker),
            backend: Arc::new(KubeBackend {
                heads: KubernetesRevisionStore::serving_reader(
                    client.clone(),
                    &config.namespace,
                    timeout,
                ),
                leases: Api::namespaced(client, &config.namespace),
                name,
                bridge: RuntimeBridge::with_timeout(timeout),
            }),
            clock: Arc::new(RealClock),
            gate,
            loaded,
            routes,
            config,
        })
    }
    /// Confirm heads, CAS the Lease, then confirm heads again before extending.
    pub fn cycle(&self) -> Result<(), ServingError> {
        let t0 = self.clock.now();
        let budget = Duration::from_secs(self.config.serving_lease_secs)
            .saturating_sub(Duration::from_millis(self.config.serving_margin_ms))
            .saturating_sub(Duration::from_millis(self.config.serving_renew_interval_ms));
        let end = t0.saturating_add(budget.as_nanos().try_into().map_err(|_| ServingError::Invalid)?);
        let result = self.renew(t0, end);
        if let Some(t) = &self.tracker {
            if result.is_err() { t.failed(); }
            if t.is_leader() {
                let sequence = t.begin_observation();
                match self.remaining(end).and_then(|timeout| self.backend.list(timeout)) {
                    Ok(leases) => t.observe_if_current(leases, std::time::Instant::now(), sequence),
                    Err(_) => t.unavailable(),
                }
            } else { t.observe(Vec::new(), std::time::Instant::now()); }
        }
        let deadline = result?;
        self.remaining(end)?;
        let stopped = self.stopped.lock().map_err(|_| ServingError::Unavailable)?;
        if *stopped { return Err(ServingError::Unavailable); }
        self.gate.extend_to(deadline);
        Ok(())
    }
    fn remaining(&self, end: u64) -> Result<Duration, ServingError> {
        let now = self.clock.now();
        if now >= end { return Err(ServingError::Unavailable); }
        Ok(Duration::from_nanos(end - now))
    }
    fn renew(&self, t0: u64, end: u64) -> Result<u64, ServingError> {
        let mut loaded_heads = BTreeMap::new();
        let mut confirmed = self.confirmed.lock().map_err(|_| ServingError::Unavailable)?.clone();
        let wait = Duration::from_millis(self.config.serving_renew_interval_ms).min(Duration::from_secs(1));
        let wait_start = std::time::Instant::now();
        for route in &self.routes {
            let head = self.backend.head(route, self.remaining(end)?)?;
            let local = self.loaded.loaded(route)?;
            if head != 0 && local != head {
                let revision = self.backend.revision(route, self.remaining(end)?)?;
                if revision.number != head { return Err(ServingError::Invalid); }
                self.loaded.adopt(route, &revision)?;
            }
            let loaded = if head == 0 { 0 } else { self.loaded.loaded(route)? };
            if loaded != head {
                return Err(ServingError::Invalid);
            }
            loaded_heads.insert(route.as_str().to_owned(), loaded);
            if head == 0 || self.loaded.released(route, wait.saturating_sub(wait_start.elapsed()).min(self.remaining(end)?)) { confirmed.insert(route.as_str().to_owned(), loaded); }

        }
        for attempt in 0..2 {
            let previous = self.backend.lease(self.remaining(end)?)?;
            let create = previous.is_none();
            let mut lease = previous.unwrap_or_default();
            lease.metadata.name = Some(format!("nunya-serving-{}", self.config.pod_name));
            lease
                .metadata
                .labels
                .get_or_insert_default()
                .insert("nunya.gigglesdb.dev/serving".into(), "true".into());
            lease.metadata.annotations.get_or_insert_default().insert(
                CONFIRMED.into(),
                serde_json::to_string(&confirmed).map_err(|_| ServingError::Invalid)?,
            );
            lease.metadata.owner_references = self.config.pod_uid.as_ref().map(|uid| {
                vec![OwnerReference {
                    api_version: "v1".into(),
                    kind: "Pod".into(),
                    name: self.config.pod_name.clone(),
                    uid: uid.clone(),
                    block_owner_deletion: None,
                    controller: None,
                }]
            });
            let micros = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| ServingError::Unavailable)?
                .as_micros();
            lease.spec = Some(LeaseSpec {
                holder_identity: Some(self.config.pod_name.clone()),
                lease_duration_seconds: Some(self.config.serving_lease_secs as i32),
                renew_time: Some(MicroTime(
                    k8s_openapi::jiff::Timestamp::from_microsecond(
                        micros.try_into().map_err(|_| ServingError::Unavailable)?,
                    )
                    .map_err(|_| ServingError::Unavailable)?,
                )),
                ..Default::default()
            });
            match self.backend.save(lease, create, self.remaining(end)?) {
                Ok(()) => break,
                Err(ServingError::Conflict) if attempt == 0 => continue,
                Err(e) => return Err(e),
            }
        }
        *self.confirmed.lock().map_err(|_| ServingError::Unavailable)? = confirmed.clone();
        for route in &self.routes {
            if let Some(t) = &self.tracker { t.local(route.as_str(), loaded_heads[route.as_str()], confirmed.get(route.as_str()).copied().unwrap_or(0)); }
        }
        for route in &self.routes {
            if self.backend.head(route, self.remaining(end)?)? != loaded_heads[route.as_str()]
                || (loaded_heads[route.as_str()] != 0 && self.loaded.loaded(route)? != loaded_heads[route.as_str()])
            {
                return Err(ServingError::Invalid);
            }
        }
        let usable = Duration::from_secs(self.config.serving_lease_secs)
            - Duration::from_millis(self.config.serving_margin_ms);
        let deadline = t0.saturating_add(
            usable
                .as_nanos()
                .try_into()
                .map_err(|_| ServingError::Invalid)?,
        );
        self.remaining(end)?;
        Ok(deadline)
    }
    /// Renew alongside election, closing promptly when shutdown is requested.
    pub async fn run(self, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        let interval = Duration::from_millis(self.config.serving_renew_interval_ms);
        let renewer = Arc::new(self);
        let _close = CloseOnDrop(renewer.clone());
        loop {
            if *shutdown.borrow() {
                break;
            }
            let cycle = renewer.clone();
            let task = tokio::task::spawn_blocking(move || cycle.cycle());
            tokio::select! {
                _ = task => {},
                _ = shutdown.changed() => break,
            }
            tokio::select! { _ = tokio::time::sleep(interval) => {}, _ = shutdown.changed() => break }
        }
    }
}
struct CloseOnDrop(Arc<ServingRenewer>);
impl Drop for CloseOnDrop {
    fn drop(&mut self) {
        *self
            .0
            .stopped
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        self.0.gate.close();
    }
}

#[cfg(all(test, feature = "kubernetes"))]
mod tests {
    use super::*;
    use std::sync::{
        Mutex,
        atomic::{AtomicU64, Ordering::SeqCst},
    };
    #[derive(Default)]
    struct Fake {
        now: AtomicU64,
        heads: Mutex<std::collections::VecDeque<Result<u64, ServingError>>>,
        saves: Mutex<std::collections::VecDeque<Result<(), ServingError>>>,
        written: Mutex<Vec<Lease>>,
        reads: AtomicU64,
        delay: AtomicU64,
        list_delay: AtomicU64,
    }
    impl Clock for Fake {
        fn now(&self) -> u64 {
            self.now.load(SeqCst)
        }
    }
    impl Backend for Fake {
        fn list(&self, _timeout: Duration) -> Result<Vec<Lease>, ServingError> {
            self.now.fetch_add(self.list_delay.load(SeqCst), SeqCst);
            Ok(Vec::new())
        }
        fn head(&self, _: &RouteId, _timeout: Duration) -> Result<u64, ServingError> {
            self.now.fetch_add(100_000_000 + self.delay.load(SeqCst), SeqCst);
            self.heads.lock().unwrap().pop_front().unwrap_or(Ok(1))
        }
        fn lease(&self, _timeout: Duration) -> Result<Option<Lease>, ServingError> {
            let n = self.reads.fetch_add(1, SeqCst);
            let mut lease = Lease::default();
            lease.metadata.resource_version = Some(n.to_string());
            Ok(Some(lease))
        }
        fn save(&self, lease: Lease, _: bool, _timeout: Duration) -> Result<(), ServingError> {
            self.written.lock().unwrap().push(lease);
            self.saves.lock().unwrap().pop_front().unwrap_or(Ok(()))
        }
    }
    struct Loaded;
    impl LoadedPolicies for Loaded {
        fn loaded(&self, _: &RouteId) -> Result<u64, ServingError> {
            Ok(1)
        }
        fn adopt(&self, _: &RouteId, _: &crate::policy::PolicyRevision) -> Result<(), ServingError> {
            Err(ServingError::Invalid)
        }
    }
    fn fixture() -> (Arc<Fake>, ServingRenewer) {
        let fake = Arc::new(Fake::default());
        fake.now.store(now_nanos(), SeqCst);
        let renewer = ServingRenewer {
            backend: fake.clone(),
            clock: fake.clone(),
            stopped: Mutex::new(false),
            confirmed: Mutex::new(BTreeMap::new()),
            tracker: None,
            loaded: Arc::new(Loaded),
            gate: Arc::default(),
            routes: vec![RouteId::new("route").unwrap()],
            config: KubernetesPolicyStore {
                namespace: "default".into(),
                lease_name: "leader".into(),
                pod_name: "pod".into(),
                pod_uid: Some("uid".into()),
                management_url: "https://pod".into(),
                lease_duration_secs: 15,
                renew_deadline_secs: 10,
                retry_period_secs: 2,
                serving_lease_secs: 6,
                serving_margin_ms: 1000,
                serving_renew_interval_ms: 1000,
            },
        };
        (fake, renewer)
    }
    #[test]
    fn successful_cycle_uses_request_start_and_pod_metadata() {
        let (fake, r) = fixture();
        let t0 = fake.now();
        r.cycle().unwrap();
        assert!(r.gate.allow());
        // Probe the precise boundary without sleeping or depending on wall time.
        assert!(r.gate.allow_at_for_test(t0 + 5_000_000_000 - 1));
        assert!(!r.gate.allow_at_for_test(t0 + 5_000_000_000));
        let leases = fake.written.lock().unwrap();
        let l = &leases[0];
        assert_eq!(l.metadata.name.as_deref(), Some("nunya-serving-pod"));
        assert_eq!(
            l.metadata.annotations.as_ref().unwrap()[CONFIRMED],
            "{\"route\":1}"
        );
        assert_eq!(
            l.metadata.labels.as_ref().unwrap()["nunya.gigglesdb.dev/serving"],
            "true"
        );
        assert_eq!(
            l.metadata.owner_references.as_ref().unwrap()[0].block_owner_deletion,
            None
        );
        assert_eq!(
            l.spec.as_ref().unwrap().holder_identity.as_deref(),
            Some("pod")
        );
    }
    #[test]
    fn cycle_budget_exhaustion_never_extends() {
        let (fake, r) = fixture();
        fake.delay.store(2_100_000_000, SeqCst);
        assert_eq!(r.cycle(), Err(ServingError::Unavailable));
        assert!(!r.gate.allow());
    }
    #[test]
    fn leader_list_exceeding_budget_does_not_extend() {
        let (fake, mut r) = fixture();
        r.tracker = Some(Arc::new(super::super::kube_activation::ActivationTracker::new(super::super::kube_leader::LeaderState::live_for_test(), Duration::from_secs(6))));
        fake.list_delay.store(4_000_000_000, SeqCst);
        assert_eq!(r.cycle(), Err(ServingError::Unavailable));
        assert!(!r.gate.allow());
    }
    #[test]
    fn file_owned_head_skips_adoption_and_confirms_zero() {
        let (fake, r) = fixture();
        fake.heads.lock().unwrap().extend([Ok(0), Ok(0)]);
        r.cycle().unwrap();
        assert!(r.gate.allow());
        assert_eq!(fake.written.lock().unwrap()[0].metadata.annotations.as_ref().unwrap()[CONFIRMED], "{\"route\":0}");
    }
    #[test]
    fn write_error_or_timeout_preserves_old_deadline() {
        for error in [ServingError::Unavailable, ServingError::Conflict] {
            let (fake, r) = fixture();
            let old = fake.now() + 10;
            r.gate.extend_to(old);
            fake.saves.lock().unwrap().extend([Err(error), Err(error)]);
            assert!(r.cycle().is_err());
            assert!(r.gate.allow_at_for_test(old - 1));
            assert!(!r.gate.allow_at_for_test(old));
        }
    }
    #[test]
    fn head_changes_after_write_do_not_extend() {
        let (fake, r) = fixture();
        fake.heads.lock().unwrap().extend([Ok(1), Ok(2)]);
        assert!(r.cycle().is_err());
        assert!(!r.gate.allow());
        assert_eq!(fake.written.lock().unwrap().len(), 1);
    }
    #[test]
    fn head_ahead_skips_write() {
        let (fake, r) = fixture();
        fake.heads.lock().unwrap().push_back(Ok(2));
        assert!(r.cycle().is_err());
        assert!(!r.gate.allow());
        assert!(fake.written.lock().unwrap().is_empty());
    }
    #[test]
    fn conflicts_reread_and_retry_once() {
        for succeeds in [true, false] {
            let (fake, r) = fixture();
            fake.saves.lock().unwrap().extend([
                Err(ServingError::Conflict),
                if succeeds {
                    Ok(())
                } else {
                    Err(ServingError::Conflict)
                },
            ]);
            assert_eq!(r.cycle().is_ok(), succeeds);
            assert_eq!(r.gate.allow(), succeeds);
            let written = fake.written.lock().unwrap();
            assert_eq!(written.len(), 2);
            assert_eq!(written[0].metadata.resource_version.as_deref(), Some("0"));
            assert_eq!(written[1].metadata.resource_version.as_deref(), Some("1"));
        }
    }
    #[test]
    fn unavailable_head_does_not_write_or_extend() {
        let (fake, r) = fixture();
        fake.heads
            .lock()
            .unwrap()
            .push_back(Err(ServingError::Unavailable));
        assert!(r.cycle().is_err());
        assert!(!r.gate.allow());
        assert!(fake.written.lock().unwrap().is_empty());
    }
    #[test]
    fn api_bridge_enforces_serving_timeout() {
        let bridge = RuntimeBridge::with_timeout(Duration::from_millis(10));
        assert_eq!(
            bridge.run(async { std::future::pending::<()>().await }),
            None
        );
    }
    #[test]
    fn shutdown_fences_inflight_cycle() {
        let (_, r) = fixture();
        let r = Arc::new(r);
        drop(CloseOnDrop(r.clone()));
        assert!(r.cycle().is_err());
        assert!(!r.gate.allow());
    }

    #[test]
    fn loaded_revision_can_extend_gate_while_confirmation_waits_for_old_slot() {
        struct Policies { fence: Arc<crate::policy::RevisionFence> }
        impl LoadedPolicies for Policies {
            fn loaded(&self, _: &RouteId) -> Result<u64, ServingError> { Ok(1) }
            fn adopt(&self, _: &RouteId, _: &crate::policy::PolicyRevision) -> Result<(), ServingError> { Err(ServingError::Invalid) }
            fn released(&self, _: &RouteId, timeout: Duration) -> bool { self.fence.wait_released(self.fence.epoch(),timeout) == 0 }
        }
        let (fake, mut r) = fixture(); let fence = Arc::new(crate::policy::RevisionFence::default());
        let slot = fence.register(); slot.begin(&fence);
        // Entering a later epoch represents publication of a tightening.
        let old = crate::policy::Policy { columns: BTreeMap::from([(crate::schema::ColumnRef { schema:"public".into(),table:"t".into(),column:"c".into() },crate::policy::Treatment::Show)]), domains:BTreeMap::new(), retired_domains:Default::default(), json_filters:BTreeMap::new(),snapshot_max_age_ms:60000,readlog:crate::policy::ReadLogSink::Stdout };
        let mut new = old.clone(); new.columns.clear(); assert!(fence.publish(&old,&new).is_some());
        r.loaded = Arc::new(Policies { fence }); r.config.serving_renew_interval_ms = 1;
        r.confirmed.lock().unwrap().insert("route".into(),0);
        r.cycle().unwrap(); assert!(r.gate.allow());
        assert_eq!(fake.written.lock().unwrap()[0].metadata.annotations.as_ref().unwrap()[CONFIRMED],"{\"route\":0}");
        slot.end(); r.cycle().unwrap();
        assert_eq!(fake.written.lock().unwrap()[1].metadata.annotations.as_ref().unwrap()[CONFIRMED],"{\"route\":1}");
    }
}
