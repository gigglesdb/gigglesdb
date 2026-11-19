//! Leader-owned activation based only on monotonic Lease observations.
use super::kube_leader::LeaderState;
use k8s_openapi::api::coordination::v1::Lease;
use std::{
    collections::BTreeMap,
    sync::Mutex,
    time::{Duration, Instant},
};

struct Observation {
    version: String,
    changed: Instant,
    confirmed: BTreeMap<String, u64>,
    lifetime: Option<Duration>,
}
#[derive(Default)]
struct View {
    generation: Option<u64>,
    ready: bool,
    sequence: u64,
    pods: BTreeMap<String, Observation>,
    revisions: BTreeMap<String, (u64, bool)>,
    local: BTreeMap<String, (u64, u64)>,
    failure: Option<&'static str>,
}
/// Shared activation state; every report rechecks the leadership deadline.
pub struct ActivationTracker {
    leader: LeaderState,
    term: Duration,
    view: Mutex<View>,
}
impl ActivationTracker {
    /// Start with no authoritative Lease observations.
    pub fn new(leader: LeaderState, term: Duration) -> Self {
        Self {
            leader,
            term,
            view: Mutex::default(),
        }
    }
    /// Record a local publication and whether its fence tightened.
    pub fn published(&self, route: &str, revision: u64, tightened: bool) {
        let mut v = self.view.lock().unwrap_or_else(|e| e.into_inner());
        let pending = v.revisions.get(route).is_some_and(|(n, _)| !self.active_view(&v, route, *n, Instant::now()));
        v.sequence = v.sequence.wrapping_add(1);
        v.ready = false;
        // Publications made before leadership cannot establish cluster classification.
        let known = self.leader.is_leader();
        v.revisions
            .insert(route.into(), (revision, tightened || !known || pending));
    }
    /// Read whether this process may observe cluster activation.
    pub fn is_leader(&self) -> bool {
        self.leader.is_leader()
    }
    /// Capture the publication sequence before starting a Lease list request.
    pub fn begin_observation(&self) -> u64 {
        self.view.lock().unwrap_or_else(|e| e.into_inner()).sequence
    }
    /// Accept a Lease list only if no publication raced its request.
    pub fn observe_if_current(&self, leases: Vec<Lease>, now: Instant, sequence: u64) {
        self.observe_version(leases, now, Some(sequence));
    }
    /// Replace the complete label-filtered Lease list at a monotonic instant.
    pub fn observe(&self, leases: Vec<Lease>, now: Instant) {
        self.observe_version(leases, now, None);
    }
    fn observe_version(&self, leases: Vec<Lease>, now: Instant, sequence: Option<u64>) {
        let mut v = self.view.lock().unwrap_or_else(|e| e.into_inner());
        if sequence.is_some_and(|s| s != v.sequence) {
            return;
        }
        if !self.leader.is_leader() {
            v.ready = false;
            v.generation = None;
            v.pods.clear();
            return;
        }
        let generation = self.leader.generation();
        if v.generation != Some(generation) {
            v.pods.clear();
            v.generation = Some(generation);
        }
        let mut next = BTreeMap::new();
        for lease in leases {
            let Some(name) = lease.metadata.name else {
                continue;
            };
            let version = lease.metadata.resource_version.unwrap_or_default();
            let spec = lease.spec.as_ref();
            let identity_ok = name.strip_prefix("nunya-serving-").is_some_and(|pod| spec.and_then(|s| s.holder_identity.as_deref()) == Some(pod));
            let lifetime = spec.and_then(|s| s.lease_duration_seconds).filter(|n| *n > 0).map(|n| self.term.max(Duration::from_secs(n as u64)));
            let mut confirmed = lease
                .metadata
                .annotations
                .and_then(|a| a.get(super::kube_serving::CONFIRMED).cloned())
                .and_then(|a| serde_json::from_str(&a).ok())
                .unwrap_or_default();
            if !identity_ok { confirmed = BTreeMap::new(); }
            let changed = v
                .pods
                .get(&name)
                .filter(|o| !version.is_empty() && o.version == version)
                .map_or(now, |o| o.changed);
            next.insert(
                name,
                Observation {
                    version,
                    changed,
                    confirmed,
                    lifetime,
                },
            );
        }
        v.pods = next;
        v.ready = true;
    }
    /// Invalidate cluster reports when the API list fails.
    pub fn unavailable(&self) {
        self.view.lock().unwrap_or_else(|e| e.into_inner()).ready = false;
    }
    /// Store only value-free local renewal progress.
    pub fn local(&self, route: &str, loaded: u64, confirmed: u64) {
        let mut v = self.view.lock().unwrap_or_else(|e| e.into_inner());
        v.local.insert(route.into(), (loaded, confirmed));
        v.failure = None;
    }
    /// Retain a fixed refusal reason without policy text.
    pub fn failed(&self) {
        self.view.lock().unwrap_or_else(|e| e.into_inner()).failure =
            Some("adoption_or_renewal_unavailable");
    }
    /// A known loosening is immediate only if no predecessor remains pending.
    pub fn loosened(&self, route: &str, revision: u64) -> bool {
        self.leader.is_leader() && self.view.lock().unwrap_or_else(|e| e.into_inner()).revisions.get(route) == Some(&(revision, false))
    }
    /// Whether a revision is cluster active; unknown classification is tightening.
    pub fn active(&self, route: &str, revision: u64, now: Instant) -> bool {
        if !self.leader.is_leader() {
            return false;
        }
        let v = self.view.lock().unwrap_or_else(|e| e.into_inner());
        self.active_view(&v, route, revision, now)
    }
    fn active_view(&self, v: &View, route: &str, revision: u64, now: Instant) -> bool {
        if v.revisions.get(route) == Some(&(revision, false)) {
            return true;
        }
        v.ready
            && v.generation == Some(self.leader.generation())
            && v.pods.values().all(|p| {
                p.lifetime.is_some_and(|term| now.saturating_duration_since(p.changed) >= term)
                    || p.confirmed.get(route).copied().unwrap_or(0) >= revision
            })
    }
    /// Build sanitized per-pod and local progress for management responses.
    pub fn status(&self, route: &str, now: Instant) -> serde_json::Value {
        let v = self.view.lock().unwrap_or_else(|e| e.into_inner());
        let leader = self.leader.is_leader();
        let pods: Vec<_> = if leader && v.generation == Some(self.leader.generation()) {
            v.pods.iter().map(|(name, p)| {
            let age = now.saturating_duration_since(p.changed);
            serde_json::json!({"lease_name":name,"confirmed_revision":p.confirmed.get(route).copied().unwrap_or(0),"live":!p.lifetime.is_some_and(|term| age >= term),"unchanged_ms":age.as_millis() as u64})
        }).collect()
        } else {
            Vec::new()
        };
        let local = v.local.get(route).copied().unwrap_or_default();
        serde_json::json!({"activation_owner":"leader","activation_reason":if leader { None } else { Some("leader owns activation") },"is_leader":leader,"loaded_revision":local.0,"confirmed_revision":local.1,"last_failure":v.failure,"pods":pods})
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn lease(name: &str, version: &str, revision: u64) -> Lease {
        let mut l = Lease::default();
        l.metadata.name = Some(format!("nunya-serving-{name}"));
        l.spec = Some(k8s_openapi::api::coordination::v1::LeaseSpec { holder_identity: Some(name.into()), lease_duration_seconds: Some(6), ..Default::default() });
        l.metadata.resource_version = Some(version.into());
        l.metadata.annotations = Some(BTreeMap::from([(
            super::super::kube_serving::CONFIRMED.into(),
            format!("{{\"a\":{revision}}}"),
        )]));
        l
    }
    #[test]
    fn tightening_requires_every_live_confirmation_and_drops_disappeared() {
        let t = ActivationTracker::new(LeaderState::live_for_test(), Duration::from_secs(6));
        let now = Instant::now();
        t.published("a", 2, true);
        t.observe(
            vec![lease("a", "1", 2), lease("b", "1", 1), lease("c", "1", 2)],
            now,
        );
        assert!(!t.active("a", 2, now));
        t.observe(
            vec![lease("a", "2", 2), lease("b", "2", 2), lease("c", "1", 2)],
            now,
        );
        assert!(t.active("a", 2, now));
        t.published("a", 3, true);
        assert!(!t.active("a", 3, now));
        t.observe(vec![lease("a", "3", 3)], now);
        assert!(t.active("a", 3, now));
        assert_eq!(t.status("a", now)["pods"].as_array().unwrap().len(), 1);
    }
    #[test]
    fn expiry_is_full_observed_term_and_new_leader_restarts_observations() {
        let leader = LeaderState::live_for_test();
        let term = Duration::from_secs(6);
        let t = ActivationTracker::new(leader.clone(), term);
        let now = Instant::now();
        t.observe(vec![lease("dead", "1", 1), lease("confirmed", "1", 2)], now);
        assert!(!t.active("a", 2, now + term - Duration::from_nanos(1)));
        assert!(t.active("a", 2, now + term));
        // A renewal resets only that Lease's observation age.
        t.observe(
            vec![lease("dead", "2", 1), lease("confirmed", "1", 2)],
            now + term,
        );
        assert!(!t.active("a", 2, now + term));
        let successor = ActivationTracker::new(leader, term);
        successor.observe(vec![lease("dead", "2", 1)], now + term * 2);
        assert!(!successor.active("a", 2, now + term * 2));
        assert!(successor.active("a", 2, now + term * 3));
    }
    #[test]
    fn loosening_is_immediate_unknown_and_unavailable_are_conservative() {
        let t = ActivationTracker::new(LeaderState::live_for_test(), Duration::from_secs(6));
        let now = Instant::now();
        assert!(!t.active("a", 2, now));
        t.published("a", 2, false);
        assert!(t.active("a", 2, now));
        t.published("a", 3, true);
        assert!(!t.active("a", 3, now));
        t.observe(vec![lease("a", "1", 3)], now);
        assert!(t.active("a", 3, now));
        t.unavailable();
        assert!(!t.active("a", 3, now));
        let follower = ActivationTracker::new(LeaderState::default(), Duration::from_secs(6));
        follower.published("a", 2, false);
        assert!(!follower.active("a", 2, now));
    }

    #[test]
    fn per_pod_duration_and_invalid_duration_stay_live() {
        let t = ActivationTracker::new(LeaderState::live_for_test(), Duration::from_secs(6));
        let now = Instant::now();
        let mut l = lease("slow", "1", 1);
        l.spec.as_mut().unwrap().lease_duration_seconds = Some(20);
        t.observe(vec![l.clone()], now);
        for secs in [6, 19] { assert!(!t.active("a", 2, now + Duration::from_secs(secs))); }
        assert!(t.active("a", 2, now + Duration::from_secs(20)));
        for duration in [None, Some(0), Some(-1)] {
            l.spec.as_mut().unwrap().lease_duration_seconds = duration;
            t.observe(vec![l.clone()], now);
            assert!(!t.active("a", 2, now + Duration::from_secs(200)));
        }
    }
    #[test]
    fn mismatched_identity_cannot_confirm() {
        let t = ActivationTracker::new(LeaderState::live_for_test(), Duration::from_secs(6));
        let now = Instant::now();
        let mut l = lease("pod", "1", 2);
        l.spec.as_mut().unwrap().holder_identity = Some("other".into());
        t.observe(vec![l], now);
        assert!(!t.active("a", 2, now));
        assert_eq!(t.status("a", now)["pods"][0]["live"], true);
    }
    #[test]
    fn loosening_and_identical_revision_wait_for_pending_predecessor() {
        for expires in [false, true] {
            let t = ActivationTracker::new(LeaderState::live_for_test(), Duration::from_secs(6));
            let now = Instant::now();
            t.published("a", 2, true);
            t.observe(vec![lease("pod", "1", 1)], now);
            t.published("a", 3, false);
            assert!(!t.loosened("a", 3));
            t.observe(vec![lease("pod", "1", 1)], now);
            assert!(!t.active("a", 3, now));
            t.published("a", 4, false);
            t.observe(vec![lease("pod", "2", if expires { 1 } else { 4 })], now);
            assert_eq!(t.active("a", 4, now), !expires);
            assert!(t.active("a", 4, now + Duration::from_secs(6)));
        }
    }
    #[test]
    fn publication_requires_list_started_after_write() {
        let t = ActivationTracker::new(LeaderState::live_for_test(), Duration::from_secs(6));
        let now = Instant::now();
        t.observe(vec![lease("old", "1", 1)], now);
        let stale = t.begin_observation();
        t.published("a", 2, true);
        assert!(!t.active("a", 2, now + Duration::from_secs(6)));
        t.observe_if_current(
            vec![lease("old", "1", 1)],
            now + Duration::from_secs(6),
            stale,
        );
        assert!(!t.active("a", 2, now + Duration::from_secs(6)));
        let fresh = t.begin_observation();
        t.observe_if_current(
            vec![lease("old", "2", 1)],
            now + Duration::from_secs(6),
            fresh,
        );
        assert!(!t.active("a", 2, now + Duration::from_secs(6)));
        assert!(t.active("a", 2, now + Duration::from_secs(12)));
    }
}
