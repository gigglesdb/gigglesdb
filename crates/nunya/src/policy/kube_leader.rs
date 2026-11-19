//! CAS Lease election with monotonic local leadership expiration.
use super::kube_store::RuntimeBridge;
use k8s_openapi::{api::coordination::v1::Lease, apimachinery::pkg::apis::meta::v1::MicroTime};
use kube::{Api, Client, api::PostParams};
use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
const URL: &str = "nunya.gigglesdb.dev/mgmt-url";

trait Clock: Send + Sync {
    fn mono(&self) -> Duration;
    fn wall(&self) -> i128;
}
struct RealClock(Instant);
impl Clock for RealClock {
    fn mono(&self) -> Duration {
        self.0.elapsed()
    }
    fn wall(&self) -> i128 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .and_then(|d| d.as_nanos().try_into().ok())
            .unwrap_or(0)
    }
}
#[derive(Default)]
struct Published {
    until: Option<Duration>,
    generation: u64,
    term: u64,
    url: Option<String>,
}
/// Cheap shared leadership view; expired renewals are fenced even during a hung API call.
#[derive(Clone)]
pub struct LeaderState {
    view: Arc<RwLock<Published>>,
    clock: Arc<dyn Clock>,
}
impl Default for LeaderState {
    fn default() -> Self {
        Self {
            view: Arc::default(),
            clock: Arc::new(RealClock(Instant::now())),
        }
    }
}
impl LeaderState {
    /// Whether a successful local renewal is still within its deadline.
    pub fn is_leader(&self) -> bool {
        self.view
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .until
            .is_some_and(|d| self.clock.mono() < d)
    }
    /// Held Lease term read atomically with its renewal deadline.
    pub fn held_term(&self) -> Option<u64> {
        let view = self.view.read().unwrap_or_else(std::sync::PoisonError::into_inner);
        view.until.filter(|d| self.clock.mono() < *d).map(|_| view.term)
    }
    #[cfg(test)]
    pub(crate) fn live_for_test() -> Self {
        let s = Self::default(); s.publish(Some(Duration::MAX), None); s
    }
    #[cfg(test)]
    pub(crate) fn term_for_test(&self, term: u64) { self.view.write().unwrap().term = term; }
    /// Acquisition generation used to discard previous activation observations.
    pub fn generation(&self) -> u64 { self.view.read().unwrap_or_else(std::sync::PoisonError::into_inner).generation }
    /// Last observed or held Lease transition term.
    pub fn term(&self) -> u64 { self.view.read().unwrap_or_else(std::sync::PoisonError::into_inner).term }
    /// Last observed leader management URL, if known.
    pub fn leader_url(&self) -> Option<String> {
        self.snapshot().1
    }
    fn snapshot(&self) -> (bool, Option<String>) {
        let v = self
            .view
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let leader = v.until.is_some_and(|d| self.clock.mono() < d);
        // An expired local holder is not a useful redirect target.
        let url = if v.until.is_some() && !leader {
            None
        } else {
            v.url.clone()
        };
        (leader, url)
    }
    fn publish(&self, until: Option<Duration>, url: Option<String>) {
        self.publish_term(until, url, None);
    }
    fn publish_term(&self, until: Option<Duration>, url: Option<String>, term: Option<u64>) {
        let mut view = self.view.write().unwrap_or_else(std::sync::PoisonError::into_inner);
        let generation = view.generation + u64::from(until.is_some() && view.until.is_none_or(|d| self.clock.mono() >= d));
        let term = term.unwrap_or(view.term);
        *view = Published { until, url, generation, term };
    }
}
impl crate::mgmt::routes::Leadership for LeaderState {
    fn leadership(&self) -> (bool, Option<String>) {
        self.snapshot()
    }
}
/// Election timing parameters.
#[derive(Clone, Copy)]
pub struct Timings {
    /// Duration advertised in the Lease, in whole seconds.
    pub lease_duration: Duration,
    /// Maximum time without a successful renewal.
    pub renew_deadline: Duration,
    /// Delay between election attempts.
    pub retry_period: Duration,
}
impl Default for Timings {
    fn default() -> Self {
        Self {
            lease_duration: Duration::from_secs(15),
            renew_deadline: Duration::from_secs(10),
            retry_period: Duration::from_secs(2),
        }
    }
}
impl Timings {
    /// Reject unordered, zero or unrepresentable durations.
    pub fn validate(&self) -> Result<(), ElectionError> {
        if self.retry_period.is_zero()
            || self.retry_period >= self.renew_deadline
            || self.renew_deadline >= self.lease_duration
            || self.lease_duration.subsec_nanos() != 0
            || i32::try_from(self.lease_duration.as_secs()).is_err()
        {
            Err(ElectionError::Invalid)
        } else {
            Ok(())
        }
    }
}
/// Sanitized election failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ElectionError {
    /// Invalid election configuration or malformed Lease.
    Invalid,
    /// Resource version race.
    Conflict,
    /// API unavailable.
    Unavailable,
}
trait LeaseApi: Send + Sync {
    fn get(&self) -> Result<Option<Lease>, ElectionError>;
    fn save(&self, lease: Lease, create: bool) -> Result<(), ElectionError>;
    fn release(&self, identity: &str) -> Result<(), ElectionError> {
        if let Some(mut lease) = self.get()? && clear_holder(&mut lease, identity) {
            self.save(lease, false)?;
        }
        Ok(())
    }
}
fn clear_holder(lease: &mut Lease, identity: &str) -> bool {
    let Some(spec) = &mut lease.spec else { return false; };
    if spec.holder_identity.as_deref() != Some(identity) { return false; }
    spec.holder_identity = None;
    if let Some(a) = &mut lease.metadata.annotations { a.remove(URL); }
    true
}
struct KubeLease {
    api: Api<Lease>,
    name: String,
    bridge: RuntimeBridge,
}
fn api_error(e: kube::Error) -> ElectionError {
    match e {
        kube::Error::Api(e) if e.code == 409 => ElectionError::Conflict,
        _ => ElectionError::Unavailable,
    }
}
impl LeaseApi for KubeLease {
    fn get(&self) -> Result<Option<Lease>, ElectionError> {
        let api = self.api.clone();
        let name = self.name.clone();
        self.bridge
            .run(async move { api.get_opt(&name).await })
            .ok_or(ElectionError::Unavailable)?
            .map_err(api_error)
    }
    fn release(&self, identity: &str) -> Result<(), ElectionError> {
        let api = self.api.clone();
        let name = self.name.clone();
        let identity = identity.to_owned();
        // The read and CAS share one deadline, so shutdown cannot spend five seconds on each.
        self.bridge.run(async move {
            if let Some(mut lease) = api.get_opt(&name).await? && clear_holder(&mut lease, &identity) {
                api.replace(&name, &PostParams::default(), &lease).await?;
            }
            Ok::<(), kube::Error>(())
        }).ok_or(ElectionError::Unavailable)?.map_err(api_error)
    }
    fn save(&self, lease: Lease, create: bool) -> Result<(), ElectionError> {
        let api = self.api.clone();
        let name = self.name.clone();
        self.bridge
            .run(async move {
                if create {
                    api.create(&PostParams::default(), &lease).await
                } else {
                    api.replace(&name, &PostParams::default(), &lease).await
                }
            })
            .ok_or(ElectionError::Unavailable)?
            .map(|_| ())
            .map_err(api_error)
    }
}
#[derive(Default)]
struct Observation {
    resource_version: Option<String>,
    holder: Option<String>,
    since: Duration,
}
/// A single candidate; drive with run, or tick from an external scheduler.
pub struct LeaderElector {
    api: Arc<dyn LeaseApi>,
    state: LeaderState,
    timings: Timings,
    name: String,
    identity: String,
    url: String,
    observed: std::sync::Mutex<Option<Observation>>,
}
impl LeaderElector {
    /// Construct a namespaced candidate without claiming leadership.
    pub fn new(
        client: Client,
        namespace: &str,
        name: &str,
        identity: &str,
        url: &str,
        timings: Timings,
    ) -> Result<Self, ElectionError> {
        timings.validate()?;
        let uri: http::Uri = url.parse().map_err(|_| ElectionError::Invalid)?;
        if name.is_empty()
            || identity.is_empty()
            || !matches!(uri.scheme_str(), Some("https" | "http"))
            || uri.authority().is_none_or(|a| a.as_str().contains('@'))
            || url.contains('#')
            || uri.path_and_query().is_some_and(|p| p.as_str() != "/")
        {
            return Err(ElectionError::Invalid);
        }
        Ok(Self {
            api: Arc::new(KubeLease {
                api: Api::namespaced(client, namespace),
                name: name.into(),
                bridge: RuntimeBridge::new(),
            }),
            state: LeaderState::default(),
            timings,
            name: name.into(),
            identity: identity.into(),
            url: url.trim_end_matches('/').into(),
            observed: std::sync::Mutex::new(None),
        })
    }
    /// Obtain the shared view before starting the election loop.
    pub fn state(&self) -> LeaderState {
        self.state.clone()
    }
    /// Attempt acquisition or renewal once; CAS failures immediately revoke leadership.
    pub fn tick(&self) -> Result<(), ElectionError> {
        let old = match self.api.get() {
            Err(ElectionError::Conflict) => {
                self.state.publish(None, None);
                return Err(ElectionError::Conflict);
            }
            result => result?,
        };
        let create = old.is_none();
        let mut lease = old.unwrap_or_default();
        let mut spec = lease.spec.take().unwrap_or_default();
        let observed_term = u64::try_from(spec.lease_transitions.unwrap_or(0)).map_err(|_| ElectionError::Invalid)?;
        let held = spec.holder_identity.as_deref().filter(|s| !s.is_empty());
        let ours = held == Some(self.identity.as_str());
        if !ours {
            self.state.publish_term(
                None,
                lease
                    .metadata
                    .annotations
                    .as_ref()
                    .and_then(|a| a.get(URL))
                    .cloned()
                    .filter(|_| held.is_some()),
                Some(observed_term),
            );
        }
        // Resource version changes are renewals, regardless of remote wall-clock skew.
        let now_mono = self.state.clock.mono();
        let mut observed = self.observed.lock().map_err(|_| ElectionError::Unavailable)?;
        let identity = (lease.metadata.resource_version.clone(), spec.holder_identity.clone());
        if observed.as_ref().is_none_or(|o| (&o.resource_version, &o.holder) != (&identity.0, &identity.1)) {
            *observed = Some(Observation { resource_version: identity.0, holder: identity.1, since: now_mono });
        }
        let since = observed.as_ref().map_or(now_mono, |o| o.since);
        drop(observed);
        let duration = spec.lease_duration_seconds.filter(|d| *d > 0)
            .map_or(self.timings.lease_duration, |d| Duration::from_secs(d as u64));
        if held.is_some() && !ours && now_mono.saturating_sub(since) <= duration {
            return Ok(());
        }
        if !create && lease.metadata.resource_version.is_none() {
            self.state.publish(None, None);
            return Err(ElectionError::Invalid);
        }
        let now = self.state.clock.wall();
        let stamp = MicroTime(
            k8s_openapi::jiff::Timestamp::from_nanosecond(now)
                .map_err(|_| ElectionError::Invalid)?,
        );
        if !ours {
            spec.lease_transitions = Some(
                spec.lease_transitions
                    .unwrap_or(0)
                    .checked_add(i32::from(!create))
                    .ok_or(ElectionError::Invalid)?,
            );
            spec.acquire_time = Some(stamp.clone());
        }
        spec.renew_time = Some(stamp);
        spec.holder_identity = Some(self.identity.clone());
        spec.lease_duration_seconds = Some(self.timings.lease_duration.as_secs() as i32);
        let acquired_term = u64::try_from(spec.lease_transitions.unwrap_or(0)).map_err(|_| ElectionError::Invalid)?;
        lease.spec = Some(spec);
        lease.metadata.name = Some(self.name.clone());
        lease
            .metadata
            .annotations
            .get_or_insert_with(BTreeMap::new)
            .insert(URL.into(), self.url.clone());
        // Measure from request start: a slow successful response must not extend an old lease.
        let until = self.state.clock.mono() + self.timings.renew_deadline;
        match self.api.save(lease, create) {
            Ok(()) => {
                self.state.publish_term(Some(until), Some(self.url.clone()), Some(acquired_term));
                Ok(())
            }
            Err(ElectionError::Conflict) => {
                self.state.publish(None, None);
                Err(ElectionError::Conflict)
            }
            Err(e) => Err(e),
        }
    }
    /// Relinquish a Lease only when its current holder still matches this pod.
    pub fn stop(&self) -> Result<(), ElectionError> {
        self.state.publish(None, None);
        self.api.release(&self.identity)
    }
    /// Run attempts until graceful shutdown, then release the held Lease.
    pub async fn run(
        self,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Result<(), ElectionError> {
        let this = Arc::new(self);
        loop {
            if *shutdown.borrow() {
                break;
            }
            let candidate = this.clone();
            let _ = tokio::task::spawn_blocking(move || candidate.tick()).await;
            tokio::select! {
                _ = tokio::time::sleep(this.timings.retry_period) => {},
                changed = shutdown.changed() => { if changed.is_err() || *shutdown.borrow() { break; } }
            }
        }
        tokio::time::timeout(super::kube_store::API_TIMEOUT, tokio::task::spawn_blocking(move || this.stop()))
            .await
            .map_err(|_| ElectionError::Unavailable)?
            .map_err(|_| ElectionError::Unavailable)?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    };
    #[derive(Default)]
    struct FakeClock(AtomicU64);
    impl Clock for FakeClock {
        fn mono(&self) -> Duration {
            Duration::from_secs(self.0.load(Ordering::SeqCst))
        }
        fn wall(&self) -> i128 {
            self.mono().as_nanos() as i128
        }
    }
    #[derive(Default)]
    struct Fake {
        lease: Mutex<Option<Lease>>,
        fail: AtomicBool,
        conflict: AtomicBool,
        barrier: Option<std::sync::Barrier>,
        reads: AtomicU64,
    }
    impl LeaseApi for Fake {
        fn get(&self) -> Result<Option<Lease>, ElectionError> {
            if self.fail.load(Ordering::SeqCst) {
                return Err(ElectionError::Unavailable);
            }
            let lease = self.lease.lock().unwrap().clone();
            if self.reads.fetch_add(1, Ordering::SeqCst) < 2
                && let Some(b) = &self.barrier
            {
                b.wait();
            }
            Ok(lease)
        }
        fn save(&self, mut lease: Lease, create: bool) -> Result<(), ElectionError> {
            if self.fail.load(Ordering::SeqCst) {
                return Err(ElectionError::Unavailable);
            }
            let mut old = self.lease.lock().unwrap();
            if self.conflict.load(Ordering::SeqCst)
                || create != old.is_none()
                || (!create
                    && old.as_ref().unwrap().metadata.resource_version
                        != lease.metadata.resource_version)
            {
                return Err(ElectionError::Conflict);
            }
            let rv = old
                .as_ref()
                .and_then(|l| l.metadata.resource_version.as_ref())
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(0)
                + 1;
            lease.metadata.resource_version = Some(rv.to_string());
            *old = Some(lease);
            Ok(())
        }
    }
    fn candidate(api: Arc<Fake>, clock: Arc<FakeClock>, id: &str) -> LeaderElector {
        LeaderElector {
            api,
            state: LeaderState {
                view: Arc::default(),
                clock,
            },
            timings: Timings::default(),
            name: "leader".into(),
            identity: id.into(),
            url: format!("https://{id}:443"),
            observed: std::sync::Mutex::new(None),
        }
    }
    #[test]
    fn acquire_expiry_takeover_and_release() {
        let api = Arc::new(Fake::default());
        let clock = Arc::new(FakeClock::default());
        let a = candidate(api.clone(), clock.clone(), "a");
        let b = candidate(api.clone(), clock.clone(), "b");
        assert!(!a.state.is_leader());
        a.tick().unwrap();
        assert!(a.state.is_leader());
        b.tick().unwrap();
        assert!(!b.state.is_leader());
        assert_eq!(b.state.leader_url().as_deref(), Some("https://a:443"));
        clock.0.store(15, Ordering::SeqCst);
        b.tick().unwrap();
        assert!(!b.state.is_leader());
        clock.0.store(16, Ordering::SeqCst);
        b.tick().unwrap();
        assert!(b.state.is_leader());
        assert_eq!(b.state.term(), 1);
        assert_eq!(b.state.generation(), 1);
        a.tick().unwrap();
        assert_eq!(a.state.term(), 1);
        assert!(!a.state.is_leader());
        assert_eq!(
            api.lease
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .spec
                .as_ref()
                .unwrap()
                .lease_transitions,
            Some(1)
        );
        a.stop().unwrap();
        assert_eq!(
            api.lease
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .spec
                .as_ref()
                .unwrap()
                .holder_identity
                .as_deref(),
            Some("b")
        );
        b.stop().unwrap();
        assert!(!b.state.is_leader());
        assert!(
            api.lease
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .spec
                .as_ref()
                .unwrap()
                .holder_identity
                .is_none()
        );
    }
    #[test]
    fn renewal_deadline_conflict_and_lost_holder() {
        let api = Arc::new(Fake::default());
        let clock = Arc::new(FakeClock::default());
        let a = candidate(api.clone(), clock.clone(), "a");
        a.tick().unwrap();
        clock.0.store(2, Ordering::SeqCst);
        a.tick().unwrap();
        api.fail.store(true, Ordering::SeqCst);
        assert_eq!(a.tick(), Err(ElectionError::Unavailable));
        clock.0.store(12, Ordering::SeqCst);
        assert!(!a.state.is_leader());
        api.fail.store(false, Ordering::SeqCst);
        a.tick().unwrap();
        assert!(a.state.is_leader());
        api.conflict.store(true, Ordering::SeqCst);
        assert_eq!(a.tick(), Err(ElectionError::Conflict));
        assert!(!a.state.is_leader());
        api.conflict.store(false, Ordering::SeqCst);
        a.tick().unwrap();
        api.lease
            .lock()
            .unwrap()
            .as_mut()
            .unwrap()
            .spec
            .as_mut()
            .unwrap()
            .holder_identity = Some("other".into());
        a.tick().unwrap();
        assert!(!a.state.is_leader());
    }
    #[test]
    fn racing_candidates_have_one_winner() {
        let api = Arc::new(Fake {
            barrier: Some(std::sync::Barrier::new(2)),
            ..Fake::default()
        });
        let clock = Arc::new(FakeClock::default());
        let a = Arc::new(candidate(api.clone(), clock.clone(), "a"));
        let b = Arc::new(candidate(api, clock, "b"));
        let threads = [a.clone(), b.clone()].map(|c| {
            std::thread::spawn(move || {
                let _ = c.tick();
            })
        });
        for t in threads {
            t.join().unwrap();
        }
        assert_eq!(
            usize::from(a.state.is_leader()) + usize::from(b.state.is_leader()),
            1
        );
    }
    #[test]
    fn takeover_ignores_skew_malformed_fields_and_resets_on_renewal() {
        for stamp in [Some(-9_000_000_000_000_i128), Some(9_000_000_000_000), None] {
            for duration in [Some(15), Some(0), Some(-1), None] {
                let api = Arc::new(Fake::default());
                let clock = Arc::new(FakeClock::default());
                let a = candidate(api.clone(), clock.clone(), "a");
                let b = candidate(api.clone(), clock.clone(), "b");
                a.tick().unwrap();
                {
                    let mut lease = api.lease.lock().unwrap();
                    let spec = lease.as_mut().unwrap().spec.as_mut().unwrap();
                    spec.renew_time = stamp.map(|s| MicroTime(k8s_openapi::jiff::Timestamp::from_nanosecond(s).unwrap()));
                    spec.lease_duration_seconds = duration;
                }
                b.tick().unwrap();
                clock.0.store(14, Ordering::SeqCst);
                b.tick().unwrap();
                assert!(!b.state.is_leader());
                api.lease.lock().unwrap().as_mut().unwrap().metadata.resource_version = Some("renewed".into());
                b.tick().unwrap();
                clock.0.store(16, Ordering::SeqCst);
                b.tick().unwrap();
                assert!(!b.state.is_leader());
                clock.0.store(30, Ordering::SeqCst);
                b.tick().unwrap();
                assert!(b.state.is_leader());
            }
        }
    }
    #[test]
    fn takeover_respects_observed_positive_lease_duration() {
        let api = Arc::new(Fake::default());
        let clock = Arc::new(FakeClock::default());
        let a = candidate(api.clone(), clock.clone(), "a");
        let b = candidate(api.clone(), clock.clone(), "b");
        a.tick().unwrap();
        api.lease.lock().unwrap().as_mut().unwrap().spec.as_mut().unwrap().lease_duration_seconds = Some(40);
        b.tick().unwrap();
        clock.0.store(39, Ordering::SeqCst);
        b.tick().unwrap();
        assert!(!b.state.is_leader());
        clock.0.store(41, Ordering::SeqCst);
        b.tick().unwrap();
        assert!(b.state.is_leader());
    }
    struct SlowRelease;
    impl LeaseApi for SlowRelease {
        fn get(&self) -> Result<Option<Lease>, ElectionError> {
            Ok(Some(Lease {
                spec: Some(k8s_openapi::api::coordination::v1::LeaseSpec {
                    holder_identity: Some("a".into()), ..Default::default()
                }),
                ..Default::default()
            }))
        }
        fn save(&self, _: Lease, _: bool) -> Result<(), ElectionError> {
            std::thread::sleep(Duration::from_secs(6));
            Ok(())
        }
    }
    #[tokio::test]
    async fn shutdown_release_has_total_api_deadline() {
        let mut a = candidate(Arc::new(Fake::default()), Arc::new(FakeClock::default()), "a");
        a.api = Arc::new(SlowRelease);
        let (_tx, rx) = tokio::sync::watch::channel(true);
        let start = Instant::now();
        assert_eq!(a.run(rx).await, Err(ElectionError::Unavailable));
        assert!(start.elapsed() < Duration::from_secs(6));
    }
    #[test]
    fn invalid_timings_refused() {
        assert!(Timings::default().validate().is_ok());
        assert!(
            Timings {
                retry_period: Duration::from_secs(10),
                ..Timings::default()
            }
            .validate()
            .is_err()
        );
        assert!(
            Timings {
                renew_deadline: Duration::from_secs(15),
                ..Timings::default()
            }
            .validate()
            .is_err()
        );
    }

    #[test]
    fn sigkill_takeover_uses_challenger_clock_and_fences_old_activation() {
        let api = Arc::new(Fake::default());
        let old_clock = Arc::new(FakeClock::default());
        let challenger_clock = Arc::new(FakeClock::default());
        let a = candidate(api.clone(), old_clock.clone(), "a");
        let b = candidate(api, challenger_clock.clone(), "b");
        a.tick().unwrap(); // Simulated SIGKILL: no stop(), no release.
        let tracker = super::super::kube_activation::ActivationTracker::new(a.state.clone(), Duration::from_secs(6));
        tracker.published("a", 2, false);
        assert!(tracker.active("a", 2, Instant::now()));
        b.tick().unwrap();
        challenger_clock.0.store(15, Ordering::SeqCst); b.tick().unwrap(); assert!(!b.state.is_leader());
        challenger_clock.0.store(16, Ordering::SeqCst); b.tick().unwrap(); assert!(b.state.is_leader());
        // Paused old process has not observed its own clock yet.
        assert!(a.state.is_leader());
        old_clock.0.store(10, Ordering::SeqCst);
        assert!(!a.state.is_leader()); assert!(!tracker.active("a", 2, Instant::now()));
    }

    #[test]
    fn reacquisition_resets_existing_tracker_observation_age() {
        let state = LeaderState::live_for_test();
        let tracker = super::super::kube_activation::ActivationTracker::new(state.clone(),Duration::from_secs(6));
        let now = Instant::now(); let mut lease = Lease::default();
        lease.metadata.name = Some("nunya-serving-stopped".into()); lease.metadata.resource_version = Some("1".into());
        lease.spec = Some(k8s_openapi::api::coordination::v1::LeaseSpec { holder_identity: Some("stopped".into()), lease_duration_seconds: Some(6), ..Default::default() });
        tracker.observe(vec![lease.clone()],now);
        assert!(tracker.active("a",2,now+Duration::from_secs(6)));
        state.publish(None,None); state.publish(Some(Duration::MAX),None);
        assert!(!tracker.active("a",2,now+Duration::from_secs(6)));
        tracker.observe(vec![lease],now+Duration::from_secs(6));
        assert!(!tracker.active("a",2,now+Duration::from_secs(11)));
        assert!(tracker.active("a",2,now+Duration::from_secs(12)));
    }
}
