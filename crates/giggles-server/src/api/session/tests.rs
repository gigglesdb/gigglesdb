use super::*;
use crate::{Binding, Request, Role, clock::Clock as ServiceClock};
use giggles_core::{Admin, Limits};
use std::{
    collections::{BTreeSet, VecDeque},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

static NEXT_FIXTURE: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

struct TestClock(Mutex<Instant>);
impl Clock for TestClock {
    fn now(&self) -> Instant {
        *self.0.lock().unwrap()
    }
}
impl TestClock {
    fn advance(&self, duration: Duration) {
        *self.0.lock().unwrap() += duration;
    }
}
struct TestTokens(Mutex<VecDeque<Result<[u8; 32], ApiError>>>);
impl Tokens for TestTokens {
    fn next(&self) -> Result<[u8; 32], ApiError> {
        self.0
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(Err(ApiError::Internal))
    }
}
struct Fixture {
    registry: Arc<Registry>,
    service: Arc<Service>,
    owner: ConnectionIdentity,
    other: ConnectionIdentity,
    clock: Arc<TestClock>,
    bindings: Vec<Binding>,
}
impl Fixture {
    fn new(slots: usize) -> Self {
        let path = crate::test_support::fixture_root().join(format!(
            "api-registry-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).unwrap();
        let admin = Admin::create(path.join("db.redb"), Limits::default()).unwrap();
        admin
            .sql_session()
            .execute("CREATE TABLE app (id BIGINT PRIMARY KEY)", &[], None)
            .unwrap();
        let role = Role::Application {
            domain: admin.authority_metadata().unwrap().0.to_string(),
            tables: BTreeSet::from(["app".into()]),
        };
        let bindings = vec![
            Binding {
                fingerprint: "11".repeat(32),
                role: role.clone(),
            },
            Binding {
                fingerprint: "22".repeat(32),
                role,
            },
        ];
        let service =
            Service::new(admin, bindings.clone(), ServiceClock::new(0, 3600).unwrap()).unwrap();
        let owner = ConnectionIdentity::verified(&service, &bindings[0].fingerprint).unwrap();
        let other = ConnectionIdentity::verified(&service, &bindings[1].fingerprint).unwrap();
        let clock = Arc::new(TestClock(Mutex::new(Instant::now())));
        let registry = Registry::with_sources(
            service.clone(),
            ApiLimits { sessions: slots },
            clock.clone(),
            Arc::new(RandomTokens),
        );
        Self {
            registry,
            service,
            owner,
            other,
            clock,
            bindings,
        }
    }
    fn lease(&self, token: &str) -> SessionLease {
        self.registry
            .lease(token, &self.owner, Cancellation::default())
            .unwrap()
    }
}
fn sql(text: &str) -> Request {
    Request::Sql {
        sql: text.into(),
        params: vec![],
    }
}

#[test]
fn open_capacity_collision_and_entropy_failure_release_slots() {
    let f = Fixture::new(2);
    let tokens = Arc::new(TestTokens(Mutex::new(VecDeque::from([
        Ok([1; 32]),
        Ok([1; 32]),
        Ok([1; 32]),
        Ok([1; 32]),
        Err(ApiError::Internal),
        Ok([2; 32]),
    ]))));
    let registry = Registry::with_sources(
        f.service.clone(),
        ApiLimits { sessions: 2 },
        f.clock.clone(),
        tokens,
    );
    let one = registry.open(&f.owner).unwrap();
    assert!(matches!(registry.open(&f.owner), Err(ApiError::Internal)));
    assert_eq!(registry.slots.available_permits(), 1);
    assert!(matches!(registry.open(&f.owner), Err(ApiError::Internal)));
    let two = registry.open(&f.owner).unwrap();
    assert!(matches!(
        registry.open(&f.owner),
        Err(ApiError::ResourceExhausted)
    ));
    registry.close(&one, &f.owner).unwrap();
    registry.close(&two, &f.owner).unwrap();
    assert_eq!(registry.slots.available_permits(), 2);
}

#[test]
fn close_during_lease_cancels_and_completion_cannot_resurrect_handle() {
    let f = Fixture::new(1);
    let token = f.registry.open(&f.owner).unwrap();
    let context = Cancellation::default();
    let mut lease = f.registry.lease(&token, &f.owner, context.clone()).unwrap();
    assert!(matches!(
        f.registry.lease(&token, &f.owner, Cancellation::default()),
        Err(ApiError::SessionBusy)
    ));
    assert_eq!(lease.session().handle(sql("BEGIN")).code, "ok");
    f.registry.close(&token, &f.owner).unwrap();
    assert!(context.check().is_err());
    assert_eq!(f.registry.slots.available_permits(), 0);
    lease.completed();
    drop(lease);
    assert_eq!(f.registry.slots.available_permits(), 1);
    assert!(matches!(
        f.registry.close(&token, &f.owner),
        Err(ApiError::SessionNotFound)
    ));
    assert!(f.registry.entries.lock().unwrap().is_empty());
}

#[test]
fn owner_mismatch_is_hidden_and_removed_readded_epoch_never_revives() {
    let f = Fixture::new(2);
    let token = f.registry.open(&f.owner).unwrap();
    assert!(matches!(
        f.registry.lease(&token, &f.other, Cancellation::default()),
        Err(ApiError::SessionNotFound)
    ));
    assert!(matches!(
        f.registry.close(&token, &f.other),
        Err(ApiError::SessionNotFound)
    ));
    f.service
        .reload_bindings(vec![f.bindings[1].clone()])
        .unwrap();
    f.registry.sweep();
    f.service.reload_bindings(f.bindings.clone()).unwrap();
    assert!(matches!(
        f.registry.open(&f.owner),
        Err(ApiError::Unauthenticated)
    ));
    let replacement = ConnectionIdentity::verified(&f.service, &f.bindings[0].fingerprint).unwrap();
    assert!(matches!(
        f.registry
            .lease(&token, &replacement, Cancellation::default()),
        Err(ApiError::SessionNotFound)
    ));
    assert!(f.registry.open(&replacement).is_ok());
}

#[test]
fn idle_timeout_exact_boundary_and_invalid_requests_do_not_refresh() {
    let f = Fixture::new(1);
    let token = f.registry.open(&f.owner).unwrap();
    f.clock.advance(IDLE_TIMEOUT - Duration::from_nanos(1));
    drop(f.lease(&token)); // Abandoned, undispatched call is not a refresh.
    f.clock.advance(Duration::from_nanos(1));
    assert!(matches!(
        f.registry.lease(&token, &f.owner, Cancellation::default()),
        Err(ApiError::SessionNotFound)
    ));
    assert_eq!(f.registry.slots.available_permits(), 1);
}

#[test]
fn transaction_age_survives_ping_and_aborted_state() {
    let f = Fixture::new(1);
    let token = f.registry.open(&f.owner).unwrap();
    let mut begin = f.lease(&token);
    assert_eq!(begin.session().handle(sql("BEGIN")).code, "ok");
    begin.completed();
    drop(begin);
    f.clock.advance(Duration::from_secs(30));
    let mut ping = f.lease(&token);
    assert_eq!(ping.session().handle(Request::Ping).code, "ok");
    ping.completed();
    drop(ping);
    let mut bad = f.lease(&token);
    assert_ne!(
        bad.session().handle(sql("SELECT missing FROM app")).code,
        "ok"
    );
    assert!(bad.session().is_aborted());
    bad.completed();
    drop(bad);
    f.clock.advance(Duration::from_secs(30));
    assert!(matches!(
        f.registry.lease(&token, &f.owner, Cancellation::default()),
        Err(ApiError::SessionNotFound)
    ));
    assert_eq!(f.registry.slots.available_permits(), 1);
}

#[test]
fn absolute_age_survives_successful_keepalives() {
    let f = Fixture::new(1);
    let token = f.registry.open(&f.owner).unwrap();
    for _ in 0..61 {
        f.clock.advance(Duration::from_secs(59));
        let mut lease = f.lease(&token);
        assert_eq!(lease.session().handle(Request::Ping).code, "ok");
        lease.completed();
    }
    f.clock.advance(Duration::from_secs(1));
    assert!(matches!(
        f.registry.lease(&token, &f.owner, Cancellation::default()),
        Err(ApiError::SessionNotFound)
    ));
}

#[test]
fn idle_session_releases_socket_and_shutdown_drops_staged_transaction() {
    let f = Fixture::new(1);
    let token = f.registry.open(&f.owner).unwrap();
    let socket = f.service.admission.connect().unwrap();
    let weak = Arc::downgrade(&socket);
    let mut lease = f.lease(&token);
    lease.session().connection = Some(socket);
    assert_eq!(lease.session().handle(sql("BEGIN")).code, "ok");
    assert_eq!(
        lease
            .session()
            .handle(sql("INSERT INTO app (id) VALUES (7)"))
            .code,
        "ok"
    );
    lease.completed();
    drop(lease);
    assert!(weak.upgrade().is_none());
    f.registry.stop();
    assert_eq!(f.registry.slots.available_permits(), 1);
    assert!(
        f.service
            .admin
            .sql_session()
            .execute("SELECT id FROM app WHERE id=7", &[], None)
            .unwrap()
            .rows
            .is_empty()
    );
    assert!(matches!(
        f.registry.open(&f.owner),
        Err(ApiError::Unavailable)
    ));
}

#[test]
fn panic_in_lease_invalidates_instead_of_restoring() {
    let f = Fixture::new(1);
    let token = f.registry.open(&f.owner).unwrap();
    let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _lease = f.lease(&token);
        panic!("test lease unwind");
    }));
    assert!(caught.is_err());
    assert!(matches!(
        f.registry.close(&token, &f.owner),
        Err(ApiError::SessionNotFound)
    ));
    assert_eq!(f.registry.slots.available_permits(), 1);
}

#[test]
fn sweep_reclaims_expired_entries_and_never_leaves_tombstones() {
    let f = Fixture::new(1);
    let expired = f.registry.open(&f.owner).unwrap();
    f.clock.advance(IDLE_TIMEOUT);
    f.registry.sweep();
    assert_eq!(f.registry.slots.available_permits(), 1);
    assert!(f.registry.entries.lock().unwrap().is_empty());
    let next = f.registry.open(&f.owner).unwrap();
    assert_ne!(next, expired);
    assert!(matches!(
        f.registry.close(&expired, &f.owner),
        Err(ApiError::SessionNotFound)
    ));
    assert_eq!(f.registry.entries.lock().unwrap().len(), 1);
}
