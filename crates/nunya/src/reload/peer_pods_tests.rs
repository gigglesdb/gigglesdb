use super::*;
use crate::{
    policy::{kube_leader::LeaderState, serving::ServingGate},
    state::{
        authority::{Authority, SessionLimits},
        peer::{PeerAuth, PeerClient, PeerServer, PeerStateCache},
        runtime::Registry,
    },
    token::{
        StandardCodec, TokenCodec,
        keys::{RootKey, derive_scope_v2},
    },
    value::{ColumnType, SqlValue},
};

pub(crate) fn tokens_equal(left: &Policy, right: &Policy, columns: &[&str]) {
    for column in columns {
        let field = customer_column(column);
        let encode = |policy: &Policy| {
            let keys = derive_scope_v2(
                &RootKey::from_hex(&"01".repeat(32)).unwrap(),
                &[9; 32],
                &[4; 16],
                "postgres",
            )
            .unwrap();
            StandardCodec
                .encode(
                    &keys,
                    policy.domain_of(&field).unwrap(),
                    &ColumnType::Text,
                    None,
                    &SqlValue::Text("synthetic-sensitive-value".into()),
                )
                .unwrap()
        };
        // Do not print token or plaintext values on assertion failure.
        assert!(encode(left) == encode(right), "pod token bytes differ");
    }
    assert!(left.retired_domains == right.retired_domains);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_pods_token_bytes_membership_and_reset_feed() {
    use crate::pgwire::auth::ScramVerifier;
    use crate::state::{
        authority::AcquireRequest,
        model::*,
        peer::{ChangeKind, ChangesRequest},
    };
    let files = Files::new();
    let follower_files = Files::new();
    let leader = tokenize_hub(&files, &["postgres"]);
    let follower = tokenize_hub(&follower_files, &["postgres"]);
    let store = binding_store(&files);
    let local = binding_store(&follower_files);
    leader.set_store(store.clone()).unwrap();
    follower.set_store(local.clone()).unwrap();
    leader.set_peer_bindings(Arc::new(|| true), Arc::new(|| None));
    let authority = Authority::load(store.clone(), SessionLimits::default()).unwrap();
    leader.set_authority(Arc::downgrade(&authority)).unwrap();
    let principal = authority
        .create_principal("test-agent", PrincipalKind::Agent, false, "test")
        .unwrap();
    authority
        .set_grants(principal.id, vec![(id("postgres"), Mode::ReadWrite)])
        .unwrap();
    let gate = Arc::new(ServingGate::default());
    gate.extend_to(u64::MAX);
    let server = PeerServer::new(
        authority.clone(),
        LeaderState::live_for_test(),
        Arc::new(PeerAuth::new(&[7; 32]).unwrap()),
        Default::default(),
    )
    .with_bindings_store(store.clone())
    .unwrap()
    .with_serving_gate(gate.clone())
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = Arc::new(PeerClient::with_url(
        format!("http://{}", listener.local_addr().unwrap()),
        "follower".into(),
        Arc::new(PeerAuth::new(&[7; 32]).unwrap()),
    ));
    let task = tokio::spawn(server.clone().serve(listener));
    let cursor = client
        .changes(
            ChangesRequest {
                since: 0,
                touched: vec![],
            },
            Duration::from_secs(5),
        )
        .await
        .unwrap()
        .next;
    let clock = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let c = clock.clone();
    let cache = Arc::new(PeerStateCache::new(
        client.clone(),
        gate.clone(),
        Registry::default(),
        Duration::from_secs(30),
        16,
    ).with_clocks(Arc::new(move || c.load(std::sync::atomic::Ordering::SeqCst)), Arc::new(|| 0)));
    let expiring_cache = Arc::new(PeerStateCache::new(
        client.clone(),
        gate.clone(),
        Registry::default(),
        Duration::from_millis(250),
        16,
    ));
    let source = cache.clone();
    follower.set_peer_bindings(
        Arc::new(|| false),
        Arc::new(move || source.bindings_snapshot()),
    );
    let weak = Arc::downgrade(&follower);
    cache.set_bindings_listener(Arc::new(move || { if let Some(hub) = weak.upgrade() { hub.recompile_bindings(); } }));
    let worker_authority = authority.clone();
    let sessions = tokio::task::spawn_blocking(move || {
        let route = id("postgres");
        let initial = leader.state.lock().unwrap().units[0].doc.as_ref().clone();
        let stamp = |n| Some(format!("rev-{n}-test"));
        leader.state.lock().unwrap().units[0].revisions.write().unwrap().policy = "ab".repeat(32);
        let leader_fixture = crate::pgwire::session::tests::Fixture::new();
        let follower_fixture = crate::pgwire::session::tests::Fixture::new();
        let lctx = leader_fixture.ctx.clone();
        let fctx = follower_fixture.ctx.clone();
        *lctx.snapshot.write().unwrap() = Arc::new(customers(&[("email", 1)]));
        *fctx.snapshot.write().unwrap() = Arc::new(customers(&[("email", 1)]));
        leader.register(Route::Postgres(lctx.clone())).unwrap();
        follower.state.lock().unwrap().units[0].revisions.write().unwrap().policy = "ab".repeat(32);
        let before = bindings::snapshot(local.as_ref()).unwrap();
        follower.register(Route::Postgres(fctx.clone())).unwrap();
        assert!(!follower.peer_bindings_ready());
        cache.prime_bindings();
        assert!(follower.peer_bindings_ready());
        let doc = follower.state.lock().unwrap().units[0].doc.as_ref().clone();
        assert!(follower.compile(&doc, &route, &customers(&[("email", 1)]), Some("other-digest"), true).unwrap_err().membership);
        // The feed can stay live while the independent binding snapshot expires.
        clock.store(30_000_000_000, std::sync::atomic::Ordering::SeqCst);
        assert!(!follower.peer_bindings_ready());
        follower.recompile_bindings();
        assert!(follower.state.lock().unwrap().units[0].routes[0].membership_failed);
        // Same version => snapshot: None. Recovery must still invoke the listener.
        cache.sync().unwrap();
        assert!(follower.peer_bindings_ready());
        assert!(!follower.state.lock().unwrap().units[0].routes[0].membership_failed);
        let unchanged = || assert_eq!(bindings::snapshot(local.as_ref()).unwrap(), before);
        let policies = || (lctx.policy.read().unwrap().clone(), fctx.policy.read().unwrap().clone());
        let (l, f) = policies(); tokens_equal(&l, &f, &["email"]); unchanged();
        let expanded = customers(&[("email", 1), ("other", 2)]);
        let refresh = |hub: &Hub| hub.refresh(&Route::Postgres(if hub.is_follower() { fctx.clone() } else { lctx.clone() }), expanded.clone());
        refresh(&follower).unwrap();
        let withheld = fctx.policy.read().unwrap().clone();
        assert!(withheld.domain_of(&customer_column("other")).is_none());
        assert_eq!(withheld.treatment(&customer_column("other")), Some(&Treatment::Masked { placeholder: None }));
        tokens_equal(&l, &withheld, &["email"]); unchanged();
        refresh(&leader).unwrap(); cache.sync().unwrap(); refresh(&follower).unwrap();
        let (l, f) = policies(); tokens_equal(&l, &f, &["email", "other"]); unchanged();
    let sessions: Vec<_> = (0..2).map(|n| worker_authority.acquire_session(principal.id, AcquireRequest {
        route: id("postgres"), mode: None, lifetime_ms: None, isolated: false, label: None,
        idempotency_key: format!("session-{n}"),
        verifier: Verifier(ScramVerifier::from_password("test-password", b"salt", 1)),
        secret_sha256: [0; 32],
    }).unwrap().id).collect();
        let linked = parse(&files.a, b"version=2\ndefault='tokenize'\n[[link]]\nfields=['public.customers.email','public.customers.other']\n", &leader.service).unwrap();
        for (revision, doc, linking) in [(2, linked, true), (3, initial, false)] {
            // An old publication cannot authorize a newer policy revision.
            assert!(follower.compile(&doc, &route, &expanded, Some(&format!("rev-{}-test", revision)), true).unwrap_err().membership);
            assert!(follower.apply(0, Ok(doc.clone()), stamp(revision)).is_err());
            assert!(!follower.peer_bindings_ready()); unchanged();
            let old = lctx.policy.read().unwrap().clone();
            let (candidate, change) = leader.compile(&doc, &route, &expanded, Some(&format!("rev-{}-test", revision)), false).unwrap();
            assert!(change.as_ref().unwrap().requires_reset());
            let published = bindings::snapshot(store.as_ref()).unwrap();
            assert!(published.routes["postgres"].reset_owed);
            cache.sync().unwrap();
            assert!(follower.compile(&doc, &route, &expanded, Some(&format!("rev-{}-test", revision)), true).unwrap_err().membership);
            assert!(follower.apply(0, Ok(doc.clone()), stamp(revision)).is_err());
            assert!(!follower.peer_bindings_ready()); unchanged();
            if linking {
                for column in ["email", "other"] {
                    assert!(candidate.retired_domains.contains(old.domain_of(&customer_column(column)).unwrap()));
                }
            } else {
                let shared = old.domain_of(&customer_column("email")).unwrap();
                assert!(candidate.retired_domains.contains(shared));
            }
            if linking {
                for session in &sessions {
                    assert_eq!(worker_authority.session(*session).unwrap().state, SessionState::Active);
                }
            }
            leader.reset_membership(change.as_ref(), &old, false).unwrap();
            for session in &sessions {
                assert_eq!(worker_authority.session(*session).unwrap().revoke_reason, Some(EndReason::PolicyReset));
            }
            leader.apply(0, Ok(doc.clone()), stamp(revision)).unwrap();
            assert!(!bindings::snapshot(store.as_ref()).unwrap().routes["postgres"].reset_owed);
            cache.sync().unwrap();
            follower.apply(0, Ok(doc), stamp(revision)).unwrap();
            assert!(follower.peer_bindings_ready());
            let (l, f) = policies(); tokens_equal(&l, &f, &["email", "other"]);
            for p in [&l, &f] {
                assert_eq!(p.domain_of(&customer_column("email")) == p.domain_of(&customer_column("other")), linking);
            }
            unchanged();
        }
        for session in &sessions {
            let row = worker_authority.session(*session).unwrap();
            assert_eq!(row.state, SessionState::Revoked);
            assert_eq!(row.revoke_reason, Some(EndReason::PolicyReset));
        }
        expiring_cache.sync().unwrap();
        std::thread::sleep(Duration::from_millis(251));
        assert!(expiring_cache.bindings_snapshot().is_none());
        follower.set_peer_bindings(Arc::new(|| false), Arc::new(move || expiring_cache.bindings_snapshot()));
        let doc = follower.state.lock().unwrap().units[0].doc.as_ref().clone();
        assert!(follower.compile(&doc, &route, &expanded, Some(&format!("rev-{}-test", 3)), true).unwrap_err().membership);
        assert!(follower.apply(0, Ok(doc), stamp(3)).is_err());
        assert!(!follower.peer_bindings_ready()); unchanged();
        sessions
    }).await.unwrap();
    let changes = client
        .changes(
            ChangesRequest {
                since: cursor,
                touched: vec![],
            },
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    for session in sessions {
        assert!(
            changes
                .changes
                .iter()
                .any(|change| change.session_id == session
                    && matches!(
                        change.kind,
                        ChangeKind::End {
                            reason: EndReason::PolicyReset
                        }
                    ))
        );
    }
    task.abort();
    let _ = task.await;
    drop(client);
    drop(server);
    drop(authority);
}

#[test]
fn role_flip_helpers_fail_closed_and_preserve_unperformed_resets() {
    use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
    let files = Files::new();
    let hub = tokenize_hub(&files, &["a"]);
    let store = binding_store(&files);
    hub.set_store(store.clone()).unwrap();
    let flag = Arc::new(AtomicBool::new(true));
    let f = flag.clone();
    hub.set_peer_bindings(Arc::new(move || f.load(SeqCst)), Arc::new(|| None));
    let catalog = customers(&[("email", 1), ("other", 2)]);
    hub.register(test_route("a", catalog.clone())).unwrap();
    let linked = parse(&files.a, b"version=2\ndefault='tokenize'\n[[link]]\nfields=['public.customers.email','public.customers.other']\n", &hub.service).unwrap();
    let (policy, change) = hub.compile(&linked, &id("a"), &catalog, Some("digest"), false).unwrap();
    assert!(change.as_ref().unwrap().requires_reset());
    flag.store(false, SeqCst);
    assert!(hub.reset_membership(change.as_ref(), &policy, true).is_err());
    assert!(bindings::load_reset(store.as_ref(), &id("a")).unwrap().is_some());
    // A follower operation retains its role even if the source changes afterwards.
    let follower = hub.is_follower();
    flag.store(true, SeqCst);
    let before = bindings::snapshot(store.as_ref()).unwrap();
    hub.membership_published("a", follower);
    hub.record_bindings_revision("a", "must-not-publish", follower);
    assert_eq!(bindings::snapshot(store.as_ref()).unwrap(), before);
    // A leader operation sampled before a flip still performs its required reset.
    let count = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let n = count.clone();
    *hub.reset_callback.write().unwrap() = Some(Arc::new(move |_, _| { n.fetch_add(1, SeqCst); Ok(0) }));
    let follower = hub.is_follower();
    flag.store(false, SeqCst);
    hub.reset_membership(change.as_ref(), &policy, follower).unwrap();
    assert_eq!(count.load(SeqCst), 1);
}

#[test]
fn publication_samples_role_once() {
    use std::sync::atomic::{AtomicU64, Ordering::SeqCst};
    let files = Files::new();
    let hub = tokenize_hub(&files, &["a"]);
    hub.set_store(binding_store(&files)).unwrap();
    let calls = Arc::new(AtomicU64::new(0));
    let n = calls.clone();
    hub.set_peer_bindings(Arc::new(move || n.fetch_add(1, SeqCst) == 0), Arc::new(|| None));
    hub.register(test_route("a", customers(&[("email", 1)]))).unwrap();
    assert_eq!(calls.load(SeqCst), 1);
}

#[test]
fn stamp_record_failure_is_nonfatal_and_retried() {
    let files = Files::new();
    let hub = hub();
    hub.set_peer_bindings(Arc::new(|| true), Arc::new(|| None));
    hub.register(test_route("postgres", customers(&[("email", 1)]))).unwrap();
    assert!(!hub.state.lock().unwrap().units[0].routes[0].membership_failed);
    let store = binding_store(&files);
    *hub.store.write().unwrap() = Some(store.clone());
    hub.recompile_bindings();
    let expected = hub.state.lock().unwrap().units[0].revisions.read().unwrap().policy.clone();
    assert_eq!(bindings::snapshot(store.as_ref()).unwrap().routes["postgres"].stamp, expected);
}

/// Hub fixture sharing production bindings compilation with the failover tests.
pub(crate) struct FailoverHub {
    fixture: crate::pgwire::session::tests::Fixture,
    hub: Arc<Hub>,
    _files: Files,
}
impl FailoverHub {
    /// Wire a leader or follower Hub to its closed-capable store and peer snapshot.
    pub(crate) fn new(store: Arc<dyn crate::state::store::Store>, ready: Arc<dyn Fn() -> bool + Send + Sync>, cache: Option<Arc<PeerStateCache>>) -> Self {
        let files = Files::new();
        let hub = tokenize_hub(&files, &["postgres"]);
        hub.set_store(store).unwrap();
        let source = cache.clone();
        hub.set_peer_bindings(ready, Arc::new(move || source.as_ref().and_then(|c| c.bindings_snapshot())));
        let fixture = crate::pgwire::session::tests::Fixture::new();
        *fixture.ctx.snapshot.write().unwrap() = Arc::new(customers(&[("email", 1)]));
        hub.register(Route::Postgres(fixture.ctx.clone())).unwrap();
        if let Some(cache) = cache {
            let weak = Arc::downgrade(&hub);
            cache.set_bindings_listener(Arc::new(move || { if let Some(h) = weak.upgrade() { h.recompile_bindings(); } }));
        }
        Self { fixture, hub, _files: files }
    }
    /// Recompile using the current role and return whether membership is usable.
    pub(crate) fn recompile(&self) -> bool {
        self.hub.recompile_bindings();
        !self.hub.state.lock().unwrap().units[0].routes[0].membership_failed
    }
    /// Read the bound policy without exposing values in assertions.
    pub(crate) fn policy(&self) -> Arc<Policy> { self.fixture.ctx.policy.read().unwrap().clone() }
}
