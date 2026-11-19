//! Shared-directory failover proofs using independent pod runtimes.
use super::{
    authority::{AcquireRequest, Operation, TokenizeLimits},
    leader_store::{LeaderStore, LeadershipSource, StateHolder},
    model::{AccessSession, Mode, PrincipalKind, RouteId, Verifier},
    peer::{Lookup, PeerAuth, PeerClient, PeerError, PeerServer, PeerStateCache},
    runtime::{Registry, StateRuntime},
    schema::Table,
    serving_state::ServingState,
    store::{RedbStore, Store, StoreError},
};
use crate::{
    config::SessionSettings,
    pgwire::auth::ScramVerifier,
    policy::{DomainName, bindings, kube_leader::LeaderState, serving::ServingGate},
    reload::tests::peer_pods::{FailoverHub, tokens_equal},
    token::{SessionId, keys::RootKey},
};
use std::{
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
const TERM: Duration = Duration::from_secs(6);
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = PathBuf::from(std::env::var_os("TEMP").unwrap())
            .join(format!("failover-{}", SessionId::random().to_hex()));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        assert!(std::fs::remove_dir_all(&self.0).is_ok());
    }
}
fn root() -> RootKey {
    RootKey::from_hex(&"01".repeat(32)).unwrap()
}
fn limits() -> TokenizeLimits {
    TokenizeLimits {
        per_session: 1000,
        per_principal: 1000,
        principal_window_ms: 100_000,
    }
}
fn domain() -> DomainName {
    DomainName("email".into())
}
fn request(key: &str) -> AcquireRequest {
    AcquireRequest {
        route: RouteId::new("postgres").unwrap(),
        mode: None,
        lifetime_ms: None,
        isolated: false,
        label: None,
        idempotency_key: key.into(),
        verifier: Verifier(ScramVerifier::from_password("synthetic", b"salt", 1)),
        secret_sha256: [0; 32],
    }
}
struct Pod {
    cell: Arc<Mutex<Option<u64>>>,
    proxy: Arc<LeaderStore>,
    holder: Arc<StateHolder>,
    runtime: Arc<StateRuntime>,
    leader: LeaderState,
    owned: Arc<AtomicU64>,
}
impl Pod {
    fn new(path: &Path) -> Self {
        let cell = Arc::new(Mutex::new(None));
        let source: LeadershipSource = {
            let c = cell.clone();
            Arc::new(move || *c.lock().unwrap())
        };
        let proxy = Arc::new(LeaderStore::new(source.clone(), Arc::new(|| 1000)));
        let path = path.to_owned();
        let owned = Arc::new(AtomicU64::new(0));
        let attempts = owned.clone();
        let holder = Arc::new(StateHolder::new(
            proxy.clone(),
            source,
            Arc::new(move || {
                let result = RedbStore::open(&path, &root());
                if matches!(result, Err(StoreError::Owned)) {
                    attempts.fetch_add(1, Ordering::SeqCst);
                }
                result
            }),
        ));
        let runtime =
            StateRuntime::open_deferred(proxy.clone(), holder.clone(), &SessionSettings::default());
        let authority = runtime.authority.clone();
        holder.on_ready(Arc::new(move || {
            authority.reload().map_err(|_| StoreError::Io)?;
            authority
                .sync_config_principals(&[])
                .map_err(|_| StoreError::Io)?;
            Ok(())
        }));
        let authority = runtime.authority.clone();
        holder.on_closed(Arc::new(move || authority.clear()));
        Self {
            cell,
            proxy,
            holder,
            runtime,
            leader: LeaderState::live_for_test(),
            owned,
        }
    }
    fn term(&self, term: Option<u64>) {
        *self.cell.lock().unwrap() = term;
        if let Some(term) = term {
            self.leader.term_for_test(term);
        }
    }
    fn acquire(&self) -> AccessSession {
        let a = &self.runtime.authority;
        let p = a
            .create_principal("agent", PrincipalKind::Agent, false, "principal")
            .unwrap();
        a.set_grants(
            p.id,
            vec![(RouteId::new("postgres").unwrap(), Mode::ReadWrite)],
        )
        .unwrap();
        a.acquire_session(p.id, request("session")).unwrap()
    }
    async fn endpoint(&self) -> Endpoint {
        let gate = Arc::new(ServingGate::default());
        gate.extend_to(u64::MAX);
        let holder = self.holder.clone();
        let server = PeerServer::new(
            self.runtime.authority.clone(),
            self.leader.clone(),
            Arc::new(PeerAuth::new(&[7; 32]).unwrap()),
            Default::default(),
        )
        .with_ready(Arc::new(move || holder.is_ready()))
        .unwrap()
        .with_bindings_store(self.runtime.store())
        .unwrap()
        .with_serving_gate(gate)
        .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = Arc::new(PeerClient::with_url(
            format!("http://{}", listener.local_addr().unwrap()),
            "follower".into(),
            Arc::new(PeerAuth::new(&[7; 32]).unwrap()),
        ));
        Endpoint {
            client,
            task: tokio::spawn(server.serve(listener)),
        }
    }
}
impl Drop for Pod {
    fn drop(&mut self) {
        self.holder.close();
    }
}
struct Endpoint {
    client: Arc<PeerClient>,
    task: tokio::task::JoinHandle<Result<(), PeerError>>,
}
impl Endpoint {
    async fn stop(self) {
        self.task.abort();
        let _ = self.task.await;
    }
}
fn cache(client: Arc<PeerClient>, clock: Arc<AtomicU64>) -> Arc<PeerStateCache> {
    let gate = Arc::new(ServingGate::default());
    gate.extend_to(u64::MAX);
    Arc::new(
        PeerStateCache::new(client, gate, Registry::default(), TERM, 16).with_clocks(
            Arc::new(move || clock.load(Ordering::SeqCst)),
            Arc::new(|| 1000),
        ),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn handover_preserves_sessions_scopes_counters_bindings_and_tokens() {
    let dir = Directory::new();
    let a = Pod::new(&dir.0);
    let b = Pod::new(&dir.0);
    let c = Pod::new(&dir.0);
    a.term(Some(1));
    a.holder.tick();
    assert!(a.holder.is_ready() == Some(1));
    let session = a.acquire();
    let scope = a.runtime.authority.scope(session.scope).unwrap();
    let (reserved, _, _) = a
        .runtime
        .authority
        .peer_reserve(session.id, &domain(), &limits(), 16)
        .unwrap();
    let reserved_end = a
        .runtime
        .authority
        .session_tokenizations(session.id, &domain())
        .unwrap();
    assert!(reserved == 16 && reserved_end == 16);
    let ahub = FailoverHub::new(a.runtime.store(), Arc::new(|| true), None);
    let snapshot = bindings::snapshot(a.runtime.store().as_ref()).unwrap();
    assert!(!snapshot.routes.is_empty());
    let policy = ahub.policy();
    a.term(None);
    a.holder.tick();
    assert!(a.holder.is_ready().is_none());
    assert!(a.runtime.authority.session(session.id).is_none());
    b.term(Some(2));
    b.holder.tick();
    assert!(b.holder.is_ready() == Some(2));
    let found = b.runtime.authority.session(session.id).unwrap();
    assert!(found.id == session.id && found.scope == session.scope);
    assert!(
        b.runtime
            .authority
            .admit(session.id, Operation::Read)
            .is_ok()
    );
    let restored = b.runtime.authority.scope(session.scope).unwrap();
    assert!(serde_json::to_vec(&scope).unwrap() == serde_json::to_vec(&restored).unwrap());
    let start = b
        .runtime
        .authority
        .session_tokenizations(session.id, &domain())
        .unwrap();
    assert!(start >= reserved_end);
    let (next, _, _) = b
        .runtime
        .authority
        .peer_reserve(session.id, &domain(), &limits(), 8)
        .unwrap();
    assert!(next == 8);
    assert!(
        b.runtime
            .authority
            .session_tokenizations(session.id, &domain())
            .unwrap()
            == start + next
    );
    let restored = bindings::snapshot(b.runtime.store().as_ref()).unwrap();
    assert!(restored.version >= snapshot.version && restored.routes == snapshot.routes);
    let bhub = FailoverHub::new(b.runtime.store(), Arc::new(|| true), None);
    tokens_equal(&policy, &bhub.policy(), &["email"]);
    let endpoint = b.endpoint().await;
    let follower = cache(endpoint.client.clone(), Arc::new(AtomicU64::new(1)));
    let cc = follower.clone();
    let sid = session.id;
    tokio::task::spawn_blocking(move || {
        cc.sync().unwrap();
        assert!(cc.admit(sid, Operation::Read).is_ok());
    })
    .await
    .unwrap();
    let chub = FailoverHub::new(
        c.runtime.store(),
        Arc::new(|| false),
        Some(follower.clone()),
    );
    assert!(chub.recompile());
    tokens_equal(&policy, &chub.policy(), &["email"]);
    assert!(c.proxy.read().is_err() && c.proxy.write().is_err());
    drop(chub);
    drop(follower);
    endpoint.stop().await;
    drop(bhub);
    drop(ahub);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_owner_excludes_challenger_and_deposed_writes_abort() {
    let dir = Directory::new();
    let a = Pod::new(&dir.0);
    let b = Pod::new(&dir.0);
    let _c = Pod::new(&dir.0);
    a.term(Some(1));
    a.holder.tick();
    let session = a.acquire();
    b.term(Some(2));
    b.holder.tick();
    assert!(b.owned.load(Ordering::SeqCst) == 1);
    assert!(b.holder.is_ready().is_none());
    assert!(b.proxy.read().is_err() && b.proxy.write().is_err());
    let be = b.endpoint().await;
    assert!(matches!(
        be.client
            .session_lookup(
                Lookup {
                    session_id: session.id
                },
                TERM
            )
            .await,
        Err(PeerError::NotLeader)
    ));
    let store = a.runtime.store();
    let mut tx = store.write().unwrap();
    tx.put(Table::Sessions, b"aborted", b"opaque").unwrap();
    a.term(None);
    assert!(tx.commit().is_err());
    b.holder.tick();
    assert!(b.owned.load(Ordering::SeqCst) == 2);
    a.holder.tick();
    b.holder.tick();
    assert!(b.holder.is_ready() == Some(2));
    assert!(
        b.proxy
            .read()
            .unwrap()
            .get(Table::Sessions, b"aborted")
            .unwrap()
            .is_none()
    );
    assert!(
        a.runtime
            .authority
            .acquire_session(session.principal, request("deposed"))
            .is_err()
    );
    assert!(
        a.runtime
            .authority
            .peer_reserve(session.id, &domain(), &limits(), 1)
            .is_err()
    );
    assert!(
        bindings::record_route_stamp(
            a.runtime.store().as_ref(),
            &RouteId::new("postgres").unwrap(),
            "deposed"
        )
        .is_err()
    );
    let ae = a.endpoint().await;
    assert!(matches!(
        ae.client
            .session_lookup(
                Lookup {
                    session_id: session.id
                },
                TERM
            )
            .await,
        Err(PeerError::NotLeader)
    ));
    ae.stop().await;
    be.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn follower_outage_lease_boundary_and_idempotent_shutdown() {
    let dir = Directory::new();
    let a = Pod::new(&dir.0);
    let b = Pod::new(&dir.0);
    let c = Pod::new(&dir.0);
    a.term(Some(1));
    a.holder.tick();
    let session = a.acquire();
    let ahub = FailoverHub::new(a.runtime.store(), Arc::new(|| true), None);
    let endpoint = a.endpoint().await;
    let clock = Arc::new(AtomicU64::new(1));
    let follower = cache(endpoint.client.clone(), clock.clone());
    let f = follower.clone();
    let sid = session.id;
    tokio::task::spawn_blocking(move || {
        f.sync().unwrap();
        assert!(f.admit(sid, Operation::Read).is_ok());
    })
    .await
    .unwrap();
    let chub = FailoverHub::new(
        c.runtime.store(),
        Arc::new(|| false),
        Some(follower.clone()),
    );
    assert!(chub.recompile());
    a.term(None);
    a.holder.tick();
    assert!([&a, &b, &c].iter().all(|p| p.holder.is_ready().is_none()));
    let f = follower.clone();
    tokio::task::spawn_blocking(move || {
        assert!(f.sync().is_err());
        assert!(f.admit(SessionId::random(), Operation::Read).is_err());
        clock.store(6_000_000_000, Ordering::SeqCst);
        assert!(f.admit(sid, Operation::Read).is_ok());
        clock.store(6_000_000_001, Ordering::SeqCst);
        assert!(f.admit(sid, Operation::Read).is_err());
        assert!(f.bindings_snapshot().is_none());
    })
    .await
    .unwrap();
    assert!(
        c.runtime
            .authority
            .acquire_session(session.principal, request("follower"))
            .is_err()
    );
    assert!(c.proxy.write().is_err());
    assert!(
        bindings::record_route_stamp(
            c.runtime.store().as_ref(),
            &RouteId::new("postgres").unwrap(),
            "follower"
        )
        .is_err()
    );
    assert!(!chub.recompile());
    a.holder.close();
    a.holder.close();
    b.term(Some(2));
    b.holder.tick();
    assert!(b.holder.is_ready() == Some(2));
    b.holder.close();
    b.holder.close();
    c.term(Some(3));
    c.holder.tick();
    assert!(c.holder.is_ready() == Some(3));
    drop(chub);
    drop(ahub);
    drop(follower);
    endpoint.stop().await;
}
