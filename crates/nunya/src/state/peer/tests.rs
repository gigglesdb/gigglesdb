use super::*;
use crate::{
    pgwire::auth::ScramVerifier,
    policy::kube_leader::LeaderState,
    state::{
        authority::{AcquireRequest, Authority, SessionLimits, TokenizeLimits},
        model::*,
        store::RedbStore,
    },
    token::{SessionId, keys::RootKey},
};
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
const TIMEOUT: Duration = Duration::from_secs(5);
struct Fixture {
    store: Arc<RedbStore>,
    a: Arc<Authority>,
    clock: Arc<AtomicU64>,
    dir: PathBuf,
    server: Arc<PeerServer>,
    leader: LeaderState,
    gate: Arc<crate::policy::serving::ServingGate>,
    task: tokio::task::JoinHandle<Result<(), PeerError>>,
    url: String,
    session: AccessSession,
}
impl Fixture {
    async fn new(leader: bool, limits: TokenizeLimits) -> Self {
        Self::with_session_limits(leader, limits, SessionLimits::default()).await
    }
    async fn with_session_limits(leader: bool, limits: TokenizeLimits, session_limits: SessionLimits) -> Self {
        let dir = PathBuf::from(std::env::var_os("TEMP").unwrap())
            .join(format!("peer-{}", SessionId::random().to_hex()));
        std::fs::create_dir_all(&dir).unwrap();
        let root = RootKey::from_hex(&"01".repeat(32)).unwrap();
        let clock = Arc::new(AtomicU64::new(1000));
        let c = clock.clone();
        let store = Arc::new(
            RedbStore::with_clock(&dir, &root, Arc::new(move || c.load(Ordering::SeqCst))).unwrap(),
        );
        let a = Authority::load(store.clone(), session_limits).unwrap();
        let p = a
            .create_principal("agent", PrincipalKind::Agent, false, "agent")
            .unwrap();
        let route = RouteId::new("postgres").unwrap();
        a.set_grants(p.id, vec![(route.clone(), Mode::ReadWrite)])
            .unwrap();
        let session = a
            .acquire_session(
                p.id,
                AcquireRequest {
                    route,
                    mode: None,
                    lifetime_ms: None,
                    isolated: false,
                    label: None,
                    idempotency_key: "one".into(),
                    verifier: Verifier(ScramVerifier::from_password("password", b"salt", 1)),
                    secret_sha256: Sha256::digest(b"secret").into(),
                },
            )
            .unwrap();
        let state = if leader {
            LeaderState::live_for_test()
        } else {
            LeaderState::default()
        };
        let gate = Arc::new(crate::policy::serving::ServingGate::default()); gate.extend_to(u64::MAX);
        let server = PeerServer::new(
            a.clone(),
            state.clone(),
            Arc::new(PeerAuth::new(&[7; 32]).unwrap()),
            HashMap::from([("email".into(), limits)]),
        ).with_bindings_store(store.clone()).unwrap().with_serving_gate(gate.clone()).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(server.clone().serve(listener));
        Self {
            store,
            a,
            clock,
            dir,
            server,
            leader: state,
            gate,
            task,
            url,
            session,
        }
    }
    fn client(&self) -> PeerClient {
        PeerClient::with_url(
            self.url.clone(),
            "pod-a".into(),
            Arc::new(PeerAuth::new(&[7; 32]).unwrap()),
        )
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort(); /* Redb handles are still live; clean after asynchronous task cancellation in test teardown. */
    }
}
fn limits() -> TokenizeLimits {
    TokenizeLimits {
        per_session: 100,
        per_principal: 100,
        principal_window_ms: 100_000,
    }
}
async fn cleanup(f: Fixture) {
    let dir = f.dir.clone();
    f.task.abort();
    tokio::task::yield_now().await;
    drop(f);
    let _ = std::fs::remove_dir_all(dir);
}
fn granted(result: ReserveResult) -> Reservation {
    match result {
        ReserveResult::Granted(r) => r,
        ReserveResult::Ended(_) => panic!("unexpected ended session"),
    }
}
#[tokio::test]
async fn all_operations_round_trip() {
    let f = Fixture::new(true, limits()).await;
    let c = f.client();
    assert_eq!(format!("{:?}", f.server), "PeerServer { [REDACTED] }");
    let id = f.session.id;
    match c
        .session_lookup(Lookup { session_id: id }, TIMEOUT)
        .await
        .unwrap()
    {
        LookupResult::Active {
            session,
            credentials,
            verifiers,
        } => {
            assert_eq!(session.id, id);
            assert_eq!(credentials.len(), 1);
            assert_eq!(verifiers.len(), 1);
            assert_eq!(
                credentials[0].secret_hash,
                <[u8; 32]>::from(Sha256::digest(b"secret"))
            );
        }
        LookupResult::Unknown | LookupResult::Ended { .. } => panic!("missing active session"),
    }
    assert_eq!(
        c.scope_get(
            ScopeGet {
                scope_id: f.session.scope
            },
            TIMEOUT
        )
        .await
        .unwrap()
        .scope
        .unwrap()
        .id,
        f.session.scope
    );
    assert!(matches!(
        c.session_lookup(
            Lookup {
                session_id: SessionId::random()
            },
            TIMEOUT
        )
        .await
        .unwrap(),
        LookupResult::Unknown
    ));
    let change = c
        .changes(ChangesRequest { since: 0, touched: vec![] }, TIMEOUT)
        .await
        .unwrap();
    assert!(change.reset);
    let reservation = granted(
        c.counter_reserve(
            Reserve {
                session_id: id,
                domain: "email".into(),
                want: 1000,
            },
            TIMEOUT,
        )
        .await
        .unwrap(),
    );
    assert_eq!(reservation.granted, 64);
    c.counter_return(
        Return {
            batch_id: reservation.batch_id.clone(),
            unused: 14,
        },
        TIMEOUT,
    )
    .await
    .unwrap();
    c.counter_return(
        Return {
            batch_id: reservation.batch_id,
            unused: 14,
        },
        TIMEOUT,
    )
    .await
    .unwrap();
    assert_eq!(
        f.a.session_tokenizations(id, &crate::policy::DomainName("email".into()))
            .unwrap(),
        50
    );
    for event in [Event::Start, Event::End] {
        c.session_report(
            Report {
                session_id: id,
                event,
                connections: Some(1),
            },
            TIMEOUT,
        )
        .await
        .unwrap();
    }
    assert_eq!(f.a.session(id).unwrap().state, SessionState::Active);
    f.a.revoke(id, EndReason::Requested).unwrap();
    let changes = c
        .changes(ChangesRequest { since: change.next, touched: vec![] }, TIMEOUT)
        .await
        .unwrap();
    assert!(!changes.reset);
    assert_eq!(changes.changes.len(), 1);
    assert_eq!(match changes.changes[0].kind { ChangeKind::End { reason } => reason, ChangeKind::Refresh {} => panic!("expected end") }, EndReason::Requested);
    assert!(matches!(
        c.counter_reserve(
            Reserve {
                session_id: id,
                domain: "email".into(),
                want: 1
            },
            TIMEOUT
        )
        .await
        .unwrap(),
        ReserveResult::Ended(_)
    ));
    assert!(matches!(
        c.session_lookup(Lookup { session_id: id }, TIMEOUT)
            .await
            .unwrap(),
        LookupResult::Ended { .. }
    ));
    cleanup(f).await;
}
#[tokio::test]
async fn expiry_and_unknown_cursor_reset() {
    let f = Fixture::new(true, limits()).await;
    let c = f.client();
    let cursor = c
        .changes(ChangesRequest { since: 0, touched: vec![] }, TIMEOUT)
        .await
        .unwrap()
        .next;
    f.clock.store(f.session.expires_ms, Ordering::SeqCst);
    let r = c
        .changes(ChangesRequest { since: cursor, touched: vec![] }, TIMEOUT)
        .await
        .unwrap();
    assert_eq!(r.changes.len(), 1);
    assert_eq!(match r.changes[0].kind { ChangeKind::End { reason } => reason, ChangeKind::Refresh {} => panic!("expected end") }, EndReason::Expired);
    assert!(
        c.changes(ChangesRequest { since: u64::MAX, touched: vec![] }, TIMEOUT)
            .await
            .unwrap()
            .reset
    );
    cleanup(f).await;
}
#[tokio::test]
async fn two_pods_share_both_limits_and_late_returns_stay_consumed() {
    for (session_limit, principal_limit) in [(23, 100), (100, 23)] {
        let f = Fixture::new(
            true,
            TokenizeLimits {
                per_session: session_limit,
                per_principal: principal_limit,
                ..limits()
            },
        )
        .await;
        let a = f.client();
        let b = f.client();
        let id = f.session.id;
        let other = if principal_limit == 23 {
            f.a.acquire_session(
                f.session.principal,
                AcquireRequest {
                    route: f.session.route.clone(),
                    mode: None,
                    lifetime_ms: None,
                    isolated: false,
                    label: None,
                    idempotency_key: "second".into(),
                    verifier: Verifier(ScramVerifier::from_password("password", b"salt", 1)),
                    secret_sha256: [1; 32],
                },
            )
            .unwrap()
            .id
        } else {
            id
        };
        let reserve = |c: PeerClient, id| async move {
            let mut total = 0;
            for _ in 0..20 {
                total += granted(
                    c.counter_reserve(
                        Reserve {
                            session_id: id,
                            domain: "email".into(),
                            want: 3,
                        },
                        TIMEOUT,
                    )
                    .await
                    .unwrap(),
                )
                .granted;
            }
            total
        };
        let (x, y) = tokio::join!(reserve(a, id), reserve(b, other));
        assert_eq!(x + y, 23);
        cleanup(f).await;
    }
    let f = Fixture::new(true, limits()).await;
    let c = f.client();
    let r = granted(
        c.counter_reserve(
            Reserve {
                session_id: f.session.id,
                domain: "email".into(),
                want: 4,
            },
            TIMEOUT,
        )
        .await
        .unwrap(),
    );
    f.server.expire_batch(&r.batch_id);
    c.counter_return(
        Return {
            batch_id: r.batch_id,
            unused: 4,
        },
        TIMEOUT,
    )
    .await
    .unwrap();
    assert_eq!(
        f.a.session_tokenizations(f.session.id, &crate::policy::DomainName("email".into()))
            .unwrap(),
        4
    );
    cleanup(f).await;
}
#[tokio::test]
async fn nonleader_and_stale_generation() {
    let f = Fixture::new(false, limits()).await;
    let before = f.a.session(f.session.id).unwrap().last_used_ms;
    assert!(matches!(
        f.client()
            .session_report(
                Report {
                    session_id: f.session.id,
                    event: Event::Start,
                    connections: None
                },
                TIMEOUT
            )
            .await,
        Err(PeerError::NotLeader)
    ));
    assert_eq!(f.a.session(f.session.id).unwrap().last_used_ms, before);
    cleanup(f).await;
    let f = Fixture::new(true, limits()).await;
    let c = f.client();
    c.observe_generation(2);
    assert!(matches!(
        c.scope_get(
            ScopeGet {
                scope_id: f.session.scope
            },
            TIMEOUT
        )
        .await,
        Err(PeerError::Stale)
    ));
    cleanup(f).await;
}
async fn raw(f: &Fixture, key: &PeerAuth, ts: u64, nonce: &str, bad: bool) -> u16 {
    let uri: http::Uri = f.url.parse().unwrap();
    let stream = tokio::net::TcpStream::connect((uri.host().unwrap(), uri.port_u16().unwrap()))
        .await
        .unwrap();
    let (mut sender, conn) =
        hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(stream))
            .await
            .unwrap();
    let task = tokio::spawn(conn);
    let body = serde_json::to_vec(&Lookup {
        session_id: f.session.id,
    })
    .unwrap();
    let ts = ts.to_string();
    let path = "/peer/v1/session/lookup";
    let mac = if bad {
        "00".repeat(32)
    } else {
        key.sign(path, &ts, nonce, "pod", &body)
    };
    let req = hyper::Request::builder()
        .method("POST")
        .uri(path)
        .header("host", "localhost")
        .header("x-nunya-peer-ts", ts)
        .header("x-nunya-peer-nonce", nonce)
        .header("x-nunya-peer-pod", "pod")
        .header("x-nunya-peer-mac", mac)
        .body(Full::new(Bytes::from(body)))
        .unwrap();
    let response = sender.send_request(req).await.unwrap();
    let status = response.status().as_u16();
    let _ = response.into_body().collect().await;
    task.abort();
    status
}
#[tokio::test]
async fn rejects_bad_mac_wrong_secret_old_timestamp_and_replay() {
    let f = Fixture::new(true, limits()).await;
    let key = PeerAuth::new(&[7; 32]).unwrap();
    assert_eq!(
        raw(&f, &key, auth::now(), &auth::nonce().unwrap(), true).await,
        401
    );
    assert_eq!(
        raw(
            &f,
            &PeerAuth::new(&[8; 32]).unwrap(),
            auth::now(),
            &auth::nonce().unwrap(),
            false
        )
        .await,
        401
    );
    assert_eq!(
        raw(
            &f,
            &key,
            auth::now() - 31_000,
            &auth::nonce().unwrap(),
            false
        )
        .await,
        401
    );
    let nonce = auth::nonce().unwrap();
    assert_eq!(raw(&f, &key, auth::now(), &nonce, false).await, 200);
    assert_eq!(raw(&f, &key, auth::now(), &nonce, false).await, 401);
    assert!(matches!(
        PeerClient::with_url(
            f.url.clone(),
            "pod".into(),
            Arc::new(PeerAuth::new(&[8; 32]).unwrap())
        )
        .session_lookup(
            Lookup {
                session_id: f.session.id
            },
            TIMEOUT
        )
        .await,
        Err(PeerError::Unauthorized)
    ));
    cleanup(f).await;
}
#[tokio::test]
async fn client_refuses_tampered_sealed_response() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        hyper::server::conn::http1::Builder::new()
            .serve_connection(
                hyper_util::rt::TokioIo::new(stream),
                hyper::service::service_fn(
                    |req: hyper::Request<hyper::body::Incoming>| async move {
                        let nonce = req.headers()["x-nunya-peer-nonce"].to_str().unwrap();
                        let key = PeerAuth::new(&[7; 32]).unwrap();
                        let mut body = key.seal(b"{\"state\":\"unknown\"}", nonce, 200, 1).unwrap();
                        body[0] ^= 1;
                        Ok::<_, std::convert::Infallible>(
                            hyper::Response::builder()
                                .header("x-nunya-peer-generation", "1")
                                .body(Full::new(Bytes::from(body)))
                                .unwrap(),
                        )
                    },
                ),
            )
            .await
            .unwrap();
    });
    let c = PeerClient::with_url(
        url,
        "pod".into(),
        Arc::new(PeerAuth::new(&[7; 32]).unwrap()),
    );
    assert!(matches!(
        c.session_lookup(
            Lookup {
                session_id: SessionId::random()
            },
            TIMEOUT
        )
        .await,
        Err(PeerError::Protocol)
    ));
    task.abort();
}
#[test]
fn strict_messages_keys_and_redacted_debug() {
    assert!(PeerAuth::new(&[1; 31]).is_err());
    assert!(serde_json::from_str::<ChangesRequest>("{\"since\":0,\"extra\":1}").is_err());
    assert!(serde_json::from_str::<Event>("\"unknown\"").is_err());
    let key = Arc::new(PeerAuth::new(&[7; 32]).unwrap());
    let c = PeerClient::with_url(
        "http://localhost".into(),
        "sensitive-pod".into(),
        key.clone(),
    );
    assert_eq!(format!("{key:?}"), "PeerAuth([REDACTED])");
    assert!(!format!("{c:?}").contains("sensitive-pod"));
    let config = crate::config::PeerSection {
        counter_batch: 16,
        listen: "0.0.0.0:6482".parse().unwrap(),
        secret_file: PathBuf::from("secret-path"),
    };
    assert!(!format!("{config:?}").contains("secret-path"));
    let log = ChangeLog::default();
    let mut log = log;
    let cursor = log.read(0, 1).next;
    for _ in 0..4100 {
        log.push(SessionId::random(), EndReason::Requested);
    }
    assert!(log.read(cursor, 1).reset);
    let cursor = log.read(0, 1).next;
    assert!(log.read(cursor, 2).reset);
}

#[tokio::test]
async fn timeout_and_credential_revocation_invalidation() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = PeerClient::with_url(
        format!("http://{}", listener.local_addr().unwrap()),
        "pod".into(),
        Arc::new(PeerAuth::new(&[7; 32]).unwrap()),
    );
    assert!(matches!(
        client
            .session_lookup(
                Lookup {
                    session_id: SessionId::random()
                },
                Duration::from_millis(10)
            )
            .await,
        Err(PeerError::Timeout)
    ));
    let f = Fixture::new(true, limits()).await;
    let c = f.client();
    let cursor = c
        .changes(ChangesRequest { since: 0, touched: vec![] }, TIMEOUT)
        .await
        .unwrap()
        .next;
    let credential =
        f.a.add_principal_credential(
            f.session.principal,
            Verifier(ScramVerifier::from_password("password", b"salt", 1)),
            None,
        )
        .unwrap();
    f.a.revoke_principal_credential(f.session.principal, credential)
        .unwrap();
    let changes = c
        .changes(ChangesRequest { since: cursor, touched: vec![] }, TIMEOUT)
        .await
        .unwrap();
    assert_eq!(changes.changes.len(), 1);
    assert_eq!(changes.changes[0].session_id, f.session.id);
    assert_eq!(
        f.a.session(f.session.id).unwrap().state,
        SessionState::Active
    );
    cleanup(f).await;
}

#[test]
fn config_defaults_required_secret_and_key_loading() {
    let config: crate::config::KubernetesSection =
        toml::from_str("[peer]\nsecret_file='mounted-secret'\n").unwrap();
    assert_eq!(config.peer.listen, "0.0.0.0:6482".parse().unwrap());
    assert!(
        toml::from_str::<crate::config::KubernetesSection>("[peer]\nlisten='127.0.0.1:1'\n")
            .is_err()
    );
    assert!(
        toml::from_str::<crate::config::KubernetesSection>("[peer]\nsecret_file='x'\nunknown=1\n")
            .is_err()
    );
    let dir = PathBuf::from(std::env::var_os("TEMP").unwrap())
        .join(format!("peer-key-{}", SessionId::random().to_hex()));
    std::fs::create_dir_all(&dir).unwrap();
    let file = dir.join("secret");
    std::fs::write(&file, [1; 31]).unwrap();
    assert!(PeerAuth::from_file(&file).is_err());
    std::fs::write(&file, [1; 32]).unwrap();
    assert!(PeerAuth::from_file(&file).is_ok());
    std::fs::remove_dir_all(dir).unwrap();
}


fn pod(f: &Fixture, mono: Arc<AtomicU64>) -> Arc<PeerStateCache> {
    let gate=Arc::new(crate::policy::serving::ServingGate::default()); gate.extend_to(u64::MAX);
    let wall=f.clock.clone();
    Arc::new(PeerStateCache::new(Arc::new(PeerClient::with_url(f.url.clone(),format!("pod-{}",SessionId::random().to_hex()),Arc::new(PeerAuth::new(&[7;32]).unwrap()))),gate,crate::state::runtime::Registry::default(),Duration::from_secs(6),16)
        .with_clocks(Arc::new(move||mono.load(Ordering::SeqCst)),Arc::new(move||wall.load(Ordering::SeqCst))))
}
#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn cache_two_pods_login_tokens_and_revocation() {
    use crate::state::{serving_state::ServingState, authority::Operation};
    use crate::{token::{TokenCodec,StandardCodec,keys::derive_scope_v2},value::{ColumnType,SqlValue},policy::DomainName};
    let f=Fixture::new(true,limits()).await;
    let a=pod(&f,Arc::new(AtomicU64::new(1))); let b=pod(&f,Arc::new(AtomicU64::new(1)));
    let id=f.session.id; let route=f.session.route.clone();
    let authority=f.a.clone(); let aa=a.clone(); let bb=b.clone();
    tokio::task::spawn_blocking(move|| {
        aa.sync().unwrap(); bb.sync().unwrap();
        for cache in [&aa,&bb] { assert!(cache.admit(id,Operation::Read).is_ok()); assert!(cache.session_secret_credential(id,b"secret").is_some()); assert!(cache.session_secret_credential(id,b"wrong").is_none()); }
        let x=aa.session_login(id,&route).unwrap(); let y=bb.session_login(id,&route).unwrap();
        let keys=|s: &TokenScope|derive_scope_v2(&RootKey::from_hex(&"01".repeat(32)).unwrap(),&s.salt,&s.deployment_id,s.route.as_str()).unwrap();
        let token=|s|StandardCodec.encode(&keys(s),&DomainName("email".into()),&ColumnType::Text,None,&SqlValue::Text("private".into())).unwrap();
        assert_eq!(token(&x.scope),token(&y.scope));
        let (_ga,sa)=aa.registry_for_test().register(id,x.principal,route.as_str(),None);
        let (_gb,sb)=bb.registry_for_test().register(id,y.principal,route.as_str(),None);
        authority.revoke(id,EndReason::AdminRevoked).unwrap(); aa.sync().unwrap(); bb.sync().unwrap();
        assert_eq!(sa.try_ended(),Some(crate::state::registry::EndReason::Revoked)); assert_eq!(sb.try_ended(),sa.try_ended());
        assert_eq!(aa.cached(),(0,0)); assert_eq!(bb.cached(),(0,0)); assert!(aa.admit(id,Operation::Read).is_err());
    }).await.unwrap(); drop(a);drop(b);cleanup(f).await;
}
#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn cache_counter_limit_shared_and_unused_returned() {
    use crate::state::{serving_state::ServingState,authority::{TokenizeError,Operation}};
    let f=Fixture::new(true,TokenizeLimits {per_session:20,per_principal:20,..limits()}).await;
    let a=pod(&f,Arc::new(AtomicU64::new(1))); let b=pod(&f,Arc::new(AtomicU64::new(1)));
    let id=f.session.id; let authority=f.a.clone(); let aa=a.clone();let bb=b.clone();
    tokio::task::spawn_blocking(move|| {
        aa.sync().unwrap();bb.sync().unwrap();aa.admit(id,Operation::Read).unwrap();bb.admit(id,Operation::Read).unwrap();
        let domain=crate::policy::DomainName("email".into()); let mut successes=0;
        for _ in 0..25 { for c in [&aa,&bb] { match c.record_tokenization(id,&domain,&limits()) {Ok(())=>successes+=1,Err(TokenizeError::LimitReached)=>{},Err(e)=>panic!("unexpected {e}")} } }
        assert_eq!(successes,20); assert_eq!(authority.session_tokenizations(id,&domain).unwrap(),20);
    }).await.unwrap();
    drop(a);drop(b);cleanup(f).await;
    let f=Fixture::new(true,limits()).await;let a=pod(&f,Arc::new(AtomicU64::new(1)));let id=f.session.id;let aa=a.clone();
    tokio::task::spawn_blocking(move|| {aa.sync().unwrap();aa.report(id,true);aa.record_tokenization(id,&crate::policy::DomainName("email".into()),&limits()).unwrap();aa.report(id,false);}).await.unwrap();
    for _ in 0..100 {if f.a.session_tokenizations(id,&crate::policy::DomainName("email".into())).unwrap()==1 {break;} tokio::time::sleep(Duration::from_millis(10)).await;}
    assert_eq!(f.a.session_tokenizations(id,&crate::policy::DomainName("email".into())).unwrap(),1);
    drop(a);cleanup(f).await;
}
#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn cache_deadline_failed_sync_and_term_reset() {
    use crate::state::{serving_state::ServingState,authority::Operation};
    let f=Fixture::new(true,limits()).await; let mono=Arc::new(AtomicU64::new(1));let a=pod(&f,mono.clone());let id=f.session.id;
    let aa=a.clone();tokio::task::spawn_blocking(move||{aa.sync().unwrap();aa.admit(id,Operation::Read).unwrap();aa.record_tokenization(id,&crate::policy::DomainName("email".into()),&limits()).unwrap();}).await.unwrap();
    let (_live_guard,live_signal)=a.registry_for_test().register(id,f.session.principal,f.session.route.as_str(),None);
    f.leader.term_for_test(2);
    let aa=a.clone();tokio::task::spawn_blocking(move||{aa.sync().unwrap();assert_eq!(aa.cached(),(0,0));aa.admit(id,Operation::Read).unwrap();}).await.unwrap();
    assert_eq!(live_signal.try_ended(),None);
    f.leader.term_for_test(1);
    let aa=a.clone();tokio::task::spawn_blocking(move||{assert_eq!(aa.sync(),Err(PeerError::Stale));assert!(aa.admit(id,Operation::Read).is_ok());}).await.unwrap();
    mono.store(6_000_000_001,Ordering::SeqCst);
    let aa=a.clone();tokio::task::spawn_blocking(move||{assert!(!aa.available());assert!(aa.admit(id,Operation::Read).is_err());assert!(aa.record_tokenization(id,&crate::policy::DomainName("email".into()),&limits()).is_err());assert_eq!(aa.cached(),(0,0));}).await.unwrap();
    drop(a);cleanup(f).await;
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn cache_grant_restriction_gate_expiry_and_outage_misses() {
    use crate::state::{serving_state::ServingState,authority::{Operation,AdmitError}};
    let f=Fixture::new(true,limits()).await; let a=pod(&f,Arc::new(AtomicU64::new(1)));let id=f.session.id;
    let aa=a.clone();let authority=f.a.clone();let row=f.session.clone();
    tokio::task::spawn_blocking(move|| {
        aa.sync().unwrap(); assert!(aa.admit(id,Operation::Write).is_ok());
        authority.set_grants(row.principal,vec![(row.route,Mode::ReadOnly)]).unwrap();
        aa.sync().unwrap();assert!(matches!(aa.admit(id,Operation::Write),Err(AdmitError::ReadOnly)));
    }).await.unwrap();
    f.gate.close();
    let aa=a.clone();tokio::task::spawn_blocking(move|| {
        assert!(aa.admit(id,Operation::Read).is_ok());
        assert!(aa.admit(SessionId::random(),Operation::Read).is_err());
        assert_eq!(aa.sync(),Err(PeerError::NotLeader));
    }).await.unwrap();
    f.task.abort();tokio::task::yield_now().await;
    let aa=a.clone();tokio::task::spawn_blocking(move|| {
        assert!(aa.admit(id,Operation::Read).is_ok());
        assert!(aa.admit(SessionId::random(),Operation::Read).is_err());
        assert!(aa.sync().is_err()); assert!(aa.admit(id,Operation::Read).is_ok());
        aa.close_gate_for_test();assert!(!aa.available());assert!(aa.admit(id,Operation::Read).is_err());assert!(aa.record_tokenization(id,&crate::policy::DomainName("email".into()),&limits()).is_err());assert_eq!(aa.cached(),(0,0));
    }).await.unwrap();drop(a);cleanup(f).await;
}

#[tokio::test(flavor="multi_thread", worker_threads=2)]
async fn renew_refresh_narrows_mode_without_ending_live_connection() {
    use crate::state::{serving_state::ServingState, authority::{Operation, AdmitError}};
    let f = Fixture::new(true, limits()).await;
    let a = pod(&f, Arc::new(AtomicU64::new(1)));
    let id = f.session.id;
    let aa = a.clone(); let authority = f.a.clone(); let principal = f.session.principal;
    tokio::task::spawn_blocking(move || {
        aa.sync().unwrap(); aa.admit(id, Operation::Write).unwrap();
        let (_guard, signal) = aa.registry_for_test().register(id, principal, "postgres", None);
        authority.renew(principal, id, Some(Mode::ReadOnly), Some((Verifier(ScramVerifier::from_password("new", b"salt", 1)), [2;32]))).unwrap();
        aa.sync().unwrap();
        assert_eq!(aa.admit(id, Operation::Write).err(), Some(AdmitError::ReadOnly));
        assert_eq!(signal.try_ended(), None);
        assert!(aa.session_secret_credential(id, b"secret").is_some());
    }).await.unwrap();
    drop(a); cleanup(f).await;
}
#[tokio::test(flavor="multi_thread", worker_threads=2)]
async fn pod_admissions_refresh_leader_idle_before_sweep() {
    use crate::state::{serving_state::ServingState, authority::Operation};
    let f = Fixture::with_session_limits(true, limits(), SessionLimits {idle_timeout_ms:Some(10),..SessionLimits::default()}).await;
    // Short timeout, with no counter or connection reports.
    let a = pod(&f, Arc::new(AtomicU64::new(1)));
    let aa=a.clone(); let clock=f.clock.clone(); let id=f.session.id;
    tokio::task::spawn_blocking(move || {
        aa.sync().unwrap(); aa.admit(id, Operation::Read).unwrap();
        for now in [1009, 1018, 1027] {
            clock.store(now, Ordering::SeqCst);
            aa.admit(id, Operation::Read).unwrap(); aa.sync().unwrap();
        }
    }).await.unwrap();
    assert_eq!(f.a.session(id).unwrap().state, SessionState::Active);
    assert_eq!(f.a.session(id).unwrap().last_used_ms, 1027);
    drop(a); cleanup(f).await;
}
#[tokio::test(flavor="multi_thread", worker_threads=2)]
async fn outage_miss_is_unavailable_and_gate_lapse_does_not_revoke() {
    use crate::state::{serving_state::ServingState, authority::{Operation, AdmitError}};
    let f=Fixture::new(true,limits()).await; let a=pod(&f,Arc::new(AtomicU64::new(1))); let id=f.session.id;
    let aa=a.clone(); tokio::task::spawn_blocking(move || {aa.sync().unwrap();aa.admit(id,Operation::Read).unwrap();}).await.unwrap();
    let (_guard, signal)=a.registry_for_test().register(id,f.session.principal,"postgres",None);
    f.task.abort(); tokio::task::yield_now().await;
    let aa=a.clone(); tokio::task::spawn_blocking(move || {
        assert_eq!(aa.admit(SessionId::random(),Operation::Read).err(),Some(AdmitError::Unavailable));
        aa.close_gate_for_test(); assert!(!aa.available());
    }).await.unwrap();
    assert_eq!(signal.try_ended(),None); drop(a);cleanup(f).await;
}
#[tokio::test]
async fn counter_batch_expires_at_principal_window_end() {
    let f=Fixture::new(true,TokenizeLimits {principal_window_ms:2000,..limits()}).await;
    let r=granted(f.client().counter_reserve(Reserve {session_id:f.session.id,domain:"email".into(),want:1},TIMEOUT).await.unwrap());
    assert_eq!(r.expires_ms,3000); cleanup(f).await;
}
#[tokio::test(flavor="multi_thread", worker_threads=2)]
async fn slow_lookup_does_not_block_cached_admission_and_reset_discards_result() {
    use crate::state::{serving_state::ServingState,authority::Operation};
    let f=Fixture::new(true,limits()).await; let a=pod(&f,Arc::new(AtomicU64::new(1)));let id=f.session.id;
    let aa=a.clone();tokio::task::spawn_blocking(move||{aa.sync().unwrap();aa.admit(id,Operation::Read).unwrap();}).await.unwrap();
    let unknown=SessionId::random();let entered=Arc::new(tokio::sync::Notify::new());
    *f.server.slow_lookup.lock().unwrap()=Some((unknown,entered.clone()));
    let aa=a.clone();let slow=tokio::task::spawn_blocking(move||aa.admit(unknown,Operation::Read));
    entered.notified().await;
    let aa=a.clone();tokio::time::timeout(Duration::from_millis(150),tokio::task::spawn_blocking(move||aa.admit(id,Operation::Read))).await.unwrap().unwrap().unwrap();
    a.close_gate_for_test();assert!(!a.available());
    assert_eq!(slow.await.unwrap().err(),Some(crate::state::authority::AdmitError::Unavailable));
    drop(a);cleanup(f).await;
}
#[tokio::test]
async fn peer_body_cap_precedes_auth_and_connection_limit_refuses() {
    let f=Fixture::new(true,limits()).await;
    let uri:http::Uri=f.url.parse().unwrap();
    let stream=tokio::net::TcpStream::connect((uri.host().unwrap(),uri.port_u16().unwrap())).await.unwrap();
    let (mut sender,conn)=hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(stream)).await.unwrap();let task=tokio::spawn(conn);
    let req=hyper::Request::builder().method("POST").uri("/peer/v1/changes").header("host","localhost").body(Full::new(Bytes::from(vec![0;65537]))).unwrap();
    assert_eq!(sender.send_request(req).await.unwrap().status(),400);task.abort();drop(sender);
    // Hold all slots to exercise the exact same refusal path without 256 sockets.
    let permits=f.server.slots.clone().acquire_many_owned(256).await.unwrap();
    assert!(f.client().changes(ChangesRequest {since:0,touched:vec![]},TIMEOUT).await.is_err());
    drop(permits);cleanup(f).await;
}
#[tokio::test]
async fn shared_builders_refuse_and_server_requires_gate() {
    let f=Fixture::new(true,limits()).await;
    assert!(f.server.clone().with_serving_gate(f.gate.clone()).is_err());
    assert!(f.server.clone().with_limit_source(Arc::new(|_|Some(limits()))).is_err());
    let server=PeerServer::new(f.a.clone(),f.leader.clone(),Arc::new(PeerAuth::new(&[7;32]).unwrap()),HashMap::new());
    let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    assert_eq!(server.serve(listener).await,Err(PeerError::Protocol));cleanup(f).await;
}
#[test]
fn unknown_change_kinds_and_fields_are_refused() {
    assert!(serde_json::from_str::<ChangeKind>(r#"{"kind":"future"}"#).is_err());
    assert!(serde_json::from_str::<ChangeKind>(r#"{"kind":"refresh","reason":"requested"}"#).is_err());
}

#[tokio::test(flavor="multi_thread", worker_threads=2)]
async fn refresh_refetch_signals_session_ended_after_feed_snapshot() {
    use crate::state::{serving_state::ServingState,authority::Operation};
    let f=Fixture::new(true,limits()).await;let a=pod(&f,Arc::new(AtomicU64::new(1)));let id=f.session.id;
    let aa=a.clone();tokio::task::spawn_blocking(move||{aa.sync().unwrap();aa.admit(id,Operation::Read).unwrap();}).await.unwrap();
    let (_guard,signal)=a.registry_for_test().register(id,f.session.principal,"postgres",None);
    f.a.renew(f.session.principal,id,None,None).unwrap();
    let entered=Arc::new(tokio::sync::Notify::new());*f.server.slow_lookup.lock().unwrap()=Some((id,entered.clone()));
    let aa=a.clone();let sync=tokio::task::spawn_blocking(move||aa.sync());entered.notified().await;
    f.a.revoke(id,EndReason::Requested).unwrap();sync.await.unwrap().unwrap();
    assert_eq!(signal.try_ended(),Some(crate::state::registry::EndReason::Revoked));drop(a);cleanup(f).await;
}
#[tokio::test(flavor="multi_thread", worker_threads=2)]
async fn credential_and_grant_refresh_preserve_live_connections_and_drop_batches() {
    use crate::state::{serving_state::ServingState,authority::Operation};
    let f=Fixture::new(true,limits()).await;let a=pod(&f,Arc::new(AtomicU64::new(1)));let id=f.session.id;
    let aa=a.clone();tokio::task::spawn_blocking(move||{aa.sync().unwrap();aa.admit(id,Operation::Write).unwrap();aa.record_tokenization(id,&crate::policy::DomainName("email".into()),&limits()).unwrap();}).await.unwrap();
    let (_guard,signal)=a.registry_for_test().register(id,f.session.principal,"postgres",None);
    let credential=f.a.add_principal_credential(f.session.principal,Verifier(ScramVerifier::from_password("password",b"salt",1)),None).unwrap();
    f.a.revoke_principal_credential(f.session.principal,credential).unwrap();
    f.a.set_grants(f.session.principal,vec![(f.session.route.clone(),Mode::ReadOnly)]).unwrap();
    let aa=a.clone();tokio::task::spawn_blocking(move||{aa.sync().unwrap();assert_eq!(aa.cached(),(0,0));assert_eq!(aa.admit(id,Operation::Write).err(),Some(crate::state::authority::AdmitError::ReadOnly));}).await.unwrap();
    assert_eq!(signal.try_ended(),None);drop(a);cleanup(f).await;
}

#[tokio::test]
async fn bindings_conditional_round_trip_and_refusals() {
    let f = Fixture::new(true, limits()).await;
    crate::policy::bindings::record_route_stamp(
        f.store.as_ref(),
        &RouteId::new("postgres").unwrap(),
        "rev-42-test",
    )
    .unwrap();
    let c = f.client();
    let first = c
        .bindings(
            BindingsRequest {
                have_term: 0,
                have_version: 0,
            },
            TIMEOUT,
        )
        .await
        .unwrap();
    assert_eq!(first.term, f.leader.term());
    assert_eq!(first.version, 1);
    let snapshot = first.snapshot.unwrap();
    assert_eq!(snapshot.version, 1);
    assert_eq!(snapshot.routes["postgres"].revision, 42);
    let current = c
        .bindings(
            BindingsRequest {
                have_term: first.term,
                have_version: 1,
            },
            TIMEOUT,
        )
        .await
        .unwrap();
    assert!(current.snapshot.is_none());
    let changed_term = c
        .bindings(
            BindingsRequest {
                have_term: first.term + 1,
                have_version: 1,
            },
            TIMEOUT,
        )
        .await
        .unwrap();
    assert!(changed_term.snapshot.is_some());
    let changed_version = c
        .bindings(
            BindingsRequest {
                have_term: first.term,
                have_version: 0,
            },
            TIMEOUT,
        )
        .await
        .unwrap();
    assert!(changed_version.snapshot.is_some());
    let bad = PeerClient::with_url(
        f.url.clone(),
        "bad-pod".into(),
        Arc::new(PeerAuth::new(&[8; 32]).unwrap()),
    );
    assert!(matches!(
        bad.bindings(
            BindingsRequest {
                have_term: 0,
                have_version: 0
            },
            TIMEOUT
        )
        .await,
        Err(PeerError::Unauthorized)
    ));
    c.observe_generation(first.term + 1);
    assert!(matches!(
        c.bindings(
            BindingsRequest {
                have_term: 0,
                have_version: 0
            },
            TIMEOUT
        )
        .await,
        Err(PeerError::Stale)
    ));
    cleanup(f).await;
    let follower = Fixture::new(false, limits()).await;
    assert!(matches!(
        follower
            .client()
            .bindings(
                BindingsRequest {
                    have_term: 0,
                    have_version: 0
                },
                TIMEOUT
            )
            .await,
        Err(PeerError::NotLeader)
    ));
    cleanup(follower).await;
}

#[test]
fn bindings_messages_refuse_unknown_fields() {
    assert!(
        serde_json::from_str::<BindingsRequest>(r#"{"have_term":0,"have_version":0,"extra":true}"#)
            .is_err()
    );
    assert!(
        serde_json::from_str::<BindingsReply>(
            r#"{"term":0,"version":0,"snapshot":null,"extra":true}"#
        )
        .is_err()
    );
    assert!(
        serde_json::from_str::<BindingsReply>(
            r#"{"term":0,"version":0,"snapshot":{"version":0,"routes":{},"extra":true}}"#
        )
        .is_err()
    );
    assert!(serde_json::from_str::<BindingsReply>(r#"{"term":0,"version":0,"snapshot":{"version":0,"routes":{"route":{"bindings":[],"retired":[],"revision":0,"reset_owed":false,"extra":true}}}}"#).is_err());
}

#[tokio::test]
async fn bindings_store_unavailable_and_follower_never_reads_it() {
    let f = Fixture::new(true, limits()).await;
    for leader in [false, true] {
        let state = if leader {
            LeaderState::live_for_test()
        } else {
            LeaderState::default()
        };
        let server = PeerServer::new(
            f.a.clone(),
            state,
            Arc::new(PeerAuth::new(&[7; 32]).unwrap()),
            HashMap::new(),
        )
        .with_serving_gate(f.gate.clone())
        .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = PeerClient::with_url(
            format!("http://{}", listener.local_addr().unwrap()),
            "pod-a".into(),
            Arc::new(PeerAuth::new(&[7; 32]).unwrap()),
        );
        let task = tokio::spawn(server.serve(listener));
        let result = client
            .bindings(
                BindingsRequest {
                    have_term: 0,
                    have_version: 0,
                },
                TIMEOUT,
            )
            .await;
        assert!(matches!(
            (leader, result),
            (true, Err(PeerError::Protocol)) | (false, Err(PeerError::NotLeader))
        ));
        task.abort();
        let _ = task.await;
    }
    cleanup(f).await;
}

#[cfg(all(test, feature = "kubernetes"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bindings_expire_after_one_term_and_refuse_observed_term_change() {
    let f = Fixture::new(true, limits()).await;
    crate::policy::bindings::record_route_stamp(f.store.as_ref(), &f.session.route, "rev-1-test").unwrap();
    let mono = Arc::new(AtomicU64::new(1));
    let client = Arc::new(f.client());
    let gate = Arc::new(crate::policy::serving::ServingGate::default());
    gate.extend_to(u64::MAX);
    let clock = mono.clone();
    let wall = f.clock.clone();
    let cache = Arc::new(PeerStateCache::new(client.clone(), gate,
        crate::state::runtime::Registry::default(), Duration::from_secs(6), 16)
        .with_clocks(Arc::new(move || clock.load(Ordering::SeqCst)),
            Arc::new(move || wall.load(Ordering::SeqCst))));
    let worker = cache.clone();
    tokio::task::spawn_blocking(move || worker.sync().unwrap()).await.unwrap();
    assert_eq!(cache.bindings_snapshot().unwrap().routes["postgres"].revision, 1);
    mono.store(6_000_000_000, Ordering::SeqCst);
    assert!(cache.bindings_snapshot().is_some());
    mono.store(6_000_000_001, Ordering::SeqCst);
    assert!(cache.bindings_snapshot().is_none());
    let worker = cache.clone();
    tokio::task::spawn_blocking(move || worker.sync().unwrap()).await.unwrap();
    assert!(cache.bindings_snapshot().is_some());
    // A newer term observed by the shared client invalidates the old bindings
    // immediately, even before another successful bindings fetch.
    client.observe_generation(f.leader.term() + 1);
    assert!(cache.bindings_snapshot().is_none());
    drop(cache);
    cleanup(f).await;
}
