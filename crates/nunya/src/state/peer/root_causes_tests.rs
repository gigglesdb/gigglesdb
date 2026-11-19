use super::*;
use crate::state::{runtime::Registry, serving_state::ServingState};

#[tokio::test(flavor = "current_thread")]
async fn abort_with_blocked_handler_does_not_block_executor_and_releases_listener() {
    let f = Fixture::new(true, limits()).await;
    let entered = Arc::new(tokio::sync::Notify::new());
    let notify = entered.clone();
    let (release, paused) = std::sync::mpsc::channel();
    let paused = std::sync::Mutex::new(paused);
    let server = PeerServer::new(
        f.a.clone(), f.leader.clone(), Arc::new(PeerAuth::new(&[7; 32]).unwrap()), HashMap::new(),
    ).with_serving_gate(f.gate.clone()).unwrap().with_limit_source(Arc::new(move |_| {
        notify.notify_one();
        let _ = paused.lock().unwrap().recv_timeout(Duration::from_secs(3));
        Some(limits())
    })).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let client = PeerClient::with_url(format!("http://{addr}"), "pod-a".into(), Arc::new(PeerAuth::new(&[7; 32]).unwrap()));
    let task = tokio::spawn(server.clone().serve(listener));
    let sid = f.session.id;
    let request = tokio::spawn(async move {
        client.counter_reserve(Reserve { session_id: sid, domain: "email".into(), want: 1 }, TIMEOUT).await
    });
    tokio::time::timeout(Duration::from_secs(1), entered.notified()).await.unwrap();
    let start = std::time::Instant::now();
    task.abort();
    // Awaiting cancellation runs PeerThread::drop on this single executor.
    // A regressed join delays both the cancellation and unrelated async work.
    let progress = tokio::spawn(async { tokio::task::yield_now().await; 1 });
    let _ = task.await;
    assert_eq!(progress.await.unwrap(), 1);
    let elapsed = start.elapsed();
    let _ = release.send(());
    request.abort();
    let _ = request.await;
    // The reaper must still let the serving thread release its listening socket.
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if let Ok(listener) = tokio::net::TcpListener::bind(addr).await { drop(listener); break; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.unwrap();
    drop(server);
    cleanup(f).await;
    assert!(elapsed < Duration::from_millis(250), "cancellation blocked the gateway executor");
}

#[tokio::test(flavor = "current_thread")]
async fn leader_and_follower_resolve_the_same_session_without_executor_starvation() {
    let f = Fixture::new(true, limits()).await;
    let new_cache = || {
        let gate = Arc::new(crate::policy::serving::ServingGate::default());
        gate.extend_to(u64::MAX);
        let clock = f.clock.clone();
        Arc::new(PeerStateCache::new(
            Arc::new(f.client()), gate, Registry::default(), Duration::from_secs(60), 16,
        ).with_clocks(Arc::new(|| 0), Arc::new(move || clock.load(Ordering::SeqCst))))
    };
    let leader_cache = new_cache();
    let follower_cache = new_cache();
    let sid = f.session.id;
    let route = f.session.route.clone();
    let local = f.a.session_login(sid, &route).unwrap();
    let lc = leader_cache.clone();
    let fc = follower_cache.clone();
    let remote_route = route.clone();
    let remote = tokio::task::spawn_blocking(move || {
        lc.sync().unwrap();
        fc.sync().unwrap();
        // Independent follower executor leaves the leader transport free.
        fc.session_login(sid, &remote_route).unwrap()
    }).await.unwrap();
    assert!(local.principal == remote.principal && local.scope.id == remote.scope.id);
    assert!(serde_json::to_vec(&local.scope).unwrap() == serde_json::to_vec(&remote.scope).unwrap());
    assert!(local.verifiers[0].1.0.to_pg_string() == remote.verifiers[0].1.0.to_pg_string());
    assert!(leader_cache.available());
    // Production pgwire calls this synchronously on its Tokio executor. In the
    // leader pod that executor also owns PeerServer::serve. A one-worker runtime
    // deterministically exposes the circular wait, without sleeps or injected I/O.
    let leader_login = leader_cache.session_login(sid, &route);
    assert!(leader_login.as_ref().is_some_and(|login|
        login.principal == remote.principal && login.scope.id == remote.scope.id
        && serde_json::to_vec(&login.scope).unwrap() == serde_json::to_vec(&remote.scope).unwrap()
        && login.verifiers[0].1.0.to_pg_string() == remote.verifiers[0].1.0.to_pg_string()));
    drop(leader_cache);
    drop(follower_cache);
    cleanup(f).await;
    assert!(leader_login.is_some(),
        "8c5 root cause 2: leader self-peer login must not block its own transport executor");
}
