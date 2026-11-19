use super::*;

struct PausedRevisionStore {
    head: PolicyRevision,
    entered: Arc<tokio::sync::Notify>,
    release: Mutex<Option<std::sync::mpsc::Receiver<()>>>,
}
impl RevisionStore for PausedRevisionStore {
    fn owner(&self, _: &RouteId) -> Result<Owner, RevisionStoreError> { Ok(Owner::Service) }
    fn active(&self, _: &RouteId) -> Result<Option<PolicyRevision>, RevisionStoreError> {
        if let Some(release) = self.release.lock().unwrap().take() {
            self.entered.notify_one();
            release.recv_timeout(Duration::from_secs(3)).map_err(|_| RevisionStoreError::Unavailable)?;
        }
        Ok(Some(self.head.clone()))
    }
    fn write(&self, _: &RouteId, _: Option<u64>, _: NewRevision) -> Result<WriteOutcome, RevisionStoreError> { Err(RevisionStoreError::Unavailable) }
    fn history(&self, _: &RouteId) -> Result<Vec<RevisionSummary>, RevisionStoreError> { Err(RevisionStoreError::Unavailable) }
    fn get(&self, _: &RouteId, _: u64) -> Result<Option<PolicyRevision>, RevisionStoreError> { Err(RevisionStoreError::Unavailable) }
    fn release_to_file(&self, _: &RouteId) -> Result<(), RevisionStoreError> { Err(RevisionStoreError::Unavailable) }
}

#[tokio::test(flavor = "current_thread")]
async fn paused_follower_head_read_leaves_publication_available_and_recovers() {
    let files = Files::new();
    let follower_files = Files::new();
    let leader = tokenize_hub(&files, &["postgres"]);
    let mut follower = tokenize_hub(&follower_files, &["postgres"]);
    let gate = Arc::new(ServingGate::default());
    gate.extend_to(u64::MAX);
    Arc::get_mut(&mut follower).unwrap().serving_gate = Some(gate.clone());
    let store = binding_store(&files);
    leader.set_store(store.clone()).unwrap();
    leader.set_revision_store(Arc::new(crate::policy::LocalRevisionStore::new(store.clone()))).unwrap();
    leader.set_peer_bindings(Arc::new(|| true), Arc::new(|| None));
    let catalog = customers(&[("email", 1)]);
    leader.register(test_route("postgres", catalog.clone())).unwrap();
    let route = id("postgres");
    let head = leader.import_policy(&route, "test", None).unwrap();
    follower.set_store(binding_store(&follower_files)).unwrap();
    follower.register(test_route("postgres", catalog)).unwrap();
    let snapshot = Arc::new(bindings::snapshot(store.as_ref()).unwrap());
    follower.set_peer_bindings(Arc::new(|| false), Arc::new(move || Some(snapshot.clone())));
    let entered = Arc::new(tokio::sync::Notify::new());
    let (release, paused) = std::sync::mpsc::channel();
    *follower.revision_store.write().unwrap() = Some(Arc::new(PausedRevisionStore {
        head: head.clone(), entered: entered.clone(), release: Mutex::new(Some(paused)),
    }));
    let hub = follower.clone();
    let worker = tokio::task::spawn_blocking(move || hub.recompile_bindings());
    tokio::time::timeout(Duration::from_secs(1), entered.notified()).await.unwrap();
    // A separate thread bounds even a regressed synchronous lock wait. The
    // current-thread executor remains free to release the paused backend.
    let (read, read_done) = tokio::sync::oneshot::channel();
    let hub = follower.clone();
    let reader = std::thread::spawn(move || { let _ = read.send(hub.readiness(true, true)); });
    let available = tokio::time::timeout(Duration::from_millis(250), read_done).await;
    release.send(()).unwrap();
    worker.await.unwrap();
    reader.join().unwrap();
    assert!(available.is_ok(), "publication lock held across head I/O");
    assert!(follower.peer_bindings_ready());
    assert_eq!(follower.policy_revision_for(&route), Some(revision_stamp(&head)));
    assert_eq!(follower.route_owners.read().unwrap()["postgres"], Owner::Service);
    // Exhausting the caller's shared budget must close the gate and retain
    // membership failure, even when the document was already loaded.
    assert!(follower.adopt_revision_with_timeout(&route, &head, Duration::ZERO).is_err());
    assert!(!gate.allow());
    assert!(!follower.peer_bindings_ready());
    follower.recompile_bindings();
    assert!(follower.peer_bindings_ready());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn early_follower_adopts_service_head_before_membership_readiness() {
    follower_recovers_on_renewal(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn follower_refuses_renewal_until_bindings_cover_the_service_head() {
    follower_recovers_on_renewal(true).await;
}

async fn follower_recovers_on_renewal(behind: bool) {
    let files = Files::new();
    let follower_files = Files::new();
    let leader = tokenize_hub(&files, &["postgres"]);
    let follower = tokenize_hub(&follower_files, &["postgres"]);
    let store = binding_store(&files);
    leader.set_store(store.clone()).unwrap();
    follower.set_store(binding_store(&follower_files)).unwrap();
    let revisions = Arc::new(crate::policy::LocalRevisionStore::new(store.clone()));
    leader.set_revision_store(revisions.clone()).unwrap();
    if !behind { follower.set_revision_store(revisions.clone()).unwrap(); }
    leader.set_peer_bindings(Arc::new(|| true), Arc::new(|| None));
    let authority = Authority::load(store.clone(), SessionLimits::default()).unwrap();
    let gate = Arc::new(ServingGate::default());
    gate.extend_to(u64::MAX);
    let server = PeerServer::new(
        authority,
        LeaderState::live_for_test(),
        Arc::new(PeerAuth::new(&[7; 32]).unwrap()),
        Default::default(),
    )
    .with_bindings_store(store.clone()).unwrap()
    .with_serving_gate(gate.clone()).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = Arc::new(PeerClient::with_url(
        format!("http://{}", listener.local_addr().unwrap()),
        "early-follower".into(),
        Arc::new(PeerAuth::new(&[7; 32]).unwrap()),
    ));
    let task = tokio::spawn(server.serve(listener));
    let cache = Arc::new(PeerStateCache::new(
        client, gate, Registry::default(), Duration::from_secs(60), 16,
    ));
    let source = cache.clone();
    follower.set_peer_bindings(Arc::new(|| false), Arc::new(move || source.bindings_snapshot()));
    // Miss the listener notification: renewal must recover even after a
    // snapshot was fetched and its version no longer changes.
    let result = tokio::task::spawn_blocking(move || {
        // Match the live boot order: both pods initially load a file policy.
        // The follower is initially healthy, confirming revision zero.
        let lfixture = crate::pgwire::session::tests::Fixture::new();
        let ffixture = crate::pgwire::session::tests::Fixture::new();
        let catalog = customers(&[("email", 1)]);
        *lfixture.ctx.snapshot.write().unwrap() = Arc::new(catalog.clone());
        *ffixture.ctx.snapshot.write().unwrap() = Arc::new(catalog.clone());
        leader.register(Route::Postgres(lfixture.ctx.clone())).unwrap();
        cache.sync().unwrap();
        if !behind {
            follower.register(Route::Postgres(ffixture.ctx.clone())).unwrap();
            assert!(follower.peer_bindings_ready(), "file-stamped baseline must be usable");
        }
        let first_version = cache.bindings_snapshot().unwrap().version;
        let route = id("postgres");
        let imported = leader.import_policy(&route, "test", Some("import".into())).unwrap();
        if behind { cache.sync().unwrap(); }
        let initial = leader.policy_source(&route).unwrap();
        let next = leader.apply_policy(
            &route, "test", &initial, imported.number,
            &leader.current_catalog_fingerprint(&route).unwrap(), Some("apply".into()),
        ).unwrap();
        if behind {
            // A cold follower already knows the head document and stamp, but
            // its cached leader bindings still cover only the preceding head.
            follower.set_revision_store(revisions).unwrap();
            follower.register(Route::Postgres(ffixture.ctx.clone())).unwrap();
            assert_eq!(follower.policy_revision_for(&route), Some(revision_stamp(&next)));
            assert!(follower.state.lock().unwrap().units[0].routes[0].membership_failed);
        }
        let serving_gate = Arc::new(ServingGate::default());
        let (renewer, writes) = crate::policy::kube_serving::tests::revision_renewer(
            Arc::new(crate::serve::ServingPolicies(follower.clone())), next.clone(), serving_gate.clone(),
        );
        if behind {
            let old_stamp = follower.policy_revision_for(&route);
            assert!(renewer.cycle().is_err());
            assert!(follower.state.lock().unwrap().units[0].routes[0].membership_failed);
            assert_eq!(follower.policy_revision_for(&route), old_stamp);
            assert!(!serving_gate.allow());
            assert_eq!(writes.load(std::sync::atomic::Ordering::SeqCst), 0);
            // A snapshot of the older bindings cannot clear failure by
            // recompiling the previously loaded document.
            follower.recompile_bindings();
            assert!(!follower.peer_bindings_ready());
            assert!(renewer.cycle().is_err());
            assert_eq!(writes.load(std::sync::atomic::Ordering::SeqCst), 0);
        }
        // Each sync uses real loopback HTTP. All but the first have the same version.
        for _ in 0..3 { cache.sync().unwrap(); }
        let fetched = cache.bindings_snapshot().unwrap();
        assert!(fetched.version > first_version, "import/apply must bump binding version");
        assert_eq!(fetched.routes["postgres"].stamp, revision_stamp(&next));
        let version = fetched.version;
        renewer.cycle().unwrap();
        assert_eq!(cache.bindings_snapshot().unwrap().version, version);
        assert!(serving_gate.allow());
        assert_eq!(writes.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(follower.owner(&route).unwrap(), Owner::Service);
        assert_eq!(follower.route_owners.read().unwrap()["postgres"], Owner::Service);
        assert_eq!(follower.policy_source(&route).unwrap(), next.policy_toml);
        let ready = follower.peer_bindings_ready();
        let loaded = follower.policy_revision_for(&route);
        let refresh = follower.refresh(&Route::Postgres(ffixture.ctx.clone()), catalog);
        assert!(follower.peer_bindings_ready(), "head adoption must recover safely");
        tokens_equal(&lfixture.ctx.policy.read().unwrap(), &ffixture.ctx.policy.read().unwrap(), &["email"]);
        (ready, loaded == Some(revision_stamp(&next)), refresh)
    }).await;
    task.abort();
    let _ = task.await;
    let (ready, current, refresh) = result.unwrap();
    assert!(ready && current && refresh.is_ok(),
        "8c5 root causes 1/4: fresh versioned leader bindings must recover the early follower and allow serving renewal");
}
