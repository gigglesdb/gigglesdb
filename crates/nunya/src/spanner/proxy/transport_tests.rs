// Unit coverage uses loopback fakes only; no live upstream is required.
#[tokio::test]
async fn closed_port_writes_are_ny010_without_unknown_audit() {
    let lifecycle = Arc::new(Mutex::new(vec![]));
    let mut f = Fixture::with_log(Box::new(CaptureLog(Arc::default(), lifecycle.clone()))).await;
    let mut sql = f.query("UPDATE customers SET email = @v").await;
    sql.transaction = Some(TransactionSelector {
        selector: Some(transaction_selector::Selector::Begin(rw_options())),
    });
    sql.params = Some(prost_types::Struct { fields: [("v".into(), f.token())].into() });
    let session = sql.session.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    f.svc.channel = Endpoint::from_shared(format!("http://{addr}")).unwrap().connect_lazy();
    let commit = f.svc.commit(f.request(CommitRequest { session: session.clone(), ..Default::default() }, "alice")).await.unwrap_err();
    let dml = f.svc.execute_sql(f.request(sql, "alice")).await.unwrap_err();
    for error in [commit, dml] {
        assert_eq!(error.code(), tonic::Code::Unavailable);
        assert_eq!(error.message(), nunya_status(NyCode::ProtocolNotAllowed).message());
        assert!(error.metadata().is_empty());
        assert!(error.details().is_empty());
    }
    let read = f.svc.execute_sql(f.request(ExecuteSqlRequest {
        session, sql: "SELECT email FROM customers".into(), ..Default::default()
    }, "alice")).await.unwrap_err();
    // Reads retain the existing redacted transport status (no NY code).
    assert_eq!(read.message(), redact_status(read.code()).message());
    assert!(lifecycle.lock().unwrap().is_empty());
}

fn assert_unknown_audit(lifecycle: &Mutex<Vec<LifecycleRecord>>, error: Status) {
    assert_eq!(error.message(), nunya_status(NyCode::WriteOutcomeUnknown).message());
    let records = lifecycle.lock().unwrap();
    let records: Vec<_> = records.iter().filter(|r| r.kind == LifecycleKind::WriteOutcomeUnknown).collect();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].reason, "upstream_lost");
}

#[tokio::test]
async fn upstream_readiness_prefix_is_an_unknown_write() {
    for commit in [true, false] {
        let lifecycle = Arc::new(Mutex::new(vec![]));
        let f = Fixture::with_log(Box::new(CaptureLog(Arc::default(), lifecycle.clone()))).await;
        let error = Status::unknown("Service was not ready: transport error");
        let error = if commit {
            let session = f.session().await;
            *f.fake.commit_status.lock().unwrap() = Some(error);
            let error = f.svc.commit(f.request(CommitRequest { session, ..Default::default() }, "alice")).await.unwrap_err();
            assert_eq!(f.fake.commits.lock().unwrap().len(), 1);
            error
        } else {
            let mut req = f.query("UPDATE customers SET email = @v").await;
            req.transaction = Some(TransactionSelector {
                selector: Some(transaction_selector::Selector::Begin(rw_options())),
            });
            req.params = Some(prost_types::Struct { fields: [("v".into(), f.token())].into() });
            *f.fake.sql_status.lock().unwrap() = Some(error);
            let error = f.svc.execute_sql(f.request(req, "alice")).await.unwrap_err();
            assert_eq!(f.fake.seen.lock().unwrap().len(), 1);
            error
        };
        assert_unknown_audit(&lifecycle, error);
    }
}

#[tokio::test]
async fn streaming_errors_after_first_part_are_unknown_writes() {
    for code in [tonic::Code::Unknown, tonic::Code::Unavailable] {
        let lifecycle = Arc::new(Mutex::new(vec![]));
        let f = Fixture::with_log(Box::new(CaptureLog(Arc::default(), lifecycle.clone()))).await;
        let mut req = f.query("UPDATE customers SET email = @v").await;
        req.transaction = Some(TransactionSelector {
            selector: Some(transaction_selector::Selector::Begin(rw_options())),
        });
        req.params = Some(prost_types::Struct { fields: [("v".into(), f.token())].into() });
        *f.fake.stream_error_after_first.lock().unwrap() = Some(Status::new(code, "Service was not ready: transport error"));
        let mut stream = f.svc.execute_streaming_sql(f.request(req, "alice")).await.unwrap().into_inner();
        assert!(stream.next().await.unwrap().is_ok());
        let error = stream.next().await.unwrap().unwrap_err();
        assert!(stream.next().await.is_none());
        assert_unknown_audit(&lifecycle, error);
    }
}

#[tokio::test]
async fn local_readiness_failure_leaves_dispatch_marker_unset() {
    #[derive(Clone)]
    struct NotReady;
    impl tower::Service<()> for NotReady {
        type Response = ();
        type Error = Status;
        type Future = std::future::Ready<Result<(), Status>>;
        fn poll_ready(&mut self, _: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Status>> {
            std::task::Poll::Ready(Err(Status::unknown("local readiness failure")))
        }
        fn call(&mut self, _: ()) -> Self::Future {
            panic!("unready service must not dispatch")
        }
    }
    let lifecycle = Arc::new(Mutex::new(vec![]));
    let f = Fixture::with_log(Box::new(CaptureLog(Arc::default(), lifecycle.clone()))).await;
    let who = f.svc.authenticate(&f.request((), "alice")).unwrap();
    let audit = WriteAudit::new(&f.svc, &who);
    let marker = DispatchMarker::default();
    let mut service = DispatchService { inner: NotReady, marker: marker.clone() };
    let result = tower::ServiceExt::ready(&mut service).await;
    let error = match result { Err(error) => error, Ok(_) => panic!("service unexpectedly ready") };
    assert!(!marker.dispatched());
    let error = audit.finish_error(marker.classify(error, true));
    assert_eq!(error.code(), tonic::Code::Unavailable);
    assert!(error.message().starts_with("NY010: "));
    assert!(lifecycle.lock().unwrap().is_empty());

    // Keep transport loss distinct from concurrent session revocation.
    revoke(&f);
    let error = audit.finish_error(forwarded_transport(Status::unknown("transport error"), true));
    assert_unknown_audit(&lifecycle, error);
}

fn assert_cancelled_before_dispatch(
    audit: &WriteAudit,
    marker: &DispatchMarker,
    lifecycle: &Mutex<Vec<LifecycleRecord>>,
    error: Status,
) {
    assert!(!marker.dispatched());
    let error = audit.finish_error(error);
    assert_eq!(error.code(), nunya_status(NyCode::SessionEnded).code());
    assert_eq!(error.message(), nunya_status(NyCode::SessionEnded).message());
    assert_ne!(error.message(), nunya_status(NyCode::WriteOutcomeUnknown).message());
    assert!(lifecycle.lock().unwrap().is_empty());
}

#[tokio::test]
async fn already_ended_write_does_not_poll_or_audit_unknown() {
    let lifecycle = Arc::new(Mutex::new(vec![]));
    let f = Fixture::with_log(Box::new(CaptureLog(Arc::default(), lifecycle.clone()))).await;
    let who = f.svc.authenticate(&f.request((), "alice")).unwrap();
    let audit = WriteAudit::new(&f.svc, &who);
    let marker = DispatchMarker::default();
    let (_sender, mut signal) = tokio::sync::watch::channel(true);
    let rpc = async {
        panic!("already-ended write must not poll the RPC future");
        #[allow(unreachable_code)]
        Ok::<(), Status>(())
    };
    let error = cancellable_write(&mut signal, true, &marker, rpc).await.unwrap_err();
    assert_cancelled_before_dispatch(&audit, &marker, &lifecycle, error);
}

#[tokio::test]
async fn write_cancelled_during_readiness_does_not_audit_unknown() {
    struct PendingReady(Option<tokio::sync::oneshot::Sender<()>>);
    impl tower::Service<()> for PendingReady {
        type Response = ();
        type Error = Status;
        type Future = std::future::Ready<Result<(), Status>>;
        fn poll_ready(&mut self, _: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Status>> {
            if let Some(sender) = self.0.take() {
                sender.send(()).unwrap();
            }
            std::task::Poll::Pending
        }
        fn call(&mut self, _: ()) -> Self::Future {
            panic!("pending readiness must not dispatch")
        }
    }
    let lifecycle = Arc::new(Mutex::new(vec![]));
    let f = Fixture::with_log(Box::new(CaptureLog(Arc::default(), lifecycle.clone()))).await;
    let who = f.svc.authenticate(&f.request((), "alice")).unwrap();
    let audit = WriteAudit::new(&f.svc, &who);
    let marker = DispatchMarker::default();
    let (ready_sender, ready_receiver) = tokio::sync::oneshot::channel();
    let mut service = DispatchService { inner: PendingReady(Some(ready_sender)), marker: marker.clone() };
    let (sender, mut signal) = tokio::sync::watch::channel(false);
    let rpc = async {
        let service = tower::ServiceExt::ready(&mut service).await?;
        tower::Service::call(service, ()).await
    };
    let cancel = async {
        ready_receiver.await.unwrap();
        sender.send(true).unwrap();
    };
    let (result, ()) = tokio::time::timeout(Duration::from_secs(3), async {
        tokio::join!(cancellable_write(&mut signal, true, &marker, rpc), cancel)
    }).await.unwrap();
    assert_cancelled_before_dispatch(&audit, &marker, &lifecycle, result.unwrap_err());
}

#[tokio::test]
async fn commit_cancellation_records_actual_end_reason() {
    for (end, expected) in [
        (crate::state::model::EndReason::AdminRevoked, "revoked"),
        (crate::state::model::EndReason::Expired, "expired"),
    ] {
        let lifecycle = Arc::new(Mutex::new(vec![]));
        let f = Fixture::with_log(Box::new(CaptureLog(Arc::default(), lifecycle.clone()))).await;
        let session = f.session().await;
        f.fake.hold_commit.store(true, std::sync::atomic::Ordering::Relaxed);
        let call = f.svc.commit(f.request(CommitRequest { session, ..Default::default() }, "alice"));
        let cancel = async {
            wait_for(|| f.fake.commits.lock().unwrap().len() == 1).await;
            f.svc.ctx.authority.revoke(f.alice, end).unwrap();
        };
        let (response, ()) = tokio::time::timeout(Duration::from_secs(3), async { tokio::join!(call, cancel) }).await.unwrap();
        assert_ended(response.unwrap_err(), true);
        let records = lifecycle.lock().unwrap();
        let records: Vec<_> = records.iter().filter(|r| r.kind == LifecycleKind::WriteOutcomeUnknown).collect();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].reason, expected);
    }
}
