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

#[tokio::test]
async fn generated_readiness_prefix_is_never_an_unknown_write() {
    let lifecycle = Arc::new(Mutex::new(vec![]));
    let f = Fixture::with_log(Box::new(CaptureLog(Arc::default(), lifecycle.clone()))).await;
    let who = f.svc.authenticate(&f.request((), "alice")).unwrap();
    let audit = WriteAudit::new(&f.svc, &who);
    // Pin googleapis-tonic 0.22.0's generated ready() failure prefix.
    let error = audit.finish_error(forwarded_transport(Status::unknown("Service was not ready: transport error"), true));
    assert_eq!(error.code(), tonic::Code::Unavailable);
    assert!(error.message().starts_with("NY010: "));
    assert!(lifecycle.lock().unwrap().is_empty());
    // A concurrent revoke must not relabel a genuine transport loss.
    revoke(&f);
    let error = audit.finish_error(forwarded_transport(Status::unknown("unrelated error"), true));
    assert_ended(error, true);
    let records = lifecycle.lock().unwrap();
    let records: Vec<_> = records.iter().filter(|r| r.kind == LifecycleKind::WriteOutcomeUnknown).collect();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].reason, "upstream_lost");
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
