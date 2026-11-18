// A real upstream socket holds the gap after CommandComplete open deterministically.
async fn segment_socket_case(case: &'static str) {
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        let mut f = Fixture::new();
        let (id, _) = acquire_login(&f, "postgres");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ctx = Arc::get_mut(&mut f.ctx).unwrap();
        ctx.upstream.host = "127.0.0.1".into();
        ctx.upstream.port = listener.local_addr().unwrap().port();
        let ctx = f.ctx.clone();
        let (release, released) = tokio::sync::oneshot::channel();
        let fake = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut wire = Agent { io: Box::new(stream), buffer: BytesMut::new(), ctx: ctx.clone() };
            wire.startup().await.unwrap();
            wire.send(&[Backend::AuthenticationOk,
                Backend::BackendKeyData { pid: 123, secret: vec![1, 2, 3, 4] },
                Backend::ReadyForQuery(b'I')]).await.unwrap();
            assert!(matches!(wire.recv().await.unwrap(), Frontend::Query(_)));
            wire.send(&[Backend::CommandComplete("SET".into()), Backend::ReadyForQuery(b'I')]).await.unwrap();
            loop {
                let reply = match wire.recv().await.unwrap() {
                    Frontend::Query(_) => { assert_eq!(case, "simple"); break; }
                    Frontend::Parse { .. } => vec![Backend::ParseComplete],
                    Frontend::Bind { .. } => vec![Backend::BindComplete],
                    Frontend::Describe { kind: b'P', .. } => vec![Backend::NoData],
                    Frontend::Execute { .. } => vec![],
                    Frontend::Sync => break,
                    _ => panic!("unexpected segment request"),
                };
                wire.send(&reply).await.unwrap();
            }
            wire.send(&[Backend::CommandComplete(if case == "read" { "SELECT 0" } else { "INSERT 0 1" }.into())]).await.unwrap();
            if case == "idle" { wire.send(&[Backend::ReadyForQuery(b'I')]).await.unwrap(); }
            released.await.unwrap();
            if case == "revoke" {
                let (stream, _) = listener.accept().await.unwrap();
                let mut cancel = Agent { io: Box::new(stream), buffer: BytesMut::new(), ctx };
                assert!(matches!(cancel.startup().await.unwrap(), StartupMessage::CancelRequest { .. }));
                assert!(wire.recv().await.is_err());
            } else {
                // Half-close to detect any extra work rather than hiding it in an RST.
                wire.io.shutdown().await.unwrap();
                assert!(wire.recv().await.is_err());
            }
            assert!(tokio::time::timeout(std::time::Duration::from_millis(50), listener.accept()).await.is_err());
        });
        let (mut agent, server) = close_test_connect(&f, id, false).await;
        let parse = Frontend::Parse { name: "s".into(), query: if case == "read" {
            "SELECT value FROM items"
        } else { "INSERT INTO items(value) VALUES (NULL)" }.into(), param_types: vec![] };
        let bind = Frontend::Bind { portal: "p".into(), statement: "s".into(),
            param_formats: vec![], params: vec![], result_formats: vec![] };
        let execute = Frontend::Execute { portal: "p".into(), max_rows: 0 };
        if case == "simple" {
            close_test_send(&mut agent, Frontend::Query("INSERT INTO items(value) VALUES (NULL); SELECT 1".into())).await;
        } else if case == "pipeline" {
            let mut bytes = BytesMut::new();
            for message in [parse, bind, Frontend::Describe { kind: b'P', name: "p".into() }, execute, Frontend::Sync] {
                codec::encode_frontend(&message, &mut bytes);
            }
            agent.io.write_all(&bytes).await.unwrap();
            assert!(matches!(close_test_recv(&mut agent).await, Backend::ParseComplete));
            assert!(matches!(close_test_recv(&mut agent).await, Backend::BindComplete));
            assert!(matches!(close_test_recv(&mut agent).await, Backend::NoData));
        } else {
            close_test_send(&mut agent, parse).await;
            assert!(matches!(close_test_recv(&mut agent).await, Backend::ParseComplete));
            close_test_send(&mut agent, bind).await;
            assert!(matches!(close_test_recv(&mut agent).await, Backend::BindComplete));
            close_test_send(&mut agent, execute).await;
            close_test_send(&mut agent, Frontend::Sync).await;
        }
        assert!(matches!(close_test_recv(&mut agent).await, Backend::CommandComplete(_)));
        if case == "idle" {
            assert!(matches!(close_test_recv(&mut agent).await, Backend::ReadyForQuery(b'I')));
        }
        release.send(()).unwrap();
        if case == "revoke" {
            f.ctx.authority.revoke(id, crate::state::model::EndReason::AdminRevoked).unwrap();
        }
        let Backend::ErrorResponse(fields) = close_test_recv(&mut agent).await else { panic!("expected loss error") };
        let unknown = !matches!(case, "read" | "idle");
        assert!(fields.iter().any(|(k, v)| *k == b'C' && v == if unknown { "NY012" } else { "NY010" }));
        drop(agent);
        server.await.unwrap();
        fake.await.unwrap();
        let audit = f.lifecycle.lock().unwrap();
        let records: Vec<_> = audit.iter().filter(|r| r.kind == LifecycleKind::WriteOutcomeUnknown).collect();
        assert_eq!(records.len(), usize::from(unknown));
        if unknown {
            assert_eq!(records[0].reason, if case == "revoke" { "revoked" } else { "upstream_lost" });
            assert_eq!(records[0].session, Some(id.to_hex()));
        }
    }).await.unwrap();
}

#[tokio::test]
async fn segment_extended_loss_after_command_complete() { segment_socket_case("extended").await; }
#[tokio::test]
async fn segment_pipelined_loss_after_command_complete() { segment_socket_case("pipeline").await; }
#[tokio::test]
async fn segment_revoked_after_command_complete() { segment_socket_case("revoke").await; }
#[tokio::test]
async fn segment_idle_loss_after_ready() { segment_socket_case("idle").await; }
#[tokio::test]
async fn segment_simple_multistatement_loss_after_command_complete() { segment_socket_case("simple").await; }
#[tokio::test]
async fn segment_read_only_loss_before_ready() { segment_socket_case("read").await; }

#[test]
fn segment_transaction_write_clears_only_at_ready() {
    for status in *b"ITE" {
        for extended in [false, true] {
            let f = Fixture::new();
            let mut s = f.session();
            s.status = b'T';
            if extended {
                prepare_admission_portal(&mut s, "INSERT INTO items(value) VALUES (NULL)");
                s.frontend(Frontend::Execute { portal: "p".into(), max_rows: 0 });
                s.frontend(Frontend::Sync);
            } else {
                s.frontend(Frontend::Query("INSERT INTO items(value) VALUES (NULL)".into()));
            }
            s.backend(Backend::CommandComplete("INSERT 0 1".into()));
            assert!(s.write_in_flight());
            assert!(!s.backend(Backend::ReadyForQuery(status)).close);
            assert!(!s.write_in_flight());
        }
    }
}

#[test]
fn segment_error_preserves_unknown_until_ready() {
    for extended in [false, true] {
        let f = Fixture::new();
        let mut s = f.session();
        if extended {
            prepare_admission_portal(&mut s, "INSERT INTO items(value) VALUES (NULL)");
            s.frontend(Frontend::Execute { portal: "p".into(), max_rows: 0 });
            s.frontend(Frontend::Sync);
        } else { s.frontend(Frontend::Query("INSERT INTO items(value) VALUES (NULL)".into())); }
        s.backend(Backend::ErrorResponse(vec![(b'S', "ERROR".into()), (b'C', "23505".into())]));
        assert!(s.write_in_flight());
        assert_eq!(s.upstream_lost(failure(NyCode::ProtocolNotAllowed)).code, NyCode::WriteOutcomeUnknown);
        s.backend(Backend::ReadyForQuery(b'I'));
        assert!(!s.write_in_flight());
    }
}
