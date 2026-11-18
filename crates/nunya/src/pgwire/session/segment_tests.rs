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
                    Frontend::Query(_) => { assert!(matches!(case, "simple" | "simple_error" | "simple_commit_error")); break; }
                    Frontend::Parse { .. } => vec![Backend::ParseComplete],
                    Frontend::Bind { .. } => vec![Backend::BindComplete],
                    Frontend::Describe { kind: b'P', .. } => vec![Backend::NoData],
                    Frontend::Execute { .. } => vec![],
                    Frontend::Sync => break,
                    _ => panic!("unexpected segment request"),
                };
                wire.send(&reply).await.unwrap();
            }
            if !matches!(case, "simple_error" | "extended_error") {
                wire.send(&[Backend::CommandComplete(if case == "read" { "SELECT 0" } else { "INSERT 0 1" }.into())]).await.unwrap();
            }
            if case.ends_with("error") { wire.send(&[sql_error_fixture("23505")]).await.unwrap(); }
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
        if matches!(case, "simple_error" | "simple_commit_error") {
            close_test_send(&mut agent, Frontend::Query("INSERT INTO items(value) VALUES (NULL)".into())).await;
        } else if case == "simple" {
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
        if !matches!(case, "simple_error" | "extended_error") {
            assert!(matches!(close_test_recv(&mut agent).await, Backend::CommandComplete(_)));
        }
        if case.ends_with("error") {
            let error = close_test_recv(&mut agent).await;
            assert_redacted_commit_error(&[error]);
        }
        if case == "idle" {
            assert!(matches!(close_test_recv(&mut agent).await, Backend::ReadyForQuery(b'I')));
        }
        release.send(()).unwrap();
        if case == "revoke" {
            f.ctx.authority.revoke(id, crate::state::model::EndReason::AdminRevoked).unwrap();
        }
        let Backend::ErrorResponse(fields) = close_test_recv(&mut agent).await else { panic!("expected loss error") };
        let unknown = !matches!(case, "read" | "idle") && !case.ends_with("error");
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

// Keep each response ordered as it would arrive in one Query / Sync segment.
fn segment_before_error(s: &mut Session, extended: bool, history: &str, complete: bool) {
    let insert = "INSERT INTO items(value) VALUES (NULL)";
    let earlier: Vec<(&str, &str)> = match history {
        "none" => vec![],
        "same" => vec![(insert, "INSERT 0 1")],
        "commit" => vec![("BEGIN", "BEGIN"), (insert, "INSERT 0 1"), ("COMMIT", "COMMIT")],
        "rollback" => vec![("BEGIN", "BEGIN"), (insert, "INSERT 0 1"), ("ROLLBACK", "ROLLBACK")],
        _ => panic!("unknown test history"),
    };
    if extended {
        for (sql, tag) in &earlier {
            prepare_admission_portal(s, sql);
            let out = s.frontend(Frontend::Execute { portal: "p".into(), max_rows: 0 });
            assert!(matches!(out.upstream.as_slice(), [Frontend::Execute { .. }]));
            assert!(!s.backend(Backend::CommandComplete((*tag).into())).close);
        }
        prepare_admission_portal(s, insert);
        let out = s.frontend(Frontend::Execute { portal: "p".into(), max_rows: 0 });
        assert!(matches!(out.upstream.as_slice(), [Frontend::Execute { .. }]));
        s.frontend(Frontend::Sync);
    } else {
        let query = earlier.iter().map(|(sql, _)| *sql).chain([insert]).collect::<Vec<_>>().join("; ");
        let out = s.frontend(Frontend::Query(query));
        assert!(matches!(out.upstream.as_slice(), [Frontend::Query(_)]));
        for (_, tag) in &earlier {
            assert!(!s.backend(Backend::CommandComplete((*tag).into())).close);
        }
    }
    if complete {
        assert!(!s.backend(Backend::CommandComplete("INSERT 0 1".into())).close);
    }
    assert!(s.write_in_flight());
}

#[test]
fn segment_definite_error_clears_current_transaction_uncertainty() {
    for extended in [false, true] {
        for complete in [false, true] {
            for history in ["none", "same"] {
                for code in ["23505", "40001", "40P01"] {
                    for revoke in [false, true] {
                        let f = Fixture::new();
                        let mut s = f.session();
                        segment_before_error(&mut s, extended, history, complete);
                        assert!(!s.backend(sql_error_fixture(code)).close);
                        assert!(!s.write_in_flight());
                        if revoke {
                            let out = s.end(crate::state::registry::EndReason::Revoked);
                            assert!(out.close);
                            assert!(matches!(&out.client[0], Backend::ErrorResponse(fields)
                                if fields.iter().any(|(k, v)| *k == b'C' && v == NyCode::SessionEnded.sqlstate())));
                        } else {
                            assert_ne!(s.upstream_lost(failure(NyCode::ProtocolNotAllowed), true).code,
                                NyCode::WriteOutcomeUnknown);
                            assert!(!s.backend(Backend::ReadyForQuery(b'I')).close);
                            assert!(!s.write_in_flight());
                            assert!(!s.segment_known_failure);
                            assert!(!s.segment_unresolved_before_failure);
                        }
                        assert!(!f.lifecycle.lock().unwrap().iter()
                            .any(|r| r.kind == LifecycleKind::WriteOutcomeUnknown));
                    }
                }
            }
        }
    }
}

#[test]
fn segment_definite_error_preserves_previously_ended_transaction_uncertainty() {
    for extended in [false, true] {
        for history in ["commit", "rollback"] {
            for revoke in [false, true] {
                let f = Fixture::new();
                let mut s = f.session();
                segment_before_error(&mut s, extended, history, false);
                assert!(!s.backend(sql_error_fixture("23505")).close);
                assert!(s.write_in_flight());
                if revoke {
                    let out = s.end(crate::state::registry::EndReason::Revoked);
                    assert!(out.close);
                    assert!(matches!(&out.client[0], Backend::ErrorResponse(fields)
                        if fields.iter().any(|(k, v)| *k == b'C' && v == "NY012")));
                } else {
                    assert_eq!(s.upstream_lost(failure(NyCode::ProtocolNotAllowed), true).code,
                        NyCode::WriteOutcomeUnknown);
                }
                let audit = f.lifecycle.lock().unwrap();
                let unknown: Vec<_> = audit.iter().filter(|r| r.kind == LifecycleKind::WriteOutcomeUnknown).collect();
                assert_eq!(unknown.len(), 1);
                assert_eq!(unknown[0].reason, if revoke { "revoked" } else { "upstream_lost" });
                drop(audit);
                assert!(!s.backend(Backend::ReadyForQuery(b'I')).close);
                assert!(!s.write_in_flight());
                assert!(!s.segment_known_failure);
                assert!(!s.segment_unresolved_before_failure);
            }
        }
    }
}

#[tokio::test]
async fn segment_definite_error_then_socket_eof() {
    for case in ["simple_error", "extended_error", "simple_commit_error", "extended_commit_error"] {
        segment_socket_case(case).await;
    }
}
