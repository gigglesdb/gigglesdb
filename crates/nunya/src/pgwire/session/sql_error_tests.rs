// Real sockets exercise Upstream::recv as well as session redaction and recovery.
#[tokio::test]
async fn tcp_upstream_terminating_errors_are_connection_loss() {
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        for (request, severity, nonlocalized) in [
            ("COMMIT", "FATAL", Some("FATAL")),
            ("SELECT value FROM items", "FATAL", Some("FATAL")),
            ("idle", "FATAL", Some("FATAL")),
            ("COMMIT", "FATAL", None),
            ("COMMIT", "PANIC", Some("PANIC")),
            ("COMMIT", "ERROR", Some("FATAL")),
        ] {
            let mut f = Fixture::new();
            let (id, _) = acquire_login(&f, "postgres");
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let ctx = Arc::get_mut(&mut f.ctx).unwrap();
            ctx.upstream.host = "127.0.0.1".into();
            ctx.upstream.port = listener.local_addr().unwrap().port();
            let ctx = f.ctx.clone();
            let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
            let fake = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                let mut wire = Agent {
                    io: Box::new(stream),
                    buffer: BytesMut::new(),
                    ctx,
                };
                wire.startup().await.unwrap();
                wire.send(&[Backend::AuthenticationOk, Backend::ReadyForQuery(b'I')])
                    .await
                    .unwrap();
                assert!(matches!(wire.recv().await.unwrap(), Frontend::Query(_)));
                wire.send(&[
                    Backend::CommandComplete("SET".into()),
                    Backend::ReadyForQuery(b'I'),
                ])
                .await
                .unwrap();
                ready_rx.await.unwrap();
                if request != "idle" {
                    assert!(
                        matches!(wire.recv().await.unwrap(), Frontend::Query(sql) if sql == request)
                    );
                }
                let mut fields = vec![
                    (b'S', severity.into()),
                    (b'C', "57P01".into()),
                    (b'M', "fixture-termination-text".into()),
                ];
                if let Some(value) = nonlocalized {
                    fields.push((b'V', value.into()));
                }
                wire.send(&[Backend::ErrorResponse(fields)]).await.unwrap();
                // Keep the socket open: the error itself must cause loss handling.
                // No replay, Terminate, or other frontend message may follow it.
                assert!(wire.recv().await.is_err());
                assert!(
                    tokio::time::timeout(std::time::Duration::from_millis(50), listener.accept())
                        .await
                        .is_err()
                );
            });
            let gateway = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = gateway.local_addr().unwrap();
            let ctx = f.ctx.clone();
            let server = tokio::spawn(async move {
                let (stream, _) = gateway.accept().await.unwrap();
                serve_tcp(stream, ctx).await;
            });
            let (client, connection) = tokio_postgres::Config::new()
                .host("127.0.0.1")
                .port(address.port())
                .user(format!("session:{}", id.to_hex()))
                .password("login-secret")
                .dbname("app")
                .connect(tokio_postgres::NoTls)
                .await
                .unwrap();
            let driver = tokio::spawn(connection);
            ready_tx.send(()).unwrap();
            let error = if request == "idle" {
                driver.await.unwrap().unwrap_err()
            } else {
                let error = client.batch_execute(request).await.unwrap_err();
                drop(client);
                let _ = driver.await.unwrap();
                error
            };
            assert_eq!(
                error.code().map(|c| c.code()),
                Some(if request == "COMMIT" {
                    "NY012"
                } else {
                    "NY010"
                })
            );
            assert!(
                !error
                    .as_db_error()
                    .unwrap()
                    .message()
                    .contains("fixture-termination-text")
            );
            server.await.unwrap();
            fake.await.unwrap();
            let audit = f.lifecycle.lock().unwrap();
            assert_eq!(audit.len(), usize::from(request == "COMMIT"));
            if request == "COMMIT" {
                assert_eq!(audit[0].kind, LifecycleKind::WriteOutcomeUnknown);
                assert_eq!(audit[0].reason, "upstream_lost");
                assert_eq!(audit[0].session, Some(id.to_hex()));
            }
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn tcp_upstream_error_severity_precedence_and_default() {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        for severities in [
            vec![],
            vec![(b'S', "ERROR".into())],
            vec![(b'S', "FATAL".into()), (b'V', "ERROR".into())],
        ] {
            let mut f = Fixture::new();
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let cfg = &mut Arc::get_mut(&mut f.ctx).unwrap().upstream;
            cfg.host = "127.0.0.1".into();
            cfg.port = listener.local_addr().unwrap().port();
            let ctx = f.ctx.clone();
            let fake = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                let mut wire = Agent {
                    io: Box::new(stream),
                    buffer: BytesMut::new(),
                    ctx,
                };
                wire.startup().await.unwrap();
                wire.send(&[Backend::AuthenticationOk, Backend::ReadyForQuery(b'I')])
                    .await
                    .unwrap();
                assert!(matches!(wire.recv().await.unwrap(), Frontend::Query(_)));
                wire.send(&[
                    Backend::CommandComplete("SET".into()),
                    Backend::ReadyForQuery(b'I'),
                ])
                .await
                .unwrap();
                let mut fields = severities;
                fields.extend([
                    (b'C', "40001".into()),
                    (b'M', "fixture-statement-text".into()),
                ]);
                wire.send(&[Backend::ErrorResponse(fields), Backend::ReadyForQuery(b'I')])
                    .await
                    .unwrap();
            });
            let mut upstream =
                crate::pgwire::upstream::Upstream::connect(&f.ctx.upstream, &["public".into()])
                    .await
                    .unwrap();
            assert!(
                matches!(upstream.recv().await.unwrap(), Backend::ErrorResponse(fields)
                if fields.iter().any(|(k, v)| *k == b'C' && v == "40001"))
            );
            assert!(matches!(
                upstream.recv().await.unwrap(),
                Backend::ReadyForQuery(b'I')
            ));
            fake.await.unwrap();
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn tcp_upstream_sql_errors_preserve_session() {
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        for variant in ["simple", "extended", "transaction"] {
            let mut f = Fixture::new();
            let (id, _) = acquire_login(&f, "postgres");
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let ctx = Arc::get_mut(&mut f.ctx).unwrap();
            ctx.upstream.host = "127.0.0.1".into();
            ctx.upstream.port = listener.local_addr().unwrap().port();
            let fake_ctx = f.ctx.clone();
            let fake = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                let mut wire = Agent {
                    io: Box::new(stream),
                    buffer: BytesMut::new(),
                    ctx: fake_ctx,
                };
                wire.startup().await.unwrap();
                wire.send(&[Backend::AuthenticationOk, Backend::ReadyForQuery(b'I')])
                    .await
                    .unwrap();
                assert!(matches!(wire.recv().await.unwrap(), Frontend::Query(_)));
                wire.send(&[
                    Backend::CommandComplete("SET".into()),
                    Backend::ReadyForQuery(b'I'),
                ])
                .await
                .unwrap();
                let mut failed = false;
                let mut discard = false;
                loop {
                    let request = wire.recv().await;
                    if discard {
                        match request {
                            Ok(Frontend::Sync) => {
                                discard = false;
                                wire.send(&[Backend::ReadyForQuery(b'I')]).await.unwrap();
                            }
                            Ok(Frontend::Parse { .. } | Frontend::Bind { .. }
                                | Frontend::Describe { .. } | Frontend::Execute { .. }
                                | Frontend::Close { .. } | Frontend::Flush) => {}
                            _ => panic!("unexpected request during error recovery"),
                        }
                        continue;
                    }
                    let reply = match request {
                        Ok(Frontend::Query(sql)) if sql == "BEGIN" => {
                            vec![
                                Backend::CommandComplete("BEGIN".into()),
                                Backend::ReadyForQuery(b'T'),
                            ]
                        }
                        Ok(Frontend::Query(sql)) if sql == "ROLLBACK" => {
                            vec![
                                Backend::CommandComplete("ROLLBACK".into()),
                                Backend::ReadyForQuery(b'I'),
                            ]
                        }
                        Ok(Frontend::Query(sql)) if !failed && sql.starts_with("INSERT") => {
                            failed = true;
                            vec![
                                sql_error_fixture("23505"),
                                Backend::ReadyForQuery(if variant == "transaction" {
                                    b'E'
                                } else {
                                    b'I'
                                }),
                            ]
                        }
                        Ok(Frontend::Query(sql)) if failed && sql.starts_with("SELECT") => {
                            wire.send(&[
                                Backend::CommandComplete("SELECT 0".into()),
                                Backend::ReadyForQuery(b'I'),
                            ])
                            .await
                            .unwrap();
                            break;
                        }
                        Ok(Frontend::Parse { .. }) if variant == "extended" => {
                            vec![Backend::ParseComplete]
                        }
                        Ok(Frontend::Describe { .. }) if variant == "extended" => {
                            vec![Backend::ParameterDescription(vec![]), Backend::NoData]
                        }
                        Ok(Frontend::Bind { .. }) if variant == "extended" => {
                            vec![Backend::BindComplete]
                        }
                        Ok(Frontend::Close { .. }) if variant == "extended" => {
                            vec![Backend::CloseComplete]
                        }
                        Ok(Frontend::Execute { .. }) if variant == "extended" && !failed => {
                            failed = true;
                            discard = true;
                            vec![sql_error_fixture("40001")]
                        }
                        Ok(Frontend::Sync) if variant == "extended" => {
                            vec![Backend::ReadyForQuery(b'I')]
                        }
                        Ok(Frontend::Sync) if variant == "transaction" && failed => {
                            vec![Backend::ReadyForQuery(b'E')]
                        }
                        Ok(Frontend::Flush) => vec![],
                        _ => panic!("unexpected fixture request"),
                    };
                    wire.send(&reply).await.unwrap();
                }
            });
            let gateway = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = gateway.local_addr().unwrap();
            let ctx = f.ctx.clone();
            let server = tokio::spawn(async move {
                let (stream, _) = gateway.accept().await.unwrap();
                serve_tcp(stream, ctx).await;
            });
            let (client, connection) = tokio_postgres::Config::new()
                .host("127.0.0.1")
                .port(address.port())
                .user(format!("session:{}", id.to_hex()))
                .password("login-secret")
                .dbname("app")
                .connect(tokio_postgres::NoTls)
                .await
                .unwrap();
            let driver = tokio::spawn(connection);
            if variant == "transaction" {
                client.batch_execute("BEGIN").await.unwrap();
            }
            let sql = "INSERT INTO items(value) VALUES (NULL)";
            let error = if variant == "extended" {
                client.execute(sql, &[]).await.unwrap_err()
            } else {
                client.batch_execute(sql).await.unwrap_err()
            };
            assert_eq!(
                error.code().map(|c| c.code()),
                Some(if variant == "extended" {
                    "40001"
                } else {
                    "23505"
                })
            );
            let error = error.as_db_error().unwrap();
            assert!(!error.message().contains("555-0100"));
            assert!(!error.detail().unwrap_or_default().contains("555-0100"));
            if variant == "transaction" {
                // Existing aborted-session logic produces PG's 25P02 locally.
                let error = client
                    .batch_execute("SELECT value FROM items")
                    .await
                    .unwrap_err();
                assert_eq!(error.code().map(|c| c.code()), Some("25P02"));
                let error = client.prepare("SELECT value FROM items").await.unwrap_err();
                assert_eq!(error.code().map(|c| c.code()), Some("25P02"));
                client.batch_execute("ROLLBACK").await.unwrap();
            }
            client
                .batch_execute("SELECT value FROM items")
                .await
                .unwrap();
            drop(client);
            fake.await.unwrap();
            server.await.unwrap();
            let _ = driver.await.unwrap();
            assert!(f.lifecycle.lock().unwrap().iter().all(|r| r.kind
                != LifecycleKind::WriteOutcomeUnknown
                && r.reason != "upstream_lost"));
        }
    })
    .await
    .unwrap();
}

fn sql_error_fixture(code: &str) -> Backend {
    Backend::ErrorResponse(vec![
        (b'S', "ERROR".into()),
        (b'C', code.into()),
        (b'M', "duplicate 555-0100".into()),
        (b'D', "Key (phone)=(555-0100) already exists.".into()),
    ])
}

#[tokio::test]
async fn tcp_upstream_startup_sql_errors_still_refused() {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        // Authentication, post-authentication startup, and fixed-settings phases.
        for phase in 0..3 {
            let mut f = Fixture::new();
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let cfg = &mut Arc::get_mut(&mut f.ctx).unwrap().upstream;
            cfg.host = "127.0.0.1".into();
            cfg.port = listener.local_addr().unwrap().port();
            let ctx = f.ctx.clone();
            let fake = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                let mut wire = Agent {
                    io: Box::new(stream),
                    buffer: BytesMut::new(),
                    ctx,
                };
                wire.startup().await.unwrap();
                if phase > 0 {
                    wire.send(&[Backend::AuthenticationOk]).await.unwrap();
                }
                if phase > 1 {
                    wire.send(&[Backend::ReadyForQuery(b'I')]).await.unwrap();
                    assert!(matches!(wire.recv().await.unwrap(), Frontend::Query(_)));
                }
                wire.send(&[sql_error_fixture("28P01")]).await.unwrap();
            });
            assert!(
                crate::pgwire::upstream::Upstream::connect(&f.ctx.upstream, &["public".into()])
                    .await
                    .is_err()
            );
            fake.await.unwrap();
        }
    })
    .await
    .unwrap();
}

#[test]
fn sql_error_ready_clears_write_in_flight() {
    let f = Fixture::new();
    let mut s = f.session();
    assert_eq!(
        s.frontend(Frontend::Query(
            "INSERT INTO items(value) VALUES (NULL)".into()
        ))
        .upstream
        .len(),
        1
    );
    assert!(s.write_in_flight());
    assert!(!s.backend(sql_error_fixture("23505")).close);
    assert!(!s.backend(Backend::ReadyForQuery(b'I')).close);
    assert!(!s.write_in_flight());
    assert!(f.lifecycle.lock().unwrap().is_empty());
}

#[test]
fn implicit_commit_error_preserves_boundary_and_drains_deferred() {
    for extended in [false, true] {
        let f = Fixture::new();
        let mut s = f.session();
        if extended {
            prepare_admission_portal(&mut s, "INSERT INTO items(value) VALUES (NULL)");
            s.frontend(Frontend::Execute { portal: "p".into(), max_rows: 0 });
            s.frontend(Frontend::Sync);
        } else {
            s.frontend(Frontend::Query("INSERT INTO items(value) VALUES (NULL)".into()));
        }
        let before_complete = f.records.lock().unwrap().len();
        let complete = s.backend(Backend::CommandComplete("INSERT 0 1".into()));
        assert!(!complete.close);
        assert_eq!(f.records.lock().unwrap().len(), before_complete + 1);
        if extended {
            let deferred = s.frontend(Frontend::Parse {
                name: "next".into(), query: "SELECT value FROM items".into(), param_types: vec![],
            });
            assert!(!deferred.close);
            assert!(deferred.upstream.is_empty());
        }
        let error = s.backend(sql_error_fixture("23505"));
        assert!(!error.close);
        assert!(error.upstream.is_empty());
        assert_redacted_commit_error(&error.client);
        assert!(s.write_in_flight());
        if extended {
            assert!(matches!(s.pending.front(), Some(Pending::Sync)));
            assert!(!s.skip);
            assert!(!s.rollback_on_sync);
            assert!(!s.extended_execute_forwarded);
        } else {
            assert!(s.waiting);
        }
        let ready = s.backend(Backend::ReadyForQuery(b'I'));
        assert!(!ready.close);
        assert!(matches!(ready.client.as_slice(), [Backend::ReadyForQuery(b'I')]));
        assert_eq!(f.records.lock().unwrap().len(), before_complete + 1);
        if extended {
            assert!(matches!(ready.upstream.as_slice(), [Frontend::Parse { name, .. }] if name == "next"));
        } else {
            // The socket loop owns simple-query deferral, outside Session.
            assert!(ready.upstream.is_empty());
            let next = s.frontend(Frontend::Query("SELECT value FROM items".into()));
            assert!(!next.close);
            assert!(matches!(next.upstream.as_slice(), [Frontend::Query(_)]));
        }
        assert!(!s.write_in_flight());
        assert!(s.extended_deferred.is_empty());
        assert!(f.lifecycle.lock().unwrap().is_empty());
    }
    let f = Fixture::new();
    assert!(f.session().backend(sql_error_fixture("23505")).close);
}

fn assert_redacted_commit_error(messages: &[Backend]) {
    let [Backend::ErrorResponse(fields)] = messages else { panic!("expected commit error") };
    assert!(fields.iter().any(|(k, v)| *k == b'C' && v == "23505"));
    assert!(fields.iter().all(|(_, v)| !v.contains("555-0100")));
}

#[tokio::test]
async fn tcp_implicit_commit_errors_preserve_session_and_pipeline() {
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        for extended in [false, true] {
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
                let mut wire = Agent { io: Box::new(stream), buffer: BytesMut::new(), ctx };
                wire.startup().await.unwrap();
                wire.send(&[Backend::AuthenticationOk, Backend::ReadyForQuery(b'I')]).await.unwrap();
                assert!(matches!(wire.recv().await.unwrap(), Frontend::Query(_)));
                wire.send(&[Backend::CommandComplete("SET".into()), Backend::ReadyForQuery(b'I')]).await.unwrap();
                if extended {
                    assert!(matches!(wire.recv().await.unwrap(), Frontend::Parse { .. }));
                    wire.send(&[Backend::ParseComplete]).await.unwrap();
                    assert!(matches!(wire.recv().await.unwrap(), Frontend::Bind { .. }));
                    wire.send(&[Backend::BindComplete]).await.unwrap();
                    assert!(matches!(wire.recv().await.unwrap(), Frontend::Execute { .. }));
                    wire.send(&[Backend::CommandComplete("INSERT 0 1".into())]).await.unwrap();
                    assert!(matches!(wire.recv().await.unwrap(), Frontend::Sync));
                } else {
                    assert!(matches!(wire.recv().await.unwrap(), Frontend::Query(_)));
                    wire.send(&[Backend::CommandComplete("INSERT 0 1".into())]).await.unwrap();
                }
                released.await.unwrap();
                // The client has queued its next segment, but it cannot reach
                // upstream before the first ReadyForQuery.
                assert!(tokio::time::timeout(std::time::Duration::from_millis(50), wire.recv()).await.is_err());
                wire.send(&[sql_error_fixture("23505"), Backend::ReadyForQuery(b'I')]).await.unwrap();
                assert!(matches!(wire.recv().await.unwrap(), Frontend::Parse { name, .. } if name == "next"));
                wire.send(&[Backend::ParseComplete]).await.unwrap();
                assert!(matches!(wire.recv().await.unwrap(), Frontend::Bind { .. }));
                wire.send(&[Backend::BindComplete]).await.unwrap();
                assert!(matches!(wire.recv().await.unwrap(), Frontend::Execute { .. }));
                wire.send(&[Backend::CommandComplete("SELECT 0".into())]).await.unwrap();
                assert!(matches!(wire.recv().await.unwrap(), Frontend::Sync));
                wire.send(&[Backend::ReadyForQuery(b'I')]).await.unwrap();
                assert!(matches!(wire.recv().await.unwrap(), Frontend::Query(_)));
                wire.send(&[Backend::CommandComplete("SELECT 0".into()), Backend::ReadyForQuery(b'I')]).await.unwrap();
                assert!(wire.recv().await.is_err());
            });
            let (mut agent, server) = close_test_connect(&f, id, false).await;
            if extended {
                close_test_send(&mut agent, Frontend::Parse { name: "first".into(), query: "INSERT INTO items(value) VALUES (NULL)".into(), param_types: vec![] }).await;
                assert!(matches!(close_test_recv(&mut agent).await, Backend::ParseComplete));
                close_test_send(&mut agent, Frontend::Bind { portal: "first".into(), statement: "first".into(), param_formats: vec![], params: vec![], result_formats: vec![] }).await;
                assert!(matches!(close_test_recv(&mut agent).await, Backend::BindComplete));
                close_test_send(&mut agent, Frontend::Execute { portal: "first".into(), max_rows: 0 }).await;
                close_test_send(&mut agent, Frontend::Sync).await;
            } else {
                close_test_send(&mut agent, Frontend::Query("INSERT INTO items(value) VALUES (NULL)".into())).await;
            }
            assert!(matches!(close_test_recv(&mut agent).await, Backend::CommandComplete(_)));
            let mut bytes = BytesMut::new();
            for message in [
                Frontend::Parse { name: "next".into(), query: "SELECT value FROM items".into(), param_types: vec![] },
                Frontend::Bind { portal: "next".into(), statement: "next".into(), param_formats: vec![], params: vec![], result_formats: vec![] },
                Frontend::Execute { portal: "next".into(), max_rows: 0 }, Frontend::Sync,
            ] { codec::encode_frontend(&message, &mut bytes); }
            agent.io.write_all(&bytes).await.unwrap();
            release.send(()).unwrap();
            assert_redacted_commit_error(&[close_test_recv(&mut agent).await]);
            assert!(matches!(close_test_recv(&mut agent).await, Backend::ReadyForQuery(b'I')));
            assert!(matches!(close_test_recv(&mut agent).await, Backend::ParseComplete));
            assert!(matches!(close_test_recv(&mut agent).await, Backend::BindComplete));
            assert!(matches!(close_test_recv(&mut agent).await, Backend::CommandComplete(_)));
            assert!(matches!(close_test_recv(&mut agent).await, Backend::ReadyForQuery(b'I')));
            close_test_send(&mut agent, Frontend::Query("SELECT value FROM items".into())).await;
            assert!(matches!(close_test_recv(&mut agent).await, Backend::CommandComplete(_)));
            assert!(matches!(close_test_recv(&mut agent).await, Backend::ReadyForQuery(b'I')));
            close_test_send(&mut agent, Frontend::Terminate).await;
            drop(agent);
            server.await.unwrap();
            fake.await.unwrap();
            assert!(f.lifecycle.lock().unwrap().iter().all(|r| r.kind != LifecycleKind::WriteOutcomeUnknown && r.reason != "upstream_lost"));

        }
    }).await.unwrap();
}
