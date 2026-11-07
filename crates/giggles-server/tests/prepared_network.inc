use super::*;
use tokio_postgres::types::{ToSql, Type};

pub(super) async fn connect_at(
    address: std::net::SocketAddr,
    config: Arc<ClientConfig>,
) -> (tokio_postgres::Client, tokio::task::JoinHandle<()>) {
    let mut cfg = tokio_postgres::Config::new();
    cfg.host("localhost")
        .hostaddr(address.ip())
        .port(address.port())
        .user("giggles")
        .dbname("giggles")
        .ssl_mode(tokio_postgres::config::SslMode::Require);
    let (c, conn) = cfg
        .connect(tokio_postgres_rustls::MakeRustlsConnect::new(
            (*config).clone(),
        ))
        .await
        .unwrap();
    (
        c,
        tokio::spawn(async move {
            let _ = conn.await;
        }),
    )
}
pub(super) fn task_service(f: &Fixture, task: u64, skew: u64) -> Arc<Service> {
    let cert = std::fs::read_to_string(f.path.join("task.pem")).unwrap();
    let der = rustls_pemfile::certs(&mut std::io::Cursor::new(cert))
        .next()
        .unwrap()
        .unwrap();
    Service::new(
        f.admin.clone(),
        vec![Binding {
            fingerprint: tls::fingerprint(&der),
            role: Role::Task {
                domain: f.admin.authority_metadata().unwrap().0.to_string(),
                task: TaskId(task),
                principal: Principal(2),
            },
        }],
        Clock::new(skew, 3600).unwrap(),
    )
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prepared_task_expired_after_commit_withholds_output_but_preserves_original_decision() {
    let f = Fixture::new().await;
    let running = serve_pg(
        "127.0.0.1:0".parse().unwrap(),
        task_service(&f, 3, 0),
        f.server_config.clone(),
        8,
    )
    .await
    .unwrap();
    let (c, conn) = connect_at(running.address, f.task.clone()).await;
    let s = c.prepare(KEY).await.unwrap();
    // Start the expiry window only after server and connection setup.
    let mut g = grant(3, 42, 1, 1);
    g.policy.expires = now() + 3;
    let expiry = g.policy.expires;
    f.admin.grant(g).unwrap();
    f.admin
        .provision_task_resources(
            TaskId(3),
            giggles_core::TaskResources {
                read_rows: 1_000_000,
                disclosed_rows: 1_000_000,
                encoded_bytes: 1_000_000_000,
            },
        )
        .unwrap();
    let (rx, release) = paused(&f.admin, giggles_core::DecisionPoint::Committed);
    let job = tokio::spawn(async move {
        let r = key(&c, &s, 4850, 0, None).await;
        (c, s, r)
    });
    let context = rx.await.unwrap();
    assert!(!context.cancel());
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while now() < expiry {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    release.send(()).unwrap();
    let (c, s, response) = job.await.unwrap();
    f.admin.set_decision_hook(None);
    assert_eq!(response.code, "42501");
    assert_eq!(response.count, None);
    assert_eq!(response.digest, None);
    let state = f.admin.snapshot().unwrap();
    assert_eq!(state.tasks[&TaskId(3)].transitions, 1);
    assert_eq!(
        state.rows[&Key {
            tenant: TenantId(42),
            row: RowId(0)
        }]
            .status,
        Status::Queued
    );
    let executor = f.admin.executor(TaskId(3), Principal(2)).unwrap();
    assert!(matches!(
        executor.status(OperationId(4850)).unwrap(),
        giggles_core::OperationStatus::Decided(Ok(_))
    ));
    drop(executor);
    drop(s);
    drop(c);
    conn.await.unwrap();
    running.shutdown().await.unwrap();
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prepared_task_expired_approval_at_execute_is_not_consumed() {
    let f = Fixture::new().await;
    let running = serve_pg(
        "127.0.0.1:0".parse().unwrap(),
        task_service(&f, 3, 0),
        f.server_config.clone(),
        8,
    )
    .await
    .unwrap();
    let (c, conn) = connect_at(running.address, f.task.clone()).await;
    let s = c.prepare(KEY).await.unwrap();
    // Start the expiry window only after server and connection setup.
    let mut g = grant(3, 42, 1, 0);
    g.policy.expires = now() + 3;
    let expiry = g.policy.expires;
    f.admin.grant(g).unwrap();
    f.admin
        .provision_task_resources(
            TaskId(3),
            giggles_core::TaskResources {
                read_rows: 1_000_000,
                disclosed_rows: 1_000_000,
                encoded_bytes: 1_000_000_000,
            },
        )
        .unwrap();
    let executor = f.admin.executor(TaskId(3), Principal(2)).unwrap();
    let preview = executor
        .preview(&repair(4851, 0..1), Time::Certified(now()))
        .unwrap();
    assert_eq!(preview.expires, expiry); // Current core approval expiry equals task expiry.
    let approval = f
        .admin
        .approve(Principal(3), preview, Time::Certified(now()))
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(4), async {
        while now() < expiry {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(key(&c, &s, 4851, 0, Some(approval.0)).await.code, "42501");
    let state = f.admin.snapshot().unwrap();
    assert_eq!(state.tasks[&TaskId(3)].transitions, 0);
    assert!(!state.approvals[&approval].consumed);
    assert!(
        !state
            .operations
            .contains_key(&(TaskId(3), OperationId(4851)))
    );
    drop(executor);
    drop(s);
    drop(c);
    conn.await.unwrap();
    running.shutdown().await.unwrap();
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prepared_task_range_signed_maximum_and_binary_malformed_widths() {
    let f = Fixture::new().await;
    let mut s = raw_task(&f).await;
    pg_send(&mut s, b'P', &parse("r", RANGE)).await;
    assert_eq!(pg_sync(&mut s).await[0].0, b'1');
    let before = f.admin.snapshot().unwrap();
    let clock = f.admin.authority_metadata().unwrap();
    for (from, until, expected) in [
        ("9223372036854775808", "9223372036854775809", "22003"),
        ("-9223372036854775808", "107", "22003"),
        ("100", "18446744073709551615", "22003"),
    ] {
        pg_send(
            &mut s,
            b'B',
            &bind(
                "p",
                "r",
                &[
                    Some("\\x0000000000001300"),
                    Some("\\x000000000000002a"),
                    Some("failed"),
                    Some(from),
                    Some(until),
                    Some("queued"),
                    None,
                ],
                true,
            ),
        )
        .await;
        assert!(has_code(&pg_sync(&mut s).await, expected));
    }
    for width in [0usize, 1, 7, 9] {
        let values = [
            Some(id(4900)),
            Some(id(42)),
            Some(b"failed".to_vec()),
            Some(vec![0; width]),
            Some(107i64.to_be_bytes().to_vec()),
            Some(b"queued".to_vec()),
            None,
        ];
        let mut b = vec![];
        cstr("p", &mut b);
        cstr("r", &mut b);
        b.extend(1u16.to_be_bytes());
        b.extend(1u16.to_be_bytes());
        b.extend(7u16.to_be_bytes());
        for v in values {
            match v {
                Some(v) => {
                    b.extend((v.len() as i32).to_be_bytes());
                    b.extend(v);
                }
                None => b.extend((-1i32).to_be_bytes()),
            }
        }
        b.extend(0u16.to_be_bytes());
        pg_send(&mut s, b'B', &b).await;
        assert!(has_code(&pg_sync(&mut s).await, "22P03"));
    }
    assert_eq!(f.admin.snapshot().unwrap(), before);
    assert_eq!(f.admin.authority_metadata().unwrap(), clock);
    let p = [
        Some("\\x0000000000001300"),
        Some("\\xffffffffffffffff"),
        Some("failed"),
        Some("0"),
        Some("9223372036854775807"),
        Some("queued"),
        None,
    ];
    pg_send(&mut s, b'B', &bind("p", "r", &p, false)).await;
    pg_send(&mut s, b'E', &execute("p", 0)).await;
    assert_eq!(row_response(&pg_sync(&mut s).await).code, "42501"); // Full unsigned tenant remains scoped.
    assert_eq!(f.admin.snapshot().unwrap().tasks[&TaskId(1)].transitions, 0);
    drop(s);
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prepared_task_statement_and_portal_caps_do_not_execute_or_advance_clock() {
    let f = Fixture::new().await;
    let mut s = raw_task(&f).await;
    let before = f.admin.snapshot().unwrap();
    let clock = f.admin.authority_metadata().unwrap();
    for i in 0..32 {
        pg_send(&mut s, b'P', &parse(&format!("r{i}"), KEY)).await;
        assert_eq!(pg_sync(&mut s).await[0].0, b'1');
    }
    pg_send(&mut s, b'P', &parse("overflow", KEY)).await;
    assert!(has_code(&pg_sync(&mut s).await, "54000"));
    pg_send(&mut s, b'P', &parse("r0", KEY)).await;
    assert!(has_code(&pg_sync(&mut s).await, "42P05"));
    for i in 0..32 {
        pg_send(&mut s, b'B', &key_bind(&format!("p{i}"), "r0", 5000 + i, 0)).await;
        pg_send(&mut s, b'H', &[]).await;
        assert_eq!(pg_read(&mut s).await.0, b'2');
    }
    pg_send(&mut s, b'B', &key_bind("overflow", "r0", 5100, 0)).await;
    pg_send(&mut s, b'H', &[]).await;
    assert!(has_code(&[pg_read(&mut s).await], "54000"));
    assert_eq!(f.admin.snapshot().unwrap(), before);
    assert_eq!(f.admin.authority_metadata().unwrap(), clock);
    let frames = pg_sync(&mut s).await;
    assert_eq!(frames.last().unwrap().0, b'Z');
    pg_send(&mut s, b'B', &key_bind("fresh", "r0", 5100, 0)).await;
    pg_send(&mut s, b'E', &execute("fresh", 0)).await;
    assert_eq!(row_response(&pg_sync(&mut s).await).code, "committed");
    drop(s);
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn prepared_task_native_simple_extended_last_budget_race_has_one_winner() {
    let f = Fixture::new().await;
    f.admin.grant(grant(3, 42, 1, 1)).unwrap();
    f.admin
        .provision_task_resources(
            TaskId(3),
            giggles_core::TaskResources {
                read_rows: 1_000_000,
                disclosed_rows: 1_000_000,
                encoded_bytes: 1_000_000_000,
            },
        )
        .unwrap();
    let service = task_service(&f, 3, 0);
    let native = serve(
        "127.0.0.1:0".parse().unwrap(),
        service.clone(),
        f.server_config.clone(),
        32,
    )
    .await
    .unwrap();
    let pg = serve_pg(
        "127.0.0.1:0".parse().unwrap(),
        service,
        f.server_config.clone(),
        32,
    )
    .await
    .unwrap();
    let barrier = Arc::new(tokio::sync::Barrier::new(12));
    let mut jobs = vec![];
    for i in 0..12u64 {
        let barrier = barrier.clone();
        let config = f.task.clone();
        let address = pg.address;
        let native_address = native.address;
        jobs.push(tokio::spawn(async move {
            if i % 3 == 0 {
                let mut c = Client::connect(native_address, "localhost", config)
                    .await
                    .unwrap();
                barrier.wait().await;
                c.call(&Request::Execute {
                    request: repair(4600 + i, i..i + 1),
                    approval: None,
                })
                .await
                .unwrap()
            } else {
                let (c, conn) = connect_at(address, config).await;
                let s = c.prepare(KEY).await.unwrap(); // All prepared before racing.
                barrier.wait().await;
                let response = if i % 3 == 1 {
                    task_wire(
                        &c,
                        &Request::Execute {
                            request: repair(4600 + i, i..i + 1),
                            approval: None,
                        },
                    )
                    .await
                } else {
                    key(&c, &s, 4600 + i, i, None).await
                };
                drop(s);
                drop(c);
                conn.await.unwrap();
                response
            }
        }));
    }
    let mut winners = 0;
    for job in jobs {
        let r = job.await.unwrap();
        if r.code == "committed" {
            winners += 1;
        } else {
            assert_eq!(r.code, "54000");
        }
    }
    assert_eq!(winners, 1);
    let state = f.admin.snapshot().unwrap();
    assert_eq!(state.tasks[&TaskId(3)].transitions, 1);
    assert_eq!(
        state
            .rows
            .values()
            .filter(|r| r.status == Status::Queued)
            .count(),
        1
    );
    assert_eq!(
        state
            .operations
            .iter()
            .filter(|((t, _), d)| *t == TaskId(3) && d.decision.is_ok())
            .count(),
        1
    );
    native.shutdown().await.unwrap();
    pg.shutdown().await.unwrap();
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prepared_task_lost_ack_late_cancel_reconnect_and_restart_original_status() {
    use giggles_core::DecisionPoint;
    let f = Fixture::new().await;
    let path = f.path.clone();
    let config = f.task.clone();
    let server = f.server_config.clone();
    let bindings: Vec<Binding> =
        serde_json::from_slice(&std::fs::read(path.join("bindings.json")).unwrap()).unwrap();
    let (c, conn) = f.pg(f.task.clone()).await;
    let s = c.prepare(KEY).await.unwrap();
    let token = c.cancel_token();
    let (rx, release) = paused(&f.admin, DecisionPoint::Committed);
    let job = tokio::spawn(async move { key(&c, &s, 4700, 0, None).await });
    let context = rx.await.unwrap();
    token
        .cancel_query(tokio_postgres_rustls::MakeRustlsConnect::new(
            (*f.task).clone(),
        ))
        .await
        .unwrap();
    assert!(!context.cancel());
    job.abort();
    conn.abort();
    let _ = job.await;
    let _ = conn.await;
    release.send(()).unwrap();
    let (c, conn) = f.pg(f.task.clone()).await;
    let st = c.prepare(STATUS).await.unwrap();
    let status = decision(&c, &st, &[&id(4700)]).await;
    assert_eq!(status.code, "committed");
    assert_eq!(status.count, Some(1));
    f.admin.set_decision_hook(None);
    drop(st);
    drop(c);
    conn.await.unwrap();
    f.stop().await;
    let admin = reopen_after_shutdown(path.join("db.redb")).await.unwrap();
    let service = Service::new(admin.clone(), bindings, Clock::new(0, 3600).unwrap()).unwrap();
    let running = serve_pg("127.0.0.1:0".parse().unwrap(), service, server, 8)
        .await
        .unwrap();
    let (c, conn) = connect_at(running.address, config).await;
    let st = c.prepare(STATUS).await.unwrap();
    let s = c.prepare(KEY).await.unwrap();
    assert_eq!(decision(&c, &st, &[&id(4700)]).await, status);
    assert_eq!(key(&c, &s, 4700, 0, None).await, status);
    assert_eq!(key(&c, &s, 4700, 1, None).await.code, "GD002");
    assert_eq!(admin.snapshot().unwrap().tasks[&TaskId(1)].transitions, 1);
    drop(s);
    drop(st);
    drop(c);
    conn.await.unwrap();
    running.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prepared_task_expiry_between_metadata_execute_and_cached_replay() {
    for committed_first in [false, true] {
        let f = Fixture::new().await;
        let running = serve_pg(
            "127.0.0.1:0".parse().unwrap(),
            task_service(&f, 3, 0),
            f.server_config.clone(),
            8,
        )
        .await
        .unwrap();
        // Raw same portal, no Sync: completed Execute must still recheck expiry.
        let mut raw = raw_tls_at(running.address, f.task.clone()).await;
        pg_send(&mut raw, b'P', &parse("r", KEY)).await;
        pg_send(&mut raw, b'B', &key_bind("p", "r", 4800, 0)).await;
        pg_send(&mut raw, b'H', &[]).await;
        assert_eq!(pg_read(&mut raw).await.0, b'1');
        assert_eq!(pg_read(&mut raw).await.0, b'2');
        // Start the expiry window only after server and connection setup.
        let mut g = grant(3, 42, 3, 3);
        g.policy.expires = now() + 3;
        let expiry = g.policy.expires;
        f.admin.grant(g).unwrap();
        f.admin
            .provision_task_resources(
                TaskId(3),
                giggles_core::TaskResources {
                    read_rows: 1_000_000,
                    disclosed_rows: 1_000_000,
                    encoded_bytes: 1_000_000_000,
                },
            )
            .unwrap();
        if committed_first {
            assert_eq!(
                row_response(&raw_execute(&mut raw, "p", 0).await).code,
                "committed"
            );
        }
        tokio::time::timeout(std::time::Duration::from_secs(4), async {
            while now() < expiry {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        let response = row_response(&raw_execute(&mut raw, "p", 0).await);
        assert_eq!(response.code, "42501");
        assert_eq!(response.count, None);
        assert_eq!(response.digest, None);
        assert_eq!(
            f.admin.snapshot().unwrap().tasks[&TaskId(3)].transitions,
            u64::from(committed_first)
        );
        drop(raw);
        running.shutdown().await.unwrap();
        f.stop().await;
    }
}
async fn raw_tls_at(
    address: std::net::SocketAddr,
    config: Arc<ClientConfig>,
) -> tokio_rustls::client::TlsStream<TcpStream> {
    let mut tcp = TcpStream::connect(address).await.unwrap();
    tcp.write_all(&[0, 0, 0, 8, 4, 210, 22, 47]).await.unwrap();
    assert_eq!(tcp.read_u8().await.unwrap(), b'S');
    let mut s = tokio_rustls::TlsConnector::from(config)
        .connect("localhost".try_into().unwrap(), tcp)
        .await
        .unwrap();
    let mut startup = 196608u32.to_be_bytes().to_vec();
    startup.extend(b"user\0giggles\0database\0giggles\0\0");
    s.write_u32((startup.len() + 4) as u32).await.unwrap();
    s.write_all(&startup).await.unwrap();
    while pg_read(&mut s).await.0 != b'Z' {}
    s
}

fn paused(
    admin: &Admin,
    point: giggles_core::DecisionPoint,
) -> (
    tokio::sync::oneshot::Receiver<giggles_core::Cancellation>,
    std::sync::mpsc::Sender<()>,
) {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let tx = std::sync::Mutex::new(Some(tx));
    let (release, wait) = std::sync::mpsc::channel();
    let wait = std::sync::Mutex::new(wait);
    admin.set_decision_hook(Some(Arc::new(move |p| {
        if p == point
            && let Some(tx) = tx.lock().unwrap().take()
        {
            assert!(
                tx.send(giggles_core::Cancellation::current().unwrap())
                    .is_ok()
            );
            wait.lock()
                .unwrap()
                .recv_timeout(std::time::Duration::from_secs(10))
                .unwrap();
        }
    })));
    (rx, release)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prepared_task_typed_malformed_null_oid_count_grammar_and_recovery_corpus() {
    let f = Fixture::new().await;
    let (c, conn) = f.pg(f.task.clone()).await;
    for text in [
        "GIGGLES STATUS ($2)",
        "GIGGLES STATUS ($1,$1)",
        "GIGGLES STATUS (1)",
        "GIGGLES STATUS ($1);",
        "GIGGLES STATUS ($1) -- ignored",
        "GIGGLES STATUS ($1::bytea)",
        "GIGGLES EXECUTE KEY ($1,$2,$3,$4,$5) SELECT 1",
        "GIGGLES PREVIEW KEY ($1,$3,$2,$4)",
        "GIGGLES status ($1)",
    ] {
        assert_eq!(code(c.prepare(text).await.unwrap_err()), "42601", "{text}");
    }
    assert_eq!(
        code(c.prepare_typed(STATUS, &[Type::TEXT]).await.unwrap_err()),
        "0A000"
    );
    assert_eq!(
        code(
            c.prepare_typed(STATUS, &[Type::BYTEA, Type::BYTEA])
                .await
                .unwrap_err()
        ),
        "08P01"
    );
    let s = c.prepare(KEY).await.unwrap();
    let before = f.admin.snapshot().unwrap();
    let clock = f.admin.authority_metadata().unwrap();
    for n in [0, 1, 7, 9, 256, 65536] {
        let bad = vec![1u8; n];
        assert_eq!(
            code(
                c.query(&s, &[&bad, &id(42), &id(0), &"queued", &None::<Vec<u8>>])
                    .await
                    .unwrap_err()
            ),
            "22023",
            "width {n}"
        );
    }
    for slot in 0..4 {
        let operation = Some(id(4300));
        let tenant = Some(id(42));
        let row = Some(id(0));
        let status = Some("queued");
        let approval = None::<Vec<u8>>;
        let null = None::<Vec<u8>>;
        let null_text = None::<String>;
        let mut p: Vec<&(dyn ToSql + Sync)> = vec![&operation, &tenant, &row, &status, &approval];
        p[slot] = if slot == 3 { &null_text } else { &null };
        assert_eq!(code(c.query(&s, &p).await.unwrap_err()), "23502");
    }
    for text in [
        "Queued",
        "queued' WHERE tenant = 43",
        "",
        "succeeded; DROP TABLE app",
        &"x".repeat(65536),
    ] {
        assert_eq!(
            code(
                c.query(&s, &[&id(4300), &id(42), &id(0), &text, &None::<Vec<u8>>])
                    .await
                    .unwrap_err()
            ),
            "22023"
        );
    }
    assert_eq!(
        code(
            c.query(
                &s,
                &[&id(4300), &id(42), &id(0), &"queued", &Some(vec![1u8; 7])]
            )
            .await
            .unwrap_err()
        ),
        "22023"
    );
    let range = c.prepare(RANGE).await.unwrap();
    for (from, until, expected) in [
        (-1i64, 107i64, "22003"),
        (100, -1, "22003"),
        (100, 100, "22023"),
        (107, 100, "22023"),
    ] {
        assert_eq!(
            code(
                c.query(
                    &range,
                    &[
                        &id(4300),
                        &id(42),
                        &"failed",
                        &from,
                        &until,
                        &"queued",
                        &None::<Vec<u8>>
                    ]
                )
                .await
                .unwrap_err()
            ),
            expected
        );
    }
    assert_eq!(f.admin.snapshot().unwrap(), before);
    assert_eq!(f.admin.authority_metadata().unwrap(), clock);
    assert_eq!(key(&c, &s, 4300, 0, None).await.code, "committed");
    drop(range);
    drop(s);
    drop(c);
    conn.await.unwrap();
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prepared_task_raw_text_binary_bounds_bind_drain_and_close_sync() {
    let f = Fixture::new().await;
    let mut s = raw_task(&f).await;
    pg_send(&mut s, b'P', &parse("k", KEY)).await;
    assert_eq!(pg_sync(&mut s).await[0].0, b'1');
    let before = f.admin.snapshot().unwrap();
    let time = f.admin.authority_metadata().unwrap();
    for p in [
        vec![
            Some("\\x00"),
            Some("\\x000000000000002a"),
            Some("\\x0000000000000000"),
            Some("queued"),
            None,
        ],
        vec![Some("bad-hex"), None, None, None, None],
        vec![None, None, None, None, None],
        vec![],
    ] {
        pg_send(&mut s, b'B', &bind("bad", "k", &p, false)).await;
        pg_send(&mut s, b'B', &key_bind("would_run", "k", 4400, 0)).await;
        pg_send(&mut s, b'E', &execute("would_run", 0)).await;
        let frames = pg_sync(&mut s).await;
        assert_eq!(frames.iter().map(|f| f.0).collect::<Vec<_>>(), b"EZ");
    }
    assert_eq!(f.admin.snapshot().unwrap(), before);
    assert_eq!(f.admin.authority_metadata().unwrap(), time);
    // Text BYTEA and binary TEXT result; whitespace grammar remains finite.
    pg_send(&mut s, b'B', &key_bind("p", "k", u64::MAX, 0)).await;
    pg_send(&mut s, b'H', &[]).await;
    assert_eq!(pg_read(&mut s).await.0, b'2');
    assert_eq!(
        row_response(&raw_execute(&mut s, "p", 1).await).code,
        "committed"
    );
    pg_send(&mut s, b'E', &execute("p", u32::MAX)).await;
    assert!(has_code(&pg_sync(&mut s).await, "08P01"));
    pg_send(&mut s, b'C', b"Sk\0").await;
    assert_eq!(pg_sync(&mut s).await[0].0, b'3');
    pg_send(&mut s, b'B', &key_bind("p", "k", 4401, 1)).await;
    assert!(has_code(&pg_sync(&mut s).await, "26000"));
    pg_send(&mut s, b'P', &parse("st", " \tGIGGLES STATUS\n( $1 ) ")).await;
    assert_eq!(pg_sync(&mut s).await[0].0, b'1');
    let op = format!("\\x{}", hex::encode(id(u64::MAX)));
    pg_send(&mut s, b'B', &bind("q", "st", &[Some(&op)], false)).await;
    pg_send(&mut s, b'E', &execute("q", 0)).await;
    assert_eq!(row_response(&pg_sync(&mut s).await).code, "committed");
    pg_send(&mut s, b'E', &execute("q", 0)).await;
    assert!(has_code(&pg_sync(&mut s).await, "34000"));
    assert_eq!(f.admin.snapshot().unwrap().tasks[&TaskId(1)].transitions, 1);
    drop(s);
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prepared_task_cancel_each_decision_fence_preserves_approval_budget_and_next_context() {
    use giggles_core::DecisionPoint;
    for point in [
        DecisionPoint::BeforeDecision,
        DecisionPoint::Decided,
        DecisionPoint::Committed,
    ] {
        let f = Fixture::new().await;
        let (c, conn) = f.pg(f.task.clone()).await;
        let s = c.prepare(KEY).await.unwrap();
        let st = c.prepare(STATUS).await.unwrap();
        let e = f.admin.executor(TaskId(1), Principal(2)).unwrap();
        let preview = e
            .preview(&repair(4500, 0..1), Time::Certified(now()))
            .unwrap();
        let a = f
            .admin
            .approve(Principal(3), preview, Time::Certified(now()))
            .unwrap();
        let before = f.admin.snapshot().unwrap();
        let token = c.cancel_token();
        let (rx, release) = paused(&f.admin, point);
        let job = tokio::spawn(async move {
            let r = key(&c, &s, 4500, 0, Some(a.0)).await;
            (c, s, r)
        });
        let context = tokio::time::timeout(std::time::Duration::from_secs(10), rx)
            .await
            .unwrap()
            .unwrap();
        token
            .cancel_query(tokio_postgres_rustls::MakeRustlsConnect::new(
                (*f.task).clone(),
            ))
            .await
            .unwrap();
        let wins = point == DecisionPoint::BeforeDecision;
        if wins {
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                while context.check().is_ok() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
        } else {
            assert!(!context.cancel());
        }
        release.send(()).unwrap();
        let (c, s, r) = job.await.unwrap();
        f.admin.set_decision_hook(None);
        assert_eq!(r.code, if wins { "57014" } else { "committed" });
        if wins {
            assert_eq!(f.admin.snapshot().unwrap(), before);
        }
        assert_eq!(
            decision(&c, &st, &[&id(4500)]).await.code,
            if wins { "absent" } else { "committed" }
        );
        let committed = key(&c, &s, 4500, 0, Some(a.0)).await;
        assert_eq!(committed.code, "committed");
        assert_eq!(key(&c, &s, 4500, 0, Some(a.0)).await, committed);
        assert_eq!(f.admin.snapshot().unwrap().tasks[&TaskId(1)].transitions, 1);
        assert!(f.admin.snapshot().unwrap().approvals[&a].consumed);
        assert_eq!(key(&c, &s, 4501, 1, None).await.code, "committed");
        drop(e);
        drop(s);
        drop(st);
        drop(c);
        conn.await.unwrap();
        f.stop().await;
    }
}

const KEY: &str = "GIGGLES EXECUTE KEY ($1,$2,$3,$4,$5)";
const PREVIEW: &str = "GIGGLES PREVIEW KEY ($1,$2,$3,$4)";
const RANGE: &str = "GIGGLES EXECUTE RANGE ($1,$2,$3,$4,$5,$6,$7)";
const RANGE_PREVIEW: &str = "GIGGLES PREVIEW RANGE ($1,$2,$3,$4,$5,$6)";
const STATUS: &str = "GIGGLES STATUS ($1)";
fn id(n: u64) -> Vec<u8> {
    n.to_be_bytes().to_vec()
}
async fn decision(
    c: &tokio_postgres::Client,
    s: &tokio_postgres::Statement,
    p: &[&(dyn ToSql + Sync)],
) -> Response {
    let rows = c.query(s, p).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].columns()[0].name(), "decision");
    serde_json::from_str(&rows[0].get::<_, String>(0)).unwrap()
}
async fn key(
    c: &tokio_postgres::Client,
    s: &tokio_postgres::Statement,
    op: u64,
    row: u64,
    approval: Option<u64>,
) -> Response {
    decision(
        c,
        s,
        &[&id(op), &id(42), &id(row), &"queued", &approval.map(id)],
    )
    .await
}
pub(super) async fn raw_task(f: &Fixture) -> tokio_rustls::client::TlsStream<TcpStream> {
    let mut s = raw_tls(f, f.task.clone(), true).await;
    let mut startup = 196608u32.to_be_bytes().to_vec();
    startup.extend(b"user\0giggles\0database\0giggles\0\0");
    s.write_u32((startup.len() + 4) as u32).await.unwrap();
    s.write_all(&startup).await.unwrap();
    while pg_read(&mut s).await.0 != b'Z' {}
    s
}
pub(super) fn row_response(frames: &[(u8, Vec<u8>)]) -> Response {
    assert!(!frames.iter().any(|f| f.0 == b'E'), "{frames:?}");
    let b = &frames.iter().find(|f| f.0 == b'D').expect("decision row").1;
    assert_eq!(&b[..2], &1u16.to_be_bytes());
    let n = i32::from_be_bytes(b[2..6].try_into().unwrap()) as usize;
    assert_eq!(n + 6, b.len());
    serde_json::from_slice(&b[6..]).unwrap()
}
pub(super) async fn raw_execute(
    s: &mut tokio_rustls::client::TlsStream<TcpStream>,
    portal: &str,
    max: u32,
) -> Vec<(u8, Vec<u8>)> {
    pg_send(s, b'E', &execute(portal, max)).await;
    pg_send(s, b'H', &[]).await;
    let mut frames = vec![];
    loop {
        let f = pg_read(s).await;
        let done = matches!(f.0, b'C' | b'E' | b's');
        frames.push(f);
        if done {
            return frames;
        }
    }
}
fn key_text(op: u64, row: u64) -> Vec<String> {
    vec![
        format!("\\x{}", hex::encode(id(op))),
        format!("\\x{}", hex::encode(id(42))),
        format!("\\x{}", hex::encode(id(row))),
        "queued".into(),
    ]
}
fn key_bind(portal: &str, stmt: &str, op: u64, row: u64) -> Vec<u8> {
    let p = key_text(op, row);
    bind(
        portal,
        stmt,
        &[Some(&p[0]), Some(&p[1]), Some(&p[2]), Some(&p[3]), None],
        true,
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prepared_task_actual_driver_metadata_native_equivalence_and_unsigned_ids() {
    let f = Fixture::new().await;
    let (c, conn) = f.pg(f.task.clone()).await;
    let before = f.admin.snapshot().unwrap();
    let metadata = f.admin.authority_metadata().unwrap();
    let s = c.prepare(KEY).await.unwrap();
    assert_eq!(
        s.params(),
        &[
            Type::BYTEA,
            Type::BYTEA,
            Type::BYTEA,
            Type::TEXT,
            Type::BYTEA
        ]
    );
    assert_eq!(s.columns().len(), 1);
    assert_eq!(s.columns()[0].type_(), &Type::TEXT);
    let pv = c.prepare(PREVIEW).await.unwrap();
    let st = c.prepare(STATUS).await.unwrap();
    let range = c.prepare(RANGE).await.unwrap();
    assert_eq!(
        range.params(),
        &[
            Type::BYTEA,
            Type::BYTEA,
            Type::TEXT,
            Type::INT8,
            Type::INT8,
            Type::TEXT,
            Type::BYTEA
        ]
    );
    assert_eq!(f.admin.snapshot().unwrap(), before);
    assert_eq!(f.admin.authority_metadata().unwrap(), metadata);
    let preview = decision(&c, &pv, &[&id(u64::MAX), &id(42), &id(0), &"queued"]).await;
    assert_eq!(preview.code, "preview");
    assert_eq!(f.admin.snapshot().unwrap().tasks[&TaskId(1)].transitions, 0);
    let committed = key(&c, &s, u64::MAX, 0, None).await;
    assert_eq!(committed.code, "committed");
    assert_eq!(committed.count, Some(1));
    assert_eq!(committed.digest, preview.digest);
    assert_eq!(key(&c, &s, u64::MAX, 0, None).await, committed);
    assert_eq!(key(&c, &s, u64::MAX, 1, None).await.code, "GD002");
    assert_eq!(decision(&c, &st, &[&id(u64::MAX)]).await, committed);
    let mut native = f.native(f.task.clone()).await;
    assert_eq!(
        native
            .call(&Request::Execute {
                request: repair(u64::MAX, 0..1),
                approval: None
            })
            .await
            .unwrap(),
        committed
    );
    assert_eq!(
        task_wire(
            &c,
            &Request::Status {
                operation: OperationId(u64::MAX)
            }
        )
        .await,
        committed
    );
    let zero = key(&c, &s, 0, 1, None).await;
    assert_eq!(zero.code, "committed");
    let (other, other_conn) = f.pg(f.other.clone()).await;
    let os = other.prepare(STATUS).await.unwrap();
    assert_eq!(decision(&other, &os, &[&id(u64::MAX)]).await.code, "absent");
    let (app, app_conn) = f.pg(f.app.clone()).await;
    assert_eq!(code(app.prepare(KEY).await.unwrap_err()), "42501");
    assert_eq!(
        code(c.prepare("SELECT id FROM app LIMIT 1").await.unwrap_err()),
        "42501"
    );
    let raw = serde_json::to_string(&committed).unwrap();
    for field in [
        "policy",
        "before",
        "after",
        "tenant",
        "principal",
        "dependencies",
        "issuer",
    ] {
        assert!(!raw.contains(field));
    }
    assert_eq!(f.admin.snapshot().unwrap().tasks[&TaskId(1)].transitions, 2);
    drop(native);
    drop(s);
    drop(pv);
    drop(st);
    drop(range);
    drop(os);
    drop(c);
    drop(other);
    drop(app);
    conn.await.unwrap();
    other_conn.await.unwrap();
    app_conn.await.unwrap();
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prepared_task_bind_describe_are_pure_and_completed_portal_rechecks_revoke() {
    let f = Fixture::new().await;
    let mut s = raw_task(&f).await;
    let before = f.admin.snapshot().unwrap();
    let time = f.admin.authority_metadata().unwrap();
    pg_send(&mut s, b'P', &parse("repair", KEY)).await;
    pg_send(&mut s, b'D', b"Srepair\0").await;
    pg_send(&mut s, b'B', &key_bind("p", "repair", 4100, 0)).await;
    pg_send(&mut s, b'D', b"Pp\0").await;
    pg_send(&mut s, b'H', &[]).await;
    for tag in b"1tT2T" {
        assert_eq!(pg_read(&mut s).await.0, *tag);
    }
    assert_eq!(f.admin.snapshot().unwrap(), before);
    assert_eq!(f.admin.authority_metadata().unwrap(), time);
    let first = row_response(&raw_execute(&mut s, "p", 1).await);
    assert_eq!(first.code, "committed");
    assert_eq!(row_response(&raw_execute(&mut s, "p", 0).await), first);
    assert_eq!(f.admin.snapshot().unwrap().tasks[&TaskId(1)].transitions, 1);
    f.admin.revoke(TaskId(1)).unwrap();
    let denied = row_response(&raw_execute(&mut s, "p", 1).await);
    assert_eq!(denied.code, "42501");
    assert_eq!(denied.count, None);
    assert_eq!(denied.digest, None);
    // Metadata remains static even after revoke. No cached success or policy read.
    pg_send(&mut s, b'D', b"Pp\0").await;
    pg_send(&mut s, b'H', &[]).await;
    assert_eq!(pg_read(&mut s).await.0, b'T');
    pg_send(&mut s, b'C', b"Pp\0").await;
    assert_eq!(pg_sync(&mut s).await[0].0, b'3');
    pg_send(&mut s, b'E', &execute("p", 0)).await;
    assert!(has_code(&pg_sync(&mut s).await, "34000"));
    drop(s);
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prepared_task_revoked_between_parse_bind_execute_and_clock_uncertainty() {
    for revoke_at in [0, 1, 2] {
        let f = Fixture::new().await;
        let mut s = raw_task(&f).await;
        if revoke_at == 0 {
            f.admin.revoke(TaskId(1)).unwrap();
        }
        pg_send(&mut s, b'P', &parse("r", KEY)).await;
        pg_send(&mut s, b'H', &[]).await;
        assert_eq!(pg_read(&mut s).await.0, b'1');
        if revoke_at == 1 {
            f.admin.revoke(TaskId(1)).unwrap();
        }
        pg_send(&mut s, b'B', &key_bind("p", "r", 4101, 0)).await;
        pg_send(&mut s, b'H', &[]).await;
        assert_eq!(pg_read(&mut s).await.0, b'2');
        if revoke_at == 2 {
            f.admin.revoke(TaskId(1)).unwrap();
        }
        assert_eq!(
            row_response(&raw_execute(&mut s, "p", 0).await).code,
            "42501"
        );
        assert_eq!(f.admin.snapshot().unwrap().tasks[&TaskId(1)].transitions, 0);
        drop(s);
        f.stop().await;
    }
    let f = Fixture::new().await;
    let (c, conn) = f.pg(f.task.clone()).await;
    let s = c.prepare(KEY).await.unwrap();
    f.admin
        .certify_server_time(now() + 10000, now() + 10000)
        .unwrap();
    assert_eq!(key(&c, &s, 4102, 0, None).await.code, "55000");
    assert_eq!(f.admin.snapshot().unwrap().tasks[&TaskId(1)].transitions, 0);
    drop(s);
    drop(c);
    conn.await.unwrap();
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prepared_task_range_exact_approval_binding_stale_rows_and_replay() {
    for variant in ["exact", "changed", "stale", "revoked", "wrong_operation"] {
        let f = Fixture::new().await;
        for row in 6..30 {
            f.admin
                .put(
                    Key {
                        tenant: TenantId(42),
                        row: RowId(row),
                    },
                    Row {
                        status: Status::Failed,
                        failed_at: 106,
                        attempt_count: 0,
                        version: 0,
                    },
                )
                .unwrap();
        }
        let (c, conn) = f.pg(f.task.clone()).await;
        let s = c.prepare(RANGE).await.unwrap();
        let pv = c.prepare(RANGE_PREVIEW).await.unwrap();
        let request = giggles_core::Request {
            operation: OperationId(4200),
            selection: Selection::Predicate {
                tenant: TenantId(42),
                status: Status::Failed,
                from: 100,
                until: 104,
            },
            column: Column::Status,
            after: Status::Queued,
        };
        let e = f.admin.executor(TaskId(1), Principal(2)).unwrap();
        let preview = e.preview(&request, Time::Certified(now())).unwrap();
        assert_eq!(preview.effects.len(), 6);
        let a = f
            .admin
            .approve(Principal(3), preview, Time::Certified(now()))
            .unwrap();
        let pp = decision(
            &c,
            &pv,
            &[&id(4200), &id(42), &"failed", &100i64, &104i64, &"queued"],
        )
        .await;
        assert_eq!(pp.code, "preview");
        assert_eq!(pp.count, Some(6));
        assert_eq!(f.admin.snapshot().unwrap().tasks[&TaskId(1)].transitions, 0);
        if variant == "stale" {
            f.admin
                .put(
                    Key {
                        tenant: TenantId(42),
                        row: RowId(0),
                    },
                    Row {
                        status: Status::Failed,
                        failed_at: 103,
                        attempt_count: 1,
                        version: 0,
                    },
                )
                .unwrap();
        }
        if variant == "revoked" {
            f.admin.revoke_approval(a).unwrap();
        }
        let until = if variant == "changed" { 105i64 } else { 104i64 };
        let op = if variant == "wrong_operation" {
            4201
        } else {
            4200
        };
        let response = decision(
            &c,
            &s,
            &[
                &id(op),
                &id(42),
                &"failed",
                &100i64,
                &until,
                &"queued",
                &Some(id(a.0)),
            ],
        )
        .await;
        if variant == "exact" {
            assert_eq!(response.code, "committed");
            assert_eq!(response.count, Some(6));
            assert_eq!(response.digest, pp.digest);
            // Consumed approval is valid only through original durable dedupe.
            assert_eq!(
                decision(
                    &c,
                    &s,
                    &[
                        &id(4200),
                        &id(42),
                        &"failed",
                        &100i64,
                        &104i64,
                        &"queued",
                        &Some(id(a.0))
                    ]
                )
                .await,
                response
            );
            assert_eq!(
                task_wire(
                    &c,
                    &Request::Execute {
                        request,
                        approval: Some(a)
                    }
                )
                .await,
                response
            );
            assert_eq!(f.admin.snapshot().unwrap().tasks[&TaskId(1)].transitions, 6);
            assert!(f.admin.snapshot().unwrap().approvals[&a].consumed);
        } else {
            assert_eq!(response.code, "GD001", "{variant}");
            assert_eq!(f.admin.snapshot().unwrap().tasks[&TaskId(1)].transitions, 0);
            assert!(!f.admin.snapshot().unwrap().approvals[&a].consumed);
        }
        drop(s);
        drop(pv);
        drop(c);
        drop(e);
        conn.await.unwrap();
        f.stop().await;
    }
}
