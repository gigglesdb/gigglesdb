// Actual processes, TLS sockets and pinned tokio-postgres. No local service kernel.
struct PgSetup {
    n: NativeSetup,
    addresses: BTreeMap<u64, String>,
}
fn pg_setup() -> PgSetup {
    let n = native_setup();
    let mut addresses = BTreeMap::new();
    std::fs::create_dir(n.s.root.join("pgwire-hooks")).unwrap();
    for id in VOTERS {
        let mut c: Configuration =
            serde_json::from_slice(&std::fs::read(&n.s.configs[&id]).unwrap()).unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let mut pg = c.task.clone().unwrap();
        pg.listen = address.clone();
        c.pgwire = Some(pg);
        std::fs::write(&n.s.configs[&id], serde_json::to_vec_pretty(&c).unwrap()).unwrap();
        addresses.insert(id, address);
    }
    PgSetup { n, addresses }
}
fn pg_tls(p: &PgSetup) -> rustls::ClientConfig {
    let mut roots = rustls::RootCertStore::empty();
    let certs = |path: &Path| {
        rustls_pemfile::certs(&mut BufReader::new(std::fs::File::open(path).unwrap()))
            .collect::<std::io::Result<Vec<_>>>()
            .unwrap()
    };
    for c in certs(&p.n.client.ca) {
        roots.add(c).unwrap();
    }
    let key = rustls_pemfile::private_key(&mut BufReader::new(
        std::fs::File::open(&p.n.client.key).unwrap(),
    ))
    .unwrap()
    .unwrap();
    rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_client_auth_cert(certs(&p.n.client.certificate), key)
        .unwrap()
}
async fn pg_client(p: &PgSetup, id: u64) -> tokio_postgres::Client {
    let stream = tokio::net::TcpStream::connect(&p.addresses[&id])
        .await
        .unwrap();
    let mut c = tokio_postgres::Config::new();
    c.host("localhost")
        .user("spoofed-owner")
        .dbname("spoofed-other-domain")
        .ssl_mode(tokio_postgres::config::SslMode::Require);
    let mut tls = tokio_postgres_rustls::MakeRustlsConnect::new(pg_tls(p));
    let connector = tokio_postgres::tls::MakeTlsConnect::<tokio::net::TcpStream>::make_tls_connect(
        &mut tls,
        "localhost",
    )
    .unwrap();
    let (client, connection) = c.connect_raw(stream, connector).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}
async fn pg_raw(p: &PgSetup, id: u64) -> tokio_rustls::client::TlsStream<tokio::net::TcpStream> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut tcp = tokio::net::TcpStream::connect(&p.addresses[&id])
        .await
        .unwrap();
    tcp.write_all(&[0, 0, 0, 8, 4, 210, 22, 47]).await.unwrap();
    assert_eq!(tcp.read_u8().await.unwrap(), b'S');
    let mut tls = tokio_rustls::TlsConnector::from(std::sync::Arc::new(pg_tls(p)))
        .connect(
            rustls::pki_types::ServerName::try_from("localhost").unwrap(),
            tcp,
        )
        .await
        .unwrap();
    let mut startup = 196608u32.to_be_bytes().to_vec();
    startup.extend(b"user\0owner\0database\0other\0\0");
    tls.write_u32((startup.len() + 4) as u32).await.unwrap();
    tls.write_all(&startup).await.unwrap();
    loop {
        let (tag, _) = pg_read(&mut tls).await;
        if tag == b'Z' {
            break;
        }
    }
    tls
}
async fn pg_read(t: &mut tokio_rustls::client::TlsStream<tokio::net::TcpStream>) -> (u8, Vec<u8>) {
    use tokio::io::AsyncReadExt;
    tokio::time::timeout(Duration::from_secs(7), async {
        let tag = t.read_u8().await.unwrap();
        let n = t.read_u32().await.unwrap() as usize;
        assert!((4..=65536).contains(&n));
        let mut b = vec![0; n - 4];
        t.read_exact(&mut b).await.unwrap();
        (tag, b)
    })
    .await
    .unwrap()
}
async fn pg_send(
    t: &mut tokio_rustls::client::TlsStream<tokio::net::TcpStream>,
    tag: u8,
    b: &[u8],
) {
    use tokio::io::AsyncWriteExt;
    let mut out = vec![];
    giggles_server::pgwire::frame(tag, b, &mut out).unwrap();
    t.write_all(&out).await.unwrap();
    t.flush().await.unwrap();
}
fn pg_simple(a: TaskAction) -> String {
    let r = match a {
        TaskAction::Status(operation) => giggles_server::Request::Status { operation },
        TaskAction::Preview(request) => giggles_server::Request::Preview { request },
        TaskAction::Execute(request, approval) => {
            giggles_server::Request::Execute { request, approval }
        }
    };
    format!("GIGGLES {}", serde_json::to_string(&r).unwrap())
}
fn pg_value(v: &str) -> Value {
    serde_json::from_str(v).unwrap()
}
fn pg_pause(p: &PgSetup, stage: &str, id: u64) {
    std::fs::write(
        p.n.s
            .root
            .join("pgwire-hooks")
            .join(format!("pause-{stage}-{id}")),
        b"pause",
    )
    .unwrap();
}
async fn pg_reached(p: &PgSetup, stage: &str, id: u64) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(4);
    while !p
        .n
        .s
        .root
        .join("pgwire-hooks")
        .join(format!("reached-{stage}-{id}"))
        .exists()
    {
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}
fn pg_resume(p: &PgSetup, stage: &str, id: u64) {
    std::fs::write(
        p.n.s
            .root
            .join("pgwire-hooks")
            .join(format!("resume-{stage}-{id}")),
        b"resume",
    )
    .unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn external_pgwire_simple_typed_key_range_status_and_original_receipt() {
    let p = pg_setup();
    let (mut nodes, l) = native_start(&p.n, limits(100, 100, 100000), 10);
    let c = pg_client(&p, l).await;
    let preview = c
        .simple_query(&pg_simple(TaskAction::Preview(request(9000, &[0]))))
        .await
        .unwrap();
    let row = preview
        .iter()
        .find_map(|m| {
            if let tokio_postgres::SimpleQueryMessage::Row(r) = m {
                Some(r)
            } else {
                None
            }
        })
        .unwrap();
    assert_eq!(pg_value(row.get(0).unwrap())["code"], "preview");
    let s = c
        .prepare("GIGGLES EXECUTE KEY ($1,$2,$3,$4,$5)")
        .await
        .unwrap();
    assert_eq!(
        s.params(),
        &[
            tokio_postgres::types::Type::BYTEA,
            tokio_postgres::types::Type::BYTEA,
            tokio_postgres::types::Type::BYTEA,
            tokio_postgres::types::Type::TEXT,
            tokio_postgres::types::Type::BYTEA
        ]
    );
    let op = 9000u64.to_be_bytes().to_vec();
    let tenant = 42u64.to_be_bytes().to_vec();
    let key = 0u64.to_be_bytes().to_vec();
    let no: Option<Vec<u8>> = None;
    let preview_key = c
        .prepare("GIGGLES PREVIEW KEY ($1,$2,$3,$4)")
        .await
        .unwrap();
    let rows = c
        .query(&preview_key, &[&op, &tenant, &key, &"queued"])
        .await
        .unwrap();
    assert_eq!(pg_value(rows[0].get(0))["count"], 1);
    let rows = c
        .query(&s, &[&op, &tenant, &key, &"queued", &no])
        .await
        .unwrap();
    let result = pg_value(rows[0].get(0));
    assert_eq!(result["code"], "committed");
    assert_eq!(result["operation"], 9000);
    let status = c.prepare("GIGGLES STATUS ($1)").await.unwrap();
    let rows = c.query(&status, &[&op]).await.unwrap();
    assert_eq!(pg_value(rows[0].get(0)), result);
    let range = c
        .prepare("GIGGLES PREVIEW RANGE ($1,$2,$3,$4,$5,$6)")
        .await
        .unwrap();
    let rows = c
        .query(
            &range,
            &[
                &9001u64.to_be_bytes().to_vec(),
                &tenant,
                &"failed",
                &100i64,
                &107i64,
                &"queued",
            ],
        )
        .await
        .unwrap();
    assert_eq!(pg_value(rows[0].get(0))["code"], "preview");
    let execute_range = c
        .prepare("GIGGLES EXECUTE RANGE ($1,$2,$3,$4,$5,$6,$7)")
        .await
        .unwrap();
    let rows = c
        .query(
            &execute_range,
            &[
                &9001u64.to_be_bytes().to_vec(),
                &tenant,
                &"failed",
                &100i64,
                &107i64,
                &"queued",
                &no,
            ],
        )
        .await
        .unwrap();
    assert_eq!(pg_value(rows[0].get(0))["code"], "committed");
    assert_eq!(pg_value(rows[0].get(0))["count"], 3);
    assert!(
        resources(&mut nodes, l)["used"]["encoded_bytes"]
            .as_u64()
            .unwrap()
            > 0
    );
    std::fs::write(
        p.n.s.root.join("pgwire-results.json"),
        serde_json::to_vec_pretty(&result).unwrap(),
    )
    .unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn external_pgwire_repeated_portal_fresh_charges_and_sync_error_recovery() {
    let p = pg_setup();
    let (mut nodes, l) = native_start(&p.n, limits(100, 100, 100000), 10);
    let mut t = pg_raw(&p, l).await;
    let mut parse = b"s\0GIGGLES STATUS ($1)\0".to_vec();
    parse.extend(0u16.to_be_bytes());
    pg_send(&mut t, b'P', &parse).await;
    assert_eq!(pg_read(&mut t).await.0, b'1');
    let mut bind = b"p\0s\0".to_vec();
    bind.extend(1u16.to_be_bytes());
    bind.extend(1u16.to_be_bytes());
    bind.extend(1u16.to_be_bytes());
    bind.extend(8u32.to_be_bytes());
    bind.extend(9100u64.to_be_bytes());
    bind.extend(0u16.to_be_bytes());
    pg_send(&mut t, b'B', &bind).await;
    assert_eq!(pg_read(&mut t).await.0, b'2');
    let mut execute = b"p\0".to_vec();
    execute.extend(0u32.to_be_bytes());
    let before = resources(&mut nodes, l);
    let mut payload = None;
    for _ in 0..2 {
        pg_send(&mut t, b'E', &execute).await;
        let (tag, row) = pg_read(&mut t).await;
        assert_eq!(tag, b'D');
        if let Some(old) = &payload {
            assert_eq!(old, &row);
        }
        payload = Some(row);
        assert_eq!(pg_read(&mut t).await.0, b'C');
    }
    let after = resources(&mut nodes, l);
    assert_eq!(
        after["used"]["disclosed_rows"].as_u64().unwrap()
            - before["used"]["disclosed_rows"].as_u64().unwrap(),
        2
    );
    let mut bad = b"p\0".to_vec();
    bad.extend(1u32.to_be_bytes());
    pg_send(&mut t, b'E', &bad).await;
    let (tag, b) = pg_read(&mut t).await;
    assert_eq!(tag, b'E');
    assert!(b.windows(6).any(|w| w == b"0A000\0"));
    pg_send(&mut t, b'E', &execute).await;
    pg_send(&mut t, b'S', &[]).await;
    assert_eq!(pg_read(&mut t).await.0, b'Z');
    pg_send(&mut t, b'E', &execute).await;
    assert_eq!(pg_read(&mut t).await.0, b'E');
    pg_send(&mut t, b'S', &[]).await;
    assert_eq!(pg_read(&mut t).await.0, b'Z');
    let q = pg_simple(TaskAction::Status(giggles_core::OperationId(9100))) + "\0";
    pg_send(&mut t, b'Q', q.as_bytes()).await;
    assert_eq!(pg_read(&mut t).await.0, b'T');
    assert_eq!(pg_read(&mut t).await.0, b'D');
    assert_eq!(pg_read(&mut t).await.0, b'C');
    assert_eq!(pg_read(&mut t).await.0, b'Z');
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn external_pgwire_native_last_allowance_and_held_revoke() {
    let p = pg_setup();
    let (mut nodes, l) = native_start(&p.n, limits(100, 1, 100000), 10);
    let c = pg_client(&p, l).await;
    let a = pg_simple(TaskAction::Status(giggles_core::OperationId(9200)));
    let b = nr(9201, TaskAction::Status(giggles_core::OperationId(9201)));
    let (pg, native) = tokio::join!(c.simple_query(&a), nc(&p.n, l, &b));
    let native_ok = native.reply.is_some_and(|r| r.data["code"] == "absent");
    assert_eq!(usize::from(pg.is_ok()) + usize::from(native_ok), 1);
    assert_eq!(resources(&mut nodes, l)["used"]["disclosed_rows"], 1);
    drop(c);
    let p = pg_setup();
    let (mut nodes, l) = native_start(&p.n, limits(100, 100, 100000), 10);
    let c = pg_client(&p, l).await;
    pg_pause(&p, "held", 9202);
    let query = tokio::spawn(async move {
        c.simple_query(&pg_simple(TaskAction::Execute(request(9202, &[0]), None)))
            .await
    });
    pg_reached(&p, "held", 9202).await;
    assert_eq!(
        nodes
            .get_mut(&l)
            .unwrap()
            .request(9299, admin(json!({"command":"revoke","task":1})))["reply"]["code"],
        0
    );
    pg_resume(&p, "held", 9202);
    assert!(query.await.unwrap().is_err());
    assert_eq!(resources(&mut nodes, l)["used"]["disclosed_rows"], 1);
}

fn limits(read: u64, rows: u64, bytes: u64) -> Value {
    json!({"read_rows":read,"disclosed_rows":rows,"encoded_bytes":bytes})
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn external_pgwire_cancel_preproposal_postproposal_and_fresh_context() {
    let p = pg_setup();
    let (mut nodes, l) = native_start(&p.n, limits(100, 100, 100000), 10);
    let c = pg_client(&p, l).await;
    for (id, stage, expected) in [(9300, "before", 0), (9301, "held", 1)] {
        pg_pause(&p, stage, id);
        let token = c.cancel_token();
        let q = pg_simple(TaskAction::Execute(request(id, &[0]), None));
        let query = c.simple_query(&q);
        tokio::pin!(query);
        tokio::select! {r=&mut query=>panic!("premature result {r:?}"),_=pg_reached(&p,stage,id)=>{}}
        let stream = tokio::net::TcpStream::connect(&p.addresses[&l])
            .await
            .unwrap();
        let mut maker = tokio_postgres_rustls::MakeRustlsConnect::new(pg_tls(&p));
        let connector =
            tokio_postgres::tls::MakeTlsConnect::<tokio::net::TcpStream>::make_tls_connect(
                &mut maker,
                "localhost",
            )
            .unwrap();
        token.cancel_query_raw(stream, connector).await.unwrap();
        let e = query.await.unwrap_err();
        assert_eq!(e.code().unwrap().code(), "08007");
        pg_resume(&p, stage, id);
        assert_eq!(
            inspect(&mut nodes, l)["tasks"][0][1]["transitions"],
            expected
        );
        // A new query on the same backend gets a fresh cancellation context.
        let rows = c
            .simple_query(&pg_simple(TaskAction::Status(giggles_core::OperationId(
                id,
            ))))
            .await
            .unwrap();
        let row = rows
            .iter()
            .find_map(|m| {
                if let tokio_postgres::SimpleQueryMessage::Row(r) = m {
                    Some(r)
                } else {
                    None
                }
            })
            .unwrap();
        assert_eq!(
            pg_value(row.get(0).unwrap())["code"],
            if expected == 0 { "absent" } else { "committed" }
        );
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn external_pgwire_lost_reply_failover_status_and_no_quorum() {
    let p = pg_setup();
    let (mut nodes, l) = native_start(&p.n, limits(100, 100, 100000), 10);
    let follower = VOTERS.into_iter().find(|v| *v != l).unwrap();
    wait_binding(nodes.get_mut(&follower).unwrap(), &p.n.binding.fingerprint);
    let c = pg_client(&p, follower).await;
    let e = c
        .simple_query(&pg_simple(TaskAction::Status(giggles_core::OperationId(
            9400,
        ))))
        .await
        .unwrap_err();
    assert_eq!(e.code().unwrap().code(), "55000");
    drop(c);
    let c = pg_client(&p, l).await;
    pg_pause(&p, "held", 9400);
    let query = tokio::spawn(async move {
        c.simple_query(&pg_simple(TaskAction::Execute(request(9400, &[0]), None)))
            .await
    });
    pg_reached(&p, "held", 9400).await;
    assert_eq!(inspect(&mut nodes, l)["tasks"][0][1]["transitions"], 1);
    nodes.get_mut(&l).unwrap().kill();
    drop(nodes.remove(&l));
    assert!(query.await.unwrap().is_err());
    pg_resume(&p, "held", 9400);
    let successor = find_leader(&mut nodes);
    let c = pg_client(&p, successor).await;
    let s = c.prepare("GIGGLES STATUS ($1)").await.unwrap();
    let rows = c
        .query(&s, &[&9400u64.to_be_bytes().to_vec()])
        .await
        .unwrap();
    let receipt = pg_value(rows[0].get(0));
    assert_eq!(receipt["operation"], 9400);
    assert_eq!(receipt["code"], "committed");
    let retry = c
        .simple_query(&pg_simple(TaskAction::Execute(request(9400, &[0]), None)))
        .await
        .unwrap();
    let row = retry
        .iter()
        .find_map(|m| {
            if let tokio_postgres::SimpleQueryMessage::Row(r) = m {
                Some(r)
            } else {
                None
            }
        })
        .unwrap();
    assert_eq!(pg_value(row.get(0).unwrap()), receipt);
    assert_eq!(
        inspect(&mut nodes, successor)["tasks"][0][1]["transitions"],
        1
    );
    let other = *nodes.keys().find(|v| **v != successor).unwrap();
    nodes.get_mut(&other).unwrap().kill();
    drop(nodes.remove(&other));
    assert!(
        c.query(&s, &[&9400u64.to_be_bytes().to_vec()])
            .await
            .is_err()
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn external_pgwire_metadata_reload_aba_and_held_isolation() {
    let p = pg_setup();
    let (mut nodes, l) = native_start(&p.n, limits(100, 100, 100000), 10);
    let c = pg_client(&p, l).await;
    let s = c.prepare("GIGGLES STATUS ($1)").await.unwrap();
    assert_eq!(
        control(
            nodes.get_mut(&l).unwrap(),
            9501,
            json!({"op":"reload_credentials","expected":1,"bindings":[]})
        )["reply"]["data"]["epoch"],
        2
    );
    assert_eq!(
        control(
            nodes.get_mut(&l).unwrap(),
            9502,
            json!({"op":"reload_credentials","expected":2,"bindings":[p.n.binding]})
        )["reply"]["data"]["epoch"],
        3
    );
    assert!(
        c.query(&s, &[&9500u64.to_be_bytes().to_vec()])
            .await
            .is_err()
    );
    assert_eq!(resources(&mut nodes, l)["used"]["disclosed_rows"], 0);
    drop(c);
    let c = pg_client(&p, l).await;
    pg_pause(&p, "held", 9503);
    let query = tokio::spawn(async move {
        c.simple_query(&pg_simple(TaskAction::Status(giggles_core::OperationId(
            9503,
        ))))
        .await
    });
    pg_reached(&p, "held", 9503).await;
    for node in nodes.values_mut() {
        node.send(json!({"op":"pause_peers","paused":true}));
        node.until(|v| v["peers_paused"] == true, Duration::from_secs(2));
    }
    pg_resume(&p, "held", 9503);
    assert!(query.await.unwrap().is_err());
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn external_pgwire_raw_corpus_plaintext_frames_metadata_and_recovery() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let p = pg_setup();
    let (mut nodes, l) = native_start(&p.n, limits(100, 100, 100000), 10);
    let mut t = pg_raw(&p, l).await;
    let mut cases = vec![
        (
            b'P',
            b"s\0GIGGLES STATUS ($1)\0\0\x01\0\0\0\x19".to_vec(),
            "0A000",
        ),
        (b'P', b"s\0GIGGLES STATUS ($2)\0\0\0".to_vec(), "42601"),
        (b'Q', b"BEGIN\0".to_vec(), "0A000"),
        (b'Q', b"GIGGLES {\"kind\":\"Ping\"}\0".to_vec(), "0A000"),
        (b'Q', b"GIGGLES {\"op\":\"Admin\"}\0".to_vec(), "08P01"),
        (b'P', b"s\0GIGGLES STATUS ($1)\0\0".to_vec(), "08P01"),
    ];
    for (tag, b, code) in cases.drain(..) {
        pg_send(&mut t, tag, &b).await;
        let (got, e) = pg_read(&mut t).await;
        assert_eq!(got, b'E');
        assert!(e.windows(code.len()).any(|v| v == code.as_bytes()), "{e:?}");
        if tag == b'Q' {
            assert_eq!(pg_read(&mut t).await.0, b'Z');
        } else {
            pg_send(&mut t, b'S', &[]).await;
            assert_eq!(pg_read(&mut t).await.0, b'Z');
        }
    }
    let mut parse = b"s\0GIGGLES STATUS ($1)\0".to_vec();
    parse.extend(0u16.to_be_bytes());
    pg_send(&mut t, b'P', &parse).await;
    assert_eq!(pg_read(&mut t).await.0, b'1');
    for (bytes, code) in [(vec![0xff; 8], "23502"), (vec![0, 0, 0, 1, 0], "08P01")] {
        let mut b = b"p\0s\0\0\0".to_vec();
        if code == "23502" {
            b.extend(1u16.to_be_bytes());
            b.extend(u32::MAX.to_be_bytes());
            b.extend(0u16.to_be_bytes());
        } else {
            b.extend(bytes);
        }
        pg_send(&mut t, b'B', &b).await;
        let (tag, e) = pg_read(&mut t).await;
        assert_eq!(tag, b'E');
        assert!(e.windows(5).any(|w| w == code.as_bytes()));
        pg_send(&mut t, b'S', &[]).await;
        assert_eq!(pg_read(&mut t).await.0, b'Z');
    }
    assert_eq!(resources(&mut nodes, l)["used"]["disclosed_rows"], 0);
    // Oversized frame closes before allocating payload.
    t.write_u8(b'P').await.unwrap();
    t.write_u32(65537).await.unwrap();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), t.read_u8()).await,
        Ok(Err(_))
    ));
    let mut plain = tokio::net::TcpStream::connect(&p.addresses[&l])
        .await
        .unwrap();
    plain.write_all(&[0, 0, 0, 8, 0, 3, 0, 0]).await.unwrap();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), plain.read_u8()).await,
        Ok(Err(_))
    ));
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn external_pgwire_certificate_expiry_unauthorized_peer_and_spoofed_cancel() {
    let mut p = pg_setup();
    // Task and peer trust bundles overlap deliberately; role denial must be explicit.
    let bundle = p.n.client.ca.clone();
    let mut pem = std::fs::read_to_string(&bundle).unwrap();
    pem.push_str(&p.n.s.ca.pem());
    std::fs::write(&bundle, pem).unwrap();
    let (mut nodes, l) = native_start(&p.n, limits(100, 100, 100000), 10);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let mut params = CertificateParams::new(vec!["localhost".into()]).unwrap();
    params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    params.not_before = rcgen::date_time_ymd(1970, 1, 1) + Duration::from_secs(now - 10);
    params.not_after = rcgen::date_time_ymd(1970, 1, 1) + Duration::from_secs(now + 3);
    let key = KeyPair::generate().unwrap();
    let cert = params.signed_by(&key, &p.n.s.ca, &p.n.s.ca_key).unwrap();
    let short = authority::Binding {
        fingerprint: fingerprint(cert.der()),
        ..p.n.binding.clone()
    };
    let peer = authority::Binding {
        fingerprint: p.n.s.config.peers[&1].fingerprint.clone(),
        ..p.n.binding.clone()
    };
    assert_eq!(
        control(
            nodes.get_mut(&l).unwrap(),
            9600,
            json!({"op":"reload_credentials","expected":1,"bindings":[p.n.binding,short,peer]})
        )["reply"]["data"]["epoch"],
        2
    );
    let original = p.n.client.clone();
    p.n.client.certificate = p.n.s.config.certificate.clone();
    p.n.client.key = p.n.s.config.key.clone();
    let tcp = tokio::net::TcpStream::connect(&p.addresses[&l])
        .await
        .unwrap();
    let mut maker = tokio_postgres_rustls::MakeRustlsConnect::new(pg_tls(&p));
    let tls = tokio_postgres::tls::MakeTlsConnect::<tokio::net::TcpStream>::make_tls_connect(
        &mut maker,
        "localhost",
    )
    .unwrap();
    assert!(
        tokio_postgres::Config::new()
            .user("owner")
            .connect_raw(tcp, tls)
            .await
            .is_err()
    );
    p.n.client = original.clone();
    let c = pg_client(&p, l).await;
    let token = c.cancel_token();
    pg_pause(&p, "held", 9601);
    let q = pg_simple(TaskAction::Status(giggles_core::OperationId(9601)));
    let query = c.simple_query(&q);
    tokio::pin!(query);
    tokio::select! {r=&mut query=>panic!("premature {r:?}"),_=pg_reached(&p,"held",9601)=>{}}
    // Another CA-valid, committed task certificate cannot use this backend key.
    let cp = p.n.s.root.join("pg-short.pem");
    let kp = p.n.s.root.join("pg-short.key.pem");
    std::fs::write(&cp, cert.pem()).unwrap();
    std::fs::write(&kp, key.serialize_pem()).unwrap();
    p.n.client.certificate = cp.clone();
    p.n.client.key = kp.clone();
    let tcp = tokio::net::TcpStream::connect(&p.addresses[&l])
        .await
        .unwrap();
    let mut maker = tokio_postgres_rustls::MakeRustlsConnect::new(pg_tls(&p));
    let tls = tokio_postgres::tls::MakeTlsConnect::<tokio::net::TcpStream>::make_tls_connect(
        &mut maker,
        "localhost",
    )
    .unwrap();
    token.cancel_query_raw(tcp, tls).await.unwrap();
    pg_resume(&p, "held", 9601);
    assert!(query.await.is_ok());
    let c = pg_client(&p, l).await;
    pg_pause(&p, "held", 9602);
    let query = tokio::spawn(async move {
        c.simple_query(&pg_simple(TaskAction::Status(giggles_core::OperationId(
            9602,
        ))))
        .await
    });
    pg_reached(&p, "held", 9602).await;
    while std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        <= now + 3
    {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    pg_resume(&p, "held", 9602);
    assert!(query.await.unwrap().is_err());
    p.n.client = original;
    assert_eq!(
        control(
            nodes.get_mut(&l).unwrap(),
            9603,
            json!({"op":"reload_credentials","expected":2,"bindings":[]})
        )["reply"]["data"]["epoch"],
        3
    );
    let tcp = tokio::net::TcpStream::connect(&p.addresses[&l])
        .await
        .unwrap();
    let mut maker = tokio_postgres_rustls::MakeRustlsConnect::new(pg_tls(&p));
    let tls = tokio_postgres::tls::MakeTlsConnect::<tokio::net::TcpStream>::make_tls_connect(
        &mut maker,
        "localhost",
    )
    .unwrap();
    assert!(
        tokio_postgres::Config::new()
            .user("owner")
            .connect_raw(tcp, tls)
            .await
            .is_err()
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn external_pgwire_shared_session_cap_reclaim_and_preproposal_deadline() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let p = pg_setup();
    let (mut nodes, l) = native_start(&p.n, limits(100, 100, 100000), 10);
    let mut sessions = vec![];
    for _ in 0..native::MAX_SESSIONS {
        sessions.push(pg_raw(&p, l).await);
    }
    let mut config = p.n.client.clone();
    config.addresses = vec![p.n.addresses[&l].clone()];
    assert!(
        native::call(
            &config,
            &nr(9700, TaskAction::Status(giggles_core::OperationId(9700)))
        )
        .await
        .is_err()
    );
    // Explicit TLS shutdown plus socket drop; a new connection proves reclamation.
    for mut s in sessions {
        let _ = s.shutdown().await;
    }
    let c = pg_client(&p, l).await;
    pg_pause(&p, "before", 9701);
    let query = tokio::spawn(async move {
        c.simple_query(&pg_simple(TaskAction::Execute(request(9701, &[0]), None)))
            .await
    });
    pg_reached(&p, "before", 9701).await;
    assert!(query.await.unwrap().is_err());
    pg_resume(&p, "before", 9701);
    assert_eq!(inspect(&mut nodes, l)["tasks"][0][1]["transitions"], 0);
    assert_eq!(resources(&mut nodes, l)["used"]["disclosed_rows"], 0);
    let mut s = pg_raw(&p, l).await;
    s.write_u8(b'P').await.unwrap();
    s.write_u32(100).await.unwrap();
    s.write_all(b"partial").await.unwrap();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(6), s.read_u8()).await,
        Ok(Err(_))
    ));
    let _fresh = pg_raw(&p, l).await;
}

// Observe the exact prerequisite, rather than assuming leader apply means a
// follower has applied the credential reload. No sleep is evidence of catch-up.
fn wait_binding(node: &mut Running, fingerprint: &str) {
    let deadline = Instant::now() + Duration::from_secs(4);
    loop {
        node.send(json!({"op":"authenticate","fingerprint":fingerprint}));
        let v = node.until(
            |v| v.get("session").is_some() || v.get("error").is_some(),
            Duration::from_secs(2),
        );
        if v.get("session").is_some() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "follower did not apply binding: {v}"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}
