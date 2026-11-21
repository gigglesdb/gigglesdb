use super::*;
use giggles_server::admission::ServiceLimits;
use std::time::Duration;
fn bindings(f: &Fixture) -> Vec<Binding> {
    serde_json::from_slice(&std::fs::read(f.path.join("bindings.json")).unwrap()).unwrap()
}
fn fp(f: &Fixture, name: &str) -> String {
    tls::fingerprint(&tls::certs(f.path.join(format!("{name}.pem"))).unwrap()[0])
}
async fn eof<S: tokio::io::AsyncRead + Unpin>(s: &mut S) {
    assert!(
        tokio::time::timeout(Duration::from_secs(5), s.read_u8())
            .await
            .unwrap()
            .is_err()
    );
}
async fn assert_revoked(client: &mut Client, request: proto::ExecuteRequest) {
    // Reload wakes idle wire sessions. Read the notification before writing so
    // a TCP reset from writing during teardown cannot mask the wire error.
    let rejected = tokio::time::timeout(Duration::from_secs(5), client.recv())
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(wire_error(&rejected).code, "UNAUTHENTICATED");
    let closed = tokio::time::timeout(Duration::from_secs(5), client.execute(request))
        .await
        .unwrap()
        .unwrap_err();
    assert!(
        matches!(
            closed.kind(),
            io::ErrorKind::UnexpectedEof
                | io::ErrorKind::ConnectionReset
                | io::ErrorKind::ConnectionAborted
                | io::ErrorKind::BrokenPipe
                | io::ErrorKind::NotConnected
        ),
        "revoked session should close: {closed:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_overlap_new_certificate_both_transports_and_old_revoked() {
    let f = Fixture::new().await;
    let mut old = f.native("app").await;
    let (pg, conn) = f.pg(f.app.clone()).await;
    let prepared = pg.prepare("SELECT id FROM app LIMIT 1").await.unwrap();
    let unbound = f.try_native("unbound").await.err().expect("unbound certificate");
    assert_eq!(wire_error(&unbound).code, "UNAUTHENTICATED");
    let mut b = bindings(&f);
    b.push(Binding {
        fingerprint: fp(&f, "unbound"),
        role: b[0].role.clone(),
    });
    f.service.reload_bindings(b.clone()).unwrap();
    let mut rotated = f.native("unbound").await;
    let (newpg, newconn) = f.pg(f.unbound.clone()).await;
    assert_eq!(
        old.execute(sql("INSERT INTO app (id) VALUES (9)", vec![]))
            .await
            .unwrap()
            .code,
        "ok"
    );
    assert_eq!(
        newpg
            .query("SELECT id FROM app WHERE id=9", &[])
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(rotated.execute(ping()).await.unwrap().code, "ok");
    b.retain(|entry| entry.fingerprint != fp(&f, "app"));
    f.service.reload_bindings(b).unwrap();
    assert_revoked(&mut old, sql("DELETE FROM app WHERE id=9", vec![])).await;
    assert!(pg.query(&prepared, &[]).await.is_err());
    assert_eq!(
        newpg
            .query("SELECT id FROM app WHERE id=9", &[])
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(rotated.execute(ping()).await.unwrap().code, "ok");
    drop(pg);
    drop(newpg);
    conn.await.unwrap();
    newconn.await.unwrap();
    f.stop().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn revoke_before_bind_and_after_cached_application_page_never_releases_rows() {
    for cached in [false, true] {
        let f = Fixture::new().await;
        f.admin
            .sql_session()
            .execute("INSERT INTO app (id) VALUES (1)", &[], None)
            .unwrap();
        f.admin
            .sql_session()
            .execute("INSERT INTO app (id) VALUES (2)", &[], None)
            .unwrap();
        let mut s = raw_pg(&f).await;
        pg_send(&mut s, b'Q', b"BEGIN\0").await;
        while pg_read(&mut s).await.0 != b'Z' {}
        pg_send(&mut s, b'P', &parse("s", "SELECT id FROM app LIMIT 2")).await;
        assert_eq!(pg_sync(&mut s).await[0].0, b'1');
        if cached {
            pg_send(&mut s, b'B', &bind("p", "s", &[], false)).await;
            pg_send(&mut s, b'E', &execute("p", 1)).await;
            let frames = pg_sync(&mut s).await;
            assert!(frames.iter().any(|(tag, _)| *tag == b'D'));
            assert!(frames.iter().any(|(tag, _)| *tag == b's'));
        }
        f.service.reload_bindings(vec![]).unwrap();
        pg_send(
            &mut s,
            if cached { b'E' } else { b'B' },
            &if cached {
                execute("p", 1)
            } else {
                bind("p", "s", &[], false)
            },
        )
        .await;
        eof(&mut s).await;
        f.stop().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn invalid_reload_retains_live_driver_and_native_and_restart_uses_new_bindings() {
    let f = Fixture::new().await;
    let mut n = f.native("app").await;
    let (pg, conn) = f.pg(f.app.clone()).await;
    let mut bad = bindings(&f);
    bad.push(bad[0].clone());
    assert!(f.service.reload_bindings(bad).is_err());
    assert_eq!(n.execute(ping()).await.unwrap().code, "ok");
    assert!(
        pg.query("SELECT id FROM app LIMIT 1", &[])
            .await
            .unwrap()
            .is_empty()
    );
    let mut b = bindings(&f);
    b.retain(|entry| entry.fingerprint != fp(&f, "app"));
    f.service.reload_bindings(b.clone()).unwrap();
    let restarted = Service::new(f.admin.clone(), b, Clock::new(0, 3600).unwrap()).unwrap();
    assert!(restarted.session(&fp(&f, "app")).is_err());
    assert!(restarted.session(&fp(&f, "task")).is_ok());
    drop(pg);
    conn.await.unwrap();
    f.stop().await;
}

pub(super) async fn limited(f: &Fixture, limits: ServiceLimits) -> (Arc<Service>, Running, Running) {
    let service = Service::with_limits(
        f.admin.clone(),
        bindings(f),
        Clock::new(0, 3600).unwrap(),
        limits,
    )
    .unwrap();
    let n = start_wire(service.clone(), f.server_config.clone(), 128).await;
    let p = serve_pg(
        "127.0.0.1:0".parse().unwrap(),
        service.clone(),
        f.server_config.clone(),
        128,
    )
    .await
    .unwrap();
    (service, n, p)
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn aggregate_native_session_saturation_caps_postgres_and_cancel_socket() {
    let f = Fixture::new().await;
    let (_service, n, p) = limited(
        &f,
        ServiceLimits {
            sessions: 1,
            burst: 1000,
            per_second: 1000,
        },
    )
    .await;
    let mut first = connect_wire(n.address, wire_tls(&f.path, Some("app")).unwrap())
        .await
        .unwrap();
    assert_eq!(first.execute(ping()).await.unwrap().code, "ok");
    let mut denied = TcpStream::connect(p.address).await.unwrap();
    // Shared cap rejects before SSL negotiation, including CancelRequest connections.
    eof(&mut denied).await;
    assert_eq!(first.execute(ping()).await.unwrap().code, "ok");
    drop(first);
    n.shutdown().await.unwrap();
    p.shutdown().await.unwrap();
    f.stop().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shared_rate_native_simple_prepared_and_malformed_frames_cannot_evade() {
    let f = Fixture::new().await;
    let (_service, n, p) = limited(
        &f,
        ServiceLimits {
            sessions: 8,
            burst: 8,
            per_second: 1,
        },
    )
    .await;
    let mut native = connect_wire(n.address, wire_tls(&f.path, Some("app")).unwrap())
        .await
        .unwrap(); //1
    let (pg, conn) = prepared_network::connect_at(p.address, f.app.clone()).await; //2+startup3
    assert!(pg.simple_query("SELECT id FROM app LIMIT 1").await.is_ok()); //4
    let prepared = pg
        .prepare("INSERT INTO app (id) VALUES ($1)")
        .await
        .unwrap(); //Parse5 Describe6 Sync7
    assert_eq!(native.execute(ping()).await.unwrap().code, "ok"); //8
    assert!(pg.execute(&prepared, &[&44i64]).await.is_err()); //Bind denied, no execution
    let rejected = native
        .execute(sql("INSERT INTO app (id) VALUES (45)", vec![]))
        .await
        .unwrap_err();
    assert_eq!(wire_error(&rejected).code, "RESOURCE_EXHAUSTED");
    assert!(
        f.admin
            .sql_session()
            .execute("SELECT id FROM app LIMIT 10", &[], None)
            .unwrap()
            .rows
            .is_empty()
    );
    drop(pg);
    conn.await.unwrap();
    n.shutdown().await.unwrap();
    p.shutdown().await.unwrap();
    f.stop().await;
}


#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn expired_leaf_is_rejected_by_rustls_on_both_listeners() {
    let f = Fixture::new().await;
    let key = KeyPair::generate().unwrap();
    let mut params = CertificateParams::new(vec!["localhost".into()]).unwrap();
    params.not_before = rcgen::date_time_ymd(2019, 1, 1);
    params.not_after = rcgen::date_time_ymd(2020, 1, 1);
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    let expired = Cert {
        cert: params.signed_by(&key, &f.ca.cert, &f.ca.key).unwrap(),
        key,
    };
    std::fs::write(f.path.join("expired.pem"), expired.cert.pem()).unwrap();
    let mut b = bindings(&f);
    b.push(Binding {
        fingerprint: tls::fingerprint(expired.cert.der()),
        role: b[0].role.clone(),
    });
    f.service.reload_bindings(b).unwrap();
    for pg in [false, true] {
        let mut socket = TcpStream::connect(if pg { f.pg.address } else { f.native.address })
            .await
            .unwrap();
        if pg {
            socket
                .write_all(&[0, 0, 0, 8, 4, 210, 22, 47])
                .await
                .unwrap();
            assert_eq!(socket.read_u8().await.unwrap(), b'S');
        }
        let result = tokio_rustls::TlsConnector::from(client(&expired, &f.ca))
            .connect(
                rustls::pki_types::ServerName::try_from("localhost").unwrap(),
                socket,
            )
            .await;
        if let Ok(mut stream) = result {
            // TLS1.3 client completion can precede the server's client-cert alert.
            let _ = stream.write_all(&[0, 0, 0, 8, 0, 3, 0, 0]).await;
            eof(&mut stream).await;
        }
    }
    f.stop().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn executable_trusted_stdin_reload_invalid_atomicity_and_restart_config() {
    use tokio::io::{AsyncBufReadExt, BufReader};
    let f = Fixture::new().await;
    let path = std::fs::canonicalize(&f.path).unwrap();
    let mut config: serde_json::Value =
        serde_json::from_slice(&std::fs::read(path.join("server.TEST-ONLY.json")).unwrap())
            .unwrap();
    for (field, file) in [
        ("database", "db.redb"),
        ("certificate", "server.pem"),
        ("private_key", "server.key.TEST-ONLY"),
        ("client_ca", "ca.pem"),
    ] {
        config[field] = serde_json::json!(path.join(file));
    }
    config["native"] = serde_json::json!("127.0.0.1:0");
    config["postgres"] = serde_json::json!("127.0.0.1:0");
    config["wire"] = serde_json::json!("127.0.0.1:0");
    config["bindings_file"] = serde_json::json!(path.join("bindings.json"));
    let initial = bindings(&f);
    let changed = vec![initial[1].clone(), initial[2].clone()];
    std::fs::write(
        path.join("reload.TEST-ONLY.json"),
        serde_json::to_vec_pretty(&config).unwrap(),
    )
    .unwrap();
    f.stop().await;
    for restart in [false, true] {
        let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_giggles-server"))
            .arg(path.join("reload.TEST-ONLY.json"))
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        let mut output = BufReader::new(child.stdout.take().unwrap()).lines();
        let line = tokio::time::timeout(Duration::from_secs(8), output.next_line())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        std::fs::write(path.join(format!("executable-start-{restart}.txt")), &line).unwrap();
        let address = listening_address(&line, "wire").parse().unwrap();
        let mut input = child.stdin.take().unwrap();
        let native = connect_wire(address, wire_tls(&path, Some("app")).unwrap()).await;
        if !restart {
            let mut native = native.unwrap();
            assert_eq!(native.execute(ping()).await.unwrap().code, "ok");
            // Parse failure is rejected without changing a live connection.
            std::fs::write(path.join("bindings.json"), b"[").unwrap();
            input.write_all(b"reload\n").await.unwrap();
            input.flush().await.unwrap();
            let stderr = child.stderr.take().unwrap();
            let mut errors = BufReader::new(stderr).lines();
            let rejection = tokio::time::timeout(Duration::from_secs(5), errors.next_line())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert!(rejection.contains("prior bindings retained"));
            std::fs::write(path.join("executable-invalid-reload.txt"), rejection).unwrap();
            assert_eq!(native.execute(ping()).await.unwrap().code, "ok");
            std::fs::write(
                path.join("bindings.json"),
                serde_json::to_vec_pretty(&changed).unwrap(),
            )
            .unwrap();
            input.write_all(b"reload\n").await.unwrap();
            input.flush().await.unwrap();
            let ack = tokio::time::timeout(Duration::from_secs(5), output.next_line())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert!(ack.starts_with("bindings reloaded epoch="));
            std::fs::write(path.join("executable-reload-ack.txt"), ack).unwrap();
            assert_revoked(&mut native, ping()).await;
        } else {
            let rejected = native.err().expect("revoked certificate cannot reopen");
            assert_eq!(wire_error(&rejected).code, "UNAUTHENTICATED");
        }
        let mut allowed = connect_wire(address, wire_tls(&path, Some("task")).unwrap())
            .await
            .unwrap();
        assert_eq!(allowed.execute(ping()).await.unwrap().code, "ok");
        drop(allowed);
        input.write_all(b"shutdown\n").await.unwrap();
        input.flush().await.unwrap();
        drop(input);
        let status = tokio::time::timeout(Duration::from_secs(8), child.wait())
            .await
            .unwrap()
            .unwrap();
        assert!(status.success());
        std::fs::write(
            path.join(format!("executable-exit-{restart}.txt")),
            status.to_string(),
        )
        .unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn streaming_portals_do_not_retain_complete_result_vectors() {
    let f = Fixture::new().await;
    let payload = "\u{0001}".repeat(16_000);
    let mut sql = f.admin.sql_session();
    for id in 0..60i64 {
        sql.execute(
            "INSERT INTO app (id,body) VALUES ($1,$2)",
            &[Value::Int(id), Value::Text(payload.clone())],
            None,
        )
        .unwrap();
    }
    let mut s = raw_pg(&f).await;
    pg_send(&mut s, b'Q', b"BEGIN\0").await;
    while pg_read(&mut s).await.0 != b'Z' {}
    pg_send(&mut s, b'P', &parse("s", "SELECT body FROM app LIMIT 60")).await;
    pg_send(&mut s, b'B', &bind("first", "s", &[], false)).await;
    pg_send(&mut s, b'E', &execute("first", 1)).await;
    let first = pg_sync(&mut s).await;
    assert!(first.iter().any(|(tag, _)| *tag == b'D'));
    pg_send(&mut s, b'B', &bind("second", "s", &[], false)).await;
    pg_send(&mut s, b'E', &execute("second", 1)).await;
    let second = pg_sync(&mut s).await;
    assert!(second.iter().any(|(tag, _)| *tag == b'D'));
    assert!(second.iter().any(|(tag, _)| *tag == b's'));
    assert!(!has_code(&second, "54000"));
    assert_eq!(second.last(), Some(&(b'Z', vec![b'T'])));
    // Both streaming portals remain usable in the open transaction.
    pg_send(&mut s, b'Q', b"ROLLBACK\0").await;
    while pg_read(&mut s).await.0 != b'Z' {}
    assert_eq!(
        f.admin
            .sql_session()
            .execute("SELECT id FROM app LIMIT 60", &[], None)
            .unwrap()
            .rows
            .len(),
        60
    );
    f.stop().await;
}
