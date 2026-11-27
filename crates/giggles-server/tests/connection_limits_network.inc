// PostgreSQL 18.6 oracle: connlimits/oracle/report.txt. Real dev processes,
// configured limits, occupied TCP sessions and raw cancellation packets.
mod connection_limits {
    use super::*;
    use tokio::{io::AsyncReadExt, net::TcpStream};

    async fn frame(s: &mut TcpStream) -> (u8, Vec<u8>) {
        let tag = s.read_u8().await.unwrap();
        let n = s.read_u32().await.unwrap();
        assert!((4..=65536).contains(&n));
        let mut b = vec![0; n as usize - 4];
        s.read_exact(&mut b).await.unwrap();
        (tag, b)
    }

    async fn query(s: &mut TcpStream, sql: &str) {
        s.write_u8(b'Q').await.unwrap();
        s.write_u32((sql.len() + 5) as u32).await.unwrap();
        s.write_all(sql.as_bytes()).await.unwrap();
        s.write_u8(0).await.unwrap();
    }

    async fn cancel(port: u16, key: (u32, u32)) {
        let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        for n in [16, 80877102, key.0, key.1] { s.write_u32(n).await.unwrap(); }
        assert_eq!(s.read(&mut [0]).await.unwrap(), 0, "cancel must close silently");
    }

    async fn overflow(url: &str) {
        let error = tokio_postgres::connect(url, NoTls).await.err().expect("full listener admitted login");
        let error = error.as_db_error().unwrap();
        assert_eq!(error.code().code(), "53300");
        assert_eq!(error.severity(), "FATAL");
        assert_eq!(error.message(), "sorry, too many clients already");
    }

    async fn observe_wait(observer: &tokio_postgres::Client, _pid: u32) {
        timeout(Duration::from_secs(5), async {
            loop {
                // Advisory locks are service-owned and aren't exposed by pg_locks.
                // The target first takes this witness lock, then blocks on 99182.
                // Unlike a quiet socket, a conflicting lock proves target execution.
                let held: bool = observer.query_one("SELECT pg_try_advisory_lock(99183)", &[]).await.unwrap().get(0);
                if !held { break; }
                observer.batch_execute("SELECT pg_advisory_unlock(99183)").await.unwrap();
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.expect("target never entered advisory lock wait");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn configured_limits_cancel_at_capacity_and_report_settings() {
        timeout(BOUND * 3, async {
            for limit in [5, 7] {
                let root = fixture();
                let (server, url) = Server::start_limit(&root, true, true, Some(limit)).await;
                let (admin, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
                let driver = tokio::spawn(connection);
                admin.batch_execute("CREATE ROLE ordinary LOGIN NOSUPERUSER").await.unwrap();
                drop(admin);
                driver.await.unwrap().unwrap();
                // Fill the pool with ordinary LOGIN clients, like the oracle.
                let url = url.replace("owner:Dev-pass_1", "ordinary");
                let config: tokio_postgres::Config = url.parse().unwrap();
                let port = config.get_ports()[0];
                let mut clients = Vec::new();
                for _ in 1..limit { clients.push(connect(&url).await); }
                let observer = &clients[0];
                for sql in ["SHOW max_connections", "SELECT current_setting('max_connections')"] {
                    let rows = observer.query(sql, &[]).await.unwrap();
                    assert_eq!(rows[0].columns()[0].type_().oid(), 25);
                    assert_eq!(rows[0].get::<_, String>(0), limit.to_string());
                }
                let rows = observer.query("SELECT setting, vartype, unit, context, source, boot_val, reset_val, min_val, max_val FROM pg_settings WHERE name = 'max_connections'", &[]).await.unwrap();
                let expected = [Some(limit.to_string()), Some("integer".into()), None, Some("postmaster".into()), Some("command line".into()), Some("100".into()), Some(limit.to_string()), Some("1".into()), Some("262143".into())];
                assert_eq!(rows.len(), 1);
                for (i, value) in expected.iter().enumerate() {
                    assert_eq!(rows[0].columns()[i].type_().oid(), 25);
                    assert_eq!(&rows[0].get::<_, Option<String>>(i), value);
                }
                for sql in ["RESET ALL", "BEGIN; SAVEPOINT a; RESET ALL; ROLLBACK TO a; COMMIT", "BEGIN; RESET ALL; ROLLBACK"] {
                    observer.batch_execute(sql).await.unwrap();
                    let row = observer.query_one("SELECT setting, source, reset_val FROM pg_settings WHERE name = 'max_connections'", &[]).await.unwrap();
                    for (i, expected) in [limit.to_string(), "command line".into(), limit.to_string()].iter().enumerate() {
                        assert_eq!(row.columns()[i].type_().oid(), 25);
                        assert_eq!(&row.get::<_, String>(i), expected);
                    }
                }
                observer.batch_execute("BEGIN; SELECT pg_advisory_xact_lock(99182)").await.unwrap();
                let mut target = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
                let startup = b"\0\x03\0\0user\0ordinary\0database\0app\0\0";
                target.write_u32((startup.len() + 4) as u32).await.unwrap();
                target.write_all(startup).await.unwrap();
                let mut key = None;
                loop {
                    let (tag, b) = frame(&mut target).await;
                    if tag == b'K' { key = Some((u32::from_be_bytes(b[..4].try_into().unwrap()), u32::from_be_bytes(b[4..].try_into().unwrap()))); }
                    if tag == b'Z' { assert_eq!(b, b"I"); break; }
                    assert_ne!(tag, b'E');
                }
                let key = key.unwrap();
                overflow(&url).await;
                // A lock wait is an indefinitely running statement with deterministic
                // synchronization, without requiring a sleep implementation.
                query(&mut target, "SELECT pg_advisory_lock(99183); SELECT pg_advisory_xact_lock(99182)").await;
                observe_wait(observer, key.0).await;
                cancel(port, (key.0, key.1 ^ 1)).await;
                observe_wait(observer, key.0).await;
                // The lock witness proves dispatch; silence here additionally
                // proves the wrong-secret request did not end that query.
                assert!(timeout(Duration::from_millis(300), target.read_u8()).await.is_err());
                cancel(port, key).await;
                let error = loop {
                    let (tag, body) = frame(&mut target).await;
                    if tag == b'E' { break body; }
                    assert!(matches!(tag, b'T' | b'D' | b'C'), "unexpected pre-cancel frame {tag}");
                };
                assert!(error.split(|b| *b == 0).any(|v| v == b"SERROR"));
                assert!(error.windows(7).any(|w| w == b"C57014\0"));
                assert!(error.split(|b| *b == 0).any(|v| v == b"Mcanceling statement due to user request"));
                assert_eq!(frame(&mut target).await, (b'Z', b"I".to_vec()));
                overflow(&url).await;
                observer.batch_execute("ROLLBACK").await.unwrap();
                for c in &clients { assert_eq!(c.query_one("SELECT 1", &[]).await.unwrap().get::<_, i32>(0), 1); }
                query(&mut target, "SELECT 1").await;
                let mut row = false;
                loop {
                    let (tag, b) = frame(&mut target).await;
                    assert_ne!(tag, b'E');
                    if tag == b'D' { assert_eq!(b, b"\0\x01\0\0\0\x011"); row = true; }
                    if tag == b'Z' { assert_eq!(b, b"I"); break; }
                }
                assert!(row);

                // Oversized and malformed CancelRequest lengths cannot allocate
                // arbitrary memory or consume session admission.
                for len in [0, 7, 8193, u32::MAX] {
                    let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
                    s.write_u32(len).await.unwrap();
                    assert_eq!(s.read(&mut [0]).await.unwrap(), 0);
                }
                let mut malformed = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
                malformed.write_u32(12).await.unwrap();
                malformed.write_u32(80877102).await.unwrap();
                malformed.write_u32(key.0).await.unwrap();
                assert_eq!(malformed.read(&mut [0]).await.unwrap(), 0);
                // Eight incomplete startups fill only the rejection budget; the
                // ninth closes promptly and the first eight expire in two seconds.
                let mut slow = Vec::new();
                for _ in 0..8 {
                    let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
                    s.write_u32(8192).await.unwrap();
                    slow.push(s);
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
                let mut ninth = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
                assert_eq!(timeout(Duration::from_secs(1), ninth.read(&mut [0])).await.unwrap().unwrap(), 0);
                for mut s in slow { assert_eq!(timeout(Duration::from_secs(3), s.read(&mut [0])).await.unwrap().unwrap(), 0); }
                overflow(&url).await;
                cancel(port, (key.0, key.1 ^ 1)).await;
                drop(target);
                drop(clients);
                server.shutdown().await;
                std::fs::remove_dir_all(root).unwrap();
            }
        }).await.expect("connection-limit scenario exceeded deadline");
    }
}
