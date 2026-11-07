#[tokio::test]
async fn dd68_negative_scalar_routing_tls_redaction_and_authority() {
    use sha2::{Digest, Sha256};
    let f = Fixture::new().await;
    let (pg, conn) = f.pg(f.app.clone()).await;
    let sql = "SELECT private_probe('never_log_this_secret')";
    // Simple query and extended Parse must preserve the same negative contract.
    let simple = pg.simple_query(sql).await.unwrap_err();
    let extended = pg.prepare(sql).await.unwrap_err();
    for error in [&simple, &extended] {
        let error = error.as_db_error().unwrap();
        assert_eq!(error.code().code(), "0A000");
        assert!(error.message().contains("unsupported SQL feature"));
        let detail = error.detail().unwrap();
        assert!(detail.contains("phase=catalog planning"));
        assert!(detail.contains(&hex::encode(Sha256::digest(sql.as_bytes()))));
        for secret in ["private_probe", "never_log_this_secret", "SELECT"] {
            assert!(!error.message().contains(secret));
            assert!(!detail.contains(secret));
        }
    }
    assert_eq!(
        pg.query_one("SELECT lower('ABC'),abs(-7)", &[])
            .await
            .unwrap()
            .get::<_, String>(0),
        "abc"
    );
    for sql in [
        "SELECT SUM(abs(-1))",
        "SELECT COUNT(lower('x'))",
        "SELECT lower(body)",
        "SELECT lower(body) FROM other",
        "SELECT lower(app.body) FROM app JOIN other ON app.id=other.id",
    ] {
        assert_eq!(
            code(pg.query(sql, &[]).await.unwrap_err()),
            "42501",
            "{sql}"
        );
    }
    drop(pg);
    conn.await.unwrap();
    f.stop().await;
}

#[tokio::test]
async fn dd68_negative_scalar_task_authority_precedes_routine_classification() {
    let f = Fixture::new().await;
    for (role, config) in [("task", f.task.clone()), ("other", f.other.clone())] {
        let (pg, conn) = f.pg(config).await;
        assert_eq!(
            code(pg.query("SELECT lower('ABC')", &[]).await.unwrap_err()),
            "42501",
            "role={role}"
        );
        for sql in [
            "SELECT private_probe('never_log_this_secret')",
            "SELECT pg_catalog.unknown_x(1)",
            "SELECT \"ASCII\"('a')",
        ] {
            let simple = pg.simple_query(sql).await.unwrap_err();
            let extended = pg.prepare(sql).await.unwrap_err();
            for (method, error) in [("simple_query", &simple), ("prepare", &extended)] {
                let error = error.as_db_error().unwrap();
                assert_eq!(
                    error.code().code(),
                    "42501",
                    "role={role} method={method} sql={sql}"
                );
                for secret in ["private_probe", "never_log_this_secret", "SELECT"] {
                    assert!(!error.message().contains(secret));
                    assert!(!error.detail().unwrap_or("").contains(secret));
                }
            }
        }
        drop(pg);
        conn.await.unwrap();
    }
    f.stop().await;
}

async fn dd68_raw_role_pg(
    f: &Fixture,
    config: Arc<ClientConfig>,
) -> tokio_rustls::client::TlsStream<TcpStream> {
    let mut s = raw_tls(f, config, true).await;
    let mut startup = 196608u32.to_be_bytes().to_vec();
    startup.extend(b"user\0giggles\0database\0giggles\0\0");
    s.write_u32((startup.len() + 4) as u32).await.unwrap();
    s.write_all(&startup).await.unwrap();
    while pg_read(&mut s).await.0 != b'Z' {}
    s
}

async fn dd68_assert_negative_scalar_pipeline(
    s: &mut tokio_rustls::client::TlsStream<TcpStream>,
    role: &str,
    expected: &str,
    absent: &str,
) {
    for sql in [
        "SELECT private_probe('never_log_this_secret')",
        "SELECT pg_catalog.unknown_x(1)",
    ] {
        for statement_describe in [false, true] {
            pg_send(s, b'P', &parse("", sql)).await;
            if statement_describe {
                pg_send(s, b'D', b"S\0").await;
            } else {
                pg_send(s, b'B', &bind("", "", &[], false)).await;
                pg_send(s, b'D', b"P\0").await;
                pg_send(s, b'E', &execute("", 0)).await;
            }
            let frames = pg_sync(s).await;
            let evidence: Vec<_> = frames
                .iter()
                .map(|(tag, body)| (char::from(*tag), String::from_utf8_lossy(body).into_owned()))
                .collect();
            assert!(
                has_code(&frames, expected),
                "role={role} sql={sql} statement_describe={statement_describe} expected={expected} frames={evidence:?}"
            );
            assert!(
                !has_code(&frames, absent),
                "role={role} sql={sql} statement_describe={statement_describe} forbidden={absent} frames={evidence:?}"
            );
            assert_eq!(
                frames.iter().filter(|(tag, _)| *tag == b'E').count(),
                1,
                "role={role} frames={evidence:?}"
            );
            assert_eq!(
                frames.last().map(|(tag, _)| *tag),
                Some(b'Z'),
                "role={role} frames={evidence:?}"
            );
        }
    }
}

#[tokio::test]
async fn dd68_negative_scalar_task_raw_extended_pipeline_authority() {
    let f = Fixture::new().await;
    for (role, config) in [("task", f.task.clone()), ("other", f.other.clone())] {
        let mut s = dd68_raw_role_pg(&f, config).await;
        dd68_assert_negative_scalar_pipeline(&mut s, role, "42501", "0A000").await;
        drop(s);
    }
    f.stop().await;
}

#[tokio::test]
async fn dd68_negative_scalar_app_raw_extended_pipeline_keeps_unsupported() {
    let f = Fixture::new().await;
    let mut s = raw_pg(&f).await;
    dd68_assert_negative_scalar_pipeline(&mut s, "app", "0A000", "42501").await;
    drop(s);
    f.stop().await;
}

#[tokio::test]
async fn dd68_table_free_keepalive_unchanged_by_role() {
    let f = Fixture::new().await;
    for (role, config) in [
        ("app", f.app.clone()),
        ("task", f.task.clone()),
        ("other", f.other.clone()),
    ] {
        let (pg, conn) = f.pg(config).await;
        if role == "app" {
            let simple = pg.simple_query("SELECT 1").await.unwrap();
            let rows: Vec<_> = simple
                .iter()
                .filter_map(|message| match message {
                    tokio_postgres::SimpleQueryMessage::Row(row) => Some(row.get(0).unwrap()),
                    _ => None,
                })
                .collect();
            assert_eq!(rows, ["1"]);
            let rows = pg.query("SELECT 1", &[]).await.unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].get::<_, i32>(0), 1);
        } else {
            // Measured on HEAD before writing these assertions: both task roles
            // reject table-free keepalives on simple and extended query paths.
            assert_eq!(
                code(pg.simple_query("SELECT 1").await.unwrap_err()),
                "42501",
                "role={role} simple_query"
            );
            assert_eq!(
                code(pg.query("SELECT 1", &[]).await.unwrap_err()),
                "42501",
                "role={role} query"
            );
        }
        drop(pg);
        conn.await.unwrap();
    }
    f.stop().await;
}
