// Authenticated TLS controls for format0 Text domain admission only.
async fn state_cycle(
    s: &mut tokio_rustls::client::TlsStream<TcpStream>,
    query: &str,
) -> Vec<(u8, Vec<u8>)> {
    let mut q = query.as_bytes().to_vec();
    q.push(0);
    pg_send(s, b'Q', &q).await;
    let mut frames = vec![];
    loop {
        let frame = pg_read(s).await;
        let end = frame.0 == b'Z';
        frames.push(frame);
        if end {
            return frames;
        }
    }
}
fn text_nul_bind(portal: &str, value: Option<&[u8]>, format: u16) -> Vec<u8> {
    let mut b = Vec::new();
    cstr(portal, &mut b);
    cstr("nul_stmt", &mut b);
    b.extend(1u16.to_be_bytes());
    b.extend(format.to_be_bytes());
    b.extend(1u16.to_be_bytes());
    if let Some(value) = value {
        b.extend((value.len() as i32).to_be_bytes());
        b.extend(value);
    } else {
        b.extend((-1i32).to_be_bytes());
    }
    b.extend(0u16.to_be_bytes());
    b
}
async fn text_nul_prepare(
    s: &mut tokio_rustls::client::TlsStream<TcpStream>,
    query: &str,
    oid: u32,
) {
    pg_send(s, b'P', &parse_oids("nul_stmt", query, &[oid])).await;
    pg_send(s, b'D', b"Snul_stmt\0").await;
    let frames = pg_sync(s).await;
    assert!(!frames.iter().any(|f| f.0 == b'E'), "{frames:?}");
}
async fn text_nul_cycle(
    s: &mut tokio_rustls::client::TlsStream<TcpStream>,
    value: Option<&[u8]>,
    format: u16,
) -> Vec<(u8, Vec<u8>)> {
    pg_send(s, b'B', &text_nul_bind("nul_portal", value, format)).await;
    pg_send(s, b'E', &execute("nul_portal", 0)).await;
    pg_sync(s).await
}
#[tokio::test]
async fn text_nul_actual_tls_domain_and_precedence() {
    let f = Fixture::new().await;
    let before = f.admin.snapshot().unwrap();
    let mut raw = raw_pg(&f).await;
    let mut report = vec![];
    for (id, q, oid, value, format, state) in [
        (
            "unicode",
            "SELECT $1::text",
            25,
            Some("\u{e9}\u{1f600}".as_bytes()),
            0,
            None,
        ),
        ("empty", "SELECT $1::text", 25, Some(&b""[..]), 0, None),
        ("valid", "SELECT $1::text", 25, Some(&b"valid"[..]), 0, None),
        ("null", "SELECT $1::text", 25, None, 0, None),
        (
            "nul",
            "SELECT $1::text",
            25,
            Some(&b"a\0b"[..]),
            0,
            Some("22021"),
        ),
        (
            "strict_null",
            "SELECT overlay($1::text,NULL::text,1)",
            25,
            Some(&b"a\0b"[..]),
            0,
            Some("22021"),
        ),
        (
            "integer_nul",
            "SELECT $1::integer",
            23,
            Some(&b"1\0"[..]),
            0,
            Some("22P02"),
        ),
        (
            "invalid_utf8",
            "SELECT $1::text",
            25,
            Some(&b"\xff"[..]),
            0,
            Some("22021"),
        ),
        (
            "binary_text_nul",
            "SELECT $1::text",
            25,
            Some(&b"a\0b"[..]),
            1,
            None,
        ),
        (
            "binary_bytea",
            "SELECT $1::bytea",
            17,
            Some(&b"\0\xff"[..]),
            1,
            None,
        ),
    ] {
        text_nul_prepare(&mut raw, q, oid).await;
        let frames = text_nul_cycle(&mut raw, value, format).await;
        if let Some(state) = state {
            assert!(has_code(&frames, state), "{id}: {frames:?}");
            assert!(
                !frames.iter().any(|f| matches!(f.0, b'2' | b'D' | b'C')),
                "{id}"
            );
            pg_send(&mut raw, b'E', &execute("nul_portal", 0)).await;
            let absent = pg_sync(&mut raw).await;
            assert!(has_code(&absent, "34000"), "{id}: {absent:?}");
        } else {
            assert!(!frames.iter().any(|f| f.0 == b'E'), "{id}: {frames:?}");
            assert!(frames.iter().any(|f| f.0 == b'2'));
            assert!(frames.iter().any(|f| f.0 == b'D'));
            let expected = if id == "binary_bytea" {
                Some(&b"\\x00ff"[..])
            } else {
                value
            };
            let mut payload = 1u16.to_be_bytes().to_vec();
            if let Some(v) = expected {
                payload.extend((v.len() as i32).to_be_bytes());
                payload.extend(v);
            } else {
                payload.extend((-1i32).to_be_bytes());
            }
            assert!(
                frames.iter().any(|f| f.0 == b'D' && f.1 == payload),
                "{id}: {frames:?}"
            );
        }
        pg_send(&mut raw, b'C', b"Snul_stmt\0").await;
        let closed = pg_sync(&mut raw).await;
        assert!(!closed.iter().any(|f| f.0 == b'E'));
        report.push(serde_json::json!({"id":id,"frames":frames.iter().map(|f|serde_json::json!({"tag":char::from(f.0),"hex":hex::encode(&f.1)})).collect::<Vec<_>>()}));
    }
    // Parse before BEGIN makes the subsequent failed-transaction check a Bind check.
    text_nul_prepare(&mut raw, "INSERT INTO app(id,body) VALUES(840001,$1)", 25).await;
    assert!(
        !state_cycle(&mut raw, "BEGIN")
            .await
            .iter()
            .any(|f| f.0 == b'E')
    );
    assert!(
        !state_cycle(
            &mut raw,
            "INSERT INTO app(id,body) VALUES(840000,'rollback')"
        )
        .await
        .iter()
        .any(|f| f.0 == b'E')
    );
    let bad = text_nul_cycle(&mut raw, Some(b"a\0b"), 0).await;
    assert!(has_code(&bad, "22021"));
    assert!(bad.iter().any(|f| f.0 == b'Z' && f.1 == b"E"));
    let aborted = text_nul_cycle(&mut raw, Some(b"a\0b"), 0).await;
    assert!(has_code(&aborted, "25P02"), "{aborted:?}");
    assert!(!aborted.iter().any(|f| f.0 == b'2'));
    assert!(
        !state_cycle(&mut raw, "ROLLBACK")
            .await
            .iter()
            .any(|f| f.0 == b'E')
    );
    assert_eq!(f.admin.snapshot().unwrap(), before);
    pg_send(&mut raw, b'C', b"Snul_stmt\0").await;
    let closed = pg_sync(&mut raw).await;
    assert!(!closed.iter().any(|f| f.0 == b'E'));
    // Table authority rejects at Parse before queued malformed Bind decoding.
    pg_send(
        &mut raw,
        b'P',
        &parse_oids("nul_stmt", "SELECT body FROM other WHERE body=$1", &[25]),
    )
    .await;
    pg_send(
        &mut raw,
        b'B',
        &text_nul_bind("nul_portal", Some(b"a\0b"), 0),
    )
    .await;
    pg_send(&mut raw, b'E', &execute("nul_portal", 0)).await;
    let denied = pg_sync(&mut raw).await;
    assert!(has_code(&denied, "42501"), "{denied:?}");
    assert!(!denied.iter().any(|f| matches!(f.0, b'1' | b'2' | b'D')));
    let mut task = prepared_network::raw_task(&f).await;
    // Task authority rejects at Parse even when malformed bytes are already queued for Bind.
    pg_send(
        &mut task,
        b'P',
        &parse_oids("nul_stmt", "SELECT $1::text", &[25]),
    )
    .await;
    pg_send(
        &mut task,
        b'B',
        &text_nul_bind("nul_portal", Some(b"a\0b"), 0),
    )
    .await;
    pg_send(&mut task, b'E', &execute("nul_portal", 0)).await;
    let task_denied = pg_sync(&mut task).await;
    assert!(has_code(&task_denied, "42501"), "{task_denied:?}");
    assert!(
        !task_denied
            .iter()
            .any(|f| matches!(f.0, b'1' | b'2' | b'D'))
    );
    assert_eq!(f.admin.snapshot().unwrap(), before);
    if let Ok(path) = std::env::var("DD68_TEXT_NUL_REPORT") {
        std::fs::write(path,serde_json::to_vec_pretty(&serde_json::json!({"cases":report,"transaction_nul":bad,"transaction_aborted":aborted,"denied":denied,"task_denied":task_denied})).unwrap()).unwrap();
    }
    pg_send(&mut task, b'X', &[]).await;
    drop(task);
    pg_send(&mut raw, b'X', &[]).await;
    drop(raw);
    f.stop().await;
}
