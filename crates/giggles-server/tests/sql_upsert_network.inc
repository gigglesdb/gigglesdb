// Uses the authenticated Application-role harness, with the unique index installed
// by the trusted admin before the listener starts.
struct SqlUpsertFixture {
    pg: Running,
    app: Arc<ClientConfig>,
}

impl SqlUpsertFixture {
    async fn new() -> Self {
        let path = PathBuf::from("artifacts").join(format!(
            "sql-upsert-network-{}-{}-{}",
            std::process::id(),
            now(),
            SERIAL.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).unwrap();
        println!("retained_fixture={}", path.display());
        let admin = Admin::create(path.join("db.redb"), Limits::default()).unwrap();
        let mut sql = admin.sql_session();
        sql.execute(
            "CREATE TABLE app (id BIGINT PRIMARY KEY, body TEXT, flag BOOLEAN, small SMALLINT, num INTEGER, data BYTEA)",
            &[],
            None,
        )
        .unwrap();
        sql.execute(
            "CREATE UNIQUE INDEX sql_upsert_body ON app (body)",
            &[],
            None,
        )
        .unwrap();
        drop(sql);

        let ca = cert(None, false);
        let server = cert(Some(&ca), true);
        let app = cert(Some(&ca), false);
        let domain = admin.authority_metadata().unwrap().0.to_string();
        let service = Service::new(
            admin,
            vec![Binding {
                fingerprint: tls::fingerprint(app.cert.der()),
                role: Role::Application {
                    domain,
                    tables: BTreeSet::from(["app".into()]),
                },
            }],
            Clock::new(0, 3600).unwrap(),
        )
        .unwrap();
        let server_config = tls::server(
            vec![server.cert.der().clone()],
            PrivatePkcs8KeyDer::from(server.key.serialize_der()).into(),
            vec![ca.cert.der().clone()],
        )
        .unwrap();
        let pg = serve_pg("127.0.0.1:0".parse().unwrap(), service, server_config, 8)
            .await
            .unwrap();
        Self {
            pg,
            app: client(&app, &ca),
        }
    }

    async fn client(&self) -> (tokio_postgres::Client, tokio::task::JoinHandle<()>) {
        let (client, connection) = tokio_postgres::Config::new()
            .host("localhost")
            .hostaddr(self.pg.address.ip())
            .port(self.pg.address.port())
            .user("giggles")
            .dbname("giggles")
            .ssl_mode(tokio_postgres::config::SslMode::Require)
            .connect(tokio_postgres_rustls::MakeRustlsConnect::new(
                (*self.app).clone(),
            ))
            .await
            .unwrap();
        (
            client,
            tokio::spawn(async move {
                connection.await.unwrap();
            }),
        )
    }

    async fn raw(&self) -> tokio_rustls::client::TlsStream<TcpStream> {
        let mut socket = TcpStream::connect(self.pg.address).await.unwrap();
        socket
            .write_all(&[0, 0, 0, 8, 4, 210, 22, 47])
            .await
            .unwrap();
        assert_eq!(socket.read_u8().await.unwrap(), b'S');
        let mut raw = tokio_rustls::TlsConnector::from(self.app.clone())
            .connect(
                rustls::pki_types::ServerName::try_from("localhost").unwrap(),
                socket,
            )
            .await
            .unwrap();
        let mut startup = 196608u32.to_be_bytes().to_vec();
        startup.extend(b"user\0giggles\0database\0giggles\0\0");
        raw.write_u32((startup.len() + 4) as u32).await.unwrap();
        raw.write_all(&startup).await.unwrap();
        loop {
            let frame = pg_read(&mut raw).await;
            assert_ne!(frame.0, b'E', "startup: {frame:?}");
            if frame.0 == b'Z' {
                break;
            }
        }
        raw
    }

    async fn stop(self) {
        self.pg.shutdown().await.unwrap();
    }
}

async fn sql_upsert_simple(
    raw: &mut tokio_rustls::client::TlsStream<TcpStream>,
    query: &str,
) -> Vec<(u8, Vec<u8>)> {
    let mut body = Vec::new();
    cstr(query, &mut body);
    pg_send(raw, b'Q', &body).await;
    let mut frames = Vec::new();
    loop {
        let frame = pg_read(raw).await;
        let done = frame.0 == b'Z';
        frames.push(frame);
        if done {
            return frames;
        }
    }
}

fn sql_upsert_tag(frames: &[(u8, Vec<u8>)]) -> &str {
    assert!(frames.iter().all(|frame| frame.0 != b'E'), "{frames:?}");
    let commands = frames
        .iter()
        .filter(|frame| frame.0 == b'C')
        .collect::<Vec<_>>();
    assert_eq!(commands.len(), 1, "{frames:?}");
    let body = commands[0].1.strip_suffix(&[0]).unwrap();
    std::str::from_utf8(body).unwrap()
}

#[tokio::test]
async fn sql_upsert_simple_multi_row_command_tag() {
    let f = SqlUpsertFixture::new().await;
    let mut raw = f.raw().await;
    let frames = sql_upsert_simple(
        &mut raw,
        "INSERT INTO app (id, body) VALUES (1, 'one'), (2, 'two'), (3, 'three')",
    )
    .await;
    assert_eq!(sql_upsert_tag(&frames), "INSERT 0 3");
    let (client, connection) = f.client().await;
    let rows = client
        .query("SELECT id FROM app ORDER BY id LIMIT 10", &[])
        .await
        .unwrap();
    assert_eq!(
        rows.iter()
            .map(|row| row.get::<_, i64>(0))
            .collect::<Vec<_>>(),
        [1, 2, 3]
    );
    drop(client);
    connection.await.unwrap();
    drop(raw);
    f.stop().await;
}

#[tokio::test]
async fn sql_upsert_extended_prepare_parameters_update_values_and_tag() {
    let f = SqlUpsertFixture::new().await;
    let (client, connection) = f.client().await;
    client
        .execute("INSERT INTO app (id, body, num) VALUES (1, 'old', 10)", &[])
        .await
        .unwrap();
    let query = "INSERT INTO app (id, body, num) VALUES ($1, $2, $3), ($4, $5, $6) ON CONFLICT (id) DO UPDATE SET body = EXCLUDED.body, num = EXCLUDED.num";
    let prepared = client.prepare(query).await.unwrap();
    assert_eq!(
        prepared.params(),
        &[
            tokio_postgres::types::Type::INT8,
            tokio_postgres::types::Type::TEXT,
            tokio_postgres::types::Type::INT4,
            tokio_postgres::types::Type::INT8,
            tokio_postgres::types::Type::TEXT,
            tokio_postgres::types::Type::INT4,
        ]
    );
    assert_eq!(
        client
            .execute(
                &prepared,
                &[&1i64, &"updated", &20i32, &2i64, &"inserted", &30i32]
            )
            .await
            .unwrap(),
        2
    );
    let rows = client
        .query("SELECT id, body, num FROM app ORDER BY id LIMIT 10", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(
        (
            rows[0].get::<_, i64>(0),
            rows[0].get::<_, String>(1),
            rows[0].get::<_, i32>(2)
        ),
        (1, "updated".into(), 20)
    );
    assert_eq!(
        (
            rows[1].get::<_, i64>(0),
            rows[1].get::<_, String>(1),
            rows[1].get::<_, i32>(2)
        ),
        (2, "inserted".into(), 30)
    );

    // The pinned driver's execute API exposes only the count. Inspect the actual
    // CommandComplete text in an additional extended-protocol execution.
    let mut raw = f.raw().await;
    pg_send(&mut raw, b'P', &parse("", query)).await;
    let parsed = pg_sync(&mut raw).await;
    assert!(parsed.iter().all(|frame| frame.0 != b'E'), "{parsed:?}");
    pg_send(
        &mut raw,
        b'B',
        &bind(
            "",
            "",
            &[
                Some("1"),
                Some("updated"),
                Some("20"),
                Some("2"),
                Some("inserted"),
                Some("30"),
            ],
            false,
        ),
    )
    .await;
    pg_send(&mut raw, b'E', &execute("", 0)).await;
    assert_eq!(sql_upsert_tag(&pg_sync(&mut raw).await), "INSERT 0 2");
    drop(raw);
    drop(client);
    connection.await.unwrap();
    f.stop().await;
}

#[tokio::test]
async fn sql_upsert_no_target_do_nothing_command_tag() {
    let f = SqlUpsertFixture::new().await;
    let mut raw = f.raw().await;
    assert_eq!(
        sql_upsert_tag(
            &sql_upsert_simple(&mut raw, "INSERT INTO app (id, body) VALUES (1, 'one')").await
        ),
        "INSERT 0 1"
    );
    for query in [
        "INSERT INTO app (id, body) VALUES (1, 'other') ON CONFLICT DO NOTHING",
        "INSERT INTO app (id, body) VALUES (2, 'one') ON CONFLICT DO NOTHING",
    ] {
        assert_eq!(
            sql_upsert_tag(&sql_upsert_simple(&mut raw, query).await),
            "INSERT 0 0"
        );
    }
    drop(raw);
    f.stop().await;
}

#[tokio::test]
async fn sql_upsert_plain_duplicate_sqlstate_and_atomicity() {
    let f = SqlUpsertFixture::new().await;
    let (client, connection) = f.client().await;
    client
        .execute("INSERT INTO app (id, body) VALUES (1, 'one')", &[])
        .await
        .unwrap();
    for query in [
        "INSERT INTO app (id, body) VALUES (2, 'two'), (1, 'different')",
        "INSERT INTO app (id, body) VALUES (2, 'two'), (3, 'one')",
    ] {
        assert_eq!(code(client.simple_query(query).await.unwrap_err()), "23505");
        let rows = client
            .query("SELECT id FROM app ORDER BY id LIMIT 10", &[])
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get::<_, i64>(0), 1);
    }
    drop(client);
    connection.await.unwrap();
    f.stop().await;
}

#[tokio::test]
async fn sql_upsert_returning_unsupported_sqlstate() {
    let f = SqlUpsertFixture::new().await;
    let (client, connection) = f.client().await;
    for query in [
        "INSERT INTO app (id, body) VALUES (1, 'one'), (2, 'two') RETURNING id",
        "INSERT INTO app (id, body) VALUES (1, 'one') ON CONFLICT DO NOTHING RETURNING id",
    ] {
        assert_eq!(code(client.simple_query(query).await.unwrap_err()), "0A000");
        assert!(
            client
                .query("SELECT id FROM app LIMIT 10", &[])
                .await
                .unwrap()
                .is_empty()
        );
    }
    drop(client);
    connection.await.unwrap();
    f.stop().await;
}
