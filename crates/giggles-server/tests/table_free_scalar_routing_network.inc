#[tokio::test]
async fn table_free_scalar_routing_restricted_roles() {
    let f = Fixture::new().await;
    for (role, config, probes) in [
        (
            "app",
            &f.app,
            &[
                ("SELECT pg_typeof(1)", "0A000"),
                ("SELECT information_schema._pg_expandarray(1)", "0A000"),
            ][..],
        ),
        ("task", &f.task, &[("SELECT pg_typeof(1)", "42501")][..]),
        ("other", &f.other, &[("SELECT pg_typeof(1)", "42501")][..]),
    ] {
        let (pg, conn) = f.pg(config.clone()).await;
        for &(sql, expected) in probes {
            let simple = code(pg.simple_query(sql).await.unwrap_err());
            let extended = code(pg.prepare(sql).await.unwrap_err());
            assert_eq!(simple, expected, "role={role} simple query {sql}");
            assert_eq!(extended, expected, "role={role} extended prepare {sql}");
        }
        drop(pg);
        conn.await.unwrap();
    }
    f.stop().await;
}

#[tokio::test]
async fn table_free_scalar_routing_database_role() {
    use giggles_core::roles::NewRole;
    use giggles_server::auth::{PostgresqlConfig, TlsMode, serve_postgresql};
    use tokio_postgres::SimpleQueryMessage;

    // Use the same server fixture's service on a PostgreSQL listener with
    // loopback trust, so startup admits a Database role.
    let f = Fixture::new().await;
    f.admin
        .roles()
        .create(NewRole {
            name: "owner".into(),
            superuser: true,
            login: true,
            ..NewRole::default()
        })
        .unwrap();
    let config = PostgresqlConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        tls: TlsMode::Disabled,
        certificate: None,
        private_key: None,
        client_ca: None,
        certificates: Default::default(),
        databases: vec!["postgres".into()],
        loopback_trust: true,
        connections: 8,
    };
    let listener = Arc::new(config.build().unwrap());
    let server = serve_postgresql(config.listen, f.service.clone(), listener)
        .await
        .unwrap();
    let (pg, conn) = tokio_postgres::Config::new()
        .host("127.0.0.1")
        .port(server.address.port())
        .user("owner")
        .dbname("postgres")
        .connect(tokio_postgres::NoTls)
        .await
        .unwrap();
    let conn = tokio::spawn(conn);

    // Bind with text result format so the exact regtype results can be read as
    // PostgreSQL type names. tokio-postgres requests binary results in query().
    let mut wire = TcpStream::connect(server.address).await.unwrap();
    let mut startup = 196608u32.to_be_bytes().to_vec();
    startup.extend(b"user\0owner\0database\0postgres\0\0");
    wire.write_u32((startup.len() + 4) as u32).await.unwrap();
    wire.write_all(&startup).await.unwrap();
    loop {
        let (tag, body) = pg_read(&mut wire).await;
        assert_ne!(tag, b'E', "Database role startup failed: {body:?}");
        if tag == b'Z' {
            assert_eq!(body, b"I");
            break;
        }
    }

    const TYPEOF_INTEGER: &str = "SELECT pg_typeof(1)";
    const UNKNOWN: &str = "SELECT nosuchfn(1)";
    let cases = [
        (TYPEOF_INTEGER, &["integer"][..]),
        ("SELECT pg_typeof(NULL::text)", &["text"][..]),
        ("SELECT pg_typeof('today'::text::date)", &["date"][..]),
        ("SELECT pg_typeof(ARRAY[1,2])", &["integer[]"][..]),
        (
            "SELECT pg_typeof(timestamp with time zone '2024-01-01 00:00+00')",
            &["timestamp with time zone"][..],
        ),
        (
            "SELECT pg_typeof(1::smallint), pg_typeof('a'::text), pg_typeof(1.5)",
            &["smallint", "text", "numeric"][..],
        ),
        (
            "SELECT pg_typeof(localtimestamp)",
            &["timestamp without time zone"][..],
        ),
    ];
    for (sql, expected) in cases {
        let in_transaction = sql == "SELECT pg_typeof(localtimestamp)";
        if in_transaction {
            pg.simple_query("BEGIN").await.unwrap();
        }

        let messages = pg
            .simple_query(sql)
            .await
            .unwrap_or_else(|err| panic!("{sql}: {err:?}"));
        let mut rows = messages.iter().filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        });
        let row = rows.next().unwrap();
        assert_eq!(row.len(), expected.len(), "{sql}");
        for (column, value) in expected.iter().enumerate() {
            assert_eq!(row.get(column), Some(*value), "{sql}, column {column}");
        }
        assert!(rows.next().is_none(), "{sql}");
        if in_transaction {
            pg.simple_query("COMMIT").await.unwrap();
        }

        pg.prepare(sql)
            .await
            .unwrap_or_else(|err| panic!("{sql}: {err:?}"));
        if in_transaction {
            let frames = scalar_routing_extended(&mut wire, "BEGIN", &[]).await;
            assert!(frames.iter().all(|(tag, _)| *tag != b'E'), "{frames:?}");
            assert_eq!(frames.last().unwrap().1, b"T");
        }
        let frames = scalar_routing_extended(&mut wire, sql, &[]).await;
        assert_scalar_routing_row(&frames, expected, sql);
        assert_eq!(
            frames.last().unwrap().1,
            if in_transaction { b"T" } else { b"I" }
        );

        if in_transaction {
            let frames = scalar_routing_extended(&mut wire, "COMMIT", &[]).await;
            assert!(frames.iter().all(|(tag, _)| *tag != b'E'), "{frames:?}");
            assert_eq!(frames.last().unwrap().1, b"I");
        }
    }

    let parameter_sql = "SELECT pg_typeof($1::int)";
    pg.prepare(parameter_sql).await.unwrap();
    let frames = scalar_routing_extended(&mut wire, parameter_sql, &[Some("7")]).await;
    assert_scalar_routing_row(&frames, &["integer"], parameter_sql);
    let statement = pg.prepare("SELECT pg_typeof($1::int)::text").await.unwrap();
    let row = pg.query_one(&statement, &[&7_i32]).await.unwrap();
    assert_eq!(row.get::<_, &str>(0), "integer");

    assert_eq!(code(pg.simple_query(UNKNOWN).await.unwrap_err()), "42883");
    let messages = pg.simple_query(TYPEOF_INTEGER).await.unwrap();
    assert!(messages.iter().any(|message| {
        matches!(message, SimpleQueryMessage::Row(row) if row.get(0) == Some("integer"))
    }));
    assert_eq!(code(pg.prepare(UNKNOWN).await.unwrap_err()), "42883");

    pg.prepare(TYPEOF_INTEGER).await.unwrap();

    let frames = scalar_routing_simple(&mut wire, UNKNOWN).await;
    assert!(has_code(&frames, "42883"), "{frames:?}");
    assert_eq!(frames.last().unwrap().1, b"I");
    let frames = scalar_routing_simple(&mut wire, TYPEOF_INTEGER).await;
    assert_scalar_routing_row(&frames, &["integer"], TYPEOF_INTEGER);
    assert_eq!(frames.last().unwrap().1, b"I");

    let frames = scalar_routing_extended(&mut wire, UNKNOWN, &[]).await;
    assert!(has_code(&frames, "42883"), "{frames:?}");
    assert_eq!(frames.last().unwrap().1, b"I");
    let frames = scalar_routing_extended(&mut wire, TYPEOF_INTEGER, &[]).await;
    assert_scalar_routing_row(&frames, &["integer"], TYPEOF_INTEGER);
    assert_eq!(frames.last().unwrap().1, b"I");

    // A statement core genuinely does not evaluate reaches core on the
    // changed path and keeps core's 0A000; the session stays usable.
    {
        let sql = "SELECT pg_typeof(1), to_char(1,'9')";
        assert_eq!(code(pg.simple_query(sql).await.unwrap_err()), "0A000", "{sql}");
        assert_eq!(code(pg.prepare(sql).await.unwrap_err()), "0A000", "{sql}");
        let frames = scalar_routing_extended(&mut wire, sql, &[]).await;
        assert!(has_code(&frames, "0A000"), "{sql}: {frames:?}");
        assert_eq!(frames.last().unwrap().1, b"I");
        let frames = scalar_routing_extended(&mut wire, TYPEOF_INTEGER, &[]).await;
        assert_scalar_routing_row(&frames, &["integer"], TYPEOF_INTEGER);
    }

    assert_pg_typeof_evaluates_arguments(&pg).await;

    // An ordinary (non-superuser) database role keeps main's 0A000 for every
    // table-free scalar the catalog declines, on both protocols; so does a
    // superuser session after SET ROLE to it, and RESET ROLE restores core.
    f.admin
        .roles()
        .create(NewRole {
            name: "plain".into(),
            login: true,
            ..NewRole::default()
        })
        .unwrap();
    let (plain, plain_conn) = tokio_postgres::Config::new()
        .host("127.0.0.1")
        .port(server.address.port())
        .user("plain")
        .dbname("postgres")
        .connect(tokio_postgres::NoTls)
        .await
        .unwrap();
    let plain_conn = tokio::spawn(plain_conn);
    for sql in [
        TYPEOF_INTEGER,
        "SELECT pg_typeof($1::int)",
        "SELECT pg_typeof(1), to_char(1,'9')",
    ] {
        if !sql.contains('$') {
            assert_eq!(code(plain.simple_query(sql).await.unwrap_err()), "0A000", "plain {sql}");
        }
        assert_eq!(code(plain.prepare(sql).await.unwrap_err()), "0A000", "plain {sql}");
    }
    let bucket = "SELECT width_bucket(5, 0, 10, 5)";
    let frames = scalar_routing_extended(&mut wire, bucket, &[]).await;
    assert_scalar_routing_row(&frames, &["3"], bucket);
    for client in [&pg, &plain] {
        let messages = client.simple_query(bucket).await.unwrap();
        assert!(messages.iter().any(|m| matches!(m, SimpleQueryMessage::Row(row) if row.get(0) == Some("3"))));
        let prepared = client.prepare(bucket).await.unwrap();
        assert_eq!(client.query_one(&prepared, &[]).await.unwrap().get::<_, i32>(0), 3);
    }
    assert_eq!(
        plain.query_one("SELECT lower('ABC')", &[]).await.unwrap().get::<_, String>(0),
        "abc"
    );
    pg.simple_query("SET ROLE plain").await.unwrap();
    assert_eq!(code(pg.simple_query(TYPEOF_INTEGER).await.unwrap_err()), "0A000");
    assert_eq!(code(pg.prepare(TYPEOF_INTEGER).await.unwrap_err()), "0A000");
    pg.simple_query("RESET ROLE").await.unwrap();
    let messages = pg.simple_query(TYPEOF_INTEGER).await.unwrap();
    assert!(messages.iter().any(|message| {
        matches!(message, SimpleQueryMessage::Row(row) if row.get(0) == Some("integer"))
    }));

    drop(plain);
    plain_conn.await.unwrap().unwrap();
    drop(wire);
    drop(pg);
    conn.await.unwrap().unwrap();
    server.shutdown().await.unwrap();
    f.stop().await;
}

async fn assert_pg_typeof_evaluates_arguments(pg: &tokio_postgres::Client) {
    use tokio_postgres::SimpleQueryMessage;

    let divide = "SELECT pg_typeof(1/0)";
    assert_eq!(code(pg.simple_query(divide).await.unwrap_err()), "22012");
    assert_eq!(code(pg.query(divide, &[]).await.unwrap_err()), "22012");

    pg.simple_query("BEGIN").await.unwrap();
    assert_eq!(code(pg.simple_query(divide).await.unwrap_err()), "22012");
    assert_eq!(
        code(pg.simple_query("SELECT 1").await.unwrap_err()),
        "25P02"
    );
    pg.simple_query("ROLLBACK").await.unwrap();

    pg.batch_execute("BEGIN").await.unwrap();
    assert_eq!(code(pg.query(divide, &[]).await.unwrap_err()), "22012");
    assert_eq!(code(pg.query("SELECT 1", &[]).await.unwrap_err()), "25P02");
    pg.batch_execute("ROLLBACK").await.unwrap();

    pg.batch_execute("CREATE TABLE typeof_rows (id int primary key, x int)")
        .await
        .unwrap();
    pg.batch_execute("INSERT INTO typeof_rows VALUES (1, 0), (2, 5)")
        .await
        .unwrap();
    let table_divide = "SELECT id, pg_typeof(10/x) FROM typeof_rows ORDER BY id";
    assert_eq!(
        code(pg.simple_query(table_divide).await.unwrap_err()),
        "22012"
    );
    assert_eq!(
        code(pg.query(table_divide, &[]).await.unwrap_err()),
        "22012"
    );
    let no_rows = "SELECT pg_typeof(1/0) FROM typeof_rows WHERE false";
    assert_eq!(code(pg.query(no_rows, &[]).await.unwrap_err()), "22012");

    pg.batch_execute("CREATE SEQUENCE typeof_seq")
        .await
        .unwrap();
    let messages = pg
        .simple_query("SELECT pg_typeof(nextval('typeof_seq'))")
        .await
        .unwrap();
    assert!(messages.iter().any(|message| {
        matches!(message, SimpleQueryMessage::Row(row) if row.get(0) == Some("bigint"))
    }));
    let currval = "SELECT currval('typeof_seq')";
    assert_eq!(
        pg.query_one(currval, &[]).await.unwrap().get::<_, i64>(0),
        1
    );
    let row = pg
        .query_one("SELECT pg_typeof(nextval('typeof_seq'))::text", &[])
        .await
        .unwrap();
    assert_eq!(row.get::<_, &str>(0), "bigint");
    assert_eq!(
        pg.query_one(currval, &[]).await.unwrap().get::<_, i64>(0),
        2
    );
}

async fn scalar_routing_simple(wire: &mut TcpStream, sql: &str) -> Vec<(u8, Vec<u8>)> {
    let mut query = vec![];
    cstr(sql, &mut query);
    pg_send(wire, b'Q', &query).await;
    let mut frames = vec![];
    loop {
        let frame = pg_read(wire).await;
        let ready = frame.0 == b'Z';
        frames.push(frame);
        if ready {
            return frames;
        }
    }
}

async fn scalar_routing_extended(
    wire: &mut TcpStream,
    sql: &str,
    parameters: &[Option<&str>],
) -> Vec<(u8, Vec<u8>)> {
    pg_send(wire, b'P', &parse("", sql)).await;
    pg_send(wire, b'B', &bind("", "", parameters, false)).await;
    pg_send(wire, b'E', &execute("", 0)).await;
    pg_sync(wire).await
}

fn assert_scalar_routing_row(frames: &[(u8, Vec<u8>)], expected: &[&str], sql: &str) {
    assert!(
        frames.iter().all(|(tag, _)| *tag != b'E'),
        "{sql}: {frames:?}"
    );
    let mut row = (expected.len() as u16).to_be_bytes().to_vec();
    for value in expected {
        row.extend((value.len() as i32).to_be_bytes());
        row.extend(value.as_bytes());
    }
    let actual: Vec<_> = frames
        .iter()
        .filter(|(tag, _)| *tag == b'D')
        .map(|(_, body)| body)
        .collect();
    assert_eq!(actual, [&row], "{sql}");
}
