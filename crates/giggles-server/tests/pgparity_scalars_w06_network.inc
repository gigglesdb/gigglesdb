#[tokio::test]
async fn pgparity_scalars_w06_extended_simple_and_transaction_recovery() {
    let fixture = Fixture::new().await;
    let (client, connection) = fixture.pg(fixture.app.clone()).await;
    let simple = client
        .simple_query("SELECT 2 ^ 3 ^ 2, 1 & 2 | 4, 'ABC' ~~* 'a%', 'a'||'b', ROW(1,NULL) IS NULL")
        .await
        .unwrap();
    let row = simple
        .iter()
        .find_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .expect("expected a row");
    assert_eq!(
        (row.get(0), row.get(1), row.get(2), row.get(3), row.get(4)),
        (Some("64"), Some("4"), Some("t"), Some("ab"), Some("f"))
    );

    let numbers = client
        .prepare(
            "SELECT $1::float8 ^ 2, $2 & 3, $2 << 2, 3 BETWEEN SYMMETRIC $2 AND $3,
                width_bucket($1,0,10,5), ROW($2,1) < ROW($3,0)",
        )
        .await
        .unwrap();
    let row = client
        .query_one(&numbers, &[&5.0f64, &5i32, &1i32])
        .await
        .unwrap();
    assert_eq!(row.get::<_, f64>(0), 25.0);
    assert_eq!(row.get::<_, i32>(1), 1);
    assert_eq!(row.get::<_, i32>(2), 20);
    assert!(row.get::<_, bool>(3));
    assert_eq!(row.get::<_, i32>(4), 3);
    assert!(!row.get::<_, bool>(5));

    let strings = client
        .prepare("SELECT nullif($1,''), normalize($1,NFC), upper($1), initcap($1)")
        .await
        .unwrap();
    let row = client
        .query_one(&strings, &[&"e\u{301} ÉCLAIR"])
        .await
        .unwrap();
    assert_eq!(row.get::<_, &str>(0), "e\u{301} ÉCLAIR");
    assert_eq!(row.get::<_, &str>(1), "é ÉCLAIR");
    assert_eq!(row.get::<_, &str>(2), "E\u{301} ÉCLAIR");
    assert_eq!(row.get::<_, &str>(3), "E\u{301} Éclair");
    let row = client.query_one(&strings, &[&""]).await.unwrap();
    assert_eq!(row.get::<_, Option<&str>>(0), None);

    let error = client
        .simple_query("SELECT width_bucket(1,0,10,0)")
        .await
        .unwrap_err();
    let error = error.as_db_error().unwrap();
    assert_eq!(error.code().code(), "2201G");
    assert_eq!(error.message(), "count must be greater than zero");
    assert_eq!(
        client
            .query_one("SELECT width_bucket(5,0,10,5)", &[])
            .await
            .unwrap()
            .get::<_, i32>(0),
        3
    );
    client.batch_execute("BEGIN").await.unwrap();
    assert_eq!(
        code(
            client
                .simple_query("SELECT width_bucket(1,0,10,0)")
                .await
                .unwrap_err()
        ),
        "2201G"
    );
    assert_eq!(
        code(client.simple_query("SELECT 1").await.unwrap_err()),
        "25P02"
    );
    client.batch_execute("ROLLBACK").await.unwrap();
    assert_eq!(
        client
            .query_one("SELECT 1", &[])
            .await
            .unwrap()
            .get::<_, i32>(0),
        1
    );
    drop(client);
    connection.await.unwrap();
    fixture.stop().await;
}

#[tokio::test]
async fn pgparity_scalars_w06_review_adjacency_and_diagnostics() {
    let fixture = Fixture::new().await;
    let (client, connection) = fixture.pg(fixture.app.clone()).await;
    for (sql, expected) in [
        ("SELECT 1*-2, 1+-2, 1/-2, 1<<-1, 1<-2", vec!["-2", "-1", "0", "-2147483648", "f"]),
        ("SELECT 1--2", vec!["1"]),
        ("SELECT 2*/*comment*/-3, 2+/*comment*/3, 2+--comment\n3", vec!["-6", "5", "5"]),
        ("SELECT a<-1, a<=-1, a<>-1, a=-1, x>=+1 FROM (VALUES (0,1)) AS v(a,x)",
         vec!["f", "f", "t", "f", "t"]),
        (r#"SELECT j->>'k', j->'k', j#>'{a}', j#>>'{a}' FROM
             (VALUES ('{"k":"x","a":1}'::jsonb)) AS v(j)"#, vec!["x", "\"x\"", "1", "1"]),
        ("SELECT a~b, a!~c, a~*d, a!~*e FROM (VALUES ('a','a','b','A','B')) AS v(a,b,c,d,e)",
         vec!["t", "t", "t", "t"]),
        (r#"SELECT unistr('\0130') ~~* 'i', unistr('\0130') ILIKE 'i',
             unistr('\00C9') COLLATE "C" ~~* unistr('\00E9'),
             unistr('\00C9') COLLATE "C" ILIKE unistr('\00E9')"#, vec!["t", "t", "f", "f"]),
    ] {
        let result = client.simple_query(sql).await.unwrap_or_else(|err| panic!("{sql}: {err:?}"));
        let row = result.iter().find_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        }).unwrap();
        assert_eq!((0..row.len()).map(|i| row.get(i).unwrap()).collect::<Vec<_>>(), expected, "{sql}");
    }
    for sql in [
        "SELECT a@>b FROM (VALUES (ARRAY[1,2],ARRAY[1])) AS v(a,b)",
        "SELECT b<@a FROM (VALUES (ARRAY[1,2],ARRAY[1])) AS v(a,b)",
    ] {
        let error = client.simple_query(sql).await.unwrap_err();
        assert_eq!(error.as_db_error().unwrap().code().code(), "0A000", "{sql}");
    }
    assert_eq!(client.query_one("SELECT $1::int", &[&3i32]).await.unwrap().get::<_, i32>(0), 3);
    drop(client);
    connection.await.unwrap();
    // Table-free scalars the catalog engine declines reach core only for a
    // superuser (W0-1, 81ed8b565); Application identities keep 0A000 by the
    // fc080f790 contract. Run the error probes as a superuser database role.
    let (client, connection) = w06_superuser(&fixture).await;
    for (sql, state, message) in [
        ("SELECT 2^-2", "42883", "operator does not exist: integer ^- integer"),
        ("SELECT 1&~2", "42883", "operator does not exist: integer &~ integer"),
        ("SELECT a!=-1 FROM (VALUES (0)) AS v(a)", "42883", "operator does not exist: integer !=- integer"),
        ("SELECT 'a'||-1", "42883", "operator does not exist: unknown ||- integer"),
        ("SELECT 1%-2", "42883", "operator does not exist: integer %- integer"),
        ("SELECT @-5", "42883", "operator does not exist: @- integer"),
        ("SELECT |/-4", "42883", "operator does not exist: |/- integer"),
        ("SELECT ~-1", "42883", "operator does not exist: ~- integer"),
        ("SELECT -~1", "42883", "operator does not exist: -~ integer"),
        ("SELECT num_nulls(VARIADIC 1)", "42804", "VARIADIC argument must be an array"),
        (r"SELECT unistr('\D800\+110000')", "22023", "invalid Unicode code point: 110000"),
        ("SELECT pg_catalog.normalize('a','bad')", "22023", "invalid normalization form: bad"),
    ] {
        let simple = client.simple_query(sql).await.unwrap_err();
        let extended = client.query(sql, &[]).await.unwrap_err();
        for error in [simple, extended] {
            let error = error.as_db_error().unwrap();
            assert_eq!(error.code().code(), state, "{sql}");
            assert_eq!(error.message(), message, "{sql}");
        }
    }
    drop(client);
    connection.await.unwrap().unwrap();
    fixture.stop().await;
}

async fn w06_superuser(
    f: &Fixture,
) -> (tokio_postgres::Client, tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>) {
    use giggles_core::roles::NewRole;
    use giggles_server::auth::{PostgresqlConfig, TlsMode, serve_postgresql};
    f.admin
        .roles()
        .create(NewRole {
            name: "w06_owner".into(),
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
    let (client, connection) = tokio_postgres::Config::new()
        .host("127.0.0.1")
        .port(server.address.port())
        .user("w06_owner")
        .dbname("postgres")
        .connect(tokio_postgres::NoTls)
        .await
        .unwrap();
    // The listener must outlive the connection; leak it for the test's duration.
    std::mem::forget(server);
    (client, tokio::spawn(connection))
}
