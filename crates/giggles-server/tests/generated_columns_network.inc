async fn generated_columns_over_pgwire(engine: giggles_core::Engine) {
    use giggles_core::roles::NewRole;
    use giggles_server::auth::{PostgresqlConfig, TlsMode, scram::Verifier, serve_postgresql};
    use tokio_postgres::SimpleQueryMessage;

    let path = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "generated-columns-{}-{}-{}",
        std::process::id(),
        now(),
        SERIAL.fetch_add(1, Ordering::Relaxed)
    ));
    let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
    admin
        .roles()
        .create(NewRole {
            name: "migration_owner".into(),
            login: true,
            superuser: true,
            password: Some(Verifier::generate("fixture-password")),
            ..NewRole::default()
        })
        .unwrap();
    let service = Service::new(admin, vec![], Clock::new(0, 3600).unwrap()).unwrap();
    let config = PostgresqlConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        tls: TlsMode::Disabled,
        certificate: None,
        private_key: None,
        client_ca: None,
        certificates: Default::default(),
        databases: vec!["postgres".into()],
        loopback_trust: false,
        connections: 8,
    };
    let running = serve_postgresql(config.listen, service, Arc::new(config.build().unwrap()))
        .await
        .unwrap();
    let (client, connection) = tokio_postgres::Config::new()
        .host("127.0.0.1")
        .port(running.address.port())
        .dbname("postgres")
        .user("migration_owner")
        .password("fixture-password")
        .ssl_mode(tokio_postgres::config::SslMode::Disable)
        .connect(tokio_postgres::NoTls)
        .await
        .unwrap();
    let connection = tokio::spawn(connection);
    client.batch_execute("
        CREATE TABLE articles (
            id integer PRIMARY KEY,
            title text,
            body text,
            keys text[],
            metadata jsonb,
            summary text GENERATED ALWAYS AS (coalesce(title,'') || ' ' || coalesce(body,'')) STORED,
            title_upper text GENERATED ALWAYS AS (upper(title)) VIRTUAL
        );
        CREATE INDEX article_keys ON articles USING GIN (keys);
        CREATE INDEX article_metadata ON articles USING GIN (metadata jsonb_path_ops) WITH (fastupdate=off);
    ").await.unwrap();
    let insert = client
        .prepare(
            "INSERT INTO articles(id,title,body) VALUES ($1,$2,$3) RETURNING summary,title_upper",
        )
        .await
        .unwrap();
    let row = client
        .query_one(&insert, &[&1i32, &"Ada", &"Lovelace"])
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>(0), "Ada Lovelace");
    assert_eq!(row.get::<_, String>(1), "ADA");
    let messages = client.simple_query("UPDATE articles SET title='Grace',body='Hopper' WHERE id=1 RETURNING summary,title_upper")
        .await.unwrap();
    assert!(messages.iter().any(|message| matches!(message,
        SimpleQueryMessage::Row(row) if row.get(0) == Some("Grace Hopper") && row.get(1) == Some("GRACE"))));
    let rows = client
        .query(
            "
        SELECT column_name,is_generated,generation_expression,column_default,is_updatable
        FROM information_schema.columns
        WHERE table_name=$1 AND is_generated='ALWAYS'
        ORDER BY ordinal_position",
            &[&"articles"],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0].get::<_, String>(2),
        "((COALESCE(title, ''::text) || ' '::text) || COALESCE(body, ''::text))"
    );
    assert_eq!(rows[0].get::<_, Option<String>>(3), None);
    assert_eq!(rows[1].get::<_, String>(2), "upper(title)");
    assert_eq!(rows[1].get::<_, String>(4), "YES");
    let row = client
        .query_one(
            "
        SELECT a.attgenerated::text, a.atthasdef, pg_get_expr(d.adbin,d.adrelid)
        FROM pg_attribute a JOIN pg_attrdef d ON a.attrelid=d.adrelid AND a.attnum=d.adnum
        WHERE a.attrelid='articles'::regclass AND a.attname='title_upper'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>(0), "v");
    assert!(row.get::<_, bool>(1));
    assert_eq!(row.get::<_, String>(2), "upper(title)");
    let row = client
        .query_one(
            "
        SELECT am.amname,opc.opcname,pg_get_indexdef(i.indexrelid)
        FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid
        JOIN pg_am am ON am.oid=c.relam JOIN pg_opclass opc ON opc.oid=i.indclass[0]
        WHERE c.relname='article_metadata'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>(0), "gin");
    assert_eq!(row.get::<_, String>(1), "jsonb_path_ops");
    assert_eq!(
        row.get::<_, String>(2),
        "CREATE INDEX article_metadata ON public.articles USING gin (metadata jsonb_path_ops) WITH (fastupdate=off)"
    );
    for error in [
        client
            .simple_query("INSERT INTO articles(id,summary) VALUES (2,'manual')")
            .await
            .unwrap_err(),
        client
            .query(
                "INSERT INTO articles(id,summary) VALUES ($1,$2)",
                &[&2i32, &"manual"],
            )
            .await
            .unwrap_err(),
    ] {
        let error = error.as_db_error().unwrap();
        assert_eq!(error.code().code(), "428C9");
        assert_eq!(
            error.message(),
            "cannot insert a non-DEFAULT value into column \"summary\""
        );
        assert_eq!(
            error.detail(),
            Some("Column \"summary\" is a generated column.")
        );
    }
    let statement = "CREATE TABLE bad_generation (a int, b int GENERATED ALWAYS AS (b + 1) STORED)";
    let error = client.simple_query(statement).await.unwrap_err();
    let error = error.as_db_error().unwrap();
    assert_eq!(error.code().code(), "42P17");
    assert_eq!(
        error.detail(),
        Some("A generated column cannot reference another generated column.")
    );
    assert!(
        matches!(error.position(), Some(tokio_postgres::error::ErrorPosition::Original(position))
        if *position as usize == statement.find("(b +").unwrap() + 2)
    );
    client
        .batch_execute("CREATE UNIQUE INDEX article_summary ON articles(summary)")
        .await
        .unwrap();
    let error = client
        .simple_query("INSERT INTO articles(id,title,body) VALUES (2,'Grace','Hopper')")
        .await
        .unwrap_err();
    let error = error.as_db_error().unwrap();
    assert_eq!(error.code().code(), "23505");
    assert_eq!(
        error.detail(),
        Some("Key (summary)=(Grace Hopper) already exists.")
    );
    drop(client);
    connection.await.unwrap().unwrap();
    running.shutdown().await.unwrap();
    std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test]
async fn generated_columns_legacy_network() {
    generated_columns_over_pgwire(giggles_core::Engine::Legacy).await;
}

#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn generated_columns_v2_network() {
    generated_columns_over_pgwire(giggles_core::Engine::V2).await;
}
