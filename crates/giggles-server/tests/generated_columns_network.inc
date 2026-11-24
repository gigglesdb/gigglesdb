async fn generated_columns_over_pgwire(engine: giggles_core::Engine) {
    use giggles_core::roles::NewRole;
    use giggles_server::auth::{PostgresqlConfig, TlsMode, scram::Verifier, serve_postgresql};
    use futures_core::Stream;
    use tokio_postgres::{binary_copy::BinaryCopyInWriter, types::Type};
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
    let listener = Arc::new(config.build().unwrap());
    let running = serve_postgresql(config.listen, service, listener.clone()).await.unwrap();
    let client_config = |port| {
        let mut config = tokio_postgres::Config::new();
        config.host("127.0.0.1")
            .port(port)
            .dbname("postgres")
            .user("migration_owner")
            .password("fixture-password")
            .ssl_mode(tokio_postgres::config::SslMode::Disable);
        config
    };
    let (client, connection) = client_config(running.address.port()).connect(tokio_postgres::NoTls).await.unwrap();
    let (send_notice, mut notices) = tokio::sync::mpsc::unbounded_channel();
    let mut connection = connection;
    let connection = tokio::spawn(async move {
        while let Some(message) = std::future::poll_fn(|cx| connection.poll_message(cx)).await {
            if let tokio_postgres::AsyncMessage::Notice(notice) = message? {
                let _ = send_notice.send(notice);
            }
        }
        Ok::<_, tokio_postgres::Error>(())
    });
    text_search_configuration_wire(&client).await;
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
    client.batch_execute("
        CREATE TABLE f1 (id int PRIMARY KEY, a int,
            s int GENERATED ALWAYS AS (a * 2) STORED,
            v int GENERATED ALWAYS AS (nullif(a, -10)) VIRTUAL NOT NULL);
        INSERT INTO f1 VALUES (1,5);
        CREATE TABLE only_gen (v int GENERATED ALWAYS AS (5) STORED);
        INSERT INTO only_gen DEFAULT VALUES;
    ").await.unwrap();
    {
        let copy = client.copy_in("COPY f1 FROM STDIN WITH (FORMAT binary)").await.unwrap();
        let mut copy = std::pin::pin!(BinaryCopyInWriter::new(copy, &[Type::INT4, Type::INT4]));
        copy.as_mut().write(&[&2i32, &6i32]).await.unwrap();
        assert_eq!(copy.as_mut().finish().await.unwrap(), 1);
    }
    for (sql, expected) in [
        ("COPY f1 TO STDOUT WITH (FORMAT csv, HEADER)", "id,a\n1,5\n2,6\n"),
        ("COPY f1 (a,id) TO STDOUT WITH (FORMAT csv, HEADER)", "a,id\n5,1\n6,2\n"),
        ("COPY only_gen TO STDOUT", "\n"),
        ("COPY (SELECT * FROM f1 ORDER BY id) TO STDOUT", "1\t5\t10\t5\n2\t6\t12\t6\n"),
    ] {
        let copy = client.copy_out(sql).await.unwrap();
        let mut copy = std::pin::pin!(copy);
        let mut bytes = Vec::new();
        while let Some(chunk) = std::future::poll_fn(|cx| copy.as_mut().poll_next(cx)).await {
            bytes.extend_from_slice(&chunk.unwrap());
        }
        assert_eq!(bytes, expected.as_bytes(), "{sql}");
    }
    let errors = [
        (client.copy_out("COPY f1 (v) TO STDOUT").await.err().unwrap(), "v"),
        (client.copy_out("COPY f1 TO STDOUT WITH (FORMAT csv, FORCE_QUOTE(s))").await.err().unwrap(), "s"),
        (client.copy_in::<_, bytes::Bytes>("COPY f1 (s) FROM STDIN").await.err().unwrap(), "s"),
    ];
    for (error, column) in errors {
        let error = error.as_db_error().unwrap();
        assert_eq!(error.code().code(), "42P10");
        assert_eq!(error.message(), format!("column \"{column}\" is a generated column"));
        assert_eq!(error.detail(), Some("Generated columns cannot be used in COPY."));
    }

    for (sql, state, message) in [
        ("CREATE TABLE both_default (a int DEFAULT 1 GENERATED ALWAYS AS (2) STORED)",
         "42601", "both default and generation expression specified for column \"a\" of table \"both_default\""),
        ("CREATE TABLE sub (a int GENERATED ALWAYS AS ((SELECT 1)) STORED)",
         "0A000", "cannot use subquery in column generation expression"),
    ] {
        let error = client.simple_query(sql).await.unwrap_err();
        let error = error.as_db_error().unwrap();
        assert_eq!((error.code().code(), error.message()), (state, message));
    }
    let error = client.simple_query("INSERT INTO f1 VALUES (5,-10)").await.unwrap_err();
    let error = error.as_db_error().unwrap();
    assert_eq!(error.code().code(), "23502");
    assert_eq!(error.detail(), Some("Failing row contains (5, -10, -20, virtual)."));
    client.batch_execute("CREATE TABLE f2_check (id int PRIMARY KEY, a int CHECK (a > 0),
        v int GENERATED ALWAYS AS (a * 2) VIRTUAL)").await.unwrap();
    let error = client.simple_query("INSERT INTO f2_check VALUES (1, -5)").await.unwrap_err();
    let error = error.as_db_error().unwrap();
    assert_eq!(error.code().code(), "23514");
    assert_eq!(error.detail(), Some("Failing row contains (1, -5, virtual)."));

    client.batch_execute("CREATE TABLE f2_search (id int PRIMARY KEY, body text,
        doc tsvector GENERATED ALWAYS AS (to_tsvector('english', body)) STORED)").await.unwrap();
    for (sql, input) in [
        ("COPY f2_search FROM STDIN WITH (FORMAT binary)", (1i32, "Fat cats ran")),
        ("COPY f2_search (id, body) FROM STDIN WITH (FORMAT binary)", (2i32, "Running dogs")),
    ] {
        let copy = client.copy_in(sql).await.unwrap();
        let mut copy = std::pin::pin!(BinaryCopyInWriter::new(copy, &[Type::INT4, Type::TEXT]));
        copy.as_mut().write(&[&input.0, &input.1]).await.unwrap();
        assert_eq!(copy.as_mut().finish().await.unwrap(), 1);
    }
    let rows = client.query("SELECT doc::text FROM f2_search ORDER BY id", &[]).await.unwrap();
    assert_eq!(rows.iter().map(|row| row.get::<_, String>(0)).collect::<Vec<_>>(),
        ["'cat':2 'fat':1 'ran':3", "'dog':2 'run':1"]);

    client.batch_execute("ALTER TABLE f1 ALTER COLUMN a DROP EXPRESSION IF EXISTS").await.unwrap();
    let notice = tokio::time::timeout(std::time::Duration::from_secs(5), notices.recv()).await.unwrap().unwrap();
    assert_eq!(notice.code().code(), "00000");
    assert_eq!(notice.message(), "column \"a\" of relation \"f1\" is not a generated column, skipping");

    if engine == giggles_core::Engine::V2 {
        client.batch_execute("CREATE VIEW fv AS SELECT s FROM f1; CREATE VIEW fv2 AS SELECT * FROM fv").await.unwrap();
        let error = client.simple_query("ALTER TABLE f1 DROP COLUMN s RESTRICT").await.unwrap_err();
        let error = error.as_db_error().unwrap();
        assert_eq!(error.code().code(), "2BP01");
        assert_eq!(error.detail(), Some("view fv depends on column s of table f1\nview fv2 depends on view fv"));
        let error = client.simple_query("ALTER TABLE f1 DROP COLUMN a RESTRICT").await.unwrap_err();
        let error = error.as_db_error().unwrap();
        assert_eq!(error.code().code(), "2BP01");
        assert_eq!(error.detail(), Some(concat!(
            "column s of table f1 depends on column a of table f1\n",
            "view fv depends on column s of table f1\n",
            "view fv2 depends on view fv\n",
            "column v of table f1 depends on column a of table f1",
        )));
        client.batch_execute("ALTER TABLE f1 DROP COLUMN s CASCADE").await.unwrap();
        let notice = tokio::time::timeout(std::time::Duration::from_secs(5), notices.recv()).await.unwrap().unwrap();
        assert_eq!(notice.code().code(), "00000");
        assert_eq!(notice.message(), "drop cascades to 2 other objects");
        assert_eq!(notice.detail(), Some("drop cascades to view fv\ndrop cascades to view fv2"));
    }

    if engine == giggles_core::Engine::V2 {
        client.batch_execute("CREATE TABLE f2_generation (
            id int PRIMARY KEY, n int, s int GENERATED ALWAYS AS (n * 2) STORED);
            ALTER TABLE f2_generation ALTER COLUMN s SET EXPRESSION AS (n * 3)").await.unwrap();
    }
    drop(client);
    connection.await.unwrap().unwrap();
    running.shutdown().await.unwrap();

    if engine == giggles_core::Engine::V2 {
        let admin = Admin::open(&path, Limits::default()).unwrap();
        let service = Service::new(admin, vec![], Clock::new(0, 3600).unwrap()).unwrap();
        let running = serve_postgresql(config.listen, service, listener).await.unwrap();
        let (client, connection) = client_config(running.address.port()).connect(tokio_postgres::NoTls).await.unwrap();
        let connection = tokio::spawn(connection);
        let row = client.query_one("SELECT generation_expression FROM information_schema.columns
            WHERE table_name='f2_generation' AND column_name='s'", &[]).await.unwrap();
        assert_eq!(row.get::<_, &str>(0), "(n * 3)");
        let row = client.query_one("INSERT INTO f2_generation VALUES (2, 7) RETURNING s", &[]).await.unwrap();
        assert_eq!(row.get::<_, i32>(0), 21);
        drop(client);
        connection.await.unwrap().unwrap();
        running.shutdown().await.unwrap();
    }
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
