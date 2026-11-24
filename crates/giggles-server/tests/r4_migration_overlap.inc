use giggles_core::Engine as R4Engine;

const R4_ROWS: &str = "SELECT id,state::text,tags::text,search::text FROM r4_docs ORDER BY id";
const R4_DEFAULTS: &str = "
    SELECT a.attname, a.attgenerated::text, pg_get_expr(d.adbin,d.adrelid)
    FROM pg_attribute a JOIN pg_attrdef d ON a.attrelid=d.adrelid AND a.attnum=d.adnum
    WHERE a.attrelid='r4_docs'::regclass ORDER BY a.attnum";
const R4_INDEX: &str = "
    SELECT am.amname FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid
    JOIN pg_am am ON am.oid=c.relam WHERE c.relname='r4_docs_search'";

async fn r4_rows(client: &tokio_postgres::Client, sql: &str) -> Vec<Vec<String>> {
    client
        .simple_query(sql)
        .await
        .unwrap_or_else(|err| panic!("{sql}: {err:?}"))
        .into_iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|i| row.get(i).unwrap().to_owned())
                    .collect(),
            ),
            _ => None,
        })
        .collect()
}

async fn r4_initial_rows_and_catalog(client: &tokio_postgres::Client) {
    assert_eq!(
        r4_rows(client, R4_ROWS).await,
        [
            ["1", "live", "{}", "'runner':2 'updat':1"],
            ["2", "live", "{}", "'test':1"],
            ["3", "draft", "{}", "'copi':1 'word':2"],
        ]
    );
    let defaults = r4_rows(client, R4_DEFAULTS).await;
    assert_eq!(defaults.len(), 2);
    assert_eq!(defaults[0], ["tags", "", "ARRAY[]::text[]"]);
    assert_eq!(
        defaults[1],
        [
            "search",
            "s",
            "to_tsvector('english'::regconfig, COALESCE(title, ''::text))",
        ]
    );
    assert_eq!(r4_rows(client, R4_INDEX).await, [["gin"]]);
}

async fn r4_run_ddl_and_reuse_plans(client: &tokio_postgres::Client) {
    client
        .batch_execute(
            "ALTER TABLE r4_docs ADD COLUMN extra text DEFAULT 'added', DROP COLUMN tags",
        )
        .await
        .unwrap();
    let rows = r4_rows(
        client,
        "SELECT id,extra,search::text FROM r4_docs ORDER BY id",
    )
    .await;
    assert_eq!(
        rows,
        [
            ["1", "added", "'runner':2 'updat':1"],
            ["2", "added", "'test':1"],
            ["3", "added", "'copi':1 'word':2"],
        ]
    );
    assert_eq!(r4_rows(client, R4_INDEX).await, [["gin"]]);

    let error = client
        .simple_query(
            "ALTER TABLE r4_docs ALTER COLUMN title TYPE varchar,
        ADD COLUMN denied text DEFAULT 'absent'",
        )
        .await
        .unwrap_err();
    let error = error.as_db_error().unwrap();
    assert_eq!(error.code().code(), "0A000");
    assert_eq!(
        error.detail(),
        Some("Column \"title\" is used by generated column \"search\".")
    );
    assert_eq!(
        client
            .simple_query("SELECT denied FROM r4_docs")
            .await
            .unwrap_err()
            .code()
            .unwrap()
            .code(),
        "42703"
    );

    let error = client
        .simple_query(
            "ALTER TABLE r4_docs ALTER COLUMN search DROP DEFAULT,
        ALTER COLUMN search SET DEFAULT ''::tsvector",
        )
        .await
        .unwrap_err();
    let error = error.as_db_error().unwrap();
    assert_eq!(error.code().code(), "42601");
    assert_eq!(
        error.hint(),
        Some("Use ALTER TABLE ... ALTER COLUMN ... DROP EXPRESSION instead.")
    );

    client
        .batch_execute("PREPARE r4_select(int) AS SELECT id,search::text FROM r4_docs WHERE id=$1")
        .await
        .unwrap();
    let statement = client
        .prepare("SELECT search::text FROM r4_docs WHERE id=$1")
        .await
        .unwrap();
    for (ddl, id, expected) in [
        ("", 1, "'runner':2 'updat':1"),
        (
            "ALTER TABLE r4_docs ALTER COLUMN search SET EXPRESSION AS
          (to_tsvector('simple', coalesce(title,'')))",
            1,
            "'runners':2 'updated':1",
        ),
        ("DROP INDEX r4_docs_search", 1, "'runners':2 'updated':1"),
        (
            "ALTER TABLE r4_docs ALTER COLUMN search DROP EXPRESSION;
          INSERT INTO r4_docs(id,state,title,search) VALUES (4,'draft','ignored','manual')",
            4,
            "'manual'",
        ),
        (
            "CREATE INDEX r4_docs_search ON r4_docs USING gin(search)",
            4,
            "'manual'",
        ),
        (
            "ALTER TABLE r4_docs ADD COLUMN fresh tsvector
          GENERATED ALWAYS AS (to_tsvector('simple', title)) STORED",
            4,
            "'manual'",
        ),
        ("ALTER TABLE r4_docs DROP COLUMN fresh", 4, "'manual'"),
    ] {
        if !ddl.is_empty() {
            client
                .batch_execute(ddl)
                .await
                .unwrap_or_else(|err| panic!("{ddl}: {err:?}"));
        }
        let rows = r4_rows(client, &format!("EXECUTE r4_select({id})")).await;
        assert_eq!(rows, [[id.to_string(), expected.to_owned()]], "{ddl}");
        let row = client
            .query_one(&statement, &[&id])
            .await
            .unwrap_or_else(|err| panic!("extended Bind after {ddl:?}: {err:?}"));
        assert_eq!(row.get::<_, &str>(0), expected, "{ddl}");
        if ddl == "DROP INDEX r4_docs_search" {
            assert!(r4_rows(client, R4_INDEX).await.is_empty());
        }
    }
    assert_eq!(r4_rows(client, R4_INDEX).await, [["gin"]]);
    let defaults = r4_rows(client, R4_DEFAULTS).await;
    assert_eq!(defaults.len(), 1);
    assert_eq!(defaults[0][..2], ["extra", ""]);
    client
        .batch_execute(
            "BEGIN; DECLARE r4_cursor CURSOR FOR
        SELECT id,search::text FROM r4_docs WHERE id=4",
        )
        .await
        .unwrap();
    assert_eq!(
        r4_rows(client, "FETCH ALL FROM r4_cursor").await,
        [["4", "'manual'"]]
    );
    client.batch_execute("COMMIT").await.unwrap();
}

fn r4_migration_overlap(engine: R4Engine) {
    use giggles_core::roles::NewRole;
    use giggles_server::auth::scram::Verifier;

    let path = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "r4-migration-overlap-{}-{}",
        std::process::id(),
        SERIAL.fetch_add(1, Ordering::Relaxed)
    ));
    let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
    admin
        .roles()
        .create(NewRole {
            name: "r4_owner".into(),
            login: true,
            superuser: true,
            password: Some(Verifier::generate("fixture-password")),
            ..NewRole::default()
        })
        .unwrap();

    for (admin, reopened) in std::iter::once((admin, false)).chain(std::iter::once_with(|| {
        (Admin::open(&path, Limits::default()).unwrap(), true)
    })) {
        // Dropping the runtime joins detached pgwire blocking work before the database reopens.
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(r4_migration_overlap_phase(admin, reopened));
    }
    std::fs::remove_dir_all(path).unwrap();
}

async fn r4_migration_overlap_phase(admin: Admin, reopened: bool) {
    use giggles_server::auth::{PostgresqlConfig, TlsMode, serve_postgresql};
    use tokio_postgres::{binary_copy::BinaryCopyInWriter, types::Type};

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
    let service = Service::new(admin, vec![], Clock::new(0, 3600).unwrap()).unwrap();
    let running = serve_postgresql(config.listen, service, listener)
        .await
        .unwrap();
    let (client, connection) = tokio_postgres::Config::new()
        .host("127.0.0.1")
        .port(running.address.port())
        .dbname("postgres")
        .user("r4_owner")
        .password("fixture-password")
        .ssl_mode(tokio_postgres::config::SslMode::Disable)
        .connect(tokio_postgres::NoTls)
        .await
        .unwrap();
    let connection = tokio::spawn(connection);
    if reopened {
        r4_initial_rows_and_catalog(&client).await;
        r4_run_ddl_and_reuse_plans(&client).await;
    } else {
        client.batch_execute("
            CREATE TYPE r4_state AS ENUM ('draft','live');
            CREATE TABLE r4_docs (
                id int PRIMARY KEY, state r4_state, title text,
                tags text[] DEFAULT ARRAY[]::text[],
                search tsvector GENERATED ALWAYS AS (to_tsvector('english', coalesce(title,''))) STORED
            );
            CREATE INDEX r4_docs_search ON r4_docs USING gin(search);
            INSERT INTO r4_docs(id,state,title) VALUES (1,'draft','Running fast'),(2,'live','Testing');
            UPDATE r4_docs SET state='live',title='Updated runners' WHERE id=1;
        ").await.unwrap();
        let copy = client
            .copy_in("COPY r4_docs(id,state,title) FROM STDIN (FORMAT BINARY)")
            .await
            .unwrap();
        // PostgreSQL represents a binary enum value by its label bytes.
        let mut copy = std::pin::pin!(BinaryCopyInWriter::new(
            copy,
            &[Type::INT4, Type::TEXT, Type::TEXT]
        ));
        copy.as_mut()
            .write(&[&3i32, &"draft", &"Copy words"])
            .await
            .unwrap();
        assert_eq!(copy.as_mut().finish().await.unwrap(), 1);
        r4_initial_rows_and_catalog(&client).await;
    }
    drop(client);
    connection.await.unwrap().unwrap();
    running.shutdown().await.unwrap();
}

#[test]
fn r4_migration_overlap_legacy() {
    r4_migration_overlap(R4Engine::Legacy);
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn r4_migration_overlap_v2() {
    r4_migration_overlap(R4Engine::V2);
}
