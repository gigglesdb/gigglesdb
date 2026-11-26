use giggles_core::{Admin, Engine, Limits, roles::NewRole};
use giggles_server::{
    Running, Service,
    auth::{PostgresqlConfig, TlsMode, serve_postgresql},
    clock::Clock,
};
use std::{
    future::poll_fn,
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::mpsc;
use tokio_postgres::{AsyncMessage, Client, NoTls, error::DbError};

const FUNCTION_HINT: &str = "No function matches the given name and argument types. You might need to add explicit type casts.";
const ABORTED: &str =
    "current transaction is aborted, commands ignored until end of transaction block";
const GENERIC: &str = "request rejected; consult documented code";
const PARENT_FK: &str =
    r#"update or delete on table "p" violates foreign key constraint "c_pid_fkey" on table "c""#;
const CHILD_FK: &str =
    r#"insert or update on table "c" violates foreign key constraint "c_pid_fkey""#;
const NOT_NULL: &str = r#"null value in column "n" of relation "c" violates not-null constraint"#;

struct Fixture {
    server: Running,
    path: PathBuf,
}

impl Fixture {
    async fn new(engine: Engine) -> Self {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "pg-error-messages-{}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        ));
        let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
        for (name, superuser) in [("owner", true), ("restricted", false)] {
            admin
                .roles()
                .create(NewRole {
                    name: name.into(),
                    login: true,
                    superuser,
                    ..NewRole::default()
                })
                .unwrap();
        }
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
        let service = Service::new(admin, vec![], Clock::new(0, 3600).unwrap()).unwrap();
        let server = serve_postgresql(config.listen, service, Arc::new(config.build().unwrap()))
            .await
            .unwrap();
        Self { server, path }
    }

    async fn connect(&self, user: &str) -> (Client, mpsc::UnboundedReceiver<DbError>) {
        let (client, mut connection) = tokio_postgres::Config::new()
            .hostaddr(self.server.address.ip())
            .port(self.server.address.port())
            .user(user)
            .dbname("postgres")
            .connect(NoTls)
            .await
            .unwrap();
        let (send, receive) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Some(message) = poll_fn(|cx| connection.poll_message(cx)).await {
                if let AsyncMessage::Notice(notice) = message.unwrap() {
                    let _ = send.send(notice);
                }
            }
        });
        (client, receive)
    }

    async fn stop(self) {
        self.server.shutdown().await.unwrap();
        std::fs::remove_dir_all(self.path).unwrap();
    }
}

#[derive(Clone, Copy)]
enum Protocol {
    Simple,
    Extended,
}

impl Protocol {
    async fn run(self, client: &Client, sql: &str) -> Result<(), tokio_postgres::Error> {
        match self {
            Self::Simple => client.simple_query(sql).await.map(|_| ()),
            Self::Extended => client.query(sql, &[]).await.map(|_| ()),
        }
    }
}

#[track_caller]
fn diagnostic(
    error: tokio_postgres::Error,
    code: &str,
    message: &str,
    detail: Option<&str>,
    hint: Option<&str>,
) {
    let error = error.as_db_error().expect("database error");
    assert_eq!((error.code().code(), error.message()), (code, message));
    if let Some(detail) = detail {
        assert_eq!(error.detail(), Some(detail), "{message}");
    }
    assert_eq!(error.hint(), hint, "{message}");
}

fn assert_missing_relation_position(error: tokio_postgres::Error, sql: &str, position: u32) {
    assert!(matches!(
        error.as_db_error().unwrap().position(),
        Some(tokio_postgres::error::ErrorPosition::Original(actual)) if *actual == position
    ), "{sql}: {error:?}");
    diagnostic(error, "42P01", r#"relation "nosuch" does not exist"#, None, None);
}

async fn statement_messages(engine: Engine) {
    let fixture = Fixture::new(engine).await;
    let (client, mut notices) = fixture.connect("owner").await;
    client
        .batch_execute(
            "
        CREATE TABLE p (id int PRIMARY KEY, name text UNIQUE);
        CREATE TABLE c (id int PRIMARY KEY, pid int REFERENCES p(id),
            v varchar(5), n int NOT NULL DEFAULT 0, k int CHECK (k > 0));
        CREATE TABLE d (id int PRIMARY KEY, pid int, CONSTRAINT d_pid_fkey
            FOREIGN KEY (pid) REFERENCES p(id) DEFERRABLE INITIALLY DEFERRED);
        INSERT INTO p VALUES (1,'a'),(2,'b');
        INSERT INTO c VALUES (1,1,'x',0,1);
        CREATE TABLE fixedchar (id int PRIMARY KEY, v char(5));
        CREATE SCHEMA o;
        CREATE TABLE zz (id int PRIMARY KEY);
        CREATE TABLE pn (id numeric(10,2) PRIMARY KEY);
        CREATE TABLE cn (id int PRIMARY KEY, pid int REFERENCES pn);
        INSERT INTO pn VALUES (1);
        INSERT INTO cn VALUES (1, 1);
        CREATE TABLE name_parent (id int PRIMARY KEY);
        CREATE TABLE named_fk (id int PRIMARY KEY, pid int,
            CONSTRAINT \"QuotedFk\" FOREIGN KEY (pid) REFERENCES name_parent(id) DEFERRABLE);
    ",
        )
        .await
        .unwrap();
    for grant in [
        "GRANT DELETE ON p TO restricted",
        "GRANT INSERT ON c TO restricted",
    ] {
        client.batch_execute(grant).await.unwrap();
    }
    let (restricted, _) = fixture.connect("restricted").await;
    for protocol in [Protocol::Simple, Protocol::Extended] {
        client.batch_execute("BEGIN").await.unwrap();
        let divide = protocol.run(&client, "SELECT 1/0").await.unwrap_err();
        assert!(
            divide
                .as_db_error()
                .unwrap()
                .detail()
                .unwrap()
                .starts_with("phase=")
        );
        diagnostic(divide, "22012", "division by zero", None, None);
        diagnostic(
            protocol.run(&client, "SELECT 1").await.unwrap_err(),
            "25P02",
            ABORTED,
            None,
            None,
        );
        client.batch_execute("ROLLBACK").await.unwrap();

        for (sql, relation) in [
            ("SELECT * FROM nosuch", "nosuch"),
            ("SELECT * FROM public.nosuch", "public.nosuch"),
            ("SELECT * FROM nosch.t", "nosch.t"),
            ("INSERT INTO nosuch VALUES (1)", "nosuch"),
            ("UPDATE nosuch SET a = 1", "nosuch"),
            ("DELETE FROM nosuch", "nosuch"),
            ("SELECT * FROM p JOIN nosuch ON true", "nosuch"),
            (r#"SELECT count(*) FROM "NoSuch""#, "NoSuch"),
        ] {
            let error = protocol.run(&client, sql).await.unwrap_err();
            if sql == "SELECT * FROM nosuch" {
                assert!(matches!(
                    error.as_db_error().unwrap().position(),
                    Some(tokio_postgres::error::ErrorPosition::Original(15))
                ));
            }
            diagnostic(
                error,
                "42P01",
                &format!(r#"relation "{relation}" does not exist"#),
                None,
                None,
            );
        }
        for (sql, position) in [
            ("SELECT * FROM p, nosuch", 18),
            ("SELECT * FROM (SELECT * FROM nosuch) q", 30),
            ("WITH x AS (SELECT * FROM nosuch) SELECT * FROM x", 26),
            ("SELECT * FROM p WHERE EXISTS (SELECT 1 FROM nosuch)", 45),
            ("UPDATE c SET v = 'x' FROM nosuch WHERE true", 27),
            ("DELETE FROM c USING nosuch WHERE true", 21),
            ("  /* é comment */  SELECT * FROM nosuch", 34),
            ("SELECT /* é comment */ * FROM nosuch", 31),
        ] {
            assert_missing_relation_position(protocol.run(&client, sql).await.unwrap_err(), sql, position);
        }
        if let Protocol::Simple = protocol {
            let sql = "SELECT 1; SELECT * FROM nosuch;";
            assert_missing_relation_position(protocol.run(&client, sql).await.unwrap_err(), sql, 25);
            let sql = "BEGIN ; SELECT count(*) FROM zz ; SET LOCAL search_path = o ; INSERT INTO public.c SELECT id FROM zz ; COMMIT;";
            let error = client.simple_query(sql).await.unwrap_err();
            let position = sql.rfind("zz").unwrap() as u32 + 1;
            assert!(matches!(
                error.as_db_error().unwrap().position(),
                Some(tokio_postgres::error::ErrorPosition::Original(actual)) if *actual == position
            ), "{error:?}");
            diagnostic(error, "42P01", r#"relation "zz" does not exist"#, None, None);
            client.batch_execute("ROLLBACK").await.unwrap();

            diagnostic(
                client.simple_query("SELECT upper('a'); SELECT pg_catalog.upper(1)").await.unwrap_err(),
                "42883", "function pg_catalog.upper(integer) does not exist", None, Some(FUNCTION_HINT),
            );
        }
        diagnostic(
            protocol
                .run(&client, "SELECT x.id FROM p")
                .await
                .unwrap_err(),
            "42P01",
            r#"missing FROM-clause entry for table "x""#,
            None,
            None,
        );
        for (sql, name) in [
            ("SELECT nocol FROM p", r#""nocol""#),
            ("SELECT p.nocol FROM p", "p.nocol"),
            ("SELECT * FROM p WHERE nocol = 1", r#""nocol""#),
            ("SELECT * FROM p ORDER BY nocol", r#""nocol""#),
            ("UPDATE p SET name = 'q' WHERE nocol = 1", r#""nocol""#),
            (
                "INSERT INTO p (id, nocol) VALUES (3, 1)",
                r#""nocol" of relation "p""#,
            ),
            ("UPDATE p SET nocol = 1", r#""nocol" of relation "p""#),
        ] {
            diagnostic(
                protocol.run(&client, sql).await.unwrap_err(),
                "42703",
                &format!("column {name} does not exist"),
                None,
                None,
            );
        }
        for (sql, signature) in [
            ("SELECT nosuch_fn(1)", "nosuch_fn(integer)"),
            ("SELECT nosuch_fn(id) FROM p", "nosuch_fn(integer)"),
            ("SELECT abs('x'::text)", "abs(text)"),
            ("SELECT lower(1)", "lower(integer)"),
            ("SELECT upper(1)", "upper(integer)"),
            ("SELECT length(1)", "length(integer)"),
            ("SELECT pg_catalog.upper(1)", "pg_catalog.upper(integer)"),
            ("SELECT pg_catalog.upper(1), upper('a')", "pg_catalog.upper(integer)"),
            ("SELECT upper('a'), pg_catalog.upper(1)", "pg_catalog.upper(integer)"),
            ("SELECT upper(1), pg_catalog.upper('a')", "upper(integer)"),
            ("SELECT pg_catalog.sum(true), sum(1)", "pg_catalog.sum(boolean)"),
            ("SELECT pg_catalog.array_agg(1,2), array_agg(1)", "pg_catalog.array_agg(integer, integer)"),
            ("SELECT pg_catalog.upper(foo => 1)", "pg_catalog.upper(foo => integer)"),
            ("SELECT sum(true)", "sum(boolean)"),
            ("SELECT nosuch(*)", "nosuch()"),
            ("SELECT nosuch(1, 'x')", "nosuch(integer, unknown)"),
            ("SELECT nosuch(1.5)", "nosuch(numeric)"),
            ("SELECT nosuch(true)", "nosuch(boolean)"),
            ("SELECT nosuch(NULL)", "nosuch(unknown)"),
            ("SELECT nosuch(1::bigint, now())", "nosuch(bigint, timestamp with time zone)"),
            ("SELECT nosuch()", "nosuch()"),
            ("SELECT public.nosuch(1)", "public.nosuch(integer)"),
            ("SELECT nosuch_fn('secret')", "nosuch_fn(unknown)"),
        ] {
            diagnostic(
                protocol.run(&client, sql).await.unwrap_err(),
                "42883",
                &format!("function {signature} does not exist"),
                None,
                Some(FUNCTION_HINT),
            );
        }
        for sql in [
            "SELECT nocol FROM p",
            "SELECT * FROM nosuch_fn_tbl JOIN p ON true",
        ] {
            diagnostic(
                protocol.run(&restricted, sql).await.unwrap_err(),
                "42501",
                GENERIC,
                None,
                None,
            );
        }
        diagnostic(
            protocol.run(&client, "DELETE FROM pn").await.unwrap_err(),
            "23503",
            r#"update or delete on table "pn" violates foreign key constraint "cn_pid_fkey" on table "cn""#,
            Some(r#"Key (id)=(1.00) is still referenced from table "cn"."#),
            None,
        );
        let child = match protocol {
            Protocol::Simple => client
                .simple_query("UPDATE c SET pid = 98 WHERE id = 1")
                .await
                .unwrap_err(),
            Protocol::Extended => client
                .query("UPDATE c SET pid = $1 WHERE id = 1", &[&98_i32])
                .await
                .unwrap_err(),
        };
        diagnostic(
            child,
            "23503",
            CHILD_FK,
            Some(r#"Key (pid)=(98) is not present in table "p"."#),
            None,
        );
        diagnostic(
            protocol
                .run(&client, "INSERT INTO c VALUES (2,99,'x',0,1)")
                .await
                .unwrap_err(),
            "23503",
            CHILD_FK,
            Some(r#"Key (pid)=(99) is not present in table "p"."#),
            None,
        );
        for sql in [
            "DELETE FROM p WHERE id = 1",
            "UPDATE p SET id = 7 WHERE id = 1",
        ] {
            diagnostic(
                protocol.run(&client, sql).await.unwrap_err(),
                "23503",
                PARENT_FK,
                Some(r#"Key (id)=(1) is still referenced from table "c"."#),
                None,
            );
        }
        diagnostic(
            protocol
                .run(&restricted, "DELETE FROM p")
                .await
                .unwrap_err(),
            "23503",
            PARENT_FK,
            Some(r#"Key is still referenced from table "c"."#),
            None,
        );

        for (sql, row) in [
            (
                "INSERT INTO c VALUES (3, 1, 'x', NULL, 1)",
                "Failing row contains (3, 1, x, null, 1).",
            ),
            (
                "UPDATE c SET n = NULL WHERE id = 1",
                "Failing row contains (1, 1, x, null, 1).",
            ),
        ] {
            diagnostic(
                protocol.run(&client, sql).await.unwrap_err(),
                "23502",
                NOT_NULL,
                Some(row),
                None,
            );
        }
        let error = protocol
            .run(&restricted, "INSERT INTO c (id, n) VALUES (5, NULL)")
            .await
            .unwrap_err();
        diagnostic(error, "23502", NOT_NULL, Some("Failing row contains (id, n) = (5, null)."), None);

        for (sql, ty) in [
            ("SELECT 32767::smallint + 1::smallint", "smallint"),
            ("SELECT 2147483647 + 1", "integer"),
            ("SELECT 9223372036854775807 + 1", "bigint"),
        ] {
            diagnostic(
                protocol.run(&client, sql).await.unwrap_err(),
                "22003",
                &format!("{ty} out of range"),
                None,
                None,
            );
        }
        diagnostic(
            protocol
                .run(&client, "INSERT INTO c VALUES (5, 1, 'toolong', 0, 1)")
                .await
                .unwrap_err(),
            "22001",
            "value too long for type character varying(5)",
            None,
            None,
        );
        diagnostic(
            protocol
                .run(&client, "INSERT INTO fixedchar VALUES (5, 'toolong')")
                .await
                .unwrap_err(),
            "22001",
            "value too long for type character(5)",
            None,
            None,
        );
        diagnostic(
            protocol.run(&client, "DROP TABLE p").await.unwrap_err(),
            "2BP01",
            "cannot drop table p because other objects depend on it",
            Some(
                "constraint c_pid_fkey on table c depends on table p\nconstraint d_pid_fkey on table d depends on table p",
            ),
            Some("Use DROP ... CASCADE to drop the dependent objects too."),
        );
        diagnostic(
            protocol
                .run(
                    &client,
                    "INSERT INTO c (id, pid) VALUES ('2020-01-01'::date, 1)",
                )
                .await
                .unwrap_err(),
            "42804",
            r#"column "id" is of type integer but expression is of type date"#,
            None,
            Some("You will need to rewrite or cast the expression."),
        );

        protocol
            .run(&client, "SET CONSTRAINTS ALL DEFERRED")
            .await
            .unwrap();
        let warning = tokio::time::timeout(Duration::from_secs(5), notices.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (warning.severity(), warning.code().code(), warning.message()),
            (
                "WARNING",
                "25P01",
                "SET CONSTRAINTS can only be used in transaction blocks"
            )
        );
        client.batch_execute("BEGIN").await.unwrap();
        for sql in [
            "SET CONSTRAINTS ALL DEFERRED",
            "SET CONSTRAINTS ALL IMMEDIATE",
            "SET CONSTRAINTS d_pid_fkey IMMEDIATE",
            r#"SET CONSTRAINTS "QuotedFk" IMMEDIATE"#,
            r#"SET CONSTRAINTS public."QuotedFk" DEFERRED"#,
            "SET CONSTRAINTS public.d_pid_fkey DEFERRED",
            "SET CONSTRAINTS c_pid_fkey IMMEDIATE",
        ] {
            protocol.run(&client, sql).await.unwrap();
        }
        diagnostic(
            protocol
                .run(&client, "SET CONSTRAINTS nosuch_con DEFERRED")
                .await
                .unwrap_err(),
            "42704",
            r#"constraint "nosuch_con" does not exist"#,
            None,
            None,
        );
        for (sql, code, message) in [
            ("SET CONSTRAINTS nosch.x IMMEDIATE", "3F000", r#"schema "nosch" does not exist"#),
            ("SET CONSTRAINTS c_pid_fkey DEFERRED", "42809", r#"constraint "c_pid_fkey" is not deferrable"#),
        ] {
            client.batch_execute("ROLLBACK; BEGIN").await.unwrap();
            diagnostic(protocol.run(&client, sql).await.unwrap_err(), code, message, None, None);
        }
        client
            .batch_execute("ROLLBACK; BEGIN; INSERT INTO d VALUES (2,77)")
            .await
            .unwrap();
        diagnostic(
            protocol
                .run(&client, "SET CONSTRAINTS d_pid_fkey IMMEDIATE")
                .await
                .unwrap_err(),
            "23503",
            r#"insert or update on table "d" violates foreign key constraint "d_pid_fkey""#,
            Some(r#"Key (pid)=(77) is not present in table "p"."#),
            None,
        );
        client
            .batch_execute("ROLLBACK; BEGIN; INSERT INTO d VALUES (2,77)")
            .await
            .unwrap();
        diagnostic(
            protocol.run(&client, "COMMIT").await.unwrap_err(),
            "23503",
            r#"insert or update on table "d" violates foreign key constraint "d_pid_fkey""#,
            Some(r#"Key (pid)=(77) is not present in table "p"."#),
            None,
        );
    }
    assert!(notices.try_recv().is_err());
    drop((client, restricted));
    fixture.stop().await;
}

#[tokio::test]
async fn statement_diagnostics_legacy() {
    statement_messages(Engine::Legacy).await;
}

#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn statement_diagnostics_v2() {
    statement_messages(Engine::V2).await;
}

#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn drop_table_lists_dependencies_in_postgres_order() {
    let fixture = Fixture::new(Engine::V2).await;
    let (client, _) = fixture.connect("owner").await;
    client.batch_execute("CREATE TABLE root (id int PRIMARY KEY)").await.unwrap();
    for (ddl, detail) in [
        ("CREATE VIEW first_v AS SELECT id FROM root", "view first_v depends on table root"),
        ("CREATE VIEW second_v AS SELECT id FROM root",
            "view first_v depends on table root\nview second_v depends on table root"),
        ("CREATE VIEW nested_v AS SELECT id FROM first_v",
            "view first_v depends on table root\nview nested_v depends on view first_v\nview second_v depends on table root"),
        ("CREATE TABLE referring (pid int CONSTRAINT root_fk REFERENCES root)",
            "view first_v depends on table root\nview nested_v depends on view first_v\nview second_v depends on table root\nconstraint root_fk on table referring depends on table root"),
    ] {
        client.batch_execute(ddl).await.unwrap();
        for protocol in [Protocol::Simple, Protocol::Extended] {
            diagnostic(
                protocol.run(&client, "DROP TABLE root").await.unwrap_err(),
                "2BP01",
                "cannot drop table root because other objects depend on it",
                Some(detail),
                Some("Use DROP ... CASCADE to drop the dependent objects too."),
            );
        }
    }
    drop(client);
    fixture.stop().await;
}

#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn drop_table_dependency_names_follow_search_path() {
    let fixture = Fixture::new(Engine::V2).await;
    let (client, _) = fixture.connect("owner").await;
    client.batch_execute(
        "CREATE SCHEMA o;
         CREATE TABLE r9 (id int PRIMARY KEY);
         CREATE VIEW r9v AS SELECT id FROM r9;
         CREATE TABLE r9c (id int PRIMARY KEY, pid int REFERENCES r9);
         CREATE VIEW o.r9w AS SELECT id FROM r9v",
    ).await.unwrap();
    for (path, table, detail) in [
        ("o", "public.r9",
            "view public.r9v depends on table public.r9\nview r9w depends on view public.r9v\nconstraint r9c_pid_fkey on table public.r9c depends on table public.r9"),
        ("o, public", "r9",
            "view r9v depends on table r9\nview r9w depends on view r9v\nconstraint r9c_pid_fkey on table r9c depends on table r9"),
    ] {
        client.batch_execute(&format!("SET search_path = {path}")).await.unwrap();
        for protocol in [Protocol::Simple, Protocol::Extended] {
            diagnostic(
                protocol.run(&client, &format!("DROP TABLE {table}")).await.unwrap_err(),
                "2BP01",
                &format!("cannot drop table {table} because other objects depend on it"),
                Some(detail),
                Some("Use DROP ... CASCADE to drop the dependent objects too."),
            );
        }
    }
    drop(client);
    fixture.stop().await;
}

#[cfg(feature = "engine-rocksdb")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transaction_conflict_diagnostics() {
    let fixture = Fixture::new(Engine::V2).await;
    let (a, _) = fixture.connect("owner").await;
    let (b, _) = fixture.connect("owner").await;
    a.batch_execute(
        "CREATE TABLE ct (id int PRIMARY KEY, v int); INSERT INTO ct VALUES (1,0),(2,0);
         CREATE TABLE cp (id int PRIMARY KEY, v int); INSERT INTO cp VALUES (2,0);
         CREATE TABLE cc (id int PRIMARY KEY, pid int REFERENCES cp)",
    )
    .await
    .unwrap();
    for protocol in [Protocol::Simple, Protocol::Extended] {
        for (concurrent, failing) in [
            ("UPDATE ct SET v=v+1 WHERE id=1", "UPDATE ct SET v=v+1 WHERE id=1"),
            ("UPDATE ct SET v=v+1 WHERE id=2", "SELECT id FROM ct WHERE id=2 FOR UPDATE"),
            ("UPDATE cp SET v=v+1 WHERE id=2", "DELETE FROM cp WHERE id=2"),
        ] {
            a.batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ; SELECT v FROM ct WHERE id=1")
                .await
                .unwrap();
            b.batch_execute(concurrent).await.unwrap();
            diagnostic(
                protocol.run(&a, failing).await.unwrap_err(),
                "40001",
                "could not serialize access due to concurrent update",
                None,
                None,
            );
            a.batch_execute("ROLLBACK").await.unwrap();
        }

        for client in [&a, &b] {
            client
                .batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE; SELECT sum(v) FROM ct")
                .await
                .unwrap();
        }
        protocol
            .run(&a, "UPDATE ct SET v=v+1 WHERE id=1")
            .await
            .unwrap();
        protocol
            .run(&b, "UPDATE ct SET v=v+1 WHERE id=2")
            .await
            .unwrap();
        protocol.run(&a, "COMMIT").await.unwrap();
        diagnostic(
            protocol.run(&b, "COMMIT").await.unwrap_err(),
            "40001",
            "could not serialize access due to read/write dependencies among transactions",
            Some("Reason code: Canceled on identification as a pivot, during commit attempt."),
            Some("The transaction might succeed if retried."),
        );

        protocol.run(&b, "SELECT 1").await.unwrap();
        let unrelated = protocol.run(&b, "SELECT * FROM nosuch").await.unwrap_err();
        assert_missing_relation_position(unrelated, "SELECT * FROM nosuch", 15);

        a.batch_execute("BEGIN; UPDATE ct SET v=v+1 WHERE id=1")
            .await
            .unwrap();
        b.batch_execute("BEGIN; UPDATE ct SET v=v+1 WHERE id=2")
            .await
            .unwrap();
        let outcomes = tokio::time::timeout(Duration::from_secs(15), async {
            async fn contend(
                protocol: Protocol,
                client: &Client,
                sql: &str,
            ) -> Result<(), tokio_postgres::Error> {
                let result = protocol.run(client, sql).await;
                client
                    .batch_execute(if result.is_ok() { "COMMIT" } else { "ROLLBACK" })
                    .await
                    .unwrap();
                result
            }
            tokio::join!(
                contend(protocol, &a, "UPDATE ct SET v=v+1 WHERE id=2"),
                contend(protocol, &b, "UPDATE ct SET v=v+1 WHERE id=1"),
            )
        })
        .await
        .expect("deadlock must resolve");
        let ((Ok(()), Err(error)) | (Err(error), Ok(()))) = outcomes else {
            panic!("expected one deadlock victim: {outcomes:?}");
        };
        diagnostic(
            error,
            "40P01",
            "deadlock detected",
            None,
            Some("See server log for query details."),
        );
    }
    drop((a, b));
    fixture.stop().await;
}
