use giggles_core::{Admin, Limits, roles::NewRole};
use giggles_server::{
    Running, Service,
    auth::{PostgresqlConfig, TlsMode, serve_postgresql},
    clock::Clock,
};
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};
use tokio_postgres::{Client, Config, NoTls, SimpleQueryMessage, config::SslMode};
static SERIAL: AtomicU64 = AtomicU64::new(0);

struct Fixture {
    service: Arc<Service>,
    pg: Running,
    path: std::path::PathBuf,
}
impl Fixture {
    async fn new() -> Self {
        Self::with_engine(giggles_core::Engine::Legacy).await
    }
    async fn with_engine(engine: giggles_core::Engine) -> Self {
        let path = std::env::temp_dir().join(format!(
            "pg-dump-preamble-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            SERIAL.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).unwrap();
        let admin = Admin::create_engine(path.join("db.redb"), Limits::default(), engine).unwrap();
        admin
            .roles()
            .create(NewRole {
                name: "owner".into(),
                superuser: true,
                login: true,
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
            certificates: BTreeMap::new(),
            databases: vec!["postgres".into()],
            loopback_trust: true,
            connections: 8,
        };
        let listener = Arc::new(config.build().unwrap());
        let pg = serve_postgresql(config.listen, service.clone(), listener)
            .await
            .unwrap();
        Self { service, pg, path }
    }
    async fn client(&self) -> Client {
        self.client_as("owner").await
    }
    async fn client_as(&self, user: &str) -> Client {
        let (client, connection) = Config::new()
            .host("localhost")
            .hostaddr(self.pg.address.ip())
            .port(self.pg.address.port())
            .user(user)
            .dbname("postgres")
            .ssl_mode(SslMode::Disable)
            .connect(NoTls)
            .await
            .unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
    }
    async fn stop(self) {
        self.pg.shutdown().await.unwrap();
        drop(self.service);
        // Listener shutdown waits for active connections to release the file.
        std::fs::remove_dir_all(self.path).unwrap();
    }
}
fn values(messages: &[SimpleQueryMessage]) -> Vec<String> {
    messages
        .iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(row.get(0).unwrap().to_owned()),
            _ => None,
        })
        .collect()
}
fn code(error: tokio_postgres::Error) -> String {
    error.code().unwrap().code().into()
}

#[tokio::test]
async fn bytea_untyped_literal_behavior_without_wo16() {
    let f = Fixture::new().await;
    let c = f.client().await;
    c.simple_query("CREATE TABLE b (id integer PRIMARY KEY, data bytea)")
        .await
        .unwrap();
    let mut observed = Vec::new();
    for sql in [
        r"INSERT INTO b VALUES (1, '\x00ff10')",
        "SELECT id FROM b WHERE id = 1 LIMIT 1",
        r"SELECT id FROM b WHERE data = '\x00ff10' LIMIT 1",
    ] {
        let state = match c.simple_query(sql).await {
            Ok(_) => "00000".to_owned(),
            Err(e) => code(e),
        };
        observed.push(state);
    }
    println!("WO18 bytea baseline SQLSTATEs: {observed:?}");
    // F18: the INSERT's unknown bytea literal uses bytea input (PG18.6 INSERT 0 1).
    // The bytea predicate is still 42804: predicate unknown-literal typing is a pending follow-up (PG returns 1 row).
    assert_eq!(observed, ["00000", "00000", "42804"]);
    drop(c);
    f.stop().await;
}

const PREAMBLE: &str = "SET statement_timeout = 0;\nSET lock_timeout = 0;\nSET idle_in_transaction_session_timeout = 0;\nSET transaction_timeout = 0;\nSET client_encoding = 'UTF8';\nSET standard_conforming_strings = on;\nSELECT pg_catalog.set_config('search_path', '', false);\nSET check_function_bodies = false;\nSET xmloption = content;\nSET client_min_messages = warning;\nSET row_security = off;\nSET default_tablespace = '';\nSET default_table_access_method = heap;";

#[tokio::test]
async fn exact_pg_dump_preamble_simple_and_extended_then_restore_sql() {
    let f = Fixture::new().await;
    for extended in [false, true] {
        let c = f.client().await;
        if extended {
            for line in PREAMBLE.lines() {
                c.query(line, &[]).await.unwrap();
            }
        } else {
            let messages = c.simple_query(PREAMBLE).await.unwrap();
            assert_eq!(
                messages
                    .iter()
                    .filter(|m| matches!(m, SimpleQueryMessage::CommandComplete(_)))
                    .count(),
                13
            );
            assert_eq!(values(&messages), [""]);
        }
        for (name, expected) in [
            ("transaction_timeout", "0"),
            ("client_min_messages", "warning"),
            ("row_security", "off"),
            ("check_function_bodies", "off"),
            ("xmloption", "content"),
            ("default_tablespace", ""),
            ("default_table_access_method", "heap"),
            ("search_path", ""),
        ] {
            assert_eq!(
                values(&c.simple_query(&format!("SHOW {name}")).await.unwrap()),
                [expected]
            );
            assert_eq!(
                c.query(&format!("SHOW {name}"), &[]).await.unwrap()[0].get::<_, String>(0),
                expected
            );
        }
        assert_eq!(
            code(
                c.simple_query("CREATE TABLE no_path(id integer)")
                    .await
                    .unwrap_err()
            ),
            "3F000"
        );
        c.simple_query("CREATE TABLE public.t (id integer PRIMARY KEY, body text)")
            .await
            .unwrap();
        c.execute("INSERT INTO public.t VALUES (1, 'restored')", &[])
            .await
            .unwrap();
        assert_eq!(
            values(
                &c.simple_query("SELECT body FROM public.t LIMIT 1")
                    .await
                    .unwrap()
            ),
            ["restored"]
        );
        c.query("RESET ALL", &[]).await.unwrap();
        for (name, expected) in [
            ("transaction_timeout", "0"),
            ("client_min_messages", "notice"),
            ("row_security", "off"),
            ("check_function_bodies", "on"),
            ("xmloption", "content"),
            ("default_tablespace", ""),
            ("default_table_access_method", "heap"),
            ("search_path", r#""$user", public"#),
        ] {
            assert_eq!(
                c.query(&format!("SHOW {name}"), &[]).await.unwrap()[0].get::<_, String>(0),
                expected
            );
        }
        c.simple_query("DROP TABLE t").await.unwrap();
        drop(c);
    }
    f.stop().await;
}

#[tokio::test]
async fn new_gucs_validate_and_follow_transaction_and_local_semantics() {
    let f = Fixture::new().await;
    let c = f.client().await;
    for (name, value, expected) in [
        ("client_min_messages", "error", "error"),
        ("check_function_bodies", "false", "off"),
        ("xmloption", "document", "document"),
        ("row_security", "off", "off"),
        ("default_tablespace", "''", ""),
        ("default_table_access_method", "heap", "heap"),
        ("transaction_timeout", "0", "0"),
    ] {
        let before = c.query(&format!("SHOW {name}"), &[]).await.unwrap()[0].get::<_, String>(0);
        c.simple_query("BEGIN").await.unwrap();
        c.simple_query(&format!("SET {name} TO {value}"))
            .await
            .unwrap();
        assert_eq!(
            c.query(&format!("SHOW {name}"), &[]).await.unwrap()[0].get::<_, String>(0),
            expected
        );
        c.simple_query("ROLLBACK").await.unwrap();
        assert_eq!(
            c.query(&format!("SHOW {name}"), &[]).await.unwrap()[0].get::<_, String>(0),
            before
        );
        c.execute("BEGIN", &[]).await.unwrap();
        c.query(&format!("SET LOCAL {name} TO {value}"), &[])
            .await
            .unwrap();
        assert_eq!(
            values(&c.simple_query(&format!("SHOW {name}")).await.unwrap()),
            [expected]
        );
        c.execute("COMMIT", &[]).await.unwrap();
        assert_eq!(
            c.query(&format!("SHOW {name}"), &[]).await.unwrap()[0].get::<_, String>(0),
            before
        );
        c.query(&format!("SET {name} TO {value}"), &[])
            .await
            .unwrap();
        c.query(&format!("RESET {name}"), &[]).await.unwrap();
    }
    for level in [
        "debug5", "debug4", "debug3", "debug2", "debug1", "log", "notice", "warning", "error",
    ] {
        c.query(&format!("SET client_min_messages={level}"), &[])
            .await
            .unwrap();
        assert_eq!(
            values(&c.simple_query("SHOW client_min_messages").await.unwrap()),
            [level]
        );
    }
    for (sql, state) in [
        ("SET transaction_timeout=1", "22023"),
        ("SET client_min_messages=info", "22023"),
        ("SET row_security=on", "0A000"),
        ("SET check_function_bodies=maybe", "22023"),
        ("SET xmloption=invalid", "22023"),
        ("SET default_tablespace='custom'", "22023"),
        ("SET default_table_access_method=custom", "22023"),
    ] {
        assert_eq!(code(c.simple_query(sql).await.unwrap_err()), state, "{sql}");
        assert_eq!(code(c.query(sql, &[]).await.unwrap_err()), state, "{sql}");
    }
    c.simple_query("BEGIN; SET xmloption=document; COMMIT")
        .await
        .unwrap();
    assert_eq!(
        values(&c.simple_query("SHOW xmloption").await.unwrap()),
        ["document"]
    );
    c.simple_query("BEGIN; SET LOCAL xmloption=content; ROLLBACK")
        .await
        .unwrap();
    assert_eq!(
        values(&c.simple_query("SHOW xmloption").await.unwrap()),
        ["document"]
    );
    drop(c);
    f.stop().await;
}

#[tokio::test]
async fn configuration_functions_literals_aliases_null_missing_and_transaction_lifetimes() {
    let f = Fixture::new().await;
    let c = f.client().await;
    for extended in [false, true] {
        for (sql, expected, column) in [
            (
                "SELECT pg_catalog.set_config('application_name', 'a''b', false) AS \"App Name\"",
                Some("a'b"),
                "App Name",
            ),
            (
                "SELECT current_setting('application_name')",
                Some("a'b"),
                "current_setting",
            ),
            (
                "SELECT pg_catalog.current_setting('nonexistent', true) AS missing",
                None,
                "missing",
            ),
            (
                "SELECT set_config('search_path', 'public,pg_catalog', false)",
                Some("public,pg_catalog"),
                "set_config",
            ),
            (
                "SELECT current_setting('search_path')",
                Some("public,pg_catalog"),
                "current_setting",
            ),
            (
                "SELECT set_config('search_path', '', false)",
                Some(""),
                "set_config",
            ),
            (
                "SELECT current_setting('search_path', false)",
                Some(""),
                "current_setting",
            ),
        ] {
            if extended {
                let rows = c.query(sql, &[]).await.unwrap();
                assert_eq!(rows[0].columns()[0].name(), column);
                assert_eq!(rows[0].get::<_, Option<String>>(0).as_deref(), expected);
            } else {
                let messages = c.simple_query(sql).await.unwrap();
                let row = messages
                    .iter()
                    .find_map(|m| {
                        if let SimpleQueryMessage::Row(r) = m {
                            Some(r)
                        } else {
                            None
                        }
                    })
                    .unwrap();
                assert_eq!(row.columns()[0].name(), column);
                assert_eq!(row.get(0), expected);
            }
        }
        c.simple_query("SET application_name='session'")
            .await
            .unwrap();
        let sql = "SELECT set_config('application_name', 'local', true)";
        // A SELECT's implicit transaction permits the local value while evaluating,
        // and restores it before the next statement (no explicit block required).
        if extended {
            assert_eq!(
                c.query(sql, &[]).await.unwrap()[0].get::<_, String>(0),
                "local"
            );
        } else {
            assert_eq!(values(&c.simple_query(sql).await.unwrap()), ["local"]);
        }
        assert_eq!(
            values(&c.simple_query("SHOW application_name").await.unwrap()),
            ["session"]
        );
        c.execute("BEGIN", &[]).await.unwrap();
        c.query(sql, &[]).await.unwrap();
        assert_eq!(
            values(
                &c.simple_query("SELECT current_setting('application_name')")
                    .await
                    .unwrap()
            ),
            ["local"]
        );
        c.execute("COMMIT", &[]).await.unwrap();
        assert_eq!(
            values(&c.simple_query("SHOW application_name").await.unwrap()),
            ["session"]
        );
        c.simple_query(
            "BEGIN; SELECT set_config('application_name','rolled back',false); ROLLBACK",
        )
        .await
        .unwrap();
        assert_eq!(
            values(&c.simple_query("SHOW application_name").await.unwrap()),
            ["session"]
        );
        c.simple_query("BEGIN; SELECT set_config('application_name','committed',false); COMMIT")
            .await
            .unwrap();
        assert_eq!(
            values(&c.simple_query("SHOW application_name").await.unwrap()),
            ["committed"]
        );
    }
    for (sql, state) in [
        ("SELECT set_config('nonexistent', 'x', false)", "42704"),
        ("SELECT current_setting('nonexistent')", "42704"),
        ("SELECT current_setting('nonexistent', false)", "42704"),
        (
            "SELECT set_config('transaction_timeout', '1', false)",
            "22023",
        ),
        ("SELECT set_config('row_security', 'on', false)", "0A000"),
    ] {
        assert_eq!(code(c.simple_query(sql).await.unwrap_err()), state, "{sql}");
        assert_eq!(code(c.query(sql, &[]).await.unwrap_err()), state, "{sql}");
    }
    // SQL SET quotes an empty list item, while set_config receives a raw list.
    c.query("SET search_path TO ''", &[]).await.unwrap();
    assert_eq!(
        values(&c.simple_query("SHOW search_path").await.unwrap()),
        ["\"\""]
    );
    assert_eq!(
        values(
            &c.simple_query("SELECT set_config('search_path', '', false); SHOW search_path")
                .await
                .unwrap()
        ),
        ["", ""]
    );
    // Only whole, literal SELECT calls are intercepted; larger SQL stays untouched.
    c.simple_query("SET application_name='unchanged'")
        .await
        .unwrap();
    for sql in [
        "SELECT set_config('application_name','side effect',false), 1 AS n",
        "SELECT set_config('application_name','side effect',false) WHERE true",
    ] {
        assert!(c.simple_query(sql).await.is_err());
        assert_eq!(
            values(&c.simple_query("SHOW application_name").await.unwrap()),
            ["unchanged"]
        );
    }
    assert!(
        c.query(
            "SELECT set_config($1, $2, $3)",
            &[&"application_name", &"parameter", &false]
        )
        .await
        .is_err()
    );
    assert_eq!(
        values(&c.simple_query("SHOW application_name").await.unwrap()),
        ["unchanged"]
    );
    drop(c);
    f.stop().await;
}

#[tokio::test]
async fn public_qualified_names_are_core_gap_until_e1_a3() {
    // Flip to true when E1 WO-A3 (public./"public". qualifiers, GD-0059) lands.
    const PUBLIC_QUALIFIED_SUPPORTED: bool = true;
    let f = Fixture::new().await;
    let c = f.client().await;
    c.simple_query(PREAMBLE).await.unwrap();
    if PUBLIC_QUALIFIED_SUPPORTED {
        for schema in ["public", r#""public""#] {
            let table = format!("{schema}.t2");
            c.simple_query(&format!("CREATE TABLE {table} (id integer PRIMARY KEY)"))
                .await
                .unwrap();
            c.execute(&format!("INSERT INTO {table} VALUES (1)"), &[])
                .await
                .unwrap();
            let select = format!("SELECT id FROM {table} WHERE id = 1 LIMIT 1");
            assert_eq!(values(&c.simple_query(&select).await.unwrap()), ["1"]);
            let rows = c.query(&select, &[]).await.unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].get::<_, i32>(0), 1);
            c.simple_query(&format!("DROP TABLE {table}"))
                .await
                .unwrap();
        }
    } else {
        // schema-qualified names: core/E1 (GD-0059)
        let sql = "CREATE TABLE public.t2 (id integer PRIMARY KEY)";
        let error = c.simple_query(sql).await.unwrap_err();
        println!(
            "WO18 CREATE TABLE public.t2 core SQLSTATE: {}",
            error.code().unwrap().code()
        );
        assert_eq!(code(error), "42601");
        assert_eq!(code(c.query(sql, &[]).await.unwrap_err()), "42601");
    }
    drop(c);
    f.stop().await;
}

#[tokio::test]
async fn synchronize_seqscans_pg_dump_setting_and_session_lifetimes() {
    let f = Fixture::new().await;
    for extended in [false, true] {
        let c = f.client().await;
        for (sql, expected) in [
            ("SHOW synchronize_seqscans", Some("on")),
            ("SET synchronize_seqscans TO off", None),
            ("SHOW synchronize_seqscans", Some("off")),
            ("RESET synchronize_seqscans", None),
            ("SHOW synchronize_seqscans", Some("on")),
            ("BEGIN", None),
            ("SET synchronize_seqscans TO off", None),
            ("SHOW synchronize_seqscans", Some("off")),
            ("ROLLBACK", None),
            ("SHOW synchronize_seqscans", Some("on")),
            ("BEGIN", None),
            ("SET LOCAL synchronize_seqscans TO off", None),
            ("SHOW synchronize_seqscans", Some("off")),
            ("COMMIT", None),
            ("SHOW synchronize_seqscans", Some("on")),
            ("BEGIN", None),
            ("SET synchronize_seqscans TO false", None),
            ("COMMIT", None),
            ("SHOW synchronize_seqscans", Some("off")),
            ("RESET ALL", None),
            ("SHOW synchronize_seqscans", Some("on")),
        ] {
            let actual = if extended {
                c.query(sql, &[])
                    .await
                    .unwrap()
                    .first()
                    .map(|row| row.get::<_, String>(0))
            } else {
                values(&c.simple_query(sql).await.unwrap()).first().cloned()
            };
            assert_eq!(actual.as_deref(), expected, "{sql} (extended={extended})");
        }
        for (value, expected) in [
            ("true", "on"),
            ("false", "off"),
            ("yes", "on"),
            ("no", "off"),
            ("1", "on"),
            ("0", "off"),
        ] {
            let sql = format!("SET synchronize_seqscans TO {value}");
            if extended {
                c.query(&sql, &[]).await.unwrap();
            } else {
                c.simple_query(&sql).await.unwrap();
            }
            assert_eq!(
                values(&c.simple_query("SHOW synchronize_seqscans").await.unwrap()),
                [expected]
            );
        }
        let sql = "SET synchronize_seqscans TO maybe";
        let error = if extended {
            c.query(sql, &[]).await.unwrap_err()
        } else {
            c.simple_query(sql).await.unwrap_err()
        };
        assert_eq!(code(error), "22023");
        assert_eq!(
            values(&c.simple_query("SHOW synchronize_seqscans").await.unwrap()),
            ["off"]
        );
        drop(c);
    }
    f.stop().await;
}

#[tokio::test]
async fn pg_dump_set_config_from_pg_settings_simple_and_extended() {
    let f = Fixture::new().await;
    for extended in [false, true] {
        let c = f.client().await;
        for (sql, expected) in [
            ("SHOW restrict_nonsystem_relation_kind", Some("")),
            // PostgreSQL 18.6 compares pg_settings names case-sensitively (TimeZone, DateStyle).
            (
                "SELECT set_config(name, 'UTC', false) FROM pg_settings WHERE name = 'TimeZone'",
                Some("UTC"),
            ),
            (
                "SELECT set_config(name, 'Europe/Paris', false) FROM pg_settings WHERE name = 'timezone'",
                None,
            ),
            ("SHOW TimeZone", Some("UTC")),
            (
                "SELECT set_config(name, 'ISO, MDY', false) FROM pg_settings WHERE name = 'DateStyle'",
                Some("ISO, MDY"),
            ),
            (
                "SELECT set_config(name, 'SQL, DMY', false) FROM pg_settings WHERE name = 'datestyle'",
                None,
            ),
            ("SHOW DateStyle", Some("ISO, MDY")),
            (
                "SELECT set_config(name, 'view, foreign-table', false) FROM pg_settings WHERE name = 'restrict_nonsystem_relation_kind'",
                Some("view, foreign-table"),
            ),
            (
                "SHOW restrict_nonsystem_relation_kind",
                Some("view, foreign-table"),
            ),
            ("BEGIN", None),
            (
                "SELECT set_config(name, 'foreign-table,view', false) FROM pg_settings WHERE name = 'restrict_nonsystem_relation_kind'",
                Some("foreign-table,view"),
            ),
            (
                "SHOW restrict_nonsystem_relation_kind",
                Some("foreign-table,view"),
            ),
            ("ROLLBACK", None),
            (
                "SHOW restrict_nonsystem_relation_kind",
                Some("view, foreign-table"),
            ),
            ("BEGIN", None),
            ("SET LOCAL restrict_nonsystem_relation_kind='view'", None),
            ("SHOW restrict_nonsystem_relation_kind", Some("view")),
            ("COMMIT", None),
            (
                "SHOW restrict_nonsystem_relation_kind",
                Some("view, foreign-table"),
            ),
            // A missing catalog row suppresses both assignment and value validation.
            (
                "SELECT set_config(name, 'invalid', false) FROM pg_settings WHERE name = 'nonexistent'",
                None,
            ),
            (
                "SELECT set_config(name, 'view', false) FROM pg_settings WHERE name = 'RESTRICT_NONSYSTEM_RELATION_KIND'",
                None,
            ),
            (
                "SHOW restrict_nonsystem_relation_kind",
                Some("view, foreign-table"),
            ),
            ("RESET ALL", None),
            ("SHOW restrict_nonsystem_relation_kind", Some("")),
        ] {
            let actual = if extended {
                let rows = c.query(sql, &[]).await.unwrap();
                assert_eq!(rows.len(), usize::from(expected.is_some()), "{sql}");
                if sql.starts_with("SELECT") && !rows.is_empty() {
                    assert_eq!(rows[0].columns()[0].name(), "set_config");
                    assert_eq!(
                        *rows[0].columns()[0].type_(),
                        tokio_postgres::types::Type::TEXT
                    );
                }
                rows.first().map(|row| row.get::<_, String>(0))
            } else {
                let messages = c.simple_query(sql).await.unwrap();
                if sql.starts_with("SELECT") {
                    assert!(messages.iter().any(|message| matches!(message,
                        SimpleQueryMessage::CommandComplete(n) if *n == u64::from(expected.is_some()))));
                    if let Some(SimpleQueryMessage::Row(row)) = messages
                        .iter()
                        .find(|message| matches!(message, SimpleQueryMessage::Row(_)))
                    {
                        assert_eq!(row.columns()[0].name(), "set_config");
                    }
                }
                let rows = values(&messages);
                assert_eq!(rows.len(), usize::from(expected.is_some()), "{sql}");
                rows.first().cloned()
            };
            assert_eq!(actual.as_deref(), expected, "{sql} (extended={extended})");
        }
        for value in ["table", "view,", "view,,foreign-table"] {
            let sql = format!(
                "SELECT set_config(name, '{value}', false) FROM pg_settings WHERE name = 'restrict_nonsystem_relation_kind'"
            );
            let error = if extended {
                c.query(&sql, &[]).await.unwrap_err()
            } else {
                c.simple_query(&sql).await.unwrap_err()
            };
            assert_eq!(code(error), "22023");
        }
        for suffix in [
            " AND true",
            " trailing",
            " = 'view'",
            " ORDER BY name",
            " OR name = 'application_name'",
        ] {
            let sql = format!(
                "SELECT set_config(name, 'view', false) FROM pg_settings WHERE name = 'restrict_nonsystem_relation_kind'{suffix}"
            );
            if extended {
                assert!(c.query(&sql, &[]).await.is_err(), "{sql}");
            } else {
                assert!(c.simple_query(&sql).await.is_err(), "{sql}");
            }
            assert_eq!(
                values(
                    &c.simple_query("SHOW restrict_nonsystem_relation_kind")
                        .await
                        .unwrap()
                ),
                [""]
            );
        }
        drop(c);
    }
    f.stop().await;
}

#[tokio::test]
async fn pg_dump_relation_inventory_statistics_simple_and_extended() {
    let f = Fixture::new().await;
    let c = f.client().await;
    c.simple_query("CREATE TABLE dump_stats (id integer PRIMARY KEY)")
        .await
        .unwrap();
    let sql = "SELECT relpages,reltuples,relallvisible,relfrozenxid,relminmxid,relispopulated FROM pg_class WHERE relname='dump_stats'";
    let messages = c.simple_query(sql).await.unwrap();
    let row = messages
        .iter()
        .find_map(|m| {
            if let SimpleQueryMessage::Row(r) = m {
                Some(r)
            } else {
                None
            }
        })
        .unwrap();
    assert_eq!(
        (0..6).map(|i| row.get(i).unwrap()).collect::<Vec<_>>(),
        ["0", "-1", "0", "2", "1", "t"]
    );
    let rows = c.query(sql, &[]).await.unwrap();
    assert_eq!(rows[0].get::<_, i32>(0), 0);
    assert_eq!(rows[0].get::<_, f32>(1), -1.0);
    assert_eq!(rows[0].columns()[3].type_().oid(), 28);
    assert!(rows[0].get::<_, bool>(5));
    drop(c);
    f.stop().await;
}

#[tokio::test]
async fn access_share_lock_validates_transactions_tables_and_any_write_grant() {
    let f = Fixture::new().await;
    let c = f.client().await;
    c.simple_query("CREATE TABLE lock_a (id integer PRIMARY KEY)")
        .await
        .unwrap();
    c.simple_query("CREATE TABLE lock_b (id integer PRIMARY KEY)")
        .await
        .unwrap();
    c.simple_query("CREATE ROLE lockreader LOGIN")
        .await
        .unwrap();
    for extended in [false, true] {
        for sql in [
            "LOCK lock_a IN ACCESS SHARE MODE",
            "LOCK lock_a IN ROW SHARE MODE",
            "LOCK lock_a IN ROW EXCLUSIVE MODE",
            "LOCK lock_a IN SHARE UPDATE EXCLUSIVE MODE",
            "LOCK lock_a IN SHARE MODE",
            "LOCK lock_a IN SHARE ROW EXCLUSIVE MODE",
            "LOCK lock_a IN EXCLUSIVE MODE",
            "LOCK lock_a IN ACCESS EXCLUSIVE MODE",
            "LOCK lock_a NOWAIT",
            "LOCK TABLE ONLY public.lock_a, ONLY lock_b IN ACCESS SHARE MODE NOWAIT",
        ] {
            let error = if extended {
                c.query(sql, &[]).await.unwrap_err()
            } else {
                c.simple_query(sql).await.unwrap_err()
            };
            assert_eq!(code(error), "25P01");
            c.simple_query("BEGIN").await.unwrap();
            if extended {
                c.query(sql, &[]).await.unwrap();
            } else {
                c.simple_query(sql).await.unwrap();
            }
            c.simple_query("ROLLBACK").await.unwrap();
        }
        for (sql, state) in [
            ("LOCK lock_a, missing IN ACCESS SHARE MODE", "42P01"),
            ("LOCK missing IN EXCLUSIVE MODE", "42P01"),
        ] {
            c.simple_query("BEGIN").await.unwrap();
            let error = if extended {
                c.query(sql, &[]).await.unwrap_err()
            } else {
                c.simple_query(sql).await.unwrap_err()
            };
            assert_eq!(code(error), state);
            c.simple_query("ROLLBACK").await.unwrap();
        }
    }
    let reader = f.client_as("lockreader").await;
    reader.simple_query("BEGIN").await.unwrap();
    assert_eq!(
        code(
            reader
                .simple_query("LOCK lock_a IN ACCESS SHARE MODE")
                .await
                .unwrap_err()
        ),
        "42501"
    );
    reader.simple_query("ROLLBACK").await.unwrap();
    c.simple_query("GRANT INSERT ON lock_a TO lockreader")
        .await
        .unwrap();
    reader.simple_query("BEGIN").await.unwrap();
    reader
        .simple_query("LOCK lock_a IN ACCESS SHARE MODE NOWAIT")
        .await
        .unwrap();
    reader.simple_query("ROLLBACK").await.unwrap();
    reader.simple_query("BEGIN").await.unwrap();
    assert_eq!(
        code(
            reader
                .simple_query("LOCK lock_a IN SHARE MODE")
                .await
                .unwrap_err()
        ),
        "42501"
    );
    reader.simple_query("ROLLBACK").await.unwrap();
    c.simple_query("GRANT UPDATE ON lock_a TO lockreader")
        .await
        .unwrap();
    reader
        .simple_query("BEGIN; LOCK lock_a IN ACCESS EXCLUSIVE MODE NOWAIT; ROLLBACK")
        .await
        .unwrap();
    drop(reader);
    drop(c);
    f.stop().await;
}

#[tokio::test]
async fn pg_dump_access_method_handlers_are_named_on_the_wire() {
    let f = Fixture::new().await;
    let c = f.client().await;
    assert_eq!(
        values(
            &c.simple_query("SELECT amhandler::pg_catalog.regproc FROM pg_am ORDER BY oid")
                .await
                .unwrap()
        ),
        ["heap_tableam_handler", "bthandler"]
    );
    drop(c);
    f.stop().await;
}

#[tokio::test]
async fn text_copy_stdout_escapes_rows_and_preserves_nulls() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    async fn message(s: &mut tokio::net::TcpStream) -> (u8, Vec<u8>) {
        let tag = s.read_u8().await.unwrap();
        let len = s.read_u32().await.unwrap();
        let mut body = vec![0; (len - 4) as usize];
        s.read_exact(&mut body).await.unwrap();
        (tag, body)
    }
    let f = Fixture::new().await;
    let c = f.client().await;
    c.simple_query("CREATE TABLE copy_out (id integer PRIMARY KEY, body text, data bytea)")
        .await
        .unwrap();
    c.execute(
        "INSERT INTO copy_out VALUES ($1,$2,$3)",
        &[&1i32, &"tab\tline\nslash\\cr\r", &vec![0u8, 1, 255]],
    )
    .await
    .unwrap();
    c.simple_query("INSERT INTO copy_out VALUES (2,NULL,NULL)")
        .await
        .unwrap();
    let mut s = tokio::net::TcpStream::connect(f.pg.address).await.unwrap();
    let mut startup = 196608u32.to_be_bytes().to_vec();
    startup.extend(b"user\0owner\0database\0postgres\0\0");
    s.write_u32((startup.len() + 4) as u32).await.unwrap();
    s.write_all(&startup).await.unwrap();
    loop {
        if message(&mut s).await.0 == b'Z' {
            break;
        }
    }
    let sql = b"COPY public.copy_out (id,body,data) TO stdout;\0";
    s.write_u8(b'Q').await.unwrap();
    s.write_u32((sql.len() + 4) as u32).await.unwrap();
    s.write_all(sql).await.unwrap();
    let mut data = Vec::new();
    let mut done = false;
    let mut complete = false;
    loop {
        let (tag, body) = message(&mut s).await;
        match tag {
            b'H' => assert_eq!(body, [0, 0, 3, 0, 0, 0, 0, 0, 0]),
            b'd' => data.extend(body),
            b'c' => done = true,
            b'C' => {
                assert_eq!(body, b"COPY 2\0");
                complete = true;
            }
            b'Z' => break,
            _ => panic!("unexpected COPY message: {tag} {body:?}"),
        }
    }
    assert!(done && complete);
    assert_eq!(
        data,
        b"1\ttab\\tline\\nslash\\\\cr\\r\t\\\\x0001ff\n2\t\\N\t\\N\n"
    );
    s.shutdown().await.unwrap();
    drop(s);
    drop(c);
    f.stop().await;
}

#[tokio::test]
async fn copy_stdout_requires_select_and_a_real_table() {
    let f = Fixture::new().await;
    let c = f.client().await;
    c.simple_query("CREATE TABLE copy_private (id integer PRIMARY KEY)")
        .await
        .unwrap();
    c.simple_query("CREATE ROLE copyreader LOGIN")
        .await
        .unwrap();
    c.simple_query("GRANT INSERT ON copy_private TO copyreader")
        .await
        .unwrap();
    let reader = f.client_as("copyreader").await;
    assert_eq!(
        code(
            reader
                .simple_query("COPY copy_private TO stdout")
                .await
                .unwrap_err()
        ),
        "42501"
    );
    assert_eq!(
        code(
            c.simple_query("COPY missing_copy_table TO stdout")
                .await
                .unwrap_err()
        ),
        "42P01"
    );
    drop(reader);
    drop(c);
    f.stop().await;
}

#[tokio::test]
async fn dump_routes_resolve_schema_qualifiers_and_search_path() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    async fn message(s: &mut tokio::net::TcpStream) -> (u8, Vec<u8>) {
        let tag = s.read_u8().await.unwrap();
        let len = s.read_u32().await.unwrap();
        let mut body = vec![0; (len - 4) as usize];
        s.read_exact(&mut body).await.unwrap();
        (tag, body)
    }
    let f = Fixture::new().await;
    let c = f.client().await;
    for sql in [
        "CREATE SCHEMA dump_s",
        "CREATE TABLE dump_s.t(id integer PRIMARY KEY)",
        "INSERT INTO dump_s.t VALUES(9)",
    ] {
        c.batch_execute(sql).await.unwrap();
    }
    c.batch_execute("BEGIN; LOCK TABLE dump_s.t IN ACCESS SHARE MODE; COMMIT")
        .await
        .unwrap();
    c.batch_execute("SET search_path TO dump_s,public")
        .await
        .unwrap();
    c.batch_execute("BEGIN; LOCK TABLE t IN ACCESS SHARE MODE; COMMIT")
        .await
        .unwrap();
    c.batch_execute("SET search_path TO public; BEGIN")
        .await
        .unwrap();
    assert_eq!(
        code(
            c.batch_execute("LOCK TABLE t IN ACCESS SHARE MODE")
                .await
                .unwrap_err()
        ),
        "42P01"
    );
    c.batch_execute("ROLLBACK; BEGIN").await.unwrap();
    assert_eq!(
        code(
            c.batch_execute("LOCK TABLE public.t IN ACCESS SHARE MODE")
                .await
                .unwrap_err()
        ),
        "42P01"
    );
    c.batch_execute("ROLLBACK").await.unwrap();
    assert_eq!(
        code(c.simple_query("COPY public.t TO STDOUT").await.unwrap_err()),
        "42P01"
    );
    let mut s = tokio::net::TcpStream::connect(f.pg.address).await.unwrap();
    let mut startup = 196608u32.to_be_bytes().to_vec();
    startup.extend(b"user\0owner\0database\0postgres\0\0");
    s.write_u32((startup.len() + 4) as u32).await.unwrap();
    s.write_all(&startup).await.unwrap();
    loop {
        if message(&mut s).await.0 == b'Z' {
            break;
        }
    }
    for sql in [
        "COPY dump_s.t TO STDOUT",
        "SET search_path TO dump_s,public",
        "COPY t TO STDOUT",
    ] {
        let sql = format!("{sql}\0");
        s.write_u8(b'Q').await.unwrap();
        s.write_u32((sql.len() + 4) as u32).await.unwrap();
        s.write_all(sql.as_bytes()).await.unwrap();
        let mut data = Vec::new();
        loop {
            let (tag, body) = message(&mut s).await;
            match tag {
                b'd' => data.extend(body),
                b'E' => panic!("{body:?}"),
                b'Z' => break,
                _ => {}
            }
        }
        if sql.starts_with("COPY") {
            assert_eq!(data, b"9\n");
        }
    }
    s.shutdown().await.unwrap();
    drop(s);
    drop(c);
    f.stop().await;
}

// Expected payloads captured from PostgreSQL 18.6 (Docker postgres:18.6).
#[tokio::test]
async fn copy_formats_match_pg18_bytes() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    async fn message(s: &mut tokio::net::TcpStream) -> (u8, Vec<u8>) {
        let tag = s.read_u8().await.unwrap();
        let len = s.read_u32().await.unwrap();
        let mut body = vec![0; (len - 4) as usize];
        s.read_exact(&mut body).await.unwrap();
        (tag, body)
    }
    let f = Fixture::new().await;
    let c = f.client().await;
    c.simple_query(
        "CREATE TABLE copy_formats (id integer, body text, data bytea, n numeric, ts timestamptz)",
    )
    .await
    .unwrap();
    c.simple_query("INSERT INTO copy_formats VALUES (1,'a,b','\\x0001ff',12.50,'2001-01-01 00:00:00+00'),(2,'',NULL,NULL,NULL),(3,NULL,NULL,NULL,NULL)").await.unwrap();
    for (format, expected) in [
        (
            "csv, HEADER",
            &[
                105, 100, 44, 98, 111, 100, 121, 44, 100, 97, 116, 97, 44, 110, 44, 116, 115, 10,
                49, 44, 34, 97, 44, 98, 34, 44, 92, 120, 48, 48, 48, 49, 102, 102, 44, 49, 50, 46,
                53, 48, 44, 50, 48, 48, 49, 45, 48, 49, 45, 48, 49, 32, 48, 48, 58, 48, 48, 58, 48,
                48, 43, 48, 48, 10, 50, 44, 34, 34, 44, 44, 44, 10, 51, 44, 44, 44, 44, 10,
            ][..],
        ),
        (
            "binary",
            &[
                80, 71, 67, 79, 80, 89, 10, 255, 13, 10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0,
                4, 0, 0, 0, 1, 0, 0, 0, 3, 97, 44, 98, 0, 0, 0, 3, 0, 1, 255, 0, 0, 0, 12, 0, 2, 0,
                0, 0, 0, 0, 2, 0, 12, 19, 136, 0, 0, 0, 8, 0, 0, 28, 194, 169, 235, 64, 0, 0, 5, 0,
                0, 0, 4, 0, 0, 0, 2, 0, 0, 0, 0, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
                255, 255, 0, 5, 0, 0, 0, 4, 0, 0, 0, 3, 255, 255, 255, 255, 255, 255, 255, 255,
                255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
            ][..],
        ),
        (
            "csv, HEADER, DELIMITER '|', QUOTE '~', NULL 'nil'",
            &[
                105, 100, 124, 98, 111, 100, 121, 124, 100, 97, 116, 97, 124, 110, 124, 116, 115,
                10, 49, 124, 97, 44, 98, 124, 92, 120, 48, 48, 48, 49, 102, 102, 124, 49, 50, 46,
                53, 48, 124, 50, 48, 48, 49, 45, 48, 49, 45, 48, 49, 32, 48, 48, 58, 48, 48, 58,
                48, 48, 43, 48, 48, 10, 50, 124, 124, 110, 105, 108, 124, 110, 105, 108, 124, 110,
                105, 108, 10, 51, 124, 110, 105, 108, 124, 110, 105, 108, 124, 110, 105, 108, 124,
                110, 105, 108, 10,
            ][..],
        ),
    ] {
        let mut s = tokio::net::TcpStream::connect(f.pg.address).await.unwrap();
        let mut startup = 196608u32.to_be_bytes().to_vec();
        startup.extend(b"user\0owner\0database\0postgres\0\0");
        s.write_u32((startup.len() + 4) as u32).await.unwrap();
        s.write_all(&startup).await.unwrap();
        loop {
            if message(&mut s).await.0 == b'Z' {
                break;
            }
        }
        let sql =
            format!("COPY (SELECT * FROM copy_formats ORDER BY id) TO STDOUT (FORMAT {format});\0");
        let sql = sql.as_bytes();
        s.write_u8(b'Q').await.unwrap();
        s.write_u32((sql.len() + 4) as u32).await.unwrap();
        s.write_all(sql).await.unwrap();
        let mut data = Vec::new();
        let mut done = false;
        let mut complete = false;
        loop {
            let (tag, body) = message(&mut s).await;
            match tag {
                b'H' => assert_eq!(body[0], u8::from(format == "binary")),
                b'd' => data.extend(body),
                b'c' => done = true,
                b'C' => {
                    assert_eq!(body, b"COPY 3\0");
                    complete = true;
                }
                b'Z' => break,
                _ => panic!("unexpected COPY message: {tag} {body:?}"),
            }
        }
        assert!(done && complete);
        assert_eq!(data, expected, "{format}");
        s.shutdown().await.unwrap();
        drop(s);
    }
    drop(c);
    f.stop().await;
}

#[cfg(feature = "engine-rocksdb")]
async fn lock_fixture() -> (Fixture, Client, Client) {
    let f = Fixture::with_engine(giggles_core::Engine::V2).await;
    let a = f.client().await;
    let b = f.client().await;
    a.simple_query(
        "CREATE TABLE t(id integer PRIMARY KEY); CREATE TABLE u(id integer PRIMARY KEY)",
    )
    .await
    .unwrap();
    (f, a, b)
}

#[cfg(feature = "engine-rocksdb")]
async fn bounded_query(
    c: &Client,
    sql: &str,
) -> Result<Vec<SimpleQueryMessage>, tokio_postgres::Error> {
    tokio::time::timeout(std::time::Duration::from_secs(5), c.simple_query(sql))
        .await
        .expect("statement exceeded bounded wait")
}

#[cfg(feature = "engine-rocksdb")]
async fn blocked_then_release(a: &Client, b: &Client, sql: &str) -> Vec<SimpleQueryMessage> {
    let query = b.simple_query(sql);
    tokio::pin!(query);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(300), &mut query)
            .await
            .is_err(),
        "conflicting statement returned before release"
    );
    bounded_query(a, "COMMIT").await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), query)
        .await
        .expect("lock was not released")
        .unwrap()
}

#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn lock_access_exclusive_blocks_select() {
    let (f, a, b) = lock_fixture().await;
    a.simple_query("BEGIN; LOCK TABLE t IN ACCESS EXCLUSIVE MODE")
        .await
        .unwrap();
    blocked_then_release(&a, &b, "SELECT * FROM t").await;
    drop((a, b));
    f.stop().await;
}

#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn lock_nowait_wire_code_and_rollback_release() {
    let (f, a, b) = lock_fixture().await;
    a.simple_query("BEGIN; LOCK t IN ACCESS SHARE MODE")
        .await
        .unwrap();
    b.simple_query("BEGIN").await.unwrap();
    // Exercise extended execution as well as simple-query locking.
    let error = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        b.query("LOCK t IN ACCESS EXCLUSIVE MODE NOWAIT", &[]),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert_eq!(code(error), "55P03");
    a.simple_query("ROLLBACK").await.unwrap();
    b.simple_query("ROLLBACK; BEGIN").await.unwrap();
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        b.query("LOCK t IN ACCESS EXCLUSIVE MODE NOWAIT", &[]),
    )
    .await
    .unwrap()
    .unwrap();
    b.simple_query("ROLLBACK").await.unwrap();
    drop((a, b));
    f.stop().await;
}

#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn lock_share_blocks_insert() {
    let (f, a, b) = lock_fixture().await;
    a.simple_query("BEGIN; LOCK t IN SHARE MODE").await.unwrap();
    blocked_then_release(&a, &b, "INSERT INTO t VALUES (1)").await;
    assert_eq!(
        values(&a.simple_query("SELECT id FROM t").await.unwrap()),
        ["1"]
    );
    drop((a, b));
    f.stop().await;
}

#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn lock_share_compatible_and_reentrant() {
    let (f, a, b) = lock_fixture().await;
    a.simple_query("BEGIN; LOCK t IN SHARE MODE").await.unwrap();
    b.simple_query("BEGIN").await.unwrap();
    bounded_query(&b, "LOCK t, public.t IN SHARE MODE NOWAIT")
        .await
        .unwrap();
    bounded_query(&b, "LOCK t IN ROW SHARE MODE NOWAIT")
        .await
        .unwrap();
    b.simple_query("ROLLBACK").await.unwrap();
    a.simple_query("ROLLBACK").await.unwrap();
    drop((a, b));
    f.stop().await;
}

#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn lock_exclusive_allows_select() {
    let (f, a, b) = lock_fixture().await;
    a.simple_query("BEGIN; LOCK t IN EXCLUSIVE MODE")
        .await
        .unwrap();
    bounded_query(&b, "SELECT * FROM t").await.unwrap();
    a.simple_query("ROLLBACK").await.unwrap();
    drop((a, b));
    f.stop().await;
}

#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn lock_deadlock_reports_40p01() {
    let (f, a, b) = lock_fixture().await;
    a.simple_query("BEGIN; LOCK t").await.unwrap();
    b.simple_query("BEGIN; LOCK u").await.unwrap();
    {
        let waiting = a.simple_query("LOCK u");
        tokio::pin!(waiting);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(300), &mut waiting)
                .await
                .is_err()
        );
        assert_eq!(
            code(bounded_query(&b, "LOCK t").await.unwrap_err()),
            "40P01"
        );
        b.simple_query("ROLLBACK").await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), waiting)
            .await
            .unwrap()
            .unwrap();
    }
    a.simple_query("ROLLBACK").await.unwrap();
    drop((a, b));
    f.stop().await;
}

#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn lock_repeatable_read_select_refreshes_after_grant() {
    let (f, a, b) = lock_fixture().await;
    a.simple_query("BEGIN; INSERT INTO t VALUES (1)")
        .await
        .unwrap();
    b.simple_query("BEGIN ISOLATION LEVEL REPEATABLE READ")
        .await
        .unwrap();
    blocked_then_release(&a, &b, "LOCK t IN SHARE MODE").await;
    assert_eq!(
        values(&bounded_query(&b, "SELECT id FROM t").await.unwrap()),
        ["1"]
    );
    b.simple_query("ROLLBACK").await.unwrap();
    drop((a, b));
    f.stop().await;
}
