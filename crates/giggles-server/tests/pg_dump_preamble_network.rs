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
        let admin = Admin::create(path.join("db.redb"), Limits::default()).unwrap();
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
        let (client, connection) = Config::new()
            .host("localhost")
            .hostaddr(self.pg.address.ip())
            .port(self.pg.address.port())
            .user("owner")
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
        c.simple_query("CREATE TABLE t (id integer PRIMARY KEY, body text)")
            .await
            .unwrap();
        c.execute("INSERT INTO t VALUES (1, 'restored')", &[])
            .await
            .unwrap();
        assert_eq!(
            values(&c.simple_query("SELECT body FROM t LIMIT 1").await.unwrap()),
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
            c.simple_query("DROP TABLE t2").await.unwrap();
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
            (
                "SELECT set_config(name, 'UTC', false) FROM pg_settings WHERE name = 'timezone'",
                Some("UTC"),
            ),
            (
                "SELECT set_config(name, 'ISO, MDY', false) FROM pg_settings WHERE name = 'datestyle'",
                Some("ISO, MDY"),
            ),
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
