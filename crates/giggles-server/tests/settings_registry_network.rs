use super::session_settings_network::Fixture;
use tokio_postgres::{Client, SimpleQueryMessage};

#[cfg(feature = "engine-rocksdb")]
mod agent_support;

async fn simple_text(client: &Client, sql: &str) -> Vec<Vec<Option<String>>> {
    client
        .simple_query(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error:?}"))
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|i| row.get(i).map(str::to_owned))
                    .collect(),
            ),
            _ => None,
        })
        .collect()
}

async fn check_display(client: &Client, name: &str, expected: &str) {
    let show = format!("SHOW {name}");
    assert_eq!(
        simple_text(client, &show).await,
        vec![vec![Some(expected.into())]]
    );
    let rows = client.query(&show, &[]).await.unwrap();
    assert_eq!(rows.len(), 1, "{show}");
    assert_eq!(rows[0].get::<_, &str>(0), expected, "{show}");
    let current = format!("SELECT current_setting('{name}')");
    assert_eq!(
        simple_text(client, &current).await,
        vec![vec![Some(expected.into())]]
    );
    assert_eq!(
        client
            .query_one(&current, &[])
            .await
            .unwrap()
            .get::<_, &str>(0),
        expected,
        "{current}"
    );
}

#[tokio::test]
async fn settings_are_session_local_and_share_a_single_view_across_protocols() {
    let fixture = Fixture::new().await;
    let client = fixture.client().await;
    let other = fixture.client().await;

    client
        .simple_query("SET \"Work_Mem\" TO '16MB'")
        .await
        .unwrap();
    client
        .execute("SET SESSION statement_timeout = '5s'", &[])
        .await
        .unwrap();
    client
        .simple_query("SET enable_seqscan = OFF")
        .await
        .unwrap();
    client
        .execute("SET plan_cache_mode = 'FORCE_CUSTOM_PLAN'", &[])
        .await
        .unwrap();
    client
        .execute("SET app.tenant_id = '42'", &[])
        .await
        .unwrap();
    for (name, value) in [
        ("work_mem", "16MB"),
        ("statement_timeout", "5s"),
        ("enable_seqscan", "off"),
        ("plan_cache_mode", "force_custom_plan"),
        ("app.tenant_id", "42"),
    ] {
        check_display(&client, name, value).await;
    }
    check_display(&other, "work_mem", "4MB").await;
    let hidden = other
        .query_one("SELECT current_setting('app.tenant_id', true)", &[])
        .await
        .unwrap();
    assert_eq!(hidden.get::<_, Option<&str>>(0), None);

    let sql = "SELECT set_config('work_mem','32MB',false), current_setting('work_mem')";
    let row = client.query_one(sql, &[]).await.unwrap();
    assert_eq!(
        (row.get::<_, &str>(0), row.get::<_, &str>(1)),
        ("32MB", "32MB")
    );
    assert_eq!(
        simple_text(&client, "SELECT set_config('app.tenant_id','99',false)").await,
        vec![vec![Some("99".into())]]
    );

    for ending in ["COMMIT", "ROLLBACK"] {
        client
            .simple_query("BEGIN; SET LOCAL work_mem = '1MB'")
            .await
            .unwrap();
        check_display(&client, "work_mem", "1MB").await;
        client
            .execute("SET LOCAL app.tenant_id = '7'", &[])
            .await
            .unwrap();
        client.execute("SAVEPOINT s", &[]).await.unwrap();
        client.execute("SET work_mem = '2MB'", &[]).await.unwrap();
        check_display(&client, "work_mem", "2MB").await;
        client.execute("ROLLBACK TO s", &[]).await.unwrap();
        check_display(&client, "work_mem", "1MB").await;
        check_display(&client, "app.tenant_id", "7").await;
        client.execute(ending, &[]).await.unwrap();
        check_display(&client, "work_mem", "32MB").await;
        check_display(&client, "app.tenant_id", "99").await;
    }
    client
        .simple_query("BEGIN; SET work_mem = '4MB'; ROLLBACK")
        .await
        .unwrap();
    check_display(&client, "work_mem", "32MB").await;
    let local = client
        .query_one("SELECT set_config('work_mem','64MB',true)", &[])
        .await
        .unwrap();
    assert_eq!(local.get::<_, &str>(0), "64MB");
    check_display(&client, "work_mem", "32MB").await;

    client
        .execute("SET work_mem TO DEFAULT", &[])
        .await
        .unwrap();
    check_display(&client, "work_mem", "4MB").await;
    client.execute("RESET app.tenant_id", &[]).await.unwrap();
    check_display(&client, "app.tenant_id", "").await;
    client
        .simple_query("SET a.b.c = 'custom'; RESET ALL")
        .await
        .unwrap();
    for (name, value) in [
        ("work_mem", "4MB"),
        ("statement_timeout", "0"),
        ("enable_seqscan", "on"),
        ("plan_cache_mode", "auto"),
        ("a.b.c", ""),
    ] {
        check_display(&client, name, value).await;
    }
    drop((client, other));
    fixture.stop().await;
}

#[tokio::test]
async fn show_checks_the_full_name_before_lookup_in_both_protocols() {
    let fixture = Fixture::new().await;
    let client = fixture.client().await;
    for (sql, token) in [
        ("SHOW frobnicate 'a2'", "'a2'"),
        ("SHOW operation 'a2'", "'a2'"),
        ("SHOW work_mem 'x' EXTRA", "'x'"),
        ("SHOW work_mem *", "*"),
        ("SHOW work_mem != 1", "!="),
        ("SHOW work_mem +1", "+"),
        ("SHOW work_mem E'x'", "E'x'"),
        ("SHOW work_mem $body$x$body$", "$body$x$body$"),
        ("SHOW work_mem = 'x'", "="),
        ("SHOW ALL work_mem", "work_mem"),
        ("SHOW TIME ZONE UTC", "UTC"),
        ("SHOW TRANSACTION ISOLATION LEVEL EXTRA", "EXTRA"),
        ("SHOW SESSION AUTHORIZATION EXTRA", "EXTRA"),
    ] {
        let message = format!(r#"syntax error at or near "{token}""#);
        for error in [
            client.simple_query(sql).await.unwrap_err(),
            client.query(sql, &[]).await.unwrap_err(),
        ] {
            let error = error.as_db_error().unwrap();
            assert_eq!(error.code().code(), "42601", "{sql}: {error:?}");
            assert_eq!(error.message(), message, "{sql}");
        }
    }
    for name in ["frobnicate", "time", "session", "transaction"] {
        let message = format!(r#"unrecognized configuration parameter "{name}""#);
        check_error(&client, &format!("SHOW {name}"), "42704", &message, None, None).await;
    }
    client.simple_query("SET app.name = 'custom'").await.unwrap();
    let rows = client.query(r#"SHOW app . "name""#, &[]).await.unwrap();
    assert_eq!(rows[0].get::<_, &str>(0), "custom");
    for sql in [
        "SHOW ALL",
        "SHOW TIME ZONE",
        "SHOW TRANSACTION ISOLATION LEVEL",
        "SHOW SESSION AUTHORIZATION",
    ] {
        assert!(!client.query(sql, &[]).await.unwrap().is_empty(), "{sql}");
        assert!(!simple_text(&client, sql).await.is_empty(), "{sql}");
    }
    drop(client);
    fixture.stop().await;
}

async fn check_error(
    client: &Client,
    sql: &str,
    code: &str,
    message: &str,
    hint: Option<&str>,
    detail: Option<&str>,
) {
    let errors = [
        client.simple_query(sql).await.unwrap_err(),
        client.query(sql, &[]).await.unwrap_err(),
    ];
    for error in errors {
        let error = error.as_db_error().unwrap();
        assert_eq!(error.code().code(), code, "{sql}: {error:?}");
        assert_eq!(error.message(), message, "{sql}");
        assert_eq!(error.hint(), hint, "{sql}");
        assert_eq!(error.detail(), detail, "{sql}");
    }
}

#[tokio::test]
async fn setting_diagnostics_permissions_and_inventory_match_postgresql() {
    let fixture = Fixture::new().await;
    let client = fixture.client().await;
    for (sql, code, message, hint) in [
        (
            "SET work_mem = 32",
            "22023",
            "32 kB is outside the valid range for parameter \"work_mem\" (64 kB .. 2147483647 kB)",
            None,
        ),
        (
            "SELECT set_config('work_mem','16mb',false)",
            "22023",
            "invalid value for parameter \"work_mem\": \"16mb\"",
            Some("Valid units for this parameter are \"B\", \"kB\", \"MB\", \"GB\", and \"TB\"."),
        ),
        (
            "SET work_mem = '3000000000'",
            "22023",
            "invalid value for parameter \"work_mem\": \"3000000000\"",
            Some("Value exceeds integer range."),
        ),
        (
            "SET enable_seqscan = 'maybe'",
            "22023",
            "parameter \"enable_seqscan\" requires a Boolean value",
            None,
        ),
        (
            "SET plan_cache_mode = 'bad'",
            "22023",
            "invalid value for parameter \"plan_cache_mode\": \"bad\"",
            Some("Available values: auto, force_generic_plan, force_custom_plan."),
        ),
        (
            "SET shared_buffers='256MB'",
            "55P02",
            "parameter \"shared_buffers\" cannot be changed without restarting the server",
            None,
        ),
        (
            "SET autovacuum = off",
            "55P02",
            "parameter \"autovacuum\" cannot be changed now",
            None,
        ),
        (
            "SET data_checksums = off",
            "55P02",
            "parameter \"data_checksums\" cannot be changed",
            None,
        ),
        (
            "SET ignore_system_indexes = on",
            "55P02",
            "parameter \"ignore_system_indexes\" cannot be set after connection start",
            None,
        ),
        (
            "SET work_mem = '1MB', '2MB'",
            "22023",
            "SET work_mem takes only one argument",
            None,
        ),
        (
            "SET temp_tablespaces = 'a', 'b'",
            "42704",
            "tablespace \"a\" does not exist",
            None,
        ),
        (
            "SHOW no_such_setting",
            "42704",
            "unrecognized configuration parameter \"no_such_setting\"",
            None,
        ),
        (
            "SET no_such_setting = 1",
            "42704",
            "unrecognized configuration parameter \"no_such_setting\"",
            None,
        ),
        (
            "RESET no_such_setting",
            "42704",
            "unrecognized configuration parameter \"no_such_setting\"",
            None,
        ),
        (
            "SELECT current_setting('no_such_setting')",
            "42704",
            "unrecognized configuration parameter \"no_such_setting\"",
            None,
        ),
        (
            "SELECT set_config('no_such_setting','1',false)",
            "42704",
            "unrecognized configuration parameter \"no_such_setting\"",
            None,
        ),
        (
            "SHOW app.unknown",
            "42704",
            "unrecognized configuration parameter \"app.unknown\"",
            None,
        ),
    ] {
        check_error(&client, sql, code, message, hint, None).await;
    }
    assert_eq!(
        client
            .simple_query("SET work_mem=64kB")
            .await
            .unwrap_err()
            .as_db_error()
            .unwrap()
            .code()
            .code(),
        "42601"
    );
    client
        .simple_query("SET temp_tablespaces = ''")
        .await
        .unwrap();
    let count = "SELECT count(*) FROM pg_settings";
    assert_eq!(
        client.query_one(count, &[]).await.unwrap().get::<_, i64>(0),
        399
    );
    for (name, expected) in [("timezone", 0_i64), ("TimeZone", 1)] {
        let sql = format!("SELECT count(*) FROM pg_settings WHERE name = '{name}'");
        assert_eq!(
            simple_text(&client, &sql).await,
            vec![vec![Some(expected.to_string())]],
            "{sql}"
        );
        assert_eq!(
            client.query_one(&sql, &[]).await.unwrap().get::<_, i64>(0),
            expected,
            "{sql}"
        );
    }
    // Outside the bounded inventory, keep main's application SQL diagnostic.
    let unsupported = "SELECT count(*) FROM pg_settings ORDER BY name";
    for error in [
        client.simple_query(unsupported).await.unwrap_err(),
        client.query(unsupported, &[]).await.unwrap_err(),
    ] {
        let error = error.as_db_error().unwrap();
        assert_eq!(error.code().code(), "0A000");
        assert_eq!(
            error.message(),
            "unsupported SQL feature or parameter type in this server profile"
        );
    }
    let show = client.query("SHOW ALL", &[]).await.unwrap();
    assert_eq!(show.len(), 399);
    assert_eq!(
        show[0]
            .columns()
            .iter()
            .map(|column| column.name())
            .collect::<Vec<_>>(),
        ["name", "setting", "description"]
    );
    let names: Vec<_> = show
        .iter()
        .map(|row| row.get::<_, &str>("name").to_ascii_lowercase())
        .collect();
    assert!(names.is_sorted());
    assert_eq!(
        show.iter()
            .find(|r| r.get::<_, &str>(0) == "work_mem")
            .unwrap()
            .get::<_, &str>(2),
        "Sets the maximum memory to be used for query workspaces."
    );

    client.execute("SET work_mem = '16MB'", &[]).await.unwrap();
    let inventory = "SELECT name, setting, unit, source, boot_val, reset_val FROM pg_catalog.pg_show_all_settings() WHERE name = 'work_mem'";
    assert_eq!(
        simple_text(&client, inventory).await,
        vec![
            ["work_mem", "16384", "kB", "session", "4096", "4096"]
                .map(|value| Some(value.into()))
                .to_vec()
        ]
    );
    let row = client
        .query_one(
            "SELECT enumvals FROM pg_settings WHERE name='plan_cache_mode'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(
        row.get::<_, Vec<String>>(0),
        ["auto", "force_generic_plan", "force_custom_plan"]
    );
    assert_eq!(
        client
            .query_one("SELECT setting FROM pg_settings WHERE name='work_mem'", &[])
            .await
            .unwrap()
            .get::<_, &str>(0),
        "16384"
    );
    assert!(
        client
            .query("SELECT setting FROM pg_settings WHERE name='Work_Mem'", &[])
            .await
            .unwrap()
            .is_empty()
    );
    client.simple_query("SET app.tenant_id='42'").await.unwrap();
    assert!(
        client
            .query(
                "SELECT setting FROM pg_settings WHERE name='app.tenant_id'",
                &[]
            )
            .await
            .unwrap()
            .is_empty()
    );

    client
        .simple_query("CREATE ROLE settings_plain")
        .await
        .unwrap();
    client
        .simple_query("SET ROLE settings_plain")
        .await
        .unwrap();
    check_error(
        &client,
        "SET log_statement = 'all'",
        "42501",
        "permission denied to set parameter \"log_statement\"",
        None,
        None,
    )
    .await;
    for sql in [
        "SHOW data_directory",
        "SELECT current_setting('data_directory', true)",
    ] {
        check_error(&client, sql, "42501", "permission denied to examine \"data_directory\"", None,
            Some("Only roles with privileges of the \"pg_read_all_settings\" role may examine this parameter.")).await;
    }
    assert_eq!(
        client.query_one(count, &[]).await.unwrap().get::<_, i64>(0),
        375
    );
    assert_eq!(simple_text(&client, "SHOW ALL").await.len(), 375);
    assert!(
        client
            .query(
                "SELECT setting FROM pg_settings WHERE name='data_directory'",
                &[]
            )
            .await
            .unwrap()
            .is_empty()
    );
    client.simple_query("RESET ROLE").await.unwrap();
    check_display(&client, "data_directory", "").await;
    drop(client);
    fixture.stop().await;
}

#[tokio::test]
async fn text_search_config_and_infinite_real_range_diagnostics_match_postgresql() {
    let fixture = Fixture::new().await;
    let client = fixture.client().await;
    client
        .simple_query(r#"SET default_text_search_config = ' PG_CATALOG . "simple" '"#)
        .await
        .unwrap();
    check_display(&client, "default_text_search_config", "pg_catalog.simple").await;
    let row = client
        .query_one("SELECT set_config('default_text_search_config','HINDI',false)", &[])
        .await
        .unwrap();
    assert_eq!(row.get::<_, &str>(0), "pg_catalog.hindi");
    check_display(&client, "default_text_search_config", "pg_catalog.hindi").await;
    for sql in [
        "SET default_text_search_config = 'review_missing_config'",
        "SELECT set_config('default_text_search_config','review_missing_config',false)",
    ] {
        check_error(
            &client,
            sql,
            "22023",
            "invalid value for parameter \"default_text_search_config\": \"review_missing_config\"",
            None,
            None,
        )
        .await;
    }
    for value in ["Infinity", "-Infinity"] {
        check_error(
            &client,
            &format!("SET random_page_cost = '{value}'"),
            "22023",
            &format!("{value} is outside the valid range for parameter \"random_page_cost\" (0 .. 1.79769e+308)"),
            None,
            None,
        )
        .await;
    }
    drop(client);
    fixture.stop().await;
}

#[tokio::test]
async fn transaction_deferrable_checks_savepoints_and_the_first_query() {
    let fixture = Fixture::new().await;
    let client = fixture.client().await;
    for sql in [
        "SET TRANSACTION DEFERRABLE",
        "SET TRANSACTION NOT DEFERRABLE",
        "SET transaction_deferrable = off",
    ] {
        for (setup, reason) in [
            ("BEGIN; SAVEPOINT s", "cannot be called within a subtransaction"),
            ("BEGIN; SAVEPOINT s; SELECT 1", "cannot be called within a subtransaction"),
            ("BEGIN; SELECT 1", "must be called before any query"),
            ("BEGIN; SAVEPOINT s; SELECT 1; ROLLBACK TO s; RELEASE s", "must be called before any query"),
        ] {
            let expected = format!("SET TRANSACTION [NOT] DEFERRABLE {reason}");
            for extended in [false, true] {
                client.simple_query(setup).await.unwrap();
                let error = if extended {
                    client.execute(sql, &[]).await.unwrap_err()
                } else {
                    client.simple_query(sql).await.unwrap_err()
                };
                let error = error.as_db_error().unwrap();
                assert_eq!(error.code().code(), "25001", "{setup}; {sql}");
                assert_eq!(error.message(), expected, "{setup}; {sql}");
                client.simple_query("ROLLBACK").await.unwrap();
            }
        }
    }
    client
        .simple_query("BEGIN; SAVEPOINT s; RELEASE s; SET transaction_deferrable = on")
        .await
        .unwrap();
    check_display(&client, "transaction_deferrable", "on").await;
    client.simple_query("ROLLBACK").await.unwrap();
    drop(client);
    fixture.stop().await;
}

#[tokio::test]
async fn restating_transaction_isolation_preserves_deferrable_errors() {
    let fixture = Fixture::new().await;
    let client = fixture.client().await;
    for action in ["DEFERRABLE", "NOT DEFERRABLE"] {
        for sql in [
            format!("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE, {action}"),
            format!("SET TRANSACTION {action}, ISOLATION LEVEL SERIALIZABLE"),
        ] {
            for (setup, reason) in [
                ("SELECT 1", "must be called before any query"),
                ("SAVEPOINT s", "cannot be called within a subtransaction"),
                ("SAVEPOINT s; SELECT 1", "cannot be called within a subtransaction"),
            ] {
                for extended in [false, true] {
                    client.simple_query(&format!("BEGIN ISOLATION LEVEL SERIALIZABLE; {setup}"))
                        .await.unwrap();
                    let error = if extended {
                        client.execute(&sql, &[]).await.unwrap_err()
                    } else {
                        client.simple_query(&sql).await.unwrap_err()
                    };
                    let error = error.as_db_error().unwrap();
                    assert_eq!(error.code().code(), "25001", "{setup}; {sql}");
                    assert_eq!(
                        error.message(),
                        format!("SET TRANSACTION [NOT] DEFERRABLE {reason}"),
                        "{setup}; {sql}"
                    );
                    client.simple_query("ROLLBACK").await.unwrap();
                }
            }
        }
    }
    drop(client);
    fixture.stop().await;
}

#[tokio::test]
async fn deferrability_follows_transaction_and_session_modes() {
    let fixture = Fixture::new().await;
    let client = fixture.client().await;
    client
        .simple_query("START TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY DEFERRABLE")
        .await
        .unwrap();
    check_display(&client, "transaction_deferrable", "on").await;
    client.simple_query("END").await.unwrap();
    check_display(&client, "transaction_deferrable", "off").await;

    client
        .execute("SET SESSION CHARACTERISTICS AS TRANSACTION DEFERRABLE", &[])
        .await
        .unwrap();
    check_display(&client, "default_transaction_deferrable", "on").await;
    client
        .simple_query("BEGIN; SET TRANSACTION NOT DEFERRABLE")
        .await
        .unwrap();
    check_display(&client, "transaction_deferrable", "off").await;
    client.simple_query("ROLLBACK").await.unwrap();
    check_display(&client, "transaction_deferrable", "on").await;
    client
        .execute("SET default_transaction_deferrable TO 'f'", &[])
        .await
        .unwrap();
    check_display(&client, "default_transaction_deferrable", "off").await;
    drop(client);
    fixture.stop().await;
}

#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn registry_settings_keep_the_task_session_allow_list() {
    use agent_support::{agent_wire, error_of};
    use giggles_core::task::{StatementClass, TaskError};

    let (_server, _agent, _ordinary, mut wire) = agent_wire().await;
    for (sql, class) in [
        ("SET work_mem = '8MB'", StatementClass::Set),
        ("SET work_mem = '8MB', '16MB'", StatementClass::Set),
        ("RESET app.tenant_id", StatementClass::Set),
        ("SHOW work_mem", StatementClass::Show),
        ("SHOW ALL", StatementClass::Show),
        ("SELECT current_setting('work_mem')", StatementClass::Show),
        (
            "SELECT current_setting('application_name'), set_config('work_mem','8MB',false)",
            StatementClass::Set,
        ),
        (
            "SELECT name, setting FROM pg_settings WHERE name = 'work_mem'",
            StatementClass::Catalog,
        ),
    ] {
        let expected = Some((
            "0A000".into(),
            TaskError::NotAllowedInTaskSession(class).message().into(),
        ));
        assert_eq!(error_of(&wire.simple(sql).await), expected, "{sql}");
        assert_eq!(
            error_of(&wire.extended(sql, &[], 0).await),
            expected,
            "{sql}"
        );
    }
}

#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn task_sessions_refuse_new_settings_and_prepared_statement_forms() {
    use agent_support::{Msg, agent_wire, error_of};
    use giggles_core::task::{StatementClass, TaskError};

    let (_server, _agent, _ordinary, mut wire) = agent_wire().await;
    let refusal = |class| {
        Some(("0A000".into(), TaskError::NotAllowedInTaskSession(class).message().into()))
    };
    for (sql, expected) in [
        ("SELECT current_setting('application_name'), set_config('application_name','x',false)",
            refusal(StatementClass::Set)),
        ("SELECT set_config('application_name','x',false), current_setting('application_name')",
            refusal(StatementClass::Set)),
        ("SELECT name FROM pg_prepared_statements",
            refusal(StatementClass::Catalog)),
        ("SELECT pg_typeof(name) FROM pg_prepared_statements",
            refusal(StatementClass::Catalog)),
        (r#"SELECT p.name FROM "pg_catalog"."pg_prepared_statements" p JOIN shipments s ON true"#,
            refusal(StatementClass::Catalog)),
    ] {
        assert_eq!(error_of(&wire.simple(sql).await), expected, "{sql}");
        assert_eq!(error_of(&wire.extended(sql, &[], 0).await), expected, "{sql}");
    }
    for sql in ["PREPARE p AS SELECT 1", "EXECUTE p", "DEALLOCATE p", "DISCARD ALL"] {
        for messages in [wire.simple(sql).await, wire.extended(sql, &[], 0).await] {
            assert!(error_of(&messages).is_some(), "{sql}: {messages:?}");
            assert!(!messages.iter().any(|message| matches!(message, Msg::ParseComplete | Msg::Complete(_))), "{sql}: {messages:?}");
        }
    }

    let current = "SELECT current_setting('application_name'), current_setting('application_name')";
    for messages in [wire.simple(current).await, wire.extended(current, &[], 0).await] {
        assert!(messages.contains(&Msg::Row(vec![Some(String::new()), Some(String::new())])), "{messages:?}");
    }
}
