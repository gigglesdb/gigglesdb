#![cfg(feature = "engine-rocksdb")]

use giggles_core::{Admin, Engine, Limits, roles::NewRole};
use giggles_server::{
    Service,
    auth::{PostgresqlConfig, TlsMode, serve_postgresql},
    clock::Clock,
};
use std::{collections::BTreeMap, sync::Arc};
use tokio_postgres::{NoTls, SimpleQueryMessage};

const BODY: &str = r#"
BEGIN
  DELETE FROM "SearchVector" WHERE
    ("entityType" = TG_ARGV[0] AND "entityId" = OLD.id)
    OR (TG_ARGV[0] = 'design-doc' AND (
      ("entityType" = 'supporting-doc' AND "entityId" IN
        (SELECT id FROM "DesignDocSupportingDoc" WHERE "designDocId" = OLD.id))
      OR ("entityType" = 'knowledge-item' AND "entityId" IN
        (SELECT id FROM "DesignDocKnowledgeItem" WHERE "designDocId" = OLD.id))
    ));
  IF TG_OP = 'DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF;
END;
"#;

#[tokio::test]
async fn archdocs_trigger_ddl_and_pg_dump_catalog_queries() {
    let path = std::env::temp_dir().join(format!(
        "triggers-wire-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
    admin
        .roles()
        .create(NewRole {
            name: "owner".into(),
            login: true,
            superuser: true,
            ..Default::default()
        })
        .unwrap();
    admin
        .roles()
        .create(NewRole {
            name: "reader".into(),
            login: true,
            ..Default::default()
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
    let running = serve_postgresql(
        config.listen,
        service.clone(),
        Arc::new(config.build().unwrap()),
    )
    .await
    .unwrap();
    let config = |user| {
        let mut config = tokio_postgres::Config::new();
        config
            .hostaddr(running.address.ip())
            .port(running.address.port())
            .user(user)
            .dbname("postgres");
        config
    };
    let (client, connection) = config("owner").connect(NoTls).await.unwrap();
    let connection = tokio::spawn(connection);

    client.batch_execute(r#"
        CREATE TABLE "DesignDoc" (id uuid PRIMARY KEY, title text, "deletedAt" TIMESTAMP(3));
        CREATE TABLE "DesignDocSupportingDoc" (id uuid PRIMARY KEY, "designDocId" uuid REFERENCES "DesignDoc"(id) ON DELETE CASCADE, "archivedAt" TIMESTAMP(3), note text);
        CREATE TABLE "DesignDocKnowledgeItem" (id uuid PRIMARY KEY, "designDocId" uuid REFERENCES "DesignDoc"(id) ON DELETE CASCADE, "retiredAt" TIMESTAMP(3), "suppressedAt" TIMESTAMP(3), note text);
        CREATE TABLE "SearchVector" (id uuid PRIMARY KEY DEFAULT gen_random_uuid(), "entityType" text NOT NULL, "entityId" uuid NOT NULL);
    "#).await.unwrap();
    client.batch_execute(&format!("CREATE FUNCTION cleanup_semantic_vectors() RETURNS trigger LANGUAGE plpgsql AS $body${BODY}$body$;")).await.unwrap();
    let triggers = [
        (
            "cleanup_design_doc_vectors",
            "DesignDoc",
            "\"deletedAt\"",
            "3",
            "design-doc",
        ),
        (
            "cleanup_knowledge_item_vectors",
            "DesignDocKnowledgeItem",
            "\"retiredAt\", \"suppressedAt\"",
            "3 4",
            "knowledge-item",
        ),
        (
            "cleanup_supporting_doc_vectors",
            "DesignDocSupportingDoc",
            "\"archivedAt\"",
            "3",
            "supporting-doc",
        ),
    ];
    for (name, table, columns, _, arg) in triggers {
        client
            .execute(
                &format!(
                    "CREATE TRIGGER {name} BEFORE DELETE OR UPDATE OF {columns} ON \"{table}\" \
            FOR EACH ROW EXECUTE FUNCTION cleanup_semantic_vectors('{arg}')"
                ),
                &[],
            )
            .await
            .unwrap();
    }
    let rows = client
        .simple_query(
            "SELECT tgname, tgfoid::regproc, tgtype, tgenabled, tgisinternal, tgnargs, tgattr, \
        encode(tgargs,'escape'), pg_get_triggerdef(oid), pg_get_triggerdef(oid,true) \
        FROM pg_catalog.pg_trigger WHERE NOT tgisinternal ORDER BY tgname",
        )
        .await
        .unwrap();
    let rows: Vec<_> = rows
        .iter()
        .filter_map(|msg| match msg {
            SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .collect();
    assert_eq!(rows.len(), 3);
    for (row, (name, table, columns, attnums, arg)) in rows.into_iter().zip(triggers) {
        for (index, expected) in [
            name,
            "cleanup_semantic_vectors",
            "27",
            "O",
            "f",
            "1",
            attnums,
            &format!("{arg}\\000"),
        ]
        .into_iter()
        .enumerate()
        {
            assert_eq!(row.get(index), Some(expected));
        }
        for (i, prefix) in [(8, "public."), (9, "")] {
            assert_eq!(
                row.get(i).unwrap(),
                format!(
                    "CREATE TRIGGER {name} BEFORE DELETE OR UPDATE OF {columns} ON {prefix}\"{table}\" FOR EACH ROW EXECUTE FUNCTION cleanup_semantic_vectors('{arg}')"
                )
            );
        }
    }
    let rows = client.query("SELECT tgname, pg_get_triggerdef(oid) FROM pg_trigger WHERE NOT tgisinternal ORDER BY tgname", &[]).await.unwrap();
    assert_eq!(rows.len(), 3);
    let def: String = client
        .query_one(
            "SELECT pg_get_functiondef('cleanup_semantic_vectors'::regproc)",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        def,
        format!(
            "CREATE OR REPLACE FUNCTION public.cleanup_semantic_vectors()\n RETURNS trigger\n LANGUAGE plpgsql\nAS $function${BODY}$function$\n"
        )
    );
    let owner: bool = client.query_one("SELECT proowner=(SELECT oid FROM pg_roles WHERE rolname=current_user) FROM pg_proc WHERE proname='cleanup_semantic_vectors'", &[]).await.unwrap().get(0);
    assert!(owner);

    let (reader, mut reader_connection) = config("reader").connect(NoTls).await.unwrap();
    let (notice_tx, mut notices) = tokio::sync::mpsc::unbounded_channel();
    let reader_connection = tokio::spawn(async move {
        while let Some(message) = std::future::poll_fn(|cx| reader_connection.poll_message(cx)).await {
            if let tokio_postgres::AsyncMessage::Notice(notice) = message? {
                let _ = notice_tx.send(notice);
            }
        }
        Ok::<(), tokio_postgres::Error>(())
    });
    let sql = "CREATE TRIGGER reader_trigger AFTER DELETE ON \"DesignDoc\" EXECUTE FUNCTION cleanup_semantic_vectors()";
    let error = reader.batch_execute(sql).await.unwrap_err();
    let error = error.as_db_error().unwrap();
    assert_eq!(error.code().code(), "42501");
    assert_eq!(error.message(), "permission denied for table DesignDoc");
    client
        .batch_execute("GRANT TRIGGER ON \"DesignDoc\" TO reader")
        .await
        .unwrap();
    reader.batch_execute(sql).await.unwrap();
    client.batch_execute("REVOKE ALL ON \"DesignDoc\" FROM reader").await.unwrap();
    assert_wire_error(&reader,
        "CREATE TRIGGER revoked_trigger AFTER DELETE ON \"DesignDoc\" EXECUTE FUNCTION cleanup_semantic_vectors()",
        "42501", "permission denied for table DesignDoc").await;
    for (grant, name) in [("ALL", "all_trigger"), ("ALL PRIVILEGES", "all_privileges_trigger")] {
        client.batch_execute(&format!("GRANT {grant} ON TABLE \"DesignDoc\" TO reader")).await.unwrap();
        reader.batch_execute(&format!(
            "CREATE TRIGGER {name} AFTER DELETE ON \"DesignDoc\" EXECUTE FUNCTION cleanup_semantic_vectors()",
        )).await.unwrap();
        client.batch_execute("REVOKE ALL ON \"DesignDoc\" FROM reader").await.unwrap();
    }

    client.batch_execute("CREATE TABLE t (id int); CREATE TABLE secret (id int); \
        CREATE FUNCTION f1() RETURNS trigger LANGUAGE plpgsql SECURITY INVOKER AS $$ BEGIN RETURN NEW; END $$; \
        CREATE TRIGGER tr_owner BEFORE INSERT ON t FOR EACH ROW EXECUTE FUNCTION f1()").await.unwrap();
    for sql in [
        "CREATE FUNCTION refused() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER AS $$ BEGIN RETURN NEW; END $$",
        "CREATE OR REPLACE FUNCTION f1() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER AS $$ BEGIN RETURN OLD; END $$",
    ] {
        assert_wire_error(&client, sql, "0A000", "SECURITY DEFINER functions are not supported").await;
    }
    let row = client.query_one("SELECT prosrc, prosecdef FROM pg_proc WHERE proname = 'f1'", &[]).await.unwrap();
    assert_eq!(row.get::<_, &str>(0), " BEGIN RETURN NEW; END ");
    assert!(!row.get::<_, bool>(1));
    assert!(client.query_opt("SELECT oid FROM pg_proc WHERE proname = 'refused'", &[]).await.unwrap().is_none());

    for (sql, code, message) in [
        ("DROP FUNCTION f1()", "42501", "must be owner of function f1"),
        ("DROP TRIGGER tr_owner ON t", "42501", "must be owner of relation t"),
        ("DROP TRIGGER nosuch ON t", "42704", "trigger \"nosuch\" for table \"t\" does not exist"),
        ("ALTER TRIGGER tr_owner ON t RENAME TO tr_x", "42501", "must be owner of table t"),
        ("ALTER TABLE t DISABLE TRIGGER tr_owner", "42501", "must be owner of table t"),
        ("ALTER TABLE t ENABLE TRIGGER ALL", "42501", "must be owner of table t"),
        ("SELECT f1() FROM secret", "42501", "permission denied for table secret"),
        ("SELECT f1() FROM secret WHERE id = 1", "42501", "permission denied for table secret"),
        ("SELECT CASE WHEN false THEN f1() END", "0A000", "cannot accept a value of type trigger"),
    ] {
        assert_wire_error(&reader, sql, code, message).await;
    }
    for sql in [
        "SELECT nosuch_fn() FROM secret",
        "SELECT nosuch_fn() FROM secret WHERE id = 1",
        "SELECT nosuch_fn() FROM secret ORDER BY 1",
        "SELECT nosuch_fn(), 1 FROM secret",
        "SELECT nosuch_fn(id) FROM secret",
        "SELECT abs(id) FROM secret",
    ] {
        assert_wire_error(&reader, sql, "42501", "request rejected; consult documented code").await;
    }
    reader.batch_execute("DROP TRIGGER IF EXISTS nosuch ON t").await.unwrap();
    let notice = tokio::time::timeout(std::time::Duration::from_secs(5), notices.recv()).await.unwrap().unwrap();
    assert_eq!(notice.message(), "trigger \"nosuch\" for relation \"t\" does not exist, skipping");
    let replace = "CREATE OR REPLACE FUNCTION f1() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$";
    assert_wire_error(&reader, replace, "42501", "permission denied for schema public").await;
    client.batch_execute("GRANT CREATE ON SCHEMA public TO reader").await.unwrap();
    assert_wire_error(&reader, replace, "42501", "must be owner of function f1").await;

    let rows = reader.simple_query("SELECT f1() WHERE false").await.unwrap();
    assert!(!rows.iter().any(|message| matches!(message, SimpleQueryMessage::Row(_))));
    reader.batch_execute("PREPARE p (int) AS SELECT $1").await.unwrap();
    let rows = reader.simple_query("EXECUTE p(1)").await.unwrap();
    assert!(rows.iter().any(|message| matches!(message, SimpleQueryMessage::Row(row) if row.get(0) == Some("1"))));
    reader.batch_execute("DEALLOCATE p; DEALLOCATE ALL").await.unwrap();
    reader.batch_execute("PREPARE TRANSACTION 'f2'").await.unwrap();
    reader.batch_execute("BEGIN").await.unwrap();
    assert_wire_error(&reader, "PREPARE TRANSACTION 'f2'", "55000", "prepared transactions are disabled").await;
    reader.batch_execute("ROLLBACK").await.unwrap();
    for sql in ["SELECT f1()", "SELECT public.f1()"] {
        let error = reader.simple_query(sql).await.unwrap_err();
        let error = error.as_db_error().unwrap();
        assert_eq!(error.code().code(), "0A000", "{sql}");
        assert_eq!(error.message(), "trigger functions can only be called as triggers", "{sql}");
        assert_eq!(error.where_(), Some("compilation of PL/pgSQL function \"f1\" near line 1"), "{sql}");
    }
    let prepared = reader.prepare("SELECT f1()").await.unwrap();
    assert_eq!(prepared.columns()[0].type_().oid(), 2279);
    let error = reader.query(&prepared, &[]).await.unwrap_err();
    assert_eq!(error.as_db_error().unwrap().message(), "trigger functions can only be called as triggers");
    for (sql, signature) in [
        ("SELECT f1(1)", "f1(integer)"),
        ("SELECT public.f1(1)", "public.f1(integer)"),
        ("SELECT f1('a')", "f1(unknown)"),
    ] {
        let error = reader.batch_execute(sql).await.unwrap_err();
        let error = error.as_db_error().unwrap();
        assert_eq!(error.code().code(), "42883", "{sql}");
        assert_eq!(error.message(), format!("function {signature} does not exist"));
        assert_eq!(error.hint(), Some("No function matches the given name and argument types. You might need to add explicit type casts."));
    }
    client.batch_execute("INSERT INTO secret VALUES (1)").await.unwrap();
    client.batch_execute("GRANT SELECT ON secret TO reader").await.unwrap();
    assert_wire_error(&reader, "SELECT f1() FROM secret", "0A000", "trigger functions can only be called as triggers").await;

    check_function_body_setting(&client).await;
    check_drop_trigger_after_snapshot(&client, &reader).await;
    check_copy_admission(&client, &reader).await;

    let error = client
        .batch_execute("DROP FUNCTION cleanup_semantic_vectors()")
        .await
        .unwrap_err();
    let error = error.as_db_error().unwrap();
    assert_eq!(error.code().code(), "2BP01");
    assert!(
        error
            .detail()
            .unwrap()
            .contains("trigger cleanup_design_doc_vectors")
    );
    assert_eq!(
        error.hint(),
        Some("Use DROP ... CASCADE to drop the dependent objects too.")
    );

    drop(reader);
    reader_connection.await.unwrap().unwrap();
    drop(client);
    connection.await.unwrap().unwrap();
    running.shutdown().await.unwrap();
    drop(service);
    std::fs::remove_dir_all(path).unwrap();
}

async fn assert_wire_error(client: &tokio_postgres::Client, sql: &str, code: &str, message: &str) {
    let error = client.batch_execute(sql).await.unwrap_err();
    let error = error.as_db_error().unwrap();
    assert_eq!(error.code().code(), code, "{sql}");
    assert_eq!(error.message(), message, "{sql}");
}

async fn check_function_body_setting(client: &tokio_postgres::Client) {
    let malformed = "CREATE FUNCTION late_body() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW END $$";
    let show = "SHOW check_function_bodies";
    let settings = client.simple_query(
        "SELECT name, setting, unit, category, short_desc, extra_desc, context, vartype, source, min_val, max_val, enumvals, boot_val, reset_val, sourcefile, sourceline, pending_restart FROM pg_settings WHERE name = 'check_function_bodies'",
    ).await.unwrap();
    let [SimpleQueryMessage::RowDescription(_), SimpleQueryMessage::Row(setting), SimpleQueryMessage::CommandComplete(_)] = settings.as_slice() else {
        panic!("expected one check_function_bodies row");
    };
    assert_eq!((0..17).map(|i| setting.get(i)).collect::<Vec<_>>(), [
        Some("check_function_bodies"), Some("on"), None,
        Some("Client Connection Defaults / Statement Behavior"),
        Some("Check routine bodies during CREATE FUNCTION and CREATE PROCEDURE."),
        None, Some("user"), Some("bool"), Some("default"), None, None, None,
        Some("on"), Some("on"), None, None, Some("f"),
    ]);
    assert_wire_error(client, malformed, "42601", "syntax error at end of input").await;
    client
        .batch_execute("BEGIN; SET LOCAL check_function_bodies = 'FALSE'")
        .await
        .unwrap();
    assert_eq!(
        client.query_one(show, &[]).await.unwrap().get::<_, &str>(0),
        "off"
    );
    // Extended protocol reads the setting at Execute.
    let prepared = client.prepare(malformed).await.unwrap();
    client.execute(&prepared, &[]).await.unwrap();
    client.batch_execute("COMMIT").await.unwrap();
    assert_eq!(
        client.query_one(show, &[]).await.unwrap().get::<_, &str>(0),
        "on"
    );
    assert_eq!(
        client
            .query_one(
                "SELECT prosrc FROM pg_proc WHERE proname = 'late_body'",
                &[]
            )
            .await
            .unwrap()
            .get::<_, &str>(0),
        " BEGIN RETURN NEW END ",
    );
    assert_wire_error(
        client,
        "SELECT late_body()",
        "0A000",
        "trigger functions can only be called as triggers",
    )
    .await;
    let setting = client
        .query_one(
            "SELECT set_config('check_function_bodies', 'of', false)",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(setting.get::<_, &str>(0), "off");
    assert_eq!(
        client
            .query_one("SELECT current_setting('check_function_bodies')", &[])
            .await
            .unwrap()
            .get::<_, &str>(0),
        "off"
    );
    client
        .batch_execute(&malformed.replace("late_body", "late_body_simple"))
        .await
        .unwrap();
    client
        .batch_execute("RESET check_function_bodies")
        .await
        .unwrap();
    assert_eq!(
        client.query_one(show, &[]).await.unwrap().get::<_, &str>(0),
        "on"
    );
    for sql in [
        "SET check_function_bodies = 'o'",
        "SELECT set_config('check_function_bodies', 'bogus', false)",
    ] {
        assert_wire_error(
            client,
            sql,
            "22023",
            "parameter \"check_function_bodies\" requires a Boolean value",
        )
        .await;
    }
}

async fn check_drop_trigger_after_snapshot(
    owner: &tokio_postgres::Client,
    reader: &tokio_postgres::Client,
) {
    reader
        .batch_execute("CREATE TABLE drop_trigger_race (id int)")
        .await
        .unwrap();
    reader
        .batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ; SELECT id FROM secret")
        .await
        .unwrap();
    owner.batch_execute("CREATE TRIGGER appeared BEFORE INSERT ON drop_trigger_race FOR EACH ROW EXECUTE FUNCTION f1()").await.unwrap();
    reader
        .batch_execute("DROP TRIGGER IF EXISTS appeared ON drop_trigger_race; COMMIT")
        .await
        .unwrap();
    assert!(
        owner
            .query_opt("SELECT oid FROM pg_trigger WHERE tgname = 'appeared'", &[])
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        reader
            .execute("INSERT INTO drop_trigger_race VALUES (1)", &[])
            .await
            .unwrap(),
        1
    );

    reader
        .batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ; SELECT id FROM secret")
        .await
        .unwrap();
    owner
        .batch_execute(
            "CREATE TRIGGER appeared BEFORE INSERT ON t FOR EACH ROW EXECUTE FUNCTION f1()",
        )
        .await
        .unwrap();
    assert_wire_error(
        reader,
        "DROP TRIGGER IF EXISTS appeared ON t",
        "42501",
        "must be owner of relation t",
    )
    .await;
    reader.batch_execute("ROLLBACK").await.unwrap();
    assert!(
        owner
            .query_opt("SELECT oid FROM pg_trigger WHERE tgname = 'appeared'", &[])
            .await
            .unwrap()
            .is_some()
    );
    owner
        .batch_execute("DROP TRIGGER appeared ON t")
        .await
        .unwrap();
}

async fn check_copy_admission(owner: &tokio_postgres::Client, writer: &tokio_postgres::Client) {
    owner
        .batch_execute("CREATE TABLE copy_trigger_t (id int PRIMARY KEY); INSERT INTO copy_trigger_t VALUES (1)")
        .await
        .unwrap();
    owner
        .batch_execute("GRANT INSERT ON copy_trigger_t TO reader")
        .await
        .unwrap();
    let create = "CREATE TRIGGER copy_guard BEFORE INSERT ON copy_trigger_t FOR EACH ROW EXECUTE FUNCTION f1()";
    let query = "SELECT id FROM copy_trigger_t ORDER BY id";
    let original_rows = vec![1];
    owner.batch_execute(create).await.unwrap();
    let sql = "COPY copy_trigger_t FROM STDIN";
    assert_wire_error(
        writer,
        sql,
        "0A000",
        "COPY FROM on a table with triggers is not supported",
    )
    .await;
    assert_eq!(
        owner
            .query(query, &[])
            .await
            .unwrap()
            .iter()
            .map(|row| row.get::<_, i32>(0))
            .collect::<Vec<_>>(),
        original_rows
    );
    owner
        .batch_execute("DROP TRIGGER copy_guard ON copy_trigger_t")
        .await
        .unwrap();

    // Start COPY before the DDL commits. CopyDone must still refuse the
    // buffered input, including when its transaction snapshot predates DDL.
    writer
        .batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ; SELECT id FROM secret")
        .await
        .unwrap();
    let sink = writer
        .copy_in("COPY copy_trigger_t FROM STDIN (FORMAT BINARY)")
        .await
        .unwrap();
    let copy = tokio_postgres::binary_copy::BinaryCopyInWriter::new(
        sink,
        &[tokio_postgres::types::Type::INT4],
    );
    let mut copy = std::pin::pin!(copy);
    copy.as_mut().write(&[&2i32]).await.unwrap();
    owner.batch_execute(create).await.unwrap();
    let error = copy.as_mut().finish().await.unwrap_err();
    let error = error.as_db_error().unwrap();
    assert_eq!(error.code().code(), "0A000");
    assert_eq!(
        error.message(),
        "COPY FROM on a table with triggers is not supported"
    );
    writer.batch_execute("ROLLBACK").await.unwrap();
    assert_eq!(
        owner
            .query(query, &[])
            .await
            .unwrap()
            .iter()
            .map(|row| row.get::<_, i32>(0))
            .collect::<Vec<_>>(),
        original_rows
    );
}
