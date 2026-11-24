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

    let (reader, reader_connection) = config("reader").connect(NoTls).await.unwrap();
    let reader_connection = tokio::spawn(reader_connection);
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
