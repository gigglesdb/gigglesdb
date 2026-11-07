use giggles_core::{Admin, Limits, roles::NewRole};
use giggles_server::{
    Service,
    auth::{PostgresqlConfig, TlsMode, serve_postgresql},
    clock::Clock,
};
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio_postgres::{Client, Config, NoTls};
static SERIAL: AtomicU64 = AtomicU64::new(0);

async fn fixture() -> (giggles_server::Running, Client) {
    let path = std::env::temp_dir().join(format!(
        "secondary-index-network-{}-{}",
        std::process::id(),
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
        connections: 4,
    };
    let listener = config.build().unwrap();
    let running = serve_postgresql(config.listen, service, Arc::new(listener))
        .await
        .unwrap();
    let (client, connection) = Config::new()
        .host("127.0.0.1")
        .port(running.address.port())
        .user("owner")
        .dbname("postgres")
        .connect(NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    for statement in [
        "CREATE TABLE t (id BIGINT PRIMARY KEY, email TEXT, a INTEGER, b INTEGER)",
        "CREATE UNIQUE INDEX t_email_key ON t (email)",
        "CREATE INDEX t_ab_idx ON t (a, b)",
    ] {
        client.batch_execute(statement).await.unwrap();
    }
    (running, client)
}

#[tokio::test]
async fn secondary_indexes_pg_indexes_and_supported_inner_joins() {
    let (_running, client) = fixture().await;
    let handler = client
        .simple_query("SELECT amhandler FROM pg_catalog.pg_am WHERE oid=403")
        .await
        .unwrap();
    let handler = handler
        .iter()
        .find_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .expect("expected btree access method row");
    assert_eq!(handler.get(0), Some("bthandler"));
    let rows = client.query("SELECT indexname, indexdef FROM pg_catalog.pg_indexes WHERE tablename = 't' ORDER BY indexname", &[]).await.unwrap();
    assert_eq!(rows.len(), 3);
    let actual: Vec<(String, String)> = rows.iter().map(|r| (r.get(0), r.get(1))).collect();
    assert_eq!(
        actual,
        vec![
            (
                "t_ab_idx".into(),
                "CREATE INDEX t_ab_idx ON public.t USING btree (a, b)".into()
            ),
            (
                "t_email_key".into(),
                "CREATE UNIQUE INDEX t_email_key ON public.t USING btree (email)".into()
            ),
            (
                "t_pkey".into(),
                "CREATE UNIQUE INDEX t_pkey ON public.t USING btree (id)".into()
            ),
        ]
    );
    let oid: u32 = client
        .query_one(
            "SELECT oid FROM pg_catalog.pg_class WHERE relname = 't'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    let supported = format!(
        "SELECT c2.relname, i.indisprimary, i.indisunique, i.indisclustered, i.indisvalid, pg_catalog.pg_get_indexdef(i.indexrelid, 0, true), i.indisreplident, c2.reltablespace FROM pg_catalog.pg_class c JOIN pg_catalog.pg_index i ON c.oid=i.indrelid JOIN pg_catalog.pg_class c2 ON c2.oid=i.indexrelid WHERE c.oid={oid} ORDER BY i.indisprimary DESC, c2.relname"
    );
    let rows = client.query(&supported, &[]).await.unwrap();
    assert_eq!(rows.len(), 3);
    for (row, (name, primary, unique)) in rows.iter().zip([
        ("t_pkey", true, true),
        ("t_ab_idx", false, false),
        ("t_email_key", false, true),
    ]) {
        assert_eq!(row.get::<_, String>(0), name);
        assert_eq!(row.get::<_, bool>(1), primary);
        assert_eq!(row.get::<_, bool>(2), unique);
        assert!(!row.get::<_, bool>(3));
        assert!(row.get::<_, bool>(4));
        assert_eq!(
            row.get::<_, String>(5),
            actual
                .iter()
                .find(|(n, _)| n == name)
                .unwrap()
                .1
                .replace("ON public.t ", "ON t ")
        );
        assert!(!row.get::<_, bool>(6));
        assert_eq!(row.get::<_, u32>(7), 0);
    }
    let constraints = format!(
        "SELECT c2.relname, pg_catalog.pg_get_constraintdef(con.oid, true), con.contype, con.condeferrable, con.condeferred FROM pg_catalog.pg_class c JOIN pg_catalog.pg_index i ON c.oid=i.indrelid JOIN pg_catalog.pg_class c2 ON c2.oid=i.indexrelid JOIN pg_catalog.pg_constraint con ON con.conindid=i.indexrelid WHERE c.oid={oid} ORDER BY c2.relname"
    );
    let rows = client.query(&constraints, &[]).await.unwrap();
    assert_eq!(
        rows.len(),
        1,
        "secondary indexes have no pg_constraint rows"
    );
    assert_eq!(rows[0].get::<_, String>(0), "t_pkey");
    assert_eq!(rows[0].get::<_, String>(1), "PRIMARY KEY (id)");
    assert_eq!(rows[0].get::<_, i8>(2), b'p' as i8);
    assert!(!rows[0].get::<_, bool>(3));
    assert!(!rows[0].get::<_, bool>(4));
}

#[tokio::test]
async fn exact_psql_query_tracks_left_join_planning_gap() {
    let (_running, client) = fixture().await;
    let oid: u32 = client
        .query_one(
            "SELECT oid FROM pg_catalog.pg_class WHERE relname = 't'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    let exact = format!(
        "SELECT c2.relname, i.indisprimary, i.indisunique, i.indisclustered, i.indisvalid, pg_catalog.pg_get_indexdef(i.indexrelid, 0, true), pg_catalog.pg_get_constraintdef(con.oid, true), contype, condeferrable, condeferred, i.indisreplident, c2.reltablespace FROM pg_catalog.pg_class c, pg_catalog.pg_class c2, pg_catalog.pg_index i LEFT JOIN pg_catalog.pg_constraint con ON (conrelid = i.indrelid AND conindid = i.indexrelid AND contype IN ('p','u','x')) WHERE c.oid = '{oid}' AND c.oid = i.indrelid AND i.indexrelid = c2.oid ORDER BY i.indisprimary DESC, c2.relname;"
    );
    // Known gap: track G join planning (LEFT JOIN predicate placement). Flip to an exact-row assertion when fixed.
    let error = client.query(&exact, &[]).await.unwrap_err();
    assert_eq!(error.code().unwrap().code(), "54000");
}

#[tokio::test]
async fn secondary_index_catalog_is_committed_only() {
    let (_running, client) = fixture().await;
    client.batch_execute("BEGIN").await.unwrap();
    client
        .batch_execute("CREATE INDEX t_tmp ON t (a)")
        .await
        .unwrap();
    assert!(
        client
            .query(
                "SELECT indexname FROM pg_catalog.pg_indexes WHERE indexname = 't_tmp'",
                &[]
            )
            .await
            .unwrap()
            .is_empty()
    );
    client.batch_execute("COMMIT").await.unwrap();
    assert_eq!(
        client
            .query(
                "SELECT indexname FROM pg_catalog.pg_indexes WHERE indexname = 't_tmp'",
                &[]
            )
            .await
            .unwrap()
            .len(),
        1
    );
    client.batch_execute("BEGIN").await.unwrap();
    client
        .batch_execute("CREATE INDEX t_rollback ON t (b)")
        .await
        .unwrap();
    client.batch_execute("ROLLBACK").await.unwrap();
    assert!(
        client
            .query(
                "SELECT indexname FROM pg_catalog.pg_indexes WHERE indexname = 't_rollback'",
                &[]
            )
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn secondary_unique_index_reports_23505() {
    let (_running, client) = fixture().await;
    client
        .batch_execute("INSERT INTO t (id, email, a, b) VALUES (1, 'same', 1, 2)")
        .await
        .unwrap();
    let error = client
        .batch_execute("INSERT INTO t (id, email, a, b) VALUES (2, 'same', 3, 4)")
        .await
        .unwrap_err();
    assert_eq!(error.code().unwrap().code(), "23505");
}
