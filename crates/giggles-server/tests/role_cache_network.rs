//! Committed authority images must become visible on the next pgwire statement.
#![cfg(feature = "engine-rocksdb")]
mod api_support;
use api_support::Fixture as ApiFixture;
use giggles_core::{Engine, roles::NewRole};
use giggles_server::{
    Running,
    auth::{PostgresqlConfig, serve_postgresql},
};
use serde_json::json;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio_postgres::{Client, NoTls};

struct Fixture {
    api: ApiFixture,
    pg: Running,
    admin: Client,
}
impl Fixture {
    async fn new() -> Self {
        let api = ApiFixture::catalog_empty_engine(Engine::V2).await;
        assert_eq!(api.admin.engine(), Engine::V2);
        for (name, superuser) in [("root", true), ("r", false)] {
            api.admin
                .roles()
                .create(NewRole {
                    name: name.into(),
                    login: true,
                    superuser,
                    password: Some(giggles_server::auth::scram::Verifier::generate("secret")),
                    ..Default::default()
                })
                .unwrap();
        }
        let config: PostgresqlConfig = serde_json::from_value(json!({
            "listen":"127.0.0.1:0", "tls":"disabled", "loopback_trust":true
        }))
        .unwrap();
        let pg = serve_postgresql(
            "127.0.0.1:0".parse().unwrap(),
            api.service.clone(),
            Arc::new(config.build().unwrap()),
        )
        .await
        .unwrap();
        let admin = Self::connect(&pg, "root").await;
        admin
            .batch_execute("CREATE TABLE t (id BIGINT PRIMARY KEY); INSERT INTO t VALUES (1)")
            .await
            .unwrap();
        Self { api, pg, admin }
    }
    async fn connect(pg: &Running, role: &str) -> Client {
        let (client, connection) = tokio_postgres::Config::new()
            .host("127.0.0.1")
            .port(pg.address.port())
            .user(role)
            .password("secret")
            .dbname("postgres")
            .connect(NoTls)
            .await
            .unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
    }
    async fn flip(&self, grant: bool) {
        self.admin
            .batch_execute(if grant {
                "GRANT SELECT ON t TO r"
            } else {
                "REVOKE SELECT ON t FROM r"
            })
            .await
            .unwrap();
    }
    async fn stop(self) {
        drop(self.admin);
        self.pg.shutdown().await.unwrap();
        self.api.stop().await;
    }
}
fn outcome<T>(result: Result<T, tokio_postgres::Error>) -> bool {
    match result {
        Ok(_) => true,
        Err(error) => {
            assert_eq!(error.code().map(|c| c.code()), Some("42501"), "{error}");
            false
        }
    }
}
// query_typed sends Parse and Bind with empty statement names.
async fn unnamed(client: &Client) -> Result<Vec<tokio_postgres::Row>, tokio_postgres::Error> {
    client.query_typed("SELECT id FROM t", &[]).await
}

#[tokio::test]
async fn next_statement_grant_revoke_simple_and_prepared() {
    let f = Fixture::new().await;
    let reader = Fixture::connect(&f.pg, "r").await;
    assert!(!outcome(unnamed(&reader).await));
    assert!(!outcome(unnamed(&reader).await));
    f.flip(true).await;
    let named_reader = Fixture::connect(&f.pg, "r").await;
    let simple_reader = Fixture::connect(&f.pg, "r").await;
    let named = named_reader.prepare("SELECT id FROM t").await.unwrap();
    for granted in [true, false, true, false, true] {
        f.flip(granted).await;
        for _ in 0..2 {
            assert_eq!(outcome(named_reader.query(&named, &[]).await), granted);
            assert_eq!(outcome(unnamed(&reader).await), granted);
        }
        assert_eq!(
            outcome(simple_reader.simple_query("SELECT id FROM t").await),
            granted
        );
    }
    drop(simple_reader);
    drop(named_reader);
    drop(reader);
    f.stop().await;
}

#[tokio::test]
async fn grants_and_revokes_take_effect_inside_explicit_transactions() {
    let f = Fixture::new().await;
    for sql in ["CREATE ROLE readers", "GRANT SELECT ON t TO readers"] {
        f.admin.batch_execute(sql).await.unwrap();
    }

    for isolation in ["READ COMMITTED", "REPEATABLE READ"] {
        for (grant, revoke) in [
            ("GRANT SELECT ON t TO r", "REVOKE SELECT ON t FROM r"),
            ("GRANT readers TO r", "REVOKE readers FROM r"),
        ] {
            for named in [true, false] {
                let reader = Fixture::connect(&f.pg, "r").await;
                reader.batch_execute(&format!("BEGIN ISOLATION LEVEL {isolation}"))
                    .await.unwrap();
                // Pin the transaction snapshot and warm authorization before
                // the other session grants access to the target table.
                let warm_sql = "SELECT 1::bigint";
                let warm_plan = if named {
                    Some(reader.prepare(warm_sql).await.unwrap())
                } else {
                    None
                };
                for _ in 0..2 {
                    if let Some(plan) = &warm_plan {
                        reader.query(plan, &[]).await.unwrap();
                    } else {
                        reader.query_typed(warm_sql, &[]).await.unwrap();
                    }
                }

                f.admin.batch_execute(grant).await.unwrap();
                let target = if named {
                    Some(reader.prepare("SELECT id FROM t").await.unwrap())
                } else {
                    None
                };
                for _ in 0..2 {
                    let rows = if let Some(plan) = &target {
                        reader.query(plan, &[]).await.unwrap()
                    } else {
                        unnamed(&reader).await.unwrap()
                    };
                    assert_eq!(rows[0].get::<_, i64>(0), 1);
                }

                f.admin.batch_execute(revoke).await.unwrap();
                let result = if let Some(plan) = &target {
                    reader.query(plan, &[]).await
                } else {
                    unnamed(&reader).await
                };
                assert!(!outcome(result), "{isolation}, {revoke}, named={named}");
                reader.batch_execute("ROLLBACK").await.unwrap();
            }
        }
    }
    f.stop().await;
}

async fn fence(drop_role: bool) {
    let f = Fixture::new().await;
    let sql = "SELECT 1::bigint";
    let named = Fixture::connect(&f.pg, "r").await;
    let plan = named.prepare(sql).await.unwrap();
    let unnamed = Fixture::connect(&f.pg, "r").await;
    for _ in 0..2 {
        named.query(&plan, &[]).await.unwrap();
        unnamed.query_typed(sql, &[]).await.unwrap();
    }
    f.admin
        .batch_execute(if drop_role {
            "DROP ROLE r"
        } else {
            "ALTER ROLE r NOLOGIN"
        })
        .await
        .unwrap();
    for error in [
        named.query(&plan, &[]).await.unwrap_err(),
        unnamed.query_typed(sql, &[]).await.unwrap_err(),
    ] {
        // Idle binding revalidation closes the connection. An in-flight
        // binding recheck instead returns the authentication SQLSTATE.
        assert!(
            error.is_closed() || error.code().is_some_and(|c| c.code() == "28000"),
            "{error}"
        );
    }
    drop(named);
    drop(unnamed);
    f.stop().await;
}
#[tokio::test]
async fn next_statement_nologin() {
    fence(false).await;
}
#[tokio::test]
async fn next_statement_drop_role() {
    fence(true).await;
}

#[tokio::test]
async fn next_statement_membership_and_owner_on_both_prepared_paths() {
    let f = Fixture::new().await;
    for sql in ["CREATE ROLE readers", "GRANT readers TO r"] {
        f.admin.batch_execute(sql).await.unwrap();
    }
    let roles = f.api.admin.roles();
    roles.set_table_owner("t", "readers").unwrap();
    let reader = Fixture::connect(&f.pg, "r").await;
    let named = Fixture::connect(&f.pg, "r").await;
    let plan = named.prepare("SELECT id FROM t").await.unwrap();
    for _ in 0..2 {
        assert!(outcome(named.query(&plan, &[]).await));
        assert!(outcome(unnamed(&reader).await));
    }

    f.admin
        .batch_execute("REVOKE readers FROM r")
        .await
        .unwrap();
    assert!(!outcome(named.query(&plan, &[]).await));
    assert!(!outcome(unnamed(&reader).await));
    f.admin.batch_execute("GRANT readers TO r").await.unwrap();
    for _ in 0..2 {
        assert!(outcome(named.query(&plan, &[]).await));
        assert!(outcome(unnamed(&reader).await));
    }

    // The native catalog owner API publishes the same authority records as DDL.
    roles.set_table_owner("t", "root").unwrap();
    assert!(!outcome(named.query(&plan, &[]).await));
    assert!(!outcome(unnamed(&reader).await));
    drop(named);
    drop(reader);
    f.stop().await;
}

#[tokio::test]
async fn prepared_authority_tracks_role_session_authorization_and_search_path() {
    let f = Fixture::new().await;
    let root = Fixture::connect(&f.pg, "root").await;
    let prepared = root.prepare("SELECT id FROM t").await.unwrap();
    for (setting, reset) in [
        ("SET ROLE r", "RESET ROLE"),
        ("SET SESSION AUTHORIZATION r", "RESET SESSION AUTHORIZATION"),
    ] {
        assert!(outcome(root.query(&prepared, &[]).await));
        assert!(outcome(root.query(&prepared, &[]).await));
        root.batch_execute(setting).await.unwrap();
        assert!(!outcome(root.query(&prepared, &[]).await));
        assert!(!outcome(unnamed(&root).await));
        root.batch_execute(reset).await.unwrap();
        assert!(outcome(root.query(&prepared, &[]).await));
        assert!(outcome(unnamed(&root).await));
    }

    for sql in [
        "CREATE SCHEMA alternate",
        "CREATE TABLE alternate.t (id BIGINT PRIMARY KEY)",
        "INSERT INTO alternate.t VALUES (2)",
        "GRANT USAGE ON SCHEMA alternate TO r",
        "GRANT SELECT ON public.t TO r",
    ] {
        f.admin.batch_execute(sql).await.unwrap();
    }
    let reader = Fixture::connect(&f.pg, "r").await;
    let prepared = reader.prepare("SELECT id FROM t").await.unwrap();
    for _ in 0..2 {
        assert_eq!(
            reader
                .query_one(&prepared, &[])
                .await
                .unwrap()
                .get::<_, i64>(0),
            1
        );
    }
    reader
        .batch_execute("SET search_path = alternate, public")
        .await
        .unwrap();
    assert!(!outcome(reader.query(&prepared, &[]).await));
    assert!(!outcome(unnamed(&reader).await));
    f.admin
        .batch_execute("GRANT SELECT ON alternate.t TO r")
        .await
        .unwrap();
    assert_eq!(
        reader
            .query_one(&prepared, &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        2
    );
    f.admin
        .batch_execute("REVOKE USAGE ON SCHEMA alternate FROM r")
        .await
        .unwrap();
    assert!(!outcome(reader.query(&prepared, &[]).await));
    reader.batch_execute("RESET search_path").await.unwrap();
    assert_eq!(
        reader
            .query_one(&prepared, &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        1
    );
    drop(root);
    drop(reader);
    f.stop().await;
}

#[tokio::test]
async fn prepared_serial_defaults_recheck_sequence_authority() {
    let f = Fixture::new().await;
    for sql in [
        "CREATE ROLE writers",
        "CREATE TABLE serial_t (id BIGSERIAL PRIMARY KEY)",
        "GRANT INSERT ON serial_t TO r",
    ] {
        f.admin.batch_execute(sql).await.unwrap();
    }
    let reader = Fixture::connect(&f.pg, "r").await;
    let sql = "INSERT INTO serial_t DEFAULT VALUES";
    assert!(!outcome(reader.prepare(sql).await));
    f.api
        .admin
        .roles()
        .set_table_owner("serial_t", "writers")
        .unwrap();
    f.admin.batch_execute("GRANT writers TO r").await.unwrap();
    let prepared = reader.prepare(sql).await.unwrap();
    for _ in 0..2 {
        assert_eq!(reader.execute(&prepared, &[]).await.unwrap(), 1);
    }
    f.admin
        .batch_execute("REVOKE writers FROM r")
        .await
        .unwrap();
    assert!(!outcome(reader.execute(&prepared, &[]).await));
    assert!(!outcome(reader.execute(&prepared, &[]).await));
    f.admin.batch_execute("GRANT writers TO r").await.unwrap();
    assert_eq!(reader.execute(&prepared, &[]).await.unwrap(), 1);
    drop(reader);
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_grant_revoke_visibility() {
    let rounds: usize = std::env::var("GIGGLES_ROLE_CACHE_ROUNDS")
        .ok()
        .map(|s| s.parse().expect("positive round count"))
        .unwrap_or(50);
    assert!(rounds > 0);
    let f = Fixture::new().await;
    let mut clients = Vec::new();
    for _ in 0..16 {
        clients.push(Fixture::connect(&f.pg, "r").await);
    }
    let done = Arc::new(AtomicBool::new(false));
    let epoch = Arc::new(AtomicU64::new(0));
    let completed = Arc::new(std::array::from_fn::<_, 16, _>(|_| AtomicU64::new(0)));
    let barrier = Arc::new(tokio::sync::Barrier::new(17));
    let mut readers = Vec::new();
    for (i, client) in clients.into_iter().enumerate() {
        let done = done.clone();
        let barrier = barrier.clone();
        let epoch = epoch.clone();
        let completed = completed.clone();
        readers.push(tokio::spawn(async move {
            let extended = i % 2 != 0;
            let mut observations = Vec::new();
            barrier.wait().await;
            while !done.load(Ordering::Acquire) {
                // An acquired epoch is published only after that flip completes.
                let observed_epoch = epoch.load(Ordering::Acquire);
                let start = Instant::now();
                let ok = if extended {
                    outcome(client.query("SELECT id FROM t", &[]).await)
                } else {
                    outcome(client.simple_query("SELECT id FROM t").await)
                };
                observations.push((start, Instant::now(), ok));
                completed[i].store(observed_epoch, Ordering::Release);
                // One observation per epoch avoids exhausting the fixture's
                // admission budget while the coordinator waits for peers.
                while epoch.load(Ordering::Acquire) == observed_epoch
                    && !done.load(Ordering::Acquire)
                {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }
            observations
        }));
    }
    let mut flips = Vec::new();
    barrier.wait().await;
    for k in 0..rounds {
        let grant = k % 2 == 0;
        let start = Instant::now();
        f.flip(grant).await;
        flips.push((start, Instant::now(), grant));
        let current_epoch = u64::try_from(k).unwrap() + 1;
        epoch.store(current_epoch, Ordering::Release);
        // Keep this state stable until every reader finishes a SELECT that
        // started after the flip ended, regardless of debug-build latency.
        let wait = tokio::time::timeout(Duration::from_secs(10), async {
            while completed
                .iter()
                .any(|reader| reader.load(Ordering::Acquire) < current_epoch)
            {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await;
        assert!(
            wait.is_ok(),
            "timed out waiting for post-flip SELECTs after flip {k} (grant={grant}); reader epochs: {:?}",
            completed
                .iter()
                .map(|reader| reader.load(Ordering::Acquire))
                .collect::<Vec<_>>()
        );
    }
    done.store(true, Ordering::Release);
    let mut checked = [0, 0];
    for reader in readers {
        let observations = reader.await.unwrap();
        assert!(!observations.is_empty());
        for (start, end, ok) in observations {
            if let Some((k, flip)) = flips.iter().enumerate().rev().find(|(_, f)| f.1 <= start)
                && flips.get(k + 1).is_none_or(|next| end < next.0)
            {
                assert_eq!(ok, flip.2, "stable interval after flip {k}");
                checked[usize::from(ok)] += 1;
            }
        }
    }
    assert!(
        checked[1] > 0 && (rounds == 1 || checked[0] > 0),
        "no stable grant/revoke observations: {checked:?}"
    );
    f.stop().await;
}

#[tokio::test]
async fn same_session_revoke_and_transactional_revoke_rejection() {
    let f = Fixture::new().await;
    f.flip(true).await;
    f.admin.batch_execute("SET ROLE r").await.unwrap();
    assert!(outcome(f.admin.simple_query("SELECT id FROM t").await));
    f.admin.batch_execute("RESET ROLE").await.unwrap();
    f.admin.batch_execute("BEGIN").await.unwrap();
    // Role DDL is currently forbidden in explicit/implicit transactions. Private
    // catalog overlays are covered by the core image_txn bypass tests instead.
    let error = f
        .admin
        .batch_execute("REVOKE SELECT ON t FROM r")
        .await
        .unwrap_err();
    assert_eq!(error.code().map(|c| c.code()), Some("25001"));
    f.admin.batch_execute("ROLLBACK").await.unwrap();
    f.admin.batch_execute("SET ROLE r").await.unwrap();
    assert!(outcome(f.admin.simple_query("SELECT id FROM t").await));
    f.admin.batch_execute("RESET ROLE").await.unwrap();
    f.flip(false).await;
    f.admin.batch_execute("SET ROLE r").await.unwrap();
    assert!(!outcome(f.admin.simple_query("SELECT id FROM t").await));
    f.stop().await;
}

#[tokio::test]
async fn other_session_ignores_rejected_private_revoke_then_sees_committed_revoke() {
    let f = Fixture::new().await;
    let reader = Fixture::connect(&f.pg, "r").await;
    f.flip(true).await;
    assert!(outcome(reader.simple_query("SELECT id FROM t").await));
    f.admin.batch_execute("BEGIN").await.unwrap();
    let error = f
        .admin
        .batch_execute("REVOKE SELECT ON t FROM r")
        .await
        .unwrap_err();
    assert_eq!(error.code().map(|c| c.code()), Some("25001"));
    assert!(outcome(reader.simple_query("SELECT id FROM t").await));
    f.admin.batch_execute("ROLLBACK").await.unwrap();
    f.flip(false).await;
    assert!(!outcome(reader.simple_query("SELECT id FROM t").await));
    drop(reader);
    f.stop().await;
}

#[tokio::test]
async fn next_statement_membership_revoke_drop_granted_role_and_nosuperuser() {
    let f = Fixture::new().await;
    let reader = Fixture::connect(&f.pg, "r").await;
    for sql in [
        "CREATE ROLE readers",
        "GRANT SELECT ON t TO readers",
        "GRANT readers TO r",
    ] {
        f.admin.batch_execute(sql).await.unwrap();
    }
    assert!(outcome(reader.simple_query("SELECT id FROM t").await));
    f.admin
        .batch_execute("REVOKE readers FROM r")
        .await
        .unwrap();
    assert!(!outcome(reader.simple_query("SELECT id FROM t").await));
    f.admin.batch_execute("GRANT readers TO r").await.unwrap();
    assert!(outcome(reader.simple_query("SELECT id FROM t").await));
    f.admin
        .batch_execute("REVOKE SELECT ON t FROM readers")
        .await
        .unwrap();
    // Warm the post-REVOKE version so the DROP below needs its own invalidation.
    assert!(!outcome(reader.simple_query("SELECT id FROM t").await));
    f.admin.batch_execute("DROP ROLE readers").await.unwrap();
    assert!(!outcome(reader.simple_query("SELECT id FROM t").await));
    drop(reader);
    f.admin
        .batch_execute("ALTER ROLE r SUPERUSER")
        .await
        .unwrap();
    let reader = Fixture::connect(&f.pg, "r").await;
    assert!(outcome(reader.simple_query("SELECT id FROM t").await));
    f.admin
        .batch_execute("ALTER ROLE r NOSUPERUSER")
        .await
        .unwrap();
    // Attribute changes can fence the retained login binding before SQL runs,
    // just as the existing DROP ROLE / NOLOGIN tests above document.
    let error = reader.simple_query("SELECT id FROM t").await.unwrap_err();
    assert!(
        error.is_closed()
            || error
                .code()
                .is_some_and(|c| matches!(c.code(), "28000" | "42501")),
        "{error}"
    );
    drop(reader);
    let reader = Fixture::connect(&f.pg, "r").await;
    assert!(!outcome(reader.simple_query("SELECT id FROM t").await));
    drop(reader);
    f.stop().await;
}
