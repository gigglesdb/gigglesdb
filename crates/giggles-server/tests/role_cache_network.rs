//! Committed authority images must become visible on the next pgwire statement.
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
#[tokio::test]
async fn next_statement_grant_revoke_simple_and_prepared() {
    let f = Fixture::new().await;
    let reader = Fixture::connect(&f.pg, "r").await;
    f.flip(true).await;
    let prepared = reader.prepare("SELECT id FROM t").await.unwrap();
    for granted in [true, false, true, false, true] {
        f.flip(granted).await;
        assert_eq!(
            outcome(reader.simple_query("SELECT id FROM t").await),
            granted
        );
        assert_eq!(outcome(reader.query(&prepared, &[]).await), granted);
    }
    drop(reader);
    f.stop().await;
}

async fn fence(drop_role: bool) {
    let f = Fixture::new().await;
    let reader = Fixture::connect(&f.pg, "r").await;
    reader.simple_query("SELECT 1").await.unwrap();
    f.admin
        .batch_execute(if drop_role {
            "DROP ROLE r"
        } else {
            "ALTER ROLE r NOLOGIN"
        })
        .await
        .unwrap();
    let error = reader.simple_query("SELECT 1").await.unwrap_err();
    // As in write_fence_network, a retained session must be refused. pgwire's
    // idle binding recheck closes the connection; an in-message check emits 28000.
    assert!(
        error.is_closed() || error.code().is_some_and(|c| c.code() == "28000"),
        "{error}"
    );
    drop(reader);
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
