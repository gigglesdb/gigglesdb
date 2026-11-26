#![cfg(feature = "engine-rocksdb")]

use giggles_core::{Admin, Engine, Limits, roles::NewRole};
use giggles_server::{
    Running, Service,
    auth::{PostgresqlConfig, TlsMode, serve_postgresql},
    clock::Clock,
};
use std::{collections::BTreeMap, path::PathBuf, sync::Arc, time::Duration};
use tokio::time::timeout;
use tokio_postgres::{Client, Config, Error, NoTls, error::SqlState};

const SCHEMA: &str = "
    CREATE TABLE usr(id int PRIMARY KEY, email text UNIQUE);
    CREATE TABLE org(id int PRIMARY KEY, owner int REFERENCES usr(id) ON UPDATE CASCADE, name text);
    CREATE TABLE member(id int PRIMARY KEY,
        org_id int REFERENCES org(id) ON DELETE CASCADE ON UPDATE CASCADE,
        user_id int REFERENCES usr(id) ON UPDATE CASCADE);
    CREATE TABLE msg(id int PRIMARY KEY,
        org_id int REFERENCES org(id) ON DELETE CASCADE ON UPDATE CASCADE,
        author int REFERENCES usr(id) ON DELETE SET NULL ON UPDATE CASCADE, status text);
    CREATE TABLE turn(id int PRIMARY KEY,
        msg_id int UNIQUE REFERENCES msg(id) ON DELETE CASCADE ON UPDATE CASCADE, status text);
    CREATE TABLE credits(id int PRIMARY KEY,
        org_id int UNIQUE REFERENCES org(id) ON DELETE CASCADE ON UPDATE CASCADE, n int);
    CREATE TABLE other(id int PRIMARY KEY, v text);
    INSERT INTO usr VALUES (1,'u1'),(2,'u2');
    INSERT INTO org VALUES (1,NULL,'o1'),(2,NULL,'o2');
    INSERT INTO msg VALUES (1,1,1,'run'),(2,1,NULL,'run');
    INSERT INTO turn VALUES (1,1,'run'),(2,2,'run');
";
const NONBLOCKING: Duration = Duration::from_millis(750);
const MUST_WAIT: Duration = Duration::from_millis(150);
const COMPLETION: Duration = Duration::from_secs(10);

struct Fixture {
    server: Running,
    service: Arc<Service>,
    path: PathBuf,
}

impl Fixture {
    async fn new() -> (Self, Client, Client) {
        let mut nonce = [0; 8];
        getrandom::fill(&mut nonce).unwrap();
        let path =
            std::env::temp_dir().join(format!("fk-component-{:016x}", u64::from_be_bytes(nonce)));
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
        let server = serve_postgresql(
            config.listen,
            service.clone(),
            Arc::new(config.build().unwrap()),
        )
        .await
        .unwrap();
        let mut config = Config::new();
        config
            .hostaddr(server.address.ip())
            .port(server.address.port())
            .user("owner")
            .dbname("postgres");
        let (a, a_connection) = config.connect(NoTls).await.unwrap();
        let (b, b_connection) = config.connect(NoTls).await.unwrap();
        tokio::spawn(a_connection);
        tokio::spawn(b_connection);
        a.batch_execute(SCHEMA).await.unwrap();
        (
            Self {
                server,
                service,
                path,
            },
            a,
            b,
        )
    }

    async fn stop(self, a: Client, b: Client) {
        drop((a, b));
        self.server.shutdown().await.unwrap();
        drop(self.service);
        std::fs::remove_dir_all(self.path).unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nonkey_updates_allow_writes_across_the_fk_component() {
    let (fixture, a, b) = Fixture::new().await;
    a.execute("BEGIN", &[]).await.unwrap();
    assert_eq!(
        a.query_one("SELECT id FROM turn WHERE id = $1 FOR UPDATE", &[&1_i32])
            .await
            .unwrap()
            .get::<_, i32>(0),
        1,
    );
    for (sql, status) in [
        ("UPDATE turn SET status = $1 WHERE id = $2", "x"),
        ("UPDATE msg SET status = $1 WHERE id = $2", "done"),
    ] {
        assert_eq!(a.execute(sql, &[&status, &1_i32]).await.unwrap(), 1);
    }
    for sql in [
        "INSERT INTO usr VALUES (10,'b10')",
        "INSERT INTO credits VALUES (10,1,5)",
        "INSERT INTO member VALUES (10,1,1)",
        "INSERT INTO msg VALUES (10,2,2,'new')",
    ] {
        assert_eq!(
            timeout(NONBLOCKING, b.execute(sql, &[]))
                .await
                .unwrap_or_else(|_| panic!("blocked during non-key updates: {sql}"))
                .unwrap(),
            1,
        );
    }
    a.execute("COMMIT", &[]).await.unwrap();
    assert_eq!(
        b.query_one("SELECT status FROM msg WHERE id = 1", &[])
            .await
            .unwrap()
            .get::<_, String>(0),
        "done",
    );
    fixture.stop(a, b).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn conditional_nonkey_update_and_member_inserts_do_not_deadlock() {
    let (fixture, a, b) = Fixture::new().await;
    let sql = "UPDATE org SET owner = $1 WHERE id = $2 AND owner IS NULL";
    a.execute("BEGIN", &[]).await.unwrap();
    assert_eq!(a.execute(sql, &[&1_i32, &1_i32]).await.unwrap(), 1);
    b.execute("BEGIN", &[]).await.unwrap();
    let update = b.prepare(sql).await.unwrap();
    {
        let values: [&(dyn tokio_postgres::types::ToSql + Sync); 2] = [&2_i32, &1_i32];
        let waiting = b.execute(&update, &values);
        tokio::pin!(waiting);
        assert!(timeout(MUST_WAIT, &mut waiting).await.is_err());
        timeout(COMPLETION, async {
            assert_eq!(
                a.execute("INSERT INTO member VALUES (1,1,1)", &[])
                    .await
                    .unwrap(),
                1,
            );
            a.execute("COMMIT", &[]).await.unwrap();
            assert_eq!(waiting.await.unwrap(), 0);
        })
        .await
        .expect("the member insert and waiting update must complete");
    }
    assert_eq!(
        b.execute("INSERT INTO member VALUES (2,1,2)", &[])
            .await
            .unwrap(),
        1,
    );
    b.execute("COMMIT", &[]).await.unwrap();
    assert_eq!(
        b.query_one("SELECT owner FROM org WHERE id = 1", &[])
            .await
            .unwrap()
            .get::<_, i32>(0),
        1,
    );
    let members = b
        .query("SELECT id,org_id,user_id FROM member ORDER BY id", &[])
        .await
        .unwrap()
        .iter()
        .map(|row| {
            (
                row.get::<_, i32>(0),
                row.get::<_, i32>(1),
                row.get::<_, i32>(2),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(members, [(1, 1, 1), (2, 1, 2)]);
    fixture.stop(a, b).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn referenced_key_update_blocks_child_insert_until_commit() {
    let (fixture, a, b) = Fixture::new().await;
    a.execute("BEGIN", &[]).await.unwrap();
    assert_eq!(
        a.execute("UPDATE org SET id = $1 WHERE id = $2", &[&5_i32, &1_i32])
            .await
            .unwrap(),
        1,
    );
    let insert = b
        .prepare("INSERT INTO member VALUES (20,1,1)")
        .await
        .unwrap();
    {
        let waiting = b.execute(&insert, &[]);
        tokio::pin!(waiting);
        assert!(timeout(MUST_WAIT, &mut waiting).await.is_err());
        a.execute("COMMIT", &[]).await.unwrap();
        let error = timeout(COMPLETION, waiting).await.unwrap().unwrap_err();
        let error = error.as_db_error().unwrap();
        assert_eq!(error.code(), &SqlState::FOREIGN_KEY_VIOLATION);
        assert_eq!(
            error.message(),
            "insert or update on table \"member\" violates foreign key constraint \"member_org_id_fkey\"",
        );
        assert_eq!(
            error.detail(),
            Some("Key (org_id)=(1) is not present in table \"org\".")
        );
    }
    assert_eq!(
        timeout(
            NONBLOCKING,
            b.execute("INSERT INTO other VALUES (20,'x')", &[])
        )
        .await
        .unwrap()
        .unwrap(),
        1,
    );
    fixture.stop(a, b).await;
}

async fn finish_email_update(client: &Client, id: i32, email: &str) -> Result<u64, Error> {
    let result = client
        .execute("UPDATE usr SET email = $1 WHERE id = $2", &[&email, &id])
        .await;
    client
        .execute(if result.is_ok() { "COMMIT" } else { "ROLLBACK" }, &[])
        .await
        .unwrap();
    result
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn opposing_email_updates_detect_a_row_deadlock() {
    let (fixture, a, b) = Fixture::new().await;
    // READ COMMITTED uses an exclusive writer sentinel for UNIQUE-changing
    // updates. Use fixed snapshots to reach the two-row deadlock on this schema.
    for (client, id, email) in [(&a, 1_i32, "a1"), (&b, 2_i32, "b2")] {
        client
            .execute("BEGIN ISOLATION LEVEL REPEATABLE READ", &[])
            .await
            .unwrap();
        assert_eq!(
            timeout(
                COMPLETION,
                client.execute("UPDATE usr SET email = $1 WHERE id = $2", &[&email, &id])
            )
            .await
            .unwrap()
            .unwrap(),
            1,
        );
    }
    let (first, second) = timeout(COMPLETION, async {
        tokio::join!(
            finish_email_update(&a, 2, "a2"),
            finish_email_update(&b, 1, "b1")
        )
    })
    .await
    .expect("the deadlock detector must resolve the cycle");
    let expected = match (first, second) {
        (Ok(1), Err(error)) => {
            assert_eq!(error.code(), Some(&SqlState::T_R_DEADLOCK_DETECTED));
            ["a1", "a2"]
        }
        (Err(error), Ok(1)) => {
            assert_eq!(error.code(), Some(&SqlState::T_R_DEADLOCK_DETECTED));
            ["b1", "b2"]
        }
        results => panic!("expected one 40P01 and one successful update, got {results:?}"),
    };
    let emails = b
        .query("SELECT email FROM usr ORDER BY id", &[])
        .await
        .unwrap()
        .iter()
        .map(|row| row.get::<_, String>(0))
        .collect::<Vec<_>>();
    assert_eq!(emails, expected);
    fixture.stop(a, b).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn conflict_update_of_nonkey_column_allows_component_insert() {
    let (fixture, a, b) = Fixture::new().await;
    a.execute("BEGIN", &[]).await.unwrap();
    assert_eq!(
        a.execute(
            "INSERT INTO msg VALUES (1,1,1,$1) ON CONFLICT (id) DO UPDATE SET status = excluded.status",
            &[&"done"],
        )
        .await
        .unwrap(),
        1,
    );
    assert_eq!(
        timeout(
            NONBLOCKING,
            b.execute("INSERT INTO usr VALUES (10,'b10')", &[])
        )
        .await
        .unwrap()
        .unwrap(),
        1,
    );
    a.execute("COMMIT", &[]).await.unwrap();
    assert_eq!(
        b.query_one("SELECT status FROM msg WHERE id = 1", &[])
            .await
            .unwrap()
            .get::<_, String>(0),
        "done",
    );
    fixture.stop(a, b).await;
}
