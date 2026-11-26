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

async fn assert_ids(client: &Client, sql: &str, expected: &[i32]) {
    let ids: Vec<i32> = client
        .query(sql, &[])
        .await
        .unwrap()
        .iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(ids, expected, "{sql}");
}

async fn assert_no_orphans(client: &Client, edges: &[(&str, &str, &str)]) {
    for (child, column, parent) in edges {
        assert_ids(
            client,
            &format!(
                "SELECT c.id FROM {child} c LEFT JOIN {parent} p ON c.{column}=p.id \
             WHERE p.id IS NULL AND c.{column} IS NOT NULL"
            ),
            &[],
        )
        .await;
    }
}

async fn execute_without_wait(client: &Client, sql: &str) {
    assert_eq!(
        timeout(NONBLOCKING, client.execute(sql, &[]))
            .await
            .unwrap_or_else(|_| panic!("blocked during cascade: {sql}"))
            .unwrap(),
        1,
        "{sql}"
    );
}

async fn expect_fk_wait(holder: &Client, other: &Client, sql: &str) {
    let statement = other.prepare(sql).await.unwrap();
    let pending = other.execute(&statement, &[]);
    tokio::pin!(pending);
    assert!(timeout(MUST_WAIT, &mut pending).await.is_err(), "{sql}");
    holder.batch_execute("COMMIT").await.unwrap();
    let error = timeout(COMPLETION, pending).await.unwrap().unwrap_err();
    assert_eq!(error.code(), Some(&SqlState::FOREIGN_KEY_VIOLATION));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cascade_delete_allows_disjoint_writes_and_fences_deleted_keys() {
    let (fixture, a, b) = Fixture::new().await;
    a.batch_execute(
        "CREATE TABLE \"Organization\"(id int PRIMARY KEY, owner int REFERENCES usr(id)); \
         CREATE TABLE membership(id int PRIMARY KEY, org_id int REFERENCES \"Organization\"(id) ON DELETE CASCADE); \
         CREATE TABLE invoices(id int PRIMARY KEY, member_id int REFERENCES membership(id) ON DELETE CASCADE); \
         CREATE TABLE requests(id int PRIMARY KEY, org_id int REFERENCES \"Organization\"(id) \
             ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED); \
         INSERT INTO \"Organization\" VALUES(1,1),(2,2); \
         INSERT INTO membership VALUES(1,1),(2,2); \
         INSERT INTO invoices VALUES(1,1); \
         INSERT INTO requests VALUES(1,1)",
    ).await.unwrap();
    a.batch_execute("BEGIN").await.unwrap();
    assert_eq!(
        a.execute("DELETE FROM \"Organization\" WHERE id = $1", &[&1_i32])
            .await
            .unwrap(),
        1
    );
    for sql in [
        "INSERT INTO usr VALUES(10,'u10')",
        "UPDATE usr SET email='b' WHERE id=1",
        "INSERT INTO other VALUES(10,'b')",
        "INSERT INTO \"Organization\" VALUES(3,NULL)",
        "INSERT INTO membership VALUES(3,2)",
        "DELETE FROM membership WHERE id=2",
        "INSERT INTO invoices VALUES(2,3)",
    ] {
        execute_without_wait(&b, sql).await;
    }
    expect_fk_wait(&a, &b, "INSERT INTO membership VALUES(4,1)").await;
    assert_ids(&b, "SELECT id FROM \"Organization\" ORDER BY id", &[2, 3]).await;
    assert_ids(&b, "SELECT id FROM membership ORDER BY id", &[3]).await;
    assert_ids(&b, "SELECT id FROM invoices ORDER BY id", &[2]).await;
    assert_ids(&b, "SELECT id FROM requests", &[]).await;
    assert_no_orphans(
        &b,
        &[
            ("membership", "org_id", "\"Organization\""),
            ("invoices", "member_id", "membership"),
            ("requests", "org_id", "\"Organization\""),
        ],
    )
    .await;
    fixture.stop(a, b).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fk_updates_probe_only_their_changed_references() {
    let (fixture, a, b) = Fixture::new().await;
    a.batch_execute(
        "INSERT INTO usr VALUES(3,'u3'); \
         INSERT INTO member VALUES(1,1,1),(2,2,2); \
         INSERT INTO msg VALUES(3,2,2,'run')",
    )
    .await
    .unwrap();
    a.batch_execute("BEGIN; DELETE FROM org WHERE id=1")
        .await
        .unwrap();
    for sql in [
        "UPDATE msg SET author=3 WHERE id=3",
        "UPDATE member SET user_id=3 WHERE id=2",
    ] {
        b.batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
            .await
            .unwrap();
        execute_without_wait(&b, sql).await;
        timeout(NONBLOCKING, b.batch_execute("COMMIT"))
            .await
            .unwrap()
            .unwrap();
    }
    execute_without_wait(&b, "UPDATE member SET user_id=2 WHERE id=2").await;
    a.batch_execute("COMMIT").await.unwrap();
    assert_ids(&b, "SELECT id FROM org", &[2]).await;
    a.batch_execute("INSERT INTO org VALUES(1,NULL,'o1'); INSERT INTO member VALUES(1,1,1)")
        .await
        .unwrap();

    // org 1 belongs to an untouched row. org 2 belongs to the updated row,
    // but that reference also stays unchanged.
    a.batch_execute("BEGIN; SELECT id FROM org ORDER BY id FOR UPDATE")
        .await
        .unwrap();
    execute_without_wait(&b, "UPDATE member SET user_id=3 WHERE id=2").await;
    execute_without_wait(&b, "UPDATE member SET id=4 WHERE id=2").await;
    let values = (100..300)
        .map(|id| format!("({id},NULL,NULL)"))
        .collect::<Vec<_>>()
        .join(",");
    let upsert = format!("INSERT INTO member VALUES (4,1,3),{values} \
        ON CONFLICT(id) DO UPDATE SET user_id=excluded.user_id");
    assert_eq!(timeout(NONBLOCKING, b.execute(&upsert, &[])).await.unwrap().unwrap(), 201);
    a.batch_execute("COMMIT").await.unwrap();
    for bad_row in ["(500,99,1)", "(4,2,99)"] {
        let upsert = format!("INSERT INTO member VALUES {bad_row},{values} \
            ON CONFLICT(id) DO UPDATE SET user_id=excluded.user_id");
        let error = b.execute(&upsert, &[]).await.unwrap_err();
        assert_eq!(error.code(), Some(&SqlState::FOREIGN_KEY_VIOLATION), "{bad_row}");
    }
    assert_ids(&b, "SELECT id FROM msg WHERE author=3", &[3]).await;
    assert_ids(&b, "SELECT id FROM member WHERE org_id=2 AND user_id=3", &[4]).await;
    assert_ids(&b, "SELECT id FROM member WHERE id=500", &[]).await;
    assert_no_orphans(&b, &[("msg", "org_id", "org"), ("msg", "author", "usr"),
        ("member", "org_id", "org"), ("member", "user_id", "usr")]).await;
    fixture.stop(a, b).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn moved_fk_waits_for_the_new_parent_and_cascade_waits_for_the_child() {
    let (fixture, a, b) = Fixture::new().await;
    a.batch_execute("INSERT INTO member VALUES(1,1,1),(2,2,2)")
        .await
        .unwrap();
    a.batch_execute("BEGIN; DELETE FROM org WHERE id=1")
        .await
        .unwrap();
    expect_fk_wait(&a, &b, "UPDATE member SET org_id=1 WHERE id=2").await;
    assert_ids(&b, "SELECT id FROM member WHERE org_id=2", &[2]).await;

    b.batch_execute("BEGIN; UPDATE member SET org_id=org_id WHERE id=2")
        .await
        .unwrap();
    let delete = a.prepare("DELETE FROM org WHERE id=2").await.unwrap();
    {
        let pending = a.execute(&delete, &[]);
        tokio::pin!(pending);
        assert!(timeout(MUST_WAIT, &mut pending).await.is_err());
        b.batch_execute("COMMIT").await.unwrap();
        assert_eq!(timeout(COMPLETION, pending).await.unwrap().unwrap(), 1);
    }
    assert_ids(&a, "SELECT id FROM member", &[]).await;
    assert_ids(&a, "SELECT id FROM org", &[]).await;
    assert_no_orphans(&a, &[("member", "org_id", "org")]).await;
    fixture.stop(a, b).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fk_postimages_survive_merge_upsert_and_referential_actions() {
    let (fixture, a, b) = Fixture::new().await;
    a.batch_execute(
        "CREATE TABLE parent_key(id int PRIMARY KEY); \
         CREATE TABLE child_key(id int PRIMARY KEY, p int REFERENCES parent_key(id) ON UPDATE CASCADE, \
             org_id int REFERENCES org(id)); \
         INSERT INTO parent_key VALUES(1); INSERT INTO child_key VALUES(1,1,1)",
    )
    .await
    .unwrap();
    for sql in [
        "MERGE INTO child_key c USING (VALUES (1,99)) AS s(id,p) ON c.id=s.id \
         WHEN MATCHED THEN UPDATE SET org_id=s.p",
        "INSERT INTO child_key VALUES(1,1,99) ON CONFLICT(id) DO UPDATE SET org_id=excluded.org_id",
        "WITH written AS (UPDATE child_key SET org_id=99 WHERE id=1 RETURNING id), \
         rekeyed AS (UPDATE parent_key SET id=2 WHERE id=1 RETURNING id) SELECT id FROM written",
        "WITH written AS (INSERT INTO child_key VALUES(2,1,99) RETURNING id), \
         rekeyed AS (UPDATE parent_key SET id=2 WHERE id=1 RETURNING id) SELECT id FROM written",
    ] {
        let error = b.simple_query(sql).await.unwrap_err();
        assert_eq!(error.code(), Some(&SqlState::FOREIGN_KEY_VIOLATION), "{sql}");
        assert_ids(&b, "SELECT id FROM child_key WHERE p=1 AND org_id=1", &[1]).await;
        assert_ids(&b, "SELECT id FROM parent_key", &[1]).await;
    }
    assert_ids(&b, "SELECT id FROM child_key", &[1]).await;
    assert_no_orphans(&b, &[("child_key", "p", "parent_key"), ("child_key", "org_id", "org")]).await;
    fixture.stop(a, b).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn self_reference_to_an_insert_does_not_hide_its_missing_parent() {
    let (fixture, a, b) = Fixture::new().await;
    a.batch_execute("CREATE TABLE sr4(id int PRIMARY KEY, parent int REFERENCES sr4(id)); \
                     INSERT INTO sr4 VALUES (1,NULL)")
        .await
        .unwrap();
    for sql in [
        "INSERT INTO sr4 VALUES (2,3),(3,99)",
        "INSERT INTO sr4 VALUES (2,3),(3,99) ON CONFLICT (id) DO UPDATE SET parent=excluded.parent",
        "INSERT INTO sr4 SELECT g, CASE WHEN g=3 THEN 99 WHEN g=2 THEN 3 END
         FROM generate_series(2,202) g WHERE g<>99
         ON CONFLICT (id) DO UPDATE SET parent=excluded.parent",
    ] {
        let error = b.simple_query(sql).await.unwrap_err();
        let error = error.as_db_error().unwrap();
        assert_eq!(error.code(), &SqlState::FOREIGN_KEY_VIOLATION, "{sql}");
        assert_eq!(error.message(),
            "insert or update on table \"sr4\" violates foreign key constraint \"sr4_parent_fkey\"");
        assert_eq!(error.detail(), Some("Key (parent)=(99) is not present in table \"sr4\"."));
        assert_ids(&b, "SELECT id FROM sr4", &[1]).await;
    }
    fixture.stop(a, b).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multilevel_and_self_cascades_lock_each_deleted_key() {
    let (fixture, a, b) = Fixture::new().await;
    a.batch_execute(
        "INSERT INTO msg VALUES(3,2,2,'run'); \
         CREATE TABLE tree(id int PRIMARY KEY, parent int REFERENCES tree(id) ON DELETE CASCADE); \
         INSERT INTO tree VALUES(1,NULL),(2,1),(3,2),(10,NULL),(11,10)",
    )
    .await
    .unwrap();
    a.batch_execute("BEGIN; DELETE FROM org WHERE id=1")
        .await
        .unwrap();
    execute_without_wait(&b, "INSERT INTO turn VALUES(30,3,'x')").await;
    expect_fk_wait(&a, &b, "INSERT INTO turn VALUES(31,2,'x')").await;
    assert_ids(&b, "SELECT id FROM turn ORDER BY id", &[30]).await;
    assert_no_orphans(&b, &[("turn", "msg_id", "msg"), ("msg", "org_id", "org")]).await;

    a.batch_execute("BEGIN; DELETE FROM tree WHERE id=1")
        .await
        .unwrap();
    execute_without_wait(&b, "INSERT INTO tree VALUES(21,11)").await;
    expect_fk_wait(&a, &b, "INSERT INTO tree VALUES(20,3)").await;
    assert_ids(&b, "SELECT id FROM tree ORDER BY id", &[10, 11, 21]).await;
    assert_no_orphans(&b, &[("tree", "parent", "tree")]).await;
    fixture.stop(a, b).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn set_null_preserves_a_waiting_nonkey_update() {
    let (fixture, a, b) = Fixture::new().await;
    a.batch_execute("BEGIN; DELETE FROM usr WHERE id=1")
        .await
        .unwrap();
    execute_without_wait(&b, "INSERT INTO msg VALUES(12,2,2,'x')").await;
    let update = b
        .prepare("UPDATE msg SET status='b' WHERE id=1")
        .await
        .unwrap();
    {
        let pending = b.execute(&update, &[]);
        tokio::pin!(pending);
        assert!(timeout(MUST_WAIT, &mut pending).await.is_err());
        a.batch_execute("COMMIT").await.unwrap();
        assert_eq!(timeout(COMPLETION, pending).await.unwrap().unwrap(), 1);
    }
    assert_ids(
        &b,
        "SELECT id FROM msg WHERE author IS NULL AND status='b'",
        &[1],
    )
    .await;
    assert_no_orphans(&b, &[("msg", "author", "usr")]).await;
    fixture.stop(a, b).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deferred_child_checks_wait_at_commit() {
    let (fixture, a, b) = Fixture::new().await;
    a.batch_execute(
        "CREATE TABLE deferred_child(id int PRIMARY KEY, p int REFERENCES org(id) \
         ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED)",
    )
    .await
    .unwrap();
    a.batch_execute("BEGIN; DELETE FROM org WHERE id=1")
        .await
        .unwrap();
    b.batch_execute("BEGIN").await.unwrap();
    execute_without_wait(&b, "INSERT INTO deferred_child VALUES(1,1)").await;
    expect_fk_wait(&a, &b, "COMMIT").await;
    assert_ids(&a, "SELECT id FROM deferred_child", &[]).await;

    b.batch_execute("BEGIN").await.unwrap();
    let values = (1..=201)
        .map(|id| format!("({id},2)"))
        .collect::<Vec<_>>()
        .join(",");
    let insert = format!("INSERT INTO deferred_child VALUES {values}");
    assert_eq!(
        timeout(NONBLOCKING, b.execute(&insert, &[]))
            .await
            .unwrap()
            .unwrap(),
        201,
    );
    execute_without_wait(&a, "DELETE FROM org WHERE id=2").await;
    let error = b.batch_execute("COMMIT").await.unwrap_err();
    assert_eq!(error.code(), Some(&SqlState::FOREIGN_KEY_VIOLATION));
    assert_ids(&a, "SELECT id FROM deferred_child", &[]).await;
    assert_no_orphans(&a, &[("deferred_child", "p", "org")]).await;
    fixture.stop(a, b).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fixed_snapshot_rejects_new_or_changed_cascade_children() {
    let (fixture, a, b) = Fixture::new().await;
    for sql in [
        "INSERT INTO member VALUES(1,1,1)",
        "UPDATE member SET user_id=2 WHERE id=1",
    ] {
        a.batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ; SELECT count(*) FROM org")
            .await
            .unwrap();
        execute_without_wait(&b, sql).await;
        let error = a
            .execute("DELETE FROM org WHERE id=1", &[])
            .await
            .unwrap_err();
        assert_eq!(error.code(), Some(&SqlState::T_R_SERIALIZATION_FAILURE));
        a.batch_execute("ROLLBACK").await.unwrap();
        assert_ids(&a, "SELECT id FROM org ORDER BY id", &[1, 2]).await;
        assert_ids(&a, "SELECT id FROM member", &[1]).await;
    }
    assert_no_orphans(&a, &[("member", "org_id", "org")]).await;
    fixture.stop(a, b).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn disjoint_cascades_then_opposing_writes_detect_deadlock() {
    let (fixture, a, b) = Fixture::new().await;
    for (client, id) in [(&a, 1_i32), (&b, 2_i32)] {
        client.batch_execute("BEGIN").await.unwrap();
        assert_eq!(
            timeout(
                NONBLOCKING,
                client.execute("DELETE FROM org WHERE id=$1", &[&id])
            )
            .await
            .unwrap()
            .unwrap(),
            1
        );
    }
    async fn update_then_end(client: &Client, id: i32) -> Result<u64, Error> {
        let result = client
            .execute("UPDATE org SET name='b' WHERE id=$1", &[&id])
            .await;
        client
            .batch_execute(if result.is_ok() { "COMMIT" } else { "ROLLBACK" })
            .await
            .unwrap();
        result
    }
    let (a_result, b_result) = timeout(COMPLETION, async {
        tokio::join!(update_then_end(&a, 2), update_then_end(&b, 1))
    })
    .await
    .unwrap();
    let surviving = match (a_result, b_result) {
        (Ok(1), Err(error)) => {
            assert_eq!(error.code(), Some(&SqlState::T_R_DEADLOCK_DETECTED));
            2
        }
        (Err(error), Ok(1)) => {
            assert_eq!(error.code(), Some(&SqlState::T_R_DEADLOCK_DETECTED));
            1
        }
        outcomes => panic!("expected exactly one deadlock victim, got {outcomes:?}"),
    };
    assert_ids(&a, "SELECT id FROM org", &[surviving]).await;
    assert_no_orphans(&a, &[("msg", "org_id", "org"), ("turn", "msg_id", "msg")]).await;
    fixture.stop(a, b).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cascade_rechecks_children_moved_away_from_and_into_the_deleted_key() {
    for (initial, moved, expected) in [(1_i32, 2_i32, &[1][..]), (2, 1, &[][..])] {
        let (fixture, a, b) = Fixture::new().await;
        a.execute("INSERT INTO member VALUES(1,$1,1)", &[&initial])
            .await
            .unwrap();
        a.batch_execute("BEGIN").await.unwrap();
        a.execute("UPDATE member SET org_id=$1 WHERE id=1", &[&moved])
            .await
            .unwrap();
        let delete = b.prepare("DELETE FROM org WHERE id=$1").await.unwrap();
        {
            let params: [&(dyn tokio_postgres::types::ToSql + Sync); 1] = [&1_i32];
            let pending = b.execute(&delete, &params);
            tokio::pin!(pending);
            assert!(timeout(MUST_WAIT, &mut pending).await.is_err());
            a.batch_execute("COMMIT").await.unwrap();
            assert_eq!(timeout(COMPLETION, pending).await.unwrap().unwrap(), 1);
        }
        assert_ids(&b, "SELECT id FROM member ORDER BY id", expected).await;
        assert_ids(&b, "SELECT id FROM member WHERE org_id=2", expected).await;
        assert_no_orphans(&b, &[("member", "org_id", "org")]).await;
        fixture.stop(a, b).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cascade_follows_a_child_rekey_that_keeps_the_reference() {
    for full_image in [false, true] {
        let (fixture, a, b) = Fixture::new().await;
        if full_image {
            a.batch_execute(
                "CREATE TABLE restricts_org(id int PRIMARY KEY, p int REFERENCES org(id))",
            )
            .await
            .unwrap();
        }
        a.batch_execute(
            "INSERT INTO member VALUES(300,1,1); BEGIN; UPDATE member SET id=0 WHERE id=300",
        )
        .await
        .unwrap();
        let delete = b.prepare("DELETE FROM org WHERE id=1").await.unwrap();
        {
            let pending = b.execute(&delete, &[]);
            tokio::pin!(pending);
            assert!(timeout(MUST_WAIT, &mut pending).await.is_err());
            a.batch_execute("COMMIT").await.unwrap();
            assert_eq!(timeout(COMPLETION, pending).await.unwrap().unwrap(), 1);
        }
        assert_ids(&b, "SELECT id FROM member", &[]).await;
        assert_no_orphans(&b, &[("member", "org_id", "org")]).await;
        fixture.stop(a, b).await;
    }
}
