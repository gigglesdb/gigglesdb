use super::session_settings_network::Fixture;
use std::time::Duration;
use tokio::time::timeout;
use tokio_postgres::{Client, SimpleQueryMessage, types::Type};

const HASH_LOCK: &str = "SELECT pg_advisory_xact_lock(hashtext($1))::text";
const HASH_TRY_LOCK: &str = "SELECT pg_try_advisory_xact_lock(hashtext($1))";
const LOCK_KEY: &str = "adr:123e4567-e89b-12d3-a456-426614174000";
const DEADLINE: Duration = Duration::from_secs(5);

async fn boolean(client: &Client, sql: &str) -> bool {
    client.query_one(sql, &[]).await.unwrap().get(0)
}

async fn named_transactions(fixture: Fixture) {
    let mut pooled = fixture.client().await;
    let mut waiter = fixture.client().await;
    // tokio-postgres::prepare sends named Parse/Describe, just as a pooled
    // client does before Bind/Execute with a binary text parameter.
    let lock = pooled.prepare(HASH_LOCK).await.unwrap();
    assert_eq!(lock.params(), &[Type::TEXT]);
    assert_eq!(lock.columns()[0].name(), "pg_advisory_xact_lock");
    assert_eq!(lock.columns()[0].type_(), &Type::TEXT);
    let wait_lock = waiter.prepare(HASH_LOCK).await.unwrap();
    let try_lock = waiter.prepare(HASH_TRY_LOCK).await.unwrap();

    let tx = pooled.transaction().await.unwrap();
    assert_eq!(
        tx.query_one(&lock, &[&LOCK_KEY])
            .await
            .unwrap()
            .get::<_, &str>(0),
        ""
    );
    tx.simple_query("CREATE TABLE advisory_example (id int PRIMARY KEY)")
        .await
        .unwrap();
    tx.execute("INSERT INTO advisory_example VALUES (1)", &[])
        .await
        .unwrap();
    assert!(
        !waiter
            .query_one(&try_lock, &[&LOCK_KEY])
            .await
            .unwrap()
            .get::<_, bool>(0)
    );

    let waiting_tx = waiter.transaction().await.unwrap();
    {
        let params: &[&(dyn tokio_postgres::types::ToSql + Sync)] = &[&LOCK_KEY];
        let request = waiting_tx.query_one(&wait_lock, params);
        tokio::pin!(request);
        assert!(
            timeout(Duration::from_millis(150), &mut request)
                .await
                .is_err()
        );
        timeout(DEADLINE, tx.commit()).await.unwrap().unwrap();
        assert_eq!(
            timeout(DEADLINE, request)
                .await
                .unwrap()
                .unwrap()
                .get::<_, &str>(0),
            ""
        );
    }
    waiting_tx.rollback().await.unwrap();

    let tx = pooled.transaction().await.unwrap();
    tx.query_one(&lock, &[&LOCK_KEY]).await.unwrap();
    tx.simple_query("SELECT pg_advisory_lock(702)")
        .await
        .unwrap();
    let error = tx.query_one("SELECT 1 / 0", &[]).await.unwrap_err();
    assert_eq!(error.code().unwrap().code(), "22012");
    tx.rollback().await.unwrap();
    assert!(
        waiter
            .query_one(&try_lock, &[&LOCK_KEY])
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    assert!(
        waiter
            .query_one(&try_lock, &[&LOCK_KEY])
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    assert!(!boolean(&waiter, "SELECT pg_try_advisory_lock(702)").await);

    let count: i64 = pooled
        .query_one(
            "SELECT count(*) FROM information_schema.tables WHERE table_name = 'advisory_example'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 1);
    drop(pooled);
    timeout(DEADLINE, async {
        loop {
            if boolean(&waiter, "SELECT pg_try_advisory_lock(702)").await {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    drop(waiter);
    fixture.stop().await;
}

#[tokio::test]
async fn named_transactions_legacy() {
    named_transactions(Fixture::new().await).await;
}

#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn named_transactions_v2() {
    named_transactions(Fixture::with_engine(giggles_core::Engine::V2).await).await;
}

#[tokio::test]
async fn overloads_modes_strictness_and_result_casts() {
    let fixture = Fixture::new().await;
    let a = fixture.client().await;
    let b = fixture.client().await;
    let c = fixture.client().await;

    let one_key = a
        .prepare("SELECT pg_try_advisory_lock_shared($1)")
        .await
        .unwrap();
    assert_eq!(one_key.params(), &[Type::INT8]);
    let two_keys = a
        .prepare("SELECT pg_try_advisory_xact_lock_shared($1, $2)")
        .await
        .unwrap();
    assert_eq!(two_keys.params(), &[Type::INT4, Type::INT4]);
    let null = a.query_one(&one_key, &[&None::<i64>]).await.unwrap();
    assert_eq!(null.get::<_, Option<bool>>(0), None);
    assert_eq!(
        a.query_one("SELECT pg_advisory_lock(NULL)::text AS absent", &[])
            .await
            .unwrap()
            .get::<_, Option<&str>>("absent"),
        None,
    );
    let session_lock = a
        .simple_query("SELECT pg_catalog.pg_advisory_lock_shared(('12'))::text AS locked")
        .await
        .unwrap();
    assert!(session_lock.iter().any(|message| matches!(message,
        SimpleQueryMessage::Row(row) if row.get("locked") == Some("")
    )));
    a.simple_query("SELECT pg_advisory_lock_shared(11::int2 + 1)")
        .await
        .unwrap();
    assert!(boolean(&b, "SELECT pg_try_advisory_lock_shared(12)").await);
    assert!(!boolean(&c, "SELECT pg_try_advisory_lock(12)").await);
    assert!(boolean(&a, "SELECT pg_advisory_unlock_shared(12)").await);
    assert!(boolean(&b, "SELECT pg_advisory_unlock_shared(12)").await);
    assert!(!boolean(&c, "SELECT pg_try_advisory_lock(12)").await);
    assert!(boolean(&a, "SELECT pg_advisory_unlock_shared(12)").await);
    assert!(boolean(&c, "SELECT pg_try_advisory_lock(12)").await);
    assert!(boolean(&c, "SELECT pg_try_advisory_xact_lock_shared(12)").await);

    a.batch_execute("BEGIN").await.unwrap();
    a.query_one(&two_keys, &[&-8i32, &9i32]).await.unwrap();
    a.simple_query("SELECT pg_advisory_xact_lock_shared(-8, 9)")
        .await
        .unwrap();
    assert!(boolean(&b, "SELECT pg_try_advisory_xact_lock_shared(-8::int2, 9)").await);
    assert!(!boolean(&b, "SELECT pg_try_advisory_lock(-8, 9)").await);
    assert!(!boolean(&a, "SELECT pg_advisory_unlock_shared(-8, 9)").await);
    a.simple_query("SELECT pg_advisory_unlock_all(); COMMIT")
        .await
        .unwrap();
    assert!(boolean(&b, "SELECT pg_try_advisory_lock(-8, 9)").await);
    let text: String = b
        .query_one("SELECT pg_try_advisory_lock(-8, 9)::text success", &[])
        .await
        .unwrap()
        .try_get::<_, String>("success")
        .unwrap();
    assert_eq!(text, "true");
    let name = b
        .query_one("SELECT pg_try_advisory_lock(-8, 9)::pg_catalog.name", &[])
        .await
        .unwrap();
    assert_eq!(name.columns()[0].type_(), &Type::NAME);
    assert_eq!(name.get::<_, &str>(0), "t");
    let empty = b
        .prepare("SELECT pg_advisory_unlock_all()::void")
        .await
        .unwrap();
    assert_eq!(empty.columns()[0].type_(), &Type::VOID);
    b.query_one(&empty, &[]).await.unwrap();
    for invalid in [
        "SELECT pg_advisory_lock(1.5)",
        "SELECT pg_advisory_lock(1, 2::int8)",
        "SELECT pg_advisory_lock(4294967296, 1)",
    ] {
        assert_eq!(
            a.prepare(invalid).await.unwrap_err().code().unwrap().code(),
            "42883",
            "{invalid}"
        );
    }
    for (invalid, message) in [
        (
            "SELECT pg_try_advisory_lock(1)::void",
            "cannot cast type boolean to void",
        ),
        (
            "SELECT pg_advisory_lock(1)::int",
            "cannot cast type void to integer",
        ),
    ] {
        for result in [
            a.prepare(invalid).await.map(|_| ()),
            a.simple_query(invalid).await.map(|_| ()),
        ] {
            let error = result.unwrap_err();
            let error = error.as_db_error().unwrap();
            assert_eq!(error.code().code(), "42846", "{invalid}");
            assert_eq!(error.message(), message, "{invalid}");
        }
    }
    assert!(boolean(&c, "SELECT pg_try_advisory_xact_lock(1)").await);
    let error = a.query_one("SELECT hashtext(1)", &[]).await.unwrap_err();
    let error = error.as_db_error().unwrap();
    assert_eq!(error.code().code(), "42883");
    assert_eq!(error.message(), "function hashtext(integer) does not exist");
    a.batch_execute(
        "PREPARE advisory_sql(text) AS SELECT pg_advisory_xact_lock(hashtext($1))::text",
    )
    .await
    .unwrap();
    assert!(
        a.simple_query("EXECUTE advisory_sql('via SQL PREPARE')")
            .await
            .unwrap()
            .iter()
            .any(
                |message| matches!(message, SimpleQueryMessage::Row(row) if row.get(0) == Some(""))
            )
    );
    drop(a);
    drop(b);
    drop(c);
    fixture.stop().await;
}

#[tokio::test]
async fn shared_holders_participate_in_deadlock_detection() {
    let fixture = Fixture::new().await;
    let a = fixture.client().await;
    let b = fixture.client().await;
    let c = fixture.client().await;
    for client in [&a, &b] {
        client
            .batch_execute("BEGIN; SELECT pg_advisory_xact_lock_shared(981)")
            .await
            .unwrap();
    }
    {
        let upgrade = a.query_one("SELECT pg_advisory_xact_lock(981)", &[]);
        tokio::pin!(upgrade);
        assert!(
            timeout(Duration::from_millis(150), &mut upgrade)
                .await
                .is_err()
        );
        // A queued exclusive request prevents a new shared holder from
        // jumping the queue, even though the current holders are shared.
        assert!(!boolean(&c, "SELECT pg_try_advisory_lock_shared(981)").await);
        let error = timeout(
            DEADLINE,
            b.query_one("SELECT pg_advisory_xact_lock(981)", &[]),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert_eq!(error.code().unwrap().code(), "40P01");
        b.batch_execute("ROLLBACK").await.unwrap();
        timeout(DEADLINE, upgrade).await.unwrap().unwrap();
    }
    a.batch_execute("COMMIT").await.unwrap();
    drop(a);
    drop(b);
    drop(c);
    fixture.stop().await;
}

#[tokio::test]
async fn shared_unlock_warning_uses_postgres_state_and_mode() {
    use std::future::poll_fn;
    use tokio_postgres::{AsyncMessage, Config, NoTls, config::SslMode};

    let fixture = Fixture::new().await;
    let (client, mut connection) = Config::new()
        .hostaddr(fixture.pg.address.ip())
        .port(fixture.pg.address.port())
        .user("owner")
        .dbname("postgres")
        .ssl_mode(SslMode::Disable)
        .connect(NoTls)
        .await
        .unwrap();
    let (send, mut notices) = tokio::sync::mpsc::unbounded_channel();
    let driver = tokio::spawn(async move {
        while let Some(message) = poll_fn(|cx| connection.poll_message(cx)).await {
            if let AsyncMessage::Notice(notice) = message.unwrap() {
                send.send(notice).unwrap();
            }
        }
    });
    assert!(!boolean(&client, "SELECT pg_advisory_unlock_shared(923)").await);
    let notice = timeout(DEADLINE, notices.recv()).await.unwrap().unwrap();
    assert_eq!(notice.severity(), "WARNING");
    assert_eq!(notice.code().code(), "01000");
    assert_eq!(notice.message(), "you don't own a lock of type ShareLock");
    drop(client);
    driver.await.unwrap();
    fixture.stop().await;
}

#[tokio::test]
async fn ordinary_role_can_run_table_free_advisory_expressions() {
    use tokio_postgres::{Config, NoTls, config::SslMode};

    let fixture = Fixture::new().await;
    let admin = fixture.client().await;
    admin
        .batch_execute("CREATE ROLE advisory_reader LOGIN")
        .await
        .unwrap();
    let (client, connection) = Config::new()
        .hostaddr(fixture.pg.address.ip())
        .port(fixture.pg.address.port())
        .user("advisory_reader")
        .dbname("postgres")
        .ssl_mode(SslMode::Disable)
        .connect(NoTls)
        .await
        .unwrap();
    let driver = tokio::spawn(connection);
    let lock = client.prepare(HASH_LOCK).await.unwrap();
    assert_eq!(lock.params(), &[Type::TEXT]);
    assert_eq!(
        client
            .query_one(&lock, &[&LOCK_KEY])
            .await
            .unwrap()
            .get::<_, &str>(0),
        ""
    );
    assert_eq!(
        client
            .query_one("SELECT pg_catalog.hashtext('x')", &[])
            .await
            .unwrap()
            .get::<_, i32>(0),
        1074944137,
    );
    drop(client);
    driver.await.unwrap().unwrap();
    drop(admin);
    fixture.stop().await;
}

#[tokio::test]
async fn same_role_advisory_waiters_leave_admission_for_holder_dml() {
    // Exceed both the default role limit (16) and the blocking pool (32).
    const WAITERS: usize = 33;
    let fixture = Fixture::with_connection_limit(WAITERS + 1).await;
    let holder = fixture.client().await;
    holder
        .batch_execute("CREATE TABLE advisory_admission (id int PRIMARY KEY)")
        .await
        .unwrap();
    holder
        .batch_execute("BEGIN; SELECT pg_advisory_xact_lock(951)")
        .await
        .unwrap();

    let mut clients = Vec::new();
    for _ in 0..WAITERS {
        clients.push(fixture.client().await);
    }
    timeout(DEADLINE, async {
        while fixture.service.pgwire_blocking_live() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let mut waiters = tokio::task::JoinSet::new();
    for client in clients {
        // Each acquisition releases at the end of its single-statement
        // transaction, without consuming a Legacy explicit-transaction slot.
        waiters.spawn(async move {
            client
                .batch_execute("SELECT pg_advisory_xact_lock(951)")
                .await
        });
    }
    // The holder still owns the key. Live workers with all role slots returned
    // establish that the waiters reached the lock wait before the holder's DML.
    timeout(DEADLINE, async {
        loop {
            if let Some(result) = waiters.try_join_next() {
                panic!("waiter finished before holder COMMIT: {result:?}");
            }
            if fixture.service.pgwire_blocking_live() == WAITERS
                && fixture.service.blocking_permits_held() == 0
                && fixture.service.role_pool_counts() == [(16, 16, 0)]
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("advisory waiters did not release admission");

    timeout(DEADLINE, async {
        assert_eq!(
            holder
                .execute("INSERT INTO advisory_admission VALUES (1)", &[])
                .await
                .unwrap(),
            1
        );
        holder.batch_execute("COMMIT").await.unwrap();
        while let Some(waiter) = waiters.join_next().await {
            waiter.unwrap().unwrap();
        }
    })
    .await
    .expect("holder DML, COMMIT and queued advisory acquisitions");
    drop(holder);
    fixture.stop().await;
}

#[tokio::test]
async fn advisory_wait_distinguishes_lock_and_statement_timeout() {
    let fixture = Fixture::new().await;
    let holder = fixture.client().await;
    let waiter = fixture.client().await;
    holder
        .batch_execute("SELECT pg_advisory_lock(953)")
        .await
        .unwrap();

    waiter
        .batch_execute("SET lock_timeout='50ms'")
        .await
        .unwrap();
    for extended in [false, true] {
        let error = timeout(DEADLINE, async {
            if extended {
                waiter
                    .query_one("SELECT pg_advisory_lock(953)", &[])
                    .await
                    .unwrap_err()
            } else {
                waiter
                    .simple_query("SELECT pg_advisory_lock(953)")
                    .await
                    .unwrap_err()
            }
        })
        .await
        .unwrap();
        let error = error.as_db_error().unwrap();
        assert_eq!(error.code().code(), "55P03");
        assert_eq!(error.message(), "canceling statement due to lock timeout");
    }

    waiter
        .batch_execute("SET lock_timeout='2s'; SET statement_timeout='50ms'")
        .await
        .unwrap();
    let error = timeout(
        DEADLINE,
        waiter.query_one("SELECT pg_advisory_lock(953)", &[]),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert_eq!(error.code().unwrap().code(), "57014");

    assert!(boolean(&holder, "SELECT pg_advisory_unlock(953)").await);
    assert!(boolean(&waiter, "SELECT pg_try_advisory_xact_lock(953)").await);
    drop(holder);
    drop(waiter);
    fixture.stop().await;
}
