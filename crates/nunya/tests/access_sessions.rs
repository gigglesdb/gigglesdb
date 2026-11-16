//! Opt-in PostgreSQL access-session continuity and refusal tests.
use super::common::{ServeHarness, ServeResult, file_sink_policy};
use nunya::{
    mgmt::client::{
        AcquireParams, ClientError, Credentials, MgmtClient, SessionCredential, SessionSecret,
        SessionView,
    },
    state::model::Mode,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

struct Driver(tokio::task::JoinHandle<()>);
impl Drop for Driver {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn connect(
    h: &ServeHarness,
    user: &str,
    secret: &SessionSecret,
) -> Result<(Client, Driver), tokio_postgres::Error> {
    let (client, connection) = tokio_postgres::Config::new()
        .host("127.0.0.1")
        .port(h.pg_port())
        .dbname("app")
        .user(user)
        .password(secret.secret.as_bytes())
        .connect(NoTls)
        .await?;
    Ok((
        client,
        Driver(tokio::spawn(async move {
            let _ = connection.await;
        })),
    ))
}

async fn session_connect(
    h: &ServeHarness,
    s: &SessionView,
    secret: &SessionSecret,
) -> ServeResult<(Client, Driver)> {
    Ok(connect(h, &format!("session:{}", s.id), secret).await?)
}

fn owner(h: &ServeHarness, name: &str, secret: &SessionSecret) -> ServeResult<MgmtClient> {
    let config: toml::Value =
        toml::from_str(&std::fs::read_to_string(h.dir().join("nunya.toml"))?)?;
    let addr = config["management"]["listen"]
        .as_str()
        .ok_or("missing management address")?;
    Ok(MgmtClient::new(
        format!("http://{addr}").parse()?,
        None,
        Credentials::Bearer {
            name: name.into(),
            secret: secret.secret.clone(),
        },
    )?)
}

async fn acquire(
    owner: &MgmtClient,
    isolated: bool,
    lifetime: Option<&str>,
) -> ServeResult<(SessionView, SessionSecret)> {
    let secret = SessionSecret::generate();
    let session = owner
        .acquire(AcquireParams {
            route: "postgres".into(),
            mode: Some(Mode::ReadWrite),
            lifetime: lifetime.map(str::to_owned),
            isolated,
            label: None,
            verifier: secret.verifier.clone(),
            secret_sha256: secret.sha256_hex.clone(),
        })
        .await?;
    Ok((session, secret))
}

async fn values(client: &Client, sql: &str) -> ServeResult<Vec<Vec<String>>> {
    let mut rows = Vec::new();
    for message in client.simple_query(sql).await? {
        if let SimpleQueryMessage::Row(row) = message {
            rows.push(
                (0..row.len())
                    .map(|i| row.get(i).map(str::to_owned).ok_or("unexpected NULL"))
                    .collect::<Result<Vec<_>, _>>()?,
            );
        }
    }
    Ok(rows)
}

async fn token(client: &Client) -> ServeResult<String> {
    let rows = values(client, "SELECT email FROM customers WHERE id = 1").await?;
    Ok(rows
        .first()
        .and_then(|r| r.first())
        .ok_or("missing protected row")?
        .clone())
}

fn ended<T>(result: Result<T, tokio_postgres::Error>) {
    let error = match result {
        Err(e) => e,
        Ok(_) => panic!("ended session accepted database operation"),
    };
    if let Some(db) = error.as_db_error() {
        assert!(
            db.code().code() == "NY011",
            "unexpected session refusal SQLSTATE: {}",
            db.code().code()
        );
    } else {
        assert!(
            error.is_closed() || error.to_string().contains("connection"),
            "expected closed connection"
        );
    }
}

async fn tokenize(client: &Client, label: &str) -> ServeResult<()> {
    let rows = values(
        client,
        &format!("SELECT nunya_tokenize('email', '{label}')"),
    )
    .await?;
    assert!(
        rows.first()
            .and_then(|r| r.first())
            .is_some_and(|s| s.starts_with("tok_")),
        "missing explicit token"
    );
    Ok(())
}

async fn limited(client: &Client) {
    let result = client
        .simple_query("SELECT nunya_tokenize('email', 'quota-refused')")
        .await;
    let error = match result {
        Err(e) => e,
        Ok(_) => panic!("tokenize quota was reset or exceeded"),
    };
    assert!(
        error.code().is_some_and(|code| code.code() == "NY007"),
        "expected NY007 quota refusal"
    );
}

// Drop runs cleanup on a separate runtime so it also works during assertion unwinding.
struct TableGuard {
    url: String,
    name: String,
}
impl Drop for TableGuard {
    fn drop(&mut self) {
        let url = self.url.clone();
        let name = self.name.clone();
        let cleanup = std::thread::spawn(move || -> ServeResult<()> {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?
                .block_on(async {
                    let (client, connection) = tokio_postgres::connect(&url, NoTls).await?;
                    let _driver = Driver(tokio::spawn(async move {
                        let _ = connection.await;
                    }));
                    client
                        .batch_execute(&format!("DROP TABLE IF EXISTS public.{name}"))
                        .await?;
                    Ok(())
                })
        })
        .join();
        // Never panic in Drop: during unwinding that would abort the whole test binary.
        if !matches!(cleanup, Ok(Ok(()))) {
            eprintln!("upstream test table cleanup failed");
        }
    }
}

#[tokio::test]
#[ignore = "requires NUNYA_TEST_PG and NUNYA_TEST_PG_ADMIN pointing at harness database"]
async fn pg_reconnect_and_pool_every_token_type() -> ServeResult<()> {
    let url = std::env::var("NUNYA_TEST_PG")?;
    let admin_url = std::env::var("NUNYA_TEST_PG_ADMIN")?;
    let name = format!(
        "access_types_{}",
        nunya::token::SessionId::random().to_hex()
    );
    let _table = TableGuard {
        url: admin_url.clone(),
        name: name.clone(),
    };
    let (admin, connection) = tokio_postgres::connect(&admin_url, NoTls).await?;
    let _driver = Driver(tokio::spawn(async move {
        let _ = connection.await;
    }));
    // Int2 is refused by policy validation: "too few values to encrypt safely".
    // Bool is refused by policy validation: "mask it instead". Neither has an FPE codec.
    // Numeric/bytea/json/jsonb/array/enum/other are explicitly unsupported by StandardCodec.
    let types = [
        ("txt", "text", "'alpha'"),
        ("vc", "varchar(254)", "'bravo'"),
        ("bp", "char(100)", "'charlie'"),
        ("i4", "int4", "42"),
        ("i8", "int8", "42000000000"),
        ("dt", "date", "'2024-05-06'"),
        ("ts", "timestamp", "'2024-05-06 07:08:09'"),
        ("tz", "timestamptz", "'2024-05-06 07:08:09+00'"),
        ("uid", "uuid", "'12345678-1234-5678-9abc-123456789abc'"),
    ];
    let definitions = types
        .iter()
        .map(|(col, ty, _)| format!("{col} {ty}"))
        .collect::<Vec<_>>()
        .join(", ");
    let originals = types
        .iter()
        .map(|(_, _, value)| *value)
        .collect::<Vec<_>>()
        .join(", ");
    admin.batch_execute(&format!("CREATE TABLE public.{name} (id int PRIMARY KEY, {definitions}); INSERT INTO public.{name} VALUES (1, {originals}); GRANT SELECT, INSERT, DELETE ON public.{name} TO nunya")).await?;
    let columns = types
        .iter()
        .map(|(col, _, _)| *col)
        .collect::<Vec<_>>()
        .join(", ");
    let mut policy =
        "snapshot_max_age_ms = 60000\n[readlog]\nsink = 'file'\npath = 'read.log'\n".to_owned();
    for (col, _, _) in types {
        policy.push_str(&format!("[domains.{col}]\ntokenize = 'off'\n"));
    }
    for (col, _, _) in types {
        policy.push_str(&format!(
            "[[column]]\ntable = 'public.{name}'\nname = '{col}'\nblind = '{col}'\n"
        ));
    }
    let mut h = ServeHarness::new(&policy, Some(&url), None, None, "")?;
    h.start().await?;
    let (session, secret, _) = h
        .acquire_session("types-agent", "postgres", Mode::ReadWrite)
        .await?;
    let (one, driver_one) = session_connect(&h, &session, &secret).await?;
    let select = format!("SELECT {columns} FROM {name} WHERE id = 1");
    let first = values(&one, &select).await?;
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].len(), types.len());
    let direct = values(&admin, &select).await?;
    for (got, original) in first[0].iter().zip(&direct[0]) {
        assert!(got != original, "blind column returned its original value");
    }
    let (two, driver_two) = session_connect(&h, &session, &secret).await?;
    assert!(
        values(&two, &select).await? == first,
        "pooled connection changed tokens"
    );
    {
        let (client, inserted_id) = (&two, 2);
        for ((col, _, _), token) in types.iter().zip(&first[0]) {
            let rows = values(
                client,
                &format!(
                    "SELECT id FROM {name} WHERE {col} = '{}'",
                    token.replace('\'', "''")
                ),
            )
            .await?;
            assert!(
                rows == vec![vec!["1".to_owned()]],
                "pooled token predicate failed for {col}"
            );
        }
        let literals = first[0]
            .iter()
            .map(|t| format!("'{}'", t.replace('\'', "''")))
            .collect::<Vec<_>>()
            .join(", ");
        client
            .simple_query(&format!(
                "INSERT INTO {name} (id, {columns}) VALUES ({inserted_id}, {literals})"
            ))
            .await?;
    }
    drop(one);
    drop(driver_one);
    drop(two);
    drop(driver_two);
    let (three, _driver_three) = session_connect(&h, &session, &secret).await?;
    for ((col, _, _), token) in types.iter().zip(&first[0]) {
        let rows = values(
            &three,
            &format!(
                "SELECT id FROM {name} WHERE {col} = '{}' ORDER BY id",
                token.replace('\'', "''")
            ),
        )
        .await?;
        assert!(
            rows == vec![vec!["1".to_owned()], vec!["2".to_owned()]],
            "reconnected token predicate failed for {col}"
        );
    }
    assert!(
        values(
            &admin,
            &format!("SELECT {columns} FROM {name} WHERE id = 2")
        )
        .await?
            == direct,
        "upstream insert did not restore original values"
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires NUNYA_TEST_PG pointing at harness database"]
async fn pg_session_cannot_attach_to_other_scope() -> ServeResult<()> {
    let url = std::env::var("NUNYA_TEST_PG")?;
    let mut h = ServeHarness::new(&file_sink_policy(), Some(&url), None, None, "")?;
    h.start().await?;
    let (a, a_secret, principal) = h
        .acquire_session("scope-a", "postgres", Mode::ReadWrite)
        .await?;
    let (b, b_secret, _) = h
        .acquire_session("scope-b", "postgres", Mode::ReadWrite)
        .await?;
    let owner = owner(&h, "scope-a", &principal)?;
    let (isolated, isolated_secret) = acquire(&owner, true, None).await?;
    assert_ne!(a.scope, b.scope);
    assert_ne!(a.scope, isolated.scope);
    for target in [&b, &isolated] {
        assert!(
            connect(&h, &format!("session:{}", target.id), &a_secret)
                .await
                .is_err(),
            "foreign session credential accepted"
        );
    }
    assert!(
        connect(&h, "scope-a", &principal).await.is_err(),
        "principal authenticated on database port"
    );
    let (ca, _da) = session_connect(&h, &a, &a_secret).await?;
    let (cb, _db) = session_connect(&h, &b, &b_secret).await?;
    let (ci, _di) = session_connect(&h, &isolated, &isolated_secret).await?;
    let ta = token(&ca).await?;
    assert!(
        ta != token(&cb).await?,
        "different principal reused token scope"
    );
    assert!(
        ta != token(&ci).await?,
        "isolated session reused token scope"
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires NUNYA_TEST_PG pointing at harness database"]
async fn pg_revoke_ends_open_connection_and_prepared_statement() -> ServeResult<()> {
    let url = std::env::var("NUNYA_TEST_PG")?;
    let mut h = ServeHarness::new(&file_sink_policy(), Some(&url), None, None, "")?;
    h.start().await?;
    let (session, secret, principal) = h
        .acquire_session("revoked-agent", "postgres", Mode::ReadWrite)
        .await?;
    let owner = owner(&h, "revoked-agent", &principal)?;
    let (client, _driver) = session_connect(&h, &session, &secret).await?;
    let prepared = client
        .prepare("SELECT email FROM customers WHERE id = 1")
        .await?;
    let (idle, _idle_driver) = session_connect(&h, &session, &secret).await?;
    assert_eq!(client.query(&prepared, &[]).await?.len(), 1);
    assert_eq!(
        h.admin()
            .connections()
            .await?
            .iter()
            .filter(|c| c.session == session.id)
            .count(),
        2
    );
    h.admin().revoke(&session.id).await?;
    ended(
        idle.simple_query("SELECT email FROM customers WHERE id = 1")
            .await,
    );
    ended(client.query(&prepared, &[]).await);
    assert!(
        connect(&h, &format!("session:{}", session.id), &secret)
            .await
            .is_err(),
        "revoked login accepted"
    );
    assert!(
        matches!(owner.renew(&session.id, None, None).await, Err(ClientError::Api { code, .. }) if code == "session_ended"),
        "revoked renewal accepted or wrong refusal"
    );
    assert!(
        h.admin()
            .connections()
            .await?
            .iter()
            .all(|c| c.session != session.id),
        "revoked connection still listed"
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires NUNYA_TEST_PG pointing at harness database"]
async fn pg_expiry_and_renewal() -> ServeResult<()> {
    let url = std::env::var("NUNYA_TEST_PG")?;
    // Config minimum is 60s; a 5s acquire lifetime exercises expiry without a long sleep.
    let mut h = ServeHarness::new(
        &file_sink_policy(),
        Some(&url),
        None,
        None,
        "\n[session]\nmax_lifetime = '60s'\nidle_timeout = 'off'\n",
    )?;
    h.start().await?;
    let (_, _, principal) = h
        .acquire_session("expiring-agent", "postgres", Mode::ReadWrite)
        .await?;
    let owner = owner(&h, "expiring-agent", &principal)?;
    let (session, secret) = acquire(&owner, false, Some("5s")).await?;
    let (client, _driver) = session_connect(&h, &session, &secret).await?;
    let before = token(&client).await?;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let renewed = owner.renew(&session.id, None, None).await?;
    assert_eq!(renewed.scope, session.scope);
    assert_eq!(
        renewed.expires_ms, session.expires_ms,
        "renewal extended absolute expiry"
    );
    assert!(
        renewed.expires_ms <= session.created_ms + 60_000,
        "renewal exceeded max_lifetime"
    );
    assert!(
        token(&client).await? == before,
        "renewal changed token or closed connection"
    );
    let now = u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
    let wait = session.expires_ms.saturating_sub(now) + 150;
    assert!(wait < 10_000, "expiry test sleep exceeds bound");
    tokio::time::sleep(Duration::from_millis(wait)).await;
    ended(
        client
            .simple_query("SELECT email FROM customers WHERE id = 1")
            .await,
    );
    assert!(
        connect(&h, &format!("session:{}", session.id), &secret)
            .await
            .is_err(),
        "expired login accepted"
    );
    assert!(
        matches!(owner.renew(&session.id, None, None).await, Err(ClientError::Api { code, .. }) if code == "session_ended"),
        "expired renewal accepted or wrong refusal"
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires NUNYA_TEST_PG pointing at harness database"]
async fn pg_quota_continuity_reconnect_rotation_reacquire() -> ServeResult<()> {
    let url = std::env::var("NUNYA_TEST_PG")?;
    let policy = file_sink_policy()
        .replace(
            "tokenize_limit_per_session = 20",
            "tokenize_limit_per_session = 3",
        )
        .replace(
            "tokenize_limit_per_principal = 200",
            "tokenize_limit_per_principal = 5",
        );
    let mut h = ServeHarness::new(&policy, Some(&url), None, None, "")?;
    h.start().await?;
    let (session, secret, principal) = h
        .acquire_session("quota-agent", "postgres", Mode::ReadWrite)
        .await?;
    let owner = owner(&h, "quota-agent", &principal)?;
    let (one, driver_one) = session_connect(&h, &session, &secret).await?;
    tokenize(&one, "quota-one").await?;
    drop(one);
    drop(driver_one);
    let (two, _driver_two) = session_connect(&h, &session, &secret).await?;
    tokenize(&two, "quota-two").await?;
    let salt = data_encoding::BASE64.decode(secret.scram_salt.as_bytes())?;
    let rotated = SessionSecret::generate_with(&salt, secret.scram_iterations)?;
    owner
        .renew(
            &session.id,
            None,
            Some(SessionCredential {
                verifier: rotated.verifier.clone(),
                secret_sha256: rotated.sha256_hex.clone(),
            }),
        )
        .await?;
    let (old, _old_driver) = session_connect(&h, &session, &secret).await?;
    let (new, _new_driver) = session_connect(&h, &session, &rotated).await?;
    tokenize(&new, "quota-three").await?;
    limited(&old).await;
    limited(&new).await;
    let (next, next_secret) = acquire(&owner, false, None).await?;
    assert_ne!(next.id, session.id);
    // Acquisition creates a fresh scope; principal quota must survive that boundary.
    let (next_client, _next_driver) = session_connect(&h, &next, &next_secret).await?;
    tokenize(&next_client, "quota-four").await?;
    let (isolated, isolated_secret) = acquire(&owner, true, None).await?;
    assert_ne!(isolated.scope, session.scope);
    let (isolated_client, _isolated_driver) =
        session_connect(&h, &isolated, &isolated_secret).await?;
    tokenize(&isolated_client, "quota-five").await?;
    limited(&next_client).await;
    limited(&isolated_client).await;
    let (last, last_secret) = acquire(&owner, false, None).await?;
    let (last_client, _last_driver) = session_connect(&h, &last, &last_secret).await?;
    limited(&last_client).await;
    Ok(())
}
