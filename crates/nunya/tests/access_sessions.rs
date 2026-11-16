//! Opt-in PostgreSQL and native Spanner access-session continuity and refusal tests.
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

use google_cloud_googleapis::spanner::v1::{
    CreateSessionRequest, DeleteSessionRequest, ExecuteSqlRequest, spanner_client::SpannerClient,
};
use tonic::{Code, Request, Status, transport::Endpoint};

#[derive(Clone)]
struct SpannerBearer(tonic::metadata::MetadataValue<tonic::metadata::Ascii>);
impl tonic::service::Interceptor for SpannerBearer {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        request
            .metadata_mut()
            .insert("authorization", self.0.clone());
        Ok(request)
    }
}
type NativeClient = SpannerClient<
    tonic::service::interceptor::InterceptedService<tonic::transport::Channel, SpannerBearer>,
>;

async fn spanner_client(
    h: &ServeHarness,
    user: &str,
    secret: &SessionSecret,
) -> ServeResult<NativeClient> {
    let channel = Endpoint::from_shared(format!("http://{}", h.spanner_addr()))?
        .connect()
        .await?;
    let bearer = SpannerBearer(format!("Bearer nunya.{user}.{}", &*secret.secret).parse()?);
    Ok(SpannerClient::with_interceptor(channel, bearer))
}

fn spanner_sql(session: &str, sql: &str) -> ExecuteSqlRequest {
    ExecuteSqlRequest {
        session: session.into(),
        sql: sql.into(),
        ..Default::default()
    }
}

fn spanner_denied<T>(result: Result<T, Status>, ny: &str) {
    let error = match result {
        Err(error) => error,
        Ok(_) => panic!("foreign Spanner credential or session accepted"),
    };
    assert_eq!(error.code(), Code::PermissionDenied);
    assert!(
        error.message().contains(ny),
        "unexpected Spanner refusal code"
    );
}

// Direct emulator deletion also works when gateway authentication has been revoked.
struct SpannerSessions {
    endpoint: String,
    names: Vec<String>,
}
impl Drop for SpannerSessions {
    fn drop(&mut self) {
        let endpoint = self.endpoint.clone();
        let names = self.names.clone();
        let cleanup = std::thread::spawn(move || -> ServeResult<()> {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?
                .block_on(async {
                    let channel = Endpoint::from_shared(endpoint)?
                        .connect_timeout(Duration::from_secs(3))
                        .timeout(Duration::from_secs(3))
                        .connect()
                        .await?;
                    let mut client = SpannerClient::new(channel);
                    for name in names {
                        let _ = client.delete_session(DeleteSessionRequest { name }).await;
                    }
                    Ok(())
                })
        })
        .join();
        if !matches!(cleanup, Ok(Ok(()))) {
            eprintln!("upstream Spanner session cleanup failed");
        }
    }
}

#[tokio::test]
#[ignore = "requires NUNYA_TEST_SPANNER_EMULATOR, NUNYA_TEST_SPANNER_DB and NUNYA_TEST_PG"]
async fn spanner_scope_isolation() -> ServeResult<()> {
    let url = std::env::var("NUNYA_TEST_PG")?;
    let database = std::env::var("NUNYA_TEST_SPANNER_DB")?;
    let endpoint = format!("http://{}", std::env::var("NUNYA_TEST_SPANNER_EMULATOR")?);
    let fixture =
        NativeFixture::new("email STRING(MAX)", "email", "'alice@example.test'", true).await?;
    let policy = fixture.policy(&["email"], 20, 200);
    let mut h = ServeHarness::new(
        &policy,
        Some(&url),
        None,
        Some(super::common::NativeUpstream {
            database: &database,
            emulator_endpoint: &endpoint,
        }),
        "",
    )?;
    let config_path = h.dir().join("nunya.toml");
    let config = std::fs::read_to_string(&config_path)?;
    std::fs::write(
        config_path,
        format!("schemas = ['public', '{}']\n{config}", fixture.schema),
    )?;
    h.start().await?;
    let (pg, pg_secret, principal) = h
        .acquire_session("native-scope", "postgres", Mode::ReadWrite)
        .await?;
    h.admin()
        .set_grants(
            &pg.principal,
            &[
                ("postgres", Mode::ReadWrite),
                ("spanner-native", Mode::ReadWrite),
            ],
        )
        .await?;
    let owner = owner(&h, "native-scope", &principal)?;
    let native_acquire = || async {
        let secret = SessionSecret::generate();
        let session = owner
            .acquire(AcquireParams {
                route: "spanner-native".into(),
                mode: Some(Mode::ReadWrite),
                lifetime: None,
                isolated: false,
                label: None,
                verifier: secret.verifier.clone(),
                secret_sha256: secret.sha256_hex.clone(),
            })
            .await?;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>((session, secret))
    };
    let (a, a_secret) = native_acquire().await?;
    let (b, b_secret) = native_acquire().await?;
    let mut cleanup = SpannerSessions {
        endpoint,
        names: vec![],
    };
    let mut ca = spanner_client(&h, &format!("session:{}", a.id), &a_secret).await?;
    let name = ca
        .create_session(CreateSessionRequest {
            database: database.clone(),
            ..Default::default()
        })
        .await?
        .into_inner()
        .name;
    cleanup.names.push(name.clone());
    let rows = ca
        .execute_sql(spanner_sql(
            &name,
            &format!("SELECT email FROM {}.t WHERE id = 1", fixture.schema),
        ))
        .await?
        .into_inner();
    let native_token = match rows
        .rows
        .first()
        .and_then(|r| r.values.first())
        .and_then(|v| v.kind.as_ref())
    {
        Some(prost_types::value::Kind::StringValue(value)) => value,
        _ => return Err("missing native email token".into()),
    };
    let (pg_client, _pg_driver) = session_connect(&h, &pg, &pg_secret).await?;
    assert!(
        values(
            &pg_client,
            &format!("SELECT email FROM {}.t WHERE id = 1", fixture.schema)
        )
        .await?[0][0]
            != *native_token,
        "routes reused token scope"
    );
    let mut mixed = spanner_client(&h, &format!("session:{}", b.id), &a_secret).await?;
    spanner_denied(
        mixed
            .create_session(CreateSessionRequest {
                database: database.clone(),
                ..Default::default()
            })
            .await,
        "NY003",
    );
    let mut principal_client = spanner_client(&h, "native-scope", &principal).await?;
    spanner_denied(
        principal_client
            .create_session(CreateSessionRequest {
                database: database.clone(),
                ..Default::default()
            })
            .await,
        "NY003",
    );
    let mut pg_route = spanner_client(&h, &format!("session:{}", pg.id), &pg_secret).await?;
    spanner_denied(
        pg_route
            .create_session(CreateSessionRequest {
                database: database.clone(),
                ..Default::default()
            })
            .await,
        "NY003",
    );
    let mut cb = spanner_client(&h, &format!("session:{}", b.id), &b_secret).await?;
    spanner_denied(
        cb.execute_sql(spanner_sql(
            &name,
            &format!("SELECT email FROM {}.t WHERE id = 1", fixture.schema),
        ))
        .await,
        "NY003",
    );
    Ok(())
}

// Emulator DDL cannot run concurrently; hold the lane through fixture cleanup.
static NATIVE_FIXTURE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

// Each fixture owns only a unique named schema, including on assertion unwinding.
struct NativeFixture {
    _lock: tokio::sync::MutexGuard<'static, ()>,
    schema: String,
    rest: String,
    database: String,
    endpoint: String,
    pg_admin: Option<String>,
}
async fn native_rest(
    base: &str,
    method: &str,
    path: &str,
    body: serde_json::Value,
) -> ServeResult<serde_json::Value> {
    use http_body_util::{BodyExt, Full};
    let uri: http::Uri = format!("{base}/v1/{path}").parse()?;
    let stream = tokio::net::TcpStream::connect((
        uri.host().ok_or("missing REST host")?,
        uri.port_u16().ok_or("missing REST port")?,
    ))
    .await?;
    let (mut sender, connection) =
        hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(stream)).await?;
    let _driver = Driver(tokio::spawn(async move {
        let _ = connection.await;
    }));
    let request = http::Request::builder()
        .method(method)
        .uri(&uri)
        .header(
            "host",
            uri.authority().ok_or("missing REST authority")?.as_str(),
        )
        .header("content-type", "application/json")
        .body(Full::new(bytes::Bytes::from(serde_json::to_vec(&body)?)))?;
    let response = sender.send_request(request).await?;
    let status = response.status();
    let success = status.is_success();
    let data = response.into_body().collect().await?.to_bytes();
    if !success {
        return Err(format!("emulator REST request refused: HTTP {}", status.as_u16()).into());
    }
    if data.is_empty() {
        return Ok(serde_json::json!({}));
    }
    Ok(serde_json::from_slice(&data)?)
}
async fn native_ddl(rest: &str, database: &str, statements: Vec<String>) -> ServeResult<()> {
    let mut operation = native_rest(
        rest,
        "PATCH",
        &format!("{database}/ddl"),
        serde_json::json!({"statements": statements}),
    )
    .await?;
    for _ in 0..120 {
        if operation.get("error").is_some() {
            return Err("emulator DDL operation failed".into());
        }
        if operation["done"] == true {
            return Ok(());
        }
        let name = operation["name"]
            .as_str()
            .ok_or("missing DDL operation name")?
            .to_owned();
        tokio::time::sleep(Duration::from_millis(100)).await;
        operation = native_rest(rest, "GET", &name, serde_json::json!({})).await?;
    }
    Err("emulator DDL operation timed out".into())
}
impl NativeFixture {
    async fn new(
        definitions: &str,
        columns: &str,
        literals: &str,
        postgres: bool,
    ) -> ServeResult<Self> {
        let fixture = Self {
            _lock: NATIVE_FIXTURE_LOCK.lock().await,
            schema: format!("b8_{}", &nunya::token::SessionId::random().to_hex()[..12]),
            rest: std::env::var("NUNYA_TEST_SPANNER_REST")?,
            database: std::env::var("NUNYA_TEST_SPANNER_DB")?,
            endpoint: format!("http://{}", std::env::var("NUNYA_TEST_SPANNER_EMULATOR")?),
            pg_admin: if postgres {
                Some(std::env::var("NUNYA_TEST_PG_ADMIN")?)
            } else {
                None
            },
        };
        let s = &fixture.schema;
        native_ddl(
            &fixture.rest,
            &fixture.database,
            vec![
                format!("CREATE SCHEMA {s}"),
                format!("CREATE TABLE {s}.t (id INT64 NOT NULL, {definitions}) PRIMARY KEY(id)"),
            ],
        )
        .await?;
        if let Some(url) = &fixture.pg_admin {
            let (client, connection) = tokio_postgres::connect(url, NoTls).await?;
            let _driver = Driver(tokio::spawn(async move {
                let _ = connection.await;
            }));
            client.batch_execute(&format!("CREATE SCHEMA {s}; CREATE TABLE {s}.t (id int PRIMARY KEY, email text); INSERT INTO {s}.t VALUES (1, 'alice@example.test'); GRANT USAGE ON SCHEMA {s} TO nunya; GRANT SELECT, INSERT, DELETE ON {s}.t TO nunya")).await?;
        }
        fixture
            .insert(&format!(
                "INSERT INTO {s}.t (id, {columns}) VALUES (1, {literals})"
            ))
            .await?;
        Ok(fixture)
    }
    async fn insert(&self, sql: &str) -> ServeResult<()> {
        let session = native_rest(
            &self.rest,
            "POST",
            &format!("{}/sessions", self.database),
            serde_json::json!({}),
        )
        .await?;
        let name = session["name"].as_str().ok_or("missing setup session")?;
        let result = async {
            let tx = native_rest(
                &self.rest,
                "POST",
                &format!("{name}:beginTransaction"),
                serde_json::json!({"options":{"readWrite":{}}}),
            )
            .await?;
            let id = tx["id"].as_str().ok_or("missing setup transaction")?;
            native_rest(
                &self.rest,
                "POST",
                &format!("{name}:executeSql"),
                serde_json::json!({"sql":sql,"transaction":{"id":id},"seqno":"1"}),
            )
            .await?;
            native_rest(
                &self.rest,
                "POST",
                &format!("{name}:commit"),
                serde_json::json!({"transactionId":id}),
            )
            .await?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        }
        .await;
        let _ = native_rest(&self.rest, "DELETE", name, serde_json::json!({})).await;
        result
    }
    fn policy(&self, columns: &[&str], session: u64, principal: u64) -> String {
        let mut policy =
            "snapshot_max_age_ms = 60000\n[readlog]\nsink = 'file'\npath = 'read.log'\n".to_owned();
        for col in columns {
            policy.push_str(&format!("[domains.{col}]\ntokenize = 'on'\ntokenize_limit_per_session = {session}\ntokenize_limit_per_principal = {principal}\ntokenize_principal_window = '24h'\n"));
        }
        for col in columns {
            policy.push_str(&format!(
                "[[column]]\ntable = '{}.t'\nname = '{col}'\nblind = '{col}'\n",
                self.schema
            ));
        }
        policy
    }
    fn harness(&self, policy: &str, pg: Option<&str>, extra: &str) -> ServeResult<ServeHarness> {
        ServeHarness::new(
            policy,
            pg,
            None,
            Some(super::common::NativeUpstream {
                database: &self.database,
                emulator_endpoint: &self.endpoint,
            }),
            extra,
        )
    }
    fn sessions(&self) -> SpannerSessions {
        SpannerSessions {
            endpoint: self.endpoint.clone(),
            names: vec![],
        }
    }
}
impl Drop for NativeFixture {
    fn drop(&mut self) {
        let (schema, rest, database, pg) = (
            self.schema.clone(),
            self.rest.clone(),
            self.database.clone(),
            self.pg_admin.clone(),
        );
        let result = std::thread::spawn(move || -> ServeResult<()> {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?
                .block_on(async {
                    let spanner = native_ddl(
                        &rest,
                        &database,
                        vec![
                            format!("DROP TABLE {schema}.t"),
                            format!("DROP SCHEMA {schema}"),
                        ],
                    )
                    .await;
                    if let Some(url) = pg {
                        let (client, connection) = tokio_postgres::connect(&url, NoTls).await?;
                        let _driver = Driver(tokio::spawn(async move {
                            let _ = connection.await;
                        }));
                        client
                            .batch_execute(&format!(
                                "DROP TABLE IF EXISTS {schema}.t; DROP SCHEMA IF EXISTS {schema}"
                            ))
                            .await?;
                    }
                    spanner
                })
        })
        .join();
        if !matches!(result, Ok(Ok(()))) {
            eprintln!("native fixture schema cleanup failed");
        }
    }
}
async fn native_open(
    h: &ServeHarness,
    access: &SessionView,
    secret: &SessionSecret,
    database: &str,
    cleanup: &mut SpannerSessions,
) -> ServeResult<(NativeClient, String)> {
    let mut client = spanner_client(h, &format!("session:{}", access.id), secret).await?;
    let name = client
        .create_session(CreateSessionRequest {
            database: database.into(),
            ..Default::default()
        })
        .await?
        .into_inner()
        .name;
    cleanup.names.push(name.clone());
    Ok((client, name))
}
async fn native_acquire(
    owner: &MgmtClient,
    isolated: bool,
    lifetime: Option<&str>,
) -> ServeResult<(SessionView, SessionSecret)> {
    let secret = SessionSecret::generate();
    let session = owner
        .acquire(AcquireParams {
            route: "spanner-native".into(),
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
fn native_cells(
    rows: &google_cloud_googleapis::spanner::v1::ResultSet,
) -> ServeResult<Vec<Vec<String>>> {
    rows.rows
        .iter()
        .map(|row| {
            row.values
                .iter()
                .map(|v| match &v.kind {
                    Some(prost_types::value::Kind::StringValue(s)) => Ok(s.clone()),
                    _ => Err("unexpected native cell type".into()),
                })
                .collect()
        })
        .collect()
}

#[tokio::test]
#[ignore = "requires NUNYA_TEST_SPANNER_EMULATOR, NUNYA_TEST_SPANNER_DB and NUNYA_TEST_SPANNER_REST"]
async fn spanner_reconnect_and_pool_every_token_type() -> ServeResult<()> {
    use google_cloud_googleapis::spanner::v1::TypeCode;
    // values::column_type maps STRING/INT64/DATE/TIMESTAMP to supported codecs.
    // BOOL has too few values (policy refuses); BYTES maps to unsupported Bytea.
    // NUMERIC/JSON/ARRAY map to unsupported Numeric/Json/Array codecs.
    // FLOAT32/FLOAT64/STRUCT/PROTO/ENUM and unspecified types map to Other,
    // which StandardCodec refuses (spanner::values::column_type, token::StandardCodec).
    let types = [
        ("email", TypeCode::String),
        ("number", TypeCode::Int64),
        ("day", TypeCode::Date),
        ("stamp", TypeCode::Timestamp),
    ];
    let f = NativeFixture::new(
        "email STRING(MAX), number INT64, day DATE, stamp TIMESTAMP",
        "email, number, day, stamp",
        "'alice@example.test', 42000000000, DATE '2024-05-06', TIMESTAMP '2024-05-06 07:08:09+00'",
        false,
    )
    .await?;
    let mut h = f.harness(
        &f.policy(&["email", "number", "day", "stamp"], 20, 200),
        None,
        "",
    )?;
    h.start().await?;
    let (access, secret, _) = h
        .acquire_session("native-types", "spanner-native", Mode::ReadWrite)
        .await?;
    let mut cleanup = f.sessions();
    let select = format!(
        "SELECT email, number, day, stamp FROM {}.t WHERE id = 1",
        f.schema
    );
    let (mut one, name) = native_open(&h, &access, &secret, &f.database, &mut cleanup).await?;
    let first = native_cells(
        &one.execute_sql(spanner_sql(&name, &select))
            .await?
            .into_inner(),
    )?;
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].len(), types.len());
    for (got, clear) in first[0].iter().zip([
        "alice@example.test",
        "42000000000",
        "2024-05-06",
        "2024-05-06T07:08:09Z",
    ]) {
        assert!(got != clear, "native blind value was not transformed");
    }
    let (mut two, name_two) = native_open(&h, &access, &secret, &f.database, &mut cleanup).await?;
    assert!(
        native_cells(
            &two.execute_sql(spanner_sql(&name_two, &select))
                .await?
                .into_inner()
        )? == first,
        "pooled native tokens changed"
    );
    native_verify(&mut two, &name_two, &select, &first, &types, &f.schema).await?;
    drop(one);
    drop(two);
    let (mut three, name_three) =
        native_open(&h, &access, &secret, &f.database, &mut cleanup).await?;
    native_verify(&mut three, &name_three, &select, &first, &types, &f.schema).await?;
    Ok(())
}
async fn native_verify(
    client: &mut NativeClient,
    name: &str,
    select: &str,
    first: &[Vec<String>],
    types: &[(&str, google_cloud_googleapis::spanner::v1::TypeCode)],
    schema: &str,
) -> ServeResult<()> {
    use google_cloud_googleapis::spanner::v1::Type;
    assert!(
        native_cells(
            &client
                .execute_sql(spanner_sql(name, select))
                .await?
                .into_inner()
        )? == first,
        "reconnected native tokens changed"
    );
    for ((col, code), token) in types.iter().zip(&first[0]) {
        let mut req = spanner_sql(name, &format!("SELECT id FROM {schema}.t WHERE {col} = @p"));
        req.params = Some(prost_types::Struct {
            fields: std::collections::BTreeMap::from([(
                "p".into(),
                prost_types::Value {
                    kind: Some(prost_types::value::Kind::StringValue(token.clone())),
                },
            )]),
        });
        req.param_types.insert(
            "p".into(),
            Type {
                code: *code as i32,
                ..Default::default()
            },
        );
        assert!(
            native_cells(&client.execute_sql(req).await?.into_inner())?
                == vec![vec!["1".to_owned()]],
            "third-channel token predicate failed for {col}"
        );
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires NUNYA_TEST_SPANNER_EMULATOR, NUNYA_TEST_SPANNER_DB and NUNYA_TEST_SPANNER_REST"]
async fn spanner_revoke_and_expiry_on_rpcs() -> ServeResult<()> {
    let f = NativeFixture::new(
        "email STRING(MAX), payload STRING(MAX)",
        "email, payload",
        "'alice@example.test', 'small'",
        false,
    )
    .await?;
    // A result larger than HTTP/2 flow-control buffers keeps an admitted stream
    // alive while management revokes it. Clear payloads carry no blind values.
    f.insert(&format!("INSERT INTO {}.t (id, email, payload) SELECT n, 'alice@example.test', REPEAT('x', 65536) FROM UNNEST(GENERATE_ARRAY(2, 513)) AS n", f.schema)).await?;
    let mut h = f.harness(
        &f.policy(&["email"], 20, 200),
        None,
        "\n[session]\nmax_lifetime = '60s'\nidle_timeout = 'off'\n",
    )?;
    h.start().await?;
    let (access, secret, principal) = h
        .acquire_session("native-ended", "spanner-native", Mode::ReadWrite)
        .await?;
    let owner = owner(&h, "native-ended", &principal)?;
    let mut cleanup = f.sessions();
    let (mut client, name) = native_open(&h, &access, &secret, &f.database, &mut cleanup).await?;
    let select = format!("SELECT email FROM {}.t WHERE id = 1", f.schema);
    client.execute_sql(spanner_sql(&name, &select)).await?;
    let mut stream = client
        .execute_streaming_sql(spanner_sql(
            &name,
            &format!("SELECT email, payload FROM {}.t", f.schema),
        ))
        .await?
        .into_inner();
    // Confirm the call was admitted before ending the access session.
    let first = stream
        .message()
        .await?
        .ok_or("stream ended before revocation")?;
    let mut completed_values = first.values.len() - usize::from(first.chunked_value);
    h.admin().revoke(&access.id).await?;
    // Chunks sent before revocation may still be in flight and cannot be recalled.
    // The gateway must send only an end status after revocation; count buffered chunks.
    // B8c3 isolated run observed 3 chunks after revoke (64 total received rows).
    let mut after_revoke = 0;
    let status = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match stream.message().await {
                Ok(Some(part)) => {
                    after_revoke += 1;
                    completed_values += part.values.len() - usize::from(part.chunked_value);
                }
                Err(status) => break status,
                Ok(None) => panic!("revoked stream completed normally"),
            }
        }
    })
    .await?;
    eprintln!(
        "buffered chunks after revoke: {after_revoke}; rows: {}",
        completed_values / 2
    );
    spanner_denied::<()>(Err(status), "NY011");
    assert!(
        completed_values / 2 < 513,
        "revoked stream delivered every row"
    );
    assert!(
        !matches!(
            tokio::time::timeout(Duration::from_secs(10), stream.message()).await?,
            Ok(Some(_))
        ),
        "stream emitted a chunk after end status"
    );
    spanner_denied(
        client.execute_sql(spanner_sql(&name, &select)).await,
        "NY003",
    );
    spanner_denied(
        client
            .execute_streaming_sql(spanner_sql(&name, &select))
            .await,
        "NY003",
    );
    spanner_denied(
        client
            .create_session(CreateSessionRequest {
                database: f.database.clone(),
                ..Default::default()
            })
            .await,
        "NY003",
    );
    assert!(
        matches!(owner.renew(&access.id, None, None).await, Err(ClientError::Api { code, .. }) if code == "session_ended"),
        "revoked native renewal accepted"
    );
    let (expiring, expiry_secret) = native_acquire(&owner, false, Some("5s")).await?;
    let (mut exp_client, exp_name) =
        native_open(&h, &expiring, &expiry_secret, &f.database, &mut cleanup).await?;
    let before = native_cells(
        &exp_client
            .execute_sql(spanner_sql(&exp_name, &select))
            .await?
            .into_inner(),
    )?;
    let renewed = owner.renew(&expiring.id, None, None).await?;
    assert_eq!(renewed.scope, expiring.scope);
    assert_eq!(
        renewed.expires_ms, expiring.expires_ms,
        "renewal extended absolute expiry"
    );
    assert!(
        native_cells(
            &exp_client
                .execute_sql(spanner_sql(&exp_name, &select))
                .await?
                .into_inner()
        )? == before,
        "renewal changed native token or channel"
    );
    let now = u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
    let wait = expiring.expires_ms.saturating_sub(now) + 150;
    assert!(wait < 10_000);
    tokio::time::sleep(Duration::from_millis(wait)).await;
    spanner_denied(
        exp_client
            .execute_sql(spanner_sql(&exp_name, &select))
            .await,
        "NY003",
    );
    spanner_denied(
        exp_client
            .execute_streaming_sql(spanner_sql(&exp_name, &select))
            .await,
        "NY003",
    );
    spanner_denied(
        exp_client
            .create_session(CreateSessionRequest {
                database: f.database.clone(),
                ..Default::default()
            })
            .await,
        "NY003",
    );
    assert!(
        matches!(owner.renew(&expiring.id, None, None).await, Err(ClientError::Api { code, .. }) if code == "session_ended"),
        "expired native renewal accepted"
    );
    Ok(())
}
async fn native_tokenize_refused(client: &mut NativeClient, name: &str) {
    let result = client
        .execute_sql(spanner_sql(name, "SELECT nunya_tokenize('email', 'x')"))
        .await;
    let error = match result {
        Err(error) => error,
        Ok(_) => panic!("native explicit tokenization accepted"),
    };
    assert_eq!(error.code(), Code::InvalidArgument);
    assert!(
        error.message().contains("NY004"),
        "unexpected native refusal"
    );
}
#[tokio::test]
#[ignore = "requires NUNYA_TEST_SPANNER_EMULATOR, NUNYA_TEST_SPANNER_DB and NUNYA_TEST_SPANNER_REST"]
async fn spanner_explicit_tokenize_refused() -> ServeResult<()> {
    // src/dialect/googlesql has no nunya_tokenize lowering; local_tokenize in
    // src/spanner/proxy.rs is reachable only from unit-built plans. The native
    // offering in site/docs/spanner.html has no explicit tokenization, so this
    // route cannot bypass per-session or per-principal tokenization limits.
    let f = NativeFixture::new("email STRING(MAX)", "email", "'alice@example.test'", false).await?;
    let mut h = f.harness(&f.policy(&["email"], 3, 5), None, "")?;
    h.start().await?;
    let (access, secret, principal) = h
        .acquire_session("native-refusal", "spanner-native", Mode::ReadWrite)
        .await?;
    let owner = owner(&h, "native-refusal", &principal)?;
    let mut cleanup = f.sessions();
    let (mut one, name_one) = native_open(&h, &access, &secret, &f.database, &mut cleanup).await?;
    native_tokenize_refused(&mut one, &name_one).await;
    drop(one);
    let salt = data_encoding::BASE64.decode(secret.scram_salt.as_bytes())?;
    let rotated = SessionSecret::generate_with(&salt, secret.scram_iterations)?;
    owner
        .renew(
            &access.id,
            None,
            Some(SessionCredential {
                verifier: rotated.verifier.clone(),
                secret_sha256: rotated.sha256_hex.clone(),
            }),
        )
        .await?;
    // Both bearers must remain valid during the rotation overlap, on new channels.
    let (mut old, name_old) = native_open(&h, &access, &secret, &f.database, &mut cleanup).await?;
    let (mut new, name_new) = native_open(&h, &access, &rotated, &f.database, &mut cleanup).await?;
    native_tokenize_refused(&mut old, &name_old).await;
    native_tokenize_refused(&mut new, &name_new).await;
    Ok(())
}
