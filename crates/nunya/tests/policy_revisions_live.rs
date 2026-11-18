//! Opt-in policy revision acceptance against PostgreSQL 18 and native Spanner.
use super::common::{NativeUpstream, ServeHarness, ServeResult};
use nunya::{
    mgmt::client::{ClientError, SessionSecret, SessionView},
    state::model::Mode,
};
use std::time::Duration;
use tokio_postgres::{NoTls, SimpleQueryMessage};
struct Driver(tokio::task::JoinHandle<()>);
impl Drop for Driver {
    fn drop(&mut self) {
        self.0.abort();
    }
}
#[derive(Default)]
struct Checks(usize);
impl Checks {
    fn pass(&mut self, label: &str, yes: bool) {
        if !yes {
            println!("SUMMARY partial passed={} failed=1", self.0);
            panic!("policy acceptance failed: {label}");
        }
        self.0 += 1;
        println!("PASS {label}");
    }
}
use googleapis_tonic_google_spanner_v1::google::spanner::v1::{
    CreateSessionRequest, ExecuteSqlRequest, ResultSet as NativeResultSet,
    spanner_client::SpannerClient,
};
use tonic::{
    Request as NativeRequest, Status as NativeStatus, transport::Endpoint as NativeEndpoint,
};

#[derive(Clone)]
struct NativeBearer(tonic::metadata::MetadataValue<tonic::metadata::Ascii>);
impl tonic::service::Interceptor for NativeBearer {
    fn call(&mut self, mut request: NativeRequest<()>) -> Result<NativeRequest<()>, NativeStatus> {
        request
            .metadata_mut()
            .insert("authorization", self.0.clone());
        Ok(request)
    }
}
type NativeClient = SpannerClient<
    tonic::service::interceptor::InterceptedService<tonic::transport::Channel, NativeBearer>,
>;

async fn native_client(
    h: &ServeHarness,
    access: &SessionView,
    secret: &SessionSecret,
) -> ServeResult<NativeClient> {
    let channel = NativeEndpoint::from_shared(format!("http://{}", h.spanner_addr()))?
        .connect()
        .await?;
    let bearer =
        NativeBearer(format!("Bearer nunya.session:{}.{}", access.id, *secret.secret).parse()?);
    Ok(SpannerClient::with_interceptor(channel, bearer))
}

fn native_sql(session: &str, sql: &str) -> ExecuteSqlRequest {
    ExecuteSqlRequest {
        session: session.into(),
        sql: sql.into(),
        ..Default::default()
    }
}

fn native_text(result: &NativeResultSet, column: usize) -> ServeResult<&str> {
    match result
        .rows
        .first()
        .and_then(|row| row.values.get(column))
        .and_then(|value| value.kind.as_ref())
    {
        Some(prost_types::value::Kind::StringValue(value)) => Ok(value),
        _ => Err("missing native string cell".into()),
    }
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
    let data = response.into_body().collect().await?.to_bytes();
    if !status.is_success() {
        return Err(format!("emulator REST request failed: HTTP {}", status.as_u16()).into());
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

async fn native_update(rest: &str, database: &str, sql: &str) -> ServeResult<()> {
    let session = native_rest(
        rest,
        "POST",
        &format!("{database}/sessions"),
        serde_json::json!({}),
    )
    .await?;
    let name = session["name"].as_str().ok_or("missing REST session")?;
    let result = async {
        let tx = native_rest(
            rest,
            "POST",
            &format!("{name}:beginTransaction"),
            serde_json::json!({"options":{"readWrite":{}}}),
        )
        .await?;
        let id = tx["id"].as_str().ok_or("missing REST transaction")?;
        native_rest(
            rest,
            "POST",
            &format!("{name}:executeSql"),
            serde_json::json!({"sql":sql,"transaction":{"id":id},"seqno":"1"}),
        )
        .await?;
        native_rest(
            rest,
            "POST",
            &format!("{name}:commit"),
            serde_json::json!({"transactionId":id}),
        )
        .await?;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    }
    .await;
    let _ = native_rest(rest, "DELETE", name, serde_json::json!({})).await;
    result
}

async fn native_session(
    h: &ServeHarness,
    database: &str,
    principal: &str,
) -> ServeResult<(NativeClient, String)> {
    let (access, secret, _) = h
        .acquire_session(principal, "spanner-native", Mode::ReadWrite)
        .await?;
    let mut client = native_client(h, &access, &secret).await?;
    let name = client
        .create_session(CreateSessionRequest {
            database: database.into(),
            ..Default::default()
        })
        .await?
        .into_inner()
        .name;
    Ok((client, name))
}
async fn native_value(c: &mut NativeClient, name: &str, sql: &str) -> ServeResult<String> {
    Ok(native_text(&c.execute_sql(native_sql(name, sql)).await?.into_inner(), 0)?.to_owned())
}

const PHONE: &str = "zz-secret-phone-5551";
const EMAIL: &str = "zz-secret-email-5551@example.test";
fn policy(mode: &str, phone: Option<&str>) -> String {
    let mut s = format!(
        "version = 2\ndefault = '{mode}'\n[domains.email]\ntokenize = 'on'\ntokenize_limit_per_session = 100\ntokenize_limit_per_principal = 1000\ntokenize_principal_window = '24h'\n[[column]]\ntable = 's5a.customers'\nname = 'id'\nshow = true\n[[column]]\ntable = 's5a.customers'\nname = 'email'\nblind = 'email'\n"
    );
    if let Some(treatment) = phone {
        s.push_str(&format!(
            "[[column]]\ntable = 's5a.customers'\nname = 'phone'\n{treatment}\n"
        ));
    }
    s
}
fn redacted(bytes: &str) -> bool {
    !bytes.contains(PHONE) && !bytes.contains(EMAIL)
}
fn api<T>(r: Result<T, ClientError>, status: u16, code: &str) -> bool {
    matches!(r, Err(ClientError::Api {status: s, code: c}) if s == status && c == code)
}
fn records(h: &ServeHarness) -> ServeResult<Vec<serde_json::Value>> {
    Ok(std::fs::read_to_string(h.dir().join("read.log"))?
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<_>, _>>()?)
}
fn policy_records(h: &ServeHarness) -> ServeResult<Vec<serde_json::Value>> {
    Ok(records(h)?
        .into_iter()
        .filter(|r| {
            r["event"]
                .as_str()
                .is_some_and(|s| s.starts_with("policy_"))
        })
        .collect())
}
async fn query(
    h: &ServeHarness,
    native: bool,
    database: &str,
    n: &mut usize,
) -> ServeResult<String> {
    *n += 1;
    let principal = format!("query-{n}");
    if native {
        let (mut c, name) = native_session(h, database, &principal).await?;
        let value =
            native_value(&mut c, &name, "SELECT phone FROM s5a.customers WHERE id=1").await?;
        c.delete_session(
            googleapis_tonic_google_spanner_v1::google::spanner::v1::DeleteSessionRequest { name },
        )
        .await?;
        Ok(value)
    } else {
        let (access, secret, _) = h
            .acquire_session(&principal, "postgres", Mode::ReadOnly)
            .await?;
        let (c, task) = h.pg_connect(&access, &secret, "app").await?;
        let _driver = Driver(task);
        for message in c
            .simple_query("SELECT phone FROM s5a.customers WHERE id=1")
            .await?
        {
            if let SimpleQueryMessage::Row(row) = message {
                return Ok(row.get(0).ok_or("missing phone")?.into());
            }
        }
        Err("missing row".into())
    }
}
async fn trigger_reload(h: &ServeHarness) -> ServeResult<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let config: toml::Table =
        toml::from_str(&std::fs::read_to_string(h.dir().join("nunya.toml"))?)?;
    let addr = config["admin_listen"].as_str().ok_or("missing admin")?;
    let before = h.admin().status().await?.config_revision;
    let mut s = tokio::net::TcpStream::connect(addr).await?;
    s.write_all(format!("POST /reload HTTP/1.1\r\nHost: {addr}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").as_bytes()).await?;
    let mut response = String::new();
    s.read_to_string(&mut response).await?;
    if !response.starts_with("HTTP/1.1 202") {
        return Err("reload not accepted".into());
    }
    // A config change supplies an observable completion marker even when policy reload is suppressed.
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if h.admin().status().await?.config_revision != before {
                return Ok::<_, ClientError>(());
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await??;
    Ok(())
}
async fn exercise(mut h: ServeHarness, native: bool, database: &str) -> ServeResult<()> {
    let route = if native { "spanner-native" } else { "postgres" };
    let mut checks = Checks::default();
    let mut n = 0;
    h.start().await?;
    let admin = h.admin();
    checks.pass(
        "file-owned",
        admin.policy_status(route).await?.owner == "file",
    );
    checks.pass(
        "initial-query",
        query(&h, native, database, &mut n).await? == PHONE,
    );
    let imported = admin
        .policy_import(route, Some("import-one".into()))
        .await?
        .0;
    checks.pass(
        "import-revision-one",
        imported.revision.number == 1
            && !imported.replayed
            && admin.policy_status(route).await?.owner == "service",
    );
    checks.pass(
        "import-audit",
        policy_records(&h)?
            .iter()
            .filter(|r| r["event"] == "policy_imported")
            .count()
            == 1,
    );
    let stamp = admin.status().await?.policy_revisions;
    checks.pass(
        "revision-stamp",
        stamp
            .get(route)
            .and_then(|s| s.as_ref())
            .is_some_and(|s| s.starts_with("rev-1-")),
    );
    std::fs::write(
        h.dir().join("policy.toml"),
        policy("show", Some("masked = true\nplaceholder = 'FILE'")),
    )?;
    let path = h.dir().join("nunya.toml");
    let mut config: toml::Table = toml::from_str(&std::fs::read_to_string(&path)?)?;
    config.insert("snapshot_refresh_ms".into(), 599999.into());
    std::fs::write(path, toml::to_string(&config)?)?;
    trigger_reload(&h).await?;
    checks.pass(
        "service-ignores-file-reload",
        admin.status().await?.policy_revisions == stamp
            && query(&h, native, database, &mut n).await? == PHONE,
    );
    let tokenize = policy("tokenize", None);
    let preview = admin.policy_preview(route, &tokenize).await?;
    checks.pass(
        "show-to-tokenize-phone",
        preview.changes["newly_tokenized"]
            .as_array()
            .is_some_and(|a| a.iter().any(|v| v["column"] == "phone")),
    );
    checks.pass(
        "preview-effective-redacted",
        redacted(&serde_json::to_string(&preview)?)
            && redacted(&serde_json::to_string(
                &admin.policy_effective(route).await?,
            )?),
    );
    let fingerprint = admin.policy_status(route).await?.catalog_fingerprint;
    let alternate = tokenize.replace(
        "tokenize_limit_per_session = 100",
        "tokenize_limit_per_session = 101",
    );
    let (a, b) = tokio::join!(
        admin.policy_apply(route, &tokenize, 1, &fingerprint, Some("race-a".into())),
        admin.policy_apply(route, &alternate, 1, &fingerprint, Some("race-b".into()))
    );
    let (winner, loser, body, key) = match (a, b) {
        (Ok(w), Err(e)) => (w.0, e, &tokenize, "race-a"),
        (Err(e), Ok(w)) => (w.0, e, &alternate, "race-b"),
        _ => return Err("concurrent apply did not produce exactly one success".into()),
    };
    checks.pass(
        "concurrent-winner-revision-two",
        winner.revision.number == 2,
    );
    checks.pass("concurrent-loser-stale-with-diff",matches!(loser,ClientError::Stale {current,diff} if current.number==2 && !diff.changes.is_empty()));
    let value = query(&h, native, database, &mut n).await?;
    checks.pass(
        "winner-served",
        value != PHONE && admin.policy_export(route).await?.policy_toml == *body,
    );
    // Preview Tokenize -> Show from an actual Tokenize-default route, regardless of race scheduling.
    let p = admin.policy_preview(route, &policy("show", None)).await?;
    checks.pass(
        "tokenize-to-show-phone",
        p.changes["newly_visible"]
            .as_array()
            .is_some_and(|a| a.iter().any(|v| v["column"] == "phone")),
    );
    checks.pass(
        "reverse-preview-redacted",
        redacted(&serde_json::to_string(&p)?),
    );
    let replay = admin
        .policy_apply(route, body, 1, &fingerprint, Some(key.into()))
        .await?
        .0;
    checks.pass(
        "idempotent-replay",
        replay.revision.number == 2 && replay.replayed,
    );
    checks.pass(
        "idempotency-conflict",
        api(
            admin
                .policy_apply(
                    route,
                    &policy("show", None),
                    1,
                    &fingerprint,
                    Some(key.into()),
                )
                .await,
            409,
            "idempotency_conflict",
        ),
    );
    checks.pass(
        "revision-count-two",
        admin.policy_revisions(route).await?.len() == 2,
    );
    let effective_before = serde_json::to_value(admin.policy_effective(route).await?)?;
    checks.pass(
        "invalid-draft-422",
        api(
            admin
                .policy_apply(
                    route,
                    "not valid [",
                    2,
                    &fingerprint,
                    Some("invalid".into()),
                )
                .await,
            422,
            "invalid_policy",
        ),
    );
    let infra = format!("{}\n[readlog]\nsink = 'stdout'\n", policy("show", None));
    checks.pass(
        "infrastructure-refused",
        api(
            admin
                .policy_apply(route, &infra, 2, &fingerprint, Some("infra".into()))
                .await,
            422,
            "infrastructure_setting",
        ),
    );
    checks.pass(
        "failures-preserve-active-and-query",
        admin
            .policy_status(route)
            .await?
            .active_revision
            .is_some_and(|r| r.number == 2)
            && query(&h, native, database, &mut n).await? != PHONE
            && serde_json::to_value(admin.policy_effective(route).await?)? == effective_before,
    );
    let rollback = admin
        .policy_rollback(route, 1, 2, Some("rollback".into()))
        .await?
        .0;
    checks.pass(
        "rollback-revision-three",
        rollback.revision.number == 3
            && matches!(
                rollback.revision.kind,
                nunya::policy::RevisionKind::Rollback { from: 1 }
            ),
    );
    checks.pass(
        "rollback-served",
        query(&h, native, database, &mut n).await? == PHONE,
    );
    h.stop()?;
    std::fs::write(
        h.dir().join("policy.toml"),
        policy("show", Some("masked = true\nplaceholder = 'RESTART-FILE'")),
    )?;
    h.start().await?;
    checks.pass(
        "restart-stored-revision",
        h.admin()
            .policy_status(route)
            .await?
            .active_revision
            .is_some_and(|r| r.number == 3)
            && query(&h, native, database, &mut n).await? == PHONE,
    );
    let audit = policy_records(&h)?;
    checks.pass("audit-redacted", redacted(&serde_json::to_string(&audit)?));
    checks.pass(
        "audit-metadata",
        audit.iter().all(|r| {
            r["route"] == route
                && !r["actor"].is_null()
                && !r["policy"]["outcome"].is_null()
                && r["policy"].get("revision").is_some()
        }),
    );
    checks.pass(
        "audit-counts",
        audit
            .iter()
            .filter(|r| r["event"] == "policy_imported")
            .count()
            == 1
            && audit
                .iter()
                .filter(|r| r["event"] == "policy_applied")
                .count()
                == 6
            && audit
                .iter()
                .filter(|r| r["event"] == "policy_rolled_back")
                .count()
                == 1,
    );
    checks.pass(
        "audit-outcomes-once",
        [
            ("active", 4),
            ("stale_revision", 1),
            ("idempotency_conflict", 1),
            ("invalid_policy", 1),
            ("infrastructure_setting", 1),
        ]
        .iter()
        .all(|(outcome, count)| {
            audit
                .iter()
                .filter(|r| r["policy"]["outcome"] == *outcome)
                .count()
                == *count
        }),
    );
    checks.pass(
        "audit-success-revisions",
        [1, 2, 3].iter().all(|n| {
            audit
                .iter()
                .any(|r| r["policy"]["revision"] == *n && r["policy"]["outcome"] == "active")
        }),
    );
    println!(
        "SUMMARY {route}_policy_revisions passed={} failed=0",
        checks.0
    );
    h.stop()?;
    Ok(())
}
fn configure(h: &ServeHarness) -> ServeResult<()> {
    let path = h.dir().join("nunya.toml");
    let mut c: toml::Table = toml::from_str(&std::fs::read_to_string(&path)?)?;
    c.insert("schemas".into(), vec!["s5a"].into());
    c.insert("search_path".into(), vec!["s5a"].into());
    c.insert("snapshot_refresh_ms".into(), 600000.into());
    c.insert("snapshot_max_age_ms".into(), 600000.into());
    c.insert(
        "readlog".into(),
        toml::Value::Table(toml::Table::from_iter([
            ("sink".into(), "file".into()),
            ("path".into(), "read.log".into()),
        ])),
    );
    std::fs::write(path, toml::to_string(&c)?)?;
    Ok(())
}
#[test]
fn policy_fixture_is_v2_and_redaction_detects_seeds() {
    assert!(policy("show", None).starts_with("version = 2"));
    assert!(!redacted(PHONE));
    assert!(!redacted(EMAIL));
    assert!(redacted("column metadata"));
}
#[tokio::test]
#[ignore = "requires PostgreSQL 18; run live-5a/run.sh"]
async fn postgres_policy_revisions() -> ServeResult<()> {
    let url = std::env::var("NUNYA_TEST_POLICY_PG")?;
    let (c, connection) = tokio_postgres::connect(&url, NoTls).await?;
    let _driver = Driver(tokio::spawn(async move {
        let _ = connection.await;
    }));
    c.batch_execute(&format!("CREATE SCHEMA s5a; CREATE TABLE s5a.customers(id bigint PRIMARY KEY,email text,phone text); INSERT INTO s5a.customers VALUES(1,'{EMAIL}','{PHONE}')")).await?;
    let h = ServeHarness::new(&policy("show", None), Some(&url), None, None, "")?;
    configure(&h)?;
    exercise(h, false, "").await
}
#[tokio::test]
#[ignore = "requires native Spanner emulator; run live-5a/run.sh"]
async fn spanner_policy_revisions() -> ServeResult<()> {
    let endpoint = std::env::var("NUNYA_TEST_SPANNER_EMULATOR")?;
    let database = std::env::var("NUNYA_TEST_SPANNER_DB")?;
    let rest = std::env::var("NUNYA_TEST_SPANNER_REST")?;
    native_ddl(&rest,&database,vec!["CREATE SCHEMA s5a".into(), "CREATE TABLE s5a.customers (id INT64 NOT NULL,email STRING(MAX),phone STRING(MAX)) PRIMARY KEY(id)".into()]).await?;
    native_update(
        &rest,
        &database,
        &format!("INSERT INTO s5a.customers(id,email,phone) VALUES(1,'{EMAIL}','{PHONE}')"),
    )
    .await?;
    let h = ServeHarness::new(
        &policy("show", None),
        None,
        None,
        Some(NativeUpstream {
            database: &database,
            emulator_endpoint: &endpoint,
        }),
        "",
    )?;
    configure(&h)?;
    exercise(h, true, &database).await
}
