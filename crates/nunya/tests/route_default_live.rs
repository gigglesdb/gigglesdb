//! Opt-in route-default acceptance checks against real PostgreSQL and native Spanner.
use super::common::{NativeUpstream, ServeHarness, ServeResult};
use nunya::{
    mgmt::client::{SessionSecret, SessionView},
    state::model::Mode,
};
use std::{process::Command, time::Duration};
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

struct Driver(tokio::task::JoinHandle<()>);
impl Drop for Driver {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[derive(Default)]
struct Checks(usize);
impl Checks {
    fn pass(&mut self, label: &str, passed: bool) {
        if !passed {
            println!("FAIL {label}");
            println!("SUMMARY partial passed={} failed=1", self.0);
            panic!("route-default acceptance failed: {label}");
        }
        self.0 += 1;
        println!("PASS {label}");
    }
    fn report(&self, label: &str) {
        println!("SUMMARY {label} passed={} failed=0", self.0);
    }
}

fn policy(schema: &str, mode: Option<&str>) -> String {
    let id_rule = if mode.is_some() {
        format!("[[column]]\ntable = '{schema}.customers'\nname = 'id'\nshow = true\n")
    } else {
        String::new()
    };
    let version = mode
        .map(|m| format!("version = 2\ndefault = '{m}'\n"))
        .unwrap_or_default();
    format!(
        "{version}[domains.email]\ntokenize = 'on'\ntokenize_limit_per_session = 100\ntokenize_limit_per_principal = 1000\ntokenize_principal_window = '24h'\n{id_rule}[[column]]\ntable = '{schema}.customers'\nname = 'email'\nblind = 'email'\n"
    )
}

fn configure(h: &ServeHarness, schema: Option<&str>, refresh_ms: i64) -> ServeResult<()> {
    let path = h.dir().join("nunya.toml");
    let mut config: toml::Table = toml::from_str(&std::fs::read_to_string(&path)?)?;
    config.insert("snapshot_refresh_ms".into(), refresh_ms.into());
    config.insert("snapshot_max_age_ms".into(), 600000.into());
    config.insert(
        "readlog".into(),
        toml::Value::Table(toml::Table::from_iter([
            ("sink".into(), "file".into()),
            ("path".into(), "read.log".into()),
        ])),
    );
    if let Some(schema) = schema {
        config.insert("schemas".into(), vec![schema].into());
        config.insert("search_path".into(), vec![schema].into());
    }
    if let Some(native) = config
        .get_mut("spanner_native")
        .and_then(toml::Value::as_table_mut)
    {
        native.insert("snapshot_refresh_ms".into(), refresh_ms.into());
    }
    std::fs::write(path, toml::to_string(&config)?)?;
    Ok(())
}

async fn refresh(h: &ServeHarness) -> ServeResult<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    configure(h, None, 100)?;
    let config: toml::Table =
        toml::from_str(&std::fs::read_to_string(h.dir().join("nunya.toml"))?)?;
    let addr = config["admin_listen"]
        .as_str()
        .ok_or("missing admin address")?;
    let mut stream = tokio::net::TcpStream::connect(addr).await?;
    stream.write_all(format!("POST /reload HTTP/1.1\r\nHost: {addr}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").as_bytes()).await?;
    let mut response = String::new();
    stream.read_to_string(&mut response).await?;
    assert!(
        response.starts_with("HTTP/1.1 202"),
        "refresh reload request refused"
    );
    println!(
        "REFRESH config POST /reload changes snapshot refresh interval from 600000ms to 100ms"
    );
    Ok(())
}

async fn rows(client: &Client, sql: &str) -> ServeResult<Vec<Vec<Option<String>>>> {
    Ok(client
        .simple_query(sql)
        .await?
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|i| row.get(i).map(str::to_owned))
                    .collect(),
            ),
            _ => None,
        })
        .collect())
}

fn cell(rows: &[Vec<Option<String>>], column: usize) -> ServeResult<&str> {
    rows.first()
        .and_then(|row| row.get(column))
        .and_then(Option::as_deref)
        .ok_or("missing expected non-NULL result".into())
}

fn generated_token(value: &str) -> bool {
    let Some(rest) = value.strip_prefix("tok_") else {
        return false;
    };
    let Some((domain, payload)) = rest.split_once('_') else {
        return false;
    };
    domain.len() == 11
        && domain.starts_with('f')
        && domain[1..]
            .bytes()
            .all(|b| b.is_ascii_lowercase() || (b'2'..=b'7').contains(&b))
        && !payload.is_empty()
        && domain != "email"
}

async fn pg_refused(
    client: &Client,
    sql: &str,
    label: &str,
    checks: &mut Checks,
) -> ServeResult<()> {
    let result = client.simple_query(sql).await;
    let code = match result {
        Err(error) => error
            .code()
            .map(|code| code.code().to_owned())
            .ok_or("missing SQLSTATE refusal")?,
        Ok(_) => {
            return Err(format!("FAIL {label}: statement unexpectedly accepted; SQL={sql}").into());
        }
    };
    checks.pass(label, code.starts_with("NY"));
    println!("CODE {label} {code}");
    Ok(())
}

struct PgFixture {
    url: String,
    schema: String,
}
impl Drop for PgFixture {
    fn drop(&mut self) {
        let url = self.url.clone();
        let schema = self.schema.clone();
        let result = std::thread::spawn(move || -> ServeResult<()> {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?
                .block_on(async {
                    let (client, connection) = tokio_postgres::connect(&url, NoTls).await?;
                    let _driver = Driver(tokio::spawn(async move {
                        let _ = connection.await;
                    }));
                    client
                        .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
                        .await?;
                    Ok(())
                })
        })
        .join();
        if !matches!(result, Ok(Ok(()))) {
            eprintln!("PostgreSQL route-default fixture cleanup failed");
        }
    }
}

fn run_drivers(h: &ServeHarness, session: &SessionView, secret: &SessionSecret) -> ServeResult<()> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    for (variable, java) in [
        ("NUNYA_TEST_DEFAULTS_PYTHON", false),
        ("NUNYA_TEST_DEFAULTS_JAVA", true),
    ] {
        let Some(executable) = std::env::var_os(variable) else {
            continue;
        };
        let mut command = Command::new(executable);
        if java {
            command
                .arg("--class-path")
                .arg(std::env::var("NUNYA_TEST_DEFAULTS_JAVA_CLASSPATH")?)
                .arg(root.join("harness/drivers/DefaultsCheck.java"));
        } else {
            command.arg(root.join("harness/drivers/defaults_check.py"));
        }
        command
            .env("PGHOST", "127.0.0.1")
            .env("PGPORT", h.pg_port().to_string())
            .env("PGDATABASE", "app")
            .env("PGUSER", format!("session:{}", session.id))
            .env("PGPASSWORD", &*secret.secret)
            .env("PGSSLMODE", "disable")
            .env("NUNYA_TEST_DEFAULTS_DECLARED_DOMAINS", "email");
        let output = command.output()?;
        // Driver scripts print only fixed PASS/FAIL labels and counts, never database values.
        print!("{}", String::from_utf8_lossy(&output.stdout));
        assert!(
            output.status.success(),
            "route-default external driver failed"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires NUNYA_TEST_DEFAULTS_PG and NUNYA_TEST_DEFAULTS_PG_ADMIN; optional Python/JDBC driver env"]
async fn postgres_route_defaults() -> ServeResult<()> {
    let url = std::env::var("NUNYA_TEST_DEFAULTS_PG")?;
    let admin_url = std::env::var("NUNYA_TEST_DEFAULTS_PG_ADMIN")?;
    for mode in [Some("tokenize"), Some("show"), None] {
        let schema = format!(
            "defaults_{}",
            &nunya::token::SessionId::random().to_hex()[..12]
        );
        let fixture = PgFixture {
            url: admin_url.clone(),
            schema: schema.clone(),
        };
        let (admin, connection) = tokio_postgres::connect(&admin_url, NoTls).await?;
        let _admin_driver = Driver(tokio::spawn(async move {
            let _ = connection.await;
        }));
        admin.batch_execute(&format!("CREATE SCHEMA {schema}; CREATE TYPE {schema}.customer_kind AS ENUM ('x', 'y'); CREATE TABLE {schema}.customers (id int PRIMARY KEY, email text, tier int2, active bool, kind {schema}.customer_kind); INSERT INTO {schema}.customers VALUES (1, 'first@example.test', 1, true, 'x'), (2, 'second@example.test', 2, false, 'y'); GRANT USAGE ON SCHEMA {schema} TO PUBLIC; GRANT SELECT ON {schema}.customers TO PUBLIC")).await?;
        let mut h = ServeHarness::new(&policy(&schema, mode), Some(&url), None, None, "")?;
        configure(&h, Some(&schema), 600000)?;
        h.start().await?;
        let (session, secret, _) = h
            .acquire_session("defaults-agent", "postgres", Mode::ReadOnly)
            .await?;
        let (client, driver) = h.pg_connect(&session, &secret, "app").await?;
        let driver = Driver(driver);
        let mut checks = Checks::default();
        if mode.is_none() {
            let result = rows(
                &client,
                "SELECT tier, active, kind FROM customers WHERE id = 1",
            )
            .await?;
            checks.pass(
                "legacy-unlisted-values",
                result == vec![vec![Some("1".into()), Some("t".into()), Some("x".into())]],
            );
            checks.report("postgres-legacy");
            h.stop()?;
            drop(fixture);
            continue;
        }
        // Establish the old shape on this same open connection before upstream DDL.
        rows(&client, "SELECT * FROM customers WHERE id = 1").await?;
        let (column_client, column_driver) = h.pg_connect(&session, &secret, "app").await?;
        admin.batch_execute(&format!("ALTER TABLE {schema}.customers ADD COLUMN recovery_email text; UPDATE {schema}.customers SET recovery_email = 'recovery@example.test' WHERE id = 1; UPDATE {schema}.customers SET recovery_email = 'other-recovery@example.test' WHERE id = 2")).await?;
        pg_refused(
            &client,
            "SELECT * FROM customers",
            "pre-refresh-star",
            &mut checks,
        )
        .await?;
        drop(client);
        drop(driver);
        let client = column_client;
        let driver = Driver(column_driver);
        pg_refused(
            &client,
            "SELECT recovery_email FROM customers",
            "pre-refresh-column",
            &mut checks,
        )
        .await?;
        refresh(&h).await?;
        let direct = tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                if let Ok(result) =
                    rows(&client, "SELECT recovery_email FROM customers WHERE id = 1").await
                {
                    return Ok::<_, Box<dyn std::error::Error + Send + Sync>>(result);
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .map_err(|_| "snapshot refresh did not expose recovery_email")??;
        let value = cell(&direct, 0)?.to_owned();
        let star = rows(&client, "SELECT * FROM customers WHERE id = 1").await?;
        if mode == Some("tokenize") {
            checks.pass(
                "generated-direct-token",
                generated_token(&value) && value != "recovery@example.test",
            );
            let email = cell(&star, 1)?;
            checks.pass(
                "generated-domain-distinct",
                email.starts_with("tok_email_") && !value.starts_with("tok_email_"),
            );
            checks.pass("star-token", cell(&star, 5)? == value);
            for (index, label) in [(2, "tier-null"), (3, "active-null"), (4, "kind-null")] {
                checks.pass(
                    label,
                    star.first()
                        .and_then(|row| row.get(index))
                        .is_some_and(Option::is_none),
                );
            }
            for (sql, label) in [
                ("SELECT id FROM customers WHERE tier = 1", "tier-predicate"),
                ("SELECT id FROM customers WHERE active", "active-predicate"),
                (
                    "SELECT id FROM customers WHERE kind = 'x'",
                    "kind-predicate",
                ),
            ] {
                pg_refused(&client, sql, label, &mut checks).await?;
            }
            drop(client);
            drop(driver);
            let (again, task) = h.pg_connect(&session, &secret, "app").await?;
            let _again_driver = Driver(task);
            checks.pass(
                "reconnect-stable",
                cell(
                    &rows(&again, "SELECT recovery_email FROM customers WHERE id = 1").await?,
                    0,
                )? == value,
            );
            // Blocking scripts must run off the runtime thread while serve and management stay live.
            tokio::task::block_in_place(|| run_drivers(&h, &session, &secret))?;
        } else {
            checks.pass("show-direct", value == "recovery@example.test");
            checks.pass("show-star", cell(&star, 5)? == "recovery@example.test");
        }
        checks.report(if mode == Some("tokenize") {
            "postgres-tokenize"
        } else {
            "postgres-show"
        });
        h.stop()?;
        drop(fixture);
    }
    Ok(())
}

use googleapis_tonic_google_spanner_v1::google::spanner::v1::{
    CreateSessionRequest, DeleteSessionRequest, ExecuteSqlRequest, KeySet, ReadRequest,
    ResultSet as NativeResultSet, spanner_client::SpannerClient,
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

fn native_read(session: &str, table: &str, columns: &[&str]) -> ReadRequest {
    ReadRequest {
        session: session.into(),
        table: table.into(),
        columns: columns.iter().map(|column| (*column).into()).collect(),
        key_set: Some(KeySet {
            keys: vec![prost_types::ListValue {
                values: vec![prost_types::Value {
                    kind: Some(prost_types::value::Kind::StringValue("1".into())),
                }],
            }],
            ..Default::default()
        }),
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

fn native_null(result: &NativeResultSet, column: usize) -> bool {
    matches!(
        result
            .rows
            .first()
            .and_then(|row| row.values.get(column))
            .and_then(|value| value.kind.as_ref()),
        Some(prost_types::value::Kind::NullValue(_))
    )
}

fn native_refused<T>(
    result: Result<T, NativeStatus>,
    label: &str,
    checks: &mut Checks,
) -> ServeResult<()> {
    let error = match result {
        Err(error) => error,
        Ok(_) => {
            return Err(format!("FAIL {label}: native statement unexpectedly accepted").into());
        }
    };
    let code = error
        .message()
        .as_bytes()
        .windows(5)
        .find(|bytes| bytes[0..2] == *b"NY" && bytes[2..].iter().all(u8::is_ascii_digit))
        .and_then(|bytes| std::str::from_utf8(bytes).ok())
        .ok_or("missing native NY refusal code")?;
    checks.pass(label, true);
    println!("CODE {label} {code}");
    Ok(())
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

struct NativeFixture {
    rest: String,
    database: String,
    schema: String,
    names: Vec<String>,
}
impl Drop for NativeFixture {
    fn drop(&mut self) {
        let (rest, database, schema, names) = (
            self.rest.clone(),
            self.database.clone(),
            self.schema.clone(),
            self.names.clone(),
        );
        let result = std::thread::spawn(move || -> ServeResult<()> {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?
                .block_on(async {
                    for name in names {
                        let _ = native_rest(&rest, "DELETE", &name, serde_json::json!({})).await;
                    }
                    native_ddl(
                        &rest,
                        &database,
                        vec![
                            format!("DROP TABLE {schema}.customers"),
                            format!("DROP SCHEMA {schema}"),
                        ],
                    )
                    .await
                })
        })
        .join();
        if !matches!(result, Ok(Ok(()))) {
            eprintln!("native route-default fixture cleanup failed");
        }
    }
}

#[tokio::test]
#[ignore = "requires NUNYA_TEST_SPANNER_EMULATOR, NUNYA_TEST_SPANNER_REST and NUNYA_TEST_SPANNER_DB"]
async fn spanner_route_defaults() -> ServeResult<()> {
    let rest = std::env::var("NUNYA_TEST_SPANNER_REST")?;
    let database = std::env::var("NUNYA_TEST_SPANNER_DB")?;
    let endpoint = format!("http://{}", std::env::var("NUNYA_TEST_SPANNER_EMULATOR")?);
    for mode in ["tokenize", "show"] {
        let schema = format!(
            "defaults_{}",
            &nunya::token::SessionId::random().to_hex()[..12]
        );
        let mut fixture = NativeFixture {
            rest: rest.clone(),
            database: database.clone(),
            schema: schema.clone(),
            names: vec![],
        };
        let table = format!("{schema}.customers");
        native_ddl(&rest, &database, vec![format!("CREATE SCHEMA {schema}"), format!("CREATE TABLE {table} (id INT64 NOT NULL, email STRING(MAX), active BOOL) PRIMARY KEY (id)")]).await?;
        native_update(&rest, &database, &format!("INSERT INTO {table} (id, email, active) VALUES (1, 'first@example.test', true), (2, 'second@example.test', false)")).await?;
        let mut h = ServeHarness::new(
            &policy(&schema, Some(mode)),
            None,
            None,
            Some(NativeUpstream {
                database: &database,
                emulator_endpoint: &endpoint,
            }),
            "",
        )?;
        configure(&h, None, 600000)?;
        h.start().await?;
        let (access, secret, _) = h
            .acquire_session("defaults-native-agent", "spanner-native", Mode::ReadOnly)
            .await?;
        let mut client = native_client(&h, &access, &secret).await?;
        let name = client
            .create_session(CreateSessionRequest {
                database: database.clone(),
                ..Default::default()
            })
            .await?
            .into_inner()
            .name;
        fixture.names.push(name.clone());
        let mut checks = Checks::default();
        client
            .execute_sql(native_sql(
                &name,
                &format!("SELECT * FROM {table} WHERE id = 1"),
            ))
            .await?;
        native_ddl(
            &rest,
            &database,
            vec![format!(
                "ALTER TABLE {table} ADD COLUMN recovery_email STRING(MAX)"
            )],
        )
        .await?;
        native_update(
            &rest,
            &database,
            &format!("UPDATE {table} SET recovery_email = 'recovery@example.test' WHERE id = 1"),
        )
        .await?;
        native_refused(
            client
                .execute_sql(native_sql(
                    &name,
                    &format!("SELECT recovery_email FROM {table} WHERE id = 1"),
                ))
                .await,
            "native-pre-refresh-column",
            &mut checks,
        )?;
        native_refused(
            client
                .read(native_read(&name, &table, &["recovery_email"]))
                .await,
            "native-pre-refresh-read",
            &mut checks,
        )?;
        refresh(&h).await?;
        let select = native_sql(
            &name,
            &format!("SELECT recovery_email FROM {table} WHERE id = 1"),
        );
        let direct = tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                if let Ok(response) = client.execute_sql(select.clone()).await {
                    return response.into_inner();
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .map_err(|_| "native snapshot refresh did not expose recovery_email")?;
        let value = native_text(&direct, 0)?.to_owned();
        let star = client
            .execute_sql(native_sql(
                &name,
                &format!("SELECT * FROM {table} WHERE id = 1"),
            ))
            .await?
            .into_inner();
        let read = client
            .read(native_read(
                &name,
                &table,
                &["id", "email", "active", "recovery_email"],
            ))
            .await?
            .into_inner();
        if mode == "tokenize" {
            checks.pass(
                "native-generated-direct-token",
                generated_token(&value) && value != "recovery@example.test",
            );
            checks.pass(
                "native-generated-domain-distinct",
                native_text(&star, 1)?.starts_with("tok_email_")
                    && !value.starts_with("tok_email_"),
            );
            checks.pass("native-star-token", native_text(&star, 3)? == value);
            checks.pass("native-read-token", native_text(&read, 3)? == value);
            checks.pass("native-bool-star-null", native_null(&star, 2));
            checks.pass("native-bool-read-null", native_null(&read, 2));
            native_refused(
                client
                    .execute_sql(native_sql(
                        &name,
                        &format!("SELECT id FROM {table} WHERE active"),
                    ))
                    .await,
                "native-bool-predicate",
                &mut checks,
            )?;
            drop(client);
            let mut again = native_client(&h, &access, &secret).await?;
            let again_name = again
                .create_session(CreateSessionRequest {
                    database: database.clone(),
                    ..Default::default()
                })
                .await?
                .into_inner()
                .name;
            fixture.names.push(again_name.clone());
            let reread = again
                .execute_sql(native_sql(
                    &again_name,
                    &format!("SELECT recovery_email FROM {table} WHERE id = 1"),
                ))
                .await?
                .into_inner();
            checks.pass("native-reconnect-stable", native_text(&reread, 0)? == value);
            again
                .delete_session(DeleteSessionRequest { name: again_name })
                .await?;
        } else {
            checks.pass("native-show-direct", value == "recovery@example.test");
            checks.pass(
                "native-show-star",
                native_text(&star, 3)? == "recovery@example.test",
            );
            checks.pass(
                "native-show-read",
                native_text(&read, 3)? == "recovery@example.test",
            );
        }
        checks.report(if mode == "tokenize" {
            "spanner-tokenize"
        } else {
            "spanner-show"
        });
        h.stop()?;
        drop(fixture);
    }
    Ok(())
}
