//! Opt-in shared-domain link/unlink regression checks.
use super::common::{NativeUpstream, ServeHarness, ServeResult};
use nunya::mgmt::client::{SessionSecret, SessionView};
use nunya::state::model::Mode;
use std::time::Duration;
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

struct Driver(tokio::task::JoinHandle<()>);
impl Drop for Driver {
    fn drop(&mut self) {
        self.0.abort();
    }
}

fn policy(schema: &str, mode: &str, linked: bool) -> String {
    let mut text = format!("version = 2\ndefault = '{mode}'\n");
    if mode == "show" {
        for domain in ["customer_email", "outbox_email"] {
            text.push_str(&format!("[domains.{domain}]\ntokenize = 'on'\ntokenize_limit_per_session = 1000\ntokenize_limit_per_principal = 10000\ntokenize_principal_window = '24h'\n"));
        }
    }
    for (table, name, domain) in [
        ("customers", "email", "customer_email"),
        ("outbox", "to_address", "outbox_email"),
    ] {
        text.push_str(&format!(
            "[[column]]\ntable = '{schema}.{table}'\nname = 'id'\nshow = true\n"
        ));
        if mode == "show" && !linked {
            text.push_str(&format!(
                "[[column]]\ntable = '{schema}.{table}'\nname = '{name}'\nblind = '{domain}'\n"
            ));
        }
    }
    if linked && mode == "show" {
        text.push_str("[[link]]\ndomain = 'customer_email'\n");
    }
    if linked {
        text.push_str(&format!(
            "{}fields = ['{schema}.customers.email', '{schema}.outbox.to_address']\n",
            if mode == "show" { "" } else { "[[link]]\n" }
        ));
    }
    text
}
async fn token(c: &Client, sql: &str) -> ServeResult<String> {
    for row in c.simple_query(sql).await? {
        if let SimpleQueryMessage::Row(row) = row {
            return row.get(0).map(str::to_owned).ok_or("missing token".into());
        }
    }
    Err("missing row".into())
}
fn check(label: &str, yes: bool, passed: &mut usize) {
    if !yes {
        println!("FAIL {label}; SUMMARY passed={passed} failed=1");
        panic!("shared-domain acceptance failed: {label}");
    }
    *passed += 1;
    println!("PASS {label}");
}
async fn refused(c: &Client, sql: &str, label: &str, passed: &mut usize) -> ServeResult<()> {
    let result = c.simple_query(sql).await;
    let code = result
        .as_ref()
        .err()
        .and_then(|e| e.code())
        .map(|c| c.code());
    println!("CODE {label} {}", code.unwrap_or("none"));
    check(label, code.is_some_and(|c| c.starts_with("NY")), passed);
    if let Err(e) = result {
        check(
            "error-values-redacted",
            !e.to_string().contains("example.test"),
            passed,
        );
    }
    Ok(())
}
async fn reload(h: &ServeHarness, text: &str) -> ServeResult<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let before = h.admin().status().await?.policy_revisions;
    std::fs::write(h.dir().join("policy.toml"), text)?;
    let config: toml::Table =
        toml::from_str(&std::fs::read_to_string(h.dir().join("nunya.toml"))?)?;
    let addr = config["admin_listen"]
        .as_str()
        .ok_or("missing admin address")?;
    let mut stream = tokio::net::TcpStream::connect(addr).await?;
    stream.write_all(format!("POST /reload HTTP/1.1\r\nHost: {addr}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").as_bytes()).await?;
    let mut response = String::new();
    stream.read_to_string(&mut response).await?;
    if !response.starts_with("HTTP/1.1 202") {
        return Err("reload refused".into());
    }
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        loop {
            if h.admin().status().await?.policy_revisions != before {
                println!("PASS reload-applied");
                return Ok::<_, Box<dyn std::error::Error + Send + Sync>>(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    })
    .await
    .map_err(|_| "FAIL reload-applied: policy revision did not change")??;
    Ok(())
}

struct ExternalDriver {
    child: std::process::Child,
    output: std::io::BufReader<std::process::ChildStdout>,
}
impl Drop for ExternalDriver {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
fn start_drivers(h: &ServeHarness, user: &str, password: &str) -> ServeResult<Vec<ExternalDriver>> {
    use std::io::BufRead;
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut drivers = vec![];
    for (variable, java) in [
        ("NUNYA_TEST_LINK_PYTHON", false),
        ("NUNYA_TEST_LINK_JAVA", true),
    ] {
        let Some(executable) = std::env::var_os(variable) else {
            continue;
        };
        let mut command = std::process::Command::new(executable);
        if java {
            command
                .arg("--class-path")
                .arg(std::env::var("NUNYA_TEST_LINK_JAVA_CLASSPATH")?)
                .arg(root.join("harness/drivers/SharedDomainsCheck.java"));
        } else {
            command.arg(root.join("harness/drivers/shared_domains_check.py"));
        }
        let mut child = command
            .env("PGHOST", "127.0.0.1")
            .env("PGPORT", h.pg_port().to_string())
            .env("PGUSER", user)
            .env("PGPASSWORD", password)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .spawn()?;
        let mut output =
            std::io::BufReader::new(child.stdout.take().ok_or("missing driver output")?);
        loop {
            let mut line = String::new();
            if output.read_line(&mut line)? == 0 {
                return Err("external driver failed before reload".into());
            }
            if line.trim() == "READY" {
                break;
            }
            print!("{line}");
        }
        drivers.push(ExternalDriver { child, output });
    }
    Ok(drivers)
}
fn finish_drivers(drivers: &mut [ExternalDriver], user: &str, password: &str) -> ServeResult<()> {
    use std::io::{Read, Write};
    for driver in drivers {
        let input = driver.child.stdin.as_mut().ok_or("missing driver input")?;
        writeln!(input, "{user}\n{password}")?;
        input.flush()?;
        let mut output = String::new();
        driver.output.read_to_string(&mut output)?;
        print!("{output}");
        if !driver.child.wait()?.success() {
            return Err("external shared-domain driver failed".into());
        }
    }
    Ok(())
}
fn audit(h: &ServeHarness, since: usize, route: &str, passed: &mut usize) -> ServeResult<()> {
    let log = std::fs::read_to_string(h.dir().join("read.log"))?;
    let records = log
        .lines()
        .skip(since)
        .map(serde_json::from_str::<serde_json::Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let resets: Vec<_> = records
        .iter()
        .filter(|r| r["event"] == "membership_reset" && r["route"] == route)
        .collect();
    check("membership-reset-count", resets.len() == 1, passed);
    check(
        "membership-reset-sessions",
        resets
            .iter()
            .all(|r| r["sessions"].as_u64().is_some_and(|n| n >= 1)),
        passed,
    );
    check(
        "membership-reset-redacted",
        resets
            .iter()
            .all(|r| !r.to_string().contains("example.test") && !r.to_string().contains("tok_")),
        passed,
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires NUNYA_TEST_LINK_PG and NUNYA_TEST_LINK_PG_ADMIN"]
async fn postgres_shared_domains() -> ServeResult<()> {
    let url = std::env::var("NUNYA_TEST_LINK_PG")?;
    let (admin, connection) =
        tokio_postgres::connect(&std::env::var("NUNYA_TEST_LINK_PG_ADMIN")?, NoTls).await?;
    let _admin_driver = Driver(tokio::spawn(async move {
        let _ = connection.await;
    }));
    for mode in ["tokenize", "show"] {
        let schema = format!(
            "links_{}",
            &nunya::token::SessionId::random().to_hex()[..12]
        );
        admin.batch_execute(&format!("CREATE SCHEMA {schema}; CREATE TABLE {schema}.customers(id int primary key, email text); CREATE TABLE {schema}.outbox(id int primary key, to_address text); INSERT INTO {schema}.customers VALUES(1,'first@example.test'),(3,'second@example.test'); INSERT INTO {schema}.outbox VALUES(2,'first@example.test');")).await?;
        let native_fixture = native_fixture(&schema).await?;
        let endpoint = format!("http://{}", std::env::var("NUNYA_TEST_SPANNER_EMULATOR")?);
        let mut h = ServeHarness::new(
            &policy(&schema, mode, false),
            Some(&url),
            None,
            Some(NativeUpstream {
                database: &native_fixture.database,
                emulator_endpoint: &endpoint,
            }),
            "",
        )?;
        separate_policy(&h, "spanner_native", &policy(&schema, mode, false))?;
        let path = h.dir().join("nunya.toml");
        let mut config: toml::Table = toml::from_str(&std::fs::read_to_string(&path)?)?;
        config.insert("schemas".into(), vec![schema.as_str()].into());
        config.insert("search_path".into(), vec![schema.as_str()].into());
        config.insert("snapshot_refresh_ms".into(), 600000.into());
        config.insert("snapshot_max_age_ms".into(), 600000.into());
        config.insert(
            "readlog".into(),
            toml::Value::Table(toml::Table::from_iter([
                ("sink".into(), "file".into()),
                ("path".into(), "read.log".into()),
            ])),
        );
        std::fs::write(path, toml::to_string(&config)?)?;
        h.start().await?;
        let (mut other, other_name) =
            native_session(&h, &native_fixture.database, "other-native-agent").await?;
        let other_sql = format!("SELECT email FROM {schema}.customers WHERE id=1");
        let other_old = native_value(&mut other, &other_name, &other_sql).await?;
        let (s, secret, _) = h
            .acquire_session("link-agent", "postgres", Mode::ReadWrite)
            .await?;
        let (mut c, task) = h.pg_connect(&s, &secret, "app").await?;
        let _driver = Driver(task);
        let mut passed = 0;
        let old = token(&c, "SELECT email FROM customers WHERE id=1").await?;
        let out = token(&c, "SELECT to_address FROM outbox WHERE id=2").await?;
        check("before-distinct", old != out, &mut passed);
        let join =
            "SELECT customers.id FROM customers JOIN outbox ON customers.email = outbox.to_address";
        let write = "INSERT INTO outbox(id,to_address) SELECT id,email FROM customers";
        refused(&c, join, "before-join", &mut passed).await?;
        refused(&c, write, "before-write", &mut passed).await?;
        refused(
            &c,
            &format!("UPDATE outbox SET to_address='{old}' WHERE id=2"),
            "before-update",
            &mut passed,
        )
        .await?;
        let (external, external_secret, _) = h
            .acquire_session("external-link-agent", "postgres", Mode::ReadWrite)
            .await?;
        let mut external_drivers = tokio::task::block_in_place(|| {
            start_drivers(
                &h,
                &format!("session:{}", external.id),
                &external_secret.secret,
            )
        })?;
        let tx = c.transaction().await?;
        let prepared = tx.prepare("SELECT email FROM customers").await?;
        let portal = tx.bind(&prepared, &[]).await?;
        let _ = tx.query_portal(&portal, 1).await?;
        let audit_start = std::fs::read_to_string(h.dir().join("read.log"))?
            .lines()
            .count();
        reload(&h, &policy(&schema, mode, true)).await?;
        let reset = tx.query_portal(&portal, 1).await;
        println!(
            "RESET portal result={} code={}",
            if reset.is_ok() { "success" } else { "error" },
            reset
                .as_ref()
                .err()
                .and_then(|e| e.code())
                .map_or("none", |c| c.code())
        );
        println!("RESET portal rows={}", reset.as_ref().map_or(0, Vec::len));
        check(
            "link-reset-NY011-or-closed",
            reset.as_ref().err().is_some_and(|e| {
                e.code()
                    .map_or_else(|| e.is_closed(), |c| c.code() == "NY011")
            }),
            &mut passed,
        );
        let (s2, secret2, _) = h
            .acquire_session("linked-agent", "postgres", Mode::ReadWrite)
            .await?;
        let (c2, task) = h.pg_connect(&s2, &secret2, "app").await?;
        let _driver2 = Driver(task);
        tokio::task::block_in_place(|| {
            finish_drivers(
                &mut external_drivers,
                &format!("session:{}", s2.id),
                &secret2.secret,
            )
        })?;
        audit(&h, audit_start, "postgres", &mut passed)?;
        check(
            "unrelated-route-stable",
            other_old == native_value(&mut other, &other_name, &other_sql).await?,
            &mut passed,
        );
        let linked = token(&c2, "SELECT email FROM customers WHERE id=1").await?;
        check(
            "linked-equal",
            linked == token(&c2, "SELECT to_address FROM outbox WHERE id=2").await?,
            &mut passed,
        );
        check(
            "linked-join",
            c2.simple_query(join)
                .await?
                .iter()
                .any(|r| matches!(r, SimpleQueryMessage::Row(_))),
            &mut passed,
        );
        c2.simple_query(write).await?;
        check("linked-write", true, &mut passed);
        refused(
            &c2,
            &format!("SELECT id FROM customers WHERE email='{old}'"),
            "old-literal",
            &mut passed,
        )
        .await?;
        let result = c2
            .query("SELECT id FROM customers WHERE email=$1", &[&old])
            .await;
        println!(
            "CODE old-parameter {}",
            result
                .as_ref()
                .err()
                .and_then(|e| e.code())
                .map_or("none", |c| c.code())
        );
        check(
            "old-parameter",
            result
                .as_ref()
                .err()
                .and_then(|e| e.code())
                .is_some_and(|c| c.code().starts_with("NY")),
            &mut passed,
        );
        let audit_start = std::fs::read_to_string(h.dir().join("read.log"))?
            .lines()
            .count();
        reload(&h, &policy(&schema, mode, false)).await?;
        let reset = c2.simple_query("SELECT id FROM customers").await;
        check(
            "unlink-reset-NY011-or-closed",
            reset.as_ref().err().is_some_and(|e| {
                e.code()
                    .map_or_else(|| e.is_closed(), |c| c.code() == "NY011")
            }),
            &mut passed,
        );
        audit(&h, audit_start, "postgres", &mut passed)?;
        check(
            "unlink-unrelated-route-stable",
            other_old == native_value(&mut other, &other_name, &other_sql).await?,
            &mut passed,
        );
        let (s3, secret3, _) = h
            .acquire_session("unlinked-agent", "postgres", Mode::ReadWrite)
            .await?;
        let (c3, task) = h.pg_connect(&s3, &secret3, "app").await?;
        let _driver3 = Driver(task);
        refused(&c3, join, "unlink-join", &mut passed).await?;
        refused(&c3, write, "unlink-write", &mut passed).await?;
        refused(
            &c3,
            &format!("SELECT id FROM customers WHERE email='{linked}'"),
            "linked-token-retired",
            &mut passed,
        )
        .await?;
        println!("SUMMARY postgres-{mode} passed={passed} failed=0");
        h.stop()?;
        drop(native_fixture);
        admin
            .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
            .await?;
    }
    Ok(())
}

#[derive(Default)]
struct Checks(usize);
impl Checks {
    fn pass(&mut self, label: &str, yes: bool) {
        check(label, yes, &mut self.0);
    }
}
use googleapis_tonic_google_spanner_v1::google::spanner::v1::{
    CreateSessionRequest, ExecuteSqlRequest, KeySet, ReadRequest, ResultSet as NativeResultSet,
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
                            format!("DROP TABLE {schema}.outbox"),
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

async fn native_fixture(schema: &str) -> ServeResult<NativeFixture> {
    let rest = std::env::var("NUNYA_TEST_SPANNER_REST")?;
    let database = std::env::var("NUNYA_TEST_SPANNER_DB")?;
    native_ddl(&rest, &database, vec![format!("CREATE SCHEMA {schema}"),
        format!("CREATE TABLE {schema}.customers (id INT64 NOT NULL, email STRING(MAX)) PRIMARY KEY(id)"),
        format!("CREATE TABLE {schema}.outbox (id INT64 NOT NULL, to_address STRING(MAX)) PRIMARY KEY(id)")]).await?;
    native_update(&rest, &database, &format!("INSERT INTO {schema}.customers(id,email) VALUES(1,'first@example.test'),(3,'second@example.test')")).await?;
    native_update(
        &rest,
        &database,
        &format!("INSERT INTO {schema}.outbox(id,to_address) VALUES(2,'first@example.test')"),
    )
    .await?;
    Ok(NativeFixture {
        rest,
        database,
        schema: schema.into(),
        names: vec![],
    })
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
fn separate_policy(h: &ServeHarness, section: &str, text: &str) -> ServeResult<()> {
    std::fs::write(h.dir().join("other-policy.toml"), text)?;
    let path = h.dir().join("nunya.toml");
    let mut config: toml::Table = toml::from_str(&std::fs::read_to_string(&path)?)?;
    config
        .get_mut(section)
        .and_then(toml::Value::as_table_mut)
        .ok_or("missing other route")?
        .insert("policy".into(), "other-policy.toml".into());
    std::fs::write(path, toml::to_string(&config)?)?;
    Ok(())
}
async fn native_value(c: &mut NativeClient, name: &str, sql: &str) -> ServeResult<String> {
    Ok(native_text(&c.execute_sql(native_sql(name, sql)).await?.into_inner(), 0)?.to_owned())
}
// Preserve the native status for SQLSTATE assertions in this proof helper.
#[allow(clippy::result_large_err)]
async fn native_dml(
    c: &mut NativeClient,
    name: &str,
    sql: &str,
) -> Result<NativeResultSet, NativeStatus> {
    use googleapis_tonic_google_spanner_v1::google::spanner::v1::{
        TransactionOptions, TransactionSelector, transaction_options, transaction_selector,
    };
    let mut request = native_sql(name, sql);
    request.transaction = Some(TransactionSelector {
        selector: Some(transaction_selector::Selector::Begin(TransactionOptions {
            mode: Some(transaction_options::Mode::ReadWrite(Default::default())),
            ..Default::default()
        })),
    });
    request.seqno = 1;
    let result = c.execute_sql(request).await?.into_inner();
    // Roll back the proof write, leaving fixture rows available for subsequent checks.
    if let Some(id) = result
        .metadata
        .as_ref()
        .and_then(|m| m.transaction.as_ref())
        .map(|t| t.id.clone())
    {
        c.rollback(
            googleapis_tonic_google_spanner_v1::google::spanner::v1::RollbackRequest {
                session: name.into(),
                transaction_id: id,
            },
        )
        .await?;
    }
    Ok(result)
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires NUNYA_TEST_SPANNER_EMULATOR, NUNYA_TEST_SPANNER_REST, NUNYA_TEST_SPANNER_DB and NUNYA_TEST_LINK_PG"]
async fn spanner_shared_domains() -> ServeResult<()> {
    let database = std::env::var("NUNYA_TEST_SPANNER_DB")?;
    let endpoint = format!("http://{}", std::env::var("NUNYA_TEST_SPANNER_EMULATOR")?);
    let pg_url = std::env::var("NUNYA_TEST_LINK_PG")?;
    let (admin, connection) =
        tokio_postgres::connect(&std::env::var("NUNYA_TEST_LINK_PG_ADMIN")?, NoTls).await?;
    let _admin_driver = Driver(tokio::spawn(async move {
        let _ = connection.await;
    }));
    for mode in ["tokenize", "show"] {
        let schema = format!(
            "links_{}",
            &nunya::token::SessionId::random().to_hex()[..12]
        );
        let fixture = native_fixture(&schema).await?;
        native_ddl(&std::env::var("NUNYA_TEST_SPANNER_REST")?, &database, vec![
            format!("CREATE TABLE {schema}.stream_rows(id INT64 NOT NULL, payload STRING(MAX)) PRIMARY KEY(id)")
        ]).await?;
        native_update(&std::env::var("NUNYA_TEST_SPANNER_REST")?, &database,
            &format!("INSERT INTO {schema}.stream_rows(id,payload) SELECT n, REPEAT('x',65536) FROM UNNEST(GENERATE_ARRAY(1,512)) AS n")).await?;
        let native_policy = |linked| format!("{}\n[[column]]\ntable = '{schema}.stream_rows'\nname = 'payload'\nshow = true\n", policy(&schema, mode, linked));
        admin.batch_execute(&format!("CREATE SCHEMA {schema}; CREATE TABLE {schema}.customers(id int primary key,email text); CREATE TABLE {schema}.outbox(id int primary key,to_address text); INSERT INTO {schema}.customers VALUES(1,'first@example.test'); INSERT INTO {schema}.outbox VALUES(2,'first@example.test');")).await?;
        let mut h = ServeHarness::new(
            &native_policy(false),
            Some(&pg_url),
            None,
            Some(NativeUpstream {
                database: &database,
                emulator_endpoint: &endpoint,
            }),
            "",
        )?;
        separate_policy(&h, "upstream", &policy(&schema, mode, false))?;
        let path = h.dir().join("nunya.toml");
        let mut config: toml::Table = toml::from_str(&std::fs::read_to_string(&path)?)?;
        config.insert("schemas".into(), vec![schema.as_str()].into());
        config.insert("search_path".into(), vec![schema.as_str()].into());
        config.insert(
            "readlog".into(),
            toml::Value::Table(toml::Table::from_iter([
                ("sink".into(), "file".into()),
                ("path".into(), "read.log".into()),
            ])),
        );
        config.insert("snapshot_refresh_ms".into(), 600000.into());
        config.insert("snapshot_max_age_ms".into(), 600000.into());
        config["spanner_native"]
            .as_table_mut()
            .ok_or("missing native config")?
            .insert("snapshot_refresh_ms".into(), 600000.into());
        std::fs::write(path, toml::to_string(&config)?)?;
        h.start().await?;
        let (other, secret, _) = h
            .acquire_session("other-pg-agent", "postgres", Mode::ReadOnly)
            .await?;
        let (other, task) = h.pg_connect(&other, &secret, "app").await?;
        let _other_driver = Driver(task);
        let other_old = token(&other, "SELECT email FROM customers WHERE id=1").await?;
        let (mut c, name) = native_session(&h, &database, "native-link-agent").await?;
        let (mut streaming, stream_name) =
            native_session(&h, &database, "native-stream-agent").await?;
        let select = format!("SELECT email FROM {schema}.customers WHERE id=1");
        let join = format!(
            "SELECT c.id FROM {schema}.customers c JOIN {schema}.outbox o ON c.email=o.to_address"
        );
        let write = format!(
            "INSERT INTO {schema}.outbox(id,to_address) SELECT id,email FROM {schema}.customers"
        );
        let old = native_value(&mut c, &name, &select).await?;
        let mut checks = Checks::default();
        checks.pass(
            "native-before-distinct",
            old != native_value(
                &mut c,
                &name,
                &format!("SELECT to_address FROM {schema}.outbox WHERE id=2"),
            )
            .await?,
        );
        native_refused(
            c.execute_sql(native_sql(&name, &join)).await,
            "native-before-join",
            &mut checks,
        )?;
        native_refused(
            native_dml(&mut c, &name, &write).await,
            "native-before-write",
            &mut checks,
        )?;
        let mut read = native_read(&stream_name, &format!("{schema}.stream_rows"), &["payload"]);
        read.key_set = Some(KeySet { all: true, ..Default::default() });
        let mut in_flight = streaming.streaming_read(read).await?.into_inner();
        let first = in_flight.message().await?.ok_or("stream ended before reload")?;
        let initial_rows = first.values.len() - usize::from(first.chunked_value);
        checks.pass("native-stream-open-before-reload", !first.values.is_empty());
        let audit_start = std::fs::read_to_string(h.dir().join("read.log"))?
            .lines()
            .count();
        reload(&h, &native_policy(true)).await?;
        audit(&h, audit_start, "spanner-native", &mut checks.0)?;
        let reset = c.execute_sql(native_sql(&name, &select)).await;
        match &reset {
            Ok(response) => println!(
                "RPC reset-ExecuteSql accepted rows={}",
                response.get_ref().rows.len()
            ),
            Err(error) => {
                let code = error
                    .message()
                    .as_bytes()
                    .windows(5)
                    .find(|b| b[..2] == *b"NY" && b[2..].iter().all(u8::is_ascii_digit))
                    .and_then(|b| std::str::from_utf8(b).ok())
                    .unwrap_or("none");
                println!("RPC reset-ExecuteSql grpc={} NY={code}", error.code());
            }
        }
        checks.pass(
            "native-reset-ExecuteSql-NY003-or-NY011",
            reset
                .as_ref()
                .err()
                .is_some_and(|e| e.message().contains("NY011") || e.message().contains("NY003")),
        );
        let reset = streaming
            .streaming_read(native_read(
                &stream_name,
                &format!("{schema}.customers"),
                &["email"],
            ))
            .await;
        let status = match reset {
            Err(e) => Some(e),
            Ok(response) => response.into_inner().message().await.err(),
        };
        checks.pass(
            "native-reset-StreamingRead-NY003-or-NY011",
            status.as_ref().is_some_and(|e| e.message().contains("NY011") || e.message().contains("NY003")),
        );
        if let Some(error) = &status {
            println!("CODE native-reset-StreamingRead {}", if error.message().contains("NY003") { "NY003" } else { "NY011" });
        }
        let mut buffered_rows = initial_rows;
        let stream_error = tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                match in_flight.message().await {
                    Err(error) => break Ok(error),
                    Ok(Some(part)) => {
                        // One string column: a chunked final value continues into the next response.
                        buffered_rows += part.values.len() - usize::from(part.chunked_value);
                    },
                    Ok(None) => break Err("in-flight stream ended without reset error"),
                }
            }
        }).await??;
        println!("CODE native-in-flight-stream {}", if stream_error.message().contains("NY011") { "NY011" } else if stream_error.message().contains("NY003") { "NY003" } else { "other" });
        checks.pass("native-in-flight-stream-reset", stream_error.message().contains("NY011") || stream_error.message().contains("NY003"));
        println!("COUNT native-in-flight-buffered-rows {buffered_rows}");
        checks.pass("native-in-flight-buffered-rows-within-fixture", buffered_rows <= 512);
        checks.pass(
            "native-unrelated-route-stable",
            other_old == token(&other, "SELECT email FROM customers WHERE id=1").await?,
        );
        let (mut n, name2) = native_session(&h, &database, "native-linked-agent").await?;
        let linked = native_value(&mut n, &name2, &select).await?;
        checks.pass(
            "native-linked-equal",
            linked
                == native_value(
                    &mut n,
                    &name2,
                    &format!("SELECT to_address FROM {schema}.outbox WHERE id=2"),
                )
                .await?,
        );
        let joined = n.execute_sql(native_sql(&name2, &join)).await?.into_inner();
        checks.pass("native-linked-join", !joined.rows.is_empty());
        let written = native_dml(&mut n, &name2, &write).await?;
        checks.pass("native-linked-write", written.stats.is_some());
        let mut request = native_sql(
            &name2,
            &format!("SELECT id FROM {schema}.customers WHERE email=@old"),
        );
        request.params = Some(prost_types::Struct {
            fields: [(
                "old".into(),
                prost_types::Value {
                    kind: Some(prost_types::value::Kind::StringValue(old)),
                },
            )]
            .into_iter()
            .collect(),
        });
        request.param_types.insert(
            "old".into(),
            googleapis_tonic_google_spanner_v1::google::spanner::v1::Type {
                code: googleapis_tonic_google_spanner_v1::google::spanner::v1::TypeCode::String
                    as i32,
                ..Default::default()
            },
        );
        native_refused(
            n.execute_sql(request).await,
            "native-old-parameter",
            &mut checks,
        )?;
        let audit_start = std::fs::read_to_string(h.dir().join("read.log"))?
            .lines()
            .count();
        reload(&h, &native_policy(false)).await?;
        audit(&h, audit_start, "spanner-native", &mut checks.0)?;
        let reset = n.execute_sql(native_sql(&name2, &select)).await;
        checks.pass(
            "native-unlink-reset-NY003-or-NY011",
            reset
                .as_ref()
                .err()
                .is_some_and(|e| e.message().contains("NY011") || e.message().contains("NY003")),
        );
        let (mut u, name3) = native_session(&h, &database, "native-unlinked-agent").await?;
        native_refused(
            u.execute_sql(native_sql(&name3, &join)).await,
            "native-unlink-join",
            &mut checks,
        )?;
        native_refused(
            native_dml(&mut u, &name3, &write).await,
            "native-unlink-write",
            &mut checks,
        )?;
        native_refused(
            u.execute_sql(native_sql(
                &name3,
                &format!("SELECT id FROM {schema}.customers WHERE email='{linked}'"),
            ))
            .await,
            "native-linked-token-retired",
            &mut checks,
        )?;
        checks.pass(
            "native-unlink-unrelated-stable",
            other_old == token(&other, "SELECT email FROM customers WHERE id=1").await?,
        );
        println!("SUMMARY spanner-{mode} passed={} failed=0", checks.0);
        h.stop()?;
        native_ddl(&std::env::var("NUNYA_TEST_SPANNER_REST")?, &database,
            vec![format!("DROP TABLE {schema}.stream_rows")]).await?;
        drop(fixture);
        admin
            .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
            .await?;
    }
    Ok(())
}
