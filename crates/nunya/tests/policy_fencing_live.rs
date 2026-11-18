//! Opt-in live revision fencing checks; run live-5b/run.sh.
use super::common::{ServeHarness, ServeResult};
use nunya::{mgmt::client::{MgmtClient, ClientError}, state::model::Mode};
use std::{io::{BufRead, BufReader, Write}, process::{Command, Stdio}, time::Duration};
use tokio_postgres::NoTls;

struct Driver(tokio::task::JoinHandle<()>);
impl Drop for Driver { fn drop(&mut self) { self.0.abort(); } }
fn policy(table: &str, masked: bool) -> String {
    let treatment = if masked { "masked = true\nplaceholder = 'MASKED'" } else { "show = true" };
    format!("version = 2\ndefault = 'show'\n[[column]]\ntable = '{table}'\nname = 'secret'\n{treatment}\n")
}
fn pass(label: &str, ok: bool) -> ServeResult<()> {
    println!("{} {label}", if ok { "PASS" } else { "FAIL" });
    if ok { Ok(()) } else { Err(format!("fencing assertion failed: {label}").into()) }
}
fn configure(h: &ServeHarness, schema: &str) -> ServeResult<()> {
    let path = h.dir().join("nunya.toml");
    let mut config: toml::Table = toml::from_str(&std::fs::read_to_string(&path)?)?;
    config.insert("schemas".into(), vec![schema].into());
    config.insert("search_path".into(), vec![schema].into());
    config.insert("snapshot_refresh_ms".into(), 600000.into());
    config.insert("snapshot_max_age_ms".into(), 600000.into());
    config.insert("readlog".into(), toml::Value::Table(toml::Table::from_iter([("sink".into(), "file".into()), ("path".into(), "read.log".into())])));
    std::fs::write(path, toml::to_string(&config)?)?;
    Ok(())
}
async fn active(admin: &MgmtClient, route: &str) -> ServeResult<()> {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let s = admin.policy_status(route).await?;
            if s.activation.is_some_and(|a| a.state == "active" && a.pending_statements == 0) { break; }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        pass("status-active-zero-pending", true)
    }).await?
}
async fn revise(admin: &MgmtClient, route: &str, table: &str, scenario: &str, pending: bool) -> ServeResult<String> {
    let mut activation = "unchanged".to_owned();
    let fingerprint = admin.policy_status(route).await?.catalog_fingerprint;
    if scenario == "rejected" {
        let invalid = admin.policy_apply(route, "not valid [", 1, &fingerprint, None).await;
        pass("invalid-draft-422", matches!(invalid, Err(ClientError::Api { status: 422, .. })))?;
        let s = admin.policy_status(route).await?;
        pass("invalid-status-preserves-active", s.active_revision.is_some_and(|r| r.number == 1) && s.last_rejected.is_some_and(|r| r.code == "invalid_policy"))?;
        let stale = admin.policy_apply(route, &policy(table, true), 0, &fingerprint, None).await;
        pass("stale-draft-409", matches!(stale, Err(ClientError::Stale { .. })))?;
        let s = admin.policy_status(route).await?;
        pass("stale-status-preserves-active", s.active_revision.is_some_and(|r| r.number == 1) && s.last_rejected.is_some_and(|r| r.code == "stale_revision"))?;
        active(admin, route).await?;
    } else {
        // Let the deliberately unread psycopg stream fill the gateway's socket.
        if pending { tokio::time::sleep(Duration::from_secs(2)).await; }
        let applied = admin.policy_apply(route, &policy(table, scenario == "tighten"), 1, &fingerprint, None).await?.0;
        println!("ACTIVATION {route} {scenario} {} pending_statements={}", applied.activation.as_deref().unwrap_or("missing"), applied.pending_statements.unwrap_or(0));
        activation = applied.activation.clone().ok_or("missing apply activation")?;
        pass("apply-activation-present", matches!(activation.as_str(), "active" | "pending"))?;
        if pending && scenario == "tighten" {
            pass("apply-pending", applied.activation.as_deref() == Some("pending") && applied.pending_statements.is_some_and(|n| n >= 1))?;
            let s = admin.policy_status(route).await?;
            pass("status-pending", s.activation.is_some_and(|a| a.state == "pending" && a.pending_statements >= 1))?;
        }
    }
    Ok(activation)
}
async fn portal_case(h: &ServeHarness, table: &str, scenario: &str) -> ServeResult<()> {
    let admin = h.admin();
    let (session, secret, _) = h.acquire_session("portal-fencing", "postgres", Mode::ReadOnly).await?;
    let (mut client, driver) = h.pg_connect(&session, &secret, "app").await?;
    let _driver = Driver(driver);
    let tx = client.transaction().await?;
    let statement = tx.prepare(&format!("SELECT id, secret FROM {table} ORDER BY id")).await?;
    let portal = tx.bind(&statement, &[]).await?;
    for _ in 0..2 { pass("portal-before-apply-ten-rows", tx.query_portal(&portal, 10).await?.len() == 10)?; }
    revise(admin, "postgres", table, scenario, false).await?;
    if scenario == "tighten" {
        let result = tx.query_portal(&portal, 10).await;
        pass("portal-next-fetch-NY008", result.as_ref().err().and_then(|e| e.code()).is_some_and(|c| c.code() == "NY008"))?;
    } else {
        let mut count = 20;
        loop {
            let rows = tx.query_portal(&portal, 10).await?;
            for row in &rows {
                if scenario == "loosen" && row.try_get::<_, String>(1)? != "MASKED" { return Err("portal did not retain old mask".into()); }
            }
            count += rows.len();
            if rows.len() < 10 { break; }
        }
        pass("portal-finished-1000", count == 1000)?;
    }
    active(admin, "postgres").await
}
struct Child(std::process::Child);
impl Drop for Child { fn drop(&mut self) { let _ = self.0.kill(); let _ = self.0.wait(); } }
async fn external_case(h: &ServeHarness, table: &str, scenario: &str, java: bool) -> ServeResult<()> {
    let (session, secret, _) = h.acquire_session("external-fencing", "postgres", Mode::ReadOnly).await?;
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut command = Command::new(std::env::var(if java { "NUNYA_TEST_FENCING_JAVA" } else { "NUNYA_TEST_FENCING_PYTHON" })?);
    if java {
        command.arg("--class-path").arg(std::env::var("NUNYA_TEST_FENCING_JAVA_CLASSPATH")?).arg(root.join("harness/drivers/FencingCheck.java"));
    } else { command.arg(root.join("harness/drivers/fencing_check.py")); }
    command.env("PGHOST", "127.0.0.1").env("PGPORT", h.pg_port().to_string()).env("PGDATABASE", "app")
        .env("PGUSER", format!("session:{}", session.id)).env("PGPASSWORD", &*secret.secret).env("PGSSLMODE", "disable")
        .env("FENCING_SCENARIO", scenario).env("FENCING_TABLE", table)
        .stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::null());
    let mut child = Child(command.spawn()?);
    let stdout = child.0.stdout.take().ok_or("missing driver stdout")?;
    let (mut child, mut reader) = tokio::task::spawn_blocking(move || -> ServeResult<_> {
        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        reader.read_line(&mut line)?;
        if line.trim() != "READY" { print!("{line}"); return Err("external driver did not reach handshake".into()); }
        Ok((child, reader))
    }).await??;
    let activation = revise(h.admin(), "postgres", table, scenario, !java).await?;
    writeln!(child.0.stdin.as_mut().ok_or("missing driver stdin")?, "CONTINUE {activation}")?;
    tokio::task::spawn_blocking(move || -> ServeResult<()> {
        let mut line = String::new();
        while reader.read_line(&mut line)? > 0 { print!("{line}"); line.clear(); }
        pass("external-driver-success", child.0.wait()?.success())
    }).await??;
    active(h.admin(), "postgres").await
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires local PostgreSQL and external drivers"]
async fn postgres_fencing() -> ServeResult<()> {
    let url = std::env::var("NUNYA_TEST_POLICY_PG")?;
    let (setup, driver) = tokio_postgres::connect(&url, NoTls).await?;
    let _driver = Driver(tokio::spawn(async move { let _ = driver.await; }));
    for driver in ["tokio", "jdbc", "psycopg"] {
        for scenario in ["tighten", "loosen", "rejected"] {
            let schema = format!("s5b_{}", &nunya::token::SessionId::random().to_hex()[..12]);
            let table = format!("{schema}.items");
            let n = if driver == "psycopg" { 50000 } else { 1000 };
            setup.batch_execute(&format!("CREATE SCHEMA {schema}; CREATE TABLE {table}(id int PRIMARY KEY, secret text); INSERT INTO {table} SELECT n, repeat('fencing-fixture-', 140) FROM generate_series(1,{n}) n")).await?;
            println!("CASE {driver} {scenario}");
            let result = tokio::time::timeout(Duration::from_secs(180), async {
                let mut h = ServeHarness::new(&policy(&table, scenario == "loosen"), Some(&url), None, None, "")?;
                configure(&h, &schema)?;
                h.start().await?;
                h.admin().policy_import("postgres", None).await?;
                if driver == "tokio" { portal_case(&h, &table, scenario).await } else { external_case(&h, &table, scenario, driver == "jdbc").await }
            }).await;
            setup.batch_execute(&format!("DROP SCHEMA {schema} CASCADE")).await?;
            result??;
        }
    }
    if setup.query_one("SHOW server_version_num", &[]).await?.get::<_, String>(0).starts_with("18") {
        let schema = format!("s5b_{}", &nunya::token::SessionId::random().to_hex()[..12]);
        let table = format!("{schema}.items");
        setup.batch_execute(&format!("CREATE SCHEMA {schema}; CREATE TABLE {table}(id int PRIMARY KEY, secret text); INSERT INTO {table} SELECT n, 'fencing-fixture' FROM generate_series(1,100000) n")).await?;
        let result = async {
            let mut h = ServeHarness::new(&policy(&table, false), Some(&url), None, None, "")?;
            configure(&h, &schema)?;
            h.start().await?;
            let (session, secret, _) = h.acquire_session("fencing-perf", "postgres", Mode::ReadOnly).await?;
            let (client, driver) = h.pg_connect(&session, &secret, "app").await?;
            let _driver = Driver(driver);
            let start = std::time::Instant::now();
            let result = client.simple_query(&format!("SELECT id, secret FROM {table}")).await?;
            let elapsed = start.elapsed();
            let rows = result.iter().filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_))).count();
            println!("PERF PG18 simple-query rows={rows} elapsed_ms={:.3}", elapsed.as_secs_f64() * 1000.0);
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        }.await;
        setup.batch_execute(&format!("DROP SCHEMA {schema} CASCADE")).await?;
        result?;
    }
    Ok(())
}

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


use super::common::NativeUpstream;
use nunya::mgmt::client::{SessionView, SessionSecret};
use googleapis_tonic_google_spanner_v1::google::spanner::v1::{
    spanner_client::SpannerClient, CreateSessionRequest, DeleteSessionRequest,
    ExecuteSqlRequest, ReadRequest, KeySet,
};
use tonic::{Request as NativeRequest, Status as NativeStatus, transport::Endpoint as NativeEndpoint};

async fn native_case(h: &ServeHarness, database: &str, table: &str, scenario: &str, read: bool) -> ServeResult<()> {
    let (access, secret, _) = h.acquire_session("native-fencing", "spanner-native", Mode::ReadOnly).await?;
    let mut client = native_client(h, &access, &secret).await?;
    let name = client.create_session(CreateSessionRequest { database: database.into(), ..Default::default() }).await?.into_inner().name;
    let result = async {
        let mut stream = if read {
            client.streaming_read(ReadRequest { session: name.clone(), table: table.into(), columns: vec!["id".into(), "secret".into()], key_set: Some(KeySet { all: true, ..Default::default() }), ..Default::default() }).await?.into_inner()
        } else {
            client.execute_streaming_sql(native_sql(&name, &format!("SELECT id, secret FROM {table} ORDER BY id"))).await?.into_inner()
        };
        // Fixture ids are integer strings; secret strings/fragments are never numeric.
        let row_count = |part: &googleapis_tonic_google_spanner_v1::google::spanner::v1::PartialResultSet| {
            part.values.iter().filter(|v| matches!(&v.kind,
                Some(prost_types::value::Kind::StringValue(s)) if s.parse::<i64>().is_ok())).count()
        };
        let mut before_rows = 0;
        for _ in 0..2 {
            let part = stream.message().await?.ok_or("native stream ended before apply")?;
            before_rows += row_count(&part);
            pass("native-two-initial-parts", true)?;
        }
        let activation = revise(h.admin(), "spanner-native", table, scenario, false).await?;
        if scenario == "tighten" {
            let mut after_parts = 0;
            let mut after_rows = 0;
            loop {
                match stream.message().await {
                    Err(error) => {
                        pass("native-stream-NY008", error.message().contains("NY008"))?;
                        break;
                    }
                    Ok(Some(part)) => {
                        after_parts += 1;
                        after_rows += row_count(&part);
                        if activation == "active" {
                            return Err("native active apply released a later part".into());
                        }
                    }
                    Ok(None) => {
                        println!("AFTER_APPLY native exhausted parts={after_parts} rows={after_rows} remaining={}", 20000 - before_rows);
                        return Err("native tightened stream exhausted without NY008".into());
                    }
                }
            }
            println!("AFTER_APPLY native parts={after_parts} rows={after_rows} remaining={}", 20000 - before_rows);
            pass("native-refused-far-before-exhaustion", after_rows < (20000 - before_rows) / 10)?;
            pass("native-no-part-after-error", stream.message().await?.is_none())?;
        } else {
            let mut parts = 2;
            while let Some(part) = stream.message().await? {
                if scenario == "loosen" {
                    // Chunk fragments can split string cells; no fragment may expose fixture text.
                    pass("native-old-mask-no-plaintext", !part.values.iter().any(|v| matches!(&v.kind, Some(prost_types::value::Kind::StringValue(s)) if s.contains("fencing-fixture"))))?;
                }
                parts += 1;
            }
            pass("native-stream-finished", parts > 2)?;
        }
        active(h.admin(), "spanner-native").await
    }.await;
    client.delete_session(DeleteSessionRequest { name }).await?;
    result
}
#[tokio::test]
#[ignore = "requires native Spanner emulator"]
async fn spanner_fencing() -> ServeResult<()> {
    let endpoint = std::env::var("NUNYA_TEST_SPANNER_EMULATOR")?;
    let rest = std::env::var("NUNYA_TEST_SPANNER_REST")?;
    let database = std::env::var("NUNYA_TEST_SPANNER_DB")?;
    for read in [false, true] {
        for scenario in ["tighten", "loosen", "rejected"] {
            let schema = format!("s5b_{}", &nunya::token::SessionId::random().to_hex()[..12]);
            let table = format!("{schema}.items");
            native_ddl(&rest, &database, vec![format!("CREATE SCHEMA {schema}"), format!("CREATE TABLE {table}(id INT64 NOT NULL, secret STRING(MAX)) PRIMARY KEY(id)")]).await?;
            // 20k rows (~42 MB): 2k rows can fit entirely in the already released parts.
            // Keep enough independent parts unread at apply, well below 1 GB.
            let value = "fencing-fixture-".repeat(140);
            for batch in 0..200 {
                let values = (batch*100..(batch+1)*100).map(|n| format!("({n}, '{value}')")).collect::<Vec<_>>().join(",");
                native_update(&rest, &database, &format!("INSERT INTO {table}(id,secret) VALUES {values}")).await?;
            }
            println!("CASE native-{} {scenario}", if read { "read" } else { "sql" });
            let result = tokio::time::timeout(Duration::from_secs(180), async {
                let mut h = ServeHarness::new(&policy(&table, scenario == "loosen"), None, None, Some(NativeUpstream { database: &database, emulator_endpoint: &endpoint }), "")?;
                configure(&h, &schema)?;
                h.start().await?;
                h.admin().policy_import("spanner-native", None).await?;
                native_case(&h, &database, &table, scenario, read).await
            }).await;
            native_ddl(&rest, &database, vec![format!("DROP TABLE {table}"), format!("DROP SCHEMA {schema}")]).await?;
            result??;
        }
    }
    Ok(())
}
