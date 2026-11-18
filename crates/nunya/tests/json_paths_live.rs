//! Opt-in JSON path acceptance checks against live databases.
use super::common::{NativeUpstream, ServeHarness, ServeResult};
use nunya::{
    mgmt::client::{SessionSecret, SessionView},
    state::model::Mode,
};

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

fn policy(schema: &str, mode: &str, columns: &[&str]) -> String {
    let mut text = format!(
        "version = 2\ndefault = '{mode}'\n[domains.email]\ntokenize = 'on'\ntokenize_limit_per_session = 1000\ntokenize_limit_per_principal = 10000\ntokenize_principal_window = '24h'\n[[column]]\ntable = '{schema}.events'\nname = 'id'\nshow = true\n"
    );
    for column in columns {
        let prefix = format!("[[json_path]]\ntable = '{schema}.events'\nname = '{column}'\n");
        if mode == "tokenize" {
            text.push_str(&format!("{prefix}path = '$.status'\nshow = true\ntype = 'string'\n{prefix}path = '$.recipients[*].email'\nblind = 'email'\ntype = 'text'\n"));
        } else {
            text.push_str(&format!("{prefix}path = '$.payment.card'\nmasked = true\n"));
        }
    }
    text
}

const ORIGINAL: &str = r#"{"status":"sent","recipients":[{"email":"first@example.test","phone":"fixture-phone"},{"email":"second@example.test","phone":"fixture-phone"}],"processor_response":"fixture-processor"}"#;
const CHANGED: &str = r#"{"status":{"unapproved":"fixture-hidden"},"recipients":[{"email":"first@example.test","phone":"fixture-phone"},7,null,"fixture-hidden",{}],"new_top":"fixture-top","processor_response":"fixture-processor"}"#;
const PAYMENT: &str =
    r#"{"event_type":"payment","payment":{"card":{"number":"fixture-number"},"amount":42}}"#;
const PAYMENT_CHANGED: &str = r#"{"event_type":"payment","payment":{"card":{"number":"fixture-number","cvv":"fixture-cvv"},"amount":42},"new_top":"fixture-top"}"#;

fn json(text: &str) -> ServeResult<serde_json::Value> {
    Ok(serde_json::from_str(text)?)
}
fn filtered(v: &serde_json::Value, changed: bool) -> bool {
    let text = v.to_string();
    let safe = ![
        "phone",
        "processor_response",
        "fixture-hidden",
        "new_top",
        "example.test",
    ]
    .iter()
    .any(|s| text.contains(s));
    let Some(recipients) = v["recipients"].as_array() else {
        return false;
    };
    let tokens = recipients.iter().filter_map(|r| r.get("email")).all(|e| {
        e.as_str()
            .is_some_and(|s| s.starts_with("tok_email_") && s.len() > 10)
    });
    safe && tokens
        && if changed {
            v.get("status").is_none()
                && recipients.len() == 5
                && recipients[1..4].iter().all(serde_json::Value::is_null)
                && recipients[4].get("email").is_none()
        } else {
            v["status"] == "sent" && recipients.len() == 2
        }
}
fn shown(v: &serde_json::Value, changed: bool) -> bool {
    v["event_type"] == "payment"
        && v["payment"]["amount"] == 42
        && !v.to_string().contains("card")
        && (!changed || v["new_top"] == "fixture-top")
}

struct RawJson(Option<String>);
impl<'a> tokio_postgres::types::FromSql<'a> for RawJson {
    fn from_sql(
        ty: &tokio_postgres::types::Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        let raw = if *ty == tokio_postgres::types::Type::JSONB {
            if raw.first() != Some(&1) {
                return Err("invalid JSONB binary version".into());
            }
            &raw[1..]
        } else {
            raw
        };
        Ok(Self(Some(std::str::from_utf8(raw)?.into())))
    }
    fn from_sql_null(
        _: &tokio_postgres::types::Type,
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(Self(None))
    }
    fn accepts(ty: &tokio_postgres::types::Type) -> bool {
        *ty == tokio_postgres::types::Type::JSON || *ty == tokio_postgres::types::Type::JSONB
    }
}

async fn update(admin: &Client, schema: &str, value: &str) -> ServeResult<()> {
    admin
        .batch_execute(&format!(
            "UPDATE {schema}.events SET doc_json = '{value}', doc_jsonb = '{value}' WHERE id = 1"
        ))
        .await?;
    Ok(())
}

async fn refused(client: &Client, sql: &str, checks: &mut Checks) -> ServeResult<()> {
    match client.simple_query(sql).await {
        Err(error) => {
            let code = error.code().ok_or("missing refusal SQLSTATE")?.code();
            let message = error.as_db_error().map(|e| e.message()).unwrap_or("");
            checks.pass(
                "refusal-no-data",
                code.starts_with("NY")
                    && ![
                        "example.test",
                        "fixture-phone",
                        "fixture-number",
                        "fixture-hidden",
                        "fixture-processor",
                    ]
                    .iter()
                    .any(|s| message.contains(s)),
            );
            println!("CODE {sql}: {code}");
        }
        Ok(_) => {
            return Err(format!(
                "PRODUCT BUG: accepted SQL: {sql}; expected NY refusal before execution"
            )
            .into());
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires NUNYA_TEST_JSON_PG and NUNYA_TEST_JSON_PG_ADMIN; optional JSON driver env"]
async fn postgres_json_paths() -> ServeResult<()> {
    let url = std::env::var("NUNYA_TEST_JSON_PG")?;
    let admin_url = std::env::var("NUNYA_TEST_JSON_PG_ADMIN")?;
    for mode in ["tokenize", "show"] {
        let schema = format!("json_{}", &nunya::token::SessionId::random().to_hex()[..12]);
        let _fixture = PgFixture {
            url: admin_url.clone(),
            schema: schema.clone(),
        };
        let (admin, connection) = tokio_postgres::connect(&admin_url, NoTls).await?;
        let _admin_driver = Driver(tokio::spawn(async move {
            let _ = connection.await;
        }));
        admin.batch_execute(&format!("CREATE SCHEMA {schema}; CREATE TABLE {schema}.events(id int PRIMARY KEY, doc_json json, doc_jsonb jsonb); CREATE TABLE {schema}.copies(doc_json json); INSERT INTO {schema}.events VALUES (1, NULL, NULL), (2, NULL, NULL), (3, NULL, NULL); GRANT USAGE ON SCHEMA {schema} TO PUBLIC; GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA {schema} TO PUBLIC")).await?;
        let original = if mode == "tokenize" {
            ORIGINAL
        } else {
            PAYMENT
        };
        let changed = if mode == "tokenize" {
            CHANGED
        } else {
            PAYMENT_CHANGED
        };
        update(&admin, &schema, original).await?;
        admin.batch_execute(&format!("UPDATE {schema}.events SET doc_json = '{original}', doc_jsonb = '{original}' WHERE id > 1")).await?;
        let mut h = ServeHarness::new(
            &policy(&schema, mode, &["doc_json", "doc_jsonb"]),
            Some(&url),
            None,
            None,
            "",
        )?;
        configure(&h, Some(&schema), 600000)?;
        h.start().await?;
        let (access, secret, _) = h
            .acquire_session("json-agent", "postgres", Mode::ReadWrite)
            .await?;
        let (mut client, task) = h.pg_connect(&access, &secret, "app").await?;
        let driver = Driver(task);
        let mut checks = Checks::default();
        let sql = "SELECT doc_json, doc_jsonb FROM events WHERE id = 1";
        let initial = rows(&client, sql).await?;
        let initial_json = json(cell(&initial, 0)?)?;
        for col in 0..2 {
            let value = json(cell(&initial, col)?)?;
            checks.pass(
                "simple-filter",
                if mode == "tokenize" {
                    filtered(&value, false)
                } else {
                    shown(&value, false)
                },
            );
            checks.pass("json-jsonb-equivalent", value == initial_json);
        }
        let statement = client
            .prepare("SELECT doc_json, doc_jsonb FROM events WHERE id = $1")
            .await?;
        let binary = client.query(&statement, &[&1i32]).await?;
        for col in 0..2 {
            let value: RawJson = binary[0].try_get(col)?;
            checks.pass(
                "extended-binary",
                json(value.0.as_deref().ok_or("unexpected binary SQL NULL")?)? == initial_json,
            );
        }
        let tx = client.transaction().await?;
        let portal_statement = tx
            .prepare("SELECT doc_json, doc_jsonb FROM events ORDER BY id")
            .await?;
        let portal = tx.bind(&portal_statement, &[]).await?;
        let mut count = 0;
        loop {
            let result = tx.query_portal(&portal, 1).await?;
            if result.is_empty() {
                break;
            }
            count += result.len();
            for row in result {
                for col in 0..2 {
                    let value: RawJson = row.try_get(col)?;
                    checks.pass(
                        "suspended-portal-filter",
                        json(value.0.as_deref().ok_or("portal NULL")?)? == initial_json,
                    );
                }
            }
        }
        checks.pass("portal-three-rows", count == 3);
        tx.commit().await?;
        update(&admin, &schema, changed).await?;
        let result = rows(&client, sql).await?;
        for col in 0..2 {
            let value = json(cell(&result, col)?)?;
            checks.pass(
                "shape-change-no-refresh",
                if mode == "tokenize" {
                    filtered(&value, true)
                } else {
                    shown(&value, true)
                },
            );
        }
        if mode == "tokenize" {
            update(&admin, &schema, r#"{"status":null,"recipients":[{}]}"#).await?;
            let result = rows(&client, sql).await?;
            checks.pass(
                "json-null-leaf",
                json(cell(&result, 0)?)?["status"].is_null()
                    && json(cell(&result, 0)?)?.get("status").is_some(),
            );
        }
        for value in ["null", "SQL NULL"] {
            if value == "SQL NULL" {
                admin
                    .batch_execute(&format!(
                        "UPDATE {schema}.events SET doc_json=NULL, doc_jsonb=NULL WHERE id=1"
                    ))
                    .await?;
            } else {
                update(&admin, &schema, value).await?;
            }
            let result = rows(&client, sql).await?;
            println!(
                "OBSERVATION {mode} {value}: json={} jsonb={}",
                if result[0][0].is_none() {
                    "SQL-NULL"
                } else if result[0][0].as_deref() == Some("null") {
                    "JSON-null"
                } else {
                    "document"
                },
                if result[0][1].is_none() {
                    "SQL-NULL"
                } else if result[0][1].as_deref() == Some("null") {
                    "JSON-null"
                } else {
                    "document"
                }
            );
        }
        update(&admin, &schema, original).await?;
        drop(client);
        drop(driver);
        let (again, task) = h.pg_connect(&access, &secret, "app").await?;
        let _again_driver = Driver(task);
        checks.pass(
            "reconnect-stable",
            json(cell(&rows(&again, sql).await?, 0)?)? == initial_json,
        );
        if mode == "tokenize" {
            tokio::task::block_in_place(|| run_drivers(&h, &access, &secret, &schema, "0"))?;
            for refused_sql in [
                "SELECT id FROM events WHERE doc_jsonb->>'status' = 'x'",
                "SELECT doc_jsonb::text FROM events",
                "SELECT jsonb_pretty(doc_jsonb) FROM events",
                "UPDATE events SET doc_jsonb = '{}'",
                "INSERT INTO copies(doc_json) SELECT doc_jsonb FROM events",
                "UPDATE events SET id = id RETURNING doc_jsonb",
            ] {
                refused(&again, refused_sql, &mut checks).await?;
            }
            match again
                .simple_query("SELECT id FROM events WHERE doc_jsonb IS NULL")
                .await
            {
                Ok(_) => println!("OBSERVATION IS NULL accepted"),
                Err(e) => println!(
                    "OBSERVATION IS NULL {}",
                    e.code().map(|c| c.code()).unwrap_or("no-code")
                ),
            }
        }
        checks.report(&format!("postgres-{mode}"));
        h.stop()?;
    }
    Ok(())
}
fn run_drivers(
    h: &ServeHarness,
    access: &SessionView,
    secret: &SessionSecret,
    schema: &str,
    operators: &str,
) -> ServeResult<()> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    for (variable, java) in [
        ("NUNYA_TEST_JSON_PYTHON", false),
        ("NUNYA_TEST_JSON_JAVA", true),
    ] {
        let Some(executable) = std::env::var_os(variable) else {
            continue;
        };
        let mut command = std::process::Command::new(executable);
        if java {
            command
                .arg("--class-path")
                .arg(std::env::var("NUNYA_TEST_JSON_JAVA_CLASSPATH")?)
                .arg(root.join("harness/drivers/JsonPathsCheck.java"));
        } else {
            command.arg(root.join("harness/drivers/json_paths_check.py"));
        }
        let output = command
            .env("NUNYA_TEST_JSON_OPERATORS", operators)
            .env("PGHOST", "127.0.0.1")
            .env("PGPORT", h.pg_port().to_string())
            .env("PGDATABASE", "app")
            .env("PGUSER", format!("session:{}", access.id))
            .env("PGPASSWORD", &*secret.secret)
            .env("PGSSLMODE", "disable")
            .env("NUNYA_TEST_JSON_SCHEMA", schema)
            .output()?;
        print!("{}", String::from_utf8_lossy(&output.stdout));
        if !output.status.success() {
            return Err("external JSON driver failed; inspect fixed step labels".into());
        }
    }
    Ok(())
}
use googleapis_tonic_google_spanner_v1::google::spanner::v1::{
    CreateSessionRequest, DeleteSessionRequest, ExecuteSqlRequest, KeySet, ReadRequest,
    ResultSet as NativeResultSet, spanner_client::SpannerClient,
};
use std::time::Duration;
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
    checks.pass(
        label,
        ![
            "example.test",
            "fixture-phone",
            "fixture-number",
            "fixture-hidden",
            "fixture-processor",
        ]
        .iter()
        .any(|value| error.message().contains(value)),
    );
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
                            format!("DROP TABLE {schema}.events"),
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

async fn native_stream(
    mut stream: tonic::Streaming<
        googleapis_tonic_google_spanner_v1::google::spanner::v1::PartialResultSet,
    >,
) -> ServeResult<(String, usize)> {
    let mut result = String::new();
    let mut chunks = 0;
    while let Some(part) = stream.message().await? {
        if part.chunked_value {
            chunks += 1;
        }
        for value in part.values {
            match value.kind {
                Some(prost_types::value::Kind::StringValue(text)) => result.push_str(&text),
                _ => return Err("unexpected native streamed JSON cell".into()),
            }
        }
    }
    if result.is_empty() {
        return Err("empty native stream".into());
    }
    Ok((result, chunks))
}

async fn native_four(
    client: &mut NativeClient,
    session: &str,
    table: &str,
    expected: &serde_json::Value,
    checks: &mut Checks,
) -> ServeResult<usize> {
    let sql = native_sql(session, &format!("SELECT doc FROM {table} WHERE id = 1"));
    let result = client.execute_sql(sql.clone()).await?.into_inner();
    checks.pass(
        "native-ExecuteSql",
        json(native_text(&result, 0)?)? == *expected,
    );
    let (value, sql_chunks) =
        native_stream(client.execute_streaming_sql(sql).await?.into_inner()).await?;
    checks.pass("native-ExecuteStreamingSql", json(&value)? == *expected);
    let request = native_read(session, table, &["doc"]);
    let result = client.read(request.clone()).await?.into_inner();
    checks.pass("native-Read", json(native_text(&result, 0)?)? == *expected);
    let (value, read_chunks) =
        native_stream(client.streaming_read(request).await?.into_inner()).await?;
    checks.pass("native-StreamingRead", json(&value)? == *expected);
    Ok(sql_chunks + read_chunks)
}

#[tokio::test]
#[ignore = "requires NUNYA_TEST_SPANNER_EMULATOR, NUNYA_TEST_SPANNER_REST and NUNYA_TEST_SPANNER_DB"]
async fn spanner_json_paths() -> ServeResult<()> {
    use googleapis_tonic_google_spanner_v1::google::spanner::v1::{
        CommitRequest, Mutation, TransactionOptions, commit_request, mutation, transaction_options,
    };
    let rest = std::env::var("NUNYA_TEST_SPANNER_REST")?;
    let database = std::env::var("NUNYA_TEST_SPANNER_DB")?;
    let endpoint = format!("http://{}", std::env::var("NUNYA_TEST_SPANNER_EMULATOR")?);
    for mode in ["tokenize", "show"] {
        let schema = format!("json_{}", &nunya::token::SessionId::random().to_hex()[..12]);
        let mut fixture = NativeFixture {
            rest: rest.clone(),
            database: database.clone(),
            schema: schema.clone(),
            names: vec![],
        };
        let table = format!("{schema}.events");
        native_ddl(
            &rest,
            &database,
            vec![
                format!("CREATE SCHEMA {schema}"),
                format!("CREATE TABLE {table} (id INT64 NOT NULL, doc JSON) PRIMARY KEY (id)"),
            ],
        )
        .await?;
        let original = if mode == "tokenize" {
            ORIGINAL
        } else {
            PAYMENT
        };
        let changed = if mode == "tokenize" {
            CHANGED
        } else {
            PAYMENT_CHANGED
        };
        native_update(
            &rest,
            &database,
            &format!("INSERT INTO {table} (id,doc) VALUES (1, JSON '{original}')"),
        )
        .await?;
        let mut h = ServeHarness::new(
            &policy(&schema, mode, &["doc"]),
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
            .acquire_session("json-native-agent", "spanner-native", Mode::ReadWrite)
            .await?;
        let mut client = native_client(&h, &access, &secret).await?;
        let session = client
            .create_session(CreateSessionRequest {
                database: database.clone(),
                ..Default::default()
            })
            .await?
            .into_inner()
            .name;
        fixture.names.push(session.clone());
        let mut checks = Checks::default();
        let first = client
            .execute_sql(native_sql(
                &session,
                &format!("SELECT doc FROM {table} WHERE id = 1"),
            ))
            .await?
            .into_inner();
        let initial = json(native_text(&first, 0)?)?;
        checks.pass(
            "native-filter-original",
            if mode == "tokenize" {
                filtered(&initial, false)
            } else {
                shown(&initial, false)
            },
        );
        native_four(&mut client, &session, &table, &initial, &mut checks).await?;
        let mut reconnect = native_client(&h, &access, &secret).await?;
        let reconnect_session = reconnect
            .create_session(CreateSessionRequest {
                database: database.clone(),
                ..Default::default()
            })
            .await?
            .into_inner()
            .name;
        fixture.names.push(reconnect_session.clone());
        let repeated = reconnect
            .execute_sql(native_sql(
                &reconnect_session,
                &format!("SELECT doc FROM {table} WHERE id = 1"),
            ))
            .await?
            .into_inner();
        checks.pass(
            "native-reconnect-stable",
            json(native_text(&repeated, 0)?)? == initial,
        );
        reconnect
            .delete_session(DeleteSessionRequest {
                name: reconnect_session,
            })
            .await?;

        native_update(
            &rest,
            &database,
            &format!("UPDATE {table} SET doc = JSON '{changed}' WHERE id = 1"),
        )
        .await?;
        let result = client
            .execute_sql(native_sql(
                &session,
                &format!("SELECT doc FROM {table} WHERE id = 1"),
            ))
            .await?
            .into_inner();
        let altered = json(native_text(&result, 0)?)?;
        checks.pass(
            "native-filter-shape-change",
            if mode == "tokenize" {
                filtered(&altered, true)
            } else {
                shown(&altered, true)
            },
        );
        native_four(&mut client, &session, &table, &altered, &mut checks).await?;
        if mode == "tokenize" {
            let doc = r#"{"status":null,"recipients":[{}]}"#;
            native_update(
                &rest,
                &database,
                &format!("UPDATE {table} SET doc = JSON '{doc}' WHERE id = 1"),
            )
            .await?;
            native_four(&mut client, &session, &table, &json(doc)?, &mut checks).await?;
            for (sql, label) in [
                (
                    format!("SELECT JSON_VALUE(doc, '$.status') FROM {table}"),
                    "native-json-function",
                ),
                (
                    format!("SELECT id FROM {table} WHERE JSON_VALUE(doc, '$.status') = 'x'"),
                    "native-json-predicate",
                ),
            ] {
                native_refused(
                    client.execute_sql(native_sql(&session, &sql)).await,
                    label,
                    &mut checks,
                )?;
            }
            let mutation = Mutation {
                operation: Some(mutation::Operation::Update(mutation::Write {
                    table: table.clone(),
                    columns: vec!["id".into(), "doc".into()],
                    values: vec![prost_types::ListValue {
                        values: vec![
                            prost_types::Value {
                                kind: Some(prost_types::value::Kind::StringValue("1".into())),
                            },
                            prost_types::Value {
                                kind: Some(prost_types::value::Kind::StringValue("{}".into())),
                            },
                        ],
                    }],
                })),
            };
            native_refused(
                client
                    .commit(CommitRequest {
                        session: session.clone(),
                        transaction: Some(commit_request::Transaction::SingleUseTransaction(
                            TransactionOptions {
                                mode: Some(transaction_options::Mode::ReadWrite(
                                    transaction_options::ReadWrite::default(),
                                )),
                                ..Default::default()
                            },
                        )),
                        mutations: vec![mutation],
                        ..Default::default()
                    })
                    .await,
                "native-json-mutation",
                &mut checks,
            )?;
        } else {
            let mut large = json(PAYMENT_CHANGED)?;
            large["visible_large"] = "x".repeat(1_500_000).into();
            let mut source = large.clone();
            source["payment"]["card"] =
                serde_json::json!({"number":"fixture-number","cvv":"fixture-cvv"});
            large["payment"]
                .as_object_mut()
                .ok_or("missing payment object")?
                .remove("card");
            let upstream_session = native_rest(
                &rest,
                "POST",
                &format!("{database}/sessions"),
                serde_json::json!({}),
            )
            .await?;
            let upstream_name = upstream_session["name"]
                .as_str()
                .ok_or("missing fixture session")?;
            fixture.names.push(upstream_name.into());
            native_rest(&rest,"POST",&format!("{upstream_name}:commit"),serde_json::json!({"singleUseTransaction":{"readWrite":{}},"mutations":[{"update":{"table":table,"columns":["id","doc"],"values":[["1",source.to_string()]]}}]})).await?;
            let mut upstream = SpannerClient::new(
                NativeEndpoint::from_shared(endpoint.clone())?
                    .connect()
                    .await?,
            );
            let (raw, upstream_chunks) = native_stream(
                upstream
                    .execute_streaming_sql(native_sql(
                        upstream_name,
                        &format!("SELECT doc FROM {table} WHERE id = 1"),
                    ))
                    .await?
                    .into_inner(),
            )
            .await?;
            checks.pass("native-large-fixture", json(&raw)? == source);
            println!(
                "OBSERVATION emulator-visible-1500000-byte-member chunked-parts={upstream_chunks}"
            );
            let chunks = native_four(&mut client, &session, &table, &large, &mut checks).await?;
            println!(
                "OBSERVATION native-visible-1500000-byte-member downstream-chunked-parts={chunks}"
            );
        }
        for expression in ["JSON 'null'", "NULL"] {
            native_update(
                &rest,
                &database,
                &format!("UPDATE {table} SET doc = {expression} WHERE id = 1"),
            )
            .await?;
            let result = client
                .execute_sql(native_sql(
                    &session,
                    &format!("SELECT doc FROM {table} WHERE id = 1"),
                ))
                .await?
                .into_inner();
            let kind = result
                .rows
                .first()
                .and_then(|row| row.values.first())
                .and_then(|cell| cell.kind.as_ref());
            let observation = match kind {
                Some(prost_types::value::Kind::NullValue(_)) => "SQL-NULL",
                Some(prost_types::value::Kind::StringValue(value)) if value == "null" => {
                    "JSON-null"
                }
                _ => return Err("unexpected native NULL observation".into()),
            };
            println!("OBSERVATION native {mode} {expression}: {observation}");
        }
        checks.report(&format!("spanner-{mode}"));
        client
            .delete_session(DeleteSessionRequest { name: session })
            .await?;
        h.stop()?;
    }
    Ok(())
}

// S7 operator contract, separate from the S6 whole-document acceptance checks.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires NUNYA_TEST_JSON_PG and NUNYA_TEST_JSON_PG_ADMIN"]
async fn postgres_json_operators() -> ServeResult<()> {
    let url = std::env::var("NUNYA_TEST_JSON_PG")?;
    let admin_url = std::env::var("NUNYA_TEST_JSON_PG_ADMIN")?;
    let schema = format!("json_{}", &nunya::token::SessionId::random().to_hex()[..12]);
    let _fixture = PgFixture {
        url: admin_url.clone(),
        schema: schema.clone(),
    };
    let (admin, connection) = tokio_postgres::connect(&admin_url, NoTls).await?;
    let _admin_driver = Driver(tokio::spawn(async move {
        let _ = connection.await;
    }));
    admin.batch_execute(&format!("CREATE SCHEMA {schema}; CREATE TABLE {schema}.events(id int PRIMARY KEY, doc_json json, doc_jsonb jsonb); CREATE TABLE {schema}.customers(email text); INSERT INTO {schema}.events VALUES (1, '{ORIGINAL}', '{ORIGINAL}'); INSERT INTO {schema}.customers VALUES ('first@example.test'); GRANT USAGE ON SCHEMA {schema} TO PUBLIC; GRANT SELECT, UPDATE ON ALL TABLES IN SCHEMA {schema} TO PUBLIC")).await?;
    let p = format!(
        "{}\n[[column]]\ntable = '{schema}.customers'\nname = 'email'\nblind = 'email'\n",
        policy(&schema, "tokenize", &["doc_json", "doc_jsonb"])
    );
    let mut h = ServeHarness::new(&p, Some(&url), None, None, "")?;
    configure(&h, Some(&schema), 600000)?;
    h.start().await?;
    let (access, secret, _) = h
        .acquire_session("json-operators", "postgres", Mode::ReadWrite)
        .await?;
    let (client, task) = h.pg_connect(&access, &secret, "app").await?;
    let _driver = Driver(task);
    let mut checks = Checks::default();
    for column in ["doc_json", "doc_jsonb"] {
        println!("STAGE whole-document {column}");
        let whole = json(cell(
            &rows(&client, &format!("SELECT {column} FROM events")).await?,
            0,
        )?)?;
        println!("STAGE extraction-projection {column}");
        let mut projected = vec![Vec::new()];
        for expression in [
            format!("{column}->'processor_response'"),
            format!("{column}->'recipients'"),
            format!("{column}->>'status'"),
            format!("{column} #>> '{{recipients,0,email}}'"),
        ] {
            println!("STAGE SELECT {expression} FROM events");
            let result = rows(&client, &format!("SELECT {expression} FROM events")).await?;
            checks.pass("direct-extraction-accepted", result.len() == 1);
            projected[0].push(result[0][0].clone());
        }
        checks.pass("masked-projection", projected[0][0].is_none());
        checks.pass(
            "parent-equals-whole",
            json(cell(&projected, 1)?)? == whole["recipients"],
        );
        checks.pass("show-projection", cell(&projected, 2)? == "sent");
        let token = cell(&projected, 3)?.to_owned();
        checks.pass(
            "blind-token-equals-whole",
            whole["recipients"][0]["email"] == token,
        );
        let statement = client.prepare(&format!("SELECT {column}->'recipients', {column}->>'status', {column} #>> '{{recipients,0,email}}' FROM events")).await?;
        let binary = client.query(&statement, &[]).await?;
        let parent: RawJson = binary[0].try_get(0)?;
        checks.pass(
            "extended-binary-parent",
            json(parent.0.as_deref().ok_or("NULL parent")?)? == whole["recipients"],
        );
        checks.pass(
            "extended-binary-show-and-blind",
            binary[0].try_get::<_, String>(1)? == "sent"
                && binary[0].try_get::<_, String>(2)? == token,
        );
        checks.pass(
            "show-equality",
            rows(
                &client,
                &format!("SELECT id FROM events WHERE {column}->>'status' = 'sent'"),
            )
            .await?
            .len()
                == 1,
        );
        checks.pass(
            "show-is-null",
            rows(
                &client,
                &format!("SELECT id FROM events WHERE {column}->>'status' IS NULL"),
            )
            .await?
            .is_empty(),
        );
        let statement = client
            .prepare(&format!(
                "SELECT id FROM events WHERE {column} #>> '{{recipients,0,email}}' = $1"
            ))
            .await?;
        checks.pass(
            "blind-param-extended-binary",
            client.query(&statement, &[&token]).await?.len() == 1,
        );
        checks.pass(
            "blind-literal",
            rows(
                &client,
                &format!(
                    "SELECT id FROM events WHERE {column} #>> '{{recipients,0,email}}' = '{token}'"
                ),
            )
            .await?
            .len()
                == 1,
        );
        checks.pass("blind-join", rows(&client, &format!("SELECT e.id FROM events e JOIN customers c ON e.{column} #>> '{{recipients,0,email}}' = c.email")).await?.len() == 1);
        for sql in [
            format!("SELECT id FROM events WHERE {column}->>'processor_response' = 'x'"),
            format!("SELECT id FROM events WHERE {column}->>'processor_response' IS NULL"),
        ] {
            let error = client
                .simple_query(&sql)
                .await
                .err()
                .ok_or("PRODUCT BUG: masked predicate accepted")?;
            checks.pass(
                "masked-predicate-NY005",
                error.code().is_some_and(|c| c.code() == "NY005"),
            );
        }
    }
    update(&admin, &schema, CHANGED).await?;
    checks.pass(
        "show-shape-change-projection",
        rows(&client, "SELECT doc_jsonb->>'status' FROM events").await?[0][0].is_none(),
    );
    checks.pass("show-shape-change-predicate", rows(&client, "SELECT id FROM events WHERE doc_jsonb->>'status' = '{\"unapproved\": \"fixture-hidden\"}'").await?.is_empty());
    for sql in [
        "SELECT doc_jsonb @> '{}' FROM events",
        "SELECT doc_jsonb ? 'status' FROM events",
        "SELECT jsonb_path_query(doc_jsonb, '$.status') FROM events",
        "SELECT doc_jsonb->id FROM events",
        "UPDATE events SET id = id WHERE doc_jsonb->>'status' = 'sent'",
    ] {
        refused(&client, sql, &mut checks).await?;
    }
    update(&admin, &schema, ORIGINAL).await?;
    tokio::task::block_in_place(|| run_drivers(&h, &access, &secret, &schema, "1"))?;
    h.stop()?;
    let show_policy = format!(
        "{}\n[[json_path]]\ntable = '{schema}.events'\nname = 'doc_json'\npath = '$.items[*].card'\nmasked = true\n[[json_path]]\ntable = '{schema}.events'\nname = 'doc_jsonb'\npath = '$.items[*].card'\nmasked = true\n",
        policy(&schema, "show", &["doc_json", "doc_jsonb"])
    );
    let mut show = ServeHarness::new(&show_policy, Some(&url), None, None, "")?;
    configure(&show, Some(&schema), 600000)?;
    show.start().await?;
    let (a, b, _) = show
        .acquire_session("json-show-operators", "postgres", Mode::ReadWrite)
        .await?;
    let (c, task) = show.pg_connect(&a, &b, "app").await?;
    let _show_driver = Driver(task);
    update(&admin, &schema, r#"{"payment":{"card":"fixture-hidden","amount":42},"new_top":"fixture-top","items":{"0":{"card":"fixture-hidden"}}}"#).await?;
    for col in ["doc_json", "doc_jsonb"] {
        let whole = json(cell(
            &rows(&c, &format!("SELECT {col} FROM events")).await?,
            0,
        )?)?;
        checks.pass(
            "show-parent-filtered",
            json(cell(
                &rows(&c, &format!("SELECT {col}->'payment' FROM events")).await?,
                0,
            )?)? == whole["payment"],
        );
        checks.pass(
            "show-new-key",
            cell(
                &rows(&c, &format!("SELECT {col}->>'new_top' FROM events")).await?,
                0,
            )? == "fixture-top",
        );
        checks.pass(
            "show-masked-projection",
            rows(
                &c,
                &format!("SELECT {col} #> '{{payment,card}}' FROM events"),
            )
            .await?[0][0]
                .is_none(),
        );
        checks.pass(
            "index-container-guard",
            rows(
                &c,
                &format!("SELECT {col} #> '{{items,0,card}}' FROM events"),
            )
            .await?[0][0]
                .is_none(),
        );
        refused(
            &c,
            &format!("SELECT id FROM events WHERE {col} #> '{{payment,card}}' IS NULL"),
            &mut checks,
        )
        .await?;
    }
    tokio::task::block_in_place(|| run_drivers(&show, &a, &b, &schema, "show"))?;
    checks.report("postgres-operators-tokio");
    show.stop()?;
    Ok(())
}

async fn operator_native_rows(
    client: &mut NativeClient,
    session: &str,
    sql: &str,
    streaming: bool,
) -> ServeResult<Vec<Vec<Option<String>>>> {
    fn value(v: prost_types::Value) -> ServeResult<Option<String>> {
        match v.kind {
            Some(prost_types::value::Kind::StringValue(s)) => Ok(Some(s)),
            Some(prost_types::value::Kind::NullValue(_)) => Ok(None),
            _ => Err("unexpected operator result type".into()),
        }
    }
    if !streaming {
        return client
            .execute_sql(native_sql(session, sql))
            .await?
            .into_inner()
            .rows
            .into_iter()
            .map(|r| r.values.into_iter().map(value).collect())
            .collect();
    }
    let mut stream = client
        .execute_streaming_sql(native_sql(session, sql))
        .await?
        .into_inner();
    let mut out = Vec::new();
    while let Some(part) = stream.message().await? {
        if part.chunked_value {
            return Err("unexpected chunked small operator fixture".into());
        }
        for v in part.values {
            out.push(vec![value(v)?]);
        }
    }
    Ok(out)
}

#[tokio::test]
#[ignore = "requires Spanner emulator environment"]
async fn spanner_json_operators() -> ServeResult<()> {
    let rest = std::env::var("NUNYA_TEST_SPANNER_REST")?;
    let database = std::env::var("NUNYA_TEST_SPANNER_DB")?;
    let endpoint = format!("http://{}", std::env::var("NUNYA_TEST_SPANNER_EMULATOR")?);
    // Probe upstream semantics independently of Nunya's mandatory container guards.
    let probe = native_rest(
        &rest,
        "POST",
        &format!("{database}/sessions"),
        serde_json::json!({}),
    )
    .await?;
    let probe_name = probe["name"].as_str().ok_or("probe session missing")?;
    let observation = native_rest(
        &rest,
        "POST",
        &format!("{probe_name}:executeSql"),
        serde_json::json!({"sql": r#"SELECT JSON_VALUE(JSON '{"0":"member"}', '$[0]')"#}),
    )
    .await?;
    println!(
        "OBSERVATION emulator-object-index-zero {}",
        observation["rows"]
    );
    native_rest(&rest, "DELETE", probe_name, serde_json::json!({})).await?;
    for mode in ["tokenize", "show"] {
        let schema = format!("json_{}", &nunya::token::SessionId::random().to_hex()[..12]);
        let table = format!("{schema}.events");
        let customers = format!("{schema}.customers");
        let mut fixture = NativeFixture {
            rest: rest.clone(),
            database: database.clone(),
            schema: schema.clone(),
            names: vec![],
        };
        native_ddl(
            &rest,
            &database,
            vec![
                format!("CREATE SCHEMA {schema}"),
                format!("CREATE TABLE {table} (id INT64 NOT NULL, doc JSON) PRIMARY KEY (id)"),
                format!("CREATE TABLE {customers} (email STRING(MAX) NOT NULL) PRIMARY KEY(email)"),
            ],
        )
        .await?;
        native_update(
            &rest,
            &database,
            &format!("INSERT INTO {table} (id,doc) VALUES (1, JSON '{ORIGINAL}')"),
        )
        .await?;
        native_update(
            &rest,
            &database,
            &format!("INSERT INTO {customers} (email) VALUES ('first@example.test')"),
        )
        .await?;
        let p = format!(
            "{}\n[[column]]\ntable = '{customers}'\nname = 'email'\nblind = 'email'\n[[json_path]]\ntable = '{table}'\nname = 'doc'\npath = '$.items[*].card'\nmasked = true\n",
            policy(&schema, mode, &["doc"])
        );
        let mut h = ServeHarness::new(
            &p,
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
        let (a, b, _) = h
            .acquire_session("json-native-operators", "spanner-native", Mode::ReadWrite)
            .await?;
        let mut client = native_client(&h, &a, &b).await?;
        let session = client
            .create_session(CreateSessionRequest {
                database: database.clone(),
                ..Default::default()
            })
            .await?
            .into_inner()
            .name;
        fixture.names.push(session.clone());
        for streaming in [false, true] {
            let mut checks = Checks::default();
            let initial = if mode == "tokenize" {
                ORIGINAL
            } else {
                r#"{"payment":{"card":"fixture-hidden","amount":42},"new_top":"fixture-top","items":{"0":{"card":"fixture-hidden"}}}"#
            };
            native_update(
                &rest,
                &database,
                &format!("UPDATE {table} SET doc=JSON '{initial}' WHERE id=1"),
            )
            .await?;
            let whole = json(cell(
                &operator_native_rows(
                    &mut client,
                    &session,
                    &format!("SELECT doc FROM {table}"),
                    streaming,
                )
                .await?,
                0,
            )?)?;
            let parent = if mode == "tokenize" {
                "recipients"
            } else {
                "payment"
            };
            let got = operator_native_rows(
                &mut client,
                &session,
                &format!("SELECT JSON_QUERY(doc, '$.{parent}') FROM {table}"),
                streaming,
            )
            .await?;
            checks.pass(
                "native-parent-filtered",
                json(cell(&got, 0)?)? == whole[parent],
            );
            let masked = if mode == "tokenize" {
                "processor_response"
            } else {
                "payment.card"
            };
            checks.pass(
                "native-masked-null",
                operator_native_rows(
                    &mut client,
                    &session,
                    &format!("SELECT JSON_QUERY(doc, '$.{masked}') FROM {table}"),
                    streaming,
                )
                .await?[0][0]
                    .is_none(),
            );
            if mode == "tokenize" {
                let got = operator_native_rows(
                    &mut client,
                    &session,
                    &format!("SELECT JSON_VALUE(doc, '$.recipients[0].email') FROM {table}"),
                    streaming,
                )
                .await?;
                let token = cell(&got, 0)?;
                checks.pass(
                    "native-token-whole",
                    whole["recipients"][0]["email"] == token,
                );
                for (expr, label, n) in [
                    (
                        "JSON_VALUE(doc, '$.status') = 'sent'".to_owned(),
                        "show-equality",
                        1,
                    ),
                    (
                        "JSON_VALUE(doc, '$.status') IS NULL".to_owned(),
                        "show-null",
                        0,
                    ),
                    (
                        format!("JSON_VALUE(doc, '$.recipients[0].email') = '{token}'"),
                        "blind-literal",
                        1,
                    ),
                ] {
                    checks.pass(
                        label,
                        operator_native_rows(
                            &mut client,
                            &session,
                            &format!("SELECT id FROM {table} WHERE {expr}"),
                            streaming,
                        )
                        .await?
                        .len()
                            == n,
                    );
                }
                let mut req = native_sql(
                    &session,
                    &format!(
                        "SELECT id FROM {table} WHERE JSON_VALUE(doc, '$.recipients[0].email') = @token"
                    ),
                );
                req.params = Some(prost_types::Struct {
                    fields: std::collections::BTreeMap::from([(
                        "token".into(),
                        prost_types::Value {
                            kind: Some(prost_types::value::Kind::StringValue(token.into())),
                        },
                    )]),
                });
                req.param_types.insert("token".into(),googleapis_tonic_google_spanner_v1::google::spanner::v1::Type { code: googleapis_tonic_google_spanner_v1::google::spanner::v1::TypeCode::String as i32, ..Default::default() });
                if streaming {
                    let mut st = client.execute_streaming_sql(req).await?.into_inner();
                    let mut n = 0;
                    while let Some(p) = st.message().await? {
                        n += p.values.len();
                    }
                    checks.pass("blind-param", n == 1);
                } else {
                    checks.pass(
                        "blind-param",
                        client.execute_sql(req).await?.into_inner().rows.len() == 1,
                    );
                }
                checks.pass("native-blind-join",operator_native_rows(&mut client,&session,&format!("SELECT e.id FROM {table} e JOIN {customers} c ON JSON_VALUE(e.doc, '$.recipients[0].email') = c.email"),streaming).await?.len()==1);
                native_update(
                    &rest,
                    &database,
                    &format!("UPDATE {table} SET doc=JSON '{CHANGED}' WHERE id=1"),
                )
                .await?;
                checks.pass(
                    "native-shape-null",
                    operator_native_rows(
                        &mut client,
                        &session,
                        &format!("SELECT JSON_VALUE(doc, '$.status') FROM {table}"),
                        streaming,
                    )
                    .await?[0][0]
                        .is_none(),
                );
                checks.pass("native-shape-predicate",operator_native_rows(&mut client,&session,&format!(r#"SELECT id FROM {table} WHERE JSON_VALUE(doc, '$.status') = '{{"unapproved":"fixture-hidden"}}'"#),streaming).await?.is_empty());
            } else {
                checks.pass(
                    "native-show-new-key",
                    cell(
                        &operator_native_rows(
                            &mut client,
                            &session,
                            &format!("SELECT JSON_VALUE(doc, '$.new_top') FROM {table}"),
                            streaming,
                        )
                        .await?,
                        0,
                    )? == "fixture-top",
                );
                checks.pass(
                    "native-index-container-guard",
                    operator_native_rows(
                        &mut client,
                        &session,
                        &format!("SELECT JSON_QUERY(doc, '$.items[0].card') FROM {table}"),
                        streaming,
                    )
                    .await?[0][0]
                        .is_none(),
                );
            }
            for sql in [
                format!("SELECT id FROM {table} WHERE JSON_VALUE(doc, '$.{masked}') IS NULL"),
                format!("SELECT id FROM {table} WHERE JSON_VALUE(doc, '$.{masked}') = 'x'"),
                format!("SELECT id FROM {table} WHERE JSON_QUERY(doc, '$.status') IS NULL"),
                format!("SELECT JSON_VALUE(doc, doc) FROM {table}"),
                format!("UPDATE {table} SET id=id WHERE JSON_VALUE(doc, '$.status') = 'sent'"),
                format!("SELECT JSON_KEYS(doc) FROM {table}"),
            ] {
                if streaming {
                    native_refused(
                        client
                            .execute_streaming_sql(native_sql(&session, &sql))
                            .await,
                        "native-refusal",
                        &mut checks,
                    )?;
                } else {
                    native_refused(
                        client.execute_sql(native_sql(&session, &sql)).await,
                        "native-refusal",
                        &mut checks,
                    )?;
                }
            }
            checks.report(&format!(
                "spanner-operators-{mode}-{}",
                if streaming { "streaming" } else { "unary" }
            ));
        }
        client
            .delete_session(DeleteSessionRequest { name: session })
            .await?;
        h.stop()?;
        native_ddl(&rest, &database, vec![format!("DROP TABLE {customers}")]).await?;
    }
    Ok(())
}
