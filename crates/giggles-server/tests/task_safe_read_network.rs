//! GD-0061 R3-315: Phase 3 evidence over real connections. Every test runs a
//! v2 server with the worked example of spec section 14 (`shipments`, since
//! `deliveries` is reserved) and task `inc-436`, and talks to the agent
//! listeners: pgwire in the text (simple) and the binary (extended,
//! tokio-postgres) formats, HTTP and gRPC. `kill_after_page_sent_is_charged_after_restart`
//! runs the server binary as a child process and kills it.
//!
//! The table: 31 failed rows of merchant 42 in the incident window (ids
//! 1-31), 5 delivered rows of merchant 42 (32-36), and 4 failed rows of
//! merchant 43 (100-103). The task reads `id`, `status`, `failed_at` and
//! `attempt_count`, masks `customer_email` (as NULL) and hides
//! `merchant_id` and `card_token`. No reply may ever carry an e-mail address
//! or a card token: every value of those columns contains `example.com` or
//! `tok_`, and every reply in this file is scanned for both.
#![cfg(feature = "engine-rocksdb")]
mod agent_support;

use agent_support::{
    AGENT_STARTUP, AgentServer, DAY_MS, Msg, Startup, TestCert, Wire, binding, certificate,
    connect_raw, error_of, now_ms, raw_startup, scratch, timestamptz,
};
use bytes::Bytes;
use giggles_core::{
    Admin, Engine, Limits,
    roles::{NewRole, TablePrivilege},
    task::{
        StatementClass,
        definition::{
            AllowedWrite, DisclosureCaps, LoadLimits, MutationCaps, NewTask, PredicateText,
            WallClockMs,
        },
        readlog::{
            ENTRY_GRPC, ENTRY_HTTP, ENTRY_PGWIRE_EXTENDED, ENTRY_PGWIRE_SIMPLE, OUTCOME_OPEN,
            ReadRecord, class_code,
        },
        release::Disclosure,
    },
};
use giggles_server::{
    Running,
    api::{
        ApiLimits, ApiRuntime,
        json as wire_json,
        net::{serve_grpc_bound, serve_http_bound},
        proto,
    },
    tls,
};
use http_body_util::{BodyExt, Full};
use hyper_util::rt::TokioIo;
use prost::Message;
use rustls::pki_types::{PrivatePkcs8KeyDer, ServerName};
use serde_json::{Value as Json, json};
use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    process::{Child, Command},
    sync::{Barrier, watch},
};
use tokio_postgres::config::SslMode;
use tokio_postgres_rustls::MakeRustlsConnect;
use tokio_rustls::TlsConnector;
use tonic::transport::{Certificate as CaCertificate, Channel, ClientTlsConfig, Identity};

const TASK: &str = "inc-436";
/// Step 1 of the worked example.
const STEP_1: &str = "SELECT * FROM shipments WHERE status = 'failed'";
/// The same rows as step 1, ordered, with every shown column named.
const FAILED_ROWS: &str = "SELECT id, status, failed_at, attempt_count, customer_email \
     FROM shipments WHERE status = 'failed' ORDER BY id";
/// Text that occurs in every `customer_email` and `card_token` value.
const SECRETS: [&str; 2] = ["example.com", "tok_"];

/// Visible failed rows (ids 1-31) and delivered rows (32-36) of merchant 42.
const FAILED: i64 = 31;
const VISIBLE: i64 = 36;

fn email(id: i64) -> String {
    // The first letters are not in id order, so a sort by e-mail is visible
    // in the order of the ids; only id 26 starts with `a`.
    let letter = (b'a' + (id * 11 % 26) as u8) as char;
    format!("{letter}{id:04}@example.com")
}

fn token(id: i64) -> String {
    format!("tok_{id:04}_card")
}

/// The ids of merchant 42's rows in the order of their real e-mail.
fn email_order() -> Vec<String> {
    let mut ids: Vec<i64> = (1..=VISIBLE).collect();
    ids.sort_by_key(|id| email(*id));
    ids.iter().map(i64::to_string).collect()
}

struct Options {
    caps: DisclosureCaps,
    /// More delivered rows of merchant 42, ids from 1000.
    extra_rows: i64,
    /// `status` references `statuses(code)`, which has no 'queued'.
    status_fk: bool,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            caps: DisclosureCaps {
                rows_read: 1000,
                rows_returned: 1000,
                bytes_returned: 2_097_152,
            },
            extra_rows: 0,
            status_fk: false,
        }
    }
}

/// The worked example's database at `path`: the table, the roles (the
/// executor holds SELECT and UPDATE, as in section 14), and task `inc-436`.
fn example_admin(path: &Path, options: &Options) -> Admin {
    let admin = Admin::create_engine(path, Limits::default(), Engine::V2).unwrap();
    let mut statements = Vec::new();
    let status = if options.status_fk {
        statements.push("CREATE TABLE statuses (code text primary key)".to_owned());
        statements.push("INSERT INTO statuses VALUES ('failed'), ('delivered')".to_owned());
        "status text REFERENCES statuses (code)"
    } else {
        "status text"
    };
    statements.push(format!(
        "CREATE TABLE shipments (id bigint primary key, merchant_id bigint, {status}, \
         failed_at timestamptz, attempt_count int, customer_email text, card_token text)"
    ));
    let mut rows = Vec::new();
    for id in 1..=FAILED {
        rows.push(format!(
            "({id}, 42, 'failed', '2026-11-05 17:00:{:02}+00', {}, '{}', '{}')",
            id, // seconds 1-31 of 17:00, inside the window
            id % 4,
            email(id),
            token(id)
        ));
    }
    for id in FAILED + 1..=VISIBLE {
        rows.push(format!(
            "({id}, 42, 'delivered', '2026-11-04 10:00:00+00', 0, '{}', '{}')",
            email(id),
            token(id)
        ));
    }
    for id in 100..=103 {
        rows.push(format!(
            "({id}, 43, 'failed', '2026-11-05 17:01:00+00', 1, '{}', '{}')",
            email(id),
            token(id)
        ));
    }
    for id in 1000..1000 + options.extra_rows {
        rows.push(format!(
            "({id}, 42, 'delivered', '2026-11-04 10:00:00+00', 0, '{}', '{}')",
            email(id),
            token(id)
        ));
    }
    statements.push(format!("INSERT INTO shipments VALUES {}", rows.join(", ")));
    for statement in &statements {
        admin.sql_session().execute(statement, &[], None).unwrap();
    }
    let roles = admin.roles();
    for (name, task_only) in [
        ("incident_controller", false),
        ("oncall_lead", false),
        ("agent_exec", true),
    ] {
        roles
            .create(NewRole {
                name: name.into(),
                login: true,
                ..NewRole::default()
            })
            .unwrap();
        if task_only {
            roles.set_task_only(name, true).unwrap();
        }
    }
    for (name, privileges) in [
        (
            "incident_controller",
            &[TablePrivilege::Select, TablePrivilege::Update][..],
        ),
        (
            "agent_exec",
            &[TablePrivilege::Select, TablePrivilege::Update][..],
        ),
        ("oncall_lead", &[TablePrivilege::Select][..]),
    ] {
        roles.grant_table("shipments", name, privileges).unwrap();
    }
    let issuer = roles.get("incident_controller").unwrap().unwrap();
    admin
        .tasks()
        .create(
            &issuer,
            NewTask {
                name: TASK.into(),
                executor: "agent_exec".into(),
                approver: Some("oncall_lead".into()),
                schema: None,
                table: "shipments".into(),
                visible_rows: PredicateText("merchant_id = 42".into()),
                readable: ["id", "status", "failed_at", "attempt_count"]
                    .map(String::from)
                    .to_vec(),
                masked: vec![("customer_email".into(), None)],
                untrusted_text: Vec::new(),
                allowed_write: Some(AllowedWrite {
                    columns: ["status".to_string()].into_iter().collect(),
                    old_row: PredicateText(
                        "status = 'failed' AND failed_at >= '2026-11-05 17:00:00+00' \
                         AND failed_at < '2026-11-05 17:07:00+00'"
                            .into(),
                    ),
                    new_row: PredicateText("status = 'queued'".into()),
                }),
                mutation_caps: MutationCaps {
                    distinct_rows: 25,
                    transitions: 25,
                },
                disclosure_caps: options.caps,
                // The worked example's statement_ms is 2000; a loaded test
                // machine gets more room so a slow build never cancels a read.
                limits: LoadLimits {
                    statement_ms: 30_000,
                    rows_scanned: 100_000,
                    concurrent_statements: 2,
                    ..LoadLimits::default()
                },
                approval_threshold: 5,
                // The spec's 17:30 expiry is in the past; one day from now.
                expires: timestamptz(now_ms() + DAY_MS),
            },
            WallClockMs(now_ms()),
        )
        .unwrap();
    admin
}

/// The agent pgwire, HTTP and gRPC listeners over one service.
struct Example {
    server: AgentServer,
    agent: TestCert,
    api: Arc<ApiRuntime>,
    http: Running,
    grpc: Running,
}

impl Example {
    async fn start(options: Options) -> Self {
        let path = scratch("r3-315");
        let admin = example_admin(&path.join("db.redb"), &options);
        let ca = certificate(None, false);
        let agent = certificate(Some(&ca), false);
        let server = AgentServer::start(
            admin,
            ca,
            vec![],
            vec![binding(&agent, "agent_exec", &[TASK])],
        )
        .await;
        let api = ApiRuntime::new_agent(server.service.clone(), ApiLimits { sessions: 4 }).unwrap();
        let (_, gate) = watch::channel(true);
        let bind = || async { TcpListener::bind("127.0.0.1:0").await.unwrap() };
        let http = serve_http_bound(bind().await, api.clone(), server.tls.clone(), 8, gate.clone())
            .await
            .unwrap();
        let grpc = serve_grpc_bound(bind().await, api.clone(), server.tls.clone(), 8, gate)
            .await
            .unwrap();
        Self {
            server,
            agent,
            api,
            http,
            grpc,
        }
    }

    async fn stop(self) {
        for listener in [self.http, self.grpc] {
            listener.shutdown().await.unwrap();
        }
        self.api.shutdown().await;
    }

    fn admin(&self) -> &Admin {
        &self.server.admin
    }

    fn task(&self) -> giggles_core::task::definition::TaskObjectId {
        self.admin().tasks().get(TASK).unwrap().unwrap().id
    }

    fn disclosure(&self) -> Disclosure {
        self.admin().task_disclosure(self.task()).unwrap()
    }

    fn records(&self) -> Vec<ReadRecord> {
        self.admin().task_read_log(self.task()).unwrap()
    }

    /// A raw protocol-v3 session on the agent listener.
    async fn wire(&self) -> Wire {
        connect_raw(
            self.server.agent.address,
            &self.server.ca,
            &self.agent,
            AGENT_STARTUP,
        )
        .await
    }

    /// tokio-postgres on the agent listener: the extended protocol with
    /// binary results.
    async fn driver(&self) -> tokio_postgres::Client {
        let address = self.server.agent.address;
        let (client, connection) = tokio_postgres::Config::new()
            .host("localhost")
            .hostaddr(address.ip())
            .port(address.port())
            .user("agent_exec")
            .dbname("giggles")
            .ssl_mode(SslMode::Require)
            .connect(MakeRustlsConnect::new(self.agent.client(&self.server.ca)))
            .await
            .expect("agent connects");
        tokio::spawn(connection);
        client
    }

    /// One HTTP/1.1 POST to the agent HTTP listener; the status and body.
    async fn post(&self, path: &str, token: Option<&str>, body: &Json) -> (u16, Vec<u8>) {
        let address = self.http.address;
        let body = serde_json::to_vec(body).unwrap();
        tokio::time::timeout(Duration::from_secs(30), async {
            let config = tls::client(
                vec![self.agent.cert.der().clone()],
                PrivatePkcs8KeyDer::from(self.agent.key.serialize_der()).into(),
                vec![self.server.ca.cert.der().clone()],
            )
            .unwrap();
            let mut config = (*config).clone();
            config.alpn_protocols = vec![b"http/1.1".to_vec()];
            let socket = TcpStream::connect(address).await.unwrap();
            let stream = TlsConnector::from(Arc::new(config))
                .connect(ServerName::try_from("localhost").unwrap(), socket)
                .await
                .unwrap();
            let mut request = hyper::Request::builder()
                .method("POST")
                .uri(format!("https://localhost:{}{path}", address.port()))
                .header("host", "localhost")
                .header("content-type", "application/json");
            if let Some(token) = token {
                request = request.header("x-giggles-session", token);
            }
            let request = request.body(Full::new(Bytes::from(body))).unwrap();
            let (mut sender, connection) =
                hyper::client::conn::http1::handshake(TokioIo::new(stream)).await.unwrap();
            let task = tokio::spawn(async move {
                let _ = connection.await;
            });
            let response = sender.send_request(request).await.unwrap();
            let status = response.status().as_u16();
            let bytes = response.into_body().collect().await.unwrap().to_bytes();
            task.abort();
            let _ = task.await;
            (status, bytes.to_vec())
        })
        .await
        .expect("HTTP call finished")
    }

    async fn http_session(&self) -> String {
        let (status, body) = self.post("/v1/sessions", None, &json!({})).await;
        let body: Json = serde_json::from_slice(&body).unwrap();
        assert_eq!(status, 201, "{body}");
        body["sessionId"].as_str().unwrap().to_owned()
    }

    /// One execute over HTTP; the raw JSON reply body.
    async fn http(&self, token: &str, request: &Json) -> Vec<u8> {
        let (status, body) = self.post("/v1/session/execute", Some(token), request).await;
        assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
        body
    }

    async fn grpc(&self) -> (proto::giggles_client::GigglesClient<Channel>, String) {
        let channel = Channel::from_shared(format!("https://127.0.0.1:{}", self.grpc.address.port()))
            .unwrap()
            .tls_config(
                ClientTlsConfig::new()
                    .domain_name("localhost")
                    .ca_certificate(CaCertificate::from_pem(self.server.ca.cert.pem()))
                    .identity(Identity::from_pem(
                        self.agent.cert.pem(),
                        self.agent.key.serialize_pem(),
                    )),
            )
            .unwrap()
            .connect_timeout(Duration::from_secs(10))
            .connect()
            .await
            .unwrap();
        let mut client = proto::giggles_client::GigglesClient::new(channel)
            .max_decoding_message_size(giggles_server::MAX_RESPONSE);
        let token = client
            .open_session(proto::OpenSessionRequest::default())
            .await
            .unwrap()
            .into_inner()
            .session_id;
        (client, token)
    }
}

async fn exec_grpc(
    client: &mut proto::giggles_client::GigglesClient<Channel>,
    token: &str,
    request: &Json,
) -> proto::ExecuteReply {
    let request = wire_json::decode_execute(&serde_json::to_vec(request).unwrap()).unwrap();
    let mut request = tonic::Request::new(request);
    request
        .metadata_mut()
        .insert("x-giggles-session", token.parse().unwrap());
    client.execute(request).await.unwrap().into_inner()
}

fn sql_request(text: &str) -> Json {
    json!({"sql": {"sql": text}})
}

fn json_code(body: &[u8]) -> String {
    let body: Json = serde_json::from_slice(body).unwrap();
    body["code"].as_str().unwrap_or_default().to_owned()
}

/// The first column (`id`) of every row of a JSON reply.
fn json_ids(body: &[u8]) -> Vec<String> {
    let view: Json = serde_json::from_slice(body).unwrap();
    let rows = view["operationOutcome"]["rows"]
        .as_array()
        .or_else(|| view["sql"]["rows"].as_array())
        .unwrap_or_else(|| panic!("no rows: {view}"));
    rows.iter()
        .map(|row| row["values"][0]["intValue"].as_str().unwrap().to_owned())
        .collect()
}

#[track_caller]
fn assert_no_secret(text: &str, context: &str) {
    for secret in SECRETS {
        assert!(!text.contains(secret), "{context}: `{secret}` on the wire: {text}");
    }
}

fn rows(messages: &[Msg]) -> Vec<Vec<Option<String>>> {
    messages
        .iter()
        .filter_map(|m| match m {
            Msg::Row(values) => Some(values.clone()),
            _ => None,
        })
        .collect()
}

fn ids(messages: &[Msg], column: usize) -> Vec<String> {
    rows(messages)
        .into_iter()
        .map(|row| row[column].clone().unwrap_or_default())
        .collect()
}

fn complete(messages: &[Msg]) -> Option<&str> {
    messages.iter().find_map(|m| match m {
        Msg::Complete(tag) => Some(tag.as_str()),
        _ => None,
    })
}

/// What a task was charged: the head's counters and the debit records.
fn charge(d: &Disclosure) -> (u64, u64, u64, usize) {
    (d.rows_read, d.rows_returned, d.bytes_returned, d.debits.len())
}

// --- raw protocol frames -----------------------------------------------------

fn frame(tag: u8, body: &[u8], out: &mut Vec<u8>) {
    out.push(tag);
    out.extend(((body.len() + 4) as u32).to_be_bytes());
    out.extend_from_slice(body);
}

fn cstr(text: &str, out: &mut Vec<u8>) {
    out.extend_from_slice(text.as_bytes());
    out.push(0);
}

fn query(sql: &str) -> Vec<u8> {
    let mut body = Vec::new();
    cstr(sql, &mut body);
    let mut out = Vec::new();
    frame(b'Q', &body, &mut out);
    out
}

fn parse(statement: &str, sql: &str, out: &mut Vec<u8>) {
    let mut body = Vec::new();
    cstr(statement, &mut body);
    cstr(sql, &mut body);
    body.extend(0u16.to_be_bytes());
    frame(b'P', &body, out);
}

/// Bind with text parameters and one result format for every column.
fn bind(statement: &str, params: &[&str], result_format: u16, out: &mut Vec<u8>) {
    let mut body = Vec::new();
    cstr("", &mut body);
    cstr(statement, &mut body);
    body.extend(0u16.to_be_bytes());
    body.extend((params.len() as u16).to_be_bytes());
    for param in params {
        body.extend((param.len() as i32).to_be_bytes());
        body.extend_from_slice(param.as_bytes());
    }
    body.extend(1u16.to_be_bytes());
    body.extend(result_format.to_be_bytes());
    frame(b'B', &body, out);
}

fn execute(max_rows: u32, out: &mut Vec<u8>) {
    let mut body = Vec::new();
    cstr("", &mut body);
    body.extend(max_rows.to_be_bytes());
    frame(b'E', &body, out);
}

fn sync(out: &mut Vec<u8>) {
    frame(b'S', &[], out);
}

/// The (tag, body) of each message in a raw answer.
fn frames(raw: &[u8]) -> Vec<(u8, &[u8])> {
    let mut out = Vec::new();
    let mut at = 0;
    while at < raw.len() {
        let length = u32::from_be_bytes(raw[at + 1..at + 5].try_into().unwrap()) as usize;
        out.push((raw[at], &raw[at + 5..at + 1 + length]));
        at += 1 + length;
    }
    out
}

/// The SQLSTATE of the first ErrorResponse in a raw answer.
fn raw_code(raw: &[u8]) -> Option<String> {
    frames(raw).into_iter().find(|(tag, _)| *tag == b'E').map(|(_, body)| {
        body.split(|b| *b == 0)
            .find(|field| field.first() == Some(&b'C'))
            .map(|field| String::from_utf8_lossy(&field[1..]).into_owned())
            .unwrap_or_default()
    })
}

/// Total bytes of the DataRow messages in a raw answer, and their count.
fn data_rows(raw: &[u8]) -> (usize, usize) {
    frames(raw)
        .into_iter()
        .filter(|(tag, _)| *tag == b'D')
        .fold((0, 0), |(bytes, n), (_, body)| (bytes + 5 + body.len(), n + 1))
}

// --- 1. the race for the last page ------------------------------------------

/// Two sessions of the task read the 31 failed rows at the same moment (a
/// barrier releases both), and the task may return 40 rows in all: exactly
/// one reader gets every row, the other gets 54T01 and no row, and the head
/// never passes the cap. Repeated over several rounds on fresh tasks.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_readers_race_last_page_one_wins() {
    for round in 0..3 {
        let example = Example::start(Options {
            caps: DisclosureCaps {
                rows_read: 1000,
                rows_returned: 40,
                bytes_returned: 2_097_152,
            },
            ..Options::default()
        })
        .await;
        let barrier = Arc::new(Barrier::new(2));
        let mut readers = Vec::new();
        for _ in 0..2 {
            let mut wire = example.wire().await;
            let barrier = barrier.clone();
            readers.push(tokio::spawn(async move {
                barrier.wait().await;
                wire.simple(FAILED_ROWS).await
            }));
        }
        let mut answers = Vec::new();
        for reader in readers {
            answers.push(reader.await.unwrap());
        }
        let (mut won, mut lost) = (0, 0);
        for messages in &answers {
            assert_no_secret(&format!("{messages:?}"), "race");
            match error_of(messages) {
                None => {
                    assert_eq!(rows(messages).len(), FAILED as usize, "round {round}");
                    assert!(rows(messages).iter().all(|row| row[4].is_none()));
                    won += 1;
                }
                Some((code, _)) => {
                    assert_eq!(code, "54T01", "round {round}: {messages:?}");
                    assert!(rows(messages).is_empty(), "round {round}: {messages:?}");
                    lost += 1;
                }
            }
        }
        assert_eq!((won, lost), (1, 1), "round {round}: {answers:?}");
        let disclosure = example.disclosure();
        assert!(disclosure.rows_returned <= 40, "{disclosure:?}");
        assert_eq!(
            disclosure.debits.iter().map(|d| d.rows_returned).sum::<u64>(),
            FAILED as u64,
            "{disclosure:?}"
        );
        let mut released: Vec<_> = example
            .records()
            .iter()
            .map(|r| (r.released_rows, r.sqlstate_text()))
            .collect();
        released.sort();
        assert_eq!(
            released,
            [(0, "54T01".to_owned()), (FAILED as u64, "00000".to_owned())],
            "round {round}"
        );
        example.stop().await;
    }
}

// --- 2. revocation between pages --------------------------------------------

/// The agent pages through its rows with a row limit (`LIMIT 10` over a
/// named statement, extended protocol). The task is revoked after the first
/// page: the next page sends no row and fails 28T01, and nothing more is
/// charged. A portal row limit (Execute max_rows) is refused as a cursor, so
/// no suspended portal can outlive a revocation.
#[tokio::test]
async fn revocation_between_pages_stops_next_page() {
    let example = Example::start(Options::default()).await;
    let mut session =
        RawSession::connect(example.server.agent.address, &example.server.ca, &example.agent)
            .await;
    let mut prepare = Vec::new();
    parse(
        "page",
        "SELECT id, status FROM shipments WHERE id > $1 ORDER BY id LIMIT 10",
        &mut prepare,
    );
    sync(&mut prepare);
    let answer = session.exchange(&prepare).await;
    assert_eq!(answer_code(&answer), None, "{answer:?}");
    let page = |after: &str, max_rows: u32| {
        let mut out = Vec::new();
        bind("page", &[after], 0, &mut out);
        execute(max_rows, &mut out);
        sync(&mut out);
        out
    };

    // A portal row limit is refused before the statement runs.
    let answer = session.exchange(&page("0", 5)).await;
    assert_eq!(answer_code(&answer).as_deref(), Some("0A000"), "{answer:?}");
    assert_eq!(answer_rows(&answer), 0);
    assert_eq!(example.disclosure().rows_returned, 0);

    // Page 1.
    let answer = session.exchange(&page("0", 0)).await;
    assert_eq!(answer_code(&answer), None, "{answer:?}");
    assert_eq!(answer_rows(&answer), 10);
    let after_page_1 = example.disclosure();
    assert!(after_page_1.rows_returned >= 10, "{after_page_1:?}");

    // The issuer revokes the task between the pages.
    let issuer = example.admin().roles().get("incident_controller").unwrap().unwrap();
    example.admin().tasks().revoke(&issuer, TASK).unwrap();

    // Page 2: no row, revoked (the server may also close the session),
    // nothing charged.
    let answer = session.exchange(&page("10", 0)).await;
    assert_eq!(answer_rows(&answer), 0, "{answer:?}");
    assert_eq!(answer_code(&answer).as_deref(), Some("28T01"), "{answer:?}");
    assert_eq!(charge(&example.disclosure()), charge(&after_page_1));
    // A new session of the revoked task is refused at startup.
    let startup = raw_startup(
        example.server.agent.address,
        &example.server.ca,
        &example.agent,
        AGENT_STARTUP,
    )
    .await;
    assert!(!matches!(startup, Startup::Ready(_)), "{startup:?}");
    assert_eq!(charge(&example.disclosure()), charge(&after_page_1));
    example.stop().await;
}

/// The SQLSTATE of the first ErrorResponse among `messages`.
fn answer_code(messages: &[(u8, Vec<u8>)]) -> Option<String> {
    messages.iter().find(|(tag, _)| *tag == b'E').map(|(_, body)| {
        body.split(|b| *b == 0)
            .find(|field| field.first() == Some(&b'C'))
            .map(|field| String::from_utf8_lossy(&field[1..]).into_owned())
            .unwrap_or_default()
    })
}

fn answer_rows(messages: &[(u8, Vec<u8>)]) -> usize {
    messages.iter().filter(|(tag, _)| *tag == b'D').count()
}

// --- 3. kill after a page was sent -------------------------------------------

/// A raw TLS session that can stop reading in the middle of a statement.
struct RawSession {
    stream: tokio_rustls::client::TlsStream<TcpStream>,
}

impl RawSession {
    async fn connect(address: SocketAddr, ca: &TestCert, cert: &TestCert) -> Self {
        let mut socket = TcpStream::connect(address).await.unwrap();
        socket
            .write_all(&[0, 0, 0, 8, 0x04, 0xd2, 0x16, 0x2f])
            .await
            .unwrap();
        let mut answer = [0u8; 1];
        socket.read_exact(&mut answer).await.unwrap();
        assert_eq!(answer[0], b'S');
        let stream = TlsConnector::from(Arc::new(cert.client(ca)))
            .connect(ServerName::try_from("localhost").unwrap(), socket)
            .await
            .unwrap();
        let mut session = Self { stream };
        let mut body = 196608u32.to_be_bytes().to_vec();
        for (key, value) in AGENT_STARTUP {
            cstr(key, &mut body);
            cstr(value, &mut body);
        }
        body.push(0);
        let mut packet = ((body.len() + 4) as u32).to_be_bytes().to_vec();
        packet.extend_from_slice(&body);
        session.stream.write_all(&packet).await.unwrap();
        session.stream.flush().await.unwrap();
        while session.next().await.expect("startup").0 != b'Z' {}
        session
    }

    async fn send(&mut self, bytes: &[u8]) {
        self.stream.write_all(bytes).await.unwrap();
        self.stream.flush().await.unwrap();
    }

    /// Sends `bytes` and returns the messages up to ReadyForQuery, or up to
    /// the server closing the session.
    async fn exchange(&mut self, bytes: &[u8]) -> Vec<(u8, Vec<u8>)> {
        self.send(bytes).await;
        let mut answer = Vec::new();
        while let Some((tag, body)) = self.next().await {
            answer.push((tag, body));
            if tag == b'Z' {
                break;
            }
        }
        answer
    }

    /// The next message, or `None` once the server is gone.
    async fn next(&mut self) -> Option<(u8, Vec<u8>)> {
        tokio::time::timeout(Duration::from_secs(30), async {
            let tag = self.stream.read_u8().await.ok()?;
            let length = self.stream.read_u32().await.ok()? as usize;
            let mut body = vec![0; length - 4];
            self.stream.read_exact(&mut body).await.ok()?;
            Some((tag, body))
        })
        .await
        .expect("server answered or closed")
    }
}

/// The server binary with the example database, an agent pgwire listener
/// and the agent certificate bound to `inc-436`.
struct Process {
    child: Child,
    agent_pg: SocketAddr,
}

fn write_pem(dir: &Path, name: &str, text: &str) -> PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, text).unwrap();
    path
}

async fn spawn_server(dir: &Path, config: &Json, label: &str) -> Process {
    let file = dir.join(format!("{label}.TEST-ONLY.json"));
    std::fs::write(&file, serde_json::to_vec_pretty(config).unwrap()).unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_giggles-server"))
        .arg(&file)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(std::fs::File::create(dir.join(format!("{label}.stderr.log"))).unwrap())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut output = BufReader::new(child.stdout.take().unwrap());
    let mut line = String::new();
    tokio::time::timeout(Duration::from_secs(60), output.read_line(&mut line))
        .await
        .expect("server ready")
        .unwrap();
    let agent_pg = line
        .split_whitespace()
        .find_map(|part| part.strip_prefix("agent_postgres="))
        .unwrap_or_else(|| panic!("no agent listener in {line}"))
        .parse()
        .unwrap();
    // Keep stdout drained so the child never blocks on it.
    tokio::spawn(async move {
        let mut sink = Vec::new();
        let _ = output.read_to_end(&mut sink).await;
    });
    Process { child, agent_pg }
}

/// The agent reads 2036 rows (eight pages) and the server process is killed
/// as soon as the first row arrived. After a restart every row the agent
/// received is charged: the head covers them, the statement is in the read
/// log, and the restarted server refuses a second full read that the
/// remaining budget no longer covers.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kill_after_page_sent_is_charged_after_restart() {
    const EXTRA: i64 = 2000;
    let total = (VISIBLE + EXTRA) as u64;
    let dir = scratch("r3-315-kill");
    let database = dir.join("db.redb");
    let admin = example_admin(
        &database,
        &Options {
            caps: DisclosureCaps {
                rows_read: 1_000_000,
                rows_returned: total + 64,
                bytes_returned: 100_000_000,
            },
            extra_rows: EXTRA,
            status_fk: false,
        },
    );
    let task = admin.tasks().get(TASK).unwrap().unwrap().id;
    drop(admin);
    let ca = certificate(None, false);
    let server = certificate(Some(&ca), true);
    let agent = certificate(Some(&ca), false);
    let config = json!({
        "database": database,
        "native": "127.0.0.1:0",
        "postgres": "127.0.0.1:0",
        "certificate": write_pem(&dir, "server.pem", &server.cert.pem()),
        "private_key": write_pem(&dir, "server.key.TEST-ONLY", &server.key.serialize_pem()),
        "client_ca": write_pem(&dir, "ca.pem", &ca.cert.pem()),
        "bindings": [],
        "connections": 16,
        "skew_seconds": 0,
        "max_pause_seconds": 3600,
        "agent": {
            "postgres": "127.0.0.1:0",
            "bindings": [serde_json::to_value(binding(&agent, "agent_exec", &[TASK])).unwrap()],
        },
    });
    let sql = "SELECT id, status FROM shipments ORDER BY id";

    let mut first = spawn_server(&dir, &config, "first").await;
    let mut session = RawSession::connect(first.agent_pg, &ca, &agent).await;
    session.send(&query(sql)).await;
    let mut received = 0u64;
    loop {
        let (tag, body) = session.next().await.expect("rows before the kill");
        assert_ne!(tag, b'E', "{}", String::from_utf8_lossy(&body));
        if tag == b'D' {
            received += 1;
            break;
        }
    }
    first.child.start_kill().unwrap();
    first.child.wait().await.unwrap();
    // Whatever else was already on its way is counted too.
    while let Some((tag, _)) = session.next().await {
        if tag == b'D' {
            received += 1;
        }
    }
    assert!(received >= 1);

    // Restart: the charge is durable and covers every row received.
    let reopened = Admin::open(&database, Limits::default()).unwrap();
    let disclosure = reopened.task_disclosure(task).unwrap();
    assert!(disclosure.rows_returned >= received, "{received}: {disclosure:?}");
    assert!(!disclosure.debits.is_empty(), "{disclosure:?}");
    let records = reopened.task_read_log(task).unwrap();
    assert_eq!(records.len(), 1, "{records:?}");
    assert_eq!(records[0].text, sql);
    assert!(
        records[0].outcome == OUTCOME_OPEN || records[0].released_rows >= received,
        "{records:?}"
    );
    let charged = disclosure.rows_returned;
    drop(reopened);

    let mut second = spawn_server(&dir, &config, "second").await;
    let mut session = RawSession::connect(second.agent_pg, &ca, &agent).await;
    let answer = session.exchange(&query(sql)).await;
    // Pages are released one by one: the restarted server sends only what
    // the remaining budget covers, then refuses the next page.
    let cap = total + 64;
    let rows = answer_rows(&answer) as u64;
    assert!(rows <= cap - charged, "{rows} rows after restart, {charged} charged");
    assert!(rows < total, "the whole result again after restart");
    assert_eq!(answer_code(&answer).as_deref(), Some("54T01"), "{rows} rows");
    second.child.start_kill().unwrap();
    second.child.wait().await.unwrap();
    let reopened = Admin::open(&database, Limits::default()).unwrap();
    let after = reopened.task_disclosure(task).unwrap();
    assert!(after.rows_returned <= cap, "{after:?}");
    assert!(after.rows_returned >= charged + rows, "{after:?}");
}

// --- 4. the same result over every wire --------------------------------------

/// The failed rows read once in each format: pgwire text (simple), pgwire
/// binary (tokio-postgres), HTTP JSON and gRPC protobuf. Each answer has the
/// same 31 rows with `customer_email` masked, and each read is charged the
/// same rows and bytes, logged with its own entry point.
#[tokio::test]
async fn same_result_same_charge_text_binary_http_grpc() {
    let example = Example::start(Options::default()).await;
    let expected: Vec<String> = (1..=FAILED).map(|id| id.to_string()).collect();

    let text = example.wire().await.simple(FAILED_ROWS).await;
    assert_eq!(error_of(&text), None, "{text:?}");
    assert_no_secret(&format!("{text:?}"), "text");
    assert_eq!(ids(&text, 0), expected);
    assert!(rows(&text).iter().all(|row| row[4].is_none()));

    let driver = example.driver().await;
    let binary = driver.query(FAILED_ROWS, &[]).await.unwrap();
    assert_eq!(
        binary.iter().map(|r| r.get::<_, i64>(0).to_string()).collect::<Vec<_>>(),
        expected
    );
    assert!(binary.iter().all(|r| r.get::<_, Option<String>>(4).is_none()));
    assert_eq!(binary[0].columns().len(), 5);

    let token = example.http_session().await;
    let body = example.http(&token, &sql_request(FAILED_ROWS)).await;
    assert_eq!(json_code(&body), "ok", "{}", String::from_utf8_lossy(&body));
    assert_no_secret(&String::from_utf8_lossy(&body), "http");
    assert_eq!(json_ids(&body), expected);

    let (mut client, token) = example.grpc().await;
    let reply = exec_grpc(&mut client, &token, &sql_request(FAILED_ROWS)).await;
    assert_eq!(reply.code, "ok");
    assert_no_secret(&String::from_utf8_lossy(&reply.encode_to_vec()), "grpc");
    assert_eq!(json_ids(&wire_json::encode_reply(&reply).unwrap()), expected);

    let records: Vec<_> = example
        .records()
        .into_iter()
        .filter(|r| r.text == FAILED_ROWS)
        .collect();
    assert_eq!(
        records.iter().map(|r| r.entry).collect::<Vec<_>>(),
        [ENTRY_PGWIRE_SIMPLE, ENTRY_PGWIRE_EXTENDED, ENTRY_HTTP, ENTRY_GRPC],
        "{records:?}"
    );
    for record in &records {
        assert_eq!(
            (record.class, record.released_rows, record.masked_values, record.released_bytes),
            (
                class_code(StatementClass::Select),
                FAILED as u64,
                FAILED as u64,
                records[0].released_bytes
            ),
            "{records:?}"
        );
    }
    assert!(records[0].released_bytes > 0);
    let debits = example.disclosure().debits;
    assert!(debits.iter().all(|d| d.rows_returned == FAILED as u64), "{debits:?}");
    assert_eq!(debits.len(), 4, "{debits:?}");
    example.stop().await;
}

// --- 5. wire bytes against the charged cost ----------------------------------

/// Measures, for the 31 failed rows, the bytes each wire format carries
/// against the cost the task is charged (the same on every format), and
/// writes the ratios to the test output and to `$TEMP/r3-315-wire-ratios.txt`
/// for the formula note.
#[tokio::test]
async fn wire_bytes_to_cost_ratio_recorded() {
    let example = Example::start(Options::default()).await;
    let mut wire = example.wire().await;
    let text = wire.exchange_raw(&query(FAILED_ROWS)).await;
    assert_eq!(raw_code(&text), None);
    let mut out = Vec::new();
    parse("", FAILED_ROWS, &mut out);
    bind("", &[], 1, &mut out);
    execute(0, &mut out);
    sync(&mut out);
    let binary = wire.exchange_raw(&out).await;
    assert_eq!(raw_code(&binary), None);
    let token = example.http_session().await;
    let http = example.http(&token, &sql_request(FAILED_ROWS)).await;
    let (mut client, token) = example.grpc().await;
    let grpc = exec_grpc(&mut client, &token, &sql_request(FAILED_ROWS)).await;
    assert_eq!(grpc.code, "ok");

    let records: Vec<_> = example
        .records()
        .into_iter()
        .filter(|r| r.text == FAILED_ROWS)
        .collect();
    assert_eq!(records.len(), 4, "{records:?}");
    let cost = records[0].released_bytes;
    assert!(cost > 0);
    assert!(records.iter().all(|r| r.released_bytes == cost && r.released_rows == FAILED as u64));
    let (text_rows, n) = data_rows(&text);
    assert_eq!(n, FAILED as usize);
    let (binary_rows, n) = data_rows(&binary);
    assert_eq!(n, FAILED as usize);
    let measured = [
        ("pgwire text DataRow bytes", text_rows),
        ("pgwire text whole answer", text.len()),
        ("pgwire binary DataRow bytes", binary_rows),
        ("pgwire binary whole answer", binary.len()),
        ("HTTP JSON body", http.len()),
        ("gRPC protobuf reply", grpc.encoded_len()),
    ];
    let mut report = format!(
        "R3-315 wire/cost ratios: {FAILED} rows, 5 columns (customer_email masked as NULL), \
         charged cost {cost} bytes\n"
    );
    for (name, bytes) in measured {
        assert!(bytes > 0, "{name}");
        let ratio = bytes as f64 / cost as f64;
        assert!(ratio.is_finite() && ratio > 0.0, "{name}");
        report.push_str(&format!("  {name}: {bytes} bytes, ratio {ratio:.3}\n"));
    }
    println!("{report}");
    if let Some(temp) = std::env::var_os("TEMP") {
        let _ = std::fs::write(PathBuf::from(temp).join("r3-315-wire-ratios.txt"), &report);
    }
    example.stop().await;
}

// --- 6. the masking matrix ---------------------------------------------------

/// What a position may answer for a masked column. A hidden column is always
/// `Refused`, with the error of a column that does not exist.
#[derive(Clone, Copy, Debug)]
enum Want {
    /// An error and no row.
    Refused,
    /// An error, or rows whose given column is NULL in every row.
    Mask(&'static [usize]),
    /// An error, or rows whose `id` column (index) is not in e-mail order.
    NoEmailOrder(usize),
    /// An error, or at most one row.
    AtMostOneRow,
    /// An error, or exactly one row whose first value is this text.
    Count(&'static str),
    /// Catalog metadata: no row for a hidden column; any answer for a
    /// masked one (its name is shown, never its values).
    NameOnly,
}

/// One statement template: `{c}` is the column, `{op}` a fresh operation
/// ID and `{id}` a fresh failed row of merchant 42.
const MATRIX: &[(&str, &str, Want)] = &[
    ("projection", "SELECT id, {c} FROM shipments ORDER BY id", Want::Mask(&[1])),
    ("projection", "SELECT {c} FROM shipments", Want::Mask(&[0])),
    ("projection", "SELECT {c}, {c} AS again FROM shipments", Want::Mask(&[0, 1])),
    ("filter", "SELECT id FROM shipments WHERE {c} = 'x'", Want::Refused),
    ("filter", "SELECT id FROM shipments WHERE {c} LIKE 'a%'", Want::Refused),
    ("filter", "SELECT id FROM shipments WHERE {c} IS NOT NULL", Want::Refused),
    ("filter", "SELECT id FROM shipments WHERE {c} > ''", Want::Refused),
    ("filter", "SELECT id FROM shipments WHERE id = 1 OR {c} IN ('x')", Want::Refused),
    ("sort", "SELECT id FROM shipments ORDER BY {c}", Want::Refused),
    ("sort", "SELECT id FROM shipments ORDER BY {c} DESC LIMIT 1", Want::Refused),
    ("sort", "SELECT id, {c} FROM shipments ORDER BY 2", Want::NoEmailOrder(0)),
    ("sort", "SELECT id, {c} AS e FROM shipments ORDER BY e", Want::NoEmailOrder(0)),
    ("sort", "SELECT {c} AS status, id FROM shipments ORDER BY status", Want::NoEmailOrder(1)),
    ("join", "SELECT a.id FROM shipments a JOIN shipments b ON a.{c} = b.{c}", Want::Refused),
    (
        "join",
        "SELECT a.id FROM shipments a, shipments b WHERE a.id = b.id AND b.{c} LIKE 'a%'",
        Want::Refused,
    ),
    ("group", "SELECT count(*) FROM shipments GROUP BY {c}", Want::Refused),
    ("group", "SELECT {c} FROM shipments GROUP BY 1", Want::AtMostOneRow),
    ("group", "SELECT DISTINCT {c} FROM shipments", Want::AtMostOneRow),
    ("group", "SELECT {c}, count(*) FROM shipments GROUP BY {c}", Want::Refused),
    (
        "group",
        "SELECT count(*) FROM shipments GROUP BY id HAVING max({c}) > ''",
        Want::Refused,
    ),
    ("aggregate", "SELECT count({c}) FROM shipments", Want::Refused),
    ("aggregate", "SELECT max({c}) FROM shipments", Want::Refused),
    ("aggregate", "SELECT min({c}) FROM shipments", Want::Refused),
    ("aggregate", "SELECT count(DISTINCT {c}) FROM shipments", Want::Refused),
    ("function", "SELECT lower({c}) FROM shipments", Want::Refused),
    ("function", "SELECT length({c}) FROM shipments", Want::Refused),
    ("function", "SELECT coalesce({c}, 'x') FROM shipments", Want::Refused),
    ("function", "SELECT {c} || 'x' FROM shipments", Want::Refused),
    (
        "function",
        "SELECT CASE WHEN {c} LIKE 'a%' THEN 1 ELSE 0 END FROM shipments",
        Want::Refused,
    ),
    (
        "SET source",
        "OPERATION '{op}' UPDATE shipments SET status = {c} WHERE id = {id}",
        Want::Refused,
    ),
    (
        "SET source",
        "OPERATION '{op}' UPDATE shipments SET status = CASE WHEN {c} LIKE 'a%' THEN 'queued' \
         ELSE status END WHERE id = {id}",
        Want::Refused,
    ),
    (
        "SET source",
        "OPERATION '{op}' UPDATE shipments SET status = 'queued' WHERE id = {id} AND {c} LIKE 'a%'",
        Want::Refused,
    ),
    (
        "SET source",
        "OPERATION '{op}' UPDATE shipments SET {c} = 'x' WHERE id = {id}",
        Want::Refused,
    ),
    (
        "RETURNING",
        "OPERATION '{op}' UPDATE shipments SET status = 'queued' WHERE id = {id} RETURNING {c}",
        Want::Mask(&[0]),
    ),
    (
        "RETURNING",
        "OPERATION '{op}' UPDATE shipments SET status = 'queued' WHERE id = {id} \
         RETURNING id, lower({c})",
        Want::Refused,
    ),
    ("error text", "SELECT {c}::int FROM shipments", Want::Refused),
    ("error text", "SELECT id FROM shipments WHERE {c}::int = 1", Want::Refused),
    (
        "error text",
        "SELECT 1 / (length({c}) - length({c})) FROM shipments",
        Want::Refused,
    ),
    (
        "catalog query",
        "SELECT t.id, t.{c} FROM shipments t JOIN pg_catalog.pg_class k \
         ON k.relname = 'shipments' ORDER BY t.id",
        Want::Mask(&[1]),
    ),
    (
        "catalog query",
        "SELECT count(*) FROM shipments t JOIN pg_catalog.pg_class k \
         ON k.relname = 'shipments' WHERE t.{c} LIKE 'a%'",
        Want::Count("0"),
    ),
    (
        "catalog query",
        "SELECT t.id FROM shipments t JOIN pg_catalog.pg_class k \
         ON k.relname = 'shipments' ORDER BY t.{c}, t.id",
        Want::NoEmailOrder(0),
    ),
    (
        "catalog query",
        "SELECT count(DISTINCT t.{c}) FROM shipments t JOIN pg_catalog.pg_class k \
         ON k.relname = 'shipments'",
        Want::Count("0"),
    ),
    (
        "catalog query",
        "SELECT a.attname FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_class k \
         ON a.attrelid = k.oid WHERE k.relname = 'shipments' AND a.attname = '{c}'",
        Want::NameOnly,
    ),
    ("COPY", "COPY shipments ({c}) TO STDOUT", Want::Refused),
    ("COPY", "COPY (SELECT {c} FROM shipments) TO STDOUT", Want::Refused),
    ("COPY", "COPY (SELECT id FROM shipments ORDER BY {c}) TO STDOUT", Want::Refused),
];

/// Every position of spec section 9, for the masked `customer_email` and the
/// hidden `merchant_id` and `card_token`, over the simple and the extended
/// protocol (and the native API over HTTP for projection, filter and sort):
/// each answer is a refusal or the mask, never a value, an order, a count
/// or an error text that depends on the real values. A hidden column's
/// refusal is the same as a nonexistent column's. All failures are collected
/// and reported together.
#[tokio::test]
async fn masking_matrix() {
    let example = Example::start(Options::default()).await;
    let mut wire = example.wire().await;
    let email_order = email_order();
    let mut failures = Vec::new();
    let mut serial = 0u32;
    let mut next_row = 0i64;
    let before = statuses(example.admin());
    let mut committed = Vec::new();
    for (column, masked) in [
        ("customer_email", true),
        ("merchant_id", false),
        ("card_token", false),
    ] {
        for (position, template, want) in MATRIX {
            for extended in [false, true] {
                serial += 1;
                next_row = next_row % 25 + 1;
                let sql = template
                    .replace("{c}", column)
                    .replace("{op}", &format!("m{serial}"))
                    .replace("{id}", &next_row.to_string());
                let messages = if extended {
                    wire.extended(&sql, &[], 0).await
                } else {
                    wire.simple(&sql).await
                };
                let case = format!(
                    "{position} / {column} / {}: {sql}",
                    if extended { "extended" } else { "simple" }
                );
                let text = format!("{messages:?}");
                for secret in SECRETS {
                    if text.contains(secret) {
                        failures.push(format!("LEAK {case}: `{secret}` in {text}"));
                    }
                }
                let error = error_of(&messages);
                let got = rows(&messages);
                if error.is_some() && !got.is_empty() {
                    failures.push(format!("{case}: rows before an error: {text}"));
                }
                if !masked {
                    if matches!(want, Want::NameOnly) {
                        if error.is_some() || !got.is_empty() {
                            failures.push(format!("{case}: hidden name in the catalog: {text}"));
                        }
                        continue;
                    }
                    let Some(error) = error else {
                        failures.push(format!("{case}: hidden column not refused: {text}"));
                        continue;
                    };
                    // The same statement with a column that does not exist.
                    serial += 1;
                    let absent = template
                        .replace("{c}", "no_such_col")
                        .replace("{op}", &format!("m{serial}"))
                        .replace("{id}", &next_row.to_string());
                    let other = if extended {
                        wire.extended(&absent, &[], 0).await
                    } else {
                        wire.simple(&absent).await
                    };
                    if error_of(&other).as_ref() != Some(&error) {
                        failures.push(format!(
                            "{case}: hidden {error:?} differs from nonexistent {:?}",
                            error_of(&other)
                        ));
                    }
                    continue;
                }
                if error.is_none() && sql.starts_with("OPERATION") {
                    committed.push(next_row);
                }
                let ok = match want {
                    Want::Refused => error.is_some(),
                    _ if error.is_some() => true,
                    Want::Mask(columns) => {
                        !got.is_empty()
                            && got.iter().all(|row| columns.iter().all(|c| row[*c].is_none()))
                    }
                    Want::NoEmailOrder(column) => {
                        let order: Vec<String> = got
                            .iter()
                            .map(|row| row[*column].clone().unwrap_or_default())
                            .collect();
                        order.len() == VISIBLE as usize && order != email_order
                    }
                    Want::AtMostOneRow => got.len() <= 1,
                    Want::Count(count) => {
                        got.len() == 1 && got[0][0].as_deref() == Some(*count)
                    }
                    Want::NameOnly => got.len() <= 1,
                };
                if !ok {
                    failures.push(format!("LEAK {case}: want {want:?}, got {text}"));
                }
            }
        }
    }

    // The native API over HTTP: projection, filter and sort.
    let token = example.http_session().await;
    let real = email(26);
    for (column, masked) in [("customer_email", true), ("card_token", false)] {
        let read = |extra: Json| {
            let mut read = json!({"table": "shipments", "projection": ["id", column],
                "order": [{"field": "id", "descending": false}],
                "limit": {"literal": {"intValue": "100"}}});
            for (key, value) in extra.as_object().unwrap() {
                read[key] = value.clone();
            }
            json!({"operation": {"request": {"read": read}, "bindings": []}})
        };
        for (position, request) in [
            ("native projection", read(json!({}))),
            (
                "native filter",
                read(json!({"filter": {"compare": {"left": {"field": column},
                    "comparison": "NATIVE_COMPARISON_EQ",
                    "right": {"value": {"literal": {"textValue": real}}}}}})),
            ),
            (
                "native sort",
                read(json!({"order": [{"field": column, "descending": false}]})),
            ),
        ] {
            let body = example.http(&token, &request).await;
            let text = String::from_utf8_lossy(&body).into_owned();
            let case = format!("{position} / {column}");
            for secret in SECRETS {
                if text.contains(secret) {
                    failures.push(format!("LEAK {case}: `{secret}` in {text}"));
                }
            }
            let ok = json_code(&body) == "ok";
            let fine = match (position, masked) {
                (_, false) => !ok,
                ("native projection", true) => ok && json_ids(&body).len() == VISIBLE as usize,
                ("native filter", true) => !ok || json_ids(&body).is_empty(),
                _ => !ok || json_ids(&body) != email_order,
            };
            if !fine {
                failures.push(format!("LEAK {case}: {text}"));
            }
        }
    }

    // Nothing changed but rows a successful RETURNING case committed.
    let after = statuses(example.admin());
    for (i, (old, new)) in before.iter().zip(&after).enumerate() {
        let id = i as i64 + 1;
        if old != new && !committed.contains(&id) {
            failures.push(format!("row {id} changed by a refused statement: {old} -> {new}"));
        }
    }
    assert!(
        failures.is_empty(),
        "{} masking failures:\n{}",
        failures.len(),
        failures.join("\n")
    );
    example.stop().await;
}

fn statuses(admin: &Admin) -> Vec<String> {
    admin
        .sql_session()
        .execute("SELECT status FROM shipments ORDER BY id", &[], None)
        .unwrap()
        .rows
        .into_iter()
        .map(|row| format!("{:?}", row.values[0]))
        .collect()
}

// --- 7. foreign-key conflicts ------------------------------------------------

/// `status` references `statuses(code)`, which has no 'queued'. The agent's
/// allowed requeue then fails on the foreign key: the error (every field of
/// the ErrorResponse, on both protocols) names no key value, no row and no
/// other table, and nothing changes.
#[tokio::test]
async fn fk_conflict_error_has_no_key_values() {
    let example = Example::start(Options {
        status_fk: true,
        ..Options::default()
    })
    .await;
    let mut wire = example.wire().await;
    let before = statuses(example.admin());
    for (n, extended) in [false, true].into_iter().enumerate() {
        let sql = format!(
            "OPERATION 'fk-{n}' UPDATE shipments SET status = 'queued' WHERE id = 7"
        );
        let raw = if extended {
            let mut out = Vec::new();
            parse("", &sql, &mut out);
            bind("", &[], 0, &mut out);
            execute(0, &mut out);
            sync(&mut out);
            wire.exchange_raw(&out).await
        } else {
            wire.exchange_raw(&query(&sql)).await
        };
        let code = raw_code(&raw).unwrap_or_else(|| panic!("{sql}: not refused: {raw:?}"));
        // The foreign-key violation itself, from the protected commit.
        assert_eq!(code, "23503", "{sql}");
        let error = frames(&raw)
            .into_iter()
            .find(|(tag, _)| *tag == b'E')
            .map(|(_, body)| String::from_utf8_lossy(body).into_owned())
            .unwrap();
        for forbidden in ["queued", "statuses", "Key (", "(status)", "=(", "id = 7", "failed"] {
            assert!(!error.contains(forbidden), "{sql}: `{forbidden}` in {error}");
        }
        assert_no_secret(&error, &sql);
        assert_eq!(data_rows(&raw).1, 0);
    }
    assert_eq!(statuses(example.admin()), before);
    example.stop().await;
}

// --- 8. the worked example ---------------------------------------------------

/// Steps 1, 2, 3 and 15 of spec section 14 over the agent listener.
#[tokio::test]
async fn worked_example_steps_1_2_3_15() {
    let example = Example::start(Options::default()).await;
    let mut wire = example.wire().await;

    // Step 1: 31 rows; customer_email is NULL; hidden columns are left out;
    // 31 rows debited before release; the statement is in the read log.
    let step_1 = wire.simple(STEP_1).await;
    assert_eq!(error_of(&step_1), None, "{step_1:?}");
    assert_no_secret(&format!("{step_1:?}"), "step 1");
    assert!(
        step_1.contains(&Msg::Columns(
            ["id", "status", "failed_at", "attempt_count", "customer_email"]
                .map(String::from)
                .to_vec()
        )),
        "{step_1:?}"
    );
    let got = rows(&step_1);
    assert_eq!(got.len(), FAILED as usize);
    assert!(got.iter().all(|row| row[1].as_deref() == Some("failed") && row[4].is_none()));
    let mut seen: Vec<i64> = got.iter().map(|row| row[0].as_ref().unwrap().parse().unwrap()).collect();
    seen.sort();
    assert_eq!(seen, (1..=FAILED).collect::<Vec<_>>());
    assert_eq!(complete(&step_1), Some("SELECT 31"));
    let disclosure = example.disclosure();
    assert_eq!(disclosure.debits.len(), 1, "{disclosure:?}");
    assert_eq!(disclosure.debits[0].rows_returned, FAILED as u64);
    assert!(disclosure.rows_returned >= FAILED as u64);
    let records = example.records();
    assert_eq!(records.len(), 1, "{records:?}");
    assert_eq!(
        (records[0].text.as_str(), records[0].released_rows, records[0].masked_values),
        (STEP_1, FAILED as u64, FAILED as u64)
    );
    let after_step_1 = charge(&example.disclosure());

    // Step 2: fails at binding, column hidden; no rows read.
    for extended in [false, true] {
        let sql = "SELECT card_token FROM shipments";
        let messages = if extended { wire.extended(sql, &[], 0).await } else { wire.simple(sql).await };
        // Prepare-time refusals answer with the generic text of their
        // SQLSTATE on both protocols.
        assert_eq!(error_of(&messages).map(|e| e.0), Some("42501".to_owned()), "{messages:?}");
        assert!(rows(&messages).is_empty());
        let absent = "SELECT no_such_col FROM shipments";
        let other = if extended { wire.extended(absent, &[], 0).await } else { wire.simple(absent).await };
        assert_eq!(error_of(&other), error_of(&messages), "hidden and absent differ");
    }
    assert_eq!(charge(&example.disclosure()), after_step_1);

    // Step 3: fails at binding, masked column in a filter; no rows read.
    for extended in [false, true] {
        let sql = "SELECT id FROM shipments WHERE customer_email LIKE 'a%'";
        let messages = if extended { wire.extended(sql, &[], 0).await } else { wire.simple(sql).await };
        // Prepare-time refusals answer with the generic text of their
        // SQLSTATE on both protocols.
        assert_eq!(error_of(&messages).map(|e| e.0), Some("42501".to_owned()), "{messages:?}");
        assert!(rows(&messages).is_empty());
    }
    assert_eq!(charge(&example.disclosure()), after_step_1);

    // Step 15: an update of a merchant 43 row: zero rows visible, nothing
    // changes, and the answer is the same as for a row that does not exist.
    let before = statuses(example.admin());
    for extended in [false, true] {
        let mut answers = Vec::new();
        for (op, id) in [("s15", 100), ("s15-absent", 999_999)] {
            let op = format!("{op}-{extended}");
            let sql = format!("OPERATION '{op}' UPDATE shipments SET status = 'queued' WHERE id = {id}");
            let messages = if extended { wire.extended(&sql, &[], 0).await } else { wire.simple(&sql).await };
            assert_eq!(error_of(&messages), None, "{sql}: {messages:?}");
            assert_eq!(complete(&messages), Some("UPDATE 0"), "{sql}: {messages:?}");
            answers.push(format!("{messages:?}").replace(&op, "OP"));
        }
        assert_eq!(answers[0], answers[1], "a hint that the row exists");
    }
    assert_eq!(statuses(example.admin()), before);
    example.stop().await;
}
