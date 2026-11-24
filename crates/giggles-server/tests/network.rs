// Tests may call the plain exits `clippy.toml` bans in server sources (GD-0061 R3-314).
#![allow(clippy::disallowed_methods)]
#[cfg(feature = "legacy-task")]
use giggles_core::TaskResources as LegacyTaskResources;
#[cfg(not(feature = "legacy-task"))]
#[allow(dead_code)]
struct LegacyTaskResources {
    read_rows: u64,
    disclosed_rows: u64,
    encoded_bytes: u64,
}
#[path = "wire_support/mod.rs"]
mod wire_support;
use wire_support::{bind, cstr, execute, parse, parse_oids, pg_read, pg_send, pg_sync};

use giggles_core::{
    Admin, Limits, relational::Value,
};
use giggles_server::{
    Binding, MAX_FRAME, Role, Running, Service,
    api::{ApiLimits, ApiRuntime, codec, proto},
    clock::Clock,
    serve_pg, tls,
    wire::{
        client::{Client, ClientTls, Credential, WireError},
        frame::{
            Direction, Flags, FrameDecoder, FrameHeader, FrameKind, HEADER_LEN, encode_frame,
            encode_message, write_preface,
        },
        listen::serve_wire_bound,
        messages::{WireConnError, WireReply},
    },
};
use prost::Message;
use rcgen::{
    BasicConstraints, Certificate, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair,
};
use rustls::{ClientConfig, pki_types::PrivatePkcs8KeyDer};
use std::{
    collections::BTreeSet,
    io,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
static SERIAL: AtomicU64 = AtomicU64::new(0);

#[tokio::test(flavor = "current_thread")]
async fn single_runtime_thread_native_and_actual_driver_transactions_progress() {
    let f = Fixture::new().await;
    let mut native = f.native("app").await;
    let (pg, conn) = f.pg(f.app.clone()).await;
    let native_work = async {
        for id in 0..16 {
            assert_eq!(native.execute(sql("BEGIN", vec![])).await.unwrap().code, "ok");
            assert_eq!(
                native
                    .execute(sql(
                        "INSERT INTO app (id, body) VALUES ($1, $2)",
                        vec![Value::Int(id), Value::Text("discard".into())]
                    ))
                    .await
                    .unwrap()
                    .code,
                "ok"
            );
            assert_eq!(
                native.execute(sql("ROLLBACK", vec![])).await.unwrap().code,
                "ok"
            );
        }
    };
    let pg_work = async {
        for id in 100i64..116 {
            pg.execute(
                "INSERT INTO app (id, body) VALUES ($1, $2)",
                &[&id, &"kept"],
            )
            .await
            .unwrap();
            let rows = pg
                .query("SELECT body FROM app WHERE id = $1", &[&id])
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].get::<_, String>(0), "kept");
        }
    };
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        tokio::join!(native_work, pg_work);
    })
    .await
    .unwrap();
    assert!(
        pg.query("SELECT id FROM app WHERE id = 0", &[])
            .await
            .unwrap()
            .is_empty()
    );
    drop(pg);
    conn.await.unwrap();
    drop(native);
    f.stop().await;
}
fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}
struct Cert {
    cert: Certificate,
    key: KeyPair,
}
fn cert(ca: Option<&Cert>, server: bool) -> Cert {
    let key = KeyPair::generate().unwrap();
    let mut p = CertificateParams::new(vec!["localhost".into()]).unwrap();
    if ca.is_none() {
        p.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    } else {
        p.extended_key_usages = vec![if server {
            ExtendedKeyUsagePurpose::ServerAuth
        } else {
            ExtendedKeyUsagePurpose::ClientAuth
        }];
    }
    let cert = if let Some(ca) = ca {
        p.signed_by(&key, &ca.cert, &ca.key).unwrap()
    } else {
        p.self_signed(&key).unwrap()
    };
    Cert { cert, key }
}
fn client(c: &Cert, ca: &Cert) -> Arc<ClientConfig> {
    tls::client(
        vec![c.cert.der().clone()],
        PrivatePkcs8KeyDer::from(c.key.serialize_der()).into(),
        vec![ca.cert.der().clone()],
    )
    .unwrap()
}

fn wire_tls(path: &Path, identity: Option<&str>) -> io::Result<ClientTls> {
    Ok(ClientTls {
        ca: std::fs::read(path.join("ca.pem"))?,
        cert: identity
            .map(|name| std::fs::read(path.join(format!("{name}.pem"))))
            .transpose()?,
        key: identity
            .map(|name| std::fs::read(path.join(format!("{name}.key.TEST-ONLY"))))
            .transpose()?,
        server_name: "localhost".into(),
    })
}

async fn connect_wire(address: SocketAddr, tls: ClientTls) -> io::Result<Client> {
    Client::connect(
        address,
        Some(tls),
        proto::OpenSessionRequest::default(),
        Credential::None,
    )
    .await
    .map(|(client, _)| client)
}

async fn start_wire(
    service: Arc<Service>,
    config: Arc<rustls::ServerConfig>,
    connections: usize,
) -> Running {
    let runtime = ApiRuntime::new(service, ApiLimits::default()).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (_gate, start) = tokio::sync::watch::channel(true);
    serve_wire_bound(listener, runtime, config, connections, start)
        .await
        .unwrap()
}

struct Fixture {
    path: PathBuf,
    admin: Admin,
    native: Running,
    pg: Running,
    app: Arc<ClientConfig>,
    task: Arc<ClientConfig>,
    other: Arc<ClientConfig>,
    unbound: Arc<ClientConfig>,
    server_config: Arc<rustls::ServerConfig>,
    service: Arc<Service>,
    ca: Cert,
}
impl Fixture {
    async fn new() -> Self {
        Self::with_task_resources(LegacyTaskResources {
            read_rows: 1_000_000,
            disclosed_rows: 1_000_000,
            encoded_bytes: 1_000_000_000,
        })
        .await
    }
    async fn with_task_resources(_task_limits: LegacyTaskResources) -> Self {
        Self::with_app_table(
            "app",
            &["CREATE TABLE app (id BIGINT PRIMARY KEY, body TEXT, flag BOOLEAN, small SMALLINT, num INTEGER, data BYTEA)"],
        )
        .await
    }
    async fn with_app_table(table: &str, setup: &[&str]) -> Self {
        Self::with_app_table_engine(table, setup, giggles_core::Engine::Legacy).await
    }
    async fn with_app_table_engine(table: &str, setup: &[&str], engine: giggles_core::Engine) -> Self {
        let path = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!(
            "network-fixture-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            SERIAL.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).unwrap();
        println!("retained_fixture={}", path.display());
        let ca = cert(None, false);
        let server = cert(Some(&ca), true);
        let app = cert(Some(&ca), false);
        let task = cert(Some(&ca), false);
        let other = cert(Some(&ca), false);
        let unbound = cert(Some(&ca), false);
        let bad_ca = cert(None, false);
        let wrong = cert(Some(&bad_ca), false);
        for (name, c) in [
            ("ca", &ca),
            ("server", &server),
            ("app", &app),
            ("task", &task),
            ("other", &other),
            ("unbound", &unbound),
            ("wrong", &wrong),
        ] {
            std::fs::write(path.join(format!("{name}.pem")), c.cert.pem()).unwrap();
            std::fs::write(
                path.join(format!("{name}.key.TEST-ONLY")),
                c.key.serialize_pem(),
            )
            .unwrap();
        }
        std::fs::write(path.join("FIXTURES-ONLY.txt"),"Generated local TEST certificates and private keys. Not deployment credentials. Retained intentionally.\n").unwrap();
        let admin = Admin::create_engine(path.join("db.redb"), Limits::default(), engine).unwrap();
        let mut sql = admin.sql_session();
        for ddl in setup {
            sql.execute(ddl, &[], None).unwrap();
        }
        sql.execute(
            "CREATE TABLE other (id BIGINT PRIMARY KEY, body TEXT)",
            &[],
            None,
        )
        .unwrap();
        drop(sql);
        let domain = admin.authority_metadata().unwrap().0.to_string();
        let bindings = vec![
            Binding {
                fingerprint: tls::fingerprint(app.cert.der()),
                role: Role::Application {
                    domain: domain.clone(),
                    tables: BTreeSet::from([table.into()]),
                },
            },
            Binding {
                fingerprint: tls::fingerprint(task.cert.der()),
                role: Role::NoSql {
                    domain: domain.clone(),
                },
            },
            Binding {
                fingerprint: tls::fingerprint(other.cert.der()),
                role: Role::NoSql { domain },
            },
        ];
        std::fs::write(
            path.join("bindings.json"),
            serde_json::to_vec_pretty(&bindings).unwrap(),
        )
        .unwrap();
        let service = Service::new(
            admin.clone(),
            bindings.clone(),
            Clock::new(0, 3600).unwrap(),
        )
        .unwrap();
        let server_config = tls::server(
            vec![server.cert.der().clone()],
            PrivatePkcs8KeyDer::from(server.key.serialize_der()).into(),
            vec![ca.cert.der().clone()],
        )
        .unwrap();
        let native = start_wire(service.clone(), server_config.clone(), 32).await;
        let pg = serve_pg(
            "127.0.0.1:0".parse().unwrap(),
            service.clone(),
            server_config.clone(),
            32,
        )
        .await
        .unwrap();
        let config = serde_json::json!({"database":path.join("db.redb"),"wire":"127.0.0.1:7432","postgres":"127.0.0.1:7433","certificate":path.join("server.pem"),"private_key":path.join("server.key.TEST-ONLY"),"client_ca":path.join("ca.pem"),"bindings":bindings,"connections":32,"skew_seconds":0,"max_pause_seconds":3600});
        std::fs::write(
            path.join("server.TEST-ONLY.json"),
            serde_json::to_vec_pretty(&config).unwrap(),
        )
        .unwrap();
        Self {
            path,
            admin,
            native,
            pg,
            app: client(&app, &ca),
            task: client(&task, &ca),
            other: client(&other, &ca),
            unbound: client(&unbound, &ca),
            server_config,
            service,
            ca,
        }
    }
    async fn try_native(&self, identity: &str) -> io::Result<Client> {
        connect_wire(self.native.address, wire_tls(&self.path, Some(identity))?).await
    }
    async fn native(&self, identity: &str) -> Client {
        self.try_native(identity).await.unwrap()
    }
    async fn pg(
        &self,
        config: Arc<ClientConfig>,
    ) -> (tokio_postgres::Client, tokio::task::JoinHandle<()>) {
        let mut c = tokio_postgres::Config::new();
        c.host("localhost")
            .hostaddr(self.pg.address.ip())
            .port(self.pg.address.port())
            .user("giggles")
            .dbname("giggles")
            .ssl_mode(tokio_postgres::config::SslMode::Require);
        let (client, conn) = c
            .connect(tokio_postgres_rustls::MakeRustlsConnect::new(
                (*config).clone(),
            ))
            .await
            .unwrap();
        (
            client,
            tokio::spawn(async move {
                let _ = conn.await;
            }),
        )
    }
    async fn stop(self) {
        self.native.shutdown().await.unwrap();
        self.pg.shutdown().await.unwrap();
    }
}



fn sql(sql: &str, params: Vec<Value>) -> proto::ExecuteRequest {
    proto::ExecuteRequest {
        request: Some(proto::execute_request::Request::Sql(proto::SqlRequest {
            sql: sql.into(),
            params: params.into_iter().map(codec::encode_value).collect(),
        })),
        ..Default::default()
    }
}

fn ping() -> proto::ExecuteRequest {
    proto::ExecuteRequest {
        request: Some(proto::execute_request::Request::Ping(proto::PingRequest {})),
        ..Default::default()
    }
}

fn row_values(row: &proto::SqlRow) -> Vec<Value> {
    row.values
        .iter()
        .cloned()
        .map(|value| codec::decode_value(value).unwrap())
        .collect()
}

fn wire_error(error: &io::Error) -> &WireError {
    error
        .get_ref()
        .and_then(|error| error.downcast_ref())
        .unwrap_or_else(|| panic!("expected a wire connection error, got {error:?}"))
}

fn listening_address<'a>(line: &'a str, protocol: &str) -> &'a str {
    line.split_whitespace()
        .filter_map(|field| field.split_once('='))
        .find_map(|(name, address)| (name == protocol).then_some(address))
        .unwrap()
}

fn code(e: tokio_postgres::Error) -> String {
    e.code().unwrap().code().into()
}



#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pinned_driver_extended_binary_types_describe_and_rollback() {
    let f = Fixture::new().await;
    let (mut c, conn) = f.pg(f.app.clone()).await;
    let insert = c
        .prepare("INSERT INTO app VALUES ($1,$2,$3,$4,$5,$6)")
        .await
        .unwrap();
    // Prepare/Describe must not execute the write, even when executed repeatedly.
    assert!(
        f.admin
            .sql_session()
            .execute("SELECT * FROM app LIMIT 10", &[], None)
            .unwrap()
            .rows
            .is_empty()
    );
    assert_eq!(
        insert.params(),
        &[
            tokio_postgres::types::Type::INT8,
            tokio_postgres::types::Type::TEXT,
            tokio_postgres::types::Type::BOOL,
            tokio_postgres::types::Type::INT2,
            tokio_postgres::types::Type::INT4,
            tokio_postgres::types::Type::BYTEA
        ]
    );
    let body = "nul\0 unicode Î»";
    let bytes = vec![0u8, 255, 1];
    assert_eq!(
        c.execute(
            &insert,
            &[&-9i64, &body, &true, &i16::MIN, &i32::MAX, &bytes]
        )
        .await
        .unwrap(),
        1
    );
    let select = c.prepare("SELECT * FROM app WHERE id=$1").await.unwrap();
    let row = c.query_one(&select, &[&-9i64]).await.unwrap();
    assert_eq!(row.get::<_, i64>(0), -9);
    assert_eq!(row.get::<_, String>(1), body);
    assert!(row.get::<_, bool>(2));
    assert_eq!(row.get::<_, i16>(3), i16::MIN);
    assert_eq!(row.get::<_, i32>(4), i32::MAX);
    assert_eq!(row.get::<_, Vec<u8>>(5), bytes);
    let tx = c.transaction().await.unwrap();
    tx.execute("UPDATE app SET body=$1 WHERE id=$2", &[&"changed", &-9i64])
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    assert_eq!(
        c.query_one(&select, &[&-9i64])
            .await
            .unwrap()
            .get::<_, String>(1),
        body
    );
    c.batch_execute("BEGIN").await.unwrap();
    assert_eq!(
        code(
            c.execute("INSERT INTO app (id) VALUES ($1)", &[&-9i64])
                .await
                .unwrap_err()
        ),
        "23505"
    );
    assert_eq!(
        code(c.query(&select, &[&-9i64]).await.unwrap_err()),
        "25P02"
    );
    c.batch_execute("ROLLBACK").await.unwrap();
    let null: Option<&str> = None;
    c.execute("INSERT INTO app (id,body) VALUES ($1,$2)", &[&2i64, &null])
        .await
        .unwrap();
    assert!(
        c.query_one("SELECT body FROM app WHERE id=$1", &[&2i64])
            .await
            .unwrap()
            .get::<_, Option<String>>(0)
            .is_none()
    );
    {
        // PG18.6: bigint = float8 via implicit cast (DD-0068 F13)
        let statement = c
            .prepare_typed(
                "SELECT id FROM app WHERE id=$1",
                &[tokio_postgres::types::Type::FLOAT8],
            )
            .await
            .unwrap();
        assert_eq!(statement.params(), &[tokio_postgres::types::Type::FLOAT8]);
        assert_eq!(
            c.query_one(&statement, &[&2.0f64])
                .await
                .unwrap()
                .get::<_, i64>(0),
            2
        );
        assert!(c.query(&statement, &[&2.5f64]).await.unwrap().is_empty());
    }
    let copy = c
        .copy_in::<_, bytes::Bytes>("COPY app FROM STDIN")
        .await
        .unwrap();
    tokio::pin!(copy);
    assert_eq!(copy.as_mut().finish().await.unwrap(), 0);
    // STDOUT formats are covered by raw-wire pg_dump tests.
    let copy = c.copy_in::<_, bytes::Bytes>("COPY app FROM STDIN WITH (FORMAT csv, HEADER true)").await.unwrap();
    tokio::pin!(copy);
    assert_eq!(copy.as_mut().finish().await.unwrap(), 0);
    for query in [
        "COPY app TO 'file' WITH (FORMAT csv)",
        "COPY app FROM 'file'",
    ] {
        assert_eq!(
            code(c.simple_query(query).await.unwrap_err()),
            "42501",
            "{query}"
        );
    }
    for query in [
        "SET ROLE admin",
        "SELECT * FROM other LIMIT 1",
        "SELECT * FROM pg_catalog LIMIT 1",
        "CREATE TABLE evil (id BIGINT PRIMARY KEY)",
        "BEGIN; COMMIT",
    ] {
        assert!(c.simple_query(query).await.is_err(), "{query}");
    }
    drop(c);
    conn.await.unwrap();
    f.stop().await;
}






#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn authentication_tls_downgrade_unknown_certificate_and_spoof() {
    let f = Fixture::new().await;
    assert!(f.try_native("wrong").await.is_err());
    let unbound = f.try_native("unbound").await.err().expect("unbound certificate");
    assert_eq!(wire_error(&unbound).code, "UNAUTHENTICATED");
    assert!(
        connect_wire(
            f.native.address,
            ClientTls {
                server_name: "wrong-host".into(),
                ..wire_tls(&f.path, Some("app")).unwrap()
            },
        )
        .await
        .is_err()
    );
    assert!(
        connect_wire(f.native.address, wire_tls(&f.path, None).unwrap())
            .await
            .is_err()
    );
    let mut plain = TcpStream::connect(f.pg.address).await.unwrap();
    plain.write_all(&[0, 0, 0, 8, 0, 3, 0, 0]).await.unwrap();
    assert!(plain.read_u8().await.is_err());
    for identity in ["task", "other"] {
        let mut client = f.native(identity).await;
        assert_eq!(client.execute(ping()).await.unwrap().code, "ok");
        assert_eq!(
            client.execute(sql("SELECT id FROM app", vec![])).await.unwrap().code,
            "42501"
        );
    }
    // Protobuf-valid authority probes (the legacy JSON `principal`/`time`/`task`
    // fields have no wire form: the wire carries authority only in the
    // certificate-bound session). A NoSql identity sending the task-prototype
    // requests must be refused, stay refused, and keep its certificate role.
    let mut probe = f.native("task").await;
    for request in task_prototype_probes() {
        let refused = probe.execute(request).await.unwrap_err();
        assert_eq!(wire_error(&refused).code, "INVALID_ARGUMENT");
    }
    // The refusals are request-scoped: the session keeps its certificate role
    // (ping only) and no probe selected other authority.
    assert_eq!(probe.execute(ping()).await.unwrap().code, "ok");
    assert_eq!(
        probe.execute(sql("SELECT id FROM app", vec![])).await.unwrap().code,
        "42501"
    );
    drop(probe);
    // Lost assertion (GD-0091 Stack 5a): the legacy JSON listener rejected
    // extra `principal`/`time`/`task` keys (deny_unknown_fields, 08P01). The
    // wire has no such keys, so the replacement is the protobuf-valid probes
    // above plus these undecodable EXECUTE bodies, which must be refused
    // request-by-request without closing the connection. The wire authority
    // tests are wire_basic::wire_agent_forms_only_on_agent_listener and
    // wire_identity_rechecked_after_binding_change.
    let mut tls_stream = raw_tls(&f, f.task.clone(), false).await;
    raw_wire_open(&mut tls_stream).await;
    for (offset, json) in [
        r#"{"kind":"Status","operation":1,"principal":3}"#,
        r#"{"kind":"Status","operation":1,"time":{"Certified":0}}"#,
        r#"{"kind":"Status","operation":1,"task":2}"#,
        r#"{"kind":"grant","task":3}"#,
    ]
    .into_iter()
    .enumerate()
    {
        let id = offset as u32 + 2;
        raw_wire_execute(&mut tls_stream, id, json.as_bytes()).await;
        assert_eq!(
            raw_wire_error(&mut tls_stream, id).await.code,
            "INVALID_ARGUMENT"
        );
    }
    drop(tls_stream);
    f.stop().await;
}

async fn raw_wire_open(stream: &mut (impl tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin)) {
    let mut request = Vec::new();
    write_preface(&mut request);
    encode_message(
        &mut request,
        FrameKind::Hello,
        Flags(0),
        1,
        &proto::OpenSessionRequest::default(),
    );
    stream.write_all(&request).await.unwrap();
    let (header, _) = raw_wire_read(stream).await.unwrap();
    assert_eq!((header.kind, header.request_id), (FrameKind::Ready, 1));
}

async fn raw_wire_execute(stream: &mut (impl tokio::io::AsyncWrite + Unpin), id: u32, body: &[u8]) {
    let mut frame = Vec::new();
    encode_frame(&mut frame, FrameKind::Execute, Flags(0), id, body);
    stream.write_all(&frame).await.unwrap();
}

async fn raw_wire_read(
    stream: &mut (impl tokio::io::AsyncRead + Unpin),
) -> io::Result<(FrameHeader, Vec<u8>)> {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let mut header = [0; HEADER_LEN];
        stream.read_exact(&mut header).await?;
        let mut decoder = FrameDecoder::new(Direction::ServerToClient, MAX_FRAME);
        decoder.read_buf().extend_from_slice(&header);
        let decode_error = |error| io::Error::new(io::ErrorKind::InvalidData, format!("{error:?}"));
        if let Some((header, _)) = decoder.next_frame().map_err(decode_error)? {
            return Ok((header, Vec::new()));
        }
        // The decoder already validated the length and reserved the body buffer.
        let body_len = u32::from_be_bytes(header[..4].try_into().unwrap()) as usize - (HEADER_LEN - 4);
        decoder.read_buf().resize(HEADER_LEN + body_len, 0);
        stream.read_exact(&mut decoder.read_buf()[HEADER_LEN..]).await?;
        let (header, body) = decoder.next_frame().map_err(decode_error)?.unwrap();
        Ok((header, decoder.body(body).to_vec()))
    })
    .await?
}

async fn raw_wire_error(
    stream: &mut (impl tokio::io::AsyncRead + Unpin),
    id: u32,
) -> WireConnError {
    let (header, body) = raw_wire_read(stream).await.unwrap();
    assert_eq!(
        (header.kind, header.flags, header.request_id),
        (FrameKind::ConnError, Flags(0), id)
    );
    WireConnError::decode(body.as_slice()).unwrap()
}

fn wire_header_with_length(length: u32) -> [u8; HEADER_LEN] {
    let mut header = [0; HEADER_LEN];
    header[..4].copy_from_slice(&length.to_be_bytes());
    header[4] = FrameKind::Execute as u8;
    header[8..].copy_from_slice(&2u32.to_be_bytes());
    header
}

async fn raw_tls(
    f: &Fixture,
    config: Arc<ClientConfig>,
    pg: bool,
) -> tokio_rustls::client::TlsStream<TcpStream> {
    let mut socket = TcpStream::connect(if pg { f.pg.address } else { f.native.address })
        .await
        .unwrap();
    if pg {
        socket
            .write_all(&[0, 0, 0, 8, 4, 210, 22, 47])
            .await
            .unwrap();
        assert_eq!(socket.read_u8().await.unwrap(), b'S');
    }
    tokio_rustls::TlsConnector::from(config)
        .connect(
            rustls::pki_types::ServerName::try_from("localhost").unwrap(),
            socket,
        )
        .await
        .unwrap()
}



#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn disconnect_reconnect_pool_isolation_and_statement_fences() {
    let f = Fixture::new().await;
    let (c, conn) = f.pg(f.app.clone()).await;
    c.batch_execute("BEGIN").await.unwrap();
    c.execute(
        "INSERT INTO app (id,body) VALUES ($1,$2)",
        &[&1i64, &"uncommitted"],
    )
    .await
    .unwrap();
    let p = c.prepare("SELECT body FROM app WHERE id=$1").await.unwrap();
    c.batch_execute("ROLLBACK").await.unwrap();
    c.batch_execute("BEGIN").await.unwrap();
    // Ordinary prepared statements survive transaction boundaries, while
    // rolled-back effects must remain absent from their later results.
    assert!(c.query(&p, &[&1i64]).await.unwrap().is_empty());
    c.batch_execute("ROLLBACK").await.unwrap();
    c.batch_execute("BEGIN").await.unwrap();
    c.execute("INSERT INTO app (id) VALUES ($1)", &[&2i64])
        .await
        .unwrap();
    drop(c);
    conn.await.unwrap();
    let (next, conn2) = f.pg(f.app.clone()).await;
    assert!(
        next.query("SELECT * FROM app LIMIT 10", &[])
            .await
            .unwrap()
            .is_empty()
    );
    next.execute("INSERT INTO app (id) VALUES ($1)", &[&2i64])
        .await
        .unwrap();
    drop(next);
    conn2.await.unwrap();
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn malformed_oversize_frames_and_connection_exhaustion() {
    let f = Fixture::new().await;
    let max_length = (MAX_FRAME + HEADER_LEN - 4) as u32;
    for n in [0, u32::MAX, max_length + 1] {
        let mut s = raw_tls(&f, f.task.clone(), false).await;
        raw_wire_open(&mut s).await;
        s.write_all(&wire_header_with_length(n)).await.unwrap();
        assert_eq!(raw_wire_error(&mut s, 0).await.code, "protocol");
        assert!(connection_closed(&mut s).await);
    }
    let mut decoder = FrameDecoder::new(Direction::ClientToServer, 256);
    decoder
        .read_buf()
        .extend_from_slice(&wire_header_with_length(u32::MAX));
    assert!(decoder.next_frame().is_err());
    assert!(giggles_server::clock::interval(u64::MAX, 1).is_err());
    assert!(giggles_server::clock::interval(0, 1).is_err());
    let running = start_wire(f.service.clone(), f.server_config.clone(), 1).await;
    let mut first = connect_wire(running.address, wire_tls(&f.path, Some("task")).unwrap())
        .await
        .unwrap();
    assert_eq!(first.execute(ping()).await.unwrap().code, "ok");
    assert!(
        connect_wire(running.address, wire_tls(&f.path, Some("task")).unwrap())
            .await
            .is_err()
    );
    drop(first);
    running.shutdown().await.unwrap();
    f.stop().await;
}

#[test]
fn durable_clock_restart_backward_uncertain_and_maximum_bounds() {
    let path =
        PathBuf::from("artifacts").join(format!("clock-fixture-{}-{}", std::process::id(), now()));
    std::fs::create_dir_all(&path).unwrap();
    let admin = Admin::create(path.join("db.redb"), Limits::default()).unwrap();
    admin.certify_server_time(u64::MAX, u64::MAX).unwrap();
    drop(admin);
    let admin = Admin::open(path.join("db.redb"), Limits::default()).unwrap();
    assert!(Clock::new(0, 3600).unwrap().now(&admin).is_err());
    assert!(admin.certify_server_time(1, 2).is_err());
    assert!(admin.certify_server_time(3, 2).is_err());
    assert!(Clock::new(61, 1).is_err());
    assert!(Clock::new(0, 0).is_err());
}

async fn raw_pg(f: &Fixture) -> tokio_rustls::client::TlsStream<TcpStream> {
    let mut s = raw_tls(f, f.app.clone(), true).await;
    let mut startup = 196608u32.to_be_bytes().to_vec();
    startup.extend(b"user\0giggles\0database\0giggles\0\0");
    s.write_u32((startup.len() + 4) as u32).await.unwrap();
    s.write_all(&startup).await.unwrap();
    while pg_read(&mut s).await.0 != b'Z' {}
    s
}
#[tokio::test]
async fn startup_rejections_explain_fixed_requirements_without_echoing_input() {
    let f = Fixture::new().await;
    for (fields, code, message) in [
        (
            "user\0private-user\0database\0giggles\0\0",
            "28000",
            "startup user must be giggles",
        ),
        (
            "user\0giggles\0database\0private-db\0\0",
            "28000",
            "startup database must be giggles",
        ),
        (
            "database\0giggles\0\0",
            "28000",
            "startup user must be giggles",
        ),
        (
            "user\0giggles\0\0",
            "28000",
            "startup database must be giggles",
        ),
        (
            "user\0giggles\0database\0giggles\0options\0private-options\0\0",
            "0A000",
            "unsupported startup parameter",
        ),
        (
            "user\0giggles\0database\0giggles\0client_encoding\0private-encoding\0\0",
            "0A000",
            "startup client_encoding must be UTF8",
        ),
        (
            "user\0giggles\0user\0private-duplicate\0\0",
            "08P01",
            "malformed PostgreSQL startup packet",
        ),
    ] {
        let mut s = raw_tls(&f, f.app.clone(), true).await;
        let mut startup = 196608u32.to_be_bytes().to_vec();
        startup.extend(fields.as_bytes());
        s.write_u32((startup.len() + 4) as u32).await.unwrap();
        s.write_all(&startup).await.unwrap();
        let (tag, body) = pg_read(&mut s).await;
        assert_eq!(tag, b'E');
        let text = String::from_utf8(body).unwrap();
        assert!(text.contains(code), "{text:?}");
        assert!(text.contains(message), "{text:?}");
        assert!(!text.contains("private-"), "input leaked: {text:?}");
        assert!(s.read_u8().await.is_err());
    }
    // Failed startup must not poison valid authenticated connections.
    let mut valid = raw_pg(&f).await;
    pg_send(&mut valid, b'X', &[]).await;
    f.stop().await;
}
#[tokio::test]
async fn jdbc_startup_presentation_hints_are_bounded_and_do_not_grant_authority() {
    let f = Fixture::new().await;
    for (hints, accepted) in [
        (
            "DateStyle\0ISO\0TimeZone\0America/Chicago\0extra_float_digits\x003\0",
            true,
        ),
        (
            "DateStyle\0iso\0TimeZone\0GMT-05:00\0extra_float_digits\x002\0",
            true,
        ),
        ("TimeZone\0UTC\0extra_float_digits\0-15\0", true),
        ("DateStyle\0SQL\0", true),
        ("TimeZone\0\0", false),
        ("TimeZone\0UTC\nforged-log\0", false),
        ("extra_float_digits\x004\0", false),
        ("extra_float_digits\0-16\0", false),
        ("extra_float_digits\0NaN\0", false),
        ("search_path\0public\0", false),
        ("replication\0true\0", false),
        ("options\0-c role=admin\0", false),
    ] {
        let mut s = raw_tls(&f, f.app.clone(), true).await;
        let mut startup = 196608u32.to_be_bytes().to_vec();
        startup.extend(b"user\0giggles\0database\0giggles\0");
        startup.extend(hints.as_bytes());
        startup.push(0);
        s.write_u32((startup.len() + 4) as u32).await.unwrap();
        s.write_all(&startup).await.unwrap();
        let (tag, body) = pg_read(&mut s).await;
        if accepted {
            assert_eq!(tag, b'R');
            let mut statuses = Vec::new();
            loop {
                let (tag, body) = pg_read(&mut s).await;
                if tag == b'S' {
                    statuses.push(String::from_utf8(body).unwrap());
                }
                if tag == b'Z' {
                    break;
                }
            }
            let date_style = if hints.starts_with("DateStyle\0SQL\0") {
                "DateStyle\0SQL, MDY\0"
            } else {
                "DateStyle\0ISO, MDY\0"
            };
            assert!(statuses.iter().any(|v| v == date_style));
            let expected_timezone = statuses
                .iter()
                .find_map(|v| v.strip_prefix("TimeZone\0"))
                .unwrap()
                .trim_end_matches('\0');
            pg_send(&mut s, b'Q', b"SHOW TimeZone\0").await;
            let mut returned = false;
            loop {
                let (tag, body) = pg_read(&mut s).await;
                assert_ne!(tag, b'E');
                if tag == b'D' {
                    assert_eq!(&body[..2], &1u16.to_be_bytes());
                    let length = i32::from_be_bytes(body[2..6].try_into().unwrap()) as usize;
                    assert_eq!(&body[6..6 + length], expected_timezone.as_bytes());
                    returned = true;
                }
                if tag == b'Z' {
                    break;
                }
            }
            assert!(returned);

            pg_send(&mut s, b'Q', b"SELECT id FROM app WHERE id=1\0").await;
            loop {
                let (tag, _) = pg_read(&mut s).await;
                assert_ne!(tag, b'E');
                if tag == b'Z' {
                    break;
                }
            }
            pg_send(&mut s, b'Q', b"SELECT id FROM forbidden WHERE id=1\0").await;
            assert_eq!(pg_read(&mut s).await.0, b'E');
        } else {
            assert_eq!(tag, b'E');
            assert!(String::from_utf8(body).unwrap().contains("0A000"));
        }
    }
    f.stop().await;
}
#[tokio::test]
async fn jdbc_varchar_and_integer_wire_widths_preserve_core_range_checks() {
    let f = Fixture::new().await;
    let mut s = raw_pg(&f).await;
    pg_send(
        &mut s,
        b'P',
        &parse_oids(
            "jdbc",
            "INSERT INTO app (id,body) VALUES ($1,$2)",
            &[23, 1043],
        ),
    )
    .await;
    pg_send(&mut s, b'D', b"Sjdbc\0").await;
    let frames = pg_sync(&mut s).await;
    assert_eq!(
        frames[1],
        (
            b't',
            [
                2u16.to_be_bytes().as_slice(),
                23u32.to_be_bytes().as_slice(),
                1043u32.to_be_bytes().as_slice()
            ]
            .concat()
        )
    );
    let mut bound = b"\0jdbc\0".to_vec();
    bound.extend(1u16.to_be_bytes());
    bound.extend(1u16.to_be_bytes());
    bound.extend(2u16.to_be_bytes());
    bound.extend(4u32.to_be_bytes());
    bound.extend(77i32.to_be_bytes());
    bound.extend(3u32.to_be_bytes());
    bound.extend(b"abc");
    bound.extend(0u16.to_be_bytes());
    pg_send(&mut s, b'B', &bound).await;
    pg_send(&mut s, b'E', &execute("", 0)).await;
    let frames = pg_sync(&mut s).await;
    assert!(!frames.iter().any(|f| f.0 == b'E'));
    pg_send(
        &mut s,
        b'P',
        &parse_oids(
            "range",
            "INSERT INTO app (id,small) VALUES ($1,$2)",
            &[20, 20],
        ),
    )
    .await;
    pg_send(
        &mut s,
        b'B',
        &bind(
            "",
            "range",
            &[Some("78"), Some("9223372036854775807")],
            false,
        ),
    )
    .await;
    pg_send(&mut s, b'E', &execute("", 0)).await;
    assert!(pg_sync(&mut s).await.iter().any(|f| f.0 == b'E'));
    drop(s);
    let (client, connection) = f.pg(f.app.clone()).await;
    let rows = client
        .query("SELECT body FROM app WHERE id=77", &[])
        .await
        .unwrap();
    assert_eq!(rows[0].get::<_, String>(0), "abc");
    assert!(
        client
            .query("SELECT id FROM app WHERE id=78", &[])
            .await
            .unwrap()
            .is_empty()
    );
    drop(client);
    connection.await.unwrap();
    f.stop().await;
}
fn has_code(frames: &[(u8, Vec<u8>)], code: &str) -> bool {
    frames
        .iter()
        .any(|(t, b)| *t == b'E' && b.windows(code.len()).any(|w| w == code.as_bytes()))
}

#[tokio::test]
async fn empty_extended_query_validates_connections_without_sql_or_transaction_effects() {
    let f = Fixture::new().await;
    let mut s = raw_pg(&f).await;
    for sql in [
        "",
        " \t\r\n",
        ";;",
        "-- empty\n",
        "/* nested /* empty */ */;",
    ] {
        pg_send(&mut s, b'P', &parse("", sql)).await;
        pg_send(&mut s, b'B', &bind("", "", &[], false)).await;
        pg_send(&mut s, b'D', b"S\0").await;
        pg_send(&mut s, b'D', b"P\0").await;
        pg_send(&mut s, b'E', &execute("", 0)).await;
        let frames = pg_sync(&mut s).await;
        assert_eq!(frames.iter().map(|f| f.0).collect::<Vec<_>>(), b"12tnnIZ");
        assert_eq!(frames.last().unwrap().1, b"I");
    }
    pg_send(&mut s, b'P', &parse("", "")).await;
    pg_send(&mut s, b'B', &bind("", "", &[Some("1")], false)).await;
    assert!(has_code(&pg_sync(&mut s).await, "08P01"));
    pg_send(&mut s, b'Q', b"BEGIN\0").await;
    assert_eq!(pg_read(&mut s).await.0, b'C');
    assert_eq!(pg_read(&mut s).await, (b'Z', b"T".to_vec()));
    pg_send(&mut s, b'P', &parse("", "")).await;
    pg_send(&mut s, b'B', &bind("", "", &[], false)).await;
    pg_send(&mut s, b'E', &execute("", 0)).await;
    let frames = pg_sync(&mut s).await;
    assert_eq!(frames.iter().map(|f| f.0).collect::<Vec<_>>(), b"12IZ");
    assert_eq!(frames.last().unwrap().1, b"T");
    pg_send(&mut s, b'Q', b"ROLLBACK\0").await;
    assert_eq!(pg_read(&mut s).await.0, b'C');
    assert_eq!(pg_read(&mut s).await, (b'Z', b"I".to_vec()));
    drop(s);
    f.stop().await;
}

#[tokio::test]
async fn extended_cycle_rolls_back_prior_effects_on_parse_failure_and_disconnect() {
    let f = Fixture::new().await;
    let mut s = raw_pg(&f).await;
    pg_send(
        &mut s,
        b'P',
        &parse("write", "INSERT INTO app (id) VALUES ($1)"),
    )
    .await;
    pg_send(&mut s, b'B', &bind("", "write", &[Some("101")], false)).await;
    pg_send(&mut s, b'E', &execute("", 0)).await;
    pg_send(&mut s, b'P', &parse("bad", "NOT SQL")).await;
    let frames = pg_sync(&mut s).await;
    assert!(frames.iter().any(|f| f.0 == b'E'));
    assert_eq!(frames.last().unwrap(), &(b'Z', b"I".to_vec()));
    let mut native = f.native("app").await;
    assert!(
        native
            .execute(sql("SELECT id FROM app WHERE id=101", vec![]))
            .await
            .unwrap()
            .sql
            .unwrap()
            .rows
            .is_empty()
    );
    for id in [102, 103] {
        pg_send(
            &mut s,
            b'B',
            &bind("", "write", &[Some(&id.to_string())], false),
        )
        .await;
        pg_send(&mut s, b'E', &execute("", 0)).await;
    }
    assert!(!pg_sync(&mut s).await.iter().any(|f| f.0 == b'E'));
    for id in [102, 103] {
        assert_eq!(
            native
                .execute(sql("SELECT id FROM app WHERE id=$1", vec![Value::Int(id)]))
                .await
                .unwrap()
                .sql
                .unwrap()
                .rows
                .len(),
            1
        );
    }
    pg_send(&mut s, b'B', &bind("", "write", &[Some("104")], false)).await;
    pg_send(&mut s, b'E', &execute("", 0)).await;
    pg_send(&mut s, b'H', &[]).await;
    assert_eq!(pg_read(&mut s).await.0, b'2');
    assert_eq!(pg_read(&mut s).await.0, b'C');
    assert!(
        native
            .execute(sql("SELECT id FROM app WHERE id=104", vec![]))
            .await
            .unwrap()
            .sql
            .unwrap()
            .rows
            .is_empty()
    );
    pg_send(&mut s, b'X', &[]).await;
    assert!(s.read_u8().await.is_err());
    assert!(
        native
            .execute(sql("SELECT id FROM app WHERE id=104", vec![]))
            .await
            .unwrap()
            .sql
            .unwrap()
            .rows
            .is_empty()
    );
    drop(native);
    drop(s);
    f.stop().await;
}

#[tokio::test]
async fn implicit_sync_conflict_returns_error_and_idle_without_partial_effects() {
    let f = Fixture::new().await;
    let mut s = raw_pg(&f).await;
    pg_send(
        &mut s,
        b'P',
        &parse("write", "INSERT INTO app (id) VALUES (301)"),
    )
    .await;
    pg_send(&mut s, b'B', &bind("", "write", &[], false)).await;
    pg_send(&mut s, b'E', &execute("", 0)).await;
    pg_send(&mut s, b'H', &[]).await;
    for tag in *b"12C" {
        assert_eq!(pg_read(&mut s).await.0, tag);
    }
    f.admin
        .sql_session()
        .execute("INSERT INTO app (id) VALUES (302)", &[], None)
        .unwrap();
    let frames = pg_sync(&mut s).await;
    assert!(has_code(&frames, "40001"));
    assert_eq!(frames.last(), Some(&(b'Z', vec![b'I'])));
    let rows = f
        .admin
        .sql_session()
        .execute("SELECT id FROM app LIMIT 10", &[], None)
        .unwrap()
        .rows;
    assert_eq!(rows.len(), 1);
    // The same named statement remains usable after the failed cycle.
    pg_send(&mut s, b'B', &bind("", "write", &[], false)).await;
    pg_send(&mut s, b'E', &execute("", 0)).await;
    assert!(!pg_sync(&mut s).await.iter().any(|f| f.0 == b'E'));
    assert_eq!(
        f.admin
            .sql_session()
            .execute("SELECT id FROM app LIMIT 10", &[], None)
            .unwrap()
            .rows
            .len(),
        2
    );
    drop(s);
    f.stop().await;
}

#[tokio::test]
async fn started_postgres_frame_still_has_a_completion_deadline() {
    let f = Fixture::new().await;
    let mut s = raw_pg(&f).await;
    s.write_u8(b'Q').await.unwrap();
    s.flush().await.unwrap();
    assert!(
        tokio::time::timeout(
            giggles_server::IO_TIMEOUT + std::time::Duration::from_secs(3),
            s.read_u8()
        )
        .await
        .unwrap()
        .is_err()
    );
    drop(s);
    f.stop().await;
}

#[tokio::test]
async fn completed_utility_and_write_portals_reject_reexecution() {
    let f = Fixture::new().await;
    let mut s = raw_pg(&f).await;
    for query in [
        "SET application_name='once'",
        "BEGIN",
        "INSERT INTO app (id) VALUES (401)",
    ] {
        pg_send(&mut s, b'Q', b"BEGIN\0").await;
        assert_eq!(pg_read(&mut s).await.0, b'C');
        assert_eq!(pg_read(&mut s).await, (b'Z', vec![b'T']));
        pg_send(&mut s, b'P', &parse("once", query)).await;
        pg_send(&mut s, b'B', &bind("once", "once", &[], false)).await;
        pg_send(&mut s, b'E', &execute("once", 0)).await;
        assert!(!pg_sync(&mut s).await.iter().any(|f| f.0 == b'E'));
        pg_send(&mut s, b'E', &execute("once", 0)).await;
        let frames = pg_sync(&mut s).await;
        assert!(has_code(&frames, "55000"), "{query}: {frames:?}");
        assert_eq!(frames.last(), Some(&(b'Z', vec![b'E'])));
        pg_send(&mut s, b'Q', b"ROLLBACK\0").await;
        assert_eq!(pg_read(&mut s).await.0, b'C');
        assert_eq!(pg_read(&mut s).await, (b'Z', vec![b'I']));
        pg_send(&mut s, b'C', b"Sonce\0").await;
        assert_eq!(pg_sync(&mut s).await[0].0, b'3');
    }
    assert!(
        f.admin
            .sql_session()
            .execute("SELECT id FROM app LIMIT 10", &[], None)
            .unwrap()
            .rows
            .is_empty()
    );
    drop(s);
    f.stop().await;
}

#[tokio::test]
async fn failed_simple_commit_discards_cached_portals() {
    let f = Fixture::new().await;
    for id in [501, 502] {
        f.admin
            .sql_session()
            .execute("INSERT INTO app (id) VALUES ($1)", &[Value::Int(id)], None)
            .unwrap();
    }
    let mut s = raw_pg(&f).await;
    pg_send(&mut s, b'Q', b"BEGIN\0").await;
    assert_eq!(pg_read(&mut s).await.0, b'C');
    assert_eq!(pg_read(&mut s).await, (b'Z', vec![b'T']));
    pg_send(&mut s, b'P', &parse("read", "SELECT id FROM app LIMIT 10")).await;
    pg_send(&mut s, b'B', &bind("page", "read", &[], false)).await;
    pg_send(&mut s, b'E', &execute("page", 1)).await;
    let frames = pg_sync(&mut s).await;
    assert!(frames.iter().any(|f| f.0 == b's'));
    f.admin
        .sql_session()
        .execute("INSERT INTO app (id) VALUES (503)", &[], None)
        .unwrap();
    pg_send(&mut s, b'Q', b"COMMIT\0").await;
    let failure = pg_read(&mut s).await;
    assert!(has_code(&[failure], "40001"));
    assert_eq!(pg_read(&mut s).await, (b'Z', vec![b'I']));
    pg_send(&mut s, b'E', &execute("page", 0)).await;
    let frames = pg_sync(&mut s).await;
    assert!(has_code(&frames, "34000"));
    assert!(!frames.iter().any(|f| f.0 == b'D'));
    drop(s);
    f.stop().await;
}

#[tokio::test]
async fn failed_explicit_commit_rolls_back_and_named_statements_survive_commit() {
    let f = Fixture::new().await;
    let (mut client, connection) = f.pg(f.app.clone()).await;
    let tx = client.transaction().await.unwrap();
    let prepared = tx.prepare("SELECT id FROM app WHERE id=$1").await.unwrap();
    tx.query(&prepared, &[&1i64]).await.unwrap();
    tx.commit().await.unwrap();
    client.query(&prepared, &[&1i64]).await.unwrap();
    client.batch_execute("BEGIN").await.unwrap();
    client
        .execute("INSERT INTO app (id) VALUES (201)", &[])
        .await
        .unwrap();
    assert!(client.batch_execute("NOT SQL").await.is_err());
    client.batch_execute("COMMIT").await.unwrap();
    assert!(
        client
            .query("SELECT id FROM app WHERE id=201", &[])
            .await
            .unwrap()
            .is_empty()
    );
    client.batch_execute("COMMIT").await.unwrap();
    drop(client);
    connection.await.unwrap();
    f.stop().await;
}

#[tokio::test]
async fn idle_postgres_connection_survives_frame_deadline_and_boolean_text_is_case_insensitive() {
    let f = Fixture::new().await;
    let (client, connection) = f.pg(f.app.clone()).await;
    tokio::time::sleep(giggles_server::IO_TIMEOUT + std::time::Duration::from_millis(200)).await;
    client
        .query("SELECT id FROM app WHERE id=1", &[])
        .await
        .unwrap();
    for value in ["TRUE", "True", "Tr", "YES", "ON", "1", " true "] {
        assert_eq!(
            giggles_server::pgwire::decode(
                value.as_bytes(),
                &giggles_core::relational::DataType::Boolean,
                0
            )
            .unwrap(),
            Value::Bool(true)
        );
    }
    for value in ["FALSE", "False", "Fa", "NO", "OFF", "0"] {
        assert_eq!(
            giggles_server::pgwire::decode(
                value.as_bytes(),
                &giggles_core::relational::DataType::Boolean,
                0
            )
            .unwrap(),
            Value::Bool(false)
        );
    }
    for value in ["", "o", "truefalse", "2", "ðŸ¦€"] {
        assert!(
            giggles_server::pgwire::decode(
                value.as_bytes(),
                &giggles_core::relational::DataType::Boolean,
                0
            )
            .is_err()
        );
    }
    // PG18.6 boolrecv accepts any nonzero byte as true (DD-0068 F13).
    assert!(matches!(
        giggles_server::pgwire::decode(&[2], &giggles_core::relational::DataType::Boolean, 1),
        Ok(Value::Bool(true))
    ));
    drop(client);
    connection.await.unwrap();
    f.stop().await;
}

#[tokio::test]
async fn jdbc_session_inspection_reports_actual_settings_and_transaction_rollback() {
    let f = Fixture::new().await;
    let (client, connection) = f.pg(f.app.clone()).await;
    assert_eq!(
        client
            .query_one("SHOW TRANSACTION ISOLATION LEVEL", &[])
            .await
            .unwrap()
            .get::<_, String>(0),
        "serializable"
    );
    client
        .batch_execute("SET application_name='before'")
        .await
        .unwrap();
    let prepared = client.prepare("SHOW application_name").await.unwrap();
    assert_eq!(
        client
            .query_one(&prepared, &[])
            .await
            .unwrap()
            .get::<_, String>(0),
        "before"
    );
    client.batch_execute("BEGIN").await.unwrap();
    client
        .batch_execute("SET application_name='during'")
        .await
        .unwrap();
    assert_eq!(
        client
            .query_one(&prepared, &[])
            .await
            .unwrap()
            .get::<_, String>(0),
        "during"
    );
    client.batch_execute("ROLLBACK").await.unwrap();
    assert_eq!(
        client
            .query_one(&prepared, &[])
            .await
            .unwrap()
            .get::<_, String>(0),
        "before"
    );
    client.batch_execute("BEGIN").await.unwrap();
    assert!(client.batch_execute("NOT SQL").await.is_err());
    assert_eq!(
        code(client.query_one(&prepared, &[]).await.unwrap_err()),
        "25P02"
    );
    client.batch_execute("ROLLBACK").await.unwrap();
    let messages = client.simple_query("SHOW search_path").await.unwrap();
    assert!(messages.iter().any(
        |m| matches!(m,tokio_postgres::SimpleQueryMessage::Row(row) if row.get(0)==Some("public"))
    ));
    drop(client);
    connection.await.unwrap();
    let (task, connection) = f.pg(f.task.clone()).await;
    assert_eq!(
        code(task.query("SHOW application_name", &[]).await.unwrap_err()),
        "42501"
    );
    drop(task);
    connection.await.unwrap();
    f.stop().await;
}

#[tokio::test]
async fn jdbc_initialization_settings_reject_sql_escapes_and_respect_aborted_transactions() {
    let f = Fixture::new().await;
    let mut s = raw_pg(&f).await;
    pg_send(
        &mut s,
        b'Q',
        b"SET extra_float_digits = 3; SET application_name = 'PostgreSQL JDBC Driver'\0",
    )
    .await;
    assert_eq!(pg_read(&mut s).await, (b'C', b"SET\0".to_vec()));
    assert_eq!(pg_read(&mut s).await, (b'C', b"SET\0".to_vec()));
    assert_eq!(pg_read(&mut s).await, (b'Z', b"I".to_vec()));
    pg_send(
        &mut s,
        b'P',
        &parse("setting", "SET application_name = 'prepared'"),
    )
    .await;
    pg_send(&mut s, b'B', &bind("", "setting", &[], false)).await;
    pg_send(&mut s, b'E', &execute("", 0)).await;
    assert_eq!(
        pg_sync(&mut s)
            .await
            .iter()
            .map(|f| f.0)
            .collect::<Vec<_>>(),
        b"12CZ"
    );
    for sql in [
        "SET application_name='x'; INSERT INTO app (id) VALUES (77)",
        "SET application_name='x'; SET role=admin",
        "SET application_name='x'; SET extra_float_digits=99",
    ] {
        let mut query = sql.as_bytes().to_vec();
        query.push(0);
        pg_send(&mut s, b'Q', &query).await;
        assert_eq!(pg_read(&mut s).await.0, b'E');
        assert_eq!(pg_read(&mut s).await, (b'Z', b"I".to_vec()));
    }
    pg_send(&mut s, b'Q', b"BEGIN\0").await;
    assert_eq!(pg_read(&mut s).await.0, b'C');
    assert_eq!(pg_read(&mut s).await.1, b"T");
    pg_send(&mut s, b'Q', b"unsupported\0").await;
    assert_eq!(pg_read(&mut s).await.0, b'E');
    assert_eq!(pg_read(&mut s).await.1, b"E");
    pg_send(&mut s, b'B', &bind("", "setting", &[], false)).await;
    pg_send(&mut s, b'E', &execute("", 0)).await;
    assert!(has_code(&pg_sync(&mut s).await, "25P02"));
    pg_send(&mut s, b'Q', b"ROLLBACK\0").await;
    assert_eq!(pg_read(&mut s).await.0, b'C');
    assert_eq!(pg_read(&mut s).await.1, b"I");
    drop(s);
    let (task, connection) = f.pg(f.task.clone()).await;
    assert_eq!(
        task.batch_execute("SET application_name='task'")
            .await
            .unwrap_err()
            .code()
            .unwrap()
            .code(),
        "42501"
    );
    drop(task);
    connection.await.unwrap();
    let (app, connection) = f.pg(f.app.clone()).await;
    assert!(
        app.query("SELECT id FROM app WHERE id=77", &[])
            .await
            .unwrap()
            .is_empty()
    );
    drop(app);
    connection.await.unwrap();
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raw_wire_text_parameters_binary_results_close_and_error_drain() {
    let f = Fixture::new().await;
    let mut s = raw_pg(&f).await;
    pg_send(
        &mut s,
        b'P',
        &parse("insert", "INSERT INTO app VALUES ($1,$2,$3,$4,$5,$6)"),
    )
    .await;
    pg_send(
        &mut s,
        b'B',
        &bind(
            "p",
            "insert",
            &[
                Some("-8"),
                Some("hello"),
                Some("t"),
                Some("-32768"),
                Some("2147483647"),
                Some("\\x00ff"),
            ],
            false,
        ),
    )
    .await;
    pg_send(&mut s, b'E', &execute("p", 0)).await;
    let frames = pg_sync(&mut s).await;
    assert_eq!(frames.iter().map(|f| f.0).collect::<Vec<_>>(), b"12CZ");
    pg_send(
        &mut s,
        b'P',
        &parse("select", "SELECT * FROM app WHERE id=$1"),
    )
    .await;
    pg_send(&mut s, b'B', &bind("p", "select", &[Some("-8")], true)).await;
    pg_send(&mut s, b'D', b"Pp\0").await;
    pg_send(&mut s, b'E', &execute("p", 0)).await;
    let frames = pg_sync(&mut s).await;
    assert_eq!(frames.iter().map(|f| f.0).collect::<Vec<_>>(), b"12TDCZ");
    let data = &frames.iter().find(|f| f.0 == b'D').unwrap().1;
    assert_eq!(&data[0..2], &6u16.to_be_bytes());
    assert_eq!(&data[6..14], &(-8i64).to_be_bytes());
    pg_send(&mut s, b'C', b"Sinsert\0").await;
    assert_eq!(pg_sync(&mut s).await[0].0, b'3');
    // A failed Parse drains pipelined Bind/Execute until Sync, so this insert cannot run.
    pg_send(
        &mut s,
        b'P',
        &parse("bad", "COPY app FROM STDIN WITH (FORMAT csv, HEADER 2)"),
    )
    .await;
    pg_send(
        &mut s,
        b'B',
        &bind(
            "bad",
            "insert",
            &[Some("99"), None, None, None, None, None],
            false,
        ),
    )
    .await;
    pg_send(&mut s, b'E', &execute("bad", 0)).await;
    let frames = pg_sync(&mut s).await;
    assert!(has_code(&frames, "42601"));
    assert_eq!(frames.len(), 2);
    assert_eq!(
        f.admin
            .sql_session()
            .execute("SELECT id FROM app LIMIT 10", &[], None)
            .unwrap()
            .rows
            .len(),
        1
    );
    // Malformed parameter width is an error, then a fresh cycle works.
    let mut b = bind("bad", "select", &[], false);
    b["bad".len() + 1 + "select".len() + 1..].fill(255);
    pg_send(&mut s, b'B', &b).await;
    assert!(has_code(&pg_sync(&mut s).await, "08P01"));
    pg_send(&mut s, b'Q', b"SELECT id FROM app LIMIT 10\0").await;
    let mut count = 0;
    loop {
        let (t, _) = pg_read(&mut s).await;
        if t == b'D' {
            count += 1;
        }
        if t == b'Z' {
            break;
        }
    }
    assert_eq!(count, 1);
    drop(s);
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn portal_suspension_transaction_cleanup_and_object_limits() {
    let f = Fixture::new().await;
    let (c, conn) = f.pg(f.app.clone()).await;
    for id in 0..3i64 {
        c.execute("INSERT INTO app (id) VALUES ($1)", &[&id])
            .await
            .unwrap();
    }
    drop(c);
    conn.await.unwrap();
    let mut s = raw_pg(&f).await;
    pg_send(&mut s, b'Q', b"BEGIN\0").await;
    while pg_read(&mut s).await.0 != b'Z' {}
    pg_send(&mut s, b'P', &parse("scan", "SELECT id FROM app LIMIT 3")).await;
    pg_send(&mut s, b'B', &bind("page", "scan", &[], false)).await;
    pg_send(&mut s, b'E', &execute("page", 1)).await;
    let frames = pg_sync(&mut s).await;
    assert_eq!(frames.iter().map(|f| f.0).collect::<Vec<_>>(), b"12DsZ");
    pg_send(&mut s, b'E', &execute("page", 1)).await;
    assert_eq!(
        pg_sync(&mut s)
            .await
            .iter()
            .map(|f| f.0)
            .collect::<Vec<_>>(),
        b"DsZ"
    );
    pg_send(&mut s, b'E', &execute("page", 1)).await;
    assert_eq!(
        pg_sync(&mut s)
            .await
            .iter()
            .map(|f| f.0)
            .collect::<Vec<_>>(),
        b"DCZ"
    );
    pg_send(&mut s, b'Q', b"ROLLBACK\0").await;
    while pg_read(&mut s).await.0 != b'Z' {}
    pg_send(&mut s, b'E', &execute("page", 0)).await;
    assert!(has_code(&pg_sync(&mut s).await, "34000"));
    for i in 0..1023 {
        pg_send(
            &mut s,
            b'P',
            &parse(&format!("p{i}"), "SELECT id FROM app LIMIT 1"),
        )
        .await;
        assert_eq!(pg_sync(&mut s).await[0].0, b'1');
    }
    pg_send(
        &mut s,
        b'P',
        &parse("overflow", "SELECT id FROM app LIMIT 1"),
    )
    .await;
    assert!(has_code(&pg_sync(&mut s).await, "54000"));
    drop(s);
    f.stop().await;
}



#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn executable_server_starts_and_native_client_connects() {
    use tokio::io::AsyncBufReadExt;
    let f = Fixture::new().await;
    let path = f.path.clone();
    f.stop().await;
    let mut config: serde_json::Value =
        serde_json::from_slice(&std::fs::read(path.join("server.TEST-ONLY.json")).unwrap())
            .unwrap();
    config["postgres"] = "127.0.0.1:0".into();
    config["wire"] = "127.0.0.1:0".into();
    let config_path = path.join("ephemeral-server.TEST-ONLY.json");
    std::fs::write(&config_path, serde_json::to_vec(&config).unwrap()).unwrap();
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_giggles-server"))
        .arg(config_path)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut lines = tokio::io::BufReader::new(child.stdout.take().unwrap()).lines();
    let line = tokio::time::timeout(std::time::Duration::from_secs(10), lines.next_line())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    std::fs::write(path.join("server-startup.log"), &line).unwrap();
    let addr = listening_address(&line, "wire").parse().unwrap();
    let cfg = tls::client(
        tls::certs(path.join("app.pem")).unwrap(),
        tls::key(path.join("app.key.TEST-ONLY")).unwrap(),
        tls::certs(path.join("ca.pem")).unwrap(),
    )
    .unwrap();
    let mut c = connect_wire(addr, wire_tls(&path, Some("app")).unwrap())
        .await
        .unwrap();
    assert_eq!(c.execute(ping()).await.unwrap().code, "ok");
    drop(c);
    let pg_addr = line
        .split_whitespace()
        .nth(1)
        .unwrap()
        .strip_prefix("postgres=")
        .unwrap();
    let mut socket = TcpStream::connect(pg_addr).await.unwrap();
    socket
        .write_all(&[0, 0, 0, 8, 4, 210, 22, 47])
        .await
        .unwrap();
    assert_eq!(socket.read_u8().await.unwrap(), b'S');
    let mut pg = tokio_rustls::TlsConnector::from(cfg)
        .connect(
            rustls::pki_types::ServerName::try_from("localhost").unwrap(),
            socket,
        )
        .await
        .unwrap();
    let mut startup = 196608u32.to_be_bytes().to_vec();
    startup.extend(b"user\0private-user\0database\0giggles\0\0");
    pg.write_u32((startup.len() + 4) as u32).await.unwrap();
    pg.write_all(&startup).await.unwrap();
    let (tag, body) = pg_read(&mut pg).await;
    assert_eq!(tag, b'E');
    assert!(
        String::from_utf8(body)
            .unwrap()
            .contains("startup user must be giggles")
    );
    let mut errors = tokio::io::BufReader::new(child.stderr.take().unwrap()).lines();
    let diagnostic = tokio::time::timeout(std::time::Duration::from_secs(5), errors.next_line())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(diagnostic.contains("protocol=postgres sqlstate=28000"));
    assert!(diagnostic.contains("startup user must be giggles"));
    assert!(!diagnostic.contains("private-user"));
    std::fs::write(path.join("server-rejection.log"), diagnostic).unwrap();
    drop(pg);
    child.start_kill().unwrap();
    let status = child.wait().await.unwrap();
    std::fs::write(path.join("server-killed-status.txt"), format!("{status:?}")).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn seeded_native_and_pinned_driver_histories_match_independent_map() {
    use std::collections::BTreeMap;
    let f = Fixture::new().await;
    let mut native = f.native("app").await;
    let (pg, conn) = f.pg(f.app.clone()).await;
    let mut oracle = BTreeMap::new();
    let mut trace = String::new();
    for seed in 1..=10u64 {
        let mut state = seed;
        for step in 0..40 {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let id = ((state >> 32) % 16) as i64;
            let body = format!("seed{seed}-step{step}");
            let delete = state & 3 == 0;
            trace.push_str(&format!(
                "seed={seed} step={step} id={id} delete={delete} body={body}\n"
            ));
            if delete {
                let expected = u64::from(oracle.remove(&id).is_some());
                if step % 2 == 0 {
                    assert_eq!(
                        native
                            .execute(sql("DELETE FROM app WHERE id=$1", vec![Value::Int(id)]))
                            .await
                            .unwrap()
                            .sql
                            .unwrap()
                            .affected,
                        expected
                    );
                } else {
                    assert_eq!(
                        pg.execute("DELETE FROM app WHERE id=$1", &[&id])
                            .await
                            .unwrap(),
                        expected
                    );
                }
            } else {
                let exists = oracle.insert(id, body.clone()).is_some();
                let query = if exists {
                    "UPDATE app SET body=$2 WHERE id=$1"
                } else {
                    "INSERT INTO app (id,body) VALUES ($1,$2)"
                };
                if step % 2 == 0 {
                    assert_eq!(
                        native
                            .execute(sql(query, vec![Value::Int(id), Value::Text(body)]))
                            .await
                            .unwrap()
                            .sql
                            .unwrap()
                            .affected,
                        1
                    );
                } else {
                    assert_eq!(pg.execute(query, &[&id, &body]).await.unwrap(), 1);
                }
            }
            let rows = pg
                .query("SELECT id,body FROM app ORDER BY id LIMIT 16", &[])
                .await
                .unwrap();
            let actual: BTreeMap<i64, String> =
                rows.into_iter().map(|r| (r.get(0), r.get(1))).collect();
            assert_eq!(actual, oracle, "seed={seed} step={step}");
        }
    }
    std::fs::write(f.path.join("seeded-history.txt"), trace).unwrap();
    drop(native);
    drop(pg);
    conn.await.unwrap();
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn protocol_cancel_request_rejected_without_rolling_back_other_session() {
    let f = Fixture::new().await;
    let (pg, conn) = f.pg(f.app.clone()).await;
    pg.execute("INSERT INTO app (id) VALUES ($1)", &[&90i64])
        .await
        .unwrap();
    let mut s = TcpStream::connect(f.pg.address).await.unwrap();
    s.write_all(&[0, 0, 0, 16, 4, 210, 22, 46, 0, 0, 0, 0, 0, 0, 0, 0])
        .await
        .unwrap();
    assert!(s.read_u8().await.is_err());
    assert_eq!(
        pg.query_one("SELECT id FROM app WHERE id=$1", &[&90i64])
            .await
            .unwrap()
            .get::<_, i64>(0),
        90
    );
    drop(pg);
    conn.await.unwrap();
    f.stop().await;
}



#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn startup_spoof_and_malformed_wire_corpus_remains_isolated() {
    let f = Fixture::new().await;
    let mut trace = String::new();
    for fields in [
        &b"user\0admin\0database\0giggles\0\0"[..],
        &b"user\0giggles\0database\0other\0\0"[..],
        &b"user\0giggles\0database\0giggles\0options\0-c role=admin\0\0"[..],
    ] {
        let mut s = raw_tls(&f, f.app.clone(), true).await;
        let mut b = 196608u32.to_be_bytes().to_vec();
        b.extend(fields);
        s.write_u32((b.len() + 4) as u32).await.unwrap();
        s.write_all(&b).await.unwrap();
        let (tag, b) = pg_read(&mut s).await;
        assert_eq!(tag, b'E');
        trace.push_str(&format!("startup_response={}\n", hex::encode(b)));
    }
    for tag in [b'P', b'B', b'D', b'E', b'C', 255] {
        for length in 0..8 {
            let mut s = raw_pg(&f).await;
            let bytes = vec![255; length];
            pg_send(&mut s, tag, &bytes).await;
            let frames = pg_sync(&mut s).await;
            assert_eq!(frames[0].0, b'E', "tag={tag} length={length}");
            assert_eq!(frames.last().unwrap().0, b'Z');
            trace.push_str(&format!(
                "tag={tag} bytes={} response={:?}\n",
                hex::encode(bytes),
                frames
            ));
        }
    }
    // COPY data/done/fail outside CopyIn are ignored, as on PostgreSQL 18.6.
    let mut s = raw_pg(&f).await;
    for (tag, bytes) in [
        (b'd', b"unused".as_slice()),
        (b'c', b""),
        (b'f', b"unused\0"),
    ] {
        pg_send(&mut s, tag, bytes).await;
    }
    let frames = pg_sync(&mut s).await;
    assert_eq!(frames.iter().map(|frame| frame.0).collect::<Vec<_>>(), b"Z");
    drop(s);
    for length in [0, 3, u32::MAX, giggles_server::MAX_FRAME as u32 + 1] {
        let mut s = raw_pg(&f).await;
        s.write_u8(b'Q').await.unwrap();
        s.write_u32(length).await.unwrap();
        assert!(s.read_u8().await.is_err());
    }
    std::fs::write(f.path.join("malformed-wire-traces.txt"), trace).unwrap();
    let (pg, conn) = f.pg(f.app.clone()).await;
    pg.execute("INSERT INTO app (id) VALUES ($1)", &[&101i64])
        .await
        .unwrap();
    drop(pg);
    conn.await.unwrap();
    f.stop().await;
}





#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn extended_rollback_recovers_an_aborted_transaction() {
    let f = Fixture::new().await;
    let (pg, conn) = f.pg(f.app.clone()).await;
    pg.execute("INSERT INTO app (id) VALUES ($1)", &[&1i64])
        .await
        .unwrap();
    pg.batch_execute("BEGIN").await.unwrap();
    assert_eq!(
        code(
            pg.execute("INSERT INTO app (id) VALUES ($1)", &[&1i64])
                .await
                .unwrap_err()
        ),
        "23505"
    );
    pg.execute("ROLLBACK", &[]).await.unwrap();
    pg.execute("INSERT INTO app (id) VALUES ($1)", &[&2i64])
        .await
        .unwrap();
    drop(pg);
    conn.await.unwrap();
    f.stop().await;
}

#[path = "cancellation_network.inc"]
mod cancellation_network;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn catalog_driver_identity_typed_metadata_and_hidden_relations() {
    let f = Fixture::new().await;
    f.admin
        .sql_session()
        .execute(
            "CREATE TABLE hidden_catalog_test (id BIGINT PRIMARY KEY, secret TEXT)",
            &[],
            None,
        )
        .unwrap();
    let (pg, conn) = f.pg(f.app.clone()).await;
    let identity = pg
        .query_one(
            "SELECT current_database(), current_schema(), current_user",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(identity.get::<_, String>(0), "giggles");
    assert_eq!(identity.get::<_, String>(1), "public");
    assert_eq!(identity.get::<_, String>(2), "giggles");
    let tables = pg.query("SELECT oid, relname FROM pg_catalog.pg_class WHERE relkind='r' AND relnamespace=2200 ORDER BY relname", &[]).await.unwrap();
    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0].get::<_, String>(1), "app");
    let oid: u32 = tables[0].get(0);
    let statement = pg.prepare("SELECT attname, attnum, atttypid, attnotnull FROM pg_catalog.pg_attribute WHERE attrelid=$1 AND attnum>0 ORDER BY attnum").await.unwrap();
    assert_eq!(statement.params(), &[tokio_postgres::types::Type::OID]);
    let columns = pg.query(&statement, &[&oid]).await.unwrap();
    assert_eq!(columns.len(), 6);
    assert_eq!(columns[0].get::<_, String>(0), "id");
    assert_eq!(columns[0].get::<_, i16>(1), 1);
    assert_eq!(columns[0].get::<_, u32>(2), 20);
    assert!(columns[0].get::<_, bool>(3));
    assert_eq!(columns[1].get::<_, String>(0), "body");
    pg.batch_execute("BEGIN").await.unwrap();
    pg.batch_execute("COMMIT").await.unwrap();
    assert_eq!(pg.query(&statement, &[&oid]).await.unwrap().len(), 6);
    let visible = pg
        .simple_query(
            "SELECT relname FROM pg_catalog.pg_class WHERE relnamespace=2200 ORDER BY relname",
        )
        .await
        .unwrap();
    assert!(!format!("{visible:?}").contains("hidden_catalog_test"));
    assert_eq!(code(pg.query("SELECT c.relname FROM pg_catalog.pg_class c JOIN hidden_catalog_test h ON h.id=c.oid", &[]).await.unwrap_err()), "42501");
    drop(pg);
    conn.await.unwrap();
    let (task, conn) = f.pg(f.task.clone()).await;
    assert_eq!(
        code(
            task.query("SELECT current_database()", &[])
                .await
                .unwrap_err()
        ),
        "42501"
    );
    drop(task);
    conn.await.unwrap();
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn catalog_driver_schema_change_and_aborted_transaction_fence() {
    let f = Fixture::new().await;
    let (pg, conn) = f.pg(f.app.clone()).await;
    let prepared = pg
        .prepare("SELECT relname FROM pg_catalog.pg_class WHERE relnamespace=2200")
        .await
        .unwrap();
    f.admin
        .sql_session()
        .execute(
            "CREATE TABLE catalog_new (id BIGINT PRIMARY KEY)",
            &[],
            None,
        )
        .unwrap();
    assert_eq!(code(pg.query(&prepared, &[]).await.unwrap_err()), "0A000");
    pg.batch_execute("BEGIN").await.unwrap();
    assert!(
        pg.batch_execute("SELECT no_such_column FROM app")
            .await
            .is_err()
    );
    assert_eq!(
        code(pg.query("SELECT current_schema()", &[]).await.unwrap_err()),
        "25P02"
    );
    pg.batch_execute("ROLLBACK").await.unwrap();
    assert_eq!(
        pg.query_one("SELECT current_schema()", &[])
            .await
            .unwrap()
            .get::<_, String>(0),
        "public"
    );
    drop(pg);
    conn.await.unwrap();
    f.stop().await;
}



#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn catalog_cached_pages_reauthorize_before_release() {
    let f = Fixture::new().await;
    let mut s = raw_pg(&f).await;
    pg_send(&mut s, b'Q', b"BEGIN\0").await;
    while pg_read(&mut s).await.0 != b'Z' {}
    pg_send(
        &mut s,
        b'P',
        &parse(
            "catalog",
            "SELECT attname FROM pg_catalog.pg_attribute ORDER BY attname",
        ),
    )
    .await;
    assert_eq!(pg_sync(&mut s).await[0].0, b'1');
    pg_send(&mut s, b'B', &bind("page", "catalog", &[], false)).await;
    pg_send(&mut s, b'E', &execute("page", 1)).await;
    let frames = pg_sync(&mut s).await;
    assert_eq!(frames.iter().filter(|(tag, _)| *tag == b'D').count(), 1);
    assert!(frames.iter().any(|(tag, _)| *tag == b's'));
    f.service.reload_bindings(vec![]).unwrap();
    pg_send(&mut s, b'E', &execute("page", 1)).await;
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(5), s.read_u8())
            .await
            .unwrap()
            .is_err()
    );
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn catalog_and_application_pages_share_aggregate_cache_admission() {
    let f = Fixture::new().await;
    let mut owner = f.admin.sql_session();
    for id in 0..60i64 {
        owner
            .execute(
                "INSERT INTO app (id,body) VALUES ($1,$2)",
                &[Value::Int(id), Value::Text("\u{0001}".repeat(16_000))],
                None,
            )
            .unwrap();
    }
    let mut s = raw_pg(&f).await;
    pg_send(&mut s, b'Q', b"BEGIN\0").await;
    while pg_read(&mut s).await.0 != b'Z' {}
    pg_send(&mut s, b'P', &parse("app", "SELECT body FROM app LIMIT 60")).await;
    pg_send(&mut s, b'B', &bind("app_page", "app", &[], false)).await;
    pg_send(&mut s, b'E', &execute("app_page", 1)).await;
    let first = pg_sync(&mut s).await;
    assert!(first.iter().any(|(tag, _)| *tag == b'D'));
    pg_send(
        &mut s,
        b'P',
        &parse("app_more", "SELECT body FROM app LIMIT 10"),
    )
    .await;
    pg_send(&mut s, b'B', &bind("more_page", "app_more", &[], false)).await;
    pg_send(&mut s, b'E', &execute("more_page", 1)).await;
    let more = pg_sync(&mut s).await;
    assert!(more.iter().any(|(tag, _)| *tag == b'D'));
    let query = format!(
        "SELECT '{}' AS payload FROM (SELECT oid FROM pg_catalog.pg_type LIMIT 12) a CROSS JOIN (SELECT oid FROM pg_catalog.pg_type LIMIT 10) b LIMIT 120",
        "x".repeat(16_000)
    );
    let planned = giggles_pg_catalog::plan(&query).unwrap().unwrap();
    let control = giggles_pg_catalog::execute(
        &planned,
        &giggles_pg_catalog::CatalogSnapshot::default(),
        &[],
    )
    .unwrap();
    assert_eq!(control.rows.len(), 120);
    pg_send(&mut s, b'P', &parse("catalog", &query)).await;
    pg_send(&mut s, b'B', &bind("catalog_page", "catalog", &[], false)).await;
    pg_send(&mut s, b'E', &execute("catalog_page", 1)).await;
    let admitted = pg_sync(&mut s).await;
    assert!(admitted.iter().any(|(tag, _)| *tag == b'D'));
    assert!(admitted.iter().any(|(tag, _)| *tag == b's'));
    assert!(!has_code(&admitted, "54000"));
    assert_eq!(admitted.last(), Some(&(b'Z', vec![b'T'])));
    pg_send(&mut s, b'Q', b"ROLLBACK\0").await;
    while pg_read(&mut s).await.0 != b'Z' {}
    // The same catalog also fits after the application portals are closed.
    pg_send(&mut s, b'P', &parse("catalog_alone", &query)).await;
    pg_send(&mut s, b'B', &bind("alone", "catalog_alone", &[], false)).await;
    pg_send(&mut s, b'E', &execute("alone", 1)).await;
    let alone = pg_sync(&mut s).await;
    assert!(alone.iter().any(|(tag, _)| *tag == b'D'), "{alone:?}");
    assert!(!alone.iter().any(|(tag, _)| *tag == b'E'));
    f.stop().await;
}



#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rejected_queries_report_stage_and_fingerprint_without_sql_or_values() {
    use sha2::{Digest, Sha256};
    let f = Fixture::new().await;
    let (pg, conn) = f.pg(f.app.clone()).await;
    let sql = "SELECT private_probe('never_log_this_secret')";
    let error = pg.query(sql, &[]).await.unwrap_err();
    let error = error.as_db_error().unwrap();
    // DD-0068 F9: pgwire follows PG 18 (42883/3F000) for deferred-routine-only queries; native surfaces keep 42501.
    assert_eq!(error.code().code(), "42883");
    assert!(
        error
            .message()
            .contains("request rejected; consult documented code")
    );
    let detail = error.detail().unwrap();
    assert!(detail.contains("phase=application prepare"));
    assert!(detail.contains(&hex::encode(Sha256::digest(sql.as_bytes()))));
    for private in ["private_probe", "never_log_this_secret", "SELECT"] {
        assert!(!detail.contains(private));
        assert!(!error.message().contains(private));
    }
    let prepared_sql = "SELECT $1::int8";
    let mut raw = raw_pg(&f).await;
    pg_send(&mut raw, b'P', &parse("d", prepared_sql)).await;
    assert_eq!(pg_sync(&mut raw).await[0].0, b'1');
    pg_send(
        &mut raw,
        b'B',
        &bind("p", "d", &[Some("private_bind_value")], false),
    )
    .await;
    let frames = pg_sync(&mut raw).await;
    assert!(has_code(&frames, "22P02"));
    let body = frames.iter().find(|(t, _)| *t == b'E').unwrap().1.clone();
    let body = String::from_utf8(body).unwrap();
    assert!(body.contains("phase=bind"));
    assert!(body.contains(&hex::encode(Sha256::digest(prepared_sql.as_bytes()))));
    assert!(!body.contains("private_bind_value"));
    // An unrelated missing statement must not inherit the preceding query id.
    pg_send(&mut raw, b'B', &bind("p", "missing", &[], false)).await;
    let frames = pg_sync(&mut raw).await;
    assert!(has_code(&frames, "26000"));
    let body =
        String::from_utf8(frames.iter().find(|(t, _)| *t == b'E').unwrap().1.clone()).unwrap();
    assert!(body.contains("query_id=none"));
    drop(pg);
    conn.await.unwrap();
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn datagrip_keepalive_table_free_select_simple_prepared_and_role_boundaries() {
    let f = Fixture::new().await;
    let (pg, conn) = f.pg(f.app.clone()).await;
    for sql in [
        "SELECT 1",
        "/* keep-alive */ select 1;",
        "SELECT 1 AS alive",
    ] {
        let messages = pg.simple_query(sql).await.unwrap();
        let values = messages
            .iter()
            .filter_map(|m| {
                if let tokio_postgres::SimpleQueryMessage::Row(row) = m {
                    Some(row.get(0))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        assert_eq!(values, [Some("1")]);
        let statement = pg.prepare(sql).await.unwrap();
        assert_eq!(
            statement.columns()[0].type_(),
            &tokio_postgres::types::Type::INT4
        );
        assert_eq!(
            pg.query_one(&statement, &[])
                .await
                .unwrap()
                .get::<_, i32>(0),
            1
        );
    }
    assert!(
        pg.query("SELECT 1 WHERE false", &[])
            .await
            .unwrap()
            .is_empty()
    );
    assert!(pg.simple_query("SELECT 1; DELETE FROM app").await.is_err());
    assert!(
        pg.query("SELECT (SELECT id FROM other LIMIT 1)", &[])
            .await
            .is_err()
    );
    pg.batch_execute("BEGIN").await.unwrap();
    assert!(pg.batch_execute("nonsense").await.is_err());
    assert_eq!(code(pg.query("SELECT 1", &[]).await.unwrap_err()), "25P02");
    pg.batch_execute("ROLLBACK").await.unwrap();
    assert_eq!(
        pg.query_one("SELECT 1", &[])
            .await
            .unwrap()
            .get::<_, i32>(0),
        1
    );
    drop(pg);
    conn.await.unwrap();
    let (task, conn) = f.pg(f.task.clone()).await;
    assert_eq!(
        code(task.query("SELECT 1", &[]).await.unwrap_err()),
        "42501"
    );
    drop(task);
    conn.await.unwrap();
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn catalog_declared_parameter_type_cannot_disagree_with_output_description() {
    use tokio_postgres::types::Type;
    let f = Fixture::new().await;
    let (pg, conn) = f.pg(f.app.clone()).await;
    assert_eq!(
        code(
            pg.prepare_typed("SELECT $1 FROM pg_catalog.pg_type LIMIT 1", &[Type::INT8])
                .await
                .unwrap_err()
        ),
        "0A000"
    );
    let typed = pg
        .prepare_typed(
            "SELECT $1::int8 AS value FROM pg_catalog.pg_type LIMIT 1",
            &[Type::INT8],
        )
        .await
        .unwrap();
    assert_eq!(typed.params(), &[Type::INT8]);
    assert_eq!(typed.columns()[0].type_(), &Type::INT8);
    assert_eq!(
        pg.query_one(&typed, &[&7i64])
            .await
            .unwrap()
            .get::<_, i64>(0),
        7
    );
    assert_eq!(
        pg.query_one(&typed, &[&Option::<i64>::None])
            .await
            .unwrap()
            .get::<_, Option<i64>>(0),
        None
    );
    let text = pg
        .prepare_typed("SELECT $1 FROM pg_catalog.pg_type LIMIT 1", &[Type::TEXT])
        .await
        .unwrap();
    assert_eq!(
        pg.query_one(&text, &[&"seven"])
            .await
            .unwrap()
            .get::<_, String>(0),
        "seven"
    );
    drop(pg);
    conn.await.unwrap();
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn catalog_multiple_granted_tables_composite_key_order_and_empty_visible_set() {
    let f = Fixture::new().await;
    f.admin.sql_session().execute("CREATE TABLE composite (tenant INTEGER, id BIGINT, body TEXT, PRIMARY KEY (id, tenant))", &[], None).unwrap();
    let mut bindings: Vec<Binding> =
        serde_json::from_slice(&std::fs::read(f.path.join("bindings.json")).unwrap()).unwrap();
    let Role::Application { tables, .. } = &mut bindings[0].role else {
        panic!("fixture role")
    };
    tables.insert("composite".into());
    f.service.reload_bindings(bindings.clone()).unwrap();
    let (pg, conn) = f.pg(f.app.clone()).await;
    let rows = pg.query("SELECT relname FROM pg_catalog.pg_class WHERE relnamespace=2200 AND relkind='r' ORDER BY relname", &[]).await.unwrap();
    assert_eq!(
        rows.iter()
            .map(|r| r.get::<_, String>(0))
            .collect::<Vec<_>>(),
        ["app", "composite"]
    );
    let key = pg.query_one("SELECT conkey FROM pg_catalog.pg_constraint c JOIN pg_catalog.pg_class t ON t.oid=c.conrelid WHERE t.relname='composite' AND c.contype='p'", &[]).await.unwrap();
    assert_eq!(key.get::<_, Vec<i16>>(0), [2, 1]);
    let attrs = pg.query("SELECT attname, attnotnull FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_class t ON t.oid=a.attrelid WHERE t.relname='composite' AND attnum>0 ORDER BY attnum", &[]).await.unwrap();
    assert_eq!(attrs.len(), 3);
    assert!(attrs[0].get::<_, bool>(1));
    assert!(attrs[1].get::<_, bool>(1));
    assert!(!attrs[2].get::<_, bool>(1));
    drop(pg);
    conn.await.unwrap();
    let Role::Application { tables, .. } = &mut bindings[0].role else {
        unreachable!()
    };
    *tables = BTreeSet::from(["not_created".into()]);
    f.service.reload_bindings(bindings).unwrap();
    let (pg, conn) = f.pg(f.app.clone()).await;
    assert!(
        pg.query(
            "SELECT relname FROM pg_catalog.pg_class WHERE relnamespace=2200",
            &[]
        )
        .await
        .unwrap()
        .is_empty()
    );
    drop(pg);
    conn.await.unwrap();
    f.stop().await;
}

#[path = "lifecycle_network.inc"]
mod lifecycle_network;

#[tokio::test]
async fn installed_datagrip_detection_and_startup_queries() {
    let f = Fixture::new().await;
    let (pg, conn) = f.pg(f.app.clone()).await;
    for (sql, expected) in [("show DateStyle", "ISO, MDY"), ("SHOW TimeZone", "UTC")] {
        assert_eq!(
            pg.query_one(sql, &[]).await.unwrap().get::<_, String>(0),
            expected
        );
        assert!(pg.simple_query(sql).await.unwrap().iter().any(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(r) if r.get(0) == Some(expected))));
    }

    let lookup =
        include_str!("../../giggles-pg-catalog/tests/fixtures/jdbc-column-type-lookup.sql");
    let typed = pg
        .prepare_typed(lookup, &[tokio_postgres::types::Type::INT8])
        .await
        .unwrap();
    let row = pg.query_one(&typed, &[&28i64]).await.unwrap();
    assert_eq!(row.get::<_, String>(2), "xid");
    assert!(!row.get::<_, bool>(0));
    for value in [-1i64, 4_294_967_296i64] {
        assert_eq!(
            code(pg.query(&typed, &[&value]).await.unwrap_err()),
            "42804"
        );
    }
    assert_eq!(pg.query(&typed, &[&28i64]).await.unwrap().len(), 1);

    let version: String = pg.query_one("select version()", &[]).await.unwrap().get(0);
    assert!(version.starts_with("PostgreSQL 18.6 (GigglesDB "));
    let sql = "select round(extract(epoch from pg_postmaster_start_time() at time zone 'UTC')) as startup_time";
    let p = pg.prepare(sql).await.unwrap();
    let first: i64 = pg.query_one(&p, &[]).await.unwrap().get(0);
    assert!(first > 0);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    assert!(first <= now);
    let second: i64 = pg.query_one(&p, &[]).await.unwrap().get(0);
    assert_eq!(first, second);
    let messages = pg.simple_query(sql).await.unwrap();
    assert!(messages.iter().any(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(r) if r.get(0) == Some(first.to_string().as_str()))));
    drop(pg);
    conn.await.unwrap();
    let (task, conn) = f.pg(f.task.clone()).await;
    assert_eq!(code(task.query(sql, &[]).await.unwrap_err()), "42501");
    drop(task);
    conn.await.unwrap();
    f.stop().await;
}

include!("sqlstate_network.inc");
include!("immutable_planning_network.inc");
include!("filter_planning_network.inc");
include!("aggregate_scalar_network.inc");
include!("array_ordering_network.inc");
include!("uuid_array_network.inc");
include!("text_search_network.inc");
include!("count_parameter_network.inc");
include!("character_network.inc");
include!("pgparity_scalars_w06_network.inc");
include!("integer_formats_network.inc");
include!("trim_syntax_network.inc");
include!("substring_order_network.inc");
include!("order_select_items_network.inc");
include!("degree_math_network.inc");
include!("md5_functions_network.inc");
include!("overlay_functions_observe_network.inc");
include!("quoting_functions_network.inc");
include!("integer_input_network.inc");
include!("integer_cast_network.inc");
include!("insert_functions_network.inc");
include!("position_functions_network.inc");
include!("bytea_input_network.inc");

include!("routing_planning_network.inc");

mod text_nul_network {
    use super::*;
    include!("text_nul_network.inc");
}

include!("routing_rejection_network.inc");
include!("table_free_scalar_routing_network.inc");
include!("deferred_routine_surface_network.inc");

include!("sql_upsert_network.inc");
include!("partial_indexes_network.inc");

#[tokio::test]
async fn datagrip_ssl_probe_reports_tls_on_certificate_listener() {
    let f = Fixture::new().await;
    let (pg, connection) = f.pg(f.app.clone()).await;
    let statement = pg
        .prepare("select ssl from pg_stat_ssl where pid = pg_backend_pid()")
        .await
        .unwrap();
    let rows = pg.query(&statement, &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert!(rows[0].get::<_, bool>(0));
    drop(pg);
    connection.await.unwrap();
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exists_admission_returns_results() {
    let f = Fixture::new().await;
    let (pg, conn) = f.pg(f.app.clone()).await;
    pg.execute("INSERT INTO app(id) VALUES(1),(2)", &[]).await.unwrap();
    for (sql, count) in [("SELECT 1 WHERE EXISTS (SELECT NULL)", 1),
        ("SELECT id FROM app WHERE NOT EXISTS (SELECT NULL FROM app)", 0),
        ("SELECT 1 WHERE false OR NOT (EXISTS (SELECT NULL))", 0),
        ("SELECT id FROM app o WHERE EXISTS(SELECT 1/0 FROM app i WHERE i.id=o.id)", 2)] {
        let messages = pg.simple_query(sql).await.unwrap();
        assert_eq!(messages.iter().filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_))).count(), count);
        let statement = pg.prepare(sql).await.unwrap();
        assert_eq!(pg.query(&statement, &[]).await.unwrap().len(), count);
    }
    for sql in ["SELECT id FROM app WHERE EXISTS(SELECT missing FROM other)",
        "SELECT id FROM app WHERE EXISTS(SELECT 1/0 FROM other)",
        "SELECT id FROM app WHERE EXISTS(SELECT nosuchfn() FROM other)"] {
        assert_eq!(code(pg.simple_query(sql).await.unwrap_err()), "42501");
        assert_eq!(code(pg.prepare(sql).await.unwrap_err()), "42501");
    }
    for sql in ["SELECT id FROM app WHERE EXISTS(SELECT nosuchfn() FROM app)",
        "SELECT id FROM app WHERE EXISTS(SELECT public.abs(-1) FROM app)"] {
        assert_eq!(code(pg.simple_query(sql).await.unwrap_err()), "42883");
        assert_eq!(code(pg.prepare(sql).await.unwrap_err()), "42883");
    }
    assert_eq!(pg.execute("UPDATE app SET body='x' WHERE EXISTS(SELECT NULL)", &[]).await.unwrap(), 2);
    assert_eq!(pg.execute("DELETE FROM app WHERE NOT EXISTS(SELECT NULL LIMIT 0)", &[]).await.unwrap(), 2);
    drop(pg);
    conn.await.unwrap();
    f.stop().await;
}

// Connection helpers shared by SQL authority tests with NoSql certificates.
mod prepared_network {
    use super::*;
pub(super) async fn connect_at(
    address: std::net::SocketAddr,
    config: Arc<ClientConfig>,
) -> (tokio_postgres::Client, tokio::task::JoinHandle<()>) {
    let mut cfg = tokio_postgres::Config::new();
    cfg.host("localhost")
        .hostaddr(address.ip())
        .port(address.port())
        .user("giggles")
        .dbname("giggles")
        .ssl_mode(tokio_postgres::config::SslMode::Require);
    let (c, conn) = cfg
        .connect(tokio_postgres_rustls::MakeRustlsConnect::new(
            (*config).clone(),
        ))
        .await
        .unwrap();
    (
        c,
        tokio::spawn(async move {
            let _ = conn.await;
        }),
    )
}
pub(super) async fn raw_task(f: &Fixture) -> tokio_rustls::client::TlsStream<TcpStream> {
    let mut s = raw_tls(f, f.task.clone(), true).await;
    let mut startup = 196608u32.to_be_bytes().to_vec();
    startup.extend(b"user\0giggles\0database\0giggles\0\0");
    s.write_u32((startup.len() + 4) as u32).await.unwrap();
    s.write_all(&startup).await.unwrap();
    while pg_read(&mut s).await.0 != b'Z' {}
    s
}
}

async fn connection_closed(stream: &mut (impl tokio::io::AsyncRead + Unpin)) -> bool {
    tokio::time::timeout(std::time::Duration::from_secs(5), stream.read_u8())
        .await
        .is_ok_and(|result| result.is_err())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_application_crud_denied_table_aborts_transaction() {
    let f = Fixture::new().await;
    let mut c = f.native("app").await;
    assert_eq!(
        c.execute(sql(
            "INSERT INTO app (id,body) VALUES ($1,$2)",
            vec![Value::Int(1), Value::Text("hello".into())]
        ))
        .await
        .unwrap()
        .sql
        .unwrap()
        .affected,
        1
    );
    assert_eq!(
        row_values(
            &c.execute(sql("SELECT body FROM app WHERE id=$1", vec![Value::Int(1)]))
                .await
                .unwrap()
                .sql
                .unwrap()
                .rows[0]
        ),
        vec![Value::Text("hello".into())]
    );
    assert_eq!(c.execute(sql("BEGIN", vec![])).await.unwrap().code, "ok");
    assert_eq!(
        c.execute(sql("UPDATE app SET body='staged' WHERE id=1", vec![]))
            .await
            .unwrap()
            .code,
        "ok"
    );
    assert_eq!(
        c.execute(sql("SELECT * FROM other WHERE id=1", vec![]))
            .await
            .unwrap()
            .code,
        "42501"
    );
    assert_eq!(c.execute(sql("COMMIT", vec![])).await.unwrap().code, "25P02");
    assert_eq!(c.execute(sql("ROLLBACK", vec![])).await.unwrap().code, "ok");
    assert_eq!(
        row_values(
            &c.execute(sql("SELECT body FROM app WHERE id=$1", vec![Value::Int(1)]))
                .await
                .unwrap()
                .sql
                .unwrap()
                .rows[0]
        ),
        vec![Value::Text("hello".into())]
    );
    drop(c);
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn malformed_denied_native_flood_spends_global_rate_without_sql_effects() {
    let f = Fixture::new().await;
    let (_service, n, p) = lifecycle_network::limited(
        &f,
        giggles_server::admission::ServiceLimits {
            sessions: 8,
            burst: 5,
            per_second: 1,
        },
    )
    .await;
    let mut socket = TcpStream::connect(n.address).await.unwrap();
    let mut s = tokio_rustls::TlsConnector::from(f.app.clone())
        .connect(
            rustls::pki_types::ServerName::try_from("localhost").unwrap(),
            &mut socket,
        )
        .await
        .unwrap();
    raw_wire_open(&mut s).await;
    let denied = sql("SELECT * FROM other", vec![]).encode_to_vec();
    for (id, payload, expected) in [
        (2, b"{".as_slice(), "INVALID_ARGUMENT"),
        (3, denied.as_slice(), "42501"),
        (4, b"[]".as_slice(), "INVALID_ARGUMENT"),
        (5, b"false".as_slice(), "INVALID_ARGUMENT"),
    ] {
        raw_wire_execute(&mut s, id, payload).await;
        let code = match expected {
            "42501" => {
                let (header, body) = raw_wire_read(&mut s).await.unwrap();
                assert_eq!(
                    (header.kind, header.flags, header.request_id),
                    (FrameKind::Reply, Flags(0), id)
                );
                WireReply::decode(body.as_slice()).unwrap().reply.unwrap().code
            }
            _ => raw_wire_error(&mut s, id).await.code,
        };
        assert_eq!(code, expected);
    }
    // One connection plus four attempts exhaust the shared burst. Wire
    // reports admission failure with a request-scoped error.
    raw_wire_execute(
        &mut s,
        6,
        &sql("INSERT INTO app (id) VALUES (55)", vec![]).encode_to_vec(),
    )
    .await;
    let (header, body) = raw_wire_read(&mut s).await.unwrap();
    let insert_code = match header.kind {
        FrameKind::ConnError => WireConnError::decode(body.as_slice()).unwrap().code,
        FrameKind::Reply => WireReply::decode(body.as_slice()).unwrap().reply.unwrap().code,
        _ => panic!("unexpected insert response: {header:?}"),
    };
    let rows = f
        .admin
        .sql_session()
        .execute("SELECT id FROM app LIMIT 1", &[], None)
        .unwrap()
        .rows;
    let mut reconnect = TcpStream::connect(p.address).await.unwrap();
    let reconnect_closed = connection_closed(&mut reconnect).await;
    n.shutdown().await.unwrap();
    p.shutdown().await.unwrap();
    f.stop().await;
    assert_eq!(
        (
            header.kind,
            header.flags,
            header.request_id,
            insert_code.as_str(),
            rows.is_empty(),
            reconnect_closed,
        ),
        (FrameKind::ConnError, Flags(0), 6, "RESOURCE_EXHAUSTED", true, true),
        "rate witness left rows: {rows:?}"
    );
}

/// Protobuf-valid requests that name a task or operation: the wire has no
/// principal/time/task fields, so these are the forms that could try to
/// select authority other than the certificate-bound session.
fn task_prototype_probes() -> Vec<proto::ExecuteRequest> {
    use proto::execute_request::Request as R;
    let mut probes: Vec<proto::ExecuteRequest> = [
        R::Ping(proto::PingRequest {}),
        R::Sql(proto::SqlRequest { sql: "SELECT id FROM app".into(), params: vec![] }),
    ]
    .into_iter()
    .map(|request| proto::ExecuteRequest {
        request: Some(request),
        budget_ms: None,
        operation_id: Some("task-2".into()),
    })
    .collect();
    probes.extend(
        [
            R::TaskPreview(proto::TaskPreviewRequest { change: None }),
            R::TaskApply(proto::TaskApplyRequest { change_id: Some(3), digest: None }),
            R::TaskOutcome(proto::TaskOutcomeRequest {}),
        ]
        .into_iter()
        .map(|request| proto::ExecuteRequest {
            request: Some(request),
            budget_ms: None,
            operation_id: Some("task-2".into()),
        }),
    );
    probes
}

include!("catalog_scale_network.inc");
#[path = "temporal_index_network.inc"]
mod temporal_index_network;

include!("dml_sources_network.inc");

include!("generated_columns_network.inc");
