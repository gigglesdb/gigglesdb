//! Native wire role sign-in and listener lifecycle.
mod api_support;

use api_support::{Fixture, sql};
use giggles_core::roles::NewRole;
use giggles_server::{
    Running,
    api::{ApiLimits, ApiRuntime, error::ApiError, proto},
    auth::{RoleApiConfig, RoleApiListener, TlsMode},
    tls,
    transport::IO_TIMEOUT,
    wire::{
        client::{Client, ClientTls, Credential, WireError},
        frame::{Flags, FrameKind},
        listen::serve_role_wire_bound,
        messages::WireConnError,
    },
};
use prost::Message;
use serde_json::json;
use std::{io, net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::watch,
    time::{sleep, timeout},
};

const ROLE: &str = "wire_user";
const ADMIN: &str = "wire_admin";
const PASSWORD: &str = "wire test secret";

async fn fixture() -> Fixture {
    let f = Fixture::catalog_empty().await;
    for (name, superuser) in [(ROLE, false), (ADMIN, true)] {
        f.admin
            .roles()
            .create(NewRole {
                name: name.into(),
                login: true,
                superuser,
                password: (!superuser)
                    .then(|| giggles_server::auth::scram::Verifier::generate(PASSWORD)),
                ..Default::default()
            })
            .unwrap();
    }
    f
}

fn settings(f: &Fixture, mode: TlsMode, trust: bool, connections: usize) -> RoleApiListener {
    let mut value = json!({
        "wire": "127.0.0.1:0",
        "tls": "disabled",
        "wire_tls": mode,
        "loopback_trust": trust,
        "connections": connections,
    });
    if mode != TlsMode::Disabled {
        value["certificate"] = json!(f.path.join("server.pem"));
        value["private_key"] = json!(f.path.join("server.key.TEST-ONLY"));
        value["client_ca"] = json!(f.path.join("ca.pem"));
        value["certificates"] = json!({(tls::fingerprint(f.unbound.cert.der().as_ref())): ROLE});
    }
    serde_json::from_value::<RoleApiConfig>(value)
        .unwrap()
        .build()
        .unwrap()
}

struct RoleWire {
    listener: Running,
    runtime: Arc<ApiRuntime>,
}

impl RoleWire {
    async fn start(f: &Fixture, settings: RoleApiListener) -> Self {
        let socket = TcpListener::bind(settings.wire.unwrap()).await.unwrap();
        let settings = Arc::new(settings);
        let runtime =
            ApiRuntime::new_role(f.service.clone(), ApiLimits::default(), settings.clone())
                .unwrap();
        let (_, gate) = watch::channel(true);
        let listener = serve_role_wire_bound(socket, runtime.clone(), settings, gate)
            .await
            .unwrap();
        Self { listener, runtime }
    }

    fn address(&self) -> SocketAddr {
        self.listener.address
    }

    async fn stop(self) {
        self.listener.shutdown().await.unwrap();
        self.runtime.shutdown().await;
    }
}

fn open(role: &str) -> proto::OpenSessionRequest {
    proto::OpenSessionRequest {
        role: Some(role.into()),
        database: Some("postgres".into()),
        ..Default::default()
    }
}

fn scram(password: &str) -> Credential {
    Credential::Scram {
        role: ROLE.into(),
        password: password.into(),
        database: None,
    }
}

fn client_tls(f: &Fixture, mapped: bool) -> ClientTls {
    ClientTls {
        ca: f.ca.cert.pem().into_bytes(),
        cert: mapped.then(|| f.unbound.cert.pem().into_bytes()),
        key: mapped.then(|| f.unbound.key.serialize_pem().into_bytes()),
        server_name: "localhost".into(),
    }
}

fn ping() -> proto::ExecuteRequest {
    proto::ExecuteRequest {
        request: Some(proto::execute_request::Request::Ping(proto::PingRequest {})),
        ..Default::default()
    }
}

fn assert_wire_error(error: &io::Error, expected: ApiError, context: &str) {
    let wire = error
        .get_ref()
        .and_then(|error| error.downcast_ref::<WireError>())
        .unwrap_or_else(|| panic!("{context}: {error:?}"));
    assert_eq!(
        (wire.code.as_str(), wire.message.as_str()),
        (expected.code(), expected.message()),
        "{context}"
    );
}

#[tokio::test]
async fn wire_role_auth_matrix() {
    let f = fixture().await;
    for (label, mode, tls) in [
        ("disabled", TlsMode::Disabled, false),
        ("optional plain", TlsMode::Optional, false),
        ("optional TLS", TlsMode::Optional, true),
        ("required", TlsMode::Required, true),
    ] {
        let wire = RoleWire::start(&f, settings(&f, mode, false, 32)).await;
        let trusted = RoleWire::start(&f, settings(&f, mode, true, 32)).await;
        let password = proto::OpenSessionRequest {
            credential: Some(proto::open_session_request::Credential::Password(
                PASSWORD.into(),
            )),
            ..open(ROLE)
        };
        for (method, listener, open, credential, cert, allowed) in [
            (
                "SCRAM good",
                &wire,
                open(ROLE),
                scram(PASSWORD),
                false,
                true,
            ),
            ("SCRAM bad", &wire, open(ROLE), scram("wrong"), false, false),
            (
                "password",
                &wire,
                password.clone(),
                Credential::None,
                false,
                tls,
            ),
            (
                "loopback trust",
                &trusted,
                open(ROLE),
                Credential::None,
                false,
                true,
            ),
            (
                "mapped certificate",
                &wire,
                open(ROLE),
                Credential::None,
                true,
                tls,
            ),
            (
                "password with trust",
                &trusted,
                password,
                Credential::None,
                false,
                tls,
            ),
        ] {
            let result = Client::connect(
                listener.address(),
                tls.then(|| client_tls(&f, cert)),
                open,
                credential,
            )
            .await;
            let context = format!("{label}, {method}");
            if allowed {
                let (mut client, _) = result.unwrap_or_else(|error| panic!("{context}: {error:?}"));
                assert_eq!(
                    client.execute(ping()).await.unwrap().code,
                    "ok",
                    "{context}"
                );
                client.goodbye().await.unwrap();
            } else {
                let error = result
                    .err()
                    .unwrap_or_else(|| panic!("{context}: opened a session"));
                assert_wire_error(&error, ApiError::Unauthenticated, &context);
            }
        }
        wire.stop().await;
        trusted.stop().await;
    }
    f.stop().await;
}

#[tokio::test]
async fn wire_optional_port_sniffs_tls_and_plain() {
    let f = fixture().await;
    let wire = RoleWire::start(&f, settings(&f, TlsMode::Optional, false, 2)).await;
    let (mut plain, _) = Client::connect(wire.address(), None, open(ROLE), scram(PASSWORD))
        .await
        .unwrap();
    let (mut tls, _) = Client::connect(
        wire.address(),
        Some(client_tls(&f, true)),
        open(ROLE),
        Credential::None,
    )
    .await
    .unwrap();
    for client in [&mut plain, &mut tls] {
        assert_eq!(client.execute(ping()).await.unwrap().code, "ok");
    }
    plain.goodbye().await.unwrap();
    tls.goodbye().await.unwrap();
    wire.stop().await;
    f.stop().await;
}

#[tokio::test]
async fn wire_optional_port_rejects_garbage_and_other_protocols() {
    const CONNECTIONS: usize = 8;
    let f = fixture().await;
    let wire = RoleWire::start(&f, settings(&f, TlsMode::Optional, true, CONNECTIONS)).await;
    let pg_body = b"\0\x03\0\0user\0wire_user\0database\0postgres\0\0";
    let mut pg_start = ((4 + pg_body.len()) as u32).to_be_bytes().to_vec();
    pg_start.extend_from_slice(pg_body);
    let mut garbage = [0; 64];
    getrandom::fill(&mut garbage).unwrap();
    // Keep this case in the unrecognized branch. HTTP covers the plain branch.
    garbage[0] |= 0x80;
    let mut checks = tokio::task::JoinSet::new();
    for (name, bytes) in [
        ("HTTP", b"GET / HTTP/1.1\r\n\r\n".as_slice()),
        ("pgwire startup", pg_start.as_slice()),
        ("random bytes", garbage.as_slice()),
        ("silence", &[]),
        // A valid first byte followed by a stall: the preface and the TLS
        // handshake stages are bounded too.
        ("lone G", b"G"),
        ("lone TLS byte", &[0x16]),
    ] {
        let mut socket = TcpStream::connect(wire.address()).await.unwrap();
        socket.write_all(bytes).await.unwrap();
        checks.spawn(async move {
            let mut bytes = Vec::new();
            let result = timeout(
                IO_TIMEOUT + Duration::from_secs(1),
                socket.read_to_end(&mut bytes),
            )
            .await
            .unwrap_or_else(|_| panic!("{name}: socket stayed open"));
            // Dropping a socket with unread input can reset the connection.
            if let Err(error) = result {
                assert_eq!(
                    error.kind(),
                    io::ErrorKind::ConnectionReset,
                    "{name}: {error}"
                );
            }
        });
    }
    while let Some(result) = checks.join_next().await {
        result.unwrap();
    }

    let mut clients = Vec::new();
    for use_tls in (0..CONNECTIONS).map(|n| n % 2 == 1) {
        let (mut client, _) = Client::connect(
            wire.address(),
            use_tls.then(|| client_tls(&f, false)),
            open(ROLE),
            Credential::None,
        )
        .await
        .unwrap();
        assert_eq!(client.execute(ping()).await.unwrap().code, "ok");
        clients.push(client);
    }
    assert_eq!(clients.len(), CONNECTIONS);
    for client in clients {
        client.goodbye().await.unwrap();
    }
    wire.stop().await;
    f.stop().await;
}

async fn expect_closed(client: &mut Client) {
    let error = timeout(Duration::from_secs(5), client.recv_raw_frame())
        .await
        .expect("server closes the connection")
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
}

#[tokio::test]
async fn wire_role_idle_timeout_rolls_back_and_closes() {
    let f = fixture().await;
    let wire = RoleWire::start(
        &f,
        RoleApiListener {
            idle_timeout_seconds: 1,
            ..settings(&f, TlsMode::Disabled, true, 2)
        },
    )
    .await;
    let (mut client, _) = Client::connect(wire.address(), None, open(ADMIN), Credential::None)
        .await
        .unwrap();
    for statement in [
        "CREATE TABLE wire_idle (id BIGINT PRIMARY KEY)",
        "BEGIN",
        "INSERT INTO wire_idle VALUES (42)",
    ] {
        let result = client.execute(sql(statement)).await.unwrap();
        assert_eq!(result.code, "ok", "{statement}: {result:?}");
    }
    sleep(Duration::from_millis(2500)).await;
    let (header, body) = client.recv_raw_frame().await.unwrap();
    assert_eq!(
        (header.kind, header.flags, header.request_id),
        (FrameKind::ConnError, Flags(0), 0)
    );
    let error = WireConnError::decode(body.as_slice()).unwrap();
    assert_eq!(error.code, ApiError::SessionNotFound.code());
    expect_closed(&mut client).await;

    let (mut observer, _) = Client::connect(wire.address(), None, open(ADMIN), Credential::None)
        .await
        .unwrap();
    let reply = observer
        .execute(sql("SELECT id FROM wire_idle"))
        .await
        .unwrap();
    assert_eq!(reply.code, "ok", "{reply:?}");
    assert!(reply.sql.unwrap().rows.is_empty());
    observer.goodbye().await.unwrap();
    wire.stop().await;
    f.stop().await;
}

#[tokio::test]
async fn wire_role_nologin_while_connected_ends_session() {
    let f = fixture().await;
    let wire = RoleWire::start(&f, settings(&f, TlsMode::Disabled, false, 2)).await;
    let (mut client, _) = Client::connect(wire.address(), None, open(ROLE), scram(PASSWORD))
        .await
        .unwrap();
    assert_eq!(client.execute(ping()).await.unwrap().code, "ok");
    wire.runtime
        .test_wire_connection(client.local_addr())
        .unwrap()
        .hold_idle_checks();

    let pg_config: giggles_server::auth::PostgresqlConfig = serde_json::from_value(json!({
        "listen": "127.0.0.1:0", "tls": "disabled", "loopback_trust": true,
    }))
    .unwrap();
    let pg = giggles_server::auth::serve_postgresql(
        pg_config.listen,
        f.service.clone(),
        Arc::new(pg_config.build().unwrap()),
    )
    .await
    .unwrap();
    let (admin, connection) = tokio_postgres::Config::new()
        .host("127.0.0.1")
        .port(pg.address.port())
        .user(ADMIN)
        .dbname("postgres")
        .connect(tokio_postgres::NoTls)
        .await
        .unwrap();
    let pg_task = tokio::spawn(connection);
    admin
        .batch_execute("ALTER ROLE wire_user NOLOGIN")
        .await
        .unwrap();

    let error = client.execute(ping()).await.unwrap_err();
    assert_wire_error(
        &error,
        ApiError::Unauthenticated,
        "next request after NOLOGIN",
    );
    expect_closed(&mut client).await;

    drop(admin);
    pg_task.await.unwrap().unwrap();
    pg.shutdown().await.unwrap();
    wire.stop().await;
    f.stop().await;
}
