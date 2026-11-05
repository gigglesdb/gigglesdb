use giggles_core::{
    Admin, Column, Grant, Key, Limits, OperationId, Policy, Principal, Row, RowId, Selection,
    Status, TaskId, TenantId, Time, relational::Value,
};
use giggles_server::{
    Binding, Client, Request, Response, Role, Running, Service, clock::Clock, serve, serve_pg, tls,
};
use rcgen::{
    BasicConstraints, Certificate, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair,
};
use rustls::{ClientConfig, pki_types::PrivatePkcs8KeyDer};
use std::{
    collections::BTreeSet,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
static SERIAL: AtomicU64 = AtomicU64::new(0);
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
struct Fixture {
    path: PathBuf,
    admin: Admin,
    native: Running,
    pg: Running,
    app: Arc<ClientConfig>,
    task: Arc<ClientConfig>,
    other: Arc<ClientConfig>,
    unbound: Arc<ClientConfig>,
    wrong: Arc<ClientConfig>,
    server_config: Arc<rustls::ServerConfig>,
    service: Arc<Service>,
    ca: Cert,
}
impl Fixture {
    async fn new() -> Self {
        let path = PathBuf::from("artifacts").join(format!(
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
        let admin = Admin::create(path.join("db.redb"), Limits::default()).unwrap();
        let mut sql = admin.sql_session();
        sql.execute("CREATE TABLE app (id BIGINT PRIMARY KEY, body TEXT, flag BOOLEAN, small SMALLINT, num INTEGER, data BYTEA)",&[],None).unwrap();
        sql.execute(
            "CREATE TABLE other (id BIGINT PRIMARY KEY, body TEXT)",
            &[],
            None,
        )
        .unwrap();
        drop(sql);
        for id in 0..30 {
            admin
                .put(
                    Key {
                        tenant: TenantId(42),
                        row: RowId(id),
                    },
                    Row {
                        status: Status::Failed,
                        failed_at: 103,
                        attempt_count: 0,
                        version: 0,
                    },
                )
                .unwrap();
        }
        admin.grant(grant(1, 42, 25, 5)).unwrap();
        admin.grant(grant(2, 43, 25, 5)).unwrap();
        let domain = admin.authority_metadata().unwrap().0.to_string();
        let bindings = vec![
            Binding {
                fingerprint: tls::fingerprint(app.cert.der()),
                role: Role::Application {
                    domain: domain.clone(),
                    tables: BTreeSet::from(["app".into()]),
                },
            },
            Binding {
                fingerprint: tls::fingerprint(task.cert.der()),
                role: Role::Task {
                    domain: domain.clone(),
                    task: TaskId(1),
                    principal: Principal(2),
                },
            },
            Binding {
                fingerprint: tls::fingerprint(other.cert.der()),
                role: Role::Task {
                    domain,
                    task: TaskId(2),
                    principal: Principal(2),
                },
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
        let native = serve(
            "127.0.0.1:0".parse().unwrap(),
            service.clone(),
            server_config.clone(),
            32,
        )
        .await
        .unwrap();
        let pg = serve_pg(
            "127.0.0.1:0".parse().unwrap(),
            service.clone(),
            server_config.clone(),
            32,
        )
        .await
        .unwrap();
        let config = serde_json::json!({"database":path.join("db.redb"),"native":"127.0.0.1:7432","postgres":"127.0.0.1:7433","certificate":path.join("server.pem"),"private_key":path.join("server.key.TEST-ONLY"),"client_ca":path.join("ca.pem"),"bindings":bindings,"connections":32,"skew_seconds":0,"max_pause_seconds":3600});
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
            wrong: client(&wrong, &ca),
            server_config,
            service,
            ca,
        }
    }
    async fn native(&self, config: Arc<ClientConfig>) -> Client {
        Client::connect(self.native.address, "localhost", config)
            .await
            .unwrap()
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
fn grant(task: u64, tenant: u64, limit: u64, approval: u64) -> Grant {
    Grant {
        task: TaskId(task),
        issuer: Principal(1),
        executor: Principal(2),
        approver: Principal(3),
        policy: Policy {
            tenant: TenantId(tenant),
            columns: BTreeSet::from([Column::Status]),
            transitions: BTreeSet::from([(Status::Failed, Status::Queued)]),
            incident: (100, 107),
            distinct_limit: limit,
            transition_limit: limit,
            approval_above: approval,
            valid_from: 1,
            expires: now() + 7200,
        },
    }
}
fn repair(op: u64, ids: std::ops::Range<u64>) -> giggles_core::Request {
    giggles_core::Request {
        operation: OperationId(op),
        selection: Selection::Keys(
            ids.map(|row| Key {
                tenant: TenantId(42),
                row: RowId(row),
            })
            .collect(),
        ),
        column: Column::Status,
        after: Status::Queued,
    }
}
fn sql(sql: &str, params: Vec<Value>) -> Request {
    Request::Sql {
        sql: sql.into(),
        params,
    }
}
fn code(e: tokio_postgres::Error) -> String {
    e.code().unwrap().code().into()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_crud_transactions_and_role_boundaries() {
    let f = Fixture::new().await;
    let mut c = f.native(f.app.clone()).await;
    assert_eq!(
        c.call(&sql(
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
        c.call(&sql(
            "SELECT body FROM app WHERE id=$1",
            vec![Value::Int(1)]
        ))
        .await
        .unwrap()
        .sql
        .unwrap()
        .rows[0]
            .values,
        vec![Value::Text("hello".into())]
    );
    assert_eq!(c.call(&sql("BEGIN", vec![])).await.unwrap().code, "ok");
    assert_eq!(
        c.call(&sql("UPDATE app SET body='staged' WHERE id=1", vec![]))
            .await
            .unwrap()
            .code,
        "ok"
    );
    assert_eq!(
        c.call(&sql("SELECT * FROM other WHERE id=1", vec![]))
            .await
            .unwrap()
            .code,
        "42501"
    );
    assert_eq!(c.call(&sql("COMMIT", vec![])).await.unwrap().code, "25P02");
    assert_eq!(c.call(&sql("ROLLBACK", vec![])).await.unwrap().code, "ok");
    let mut t = f.native(f.task.clone()).await;
    for query in [
        "SELECT * FROM app LIMIT 1",
        "SELECT * FROM deliveries LIMIT 1",
        "SET ROLE admin",
        "COPY app FROM STDIN",
        "CREATE TABLE evil (id BIGINT PRIMARY KEY)",
        "BEGIN; COMMIT",
    ] {
        assert_ne!(
            t.call(&sql(query, vec![])).await.unwrap().code,
            "ok",
            "{query}"
        );
    }
    assert_eq!(
        t.call(&Request::Status {
            operation: OperationId(1)
        })
        .await
        .unwrap()
        .code,
        "absent"
    );
    assert_eq!(
        c.call(&Request::Status {
            operation: OperationId(1)
        })
        .await
        .unwrap()
        .code,
        "42501"
    );
    drop(c);
    drop(t);
    f.stop().await;
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
    let body = "nul\0 unicode λ";
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
    assert_eq!(
        code(
            c.prepare_typed(
                "SELECT id FROM app WHERE id=$1",
                &[tokio_postgres::types::Type::FLOAT8]
            )
            .await
            .unwrap_err()
        ),
        "0A000"
    );
    for query in [
        "SET ROLE admin",
        "COPY app FROM STDIN",
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

async fn task_wire(c: &tokio_postgres::Client, request: &Request) -> Response {
    let messages = c
        .simple_query(&format!(
            "GIGGLES {}",
            serde_json::to_string(request).unwrap()
        ))
        .await
        .unwrap();
    let row = messages
        .iter()
        .find_map(|m| {
            if let tokio_postgres::SimpleQueryMessage::Row(row) = m {
                Some(row)
            } else {
                None
            }
        })
        .expect("expected task row");
    serde_json::from_str(row.get(0).unwrap()).unwrap()
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_and_wire_task_approval_retry_status_redaction_and_cross_tenant() {
    let f = Fixture::new().await;
    let mut c = f.native(f.task.clone()).await;
    let (pg, conn) = f.pg(f.task.clone()).await;
    let r = repair(10, 0..6);
    let preview = c
        .call(&Request::Preview { request: r.clone() })
        .await
        .unwrap();
    assert_eq!(preview.code, "preview");
    assert_eq!(preview.count, Some(6));
    let p = f
        .admin
        .executor(TaskId(1), Principal(2))
        .unwrap()
        .preview(&r, Time::Certified(now()))
        .unwrap();
    let approval = f
        .admin
        .approve(Principal(3), p, Time::Certified(now()))
        .unwrap();
    let exec = Request::Execute {
        request: r.clone(),
        approval: Some(approval),
    };
    let a = c.call(&exec).await.unwrap();
    assert_eq!(a.code, "committed");
    assert_eq!(a.count, Some(6));
    let b = task_wire(&pg, &exec).await;
    assert_eq!(a, b);
    assert_eq!(
        c.call(&Request::Status {
            operation: OperationId(10)
        })
        .await
        .unwrap(),
        a
    );
    assert_eq!(
        task_wire(
            &pg,
            &Request::Status {
                operation: OperationId(10)
            }
        )
        .await,
        a
    );
    assert_eq!(preview.digest, a.digest);
    let raw = serde_json::to_string(&a).unwrap();
    for forbidden in [
        "issuer",
        "principal",
        "policy",
        "before",
        "after",
        "tenant",
        "dependencies",
        "lifetime",
    ] {
        assert!(!raw.contains(forbidden));
    }
    let mut other = f.native(f.other.clone()).await;
    assert_eq!(
        other
            .call(&Request::Status {
                operation: OperationId(10)
            })
            .await
            .unwrap()
            .code,
        "absent"
    );
    let mut out = repair(11, 8..9);
    out.selection = Selection::Keys(vec![Key {
        tenant: TenantId(43),
        row: RowId(8),
    }]);
    assert_eq!(
        c.call(&Request::Execute {
            request: out,
            approval: None
        })
        .await
        .unwrap()
        .code,
        "42501"
    );
    f.admin.revoke(TaskId(1)).unwrap();
    for request in [
        Request::Status {
            operation: OperationId(10),
        },
        exec,
        Request::Preview { request: r },
        Request::RepairSql {
            operation: OperationId(12),
            sql: "UPDATE deliveries SET status = 'queued' WHERE tenant = 42 AND id = 7".into(),
            approval: None,
        },
    ] {
        assert_eq!(c.call(&request).await.unwrap().code, "42501");
        assert_eq!(task_wire(&pg, &request).await.code, "42501");
    }
    drop(c);
    drop(other);
    drop(pg);
    conn.await.unwrap();
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn ten_real_tls_sessions_race_for_last_slot() {
    let f = Fixture::new().await;
    // Offline provision a distinct grant with one slot; use an isolated service binding.
    f.admin.grant(grant(3, 42, 1, 1)).unwrap();
    let fingerprint = f.service.session(&tls::fingerprint(f.ca.cert.der()));
    assert!(fingerprint.is_err());
    let certificate = std::fs::read_to_string(f.path.join("task.pem")).unwrap();
    let der = rustls_pemfile::certs(&mut std::io::Cursor::new(certificate))
        .next()
        .unwrap()
        .unwrap();
    let service = Service::new(
        f.admin.clone(),
        vec![Binding {
            fingerprint: tls::fingerprint(&der),
            role: Role::Task {
                domain: f.admin.authority_metadata().unwrap().0.to_string(),
                task: TaskId(3),
                principal: Principal(2),
            },
        }],
        Clock::new(0, 3600).unwrap(),
    )
    .unwrap();
    let running = serve(
        "127.0.0.1:0".parse().unwrap(),
        service.clone(),
        f.server_config.clone(),
        16,
    )
    .await
    .unwrap();
    let running_pg = serve_pg(
        "127.0.0.1:0".parse().unwrap(),
        service,
        f.server_config.clone(),
        16,
    )
    .await
    .unwrap();
    let barrier = Arc::new(tokio::sync::Barrier::new(10));
    let mut tasks = vec![];
    for i in 0..10 {
        let addr = running.address;
        let pg_addr = running_pg.address;
        let config = f.task.clone();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            let request = Request::Execute {
                request: repair(100 + i, i..i + 1),
                approval: None,
            };
            if i % 2 == 0 {
                let mut c = Client::connect(addr, "localhost", config).await.unwrap();
                barrier.wait().await;
                c.call(&request).await.unwrap()
            } else {
                let mut cfg = tokio_postgres::Config::new();
                cfg.host("localhost")
                    .hostaddr(pg_addr.ip())
                    .port(pg_addr.port())
                    .user("giggles")
                    .dbname("giggles")
                    .ssl_mode(tokio_postgres::config::SslMode::Require);
                let (c, conn) = cfg
                    .connect(tokio_postgres_rustls::MakeRustlsConnect::new(
                        (*config).clone(),
                    ))
                    .await
                    .unwrap();
                let handle = tokio::spawn(async move {
                    let _ = conn.await;
                });
                barrier.wait().await;
                let response = task_wire(&c, &request).await;
                drop(c);
                handle.await.unwrap();
                response
            }
        }));
    }
    let mut successes = 0;
    for task in tasks {
        let response = task.await.unwrap();
        if response.code == "committed" {
            successes += 1;
        } else {
            assert_eq!(response.code, "54000");
        }
    }
    assert_eq!(successes, 1);
    let snap = f.admin.snapshot().unwrap();
    assert_eq!(snap.tasks[&TaskId(3)].transitions, 1);
    assert_eq!(
        snap.rows
            .values()
            .filter(|r| r.status == Status::Queued)
            .count(),
        1
    );
    running.shutdown().await.unwrap();
    running_pg.shutdown().await.unwrap();
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn authentication_tls_downgrade_unknown_certificate_and_spoof() {
    let f = Fixture::new().await;
    for config in [f.wrong.clone(), f.unbound.clone()] {
        match Client::connect(f.native.address, "localhost", config).await {
            Err(_) => {}
            Ok(mut c) => assert!(c.call(&Request::Ping).await.is_err()),
        }
    }
    assert!(
        Client::connect(f.native.address, "wrong-host", f.app.clone())
            .await
            .is_err()
    );
    let no_cert = Arc::new(
        ClientConfig::builder()
            .with_root_certificates(tls::roots(vec![f.ca.cert.der().clone()]).unwrap())
            .with_no_client_auth(),
    );
    match Client::connect(f.native.address, "localhost", no_cert).await {
        Err(_) => {}
        Ok(mut c) => assert!(c.call(&Request::Ping).await.is_err()),
    }
    let mut plain = TcpStream::connect(f.pg.address).await.unwrap();
    plain.write_all(&[0, 0, 0, 8, 0, 3, 0, 0]).await.unwrap();
    assert!(plain.read_u8().await.is_err());
    let mut tls_stream = raw_tls(&f, f.task.clone(), false).await;
    for json in [
        r#"{"kind":"Status","operation":1,"principal":3}"#,
        r#"{"kind":"Status","operation":1,"time":{"Certified":0}}"#,
        r#"{"kind":"Status","operation":1,"task":2}"#,
        r#"{"kind":"grant","task":3}"#,
    ] {
        giggles_server::write_frame(&mut tls_stream, json.as_bytes(), giggles_server::MAX_FRAME)
            .await
            .unwrap();
        let r: Response = serde_json::from_slice(
            &giggles_server::read_frame(&mut tls_stream, giggles_server::MAX_RESPONSE)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(r.code, "08P01");
    }
    drop(tls_stream);
    f.stop().await;
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
async fn approval_revocation_staleness_expiry_and_unknown_status() {
    let f = Fixture::new().await;
    let mut c = f.native(f.task.clone()).await;
    for (op, revoke) in [(20, true), (21, false)] {
        let r = repair(op, 0..6);
        let p = f
            .admin
            .executor(TaskId(1), Principal(2))
            .unwrap()
            .preview(&r, Time::Certified(now()))
            .unwrap();
        let a = f
            .admin
            .approve(Principal(3), p, Time::Certified(now()))
            .unwrap();
        if revoke {
            f.admin.revoke_approval(a).unwrap();
        } else {
            f.admin
                .put(
                    Key {
                        tenant: TenantId(42),
                        row: RowId(0),
                    },
                    Row {
                        status: Status::Failed,
                        failed_at: 103,
                        attempt_count: 1,
                        version: 0,
                    },
                )
                .unwrap();
        }
        assert_eq!(
            c.call(&Request::Execute {
                request: r,
                approval: Some(a)
            })
            .await
            .unwrap()
            .code,
            "GD001"
        );
    }
    assert_eq!(
        c.call(&Request::Execute {
            request: repair(22, 0..6),
            approval: None
        })
        .await
        .unwrap()
        .code,
        "GD001"
    );
    let mut expired = grant(4, 42, 1, 1);
    expired.policy.expires = now() - 1;
    f.admin.grant(expired).unwrap();
    let e = f.admin.executor(TaskId(4), Principal(2)).unwrap();
    assert!(e.status_current(OperationId(1), now(), now()).is_err());
    assert!(e.preview_current(&repair(1, 0..1), now(), now()).is_err());
    assert!(
        e.execute_current(repair(1, 0..1), now(), now(), None)
            .is_err()
    );
    let mut future = grant(5, 42, 1, 1);
    future.policy.valid_from = now() + 1;
    f.admin.grant(future).unwrap();
    let e = f.admin.executor(TaskId(5), Principal(2)).unwrap();
    assert!(
        e.execute_current(repair(1, 0..1), now(), now() + 2, None)
            .is_err()
    );
    drop(c);
    f.stop().await;
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
    assert_eq!(code(c.query(&p, &[&1i64]).await.unwrap_err()), "0A000");
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
    for n in [0, u32::MAX, giggles_server::MAX_FRAME as u32 + 1] {
        let mut s = raw_tls(&f, f.task.clone(), false).await;
        s.write_u32(n).await.unwrap();
        assert!(s.read_u8().await.is_err());
    }
    assert!(giggles_server::frame_len(u32::MAX, 256).is_err());
    assert!(giggles_server::clock::interval(u64::MAX, 1).is_err());
    assert!(giggles_server::clock::interval(0, 1).is_err());
    let running = serve(
        "127.0.0.1:0".parse().unwrap(),
        f.service.clone(),
        f.server_config.clone(),
        1,
    )
    .await
    .unwrap();
    let mut first = Client::connect(running.address, "localhost", f.task.clone())
        .await
        .unwrap();
    first.call(&Request::Ping).await.unwrap();
    assert!(
        Client::connect(running.address, "localhost", f.task.clone())
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
async fn pg_send<S: tokio::io::AsyncWrite + Unpin>(s: &mut S, tag: u8, b: &[u8]) {
    s.write_u8(tag).await.unwrap();
    s.write_u32((b.len() + 4) as u32).await.unwrap();
    s.write_all(b).await.unwrap();
    s.flush().await.unwrap();
}
async fn pg_read<S: tokio::io::AsyncRead + Unpin>(s: &mut S) -> (u8, Vec<u8>) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let tag = s.read_u8().await.unwrap();
        let n = s.read_u32().await.unwrap();
        assert!((4..=8 * 1024 * 1024).contains(&n));
        let mut b = vec![0; n as usize - 4];
        s.read_exact(&mut b).await.unwrap();
        (tag, b)
    })
    .await
    .expect("wire response timeout")
}
async fn pg_sync<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin>(
    s: &mut S,
) -> Vec<(u8, Vec<u8>)> {
    pg_send(s, b'S', &[]).await;
    let mut frames = vec![];
    loop {
        let f = pg_read(s).await;
        let end = f.0 == b'Z';
        frames.push(f);
        if end {
            return frames;
        }
    }
}
fn cstr(s: &str, b: &mut Vec<u8>) {
    b.extend(s.as_bytes());
    b.push(0);
}
fn parse(name: &str, sql: &str) -> Vec<u8> {
    let mut b = vec![];
    cstr(name, &mut b);
    cstr(sql, &mut b);
    b.extend(0u16.to_be_bytes());
    b
}
fn bind(portal: &str, stmt: &str, params: &[Option<&str>], binary_result: bool) -> Vec<u8> {
    let mut b = vec![];
    cstr(portal, &mut b);
    cstr(stmt, &mut b);
    b.extend(0u16.to_be_bytes());
    b.extend((params.len() as u16).to_be_bytes());
    for v in params {
        if let Some(v) = v {
            b.extend((v.len() as i32).to_be_bytes());
            b.extend(v.as_bytes());
        } else {
            b.extend((-1i32).to_be_bytes());
        }
    }
    b.extend(1u16.to_be_bytes());
    b.extend(u16::from(binary_result).to_be_bytes());
    b
}
fn execute(portal: &str, max: u32) -> Vec<u8> {
    let mut b = vec![];
    cstr(portal, &mut b);
    b.extend(max.to_be_bytes());
    b
}
fn has_code(frames: &[(u8, Vec<u8>)], code: &str) -> bool {
    frames
        .iter()
        .any(|(t, b)| *t == b'E' && b.windows(code.len()).any(|w| w == code.as_bytes()))
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
    pg_send(&mut s, b'P', &parse("bad", "COPY app FROM STDIN")).await;
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
    assert!(has_code(&frames, "0A000"));
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
    for i in 0..31 {
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
async fn lost_response_restart_original_operation_and_native_executable() {
    let f = Fixture::new().await;
    let path = f.path.clone();
    let config = f.task.clone();
    let server_config = f.server_config.clone();
    let request = Request::Execute {
        request: repair(700, 0..1),
        approval: None,
    };
    let mut s = raw_tls(&f, f.task.clone(), false).await;
    giggles_server::write_frame(
        &mut s,
        &serde_json::to_vec(&request).unwrap(),
        giggles_server::MAX_FRAME,
    )
    .await
    .unwrap();
    // Read only frame length, deliberately lose the decision payload.
    assert!(s.read_u32().await.unwrap() > 0);
    drop(s);
    let bindings: Vec<Binding> =
        serde_json::from_slice(&std::fs::read(path.join("bindings.json")).unwrap()).unwrap();
    f.stop().await;
    let admin = Admin::open(path.join("db.redb"), Limits::default()).unwrap();
    let service = Service::new(admin.clone(), bindings, Clock::new(0, 3600).unwrap()).unwrap();
    let running = serve("127.0.0.1:0".parse().unwrap(), service, server_config, 8)
        .await
        .unwrap();
    let mut c = Client::connect(running.address, "localhost", config)
        .await
        .unwrap();
    let r = c
        .call(&Request::Status {
            operation: OperationId(700),
        })
        .await
        .unwrap();
    assert_eq!(r.code, "committed");
    assert_eq!(r.count, Some(1));
    assert_eq!(c.call(&request).await.unwrap(), r);
    assert_eq!(admin.snapshot().unwrap().tasks[&TaskId(1)].transitions, 1);
    let request_file = path.join("status.json");
    std::fs::write(
        &request_file,
        serde_json::to_vec(&Request::Status {
            operation: OperationId(700),
        })
        .unwrap(),
    )
    .unwrap();
    let output = tokio::process::Command::new(env!("CARGO_BIN_EXE_giggles-client"))
        .args([
            running.address.to_string(),
            "localhost".into(),
            path.join("task.pem").display().to_string(),
            path.join("task.key.TEST-ONLY").display().to_string(),
            path.join("ca.pem").display().to_string(),
            request_file.display().to_string(),
        ])
        .output()
        .await
        .unwrap();
    std::fs::write(path.join("native-client.stdout.json"), &output.stdout).unwrap();
    std::fs::write(path.join("native-client.stderr.txt"), &output.stderr).unwrap();
    assert!(output.status.success());
    assert_eq!(
        serde_json::from_slice::<Response>(&output.stdout).unwrap(),
        r
    );
    drop(c);
    running.shutdown().await.unwrap();
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
    config["native"] = "127.0.0.1:0".into();
    config["postgres"] = "127.0.0.1:0".into();
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
    let addr = line
        .split_whitespace()
        .next()
        .unwrap()
        .strip_prefix("native=")
        .unwrap()
        .parse()
        .unwrap();
    let cfg = tls::client(
        tls::certs(path.join("app.pem")).unwrap(),
        tls::key(path.join("app.key.TEST-ONLY")).unwrap(),
        tls::certs(path.join("ca.pem")).unwrap(),
    )
    .unwrap();
    let mut c = Client::connect(addr, "localhost", cfg).await.unwrap();
    assert_eq!(c.call(&Request::Ping).await.unwrap().code, "ok");
    drop(c);
    child.start_kill().unwrap();
    let status = child.wait().await.unwrap();
    std::fs::write(path.join("server-killed-status.txt"), format!("{status:?}")).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn seeded_native_and_pinned_driver_histories_match_independent_map() {
    use std::collections::BTreeMap;
    let f = Fixture::new().await;
    let mut native = f.native(f.app.clone()).await;
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
                            .call(&sql("DELETE FROM app WHERE id=$1", vec![Value::Int(id)]))
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
                            .call(&sql(query, vec![Value::Int(id), Value::Text(body)]))
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
async fn native_and_task_wire_deadline_before_dispatch_does_not_mutate() {
    let f = Fixture::new().await;
    let mut c = f.native(f.task.clone()).await;
    let (pg, conn) = f.pg(f.task.clone()).await;
    let request = Request::Deadline {
        budget_ms: 0,
        request: Box::new(Request::Execute {
            request: repair(900, 0..1),
            approval: None,
        }),
    };
    assert_eq!(c.call(&request).await.unwrap().code, "57014");
    assert_eq!(task_wire(&pg, &request).await.code, "57014");
    assert_eq!(
        c.call(&Request::Status {
            operation: OperationId(900)
        })
        .await
        .unwrap()
        .code,
        "absent"
    );
    assert_eq!(f.admin.snapshot().unwrap().tasks[&TaskId(1)].transitions, 0);
    let mut app = f.native(f.app.clone()).await;
    assert_eq!(app.call(&sql("BEGIN", vec![])).await.unwrap().code, "ok");
    assert_eq!(
        app.call(&Request::Deadline {
            budget_ms: 0,
            request: Box::new(sql("INSERT INTO app (id) VALUES (1)", vec![]))
        })
        .await
        .unwrap()
        .code,
        "57014"
    );
    assert_eq!(
        app.call(&sql("COMMIT", vec![])).await.unwrap().code,
        "25P02"
    );
    assert_eq!(app.call(&sql("ROLLBACK", vec![])).await.unwrap().code, "ok");
    assert!(
        app.call(&sql("SELECT * FROM app LIMIT 1", vec![]))
            .await
            .unwrap()
            .sql
            .unwrap()
            .rows
            .is_empty()
    );
    drop(c);
    drop(app);
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
    for tag in [b'P', b'B', b'D', b'E', b'C', b'd', 255] {
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
async fn fail_closed_future_persisted_time_affects_native_and_wire_task_paths() {
    let f = Fixture::new().await;
    let mut c = f.native(f.task.clone()).await;
    let (pg, conn) = f.pg(f.task.clone()).await;
    f.admin.certify_server_time(u64::MAX, u64::MAX).unwrap();
    for request in [
        Request::Preview {
            request: repair(1, 0..1),
        },
        Request::Execute {
            request: repair(1, 0..1),
            approval: None,
        },
        Request::Status {
            operation: OperationId(1),
        },
        Request::RepairSql {
            operation: OperationId(1),
            sql: "UPDATE deliveries SET status = 'queued' WHERE tenant = 42 AND id = 0".into(),
            approval: None,
        },
    ] {
        assert_eq!(c.call(&request).await.unwrap().code, "55000");
        assert_eq!(task_wire(&pg, &request).await.code, "55000");
    }
    assert_eq!(f.admin.snapshot().unwrap().tasks[&TaskId(1)].transitions, 0);
    drop(c);
    drop(pg);
    conn.await.unwrap();
    f.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn expired_task_and_revoked_approval_are_denied_on_both_network_paths() {
    let f = Fixture::new().await;
    let mut expired = grant(4, 42, 25, 5);
    expired.policy.expires = now() - 1;
    f.admin.grant(expired).unwrap();
    let der = tls::certs(f.path.join("task.pem")).unwrap().remove(0);
    let service = Service::new(
        f.admin.clone(),
        vec![Binding {
            fingerprint: tls::fingerprint(&der),
            role: Role::Task {
                domain: f.admin.authority_metadata().unwrap().0.to_string(),
                task: TaskId(4),
                principal: Principal(2),
            },
        }],
        Clock::new(0, 3600).unwrap(),
    )
    .unwrap();
    let native = serve(
        "127.0.0.1:0".parse().unwrap(),
        service.clone(),
        f.server_config.clone(),
        8,
    )
    .await
    .unwrap();
    let pg = serve_pg(
        "127.0.0.1:0".parse().unwrap(),
        service,
        f.server_config.clone(),
        8,
    )
    .await
    .unwrap();
    let mut c = Client::connect(native.address, "localhost", f.task.clone())
        .await
        .unwrap();
    let mut cfg = tokio_postgres::Config::new();
    cfg.host("localhost")
        .hostaddr(pg.address.ip())
        .port(pg.address.port())
        .user("giggles")
        .dbname("giggles")
        .ssl_mode(tokio_postgres::config::SslMode::Require);
    let (wire, conn) = cfg
        .connect(tokio_postgres_rustls::MakeRustlsConnect::new(
            (*f.task).clone(),
        ))
        .await
        .unwrap();
    let handle = tokio::spawn(async move {
        let _ = conn.await;
    });
    for request in [
        Request::Preview {
            request: repair(1, 0..1),
        },
        Request::Execute {
            request: repair(1, 0..1),
            approval: None,
        },
        Request::Status {
            operation: OperationId(1),
        },
        Request::RepairSql {
            operation: OperationId(1),
            sql: "UPDATE deliveries SET status = 'queued' WHERE tenant = 42 AND id = 0".into(),
            approval: None,
        },
    ] {
        assert_eq!(c.call(&request).await.unwrap().code, "42501");
        assert_eq!(task_wire(&wire, &request).await.code, "42501");
    }
    drop(c);
    drop(wire);
    handle.await.unwrap();
    native.shutdown().await.unwrap();
    pg.shutdown().await.unwrap();
    let r = repair(55, 0..6);
    let preview = f
        .admin
        .executor(TaskId(1), Principal(2))
        .unwrap()
        .preview(&r, Time::Certified(now()))
        .unwrap();
    let approval = f
        .admin
        .approve(Principal(3), preview, Time::Certified(now()))
        .unwrap();
    f.admin.revoke_approval(approval).unwrap();
    let mut c = f.native(f.task.clone()).await;
    let (wire, conn) = f.pg(f.task.clone()).await;
    let request = Request::Execute {
        request: r,
        approval: Some(approval),
    };
    assert_eq!(c.call(&request).await.unwrap().code, "GD001");
    assert_eq!(task_wire(&wire, &request).await.code, "GD001");
    drop(c);
    drop(wire);
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
