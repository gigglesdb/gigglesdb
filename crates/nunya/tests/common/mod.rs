//! Harness fixtures shared by offline and opt-in network tests.
use nunya::{
    policy::{DomainName, DomainSettings, Policy, ReadLogSink, TokenizeSetting, Treatment},
    schema::{Column, ColumnRef, Relation, RelationKind, Snapshot},
    value::ColumnType,
};
use std::collections::BTreeMap;

/// Mirror harness/seed.sql and harness/policy.toml without a database connection.
pub fn harness_fixture() -> (Snapshot, Policy) {
    let mut relations = Vec::new();
    for (table, names) in [
        (
            "customers",
            vec!["id", "email", "country", "phone", "failed_payments"],
        ),
        ("outbox", vec!["id", "to_address", "template"]),
        ("notes", vec!["id", "body"]),
    ] {
        relations.push(Relation {
            schema: "public".into(),
            name: table.into(),
            kind: RelationKind::Table,
            columns: names
                .into_iter()
                .map(|name| Column {
                    name: name.into(),
                    ty: if matches!(name, "id" | "failed_payments") {
                        ColumnType::Int4
                    } else {
                        ColumnType::Text
                    },
                    nullable: name != "id",
                    generated: None,
                    default: if table == "outbox" && name == "id" {
                        Some("nextval('outbox_id_seq'::regclass)".into())
                    } else {
                        None
                    },
                })
                .collect(),
            view_definition: None,
            primary_key: vec!["id".into()],
        });
    }
    relations.push(Relation {
        schema: "public".into(),
        name: "v_emails".into(),
        kind: RelationKind::View,
        columns: relations[0].columns[..2].to_vec(),
        view_definition: Some("SELECT id, email FROM public.customers".into()),
        primary_key: vec![],
    });
    let column = |table: &str, name: &str| ColumnRef {
        schema: "public".into(),
        table: table.into(),
        column: name.into(),
    };
    let domain = DomainName("email".into());
    let policy = Policy {
        columns: BTreeMap::from([
            (
                column("customers", "email"),
                Treatment::Blind {
                    domain: domain.clone(),
                },
            ),
            (
                column("outbox", "to_address"),
                Treatment::Blind {
                    domain: domain.clone(),
                },
            ),
            (
                column("customers", "phone"),
                Treatment::Masked {
                    placeholder: Some("[hidden]".into()),
                },
            ),
        ]),
        domains: BTreeMap::from([(
            domain,
            DomainSettings {
                tokenize: TokenizeSetting::On {
                    limit_per_session: 20,
                    limit_per_principal: 200,
                    principal_window: std::time::Duration::from_secs(86400),
                },
                range: None,
            },
        )]),
        snapshot_max_age_ms: 60_000,
        readlog: ReadLogSink::Stdout,
    };
    let snapshot = Snapshot::new(relations, vec![], vec!["public".into()], 0);
    let doc = nunya::policy::PolicyDocument {
        policy,
        version: nunya::policy::PolicyVersion::Legacy,
        route_default: nunya::policy::RouteDefault::Show,
        json_paths: vec![],
        links: vec![],
        file_readlog: None,
        file_snapshot_max_age_ms: None,
    };
    let policy = nunya::policy::effective::served_policy(
        &doc,
        &snapshot,
        &nunya::policy::bindings::RouteBindings::default(),
    );
    (snapshot, policy)
}

/// Fallible operations in the process-based serve fixture.
pub type ServeResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Derive the file-backed read log policy used by live harness tests.
pub fn file_sink_policy() -> String {
    include_str!("../../harness/policy.toml")
        .replace("sink = \"stdout\"", "sink = \"file\"\npath = \"read.log\"")
}

/// Optional native Spanner upstream for a serve fixture.
pub struct NativeUpstream<'a> {
    /// Fully qualified database resource name.
    pub database: &'a str,
    /// Emulator endpoint for this test.
    pub emulator_endpoint: &'a str,
}

/// Real serve process with reusable configuration, ports and state directory.
#[allow(dead_code)] // Restart/native helpers are shared with subsequent live tests.
pub struct ServeHarness {
    dir: std::path::PathBuf,
    state_dir: std::path::PathBuf,
    pg_port: u16,
    spanner_addr: std::net::SocketAddr,
    management: std::net::SocketAddr,
    admin: nunya::mgmt::client::MgmtClient,
    child: Option<std::process::Child>,
    second_pids: Vec<u32>,
    starts: usize,
    reservations: Vec<std::net::TcpListener>,
}

#[allow(dead_code)]
impl ServeHarness {
    /// Write a fresh fixture; start it explicitly once configuration is ready.
    pub fn new(
        policy: &str,
        pg_url: Option<&str>,
        dialect: Option<&str>,
        native: Option<NativeUpstream<'_>>,
        extra_toml: &str,
    ) -> ServeResult<Self> {
        use nunya::mgmt::client::SessionSecret;
        let dir = std::env::temp_dir().join(format!(
            "serve-harness-{}",
            nunya::token::SessionId::random().to_hex()
        ));
        std::fs::create_dir_all(&dir)?;
        let result = (|| {
            std::fs::write(dir.join("root.key"), "ab".repeat(32))?;
            std::fs::write(dir.join("policy.toml"), policy)?;
            let reservations: Vec<_> = (0..4)
                .map(|_| std::net::TcpListener::bind("127.0.0.1:0"))
                .collect::<Result<_, _>>()?;
            let pg_port = reservations[0].local_addr()?.port();
            let admin_addr = reservations[1].local_addr()?;
            let management = reservations[2].local_addr()?;
            let spanner_addr = reservations[3].local_addr()?;
            let credential = SessionSecret::generate();
            let mut text = format!(
                "listen = '127.0.0.1:{pg_port}'\nadmin_listen = '{admin_addr}'\npolicy = 'policy.toml'\nstate_dir = 'state'\nroot_key_file = 'root.key'\nallow_plaintext = true\n[management]\nlisten = '{management}'\n[[management.admin]]\nname = 'bootstrap'\nverifier = '{}'\n",
                credential.verifier
            );
            if let Some(url) = pg_url {
                let pg: tokio_postgres::Config =
                    url.parse().map_err(|_| "invalid test upstream URL")?;
                let host = match pg.get_hosts().first() {
                    Some(tokio_postgres::config::Host::Tcp(host)) => host,
                    _ => return Err("TCP upstream required".into()),
                };
                std::fs::write(dir.join("password"), pg.get_password().unwrap_or_default())?;
                let mut upstream = toml::map::Map::new();
                for (key, value) in [
                    ("host", host.as_str()),
                    ("user", pg.get_user().unwrap_or("postgres")),
                    ("dbname", pg.get_dbname().unwrap_or("app")),
                    ("password_file", "password"),
                    ("tls", "disable"),
                ] {
                    upstream.insert(key.into(), toml::Value::String(value.into()));
                }
                if let Some(dialect) = dialect {
                    upstream.insert("dialect".into(), toml::Value::String(dialect.into()));
                }
                upstream.insert(
                    "port".into(),
                    toml::Value::Integer(i64::from(*pg.get_ports().first().unwrap_or(&5432))),
                );
                text.push_str(&format!("[upstream]\n{}", toml::to_string(&upstream)?));
            }
            if let Some(native) = native {
                let section = toml::Table::from_iter([
                    (
                        "listen".into(),
                        toml::Value::String(spanner_addr.to_string()),
                    ),
                    (
                        "database".into(),
                        toml::Value::String(native.database.into()),
                    ),
                    (
                        "emulator_endpoint".into(),
                        toml::Value::String(native.emulator_endpoint.into()),
                    ),
                ]);
                text.push_str(&format!("[spanner_native]\n{}", toml::to_string(&section)?));
            }
            text.push_str(extra_toml);
            std::fs::write(dir.join("nunya.toml"), text)?;
            let admin = management_client(management, "bootstrap", &credential.secret)?;
            Ok(Self {
                state_dir: dir.join("state"),
                dir: dir.clone(),
                pg_port,
                spanner_addr,
                management,
                admin,
                child: None,
                second_pids: vec![],
                starts: 0,
                reservations,
            })
        })();
        if result.is_err() {
            let _ = std::fs::remove_dir_all(&dir);
        }
        result
    }

    /// Repoint a stopped fixture without changing its deployment, root key or sessions.
    pub fn set_upstream(
        &mut self,
        url: &str,
        tls: &str,
        ca_file: Option<&str>,
        host: Option<&str>,
    ) -> ServeResult<()> {
        if self.child.is_some() {
            return Err("stop serve before changing its upstream".into());
        }
        let pg: tokio_postgres::Config = url.parse().map_err(|_| "invalid test upstream URL")?;
        let url_host = match pg.get_hosts().first() {
            Some(tokio_postgres::config::Host::Tcp(host)) => host.as_str(),
            _ => return Err("TCP upstream required".into()),
        };
        let path = self.dir.join("nunya.toml");
        let mut config: toml::Value = toml::from_str(&std::fs::read_to_string(&path)?)?;
        let upstream = config
            .get_mut("upstream")
            .and_then(toml::Value::as_table_mut)
            .ok_or("fixture has no upstream")?;
        for (key, value) in [
            ("host", host.unwrap_or(url_host)),
            ("tls", tls),
            ("user", pg.get_user().unwrap_or("postgres")),
            ("dbname", pg.get_dbname().unwrap_or("app")),
        ] {
            upstream.insert(key.into(), toml::Value::String(value.into()));
        }
        upstream.insert(
            "port".into(),
            toml::Value::Integer(i64::from(*pg.get_ports().first().unwrap_or(&5432))),
        );
        upstream.insert(
            "application_name".into(),
            toml::Value::String(
                self.dir
                    .file_name()
                    .ok_or("missing fixture name")?
                    .to_string_lossy()
                    .into_owned(),
            ),
        );
        upstream.remove("ca_file");
        if let Some(ca) = ca_file {
            upstream.insert("ca_file".into(), toml::Value::String(ca.into()));
        }
        std::fs::write(
            self.dir.join("password"),
            pg.get_password().unwrap_or_default(),
        )?;
        std::fs::write(path, toml::to_string(&config)?)?;
        Ok(())
    }

    fn spawn(&mut self) -> ServeResult<std::process::Child> {
        self.reservations.clear();
        self.starts += 1;
        Ok(std::process::Command::new(env!("CARGO_BIN_EXE_nunya"))
            .arg("serve")
            .arg("--config")
            .arg(self.dir.join("nunya.toml"))
            .stdin(std::process::Stdio::null())
            .stdout(std::fs::File::create(
                self.dir.join(format!("stdout-{}.log", self.starts)),
            )?)
            .stderr(std::fs::File::create(
                self.dir.join(format!("stderr-{}.log", self.starts)),
            )?)
            .spawn()?)
    }

    fn startup_error(&self, reason: &str) -> Box<dyn std::error::Error + Send + Sync> {
        let stderr =
            std::fs::read(self.dir.join(format!("stderr-{}.log", self.starts))).unwrap_or_default();
        let tail = String::from_utf8_lossy(&stderr[stderr.len().saturating_sub(4096)..]);
        format!("{reason}; serve stderr tail: {tail}").into()
    }

    /// Spawn the binary and wait for management readiness, checking early exit.
    pub async fn start(&mut self) -> ServeResult<()> {
        if self.child.is_some() {
            return Err("serve already started".into());
        }
        self.child = Some(self.spawn()?);
        for _ in 0..200 {
            if let Some(child) = &mut self.child
                && child.try_wait()?.is_some()
            {
                self.child = None;
                return Err(self.startup_error("serve exited during startup"));
            }
            if self.admin.status().await.is_ok() {
                return Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        let error = self.startup_error("serve management readiness timed out");
        self.stop()?;
        Err(error)
    }

    /// Observe an early serve exit without killing an otherwise running process.
    pub fn has_exited(&mut self) -> ServeResult<bool> {
        match &mut self.child {
            Some(child) => Ok(child.try_wait()?.is_some()),
            None => Ok(true),
        }
    }

    /// Kill and reap the primary child, keeping configuration and state intact.
    pub fn stop(&mut self) -> ServeResult<()> {
        if let Some(mut child) = self.child.take() {
            if child.try_wait()?.is_none() {
                child.kill()?;
            }
            child.wait()?;
        }
        Ok(())
    }

    /// Restart the binary on the same ports and state directory.
    pub async fn restart(&mut self) -> ServeResult<()> {
        self.stop()?;
        self.start().await
    }

    /// Spawn a competing process on the same config without waiting for readiness.
    pub fn spawn_second(&mut self) -> ServeResult<std::process::Child> {
        let child = self.spawn()?;
        self.second_pids.push(child.id());
        Ok(child)
    }

    /// Bootstrap management client.
    pub fn admin(&self) -> &nunya::mgmt::client::MgmtClient {
        &self.admin
    }
    /// PostgreSQL listener port.
    pub fn pg_port(&self) -> u16 {
        self.pg_port
    }
    /// Native Spanner listener address.
    pub fn spanner_addr(&self) -> std::net::SocketAddr {
        self.spanner_addr
    }
    /// Fixture configuration and log directory.
    pub fn dir(&self) -> &std::path::Path {
        &self.dir
    }
    /// Persistent state directory reused across starts.
    pub fn state_dir(&self) -> &std::path::Path {
        &self.state_dir
    }

    /// Create a principal, grant the requested route and mode, and acquire its session.
    pub async fn acquire_session(
        &self,
        principal_name: &str,
        route: &str,
        mode: nunya::state::model::Mode,
    ) -> ServeResult<(
        nunya::mgmt::client::SessionView,
        nunya::mgmt::client::SessionSecret,
        nunya::mgmt::client::SessionSecret,
    )> {
        use nunya::{
            mgmt::client::{AcquireParams, SessionSecret},
            state::model::PrincipalKind,
        };
        let principal_secret = SessionSecret::generate();
        let principal = self
            .admin
            .create_principal(
                principal_name,
                PrincipalKind::Agent,
                false,
                Some(&principal_secret.verifier),
            )
            .await?;
        self.admin
            .set_grants(&principal.id, &[(route, mode)])
            .await?;
        let owner = management_client(self.management, principal_name, &principal_secret.secret)?;
        let secret = SessionSecret::generate();
        let session = owner
            .acquire(AcquireParams {
                route: route.into(),
                mode: Some(mode),
                lifetime: None,
                isolated: false,
                label: None,
                verifier: secret.verifier.clone(),
                secret_sha256: secret.sha256_hex.clone(),
            })
            .await?;
        Ok((session, secret, principal_secret))
    }

    /// Connect with a session credential and return the client and running driver.
    pub async fn pg_connect(
        &self,
        session: &nunya::mgmt::client::SessionView,
        secret: &nunya::mgmt::client::SessionSecret,
        dbname: &str,
    ) -> ServeResult<(tokio_postgres::Client, tokio::task::JoinHandle<()>)> {
        let mut config = tokio_postgres::Config::new();
        config
            .host("127.0.0.1")
            .port(self.pg_port)
            .dbname(dbname)
            .user(format!("session:{}", session.id))
            .password(secret.secret.as_bytes());
        for _ in 0..100 {
            if let Ok((client, connection)) = config.connect(tokio_postgres::NoTls).await {
                return Ok((
                    client,
                    tokio::spawn(async move {
                        let _ = connection.await;
                    }),
                ));
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        Err("session PostgreSQL connection did not become ready".into())
    }
}

fn management_client(
    addr: std::net::SocketAddr,
    name: &str,
    secret: &str,
) -> ServeResult<nunya::mgmt::client::MgmtClient> {
    use nunya::mgmt::client::{Credentials, MgmtClient};
    Ok(MgmtClient::new(
        format!("http://{addr}").parse()?,
        None,
        Credentials::Bearer {
            name: name.into(),
            secret: zeroize::Zeroizing::new(secret.into()),
        },
    )?)
}

impl Drop for ServeHarness {
    fn drop(&mut self) {
        let _ = self.stop();
        for pid in &self.second_pids {
            // The caller owns each returned Child and should reap it after inspecting exit.
            #[cfg(windows)]
            let _ = std::process::Command::new("taskkill")
                .args(["/F", "/T", "/PID", &pid.to_string()])
                .output();
            #[cfg(unix)]
            let _ = std::process::Command::new("kill")
                .args(["-KILL", &pid.to_string()])
                .output();
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}
