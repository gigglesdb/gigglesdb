//! Opt-in, real PostgreSQL information-flow comparison through the gateway.
use nunya::{
    dialect::{Lowering, pg::PgLowering},
    error::Refusal,
    ir::{OutputRewrite, check::check},
    pgwire::{
        auth::ScramVerifier,
        context::{GatewayContext, Metrics},
        session::serve_tcp,
        upstream::{UpstreamConfig, UpstreamTls},
    },
    readlog::{ReadLog, ReadLogRecord},
    task::TaskStore,
    token::{StandardCodec, keys::RootKey},
};
use std::{
    collections::{BTreeMap, HashMap},
    path::PathBuf,
    sync::{Arc, Mutex, RwLock},
};
use tokio_postgres::{Client, Config, NoTls, SimpleQueryMessage};

type TestResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
type Rows = Vec<Vec<Option<String>>>;

struct Audit;
impl ReadLog for Audit {
    fn write(&self, _: &ReadLogRecord) -> Result<(), Refusal> {
        Ok(())
    }
}

struct Gateway {
    client: Client,
    server: tokio::task::JoinHandle<()>,
    connection: tokio::task::JoinHandle<()>,
    dir: PathBuf,
}
impl Drop for Gateway {
    fn drop(&mut self) {
        self.connection.abort();
        self.server.abort();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

async fn gateway(url: &str, side: &str) -> TestResult<Gateway> {
    let config: Config = url.parse().map_err(|_| "invalid NI database URL")?;
    let host = match config.get_hosts() {
        [tokio_postgres::config::Host::Tcp(host)] => host.clone(),
        _ => return Err("NI requires one TCP upstream host".into()),
    };
    let (snapshot, policy) = crate::common::harness_fixture();
    let dir = std::env::temp_dir().join(format!(
        "noninterference-{}-{side}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos()
    ));
    let context = Arc::new(GatewayContext {
        policy: RwLock::new(Arc::new(policy)),
        snapshot: RwLock::new(Arc::new(snapshot)),
        // serve_tcp chooses a fresh random scope salt for standing credentials.
        // Its public API cannot inject the requested fixed salt. Paired token
        // pools below preserve each live connection's own scope instead.
        root_key: RootKey::from_hex(&"42".repeat(32))?,
        tasks: TaskStore::with_clock(&dir, Arc::new(|| 0))?,
        readlog: Box::new(Audit),
        upstream: UpstreamConfig {
            host,
            port: config.get_ports().first().copied().unwrap_or(5432),
            user: config.get_user().ok_or("NI upstream user required")?.into(),
            password: zeroize::Zeroizing::new(
                String::from_utf8(config.get_password().unwrap_or_default().to_vec())
                    .map_err(|_| "NI upstream password must be UTF-8")?,
            ),
            dbname: config
                .get_dbname()
                .ok_or("NI upstream database required")?
                .into(),
            tls: UpstreamTls::Disable,
            application_name: "nunya-ni".into(),
            connect_timeout_ms: 10_000,
        },
        search_path: vec!["public".into()],
        standing: BTreeMap::from([(
            "ni-agent".into(),
            ScramVerifier::from_password("ni-test-only", &[7; 16], 4096),
        )]),
        lowering: PgLowering,
        codec: StandardCodec,
        clock: Arc::new(|| 0),
        cancel_map: Mutex::new(HashMap::new()),
        metrics: Metrics::default(),
        tls: None,
        allow_plaintext: true,
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(async move {
        if let Ok((stream, _)) = listener.accept().await {
            serve_tcp(stream, context).await;
        }
    });
    let mut agent = Config::new();
    agent
        .host("127.0.0.1")
        .port(address.port())
        .user("ni-agent")
        .password("ni-test-only")
        .dbname("app");
    let (client, connection) = agent
        .connect(NoTls)
        .await
        .map_err(|_| "NI agent connection failed")?;
    let connection = tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(Gateway {
        client,
        server,
        connection,
        dir,
    })
}

async fn query(client: &Client, sql: &str) -> TestResult<Rows> {
    let messages = client
        .simple_query(sql)
        .await
        .map_err(|_| "NI query failed")?;
    Ok(messages
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

#[derive(Default)]
struct Canonical {
    seed: u64,
    domains: BTreeMap<String, HashMap<String, usize>>,
}
impl Canonical {
    fn rows(&mut self, rows: &Rows, outputs: &[OutputRewrite]) -> Rows {
        let seed = self.seed;
        rows.iter()
            .map(|row| {
                assert!(
                    row.len() == outputs.len(),
                    "NI output shape mismatch: seed={seed}"
                );
                row.iter()
                    .enumerate()
                    .map(|(i, value)| match (value, outputs.get(i)) {
                        (Some(token), Some(OutputRewrite::Tokenize { domain, .. })) => {
                            let body = token
                                .strip_prefix(&format!("tok_{}_", domain.0))
                                .unwrap_or_else(|| {
                                    panic!("NI blind output was not a domain token: seed={seed}")
                                });
                            assert!(
                                body.bytes()
                                    .all(|b| b.is_ascii_lowercase() || (b'2'..=b'7').contains(&b)),
                                "NI token encoding was invalid: seed={seed}"
                            );
                            let decoded = data_encoding::BASE32_NOPAD
                                .decode(body.to_ascii_uppercase().as_bytes())
                                .unwrap_or_else(|_| {
                                    panic!("NI token encoding was invalid: seed={seed}")
                                });
                            assert!(
                                [32, 48, 80, 144, 272].contains(&decoded.len()),
                                "NI token size was invalid: seed={seed}"
                            );
                            let seen = self.domains.entry(domain.0.clone()).or_default();
                            let next = seen.len();
                            let index = seen.entry(token.clone()).or_insert(next);
                            Some(format!("token:{}:{index}", domain.0))
                        }
                        _ => value.clone(),
                    })
                    .collect()
            })
            .collect()
    }
}

struct Generator(u64);
impl Generator {
    fn pick(&mut self, n: u64) -> u64 {
        // SplitMix64 also handles a seed of zero without degenerating.
        self.0 = self.0.wrapping_add(0x9e3779b97f4a7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
        (z ^ (z >> 31)) % n
    }
    fn statement(&mut self) -> (String, bool) {
        let limit = self.pick(6) + 1;
        let threshold = self.pick(5);
        let projection = [
            "id",
            "email",
            "country",
            "phone",
            "failed_payments",
            "id,email,country,phone,failed_payments",
        ][self.pick(6) as usize];
        match self.pick(10) {
            0 => (
                format!(
                    "SELECT {projection} FROM customers WHERE failed_payments >= {threshold} ORDER BY id LIMIT {limit}"
                ),
                false,
            ),
            1 => (
                format!(
                    "SELECT {projection} FROM customers WHERE id <= {limit} ORDER BY id DESC LIMIT {limit}"
                ),
                false,
            ),
            2 => (
                format!(
                    "SELECT {projection} FROM customers WHERE email = $1 ORDER BY id LIMIT {limit}"
                ),
                true,
            ),
            3 => (
                format!(
                    "SELECT {projection} FROM customers WHERE email IS NULL ORDER BY id LIMIT {limit}"
                ),
                false,
            ),
            4 => (
                format!(
                    "SELECT {projection} FROM customers WHERE email LIKE 'a%' ORDER BY id LIMIT {limit}"
                ),
                false,
            ),
            5 => (
                format!(
                    "SELECT c.id,c.email,o.to_address FROM customers c JOIN outbox o ON c.email = o.to_address ORDER BY c.id,o.id LIMIT {limit}"
                ),
                false,
            ),
            6 => (
                format!(
                    "SELECT email,count(*) FROM customers GROUP BY email ORDER BY min(id) LIMIT {limit}"
                ),
                false,
            ),
            7 => (
                format!(
                    "SELECT country,count(email) FROM customers GROUP BY country ORDER BY country LIMIT {limit}"
                ),
                false,
            ),
            8 => {
                let projection = ["id", "to_address", "template", "id,to_address,template"]
                    [self.pick(4) as usize];
                let predicate = match self.pick(3) {
                    0 => format!("id >= {threshold}"),
                    1 => "to_address IS NULL".into(),
                    _ => "template = 'reminder'".into(),
                };
                (
                    format!(
                        "SELECT {projection} FROM outbox WHERE {predicate} ORDER BY id LIMIT {limit}"
                    ),
                    false,
                )
            }
            _ => {
                let projection = ["id", "body", "id,body"][self.pick(3) as usize];
                let predicate = match self.pick(3) {
                    0 => format!("id <= {limit}"),
                    1 => "body IS NULL".into(),
                    _ => "body = 'public note'".into(),
                };
                (
                    format!(
                        "SELECT {projection} FROM notes WHERE {predicate} ORDER BY id LIMIT {limit}"
                    ),
                    false,
                )
            }
        }
    }
}

#[tokio::test]
#[ignore = "requires two independently seeded PostgreSQL databases; never run in the normal suite"]
async fn gateway_preserves_noninterference() -> TestResult<()> {
    let (Ok(a_url), Ok(b_url)) = (
        std::env::var("NUNYA_TEST_PG_A"),
        std::env::var("NUNYA_TEST_PG_B"),
    ) else {
        eprintln!("NI skipped: set both NUNYA_TEST_PG_A and NUNYA_TEST_PG_B");
        return Ok(());
    };
    let seed = std::env::var("NUNYA_NI_SEED")
        .unwrap_or_else(|_| "1".into())
        .parse::<u64>()?;
    let cases = std::env::var("NUNYA_NI_CASES")
        .unwrap_or_else(|_| "500".into())
        .parse::<usize>()?;
    let a = gateway(&a_url, "a").await?;
    let b = gateway(&b_url, "b").await?;
    let (snapshot, policy) = crate::common::harness_fixture();
    let mut canonical_a = Canonical {
        seed,
        ..Canonical::default()
    };
    let mut canonical_b = Canonical {
        seed,
        ..Canonical::default()
    };
    let bootstrap = "SELECT id,email FROM customers ORDER BY id";
    let statements = PgLowering.lower(bootstrap, &snapshot, &policy, &[])?;
    let plan = check(&statements[0], &policy, &snapshot)?;
    let tokens_a = query(&a.client, bootstrap).await?;
    let tokens_b = query(&b.client, bootstrap).await?;
    assert!(
        tokens_a.len() == tokens_b.len(),
        "NI bootstrap row count differs: seed={seed}"
    );
    if canonical_a.rows(&tokens_a, &plan.outputs) != canonical_b.rows(&tokens_b, &plan.outputs) {
        panic!("NI bootstrap differs: seed={seed}");
    }
    let pairs: Vec<_> = tokens_a
        .iter()
        .zip(&tokens_b)
        .filter_map(|(a, b)| Some((a.get(1)?.as_ref()?.clone(), b.get(1)?.as_ref()?.clone())))
        .collect();
    if pairs.is_empty() {
        return Err("NI fixtures need at least one non-null email".into());
    }
    let mut generator = Generator(seed);
    let mut refused = 0;
    for case in 0..cases {
        let (sql, parameter) = generator.statement();
        let plans = PgLowering
            .lower(&sql, &snapshot, &policy, &[])
            .and_then(|statements| {
                statements
                    .iter()
                    .map(|s| check(s, &policy, &snapshot))
                    .collect::<Result<Vec<_>, _>>()
            });
        let Ok(plans) = plans else {
            refused += 1;
            continue;
        };
        let (sql_a, sql_b) = if parameter {
            let pair = &pairs[generator.pick(pairs.len() as u64) as usize];
            (
                sql.replace("$1", &format!("'{}'", pair.0.replace('\'', "''"))),
                sql.replace("$1", &format!("'{}'", pair.1.replace('\'', "''"))),
            )
        } else {
            (sql.clone(), sql)
        };
        // Check the exact literal-bearing statements as well as the generated
        // parameter form before either connection executes them.
        for executable in [&sql_a, &sql_b] {
            let statements = PgLowering.lower(executable, &snapshot, &policy, &[])?;
            for statement in statements {
                check(&statement, &policy, &snapshot)?;
            }
        }
        let rows_a = query(&a.client, &sql_a).await;
        let rows_b = query(&b.client, &sql_b).await;
        let (Ok(rows_a), Ok(rows_b)) = (rows_a, rows_b) else {
            panic!("NI allowed query failed: seed={seed} case={case}");
        };
        if canonical_a.rows(&rows_a, &plans[0].outputs)
            != canonical_b.rows(&rows_b, &plans[0].outputs)
        {
            panic!("NI result differs: seed={seed} case={case}");
        }
    }
    let mut leaks = Vec::new();
    for url in [&a_url, &b_url] {
        let (client, connection) = tokio_postgres::connect(url, NoTls)
            .await
            .map_err(|_| "NI direct connection failed")?;
        let connection = tokio::spawn(async move {
            let _ = connection.await;
        });
        let rows = query(&client, "SELECT length(email) FROM customers ORDER BY id").await?;
        leaks.push(
            Canonical {
                seed,
                ..Canonical::default()
            }
            .rows(&rows, &[OutputRewrite::Pass]),
        );
        drop(client);
        connection.await?;
    }
    assert!(
        leaks[0] != leaks[1],
        "NI deliberate length leak did not differ: seed={seed}"
    );
    eprintln!(
        "NI seed={seed} cases={cases} allowed={} refused={refused}",
        cases - refused
    );
    Ok(())
}
