use crate as giggles_server;
use giggles_core::{Admin, Limits, roles::NewRole};
use giggles_server::{
    Running, Service,
    auth::{PostgresqlConfig, TlsMode, serve_postgresql},
    clock::Clock,
};
use std::{
    collections::BTreeMap,
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
// Both scenarios use the same pgwire test counters and cache-disable switch.
static SCENARIO: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct ResetReuse;
impl Drop for ResetReuse {
    fn drop(&mut self) {
        super::UNNAMED_REUSE_DISABLED.store(false, Ordering::SeqCst);
    }
}

struct Fixture {
    service: Arc<Service>,
    pg: Running,
    path: std::path::PathBuf,
}
impl Fixture {
    async fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "sql-clock-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            SERIAL.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).unwrap();
        let admin = Admin::create_engine(path.join("db"), Limits::default(), giggles_core::Engine::V2).unwrap();
        admin
            .roles()
            .create(NewRole {
                name: "owner".into(),
                superuser: true,
                login: true,
                ..NewRole::default()
            })
            .unwrap();
        let service = Service::new(admin, vec![], Clock::new(0, 3600).unwrap()).unwrap();
        let config = PostgresqlConfig {
            listen: "127.0.0.1:0".parse().unwrap(),
            tls: TlsMode::Disabled,
            certificate: None,
            private_key: None,
            client_ca: None,
            certificates: BTreeMap::new(),
            databases: vec!["postgres".into()],
            loopback_trust: true,
            connections: 8,
        };
        let listener = Arc::new(config.build().unwrap());
        let pg = serve_postgresql(config.listen, service.clone(), listener)
            .await
            .unwrap();
        Self { service, pg, path }
    }
    async fn stop(self) {
        self.pg.shutdown().await.unwrap();
        drop(self.service);
        // Listener shutdown waits for active connections to release the file.
        let _ = std::fs::remove_dir_all(self.path);
    }
}
async fn wire_cycle(stream: &mut TcpStream) -> Vec<(u8, Vec<u8>)> {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        let mut messages = Vec::new();
        loop {
            let tag = stream.read_u8().await.unwrap();
            let length = stream.read_u32().await.unwrap();
            assert!((4..65536).contains(&length));
            let mut body = vec![0; length as usize - 4];
            stream.read_exact(&mut body).await.unwrap();
            messages.push((tag, body));
            if tag == b'Z' {
                return messages;
            }
        }
    })
    .await
    .unwrap()
}
async fn wire_query(stream: &mut TcpStream, sql: &str) -> Vec<(u8, Vec<u8>)> {
    let mut body = Vec::new();
    giggles_server::pgwire::z(sql, &mut body);
    let mut packet = Vec::new();
    giggles_server::pgwire::frame(b'Q', &body, &mut packet).unwrap();
    stream.write_all(&packet).await.unwrap();
    wire_cycle(stream).await
}


async fn connect(f: &Fixture) -> TcpStream {
    let mut stream = TcpStream::connect(f.pg.address).await.unwrap();
    let body = b"\0\x03\0\0user\0owner\0database\0postgres\0\0";
    stream.write_u32((body.len() + 4) as u32).await.unwrap();
    stream.write_all(body).await.unwrap();
    wire_cycle(&mut stream).await;
    stream
}
async fn extended(stream: &mut TcpStream, sql: &str, named: bool) -> Vec<(u8, Vec<u8>)> {
    extended_oids(stream, sql, named, &[]).await
}
async fn extended_oids(stream: &mut TcpStream, sql: &str, named: bool, oids: &[u32]) -> Vec<(u8, Vec<u8>)> {
    let name = if named { "control" } else { "" };
    let params = vec!["9"; oids.len()];
    extended_values(stream, sql, name, oids, &params, false).await
}
async fn extended_values(
    stream: &mut TcpStream,
    sql: &str,
    name: &str,
    oids: &[u32],
    params: &[&str],
    describe_statement: bool,
) -> Outcome {
    let mut parse = vec![];
    giggles_server::pgwire::z(name, &mut parse);
    giggles_server::pgwire::z(sql, &mut parse);
    parse.extend((oids.len() as u16).to_be_bytes());
    for oid in oids { parse.extend(oid.to_be_bytes()); }
    let mut bind = vec![0];
    giggles_server::pgwire::z(name, &mut bind);
    bind.extend(0u16.to_be_bytes());
    bind.extend((params.len() as u16).to_be_bytes());
    for param in params {
        bind.extend((param.len() as u32).to_be_bytes());
        bind.extend_from_slice(param.as_bytes());
    }
    bind.extend(0u16.to_be_bytes());
    let mut packet = Vec::new();
    giggles_server::pgwire::frame(b'P', &parse, &mut packet).unwrap();
    if describe_statement {
        giggles_server::pgwire::frame(b'D', b"S\0", &mut packet).unwrap();
    }
    for (tag, body) in [(b'B', bind.as_slice()), (b'D', b"P\0"),
        (b'E', &[0; 5]), (b'S', &[])] {
        giggles_server::pgwire::frame(tag, body, &mut packet).unwrap();
    }
    stream.write_all(&packet).await.unwrap();
    let result = wire_cycle(stream).await;
    if !name.is_empty() {
        let mut packet = vec![];
        giggles_server::pgwire::frame(b'C', b"Scontrol\0", &mut packet).unwrap();
        giggles_server::pgwire::frame(b'S', &[], &mut packet).unwrap();
        stream.write_all(&packet).await.unwrap();
        wire_cycle(stream).await;
    }
    result
}
fn error_field(messages: &[(u8, Vec<u8>)], field: u8) -> Option<&[u8]> {
    messages.iter().filter(|(tag, _)| *tag == b'E')
        .flat_map(|(_, body)| body.split(|b| *b == 0))
        .find(|value| value.first() == Some(&field))
        .map(|value| &value[1..])
}
fn code(messages: &[(u8, Vec<u8>)]) -> Option<String> {
    messages.iter().find(|(tag, _)| *tag == b'E').and_then(|(_, b)| {
        b.split(|x| *x == 0).find(|s| s.first() == Some(&b'C'))
            .map(|s| String::from_utf8(s[1..].to_vec()).unwrap())
    })
}
async fn command(stream: &mut TcpStream, sql: &str) {
    let result = extended(stream, sql, true).await;
    assert_eq!(code(&result), None, "{sql}: {result:?}");
}
// Preserve complete ErrorResponse and NoticeResponse fields and message boundaries.
// This scenario emits no nondeterministic wire values.
type Outcome = Vec<(u8, Vec<u8>)>;
fn outcome(messages: Outcome) -> Outcome { messages }
async fn checked(stream: &mut TcpStream, sql: &str, hit: bool) -> Outcome {
    check_reuse(sql, hit, extended(stream, sql, false)).await
}
async fn check_reuse(sql: &str, hit: bool, response: impl Future<Output = Outcome>) -> Outcome {
    let hits = super::UNNAMED_REUSE_HITS.load(Ordering::SeqCst);
    let misses = super::UNNAMED_REUSE_MISSES.load(Ordering::SeqCst);
    let result = response.await;
    let hit = hit && !super::UNNAMED_REUSE_DISABLED.load(Ordering::SeqCst);
    assert_eq!(super::UNNAMED_REUSE_HITS.load(Ordering::SeqCst) - hits, usize::from(hit), "hit: {sql}");
    assert_eq!(super::UNNAMED_REUSE_MISSES.load(Ordering::SeqCst) - misses, usize::from(!hit), "miss: {sql}");
    result
}
async fn scenario(disabled: bool) -> Vec<Outcome> {
    super::UNNAMED_REUSE_DISABLED.store(disabled, Ordering::SeqCst);
    let f = Fixture::new().await;
    let mut a = connect(&f).await;
    let mut b = connect(&f).await;
    for setup in ["CREATE TABLE r3_parse_source(id INT)", "INSERT INTO r3_parse_source VALUES(7)", "CREATE ROLE r", "GRANT SELECT ON r3_parse_source TO r"] {
        assert_eq!(code(&wire_query(&mut b, setup).await), None, "{setup}");
    }
    let sql = "SELECT * FROM r3_parse_source";
    let mut results = vec![];
    results.push(outcome(checked(&mut a, sql, false).await));
    let before = super::UNNAMED_REUSE_HITS.load(Ordering::SeqCst);
    results.push(outcome(checked(&mut a, sql, true).await));
    let hits = super::UNNAMED_REUSE_HITS.load(Ordering::SeqCst);
    assert_eq!(hits - before, usize::from(!disabled), "second identical unnamed Parse");
    for ddl in ["ALTER TABLE r3_parse_source ADD COLUMN extra INT", "ALTER TABLE r3_parse_source ALTER COLUMN id TYPE BIGINT",
        "DROP TABLE r3_parse_source", "CREATE TABLE r3_parse_source(id INT); INSERT INTO r3_parse_source VALUES(9)"] {
        let ddl_result = wire_query(&mut b, ddl).await;
        assert_eq!(code(&ddl_result).as_deref(), if ddl.contains("TYPE BIGINT") { Some("0A000") } else { None }, "{ddl}");
        results.push(outcome(ddl_result));
        let result = checked(&mut a, sql, ddl.contains("TYPE BIGINT")).await;
        assert_eq!(code(&result).as_deref(), if ddl == "DROP TABLE r3_parse_source" { Some("42P01") } else { None });
        results.push(outcome(result));
    }
    command(&mut a, "SET ROLE r").await;
    let denied = checked(&mut a, sql, false).await;
    assert_eq!(code(&denied).as_deref(), Some("42501")); results.push(outcome(denied));
    assert_eq!(code(&wire_query(&mut b, "GRANT SELECT ON r3_parse_source TO r").await), None);
    results.push(outcome(checked(&mut a, sql, false).await));
    results.push(outcome(checked(&mut a, sql, true).await));
    assert_eq!(code(&wire_query(&mut b, "REVOKE SELECT ON r3_parse_source FROM r").await), None);
    let denied = checked(&mut a, sql, false).await;
    assert_eq!(code(&denied).as_deref(), Some("42501"));
    assert!(!denied.iter().any(|(tag, _)| *tag == b'1'), "revocation must fail at Parse");
    results.push(outcome(denied));
    assert_eq!(code(&wire_query(&mut b, "GRANT SELECT ON r3_parse_source TO r").await), None);
    results.push(outcome(checked(&mut a, sql, false).await));
    command(&mut a, "RESET ROLE").await;
    results.push(outcome(checked(&mut a, sql, false).await));
    command(&mut a, "SET SESSION AUTHORIZATION r").await;
    results.push(outcome(checked(&mut a, sql, false).await));
    command(&mut a, "RESET SESSION AUTHORIZATION").await;
    assert_eq!(code(&wire_query(&mut b, "CREATE SCHEMA s; CREATE TABLE s.r3_parse_source(id INT); INSERT INTO s.r3_parse_source VALUES(22)").await), None);
    command(&mut a, "SET search_path TO s").await;
    results.push(outcome(checked(&mut a, sql, false).await));
    command(&mut a, "SET search_path TO public").await;
    command(&mut a, "BEGIN").await;
    command(&mut a, "ALTER TABLE r3_parse_source ADD COLUMN private INT").await;
    results.push(outcome(checked(&mut a, sql, false).await));
    let error = extended(&mut a, "SELECT * FROM absent", false).await;
    assert_eq!(code(&error).as_deref(), Some("42P01"));
    let aborted = checked(&mut a, sql, false).await;
    assert_eq!(code(&aborted).as_deref(), Some("25P02")); results.push(outcome(aborted));
    command(&mut a, "ROLLBACK").await;
    results.push(outcome(checked(&mut a, sql, false).await));
    // Rotate distinct texts, touch an old entry, then exceed the 16-entry cap.
    for id in 0..16 {
        let query = format!("SELECT id FROM r3_parse_source WHERE id = {id}");
        results.push(checked(&mut a, &query, false).await);
    }
    for id in 0..16 {
        let query = format!("SELECT id FROM r3_parse_source WHERE id = {id}");
        results.push(checked(&mut a, &query, true).await);
    }
    results.push(checked(&mut a, "SELECT id FROM r3_parse_source WHERE id = 16", false).await);
    results.push(checked(&mut a, "SELECT id FROM r3_parse_source WHERE id = 0", false).await);
    command(&mut b, "ALTER TABLE r3_parse_source ADD COLUMN lru INT").await;
    for id in 0..16 {
        let query = format!("SELECT id FROM r3_parse_source WHERE id = {id}");
        results.push(checked(&mut a, &query, false).await);
    }
    command(&mut a, "SET ROLE r").await;
    for id in 0..16 {
        let query = format!("SELECT id FROM r3_parse_source WHERE id = {id}");
        results.push(checked(&mut a, &query, false).await);
        results.push(checked(&mut a, &query, true).await);
    }
    command(&mut b, "REVOKE SELECT ON r3_parse_source FROM r").await;
    for id in 0..16 {
        let query = format!("SELECT id FROM r3_parse_source WHERE id = {id}");
        let denied = checked(&mut a, &query, false).await;
        assert_eq!(code(&denied).as_deref(), Some("42501"));
        results.push(denied);
    }
    command(&mut b, "GRANT SELECT ON r3_parse_source TO r").await;
    command(&mut a, "RESET ROLE").await;
    results.push(checked(&mut a, sql, false).await);
    results.push(checked(&mut a, sql, true).await);
    super::UNNAMED_REAUTHORIZE_FAIL.store(true, Ordering::SeqCst);
    results.push(checked(&mut a, sql, false).await);
    super::UNNAMED_REAUTHORIZE_FAIL.store(false, Ordering::SeqCst);
    for setting in ["SET DateStyle TO 'ISO, DMY'", "SET TimeZone TO 'America/New_York'"] {
        command(&mut a, setting).await;
        results.push(checked(&mut a, sql, false).await);
        results.push(checked(&mut a, sql, true).await);
    }
    command(&mut a, "SET ROLE r").await;
    let locked = "SELECT * FROM r3_parse_source FOR UPDATE";
    let denied = checked(&mut a, locked, false).await;
    assert_eq!(code(&denied).as_deref(), Some("42501"));
    results.push(denied);
    command(&mut a, "RESET ROLE").await;
    // Explicit parameter OID is part of the key, and successful reuse preserves it.
    let parameter_sql = "SELECT id FROM r3_parse_source WHERE id = $1";
    for hit in [false, true] {
        let before = super::UNNAMED_REUSE_HITS.load(Ordering::SeqCst);
        let misses = super::UNNAMED_REUSE_MISSES.load(Ordering::SeqCst);
        let response = extended_oids(&mut a, parameter_sql, false, &[23]).await;
        assert_eq!(code(&response), None);
        let hit = hit && !disabled;
        assert_eq!(super::UNNAMED_REUSE_HITS.load(Ordering::SeqCst) - before, usize::from(hit));
        assert_eq!(super::UNNAMED_REUSE_MISSES.load(Ordering::SeqCst) - misses, usize::from(!hit));
        results.push(response);
    }
    command(&mut b, "CREATE SEQUENCE r3_parse_source_seq OWNED BY r3_parse_source.id").await;
    // Sequence authority in this profile follows inherited ownership, not sequence ACL syntax.
    command(&mut b, "GRANT owner TO r").await;
    command(&mut a, "SET ROLE r").await;
    let sequence_sql = "SELECT nextval('r3_parse_source_seq')";
    results.push(checked(&mut a, sequence_sql, false).await);
    results.push(checked(&mut a, sequence_sql, true).await);
    command(&mut b, "REVOKE owner FROM r").await;
    let denied = checked(&mut a, sequence_sql, false).await;
    assert_eq!(code(&denied).as_deref(), Some("42501"));
    results.push(denied);
    // Regrant behind the same cached text: authorization must become live again.
    command(&mut b, "GRANT owner TO r").await;
    // A rejected Parse resets the core prepared context, so regrant first
    // succeeds through the fresh path and the following execution hits again.
    let granted = checked(&mut a, sequence_sql, false).await;
    assert_eq!(code(&granted), None);
    results.push(granted);
    results.push(checked(&mut a, sequence_sql, true).await);
    // Sequence authority must also be found below the outer SELECT.
    for nested in [
        "SELECT (SELECT nextval('r3_parse_source_seq'))",
        "SELECT EXISTS (SELECT nextval('r3_parse_source_seq'))",
        "SELECT 1 IN (SELECT nextval('r3_parse_source_seq'))",
    ] {
        results.push(checked(&mut a, nested, false).await);
        let warm = checked(&mut a, nested, true).await;
        assert_eq!(code(&warm), None);
        results.push(warm);
        command(&mut b, "REVOKE owner FROM r").await;
        let denied = checked(&mut a, nested, false).await;
        assert_eq!(code(&denied).as_deref(), Some("42501"));
        results.push(denied);
        command(&mut b, "GRANT owner TO r").await;
    }
    // DDL may invalidate the entry rather than hit it; both routes must give
    // precisely the same complete ErrorResponse as a fresh parse.
    results.push(checked(&mut a, sequence_sql, false).await);
    results.push(checked(&mut a, sequence_sql, true).await);
    // Main deliberately rejects sequence rename (also pinned by
    // sequence_ddl_network); keep that behavior and prove the old text still hits.
    let rename = extended(&mut b,
        "ALTER SEQUENCE r3_parse_source_seq RENAME TO r3_parse_source_seq2", true).await;
    assert_eq!(code(&rename).as_deref(), Some("0A000"));
    results.push(rename);
    results.push(checked(&mut a, sequence_sql, true).await);
    command(&mut b, "DROP SEQUENCE r3_parse_source_seq").await;
    let dropped = checked(&mut a, sequence_sql, false).await;
    // With no remaining owned sequence, this profile denies before name lookup.
    assert_eq!(code(&dropped).as_deref(), Some("42501"));
    results.push(dropped);
    command(&mut b, "REVOKE owner FROM r").await;
    command(&mut a, "RESET ROLE").await;
    command(&mut b, "GRANT USAGE ON SCHEMA s TO r").await;
    command(&mut b, "GRANT SELECT ON s.r3_parse_source TO r").await;
    command(&mut a, "SET ROLE r").await;
    let schema_sql = "SELECT * FROM s.r3_parse_source";
    results.push(checked(&mut a, schema_sql, false).await);
    results.push(checked(&mut a, schema_sql, true).await);
    command(&mut b, "REVOKE USAGE ON SCHEMA s FROM r").await;
    let denied = checked(&mut a, schema_sql, false).await;
    assert_eq!(code(&denied).as_deref(), Some("42501"));
    results.push(denied);
    command(&mut a, "RESET ROLE").await;
    // A nonsuperuser session keeps SET ROLE active across membership REVOKE.
    command(&mut b, "CREATE ROLE member LOGIN").await;
    command(&mut b, "GRANT r TO member").await;
    command(&mut a, "SET SESSION AUTHORIZATION member").await;
    command(&mut a, "SET ROLE r").await;
    results.push(checked(&mut a, sql, false).await);
    results.push(checked(&mut a, sql, true).await);
    command(&mut b, "REVOKE r FROM member").await;
    let context_misses = super::UNNAMED_CONTEXT_MISSES.load(Ordering::SeqCst);
    let denied = checked(&mut a, sql, false).await;
    assert_eq!(code(&denied).as_deref(), Some("42501"));
    assert_eq!(super::UNNAMED_CONTEXT_MISSES.load(Ordering::SeqCst) - context_misses,
        usize::from(!disabled), "membership REVOKE resets the effective role, invalidating Parse context");
    results.push(denied);
    command(&mut a, "RESET ROLE").await;
    command(&mut a, "RESET SESSION AUTHORIZATION").await;
    // SELECT set_config changes context despite passing the SELECT text filter.
    for setting in ["SELECT set_config('search_path', 's', false)",
        "SELECT set_config('role', 'r', false)"] {
        command(&mut a, "RESET ROLE").await;
        command(&mut a, "SET search_path TO public").await;
        results.push(checked(&mut a, sql, false).await);
        results.push(checked(&mut a, sql, true).await);
        let response = extended(&mut a, setting, true).await;
        // role is not admitted by set_config in this profile (SET ROLE is
        // covered above); rejected settings must preserve the valid context.
        let rejected_role = setting.contains("'role'");
        assert_eq!(code(&response).as_deref(), if rejected_role { Some("22023") } else { None });
        results.push(response);
        results.push(checked(&mut a, sql, rejected_role).await);
    }
    command(&mut a, "RESET ROLE").await;
    command(&mut a, "SET search_path TO public").await;
    // An error with both diagnostic fields, followed by a hit and a plain error.
    command(&mut b, "CREATE SCHEMA diag").await;
    command(&mut b, "CREATE EXTENSION \"uuid-ossp\" WITH SCHEMA diag").await;
    let diagnostic_error = extended(&mut a, "DROP SCHEMA diag", true).await;
    for field in *b"DH" { assert!(error_field(&diagnostic_error, field).is_some()); }
    results.push(diagnostic_error.clone());
    results.push(checked(&mut a, sql, false).await);
    results.push(checked(&mut a, sql, true).await);
    let plain_error = extended(&mut a, "SELECT * FROM absent", false).await;
    assert_eq!(code(&plain_error).as_deref(), Some("42P01"));
    for field in *b"DH" {
        assert_ne!(error_field(&plain_error, field), error_field(&diagnostic_error, field));
    }
    results.push(plain_error);
    // Deterministically commit changes after the first validity check.
    for change in ["ALTER TABLE r3_parse_source ADD COLUMN raced INT",
        "REVOKE SELECT ON r3_parse_source FROM r"] {
        command(&mut a, "SET ROLE r").await;
        results.push(checked(&mut a, sql, false).await);
        results.push(checked(&mut a, sql, true).await);
        let response = if disabled {
            command(&mut b, change).await;
            checked(&mut a, sql, false).await
        } else {
            let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
            let (resume_tx, resume_rx) = std::sync::mpsc::channel();
            *super::UNNAMED_BEFORE_REAUTHORIZE.lock().unwrap() = Some(Box::new(move || {
                entered_tx.send(()).unwrap();
                resume_rx.recv_timeout(std::time::Duration::from_secs(10)).unwrap();
            }));
            let change_in_window = async {
                tokio::time::timeout(std::time::Duration::from_secs(10), entered_rx)
                    .await.expect("unnamed reauthorization hook did not fire").unwrap();
                command(&mut b, change).await;
                resume_tx.send(()).unwrap();
            };
            let (response, ()) = tokio::join!(checked(&mut a, sql, false), change_in_window);
            assert!(super::UNNAMED_BEFORE_REAUTHORIZE.lock().unwrap().is_none(),
                "unnamed reauthorization hook was not consumed");
            response
        };
        assert_eq!(code(&response).as_deref(), if change.starts_with("REVOKE") { Some("42501") } else { None });
        results.push(response);
        command(&mut a, "RESET ROLE").await;
    }
    command(&mut b, "GRANT SELECT ON r3_parse_source TO r").await;
    // Two readers race committed DDL and privilege changes from a third session.
    let mut c = connect(&f).await;
    command(&mut a, "SET ROLE r").await;
    command(&mut c, "SET ROLE r").await;
    for change in ["REVOKE SELECT ON r3_parse_source FROM r",
        "ALTER TABLE r3_parse_source ADD COLUMN churn INT",
        "ALTER TABLE r3_parse_source DROP COLUMN churn",
        "GRANT SELECT ON r3_parse_source TO r"] {
        command(&mut b, change).await;
        // Both readers cross the committed change before either next Parse.
        let hits = super::UNNAMED_REUSE_HITS.load(Ordering::SeqCst);
        let misses = super::UNNAMED_REUSE_MISSES.load(Ordering::SeqCst);
        let (left, right) = tokio::join!(extended(&mut a, sql, false), extended(&mut c, sql, false));
        assert_eq!(super::UNNAMED_REUSE_HITS.load(Ordering::SeqCst) - hits, 0);
        assert_eq!(super::UNNAMED_REUSE_MISSES.load(Ordering::SeqCst) - misses, 2);
        assert!(matches!(code(&left).as_deref(), None | Some("42501")));
        assert_eq!(left, right);
        results.push(left);
        results.push(right);
    }
    results.push(checked(&mut a, sql, true).await);
    results.push(checked(&mut c, sql, true).await);
    drop(a); drop(b); drop(c); f.stop().await;
    results
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repeated_unnamed_parse_matches_full_path() {
    let _scenario = SCENARIO.lock().await;
    let _reset = ResetReuse;
    let cached = scenario(false).await;
    let full = scenario(true).await;
    assert_eq!(cached, full);
}

async fn checked_dml(
    stream: &mut TcpStream,
    sql: &str,
    params: &[&str],
    hit: bool,
    expected: Option<&str>,
) -> Outcome {
    let reply = check_reuse(sql, hit, extended_values(stream, sql, "", &[], params, true)).await;
    assert_eq!(code(&reply).as_deref(), expected, "{sql}: {reply:?}");
    if let Some(code) = expected {
        // Read-only checks happen at Execute. The other refusals belong to Parse.
        assert_eq!(reply.iter().any(|(tag, _)| *tag == b'1'), code == "25006", "{sql}");
    }
    reply
}

async fn simple_ok(stream: &mut TcpStream, sql: &str) -> Outcome {
    let reply = wire_query(stream, sql).await;
    assert_eq!(code(&reply), None, "{sql}: {reply:?}");
    reply
}

fn row_description_counts(reply: &Outcome) -> Vec<u16> {
    reply.iter().filter(|(tag, _)| *tag == b'T')
        .map(|(_, body)| u16::from_be_bytes(body[..2].try_into().unwrap()))
        .collect()
}

async fn dml_scenario(disabled: bool) -> Vec<Outcome> {
    super::UNNAMED_REUSE_DISABLED.store(disabled, Ordering::SeqCst);
    let f = Fixture::new().await;
    let mut a = connect(&f).await;
    let mut b = connect(&f).await;
    let mut results = Vec::new();
    for sql in [
        "CREATE TABLE r3_parse_source (id INT, version INT)",
        "CREATE ROLE dml_writer",
        "CREATE ROLE dml_denied",
        "GRANT SELECT, INSERT, UPDATE, DELETE ON r3_parse_source TO dml_writer",
        "CREATE SCHEMA dml_other",
        "CREATE TABLE dml_other.r3_parse_source (id INT, version INT)",
    ] {
        simple_ok(&mut b, sql).await;
    }

    let insert = "INSERT INTO r3_parse_source (id, version) VALUES ($1, $2) RETURNING *";
    let update = "UPDATE r3_parse_source SET version = $2 WHERE id = $1 RETURNING version";
    let optimistic = "UPDATE r3_parse_source SET version = version + 1 WHERE id = $1 AND version = 20";
    let delete = "DELETE FROM r3_parse_source WHERE id = $1 RETURNING id, version";
    // `version` makes the optimistic UPDATE go through the catalog fallback.
    assert!(!giggles_core::relational::Prepared::application_dml(
        optimistic, giggles_pg_catalog::catalog_identifier,
    ));
    let cycle = [
        (insert, &["1", "10"][..], 2, 2),
        (update, &["1", "20"][..], 2, 1),
        (optimistic, &["1"][..], 1, 0),
        (delete, &["1"][..], 1, 2),
    ];
    for hit in [false, true] {
        for (sql, params, num_oids, columns) in cycle {
            let reply = checked_dml(&mut a, sql, params, hit, None).await;
            let mut parameter_description = vec![0, num_oids];
            for _ in 0..num_oids {
                parameter_description.extend(23u32.to_be_bytes());
            }
            assert!(reply.contains(&(b't', parameter_description)), "{sql}");
            let descriptions = if columns == 0 { vec![] } else { vec![columns; 2] };
            assert_eq!(row_description_counts(&reply), descriptions, "{sql}");
            if sql == delete {
                // DELETE returns the changed value from the preceding optimistic UPDATE.
                assert!(reply.contains(&(b'D', b"\0\x02\0\0\0\x011\0\0\0\x0221".to_vec())));
            }
            results.push(reply);
        }
    }
    assert_eq!(results[..cycle.len()], results[cycle.len()..]);

    // Simple writes preserve the entries. Utility commands still invalidate them.
    simple_ok(&mut a, "UPDATE r3_parse_source SET version = 0 WHERE id = 999").await;
    results.push(checked_dml(&mut a, optimistic, &["1"], true, None).await);
    for sql in [
        "PREPARE dml_plan AS UPDATE r3_parse_source SET version = 0 WHERE id = 999",
        "EXECUTE dml_plan",
        "DEALLOCATE ALL",
        "DISCARD PLANS",
        "DISCARD ALL",
        "UPDATE r3_parse_source SET version = 0 WHERE id = 999; SET search_path TO public",
    ] {
        results.push(simple_ok(&mut a, sql).await);
        results.push(checked_dml(&mut a, optimistic, &["1"], false, None).await);
        results.push(checked_dml(&mut a, optimistic, &["1"], true, None).await);
    }

    let bad = "UPDATE r3_parse_source SET absent = 0 WHERE id = $1";
    for _ in 0..2 {
        results.push(checked_dml(&mut a, bad, &["1"], false, Some("42703")).await);
    }
    let modifying_cte = "WITH gone AS (DELETE FROM r3_parse_source WHERE id = 999 RETURNING id) SELECT id FROM gone";
    for _ in 0..2 {
        results.push(checked_dml(&mut a, modifying_cte, &[], false, None).await);
    }
    simple_ok(&mut a, "SET default_transaction_read_only = on").await;
    for hit in [false, true] {
        results.push(checked_dml(&mut a, optimistic, &["1"], hit, Some("25006")).await);
    }
    simple_ok(&mut a, "RESET default_transaction_read_only").await;
    results.push(checked_dml(&mut a, optimistic, &["1"], false, None).await);
    results.push(checked_dml(&mut a, optimistic, &["1"], true, None).await);
    simple_ok(&mut a, "BEGIN READ ONLY").await;
    results.push(checked_dml(&mut a, optimistic, &["1"], false, Some("25006")).await);
    simple_ok(&mut a, "ROLLBACK").await;
    simple_ok(&mut a, "BEGIN").await;
    for _ in 0..2 {
        results.push(checked_dml(&mut a, optimistic, &["1"], false, None).await);
    }
    simple_ok(&mut a, "ROLLBACK").await;

    for (set, reset) in [
        ("SET ROLE dml_denied", "RESET ROLE"),
        ("SET SESSION AUTHORIZATION dml_denied", "RESET SESSION AUTHORIZATION"),
    ] {
        results.push(checked_dml(&mut a, optimistic, &["1"], false, None).await);
        results.push(checked_dml(&mut a, optimistic, &["1"], true, None).await);
        simple_ok(&mut a, set).await;
        results.push(checked_dml(&mut a, optimistic, &["1"], false, Some("42501")).await);
        simple_ok(&mut a, reset).await;
    }
    simple_ok(&mut a, "SET ROLE dml_writer").await;
    for (privilege, sql, params) in [
        ("INSERT", insert, &["2", "10"][..]),
        ("UPDATE", update, &["2", "20"][..]),
        ("DELETE", delete, &["2"][..]),
        ("SELECT", optimistic, &["2"][..]),
        ("SELECT", insert, &["3", "10"][..]),
    ] {
        results.push(checked_dml(&mut a, sql, params, false, None).await);
        results.push(checked_dml(&mut a, sql, params, true, None).await);
        simple_ok(&mut b, &format!("REVOKE {privilege} ON r3_parse_source FROM dml_writer")).await;
        results.push(checked_dml(&mut a, sql, params, false, Some("42501")).await);
        simple_ok(&mut b, &format!("GRANT {privilege} ON r3_parse_source TO dml_writer")).await;
        results.push(checked_dml(&mut a, sql, params, false, None).await);
        results.push(checked_dml(&mut a, sql, params, true, None).await);
    }
    simple_ok(&mut a, "RESET ROLE").await;

    // Exercise the context comparison without a standalone SET clearing the LRU.
    results.push(checked_dml(&mut a, insert, &["8", "10"], false, None).await);
    results.push(simple_ok(&mut a, "SELECT set_config('search_path', 'dml_other', false)").await);
    results.push(checked_dml(&mut a, insert, &["9", "20"], false, None).await);
    results.push(checked_dml(&mut a, insert, &["10", "30"], true, None).await);
    for (sql, has_row) in [
        ("SELECT version FROM public.r3_parse_source WHERE id = 9", false),
        ("SELECT version FROM dml_other.r3_parse_source WHERE id = 9 AND version = 20", true),
    ] {
        let reply = simple_ok(&mut b, sql).await;
        assert_eq!(reply.iter().any(|(tag, _)| *tag == b'D'), has_row, "{sql}");
        results.push(reply);
    }
    simple_ok(&mut a, "SET search_path TO public").await;
    results.push(checked_dml(&mut a, insert, &["11", "10"], false, None).await);
    results.push(checked_dml(&mut a, insert, &["12", "10"], true, None).await);
    simple_ok(&mut b, "ALTER TABLE r3_parse_source ADD COLUMN extra INT").await;
    let after_alter = checked_dml(&mut a, insert, &["13", "10"], false, None).await;
    assert_eq!(row_description_counts(&after_alter), [3, 3]);
    results.push(after_alter);
    results.push(checked_dml(&mut a, insert, &["14", "10"], true, None).await);
    simple_ok(&mut b, "DROP TABLE r3_parse_source").await;
    results.push(checked_dml(&mut a, insert, &["15", "10"], false, Some("42P01")).await);

    // This profile grants serial sequence USAGE through the sequence owner's role.
    // Keep the table INSERT grant while revoking that role from the writer.
    for sql in [
        "CREATE ROLE dml_sequence_owner",
        "GRANT CREATE ON SCHEMA public TO dml_sequence_owner",
        "SET ROLE dml_sequence_owner",
        "CREATE TABLE r3_parse_source_serial (id SERIAL PRIMARY KEY, payload INT)",
        "RESET ROLE",
        "GRANT INSERT ON r3_parse_source_serial TO dml_writer",
        "GRANT dml_sequence_owner TO dml_writer",
    ] {
        simple_ok(&mut b, sql).await;
    }
    simple_ok(&mut a, "SET ROLE dml_writer").await;
    let serial = "INSERT INTO r3_parse_source_serial (payload) VALUES ($1)";
    results.push(checked_dml(&mut a, serial, &["1"], false, None).await);
    results.push(checked_dml(&mut a, serial, &["2"], true, None).await);
    simple_ok(&mut b, "REVOKE dml_sequence_owner FROM dml_writer").await;
    results.push(checked_dml(&mut a, serial, &["3"], false, Some("42501")).await);
    simple_ok(&mut b, "GRANT dml_sequence_owner TO dml_writer").await;
    results.push(checked_dml(&mut a, serial, &["3"], false, None).await);
    results.push(checked_dml(&mut a, serial, &["4"], true, None).await);

    drop(a);
    drop(b);
    f.stop().await;
    results
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repeated_unnamed_dml_parse_matches_full_path() {
    let _scenario = SCENARIO.lock().await;
    let _reset = ResetReuse;
    let cached = dml_scenario(false).await;
    let full = dml_scenario(true).await;
    assert_eq!(cached, full);
}
