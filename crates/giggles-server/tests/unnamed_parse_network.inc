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
    let mut parse = vec![];
    giggles_server::pgwire::z(name, &mut parse);
    giggles_server::pgwire::z(sql, &mut parse);
    parse.extend((oids.len() as u16).to_be_bytes());
    for oid in oids { parse.extend(oid.to_be_bytes()); }
    let mut bind = vec![0];
    giggles_server::pgwire::z(name, &mut bind);
    bind.extend(0u16.to_be_bytes());
    bind.extend((oids.len() as u16).to_be_bytes());
    for _ in oids { bind.extend(1u32.to_be_bytes()); bind.push(b'9'); }
    bind.extend(0u16.to_be_bytes());
    let mut packet = Vec::new();
    for (tag, body) in [(b'P', parse.as_slice()), (b'B', &bind), (b'D', b"P\0"),
        (b'E', &[0; 5]), (b'S', &[])] {
        giggles_server::pgwire::frame(tag, body, &mut packet).unwrap();
    }
    stream.write_all(&packet).await.unwrap();
    let result = wire_cycle(stream).await;
    if named {
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
    let hits = super::UNNAMED_REUSE_HITS.load(Ordering::SeqCst);
    let misses = super::UNNAMED_REUSE_MISSES.load(Ordering::SeqCst);
    let result = extended(stream, sql, false).await;
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
    command(&mut b, "CREATE SEQUENCE r3_parse_source_seq").await;
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
    struct Reset;
    impl Drop for Reset { fn drop(&mut self) { super::UNNAMED_REUSE_DISABLED.store(false, Ordering::SeqCst); } }
    let _reset = Reset;
    let cached = scenario(false).await;
    let full = scenario(true).await;
    assert_eq!(cached, full);
}
