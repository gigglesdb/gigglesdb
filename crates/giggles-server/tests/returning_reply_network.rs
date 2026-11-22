#[path = "wire_support/mod.rs"]
#[allow(dead_code)]
mod wire_support;
use giggles_core::{Admin, Engine, Limits, roles::NewRole};
use giggles_server::{
    Running, Service,
    auth::{PostgresqlConfig, TlsMode, serve_postgresql},
    clock::Clock,
};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{io::AsyncWriteExt, net::TcpStream};
use tokio_postgres::{Client, Config, NoTls, SimpleQueryMessage};
use wire_support::{bind, cstr, execute, parse, pg_read, pg_send, pg_sync};

const BAD_TS: &str = "TIMESTAMPTZ '294276-12-31 23:59:59+00'";
const OK_TS: &str = "TIMESTAMPTZ '2026-01-01 00:00:00+00'";

struct Fixture {
    running: Running,
    path: PathBuf,
}

impl Fixture {
    async fn new(engine: Engine) -> Self {
        // Parallel tests can read the same clock value, so a counter keeps the path unique.
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "returning-reply-{}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&path).unwrap();
        let admin = Admin::create_engine(path.join("db"), Limits::default(), engine).unwrap();
        admin
            .roles()
            .create(NewRole {
                name: "owner".into(),
                superuser: true,
                login: true,
                ..NewRole::default()
            })
            .unwrap();
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
        let service = Service::new(admin, vec![], Clock::new(0, 3600).unwrap()).unwrap();
        let running = serve_postgresql(config.listen, service, Arc::new(config.build().unwrap()))
            .await
            .unwrap();
        Self { running, path }
    }

    async fn client(&self) -> Client {
        let mut config = Config::new();
        config
            .hostaddr(self.running.address.ip())
            .port(self.running.address.port())
            .user("owner")
            .dbname("postgres");
        let (client, connection) = config.connect(NoTls).await.unwrap();
        tokio::spawn(async move { connection.await.unwrap() });
        client
    }

    async fn raw(&self) -> TcpStream {
        let mut stream = TcpStream::connect(self.running.address).await.unwrap();
        let mut startup = 196608u32.to_be_bytes().to_vec();
        for value in ["user", "owner", "database", "postgres", ""] {
            cstr(value, &mut startup);
        }
        stream.write_u32((startup.len() + 4) as u32).await.unwrap();
        stream.write_all(&startup).await.unwrap();
        loop {
            let (tag, body) = pg_read(&mut stream).await;
            assert_ne!(tag, b'E', "startup: {}", String::from_utf8_lossy(&body));
            if tag == b'Z' {
                return stream;
            }
        }
    }

    async fn stop(self) {
        self.running.shutdown().await.unwrap();
        std::fs::remove_dir_all(self.path).unwrap();
    }
}

async fn count(client: &Client) -> i64 {
    client
        .query_one("SELECT count(*) FROM t", &[])
        .await
        .unwrap()
        .get(0)
}

async fn setup(client: &Client) {
    client
        .batch_execute("CREATE TABLE t (id int PRIMARY KEY, ts timestamptz, pad text)")
        .await
        .unwrap();
}

fn check_error(error: tokio_postgres::Error) {
    let error = error.as_db_error().unwrap();
    assert_eq!(error.code().code(), "22008");
    assert_eq!(error.message(), "request rejected; consult documented code");
}

async fn simple_rollback(engine: Engine) {
    let f = Fixture::new(engine).await;
    let writer = f.client().await;
    let observer = f.client().await;
    setup(&observer).await;
    writer.simple_query("SET TIME ZONE 1").await.unwrap();
    let query = format!("INSERT INTO t VALUES (1, {BAD_TS}) RETURNING ts");
    check_error(writer.simple_query(&query).await.unwrap_err());
    assert_eq!(count(&observer).await, 0);

    writer.simple_query("SET TIME ZONE 0").await.unwrap();
    let reply = writer.simple_query(&query).await.unwrap();
    assert!(matches!(&reply[..], [SimpleQueryMessage::RowDescription(_),
        SimpleQueryMessage::Row(row), SimpleQueryMessage::CommandComplete(1)]
        if row.get(0).unwrap().starts_with("294276-12-31 23:59:59")));
    assert_eq!(count(&observer).await, 1);
    drop((writer, observer));
    f.stop().await;
}

#[tokio::test]
async fn returning_reply_legacy_simple_rolls_back_encoding_error() {
    simple_rollback(Engine::Legacy).await;
}

#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn returning_reply_v2_simple_rolls_back_encoding_error() {
    simple_rollback(Engine::V2).await;
}

#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn returning_reply_v2_spill_rolls_back_late_encoding_error() {
    let _serial = crate::api_result_stream::SERIAL.lock().await;
    let f = Fixture::new(Engine::V2).await;
    let writer = f.client().await;
    let observer = f.client().await;
    setup(&observer).await;
    writer.simple_query("SET TIME ZONE 1").await.unwrap();
    const ROWS: i64 = 800;
    let insert = |last| {
        let values = (1..=ROWS)
            .map(|id| format!("({id}, {})", if id == ROWS { last } else { OK_TS }))
            .collect::<Vec<_>>()
            .join(",");
        format!("INSERT INTO t (id, ts) VALUES {values} RETURNING ts, repeat('r', 2000)")
    };
    check_error(writer.simple_query(&insert(BAD_TS)).await.unwrap_err());
    assert_eq!(count(&observer).await, 0);

    let reply = writer.simple_query(&insert(OK_TS)).await.unwrap();
    assert_eq!(
        reply
            .iter()
            .filter(|m| matches!(m, SimpleQueryMessage::Row(_)))
            .count(),
        ROWS as usize
    );
    assert!(
        matches!(reply.last(), Some(SimpleQueryMessage::CommandComplete(n)) if *n == ROWS as u64)
    );
    assert_eq!(count(&observer).await, ROWS);
    drop((writer, observer));
    f.stop().await;
}

#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn returning_reply_v2_spill_rolls_back_frame_limit() {
    let _serial = crate::api_result_stream::SERIAL.lock().await;
    let f = Fixture::new(Engine::V2).await;
    let writer = f.client().await;
    let observer = f.client().await;
    setup(&observer).await;
    let values = ids(256);
    // The existing 256-row wire chunk exceeds the 8 MiB frame buffer.
    let error = writer
        .simple_query(&format!(
            "INSERT INTO t (id) VALUES {values} RETURNING repeat('r', 33000)"
        ))
        .await
        .unwrap_err();
    assert_eq!(error.as_db_error().unwrap().code().code(), "54000");
    assert_eq!(count(&observer).await, 0);
    drop((writer, observer));
    f.stop().await;
}

#[cfg(feature = "engine-rocksdb")]
fn ids(count: i64) -> String {
    (1..=count)
        .map(|id| format!("({id})"))
        .collect::<Vec<_>>()
        .join(",")
}

#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn returning_reply_v2_spill_fault_reports_unknown_outcome() {
    let _serial = crate::api_result_stream::SERIAL.lock().await;
    let f = Fixture::new(Engine::V2).await;
    let observer = f.client().await;
    setup(&observer).await;
    let mut writer = f.raw().await;
    let marker = f.path.file_name().unwrap().to_str().unwrap().to_owned();
    let mut probe = crate::api_result_stream::SpillProbe::new(marker.clone());
    let values = ids(2000);
    let mut query = Vec::new();
    cstr(
        &format!("INSERT INTO t (id) VALUES {values} RETURNING repeat('{marker}', 600)"),
        &mut query,
    );
    pg_send(&mut writer, b'Q', &query).await;
    // The first delivered frame proves execution reached post-commit replay.
    assert_eq!(pg_read(&mut writer).await.0, b'T');
    probe.truncate();
    let mut error = None;
    loop {
        let (tag, body) = pg_read(&mut writer).await;
        match tag {
            b'E' => {
                assert!(error.replace(body).is_none());
            }
            b'Z' => {
                assert_eq!(body, b"I");
                break;
            }
            b'D' => {}
            _ => panic!("unexpected frame {tag}: {body:?}"),
        }
    }
    probe.cleaned().await;
    assert_eq!(count(&observer).await, 2000);
    drop((writer, observer));
    f.stop().await;
    let error = error.expect("late spill error");
    assert!(
        error.windows(7).any(|f| f == b"C08007\0"),
        "{}",
        String::from_utf8_lossy(&error)
    );
}

async fn extended(stream: &mut TcpStream, sql: &str, max: u32, binary: bool) -> Vec<(u8, Vec<u8>)> {
    pg_send(stream, b'P', &parse("", sql)).await;
    pg_send(stream, b'B', &bind("", "", &[], binary)).await;
    pg_send(stream, b'E', &execute("", max)).await;
    pg_sync(stream).await
}

fn tags(frames: &[(u8, Vec<u8>)]) -> Vec<u8> {
    frames.iter().map(|f| f.0).collect()
}

#[tokio::test]
async fn returning_reply_extended_text_binary_and_suspended_portal() {
    #[cfg(feature = "engine-rocksdb")]
    let engine = Engine::V2;
    #[cfg(not(feature = "engine-rocksdb"))]
    let engine = Engine::Legacy;
    let f = Fixture::new(engine).await;
    let observer = f.client().await;
    setup(&observer).await;
    let mut writer = f.raw().await;
    pg_send(&mut writer, b'Q', b"SET TIME ZONE 1\0").await;
    loop {
        let (tag, body) = pg_read(&mut writer).await;
        assert_ne!(tag, b'E', "SET: {}", String::from_utf8_lossy(&body));
        if tag == b'Z' {
            break;
        }
    }
    let bad = format!("INSERT INTO t VALUES (1, {BAD_TS}) RETURNING ts");
    let failed = extended(&mut writer, &bad, 0, false).await;
    assert_eq!(tags(&failed), b"12EZ", "{failed:?}");
    assert!(failed[2].1.windows(7).any(|f| f == b"C22008\0"));
    assert_eq!(count(&observer).await, 0);

    let ok = format!("INSERT INTO t VALUES (2, {OK_TS}) RETURNING ts");
    let success = extended(&mut writer, &ok, 0, false).await;
    assert_eq!(tags(&success), b"12DCZ", "{success:?}");
    assert_eq!(count(&observer).await, 1);

    let binary = extended(&mut writer, &bad, 0, true).await;
    assert_eq!(tags(&binary), b"12DCZ", "{binary:?}");
    assert_eq!(&binary[2].1[..6], &[0, 1, 0, 0, 0, 8]);
    assert_eq!(count(&observer).await, 2);

    let two = format!("INSERT INTO t VALUES (3, {OK_TS}), (4, {BAD_TS}) RETURNING ts");
    let suspended = extended(&mut writer, &two, 1, false).await;
    assert_eq!(tags(&suspended), b"12DsZ", "{suspended:?}");
    assert_eq!(suspended.last().unwrap().1, b"I");
    assert_eq!(count(&observer).await, 4);
    drop((writer, observer));
    f.stop().await;
}

#[tokio::test]
async fn returning_reply_implicit_batch_rolls_back_both_statements() {
    #[cfg(feature = "engine-rocksdb")]
    let engine = Engine::V2;
    #[cfg(not(feature = "engine-rocksdb"))]
    let engine = Engine::Legacy;
    let f = Fixture::new(engine).await;
    let writer = f.client().await;
    let observer = f.client().await;
    setup(&observer).await;
    writer.simple_query("SET TIME ZONE 1").await.unwrap();
    check_error(writer.simple_query(&format!(
        "INSERT INTO t VALUES (1, {OK_TS}) RETURNING ts;          INSERT INTO t VALUES (2, {BAD_TS}) RETURNING ts"
    )).await.unwrap_err());
    assert_eq!(count(&observer).await, 0);
    drop((writer, observer));
    f.stop().await;
}

#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn returning_reply_implicit_batch_reserves_commit_parameter_status() {
    let _serial = crate::api_result_stream::SERIAL.lock().await;
    let f = Fixture::new(Engine::V2).await;
    let writer = f.client().await;
    let observer = f.client().await;
    setup(&observer).await;
    let path = format!("{},public", vec!["s".repeat(63); 80].join(","));
    writer
        .simple_query(&format!("SET search_path TO {path}"))
        .await
        .unwrap();
    let values = ids(256);
    let reply = writer.simple_query(&format!(
        "SET LOCAL search_path TO public;          INSERT INTO t (id) VALUES {values} RETURNING repeat('r', 32750)"
    )).await.unwrap();
    assert!(matches!(
        reply.last(),
        Some(SimpleQueryMessage::CommandComplete(256))
    ));
    assert_eq!(count(&observer).await, 256);
    drop((writer, observer));
    f.stop().await;
}

async fn query_frames(stream: &mut TcpStream, sql: &str) -> Vec<(u8, Vec<u8>)> {
    let mut query = Vec::new();
    cstr(sql, &mut query);
    pg_send(stream, b'Q', &query).await;
    until_query_ready(stream).await
}

async fn until_query_ready(stream: &mut TcpStream) -> Vec<(u8, Vec<u8>)> {
    let mut frames = Vec::new();
    loop {
        let frame = pg_read(stream).await;
        let ready = frame.0 == b'Z';
        frames.push(frame);
        if ready {
            return frames;
        }
    }
}

#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn returning_reply_copy_to_rolls_back_encoding_error() {
    let _serial = crate::api_result_stream::SERIAL.lock().await;
    let f = Fixture::new(Engine::V2).await;
    let observer = f.client().await;
    setup(&observer).await;
    let mut writer = f.raw().await;
    assert_eq!(
        tags(&query_frames(&mut writer, "SET TIME ZONE 1").await),
        b"SCZ"
    );

    // Exercise both the in-memory and spilled versions of the same COPY reply.
    for n in [1, 600] {
        let values = (1..=n)
            .map(|id| format!("({id}, {})", if id == n { BAD_TS } else { OK_TS }))
            .collect::<Vec<_>>()
            .join(",");
        let copy = format!(
            "COPY (INSERT INTO t (id, ts) VALUES {values} RETURNING ts, repeat('r', 2000)) TO STDOUT"
        );
        let failed = query_frames(&mut writer, &copy).await;
        let written = count(&observer).await;
        assert_eq!(written, 0);
        assert_eq!(tags(&failed), b"EZ");
        assert!(failed[0].1.windows(7).any(|field| field == b"C22008\0"));
        assert_eq!(failed[1].1, b"I");
    }

    let ok = query_frames(
        &mut writer,
        &format!("COPY (INSERT INTO t VALUES (1, {OK_TS}) RETURNING ts) TO STDOUT"),
    )
    .await;
    assert_eq!(tags(&ok), b"HdcCZ");
    assert_eq!(ok[3].1, b"COPY 1\0");
    assert_eq!(count(&observer).await, 1);

    let values = (2..=601)
        .map(|id| format!("({id}, {OK_TS})"))
        .collect::<Vec<_>>()
        .join(",");
    let ok = query_frames(&mut writer, &format!(
        "COPY (INSERT INTO t (id, ts) VALUES {values} RETURNING ts, repeat('r', 2000)) TO STDOUT"
    )).await;
    assert_eq!(ok[0].0, b'H');
    assert_eq!(ok.iter().filter(|frame| frame.0 == b'd').count(), 600);
    assert_eq!(ok[ok.len() - 3].0, b'c');
    assert_eq!(ok[ok.len() - 2], (b'C', b"COPY 600\0".to_vec()));
    assert_eq!(ok.last().unwrap().1, b"I");
    assert_eq!(count(&observer).await, 601);
    drop((writer, observer));
    f.stop().await;
}

#[tokio::test]
async fn returning_reply_copy_to_obeys_read_only() {
    let f = Fixture::new(Engine::Legacy).await;
    let observer = f.client().await;
    setup(&observer).await;
    let mut writer = f.raw().await;
    for (setting, expected_status) in [
        ("BEGIN READ ONLY", b'E'),
        ("SET default_transaction_read_only = on", b'I'),
        ("SET session_replication_role = replica", b'I'),
    ] {
        let setup = query_frames(&mut writer, setting).await;
        assert!(!tags(&setup).contains(&b'E'), "{setup:?}");
        // Replica mode still obeys the preceding default read-only setting.
        let direct = query_frames(&mut writer, "INSERT INTO t (id) VALUES (1) RETURNING id").await;
        assert!(direct[0].1.windows(7).any(|field| field == b"C25006\0"));
        if expected_status == b'E' {
            assert_eq!(tags(&query_frames(&mut writer, "ROLLBACK").await), b"CZ");
            let _ = query_frames(&mut writer, setting).await;
        }
        let copy = query_frames(
            &mut writer,
            "COPY (INSERT INTO t (id) VALUES (1) RETURNING id) TO STDOUT",
        )
        .await;
        assert_eq!(tags(&copy), b"EZ");
        assert!(copy[0].1.windows(7).any(|field| field == b"C25006\0"));
        assert_eq!(copy[1].1, [expected_status]);
        assert_eq!(count(&observer).await, 0);
        if expected_status == b'E' {
            assert_eq!(tags(&query_frames(&mut writer, "ROLLBACK").await), b"CZ");
        }
    }
    let _ = query_frames(&mut writer, "SET default_transaction_read_only = off").await;
    let copy = query_frames(
        &mut writer,
        "COPY (INSERT INTO t (id) VALUES (1) RETURNING id) TO STDOUT",
    )
    .await;
    assert_eq!(tags(&copy), b"HdcCZ");
    assert_eq!(count(&observer).await, 1);
    drop((writer, observer));
    f.stop().await;
}

#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn returning_reply_w6_copy_from_near_limit_rollback_restores_search_path() {
    let _serial = crate::api_result_stream::SERIAL.lock().await;
    let f = Fixture::new(Engine::V2).await;
    let observer = f.client().await;
    let mut writer = f.raw().await;
    observer
        .batch_execute("CREATE TABLE t (id int PRIMARY KEY)")
        .await
        .unwrap();
    let path = format!("{},public", vec!["s".repeat(63); 80].join(","));
    let status = query_frames(&mut writer, &format!("SET search_path TO {path}")).await;
    assert_eq!(tags(&status), b"SCZ");

    let values = ids(256);
    let sql = format!(
        "COPY t FROM STDIN; SET LOCAL search_path TO public; \
         INSERT INTO t VALUES {values} RETURNING repeat('x',32750); \
         INSERT INTO t VALUES (257) RETURNING repeat('x',4000)"
    );
    let frames = copy_from_stdin(&mut writer, &sql, b"0\n").await;
    let written = count(&observer).await;
    drop((writer, observer));
    f.stop().await;

    assert_eq!(written, 0);
    assert_eq!(frames[1], (b'S', b"search_path\0public\0".to_vec()));
    let tail = &frames[frames.len() - 4..];
    assert_eq!(
        tags(tail),
        b"CSEZ",
        "tail tags, including restored search_path"
    );
    assert_eq!(tail[1], status[0]);
    assert!(tail[2].1.windows(7).any(|field| field == b"C54000\0"));
    assert_eq!(tail[3].1, b"I");
}

#[cfg(feature = "engine-rocksdb")]
async fn copy_from_stdin(writer: &mut TcpStream, sql: &str, data: &[u8]) -> Vec<(u8, Vec<u8>)> {
    let mut query = Vec::new();
    cstr(sql, &mut query);
    pg_send(writer, b'Q', &query).await;
    assert_eq!(pg_read(writer).await.0, b'G');
    pg_send(writer, b'd', data).await;
    pg_send(writer, b'c', &[]).await;
    until_query_ready(writer).await
}

#[cfg(feature = "engine-rocksdb")]
async fn explicit_commit_near_reply_limit(copy: bool, width: usize) {
    let _serial = crate::api_result_stream::SERIAL.lock().await;
    let f = Fixture::new(Engine::V2).await;
    let observer = f.client().await;
    let mut writer = f.raw().await;
    observer
        .batch_execute("CREATE TABLE t (id int PRIMARY KEY)")
        .await
        .unwrap();
    let path = format!("{},public", vec!["s".repeat(63); 80].join(","));
    let status = query_frames(&mut writer, &format!("SET search_path TO {path}")).await;
    assert_eq!(tags(&status), b"SCZ");

    let values = ids(256);
    let prefix = if copy {
        "COPY t FROM STDIN; "
    } else {
        "BEGIN; "
    };
    let sql = format!(
        "{prefix}SET LOCAL search_path TO public; \
         INSERT INTO t (id) VALUES {values} RETURNING repeat('x',{width}); COMMIT"
    );
    let frames = if copy {
        copy_from_stdin(&mut writer, &sql, b"0\n").await
    } else {
        query_frames(&mut writer, &sql).await
    };
    let written = count(&observer).await;
    drop((writer, observer));
    f.stop().await;

    let error = frames.iter().find(|(tag, _)| *tag == b'E');
    assert_eq!(
        error.map(|(_, body)| String::from_utf8_lossy(body)),
        None,
        "COMMIT reply failed with {written} rows visible"
    );
    assert_eq!(frames.iter().filter(|(tag, _)| *tag == b'D').count(), 256);
    assert_eq!(
        &frames[frames.len() - 3..],
        &[
            status[0].clone(),
            (b'C', b"COMMIT\0".to_vec()),
            (b'Z', b"I".to_vec())
        ],
    );
    assert_eq!(written, if copy { 257 } else { 256 });
    if width == 32750 {
        let through_commit: usize = frames[..frames.len() - 1]
            .iter()
            .map(|(_, body)| 5 + body.len())
            .sum();
        assert!(through_commit > giggles_server::MAX_RESPONSE, "{through_commit}");
    }
}

#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn returning_reply_w6_copy_from_explicit_commit_near_limit() {
    explicit_commit_near_reply_limit(true, 32750).await;
}

#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn returning_reply_w6_simple_batch_explicit_commit_near_limit() {
    explicit_commit_near_reply_limit(false, 32750).await;
}

#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn returning_reply_w6_explicit_commit_restores_status_with_room_to_spare() {
    explicit_commit_near_reply_limit(false, 16).await;
}

#[cfg(feature = "engine-rocksdb")]
async fn commit_with_followup(copy: bool, show: bool) {
    let _serial = crate::api_result_stream::SERIAL.lock().await;
    let f = Fixture::new(Engine::V2).await;
    let observer = f.client().await;
    let mut writer = f.raw().await;
    observer
        .batch_execute("CREATE TABLE t (id int PRIMARY KEY)")
        .await
        .unwrap();
    let path = format!("{},public", vec!["s".repeat(63); 80].join(","));
    let status = query_frames(&mut writer, &format!("SET search_path TO {path}")).await;
    assert_eq!(tags(&status), b"SCZ");
    let restored = &status[0].1[b"search_path\0".len()..status[0].1.len() - 1];
    let prefix = if copy { "COPY t FROM STDIN" } else { "BEGIN" };
    let followup = if show {
        "SHOW search_path"
    } else {
        "INSERT INTO t VALUES (258) RETURNING repeat('y',4000)"
    };
    let sql = format!(
        "{prefix}; SET LOCAL search_path TO public; \
         INSERT INTO t VALUES {} RETURNING repeat('x',32735); COMMIT; {followup}",
        ids(256)
    );
    let frames = if copy {
        copy_from_stdin(&mut writer, &sql, b"0\n").await
    } else {
        query_frames(&mut writer, &sql).await
    };
    let written = count(&observer).await;
    drop((writer, observer));
    f.stop().await;

    let commit = frames
        .iter()
        .position(|(tag, body)| *tag == b'C' && body == b"COMMIT\0")
        .expect("missing publishing COMMIT");
    assert_eq!(frames[commit - 1], status[0]);
    let through_commit: usize = frames[..=commit].iter().map(|(_, body)| 5 + body.len()).sum();
    assert!(through_commit <= giggles_server::MAX_RESPONSE, "{through_commit}");
    let tail = &frames[commit + 1..];
    assert_eq!(tags(tail), b"TDCZ", "after COMMIT with {written} visible rows");
    assert_eq!(tail[3].1, b"I");
    if show {
        assert_eq!(&tail[1].1[6..], restored);
        assert_eq!(tail[2].1, b"SHOW\0");
    } else {
        assert_eq!(&tail[1].1[6..], b"y".repeat(4000));
        assert_eq!(tail[2].1, b"INSERT 0 1\0");
    }
    assert_eq!(written, 256 + i64::from(copy) + i64::from(!show));
}

#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn returning_reply_w7_simple_commit_frees_show_budget() {
    commit_with_followup(false, true).await;
}

#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn returning_reply_w7_copy_done_commit_frees_show_budget() {
    commit_with_followup(true, true).await;
}

#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn returning_reply_w7_simple_commit_frees_insert_budget() {
    commit_with_followup(false, false).await;
}

#[cfg(feature = "engine-rocksdb")]
enum FailedCommit {
    Implicit,
    Explicit,
    CopyDone,
    Sync,
}

#[cfg(feature = "engine-rocksdb")]
async fn failed_commit_restores_search_path(mode: FailedCommit) {
    let f = Fixture::new(Engine::V2).await;
    let observer = f.client().await;
    let mut writer = f.raw().await;
    observer
        .batch_execute(
            "CREATE TABLE parent (id int PRIMARY KEY); \
             CREATE TABLE t (id int PRIMARY KEY, pid int REFERENCES parent \
             DEFERRABLE INITIALLY DEFERRED)",
        )
        .await
        .unwrap();
    let initial = query_frames(&mut writer, "SET search_path TO missing, public").await;
    assert_eq!(tags(&initial), b"SCZ");
    let restored = (b'S', b"search_path\0missing, public\0".to_vec());
    assert_eq!(initial[0], restored);
    let local = (b'S', b"search_path\0public\0".to_vec());
    let set_local = "SET LOCAL search_path TO public";
    let insert = "INSERT INTO t VALUES (1, 99)";
    let frames = match mode {
        FailedCommit::Implicit => {
            query_frames(&mut writer, &format!("{set_local}; {insert}")).await
        }
        FailedCommit::Explicit => {
            let before = query_frames(&mut writer, &format!("BEGIN; {set_local}; {insert}")).await;
            assert_eq!(tags(&before), b"CSCCZ");
            assert_eq!(before[1], local);
            query_frames(&mut writer, "COMMIT").await
        }
        FailedCommit::CopyDone => {
            copy_from_stdin(&mut writer, "COPY t FROM STDIN; SET LOCAL search_path TO public", b"1\t99\n").await
        }
        FailedCommit::Sync => {
            for sql in ["SELECT set_config('search_path', 'public', true)", insert] {
                pg_send(&mut writer, b'P', &parse("", sql)).await;
                pg_send(&mut writer, b'B', &bind("", "", &[], false)).await;
                pg_send(&mut writer, b'E', &execute("", 0)).await;
            }
            pg_sync(&mut writer).await
        }
    };
    let show = query_frames(&mut writer, "SHOW search_path").await;
    let written = count(&observer).await;
    drop((writer, observer));
    f.stop().await;

    assert_eq!(written, 0);
    assert_eq!(tags(&show), b"TDCZ");
    assert_eq!(&show[1].1[6..], b"missing, public");
    assert!(frames.len() >= 3, "commit omitted restored status: {frames:?}");
    let tail = &frames[frames.len() - 3..];
    assert_eq!(tags(tail), b"SEZ", "commit omitted restored status: {frames:?}");
    assert_eq!(tail[0], restored);
    assert!(tail[1].1.windows(7).any(|field| field == b"C23503\0"));
    assert_eq!(tail[2].1, b"I");
    if !matches!(mode, FailedCommit::Explicit) {
        assert!(frames[..frames.len() - 3].contains(&local), "{frames:?}");
    }
}

#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn returning_reply_w7_implicit_commit_failure_restores_search_path() {
    failed_commit_restores_search_path(FailedCommit::Implicit).await;
}

#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn returning_reply_w7_explicit_commit_failure_restores_search_path() {
    failed_commit_restores_search_path(FailedCommit::Explicit).await;
}

#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn returning_reply_w7_copy_done_commit_failure_restores_search_path() {
    failed_commit_restores_search_path(FailedCommit::CopyDone).await;
}

#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn returning_reply_w7_sync_commit_failure_restores_search_path() {
    failed_commit_restores_search_path(FailedCommit::Sync).await;
}

#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn returning_reply_w6_explicit_commit_sends_role_reset_notice_near_limit() {
    let _serial = crate::api_result_stream::SERIAL.lock().await;
    let f = Fixture::new(Engine::V2).await;
    let observer = f.client().await;
    let mut writer = f.raw().await;
    observer
        .batch_execute("CREATE TABLE t (id int PRIMARY KEY)")
        .await
        .unwrap();
    observer.batch_execute("CREATE ROLE retired").await.unwrap();
    for sql in ["SET ROLE retired", "BEGIN", "SET LOCAL ROLE NONE"] {
        let frames = query_frames(&mut writer, sql).await;
        assert!(!tags(&frames).contains(&b'E'), "{sql}: {frames:?}");
    }
    observer.batch_execute("DROP ROLE retired").await.unwrap();
    let values = ids(256);
    let frames = query_frames(
        &mut writer,
        &format!("INSERT INTO t (id) VALUES {values} RETURNING repeat('x', CASE WHEN id = 256 THEN 32922 ELSE 32752 END); COMMIT"),
    )
    .await;
    let written = count(&observer).await;
    drop((writer, observer));
    f.stop().await;

    let error = frames.iter().find(|(tag, _)| *tag == b'E');
    assert_eq!(
        error.map(|(_, body)| String::from_utf8_lossy(body)),
        None,
        "COMMIT reply failed with {written} rows visible"
    );
    assert_eq!(frames.iter().filter(|(tag, _)| *tag == b'D').count(), 256);
    let tail = &frames[frames.len() - 3..];
    assert_eq!(tags(tail), b"CNZ");
    assert_eq!(tail[0].1, b"COMMIT\0");
    let notice = b"role \"retired\" can no longer be set; role reset\0";
    assert!(tail[1].1.windows(notice.len()).any(|field| field == notice));
    assert_eq!(tail[2].1, b"I");
    assert_eq!(written, 256);
}

#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn returning_reply_w6_standalone_copy_commit_failure_has_no_completion() {
    let f = Fixture::new(Engine::V2).await;
    let observer = f.client().await;
    let mut writer = f.raw().await;
    observer
        .batch_execute(
            "CREATE TABLE parent (id int PRIMARY KEY); \
             CREATE TABLE t (id int PRIMARY KEY, pid int REFERENCES parent \
             DEFERRABLE INITIALLY DEFERRED)",
        )
        .await
        .unwrap();
    let frames = copy_from_stdin(&mut writer, "COPY t FROM STDIN", b"2\t99\n").await;
    let written = count(&observer).await;
    drop((writer, observer));
    f.stop().await;

    assert_eq!(written, 0);
    assert_eq!(tags(&frames), b"EZ");
    assert!(frames[0].1.windows(7).any(|field| field == b"C23503\0"));
    assert_eq!(frames[1].1, b"I");
}

#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn returning_reply_w4_copy_to_file_rolls_back_encoding_error() {
    let f = Fixture::new(Engine::V2).await;
    let observer = f.client().await;
    setup(&observer).await;
    let mut writer = f.raw().await;
    let path = f.path.join("copy-output");
    let filename = path
        .to_str()
        .unwrap()
        .replace('\\', "/")
        .replace('\'', "''");
    let copy =
        |id| format!("COPY (INSERT INTO t VALUES ({id}, {BAD_TS}) RETURNING ts) TO '{filename}'");
    assert_eq!(
        tags(&query_frames(&mut writer, "SET TIME ZONE 1").await),
        b"SCZ"
    );
    let failed = query_frames(&mut writer, &copy(1)).await;
    assert_eq!(tags(&failed), b"EZ", "{failed:?}");
    assert!(failed[0].1.windows(7).any(|field| field == b"C22008\0"));
    assert_eq!(failed[1].1, b"I");
    assert_eq!(count(&observer).await, 0, "failed COPY published its row");

    let failed = extended(&mut writer, &copy(1), 0, false).await;
    assert_eq!(tags(&failed), b"12EZ", "{failed:?}");
    assert!(failed[2].1.windows(7).any(|field| field == b"C22008\0"));
    assert_eq!(failed[3].1, b"I");
    assert_eq!(count(&observer).await, 0);

    assert_eq!(
        tags(&query_frames(&mut writer, "SET TIME ZONE 0").await),
        b"SCZ"
    );
    let success = query_frames(&mut writer, &copy(1)).await;
    assert_eq!(tags(&success), b"CZ", "{success:?}");
    assert_eq!(success[0].1, b"COPY 1\0");
    assert!(
        std::fs::read(&path)
            .unwrap()
            .starts_with(b"294276-12-31 23:59:59")
    );
    assert_eq!(count(&observer).await, 1);

    pg_send(&mut writer, b'P', &parse("", &copy(2))).await;
    pg_send(&mut writer, b'B', &bind("", "", &[], false)).await;
    pg_send(&mut writer, b'E', &execute("", 0)).await;
    pg_send(&mut writer, b'H', &[]).await;
    assert_eq!(pg_read(&mut writer).await.0, b'1');
    assert_eq!(pg_read(&mut writer).await.0, b'2');
    assert_eq!(pg_read(&mut writer).await, (b'C', b"COPY 1\0".to_vec()));
    assert_eq!(count(&observer).await, 1, "Execute published before Sync");
    assert_eq!(pg_sync(&mut writer).await, [(b'Z', b"I".to_vec())]);
    assert_eq!(count(&observer).await, 2);
    drop((writer, observer));
    f.stop().await;
}

#[cfg(all(feature = "engine-rocksdb", any(unix, windows)))]
#[tokio::test]
async fn returning_reply_w4_copy_to_program_rolls_back_failed_exit() {
    let f = Fixture::new(Engine::V2).await;
    let observer = f.client().await;
    setup(&observer).await;
    let mut writer = f.raw().await;
    #[cfg(unix)]
    let command = "exit 1";
    #[cfg(windows)]
    let command = "exit /b 1";
    let failed = query_frames(
        &mut writer,
        &format!("COPY (INSERT INTO t (id) VALUES (1) RETURNING id) TO PROGRAM '{command}'"),
    )
    .await;
    assert_eq!(tags(&failed), b"EZ", "{failed:?}");
    assert!(failed[0].1.windows(7).any(|field| field == b"C38000\0"));
    assert_eq!(failed[1].1, b"I");
    assert_eq!(count(&observer).await, 0, "failed COPY published its row");
    drop((writer, observer));
    f.stop().await;
}

#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn returning_reply_w4_copy_to_read_only_allows_nonwriting_row_lock() {
    // Pin base behavior here. PostgreSQL rejects this row lock with 25006.
    let f = Fixture::new(Engine::V2).await;
    let observer = f.client().await;
    setup(&observer).await;
    observer
        .batch_execute("INSERT INTO t (id) VALUES (1)")
        .await
        .unwrap();
    let mut writer = f.raw().await;
    let begin = query_frames(&mut writer, "BEGIN READ ONLY").await;
    assert_eq!(tags(&begin), b"CZ", "{begin:?}");
    assert_eq!(begin[1].1, b"T");
    let read = query_frames(&mut writer, "COPY (SELECT id FROM t FOR UPDATE) TO STDOUT").await;
    assert_eq!(tags(&read), b"HdcCZ", "{read:?}");
    assert_eq!(read[1].1, b"1\n");
    assert_eq!(read[4].1, b"T");
    let write = query_frames(
        &mut writer,
        "COPY (INSERT INTO t (id) VALUES (2) RETURNING id) TO STDOUT",
    )
    .await;
    assert_eq!(tags(&write), b"EZ", "{write:?}");
    assert!(write[0].1.windows(7).any(|field| field == b"C25006\0"));
    assert_eq!(write[1].1, b"E");
    assert_eq!(tags(&query_frames(&mut writer, "ROLLBACK").await), b"CZ");
    assert_eq!(count(&observer).await, 1);
    drop((writer, observer));
    f.stop().await;
}

#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn returning_reply_w4_copy_to_applies_v2_replica_role() {
    let f = Fixture::new(Engine::V2).await;
    let observer = f.client().await;
    observer
        .batch_execute(
            "CREATE TABLE p (id int PRIMARY KEY);
         CREATE TABLE t (id int PRIMARY KEY REFERENCES p(id))",
        )
        .await
        .unwrap();
    let mut writer = f.raw().await;
    let copy = "COPY (INSERT INTO t VALUES (1) RETURNING id) TO STDOUT";
    let origin = query_frames(&mut writer, copy).await;
    assert_eq!(tags(&origin), b"EZ", "{origin:?}");
    assert!(origin[0].1.windows(7).any(|field| field == b"C23503\0"));
    assert_eq!(count(&observer).await, 0);

    let set = query_frames(&mut writer, "SET session_replication_role = replica").await;
    assert_eq!(tags(&set), b"CZ", "{set:?}");
    let replica = query_frames(&mut writer, copy).await;
    assert_eq!(tags(&replica), b"HdcCZ", "{replica:?}");
    assert_eq!(replica[1].1, b"1\n");
    assert_eq!(replica[4].1, b"I");
    assert_eq!(count(&observer).await, 1);
    drop((writer, observer));
    f.stop().await;
}

#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn returning_reply_w4_copy_to_replica_spill_rolls_back_late_encoding_error() {
    let _serial = crate::api_result_stream::SERIAL.lock().await;
    let f = Fixture::new(Engine::V2).await;
    let observer = f.client().await;
    setup(&observer).await;
    let mut writer = f.raw().await;
    let set = query_frames(
        &mut writer,
        "SET session_replication_role = replica; SET TIME ZONE 1",
    )
    .await;
    assert!(!tags(&set).contains(&b'E'), "{set:?}");
    let values = (1..=600)
        .map(|id| format!("({id}, {})", if id == 600 { BAD_TS } else { OK_TS }))
        .collect::<Vec<_>>()
        .join(",");
    let copy = format!(
        "COPY (INSERT INTO t (id, ts) VALUES {values} RETURNING ts, repeat('r', 2000)) TO STDOUT"
    );
    let frames = query_frames(&mut writer, &copy).await;
    assert_eq!(frames.first().unwrap().0, b'H');
    let (prefix, suffix) = frames.split_at(frames.len() - 2);
    assert!(prefix.len() > 1, "expected provisional CopyData");
    assert!(prefix[1..].iter().all(|(tag, _)| *tag == b'd'));
    assert_eq!(tags(suffix), b"EZ");
    assert!(suffix[0].1.windows(7).any(|field| field == b"C22008\0"));
    assert_eq!(suffix[1].1, b"I");
    assert_eq!(count(&observer).await, 0);
    drop((writer, observer));
    f.stop().await;
}
