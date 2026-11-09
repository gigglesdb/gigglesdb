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
use tokio_postgres::{Client, Config, NoTls, config::SslMode};
static SERIAL: AtomicU64 = AtomicU64::new(0);

struct Fixture {
    service: Arc<Service>,
    pg: Running,
    path: std::path::PathBuf,
}

#[tokio::test]
async fn psycopg_sequence_preserves_field_types_and_exhaustion() {
    let f = Fixture::new().await;
    let c = f.client().await;
    c.batch_execute("CREATE TABLE items(id integer PRIMARY KEY, name text, score bigint); INSERT INTO items VALUES (2,'b',NULL); BEGIN").await.unwrap();
    c.execute(
        "DECLARE \"probe_point\" CURSOR FOR SELECT id,name,score FROM items WHERE id=$1",
        &[&2i32],
    )
    .await
    .unwrap();
    let row = c
        .query_one("FETCH FORWARD 1 FROM \"probe_point\"", &[])
        .await
        .unwrap();
    assert_eq!(row.get::<_, i32>(0), 2);
    assert_eq!(row.get::<_, String>(1), "b");
    assert_eq!(row.get::<_, Option<i64>>(2), None);
    assert_eq!(
        row.columns()
            .iter()
            .map(|c| c.type_().oid())
            .collect::<Vec<_>>(),
        vec![23, 25, 20]
    );
    assert!(
        c.query("FETCH FORWARD 1 FROM \"probe_point\"", &[])
            .await
            .unwrap()
            .is_empty()
    );
    c.batch_execute("CLOSE \"probe_point\"; COMMIT")
        .await
        .unwrap();
    drop(c);
    f.stop().await;
}
#[tokio::test]
async fn simple_fetch_move_and_close_tags() {
    let f = Fixture::new().await;
    let c = f.client().await;
    c.batch_execute("CREATE TABLE items(id integer PRIMARY KEY); INSERT INTO items VALUES (1),(2),(3); BEGIN; DECLARE \"Odd cursor\" NO SCROLL CURSOR WITHOUT HOLD FOR SELECT id FROM items ORDER BY id").await.unwrap();
    let rows = c
        .simple_query("FETCH NEXT FROM \"Odd cursor\"")
        .await
        .unwrap();
    assert!(
        matches!(&rows[0], tokio_postgres::SimpleQueryMessage::RowDescription(d) if d[0].name() == "id")
    );
    assert!(
        matches!(&rows[1], tokio_postgres::SimpleQueryMessage::Row(r) if r.get(0) == Some("1"))
    );
    assert!(matches!(
        &rows[2],
        tokio_postgres::SimpleQueryMessage::CommandComplete(1)
    ));
    assert_eq!(
        c.execute("MOVE FORWARD 1 FROM \"Odd cursor\"", &[])
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        c.query_one("FETCH 1 IN \"Odd cursor\"", &[])
            .await
            .unwrap()
            .get::<_, i32>(0),
        3
    );
    let empty = c
        .simple_query("FETCH ALL FROM \"Odd cursor\"")
        .await
        .unwrap();
    assert!(
        matches!(&empty[0], tokio_postgres::SimpleQueryMessage::RowDescription(d) if d.len() == 1)
    );
    assert!(matches!(
        &empty[1],
        tokio_postgres::SimpleQueryMessage::CommandComplete(0)
    ));
    c.batch_execute("CLOSE ALL; COMMIT").await.unwrap();
    assert_eq!(
        c.simple_query("FETCH FROM \"Odd cursor\"")
            .await
            .unwrap_err()
            .code()
            .unwrap()
            .code(),
        "34000"
    );
    drop(c);
    f.stop().await;
}
#[tokio::test]
async fn cursor_errors_match_postgres() {
    let f = Fixture::new().await;
    let c = f.client().await;
    for (sql, code) in [
        ("FETCH NEXT FROM missing", "34000"),
        ("DECLARE c CURSOR FOR SELECT 1", "25P01"),
        ("CLOSE missing", "34000"),
    ] {
        assert_eq!(
            c.simple_query(sql)
                .await
                .unwrap_err()
                .code()
                .unwrap()
                .code(),
            code
        );
    }
    c.batch_execute("BEGIN; DECLARE c CURSOR FOR SELECT 1")
        .await
        .unwrap();
    assert_eq!(
        c.execute("DECLARE c CURSOR FOR SELECT 2", &[])
            .await
            .unwrap_err()
            .code()
            .unwrap()
            .code(),
        "42P03"
    );
    assert_eq!(
        c.simple_query("FETCH FROM c")
            .await
            .unwrap_err()
            .code()
            .unwrap()
            .code(),
        "25P02"
    );
    c.batch_execute("ROLLBACK").await.unwrap();
    assert_eq!(
        c.simple_query("FETCH FROM c")
            .await
            .unwrap_err()
            .code()
            .unwrap()
            .code(),
        "34000"
    );
    drop(c);
    f.stop().await;
}
#[tokio::test]
async fn transaction_end_and_hold_lifetimes() {
    let f = Fixture::new().await;
    let c = f.client().await;
    c.batch_execute("BEGIN; DECLARE ordinary CURSOR FOR SELECT 1; DECLARE held CURSOR WITH HOLD FOR SELECT 2; COMMIT").await.unwrap();
    assert_eq!(
        c.query_one("FETCH NEXT FROM held", &[])
            .await
            .unwrap()
            .get::<_, i32>(0),
        2
    );
    assert_eq!(
        c.simple_query("FETCH FROM ordinary")
            .await
            .unwrap_err()
            .code()
            .unwrap()
            .code(),
        "34000"
    );
    c.batch_execute("BEGIN; DECLARE doomed CURSOR WITH HOLD FOR SELECT 3; ROLLBACK")
        .await
        .unwrap();
    assert_eq!(
        c.simple_query("FETCH FROM doomed")
            .await
            .unwrap_err()
            .code()
            .unwrap()
            .code(),
        "34000"
    );
    assert!(
        c.query("FETCH ALL FROM held", &[])
            .await
            .unwrap()
            .is_empty()
    );
    c.batch_execute("DECLARE automatic CURSOR WITH HOLD FOR SELECT 4")
        .await
        .unwrap();
    assert_eq!(
        c.query_one("FETCH FROM automatic", &[])
            .await
            .unwrap()
            .get::<_, i32>(0),
        4
    );
    c.batch_execute("CLOSE ALL").await.unwrap();
    drop(c);
    f.stop().await;
}
#[tokio::test]
async fn materialized_rows_are_isolated_per_session() {
    let f = Fixture::new().await;
    let c = f.client().await;
    let other = f.client().await;
    c.batch_execute("CREATE TABLE items(id integer PRIMARY KEY); INSERT INTO items VALUES(1); BEGIN; DECLARE c CURSOR FOR SELECT id FROM items; DELETE FROM items").await.unwrap();
    assert_eq!(
        other
            .simple_query("FETCH FROM c")
            .await
            .unwrap_err()
            .code()
            .unwrap()
            .code(),
        "34000"
    );
    assert_eq!(
        c.query_one("FETCH FROM c", &[])
            .await
            .unwrap()
            .get::<_, i32>(0),
        1
    );
    c.batch_execute("COMMIT").await.unwrap();
    assert_eq!(
        c.simple_query("FETCH FROM c")
            .await
            .unwrap_err()
            .code()
            .unwrap()
            .code(),
        "34000"
    );
    drop(other);
    drop(c);
    f.stop().await;
}
impl Fixture {
    async fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "sql-cursors-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            SERIAL.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).unwrap();
        let admin = Admin::create(path.join("db.redb"), Limits::default()).unwrap();
        admin
            .roles()
            .create(NewRole {
                name: "owner".into(),
                superuser: true,
                login: true,
                ..NewRole::default()
            })
            .unwrap();
        admin
            .roles()
            .create(NewRole {
                name: "ordinary".into(),
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
    async fn client(&self) -> Client {
        let (client, connection) = Config::new()
            .host("localhost")
            .hostaddr(self.pg.address.ip())
            .port(self.pg.address.port())
            .user("owner")
            .dbname("postgres")
            .ssl_mode(SslMode::Disable)
            .connect(NoTls)
            .await
            .unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
    }
    async fn stop(self) {
        self.pg.shutdown().await.unwrap();
        drop(self.service);
        // Listener shutdown waits for active connections to release the file.
        std::fs::remove_dir_all(self.path).unwrap();
    }
}

async fn cycle(stream: &mut TcpStream) -> Vec<(u8, Vec<u8>)> {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        let mut messages = Vec::new();
        loop {
            let tag = stream.read_u8().await.unwrap();
            let len = stream.read_u32().await.unwrap();
            assert!((4..65536).contains(&len));
            let mut body = vec![0; len as usize - 4];
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
async fn query(stream: &mut TcpStream, sql: &str, extended: bool) -> Vec<(u8, Vec<u8>)> {
    use giggles_server::pgwire::{frame, z};
    let mut packet = Vec::new();
    if extended {
        let mut parse = vec![0];
        z(sql, &mut parse);
        parse.extend(0u16.to_be_bytes());
        frame(b'P', &parse, &mut packet).unwrap();
        frame(b'B', &[0, 0, 0, 0, 0, 0, 0, 0], &mut packet).unwrap();
        frame(b'E', &[0, 0, 0, 0, 0], &mut packet).unwrap();
        frame(b'S', &[], &mut packet).unwrap();
    } else {
        let mut body = Vec::new();
        z(sql, &mut body);
        frame(b'Q', &body, &mut packet).unwrap();
    }
    stream.write_all(&packet).await.unwrap();
    cycle(stream).await
}
fn status(messages: &[(u8, Vec<u8>)]) -> u8 {
    messages.last().unwrap().1[0]
}
fn error(messages: &[(u8, Vec<u8>)]) -> &str {
    let body = &messages.iter().find(|m| m.0 == b'E').unwrap().1;
    let at = body.iter().position(|b| *b == b'C').unwrap() + 1;
    std::str::from_utf8(&body[at..at + 5]).unwrap()
}
async fn wire(f: &Fixture) -> TcpStream {
    wire_as(f, "owner").await
}
async fn wire_as(f: &Fixture, user: &str) -> TcpStream {
    let mut stream = TcpStream::connect(f.pg.address).await.unwrap();
    let mut body = 196608u32.to_be_bytes().to_vec();
    body.extend(b"user\0");
    body.extend(user.as_bytes());
    body.extend(b"\0database\0postgres\0\0");
    stream.write_u32((body.len() + 4) as u32).await.unwrap();
    stream.write_all(&body).await.unwrap();
    assert_eq!(status(&cycle(&mut stream).await), b'I');
    stream
}

#[tokio::test]
async fn describe_and_close_sql_cursor_over_wire() {
    use giggles_server::pgwire::{frame, z};
    use tokio::io::AsyncWriteExt;
    let f = Fixture::new().await;
    let mut stream = wire(&f).await;
    query(
        &mut stream,
        "BEGIN; DECLARE c NO SCROLL CURSOR FOR SELECT 1 AS id",
        false,
    )
    .await;
    let mut packet = Vec::new();
    let mut describe = vec![b'P'];
    z("c", &mut describe);
    frame(b'D', &describe, &mut packet).unwrap();
    frame(b'S', &[], &mut packet).unwrap();
    stream.write_all(&packet).await.unwrap();
    let description = cycle(&mut stream).await;
    assert_eq!(description[0].0, b'T');
    let fetched = query(&mut stream, "FETCH 0 FROM c", false).await;
    assert_eq!(description[0], fetched[0]);
    packet.clear();
    frame(b'C', &describe, &mut packet).unwrap();
    frame(b'S', &[], &mut packet).unwrap();
    stream.write_all(&packet).await.unwrap();
    assert_eq!(cycle(&mut stream).await[0].0, b'3');
    assert_eq!(
        error(&query(&mut stream, "FETCH FROM c", false).await),
        "34000"
    );
    query(&mut stream, "ROLLBACK", false).await;
    drop(stream);
    f.stop().await;
}
