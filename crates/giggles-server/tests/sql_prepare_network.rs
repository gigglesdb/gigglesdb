//! SQL PREPARE / EXECUTE / DEALLOCATE and DISCARD on the real pgwire listener.
use giggles_core::{Admin, Limits, roles::NewRole};
use giggles_server::{
    Service,
    auth::{PostgresqlConfig, TlsMode, serve_postgresql},
    clock::Clock,
};
use std::{collections::BTreeMap, path::PathBuf, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

struct Fixture {
    running: giggles_server::Running,
    path: PathBuf,
}
impl Fixture {
    async fn new(label: &str) -> Self {
        let path = std::env::temp_dir().join(format!("sql-prepare-{}-{label}", std::process::id()));
        std::fs::create_dir_all(&path).unwrap();
        #[cfg(feature = "engine-rocksdb")]
        let admin =
            Admin::create_engine(path.join("db"), Limits::default(), giggles_core::Engine::V2)
                .unwrap();
        #[cfg(not(feature = "engine-rocksdb"))]
        let admin = Admin::create(path.join("db.redb"), Limits::default()).unwrap();
        for (name, superuser) in [("owner", true), ("someone", false)] {
            admin
                .roles()
                .create(NewRole {
                    name: name.into(),
                    login: true,
                    superuser,
                    ..NewRole::default()
                })
                .unwrap();
        }
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
        let running = serve_postgresql(config.listen, service, Arc::new(config.build().unwrap()))
            .await
            .unwrap();
        Self { running, path }
    }

    async fn connect(&self) -> TcpStream {
        let mut connection = TcpStream::connect(self.running.address).await.unwrap();
        let mut startup = 196608u32.to_be_bytes().to_vec();
        startup.extend(b"user\0owner\0database\0postgres\0\0");
        connection
            .write_u32((startup.len() + 4) as u32)
            .await
            .unwrap();
        connection.write_all(&startup).await.unwrap();
        receive(&mut connection).await.success();
        connection
    }

    async fn shutdown(self) {
        self.running.shutdown().await.unwrap();
        std::fs::remove_dir_all(self.path).unwrap();
    }
}

#[derive(Debug, Default)]
struct Response(Vec<(u8, Vec<u8>)>);
impl Response {
    fn error(&self) -> Option<&[u8]> {
        self.0
            .iter()
            .find(|(tag, _)| *tag == b'E')
            .map(|(_, body)| body.as_slice())
    }

    fn success(&self) {
        assert!(self.error().is_none(), "{self:?}");
    }

    fn field(&self, field: u8) -> Option<&str> {
        let mut error = self.error()?;
        while let Some((&tag, tail)) = error.split_first() {
            if tag == 0 {
                return None;
            }
            let end = tail.iter().position(|&byte| byte == 0).unwrap();
            if tag == field {
                return Some(std::str::from_utf8(&tail[..end]).unwrap());
            }
            error = &tail[end + 1..];
        }
        None
    }

    fn assert_error(&self, code: &str, message: &str) {
        assert_eq!(self.field(b'C'), Some(code), "{self:?}");
        assert_eq!(self.field(b'M'), Some(message), "{self:?}");
    }

    fn tags(&self, expected: &[&str]) {
        self.success();
        let actual: Vec<_> = self
            .0
            .iter()
            .filter(|(tag, _)| *tag == b'C')
            .map(|(_, body)| std::str::from_utf8(body.strip_suffix(&[0]).unwrap()).unwrap())
            .collect();
        assert_eq!(actual, expected);
    }

    fn rows(&self) -> Vec<Vec<Option<Vec<u8>>>> {
        self.success();
        self.0
            .iter()
            .filter(|(tag, _)| *tag == b'D')
            .map(|(_, body)| {
                let mut tail = &body[2..];
                let columns = u16::from_be_bytes(body[..2].try_into().unwrap());
                (0..columns)
                    .map(|_| {
                        let len = i32::from_be_bytes(tail[..4].try_into().unwrap());
                        tail = &tail[4..];
                        if len == -1 {
                            None
                        } else {
                            let (value, rest) = tail.split_at(len as usize);
                            tail = rest;
                            Some(value.to_vec())
                        }
                    })
                    .collect()
            })
            .collect()
    }

    fn text_rows(&self, expected: &[&[Option<&str>]]) {
        let rows = self.rows();
        let actual: Vec<Vec<Option<&str>>> = rows
            .iter()
            .map(|row| {
                row.iter()
                    .map(|field| field.as_deref().map(|v| std::str::from_utf8(v).unwrap()))
                    .collect()
            })
            .collect();
        assert_eq!(actual, expected, "{self:?}");
    }

    fn first_column_type(&self) -> u32 {
        let body = &self.0.iter().find(|(tag, _)| *tag == b'T').unwrap().1;
        let name_end = body[2..].iter().position(|&byte| byte == 0).unwrap() + 3;
        u32::from_be_bytes(body[name_end + 6..name_end + 10].try_into().unwrap())
    }
}

async fn send(stream: &mut TcpStream, tag: u8, body: &[u8]) {
    stream.write_u8(tag).await.unwrap();
    stream.write_u32((body.len() + 4) as u32).await.unwrap();
    stream.write_all(body).await.unwrap();
}

async fn receive(stream: &mut TcpStream) -> Response {
    receive_until(stream, b'Z').await
}

async fn receive_until(stream: &mut TcpStream, last_tag: u8) -> Response {
    tokio::time::timeout(Duration::from_secs(15), async {
        let mut response = Response::default();
        loop {
            let tag = stream.read_u8().await.unwrap();
            let len = stream.read_u32().await.unwrap() as usize;
            let mut body = vec![0; len - 4];
            stream.read_exact(&mut body).await.unwrap();
            response.0.push((tag, body));
            if tag == last_tag {
                return response;
            }
        }
    })
    .await
    .unwrap()
}

async fn simple(stream: &mut TcpStream, sql: &str) -> Response {
    send(stream, b'Q', format!("{sql}\0").as_bytes()).await;
    receive(stream).await
}

async fn parse(stream: &mut TcpStream, name: &str, sql: &str) {
    send(stream, b'P', format!("{name}\0{sql}\0\0\0").as_bytes()).await;
}

async fn sync(stream: &mut TcpStream) -> Response {
    send(stream, b'S', &[]).await;
    receive(stream).await
}

async fn extended(stream: &mut TcpStream, sql: &str, inputs: &[&str], binary: bool) -> Response {
    parse(stream, "", sql).await;
    bind_execute(stream, "", inputs, binary).await
}

async fn bind(stream: &mut TcpStream, name: &str, inputs: &[&str], result_formats: &[u16]) {
    bind_portal(stream, "", name, inputs, result_formats).await;
}

async fn bind_portal(
    stream: &mut TcpStream, portal: &str, name: &str, inputs: &[&str], result_formats: &[u16],
) {
    let mut bind = format!("{portal}\0{name}\0\0\0").into_bytes(); // default input formats
    bind.extend((inputs.len() as u16).to_be_bytes());
    for input in inputs {
        bind.extend((input.len() as u32).to_be_bytes());
        bind.extend(input.as_bytes());
    }
    bind.extend((result_formats.len() as u16).to_be_bytes());
    for format in result_formats {
        bind.extend(format.to_be_bytes());
    }
    send(stream, b'B', &bind).await;
}

async fn bind_execute(
    stream: &mut TcpStream,
    name: &str,
    inputs: &[&str],
    binary: bool,
) -> Response {
    bind(stream, name, inputs, if binary { &[1] } else { &[] }).await;
    send(stream, b'D', b"P\0").await;
    send(stream, b'E', &[0; 5]).await;
    sync(stream).await
}

#[tokio::test]
async fn prepare_execute_and_transaction_lifetime() {
    let fixture = Fixture::new("statements").await;
    let mut pg = fixture.connect().await;
    simple(&mut pg, "CREATE TABLE t2 (id int PRIMARY KEY, label text)")
        .await
        .tags(&["CREATE TABLE"]);
    let insert = "PREPARE ins (int, text) AS INSERT INTO t2 VALUES ($1,$2) RETURNING id;";
    simple(&mut pg, insert).await.tags(&["PREPARE"]);
    for (sql, id) in [
        ("EXECUTE ins(1,'first')", "1"),
        ("EXECUTE ins(1+1,'second')", "2"),
        ("EXECUTE ins('3','third')", "3"),
    ] {
        let response = simple(&mut pg, sql).await;
        response.tags(&["INSERT 0 1"]);
        response.text_rows(&[&[Some(id)]]);
        assert_eq!(response.first_column_type(), 23);
    }
    simple(&mut pg, insert)
        .await
        .assert_error("42P05", "prepared statement \"ins\" already exists");
    for (sql, count) in [("EXECUTE ins(1)", 1), ("EXECUTE ins(1,'a','b')", 3)] {
        let response = simple(&mut pg, sql).await;
        response.assert_error(
            "42601",
            "wrong number of parameters for prepared statement \"ins\"",
        );
        assert_eq!(
            response.field(b'D'),
            Some(format!("Expected 2 parameters but got {count}.").as_str())
        );
    }
    simple(&mut pg, "EXECUTE ins('x','y')")
        .await
        .assert_error("22P02", "invalid input syntax for type integer: \"x\"");
    simple(&mut pg, "PREPARE sel AS SELECT label FROM t2 WHERE id = $1")
        .await
        .tags(&["PREPARE"]);
    simple(&mut pg, "EXECUTE sel(1+1)")
        .await
        .text_rows(&[&[Some("second")]]);
    simple(
        &mut pg,
        "PREPARE upd (text,int) AS UPDATE t2 SET label=$1 WHERE id=$2",
    )
    .await
    .tags(&["PREPARE"]);
    simple(&mut pg, "EXECUTE upd('changed',1)")
        .await
        .tags(&["UPDATE 1"]);
    simple(&mut pg, "EXECUTE sel(1)")
        .await
        .text_rows(&[&[Some("changed")]]);
    simple(&mut pg, "PREPARE del AS DELETE FROM t2 WHERE id = $1")
        .await
        .tags(&["PREPARE"]);
    simple(&mut pg, "EXECUTE del(2)").await.tags(&["DELETE 1"]);
    simple(&mut pg, "EXECUTE sel(2)").await.text_rows(&[]);
    simple(&mut pg, "PREPARE w AS VALUES (1),(2)")
        .await
        .tags(&["PREPARE"]);
    simple(&mut pg, "EXECUTE w")
        .await
        .text_rows(&[&[Some("1")], &[Some("2")]]);
    let explain = simple(&mut pg, "EXPLAIN EXECUTE w").await;
    assert!(
        matches!(explain.field(b'C'), Some("0A000" | "42601")),
        "{explain:?}"
    );
    let ctas = simple(&mut pg, "CREATE TABLE not_created AS EXECUTE w").await;
    assert!(
        matches!(ctas.field(b'C'), Some("0A000" | "42601")),
        "{ctas:?}"
    );
    assert_eq!(
        simple(&mut pg, "SELECT * FROM not_created")
            .await
            .field(b'C'),
        Some("42P01")
    );
    simple(&mut pg, "PREPARE p4 AS SELECT $2")
        .await
        .assert_error("42P18", "could not determine data type of parameter $1");
    simple(&mut pg, "PREPARE p5(int) AS SELECT $2")
        .await
        .tags(&["PREPARE"]);
    simple(
        &mut pg,
        "SELECT parameter_types FROM pg_prepared_statements WHERE name='p5'",
    )
    .await
    .text_rows(&[&[Some("{integer,text}")]]);
    simple(&mut pg, "DISCARD PLANS")
        .await
        .tags(&["DISCARD PLANS"]);
    simple(&mut pg, "EXECUTE p5(7, 'kept')")
        .await
        .text_rows(&[&[Some("kept")]]);
    simple(&mut pg, "PREPARE date_gap(date) AS SELECT $2")
        .await
        .tags(&["PREPARE"]);
    simple(&mut pg, "EXECUTE date_gap(DATE '2026-11-22', 'kept')")
        .await
        .text_rows(&[&[Some("kept")]]);
    bind_execute(&mut pg, "date_gap", &["2026-11-22", "kept"], false)
        .await
        .text_rows(&[&[Some("kept")]]);
    simple(&mut pg, "PREPARE gap(int) AS SELECT $3")
        .await
        .assert_error("42P18", "could not determine data type of parameter $2");
    simple(&mut pg, "PREPARE badtype (nosuchtype) AS SELECT 1")
        .await
        .assert_error("42704", "type \"nosuchtype\" does not exist");
    assert_eq!(
        simple(&mut pg, "PREPARE bad AS SELECT * FROM no_such_table")
            .await
            .field(b'C'),
        Some("42P01")
    );
    simple(&mut pg, "PREPARE c AS CREATE TABLE impossible(a int)")
        .await
        .assert_error("42601", "syntax error at or near \"CREATE\"");
    simple(
        &mut pg,
        "BEGIN; PREPARE \"Quoted Name\" AS SELECT 2; ROLLBACK",
    )
    .await
    .tags(&["BEGIN", "PREPARE", "ROLLBACK"]);
    simple(&mut pg, "EXECUTE \"Quoted Name\"")
        .await
        .text_rows(&[&[Some("2")]]);
    simple(
        &mut pg,
        "BEGIN; DEALLOCATE PREPARE \"Quoted Name\"; ROLLBACK",
    )
    .await
    .tags(&["BEGIN", "DEALLOCATE", "ROLLBACK"]);
    simple(&mut pg, "EXECUTE \"Quoted Name\"")
        .await
        .assert_error("26000", "prepared statement \"Quoted Name\" does not exist");
    simple(&mut pg, "DEALLOCATE nosuch")
        .await
        .assert_error("26000", "prepared statement \"nosuch\" does not exist");
    simple(&mut pg, "BEGIN").await.tags(&["BEGIN"]);
    simple(&mut pg, "EXECUTE nosuch")
        .await
        .assert_error("26000", "prepared statement \"nosuch\" does not exist");
    for sql in [
        "PREPARE blocked AS SELECT 1",
        "EXECUTE sel(1)",
        "DEALLOCATE sel",
        "DISCARD PLANS",
    ] {
        assert_eq!(
            simple(&mut pg, sql).await.field(b'C'),
            Some("25P02"),
            "{sql}"
        );
    }
    simple(&mut pg, "ROLLBACK").await.tags(&["ROLLBACK"]);
    drop(pg);
    fixture.shutdown().await;
}

#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn execute_rechecks_triggers_created_after_prepare() {
    let fixture = Fixture::new("trigger-recheck").await;
    let mut writer = fixture.connect().await;
    let mut owner = fixture.connect().await;
    simple(&mut owner, "CREATE TABLE trigger_target (id int PRIMARY KEY); CREATE FUNCTION trigger_fn() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$").await.success();
    simple(&mut writer, "PREPARE insert_target (int) AS INSERT INTO trigger_target VALUES ($1); EXECUTE insert_target(1)").await.tags(&["PREPARE", "INSERT 0 1"]);
    parse(&mut writer, "insert_protocol", "INSERT INTO trigger_target VALUES ($1)").await;
    sync(&mut writer).await.success();

    simple(&mut owner, "CREATE TRIGGER guard BEFORE INSERT ON trigger_target FOR EACH ROW EXECUTE FUNCTION trigger_fn()").await.tags(&["CREATE TRIGGER"]);
    let message = "INSERT on a table with triggers is not supported";
    bind_execute(&mut writer, "insert_protocol", &["2"], false).await.assert_error("0A000", message);
    simple(&mut writer, "EXECUTE insert_target(2)").await.assert_error("0A000", message);

    simple(&mut owner, "DROP TRIGGER guard ON trigger_target").await.tags(&["DROP TRIGGER"]);
    simple(&mut writer, "EXECUTE insert_target(2)").await.tags(&["INSERT 0 1"]);
    drop(writer);
    drop(owner);
    fixture.shutdown().await;
}

#[tokio::test]
async fn discard_resets_session_and_preserves_sequence_storage() {
    let fixture = Fixture::new("discard").await;
    let mut pg = fixture.connect().await;
    let mut other = fixture.connect().await;
    for (sql, tag) in [
        ("SET application_name TO 'changed'", "SET"),
        ("SET work_mem = '16MB'", "SET"),
        ("SET worker_c.probe = 'value'", "SET"),
        ("CREATE SEQUENCE sq", "CREATE SEQUENCE"),
        ("PREPARE a1 AS SELECT 42 AS answer", "PREPARE"),
        ("DISCARD PLANS", "DISCARD PLANS"),
        ("DISCARD TEMPORARY", "DISCARD TEMP"),
        ("BEGIN", "BEGIN"),
        (
            "DECLARE held CURSOR WITH HOLD FOR SELECT 7 AS value",
            "DECLARE CURSOR",
        ),
        ("COMMIT", "COMMIT"),
    ] {
        simple(&mut pg, sql).await.tags(&[tag]);
    }
    simple(&mut pg, "EXECUTE a1")
        .await
        .text_rows(&[&[Some("42")]]);
    simple(&mut pg, "SELECT nextval('sq')")
        .await
        .text_rows(&[&[Some("1")]]);
    simple(&mut pg, "SELECT lastval()")
        .await
        .text_rows(&[&[Some("1")]]);
    simple(&mut pg, "DISCARD SEQUENCES")
        .await
        .tags(&["DISCARD SEQUENCES"]);
    simple(&mut pg, "SELECT currval('sq')").await.assert_error(
        "55000",
        "currval of sequence \"sq\" is not yet defined in this session",
    );
    simple(&mut pg, "SELECT lastval()")
        .await
        .assert_error("55000", "lastval is not yet defined in this session");
    simple(&mut pg, "SELECT nextval('sq')")
        .await
        .text_rows(&[&[Some("2")]]);
    simple(&mut pg, "SELECT pg_advisory_lock(12345)")
        .await
        .success();
    simple(&mut other, "SELECT pg_try_advisory_lock(12345)")
        .await
        .text_rows(&[&[Some("f")]]);
    simple(&mut pg, "BEGIN").await.tags(&["BEGIN"]);
    simple(&mut pg, "DISCARD ALL")
        .await
        .assert_error("25001", "DISCARD ALL cannot run inside a transaction block");
    simple(&mut pg, "ROLLBACK").await.tags(&["ROLLBACK"]);
    simple(&mut pg, "SET SESSION AUTHORIZATION someone")
        .await
        .tags(&["SET"]);
    simple(&mut pg, "DISCARD ALL").await.tags(&["DISCARD ALL"]);
    simple(&mut pg, "SELECT current_user")
        .await
        .text_rows(&[&[Some("owner")]]);
    simple(&mut other, "SELECT pg_try_advisory_lock(12345)")
        .await
        .text_rows(&[&[Some("t")]]);
    simple(&mut pg, "SHOW application_name")
        .await
        .text_rows(&[&[Some("")]]);
    simple(&mut pg, "SHOW work_mem")
        .await
        .text_rows(&[&[Some("4MB")]]);
    simple(&mut pg, "SHOW worker_c.probe")
        .await
        .text_rows(&[&[Some("")]]);
    simple(
        &mut pg,
        "SELECT count(*) FROM pg_prepared_statements WHERE name='a1'",
    )
    .await
    .text_rows(&[&[Some("0")]]);
    assert_eq!(
        simple(&mut pg, "FETCH NEXT FROM held").await.field(b'C'),
        Some("34000")
    );
    simple(&mut pg, "SELECT currval('sq')").await.assert_error(
        "55000",
        "currval of sequence \"sq\" is not yet defined in this session",
    );
    drop((pg, other));
    fixture.shutdown().await;
}

#[tokio::test]
async fn extended_commands_share_named_statements_and_result_formats() {
    let fixture = Fixture::new("extended").await;
    let mut pg = fixture.connect().await;
    let mut other = fixture.connect().await;

    parse(&mut pg, "protocol", "SELECT 41 AS answer").await;
    sync(&mut pg).await.success();
    simple(&mut pg, "EXECUTE protocol")
        .await
        .text_rows(&[&[Some("41")]]);
    let prepared = "/*lead*/ PREPARE sql_level (int) AS SELECT $1 + 1 AS answer;";
    extended(&mut pg, prepared, &[], false)
        .await
        .tags(&["PREPARE"]);
    let absent = extended(&mut pg, "EXECUTE missing", &[], false).await;
    assert!(absent.0.iter().any(|(tag, _)| *tag == b'1'));
    assert!(absent.0.iter().any(|(tag, _)| *tag == b'2'));
    absent.assert_error("26000", r#"prepared statement "missing" does not exist"#);
    extended(&mut pg, "EXECUTE sql_level($1)", &[], false)
        .await
        .assert_error("42P02", "there is no parameter $1");
    let response = extended(&mut pg, "EXECUTE sql_level(40+1)", &[], true).await;
    response.tags(&["SELECT 1"]);
    assert_eq!(response.first_column_type(), 23);
    assert_eq!(response.rows(), [vec![Some(42i32.to_be_bytes().to_vec())]]);
    simple(&mut pg, "PREPARE unused (int) AS SELECT 9")
        .await
        .tags(&["PREPARE"]);
    simple(&mut pg, "EXECUTE unused(12)")
        .await
        .text_rows(&[&[Some("9")]]);
    simple(&mut pg, "CREATE TABLE t(id int PRIMARY KEY)")
        .await
        .tags(&["CREATE TABLE"]);
    simple(&mut pg, "PREPARE no_result AS DELETE FROM t WHERE id=$1")
        .await
        .tags(&["PREPARE"]);
    simple(&mut pg, "SELECT name, statement, parameter_types, result_types, from_sql FROM pg_prepared_statements ORDER BY name")
        .await.text_rows(&[
        &[Some("no_result"), Some("PREPARE no_result AS DELETE FROM t WHERE id=$1"), Some("{integer}"), None, Some("t")],
        &[Some("protocol"), Some("SELECT 41 AS answer"), Some("{}"), Some("{integer}"), Some("f")],
        &[Some("sql_level"), Some(prepared), Some("{integer}"), Some("{integer}"), Some("t")],
        &[Some("unused"), Some("PREPARE unused (int) AS SELECT 9"), Some("{integer}"), Some("{integer}"), Some("t")],
    ]);
    let response = extended(
        &mut pg,
        "SELECT parameter_types FROM pg_prepared_statements WHERE name=$1",
        &["sql_level"],
        true,
    )
    .await;
    assert_eq!(response.first_column_type(), 2211);
    let row = response.rows();
    let array = row[0][0].as_deref().unwrap();
    assert_eq!(&array[8..12], &2206u32.to_be_bytes());
    assert_eq!(&array[24..28], &23u32.to_be_bytes());
    simple(&mut pg, "SELECT name, generic_plans + custom_plans >= 0 AS plans FROM pg_prepared_statements WHERE name='sql_level'")
        .await.text_rows(&[&[Some("sql_level"), Some("t")]]);
    simple(
        &mut other,
        "SELECT count(*) FROM pg_catalog.pg_prepared_statements",
    )
    .await
    .text_rows(&[&[Some("0")]]);
    extended(&mut pg, "DEALLOCATE PREPARE ALL", &[], false)
        .await
        .tags(&["DEALLOCATE ALL"]);
    simple(&mut pg, "EXECUTE protocol")
        .await
        .assert_error("26000", "prepared statement \"protocol\" does not exist");
    let batch = "PREPARE a AS SELECT 1; PREPARE b AS SELECT 2;";
    simple(&mut pg, batch).await.tags(&["PREPARE", "PREPARE"]);
    simple(
        &mut pg,
        "SELECT name, statement FROM pg_prepared_statements ORDER BY name",
    )
    .await
    .text_rows(&[&[Some("a"), Some(batch)], &[Some("b"), Some(batch)]]);
    parse(&mut pg, "protocol", "SELECT 1").await;
    sync(&mut pg).await.success();
    extended(&mut pg, "DISCARD ALL", &[], false)
        .await
        .tags(&["DISCARD ALL"]);
    simple(&mut pg, "SELECT count(*) FROM pg_prepared_statements")
        .await
        .text_rows(&[&[Some("0")]]);
    drop((pg, other));
    fixture.shutdown().await;
}

#[tokio::test]
async fn prepare_after_copy_keeps_original_batch_text() {
    let fixture = Fixture::new("prepare-after-copy").await;
    let mut pg = fixture.connect().await;
    simple(&mut pg, "CREATE TABLE t (id int PRIMARY KEY)")
        .await
        .success();
    let batch = "COPY t FROM STDIN; COPY t FROM STDIN; /* preserved */ PREPARE after_copy AS SELECT id FROM t ORDER BY id;";
    send(&mut pg, b'Q', format!("{batch}\0").as_bytes()).await;
    receive_until(&mut pg, b'G').await.success();
    send(&mut pg, b'd', b"1\n").await;
    send(&mut pg, b'c', &[]).await;
    receive_until(&mut pg, b'G').await.tags(&["COPY 1"]);
    send(&mut pg, b'd', b"2\n").await;
    send(&mut pg, b'c', &[]).await;
    receive(&mut pg).await.tags(&["COPY 1", "PREPARE"]);

    simple(&mut pg, "SELECT statement FROM pg_prepared_statements WHERE name = 'after_copy'")
        .await
        .text_rows(&[&[Some(batch)]]);
    simple(&mut pg, "EXECUTE after_copy")
        .await
        .text_rows(&[&[Some("1")], &[Some("2")]]);
    drop(pg);
    fixture.shutdown().await;
}

#[tokio::test]
async fn execute_assigns_every_argument_and_forwards_only_plan_slots() {
    let fixture = Fixture::new("execute-assignment").await;
    let mut pg = fixture.connect().await;
    for (name, prepare, args, expected) in [
        (
            "unused",
            "PREPARE unused (int) AS SELECT 7 AS n",
            &["999"][..],
            "7",
        ),
        (
            "extra",
            "PREPARE extra (int, text) AS SELECT $1 AS n",
            &["8", "unused"][..],
            "8",
        ),
    ] {
        simple(&mut pg, prepare).await.tags(&["PREPARE"]);
        let args_sql = args
            .iter()
            .map(|value| format!("'{value}'"))
            .collect::<Vec<_>>()
            .join(",");
        simple(&mut pg, &format!("EXECUTE {name}({args_sql})"))
            .await
            .text_rows(&[&[Some(expected)]]);
        bind_execute(&mut pg, name, args, false)
            .await
            .text_rows(&[&[Some(expected)]]);
    }
    for (sql, code, message) in [
        (
            "EXECUTE unused('bad')",
            "22P02",
            r#"invalid input syntax for type integer: "bad""#,
        ),
        (
            "EXECUTE unused('2147483648')",
            "22003",
            r#"value "2147483648" is out of range for type integer"#,
        ),
        (
            "EXECUTE unused(2147483648)",
            "22003",
            "integer out of range",
        ),
        (
            "EXECUTE unused((SELECT 1))",
            "0A000",
            "cannot use subquery in EXECUTE parameter",
        ),
        (
            "EXECUTE unused(ARRAY(SELECT 1))",
            "0A000",
            "cannot use subquery in EXECUTE parameter",
        ),
    ] {
        simple(&mut pg, sql).await.assert_error(code, message);
    }
    let invalid_bind = bind_execute(&mut pg, "unused", &["bad"], false).await;
    assert_eq!(invalid_bind.field(b'C'), Some("22P02"), "{invalid_bind:?}");
    for (argument, source) in [
        ("'1'::text", "text"),
        ("true", "boolean"),
        ("NULL::text", "text"),
    ] {
        let response = simple(&mut pg, &format!("EXECUTE unused({argument})")).await;
        response.assert_error(
            "42804",
            &format!(
                "parameter $1 of type {source} cannot be coerced to the expected type integer"
            ),
        );
        assert_eq!(
            response.field(b'H'),
            Some("You will need to rewrite or cast the expression.")
        );
    }
    simple(&mut pg, "EXECUTE extra(1.5, true)")
        .await
        .text_rows(&[&[Some("2")]]);
    simple(&mut pg, "EXECUTE extra(NULL, 12)")
        .await
        .text_rows(&[&[None]]);
    // An unused argument is still executed.
    simple(&mut pg, "CREATE SEQUENCE evaluated")
        .await
        .tags(&["CREATE SEQUENCE"]);
    simple(&mut pg, "EXECUTE unused(nextval('evaluated'))")
        .await
        .text_rows(&[&[Some("7")]]);
    simple(&mut pg, "SELECT lastval()")
        .await
        .text_rows(&[&[Some("1")]]);
    drop(pg);
    fixture.shutdown().await;
}

#[tokio::test]
async fn execute_utility_uses_parse_and_bind_descriptions_with_live_authority() {
    let fixture = Fixture::new("execute-description").await;
    let mut pg = fixture.connect().await;
    simple(&mut pg, "PREPARE changing AS SELECT 1 AS old_result")
        .await
        .tags(&["PREPARE"]);
    parse(&mut pg, "utility", "EXECUTE changing").await;
    sync(&mut pg).await.success();
    simple(&mut pg, "DEALLOCATE changing")
        .await
        .tags(&["DEALLOCATE"]);
    simple(
        &mut pg,
        "PREPARE changing AS SELECT 'new'::text AS new_result, 5 AS n",
    )
    .await
    .tags(&["PREPARE"]);
    send(&mut pg, b'D', b"Sutility\0").await;
    let description = sync(&mut pg).await;
    assert_eq!(description.first_column_type(), 23);
    assert!(
        description
            .0
            .iter()
            .any(|(tag, body)| *tag == b'T' && body.starts_with(b"\0\x01old_result\0"))
    );

    bind(&mut pg, "utility", &[], &[0, 0, 0]).await;
    sync(&mut pg).await.assert_error(
        "08P01",
        "bind message has 3 result formats but query has 2 columns",
    );
    bind(&mut pg, "utility", &[], &[0, 1]).await;
    send(&mut pg, b'D', b"P\0").await;
    send(&mut pg, b'E', &[0; 5]).await;
    let bound = sync(&mut pg).await;
    bound.tags(&["SELECT 1"]);
    assert_eq!(bound.first_column_type(), 25);
    assert!(
        bound.0.iter().any(|(tag, body)|
            *tag == b'T' && body.starts_with(b"\0\x02new_result\0"))
    );
    assert_eq!(
        bound.rows(),
        [vec![
            Some(b"new".to_vec()),
            Some(5i32.to_be_bytes().to_vec())
        ]]
    );

    simple(&mut pg, "DEALLOCATE changing")
        .await
        .tags(&["DEALLOCATE"]);
    bind(&mut pg, "utility", &[], &[0, 1]).await;
    send(&mut pg, b'E', &[0; 5]).await;
    let absent = sync(&mut pg).await;
    assert!(absent.0.iter().any(|(tag, _)| *tag == b'2'));
    absent.assert_error("26000", r#"prepared statement "changing" does not exist"#);

    simple(&mut pg, "CREATE TABLE hidden (secret int)")
        .await
        .tags(&["CREATE TABLE"]);
    simple(&mut pg, "PREPARE changing AS SELECT secret FROM hidden")
        .await
        .tags(&["PREPARE"]);
    simple(
        &mut pg,
        "PREPARE catalog_target AS SELECT hidden.secret FROM pg_namespace CROSS JOIN hidden",
    )
    .await
    .tags(&["PREPARE"]);
    parse(&mut pg, "catalog_utility", "EXECUTE catalog_target").await;
    sync(&mut pg).await.success();
    simple(&mut pg, "SET ROLE someone").await.tags(&["SET"]);
    send(&mut pg, b'D', b"Sutility\0").await;
    let denied = sync(&mut pg).await;
    assert_eq!(denied.field(b'C'), Some("42501"), "{denied:?}");
    assert!(!denied.0.iter().any(|(tag, _)| *tag == b'T'));
    bind(&mut pg, "utility", &[], &[]).await;
    let denied = sync(&mut pg).await;
    assert_eq!(denied.field(b'C'), Some("42501"), "{denied:?}");
    assert!(!denied.0.iter().any(|(tag, _)| *tag == b'T' || *tag == b'2'));
    send(&mut pg, b'D', b"Scatalog_utility\0").await;
    let denied = sync(&mut pg).await;
    assert_eq!(denied.field(b'C'), Some("42501"), "{denied:?}");
    assert!(!denied.0.iter().any(|(tag, _)| *tag == b'T'));
    drop(pg);
    fixture.shutdown().await;
}

#[tokio::test]
async fn execute_analyzes_unknown_literals_before_running_any_argument() {
    let fixture = Fixture::new("execute-literal-order").await;
    let mut pg = fixture.connect().await;
    simple(&mut pg, "CREATE SEQUENCE argument_order").await.tags(&["CREATE SEQUENCE"]);
    simple(&mut pg, "PREPARE order_check(int, int) AS SELECT 7").await.tags(&["PREPARE"]);
    for (literal, code, message) in [
        ("'bad'", "22P02", r#"invalid input syntax for type integer: "bad""#),
        ("'2147483648'", "22003", r#"value "2147483648" is out of range for type integer"#),
    ] {
        let volatile = "nextval('argument_order')";
        for (first, second) in [(literal, volatile), (volatile, literal)] {
            simple(&mut pg, &format!("EXECUTE order_check({first}, {second})"))
                .await.assert_error(code, message);
        }
    }
    simple(&mut pg, "SELECT lastval()")
        .await.assert_error("55000", "lastval is not yet defined in this session");
    simple(&mut pg, "SELECT nextval('argument_order')")
        .await.text_rows(&[&[Some("1")]]);
    drop(pg);
    fixture.shutdown().await;
}

#[tokio::test]
async fn execute_oid_arguments_follow_oid_input_and_assignment_rules() {
    let fixture = Fixture::new("execute-oid-assignment").await;
    let mut pg = fixture.connect().await;
    simple(&mut pg, "PREPARE oid_arg(oid) AS SELECT 7").await.tags(&["PREPARE"]);
    for argument in ["'-1'", "-1", "4294967295::bigint", "'0xffffffff'"] {
        simple(&mut pg, &format!("EXECUTE oid_arg({argument})"))
            .await.text_rows(&[&[Some("7")]]);
    }
    let bad_assignment = simple(&mut pg, "EXECUTE oid_arg(1.2::numeric)").await;
    bad_assignment.assert_error(
        "42804", "parameter $1 of type numeric cannot be coerced to the expected type oid",
    );
    assert_eq!(bad_assignment.field(b'H'), Some("You will need to rewrite or cast the expression."));
    for argument in ["4294967296::bigint", "(-1)::bigint"] {
        simple(&mut pg, &format!("EXECUTE oid_arg({argument})"))
            .await.assert_error("22003", "OID out of range");
    }
    simple(&mut pg, "EXECUTE oid_arg('4294967296')")
        .await.assert_error("22003", r#"value "4294967296" is out of range for type oid"#);
    bind_execute(&mut pg, "oid_arg", &["-1"], false)
        .await.text_rows(&[&[Some("7")]]);
    let bind_range = bind_execute(&mut pg, "oid_arg", &["4294967296"], false).await;
    bind_range.assert_error("22003", r#"value "4294967296" is out of range for type oid"#);
    drop(pg);
    fixture.shutdown().await;
}

#[tokio::test]
async fn execute_portal_keeps_bound_label_and_requires_compatible_result_types() {
    let fixture = Fixture::new("execute-portal-result").await;
    let mut pg = fixture.connect().await;
    simple(&mut pg, "PREPARE changing AS SELECT 'old'::text AS old_result")
        .await.tags(&["PREPARE"]);
    parse(&mut pg, "utility", "EXECUTE changing").await;
    sync(&mut pg).await.success();

    simple(&mut pg, "BEGIN").await.tags(&["BEGIN"]);
    bind_portal(&mut pg, "before_rename", "utility", &[], &[]).await;
    sync(&mut pg).await.success();
    simple(&mut pg, "DEALLOCATE changing; PREPARE changing AS SELECT 'again'::text AS renamed")
        .await.tags(&["DEALLOCATE", "PREPARE"]);
    send(&mut pg, b'D', b"Pbefore_rename\0").await;
    send(&mut pg, b'E', b"before_rename\0\0\0\0\0").await;
    send(&mut pg, b'D', b"Pbefore_rename\0").await;
    let renamed = sync(&mut pg).await;
    renamed.tags(&["SELECT 1"]);
    renamed.text_rows(&[&[Some("again")]]);
    let descriptions = renamed.0.iter().filter(|(tag, _)| *tag == b'T').collect::<Vec<_>>();
    assert_eq!(descriptions.len(), 2);
    assert!(descriptions.iter().all(|(_, body)| body.starts_with(b"\0\x01old_result\0")));
    simple(&mut pg, "ROLLBACK").await.tags(&["ROLLBACK"]);

    simple(&mut pg, "BEGIN").await.tags(&["BEGIN"]);
    bind_portal(&mut pg, "before_type_change", "utility", &[], &[]).await;
    sync(&mut pg).await.success();
    simple(&mut pg, "DEALLOCATE changing; PREPARE changing AS SELECT 73::int AS n")
        .await.tags(&["DEALLOCATE", "PREPARE"]);
    send(&mut pg, b'E', b"before_type_change\0\0\0\0\0").await;
    let changed = sync(&mut pg).await;
    changed.assert_error("42804", "query result type does not match portal result type");
    assert_eq!(changed.field(b'D'), Some(
        r#"Returned type integer does not match expected type text in column "renamed" (position 1)."#
    ));
    simple(&mut pg, "ROLLBACK").await.tags(&["ROLLBACK"]);
    drop(pg);
    fixture.shutdown().await;
}

#[tokio::test]
async fn execute_portal_compares_catalog_result_types() {
    let fixture = Fixture::new("execute-portal-catalog-result").await;
    let mut pg = fixture.connect().await;
    simple(&mut pg, "PREPARE c AS SELECT nspname FROM pg_namespace WHERE oid = 11")
        .await.tags(&["PREPARE"]);
    parse(&mut pg, "utility", "EXECUTE c").await;
    sync(&mut pg).await.success();

    simple(&mut pg, "BEGIN").await.tags(&["BEGIN"]);
    bind_portal(&mut pg, "catalog_name", "utility", &[], &[1]).await;
    send(&mut pg, b'D', b"Pcatalog_name\0").await;
    let bound = sync(&mut pg).await;
    bound.success();
    assert_eq!(bound.first_column_type(), 19);
    simple(&mut pg, "DEALLOCATE c; PREPARE c AS SELECT oid FROM pg_namespace WHERE oid = 11")
        .await.tags(&["DEALLOCATE", "PREPARE"]);
    send(&mut pg, b'E', b"catalog_name\0\0\0\0\0").await;
    let changed = sync(&mut pg).await;
    changed.assert_error("42804", "query result type does not match portal result type");
    assert_eq!(changed.field(b'D'), Some(
        r#"Returned type oid does not match expected type name in column "nspname" (position 1)."#
    ));
    simple(&mut pg, "ROLLBACK").await.tags(&["ROLLBACK"]);

    simple(&mut pg, "DEALLOCATE c; PREPARE c AS SELECT typlen FROM pg_type WHERE oid = 23")
        .await.tags(&["DEALLOCATE", "PREPARE"]);
    simple(&mut pg, "BEGIN").await.tags(&["BEGIN"]);
    bind_portal(&mut pg, "catalog_short", "utility", &[], &[1]).await;
    send(&mut pg, b'D', b"Pcatalog_short\0").await;
    let bound = sync(&mut pg).await;
    bound.success();
    assert_eq!(bound.first_column_type(), 21);
    simple(&mut pg, "DEALLOCATE c; PREPARE c AS SELECT 2::int2 AS typlen")
        .await.tags(&["DEALLOCATE", "PREPARE"]);
    send(&mut pg, b'E', b"catalog_short\0\0\0\0\0").await;
    send(&mut pg, b'D', b"Pcatalog_short\0").await;
    let replaced = sync(&mut pg).await;
    replaced.tags(&["SELECT 1"]);
    assert_eq!(replaced.rows(), vec![vec![Some(2i16.to_be_bytes().to_vec())]]);
    assert_eq!(replaced.first_column_type(), 21);
    simple(&mut pg, "ROLLBACK").await.tags(&["ROLLBACK"]);
    drop(pg);
    fixture.shutdown().await;
}

#[tokio::test]
async fn prepare_validates_declarations_and_reports_analysis_errors_before_duplicates() {
    let fixture = Fixture::new("prepare-analysis").await;
    let mut pg = fixture.connect().await;
    simple(&mut pg, "PREPARE shape AS ((SELECT 3 AS n))")
        .await
        .tags(&["PREPARE"]);
    simple(&mut pg, "EXECUTE shape")
        .await
        .text_rows(&[&[Some("3")]]);
    simple(
        &mut pg,
        "PREPARE shape AS SELECT * FROM missing_before_duplicate",
    )
    .await
    .assert_error(
        "42P01",
        r#"relation "missing_before_duplicate" does not exist"#,
    );
    simple(&mut pg, "EXECUTE shape")
        .await
        .text_rows(&[&[Some("3")]]);
    for (ty, message) in [
        (
            "numeric(0)",
            "NUMERIC precision 0 must be between 1 and 1000",
        ),
        ("varchar(0)", "length for type varchar must be at least 1"),
        (
            "float(0)",
            "precision for type float must be at least 1 bit",
        ),
        (
            "float(54)",
            "precision for type float must be less than 54 bits",
        ),
    ] {
        simple(&mut pg, &format!("PREPARE rejected ({ty}) AS SELECT $1"))
            .await
            .assert_error("22023", message);
        simple(&mut pg, &format!("SELECT '{ty}'::regtype"))
            .await
            .assert_error("22023", message);
    }
    for (ty, code, message) in [
        ("pg_catalog.numeric(0)", "22023", "NUMERIC precision 0 must be between 1 and 1000"),
        (r#""varchar"(0)"#, "22023", "length for type varchar must be at least 1"),
        ("pg_catalog.varchar(0)", "22023", "length for type varchar must be at least 1"),
        ("numeric(1,-1001)", "22023", "NUMERIC scale -1001 must be between -1000 and 1000"),
        ("pg_catalog.float4(1)", "42601", r#"type modifier is not allowed for type "pg_catalog.float4""#),
        ("pg_catalog.int4(1)", "42601", r#"type modifier is not allowed for type "pg_catalog.int4""#),
        ("int4(1)", "42601", r#"type modifier is not allowed for type "int4""#),
        (r#""text"(5)"#, "42601", r#"type modifier is not allowed for type "text""#),
        ("pg_catalog.text(5)", "42601", r#"type modifier is not allowed for type "pg_catalog.text""#),
        ("pg_catalog.bool(1)", "42601", r#"type modifier is not allowed for type "pg_catalog.bool""#),
        ("bool(1)", "42601", r#"type modifier is not allowed for type "bool""#),
        ("pg_catalog.int4(1,2)", "42601", r#"type modifier is not allowed for type "pg_catalog.int4""#),
        ("pg_catalog.timestamp(-1)", "22023", "TIMESTAMP(-1) precision must not be negative"),
        ("pg_catalog.interval(7)", "22023", "invalid INTERVAL type modifier"),
    ] {
        simple(&mut pg, &format!("PREPARE rejected ({ty}) AS SELECT $1"))
            .await.assert_error(code, message);
        simple(&mut pg, &format!("SELECT '{ty}'::regtype"))
            .await.assert_error(code, message);
    }
    let invalid_timestamp = simple(&mut pg, "SELECT 'timestamp(-1)'::regtype").await;
    invalid_timestamp.assert_error("42601", r#"syntax error at or near "-""#);
    assert_eq!(invalid_timestamp.field(b'W'), Some(r#"invalid type name "timestamp(-1)""#));
    simple(&mut pg, "PREPARE rejected (timestamp(-1)) AS SELECT $1")
        .await.assert_error("42601", r#"syntax error at or near "-""#);
    for (ty, resolved) in [
        ("pg_catalog.timestamp(7)", "timestamp without time zone"),
        ("pg_catalog.timestamptz(3)", "timestamp with time zone"),
        ("pg_catalog.time(7)", "time without time zone"),
        ("pg_catalog.timetz(3)", "time with time zone"),
        ("interval(7)", "interval"),
    ] {
        simple(&mut pg, &format!("SELECT '{ty}'::regtype"))
            .await.text_rows(&[&[Some(resolved)]]);
    }
    simple(&mut pg, "PREPARE n2f (pg_catalog.timestamp(7)) AS SELECT $1")
        .await.tags(&["PREPARE"]);
    simple(&mut pg, "PREPARE n2g (timestamp(3)) AS SELECT $1")
        .await.tags(&["PREPARE"]);
    simple(&mut pg, "SELECT name, parameter_types FROM pg_prepared_statements
        WHERE name IN ('n2f', 'n2g') ORDER BY name")
        .await.text_rows(&[
            &[Some("n2f"), Some(r#"{"timestamp without time zone"}"#)],
            &[Some("n2g"), Some(r#"{"timestamp without time zone"}"#)],
        ]);
    simple(&mut pg, "PREPARE signed_scale(numeric(3,-2)) AS SELECT $1")
        .await.tags(&["PREPARE"]);
    simple(&mut pg, "EXECUTE signed_scale(1234)")
        .await.text_rows(&[&[Some("1234")]]);
    simple(&mut pg, "SELECT 'numeric(3,-2)'::regtype")
        .await.text_rows(&[&[Some("numeric")]]);
    simple(
        &mut pg,
        "PREPARE declared_plan (float(24), float(25), varchar(2)) AS SELECT $1, $2, $3",
    )
    .await
    .tags(&["PREPARE"]);
    simple(&mut pg, "EXECUTE declared_plan(1.5, 2.5, 'untruncated')")
        .await
        .text_rows(&[&[Some("1.5"), Some("2.5"), Some("untruncated")]]);
    simple(&mut pg, "SELECT parameter_types,result_types FROM pg_prepared_statements WHERE name='declared_plan'")
        .await.text_rows(&[&[Some("{real,\"double precision\",\"character varying\"}"), Some("{real,\"double precision\",\"character varying\"}")]]);

    simple(&mut pg, "PREPARE counted (int) AS SELECT $1 AS n")
        .await
        .tags(&["PREPARE"]);
    for mode in ["force_generic_plan", "force_custom_plan"] {
        simple(&mut pg, &format!("SET plan_cache_mode = {mode}"))
            .await
            .tags(&["SET"]);
        simple(&mut pg, "EXECUTE counted(7)")
            .await
            .text_rows(&[&[Some("7")]]);
        bind_execute(&mut pg, "counted", &["8"], false)
            .await
            .text_rows(&[&[Some("8")]]);
    }
    simple(&mut pg, "EXECUTE shape")
        .await
        .text_rows(&[&[Some("3")]]);
    simple(&mut pg, "SELECT name,generic_plans,custom_plans FROM pg_prepared_statements WHERE name IN ('counted','shape') ORDER BY name")
        .await.text_rows(&[&[Some("counted"), Some("2"), Some("2")], &[Some("shape"), Some("3"), Some("0")]]);
    drop(pg);
    fixture.shutdown().await;
}

#[tokio::test]
async fn lastval_checks_session_observation_then_resolved_sequence_authority() {
    let fixture = Fixture::new("lastval-authority").await;
    let mut pg = fixture.connect().await;
    simple(&mut pg, "CREATE SEQUENCE owner_only")
        .await
        .tags(&["CREATE SEQUENCE"]);
    simple(&mut pg, "SET ROLE someone").await.tags(&["SET"]);
    for result in [
        simple(&mut pg, "SELECT lastval()").await,
        extended(&mut pg, "SELECT pg_catalog.lastval()", &[], false).await,
    ] {
        result.assert_error("55000", "lastval is not yet defined in this session");
    }
    simple(&mut pg, "RESET ROLE").await.tags(&["RESET"]);
    simple(&mut pg, "SELECT nextval('owner_only')")
        .await
        .text_rows(&[&[Some("1")]]);
    simple(&mut pg, "SET ROLE someone").await.tags(&["SET"]);
    let denied = simple(&mut pg, "SELECT lastval()").await;
    assert_eq!(denied.field(b'C'), Some("42501"), "{denied:?}");
    simple(&mut pg, "RESET ROLE").await.tags(&["RESET"]);
    simple(&mut pg, "SELECT lastval()")
        .await
        .text_rows(&[&[Some("1")]]);
    drop(pg);
    fixture.shutdown().await;
}

#[tokio::test]
async fn execute_preserves_enum_parameter_and_join_result_oids() {
    let fixture = Fixture::new("execute-enum-oids").await;
    let mut pg = fixture.connect().await;
    simple(
        &mut pg,
        "CREATE TYPE mood AS ENUM ('z', 'a');
         CREATE TYPE \"Quoted\" AS ENUM ('q');
         CREATE TABLE lhs (id int PRIMARY KEY, item mood, items mood[]);
         CREATE TABLE rhs (id int PRIMARY KEY, item mood, items mood[]);
         INSERT INTO lhs VALUES (1, 'z', '{z,a}');
         INSERT INTO rhs VALUES (2, 'z', '{z,a}');",
    )
    .await
    .tags(&["CREATE TYPE", "CREATE TYPE", "CREATE TABLE", "CREATE TABLE", "INSERT 0 1", "INSERT 0 1"]);
    simple(&mut pg, r#"SELECT 'mood'::regtype, 'mood[]'::regtype, '"Quoted"'::regtype"#)
        .await
        .text_rows(&[&[Some("mood"), Some("mood[]"), Some(r#""Quoted""#)]]);
    simple(
        &mut pg,
        "SELECT attname, format_type(atttypid, atttypmod) FROM pg_attribute
         WHERE attrelid = 'lhs'::regclass AND attname IN ('item', 'items') ORDER BY attnum",
    )
    .await
    .text_rows(&[&[Some("item"), Some("mood")], &[Some("items"), Some("mood[]")]]);
    let oid_rows = simple(&mut pg, "SELECT oid, typarray FROM pg_type WHERE typname = 'mood'")
        .await
        .rows();
    let parse_oid = |i: usize| {
        std::str::from_utf8(oid_rows[0][i].as_deref().unwrap())
            .unwrap()
            .parse::<u32>()
            .unwrap()
    };
    let (enum_oid, array_oid) = (parse_oid(0), parse_oid(1));
    assert_ne!(enum_oid, array_oid);
    assert!(enum_oid >= 65536 && array_oid >= 65536);
    for sql in [
        "PREPARE typed(mood, mood[]) AS SELECT $1 AS item, $2 AS items",
        "PREPARE inferred AS SELECT item, items FROM lhs WHERE item = $1 AND items = $2",
        "PREPARE from_join AS SELECT item, items FROM lhs JOIN rhs USING (item, items)",
        "PREPARE unused(mood, mood[]) AS SELECT 7",
    ] {
        simple(&mut pg, sql).await.tags(&["PREPARE"]);
    }
    simple(
        &mut pg,
        "SELECT name, parameter_types, result_types FROM pg_prepared_statements ORDER BY name",
    )
    .await
    .text_rows(&[
        &[Some("from_join"), Some("{}"), Some("{mood,mood[]}")],
        &[Some("inferred"), Some("{mood,mood[]}"), Some("{mood,mood[]}")],
        &[Some("typed"), Some("{mood,mood[]}"), Some("{mood,mood[]}")],
        &[Some("unused"), Some("{mood,mood[]}"), Some("{integer}")],
    ]);
    let binary = extended(
        &mut pg,
        "SELECT parameter_types, result_types FROM pg_prepared_statements WHERE name = 'typed'",
        &[],
        true,
    )
    .await;
    let mut expected_oids = Vec::new();
    for part in [1, 0, 2206, 2, 1, 4, enum_oid, 4, array_oid] {
        expected_oids.extend(part.to_be_bytes());
    }
    assert_eq!(binary.rows(), [vec![Some(expected_oids.clone()), Some(expected_oids)]]);
    for sql in ["EXECUTE typed('z', '{z,a}')", "EXECUTE inferred('z', '{z,a}')", "EXECUTE from_join"] {
        let response = simple(&mut pg, sql).await;
        response.text_rows(&[&[Some("z"), Some("{z,a}")]]);
        let mut columns = &response.0.iter().find(|(tag, _)| *tag == b'T').unwrap().1[..];
        assert_eq!(&columns[..2], &[0, 2], "{sql}");
        columns = &columns[2..];
        for (expected_name, expected_oid) in [(b"item\0".as_slice(), enum_oid), (b"items\0", array_oid)] {
            columns = columns.strip_prefix(expected_name).unwrap();
            assert_eq!(u32::from_be_bytes(columns[6..10].try_into().unwrap()), expected_oid, "{sql}");
            columns = &columns[18..];
        }
        assert!(columns.is_empty(), "{sql}");
    }
    simple(&mut pg, "EXECUTE unused('z', '{z}')").await.text_rows(&[&[Some("7")]]);
    simple(&mut pg, "EXECUTE typed('z'::mood, '{z,a}'::mood[])")
        .await
        .text_rows(&[&[Some("z"), Some("{z,a}")]]);
    simple(&mut pg, "EXECUTE typed(NULL, NULL)").await.text_rows(&[&[None, None]]);

    simple(&mut pg, "BEGIN").await.tags(&["BEGIN"]);
    simple(&mut pg, "ALTER TYPE mood ADD VALUE 'm' AFTER 'z'").await.tags(&["ALTER TYPE"]);
    let unsafe_value = simple(&mut pg, "EXECUTE typed('m', '{m,a}')").await;
    unsafe_value.assert_error("55P04", r#"unsafe use of new value "m" of enum type mood"#);
    assert_eq!(unsafe_value.field(b'H'), Some("New enum values must be committed before they can be used."));
    simple(&mut pg, "ROLLBACK").await.tags(&["ROLLBACK"]);
    simple(&mut pg, "ALTER TYPE mood ADD VALUE 'm' AFTER 'z'").await.tags(&["ALTER TYPE"]);
    simple(&mut pg, "EXECUTE typed('m', '{m,a}')").await.text_rows(&[&[Some("m"), Some("{m,a}")]]);
    simple(&mut pg, "EXECUTE inferred('m', '{m,a}')").await.text_rows(&[]);
    let wrong_type = simple(&mut pg, "EXECUTE typed('m'::text, '{m}'::mood[])").await;
    wrong_type.assert_error("42804", "parameter $1 of type text cannot be coerced to the expected type mood");
    assert_eq!(wrong_type.field(b'H'), Some("You will need to rewrite or cast the expression."));
    drop(pg);
    fixture.shutdown().await;
}

const ENUM_CACHE_QUERY: &str = "SELECT key_b, k, ks, $2 AS supplied FROM enum_cache WHERE k = $1";

fn enum_array_binary(element_oid: u32, value: Option<&str>) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend(u32::from(value.is_some()).to_be_bytes());
    bytes.extend(0u32.to_be_bytes());
    bytes.extend(element_oid.to_be_bytes());
    if let Some(value) = value {
        bytes.extend(1u32.to_be_bytes());
        bytes.extend(1u32.to_be_bytes());
        bytes.extend((value.len() as u32).to_be_bytes());
        bytes.extend(value.as_bytes());
    }
    bytes
}

async fn execute_enum_portal(
    pg: &mut TcpStream,
    oid: u32,
    value: &str,
    items: Option<&str>,
    binary: bool,
) -> Response {
    let array_text = format!("{{{}}}", items.unwrap_or_default());
    let array_binary = enum_array_binary(oid, items);
    let array = if binary {
        array_binary.as_slice()
    } else {
        array_text.as_bytes()
    };
    let inputs = [value.as_bytes(), array];
    let format = u16::from(binary);
    let mut body = b"\0cached_wire\0".to_vec();
    body.extend(1u16.to_be_bytes());
    body.extend(format.to_be_bytes());
    body.extend(2u16.to_be_bytes());
    for input in inputs {
        body.extend((input.len() as u32).to_be_bytes());
        body.extend(input);
    }
    body.extend(1u16.to_be_bytes());
    body.extend(format.to_be_bytes());
    send(pg, b'B', &body).await;
    send(pg, b'D', b"P\0").await;
    send(pg, b'E', b"\0\0\0\0\0").await;
    sync(pg).await
}

fn assert_enum_columns(response: &Response, oid: u32, array_oid: u32, binary: bool) {
    response.success();
    let desc = &response.0.iter().find(|(tag, _)| *tag == b'T').unwrap().1;
    assert_eq!(&desc[..2], &4u16.to_be_bytes());
    let mut fields = &desc[2..];
    for (name, expected_oid) in [
        ("key_b", 23),
        ("k", oid),
        ("ks", array_oid),
        ("supplied", array_oid),
    ] {
        fields = fields.strip_prefix(name.as_bytes()).unwrap();
        fields = fields.strip_prefix(&[0]).unwrap();
        assert_eq!(&fields[6..10], &expected_oid.to_be_bytes());
        assert_eq!(&fields[16..18], &u16::from(binary).to_be_bytes());
        fields = &fields[18..];
    }
    assert!(fields.is_empty());
}

async fn expect_enum_portal(
    pg: &mut TcpStream,
    oids: (u32, u32),
    key: i32,
    value: &str,
    items: Option<&str>,
    binary: bool,
) {
    let (oid, array_oid) = oids;
    let response = execute_enum_portal(pg, oid, value, items, binary).await;
    assert_enum_columns(&response, oid, array_oid, binary);
    if binary {
        let array = enum_array_binary(oid, items);
        assert_eq!(
            response.rows(),
            [vec![
                Some(key.to_be_bytes().to_vec()),
                Some(value.as_bytes().to_vec()),
                Some(array.clone()),
                Some(array),
            ]],
        );
    } else {
        let array = format!("{{{}}}", items.unwrap_or_default());
        response.text_rows(&[&[
            Some(&key.to_string()),
            Some(value),
            Some(&array),
            Some(&array),
        ]]);
    }
}

#[tokio::test]
async fn prepared_enum_after_catalog_changes() {
    const UNSAFE: &str = r#"unsafe use of new value "pending" of enum type cache_mood"#;
    const NO_SQL: &str = r#"prepared statement "cached_sql" does not exist"#;
    const EXECUTE_OLD: &str = "EXECUTE cached_sql('old', '{old}')";
    const EXECUTE_NEW: &str = "EXECUTE cached_sql('new', '{}')";

    let fixture = Fixture::new("prepared-enum-after-catalog-changes").await;
    let mut pg = fixture.connect().await;
    simple(&mut pg, "CREATE TYPE cache_mood AS ENUM ('old')")
        .await
        .success();
    simple(
        &mut pg,
        "CREATE TABLE enum_cache (
            key_a int PRIMARY KEY, key_b int NOT NULL, k cache_mood,
            ks cache_mood[] DEFAULT ARRAY['old']::cache_mood[]
        )",
    )
    .await
    .success();
    simple(
        &mut pg,
        "INSERT INTO enum_cache (key_a, key_b, k) VALUES (1, 11, 'old')",
    )
    .await
    .success();

    let rows = simple(
        &mut pg,
        "SELECT oid, typarray FROM pg_type WHERE typname = 'cache_mood'",
    )
    .await
    .rows();
    let parse_oid = |index: usize| {
        std::str::from_utf8(rows[0][index].as_deref().unwrap())
            .unwrap()
            .parse::<u32>()
            .unwrap()
    };
    let (oid, array_oid) = (parse_oid(0), parse_oid(1));
    // One comparison and no sort, so V2 can reuse the streaming binding.
    let mut body = format!("cached_wire\0{ENUM_CACHE_QUERY}\0").into_bytes();
    body.extend(2u16.to_be_bytes());
    body.extend(oid.to_be_bytes());
    body.extend(array_oid.to_be_bytes());
    send(&mut pg, b'P', &body).await;
    send(&mut pg, b'D', b"Scached_wire\0").await;
    let description = sync(&mut pg).await;
    assert_enum_columns(&description, oid, array_oid, false);
    let params = &description
        .0
        .iter()
        .find(|(tag, _)| *tag == b't')
        .unwrap()
        .1;
    assert_eq!(&params[..2], &2u16.to_be_bytes());
    let (parameter_oids, trailing_bytes) = params[2..].as_chunks::<4>();
    assert!(trailing_bytes.is_empty());
    assert_eq!(parameter_oids, [oid.to_be_bytes(), array_oid.to_be_bytes()]);

    let prepare_sql = format!("PREPARE cached_sql(cache_mood, cache_mood[]) AS {ENUM_CACHE_QUERY}");
    simple(&mut pg, &prepare_sql).await.success();
    for binary in [false, true, false] {
        expect_enum_portal(&mut pg, (oid, array_oid), 11, "old", Some("old"), binary).await;
    }
    simple(&mut pg, EXECUTE_OLD).await.text_rows(&[&[
        Some("11"),
        Some("old"),
        Some("{old}"),
        Some("{old}"),
    ]]);

    simple(&mut pg, "BEGIN").await.success();
    simple(&mut pg, "ALTER TYPE cache_mood ADD VALUE 'pending'")
        .await
        .success();
    execute_enum_portal(&mut pg, oid, "pending", None, false)
        .await
        .assert_error("55P04", UNSAFE);
    simple(&mut pg, "ROLLBACK").await.success();

    simple(&mut pg, "BEGIN").await.success();
    simple(&mut pg, "ALTER TYPE cache_mood ADD VALUE 'pending'")
        .await
        .success();
    simple(&mut pg, "EXECUTE cached_sql('pending', '{}')")
        .await
        .assert_error("55P04", UNSAFE);
    simple(&mut pg, "ROLLBACK").await.success();

    simple(&mut pg, "ALTER TYPE cache_mood ADD VALUE 'new'")
        .await
        .success();
    simple(&mut pg, "BEGIN").await.success();
    for sql in [
        "ALTER TABLE enum_cache DROP CONSTRAINT enum_cache_pkey",
        "ALTER TABLE enum_cache ADD PRIMARY KEY (key_b)",
        "ALTER TABLE enum_cache ALTER COLUMN ks SET DEFAULT ARRAY[]::cache_mood[]",
        "INSERT INTO enum_cache (key_a, key_b, k) VALUES (1, 12, 'new')",
    ] {
        simple(&mut pg, sql).await.success();
    }
    simple(&mut pg, EXECUTE_NEW).await.text_rows(&[&[
        Some("12"),
        Some("new"),
        Some("{}"),
        Some("{}"),
    ]]);
    expect_enum_portal(&mut pg, (oid, array_oid), 12, "new", None, false).await;
    simple(&mut pg, "COMMIT").await.success();

    for binary in [false, true, false] {
        expect_enum_portal(&mut pg, (oid, array_oid), 12, "new", None, binary).await;
    }
    simple(&mut pg, EXECUTE_NEW).await.text_rows(&[&[
        Some("12"),
        Some("new"),
        Some("{}"),
        Some("{}"),
    ]]);
    let bind_sql_prepare = &["new", "{}"];
    for _ in 0..3 {
        bind_execute(&mut pg, "cached_sql", bind_sql_prepare, false)
            .await
            .text_rows(&[&[Some("12"), Some("new"), Some("{}"), Some("{}")]]);
    }
    simple(&mut pg, "DEALLOCATE cached_sql").await.success();
    simple(&mut pg, EXECUTE_NEW)
        .await
        .assert_error("26000", NO_SQL);
    simple(&mut pg, &prepare_sql).await.success();
    simple(&mut pg, EXECUTE_NEW).await.text_rows(&[&[
        Some("12"),
        Some("new"),
        Some("{}"),
        Some("{}"),
    ]]);
    bind_execute(&mut pg, "cached_sql", bind_sql_prepare, false)
        .await
        .text_rows(&[&[Some("12"), Some("new"), Some("{}"), Some("{}")]]);
    simple(&mut pg, "DISCARD ALL").await.success();
    simple(&mut pg, EXECUTE_NEW)
        .await
        .assert_error("26000", NO_SQL);
    let response = execute_enum_portal(&mut pg, oid, "new", None, false).await;
    assert_eq!(response.field(b'C'), Some("26000"), "{response:?}");

    drop(pg);
    fixture.shutdown().await;
}
