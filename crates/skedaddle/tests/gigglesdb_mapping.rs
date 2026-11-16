//! Live qualification: no substitute database is used when the server is absent.

use skedaddle::mapping::{
    Classification, DomainConstraints, Mapping, MappingError, PgType, TypeIdentity, Value,
    gigglesdb::GigglesdbMapping,
};
use std::{
    error::Error,
    fs,
    net::TcpListener,
    path::PathBuf,
    process::{Child, Command, Stdio},
    time::Duration,
};
use tokio_postgres::{
    Client, NoTls, SimpleQueryMessage,
    types::{Format, IsNull, ToSql, Type, private::BytesMut},
};

type TestError = Box<dyn Error + Send + Sync>;

struct Server {
    child: Child,
    root: PathBuf,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if let Err(error) = fs::remove_dir_all(&self.root) {
            eprintln!(
                "FAILED to remove test server data {}: {error}",
                self.root.display()
            );
        }
    }
}

async fn start_server(
    binary: PathBuf,
) -> Result<(Server, Client, tokio::task::JoinHandle<()>), TestError> {
    let parent = std::env::var_os("SKEDADDLE_TEST_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    let root = parent.join(format!("gd65-n4-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&root)?;
    let listener = TcpListener::bind(("127.0.0.1", 0))?;
    let port = listener.local_addr()?.port();
    drop(listener);
    let child = match Command::new(binary)
        .args(["--dev", "--trust-local", "--data"])
        .arg(root.join("data"))
        .args(["--port", &port.to_string()])
        .env("GIGGLES_OWNER_PASSWORD", uuid::Uuid::new_v4().to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
    {
        Ok(child) => child,
        Err(error) => {
            fs::remove_dir_all(root)?;
            return Err(error.into());
        }
    };
    let mut server = Server { child, root };
    let config =
        format!("host=127.0.0.1 port={port} user=postgres dbname=postgres connect_timeout=1");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(status) = server.child.try_wait()? {
            return Err(format!("giggles-server exited during startup: {status}").into());
        }
        if let Ok(Ok((client, connection))) = tokio::time::timeout(
            Duration::from_secs(1),
            tokio_postgres::connect(&config, NoTls),
        )
        .await
        {
            let task = tokio::spawn(async move {
                if let Err(error) = connection.await {
                    eprintln!("live mapping test connection: {error}");
                }
            });
            return Ok((server, client, task));
        }
        if tokio::time::Instant::now() >= deadline {
            return Err("giggles-server startup exceeded 30 seconds".into());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

// Real prepared text Bind, including non-default array bounds. BytesMut is
// already exposed by tokio-postgres for ToSql implementations; no new crate.
#[derive(Debug)]
struct TextBind(Option<Vec<u8>>);

impl ToSql for TextBind {
    fn to_sql(&self, _: &Type, output: &mut BytesMut) -> Result<IsNull, TestError> {
        match &self.0 {
            Some(bytes) => {
                output.extend_from_slice(bytes);
                Ok(IsNull::No)
            }
            None => Ok(IsNull::Yes),
        }
    }

    fn accepts(_: &Type) -> bool {
        true
    }
    fn encode_format(&self, _: &Type) -> Format {
        Format::Text
    }
    tokio_postgres::types::to_sql_checked!();
}

fn identity(name: &str) -> TypeIdentity {
    TypeIdentity {
        schema: "public".into(),
        name: name.into(),
        definition_digest: [4; 32],
    }
}

struct Positive;
impl DomainConstraints for Positive {
    fn check(
        &self,
        id: &TypeIdentity,
        constraint_ref: &str,
        base: &Value,
    ) -> Result<(), MappingError> {
        if id == &identity("n4_domain")
            && constraint_ref == "VALUE > 0"
            && matches!(base, Value::Null | Value::Int4(1..))
        {
            Ok(())
        } else {
            Err(MappingError::OutOfDomain(
                "n4_domain CHECK VALUE > 0".into(),
            ))
        }
    }
}

fn cases() -> Vec<(&'static str, PgType, Vec<&'static str>)> {
    let utf8 = || "UTF8".to_string();
    vec![
        ("boolean", PgType::Bool, vec!["true", "false"]),
        ("smallint", PgType::Int2, vec!["-32768", "32767"]),
        ("integer", PgType::Int4, vec!["-2147483648", "2147483647"]),
        (
            "bigint",
            PgType::Int8,
            vec!["-9223372036854775808", "9223372036854775807"],
        ),
        (
            "numeric",
            PgType::Numeric,
            vec![
                "12345678901234567890.123400",
                "NaN",
                "Infinity",
                "-Infinity",
            ],
        ),
        (
            "real",
            PgType::Float4,
            vec!["1.25", "-0", "NaN", "Infinity", "-Infinity"],
        ),
        (
            "double precision",
            PgType::Float8,
            vec!["1.25", "-0", "NaN", "Infinity", "-Infinity"],
        ),
        (
            "text",
            PgType::Text { encoding: utf8() },
            vec!["", "h\u{e9}llo \u{1f680}"],
        ),
        (
            "varchar(20)",
            PgType::Varchar { encoding: utf8() },
            vec!["", "h\u{e9}llo"],
        ),
        (
            "char(5)",
            PgType::Char { encoding: utf8() },
            vec!["ab   ", "     "],
        ),
        ("bytea", PgType::Bytea, vec!["\\x", "\\x0001ff"]),
        (
            "uuid",
            PgType::Uuid,
            vec!["00112233-4455-6677-8899-aabbccddeeff"],
        ),
        (
            "date",
            PgType::Date,
            vec!["2024-02-29", "infinity", "-infinity"],
        ),
        (
            "time",
            PgType::Time,
            vec!["00:00:00", "24:00:00", "12:34:56.123456"],
        ),
        (
            "timetz",
            PgType::Timetz,
            vec!["12:34:56.123456+02", "00:00:00-05:30"],
        ),
        (
            "timestamp",
            PgType::Timestamp,
            vec!["2024-02-29 12:34:56.123456", "infinity", "-infinity"],
        ),
        (
            "timestamptz",
            PgType::Timestamptz,
            vec!["2024-02-29 12:34:56.123456+00", "infinity", "-infinity"],
        ),
        (
            "interval",
            PgType::Interval,
            vec![
                "1 year 2 mons 3 days 04:05:06.123456",
                "-2 mons -3 days -04:05:06",
            ],
        ),
        (
            "jsonb",
            PgType::Jsonb,
            vec!["{\"b\":2,\"a\":1}", "null", "[]"],
        ),
        (
            "public.n4_enum",
            PgType::Enum {
                type_id: identity("n4_enum"),
                labels: vec![b"red".to_vec(), b"blue".to_vec()],
                encoding: utf8(),
            },
            vec!["red", "blue"],
        ),
        (
            "public.n4_domain",
            PgType::Domain {
                type_id: identity("n4_domain"),
                base: Box::new(PgType::Int4),
                constraint_ref: "VALUE > 0".into(),
            },
            vec!["1", "42", "2147483647"],
        ),
        (
            "integer[]",
            PgType::Array(Box::new(PgType::Int4)),
            vec!["{-2147483648,NULL,2147483647}", "{}"],
        ),
        (
            "bigint[]",
            PgType::Array(Box::new(PgType::Int8)),
            vec!["{-9223372036854775808,NULL,9223372036854775807}", "{}"],
        ),
        (
            "text[]",
            PgType::Array(Box::new(PgType::Text { encoding: utf8() })),
            vec!["{\"\",NULL,\"h\u{e9}llo\"}", "{}"],
        ),
    ]
}

fn assert_sqlstate(error: tokio_postgres::Error, expected: &str) {
    assert_eq!(
        error.code().map(|code| code.code()),
        Some(expected),
        "{error}"
    );
}

async fn round_trip(client: &Client) -> Result<(), TestError> {
    client.batch_execute("SET DateStyle = 'ISO, MDY'; SET IntervalStyle = 'postgres'; SET TIME ZONE 'UTC'; SET extra_float_digits = 3").await?;
    client.batch_execute("CREATE TYPE public.n4_enum AS ENUM ('red', 'blue'); CREATE DOMAIN public.n4_domain AS integer CHECK (VALUE > 0)").await?;
    let cases = cases();
    assert_eq!(cases.len(), 24);
    let mut count = 0;
    for (index, (sql_type, ty, inputs)) in cases.into_iter().enumerate() {
        let mapping = GigglesdbMapping::between(ty.clone(), &ty)?.with_constraints(&Positive);
        assert_eq!(mapping.classification(), Classification::Supported);
        client
            .batch_execute(&format!("CREATE TABLE n4_{index} (v {sql_type})"))
            .await?;
        let insert = client
            .prepare(&format!("INSERT INTO n4_{index} (v) VALUES ($1)"))
            .await?;
        for input in inputs.into_iter().map(Some).chain([None]) {
            let original = Value::parse_pg_text(&ty, input.map(str::as_bytes))?;
            let expected = mapping.forward(&original)?;
            let bind = TextBind(expected.format_pg_text(&ty)?);
            client
                .execute(&insert, &[&bind])
                .await
                .map_err(|error| format!("{sql_type} {input:?} insert: {error}"))?;
            // Simple Query requests the server's text output for the stored type.
            // In particular, SELECT v preserves char padding; v::text would trim it.
            let messages = client
                .simple_query(&format!("SELECT v FROM n4_{index}"))
                .await?;
            let rows: Vec<_> = messages
                .iter()
                .filter_map(|message| match message {
                    SimpleQueryMessage::Row(row) => Some(row),
                    _ => None,
                })
                .collect();
            assert_eq!(rows.len(), 1);
            let read = Value::parse_pg_text(&ty, rows[0].get(0).map(str::as_bytes))?;
            assert_eq!(expected, read, "{sql_type}: {input:?}");
            assert_eq!(mapping.inverse(&read)?, original);
            client
                .batch_execute(&format!("DELETE FROM n4_{index}"))
                .await?;
            count += 1;
        }
        eprintln!("LIVE: {sql_type} identity round trips passed");
    }
    assert_sqlstate(
        client
            .batch_execute("CREATE TABLE n4_json (v json)")
            .await
            .unwrap_err(),
        "42704",
    );
    client
        .batch_execute("CREATE TABLE n4_bounds (v integer[])")
        .await?;
    let insert = client
        .prepare("INSERT INTO n4_bounds (v) VALUES ($1)")
        .await?;
    assert_sqlstate(
        client
            .execute(&insert, &[&TextBind(Some(b"[0:1]={1,2}".to_vec()))])
            .await
            .unwrap_err(),
        "22P02",
    );
    eprintln!(
        "LIVE: 24 supported types, {count} value round trips, 2 server rejection checks passed"
    );
    Ok(())
}

#[tokio::test]
async fn gigglesdb_mapping_live_round_trip() -> Result<(), TestError> {
    let Some(binary) = std::env::var_os("GIGGLES_SERVER_BIN") else {
        eprintln!(
            "SKIPPED: set GIGGLES_SERVER_BIN to a giggles-server executable to run live GigglesDB mapping round trips"
        );
        return Ok(());
    };
    let (server, client, connection) = start_server(binary.into()).await?;
    let result = tokio::time::timeout(Duration::from_secs(120), round_trip(&client)).await;
    drop(client);
    connection.abort();
    let _ = connection.await;
    drop(server);
    result??;
    Ok(())
}
