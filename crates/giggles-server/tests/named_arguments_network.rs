//! Exercise unnamed PostgreSQL extended queries with unspecified OIDs.
use giggles_core::{Admin, Limits, roles::NewRole};
use giggles_server::{
    Service,
    auth::{PostgresqlConfig, TlsMode, serve_postgresql},
    clock::Clock,
};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

type Frame = (u8, Vec<u8>);

async fn send(stream: &mut TcpStream, tag: u8, body: &[u8]) {
    stream.write_u8(tag).await.unwrap();
    stream.write_u32((body.len() + 4) as u32).await.unwrap();
    stream.write_all(body).await.unwrap();
}

async fn until_ready(stream: &mut TcpStream) -> Vec<Frame> {
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut frames = Vec::new();
        loop {
            let tag = stream.read_u8().await.unwrap();
            let length = stream.read_u32().await.unwrap();
            assert!((4..=1024 * 1024).contains(&length));
            let mut body = vec![0; length as usize - 4];
            stream.read_exact(&mut body).await.unwrap();
            if tag == b'Z' {
                return frames;
            }
            frames.push((tag, body));
        }
    })
    .await
    .expect("pgwire response timeout")
}

async fn extended(
    stream: &mut TcpStream,
    sql: &str,
    format: u16,
    parameters: &[&[u8]],
) -> Vec<Frame> {
    let mut parse = vec![0];
    parse.extend(sql.as_bytes());
    parse.push(0);
    parse.extend((parameters.len() as u16).to_be_bytes());
    for _ in parameters {
        parse.extend(0u32.to_be_bytes());
    }
    send(stream, b'P', &parse).await;
    send(stream, b'D', &[b'S', 0]).await;
    let mut bind = vec![0, 0];
    bind.extend(1u16.to_be_bytes());
    bind.extend(format.to_be_bytes());
    bind.extend((parameters.len() as u16).to_be_bytes());
    for parameter in parameters {
        bind.extend((parameter.len() as u32).to_be_bytes());
        bind.extend(*parameter);
    }
    bind.extend(0u16.to_be_bytes()); // Text result format.
    send(stream, b'B', &bind).await;
    send(stream, b'E', &[0; 5]).await;
    send(stream, b'S', &[]).await;
    until_ready(stream).await
}

fn row(frames: &[Frame], parameter_types: &[u32], value: &str) {
    assert!(!frames.iter().any(|(tag, _)| *tag == b'E'), "{frames:?}");
    assert!(frames.iter().any(|(tag, _)| *tag == b'1'), "{frames:?}");
    assert!(frames.iter().any(|(tag, _)| *tag == b'2'), "{frames:?}");
    let expected: Vec<_> = (parameter_types.len() as u16)
        .to_be_bytes()
        .into_iter()
        .chain(parameter_types.iter().flat_map(|oid| oid.to_be_bytes()))
        .collect();
    assert!(frames.contains(&(b't', expected)), "{frames:?}");
    let data = &frames
        .iter()
        .find(|(tag, _)| *tag == b'D')
        .expect("DataRow")
        .1;
    assert_eq!(data[..2], 1u16.to_be_bytes());
    assert_eq!(data[2..6], (value.len() as u32).to_be_bytes());
    assert_eq!(&data[6..], value.as_bytes());
}

fn error_field(frames: &[Frame], field: u8) -> Option<&str> {
    let (_, response) = frames
        .iter()
        .find(|(tag, _)| *tag == b'E')
        .expect("ErrorResponse");
    response.split(|&byte| byte == 0).find_map(|part| {
        (part.first() == Some(&field)).then(|| std::str::from_utf8(&part[1..]).unwrap())
    })
}

fn assert_error(frames: &[Frame], sql: &str, code: &str, message: &str, hint: Option<&str>) {
    assert_eq!(error_field(frames, b'C'), Some(code), "{sql}");
    assert_eq!(error_field(frames, b'M'), Some(message), "{sql}");
    assert_eq!(error_field(frames, b'H'), hint, "{sql}");
}

#[tokio::test]
async fn named_arguments_with_unspecified_parse_oids() {
    let path = std::env::temp_dir().join(format!("named-arguments-wire-{}", std::process::id()));
    #[cfg(feature = "engine-rocksdb")]
    let admin = Admin::create_engine(&path, Limits::default(), giggles_core::Engine::V2).unwrap();
    #[cfg(not(feature = "engine-rocksdb"))]
    let admin = Admin::create(&path, Limits::default()).unwrap();
    admin
        .roles()
        .create(NewRole {
            name: "owner".into(),
            superuser: true,
            login: true,
            ..Default::default()
        })
        .unwrap();
    let mut session = admin.sql_session();
    for sql in [
        r#"CREATE TABLE "Interview" ("id" int PRIMARY KEY, "endedAt" timestamptz, "retentionPurgeAt" timestamptz)"#,
        r#"INSERT INTO "Interview" ("id", "endedAt") VALUES (1, '2024-01-01 00:00:00+00')"#,
    ] {
        session.execute(sql, &[], None).unwrap();
    }
    session
        .execute(
            "CREATE TABLE named_defaults (id int PRIMARY KEY, d interval)",
            &[],
            None,
        )
        .unwrap();
    drop(session);
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
    let mut stream = TcpStream::connect(running.address).await.unwrap();
    let mut startup = 196608u32.to_be_bytes().to_vec();
    startup.extend(b"user\0owner\0database\0postgres\0\0");
    stream.write_u32((startup.len() + 4) as u32).await.unwrap();
    stream.write_all(&startup).await.unwrap();
    assert!(
        until_ready(&mut stream)
            .await
            .iter()
            .any(|(tag, _)| *tag == b'R')
    );

    let update = r#"UPDATE "Interview" SET "retentionPurgeAt" = "endedAt" + make_interval(days => $1::int) WHERE "id" = $2 RETURNING "retentionPurgeAt""#;
    let days = 7i32.to_be_bytes();
    let id = 1i32.to_be_bytes();
    for (format, parameters) in [
        (0, [b"7".as_slice(), b"1".as_slice()]),
        (1, [&days[..], &id[..]]),
    ] {
        row(
            &extended(&mut stream, update, format, &parameters).await,
            &[23, 23],
            "2024-01-08 00:00:00+00",
        );
        row(
            &extended(
                &mut stream,
                "SELECT make_interval(days => $1)",
                format,
                &parameters[..1],
            )
            .await,
            &[23],
            "7 days",
        );
    }
    let frames = extended(
        &mut stream,
        "SELECT make_interval(days => 1, days => 2)",
        0,
        &[],
    )
    .await;
    assert_eq!(error_field(&frames, b'C'), Some("42601"));
    assert_eq!(
        error_field(&frames, b'M'),
        Some("argument name \"days\" used more than once")
    );
    let frames = extended(&mut stream, "SELECT make_interval(days => 1.5)", 0, &[]).await;
    assert_eq!(error_field(&frames, b'C'), Some("42883"));
    assert_eq!(
        error_field(&frames, b'M'),
        Some("function make_interval(days => numeric) does not exist")
    );
    assert_eq!(
        error_field(&frames, b'H'),
        Some(
            "No function matches the given name and argument types. You might need to add explicit type casts."
        )
    );
    row(
        &extended(
            &mut stream,
            "SELECT option_value FROM pg_options_to_table(OPTIONS_ARRAY := ARRAY['foo=bar'])",
            0,
            &[],
        )
        .await,
        &[],
        "bar",
    );
    let undefined_hint = "No function matches the given name and argument types. You might need to add explicit type casts.";
    let ambiguous_hint =
        "Could not choose a best candidate function. You might need to add explicit type casts.";
    let unsupported = "unsupported SQL feature or parameter type in this server profile";
    let generic = "request rejected; consult documented code";
    for (sql, code, message, hint) in [
        (
            r#"SELECT jsonb_path_exists(target => '{"a":1}', path => '$.a')"#,
            "0A000",
            unsupported,
            None,
        ),
        (
            "SELECT random_normal(mean => 0, stddev => 0)",
            "0A000",
            unsupported,
            None,
        ),
        (
            r#"SELECT jsonb_extract_path('{"a":1}', VARIADIC path_elems => ARRAY['a'])"#,
            "0A000",
            unsupported,
            None,
        ),
        (
            "SELECT * FROM make_interval(days => 1)",
            "42883",
            generic,
            None,
        ),
        ("SELECT * FROM make_interval(1)", "42883", generic, None),
        (
            r#"SELECT * FROM jsonb_path_query(target => '{"a":1}', path => '$.a')"#,
            "42883",
            generic,
            None,
        ),
        (
            "SELECT nosuch.make_interval(days => 3)",
            "3F000",
            generic,
            None,
        ),
        ("SELECT nosuch.make_interval(3)", "3F000", generic, None),
        (
            "SELECT random(min => 'a', max => 'b')",
            "42725",
            "function random(min => unknown, max => unknown) is not unique",
            Some(ambiguous_hint),
        ),
        (
            "PREPARE q4 AS SELECT random(min => $1, max => $2)",
            "42725",
            "function random(min => unknown, max => unknown) is not unique",
            Some(ambiguous_hint),
        ),
        (
            "SELECT * FROM pg_options_to_table(foo => ARRAY['a=b'])",
            "42883",
            "function pg_options_to_table(foo => text[]) does not exist",
            Some(undefined_hint),
        ),
        (
            r#"SELECT * FROM pg_options_to_table("OPTIONS_ARRAY" => ARRAY['a=b'])"#,
            "42883",
            "function pg_options_to_table(OPTIONS_ARRAY => text[]) does not exist",
            Some(undefined_hint),
        ),
        (
            "SELECT * FROM pg_catalog.pg_options_to_table(foo => ARRAY['a=b'])",
            "42883",
            "function pg_catalog.pg_options_to_table(foo => text[]) does not exist",
            Some(undefined_hint),
        ),
        (
            "CREATE TABLE with_default (id int PRIMARY KEY, d interval DEFAULT make_interval(days => 2::int))",
            "0A000",
            unsupported,
            None,
        ),
        (
            "CREATE TABLE with_check (id int PRIMARY KEY, d interval CHECK (d >= make_interval(days => 1)))",
            "0A000",
            unsupported,
            None,
        ),
        (
            "CREATE TABLE with_generated (id int PRIMARY KEY, g interval GENERATED ALWAYS AS (make_interval(days => id)) STORED)",
            "0A000",
            unsupported,
            None,
        ),
        (
            "CREATE INDEX named_expression ON named_defaults ((make_interval(days => id)))",
            "0A000",
            unsupported,
            None,
        ),
        (
            "CREATE INDEX named_partial ON named_defaults (id) WHERE d >= make_interval(days => 1)",
            "0A000",
            unsupported,
            None,
        ),
        (
            "CREATE VIEW named_list AS SELECT make_interval(days => id) FROM named_defaults",
            "0A000",
            unsupported,
            None,
        ),
        (
            "CREATE VIEW named_view AS SELECT id FROM named_defaults WHERE d >= make_interval(days := 1)",
            "0A000",
            unsupported,
            None,
        ),
    ] {
        let mut query = sql.as_bytes().to_vec();
        query.push(0);
        send(&mut stream, b'Q', &query).await;
        assert_error(&until_ready(&mut stream).await, sql, code, message, hint);
    }
    for (sql, params, code, message, hint) in [
        (
            "SELECT random(min => $1, max => $2)",
            &[b"a".as_slice(), b"b".as_slice()][..],
            "42725",
            "function random(min => unknown, max => unknown) is not unique",
            ambiguous_hint,
        ),
        (
            "SELECT * FROM pg_options_to_table(foo => ARRAY['a=b'])",
            &[],
            "42883",
            "function pg_options_to_table(foo => text[]) does not exist",
            undefined_hint,
        ),
    ] {
        let frames = extended(&mut stream, sql, 0, params).await;
        assert_error(&frames, sql, code, message, Some(hint));
    }
    drop(stream);
    running.shutdown().await.unwrap();
    std::fs::remove_dir_all(path).unwrap();
}
