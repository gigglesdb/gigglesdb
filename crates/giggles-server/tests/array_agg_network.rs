//! Archdocs-shaped array aggregation with PostgreSQL Parse, Describe and text Bind.
#[path = "wire_support/mod.rs"]
#[allow(dead_code)]
mod wire_support;

use giggles_core::{Admin, Limits, roles::NewRole};
use giggles_server::{
    Service,
    auth::{PostgresqlConfig, TlsMode, serve_postgresql},
    clock::Clock,
};
use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{io::AsyncWriteExt, net::TcpStream};
use wire_support::{bind, execute, parse_oids, pg_read, pg_send, pg_sync};

const HINTS: &str = r#"WITH normalized AS (
    SELECT * FROM (VALUES
        (1, 20, 2, 'Later'::text, 'b'::text, $1::text),
        (2, 10, 2, 'Second', 'a', $1::text),
        (3, 10, 1, $2::text, 'a', $1::text),
        (3, 10, 3, 'Third', NULL, $1::text),
        (4, 15, 1, 'Other', 'skip', 'other')
    ) AS entries(id, number, ordinality, heading, outline, key)
)
SELECT (array_agg(heading ORDER BY number, ordinality))[1] AS heading,
       count(DISTINCT id) AS count, array_agg(DISTINCT outline) AS outlines
FROM normalized GROUP BY key HAVING count(DISTINCT id) >= 3"#;

fn frame(frames: &[(u8, Vec<u8>)], tag: u8) -> &[u8] {
    assert!(frames.iter().all(|(tag, _)| *tag != b'E'), "{frames:?}",);
    frames
        .iter()
        .find(|(kind, _)| *kind == tag)
        .unwrap()
        .1
        .as_slice()
}

fn column_oids(mut description: &[u8]) -> Vec<u32> {
    let count = u16::from_be_bytes(description[..2].try_into().unwrap());
    description = &description[2..];
    (0..count)
        .map(|_| {
            let name = description.iter().position(|&byte| byte == 0).unwrap();
            description = &description[name + 1..];
            let oid = u32::from_be_bytes(description[6..10].try_into().unwrap());
            description = &description[18..];
            oid
        })
        .collect()
}

#[tokio::test]
async fn archdocs_hints_describe_and_execute_with_text_parameters() {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path =
        std::env::temp_dir().join(format!("array-agg-network-{}-{stamp}", std::process::id()));
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
    let server = serve_postgresql(config.listen, service, Arc::new(config.build().unwrap()))
        .await
        .unwrap();
    let mut stream = TcpStream::connect(server.address).await.unwrap();
    let mut startup = 196608u32.to_be_bytes().to_vec();
    startup.extend(b"user\0owner\0database\0postgres\0\0");
    stream.write_u32((startup.len() + 4) as u32).await.unwrap();
    stream.write_all(&startup).await.unwrap();
    loop {
        let (tag, body) = pg_read(&mut stream).await;
        assert_ne!(tag, b'E', "{body:?}");
        if tag == b'Z' {
            break;
        }
    }

    pg_send(&mut stream, b'P', &parse_oids("hints", HINTS, &[25, 25])).await;
    pg_send(&mut stream, b'D', b"Shints\0").await;
    let frames = pg_sync(&mut stream).await;
    assert_eq!(frame(&frames, b't'), &[0, 2, 0, 0, 0, 25, 0, 0, 0, 25]);
    assert_eq!(column_oids(frame(&frames, b'T')), vec![25, 20, 1009]);

    let bound = bind("", "hints", &[Some("shared"), Some("Shared")], false);
    pg_send(&mut stream, b'B', &bound).await;
    pg_send(&mut stream, b'E', &execute("", 0)).await;
    let frames = pg_sync(&mut stream).await;
    let mut expected = 3u16.to_be_bytes().to_vec();
    for field in ["Shared", "3", "{a,b,NULL}"] {
        expected.extend((field.len() as i32).to_be_bytes());
        expected.extend(field.as_bytes());
    }
    assert_eq!(frame(&frames, b'D'), expected);
    assert_eq!(frame(&frames, b'C'), b"SELECT 1\0");

    let arrays = "SELECT array_agg($1::varchar), array_agg(1::smallint), array_agg(1),
        array_agg(1::bigint), array_agg(1::numeric), array_agg(true),
        array_agg('2024-01-02'::date), array_agg('2024-01-02'::timestamp),
        array_agg('2024-01-02 00:00:00+00'::timestamptz), array_agg('{}'::json),
        array_agg('{}'::jsonb), array_agg('00000000-0000-0000-0000-000000000001'::uuid)";
    pg_send(&mut stream, b'P', &parse_oids("arrays", arrays, &[25])).await;
    pg_send(&mut stream, b'D', b"Sarrays\0").await;
    let frames = pg_sync(&mut stream).await;
    assert_eq!(
        column_oids(frame(&frames, b'T')),
        vec![
            1015, 1005, 1007, 1016, 1231, 1000, 1182, 1115, 1185, 199, 3807, 2951
        ]
    );
    pg_send(
        &mut stream,
        b'B',
        &bind("", "arrays", &[Some("text")], true),
    )
    .await;
    pg_send(&mut stream, b'E', &execute("", 0)).await;
    let frames = pg_sync(&mut stream).await;
    assert_eq!(&frame(&frames, b'D')[..2], &12u16.to_be_bytes());

    let create = "CREATE TABLE array_dst (a text[])";
    pg_send(&mut stream, b'P', &parse_oids("create_array", create, &[])).await;
    pg_send(&mut stream, b'B', &bind("", "create_array", &[], false)).await;
    pg_send(&mut stream, b'E', &execute("", 0)).await;
    let frames = pg_sync(&mut stream).await;
    assert_eq!(frame(&frames, b'C'), b"CREATE TABLE\0");
    let insert = "INSERT INTO array_dst SELECT array_agg($1::varchar)";
    pg_send(
        &mut stream,
        b'P',
        &parse_oids("insert_array", insert, &[25]),
    )
    .await;
    pg_send(
        &mut stream,
        b'B',
        &bind("", "insert_array", &[Some("a,b")], false),
    )
    .await;
    pg_send(&mut stream, b'E', &execute("", 0)).await;
    let frames = pg_sync(&mut stream).await;
    assert_eq!(frame(&frames, b'C'), b"INSERT 0 1\0");

    let read = "SELECT a FROM array_dst";
    pg_send(&mut stream, b'P', &parse_oids("read_array", read, &[])).await;
    pg_send(&mut stream, b'B', &bind("", "read_array", &[], false)).await;
    pg_send(&mut stream, b'D', b"P\0").await;
    pg_send(&mut stream, b'E', &execute("", 0)).await;
    let frames = pg_sync(&mut stream).await;
    assert_eq!(column_oids(frame(&frames, b'T')), vec![1009]);
    assert_eq!(frame(&frames, b'D'), b"\0\x01\0\0\0\x07{\"a,b\"}");

    drop(stream);
    server.shutdown().await.unwrap();
    std::fs::remove_dir_all(path).unwrap();
}
