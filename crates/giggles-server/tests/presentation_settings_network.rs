use super::session_settings_network::{Fixture, wire_cycle, wire_query};
use futures_core::Stream;
use std::future::poll_fn;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use tokio_postgres::{
    Client, SimpleQueryMessage,
    types::{FromSql, Type},
};

const SELECT: &str = "SELECT d, ts, tz, b FROM presentation_data ORDER BY id";
const INSERT: &str = r"CREATE TABLE presentation_data (
    id integer PRIMARY KEY, d date, ts timestamp, tz timestamptz, b bytea);
    INSERT INTO presentation_data VALUES
    (1, '2024-01-02', '2024-01-02 03:04:05.678', '2024-01-02 03:04:05.678+00', '\x00ff5c41270a7f80'),
    (2, '0044-03-15 BC', '0044-03-15 03:04:05 BC', '0044-03-15 03:04:05+00 BC', '\x'),
    (3, 'infinity', 'infinity', 'infinity', NULL),
    (4, '-infinity', '-infinity', '-infinity', NULL);";

async fn text_rows(client: &Client, sql: &str) -> Vec<Vec<Option<String>>> {
    client
        .simple_query(sql)
        .await
        .unwrap()
        .into_iter()
        .filter_map(|message| {
            let SimpleQueryMessage::Row(row) = message else {
                return None;
            };
            Some(
                (0..row.len())
                    .map(|index| row.get(index).map(str::to_owned))
                    .collect(),
            )
        })
        .collect()
}

struct RawBinary(Vec<u8>);

impl<'a> FromSql<'a> for RawBinary {
    fn from_sql(_: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Ok(Self(raw.to_vec()))
    }

    fn accepts(ty: &Type) -> bool {
        [Type::DATE, Type::TIMESTAMP, Type::TIMESTAMPTZ, Type::BYTEA].contains(ty)
    }
}

async fn binary_rows(client: &Client) -> Vec<Vec<Option<Vec<u8>>>> {
    client
        .query(SELECT, &[])
        .await
        .unwrap()
        .iter()
        .map(|row| {
            assert_eq!(
                row.columns()
                    .iter()
                    .map(|column| column.type_())
                    .collect::<Vec<_>>(),
                [
                    &Type::DATE,
                    &Type::TIMESTAMP,
                    &Type::TIMESTAMPTZ,
                    &Type::BYTEA
                ]
            );
            (0..row.len())
                .map(|index| row.get::<_, Option<RawBinary>>(index).map(|raw| raw.0))
                .collect()
        })
        .collect()
}

#[tokio::test]
async fn datetime_and_bytea_text_follow_session_settings_while_binary_is_identical() {
    let fixture = Fixture::new().await;
    let client = fixture.client().await;
    client.batch_execute(INSERT).await.unwrap();
    let binary = binary_rows(&client).await;
    assert_eq!(
        binary[0][3].as_deref(),
        Some(&[0, 255, 92, 65, 39, 10, 127, 128][..])
    );
    for (style, ad, bc) in [
        (
            "ISO, MDY",
            [
                "2024-01-02",
                "2024-01-02 03:04:05.678",
                "2024-01-02 03:04:05.678+00",
            ],
            [
                "0044-03-15 BC",
                "0044-03-15 03:04:05 BC",
                "0044-03-15 03:04:05+00 BC",
            ],
        ),
        (
            "ISO, DMY",
            [
                "2024-01-02",
                "2024-01-02 03:04:05.678",
                "2024-01-02 03:04:05.678+00",
            ],
            [
                "0044-03-15 BC",
                "0044-03-15 03:04:05 BC",
                "0044-03-15 03:04:05+00 BC",
            ],
        ),
        (
            "SQL, MDY",
            [
                "01/02/2024",
                "01/02/2024 03:04:05.678",
                "01/02/2024 03:04:05.678 UTC",
            ],
            [
                "03/15/0044 BC",
                "03/15/0044 03:04:05 BC",
                "03/15/0044 03:04:05 UTC BC",
            ],
        ),
        (
            "SQL, YMD",
            [
                "01/02/2024",
                "01/02/2024 03:04:05.678",
                "01/02/2024 03:04:05.678 UTC",
            ],
            [
                "03/15/0044 BC",
                "03/15/0044 03:04:05 BC",
                "03/15/0044 03:04:05 UTC BC",
            ],
        ),
        (
            "SQL, DMY",
            [
                "02/01/2024",
                "02/01/2024 03:04:05.678",
                "02/01/2024 03:04:05.678 UTC",
            ],
            [
                "15/03/0044 BC",
                "15/03/0044 03:04:05 BC",
                "15/03/0044 03:04:05 UTC BC",
            ],
        ),
        (
            "Postgres, MDY",
            [
                "01-02-2024",
                "Tue Jan 02 03:04:05.678 2024",
                "Tue Jan 02 03:04:05.678 2024 UTC",
            ],
            [
                "03-15-0044 BC",
                "Fri Mar 15 03:04:05 0044 BC",
                "Fri Mar 15 03:04:05 0044 UTC BC",
            ],
        ),
        (
            "Postgres, YMD",
            [
                "01-02-2024",
                "Tue Jan 02 03:04:05.678 2024",
                "Tue Jan 02 03:04:05.678 2024 UTC",
            ],
            [
                "03-15-0044 BC",
                "Fri Mar 15 03:04:05 0044 BC",
                "Fri Mar 15 03:04:05 0044 UTC BC",
            ],
        ),
        (
            "Postgres, DMY",
            [
                "02-01-2024",
                "Tue 02 Jan 03:04:05.678 2024",
                "Tue 02 Jan 03:04:05.678 2024 UTC",
            ],
            [
                "15-03-0044 BC",
                "Fri 15 Mar 03:04:05 0044 BC",
                "Fri 15 Mar 03:04:05 0044 UTC BC",
            ],
        ),
        (
            "German, DMY",
            [
                "02.01.2024",
                "02.01.2024 03:04:05.678",
                "02.01.2024 03:04:05.678 UTC",
            ],
            [
                "15.03.0044 BC",
                "15.03.0044 03:04:05 BC",
                "15.03.0044 03:04:05 UTC BC",
            ],
        ),
        (
            "German, MDY",
            [
                "02.01.2024",
                "02.01.2024 03:04:05.678",
                "02.01.2024 03:04:05.678 UTC",
            ],
            [
                "15.03.0044 BC",
                "15.03.0044 03:04:05 BC",
                "15.03.0044 03:04:05 UTC BC",
            ],
        ),
    ] {
        client
            .batch_execute(&format!(
                "SET DateStyle='{style}'; SET bytea_output='ESCAPE'"
            ))
            .await
            .unwrap();
        let text = text_rows(&client, SELECT).await;
        assert_eq!(text[0][..3], ad.map(|s| Some(s.to_owned())), "{style}");
        assert_eq!(text[1][..3], bc.map(|s| Some(s.to_owned())), "{style}");
        assert_eq!(
            text[2][..3],
            ["infinity"; 3].map(|s| Some(s.to_owned())),
            "{style}"
        );
        assert_eq!(
            text[3][..3],
            ["-infinity"; 3].map(|s| Some(s.to_owned())),
            "{style}"
        );
        assert_eq!(text[0][3].as_deref(), Some(r"\000\377\\A'\012\177\200"));
        assert_eq!(text[1][3].as_deref(), Some(""));
        assert_eq!(binary_rows(&client).await, binary, "{style}");
    }
    let text = text_rows(
        &client,
        r"SELECT b::text, b||'\x41'::bytea, encode(b,'escape')
        FROM presentation_data WHERE id <= 2 ORDER BY id",
    )
    .await;
    assert_eq!(
        text[0],
        [
            r"\000\377\\A'\012\177\200",
            r"\000\377\\A'\012\177\200A",
            "\\000\\377\\\\A'\n\x7f\\200",
        ]
        .map(|s| Some(s.to_owned()))
    );
    assert_eq!(
        text[1],
        ["", "A", ""].map(|s| Some(s.to_owned()))
    );
    let decoded = text_rows(&client, "SELECT decode('ffee80','hex')").await;
    assert_eq!(decoded[0][0].as_deref(), Some(r"\377\356\200"));
    client.batch_execute("RESET bytea_output").await.unwrap();
    assert_eq!(
        text_rows(&client, "SELECT b::text FROM presentation_data ORDER BY id").await[0][0]
            .as_deref(),
        Some(r"\x00ff5c41270a7f80")
    );
    drop(client);
    fixture.stop().await;
}

async fn copy_out(client: &Client, sql: &str) -> String {
    let stream = client.copy_out(sql).await.unwrap();
    tokio::pin!(stream);
    let mut output = Vec::new();
    while let Some(bytes) = poll_fn(|cx| stream.as_mut().poll_next(cx)).await {
        output.extend_from_slice(&bytes.unwrap());
    }
    String::from_utf8(output).unwrap()
}

#[tokio::test]
async fn settings_apply_to_copy_text_and_csv() {
    let fixture = Fixture::new().await;
    let client = fixture.client().await;
    client.batch_execute(r"CREATE TABLE copy_types (id integer PRIMARY KEY, d date, ts timestamp, tz timestamptz, b bytea);
        INSERT INTO copy_types VALUES (1, '2024-01-02','2024-01-02 03:04:05','2024-01-02 03:04:05+00','\x00ff5c41270a7f80'),
        (2,NULL,NULL,NULL,'\x');
        SET timezone='America/Los_Angeles'; SET DateStyle='SQL, DMY';").await.unwrap();
    assert_eq!(
        copy_out(&client, "COPY copy_types TO STDOUT").await,
        "1\t02/01/2024\t02/01/2024 03:04:05\t01/01/2024 19:04:05 PST\t\\\\x00ff5c41270a7f80\n2\t\\N\t\\N\t\\N\t\\\\x\n"
    );
    assert_eq!(
        copy_out(&client, "COPY copy_types TO STDOUT (FORMAT csv)").await,
        "1,02/01/2024,02/01/2024 03:04:05,01/01/2024 19:04:05 PST,\\x00ff5c41270a7f80\n2,,,,\\x\n"
    );
    client
        .batch_execute("SET DateStyle='Postgres, MDY'")
        .await
        .unwrap();
    assert_eq!(
        copy_out(&client, "COPY copy_types TO STDOUT").await,
        "1\t01-02-2024\tTue Jan 02 03:04:05 2024\tMon Jan 01 19:04:05 2024 PST\t\\\\x00ff5c41270a7f80\n2\t\\N\t\\N\t\\N\t\\\\x\n"
    );
    client
        .batch_execute("SET bytea_output='escape'")
        .await
        .unwrap();
    assert_eq!(
        copy_out(&client, "COPY copy_types(id,b) TO STDOUT").await,
        "1\t\\\\000\\\\377\\\\\\\\A'\\\\012\\\\177\\\\200\n2\t\n"
    );
    assert_eq!(
        copy_out(&client, "COPY copy_types(id,b) TO STDOUT (FORMAT csv)").await,
        "1,\\000\\377\\\\A'\\012\\177\\200\n2,\"\"\n"
    );

    let mut wire = startup(&fixture, "SQL, DMY").await;
    assert!(
        wire_cycle(&mut wire)
            .await
            .iter()
            .any(|(tag, _)| *tag == b'R')
    );
    let create = wire_query(
        &mut wire,
        "CREATE TABLE copy_input (id integer PRIMARY KEY, d date, ts timestamp, tz timestamptz)",
    )
    .await;
    assert!(create.iter().all(|(tag, _)| *tag != b'E'));
    let mut sql = b"COPY copy_input FROM STDIN".to_vec();
    sql.push(0);
    send(&mut wire, b'Q', &sql).await;
    assert_eq!(read_frame(&mut wire).await.0, b'G');
    send(
        &mut wire,
        b'd',
        b"1\t01/02/2024\t01/02/2024 03:04:05\t01/02/2024 03:04:05-08\n",
    )
    .await;
    send(&mut wire, b'c', &[]).await;
    let copied = wire_cycle(&mut wire).await;
    assert!(
        copied
            .iter()
            .any(|(tag, bytes)| *tag == b'C' && bytes == b"COPY 1\0"),
        "{copied:?}"
    );
    client
        .batch_execute("SET DateStyle='ISO, MDY'")
        .await
        .unwrap();
    assert_eq!(
        text_rows(&client, "SELECT d, ts, tz FROM copy_input").await[0],
        [
            "2024-02-01",
            "2024-02-01 03:04:05",
            "2024-02-01 03:04:05-08"
        ]
        .map(|s| Some(s.to_owned()))
    );
    drop(wire);
    drop(client);
    fixture.stop().await;
}

async fn startup(fixture: &Fixture, style: &str) -> TcpStream {
    let mut wire = TcpStream::connect(fixture.pg.address).await.unwrap();
    let mut packet = 196608u32.to_be_bytes().to_vec();
    packet.extend(format!("user\0owner\0database\0postgres\0DateStyle\0{style}\0\0").as_bytes());
    wire.write_u32((packet.len() + 4) as u32).await.unwrap();
    wire.write_all(&packet).await.unwrap();
    wire
}

async fn send(wire: &mut TcpStream, tag: u8, body: &[u8]) {
    let mut packet = Vec::new();
    giggles_server::pgwire::frame(tag, body, &mut packet).unwrap();
    wire.write_all(&packet).await.unwrap();
}

async fn read_frame(wire: &mut TcpStream) -> (u8, Vec<u8>) {
    let tag = wire.read_u8().await.unwrap();
    let len = wire.read_u32().await.unwrap();
    let mut body = vec![0; len as usize - 4];
    wire.read_exact(&mut body).await.unwrap();
    (tag, body)
}

fn status(frames: &[(u8, Vec<u8>)], style: &str) -> bool {
    frames.iter().any(|(tag, bytes)| {
        *tag == b'S' && bytes.as_slice() == format!("DateStyle\0{style}\0").as_bytes()
    })
}

#[tokio::test]
async fn startup_transactions_and_text_bind_use_the_appropriate_date_order() {
    let fixture = Fixture::new().await;
    let mut wire = startup(&fixture, "SQL, DMY").await;
    assert!(status(&wire_cycle(&mut wire).await, "SQL, DMY"));
    assert!(status(
        &wire_query(&mut wire, "SET DateStyle='ISO, MDY'").await,
        "ISO, MDY"
    ));
    assert!(status(
        &wire_query(&mut wire, "BEGIN; SET LOCAL DateStyle='German'").await,
        "German, DMY"
    ));
    assert!(status(&wire_query(&mut wire, "ROLLBACK").await, "ISO, MDY"));
    assert!(status(
        &wire_query(&mut wire, "SET DateStyle TO DEFAULT").await,
        "SQL, DMY"
    ));
    let mut invalid = startup(&fixture, "ISO, SQL").await;
    let (tag, error) = read_frame(&mut invalid).await;
    assert_eq!(tag, b'E');
    let error = std::str::from_utf8(&error).unwrap();
    assert!(error.contains("SFATAL\0"));
    assert!(error.contains("C22023\0"));
    assert!(error.contains("Minvalid value for parameter \"DateStyle\": \"ISO, SQL\"\0"));
    assert!(error.contains("DConflicting \"DateStyle\" specifications.\0"));

    let mut parse = b"prepared\0SELECT $1, $1::text\0".to_vec();
    parse.extend(1u16.to_be_bytes());
    parse.extend(1082u32.to_be_bytes());
    send(&mut wire, b'P', &parse).await;
    send(&mut wire, b'S', &[]).await;
    assert!(
        wire_cycle(&mut wire)
            .await
            .iter()
            .any(|(tag, _)| *tag == b'1')
    );
    for (style, output) in [("SQL, DMY", "01/02/2024"), ("ISO, MDY", "2024-01-02")] {
        wire_query(&mut wire, &format!("SET DateStyle='{style}'")).await;
        let mut bind = b"\0prepared\0".to_vec();
        bind.extend(0u16.to_be_bytes());
        bind.extend(1u16.to_be_bytes());
        bind.extend(10u32.to_be_bytes());
        bind.extend(b"01/02/2024");
        bind.extend(0u16.to_be_bytes());
        send(&mut wire, b'B', &bind).await;
        send(&mut wire, b'E', &[0; 5]).await;
        send(&mut wire, b'S', &[]).await;
        let result = wire_cycle(&mut wire).await;
        let mut expected = 2u16.to_be_bytes().to_vec();
        for _ in 0..2 {
            expected.extend(10u32.to_be_bytes());
            expected.extend(output.as_bytes());
        }
        assert!(
            result
                .iter()
                .any(|(tag, bytes)| *tag == b'D' && *bytes == expected),
            "{style}: {result:?}"
        );
    }
    let client = fixture.client().await;
    let prepared = client
        .prepare("SELECT '01/02/2024'::date::text, '01/02/2024'::text::date::text")
        .await
        .unwrap();
    client
        .batch_execute("SET DateStyle='SQL, DMY'")
        .await
        .unwrap();
    let row = client.query_one(&prepared, &[]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "02/01/2024");
    assert_eq!(row.get::<_, String>(1), "01/02/2024");
    let inventory = text_rows(
        &client,
        "SELECT setting, reset_val, boot_val FROM pg_settings WHERE name='DateStyle'",
    )
    .await;
    assert_eq!(
        inventory[0],
        ["SQL, DMY", "ISO, MDY", "ISO, MDY"].map(|s| Some(s.to_owned()))
    );
    let date = text_rows(
        &client,
        "SELECT set_config('DateStyle', 'German', false); SELECT current_setting('DateStyle')",
    )
    .await;
    assert_eq!(date, vec![vec![Some("German, DMY".into())]; 2]);
    let bytes = text_rows(&client, "SELECT set_config('bytea_output','escape',false); SELECT setting FROM pg_settings WHERE name='bytea_output'").await;
    assert_eq!(bytes, vec![vec![Some("escape".into())]; 2]);
    let error = client
        .simple_query("SET DateStyle='ISO, SQL'")
        .await
        .unwrap_err();
    let error = error.as_db_error().unwrap();
    assert_eq!(error.code().code(), "22023");
    assert_eq!(
        error.message(),
        r#"invalid value for parameter "DateStyle": "ISO, SQL""#
    );
    assert_eq!(
        error.detail(),
        Some(r#"Conflicting "DateStyle" specifications."#)
    );
    let error = client
        .query("SET bytea_output='base64'", &[])
        .await
        .unwrap_err();
    let error = error.as_db_error().unwrap();
    assert_eq!(error.code().code(), "22023");
    assert_eq!(
        error.message(),
        r#"invalid value for parameter "bytea_output": "base64""#
    );
    assert_eq!(error.hint(), Some("Available values: escape, hex."));
    drop(invalid);
    drop(wire);
    drop(client);
    fixture.stop().await;
}

#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn named_refresh_and_unnamed_reuse_keep_separate_literal_orders() {
    let fixture = Fixture::with_engine(giggles_core::Engine::V2).await;
    let client = fixture.client().await;
    let sql = "SELECT '01/02/2024'::date::text, '01/02/2024'::text::date::text";
    let named = client.prepare(sql).await.unwrap();
    assert_eq!(
        text_rows(&client, sql).await[0],
        [Some("2024-01-02".into()), Some("2024-01-02".into())]
    );
    client
        .batch_execute(
            "SET DateStyle='SQL, DMY'; CREATE TABLE presentation_refresh (id integer PRIMARY KEY)",
        )
        .await
        .unwrap();
    let named_row = client.query_one(&named, &[]).await.unwrap();
    assert_eq!(named_row.get::<_, String>(0), "02/01/2024");
    assert_eq!(named_row.get::<_, String>(1), "01/02/2024");
    assert_eq!(
        text_rows(&client, sql).await[0],
        [Some("01/02/2024".into()), Some("01/02/2024".into())]
    );
    drop(client);
    fixture.stop().await;
}

async fn assert_wire_text(
    wire: &mut TcpStream,
    sql: &str,
    parameter: Option<(u32, &str)>,
    expected: &str,
) {
    let mut parse = format!("\0{sql}\0").into_bytes();
    let mut bind = b"\0\0\0\0".to_vec();
    if let Some((oid, value)) = parameter {
        parse.extend(1u16.to_be_bytes());
        parse.extend(oid.to_be_bytes());
        bind.extend(1u16.to_be_bytes());
        bind.extend((value.len() as u32).to_be_bytes());
        bind.extend(value.as_bytes());
    } else {
        parse.extend(0u16.to_be_bytes());
        bind.extend(0u16.to_be_bytes());
    }
    bind.extend(0u16.to_be_bytes());
    send(wire, b'P', &parse).await;
    send(wire, b'B', &bind).await;
    send(wire, b'E', &[0; 5]).await;
    send(wire, b'S', &[]).await;
    let frames = wire_cycle(wire).await;
    let mut body = 1u16.to_be_bytes().to_vec();
    body.extend((expected.len() as u32).to_be_bytes());
    body.extend(expected.as_bytes());
    let data = frames
        .iter()
        .filter(|(tag, _)| *tag == b'D')
        .map(|(_, bytes)| bytes);
    assert!(data.eq([&body]), "{sql}: {parameter:?}: {frames:?}");
}

#[tokio::test]
async fn date_order_applies_to_wire_comparisons_joins_and_mutations() {
    #[cfg(feature = "engine-rocksdb")]
    let fixture = Fixture::with_engine(giggles_core::Engine::V2).await;
    #[cfg(not(feature = "engine-rocksdb"))]
    let fixture = Fixture::new().await;
    let client = fixture.client().await;
    client
        .batch_execute(
            "CREATE TABLE date_lookup (id integer PRIMARY KEY, d date, marker integer);
         CREATE TABLE date_peers (id integer PRIMARY KEY);
         INSERT INTO date_lookup VALUES (1,'2002-03-04',0),(2,'2004-03-02',0);
         INSERT INTO date_peers VALUES (1),(2)",
        )
        .await
        .unwrap();

    const EQUALITY: &str = "SELECT id FROM date_lookup WHERE d = '04/03/02'";
    let mut wire = startup(&fixture, "ISO, MDY").await;
    wire_cycle(&mut wire).await;
    client
        .batch_execute("SET DateStyle='ISO, DMY'")
        .await
        .unwrap();
    let named = client.prepare(EQUALITY).await.unwrap();

    for (style, expected) in [("ISO, DMY", "1"), ("ISO, YMD", "2")] {
        let set = format!("SET DateStyle='{style}'");
        client.batch_execute(&set).await.unwrap();
        assert!(status(&wire_query(&mut wire, &set).await, style));
        assert_wire_text(&mut wire, EQUALITY, None, expected).await;
        for sql in [
            EQUALITY,
            "SELECT id FROM date_lookup WHERE d IN ('04/03/02')",
            "SELECT a.id FROM date_lookup a JOIN date_peers b ON a.id = b.id AND a.d = '04/03/02'",
            "UPDATE date_lookup SET marker = 1 WHERE d = '04/03/02' RETURNING id",
        ] {
            assert_eq!(
                text_rows(&client, sql).await,
                [[Some(expected.to_owned())]],
                "{style}: {sql}"
            );
        }
    }
    // A V2 catalog version change forces the named statement to rebind.
    #[cfg(feature = "engine-rocksdb")]
    client
        .batch_execute("CREATE TABLE date_lookup_refresh (id integer PRIMARY KEY)")
        .await
        .unwrap();
    let row = client.query_one(&named, &[]).await.unwrap();
    assert_eq!(row.get::<_, i32>(0), 1);
    for (style, expected) in [("ISO, YMD", "2"), ("ISO, DMY", "1")] {
        client
            .batch_execute(&format!("SET DateStyle='{style}'"))
            .await
            .unwrap();
        assert_eq!(
            text_rows(
                &client,
                "DELETE FROM date_lookup WHERE d = '04/03/02' RETURNING id"
            )
            .await,
            [[Some(expected.to_owned())]]
        );
    }
    drop(wire);
    drop(client);
    fixture.stop().await;
}

#[tokio::test]
async fn timetz_session_abbreviations_match_for_literals_and_text_bind() {
    let fixture = Fixture::new().await;
    let mut wire = startup(&fixture, "ISO, MDY").await;
    wire_cycle(&mut wire).await;
    let client = fixture.client().await;
    for (zone, input, expected) in [
        ("Asia/Kolkata", "03:04:05 IST", "03:04:05+05:30"),
        ("Asia/Kolkata", "03:04:05 LMT", "03:04:05+05:53:28"),
        ("America/Los_Angeles", "03:04:05 PST", "03:04:05-08"),
        ("UTC", "03:04:05 PST", "03:04:05-08"),
        ("UTC", "03:04:05 MSK", "03:04:05+03"),
        ("UTC", "2011-06-01 03:04:05 MSK", "03:04:05+04"),
    ] {
        let set = format!("SET TIME ZONE '{zone}'");
        client.batch_execute(&set).await.unwrap();
        assert!(
            wire_query(&mut wire, &set)
                .await
                .iter()
                .all(|(tag, _)| *tag != b'E')
        );
        let sql = format!("SELECT '{input}'::timetz");
        assert_eq!(
            text_rows(&client, &sql).await,
            [[Some(expected.to_owned())]],
            "{zone}: {input}"
        );
        assert_wire_text(
            &mut wire,
            "SELECT $1::timetz",
            Some((1266, input)),
            expected,
        )
        .await;
    }
    drop(wire);
    drop(client);
    fixture.stop().await;
}

#[tokio::test]
async fn setting_lists_reject_unquoted_default() {
    let fixture = Fixture::new().await;
    let client = fixture.client().await;
    for (sql, token) in [
        ("SET DateStyle = default, 'SQL'", ","),
        ("SET DateStyle = 'SQL', default", "default"),
        ("SET search_path = default, 'public'", ","),
        ("SET search_path = 'public', default", "default"),
    ] {
        let error = client.simple_query(sql).await.unwrap_err();
        let error = error.as_db_error().unwrap();
        assert_eq!(error.code().code(), "42601", "{sql}");
        assert_eq!(
            error.message(),
            format!("syntax error at or near \"{token}\""),
            "{sql}"
        );
    }
    client
        .batch_execute("SET DateStyle = 'SQL, default'")
        .await
        .unwrap();
    assert_eq!(
        text_rows(&client, "SHOW DateStyle").await,
        [[Some("SQL, MDY".into())]]
    );
    client
        .batch_execute("SET DateStyle = 'SQL', 'default'")
        .await
        .unwrap();
    assert_eq!(
        text_rows(&client, "SHOW DateStyle").await,
        [[Some("SQL, MDY".into())]]
    );
    drop(client);
    fixture.stop().await;
}
