use super::session_settings_network::{Fixture, wire_cycle, wire_query};
use giggles_server::pgwire::frame;
use tokio::{io::AsyncWriteExt, net::TcpStream};

fn check_description(messages: &[(u8, Vec<u8>)], expected: &[(&str, u32)]) {
    let rows: Vec<_> = messages.iter().filter(|(tag, _)| *tag == b'T').collect();
    assert_eq!(rows.len(), 1, "{messages:?}");
    assert!(
        !messages.iter().any(|(tag, _)| *tag == b'E'),
        "{messages:?}"
    );
    let (count, mut fields) = rows[0].1.split_at(2);
    assert_eq!(
        usize::from(u16::from_be_bytes(count.try_into().unwrap())),
        expected.len()
    );
    for &(name, oid) in expected {
        let end = fields.iter().position(|byte| *byte == 0).unwrap();
        assert_eq!(&fields[..end], name.as_bytes());
        let (metadata, rest) = fields[end + 1..].split_at(18);
        assert_eq!(u32::from_be_bytes(metadata[6..10].try_into().unwrap()), oid);
        fields = rest;
    }
    assert!(
        fields.is_empty(),
        "unparsed RowDescription bytes: {fields:?}"
    );
}

#[tokio::test]
async fn derived_cast_row_description_is_valid_in_both_protocols() {
    let fixture = Fixture::new().await;
    let mut stream = TcpStream::connect(fixture.pg.address).await.unwrap();
    let startup = b"\0\x03\0\0user\0owner\0database\0postgres\0\0";
    stream.write_u32((startup.len() + 4) as u32).await.unwrap();
    stream.write_all(startup).await.unwrap();
    wire_cycle(&mut stream).await;

    for (sql, expected) in [
        (
            "SELECT (t)::int, x.t::int, t::int + 1 FROM (VALUES ('7')) x(t)",
            &[("t", 23), ("t", 23), ("?column?", 23)][..],
        ),
        (
            "SELECT t::date, t::timestamp, t::bytea, t::date AS d FROM (VALUES ('2024-01-02')) x(t)",
            &[("t", 1082), ("t", 1114), ("t", 17), ("d", 1082)],
        ),
    ] {
        check_description(&wire_query(&mut stream, sql).await, expected);

        let mut parse = vec![0];
        parse.extend_from_slice(sql.as_bytes());
        parse.extend_from_slice(&[0; 3]);
        let mut packet = Vec::new();
        frame(b'P', &parse, &mut packet).unwrap();
        frame(b'B', &[0; 8], &mut packet).unwrap();
        frame(b'D', b"P\0", &mut packet).unwrap();
        frame(b'E', &[0; 5], &mut packet).unwrap();
        frame(b'S', &[], &mut packet).unwrap();
        stream.write_all(&packet).await.unwrap();
        check_description(&wire_cycle(&mut stream).await, expected);
    }
    drop(stream);
    fixture.stop().await;
}
