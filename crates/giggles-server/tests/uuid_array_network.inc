fn bind_uuid_array_binary(statement: &str, params: &[&[u8]]) -> Vec<u8> {
    let mut body = vec![0];
    cstr(statement, &mut body);
    body.extend(1u16.to_be_bytes());
    body.extend(1u16.to_be_bytes());
    body.extend((params.len() as u16).to_be_bytes());
    for value in params {
        body.extend((value.len() as i32).to_be_bytes());
        body.extend(*value);
    }
    body.extend(1u16.to_be_bytes());
    body.extend(1u16.to_be_bytes());
    body
}

async fn uuid_array_query_frames(
    stream: &mut (impl AsyncReadExt + AsyncWriteExt + Unpin),
    query: &str,
) -> Vec<(u8, Vec<u8>)> {
    let mut body = Vec::new();
    cstr(query, &mut body);
    pg_send(stream, b'Q', &body).await;
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

async fn uuid_array_prisma_protocol(engine: giggles_core::Engine) {
    let f = Fixture::with_app_table_engine(
        "versions",
        &["CREATE TABLE versions(id uuid PRIMARY KEY, authors uuid[] NOT NULL, approvers uuid[])"],
        engine,
    )
    .await;
    let mut s = raw_pg(&f).await;
    let uuid = hex::decode("00000000000000000000000000000001").unwrap();
    // PG 18.6 array_send output for {NULL,00000000-0000-0000-0000-000000000001}.
    let array = hex::decode(
        "000000010000000100000b860000000200000001ffffffff0000001000000000000000000000000000000001",
    )
    .unwrap();
    let empty = hex::decode("000000000000000000000b86").unwrap();

    let oids = [2950, 2951];
    for (name, sql, params) in [
        (
            "insert",
            "INSERT INTO versions (id,authors) VALUES ($1,$2)",
            [&uuid[..], &array[..]],
        ),
        (
            "update",
            "UPDATE versions SET approvers=$2 WHERE id=$1",
            [&uuid[..], &empty[..]],
        ),
    ] {
        pg_send(&mut s, b'P', &parse_oids(name, sql, &oids)).await;
        pg_send(&mut s, b'B', &bind_uuid_array_binary(name, &params)).await;
        pg_send(&mut s, b'E', &execute("", 0)).await;
        let frames = pg_sync(&mut s).await;
        assert_eq!(
            frames.iter().map(|f| f.0).collect::<Vec<_>>(),
            b"12CZ",
            "{frames:?}"
        );
    }

    pg_send(
        &mut s,
        b'P',
        &parse_oids(
            "filter",
            "SELECT authors, approvers FROM versions WHERE $1 = ANY(authors)",
            &[2950],
        ),
    )
    .await;
    pg_send(&mut s, b'B', &bind_uuid_array_binary("filter", &[&uuid])).await;
    pg_send(&mut s, b'D', b"P\0").await;
    pg_send(&mut s, b'E', &execute("", 0)).await;
    let frames = pg_sync(&mut s).await;
    assert_eq!(
        frames.iter().map(|f| f.0).collect::<Vec<_>>(),
        b"12TDCZ",
        "{frames:?}"
    );
    let row_description = &frames[2].1;
    assert_eq!(&row_description[..2], &2u16.to_be_bytes());
    let authors_oid = 2 + "authors".len() + 1 + 4 + 2;
    assert_eq!(
        &row_description[authors_oid..authors_oid + 4],
        &2951u32.to_be_bytes()
    );
    let mut row = 2u16.to_be_bytes().to_vec();
    for expected in [&array, &empty] {
        row.extend((expected.len() as i32).to_be_bytes());
        row.extend(expected);
    }
    assert_eq!(frames[3].1, row);

    let text = "{00000000-0000-0000-0000-000000000001}";
    pg_send(
        &mut s,
        b'P',
        &parse(
            "contains",
            "SELECT authors FROM versions WHERE authors @> $1::uuid[]",
        ),
    )
    .await;
    pg_send(&mut s, b'B', &bind("p", "contains", &[Some(text)], false)).await;
    pg_send(&mut s, b'E', &execute("p", 0)).await;
    let frames = pg_sync(&mut s).await;
    assert_eq!(
        frames.iter().map(|f| f.0).collect::<Vec<_>>(),
        b"12DCZ",
        "{frames:?}"
    );
    assert_eq!(
        &frames[2].1[6..],
        b"{NULL,00000000-0000-0000-0000-000000000001}"
    );

    let mut one = hex::decode("000000010000000000000b86000000010000000100000010").unwrap();
    one.extend(&uuid);
    pg_send(&mut s, b'B', &bind_uuid_array_binary("contains", &[&one])).await;
    pg_send(&mut s, b'E', &execute("", 0)).await;
    let frames = pg_sync(&mut s).await;
    assert_eq!(
        frames.iter().map(|f| f.0).collect::<Vec<_>>(),
        b"2DCZ",
        "{frames:?}"
    );
    assert_eq!(&frames[1].1[6..], &array);

    let frames = uuid_array_query_frames(
        &mut s,
        "SELECT a.id FROM versions v CROSS JOIN LATERAL unnest(v.authors) a(id) ORDER BY 1",
    )
    .await;
    assert_eq!(
        frames.iter().map(|f| f.0).collect::<Vec<_>>(),
        b"TDDCZ",
        "{frames:?}"
    );
    assert_eq!(&frames[1].1[6..], b"00000000-0000-0000-0000-000000000001");
    assert_eq!(&frames[2].1[2..], &(-1i32).to_be_bytes());

    let (pg, connection) = f.pg(f.app.clone()).await;
    let row = pg.query_one(
        "SELECT data_type,udt_name,is_nullable FROM information_schema.columns WHERE table_name='versions' AND column_name='authors'",
        &[],
    ).await.unwrap();
    assert_eq!(row.get::<_, &str>(0), "ARRAY");
    assert_eq!(row.get::<_, &str>(1), "_uuid");
    assert_eq!(row.get::<_, &str>(2), "NO");
    let row = pg.query_one(
        "SELECT format_type(atttypid,atttypmod),atttypid::regtype::text,attndims FROM pg_attribute WHERE attrelid='versions'::regclass AND attname='authors'",
        &[],
    ).await.unwrap();
    assert_eq!(row.get::<_, &str>(0), "uuid[]");
    assert_eq!(row.get::<_, &str>(1), "uuid[]");
    assert_eq!(row.get::<_, i16>(2), 1);
    let row = pg.query_one(
        "SELECT typcategory::text,typelem::regtype::text,oid FROM pg_type WHERE typname='_uuid'",
        &[],
    ).await.unwrap();
    assert_eq!(row.get::<_, &str>(0), "A");
    assert_eq!(row.get::<_, &str>(1), "uuid");
    assert_eq!(row.get::<_, u32>(2), 2951);
    drop(pg);
    connection.await.unwrap();
    drop(s);
    f.stop().await;
}

#[tokio::test]
async fn uuid_array_prisma_protocol_legacy() {
    uuid_array_prisma_protocol(giggles_core::Engine::Legacy).await;
}

#[tokio::test]
#[cfg(feature = "engine-rocksdb")]
async fn uuid_array_prisma_protocol_v2() {
    uuid_array_prisma_protocol(giggles_core::Engine::V2).await;
}
