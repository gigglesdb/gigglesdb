fn search_data_row(body: &[u8]) -> Vec<Option<&[u8]>> {
    let columns = u16::from_be_bytes(body[..2].try_into().unwrap());
    let mut rest = &body[2..];
    (0..columns)
        .map(|_| {
            let size = i32::from_be_bytes(rest[..4].try_into().unwrap());
            rest = &rest[4..];
            if size == -1 {
                return None;
            }
            let (value, tail) = rest.split_at(size as usize);
            rest = tail;
            Some(value)
        })
        .collect()
}

fn search_type_oids(body: &[u8]) -> Vec<u32> {
    let count = u16::from_be_bytes(body[..2].try_into().unwrap());
    let mut rest = &body[2..];
    (0..count)
        .map(|_| {
            let name_end = rest.iter().position(|byte| *byte == 0).unwrap();
            rest = &rest[name_end + 1..];
            let oid = u32::from_be_bytes(rest[6..10].try_into().unwrap());
            rest = &rest[18..];
            oid
        })
        .collect()
}

fn search_wire_field(body: &[u8], target: u8) -> Option<&str> {
    let mut rest = body;
    while let [field, tail @ ..] = rest {
        if *field == 0 {
            break;
        }
        let end = tail.iter().position(|byte| *byte == 0).unwrap();
        if *field == target {
            return Some(std::str::from_utf8(&tail[..end]).unwrap());
        }
        rest = &tail[end + 1..];
    }
    None
}

async fn text_search_configuration_wire(client: &tokio_postgres::Client) {
    use tokio_postgres::SimpleQueryMessage;

    client.simple_query("SET default_text_search_config = 'hindi'").await.unwrap();
    for (sql, expected) in [
        ("SHOW default_text_search_config", "pg_catalog.hindi"),
        ("SELECT 1", "1"),
        ("SELECT to_tsvector('english','cats')", "'cat':1"),
    ] {
        let messages = client.simple_query(sql).await.unwrap();
        assert!(messages.iter().any(|message| matches!(message,
            SimpleQueryMessage::Row(row) if row.get(0) == Some(expected))), "{sql}");
    }
    for sql in [
        "SELECT to_tsvector('x')",
        "SELECT to_tsvector('hindi','x')",
        "SELECT 'hindi'::regconfig",
    ] {
        let error = client.simple_query(sql).await.unwrap_err();
        let error = error.as_db_error().unwrap();
        assert_eq!(error.code().code(), "0A000", "{sql}");
        assert_eq!(error.message(), "text search configuration \"hindi\" is not supported", "{sql}");
    }
    client.simple_query("SET default_text_search_config = 'english'").await.unwrap();
    let messages = client.simple_query("SELECT to_tsvector('cats')").await.unwrap();
    assert!(messages.iter().any(|message| matches!(message,
        SimpleQueryMessage::Row(row) if row.get(0) == Some("'cat':1"))));

    client.execute("SET default_text_search_config = 'hindi'", &[]).await.unwrap();
    let row = client.query_one("SELECT 1", &[]).await.unwrap();
    assert_eq!(row.get::<_, i32>(0), 1);
    let search = client.prepare("SELECT to_tsvector($1)::text").await.unwrap();
    let error = client.query_one(&search, &[&"cats"]).await.unwrap_err();
    let error = error.as_db_error().unwrap();
    assert_eq!(error.code().code(), "0A000");
    assert_eq!(error.message(), "text search configuration \"hindi\" is not supported");

    client.execute("SET default_text_search_config = 'english'", &[]).await.unwrap();
    let row = client.query_one(&search, &[&"cats"]).await.unwrap();
    assert_eq!(row.get::<_, &str>(0), "'cat':1");
}

async fn text_search_wire(engine: giggles_core::Engine) {
    let f = Fixture::with_app_table_engine(
        "terms",
        &["CREATE TABLE terms (id integer PRIMARY KEY, v tsvector, q tsquery)"],
        engine,
    )
    .await;
    let mut stream = raw_pg(&f).await;
    let vector = hex::decode("000000026361740000010001646f670000010002").unwrap();
    let query = hex::decode("0000000101000063617400").unwrap();

    pg_send(
        &mut stream,
        b'P',
        &parse_oids(
            "insert_fts",
            "INSERT INTO terms VALUES (1,$1,$2)",
            &[3614, 3615],
        ),
    )
    .await;
    pg_send(
        &mut stream,
        b'B',
        &bind_uuid_array_binary("insert_fts", &[&vector, &query]),
    )
    .await;
    pg_send(&mut stream, b'E', &execute("", 0)).await;
    let frames = pg_sync(&mut stream).await;
    assert_eq!(
        frames.iter().map(|frame| frame.0).collect::<Vec<_>>(),
        b"12CZ",
        "{frames:?}"
    );

    pg_send(&mut stream, b'P', &parse(
        "read_fts",
        "SELECT v, q, get_current_ts_config() AS cfg, ts_rank(v,q) AS score FROM terms WHERE v @@ $1",
    )).await;
    pg_send(&mut stream, b'D', b"Sread_fts\0").await;
    pg_send(
        &mut stream,
        b'B',
        &bind("", "read_fts", &[Some("cat")], true),
    )
    .await;
    pg_send(&mut stream, b'E', &execute("", 0)).await;
    let frames = pg_sync(&mut stream).await;
    assert_eq!(
        frames.iter().map(|frame| frame.0).collect::<Vec<_>>(),
        b"1tT2DCZ",
        "{frames:?}"
    );
    assert_eq!(&frames[1].1, &[0, 1, 0, 0, 14, 31]);
    assert_eq!(search_type_oids(&frames[2].1), vec![3614, 3615, 3734, 700]);
    let row = search_data_row(&frames[4].1);
    assert_eq!(row[0], Some(vector.as_slice()));
    assert_eq!(row[1], Some(query.as_slice()));
    assert_eq!(row[2], Some(13248u32.to_be_bytes().as_slice()));

    let frames = uuid_array_query_frames(&mut stream, "SELECT v, q FROM terms").await;
    assert_eq!(search_type_oids(&frames[0].1), vec![3614, 3615]);
    assert_eq!(
        search_data_row(&frames[1].1),
        vec![
            Some(b"'cat':1 'dog':2".as_slice()),
            Some(b"'cat'".as_slice())
        ]
    );

    let sql = "INSERT INTO terms VALUES (2, to_tsvector('english','fat cats'), 'cat'::tsquery)
        RETURNING v::text, q::text";
    let frames = uuid_array_query_frames(&mut stream, sql).await;
    assert_eq!(search_data_row(&frames[1].1), [Some(b"'cat':2 'fat':1".as_slice()), Some(b"'cat'".as_slice())]);

    pg_send(&mut stream, b'P', &parse("fts_insert", "INSERT INTO terms (id,v) VALUES ($1,to_tsvector($2))")).await;
    pg_send(&mut stream, b'B', &bind("", "fts_insert", &[Some("3"), Some("Running dogs")], false)).await;
    pg_send(&mut stream, b'E', &execute("", 0)).await;
    let frames = pg_sync(&mut stream).await;
    assert_eq!(frames.iter().map(|frame| frame.0).collect::<Vec<_>>(), b"12CZ", "{frames:?}");

    pg_send(&mut stream, b'P', &parse("config_fts", "SELECT to_tsvector($1,$2), to_tsquery($1,$3),
        plainto_tsquery($1,$2), phraseto_tsquery($1,$2), websearch_to_tsquery($1,$2), ts_headline($1,$2,$3::tsquery)")).await;
    pg_send(&mut stream, b'D', b"Sconfig_fts\0").await;
    pg_send(&mut stream, b'B', &bind("", "config_fts", &[Some("english"), Some("Running dogs"), Some("dog")], false)).await;
    pg_send(&mut stream, b'E', &execute("", 0)).await;
    let frames = pg_sync(&mut stream).await;
    assert_eq!(frames.iter().map(|frame| frame.0).collect::<Vec<_>>(), b"1tT2DCZ", "{frames:?}");
    assert_eq!(&frames[1].1, &[0, 3, 0, 0, 14, 150, 0, 0, 0, 25, 0, 0, 0, 25]);
    assert_eq!(search_data_row(&frames[4].1).into_iter().flatten().collect::<Vec<_>>(), [
        b"'dog':2 'run':1".as_slice(), b"'dog'", b"'run' & 'dog'", b"'run' <-> 'dog'", b"'run' & 'dog'", b"Running <b>dogs</b>"
    ]);

    let frames = uuid_array_query_frames(&mut stream, "SELECT * FROM unnest('a:1'::tsvector)").await;
    assert!(frames[0].1[2..].starts_with(b"lexeme\0"));
    assert_eq!(search_type_oids(&frames[0].1), [25, 1005, 1002]);

    let frames = uuid_array_query_frames(&mut stream, "SELECT * FROM ts_debug('english','Running the')").await;
    assert_eq!(search_type_oids(&frames[0].1), [25, 25, 25, 3770, 3769, 1009]);
    let row = search_data_row(&frames[1].1);
    assert_eq!(row[3], Some(b"{english_stem}".as_slice()));
    assert_eq!(row[4], Some(b"english_stem".as_slice()));
    assert_eq!(row[5], Some(b"{run}".as_slice()));

    pg_send(&mut stream, b'P', &parse("fts_dict", "SELECT dictionaries, dictionary FROM ts_debug('english','Running')")).await;
    pg_send(&mut stream, b'B', &bind("", "fts_dict", &[], true)).await;
    pg_send(&mut stream, b'E', &execute("", 0)).await;
    let frames = pg_sync(&mut stream).await;
    let row = search_data_row(&frames.iter().find(|frame| frame.0 == b'D').expect("DataRow").1);
    let dictionary_oid = giggles_core::relational::text_search_types::input(&giggles_core::relational::DataType::RegDictionary, "english_stem").unwrap();
    let giggles_core::relational::Value::Int(dictionary_oid) = dictionary_oid else { unreachable!() };
    let dictionary_oid = u32::try_from(dictionary_oid).unwrap();
    let array = [1u32, 0, 3769, 1, 1, 4, dictionary_oid].into_iter()
        .flat_map(u32::to_be_bytes).collect::<Vec<_>>();
    assert_eq!(row, [Some(array.as_slice()), Some(dictionary_oid.to_be_bytes().as_slice())]);

    for (sql, types, first) in [
        ("SELECT * FROM ts_parse('default','Running')", &[23, 25][..], &b"Running"[..]),
        ("SELECT * FROM ts_token_type(3722::oid) LIMIT 1", &[23, 25, 25][..], &b"asciiword"[..]),
    ] {
        let frames = uuid_array_query_frames(&mut stream, sql).await;
        assert_eq!(search_type_oids(&frames[0].1), types);
        assert_eq!(search_data_row(&frames[1].1)[1], Some(first));
    }

    let notice_sql = "SELECT to_tsquery('english','the')";
    let simple = uuid_array_query_frames(&mut stream, notice_sql).await;
    pg_send(&mut stream, b'P', &parse("fts_notice", notice_sql)).await;
    pg_send(&mut stream, b'B', &bind("", "fts_notice", &[], false)).await;
    pg_send(&mut stream, b'E', &execute("", 0)).await;
    let extended = pg_sync(&mut stream).await;
    for frames in [simple, extended] {
        let mut notices = frames.iter().filter(|frame| frame.0 == b'N');
        let notice = &notices.next().expect("NoticeResponse").1;
        assert!(notices.next().is_none(), "{frames:?}");
        assert_eq!(search_wire_field(notice, b'S'), Some("NOTICE"));
        assert_eq!(search_wire_field(notice, b'C'), Some("00000"));
        assert_eq!(
            search_wire_field(notice, b'M'),
            Some("text-search query contains only stop words or doesn't contain lexemes, ignored")
        );
    }

    for (sql, code, message) in [
        (
            "SELECT 'cat &'::tsquery",
            "42601",
            "no operand in tsquery: \"cat &\"",
        ),
        (
            "SELECT 'nosuch'::regconfig",
            "42704",
            "text search configuration \"nosuch\" does not exist",
        ),
    ] {
        let frames = uuid_array_query_frames(&mut stream, sql).await;
        let error = &frames
            .iter()
            .find(|frame| frame.0 == b'E')
            .expect("ErrorResponse")
            .1;
        assert_eq!(search_wire_field(error, b'C'), Some(code), "{sql}");
        assert_eq!(search_wire_field(error, b'M'), Some(message), "{sql}");
    }

    let frames = uuid_array_query_frames(&mut stream,
        "SELECT oprcode FROM pg_operator WHERE oprname = '@@' ORDER BY oid").await;
    let names = frames.iter().filter(|frame| frame.0 == b'D')
        .map(|frame| search_data_row(&frame.1)[0].unwrap()).collect::<Vec<_>>();
    assert_eq!(names, [b"ts_match_vq", b"ts_match_qv", b"ts_match_tt", b"ts_match_tq"]);

    let (pg, connection) = f.pg(f.app.clone()).await;
    let row = pg.query_one(
        "SELECT data_type, udt_name FROM information_schema.columns WHERE table_name='terms' AND column_name='v'", &[],
    ).await.unwrap();
    assert_eq!(row.get::<_, &str>(0), "tsvector");
    assert_eq!(row.get::<_, &str>(1), "tsvector");
    let row = pg.query_one(
        "SELECT format_type(oid, NULL), typlen, typcategory::text, typarray FROM pg_type WHERE typname='tsquery'", &[],
    ).await.unwrap();
    assert_eq!(row.get::<_, &str>(0), "tsquery");
    assert_eq!(row.get::<_, i16>(1), -1);
    assert_eq!(row.get::<_, &str>(2), "U");
    assert_eq!(row.get::<_, u32>(3), 3645);
    let operators = pg.query(
        "SELECT oid, oprleft::oid, oprright::oid, oprresult::oid, oprcode::oid
         FROM pg_operator WHERE oprname = '@@' ORDER BY oid", &[],
    ).await.unwrap();
    assert_eq!(
        operators.iter().map(|row| std::array::from_fn(|i| row.get::<_, u32>(i))).collect::<Vec<_>>(),
        [[3636, 3614, 3615, 16, 3634], [3637, 3615, 3614, 16, 3635],
         [3762, 25, 25, 16, 3760], [3763, 25, 3615, 16, 3761]]
    );
    drop(pg);
    connection.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn text_search_network_legacy() {
    text_search_wire(giggles_core::Engine::Legacy).await;
}

#[tokio::test(flavor = "multi_thread")]
#[cfg(feature = "engine-rocksdb")]
async fn text_search_network_v2() {
    text_search_wire(giggles_core::Engine::V2).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn text_search_network_archdocs() {
    let setup = [
        r#"CREATE TABLE "Organization" ("id" UUID PRIMARY KEY)"#,
        r#"INSERT INTO "Organization" VALUES ('00000000-0000-0000-0000-0000000000aa')"#,
    ]
    .into_iter()
    .chain(
        include_str!("../../giggles-core/tests/fixtures/text_search_archdocs_setup.sql")
            .split_terminator(';')
            .chain(
                include_str!(
                    "../../giggles-core/tests/fixtures/text_search_archdocs_match_body.sql"
                )
                .split_terminator(';'),
            )
            .map(str::trim)
            .filter(|sql| !sql.is_empty()),
    )
    .collect::<Vec<_>>();
    let engine = if cfg!(feature = "engine-rocksdb") {
        giggles_core::Engine::V2
    } else {
        giggles_core::Engine::Legacy
    };
    let f = Fixture::with_app_table_engine("SearchDocument", &setup, engine).await;
    let mut stream = raw_pg(&f).await;
    let organization = "00000000-0000-0000-0000-0000000000aa";
    let insert = r#"INSERT INTO "SearchDocument" ("entityType","entityId","organizationId",title,body,"updatedAt")
        VALUES ($1,$2,$3,$4,$5,$6) RETURNING tsv"#;
    pg_send(
        &mut stream,
        b'P',
        &parse_oids("insert_document", insert, &[25, 2950, 2950, 25, 25, 1114]),
    )
    .await;
    for (entity, id, title, body, vector) in [
        (
            "doc",
            "00000000-0000-0000-0000-000000000001",
            "Payment service architecture",
            "The payment service talks to Stripe through the billing gateway. Retries are idempotent.",
            "'architectur':3A 'bill':12B 'gateway':13B 'idempot':16B 'payment':1A,5B 'retri':14B 'servic':2A,6B 'stripe':9B 'talk':7B",
        ),
        (
            "design-doc",
            "00000000-0000-0000-0000-000000000002",
            "Search indexing design",
            "We index documents into a tsvector column and rank results with ts_rank.",
            "'column':10B 'design':3A 'document':6B 'index':2A,5B 'rank':12B,16B 'result':13B 'search':1A 'ts':15B 'tsvector':9B",
        ),
        (
            "object",
            "00000000-0000-0000-0000-000000000003",
            "Billing gateway",
            "Gateway for payments; owns invoices.",
            "'bill':1A 'gateway':2A,3B 'invoic':7B 'own':6B 'payment':5B",
        ),
    ] {
        pg_send(
            &mut stream,
            b'B',
            &bind(
                "",
                "insert_document",
                &[entity, id, organization, title, body, "2026-01-01"].map(Some),
                false,
            ),
        )
        .await;
        pg_send(&mut stream, b'E', &execute("", 0)).await;
        let frames = pg_sync(&mut stream).await;
        assert!(!frames.iter().any(|frame| frame.0 == b'E'), "{frames:?}");
        let row = &frames.iter().find(|frame| frame.0 == b'D').unwrap().1;
        assert_eq!(search_data_row(row), [Some(vector.as_bytes())]);
    }
    for (column, value, id, vector) in [
        (
            "body",
            "Payments are retried by the gateway",
            "00000000-0000-0000-0000-000000000003",
            "'bill':1A 'gateway':2A,8B 'payment':3B 'retri':5B",
        ),
        (
            "matchOnlyBody",
            "journal entry about refunds",
            "00000000-0000-0000-0000-000000000001",
            "'architectur':3A 'bill':12B 'entri':18C 'gateway':13B 'idempot':16B 'journal':17C 'payment':1A,5B 'refund':20C 'retri':14B 'servic':2A,6B 'stripe':9B 'talk':7B",
        ),
    ] {
        let sql = format!(
            r#"UPDATE "SearchDocument" SET "{column}"=$1 WHERE "entityId"=$2 RETURNING tsv"#
        );
        pg_send(&mut stream, b'P', &parse_oids("", &sql, &[25, 2950])).await;
        pg_send(
            &mut stream,
            b'B',
            &bind("", "", &[Some(value), Some(id)], false),
        )
        .await;
        pg_send(&mut stream, b'E', &execute("", 0)).await;
        let frames = pg_sync(&mut stream).await;
        assert!(!frames.iter().any(|frame| frame.0 == b'E'), "{frames:?}");
        let row = &frames.iter().find(|frame| frame.0 == b'D').unwrap().1;
        assert_eq!(search_data_row(row), [Some(vector.as_bytes())]);
    }

    let expected_expression = r#"((setweight(to_tsvector('english'::regconfig, COALESCE(title, ''::text)), 'A'::"char") || setweight(to_tsvector('english'::regconfig, COALESCE(body, ''::text)), 'B'::"char")) || setweight(to_tsvector('english'::regconfig, COALESCE("matchOnlyBody", ''::text)), 'C'::"char"))"#;
    let (pg, connection) = f.pg(f.app.clone()).await;
    let row = pg.query_one(
        "SELECT data_type, udt_name, is_generated, generation_expression FROM information_schema.columns WHERE table_name='SearchDocument' AND column_name='tsv'",
        &[],
    ).await.unwrap();
    assert_eq!(
        (0..4).map(|i| row.get::<_, &str>(i)).collect::<Vec<_>>(),
        ["tsvector", "tsvector", "ALWAYS", expected_expression]
    );
    let row = pg.query_one(
        r#"SELECT pg_get_expr(d.adbin,d.adrelid), a.attnum, pg_get_expr(d.adbin,d.adrelid,true)
        FROM pg_attribute a JOIN pg_attrdef d ON a.attrelid=d.adrelid AND a.attnum=d.adnum
        WHERE a.attrelid='"SearchDocument"'::regclass AND a.attname='tsv'"#,
        &[],
    ).await.unwrap();
    assert_eq!(row.get::<_, &str>(0), expected_expression);
    let attnum = row.get::<_, i16>(1);
    assert_eq!(
        row.get::<_, &str>(2),
        &expected_expression[1..expected_expression.len() - 1]
    );
    let row = pg
        .query_one(
            "SELECT indexdef FROM pg_indexes WHERE indexname='SearchDocument_tsv_idx'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(
        row.get::<_, &str>(0),
        r#"CREATE INDEX "SearchDocument_tsv_idx" ON public."SearchDocument" USING gin (tsv)"#
    );
    let row = pg
        .query_one(
            "SELECT am.amname, opc.opcname, opc.oid, i.indisunique, i.indkey[0]
         FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid
         JOIN pg_am am ON am.oid=c.relam JOIN pg_opclass opc ON opc.oid=i.indclass[0]
         WHERE c.relname='SearchDocument_tsv_idx'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, &str>(0), "gin");
    assert_eq!(row.get::<_, &str>(1), "tsvector_ops");
    assert_eq!(row.get::<_, u32>(2), 10073);
    assert!(!row.get::<_, bool>(3));
    assert_eq!(row.get::<_, i16>(4), attnum);
    drop(pg);
    connection.abort();

    let query = include_str!("../../giggles-core/tests/fixtures/text_search_archdocs_query.sql")
        .replace("LIMIT $3", "LIMIT ($3::bigint)");
    pg_send(
        &mut stream,
        b'P',
        &parse_oids("search", &query, &[2950, 25, 23, 1009]),
    )
    .await;
    let parameter_sets = [
        (
            "payment gateway",
            vec![
                [
                    "object",
                    "Billing gateway",
                    "f",
                    "0.763366",
                    "Billing <mark>gateway</mark> \u{2014} <mark>Payments</mark> are retried by the <mark>gateway</mark>",
                ],
                [
                    "doc",
                    "Payment service architecture",
                    "f",
                    "0.35984504",
                    "<mark>Payment</mark> service architecture \u{2014} The <mark>payment</mark> service talks to Stripe through the billing <mark>gateway</mark>. Retries are idempotent",
                ],
            ],
        ),
        (
            "refunds",
            vec![[
                "doc",
                "Payment service architecture",
                "t",
                "0.12158542",
                "Payment service architecture \u{2014} The payment service talks to",
            ]],
        ),
        (
            "\"billing gateway\" -invoices",
            vec![
                [
                    "object",
                    "Billing gateway",
                    "f",
                    "0.9953511",
                    "<mark>Billing</mark> <mark>gateway</mark> \u{2014} Payments are retried by the <mark>gateway</mark>",
                ],
                [
                    "doc",
                    "Payment service architecture",
                    "f",
                    "0.39641288",
                    "Payment service architecture \u{2014} The payment service talks to Stripe through the <mark>billing</mark> <mark>gateway</mark>. Retries are idempotent",
                ],
            ],
        ),
        ("the", vec![]),
    ];
    for (terms, expected) in parameter_sets {
        pg_send(
            &mut stream,
            b'B',
            &bind(
                "",
                "search",
                &[
                    Some(organization),
                    Some(terms),
                    Some("10"),
                    Some("{doc,object,design-doc}"),
                ],
                false,
            ),
        )
        .await;
        pg_send(&mut stream, b'E', &execute("", 0)).await;
        let frames = pg_sync(&mut stream).await;
        assert!(!frames.iter().any(|frame| frame.0 == b'E'), "{frames:?}");
        let rows = frames
            .iter()
            .filter(|frame| frame.0 == b'D')
            .map(|frame| search_data_row(&frame.1))
            .collect::<Vec<_>>();
        let expected = expected
            .iter()
            .map(|row| {
                row.iter()
                    .map(|text| Some(text.as_bytes()))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(rows, expected, "{terms}");
    }
}
