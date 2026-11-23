async fn dml_sources_protocol(engine: giggles_core::Engine) {
    let f = Fixture::with_app_table_engine(
        "migration",
        &[
            "CREATE TABLE migration(id int PRIMARY KEY, kind text, label text, payload jsonb)",
            r#"INSERT INTO migration VALUES
                (1,'spec','pending','[{"key":"a"},{"key":"b","computed":false},{"key":"c"}]'),
                (2,'spec','pending',NULL),(3,'plan','pending',NULL)"#,
        ],
        engine,
    )
    .await;
    let (pg, conn) = f.pg(f.app.clone()).await;
    for (sql, affected) in [
        (
            "INSERT INTO migration (id,kind,label) SELECT m.id+10, m.kind, v.label
          FROM migration m CROSS JOIN (VALUES ('copied')) AS v(label)
          WHERE m.id <= 2 ON CONFLICT (id) DO NOTHING",
            2,
        ),
        (
            "WITH duplicates AS (
              SELECT id, row_number() OVER (PARTITION BY kind ORDER BY id) AS n
              FROM migration WHERE label = 'pending'
          )
          UPDATE migration SET label='done' WHERE id IN (SELECT id FROM duplicates WHERE n > 1)",
            1,
        ),
        (
            r#"UPDATE migration AS t SET payload = (
            SELECT jsonb_agg(CASE WHEN f = '{"key":"b","computed":false}'::jsonb
                THEN jsonb_set(f, '{computed}', 'true'::jsonb) ELSE f END ORDER BY ordinal)
            FROM jsonb_array_elements(t.payload) WITH ORDINALITY AS fields(f, ordinal)
          ) WHERE kind='spec' AND payload @> '[{"key":"b","computed":false}]'::jsonb"#,
            1,
        ),
    ] {
        let output = pg
            .simple_query(sql)
            .await
            .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
        assert!(
            matches!(&output[..], [tokio_postgres::SimpleQueryMessage::CommandComplete(n)] if *n == affected)
        );
    }
    let output = pg
        .simple_query("SELECT payload::text FROM migration WHERE id=1")
        .await
        .unwrap();
    assert!(
        matches!(&output[1], tokio_postgres::SimpleQueryMessage::Row(row)
        if row.get(0) == Some(r#"[{"key": "a"}, {"key": "b", "computed": true}, {"key": "c"}]"#))
    );

    let error = pg
        .simple_query("UPDATE migration SET label=(SELECT label FROM migration)")
        .await
        .unwrap_err();
    let error = error.as_db_error().unwrap();
    assert_eq!(error.code().code(), "21000");
    assert_eq!(
        error.message(),
        "more than one row returned by a subquery used as an expression"
    );

    let mut raw = raw_pg(&f).await;
    for (name, sql, param, expected, tag) in [
        (
            "ins",
            "INSERT INTO migration (id,kind,label) SELECT 30,'spec',v.x
                  FROM (VALUES ($1)) AS v(x) RETURNING label",
            "from-values",
            "from-values",
            b"INSERT 0 1\0".as_slice(),
        ),
        (
            "upd",
            "UPDATE migration AS m SET label=(SELECT $1 FROM migration src
                                               WHERE src.id=m.id ORDER BY src.id LIMIT 1)
                  WHERE id=30 RETURNING label",
            "from-subquery",
            "from-subquery",
            b"UPDATE 1\0".as_slice(),
        ),
        (
            "cte",
            "WITH d AS (SELECT id FROM migration WHERE id = 30)
                  UPDATE migration SET label=$1 WHERE id IN (SELECT id FROM d) RETURNING label",
            "from-cte",
            "from-cte",
            b"UPDATE 1\0".as_slice(),
        ),
        (
            "json",
            r#"UPDATE migration t SET payload = (
                    SELECT jsonb_agg(CASE WHEN f->>'key' = $1
                        THEN jsonb_set(f, '{computed}', 'false'::jsonb) ELSE f END ORDER BY ordinal)
                    FROM jsonb_array_elements(t.payload) WITH ORDINALITY AS fields(f, ordinal)
                ) WHERE t.id=1 RETURNING payload::text"#,
            "b",
            r#"[{"key": "a"}, {"key": "b", "computed": false}, {"key": "c"}]"#,
            b"UPDATE 1\0".as_slice(),
        ),
        (
            "insert_cte",
            "WITH d AS (SELECT $1::text AS label) INSERT INTO migration (id,label)
                SELECT 31,d.label FROM d RETURNING label",
            "cte-label",
            "cte-label",
            b"INSERT 0 1\0".as_slice(),
        ),
        (
            "delete_cte",
            "WITH d AS (SELECT id FROM migration WHERE label=$1)
                DELETE FROM migration WHERE id IN (SELECT id FROM d) RETURNING label",
            "cte-label",
            "cte-label",
            b"DELETE 1\0".as_slice(),
        ),
    ] {
        pg_send(&mut raw, b'P', &parse(name, sql)).await;
        pg_send(&mut raw, b'B', &bind("", name, &[Some(param)], true)).await;
        pg_send(&mut raw, b'E', &execute("", 0)).await;
        let frames = pg_sync(&mut raw).await;
        assert_eq!(
            frames.iter().map(|frame| frame.0).collect::<Vec<_>>(),
            b"12DCZ",
            "{name}: {frames:?}"
        );
        let mut row = 1u16.to_be_bytes().to_vec();
        row.extend((expected.len() as i32).to_be_bytes());
        row.extend(expected.as_bytes());
        assert_eq!(frames[2].1, row);
        assert_eq!(frames[3].1, tag);
    }
    for sql in [
        "UPDATE migration SET label=(SELECT label FROM migration WHERE id=$1)
         WHERE id=$2 RETURNING label",
        "UPDATE migration SET label=(SELECT label FROM migration WHERE id=$1::int)
         WHERE id=$2::int RETURNING label",
    ] {
        let statement = pg.prepare(sql).await.unwrap();
        assert_eq!(statement.params(), &[tokio_postgres::types::Type::INT4; 2]);
        let row = pg.query_one(&statement, &[&30i32, &3i32]).await.unwrap();
        assert_eq!(row.get::<_, &str>(0), "from-cte");
    }
    let statement = pg.prepare(
        "UPDATE migration SET label=(SELECT label FROM migration WHERE id=$1), kind=$3
         WHERE id=$2 RETURNING label, kind"
    ).await.unwrap();
    use tokio_postgres::types::Type;
    assert_eq!(statement.params(), &[Type::INT4, Type::INT4, Type::TEXT]);
    let row = pg.query_one(&statement, &[&30i32, &3i32, &"reclassified"]).await.unwrap();
    assert_eq!((row.get::<_, &str>(0), row.get::<_, &str>(1)), ("from-cte", "reclassified"));

    let statement = pg.prepare(
        "UPDATE migration SET label=(SELECT label FROM migration
          WHERE id=$1::int AND label=$3::text)
         WHERE id=$2::int RETURNING label"
    ).await.unwrap();
    assert_eq!(statement.params(), &[Type::INT4, Type::INT4, Type::TEXT]);
    let row = pg.query_one(&statement, &[&30i32, &3i32, &"from-cte"]).await.unwrap();
    assert_eq!(row.get::<_, &str>(0), "from-cte");
    drop(raw);
    drop(pg);
    conn.await.unwrap();
    f.stop().await;
}

#[tokio::test]
async fn dml_sources_legacy() {
    dml_sources_protocol(giggles_core::Engine::Legacy).await;
}

#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn dml_sources_v2() {
    dml_sources_protocol(giggles_core::Engine::V2).await;
}

async fn dml_sources_unique_index_protocol(engine: giggles_core::Engine) {
    let f = Fixture::with_app_table_engine(
        "indexed_update",
        &[
            "CREATE TABLE indexed_update(id uuid PRIMARY KEY, grp text NOT NULL, k text NOT NULL)",
            "INSERT INTO indexed_update VALUES
                ('00000000-0000-0000-0000-000000000001','a','x'),
                ('00000000-0000-0000-0000-000000000002','b','x'),
                ('00000000-0000-0000-0000-000000000003','a','y')",
        ],
        engine,
    )
    .await;
    // The fixture creates another table. Allocate the index after that table
    // so its catalog ID is the next one the old CTE allocator would pick.
    f.admin
        .sql_session()
        .execute(
            "CREATE UNIQUE INDEX indexed_update_gk ON indexed_update(grp,k)",
            &[],
            None,
        )
        .unwrap();
    let (pg, conn) = f.pg(f.app.clone()).await;
    let statement = pg
        .prepare(
            "WITH d AS (SELECT id FROM indexed_update WHERE k='x')
             UPDATE indexed_update SET k=$1 WHERE id IN (SELECT id FROM d)",
        )
        .await
        .unwrap();
    assert_eq!(pg.execute(&statement, &[&"xx"]).await.unwrap(), 2);
    let error = pg
        .simple_query(
            "WITH d AS (SELECT id FROM indexed_update WHERE grp='a' AND k='xx')
             UPDATE indexed_update SET k='y' WHERE id IN (SELECT id FROM d)",
        )
        .await
        .unwrap_err();
    let error = error.as_db_error().unwrap();
    assert_eq!(error.code().code(), "23505");
    assert_eq!(
        error.message(),
        "duplicate key value violates unique constraint \"indexed_update_gk\""
    );
    assert_eq!(error.detail(), Some("Key (grp, k)=(a, y) already exists."));
    let rows = pg
        .query("SELECT grp,k FROM indexed_update ORDER BY grp,k", &[])
        .await
        .unwrap();
    let rows: Vec<(&str, &str)> = rows.iter().map(|row| (row.get(0), row.get(1))).collect();
    assert_eq!(rows, [("a", "xx"), ("a", "y"), ("b", "xx")]);
    drop(pg);
    conn.await.unwrap();
    f.stop().await;
}

#[tokio::test]
async fn dml_sources_unique_index_legacy() {
    dml_sources_unique_index_protocol(giggles_core::Engine::Legacy).await;
}

#[cfg(feature = "engine-rocksdb")]
#[tokio::test]
async fn dml_sources_unique_index_v2() {
    dml_sources_unique_index_protocol(giggles_core::Engine::V2).await;
}
