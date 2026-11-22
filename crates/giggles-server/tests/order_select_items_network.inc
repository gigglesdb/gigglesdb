#[tokio::test]
async fn order_select_items_preserves_sequence_values_over_pgwire() {
    let f = Fixture::with_app_table(
        "items",
        &[
            "CREATE SEQUENCE ids",
            "CREATE TABLE items (ord int PRIMARY KEY, name text, id bigint DEFAULT nextval('ids'))",
            "ALTER SEQUENCE ids OWNED BY items.id",
        ],
    )
    .await;
    let (pg, connection) = f.pg(f.app.clone()).await;
    pg.batch_execute("INSERT INTO items VALUES (1,'first',0),(2,'second',0)")
        .await
        .unwrap();

    for (order, expected) in [
        ("1", [("1", "first"), ("2", "second")]),
        ("id DESC", [("4", "second"), ("3", "first")]),
    ] {
        let sql = format!("SELECT nextval('ids') AS id, name FROM items ORDER BY {order}");
        let result = pg.simple_query(&sql).await.unwrap();
        let rows = result
            .iter()
            .filter_map(|message| match message {
                tokio_postgres::SimpleQueryMessage::Row(row) => {
                    Some((row.get(0).unwrap(), row.get(1).unwrap()))
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(rows, expected, "{sql}");
    }
    for (order, expected) in [
        ("1", [(5, "first".to_owned()), (6, "second".to_owned())]),
        (
            "id DESC",
            [(8, "second".to_owned()), (7, "first".to_owned())],
        ),
    ] {
        let sql = format!(
            "SELECT nextval('ids') AS id, name FROM items WHERE ord <= $1 ORDER BY {order}"
        );
        let statement = pg.prepare(&sql).await.unwrap();
        assert_eq!(statement.params(), &[tokio_postgres::types::Type::INT4]);
        let rows = pg
            .query(&statement, &[&2i32])
            .await
            .unwrap()
            .iter()
            .map(|row| (row.get::<_, i64>(0), row.get::<_, String>(1)))
            .collect::<Vec<_>>();
        assert_eq!(rows, expected, "{sql}");
    }
    let messages = pg.simple_query("SELECT nextval('ids')").await.unwrap();
    assert!(messages.iter().any(|message| matches!(
        message,
        tokio_postgres::SimpleQueryMessage::Row(row) if row.get(0) == Some("9")
    )));
    let sql = "SELECT nextval('ids') AS id, name FROM items ORDER BY nextval('ids') DESC, id";
    let result = pg.simple_query(sql).await.unwrap();
    let rows: Vec<_> = result
        .iter()
        .filter_map(|message| match message {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some((row.get(0), row.get(1))),
            _ => None,
        })
        .collect();
    assert_eq!(rows, [(Some("11"), Some("second")), (Some("10"), Some("first"))]);
    assert_eq!(pg.query_one("SELECT currval('ids')", &[]).await.unwrap().get::<_, i64>(0), 11);

    let rows: Vec<_> = pg
        .query(
            "SELECT nextval('ids') AS id, name FROM items WHERE ord <= $1 ORDER BY nextval('ids') DESC, id",
            &[&2i32],
        )
        .await
        .unwrap()
        .iter()
        .map(|row| (row.get::<_, i64>(0), row.get::<_, String>(1)))
        .collect();
    assert_eq!(rows, [(13, "second".into()), (12, "first".into())]);
    assert_eq!(pg.query_one("SELECT currval('ids')", &[]).await.unwrap().get::<_, i64>(0), 13);

    let sql = "SELECT ord AS n, name AS n FROM items ORDER BY n";
    for error in [
        pg.simple_query(sql).await.unwrap_err(),
        pg.prepare(sql).await.unwrap_err(),
    ] {
        let error = error.as_db_error().unwrap();
        assert_eq!(error.code().code(), "42702");
        assert_eq!(error.message(), r#"ORDER BY "n" is ambiguous"#);
    }

    drop(pg);
    connection.await.unwrap();
    f.stop().await;
}

#[tokio::test]
async fn order_select_items_inserts_distinct_array_star_over_pgwire() {
    let f = Fixture::with_app_table(
        "gd92_dst",
        &[
            "CREATE TABLE gd92_src(id int PRIMARY KEY, v int[])",
            "CREATE TABLE gd92_dst(id int PRIMARY KEY, v int[])",
            "INSERT INTO gd92_src VALUES (1,'{10}'),(2,'{2}')",
        ],
    )
    .await;
    let mut bindings: Vec<Binding> =
        serde_json::from_slice(&std::fs::read(f.path.join("bindings.json")).unwrap()).unwrap();
    let Role::Application { tables, .. } = &mut bindings[0].role else {
        panic!("fixture role")
    };
    tables.insert("gd92_src".into());
    f.service.reload_bindings(bindings).unwrap();

    let (pg, connection) = f.pg(f.app.clone()).await;
    let result = pg
        .simple_query("INSERT INTO gd92_dst SELECT DISTINCT * FROM gd92_src ORDER BY 2")
        .await
        .unwrap();
    assert!(matches!(
        result.as_slice(),
        [tokio_postgres::SimpleQueryMessage::CommandComplete(2)]
    ));
    for extended in [false, true] {
        if extended {
            pg.execute("DELETE FROM gd92_dst", &[]).await.unwrap();
            let inserted = pg
                .execute(
                    "INSERT INTO gd92_dst SELECT DISTINCT * FROM gd92_src WHERE id <= $1 ORDER BY 2",
                    &[&2i32],
                )
                .await
                .unwrap();
            assert_eq!(inserted, 2);
        }
        let rows: Vec<_> = pg
            .query("SELECT * FROM gd92_dst ORDER BY 2", &[])
            .await
            .unwrap()
            .iter()
            .map(|row| (row.get::<_, i32>(0), row.get::<_, Vec<i32>>(1)))
            .collect();
        assert_eq!(rows, [(2, vec![2]), (1, vec![10])]);
    }
    drop(pg);
    connection.await.unwrap();
    f.stop().await;
}
