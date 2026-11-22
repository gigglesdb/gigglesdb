#[tokio::test]
async fn array_ordering_for_application_tags() {
    use tokio_postgres::types::Type;

    let f = Fixture::new().await;
    let mut owner = f.admin.sql_session();
    for sql in [
        "CREATE TABLE posts(id int PRIMARY KEY, tags int[])",
        "INSERT INTO posts VALUES (1,'{10}'),(2,'{2}'),(3,'{9,1}'),(4,'{2,NULL}'),(5,'{}')",
    ] {
        owner.execute(sql, &[], None).unwrap();
    }
    drop(owner);
    let mut bindings: Vec<Binding> =
        serde_json::from_slice(&std::fs::read(f.path.join("bindings.json")).unwrap()).unwrap();
    let Role::Application { tables, .. } = &mut bindings[0].role else {
        panic!("fixture role")
    };
    tables.insert("posts".into());
    f.service.reload_bindings(bindings).unwrap();

    let (pg, connection) = f.pg(f.app.clone()).await;
    let rows = pg
        .query("SELECT id, tags FROM posts ORDER BY tags", &[])
        .await
        .unwrap();
    let posts: Vec<_> = rows
        .iter()
        .map(|row| (row.get::<_, i32>(0), row.get::<_, Vec<Option<i32>>>(1)))
        .collect();
    assert_eq!(
        posts,
        [
            (5, vec![]),
            (2, vec![Some(2)]),
            (4, vec![Some(2), None]),
            (3, vec![Some(9), Some(1)]),
            (1, vec![Some(10)]),
        ]
    );

    let row = pg
        .query_one("SELECT max(tags), min(tags) FROM posts", &[])
        .await
        .unwrap();
    assert_eq!(row.get::<_, Vec<i32>>(0), [10]);
    assert!(row.get::<_, Vec<i32>>(1).is_empty());

    let statement = pg
        .prepare("SELECT id FROM posts WHERE tags > $1 ORDER BY tags")
        .await
        .unwrap();
    assert_eq!(statement.params(), &[Type::INT4_ARRAY]);
    let rows = pg.query(&statement, &[&vec![2i32]]).await.unwrap();
    let ids: Vec<_> = rows.iter().map(|row| row.get::<_, i32>(0)).collect();
    assert_eq!(ids, [4, 3, 1]);

    drop(pg);
    connection.await.unwrap();
    f.stop().await;
}
