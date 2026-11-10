use super::postgresql_network::{Fixture, PASSWORD};
use giggles_server::auth::TlsMode;
use tokio_postgres::SimpleQueryMessage;

#[tokio::test]
async fn simple_extended_and_sql_cursors_deliver_large_results() {
    let f = Fixture::new(TlsMode::Disabled, true, false).await;
    let mut c = f.plain("owner", Some(PASSWORD)).await;
    c.batch_execute("CREATE TABLE stream_rows (id bigint PRIMARY KEY, body text)")
        .await
        .unwrap();
    let insert = c
        .prepare("INSERT INTO stream_rows VALUES ($1,$2)")
        .await
        .unwrap();
    let body = "s".repeat(4000);
    let select = "SELECT id,body,body,body,body,body,body,body,body FROM stream_rows ORDER BY id";
    for i in 0..330i64 {
        c.execute(&insert, &[&i, &body]).await.unwrap();
    }
    let messages = c.simple_query(select).await.unwrap();
    let mut seen = 0;
    for message in messages {
        if let SimpleQueryMessage::Row(row) = message {
            assert_eq!(row.get(0).unwrap(), seen.to_string());
            for i in 1..9 {
                assert_eq!(row.get(i).unwrap(), body);
            }
            seen += 1;
        }
    }
    assert_eq!(seen, 330);
    let txn = c.transaction().await.unwrap();
    let statement = txn.prepare(select).await.unwrap();
    let portal = txn.bind(&statement, &[]).await.unwrap();
    let mut seen = 0;
    let mut sizes = Vec::new();
    loop {
        let rows = txn.query_portal(&portal, 100).await.unwrap();
        sizes.push(rows.len());
        for row in &rows {
            assert_eq!(row.get::<_, i64>(0), seen);
            for i in 1..9 {
                assert_eq!(row.get::<_, String>(i), body);
            }
            seen += 1;
        }
        if rows.len() < 100 {
            break;
        }
    }
    assert_eq!(sizes, vec![100, 100, 100, 30]);
    assert_eq!(seen, 330);
    txn.batch_execute(&format!("DECLARE large CURSOR FOR {select}"))
        .await
        .unwrap();
    let rows = txn.simple_query("FETCH ALL FROM large").await.unwrap();
    assert_eq!(
        rows.iter()
            .filter(|r| matches!(r, SimpleQueryMessage::Row(_)))
            .count(),
        330
    );
    txn.commit().await.unwrap();
    drop(c);
    f.stop().await;
}

#[tokio::test]
async fn volatile_sql_cursors_evaluate_only_the_fetched_row() {
    let f = Fixture::new(TlsMode::Disabled, true, false).await;
    let c = f.plain("owner", Some(PASSWORD)).await;
    c.batch_execute("CREATE TABLE volatile_rows (id bigint PRIMARY KEY); INSERT INTO volatile_rows VALUES (1),(2),(3)").await.unwrap();
    for (name, select) in [
        ("single_seq", "SELECT nextval('single_seq')"),
        (
            "projection_seq",
            "SELECT nextval('projection_seq') FROM volatile_rows ORDER BY id",
        ),
        (
            "filter_seq",
            "SELECT id FROM volatile_rows WHERE nextval('filter_seq') > 0 ORDER BY id",
        ),
    ] {
        c.batch_execute(&format!(
            "CREATE SEQUENCE {name}; BEGIN; DECLARE only_one CURSOR FOR {select}"
        ))
        .await
        .unwrap();
        let rows = c.simple_query("FETCH 1 FROM only_one").await.unwrap();
        assert_eq!(
            rows.iter()
                .filter(|row| matches!(row, SimpleQueryMessage::Row(_)))
                .count(),
            1
        );
        c.batch_execute("CLOSE only_one").await.unwrap();
        let current = c
            .query_one(&format!("SELECT currval('{name}')"), &[])
            .await
            .unwrap();
        assert_eq!(current.get::<_, i64>(0), 1, "{name}");
        c.batch_execute("ROLLBACK").await.unwrap();
    }
    drop(c);
    f.stop().await;
}

#[tokio::test]
async fn fetch_errors_abort_sql_and_extended_transactions() {
    let f = Fixture::new(TlsMode::Disabled, true, false).await;
    let mut c = f.plain("owner", Some(PASSWORD)).await;
    c.batch_execute("CREATE TABLE failure_rows (id bigint PRIMARY KEY); INSERT INTO failure_rows VALUES (0),(1),(2)").await.unwrap();
    let select = "SELECT 10/(id-1) FROM failure_rows ORDER BY id";
    c.batch_execute(&format!("BEGIN; DECLARE failing CURSOR FOR {select}"))
        .await
        .unwrap();
    let error = c.simple_query("FETCH ALL FROM failing").await.unwrap_err();
    assert_eq!(error.code().unwrap().code(), "22012");
    let error = c.simple_query("SELECT 1").await.unwrap_err();
    assert_eq!(error.code().unwrap().code(), "25P02");
    c.batch_execute("ROLLBACK").await.unwrap();
    assert_eq!(
        c.query_one("SELECT 1", &[]).await.unwrap().get::<_, i32>(0),
        1
    );

    let transaction = c.transaction().await.unwrap();
    let statement = transaction.prepare(select).await.unwrap();
    let portal = transaction.bind(&statement, &[]).await.unwrap();
    let error = transaction.query_portal(&portal, 100).await.unwrap_err();
    assert_eq!(error.code().unwrap().code(), "22012");
    let error = transaction.query_one("SELECT 1", &[]).await.unwrap_err();
    assert_eq!(error.code().unwrap().code(), "25P02");
    transaction.rollback().await.unwrap();
    assert_eq!(
        c.query_one("SELECT 1", &[]).await.unwrap().get::<_, i32>(0),
        1
    );
    drop(c);
    f.stop().await;
}
