#[tokio::test]
async fn dd68_negative_scalar_routing_tls_redaction_and_authority() {
    use sha2::{Digest, Sha256};
    let f = Fixture::new().await;
    let (pg, conn) = f.pg(f.app.clone()).await;
    let sql = "SELECT private_probe('never_log_this_secret')";
    // Simple query and extended Parse must preserve the same negative contract.
    let simple = pg.simple_query(sql).await.unwrap_err();
    let extended = pg.prepare(sql).await.unwrap_err();
    for error in [&simple, &extended] {
        let error = error.as_db_error().unwrap();
        assert_eq!(error.code().code(), "0A000");
        assert!(error.message().contains("unsupported SQL feature"));
        let detail = error.detail().unwrap();
        assert!(detail.contains("phase=catalog planning"));
        assert!(detail.contains(&hex::encode(Sha256::digest(sql.as_bytes()))));
        for secret in ["private_probe", "never_log_this_secret", "SELECT"] {
            assert!(!error.message().contains(secret));
            assert!(!detail.contains(secret));
        }
    }
    assert_eq!(
        pg.query_one("SELECT lower('ABC'),abs(-7)", &[])
            .await
            .unwrap()
            .get::<_, String>(0),
        "abc"
    );
    for sql in [
        "SELECT SUM(abs(-1))",
        "SELECT COUNT(lower('x'))",
        "SELECT lower(body)",
        "SELECT lower(body) FROM other",
        "SELECT lower(app.body) FROM app JOIN other ON app.id=other.id",
    ] {
        assert_eq!(
            code(pg.query(sql, &[]).await.unwrap_err()),
            "42501",
            "{sql}"
        );
    }
    drop(pg);
    conn.await.unwrap();
    f.stop().await;
}

#[tokio::test]
async fn dd68_negative_scalar_task_original_candidate_capture() {
    let f = Fixture::new().await;
    let sql = "SELECT private_probe('never_log_this_secret')";
    let (task, task_conn) = f.pg(f.task.clone()).await;
    assert_eq!(
        code(task.query("SELECT lower('ABC')", &[]).await.unwrap_err()),
        "42501"
    );
    // Capture the original and candidate result with this same witness; do not
    // infer a grant or change its expected code before comparing both captures.
    let task_unknown = task.query(sql, &[]).await.unwrap_err();
    let task_unknown = task_unknown.as_db_error().unwrap();
    println!(
        "task_unknown_code={} detail={}",
        task_unknown.code().code(),
        task_unknown.detail().unwrap_or("")
    );
    for secret in ["private_probe", "never_log_this_secret", "SELECT"] {
        assert!(!task_unknown.message().contains(secret));
        assert!(!task_unknown.detail().unwrap_or("").contains(secret));
    }
    drop(task);
    task_conn.await.unwrap();
    f.stop().await;
}
