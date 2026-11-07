#[tokio::test]
async fn dd68_postjoint_scalar_routing_tls_authority_witness() {
    let f = Fixture::new().await;
    let (pg, conn) = f.pg(f.app.clone()).await;
    assert_eq!(pg.query_one("SELECT 1", &[]).await.unwrap().get::<_, i32>(0), 1);
    let row = pg.query_one("SELECT lower('ABC'),abs(-7)", &[]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "abc");
    assert_eq!(row.get::<_, i32>(1), 7);
    pg.execute("INSERT INTO app (id,body) VALUES ($1,$2)", &[&71i64,&"ABC"]).await.unwrap();
    let row = pg.query_one("SELECT lower(body),abs(id) FROM app WHERE id=71", &[]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "abc");
    assert_eq!(row.get::<_, i64>(1), 71);
    assert!(pg.query("SELECT unknown_namespace.lower('ABC')", &[]).await.is_err());
    assert_eq!(code(pg.query("SELECT lower(body) FROM other WHERE id=71", &[]).await.unwrap_err()), "42501");
    let (task, task_conn) = f.pg(f.task.clone()).await;
    assert_eq!(code(task.query("SELECT lower('ABC')", &[]).await.unwrap_err()), "42501");
    drop(task); task_conn.await.unwrap();
    drop(pg); conn.await.unwrap();
    let mut revoked = raw_pg(&f).await;
    pg_send(&mut revoked, b'P', &parse("kept", "SELECT lower('KEPT')")).await;
    let parsed = pg_sync(&mut revoked).await;
    assert!(parsed.iter().any(|(tag,_)| *tag==b'1'));
    pg_send(&mut revoked, b'B', &bind("kept", "kept", &[], false)).await;
    pg_send(&mut revoked, b'H', &[]).await;
    assert_eq!(pg_read(&mut revoked).await.0, b'2');
    f.service.reload_bindings(vec![]).unwrap();
    pg_send(&mut revoked, b'E', &execute("kept",0)).await;
    match tokio::time::timeout(std::time::Duration::from_secs(5), revoked.read_u8())
        .await.expect("revocation must terminate or reject, not time out") {
        Ok(tag) => {
            assert_ne!(tag, b'D', "revoked portal disclosed a row");
            assert_eq!(tag, b'E');
            let n = revoked.read_u32().await.unwrap();
            assert!((4..=8 * 1024 * 1024).contains(&n));
            let mut body = vec![0; n as usize - 4];
            revoked.read_exact(&mut body).await.unwrap();
            assert!(has_code(&[(tag, body)], "28000"));
            println!("revocation=authentication_error_28000");
        }
        Err(e) => {
            assert!(matches!(e.kind(), std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::ConnectionReset),
                "unexpected revocation transport error: {e}");
            println!("revocation_transport={:?}",e.kind());
        }
    }
    assert_eq!(f.admin.sql_session().execute("SELECT COUNT(*) FROM app", &[], None).unwrap().rows[0].values,
        vec![Value::Int(1)]);
    drop(revoked);
    f.stop().await;
}

#[tokio::test]
async fn dd68_postjoint_immutable_bind_and_dependent_execute_tls_phases() {
    let f = Fixture::new().await;
    for (sql, params, bind_error, execute_error) in [
        ("SELECT abs(-2147483648)", vec![], Some("22003"), None),
        ("SELECT date_trunc('nonsense',timestamptz '2000-01-01 UTC')", vec![], None, Some("22023")),
        ("SELECT abs($1::integer)", vec![Some("-2147483648")], None, Some("22003")),
        ("SELECT coalesce($1::integer,abs(-2147483648))", vec![Some("1")], None, None),
    ] {
        let mut wire = raw_pg(&f).await;
        pg_send(&mut wire, b'P', &parse("s", sql)).await;
        let parsed = pg_sync(&mut wire).await;
        assert!(parsed.iter().any(|(tag,_)| *tag==b'1'), "Parse {sql}: {parsed:?}");
        assert!(!parsed.iter().any(|(tag,_)| *tag==b'E'), "Parse {sql}: {parsed:?}");
        pg_send(&mut wire, b'B', &bind("p", "s", &params, false)).await;
        pg_send(&mut wire, b'H', &[]).await;
        let bound = vec![pg_read(&mut wire).await];
        if let Some(expected) = bind_error {
            assert!(has_code(&bound, expected), "Bind {sql}: {bound:?}");
            assert!(!bound.iter().any(|(tag,_)| *tag==b'2'));
            let _ = pg_sync(&mut wire).await;
        } else {
            assert!(bound.iter().any(|(tag,_)| *tag==b'2'), "Bind {sql}: {bound:?}");
            assert!(!bound.iter().any(|(tag,_)| *tag==b'E'), "Bind {sql}: {bound:?}");
            pg_send(&mut wire, b'E', &execute("p",0)).await;
            let executed = pg_sync(&mut wire).await;
            if let Some(expected) = execute_error {
                assert!(has_code(&executed, expected), "Execute {sql}: {executed:?}");
            } else {
                assert!(executed.iter().any(|(tag,_)| *tag==b'D'), "Execute {sql}: {executed:?}");
                assert!(!executed.iter().any(|(tag,_)| *tag==b'E'), "Execute {sql}: {executed:?}");
            }
        }
    }
    f.stop().await;
}
