// Closed temporal SQLSTATE qualification through authenticated TLS and raw pgwire.
async fn temporal_state_cycle(
    s: &mut tokio_rustls::client::TlsStream<TcpStream>,
    query: &str,
) -> Vec<(u8, Vec<u8>)> {
    let mut q = query.as_bytes().to_vec();
    q.push(0);
    pg_send(s, b'Q', &q).await;
    let mut frames = vec![];
    loop {
        let frame = pg_read(s).await;
        let end = frame.0 == b'Z';
        frames.push(frame);
        if end {
            return frames;
        }
    }
}
fn temporal_state_frames(frames: &[(u8, Vec<u8>)], stage: &str) -> serde_json::Value {
    let sqlstate = frames
        .iter()
        .find(|f| f.0 == b'E')
        .and_then(|f| f.1.split(|b| *b == 0).find(|v| v.first() == Some(&b'C')))
        .map(|v| String::from_utf8(v[1..].to_vec()).unwrap());
    serde_json::json!({"sqlstate":sqlstate,"stage":stage,"ready":frames.iter().find(|f|f.0==b'Z').map(|f|String::from_utf8(f.1.clone()).unwrap()),"data_rows":frames.iter().filter(|f|f.0==b'D').count(),"error_responses":frames.iter().filter(|f|f.0==b'E').count(),"tags":frames.iter().map(|f|char::from(f.0).to_string()).collect::<Vec<_>>()})
}
async fn temporal_state_extended(
    s: &mut tokio_rustls::client::TlsStream<TcpStream>,
    query: &str,
) -> serde_json::Value {
    pg_send(s, b'P', &parse("", query)).await;
    pg_send(s, b'D', b"S\0").await;
    let frames = pg_sync(s).await;
    if frames.iter().any(|f| f.0 == b'E') {
        return temporal_state_frames(
            &frames,
            if frames.iter().any(|f| f.0 == b'1') {
                "describe"
            } else {
                "parse"
            },
        );
    }
    pg_send(s, b'B', &bind("", "", &[], false)).await;
    let mut execute = vec![];
    cstr("", &mut execute);
    execute.extend(0u32.to_be_bytes());
    pg_send(s, b'E', &execute).await;
    let frames = pg_sync(s).await;
    temporal_state_frames(
        &frames,
        if frames.iter().any(|f| f.0 == b'2') {
            "execute"
        } else {
            "bind"
        },
    )
}
#[tokio::test]
async fn temporal_sqlstate_authenticated_negative_wire_contracts() {
    let cases: Vec<serde_json::Value> = if let Ok(path) = std::env::var("DD68_SQLSTATE_CASES") {
        serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
    } else {
        vec![
            serde_json::json!({"id":"invalid-field","query":"SELECT date_part('nonsense',date '2000-01-01')","sqlstate":"22023"}),
            serde_json::json!({"id":"unsupported-field","query":"SELECT extract(hour from date '2000-01-01')","sqlstate":"0A000"}),
            serde_json::json!({"id":"bad-date","query":"SELECT date 'bogus'","sqlstate":"22007"}),
            serde_json::json!({"id":"bad-time","query":"SELECT make_time(24,1,0)","sqlstate":"22008"}),
            serde_json::json!({"id":"bad-arity","query":"SELECT make_time(1,2)","sqlstate":"42883"}),
            serde_json::json!({"id":"interval-range","query":"SELECT interval '2147483648 months'","sqlstate":"22015"}),
        ]
    };
    let f = Fixture::new().await;
    let mut s = raw_pg(&f).await;
    let mut report = vec![];
    for (i, case) in cases.iter().enumerate() {
        let query = case["query"].as_str().unwrap();
        let autocommit = temporal_state_extended(&mut s, query).await;
        let begin = temporal_state_cycle(&mut s, "BEGIN").await;
        assert_eq!(temporal_state_frames(&begin, "query")["ready"], "T");
        let id = 1_000_000 + i;
        let insert = temporal_state_cycle(
            &mut s,
            &format!("INSERT INTO app(id,body) VALUES({id},'rollback-probe')"),
        )
        .await;
        assert!(!insert.iter().any(|f| f.0 == b'E'));
        let transaction = temporal_state_extended(&mut s, query).await;
        let aborted =
            temporal_state_frames(&temporal_state_cycle(&mut s, "SELECT 1").await, "query");
        let rollback =
            temporal_state_frames(&temporal_state_cycle(&mut s, "ROLLBACK").await, "query");
        let effects = temporal_state_frames(
            &temporal_state_cycle(&mut s, &format!("SELECT id FROM app WHERE id={id} LIMIT 1"))
                .await,
            "query",
        );
        let stage_ok = case
            .get("stage")
            .is_none_or(|stage| *stage == autocommit["stage"]);
        let pass = autocommit["sqlstate"] == case["sqlstate"]
            && autocommit["ready"] == "I"
            && autocommit["data_rows"] == 0
            && autocommit["error_responses"] == 1
            && stage_ok
            && transaction["sqlstate"] == case["sqlstate"]
            && transaction["ready"] == "E"
            && transaction["data_rows"] == 0
            && transaction["error_responses"] == 1
            && aborted["sqlstate"] == "25P02"
            && aborted["ready"] == "E"
            && aborted["error_responses"] == 1
            && rollback["ready"] == "I"
            && rollback["sqlstate"].is_null()
            && effects["data_rows"] == 0
            && effects["ready"] == "I"
            && effects["sqlstate"].is_null();
        report.push(serde_json::json!({"id":case["id"],"query":query,"reference_sqlstate":case["sqlstate"],"reference_stage":case.get("stage"),"candidate":autocommit,"transaction":transaction,"aborted":aborted,"rollback":rollback,"no_effects":effects,"pass":pass}));
    }
    let failed: Vec<_> = report
        .iter()
        .filter(|r| r["pass"] != true)
        .map(|r| r["id"].clone())
        .collect();
    if let Ok(path) = std::env::var("DD68_SQLSTATE_REPORT") {
        std::fs::write(path, serde_json::to_vec_pretty(&report).unwrap()).unwrap()
    }
    pg_send(&mut s, b'X', &[]).await;
    drop(s);
    f.stop().await;
    assert!(failed.is_empty(), "negative wire failures: {failed:?}");
}

#[test]
fn temporal_closed_sqlstate_mapping_preserves_old_classes() {
    use giggles_core::{CoreError, relational::SqlError as E};
    for (error, state) in [
        (E::Unsupported, "0A000"),
        (E::Syntax, "42601"),
        (E::Reserved, "0A000"),
        (E::UnknownTable, "42P01"),
        (E::UnknownColumn, "42703"),
        (E::Duplicate, "23505"),
        (E::Type, "42804"),
        (E::Null, "23502"),
        (E::Parameters, "42804"),
        (E::Limit, "54000"),
        (E::Unbounded, "54000"),
        (E::Version, "40001"),
        (E::Conflict, "40001"),
        (E::StaleSchema, "0A000"),
        (E::Transaction, "25000"),
        (E::Aborted, "25P02"),
        (E::Overflow, "22003"),
        (E::UndefinedFunction, "42883"),
        (E::InvalidParameterValue, "22023"),
        (E::InvalidDatetimeFormat, "22007"),
        (E::DatetimeFieldOverflow, "22008"),
        (E::IntervalFieldOverflow, "22015"),
    ] {
        assert_eq!(giggles_server::sqlstate(&CoreError::Sql(error)), state);
    }
}
#[tokio::test]
async fn temporal_sqlstate_authenticated_simple_query_contracts() {
    let cases: Vec<serde_json::Value> = if let Ok(path) = std::env::var("DD68_SQLSTATE_CASES") {
        serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
    } else {
        vec![
            serde_json::json!({"id":"invalid-field","query":"SELECT date_part('nonsense',date '2000-01-01')","sqlstate":"22023"}),
            serde_json::json!({"id":"unsupported-field","query":"SELECT extract(hour from date '2000-01-01')","sqlstate":"0A000"}),
            serde_json::json!({"id":"bad-date","query":"SELECT date 'bogus'","sqlstate":"22007"}),
            serde_json::json!({"id":"bad-time","query":"SELECT make_time(24,1,0)","sqlstate":"22008"}),
            serde_json::json!({"id":"bad-arity","query":"SELECT make_time(1,2)","sqlstate":"42883"}),
            serde_json::json!({"id":"interval-range","query":"SELECT interval '2147483648 months'","sqlstate":"22015"}),
        ]
    };
    let f = Fixture::new().await;
    let mut s = raw_pg(&f).await;
    let mut report = vec![];
    for (i, case) in cases.iter().enumerate() {
        let query = case["query"].as_str().unwrap();
        let autocommit = temporal_state_frames(&temporal_state_cycle(&mut s, query).await, "query");
        let begin = temporal_state_cycle(&mut s, "BEGIN").await;
        assert_eq!(temporal_state_frames(&begin, "query")["ready"], "T");
        let id = 1_000_000 + i;
        let insert = temporal_state_cycle(
            &mut s,
            &format!("INSERT INTO app(id,body) VALUES({id},'rollback-probe')"),
        )
        .await;
        assert!(!insert.iter().any(|f| f.0 == b'E'));
        let transaction =
            temporal_state_frames(&temporal_state_cycle(&mut s, query).await, "query");
        let aborted =
            temporal_state_frames(&temporal_state_cycle(&mut s, "SELECT 1").await, "query");
        let rollback =
            temporal_state_frames(&temporal_state_cycle(&mut s, "ROLLBACK").await, "query");
        let effects = temporal_state_frames(
            &temporal_state_cycle(&mut s, &format!("SELECT id FROM app WHERE id={id} LIMIT 1"))
                .await,
            "query",
        );
        let stage_ok = case
            .get("stage")
            .is_none_or(|stage| *stage == autocommit["stage"]);
        let pass = autocommit["sqlstate"] == case["sqlstate"]
            && autocommit["ready"] == "I"
            && autocommit["data_rows"] == 0
            && autocommit["error_responses"] == 1
            && stage_ok
            && transaction["sqlstate"] == case["sqlstate"]
            && transaction["ready"] == "E"
            && transaction["data_rows"] == 0
            && transaction["error_responses"] == 1
            && aborted["sqlstate"] == "25P02"
            && aborted["ready"] == "E"
            && aborted["error_responses"] == 1
            && rollback["ready"] == "I"
            && rollback["sqlstate"].is_null()
            && effects["data_rows"] == 0
            && effects["ready"] == "I"
            && effects["sqlstate"].is_null();
        report.push(serde_json::json!({"id":case["id"],"query":query,"reference_sqlstate":case["sqlstate"],"reference_stage":case.get("stage"),"candidate":autocommit,"transaction":transaction,"aborted":aborted,"rollback":rollback,"no_effects":effects,"pass":pass}));
    }
    let failed: Vec<_> = report
        .iter()
        .filter(|r| r["pass"] != true)
        .map(|r| r["id"].clone())
        .collect();
    if let Ok(path) = std::env::var("DD68_SQLSTATE_SIMPLE_REPORT") {
        std::fs::write(path, serde_json::to_vec_pretty(&report).unwrap()).unwrap()
    }
    pg_send(&mut s, b'X', &[]).await;
    drop(s);
    f.stop().await;
    assert!(failed.is_empty(), "negative wire failures: {failed:?}");
}

#[tokio::test]
async fn temporal_sqlstate_preserves_dependency_and_task_authorization() {
    let f = Fixture::new().await;
    let (app, connection) = f.pg(f.app.clone()).await;
    for (q, state) in [
        ("SELECT unknown_routine(1)", "0A000"),
        ("SELECT public.isfinite(date '2000-01-01')", "42501"),
        ("SELECT id FROM other LIMIT 1", "42501"),
        (
            "SELECT a.id FROM app a JOIN other b ON a.id=b.id LIMIT 1",
            "42501",
        ),
    ] {
        assert_eq!(code(app.prepare(q).await.unwrap_err()), state, "{q}");
    }
    drop(app);
    connection.await.unwrap();
    let (task, connection) = f.pg(f.task.clone()).await;
    for q in [
        "SELECT isfinite(date '2000-01-01')",
        "SELECT make_time(1,2)",
        "SELECT date 'bogus'",
    ] {
        assert_eq!(code(task.prepare(q).await.unwrap_err()), "42501", "{q}");
    }
    drop(task);
    connection.await.unwrap();
    f.stop().await;
}
