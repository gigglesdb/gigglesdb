use super::postgresql_network::Fixture;
use giggles_server::auth::TlsMode;

#[tokio::test]
async fn parameters_lateral_ordinality_and_pattern_diagnostics() {
    let fixture = Fixture::new(TlsMode::Disabled, true, false).await;
    let client = fixture.plain("owner", None).await;
    client
        .batch_execute("INSERT INTO app VALUES (1,'a12b34'),(2,'none'),(3,NULL)")
        .await
        .unwrap();
    let rows = client
        .query(
            "SELECT id FROM app WHERE body ~ $1 ORDER BY id",
            &[&r"[0-9]+"],
        )
        .await
        .unwrap();
    assert_eq!(
        rows.iter().map(|r| r.get::<_, i64>(0)).collect::<Vec<_>>(),
        [1]
    );

    let rows = client
        .query(
            "SELECT id,m,ord FROM app CROSS JOIN LATERAL regexp_matches(body,$1,'g')
         WITH ORDINALITY AS r(m,ord) ORDER BY id,ord",
            &[&r"([0-9]+)"],
        )
        .await
        .unwrap();
    let values = rows
        .iter()
        .map(|r| {
            (
                r.get::<_, i64>(0),
                r.get::<_, Vec<String>>(1),
                r.get::<_, i64>(2),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        values,
        [
            (1, vec![String::from("12")], 1),
            (1, vec![String::from("34")], 2)
        ]
    );

    let error = client
        .query("SELECT id FROM app WHERE body ~ $1", &[&"("])
        .await
        .unwrap_err();
    let error = error.as_db_error().unwrap();
    assert_eq!(error.code().code(), "2201B");
    assert_eq!(
        error.message(),
        "invalid regular expression: parentheses () not balanced"
    );
    assert_eq!(error.detail(), None);
    assert_eq!(error.hint(), None);
    let error = client
        .simple_query("SELECT 'x' SIMILAR TO 'x' ESCAPE 'ab'")
        .await
        .unwrap_err();
    let error = error.as_db_error().unwrap();
    assert_eq!(error.code().code(), "22025");
    assert_eq!(error.message(), "invalid escape string");
    assert_eq!(error.detail(), None);
    assert_eq!(
        error.hint(),
        Some("Escape string must be empty or one character.")
    );

    let error = client
        .simple_query("SELECT regexp_match('abc','b','g')")
        .await
        .unwrap_err();
    let error = error.as_db_error().unwrap();
    assert_eq!(error.code().code(), "22023");
    assert_eq!(
        error.message(),
        r#"regexp_match() does not support the "global" option"#
    );
    assert_eq!(error.detail(), None);
    assert_eq!(
        error.hint(),
        Some("Use the regexp_matches function instead.")
    );

    drop(client);
    fixture.stop().await;
}
