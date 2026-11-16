//! Both collectors must produce identical JSON on real harness PostgreSQL.
use serde_json::Value;
use skedaddle::{
    definition::{Engine, ObjectKind, ObjectSelector},
    pg::{preflight, tls},
};
#[tokio::test]
async fn postgres_and_gigglesdb_collection_are_equivalent() {
    let Some(config) = std::env::var_os("SKEDADDLE_TEST_CONFIG") else {
        eprintln!("SKIP gigglesdb_collect: requires real PostgreSQL harness; equivalence NOT RUN");
        return;
    };
    let cfg: Value = serde_json::from_slice(&std::fs::read(config).unwrap()).unwrap();
    let (mut client, driver, _) = tls::connect(
        &tls::Config::new(
            cfg["pg_ddl"].as_str().unwrap().parse().unwrap(),
            cfg["ca_pem"].as_str().unwrap(),
        ),
        &skedaddle::security::TlsPolicy::Required,
    )
    .await
    .unwrap();
    let driver = tokio::spawn(driver);
    let schema = format!("n5d{}", uuid::Uuid::new_v4().simple());
    client.batch_execute(&format!("CREATE SCHEMA {schema}; CREATE TABLE {schema}.items(id bigint PRIMARY KEY, body text, doc jsonb, numbers integer[]); INSERT INTO {schema}.items VALUES (1,'quote''test',NULL,ARRAY[1,2])")).await.unwrap();
    let objects = vec![ObjectSelector {
        schema: schema.clone(),
        name: "items".into(),
        kind: ObjectKind::Table,
        exclude_columns: vec![],
    }];
    let pg = preflight::collect_destination(&mut client, &objects, Engine::Postgresql)
        .await
        .unwrap();
    let gd = preflight::collect_destination(&mut client, &objects, Engine::Gigglesdb)
        .await
        .unwrap();
    let mut failures = vec![];
    for (left, right) in pg.observations.iter().zip(&gd.observations) {
        if left.check != right.check
            || left.object != right.object
            || left.rows != right.rows
            || left.error != right.error
            || left.error.is_some()
        {
            failures.push(format!(
                "{:?}: PG {:?} {:?}, GD {:?} {:?}",
                left.check, left.rows, left.error, right.rows, right.error
            ));
        }
    }
    assert_eq!(pg.observations.len(), gd.observations.len());
    client
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .await
        .unwrap();
    driver.abort();
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    eprintln!(
        "PASS equivalence: {} observations, all successful",
        pg.observations.len()
    );
}
