#[path = "support/spanner.rs"]
mod spanner_support;

use google_cloud_spanner::client::Spanner;
use serde_json::json;
use skedaddle::{
    definition::{ObjectKind, ObjectSelector, SpannerDialect},
    pg::preflight::{Check, Observation, Snapshot},
    spanner_dest::{
        apply::{CommitFault, Destination},
        limits::Config,
        preflight::{self, Options, Rule},
    },
};
use std::{io::Write as _, time::Duration};
type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

async fn ddl(root: &Spanner, database: &str, statements: Vec<String>) -> TestResult {
    let admin = root.database_admin_builder().build().await?;
    let mut op = admin
        .update_database_ddl()
        .set_database(database)
        .set_statements(statements)
        .send()
        .await?;
    for _ in 0..600 {
        if op.done {
            assert!(op.error().is_none(), "DDL error: {:?}", op.error());
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        op = admin.get_operation().set_name(&op.name).send().await?;
    }
    Err("DDL timeout".into())
}
fn source(name: &str) -> (Snapshot, Vec<ObjectSelector>) {
    let o = ObjectSelector {
        schema: "public".into(),
        name: name.into(),
        kind: ObjectKind::Table,
        exclude_columns: vec![],
    };
    let obs = |check, rows| Observation {
        check,
        object: Some(o.clone()),
        sql: "constructed PG catalog descriptor".into(),
        rows,
        error: None,
    };
    let column = |n| json!({"column":n,"oid":20,"schema":"pg_catalog","name":"int8","kind":"b","base_oid":0,"element_oid":0,"typmod":-1});
    (
        Snapshot {
            observations: vec![
                obs(Check::ColumnTypes, vec![column("id"), column("v")]),
                obs(
                    Check::Key,
                    vec![
                        json!({"primary":true,"unique":true,"valid":true,"ready":true,"partial":false,"expression":false,"columns":["id"]}),
                    ],
                ),
                obs(Check::SchemaDescriptor, vec![json!({"constraints":null})]),
            ],
        },
        vec![o],
    )
}
async fn run(dialect: SpannerDialect, name: &str) -> TestResult {
    let Ok(path) = std::env::var("SKEDADDLE_SPANNER_CONFIG") else {
        writeln!(
            std::io::stderr(),
            "SPANNER EMULATOR SKIPPED: use harness/run.ps1 -Phase spanner-emulator -SpannerCargoTest '-p skedaddle --test spanner_dest_preflight -- --nocapture'"
        )?;
        return Ok(());
    };
    let config: serde_json::Value = serde_json::from_slice(&std::fs::read(path)?)?;
    let isolated = spanner_support::Database::create(config, name).await?;
    let config = isolated.config();
    let database = config["databases"][name]["database"].as_str().unwrap();
    let credentials = || google_cloud_auth::credentials::anonymous::Builder::new().build();
    let root = Spanner::builder()
        .with_endpoint(format!(
            "http://{}",
            config["grpc_endpoint"].as_str().unwrap()
        ))
        .with_credentials(credentials())
        .build()
        .await?;
    let admin = Spanner::builder()
        .with_endpoint(config["endpoint"].as_str().unwrap())
        .with_credentials(credentials())
        .build()
        .await?;
    let mut dest = Destination {
        database: root.database_client(database).build().await?,
        dialect,
        config: Config::default(),
        fault: CommitFault::None,
    };
    let google = dialect == SpannerDialect::GoogleSql;
    let table = |n: &str, v: &str, key: &str, extra: &str| {
        if google {
            format!("CREATE TABLE {n} (id INT64 NOT NULL,v {v}{extra}) PRIMARY KEY({key})")
        } else {
            format!("CREATE TABLE {n} (id bigint NOT NULL,v {v}{extra},PRIMARY KEY({key}))")
        }
    };
    let int = if google { "INT64" } else { "bigint" };
    let string = if google { "STRING(MAX)" } else { "varchar" };
    let generated = if google {
        "INT64 AS (id+1) STORED"
    } else {
        "bigint GENERATED ALWAYS AS (id+1) STORED"
    };
    ddl(
        &admin,
        database,
        vec![
            table("s10_ok", int, "id", ""),
            table("s10_key", &format!("{int} NOT NULL"), "v,id", ""),
            table("s10_parent", int, "id", ""),
            table(
                "s10_fk",
                int,
                "id",
                ",CONSTRAINT s10_fk_constraint FOREIGN KEY(v) REFERENCES s10_parent(id)",
            ),
            table("s10_unique", int, "id", ""),
            "CREATE UNIQUE INDEX s10_unique_index ON s10_unique(v)".into(),
            table(
                "s10_check",
                int,
                "id",
                ",CONSTRAINT s10_check_constraint CHECK(v>0)",
            ),
            table("s10_generated", generated, "id", ""),
            table("s10_type", string, "id", ""),
            table("s10_conflict", int, "id", ""),
        ],
    )
    .await?;
    let opts = Options {
        worst_case_rows: Some(1),
        ..Default::default()
    };
    // Free replay name and already Skedaddle-shaped replay table both pass.
    let (s, o) = source("s10_ok");
    let report = preflight::inspect(&s, &o, &dest, &opts).await?;
    assert_eq!(report.blockers().count(), 0, "{name}: {:?}", report.issues);
    dest.install(&admin, database).await?;
    let report = preflight::inspect(&s, &o, &dest, &opts).await?;
    assert_eq!(report.blockers().count(), 0, "{name}: {:?}", report.issues);
    for (n, expected) in [
        ("s10_missing", Rule::MissingTable),
        ("s10_key", Rule::PrimaryKey),
        ("s10_fk", Rule::ExtraForeignKey),
        ("s10_unique", Rule::ExtraUnique),
        ("s10_check", Rule::ExtraCheck),
        ("s10_generated", Rule::GeneratedWrite),
        ("s10_type", Rule::ColumnType),
    ] {
        let (s, o) = source(n);
        let report = preflight::inspect(&s, &o, &dest, &opts).await?;
        assert_eq!(
            report.blockers().map(|i| i.rule).collect::<Vec<_>>(),
            [expected],
            "{name} {n}: {:?}",
            report.issues
        );
    }
    let huge = Options {
        worst_case_rows: Some(u64::MAX),
        ..Default::default()
    };
    let report = preflight::inspect(&s, &o, &dest, &huge).await?;
    assert_eq!(
        report.blockers().map(|i| i.rule).collect::<Vec<_>>(),
        [Rule::MutationLimit]
    );
    dest.config.replay_table = "s10_conflict".into();
    let report = preflight::inspect(&s, &o, &dest, &opts).await?;
    assert_eq!(
        report.blockers().map(|i| i.rule).collect::<Vec<_>>(),
        [Rule::ReplayShape]
    );
    eprintln!("{name}: free/existing replay, compliant and all nine blocking scenarios passed");
    Ok(())
}
#[tokio::test]
async fn google_sql_destination_preflight() -> TestResult {
    run(SpannerDialect::GoogleSql, "GOOGLE_STANDARD_SQL").await
}
#[tokio::test]
async fn postgresql_destination_preflight() -> TestResult {
    run(SpannerDialect::Postgresql, "POSTGRESQL").await
}
