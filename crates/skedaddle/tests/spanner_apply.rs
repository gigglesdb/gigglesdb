#[path = "support/spanner.rs"]
mod spanner_support;

use google_cloud_spanner::{client::Spanner, value::Value};
use skedaddle::{
    definition::SpannerDialect,
    event::{AssembledTxn, Position, SourceIdentity, TxnId},
    spanner_dest::{
        apply::{CommitFault, Decision, Destination, Error, Write},
        limits::{self, Config, Kind},
    },
};
use std::{collections::BTreeMap, io::Write as _, time::Duration};
type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

async fn ddl(root: &Spanner, database: &str, sql: &[String]) -> TestResult {
    let admin = root.database_admin_builder().build().await?;
    let mut op = admin
        .update_database_ddl()
        .set_database(database)
        .set_statements(sql.to_vec())
        .send()
        .await?;
    for _ in 0..600 {
        if op.done {
            assert!(op.error().is_none(), "DDL operation failed");
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        op = admin.get_operation().set_name(&op.name).send().await?;
    }
    Err("DDL timeout".into())
}
fn transaction(id: u64, payload: &str) -> AssembledTxn {
    let source = SourceIdentity {
        engine: "pg".into(),
        system_id: "s11".into(),
        lineage: "1".into(),
        db_name: "source".into(),
        stream: "items".into(),
    };
    // Distinct content under the same source identity/txn exercises hard conflict.
    AssembledTxn::new(
        source,
        TxnId(id.to_be_bytes().to_vec()),
        Position::new("pg:s11", id.to_be_bytes()),
        Some(payload.len() as i64),
        vec![],
    )
    .unwrap()
}
fn write(id: i64, payload: &str, kind: Kind) -> Write {
    let mut values = BTreeMap::from([("id".into(), Value::from(id))]);
    if kind != Kind::Delete {
        values.insert("payload".into(), Value::from(payload));
    }
    Write {
        table: "s11_items".into(),
        kind,
        values,
    }
}
async fn payload(dest: &Destination, id: i64) -> Option<String> {
    let mut rows = dest
        .database
        .single_use()
        .build()
        .execute_query(format!("SELECT payload FROM s11_items WHERE id={id}"))
        .await
        .unwrap();
    rows.next().await.map(|r| r.unwrap().get(0).unwrap())
}
async fn run(dialect: SpannerDialect, name: &str, custom_table: bool) -> TestResult {
    let Ok(path) = std::env::var("SKEDADDLE_SPANNER_CONFIG") else {
        writeln!(
            std::io::stderr(),
            "SPANNER EMULATOR SKIPPED: set SKEDADDLE_SPANNER_CONFIG via harness/run.ps1 -Phase spanner-emulator -SpannerCargoTest '-p skedaddle --test spanner_apply -- --nocapture'"
        )?;
        return Ok(());
    };
    let config: serde_json::Value = serde_json::from_slice(&std::fs::read(path)?)?;
    let isolated = spanner_support::Database::create(config, name).await?;
    let config = isolated.config();
    let replay_table = if custom_table {
        "S11_Replay"
    } else {
        limits::DEFAULT_REPLAY_TABLE
    };
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
    // Emulator maps fixed 9010->9020 only; ephemeral harness ports need the
    // separate official admin endpoint supplied by the harness.
    let admin_root = Spanner::builder()
        .with_endpoint(config["endpoint"].as_str().unwrap())
        .with_credentials(credentials())
        .build()
        .await?;
    let mut dest = Destination {
        database: root.database_client(database).build().await?,
        dialect,
        config: Config {
            replay_table: replay_table.into(),
            ..Config::default()
        },
        fault: CommitFault::None,
    };
    dest.install(&admin_root, database)
        .await
        .map_err(|e| format!("{name} install: {e}"))?;
    eprintln!("{name}: replay table installed");
    dest.install(&admin_root, database).await?;
    assert!(dest.catalog().await?.contains_key(replay_table));
    let reserved = Write {
        table: replay_table.into(),
        kind: Kind::Insert,
        values: BTreeMap::new(),
    };
    assert!(matches!(
        dest.apply(&transaction(99, "reserved"), &[reserved]).await,
        Err(Error::Invalid("reserved table"))
    ));
    let table = if dialect == SpannerDialect::GoogleSql {
        "CREATE TABLE s11_items (id INT64 NOT NULL,payload STRING(100),extra STRING(100)) PRIMARY KEY(id)"
    } else {
        "CREATE TABLE s11_items (id bigint PRIMARY KEY,payload varchar(100),extra varchar(100))"
    };
    ddl(
        &admin_root,
        database,
        &[
            table.into(),
            "CREATE INDEX s11_payload ON s11_items(payload)".into(),
        ],
    )
    .await?;
    let first = transaction(1, "initial");
    assert_eq!(
        dest.apply(&first, &[write(1, "initial", Kind::Insert)])
            .await?,
        Decision::Applied
    );
    assert_eq!(
        dest.apply(&first, &[write(1, "initial", Kind::Insert)])
            .await?,
        Decision::Replayed
    );
    assert!(matches!(
        dest.apply(
            &transaction(1, "different"),
            &[write(1, "conflict", Kind::Update)]
        )
        .await,
        Err(Error::Conflict)
    ));
    assert_eq!(payload(&dest, 1).await.as_deref(), Some("initial"));
    let update = transaction(2, "updated");
    dest.apply(&update, &[write(1, "updated", Kind::Update)])
        .await?;
    assert_eq!(
        dest.apply(&update, &[write(1, "updated", Kind::Update)])
            .await?,
        Decision::Replayed
    );
    assert_eq!(payload(&dest, 1).await.as_deref(), Some("updated"));
    // Fault injection alongside the real official-client commit path above.
    dest.fault = CommitFault::LoseReplyOnce;
    let lost = transaction(3, "lost");
    assert_eq!(
        dest.apply(&lost, &[write(2, "lost", Kind::Insert)]).await?,
        Decision::Replayed
    );
    assert_eq!(
        dest.apply(&lost, &[write(2, "lost", Kind::Insert)]).await?,
        Decision::Replayed
    );
    assert_eq!(payload(&dest, 2).await.as_deref(), Some("lost"));
    let delete = transaction(4, "delete");
    dest.apply(&delete, &[write(1, "", Kind::Delete)]).await?;
    assert_eq!(
        dest.apply(&delete, &[write(1, "", Kind::Delete)]).await?,
        Decision::Replayed
    );
    assert!(payload(&dest, 1).await.is_none());
    let catalog = dest.catalog().await?;
    assert!(!catalog["s11_items"].indexes.is_empty());
    let boundary = write(3, "limit", Kind::Insert);
    let count = limits::cells(
        &catalog["s11_items"],
        Kind::Insert,
        &boundary.values.keys().cloned().collect(),
    ) + limits::REPLAY_CELLS;
    dest.config.mutation_limit = count;
    dest.apply(&transaction(5, "limit"), &[boundary]).await?;
    assert_eq!(payload(&dest, 3).await.as_deref(), Some("limit"));
    let mut above = write(4, "blocked", Kind::Insert);
    above
        .values
        .insert("extra".into(), Value::from("one more cell"));
    let blocked = transaction(6, "blocked");
    assert!(matches!(dest.apply(&blocked, &[above.clone()]).await,
        Err(Error::Limit(limits::LimitExceeded { estimated, limit })) if estimated == count + 1 && limit == count));
    assert!(payload(&dest, 4).await.is_none());
    // Absent replay row too: raising admission now performs the transaction.
    dest.config.mutation_limit += 1;
    assert_eq!(dest.apply(&blocked, &[above]).await?, Decision::Applied);
    let quote = if dialect == SpannerDialect::GoogleSql {
        '`'
    } else {
        '"'
    };
    let mut rows = dest
        .database
        .single_use()
        .build()
        .execute_query(format!("SELECT COUNT(*) FROM {quote}{replay_table}{quote}"))
        .await?;
    let receipts: i64 = rows.next().await.unwrap()?.get(0)?;
    assert_eq!(receipts, 6);
    Ok(())
}
#[tokio::test]
async fn google_sql_apply_replay_lost_reply_and_limits() -> TestResult {
    run(SpannerDialect::GoogleSql, "GOOGLE_STANDARD_SQL", false).await
}
#[tokio::test]
async fn postgresql_apply_replay_lost_reply_and_limits() -> TestResult {
    run(SpannerDialect::Postgresql, "POSTGRESQL", true).await
}
