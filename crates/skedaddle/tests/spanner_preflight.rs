//! Both dialects against the repository's labelled, ephemeral emulator harness.
#[path = "support/spanner.rs"]
mod spanner_support;

use google_cloud_spanner::client::Spanner;
use skedaddle::{
    buffer::{
        backends::ObjectBufferStore,
        writer::{BufferWriter, WriterCheckpoint, WriterConfig},
    },
    definition::{ObjectKind, ObjectSelector, SecretRef, SpannerDialect, SpannerEndpoint},
    engine::Capture,
    spanner::{
        capture::SpannerCapture,
        client::{Client, Error},
        frontier::{DuplicateFilter, Frontier, SpannerOrder},
        preflight::{self, Options, Rule},
        schema::{self, Guard},
        stream::{Config, PartitionReader, timestamp},
    },
};
use std::{
    io::Write as _,
    sync::{Arc, mpsc},
    time::Duration,
};

async fn ddl(admin: &Spanner, db: &str, sql: &str) -> Result<(), Error> {
    let service = admin.database_admin_builder().build().await?;
    let mut op = service
        .update_database_ddl()
        .set_database(db)
        .set_statements([sql])
        .send()
        .await?;
    for _ in 0..600 {
        if op.done {
            return if let Some(e) = op.error() {
                Err(format!("{e:?}").into())
            } else {
                Ok(())
            };
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        op = service.get_operation().set_name(&op.name).send().await?;
    }
    Err("DDL timed out".into())
}
fn unavailable_option(
    mut rows: schema::Metadata,
    selected: &[ObjectSelector],
    stream: &str,
    rule: Rule,
    google: bool,
) -> Result<(), Error> {
    let option_name = if rule == Rule::TtlDeletes {
        "exclude_ttl_deletes"
    } else {
        "allow_txn_exclusion"
    };
    let opts = rows.entry("change_stream_options".into()).or_default();
    opts.retain(|r| {
        schema::text(r, "change_stream_name") != stream
            || schema::text(r, "option_name") != option_name
    });
    opts.push(
        [
            ("change_stream_schema", if google { "" } else { "public" }),
            ("change_stream_name", stream),
            ("option_name", option_name),
            ("option_value", "true"),
        ]
        .into_iter()
        .map(|(k, v)| (k.into(), Some(v.into())))
        .collect(),
    );
    let mut options = Options::default();
    for table in selected {
        options.sizes.insert(
            schema::key(&table.schema, &table.name),
            preflight::Size {
                rows: 1,
                bytes: 1024,
            },
        );
    }
    assert_eq!(
        preflight::assess(&rows, selected, stream, &options)?
            .issues
            .iter()
            .map(|i| (i.rule, i.blocking))
            .collect::<Vec<_>>(),
        [(rule, true)]
    );
    Ok(())
}
async fn run(dialect: SpannerDialect, name: &str) -> Result<(), Error> {
    let Ok(path) = std::env::var("SKEDADDLE_SPANNER_CONFIG") else {
        writeln!(
            std::io::stderr(),
            "SPANNER EMULATOR SKIPPED: run harness/run.ps1 -Phase spanner-emulator -SpannerCargoTest '-p skedaddle --test spanner_preflight -- --nocapture'"
        )?;
        return Ok(());
    };
    let c: serde_json::Value = serde_json::from_slice(&std::fs::read(path)?)?;
    let isolated = spanner_support::Database::create(c, name).await?;
    let c = isolated.config();
    let db = c["databases"][name]["database"].as_str().unwrap();
    let google = dialect == SpannerDialect::GoogleSql;
    let mut endpoint = SpannerEndpoint {
        project: c["project"].as_str().unwrap().into(),
        instance: c["instance"].as_str().unwrap().into(),
        database: db.rsplit('/').next().unwrap().into(),
        dialect,
        change_stream: "changes".into(),
        credentials: SecretRef::parse("env:UNRESOLVED_S7_TEST")?,
        emulator: true,
    };
    let host = c["grpc_endpoint"].as_str().unwrap();
    let client = Arc::new(Client::connect(&endpoint, Some(host)).await?);
    let admin = Spanner::builder()
        .with_endpoint(c["endpoint"].as_str().unwrap())
        .with_credentials(google_cloud_auth::credentials::anonymous::Builder::new().build())
        .build()
        .await?;
    let selected = vec![ObjectSelector {
        schema: if google { "" } else { "public" }.into(),
        name: "parents".into(),
        kind: ObjectKind::Table,
        exclude_columns: vec![],
    }];
    let report = preflight::inspect(&client, &selected, &Options::default()).await?;
    assert_eq!(report.blockers().count(), 0, "{name}: {:?}", report.issues);
    let metadata = schema::collect(&client, true).await?;
    let output =
        std::path::PathBuf::from(std::env::var("SKEDADDLE_TEST_ROOT")?).join(format!("s7-{name}"));
    std::fs::create_dir_all(&output)?;
    std::fs::write(
        output.join("information-schema.json"),
        serde_json::to_vec_pretty(&metadata)?,
    )?;
    let stream = |clause: &str, option: &str| {
        format!(
            "CREATE CHANGE STREAM s7_check FOR {clause} {} ({option})",
            if google { "OPTIONS" } else { "WITH" }
        )
    };
    ddl(
        &admin,
        db,
        &stream(
            "parents(payload,changed_at)",
            "value_capture_type='NEW_ROW_AND_OLD_VALUES'",
        ),
    )
    .await?;
    endpoint.change_stream = "s7_check".into();
    let complete = Client::connect(&endpoint, Some(host)).await?;
    assert_eq!(
        preflight::inspect(&complete, &selected, &Options::default())
            .await?
            .blockers()
            .count(),
        0
    );
    ddl(&admin, db, "DROP CHANGE STREAM s7_check").await?;
    for (clause, option, rule, blocking, optional) in [
        (
            "children",
            "value_capture_type='NEW_ROW_AND_OLD_VALUES'",
            Rule::Coverage,
            true,
            false,
        ),
        (
            "parents(payload)",
            "value_capture_type='NEW_ROW_AND_OLD_VALUES'",
            Rule::Coverage,
            true,
            false,
        ),
        (
            "ALL",
            "value_capture_type='OLD_AND_NEW_VALUES'",
            Rule::CaptureType,
            false,
            false,
        ),
        (
            "ALL",
            "value_capture_type='NEW_ROW_AND_OLD_VALUES',exclude_ttl_deletes=true",
            Rule::TtlDeletes,
            true,
            true,
        ),
        (
            "ALL",
            "value_capture_type='NEW_ROW_AND_OLD_VALUES',allow_txn_exclusion=true",
            Rule::TransactionExclusion,
            true,
            true,
        ),
    ] {
        match ddl(&admin, db, &stream(clause, option)).await {
            Err(e) if optional => {
                // Unsupported emulator options use its recorded INFORMATION_SCHEMA
                // rows with only the unavailable option injected, alongside live checks.
                eprintln!("{name}: emulator rejects {rule:?}: {e}");
                std::fs::write(
                    output.join(format!("{rule:?}-rejection.txt")),
                    e.to_string(),
                )?;
                unavailable_option(metadata.clone(), &selected, "changes", rule, google)?;
                continue;
            }
            other => other?,
        }
        endpoint.change_stream = "s7_check".into();
        let check = Client::connect(&endpoint, Some(host)).await?;
        let observed = schema::collect(&check, true).await?;
        std::fs::write(
            output.join(format!("{rule:?}-metadata.json")),
            serde_json::to_vec_pretty(&observed)?,
        )?;
        let report = preflight::inspect(&check, &selected, &Options::default()).await?;
        let option_name = if rule == Rule::TtlDeletes {
            "exclude_ttl_deletes"
        } else {
            "allow_txn_exclusion"
        };
        let exposed = observed
            .get("change_stream_options")
            .into_iter()
            .flatten()
            .any(|r| {
                schema::text(r, "change_stream_name") == "s7_check"
                    && schema::text(r, "option_name") == option_name
            });
        if optional && !exposed {
            // The emulator also accepts some options while omitting their catalog
            // rows. Preserve the real rows as evidence before injecting that field.
            let limitation = format!(
                "{name}: emulator accepted {rule:?} DDL but exposes no {option_name} option row"
            );
            eprintln!("{limitation}");
            std::fs::write(output.join(format!("{rule:?}-unavailable.txt")), limitation)?;
            assert!(report.issues.is_empty(), "{name}: {:?}", report.issues);
            unavailable_option(observed, &selected, "s7_check", rule, google)?;
        } else {
            assert_eq!(
                report
                    .issues
                    .iter()
                    .map(|i| (i.rule, i.blocking))
                    .collect::<Vec<_>>(),
                [(rule, blocking)],
                "{name}: {:?}",
                report.issues
            );
        }
        ddl(&admin, db, "DROP CHANGE STREAM s7_check").await?;
    }
    let virtual_ddl = if google {
        "CREATE TABLE s7_virtual (id INT64 NOT NULL,v INT64 AS (id+1)) PRIMARY KEY(id)"
    } else {
        "CREATE TABLE s7_virtual (id bigint PRIMARY KEY,v bigint GENERATED ALWAYS AS (id+1) VIRTUAL)"
    };
    match ddl(&admin, db, virtual_ddl).await {
        Ok(()) => {
            let mut virtual_selected = selected.clone();
            virtual_selected[0].name = "s7_virtual".into();
            assert_eq!(
                preflight::inspect(&client, &virtual_selected, &Options::default())
                    .await?
                    .blockers()
                    .map(|i| i.rule)
                    .collect::<Vec<_>>(),
                [Rule::VirtualGenerated]
            );
        }
        Err(e) => {
            std::fs::write(output.join("virtual-rejection.txt"), e.to_string())?;
            let mut rows = metadata.clone();
            let cols = rows.get_mut("columns").unwrap();
            let r = cols
                .iter_mut()
                .find(|r| {
                    schema::text(r, "table_name") == "parents"
                        && schema::text(r, "column_name") == "payload"
                })
                .unwrap();
            r.insert("is_generated".into(), Some("ALWAYS".into()));
            r.insert("is_stored".into(), Some("NO".into()));
            let mut options = Options::default();
            options.sizes.insert(
                schema::key(&selected[0].schema, "parents"),
                preflight::Size {
                    rows: 1,
                    bytes: 1024,
                },
            );
            assert_eq!(
                preflight::assess(&rows, &selected, "changes", &options)?
                    .blockers()
                    .map(|i| i.rule)
                    .collect::<Vec<_>>(),
                [Rule::VirtualGenerated]
            );
        }
    }
    ddl(
        &admin,
        db,
        if google {
            "CREATE TABLE s7_other (id INT64 NOT NULL) PRIMARY KEY(id)"
        } else {
            "CREATE TABLE s7_other (id bigint PRIMARY KEY)"
        },
    )
    .await?;
    let baseline = preflight::inspect(&client, &selected, &Options::default())
        .await?
        .schema;
    let snapshot = timestamp(
        client
            .write(
                &["INSERT INTO parents(parent_id,payload) VALUES (7000,'s7')".into()],
                "s7",
            )
            .await?,
    );
    let order = Arc::new(SpannerOrder::new(
        &endpoint.project,
        &endpoint.instance,
        &endpoint.database,
        "changes",
        snapshot,
    )?);
    let reader = PartitionReader::new(
        client.clone(),
        snapshot,
        Frontier::new(snapshot),
        DuplicateFilter::restore(10000, 1024, [])?,
        Config::default(),
    )?;
    let (tx, _rx) = mpsc::channel();
    let scratch = output.join("scratch");
    std::fs::create_dir_all(&scratch)?;
    let buffer = output.join("buffer");
    std::fs::create_dir_all(&buffer)?;
    let writer = BufferWriter::open_from_checkpoint(
        Arc::new(ObjectBufferStore::local(buffer, Duration::from_secs(3))?),
        WriterConfig {
            prefix: "s7".into(),
            migration_id: "s7".into(),
            stream: "changes".into(),
            epoch: 1,
            segment_bytes: 65536,
            segment_transactions: 1,
            segment_age: Duration::from_secs(1),
            max_transaction_bytes: 1048576,
        },
        WriterCheckpoint::default(),
        order,
        tx,
        scratch,
    )
    .await?;
    endpoint.change_stream = "changes".into();
    let mut capture = SpannerCapture::new(&endpoint, snapshot, reader, writer, 10000)?
        .with_schema_guard(Guard::new(client, selected, baseline, 1)?);
    let int = if google { "INT64" } else { "bigint" };
    ddl(
        &admin,
        db,
        &format!("ALTER TABLE s7_other ADD COLUMN added {int}"),
    )
    .await?;
    tokio::time::timeout(Duration::from_secs(30), capture.step()).await??;
    ddl(
        &admin,
        db,
        &format!("ALTER TABLE parents ADD COLUMN s7_added {int}"),
    )
    .await?;
    let err = match tokio::time::timeout(Duration::from_secs(30), capture.step()).await? {
        Err(e) => e,
        Ok(_) => panic!("selected drift released a boundary"),
    };
    assert!(
        matches!(err, skedaddle::coordinator::Error::RecoveryRequired(_)),
        "{err:?}"
    );
    eprintln!(
        "{name}: coverage/options/generated/retention and selected/unselected capture drift checked"
    );
    Ok(())
}
#[tokio::test]
async fn google_sql_preflight() -> Result<(), Error> {
    run(SpannerDialect::GoogleSql, "GOOGLE_STANDARD_SQL").await
}
#[tokio::test]
async fn postgresql_preflight() -> Result<(), Error> {
    run(SpannerDialect::Postgresql, "POSTGRESQL").await
}
