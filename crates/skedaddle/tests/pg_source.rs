//! Harness-gated live byte-stream-to-local-buffer qualification. No Docker startup.
//! Uses the existing `pg` and TLS/SCRAM `replication` fixture in
//! SKEDADDLE_TEST_CONFIG. Also verifies the actual replication session settings
//! on initial connection and reconnect before capturing the integer/text workload.
use std::{
    cmp::Ordering,
    error::Error,
    io::BufReader,
    panic::AssertUnwindSafe,
    sync::{Arc, mpsc},
    time::Duration,
};

use futures::FutureExt;
use rustls::RootCertStore;
use skedaddle::{
    buffer::{
        backends::ObjectBufferStore,
        format,
        reader::{BufferReader, ReaderCheckpoint, ReaderConfig},
        writer::{BufferWriter, WriterConfig},
    },
    event::{EventError, EventKind, PositionOrder, SourceIdentity},
    mapping::{PgType, Value},
    pg::{
        ddl::SchemaVersions,
        pgoutput::Limits,
        replication::{Checkpoint, ConnectionConfig},
        source::{self, Capture, ColumnMapping, SessionSettings, Step},
    },
};

struct Order(String);
struct PublicColumns<R> {
    inner: R,
    table: String,
}
impl<R: source::TypeResolver> source::TypeResolver for PublicColumns<R> {
    fn included_columns(
        &mut self,
        relation: &skedaddle::pg::pgoutput::Relation<'_>,
    ) -> source::Result<Option<std::collections::BTreeSet<String>>> {
        if relation.namespace != "public" || relation.name != self.table {
            return Err(source::Error::RecoveryRequired(
                "unexpected fixture relation",
            ));
        }
        Ok(Some(["id".into(), "value".into()].into()))
    }
    fn resolve(
        &mut self,
        relation: &skedaddle::pg::pgoutput::Relation<'_>,
        column: &skedaddle::pg::pgoutput::RelationColumn<'_>,
    ) -> source::Result<ColumnMapping> {
        assert_ne!(
            column.name, "secret",
            "excluded column must never be resolved"
        );
        self.inner.resolve(relation, column)
    }
}
const SECRET_SENTINEL: &str = "GD49_K3_SECRET_MUST_NEVER_ENTER_BUFFER_7f64c828";
fn assert_no_secret_files(path: &std::path::Path) -> std::io::Result<()> {
    for entry in std::fs::read_dir(path)? {
        let path = entry?.path();
        if path.is_dir() {
            assert_no_secret_files(&path)?;
        } else {
            assert!(
                !std::fs::read(path)?
                    .windows(SECRET_SENTINEL.len())
                    .any(|w| w == SECRET_SENTINEL.as_bytes())
            );
        }
    }
    Ok(())
}
fn assert_settings(settings: &SessionSettings) {
    assert_eq!(settings.date_style, "ISO, MDY");
    assert_eq!(settings.interval_style, "postgres");
    assert_eq!(settings.time_zone, "UTC");
    assert_eq!(settings.extra_float_digits, 3);
    assert_eq!(settings.client_encoding, "UTF8");
    assert!(settings.standard_conforming_strings);
}
impl PositionOrder for Order {
    fn domain(&self) -> &str {
        &self.0
    }
    fn compare(&self, left: &[u8], right: &[u8]) -> Result<Ordering, EventError> {
        Ok(left.cmp(right))
    }
}

#[tokio::test]
async fn seeded_committed_history_matches_local_buffer() -> Result<(), Box<dyn Error>> {
    let Some(path) = std::env::var_os("SKEDADDLE_TEST_CONFIG") else {
        use std::io::Write;
        let _ = writeln!(
            std::io::stderr().lock(),
            "SKIP pg_source: requires harness PG16/17/18 with TLS/SCRAM replication fixture in SKEDADDLE_TEST_CONFIG; no live PG qualification performed"
        );
        return Ok(());
    };
    let json: serde_json::Value = serde_json::from_slice(&std::fs::read(path)?)?;
    let Some(cfg) = json.get("replication") else {
        use std::io::Write;
        let _ = writeln!(
            std::io::stderr().lock(),
            "SKIP pg_source: harness has no TLS/SCRAM replication fixture; no live PG qualification performed"
        );
        return Ok(());
    };
    let field = |name: &str| {
        cfg[name]
            .as_str()
            .expect("replication fixture field")
            .to_owned()
    };
    let mut roots = RootCertStore::empty();
    for cert in rustls_pemfile::certs(&mut BufReader::new(std::fs::File::open(field("ca_pem"))?)) {
        roots.add(cert?)?;
    }
    let config = ConnectionConfig {
        host: field("host"),
        port: cfg["port"].as_u64().expect("port").try_into()?,
        server_name: field("server_name"),
        user: field("user"),
        password: field("password"),
        database: field("database"),
        roots,
        connect_timeout: Duration::from_secs(10),
        status_interval: Duration::from_millis(50),
        limits: Limits::default(),
    };
    let (admin, driver) = tls_connect(json["pg"].as_str().expect("admin pg")).await?;
    let driver = tokio::spawn(driver);
    let id = uuid::Uuid::new_v4().simple().to_string();
    let slot = format!("p5e_{id}");
    let table = format!("p5e_t_{id}");
    let publication = format!("p5e_p_{id}");
    // Full publication deliberately proves the PG<16 full-wire filtering path.
    // PG16+ publication column-list DDL belongs to the production publisher.
    admin.batch_execute(&format!("CREATE TABLE {table} (id integer PRIMARY KEY, value text, secret text DEFAULT '{SECRET_SENTINEL}'); CREATE PUBLICATION {publication} FOR TABLE {table}")).await?;
    let result = AssertUnwindSafe(async {
        let mut stream = source::connect(&config).await?;
        assert_settings(stream.session_settings());
        let created = stream.create_slot(&slot).await?;
        let lineage = stream.lineage().clone();
        drop(stream);
        let mut stream = source::reconnect(
            &config,
            &Checkpoint {
                lineage: lineage.clone(),
                slot: slot.clone(),
                durable_lsn: created.consistent_point,
            },
            std::slice::from_ref(&publication),
        )
        .await?;
        assert_settings(stream.session_settings());
        let root = std::path::PathBuf::from(
            std::env::var_os("SKEDADDLE_TEST_ROOT").expect("task SKEDADDLE_TEST_ROOT"),
        )
        .join(format!("pg-source-live-{id}"));
        std::fs::create_dir_all(root.join("store"))?;
        std::fs::create_dir_all(root.join("scratch"))?;
        let store = Arc::new(ObjectBufferStore::local(
            root.join("store"),
            Duration::from_secs(5),
        )?);
        let domain = format!("pg:{}:{}:{}", lineage.system_id, lineage.timeline, slot);
        let order = Arc::new(Order(domain.clone()));
        let (tx, _) = mpsc::channel();
        let writer = BufferWriter::open_with_scratch(
            store.clone(),
            WriterConfig {
                prefix: "live".into(),
                migration_id: id.clone(),
                stream: "wal".into(),
                epoch: 1,
                segment_bytes: 1_000_000,
                segment_transactions: 100,
                segment_age: Duration::from_secs(1),
                max_transaction_bytes: 1_000_000,
            },
            order.clone(),
            tx,
            root.join("scratch"),
        )
        .await?;
        let source = SourceIdentity {
            engine: "postgres".into(),
            system_id: lineage.system_id.to_string(),
            lineage: lineage.timeline.to_string(),
            db_name: lineage.database.clone(),
            stream: slot.clone(),
        };
        let resolver =
            |_: &skedaddle::pg::pgoutput::Relation<'_>,
             column: &skedaddle::pg::pgoutput::RelationColumn<'_>| {
                let ty = match column.type_oid {
                    23 => PgType::Int4,
                    25 => PgType::Text {
                        encoding: "UTF8".into(),
                    },
                    _ => {
                        return Err(skedaddle::pg::source::Error::RecoveryRequired(
                            "unexpected fixture type",
                        ));
                    }
                };
                Ok(ColumnMapping::identity(ty))
            };
        let mut capture = Capture::new(
            source,
            domain,
            created.consistent_point,
            writer,
            PublicColumns {
                inner: resolver,
                table: table.clone(),
            },
            "p5e".into(),
            SchemaVersions::new("p5e".into(), 0, [])?,
            skedaddle::pg::chunk::MESSAGE_PREFIX.into(),
        )?;
        let mut expected = Vec::new();
        let mut seed = 0x49e_u32;
        for id in 0..24_i32 {
            seed = seed.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            let value = format!("seed-{seed:08x}");
            admin
                .execute(
                    &format!("INSERT INTO {table} (id, value) VALUES ($1, $2)"),
                    &[&id, &value],
                )
                .await?;
            expected.push((EventKind::Insert, id, Some(value)));
        }
        admin
            .execute(
                &format!("UPDATE {table} SET value = 'changed' WHERE id = 3"),
                &[],
            )
            .await?;
        expected.push((EventKind::Update, 3, Some("changed".into())));
        admin
            .execute(&format!("DELETE FROM {table} WHERE id = 7"), &[])
            .await?;
        expected.push((EventKind::Delete, 7, None));
        let key = skedaddle::pg::chunk::TableKey {
            table: table.clone(),
            relation: skedaddle::pg::chunk::QualifiedName {
                schema: "public".into(),
                name: table.clone(),
            },
            columns: vec![skedaddle::pg::chunk::KeyColumn {
                name: "id".into(),
                pg_type: PgType::Int4,
                sql_type: skedaddle::pg::chunk::QualifiedName {
                    schema: "pg_catalog".into(),
                    name: "int4".into(),
                },
                collation: None,
                direction: skedaddle::pg::chunk::Direction::Asc,
            }],
        };
        let range = skedaddle::pg::chunk::plan_chunks(&key, 100, &[])?.remove(0);
        let query = key.read_query_columns(&range, &["id".into(), "value".into()])?;
        assert!(!query.sql.contains("secret"));
        for row in admin.query(&query.sql, &[]).await? {
            assert_eq!(row.len(), 2);
            assert!(row.columns().iter().all(|c| c.name() != "secret"));
            assert!(!row.get::<_, String>(1).contains(SECRET_SENTINEL));
        }
        let (_gate, mut paused) = tokio::sync::watch::channel(false);
        let mut durable = 0;
        tokio::time::timeout(Duration::from_secs(30), async {
            while durable < expected.len() {
                if matches!(
                    capture
                        .step(&mut stream, &mut |_| Ok(()), &mut paused)
                        .await?,
                    Step::Durable { .. }
                ) {
                    durable += 1;
                }
            }
            Ok::<_, skedaddle::pg::source::Error>(())
        })
        .await??;
        let mut reader = BufferReader::new(
            store,
            ReaderConfig {
                prefix: "live".into(),
                migration_id: id.clone(),
                stream: "wal".into(),
                limits: format::Limits::default(),
                required_next_mseq: expected.len() as u64,
            },
            ReaderCheckpoint::default(),
            order,
        )?;
        let mut actual = Vec::new();
        while let Some(txn) = reader.next_txn().await? {
            assert!(
                !txn.canonical_bytes()?
                    .windows(SECRET_SENTINEL.len())
                    .any(|w| w == SECRET_SENTINEL.as_bytes())
            );
            assert_eq!(txn.events.len(), 1);
            let event = &txn.events[0];
            assert!(!event.key.contains_key("secret"));
            assert!(!event.unchanged_cols.contains("secret"));
            assert!(
                event
                    .before
                    .as_ref()
                    .is_none_or(|r| !r.contains_key("secret"))
            );
            assert!(
                event
                    .after
                    .as_ref()
                    .is_none_or(|r| !r.contains_key("secret"))
            );
            let key =
                Value::from_canonical_bytes(event.key["id"].bytes.as_ref().expect("non-null id"))?;
            let Value::Int4(id) = key else {
                panic!("integer key")
            };
            let value = event.after.as_ref().map(|row| {
                let v = Value::from_canonical_bytes(
                    row["value"].bytes.as_ref().expect("non-null value"),
                )
                .unwrap();
                String::from_utf8(
                    v.format_pg_text(&PgType::Text {
                        encoding: "UTF8".into(),
                    })
                    .unwrap()
                    .unwrap(),
                )
                .unwrap()
            });
            actual.push((event.kind.clone(), id, value));
        }
        assert_eq!(actual, expected);
        assert_no_secret_files(&root)?;
        drop(stream);
        Ok::<_, Box<dyn Error>>(())
    })
    .catch_unwind()
    .await;
    // Drop the COPY connection before attempting slot removal, also on panic.
    let mut cleanup = Ok(());
    for _ in 0..100 {
        let active: bool = admin.query_one("SELECT COALESCE((SELECT active FROM pg_replication_slots WHERE slot_name=$1), false)", &[&slot]).await?.get(0);
        if !active {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    if let Err(error) = admin.batch_execute(&format!("DROP PUBLICATION IF EXISTS {publication}; DROP TABLE IF EXISTS {table}; SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name='{slot}'")).await { cleanup = Err(error); }
    driver.abort();
    match result {
        Ok(result) => {
            result?;
            cleanup?;
            Ok(())
        }
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

// The harness CA is explicit; missing trust is a failure once a DB gate is set.
fn tls_config(postgres: tokio_postgres::Config) -> skedaddle::pg::tls::Config {
    let path = std::env::var_os("SKEDADDLE_TEST_CONFIG").expect("TLS harness config required");
    let config: serde_json::Value =
        serde_json::from_slice(&std::fs::read(path).expect("read TLS harness config"))
            .expect("TLS harness JSON");
    let ca_pem = config["ca_pem"]
        .as_str()
        .or_else(|| config["replication"]["ca_pem"].as_str())
        .expect("harness ca_pem or replication.ca_pem required");
    skedaddle::pg::tls::Config::new(postgres, ca_pem)
}

async fn tls_connect(
    dsn: &str,
) -> std::result::Result<
    (tokio_postgres::Client, skedaddle::pg::tls::Driver),
    skedaddle::pg::tls::Error,
> {
    let config = tls_config(dsn.parse().expect("harness PostgreSQL configuration"));
    let (client, driver, transport) =
        skedaddle::pg::tls::connect(&config, &skedaddle::security::TlsPolicy::Required).await?;
    assert!(transport.encrypted());
    assert!(transport.warnings.is_empty());
    Ok((client, driver))
}
