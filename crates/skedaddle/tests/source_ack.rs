//! Process-death buffer coverage and a gated source-to-wire regression.
//! Live qualification needs a combined TLS PostgreSQL + MinIO harness config;
//! the existing p3 and pg-matrix lanes do not provide that combination.

use skedaddle::pg::pgoutput::StandbyStatusUpdate;

#[test]
fn public_status_encoder_rejects_acknowledgements_beyond_durability() {
    let durable = 40;
    let status = StandbyStatusUpdate {
        write_lsn: durable,
        flush_lsn: durable,
        apply_lsn: durable,
        client_time: 123,
        reply_requested: false,
    };
    let bytes = status.encode_copy_data(durable).unwrap();
    assert_eq!(bytes[0], b'd');
    assert_eq!(bytes[5], b'r');
    for range in [6..14, 14..22, 22..30] {
        assert_eq!(
            u64::from_be_bytes(bytes[range].try_into().unwrap()),
            durable
        );
    }
    for invalid in [
        StandbyStatusUpdate {
            write_lsn: 41,
            ..status
        },
        StandbyStatusUpdate {
            flush_lsn: 41,
            ..status
        },
        StandbyStatusUpdate {
            apply_lsn: 41,
            ..status
        },
    ] {
        assert!(invalid.encode_copy_data(durable).is_err());
    }
}

#[cfg(feature = "test-crash")]
mod crash {
    use futures::FutureExt;
    use skedaddle::{
        buffer::{
            backends::{BufferStore, Encryption, ObjectBufferStore, TransportSecurity},
            format::{ManifestEntry, SegmentDecoder, SegmentRecord, manifest_key, segment_key},
            writer::{BufferWriter, BufferedProgress, CrashPoint, Error, WriterConfig},
        },
        event::{
            AssembledTxn, Event, EventError, EventKind, OpaqueValue, Position, PositionOrder,
            Redelivery, Row, SourceIdentity, TxnId,
        },
        pg::{
            ddl::SchemaVersions,
            pgoutput::{Limits, Relation, RelationColumn},
            replication::{ConnectionConfig, parse_lsn},
            source::{self, Capture, ColumnMapping, Step},
        },
    };
    use std::{
        cmp::Ordering,
        io::{BufReader, Cursor},
        panic::AssertUnwindSafe,
        path::PathBuf,
        sync::{Arc, mpsc},
        time::{Duration, Instant},
    };

    struct Order;
    impl PositionOrder for Order {
        fn domain(&self) -> &str {
            "source-ack"
        }
        fn compare(&self, a: &[u8], b: &[u8]) -> Result<Ordering, EventError> {
            Ok(a.cmp(b))
        }
    }
    fn pos(n: u64) -> Position {
        Position::new("source-ack", n.to_be_bytes())
    }
    fn source_identity() -> SourceIdentity {
        SourceIdentity {
            engine: "pg".into(),
            system_id: "test".into(),
            lineage: "1".into(),
            db_name: "test".into(),
            slot: "test".into(),
        }
    }
    fn txn(n: u64) -> AssembledTxn {
        AssembledTxn::new(
            source_identity(),
            TxnId(n.to_be_bytes().to_vec()),
            pos(n),
            Some(1),
            vec![Event {
                source: source_identity(),
                txn: TxnId(n.to_be_bytes().to_vec()),
                ordinal_in_txn: 0,
                position: pos(n),
                schema_version: 1,
                kind: EventKind::Insert,
                table: Some("public.source_ack".into()),
                key: Row::new(),
                before: None,
                after: Some(Row::from([(
                    "payload".into(),
                    OpaqueValue {
                        type_id: "bytes".into(),
                        bytes: Some(format!("transaction-{n}-payload").into_bytes()),
                    },
                )])),
                unchanged_cols: Default::default(),
            }],
        )
        .unwrap()
    }
    fn config(prefix: &str) -> WriterConfig {
        WriterConfig {
            prefix: prefix.into(),
            migration_id: "migration".into(),
            stream: "wal".into(),
            epoch: 1,
            segment_bytes: 1_000_000,
            segment_transactions: 100,
            segment_age: Duration::from_secs(1),
            max_transaction_bytes: 1_000_000,
        }
    }
    fn root() -> PathBuf {
        let path = PathBuf::from(
            std::env::var_os("SKEDADDLE_TEST_ROOT")
                .expect("set task SKEDADDLE_TEST_ROOT under J:/gigglesdb-work"),
        )
        .join(format!("source-ack-{}", uuid::Uuid::new_v4()));
        assert!(path.starts_with("J:/gigglesdb-work"));
        std::fs::create_dir_all(&path).unwrap();
        path
    }
    fn local(path: &std::path::Path) -> Arc<dyn BufferStore> {
        std::fs::create_dir_all(path).unwrap();
        Arc::new(ObjectBufferStore::local(path, Duration::from_secs(30)).unwrap())
    }
    async fn open(
        store: Arc<dyn BufferStore>,
        c: WriterConfig,
        scratch: PathBuf,
    ) -> (BufferWriter, mpsc::Receiver<BufferedProgress>) {
        std::fs::create_dir_all(&scratch).unwrap();
        let (tx, rx) = mpsc::channel();
        (
            BufferWriter::open_with_scratch(store, c, Arc::new(Order), tx, scratch)
                .await
                .unwrap(),
            rx,
        )
    }
    async fn manifest(
        store: &dyn BufferStore,
        c: &WriterConfig,
        seq: u64,
    ) -> Option<ManifestEntry> {
        store
            .get(&manifest_key(&c.prefix, &c.migration_id, &c.stream, seq))
            .await
            .unwrap()
            .map(|bytes| ManifestEntry::from_canonical_json(&bytes).unwrap())
    }
    async fn contents(store: &dyn BufferStore, entry: &ManifestEntry) -> Vec<AssembledTxn> {
        let bytes = store.get(&entry.segment_key).await.unwrap().unwrap();
        let mut decoder = SegmentDecoder::new(Cursor::new(bytes), Default::default()).unwrap();
        let mut result = vec![];
        while let Some(record) = decoder.next_record().unwrap() {
            let SegmentRecord::Transaction(txn) = record else {
                panic!("unexpected chunk record")
            };
            result.push(txn);
        }
        let (_, summary) = decoder.finish().unwrap();
        assert_eq!(summary.digest, entry.segment_digest);
        result
    }
    fn point(name: &str) -> CrashPoint {
        match name {
            "before-segment" => CrashPoint::BeforeSegmentPut,
            "after-segment" => CrashPoint::AfterSegmentPut,
            "before-manifest" => CrashPoint::BeforeManifestPut,
            "after-manifest" => CrashPoint::AfterManifestPut,
            _ => panic!("unknown crash point"),
        }
    }

    #[tokio::test]
    async fn four_process_exits_recover_only_durable_manifest_progress() {
        const CHILD: &str = "SKEDADDLE_SOURCE_ACK_CRASH_ROOT";
        const POINT: &str = "SKEDADDLE_SOURCE_ACK_CRASH_POINT";
        if let Some(path) = std::env::var_os(CHILD) {
            let path = PathBuf::from(path);
            let seam = point(&std::env::var(POINT).unwrap());
            let (mut writer, progress) = open(
                local(&path.join("store")),
                config("local"),
                path.join("child-scratch"),
            )
            .await;
            assert_eq!(progress.try_recv().unwrap().position, pos(1));
            writer.append(txn(2), Instant::now()).await.unwrap();
            writer.inject_crash(seam);
            assert!(
                matches!(writer.seal().await, Err(Error::InjectedCrash(actual)) if actual == seam)
            );
            assert!(
                progress.try_recv().is_err(),
                "a crash cut must not publish progress"
            );
            // Exit with writer, runtime, open scratch handles and process state
            // still live: no Drop-based cleanup or same-process recovery.
            std::process::exit(86);
        }
        for name in [
            "before-segment",
            "after-segment",
            "before-manifest",
            "after-manifest",
        ] {
            let path = root();
            let store = local(&path.join("store"));
            let c = config("local");
            let (mut writer, progress) =
                open(store.clone(), c.clone(), path.join("seed-scratch")).await;
            writer.append(txn(1), Instant::now()).await.unwrap();
            writer.seal().await.unwrap();
            assert_eq!(progress.try_recv().unwrap().position, pos(1));
            drop(writer);
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "crash::four_process_exits_recover_only_durable_manifest_progress",
                    "--exact",
                    "--nocapture",
                    "--test-threads=1",
                ])
                .env(CHILD, &path)
                .env(POINT, name)
                .status()
                .unwrap();
            assert_eq!(status.code(), Some(86), "seam {name} must actually exit");
            let published = name == "after-manifest";
            assert_eq!(manifest(store.as_ref(), &c, 1).await.is_some(), published);
            let segment = segment_key(
                &c.prefix,
                &c.migration_id,
                &c.stream,
                &pos(2),
                &pos(2),
                c.epoch,
            );
            assert_eq!(
                store.get(&segment).await.unwrap().is_some(),
                name != "before-segment"
            );
            let (mut restarted, recovered) =
                open(store.clone(), c.clone(), path.join("restart-scratch")).await;
            assert_eq!(
                recovered.try_recv().unwrap().position,
                pos(if published { 2 } else { 1 })
            );
            assert!(recovered.try_recv().is_err());
            assert_eq!(
                restarted.append(txn(1), Instant::now()).await.unwrap(),
                Redelivery::Noop
            );
            assert_eq!(
                restarted.append(txn(2), Instant::now()).await.unwrap(),
                if published {
                    Redelivery::Noop
                } else {
                    Redelivery::New
                }
            );
            assert_eq!(restarted.seal().await.unwrap(), !published);
            if !published {
                assert_eq!(recovered.try_recv().unwrap().position, pos(2));
            }
            assert!(recovered.try_recv().is_err());
            restarted.append(txn(3), Instant::now()).await.unwrap();
            restarted.seal().await.unwrap();
            assert_eq!(recovered.try_recv().unwrap().position, pos(3));
            let mut actual = vec![];
            for seq in 0..3 {
                actual.extend(
                    contents(
                        store.as_ref(),
                        &manifest(store.as_ref(), &c, seq).await.unwrap(),
                    )
                    .await,
                );
            }
            assert_eq!(
                actual,
                vec![txn(1), txn(2), txn(3)],
                "no loss or duplicates after {name}"
            );
            assert!(manifest(store.as_ref(), &c, 3).await.is_none());
        }
    }

    #[tokio::test]
    async fn source_ack_wire_after_manifest_durable() -> Result<(), Box<dyn std::error::Error>> {
        let Some(path) = std::env::var_os("SKEDADDLE_TEST_CONFIG") else {
            eprintln!(
                "SKIP source_ack_wire_after_manifest_durable: needs combined TLS PostgreSQL replication + MinIO SKEDADDLE_TEST_CONFIG; no wire qualification performed"
            );
            return Ok(());
        };
        let json: serde_json::Value = serde_json::from_slice(&std::fs::read(path)?)?;
        let (Some(pg), Some(cfg), Some(minio)) = (
            json["pg"].as_str(),
            json.get("replication"),
            json["minio"].as_str(),
        ) else {
            eprintln!(
                "SKIP source_ack_wire_after_manifest_durable: config must combine pg, replication TLS/SCRAM and minio; existing separate harness lanes are insufficient"
            );
            return Ok(());
        };
        let field = |name: &str| {
            cfg[name]
                .as_str()
                .expect("replication fixture string")
                .to_owned()
        };
        let mut roots = rustls::RootCertStore::empty();
        for cert in
            rustls_pemfile::certs(&mut BufReader::new(std::fs::File::open(field("ca_pem"))?))
        {
            roots.add(cert?)?;
        }
        let connection = ConnectionConfig {
            host: field("host"),
            port: cfg["port"].as_u64().expect("replication port").try_into()?,
            server_name: field("server_name"),
            user: field("user"),
            password: field("password"),
            database: field("database"),
            roots,
            connect_timeout: Duration::from_secs(10),
            status_interval: Duration::from_millis(20),
            limits: Limits::default(),
        };
        let tls = skedaddle::pg::tls::Config::new(pg.parse()?, field("ca_pem"));
        let (admin, driver, _) =
            skedaddle::pg::tls::connect(&tls, &skedaddle::security::TlsPolicy::Required).await?;
        let driver = tokio::spawn(driver);
        let id = uuid::Uuid::new_v4().simple().to_string();
        let slot = format!("f4_{id}");
        let publication = format!("f4_pub_{id}");
        admin
            .batch_execute(&format!("CREATE PUBLICATION {publication}"))
            .await?;
        let outcome = AssertUnwindSafe(async {
            let mut stream = source::connect(&connection).await?;
            let created = stream.create_slot(&slot).await?;
            let lineage = stream.lineage().clone();
            stream.start(&slot, created.consistent_point, std::slice::from_ref(&publication)).await?;
            let store: Arc<dyn BufferStore> = Arc::new(ObjectBufferStore::s3(
                object_store::aws::AmazonS3Builder::new().with_bucket_name("p2-bucket").with_region("us-east-1").with_access_key_id("skedaddle").with_secret_access_key("skedaddle-secret").with_endpoint(minio),
                Encryption::UnencryptedLocalTest, TransportSecurity::DisableForLocalTest, Duration::from_secs(30),
            )?);
            let c = config(&format!("f4/{id}"));
            let (writer, progress) = open(store.clone(), c.clone(), root()).await;
            let resolver = |_: &Relation<'_>, _: &RelationColumn<'_>| -> source::Result<ColumnMapping> { Err(source::Error::RecoveryRequired("watermark fixture has no columns")) };
            let identity = SourceIdentity { engine: "postgres".into(), system_id: lineage.system_id.to_string(), lineage: lineage.timeline.to_string(), db_name: lineage.database.clone(), slot: slot.clone() };
            let mut capture = Capture::new(identity.clone(), "source-ack".into(), created.consistent_point, writer, resolver, "f4".into(), SchemaVersions::new("f4".into(), 0, [])?, skedaddle::pg::chunk::MESSAGE_PREFIX.into())?;
            admin.query_one("SELECT pg_logical_emit_message(true, $1::text, $2::text)", &[&skedaddle::pg::chunk::MESSAGE_PREFIX, &id]).await?;
            let (_paused_tx, mut paused) = tokio::sync::watch::channel(false);
            let observed_commit = std::cell::Cell::new(None);
            let mut sink = |mark: source::Watermark| { observed_commit.set(Some(mark.commit_lsn)); Ok(()) };
            let end_lsn = tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    if let Step::Durable { end_lsn, .. } = capture.step(&mut stream, &mut sink, &mut paused).await? { break Ok::<_, source::Error>(end_lsn); }
                }
            }).await??;
            let durable = progress.try_recv().expect("real manifest must publish BufferedProgress");
            let manifest_lsn = u64::from_be_bytes(durable.position.bytes().try_into()?);
            let entry = manifest(store.as_ref(), &c, 0).await.expect("real MinIO manifest");
            assert_eq!(entry.last_pos, durable.position);
            let original = contents(store.as_ref(), &entry).await;
            assert_eq!(original.len(), 1);
            assert_eq!(original[0].commit_position, durable.position);
            // Drive the actual Capture -> ReplicationConnection feedback path.
            // A bounded cancellation retains the idle capture and stream.
            let (write, flush, apply) = tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    if let Ok(step) = tokio::time::timeout(Duration::from_millis(50), capture.step(&mut stream, &mut sink, &mut paused)).await { step?; }
                    let row = admin.query_one("SELECT r.write_lsn::text, r.flush_lsn::text, r.replay_lsn::text FROM pg_stat_replication r JOIN pg_replication_slots s ON s.active_pid=r.pid WHERE s.slot_name=$1", &[&slot]).await?;
                    let values = (row.get::<_, Option<String>>(0), row.get::<_, Option<String>>(1), row.get::<_, Option<String>>(2));
                    if let (Some(write), Some(flush), Some(apply)) = values {
                        let positions = (parse_lsn(&write)?, parse_lsn(&flush)?, parse_lsn(&apply)?);
                        assert!(positions.1 <= end_lsn, "observed feedback must not exceed durable transaction end");
                        assert!(positions.0 <= positions.1 && positions.2 <= positions.1);
                        if positions.1 == end_lsn { break Ok::<_, Box<dyn std::error::Error>>(positions); }
                    }
                }
            }).await??;
            assert!(write <= flush && apply <= flush);
            assert_eq!(flush, end_lsn, "observe actual connector status on PostgreSQL");
            assert_eq!(observed_commit.get(), Some(manifest_lsn), "feedback end belongs to the manifest-durable COMMIT");
            assert!(manifest_lsn <= end_lsn);
            assert!(flush <= end_lsn, "never acknowledge beyond that durable transaction");
            let row = admin.query_one("SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=$1", &[&slot]).await?;
            let confirmed = parse_lsn(&row.get::<_, String>(0))?;
            assert_eq!(confirmed, end_lsn);
            drop(capture);
            drop(stream);
            let (mut writer, recovered) = open(store.clone(), c.clone(), root()).await;
            assert_eq!(recovered.try_recv()?.position, durable.position);
            assert_eq!(writer.append(original[0].clone(), Instant::now()).await?, Redelivery::Noop);
            assert!(!writer.seal().await?);
            assert!(recovered.try_recv().is_err(), "identical durable redelivery publishes no new progress");
            assert!(manifest(store.as_ref(), &c, 1).await.is_none());
            let later = format!("{id}-later");
            admin.query_one("SELECT pg_logical_emit_message(true, $1::text, $2::text)", &[&skedaddle::pg::chunk::MESSAGE_PREFIX, &later]).await?;
            let mut restarted = source::connect(&connection).await?;
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let row = admin.query_one("SELECT active FROM pg_replication_slots WHERE slot_name=$1", &[&slot]).await?;
                    if !row.get::<_, bool>(0) { break Ok::<_, tokio_postgres::Error>(()); }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }).await??;
            restarted.start(&slot, confirmed, std::slice::from_ref(&publication)).await?;
            let mut resumed = Capture::new(identity, "source-ack".into(), confirmed, writer, resolver, "f4".into(), SchemaVersions::new("f4".into(), 0, [])?, skedaddle::pg::chunk::MESSAGE_PREFIX.into())?;
            let observed = std::cell::RefCell::new(vec![]);
            let mut sink = |mark: source::Watermark| { observed.borrow_mut().push(mark); Ok(()) };
            tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    if let Step::Durable { .. } = resumed.step(&mut restarted, &mut sink, &mut paused).await? { break Ok::<_, source::Error>(()); }
                }
            }).await??;
            let observed = observed.into_inner();
            assert_eq!(observed.len(), 1);
            assert_eq!(observed[0].content, later.as_bytes(), "restart must not replay original as new data");
            let next = recovered.try_recv()?;
            let second = manifest(store.as_ref(), &c, 1).await.expect("later transaction manifest");
            assert_eq!(second.last_pos, next.position);
            let later_transactions = contents(store.as_ref(), &second).await;
            assert_eq!(later_transactions.len(), 1);
            assert_eq!(later_transactions[0].commit_position, next.position);
            assert_eq!(u64::from_be_bytes(next.position.bytes().try_into()?), observed[0].commit_lsn);
            assert_eq!(contents(store.as_ref(), &entry).await, original, "original durable data retained");
            assert!(manifest(store.as_ref(), &c, 2).await.is_none(), "exactly original and later transaction persisted");
            assert!(recovered.try_recv().is_err());
            Ok::<(), Box<dyn std::error::Error>>(())
        }).catch_unwind().await;
        let cleanup = async {
            for _ in 0..100 {
                let rows = admin
                    .query(
                        "SELECT active FROM pg_replication_slots WHERE slot_name=$1",
                        &[&slot],
                    )
                    .await?;
                if rows.is_empty() || !rows[0].get::<_, bool>(0) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            admin.batch_execute(&format!("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name='{slot}' AND NOT active; DROP PUBLICATION IF EXISTS {publication};")).await
        }.await;
        drop(admin);
        driver.abort();
        match outcome {
            Ok(result) => result?,
            Err(panic) => std::panic::resume_unwind(panic),
        }
        cleanup?;
        Ok(())
    }
}

#[cfg(not(feature = "test-crash"))]
#[test]
fn source_ack_wire_after_manifest_durable() {
    eprintln!(
        "SKIP source_ack_wire_after_manifest_durable: enable test-crash and combined TLS PostgreSQL + MinIO harness config; no wire qualification performed"
    );
}
