//! Probe qualification on a real TLS binlog stream. The cursor below advances
//! only over complete, manifest-durable captured transactions. Engine marker
//! admission is independently owned/tested by the connector-neutral hub.
#[path = "../src/mysql/capture/test_support.rs"]
mod startup_support;
use mysql_async::prelude::Queryable;
use skedaddle::{
    buffer::{
        backends::ObjectBufferStore,
        writer::{BufferWriter, WriterConfig},
    },
    engine::{Capture, Captured, ReadinessProbe, SchemaBoundary, SchemaStopKind},
    event::{PositionOrder, SourceIdentity},
    mysql::{
        MysqlOrder, MysqlPosition,
        capture::MysqlCapture,
        client::MysqlConfig,
        control::{WATERMARK_DDL, emit_watermark},
        readiness::MysqlReadinessProbe,
    },
    pg::chunk::{ChunkId, Watermark, WatermarkKind},
};
use std::{path::PathBuf, sync::Arc, time::Duration};

async fn next(capture: &mut MysqlCapture) -> Captured {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let captured = capture.step().await.expect("capture step");
            assert!(
                !captured.stop,
                "unexpected M8 stop: {:?}",
                capture.stop_reason()
            );
            if captured.transaction.is_some() {
                return captured;
            }
        }
    })
    .await
    .expect("complete transaction deadline")
}
#[tokio::test]
async fn idle_burst_and_file_rotation_readiness() -> Result<(), Box<dyn std::error::Error>> {
    let Some(path) = std::env::var_os("SKEDADDLE_MYSQL_CONFIG") else {
        eprintln!(
            "MYSQL SKIPPED: SKEDADDLE_MYSQL_CONFIG absent; run harness mysql80 GTID OFF / mysql84 GTID ON --test mysql_readiness"
        );
        return Ok(());
    };
    let config: MysqlConfig = serde_json::from_slice(&std::fs::read(path)?)?;
    let root = PathBuf::from(std::env::var_os("SKEDADDLE_TEST_ROOT").expect("harness test root"))
        .join(format!("readiness-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(root.join("scratch"))?;
    std::fs::create_dir_all(root.join("buffer"))?;
    let mut admin = config.connect().await?;
    admin.conn.query_drop("CREATE TABLE type_zoo.readiness_probe (id BIGINT PRIMARY KEY, value BIGINT NOT NULL) ENGINE=InnoDB").await?;
    admin
        .conn
        .query_drop("DROP TABLE _skedaddle.watermark")
        .await?;
    admin.conn.query_drop(WATERMARK_DDL).await?;
    // Build-slot queues can outlast the harness's 600-second expiry policy.
    // Start this scenario from a fresh, genuinely retained binlog baseline.
    admin.conn.query_drop("FLUSH BINARY LOGS").await?;
    let status = admin.status().await?;
    let gtid = status.gtid_on;
    assert_eq!(gtid, config.gtid == "ON");
    let start = if gtid {
        MysqlPosition::Gtid {
            set: status.executed.clone(),
            server_uuid: status.server_uuid.clone(),
            file: status.file.clone(),
            offset: status.offset,
        }
    } else {
        MysqlPosition::File {
            server_uuid: status.server_uuid.clone(),
            file_seq: status.file.rsplit('.').next().unwrap().parse()?,
            offset: status.offset,
        }
    };
    let source = SourceIdentity {
        engine: "mysql".into(),
        system_id: "readiness-test".into(),
        lineage: "harness".into(),
        db_name: "type_zoo".into(),
        stream: "readiness".into(),
    };
    let order: Arc<dyn PositionOrder + Send + Sync> = Arc::new(if gtid {
        MysqlOrder::gtid(&source.system_id)?
    } else {
        MysqlOrder::file(&status.server_uuid)?
    });
    let writer = BufferWriter::open_with_scratch(
        Arc::new(ObjectBufferStore::local(
            root.join("buffer"),
            Duration::from_secs(3),
        )?),
        WriterConfig {
            prefix: "readiness".into(),
            migration_id: "live".into(),
            stream: "readiness".into(),
            epoch: 1,
            segment_bytes: 1_000_000,
            segment_transactions: 100,
            segment_age: Duration::from_secs(1),
            max_transaction_bytes: 4_000_000,
        },
        order.clone(),
        std::sync::mpsc::channel().0,
        root.join("scratch"),
    )
    .await?;
    let mut capture = MysqlCapture::new(
        &config,
        source.clone(),
        start,
        &[("type_zoo".into(), "readiness_probe".into())],
        writer,
        6511,
    )
    .await?;
    capture = startup_support::install(
        capture,
        &config,
        &[("type_zoo".into(), "readiness_probe".into())],
        &root.join("startup"),
    )
    .await
    .0;
    let marker = Watermark {
        chunk: ChunkId {
            table: "type_zoo.readiness_probe".into(),
            plan: "a".repeat(64),
            ordinal: 0,
        },
        wm: WatermarkKind::High,
        attempt: 1,
    };
    emit_watermark(&mut admin.conn, &marker).await?;
    let first = next(&mut capture).await;
    assert!(!first.watermarks.is_empty());
    let txn = first.transaction.as_ref().unwrap();
    assert!(txn.commit_time.is_some());
    let mut applied = txn.commit_position.clone();
    let native = MysqlPosition::decode(applied.bytes())?;
    let idle = MysqlReadinessProbe::observe(&config, source.clone(), &native, 6512)
        .await?
        .with_schema_state(capture.schema_state().unwrap());
    assert_eq!(
        idle.tip().compare(&applied, order.as_ref())?,
        std::cmp::Ordering::Equal
    );
    let sample = idle.sample(&applied, None)?;
    assert_eq!(sample.lag.unwrap().value, 0);
    assert_eq!(sample.oldest_unapplied_age_ms, Some(0));
    let retention = sample
        .retention
        .unwrap_or_else(|| panic!("idle retention unknown: {idle:?}"));
    assert!(retention.history_available);
    assert!(retention.remaining.value > 0);
    assert!(
        idle.observed_at().elapsed() < Duration::from_secs(1),
        "probe must be fresh"
    );
    for id in 1..=12 {
        admin
            .conn
            .exec_drop(
                "INSERT INTO type_zoo.readiness_probe VALUES (?,?)",
                (id, id * 2),
            )
            .await?;
        if !gtid && id == 5 {
            admin.conn.query_drop("FLUSH BINARY LOGS").await?;
        }
    }
    let burst = MysqlReadinessProbe::observe(&config, source.clone(), &native, 6512)
        .await?
        .with_schema_state(capture.schema_state().unwrap());
    assert!(
        burst.observed_at().elapsed() < Duration::from_secs(1),
        "burst probe must be fresh"
    );
    let sample = burst.sample(&applied, None)?;
    let lag = sample.lag.unwrap();
    assert!(lag.value > 0);
    if gtid {
        assert_eq!(lag.unit, "transactions");
        assert_eq!(lag.value, 12);
    } else {
        assert_eq!(lag.unit, "bytes");
        let end = admin.status().await?;
        let MysqlPosition::File {
            file_seq, offset, ..
        } = &native
        else {
            unreachable!()
        };
        let from = end
            .binary_logs
            .iter()
            .position(|(f, _)| f.rsplit('.').next().unwrap().parse::<u64>().unwrap() == *file_seq)
            .unwrap();
        assert_ne!(end.binary_logs[from].0, end.file, "rotation happened");
        let expected = end.binary_logs[from].1 - offset
            + end.binary_logs[from + 1..]
                .iter()
                .take_while(|(f, _)| *f != end.file)
                .map(|(_, size)| *size)
                .sum::<u64>()
            + end.offset;
        assert_eq!(lag.value, expected, "exact physical bytes over rotation");
    }
    assert!(sample.retention.unwrap().history_available);
    let mut oldest = None;
    for i in 0..12 {
        let batch = next(&mut capture).await;
        let txn = batch.transaction.unwrap();
        assert_eq!(txn.events.len(), 1);
        assert!(txn.commit_time.is_some());
        let cp = capture.writer_checkpoint()?.expect("manifest checkpoint");
        assert_eq!(cp.last_position.as_ref(), Some(&txn.commit_position));
        if i == 0 {
            oldest = txn.commit_time;
            let age = burst
                .sample(&applied, Some(txn.commit_time))?
                .oldest_unapplied_age_ms
                .unwrap();
            let query_started = std::time::Instant::now();
            let now: i64 = admin
                .conn
                .query_first("SELECT CAST(UNIX_TIMESTAMP(NOW(6))*1000000 AS SIGNED)")
                .await?
                .unwrap();
            let actual_age = u64::try_from(now - txn.commit_time.unwrap())?.div_ceil(1000);
            // The probe extrapolates its source clock conservatively by at most
            // that sample's (<1 second) RTT; include this check query's RTT too.
            let tolerance = 1000 + u64::try_from(query_started.elapsed().as_millis())?;
            assert!(
                age.abs_diff(actual_age) <= tolerance,
                "probe age {age}, source age {actual_age}"
            );
            assert!(
                burst
                    .sample(&applied, Some(None))?
                    .oldest_unapplied_age_ms
                    .is_none()
            );
        }
        applied = txn.commit_position;
        let sample = burst.sample(&applied, if i == 11 { None } else { Some(oldest) })?;
        if gtid {
            assert_eq!(sample.lag.as_ref().unwrap().value, 11 - i);
        }
    }
    let sample = burst.sample(&applied, None)?;
    assert_eq!(sample.lag.unwrap().value, 0);
    assert_eq!(sample.oldest_unapplied_age_ms, Some(0));
    assert!(sample.retention.unwrap().history_available);
    let clear_state = sample.schema.expect("capture supplied schema state");
    assert!(clear_state.blocked_boundary.is_none());
    assert!(!clear_state.unknown_schema_job);
    admin
        .conn
        .query_drop("ALTER TABLE type_zoo.readiness_probe ADD COLUMN added BIGINT")
        .await?;
    let stopped = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let batch = capture.step().await.expect("DDL capture step");
            if batch.stop {
                break batch;
            }
        }
    })
    .await?;
    let blocked = capture.schema_state().expect("MySQL schema classifier");
    let Some(SchemaBoundary::Connector(boundary)) = &blocked.blocked_boundary else {
        panic!("expected typed MySQL DDL stop: {blocked:?}");
    };
    assert_eq!(boundary.connector, "mysql");
    assert_eq!(boundary.kind, SchemaStopKind::Ddl);
    assert_eq!(boundary.table.as_deref(), Some("type_zoo.readiness_probe"));
    assert!(boundary.diagnostic.contains("ADD COLUMN added"));
    let stopped_position = stopped.durable_position.unwrap();
    assert_eq!(boundary.position, stopped_position);
    let native = MysqlPosition::decode(stopped_position.bytes())?;
    let probe = MysqlReadinessProbe::observe(&config, source.clone(), &native, 6512)
        .await?
        .with_schema_state(blocked.clone());
    assert_eq!(
        probe.sample(&stopped_position, None)?.schema,
        Some(blocked.clone())
    );
    let clear = MysqlReadinessProbe::observe(&config, source, &native, 6512)
        .await?
        .with_schema_state(clear_state.clone());
    assert_eq!(
        clear.sample(&stopped_position, None)?.schema,
        Some(clear_state)
    );
    assert!(capture.step().await.is_err());
    assert_eq!(capture.schema_state(), Some(blocked.clone()));
    let probe = probe.with_schema_state(skedaddle::engine::SchemaState {
        blocked_boundary: None,
        unknown_schema_job: false,
    });
    assert_eq!(probe.sample(&stopped_position, None)?.schema, Some(blocked));
    drop(capture);
    // Harness owns teardown of this labelled source and task-owned buffer data.
    Ok(())
}
