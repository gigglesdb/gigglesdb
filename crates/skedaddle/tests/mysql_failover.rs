//! Real primary/replica failover. The harness owns both servers and their data.
#[path = "support/root.rs"]
mod test_root;

#[path = "../src/mysql/capture/test_support.rs"]
mod startup_support;
use mysql_async::prelude::Queryable;
use serde::Deserialize;
use skedaddle::{
    buffer::{
        backends::ObjectBufferStore,
        writer::{BufferWriter, WriterConfig},
    },
    engine::{Capture, Captured},
    event::{PositionOrder, SourceIdentity},
    mapping::Value,
    mysql::{
        GtidSet, MysqlOrder, MysqlPosition,
        capture::MysqlCapture,
        client::{MysqlClient, MysqlConfig, MysqlError, MysqlStatus},
        resume::ResumeBlocker,
    },
};
use std::{path::Path, path::PathBuf, sync::Arc, time::Duration};

#[derive(Deserialize)]
struct PairConfig {
    #[serde(flatten)]
    primary: MysqlConfig,
    replica: Option<MysqlConfig>,
}

fn config(gtid: bool) -> Option<(MysqlConfig, MysqlConfig)> {
    let Some(path) = std::env::var_os("SKEDADDLE_MYSQL_CONFIG") else {
        eprintln!(
            "MYSQL SKIPPED: SKEDADDLE_MYSQL_CONFIG absent; run mysql84 GTID ON or mysql80 GTID OFF with -MysqlReplica --test mysql_failover"
        );
        return None;
    };
    let pair: PairConfig = serde_json::from_slice(&std::fs::read(path).expect("read MySQL config"))
        .expect("parse MySQL config");
    let Some(replica) = pair.replica else {
        eprintln!("MYSQL SKIPPED: replica endpoint absent; enable harness -MysqlReplica");
        return None;
    };
    if (pair.primary.gtid == "ON") != gtid {
        eprintln!(
            "MYSQL SKIPPED: this failover case requires GTID {}",
            if gtid { "ON" } else { "OFF" }
        );
        return None;
    }
    Some((pair.primary, replica))
}

fn source() -> SourceIdentity {
    SourceIdentity {
        engine: "mysql".into(),
        system_id: "failover-cluster".into(),
        lineage: "harness".into(),
        db_name: "type_zoo".into(),
        stream: "failover".into(),
    }
}

fn position(status: &MysqlStatus) -> MysqlPosition {
    if status.gtid_on {
        MysqlPosition::Gtid {
            set: status.executed.clone(),
            server_uuid: status.server_uuid.clone(),
            file: status.file.clone(),
            offset: status.offset,
        }
    } else {
        MysqlPosition::File {
            server_uuid: status.server_uuid.clone(),
            file_seq: status.file.rsplit('.').next().unwrap().parse().unwrap(),
            offset: status.offset,
        }
    }
}

fn root(name: &str) -> PathBuf {
    crate::test_root::scratch_root().join(format!("failover-{name}-{}", uuid::Uuid::new_v4()))
}

async fn writer(root: &Path, start: &MysqlPosition) -> BufferWriter {
    for name in ["buffer", "scratch"] {
        std::fs::create_dir_all(root.join(name)).unwrap();
    }
    let order: Arc<dyn PositionOrder + Send + Sync> = Arc::new(match start {
        MysqlPosition::Gtid { .. } => MysqlOrder::gtid(&source().system_id).unwrap(),
        MysqlPosition::File { server_uuid, .. } => MysqlOrder::file(server_uuid).unwrap(),
    });
    BufferWriter::open_with_scratch(
        Arc::new(ObjectBufferStore::local(root.join("buffer"), Duration::from_secs(3)).unwrap()),
        WriterConfig {
            prefix: "failover".into(),
            migration_id: "live".into(),
            stream: "failover".into(),
            epoch: 1,
            segment_bytes: 1_000_000,
            segment_transactions: 100,
            segment_age: Duration::from_secs(1),
            max_transaction_bytes: 4_000_000,
        },
        order,
        std::sync::mpsc::channel().0,
        root.join("scratch"),
    )
    .await
    .unwrap()
}

async fn capture(
    config: &MysqlConfig,
    start: &MysqlPosition,
    root: &Path,
) -> Result<MysqlCapture, MysqlError> {
    let c = MysqlCapture::new(
        config,
        source(),
        start.clone(),
        &[("type_zoo".into(), "failover_rows".into())],
        writer(root, start).await,
        6495,
    )
    .await?;
    Ok(startup_support::install(
        c,
        config,
        &[("type_zoo".into(), "failover_rows".into())],
        &root.join("startup"),
    )
    .await
    .0)
}

async fn next(capture: &mut MysqlCapture, expected: i64) -> MysqlPosition {
    let batch: Captured = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let batch = capture.step().await.expect("capture step");
            assert!(!batch.stop);
            if batch.transaction.is_some() {
                return batch;
            }
        }
    })
    .await
    .expect("failover capture timeout");
    let transaction = batch.transaction.as_ref().unwrap();
    assert_eq!(transaction.events.len(), 1);
    assert_eq!(
        transaction.events[0].key["id"].bytes,
        Some(Value::Int8(expected).canonical_bytes().unwrap()),
        "each committed row must arrive once, in order"
    );
    MysqlPosition::decode(batch.durable_position.as_ref().unwrap().bytes()).unwrap()
}

async fn insert(admin: &mut MysqlClient, id: i64) {
    admin
        .conn
        .query_drop(format!("INSERT INTO type_zoo.failover_rows VALUES({id})"))
        .await
        .unwrap();
}

async fn caught_up(replica: &mut MysqlClient, primary: &MysqlStatus) {
    if primary.gtid_on {
        let result: Option<i64> = replica
            .conn
            .exec_first(
                "SELECT WAIT_FOR_EXECUTED_GTID_SET(?,30)",
                (primary.executed.to_string(),),
            )
            .await
            .unwrap();
        assert_eq!(result, Some(0), "replica GTID catch-up timed out");
    } else {
        let result: Option<i64> = replica
            .conn
            .exec_first(
                "SELECT SOURCE_POS_WAIT(?,?,30)",
                (&primary.file, primary.offset),
            )
            .await
            .unwrap();
        assert!(
            result.is_some_and(|n| n >= 0),
            "replica file/position catch-up failed: {result:?}"
        );
    }
    let rows: Option<u64> = replica
        .conn
        .query_first("SELECT COUNT(*) FROM type_zoo.failover_rows")
        .await
        .unwrap();
    assert!(rows.is_some());
}

async fn promote(replica: &mut MysqlClient) {
    for sql in [
        "STOP REPLICA",
        "RESET REPLICA ALL",
        "SET GLOBAL super_read_only=OFF, read_only=OFF",
    ] {
        replica.conn.query_drop(sql).await.unwrap();
    }
}

async fn divergent(
    config: &MysqlConfig,
    durable: &MysqlPosition,
    observed: &MysqlStatus,
    root: &Path,
) {
    let MysqlPosition::Gtid { set, .. } = durable else {
        panic!("GTID boundary required")
    };
    let expected_missing = set.difference(&observed.executed);
    assert_ne!(expected_missing, GtidSet::default());
    let error = capture(config, durable, root)
        .await
        .err()
        .expect("divergent replica must block");
    let MysqlError::Resume(ResumeBlocker::Divergent { server, missing }) = &error else {
        panic!("wrong blocker: {error}")
    };
    assert_eq!(server, &observed.server_uuid);
    assert_eq!(missing, &expected_missing);
    assert!(error.to_string().contains(&expected_missing.to_string()));
    eprintln!("verified divergence: {error}");
}

#[tokio::test]
async fn live_gtid_promotion_missing_history_and_errant_superset()
-> Result<(), Box<dyn std::error::Error>> {
    let Some((cfg, replica_cfg)) = config(true) else {
        return Ok(());
    };
    let root = root("gtid");
    let mut primary = cfg.connect().await?;
    let mut replica = replica_cfg.connect().await?;
    assert_eq!(
        replica
            .conn
            .query_first::<(u8, u8), _>("SELECT @@read_only,@@super_read_only")
            .await?,
        Some((1, 1))
    );
    primary
        .conn
        .query_drop("CREATE TABLE type_zoo.failover_rows(id BIGINT PRIMARY KEY) ENGINE=InnoDB")
        .await?;
    caught_up(&mut replica, &primary.status().await?).await;
    let start = position(&primary.status().await?);
    let original_uuid = primary.status().await?.server_uuid;
    assert_ne!(replica.status().await?.server_uuid, original_uuid);
    let mut original = capture(&cfg, &start, &root.join("original")).await?;
    insert(&mut primary, 1).await;
    next(&mut original, 1).await;
    caught_up(&mut replica, &primary.status().await?).await;

    // First stop replication, then capture a primary-only commit. The blocker
    // must name exactly that real missing GTID, not a fabricated foreign UUID.
    replica.conn.query_drop("STOP REPLICA").await?;
    insert(&mut primary, 2).await;
    let durable = next(&mut original, 2).await;
    drop(original);
    divergent(
        &replica_cfg,
        &durable,
        &replica.status().await?,
        &root.join("lagging"),
    )
    .await;
    replica.conn.query_drop("START REPLICA").await?;
    caught_up(&mut replica, &primary.status().await?).await;

    // A commit during capture downtime is copied before promotion. The promoted
    // server also acquires its own errant GTID before capture is repointed.
    insert(&mut primary, 3).await;
    caught_up(&mut replica, &primary.status().await?).await;
    promote(&mut replica).await;
    insert(&mut replica, 4).await;
    let observed = replica.status().await?;
    let MysqlPosition::Gtid { set, .. } = &durable else {
        unreachable!()
    };
    assert_eq!(set.difference(&observed.executed), GtidSet::default());
    let extra = observed.executed.difference(set);
    assert!(extra.to_string().contains(&observed.server_uuid));
    eprintln!("errant GTIDs are allowed while durable is contained; extra={extra}");

    let mut promoted = capture(&replica_cfg, &durable, &root.join("original")).await?;
    next(&mut promoted, 3).await;
    next(&mut promoted, 4).await;
    insert(&mut replica, 5).await;
    next(&mut promoted, 5).await;
    for _ in 0..2 {
        let idle = tokio::time::timeout(Duration::from_secs(5), promoted.step()).await??;
        assert!(idle.transaction.is_none(), "duplicate after failover");
    }
    drop(promoted);

    // An errant superset is no longer safe if the original primary subsequently
    // delivers a transaction the promoted server has never received.
    let mut original = capture(&cfg, &durable, &root.join("later-primary")).await?;
    next(&mut original, 3).await;
    insert(&mut primary, 6).await;
    let later = next(&mut original, 6).await;
    drop(original);
    divergent(
        &replica_cfg,
        &later,
        &replica.status().await?,
        &root.join("errant-missing"),
    )
    .await;
    primary.conn.disconnect().await?;
    replica.conn.disconnect().await?;
    std::fs::remove_dir_all(root)?;
    Ok(())
}

#[tokio::test]
async fn live_file_offset_rejects_replica_uuid() -> Result<(), Box<dyn std::error::Error>> {
    let Some((cfg, replica_cfg)) = config(false) else {
        return Ok(());
    };
    let root = root("file");
    let mut primary = cfg.connect().await?;
    let mut replica = replica_cfg.connect().await?;
    assert_eq!(
        replica
            .conn
            .query_first::<(u8, u8), _>("SELECT @@read_only,@@super_read_only")
            .await?,
        Some((1, 1))
    );
    primary
        .conn
        .query_drop("CREATE TABLE type_zoo.failover_rows(id BIGINT PRIMARY KEY) ENGINE=InnoDB")
        .await?;
    caught_up(&mut replica, &primary.status().await?).await;
    let start = position(&primary.status().await?);
    let mut original = capture(&cfg, &start, &root.join("original")).await?;
    insert(&mut primary, 1).await;
    let durable = next(&mut original, 1).await;
    drop(original);
    caught_up(&mut replica, &primary.status().await?).await;
    let primary_uuid = primary.status().await?.server_uuid;
    let replica_uuid = replica.status().await?.server_uuid;
    assert_ne!(primary_uuid, replica_uuid);
    promote(&mut replica).await;
    let error = capture(&replica_cfg, &durable, &root.join("wrong-uuid"))
        .await
        .err()
        .expect("file UUID must block");
    assert!(
        matches!(&error, MysqlError::Resume(ResumeBlocker::SourceMismatch {durable, observed}) if durable == &primary_uuid && observed == &replica_uuid),
        "{error}"
    );
    eprintln!("verified file failover blocker: {error}");
    primary.conn.disconnect().await?;
    replica.conn.disconnect().await?;
    std::fs::remove_dir_all(root)?;
    Ok(())
}
