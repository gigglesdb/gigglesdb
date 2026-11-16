use mysql_async::prelude::Queryable;
use skedaddle::{
    buffer::{
        backends::ObjectBufferStore,
        writer::{BufferWriter, WriterConfig},
    },
    engine::{Capture, Captured},
    event::{PositionOrder, SourceIdentity},
    mapping::Value,
    mysql::{
        MysqlOrder, MysqlPosition,
        capture::{MysqlCapture, ReconnectPolicy},
        client::{MysqlClient, MysqlConfig, MysqlError, MysqlStatus},
        resume::ResumeBlocker,
    },
};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

fn source() -> SourceIdentity {
    SourceIdentity {
        engine: "mysql".into(),
        system_id: "resume-cluster".into(),
        lineage: "harness".into(),
        db_name: "type_zoo".into(),
        stream: "resume".into(),
    }
}
fn position(s: &MysqlStatus) -> MysqlPosition {
    if s.gtid_on {
        MysqlPosition::Gtid {
            set: s.executed.clone(),
            server_uuid: s.server_uuid.clone(),
            file: s.file.clone(),
            offset: s.offset,
        }
    } else {
        MysqlPosition::File {
            server_uuid: s.server_uuid.clone(),
            file_seq: s.file.rsplit('.').next().unwrap().parse().unwrap(),
            offset: s.offset,
        }
    }
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
            prefix: "resume".into(),
            migration_id: "live".into(),
            stream: "resume".into(),
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
async fn next(capture: &mut MysqlCapture) -> Captured {
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let batch = capture.step().await.expect("capture step");
            assert!(!batch.stop);
            if batch.transaction.is_some() {
                return batch;
            }
        }
    })
    .await
    .expect("reconnecting capture transaction timeout")
}
async fn dump_thread(admin: &mut MysqlClient, previous: Option<u64>) -> u64 {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let ids: Vec<u64> = admin
                .conn
                .query("SELECT ID FROM information_schema.PROCESSLIST WHERE COMMAND LIKE 'Binlog Dump%'")
                .await
                .expect("find capture dump connection");
            assert!(ids.len() <= 1, "unexpected concurrent dump connections: {ids:?}");
            if let Some(id) = ids.first().copied().filter(|id| Some(*id) != previous) {
                eprintln!("capture dump connection {id}, previous {previous:?}");
                return id;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("replacement capture dump connection timeout")
}

#[tokio::test]
async fn live_killed_connections_exactly_once_purged_history_and_source_divergence()
-> Result<(), Box<dyn std::error::Error>> {
    let Some(path) = std::env::var_os("SKEDADDLE_MYSQL_CONFIG") else {
        eprintln!(
            "MYSQL SKIPPED: SKEDADDLE_MYSQL_CONFIG absent; run harness mysql84 GTID ON or mysql80 GTID OFF with --test mysql_resume"
        );
        return Ok(());
    };
    let cfg: MysqlConfig = serde_json::from_slice(&std::fs::read(path)?)?;
    let root = PathBuf::from(std::env::var_os("SKEDADDLE_TEST_ROOT").expect("harness test root"))
        .join(format!("resume-{}", uuid::Uuid::new_v4()));
    let mut admin = cfg.connect().await?;
    admin
        .conn
        .query_drop("CREATE TABLE type_zoo.resume_rows(id BIGINT PRIMARY KEY) ENGINE=InnoDB")
        .await?;
    let tables = vec![("type_zoo".into(), "resume_rows".into())];
    let status = admin.status().await?;
    let start = position(&status);

    // A foreign UUID cannot be silently rebound to this definition's source.
    let foreign = "3e11fa47-71ca-11e1-9e33-c80aa9429562";
    assert_ne!(status.server_uuid, foreign);
    let mut wrong = start.clone();
    match &mut wrong {
        MysqlPosition::File { server_uuid, .. } => *server_uuid = foreign.into(),
        MysqlPosition::Gtid { set, .. } => set.add(&format!("{foreign}:41"))?,
    }
    let error = MysqlCapture::new(
        &cfg,
        source(),
        wrong,
        &tables,
        writer(&root.join("wrong"), &start).await,
        6491,
    )
    .await
    .err()
    .expect("wrong source blocked");
    assert!(
        matches!(
            error,
            MysqlError::Resume(
                ResumeBlocker::SourceMismatch { .. } | ResumeBlocker::Divergent { .. }
            )
        ),
        "{error}"
    );

    let mut capture = MysqlCapture::new(
        &cfg,
        source(),
        start.clone(),
        &tables,
        writer(&root.join("capture"), &start).await,
        6492,
    )
    .await?
    .with_reconnect_policy(ReconnectPolicy {
        budget: Duration::from_secs(45),
        ..Default::default()
    })?;
    admin
        .conn
        .query_drop("INSERT INTO type_zoo.resume_rows VALUES(1)")
        .await?;
    let first = next(&mut capture).await;
    assert_eq!(
        first.transaction.as_ref().unwrap().events[0].key["id"].bytes,
        Some(Value::Int8(1).canonical_bytes()?)
    );
    // Kill the dump mid-stream with a committed transaction still unread.
    // A separate admin connection observes a replacement before writing more;
    // success therefore cannot come solely from the old stream's socket buffer.
    admin
        .conn
        .query_drop("INSERT INTO type_zoo.resume_rows VALUES(2)")
        .await?;
    let mut killer = cfg.connect().await?;
    let original_dump = dump_thread(&mut killer, None).await;
    killer
        .conn
        .query_drop(format!("KILL {original_dump}"))
        .await?;
    let reconnect_and_write = async {
        let replacement = dump_thread(&mut killer, Some(original_dump)).await;
        assert_eq!(
            killer.status().await.unwrap().server_uuid,
            status.server_uuid
        );
        killer
            .conn
            .query_drop("INSERT INTO type_zoo.resume_rows VALUES(3),(4)")
            .await
            .unwrap();
        replacement
    };
    let receive = async {
        let second = next(&mut capture).await;
        assert_eq!(second.transaction.as_ref().unwrap().events.len(), 1);
        assert_eq!(
            second.transaction.as_ref().unwrap().events[0].key["id"].bytes,
            Some(Value::Int8(2).canonical_bytes().unwrap())
        );
        let third = next(&mut capture).await;
        let txn = third.transaction.as_ref().unwrap();
        assert_eq!(txn.events.len(), 2);
        for (event, id) in txn.events.iter().zip([3, 4]) {
            assert_eq!(
                event.key["id"].bytes,
                Some(Value::Int8(id).canonical_bytes().unwrap())
            );
        }
        third
    };
    let (replacement_dump, _) = tokio::join!(reconnect_and_write, receive);
    // Kill again between transactions, with capture caught up. Polling capture
    // must reopen from its last delivered durable boundary before new writes.
    killer
        .conn
        .query_drop(format!("KILL {replacement_dump}"))
        .await?;
    let reconnect_and_write = async {
        let second_replacement = dump_thread(&mut killer, Some(replacement_dump)).await;
        assert_ne!(second_replacement, original_dump);
        assert_eq!(
            killer.status().await.unwrap().server_uuid,
            status.server_uuid
        );
        killer
            .conn
            .query_drop("INSERT INTO type_zoo.resume_rows VALUES(5),(6)")
            .await
            .unwrap();
    };
    let receive = async {
        let batch = next(&mut capture).await;
        let txn = batch.transaction.as_ref().unwrap();
        assert_eq!(txn.events.len(), 2);
        for (event, id) in txn.events.iter().zip([5, 6]) {
            assert_eq!(
                event.key["id"].bytes,
                Some(Value::Int8(id).canonical_bytes().unwrap())
            );
        }
        batch
    };
    let (_, batch) = tokio::join!(reconnect_and_write, receive);
    killer.conn.disconnect().await?;
    // Several subsequent heartbeats must not redeliver a committed transaction.
    for _ in 0..2 {
        let idle = tokio::time::timeout(Duration::from_secs(5), capture.step()).await??;
        assert!(idle.transaction.is_none());
    }
    let durable = MysqlPosition::decode(batch.durable_position.as_ref().unwrap().bytes())?;
    // Stall capture, write unseen transactions, rotate and purge beyond durable.
    // Reconstructing from this last delivered boundary must fail before the dump.
    // Disconnect first: MySQL can protect a log still held by an active dump.
    drop(capture);
    admin
        .conn
        .query_drop("INSERT INTO type_zoo.resume_rows VALUES(7)")
        .await?;
    admin.conn.query_drop("FLUSH BINARY LOGS").await?;
    let current = admin.status().await?;
    admin
        .conn
        .query_drop(format!("PURGE BINARY LOGS TO '{}'", current.file))
        .await?;
    let error = MysqlCapture::new(
        &cfg,
        source(),
        durable.clone(),
        &tables,
        writer(&root.join("capture"), &durable).await,
        6493,
    )
    .await
    .err()
    .expect("purged history blocked");
    if status.gtid_on {
        assert!(
            matches!(error, MysqlError::Resume(ResumeBlocker::HistoryLost { .. })),
            "{error}"
        );
    } else {
        assert!(
            matches!(error, MysqlError::Resume(ResumeBlocker::FilePurged { .. })),
            "{error}"
        );
    }
    assert!(error.to_string().contains("history lost"));
    admin.conn.disconnect().await?;
    std::fs::remove_dir_all(root)?;
    Ok(())
}
