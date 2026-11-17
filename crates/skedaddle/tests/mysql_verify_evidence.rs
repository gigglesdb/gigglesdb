//! Real MySQL capture/marker qualification. Applied wait uses an in-test source:
//! this lane has no live PostgreSQL destination applier.
#[path = "../src/mysql/capture/test_support.rs"]
mod startup_support;
use async_trait::async_trait;
use futures::FutureExt;
use mysql_async::prelude::Queryable;
use skedaddle::{
    buffer::{
        backends::ObjectBufferStore,
        writer::{BufferWriter, WriterConfig},
    },
    engine::Capture,
    event::{Position, PositionOrder, SourceIdentity},
    mapping::Value,
    mysql::{MysqlOrder, MysqlPosition, capture::MysqlCapture, client::MysqlConfig, control},
    pg::chunk::ChunkId,
    verify::{
        Applier, EvidenceInvalidation, Key, KeyOrder, RangePlan, VerifyError,
        mysql::evidence::{MysqlAppliedCheckpoint, MysqlAppliedWaiter, MysqlChangeEvidence},
        sql::SqlChangeEvidence,
    },
};
use std::{cmp::Ordering, sync::Arc, time::Duration};
use tokio::time::timeout;
const LIMIT: Duration = Duration::from_secs(20);
struct IntegerOrder;
impl KeyOrder for IntegerOrder {
    fn compare_keys(&self, a: &Key, b: &Key) -> skedaddle::verify::Result<Ordering> {
        match (a.as_slice(), b.as_slice()) {
            ([Value::Int8(a)], [Value::Int8(b)]) => Ok(a.cmp(b)),
            _ => Err(VerifyError::Invalid("expected bigint key")),
        }
    }
}
fn order(start: &MysqlPosition) -> MysqlOrder {
    match start {
        MysqlPosition::Gtid { .. } => MysqlOrder::gtid("evidence-cluster").unwrap(),
        MysqlPosition::File { server_uuid, .. } => MysqlOrder::file(server_uuid).unwrap(),
    }
}
// Cached rows below come from the actual SQL reads in this test. The source
// interval is armed before the snapshot opens; caching preserves that read view.
struct ReadRows(Vec<skedaddle::verify::Row>);
impl KeyOrder for ReadRows {
    fn compare_keys(&self, a: &Key, b: &Key) -> skedaddle::verify::Result<Ordering> {
        IntegerOrder.compare_keys(a, b)
    }
}
#[async_trait]
impl skedaddle::verify::RowSource for ReadRows {
    async fn fetch(
        &mut self,
        _: &RangePlan,
        _: &skedaddle::verify::Projection,
    ) -> skedaddle::verify::Result<Vec<skedaddle::verify::Row>> {
        Ok(self.0.clone())
    }
}
struct LiveInterval {
    rows: ReadRows,
    sample: Position,
    evidence: MysqlChangeEvidence<skedaddle::verify::mysql::input::MysqlCaptureInput, IntegerOrder>,
    complete: Option<skedaddle::verify::ChangeEvidence>,
}
impl KeyOrder for LiveInterval {
    fn compare_keys(&self, a: &Key, b: &Key) -> skedaddle::verify::Result<Ordering> {
        IntegerOrder.compare_keys(a, b)
    }
}
#[async_trait]
impl skedaddle::verify::RowSource for LiveInterval {
    async fn fetch(
        &mut self,
        r: &RangePlan,
        p: &skedaddle::verify::Projection,
    ) -> skedaddle::verify::Result<Vec<skedaddle::verify::Row>> {
        skedaddle::verify::RowSource::fetch(&mut self.rows, r, p).await
    }
}
#[async_trait]
impl skedaddle::verify::BoundarySource for LiveInterval {
    fn position_order(&self) -> skedaddle::verify::SourcePositionOrder {
        self.evidence.position_order()
    }
    async fn read_source(
        &mut self,
        _: &RangePlan,
        _: &skedaddle::verify::Projection,
    ) -> skedaddle::verify::Result<skedaddle::verify::SourceRead> {
        Ok(skedaddle::verify::SourceRead {
            rows: self.rows.0.clone(),
            position: self.sample.clone(),
        })
    }
    async fn changed_since(
        &mut self,
        r: &RangePlan,
        p: Position,
    ) -> skedaddle::verify::Result<skedaddle::verify::ChangeEvidence> {
        let complete = self.evidence.changes(r, p).await?;
        self.complete = Some(complete.clone());
        Ok(complete)
    }
}
fn real_rows(rows: &[(i64, i64)]) -> ReadRows {
    ReadRows(
        rows.iter()
            .map(|(id, v)| skedaddle::verify::Row {
                key: vec![Value::Int8(*id)],
                columns: [("v".into(), Value::Int8(*v))].into(),
            })
            .collect(),
    )
}
// A waiter seam only; this never claims destination authentication.
struct TestCheckpoint(Option<Position>, bool);
#[async_trait]
impl MysqlAppliedCheckpoint for TestCheckpoint {
    async fn applied_position(&mut self) -> skedaddle::verify::Result<Option<Position>> {
        if !self.1 {
            self.1 = true;
            return Ok(None);
        }
        Ok(self.0.clone())
    }
}
// Explicit live-test checkpoint seam only. Source status is NOT destination
// applied evidence; production Pass::run always uses JournalMysqlCheckpoint.
struct StatusCheckpoint(MysqlConfig);
#[async_trait]
impl MysqlAppliedCheckpoint for StatusCheckpoint {
    async fn applied_position(&mut self) -> skedaddle::verify::Result<Option<Position>> {
        let mut client = self
            .0
            .connect()
            .await
            .map_err(|_| VerifyError::Invalid("test status connection"))?;
        let status = client
            .status()
            .await
            .map_err(|_| VerifyError::Invalid("test status"))?;
        let position = if status.gtid_on {
            MysqlPosition::Gtid {
                set: status.executed,
                server_uuid: status.server_uuid,
                file: status.file,
                offset: status.offset,
            }
        } else {
            MysqlPosition::File {
                server_uuid: status.server_uuid,
                file_seq: status.file.rsplit('.').next().unwrap().parse().unwrap(),
                offset: status.offset,
            }
        };
        Ok(Some(Position::new(
            order(&position).domain(),
            position.encode().map_err(VerifyError::Position)?,
        )))
    }
}
async fn native_roundtrip(
    root: &std::path::Path,
    config: &MysqlConfig,
    start: &MysqlPosition,
    evidence: &mut MysqlChangeEvidence<
        skedaddle::verify::mysql::input::MysqlCaptureInput,
        IntegerOrder,
    >,
    rows: &[(i64, i64)],
) -> Result<(), Box<dyn std::error::Error>> {
    use skedaddle::{
        coordinator::{
            SourceBinding, SourceConnector, Transition,
            epoch::Coordinator,
            journal::Journal,
            local::Local,
            verification::{DeadLetters, Section, Sections},
        },
        mysql::{chunk_key::*, schema::load_descriptors},
        verify::mysql::{
            MysqlRowSource,
            publication::{Pass, Publication},
        },
    };
    let journal = Journal::new(
        Arc::new(skedaddle::coordinator::journal::local_store(root)?),
        "verify",
        "verify",
        Duration::from_secs(3),
    )?;
    let mut owner = Coordinator::observer(
        journal.clone(),
        Local::create(root.join("publication.local"))?,
        "owner",
    )?;
    owner.acquire().await?;
    let text = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("README.md"),
    )?;
    let mut definition = skedaddle::MigrationDefinition::from_json(
        text.split("```json")
            .nth(1)
            .unwrap()
            .split("```")
            .next()
            .unwrap(),
    )?;
    definition.selected_objects[0].schema = "verify_evidence".into();
    definition.selected_objects[0].name = "rows_live".into();
    definition.source.engine = skedaddle::definition::Engine::Mysql;
    definition.source.connection_ref = None;
    definition.source.version = "8.4".into();
    definition.source.mysql = Some(serde_json::from_value(
        serde_json::json!({"host":"127.0.0.1","port":3306,"user":"env:SKEDADDLE_TEST_MYSQL_USER","password":"env:SKEDADDLE_TEST_MYSQL_PASSWORD"}),
    )?);
    owner
        .commit(Transition::Define {
            definition: Box::new(definition),
        })
        .await?;
    owner
        .commit(Transition::SourceBindingV1 {
            binding: SourceBinding {
                version: 1,
                connector: SourceConnector::Mysql,
                order_domain: order(start).domain().into(),
                stream: "evidence".into(),
            },
        })
        .await?;
    let runtime = skedaddle::engine::Engine::new(owner, journal.clone()).runtime();
    let mut client = config.connect().await?;
    let table = load_descriptors(
        &mut client.conn,
        &[("verify_evidence".into(), "rows_live".into())],
    )
    .await?
    .remove(0);
    let column = &table.columns[0];
    let key = MysqlTableKey {
        event_table: "verify_evidence.rows_live".into(),
        schema: table.schema.clone(),
        table: table.table.clone(),
        columns: vec![MysqlKeyColumn {
            name: column.name.clone(),
            data_type: column.data_type.clone(),
            column_type: column.column_type.clone(),
            character_set: None,
            collation: None,
            type_id: skedaddle::mapping::mysql::plan_column(column)?.type_id,
            direction: MysqlKeyDirection::Asc,
        }],
    };
    let catalog_digest = skedaddle::coordinator::verification::digest(&table)?;
    let mut source = MysqlRowSource::new(
        config.clone(),
        "evidence-cluster".into(),
        table,
        key.clone(),
        IntegerOrder,
        skedaddle::verify::sql::RangeBudget {
            max_rows: 100,
            max_bytes: 1_000_000,
        },
        10,
    )?;
    // This lane has no destination applier. Keep section limitations explicit in
    // the report; this roundtrip qualifies publication, not destination readiness.
    let section = || Section {
        completed: true,
        checked: 0,
        findings: vec![skedaddle::coordinator::verification::Finding {
            object: "live publication qualification".into(),
            detail: "not qualified in this MySQL-only test".into(),
        }],
        evidence: "explicit test scope limitation".into(),
    };
    let publication = Publication {
        migration: "verify".into(),
        run: uuid::Uuid::new_v4().to_string(),
        catalog_digest,
        plan_digest: "a".repeat(64),
        sections: Sections {
            schema: section(),
            sequences: section(),
            constraints: section(),
            indexes: section(),
            destination_foreign_keys: section(),
        },
        dead_letters: DeadLetters {
            collection: "no-destination-test".into(),
            completed: true,
            count: 0,
            identities: vec![],
            evidence: "no destination applier or dead-letter writes in this test".into(),
        },
        timeout: LIMIT,
        poll_interval: Duration::from_millis(1),
    };
    let report = Pass {
        runtime: &runtime,
        journal: &journal,
        evidence,
        source: &mut source,
        destination: &mut real_rows(rows),
        key: &key,
        order: order(start),
        projection: skedaddle::verify::Projection::new(vec!["v".into()])?,
        publication,
    }
    .with_checkpoint(StatusCheckpoint(config.clone()))
    .await
    .map_err(|e| e as Box<dyn std::error::Error>)?;
    assert_eq!(report.version, 2);
    let state = runtime.state().await;
    let saved = journal
        .native_verification_report(&state, state.verification.native_report.as_ref().unwrap())
        .await?;
    assert_eq!(*saved, report);
    let marker = Position::try_from(report.interval.complete_through.clone())?;
    let current = skedaddle::runner::verification::native_current(
        &journal,
        &state,
        &marker,
        &[report.tables[0].scope.clone()],
    )
    .await?;
    assert!(
        current.verification.is_none(),
        "unqualified sections must block readiness"
    );
    assert_eq!(saved.tables[0].counts.verified, rows.len() as u64);
    client.conn.disconnect().await?;
    Ok(())
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_complete_evidence_marker_and_reconnect() -> Result<(), Box<dyn std::error::Error>> {
    let Some(path) = std::env::var_os("SKEDADDLE_MYSQL_CONFIG") else {
        eprintln!(
            "MYSQL SKIPPED: SKEDADDLE_MYSQL_CONFIG absent; run harness mysql84 GTID ON or mysql80 GTID OFF"
        );
        return Ok(());
    };
    let config: MysqlConfig = serde_json::from_slice(&std::fs::read(path)?)?;
    let mut client = config.connect().await?;
    let had_schema: u64 = client
        .conn
        .query_first(
            "SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME='_skedaddle'",
        )
        .await?
        .unwrap();
    let had_watermark: u64 = client.conn.query_first("SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA='_skedaddle' AND TABLE_NAME='watermark'").await?.unwrap();
    let backup = format!("verify_backup_{}", uuid::Uuid::new_v4().simple());
    if had_watermark != 0 {
        client
            .conn
            .query_drop(format!(
                "RENAME TABLE _skedaddle.watermark TO _skedaddle.`{backup}`"
            ))
            .await?;
    }
    let mut driver = None;
    let result = std::panic::AssertUnwindSafe(async {
    control::install_watermark_table(&mut client.conn).await?;
    client
        .conn
        .query_drop("CREATE DATABASE IF NOT EXISTS verify_evidence")
        .await?;
    client.conn.query_drop("CREATE TABLE verify_evidence.rows_live (id BIGINT PRIMARY KEY, v BIGINT NOT NULL) ENGINE=InnoDB").await?;
    client
        .conn
        .query_drop("INSERT INTO verify_evidence.rows_live VALUES (1,10),(4,40),(5,50)")
        .await?;
    let status = client.status().await?;
    let start = if status.gtid_on {
        MysqlPosition::Gtid {
            set: status.executed,
            server_uuid: status.server_uuid,
            file: status.file,
            offset: status.offset,
        }
    } else {
        MysqlPosition::File {
            server_uuid: status.server_uuid,
            file_seq: status.file.rsplit('.').next().unwrap().parse()?,
            offset: status.offset,
        }
    };
    let source = SourceIdentity {
        engine: "mysql".into(),
        system_id: "evidence-cluster".into(),
        lineage: "one".into(),
        db_name: "verify_evidence".into(),
        stream: "evidence".into(),
    };
    let root = std::path::PathBuf::from(
        std::env::var_os("SKEDADDLE_TEST_ROOT").expect("harness test root"),
    )
    .join(format!("evidence-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(root.join("buffer"))?;
    std::fs::create_dir_all(root.join("scratch"))?;
    let store = Arc::new(ObjectBufferStore::local(
        root.join("buffer"),
        Duration::from_secs(3),
    )?);
    let writer = BufferWriter::open_with_scratch(
        store,
        WriterConfig {
            prefix: "capture".into(),
            migration_id: "verify".into(),
            stream: "evidence".into(),
            epoch: 1,
            segment_bytes: 1_000_000,
            segment_transactions: 100,
            segment_age: Duration::from_secs(1),
            max_transaction_bytes: 2_000_000,
        },
        Arc::new(order(&start)),
        std::sync::mpsc::channel().0,
        root.join("scratch"),
    )
    .await?;
    let mut capture = MysqlCapture::new(
        &config,
        source,
        start.clone(),
        &[("verify_evidence".into(), "rows_live".into())],
        writer,
        6482,
    )
    .await?;
    capture = startup_support::install(capture, &config, &[("verify_evidence".into(), "rows_live".into())], &root.join("startup")).await.0;
    let input = capture.evidence_input(config.clone(), 1_000_000, 100)?;
    let mut evidence = MysqlChangeEvidence::new(input, order(&start), IntegerOrder, LIMIT, 100)?;
    let range = RangePlan {
        id: ChunkId {
            table: "verify_evidence.rows_live".into(),
            plan: "a".repeat(64),
            ordinal: 0,
        },
        lo: None,
        hi: None,
    };
    let sample = Position::new(order(&start).domain(), start.encode()?);
    evidence.arm(&range).await?;
    driver = Some(tokio::spawn(async move {
        loop {
            capture.step().await?;
        }
        #[allow(unreachable_code)]
        Ok::<(), skedaddle::coordinator::Error>(())
    }));
    // Writes commit on a second connection while the verification read view is
    // open. A key move and transient/reverted writes must all remain evidence.
    client
        .conn
        .query_drop("START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY")
        .await?;
    let before: Vec<(i64, i64)> = client
        .conn
        .query("SELECT id,v FROM verify_evidence.rows_live ORDER BY id")
        .await?;
    let mut writes = config.connect().await?;
    writes
        .conn
        .query_drop("UPDATE verify_evidence.rows_live SET id=2 WHERE id=1")
        .await?;
    writes
        .conn
        .query_drop("INSERT INTO verify_evidence.rows_live VALUES (3,30)")
        .await?;
    writes
        .conn
        .query_drop("DELETE FROM verify_evidence.rows_live WHERE id=3")
        .await?;
    writes
        .conn
        .query_drop("UPDATE verify_evidence.rows_live SET v=41 WHERE id=4")
        .await?;
    writes
        .conn
        .query_drop("UPDATE verify_evidence.rows_live SET v=40 WHERE id=4")
        .await?;
    let snapshot: Vec<(i64, i64)> = client
        .conn
        .query("SELECT id,v FROM verify_evidence.rows_live ORDER BY id")
        .await?;
    assert_eq!(before, snapshot);
    client.conn.query_drop("ROLLBACK").await?;
    let destination_read: Vec<(i64, i64)> = client
        .conn
        .query("SELECT id,v FROM verify_evidence.rows_live ORDER BY id")
        .await?;
    assert_ne!(before, destination_read);
    let mut interval = LiveInterval {
        rows: real_rows(&before),
        sample: sample.clone(),
        evidence,
        complete: None,
    };
    let mut destination = real_rows(&destination_read);
    let projection = skedaddle::verify::Projection::new(vec!["v".into()])?;
    let mut session =
        skedaddle::verify::VerificationSession::new(range.clone(), projection.clone(), 0)?;
    let mut initial_wait = MysqlAppliedWaiter::new(
        TestCheckpoint(Some(sample.clone()), false),
        order(&start),
        LIMIT,
        Duration::from_millis(1),
    )?;
    let verified = session
        .run(&mut interval, &mut destination, &mut initial_wait)
        .await?;
    assert_eq!(verified.comparison.coverage.busy, 4);
    assert_eq!(verified.comparison.coverage.verified, 1);
    assert!(verified.comparison.differences.is_empty());
    let complete = interval
        .complete
        .take()
        .expect("complete captured evidence required");
    assert_eq!(
        complete.keys,
        vec![
            vec![Value::Int8(1)],
            vec![Value::Int8(2)],
            vec![Value::Int8(3)],
            vec![Value::Int8(4)]
        ]
    );
    // No writer runs between the marker and this status sample: the exact
    // captured commit must match SHOW BINARY LOG STATUS / SHOW MASTER STATUS.
    let after = client.status().await?;
    match MysqlPosition::decode(complete.through_position.bytes())? {
        MysqlPosition::Gtid {
            set, file, offset, ..
        } => {
            assert_eq!(set, after.executed);
            assert_eq!((file, offset), (after.file, after.offset));
        }
        MysqlPosition::File {
            file_seq, offset, ..
        } => {
            assert_eq!(
                file_seq,
                after.file.rsplit('.').next().unwrap().parse::<u64>()?
            );
            assert_eq!(offset, after.offset);
        }
    }
    let mut waiter = MysqlAppliedWaiter::new(
        TestCheckpoint(Some(complete.through_position.clone()), false),
        order(&start),
        LIMIT,
        Duration::from_millis(1),
    )?;
    assert_eq!(
        waiter.wait_until(complete.through_position.clone()).await?,
        complete.through_position
    );
    let mut unavailable = MysqlAppliedWaiter::new(
        TestCheckpoint(None, false),
        order(&start),
        Duration::from_millis(10),
        Duration::from_millis(1),
    )?;
    assert!(matches!(
        unavailable.wait_until(complete.through_position).await,
        Err(VerifyError::ApplierTimeout { .. })
    ));
    native_roundtrip(&root, &config, &start, &mut interval.evidence, &destination_read).await?;
    // A second interval loses its connection before its closing marker.
    interval.evidence.arm(&range).await?;
    let dumps: Vec<u64> = writes
        .conn
        .query("SELECT ID FROM information_schema.PROCESSLIST WHERE COMMAND LIKE 'Binlog Dump%'")
        .await?;
    assert_eq!(dumps.len(), 1);
    writes
        .conn
        .query_drop(format!("KILL CONNECTION {}", dumps[0]))
        .await?;
    // Polling marker collection concurrently with the live driver must observe
    // the reconnect invalidation, never certify a partial interval.
    let mut failed_session =
        skedaddle::verify::VerificationSession::new(range.clone(), projection, 0)?;
    let failure = timeout(
        LIMIT,
        failed_session.run(&mut interval, &mut destination, &mut initial_wait),
    )
    .await?
    .unwrap_err();
    assert!(failed_session.result().is_none());
    assert!(matches!(
        failure,
        VerifyError::EvidenceInvalidated {
            reason: EvidenceInvalidation::Reconnected
        }
    ));

    writes.conn.disconnect().await?;
    eprintln!("APPLIED WAIT: in-test checkpoint source; no live PostgreSQL destination in this lane");
    Ok::<(),Box<dyn std::error::Error>>(())
    }).catch_unwind().await;
    if let Some(driver) = driver {
        driver.abort();
        let _ = driver.await;
    }
    // Restore the original table itself, preserving both its shape and data,
    // on success and every ordinary error path. This binary shares a server.
    client
        .conn
        .query_drop("DROP DATABASE IF EXISTS verify_evidence")
        .await?;
    client
        .conn
        .query_drop("DROP TABLE IF EXISTS _skedaddle.watermark")
        .await?;
    if had_watermark != 0 {
        client
            .conn
            .query_drop(format!(
                "RENAME TABLE _skedaddle.`{backup}` TO _skedaddle.watermark"
            ))
            .await?;
    }
    if had_schema == 0 {
        client.conn.query_drop("DROP DATABASE _skedaddle").await?;
    }
    client.conn.disconnect().await?;
    match result {
        Ok(result) => result,
        Err(panic) => std::panic::resume_unwind(panic),
    }
}
