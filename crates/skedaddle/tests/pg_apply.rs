//! Real-PG qualification, provisioned ONLY by the Docker harness lane.
//! Set SKEDADDLE_TEST_CONFIG with a `pg_apply` superuser DSN. The production
//! installer provisions progress/receipts. No service is started by this binary.
//! Harness invocation: cargo test --test pg_apply -- --ignored --nocapture.
use async_trait::async_trait;
use std::{collections::BTreeMap, error::Error};

use skedaddle::{
    event::{AssembledTxn, Event, EventKind, OpaqueValue, Position, SourceIdentity, TxnId},
    mapping::{EncodedBytes, PgType, Value},
    pg::apply::{
        ApplyError, Column, CommitDecision, PlanContext, Progress, RangeState, Table,
        TransactionPlan,
        executor::{self, Connector, Destination, Executor, Observer, Seam, install_progress},
        plan_transaction, resolve_commit,
    },
};
use tokio_postgres::{Client, types::Type};

#[test]
fn pgoutput_truncate_and_origin_survive_assembly_canonical_storage_and_planning() {
    use skedaddle::{
        event::{Row, SourceOrigin},
        pg::{
            assembly::{MappedEvent, TransactionAssembler},
            pgoutput::{Begin, Commit, Decoder, Message},
        },
    };
    let source = SourceIdentity {
        engine: "postgres".into(),
        system_id: "source".into(),
        lineage: "1".into(),
        db_name: "source".into(),
        slot: "slot".into(),
    };
    let mut assembler = TransactionAssembler::new(source, "pg/source", 10000, 10).unwrap();
    assembler
        .push(
            &Message::Begin(Begin {
                final_lsn: 100,
                commit_time: 0,
                xid: 9,
            }),
            |_| unreachable!(),
        )
        .unwrap();
    let decoder = Decoder::default();
    let origin = decoder.decode(b"O\0\0\0\0\0\0\0\x07upstream\0").unwrap();
    assembler
        .push(&origin, |_| unreachable!("Origin belongs to the envelope"))
        .unwrap();
    let truncate = decoder
        .decode(b"T\0\0\0\x02\x03\0\0\0\x2b\0\0\0\x2a")
        .unwrap();
    assembler
        .push(&truncate, |message| {
            let Message::Truncate {
                relation_ids,
                cascade,
                restart_identity,
            } = message
            else {
                unreachable!()
            };
            Ok(vec![MappedEvent {
                schema_version: 1,
                kind: EventKind::Truncate {
                    relations: relation_ids
                        .iter()
                        .map(|id| format!("source.{id}"))
                        .collect(),
                    cascade: *cascade,
                    restart_identity: *restart_identity,
                },
                table: None,
                key: Row::new(),
                before: None,
                after: None,
                unchanged_cols: Default::default(),
            }])
        })
        .unwrap();
    let txn = assembler
        .push(
            &Message::Commit(Commit {
                commit_lsn: 100,
                end_lsn: 120,
                commit_time: 0,
            }),
            |_| unreachable!(),
        )
        .unwrap()
        .unwrap()
        .transaction;
    let txn = AssembledTxn::from_canonical_bytes(&txn.canonical_bytes().unwrap()).unwrap();
    let tables = [42, 43].map(|id| Table {
        event_table: format!("source.{id}"),
        schema: "dest".into(),
        name: format!("table{id}"),
        schema_version: 1,
        columns: vec![Column {
            name: "id".into(),
            type_id: "int4".into(),
            pg_type: PgType::Int4,
            wire_type: Type::INT4,
        }],
        key: vec!["id".into()],
    });
    let plan = plan_transaction(
        &txn,
        &tables,
        &PlanContext {
            stream: "forward".into(),
            mseq: 1,
            origin: "destination_stream".into(),
            synthetic_commit_time: None,
        },
        |_, _| unreachable!("TRUNCATE has no row range"),
    )
    .unwrap();
    assert_eq!(txn.events.len(), 1);
    assert_eq!(plan.statements.len(), 4);
    assert_eq!(
        plan.statements[2].sql,
        "TRUNCATE TABLE \"dest\".\"table43\", \"dest\".\"table42\" RESTART IDENTITY CASCADE"
    );
    assert_eq!(
        plan.source_origin,
        Some(SourceOrigin {
            name: "upstream".into(),
            lsn: 7
        })
    );
    assert_eq!(plan.progress.lsn, 100);
    assert_eq!(plan.progress.digest, txn.digest);
}

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

fn require(condition: bool, reason: &'static str) -> TestResult<()> {
    if condition {
        Ok(())
    } else {
        Err(reason.into())
    }
}

struct Connection {
    client: Client,
    driver: tokio::task::JoinHandle<()>,
}
impl Connection {
    async fn open(dsn: &str) -> TestResult<Self> {
        let (client, driver) = tls_connect(dsn)
            .await
            .map_err(|_| "configured pg_apply harness connection failed")?;
        // A killed backend is an intended fault; SQL calls observe that error.
        let driver = tokio::spawn(async move {
            let _ = driver.await;
        });
        Ok(Self { client, driver })
    }
    async fn close(self) {
        drop(self.client);
        let _ = self.driver.await;
    }
}

struct Fixture {
    dsn: String,
    admin: Connection,
    id: String,
    origin: String,
}
impl Fixture {
    async fn open() -> TestResult<Option<Self>> {
        let Some(path) = std::env::var_os("SKEDADDLE_TEST_CONFIG") else {
            eprintln!(
                "SKIP pg_apply: Docker harness SKEDADDLE_TEST_CONFIG absent; real-PG crash/commit qualification was NOT run"
            );
            return Ok(None);
        };
        let config: serde_json::Value = serde_json::from_slice(&std::fs::read(path)?)?;
        let Some(dsn) = config["pg_apply"].as_str() else {
            eprintln!(
                "SKIP pg_apply: harness config lacks pg_apply; destination/crash lane must provision PostgreSQL with origin/DDL privileges"
            );
            return Ok(None);
        };
        let mut admin = Connection::open(dsn).await?;
        let mut intents = 0;
        install_progress(&mut admin.client, |intent| {
            assert_eq!(intent, executor::INSTALL_INTENT);
            intents += 1;
            async { Ok(()) }
        })
        .await?;
        install_progress(&mut admin.client, |_| {
            intents += 1;
            async { Ok(()) }
        })
        .await?;
        assert_eq!(intents, 2);
        let id = format!("p6b_{}", uuid::Uuid::new_v4().simple());
        let origin = format!("skedaddle_{id}");
        admin
            .client
            .query_one("SELECT pg_replication_origin_create($1)", &[&origin])
            .await?;
        if let Err(error) = admin
            .client
            .batch_execute(&format!(
                "CREATE SCHEMA {id}; CREATE TABLE {id}.rows (id integer PRIMARY KEY, body text)"
            ))
            .await
        {
            let _ = admin
                .client
                .query_one("SELECT pg_replication_origin_drop($1)", &[&origin])
                .await;
            return Err(error.into());
        }
        Ok(Some(Self {
            dsn: dsn.into(),
            admin,
            id,
            origin,
        }))
    }
    async fn cleanup(self) -> TestResult<()> {
        // Exact unique names only. Never drop the harness-owned progress table.
        let receipts = self
            .admin
            .client
            .execute(
                "DELETE FROM _skedaddle.apply_receipts WHERE stream = $1",
                &[&self.id],
            )
            .await;
        let deleted = self
            .admin
            .client
            .execute(
                "DELETE FROM _skedaddle.apply_progress WHERE stream = $1",
                &[&self.id],
            )
            .await;
        let dropped = self
            .admin
            .client
            .batch_execute(&format!("DROP SCHEMA {} CASCADE", self.id))
            .await;
        let origin = self
            .admin
            .client
            .query_one("SELECT pg_replication_origin_drop($1)", &[&self.origin])
            .await;
        self.admin.close().await;
        receipts?;
        deleted?;
        dropped?;
        origin?;
        Ok(())
    }
    fn executor(&self) -> executor::Result<Executor<HarnessConnector>> {
        Executor::new(
            HarnessConnector(self.dsn.clone()),
            self.id.clone(),
            self.origin.clone(),
            100,
        )
    }
    fn transaction(&self, kind: EventKind) -> TestResult<AssembledTxn> {
        let source = SourceIdentity {
            engine: "postgres".into(),
            system_id: "fixture".into(),
            lineage: "1".into(),
            db_name: "harness".into(),
            slot: self.id.clone(),
        };
        let value = |name: &str, value: Value| -> OpaqueValue {
            OpaqueValue {
                type_id: name.into(),
                bytes: Some(value.canonical_bytes().unwrap()),
            }
        };
        let key = BTreeMap::from([("id".into(), value("id", Value::Int4(1)))]);
        let mut after = key.clone();
        after.insert(
            "body".into(),
            value(
                "body",
                Value::Text(EncodedBytes {
                    bytes: b"once".to_vec(),
                    encoding: "UTF8".into(),
                }),
            ),
        );
        let event = Event {
            source: source.clone(),
            txn: TxnId(vec![1]),
            ordinal_in_txn: 0,
            position: Position::new(&self.id, 100_u64.to_be_bytes()),
            schema_version: 1,
            kind,
            table: Some("rows".into()),
            key,
            before: None,
            after: Some(after),
            unchanged_cols: Default::default(),
        };
        let txn = AssembledTxn::new(
            source,
            event.txn.clone(),
            event.position.clone(),
            Some(1_700_000_000_000_000),
            vec![event],
        )?;
        Ok(txn)
    }
    fn plan(&self, kind: EventKind, state: RangeState) -> TestResult<TransactionPlan> {
        self.plan_at(&self.transaction(kind)?, 1, state)
    }
    fn plan_at(
        &self,
        txn: &AssembledTxn,
        mseq: u64,
        state: RangeState,
    ) -> TestResult<TransactionPlan> {
        let table = Table {
            event_table: "rows".into(),
            schema: self.id.clone(),
            name: "rows".into(),
            schema_version: 1,
            columns: vec![
                Column {
                    name: "id".into(),
                    type_id: "id".into(),
                    pg_type: PgType::Int4,
                    wire_type: Type::INT4,
                },
                Column {
                    name: "body".into(),
                    type_id: "body".into(),
                    pg_type: PgType::Text {
                        encoding: "UTF8".into(),
                    },
                    wire_type: Type::TEXT,
                },
            ],
            key: vec!["id".into()],
        };
        Ok(plan_transaction(
            txn,
            &[table],
            &PlanContext {
                stream: self.id.clone(),
                mseq,
                origin: self.origin.clone(),
                synthetic_commit_time: None,
            },
            |_, _| Ok(state),
        )?)
    }
    async fn evidence(&self) -> TestResult<(Option<Progress>, u64)> {
        let latest = self.admin.client.query_opt("SELECT last_mseq::text, last_txn, last_position, last_digest FROM _skedaddle.apply_progress WHERE stream = $1", &[&self.id]).await?;
        let latest = latest
            .map(|row| -> TestResult<Progress> {
                let bytes: Vec<u8> = row.get(2);
                let digest: Vec<u8> = row.get(3);
                Ok(Progress {
                    stream: self.id.clone(),
                    mseq: row.get::<_, String>(0).parse()?,
                    txn: TxnId(row.get(1)),
                    lsn: u64::from_be_bytes(bytes.try_into().map_err(|_| "invalid progress LSN")?),
                    digest: digest.try_into().map_err(|_| "invalid progress digest")?,
                    evidence: None,
                })
            })
            .transpose()?;
        let origin: Option<String> = self
            .admin
            .client
            .query_one(
                "SELECT pg_replication_origin_progress($1, true)::text",
                &[&self.origin],
            )
            .await?
            .get(0);
        let origin = match origin {
            None => 0,
            Some(lsn) => {
                let (hi, lo) = lsn.split_once('/').ok_or("invalid origin LSN")?;
                (u64::from_str_radix(hi, 16)? << 32) | u64::from_str_radix(lo, 16)?
            }
        };
        Ok((latest, origin))
    }
    async fn count(&self) -> TestResult<i64> {
        Ok(self
            .admin
            .client
            .query_one(&format!("SELECT count(*) FROM {}.rows", self.id), &[])
            .await?
            .get(0))
    }
    async fn kill(&self, client: &Client) -> TestResult<()> {
        let pid: i32 = client
            .query_one("SELECT pg_backend_pid()", &[])
            .await?
            .get(0);
        let terminated: bool = self
            .admin
            .client
            .query_one("SELECT pg_terminate_backend($1, 5000)", &[&pid])
            .await?
            .get(0);
        if !terminated {
            return Err("backend termination failed".into());
        }
        Ok(())
    }
}

struct HarnessConnector(String);
#[async_trait]
impl Connector for HarnessConnector {
    async fn connect(&mut self) -> executor::Result<Client> {
        let (client, driver) = tls_connect(&self.0)
            .await
            .map_err(|_| executor::Error::Database)?;
        tokio::spawn(async move {
            let _ = driver.await;
        });
        Ok(client)
    }
}
struct Crash<'a> {
    fixture: &'a Fixture,
    seam: Seam,
    lost_commit_response: bool,
    resume_commit: bool,
    fired: bool,
}
#[async_trait]
impl Observer for Crash<'_> {
    async fn observe(&mut self, seam: Seam, client: &Client) -> executor::Result<()> {
        if self.fired || seam != self.seam {
            return Ok(());
        }
        self.fired = true;
        if self.lost_commit_response {
            // Real COMMIT, then kill the dedicated backend before the executor
            // can receive a successful COMMIT response. This forces its actual
            // reconnect/evidence path; in-flight network cuts belong to Docker.
            client
                .batch_execute("COMMIT")
                .await
                .map_err(|_| executor::Error::Database)?;
        }
        self.fixture
            .kill(client)
            .await
            .map_err(|_| executor::Error::Database)?;
        if self.resume_commit {
            Ok(())
        } else {
            Err(executor::Error::Database)
        }
    }
}
async fn replay(fixture: &Fixture, plan: &TransactionPlan) -> TestResult<()> {
    let mut executor = fixture.executor()?;
    executor.apply(plan).await?;
    executor.close().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "REAL-PG APPLY SKIPPED: Docker harness must supply pg_apply; run --ignored --nocapture"]
async fn exactly_one_effect_after_crash_matrix() -> TestResult<()> {
    equal_h_executor_campaign().await?;
    // Real backend termination at each application seam. The Toxiproxy
    // in-flight-COMMIT network cut remains the separate Docker lane campaign.
    for seam in [Seam::BeforeCommit, Seam::AfterCommit, Seam::BeforeAck] {
        let Some(fixture) = Fixture::open().await? else {
            return Ok(());
        };
        let result = async {
            let plan = fixture.plan(EventKind::Insert, RangeState::Completed)?;
            let mut executor = fixture.executor()?;
            let mut crash = Crash {
                fixture: &fixture,
                seam,
                lost_commit_response: false,
                resume_commit: false,
                fired: false,
            };
            require(
                executor.apply_observed(&plan, &mut crash).await.is_err(),
                "crash did not stop executor",
            )?;
            require(crash.fired, "crash seam was not reached")?;
            drop(executor);
            let (latest, origin) = fixture.evidence().await?;
            require(
                resolve_commit(&plan.progress, latest.as_ref(), origin, None)?
                    == if seam == Seam::BeforeCommit {
                        CommitDecision::NotApplied
                    } else {
                        CommitDecision::Applied
                    },
                "crash seam differs from durable outcome",
            )?;
            replay(&fixture, &plan).await?;
            replay(&fixture, &plan).await?;
            require(
                fixture.count().await? == 1,
                "crash replay did not leave exactly one row",
            )?;
            TestResult::Ok(())
        }
        .await;
        let cleanup = fixture.cleanup().await;
        result?;
        cleanup?;
    }
    Ok(())
}

#[tokio::test]
#[ignore = "REAL-PG APPLY SKIPPED: Docker harness must supply pg_apply; run --ignored --nocapture"]
async fn ambiguous_commit_resolved_by_progress() -> TestResult<()> {
    for committed in [false, true] {
        let Some(fixture) = Fixture::open().await? else {
            return Ok(());
        };
        let result = async {
            let plan = fixture.plan(EventKind::Insert, RangeState::Completed)?;
            let mut executor = fixture.executor()?;
            let mut crash = Crash {
                fixture: &fixture,
                seam: Seam::BeforeCommit,
                lost_commit_response: committed,
                resume_commit: true,
                fired: false,
            };
            executor.apply_observed(&plan, &mut crash).await?;
            require(crash.fired, "lost-response injection not reached")?;
            executor.close().await?;
            replay(&fixture, &plan).await?;
            require(
                fixture.count().await? == 1,
                "ambiguous replay duplicated effects",
            )?;
            TestResult::Ok(())
        }
        .await;
        let cleanup = fixture.cleanup().await;
        result?;
        cleanup?;
    }
    Ok(())
}

#[tokio::test]
#[ignore = "REAL-PG APPLY SKIPPED: Docker harness must supply pg_apply; run --ignored --nocapture"]
async fn digest_mismatch_on_skip_is_error() -> TestResult<()> {
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = async {
        let plan = fixture.plan(EventKind::Insert, RangeState::Completed)?;
        replay(&fixture, &plan).await?;
        let mut target = plan.clone();
        target.progress.digest[0] ^= 1;
        let mut executor = fixture.executor()?;
        require(
            matches!(
                executor.apply(&target).await,
                Err(executor::Error::Apply(ApplyError::DigestMismatch))
            ),
            "mismatched latest digest was not rejected",
        )?;
        executor.close().await?;
        // Advance to a later transaction in the SAME segment; a replay of the
        // first now requires historical receipt evidence, not the latest digest.
        // Fixture-only marker plan; use the planner so origin and progress SQL
        // match the new envelope rather than mutating its bound statements.
        let later_txn = AssembledTxn::new(
            SourceIdentity {
                engine: "postgres".into(),
                system_id: "fixture".into(),
                lineage: "1".into(),
                db_name: "harness".into(),
                slot: fixture.id.clone(),
            },
            TxnId(vec![2]),
            Position::new(&fixture.id, (plan.progress.lsn + 1).to_be_bytes()),
            Some(1_700_000_000_000_000),
            vec![],
        )?;
        let later = plan_transaction(
            &later_txn,
            &[],
            &PlanContext {
                stream: fixture.id.clone(),
                mseq: 1,
                origin: fixture.origin.clone(),
                synthetic_commit_time: None,
            },
            |_, _| unreachable!(),
        )?;
        let mut capped = Executor::new(
            HarnessConnector(fixture.dsn.clone()),
            fixture.id.clone(),
            fixture.origin.clone(),
            1,
        )?;
        require(
            matches!(
                capped.apply(&later).await,
                Err(executor::Error::ReceiptLimit)
            ),
            "receipt cap failed to stop apply",
        )?;
        capped.close().await?;
        require(
            fixture.evidence().await?.0 == Some(plan.progress.clone()),
            "receipt limit advanced progress",
        )?;
        replay(&fixture, &later).await?;
        replay(&fixture, &plan).await?;
        let mut executor = fixture.executor()?;
        require(
            matches!(
                executor.apply(&target).await,
                Err(executor::Error::Apply(ApplyError::DigestMismatch))
            ),
            "historical digest mismatch was not rejected",
        )?;
        executor.close().await?;
        fixture
            .admin
            .client
            .execute(
                "DELETE FROM _skedaddle.apply_receipts WHERE stream = $1 AND position = $2",
                &[&fixture.id, &plan.progress.lsn.to_be_bytes().as_slice()],
            )
            .await?;
        let mut executor = fixture.executor()?;
        require(
            matches!(
                executor.apply(&plan).await,
                Err(executor::Error::Apply(ApplyError::MissingDigestEvidence))
            ),
            "missing receipt did not fail closed",
        )?;
        executor.close().await?;
        // The cap must block a new transaction without deleting historical
        // evidence, then a durable segment checkpoint permits receipt pruning.
        let mut capped = Executor::new(
            HarnessConnector(fixture.dsn.clone()),
            fixture.id.clone(),
            fixture.origin.clone(),
            1,
        )?;
        require(
            matches!(capped.apply(&later).await, Ok(CommitDecision::Applied)),
            "latest receipt skip should not consume capacity",
        )?;
        capped
            .retain_after_checkpoint(&skedaddle::buffer::reader::ReaderCheckpoint {
                next_mseq: 2,
                ..Default::default()
            })
            .await?;
        let receipts: i64 = fixture
            .admin
            .client
            .query_one(
                "SELECT count(*) FROM _skedaddle.apply_receipts WHERE stream = $1",
                &[&fixture.id],
            )
            .await?
            .get(0);
        require(
            receipts == 0,
            "durable checkpoint did not prune older receipts",
        )?;
        capped.close().await?;
        require(fixture.count().await? == 1, "digest check altered effects")?;
        TestResult::Ok(())
    }
    .await;
    let cleanup = fixture.cleanup().await;
    result?;
    cleanup
}

#[tokio::test]
#[ignore = "REAL-PG APPLY SKIPPED: Docker harness must supply pg_apply; run --ignored --nocapture"]
async fn completed_range_missing_row_is_fault() -> TestResult<()> {
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = async {
        for kind in [EventKind::Update, EventKind::Delete] {
            let plan = fixture.plan(kind, RangeState::Completed)?;
            let mut executor = fixture.executor()?;
            let outcome = executor.apply(&plan).await;
            require(
                matches!(
                    outcome,
                    Err(executor::Error::Apply(ApplyError::MissingRow {
                        ordinal: 0,
                        ..
                    }))
                ),
                "missing row did not produce typed verifier fault",
            )?;
            // Read through another connection before close: rollback must have
            // happened inside the executor, not in test cleanup.
            require(
                fixture.evidence().await? == (None, 0),
                "failed apply advanced progress",
            )?;
            require(fixture.count().await? == 0, "failed apply wrote a row")?;
            let receipts: i64 = fixture
                .admin
                .client
                .query_one(
                    "SELECT count(*) FROM _skedaddle.apply_receipts WHERE stream = $1",
                    &[&fixture.id],
                )
                .await?
                .get(0);
            require(receipts == 0, "fault committed receipt")?;
            executor.close().await?;
        }
        TestResult::Ok(())
    }
    .await;
    let cleanup = fixture.cleanup().await;
    result?;
    cleanup
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

struct PairOrder(String);
impl skedaddle::event::PositionOrder for PairOrder {
    fn domain(&self) -> &str {
        &self.0
    }
    fn compare(
        &self,
        a: &[u8],
        b: &[u8],
    ) -> Result<std::cmp::Ordering, skedaddle::event::EventError> {
        Ok(a.cmp(b))
    }
}

async fn pair_buffer(
    fixture: &Fixture,
) -> TestResult<(
    std::sync::Arc<dyn skedaddle::buffer::backends::BufferStore>,
    skedaddle::buffer::reader::ReaderConfig,
)> {
    use skedaddle::buffer::{
        backends::{BufferStore, ObjectBufferStore},
        format::{
            Limits, ManifestEntry, SegmentEncoder, SegmentHeader, SegmentRecord, segment_key,
        },
        reader::ReaderConfig,
    };
    let root = std::path::PathBuf::from(
        std::env::var_os("SKEDADDLE_TEST_ROOT").ok_or("SKEDADDLE_TEST_ROOT required")?,
    )
    .join(&fixture.id);
    std::fs::create_dir_all(&root)?;
    let store: std::sync::Arc<dyn BufferStore> = std::sync::Arc::new(ObjectBufferStore::local(
        root,
        std::time::Duration::from_secs(30),
    )?);
    let config = ReaderConfig {
        prefix: "buffer".into(),
        migration_id: fixture.id.clone(),
        stream: fixture.id.clone(),
        limits: Limits::default(),
        required_next_mseq: 2,
    };
    let h = fixture.transaction(EventKind::Insert)?;
    let mut rows = fixture.transaction(EventKind::ChunkRows)?;
    rows.txn = TxnId(vec![2]);
    rows.events[0].txn = rows.txn.clone();
    let rows = AssembledTxn::new(
        rows.source,
        rows.txn,
        rows.commit_position,
        rows.commit_time,
        rows.events,
    )?;
    let mut previous = [0; 32];
    for (mseq, record) in [
        SegmentRecord::Transaction(h),
        SegmentRecord::ChunkRows(rows),
    ]
    .into_iter()
    .enumerate()
    {
        let pos = record.position().clone();
        let mut encoder = SegmentEncoder::new(
            Vec::new(),
            SegmentHeader {
                migration_id: config.migration_id.clone(),
                stream: config.stream.clone(),
                epoch: 1,
                first_pos: pos.clone(),
                last_pos: pos.clone(),
                record_count: 1,
            },
            config.limits,
        )?;
        encoder.write_record(&record)?;
        let (bytes, summary) = encoder.finish()?;
        let mut key = segment_key(
            &config.prefix,
            &config.migration_id,
            &config.stream,
            &pos,
            &pos,
            1,
        );
        if mseq == 1 {
            key.push_str(".chunk-rows");
        }
        let entry = ManifestEntry {
            mseq: mseq as u64,
            epoch: 1,
            prev_digest: previous,
            segment_key: key,
            segment_digest: summary.digest,
            first_pos: pos.clone(),
            last_pos: pos,
            txn_count: 1,
            bytes: summary.bytes,
        };
        store.put_create_only(&entry.segment_key, &bytes).await?;
        store
            .put_create_only(&config.manifest_key(entry.mseq), &entry.canonical_json()?)
            .await?;
        previous = entry.digest()?;
    }
    Ok((store, config))
}

// Only receipt GC is delayed, modeling retained evidence after a cleanup stall.
// SQL, origin advancement, COMMIT, reconnect and all decisions use real Executor.
struct RetainedExecutor<'a> {
    executor: Executor<HarnessConnector>,
    crash: Option<Crash<'a>>,
    plans: Vec<TransactionPlan>,
}
#[async_trait]
impl Destination for RetainedExecutor<'_> {
    async fn apply(&mut self, plan: &TransactionPlan) -> executor::Result<CommitDecision> {
        self.plans.push(plan.clone());
        if plan.progress.mseq == 1
            && let Some(crash) = self.crash.as_mut()
        {
            self.executor.apply_observed(plan, crash).await
        } else {
            self.executor.apply(plan).await
        }
    }
    async fn retain_after_checkpoint(
        &mut self,
        _: &skedaddle::buffer::reader::ReaderCheckpoint,
    ) -> executor::Result<()> {
        Ok(())
    }
}

async fn equal_h_executor_campaign() -> TestResult<()> {
    use skedaddle::buffer::reader::{BufferReader, ReaderCheckpoint};
    use skedaddle::pg::apply::executor::ReaderLoop;
    // Normal COMMIT; rollback/retry; committed-but-unacknowledged COMMIT; and
    // reconnect after the two post-COMMIT fault seams.
    for mode in 0..5 {
        let Some(fixture) = Fixture::open().await? else {
            return Ok(());
        };
        let result = async {
            let (store, config) = pair_buffer(&fixture).await?;
            let order = std::sync::Arc::new(PairOrder(fixture.id.clone()));
            let reader = BufferReader::new(
                store.clone(),
                config.clone(),
                ReaderCheckpoint::default(),
                order.clone(),
            )?;
            let mut runner = ReaderLoop::new(reader, fixture.id.clone());
            let crash = (mode != 0).then_some(Crash {
                fixture: &fixture,
                seam: match mode {
                    3 => Seam::AfterCommit,
                    4 => Seam::BeforeAck,
                    _ => Seam::BeforeCommit,
                },
                lost_commit_response: mode == 2,
                resume_commit: mode <= 2,
                fired: false,
            });
            let mut destination = RetainedExecutor {
                executor: fixture.executor()?,
                crash,
                plans: vec![],
            };
            let mut durable = ReaderCheckpoint::default();
            runner
                .step(
                    &mut destination,
                    |txn, mseq| {
                        fixture
                            .plan_at(txn, mseq, RangeState::Completed)
                            .map_err(|_| ApplyError::InvalidTransaction)
                    },
                    |cp| {
                        durable = cp;
                        async { Ok(()) }
                    },
                )
                .await?;
            require(durable.next_mseq == 1, "H checkpoint was not saved")?;
            let outcome = runner
                .step(
                    &mut destination,
                    |txn, mseq| {
                        fixture
                            .plan_at(txn, mseq, RangeState::Completed)
                            .map_err(|_| ApplyError::InvalidTransaction)
                    },
                    |_| async { Err(executor::Error::Checkpoint) },
                )
                .await;
            require(
                outcome.is_err(),
                "checkpoint/fault injection did not stop the loop",
            )?;
            if let Some(crash) = &destination.crash {
                require(crash.fired, "pair crash seam not reached")?;
            }
            let h = destination.plans[0].clone();
            let rows = destination.plans[1].clone();
            require(
                h.progress.evidence.is_some() && rows.progress.evidence.is_some(),
                "pair lost verified envelope evidence",
            )?;
            if mode >= 3 {
                drop(destination.executor);
            } else {
                destination.executor.close().await?;
            }
            let receipts: i64 = fixture
                .admin
                .client
                .query_one(
                    "SELECT count(*) FROM _skedaddle.apply_receipts WHERE stream=$1",
                    &[&fixture.id],
                )
                .await?
                .get(0);
            require(
                receipts == 2,
                "pair did not commit two distinct manifest receipts",
            )?;
            // A redelivery must not execute the upsert again: changing the live
            // value makes a duplicate row effect visible even though it is an upsert.
            fixture
                .admin
                .client
                .batch_execute(&format!("UPDATE {}.rows SET body='sentinel'", fixture.id))
                .await?;
            replay(&fixture, &h).await?; // authenticated historical Transaction(H)
            replay(&fixture, &rows).await?; // exact ChunkRows(H) redelivery
            let mut corrupt = h.clone();
            corrupt.progress.digest[0] ^= 1;
            let mut executor = fixture.executor()?;
            require(
                matches!(
                    executor.apply(&corrupt).await,
                    Err(executor::Error::Apply(ApplyError::DigestMismatch))
                ),
                "equal-H historical digest conflict accepted",
            )?;
            executor.close().await?;
            // Actual restart from the last saved checkpoint replays ChunkRows(H)
            // after its commit but before its checkpoint could be saved.
            let reader = BufferReader::new(store, config, durable, order)?;
            let mut restarted = ReaderLoop::new(reader, fixture.id.clone());
            let mut executor = fixture.executor()?;
            restarted
                .step(
                    &mut executor,
                    |txn, mseq| {
                        fixture
                            .plan_at(txn, mseq, RangeState::Completed)
                            .map_err(|_| ApplyError::InvalidTransaction)
                    },
                    |cp| async move {
                        require(cp.next_mseq == 2, "pair checkpoint did not advance")
                            .map_err(|_| executor::Error::Checkpoint)
                    },
                )
                .await?;
            let body: String = fixture
                .admin
                .client
                .query_one(
                    &format!("SELECT body FROM {}.rows WHERE id=1", fixture.id),
                    &[],
                )
                .await?
                .get(0);
            require(
                body == "sentinel" && fixture.count().await? == 1,
                "pair replay executed row effects twice",
            )?;
            // The successful checkpoint prunes the historical H receipt.
            require(
                matches!(
                    executor.apply(&h).await,
                    Err(executor::Error::Apply(ApplyError::MissingDigestEvidence))
                ),
                "pruned historical receipt did not fail closed",
            )?;
            executor.close().await?;
            TestResult::Ok(())
        }
        .await;
        let cleanup = fixture.cleanup().await;
        result?;
        cleanup?;
    }
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = async {
        let (store, config) = pair_buffer(&fixture).await?;
        let order = std::sync::Arc::new(PairOrder(fixture.id.clone()));
        let mut reader = BufferReader::new(
            store.clone(),
            config.clone(),
            ReaderCheckpoint::default(),
            order.clone(),
        )?;
        reader.next_txn().await?;
        let checkpoint = reader.checkpoint().clone();
        let reader = BufferReader::new(store, config, checkpoint, order)?;
        let mut runner = ReaderLoop::new(reader, fixture.id.clone());
        let mut executor = fixture.executor()?;
        let result = runner
            .step(
                &mut executor,
                |txn, mseq| {
                    fixture
                        .plan_at(txn, mseq, RangeState::Completed)
                        .map_err(|_| ApplyError::InvalidTransaction)
                },
                |_| async { Err(executor::Error::Checkpoint) },
            )
            .await;
        require(
            matches!(
                result,
                Err(executor::Error::Apply(ApplyError::InconsistentProgress))
            ),
            "ChunkRows without applied H was accepted",
        )?;
        executor.close().await?;
        require(
            fixture.evidence().await? == (None, 0) && fixture.count().await? == 0,
            "missing H changed destination state",
        )?;
        TestResult::Ok(())
    }
    .await;
    let cleanup = fixture.cleanup().await;
    result?;
    cleanup?;

    Ok(())
}
