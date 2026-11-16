//! Real-PG qualification, provisioned ONLY by the Docker harness lane.
//! Set SKEDADDLE_TEST_CONFIG with a `pg_apply` superuser DSN. The production
//! installer provisions progress/receipts. No service is started by this binary.
//! Harness invocation: cargo test --test pg_apply -- --ignored --nocapture.
use async_trait::async_trait;
use std::{collections::BTreeMap, error::Error, sync::Arc};

use skedaddle::{
    event::{AssembledTxn, Event, EventKind, OpaqueValue, Position, SourceIdentity, TxnId},
    mapping::{EncodedBytes, PgType, Value},
    pg::apply::{
        ApplyError, Column, CommitDecision, OriginPolicy, PlanContext, Progress, RangeState,
        SourcePositions, Table, TransactionPlan,
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
        stream: "slot".into(),
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
            positions: SourcePositions::native_pg("pg/source"),
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
    assert_eq!(
        plan.progress.position,
        Position::new("pg/source", 100_u64.to_be_bytes())
    );
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
        for table in ["apply_progress_v3", "apply_receipts_v3"] {
            if self
                .admin
                .client
                .query_one(
                    "SELECT to_regclass($1) IS NOT NULL",
                    &[&format!("_skedaddle.{table}")],
                )
                .await?
                .get::<_, bool>(0)
            {
                self.admin
                    .client
                    .execute(
                        &format!("DELETE FROM _skedaddle.{table} WHERE stream=$1"),
                        &[&self.id],
                    )
                    .await?;
            }
        }
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
            SourcePositions::native_pg(&self.id),
        )
    }
    fn transaction(&self, kind: EventKind) -> TestResult<AssembledTxn> {
        let source = SourceIdentity {
            engine: "postgres".into(),
            system_id: "fixture".into(),
            lineage: "1".into(),
            db_name: "harness".into(),
            stream: self.id.clone(),
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
        self.plan_with_positions(txn, mseq, state, SourcePositions::native_pg(&self.id))
    }
    fn plan_with_positions(
        &self,
        txn: &AssembledTxn,
        mseq: u64,
        state: RangeState,
        positions: SourcePositions,
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
                positions,
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
                    position: Position::new(
                        &self.id,
                        <[u8; 8]>::try_from(bytes).map_err(|_| "invalid progress LSN")?,
                    ),
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
    async fn receipt(&self, target: &Progress) -> TestResult<Option<Progress>> {
        let row = self.admin.client.query_opt(
            "SELECT txn, digest, record_kind, manifest_digest, previous_manifest_digest FROM _skedaddle.apply_receipts WHERE stream=$1 AND mseq=$2::text::numeric AND position=$3",
            &[&self.id, &target.mseq.to_string(), &target.position.bytes()],
        ).await?;
        row.map(|r| {
            require(
                target.evidence.is_none(),
                "ordinary receipt helper requires legacy evidence",
            )?;
            require(
                r.get::<_, Option<i16>>(2).is_none()
                    && r.get::<_, Option<Vec<u8>>>(3).is_none()
                    && r.get::<_, Option<Vec<u8>>>(4).is_none(),
                "unexpected receipt evidence encoding",
            )?;
            let mut receipt = target.clone();
            receipt.txn = TxnId(r.get(0));
            receipt.digest = r
                .get::<_, Vec<u8>>(1)
                .try_into()
                .map_err(|_| "invalid receipt digest")?;
            Ok(receipt)
        })
        .transpose()
    }
    async fn commit_evidence(&self, plan: &TransactionPlan, committed: bool) -> TestResult<()> {
        let (latest, native) = self.evidence().await?;
        let receipt = self.receipt(&plan.progress).await?;
        eprintln!("commit evidence: L={latest:?}, R={receipt:?}, N={native}");
        require(
            receipt.is_some() == committed,
            "incorrect receipt after termination",
        )?;
        require(
            latest.is_some() == committed,
            "incorrect progress after termination",
        )?;
        require(
            self.count().await? == i64::from(committed),
            "incorrect effects after termination",
        )?;
        require(
            resolve_commit(
                &SourcePositions::native_pg(&self.id),
                &plan.progress,
                latest.as_ref(),
                native,
                receipt.as_ref(),
            )? == if committed {
                CommitDecision::Applied
            } else {
                CommitDecision::NotApplied
            },
            "transactional evidence disagrees with commit",
        )?;
        if committed {
            require(
                receipt.as_ref() == Some(&plan.progress) && latest.as_ref() == Some(&plan.progress),
                "committed identity/digest changed",
            )?;
        }
        Ok(())
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
    plan: Option<&'a TransactionPlan>,
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
        if let Some(plan) = self.plan {
            self.fixture
                .commit_evidence(
                    plan,
                    self.lost_commit_response || seam != Seam::BeforeCommit,
                )
                .await
                .map_err(|_| executor::Error::Database)?;
        }
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
                plan: Some(&plan),
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
                resolve_commit(
                    &SourcePositions::native_pg(&fixture.id),
                    &plan.progress,
                    latest.as_ref(),
                    origin,
                    None,
                )? == if seam == Seam::BeforeCommit {
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
            fixture.commit_evidence(&plan, true).await?;
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
                plan: Some(&plan),
                seam: Seam::BeforeCommit,
                lost_commit_response: committed,
                resume_commit: true,
                fired: false,
            };
            executor.apply_observed(&plan, &mut crash).await?;
            require(crash.fired, "lost-response injection not reached")?;
            executor.close().await?;
            replay(&fixture, &plan).await?;
            replay(&fixture, &plan).await?;
            fixture.commit_evidence(&plan, true).await?;
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
                stream: fixture.id.clone(),
            },
            TxnId(vec![2]),
            Position::new(
                &fixture.id,
                (u64::from_be_bytes(plan.progress.position.bytes().try_into().unwrap()) + 1)
                    .to_be_bytes(),
            ),
            Some(1_700_000_000_000_000),
            vec![],
        )?;
        let later = plan_transaction(
            &later_txn,
            &[],
            &PlanContext {
                positions: SourcePositions::native_pg(&fixture.id),
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
            SourcePositions::native_pg(&fixture.id),
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
                &[&fixture.id, &plan.progress.position.bytes()],
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
            SourcePositions::native_pg(&fixture.id),
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
                fixture.evidence().await?.0.is_none(),
                "failed apply committed progress",
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
    marker: bool,
) -> TestResult<(
    std::sync::Arc<dyn skedaddle::buffer::backends::BufferStore>,
    skedaddle::buffer::reader::ReaderConfig,
)> {
    let h = fixture.transaction(if marker {
        EventKind::FinalMarker
    } else {
        EventKind::Insert
    })?;
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
    pair_buffer_records(
        fixture,
        vec![
            skedaddle::buffer::format::SegmentRecord::Transaction(h),
            skedaddle::buffer::format::SegmentRecord::ChunkRows(rows),
        ],
    )
    .await
}
async fn pair_buffer_records(
    fixture: &Fixture,
    records: Vec<skedaddle::buffer::format::SegmentRecord>,
) -> TestResult<(
    Arc<dyn skedaddle::buffer::backends::BufferStore>,
    skedaddle::buffer::reader::ReaderConfig,
)> {
    use skedaddle::buffer::{
        backends::{BufferStore, ObjectBufferStore},
        format::{Limits, ManifestEntry, SegmentEncoder, SegmentHeader, segment_key},
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
    let mut previous = [0; 32];
    for (mseq, record) in records.into_iter().enumerate() {
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
            let (store, config) = pair_buffer(&fixture, false).await?;
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
                plan: None,
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
            let before_rows = durable.clone();
            if mode == 1 {
                destination.executor.close().await?;
                abort_origin_attempt(&fixture, 200).await?;
                destination.executor = fixture.executor()?;
                require(fixture.evidence().await?.1 > 100, "abort did not lead H")?;
            }
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
            require(
                durable == before_rows,
                "failed apply advanced durable checkpoint",
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
        let (store, config) = pair_buffer(&fixture, false).await?;
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

// Set transaction metadata and write WAL, then explicitly roll back. No native
// reset is performed: PostgreSQL may retain the attempted position as high-water.
async fn abort_origin_attempt(fixture: &Fixture, lsn: u64) -> TestResult<()> {
    let connection = Connection::open(&fixture.dsn).await?;
    connection
        .client
        .query_one(
            "SELECT pg_replication_origin_session_setup($1)",
            &[&fixture.origin],
        )
        .await?;
    connection.client.batch_execute("BEGIN").await?;
    connection
        .client
        .query_one(
            "SELECT pg_replication_origin_xact_setup($1::text::pg_lsn, now())",
            &[&format!("0/{lsn:X}")],
        )
        .await?;
    connection
        .client
        .batch_execute(&format!(
            "INSERT INTO {}.rows VALUES (2, 'aborted'); ROLLBACK",
            fixture.id
        ))
        .await?;
    connection.close().await;
    Ok(())
}

#[tokio::test]
#[ignore = "REAL-PG APPLY SKIPPED: Docker harness must supply pg_apply; run --ignored --nocapture"]
async fn explicit_rollback_and_committed_prefix_with_aborted_successor() -> TestResult<()> {
    // The reverse runner constructs the same executor with a distinct _rev
    // stream/origin. Exercise both bindings without changing reverse production.
    for reverse in [false, true] {
        let Some(mut fixture) = Fixture::open().await? else {
            return Ok(());
        };
        if reverse {
            fixture
                .admin
                .client
                .query_one("SELECT pg_replication_origin_drop($1)", &[&fixture.origin])
                .await?;
            let old = fixture.id.clone();
            fixture.id.push_str("_rev");
            fixture.origin = format!("skedaddle_{}", fixture.id);
            fixture
                .admin
                .client
                .batch_execute(&format!("ALTER SCHEMA {old} RENAME TO {}", fixture.id))
                .await?;
            fixture
                .admin
                .client
                .query_one(
                    "SELECT pg_replication_origin_create($1)",
                    &[&fixture.origin],
                )
                .await?;
        }
        let result = async {
            let a = fixture.plan(EventKind::Insert, RangeState::Completed)?;
            abort_origin_attempt(&fixture, 150).await?;
            fixture.commit_evidence(&a, false).await?;
            replay(&fixture, &a).await?;
            let mut b = fixture.transaction(EventKind::Update)?;
            b.txn = TxnId(vec![2]);
            b.commit_position = Position::new(&fixture.id, 175_u64.to_be_bytes());
            b.events[0].txn = b.txn.clone();
            b.events[0].position = b.commit_position.clone();
            let b = AssembledTxn::new(b.source, b.txn, b.commit_position, b.commit_time, b.events)?;
            let b = fixture.plan_at(&b, 2, RangeState::Completed)?;
            let mut executor = fixture.executor()?;
            let mut crash = Crash {
                fixture: &fixture,
                plan: None,
                seam: Seam::BeforeCommit,
                lost_commit_response: false,
                resume_commit: false,
                fired: false,
            };
            require(
                executor.apply_observed(&b, &mut crash).await.is_err() && crash.fired,
                "B was not aborted before commit",
            )?;
            drop(executor);
            let (latest, native) = fixture.evidence().await?;
            require(
                native > u64::from_be_bytes(a.progress.position.bytes().try_into().unwrap()),
                "abort failed to retain native lead",
            )?;
            require(
                resolve_commit(
                    &SourcePositions::native_pg(&fixture.id),
                    &a.progress,
                    latest.as_ref(),
                    native,
                    fixture.receipt(&a.progress).await?.as_ref(),
                )? == CommitDecision::Applied,
                "abort invalidated committed A",
            )?;
            require(
                fixture.receipt(&b.progress).await?.is_none(),
                "aborted B has receipt",
            )?;
            require(
                resolve_commit(
                    &SourcePositions::native_pg(&fixture.id),
                    &b.progress,
                    latest.as_ref(),
                    native,
                    None,
                )? == CommitDecision::NotApplied,
                "native lead skipped B",
            )?;
            replay(&fixture, &a).await?;
            replay(&fixture, &b).await?;
            fixture
                .admin
                .client
                .batch_execute(&format!("UPDATE {}.rows SET body='sentinel'", fixture.id))
                .await?;
            replay(&fixture, &b).await?;
            let body: String = fixture
                .admin
                .client
                .query_one(
                    &format!("SELECT body FROM {}.rows WHERE id=1", fixture.id),
                    &[],
                )
                .await?
                .get(0);
            require(body == "sentinel", "B redelivery repeated row effect")?;
            fixture.commit_evidence(&b, true).await?;
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
async fn competing_same_origin_executor_is_rejected() -> TestResult<()> {
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = async {
        let plan = fixture.plan(EventKind::Insert, RangeState::Completed)?;
        let mut owner = fixture.executor()?;
        owner.apply(&plan).await?;
        let mut competitor = fixture.executor()?;
        require(
            matches!(
                competitor.apply(&plan).await,
                Err(executor::Error::Database)
            ),
            "competing origin owner was accepted",
        )?;
        competitor.close().await?;
        owner.close().await?;
        replay(&fixture, &plan).await?;
        fixture.commit_evidence(&plan, true).await
    }
    .await;
    let cleanup = fixture.cleanup().await;
    result?;
    cleanup
}

#[tokio::test]
#[ignore = "REAL-PG APPLY SKIPPED: Docker harness must supply pg_apply; run --ignored --nocapture"]
async fn aborted_final_marker_does_not_publish_checkpoint_from_native_lead() -> TestResult<()> {
    use skedaddle::buffer::reader::{BufferReader, ReaderCheckpoint};
    use skedaddle::pg::apply::executor::ReaderLoop;
    let Some(fixture) = Fixture::open().await? else {
        return Ok(());
    };
    let result = async {
        let (store, config) = pair_buffer(&fixture, true).await?;
        abort_origin_attempt(&fixture, 200).await?;
        let order = std::sync::Arc::new(PairOrder(fixture.id.clone()));
        let reader = BufferReader::new(
            store.clone(),
            config.clone(),
            ReaderCheckpoint::default(),
            order.clone(),
        )?;
        let mut runner = ReaderLoop::new(reader, fixture.id.clone());
        let mut destination = RetainedExecutor {
            executor: fixture.executor()?,
            crash: None,
            plans: vec![],
        };
        // RetainedExecutor injects on mseq 1 for the pair campaign; this marker
        // is mseq 0, so use a dedicated observer destination below.
        let mut marker_destination = MarkerCrash {
            executor: fixture.executor()?,
            crash: Crash {
                fixture: &fixture,
                plan: None,
                seam: Seam::BeforeCommit,
                lost_commit_response: false,
                resume_commit: false,
                fired: false,
            },
            plan: None,
        };
        let mut published = false;
        let outcome = runner
            .step(
                &mut marker_destination,
                |txn, mseq| {
                    fixture
                        .plan_at(txn, mseq, RangeState::Completed)
                        .map_err(|_| ApplyError::InvalidTransaction)
                },
                |_| {
                    published = true;
                    async { Ok(()) }
                },
            )
            .await;
        require(
            outcome.is_err() && marker_destination.crash.fired && !published,
            "aborted marker published checkpoint/activation evidence",
        )?;
        let plan = marker_destination
            .plan
            .as_ref()
            .ok_or("marker plan absent")?;
        let (latest, native) = fixture.evidence().await?;
        require(
            native > u64::from_be_bytes(plan.progress.position.bytes().try_into().unwrap())
                && latest.is_none()
                && fixture.receipt(&plan.progress).await?.is_none(),
            "aborted marker has transactional evidence",
        )?;
        drop(marker_destination);
        let reader = BufferReader::new(store, config, ReaderCheckpoint::default(), order)?;
        let mut restarted = ReaderLoop::new(reader, fixture.id.clone());
        restarted
            .step(
                &mut destination,
                |txn, mseq| {
                    fixture
                        .plan_at(txn, mseq, RangeState::Completed)
                        .map_err(|_| ApplyError::InvalidTransaction)
                },
                |cp| {
                    published = cp.next_mseq == 1;
                    async { Ok(()) }
                },
            )
            .await?;
        require(
            published && fixture.count().await? == 0,
            "replayed marker did not publish only after commit",
        )?;
        require(
            destination.executor.apply(&destination.plans[0]).await? == CommitDecision::Applied,
            "committed marker lacked authenticated skip",
        )?;
        destination.executor.close().await?;
        TestResult::Ok(())
    }
    .await;
    let cleanup = fixture.cleanup().await;
    result?;
    cleanup
}

struct MarkerCrash<'a> {
    executor: Executor<HarnessConnector>,
    crash: Crash<'a>,
    plan: Option<TransactionPlan>,
}
#[async_trait]
impl Destination for MarkerCrash<'_> {
    async fn apply(&mut self, plan: &TransactionPlan) -> executor::Result<CommitDecision> {
        self.plan = Some(plan.clone());
        self.executor.apply_observed(plan, &mut self.crash).await
    }
    async fn retain_after_checkpoint(
        &mut self,
        _: &skedaddle::buffer::reader::ReaderCheckpoint,
    ) -> executor::Result<()> {
        Ok(())
    }
}

fn mysql_positions(gtid: bool) -> TestResult<(SourcePositions, Position, Position)> {
    use skedaddle::{
        event::PositionOrder,
        mysql::{GtidSet, MysqlOrder, MysqlPosition},
    };
    let server = "11111111-1111-1111-1111-111111111111";
    let order = if gtid {
        MysqlOrder::gtid("opaque-live")?
    } else {
        MysqlOrder::file(server)?
    };
    let (a, b) = if gtid {
        // Random UUIDs resist TOAST compression; the full position cannot be a B-tree key.
        let mut text = (0..160)
            .map(|_| format!("{}:1-3", uuid::Uuid::new_v4()))
            .collect::<Vec<_>>()
            .join(",");
        // Filling one GTID hole compresses the canonical set: source order
        // advances while the encoded position becomes shorter and sorts first.
        text.push_str(&format!(",{server}:1-9:11-20"));
        let set: GtidSet = text.parse()?;
        let mut later = set.clone();
        later.add(&format!("{server}:10"))?;
        (
            MysqlPosition::Gtid {
                set,
                server_uuid: server.into(),
                file: "mysql-bin.000001".into(),
                offset: 4,
            },
            MysqlPosition::Gtid {
                set: later,
                server_uuid: server.into(),
                file: "mysql-bin.000001".into(),
                offset: 1000,
            },
        )
    } else {
        (
            MysqlPosition::File {
                server_uuid: server.into(),
                file_seq: 1,
                offset: 4,
            },
            MysqlPosition::File {
                server_uuid: server.into(),
                file_seq: 1,
                offset: 1000,
            },
        )
    };
    let a = Position::new(order.domain(), a.encode()?);
    let b = Position::new(order.domain(), b.encode()?);
    assert_eq!(a.compare(&b, &order)?, std::cmp::Ordering::Less);
    if gtid {
        assert!(a.bytes().len() > 3072 && b.bytes().len() > 3072);
        assert!(a.bytes() > b.bytes());
        assert!(a.bytes().len() > b.bytes().len());
    }
    Ok((
        SourcePositions::new(Arc::new(order), OriginPolicy::Opaque),
        a,
        b,
    ))
}
fn opaque_txn(f: &Fixture, position: Position, id: i32) -> TestResult<AssembledTxn> {
    let mut txn = f.transaction(EventKind::Insert)?;
    txn.source.engine = "mysql".into();
    txn.txn = TxnId(id.to_be_bytes().to_vec());
    txn.commit_position = position.clone();
    for event in &mut txn.events {
        event.source = txn.source.clone();
        event.txn = txn.txn.clone();
        event.position = position.clone();
        let value = OpaqueValue {
            type_id: "id".into(),
            bytes: Some(Value::Int4(id).canonical_bytes()?),
        };
        event.key.insert("id".into(), value.clone());
        event.after.as_mut().unwrap().insert("id".into(), value);
    }
    Ok(AssembledTxn::new(
        txn.source,
        txn.txn,
        position,
        Some(i64::MIN),
        txn.events,
    )?)
}
fn opaque_executor(
    f: &Fixture,
    positions: SourcePositions,
) -> executor::Result<Executor<HarnessConnector>> {
    Executor::new(
        HarnessConnector(f.dsn.clone()),
        f.id.clone(),
        f.origin.clone(),
        100,
        positions,
    )
}

#[tokio::test]
#[ignore = "REAL-PG APPLY SKIPPED: Docker harness must supply pg_apply; run --ignored --nocapture"]
async fn opaque_mysql_positions_recover_receipts_and_filter_session_origin() -> TestResult<()> {
    for (gtid, committed) in [(false, false), (false, true), (true, false), (true, true)] {
        let Some(mut f) = Fixture::open().await? else {
            return Ok(());
        };
        let slot = format!("{}_opaque", f.id);
        let publication = slot.clone();
        let result = async {
            executor::install_opaque_progress(&mut f.admin.client, |intent| { assert_eq!(intent, executor::INSTALL_OPAQUE_INTENT); async { Ok(()) } }).await?;
            executor::install_opaque_progress(&mut f.admin.client, |_| async { Ok(()) }).await?;
            f.admin.client.batch_execute(&format!("CREATE PUBLICATION {publication} FOR TABLE {}.rows",f.id)).await?;
            f.admin.client.query_one("SELECT pg_create_logical_replication_slot($1,'pgoutput')", &[&slot]).await?;
            let (positions,a,b) = mysql_positions(gtid)?;
            let txn = opaque_txn(&f,a,1)?;
            let first = f.plan_with_positions(&txn,1,RangeState::Completed,positions.clone())?;
            let second = f.plan_with_positions(&opaque_txn(&f,b,2)?,1,RangeState::Completed,positions.clone())?;
            let mut destination = opaque_executor(&f,positions.clone())?;
            let mut crash = Crash { fixture: &f, plan: None, seam: Seam::BeforeCommit,
                lost_commit_response: committed, resume_commit: true, fired: false };
            destination.apply_observed(&first,&mut crash).await?;
            assert!(crash.fired);
            destination.apply(&second).await?;
            destination.close().await?;
            // Earlier transaction shares mseq: only its exact hashed receipt proves replay.
            let mut restarted = opaque_executor(&f,positions.clone())?;
            restarted.apply(&first).await?; restarted.apply(&second).await?;
            restarted.close().await?;
            assert_eq!(f.count().await?,2);
            let receipt = f.admin.client.query_one("SELECT position_domain, position, octet_length(position_hash) FROM _skedaddle.apply_receipts_v3 WHERE stream=$1 AND txn=$2", &[&f.id,&first.progress.txn.0]).await?;
            assert_eq!(receipt.get::<_,String>(0),first.progress.position.order_domain());
            assert_eq!(receipt.get::<_,Vec<u8>>(1),first.progress.position.bytes());
            assert_eq!(receipt.get::<_,i32>(2),32);
            let progress = f.admin.client.query_one("SELECT position_domain,last_position FROM _skedaddle.apply_progress_v3 WHERE stream=$1", &[&f.id]).await?;
            assert_eq!(progress.get::<_,String>(0),second.progress.position.order_domain());
            assert_eq!(progress.get::<_,Vec<u8>>(1),second.progress.position.bytes());
            // Negative control proves WAL was consumed: only the untagged INSERT survives.
            f.admin.client.batch_execute(&format!("INSERT INTO {}.rows VALUES(999,'untagged')",f.id)).await?;
            let count: i64 = f.admin.client.query_one("SELECT count(*) FROM pg_logical_slot_get_binary_changes($1,NULL,NULL,'proto_version','1','publication_names',$2,'origin','none') WHERE get_byte(data,0)=73", &[&slot,&publication]).await?.get(0);
            assert_eq!(count,1);
            // A forced hash match with different complete bytes is a typed hard error.
            f.admin.client.execute("UPDATE _skedaddle.apply_receipts_v3 SET position=$2 WHERE stream=$1 AND txn=$3", &[&f.id,&second.progress.position.bytes(),&first.progress.txn.0]).await?;
            let mut collision = opaque_executor(&f,positions.clone())?;
            assert!(matches!(collision.apply(&first).await,Err(executor::Error::Apply(ApplyError::PositionHashCollision))));
            collision.close().await?;
            f.admin.client.execute("UPDATE _skedaddle.apply_receipts_v3 SET position=$2,position_domain='wrong' WHERE stream=$1 AND txn=$3", &[&f.id,&first.progress.position.bytes(),&first.progress.txn.0]).await?;
            let mut collision = opaque_executor(&f,positions.clone())?;
            assert!(matches!(collision.apply(&first).await,Err(executor::Error::Apply(ApplyError::PositionHashCollision))));
            collision.close().await?;
            f.admin.client.execute("UPDATE _skedaddle.apply_receipts_v3 SET position_domain=$2 WHERE stream=$1", &[&f.id,&first.progress.position.order_domain()]).await?;
            // Native streams cannot consume v3 evidence, even if progress is absent.
            for receipt_only in [false,true] {
                if receipt_only { f.admin.client.execute("DELETE FROM _skedaddle.apply_progress_v3 WHERE stream=$1", &[&f.id]).await?; }
                let mut native = f.executor()?;
                assert!(matches!(native.apply(&f.plan(EventKind::Insert,RangeState::Completed)?).await,Err(executor::Error::Apply(ApplyError::OriginPolicyMismatch))));
                native.close().await?;
            }
            TestResult::Ok(())
        }.await;
        let _ = f.admin.client.query_one("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name=$1", &[&slot]).await;
        let _ = f
            .admin
            .client
            .batch_execute(&format!("DROP PUBLICATION IF EXISTS {publication}"))
            .await;
        let cleanup = f.cleanup().await;
        result?;
        cleanup?;
    }
    Ok(())
}

#[tokio::test]
#[ignore = "REAL-PG APPLY SKIPPED: Docker harness must supply pg_apply; run --ignored --nocapture"]
async fn opaque_rejects_native_stream_rows_and_wrong_saved_domain() -> TestResult<()> {
    let Some(mut f) = Fixture::open().await? else {
        return Ok(());
    };
    let result = async {
        executor::install_opaque_progress(&mut f.admin.client, |_| async { Ok(()) }).await?;
        let native_plan = f.plan(EventKind::Insert, RangeState::Completed)?;
        let mut native = f.executor()?;
        native.apply(&native_plan).await?;
        native.close().await?;
        let (positions, a, _) = mysql_positions(false)?;
        let plan = f.plan_with_positions(
            &opaque_txn(&f, a, 2)?,
            1,
            RangeState::Completed,
            positions.clone(),
        )?;
        for receipt_only in [false, true] {
            if receipt_only {
                f.admin
                    .client
                    .execute(
                        "DELETE FROM _skedaddle.apply_progress WHERE stream=$1",
                        &[&f.id],
                    )
                    .await?;
            }
            let mut opaque = opaque_executor(&f, positions.clone())?;
            assert!(matches!(
                opaque.apply(&plan).await,
                Err(executor::Error::Apply(ApplyError::OriginPolicyMismatch))
            ));
            opaque.close().await?;
        }
        f.admin
            .client
            .execute(
                "DELETE FROM _skedaddle.apply_receipts WHERE stream=$1",
                &[&f.id],
            )
            .await?;
        let mut opaque = opaque_executor(&f, positions.clone())?;
        opaque.apply(&plan).await?;
        opaque.close().await?;
        f.admin
            .client
            .execute(
                "UPDATE _skedaddle.apply_progress_v3 SET position_domain='wrong' WHERE stream=$1",
                &[&f.id],
            )
            .await?;
        let mut opaque = opaque_executor(&f, positions)?;
        assert!(matches!(
            opaque.apply(&plan).await,
            Err(executor::Error::Apply(ApplyError::Position(
                skedaddle::event::EventError::PositionDomainMismatch
            )))
        ));
        opaque.close().await?;
        TestResult::Ok(())
    }
    .await;
    let cleanup = f.cleanup().await;
    result?;
    cleanup
}

#[tokio::test]
#[ignore = "REAL-PG APPLY SKIPPED: Docker harness must supply pg_apply; run --ignored --nocapture"]
async fn opaque_installer_rejects_actual_schema_mismatch() -> TestResult<()> {
    let Some(f) = Fixture::open().await? else {
        return Ok(());
    };
    // Isolate deliberately malformed schemas from every other fixture.
    let database = format!("{}_schema", f.id);
    f.admin
        .client
        .batch_execute(&format!("CREATE DATABASE {database}"))
        .await?;
    let result = async {
        let mut config: tokio_postgres::Config = f.dsn.parse()?; config.dbname(&database);
        let (mut client,driver,_) = skedaddle::pg::tls::connect(&tls_config(config), &skedaddle::security::TlsPolicy::Required).await?;
        let driver = tokio::spawn(async move { let _ = driver.await; });
        let result = async {
            client.batch_execute("CREATE SCHEMA _skedaddle; CREATE TABLE _skedaddle.apply_progress_v3 (stream text PRIMARY KEY,last_position text)").await?;
            assert!(matches!(executor::install_opaque_progress(&mut client, |_| async { Ok(()) }).await,Err(executor::Error::Apply(ApplyError::SchemaMismatch))));
            client.batch_execute("DROP TABLE _skedaddle.apply_progress_v3").await?;
            executor::install_opaque_progress(&mut client, |_| async { Ok(()) }).await?;
            for (break_schema,restore) in [
                ("ALTER TABLE _skedaddle.apply_progress_v3 ALTER COLUMN position_domain DROP NOT NULL", "ALTER TABLE _skedaddle.apply_progress_v3 ALTER COLUMN position_domain SET NOT NULL"),
                ("ALTER TABLE _skedaddle.apply_progress_v3 ALTER COLUMN last_position TYPE text USING encode(last_position,'hex')", "ALTER TABLE _skedaddle.apply_progress_v3 ALTER COLUMN last_position TYPE bytea USING decode(last_position,'hex')"),
                ("ALTER TABLE _skedaddle.apply_receipts_v3 ADD CONSTRAINT wrong_width CHECK(octet_length(position)=8)", "ALTER TABLE _skedaddle.apply_receipts_v3 DROP CONSTRAINT wrong_width"),
                ("ALTER TABLE _skedaddle.apply_receipts_v3 DROP CONSTRAINT apply_receipts_v3_pkey", "ALTER TABLE _skedaddle.apply_receipts_v3 ADD PRIMARY KEY(stream,mseq,position_hash)"),
            ] {
                client.batch_execute(break_schema).await?;
                assert!(matches!(executor::install_opaque_progress(&mut client, |_| async { Ok(()) }).await,Err(executor::Error::Apply(ApplyError::SchemaMismatch))));
                client.batch_execute(restore).await?;
            }
            executor::install_opaque_progress(&mut client, |_| async { Ok(()) }).await?;
            TestResult::Ok(())
        }.await;
        drop(client); let _ = driver.await; result
    }.await;
    let dropped = f
        .admin
        .client
        .batch_execute(&format!("DROP DATABASE {database} WITH (FORCE)"))
        .await;
    let cleanup = f.cleanup().await;
    result?;
    dropped?;
    cleanup
}

#[tokio::test]
#[ignore = "REAL-PG APPLY SKIPPED: Docker harness must supply pg_apply; run --ignored --nocapture"]
async fn opaque_equal_h_reader_evidence_and_checkpoint_retention() -> TestResult<()> {
    use skedaddle::{
        buffer::{
            format::SegmentRecord,
            reader::{BufferReader, ReaderCheckpoint},
        },
        mysql::MysqlOrder,
        pg::apply::executor::ReaderLoop,
    };
    let Some(mut f) = Fixture::open().await? else {
        return Ok(());
    };
    let result = async {
        executor::install_opaque_progress(&mut f.admin.client, |_| async { Ok(()) }).await?;
        // Native file cursors exercise the local backend's hashed component
        // sidecars. Long GTID logical keys exceed its separate 4096-byte cap;
        // their destination receipt keys are qualified in the campaign above.
        let (positions, h, _) = mysql_positions(false)?;
        let order = Arc::new(MysqlOrder::file("11111111-1111-1111-1111-111111111111")?);
        let transaction = opaque_txn(&f, h.clone(), 1)?;
        let mut rows = opaque_txn(&f, h.clone(), 1)?;
        rows.txn = TxnId(vec![2]);
        rows.events[0].txn = rows.txn.clone();
        rows.events[0].kind = EventKind::ChunkRows;
        let rows = AssembledTxn::new(rows.source, rows.txn, h, rows.commit_time, rows.events)?;
        let (store, config) = pair_buffer_records(
            &f,
            vec![
                SegmentRecord::Transaction(transaction),
                SegmentRecord::ChunkRows(rows),
            ],
        )
        .await?;
        let reader = BufferReader::new(
            store.clone(),
            config.clone(),
            ReaderCheckpoint::default(),
            order.clone(),
        )?;
        let mut runner = ReaderLoop::new(reader, f.id.clone());
        let mut destination = RetainedExecutor {
            executor: opaque_executor(&f, positions.clone())?,
            crash: None,
            plans: vec![],
        };
        let mut saved = ReaderCheckpoint::default();
        for _ in 0..2 {
            assert!(
                runner
                    .step(
                        &mut destination,
                        |txn, mseq| f
                            .plan_with_positions(txn, mseq, RangeState::Copying, positions.clone())
                            .map_err(|_| ApplyError::InvalidTransaction),
                        |cp| {
                            saved = cp;
                            async { Ok(()) }
                        }
                    )
                    .await?
            );
        }
        assert!(destination.plans.iter().all(|plan| plan.progress.evidence.is_some()));
        let kinds: Vec<i16> = f.admin.client.query(
            "SELECT record_kind FROM _skedaddle.apply_receipts_v3 WHERE stream=$1 ORDER BY mseq", &[&f.id],
        ).await?.iter().map(|row| row.get(0)).collect();
        assert_eq!(kinds, [1, 2]);
        destination.executor.apply(&destination.plans[0]).await?;
        destination.executor.apply(&destination.plans[1]).await?;
        assert_eq!(f.count().await?, 1);
        assert_eq!(saved.next_mseq, 2);
        destination.executor.retain_after_checkpoint(&saved).await?;
        let receipts: i64 = f
            .admin
            .client
            .query_one(
                "SELECT count(*) FROM _skedaddle.apply_receipts_v3 WHERE stream=$1",
                &[&f.id],
            )
            .await?
            .get(0);
        assert_eq!(receipts, 0);
        let reader = BufferReader::new(store, config, saved, order)?;
        let mut restarted = ReaderLoop::new(reader, f.id.clone());
        assert!(
            !restarted
                .step(
                    &mut destination,
                    |_, _| panic!("idle checkpoint"),
                    |_| async { Ok(()) }
                )
                .await?
        );
        destination.executor.close().await?;
        TestResult::Ok(())
    }
    .await;
    let cleanup = f.cleanup().await;
    result?;
    cleanup
}
