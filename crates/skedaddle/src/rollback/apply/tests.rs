use super::*;
use super::{Error, Result};
use crate::{
    buffer::{backends::*, format::*},
    coordinator::{self, Transition, journal::Entry},
    event::*,
    pg::apply::CommitDecision,
};
use async_trait::async_trait;
use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    sync::Mutex,
};
#[derive(Default)]
struct Memory(Mutex<BTreeMap<String, Vec<u8>>>);
#[async_trait]
impl BufferStore for Memory {
    fn report(&self) -> BackendReport {
        panic!("no backend inspection")
    }
    async fn get(&self, k: &str) -> crate::buffer::backends::Result<Option<Vec<u8>>> {
        Ok(self.0.lock().unwrap().get(k).cloned())
    }
    async fn put_create_only(
        &self,
        _: &str,
        _: &[u8],
    ) -> crate::buffer::backends::Result<PutOutcome> {
        unreachable!()
    }
    async fn delete(&self, _: &str) -> crate::buffer::backends::Result<()> {
        unreachable!()
    }
    async fn list_prefix(
        &self,
        _: &str,
        _: Option<&str>,
        _: usize,
    ) -> crate::buffer::backends::Result<ListPage> {
        unreachable!()
    }
}
struct Order;
impl PositionOrder for Order {
    fn domain(&self) -> &str {
        "m_rev"
    }
    fn compare(&self, a: &[u8], b: &[u8]) -> std::result::Result<Ordering, EventError> {
        Ok(a.cmp(b))
    }
}
fn txn(n: u64, marker: bool) -> AssembledTxn {
    let source = SourceIdentity {
        engine: "postgres".into(),
        system_id: "1".into(),
        lineage: "1".into(),
        db_name: "d".into(),
        stream: "s".into(),
    };
    let position = Position::new("m_rev", n.to_be_bytes());
    let events = if marker {
        vec![Event{source:source.clone(),txn:TxnId(vec![n as u8]),ordinal_in_txn:0,position:position.clone(),schema_version:0,kind:EventKind::Watermark,table:None,key:Default::default(),before:None,after:Some(BTreeMap::from([("control".into(),OpaqueValue{type_id:"skedaddle.watermark.v1".into(),bytes:Some(serde_json::to_vec(&serde_json::json!({"message_lsn":25,"content":br#"{"final":"r","epoch":1}"#})).unwrap())})])),unchanged_cols:Default::default()}]
    } else {
        vec![]
    };
    AssembledTxn::new(source, TxnId(vec![n as u8]), position, Some(1), events).unwrap()
}
fn replay(marker: bool, beyond: bool) -> Replay {
    let mut txns = vec![txn(10, false), txn(20, false)];
    if marker {
        txns.push(txn(30, true));
    }
    if beyond {
        txns.push(txn(40, false));
    }
    replay_txns("m", txns)
}
fn replay_txns(id: &str, txns: Vec<AssembledTxn>) -> Replay {
    let store = Arc::new(Memory::default());
    let stream = format!("{id}_rev");
    let first = txns.first().unwrap().commit_position.clone();
    let last = txns.last().unwrap().commit_position.clone();
    let mut encoder = SegmentEncoder::new(
        Vec::new(),
        SegmentHeader {
            migration_id: id.into(),
            stream: stream.clone(),
            epoch: 1,
            first_pos: first.clone(),
            last_pos: last.clone(),
            record_count: txns.len() as u64,
        },
        Limits::default(),
    )
    .unwrap();
    for t in &txns {
        encoder
            .write_record(&SegmentRecord::Transaction(t.clone()))
            .unwrap();
    }
    let (bytes, summary) = encoder.finish().unwrap();
    let entry = ManifestEntry {
        mseq: 0,
        epoch: 1,
        prev_digest: [0; 32],
        segment_key: segment_key("reverse-runner-v1", id, &stream, &first, &last, 1),
        segment_digest: summary.digest,
        first_pos: first,
        last_pos: last,
        txn_count: txns.len() as u64,
        bytes: summary.bytes,
    };
    store
        .0
        .lock()
        .unwrap()
        .insert(entry.segment_key.clone(), bytes);
    store.0.lock().unwrap().insert(
        manifest_key("reverse-runner-v1", id, &stream, 0),
        entry.canonical_json().unwrap(),
    );
    Replay {
        store,
        order: Arc::new(Order),
        required_next_mseq: 1,
        tables: vec![],
    }
}
struct Log {
    state: Materialized,
    saved: (ReaderCheckpoint, bool),
    fail: Option<usize>,
    calls: usize,
    trace: Arc<Mutex<Vec<String>>>,
}
impl Log {
    fn record(&mut self, r: Record) -> Result<()> {
        let e = Entry::new(
            &self.state,
            Transition::Rollback {
                record: Box::new(r),
            },
            "owner",
        )
        .unwrap();
        self.state
            .apply(&e)
            .map_err(|_| Error::OperatorAction("journal rejected"))
    }
    fn boundary(&mut self) -> Result<()> {
        self.calls += 1;
        if self.fail == Some(self.calls) {
            self.fail = None;
            Err(Error::Retry)
        } else {
            Ok(())
        }
    }
}
impl Journal for Log {
    fn state(&self) -> Materialized {
        self.state.clone()
    }
    fn publish(&mut self, r: Record) -> Result<()> {
        self.boundary()?;
        self.trace.lock().unwrap().push(format!("{r:?}"));
        self.record(r)?;
        self.boundary()
    }
    fn progress(&self, _: &str) -> Result<(ReaderCheckpoint, bool)> {
        Ok(self.saved.clone())
    }
    fn save(&mut self, _: &str, c: ReaderCheckpoint, reached: bool) -> Result<()> {
        self.boundary()?;
        self.saved = (c, reached);
        self.boundary()
    }
}
struct Target {
    trace: Arc<Mutex<Vec<String>>>,
    rows: BTreeSet<u64>,
    fail: Option<u64>,
}
#[async_trait]
impl Destination for Target {
    async fn apply(&mut self, p: &apply::TransactionPlan) -> executor::Result<CommitDecision> {
        self.trace.lock().unwrap().push(format!(
            "apply:{}",
            u64::from_be_bytes(p.progress.position.bytes().try_into().unwrap())
        ));
        assert_eq!(
            p.session_setup.parameters[0].bytes(),
            Some(b"skedaddle_m_rev".as_slice())
        );
        self.rows.insert(u64::from_be_bytes(
            p.progress.position.bytes().try_into().unwrap(),
        ));
        if self.fail
            == Some(u64::from_be_bytes(
                p.progress.position.bytes().try_into().unwrap(),
            ))
        {
            self.fail = None;
            return Err(executor::Error::Database);
        }
        Ok(CommitDecision::Applied)
    }
    async fn retain_after_checkpoint(&mut self, _: &ReaderCheckpoint) -> executor::Result<()> {
        Ok(())
    }
}
struct Verify {
    mismatch: bool,
    calls: usize,
}
impl Verification for Verify {
    async fn verify(&mut self) -> Result<()> {
        self.calls += 1;
        if self.mismatch {
            Err(Error::OperatorAction("mismatch"))
        } else {
            Ok(())
        }
    }
}
fn fixture() -> (Key, Log, Target, Verify) {
    let trace = Arc::new(Mutex::new(vec![]));
    let mut state = Materialized {
        epoch: 1,
        owner: "owner".into(),
        ..Default::default()
    };
    state.lifecycle.state = State::RollbackWindow;
    let mut log = Log {
        state,
        saved: (Default::default(), false),
        fail: None,
        calls: 0,
        trace: trace.clone(),
    };
    log.record(Record::Admit {
        request: "r".into(),
        deadline_at_ms: u64::MAX,
    })
    .unwrap();
    for step in [
        Step::PreHook,
        Step::FenceDestination,
        Step::Drain,
        Step::FinalMarker,
    ] {
        log.record(Record::Intent { step }).unwrap();
        log.record(Record::Checkpoint {
            step,
            final_marker: (step == Step::FinalMarker).then_some(25),
        })
        .unwrap();
    }
    (
        Key {
            migration: "m".into(),
            cutover: "c".into(),
            epoch: 1,
            coordinator: "owner".into(),
        },
        log,
        Target {
            trace,
            rows: Default::default(),
            fail: None,
        },
        Verify {
            mismatch: false,
            calls: 0,
        },
    )
}
#[tokio::test]
async fn intent_precedes_source_and_stops_exactly_at_marker() {
    let (k, mut j, mut s, mut v) = fixture();
    assert_eq!(
        execute_next(&k, &replay(true, true), &mut s, &mut v, &mut j)
            .await
            .unwrap(),
        Outcome::Applied
    );
    assert_eq!(s.rows, BTreeSet::from([10, 20, 30]));
    assert!(j.trace.lock().unwrap()[0].contains("ReverseApply"));
    assert!(j.state.rollback.as_ref().unwrap().point_of_no_return);
    assert_eq!(
        execute_next(&k, &replay(true, true), &mut s, &mut v, &mut j)
            .await
            .unwrap(),
        Outcome::Verified
    );
    assert_eq!(v.calls, 1);
}
#[tokio::test]
async fn every_journal_boundary_recovers_and_apply_lost_reply_is_idempotent() {
    for fail in 1..=12 {
        let (k, mut j, mut s, mut v) = fixture();
        j.fail = Some(fail);
        s.fail = Some(20);
        for _ in 0..6 {
            if j.state.rollback.as_ref().unwrap().next == Step::Verify {
                break;
            }
            let _ = execute_next(&k, &replay(true, true), &mut s, &mut v, &mut j).await;
        }
        assert_eq!(
            j.state.rollback.as_ref().unwrap().next,
            Step::Verify,
            "boundary {fail}"
        );
        assert_eq!(s.rows, BTreeSet::from([10, 20, 30]));
    }
}
#[tokio::test]
async fn missing_marker_and_mismatch_keep_both_fences() {
    let (k, mut j, mut s, mut v) = fixture();
    assert!(matches!(
        execute_next(&k, &replay(false, false), &mut s, &mut v, &mut j).await,
        Err(Error::OperatorAction(_))
    ));
    assert_eq!(j.state.lifecycle.authority, coordinator::Authority::Neither);
    let (k, mut j, mut s, mut v) = fixture();
    execute_next(&k, &replay(true, true), &mut s, &mut v, &mut j)
        .await
        .unwrap();
    v.mismatch = true;
    assert_eq!(
        execute_next(&k, &replay(true, true), &mut s, &mut v, &mut j)
            .await
            .unwrap_err()
            .exit_code(),
        5
    );
    assert_eq!(j.state.rollback.as_ref().unwrap().next, Step::Verify);
    assert_eq!(j.state.lifecycle.authority, coordinator::Authority::Neither);
    v.mismatch = false;
    execute_next(&k, &replay(true, true), &mut s, &mut v, &mut j)
        .await
        .unwrap();
    assert_eq!(v.calls, 2);
}
#[test]
fn marker_identity_and_commit_boundary_are_distinct() {
    assert!(is_final(&txn(30, true), "r", 25, 1).unwrap());
    assert!(is_final(&txn(30, true), "other", 25, 1).is_err());
    assert!(is_final(&txn(30, false), "r", 25, 1).is_err());
}

struct Rows(Vec<verify::Row>);
impl verify::KeyOrder for Rows {
    fn compare_keys(&self, a: &verify::Key, b: &verify::Key) -> verify::Result<Ordering> {
        match (&a[0], &b[0]) {
            (crate::mapping::Value::Int8(a), crate::mapping::Value::Int8(b)) => Ok(a.cmp(b)),
            (crate::mapping::Value::Int4(a), crate::mapping::Value::Int4(b)) => Ok(a.cmp(b)),
            _ => unreachable!(),
        }
    }
}
#[async_trait]
impl RowSource for Rows {
    async fn fetch(&mut self, _: &RangePlan, _: &Projection) -> verify::Result<Vec<verify::Row>> {
        Ok(self.0.clone())
    }
}
#[tokio::test]
async fn typed_swapped_verifier_passes_and_rejects_mismatch() {
    let row = verify::Row {
        key: vec![crate::mapping::Value::Int8(1)],
        columns: BTreeMap::new(),
    };
    let mut tables = vec![TableVerification {
        destination: Rows(vec![row.clone()]),
        source: Rows(vec![row]),
        range: RangePlan {
            id: crate::pg::chunk::ChunkId {
                table: "t".into(),
                plan: "p".into(),
                ordinal: 0,
            },
            lo: None,
            hi: None,
        },
        projection: Projection::new(vec![]).unwrap(),
    }];
    tables.verify().await.unwrap();
    tables[0].source.0.clear();
    assert!(matches!(
        tables.verify().await,
        Err(Error::OperatorAction(_))
    ));
}
#[tokio::test]
async fn verification_checkpoint_lost_reply_reruns_safely() {
    let (k, mut j, mut s, mut v) = fixture();
    let r = replay(true, true);
    execute_next(&k, &r, &mut s, &mut v, &mut j).await.unwrap();
    j.fail = Some(j.calls + 3);
    assert!(execute_next(&k, &r, &mut s, &mut v, &mut j).await.is_err());
    assert_eq!(j.state.rollback.as_ref().unwrap().next, Step::Verify);
    execute_next(&k, &r, &mut s, &mut v, &mut j).await.unwrap();
    assert_eq!(v.calls, 2);
}

struct PgConnector(crate::pg::tls::Config);
#[async_trait]
impl executor::Connector for PgConnector {
    async fn connect(&mut self) -> executor::Result<tokio_postgres::Client> {
        let (client, driver, _) =
            crate::pg::tls::connect(&self.0, &crate::security::TlsPolicy::Required)
                .await
                .map_err(|_| executor::Error::Database)?;
        tokio::spawn(async move {
            let _ = driver.await;
        });
        Ok(client)
    }
}
struct PgRows {
    client: tokio_postgres::Client,
    table: String,
}
impl verify::KeyOrder for PgRows {
    fn compare_keys(&self, a: &verify::Key, b: &verify::Key) -> verify::Result<Ordering> {
        Rows(vec![]).compare_keys(a, b)
    }
}
#[async_trait]
impl RowSource for PgRows {
    async fn fetch(&mut self, _: &RangePlan, _: &Projection) -> verify::Result<Vec<verify::Row>> {
        self.client
            .query(
                &format!("SELECT id FROM public.{} ORDER BY id", self.table),
                &[],
            )
            .await
            .map_err(|_| verify::VerifyError::Read("PG test read".into()))
            .map(|rows| {
                rows.iter()
                    .map(|r| verify::Row {
                        key: vec![crate::mapping::Value::Int4(r.get(0))],
                        columns: BTreeMap::new(),
                    })
                    .collect()
            })
    }
}
#[tokio::test]
async fn rollback_preserves_destination_writes() {
    use executor::Connector;
    use std::io::Write;
    let vars = [
        "SKEDADDLE_ROLLBACK_PG_SOURCE_URL",
        "SKEDADDLE_ROLLBACK_PG_DESTINATION_URL",
        "SKEDADDLE_TEST_CONFIG",
    ];
    let values: Vec<_> = vars
        .iter()
        .map(|v| std::env::var(v).ok().filter(|v| !v.is_empty()))
        .collect();
    if values.iter().any(Option::is_none) {
        std::io::stderr().write_all(b"SKIP rollback::apply::rollback_preserves_destination_writes: set SKEDADDLE_ROLLBACK_PG_SOURCE_URL, SKEDADDLE_ROLLBACK_PG_DESTINATION_URL, SKEDADDLE_TEST_CONFIG (disposable databases and TLS CA)\n").unwrap();
        return;
    }
    let config: serde_json::Value =
        serde_json::from_slice(&std::fs::read(values[2].as_ref().unwrap()).unwrap()).unwrap();
    let ca = config["ca_pem"]
        .as_str()
        .or_else(|| config["replication"]["ca_pem"].as_str())
        .unwrap();
    let sc = crate::pg::tls::Config::new(values[0].as_ref().unwrap().parse().unwrap(), ca);
    let dc = crate::pg::tls::Config::new(values[1].as_ref().unwrap().parse().unwrap(), ca);
    let mut src = PgConnector(sc.clone()).connect().await.unwrap();
    let dst = PgConnector(dc).connect().await.unwrap();
    let id = format!("rollback_slice2_{}", uuid::Uuid::new_v4().simple());
    let origin = format!("skedaddle_{id}_rev");
    let stream = format!("{id}_rev");
    let table = id.clone();
    for client in [&src, &dst] {
        client
            .batch_execute(&format!("CREATE TABLE public.{table} (id int PRIMARY KEY)"))
            .await
            .unwrap();
    }
    dst.batch_execute(&format!("INSERT INTO public.{table} VALUES (7)"))
        .await
        .unwrap();
    // Setup is its own previously journaled reverse-installation effect in production.
    executor::install_progress(&mut src, |_| std::future::ready(Ok(())))
        .await
        .unwrap();
    src.query_one("SELECT pg_replication_origin_create($1)", &[&origin])
        .await
        .unwrap();
    let mut t = txn(10, false);
    let value = OpaqueValue {
        type_id: "int4".into(),
        bytes: Some(b"7".to_vec()),
    };
    let event = Event {
        source: t.source.clone(),
        txn: t.txn.clone(),
        ordinal_in_txn: 0,
        position: t.commit_position.clone(),
        schema_version: 0,
        kind: EventKind::Insert,
        table: Some("t".into()),
        key: BTreeMap::from([("id".into(), value.clone())]),
        before: None,
        after: Some(BTreeMap::from([("id".into(), value)])),
        unchanged_cols: Default::default(),
    };
    t = AssembledTxn::new(t.source, t.txn, t.commit_position, Some(1), vec![event]).unwrap();
    let mut replay = replay_txns(&id, vec![t, txn(30, true), txn(40, false)]);
    replay.tables = vec![apply::Table {
        event_table: "t".into(),
        schema: "public".into(),
        name: table.clone(),
        schema_version: 0,
        columns: vec![apply::Column {
            name: "id".into(),
            type_id: "int4".into(),
            pg_type: crate::mapping::PgType::Int4,
            wire_type: tokio_postgres::types::Type::INT4,
        }],
        key: vec!["id".into()],
    }];
    let (mut key, mut journal, _, _) = fixture();
    key.migration = id;
    let mut executor = executor::Executor::new(
        PgConnector(sc),
        stream.clone(),
        origin.clone(),
        100,
        apply::SourcePositions::new(replay.order.clone(), apply::OriginPolicy::NativePgLsn),
    )
    .unwrap();
    let mut verification = vec![TableVerification {
        destination: PgRows {
            client: dst,
            table: table.clone(),
        },
        source: PgRows {
            client: src,
            table: table.clone(),
        },
        range: RangePlan {
            id: crate::pg::chunk::ChunkId {
                table: table.clone(),
                plan: "whole".into(),
                ordinal: 0,
            },
            lo: None,
            hi: None,
        },
        projection: Projection::new(vec![]).unwrap(),
    }];
    execute_next(
        &key,
        &replay,
        &mut executor,
        &mut verification,
        &mut journal,
    )
    .await
    .unwrap();
    execute_next(
        &key,
        &replay,
        &mut executor,
        &mut verification,
        &mut journal,
    )
    .await
    .unwrap();
    executor.close().await.unwrap();
    let source = &verification[0].source.client;
    source
        .execute(
            "DELETE FROM _skedaddle.apply_receipts WHERE stream=$1",
            &[&stream],
        )
        .await
        .unwrap();
    source
        .execute(
            "DELETE FROM _skedaddle.apply_progress WHERE stream=$1",
            &[&stream],
        )
        .await
        .unwrap();
    source
        .query_one("SELECT pg_replication_origin_drop($1)", &[&origin])
        .await
        .unwrap();
    for client in [
        &verification[0].source.client,
        &verification[0].destination.client,
    ] {
        client
            .batch_execute(&format!("DROP TABLE public.{table}"))
            .await
            .unwrap();
    }
}

// This test sink models only commit/checkpoint acknowledgment faults; the
// unchanged PG harness test above qualifies actual database effects.
struct TestSink {
    trace: Arc<Mutex<Vec<String>>>,
    receipts: BTreeSet<u64>,
    fail_after_commit: bool,
}
#[async_trait]
impl RollbackSink for TestSink {
    async fn apply(&mut self, txn: &AssembledTxn, context: &ApplyContext) -> Result<SinkOutcome> {
        assert_eq!(context.mseq, 0);
        let n = u64::from_be_bytes(txn.commit_position.bytes().try_into().unwrap());
        self.trace.lock().unwrap().push(format!("sink:{n}"));
        let fresh = self.receipts.insert(n);
        if self.fail_after_commit {
            self.fail_after_commit = false;
            return Err(Error::Retry);
        }
        Ok(if fresh {
            SinkOutcome::Committed
        } else {
            SinkOutcome::AlreadyCommitted
        })
    }
}
struct TestJournal(Log);
impl Journal for TestJournal {
    fn state(&self) -> Materialized {
        self.0.state()
    }
    fn publish(&mut self, r: Record) -> Result<()> {
        self.0.publish(r)
    }
    fn progress(&self, request: &str) -> Result<(ReaderCheckpoint, bool)> {
        self.0.progress(request)
    }
    fn save(&mut self, request: &str, checkpoint: ReaderCheckpoint, reached: bool) -> Result<()> {
        self.0.trace.lock().unwrap().push(format!("save:{reached}"));
        self.0.save(request, checkpoint, reached)
    }
}
#[tokio::test]
async fn neutral_sink_commits_in_order_before_request_saves() {
    let (k, log, _, mut v) = fixture();
    let trace = log.trace.clone();
    let mut j = TestJournal(log);
    let mut sink = TestSink {
        trace: trace.clone(),
        receipts: BTreeSet::new(),
        fail_after_commit: false,
    };
    execute_next_with_sink(&k, &replay(true, true), &mut sink, &mut v, &mut j)
        .await
        .unwrap();
    let trace = trace.lock().unwrap();
    assert!(trace[0].contains("ReverseApply"));
    assert_eq!(
        &trace[1..7],
        [
            "sink:10",
            "save:false",
            "sink:20",
            "save:false",
            "sink:30",
            "save:true"
        ]
    );
    assert_eq!(sink.receipts, BTreeSet::from([10, 20, 30]));
    assert!(j.0.saved.1);
}
#[tokio::test]
async fn neutral_sink_replays_lost_checkpoint_and_lost_commit_reply() {
    for lost_commit in [false, true] {
        let (k, mut log, _, mut v) = fixture();
        if !lost_commit {
            log.fail = Some(3);
        } // first save fails before durability
        let trace = log.trace.clone();
        let mut j = TestJournal(log);
        let mut sink = TestSink {
            trace: trace.clone(),
            receipts: BTreeSet::new(),
            fail_after_commit: lost_commit,
        };
        assert!(matches!(
            execute_next_with_sink(&k, &replay(true, true), &mut sink, &mut v, &mut j).await,
            Err(Error::Retry)
        ));
        assert_eq!(j.0.saved, (ReaderCheckpoint::default(), false));
        if lost_commit {
            assert!(!trace.lock().unwrap().iter().any(|s| s.starts_with("save:")));
        }
        execute_next_with_sink(&k, &replay(true, true), &mut sink, &mut v, &mut j)
            .await
            .unwrap();
        let calls: Vec<_> = trace
            .lock()
            .unwrap()
            .iter()
            .filter(|s| s.starts_with("sink:"))
            .cloned()
            .collect();
        assert_eq!(calls, ["sink:10", "sink:10", "sink:20", "sink:30"]);
        assert_eq!(sink.receipts, BTreeSet::from([10, 20, 30]));
    }
}
