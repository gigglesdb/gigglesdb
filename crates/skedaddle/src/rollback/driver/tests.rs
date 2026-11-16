use super::apply::Replay;
use super::*;
use crate::{
    buffer::{backends::*, format::*, reader::ReaderCheckpoint},
    coordinator::{self, Transition, journal::Entry, rollback::Record},
    event::*,
    pg::apply::{
        self as pgapply, CommitDecision,
        executor::{self, Destination},
    },
};
use async_trait::async_trait;
use std::{
    cell::RefCell,
    cmp::Ordering,
    collections::BTreeMap,
    rc::Rc,
    sync::{Arc, Mutex},
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
struct Order(String);
impl PositionOrder for Order {
    fn domain(&self) -> &str {
        &self.0
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
        order: Arc::new(Order(stream)),
        required_next_mseq: 1,
        tables: vec![],
    }
}

#[derive(Clone, Default)]
struct Effects {
    destination_fenced: bool,
    source_active: bool,
    stopped: bool,
    mismatch: bool,
    guarded: bool,
}
type Shared = Rc<RefCell<Effects>>;
struct Guard(Shared, bool);
impl Authority for Guard {
    fn run<T>(&mut self, _: &Key, run: impl FnOnce() -> cutover::Result<T>) -> cutover::Result<T> {
        if self.1 {
            return Err(cutover::Error::LostOwnership);
        }
        assert!(!self.0.borrow().guarded);
        self.0.borrow_mut().guarded = true;
        let result = run();
        self.0.borrow_mut().guarded = false;
        result
    }
}
struct Log {
    state: Materialized,
    saved: (ReaderCheckpoint, bool),
    crash: Option<Step>,
}
impl Log {
    fn publish(&mut self, r: Record) -> cutover::Result<()> {
        let step = match &r {
            Record::Checkpoint { step, .. } => Some(*step),
            _ => None,
        };
        let entry = Entry::new(
            &self.state,
            Transition::Rollback {
                record: Box::new(r),
            },
            &self.state.owner,
        )
        .map_err(|_| cutover::Error::Invalid("entry rejected"))?;
        self.state
            .apply(&entry)
            .map_err(|_| cutover::Error::Invalid("record rejected"))?;
        if self.crash.is_some() && self.crash == step {
            self.crash = None;
            return Err(cutover::Error::Component("lost checkpoint reply".into()));
        }
        Ok(())
    }
}
impl destination::Journal for Log {
    fn state(&self) -> Materialized {
        self.state.clone()
    }
    fn publish(&mut self, r: Record) -> cutover::Result<()> {
        self.publish(r)
    }
    fn native(&mut self, _: &cutover::Record) -> std::result::Result<(), String> {
        Ok(())
    }
}
impl apply::Journal for Log {
    fn state(&self) -> Materialized {
        self.state.clone()
    }
    fn publish(&mut self, r: Record) -> apply::Result<()> {
        self.publish(r).map_err(|_| apply::Error::Retry)
    }
    fn progress(&self, _: &str) -> apply::Result<(ReaderCheckpoint, bool)> {
        Ok(self.saved.clone())
    }
    fn save(&mut self, _: &str, cp: ReaderCheckpoint, reached: bool) -> apply::Result<()> {
        self.saved = (cp, reached);
        Ok(())
    }
}
impl source::Journal for Log {
    fn state(&self) -> Materialized {
        self.state.clone()
    }
    fn publish(&mut self, r: Record) -> apply::Result<()> {
        self.publish(r).map_err(|_| apply::Error::Retry)
    }
    fn native(&mut self, _: &crate::pg::fence::Intent) -> std::result::Result<(), String> {
        Ok(())
    }
}
struct Endpoint(Shared);
impl Endpoint {
    fn check(&self) {
        assert!(self.0.borrow().guarded);
    }
}
impl cutover::SourceFence for Endpoint {
    fn fence(
        &mut self,
        _: &Key,
        _: &mut cutover::Journal<'_>,
    ) -> cutover::Result<cutover::DrainCounts> {
        self.check();
        self.0.borrow_mut().destination_fenced = true;
        Ok(Default::default())
    }
    fn final_marker(&mut self, k: &Key) -> cutover::Result<cutover::Marker> {
        self.check();
        Ok(cutover::Marker {
            cutover: k.cutover.clone(),
            epoch: 1,
            lsn: 25,
        })
    }
    fn unfence(&mut self, _: &Key, _: &mut cutover::Journal<'_>) -> cutover::Result<()> {
        self.check();
        self.0.borrow_mut().destination_fenced = false;
        Ok(())
    }
}
impl destination::Capture for Endpoint {
    fn captured(&mut self, m: &cutover::Marker) -> cutover::Result<Option<cutover::Marker>> {
        self.check();
        Ok(Some(m.clone()))
    }
    fn stop_at(&mut self, _: &cutover::Marker) -> cutover::Result<()> {
        self.check();
        self.0.borrow_mut().stopped = true;
        Ok(())
    }
    fn restart(&mut self, _: &Key) -> cutover::Result<()> {
        self.check();
        self.0.borrow_mut().stopped = false;
        Ok(())
    }
}
impl destination::PreHook for Endpoint {
    fn run(&mut self, _: &Key) -> cutover::Result<()> {
        self.check();
        Ok(())
    }
}
impl apply::Verification for Endpoint {
    async fn verify(&mut self) -> apply::Result<()> {
        self.check();
        if self.0.borrow().mismatch {
            Err(apply::Error::OperatorAction("mismatch"))
        } else {
            Ok(())
        }
    }
}
impl source::Effects for Endpoint {
    fn validate(&self, _: &Key) -> apply::Result<()> {
        self.check();
        Ok(())
    }
    fn sequences(&mut self, _: &str) -> apply::Result<()> {
        self.check();
        Ok(())
    }
    fn restore(&mut self, _: &mut impl source::Journal) -> apply::Result<()> {
        self.check();
        self.0.borrow_mut().source_active = true;
        Ok(())
    }
    fn refence(&mut self, _: &mut impl source::Journal) -> apply::Result<()> {
        self.check();
        self.0.borrow_mut().source_active = false;
        Ok(())
    }
}
impl source::PostHook for Endpoint {
    fn run(&mut self, _: &Key) -> apply::Result<()> {
        self.check();
        Ok(())
    }
}
// async_trait requires a Send target, unlike the synchronous fake adapters.
struct Target(Arc<Mutex<Vec<u64>>>);
#[async_trait]
impl Destination for Target {
    async fn apply(&mut self, p: &pgapply::TransactionPlan) -> executor::Result<CommitDecision> {
        self.0.lock().unwrap().push(u64::from_be_bytes(
            p.progress.position.bytes().try_into().unwrap(),
        ));
        Ok(CommitDecision::Applied)
    }
    async fn retain_after_checkpoint(&mut self, _: &ReaderCheckpoint) -> executor::Result<()> {
        Ok(())
    }
}
struct Fixture {
    key: Key,
    log: Log,
    effects: Shared,
    rows: Arc<Mutex<Vec<u64>>>,
    rt: tokio::runtime::Runtime,
    lost: bool,
}
impl Fixture {
    fn new() -> Self {
        let mut state = Materialized {
            epoch: 1,
            owner: "owner".into(),
            ..Default::default()
        };
        state.lifecycle.state = coordinator::State::RollbackWindow;
        let mut log = Log {
            state,
            saved: (Default::default(), false),
            crash: None,
        };
        log.publish(Record::Admit {
            request: "r".into(),
            deadline_at_ms: u64::MAX,
        })
        .unwrap();
        Self {
            key: Key {
                migration: "m".into(),
                cutover: "forward".into(),
                epoch: 1,
                coordinator: "owner".into(),
            },
            log,
            effects: Default::default(),
            rows: Default::default(),
            rt: tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap(),
            lost: false,
        }
    }
    fn tick(&mut self, expired: bool) -> cutover::Result<Outcome> {
        let e = &self.effects;
        execute_next(
            &self.key,
            if expired { u64::MAX } else { 0 },
            &mut Guard(e.clone(), self.lost),
            &self.rt,
            &mut Components {
                destination: &mut Endpoint(e.clone()),
                capture: &mut Endpoint(e.clone()),
                pre_hook: &mut Endpoint(e.clone()),
                replay: &replay(true, true),
                target: &mut Target(self.rows.clone()),
                verification: &mut Endpoint(e.clone()),
                source: &mut Endpoint(e.clone()),
                post_hook: &mut Endpoint(e.clone()),
                journal: &mut self.log,
            },
        )
    }
    fn finish(&mut self) {
        for _ in 0..30 {
            match self.tick(false) {
                Ok(Outcome::Complete) => return,
                Ok(_) | Err(cutover::Error::Component(_)) => {}
                error => panic!("{error:?}"),
            }
        }
        panic!("rollback did not complete")
    }
    fn assert_complete(&self) {
        assert_eq!(
            self.log.state.lifecycle.state,
            State::SourceActiveAfterRollback
        );
        assert!(self.effects.borrow().destination_fenced);
        assert!(self.effects.borrow().source_active);
        assert!(self.effects.borrow().stopped);
        assert_eq!(*self.rows.lock().unwrap(), vec![10, 20, 30]);
    }
}
#[test]
fn full_happy_path_uses_real_buffer_and_exact_marker() {
    let mut f = Fixture::new();
    f.finish();
    f.assert_complete();
}
#[test]
fn lost_checkpoint_reply_at_every_step_resumes_from_durable_state() {
    for step in [
        Step::PreHook,
        Step::FenceDestination,
        Step::Drain,
        Step::FinalMarker,
        Step::ReverseApply,
        Step::Verify,
        Step::Sequences,
        Step::RestoreSource,
        Step::PostHook,
    ] {
        let mut f = Fixture::new();
        f.log.crash = Some(step);
        // Each tick reconstructs all adapters from the same durable state.
        f.finish();
        f.assert_complete();
        assert!(f.log.crash.is_none());
    }
}
#[test]
fn deadline_before_ponr_restores_destination_and_restarts_capture() {
    for boundary in 0..=4 {
        let mut f = Fixture::new();
        for _ in 0..boundary {
            f.tick(false).unwrap();
        }
        f.log.state.rollback.as_mut().unwrap().deadline_at_ms = 1;
        assert_eq!(f.tick(true).unwrap(), Outcome::DeadlineAborted);
        assert_eq!(f.log.state.lifecycle.state, State::RollbackWindow);
        assert!(!f.effects.borrow().destination_fenced);
        assert!(!f.effects.borrow().stopped);
        assert!(!f.effects.borrow().source_active);
        assert!(f.rows.lock().unwrap().is_empty());
    }
}
#[test]
fn mismatch_after_ponr_keeps_both_fences() {
    let mut f = Fixture::new();
    for _ in 0..5 {
        f.tick(false).unwrap();
    }
    f.effects.borrow_mut().mismatch = true;
    assert!(matches!(
        f.tick(true),
        Err(cutover::Error::Invalid("mismatch"))
    ));
    assert!(f.effects.borrow().destination_fenced);
    assert!(!f.effects.borrow().source_active);
    assert!(f.log.state.rollback.as_ref().unwrap().point_of_no_return);
}
#[test]
fn lost_ownership_has_no_effects_and_next_owner_resumes() {
    for boundary in 0..9 {
        let mut f = Fixture::new();
        for _ in 0..boundary {
            f.tick(false).unwrap();
        }
        f.lost = true;
        let before = serde_json::to_vec(&f.log.state).unwrap();
        assert!(matches!(f.tick(false), Err(cutover::Error::LostOwnership)));
        assert_eq!(before, serde_json::to_vec(&f.log.state).unwrap());
        f.lost = false;
        f.key.epoch = 2;
        f.key.coordinator = "next".into();
        f.log.state.epoch = 2;
        f.log.state.owner = "next".into();
        f.finish();
        f.assert_complete();
    }
}

struct OwnerCapture {
    endpoint: Endpoint,
    runtime: crate::engine::Runtime,
    digest: [u8; 32],
}
impl destination::Capture for OwnerCapture {
    fn captured(&mut self, marker: &cutover::Marker) -> cutover::Result<Option<cutover::Marker>> {
        self.endpoint.captured(marker)
    }
    fn stop_at(&mut self, marker: &cutover::Marker) -> cutover::Result<()> {
        super::bridge(self.runtime.commit(Transition::Rollback {
            record: Box::new(Record::CaptureStopped {
                boundary: crate::pg::reverse::StopBoundary {
                    marker: marker.clone(),
                    durable_lsn: 30,
                    next_mseq: 1,
                    digest: self.digest,
                },
            }),
        }))?;
        self.endpoint.stop_at(marker)
    }
    fn restart(&mut self, key: &Key) -> cutover::Result<()> {
        self.endpoint.restart(key)
    }
}

/// Fake endpoints and real buffers, run through the actual owner journal bridge.
#[derive(Clone, Default)]
pub(crate) struct OwnerModel {
    effects: Arc<Mutex<Effects>>,
    rows: Arc<Mutex<Vec<u64>>>,
}
impl OwnerModel {
    pub(crate) fn assert_complete(&self) {
        let e = self.effects.lock().unwrap();
        assert!(e.destination_fenced && e.source_active && e.stopped);
        assert_eq!(*self.rows.lock().unwrap(), vec![10, 20, 30]);
    }
    pub(crate) fn assert_aborted(&self) {
        let e = self.effects.lock().unwrap();
        assert!(!e.destination_fenced && !e.source_active && !e.stopped);
        assert!(self.rows.lock().unwrap().is_empty());
    }
    pub(crate) async fn tick(
        &self,
        runtime: crate::engine::Runtime,
        id: String,
    ) -> cutover::Result<Outcome> {
        let model = self.clone();
        tokio::task::spawn_blocking(move || {
            let rt=tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
            let state=rt.block_on(runtime.state());
            let cp=state.rollback.as_ref().unwrap();
            let key=Key {migration:id.clone(),cutover:cp.request.clone(),epoch:state.epoch as i64,coordinator:state.owner.clone()};
            let mut transactions=vec![txn(10,false),txn(20,false),txn(30,true),txn(40,false)];
            transactions[2].events[0].after.as_mut().unwrap().get_mut("control").unwrap().bytes=Some(serde_json::to_vec(&serde_json::json!({"message_lsn":25,"content":cutover::Marker {cutover:cp.request.clone(),epoch:1,lsn:25}.payload().as_bytes()})).unwrap());
            for txn in &mut transactions {
                txn.commit_position=Position::new(format!("{id}_rev"),txn.commit_position.bytes().to_vec());
                for event in &mut txn.events {event.position=txn.commit_position.clone();}
                *txn=AssembledTxn::new(txn.source.clone(),txn.txn.clone(),txn.commit_position.clone(),txn.commit_time,txn.events.clone()).unwrap();
            }
            let replay=replay_txns(&id,transactions);
            let manifest=rt.block_on(replay.store.get(&manifest_key("reverse-runner-v1",&id,&format!("{id}_rev"),0))).unwrap().unwrap();
            let digest=ManifestEntry::from_canonical_json(&manifest).unwrap().digest().unwrap();
            let e=Rc::new(RefCell::new(model.effects.lock().unwrap().clone()));
            e.borrow_mut().guarded=true;
            let mut log=RuntimeJournal::new(&runtime,Default::default())?;
            let now=std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;
            let result=execute_next(&key,now,&mut runtime.cutover_authority(),&rt,&mut Components {destination:&mut Endpoint(e.clone()),capture:&mut OwnerCapture {endpoint:Endpoint(e.clone()),runtime:runtime.clone(),digest},pre_hook:&mut Endpoint(e.clone()),replay:&replay,target:&mut Target(model.rows.clone()),verification:&mut Endpoint(e.clone()),source:&mut Endpoint(e.clone()),post_hook:&mut Endpoint(e.clone()),journal:&mut log});
            *model.effects.lock().unwrap()=e.borrow().clone();
            result
        }).await.unwrap()
    }
}
