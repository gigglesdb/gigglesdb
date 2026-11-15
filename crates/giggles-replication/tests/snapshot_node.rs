use crate::common::{CLUSTER, Cluster};
use giggles_replication::{
    error::Error,
    log_store::LogStore,
    node::Replica,
    state::{AppliedPosition, ApplyOutcome, MemStateMachine, StateError, StateMachine},
};
use raft::{
    Storage, StorageError,
    eraftpb::{Message, MessageType},
};
use std::sync::Arc;

#[test]
fn wiped_follower_rejoins_by_snapshot() {
    let mut c = Cluster::new("snapshot-rejoin", 3, 41);
    c.elect(1);
    for n in 0..30 {
        c.propose(1, &[n]).unwrap();
    }
    let manifest = c.snapshot(1);
    assert_eq!(c.compact(1, 0), Some(manifest.index));
    c.wipe(3);
    c.restart(3);
    for n in 30..40 {
        c.propose(1, &[n]).unwrap();
    }
    for _ in 0..100 {
        if c.nodes[&3].applied_index() == c.nodes[&1].applied_index() {
            break;
        }
        c.tick_all(1);
    }
    c.assert_equal();
    assert_eq!(c.states[&3].applied_batches().len(), 40);
    assert_eq!(
        c.nodes[&3].log().snapshot_meta().unwrap(),
        (manifest.index, manifest.term)
    );
    assert!(c.states[&3].applied_batches()[0].0.index > 1);
}

#[test]
fn retained_margin_lets_follower_catch_up_without_snapshot() {
    let mut c = Cluster::new("snapshot-margin", 3, 42);
    c.elect(1);
    for n in 0..30 {
        c.propose(1, &[n]).unwrap();
    }
    c.isolate(3);
    for n in 30..35 {
        c.propose(1, &[n]).unwrap();
    }
    let m = c.snapshot(1);
    assert_eq!(c.compact(1, 20), Some(m.index - 20));
    c.heal();
    c.tick_all(4);
    c.assert_equal();
    assert_eq!(c.states[&3].applied_batches().len(), 35);
    assert_eq!(c.nodes[&3].log().snapshot_meta().unwrap(), (0, 0));
}

#[test]
fn snapshot_without_received_files_is_rejected() {
    let mut c = Cluster::new("snapshot-missing", 3, 43);
    c.elect(1);
    c.propose(1, b"batch").unwrap();
    let m = c.snapshot(1);
    let mut message = Message {
        from: 1,
        to: 3,
        msg_type: MessageType::MsgSnapshot,
        term: c.nodes[&1].role().term,
        ..Default::default()
    };
    message.set_snapshot(m.to_raft());
    assert!(matches!(
        c.nodes.get_mut(&3).unwrap().step(1, message),
        Err(Error::Corrupt(_))
    ));
    assert_eq!(c.nodes[&3].log().snapshot_meta().unwrap(), (0, 0));
    assert!(c.nodes[&1].log().snapshot(m.index, 3).is_ok());
    assert!(matches!(
        c.nodes[&1].log().snapshot(m.index + 1, 3),
        Err(raft::Error::Store(
            StorageError::SnapshotTemporarilyUnavailable
        ))
    ));
}

#[test]
fn startup_reconciles_state_install_before_log_install() {
    let mut c = Cluster::new("snapshot-reconcile", 3, 44);
    c.elect(1);
    for n in 0..5 {
        c.propose(1, &[n]).unwrap();
    }
    let source = c.root.join("checkpoint");
    let p = c.states[&1].checkpoint(&source).unwrap();
    let state = Arc::new(MemStateMachine::new());
    assert_eq!(state.install(&source).unwrap(), p);
    assert!(!source.exists());
    let log = LogStore::open(&c.root.join("fresh"), CLUSTER, 3).unwrap();
    assert_eq!(log.hard_state().unwrap().commit, 0);
    let node = Replica::new(log, state.clone(), &[1, 2, 3]).unwrap();
    assert_eq!(node.log().snapshot_meta().unwrap(), (p.index, p.term));
    assert_eq!(node.log().hard_state().unwrap().commit, p.index);
    assert_eq!(node.applied_index(), p.index);
    assert_eq!(state.applied_batches(), c.states[&1].applied_batches());
}

struct BusyState(MemStateMachine);
impl StateMachine for BusyState {
    fn can_install(&self) -> bool {
        false
    }
    fn applied(&self) -> Result<Option<AppliedPosition>, StateError> {
        self.0.applied()
    }
    fn apply(&self, batch: &[u8], at: AppliedPosition) -> Result<ApplyOutcome, StateError> {
        self.0.apply(batch, at)
    }
    fn apply_noop(&self, at: AppliedPosition) -> Result<ApplyOutcome, StateError> {
        self.0.apply_noop(at)
    }
    fn checkpoint(&self, target: &std::path::Path) -> Result<AppliedPosition, StateError> {
        self.0.checkpoint(target)
    }
    fn install(&self, _: &std::path::Path) -> Result<AppliedPosition, StateError> {
        panic!("busy state must never receive an install")
    }
    fn flush(&self) -> Result<(), StateError> {
        self.0.flush()
    }
}

#[test]
fn snapshot_with_received_files_is_rejected_while_busy() {
    let mut c = Cluster::new("snapshot-busy", 3, 45);
    c.elect(1);
    c.propose(1, b"batch").unwrap();
    let m = c.snapshot(1);
    let sender = c.nodes[&1].log().snapshots().unwrap();
    let receiver = c.nodes[&3].log().snapshots().unwrap().clone();
    let mut writer = receiver.incoming(&m).unwrap();
    for (i, file) in m.files.iter().enumerate() {
        let bytes = std::fs::read(sender.built_data_dir(&m).join(&file.name)).unwrap();
        writer.write_chunk(i as u32, 0, &bytes).unwrap();
    }
    writer.finish().unwrap();
    assert!(receiver.take_incoming(m.index, m.term).is_some());
    c.crash(3);
    let mut log = LogStore::open(&c.root.join("3"), CLUSTER, 3).unwrap();
    log.attach_snapshots(receiver.clone());
    let mut node =
        Replica::new(log, Arc::new(BusyState(MemStateMachine::new())), &[1, 2, 3]).unwrap();
    let mut message = Message {
        from: 1,
        to: 3,
        msg_type: MessageType::MsgSnapshot,
        term: c.nodes[&1].role().term,
        ..Default::default()
    };
    message.set_snapshot(m.to_raft());
    assert!(matches!(node.step(1, message), Err(Error::Corrupt(_))));
    node.drive().unwrap();
    assert_eq!(node.log().snapshot_meta().unwrap(), (0, 0));
    assert!(receiver.take_incoming(m.index, m.term).is_some());
}

#[test]
fn startup_finishes_or_clears_pending_install() {
    use giggles_replication::log_store::PendingInstall;
    use raft::eraftpb::ConfState;
    let c = Cluster::new("pending-install", 3, 46);
    let membership = giggles_replication::membership::Membership::default().encode();
    for installed in [true, false] {
        let state = Arc::new(MemStateMachine::new());
        if installed {
            for index in 1..=10 {
                state
                    .apply_noop(AppliedPosition { index, term: 2 })
                    .unwrap();
            }
        }
        let log = LogStore::open(&c.root.join(format!("pending-{installed}")), CLUSTER, 1).unwrap();
        let mut conf = ConfState::default();
        conf.set_voters(vec![1, 2]);
        conf.set_learners(vec![3]);
        log.set_pending_install(&PendingInstall {
            index: 10,
            term: 2,
            conf: conf.clone(),
            membership: membership.clone(),
        })
        .unwrap();
        let node = Replica::new(log, state, &[1, 2, 3]).unwrap();
        assert!(node.log().pending_install().unwrap().is_none());
        if installed {
            assert_eq!(node.log().conf_state().unwrap(), conf);
            assert_eq!(node.log().conf_index().unwrap(), 10);
            assert_eq!(
                node.log().membership().unwrap(),
                Some(membership.clone())
            );
            assert_eq!(node.log().snapshot_meta().unwrap(), (10, 2));
        } else {
            assert_eq!(node.log().conf_state().unwrap().voters, vec![1, 2, 3]);
            assert_eq!(node.log().conf_index().unwrap(), 0);
            assert_eq!(node.log().membership().unwrap(), None);
        }
    }
}

#[test]
fn snapshot_waits_when_configuration_is_ahead_of_data() {
    let mut c = Cluster::new("snapshot-conf-ahead", 3, 47);
    c.elect(1);
    c.propose(1, b"batch").unwrap();
    let node = c.nodes.get_mut(&1).unwrap();
    let conf = node.log().conf_state().unwrap();
    node.log()
        .apply_conf(&conf, node.applied_index() + 1, b"membership")
        .unwrap();
    assert!(node.build_snapshot().unwrap().is_none());
}
