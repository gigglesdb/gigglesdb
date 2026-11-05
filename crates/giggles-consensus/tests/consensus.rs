mod common;
use common::*;
use giggles_consensus::*;
use giggles_core::*;
use raft::{GetEntriesContext, Storage, eraftpb::*};
use serde_json::json;

#[test]
fn three_full_voters_restart_snapshot_tail_and_storage_contract() {
    let mut c = Cluster::new(1);
    c.provision(3, 2, 5);
    assert_eq!(
        c.submit(1, task(TaskAction::Execute(request(1, &[0]), None)))["code"],
        0
    );
    c.equal();
    let store = &c.nodes[&1].store;
    assert_eq!(store.first_index().unwrap(), 1);
    let last = store.last_index().unwrap();
    assert_eq!(
        store
            .entries(1, last + 1, Some(0), GetEntriesContext::empty(false))
            .unwrap()
            .len(),
        1
    );
    assert!(
        store
            .entries(last + 1, last + 2, None, GetEntriesContext::empty(false))
            .is_err()
    );
    c.checkpoint_all();
    let idx = c.nodes[&1].store.applied().unwrap();
    assert_eq!(c.nodes[&1].store.first_index().unwrap(), idx + 1);
    assert!(c.nodes[&1].store.term(idx).unwrap() > 0);
    assert!(c.nodes[&1].store.term(idx - 1).is_err());
    c.submit(1, task(TaskAction::Execute(request(2, &[1]), None)));
    c.equal();
    for id in VOTERS {
        c.restart(id);
    }
    c.nodes.get_mut(&2).unwrap().campaign().unwrap();
    c.pump();
    assert!(c.nodes[&2].is_leader());
    let r = c.submit(2, task(TaskAction::Status(OperationId(1))));
    assert_eq!(r["code"], 0);
    c.equal();
    let s = c.nodes[&2].store.image().unwrap().snapshot().unwrap();
    assert_eq!(s.tasks[&TaskId(1)].transitions, 2);
    assert_eq!(s.operations.len(), 2);
    c.hash_evidence();
}

#[test]
fn isolated_old_leader_cannot_current_read_or_commit_and_restores() {
    let mut c = Cluster::new(2);
    c.provision(2, 1, 5);
    c.isolate(1);
    c.nodes
        .get_mut(&1)
        .unwrap()
        .read_barrier(b"isolated".to_vec())
        .unwrap();
    c.nodes
        .get_mut(&1)
        .unwrap()
        .propose(
            b"unknown".to_vec(),
            &task(TaskAction::Execute(request(1, &[0]), None)),
        )
        .unwrap();
    c.pump();
    assert!(!c.reads.contains(b"isolated".as_slice()));
    assert!(!c.responses.contains_key(b"unknown".as_slice()));
    // Explicit library campaign schedules are reproducible; no host clock election.
    c.elect(2);
    c.submit(2, task(TaskAction::Execute(request(2, &[1]), None)));
    c.heal();
    c.equal();
    assert_eq!(
        c.nodes[&1].store.image().unwrap().snapshot().unwrap().rows[&key(0)].status,
        Status::Failed
    );
    c.nodes
        .get_mut(&2)
        .unwrap()
        .read_barrier(b"healthy".to_vec())
        .unwrap();
    c.pump();
    assert!(c.reads.contains(b"healthy".as_slice()));
    c.hash_evidence();
}

#[test]
fn lost_ack_retry_conflict_budget_and_disclosure_survive_failover() {
    let mut c = Cluster::new(3);
    c.provision(3, 1, 5);
    let original = c.submit(1, task(TaskAction::Execute(request(11, &[0]), None)));
    assert_eq!(original["code"], 0);
    // Discard client completion and kill leader after durable apply, before caller ACK.
    c.responses.clear();
    drop(c.nodes.remove(&1));
    c.elect(2);
    let retry = c.submit(2, task(TaskAction::Execute(request(11, &[0]), None)));
    assert_eq!(original, retry);
    assert_eq!(
        c.submit(2, task(TaskAction::Execute(request(11, &[1]), None)))["code"],
        116
    );
    assert_eq!(
        c.submit(2, task(TaskAction::Execute(request(12, &[1]), None)))["code"],
        112
    );
    let image = c.nodes[&2].store.image().unwrap();
    let s = image.snapshot().unwrap();
    assert_eq!(s.tasks[&TaskId(1)].transitions, 1);
    assert_eq!(s.rows[&key(1)].status, Status::Failed);
    assert!(
        image
            .accounting(TaskId(1))
            .unwrap()
            .unwrap()
            .used
            .disclosed_rows
            >= 2
    );
    c.restart(1);
    c.heal();
    c.equal();
    c.hash_evidence();
}

#[test]
fn verified_snapshots_reject_corruption_wrong_domain_stale_and_conflict() {
    let mut c = Cluster::new(4);
    c.provision(1, 1, 5);
    let snap = c.nodes[&1].store.checkpoint().unwrap();
    let before = c.nodes[&2].store.image().unwrap();
    let mut corrupt = snap.clone();
    let mut data = corrupt.data.to_vec();
    data[40] ^= 1;
    corrupt.set_data(data.into());
    assert!(c.nodes[&2].store.verify_snapshot(&corrupt).is_err());
    let mut wrong = snap.clone();
    wrong.mut_metadata().term += 1;
    assert!(c.nodes[&2].store.verify_snapshot(&wrong).is_err());
    let root = fresh("wrong-domain");
    let foreign = Store::create(root.join("foreign.redb"), 2, 999).unwrap();
    assert!(foreign.verify_snapshot(&snap).is_err());
    let mut wrong = snap.clone();
    wrong
        .mut_metadata()
        .mut_conf_state()
        .set_voters(vec![1, 2, 4]);
    assert!(c.nodes[&2].store.verify_snapshot(&wrong).is_err());
    c.submit(1, task(TaskAction::Execute(request(1, &[0]), None)));
    assert!(c.nodes[&2].store.verify_snapshot(&snap).is_err());
    assert_ne!(c.nodes[&2].store.image().unwrap(), before);
    c.equal();
    c.hash_evidence();
}

#[test]
fn approval_consumption_and_revocation_ordered_across_leaders() {
    let mut c = Cluster::new(5);
    c.provision(2, 2, 0);
    let p = c.submit(1, task(TaskAction::Preview(request(1, &[0]))));
    assert_eq!(p["code"], 0);
    let token = p["data"]["preview"].as_str().unwrap();
    let approved = c.submit(
        1,
        admin(json!({"command":"approve","principal":3,"preview":token,"time":{"Certified":10}})),
    );
    assert_eq!(approved["code"], 0);
    let id = ApprovalId(approved["data"].as_u64().unwrap());
    c.submit(1, task(TaskAction::Execute(request(1, &[0]), Some(id))));
    c.isolate(1);
    c.elect(2);
    c.submit(2, admin(json!({"command":"revoke","task":1})));
    assert_eq!(
        c.submit(2, task(TaskAction::Status(OperationId(1))))["code"],
        104
    );
    c.heal();
    c.equal();
    let s = c.nodes[&2].store.image().unwrap().snapshot().unwrap();
    assert!(s.approvals[&id].consumed);
    assert!(s.tasks[&TaskId(1)].revoked);
    assert_eq!(s.tasks[&TaskId(1)].transitions, 1);
    c.hash_evidence();
}

#[test]
fn seeded_reorder_duplicate_drop_with_independent_serial_oracle() {
    for seed in 10..20 {
        let mut c = Cluster::new(seed);
        c.provision(8, 4, 5);
        c.drop_messages = true;
        let mut expected = std::collections::BTreeSet::new();
        let mut decisions = std::collections::BTreeMap::new();
        for op in 1..=25 {
            let row = c.rand() % 8;
            let id = if op % 5 == 0 { op - 1 } else { op };
            let request = request(id, &[row]);
            let prior = decisions.get(&id).cloned();
            let expected_code = if let Some((old_row, code)) = prior {
                if old_row == row { code } else { 116 }
            } else if !expected.contains(&row) && expected.len() >= 4 {
                112
            } else {
                0
            };
            let reply = c.submit(1, task(TaskAction::Execute(request, None)));
            assert_eq!(reply["code"], expected_code, "seed={seed} op={op}");
            if prior.is_none() {
                decisions.insert(id, (row, expected_code));
                if expected_code == 0 {
                    expected.insert(row);
                }
            }
            let s = c.nodes[&1].store.image().unwrap().snapshot().unwrap();
            let actual = s
                .rows
                .iter()
                .filter_map(|(k, r)| (r.status == Status::Queued).then_some(k.row.0))
                .collect::<std::collections::BTreeSet<_>>();
            assert_eq!(actual, expected);
            assert_eq!(s.tasks[&TaskId(1)].transitions, expected.len() as u64);
        }
        c.equal();
        c.hash_evidence();
    }
}

#[test]
fn membership_identity_frame_and_command_bounds_fail_closed() {
    let mut c = Cluster::new(6);
    let before = c.nodes[&1].store.image().unwrap();
    let mut m = Message {
        from: 2,
        to: 1,
        ..Default::default()
    };
    m.set_msg_type(MessageType::MsgHeartbeat);
    assert!(c.nodes.get_mut(&1).unwrap().step(3, m.clone()).is_err());
    assert!(c.nodes.get_mut(&1).unwrap().step(99, m.clone()).is_err());
    let mut e = Entry::default();
    e.set_entry_type(EntryType::EntryConfChange);
    m.set_entries(vec![e].into());
    assert!(c.nodes.get_mut(&1).unwrap().step(2, m).is_err());
    assert!(
        c.nodes
            .get_mut(&1)
            .unwrap()
            .propose(vec![0; 65], &admin(json!({"command":"inspect"})))
            .is_err()
    );
    assert!(
        c.nodes
            .get_mut(&1)
            .unwrap()
            .propose(vec![], &Command::Admin(vec![0; MAX_COMMAND]))
            .is_err()
    );
    assert_eq!(c.nodes[&1].store.image().unwrap(), before);
    assert!(Store::open(c.root.join("node-1.redb"), 4, 42).is_err());
    c.hash_evidence();
}

#[test]
fn durable_vote_restart_refuses_second_vote_in_same_term() {
    let root = fresh("durable-vote");
    let store = Store::create(root.join("node.redb"), 1, 42).unwrap();
    let mut n = Node::new(store).unwrap();
    let mut m = Message {
        from: 2,
        to: 1,
        term: 7,
        ..Default::default()
    };
    m.set_msg_type(MessageType::MsgRequestVote);
    n.step(2, m).unwrap();
    let out = n.drain().unwrap();
    assert!(out.messages.iter().any(|m| m.to == 2 && !m.reject));
    let h = n.store.initial_state().unwrap().hard_state;
    assert_eq!((h.term, h.vote), (7, 2));
    drop(n);
    let mut n = Node::new(Store::open(root.join("node.redb"), 1, 42).unwrap()).unwrap();
    let mut m = Message {
        from: 3,
        to: 1,
        term: 7,
        ..Default::default()
    };
    m.set_msg_type(MessageType::MsgRequestVote);
    n.step(3, m).unwrap();
    let out = n.drain().unwrap();
    assert!(out.messages.iter().any(|m| m.to == 3 && m.reject));
    let h = n.store.initial_state().unwrap().hard_state;
    assert_eq!((h.term, h.vote), (7, 2));
    std::fs::write(
        root.join("checker.txt"),
        "PASS durable term7 vote2 survived restart; same-term candidate3 rejected\n",
    )
    .unwrap();
}

#[test]
fn leader_dies_before_quorum_uncommitted_tail_is_replaced() {
    let mut c = Cluster::new(30);
    c.provision(2, 1, 5);
    c.isolate(1);
    c.nodes
        .get_mut(&1)
        .unwrap()
        .propose(
            b"before-quorum".to_vec(),
            &task(TaskAction::Execute(request(1, &[0]), None)),
        )
        .unwrap();
    c.pump();
    assert!(!c.responses.contains_key(b"before-quorum".as_slice()));
    drop(c.nodes.remove(&1));
    c.elect(2);
    assert_eq!(
        c.submit(2, task(TaskAction::Execute(request(2, &[1]), None)))["code"],
        0
    );
    c.restart(1);
    c.heal();
    c.equal();
    let s = c.nodes[&2].store.image().unwrap().snapshot().unwrap();
    assert_eq!(s.rows[&key(0)].status, Status::Failed);
    assert!(!s.operations.contains_key(&(TaskId(1), OperationId(1))));
    c.hash_evidence();
}
