mod common;
use common::*;
use giggles_consensus::store::SaveFault;
use giggles_consensus::*;
use raft::Storage;
use serde_json::json;

#[test]
fn deterministic_storage_crashes_publish_all_or_nothing_and_fence_old_handles() {
    for fault in [SaveFault::BeforeCommit, SaveFault::AfterCommit] {
        let root = fresh("simulated-storage");
        let store = Store::simulated(1, 42).unwrap();
        let initial = store.durable_bytes().unwrap();
        let mut n = Node::new(store.clone()).unwrap();
        let mut vote = raft::eraftpb::Message {
            from: 2,
            to: 1,
            term: 7,
            ..Default::default()
        };
        vote.set_msg_type(raft::eraftpb::MessageType::MsgRequestVote);
        n.step(2, vote).unwrap();
        store.fail_next_save(fault).unwrap();
        assert!(n.drain().is_err());
        assert!(store.image().is_err());
        let durable = store.durable_bytes().unwrap();
        let recovered = Store::recover_simulated(&durable, 1, 42).unwrap();
        assert_eq!(recovered.applied().unwrap(), 0);
        assert_eq!(recovered.image().unwrap(), Image::new(42).unwrap());
        if fault == SaveFault::BeforeCommit {
            assert_eq!(durable, initial);
        } else {
            assert_ne!(durable, initial);
        }
        assert!(Store::recover_simulated(&durable, 2, 42).is_err());
        std::fs::write(root.join("durable.bin"), durable).unwrap();
        std::fs::write(
            root.join("checker.txt"),
            format!(
                "PASS {fault:?}: atomic durable record and indexed image; old handles fenced\n"
            ),
        )
        .unwrap();
    }
}

#[test]
fn simulated_and_real_storage_drive_identical_library_and_kernel_history() {
    let root = fresh("runtime-conformance");
    let mut real = std::collections::BTreeMap::new();
    let mut sim = std::collections::BTreeMap::new();
    for id in VOTERS {
        real.insert(
            id,
            Node::new(Store::create(root.join(format!("node-{id}.redb")), id, 42).unwrap())
                .unwrap(),
        );
        sim.insert(id, Node::new(Store::simulated(id, 42).unwrap()).unwrap());
    }
    fn pump(nodes: &mut std::collections::BTreeMap<u64, Node>) {
        for _ in 0..100 {
            let mut messages = Vec::new();
            for n in nodes.values_mut() {
                messages.extend(n.drain().unwrap().messages);
            }
            if messages.is_empty() {
                return;
            }
            for m in messages {
                nodes.get_mut(&m.to).unwrap().step(m.from, m).unwrap();
            }
        }
        panic!("not quiescent");
    }
    real.get_mut(&1).unwrap().campaign().unwrap();
    sim.get_mut(&1).unwrap().campaign().unwrap();
    pump(&mut real);
    pump(&mut sim);
    for (i,command) in [admin(json!({"command":"seed","key":key(0),"row":row()})),admin(json!({"command":"grant","grant":grant(1,5)})),admin(json!({"command":"provision_task_resources","task":1,"limits":{"read_rows":100,"disclosed_rows":10,"encoded_bytes":100000}})),task(TaskAction::Execute(request(1,&[0]),None))].into_iter().enumerate(){for nodes in [&mut real,&mut sim]{nodes.get_mut(&1).unwrap().propose(vec![i as u8],&command).unwrap();pump(nodes);}for id in VOTERS{assert_eq!(real[&id].store.image().unwrap(),sim[&id].store.image().unwrap());assert_eq!(real[&id].store.initial_state().unwrap().hard_state,sim[&id].store.initial_state().unwrap().hard_state);}}
    for id in VOTERS {
        let bytes = sim[&id].store.durable_bytes().unwrap();
        let reopened = Store::recover_simulated(&bytes, id, 42).unwrap();
        assert_eq!(reopened.image().unwrap(), real[&id].store.image().unwrap());
        std::fs::write(root.join(format!("node-{id}.simulated.bin")), bytes).unwrap();
    }
    std::fs::write(root.join("checker.txt"),"PASS real/simulated storage conformance: same fixed schedule, library hardstate, kernel image, applied recovery\n").unwrap();
}
