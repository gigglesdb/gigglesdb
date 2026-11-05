//! Actual RawNodes and redb files; an in-process message schedule, not a network.
use giggles_membership::{
    durable::{Node, Store},
    *,
};
use std::{collections::BTreeMap, path::PathBuf};
fn pump(nodes: &mut BTreeMap<u64, Node>) -> Result<()> {
    for _ in 0..1000 {
        let mut messages = vec![];
        for n in nodes.values_mut() {
            let out = n.drive()?;
            if !out.rejected.is_empty() {
                return Err(format!("{:?}", out.rejected).into());
            }
            messages.extend(out.messages);
        }
        if messages.is_empty() {
            return Ok(());
        }
        for m in messages {
            if let Some(n) = nodes.get_mut(&m.to) {
                n.step(m.from, m)?;
            }
        }
    }
    Err("message schedule exceeded bound".into())
}
fn main() -> Result<()> {
    let root = PathBuf::from(
        std::env::args_os()
            .nth(1)
            .ok_or("usage: replace NEW_ARTIFACT_DIRECTORY")?,
    );
    std::fs::create_dir_all(&root)?;
    let identity = Identity {
        group: 7,
        owner_epoch: 1,
    };
    let initial = Membership::new(identity, vec![1, 2, 3], 8)?;
    let mut nodes = BTreeMap::new();
    for id in 1..=4 {
        nodes.insert(
            id,
            Node::new(Store::open(
                root.join(format!("node-{id}.redb")),
                id,
                initial.clone(),
            )?)?,
        );
    }
    nodes.get_mut(&1).ok_or("leader")?.campaign()?;
    pump(&mut nodes)?;
    nodes.get_mut(&1).ok_or("leader")?.propose(Command {
        identity,
        operation: Operation::Begin(Intent {
            id: 91,
            epoch: 1,
            learner: 4,
            remove: 3,
        }),
    })?;
    pump(&mut nodes)?;
    let change = |step| Change {
        identity,
        intent: 91,
        epoch: 1,
        step,
    };
    nodes
        .get_mut(&1)
        .ok_or("leader")?
        .change(change(Step::AddLearner))?;
    pump(&mut nodes)?;
    let barrier = nodes[&4].store.applied()?.min(nodes[&1].store.applied()?);
    // Same-process direct read stands in for an authenticated remote durable ack.
    let ready = Readiness {
        intent: 91,
        epoch: 1,
        term: nodes[&1].term(),
        barrier,
        durable_applied: barrier,
    };
    nodes.get_mut(&1).ok_or("leader")?.propose(Command {
        identity,
        operation: Operation::Ready(ready),
    })?;
    pump(&mut nodes)?;
    nodes
        .get_mut(&1)
        .ok_or("leader")?
        .change(change(Step::EnterJoint))?;
    pump(&mut nodes)?;
    println!("joint={:?}", nodes[&1].store.conf_state()?);
    nodes
        .get_mut(&1)
        .ok_or("leader")?
        .change(change(Step::LeaveJoint))?;
    pump(&mut nodes)?;
    println!(
        "complete={:?} applied={}",
        nodes[&1].store.membership()?,
        nodes[&1].store.applied()?
    );
    Ok(())
}
