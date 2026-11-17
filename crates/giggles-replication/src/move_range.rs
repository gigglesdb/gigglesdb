//! Move one range replica using the resumable membership replacement driver.
use crate::{
    cluster_node::ClusterNode,
    error::{Error, Result},
    membership::PeerRecord,
    registry::{self, NodeState},
    replace::{self, ReplaceError, ReplaceRequest},
    sink::ReplicaHandle,
    split::Splitter,
};
use giggles_core::range_api::{RangeDescriptor, RangeId, ReplicaDesc, ReplicaRole};
use raft::StateRole;
use std::{
    thread,
    time::{Duration, Instant},
};

#[derive(Debug, PartialEq, Eq)]
pub enum MoveOutcome {
    Done(RangeDescriptor),
    NotLeader { leader: u64 },
}

/// The active persisted replacement step, if one is running.
pub fn move_status(handle: &ReplicaHandle) -> Option<String> {
    let s = replace::status(handle).ok()?;
    s.membership.active.as_ref()?;
    Some(format!("{:?}", s.membership.next()))
}

/// Moves `from` to the active registry node `to`. Retry on the returned leader.
pub fn move_replica(
    node: &ClusterNode,
    range: RangeId,
    from: u64,
    to: u64,
    timeout: Duration,
) -> Result<MoveOutcome> {
    let start = Instant::now();
    let handle = node
        .host()
        .group(range)
        .ok_or_else(|| Error::Identity("range not hosted".into()))?;
    let not_leader = || MoveOutcome::NotLeader {
        leader: handle.role().leader,
    };
    if handle.role().state != StateRole::Leader {
        return Ok(not_leader());
    }
    let store = node
        .host()
        .store()
        .ok_or_else(|| Error::Identity("range store missing".into()))?;
    let read = || {
        store
            .descriptor(range)
            .map_err(|e| Error::Core(e.to_string()))?
            .ok_or_else(|| Error::Identity("range descriptor missing".into()))
    };
    let mut desc = read()?;
    let status = match replace::status(&handle) {
        Ok(s) => s,
        Err(ReplaceError::NotLeader { leader }) => return Ok(MoveOutcome::NotLeader { leader }),
        Err(e) => return Err(Error::Apply(e.to_string())),
    };
    if desc
        .replicas
        .iter()
        .any(|r| r.node == to && r.role == ReplicaRole::Voter)
        && !desc.replicas.iter().any(|r| r.node == from)
        && status.membership.active.is_none()
    {
        // Also repairs a crash after Retire but before directory publication.
        Splitter::new(node.host().clone(), node.client().clone()).reconcile(Some(range))?;
        return Ok(MoveOutcome::Done(desc));
    }
    if from == to {
        return Err(Error::Identity("source and target must differ".into()));
    }
    if node.node_id() == from {
        let target = desc
            .replicas
            .iter()
            .filter(|r| {
                r.node != from && r.role == ReplicaRole::Voter && status.reachable.contains(&r.node)
            })
            .min_by_key(|r| {
                let leaderships = node
                    .host()
                    .groups()
                    .iter()
                    .filter(|id| {
                        node.host()
                            .group(**id)
                            .is_some_and(|h| h.role().leader == r.node)
                    })
                    .count();
                (r.node != to, leaderships, r.node)
            })
            .ok_or_else(|| Error::Identity("no reachable voter for leadership transfer".into()))?;
        handle.transfer_leader(target.node);
        let wait = timeout.min(Duration::from_secs(5));
        while handle.role().state == StateRole::Leader && start.elapsed() < wait {
            thread::sleep(Duration::from_millis(20));
        }
        if handle.role().state == StateRole::Leader {
            return Err(Error::Apply("leadership transfer timed out".into()));
        }
        return Ok(not_leader());
    }
    let target = registry::read_all(&**node.client())
        .map_err(|e| Error::Apply(e.to_string()))?
        .into_iter()
        .find(|r| r.id == to && r.state == NodeState::Active)
        .ok_or_else(|| Error::Identity("target is not active in the registry".into()))?;
    if !desc.replicas.iter().any(|r| r.node == to) {
        desc.replicas.push(ReplicaDesc {
            node: to,
            role: ReplicaRole::Learner,
        });
        desc.epoch.0 = desc
            .epoch
            .0
            .checked_add(1)
            .ok_or_else(|| Error::Identity("epoch exhausted".into()))?;
    }
    node.transport()
        .calls()
        .host_range(to, desc)
        .map_err(|e| Error::Apply(e.to_string()))?;
    let request = ReplaceRequest {
        lost: from,
        new: PeerRecord {
            id: to,
            address: target.address,
            fingerprint: target.fingerprint,
        },
    };
    match replace::run(&handle, &request, timeout.saturating_sub(start.elapsed())) {
        Ok(_) => {}
        Err(ReplaceError::NotLeader { leader }) => return Ok(MoveOutcome::NotLeader { leader }),
        Err(e) => return Err(Error::Apply(e.to_string())),
    }
    Splitter::new(node.host().clone(), node.client().clone()).reconcile(Some(range))?;
    Ok(MoveOutcome::Done(read()?))
}
