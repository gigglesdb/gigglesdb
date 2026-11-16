//! Pre-split cluster bootstrap for tests and development (GD-0075 R5-24).
//!
//! A pre-split database starts with the system range 1 = `[MIN, first_user_key())`
//! and one user range per interval between consecutive split keys, ranges 2, 3, ...
//! in key order, all at epoch 1. Voters are assigned round-robin over the nodes.
use giggles_core::range_api::{
    Epoch, RangeDescriptor, RangeError, RangeId, RangeWrite, ReplicaDesc, ReplicaRole,
    SYSTEM_RANGE, first_user_key, split_range_key,
};

use crate::directory;

/// Descriptors of a pre-split database. `splits` are range keys strictly above
/// `first_user_key()` in increasing order (each a whole user key or a table/index
/// prefix, design.md Changes, R5-80 constraint); `nodes` are the node ids;
/// each range gets `replication` voters (at most the node count), assigned
/// round-robin starting one node later for each successive range.
pub fn presplit(
    splits: &[Vec<u8>],
    nodes: &[u64],
    replication: usize,
) -> Result<Vec<RangeDescriptor>, RangeError> {
    let invalid = |what: String| RangeError::Corrupt(format!("pre-split bootstrap: {what}"));
    let mut nodes = nodes.to_vec();
    nodes.sort_unstable();
    nodes.dedup();
    if nodes.is_empty() || replication == 0 {
        return Err(invalid("no nodes or zero replication".into()));
    }
    let replication = replication.min(nodes.len());
    let user = first_user_key();
    let mut bounds = vec![Vec::new(), user.clone()];
    for split in splits {
        if split_range_key(split).is_none() {
            return Err(invalid(format!("split {split:02x?} is not a range key")));
        }
        if split <= bounds.last().expect("non-empty") {
            return Err(invalid(format!(
                "split {split:02x?} is not above the previous bound"
            )));
        }
        bounds.push(split.clone());
    }
    let mut out = Vec::with_capacity(bounds.len());
    for (i, start) in bounds.iter().enumerate() {
        let mut replicas: Vec<ReplicaDesc> = (0..replication)
            .map(|k| ReplicaDesc {
                node: nodes[(i + k) % nodes.len()],
                role: ReplicaRole::Voter,
            })
            .collect();
        replicas.sort_by_key(|r| r.node);
        let desc = RangeDescriptor {
            id: if i == 0 {
                SYSTEM_RANGE
            } else {
                RangeId(SYSTEM_RANGE.0 + i as u64)
            },
            epoch: Epoch(1),
            start: start.clone(),
            end: bounds.get(i + 1).cloned(),
            replicas,
        };
        desc.validate()?;
        out.push(desc);
    }
    Ok(out)
}

/// The directory writes that describe `descriptors` (written to the system range).
pub fn directory_records(descriptors: &[RangeDescriptor]) -> Result<Vec<RangeWrite>, RangeError> {
    descriptors.iter().map(directory::record_write).collect()
}

/// The descriptors among `descriptors` that list `node` as a replica.
pub fn hosted_by(descriptors: &[RangeDescriptor], node: u64) -> Vec<RangeDescriptor> {
    descriptors
        .iter()
        .filter(|d| d.replicas.iter().any(|r| r.node == node))
        .cloned()
        .collect()
}
