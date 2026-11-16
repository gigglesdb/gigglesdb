//! Raft group identity for many groups per node (GD-0075 R5-10).
//!
//! Every place R4 binds bytes to a cluster (the log's identity record, every
//! entry envelope, snapshot manifests, transport frame headers) is bound to
//! the group's cluster id instead: the database cluster id with its low eight
//! bytes XOR'd with the group id (a range id). Group 0 is the database id
//! itself, so an R4 single-group database is unchanged, and a log, entry,
//! snapshot or frame of one group is rejected by every other group exactly
//! where R4 rejects another cluster's. No persisted format changes.

/// The cluster id group `group` binds its log, entries, snapshots and frames to.
pub fn group_cluster(cluster: [u8; 16], group: u64) -> [u8; 16] {
    let mut out = cluster;
    for (b, g) in out[8..].iter_mut().zip(group.to_be_bytes()) {
        *b ^= g;
    }
    out
}

/// The group a group cluster id belongs to, if it derives from `cluster`.
pub fn group_of(cluster: [u8; 16], group_cluster_id: [u8; 16]) -> Option<u64> {
    if cluster[..8] != group_cluster_id[..8] {
        return None;
    }
    let mut group = [0; 8];
    for (g, (a, b)) in group
        .iter_mut()
        .zip(cluster[8..].iter().zip(&group_cluster_id[8..]))
    {
        *g = a ^ b;
    }
    Some(u64::from_be_bytes(group))
}
