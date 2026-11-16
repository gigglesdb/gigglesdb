//! Many Raft groups over one transport (GD-0075 R5-11).
//!
//! A `GroupRouter` maps a group id (a range id; 0 for an R4 single-group
//! database) to the sink and snapshot store its inbound frames go to. The
//! frame format is unchanged: a frame of group `g` carries
//! `group::group_cluster(cluster, g)` in its header's cluster field, and the
//! receiver recovers `g` with `group::group_of`. Every group's outbox to one
//! peer shares that peer's single connection and queue.
use super::{PeerSink, Transport};
use crate::{host::GroupNetwork, sink::Outbox, snapshot::SnapshotStore};
use giggles_core::range_api::RangeId;
use std::{
    collections::BTreeMap,
    sync::{
        Arc, RwLock,
        atomic::{AtomicU64, Ordering},
    },
};

#[derive(Clone)]
pub(crate) struct Route {
    pub(crate) sink: Arc<dyn PeerSink>,
    pub(crate) snapshots: Option<Arc<SnapshotStore>>,
}

#[derive(Default)]
pub struct GroupRouter {
    groups: RwLock<BTreeMap<u64, Route>>,
    dropped: AtomicU64,
}

impl GroupRouter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Inbound frames for `group` go to `sink`; its snapshot transfers use `snapshots`.
    /// Replaces an earlier registration of the same group.
    pub fn register(
        &self,
        group: u64,
        sink: Arc<dyn PeerSink>,
        snapshots: Option<Arc<SnapshotStore>>,
    ) {
        let route = Route { sink, snapshots };
        self.groups.write().unwrap().insert(group, route);
    }

    pub fn unregister(&self, group: u64) {
        self.groups.write().unwrap().remove(&group);
    }

    pub fn groups(&self) -> Vec<u64> {
        self.groups.read().unwrap().keys().copied().collect()
    }

    /// Inbound frames dropped because their group is not registered here.
    pub fn dropped_unknown_group(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    pub(crate) fn route(&self, group: u64) -> Option<Route> {
        self.groups.read().unwrap().get(&group).cloned()
    }

    pub(crate) fn count_unknown(&self) {
        self.dropped.fetch_add(1, Ordering::Relaxed);
    }
}

/// `host::GroupNetwork` over a running transport: a `RangeHost` runs over TLS.
pub struct TransportNetwork {
    transport: Arc<Transport>,
}

impl TransportNetwork {
    pub fn new(transport: Arc<Transport>) -> Self {
        Self { transport }
    }

    pub fn transport(&self) -> &Arc<Transport> {
        &self.transport
    }
}

impl GroupNetwork for TransportNetwork {
    fn outbox(&self, group: RangeId) -> Box<dyn Outbox> {
        self.transport.group_outbox(group.0)
    }
    fn register(&self, group: RangeId, sink: Arc<dyn PeerSink>) {
        // GroupNetwork carries no snapshot store yet; snapshot sends of
        // such a group report failure until R5-14 wires the stores through.
        self.transport.router().register(group.0, sink, None);
    }
    fn register_group(
        &self,
        group: RangeId,
        sink: Arc<dyn PeerSink>,
        snapshots: Option<Arc<SnapshotStore>>,
    ) {
        self.transport.router().register(group.0, sink, snapshots);
    }
    fn unregister(&self, group: RangeId) {
        self.transport.router().unregister(group.0);
    }
}
