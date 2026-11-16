//! A pre-split database in process (GD-0075 R5-24): three store-backed
//! RangeHosts over TLS, one group per range with round-robin voters, and a
//! routed client on every node that finds and reaches every range's leader
//! through the directory (system range) and peer calls (R5-21, R5-23).
use super::common::certs::{config_for, make_pki};
use giggles_core::{
    range_api::{
        ClusterTxnId, Epoch, HomeOp, HomeReply, LeaseKind, LockGrant, LockRequest, Outcome,
        RangeClient, RangeDescriptor, RangeEngineOpener, RangeError, RangeId, RangeService,
        RangeSpace, RangeWrite, ReadDirection, ReadPage, ReadRequest, SYSTEM_RANGE, SystemService,
        Timestamp, TsOracle, range_key,
    },
    replicated::{BatchSink, RangeStore},
};
use giggles_replication::{
    bootstrap::{directory_records, hosted_by, presplit},
    client::{LocalRanges, RangeTransport, RoutedRangeClient},
    host::{RangeHost, RangeHostConfig},
    transport::{GroupRouter, Transport, TransportNetwork},
};
use std::{
    collections::BTreeMap,
    net::TcpListener,
    ops::Bound,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const CLUSTER: [u8; 16] = [78; 16];
const NODES: [u64; 3] = [1, 2, 3];

fn table(id: u64) -> Vec<u8> {
    range_key(RangeSpace::Rows, &id.to_be_bytes())
}

fn unavailable<T>() -> Result<T, RangeError> {
    Err(RangeError::Unavailable("stub".into()))
}

/// Serves its descriptor; the system range also serves the directory records
/// (raw MVCC pairs at version ts 1) for reads at ts >= 1.
struct Stub {
    descriptor: RangeDescriptor,
    directory: Vec<(Vec<u8>, Vec<u8>)>,
}
impl RangeService for Stub {
    fn descriptor(&self) -> RangeDescriptor {
        self.descriptor.clone()
    }
    fn read(&self, req: ReadRequest) -> Result<ReadPage, RangeError> {
        if req.epoch != self.descriptor.epoch {
            return Err(RangeError::StaleEpoch {
                descriptor: self.descriptor.clone(),
            });
        }
        let pairs = self
            .directory
            .iter()
            .filter(|(k, _)| {
                std::ops::RangeBounds::contains(&(req.start.clone(), req.end.clone()), k)
            })
            .filter(|_| req.space == RangeSpace::Catalog && req.ts.0 >= 1)
            .cloned()
            .collect();
        Ok(ReadPage {
            pairs,
            resume: None,
            range_end: self.descriptor.end.clone(),
        })
    }
    fn lock(&self, _: LockRequest) -> Result<LockGrant, RangeError> {
        unavailable()
    }
    fn blockers(&self, _: ClusterTxnId) -> Result<Vec<ClusterTxnId>, RangeError> {
        unavailable()
    }
    fn cancel_wait(&self, _: ClusterTxnId) -> Result<(), RangeError> {
        unavailable()
    }
    fn release(&self, _: ClusterTxnId) -> Result<(), RangeError> {
        unavailable()
    }
    fn heartbeat(&self, _: ClusterTxnId) -> Result<(), RangeError> {
        unavailable()
    }
    fn commit(
        &self,
        _: Epoch,
        _: ClusterTxnId,
        _: Vec<RangeWrite>,
        _: Option<u64>,
        _: Option<u64>,
    ) -> Result<Timestamp, RangeError> {
        unavailable()
    }
    fn prepare(
        &self,
        _: Epoch,
        _: ClusterTxnId,
        _: RangeId,
        _: Vec<RangeWrite>,
        _: Option<u64>,
    ) -> Result<Timestamp, RangeError> {
        unavailable()
    }
    fn finalize(&self, _: ClusterTxnId, _: Outcome) -> Result<(), RangeError> {
        unavailable()
    }
    fn home(&self, _: Epoch, _: HomeOp) -> Result<HomeReply, RangeError> {
        unavailable()
    }
}

struct Opener {
    directory: Vec<(Vec<u8>, Vec<u8>)>,
}
impl RangeEngineOpener for Opener {
    fn open(
        &self,
        descriptor: RangeDescriptor,
        _sink: Arc<dyn BatchSink>,
        _generation: u64,
    ) -> Result<Arc<dyn RangeService>, RangeError> {
        let directory = if descriptor.id == SYSTEM_RANGE {
            self.directory.clone()
        } else {
            Vec::new()
        };
        Ok(Arc::new(Stub {
            descriptor,
            directory,
        }))
    }
    fn close(&self, _: RangeId) {}
}

/// A node-local oracle (enough for routing: lookups only need ts >= 1).
struct Clock(AtomicU64);
impl TsOracle for Clock {
    fn next(&self, n: u32) -> Result<Timestamp, RangeError> {
        Ok(Timestamp(self.0.fetch_add(u64::from(n), Ordering::SeqCst)))
    }
}
impl SystemService for Clock {
    fn oracle(&self) -> &dyn TsOracle {
        self
    }
    fn lease(&self, _: LeaseKind, _: u64, count: u64) -> Result<std::ops::Range<u64>, RangeError> {
        Ok(0..count)
    }
}

#[test]
fn every_range_of_a_presplit_database_is_reachable_from_every_node() {
    let root = std::env::temp_dir().join(format!(
        "presplit-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    // Range 1 = system keys, ranges 2..=5 split the user keyspaces; 3 voters each.
    let descs = presplit(&[table(100), table(200), table(300)], &NODES, 3).unwrap();
    assert_eq!(descs.len(), 5);
    let directory: Vec<(Vec<u8>, Vec<u8>)> = directory_records(&descs)
        .unwrap()
        .into_iter()
        .map(|w| {
            let mut raw = w.key;
            raw.extend_from_slice(&(!1u64).to_be_bytes());
            let mut value = vec![0];
            value.extend_from_slice(&w.value.unwrap());
            (raw, value)
        })
        .collect();

    let pki = make_pki(&root.join("pki"), &NODES);
    let listeners: Vec<_> = NODES
        .iter()
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let addresses: BTreeMap<_, _> = NODES
        .iter()
        .zip(&listeners)
        .map(|(id, l)| (*id, l.local_addr().unwrap().to_string()))
        .collect();
    drop(listeners);
    let mut nodes = Vec::new();
    for &node in &NODES {
        let transport = Arc::new(
            Transport::start_groups(
                config_for(&pki, node, CLUSTER, &addresses),
                Arc::new(GroupRouter::new()),
            )
            .unwrap(),
        );
        let store = Arc::new(RangeStore::create(root.join(format!("n{node}/data")), CLUSTER).unwrap());
        for d in hosted_by(&descs, node) {
            store.create_range(&d).unwrap();
        }
        let config = RangeHostConfig {
            node_id: node,
            cluster: CLUSTER,
            log_dir: root.join(format!("n{node}/log")),
            snap_dir: root.join(format!("n{node}/snap")),
            tick: Duration::from_millis(20),
            policy: None,
            liveness: None,
        };
        let opener = Arc::new(Opener {
            directory: directory.clone(),
        });
        let host = Arc::new(
            RangeHost::open(
                config,
                Arc::new(TransportNetwork::new(transport.clone())),
                store,
                Some(opener),
            )
            .unwrap(),
        );
        host.set_system(Some(Arc::new(Clock(AtomicU64::new(10)))));
        transport.set_call_handlers(Some(host.clone() as Arc<dyn LocalRanges>), None);
        nodes.push((node, host, transport));
    }
    // Every range gets one leader engine somewhere.
    let deadline = Instant::now() + Duration::from_secs(60);
    while !descs
        .iter()
        .all(|d| nodes.iter().any(|(_, h, _)| h.service(d.id).is_some()))
    {
        assert!(Instant::now() < deadline, "leaders for every range");
        thread::sleep(Duration::from_millis(20));
    }

    let probes: Vec<(Vec<u8>, &RangeDescriptor)> = vec![
        (range_key(RangeSpace::Catalog, b"t"), &descs[0]),
        (table(1), &descs[1]),
        (table(150), &descs[2]),
        (table(250), &descs[3]),
        (table(350), &descs[4]),
        (range_key(RangeSpace::Index, &[7; 9]), &descs[4]),
    ];
    for (node, host, transport) in &nodes {
        let client = RoutedRangeClient::new(
            *node,
            descs[0].clone(),
            transport.calls().clone() as Arc<dyn RangeTransport>,
            host.clone() as Arc<dyn LocalRanges>,
        );
        for (key, want) in &probes {
            // A lookup through the directory, retried while engines settle.
            let deadline = Instant::now() + Duration::from_secs(30);
            let found = loop {
                match client.descriptor_for(key) {
                    Ok(d) => break d,
                    Err(e) => {
                        assert!(Instant::now() < deadline, "node {node} key {key:02x?}: {e}");
                        thread::sleep(Duration::from_millis(20));
                    }
                }
            };
            assert_eq!(&found, *want, "node {node} key {key:02x?}");
            // A read reaches the range's leader, local or remote.
            let page = client
                .read(ReadRequest {
                    range: found.id,
                    epoch: found.epoch,
                    space: RangeSpace::Rows,
                    start: Bound::Unbounded,
                    end: Bound::Unbounded,
                    dir: ReadDirection::Forward,
                    ts: Timestamp(5),
                    page_bytes: 1024,
                })
                .unwrap_or_else(|e| panic!("node {node} read range {}: {e}", found.id.0));
            assert_eq!(page.range_end, found.end);
        }
    }
    drop(nodes);
    let _ = std::fs::remove_dir_all(root);
}
