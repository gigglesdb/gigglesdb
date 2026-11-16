use super::*;
use crate::range_api::{
    Epoch, RangeDescriptor, RangeId, ReplicaDesc, ReplicaRole, SYSTEM_RANGE, first_user_key,
};
use crate::relational::{StatementClock, Value};
use crate::replicated::{AppliedPosition, BatchSink, ReplicateError};
use crate::storage::cluster::db::local::LocalCluster;
use std::path::PathBuf;
use std::sync::Mutex;

/// A temporary directory removed on drop.
struct Dir(PathBuf);
impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// One range's replication stand-in (as local.rs): applies batches in order, one at a time.
struct Sink {
    store: Arc<RangeStore>,
    range: RangeId,
    order: Mutex<()>,
}
impl BatchSink for Sink {
    fn replicate(&self, encoded_batch: Vec<u8>) -> Result<(), ReplicateError> {
        let _order = self.order.lock().unwrap_or_else(|p| p.into_inner());
        let index = self
            .store
            .applied(self.range)
            .map_err(|e| ReplicateError::Unavailable(format!("{e:?}")))?
            .map_or(1, |p| p.index + 1);
        self.store
            .apply(
                self.range,
                &encoded_batch,
                AppliedPosition { index, term: 1 },
            )
            .map_err(|e| ReplicateError::Unavailable(format!("{e:?}")))?;
        Ok(())
    }
}

fn settings() -> EngineSettings {
    EngineSettings {
        lease: Duration::from_secs(2),
        read_wait: Duration::from_secs(2),
        sweep: Duration::from_millis(50),
        ..EngineSettings::default()
    }
}

/// A fresh store with the system range and one user range from `first_user_key()`.
fn store() -> (Dir, Arc<RangeStore>, Vec<RangeDescriptor>) {
    let mut nonce = [0; 16];
    getrandom::fill(&mut nonce).unwrap();
    let dir =
        Dir(std::env::temp_dir().join(format!("cluster-node-{:032x}", u128::from_be_bytes(nonce))));
    std::fs::create_dir_all(&dir.0).unwrap();
    let store = Arc::new(RangeStore::create(dir.0.join("db"), [7; 16]).unwrap());
    let replicas = vec![ReplicaDesc {
        node: 1,
        role: ReplicaRole::Voter,
    }];
    let descriptors = vec![
        RangeDescriptor {
            id: SYSTEM_RANGE,
            epoch: Epoch(1),
            start: Vec::new(),
            end: Some(first_user_key()),
            replicas: replicas.clone(),
        },
        RangeDescriptor {
            id: RangeId(2),
            epoch: Epoch(1),
            start: first_user_key(),
            end: None,
            replicas,
        },
    ];
    for descriptor in &descriptors {
        store.create_range(descriptor).unwrap();
    }
    (dir, store, descriptors)
}

fn sink(store: &Arc<RangeStore>, range: RangeId) -> Arc<dyn BatchSink> {
    Arc::new(Sink {
        store: store.clone(),
        range,
        order: Mutex::new(()),
    })
}

#[test]
fn system_service_follows_the_local_system_engine() {
    let (_dir, store, descriptors) = store();
    let node = NodeEngines::new(store.clone(), settings());
    let opener = node.opener();
    let system = node.system_service();
    // Nothing open, no client: every oracle is NotLeader.
    let not_leader = Err(RangeError::NotLeader { hint: None });
    assert_eq!(system.oracle().next(1), not_leader);
    assert_eq!(node.oracle.next(1), not_leader);
    // The system range opens first: the user engine's open takes an oracle timestamp.
    for descriptor in &descriptors {
        opener
            .open(descriptor.clone(), sink(&store, descriptor.id), 1)
            .unwrap();
    }
    let first = system.oracle().next(1).unwrap();
    let second = node.oracle.next(3).unwrap();
    assert!(second > first);
    assert_eq!(system.lease(LeaseKind::ObjectId, 4242, 2).unwrap(), 1..3);

    opener.close(SYSTEM_RANGE);
    assert_eq!(system.oracle().next(1), not_leader);
    assert_eq!(
        system.lease(LeaseKind::ObjectId, 4242, 2),
        Err(RangeError::NotLeader { hint: None })
    );
    assert_eq!(node.oracle.next(1), not_leader);

    // A new term of the system range on this node: the same handle serves it again.
    opener
        .open(descriptors[0].clone(), sink(&store, SYSTEM_RANGE), 2)
        .unwrap();
    assert!(system.oracle().next(1).unwrap() > second);
    assert_eq!(system.lease(LeaseKind::ObjectId, 4242, 2).unwrap(), 3..5);
}

#[test]
fn engine_oracle_falls_back_to_the_client() {
    let (_dir, store, descriptors) = store();
    let node = NodeEngines::new(store.clone(), settings());
    let cluster = LocalCluster::new(&[]);
    node.set_client(Arc::downgrade(&cluster) as Weak<dyn RangeClient>);
    // The system range is led elsewhere: the user engine opens on the client's oracle.
    node.opener()
        .open(descriptors[1].clone(), sink(&store, RangeId(2)), 1)
        .unwrap();
    assert!(node.oracle.next(1).is_ok());
    assert_eq!(
        node.system_service().oracle().next(1),
        Err(RangeError::NotLeader { hint: None })
    );
}

fn clock() -> StatementClock {
    StatementClock {
        unavailable: None,
        transaction_us: 100,
        statement_us: 100,
        zone: Some(giggles_pg_types::temporal::SessionZone::Fixed(0)),
        wall: None,
    }
}

#[test]
fn attach_gateway_runs_sql_with_and_without_a_peer() {
    let cluster = LocalCluster::new(&[]);
    let settings = GatewaySettings {
        heartbeat: Duration::from_millis(200),
        ..GatewaySettings::default()
    };
    let a = attach_gateway(
        cluster.clone(),
        settings.clone(),
        Some(cluster.clone()),
        [9; 16],
        Limits::default(),
    )
    .unwrap();
    assert!(a.gateway_service.is_some());
    let mut sa = a.admin.sql_session();
    sa.execute_at("CREATE TABLE t(id INT PRIMARY KEY)", &[], None, &clock())
        .unwrap();
    sa.execute_at("INSERT INTO t VALUES (1)", &[], None, &clock())
        .unwrap();
    // B attaches after A's DDL and loads the catalog from the system range.
    let b = attach_gateway(
        cluster.clone(),
        GatewaySettings {
            node: 2,
            ..settings
        },
        None,
        [9; 16],
        Limits::default(),
    )
    .unwrap();
    // Without a peer there is no deadlock detection, but the gateway still answers txn_live
    // (R5-55), so its service is always registered.
    assert!(b.gateway_service.is_some());
    let mut sb = b.admin.sql_session();
    sb.execute_at("INSERT INTO t VALUES (2)", &[], None, &clock())
        .unwrap();
    let rows = sa
        .execute_at("SELECT id FROM t ORDER BY id", &[], None, &clock())
        .unwrap();
    let ids: Vec<i64> = rows
        .rows
        .iter()
        .map(|row| match &row.values[0] {
            Value::Int(id) => *id,
            other => panic!("not an int: {other:?}"),
        })
        .collect();
    assert_eq!(ids, vec![1, 2]);
}
