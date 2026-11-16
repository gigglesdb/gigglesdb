//! GD-0074 R4-41: native once receipts resolve a lost reply across a leader change.
use super::common::net_cluster::NetCluster;
use giggles_core::{
    operations::{
        Bindings, Engine, Request, ValueExpr,
        once::{
            ONCE_CONTRACT_VERSION, OnceBody, OnceClockUnavailable, OnceError, OnceIdentity,
            OnceRequest, OnceStatus, OnceTime,
        },
    },
    relational::Value,
};
use giggles_replication::{core_store::ReplicatedEngine, failpoint, sink::SnapshotPolicy};
use std::{
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

fn request(engine: &ReplicatedEngine, id: i64) -> OnceRequest {
    OnceRequest {
        contract_version: ONCE_CONTRACT_VERSION,
        identity: OnceIdentity {
            database_domain: engine.admin().authority_metadata().unwrap().0,
            tenant: 7,
            namespace: "once-net".into(),
            key: id.to_be_bytes().to_vec(),
        },
        retention_seconds: 600,
        body: OnceBody::Operation(Request::Insert {
            table: "t".into(),
            values: vec![
                ("id".into(), ValueExpr::Literal(Value::Int(id))),
                ("n".into(), ValueExpr::Literal(Value::Int(10))),
            ],
        }),
    }
}

fn clock() -> Result<OnceTime, OnceClockUnavailable> {
    Ok(OnceTime {
        lower_unix_seconds: 1_000,
        upper_unix_seconds: 1_001,
        valid_until: Instant::now() + Duration::from_secs(10),
    })
}

fn rows(engine: &ReplicatedEngine) -> usize {
    engine
        .admin()
        .sql_session()
        .execute("SELECT id FROM t", &[], None)
        .unwrap()
        .rows
        .len()
}

fn retained(engine: &Arc<ReplicatedEngine>, request: &OnceRequest) {
    let native = Engine::new(engine.admin().clone());
    let Ok(OnceStatus::Retained(receipt)) = native.outcome(&request.identity) else {
        panic!("receipt is not retained on the current leader");
    };
    assert!(receipt.replayed && receipt.outcome.is_ok());
    assert_eq!(
        native.execute_once(request, &Bindings::new(), &clock),
        Ok(receipt)
    );
}

fn setup(cluster: [u8; 16]) -> NetCluster {
    let c = NetCluster::new("once-net", cluster, SnapshotPolicy::default());
    c.execute("CREATE TABLE IF NOT EXISTS t(id bigint PRIMARY KEY, n bigint)");
    // The first insert on an engine reserves row-id/version blocks in their own
    // entries; reserve them now so the next entry is the once commit itself.
    c.execute("INSERT INTO t VALUES (100, 0) ON CONFLICT DO NOTHING");
    c.converged();
    c
}

#[test]
fn committed_once_write_with_lost_reply_replays_on_the_new_leader() {
    let cluster = [0xD7; 16];
    let mut c = setup(cluster);
    let (old, engine) = c.leader();
    let original = request(&engine, 1);
    let native = Engine::new(engine.admin().clone());
    // The leader applies a newly committed entry before it sends the commit index
    // on, so it is the first to hit this point: the entry is committed by a
    // majority and the leader's driver dies before the proposal is acknowledged.
    failpoint::arm(cluster, "node.apply.after", 1);
    let lost = native.execute_once(&original, &Bindings::new(), &clock);
    let fired = failpoint::fired(cluster);
    failpoint::disarm(cluster);
    assert_eq!(lost, Err(OnceError::Unknown));
    assert_eq!(fired, vec![("node.apply.after", Some(old as u64 + 1))]);
    drop(native);
    drop(engine);
    c.kill(old);

    let (new, engine) = c.leader();
    assert_ne!(new, old);
    retained(&engine, &original);
    assert_eq!(rows(&engine), 2);
    drop(engine);

    c.start(old);
    c.converged();
    let (_, engine) = c.leader();
    retained(&engine, &original);
    assert_eq!(rows(&engine), 2);
}

#[test]
fn uncommitted_once_write_with_lost_reply_applies_once_on_the_new_leader() {
    let cluster = [0xD8; 16];
    let mut c = setup(cluster);
    let (old, engine) = c.leader();
    let original = request(&engine, 2);
    let native = Engine::new(engine.admin().clone());
    drop(engine);
    // Without a quorum the leader appends the entry but steps down before any
    // follower holds it, so the reply is lost and the entry can never commit.
    for i in (0..3).filter(|i| *i != old) {
        c.stop(i);
    }
    let pending = {
        let request = original.clone();
        thread::spawn(move || native.execute_once(&request, &Bindings::new(), &clock))
    };
    assert_eq!(pending.join().unwrap(), Err(OnceError::Unknown));
    c.kill(old);
    for i in (0..3).filter(|i| *i != old) {
        c.start(i);
    }

    let (new, engine) = c.leader();
    assert_ne!(new, old);
    let native = Engine::new(engine.admin().clone());
    assert_eq!(native.outcome(&original.identity), Ok(OnceStatus::Absent));
    let applied = native
        .execute_once(&original, &Bindings::new(), &clock)
        .unwrap();
    assert!(!applied.replayed && applied.outcome.is_ok());
    assert_eq!(rows(&engine), 2);
    drop(native);
    drop(engine);

    // The deposed leader's uncommitted entry is truncated when it rejoins.
    c.start(old);
    c.converged();
    let (_, engine) = c.leader();
    retained(&engine, &original);
    assert_eq!(rows(&engine), 2);
}
