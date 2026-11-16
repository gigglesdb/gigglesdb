//! Coalesced node liveness (GD-0075 R5-17b): one heartbeat per peer node per
//! interval; silence past the timeout declares the peer dead, a heartbeat alive.
use giggles_replication::{
    liveness::{Liveness, LivenessConfig},
    sink::Outbox,
};
use raft::eraftpb::Message;
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

/// Delivers node 2's heartbeats to node 1's tracker while `up` is set.
struct Wire {
    to: Arc<Mutex<Option<Arc<Liveness>>>>,
    up: Arc<AtomicBool>,
}
impl Outbox for Wire {
    fn send(&mut self, m: Message) {
        if self.up.load(Ordering::SeqCst)
            && let Some(target) = self.to.lock().unwrap().as_ref()
        {
            target.heard(m.from);
        }
    }
}

fn wait_for(events: &Mutex<Vec<(u64, bool)>>, want: (u64, bool), limit: Duration) -> Duration {
    let start = Instant::now();
    while start.elapsed() < limit {
        if events.lock().unwrap().contains(&want) {
            return start.elapsed();
        }
        std::thread::yield_now();
        std::thread::park_timeout(Duration::from_millis(5));
    }
    panic!("no {want:?} within {limit:?}");
}

#[test]
fn silence_declares_a_peer_dead_and_a_heartbeat_alive_again() {
    let config = LivenessConfig {
        interval: Duration::from_millis(20),
        timeout: Duration::from_millis(100),
    };
    let one_slot = Arc::new(Mutex::new(None));
    let up = Arc::new(AtomicBool::new(true));
    // Node 1 heartbeats into nothing; node 2 heartbeats into node 1.
    let one = Liveness::start(
        1,
        config,
        Box::new(Wire {
            to: Arc::new(Mutex::new(None)),
            up: up.clone(),
        }),
    );
    *one_slot.lock().unwrap() = Some(one.clone());
    let two = Liveness::start(
        2,
        config,
        Box::new(Wire {
            to: one_slot.clone(),
            up: up.clone(),
        }),
    );
    two.add_peers([1, 2]);
    let events = Arc::new(Mutex::new(Vec::new()));
    let seen = events.clone();
    one.listen(Arc::new(move |node, alive| seen.lock().unwrap().push((node, alive))));
    one.add_peers([1, 2, 3]);
    assert_eq!(one.alive().into_iter().collect::<Vec<_>>(), [2, 3]);
    // Node 3 never heartbeats: dead after the timeout. Node 2 stays alive.
    wait_for(&events, (3, false), Duration::from_secs(5));
    std::thread::park_timeout(Duration::from_millis(150));
    assert!(!events.lock().unwrap().contains(&(2, false)));
    assert_eq!(one.alive().into_iter().collect::<Vec<_>>(), [2]);
    // Cut node 2 off: declared dead within about timeout + interval.
    up.store(false, Ordering::SeqCst);
    let took = wait_for(&events, (2, false), Duration::from_secs(5));
    assert!(took >= Duration::from_millis(60), "{took:?}");
    // Reconnect: alive again.
    up.store(true, Ordering::SeqCst);
    wait_for(&events, (2, true), Duration::from_secs(5));
    one.stop();
    two.stop();
    *one_slot.lock().unwrap() = None;
}
