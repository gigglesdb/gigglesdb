mod common;
use common::*;
use giggles_consensus::{authority::*, *};
use giggles_core::{OperationId, Principal, TaskId};
use serde_json::json;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
struct Manual(Arc<Mutex<(u64, u64)>>);
impl ClockSource for Manual {
    fn sample(&mut self) -> Result<(u64, u64)> {
        Ok(*self.0.lock().unwrap())
    }
}
fn clock(wall: u64) -> (Manual, LeaderClock<Manual>) {
    let m = Manual(Arc::new(Mutex::new((wall, 0))));
    (m.clone(), LeaderClock::new(m, 0, 10).unwrap())
}
fn binding(n: u8) -> Binding {
    Binding {
        fingerprint: format!("{n:064x}"),
        domain: 42,
        task: TaskId(1),
        principal: Principal(2),
    }
}
fn ctx(id: u64) -> Vec<u8> {
    id.to_be_bytes().to_vec()
}
fn event(c: &mut Cluster, node: u64, id: u64) -> Applied {
    c.applied_events.remove(&(node, ctx(id))).unwrap()
}
fn reload(c: &mut Cluster, expected: u64, bindings: Vec<Binding>) {
    c.nodes
        .get_mut(&1)
        .unwrap()
        .reload_credentials(ctx(900 + expected), expected, bindings)
        .unwrap();
    c.pump();
    assert_eq!(event(c, 1, 900 + expected).reply.code, 0);
}
fn setup() -> (Cluster, Manual, LeaderClock<Manual>) {
    let mut c = Cluster::new(1901);
    c.submit(1, admin(json!({"command":"seed","key":key(0),"row":row()})));
    c.submit(1, admin(json!({"command":"grant","grant":grant(10,10)})));
    c.submit(1,admin(json!({"command":"provision_task_resources","task":1,"limits":{"read_rows":10000,"disclosed_rows":20,"encoded_bytes":100000}})));
    let (m, mut clock) = clock(10);
    c.nodes
        .get_mut(&1)
        .unwrap()
        .establish_time(ctx(800), &mut clock)
        .unwrap();
    c.pump();
    reload(&mut c, 0, vec![binding(1)]);
    (c, m, clock)
}
fn decision(c: &mut Cluster, clock: &mut LeaderClock<Manual>, id: u64) -> node::HeldOutput {
    let session = c.nodes[&1].authenticate(&binding(1).fingerprint).unwrap();
    c.nodes
        .get_mut(&1)
        .unwrap()
        .decide(
            ctx(id),
            &session,
            TaskAction::Execute(request(id, &[0]), None),
            clock,
        )
        .unwrap();
    c.pump();
    let a = event(c, 1, id);
    c.nodes[&1].hold(a).unwrap()
}
#[test]
fn committed_reload_atomic_overlap_revoke_aba_and_stale_queue() {
    let (mut c, _, mut clock) = setup();
    let old = c.nodes[&1].authenticate(&binding(1).fingerprint).unwrap();
    let before = c.nodes[&1].store.authority().unwrap();
    for bad in [
        vec![binding(1), binding(1)],
        vec![Binding {
            domain: 43,
            ..binding(2)
        }],
        vec![Binding {
            fingerprint: "BAD".into(),
            ..binding(2)
        }],
        vec![binding(2); MAX_BINDINGS + 1],
    ] {
        assert!(
            c.nodes
                .get_mut(&1)
                .unwrap()
                .reload_credentials(ctx(99), 1, bad)
                .is_err()
        );
        assert_eq!(c.nodes[&1].store.authority().unwrap(), before);
    }
    // Reload is queued first; old epoch still visible when the decision is queued.
    c.nodes
        .get_mut(&1)
        .unwrap()
        .reload_credentials(ctx(100), 1, vec![binding(1), binding(2)])
        .unwrap();
    c.nodes
        .get_mut(&1)
        .unwrap()
        .decide(
            ctx(101),
            &old,
            TaskAction::Execute(request(101, &[0]), None),
            &mut clock,
        )
        .unwrap();
    c.pump();
    assert_ne!(event(&mut c, 1, 101).reply.code, 0);
    assert!(
        c.nodes[&1]
            .store
            .image()
            .unwrap()
            .snapshot()
            .unwrap()
            .operations
            .is_empty()
    );
    assert!(c.nodes[&1].authenticate(&binding(2).fingerprint).is_ok());
    assert!(c.nodes[&1].authenticate(&binding(3).fingerprint).is_err());
    assert!(
        c.nodes
            .get_mut(&1)
            .unwrap()
            .decide(
                ctx(102),
                &old,
                TaskAction::Status(OperationId(101)),
                &mut clock
            )
            .is_err()
    );
    reload(&mut c, 2, vec![binding(2)]);
    reload(&mut c, 3, vec![binding(1), binding(2)]);
    assert!(
        c.nodes
            .get_mut(&1)
            .unwrap()
            .decide(
                ctx(103),
                &old,
                TaskAction::Status(OperationId(101)),
                &mut clock
            )
            .is_err()
    );
    // Concurrent local reloads use compare-and-set at deterministic apply.
    for id in [104, 105] {
        c.nodes
            .get_mut(&1)
            .unwrap()
            .reload_credentials(ctx(id), 4, vec![binding(1)])
            .unwrap();
    }
    c.pump();
    assert_eq!(event(&mut c, 1, 104).reply.code, 0);
    assert_ne!(event(&mut c, 1, 105).reply.code, 0);
    c.equal();
}
#[test]
fn held_release_fresh_authority_without_double_charge_and_denial_keeps_commit() {
    let (mut c, _, mut clock) = setup();
    let held = decision(&mut c, &mut clock, 110);
    let used = c.nodes[&1]
        .store
        .image()
        .unwrap()
        .accounting(TaskId(1))
        .unwrap()
        .unwrap()
        .used;
    c.nodes
        .get_mut(&1)
        .unwrap()
        .request_release(ctx(111), &held, &mut clock)
        .unwrap();
    c.pump();
    let a = event(&mut c, 1, 111);
    let proof = c.nodes[&1].release_proof(a).unwrap();
    let reply = c.nodes[&1].finish_release(held, proof, &mut clock).unwrap();
    assert_eq!(reply.data["code"], "committed");
    assert_eq!(
        c.nodes[&1]
            .store
            .image()
            .unwrap()
            .accounting(TaskId(1))
            .unwrap()
            .unwrap()
            .used,
        used
    );
    let held = decision(&mut c, &mut clock, 112);
    c.nodes
        .get_mut(&1)
        .unwrap()
        .request_release(ctx(113), &held, &mut clock)
        .unwrap();
    c.pump();
    let a = event(&mut c, 1, 113);
    let proof = c.nodes[&1].release_proof(a).unwrap();
    c.submit(1, admin(json!({"command":"revoke","task":1})));
    assert!(c.nodes[&1].finish_release(held, proof, &mut clock).is_err());
    assert_eq!(
        c.nodes[&1]
            .store
            .image()
            .unwrap()
            .snapshot()
            .unwrap()
            .operations
            .len(),
        2
    );
    c.equal();
}
#[test]
fn held_epoch_fence_snapshot_restart_and_new_leader_time() {
    let (mut c, _, mut clock) = setup();
    let held = decision(&mut c, &mut clock, 120);
    reload(&mut c, 1, vec![]);
    reload(&mut c, 2, vec![binding(1)]);
    assert!(
        c.nodes
            .get_mut(&1)
            .unwrap()
            .request_release(ctx(121), &held, &mut clock)
            .is_err()
    );
    let authority = c.nodes[&1].store.authority().unwrap();
    c.checkpoint_all();
    for id in VOTERS {
        c.restart(id);
        assert_eq!(c.nodes[&id].store.authority().unwrap(), authority);
    }
    c.nodes.get_mut(&1).unwrap().campaign().unwrap();
    c.pump();
    let session = c.nodes[&1].authenticate(&binding(1).fingerprint).unwrap();
    assert!(
        c.nodes
            .get_mut(&1)
            .unwrap()
            .decide(
                ctx(122),
                &session,
                TaskAction::Status(OperationId(120)),
                &mut clock
            )
            .is_err()
    );
    c.nodes
        .get_mut(&1)
        .unwrap()
        .establish_time(ctx(123), &mut clock)
        .unwrap();
    c.pump();
    c.nodes
        .get_mut(&1)
        .unwrap()
        .decide(
            ctx(124),
            &session,
            TaskAction::Status(OperationId(120)),
            &mut clock,
        )
        .unwrap();
    c.pump();
    assert_eq!(event(&mut c, 1, 124).reply.data["code"], "committed");
    assert!(
        c.nodes
            .get_mut(&1)
            .unwrap()
            .request_release(ctx(125), &held, &mut clock)
            .is_err()
    );
    c.equal();
}
#[test]
fn runtime_clock_failures_latch_before_committed_decisions() {
    for (wall, mono) in [(9, 1), (10, 11), (100, 1), (u64::MAX, 1)] {
        let (mut c, m, mut clock) = setup();
        let held = decision(&mut c, &mut clock, 130);
        *m.0.lock().unwrap() = (wall, mono);
        assert!(
            c.nodes
                .get_mut(&1)
                .unwrap()
                .request_release(ctx(131), &held, &mut clock)
                .is_err()
        );
        *m.0.lock().unwrap() = (10, 0);
        assert!(
            c.nodes
                .get_mut(&1)
                .unwrap()
                .request_release(ctx(132), &held, &mut clock)
                .is_err()
        );
        assert_eq!(
            c.nodes[&1]
                .store
                .image()
                .unwrap()
                .snapshot()
                .unwrap()
                .operations
                .len(),
            1
        );
    }
}
#[test]
fn delayed_release_expiry_and_wrong_committed_principal() {
    let (mut c, m, mut clock) = setup();
    let held = decision(&mut c, &mut clock, 140);
    c.nodes
        .get_mut(&1)
        .unwrap()
        .request_release(ctx(141), &held, &mut clock)
        .unwrap();
    c.pump();
    let a = event(&mut c, 1, 141);
    let proof = c.nodes[&1].release_proof(a).unwrap();
    *m.0.lock().unwrap() = (16, 6);
    assert!(c.nodes[&1].finish_release(held, proof, &mut clock).is_err());
    reload(
        &mut c,
        1,
        vec![Binding {
            principal: Principal(99),
            ..binding(1)
        }],
    );
    let held = decision(&mut c, &mut clock, 142);
    c.nodes
        .get_mut(&1)
        .unwrap()
        .request_release(ctx(143), &held, &mut clock)
        .unwrap();
    c.pump();
    let a = event(&mut c, 1, 143);
    assert!(c.nodes[&1].release_proof(a).is_err());
    assert_eq!(
        c.nodes[&1]
            .store
            .image()
            .unwrap()
            .snapshot()
            .unwrap()
            .operations
            .len(),
        1
    );
}

#[test]
fn committed_release_checks_queued_revoke_and_interval_expiry() {
    for revoke in [true, false] {
        let (mut c, m, mut clock) = setup();
        // Move time gradually in a fresh clock; initial grants expire at1000.
        let (_, mut near) = self::clock(990);
        c.nodes
            .get_mut(&1)
            .unwrap()
            .establish_time(ctx(150), &mut near)
            .unwrap();
        c.pump();
        let held = decision(&mut c, &mut near, 151);
        if revoke {
            c.nodes
                .get_mut(&1)
                .unwrap()
                .propose(ctx(152), &admin(json!({"command":"revoke","task":1})))
                .unwrap();
            c.nodes
                .get_mut(&1)
                .unwrap()
                .request_release(ctx(153), &held, &mut near)
                .unwrap();
        } else {
            let (_, mut expired) = self::clock(1001);
            c.nodes
                .get_mut(&1)
                .unwrap()
                .request_release(ctx(153), &held, &mut expired)
                .unwrap();
        }
        c.pump();
        let a = event(&mut c, 1, 153);
        assert!(c.nodes[&1].release_proof(a).is_err());
        assert_eq!(
            c.nodes[&1]
                .store
                .image()
                .unwrap()
                .snapshot()
                .unwrap()
                .operations
                .len(),
            1
        );
        // Original clock cannot authorize against newer durable watermark.
        *m.0.lock().unwrap() = (10, 0);
        assert!(
            c.nodes
                .get_mut(&1)
                .unwrap()
                .request_release(ctx(154), &held, &mut clock)
                .is_err()
        );
    }
}

#[test]
fn queued_old_time_decision_is_rejected_after_committed_watermark() {
    let (mut c, _, mut clock) = setup();
    let session = c.nodes[&1].authenticate(&binding(1).fingerprint).unwrap();
    let (_, mut future) = self::clock(100);
    c.nodes
        .get_mut(&1)
        .unwrap()
        .establish_time(ctx(160), &mut future)
        .unwrap();
    c.nodes
        .get_mut(&1)
        .unwrap()
        .decide(
            ctx(161),
            &session,
            TaskAction::Execute(request(161, &[0]), None),
            &mut clock,
        )
        .unwrap();
    c.pump();
    assert_ne!(event(&mut c, 1, 161).reply.code, 0);
    assert!(
        c.nodes[&1]
            .store
            .image()
            .unwrap()
            .snapshot()
            .unwrap()
            .operations
            .is_empty()
    );
    let (_, mut behind) = self::clock(10);
    assert!(
        c.nodes
            .get_mut(&1)
            .unwrap()
            .establish_time(ctx(162), &mut behind)
            .is_err()
    );
    c.equal();
}

#[test]
fn release_proof_cannot_cross_new_leadership_term_or_reload() {
    for reload_map in [true, false] {
        let (mut c, _, mut clock) = setup();
        let held = decision(&mut c, &mut clock, 170);
        c.nodes
            .get_mut(&1)
            .unwrap()
            .request_release(ctx(171), &held, &mut clock)
            .unwrap();
        c.pump();
        let a = event(&mut c, 1, 171);
        let proof = c.nodes[&1].release_proof(a).unwrap();
        if reload_map {
            reload(&mut c, 1, vec![binding(1)]);
        } else {
            for id in VOTERS {
                c.restart(id);
            }
            c.nodes.get_mut(&1).unwrap().campaign().unwrap();
            c.pump();
            assert!(c.nodes[&1].is_leader());
            c.nodes
                .get_mut(&1)
                .unwrap()
                .establish_time(ctx(172), &mut clock)
                .unwrap();
            c.pump();
        }
        assert!(c.nodes[&1].finish_release(held, proof, &mut clock).is_err());
    }
}

#[test]
fn credential_and_watermark_metadata_transfer_in_snapshot_to_lagging_voter() {
    let (mut c, _, mut clock) = setup();
    drop(c.nodes.remove(&3));
    reload(&mut c, 1, vec![binding(1), binding(2)]);
    let held = decision(&mut c, &mut clock, 180);
    c.nodes[&1].store.checkpoint().unwrap();
    c.nodes[&2].store.checkpoint().unwrap();
    c.restart(3);
    for _ in 0..20 {
        c.nodes.get_mut(&1).unwrap().deterministic_tick();
        c.pump();
    }
    assert_eq!(
        c.nodes[&3].store.authority().unwrap(),
        c.nodes[&1].store.authority().unwrap()
    );
    assert_eq!(
        c.nodes[&3].store.image().unwrap(),
        c.nodes[&1].store.image().unwrap()
    );
    assert!(c.nodes[&3].authenticate(&binding(2).fingerprint).is_ok());
    c.nodes
        .get_mut(&1)
        .unwrap()
        .request_release(ctx(181), &held, &mut clock)
        .unwrap();
    c.pump();
    c.equal();
}

#[test]
fn clock_uncertainty_bounds_underflow_and_source_failure_deny_without_log() {
    struct Broken;
    impl ClockSource for Broken {
        fn sample(&mut self) -> Result<(u64, u64)> {
            Err("calibration unavailable".into())
        }
    }
    assert!(LeaderClock::new(Manual(Arc::new(Mutex::new((10, 0)))), 61, 10).is_err());
    assert!(LeaderClock::new(Manual(Arc::new(Mutex::new((10, 0)))), 0, 0).is_err());
    let (mut c, _, _) = setup();
    let before = c.nodes[&1].store.applied().unwrap();
    let mut broken = LeaderClock::new(Broken, 0, 10).unwrap();
    assert!(
        c.nodes
            .get_mut(&1)
            .unwrap()
            .establish_time(ctx(190), &mut broken)
            .is_err()
    );
    let mut underflow = LeaderClock::new(Manual(Arc::new(Mutex::new((0, 0)))), 1, 10).unwrap();
    assert!(
        c.nodes
            .get_mut(&1)
            .unwrap()
            .establish_time(ctx(191), &mut underflow)
            .is_err()
    );
    c.pump();
    assert_eq!(c.nodes[&1].store.applied().unwrap(), before);
}

#[test]
fn periodic_idle_observation_keeps_clock_live_but_actor_pause_fences() {
    let (mut c, sample, mut clock) = setup();
    for elapsed in 1..=30 {
        *sample.0.lock().unwrap() = (10 + elapsed, elapsed);
        clock
            .interval(c.nodes[&1].store.authority().unwrap().watermark, 0)
            .unwrap();
    }
    assert!(!clock.is_fenced());
    let held = decision(&mut c, &mut clock, 200);
    *sample.0.lock().unwrap() = (51, 41);
    assert!(
        c.nodes
            .get_mut(&1)
            .unwrap()
            .request_release(ctx(201), &held, &mut clock)
            .is_err()
    );
    assert!(clock.is_fenced());
    assert_eq!(
        c.nodes[&1]
            .store
            .image()
            .unwrap()
            .snapshot()
            .unwrap()
            .operations
            .len(),
        1
    );
}

#[test]
fn native_deadline_gate_after_serialization_never_submits_or_charges() {
    let (mut c, _, mut clock) = setup();
    let session = c.nodes[&1].authenticate(&binding(1).fingerprint).unwrap();
    let before = c.nodes[&1].store.image().unwrap();
    let called = std::cell::Cell::new(false);
    assert!(
        c.nodes
            .get_mut(&1)
            .unwrap()
            .decide_when(
                ctx(991),
                &session,
                TaskAction::Execute(request(991, &[0]), None),
                &mut clock,
                || {
                    called.set(true);
                    false
                }
            )
            .is_err()
    );
    assert!(called.get());
    c.pump();
    assert!(!c.applied_events.contains_key(&(1, ctx(991))));
    assert_eq!(c.nodes[&1].store.image().unwrap(), before);
    let held = decision(&mut c, &mut clock, 992);
    let before = c.nodes[&1].store.image().unwrap();
    assert!(
        c.nodes
            .get_mut(&1)
            .unwrap()
            .request_release_when(ctx(993), &held, &mut clock, || false)
            .is_err()
    );
    c.pump();
    assert!(!c.applied_events.contains_key(&(1, ctx(993))));
    assert_eq!(c.nodes[&1].store.image().unwrap(), before);
}
#[test]
fn final_release_resamples_after_core_validation_and_fences_pause() {
    struct Jump(u8);
    impl ClockSource for Jump {
        fn sample(&mut self) -> Result<(u64, u64)> {
            self.0 += 1;
            Ok(if self.0 == 1 { (10, 0) } else { (100, 90) })
        }
    }
    let (mut c, _, mut clock) = setup();
    let held = decision(&mut c, &mut clock, 994);
    c.nodes
        .get_mut(&1)
        .unwrap()
        .request_release(ctx(995), &held, &mut clock)
        .unwrap();
    c.pump();
    let applied = event(&mut c, 1, 995);
    let proof = c.nodes[&1].release_proof(applied).unwrap();
    let used = c.nodes[&1]
        .store
        .image()
        .unwrap()
        .accounting(TaskId(1))
        .unwrap()
        .unwrap()
        .used;
    let mut jumping = LeaderClock::new(Jump(0), 0, 10).unwrap();
    assert!(
        c.nodes[&1]
            .finish_release(held, proof, &mut jumping)
            .is_err()
    );
    assert!(jumping.is_fenced());
    assert_eq!(
        c.nodes[&1]
            .store
            .image()
            .unwrap()
            .accounting(TaskId(1))
            .unwrap()
            .unwrap()
            .used,
        used
    );
    assert_eq!(
        c.nodes[&1].store.image().unwrap().snapshot().unwrap().tasks[&TaskId(1)].transitions,
        1
    );
}
