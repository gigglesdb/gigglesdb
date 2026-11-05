mod common;
use common::*;
use giggles_distributed::{runtime::Observation, transaction::*, *};
use giggles_transactions::{Outcome, Phase};
use std::collections::BTreeMap;

struct Rig {
    machines: BTreeMap<u64, Machine>,
    index: u64,
}

#[test]
fn generation_capacity_is_reserved_across_disjoint_prepared_writes() {
    use bincode::Options;
    use giggles_core::replica::Image;
    for remaining in [1u64, 2] {
        let mut r = Rig::new();
        // Canonical core relational record: u32 format followed by u64 generation.
        // Validate the resulting image through the public decoder, not unchecked state.
        let codec = bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .with_big_endian()
            .reject_trailing_bytes();
        for machine in r.machines.values_mut() {
            let Machine::Range { data, .. } = machine else {
                panic!()
            };
            let mut records: BTreeMap<Vec<u8>, Vec<u8>> =
                codec.deserialize(data.image.bytes()).unwrap();
            records.get_mut(&vec![5]).unwrap()[4..12]
                .copy_from_slice(&(u64::MAX - remaining).to_be_bytes());
            data.image = Image::from_bytes(codec.serialize(&records).unwrap()).unwrap();
        }
        let a = r.plan(601);
        let mut b = r.plan(602);
        for participant in &mut b.participants {
            participant.writes[0].key.row.0 += 1;
        }
        r.begin(&a);
        r.lock(&a);
        r.prepare(&a);
        r.begin(&b);
        if remaining == 1 {
            // Admission must reject before B can prepare or obtain a commit decision.
            r.refused(&b.home, Step::Lock { id: b.id() }, vec![]);
            r.decide(&b, Outcome::Abort);
            r.finalize(&b);
        } else {
            r.lock(&b);
            r.prepare(&b);
            r.decide(&b, Outcome::Commit);
        }
        r.decide(&a, Outcome::Commit);
        r.finalize(&a);
        if remaining == 2 {
            r.finalize(&b);
            r.finalize(&b);
        }
        for participant in &a.participants {
            assert_eq!(
                r.data(&participant.route)
                    .image
                    .snapshot()
                    .unwrap()
                    .rows
                    .len(),
                remaining as usize
            );
        }
    }
}

#[test]
fn terminal_transaction_identity_replays_on_retained_fenced_home_but_new_work_fails() {
    let mut r = Rig::new();
    let p = r.plan(501);
    r.begin(&p);
    r.lock(&p);
    r.prepare(&p);
    r.decide(&p, Outcome::Commit);
    r.finalize(&p);
    let mut next = split();
    next.id = 88;
    next.parent = next.left.clone();
    next.key = key(25);
    next.left = next.parent.clone();
    next.left.range = 50;
    next.left.group = 5;
    next.left.epoch += 1;
    next.left.bounds.end = Some(next.key);
    next.right = next.parent.clone();
    next.right.range = 51;
    next.right.group = 6;
    next.right.epoch += 1;
    next.right.bounds.start = next.key;
    r.index += 1;
    let reply = r
        .machines
        .get_mut(&3)
        .unwrap()
        .apply(&command(3, Action::Fence(next)), r.index);
    assert_eq!(reply.code, 0, "{:?}", reply.data);
    let children = r.data(&p.home).children().unwrap();
    for child in children {
        assert!(
            !child
                .transactions
                .unresolved(42, &child.descriptor)
                .unwrap()
        );
        assert!(child.transactions.records.is_empty());
    }
    r.ok(&p.home, Step::Open(p.clone()), vec![]);
    let mut other = p.clone();
    other.nonce += 1;
    r.refused(&p.home, Step::Open(other), vec![]);
    assert_eq!(
        r.data(&p.home)
            .transactions
            .engine(42, &p.home)
            .unwrap()
            .outcome(p.id())
            .unwrap()
            .outcome,
        Outcome::Commit
    );
}
impl Rig {
    fn new() -> Self {
        let mut parent = initial(2);
        assert_eq!(parent.apply(&command(2, Action::Fence(split())), 1).code, 0);
        let Machine::Range { data, .. } = parent else {
            panic!()
        };
        let mut machines = BTreeMap::new();
        for child in data.children().unwrap() {
            let group = child.descriptor.group;
            let proof = child.provenance.clone().unwrap();
            let mut m = Machine::Vacant { domain: 42, group };
            assert_eq!(
                m.apply(&command(group, Action::Bootstrap(Box::new(child))), 2)
                    .code,
                0
            );
            assert_eq!(
                m.apply(&command(group, Action::Activate { proof, revision: 2 }), 3)
                    .code,
                0
            );
            machines.insert(group, m);
        }
        Self { machines, index: 3 }
    }
    fn plan(&self, nonce: u128) -> Plan {
        let s = split();
        Plan {
            database: 42,
            nonce,
            home: s.left.clone(),
            participants: vec![
                Participant {
                    route: s.left,
                    writes: vec![Write {
                        key: key(1),
                        before: None,
                        row: row(),
                    }],
                    reads: vec![],
                },
                Participant {
                    route: s.right,
                    writes: vec![Write {
                        key: key(60),
                        before: None,
                        row: row(),
                    }],
                    reads: vec![],
                },
            ],
        }
    }
    fn apply(
        &mut self,
        route: &Descriptor,
        step: Step,
        evidence: Vec<Pinned>,
    ) -> giggles_core::local::Reply {
        self.index += 1;
        self.machines.get_mut(&route.group).unwrap().apply(
            &command(
                route.group,
                Action::Transaction(Box::new(Request {
                    route: route.clone(),
                    step,
                    evidence,
                })),
            ),
            self.index,
        )
    }
    fn ok(&mut self, r: &Descriptor, s: Step, e: Vec<Pinned>) {
        let reply = self.apply(r, s, e);
        assert_eq!(reply.code, 0, "{:?}", reply.data);
    }
    fn refused(&mut self, r: &Descriptor, s: Step, e: Vec<Pinned>) {
        let old = self.machines[&r.group].clone();
        let reply = self.apply(r, s, e);
        assert_ne!(reply.code, 0, "{:?}", reply.data);
        assert_eq!(self.machines[&r.group], old);
    }
    fn pin(&self, r: &Descriptor, id: giggles_transactions::TransactionId) -> Pinned {
        Pinned::from_current(Observation {
            membership: giggles_membership::Membership::new(
                giggles_membership::Identity {
                    group: r.group,
                    owner_epoch: 1,
                },
                vec![1, 2, 3],
                64,
            )
            .unwrap(),
            term: 1,
            snapshot_index: 0,
            is_leader: true,
            quorum_checked: true,
            group: r.group,
            leader: 1,
            applied: self.index,
            machine: self.machines[&r.group].clone(),
            reply: None,
        })
        .unwrap()
        .for_transaction(id)
    }
    fn data(&self, r: &Descriptor) -> &Data {
        let Machine::Range { data, .. } = &self.machines[&r.group] else {
            panic!()
        };
        data
    }
    fn begin(&mut self, p: &Plan) {
        let id = p.id();
        self.ok(&p.home, Step::Open(p.clone()), vec![]);
        let h = self.pin(&p.home, id);
        for part in &p.participants {
            self.ok(&part.route, Step::Begin { id }, vec![h.clone()]);
        }
    }
    fn lock(&mut self, p: &Plan) {
        for part in &p.participants {
            self.ok(&part.route, Step::Lock { id: p.id() }, vec![]);
        }
    }
    fn prepare(&mut self, p: &Plan) {
        let pins = p
            .participants
            .iter()
            .map(|part| self.pin(&part.route, p.id()))
            .collect::<Vec<_>>();
        for part in &p.participants {
            self.ok(&part.route, Step::Prepare { id: p.id() }, pins.clone());
        }
    }
    fn decide(&mut self, p: &Plan, outcome: Outcome) {
        if outcome == Outcome::Commit {
            for part in &p.participants {
                let pin = self.pin(&part.route, p.id());
                self.ok(
                    &p.home,
                    Step::Vote {
                        id: p.id(),
                        participant: part.route.clone(),
                    },
                    vec![pin],
                );
            }
        }
        self.ok(
            &p.home,
            Step::Decide {
                id: p.id(),
                outcome,
            },
            vec![],
        );
    }
    fn finalize(&mut self, p: &Plan) {
        let h = self.pin(&p.home, p.id());
        for part in &p.participants {
            self.ok(&part.route, Step::Finalize { id: p.id() }, vec![h.clone()]);
        }
    }
}

#[test]
fn typed_adapter_requires_all_locks_and_exact_pinned_domains_indices_and_bodies() {
    let mut r = Rig::new();
    let p = r.plan(1);
    let id = p.id();
    r.begin(&p);
    r.ok(&p.home, Step::Lock { id }, vec![]);
    let pins = p
        .participants
        .iter()
        .map(|p| r.pin(&p.route, id))
        .collect::<Vec<_>>();
    r.refused(&p.home, Step::Prepare { id }, pins);
    r.ok(&p.participants[1].route, Step::Lock { id }, vec![]);
    let good = p
        .participants
        .iter()
        .map(|p| r.pin(&p.route, id))
        .collect::<Vec<_>>();
    for variant in 0..6 {
        let mut bad = good.clone();
        match variant {
            0 => bad[0].database = 99,
            1 => bad[0].route.epoch += 1,
            2 => bad[0].applied = 0,
            3 => bad[0].applied = 1,
            4 => bad[1] = bad[0].clone(),
            5 => {
                bad[0].state.records[0].plan.participants[0].writes[0]
                    .row
                    .attempt_count += 1
            }
            _ => unreachable!(),
        }
        r.refused(&p.home, Step::Prepare { id }, bad);
    }
    r.prepare(&p);
    r.refused(
        &p.home,
        Step::Decide {
            id,
            outcome: Outcome::Commit,
        },
        vec![],
    );
    r.refused(
        &p.participants[1].route,
        Step::Decide {
            id,
            outcome: Outcome::Abort,
        },
        vec![],
    );
    let stale_home = r.pin(&p.home, id);
    r.refused(
        &p.participants[1].route,
        Step::Finalize { id },
        vec![stale_home],
    );
    let mut conflict = p.clone();
    conflict.participants[0].writes[0].row.failed_at += 1;
    r.refused(&p.home, Step::Open(conflict), vec![]);
    r.decide(&p, Outcome::Commit);
    r.finalize(&p);
    r.finalize(&p);
    for part in &p.participants {
        let data = r.data(&part.route);
        let rows = data.image.snapshot().unwrap().rows;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows.values().next().unwrap().version, 0);
    }
}

#[test]
fn predicate_gap_locks_prevent_phantoms_cas_revalidates_complete_before_images() {
    let mut r = Rig::new();
    let mut reader = r.plan(10);
    for part in &mut reader.participants {
        part.writes.clear();
        part.reads = vec![Predicate {
            bounds: part.route.bounds.clone(),
            expected: Some(vec![]),
        }];
    }
    r.begin(&reader);
    r.lock(&reader);
    let writer = r.plan(11);
    r.begin(&writer);
    r.refused(&writer.home, Step::Lock { id: writer.id() }, vec![]);
    r.index += 1;
    let put = command(
        writer.home.group,
        put(writer.home.clone(), 99, key(2), None),
    );
    assert_ne!(
        r.machines
            .get_mut(&writer.home.group)
            .unwrap()
            .apply(&put, r.index)
            .code,
        0
    );
    r.decide(&reader, Outcome::Abort);
    r.finalize(&reader);
    r.lock(&writer);
    r.prepare(&writer);
    r.decide(&writer, Outcome::Commit);
    r.finalize(&writer);
    let mut stale = reader.clone();
    stale.nonce = 12;
    r.begin(&stale);
    r.refused(&stale.home, Step::Lock { id: stale.id() }, vec![]);
    r.decide(&stale, Outcome::Abort);
    r.finalize(&stale);
    let mut cas = writer.clone();
    cas.nonce = 13;
    for part in &mut cas.participants {
        let w = &mut part.writes[0];
        w.before = r
            .data(&part.route)
            .image
            .snapshot()
            .unwrap()
            .rows
            .get(&w.key)
            .cloned();
    }
    // Same version, different before-image body must still fail.
    cas.participants[0].writes[0]
        .before
        .as_mut()
        .unwrap()
        .attempt_count = 999;
    r.begin(&cas);
    r.refused(&cas.home, Step::Lock { id: cas.id() }, vec![]);
}

#[test]
fn opposite_lock_order_never_prepares_a_cycle_and_home_abort_releases_every_lock() {
    let mut r = Rig::new();
    let a = r.plan(20);
    let b = r.plan(21);
    r.begin(&a);
    r.begin(&b);
    r.ok(&a.home, Step::Lock { id: a.id() }, vec![]);
    r.ok(&b.participants[1].route, Step::Lock { id: b.id() }, vec![]);
    r.refused(&a.participants[1].route, Step::Lock { id: a.id() }, vec![]);
    r.refused(&b.home, Step::Lock { id: b.id() }, vec![]);
    r.decide(&a, Outcome::Abort);
    r.finalize(&a);
    r.decide(&b, Outcome::Abort);
    r.finalize(&b);
    let next = r.plan(22);
    r.begin(&next);
    r.lock(&next);
    r.prepare(&next);
    r.decide(&next, Outcome::Commit);
    r.finalize(&next);
}

#[test]
fn exhausted_admission_preserves_prepared_resolution_and_real_split_guard() {
    let mut r = Rig::new();
    let p = r.plan(30);
    r.begin(&p);
    r.lock(&p);
    r.prepare(&p);
    let mut exhausted = false;
    for n in 1..=MAX_TRANSACTIONS {
        let more = r.plan(30 + n as u128);
        let old = r.machines[&more.home.group].clone();
        if r.apply(&more.home, Step::Open(more.clone()), vec![]).code != 0 {
            assert_eq!(r.machines[&more.home.group], old);
            exhausted = true;
            break;
        }
    }
    assert!(
        exhausted,
        "reservation or record ceiling must stop admission"
    );
    let extra = r.plan(999);
    r.refused(&extra.home, Step::Open(extra.clone()), vec![]);
    let s = split();
    let mut next = s.clone();
    next.id = 88;
    next.parent = s.left.clone();
    next.key = key(25);
    next.left = next.parent.clone();
    next.left.range = 50;
    next.left.group = 5;
    next.left.epoch += 1;
    next.left.bounds.end = Some(next.key);
    next.right = next.parent.clone();
    next.right.range = 51;
    next.right.group = 6;
    next.right.epoch += 1;
    next.right.bounds.start = next.key;
    r.index += 1;
    assert_ne!(
        r.machines
            .get_mut(&3)
            .unwrap()
            .apply(&command(3, Action::Fence(next)), r.index)
            .code,
        0
    );
    assert_eq!(
        r.data(&p.home)
            .transactions
            .engine(42, &p.home)
            .unwrap()
            .phase(p.id()),
        Some(Phase::Prepared)
    );
    r.decide(&p, Outcome::Commit);
    r.finalize(&p);
    assert_eq!(r.data(&p.home).image.snapshot().unwrap().rows.len(), 1);
    // Resolution succeeded even though new home admission remained exhausted.
    r.refused(&extra.home, Step::Open(extra.clone()), vec![]);
}

#[test]
fn typed_plan_rejects_unbounded_duplicate_unsealed_or_overlapping_effect_sets() {
    let r = Rig::new();
    let p = r.plan(77);
    p.validate().unwrap();
    for variant in 0..8 {
        let mut bad = p.clone();
        match variant {
            0 => bad.nonce = 0,
            1 => bad.database = 0,
            2 => bad.participants.reverse(),
            3 => bad.participants.push(bad.participants[0].clone()),
            4 => {
                let duplicate = bad.participants[0].writes[0].clone();
                bad.participants[0].writes.push(duplicate);
            }
            5 => bad.participants[0].writes[0].key = key(99),
            6 => bad.participants[1].route.bounds = bad.participants[0].route.bounds.clone(),
            7 => bad.home = descriptor(),
            _ => unreachable!(),
        }
        assert!(bad.validate().is_err());
    }
}
