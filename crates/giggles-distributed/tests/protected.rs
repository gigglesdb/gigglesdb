mod common;
mod protected_support;
use giggles_core::{ApprovalId, Principal, Status, TaskId, local::AdminCommand, replica};
use giggles_distributed::{
    protected::{self as p, Step},
    *,
};
use protected_support::{image, key, plan, row};
use std::collections::BTreeMap;

struct Rig {
    machines: BTreeMap<u64, Machine>,
    index: u64,
}
impl Rig {
    fn new() -> Self {
        Self::with_limit(2)
    }
    fn with_limit(limit: u64) -> Self {
        Self::with_read_rows(limit, 1000)
    }
    fn with_read_rows(limit: u64, read_rows: u64) -> Self {
        let split = common::split();
        let mut root = Machine::root(42, common::descriptor()).unwrap();
        if let Machine::Root { directory, .. } = &mut root {
            directory.routes = vec![split.left.clone(), split.right.clone()];
            directory.revision = 2;
        }
        let mut r = Self {
            machines: BTreeMap::from([
                (1, root),
                (3, Machine::range(42, split.left).unwrap()),
                (4, Machine::range(42, split.right).unwrap()),
            ]),
            index: 0,
        };
        for (g, k) in [(3, key(1)), (4, key(60))] {
            let route = r.data(g).descriptor.clone();
            r.ok_action(
                g,
                Action::Put {
                    route,
                    operation: g,
                    key: k,
                    expected: None,
                    row: row(),
                },
            );
        }
        r.ok(
            1,
            Step::Install {
                image: protected_support::image_with_read_rows(42, limit, read_rows),
            },
        );
        r
    }
    fn plan(&self, n: u128) -> p::Plan {
        let Machine::Root { directory, .. } = &self.machines[&1] else {
            panic!()
        };
        plan(directory.routes.clone(), directory.revision, n)
    }
    fn data(&self, group: u64) -> &Data {
        let Machine::Range { data, .. } = &self.machines[&group] else {
            panic!()
        };
        data
    }
    fn home(&self) -> &p::Home {
        let Machine::Root { protected, .. } = &self.machines[&1] else {
            panic!()
        };
        protected
    }
    fn pin(&self, g: u64) -> p::Pin {
        p::Pin {
            database: 42,
            group: g,
            applied: self.index,
            machine: self.machines[&g].clone(),
        }
    }
    fn run(&mut self, g: u64, a: Action) -> giggles_core::local::Reply {
        self.index += 1;
        self.machines.get_mut(&g).unwrap().apply(
            &Command {
                domain: 42,
                group: g,
                action: a,
            },
            self.index,
        )
    }
    fn ok_action(&mut self, g: u64, a: Action) {
        let r = self.run(g, a);
        assert_eq!(r.code, 0, "{:?}", r.data);
    }
    fn ok(&mut self, g: u64, s: Step) {
        self.ok_action(g, Action::Protected(Box::new(s)));
    }
    fn bad(&mut self, g: u64, s: Step) {
        let before = self.machines[&g].clone();
        let r = self.run(g, Action::Protected(Box::new(s)));
        assert_ne!(r.code, 0, "{:?}", r.data);
        assert_eq!(self.machines[&g], before);
    }
    fn open(&mut self, n: u128) -> p::Plan {
        let p = self.plan(n);
        self.ok(1, Step::Open(p.clone()));
        p
    }
    fn lock(&mut self, p: &p::Plan) {
        let h = self.pin(1).for_home(p.nonce).unwrap();
        for route in &p.routes {
            self.ok(
                route.group,
                Step::Lock {
                    plan: p.clone(),
                    home: Box::new(h.clone()),
                },
            );
        }
    }
    fn pins(&self) -> Vec<p::Pin> {
        vec![self.pin(3), self.pin(4)]
    }
    fn releases(&self, nonce: u128) -> Vec<p::Released> {
        self.pins()
            .into_iter()
            .map(|p| p.for_release(nonce).unwrap())
            .collect()
    }
    fn abort(&mut self, p: &p::Plan) {
        self.ok(1, Step::Abort { nonce: p.nonce });
        let home = self.pin(1).for_home(p.nonce).unwrap();
        for route in &p.routes {
            self.ok(
                route.group,
                Step::Unlock {
                    plan: p.clone(),
                    home: Box::new(home.clone()),
                },
            );
        }
        self.ok(
            1,
            Step::Complete {
                nonce: p.nonce,
                rows: self.releases(p.nonce),
            },
        );
    }
}

#[test]
fn unique_authority_rejects_rows_domain_duplicate_and_catalog() {
    let mut r = Rig::new();
    r.ok(
        1,
        Step::Install {
            image: image(42, 2),
        },
    );
    r.bad(
        1,
        Step::Install {
            image: image(43, 2),
        },
    );
    r.bad(
        1,
        Step::Install {
            image: image(42, 3),
        },
    );
    let (rows, _) = image(42, 2)
        .evaluate(&replica::Command::Admin(
            serde_json::to_vec(&AdminCommand::Seed {
                key: key(1),
                row: row(),
            })
            .unwrap(),
        ))
        .unwrap();
    r.bad(1, Step::Install { image: rows });
    let (catalog, reply) = image(42, 2)
        .evaluate(&replica::Command::Admin(
            serde_json::to_vec(&AdminCommand::Sql {
                sql: "CREATE TABLE unsupported (id BIGINT PRIMARY KEY)".into(),
                params: vec![],
                expected_version: None,
            })
            .unwrap(),
        ))
        .unwrap();
    assert_eq!(reply.code, 0, "{:?}", reply.data);
    // Use a pristine uninstalled home so the catalog check itself must fail.
    let mut pristine = Machine::root(42, common::descriptor()).unwrap();
    let before = pristine.clone();
    let refused = pristine.apply(
        &Command {
            domain: 42,
            group: 1,
            action: Action::Protected(Box::new(Step::Install { image: catalog })),
        },
        1,
    );
    assert_ne!(refused.code, 0);
    assert!(refused.data.to_string().contains("catalog"));
    assert_eq!(pristine, before);
    for g in [3, 4] {
        assert!(p::data_only(r.data(g)).unwrap());
        assert!(r.data(g).image.snapshot().unwrap().tasks.is_empty());
    }
}

#[test]
fn complete_coverage_rejects_missing_gap_overlap_stale_revision_and_domain() {
    let mut r = Rig::new();
    let good = r.plan(1);
    for kind in 0..6 {
        let mut p = good.clone();
        match kind {
            0 => {
                p.routes.pop();
            }
            1 => p.routes[1].bounds.start = key(51),
            2 => p.routes[1].bounds.start = key(49),
            3 => p.revision += 1,
            4 => p.database += 1,
            _ => p.routes.swap(0, 1),
        }
        r.bad(1, Step::Open(p));
    }
    r.ok(1, Step::Open(good));
}

#[test]
fn shared_authority_serializes_and_same_id_altered_payload_rejects() {
    let mut r = Rig::new();
    let p = r.open(1);
    r.ok(1, Step::Open(p.clone()));
    for field in 0..5 {
        let mut q = p.clone();
        match field {
            0 => q.intent.principal = Principal(8),
            1 => q.intent.approval = Some(ApprovalId(8)),
            2 => q.intent.request.after = Status::Succeeded,
            3 => q.task = TaskId(9),
            _ => q.intent.upper += 1,
        };
        r.bad(1, Step::Open(q));
    }
    r.bad(1, Step::Open(r.plan(2)));
    r.abort(&p);
    r.ok(1, Step::Open(r.plan(2)));
}

#[test]
fn protected_lock_blocks_direct_put_ordinary_transaction_observe_and_split() {
    let mut r = Rig::new();
    let p = r.open(1);
    r.lock(&p);
    let route = r.data(3).descriptor.clone();
    let result = r.run(
        3,
        Action::Put {
            route: route.clone(),
            operation: 91,
            key: key(2),
            expected: None,
            row: row(),
        },
    );
    assert_ne!(result.code, 0);
    let tp = transaction::Plan {
        database: 42,
        nonce: 7,
        home: route.clone(),
        participants: vec![transaction::Participant {
            route: route.clone(),
            writes: vec![],
            reads: vec![transaction::Predicate {
                bounds: route.bounds.clone(),
                expected: None,
            }],
        }],
    };
    assert_ne!(
        r.run(
            3,
            Action::Transaction(Box::new(transaction::Request {
                route: route.clone(),
                step: transaction::Step::Open(tp),
                evidence: vec![]
            }))
        )
        .code,
        0
    );
    assert_ne!(r.run(1, Action::Begin(common::split())).code, 0);
    let mut split = common::split();
    split.parent = route.clone();
    split.key = key(25);
    split.left = route.clone();
    split.left.range = 20;
    split.left.group = 5;
    split.left.epoch = 3;
    split.left.bounds.end = Some(key(25));
    split.right = split.left.clone();
    split.right.range = 21;
    split.right.group = 6;
    split.right.bounds = Bounds {
        start: key(25),
        end: route.bounds.end,
    };
    assert_ne!(r.run(3, Action::Fence(split.clone())).code, 0);
    r.abort(&p);
    // Terminal identities cannot disappear from projected children either.
    let root_before = r.machines[&1].clone();
    assert_ne!(r.run(1, Action::Begin(split.clone())).code, 0);
    assert_eq!(
        r.machines[&1], root_before,
        "must reject before retaining an unfinishable split intent"
    );
    assert_ne!(r.run(3, Action::Fence(split)).code, 0);
}

#[test]
fn real_evaluator_stage_is_opaque_durable_idempotent_and_has_zero_effects() {
    let mut r = Rig::new();
    let p = r.open(1);
    r.lock(&p);
    let pins = r.pins();
    let authority = r.home().image.clone();
    let rows = [r.data(3).image.clone(), r.data(4).image.clone()];
    r.ok(
        1,
        Step::Stage {
            nonce: 1,
            rows: pins.clone(),
        },
    );
    assert!(r.home().record(1).unwrap().staged.is_some());
    let durable = bincode::serialize(&r.machines).unwrap();
    r.machines = bincode::deserialize(&durable).unwrap();
    r.ok(
        1,
        Step::Stage {
            nonce: 1,
            rows: pins,
        },
    );
    r.bad(1, Step::Commit { nonce: 1 });
    assert_eq!(r.home().image, authority);
    assert_eq!(r.data(3).image, rows[0]);
    assert_eq!(r.data(4).image, rows[1]);
    assert!(r.home().image.snapshot().unwrap().operations.is_empty());
    r.abort(&p);
    assert_eq!(r.home().image, authority);
}

#[test]
fn stage_requires_all_locks_exact_pins_and_refuses_row_conflict() {
    let mut r = Rig::new();
    let p = r.open(1);
    r.bad(
        1,
        Step::Stage {
            nonce: 1,
            rows: r.pins(),
        },
    );
    r.lock(&p);
    for kind in 0..6 {
        let mut pins = r.pins();
        match kind {
            0 => {
                pins.pop();
            }
            1 => pins.swap(0, 1),
            2 => pins[0].database = 43,
            3 => pins[0].applied = 0,
            4 => {
                let Machine::Range { data, .. } = &mut pins[0].machine else {
                    panic!()
                };
                data.protected.records[0].plan.nonce += 1;
            }
            _ => {
                let Machine::Range { data, .. } = &mut pins[0].machine else {
                    panic!()
                };
                data.image = replica::Image::new(42).unwrap();
            }
        }
        r.bad(
            1,
            Step::Stage {
                nonce: 1,
                rows: pins,
            },
        );
    }
}

#[test]
fn revoked_wrong_principal_approval_budget_and_expired_time_fail_real_core() {
    for kind in 0..6 {
        let mut r = Rig::new();
        let mut p = r.plan(1);
        match kind {
            0 => p.intent.principal = Principal(99),
            1 => p.intent.approval = Some(ApprovalId(99)),
            2 => {
                r.ok(
                    1,
                    Step::Admin {
                        command: serde_json::to_vec(&AdminCommand::Revoke { task: TaskId(1) })
                            .unwrap(),
                    },
                );
            }
            3 => {
                p.intent.lower = 1000;
                p.intent.upper = 1001;
            }
            4 => {
                p.intent.lower = 0;
                p.intent.upper = 0;
            }
            _ => {
                p.intent.request.selection =
                    giggles_core::Selection::Keys(vec![key(1), key(60), key(90)]);
            }
        }
        let before = r.home().image.clone();
        r.ok(1, Step::Open(p.clone()));
        r.lock(&p);
        r.bad(
            1,
            Step::Stage {
                nonce: 1,
                rows: r.pins(),
            },
        );
        assert_eq!(r.home().image, before);
        r.abort(&p);
    }
}

#[test]
fn authority_revoke_cannot_overtake_locked_stage() {
    let mut r = Rig::new();
    let p = r.open(1);
    r.lock(&p);
    r.ok(
        1,
        Step::Stage {
            nonce: 1,
            rows: r.pins(),
        },
    );
    let command = serde_json::to_vec(&AdminCommand::Revoke { task: TaskId(1) }).unwrap();
    r.bad(
        1,
        Step::Admin {
            command: command.clone(),
        },
    );
    r.abort(&p);
    r.ok(1, Step::Admin { command });
    assert!(r.home().image.snapshot().unwrap().tasks[&TaskId(1)].revoked);
}

#[test]
fn partial_lock_abort_tombstone_blocks_delayed_lock_and_complete_requires_all() {
    let mut r = Rig::new();
    let p = r.open(1);
    let old = r.pin(1).for_home(p.nonce).unwrap();
    r.ok(
        3,
        Step::Lock {
            plan: p.clone(),
            home: Box::new(old.clone()),
        },
    );
    r.bad(
        4,
        Step::Unlock {
            plan: p.clone(),
            home: Box::new(old.clone()),
        },
    );
    r.ok(1, Step::Abort { nonce: 1 });
    r.bad(
        1,
        Step::Complete {
            nonce: 1,
            rows: vec![],
        },
    );
    let home = r.pin(1).for_home(p.nonce).unwrap();
    for g in [3, 4] {
        r.ok(
            g,
            Step::Unlock {
                plan: p.clone(),
                home: Box::new(home.clone()),
            },
        );
        r.bad(
            g,
            Step::Lock {
                plan: p.clone(),
                home: Box::new(old.clone()),
            },
        );
    }
    r.ok(
        1,
        Step::Complete {
            nonce: 1,
            rows: r.releases(1),
        },
    );
    r.ok(1, Step::Abort { nonce: 1 });
    r.ok(
        1,
        Step::Complete {
            nonce: 1,
            rows: vec![],
        },
    );
    r.ok(1, Step::Open(p));
}

#[test]
fn bounded_retention_stops_admission_and_does_not_evict_retries() {
    let mut r = Rig::new();
    for n in 1..=p::MAX_RECORDS {
        let p = r.open(n as u128);
        r.abort(&p);
    }
    r.bad(1, Step::Open(r.plan(99)));
    r.ok(1, Step::Open(r.plan(1)));
}

#[test]
fn corrupt_stage_bytes_and_duplicate_retained_id_fail_machine_validation() {
    let mut r = Rig::new();
    let p = r.open(1);
    r.lock(&p);
    r.ok(
        1,
        Step::Stage {
            nonce: 1,
            rows: r.pins(),
        },
    );
    let mut root = r.machines[&1].clone();
    let Machine::Root { protected, .. } = &mut root else {
        panic!()
    };
    protected.records[0].staged = Some("00".into());
    assert!(root.validate(42, 1).is_err());
    let mut root = r.machines[&1].clone();
    let Machine::Root { protected, .. } = &mut root else {
        panic!()
    };
    protected.records.push(protected.records[0].clone());
    assert!(root.validate(42, 1).is_err());
}

#[test]
fn durable_format_fences_old_payload_and_reopens_current_stage() {
    use redb::{ReadableDatabase, TableDefinition};
    use sha2::{Digest, Sha256};
    let root = common::fresh("protected-format");
    let current = root.join("current.redb");
    let mut r = Rig::new();
    let p = r.open(1);
    r.lock(&p);
    r.ok(
        1,
        Step::Stage {
            nonce: 1,
            rows: r.pins(),
        },
    );
    let expected = r.machines[&1].clone();
    let store = Store::create(&current, 1, 42, 1, expected.clone()).unwrap();
    drop(store);
    let reopened = Store::open(&current, 1, 42, 1).unwrap();
    assert_eq!(reopened.image().unwrap(), expected);
    drop(reopened);
    let old = root.join("old.redb");
    std::fs::copy(&current, &old).unwrap();
    const T: TableDefinition<u64, &[u8]> = TableDefinition::new("p4_group_v1");
    let db = redb::Database::open(&old).unwrap();
    let bytes = {
        let tx = db.begin_read().unwrap();
        let t = tx.open_table(T).unwrap();
        t.get(0).unwrap().unwrap().value().to_vec()
    };
    assert_eq!(&bytes[32..40], b"GD44v005");
    let mut legacy = Sha256::digest(&bytes[40..]).to_vec();
    legacy.extend_from_slice(&bytes[40..]);
    let tx = db.begin_write().unwrap();
    {
        let mut t = tx.open_table(T).unwrap();
        t.insert(0, legacy.as_slice()).unwrap();
    }
    tx.commit().unwrap();
    drop(db);
    let err = Store::open(&old, 1, 42, 1)
        .err()
        .expect("old format accepted");
    assert!(
        err.to_string().contains("unsupported durable format"),
        "{err}"
    );
    let reopened = Store::open(&current, 1, 42, 1).unwrap();
    assert_eq!(reopened.image().unwrap(), expected);
}

#[test]
fn delayed_owner_messages_state_machine_never_changes_business_state() {
    // Both orders of participant lock, missing/delayed duplicate messages,
    // snapshot/restart between every command, and explicit abort at every phase.
    for seed in 0..64u64 {
        let mut r = Rig::new();
        let p = r.open(100 + seed as u128);
        let admitted = r.pin(1).for_home(p.nonce).unwrap();
        let authority = r.home().image.clone();
        let rows = [r.data(3).image.clone(), r.data(4).image.clone()];
        let mut x = seed + 1;
        for _ in 0..16 {
            x = x.wrapping_mul(6364136223846793005).wrapping_add(1);
            let group = if x & 1 == 0 { 3 } else { 4 };
            let step = match (x >> 8) % 6 {
                0 => Step::Open(p.clone()),
                1 => Step::Stage {
                    nonce: p.nonce,
                    rows: r.pins(),
                },
                2 => Step::Abort { nonce: p.nonce },
                3 => Step::Commit { nonce: p.nonce },
                _ => Step::Complete {
                    nonce: p.nonce,
                    rows: r
                        .pins()
                        .into_iter()
                        .filter_map(|q| q.for_release(p.nonce).ok())
                        .collect(),
                },
            };
            if (x >> 16) & 1 == 0 {
                r.run(
                    group,
                    Action::Protected(Box::new(Step::Lock {
                        plan: p.clone(),
                        home: Box::new(admitted.clone()),
                    })),
                );
            } else {
                let home = r.pin(1).for_home(p.nonce).unwrap();
                r.run(
                    group,
                    Action::Protected(Box::new(Step::Unlock {
                        plan: p.clone(),
                        home: Box::new(home),
                    })),
                );
            }
            r.run(1, Action::Protected(Box::new(step)));
            r.machines = bincode::deserialize(&bincode::serialize(&r.machines).unwrap()).unwrap();
            for (g, m) in &r.machines {
                m.validate(42, *g).unwrap();
            }
            assert_eq!(r.home().image, authority, "seed {seed}");
            assert_eq!(r.data(3).image, rows[0]);
            assert_eq!(r.data(4).image, rows[1]);
        }
        r.abort(&p);
        assert!(!r.home().busy());
        for g in [3, 4] {
            assert!(!r.data(g).protected.busy());
        }
    }
}

#[test]
fn real_business_budget_and_read_resource_exhaustion_leave_authority_unchanged() {
    for resources in [false, true] {
        let mut r = Rig::with_read_rows(
            if resources { 2 } else { 1 },
            if resources { 1 } else { 1000 },
        );
        let p = r.open(1);
        r.lock(&p);
        let before = r.home().image.clone();
        let reply = r.run(
            1,
            Action::Protected(Box::new(Step::Stage {
                nonce: 1,
                rows: r.pins(),
            })),
        );
        assert_ne!(reply.code, 0);
        assert!(
            reply.data.to_string().contains("Budget"),
            "{:?}",
            reply.data
        );
        assert_eq!(r.home().image, before);
        r.abort(&p);
    }
}

#[test]
fn row_home_evidence_contains_no_authority_namespaces_or_staged_bytes() {
    let mut r = Rig::new();
    let p = r.open(1);
    r.lock(&p);
    r.ok(
        1,
        Step::Stage {
            nonce: 1,
            rows: r.pins(),
        },
    );
    let pin = r.pin(1).for_home(1).unwrap();
    let value = serde_json::to_value(&pin).unwrap();
    assert_eq!(
        value
            .as_object()
            .unwrap()
            .keys()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>(),
        [
            "database",
            "applied",
            "plan",
            "opened",
            "aborted",
            "released",
            "committed",
            "all_finalized"
        ]
        .into_iter()
        .map(str::to_owned)
        .collect()
    );
    let encoded = serde_json::to_string(&Step::Lock {
        plan: p,
        home: Box::new(pin),
    })
    .unwrap();
    for forbidden in ["image", "staged", "approvals", "receipts", "grant"] {
        assert!(!encoded.contains(forbidden), "{forbidden}");
    }
}

#[test]
fn abort_completion_proof_is_minimal_and_exactly_binds_every_released_range() {
    let mut r = Rig::new();
    let plan = r.open(1);
    r.lock(&plan);
    r.ok(1, Step::Abort { nonce: 1 });
    let home = r.pin(1).for_home(1).unwrap();
    for route in &plan.routes {
        r.ok(
            route.group,
            Step::Unlock {
                plan: plan.clone(),
                home: Box::new(home.clone()),
            },
        );
    }
    let good = r.releases(1);
    for kind in 0..7 {
        let mut rows = good.clone();
        match kind {
            0 => rows.swap(0, 1),
            1 => rows[0].released = false,
            2 => rows[0].nonce += 1,
            3 => rows[0].plan_digest[0] ^= 1,
            4 => rows[0].applied = 0,
            5 => rows[0].database += 1,
            _ => rows[0].route.epoch += 1,
        }
        r.bad(1, Step::Complete { nonce: 1, rows });
    }
    let wire = serde_json::to_vec(&Step::Complete {
        nonce: 1,
        rows: good.clone(),
    })
    .unwrap();
    assert!(wire.len() < 2048);
    let value = String::from_utf8(wire).unwrap();
    for namespace in ["image", "staged", "transactions", "retries"] {
        assert!(!value.contains(namespace));
    }
    r.ok(
        1,
        Step::Complete {
            nonce: 1,
            rows: good,
        },
    );
}

#[test]
fn uninstalled_authority_validation_requires_exact_pristine_image_and_no_records() {
    let mut root = Machine::root(42, common::descriptor()).unwrap();
    root.validate(42, 1).unwrap();
    let Machine::Root { protected, .. } = &mut root else {
        panic!()
    };
    protected.image = image(42, 2);
    assert!(root.validate(42, 1).is_err());
    let mut r = Rig::new();
    r.open(1);
    let Machine::Root { protected, .. } = r.machines.get_mut(&1).unwrap() else {
        panic!()
    };
    protected.installed = None;
    assert!(r.machines[&1].validate(42, 1).is_err());
}
impl Rig {
    fn logical(limit: u64) -> Self {
        let mut r = Self::with_limit(limit);
        let Machine::Root { protected, .. } = r.machines.get_mut(&1).unwrap() else {
            panic!()
        };
        *protected = p::Home::new(42).unwrap();
        r.lok(
            1,
            p::logical::Step::Install {
                image: image(42, limit),
                profile: p::logical::LogicalProfile { epoch: 1, tick: 12 },
            },
        );
        r
    }
    fn lok(&mut self, g: u64, s: p::logical::Step) {
        self.ok(g, Step::Logical(Box::new(s)));
    }
    fn lbad(&mut self, g: u64, s: p::logical::Step) {
        self.bad(g, Step::Logical(Box::new(s)));
    }
    fn lopen(&mut self, n: u128) -> p::Plan {
        let mut plan = self.plan(n);
        plan.intent.lower = 12;
        self.lok(1, p::logical::Step::Open(plan.clone()));
        self.lock(&plan);
        plan
    }
    fn lstage(&mut self, n: u128) {
        self.ok(
            1,
            Step::Stage {
                nonce: n,
                rows: self.pins(),
            },
        );
    }
    fn lprepare(&mut self, n: u128) -> Vec<p::logical::Prepared> {
        for g in [3, 4] {
            let prep = self.pin(1).logical_preparation(n, g).unwrap();
            self.lok(g, p::logical::Step::Prepare(prep));
        }
        [3, 4]
            .into_iter()
            .map(|g| self.pin(g).logical_prepared(n).unwrap())
            .collect()
    }
    fn lcommit(&mut self, n: u128) -> Vec<p::logical::Prepared> {
        self.lstage(n);
        let proofs = self.lprepare(n);
        self.lok(
            1,
            p::logical::Step::Commit {
                nonce: n,
                prepared: proofs.clone(),
            },
        );
        proofs
    }
    fn lfinish(&mut self, n: u128) {
        let mut proofs = Vec::new();
        for g in [3, 4] {
            let seal = self.pin(1).logical_finalization(n, g).unwrap();
            self.lok(g, p::logical::Step::Finalize(seal.clone()));
            proofs.push(self.pin(g).logical_finalized(seal).unwrap());
        }
        self.lok(
            1,
            p::logical::Step::ConfirmFinalized {
                nonce: n,
                rows: proofs,
            },
        );
        let home = self.pin(1).for_home(n).unwrap();
        let plan = home.plan.clone();
        for g in [3, 4] {
            self.ok(
                g,
                Step::Unlock {
                    plan: plan.clone(),
                    home: Box::new(home.clone()),
                },
            );
        }
        self.ok(
            1,
            Step::Complete {
                nonce: n,
                rows: self.releases(n),
            },
        );
    }
}
#[test]
fn logical_real_multi_range_commit_replay_and_separate_output_accounting() {
    let mut r = Rig::logical(2);
    let plan = r.lopen(101);
    let proofs = r.lcommit(101);
    assert_eq!(
        r.home().image.snapshot().unwrap().tasks[&TaskId(1)].transitions,
        2
    );
    assert_eq!(r.home().image.snapshot().unwrap().operations.len(), 1);
    r.bad(1, Step::Abort { nonce: 101 });
    r.lfinish(101);
    let before = r.home().image.clone();
    r.lok(
        1,
        p::logical::Step::Commit {
            nonce: 101,
            prepared: proofs,
        },
    );
    r.lok(1, p::logical::Step::Open(plan.clone()));
    assert_eq!(r.home().image, before);
    let mut changed = plan;
    changed.intent.request.after = Status::Failed;
    r.lbad(1, p::logical::Step::Open(changed));
    for g in [3, 4] {
        assert!(!r.data(g).protected.busy());
        assert!(
            r.data(g)
                .image
                .snapshot()
                .unwrap()
                .rows
                .values()
                .all(|v| v.status == Status::Queued && v.version == 1)
        );
        assert!(p::data_only(r.data(g)).unwrap());
    }
    let accounting = r.home().image.accounting(TaskId(1)).unwrap().unwrap();
    r.lok(
        1,
        p::logical::Step::Output {
            nonce: 101,
            principal: Principal(2),
        },
    );
    let once = r.home().image.accounting(TaskId(1)).unwrap().unwrap();
    r.lok(
        1,
        p::logical::Step::Output {
            nonce: 101,
            principal: Principal(2),
        },
    );
    let twice = r.home().image.accounting(TaskId(1)).unwrap().unwrap();
    assert_ne!(once, accounting);
    assert_ne!(twice, once);
    assert_eq!(
        r.home().image.snapshot().unwrap().tasks[&TaskId(1)].transitions,
        2
    );
}
#[test]
fn logical_partial_publication_retains_barriers_and_exact_finalization_identity() {
    let mut r = Rig::logical(2);
    let plan = r.lopen(102);
    r.lcommit(102);
    let seal = r.pin(1).logical_finalization(102, 3).unwrap();
    r.lok(3, p::logical::Step::Finalize(seal.clone()));
    assert_eq!(
        r.data(3).image.snapshot().unwrap().rows[&key(1)].status,
        Status::Queued
    );
    assert_eq!(
        r.data(4).image.snapshot().unwrap().rows[&key(60)].status,
        Status::Failed
    );
    assert!(r.data(3).protected.busy() && r.data(4).protected.busy());
    r.bad(
        3,
        Step::Unlock {
            plan,
            home: Box::new(r.pin(1).for_home(102).unwrap()),
        },
    );
    r.lbad(
        1,
        p::logical::Step::ConfirmFinalized {
            nonce: 102,
            rows: vec![r.pin(3).logical_finalized(seal.clone()).unwrap()],
        },
    );
    let mut tampered = seal.clone();
    tampered.digest[0] ^= 1;
    r.lbad(3, p::logical::Step::Finalize(tampered));
    r.lok(3, p::logical::Step::Finalize(seal));
    r.lfinish(102);
}
#[test]
fn logical_original_pins_lost_resume_snapshot_fresh_barrier_preserves_digest() {
    let mut r = Rig::logical(2);
    r.lopen(103);
    r.lstage(103);
    let digest = r.home().record(103).unwrap().staged_digest;
    // Canonical serialization round-trip simulates snapshot material independent
    // of the opaque old ReadIndex pins. Actual process restart is separate.
    r.machines = bincode::deserialize(&bincode::serialize(&r.machines).unwrap()).unwrap();
    r.index += 10;
    r.ok(1, Step::ResumeStage { nonce: 103 });
    r.lstage(103);
    assert_eq!(digest, r.home().record(103).unwrap().staged_digest);
    let proofs = r.lprepare(103);
    r.lok(
        1,
        p::logical::Step::Commit {
            nonce: 103,
            prepared: proofs,
        },
    );
    r.lfinish(103);
}
#[test]
fn logical_physical_authority_never_enters_logical_decision_profile() {
    let mut r = Rig::new();
    r.lbad(
        1,
        p::logical::Step::Install {
            image: image(42, 2),
            profile: p::logical::LogicalProfile { epoch: 1, tick: 12 },
        },
    );
    let mut plan = r.plan(104);
    plan.intent.lower = 12;
    r.lbad(1, p::logical::Step::Open(plan));
    r.lbad(
        1,
        p::logical::Step::Advance {
            profile: p::logical::LogicalProfile {
                epoch: 2,
                tick: 100,
            },
        },
    );
    r.lbad(
        1,
        p::logical::Step::Commit {
            nonce: 104,
            prepared: vec![],
        },
    );
    r.bad(1, Step::Commit { nonce: 104 });
}
#[test]
fn logical_client_time_and_regressing_authority_rejected() {
    let mut r = Rig::logical(2);
    let mut plan = r.plan(105);
    plan.intent.lower = 12;
    plan.intent.upper = 500;
    r.lbad(1, p::logical::Step::Open(plan));
    for profile in [
        p::logical::LogicalProfile { epoch: 0, tick: 12 },
        p::logical::LogicalProfile { epoch: 1, tick: 11 },
    ] {
        r.lbad(1, p::logical::Step::Advance { profile });
    }
    r.lopen(105);
    r.lstage(105);
    let proofs = r.lprepare(105);
    r.lok(
        1,
        p::logical::Step::Advance {
            profile: p::logical::LogicalProfile { epoch: 2, tick: 13 },
        },
    );
    r.lbad(
        1,
        p::logical::Step::Commit {
            nonce: 105,
            prepared: proofs,
        },
    );
}
#[test]
fn logical_decision_uses_current_tick_and_epoch_not_proposal_time() {
    let mut r = Rig::logical(2);
    r.lopen(106);
    r.lstage(106);
    let proofs = r.lprepare(106);
    r.lok(
        1,
        p::logical::Step::Advance {
            profile: p::logical::LogicalProfile { epoch: 1, tick: 14 },
        },
    );
    r.lok(
        1,
        p::logical::Step::Commit {
            nonce: 106,
            prepared: proofs,
        },
    );
    assert_eq!(
        r.home()
            .record(106)
            .unwrap()
            .decision
            .as_ref()
            .unwrap()
            .profile
            .tick,
        14
    );
    r.lfinish(106);
}
#[test]
fn logical_revoke_before_decision_denies_and_after_decision_finishes() {
    for after in [false, true] {
        let mut r = Rig::logical(2);
        let plan = r.lopen(107);
        r.lstage(107);
        let proofs = r.lprepare(107);
        if after {
            r.lok(
                1,
                p::logical::Step::Commit {
                    nonce: 107,
                    prepared: proofs.clone(),
                },
            );
        }
        r.ok(
            1,
            Step::Admin {
                command: serde_json::to_vec(&AdminCommand::Revoke { task: TaskId(1) }).unwrap(),
            },
        );
        if after {
            r.lfinish(107);
            r.lbad(
                1,
                p::logical::Step::Output {
                    nonce: 107,
                    principal: Principal(2),
                },
            );
            assert_eq!(
                r.home().image.snapshot().unwrap().tasks[&TaskId(1)].transitions,
                2
            );
        } else {
            r.lbad(
                1,
                p::logical::Step::Commit {
                    nonce: 107,
                    prepared: proofs,
                },
            );
            r.abort(&plan);
            assert_eq!(
                r.home().image.snapshot().unwrap().tasks[&TaskId(1)].transitions,
                0
            );
        }
    }
}
#[test]
fn logical_missing_stale_tampered_and_reordered_prepare_seals_rejected() {
    let mut r = Rig::logical(2);
    r.lopen(108);
    r.lstage(108);
    let proofs = r.lprepare(108);
    r.lbad(
        1,
        p::logical::Step::Commit {
            nonce: 108,
            prepared: proofs[..1].to_vec(),
        },
    );
    let mut altered = proofs.clone();
    altered[0].preparation.staged[0] ^= 1;
    r.lbad(
        1,
        p::logical::Step::Commit {
            nonce: 108,
            prepared: altered,
        },
    );
    let mut altered = proofs.clone();
    altered[0].preparation.part.applied += 1;
    r.lbad(
        1,
        p::logical::Step::Commit {
            nonce: 108,
            prepared: altered,
        },
    );
    let mut altered = proofs.clone();
    altered.reverse();
    r.lbad(
        1,
        p::logical::Step::Commit {
            nonce: 108,
            prepared: altered,
        },
    );
    r.lok(
        1,
        p::logical::Step::Commit {
            nonce: 108,
            prepared: proofs,
        },
    );
    r.lfinish(108);
}
#[test]
fn logical_at_limit_and_contending_authority_cannot_duplicate_budget() {
    let mut r = Rig::logical(1);
    let mut first = r.plan(109);
    first.intent.lower = 12;
    first.intent.request.selection = giggles_core::Selection::Keys(vec![key(1)]);
    let mut other = r.plan(110);
    other.intent.lower = 12;
    other.intent.request.selection = giggles_core::Selection::Keys(vec![key(60)]);
    r.lok(1, p::logical::Step::Open(first.clone()));
    r.lock(&first);
    r.lbad(1, p::logical::Step::Open(other.clone()));
    r.lcommit(109);
    r.lfinish(109);
    r.lok(1, p::logical::Step::Open(other.clone()));
    r.lock(&other);
    let result = r.run(
        1,
        Action::Protected(Box::new(Step::Stage {
            nonce: 110,
            rows: r.pins(),
        })),
    );
    assert_ne!(result.code, 0);
    assert!(
        result.data.to_string().contains("Budget"),
        "{:?}",
        result.data
    );
    r.abort(&other);
    assert_eq!(
        r.home().image.snapshot().unwrap().tasks[&TaskId(1)].transitions,
        1
    );
    assert_eq!(
        r.data(4).image.snapshot().unwrap().rows[&key(60)].status,
        Status::Failed
    );
}
#[test]
fn logical_output_denied_until_complete_and_wrong_principal_denied() {
    let mut r = Rig::logical(2);
    r.lopen(111);
    r.lcommit(111);
    r.lbad(
        1,
        p::logical::Step::Output {
            nonce: 111,
            principal: Principal(2),
        },
    );
    r.lfinish(111);
    r.lbad(
        1,
        p::logical::Step::Output {
            nonce: 111,
            principal: Principal(9),
        },
    );
}
#[test]
fn logical_retained_capacity_last_slot_resolution_and_late_replay_succeed() {
    let mut r = Rig::logical(2);
    for n in 1..p::MAX_RECORDS as u128 {
        let plan = r.lopen(300 + n);
        r.abort(&plan);
    }
    r.lopen(399);
    let proofs = r.lcommit(399);
    r.lfinish(399);
    assert_eq!(r.home().records.len(), p::MAX_RECORDS);
    for g in [3, 4] {
        assert_eq!(r.data(g).protected.records.len(), p::MAX_RECORDS);
    }
    let mut next = r.plan(400);
    next.intent.lower = 12;
    r.lbad(1, p::logical::Step::Open(next));
    r.lok(
        1,
        p::logical::Step::Commit {
            nonce: 399,
            prepared: proofs,
        },
    );
}
#[test]
fn logical_original_and_decision_after_tamper_rejected_by_snapshot_validation() {
    let mut r = Rig::logical(2);
    r.lopen(401);
    r.lstage(401);
    let mut root = r.machines[&1].clone();
    let Machine::Root { protected: h, .. } = &mut root else {
        panic!()
    };
    h.records[0].original.as_mut().unwrap().parts[0].applied += 1;
    assert!(root.validate(42, 1).is_err());
    let proofs = r.lprepare(401);
    r.lok(
        1,
        p::logical::Step::Commit {
            nonce: 401,
            prepared: proofs,
        },
    );
    let mut root = r.machines[&1].clone();
    let Machine::Root { protected: h, .. } = &mut root else {
        panic!()
    };
    let original = h.records[0].original.as_ref().unwrap().parts[0]
        .image
        .clone();
    h.records[0].decision.as_mut().unwrap().parts[0].after = original;
    assert!(root.validate(42, 1).is_err());
    let mut root = r.machines[&1].clone();
    let Machine::Root { protected: h, .. } = &mut root else {
        panic!()
    };
    h.records[0].decision.as_mut().unwrap().profile.tick += 1;
    assert!(root.validate(42, 1).is_err());
}
#[test]
fn logical_release_recovery_uses_retained_finalization_after_new_ordinary_write() {
    let mut r = Rig::logical(2);
    let plan = r.lopen(402);
    r.lcommit(402);
    let mut proofs = Vec::new();
    for g in [3, 4] {
        let seal = r.pin(1).logical_finalization(402, g).unwrap();
        r.lok(g, p::logical::Step::Finalize(seal.clone()));
        proofs.push(r.pin(g).logical_finalized(seal).unwrap());
    }
    r.lok(
        1,
        p::logical::Step::ConfirmFinalized {
            nonce: 402,
            rows: proofs,
        },
    );
    let home = r.pin(1).for_home(402).unwrap();
    r.ok(
        3,
        Step::Unlock {
            plan: plan.clone(),
            home: Box::new(home),
        },
    );
    let mut changed = r.data(3).image.snapshot().unwrap().rows[&key(1)].clone();
    changed.status = Status::Failed;
    r.ok_action(
        3,
        Action::Put {
            route: plan.routes[0].clone(),
            operation: 999,
            key: key(1),
            expected: Some(1),
            row: changed,
        },
    );
    // Recovery retains proof of the old publication even though legitimate work
    // on a released participant has since advanced its row version.
    r.lfinish(402);
    assert_eq!(r.data(3).image.snapshot().unwrap().rows[&key(1)].version, 2);
}
#[test]
fn logical_approval_provisioning_public_api_gap_has_real_positive_control() {
    let authority = image(42, 2);
    let mut assembled = authority.clone();
    for n in [1, 60] {
        let (next, reply) = assembled
            .evaluate(&replica::Command::Admin(
                serde_json::to_vec(&AdminCommand::Seed {
                    key: key(n),
                    row: row(),
                })
                .unwrap(),
            ))
            .unwrap();
        assert_eq!(reply.code, 0);
        assembled = next;
    }
    let request = protected_support::plan(vec![common::descriptor()], 1, 403)
        .intent
        .request;
    let (assembled, preview) = assembled
        .evaluate(&replica::Command::Task {
            task: TaskId(1),
            principal: Principal(2),
            lower: 12,
            upper: 12,
            action: replica::TaskAction::Preview(request),
        })
        .unwrap();
    assert_eq!(preview.code, 0);
    assert_eq!(preview.data["effects"].as_array().unwrap().len(), 2);
    let command = replica::Command::Admin(
        serde_json::to_vec(&AdminCommand::Approve {
            principal: Principal(3),
            preview: preview.data["preview"].as_str().unwrap().to_owned(),
            time: giggles_core::Time::Certified(12),
        })
        .unwrap(),
    );
    let (approved, positive) = assembled.evaluate(&command).unwrap();
    assert_eq!(positive.code, 0);
    assert_eq!(approved.snapshot().unwrap().approvals.len(), 1);
    let (_, negative) = authority.evaluate(&command).unwrap();
    assert_ne!(negative.code, 0);
    // A real approved workspace cannot be installed as row-free authority, and
    // the only public projection removes approvals as well as rows outside range.
    let mut r = Rig::logical(2);
    r.lbad(
        1,
        p::logical::Step::Install {
            image: approved.clone(),
            profile: p::logical::LogicalProfile { epoch: 1, tick: 12 },
        },
    );
    assert!(
        approved
            .row_partition(minimum(), None)
            .unwrap()
            .snapshot()
            .unwrap()
            .approvals
            .is_empty()
    );
}
#[test]
fn logical_expired_tick_before_decision_rejects_without_business_debit() {
    let mut r = Rig::logical(2);
    r.lopen(404);
    r.lstage(404);
    let proofs = r.lprepare(404);
    r.lok(
        1,
        p::logical::Step::Advance {
            profile: p::logical::LogicalProfile {
                epoch: 1,
                tick: 1001,
            },
        },
    );
    r.lbad(
        1,
        p::logical::Step::Commit {
            nonce: 404,
            prepared: proofs,
        },
    );
    assert_eq!(
        r.home().image.snapshot().unwrap().tasks[&TaskId(1)].transitions,
        0
    );
}
#[test]
fn logical_operation_retry_under_new_nonce_keeps_original_receipt_and_budget() {
    let mut r = Rig::logical(2);
    let first = r.lopen(405);
    r.lcommit(405);
    r.lfinish(405);
    let before = r.home().image.snapshot().unwrap();
    let mut again = r.plan(406);
    again.intent = first.intent.clone();
    r.lok(1, p::logical::Step::Open(again.clone()));
    r.lock(&again);
    r.lcommit(406);
    r.lfinish(406);
    let after = r.home().image.snapshot().unwrap();
    assert_eq!(before.operations, after.operations);
    assert_eq!(after.tasks[&TaskId(1)].transitions, 2);
    for (g, k) in [(3, key(1)), (4, key(60))] {
        assert_eq!(r.data(g).image.snapshot().unwrap().rows[&k].version, 1);
    }
}
#[test]
fn logical_nonrevoke_admin_and_old_output_cannot_mutate_locked_authority() {
    let mut r = Rig::logical(2);
    r.lopen(407);
    r.lcommit(407);
    r.lfinish(407);
    let mut next = r.plan(408);
    next.intent.lower = 12;
    r.lok(1, p::logical::Step::Open(next));
    r.lbad(
        1,
        p::logical::Step::Output {
            nonce: 407,
            principal: Principal(2),
        },
    );
    r.bad(
        1,
        Step::Admin {
            command: serde_json::to_vec(&AdminCommand::ProvisionTaskResources {
                task: TaskId(1),
                limits: giggles_core::TaskResources {
                    read_rows: 2000,
                    disclosed_rows: 2000,
                    encoded_bytes: 2000000,
                },
            })
            .unwrap(),
        },
    );
}
