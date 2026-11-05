mod common;
mod protected_support;
use common::*;
use giggles_distributed::{
    runtime::{Config, GroupConfig, Initial},
    transport::{Configuration, Peer, fingerprint},
    *,
};
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair, KeyUsagePurpose,
};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    process::{Child, ChildStdin, Command, Stdio},
    sync::mpsc,
    time::{Duration, Instant},
};

fn transaction_plan(nonce: u128) -> transaction::Plan {
    let s = split();
    let mut a = row();
    a.attempt_count = 10;
    let mut b = row();
    b.attempt_count = 20;
    transaction::Plan {
        database: 42,
        nonce,
        home: s.left.clone(),
        participants: vec![
            transaction::Participant {
                route: s.left,
                writes: vec![transaction::Write {
                    key: key(1),
                    before: None,
                    row: a,
                }],
                reads: vec![],
            },
            transaction::Participant {
                route: s.right,
                writes: vec![transaction::Write {
                    key: key(60),
                    before: None,
                    row: b,
                }],
                reads: vec![],
            },
        ],
    }
}

impl Processes {
    fn independent(&mut self) {
        for (group, target) in [(3, 2), (4, 3)] {
            let deadline = Instant::now() + Duration::from_secs(20);
            loop {
                let leader = self.leader(group);
                if leader == target {
                    break;
                }
                let reply = self
                    .nodes
                    .get_mut(&leader)
                    .unwrap()
                    .request(json!({"command":"transfer","group":group,"to":target}));
                assert!(
                    Instant::now() < deadline,
                    "independent transfer timeout: {reply}"
                );
            }
        }
    }
    fn owner(&mut self, group: u64, input: Value) -> Value {
        let leader = self.leader(group);
        self.nodes.get_mut(&leader).unwrap().request(input)
    }
    fn pin(
        &mut self,
        route: &Descriptor,
        id: giggles_transactions::TransactionId,
    ) -> transaction::Pinned {
        let reply = self.owner(route.group, json!({"command":"tx_pin","route":route}));
        assert_eq!(reply["ok"], true, "{reply}");
        serde_json::from_value::<transaction::Pinned>(reply["data"].clone())
            .unwrap()
            .for_transaction(id)
    }
    fn tx(
        &mut self,
        route: &Descriptor,
        step: transaction::Step,
        evidence: Vec<transaction::Pinned>,
    ) -> Value {
        self.owner(route.group, json!({"command":"tx_step","request":transaction::Request { route:route.clone(), step, evidence }}))
    }
    fn tx_ok(
        &mut self,
        route: &Descriptor,
        step: transaction::Step,
        evidence: Vec<transaction::Pinned>,
    ) {
        let reply = self.tx(route, step, evidence);
        assert_eq!(reply["ok"], true, "{reply}");
    }
    fn split_for_transactions(&mut self) {
        self.apply(1, Action::Begin(split()));
        self.resume();
        self.independent();
    }
}

#[test]
fn real_transactions_typed_coordinator_recovery_status_cas_and_history() {
    let mut p = Processes::new();
    p.apply(1, Action::Begin(split()));
    p.resume();
    let plan = transaction_plan(8001);
    let reply = p.request(json!({"command":"transact","plan":plan,"outcome":"Commit"}));
    assert_eq!(reply["ok"], true, "{reply}");
    assert_eq!(reply["data"]["decision"]["outcome"], "Commit");
    // Lost client reply: restart the entire coordinator and recover by identity.
    p.restart();
    let recovered = p
        .request(json!({"command":"tx_recover","home":plan.home,"id":plan.id(),"outcome":"Abort"}));
    assert_eq!(recovered["ok"], true, "{recovered}");
    assert_eq!(recovered["data"]["decision"], reply["data"]["decision"]);
    let mut conflict = plan.clone();
    conflict.participants[0].writes[0].row.attempt_count = 99;
    let refused = p.request(json!({"command":"transact","plan":conflict,"outcome":"Commit"}));
    assert_eq!(refused["ok"], false);
    let mut stale_cas = plan.clone();
    stale_cas.nonce += 1;
    let refused = p.request(json!({"command":"transact","plan":stale_cas,"outcome":"Commit"}));
    assert_eq!(refused["ok"], false);
    let aborted = p.request(
        json!({"command":"tx_recover","home":stale_cas.home,"id":stale_cas.id(),"outcome":"Abort"}),
    );
    assert_eq!(aborted["ok"], true, "{aborted}");
    let scan = p.request(
        json!({"command":"observe","bounds":Bounds { start: key(0), end: Some(key(100)) }}),
    );
    assert_eq!(scan["ok"], true, "{scan}");
    let rows: Vec<(giggles_core::Key, giggles_core::Row)> =
        serde_json::from_value(scan["data"].clone()).unwrap();
    assert_eq!(
        rows.iter()
            .map(|(k, r)| (k.row.0, r.attempt_count))
            .collect::<Vec<_>>(),
        vec![(1, 10), (60, 20)]
    );
    // Feed externally observed results to independent semantics; no evaluator
    // output supplies expected observations. The adapter models attempt_count.
    let history = giggles_history::History {
        version: 1,
        horizon: 5,
        initial: BTreeMap::new(),
        invariants: Default::default(),
        attempts: vec![
            giggles_history::Attempt {
                id: "write-attempt".into(),
                logical_id: "owner-8001".into(),
                invoke: 1,
                complete: Some(2),
                outcome: giggles_history::Outcome::Committed,
                durable: None,
                decision_at: None,
                ops: plan
                    .participants
                    .iter()
                    .flat_map(|p| {
                        p.writes.iter().map(|w| giggles_history::Op::Write {
                            key: w.key.row.0 as i64,
                            value: i64::try_from(w.row.attempt_count).unwrap(),
                        })
                    })
                    .collect(),
                observations: vec![],
                receipt: None,
            },
            giggles_history::Attempt {
                id: "read-attempt".into(),
                logical_id: "owner-scan".into(),
                invoke: 3,
                complete: Some(4),
                outcome: giggles_history::Outcome::Committed,
                durable: None,
                decision_at: None,
                ops: vec![giggles_history::Op::Scan { start: 0, end: 100 }],
                observations: vec![giggles_history::Observation::Scan {
                    rows: rows
                        .iter()
                        .map(|(k, r)| (k.row.0 as i64, i64::try_from(r.attempt_count).unwrap()))
                        .collect(),
                }],
                receipt: None,
            },
        ],
    };
    std::fs::write(
        p.root.join("observed-transaction-history.json"),
        serde_json::to_vec_pretty(&history).unwrap(),
    )
    .unwrap();
    let check = giggles_history::check(&history, giggles_history::Limits::default()).unwrap();
    std::fs::write(
        p.root.join("independent-history-check.txt"),
        format!("{check:?}"),
    )
    .unwrap();
    assert!(matches!(check.verdict, giggles_history::Verdict::Pass));
}

#[test]
fn real_independent_leaders_partial_finalize_blocks_then_resolves_and_snapshots() {
    use transaction::Step;
    let mut p = Processes::new();
    p.split_for_transactions();
    let plan = transaction_plan(8002);
    let id = plan.id();
    p.tx_ok(&plan.home, Step::Open(plan.clone()), vec![]);
    let home = p.pin(&plan.home, id);
    for part in &plan.participants {
        p.tx_ok(&part.route, Step::Begin { id }, vec![home.clone()]);
    }
    for part in &plan.participants {
        p.tx_ok(&part.route, Step::Lock { id }, vec![]);
    }
    let locked = plan
        .participants
        .iter()
        .map(|part| p.pin(&part.route, id))
        .collect::<Vec<_>>();
    for part in &plan.participants {
        p.tx_ok(&part.route, Step::Prepare { id }, locked.clone());
    }
    for part in &plan.participants {
        let evidence = p.pin(&part.route, id);
        p.tx_ok(
            &plan.home,
            Step::Vote {
                id,
                participant: part.route.clone(),
            },
            vec![evidence],
        );
    }
    // A prepared participant has no unilateral timeout/abort surface.
    let missing = p.tx(
        &plan.participants[1].route,
        Step::Finalize { id },
        vec![home.clone()],
    );
    assert_eq!(missing["ok"], false);
    p.tx_ok(
        &plan.home,
        Step::Decide {
            id,
            outcome: giggles_transactions::Outcome::Commit,
        },
        vec![],
    );
    let decision = p.pin(&plan.home, id);
    p.tx_ok(&plan.home, Step::Finalize { id }, vec![decision.clone()]);
    // A reader takes the first range's lock, but cannot expose its half while
    // the second participant is still prepared. Drive independently led steps.
    let mut read = plan.clone();
    read.nonce += 100;
    for part in &mut read.participants {
        part.writes.clear();
        part.reads = vec![transaction::Predicate {
            bounds: part.route.bounds.clone(),
            expected: None,
        }];
    }
    let rid = read.id();
    p.tx_ok(&read.home, Step::Open(read.clone()), vec![]);
    let rh = p.pin(&read.home, rid);
    for part in &read.participants {
        p.tx_ok(&part.route, Step::Begin { id: rid }, vec![rh.clone()]);
    }
    p.tx_ok(&read.home, Step::Lock { id: rid }, vec![]);
    let blocked = p.tx(&read.participants[1].route, Step::Lock { id: rid }, vec![]);
    assert_eq!(blocked["ok"], false, "{blocked}");
    let hidden = p.owner(
        read.home.group,
        json!({"command":"tx_status","home":read.home,"id":rid}),
    );
    assert!(hidden["data"]["rows"].is_null());
    for route in [&plan.home, &plan.participants[1].route] {
        let r = p.owner(
            route.group,
            json!({"command":"checkpoint","group":route.group}),
        );
        assert_eq!(r["ok"], true);
    }
    p.restart();
    // Ordinary current observation resolves the prepared blocker from the
    // durable home, after checkpoint/full restart, before exposing either row.
    let automatic = p
        .request(json!({"command":"observe","bounds":Bounds { start:key(0), end:Some(key(100)) }}));
    assert_eq!(automatic["ok"], true, "{automatic}");
    assert_eq!(automatic["data"].as_array().unwrap().len(), 2);
    p.independent();
    let home = p.pin(&plan.home, id);
    p.tx_ok(
        &plan.participants[1].route,
        Step::Finalize { id },
        vec![home],
    );
    p.tx_ok(&read.participants[1].route, Step::Lock { id: rid }, vec![]);
    let locked = read
        .participants
        .iter()
        .map(|part| p.pin(&part.route, rid))
        .collect::<Vec<_>>();
    for part in &read.participants {
        p.tx_ok(&part.route, Step::Prepare { id: rid }, locked.clone());
    }
    for part in &read.participants {
        let e = p.pin(&part.route, rid);
        p.tx_ok(
            &read.home,
            Step::Vote {
                id: rid,
                participant: part.route.clone(),
            },
            vec![e],
        );
    }
    p.tx_ok(
        &read.home,
        Step::Decide {
            id: rid,
            outcome: giggles_transactions::Outcome::Commit,
        },
        vec![],
    );
    let home = p.pin(&read.home, rid);
    for part in &read.participants {
        p.tx_ok(&part.route, Step::Finalize { id: rid }, vec![home.clone()]);
    }
    let result = p.owner(
        read.home.group,
        json!({"command":"tx_status","home":read.home,"id":rid}),
    );
    let rows: Vec<transaction::Rows> =
        serde_json::from_value(result["data"]["rows"].clone()).unwrap();
    assert_eq!(
        rows.iter()
            .flat_map(|r| r.predicates.iter().flatten())
            .map(|(k, r)| (k.row.0, r.attempt_count))
            .collect::<Vec<_>>(),
        vec![(1, 10), (60, 20)]
    );
}

#[test]
fn real_transactions_process_crash_at_every_prepare_vote_decision_finalize_boundary() {
    use giggles_transactions::Outcome;
    use transaction::{Request, Step};
    // 2 outcomes * 7 durable transitions * both sides of redb commit.
    // The armed process exits at the storage seam, remaining owned processes
    // are killed, and all three real stores/listeners reopen for replay.
    for outcome in [Outcome::Commit, Outcome::Abort] {
        for boundary in 0..7 {
            for after in [false, true] {
                let mut p = Processes::new();
                p.split_for_transactions();
                let plan = transaction_plan(9000 + boundary as u128);
                let id = plan.id();
                std::fs::write(
                    p.root.join("fault-schedule.json"),
                    json!({"outcome":outcome,"boundary":boundary,"after":after,"id":id})
                        .to_string(),
                )
                .unwrap();
                p.tx_ok(&plan.home, Step::Open(plan.clone()), vec![]);
                let home = p.pin(&plan.home, id);
                for part in &plan.participants {
                    p.tx_ok(&part.route, Step::Begin { id }, vec![home.clone()]);
                }
                for part in &plan.participants {
                    p.tx_ok(&part.route, Step::Lock { id }, vec![]);
                }
                let locked = plan
                    .participants
                    .iter()
                    .map(|part| p.pin(&part.route, id))
                    .collect::<Vec<_>>();
                for stage in 0..7 {
                    let (route, step, evidence) = match stage {
                        0 | 1 => (
                            plan.participants[stage].route.clone(),
                            Step::Prepare { id },
                            locked.clone(),
                        ),
                        2 | 3 => {
                            let route = plan.participants[stage - 2].route.clone();
                            let e = p.pin(&route, id);
                            (
                                plan.home.clone(),
                                Step::Vote {
                                    id,
                                    participant: route,
                                },
                                vec![e],
                            )
                        }
                        4 => (plan.home.clone(), Step::Decide { id, outcome }, vec![]),
                        5 | 6 => {
                            let e = p.pin(&plan.home, id);
                            (
                                plan.participants[stage - 5].route.clone(),
                                Step::Finalize { id },
                                vec![e],
                            )
                        }
                        _ => unreachable!(),
                    };
                    let request = Request {
                        route: route.clone(),
                        step,
                        evidence,
                    };
                    if stage == boundary {
                        let leader = p.leader(route.group);
                        let arm = p.nodes.get_mut(&leader).unwrap().request(
                            json!({"command":"arm_crash","group":route.group,"after":after}),
                        );
                        assert_eq!(arm["ok"], true);
                        let node = p.nodes.get_mut(&leader).unwrap();
                        node.send(json!({"command":"tx_step","request":request}));
                        assert!(
                            node.output.recv_timeout(Duration::from_secs(12)).is_err(),
                            "armed process should exit"
                        );
                        assert_eq!(node.child.wait().unwrap().code(), Some(86));
                        p.restart();
                        p.independent();
                    }
                    // Repeating both lost and acknowledged transitions is safe.
                    let reply =
                        p.owner(route.group, json!({"command":"tx_step","request":request}));
                    assert_eq!(
                        reply["ok"], true,
                        "outcome={outcome:?} boundary={boundary} after={after} stage={stage}: {reply}"
                    );
                }
                let status = p.owner(
                    plan.home.group,
                    json!({"command":"tx_status","home":plan.home,"id":id}),
                );
                assert_eq!(status["ok"], true, "{status}");
                assert_eq!(status["data"]["decision"]["outcome"], json!(outcome));
                p.colocate();
                let scan = p.request(
                    json!({"command":"observe","bounds":Bounds{start:key(0),end:Some(key(100))}}),
                );
                assert_eq!(scan["ok"], true, "{scan}");
                let rows: Vec<(giggles_core::Key, giggles_core::Row)> =
                    serde_json::from_value(scan["data"].clone()).unwrap();
                assert_eq!(rows.len(), if outcome == Outcome::Commit { 2 } else { 0 });
                for (_, row) in &rows {
                    assert_eq!(row.version, 0, "redo must apply once");
                }
                let ops = plan
                    .participants
                    .iter()
                    .flat_map(|p| {
                        p.writes.iter().map(|w| giggles_history::Op::Write {
                            key: w.key.row.0 as i64,
                            value: i64::try_from(w.row.attempt_count).unwrap(),
                        })
                    })
                    .collect::<Vec<_>>();
                let observed_outcome = match status["data"]["decision"]["outcome"].as_str().unwrap()
                {
                    "Commit" => giggles_history::Outcome::Committed,
                    "Abort" => giggles_history::Outcome::Aborted,
                    _ => panic!(),
                };
                let history = giggles_history::History {
                    version: 1,
                    horizon: 7,
                    initial: BTreeMap::new(),
                    invariants: Default::default(),
                    attempts: vec![
                        giggles_history::Attempt {
                            id: "lost-response".into(),
                            logical_id: "fault-operation".into(),
                            invoke: 1,
                            complete: Some(2),
                            outcome: giggles_history::Outcome::Indeterminate,
                            durable: None,
                            decision_at: None,
                            ops: ops.clone(),
                            observations: vec![],
                            receipt: None,
                        },
                        giggles_history::Attempt {
                            id: "status-replay".into(),
                            logical_id: "fault-operation".into(),
                            invoke: 3,
                            complete: Some(4),
                            outcome: observed_outcome,
                            durable: None,
                            decision_at: None,
                            ops,
                            observations: vec![],
                            receipt: None,
                        },
                        giggles_history::Attempt {
                            id: "current-scan".into(),
                            logical_id: "fault-scan".into(),
                            invoke: 5,
                            complete: Some(6),
                            outcome: giggles_history::Outcome::Committed,
                            durable: None,
                            decision_at: None,
                            ops: vec![giggles_history::Op::Scan { start: 0, end: 100 }],
                            observations: vec![giggles_history::Observation::Scan {
                                rows: rows
                                    .iter()
                                    .map(|(k, r)| {
                                        (k.row.0 as i64, i64::try_from(r.attempt_count).unwrap())
                                    })
                                    .collect(),
                            }],
                            receipt: None,
                        },
                    ],
                };
                let check =
                    giggles_history::check(&history, giggles_history::Limits::default()).unwrap();
                std::fs::write(
                    p.root.join("observed-fault-history.json"),
                    serde_json::to_vec_pretty(&history).unwrap(),
                )
                .unwrap();
                std::fs::write(
                    p.root.join("observed-fault-check.json"),
                    serde_json::to_vec_pretty(&check).unwrap(),
                )
                .unwrap();
                assert_eq!(check.verdict, giggles_history::Verdict::Pass, "{check:?}");
                if outcome == Outcome::Commit {
                    let mut partial = history.clone();
                    let giggles_history::Observation::Scan { rows } =
                        &mut partial.attempts[2].observations[0]
                    else {
                        panic!()
                    };
                    rows.truncate(1);
                    let rejected =
                        giggles_history::check(&partial, giggles_history::Limits::default())
                            .unwrap();
                    std::fs::write(
                        p.root.join("partial-negative-control.json"),
                        serde_json::to_vec_pretty(&(partial, rejected.clone())).unwrap(),
                    )
                    .unwrap();
                    assert_eq!(rejected.verdict, giggles_history::Verdict::Fail);
                }
            }
        }
    }
}

#[test]
fn real_transaction_home_minority_cannot_decide_or_release_prepared_participants() {
    use transaction::Step;
    let mut p = Processes::new();
    p.split_for_transactions();
    let plan = transaction_plan(9901);
    let id = plan.id();
    p.tx_ok(&plan.home, Step::Open(plan.clone()), vec![]);
    let home = p.pin(&plan.home, id);
    for part in &plan.participants {
        p.tx_ok(&part.route, Step::Begin { id }, vec![home.clone()]);
        p.tx_ok(&part.route, Step::Lock { id }, vec![]);
    }
    let locked = plan
        .participants
        .iter()
        .map(|part| p.pin(&part.route, id))
        .collect::<Vec<_>>();
    for part in &plan.participants {
        p.tx_ok(&part.route, Step::Prepare { id }, locked.clone());
        let e = p.pin(&part.route, id);
        p.tx_ok(
            &plan.home,
            Step::Vote {
                id,
                participant: part.route.clone(),
            },
            vec![e],
        );
    }
    let before = p.pin(&plan.home, id);
    let leader = p.leader(plan.home.group);
    for voter in VOTERS {
        let peers = VOTERS
            .into_iter()
            .filter(|other| *other != voter)
            .collect::<Vec<_>>();
        let reply = p
            .nodes
            .get_mut(&voter)
            .unwrap()
            .request(json!({"command":"block","group":plan.home.group,"peers":peers}));
        assert_eq!(reply["ok"], true);
    }
    let refused=p.nodes.get_mut(&leader).unwrap().request(json!({"command":"tx_step","request":transaction::Request{route:plan.home.clone(),step:Step::Decide{id,outcome:giggles_transactions::Outcome::Commit},evidence:vec![]}}));
    assert_eq!(refused["ok"], false, "{refused}");
    let unresolved = p.tx(
        &plan.participants[1].route,
        Step::Finalize { id },
        vec![before],
    );
    assert_eq!(unresolved["ok"], false);
    let right = p.pin(&plan.participants[1].route, id);
    assert_eq!(
        right.state.engine(42, &right.route).unwrap().phase(id),
        Some(giggles_transactions::Phase::Prepared)
    );
    // Heal by full process restart. The previously submitted decision may now
    // commit; resolution consults actual current home state and never guesses.
    p.restart();
    let resumed =
        p.request(json!({"command":"tx_recover","home":plan.home,"id":id,"outcome":"Commit"}));
    assert_eq!(resumed["ok"], true, "{resumed}");
    let scan =
        p.request(json!({"command":"observe","bounds":Bounds{start:key(0),end:Some(key(100))}}));
    assert_eq!(scan["ok"], true, "{scan}");
    assert_eq!(scan["data"].as_array().unwrap().len(), 2);
}

impl Processes {
    fn tx_begin_all(&mut self, plan: &transaction::Plan) {
        let id = plan.id();
        self.tx_ok(&plan.home, transaction::Step::Open(plan.clone()), vec![]);
        let home = self.pin(&plan.home, id);
        for part in &plan.participants {
            self.tx_ok(
                &part.route,
                transaction::Step::Begin { id },
                vec![home.clone()],
            );
        }
    }
    fn tx_prepare_all(&mut self, plan: &transaction::Plan) {
        let id = plan.id();
        for part in &plan.participants {
            self.tx_ok(&part.route, transaction::Step::Lock { id }, vec![]);
        }
        let locked = plan
            .participants
            .iter()
            .map(|part| self.pin(&part.route, id))
            .collect::<Vec<_>>();
        for part in &plan.participants {
            self.tx_ok(
                &part.route,
                transaction::Step::Prepare { id },
                locked.clone(),
            );
        }
    }
    fn tx_finish_all(&mut self, plan: &transaction::Plan, outcome: giggles_transactions::Outcome) {
        let id = plan.id();
        if outcome == giggles_transactions::Outcome::Commit {
            for part in &plan.participants {
                let e = self.pin(&part.route, id);
                self.tx_ok(
                    &plan.home,
                    transaction::Step::Vote {
                        id,
                        participant: part.route.clone(),
                    },
                    vec![e],
                );
            }
        }
        self.tx_ok(
            &plan.home,
            transaction::Step::Decide { id, outcome },
            vec![],
        );
        let home = self.pin(&plan.home, id);
        for part in &plan.participants {
            self.tx_ok(
                &part.route,
                transaction::Step::Finalize { id },
                vec![home.clone()],
            );
        }
    }
}

#[test]
fn real_transactions_predicates_deadlock_exhaustion_evidence_and_split_guards() {
    use giggles_transactions::Outcome;
    use transaction::Step;
    // Each case uses fresh durable three-voter stores and different leaders.
    for case in 0..3 {
        let mut p = Processes::new();
        p.split_for_transactions();
        std::fs::write(
            p.root.join("negative-case.json"),
            json!({"case":case}).to_string(),
        )
        .unwrap();
        if case == 0 {
            let mut reader = transaction_plan(12000);
            for part in &mut reader.participants {
                part.writes.clear();
                part.reads = vec![transaction::Predicate {
                    bounds: part.route.bounds.clone(),
                    expected: Some(vec![]),
                }];
            }
            p.tx_begin_all(&reader);
            for part in &reader.participants {
                p.tx_ok(&part.route, Step::Lock { id: reader.id() }, vec![]);
            }
            let writer = transaction_plan(12001);
            p.tx_begin_all(&writer);
            let blocked = p.tx(&writer.home, Step::Lock { id: writer.id() }, vec![]);
            assert_eq!(blocked["ok"], false);
            let direct=p.owner(writer.home.group,json!({"command":"apply","group":writer.home.group,"action":put(writer.home.clone(),55,key(2),None)}));
            assert_eq!(direct["ok"], false);
            p.tx_finish_all(&writer, Outcome::Abort);
            p.tx_finish_all(&reader, Outcome::Abort);
            let writer = transaction_plan(12002);
            p.tx_begin_all(&writer);
            p.tx_prepare_all(&writer);
            p.tx_finish_all(&writer, Outcome::Commit);
            reader.nonce = 12003;
            p.tx_begin_all(&reader);
            let phantom = p.tx(&reader.home, Step::Lock { id: reader.id() }, vec![]);
            assert_eq!(phantom["ok"], false, "{phantom}");
            p.tx_finish_all(&reader, Outcome::Abort);
        } else if case == 1 {
            let a = transaction_plan(12100);
            let b = transaction_plan(12101);
            p.tx_begin_all(&a);
            p.tx_begin_all(&b);
            p.tx_ok(&a.home, Step::Lock { id: a.id() }, vec![]);
            p.tx_ok(&b.participants[1].route, Step::Lock { id: b.id() }, vec![]);
            let left = p.tx(&a.participants[1].route, Step::Lock { id: a.id() }, vec![]);
            assert_eq!(left["ok"], false);
            let right = p.tx(&b.home, Step::Lock { id: b.id() }, vec![]);
            assert_eq!(right["ok"], false);
            // Refusal creates no prepare or implicit abort. Durable home aborts
            // resolve the deliberately adversarial order without a wait cycle.
            p.tx_finish_all(&a, Outcome::Abort);
            p.tx_finish_all(&b, Outcome::Abort);
            let next = transaction_plan(12102);
            p.tx_begin_all(&next);
            p.tx_prepare_all(&next);
            p.tx_finish_all(&next, Outcome::Commit);
        } else {
            let plan = transaction_plan(12200);
            let id = plan.id();
            p.tx_begin_all(&plan);
            let mut split = split();
            split.id = 88;
            split.parent = split.left.clone();
            split.key = key(25);
            split.left = split.parent.clone();
            split.left.range = 50;
            split.left.group = 5;
            split.left.epoch += 1;
            split.left.bounds.end = Some(split.key);
            split.right = split.parent.clone();
            split.right.range = 51;
            split.right.group = 6;
            split.right.epoch += 1;
            split.right.bounds.start = split.key;
            let active=p.owner(plan.home.group,json!({"command":"apply","group":plan.home.group,"action":Action::Fence(split.clone())}));
            assert_eq!(active["ok"], false);
            p.tx_prepare_all(&plan);
            let prepared = p.owner(
                plan.home.group,
                json!({"command":"apply","group":plan.home.group,"action":Action::Fence(split)}),
            );
            assert_eq!(prepared["ok"], false);
            let mut stale = plan.home.clone();
            stale.epoch += 1;
            let refused = p.tx(&stale, Step::Lock { id }, vec![]);
            assert_eq!(refused["ok"], false);
            let mut evidence = p.pin(&plan.participants[1].route, id);
            evidence.applied = 0;
            let refused = p.tx(
                &plan.home,
                Step::Vote {
                    id,
                    participant: plan.participants[1].route.clone(),
                },
                vec![evidence],
            );
            assert_eq!(refused["ok"], false);
            let mut exhausted = false;
            for n in 1..=transaction::MAX_TRANSACTIONS {
                let mut more = plan.clone();
                more.nonce += n as u128;
                let reply = p.tx(&more.home, Step::Open(more.clone()), vec![]);
                if reply["ok"] == false {
                    exhausted = true;
                    break;
                }
            }
            assert!(exhausted);
            for part in &plan.participants {
                let e = p.pin(&part.route, id);
                assert_eq!(
                    e.state.engine(42, &part.route).unwrap().phase(id),
                    Some(giggles_transactions::Phase::Prepared)
                );
            }
            p.tx_finish_all(&plan, Outcome::Commit);
            for part in &plan.participants {
                let e = p.pin(&part.route, id);
                assert_eq!(
                    e.state.engine(42, &part.route).unwrap().phase(id),
                    Some(giggles_transactions::Phase::Committed)
                );
                let current = p.owner(
                    part.route.group,
                    json!({"command":"current","group":part.route.group}),
                );
                assert_eq!(current["ok"], true);
                let Machine::Range { data, .. } =
                    serde_json::from_value(current["data"]["machine"].clone()).unwrap()
                else {
                    panic!()
                };
                let rows = data.image.snapshot().unwrap().rows;
                assert_eq!(rows.len(), 1);
                assert_eq!(rows.values().next().unwrap().version, 0);
            }
        }
    }
}

struct Running {
    child: Child,
    input: ChildStdin,
    output: mpsc::Receiver<Value>,
    history: std::fs::File,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_embedded_runtime_releases_peer_listeners_and_stores() {
    let mut fixture = Processes::new();
    fixture.nodes.clear(); // Stops only this fixture's owned child processes.
    let config: Config =
        serde_json::from_slice(&std::fs::read(&fixture.configs[&1]).unwrap()).unwrap();
    for _ in 0..2 {
        let runtime = runtime::Runtime::start(config.clone()).await.unwrap();
        let handle = runtime.group(1).unwrap().clone();
        handle.inspect().await.unwrap();
        drop(runtime);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let listeners: std::io::Result<Vec<_>> = config
                .groups
                .iter()
                .map(|g| std::net::TcpListener::bind(&g.peer.listen))
                .collect();
            if let Ok(listeners) = listeners {
                let stores = config
                    .groups
                    .iter()
                    .map(|g| {
                        Store::open(&g.peer.database, config.node, config.domain, g.peer.group)
                    })
                    .collect::<Result<Vec<_>>>();
                if stores.is_ok() {
                    drop(stores);
                    drop(listeners);
                    break;
                }
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "runtime retained listeners or stores after drop"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(handle.inspect().await.is_err());
    }
}
impl Running {
    fn start(config: &Path, root: &Path, id: u64, round: u64) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_giggles-ranges"))
            .arg(config)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(std::fs::File::create(root.join(format!("stderr-{id}-{round}.txt"))).unwrap())
            .spawn()
            .unwrap();
        let input = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();
        let (tx, output) = mpsc::channel();
        let mut log =
            std::fs::File::create(root.join(format!("stdout-{id}-{round}.jsonl"))).unwrap();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
                writeln!(log, "{line}").unwrap();
                if let Ok(v) = serde_json::from_str(&line) {
                    let _ = tx.send(v);
                }
            }
        });
        let s = Self {
            child,
            input,
            output,
            history: std::fs::File::create(root.join(format!("input-{id}-{round}.jsonl"))).unwrap(),
        };
        assert_eq!(
            s.output.recv_timeout(Duration::from_secs(10)).unwrap()["ready"],
            true
        );
        s
    }
    fn send(&mut self, v: Value) {
        writeln!(self.history, "{v}").unwrap();
        writeln!(self.input, "{v}").unwrap();
        self.input.flush().unwrap();
    }
    fn receive(&self) -> Value {
        self.output
            .recv_timeout(Duration::from_secs(12))
            .expect("process response timeout")
    }
    fn request(&mut self, v: Value) -> Value {
        self.send(v);
        self.receive()
    }
}
impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
struct Processes {
    root: PathBuf,
    configs: BTreeMap<u64, PathBuf>,
    nodes: BTreeMap<u64, Running>,
    round: u64,
}
impl Processes {
    fn new() -> Self {
        Self::with_members(3)
    }
    fn with_members(count: u64) -> Self {
        let root = fresh("multi-process");
        // Hash compiled source bytes, not mutable working files at test time.
        // Every failed schedule retains the exact implementation/test identity.
        {
            use sha2::{Digest, Sha256};
            let sources: [(&str, &[u8]); 13] = [
                ("src/machine.rs", include_bytes!("../src/machine.rs")),
                ("src/protected.rs", include_bytes!("../src/protected.rs")),
                (
                    "src/protected/logical.rs",
                    include_bytes!("../src/protected/logical.rs"),
                ),
                (
                    "tests/protected_support/mod.rs",
                    include_bytes!("protected_support/mod.rs"),
                ),
                (
                    "src/transaction.rs",
                    include_bytes!("../src/transaction.rs"),
                ),
                ("src/runtime.rs", include_bytes!("../src/runtime.rs")),
                ("src/node.rs", include_bytes!("../src/node.rs")),
                ("src/store.rs", include_bytes!("../src/store.rs")),
                ("src/transport.rs", include_bytes!("../src/transport.rs")),
                (
                    "src/bin/giggles-ranges.rs",
                    include_bytes!("../src/bin/giggles-ranges.rs"),
                ),
                ("tests/process.rs", include_bytes!("process.rs")),
                ("Cargo.lock", include_bytes!("../Cargo.lock")),
                (
                    "transactions/machine.rs",
                    include_bytes!("../../giggles-transactions/src/machine.rs"),
                ),
            ];
            let hashes = sources
                .iter()
                .map(|(name, bytes)| (*name, hex::encode(Sha256::digest(bytes))))
                .collect::<BTreeMap<_, _>>();
            std::fs::write(
                root.join("compiled-source-sha256.json"),
                serde_json::to_vec_pretty(&hashes).unwrap(),
            )
            .unwrap();
        }
        let mut cp = CertificateParams::new(Vec::<String>::new()).unwrap();
        cp.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        cp.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        let cak = KeyPair::generate().unwrap();
        let ca = cp.self_signed(&cak).unwrap();
        std::fs::write(root.join("ca.pem"), ca.pem()).unwrap();
        let mut leaves = BTreeMap::new();
        for id in 1..=count {
            let mut p = CertificateParams::new(vec!["localhost".into()]).unwrap();
            p.key_usages = vec![KeyUsagePurpose::DigitalSignature];
            p.extended_key_usages = vec![
                ExtendedKeyUsagePurpose::ServerAuth,
                ExtendedKeyUsagePurpose::ClientAuth,
            ];
            let k = KeyPair::generate().unwrap();
            let c = p.signed_by(&k, &ca, &cak).unwrap();
            std::fs::write(root.join(format!("{id}.pem")), c.pem()).unwrap();
            std::fs::write(root.join(format!("{id}.key.pem")), k.serialize_pem()).unwrap();
            leaves.insert(id, fingerprint(c.der()));
        }
        let mut configs = BTreeMap::new();
        let mut per_node = BTreeMap::<u64, Vec<GroupConfig>>::new();
        let mut listeners = Vec::new();
        for group in 1..=4 {
            let mut peers = BTreeMap::new();
            for id in 1..=count {
                let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
                peers.insert(
                    id,
                    Peer {
                        address: listener.local_addr().unwrap().to_string(),
                        fingerprint: leaves[&id].clone(),
                    },
                );
                listeners.push(listener);
            }
            for id in 1..=count {
                per_node.entry(id).or_default().push(GroupConfig {
                    initial: match group {
                        1 => Initial::Directory(descriptor()),
                        2 => Initial::Data(descriptor()),
                        _ => Initial::Vacant,
                    },
                    peer: Configuration {
                        id,
                        domain: 42,
                        group,
                        database: root.join(format!("node-{id}-group-{group}.redb")),
                        listen: peers[&id].address.clone(),
                        certificate: root.join(format!("{id}.pem")),
                        key: root.join(format!("{id}.key.pem")),
                        ca: root.join("ca.pem"),
                        peers: peers.clone(),
                    },
                });
            }
        }
        for (id, groups) in per_node {
            let path = root.join(format!("config-{id}.json"));
            std::fs::write(
                &path,
                serde_json::to_vec_pretty(&Config {
                    domain: 42,
                    node: id,
                    groups,
                })
                .unwrap(),
            )
            .unwrap();
            configs.insert(id, path);
        }
        drop(listeners);
        let mut s = Self {
            root,
            configs,
            nodes: BTreeMap::new(),
            round: 0,
        };
        s.start();
        s
    }
    fn start(&mut self) {
        self.round += 1;
        for id in self.configs.keys().copied().collect::<Vec<_>>() {
            self.nodes.insert(
                id,
                Running::start(&self.configs[&id], &self.root, id, self.round),
            );
        }
        self.colocate();
    }
    fn restart(&mut self) {
        self.nodes = BTreeMap::new();
        self.start();
    }
    fn leader(&mut self, group: u64) -> u64 {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            for id in self.configs.keys().copied().collect::<Vec<_>>() {
                let v = self
                    .nodes
                    .get_mut(&id)
                    .unwrap()
                    .request(json!({"command":"inspect","group":group}));
                if v["ok"] == true && v["data"]["is_leader"] == true {
                    return id;
                }
            }
            assert!(Instant::now() < deadline, "no group leader {group}");
        }
    }
    fn colocate(&mut self) {
        for group in 1..=4 {
            self.nodes
                .get_mut(&1)
                .unwrap()
                .request(json!({"command":"campaign","group":group}));
            let deadline = Instant::now() + Duration::from_secs(20);
            loop {
                let leader = self.leader(group);
                if leader == 1 {
                    break;
                }
                let r = self
                    .nodes
                    .get_mut(&leader)
                    .unwrap()
                    .request(json!({"command":"transfer","group":group,"to":1}));
                // Leadership can change after discovery during restart.
                // Retry the trusted transfer against the next discovered leader.
                assert!(Instant::now() < deadline, "transfer timeout: {r}");
            }
        }
    }
    fn request(&mut self, v: Value) -> Value {
        self.nodes.get_mut(&1).unwrap().request(v)
    }
    fn current(&mut self, g: u64) -> Machine {
        let v = self.request(json!({"command":"current","group":g}));
        assert_eq!(v["ok"], true, "{v}");
        serde_json::from_value(v["data"]["machine"].clone()).unwrap()
    }
    fn apply(&mut self, g: u64, action: Action) -> Value {
        let v = self.request(json!({"command":"apply","group":g,"action":action}));
        assert_eq!(v["ok"], true, "{v}");
        v
    }
    fn data(&mut self, g: u64) -> Data {
        let Machine::Range { data, .. } = self.current(g) else {
            panic!()
        };
        *data
    }
    fn resume(&mut self) {
        let r = self.request(json!({"command":"resume","id":77}));
        assert_eq!(r["ok"], true, "{r}");
    }
}

#[test]
fn real_three_process_split_restart_at_every_durable_lifecycle_boundary() {
    for boundary in 0..8 {
        let mut p = Processes::new();
        // Oracle comes from the requested independent fixture, not a sum of child
        // metadata. Version 999 is deliberately ignored by core put semantics.
        let fixture = [1, 49, 50, 99];
        for k in fixture {
            p.apply(2, put(descriptor(), k, key(k), None));
        }
        p.apply(2, put(descriptor(), 1000, key(1), Some(0)));
        let expected = fixture
            .into_iter()
            .map(|n| {
                let mut r = row();
                r.version = if n == 1 { 1 } else { 0 };
                (key(n), r)
            })
            .collect::<BTreeMap<_, _>>();
        std::fs::write(
            p.root.join("oracle.json"),
            serde_json::to_vec(&expected.iter().collect::<Vec<_>>()).unwrap(),
        )
        .unwrap();
        let s = split();
        let mut step = 0;
        let crash = |p: &mut Processes, step: &mut usize| {
            if *step == boundary {
                p.restart();
            }
            *step += 1;
        };
        crash(&mut p, &mut step);
        p.apply(1, Action::Begin(s.clone()));
        crash(&mut p, &mut step);
        p.apply(2, Action::Fence(s.clone()));
        crash(&mut p, &mut step);
        let children = p.data(2).children().unwrap();
        let proofs = [
            children[0].provenance.clone().unwrap(),
            children[1].provenance.clone().unwrap(),
        ];
        for child in children {
            p.apply(child.descriptor.group, Action::Bootstrap(Box::new(child)));
            crash(&mut p, &mut step);
        }
        p.apply(
            1,
            Action::Publish {
                id: 77,
                ready: Box::new(proofs.clone()),
            },
        );
        crash(&mut p, &mut step);
        for proof in proofs {
            p.apply(proof.child.group, Action::Activate { proof, revision: 2 });
            crash(&mut p, &mut step);
        }
        p.restart();
        p.resume();
        p.resume();
        let actual = p
            .data(3)
            .image
            .snapshot()
            .unwrap()
            .rows
            .into_iter()
            .chain(p.data(4).image.snapshot().unwrap().rows)
            .collect::<BTreeMap<_, _>>();
        assert_eq!(actual, expected);
        for g in [3, 4] {
            let snapshot = p.data(g).image.snapshot().unwrap();
            assert!(
                snapshot.tasks.is_empty()
                    && snapshot.operations.is_empty()
                    && snapshot.approvals.is_empty()
            );
        }
        let old = p.request(
            json!({"command":"apply","group":2,"action":put(descriptor(),5000,key(2),None)}),
        );
        assert_eq!(old["ok"], false);
        let task = p.request(json!({"command":"apply","group":3,"action":Action::TaskWrite}));
        assert_eq!(task["ok"], false);
        let scan =
            p.request(json!({"command":"observe","bounds":Bounds{start:minimum(),end:None}}));
        assert_eq!(scan["ok"], true, "{scan}");
        let observed: Vec<(giggles_core::Key, giggles_core::Row)> =
            serde_json::from_value(scan["data"].clone()).unwrap();
        assert_eq!(observed.into_iter().collect::<BTreeMap<_, _>>(), expected);
        p.apply(3, put(s.left, 6000, key(2), None));
        p.apply(4, put(s.right, 6001, key(60), None));
        let ck = p.request(json!({"command":"checkpoint","group":3}));
        assert_eq!(ck["ok"], true);
        p.restart();
        assert!(
            p.data(3)
                .image
                .snapshot()
                .unwrap()
                .rows
                .contains_key(&key(2))
        );
    }
}

#[test]
fn real_process_independent_group_partition_and_one_child_ready_recovery() {
    let mut p = Processes::new();
    let s = split();
    p.apply(1, Action::Begin(s.clone()));
    p.apply(2, put(descriptor(), 1, key(1), None));
    p.apply(2, Action::Fence(s));
    let children = p.data(2).children().unwrap();
    p.apply(3, Action::Bootstrap(Box::new(children[0].clone())));
    // Isolate only right-child group, leaving directory/left independently live.
    for id in VOTERS {
        let peers = VOTERS
            .into_iter()
            .filter(|other| *other != id)
            .collect::<Vec<_>>();
        let r = p
            .nodes
            .get_mut(&id)
            .unwrap()
            .request(json!({"command":"block","group":4,"peers":peers}));
        assert_eq!(r["ok"], true);
    }
    let noquorum = p.request(json!({"command":"resume","id":77}));
    assert_eq!(noquorum["ok"], false, "{noquorum}");
    let Machine::Root { directory, .. } = p.current(1) else {
        panic!()
    };
    assert_eq!(directory.revision, 1);
    assert!(!directory.intents[&77].published);
    assert_eq!(p.data(3).descriptor.range, 11);
    let scan = p.request(json!({"command":"observe","bounds":Bounds{start:minimum(),end:None}}));
    assert_eq!(scan["ok"], false);
    assert!(scan.get("data").is_none());
    p.restart();
    p.resume();
    let Machine::Root { directory, .. } = p.current(1) else {
        panic!()
    };
    assert_eq!(directory.revision, 2);
    assert_eq!(p.data(3).image.snapshot().unwrap().rows[&key(1)].version, 0);
}

#[test]
fn concurrent_routed_write_and_split_fence_has_one_complete_outcome() {
    let mut p = Processes::new();
    p.apply(1, Action::Begin(split()));
    let route = descriptor();
    let race=p.request(json!({"command":"concurrent","group":2,"actions":[put(route.clone(),1,key(1),None),Action::Fence(split())]}));
    assert_eq!(race["ok"], true, "{race}");
    assert_eq!(race["data"].as_array().unwrap().len(), 2);
    p.resume();
    let left = p.data(3).image.snapshot().unwrap().rows;
    let successful = race["data"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|r| r["reply"]["code"] == 0)
        .count();
    assert_eq!(successful, if left.contains_key(&key(1)) { 2 } else { 1 });
    let attempt = p.request(json!({"command":"apply","group":2,"action":put(route,2,key(2),None)}));
    assert_eq!(attempt["ok"], false);
}

#[test]
fn real_process_crash_before_and_after_each_applied_lifecycle_save() {
    for after in [false, true] {
        for stage in 0..7 {
            let mut p = Processes::new();
            let s = split();
            p.apply(2, put(descriptor(), 1, key(1), None));
            let mut actions = vec![(1, Action::Begin(s.clone())), (2, Action::Fence(s.clone()))];
            if stage >= 2 {
                for (g, a) in actions.drain(..) {
                    p.apply(g, a);
                }
            }
            let children = if stage >= 2 {
                Some(p.data(2).children().unwrap())
            } else {
                None
            };
            if let Some(children) = children {
                let proofs = [
                    children[0].provenance.clone().unwrap(),
                    children[1].provenance.clone().unwrap(),
                ];
                actions.extend(
                    children
                        .into_iter()
                        .map(|child| (child.descriptor.group, Action::Bootstrap(Box::new(child)))),
                );
                actions.push((
                    1,
                    Action::Publish {
                        id: 77,
                        ready: Box::new(proofs.clone()),
                    },
                ));
                actions.extend(
                    proofs
                        .into_iter()
                        .map(|proof| (proof.child.group, Action::Activate { proof, revision: 2 })),
                );
            }
            let offset = if stage >= 2 { 2 } else { 0 };
            let (g, target) = actions.remove(stage - offset);
            for (group, action) in actions.into_iter().take(stage - offset) {
                p.apply(group, action);
            }
            let arm = p.request(json!({"command":"arm_crash","group":g,"after":after}));
            assert_eq!(arm["ok"], true, "{arm}");
            let leader = p.nodes.get_mut(&1).unwrap();
            leader.send(json!({"command":"apply","group":g,"action":target}));
            let status = leader.child.wait().unwrap();
            assert_eq!(status.code(), Some(86));
            // Kill the remaining processes before recovery: their logs may contain a
            // committed or uncommitted tail. No timeout is interpreted as rollback.
            p.restart();
            p.apply(1, Action::Begin(s));
            p.resume();
            assert_eq!(p.data(3).image.snapshot().unwrap().rows[&key(1)].version, 0);
            assert!(p.data(4).image.snapshot().unwrap().rows.is_empty());
        }
    }
}

#[test]
fn ownership_change_during_observation_returns_retry_without_partial_rows() {
    let mut p = Processes::new();
    p.apply(2, put(descriptor(), 1, key(1), None));
    p.apply(2, put(descriptor(), 2, key(99), None));
    p.apply(1, Action::Begin(split()));
    let answer = p.request(
        json!({"command":"observe_resume","bounds":Bounds{start:minimum(),end:None},"id":77}),
    );
    assert_eq!(answer["ok"], false, "{answer}");
    assert!(answer.get("data").is_none());
    assert!(
        answer["error"]
            .as_str()
            .unwrap()
            .contains("ownership changed")
    );
    let retry = p.request(json!({"command":"observe","bounds":Bounds{start:minimum(),end:None}}));
    assert_eq!(retry["ok"], true);
    assert_eq!(retry["data"].as_array().unwrap().len(), 2);
}

#[test]
fn real_process_snapshot_plus_tail_catches_up_independent_group() {
    let mut p = Processes::new();
    for id in VOTERS {
        let peers = if id == 3 { vec![1, 2] } else { vec![3] };
        let answer = p
            .nodes
            .get_mut(&id)
            .unwrap()
            .request(json!({"command":"block","group":2,"peers":peers}));
        assert_eq!(answer["ok"], true);
    }
    for n in 1..=8 {
        p.apply(2, put(descriptor(), n, key(n), None));
    }
    let checkpoint = p.request(json!({"command":"checkpoint","group":2}));
    assert_eq!(checkpoint["ok"], true);
    // The fence is a log tail after the compacted image.
    p.apply(1, Action::Begin(split()));
    p.apply(2, Action::Fence(split()));
    for id in VOTERS {
        let answer = p
            .nodes
            .get_mut(&id)
            .unwrap()
            .request(json!({"command":"block","group":2,"peers":[]}));
        assert_eq!(answer["ok"], true);
    }
    let expected = p.current(2);
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let observed = p
            .nodes
            .get_mut(&3)
            .unwrap()
            .request(json!({"command":"inspect","group":2}));
        assert_eq!(observed["ok"], true);
        let machine: Machine = serde_json::from_value(observed["data"]["machine"].clone()).unwrap();
        if machine == expected {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "snapshot/tail follower not caught up"
        );
    }
    p.restart();
    p.resume();
    assert_eq!(p.data(3).image.snapshot().unwrap().rows.len(), 8);
}

#[test]
fn different_process_group_leaders_survive_leader_process_loss() {
    let mut p = Processes::new();
    let transfer = p.request(json!({"command":"transfer","group":2,"to":2}));
    assert_eq!(transfer["ok"], true);
    let deadline = Instant::now() + Duration::from_secs(15);
    while p.leader(2) != 2 {
        assert!(Instant::now() < deadline);
    }
    let write = p
        .nodes
        .get_mut(&2)
        .unwrap()
        .request(json!({"command":"apply","group":2,"action":put(descriptor(),1,key(1),None)}));
    assert_eq!(write["ok"], true, "{write}");
    // Independent groups may legitimately elect or transfer independently;
    // establish distinct leaders without assuming an immutable node location.
    while p.leader(1) == 2 {
        let transfer = p
            .nodes
            .get_mut(&2)
            .unwrap()
            .request(json!({"command":"transfer","group":1,"to":1}));
        assert_eq!(transfer["ok"], true);
        assert!(Instant::now() < deadline, "directory transfer timeout");
    }
    drop(p.nodes.remove(&2));
    let deadline = Instant::now() + Duration::from_secs(15);
    let leader = loop {
        let mut elected = None;
        for id in [1, 3] {
            let o = p
                .nodes
                .get_mut(&id)
                .unwrap()
                .request(json!({"command":"inspect","group":2}));
            if o["data"]["is_leader"] == true {
                elected = Some(id);
                break;
            }
        }
        if let Some(id) = elected {
            break id;
        }
        assert!(Instant::now() < deadline, "survivors did not elect");
    };
    let write = p
        .nodes
        .get_mut(&leader)
        .unwrap()
        .request(json!({"command":"apply","group":2,"action":put(descriptor(),2,key(2),None)}));
    assert_eq!(write["ok"], true, "{write}");
    let deadline = Instant::now() + Duration::from_secs(15);
    'directory: loop {
        for id in [1, 3] {
            let directory = p
                .nodes
                .get_mut(&id)
                .unwrap()
                .request(json!({"command":"current","group":1}));
            if directory["ok"] == true {
                assert_eq!(directory["data"]["quorum_checked"], true);
                break 'directory;
            }
        }
        assert!(
            Instant::now() < deadline,
            "surviving directory quorum unavailable"
        );
    }
    p.restart();
    let rows = p.data(2).image.snapshot().unwrap().rows;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[&key(1)].version, 0);
    assert_eq!(rows[&key(2)].version, 0);
}

#[tokio::test]
async fn real_mtls_peer_cannot_rewrap_foreign_group_log_or_envelope() {
    let mut p = Processes::new();
    let config: Config = serde_json::from_slice(&std::fs::read(&p.configs[&2]).unwrap()).unwrap();
    let peer = &config
        .groups
        .iter()
        .find(|g| g.peer.group == 2)
        .unwrap()
        .peer;
    let mut roots = rustls::RootCertStore::empty();
    for cert in rustls_pemfile::certs(&mut BufReader::new(
        std::fs::File::open(p.root.join("ca.pem")).unwrap(),
    )) {
        roots.add(cert.unwrap()).unwrap();
    }
    let chain = rustls_pemfile::certs(&mut BufReader::new(
        std::fs::File::open(p.root.join("1.pem")).unwrap(),
    ))
    .collect::<std::result::Result<Vec<_>, _>>()
    .unwrap();
    let client_key = rustls_pemfile::private_key(&mut BufReader::new(
        std::fs::File::open(p.root.join("1.key.pem")).unwrap(),
    ))
    .unwrap()
    .unwrap();
    let client = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_client_auth_cert(chain, client_key)
        .unwrap();
    let connector = tokio_rustls::TlsConnector::from(std::sync::Arc::new(client));
    for mode in 0..3 {
        let tcp = tokio::net::TcpStream::connect(&peer.listen).await.unwrap();
        let mut tls = connector
            .connect(
                rustls::pki_types::ServerName::try_from("localhost").unwrap(),
                tcp,
            )
            .await
            .unwrap();
        let mut message = raft::eraftpb::Message {
            from: 1,
            to: 2,
            term: 999,
            ..Default::default()
        };
        message.set_msg_type(raft::eraftpb::MessageType::MsgAppend);
        message.mut_entries().push(raft::eraftpb::Entry {
            index: 1,
            term: 999,
            data: serde_json::to_vec(&common::command(3, Action::TaskWrite))
                .unwrap()
                .into(),
            ..Default::default()
        });
        let bytes = transport::encode_message(
            if mode == 0 { 43 } else { 42 },
            if mode == 1 { 3 } else { 2 },
            &message,
        )
        .unwrap();
        transport::write_frame(&mut tls, &bytes).await.unwrap();
        let reply = transport::read_frame(&mut tls).await;
        if mode < 2 {
            assert!(reply.is_err());
        } else {
            assert_eq!(reply.unwrap(), b"ok");
        }
    }
    // Enqueue a diagnostic behind the foreign log and prove the group remains
    // alive without applying it or accepting its term.
    let inspected = p
        .nodes
        .get_mut(&2)
        .unwrap()
        .request(json!({"command":"inspect","group":2}));
    assert_eq!(inspected["ok"], true);
    p.apply(2, put(descriptor(), 1, key(1), None));
    let rows = p.data(2).image.snapshot().unwrap().rows;
    assert_eq!(rows.len(), 1);
    let vacant = p.current(3);
    assert!(matches!(vacant, Machine::Vacant { .. }));
}

#[test]
fn restart_never_reuses_log_context_for_a_different_pending_attempt() {
    use raft::{GetEntriesContext, Storage};
    let mut p = Processes::new();
    p.apply(2, put(descriptor(), 1, key(1), None));
    p.restart();
    let replay = p.apply(2, put(descriptor(), 1, key(1), None));
    assert_eq!(replay["data"]["reply"]["data"]["version"], 0);
    let update = p.apply(2, put(descriptor(), 2, key(1), Some(0)));
    assert_eq!(update["data"]["reply"]["data"]["version"], 1);
    let config: Config = serde_json::from_slice(&std::fs::read(&p.configs[&1]).unwrap()).unwrap();
    let path = &config
        .groups
        .iter()
        .find(|g| g.peer.group == 2)
        .unwrap()
        .peer
        .database;
    p.nodes = BTreeMap::new();
    let store = Store::open(path, 1, 42, 2).unwrap();
    let mut contexts = std::collections::BTreeSet::new();
    let mut attempts = 0;
    for entry in store
        .entries(
            store.first_index().unwrap(),
            store.last_index().unwrap() + 1,
            None,
            GetEntriesContext::empty(false),
        )
        .unwrap()
    {
        if entry.data.is_empty() {
            continue;
        }
        let c: giggles_distributed::Command = serde_json::from_slice(&entry.data).unwrap();
        if matches!(c.action, Action::Put { .. }) {
            attempts += 1;
            assert!(
                contexts.insert(entry.context.to_vec()),
                "a retained old log entry can collide with a new pending request after restart"
            );
        }
    }
    assert_eq!(attempts, 3);
    std::fs::write(
        p.root.join("request-contexts.json"),
        serde_json::to_vec(&contexts).unwrap(),
    )
    .unwrap();
}

impl Processes {
    fn member_current(&mut self, group: u64) -> Value {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let r = self.owner(group, json!({"command":"current","group":group}));
            if r["ok"] == true {
                return r;
            }
            assert!(Instant::now() < deadline, "current after election: {r}");
        }
    }
    fn member(&mut self, group: u64, id: u64) -> Value {
        self.owner(
            group,
            json!({"command":"membership_resume","group":group,"id":id}),
        )
    }
    fn member_until(&mut self, group: u64, id: u64, phase: &str, ready: bool) -> Value {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let current = self.owner(group, json!({"command":"current","group":group}));
            if current["ok"] == true {
                let m: giggles_membership::Membership =
                    serde_json::from_value(current["data"]["membership"].clone()).unwrap();
                let p = &m.receipts[&id];
                if format!("{:?}", p.phase) == phase && (!ready || p.readiness.is_some()) {
                    return current;
                }
            }
            let result = self.member(group, id);
            assert!(
                Instant::now() < deadline,
                "membership timeout {phase}: {result}; artifacts {}",
                self.root.display()
            );
        }
    }
    fn block_group(&mut self, group: u64, partitions: &[(u64, Vec<u64>)]) {
        for (id, peers) in partitions {
            let r = self
                .nodes
                .get_mut(id)
                .unwrap()
                .request(json!({"command":"block","group":group,"peers":peers}));
            assert_eq!(r["ok"], true, "{r}");
        }
    }
    fn restart_unaligned(&mut self) {
        self.nodes = BTreeMap::new();
        self.round += 1;
        for id in self.configs.keys().copied().collect::<Vec<_>>() {
            self.nodes.insert(
                id,
                Running::start(&self.configs[&id], &self.root, id, self.round),
            );
        }
    }
}

#[test]
fn real_four_process_membership_full_transaction_snapshot_tail_joint_restart_removed_leader() {
    let mut p = Processes::with_members(4);
    p.apply(1, Action::Begin(split()));
    p.resume();
    let plan = transaction_plan(26001);
    let committed = p.request(json!({"command":"transact","plan":plan,"outcome":"Commit"}));
    assert_eq!(committed["ok"], true, "{committed}");
    p.block_group(
        3,
        &[(1, vec![4]), (2, vec![4]), (3, vec![4]), (4, vec![1, 2, 3])],
    );
    let intent = giggles_membership::Intent {
        id: 2601,
        epoch: 1,
        learner: 4,
        remove: 1,
    };
    let begin = p.request(json!({"command":"membership_begin","group":3,"intent":intent}));
    assert_eq!(begin["ok"], true, "{begin}");
    let duplicate = p.request(json!({"command":"membership_begin","group":3,"intent":intent}));
    assert_eq!(duplicate["ok"], true, "{duplicate}");
    let mut conflict = intent.clone();
    conflict.remove = 2;
    let refused = p.request(json!({"command":"membership_begin","group":3,"intent":conflict}));
    assert_eq!(refused["ok"], false, "{refused}");
    p.member_until(3, intent.id, "CatchingUp", false);
    let checkpoint = p.request(json!({"command":"checkpoint","group":3}));
    assert_eq!(checkpoint["ok"], true, "{checkpoint}");
    p.apply(3, put(plan.home.clone(), 2602, key(2), None));
    let refused = p.member(3, intent.id);
    assert_eq!(
        refused["ok"], false,
        "partition cannot assert durable readiness: {refused}"
    );
    p.block_group(3, &[(1, vec![]), (2, vec![]), (3, vec![]), (4, vec![])]);
    let ready = p.member_until(3, intent.id, "CatchingUp", true);
    let learner = p
        .nodes
        .get_mut(&4)
        .unwrap()
        .request(json!({"command":"inspect","group":3}));
    assert_eq!(learner["ok"], true, "{learner}");
    assert_eq!(
        learner["data"]["snapshot_index"], checkpoint["data"]["applied"],
        "learner must install the checkpoint, then actual row tail"
    );
    assert!(
        learner["data"]["applied"].as_u64().unwrap()
            > learner["data"]["snapshot_index"].as_u64().unwrap()
    );
    let machine: Machine = serde_json::from_value(learner["data"]["machine"].clone()).unwrap();
    let Machine::Range { data, .. } = machine else {
        panic!()
    };
    assert_eq!(data.image.snapshot().unwrap().rows.len(), 2);
    assert_eq!(data.transactions.record(plan.id()).unwrap().plan, plan);
    assert_eq!(data.descriptor, plan.home);
    assert!(learner["data"]["applied"].as_u64().unwrap() >= ready["data"]["membership"]["receipts"][intent.id.to_string()]["readiness"]["barrier"].as_u64().unwrap_or(0));
    p.member_until(3, intent.id, "Joint", false);
    p.restart_unaligned();
    let joint = p.member_current(3);
    let m: giggles_membership::Membership =
        serde_json::from_value(joint["data"]["membership"].clone()).unwrap();
    assert_eq!(
        m.progress().unwrap().phase,
        giggles_membership::Phase::Joint
    );
    p.member_until(3, intent.id, "Complete", false);
    // A published split can be resumed after an already-active child replaces
    // a voter; replaying activation must not reset its configuration or homes.
    let activation=p.owner(3,json!({"command":"apply","group":3,"action":Action::Activate{proof:data.provenance.clone().unwrap(),revision:2}}));
    assert_eq!(
        activation["ok"], true,
        "activation recovery replay: {activation}"
    );
    let leader = p.leader(3);
    assert_ne!(leader, 1);
    for command in ["current", "campaign"] {
        let refused = p
            .nodes
            .get_mut(&1)
            .unwrap()
            .request(json!({"command":command,"group":3}));
        if command == "current" {
            assert_eq!(refused["ok"], false, "removed current authority: {refused}");
        }
    }
    let refused = p.nodes.get_mut(&1).unwrap().request(
        json!({"command":"apply","group":3,"action":put(plan.home.clone(),2603,key(3),None)}),
    );
    assert_eq!(refused["ok"], false, "{refused}");
    let status = p.owner(
        3,
        json!({"command":"tx_status","home":plan.home,"id":plan.id()}),
    );
    assert_eq!(status["ok"], true, "{status}");
    let retry = p.owner(3,json!({"command":"tx_step","request":transaction::Request{route:plan.home.clone(),step:transaction::Step::Open(plan.clone()),evidence:vec![]}}));
    assert_eq!(retry["ok"], true, "{retry}");
    let checkpoint = p.owner(3, json!({"command":"checkpoint","group":3}));
    assert_eq!(checkpoint["ok"], true, "{checkpoint}");
    p.restart_unaligned();
    let after = p.member_current(3);
    let machine: Machine = serde_json::from_value(after["data"]["machine"].clone()).unwrap();
    let Machine::Range { data, .. } = machine else {
        panic!()
    };
    assert_eq!(data.image.snapshot().unwrap().rows.len(), 2);
    assert_eq!(data.transactions.record(plan.id()).unwrap().plan, plan);
    assert_eq!(data.descriptor, plan.home);
    let m: giggles_membership::Membership =
        serde_json::from_value(after["data"]["membership"].clone()).unwrap();
    assert_eq!(m.voters, vec![2, 3, 4]);
    assert_eq!(m.epoch, 2);
    let replay = p.member(3, intent.id);
    assert_eq!(replay["ok"], true, "{replay}");
    let replay = p.owner(
        3,
        json!({"command":"membership_begin","group":3,"intent":intent}),
    );
    assert_eq!(
        replay["ok"], true,
        "lost reply exact intent retry: {replay}"
    );
}

#[test]
fn real_four_process_joint_requires_both_majorities_and_exact_retry_after_partition() {
    let mut p = Processes::with_members(4);
    let intent = giggles_membership::Intent {
        id: 2610,
        epoch: 1,
        learner: 4,
        remove: 1,
    };
    let begin = p.request(json!({"command":"membership_begin","group":2,"intent":intent}));
    assert_eq!(begin["ok"], true, "{begin}");
    p.member_until(2, intent.id, "Joint", false);
    p.block_group(
        2,
        &[
            (1, vec![3, 4]),
            (2, vec![3, 4]),
            (3, vec![1, 2]),
            (4, vec![1, 2]),
        ],
    );
    let attempted =
        p.nodes.get_mut(&1).unwrap().request(
            json!({"command":"apply","group":2,"action":put(descriptor(),2611,key(1),None)}),
        );
    assert_eq!(
        attempted["ok"], false,
        "old majority alone cannot commit: {attempted}"
    );
    for id in 1..=4 {
        let read = p
            .nodes
            .get_mut(&id)
            .unwrap()
            .request(json!({"command":"current","group":2}));
        assert_eq!(read["ok"], false, "no partition has dual majority: {read}");
    }
    p.block_group(2, &[(1, vec![]), (2, vec![]), (3, vec![]), (4, vec![])]);
    let retry = p.owner(
        2,
        json!({"command":"apply","group":2,"action":put(descriptor(),2611,key(1),None)}),
    );
    assert_eq!(retry["ok"], true, "{retry}");
    p.member_until(2, intent.id, "Complete", false);
    let current = p.member_current(2);
    let Machine::Range { data, .. } =
        serde_json::from_value(current["data"]["machine"].clone()).unwrap()
    else {
        panic!()
    };
    assert_eq!(data.image.snapshot().unwrap().rows.len(), 1);
    assert_eq!(data.image.snapshot().unwrap().rows[&key(1)].version, 0);
}

#[test]
fn real_four_process_each_configuration_publication_crash_before_after_and_recovery() {
    for stage in 0..3 {
        for after in [false, true] {
            let mut p = Processes::with_members(4);
            let id = 2620 + stage;
            let intent = giggles_membership::Intent {
                id,
                epoch: 1,
                learner: 4,
                remove: 1,
            };
            p.apply(2, put(descriptor(), 1, key(1), None));
            let begin = p.request(json!({"command":"membership_begin","group":2,"intent":intent}));
            assert_eq!(begin["ok"], true, "{begin}");
            let phase = match stage {
                0 => "Planned",
                1 => "CatchingUp",
                _ => "Joint",
            };
            p.member_until(2, id, phase, stage == 1);
            // Ensure follower2 has exactly the pre-publication controller before
            // arming its next applied frontier. No unrelated data is proposed.
            let deadline = Instant::now() + Duration::from_secs(15);
            loop {
                let r = p
                    .nodes
                    .get_mut(&2)
                    .unwrap()
                    .request(json!({"command":"inspect","group":2}));
                let m: giggles_membership::Membership =
                    serde_json::from_value(r["data"]["membership"].clone()).unwrap();
                if m.receipts.get(&id).is_some_and(|r| {
                    format!("{:?}", r.phase) == phase && (stage != 1 || r.readiness.is_some())
                }) {
                    break;
                }
                assert!(Instant::now() < deadline, "follower pre-barrier {r}");
            }
            let armed = p
                .nodes
                .get_mut(&2)
                .unwrap()
                .request(json!({"command":"arm_crash","group":2,"after":after}));
            assert_eq!(armed["ok"], true, "{armed}");
            let _ = p.member(2, id);
            let deadline = Instant::now() + Duration::from_secs(15);
            loop {
                if let Some(exit) = p.nodes.get_mut(&2).unwrap().child.try_wait().unwrap() {
                    assert_eq!(exit.code(), Some(86));
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "follower failed to reach config crash stage{stage} after{after}"
                );
                std::thread::yield_now();
            }
            p.nodes.remove(&2); // Drop only this fixture's crashed process handle.
            p.round += 1;
            p.nodes
                .insert(2, Running::start(&p.configs[&2], &p.root, 2, p.round));
            p.member_until(2, id, "Complete", false);
            let current = p.member_current(2);
            let Machine::Range { data, .. } =
                serde_json::from_value(current["data"]["machine"].clone()).unwrap()
            else {
                panic!()
            };
            assert_eq!(data.image.snapshot().unwrap().rows[&key(1)].version, 0);
            p.restart_unaligned();
            let current = p.member_current(2);
            let m: giggles_membership::Membership =
                serde_json::from_value(current["data"]["membership"].clone()).unwrap();
            assert_eq!(m.voters, vec![2, 3, 4]);
        }
    }
}

#[test]
fn real_four_process_prepared_home_and_participant_recover_after_replacement() {
    use giggles_transactions::Outcome;
    let mut p = Processes::with_members(4);
    p.apply(1, Action::Begin(split()));
    p.resume();
    let plan = transaction_plan(26301);
    p.tx_begin_all(&plan);
    p.tx_prepare_all(&plan);
    p.block_group(
        3,
        &[(1, vec![4]), (2, vec![4]), (3, vec![4]), (4, vec![1, 2, 3])],
    );
    let intent = giggles_membership::Intent {
        id: 2630,
        epoch: 1,
        learner: 4,
        remove: 1,
    };
    let begin = p.owner(
        3,
        json!({"command":"membership_begin","group":3,"intent":intent}),
    );
    assert_eq!(begin["ok"], true, "{begin}");
    p.member_until(3, intent.id, "CatchingUp", false);
    let checkpoint = p.owner(3, json!({"command":"checkpoint","group":3}));
    assert_eq!(checkpoint["ok"], true, "{checkpoint}");
    p.block_group(3, &[(1, vec![]), (2, vec![]), (3, vec![]), (4, vec![])]);
    p.member_until(3, intent.id, "Complete", false);
    p.restart_unaligned();
    p.member_current(3);
    let pinned = p.pin(&plan.home, plan.id());
    assert_eq!(
        pinned
            .state
            .engine(42, &plan.home)
            .unwrap()
            .phase(plan.id()),
        Some(giggles_transactions::Phase::Prepared)
    );
    p.tx_finish_all(&plan, Outcome::Commit);
    for part in &plan.participants {
        let pinned = p.pin(&part.route, plan.id());
        assert_eq!(
            pinned
                .state
                .engine(42, &part.route)
                .unwrap()
                .phase(plan.id()),
            Some(giggles_transactions::Phase::Committed)
        );
        let current = p.member_current(part.route.group);
        let Machine::Range { data, .. } =
            serde_json::from_value(current["data"]["machine"].clone()).unwrap()
        else {
            panic!()
        };
        assert_eq!(
            data.image
                .snapshot()
                .unwrap()
                .rows
                .values()
                .next()
                .unwrap()
                .attempt_count,
            if part.route.group == 3 { 10 } else { 20 }
        );
    }
}

#[tokio::test]
async fn real_four_process_probe_rejects_wrong_certificate_group_term_intent_and_forged_owner_readiness()
 {
    let mut p = Processes::with_members(4);
    let intent = giggles_membership::Intent {
        id: 2640,
        epoch: 1,
        learner: 4,
        remove: 1,
    };
    let r = p.request(json!({"command":"membership_begin","group":2,"intent":intent}));
    assert_eq!(r["ok"], true, "{r}");
    let catching = p.member_until(2, intent.id, "CatchingUp", false);
    let barrier = catching["data"]["applied"].as_u64().unwrap();
    let term = catching["data"]["term"].as_u64().unwrap();
    let config: Config = serde_json::from_slice(&std::fs::read(&p.configs[&4]).unwrap()).unwrap();
    let target = &config
        .groups
        .iter()
        .find(|g| g.peer.group == 2)
        .unwrap()
        .peer
        .listen;
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let o = p
            .nodes
            .get_mut(&4)
            .unwrap()
            .request(json!({"command":"inspect","group":2}));
        if o["data"]["applied"].as_u64().unwrap() >= barrier {
            break;
        }
        assert!(Instant::now() < deadline, "learner application {o}");
    }
    for mode in 0..6 {
        let certificate = if mode == 0 { 2 } else { 1 };
        let mut roots = rustls::RootCertStore::empty();
        for cert in rustls_pemfile::certs(&mut BufReader::new(
            std::fs::File::open(p.root.join("ca.pem")).unwrap(),
        )) {
            roots.add(cert.unwrap()).unwrap();
        }
        let chain = rustls_pemfile::certs(&mut BufReader::new(
            std::fs::File::open(p.root.join(format!("{certificate}.pem"))).unwrap(),
        ))
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();
        let key = rustls_pemfile::private_key(&mut BufReader::new(
            std::fs::File::open(p.root.join(format!("{certificate}.key.pem"))).unwrap(),
        ))
        .unwrap()
        .unwrap();
        let client = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_client_auth_cert(chain, key)
            .unwrap();
        let connector = tokio_rustls::TlsConnector::from(std::sync::Arc::new(client));
        let tcp = tokio::net::TcpStream::connect(target).await.unwrap();
        let mut tls = connector
            .connect(
                rustls::pki_types::ServerName::try_from("localhost").unwrap(),
                tcp,
            )
            .await
            .unwrap();
        let request = json!({"domain":if mode==1{43}else{42},"group":if mode==2{3}else{2},"to":4,"probe":{"intent":if mode==3{2641}else{2640},"epoch":1,"term":if mode==4{term+1}else{term},"barrier":if mode==5{barrier+10000}else{barrier}}});
        let mut bytes = b"GDRPRO01".to_vec();
        bytes.extend(serde_json::to_vec(&request).unwrap());
        transport::write_frame(&mut tls, &bytes).await.unwrap();
        let response = transport::read_frame(&mut tls).await;
        if mode == 1 || mode == 2 {
            assert!(response.is_err());
        } else {
            let response: std::result::Result<giggles_membership::Readiness, String> =
                serde_json::from_slice(&response.unwrap()).unwrap();
            assert!(response.is_err(), "mode{mode}: {response:?}");
        }
    }
    let forged=p.request(json!({"command":"membership_ready","group":2,"intent":intent.id,"durable_applied":barrier,"term":term}));
    assert_eq!(
        forged["ok"], false,
        "owner cannot assert remote durability {forged}"
    );
    let current = p.member_current(2);
    let m: giggles_membership::Membership =
        serde_json::from_value(current["data"]["membership"].clone()).unwrap();
    assert!(m.progress().unwrap().readiness.is_none());
    p.member_until(2, intent.id, "Complete", false);
}

#[test]
fn real_four_process_leader_crashes_before_after_each_configuration_publication() {
    for stage in 0..3 {
        for after in [false, true] {
            let mut p = Processes::with_members(4);
            let id = 2650 + stage;
            let intent = giggles_membership::Intent {
                id,
                epoch: 1,
                learner: 4,
                remove: 1,
            };
            p.apply(2, put(descriptor(), 1, key(1), None));
            let r = p.request(json!({"command":"membership_begin","group":2,"intent":intent}));
            assert_eq!(r["ok"], true, "{r}");
            p.member_until(
                2,
                id,
                match stage {
                    0 => "Planned",
                    1 => "CatchingUp",
                    _ => "Joint",
                },
                stage == 1,
            );
            // Colocated initial leader remains1; a single resume now publishes
            // exactly the requested configuration, with no readiness write.
            assert_eq!(p.leader(2), 1);
            let arm = p.request(json!({"command":"arm_crash","group":2,"after":after}));
            assert_eq!(arm["ok"], true, "{arm}");
            p.nodes
                .get_mut(&1)
                .unwrap()
                .send(json!({"command":"membership_resume","group":2,"id":id}));
            let status = p.nodes.get_mut(&1).unwrap().child.wait().unwrap();
            assert_eq!(status.code(), Some(86));
            p.nodes.remove(&1);
            p.round += 1;
            p.nodes
                .insert(1, Running::start(&p.configs[&1], &p.root, 1, p.round));
            p.member_until(2, id, "Complete", false);
            let current = p.member_current(2);
            let Machine::Range { data, .. } =
                serde_json::from_value(current["data"]["machine"].clone()).unwrap()
            else {
                panic!()
            };
            assert_eq!(data.image.snapshot().unwrap().rows[&key(1)].version, 0);
            let m: giggles_membership::Membership =
                serde_json::from_value(current["data"]["membership"].clone()).unwrap();
            assert_eq!(m.voters, vec![2, 3, 4]);
        }
    }
}

fn protected_call(p: &mut Processes, group: u64, step: protected::Step) -> Value {
    p.owner(
        group,
        json!({"command":"protected_step","group":group,"step":step}),
    )
}
fn protected_ok(p: &mut Processes, group: u64, step: protected::Step) {
    let reply = protected_call(p, group, step);
    assert_eq!(reply["ok"], true, "{reply}");
}
fn protected_pin(p: &mut Processes, group: u64) -> protected::Pin {
    let reply = p.owner(group, json!({"command":"protected_pin","group":group}));
    assert_eq!(reply["ok"], true, "{reply}");
    serde_json::from_value(reply["data"].clone()).unwrap()
}
#[test]
fn real_protected_stage_independent_leaders_restart_and_abort_without_effects() {
    use protected::Step;
    let mut p = Processes::new();
    for n in [1, 60] {
        p.apply(2, put(descriptor(), n, key(n), None));
    }
    p.split_for_transactions();
    let s = split();
    let plan = protected_support::plan(vec![s.left.clone(), s.right.clone()], 2, 91);
    protected_ok(
        &mut p,
        1,
        Step::Install {
            image: protected_support::image(42, 2),
        },
    );
    protected_ok(&mut p, 1, Step::Open(plan.clone()));
    let home = protected_pin(&mut p, 1).for_home(plan.nonce).unwrap();
    // Actual process termination after authority admission, before row locking.
    p.restart();
    p.independent();
    for route in &plan.routes {
        protected_ok(
            &mut p,
            route.group,
            Step::Lock {
                plan: plan.clone(),
                home: Box::new(home.clone()),
            },
        );
    }
    let rows = vec![protected_pin(&mut p, 3), protected_pin(&mut p, 4)];
    protected_ok(
        &mut p,
        1,
        Step::Stage {
            nonce: 91,
            rows: rows.clone(),
        },
    );
    let staged = protected_pin(&mut p, 1);
    let Machine::Root { protected, .. } = staged.machine else {
        panic!()
    };
    assert!(protected.record(91).unwrap().staged.is_some());
    assert_eq!(
        protected.image.snapshot().unwrap().tasks[&giggles_core::TaskId(1)].transitions,
        0
    );
    assert!(protected.image.snapshot().unwrap().operations.is_empty());
    let rejected = protected_call(&mut p, 1, Step::Commit { nonce: 91 });
    assert_eq!(rejected["ok"], false, "{rejected}");
    assert!(rejected["error"].as_str().unwrap().contains("clock/pause"));
    // Caller cancellation/unknown result never silently aborts a retained home.
    p.restart();
    p.independent();
    protected_ok(&mut p, 1, Step::Stage { nonce: 91, rows });
    let mut altered = plan.clone();
    altered.intent.principal = giggles_core::Principal(99);
    assert_eq!(protected_call(&mut p, 1, Step::Open(altered))["ok"], false);
    // Ordinary strict Observe must release no result through a protected intent.
    p.colocate();
    let observed = p.request(json!({"command":"observe","bounds":{"start":minimum(),"end":null}}));
    assert_eq!(observed["ok"], false, "{observed}");
    // Durable abort followed by process termination BETWEEN participant unlocks.
    protected_ok(&mut p, 1, Step::Abort { nonce: 91 });
    let home = protected_pin(&mut p, 1).for_home(plan.nonce).unwrap();
    protected_ok(
        &mut p,
        3,
        Step::Unlock {
            plan: plan.clone(),
            home: Box::new(home.clone()),
        },
    );
    p.restart();
    let aborted = p.request(json!({"command":"protected_abort","nonce":91}));
    assert_eq!(aborted["ok"], true, "{aborted}");
    let again = p.request(json!({"command":"protected_abort","nonce":91}));
    assert_eq!(again["ok"], true, "{again}");
    for (group, k) in [(3, key(1)), (4, key(60))] {
        let data = p.data(group);
        assert_eq!(
            data.image.snapshot().unwrap().rows[&k].status,
            giggles_core::Status::Failed
        );
        assert!(!data.protected.busy());
        assert!(data.image.snapshot().unwrap().tasks.is_empty());
        let late = protected_call(
            &mut p,
            group,
            Step::Lock {
                plan: plan.clone(),
                home: Box::new(home.clone()),
            },
        );
        assert_eq!(late["ok"], false, "{late}");
    }
    let observed = p.request(json!({"command":"observe","bounds":{"start":minimum(),"end":null}}));
    assert_eq!(observed["ok"], true, "{observed}");
    assert_eq!(observed["data"].as_array().unwrap().len(), 2);
    // Retaining protected tombstones rejects new split intents, but never
    // invalidates exact receipts for the already-published original split.
    p.apply(1, Action::Begin(split()));
    p.resume();
}

#[test]
fn real_protected_partial_admission_abort_tombstone_and_revoke_serialization() {
    use protected::Step;
    let mut p = Processes::new();
    p.apply(1, Action::Begin(split()));
    p.resume();
    let s = split();
    let plan = protected_support::plan(vec![s.left, s.right], 2, 92);
    protected_ok(
        &mut p,
        1,
        Step::Install {
            image: protected_support::image(42, 2),
        },
    );
    protected_ok(&mut p, 1, Step::Open(plan.clone()));
    let old = protected_pin(&mut p, 1).for_home(plan.nonce).unwrap();
    protected_ok(
        &mut p,
        3,
        Step::Lock {
            plan: plan.clone(),
            home: Box::new(old.clone()),
        },
    );
    let command = serde_json::to_vec(&giggles_core::local::AdminCommand::Revoke {
        task: giggles_core::TaskId(1),
    })
    .unwrap();
    assert_eq!(
        protected_call(
            &mut p,
            1,
            Step::Admin {
                command: command.clone()
            }
        )["ok"],
        false
    );
    p.restart();
    let reply = p.request(json!({"command":"protected_abort","nonce":92}));
    assert_eq!(reply["ok"], true, "{reply}");
    assert_eq!(
        protected_call(
            &mut p,
            4,
            Step::Lock {
                plan: plan.clone(),
                home: Box::new(old)
            }
        )["ok"],
        false
    );
    protected_ok(&mut p, 1, Step::Admin { command });
    let pin = protected_pin(&mut p, 1);
    let Machine::Root { protected, .. } = pin.machine else {
        panic!()
    };
    assert!(protected.image.snapshot().unwrap().tasks[&giggles_core::TaskId(1)].revoked);
}

#[test]
fn real_protected_before_after_atomic_stage_and_abort_process_crashes() {
    use protected::Step;
    for after in [false, true] {
        for abort in [false, true] {
            let mut p = Processes::new();
            for n in [1, 60] {
                p.apply(2, put(descriptor(), n, key(n), None));
            }
            p.split_for_transactions();
            let s = split();
            let plan = protected_support::plan(vec![s.left, s.right], 2, 93);
            protected_ok(
                &mut p,
                1,
                Step::Install {
                    image: protected_support::image(42, 2),
                },
            );
            protected_ok(&mut p, 1, Step::Open(plan.clone()));
            let home = protected_pin(&mut p, 1).for_home(plan.nonce).unwrap();
            for route in &plan.routes {
                protected_ok(
                    &mut p,
                    route.group,
                    Step::Lock {
                        plan: plan.clone(),
                        home: Box::new(home.clone()),
                    },
                );
            }
            let rows = vec![protected_pin(&mut p, 3), protected_pin(&mut p, 4)];
            let step = if abort {
                Step::Abort { nonce: 93 }
            } else {
                Step::Stage { nonce: 93, rows }
            };
            let leader = p.leader(1);
            let node = p.nodes.get_mut(&leader).unwrap();
            let armed = node.request(json!({"command":"arm_crash","group":1,"after":after}));
            assert_eq!(armed["ok"], true);
            node.send(json!({"command":"protected_step","group":1,"step":step}));
            assert!(node.output.recv_timeout(Duration::from_secs(12)).is_err());
            assert_eq!(node.child.wait().unwrap().code(), Some(86));
            p.restart();
            // Only the original quorum home resolves a lost reply. Retrying the
            // same immutable step is valid whether the killed voter saved it or not.
            protected_ok(&mut p, 1, step);
            let result = p.request(json!({"command":"protected_abort","nonce":93}));
            assert_eq!(result["ok"], true, "{result}");
            for (g, k) in [(3, key(1)), (4, key(60))] {
                let data = p.data(g);
                assert_eq!(
                    data.image.snapshot().unwrap().rows[&k].status,
                    giggles_core::Status::Failed
                );
                assert!(!data.protected.busy());
            }
            let pin = protected_pin(&mut p, 1);
            let Machine::Root { protected, .. } = pin.machine else {
                panic!()
            };
            assert_eq!(
                protected.image.snapshot().unwrap().tasks[&giggles_core::TaskId(1)].transitions,
                0
            );
            assert!(protected.record(93).unwrap().released);
        }
    }
}

#[test]
fn real_protected_lock_survives_learner_snapshot_replacement_and_original_home_abort() {
    use protected::Step;
    let mut p = Processes::with_members(4);
    for n in [1, 60] {
        p.apply(2, put(descriptor(), n, key(n), None));
    }
    p.apply(1, Action::Begin(split()));
    p.resume();
    let s = split();
    let plan = protected_support::plan(vec![s.left, s.right], 2, 94);
    protected_ok(
        &mut p,
        1,
        Step::Install {
            image: protected_support::image(42, 2),
        },
    );
    protected_ok(&mut p, 1, Step::Open(plan.clone()));
    let home = protected_pin(&mut p, 1).for_home(plan.nonce).unwrap();
    for route in &plan.routes {
        protected_ok(
            &mut p,
            route.group,
            Step::Lock {
                plan: plan.clone(),
                home: Box::new(home.clone()),
            },
        );
    }
    let rejected=p.request(json!({"command":"membership_begin","group":1,"intent":{"id":3600,"epoch":1,"learner":4,"remove":3}}));
    assert_eq!(rejected["ok"], false);
    let checkpoint = p.request(json!({"command":"checkpoint","group":3}));
    assert_eq!(checkpoint["ok"], true);
    let intent = giggles_membership::Intent {
        id: 3601,
        epoch: 1,
        learner: 4,
        remove: 3,
    };
    let begin = p.request(json!({"command":"membership_begin","group":3,"intent":intent}));
    assert_eq!(begin["ok"], true, "{begin}");
    p.member_until(3, intent.id, "CatchingUp", true);
    p.member_until(3, intent.id, "Joint", true);
    p.member_until(3, intent.id, "Complete", true);
    let observed = p
        .nodes
        .get_mut(&4)
        .unwrap()
        .request(json!({"command":"inspect","group":3}));
    assert_eq!(observed["ok"], true, "{observed}");
    let machine: Machine = serde_json::from_value(observed["data"]["machine"].clone()).unwrap();
    let Machine::Range { data, .. } = machine else {
        panic!()
    };
    assert_eq!(data.descriptor, plan.routes[0]);
    assert_eq!(data.protected.records[0].plan, plan);
    assert!(data.protected.busy());
    p.restart();
    let rows = vec![protected_pin(&mut p, 3), protected_pin(&mut p, 4)];
    protected_ok(&mut p, 1, Step::Stage { nonce: 94, rows });
    let result = p.request(json!({"command":"protected_abort","nonce":94}));
    assert_eq!(result["ok"], true, "{result}");
    let home = protected_pin(&mut p, 1);
    let Machine::Root { protected, .. } = home.machine else {
        panic!()
    };
    assert!(protected.record(94).unwrap().released);
    assert_eq!(protected.record(94).unwrap().plan, plan);
}
fn logical_data(p: &mut Processes, group: u64) -> Data {
    let pin = protected_pin(p, group);
    let Machine::Range { data, .. } = pin.machine else {
        panic!()
    };
    *data
}
fn logical_ok(p: &mut Processes, g: u64, s: protected::logical::Step) {
    protected_ok(p, g, protected::Step::Logical(Box::new(s)));
}
fn logical_process_setup(nonce: u128) -> (Processes, protected::Plan) {
    use protected::{
        Step,
        logical::{LogicalProfile, Step as L},
    };
    let mut p = Processes::new();
    for n in [1, 60] {
        p.apply(2, put(descriptor(), n, key(n), None));
    }
    p.split_for_transactions();
    p.independent();
    let s = split();
    let mut plan = protected_support::plan(vec![s.left, s.right], 2, nonce);
    plan.intent.lower = 12;
    logical_ok(
        &mut p,
        1,
        L::Install {
            image: protected_support::image(42, 2),
            profile: LogicalProfile { epoch: 1, tick: 12 },
        },
    );
    logical_ok(&mut p, 1, L::Open(plan.clone()));
    let home = protected_pin(&mut p, 1).for_home(nonce).unwrap();
    for g in [3, 4] {
        protected_ok(
            &mut p,
            g,
            Step::Lock {
                plan: plan.clone(),
                home: Box::new(home.clone()),
            },
        );
    }
    let rows = vec![protected_pin(&mut p, 3), protected_pin(&mut p, 4)];
    protected_ok(&mut p, 1, Step::Stage { nonce, rows });
    (p, plan)
}
fn logical_process_prepare(p: &mut Processes, nonce: u128) -> Vec<protected::logical::Prepared> {
    let pin = protected_pin(p, 1);
    let mut proofs = Vec::new();
    for g in [3, 4] {
        logical_ok(
            p,
            g,
            protected::logical::Step::Prepare(pin.logical_preparation(nonce, g).unwrap()),
        );
        proofs.push(protected_pin(p, g).logical_prepared(nonce).unwrap());
    }
    proofs
}
fn logical_process_finish(p: &mut Processes, nonce: u128) {
    use protected::{Step, logical::Step as L};
    logical_ok(p, 1, L::VerifyDecision { nonce });
    let home = protected_pin(p, 1);
    let mut rows = Vec::new();
    for g in [3, 4] {
        let seal = home.logical_finalization(nonce, g).unwrap();
        logical_ok(p, g, L::Finalize(seal.clone()));
        rows.push(protected_pin(p, g).logical_finalized(seal).unwrap());
    }
    logical_ok(p, 1, L::ConfirmFinalized { nonce, rows });
    let home = protected_pin(p, 1).for_home(nonce).unwrap();
    let plan = home.plan.clone();
    for g in [3, 4] {
        protected_ok(
            p,
            g,
            Step::Unlock {
                plan: plan.clone(),
                home: Box::new(home.clone()),
            },
        );
    }
    let rows = [3, 4]
        .into_iter()
        .map(|g| protected_pin(p, g).for_release(nonce).unwrap())
        .collect();
    protected_ok(p, 1, Step::Complete { nonce, rows });
}
#[test]
fn real_three_process_logical_commit_independent_leaders_no_partial_results_and_output_debits() {
    use protected::{Step, logical::Step as L};
    let (mut p, plan) = logical_process_setup(201);
    let original = protected_pin(&mut p, 1);
    let Machine::Root { protected: h, .. } = &original.machine else {
        panic!()
    };
    let digest = h.record(201).unwrap().staged_digest;
    // Drop original owner pins, restart all actual listeners/stores and resume.
    drop(original);
    p.restart();
    p.independent();
    protected_ok(&mut p, 1, Step::ResumeStage { nonce: 201 });
    let rows = vec![protected_pin(&mut p, 3), protected_pin(&mut p, 4)];
    protected_ok(&mut p, 1, Step::Stage { nonce: 201, rows });
    let pin = protected_pin(&mut p, 1);
    let Machine::Root { protected: h, .. } = pin.machine else {
        panic!()
    };
    assert_eq!(h.record(201).unwrap().staged_digest, digest);
    let proofs = logical_process_prepare(&mut p, 201);
    logical_ok(
        &mut p,
        1,
        L::Commit {
            nonce: 201,
            prepared: proofs.clone(),
        },
    );
    let seal = protected_pin(&mut p, 1)
        .logical_finalization(201, 3)
        .unwrap();
    logical_ok(&mut p, 3, L::Finalize(seal));
    p.colocate();
    let result = p.request(json!({"command":"observe","bounds":Bounds {start:minimum(),end:None}}));
    assert_eq!(result["ok"], false, "{result}");
    assert!(
        result["error"].as_str().unwrap().contains("protected"),
        "{result}"
    );
    assert!(result.get("data").is_none());
    p.independent();
    assert!(logical_data(&mut p, 3).protected.busy() && logical_data(&mut p, 4).protected.busy());
    p.restart();
    let completed = p.request(json!({"command":"protected_logical_recover","nonce":201}));
    assert_eq!(completed["ok"], true, "{completed}");
    p.independent();
    logical_process_finish(&mut p, 201);
    logical_ok(
        &mut p,
        1,
        L::Commit {
            nonce: 201,
            prepared: proofs,
        },
    );
    logical_ok(&mut p, 1, L::Open(plan.clone()));
    for (g, k) in [(3, key(1)), (4, key(60))] {
        let data = logical_data(&mut p, g);
        assert_eq!(
            data.image.snapshot().unwrap().rows[&k].status,
            giggles_core::Status::Queued
        );
        assert_eq!(data.image.snapshot().unwrap().rows[&k].version, 1);
        assert!(!data.protected.busy());
    }
    let pin = protected_pin(&mut p, 1);
    let Machine::Root { protected: h, .. } = pin.machine else {
        panic!()
    };
    let used = h.image.accounting(giggles_core::TaskId(1)).unwrap();
    assert_eq!(
        h.image.snapshot().unwrap().tasks[&giggles_core::TaskId(1)].transitions,
        2
    );
    logical_ok(
        &mut p,
        1,
        L::Output {
            nonce: 201,
            principal: giggles_core::Principal(2),
        },
    );
    let pin = protected_pin(&mut p, 1);
    let Machine::Root { protected: h, .. } = pin.machine else {
        panic!()
    };
    assert_ne!(h.image.accounting(giggles_core::TaskId(1)).unwrap(), used);
    p.restart();
    let reply = protected_call(&mut p, 1, Step::Commit { nonce: 201 });
    assert_eq!(reply["ok"], false);
}
#[test]
fn real_three_process_logical_crash_prepare_decision_publication_finalize_reopens_and_converges() {
    use protected::{Step, logical::Step as L};
    // Every boundary is an actual process death, before or after its atomic
    // store/index transaction. Failed artifacts are retained by Processes.
    for boundary in 0..5 {
        for after in [false, true] {
            let nonce = 210 + boundary as u128 * 2 + u128::from(after);
            let (mut p, _) = logical_process_setup(nonce);
            let (group, step) = if boundary == 0 {
                (
                    3,
                    Step::Logical(Box::new(L::Prepare(
                        protected_pin(&mut p, 1)
                            .logical_preparation(nonce, 3)
                            .unwrap(),
                    ))),
                )
            } else {
                let proofs = logical_process_prepare(&mut p, nonce);
                if boundary == 1 {
                    (
                        1,
                        Step::Logical(Box::new(L::Commit {
                            nonce,
                            prepared: proofs,
                        })),
                    )
                } else {
                    logical_ok(
                        &mut p,
                        1,
                        L::Commit {
                            nonce,
                            prepared: proofs,
                        },
                    );
                    if boundary == 2 {
                        (
                            3,
                            Step::Logical(Box::new(L::Finalize(
                                protected_pin(&mut p, 1)
                                    .logical_finalization(nonce, 3)
                                    .unwrap(),
                            ))),
                        )
                    } else {
                        let home = protected_pin(&mut p, 1);
                        let mut rows = Vec::new();
                        for g in [3, 4] {
                            let seal = home.logical_finalization(nonce, g).unwrap();
                            logical_ok(&mut p, g, L::Finalize(seal.clone()));
                            rows.push(protected_pin(&mut p, g).logical_finalized(seal).unwrap());
                        }
                        if boundary == 3 {
                            (
                                1,
                                Step::Logical(Box::new(L::ConfirmFinalized { nonce, rows })),
                            )
                        } else {
                            logical_ok(&mut p, 1, L::ConfirmFinalized { nonce, rows });
                            let home = protected_pin(&mut p, 1).for_home(nonce).unwrap();
                            (
                                3,
                                Step::Unlock {
                                    plan: home.plan.clone(),
                                    home: Box::new(home),
                                },
                            )
                        }
                    }
                }
            };
            let leader = p.leader(group);
            let node = p.nodes.get_mut(&leader).unwrap();
            assert_eq!(
                node.request(json!({"command":"arm_crash","group":group,"after":after}))["ok"],
                true
            );
            node.send(json!({"command":"protected_step","group":group,"step":step}));
            assert!(node.output.recv_timeout(Duration::from_secs(12)).is_err());
            assert_eq!(node.child.wait().unwrap().code(), Some(86));
            p.restart();
            p.independent();
            protected_ok(&mut p, group, step);
            let pin = protected_pin(&mut p, 1);
            let Machine::Root { protected: h, .. } = pin.machine else {
                panic!()
            };
            if h.record(nonce).unwrap().decision.is_none() {
                let proofs = logical_process_prepare(&mut p, nonce);
                logical_ok(
                    &mut p,
                    1,
                    L::Commit {
                        nonce,
                        prepared: proofs,
                    },
                );
            }
            logical_process_finish(&mut p, nonce);
            for (g, k) in [(3, key(1)), (4, key(60))] {
                let data = logical_data(&mut p, g);
                assert_eq!(data.image.snapshot().unwrap().rows[&k].version, 1);
                assert_eq!(
                    data.image.snapshot().unwrap().rows[&k].status,
                    giggles_core::Status::Queued
                );
                assert!(!data.protected.busy());
            }
            let pin = protected_pin(&mut p, 1);
            let Machine::Root { protected: h, .. } = pin.machine else {
                panic!()
            };
            assert_eq!(
                h.image.snapshot().unwrap().tasks[&giggles_core::TaskId(1)].transitions,
                2
            );
            assert!(h.record(nonce).unwrap().released);
        }
    }
}
#[test]
fn real_logical_revoke_ordering_and_physical_commit_denial_after_leader_change() {
    use protected::{Step, logical::Step as L};
    for after in [false, true] {
        let nonce = 240 + u128::from(after);
        let (mut p, _) = logical_process_setup(nonce);
        let proofs = logical_process_prepare(&mut p, nonce);
        if after {
            logical_ok(
                &mut p,
                1,
                L::Commit {
                    nonce,
                    prepared: proofs.clone(),
                },
            );
        }
        protected_ok(
            &mut p,
            1,
            Step::Admin {
                command: serde_json::to_vec(&giggles_core::local::AdminCommand::Revoke {
                    task: giggles_core::TaskId(1),
                })
                .unwrap(),
            },
        );
        if after {
            p.restart();
            p.independent();
            logical_process_finish(&mut p, nonce);
        } else {
            assert_eq!(
                protected_call(
                    &mut p,
                    1,
                    Step::Logical(Box::new(L::Commit {
                        nonce,
                        prepared: proofs
                    }))
                )["ok"],
                false
            );
            p.colocate();
            assert_eq!(
                p.request(json!({"command":"protected_abort","nonce":nonce}))["ok"],
                true
            );
        }
        let leader = p.leader(1);
        let target = if leader == 1 { 2 } else { 1 };
        let moved = p
            .nodes
            .get_mut(&leader)
            .unwrap()
            .request(json!({"command":"transfer","group":1,"to":target}));
        assert_eq!(moved["ok"], true);
        let deadline = Instant::now() + Duration::from_secs(20);
        while p.leader(1) != target {
            assert!(
                Instant::now() < deadline,
                "logical authority transfer did not settle"
            );
        }
        let answer = protected_call(&mut p, 1, Step::Commit { nonce });
        assert_eq!(answer["ok"], false);
        assert!(answer["error"].as_str().unwrap().contains("clock/pause"));
        let pin = protected_pin(&mut p, 1);
        let Machine::Root { protected: h, .. } = pin.machine else {
            panic!()
        };
        assert_eq!(
            h.image.snapshot().unwrap().tasks[&giggles_core::TaskId(1)].transitions,
            if after { 2 } else { 0 }
        );
    }
}
#[test]
fn real_logical_decided_prepare_survives_membership_snapshot_and_finishes_original_home() {
    use protected::{
        Step,
        logical::{LogicalProfile, Step as L},
    };
    let mut p = Processes::with_members(4);
    for n in [1, 60] {
        p.apply(2, put(descriptor(), n, key(n), None));
    }
    p.apply(1, Action::Begin(split()));
    p.resume();
    let s = split();
    let mut plan = protected_support::plan(vec![s.left, s.right], 2, 250);
    plan.intent.lower = 12;
    logical_ok(
        &mut p,
        1,
        L::Install {
            image: protected_support::image(42, 2),
            profile: LogicalProfile { epoch: 1, tick: 12 },
        },
    );
    logical_ok(&mut p, 1, L::Open(plan.clone()));
    let home = protected_pin(&mut p, 1).for_home(250).unwrap();
    for g in [3, 4] {
        protected_ok(
            &mut p,
            g,
            Step::Lock {
                plan: plan.clone(),
                home: Box::new(home.clone()),
            },
        );
    }
    let rows = vec![protected_pin(&mut p, 3), protected_pin(&mut p, 4)];
    protected_ok(&mut p, 1, Step::Stage { nonce: 250, rows });
    let prepared = logical_process_prepare(&mut p, 250);
    logical_ok(
        &mut p,
        1,
        L::Commit {
            nonce: 250,
            prepared,
        },
    );
    assert_eq!(
        p.request(json!({"command":"checkpoint","group":3}))["ok"],
        true
    );
    let intent = giggles_membership::Intent {
        id: 4401,
        epoch: 1,
        learner: 4,
        remove: 3,
    };
    assert_eq!(
        p.request(json!({"command":"membership_begin","group":3,"intent":intent}))["ok"],
        true
    );
    for phase in ["CatchingUp", "Joint", "Complete"] {
        p.member_until(3, intent.id, phase, true);
    }
    p.restart();
    logical_process_finish(&mut p, 250);
    let data = logical_data(&mut p, 3);
    assert_eq!(data.protected.records[0].plan, plan);
    assert!(data.protected.records[0].finalized);
    assert!(data.protected.records[0].released);
    assert_eq!(data.image.snapshot().unwrap().rows[&key(1)].version, 1);
}
