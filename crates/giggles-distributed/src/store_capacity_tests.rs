use super::*;
use crate::{
    Action, Bounds, Command, Descriptor, Machine,
    protected::{
        self as p,
        logical::{self as l, LogicalProfile},
    },
};
use giggles_core::{
    local::AdminCommand,
    replica::{self, Image as CoreImage},
    *,
};
use std::collections::BTreeSet;
fn route() -> Descriptor {
    Descriptor {
        range: 10,
        epoch: 1,
        group: 2,
        bounds: Bounds {
            start: crate::minimum(),
            end: None,
        },
        voters: VOTERS.to_vec(),
    }
}
fn run(machine: &mut Machine, index: u64, step: p::Step) {
    let r = machine.apply(
        &Command {
            domain: 42,
            group: machine.group(),
            action: Action::Protected(Box::new(step)),
        },
        index,
    );
    assert_eq!(r.code, 0, "{:?}", r.data);
}
fn committed() -> Machine {
    let key = Key {
        tenant: TenantId(42),
        row: RowId(1),
    };
    let mut authority = CoreImage::new(42).unwrap();
    for command in [
        AdminCommand::Grant {
            grant: Grant {
                task: TaskId(1),
                issuer: Principal(1),
                executor: Principal(2),
                approver: Principal(3),
                policy: Policy {
                    tenant: TenantId(42),
                    columns: BTreeSet::from([Column::Status]),
                    transitions: BTreeSet::from([(Status::Failed, Status::Queued)]),
                    incident: (100, 110),
                    distinct_limit: 1,
                    transition_limit: 1,
                    approval_above: 1,
                    valid_from: 1,
                    expires: 1000,
                },
            },
        },
        AdminCommand::ProvisionTaskResources {
            task: TaskId(1),
            limits: TaskResources {
                read_rows: 100,
                disclosed_rows: 100,
                encoded_bytes: 100000,
            },
        },
    ] {
        let (next, r) = authority
            .evaluate(&replica::Command::Admin(
                serde_json::to_vec(&command).unwrap(),
            ))
            .unwrap();
        assert_eq!(r.code, 0);
        authority = next;
    }
    let mut root = Machine::root(42, route()).unwrap();
    let mut part = Machine::range(42, route()).unwrap();
    let r = part.apply(
        &Command {
            domain: 42,
            group: 2,
            action: Action::Put {
                route: route(),
                operation: 1,
                key,
                expected: None,
                row: Row {
                    status: Status::Failed,
                    failed_at: 103,
                    attempt_count: 0,
                    version: 0,
                },
            },
        },
        1,
    );
    assert_eq!(r.code, 0);
    run(
        &mut root,
        1,
        p::Step::Logical(Box::new(l::Step::Install {
            image: authority,
            profile: LogicalProfile { epoch: 1, tick: 12 },
        })),
    );
    let plan = p::Plan {
        database: 42,
        nonce: 1,
        revision: 1,
        task: TaskId(1),
        routes: vec![route()],
        intent: replica::protected::Intent {
            principal: Principal(2),
            request: Request {
                operation: OperationId(1),
                selection: Selection::Keys(vec![key]),
                column: Column::Status,
                after: Status::Queued,
            },
            approval: None,
            lower: 12,
            upper: 12,
            fence: 1,
        },
    };
    run(
        &mut root,
        2,
        p::Step::Logical(Box::new(l::Step::Open(plan.clone()))),
    );
    let home = p::Pin {
        database: 42,
        group: 1,
        applied: 2,
        machine: root.clone(),
    }
    .for_home(1)
    .unwrap();
    run(
        &mut part,
        2,
        p::Step::Lock {
            plan,
            home: Box::new(home),
        },
    );
    run(
        &mut root,
        3,
        p::Step::Stage {
            nonce: 1,
            rows: vec![p::Pin {
                database: 42,
                group: 2,
                applied: 2,
                machine: part.clone(),
            }],
        },
    );
    let prepare = p::Pin {
        database: 42,
        group: 1,
        applied: 3,
        machine: root.clone(),
    }
    .logical_preparation(1, 2)
    .unwrap();
    run(
        &mut part,
        3,
        p::Step::Logical(Box::new(l::Step::Prepare(prepare))),
    );
    let prepared = p::Pin {
        database: 42,
        group: 2,
        applied: 3,
        machine: part,
    }
    .logical_prepared(1)
    .unwrap();
    run(
        &mut root,
        4,
        p::Step::Logical(Box::new(l::Step::Commit {
            nonce: 1,
            prepared: vec![prepared],
        })),
    );
    root
}
#[test]
fn protected_log_headroom_compacts_applied_prefix_retains_actual_decision_and_uncommitted_tail() {
    let image = committed();
    let store = Store::simulated(1, 42, 1, image.clone()).unwrap();
    let mut payload = vec![b' '; 512 * 1024];
    payload.extend(
        serde_json::to_vec(&Command {
            domain: 42,
            group: 1,
            action: Action::Protected(Box::new(p::Step::Logical(Box::new(
                l::Step::VerifyDecision { nonce: 1 },
            )))),
        })
        .unwrap(),
    );
    store
        .update(|s| {
            for index in 1..=38 {
                let mut e = Entry::default();
                e.index = index;
                e.term = 1;
                e.set_data(payload.clone().into());
                s.log.insert(index, e.write_to_bytes()?);
            }
            let mut h = HardState::default();
            h.term = 1;
            h.commit = 35;
            s.hard = h.write_to_bytes()?;
            s.applied = 35;
            s.applied_term = 1;
            Ok(())
        })
        .unwrap();
    let state = store.state().unwrap();
    assert_eq!(state.image, image);
    assert_eq!(
        state.log.keys().copied().collect::<Vec<_>>(),
        vec![36, 37, 38]
    );
    assert_eq!(state.snap().unwrap().get_metadata().index, 35);
    store.verify_snapshot(&state.snap().unwrap()).unwrap();
    let reopened = Store::recover_simulated(&store.durable_bytes().unwrap(), 1, 42, 1).unwrap();
    assert_eq!(reopened.image().unwrap(), image);
    let mut recovered = reopened.image().unwrap();
    run(
        &mut recovered,
        36,
        p::Step::Logical(Box::new(l::Step::VerifyDecision { nonce: 1 })),
    );
    let Machine::Root { protected, .. } = recovered else {
        panic!()
    };
    assert_eq!(
        protected.image.snapshot().unwrap().tasks[&TaskId(1)].transitions,
        1
    );
    assert!(protected.record(1).unwrap().decision.is_some());
}
#[test]
fn protected_log_headroom_never_compacts_an_unapplied_prefix() {
    let store = Store::simulated(1, 42, 1, committed()).unwrap();
    store
        .update(|s| {
            let mut hard = HardState::default();
            hard.term = 1;
            s.hard = hard.write_to_bytes()?;
            for index in 1..=34 {
                let mut e = Entry::default();
                e.index = index;
                e.term = 1;
                e.set_data(vec![0; 512 * 1024].into());
                s.log.insert(index, e.write_to_bytes()?);
            }
            Ok(())
        })
        .unwrap();
    let state = store.state().unwrap();
    assert_eq!(state.applied, 0);
    assert_eq!(state.log.len(), 34);
    assert!(state.snap().unwrap().is_empty());
}
