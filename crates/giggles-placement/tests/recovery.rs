mod support;
use giggles_membership::Phase;
use giggles_placement::*;
use support::*;
#[test]
fn lost_begin_reply_reopens_same_intent() {
    let root = fresh("lost-begin");
    let mut m = Model::default();
    let p = plan(&mut m);
    let path = root.join("journal.redb");
    let mut c = Controller::open(&path, p.clone()).unwrap();
    m.lost = true;
    assert_code(c.step(&mut m), Code::Transport);
    drop(c);
    let mut c = Controller::open(&path, p).unwrap();
    finish(&mut c, &mut m);
    assert_eq!(
        m.actions
            .iter()
            .filter(|a| matches!(a, Action::Begin { .. }))
            .count(),
        1
    );
    assert!(
        c.events()
            .unwrap()
            .iter()
            .any(|e| e.result.starts_with("indeterminate"))
    );
}
#[test]
fn restart_mid_joint_and_completed_reply_loss() {
    let root = fresh("joint");
    let mut m = Model::default();
    let p = plan(&mut m);
    let path = root.join("journal.redb");
    let mut c = Controller::open(&path, p.clone()).unwrap();
    while m
        .o
        .membership
        .receipts
        .get(&4001)
        .is_none_or(|r| r.phase != Phase::Joint)
    {
        c.step(&mut m).unwrap();
    }
    drop(c);
    let mut c = Controller::open(&path, p.clone()).unwrap();
    m.lost = true;
    assert_code(c.step(&mut m), Code::Transport);
    drop(c);
    let mut c = Controller::open(&path, p).unwrap();
    assert_eq!(c.step(&mut m).unwrap(), StepResult::Complete);
    assert_eq!(m.o.membership.voters, vec![1, 2, 4]);
}
#[test]
fn leader_term_change_requeries() {
    let root = fresh("term");
    let mut m = Model::default();
    let p = plan(&mut m);
    let mut c = Controller::open(root.join("j"), p).unwrap();
    for _ in 0..3 {
        c.step(&mut m).unwrap();
    }
    m.o.term = 2;
    m.o.leader = 2;
    finish(&mut c, &mut m);
    assert_eq!(m.o.membership.epoch, 2);
}
#[test]
fn noquorum_makes_zero_actions() {
    let mut m = Model::default();
    let p = plan(&mut m);
    let mut c = Controller::open(fresh("noquorum").join("j"), p).unwrap();
    m.unavailable = true;
    assert_code(c.step(&mut m), Code::NoQuorum);
    assert!(m.actions.is_empty());
    assert!(c.events().unwrap().is_empty());
}
#[test]
fn journal_rebind_and_exclusive_open() {
    let root = fresh("rebind");
    let mut m = Model::default();
    let p = plan(&mut m);
    let path = root.join("j");
    let c = Controller::open(&path, p.clone()).unwrap();
    assert!(Controller::open(&path, p.clone()).is_err());
    drop(c);
    let mut r = request();
    r.intent.id += 1;
    let q = Plan::create(r, &mut m).unwrap();
    assert!(matches!(
        Controller::open(&path, q),
        Err(Error {
            code: Code::StalePlan,
            ..
        })
    ));
}
#[test]
fn journal_corruption_rejected() {
    let root = fresh("corrupt");
    let path = root.join("j");
    std::fs::write(&path, b"bad journal").unwrap();
    let mut m = Model::default();
    assert!(Controller::open(&path, plan(&mut m)).is_err());
}
#[test]
fn other_active_intent_and_conflicting_receipt() {
    for conflict in [false, true] {
        let mut m = Model::default();
        let p = plan(&mut m);
        let mut c = Controller::open(fresh("conflict").join("j"), p).unwrap();
        c.step(&mut m).unwrap();
        let before = m.actions.len();
        if conflict {
            m.o.membership
                .receipts
                .get_mut(&4001)
                .unwrap()
                .intent
                .remove = 2;
        } else {
            let mut progress = m.o.membership.receipts.remove(&4001).unwrap();
            progress.intent.id = 4002;
            m.o.membership.receipts.insert(4002, progress);
            m.o.membership.active = Some(4002);
        }
        assert!(c.step(&mut m).is_err());
        assert_eq!(m.actions.len(), before);
    }
}
#[test]
fn frontier_nonregression_survives_reopen() {
    let root = fresh("frontier");
    let path = root.join("j");
    let mut m = Model::default();
    let p = plan(&mut m);
    let mut c = Controller::open(&path, p.clone()).unwrap();
    m.o.term = 5;
    m.o.applied = 30;
    c.step(&mut m).unwrap();
    drop(c);
    let mut c = Controller::open(&path, p).unwrap();
    m.o.term = 4;
    assert_code(c.step(&mut m), Code::StalePlan);
}
#[test]
fn retained_home_changes_failclosed() {
    let mut m = Model::default();
    let p = plan(&mut m);
    let mut c = Controller::open(fresh("home").join("j"), p).unwrap();
    if let giggles_distributed::Machine::Range { data, .. } = &mut m.o.machine {
        data.descriptor.range += 1;
    }
    assert_code(c.step(&mut m), Code::StalePlan);
    assert!(m.actions.is_empty());
}
#[test]
fn restart_and_lost_reply_at_every_stage() {
    for boundary in 0..5 {
        let root = fresh("state-machine");
        let path = root.join("j");
        let mut m = Model::default();
        let p = plan(&mut m);
        for stage in 0..7 {
            let mut c = Controller::open(&path, p.clone()).unwrap();
            m.lost = stage == boundary;
            let result = c.step(&mut m);
            if stage == boundary {
                assert_code(result, Code::Transport);
            } else if result.unwrap() == StepResult::Complete {
                break;
            }
        }
        assert_eq!(m.o.membership.receipts[&4001].phase, Phase::Complete);
        assert!(m.actions.iter().all(|a| match a {
            Action::Begin { intent, .. } => intent.id == 4001,
            Action::Resume { id, .. } => *id == 4001,
        }));
    }
}
#[test]
fn stale_completed_placement_rejected() {
    let mut m = Model::default();
    let p = plan(&mut m);
    let mut c = Controller::open(fresh("superseded").join("j"), p).unwrap();
    finish(&mut c, &mut m);
    m.o.membership.epoch += 1;
    assert_code(c.step(&mut m), Code::StalePlan);
}
#[test]
fn one_action_per_step_and_monotonic_journal() {
    let mut m = Model::default();
    let p = plan(&mut m);
    let mut c = Controller::open(fresh("single").join("j"), p).unwrap();
    for _ in 0..6 {
        let n = m.actions.len();
        let result = c.step(&mut m).unwrap();
        assert!(m.actions.len() <= n + 1);
        if result == StepResult::Complete {
            break;
        }
    }
    let events = c.events().unwrap();
    assert!(
        events
            .windows(2)
            .all(|w| w[0].sequence + 1 == w[1].sequence)
    );
    assert_eq!(
        events.first().unwrap().result,
        "attempt: outcome may be unknown"
    );
}
