mod support;
use giggles_placement::MAX_BYTES;
use giggles_placement::*;
use giggles_topology::*;
use support::*;
#[test]
fn valid_stable_capacity_is_not_summed_across_transient_learner() {
    let mut r = request();
    for m in &mut r.topology.groups[0].capacity.as_mut().unwrap().measurements {
        m.available = u64::MAX / 3;
    }
    r.learner_capacity.available = u64::MAX / 3;
    assert!(
        Validated::new(r.topology.clone())
            .unwrap()
            .analyze()
            .scenarios
            .iter()
            .all(|s| s.groups[0].capacity.assessment.verdict == Verdict::Pass)
    );
    let p = Plan::create(r, &mut Model::default()).unwrap();
    p.validate().unwrap();
    let mut overflowing_final = p.request.clone();
    overflowing_final.learner_capacity.available = u64::MAX;
    assert_code(
        Plan::create(overflowing_final, &mut Model::default()),
        Code::InvalidTopology,
    );
}
#[test]
fn canonical_digest_and_changed_plan() {
    let mut m = Model::default();
    let p = plan(&mut m);
    let mut r = request();
    r.topology.nodes.reverse();
    r.topology.groups[0].members.reverse();
    assert_eq!(Plan::create(r, &mut m).unwrap(), p);
    let mut p = p;
    p.request.intent.remove = 2;
    assert_code(p.validate(), Code::StalePlan);
}
#[test]
fn exact_roundtrip_large_domain() {
    let mut m = Model::default();
    let mut r = request();
    r.domain = u128::MAX;
    m.o.machine = giggles_distributed::Machine::range(u128::MAX, descriptor()).unwrap();
    let p = Plan::create(r, &mut m).unwrap();
    let bytes = serde_json::to_vec(&p).unwrap();
    let q: Plan = decode(&bytes).unwrap();
    assert_eq!(p, q);
    q.validate().unwrap();
}
#[test]
fn mapping_alias_and_missing_node() {
    for mode in 0..3 {
        let mut r = request();
        match mode {
            0 => {
                r.nodes.insert(4, "n3".into());
            }
            1 => {
                r.nodes.remove(&4);
            }
            _ => {
                r.nodes.insert(0, "n0".into());
            }
        }
        assert!(Plan::create(r, &mut Model::default()).is_err());
    }
}
#[test]
fn ineligible_residency_for_temporary_learner() {
    let mut r = request();
    r.topology.regions.push(Region {
        id: "forbidden".into(),
        provider: "p3".into(),
    });
    r.topology.zones.push(Zone {
        id: "forbidden".into(),
        region: "forbidden".into(),
    });
    r.topology.nodes[3].zone = "forbidden".into();
    assert_code(
        Plan::create(r, &mut Model::default()),
        Code::IneligibleResidency,
    );
}
#[test]
fn final_independent_profile_must_survive() {
    let mut r = request();
    r.topology.nodes[3].zone = "z1".into();
    assert!(matches!(
        Plan::create(r, &mut Model::default()).unwrap_err().code,
        Code::IneligibleProfile | Code::InvalidTopology
    ));
}
#[test]
fn dependency_unknown_is_distinct() {
    let mut r = request();
    r.topology.groups[0].requirements.clear();
    assert_code(
        Plan::create(r, &mut Model::default()),
        Code::DependencyUnknown,
    );
}
#[test]
fn dependency_failed_is_distinct() {
    let mut r = request();
    r.topology.groups[0].requirements[0].dependency = "d1".into();
    assert_code(
        Plan::create(r, &mut Model::default()),
        Code::DependencyFailed,
    );
}
#[test]
fn capacity_unknown_is_distinct() {
    let mut r = request();
    r.topology.groups[0].capacity = None;
    assert_code(
        Plan::create(r, &mut Model::default()),
        Code::CapacityUnknown,
    );
}
#[test]
fn capacity_failed_is_distinct() {
    let mut r = request();
    r.learner_capacity.available = 0;
    assert_code(Plan::create(r, &mut Model::default()), Code::CapacityFailed);
}
#[test]
fn diagnostic_or_wrong_binding() {
    for mode in 0..4 {
        let mut m = Model::default();
        match mode {
            0 => m.o.quorum_checked = false,
            1 => m.o.is_leader = false,
            2 => m.o.group = 3,
            _ => m.o.membership.identity.owner_epoch = 2,
        }
        assert!(Plan::create(request(), &mut m).is_err());
        assert!(m.actions.is_empty());
    }
}
#[test]
fn stale_epoch_and_nonvoter_remove() {
    for mode in 0..3 {
        let mut r = request();
        match mode {
            0 => r.intent.epoch = 2,
            1 => r.intent.remove = 4,
            _ => r.intent.learner = 1,
        }
        assert_code(Plan::create(r, &mut Model::default()), Code::StalePlan);
    }
}
#[test]
fn strict_json_and_byte_limits() {
    assert!(decode::<Request>(br#"{"version":1,"extra":true}"#).is_err());
    assert_code(decode::<Request>(&vec![b' '; MAX_BYTES + 1]), Code::Limit);
    let mut json = serde_json::to_value(request()).unwrap();
    json["intent"]["force"] = serde_json::json!(true);
    assert!(decode::<Request>(&serde_json::to_vec(&json).unwrap()).is_err());
}
#[test]
fn allocation_binding_and_bad_identity() {
    let mut r = request();
    r.learner_capacity.node = "n3".into();
    assert_code(
        Plan::create(r, &mut Model::default()),
        Code::CapacityUnknown,
    );
}
#[test]
fn arbitrary_relocation_and_unmapped_group_rejected() {
    let mut r = request();
    r.topology_group = "not-declared".into();
    assert_code(
        Plan::create(r, &mut Model::default()),
        Code::InvalidTopology,
    );
}
#[test]
fn declared_simultaneous_quorum_loss_rejected() {
    let mut r = request();
    r.topology.scenarios.push(Scenario {
        id: "two".into(),
        failed: vec![Domain::Provider("p1".into()), Domain::Provider("p2".into())],
    });
    assert_code(
        Plan::create(r, &mut Model::default()),
        Code::IneligibleProfile,
    );
}
