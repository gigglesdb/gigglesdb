use giggles_topology::*;

fn model(layout: &[usize], profile: Profile) -> Topology {
    let mut t = Topology {
        version: 1,
        providers: vec![],
        regions: vec![],
        zones: vec![],
        nodes: vec![],
        groups: vec![],
        dependencies: vec![],
        scenarios: vec![],
    };
    let mut members = vec![];
    for (p, &count) in layout.iter().enumerate() {
        t.providers.push(Provider {
            id: format!("p{p}"),
            independence: Some(format!("independent{p}")),
        });
        t.regions.push(Region {
            id: format!("r{p}"),
            provider: format!("p{p}"),
        });
        for i in 0..count {
            let id = format!("{p}-{i}");
            t.zones.push(Zone {
                id: format!("z{id}"),
                region: format!("r{p}"),
            });
            t.nodes.push(Node {
                id: format!("n{id}"),
                zone: format!("z{id}"),
            });
            members.push(Member {
                node: format!("n{id}"),
                role: Role::FullDataVoter,
            });
        }
    }
    t.dependencies.push(Dependency {
        id: "endpoint".into(),
        expression: Expression::Endpoint {
            bindings: vec![Domain::Provider("p0".into())],
            assumptions: "Declared endpoint in p0, no runtime observation".into(),
        },
    });
    t.groups.push(Group {
        id: "data".into(),
        members,
        profile,
        policy: Policy {
            allowed_regions: t.regions.iter().map(|r| r.id.clone()).collect(),
            allowed_providers: t.providers.iter().map(|p| p.id.clone()).collect(),
        },
        requirements: CATEGORIES
            .iter()
            .map(|&category| Requirement {
                category,
                dependency: "endpoint".into(),
            })
            .collect(),
        capacity: None,
    });
    t
}
fn scenario(failed: Vec<Domain>) -> Scenario {
    Scenario {
        id: "custom".into(),
        failed,
    }
}
fn group(t: Topology, failed: Vec<Domain>) -> GroupReport {
    Validated::new(t)
        .unwrap()
        .analyze_scenario(&scenario(failed))
        .unwrap()
        .groups
        .remove(0)
}
fn reject(t: Topology, code: &str) {
    assert_eq!(Validated::new(t).unwrap_err().code, code);
}

// Independent oracle: a quorum is the smallest subset intersecting EVERY other
// subset of its cardinality. Enumerate actual subsets, not floor(N/2)+1 arithmetic.
fn oracle_quorum(n: usize) -> usize {
    for k in 1..=n {
        let sets: Vec<_> = (0usize..1 << n)
            .filter(|s| s.count_ones() as usize == k)
            .collect();
        if sets.iter().all(|a| sets.iter().all(|b| a & b != 0)) {
            return k;
        }
    }
    unreachable!()
}

#[test]
fn every_tiny_vote_loss_subset_matches_independent_combinatorial_oracle() {
    assert_eq!(majority(0), None);
    assert_eq!(majority(usize::MAX), Some(usize::MAX / 2 + 1));
    for n in 1..=7 {
        let t = model(&[n], Profile::Majority);
        let nodes = t.nodes.clone();
        let v = Validated::new(t).unwrap();
        let q = oracle_quorum(n);
        for loss in 0usize..1 << n {
            let s = scenario(
                nodes
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| loss & (1 << i) != 0)
                    .map(|(_, n)| Domain::Node(n.id.clone()))
                    .collect(),
            );
            let g = v.analyze_scenario(&s).unwrap().groups.remove(0);
            let remaining = ((1usize << n) - 1) ^ loss;
            let can_form_quorum = (0usize..1 << n)
                .any(|subset| subset & !remaining == 0 && subset.count_ones() as usize == q);
            assert_eq!(g.quorum, q);
            assert_eq!(
                g.voter_arithmetic.verdict == Verdict::Pass,
                can_form_quorum,
                "n={n} loss={loss}"
            );
        }
    }
}

#[test]
fn declared_profiles_survive_each_provider_loss_with_exact_margins() {
    for (layout, profile, survivors) in [
        (
            &[1, 1, 1][..],
            Profile::ThreeIndependentProviders,
            vec![2, 2, 2],
        ),
        (&[2, 2, 1][..], Profile::FiveVoters221, vec![3, 3, 4]),
    ] {
        let t = model(layout, profile);
        let v = Validated::new(t).unwrap();
        for (p, n) in survivors.iter().enumerate() {
            let g = v
                .analyze_scenario(&scenario(vec![Domain::Provider(format!("p{p}"))]))
                .unwrap()
                .groups
                .remove(0);
            assert_eq!(g.surviving_voters.len(), *n);
            assert_eq!(g.voter_arithmetic.verdict, Verdict::Pass);
        }
    }
    let t = model(&[2, 2, 1], Profile::FiveVoters221);
    assert_eq!(
        group(
            t,
            vec![Domain::Provider("p0".into()), Domain::Node("n1-0".into())]
        )
        .voter_arithmetic
        .verdict,
        Verdict::Fail
    );
    let t = model(&[3, 2], Profile::Majority);
    assert_eq!(
        group(t.clone(), vec![Domain::Provider("p0".into())])
            .voter_arithmetic
            .verdict,
        Verdict::Fail
    );
    assert_eq!(
        group(t, vec![Domain::Provider("p1".into())])
            .voter_arithmetic
            .verdict,
        Verdict::Pass
    );
}

#[test]
fn learners_never_vote_and_new_role_snapshots_change_quorum() {
    let mut t = model(&[5], Profile::Majority);
    t.groups[0].members[3].role = Role::Learner;
    t.groups[0].members[4].role = Role::Learner;
    let losses = vec![Domain::Node("n0-0".into()), Domain::Node("n0-1".into())];
    let g = group(t.clone(), losses.clone());
    assert_eq!(g.voter_count, 3);
    assert_eq!(g.quorum, 2);
    assert_eq!(g.surviving_voters, vec!["n0-2"]);
    assert_eq!(g.voter_arithmetic.verdict, Verdict::Fail);
    t.groups[0].members[3].role = Role::FullDataVoter;
    t.groups[0].members[4].role = Role::FullDataVoter;
    assert_eq!(
        group(t.clone(), losses).voter_arithmetic.verdict,
        Verdict::Pass
    );
    for m in &mut t.groups[0].members {
        m.role = Role::Learner;
    }
    reject(t, "membership");
}

#[test]
fn shared_dependency_collapse_despite_healthy_quorum() {
    let g = group(
        model(&[1, 1, 1], Profile::ThreeIndependentProviders),
        vec![Domain::Provider("p0".into())],
    );
    assert_eq!(g.voter_arithmetic.verdict, Verdict::Pass);
    assert_eq!(g.dependency_reachability.verdict, Verdict::Fail);
    for a in g.requirements.values() {
        assert_eq!(a.verdict, Verdict::Fail);
    }
    assert_eq!(g.capacity.assessment.verdict, Verdict::NotEvaluated);
}

fn alternatives() -> Topology {
    let mut t = model(&[1, 1, 1], Profile::ThreeIndependentProviders);
    t.dependencies.push(Dependency {
        id: "other".into(),
        expression: Expression::Endpoint {
            bindings: vec![Domain::Node("n1-0".into())],
            assumptions: "alternate path".into(),
        },
    });
    t.dependencies.push(Dependency {
        id: "route".into(),
        expression: Expression::Any {
            dependencies: vec!["endpoint".into(), "other".into()],
        },
    });
    for r in &mut t.groups[0].requirements {
        r.dependency = "route".into();
    }
    t
}

#[test]
fn explicit_or_and_tri_state_logic_and_hierarchy() {
    let mut t = alternatives();
    assert_eq!(
        group(t.clone(), vec![Domain::Provider("p0".into())])
            .dependency_reachability
            .verdict,
        Verdict::Pass
    );
    assert_eq!(
        group(
            t.clone(),
            vec![Domain::Provider("p0".into()), Domain::Region("r1".into())]
        )
        .dependency_reachability
        .verdict,
        Verdict::Fail
    );
    t.dependencies[1].expression = Expression::Unknown {
        reason: "not inventoried".into(),
    };
    assert_eq!(
        group(t.clone(), vec![Domain::Zone("z0-0".into())])
            .dependency_reachability
            .verdict,
        Verdict::Pass
    ); // provider endpoint not zone-bound
    assert_eq!(
        group(t.clone(), vec![Domain::Provider("p0".into())])
            .dependency_reachability
            .verdict,
        Verdict::Unknown
    );
    assert_eq!(
        group(t.clone(), vec![]).dependency_reachability.verdict,
        Verdict::Pass
    );
    t.dependencies[2].expression = Expression::All {
        dependencies: vec!["endpoint".into(), "other".into()],
    };
    assert_eq!(
        group(t.clone(), vec![]).dependency_reachability.verdict,
        Verdict::Unknown
    );
    assert_eq!(
        group(t, vec![Domain::Provider("p0".into())])
            .dependency_reachability
            .verdict,
        Verdict::Fail
    );
    for domain in [
        Domain::Zone("z1-0".into()),
        Domain::Region("r1".into()),
        Domain::Provider("p1".into()),
        Domain::Node("n1-0".into()),
    ] {
        let g = group(alternatives(), vec![Domain::Provider("p0".into()), domain]);
        assert_eq!(g.dependency_reachability.verdict, Verdict::Fail);
    }
}

#[test]
fn missing_requirements_are_unknown_not_vacuously_survivable() {
    let mut t = model(&[3], Profile::Majority);
    t.groups[0].requirements.clear();
    let g = group(t, vec![]);
    assert_eq!(g.dependency_reachability.verdict, Verdict::Unknown);
    assert!(
        g.requirements
            .values()
            .all(|a| a.verdict == Verdict::Unknown)
    );
}

#[test]
fn profiles_require_provider_independence_not_region_names() {
    let mut t = model(&[1, 1, 1], Profile::ThreeIndependentProviders);
    t.providers[1].independence = t.providers[0].independence.clone();
    reject(t, "independence");
    let mut t = model(&[1, 1, 1], Profile::ThreeIndependentProviders);
    t.providers[0].independence = None;
    reject(t, "independence");
    let mut t = model(&[1, 1, 1], Profile::ThreeIndependentProviders);
    for r in &mut t.regions {
        r.provider = "p0".into();
    }
    reject(t, "profile");
    let mut t = model(&[2, 2, 1], Profile::FiveVoters221);
    t.nodes[1].zone = t.nodes[0].zone.clone();
    reject(t, "profile");
}

#[test]
fn residency_includes_learners_and_allowlists_must_be_explicit() {
    let mut t = model(&[1, 1, 1], Profile::Majority);
    t.groups[0].members[2].role = Role::Learner;
    t.groups[0].policy.allowed_regions.pop();
    reject(t, "residency");
    let mut t = model(&[1], Profile::Majority);
    t.groups[0].policy.allowed_providers.clear();
    reject(t, "policy");
}

#[test]
fn duplicate_ids_membership_categories_and_domains_rejected() {
    let base = model(&[1, 1, 1], Profile::Majority);
    for kind in 0..10 {
        let mut t = base.clone();
        match kind {
            0 => t.providers.push(t.providers[0].clone()),
            1 => t.regions.push(t.regions[0].clone()),
            2 => t.zones.push(t.zones[0].clone()),
            3 => t.nodes.push(t.nodes[0].clone()),
            4 => t.groups.push(t.groups[0].clone()),
            5 => t.dependencies.push(t.dependencies[0].clone()),
            6 => {
                let m = t.groups[0].members[0].clone();
                t.groups[0].members.push(m);
            }
            7 => t.groups[0].requirements[1].category = t.groups[0].requirements[0].category,
            8 => {
                let r = t.groups[0].policy.allowed_regions[0].clone();
                t.groups[0].policy.allowed_regions.push(r);
            }
            _ => t.scenarios.push(scenario(vec![
                Domain::Provider("p0".into()),
                Domain::Provider("p0".into()),
            ])),
        }
        reject(t, "duplicate");
    }
}

#[test]
fn missing_domain_definitions_members_and_dependency_references_rejected() {
    for kind in 0..8 {
        let mut t = model(&[1], Profile::Majority);
        match kind {
            0 => t.regions[0].provider = "absent".into(),
            1 => t.zones[0].region = "absent".into(),
            2 => t.nodes[0].zone = "absent".into(),
            3 => t.groups[0].members[0].node = "absent".into(),
            4 => t.groups[0].requirements[0].dependency = "absent".into(),
            5 => {
                t.dependencies[0].expression = Expression::All {
                    dependencies: vec!["absent".into()],
                }
            }
            6 => t
                .scenarios
                .push(scenario(vec![Domain::Zone("absent".into())])),
            _ => {
                t.dependencies[0].expression = Expression::Endpoint {
                    bindings: vec![Domain::Region("absent".into())],
                    assumptions: "test".into(),
                }
            }
        }
        reject(t, "reference");
    }
    let mut t = model(&[1], Profile::Majority);
    t.dependencies[0].expression = Expression::Endpoint {
        bindings: vec![],
        assumptions: "unspecified".into(),
    };
    reject(t, "binding");
}

#[test]
fn cycles_including_unused_graph_and_empty_operators_rejected() {
    for any in [false, true] {
        let mut t = model(&[1], Profile::Majority);
        t.dependencies.push(Dependency {
            id: "unused".into(),
            expression: if any {
                Expression::Any {
                    dependencies: vec!["unused".into()],
                }
            } else {
                Expression::All {
                    dependencies: vec!["unused".into()],
                }
            },
        });
        reject(t, "cycle");
    }
    for deps in [vec![], vec!["endpoint".into()]] {
        let mut t = model(&[1], Profile::Majority);
        t.dependencies[0].expression = Expression::All {
            dependencies: deps.clone(),
        };
        reject(
            t,
            if deps.is_empty() {
                "expression"
            } else {
                "cycle"
            },
        );
    }
}

fn capacity_model() -> Topology {
    let mut t = model(&[1, 1, 1], Profile::Majority);
    t.groups[0].capacity = Some(Capacity { unit: "declared-work-units/s".into(), demand: 18, measurements: t.nodes.iter().map(|n| Measurement { node: n.id.clone(), available: 10, source: "test fixture only: caller-supplied synthetic measurement".into() }).collect(), assumptions: "Independent additive allocations for this group; includes declared recovery reserve. Synthetic behavior test, not benchmark.".into() });
    t
}

#[test]
fn supplied_capacity_only_survivor_sums_missing_data_and_overflow() {
    let t = capacity_model();
    let g = group(t.clone(), vec![Domain::Provider("p0".into())]);
    assert_eq!(g.capacity.surviving_available, Some(20));
    assert_eq!(g.capacity.assessment.verdict, Verdict::Pass);
    assert_eq!(
        group(
            t.clone(),
            vec![Domain::Provider("p0".into()), Domain::Provider("p1".into())]
        )
        .capacity
        .assessment
        .verdict,
        Verdict::Fail
    );
    let mut missing = t.clone();
    missing.groups[0]
        .capacity
        .as_mut()
        .unwrap()
        .measurements
        .pop();
    assert_eq!(
        group(missing.clone(), vec![]).capacity.assessment.verdict,
        Verdict::NotEvaluated
    );
    assert_eq!(
        group(missing, vec![Domain::Provider("p2".into())])
            .capacity
            .assessment
            .verdict,
        Verdict::Pass
    );
    let mut overflow = t.clone();
    overflow.groups[0].capacity.as_mut().unwrap().measurements[0].available = u64::MAX;
    reject(overflow, "overflow");
    let mut duplicate = t.clone();
    let first = duplicate.groups[0].capacity.as_ref().unwrap().measurements[0].clone();
    duplicate.groups[0]
        .capacity
        .as_mut()
        .unwrap()
        .measurements
        .push(first);
    reject(duplicate, "duplicate");
    let mut malformed = t;
    malformed.groups[0].capacity.as_mut().unwrap().measurements[0].source = " ".into();
    reject(malformed, "text");
}

#[test]
fn deterministic_report_and_export_under_all_order_permutations() {
    let mut t = alternatives();
    t.groups[0].capacity = capacity_model().groups.remove(0).capacity;
    t.scenarios.push(scenario(vec![
        Domain::Node("n1-0".into()),
        Domain::Provider("p0".into()),
    ]));
    let a = Validated::new(t.clone()).unwrap();
    t.providers.reverse();
    t.regions.reverse();
    t.zones.reverse();
    t.nodes.reverse();
    t.groups.reverse();
    t.dependencies.reverse();
    t.scenarios.reverse();
    for g in &mut t.groups {
        g.members.reverse();
        g.requirements.reverse();
        g.policy.allowed_regions.reverse();
        g.policy.allowed_providers.reverse();
        if let Some(c) = &mut g.capacity {
            c.measurements.reverse();
        }
    }
    for d in &mut t.dependencies {
        if let Expression::Any { dependencies } = &mut d.expression {
            dependencies.reverse();
        }
    }
    for s in &mut t.scenarios {
        s.failed.reverse();
    }
    let b = Validated::new(t).unwrap();
    assert_eq!(a.export_json(), b.export_json());
    assert_eq!(a.analyze(), b.analyze());
    assert_eq!(
        Validated::from_json(a.export_json().as_bytes())
            .unwrap()
            .analyze(),
        a.analyze()
    );
}

#[test]
fn bounded_input_collections_ids_edges_depth_and_shared_dag() {
    assert_eq!(
        Validated::from_json(&vec![b' '; MAX_BYTES + 1])
            .unwrap_err()
            .code,
        "limit"
    );
    let mut t = model(&[1], Profile::Majority);
    t.nodes[0].id = "x".repeat(129);
    reject(t, "id");
    let mut t = model(&[1], Profile::Majority);
    t.providers = (0..=MAX_ITEMS)
        .map(|i| Provider {
            id: format!("p{i}"),
            independence: None,
        })
        .collect();
    reject(t, "limit");
    let mut t = model(&[1], Profile::Majority);
    for i in 1..=MAX_DEPTH {
        t.dependencies.push(Dependency {
            id: format!("d{i:02}"),
            expression: Expression::All {
                dependencies: vec![if i == 1 {
                    "endpoint".into()
                } else {
                    format!("d{:02}", i - 1)
                }],
            },
        });
    }
    reject(t, "limit"); // longest path bound also catches memoized child heights
    let mut t = model(&[1], Profile::Majority);
    for i in 0..32 {
        t.dependencies.push(Dependency {
            id: format!("leaf{i}"),
            expression: Expression::Unknown {
                reason: "unknown".into(),
            },
        });
    }
    for i in 0..65 {
        t.dependencies.push(Dependency {
            id: format!("fan{i}"),
            expression: Expression::Any {
                dependencies: (0..32).map(|i| format!("leaf{i}")).collect(),
            },
        });
    }
    reject(t, "limit");
    // Repeated shared subgraphs must not expand explanations exponentially.
    let mut t = model(&[1], Profile::Majority);
    for level in 0..20 {
        for side in 0..2 {
            t.dependencies.push(Dependency {
                id: format!("d{level}-{side}"),
                expression: Expression::Any {
                    dependencies: if level == 0 {
                        vec!["endpoint".into()]
                    } else {
                        vec![format!("d{}-0", level - 1), format!("d{}-1", level - 1)]
                    },
                },
            });
        }
    }
    assert!(
        serde_json::to_vec(&Validated::new(t).unwrap().analyze())
            .unwrap()
            .len()
            < 100_000
    );
}

#[test]
fn malformed_json_unknown_fields_roles_numbers_and_reserved_scenarios() {
    for json in ["{}", "[]", "null", "{", "{\"version\":1,\"version\":1}"] {
        assert!(Validated::from_json(json.as_bytes()).is_err());
    }
    let t = model(&[1], Profile::Majority);
    let v = serde_json::to_value(&t).unwrap();
    for (key, value) in [
        ("role", serde_json::json!("witness")),
        ("role", serde_json::json!("voter")),
    ] {
        let mut v = v.clone();
        v["groups"][0]["members"][0][key] = value;
        assert_eq!(
            Validated::from_json(&serde_json::to_vec(&v).unwrap())
                .unwrap_err()
                .code,
            "json"
        );
    }
    let mut v = v.clone();
    v["unexpected"] = true.into();
    assert_eq!(
        Validated::from_json(&serde_json::to_vec(&v).unwrap())
            .unwrap_err()
            .code,
        "json"
    );
    let mut t = t;
    t.version = 2;
    reject(t, "version");
    let mut t = model(&[1], Profile::Majority);
    t.scenarios.push(Scenario {
        id: "baseline".into(),
        failed: vec![],
    });
    reject(t, "reserved");
    for n in ["-1", "18446744073709551616", "1.5"] {
        let t = capacity_model();
        let json = serde_json::to_string(&t)
            .unwrap()
            .replace("\"demand\":18", &format!("\"demand\":{n}"));
        assert_eq!(
            Validated::from_json(json.as_bytes()).unwrap_err().code,
            "json"
        );
    }
}

#[test]
fn cross_product_report_cost_and_text_limits_are_enforced() {
    let mut t = model(&[1], Profile::Majority);
    let base = t.groups[0].clone();
    t.groups = (0..MAX_ITEMS)
        .map(|i| {
            let mut g = base.clone();
            g.id = format!("g{i}");
            g
        })
        .collect();
    let e = Validated::new(t).unwrap_err();
    assert_eq!(e.code, "limit");
    assert_eq!(e.path, "analysis_cost");
    let mut t = model(&[1], Profile::Majority);
    t.dependencies[0].expression = Expression::Unknown {
        reason: "x".repeat(4097),
    };
    reject(t, "text");
}

#[test]
fn generated_scenarios_and_multiple_groups_are_independent() {
    let mut t = model(&[1, 1, 1], Profile::Majority);
    let mut g = t.groups[0].clone();
    g.id = "authority".into();
    g.members.truncate(1);
    t.groups.push(g);
    t.scenarios.push(scenario(vec![
        Domain::Region("r0".into()),
        Domain::Zone("z1-0".into()),
    ]));
    let v = Validated::new(t).unwrap();
    assert_eq!(v.scenarios().len(), 11);
    let r = v
        .analyze_scenario(&scenario(vec![Domain::Provider("p0".into())]))
        .unwrap();
    assert_eq!(r.groups[0].id, "authority");
    assert_eq!(r.groups[0].voter_arithmetic.verdict, Verdict::Fail);
    assert_eq!(r.groups[1].voter_arithmetic.verdict, Verdict::Pass);
    assert!(
        v.analyze_scenario(&scenario(vec![Domain::Node("absent".into())]))
            .is_err()
    );
}
