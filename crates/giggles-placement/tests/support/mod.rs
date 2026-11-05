#![allow(dead_code)]
use giggles_distributed::{Bounds, Descriptor, Machine, minimum, runtime::Observation};
use giggles_membership::{Identity, Intent, Membership, Phase, Progress, Readiness};
use giggles_placement::Error;
use giggles_placement::*;
use giggles_topology::*;
use std::path::PathBuf;
pub fn fresh(name: &str) -> PathBuf {
    static SERIAL: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let path = std::env::temp_dir()
        .join("giggles-placement-retained")
        .join(format!(
            "{name}-{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            SERIAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
    std::fs::create_dir_all(&path).unwrap();
    path
}
pub fn descriptor() -> Descriptor {
    Descriptor {
        range: 10,
        epoch: 1,
        group: 2,
        bounds: Bounds {
            start: minimum(),
            end: None,
        },
        voters: vec![1, 2, 3],
    }
}
pub fn request() -> Request {
    let providers: Vec<_> = (1..=3)
        .map(|n| Provider {
            id: format!("p{n}"),
            independence: Some(format!("declared-{n}")),
        })
        .collect();
    let regions = (1..=3)
        .map(|n| Region {
            id: format!("r{n}"),
            provider: format!("p{n}"),
        })
        .collect();
    let zones = (1..=3)
        .map(|n| Zone {
            id: format!("z{n}"),
            region: format!("r{n}"),
        })
        .collect();
    let nodes = (1..=4)
        .map(|n| Node {
            id: format!("n{n}"),
            zone: format!("z{}", n.min(3)),
        })
        .collect();
    let dependencies = (1..=3)
        .map(|n| Dependency {
            id: format!("d{n}"),
            expression: Expression::Endpoint {
                bindings: vec![Domain::Provider(format!("p{n}"))],
                assumptions: "Declared fixture only".into(),
            },
        })
        .chain([Dependency {
            id: "all-services".into(),
            expression: Expression::Any {
                dependencies: vec!["d1".into(), "d2".into(), "d3".into()],
            },
        }])
        .collect();
    Request {
        version: 1,
        topology: Topology {
            version: 1,
            providers,
            regions,
            zones,
            nodes,
            groups: vec![Group {
                id: "data".into(),
                members: (1..=3)
                    .map(|n| Member {
                        node: format!("n{n}"),
                        role: Role::FullDataVoter,
                    })
                    .collect(),
                profile: Profile::ThreeIndependentProviders,
                policy: Policy {
                    allowed_regions: vec!["r1".into(), "r2".into(), "r3".into()],
                    allowed_providers: vec!["p1".into(), "p2".into(), "p3".into()],
                },
                requirements: CATEGORIES
                    .into_iter()
                    .map(|category| Requirement {
                        category,
                        dependency: "all-services".into(),
                    })
                    .collect(),
                capacity: Some(Capacity {
                    unit: "declared-test-allocation".into(),
                    demand: 2,
                    measurements: (1..=3).map(measurement).collect(),
                    assumptions: "Synthetic allocation, not benchmark".into(),
                }),
            }],
            dependencies,
            scenarios: vec![],
        },
        topology_group: "data".into(),
        nodes: (1..=4).map(|n| (n, format!("n{n}"))).collect(),
        domain: 42,
        identity: Identity {
            group: 2,
            owner_epoch: 1,
        },
        intent: Intent {
            id: 4001,
            epoch: 1,
            learner: 4,
            remove: 3,
        },
        learner_capacity: measurement(4),
    }
}
pub fn measurement(n: u64) -> Measurement {
    Measurement {
        node: format!("n{n}"),
        available: 1,
        source: "Synthetic test declaration".into(),
    }
}
pub struct Model {
    pub o: Observation,
    pub actions: Vec<giggles_placement::Action>,
    pub lost: bool,
    pub unavailable: bool,
}
impl Default for Model {
    fn default() -> Self {
        Self {
            o: Observation {
                is_leader: true,
                quorum_checked: true,
                group: 2,
                leader: 1,
                applied: 10,
                machine: Machine::range(42, descriptor()).unwrap(),
                reply: None,
                membership: Membership::new(
                    Identity {
                        group: 2,
                        owner_epoch: 1,
                    },
                    vec![1, 2, 3],
                    8,
                )
                .unwrap(),
                term: 1,
                snapshot_index: 0,
            },
            actions: vec![],
            lost: false,
            unavailable: false,
        }
    }
}
impl Owner for Model {
    fn current(&mut self, _: u64) -> giggles_placement::Result<Observation> {
        if self.unavailable {
            Err(Error {
                code: Code::NoQuorum,
                message: "model partition".into(),
            })
        } else {
            Ok(self.o.clone())
        }
    }
    fn act(&mut self, a: &giggles_placement::Action) -> giggles_placement::Result<()> {
        self.actions.push(a.clone());
        self.o.applied += 1;
        let m = &mut self.o.membership;
        match a {
            giggles_placement::Action::Begin { intent, .. } => {
                m.active = Some(intent.id);
                m.receipts.insert(
                    intent.id,
                    Progress {
                        intent: intent.clone(),
                        phase: Phase::Planned,
                        added_at: 0,
                        readiness: None,
                    },
                );
            }
            giggles_placement::Action::Resume { id, .. } => {
                let p = m.receipts.get_mut(id).unwrap();
                match p.phase {
                    Phase::Planned => {
                        p.phase = Phase::CatchingUp;
                        p.added_at = self.o.applied;
                    }
                    Phase::CatchingUp
                        if p.readiness.as_ref().is_none_or(|r| r.term != self.o.term) =>
                    {
                        p.readiness = Some(Readiness {
                            intent: *id,
                            epoch: p.intent.epoch,
                            term: self.o.term,
                            barrier: self.o.applied,
                            durable_applied: self.o.applied,
                        })
                    }
                    Phase::CatchingUp => p.phase = Phase::Joint,
                    Phase::Joint => {
                        p.phase = Phase::Complete;
                        m.active = None;
                        m.epoch += 1;
                        m.voters.retain(|n| *n != p.intent.remove);
                        m.voters.push(p.intent.learner);
                    }
                    Phase::Complete => {}
                }
            }
        }
        if std::mem::take(&mut self.lost) {
            Err(Error {
                code: Code::Transport,
                message: "injected lost reply after apply".into(),
            })
        } else {
            Ok(())
        }
    }
}
pub fn plan(m: &mut Model) -> Plan {
    Plan::create(request(), m).unwrap()
}
pub fn assert_code<T: std::fmt::Debug>(r: giggles_placement::Result<T>, code: Code) {
    assert_eq!(r.unwrap_err().code, code);
}
pub fn finish(c: &mut Controller, m: &mut Model) {
    for _ in 0..10 {
        if c.step(m).unwrap() == StepResult::Complete {
            return;
        }
    }
    panic!("no completion");
}
