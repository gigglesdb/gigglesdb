#![allow(dead_code)]
mod common;
use common::*;
use giggles_core as c;
use giggles_model as m;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
fn status(v: m::Status) -> c::Status {
    match v {
        m::Status::Failed => c::Status::Failed,
        m::Status::Queued => c::Status::Queued,
        m::Status::Succeeded => c::Status::Succeeded,
    }
}
fn column(v: m::Column) -> c::Column {
    match v {
        m::Column::Status => c::Column::Status,
        m::Column::FailedAt => c::Column::FailedAt,
        m::Column::AttemptCount => c::Column::AttemptCount,
    }
}
fn ck(k: m::Key) -> c::Key {
    c::Key {
        tenant: c::TenantId(k.tenant.0),
        row: c::RowId(k.row.0),
    }
}
fn cr(r: &m::Row) -> c::Row {
    c::Row {
        status: status(r.status),
        failed_at: r.failed_at,
        attempt_count: r.attempt_count,
        version: r.version,
    }
}
fn cp(p: &m::Policy) -> c::Policy {
    c::Policy {
        tenant: c::TenantId(p.tenant.0),
        columns: p.columns.iter().map(|v| column(*v)).collect(),
        transitions: p
            .transitions
            .iter()
            .map(|(a, b)| (status(*a), status(*b)))
            .collect(),
        incident: p.incident,
        distinct_limit: p.distinct_limit,
        transition_limit: p.transition_limit,
        approval_above: p.approval_above,
        valid_from: p.valid_from,
        expires: p.expires,
    }
}
fn cg(g: &m::Grant) -> c::Grant {
    c::Grant {
        task: c::TaskId(g.task.0),
        issuer: c::Principal(g.issuer.0),
        executor: c::Principal(g.executor.0),
        approver: c::Principal(g.approver.0),
        policy: cp(&g.policy),
    }
}
fn cq(r: &m::Request) -> c::Request {
    c::Request {
        operation: c::OperationId(r.operation.0),
        column: column(r.column),
        after: status(r.after),
        selection: match &r.selection {
            m::Selection::Keys(ks) => c::Selection::Keys(ks.iter().map(|k| ck(*k)).collect()),
            m::Selection::Predicate {
                tenant,
                status: s,
                from,
                until,
            } => c::Selection::Predicate {
                tenant: c::TenantId(tenant.0),
                status: status(*s),
                from: *from,
                until: *until,
            },
        },
    }
}
fn ct(t: m::Time) -> c::Time {
    match t {
        m::Time::Certified(n) => c::Time::Certified(n),
        m::Time::Uncertain => c::Time::Uncertain,
    }
}
fn preview(p: &m::Preview, domain: u128) -> c::Preview {
    c::Preview {
        domain,
        task: c::TaskId(p.task.0),
        request: cq(&p.request),
        schema: p.schema,
        policy: cp(&p.policy),
        dependencies: p
            .dependencies
            .iter()
            .map(|(k, r)| (ck(*k), cr(r)))
            .collect(),
        effects: p
            .effects
            .iter()
            .map(|e| c::Effect {
                key: ck(e.key),
                before: cr(&e.before),
                after: cr(&e.after),
            })
            .collect(),
        expires: p.expires,
    }
}
fn normalized(mut s: c::Snapshot, domain: u128) -> c::Snapshot {
    s.domain = domain;
    for a in s.approvals.values_mut() {
        a.preview.domain = domain;
    }
    for b in s.operations.values_mut() {
        if let Ok(r) = &mut b.decision {
            r.domain = domain;
        }
    }
    s
}
fn decision(mut d: c::Decision, domain: u128) -> String {
    if let Ok(r) = &mut d {
        r.domain = domain;
    }
    format!("{d:?}")
}
#[test]
fn all_five_golden_histories_and_independent_checker() {
    for h in m::fixtures::golden_histories() {
        m::checker::verify(h.domain, &h.events, &h.final_state).unwrap();
        let p = fresh(h.name);
        let a = c::Admin::create(&p, c::Limits::default()).unwrap();
        let domain = a.snapshot().unwrap().domain;
        let mut handles = BTreeMap::new();
        for event in &h.events {
            match event {
                m::Event::Put(k, r) => a.put(ck(*k), cr(r)).unwrap(),
                m::Event::Grant(g) => {
                    handles.insert(g.task, a.grant(cg(g)).unwrap());
                }
                m::Event::Revoke(t) => a.revoke(c::TaskId(t.0)).unwrap(),
                m::Event::Schema(_) => a.bump_schema().unwrap(),
                m::Event::Approve(v) => assert_eq!(
                    a.approve(
                        c::Principal(v.approver.0),
                        preview(&v.preview, domain),
                        c::Time::Certified(10)
                    )
                    .unwrap()
                    .0,
                    v.id.0
                ),
                m::Event::RevokeApproval(id) => a.revoke_approval(c::ApprovalId(id.0)).unwrap(),
                m::Event::Decision {
                    task,
                    request,
                    time,
                    approval,
                    result,
                } => {
                    let d = handles[task]
                        .execute(
                            cq(request),
                            ct(*time),
                            approval.map(|id| c::ApprovalId(id.0)),
                        )
                        .unwrap();
                    assert_eq!(decision(d, h.domain), format!("{result:?}"), "{}", h.name);
                }
            }
        }
        assert_eq!(
            format!("{:?}", normalized(a.snapshot().unwrap(), h.domain)),
            format!("{:?}", h.final_state),
            "{}",
            h.name
        );
        drop(handles);
        drop(a);
        let a = c::Admin::open(p, c::Limits::default()).unwrap();
        assert_eq!(
            format!("{:?}", normalized(a.snapshot().unwrap(), h.domain)),
            format!("{:?}", h.final_state)
        );
    }
}
#[test]
fn generated_sequences_against_independent_p0_oracle() {
    // Exact reproducible seed range; no unrecorded random source determines actions.
    for seed in 0u64..100 {
        let p = fresh(&format!("seed-{seed}"));
        let mut a = c::Admin::create(&p, c::Limits::default()).unwrap();
        let domain = a.snapshot().unwrap().domain;
        let mut model = m::Model::new(domain);
        for id in 0..8 {
            let k = m::Key {
                tenant: m::TenantId(42),
                row: m::RowId(id),
            };
            let r = m::Row {
                status: m::Status::Failed,
                failed_at: 103,
                attempt_count: 0,
                version: 0,
            };
            model.put(k, r.clone()).unwrap();
            a.put(ck(k), cr(&r)).unwrap();
        }
        let g = m::Grant {
            task: m::TaskId(1),
            issuer: m::Principal(1),
            executor: m::Principal(2),
            approver: m::Principal(3),
            policy: m::Policy {
                tenant: m::TenantId(42),
                columns: [m::Column::Status].into(),
                transitions: [(m::Status::Failed, m::Status::Queued)].into(),
                incident: (100, 107),
                distinct_limit: 5,
                transition_limit: 12,
                approval_above: 2,
                valid_from: 1,
                expires: 100,
            },
        };
        let mc = model.grant(g.clone()).unwrap();
        let mut e = a.grant(cg(&g)).unwrap();
        let mut rng = seed + 1;
        let mut approvals = Vec::new();
        let mut trace = String::new();
        for step in 0..100u64 {
            rng = rng
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let action = (rng >> 32) % 12;
            let id = (rng >> 20) % 10;
            let now = step + 1;
            trace.push_str(&format!(
                "seed={seed} step={step} rng={rng} action={action} row={id}\n"
            ));
            let r = m::Request {
                operation: m::OperationId((rng >> 8) % 40),
                selection: if rng & 1 == 0 {
                    m::Selection::Predicate {
                        tenant: m::TenantId(42),
                        status: m::Status::Failed,
                        from: 100,
                        until: 107,
                    }
                } else {
                    m::Selection::Keys(vec![m::Key {
                        tenant: m::TenantId(if action == 4 { 43 } else { 42 }),
                        row: m::RowId(id),
                    }])
                },
                column: if action == 5 {
                    m::Column::AttemptCount
                } else {
                    m::Column::Status
                },
                after: m::Status::Queued,
            };
            match action {
                0 | 1 => {
                    let k = m::Key {
                        tenant: m::TenantId(42),
                        row: m::RowId(id),
                    };
                    let v = m::Row {
                        status: m::Status::Failed,
                        failed_at: if action == 0 { 103 } else { 99 },
                        attempt_count: 0,
                        version: 0,
                    };
                    assert_eq!(
                        format!("{:?}", model.put(k, v.clone())),
                        format!("{:?}", a.put(ck(k), cr(&v)))
                    );
                }
                2 => {
                    model.bump_schema().unwrap();
                    a.bump_schema().unwrap();
                }
                3 | 6 => {
                    let mp = model.preview(&mc, &r, m::Time::Certified(now));
                    let pp = e.preview(&cq(&r), c::Time::Certified(now));
                    match (mp, pp) {
                        (Ok(mp), Ok(pp)) => {
                            assert_eq!(format!("{mp:?}"), format!("{pp:?}"));
                            let mi = model
                                .approve(m::Principal(3), mp, m::Time::Certified(now))
                                .unwrap();
                            let ci = a
                                .approve(c::Principal(3), pp, c::Time::Certified(now))
                                .unwrap();
                            assert_eq!(mi.0, ci.0);
                            approvals.push((mi, r.clone()));
                        }
                        (Err(me), Err(c::CoreError::Policy(ce))) => {
                            assert_eq!(format!("{me:?}"), format!("{ce:?}"))
                        }
                        other => panic!("preview mismatch {other:?}; {trace}"),
                    }
                }
                7 if !approvals.is_empty() => {
                    let (id, _) = approvals[(rng as usize) % approvals.len()].clone();
                    model.revoke_approval(id).unwrap();
                    a.revoke_approval(c::ApprovalId(id.0)).unwrap();
                }
                8 if step > 70 => {
                    model.revoke(m::TaskId(1)).unwrap();
                    a.revoke(c::TaskId(1)).unwrap();
                }
                _ => {
                    let (r, ap) = if action == 9 && !approvals.is_empty() {
                        let (id, r) = approvals[(rng as usize) % approvals.len()].clone();
                        (r, Some(id))
                    } else {
                        (r, None)
                    };
                    let time = if action == 10 {
                        m::Time::Uncertain
                    } else {
                        m::Time::Certified(now)
                    };
                    let out = model.execute(&mc, r.clone(), time, ap, false);
                    let m::Outcome::Known(expected) = out else {
                        unreachable!()
                    };
                    let actual = e
                        .execute(cq(&r), ct(time), ap.map(|id| c::ApprovalId(id.0)))
                        .unwrap();
                    assert_eq!(decision(actual, domain), format!("{expected:?}"), "{trace}");
                }
            }
            assert_eq!(
                format!("{:?}", a.snapshot().unwrap()),
                format!("{:?}", model.snapshot()),
                "{trace}"
            );
            if step % 25 == 24 {
                drop(e);
                drop(a);
                a = c::Admin::open(&p, c::Limits::default()).unwrap();
                e = a.executor(c::TaskId(1), c::Principal(2)).unwrap();
            }
        }
        m::checker::verify(domain, model.history(), &model.snapshot()).unwrap();
        std::fs::write(
            p.join("replay-seed.sha256.txt"),
            format!("{:x}", Sha256::digest(trace.as_bytes())),
        )
        .unwrap();
        std::fs::write(p.join("replay-seed.txt"), trace).unwrap();
    }
}
