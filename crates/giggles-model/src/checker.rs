//! Independent committed-history verifier. Reconstructs rows, task membership,
//! charges, approvals and operation bindings without using Model execution code.
//! It checks successful decisions and state conservation on rejection; it does
//! not establish that every rejection was necessary or verify concurrent histories.
use crate::repair::*;
use std::collections::{BTreeMap, BTreeSet};

fn ensure(ok: bool, why: &str) -> Result<(), String> {
    if ok { Ok(()) } else { Err(why.into()) }
}
fn selected(rows: &BTreeMap<Key, Row>, request: &Request) -> Result<BTreeMap<Key, Row>, String> {
    let mut out = BTreeMap::new();
    match &request.selection {
        Selection::Keys(keys) => {
            for k in keys {
                ensure(!out.contains_key(k), "duplicate key")?;
                out.insert(*k, rows.get(k).ok_or("missing key")?.clone());
            }
        }
        Selection::Predicate {
            tenant,
            status,
            from,
            until,
        } => {
            ensure(from < until, "invalid predicate")?;
            for (k, r) in rows {
                if k.tenant == *tenant
                    && r.status == *status
                    && (*from..*until).contains(&r.failed_at)
                {
                    out.insert(*k, r.clone());
                }
            }
        }
    }
    Ok(out)
}
fn preview_matches(p: &Preview, s: &Snapshot) -> Result<(), String> {
    let t = s.tasks.get(&p.task).ok_or("preview unknown task")?;
    ensure(
        !t.revoked
            && p.domain == s.domain
            && p.schema == s.schema
            && p.policy == t.grant.policy
            && p.expires == t.grant.policy.expires,
        "preview fence",
    )?;
    let deps = selected(&s.rows, &p.request)?;
    ensure(deps == p.dependencies, "preview dependency mismatch")?;
    verify_effects(&deps, &p.effects, &p.request, &t.grant.policy)
}
fn verify_effects(
    deps: &BTreeMap<Key, Row>,
    effects: &[Effect],
    req: &Request,
    p: &Policy,
) -> Result<(), String> {
    ensure(
        req.column == Column::Status && p.columns.contains(&Column::Status),
        "column scope",
    )?;
    if let Selection::Predicate { tenant, .. } = req.selection {
        ensure(tenant == p.tenant, "predicate tenant")?;
    }
    let mut expected = BTreeSet::new();
    for (k, r) in deps {
        ensure(
            k.tenant == p.tenant && (p.incident.0..p.incident.1).contains(&r.failed_at),
            "row scope",
        )?;
        if r.status != req.after {
            ensure(
                p.transitions.contains(&(r.status, req.after)),
                "transition scope",
            )?;
            expected.insert(*k);
        }
    }
    let mut seen = BTreeSet::new();
    for e in effects {
        ensure(
            seen.insert(e.key) && expected.contains(&e.key),
            "effect membership",
        )?;
        ensure(deps.get(&e.key) == Some(&e.before), "before mismatch")?;
        ensure(
            e.after.status == req.after
                && e.after.failed_at == e.before.failed_at
                && e.after.attempt_count == e.before.attempt_count
                && e.before.version.checked_add(1) == Some(e.after.version),
            "after mismatch",
        )?;
    }
    ensure(seen == expected, "missing effect")
}

pub fn verify(domain: u128, history: &[Event], expected: &Snapshot) -> Result<(), String> {
    let mut s = Snapshot {
        domain,
        schema: 0,
        rows: BTreeMap::new(),
        tasks: BTreeMap::new(),
        operations: BTreeMap::new(),
        approvals: BTreeMap::new(),
    };
    let mut last_decision = 0;
    for (index, event) in history.iter().enumerate() {
        let check = (|| -> Result<(), String> {
            match event {
                Event::Put(k, r) => {
                    let version = match s.rows.get(k) {
                        None => 0,
                        Some(old) => old.version.checked_add(1).ok_or("version overflow")?,
                    };
                    ensure(r.version == version, "external version")?;
                    s.rows.insert(*k, r.clone());
                }
                Event::Grant(g) => {
                    ensure(!s.tasks.contains_key(&g.task), "task replaced")?;
                    ensure(
                        g.issuer != g.executor
                            && g.approver != g.executor
                            && g.policy.incident.0 < g.policy.incident.1
                            && g.policy.valid_from < g.policy.expires
                            && !g.policy.columns.is_empty()
                            && !g.policy.transitions.is_empty(),
                        "invalid grant",
                    )?;
                    s.tasks.insert(
                        g.task,
                        TaskState {
                            grant: g.clone(),
                            revoked: false,
                            touched: BTreeSet::new(),
                            transitions: 0,
                        },
                    );
                }
                Event::Revoke(t) => s.tasks.get_mut(t).ok_or("revoke unknown task")?.revoked = true,
                Event::Schema(epoch) => {
                    ensure(s.schema.checked_add(1) == Some(*epoch), "schema sequence")?;
                    s.schema = *epoch;
                }
                Event::Approve(a) => {
                    ensure(
                        !s.approvals.contains_key(&a.id) && !a.revoked && !a.consumed,
                        "approval lifecycle",
                    )?;
                    preview_matches(&a.preview, &s)?;
                    ensure(
                        a.approver == s.tasks[&a.preview.task].grant.approver,
                        "approver",
                    )?;
                    s.approvals.insert(a.id, a.clone());
                }
                Event::RevokeApproval(a) => {
                    s.approvals
                        .get_mut(a)
                        .ok_or("revoke unknown approval")?
                        .revoked = true
                }
                Event::Decision {
                    task,
                    request,
                    time,
                    approval,
                    result,
                } => {
                    let key = (*task, request.operation);
                    ensure(
                        !s.operations.contains_key(&key),
                        "duplicate logical decision",
                    )?;
                    let t = s.tasks.get(task).ok_or("decision unknown task")?;
                    if let Ok(r) = result {
                        let Time::Certified(now) = time else {
                            return Err("uncertain commit".into());
                        };
                        ensure(*now >= last_decision, "decision time regressed")?;
                        last_decision = *now;
                        let p = &t.grant.policy;
                        ensure(
                            !t.revoked && (p.valid_from..p.expires).contains(now),
                            "inactive commit",
                        )?;
                        ensure(
                            r.domain == domain
                                && r.schema == s.schema
                                && r.task == *task
                                && r.operation == request.operation
                                && r.payload == request.canonical()
                                && r.executor == t.grant.executor
                                && r.issuer == t.grant.issuer
                                && r.decided_at == *now,
                            "receipt identity",
                        )?;
                        let deps = selected(&s.rows, request)?;
                        verify_effects(&deps, &r.effects, request, p)?;
                        let mut touched = t.touched.clone();
                        for e in &r.effects {
                            touched.insert(e.key);
                        }
                        let count = t
                            .transitions
                            .checked_add(r.effects.len() as u64)
                            .ok_or("charge overflow")?;
                        ensure(
                            count <= p.transition_limit && touched.len() as u64 <= p.distinct_limit,
                            "budget overrun",
                        )?;
                        ensure(
                            r.charged_transitions == r.effects.len() as u64
                                && r.lifetime_transitions == count
                                && r.lifetime_distinct == touched.len() as u64,
                            "receipt accounting",
                        )?;
                        if r.effects.len() as u64 > p.approval_above || approval.is_some() {
                            let a = s
                                .approvals
                                .get(&approval.ok_or("missing approval")?)
                                .ok_or("unknown approval")?;
                            ensure(
                                !a.revoked
                                    && !a.consumed
                                    && a.preview.task == *task
                                    && a.preview.request.operation == request.operation
                                    && a.preview.request.canonical() == request.canonical()
                                    && r.approver == Some(a.approver),
                                "approval binding",
                            )?;
                            preview_matches(&a.preview, &s)?;
                            ensure(
                                a.preview.effects == r.effects && *now < a.preview.expires,
                                "approved effects",
                            )?;
                        } else {
                            ensure(r.approver.is_none(), "unexpected approver")?;
                        }
                        for e in &r.effects {
                            s.rows.insert(e.key, e.after.clone());
                        }
                        let t = s.tasks.get_mut(task).ok_or("missing task")?;
                        t.transitions = count;
                        t.touched = touched;
                        if let Some(a) = approval {
                            s.approvals.get_mut(a).ok_or("missing approval")?.consumed = true;
                        }
                    }
                    s.operations.insert(
                        key,
                        BoundDecision {
                            payload: request.canonical(),
                            decision: result.clone(),
                        },
                    );
                }
            }
            Ok(())
        })();
        check.map_err(|why| format!("event {index}: {why}"))?;
    }
    ensure(
        &s == expected,
        "final snapshot differs from reconstructed history",
    )
}
