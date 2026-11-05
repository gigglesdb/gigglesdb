use crate::storage::State;
use crate::*;
use std::collections::{BTreeMap, BTreeSet};

pub(crate) fn clock(s: &mut State, time: Time) -> Result<u64, Error> {
    match time {
        Time::Uncertain => Err(Error::UncertainTime),
        Time::Certified(n) if n < s.last_time => Err(Error::TimeRegression),
        Time::Certified(n) => {
            s.last_time = n;
            Ok(n)
        }
    }
}
pub(crate) fn active(t: &TaskState, now: u64) -> Result<(), Error> {
    if t.revoked {
        return Err(Error::Revoked);
    }
    if !(t.grant.policy.valid_from..t.grant.policy.expires).contains(&now) {
        return Err(Error::Expired);
    }
    Ok(())
}
pub(crate) fn grant_valid(g: &Grant) -> bool {
    let p = &g.policy;
    p.incident.0 < p.incident.1
        && p.valid_from < p.expires
        && g.issuer != g.executor
        && g.approver != g.executor
        && !p.columns.is_empty()
        && !p.transitions.is_empty()
}
/// Independent evaluator: scans the committed ordered row set under the local
/// serialization gate, protecting both rows and complete predicate membership.
pub(crate) fn enumerate(s: &State, t: &TaskState, r: &Request) -> Result<Preview, Error> {
    let p = &t.grant.policy;
    if r.column != Column::Status || !p.columns.contains(&Column::Status) {
        return Err(Error::Scope);
    }
    let selected: BTreeSet<Key> = match &r.selection {
        Selection::Keys(ks) => {
            let set: BTreeSet<_> = ks.iter().copied().collect();
            if set.len() != ks.len() {
                return Err(Error::DuplicateKey);
            }
            set
        }
        Selection::Predicate {
            tenant,
            status,
            from,
            until,
        } => {
            if *tenant != p.tenant {
                return Err(Error::Scope);
            }
            if from >= until {
                return Err(Error::InvalidPredicate);
            }
            s.view
                .rows
                .iter()
                .filter_map(|(k, v)| {
                    (k.tenant == *tenant
                        && v.status == *status
                        && (*from..*until).contains(&v.failed_at))
                    .then_some(*k)
                })
                .collect()
        }
    };
    let mut dependencies = BTreeMap::new();
    let mut effects = Vec::new();
    for k in selected {
        if k.tenant != p.tenant {
            return Err(Error::Scope);
        }
        let before = s.view.rows.get(&k).ok_or(Error::MissingRow)?;
        if !(p.incident.0..p.incident.1).contains(&before.failed_at) {
            return Err(Error::Scope);
        }
        dependencies.insert(k, before.clone());
        if before.status != r.after {
            if !p.transitions.contains(&(before.status, r.after)) {
                return Err(Error::Scope);
            }
            let mut after = before.clone();
            after.version = before.version.checked_add(1).ok_or(Error::Overflow)?;
            after.status = r.after;
            effects.push(Effect {
                key: k,
                before: before.clone(),
                after,
            });
        }
    }
    Ok(Preview {
        domain: s.view.domain,
        task: t.grant.task,
        request: r.clone(),
        schema: s.view.schema,
        policy: p.clone(),
        dependencies,
        effects,
        expires: p.expires,
    })
}
pub(crate) fn charges(t: &TaskState, p: &Preview) -> Result<(BTreeSet<Key>, u64), Error> {
    let mut ids = t.touched.clone();
    ids.extend(p.effects.iter().map(|e| e.key));
    let transitions = t
        .transitions
        .checked_add(p.effects.len() as u64)
        .ok_or(Error::Overflow)?;
    if ids.len() as u64 > t.grant.policy.distinct_limit
        || transitions > t.grant.policy.transition_limit
    {
        return Err(Error::Budget);
    }
    Ok((ids, transitions))
}
pub(crate) fn preview(
    s: &mut State,
    task: TaskId,
    r: &Request,
    time: Time,
) -> Result<Preview, Error> {
    let now = clock(s, time)?;
    let t = s.view.tasks.get(&task).ok_or(Error::Authentication)?;
    active(t, now)?;
    let p = enumerate(s, t, r)?;
    charges(t, &p)?;
    Ok(p)
}
pub(crate) fn execute(
    s: &mut State,
    task: TaskId,
    r: Request,
    time: Time,
    approval: Option<ApprovalId>,
) -> Decision {
    let payload = r.canonical();
    let key = (task, r.operation);
    if let Some(bound) = s.view.operations.get(&key) {
        return if bound.payload == payload {
            bound.decision.clone()
        } else {
            Err(Error::IdConflict)
        };
    }
    let result = decide(s, task, &r, time, approval);
    s.view.operations.insert(
        key,
        BoundDecision {
            payload,
            decision: result.clone(),
        },
    );
    result
}
fn decide(
    s: &mut State,
    task: TaskId,
    r: &Request,
    time: Time,
    approval: Option<ApprovalId>,
) -> Decision {
    let now = clock(s, time)?;
    let t = s.view.tasks.get(&task).ok_or(Error::Authentication)?;
    active(t, now)?;
    let p = enumerate(s, t, r)?;
    let (ids, total) = charges(t, &p)?;
    let approver = if p.effects.len() as u64 > t.grant.policy.approval_above || approval.is_some() {
        let a = s
            .view
            .approvals
            .get(&approval.ok_or(Error::ApprovalRequired)?)
            .ok_or(Error::InvalidApproval)?;
        if a.revoked
            || a.consumed
            || a.approver != t.grant.approver
            || a.preview.task != task
            || a.preview.request.operation != r.operation
            || a.preview.request.canonical() != r.canonical()
        {
            return Err(Error::InvalidApproval);
        }
        // Canonical payload equivalence permits reordered unique explicit keys.
        let mut expected = p.clone();
        expected.request = a.preview.request.clone();
        if a.preview != expected || now >= a.preview.expires {
            return Err(Error::StalePreview);
        }
        Some(a.approver)
    } else {
        None
    };
    let receipt = Receipt {
        domain: s.view.domain,
        task,
        operation: r.operation,
        payload: r.canonical(),
        issuer: t.grant.issuer,
        executor: t.grant.executor,
        approver,
        schema: s.view.schema,
        decided_at: now,
        effects: p.effects.clone(),
        charged_transitions: p.effects.len() as u64,
        lifetime_transitions: total,
        lifetime_distinct: ids.len() as u64,
    };
    for e in p.effects {
        s.view.rows.insert(e.key, e.after);
    }
    let t = s.view.tasks.get_mut(&task).expect("validated task");
    t.touched = ids;
    t.transitions = total;
    if let Some(a) = approval {
        s.view
            .approvals
            .get_mut(&a)
            .expect("validated approval")
            .consumed = true;
    }
    Ok(receipt)
}
