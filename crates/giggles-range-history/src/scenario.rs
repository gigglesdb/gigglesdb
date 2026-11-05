use crate::process::{Cluster, event};
use crate::{
    CaptureResult, Result, balance_row, decode, fresh, key, negative_controls, require, save,
    source_hashes,
};
use giggles_core::{Key, Row};
use giggles_distributed::{
    Action, Bounds, Descriptor, Split, minimum,
    transaction::{Participant, Pinned, Plan, Request, Step, Write},
};
use giggles_history::{Attempt, History, Invariants, Limits, Observation, Op, Outcome, Verdict};
use giggles_transactions::{Outcome as Decision, TransactionId};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    path::Path,
    time::{Duration, Instant},
};

fn split() -> Split {
    let parent = Descriptor {
        range: 10,
        epoch: 1,
        group: 2,
        bounds: Bounds {
            start: minimum(),
            end: None,
        },
        voters: vec![1, 2, 3],
    };
    let mut left = parent.clone();
    left.range = 11;
    left.group = 3;
    left.epoch = 2;
    left.bounds.end = Some(key(50));
    let mut right = parent.clone();
    right.range = 12;
    right.group = 4;
    right.epoch = 2;
    right.bounds.start = key(50);
    Split {
        id: 77,
        parent,
        key: key(50),
        left,
        right,
    }
}
fn bounds() -> Bounds {
    Bounds {
        start: key(0),
        end: Some(key(100)),
    }
}
fn transfer_plan(nonce: u128, rows: &[(Key, Row)], amount: u32) -> Result<Plan> {
    require(
        decode(rows)?.len() == 2 && rows[0].0 == key(1) && rows[1].0 == key(60),
        "missing account",
    )?;
    require(
        amount > 0 && rows[0].1.attempt_count >= u64::from(amount),
        "invalid transfer",
    )?;
    let s = split();
    let mut a = rows[0].1.clone();
    let mut b = rows[1].1.clone();
    a.attempt_count -= u64::from(amount);
    b.attempt_count = b
        .attempt_count
        .checked_add(u64::from(amount))
        .filter(|v| *v <= u64::from(u32::MAX))
        .ok_or("balance overflow")?;
    let p = Plan {
        database: 42,
        nonce,
        home: s.left.clone(),
        participants: vec![
            Participant {
                route: s.left,
                writes: vec![Write {
                    key: key(1),
                    before: Some(rows[0].1.clone()),
                    row: a,
                }],
                reads: vec![],
            },
            Participant {
                route: s.right,
                writes: vec![Write {
                    key: key(60),
                    before: Some(rows[1].1.clone()),
                    row: b,
                }],
                reads: vec![],
            },
        ],
    };
    p.validate()?;
    Ok(p)
}
fn ops(p: &Plan) -> Result<Vec<Op>> {
    require(p.participants.len() == 2, "unsupported plan")?;
    let a = &p.participants[0].writes[0];
    let b = &p.participants[1].writes[0];
    let before = decode(&[
        (a.key, a.before.clone().ok_or("missing CAS")?),
        (b.key, b.before.clone().ok_or("missing CAS")?),
    ])?;
    let after = decode(&[(a.key, a.row.clone()), (b.key, b.row.clone())])?;
    let amount = before[0].1 - after[0].1;
    require(
        amount > 0 && after[1].1 - before[1].1 == amount,
        "not a conserved transfer",
    )?;
    Ok(vec![Op::Transfer {
        from: 1,
        to: 60,
        amount,
    }])
}
fn attempt(p: &Plan, invoke: u64, label: &str) -> Result<Attempt> {
    Ok(Attempt {
        id: label.into(),
        logical_id: format!("tx-{}", p.nonce),
        invoke,
        complete: None,
        outcome: Outcome::Indeterminate,
        durable: None,
        decision_at: None,
        ops: ops(p)?,
        observations: vec![],
        receipt: None,
    })
}
fn tx(c: &mut Cluster, route: &Descriptor, step: Step, evidence: Vec<Pinned>) -> Result<Value> {
    let node = c.leader(route.group)?;
    c.ok(
        node,
        json!({"command":"tx_step","request":Request{route:route.clone(),step,evidence}}),
    )
}
fn pin(c: &mut Cluster, route: &Descriptor, id: TransactionId) -> Result<Pinned> {
    let node = c.leader(route.group)?;
    Ok(
        serde_json::from_value::<Pinned>(c.ok(node, json!({"command":"tx_pin","route":route}))?)?
            .for_transaction(id),
    )
}
fn prepare(c: &mut Cluster, p: &Plan) -> Result<()> {
    let id = p.id();
    tx(c, &p.home, Step::Open(p.clone()), vec![])?;
    let h = pin(c, &p.home, id)?;
    for part in &p.participants {
        tx(c, &part.route, Step::Begin { id }, vec![h.clone()])?;
    }
    for part in &p.participants {
        tx(c, &part.route, Step::Lock { id }, vec![])?;
    }
    let locked = p
        .participants
        .iter()
        .map(|p| pin(c, &p.route, id))
        .collect::<Result<Vec<_>>>()?;
    for part in &p.participants {
        tx(c, &part.route, Step::Prepare { id }, locked.clone())?;
    }
    for part in &p.participants {
        let v = pin(c, &part.route, id)?;
        tx(
            c,
            &p.home,
            Step::Vote {
                id,
                participant: part.route.clone(),
            },
            vec![v],
        )?;
    }
    Ok(())
}
fn durable(c: &mut Cluster, p: &Plan, a: &mut Attempt) -> Result<Value> {
    let node = c.leader(p.home.group)?;
    let (_, tick, v) = c.request(
        node,
        json!({"command":"tx_status","home":p.home,"id":p.id()}),
    )?;
    require(v["ok"] == true, "status unavailable")?;
    apply_status(a, p, tick, &v["data"])?;
    // Other overlapping attempts can already have observed this same durable
    // identity. Retain the earliest explicit status after this invocation.
    let mut pending = BTreeMap::new();
    for e in c.events()? {
        let node = e["node"].as_u64().ok_or("event node")?;
        if e["kind"] == "invoke" {
            pending.insert(node, e["value"].clone());
        }
        if e["kind"] == "complete"
            && let Some(request) = pending.remove(&node)
        {
            let t = e["tick"].as_u64().ok_or("status tick")?;
            if t >= a.invoke
                && request["command"] == "tx_status"
                && request["id"] == serde_json::to_value(p.id())?
                && request["home"] == serde_json::to_value(&p.home)?
                && e["value"]["ok"] == true
            {
                // A preceding undecided observation does not regress a later
                // decision. Undecided status after the bound still rejects.
                if e["value"]["data"]["decision"].is_null()
                    && a.decision_at.is_some_and(|bound| t < bound)
                {
                    continue;
                }
                apply_status(a, p, t, &e["value"]["data"])?;
            }
        }
    }
    Ok(v["data"].clone())
}
fn apply_status(a: &mut Attempt, p: &Plan, tick: u64, data: &Value) -> Result<()> {
    require(tick >= a.invoke, "durable bound predates invocation")?;
    require(
        data["id"] == serde_json::to_value(p.id())?
            && data["digest"] == serde_json::to_value(p.digest()?)?,
        "status identity/body mismatch",
    )?;
    let observed = if data["decision"].is_null() {
        None
    } else {
        match data["decision"]["outcome"].as_str() {
            Some("Commit") => Some(Outcome::Committed),
            Some("Abort") => Some(Outcome::Aborted),
            _ => return Err("unsupported durable status".into()),
        }
    };
    require(
        a.durable.is_none() || a.durable == observed,
        "durable outcome changed or disappeared",
    )?;
    if observed.is_some() {
        a.durable = observed;
        a.decision_at = Some(a.decision_at.map_or(tick, |old| old.min(tick)));
    }
    Ok(())
}
fn recover(c: &mut Cluster, p: &Plan, outcome: Decision) -> Result<Value> {
    let end = Instant::now() + Duration::from_secs(60);
    let request = recovery_request(p, outcome);
    for round in 0..8 {
        require(
            recovery_allowed(round, end, Instant::now()),
            "recovery deadline exhausted; unresolved uncertainty",
        )?;
        c.colocate_until(end)?;
        require(Instant::now() < end, "recovery routing deadline exhausted")?;
        event(
            &c.trace,
            1,
            "recovery_attempt",
            json!({"round":round,"request":request,"digest":p.digest()?}),
        )?;
        let (_, _, response) = c.request_until(1, request.clone(), end)?;
        if response["ok"] == true {
            require(
                response["data"]["id"] == serde_json::to_value(p.id())?
                    && response["data"]["digest"] == serde_json::to_value(p.digest()?)?,
                "recovery identity/body mismatch",
            )?;
            return Ok(response["data"].clone());
        }
        let message = response["error"].as_str().unwrap_or("");
        require(
            retryable(message),
            &format!("nonretryable recovery refusal: {response}"),
        )?;
    }
    Err("recovery attempt cap exhausted; unresolved uncertainty".into())
}
fn recovery_allowed(round: usize, end: Instant, now: Instant) -> bool {
    round < 8 && now < end
}
fn recovery_request(p: &Plan, outcome: Decision) -> Value {
    json!({"command":"tx_recover","home":p.home,"id":p.id(),"outcome":outcome})
}
fn retryable(message: &str) -> bool {
    ["leadership", "leader", "unavailable", "timeout"]
        .iter()
        .any(|s| message.contains(s))
}
fn scan(c: &mut Cluster, h: &mut History, label: &str) -> Result<Vec<(Key, Row)>> {
    let (i, t, v) = c.request(1, json!({"command":"observe","bounds":bounds()}))?;
    require(v["ok"] == true, &format!("strict observe refused {v}"))?;
    let rows: Vec<(Key, Row)> = serde_json::from_value(v["data"].clone())?;
    let values = decode(&rows)?;
    h.attempts.push(Attempt {
        id: label.into(),
        logical_id: label.into(),
        invoke: i,
        complete: Some(t),
        outcome: Outcome::Committed,
        durable: None,
        decision_at: None,
        ops: vec![Op::Scan { start: 0, end: 100 }],
        observations: vec![Observation::Scan { rows: values }],
        receipt: None,
    });
    Ok(rows)
}

/// Run a retained real-process campaign. Execution failures retain error.json.
pub fn run(binary: &Path, parent: &Path, seed: u64) -> Result<CaptureResult> {
    let root = fresh(parent, &format!("range-seed-{seed}"))?;
    let result = campaign(binary, &root, seed);
    if let Err(e) = &result {
        save(
            &root.join("error.json"),
            &json!({"verdict":"INCONCLUSIVE","error":e.to_string(),"artifacts":root}),
        )?;
    }
    result.map_err(|e| format!("{e}; artifacts={}", root.display()).into())
}
fn campaign(binary: &Path, root: &Path, seed: u64) -> Result<CaptureResult> {
    use sha2::{Digest, Sha256};
    let before = source_hashes()?;
    save(&root.join("dependency-before.json"), &before)?;
    save(
        &root.join("manifest.json"),
        &json!({"seed":seed,"profile":"three local mTLS processes, trusted owner stdin, two CAS accounts","binary_sha256":format!("{:x}",Sha256::digest(std::fs::read(binary)?))}),
    )?;
    let s = split();
    let mut c = Cluster::new(binary, root, s.parent.clone())?;
    c.ok(
        1,
        json!({"command":"apply","group":1,"action":Action::Begin(s.clone())}),
    )?;
    c.ok(1, json!({"command":"resume","id":77}))?;
    for id in [1, 60] {
        c.ok(1,json!({"command":"put","key":key(id),"operation":id,"expected":null,"row":balance_row(1000)}))?;
    }
    let data = c.ok(1, json!({"command":"observe","bounds":bounds()}))?;
    let rows: Vec<(Key, Row)> = serde_json::from_value(data)?;
    let initial = decode(&rows)?;
    require(
        initial == vec![(1, 1000), (60, 1000)],
        "initial observation mismatch",
    )?;
    let mut h = History {
        version: 1,
        horizon: 0,
        initial: initial.into_iter().collect(),
        invariants: Invariants {
            conserved_keys: vec![1, 60],
            nonnegative_keys: vec![1, 60],
            ..Default::default()
        },
        attempts: vec![],
    };
    let mut checks = BTreeMap::new();
    let amount = (seed % 7 + 2) as u32;
    // Independent real leaders allow the coordinator to die without killing
    // either prepared participant leader.
    c.transfer(3, 2)?;
    c.transfer(4, 3)?;
    let p = transfer_plan(100, &rows, amount)?;
    let i = event(&c.trace, 1, "logical_invoke", json!({"plan":p}))?;
    let mut a = attempt(&p, i, "predecision-loss")?;
    prepare(&mut c, &p)?;
    // Move the durable transaction home to the process being terminated.
    // The earlier independent leaders exercised cross-process evidence pins.
    c.colocate()?;
    let status = durable(&mut c, &p, &mut a)?;
    require(status["decision"].is_null(), "predecision already decided")?;
    c.kill(1)?;
    checks.insert("coordinator_terminated_before_decision".into(), true);
    c.start(1)?;
    recover(&mut c, &p, Decision::Abort)?;
    durable(&mut c, &p, &mut a)?;
    require(
        a.durable == Some(Outcome::Aborted),
        "predecision resolution not aborted",
    )?;
    h.attempts.push(a);
    // Two real external invocations overlap: a prepared transfer and a stale
    // competing CAS. No success is inferred from the final account sum.
    let p = transfer_plan(101, &rows, amount)?;
    let q = transfer_plan(102, &rows, amount + 1)?;
    let i = event(&c.trace, 1, "logical_invoke", json!({"plan":p}))?;
    let mut a = attempt(&p, i, "postdecision-loss")?;
    prepare(&mut c, &p)?;
    // The prepared logical invocation remains open while a second external
    // transfer runs against the actual locks on the colocated owner.
    let qi = c
        .nodes
        .get_mut(&1)
        .ok_or("node1")?
        .send(json!({"command":"transact","plan":q,"outcome":"Commit"}))?;
    let mut b = attempt(&q, qi, "overlapping-stale-cas")?;
    let (qt, qv) = c.nodes.get_mut(&1).ok_or("node1")?.receive()?;
    b.complete = Some(qt);
    require(qv["ok"] == false, "competing CAS unexpectedly accepted")?;
    recover(&mut c, &q, Decision::Abort)?;
    durable(&mut c, &q, &mut b)?;
    require(
        b.durable == Some(Outcome::Aborted),
        "competing transfer unresolved",
    )?;
    h.attempts.push(b);
    tx(
        &mut c,
        &p.home,
        Step::Decide {
            id: p.id(),
            outcome: Decision::Commit,
        },
        vec![],
    )?;
    durable(&mut c, &p, &mut a)?;
    require(
        a.durable == Some(Outcome::Committed),
        "commit decision absent",
    )?;
    let home = pin(&mut c, &p.home, p.id())?;
    tx(
        &mut c,
        &p.participants[0].route,
        Step::Finalize { id: p.id() },
        vec![home],
    )?;
    event(
        &c.trace,
        1,
        "participant_finalize_delayed",
        json!({"group":4,"id":p.id()}),
    )?;
    let delayed = pin(&mut c, &p.participants[1].route, p.id())?;
    require(
        delayed
            .state
            .records
            .iter()
            .any(|r| r.plan.id() == p.id() && r.locked),
        "delayed participant lock absent",
    )?;
    c.kill(1)?;
    checks.insert("coordinator_terminated_after_decision".into(), true);
    // Surviving peers elect actual group leaders before coordinator restart.
    let survivor = c.leader(3)?;
    require(survivor != 1, "dead leader returned")?;
    checks.insert("survivor_elected".into(), true);
    c.start(1)?;
    c.colocate()?;
    // This is an actual external request while the committed participant is
    // still locked. A follower may refuse: retain that capability boundary.
    let locked = pin(&mut c, &p.participants[1].route, p.id())?;
    require(
        locked
            .state
            .records
            .iter()
            .any(|r| r.plan.id() == p.id() && r.locked),
        "read experiment lost unpublished lock",
    )?;
    let read_invoke = c
        .nodes
        .get_mut(&2)
        .ok_or("node2")?
        .send(json!({"command":"observe","bounds":bounds()}))?;
    let read_response = c
        .nodes
        .get_mut(&2)
        .ok_or("node2")?
        .poll_receive(Duration::from_millis(50))?;
    let was_pending = read_response.is_none();
    event(
        &c.trace,
        2,
        "read_barrier",
        json!({"invoke":read_invoke,"pending":was_pending,"locked_id":p.id()}),
    )?;
    // Recovery is driven through a different, available owner stdin channel.
    // Immediate refusal means parked-read liveness is unsupported, not proven.
    recover(&mut c, &p, Decision::Abort)?;
    let (read_complete, read_value) = match read_response {
        Some(v) => v,
        None => c.nodes.get_mut(&2).ok_or("node2")?.receive()?,
    };
    if read_value["ok"] == true {
        let actual: Vec<(Key, Row)> = serde_json::from_value(read_value["data"].clone())?;
        require(decode(&actual)?.len() == 2, "pending read incomplete scan")?;
        require(
            actual.iter().all(|(k, r)| {
                p.participants
                    .iter()
                    .flat_map(|part| &part.writes)
                    .any(|w| w.key == *k && w.row.attempt_count == r.attempt_count)
            }),
            "pending read returned partial commit",
        )?;
        h.attempts.push(Attempt {
            id: "external-locked-read".into(),
            logical_id: "external-locked-read".into(),
            invoke: read_invoke,
            complete: Some(read_complete),
            outcome: Outcome::Committed,
            durable: None,
            decision_at: None,
            ops: vec![Op::Scan { start: 0, end: 100 }],
            observations: vec![Observation::Scan {
                rows: decode(&actual)?,
            }],
            receipt: None,
        });
    } else {
        require(
            read_value.get("data").is_none(),
            "refused read leaked partial rows",
        )?;
        h.attempts.push(Attempt {
            id: "external-locked-read".into(),
            logical_id: "external-locked-read".into(),
            invoke: read_invoke,
            complete: Some(read_complete),
            outcome: Outcome::Indeterminate,
            durable: None,
            decision_at: None,
            ops: vec![Op::Scan { start: 0, end: 100 }],
            observations: vec![],
            receipt: None,
        });
    }
    event(
        &c.trace,
        2,
        "read_capability",
        json!({"invoke":read_invoke,"complete":read_complete,"pending_observed":was_pending,"complete_scan":read_value["ok"] == true,"response":read_value,"liveness_proven":was_pending && read_value["ok"] == true}),
    )?;
    checks.insert("external_locked_read_response_retained".into(), true);
    // A strict observation must resolve the delayed participant from the
    // retained commit decision before returning any complete scan.
    let rows = scan(&mut c, &mut h, "delayed-participant-scan")?;
    recover(&mut c, &p, Decision::Abort)?;
    durable(&mut c, &p, &mut a)?;
    require(
        a.durable == Some(Outcome::Committed),
        "recovery changed commit",
    )?;
    h.attempts.push(a);
    // Exact original body/id/home replay, after witnessed effects.
    let (i, t, v) = c.request(1, json!({"command":"transact","plan":p,"outcome":"Commit"}))?;
    require(v["ok"] == true, "same-body retry failed")?;
    let mut retry = attempt(&p, i, "same-id-retry")?;
    retry.complete = Some(t);
    retry.outcome = Outcome::Committed;
    durable(&mut c, &p, &mut retry)?;
    h.attempts.push(retry);
    // Two real automatic coordinators on distinct owner pipes, both sent before
    // either is received. Local-leader requirements may refuse both calls.
    let mut overlapping = Vec::new();
    for node in [2, 3] {
        let i = c
            .nodes
            .get_mut(&node)
            .ok_or("overlap node")?
            .send(json!({"command":"transact","plan":p,"outcome":"Commit"}))?;
        overlapping.push((
            node,
            attempt(&p, i, &format!("external-coordinator-{node}"))?,
        ));
    }
    for (node, mut a) in overlapping {
        let (t, v) = c.nodes.get_mut(&node).ok_or("overlap node")?.receive()?;
        a.complete = Some(t);
        if v["ok"] == true {
            a.outcome = Outcome::Committed;
        } else {
            require(v.get("data").is_none(), "refused coordinator leaked data")?;
        }
        durable(&mut c, &p, &mut a)?;
        h.attempts.push(a);
    }
    checks.insert("distinct_pipe_automatic_invocations".into(), true);
    let mut conflict = p.clone();
    conflict.participants[0].writes[0].row.attempt_count += 1;
    let (_, _, v) = c.request(
        1,
        json!({"command":"transact","plan":conflict,"outcome":"Commit"}),
    )?;
    require(v["ok"] == false, "changed body reused identity")?;
    checks.insert("conflicting_body_refused".into(), true);
    let stale = transfer_plan(
        103,
        &[(key(1), balance_row(1000)), (key(60), balance_row(1000))],
        amount,
    )?;
    let (i, t, v) = c.request(
        1,
        json!({"command":"transact","plan":stale,"outcome":"Commit"}),
    )?;
    require(v["ok"] == false, "stale exact CAS accepted")?;
    let mut stale_attempt = attempt(&stale, i, "stale-before-cas")?;
    stale_attempt.complete = Some(t);
    recover(&mut c, &stale, Decision::Abort)?;
    durable(&mut c, &stale, &mut stale_attempt)?;
    require(
        stale_attempt.durable == Some(Outcome::Aborted),
        "CAS rejection unresolved",
    )?;
    h.attempts.push(stale_attempt);
    checks.insert("stale_cas_refused".into(), true);
    let next = transfer_plan(104, &rows, amount + 2)?;
    let (i, t, v) = c.request(
        1,
        json!({"command":"transact","plan":next,"outcome":"Commit"}),
    )?;
    require(
        v["ok"] == true && v["data"]["decision"]["outcome"] == "Commit",
        &format!("normal transfer failed: {v}"),
    )?;
    let mut a = attempt(&next, i, "normal-transfer")?;
    a.complete = Some(t);
    a.outcome = Outcome::Committed;
    durable(&mut c, &next, &mut a)?;
    h.attempts.push(a);
    scan(&mut c, &mut h, "final-scan")?;
    checks.insert("delayed_participant_resolved".into(), true);
    // Ensure an externally unresolved attempt is represented, rather than
    // silently counted as a completed success.
    checks.insert(
        "unknown_response_retained".into(),
        h.attempts
            .iter()
            .any(|a| a.complete.is_none() && a.outcome == Outcome::Indeterminate),
    );
    c.shutdown()?;
    h.horizon = event(&c.trace, 0, "horizon", json!({}))?;
    let events = std::fs::read_to_string(root.join("events.jsonl"))?
        .lines()
        .map(serde_json::from_str)
        .collect::<std::result::Result<Vec<Value>, _>>()?;
    crate::validate_trace(&h, &events)?;
    let check = giggles_history::check(&h, Limits::default())?;
    giggles_history::write_jsonl(&h, std::fs::File::create_new(root.join("history.jsonl"))?)?;
    save(&root.join("checker.json"), &check)?;
    let mut controls = BTreeMap::new();
    if check.verdict == Verdict::Pass {
        for (name, negative) in negative_controls(&h)? {
            giggles_history::write_jsonl(
                &negative,
                std::fs::File::create_new(root.join(format!("{name}.jsonl")))?,
            )?;
            let result = giggles_history::check(&negative, Limits::default())?;
            save(&root.join(format!("{name}-checker.json")), &result)?;
            controls.insert(name, result.verdict);
        }
    }
    let after = source_hashes()?;
    save(&root.join("dependency-after.json"), &after)?;
    checks.insert("dependency_sources_unchanged".into(), before == after);
    save(&root.join("runtime-checks.json"), &checks)?;
    let r = CaptureResult {
        artifacts: root.into(),
        original_verdict: check.verdict,
        verdict: crate::combined_verdict(check.verdict, &controls, &checks),
        controls,
        runtime_checks: checks,
    };
    save(&root.join("result.json"), &r)?;
    Ok(r)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn recovery_budget_stops() {
        let start = Instant::now();
        let end = start + Duration::from_secs(60);
        for n in 0..8 {
            assert!(recovery_allowed(n, end, start));
        }
        assert!(!recovery_allowed(8, end, start));
        assert!(!recovery_allowed(0, end, end));
    }
    #[test]
    fn recovery_request_preserves_identity_body_home() {
        let (p, _, _) = status_fixture();
        let saved = serde_json::to_value(&p).unwrap();
        let request = recovery_request(&p, Decision::Abort);
        for _ in 0..8 {
            assert_eq!(request, recovery_request(&p, Decision::Abort));
        }
        assert_eq!(request["home"], saved["home"]);
        assert_eq!(request["id"], serde_json::to_value(p.id()).unwrap());
        assert_eq!(saved, serde_json::to_value(&p).unwrap());
    }
    #[test]
    fn recovery_refuses_nonretryable_errors() {
        assert!(retryable("indeterminate leadership change; inspect/resume"));
        assert!(retryable("not leader"));
        for e in [
            "changed body",
            "capacity",
            "unknown ID",
            "wrong home",
            "invalid request",
        ] {
            assert!(!retryable(e));
        }
    }
    #[test]
    fn durable_evidence_rejects_fabricated_bound() {
        let (p, mut a, v) = status_fixture();
        a.invoke = 10;
        assert!(apply_status(&mut a, &p, 9, &v).is_err());
        assert_eq!(a.durable, None);
    }
    #[test]
    fn transfer_plan_validates_before_images() {
        let rows = vec![(key(1), balance_row(10)), (key(60), balance_row(20))];
        let p = transfer_plan(1, &rows, 3).unwrap();
        assert_eq!(
            ops(&p).unwrap(),
            vec![Op::Transfer {
                from: 1,
                to: 60,
                amount: 3
            }]
        );
        for a in [0, 11] {
            assert!(transfer_plan(1, &rows, a).is_err());
        }
        let mut overflow = rows.clone();
        overflow[1].1.attempt_count = u64::from(u32::MAX);
        assert!(transfer_plan(1, &overflow, 1).is_err());
    }
    fn status_fixture() -> (Plan, Attempt, Value) {
        let p = transfer_plan(
            1,
            &[(key(1), balance_row(10)), (key(60), balance_row(20))],
            3,
        )
        .unwrap();
        let a = attempt(&p, 1, "a").unwrap();
        let data =
            json!({"id":p.id(),"digest":p.digest().unwrap(),"decision":{"outcome":"Commit"}});
        (p, a, data)
    }
    #[test]
    fn durable_evidence_keeps_earliest_observed_bound() {
        let (p, mut a, v) = status_fixture();
        apply_status(&mut a, &p, 10, &v).unwrap();
        apply_status(&mut a, &p, 20, &v).unwrap();
        assert_eq!(a.decision_at, Some(10));
        assert_eq!(a.outcome, Outcome::Indeterminate);
        assert_eq!(a.complete, None);
    }
    #[test]
    fn durable_evidence_rejects_conflict_and_regression() {
        let (p, mut a, v) = status_fixture();
        apply_status(&mut a, &p, 10, &v).unwrap();
        for decision in [
            Value::Null,
            json!({"outcome":"Abort"}),
            json!({"outcome":"invalid"}),
        ] {
            let mut bad = v.clone();
            bad["decision"] = decision;
            assert!(apply_status(&mut a, &p, 20, &bad).is_err());
            assert_eq!(a.durable, Some(Outcome::Committed));
            assert_eq!(a.decision_at, Some(10));
        }
    }
    #[test]
    fn durable_evidence_rejects_wrong_identity_or_body() {
        let (p, a, v) = status_fixture();
        for name in ["id", "digest"] {
            let mut a = a.clone();
            let mut bad = v.clone();
            bad[name] = json!("wrong");
            assert!(apply_status(&mut a, &p, 10, &bad).is_err());
            assert_eq!(a.durable, None);
        }
    }
}
