use crate::{Result, decode, require};
use giggles_distributed::transaction::Plan;
use giggles_history::{History, Observation, Op, Outcome};
use serde_json::Value;
use std::collections::BTreeMap;

// Deliberately independent of the generator's operation projection. Reject
// effects the two-account checker model cannot represent completely.
fn transfer_ops(plan: &Plan) -> Result<Vec<Op>> {
    plan.validate()?;
    require(
        plan.database == 42 && plan.participants.len() == 2,
        "unsupported transfer domain/participants",
    )?;
    require(
        plan.participants
            .iter()
            .all(|p| p.writes.len() == 1 && p.reads.is_empty()),
        "unmodeled plan effects",
    )?;
    let writes = plan
        .participants
        .iter()
        .map(|p| &p.writes[0])
        .collect::<Vec<_>>();
    let mut before = Vec::new();
    let mut after = Vec::new();
    for w in writes {
        let old = w.before.clone().ok_or("transfer missing before-image")?;
        require(old.version == w.row.version, "unmodeled version change")?;
        before.push((w.key, old));
        after.push((w.key, w.row.clone()));
    }
    before.sort_by_key(|(k, _)| *k);
    after.sort_by_key(|(k, _)| *k);
    let before = decode(&before)?;
    let after = decode(&after)?;
    require(
        before.iter().map(|(k, _)| *k).collect::<Vec<_>>() == [1, 60],
        "transfer account set",
    )?;
    let amount = before[0].1 - after[0].1;
    require(
        amount > 0 && after[1].1 - before[1].1 == amount,
        "unmodeled transfer effects",
    )?;
    Ok(vec![Op::Transfer {
        from: 1,
        to: 60,
        amount,
    }])
}

/// Independently reconcile checker inputs against retained external evidence.
/// This checks attribution, not the runtime's transaction implementation.
pub fn validate_trace(history: &History, events: &[Value]) -> Result<()> {
    let mut ticks = BTreeMap::new();
    let mut pending = BTreeMap::new();
    let mut pairs = BTreeMap::new();
    let mut plans = BTreeMap::<String, Plan>::new();
    let mut last = 0;
    for event in events {
        let tick = event["tick"].as_u64().ok_or("event tick")?;
        require(tick > last, "nonmonotonic trace")?;
        last = tick;
        ticks.insert(tick, event);
        let node = event["node"].as_u64().ok_or("event node")?;
        let kind = event["kind"].as_str().ok_or("event kind")?;
        if kind == "invoke" {
            require(
                pending.insert(node, tick).is_none(),
                "overlapping uncorrelated pipe",
            )?;
        } else if kind == "complete" {
            let invoke = pending.remove(&node).ok_or("unattributed response")?;
            pairs.insert(invoke, tick);
        }
        if ["invoke", "logical_invoke"].contains(&kind)
            && let Some(value) = event["value"].get("plan")
        {
            let plan: Plan = serde_json::from_value(value.clone())?;
            // Deliberate body-conflict probes are not checker attempts. Their
            // refusal is independently checked by the campaign.
            if let Some(attempt) = history.attempts.iter().find(|a| a.invoke == tick) {
                let id = format!("tx-{}", plan.nonce);
                require(
                    attempt.logical_id == id,
                    "invocation logical identity changed",
                )?;
                require(
                    attempt.ops == transfer_ops(&plan)?,
                    "checker operations differ from recorded plan",
                )?;
                if let Some(old) = plans.get(&id) {
                    require(
                        serde_json::to_value(old)? == *value,
                        "logical plan changed ID/body/home",
                    )?;
                } else {
                    plans.insert(id, plan);
                }
            }
        }
    }
    require(pending.is_empty(), "dropped external response")?;
    for (index, event) in events
        .iter()
        .enumerate()
        .filter(|(_, e)| e["kind"] == "recovery_attempt")
    {
        let request = &event["value"]["request"];
        let plan = plans
            .values()
            .find(|p| serde_json::to_value(p.id()).ok().as_ref() == Some(&request["id"]))
            .ok_or("recovery original plan missing")?;
        require(
            request["home"] == serde_json::to_value(&plan.home)?
                && event["value"]["digest"] == serde_json::to_value(plan.digest()?)?,
            "recovery changed home/body",
        )?;
        let next = events[index + 1..]
            .iter()
            .find(|e| e["node"] == event["node"] && e["kind"] == "invoke")
            .ok_or("recovery request not sent")?;
        require(
            next["value"] == *request,
            "recovery external request changed",
        )?;
    }
    for a in &history.attempts {
        let invocation = ticks.get(&a.invoke).ok_or("missing invocation")?;
        require(
            ["invoke", "logical_invoke"].contains(&invocation["kind"].as_str().unwrap_or("")),
            "fabricated invocation",
        )?;
        let request = &invocation["value"];
        let is_scan = request["command"] == "observe";
        if is_scan {
            require(
                request["bounds"]
                    == serde_json::json!({"start":crate::key(0),"end":crate::key(100)})
                    && a.ops == vec![Op::Scan { start: 0, end: 100 }],
                "checker scan differs from requested bounds",
            )?;
        } else {
            require(
                request.get("plan").is_some() && plans.contains_key(&a.logical_id),
                "attempt has no modeled plan",
            )?;
            require(
                a.observations.is_empty(),
                "transfer has unmodeled observations",
            )?;
        }
        if let Some(t) = a.complete {
            require(pairs.get(&a.invoke) == Some(&t), "mismatched completion")?;
            let response = &ticks[&t]["value"];
            if a.outcome == Outcome::Committed {
                require(
                    response["ok"] == true && response.get("error").is_none(),
                    "fabricated success",
                )?;
                if is_scan {
                    require(
                        matches!(a.observations.as_slice(), [Observation::Scan { .. }]),
                        "missing complete scan observation",
                    )?;
                } else {
                    let plan = &plans[&a.logical_id];
                    let data = &response["data"];
                    let id = serde_json::to_value(plan.id())?;
                    let digest = serde_json::to_value(plan.digest()?)?;
                    require(
                        data["id"] == id
                            && data["digest"] == digest
                            && data["decision"]["transaction"] == id
                            && data["decision"]["digest"] == digest
                            && data["decision"]["outcome"] == "Commit"
                            && data["phase"] == "Committed",
                        "completion contradicts committed transaction",
                    )?;
                }
                for observed in &a.observations {
                    if let Observation::Scan { rows } = observed {
                        let actual: Vec<(giggles_core::Key, giggles_core::Row)> =
                            serde_json::from_value(response["data"].clone())?;
                        require(decode(&actual)? == *rows, "incomplete or altered scan")?;
                    }
                }
            } else {
                require(
                    a.observations.is_empty()
                        && response["ok"] == false
                        && response.get("data").is_none(),
                    "refusal has observations, data or contradictory success",
                )?;
            }
        }
        if let Some(durable) = a.durable {
            let plan = plans.get(&a.logical_id).ok_or("durable plan missing")?;
            let mut first = None;
            for (&invoke, &complete) in &pairs {
                let request = &ticks[&invoke]["value"];
                let response = &ticks[&complete]["value"];
                if complete < a.invoke
                    || request["command"] != "tx_status"
                    || request["id"] != serde_json::to_value(plan.id())?
                    || response["ok"] != true
                {
                    continue;
                }
                require(
                    request["home"] == serde_json::to_value(&plan.home)?,
                    "status home changed",
                )?;
                require(
                    response["data"]["id"] == serde_json::to_value(plan.id())?
                        && response["data"]["digest"] == serde_json::to_value(plan.digest()?)?,
                    "status identity/body changed",
                )?;
                let expected = match durable {
                    Outcome::Committed => "Commit",
                    Outcome::Aborted => "Abort",
                    _ => return Err("indeterminate durable status".into()),
                };
                if response["data"]["decision"].is_null() {
                    continue;
                }
                require(
                    response["data"]["decision"]["outcome"] == expected,
                    "durable conflict",
                )?;
                first = Some(first.map_or(complete, |old: u64| old.min(complete)));
            }
            require(
                first.is_some() && first == a.decision_at,
                "fabricated or non-earliest durable bound",
            )?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use giggles_history::{Attempt, Invariants, Op};
    use serde_json::json;
    fn fixture() -> (History, Vec<Value>) {
        let history = History {
            version: 1,
            horizon: 4,
            initial: [(1, 10), (60, 20)].into(),
            invariants: Invariants::default(),
            attempts: vec![Attempt {
                id: "scan".into(),
                logical_id: "scan".into(),
                invoke: 1,
                complete: Some(3),
                outcome: Outcome::Committed,
                durable: None,
                decision_at: None,
                ops: vec![Op::Scan { start: 0, end: 100 }],
                observations: vec![Observation::Scan {
                    rows: vec![(1, 10), (60, 20)],
                }],
                receipt: None,
            }],
        };
        let events = vec![
            json!({"tick":1,"node":2,"kind":"invoke","value":{"command":"observe","bounds":{"start":crate::key(0),"end":crate::key(100)}}}),
            json!({"tick":2,"node":2,"kind":"pending","value":{}}),
            json!({"tick":3,"node":2,"kind":"complete","value":{"ok":true,"data":[[crate::key(1),crate::balance_row(10)],[crate::key(60),crate::balance_row(20)]]}}),
        ];
        (history, events)
    }
    #[test]
    fn exact_scan_and_pending_evidence_pass() {
        let (h, e) = fixture();
        validate_trace(&h, &e).unwrap();
    }
    #[test]
    fn dropped_read_response_rejected() {
        let (h, mut e) = fixture();
        e.pop();
        assert!(validate_trace(&h, &e).is_err());
    }
    #[test]
    fn incomplete_scan_rejected() {
        let (mut h, e) = fixture();
        let Observation::Scan { rows } = &mut h.attempts[0].observations[0] else {
            panic!()
        };
        rows.pop();
        assert!(validate_trace(&h, &e).is_err());
    }
    #[test]
    fn response_on_wrong_process_rejected() {
        let (h, mut e) = fixture();
        e[2]["node"] = json!(3);
        assert!(validate_trace(&h, &e).is_err());
    }
    #[test]
    fn refusal_cannot_be_success_or_partial_data() {
        let (h, mut e) = fixture();
        e[2]["value"]["ok"] = json!(false);
        assert!(validate_trace(&h, &e).is_err());
        let mut h = h;
        h.attempts[0].outcome = Outcome::Indeterminate;
        assert!(validate_trace(&h, &e).is_err());
    }
    #[test]
    fn duplicate_tick_and_duplicate_response_rejected() {
        let (h, e) = fixture();
        let mut bad = e.clone();
        bad[2]["tick"] = json!(2);
        assert!(validate_trace(&h, &bad).is_err());
        let mut bad = e;
        let mut reply = bad[2].clone();
        reply["tick"] = json!(4);
        bad.push(reply);
        assert!(validate_trace(&h, &bad).is_err());
    }

    #[test]
    fn refused_read_must_not_release_even_unmodeled_data() {
        let (mut h, mut e) = fixture();
        h.attempts[0].outcome = Outcome::Indeterminate;
        h.attempts[0].observations.clear();
        e[2]["value"] = json!({"ok":false,"error":"not leader"});
        validate_trace(&h, &e).unwrap();
        for data in [json!([]), json!(["partial-row"]), Value::Null] {
            e[2]["value"]["data"] = data;
            assert!(validate_trace(&h, &e).is_err());
        }
    }

    #[test]
    fn scan_bounds_and_modeled_operations_must_match() {
        let (h, mut e) = fixture();
        e[0]["value"]["bounds"]["end"] = json!(crate::key(50));
        assert!(validate_trace(&h, &e).is_err());
        let (mut h, e) = fixture();
        h.attempts[0].ops = vec![Op::Transfer {
            from: 60,
            to: 1,
            amount: 999,
        }];
        assert!(validate_trace(&h, &e).is_err());
    }

    fn transfer_fixture() -> (History, Vec<Value>) {
        use giggles_distributed::{
            Bounds, Descriptor,
            transaction::{Participant, Write},
        };
        let left = Descriptor {
            range: 11,
            epoch: 2,
            group: 3,
            bounds: Bounds {
                start: crate::key(0),
                end: Some(crate::key(50)),
            },
            voters: vec![1, 2, 3],
        };
        let right = Descriptor {
            range: 12,
            epoch: 2,
            group: 4,
            bounds: Bounds {
                start: crate::key(50),
                end: Some(crate::key(100)),
            },
            voters: vec![1, 2, 3],
        };
        let plan = Plan {
            database: 42,
            nonce: 1,
            home: left.clone(),
            participants: vec![
                Participant {
                    route: left,
                    writes: vec![Write {
                        key: crate::key(1),
                        before: Some(crate::balance_row(10)),
                        row: crate::balance_row(7),
                    }],
                    reads: vec![],
                },
                Participant {
                    route: right,
                    writes: vec![Write {
                        key: crate::key(60),
                        before: Some(crate::balance_row(20)),
                        row: crate::balance_row(23),
                    }],
                    reads: vec![],
                },
            ],
        };
        let (mut h, mut e) = fixture();
        h.attempts[0].id = "transfer".into();
        h.attempts[0].logical_id = "tx-1".into();
        h.attempts[0].ops = vec![Op::Transfer {
            from: 1,
            to: 60,
            amount: 3,
        }];
        h.attempts[0].observations.clear();
        e[0]["value"] = json!({"command":"transact","plan":plan,"outcome":"Commit"});
        e[2]["value"] = json!({"ok":true,"data":{"id":plan.id(),"digest":plan.digest().unwrap(),"phase":"Committed",
            "decision":{"transaction":plan.id(),"digest":plan.digest().unwrap(),"outcome":"Commit"}}});
        (h, e)
    }

    #[test]
    fn complete_transfer_and_unknown_logical_attempt_are_bound_to_plan() {
        let (h, e) = transfer_fixture();
        validate_trace(&h, &e).unwrap();
        for unknown in [false, true] {
            let mut h = h.clone();
            let mut e = e.clone();
            if unknown {
                h.attempts[0].complete = None;
                h.attempts[0].outcome = Outcome::Indeterminate;
                e.truncate(1);
                e[0]["kind"] = json!("logical_invoke");
                validate_trace(&h, &e).unwrap();
            }
            for ops in [
                vec![],
                vec![Op::Transfer {
                    from: 60,
                    to: 1,
                    amount: 999,
                }],
                vec![Op::Transfer {
                    from: 1,
                    to: 60,
                    amount: 4,
                }],
            ] {
                h.attempts[0].ops = ops;
                assert!(validate_trace(&h, &e).is_err());
            }
        }
    }

    #[test]
    fn committed_transfer_rejects_contradictory_terminal_payloads() {
        let (h, e) = transfer_fixture();
        for path in [
            "/data/id",
            "/data/digest",
            "/data/decision/transaction",
            "/data/decision/digest",
            "/data/decision/outcome",
            "/data/phase",
        ] {
            let mut bad = e.clone();
            *bad[2]["value"].pointer_mut(path).unwrap() = json!("Abort");
            assert!(validate_trace(&h, &bad).is_err(), "accepted {path}");
        }
    }

    #[test]
    fn unsupported_plan_effects_and_missing_writes_fail_without_panic() {
        let (h, e) = transfer_fixture();
        for mutation in 0..4 {
            let mut bad = e.clone();
            let plan = &mut bad[0]["value"]["plan"];
            match mutation {
                0 => plan["participants"][0]["writes"] = json!([]),
                1 => plan["participants"][0]["writes"][0]["row"]["failed_at"] = json!(999),
                2 => plan["participants"][1]["writes"][0]["row"]["attempt_count"] = json!(24),
                _ => plan["participants"][0]["writes"][0]["before"] = Value::Null,
            }
            assert!(validate_trace(&h, &bad).is_err(), "accepted {mutation}");
        }
    }
}
