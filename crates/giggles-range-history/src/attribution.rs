use crate::{Result, decode, require};
use giggles_distributed::transaction::Plan;
use giggles_history::{History, Observation, Outcome};
use serde_json::Value;
use std::collections::BTreeMap;

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
        if let Some(t) = a.complete {
            require(pairs.get(&a.invoke) == Some(&t), "mismatched completion")?;
            let response = &ticks[&t]["value"];
            if a.outcome == Outcome::Committed {
                require(response["ok"] == true, "fabricated success")?;
                for observed in &a.observations {
                    if let Observation::Scan { rows } = observed {
                        let actual: Vec<(giggles_core::Key, giggles_core::Row)> =
                            serde_json::from_value(response["data"].clone())?;
                        require(decode(&actual)? == *rows, "incomplete or altered scan")?;
                    }
                }
            } else {
                require(
                    a.observations.is_empty(),
                    "refusal has fabricated observations",
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
            json!({"tick":1,"node":2,"kind":"invoke","value":{"command":"observe"}}),
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
}
