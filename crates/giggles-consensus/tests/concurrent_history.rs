mod common;
use common::*;
use giggles_consensus::*;
use giggles_core::{Status, TaskId};
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;

// Independent bounded serial search: numeric rows/operation IDs/codes only.
// It calls no policy, model, image evaluation or Raft decision functions.
#[derive(Clone, Default)]
struct Oracle {
    rows: BTreeSet<u64>,
    decisions: BTreeMap<u64, (u64, u64)>,
}
fn search(
    events: &[(u64, u64, u64)],
    mask: u64,
    state: Oracle,
    order: &mut Vec<usize>,
    final_rows: &BTreeSet<u64>,
) -> bool {
    if mask == (1 << events.len()) - 1 {
        return &state.rows == final_rows;
    }
    for (i, (id, row, observed)) in events.iter().enumerate() {
        if mask & (1 << i) != 0 {
            continue;
        }
        let mut next = state.clone();
        let code = if let Some((old, code)) = next.decisions.get(id) {
            if old == row { *code } else { 116 }
        } else {
            let code = if !next.rows.contains(row) && next.rows.len() >= 2 {
                112
            } else {
                0
            };
            next.decisions.insert(*id, (*row, code));
            if code == 0 {
                next.rows.insert(*row);
            }
            code
        };
        if code != *observed {
            continue;
        }
        order.push(i);
        if search(events, mask | (1 << i), next, order, final_rows) {
            return true;
        }
        order.pop();
    }
    false
}
#[test]
fn independent_serial_search_checks_overlapping_id_conflict_and_last_budget_histories() {
    for seed in 40..50 {
        let mut c = Cluster::new(seed);
        c.provision(4, 2, 5);
        let requests = [(1, 0), (1, 0), (1, 1), (2, 1), (3, 2), (4, 3)];
        let mut history = std::fs::File::create(c.root.join("overlapping-history.jsonl")).unwrap();
        for (i, (id, row)) in requests.iter().enumerate() {
            let context = (1000 + i as u64).to_be_bytes().to_vec();
            writeln!(history, "{}", json!({"invoke":i,"operation":id,"row":row})).unwrap();
            c.nodes
                .get_mut(&1)
                .unwrap()
                .propose(
                    context,
                    &task(TaskAction::Execute(request(*id, &[*row]), None)),
                )
                .unwrap();
        }
        c.pump();
        let mut observed = Vec::new();
        for (i, (id, row)) in requests.iter().enumerate() {
            let context = (1000 + i as u64).to_be_bytes().to_vec();
            let reply = &c.responses[&context];
            let code = reply["code"].as_u64().unwrap();
            observed.push((*id, *row, code));
            writeln!(history, "{}", json!({"complete":i,"code":code})).unwrap();
        }
        c.equal();
        let snapshot = c.nodes[&1].store.image().unwrap().snapshot().unwrap();
        let final_rows = snapshot
            .rows
            .iter()
            .filter_map(|(key, row)| (row.status == Status::Queued).then_some(key.row.0))
            .collect::<BTreeSet<_>>();
        let mut serialization = Vec::new();
        assert!(
            search(
                &observed,
                0,
                Oracle::default(),
                &mut serialization,
                &final_rows
            ),
            "no valid strict serial history seed{seed}"
        );
        assert_eq!(
            snapshot.tasks[&TaskId(1)].transitions,
            final_rows.len() as u64
        );
        assert!(final_rows.len() <= 2);
        std::fs::write(c.root.join("overlapping-checker.json"),serde_json::to_vec_pretty(&json!({"pass":true,"seed":seed,"serialization":serialization,"events":observed,"final_rows":final_rows,"scope":"six overlapping proposals; same/conflicting IDs; lifetime budget2; exhaustive independent serial search"})).unwrap()).unwrap();
    }
}
