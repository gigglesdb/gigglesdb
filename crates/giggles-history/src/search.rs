use crate::{Attempt, History, Observation, Op, Outcome, Receipt, State};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    mem::size_of,
};

/// Limits are hard exploration/model bounds, not a timeout that can pass.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub max_nodes: u64,
    pub max_memory_bytes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            max_nodes: 1_000_000,
            max_memory_bytes: 64 * 1024 * 1024,
        }
    }
}

/// FAIL means exhaustive rejection in the finite model. Invalid input is Err.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Verdict {
    Pass,
    Fail,
    Inconclusive,
}

/// A concrete serialization point and the attempt that first committed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Step {
    pub logical_id: String,
    pub attempt_id: String,
    pub at: u64,
}

/// Machine-readable verdict, existential witness or exhaustive failure summary.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Check {
    pub verdict: Verdict,
    pub explored: u64,
    pub memory_bound: usize,
    pub reason: String,
    pub serialization: Vec<Step>,
    pub omitted: Vec<String>,
    pub final_state: Option<State>,
    /// Bounded sample of rejected edges, not a standalone unsatisfiability proof.
    pub diagnostics: Vec<String>,
}

struct Group<'a> {
    id: &'a str,
    attempts: Vec<&'a Attempt>,
    mandatory: bool,
    upper: u64,
}

/// Validate the public in-memory schema. JSON byte limits are additionally
/// enforced by parse_jsonl; these limits also protect direct library callers.
pub fn validate(h: &History) -> Result<(), String> {
    if h.version != 1 {
        return Err("unsupported schema version".into());
    }
    if h.attempts.len() > 128 || h.initial.len() > 4096 {
        return Err("model size exceeds schema bounds".into());
    }
    let mut ids = BTreeSet::new();
    let mut groups: BTreeMap<&str, &Attempt> = BTreeMap::new();
    let mut keys: BTreeSet<i64> = h.initial.keys().copied().collect();
    let mut total = 0;
    let mut cells = 0usize;
    for a in &h.attempts {
        if !valid_id(&a.id) || !valid_id(&a.logical_id) || !ids.insert(&a.id) {
            return Err("invalid/duplicate attempt identity".into());
        }
        if a.invoke > h.horizon
            || a.complete.is_some_and(|t| t < a.invoke || t > h.horizon)
            || a.decision_at.is_some_and(|t| t < a.invoke || t > h.horizon)
            || (a.outcome != Outcome::Indeterminate && a.complete.is_none())
            || (a.decision_at.is_some() && a.durable.is_none())
            || a.durable == Some(Outcome::Indeterminate)
        {
            return Err("invalid interval or durable evidence".into());
        }
        if a.ops.len() > 256 {
            return Err("too many operations per attempt".into());
        }
        total += a.ops.len();
        let mut reads = vec![];
        for op in &a.ops {
            match op {
                Op::Read { key } => {
                    keys.insert(*key);
                    reads.push(false);
                }
                Op::Scan { start, end } => {
                    if start >= end {
                        return Err("empty/reversed scan interval".into());
                    }
                    reads.push(true);
                }
                Op::Write { key, .. } | Op::Delete { key } => {
                    keys.insert(*key);
                }
                Op::Transfer { from, to, amount } => {
                    if from == to || *amount <= 0 {
                        return Err("invalid transfer".into());
                    }
                    keys.insert(*from);
                    keys.insert(*to);
                }
            }
        }
        if a.observations.len() > reads.len()
            || (a.outcome == Outcome::Committed && a.observations.len() != reads.len())
        {
            return Err("incomplete/excess observations".into());
        }
        for (obs, scan) in a.observations.iter().zip(reads) {
            match obs {
                Observation::Read { .. } if !scan => {}
                Observation::Scan { rows } if scan => {
                    cells = cells.saturating_add(rows.len());
                    if rows.len() > 4096 || rows.windows(2).any(|r| r[0].0 >= r[1].0) {
                        return Err("scan rows must be unique ordered bounded keys".into());
                    }
                    for (key, _) in rows {
                        keys.insert(*key);
                    }
                }
                _ => return Err("observation kind differs from operation".into()),
            }
        }
        if let Some(r) = &a.receipt
            && (!valid_id(&r.id) || !valid_id(&r.budget))
        {
            return Err("invalid receipt identity".into());
        }
        if let Some(first) = groups.insert(&a.logical_id, a)
            && (first.ops != a.ops || first.receipt != a.receipt)
        {
            return Err("retry identity has different semantic body/receipt".into());
        }
    }
    if total > 8192
        || cells > 16384
        || keys.len() > 4096
        || h.invariants.budgets.len() > 128
        || h.invariants.budgets.keys().any(|id| !valid_id(id))
        || h.invariants.conserved_keys.len() > 4096
        || h.invariants.nonnegative_keys.len() > 4096
        || duplicates(&h.invariants.conserved_keys)
        || duplicates(&h.invariants.nonnegative_keys)
    {
        return Err("model/invariant bounds exceeded or duplicate invariant keys".into());
    }
    Ok(())
}
fn valid_id(s: &str) -> bool {
    !s.is_empty() && s.len() <= 128 && !s.chars().any(char::is_control)
}
fn duplicates(xs: &[i64]) -> bool {
    xs.iter().collect::<BTreeSet<_>>().len() != xs.len()
}

/// Apply independently specified transaction semantics to a private state.
/// Failed/aborted execution never publishes the private state.
fn execute(state: &State, ops: &[Op]) -> Option<(State, Vec<Observation>)> {
    let mut s = state.clone();
    let mut observations = vec![];
    for op in ops {
        match *op {
            Op::Read { key } => observations.push(Observation::Read {
                value: s.get(&key).copied(),
            }),
            Op::Scan { start, end } => observations.push(Observation::Scan {
                rows: s.range(start..end).map(|(&k, &v)| (k, v)).collect(),
            }),
            Op::Write { key, value } => {
                s.insert(key, value);
            }
            Op::Delete { key } => {
                s.remove(&key);
            }
            Op::Transfer { from, to, amount } => {
                let balance = *s.get(&from)?;
                if balance < amount {
                    return None;
                }
                let target = s.get(&to)?.checked_add(amount)?;
                s.insert(from, balance.checked_sub(amount)?);
                s.insert(to, target);
            }
        }
    }
    Some((s, observations))
}

fn invariants(h: &History, s: &State) -> bool {
    let sum = |state: &State| {
        h.invariants
            .conserved_keys
            .iter()
            .map(|k| i128::from(*state.get(k).unwrap_or(&0)))
            .sum::<i128>()
    };
    sum(s) == sum(&h.initial)
        && h.invariants
            .nonnegative_keys
            .iter()
            .all(|k| s.get(k).copied().unwrap_or(0) >= 0)
}

#[derive(PartialEq, Eq, Hash)]
struct VisitKey {
    mask: u128,
    state: State,
    time: u64,
    receipts: BTreeSet<String>,
    spent: BTreeMap<String, u64>,
}

impl VisitKey {
    fn estimated_bytes(&self) -> usize {
        // Allow for unused hash buckets and transient storage during growth.
        let buckets = 5 * (size_of::<Self>() + 1) + 64;
        // The trees allocate nodes in blocks, including for a single entry.
        let tree_bytes = |len: usize| if len == 0 { 0 } else { 512 + 128 * len };
        buckets
            + tree_bytes(self.state.len())
            + tree_bytes(self.receipts.len())
            + tree_bytes(self.spent.len())
            + self.receipts.iter().map(String::capacity).sum::<usize>()
            + self.spent.keys().map(String::capacity).sum::<usize>()
    }
}

struct Search<'a> {
    h: &'a History,
    groups: Vec<Group<'a>>,
    limits: Limits,
    result: Check,
    limited: bool,
    failures: HashSet<VisitKey>,
    memo_bytes: usize,
    memo_budget: usize,
    memo_full: bool,
}
impl Search<'_> {
    fn diagnostic(&mut self, id: &str, why: &str) {
        if self.result.diagnostics.len() < 8 {
            self.result.diagnostics.push(format!("{id}: {why}"));
        }
    }
    fn visit(&mut self, key: VisitKey, path: &mut Vec<Step>) -> bool {
        if self.result.explored >= self.limits.max_nodes {
            self.limited = true;
            return false;
        }
        self.result.explored += 1;
        if self.failures.contains(&key) {
            return false;
        }

        let passed = self.visit_uncached(&key, path);
        // A node-limit cutoff proves nothing about the unexplored suffix.
        if !passed && !self.limited && !self.memo_full {
            let bytes = self.memo_bytes.saturating_add(key.estimated_bytes());
            if bytes <= self.memo_budget {
                self.failures.insert(key);
                self.memo_bytes = bytes;
            } else {
                self.memo_full = true;
            }
        }
        passed
    }

    fn visit_uncached(&mut self, key: &VisitKey, path: &mut Vec<Step>) -> bool {
        let VisitKey {
            mask,
            state: s,
            time,
            receipts,
            spent,
        } = key;
        let (mask, time) = (*mask, *time);
        if self
            .groups
            .iter()
            .enumerate()
            .any(|(i, g)| g.mandatory && mask & (1u128 << i) == 0 && g.upper < time)
        {
            return false;
        }
        if self
            .groups
            .iter()
            .enumerate()
            .all(|(i, g)| !g.mandatory || mask & (1u128 << i) != 0)
        {
            self.result.serialization = path.clone();
            self.result.final_state = Some(s.clone());
            self.result.omitted = self
                .groups
                .iter()
                .enumerate()
                .filter(|(i, _)| mask & (1u128 << i) == 0)
                .map(|(_, g)| g.id.to_owned())
                .collect();
            return true;
        }
        for i in 0..self.groups.len() {
            if mask & (1u128 << i) != 0 {
                continue;
            }
            let id = self.groups[i].id.to_owned();
            let attempts = self.groups[i].attempts.clone();
            if attempts.iter().any(|a| {
                a.durable == Some(Outcome::Aborted)
                    || (a.outcome == Outcome::Aborted && a.durable == Some(Outcome::Committed))
            }) {
                self.diagnostic(
                    &id,
                    "durable abort or contradictory evidence excludes logical commit",
                );
                continue;
            }
            let mut upper = self.h.horizon;
            for a in &attempts {
                if a.outcome == Outcome::Committed {
                    upper = upper.min(a.complete.expect("validated"));
                }
                if a.durable == Some(Outcome::Committed) {
                    upper = upper.min(a.decision_at.unwrap_or(self.h.horizon));
                }
            }
            for origin in &attempts {
                if origin.outcome == Outcome::Aborted || origin.durable == Some(Outcome::Aborted) {
                    continue;
                }
                // An earlier successful reply cannot originate in a later retry.
                let at = time.max(origin.invoke);
                if at > upper {
                    self.diagnostic(&id, "no real-time feasible serialization point");
                    continue;
                }
                let Some((next, obs)) = execute(s, &origin.ops) else {
                    self.diagnostic(&id, "transaction precondition/arithmetic failed");
                    continue;
                };
                if !obs.starts_with(&origin.observations)
                    || attempts.iter().any(|a| {
                        (a.outcome == Outcome::Committed && a.observations != obs)
                            || (a.durable == Some(Outcome::Committed)
                                && !obs.starts_with(&a.observations))
                    })
                {
                    self.diagnostic(&id, "point/complete-range observations disagree");
                    continue;
                }
                if !invariants(self.h, &next) {
                    self.diagnostic(&id, "state invariant violated");
                    continue;
                }
                let mut next_receipts = receipts.clone();
                let mut next_spent = spent.clone();
                if !self.receipt(&origin.receipt, &mut next_receipts, &mut next_spent) {
                    self.diagnostic(&id, "receipt uniqueness/budget invariant violated");
                    continue;
                }
                path.push(Step {
                    logical_id: id.clone(),
                    attempt_id: origin.id.clone(),
                    at,
                });
                if self.visit(
                    VisitKey {
                        mask: mask | (1u128 << i),
                        state: next,
                        time: at,
                        receipts: next_receipts,
                        spent: next_spent,
                    },
                    path,
                ) {
                    return true;
                }
                path.pop();
                if self.limited {
                    return false;
                }
            }
        }
        false
    }
    fn receipt(
        &self,
        r: &Option<Receipt>,
        ids: &mut BTreeSet<String>,
        spent: &mut BTreeMap<String, u64>,
    ) -> bool {
        let Some(r) = r else {
            return !self.h.invariants.require_receipts;
        };
        if !ids.insert(r.id.clone()) {
            return false;
        }
        let Some(limit) = self.h.invariants.budgets.get(&r.budget) else {
            return false;
        };
        let Some(total) = spent
            .get(&r.budget)
            .copied()
            .unwrap_or(0)
            .checked_add(r.cost)
        else {
            return false;
        };
        if total > *limit {
            return false;
        }
        spent.insert(r.budget.clone(), total);
        true
    }
}

/// Exhaustively find a legal finite serialization or report explicit limits.
pub fn check(h: &History, limits: Limits) -> Result<Check, String> {
    validate(h)?;
    let mut groups: BTreeMap<&str, Vec<&Attempt>> = BTreeMap::new();
    for a in &h.attempts {
        groups.entry(&a.logical_id).or_default().push(a);
    }
    let groups: Vec<_> = groups
        .into_iter()
        .map(|(id, attempts)| {
            let upper = attempts.iter().fold(h.horizon, |bound, a| {
                let bound = if a.outcome == Outcome::Committed {
                    bound.min(a.complete.expect("validated"))
                } else {
                    bound
                };
                if a.durable == Some(Outcome::Committed) {
                    bound.min(a.decision_at.unwrap_or(h.horizon))
                } else {
                    bound
                }
            });
            Group {
                id,
                mandatory: attempts.iter().any(|a| {
                    a.outcome == Outcome::Committed || a.durable == Some(Outcome::Committed)
                }),
                attempts,
                upper,
            }
        })
        .collect();
    // Conservative accounted modeled allocation, not an OS RSS cap. Includes
    // all possible distinct keys, recursive state copies, receipt sets, output,
    // cloned candidate vectors and input/serde representation headroom.
    // The failure memo gets only the budget this bound leaves (memo_budget).
    let wire = serde_json::to_vec(h).map_err(|e| e.to_string())?.len();
    let key_count = h.initial.len() + h.attempts.iter().map(|a| a.ops.len() * 2).sum::<usize>();
    // Unknown/durable origins may omit observations on the wire, but execution
    // still materializes full scan results. Recursive frames retain these.
    let scans = h
        .attempts
        .iter()
        .flat_map(|a| &a.ops)
        .filter(|op| matches!(op, Op::Scan { .. }))
        .count();
    let reads = h
        .attempts
        .iter()
        .flat_map(|a| &a.ops)
        .filter(|op| matches!(op, Op::Read { .. }))
        .count();
    let derived = scans
        .saturating_mul(key_count.min(4096))
        .saturating_mul(32)
        .saturating_add(reads.saturating_mul(64));
    let memory_bound = wire
        .saturating_mul(16)
        .saturating_add(derived)
        .saturating_add(
            (groups.len() + 1)
                .saturating_mul(key_count.min(4096) * 128 + groups.len() * 1024 + 8192),
        );
    let mut search = Search {
        h,
        groups,
        limits,
        limited: false,
        failures: HashSet::new(),
        memo_bytes: 0,
        memo_budget: limits.max_memory_bytes.saturating_sub(memory_bound),
        memo_full: false,
        result: Check {
            verdict: Verdict::Fail,
            explored: 0,
            memory_bound,
            reason: String::new(),
            serialization: vec![],
            omitted: vec![],
            final_state: None,
            diagnostics: vec![],
        },
    };
    if memory_bound > limits.max_memory_bytes {
        search.result.verdict = Verdict::Inconclusive;
        search.result.reason = "modeled memory bound exceeds limit".into();
        return Ok(search.result);
    }
    let passed = invariants(h, &h.initial)
        && search.visit(
            VisitKey {
                mask: 0,
                state: h.initial.clone(),
                time: 0,
                receipts: BTreeSet::new(),
                spent: BTreeMap::new(),
            },
            &mut vec![],
        );
    search.result.verdict = if passed {
        Verdict::Pass
    } else if search.limited {
        Verdict::Inconclusive
    } else {
        Verdict::Fail
    };
    search.result.reason = match search.result.verdict {
        Verdict::Pass => "a bounded strict serialization exists",
        Verdict::Fail => "all bounded serial explanations rejected",
        Verdict::Inconclusive => "search node limit reached",
    }
    .into();
    Ok(search.result)
}

/// Greedy logical-transaction deletion reduction. Only validated, exhaustive
/// FAIL candidates replace the history. Initial state/invariants never change.
/// Returns attempts used; reaching the reduction budget is not minimality proof.
pub fn minimize(
    h: &History,
    limits: Limits,
    max_checks: usize,
) -> Result<(History, usize), String> {
    if check(h, limits)?.verdict != Verdict::Fail {
        return Err("minimization requires exhaustive FAIL".into());
    }
    let mut reduced = h.clone();
    let mut checks = 0;
    let ids: BTreeSet<_> = h.attempts.iter().map(|a| a.logical_id.clone()).collect();
    for id in ids {
        if checks >= max_checks {
            break;
        }
        let mut candidate = reduced.clone();
        candidate.attempts.retain(|a| a.logical_id != id);
        checks += 1;
        if check(&candidate, limits)?.verdict == Verdict::Fail {
            reduced = candidate;
        }
    }
    Ok((reduced, checks))
}
