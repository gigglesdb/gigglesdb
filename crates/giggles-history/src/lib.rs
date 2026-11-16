//! Independent finite-history strict-serializability checking.
//!
//! A passing result is an existential explanation of the supplied observations,
//! not qualification of the system that produced them. See the package README.
#![forbid(unsafe_code)]
#![doc = include_str!("../README.md")]

pub mod record;
mod search;
mod wire;

pub use search::{Check, Limits, Step, Verdict, check, minimize, validate};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
pub use wire::{parse_jsonl, write_jsonl};

/// Complete initial modeled state; keys and values are signed integers.
pub type State = BTreeMap<i64, i64>;

/// One supported atomic transaction operation, executed in program order.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum Op {
    Read {
        key: i64,
    },
    Scan {
        start: i64,
        end: i64,
    },
    Write {
        key: i64,
        value: i64,
    },
    Delete {
        key: i64,
    },
    /// Requires distinct existing accounts, a positive amount, sufficient funds,
    /// and checked arithmetic. Both account updates are atomic.
    Transfer {
        from: i64,
        to: i64,
        amount: i64,
    },
}

/// Read results, in the order of the transaction's read/scan operations.
/// Unknown and aborted responses may supply a prefix; commits supply all.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "observation", rename_all = "snake_case", deny_unknown_fields)]
pub enum Observation {
    Read { value: Option<i64> },
    Scan { rows: Vec<(i64, i64)> },
}

/// Client response and independent durable evidence use separate fields.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Committed,
    Aborted,
    Indeterminate,
}

/// An attempt to execute/retry one immutable logical transaction.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Attempt {
    pub id: String,
    pub logical_id: String,
    pub invoke: u64,
    pub complete: Option<u64>,
    pub outcome: Outcome,
    /// Unknown absent evidence is represented by None, not guessed abort.
    pub durable: Option<Outcome>,
    /// Time by which the durable decision existed, when independently known.
    pub decision_at: Option<u64>,
    pub ops: Vec<Op>,
    pub observations: Vec<Observation>,
    /// Optional invariant evidence, applied once with logical effects.
    pub receipt: Option<Receipt>,
}

/// A receipt is modeled as part of the atomic logical transaction, not a
/// separate authority or a claim about authentication/approval correctness.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Receipt {
    pub id: String,
    pub budget: String,
    pub cost: u64,
}

/// Invariants checked initially and after every selected committed transaction.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Invariants {
    /// Sum of these keys must remain equal to their initial sum; missing = 0.
    pub conserved_keys: Vec<i64>,
    pub nonnegative_keys: Vec<i64>,
    pub budgets: BTreeMap<String, u64>,
    pub require_receipts: bool,
}

/// Version 1 finite input model. Horizon closes pending attempts for this check;
/// no statement is made about later system behavior.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct History {
    pub version: u32,
    pub horizon: u64,
    pub initial: State,
    pub invariants: Invariants,
    pub attempts: Vec<Attempt>,
}

/// Deterministic synthetic history (SplitMix64, no runtime code dependency).
/// Transactions transfer between two accounts, then record a complete scan.
pub fn generate(seed: u64, count: usize) -> Result<History, String> {
    if count > 128 {
        return Err("generator count exceeds 128 attempts".into());
    }
    let mut rng = seed;
    let mut state = State::from([(0, 1000), (1, 1000)]);
    let mut h = History {
        version: 1,
        horizon: count as u64 * 4 + 1,
        initial: state.clone(),
        invariants: Invariants {
            conserved_keys: vec![0, 1],
            nonnegative_keys: vec![0, 1],
            ..Invariants::default()
        },
        attempts: vec![],
    };
    for i in 0..count {
        rng = rng.wrapping_add(0x9e3779b97f4a7c15);
        let mut z = rng;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
        z ^= z >> 31;
        let from = (z & 1) as i64;
        let to = 1 - from;
        let amount = (z % 7 + 1) as i64;
        // Preserve validity even for long generated runs with one-sided skew.
        let amount = amount.min(state[&from]);
        let ops = if amount > 0 {
            vec![
                Op::Transfer { from, to, amount },
                Op::Scan { start: 0, end: 2 },
            ]
        } else {
            vec![Op::Scan { start: 0, end: 2 }]
        };
        if amount > 0 {
            *state.get_mut(&from).expect("account") -= amount;
            *state.get_mut(&to).expect("account") += amount;
        }
        h.attempts.push(Attempt {
            id: format!("a{i}"),
            logical_id: format!("t{i}"),
            invoke: i as u64 * 4,
            complete: Some(i as u64 * 4 + 2),
            outcome: Outcome::Committed,
            durable: Some(Outcome::Committed),
            decision_at: Some(i as u64 * 4 + 1),
            ops,
            observations: vec![Observation::Scan {
                rows: state.iter().map(|(&k, &v)| (k, v)).collect(),
            }],
            receipt: None,
        });
    }
    Ok(h)
}
