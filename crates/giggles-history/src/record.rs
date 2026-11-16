//! Concurrent recording and deterministic transfer qualification workloads.
use crate::{Attempt, History, Invariants, Observation, Op, Outcome, State};
use std::sync::{
    Mutex,
    atomic::{AtomicU64, Ordering},
};

static CLOCK: AtomicU64 = AtomicU64::new(0);

/// A recorder shares the process clock with every other recorder.
#[derive(Default)]
pub struct Recorder {
    attempts: Mutex<Vec<Attempt>>,
    counter: AtomicU64,
}

pub struct Pending {
    pub id: String,
    pub logical_id: String,
    pub invoke: u64,
    pub ops: Vec<Op>,
}

impl Recorder {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn now(&self) -> u64 {
        CLOCK.fetch_add(1, Ordering::SeqCst) + 1
    }
    pub fn invoke(&self, logical_id: &str, ops: Vec<Op>) -> Pending {
        Pending {
            id: format!("a{}", self.counter.fetch_add(1, Ordering::SeqCst)),
            logical_id: logical_id.into(),
            invoke: self.now(),
            ops,
        }
    }
    /// Unknown effects remain possible through the window horizon.
    pub fn complete(&self, p: Pending, outcome: Outcome, observations: Vec<Observation>) {
        let complete = (outcome != Outcome::Indeterminate).then(|| self.now());
        self.attempts.lock().unwrap().push(Attempt {
            id: p.id,
            logical_id: p.logical_id,
            invoke: p.invoke,
            complete,
            outcome,
            durable: None,
            decision_at: None,
            ops: p.ops,
            observations,
            receipt: None,
        });
    }
    /// Attach independent evidence without inventing a decision timestamp.
    pub fn resolve(&self, attempt_id: &str, durable: Outcome) -> Result<(), String> {
        if durable == Outcome::Indeterminate {
            return Err("durable evidence must be decisive".into());
        }
        let mut attempts = self.attempts.lock().unwrap();
        let a = attempts
            .iter_mut()
            .find(|a| a.id == attempt_id)
            .ok_or("unknown attempt")?;
        if a.durable.is_some() {
            return Err("attempt already resolved".into());
        }
        a.durable = Some(durable);
        Ok(())
    }
    pub fn len(&self) -> usize {
        self.attempts.lock().unwrap().len()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn take(&self) -> Vec<Attempt> {
        let mut attempts = std::mem::take(&mut *self.attempts.lock().unwrap());
        attempts.sort_by_key(|a| a.invoke);
        attempts
    }
}

/// Close a bounded window, rejecting invalid identities, operations and evidence.
pub fn window(initial: State, accounts: &[i64], attempts: Vec<Attempt>) -> Result<History, String> {
    let horizon = attempts
        .iter()
        .map(|a| a.invoke.max(a.complete.unwrap_or(0)))
        .max()
        .unwrap_or(0)
        .checked_add(1)
        .ok_or("window horizon overflow")?;
    let h = History {
        version: 1,
        horizon,
        initial,
        attempts,
        invariants: Invariants {
            conserved_keys: accounts.to_vec(),
            nonnegative_keys: accounts.to_vec(),
            ..Invariants::default()
        },
    };
    crate::validate(&h)?;
    Ok(h)
}

pub struct TransferWorkload {
    seed: u64,
    accounts: Vec<i64>,
    transfer_scans: bool,
}
impl TransferWorkload {
    /// Accounts must contain at least two distinct keys and a representable scan end.
    pub fn new(seed: u64, mut accounts: Vec<i64>) -> Self {
        accounts.sort_unstable();
        accounts.dedup();
        assert!(
            accounts.len() >= 2,
            "at least two distinct accounts required"
        );
        assert!(
            accounts.last() != Some(&i64::MAX),
            "scan end would overflow"
        );
        Self {
            seed,
            accounts,
            transfer_scans: true,
        }
    }
    /// Transfers read back only the two rows they write (`[Transfer, Read{from},
    /// Read{to}]`) instead of scanning; full scans run as one-statement read-only
    /// transactions. With read set == write set, such histories stay serializable
    /// under read committed with write-write conflict detection, and the
    /// post-images keep the checker's search small enough to reject controls.
    pub fn read_committed_safe(mut self) -> Self {
        self.transfer_scans = false;
        self
    }
    fn random(&mut self) -> u64 {
        self.seed = self.seed.wrapping_add(0x9e3779b97f4a7c15);
        let mut z = self.seed;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
        z ^ (z >> 31)
    }
    pub fn next_ops(&mut self) -> Vec<Op> {
        if self.random().is_multiple_of(4) {
            return vec![self.scan()];
        }
        let n = self.accounts.len();
        let from = self.random() as usize % n;
        let to = (from + 1 + self.random() as usize % (n - 1)) % n;
        let amount = (self.random() % 5 + 1) as i64;
        let mut ops = vec![Op::Transfer {
            from: self.accounts[from],
            to: self.accounts[to],
            amount,
        }];
        if self.transfer_scans {
            ops.push(self.scan());
        } else {
            ops.push(Op::Read {
                key: self.accounts[from],
            });
            ops.push(Op::Read {
                key: self.accounts[to],
            });
        }
        ops
    }
    pub fn scan(&self) -> Op {
        Op::Scan {
            start: self.accounts[0],
            end: self.accounts[self.accounts.len() - 1] + 1,
        }
    }
}

/// Pure negative-control mutations; callers independently check the verdict.
pub mod controls {
    use super::*;
    fn transfer(a: &Attempt) -> bool {
        a.outcome == Outcome::Committed && a.ops.iter().any(|op| matches!(op, Op::Transfer { .. }))
    }
    pub fn stale_final_read(h: &History) -> Option<History> {
        crate::validate(h).ok()?;
        let (i, last) = h
            .attempts
            .iter()
            .enumerate()
            .max_by_key(|(_, a)| a.invoke)?;
        let [Op::Scan { start, end }] = last.ops.as_slice() else {
            return None;
        };
        if last.outcome != Outcome::Committed
            || h.initial.keys().any(|k| !(*start..*end).contains(k))
        {
            return None;
        }
        let [Observation::Scan { rows: final_rows }] = last.observations.as_slice() else {
            return None;
        };
        let rows = h
            .attempts
            .iter()
            .filter(|a| a.invoke < last.invoke && a.outcome == Outcome::Committed)
            .filter_map(|a| {
                let mut observations = a.observations.iter();
                for op in &a.ops {
                    if matches!(op, Op::Read { .. } | Op::Scan { .. }) {
                        let obs = observations.next()?;
                        if op == &last.ops[0]
                            && let Observation::Scan { rows } = obs
                            && rows != final_rows
                        {
                            return Some((a.invoke, rows));
                        }
                    }
                }
                None
            })
            .max_by_key(|(invoke, _)| *invoke)?
            .1;
        let mut mutated = h.clone();
        mutated.attempts[i].observations = vec![Observation::Scan { rows: rows.clone() }];
        Some(mutated)
    }
    /// Stale read: the earliest committed read-only full Scan that real time
    /// forces after some committed Transfer (invoked after it completed) and
    /// that observed a state other than `initial` now observes `initial`.
    /// Unlike `stale_final_read`, the checker can reject this as soon as that
    /// read is due, so it stays decidable on large windows.
    pub fn stale_read(h: &History) -> Option<History> {
        crate::validate(h).ok()?;
        let initial: Vec<(i64, i64)> = h.initial.iter().map(|(&k, &v)| (k, v)).collect();
        let first_transfer_done = h
            .attempts
            .iter()
            .filter(|a| transfer(a))
            .filter_map(|a| a.complete)
            .min()?;
        let i = h
            .attempts
            .iter()
            .enumerate()
            .filter(|(_, a)| {
                a.outcome == Outcome::Committed
                    && a.invoke > first_transfer_done
                    && matches!(a.ops.as_slice(), [Op::Scan { .. }])
                    && matches!(a.observations.as_slice(),
                        [Observation::Scan { rows }] if *rows != initial)
            })
            .min_by_key(|(_, a)| a.invoke)?
            .0;
        let mut mutated = h.clone();
        mutated.attempts[i].observations = vec![Observation::Scan { rows: initial }];
        Some(mutated)
    }
    pub fn aborted_but_visible(h: &History) -> Option<History> {
        crate::validate(h).ok()?;
        let i = h.attempts.iter().position(transfer)?;
        let mut mutated = h.clone();
        mutated.attempts[i].outcome = Outcome::Aborted;
        mutated.attempts[i].durable = Some(Outcome::Aborted);
        Some(mutated)
    }
    pub fn duplicate_apply(h: &History) -> Option<History> {
        crate::validate(h).ok()?;
        if h.attempts.len() >= 128 {
            return None;
        }
        let mut a = h.attempts.iter().find(|a| transfer(a))?.clone();
        let fresh = |logical: bool| {
            (0..=128).map(|i| format!("duplicate{i}")).find(|id| {
                !h.attempts.iter().any(|a| {
                    if logical {
                        &a.logical_id == id
                    } else {
                        &a.id == id
                    }
                })
            })
        };
        a.id = fresh(false)?;
        a.logical_id = fresh(true)?;
        let mut mutated = h.clone();
        mutated.attempts.push(a);
        Some(mutated)
    }
}
