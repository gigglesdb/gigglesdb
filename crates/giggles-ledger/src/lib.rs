//! Bounded trusted local ledger over GigglesDB's durable SQL transactions.
//!
//! The module hides SQL authority, but an independent trusted `Admin` or file owner
//! can bypass it. Caller-certified monotonic time is required for reserve decisions.
//! See the crate README for finite caps, recovery and audit limitations.
//!
//! ```no_run
//! use giggles_ledger::*;
//! # fn example() -> Result<()> {
//! let ledger = Ledger::create("fresh-ledger.db", Definition {
//!     ledger: "business".into(), asset: "USD".into(), scale: 2,
//! })?;
//! ledger.create_account(AccountDefinition {
//!     id: "clearing".into(), rule: BalanceRule::Unrestricted, max_credit: None,
//! })?;
//! assert_eq!(ledger.reconcile()?.receipts, 0);
//! # Ok(()) }
//! ```
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
mod store;
pub use store::Ledger;

pub const MAX_ACCOUNTS: usize = 64;
pub const MAX_RECEIPTS: usize = 128;
pub const MAX_BATCH: usize = 16;
pub const MAX_PAYLOAD: usize = 60_000;
pub const MAX_HOLD: u64 = 86_400_000;
pub const MAX_RETRIES: usize = 8;
/// Conservative format-v1 growth budget for one pending hold's terminal decision.
pub const TERMINAL_HEADROOM: usize = 2048;

/// Explicit trusted failure injection for local recovery tests. Never enabled implicitly.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum Fault {
    #[default]
    None,
    BeforeCommit,
    AfterCommit,
    ErrorBeforeCommit,
    ErrorAfterCommit,
}

/// Exact unsigned units; serialized as exactly sixteen big-endian bytes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct Amount(pub u128);
impl Serialize for Amount {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        self.0.to_be_bytes().serialize(s)
    }
}
impl<'de> Deserialize<'de> for Amount {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        Ok(Self(u128::from_be_bytes(<[u8; 16]>::deserialize(d)?)))
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Definition {
    pub ledger: String,
    pub asset: String,
    pub scale: u8,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BalanceRule {
    Unrestricted,
    DebitsMayNotExceedCredits,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccountDefinition {
    pub id: String,
    pub rule: BalanceRule,
    pub max_credit: Option<Amount>,
}
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Totals {
    pub posted_debits: Amount,
    pub posted_credits: Amount,
    pub pending_debits: Amount,
    pub pending_credits: Amount,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Account {
    pub definition: AccountDefinition,
    pub totals: Totals,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Transfer {
    pub ledger: String,
    pub asset: String,
    pub debit: String,
    pub credit: String,
    pub amount: Amount,
}
/// Trusted certification, not an observation of the local system wall clock.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Clock {
    pub lower: u64,
    pub upper: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Operation {
    Post(Transfer),
    Reserve { transfer: Transfer, deadline: u64 },
    Capture { reserve: String },
    Void { reserve: String },
    Expire { reserve: String },
    Reverse { original: String },
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Intent {
    pub id: String,
    pub clock: Clock,
    pub operation: Operation,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransferState {
    Posted,
    Pending { deadline: u64 },
    Captured { by: String },
    Voided { by: String },
    Expired { by: String },
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Position {
    pub transfer: Transfer,
    pub state: TransferState,
    pub reversal: Option<String>,
}
/// An immutable recorded decision. Query `position` for a reserve's current state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Receipt {
    pub ordinal: u64,
    pub intent: Intent,
    pub digest: [u8; 32],
    pub result: Position,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Reconciliation {
    pub accounts: usize,
    pub receipts: usize,
    pub pending: usize,
    pub journal_digest: [u8; 32],
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Error {
    Invalid(String),
    Missing(String),
    DuplicateMismatch,
    InsufficientFunds,
    CreditLimit,
    Overflow,
    Terminal,
    Expired,
    TimeUncertain,
    ClockRegression,
    AlreadyReversed,
    Limit,
    Corrupt(String),
    Core(String),
    ConflictExhausted,
    /// COMMIT may have persisted. Reopen and query these original IDs.
    Unknown {
        ids: Vec<String>,
    },
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct State {
    format: u32,
    definition: Definition,
    accounts: BTreeMap<String, Account>,
    receipts: Vec<Receipt>,
    positions: BTreeMap<String, Position>,
    last_lower: u64,
}
fn identifier(s: &str) -> Result<()> {
    if s.is_empty()
        || s.len() > 64
        || !s
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        return Err(Error::Invalid("identifier".into()));
    }
    Ok(())
}
fn definition(d: &Definition) -> Result<()> {
    identifier(&d.ledger)?;
    identifier(&d.asset)?;
    if d.scale > 38 {
        return Err(Error::Invalid("scale".into()));
    }
    Ok(())
}
fn digest(i: &Intent) -> [u8; 32] {
    // The explicit enum representation and struct field order are format-v1.
    let bytes = serde_json::to_vec(&(&i.id, &i.operation)).expect("typed payload serializes");
    Sha256::digest(bytes).into()
}
fn add(a: &mut Amount, b: Amount) -> Result<()> {
    a.0 = a.0.checked_add(b.0).ok_or(Error::Overflow)?;
    Ok(())
}
fn sub(a: &mut Amount, b: Amount) -> Result<()> {
    a.0 =
        a.0.checked_sub(b.0)
            .ok_or_else(|| Error::Corrupt("pending underflow".into()))?;
    Ok(())
}
impl State {
    fn new(d: Definition) -> Result<Self> {
        definition(&d)?;
        Ok(Self {
            format: 1,
            definition: d,
            accounts: BTreeMap::new(),
            receipts: vec![],
            positions: BTreeMap::new(),
            last_lower: 0,
        })
    }
    fn create_account(&mut self, d: AccountDefinition) -> Result<()> {
        identifier(&d.id)?;
        if self.accounts.contains_key(&d.id) {
            return Err(Error::Invalid("account exists".into()));
        }
        if self.accounts.len() >= MAX_ACCOUNTS {
            return Err(Error::Limit);
        }
        self.accounts.insert(
            d.id.clone(),
            Account {
                definition: d,
                totals: Totals::default(),
            },
        );
        Ok(())
    }
    fn validate_transfer(&self, t: &Transfer) -> Result<()> {
        if t.ledger != self.definition.ledger || t.asset != self.definition.asset {
            return Err(Error::Invalid("ledger or asset mismatch".into()));
        }
        if t.amount.0 == 0 || t.debit == t.credit {
            return Err(Error::Invalid("zero or self transfer".into()));
        }
        for id in [&t.debit, &t.credit] {
            if !self.accounts.contains_key(id) {
                return Err(Error::Missing(id.clone()));
            }
        }
        Ok(())
    }
    fn rules(&self) -> Result<()> {
        for a in self.accounts.values() {
            let t = &a.totals;
            if a.definition.rule == BalanceRule::DebitsMayNotExceedCredits
                && t.posted_debits
                    .0
                    .checked_add(t.pending_debits.0)
                    .ok_or(Error::Overflow)?
                    > t.posted_credits.0
            {
                return Err(Error::InsufficientFunds);
            }
            if let Some(max) = a.definition.max_credit {
                // Incoming holds consume credit capacity; outgoing holds cannot create capacity.
                let outstanding = t.posted_credits.0.saturating_sub(t.posted_debits.0);
                if outstanding
                    .checked_add(t.pending_credits.0)
                    .ok_or(Error::Overflow)?
                    > max.0
                {
                    return Err(Error::CreditLimit);
                }
            }
        }
        Ok(())
    }
    fn pair(&mut self, t: &Transfer, pending: bool, release: bool) -> Result<()> {
        self.validate_transfer(t)?;
        let debit = &mut self.accounts.get_mut(&t.debit).expect("validated").totals;
        let d = if pending {
            &mut debit.pending_debits
        } else {
            &mut debit.posted_debits
        };
        if release {
            sub(d, t.amount)?;
        } else {
            add(d, t.amount)?;
        }
        let credit = &mut self.accounts.get_mut(&t.credit).expect("validated").totals;
        let c = if pending {
            &mut credit.pending_credits
        } else {
            &mut credit.posted_credits
        };
        if release {
            sub(c, t.amount)?;
        } else {
            add(c, t.amount)?;
        }
        Ok(())
    }
    fn apply(&mut self, i: Intent) -> Result<Receipt> {
        identifier(&i.id)?;
        let hash = digest(&i);
        if let Some(r) = self.receipts.iter().find(|r| r.intent.id == i.id) {
            return if r.digest == hash && r.intent.operation == i.operation {
                Ok(r.clone())
            } else {
                Err(Error::DuplicateMismatch)
            };
        }
        if self.receipts.len() >= MAX_RECEIPTS {
            return Err(Error::Limit);
        }
        if i.clock.lower > i.clock.upper {
            return Err(Error::TimeUncertain);
        }
        if i.clock.lower < self.last_lower {
            return Err(Error::ClockRegression);
        }
        let result = match &i.operation {
            Operation::Post(t) => {
                self.pair(t, false, false)?;
                Position {
                    transfer: t.clone(),
                    state: TransferState::Posted,
                    reversal: None,
                }
            }
            Operation::Reserve {
                transfer: t,
                deadline,
            } => {
                if *deadline <= i.clock.upper {
                    return Err(Error::Expired);
                }
                if deadline
                    .checked_sub(i.clock.lower)
                    .ok_or(Error::TimeUncertain)?
                    > MAX_HOLD
                {
                    return Err(Error::Limit);
                }
                self.pair(t, true, false)?;
                Position {
                    transfer: t.clone(),
                    state: TransferState::Pending {
                        deadline: *deadline,
                    },
                    reversal: None,
                }
            }
            Operation::Capture { reserve }
            | Operation::Void { reserve }
            | Operation::Expire { reserve } => {
                let mut p = self
                    .positions
                    .get(reserve)
                    .cloned()
                    .ok_or_else(|| Error::Missing(reserve.clone()))?;
                let TransferState::Pending { deadline } = p.state else {
                    return Err(Error::Terminal);
                };
                self.pair(&p.transfer, true, true)?;
                p.state = match &i.operation {
                    Operation::Capture { .. } => {
                        if i.clock.lower >= deadline {
                            return Err(Error::Expired);
                        }
                        if i.clock.upper >= deadline {
                            return Err(Error::TimeUncertain);
                        }
                        self.pair(&p.transfer, false, false)?;
                        TransferState::Captured { by: i.id.clone() }
                    }
                    Operation::Expire { .. } => {
                        if i.clock.lower < deadline {
                            return Err(Error::TimeUncertain);
                        }
                        TransferState::Expired { by: i.id.clone() }
                    }
                    _ => TransferState::Voided { by: i.id.clone() },
                };
                self.positions.insert(reserve.clone(), p.clone());
                p
            }
            Operation::Reverse { original } => {
                let mut p = self
                    .positions
                    .get(original)
                    .cloned()
                    .ok_or_else(|| Error::Missing(original.clone()))?;
                let original_receipt = self
                    .receipts
                    .iter()
                    .find(|r| r.intent.id == *original)
                    .expect("position receipt");
                if !matches!(
                    original_receipt.intent.operation,
                    Operation::Post(_) | Operation::Reserve { .. }
                ) || !matches!(
                    p.state,
                    TransferState::Posted | TransferState::Captured { .. }
                ) {
                    return Err(Error::Invalid(
                        "original must be direct or captured reserve".into(),
                    ));
                }
                if p.reversal.is_some() {
                    return Err(Error::AlreadyReversed);
                }
                p.reversal = Some(i.id.clone());
                self.positions.insert(original.clone(), p.clone());
                let t = Transfer {
                    debit: p.transfer.credit,
                    credit: p.transfer.debit,
                    ..p.transfer
                };
                self.pair(&t, false, false)?;
                Position {
                    transfer: t,
                    state: TransferState::Posted,
                    reversal: None,
                }
            }
        };
        self.rules()?;
        self.last_lower = i.clock.lower;
        self.positions.insert(i.id.clone(), result.clone());
        let r = Receipt {
            ordinal: self.receipts.len() as u64 + 1,
            intent: i,
            digest: hash,
            result,
        };
        self.receipts.push(r.clone());
        Ok(r)
    }
    fn audit(&self) -> Result<Reconciliation> {
        if self.format != 1
            || self.accounts.len() > MAX_ACCOUNTS
            || self.receipts.len() > MAX_RECEIPTS
        {
            return Err(Error::Corrupt("format or cap".into()));
        }
        let check = || -> Result<Self> {
            let mut replay = Self::new(self.definition.clone())?;
            for (key, a) in &self.accounts {
                if key != &a.definition.id {
                    return Err(Error::Corrupt("account key".into()));
                }
                replay.create_account(a.definition.clone())?;
            }
            for r in &self.receipts {
                if replay.apply(r.intent.clone())? != *r {
                    return Err(Error::Corrupt("receipt".into()));
                }
            }
            Ok(replay)
        };
        let replay = check().map_err(|e| Error::Corrupt(format!("replay: {e}")))?;
        if replay != *self {
            return Err(Error::Corrupt(
                "reconstructed totals or state mismatch".into(),
            ));
        }
        Ok(Reconciliation {
            accounts: self.accounts.len(),
            receipts: self.receipts.len(),
            pending: self
                .positions
                .values()
                .filter(|p| matches!(p.state, TransferState::Pending { .. }))
                .count(),
            journal_digest: Sha256::digest(
                serde_json::to_vec(&self.receipts).expect("typed receipts"),
            )
            .into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exact_amount_codec_boundary() {
        for n in [0, i64::MAX as u128 + 1, u128::MAX] {
            let bytes = serde_json::to_vec(&Amount(n)).unwrap();
            assert_eq!(serde_json::from_slice::<Amount>(&bytes).unwrap(), Amount(n));
        }
        for s in [
            "[]",
            "[1]",
            "-1",
            "1.5",
            "340282366920938463463374607431768211455",
        ] {
            assert!(serde_json::from_str::<Amount>(s).is_err());
        }
    }
    #[test]
    fn invalid_definition() {
        for name in ["", "bad space", "sql'", "☃"] {
            assert!(
                State::new(Definition {
                    ledger: name.into(),
                    asset: "USD".into(),
                    scale: 2
                })
                .is_err()
            );
        }
        assert!(
            State::new(Definition {
                ledger: "L".into(),
                asset: "USD".into(),
                scale: 39
            })
            .is_err()
        );
    }
    #[test]
    fn widest_terminal_encoding_fits_reserved_headroom() {
        for kind in 0..3 {
            let mut s = State::new(Definition {
                ledger: "L".repeat(64),
                asset: "A".repeat(64),
                scale: 38,
            })
            .unwrap();
            for id in ["D".repeat(64), "C".repeat(64)] {
                s.create_account(AccountDefinition {
                    id,
                    rule: BalanceRule::Unrestricted,
                    max_credit: None,
                })
                .unwrap();
            }
            let hold = "H".repeat(64);
            let transfer = Transfer {
                ledger: s.definition.ledger.clone(),
                asset: s.definition.asset.clone(),
                debit: "D".repeat(64),
                credit: "C".repeat(64),
                amount: Amount(u128::MAX),
            };
            s.apply(Intent {
                id: hold.clone(),
                clock: Clock {
                    lower: u64::MAX - 100,
                    upper: u64::MAX - 100,
                },
                operation: Operation::Reserve {
                    transfer,
                    deadline: u64::MAX,
                },
            })
            .unwrap();
            let before = serde_json::to_vec(&s).unwrap().len();
            let op = match kind {
                0 => Operation::Capture { reserve: hold },
                1 => Operation::Void { reserve: hold },
                _ => Operation::Expire { reserve: hold },
            };
            let now = if kind == 2 { u64::MAX } else { u64::MAX - 1 };
            s.apply(Intent {
                id: "T".repeat(64),
                clock: Clock {
                    lower: now,
                    upper: now,
                },
                operation: op,
            })
            .unwrap();
            let after = serde_json::to_vec(&s).unwrap().len();
            assert!(
                after - before <= TERMINAL_HEADROOM,
                "terminal growth {}",
                after - before
            );
            s.audit().unwrap();
        }
    }
}
