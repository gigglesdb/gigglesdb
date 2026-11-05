//! Small serial typed ledger oracle, deliberately separate from the repair model.
//! Debit-funded accounts, one asset per instance, checked integer units, one
//! terminal capture/void, and no implicit external side effects.
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct AccountId(pub u64);
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct TransferId(pub u64);
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Account {
    pub available: u128,
    pub held: u128,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum State {
    Pending,
    Posted,
    Voided,
    Expired,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Transfer {
    pub from: AccountId,
    pub to: AccountId,
    pub amount: u128,
    pub expires: u64,
    pub state: State,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Invalid,
    Missing,
    Exists,
    Conflict,
    Funds,
    Overflow,
    Terminal,
    Expired,
}
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Ledger {
    pub(crate) accounts: BTreeMap<AccountId, Account>,
    pub(crate) transfers: BTreeMap<TransferId, Transfer>,
}
impl Ledger {
    pub fn accounts(&self) -> &BTreeMap<AccountId, Account> {
        &self.accounts
    }
    pub fn transfers(&self) -> &BTreeMap<TransferId, Transfer> {
        &self.transfers
    }
    pub fn open(&mut self, id: AccountId, balance: u128) -> Result<(), Error> {
        if self.accounts.contains_key(&id) {
            return Err(Error::Exists);
        }
        self.accounts.insert(
            id,
            Account {
                available: balance,
                held: 0,
            },
        );
        Ok(())
    }
    pub fn reserve(
        &mut self,
        id: TransferId,
        from: AccountId,
        to: AccountId,
        amount: u128,
        expires: u64,
        now: u64,
    ) -> Result<State, Error> {
        if let Some(t) = self.transfers.get(&id) {
            return if (t.from, t.to, t.amount, t.expires) == (from, to, amount, expires) {
                Ok(t.state)
            } else {
                Err(Error::Conflict)
            };
        }
        if from == to || amount == 0 {
            return Err(Error::Invalid);
        }
        if now >= expires {
            return Err(Error::Expired);
        }
        let src = self.accounts.get(&from).ok_or(Error::Missing)?;
        self.accounts.get(&to).ok_or(Error::Missing)?;
        if src.available < amount {
            return Err(Error::Funds);
        }
        let held = src.held.checked_add(amount).ok_or(Error::Overflow)?;
        let src = self.accounts.get_mut(&from).expect("checked account");
        src.available -= amount;
        src.held = held;
        self.transfers.insert(
            id,
            Transfer {
                from,
                to,
                amount,
                expires,
                state: State::Pending,
            },
        );
        Ok(State::Pending)
    }
    pub fn capture(&mut self, id: TransferId, now: u64) -> Result<State, Error> {
        self.resolve(id, now, State::Posted)
    }
    pub fn void(&mut self, id: TransferId) -> Result<State, Error> {
        self.resolve(id, 0, State::Voided)
    }
    pub fn expire(&mut self, id: TransferId, now: u64) -> Result<State, Error> {
        self.resolve(id, now, State::Expired)
    }
    fn resolve(&mut self, id: TransferId, now: u64, target: State) -> Result<State, Error> {
        let t = self.transfers.get(&id).ok_or(Error::Missing)?.clone();
        if t.state == target {
            return Ok(t.state);
        }
        if t.state != State::Pending {
            return Err(Error::Terminal);
        }
        if target == State::Posted && now >= t.expires {
            return Err(Error::Expired);
        }
        if target == State::Expired && now < t.expires {
            return Err(Error::Invalid);
        }
        let recipient = if target == State::Posted {
            t.to
        } else {
            t.from
        };
        let balance = self.accounts[&recipient]
            .available
            .checked_add(t.amount)
            .ok_or(Error::Overflow)?;
        self.accounts.get_mut(&t.from).expect("source").held -= t.amount;
        self.accounts
            .get_mut(&recipient)
            .expect("recipient")
            .available = balance;
        self.transfers.get_mut(&id).expect("transfer").state = target;
        Ok(target)
    }
}
