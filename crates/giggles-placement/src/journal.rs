use crate::{Action, Code, Error, Owner, Plan, Result};
use redb::{Database, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};
use serde::{Deserialize, Serialize};
use std::path::Path;
const STATE: TableDefinition<&str, &[u8]> = TableDefinition::new("placement-v1");
const EVENTS: TableDefinition<u64, &[u8]> = TableDefinition::new("events-v1");
const MAX_EVENTS: u64 = 100_000;
fn err(e: impl ToString) -> Error {
    Error::new(Code::Journal, e)
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Event {
    pub sequence: u64,
    pub action: Option<Action>,
    pub result: String,
    pub term: u64,
    pub applied: u64,
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum StepResult {
    Pending,
    Complete,
}
/// Exclusive local journal. Runtime receipts, not journal events, decide progress.
pub struct Controller {
    db: Database,
    plan: Plan,
    term: u64,
    applied: u64,
}
impl Controller {
    pub fn open(path: impl AsRef<Path>, plan: Plan) -> Result<Self> {
        plan.validate()?;
        let db = Database::create(path).map_err(err)?;
        let tx = db.begin_write().map_err(err)?;
        {
            let mut t = tx.open_table(STATE).map_err(err)?;
            let old = t.get("plan").map_err(err)?.map(|v| v.value().to_vec());
            let bytes = serde_json::to_vec(&plan).map_err(err)?;
            if let Some(old) = old {
                if old != bytes {
                    return Err(Error::new(
                        Code::StalePlan,
                        "journal cannot rebind to a different plan",
                    ));
                }
            } else {
                t.insert("plan", bytes.as_slice()).map_err(err)?;
            }
            tx.open_table(EVENTS).map_err(err)?;
        }
        tx.commit().map_err(err)?;
        let mut c = Self {
            term: plan.term,
            applied: plan.applied,
            db,
            plan,
        };
        let events = c.events()?;
        for (index, e) in events.iter().enumerate() {
            if e.sequence != index as u64 + 1 || e.term < c.term || e.applied < c.applied {
                return Err(err("journal frontier/sequence corruption"));
            }
            c.term = e.term;
            c.applied = e.applied;
        }
        Ok(c)
    }
    pub fn events(&self) -> Result<Vec<Event>> {
        let tx = self.db.begin_read().map_err(err)?;
        let table = tx.open_table(EVENTS).map_err(err)?;
        let mut out = Vec::new();
        for row in table.iter().map_err(err)? {
            let (k, v) = row.map_err(err)?;
            if k.value() > MAX_EVENTS {
                return Err(err("journal event limit"));
            }
            let e: Event = serde_json::from_slice(v.value()).map_err(err)?;
            if e.sequence != k.value() {
                return Err(err("journal key mismatch"));
            }
            out.push(e);
        }
        Ok(out)
    }
    fn append(&mut self, action: Option<Action>, result: String) -> Result<()> {
        let tx = self.db.begin_write().map_err(err)?;
        {
            let mut t = tx.open_table(EVENTS).map_err(err)?;
            let n = t
                .len()
                .map_err(err)?
                .checked_add(1)
                .ok_or_else(|| err("sequence exhausted"))?;
            if n > MAX_EVENTS {
                return Err(err("event capacity exhausted"));
            }
            let e = Event {
                sequence: n,
                action,
                result,
                term: self.term,
                applied: self.applied,
            };
            let bytes = serde_json::to_vec(&e).map_err(err)?;
            t.insert(n, bytes.as_slice()).map_err(err)?;
        }
        tx.commit().map_err(err)
    }
    pub fn step(&mut self, owner: &mut impl Owner) -> Result<StepResult> {
        self.plan.validate()?;
        let o = owner.current(self.plan.request.identity.group)?;
        let next = self.plan.next(&o)?;
        if o.term < self.term || o.applied < self.applied {
            return Err(Error::new(
                Code::StalePlan,
                "journal current frontier regressed",
            ));
        }
        self.term = o.term;
        self.applied = o.applied;
        let Some(action) = next else {
            self.append(None, "complete: fresh matching receipt".into())?;
            return Ok(StepResult::Complete);
        };
        self.append(
            Some(action.clone()),
            "attempt: outcome may be unknown".into(),
        )?;
        match owner.act(&action) {
            Ok(()) => {
                self.append(
                    Some(action),
                    "reply received; fresh current required".into(),
                )?;
                Ok(StepResult::Pending)
            }
            Err(e) => {
                self.append(Some(action), format!("indeterminate: {e}"))?;
                Err(e)
            }
        }
    }
}
