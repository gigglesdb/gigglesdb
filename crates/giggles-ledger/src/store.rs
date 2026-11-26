use super::*;
use giggles_core::{
    Admin, CoreError, Limits, SqlSession,
    relational::{DataType, SqlError, Value},
};
use std::path::Path;

const CREATE: &str = "CREATE TABLE ledger_state (slot BIGINT PRIMARY KEY, payload BYTEA NOT NULL)";
const READ: &str = "SELECT slot,payload FROM ledger_state LIMIT 2";

/// A dedicated database owner. Clones share the same durable core handle.
#[derive(Clone)]
pub struct Ledger {
    admin: Admin,
}
fn core(e: CoreError) -> Error {
    Error::Core(e.to_string())
}
fn retry(e: &CoreError) -> bool {
    matches!(e, CoreError::Busy | CoreError::Deadlock | CoreError::Sql(SqlError::Conflict) | CoreError::SqlSerialization(_))
}
fn encode(state: &State) -> Result<Vec<u8>> {
    let b = serde_json::to_vec(state).map_err(|e| Error::Corrupt(e.to_string()))?;
    let pending = state
        .positions
        .values()
        .filter(|p| matches!(p.state, TransferState::Pending { .. }))
        .count();
    // Each accepted hold must retain capacity for a full-width terminal receipt
    // and position. Every intervening write preserves these reservations.
    if state.receipts.len() + pending > MAX_RECEIPTS
        || b.len() + pending * TERMINAL_HEADROOM > MAX_PAYLOAD
    {
        return Err(Error::Limit);
    }
    Ok(b)
}
fn schema(admin: &Admin) -> Result<()> {
    let (_, tables) = admin.catalog().map_err(core)?;
    if tables.len() != 1 {
        return Err(Error::Corrupt("dedicated schema table count".into()));
    }
    let t = &tables[0];
    if t.name != "ledger_state"
        || t.primary_key != [0]
        || t.columns.len() != 2
        || t.columns[0].name != "slot"
        || t.columns[0].data_type != DataType::BigInt
        || !t.columns[0].not_null
        || t.columns[1].name != "payload"
        || t.columns[1].data_type != DataType::Bytes
        || !t.columns[1].not_null
    {
        return Err(Error::Corrupt("schema mismatch".into()));
    }
    Ok(())
}
fn load(s: &mut SqlSession) -> Result<(State, u64)> {
    let r = s.execute(READ, &[], None).map_err(core)?;
    if r.rows.len() != 1 {
        return Err(Error::Corrupt("singleton row count".into()));
    }
    let row = &r.rows[0];
    let [Value::Int(1), Value::Bytes(b)] = row.values.as_slice() else {
        return Err(Error::Corrupt("singleton shape".into()));
    };
    if b.len() > MAX_PAYLOAD {
        return Err(Error::Corrupt("payload cap".into()));
    }
    let state: State = serde_json::from_slice(b).map_err(|e| Error::Corrupt(e.to_string()))?;
    if encode(&state).map_err(|e| Error::Corrupt(format!("capacity: {e}")))? != *b {
        return Err(Error::Corrupt("noncanonical payload".into()));
    }
    state.audit()?;
    Ok((state, row.version))
}
impl Ledger {
    pub fn create(path: impl AsRef<Path>, definition: Definition) -> Result<Self> {
        let state = State::new(definition)?;
        let admin = Admin::create(path, Limits::default()).map_err(core)?;
        let mut s = admin.sql_session();
        s.execute("BEGIN", &[], None).map_err(core)?;
        s.execute(CREATE, &[], None).map_err(core)?;
        s.execute(
            "INSERT INTO ledger_state VALUES($1,$2)",
            &[Value::Int(1), Value::Bytes(encode(&state)?)],
            None,
        )
        .map_err(core)?;
        s.execute("COMMIT", &[], None)
            .map_err(|_| Error::Unknown { ids: vec![] })?;
        let ledger = Self { admin };
        ledger.reconcile()?;
        Ok(ledger)
    }
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let ledger = Self {
            admin: Admin::open(path, Limits::default()).map_err(core)?,
        };
        ledger.reconcile()?;
        Ok(ledger)
    }
    fn read<T>(&self, f: impl FnOnce(&State) -> Result<T>) -> Result<T> {
        schema(&self.admin)?;
        let mut s = self.admin.sql_session();
        s.execute("BEGIN", &[], None).map_err(core)?;
        let (state, _) = load(&mut s)?;
        f(&state)
    }
    fn change<T>(&self, ids: Vec<String>, mut f: impl FnMut(&mut State) -> Result<T>) -> Result<T> {
        self.change_with_fault(ids, Fault::None, &mut f)
    }
    fn change_with_fault<T>(
        &self,
        ids: Vec<String>,
        fault: Fault,
        mut f: impl FnMut(&mut State) -> Result<T>,
    ) -> Result<T> {
        schema(&self.admin)?;
        for _ in 0..MAX_RETRIES {
            let mut s = self.admin.sql_session();
            match s.execute("BEGIN", &[], None) {
                Ok(_) => {}
                Err(e) if retry(&e) => {
                    std::thread::yield_now();
                    continue;
                }
                Err(e) => return Err(core(e)),
            }
            let (mut state, version) = load(&mut s)?;
            let result = f(&mut state)?;
            state.audit()?;
            let b = encode(&state)?;
            if let Err(e) = s.execute(
                "UPDATE ledger_state SET payload=$1 WHERE slot=$2",
                &[Value::Bytes(b), Value::Int(1)],
                Some(version),
            ) {
                if retry(&e) {
                    continue;
                } else {
                    return Err(core(e));
                }
            }
            use giggles_core::CrashPoint;
            let point = match fault {
                Fault::None => CrashPoint::None,
                Fault::BeforeCommit => CrashPoint::BeforeCommit,
                Fault::AfterCommit => CrashPoint::AfterCommit,
                Fault::ErrorBeforeCommit => CrashPoint::ErrorBeforeCommit,
                Fault::ErrorAfterCommit => CrashPoint::ErrorAfterCommit,
            };
            // The explicit fault seam is only safe with exclusive test ownership.
            if fault != Fault::None {
                self.admin.set_crash_point(point).map_err(core)?;
            }
            let commit = s.execute("COMMIT", &[], None);
            if fault != Fault::None {
                self.admin.set_crash_point(CrashPoint::None).map_err(core)?;
            }
            match commit {
                Ok(_) => return Ok(result),
                Err(e) if retry(&e) => {
                    std::thread::yield_now();
                    continue;
                }
                Err(_) => return Err(Error::Unknown { ids }),
            }
        }
        Err(Error::ConflictExhausted)
    }
    pub fn definition(&self) -> Result<Definition> {
        self.read(|s| Ok(s.definition.clone()))
    }
    pub fn create_account(&self, d: AccountDefinition) -> Result<()> {
        self.change(vec![], |s| s.create_account(d.clone()))
    }
    pub fn account(&self, id: &str) -> Result<Account> {
        self.read(|s| {
            s.accounts
                .get(id)
                .cloned()
                .ok_or_else(|| Error::Missing(id.into()))
        })
    }
    /// Execute a caller-persisted intent. Retry an unknown result with this same ID.
    pub fn execute(&self, intent: Intent) -> Result<Receipt> {
        self.change(vec![intent.id.clone()], |s| s.apply(intent.clone()))
    }
    /// Recovery test seam; requires exclusive ownership, never use concurrently.
    pub fn execute_with_fault(&self, intent: Intent, fault: Fault) -> Result<Receipt> {
        self.change_with_fault(vec![intent.id.clone()], fault, |s| s.apply(intent.clone()))
    }
    /// All entries commit or none do. Identical repeated IDs resolve to one receipt.
    pub fn atomic_batch(&self, intents: Vec<Intent>) -> Result<Vec<Receipt>> {
        if intents.is_empty() || intents.len() > MAX_BATCH {
            return Err(Error::Limit);
        }
        self.change(intents.iter().map(|i| i.id.clone()).collect(), |s| {
            intents.iter().map(|i| s.apply(i.clone())).collect()
        })
    }
    /// Each item commits separately, including when a neighboring item fails.
    pub fn individual_batch(&self, intents: Vec<Intent>) -> Result<Vec<Result<Receipt>>> {
        if intents.is_empty() || intents.len() > MAX_BATCH {
            return Err(Error::Limit);
        }
        Ok(intents.into_iter().map(|i| self.execute(i)).collect())
    }
    pub fn status(&self, id: &str) -> Result<Option<Receipt>> {
        self.read(|s| Ok(s.receipts.iter().find(|r| r.intent.id == id).cloned()))
    }
    pub fn position(&self, id: &str) -> Result<Option<Position>> {
        self.read(|s| Ok(s.positions.get(id).cloned()))
    }
    /// Fail closed on inconsistent caches, journal digests, lifecycle or schema.
    pub fn reconcile(&self) -> Result<Reconciliation> {
        self.read(State::audit)
    }
    /// Sealed core export retains ledger IDs and permanent receipts.
    pub fn export(&self, path: impl AsRef<Path>) -> Result<[u8; 32]> {
        self.reconcile()?;
        self.admin.export(path).map_err(core)
    }
    /// Restores an isolated historical timeline; external settlement fencing is caller-owned.
    pub fn restore(export: impl AsRef<Path>, path: impl AsRef<Path>) -> Result<Self> {
        let ledger = Self {
            admin: Admin::restore(export, path, Limits::default()).map_err(core)?,
        };
        ledger.reconcile()?;
        Ok(ledger)
    }
}
