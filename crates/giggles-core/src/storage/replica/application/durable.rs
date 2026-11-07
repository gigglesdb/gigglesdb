use super::*;
use crate::operations::{self, Bindings, BoundDescription, Compare, Expr, Intent, ValueExpr};
use crate::storage::{State, decode, encode, records};
use std::collections::BTreeMap;

pub const MAX_AUTHORITIES: usize = 64;
pub const MAX_OUTCOMES: usize = 64;
pub const MAX_LEDGER_BYTES: usize = 4 * 1024 * 1024;
const LEDGER_VERSION: u32 = 1;
// One point result has at most one MAX_ROW_BYTES row. Value enum/length
// overhead is bounded by MAX_COLUMNS; numeric canonical text is counted by
// value_bytes. Reserve a full MAX_RESULT_BYTES anyway, plus exact column-name
// encoding and fixed overhead. A mutation can grow one row by MAX_ROW_BYTES.
const OUTCOME_HEADROOM: usize = crate::relational::MAX_RESULT_BYTES + 4096;
const BUSINESS_HEADROOM: usize = crate::relational::MAX_ROW_BYTES + 4096;

/// Stable persisted failures, without engine diagnostic strings.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Failure {
    Unsupported,
    Limit,
    Bind,
    Type,
    NotFound,
    ConditionFailed,
    Conflict,
    Overflow,
    Constraint,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Success {
    pub columns: Vec<String>,
    pub rows: Vec<crate::relational::SqlRow>,
    pub matched: usize,
    pub affected: usize,
    pub changed: usize,
    pub complete: bool,
}
impl From<operations::Outcome> for Success {
    fn from(out: operations::Outcome) -> Self {
        Self {
            columns: out.columns,
            rows: out.rows,
            matched: out.matched,
            affected: out.affected,
            changed: out.changed,
            complete: out.complete,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Outcome {
    Success(Success),
    Failure(Failure),
}
/// Admission failures are not retained and never publish a candidate.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Error {
    Denied,
    Fenced,
    Conflict,
    Capacity,
    Invalid,
}
/// Closed internal owner input. Never derive this variant from remote input.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum OwnerCommand {
    Provision(Authority),
    Revoke { application: u128, generation: u64 },
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct AuthorityRecord {
    authority: Authority,
    revoked: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Retained {
    body: Body,
    outcome: Outcome,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Ledger {
    version: u32,
    authorities: BTreeMap<u128, AuthorityRecord>,
    outcomes: BTreeMap<(u128, Identity), Retained>,
}
impl Default for Ledger {
    fn default() -> Self {
        Self {
            version: LEDGER_VERSION,
            authorities: BTreeMap::new(),
            outcomes: BTreeMap::new(),
        }
    }
}
fn authority_valid(a: &Authority) -> bool {
    a.generation != 0
        && a.read_tables.len() <= MAX_TABLES
        && a.write_tables.len() <= MAX_TABLES
        && a.write_tables.is_subset(&a.read_tables)
}
impl Ledger {
    pub(super) fn is_empty_readonly(&self) -> bool {
        self.authorities.is_empty() && self.outcomes.is_empty()
    }

    pub(crate) fn bytes(&self) -> Result<Vec<u8>, CoreError> {
        let bytes = encode(self)?;
        if bytes.len() > MAX_LEDGER_BYTES {
            return Err(CoreError::Limit);
        }
        Ok(bytes)
    }
    pub(crate) fn from_bytes(bytes: &[u8]) -> Result<Self, CoreError> {
        let ledger: Self = decode(bytes)?;
        ledger.validate()?;
        if ledger.bytes()? != bytes {
            return Err(CoreError::Corrupt("noncanonical application ledger".into()));
        }
        Ok(ledger)
    }
    pub(crate) fn validate(&self) -> Result<(), CoreError> {
        if self.version != LEDGER_VERSION {
            return Err(CoreError::Unsupported);
        }
        if self.authorities.len() > MAX_AUTHORITIES || self.outcomes.len() > MAX_OUTCOMES {
            return Err(CoreError::Limit);
        }
        for (id, record) in &self.authorities {
            if *id != record.authority.application || !authority_valid(&record.authority) {
                return Err(CoreError::Corrupt("application authority invariant".into()));
            }
        }
        for ((domain, identity), retained) in &self.outcomes {
            let authority = self.authorities.get(&identity.application);
            if *domain != retained.body.domain
                || authority.is_none_or(|a| identity.authority_generation > a.authority.generation)
                || identity.authority_generation == 0
            {
                return Err(CoreError::Corrupt("application identity invariant".into()));
            }
            Command {
                identity: identity.clone(),
                body: retained.body.clone(),
            }
            .validate()?;
            if let Outcome::Success(out) = &retained.outcome
                && (out.rows.len() > 1
                    || !out.complete
                    || out.matched > 1
                    || out.affected > 1
                    || out.changed > 1
                    || out.columns.len() > crate::relational::MAX_COLUMNS
                    || out.rows.iter().any(|r| {
                        r.version == 0
                            || r.values.len() != out.columns.len()
                            || r.values
                                .iter()
                                .map(crate::relational::value_bytes)
                                .sum::<usize>()
                                > crate::relational::MAX_ROW_BYTES
                    }))
            {
                return Err(CoreError::Corrupt("application outcome invariant".into()));
            }
        }
        self.bytes()?;
        Ok(())
    }
    pub(crate) fn contains_temporal(&self) -> bool {
        self.outcomes.values().any(|r| {
            let (key, assignments) = match &r.body.operation {
                Operation::Read { key } => (key, &[][..]),
                Operation::GuardedWrite { key, assignments, .. } => (key, assignments.as_slice()),
            };
            key.iter().any(Value::is_temporal)
                || assignments.iter().any(|(_, v)| v.is_temporal())
                || matches!(&r.outcome, Outcome::Success(o) if o.rows.iter().any(|r| r.values.iter().any(Value::is_temporal)))
        })
    }
    pub(crate) fn contains_typed(&self) -> bool {
        fn typed(v: &Value) -> bool {
            matches!(v, Value::Numeric(_) | Value::Float4(_) | Value::Float8(_))
        }
        self.outcomes.values().any(|r| {
            let (key, assignments) = match &r.body.operation {
                Operation::Read { key } => (key, &[][..]),
                Operation::GuardedWrite { key, assignments, .. } => (key, assignments.as_slice()),
            };
            key.iter().any(typed) || assignments.iter().any(|(_, v)| typed(v))
                || matches!(&r.outcome, Outcome::Success(o) if o.rows.iter().any(|r| r.values.iter().any(typed)))
        })
    }
    pub(crate) fn revoke_all(&mut self) {
        for record in self.authorities.values_mut() {
            record.revoked = true;
        }
    }
}

fn fenced<'a>(
    state: &'a State,
    command: &Command,
) -> Result<(&'a Ledger, &'a crate::relational::TableDef), Error> {
    command.validate().map_err(|_| Error::Invalid)?;
    let ledger = state.applications.as_ref().ok_or(Error::Denied)?;
    let record = ledger
        .authorities
        .get(&command.identity.application)
        .ok_or(Error::Denied)?;
    if record.revoked {
        return Err(Error::Denied);
    }
    command
        .admit(&record.authority)
        .map_err(|_| Error::Denied)?;
    if state.view.domain != command.body.domain || state.sql.epoch != command.body.catalog_epoch {
        return Err(Error::Fenced);
    }
    let table = state
        .sql
        .tables
        .values()
        .find(|t| t.id == command.body.table)
        .ok_or(Error::Fenced)?;
    Ok((ledger, table))
}
fn failure(error: operations::Error) -> Failure {
    use operations::Error as E;
    match error {
        E::Limit => Failure::Limit,
        E::Bind(_) => Failure::Bind,
        E::Type => Failure::Type,
        E::NotFound => Failure::NotFound,
        E::ConditionFailed => Failure::ConditionFailed,
        E::Conflict => Failure::Conflict,
        E::Overflow => Failure::Overflow,
        E::Core(_) => Failure::Constraint,
        _ => Failure::Unsupported,
    }
}
fn request(
    table: &crate::relational::TableDef,
    body: &Body,
) -> Result<(operations::Request, BoundDescription, Option<u64>), Error> {
    let (key, expected) = match &body.operation {
        Operation::Read { key } => (key, None),
        Operation::GuardedWrite {
            key,
            expected_version,
            ..
        } => (key, Some(*expected_version)),
    };
    if key.len() != table.primary_key.len() {
        return Err(Error::Invalid);
    }
    let filter = table
        .primary_key
        .iter()
        .zip(key)
        .map(|(i, value)| {
            Expr::Compare(
                Box::new(Expr::Field(table.columns[*i].name.clone())),
                Compare::Eq,
                Box::new(Expr::Value(ValueExpr::Literal(value.clone()))),
            )
        })
        .reduce(|a, b| Expr::And(Box::new(a), Box::new(b)))
        .ok_or(Error::Invalid)?;
    let operation = match &body.operation {
        Operation::Read { .. } => operations::Request::Read {
            table: table.name.clone(),
            filter: Some(filter),
            order: vec![],
            limit: None,
            projection: vec![],
        },
        Operation::GuardedWrite { assignments, .. } => {
            let mut values = Vec::new();
            for (id, value) in assignments {
                let i = table
                    .columns
                    .iter()
                    .position(|c| c.id == *id)
                    .ok_or(Error::Invalid)?;
                if table.primary_key.contains(&i) {
                    return Err(Error::Invalid);
                }
                values.push((
                    table.columns[i].name.clone(),
                    ValueExpr::Literal(value.clone()),
                ));
            }
            operations::Request::Update {
                table: table.name.clone(),
                filter: filter.clone(),
                guard: Some(filter),
                values,
                intent: Intent::One,
            }
        }
    };
    let description = BoundDescription {
        contract_version: operations::ENGINE_CONTRACT_VERSION,
        database_domain: body.domain,
        catalog_epoch: body.catalog_epoch,
        table: table.clone(),
        columns: if expected.is_none() {
            table.columns.clone()
        } else {
            vec![]
        },
    };
    Ok((operation, description, expected))
}
impl super::super::Image {
    /// Trusted ingress lookup after a fresh quorum barrier. The authenticated
    /// application comes from local certificate configuration, never the wire.
    /// Retained operation IDs keep their original generation and exact body;
    /// ambiguous historical IDs fail closed. This does not itself prove quorum.
    pub fn application_ingress(
        &self,
        application: u128,
        operation: u128,
        body: Body,
    ) -> Result<Result<Command, Error>, CoreError> {
        let state = super::super::replica_state(&self.0)?;
        let Some(ledger) = state.applications.as_ref() else {
            return Ok(Err(Error::Denied));
        };
        let Some(record) = ledger.authorities.get(&application) else {
            return Ok(Err(Error::Denied));
        };
        let mut identity = Identity {
            application,
            authority_generation: record.authority.generation,
            operation,
        };
        let mut found = false;
        for ((_, id), retained) in &ledger.outcomes {
            if id.application == application && id.operation == operation {
                if found {
                    return Ok(Err(Error::Conflict));
                }
                found = true;
                identity = id.clone();
                // Authority is checked before even revealing a body conflict.
                let command = Command {
                    identity: identity.clone(),
                    body: retained.body.clone(),
                };
                if let Err(e) = fenced(&state, &command) {
                    return Ok(Err(e));
                }
                if retained.body != body {
                    return Ok(Err(Error::Conflict));
                }
            }
        }
        let command = Command { identity, body };
        if let Err(e) = fenced(&state, &command) {
            return Ok(Err(e));
        }
        Ok(Ok(command))
    }
    /// Internal status requires the full accepted body and fresh stored authority.
    /// It returns no forensic data after revocation, schema change or restore.
    pub fn application_status(
        &self,
        command: &Command,
    ) -> Result<Result<Option<Outcome>, Error>, CoreError> {
        let state = super::super::replica_state(&self.0)?;
        let (ledger, _) = match fenced(&state, command) {
            Ok(v) => v,
            Err(e) => return Ok(Err(e)),
        };
        Ok(
            match ledger
                .outcomes
                .get(&(command.body.domain, command.identity.clone()))
            {
                Some(r) if r.body != command.body => Err(Error::Conflict),
                Some(r) => Ok(Some(r.outcome.clone())),
                None => Ok(None),
            },
        )
    }
    /// Pure deterministic application. Effects and immutable typed outcome are
    /// returned in one canonical Image; persist it with the applied index.
    pub fn evaluate_application(
        &self,
        command: &Command,
    ) -> Result<Result<(Self, Outcome), Error>, CoreError> {
        let state = super::super::replica_state(&self.0)?;
        let (ledger, table) = match fenced(&state, command) {
            Ok(v) => v,
            Err(e) => return Ok(Err(e)),
        };
        if table.hidden_rowid().is_some() {
            return Err(CoreError::Unsupported);
        }
        let key = (command.body.domain, command.identity.clone());
        if let Some(retained) = ledger.outcomes.get(&key) {
            return Ok(if retained.body == command.body {
                Ok((self.clone(), retained.outcome.clone()))
            } else {
                Err(Error::Conflict)
            });
        }
        let (request, description, expected) = match request(table, &command.body) {
            Ok(v) => v,
            Err(e) => return Ok(Err(e)),
        };
        let column_bytes =
            encode(&table.columns.iter().map(|c| &c.name).collect::<Vec<_>>())?.len();
        let reserve = OUTCOME_HEADROOM
            .checked_add(column_bytes)
            .ok_or(CoreError::Limit)?;
        let entry_bytes = encode(&(key.clone(), &command.body))?.len();
        let ledger_bytes = ledger.bytes()?.len();
        let image_bytes = encode(&records(&state)?)?.len();
        if ledger.outcomes.len() >= MAX_OUTCOMES
            || ledger_bytes
                .checked_add(entry_bytes)
                .and_then(|n| n.checked_add(reserve))
                .is_none_or(|n| n > MAX_LEDGER_BYTES)
            || image_bytes
                .checked_add(entry_bytes)
                .and_then(|n| n.checked_add(reserve))
                .and_then(|n| n.checked_add(BUSINESS_HEADROOM))
                .is_none_or(|n| n as u64 > crate::storage::MAX_BYTES)
        {
            return Ok(Err(Error::Capacity));
        }
        let (mut next, outcome) = match crate::storage::operation_adapter::execute_fenced(
            &state,
            &request,
            &Bindings::new(),
            &description,
            expected,
        ) {
            Ok((next, out)) => (next, Outcome::Success(out.into())),
            Err(e) => (state.clone(), Outcome::Failure(failure(e))),
        };
        if encode(&outcome)?.len() > reserve {
            return Err(CoreError::Limit);
        }
        next.applications
            .as_mut()
            .ok_or(CoreError::Unsupported)?
            .outcomes
            .insert(
                key,
                Retained {
                    body: command.body.clone(),
                    outcome: outcome.clone(),
                },
            );
        crate::storage::validate(&next)?;
        let image = Self(encode(&records(&next)?)?);
        Ok(Ok((image, outcome)))
    }
    /// Closed trusted owner administration; must enter the same durable log as
    /// application commands. This is never an authenticated external Admin API.
    pub fn evaluate_application_owner(&self, command: &OwnerCommand) -> Result<Self, CoreError> {
        let mut state = super::super::replica_state(&self.0)?;
        let ledger = state.applications.get_or_insert_with(Ledger::default);
        match command {
            OwnerCommand::Provision(a) => {
                if !authority_valid(a)
                    || a.domain != state.view.domain
                    || a.catalog_epoch != state.sql.epoch
                    || a.read_tables
                        .iter()
                        .any(|id| !state.sql.tables.values().any(|t| t.id == *id))
                {
                    return Err(CoreError::Fenced);
                }
                if let Some(old) = ledger.authorities.get(&a.application) {
                    if a.generation <= old.authority.generation {
                        return Err(CoreError::Fenced);
                    }
                } else if ledger.authorities.len() >= MAX_AUTHORITIES {
                    return Err(CoreError::Limit);
                }
                ledger.authorities.insert(
                    a.application,
                    AuthorityRecord {
                        authority: a.clone(),
                        revoked: false,
                    },
                );
            }
            OwnerCommand::Revoke {
                application,
                generation,
            } => {
                let record = ledger
                    .authorities
                    .get_mut(application)
                    .ok_or(CoreError::Fenced)?;
                if record.authority.generation != *generation {
                    return Err(CoreError::Fenced);
                }
                record.revoked = true;
            }
        }
        crate::storage::validate(&state)?;
        Ok(Self(encode(&records(&state)?)?))
    }
}

#[cfg(test)]
mod tests;
