//! Trusted consensus integration for the qualified repair/relational namespaces.
//! Embedded native once outcomes are deliberately unsupported in these images.
//! Evaluation has no filesystem, network, clock or random identity inputs.
//! The caller MUST atomically persist the returned image with its applied index.
use super::*;
use crate::local::{AdminCommand, Reply};

pub mod application;
pub mod protected;
mod projected_admission;

/// Opaque, validated canonical kernel image; not an export that revokes authority.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, serde::Deserialize)]
pub struct Image(Vec<u8>);

// Image derives Deserialize for existing integrations, so every decoding entry
// must check namespace support even when callers bypass Image::from_bytes.
fn replica_state(bytes: &[u8]) -> Result<State, CoreError> {
    replica_state_with_cancel(bytes, &mut || crate::cancellation::check().is_err())
}
// Shares the complete bounded canonical decoder; never resets or reads ambient
// cancellation on behalf of trusted pure projection callers.
fn replica_state_with_cancel(
    bytes: &[u8],
    cancel: &mut impl FnMut() -> bool,
) -> Result<State, CoreError> {
    if bytes.len() as u64 > MAX_BYTES {
        return Err(CoreError::Limit);
    }
    let state = from_records_with_cancel(decode(bytes)?, cancel)?;
    if state.native_outcomes.is_some()
        || state.sql.has_secondary_indexes_engine()
        || state.sql.contains_heap()
    {
        return Err(CoreError::Unsupported);
    }
    if encode(&records(&state)?)? != bytes {
        return Err(CoreError::Corrupt("noncanonical image".into()));
    }
    Ok(state)
}

#[derive(Clone, Debug, Serialize, serde::Deserialize)]
pub enum TaskAction {
    Preview(Request),
    Execute(Request, Option<ApprovalId>),
    Status(OperationId),
}

/// Administrative input is trusted/offline only. It must never be remote dispatch.
#[derive(Clone, Debug, Serialize, serde::Deserialize)]
pub enum Command {
    Admin(Vec<u8>),
    /// Trusted consensus release revalidation. No disclosure debit: the held
    /// immutable response was already reserved by NativeTask. Never dispatch
    /// this as a standalone external request.
    ValidateRelease {
        task: TaskId,
        principal: Principal,
        lower: u64,
        upper: u64,
    },
    /// Trusted consensus input only, not a remote identity/time envelope.
    /// Produces redacted data, with a fresh disclosure debit after business work.
    NativeTask {
        task: TaskId,
        principal: Principal,
        lower: u64,
        upper: u64,
        action: TaskAction,
    },
    Task {
        task: TaskId,
        principal: Principal,
        lower: u64,
        upper: u64,
        action: TaskAction,
    },
    /// Internal application intent; claims alone confer no authority.
    Application(application::Command),
    /// Trusted owner input only. Never expose through a remote dispatch path.
    ApplicationOwner(application::OwnerCommand),
}
impl Command {
    /// Validate the closed command grammar before it can enter a durable log.
    pub fn validate(&self) -> Result<(), CoreError> {
        if let Self::Application(command) = self {
            command.validate()?;
        }
        if let Self::Admin(input) = self {
            if input.len() > local::MAX_INPUT {
                return Err(CoreError::Limit);
            }
            let command: AdminCommand =
                serde_json::from_slice(input).map_err(|_| CoreError::Unsupported)?;
            if matches!(command, AdminCommand::Export { .. }) {
                return Err(CoreError::Unsupported);
            }
        }
        Ok(())
    }
}

impl Image {
    pub fn new(domain: u128) -> Result<Self, CoreError> {
        Ok(Self(encode(&records(&empty(domain))?)?))
    }
    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self, CoreError> {
        if bytes.len() as u64 > MAX_BYTES {
            return Err(CoreError::Limit);
        }
        let s = replica_state(&bytes)?;
        if encode(&records(&s)?)? != bytes {
            return Err(CoreError::Corrupt("noncanonical image".into()));
        }
        Ok(Self(bytes))
    }
    pub fn bytes(&self) -> &[u8] {
        &self.0
    }
    pub fn domain(&self) -> Result<u128, CoreError> {
        Ok(replica_state(&self.0)?.view.domain)
    }
    pub fn snapshot(&self) -> Result<Snapshot, CoreError> {
        Ok(replica_state(&self.0)?.view)
    }
    /// Trusted data-range projection. Builds a fresh kernel containing only
    /// typed repair rows, preserving their versions. No grant, approval, receipt,
    /// accounting, relational catalog or retry authority is inherited.
    /// The caller must separately fence the source and verify its supported
    /// namespaces before using this as a split bootstrap.
    pub fn row_partition(&self, start: Key, end: Option<Key>) -> Result<Self, CoreError> {
        if end.is_some_and(|end| start >= end) {
            return Err(CoreError::Unsupported);
        }
        let source = replica_state(&self.0)?;
        let mut target = empty(source.view.domain);
        // This logical storage generation is not task authority. Retaining it
        // lets a data-only image remain canonical after ordinary row commits.
        target.sql.generation = source.sql.generation;
        target.view.rows = source
            .view
            .rows
            .into_iter()
            .filter(|(key, _)| *key >= start && end.is_none_or(|end| *key < end))
            .collect();
        Ok(Self(encode(&records(&target)?)?))
    }
    pub fn accounting(&self, task: TaskId) -> Result<Option<TaskAccounting>, CoreError> {
        Ok(replica_state(&self.0)?.accounting.get(&task).copied())
    }
    /// The Admin handle is constructed and destroyed here; it never escapes.
    pub fn evaluate(&self, command: &Command) -> Result<(Self, Reply), CoreError> {
        command.validate()?;
        if let Command::Application(application) = command {
            return match self.evaluate_application(application)? {
                Ok((image, outcome)) => {
                    Ok((image, Reply::ok(serde_json::to_value(outcome).map_err(io)?)))
                }
                Err(error) => Ok((
                    self.clone(),
                    Reply::ok(serde_json::to_value(error).map_err(io)?),
                )),
            };
        }
        if let Command::ApplicationOwner(owner) = command {
            return Ok((
                self.evaluate_application_owner(owner)?,
                Reply::ok(serde_json::json!({"authorized": true})),
            ));
        }
        self.with_admin(|a| self.evaluate_admin(a, command))
    }
    // Share the real storage/Executor path with trusted protected staging.
    // The temporary Admin never escapes and no persistent input is changed.
    fn with_admin<T>(
        &self,
        work: impl FnOnce(&Admin) -> Result<T, CoreError>,
    ) -> Result<(Self, T), CoreError> {
        let values: BTreeMap<Vec<u8>, Vec<u8>> = decode(&self.0)?;
        replica_state(&self.0)?;
        let db = Database::builder()
            .set_cache_size(1024 * 1024)
            .create_with_backend(redb::backends::InMemoryBackend::new())
            .map_err(io)?;
        let tx = db.begin_write().map_err(io)?;
        {
            let mut t = tx.open_table(RECORDS).map_err(io)?;
            for (k, v) in values {
                t.insert(k.as_slice(), v.as_slice()).map_err(io)?;
            }
        }
        tx.commit().map_err(io)?;
        let a = Admin::attach(db, Limits::default())?;
        let reply = work(&a)?;
        let state = a.inner.read()?;
        if state.sql.contains_heap() {
            return Err(CoreError::Unsupported);
        }
        let image = Self(encode(&records(&state)?)?);
        Ok((image, reply))
    }
    fn evaluate_admin(&self, a: &Admin, command: &Command) -> Result<Reply, CoreError> {
        let reply = match command {
            Command::Application(_) | Command::ApplicationOwner(_) => {
                return Err(CoreError::Unsupported);
            }
            Command::ValidateRelease {
                task,
                principal,
                lower,
                upper,
            } => {
                let result = a.executor(*task, *principal).and_then(|e| {
                    e.inner.change_decision(|s| {
                        e.auth(s)?;
                        Executor::current(s, *task, *lower, *upper)
                    })
                });
                match result {
                    Ok(()) => Reply::ok(serde_json::json!({"authorized":true})),
                    Err(error) => local::failure(error),
                }
            }
            Command::NativeTask {
                task,
                principal,
                lower,
                upper,
                action,
            } => {
                let response = native_task(a, *task, *principal, *lower, *upper, action)?;
                Reply::ok(serde_json::to_value(response).map_err(io)?)
            }
            Command::Admin(input) => {
                if input.len() > local::MAX_INPUT {
                    return Err(CoreError::Limit);
                }
                let c: AdminCommand = serde_json::from_slice(input).map_err(io)?;
                if matches!(c, AdminCommand::Export { .. }) {
                    return Err(CoreError::Unsupported);
                }
                local::admin(a, input)
            }
            Command::Task {
                task,
                principal,
                lower,
                upper,
                action,
            } => {
                let result = (|| {
                    let e = a.executor(*task, *principal)?;
                    let data = match action {
                        TaskAction::Preview(r) => {
                            let p = e.preview_current(r, *lower, *upper)?;
                            let token = hex_token(&p)?;
                            serde_json::json!({"preview":token,"effects":p.effects})
                        }
                        TaskAction::Execute(r, approval) => {
                            match e.execute_current(r.clone(), *lower, *upper, *approval)? {
                                Ok(receipt) => serde_json::to_value(receipt).map_err(io)?,
                                Err(err) => return Ok(local::failure(CoreError::Policy(err))),
                            }
                        }
                        TaskAction::Status(id) => {
                            serde_json::to_value(e.status_current(*id, *lower, *upper)?)
                                .map_err(io)?
                        }
                    };
                    let reply = Reply::ok(data);
                    // Conservative bound includes the entire JSON response. One reservation
                    // per delivered attempt; a lost ACK may still consume disclosure.
                    let size = serde_json::to_vec(&reply).map_err(io)?.len() as u64;
                    e.reserve_disclosure(size, *lower, *upper)?;
                    Ok(reply)
                })();
                match result {
                    Ok(reply) => reply,
                    Err(err) => local::failure(err),
                }
            }
        };
        Ok(reply)
    }
}
fn hex_token(p: &Preview) -> Result<String, CoreError> {
    let b = bincode::serialize(p).map_err(io)?;
    if b.len() > (local::MAX_INPUT - 1024) / 2 {
        return Err(CoreError::Limit);
    }
    Ok(b.iter().map(|v| format!("{v:02x}")).collect())
}

/// Redacted v1 task-only response. `sql` is always null in this bounded subset.
/// This is the data inside the trusted local Reply, not its outer wire envelope.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeResponse {
    pub version: u32,
    pub code: String,
    pub operation: Option<OperationId>,
    pub count: Option<usize>,
    pub digest: Option<String>,
    pub sql: Option<()>,
}
impl NativeResponse {
    fn code(code: &str) -> Self {
        Self {
            version: 1,
            code: code.into(),
            operation: None,
            count: None,
            digest: None,
            sql: None,
        }
    }
    fn effects(code: &str, operation: OperationId, effects: &[Effect]) -> Result<Self, CoreError> {
        let bytes = serde_json::to_vec(effects).map_err(io)?;
        let digest = Sha256::digest(bytes)
            .iter()
            .map(|v| format!("{v:02x}"))
            .collect();
        Ok(Self {
            operation: Some(operation),
            count: Some(effects.len()),
            digest: Some(digest),
            ..Self::code(code)
        })
    }
    fn decision(operation: OperationId, decision: Decision) -> Result<Self, CoreError> {
        match decision {
            Ok(receipt) => Self::effects("committed", operation, &receipt.effects),
            Err(error) => Err(CoreError::Policy(error)),
        }
    }
}
// Same task-policy codes as P2. No diagnostic strings or raw errors escape.
fn native_code(error: &CoreError) -> &'static str {
    match error {
        CoreError::Policy(Error::Authentication | Error::UnknownTask) => "28000",
        CoreError::Policy(Error::Revoked | Error::Expired | Error::Scope) => "42501",
        CoreError::Policy(Error::UncertainTime | Error::TimeRegression) => "55000",
        CoreError::Policy(Error::Budget) | CoreError::Limit => "54000",
        CoreError::Policy(
            Error::ApprovalRequired | Error::InvalidApproval | Error::StalePreview,
        ) => "GD001",
        CoreError::Policy(Error::IdConflict) => "GD002",
        CoreError::Policy(_) => "22023",
        CoreError::Cancelled => "57014",
        CoreError::Busy => "53300",
        CoreError::Storage(_) | CoreError::Fenced => "08007",
        _ => "XX000",
    }
}
fn native_task(
    a: &Admin,
    task: TaskId,
    principal: Principal,
    lower: u64,
    upper: u64,
    action: &TaskAction,
) -> Result<NativeResponse, CoreError> {
    let operation = match action {
        TaskAction::Preview(r) | TaskAction::Execute(r, _) => r.operation,
        TaskAction::Status(id) => *id,
    };
    let business = (|| {
        let e = a.executor(task, principal)?;
        match action {
            TaskAction::Preview(r) => {
                let preview = e.preview_current(r, lower, upper)?;
                NativeResponse::effects("preview", operation, &preview.effects)
            }
            TaskAction::Execute(r, approval) => NativeResponse::decision(
                operation,
                e.execute_current(r.clone(), lower, upper, *approval)?,
            ),
            TaskAction::Status(id) => match e.status_current(*id, lower, upper)? {
                OperationStatus::Absent => Ok(NativeResponse {
                    operation: Some(operation),
                    ..NativeResponse::code("absent")
                }),
                OperationStatus::Decided(d) => NativeResponse::decision(operation, d),
            },
        }
    })();
    let response = business.unwrap_or_else(|error| NativeResponse {
        operation: Some(operation),
        ..NativeResponse::code(native_code(&error))
    });
    let bytes = u64::try_from(serde_json::to_vec(&response).map_err(io)?.len())
        .ok()
        .and_then(|n| n.checked_add(1024))
        .ok_or(CoreError::Limit)?;
    // Separate from the business decision: re-open current authority, then debit
    // every disclosure attempt, including protected errors and deduped receipts.
    // A failed release must not undo an already decided business operation.
    let release = a
        .executor(task, principal)
        .and_then(|e| e.reserve_disclosure(bytes, lower, upper));
    Ok(match release {
        Ok(()) => response,
        Err(error) => NativeResponse::code(match error {
            CoreError::Cancelled => "57014",
            CoreError::Policy(Error::UncertainTime | Error::TimeRegression) => "55000",
            _ => "42501",
        }),
    })
}

#[cfg(test)]
mod heap_gate_tests {
    use super::*;

    #[test]
    fn heap_replica_and_restricted_projection_are_explicitly_unsupported() {
        for command in [
            AdminCommand::Sql {
                sql: "CREATE TABLE t(id BIGINT)".into(),
                params: vec![],
                expected_version: None,
            },
            AdminCommand::SqlBatch {
                statements: vec![local::SqlInput {
                    sql: "CREATE TABLE t(id BIGINT)".into(),
                    params: vec![],
                    expected_version: None,
                }],
            },
        ] {
            let image = Image::new(123).unwrap();
            let command = Command::Admin(serde_json::to_vec(&command).unwrap());
            assert!(matches!(image.evaluate(&command), Err(CoreError::Unsupported)));
        }
        let mut state = empty(123);
        let p = relational::Prepared::parse("CREATE TABLE t(id BIGINT)", 123, state.sql.epoch).unwrap();
        state.sql.execute(&p, &[], None).unwrap();
        let bytes = encode(&records(&state).unwrap()).unwrap();
        assert!(matches!(Image::from_bytes(bytes.clone()), Err(CoreError::Unsupported)));
        // Deserialize can bypass Image::from_bytes; all image entry points recheck.
        let image = Image(bytes);
        assert!(matches!(image.snapshot(), Err(CoreError::Unsupported)));
        let limits = projected_admission::SourceAdmissionLimits::default();
        let mut budget = projected_admission::SourceBudget::new(projected_admission::SourceAdmissionLimits::default());
        assert!(matches!(projected_admission::admit_projected_source(
            &image, &limits, &mut budget, &mut || false
        ), Err(CoreError::Unsupported)));
    }
}
