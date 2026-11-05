//! Trusted consensus integration. Images contain every kernel namespace.
//! Evaluation has no filesystem, network, clock or random identity inputs.
//! The caller MUST atomically persist the returned image with its applied index.
use super::*;
use crate::local::{AdminCommand, Reply};

/// Opaque, validated canonical kernel image; not an export that revokes authority.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, serde::Deserialize)]
pub struct Image(Vec<u8>);

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
}
impl Command {
    /// Validate the closed command grammar before it can enter a durable log.
    pub fn validate(&self) -> Result<(), CoreError> {
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
        let s = from_records(decode(&bytes)?)?;
        if encode(&records(&s)?)? != bytes {
            return Err(CoreError::Corrupt("noncanonical image".into()));
        }
        Ok(Self(bytes))
    }
    pub fn bytes(&self) -> &[u8] {
        &self.0
    }
    pub fn domain(&self) -> Result<u128, CoreError> {
        Ok(from_records(decode(&self.0)?)?.view.domain)
    }
    pub fn snapshot(&self) -> Result<Snapshot, CoreError> {
        Ok(from_records(decode(&self.0)?)?.view)
    }
    pub fn accounting(&self, task: TaskId) -> Result<Option<TaskAccounting>, CoreError> {
        Ok(from_records(decode(&self.0)?)?
            .accounting
            .get(&task)
            .copied())
    }
    /// The Admin handle is constructed and destroyed here; it never escapes.
    pub fn evaluate(&self, command: &Command) -> Result<(Self, Reply), CoreError> {
        command.validate()?;
        let values: BTreeMap<Vec<u8>, Vec<u8>> = decode(&self.0)?;
        from_records(values.clone())?;
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
        let reply = match command {
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
                let response = native_task(&a, *task, *principal, *lower, *upper, action)?;
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
                local::admin(&a, input)
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
        let image = Self(encode(&records(&a.inner.read()?)?)?);
        Ok((image, reply))
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
