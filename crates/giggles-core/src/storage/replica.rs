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
