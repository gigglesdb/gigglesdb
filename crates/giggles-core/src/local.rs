//! Version-1 trusted local JSON protocol. Never expose admin dispatch over RPC.
use crate::*;
use bincode::Options;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
pub const MAX_INPUT: usize = 1024 * 1024;
pub const MAX_OUTPUT: usize = 64 * 1024 * 1024;
#[derive(Debug, Serialize, Deserialize)]
pub struct Reply {
    pub version: u32,
    pub code: u32,
    pub data: Value,
}
impl Reply {
    pub fn ok(data: Value) -> Self {
        Self {
            version: 1,
            code: 0,
            data,
        }
    }
    pub fn error(code: u32, message: impl ToString) -> Self {
        Self {
            version: 1,
            code,
            data: json!({"error":message.to_string()}),
        }
    }
}
/// Stable policy codes 100..117; never depend on Rust discriminants.
pub fn policy_code(e: Error) -> u32 {
    100 + match e {
        Error::Authentication => 0,
        Error::InvalidGrant => 1,
        Error::TaskExists => 2,
        Error::UnknownTask => 3,
        Error::Revoked => 4,
        Error::Expired => 5,
        Error::UncertainTime => 6,
        Error::TimeRegression => 7,
        Error::Scope => 8,
        Error::MissingRow => 9,
        Error::DuplicateKey => 10,
        Error::InvalidPredicate => 11,
        Error::Budget => 12,
        Error::ApprovalRequired => 13,
        Error::InvalidApproval => 14,
        Error::StalePreview => 15,
        Error::IdConflict => 16,
        Error::Overflow => 17,
    }
}
pub fn failure(e: CoreError) -> Reply {
    let code = match &e {
        CoreError::Policy(e) => policy_code(*e),
        CoreError::Storage(_) => 10,
        CoreError::Corrupt(_) => 11,
        CoreError::Unsupported => 12,
        CoreError::Exists => 13,
        CoreError::Busy => 14,
        CoreError::Fenced => 15,
        CoreError::Limit => 16,
    };
    Reply::error(code, e)
}
fn result<T: Serialize>(r: Result<T, CoreError>) -> Reply {
    match r {
        Ok(v) => match serde_json::to_value(v) {
            Ok(v) => Reply::ok(v),
            Err(e) => Reply::error(17, e),
        },
        Err(e) => failure(e),
    }
}
fn decision(d: Result<Decision, CoreError>) -> Reply {
    match d {
        Ok(Ok(r)) => result(Ok(r)),
        Ok(Err(e)) => failure(CoreError::Policy(e)),
        Err(e) => failure(e),
    }
}
#[derive(Deserialize, Serialize)]
#[serde(tag = "command", rename_all = "snake_case", deny_unknown_fields)]
pub enum AdminCommand {
    Seed {
        key: Key,
        row: Row,
    },
    Grant {
        grant: Grant,
    },
    Approve {
        principal: Principal,
        preview: String,
        time: Time,
    },
    Revoke {
        task: TaskId,
    },
    RevokeApproval {
        approval: ApprovalId,
    },
    Inspect,
    Export {
        path: String,
    },
    Metadata,
}
#[derive(Deserialize, Serialize)]
#[serde(tag = "command", rename_all = "snake_case", deny_unknown_fields)]
pub enum ExecutorCommand {
    Preview {
        request: Request,
        time: Time,
    },
    Execute {
        request: Request,
        time: Time,
        approval: Option<ApprovalId>,
    },
    Sql {
        operation: OperationId,
        sql: String,
        time: Time,
        approval: Option<ApprovalId>,
    },
    Status {
        operation: OperationId,
    },
}
fn parse<T: for<'de> Deserialize<'de>>(s: &[u8]) -> Result<T, Reply> {
    if s.len() > MAX_INPUT {
        return Err(Reply::error(16, "input limit"));
    }
    serde_json::from_slice(s).map_err(|e| Reply::error(17, e))
}
fn token(p: &Preview) -> Result<String, CoreError> {
    let bytes = bincode::serialize(p).map_err(|e| CoreError::Corrupt(e.to_string()))?;
    // Approval input contains hex, so reserve JSON wrapper space too.
    if bytes.len() > (MAX_INPUT - 1024) / 2 {
        return Err(CoreError::Limit);
    }
    Ok(bytes.iter().map(|b| format!("{b:02x}")).collect())
}
fn untoken(s: &str) -> Result<Preview, CoreError> {
    if s.len() > MAX_INPUT || !s.len().is_multiple_of(2) {
        return Err(CoreError::Limit);
    }
    let bytes = s
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|b| {
            let h = (b[0] as char).to_digit(16).ok_or(CoreError::Unsupported)?;
            let l = (b[1] as char).to_digit(16).ok_or(CoreError::Unsupported)?;
            Ok((h * 16 + l) as u8)
        })
        .collect::<Result<Vec<_>, CoreError>>()?;
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_limit(MAX_INPUT as u64)
        .reject_trailing_bytes()
        .deserialize(&bytes)
        .map_err(|e| CoreError::Corrupt(e.to_string()))
}
pub fn admin(a: &Admin, input: &[u8]) -> Reply {
    let c = match parse(input) {
        Ok(c) => c,
        Err(e) => return e,
    };
    match c {
        AdminCommand::Seed { key, row } => result(a.put(key, row)),
        AdminCommand::Grant { grant } => result(a.grant(grant).map(|_| ())),
        AdminCommand::Approve {
            principal,
            preview,
            time,
        } => result(untoken(&preview).and_then(|p| a.approve(principal, p, time))),
        AdminCommand::Revoke { task } => result(a.revoke(task)),
        AdminCommand::RevokeApproval { approval } => result(a.revoke_approval(approval)),
        AdminCommand::Export { path } => result(a.export(path)),
        AdminCommand::Metadata => match a.authority_metadata() {
            Ok((domain, last_time, next_approval, origin)) => Reply::ok(
                json!({"domain":domain.to_string(),"last_time":last_time,"next_approval":next_approval,"origin":origin.map(|v|v.to_string()),"format":1,"durability":"immediate","engine":"redb"}),
            ),
            Err(e) => failure(e),
        },
        AdminCommand::Inspect => match a.snapshot() {
            Ok(s) => Reply::ok(
                json!({"domain":s.domain.to_string(),"schema":s.schema,"rows":s.rows.into_iter().collect::<Vec<_>>(),"tasks":s.tasks.into_iter().collect::<Vec<_>>(),"operations":s.operations.into_iter().collect::<Vec<_>>(),"approvals":s.approvals.into_iter().map(|(id,p)|(id,p.revoked,p.consumed)).collect::<Vec<_>>()}),
            ),
            Err(e) => failure(e),
        },
    }
}
pub fn executor(e: &Executor, input: &[u8]) -> Reply {
    let c = match parse(input) {
        Ok(c) => c,
        Err(e) => return e,
    };
    match c {
        ExecutorCommand::Preview { request, time } => match e
            .preview(&request, time)
            .and_then(|p| Ok((token(&p)?, p.effects)))
        {
            Ok((preview, effects)) => Reply::ok(json!({"preview":preview,"effects":effects})),
            Err(e) => failure(e),
        },
        ExecutorCommand::Execute {
            request,
            time,
            approval,
        } => decision(e.execute(request, time, approval)),
        ExecutorCommand::Sql {
            operation,
            sql,
            time,
            approval,
        } => decision(e.execute_sql(operation, &sql, time, approval)),
        ExecutorCommand::Status { operation } => match e.status(operation) {
            Ok(OperationStatus::Absent) => Reply {
                version: 1,
                code: 1,
                data: json!({"status":"absent"}),
            },
            Ok(OperationStatus::Decided(d)) => decision(Ok(d)),
            Err(e) => failure(e),
        },
    }
}
