//! Version-1 trusted local JSON protocol. Never expose admin dispatch over RPC.
use crate::*;
#[cfg(feature = "legacy-task")]
use bincode::Options;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
pub const MAX_INPUT: usize = 1024 * 1024;
pub const MAX_OUTPUT: usize = 64 * 1024 * 1024;
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SqlInput {
    pub sql: String,
    #[serde(default)]
    pub params: Vec<relational::Value>,
    pub expected_version: Option<u64>,
}
pub fn sql_session(session: &mut SqlSession, input: &[u8]) -> Reply {
    match parse::<SqlInput>(input) {
        Ok(s) => result(session.execute(&s.sql, &s.params, s.expected_version)),
        Err(e) => e,
    }
}
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
        CoreError::PrivilegeInquiry(_) | CoreError::SqlReplyEncoding(_) => 12,
        CoreError::Policy(e) => policy_code(*e),
        CoreError::SequencePrivilege => policy_code(Error::Scope),
        CoreError::Storage(_) => 10,
        CoreError::Corrupt(_) => 11,
        CoreError::Unsupported | CoreError::EngineUnsupported { .. } => 12,
        CoreError::Exists => 13,
        CoreError::Busy => 14,
        CoreError::LockNotAvailable => 20,
        CoreError::Deadlock => 19,
        CoreError::Fenced => 15,
        // Next free code in the CoreError band (10..=22).
        CoreError::WriteFenced { .. } => 22,
        CoreError::Limit => 16,
        CoreError::Cancelled => 17,
        CoreError::DuplicateSchema => 18,
        CoreError::Task(_) => 20,
        CoreError::TempFile { .. } => 21,
        CoreError::Sql(e) => {
            200 + match e {
                relational::SqlError::Unsupported
                | relational::SqlError::TextSearchUnsupported => 0,
                relational::SqlError::Syntax
                | relational::SqlError::TextSearchSyntax
                | relational::SqlError::TextSearchInvalidName => 1,
                relational::SqlError::Reserved => 2,
                relational::SqlError::UnknownTable => 3,
                relational::SqlError::UnknownColumn => 4,
                relational::SqlError::Duplicate => 5,
                relational::SqlError::Type
                | relational::SqlError::TextSearchDatatypeMismatch => 6,
                relational::SqlError::Null => 7,
                relational::SqlError::Parameters
                | relational::SqlError::TextSearchProtocolViolation => 8,
                relational::SqlError::Limit
                | relational::SqlError::TextSearchProgramLimit
                | relational::SqlError::TextSearchStatementTooComplex => 9,
                relational::SqlError::Unbounded => 10,
                relational::SqlError::Version => 11,
                relational::SqlError::Conflict => 12,
                relational::SqlError::StaleSchema => 13,
                relational::SqlError::Transaction => 14,
                relational::SqlError::Aborted => 15,
                relational::SqlError::Overflow
                | relational::SqlError::TextSearchNumericOutOfRange => 16,
                relational::SqlError::UndefinedFunction
                | relational::SqlError::TextSearchUndefinedFunction => 17,
                relational::SqlError::InvalidParameterValue
                | relational::SqlError::TextSearchInvalidParameter
                | relational::SqlError::TextSearchZeroLength
                | relational::SqlError::TextSearchInvalidSubscript => 18,
                relational::SqlError::InvalidDatetimeFormat => 19,
                relational::SqlError::DatetimeFieldOverflow => 20,
                relational::SqlError::IntervalFieldOverflow => 21,
                relational::SqlError::AmbiguousFunction
                | relational::SqlError::TextSearchAmbiguousFunction => 22,
                relational::SqlError::GroupingError => 23,
                relational::SqlError::IndeterminateDatatype => 24,
                relational::SqlError::AmbiguousParameter => 25,
                relational::SqlError::InvalidTextRepresentation
                | relational::SqlError::TextSearchInvalidText
                | relational::SqlError::TextSearchInvalidBinary => 26,
                relational::SqlError::SubstringError => 27,
                relational::SqlError::CharacterNotInRepertoire
                | relational::SqlError::TextSearchInvalidEncoding => 28,
                relational::SqlError::DuplicateTable => 29,
                relational::SqlError::DuplicateColumn => 30,
                relational::SqlError::DuplicateObject => 31,
                relational::SqlError::UndefinedObject
                | relational::SqlError::TextSearchUndefinedObject => 32,
                relational::SqlError::CheckViolation => 33,
                relational::SqlError::ForeignKeyViolation => 34,
                relational::SqlError::DependentObjects => 35,
                relational::SqlError::GeneratedAlways => 36,
                relational::SqlError::ObjectNotInPrerequisiteState => 37,
                relational::SqlError::SequenceLimit => 38,
                relational::SqlError::NoActiveTransaction => 39,
                relational::SqlError::InvalidSavepoint => 40,
                relational::SqlError::InvalidForeignKey => 41,
                relational::SqlError::InvalidSchema
                | relational::SqlError::TextSearchInvalidSchema => 42,
                relational::SqlError::DivisionByZero => 43,
                relational::SqlError::InvalidEscapeSequence => 44,
                relational::SqlError::InvalidTableDefinition => 45,
                relational::SqlError::RestrictViolation => 46,
                relational::SqlError::WrongObjectType => 47,
                relational::SqlError::NullValueNotAllowed
                | relational::SqlError::TextSearchNullValue => 48,
                relational::SqlError::CannotCoerce => 49,
                relational::SqlError::BadCopyFileFormat => 50,
                relational::SqlError::StringDataRightTruncation => 51,
                relational::SqlError::InvalidTimeZoneDisplacementValue => 52,
                relational::SqlError::AmbiguousColumn => 53,
                relational::SqlError::InvalidRowCountInLimitClause => 54,
                relational::SqlError::InvalidRowCountInResultOffsetClause => 55,
                relational::SqlError::CardinalityViolation => 56,
                relational::SqlError::InvalidColumnReference => 57,
                relational::SqlError::InvalidRecursion => 58,
                relational::SqlError::DuplicateAlias => 59,
                relational::SqlError::WindowingError => 60,
                relational::SqlError::InvalidFrameOffset => 61,
                relational::SqlError::InvalidNtileArgument => 62,
                relational::SqlError::InvalidNthValueArgument => 63,
                relational::SqlError::CollationMismatch => 64,
                relational::SqlError::IndeterminateCollation => 65,
                relational::SqlError::InvalidTablesampleArgument => 66,
                relational::SqlError::InvalidArgumentForLogarithm => 67,
                relational::SqlError::InvalidArgumentForPowerFunction => 68,
                relational::SqlError::InvalidArgumentForWidthBucket => 70,
                relational::SqlError::InternalError | relational::SqlError::TextSearchInternal => 69,
                relational::SqlError::InvalidIndexPredicate => 71,
                relational::SqlError::UndefinedParameter => 72,
                relational::SqlError::InvalidName => 73,
                relational::SqlError::UnsafeNewEnumValue => 74,
                relational::SqlError::InvalidObjectDefinition => 76,
            }
        }
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
#[cfg(feature = "legacy-task")]
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
    #[cfg(feature = "legacy-task")]
    ProvisionTaskResources {
        task: TaskId,
        limits: TaskResources,
    },
    #[cfg(feature = "legacy-task")]
    TaskResources {
        task: TaskId,
    },
    Sql {
        sql: String,
        params: Vec<relational::Value>,
        expected_version: Option<u64>,
    },
    SqlBatch {
        statements: Vec<SqlInput>,
    },
    Catalog,
    Seed {
        key: Key,
        row: Row,
    },
    #[cfg(feature = "legacy-task")]
    Grant {
        grant: Grant,
    },
    #[cfg(feature = "legacy-task")]
    Approve {
        principal: Principal,
        preview: String,
        time: Time,
    },
    #[cfg(feature = "legacy-task")]
    Revoke {
        task: TaskId,
    },
    #[cfg(feature = "legacy-task")]
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
#[cfg(feature = "legacy-task")]
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
#[cfg(feature = "legacy-task")]
fn token(p: &Preview) -> Result<String, CoreError> {
    let bytes = bincode::serialize(p).map_err(|e| CoreError::Corrupt(e.to_string()))?;
    // Approval input contains hex, so reserve JSON wrapper space too.
    if bytes.len() > (MAX_INPUT - 1024) / 2 {
        return Err(CoreError::Limit);
    }
    Ok(bytes.iter().map(|b| format!("{b:02x}")).collect())
}
#[cfg(feature = "legacy-task")]
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
        AdminCommand::Sql {
            sql,
            params,
            expected_version,
        } => {
            let mut session = a.sql_session();
            let r = session.execute(&sql, &params, expected_version);
            if r.is_ok() && session.in_transaction() {
                return failure(CoreError::Sql(relational::SqlError::Transaction));
            }
            result(r)
        }
        AdminCommand::SqlBatch { statements } => {
            if statements.len() > 256 {
                return Reply::error(16, "batch limit");
            }
            let mut session = a.sql_session();
            let mut results = Vec::new();
            for s in statements {
                match session.execute(&s.sql, &s.params, s.expected_version) {
                    Ok(r) => results.push(r),
                    Err(e) => return failure(e),
                }
            }
            if session.in_transaction() {
                return failure(CoreError::Sql(relational::SqlError::Transaction));
            }
            result(Ok(results))
        }
        AdminCommand::Catalog => result(a.catalog()),
        #[cfg(feature = "legacy-task")]
        AdminCommand::ProvisionTaskResources { task, limits } => {
            result(a.provision_task_resources(task, limits))
        }
        #[cfg(feature = "legacy-task")]
        AdminCommand::TaskResources { task } => result(a.task_resources(task)),
        AdminCommand::Seed { key, row } => result(a.put(key, row)),
        #[cfg(feature = "legacy-task")]
        AdminCommand::Grant { grant } => result(a.grant(grant).map(|_| ())),
        #[cfg(feature = "legacy-task")]
        AdminCommand::Approve {
            principal,
            preview,
            time,
        } => result(untoken(&preview).and_then(|p| a.approve(principal, p, time))),
        #[cfg(feature = "legacy-task")]
        AdminCommand::Revoke { task } => result(a.revoke(task)),
        #[cfg(feature = "legacy-task")]
        AdminCommand::RevokeApproval { approval } => result(a.revoke_approval(approval)),
        AdminCommand::Export { path } => result(a.export(path)),
        AdminCommand::Metadata => match a.authority_metadata() {
            Ok((domain, last_time, next_approval, origin)) => {
                let mut data = json!({"domain":domain.to_string(),"last_time":last_time,"next_approval":next_approval,"origin":origin.map(|v|v.to_string()),"write_format":3,"durability":"immediate","engine":"redb"});
                if a.engine() == Engine::V2 {
                    // V2 uses family/subversion metadata, not a legacy write format.
                    data.as_object_mut()
                        .expect("metadata object")
                        .remove("write_format");
                    data["engine"] = json!("v2");
                    data["durability"] = json!("sync");
                }
                Reply::ok(data)
            }
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
#[cfg(feature = "legacy-task")]
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
