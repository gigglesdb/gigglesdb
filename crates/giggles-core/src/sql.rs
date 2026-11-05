//! Deliberately finite SQL-shaped repair grammar. No general SQL compatibility.
use crate::*;
fn number(s: &str) -> Result<u64, CoreError> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return Err(CoreError::Unsupported);
    }
    s.parse().map_err(|_| CoreError::Unsupported)
}
fn status(s: &str) -> Result<Status, CoreError> {
    match s {
        "'failed'" => Ok(Status::Failed),
        "'queued'" => Ok(Status::Queued),
        "'succeeded'" => Ok(Status::Succeeded),
        _ => Err(CoreError::Unsupported),
    }
}
/// Keywords are ASCII case-insensitive; operators require whitespace. One
/// statement only, no semicolon/comments/expressions/parameters/coercion.
pub fn parse_repair(operation: OperationId, sql: &str) -> Result<Request, CoreError> {
    if sql.len() > 4096 {
        return Err(CoreError::Limit);
    }
    let lower = sql.to_ascii_lowercase();
    let tokens: Vec<_> = lower.split_ascii_whitespace().collect();
    let (after, selection) = match tokens.as_slice() {
        [
            "update",
            "deliveries",
            "set",
            "status",
            "=",
            after,
            "where",
            "tenant",
            "=",
            tenant,
            "and",
            "id",
            "=",
            id,
        ] => (
            status(after)?,
            Selection::Keys(vec![Key {
                tenant: TenantId(number(tenant)?),
                row: RowId(number(id)?),
            }]),
        ),
        [
            "update",
            "deliveries",
            "set",
            "status",
            "=",
            after,
            "where",
            "tenant",
            "=",
            tenant,
            "and",
            "status",
            "=",
            before,
            "and",
            "failed_at",
            ">=",
            from,
            "and",
            "failed_at",
            "<",
            until,
        ] => (
            status(after)?,
            Selection::Predicate {
                tenant: TenantId(number(tenant)?),
                status: status(before)?,
                from: number(from)?,
                until: number(until)?,
            },
        ),
        _ => return Err(CoreError::Unsupported),
    };
    Ok(Request {
        operation,
        selection,
        column: Column::Status,
        after,
    })
}
impl Executor {
    pub fn execute_sql(
        &self,
        operation: OperationId,
        sql: &str,
        time: Time,
        approval: Option<ApprovalId>,
    ) -> Result<Decision, CoreError> {
        self.execute(parse_repair(operation, sql)?, time, approval)
    }
}
