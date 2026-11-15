//! Durable transaction routines for v2 SQL sessions.
use super::*;
use engine::xact::{XactLookupError, XactStatus};
use relational::Value;

pub(super) fn callback(db: Arc<v2::V2Db>, slot: XactSlot) -> relational::clock::XactCallback {
    Arc::new(move |name, arg| match name {
        "pg_current_xact_id" | "pg_current_xact_id_if_assigned" => {
            let mut slot = slot.lock().map_err(|_| CoreError::Fenced)?;
            if name == "pg_current_xact_id" && slot.is_none() {
                *slot = Some(db.xacts.assign()?);
            }
            Ok(slot
                .as_ref()
                .map_or(Value::Null, |guard| Value::Int(guard.id() as i64)))
        }
        "pg_xact_status" => match db
            .xacts
            .status(arg.ok_or(CoreError::Sql(SqlError::Parameters))? as u64)
        {
            Ok(status) => Ok(match status {
                Some(XactStatus::Committed) => Value::Text("committed".into()),
                Some(XactStatus::Aborted) => Value::Text("aborted".into()),
                Some(XactStatus::InProgress) => Value::Text("in progress".into()),
                None => Value::Null,
            }),
            Err(XactLookupError::Future) => Err(CoreError::Sql(SqlError::InvalidParameterValue)),
            Err(XactLookupError::Txn(error)) => Err(CoreError::from(error)),
        },
        _ => Err(CoreError::Sql(SqlError::Unsupported)),
    })
}
