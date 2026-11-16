//! Durable transaction routines for v2 SQL sessions.
use super::*;
use engine::xact::{XactLookupError, XactStatus};
use relational::Value;

/// `txn` is the statement's transaction: on a cluster gateway an xid is registered for it
/// (R5-55) and cannot be assigned without one.
pub(super) fn callback(
    db: Arc<v2::V2Db>,
    slot: XactSlot,
    txn: Option<engine::locks::TxnId>,
) -> relational::clock::XactCallback {
    Arc::new(move |name, arg| match name {
        "pg_current_xact_id" | "pg_current_xact_id_if_assigned" => {
            let mut slot = slot.lock().map_err(|_| CoreError::Fenced)?;
            if name == "pg_current_xact_id" && slot.is_none() {
                let guard = db.xacts.assign()?;
                if let Some(cluster) = db.cluster_services() {
                    // On failure the guard drops here: the xid is never shown.
                    cluster.register_xact(txn.ok_or(CoreError::Unsupported)?, guard.id())?;
                }
                *slot = Some(guard);
            }
            Ok(slot
                .as_ref()
                .map_or(Value::Null, |guard| Value::Int(guard.id() as i64)))
        }
        "pg_xact_status" => {
            let xid = arg.ok_or(CoreError::Sql(SqlError::Parameters))? as u64;
            status_value(db.xacts.status(xid).or_else(|error| {
                match (error, db.cluster_services()) {
                    // Not live on this gateway: ask the system range (R5-55).
                    (XactLookupError::Cluster, Some(cluster)) => {
                        cluster.xact_status(xid).map_err(XactLookupError::Txn)
                    }
                    (error, _) => Err(error),
                }
            }))
        }
        _ => Err(CoreError::Sql(SqlError::Unsupported)),
    })
}

fn status_value(
    status: Result<Option<XactStatus>, XactLookupError>,
) -> Result<Value, CoreError> {
    match status {
        Ok(status) => Ok(match status {
            Some(XactStatus::Committed) => Value::Text("committed".into()),
            Some(XactStatus::Aborted) => Value::Text("aborted".into()),
            Some(XactStatus::InProgress) => Value::Text("in progress".into()),
            None => Value::Null,
        }),
        Err(XactLookupError::Future) => Err(CoreError::Sql(SqlError::InvalidParameterValue)),
        Err(XactLookupError::Txn(error)) => Err(CoreError::from(error)),
        Err(XactLookupError::Cluster) => Err(CoreError::Unsupported),
    }
}
