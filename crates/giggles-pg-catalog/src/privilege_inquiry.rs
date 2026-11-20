//! Bounded privilege inquiries over a freshly authorized catalog snapshot.
use crate::{CatalogSnapshot, Error, Value};
/// Evaluates only the PostgreSQL privilege inquiry family. NULL is strict.
/// The caller supplies effective and session identity after authorization.
pub fn evaluate(name: &str, args: &[Value], effective_role: &str, session_user: &str,
    snapshot: &CatalogSnapshot) -> Result<Option<bool>, Error> {
    if !matches!(name, "pg_has_role" | "has_table_privilege" | "has_column_privilege"
        | "has_any_column_privilege" | "has_sequence_privilege" | "has_schema_privilege"
        | "has_database_privilege" | "has_type_privilege") { return Err(Error::Unsupported); }
    let arity = if name == "has_column_privilege" {3} else {2};
    if args.len() != arity && args.len() != arity+1 { return Err(Error::Type); }
    if args.iter().any(|v| !matches!(v,Value::Null | Value::Text(_) | Value::Int(_))) {return Err(Error::Type);}
    // These functions depend on current_user, independently of session_user.
    let _ = session_user;
    crate::engine::evaluate_privilege(name,args,effective_role,snapshot)
}
