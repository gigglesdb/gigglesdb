//! Application-authorized typed execution on the existing retained Session.
use super::codec::Invocation;
use crate::{Role, Session};
use giggles_core::{CoreError, Error, operations as ops, relational::SqlError};

pub(crate) enum NativeResult {
    Control,
    Operation(ops::Outcome),
    Transaction(ops::TransactionOutcome),
    Error(ops::Error),
}

fn authorize_request(
    request: &ops::Request,
    tables: &std::collections::BTreeSet<String>,
) -> Result<(), CoreError> {
    let mut request = request;
    for _ in 0..=2 {
        request = match request {
            ops::Request::Returning { request, .. } | ops::Request::OnConflict { request, .. } => {
                request
            }
            ops::Request::Read { table, .. }
            | ops::Request::Insert { table, .. }
            | ops::Request::Update { table, .. }
            | ops::Request::Delete { table, .. }
            | ops::Request::UpdateExpressions { table, .. }
            | ops::Request::ReadExpressions { table, .. }
            | ops::Request::InsertExpressions { table, .. }
            | ops::Request::InsertMany { table, .. } => {
                return if table != "giggles_join" && tables.contains(table) {
                    Ok(())
                } else {
                    Err(CoreError::Policy(Error::Scope))
                };
            }
        };
    }
    Err(CoreError::Limit)
}

pub(crate) fn run(
    session: &mut Session,
    invocation: Invocation,
) -> Result<NativeResult, CoreError> {
    let result = (|| {
        session.check_binding()?;
        let Role::Application { domain, tables } = &session.role else {
            return Err(CoreError::Policy(Error::Scope));
        };
        // Traverse all targets before consulting any catalog or executing an
        // earlier block step. Expressions cannot introduce additional tables.
        match &invocation {
            Invocation::Operation { request, .. } => authorize_request(request, tables)?,
            Invocation::Transaction { transaction, .. } => {
                if transaction.steps.len() > ops::MAX_TRANSACTION_ITEMS {
                    return Err(CoreError::Limit);
                }
                for step in &transaction.steps {
                    if let ops::TransactionStep::Execute(request)
                    | ops::TransactionStep::Let { request, .. } = step
                    {
                        authorize_request(request, tables)?;
                    }
                }
            }
            Invocation::Begin | Invocation::Commit | Invocation::Rollback => {}
            Invocation::Legacy(_) | Invocation::Text { .. } => return Err(CoreError::Unsupported),
        }
        if session.service.admin.authority_metadata()?.0.to_string() != *domain {
            return Err(CoreError::Policy(Error::Authentication));
        }
        if session.is_aborted() && !matches!(invocation, Invocation::Rollback) {
            return Err(CoreError::Sql(SqlError::Aborted));
        }
        let result = match invocation {
            Invocation::Operation { request, bindings } => session
                .sql
                .execute_native(&request, &bindings)
                .map(NativeResult::Operation),
            Invocation::Transaction {
                transaction,
                bindings,
            } => session
                .sql
                .execute_native_transaction(&transaction, &bindings)
                .map(NativeResult::Transaction),
            Invocation::Begin => {
                session.sql.begin_native()?;
                return Ok(NativeResult::Control);
            }
            Invocation::Commit => {
                session.sql.commit_native()?;
                return Ok(NativeResult::Control);
            }
            Invocation::Rollback => {
                session.sql.rollback_native()?;
                return Ok(NativeResult::Control);
            }
            Invocation::Legacy(_) | Invocation::Text { .. } => unreachable!(),
        };
        match result {
            Ok(outcome) => Ok(outcome),
            Err(error) => {
                // Keep durable ambiguity dominant. Never inspect Core(String).
                if error != ops::Error::Unknown {
                    giggles_core::cancellation::check()?;
                }
                Ok(NativeResult::Error(error))
            }
        }
    })();
    if result.is_err() {
        session.abort();
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Binding, Service, clock::Clock};
    use giggles_core::{Admin, Limits, roles};

    #[test]
    fn database_binding_refuses_native_operations_and_controls_before_binding() {
        let path = crate::test_support::fixture_root().join(format!(
            "database-native-binding-{}-{}", std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        std::fs::create_dir_all(&path).unwrap();
        let admin = Admin::create(path.join("db.redb"), Limits::default()).unwrap();
        admin.sql_session().execute("CREATE TABLE app(id BIGINT PRIMARY KEY)", &[], None).unwrap();
        admin.roles().create(roles::NewRole {
            name: "bound".into(), login: true, superuser: true,
            ..roles::NewRole::default()
        }).unwrap();
        let fingerprint = "11".repeat(32);
        let service = Service::new(admin, vec![Binding {
            fingerprint: fingerprint.clone(), role: Role::Database { name: "bound".into() }
        }], Clock::new(0, 3600).unwrap()).unwrap();
        let mut session = service.session(&fingerprint).unwrap();
        for table in ["app", "missing"] {
            let invocation = Invocation::Operation {
                request: ops::Request::Read {
                    table: table.into(), filter: None, order: vec![], limit: None, projection: vec![]
                },
                bindings: ops::Bindings::new(),
            };
            assert!(matches!(run(&mut session, invocation), Err(CoreError::Policy(Error::Scope))));
        }
        for invocation in [Invocation::Begin, Invocation::Commit, Invocation::Rollback] {
            assert!(matches!(run(&mut session, invocation), Err(CoreError::Policy(Error::Scope))));
        }
        service.reload_bindings(vec![]).unwrap();
        assert!(matches!(run(&mut session, Invocation::Begin), Err(CoreError::Policy(Error::Authentication))));
    }
}
