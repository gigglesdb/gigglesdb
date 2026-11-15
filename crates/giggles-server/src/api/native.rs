//! Authorized typed execution on the existing retained Session.
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

// Collect syntactic reads before binding, at the same step as SQL authorization.
fn reads_columns(expr: &ops::Expr, depth: usize) -> Result<bool, CoreError> {
    if depth > ops::MAX_DEPTH { return Err(CoreError::Limit); }
    let read = |expr: &ops::Expr| reads_columns(expr, depth + 1);
    Ok(match expr {
        ops::Expr::Field(_) | ops::Expr::ConflictField { .. } => true,
        ops::Expr::Value(_) | ops::Expr::Local(_) => false,
        ops::Expr::Compare(a, _, b) | ops::Expr::And(a, b) | ops::Expr::Or(a, b)
        | ops::Expr::Arithmetic(a, _, b) => read(a)? | read(b)?,
        ops::Expr::IsNull(a, _) | ops::Expr::Not(a) | ops::Expr::Negate(a) => read(a)?,
        ops::Expr::Call { arguments, .. } => {
            let mut found = false;
            for arg in arguments { found |= read(arg)?; }
            found
        }
        ops::Expr::Case { operand, arms, else_expr } => {
            let mut found = operand.as_deref().map(read).transpose()?.unwrap_or(false);
            for (condition, value) in arms { found |= read(condition)? | read(value)?; }
            found | else_expr.as_deref().map(read).transpose()?.unwrap_or(false)
        }
    })
}

fn table_requirements(
    request: &ops::Request,
) -> Result<Vec<(String, giggles_core::roles::TablePrivilege)>, CoreError> {
    use giggles_core::roles::TablePrivilege::{Delete, Insert, Select, Update};
    let mut request = request;
    let (mut select, mut update) = (false, false);
    for _ in 0..=2 {
        let (table, privilege) = match request {
            ops::Request::Returning { request: inner, projection } => {
                select |= projection.is_empty();
                for item in projection { select |= reads_columns(&item.expression, 0)?; }
                request = inner;
                continue;
            }
            ops::Request::OnConflict { request: inner, target, action } => {
                select |= !target.is_empty();
                if let ops::ConflictAction::Update { guard, values } = action {
                    update = true;
                    if let Some(guard) = guard { select |= reads_columns(guard, 0)?; }
                    for (_, value) in values { select |= reads_columns(value, 0)?; }
                }
                request = inner;
                continue;
            }
            ops::Request::Read { table, .. } | ops::Request::ReadExpressions { table, .. } => {
                (table, Select)
            }
            ops::Request::Insert { table, .. } | ops::Request::InsertMany { table, .. }
            | ops::Request::InsertExpressions { table, .. } => (table, Insert),
            ops::Request::Update { table, filter, guard, .. }
            | ops::Request::UpdateExpressions { table, filter, guard, .. }
            | ops::Request::Delete { table, filter, guard, .. } => {
                select |= reads_columns(filter, 0)?;
                if let Some(guard) = guard { select |= reads_columns(guard, 0)?; }
                if let ops::Request::UpdateExpressions { values, .. } = request {
                    for (_, value) in values { select |= reads_columns(value, 0)?; }
                }
                (table, if matches!(request, ops::Request::Delete { .. }) { Delete } else { Update })
            }
        };
        let mut reqs = vec![(table.clone(), privilege)];
        if select && privilege != Select { reqs.push((table.clone(), Select)); }
        if update { reqs.push((table.clone(), Update)); }
        return Ok(reqs);
    }
    Err(CoreError::Limit)
}

pub(crate) fn run(
    session: &mut Session,
    invocation: Invocation,
) -> Result<NativeResult, CoreError> {
    let result = (|| {
        session.check_binding()?;
        if matches!(&session.role, Role::Database { .. }) {
            let mut reqs = Vec::new();
            match &invocation {
                Invocation::Operation { request, .. } => reqs.extend(table_requirements(request)?),
                Invocation::Transaction { transaction, .. } => {
                    if transaction.steps.len() > ops::MAX_TRANSACTION_ITEMS {
                        return Err(CoreError::Limit);
                    }
                    for step in &transaction.steps {
                        if let ops::TransactionStep::Execute(request)
                        | ops::TransactionStep::Let { request, .. } = step
                        {
                            reqs.extend(table_requirements(request)?);
                        }
                    }
                }
                Invocation::Begin | Invocation::Commit | Invocation::Rollback => {}
                Invocation::Legacy(_) | Invocation::Text { .. } => return Err(CoreError::Unsupported),
            }
            session.authorize_table_requirements(&reqs)?;
        } else {
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
    fn native_ast_collects_column_reads_and_conflict_updates() {
        use giggles_core::roles::TablePrivilege::{Delete, Insert, Select, Update};
        let path = crate::test_support::fixture_root().join(format!("native-grant-reads-{}", std::process::id()));
        std::fs::create_dir_all(&path).unwrap();
        let admin = Admin::create(path.join("db.redb"), Limits::default()).unwrap();
        admin.sql_session().execute("CREATE TABLE app(id BIGINT PRIMARY KEY, body BIGINT)", &[], None).unwrap();
        admin.sql_session().execute("INSERT INTO app VALUES(1,1)", &[], None).unwrap();
        admin.roles().create(roles::NewRole {
            name: "bound".into(), login: true, ..roles::NewRole::default()
        }).unwrap();
        let fingerprint = "22".repeat(32);
        let service = Service::new(admin, vec![Binding {
            fingerprint: fingerprint.clone(), role: Role::Database { name: "bound".into() }
        }], Clock::new(0, 3600).unwrap()).unwrap();
        for (source, expected) in [
            ("update app where true set body=1+1", vec![Update]),
            ("update app where id=1 set body=1", vec![Update, Select]),
            ("update app where true set body=body+1", vec![Update, Select]),
            ("delete app where true", vec![Delete]),
            ("delete app where id=1", vec![Delete, Select]),
            ("insert app set id=2 return 1 as n", vec![Insert]),
            ("update app where true set body=1 return 1 as n", vec![Update]),
            ("delete app where true return 1 as n", vec![Delete]),
            ("insert app set id=2 return id", vec![Insert, Select]),
            ("update app where true set body=1 return body", vec![Update, Select]),
            ("delete app where true return *", vec![Delete, Select]),
            ("insert into app(id) values(2) on conflict(id) do nothing", vec![Insert, Select]),
            ("insert into app(id) values(2) on conflict(id) do update set body=1", vec![Insert, Select, Update]),
            ("insert into app(id) values(2) on conflict(id) do update set body=incoming.body", vec![Insert, Select, Update]),
            ("update app where true when body=1 set body=2", vec![Update, Select]),
        ] {
            let giggles_language::Lowered::Operation { request, .. } = giggles_language::lower_text(
                source, &ops::Bindings::new(), giggles_language::LanguageVersion::Embedded016,
            ).unwrap_or_else(|error| panic!("{source}: {error:?}")) else { panic!("operation expected") };
            assert_eq!(table_requirements(&request).unwrap(), expected.iter().copied()
                .map(|privilege| ("app".into(), privilege)).collect::<Vec<_>>(), "{source}");
            let roles = service.admin.roles();
            roles.grant_table("app", "bound", &expected).unwrap();
            let invocation = || Invocation::Operation { request: request.clone(), bindings: ops::Bindings::new() };
            for missing in &expected {
                roles.revoke_table("app", "bound", &[*missing]).unwrap();
                let mut session = service.session(&fingerprint).unwrap();
                assert!(matches!(run(&mut session, invocation()), Err(CoreError::Policy(Error::Scope))), "{source}: {missing:?}");
                roles.grant_table("app", "bound", &[*missing]).unwrap();
            }
            let mut session = service.session(&fingerprint).unwrap();
            run(&mut session, Invocation::Begin).unwrap();
            assert!(matches!(run(&mut session, invocation()).unwrap(), NativeResult::Operation(_)), "{source}");
            run(&mut session, Invocation::Rollback).unwrap();
            roles.revoke_table("app", "bound", &expected).unwrap();
        }
    }

    #[test]
    fn database_binding_checks_fences_before_native_access() {
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
            let result = run(&mut session, invocation).unwrap();
            if table == "app" {
                assert!(matches!(result, NativeResult::Operation(_)));
            } else {
                assert!(matches!(result, NativeResult::Error(ops::Error::Bind(_))));
                assert_eq!(session.execute_sql("SELECT id FROM missing", &[]).unwrap_err(),
                    CoreError::Sql(SqlError::UnknownTable));
            }
        }
        for invocation in [Invocation::Begin, Invocation::Commit, Invocation::Begin, Invocation::Rollback] {
            assert!(run(&mut session, invocation).is_ok());
        }
        service.reload_bindings(vec![]).unwrap();
        assert!(matches!(run(&mut session, Invocation::Begin), Err(CoreError::Policy(Error::Authentication))));
        service.reload_bindings(vec![Binding {
            fingerprint: fingerprint.clone(), role: Role::Database { name: "bound".into() }
        }]).unwrap();
        let mut dropped = service.session(&fingerprint).unwrap();
        service.admin.roles().drop("bound").unwrap();
        assert!(matches!(run(&mut dropped, Invocation::Begin), Err(CoreError::Policy(Error::Authentication))));
    }
}
