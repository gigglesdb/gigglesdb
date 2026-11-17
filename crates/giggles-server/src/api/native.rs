//! Authorized typed execution on the existing retained Session.
use super::codec::Invocation;
use crate::{Role, Session};
use giggles_core::{
    CoreError, Error, operations as ops,
    relational::SqlError,
    task::{Released, StatementClass},
};

/// A native result, ready to encode. Rows that came out of core are held as
/// `Released`: they went through the task release protocol (GD-0061 R3-313)
/// inside `run`, before any encoder sees them. `Sql` holds only rows the
/// server builds itself (search path, sequence value); `Page` holds a keyset
/// page, which a task session never reaches (`Session::read_native_page`).
pub(crate) enum NativeResult {
    Control,
    Catalog(ops::CatalogOutcome, String),
    SequenceCatalog(u64, String),
    Sql(Released<giggles_core::relational::SqlResult>),
    Operation(Released<ops::Outcome>),
    Page(ops::Page),
    Transaction(Released<ops::TransactionOutcome>),
    Error(ops::Error),
}

fn read_tables(invocation: &Invocation) -> Vec<String> {
    match invocation {
        Invocation::Read(r) => vec![r.table.clone()],
        Invocation::Join(r) => vec![r.left.table.clone(), r.right.table.clone()],
        _ => vec![],
    }
}
fn read_outcome(result: Result<Released<giggles_core::relational::SqlResult>, CoreError>) -> Result<NativeResult, CoreError> {
    let result = match result {
        Ok(result) => result,
        Err(error @ CoreError::Sql(_)) => {
            giggles_core::cancellation::check()?;
            return Ok(NativeResult::Error(ops::Error::Sql { sqlstate: crate::sqlstate(&error).into() }));
        }
        Err(error) => return Err(error),
    };
    Ok(NativeResult::Operation(result.map(|result| ops::Outcome {
        matched: result.rows.len(), columns: result.columns, rows: result.rows,
        ..ops::Outcome::default()
    })))
}

/// The read-log text of a native invocation in a task session: its request
/// as canonical JSON, as core logs native operations (GD-0061 R3-309).
fn statement_text(invocation: &Invocation) -> String {
    let json = match invocation {
        Invocation::Operation { request, .. } | Invocation::Page { request, .. } => {
            serde_json::to_string(request)
        }
        Invocation::Transaction { transaction, .. } => serde_json::to_string(transaction),
        Invocation::Read(read) => serde_json::to_string(read),
        Invocation::Join(read) => serde_json::to_string(read),
        Invocation::Catalog(request) => serde_json::to_string(request),
        Invocation::Sequence(call) => serde_json::to_string(call),
        Invocation::SearchPath(schemas) => serde_json::to_string(schemas),
        Invocation::Begin => Ok("BEGIN".into()),
        Invocation::Commit => Ok("COMMIT".into()),
        Invocation::Rollback => Ok("ROLLBACK".into()),
        Invocation::Savepoint(name) => Ok(format!("SAVEPOINT {name}")),
        Invocation::RollbackTo(name) => Ok(format!("ROLLBACK TO SAVEPOINT {name}")),
        Invocation::Release(name) => Ok(format!("RELEASE SAVEPOINT {name}")),
        Invocation::Legacy(_) | Invocation::Text { .. } => Ok(String::new()),
    };
    json.unwrap_or_default()
}

/// The number of bindings a native invocation carries.
fn binding_count(invocation: &Invocation) -> usize {
    match invocation {
        Invocation::Operation { bindings, .. }
        | Invocation::Page { bindings, .. }
        | Invocation::Transaction { bindings, .. } => bindings.len(),
        _ => 0,
    }
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

fn catalog_table(session: &Session, request: &ops::CatalogRequest) -> String {
    use ops::CatalogRequest as C;
    // Resolution precedes execution; execution owns validation and native error mapping.
    let key = match request {
        C::CreateTable { table, .. } | C::CreateView { name: table, .. } | C::CreateSequence { name: table } => {
            let schema = session.sql.native_creation_schema(table).unwrap_or_default();
            let name = table.split_once('.').map_or(table.as_str(), |(_, name)| name);
            return format!("{schema}.{name}");
        }
        C::DropIndex { name, .. } => session.sql.native_index_table(name).ok().flatten().unwrap_or_default(),
        C::CreateIndex { table, .. } | C::DropTable { table, .. } | C::AlterTable { table, .. }
        | C::AddConstraint { table, .. } | C::DropConstraint { table, .. } =>
            session.sql.native_relation_reference(table).unwrap_or_default(),
        C::DropView { name, .. } | C::DropSequence { name, .. } =>
            session.sql.native_relation_reference(name).unwrap_or_default(),
    };
    let (schema, name) = key.split_once('\0').unwrap_or(("public", &key));
    let name = match request {
        C::AlterTable { action: ops::AlterTableAction::RenameTable { to }, .. } => to,
        _ => name,
    };
    format!("{schema}.{name}")
}

/// The task statement class of a native request. A native transaction is an
/// explicit transaction, so it is `TransactionControl` as a whole.
fn task_class(invocation: &Invocation) -> Result<StatementClass, CoreError> {
    fn request_class(request: &ops::Request) -> StatementClass {
        match request {
            ops::Request::Read { .. } | ops::Request::ReadExpressions { .. } => {
                StatementClass::Select
            }
            ops::Request::Update { .. } | ops::Request::UpdateExpressions { .. } => {
                StatementClass::Update
            }
            ops::Request::Delete { .. } => StatementClass::Delete,
            ops::Request::Insert { .. }
            | ops::Request::InsertExpressions { .. }
            | ops::Request::InsertMany { .. }
            | ops::Request::OnConflict { .. } => StatementClass::Insert,
            ops::Request::Returning { request, .. } => match request.as_ref() {
                ops::Request::Returning { .. } => StatementClass::Other,
                inner => request_class(inner),
            },
        }
    }
    Ok(match invocation {
        Invocation::Operation { request, .. } | Invocation::Page { request, .. } => {
            request_class(request)
        }
        Invocation::Read(_) | Invocation::Join(_) => StatementClass::Select,
        Invocation::Transaction { .. }
        | Invocation::Begin
        | Invocation::Commit
        | Invocation::Rollback => StatementClass::TransactionControl,
        Invocation::Savepoint(_) | Invocation::RollbackTo(_) | Invocation::Release(_) => {
            StatementClass::Savepoint
        }
        Invocation::Catalog(_) => StatementClass::Ddl,
        Invocation::Sequence(_) => StatementClass::Other,
        Invocation::SearchPath(None) => StatementClass::Show,
        Invocation::SearchPath(Some(_)) => StatementClass::Set,
        Invocation::Legacy(_) | Invocation::Text { .. } => return Err(CoreError::Unsupported),
    })
}

pub(crate) fn run(
    session: &mut Session,
    invocation: Invocation,
) -> Result<NativeResult, CoreError> {
    let result = (|| {
        session.check_binding()?;
        // GD-0061 R3-208: a task session admits the class before any
        // resolution, authorization or execution, as on the agent pgwire path.
        // A refusal here never reaches core, so it is logged here, once
        // (GD-0061 R3-313; core logs every statement that reaches it).
        if session.task().is_some() {
            let class = task_class(&invocation)?;
            if let Err(error) = session.admit_backstop(class) {
                session.sql.log_refused_task_statement(
                    &statement_text(&invocation), class, binding_count(&invocation), &error,
                );
                return Err(error);
            }
        }
        if let Invocation::Catalog(request) = &invocation {
            return session.authority_scope(|session| {
                session.authorize_native_catalog(request)?;
                if session.is_aborted() { return Err(CoreError::Sql(SqlError::Aborted)); }
                let table = catalog_table(session, request);
                if matches!(**request, ops::CatalogRequest::CreateSequence { .. } | ops::CatalogRequest::DropSequence { .. }) {
                    return match session.execute_native_sequence_catalog(request)? {
                        Ok(epoch) => Ok(NativeResult::SequenceCatalog(epoch, table)),
                        Err(error) => {
                            if error != ops::Error::Unknown { giggles_core::cancellation::check()?; }
                            Ok(NativeResult::Error(error))
                        }
                    };
                }
                match session.execute_native_catalog(request)? {
                    Ok(outcome) => Ok(NativeResult::Catalog(outcome, table)),
                    Err(error) => {
                        if error != ops::Error::Unknown { giggles_core::cancellation::check()?; }
                        Ok(NativeResult::Error(error))
                    }
                }
            });
        }
        // Collect every target before resolution, authorization, or execution.
        let mut reqs = Vec::new();
        match &invocation {
            Invocation::Read(_) | Invocation::Join(_) => reqs.extend(read_tables(&invocation)
                .into_iter().map(|t| (t, giggles_core::roles::TablePrivilege::Select))),
            Invocation::Operation { request, .. } | Invocation::Page { request, .. } => reqs.extend(table_requirements(request)?),
            Invocation::Transaction { transaction, .. } => {
                if transaction.steps.len() > ops::MAX_TRANSACTION_ITEMS {
                    return Err(CoreError::Limit);
                }
                for step in &transaction.steps {
                    if let ops::TransactionStep::Execute(request)
                    | ops::TransactionStep::Let { request, .. } = step {
                        reqs.extend(table_requirements(request)?);
                    }
                }
            }
            Invocation::Catalog(_) | Invocation::Sequence(_) | Invocation::SearchPath(_) | Invocation::Begin | Invocation::Commit | Invocation::Rollback
            | Invocation::Savepoint(_) | Invocation::RollbackTo(_) | Invocation::Release(_) => {}
            Invocation::Legacy(_) | Invocation::Text { .. } => return Err(CoreError::Unsupported),
        }
        let reserved = reqs.iter().any(|(table, _)| table == "giggles_join");
        for (table, _) in &mut reqs {
            // Failed resolution must not disclose binding errors before authority.
            *table = session.sql.native_relation_reference(table).unwrap_or_else(|_| table.clone());
        }
        if matches!(&session.role, Role::Database { .. }) {
            session.authorize_table_requirements(&reqs)?;
        } else {
            let Role::Application { domain, tables } = &session.role else {
                return Err(CoreError::Policy(Error::Scope));
            };
            if reserved || reqs.iter().any(|(table, _)| table == "giggles_join" || !tables.contains(table)) {
                return Err(CoreError::Policy(Error::Scope));
            }
            if session.service.admin.authority_metadata()?.0.to_string() != *domain {
                return Err(CoreError::Policy(Error::Authentication));
            }
        }
        if let Invocation::Sequence(call) = &invocation {
            // SQL authorizes nextval/currval/setval through the same sequence dependency check.
            let (ops::SequenceCall::Nextval(name) | ops::SequenceCall::Currval(name) | ops::SequenceCall::Setval { name, .. }) = call;
            session.authorize_native_sequence(name)?;
        }
        if session.is_aborted() && !matches!(invocation, Invocation::Rollback | Invocation::RollbackTo(_)) {
            return Err(CoreError::Sql(SqlError::Aborted));
        }
        let result = match invocation {
            Invocation::Catalog(_) => unreachable!(),
            Invocation::SearchPath(schemas) => {
                if let Some(schemas) = schemas {
                    let path = schemas.iter().map(|s| format!("\"{}\"", s.replace('"', "\"\""))).collect::<Vec<_>>().join(", ");
                    session.set_search_path(&path);
                }
                // The session's own search path: built here, never core rows.
                return Ok(NativeResult::Sql(Released::synthetic(giggles_core::relational::SqlResult {
                    columns: vec!["search_path".into()],
                    rows: vec![giggles_core::relational::SqlRow { values: vec![giggles_core::relational::Value::Text(session.search_path.clone())], version: 0 }],
                    affected: 0,
                })));
            }
            // GD-0061 R3-313: every result core returns is held until the
            // session releases it, here, before anything encodes it. Outside
            // a task session the release is a move. A release error is an
            // execution error (`Session::release_page` aborts).
            Invocation::Read(read) => {
                if session.task().is_some() {
                    session.sql.set_task_statement_text(&serde_json::to_string(&read).unwrap_or_default());
                }
                let result = session.sql.execute_typed_read_unreleased(*read, None);
                return read_outcome(result.and_then(|result| session.release_page(result)));
            }
            Invocation::Join(read) => {
                if session.task().is_some() {
                    session.sql.set_task_statement_text(&serde_json::to_string(&read).unwrap_or_default());
                }
                let result = session.sql.execute_typed_join_unreleased(*read, None);
                return read_outcome(result.and_then(|result| session.release_page(result)));
            }
            Invocation::Operation { request, bindings } => {
                match session.sql.execute_native_unreleased(&request, &bindings) {
                    Ok(outcome) => Ok(NativeResult::Operation(session.release_page(outcome)?)),
                    Err(error) => Err(error),
                }
            }
            Invocation::Page { request, bindings, after } => {
                let after = (!after.is_empty()).then_some(after.as_slice());
                match session.read_native_page(&request, after, &bindings) {
                    Ok(page) => page.map(NativeResult::Page),
                    Err(error) => {
                        session.sql.log_refused_task_statement(
                            &serde_json::to_string(&request).unwrap_or_default(),
                            StatementClass::Select, bindings.len(), &error,
                        );
                        return Err(error);
                    }
                }
            }
            Invocation::Transaction {
                transaction,
                bindings,
            } => match session.sql.execute_native_transaction_unreleased(&transaction, &bindings) {
                Ok(outcome) => Ok(NativeResult::Transaction(session.release_page(outcome)?)),
                Err(error) => Err(error),
            },
            Invocation::Sequence(call) => {
                let column = match &call { ops::SequenceCall::Nextval(_) => "nextval", ops::SequenceCall::Currval(_) => "currval", ops::SequenceCall::Setval { .. } => "setval" };
                // A sequence value, built into a row here: never table rows.
                session.sql.execute_sequence_native(&call).map(|value| NativeResult::Sql(Released::synthetic(giggles_core::relational::SqlResult {
                    columns: vec![column.into()],
                    rows: vec![giggles_core::relational::SqlRow { values: vec![giggles_core::relational::Value::Int(value)], version: 0 }],
                    affected: 1,
                })))
            }
            Invocation::Savepoint(name) => { session.sql.savepoint(&name)?; return Ok(NativeResult::Control); }
            Invocation::RollbackTo(name) => { session.sql.rollback_to(&name)?; return Ok(NativeResult::Control); }
            Invocation::Release(name) => { session.sql.release(&name)?; return Ok(NativeResult::Control); }
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
                // PG18: undefined table is 42P01; native errors carry SQL's SQLSTATE (R2 parity).
                assert!(matches!(result, NativeResult::Error(ops::Error::Sql { sqlstate }) if sqlstate == "42P01"));
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
        for sql in ["CREATE SCHEMA app", "CREATE TABLE app.t(id BIGINT PRIMARY KEY)",
            "CREATE TABLE public.t(id BIGINT PRIMARY KEY)"] {
            service.admin.sql_session().execute(sql, &[], None).unwrap();
        }
        service.reload_bindings(vec![Binding { fingerprint: fingerprint.clone(),
            role: Role::Application { domain: service.admin.authority_metadata().unwrap().0.to_string(),
                tables: ["app\0t".into()].into_iter().collect() } }]).unwrap();
        let mut session = service.session(&fingerprint).unwrap();
        session.sql.set_search_path("app,public", "reader");
        for table in ["t", "public.t"] {
            let result = run(&mut session, Invocation::Operation { request: ops::Request::Read {
                table: table.into(), filter: None, order: vec![], limit: None, projection: vec![]
            }, bindings: ops::Bindings::new() });
            if table == "t" {
                assert!(matches!(result.unwrap(), NativeResult::Operation(_)));
            } else {
                assert!(matches!(result, Err(CoreError::Policy(Error::Scope))));
            }
        }

    }
}
