//! GD-0061 R3-206 / R3-207: the statement path of a task (agent) session.
//!
//! Every statement on an agent connection comes here, from Parse
//! (`State::prepare_statement`), Execute (the `b'E'` arm) and Query
//! (`simple_query` / `simple_statement_inner`). The existing detectors run in
//! the existing order, but each statement's class is admitted through
//! `Session::admit` before anything reads the catalog, a sequence or a table.
//! Nothing here begins an implicit transaction, and `authorize_sequence_sql` is
//! never called: the `Session` backstops refuse what slips past.
use super::*;
use crate::agent::statement::{self as protected, ProtectedHead, ProtectedStatement};
use crate::pg_settings::{Function, Setting, Show};
use giggles_core::task::readlog::{ENTRY_PGWIRE_EXTENDED, ENTRY_PGWIRE_SIMPLE};
use giggles_core::task::{StatementClass, TaskError};

fn no_shape() -> Description {
    Description {
        type_oids: vec![],
        typmods: vec![],
        columns: vec![],
        parameters: vec![],
    }
}

/// Table 4(b) of design-2 section 4: the class of a pgwire plan. `None` is the
/// empty query, which needs no admission. Exhaustive, with no wildcard arm.
pub(super) fn plan_class(plan: &Plan) -> Option<StatementClass> {
    use crate::auth::statements::AdminStatement as A;
    use crate::pg_transaction::Command as C;
    Some(match plan {
        Plan::Empty => return None,
        Plan::Explain(_) => StatementClass::Explain,
        Plan::Cursor(_) => StatementClass::Cursor,
        Plan::AccessShare(_) => StatementClass::Lock,
        Plan::Admin(A::DropTable(..)) => StatementClass::Ddl,
        Plan::Admin(A::Error(_) | A::Create(..) | A::Alter(..) | A::Drop(..) | A::Grant { .. }) => {
            StatementClass::RoleStatement
        }
        Plan::Control(C::Begin(_) | C::Start(_) | C::Commit | C::Rollback | C::Set(_) | C::Session(_)) => {
            StatementClass::TransactionControl
        }
        Plan::Control(C::Savepoint(_) | C::Release(_) | C::RollbackTo(_)) => {
            StatementClass::Savepoint
        }
        Plan::Settings(_) => StatementClass::Set,
        Plan::Inspect(_) => StatementClass::Show,
        Plan::SettingsFunction(Function::Current { .. }) => StatementClass::Show,
        Plan::SettingsFunction(Function::Set { .. }) => StatementClass::Set,
        Plan::SettingsFunction(Function::Advisory { .. }) => StatementClass::Lock,
        Plan::SettingsFunction(Function::Inventory(_)) => StatementClass::Catalog,
        Plan::Catalog(plan, _, _) if !plan.depends_on_relations() => StatementClass::Show,
        Plan::Catalog(..) => StatementClass::Catalog,
        Plan::Copy(..) => StatementClass::Copy,
        Plan::Task(_) => StatementClass::Other,
        // R3-707: the approver forms are never admitted in a task session (D7.1).
        Plan::Change(_) => StatementClass::Other,
        // R3-110: task statements are authorization DDL, never run by a task.
        Plan::TaskDdl(_) => StatementClass::RoleStatement,
        Plan::Sql(p) => p.task_class(),
        Plan::Protected(statement) => statement.class(),
    })
}

/// The SET allow-list of a task session (design-2 section 4). `SET LOCAL`,
/// `RESET ALL`, `search_path`, `role`, `session_authorization`, `giggles.task`
/// and everything else are refused.
pub(super) fn setting_allowed(setting: &Setting) -> bool {
    match setting {
        Setting::ApplicationName(_) | Setting::ExtraFloatDigits(_) => true,
        Setting::Database { name, local, .. } => !*local && show_allowed(*name),
        Setting::ResetAll => false,
    }
}

fn show_allowed(name: Show) -> bool {
    matches!(
        name,
        Show::ApplicationName
            | Show::ExtraFloatDigits
            | Show::DateStyle
            | Show::TimeZone
            | Show::IntervalStyle
            | Show::ClientEncoding
            | Show::ByteaOutput
            | Show::StatementTimeout
    )
}

/// `set_config(name, value, is_local)`: the same allow-list, and never local.
fn function_allowed(function: &Function) -> bool {
    match function {
        Function::Set { name, local, .. } => {
            !*local
                && [
                    "application_name",
                    "extra_float_digits",
                    "datestyle",
                    "timezone",
                    "intervalstyle",
                    "client_encoding",
                    "bytea_output",
                    "statement_timeout",
                ]
                .iter()
                .any(|allowed| name.eq_ignore_ascii_case(allowed))
        }
        Function::Current { .. } => true,
        Function::Advisory { .. } | Function::Inventory(_) => false,
    }
}

fn leading_word_is(sql: &str, words: &[&str]) -> bool {
    let word: String = sql
        .trim_start()
        .chars()
        .take_while(|c| c.is_ascii_alphabetic())
        .collect();
    words.iter().any(|w| word.eq_ignore_ascii_case(w))
}

/// A refusal of `class` with its fixed message. It is held in the diagnostic
/// until the caller logs it with the statement text (R3-309/R3-310).
fn refuse(diagnostic: &mut Diagnostic, class: StatementClass) -> &'static str {
    let error = TaskError::NotAllowedInTaskSession(class);
    diagnostic.message = Some(error.message());
    let code = error.sqlstate();
    diagnostic.task_refusal = Some((class, CoreError::Task(error)));
    code
}

/// Whether `code` with `message` is already a class refusal from this path.
fn is_refusal(code: &str, message: Option<&str>) -> bool {
    code == "0A000"
        && message.is_some_and(|message| {
            StatementClass::ALL
                .iter()
                .any(|class| TaskError::NotAllowedInTaskSession(*class).message() == message)
        })
}

/// `Session::admit`; a refusal is held for the read log like `refuse`.
fn admit(diagnostic: &mut Diagnostic, session: &mut Session, class: StatementClass) -> R<()> {
    let admitted = session.admit(class);
    if let Err(error) = &admitted {
        diagnostic.task_refusal = Some((class, error.clone()));
    }
    diagnostic.core(admitted)
}

/// A catalog snapshot that holds no relation: enough for a relation-free plan
/// (`SELECT 1`, `version()`, `current_schema()`), and built without reading
/// the catalog.
fn relation_free_snapshot(
    session: &Session,
    search_path: &str,
) -> giggles_pg_catalog::CatalogSnapshot {
    catalog_wire::with_search_path(
        giggles_pg_catalog::CatalogSnapshot {
            backend_pid: session.backend_pid,
            backend_ssl: session.backend_ssl,
            user: match &session.role {
                Role::Database { name } => name.clone(),
                _ => "giggles".into(),
            },
            database: session.database.as_deref().unwrap_or("giggles").into(),
            ..giggles_pg_catalog::CatalogSnapshot::default()
        },
        search_path,
    )
}

impl State {
    /// Writes a held refusal (`refuse`, `admit`) to the task's read log as
    /// one finished record of `sql` (R3-309 `log_refused_task_statement`).
    fn log_task_refusal(&mut self, session: &Session, sql: &str, param_count: usize) {
        if let Some((class, error)) = self.diagnostic.task_refusal.take() {
            session
                .sql
                .log_refused_task_statement(sql, class, param_count, &error);
        }
    }

    /// Parse (and the simple path's prepare). A refusal is logged here.
    pub(super) fn task_prepare(
        &mut self,
        sql: &str,
        oids: Vec<u32>,
        session: &mut Session,
    ) -> R<Statement> {
        self.diagnostic.task_refusal = None;
        let result = self.task_prepare_inner(sql, oids.clone(), session);
        if result.is_err() {
            self.log_task_refusal(session, sql, oids.len());
        }
        result
    }

    fn task_prepare_inner(
        &mut self,
        sql: &str,
        oids: Vec<u32>,
        session: &mut Session,
    ) -> R<Statement> {
        self.diagnostic.phase = "task prepare";
        let (p, mut d) = self.task_plan(sql, &oids, session)?;
        if oids.len() > d.parameters.len() {
            return Err("08P01");
        }
        let mut parameter_oids: Vec<u32> = match &p {
            Plan::Catalog(plan, _, _) => plan.parameter_types().iter().map(|t| t.oid()).collect(),
            _ => d.parameters.iter().map(oid).collect(),
        };
        self.diagnostic.phase = "parameter type declaration";
        for (i, (o, t)) in oids.iter().copied().zip(&mut d.parameters).enumerate() {
            if o == 0 {
                continue;
            }
            if let Plan::Catalog(plan, _, _) = &p {
                catalog_wire::parameter_type(o)?;
                if o != parameter_oids[i] && !catalog_input_override(plan, i, o, parameter_oids[i])
                {
                    return Err("0A000");
                }
            } else if o != oid(t) {
                if !matches!(p, Plan::Sql(_) | Plan::Protected(_)) {
                    return Err("0A000");
                }
                *t = sql_declared_type(t, o).ok_or("0A000")?;
            }
            parameter_oids[i] = o;
        }
        Ok(Statement {
            description_version: None,
            sql_bytes: sql.len(),
            sql: sql.to_owned(),
            declared_oids: oids,
            p,
            d,
            parameter_oids,
            query_id: self.diagnostic.query_id.clone(),
        })
    }

    /// The detectors of `prepare_statement`, in its order, with the class
    /// admitted before any catalog, sequence or table read. A SET or RESET
    /// that no grammar accepts (`SET giggles.task = ...`, `SET ROLE x`) is a
    /// refused setting; only an invalid value for an accepted one keeps 22023.
    fn task_plan(
        &mut self,
        sql: &str,
        oids: &[u32],
        session: &mut Session,
    ) -> R<(Plan, Description)> {
        match self.task_detect(sql, oids, session) {
            Err(code)
                if matches!(code, "42601" | "42704" | "0A000")
                    && !is_refusal(code, self.diagnostic.message)
                    && leading_word_is(sql, &["set", "reset"]) =>
            {
                Err(refuse(&mut self.diagnostic, StatementClass::Set))
            }
            other => other,
        }
    }

    fn task_detect(
        &mut self,
        sql: &str,
        oids: &[u32],
        session: &mut Session,
    ) -> R<(Plan, Description)> {
        if let Some((head, wrapped)) = protected::parse(sql)? {
            return self.task_protected(head, wrapped, oids, session);
        }
        // R3-707: the approver forms, well formed or not, are `Other` in a
        // task session (design-7 D7.1).
        if !matches!(crate::agent::preview::approver_statement(sql), Ok(None)) {
            return Err(refuse(&mut self.diagnostic, StatementClass::Other));
        }
        let refused = |diagnostic: &mut Diagnostic, plan: &Plan| {
            refuse(diagnostic, plan_class(plan).unwrap_or(StatementClass::Other))
        };
        if crate::auth::statements::access_share_tables(sql)?.is_some() {
            return Err(refuse(&mut self.diagnostic, StatementClass::Lock));
        }
        if is_copy_sql(sql) || crate::auth::statements::copy_stdout_select(sql)?.is_some() {
            return Err(refuse(&mut self.diagnostic, StatementClass::Copy));
        }
        if let Some(command) = cursor::parse(sql)? {
            return Err(refused(&mut self.diagnostic, &Plan::Cursor(command)));
        }
        // R3-110: task statements, well formed or not, before the role
        // parser, which claims every GRANT and REVOKE head.
        if let Some(stmt) = crate::task_ddl::parse(sql) {
            return Err(refused(&mut self.diagnostic, &Plan::TaskDdl(Box::new(stmt))));
        }
        if let Some(stmt) = crate::auth::statements::parse(sql) {
            return Err(refused(&mut self.diagnostic, &Plan::Admin(stmt)));
        }
        if crate::pg_settings::empty_sql(sql)? {
            return Ok((Plan::Empty, no_shape()));
        }
        if let Some(command) = parse_control(sql, session, &mut self.diagnostic)? {
            return Err(refused(&mut self.diagnostic, &Plan::Control(command)));
        }
        if let Some(show) = show_settings(sql, session, &mut self.diagnostic)? {
            admit(&mut self.diagnostic, session, StatementClass::Show)?;
            let (description, _) = inspect(show, &self.settings.current, session)?;
            return Ok((Plan::Inspect(show), description));
        }
        match parse_settings(sql, session, &mut self.diagnostic) {
            Ok(Some(settings)) => {
                admit(&mut self.diagnostic, session, StatementClass::Set)?;
                if settings.len() != 1 {
                    return Err("42601");
                }
                if !setting_allowed(&settings[0]) {
                    return Err(refuse(&mut self.diagnostic, StatementClass::Set));
                }
                return Ok((Plan::Settings(settings), no_shape()));
            }
            Ok(None) => {}
            Err(code) => return Err(code),
        }
        if sql
            .trim_start_matches(|c: char| c.is_ascii_whitespace())
            .starts_with("GIGGLES")
        {
            return Err(refuse(&mut self.diagnostic, StatementClass::Other));
        }
        if let Some(function) = settings_functions::parse(sql, session, &mut self.diagnostic)? {
            let plan = Plan::SettingsFunction(function);
            let class = plan_class(&plan).unwrap_or(StatementClass::Other);
            admit(&mut self.diagnostic, session, class)?;
            let Plan::SettingsFunction(function) = plan else {
                unreachable!()
            };
            if !function_allowed(&function) {
                return Err(refuse(&mut self.diagnostic, class));
            }
            let description = settings_functions::description(&function);
            return Ok((Plan::SettingsFunction(function), description));
        }
        if let Some(planned) = self.task_catalog_plan(sql, session)? {
            return Ok(planned);
        }
        self.task_sql(sql, oids, session)
    }

    /// `catalog_plan` for a task session. The class is admitted before any
    /// catalog read: a plan that names a relation (or mixes catalog and
    /// application relations) is `Catalog`, a relation-free plan is `Show`.
    /// An admitted `Catalog` plan is planned against the task's filtered
    /// catalog (GD-0061 R3-312), so it can name only the task's table and
    /// its shown columns.
    fn task_catalog_plan(
        &mut self,
        sql: &str,
        session: &mut Session,
    ) -> R<Option<(Plan, Description)>> {
        self.diagnostic.phase = "catalog planning";
        if Prepared::application_dml(sql, giggles_pg_catalog::catalog_identifier)
            || !Prepared::sequence_dependencies(sql).is_empty()
        {
            return Ok(None);
        }
        let planned = match giggles_pg_catalog::plan_gateway(sql) {
            Err(giggles_pg_catalog::Error::MixedRelations) => {
                admit(&mut self.diagnostic, session, StatementClass::Catalog)?;
                let (epoch, tables, indexes, typmods, types) =
                    self.diagnostic.core(session.catalog_snapshot_with_typmods())?;
                let snapshot =
                    catalog_wire::snapshot(session, epoch, tables, indexes, typmods, types)?;
                let relations = catalog_wire::application_schemas(session, &snapshot);
                giggles_pg_catalog::plan_gateway_with_relations(sql, &relations)
            }
            other => other,
        };
        let plan = match planned {
            Ok(Some(plan)) => plan,
            Ok(None) if Prepared::rejects_unsupported_table_free_scalar(sql) => {
                return Err(catalog_wire::error(giggles_pg_catalog::Error::Unsupported));
            }
            Ok(None) | Err(giggles_pg_catalog::Error::Syntax) => return Ok(None),
            Err(error) => return Err(catalog_wire::error(error)),
        };
        let epoch = if plan.depends_on_relations() {
            admit(&mut self.diagnostic, session, StatementClass::Catalog)?;
            self.diagnostic.core(session.catalog_snapshot())?.0
        } else {
            admit(&mut self.diagnostic, session, StatementClass::Show)?;
            0
        };
        let fields = giggles_pg_catalog::describe(&plan).map_err(catalog_wire::error)?;
        let d = Description {
            type_oids: vec![],
            typmods: vec![-1; fields.len()],
            parameters: vec![DataType::Text; plan.parameter_count()],
            columns: fields
                .iter()
                .map(|f| giggles_core::relational::ColumnDef {
                    id: 0,
                    name: f.name.clone(),
                    data_type: DataType::Text,
                    not_null: false,
                })
                .collect(),
        };
        Ok(Some((Plan::Catalog(Box::new(plan), fields, epoch), d)))
    }

    /// Application SQL: classified from its text, admitted, then prepared. A
    /// bare UPDATE needs an operation ID.
    fn task_sql(&mut self, sql: &str, oids: &[u32], session: &mut Session) -> R<(Plan, Description)> {
        self.diagnostic.phase = "application prepare";
        let class = self.diagnostic.core(Prepared::task_class_of_sql(sql))?;
        if class == StatementClass::Update {
            return self
                .diagnostic
                .core(Err(CoreError::Task(TaskError::OperationIdRequired)));
        }
        admit(&mut self.diagnostic, session, class)?;
        let (plan, description) = prepare_declared_sql(session, sql, oids, false)?;
        // The bound statement must be what was admitted.
        if plan_class(&plan) != Some(class) {
            return Err(refuse(&mut self.diagnostic, StatementClass::Other));
        }
        Ok((plan, description))
    }

    /// R3-207: a protected form. Its class is admitted before the wrapped
    /// UPDATE is prepared, so a refused form never reads the catalog.
    fn task_protected(
        &mut self,
        head: ProtectedHead,
        wrapped: &str,
        oids: &[u32],
        session: &mut Session,
    ) -> R<(Plan, Description)> {
        admit(&mut self.diagnostic, session, head.class())?;
        let prepared = match &head {
            ProtectedHead::Direct { .. } | ProtectedHead::Preview => {
                self.diagnostic.phase = "application prepare";
                if self.diagnostic.core(Prepared::task_class_of_sql(wrapped))?
                    != StatementClass::Update
                {
                    return Err("42601");
                }
                let (prepared, description) = prepare_protected_sql(
                    &mut self.diagnostic,
                    session,
                    head.class(),
                    wrapped,
                    oids,
                )?;
                if prepared.task_class() != StatementClass::Update {
                    return Err("42601");
                }
                Some((Box::new(prepared), description))
            }
            ProtectedHead::Apply { .. } | ProtectedHead::Outcome { .. } => None,
        };
        let description = protected::description(&head, prepared.as_ref().map(|(_, d)| d));
        let statement = match (head, prepared) {
            (ProtectedHead::Direct { op }, Some((prepared, _))) => ProtectedStatement::Direct {
                op,
                prepared,
                text: std::sync::Arc::from(wrapped),
                bound: None,
            },
            (ProtectedHead::Preview, Some((prepared, _))) => ProtectedStatement::Preview {
                prepared,
                text: std::sync::Arc::from(wrapped),
            },
            (ProtectedHead::Apply { op, change, digest }, None) => ProtectedStatement::Apply {
                op,
                preview: change,
                digest,
            },
            (ProtectedHead::Outcome { op }, None) => ProtectedStatement::Outcome { op },
            _ => return Err("XX000"),
        };
        Ok((Plan::Protected(Box::new(statement)), description))
    }

    /// A Query message in a task session: exactly one statement. PostgreSQL
    /// runs several as one implicit transaction (Q14), which a task session
    /// never opens.
    pub(super) fn task_simple(
        &mut self,
        sql: &str,
        session: &mut Session,
        out: &mut Vec<u8>,
    ) -> R<()> {
        session.sql.set_task_entry(ENTRY_PGWIRE_SIMPLE);
        let statements = simple::split(sql)?;
        match statements.as_slice() {
            [] => frame(b'I', &[], out),
            [statement] => self.task_statement(statement, session, out),
            _ => {
                let code = refuse(&mut self.diagnostic, StatementClass::TransactionControl);
                self.log_task_refusal(session, sql, 0);
                Err(code)
            }
        }
    }

    /// One simple-protocol statement: prepared on the task path, then run.
    pub(super) fn task_statement(
        &mut self,
        sql: &str,
        session: &mut Session,
        out: &mut Vec<u8>,
    ) -> R<()> {
        statement_deadline(&self.settings.current);
        let statement = self.task_prepare(sql, Vec::new(), session)?;
        self.task_run(&statement, &[], &[], &[], true, 0, session, out)
    }

    /// Execute of a portal. `max_rows != 0` is refused as a cursor: a
    /// suspended portal would hold a snapshot while idle. A portal runs once.
    pub(super) fn task_execute(
        &mut self,
        name: &str,
        max: u32,
        retained: usize,
        session: &mut Session,
        out: &mut Vec<u8>,
    ) -> R<()> {
        session.sql.set_task_entry(ENTRY_PGWIRE_EXTENDED);
        let portal = self.portals.get(name).ok_or("34000")?;
        self.diagnostic.query_id = portal.s.query_id.clone();
        if max != 0 {
            let (sql, count) = (portal.s.sql.clone(), portal.params.len());
            let code = refuse(&mut self.diagnostic, StatementClass::Cursor);
            self.log_task_refusal(session, &sql, count);
            return Err(code);
        }
        if portal.result.is_some() {
            return Err("55000");
        }
        let (statement, params, catalog_params, formats) = (
            portal.s.clone(),
            portal.params.clone(),
            portal.catalog_params.clone(),
            portal.formats.clone(),
        );
        self.task_run(
            &statement,
            &params,
            &catalog_params,
            &formats,
            false,
            retained,
            session,
            out,
        )?;
        if let Some(portal) = self.portals.get_mut(name) {
            portal.result = ran();
        }
        Ok(())
    }

    /// Runs an admitted statement. The class is admitted again, because the
    /// task's standing is checked on every call (design-2 section 3). A
    /// refusal (admission or class) is logged here.
    #[allow(clippy::too_many_arguments)]
    fn task_run(
        &mut self,
        s: &Statement,
        params: &[Value],
        catalog_params: &[giggles_pg_catalog::Value],
        formats: &[u16],
        describe: bool,
        retained: usize,
        session: &mut Session,
        out: &mut Vec<u8>,
    ) -> R<()> {
        self.diagnostic.task_refusal = None;
        let result = self.task_run_inner(
            s,
            params,
            catalog_params,
            formats,
            describe,
            retained,
            session,
            out,
        );
        if result.is_err() {
            self.log_task_refusal(session, &s.sql, params.len());
        }
        result
    }

    #[allow(clippy::too_many_arguments)]
    fn task_run_inner(
        &mut self,
        s: &Statement,
        params: &[Value],
        catalog_params: &[giggles_pg_catalog::Value],
        formats: &[u16],
        describe: bool,
        retained: usize,
        session: &mut Session,
        out: &mut Vec<u8>,
    ) -> R<()> {
        let Some(class) = plan_class(&s.p) else {
            return frame(b'I', &[], out);
        };
        admit(&mut self.diagnostic, session, class)?;
        let zone = session.zone().unwrap_or(SessionZone::Fixed(0));
        let fits = |result: &SqlResult| -> R<()> {
            let bytes = serde_json::to_vec(result).map_err(|_| "54000")?.len();
            if retained.checked_add(bytes).ok_or("54000")? > MAX_RESPONSE {
                return Err("54000");
            }
            Ok(())
        };
        match &s.p {
            Plan::Inspect(show) => {
                let (description, result) = inspect(*show, &self.settings.current, session)?;
                let result = &result;
                if describe {
                    row_description(&description, &[], out)?;
                }
                rows(&self.settings.current, zone, result, &description, formats, 0, 1, out)?;
                complete("SHOW", result, out)
            }
            Plan::Settings(settings) => {
                if !settings.iter().all(setting_allowed) {
                    return Err(refuse(&mut self.diagnostic, StatementClass::Set));
                }
                execute_settings(settings, &mut self.settings, session, false, &mut self.diagnostic, out)
            }
            Plan::SettingsFunction(function) => {
                if !function_allowed(function) {
                    return Err(refuse(&mut self.diagnostic, class));
                }
                let result = settings_functions::evaluate(
                    function,
                    &mut self.settings,
                    session,
                    &mut self.diagnostic,
                    out,
                )?;
                fits(&result)?;
                // A settings function's value, built here: never core rows.
                let result = Released::synthetic(result);
                if describe {
                    row_description(&s.d, &[], out)?;
                }
                rows(
                    &self.settings.current,
                    zone,
                    &result,
                    &s.d,
                    formats,
                    0,
                    result.get().rows.len(),
                    out,
                )?;
                complete("SELECT", &result, out)
            }
            Plan::Catalog(plan, fields, _) if !plan.depends_on_relations() => {
                self.diagnostic.phase = "catalog execution";
                let result = catalog_wire::execute(
                    plan,
                    &relation_free_snapshot(session, &self.settings.current.search_path),
                    catalog_params,
                )
                .map_err(catalog_wire::error)?;
                if result.fields != *fields {
                    return Err("0A000");
                }
                if retained
                    .checked_add(catalog_wire::retained_bytes(&result)?)
                    .ok_or("54000")?
                    > MAX_RESPONSE
                {
                    return Err("54000");
                }
                if describe {
                    catalog_wire::description(&result.fields, &[], out)?;
                }
                let total = result.rows.len();
                // A relation-free catalog result: built from no application
                // row, so there is nothing to release.
                catalog_wire::rows(
                    self.settings.current.extra_float_digits,
                    &Released::synthetic(result),
                    formats,
                    0,
                    total,
                    out,
                )?;
                catalog_complete(total, out)
            }
            Plan::Catalog(plan, fields, epoch) => {
                self.diagnostic.phase = "catalog execution";
                // R3-309: the read log records the catalog query's text for
                // the application rows it reads.
                session.sql.set_task_statement_text(&s.sql);
                let outcome =
                    self.task_catalog(plan, fields, *epoch, catalog_params, retained, session);
                session.sql.finish_task_statement(match &outcome {
                    Err(TaskRun::Core(error)) => Err(error),
                    _ => Ok(()),
                });
                let result = outcome.map_err(|error| match error {
                    TaskRun::Core(error) => self.diagnostic.core::<()>(Err(error)).unwrap_err(),
                    TaskRun::Wire(code) => code,
                })?;
                let total = result.get().rows.len();
                if describe {
                    catalog_wire::description(&result.get().fields, &[], out)?;
                }
                catalog_wire::rows(
                    self.settings.current.extra_float_digits,
                    &result,
                    formats,
                    0,
                    total,
                    out,
                )?;
                catalog_complete(total, out)
            }
            Plan::Sql(prepared) if class == StatementClass::Select => {
                self.diagnostic.phase = "application execution";
                // R3-309: the read log records the statement's text.
                session.sql.set_task_statement_text(&s.sql);
                let outcome = self.task_select(prepared, s, params, formats, describe, session, out);
                let count = outcome.map_err(|error| match error {
                    TaskRun::Core(error) => self.diagnostic.core::<()>(Err(error)).unwrap_err(),
                    TaskRun::Wire(code) => code,
                })?;
                catalog_complete(count, out)
            }
            Plan::Protected(statement) => {
                self.diagnostic.phase = "protected execution";
                let result = self
                    .diagnostic
                    .core(session.execute_protected(statement, params))?;
                fits(&result)?;
                // A receipt count, a stored outcome or a preview summary the
                // server builds (`agent::statement::protected_seam`): never
                // core rows.
                let result = Released::synthetic(result);
                if !s.d.columns.is_empty() {
                    if describe {
                        row_description(&s.d, &[], out)?;
                    }
                    rows(
                        &self.settings.current,
                        zone,
                        &result,
                        &s.d,
                        formats,
                        0,
                        result.get().rows.len(),
                        out,
                    )?;
                }
                complete(statement.command(), &result, out)
            }
            // Prepare admitted none of these; refuse rather than run them.
            Plan::Empty
            | Plan::Explain(_)
            | Plan::Cursor(_)
            | Plan::AccessShare(_)
            | Plan::Admin(_)
            | Plan::Control(_)
            | Plan::Sql(_)
            | Plan::Copy(..)
            | Plan::Task(_)
            | Plan::Change(_)
            | Plan::TaskDdl(_) => Err(refuse(&mut self.diagnostic, class)),
        }
    }

    /// A catalog query that names a relation, against the task's filtered
    /// catalog. Application rows it reads were released (scoped, masked and
    /// charged) by `with_application_rows`; the result the catalog engine
    /// builds from them and the filtered snapshot is synthetic.
    fn task_catalog(
        &mut self,
        plan: &giggles_pg_catalog::Plan,
        fields: &[giggles_pg_catalog::Field],
        epoch: u64,
        catalog_params: &[giggles_pg_catalog::Value],
        retained: usize,
        session: &mut Session,
    ) -> Result<Released<giggles_pg_catalog::ResultSet>, TaskRun> {
        let (current, tables, indexes, typmods, types) = session.catalog_snapshot_with_typmods()?;
        if current != epoch && !plan.refreshes_view_definitions() {
            return Err(TaskRun::Wire("0A000"));
        }
        let snapshot = catalog_wire::with_search_path(
            catalog_wire::snapshot(session, current, tables, indexes, typmods, types)?,
            &self.settings.current.search_path,
        );
        let snapshot = catalog_wire::with_application_rows(session, plan, snapshot)?;
        let result = catalog_wire::execute(plan, &snapshot, catalog_params)
            .map_err(catalog_wire::error)?;
        if result.fields != fields {
            return Err(TaskRun::Wire("0A000"));
        }
        if retained
            .checked_add(catalog_wire::retained_bytes(&result)?)
            .ok_or("54000")?
            > MAX_RESPONSE
        {
            return Err(TaskRun::Wire("54000"));
        }
        Ok(Released::synthetic(result))
    }

    /// A task SELECT: one cursor, and every chunk fetched unreleased, then
    /// released (masked, checked and charged), then encoded and flushed
    /// (GD-0061 R3-310). The statement's read-log entry is finished with the
    /// outcome while the cursor is still open, on every exit.
    #[allow(clippy::too_many_arguments)]
    fn task_select(
        &mut self,
        prepared: &Prepared,
        s: &Statement,
        params: &[Value],
        formats: &[u16],
        describe: bool,
        session: &mut Session,
        out: &mut Vec<u8>,
    ) -> Result<usize, TaskRun> {
        let mut cursor = match session.open_cursor(prepared, params) {
            Ok(cursor) => cursor,
            Err(error) => {
                session.sql.finish_task_statement(Err(&error));
                return Err(error.into());
            }
        };
        let outcome = self.task_pages(&mut cursor, s, formats, describe, session, out);
        // Charges any rows_read no release charged and finalizes the record.
        session.sql.finish_task_statement(match &outcome {
            Err(TaskRun::Core(error)) => Err(error),
            _ => Ok(()),
        });
        drop(cursor);
        outcome
    }

    fn task_pages(
        &mut self,
        cursor: &mut giggles_core::Cursor,
        s: &Statement,
        formats: &[u16],
        describe: bool,
        session: &mut Session,
        out: &mut Vec<u8>,
    ) -> Result<usize, TaskRun> {
        let zone = session.zone().unwrap_or(SessionZone::Fixed(0));
        if describe {
            row_description(&s.d, &[], out)?;
        }
        let mut count = 0;
        while !cursor.is_exhausted() {
            let chunk = session.fetch_cursor_unreleased(cursor, CURSOR_ROWS, CURSOR_BYTES)?;
            let page = session.release_page(chunk)?.map(|rows| SqlResult {
                rows,
                ..SqlResult::default()
            });
            let n = page.get().rows.len();
            rows(&self.settings.current, zone, &page, &s.d, formats, 0, n, out)?;
            count += n;
            flush_output(&self.output, out)?;
        }
        Ok(count)
    }
}

/// An error of a task SELECT: from core (it ends the read-log entry with that
/// error) or from the wire.
enum TaskRun {
    Core(CoreError),
    Wire(&'static str),
}

impl From<CoreError> for TaskRun {
    fn from(error: CoreError) -> Self {
        TaskRun::Core(error)
    }
}

impl From<&'static str> for TaskRun {
    fn from(code: &'static str) -> Self {
        TaskRun::Wire(code)
    }
}
