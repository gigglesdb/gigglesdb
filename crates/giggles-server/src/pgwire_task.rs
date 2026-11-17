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

/// A refusal of `class` with its fixed message.
fn refuse(diagnostic: &mut Diagnostic, class: StatementClass) -> &'static str {
    let error = TaskError::NotAllowedInTaskSession(class);
    diagnostic.message = Some(error.message());
    error.sqlstate()
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

fn admit(diagnostic: &mut Diagnostic, session: &mut Session, class: StatementClass) -> R<()> {
    diagnostic.core(session.admit(class))
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
    pub(super) fn task_prepare(
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

    /// `catalog_plan` without its snapshot: a plan that names a relation, or
    /// mixes catalog and application relations, is refused as `Catalog`
    /// before any catalog read; a relation-free plan is a `Show`.
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
        let plan = match giggles_pg_catalog::plan_gateway(sql) {
            Ok(Some(plan)) => plan,
            Ok(None) if Prepared::rejects_unsupported_table_free_scalar(sql) => {
                return Err(catalog_wire::error(giggles_pg_catalog::Error::Unsupported));
            }
            Ok(None) | Err(giggles_pg_catalog::Error::Syntax) => return Ok(None),
            Err(giggles_pg_catalog::Error::MixedRelations) => {
                return Err(refuse(&mut self.diagnostic, StatementClass::Catalog));
            }
            Err(error) => return Err(catalog_wire::error(error)),
        };
        if plan.depends_on_relations() {
            return Err(refuse(&mut self.diagnostic, StatementClass::Catalog));
        }
        admit(&mut self.diagnostic, session, StatementClass::Show)?;
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
        Ok(Some((Plan::Catalog(Box::new(plan), fields, 0), d)))
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
        let (plan, description) = prepare_declared_sql(session, sql, oids)?;
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
            },
            (ProtectedHead::Preview, Some((prepared, _))) => ProtectedStatement::Preview { prepared },
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
        let statements = simple::split(sql)?;
        match statements.as_slice() {
            [] => frame(b'I', &[], out),
            [statement] => self.task_statement(statement, session, out),
            _ => Err(refuse(
                &mut self.diagnostic,
                StatementClass::TransactionControl,
            )),
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
        let portal = self.portals.get(name).ok_or("34000")?;
        self.diagnostic.query_id = portal.s.query_id.clone();
        if max != 0 {
            return Err(refuse(&mut self.diagnostic, StatementClass::Cursor));
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
            portal.result = Some(SqlResult::default());
        }
        Ok(())
    }

    /// Runs an admitted statement. The class is admitted again, because the
    /// task's standing is checked on every call (design-2 section 3).
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
                if describe {
                    row_description(&description, &[], out)?;
                }
                rows(&self.settings.current, zone, &result, &description, formats, 0, 1, out)?;
                complete("SHOW", &result, out)
            }
            Plan::Settings(settings) => {
                if !settings.iter().all(setting_allowed) {
                    return Err(refuse(&mut self.diagnostic, StatementClass::Set));
                }
                execute_settings(settings, &mut self.settings, session, false, out)
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
                    result.rows.len(),
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
                catalog_wire::rows(
                    self.settings.current.extra_float_digits,
                    &result,
                    formats,
                    0,
                    result.rows.len(),
                    out,
                )?;
                catalog_complete(result.rows.len(), out)
            }
            Plan::Sql(prepared) if class == StatementClass::Select => {
                self.diagnostic.phase = "application execution";
                let mut cursor = self.diagnostic.core(session.open_cursor(prepared, params))?;
                if describe {
                    row_description(&s.d, &[], out)?;
                }
                let mut count = 0;
                while !cursor.is_exhausted() {
                    let chunk = self.diagnostic.core(session.fetch_cursor(
                        &mut cursor,
                        CURSOR_ROWS,
                        CURSOR_BYTES,
                    ))?;
                    let n = chunk.len();
                    let result = SqlResult {
                        rows: chunk,
                        ..SqlResult::default()
                    };
                    rows(&self.settings.current, zone, &result, &s.d, formats, 0, n, out)?;
                    count += n;
                    flush_output(&self.output, out)?;
                }
                catalog_complete(count, out)
            }
            Plan::Protected(statement) => {
                self.diagnostic.phase = "protected execution";
                let result = self
                    .diagnostic
                    .core(session.execute_protected(statement, params))?;
                fits(&result)?;
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
                        result.rows.len(),
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
            | Plan::Catalog(..)
            | Plan::Sql(_)
            | Plan::Copy(..)
            | Plan::Task(_) => Err(refuse(&mut self.diagnostic, class)),
        }
    }
}
