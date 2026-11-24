use super::*;

#[derive(Clone)]
pub(super) enum Command {
    Prepare {
        name: String,
        types: Vec<String>,
        query: String,
        source: String,
    },
    Execute {
        name: String,
        args: Vec<String>,
    },
    Deallocate(Option<String>),
    Discard(Discard),
}

#[derive(Clone, Copy)]
pub(super) enum Discard {
    All,
    Plans,
    Sequences,
    Temp,
}

#[derive(Clone)]
pub(super) struct Invocation {
    pub command: Command,
    pub target: Option<Box<Statement>>,
}

// Read the utility prefix and delimit its lists. The SQL engine parses the
// inner statement and each argument expression.
struct Prefix<'a>(&'a str);
impl<'a> Prefix<'a> {
    fn word(&mut self) -> Option<&'a str> {
        let text = self.0.trim_start();
        let mut chars = text.char_indices();
        let (_, first) = chars.next()?;
        if first.is_ascii() && first != '_' && !first.is_ascii_alphabetic() {
            return None;
        }
        let end = chars
            .find(|(_, c)| c.is_ascii() && !c.is_ascii_alphanumeric() && *c != '_' && *c != '$')
            .map_or(text.len(), |(i, _)| i);
        self.0 = &text[end..];
        Some(&text[..end])
    }

    fn kw(&mut self, keyword: &str) -> bool {
        let before = self.0;
        if self
            .word()
            .is_some_and(|word| word.eq_ignore_ascii_case(keyword))
        {
            true
        } else {
            self.0 = before;
            false
        }
    }

    fn name(&mut self) -> R<String> {
        self.0 = self.0.trim_start();
        let mut name = if self.0.starts_with('"') {
            let end = simple::quoted_end(self.0.as_bytes(), 0)?.ok_or("42601")?;
            let name = self.0[1..end - 1].replace("\"\"", "\"");
            self.0 = &self.0[end..];
            name
        } else {
            self.word().ok_or("42601")?.to_ascii_lowercase()
        };
        if name.is_empty() {
            return Err("42601");
        }
        while name.len() > 63 {
            name.pop();
        }
        Ok(name)
    }

    fn list(&mut self) -> R<Vec<String>> {
        self.0 = self.0.trim_start();
        let Some(tail) = self.0.strip_prefix('(') else {
            return Ok(Vec::new());
        };
        let bytes = tail.as_bytes();
        let mut start = 0;
        let mut at = 0;
        let mut nesting = Vec::new();
        let mut items = Vec::new();
        while at < bytes.len() {
            if let Some(end) = simple::quoted_end(bytes, at)? {
                at = end;
                continue;
            }
            match bytes[at] {
                b'(' => nesting.push(b')'),
                b'[' => nesting.push(b']'),
                b')' | b']' if !nesting.is_empty() => {
                    if nesting.pop() != Some(bytes[at]) {
                        return Err("42601");
                    }
                }
                b',' | b')' if nesting.is_empty() => {
                    let item = tail[start..at].trim();
                    if item.is_empty() {
                        return Err("42601");
                    }
                    items.push(item.to_owned());
                    if items.len() > giggles_core::relational::MAX_PARAMS {
                        return Err("54000");
                    }
                    if bytes[at] == b')' {
                        self.0 = &tail[at + 1..];
                        return Ok(items);
                    }
                    start = at + 1;
                }
                b']' => return Err("42601"),
                _ => {}
            }
            at += 1;
        }
        Err("42601")
    }
}

pub(super) fn is_prepare(sql: &str) -> bool {
    Prefix(sql).kw("PREPARE")
}

pub(super) fn recognized(sql: &str) -> bool {
    Prefix(sql).word().is_some_and(|word| {
        ["PREPARE", "EXECUTE", "DEALLOCATE", "DISCARD"]
            .iter()
            .any(|kw| word.eq_ignore_ascii_case(kw))
    })
}

pub(super) fn parse(sql: &str, diagnostic: &mut Diagnostic) -> R<Option<Command>> {
    let mut p = Prefix(sql);
    let command = if p.kw("PREPARE") {
        // PREPARE TRANSACTION followed by a string is a separate command.
        let start = p.0;
        if p.kw("TRANSACTION") && p.0.trim_start().starts_with('\'') {
            return Ok(None);
        }
        p.0 = start;
        let name = p.name()?;
        let types = p.list()?;
        if !p.kw("AS") {
            return Err("42601");
        }
        let query = p.0.trim();
        if !query.starts_with('(')
            && !Prefix(query).word().is_some_and(|word| {
                [
                    "SELECT", "VALUES", "INSERT", "UPDATE", "DELETE", "MERGE", "WITH",
                ]
                .iter()
                .any(|kw| word.eq_ignore_ascii_case(kw))
            })
        {
            diagnostic.wire_message = Some(match Prefix(query).word() {
                Some(word) => format!("syntax error at or near \"{word}\""),
                None => "syntax error at end of input".into(),
            });
            return Err("42601");
        }
        return Ok(Some(Command::Prepare {
            name,
            types,
            query: query.into(),
            source: sql.into(),
        }));
    } else if p.kw("EXECUTE") {
        let name = p.name()?;
        let args = p.list()?;
        Command::Execute { name, args }
    } else if p.kw("DEALLOCATE") {
        p.kw("PREPARE");
        Command::Deallocate(if p.kw("ALL") { None } else { Some(p.name()?) })
    } else if p.kw("DISCARD") {
        Command::Discard(if p.kw("ALL") {
            Discard::All
        } else if p.kw("PLANS") {
            Discard::Plans
        } else if p.kw("SEQUENCES") {
            Discard::Sequences
        } else if p.kw("TEMP") || p.kw("TEMPORARY") {
            Discard::Temp
        } else {
            return Err("42601");
        })
    } else {
        return Ok(None);
    };
    if !p.0.trim().is_empty() {
        return Err("42601");
    }
    Ok(Some(command))
}

pub(super) fn argument_type_name(oid: u32) -> R<&'static str> {
    Ok(match oid {
        16 => "boolean",
        17 => "bytea",
        19 => "name",
        25 => "text",
        20 => "bigint",
        26 => "oid",
        21 => "smallint",
        23 => "integer",
        1043 => "character varying",
        1042 => "character",
        2950 => "uuid",
        3802 => "jsonb",
        1007 => "integer[]",
        1016 => "bigint[]",
        1009 => "text[]",
        1700 => "numeric",
        700 => "real",
        701 => "double precision",
        1082 => "date",
        1083 => "time without time zone",
        1266 => "time with time zone",
        1114 => "timestamp without time zone",
        1184 => "timestamp with time zone",
        1186 => "interval",
        2206 => "regtype",
        _ => return Err("0A000"),
    })
}

fn parameter_references(sql: &str) -> R<std::collections::BTreeSet<usize>> {
    giggles_core::relational::parameter_references(sql)
        .map_err(|error| crate::service::sqlstate(&CoreError::Sql(error)))
}

pub(super) fn guard(session: &Session) -> R<()> {
    if !session.role.is_sql() {
        return Err("42501");
    }
    if session.is_aborted() {
        return Err("25P02");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn command(sql: &str) -> Command {
        let sql = simple::split(sql).unwrap().pop().unwrap();
        parse(&sql, &mut Diagnostic::default()).unwrap().unwrap()
    }

    #[test]
    fn names_types_comments_and_terminator() {
        let Command::Prepare {
            name, types, query, ..
        } = command(
            "/* outside */ PrEpArE \"Quoted\"\" Name\"(numeric(8, 2), text[]) AS SELECT $1;",
        )
        else {
            panic!("expected PREPARE")
        };
        assert_eq!(name, "Quoted\" Name");
        assert_eq!(types, ["numeric(8, 2)", "text[]"]);
        assert_eq!(query, "SELECT $1");
        assert!(matches!(
            command("DEALLOCATE PREPARE ALL;"),
            Command::Deallocate(None)
        ));
        assert!(
            matches!(command("DEALLOCATE \"ALL\";"), Command::Deallocate(Some(name)) if name == "ALL")
        );
        assert!(
            matches!(command("DEALLOCATE Mixed;"), Command::Deallocate(Some(name)) if name == "mixed")
        );
    }

    #[test]
    fn arguments_respect_lexical_boundaries() {
        let Command::Execute { name, args } =
            command("EXECUTE MiX(coalesce(1,2), ARRAY[3,4], $$;,)$$, E'foo\\'bar,');")
        else {
            panic!("expected EXECUTE")
        };
        assert_eq!(name, "mix");
        assert_eq!(
            args,
            ["coalesce(1,2)", "ARRAY[3,4]", "$$;,)$$", "E'foo\\'bar,'"]
        );
        for sql in [
            "EXECUTE a()",
            "EXECUTE a(1,)",
            "EXECUTE a(ARRAY[1,2)",
            "DISCARD ALL x",
        ] {
            assert!(
                matches!(parse(sql, &mut Diagnostic::default()), Err("42601")),
                "{sql}"
            );
        }
    }
}

pub(super) fn parse_protocol(sql: &str, diagnostic: &mut Diagnostic) -> R<Option<Command>> {
    let head = sql.trim_start();
    if !recognized(head) && !head.starts_with("/*") && !head.starts_with("--") {
        return Ok(None);
    }
    let statements = simple::split(sql)?;
    let Some(first) = statements.first() else {
        return Ok(None);
    };
    if !recognized(first) {
        return Ok(None);
    }
    if statements.len() != 1 {
        return Err("42601");
    }
    let mut command = parse(first, diagnostic)?;
    if let Some(Command::Prepare { source, .. }) = &mut command {
        *source = sql.to_owned();
    }
    Ok(command)
}

#[derive(Clone)]
pub(super) struct Origin {
    source_text: Option<std::sync::Arc<str>>,
    prepare_time: i64,
    generic_plans: i64,
    custom_plans: i64,
    pub unused_types: Vec<u32>,
    pub unused_slots: Vec<usize>,
}
impl Origin {
    pub fn protocol(session: &mut Session) -> Self {
        Self {
            source_text: None,
            prepare_time: session.copy_clock().statement_us,
            generic_plans: 0,
            custom_plans: 0,
            unused_types: Vec::new(),
            unused_slots: Vec::new(),
        }
    }

    fn count_plan(&mut self, parameterized: bool, mode: &str) {
        // Auto samples five custom plans. The engine has no comparative
        // generic/custom planner cost model.
        let custom = parameterized
            && match mode {
                "force_generic_plan" => false,
                "force_custom_plan" => true,
                _ => self.custom_plans < 5,
            };
        if custom {
            self.custom_plans = self.custom_plans.saturating_add(1);
        } else {
            self.generic_plans = self.generic_plans.saturating_add(1);
        }
    }
}

impl Statement {
    pub(super) fn plan_parameter_count(&self) -> usize {
        self.parameter_oids.len()
            - self
                .origin
                .as_ref()
                .map_or(0, |origin| origin.unused_types.len())
    }

    pub(super) fn has_unused_slot(&self, index: usize) -> bool {
        index >= self.plan_parameter_count()
            || self
                .origin
                .as_ref()
                .is_some_and(|origin| origin.unused_slots.binary_search(&index).is_ok())
    }

    fn supports_sql_execute(&self) -> bool {
        match self.p.as_ref() {
            Plan::Sql(prepared) => matches!(
                prepared.command(),
                "SELECT" | "INSERT" | "UPDATE" | "DELETE" | "MERGE"
            ),
            Plan::Catalog(..) => true,
            _ => false,
        }
    }

    pub(super) fn authorize_execute_description(&self, session: &mut Session) -> R<()> {
        if let Plan::Catalog(plan, ..) = self.p.as_ref() {
            catalog_wire::authorize_application_relations(session, plan)?;
        }
        self.authorize_description(session)
    }

    pub(super) fn append_unused_parameters(&mut self, types: &[u32]) {
        self.parameter_oids.extend_from_slice(types);
        Arc::make_mut(&mut self.d).parameters.extend(
            types
                .iter()
                .map(|&oid| core_declared_type(oid).unwrap_or(DataType::Text)),
        );
    }
}

impl State {
    pub(super) fn check_named_statement(&mut self, name: &str, bytes: usize) -> R<()> {
        if self.statements.contains_key(name) {
            self.diagnostic.wire_message =
                Some(format!("prepared statement \"{name}\" already exists"));
            return Err("42P05");
        }
        if self
            .statements
            .keys()
            .filter(|name| !name.is_empty())
            .count()
            >= MAX_NAMED_STATEMENTS
            || self
                .named_statement_bytes
                .checked_add(bytes)
                .ok_or("54000")?
                > MAX_NAMED_STATEMENT_BYTES
        {
            return Err("54000");
        }
        Ok(())
    }

    pub(super) fn store_named_statement(&mut self, name: String, statement: Statement) {
        debug_assert!(!name.is_empty() && !self.statements.contains_key(&name));
        self.named_statement_bytes += statement.sql_bytes;
        self.statements.insert(name, statement);
    }

    pub(super) fn remove_statement(&mut self, name: &str, session: &Session) -> bool {
        let Some(statement) = self.statements.remove(name) else {
            return false;
        };
        statement.forget_cursor_plan(session);
        if !name.is_empty() {
            self.named_statement_bytes -= statement.sql_bytes;
        }
        true
    }

    pub(super) fn note_plan_use(&mut self, name: &str) {
        if let Some(statement) = self.statements.get_mut(name)
            && let Some(origin) = statement.origin.as_mut()
        {
            let mode = self.settings.current.registered.get("plan_cache_mode");
            origin.count_plan(
                !statement.parameter_oids.is_empty(),
                mode.map_or("auto", String::as_str),
            );
        }
    }

    fn deallocate_all(&mut self, session: &Session) {
        self.statements.retain(|name, statement| {
            if name.is_empty() {
                return true;
            }
            statement.forget_cursor_plan(session);
            false
        });
        self.named_statement_bytes = 0;
    }

    fn discard_plans(&mut self) {
        self.unnamed_cache.clear();
        for statement in self.statements.values_mut() {
            if matches!(
                statement.p.as_ref(),
                Plan::Sql(_) | Plan::Catalog(..) | Plan::Copy(..)
            ) {
                statement.discarded = true;
                statement.validated = None;
            }
        }
    }

    fn missing_statement(&mut self, name: &str) -> &'static str {
        self.diagnostic.wire_message =
            Some(format!("prepared statement \"{name}\" does not exist"));
        "26000"
    }

    fn sql_prepare(
        &mut self,
        name: String,
        types: &[String],
        query: &str,
        source: String,
        session: &mut Session,
    ) -> R<()> {
        if source.len() > MAX_NAMED_STATEMENT_BYTES {
            return Err("54000");
        }
        let mut declared = Vec::with_capacity(types.len());
        if !types.is_empty() {
            let (epoch, tables, indexes, typmods, user_types) =
                core(session.catalog_snapshot_with_typmods())?;
            let snapshot = catalog_wire::with_search_path(
                catalog_wire::snapshot(session, epoch, tables, indexes, typmods, user_types)?,
                &self.settings.current.search_path,
            );
            for (name, oid) in types.iter().zip(
                giggles_pg_catalog::resolve_type_names(&snapshot, types)
                    .map_err(|error| self.diagnostic.catalog_error(error))?,
            ) {
                let Some(oid) = oid else {
                    self.diagnostic.wire_message = Some(format!(r#"type "{name}" does not exist"#));
                    return Err("42704");
                };
                if core_declared_type(oid).is_none() && catalog_wire::parameter_type(oid).is_err() {
                    return Err("0A000");
                }
                declared.push(oid);
            }
        }
        let _budgets = session.sql.statement_budgets();
        let references = parameter_references(query)?;
        let count = references.last().copied().unwrap_or(0);
        if let Some(missing) = (declared.len() + 1..count).find(|i| !references.contains(i)) {
            self.diagnostic.wire_message = Some(format!(
                "could not determine data type of parameter ${missing}"
            ));
            return Err("42P18");
        }
        let unused_types = declared.split_off(count.min(declared.len()));
        let unused_slots = (0..declared.len())
            .filter(|i| !references.contains(&(i + 1)))
            .collect::<Vec<_>>();
        let mut statement = self
            .prepare_statement(query, declared, &unused_slots, session, false)
            .inspect_err(|code| {
                if *code == "42P01"
                    && let Some(missing) = session.sql.missing_prepared_relation(query)
                {
                    self.diagnostic.wire_message =
                        Some(format!(r#"relation "{missing}" does not exist"#));
                }
            })?;
        if !matches!(statement.p.as_ref(), Plan::Sql(_) | Plan::Catalog(..)) {
            return Err("0A000");
        }
        if let Plan::Sql(prepared) = statement.p.as_ref()
            && !matches!(
                prepared.command(),
                "SELECT" | "INSERT" | "UPDATE" | "DELETE" | "MERGE"
            )
        {
            return Err("42601");
        }
        self.check_named_statement(&name, source.len())?;
        statement.append_unused_parameters(&unused_types);
        statement.sql_bytes = source.len();
        statement.origin = Some(Box::new(Origin {
            source_text: Some(std::sync::Arc::from(source)),
            unused_types,
            unused_slots,
            ..Origin::protocol(session)
        }));
        self.store_named_statement(name, statement);
        Ok(())
    }

    pub(super) fn prepare_command(
        &mut self,
        command: Command,
        outer_oids: &[u32],
    ) -> R<(Plan, Description)> {
        self.diagnostic.phase = "prepared statement SQL";
        let target = match &command {
            Command::Execute { name, .. } => self.execute_target(name).cloned().map(Box::new),
            _ => None,
        };
        let mut description = target.as_ref().map_or_else(
            || Description {
                type_oids: vec![],
                typmods: vec![],
                columns: vec![],
                parameters: vec![],
            },
            |target| (*target.d).clone(),
        );
        description.parameters = outer_oids
            .iter()
            .map(|&oid| core_declared_type(oid).ok_or("0A000"))
            .collect::<R<_>>()?;
        let invocation = Invocation { command, target };
        Ok((Plan::PreparedCommand(Box::new(invocation)), description))
    }

    fn execute_target(&self, name: &str) -> Option<&Statement> {
        self.statements.get(name).filter(|s| s.supports_sql_execute())
    }

    pub(super) fn authorize_live_execute_target(
        &self,
        invocation: &Invocation,
        session: &mut Session,
    ) -> R<()> {
        if let Command::Execute { name, .. } = &invocation.command {
            guard(session)?;
            if let Some(target) = self.execute_target(name) {
                target.authorize_execute_description(session)?;
            }
        }
        Ok(())
    }

    /// Save the current target's description on the portal's copy. A missing
    /// target yields NoData and is reported by the Execute message.
    pub(super) fn bind_execute_description(
        &mut self,
        invocation: &mut Invocation,
        description: &mut Description,
        session: &mut Session,
    ) -> R<()> {
        let Command::Execute { name, .. } = &invocation.command else {
            return Ok(());
        };
        self.authorize_live_execute_target(invocation, session)?;
        invocation.target = None;
        description.columns.clear();
        description.type_oids.clear();
        description.typmods.clear();
        if self.execute_target(name).is_some() {
            self.refresh_statement(name, session)?;
            let target = &self.statements[name];
            let mut current = (*target.describe(session)?).clone();
            current.parameters = std::mem::take(&mut description.parameters);
            *description = current;
            invocation.target = Some(Box::new(target.clone()));
        }
        Ok(())
    }

    fn evaluate_execute_arguments(
        &mut self,
        args: &[String],
        statement: &Statement,
        session: &mut Session,
    ) -> R<Vec<Value>> {
        use giggles_core::relational::execute_arguments::{self, Syntax};

        let _budgets = session.sql.statement_budgets();
        let mut literals = Vec::with_capacity(args.len());
        for arg in args {
            let syntax = execute_arguments::syntax(arg)
                .map_err(|error| crate::service::sqlstate(&CoreError::Sql(error)))?;
            literals.push(match syntax {
                Syntax::Untyped(value) => Some(value),
                Syntax::Typed => None,
                Syntax::Subquery => {
                    self.diagnostic.wire_message =
                        Some("cannot use subquery in EXECUTE parameter".into());
                    return Err("0A000");
                }
            });
        }
        let sql = format!("SELECT ({})", args.join("), ("));
        if let Some(parameter) = parameter_references(&sql)?.first() {
            self.diagnostic.wire_message = Some(format!("there is no parameter ${parameter}"));
            return Err("42P02");
        }
        let (Plan::Sql(arguments), description) =
            prepare_declared_sql_diagnostic(&mut self.diagnostic, session, &sql, &[], false)?
        else {
            unreachable!()
        };
        let clock = session.copy_clock();
        let mut assignments = Vec::with_capacity(args.len());
        // PostgreSQL invokes the input function for an unknown constant during
        // analysis, before any expression (including an unused one) can run.
        for (i, ((source, &oid), literal)) in description
            .columns
            .iter()
            .zip(&statement.parameter_oids)
            .zip(literals)
            .enumerate()
        {
            let source_type = &source.data_type;
            let target = match oid {
                19 => DataType::Text,
                26 => DataType::BigInt,
                _ => core_declared_type(oid).ok_or("0A000")?,
            };
            let allowed = if oid == 26 {
                matches!(source_type, DataType::SmallInt | DataType::Integer | DataType::BigInt)
            } else {
                execute_arguments::assignment_allowed(source_type, &target)
            };
            if literal.is_none() && !allowed {
                let source = argument_type_name(super::oid(&source.data_type))?;
                let target = argument_type_name(oid)?;
                self.diagnostic.wire_message = Some(format!(
                    "parameter ${} of type {source} cannot be coerced to the expected type {target}",
                    i + 1
                ));
                self.diagnostic.wire_hint =
                    Some("You will need to rewrite or cast the expression.".into());
                return Err("42804");
            }
            let converted = literal
                .map(|value| {
                    if oid == 26 {
                        self.execute_oid_literal(value)
                    } else {
                        self.diagnostic.core(
                            session.sql.coerce_execute_argument(
                                value, source_type, &target, true, &clock,
                            ),
                        )
                    }
                })
                .transpose()?;
            assignments.push((source_type, target, oid, converted));
        }
        let result = self
            .diagnostic
            .core(session.execute_prepared_unreleased(&arguments, &[]))?;
        let released = self.diagnostic.core(session.release_page(result))?;
        let [row] = released.get().rows.as_slice() else {
            return Err("0A000");
        };
        row.values
            .iter()
            .cloned()
            .zip(assignments)
            .map(|(value, (source, target, oid, converted))| {
                if let Some(converted) = converted {
                    return Ok(converted);
                }
                if oid == 26 {
                    return match value {
                        Value::Null => Ok(Value::Null),
                        Value::Int(n) if *source != DataType::BigInt => {
                            Ok(Value::Int(i64::from(n as u32)))
                        }
                        Value::Int(n) if u32::try_from(n).is_ok() => Ok(Value::Int(n)),
                        _ => {
                            self.diagnostic.message = Some("OID out of range");
                            Err("22003")
                        }
                    };
                }
                self.diagnostic.core(
                    session.sql.coerce_execute_argument(value, source, &target, false, &clock),
                )
            })
            .collect()
    }

    fn execute_oid_literal(&mut self, value: Value) -> R<Value> {
        let Value::Text(text) = value else {
            return Ok(value);
        };
        catalog_wire::parse_oid_text(&text)
            .map(|oid| Value::Int(i64::from(oid)))
            .inspect_err(|&code| self.diagnostic.oid_text_error(code, &text))
    }

    pub(super) fn bind_sql_execute(
        &mut self,
        name: &str,
        args: &[String],
        session: &mut Session,
        implicit: &mut bool,
    ) -> R<Portal> {
        guard(session)?;
        let Some(target) = self.statements.get(name) else {
            return Err(self.missing_statement(name));
        };
        if args.len() != target.parameter_oids.len() {
            self.diagnostic.wire_message = Some(format!(
                r#"wrong number of parameters for prepared statement "{name}""#
            ));
            self.diagnostic.wire_detail = Some(format!(
                "Expected {} parameters but got {}.",
                target.parameter_oids.len(),
                args.len()
            ));
            return Err("42601");
        }
        if !target.supports_sql_execute() {
            return Err("0A000");
        }
        let mut statement = self.bind_statement(name, session, implicit)?;
        if let Plan::Catalog(plan, ..) = statement.p.as_ref() {
            catalog_wire::authorize_application_relations(session, plan)?;
        }
        let (params, catalog_params) = if args.is_empty() {
            (vec![], vec![])
        } else {
            let mut params = self.evaluate_execute_arguments(args, &statement, session)?;
            params.truncate(statement.plan_parameter_count());
            if matches!(statement.p.as_ref(), Plan::Catalog(..)) {
                let catalog = params
                    .into_iter()
                    .zip(&statement.parameter_oids)
                    .enumerate()
                    .map(|(i, (value, &oid))| {
                        if statement.has_unused_slot(i) {
                            return Ok(giggles_pg_catalog::Value::Null);
                        }
                        let ty = core_declared_type(oid).unwrap_or(if oid == 26 {
                            DataType::BigInt
                        } else {
                            DataType::Text
                        });
                        if oid == 26 {
                            return match value {
                                Value::Null => Ok(giggles_pg_catalog::Value::Null),
                                Value::Int(i) if u32::try_from(i).is_ok() => {
                                    Ok(giggles_pg_catalog::Value::Int(i))
                                }
                                _ => Err("22003"),
                            };
                        }
                        let bytes = encode(
                            &value,
                            &ty,
                            0,
                            &self.settings.current,
                            session.zone().unwrap_or(SessionZone::Fixed(0)),
                        )?;
                        bytes.map_or(Ok(giggles_pg_catalog::Value::Null), |bytes| {
                            catalog_wire::decode(&bytes, catalog_wire::parameter_type(oid)?, 0)
                        })
                    })
                    .collect::<R<Vec<_>>>()?;
                (vec![], catalog)
            } else {
                (params, vec![])
            }
        };
        let parameter_bytes = catalog_params.iter().try_fold(
            serde_json::to_vec(&params).map_err(|_| "54000")?.len(),
            |total, value| {
                total
                    .checked_add(catalog_wire::value_bytes(value)?)
                    .ok_or("54000")
            },
        )?;
        if parameter_bytes > MAX_RESPONSE {
            return Err("54000");
        }
        self.validate_statement_binding(name, &mut statement, session)?;
        self.note_plan_use(name);
        Ok(Portal::new(
            statement,
            params,
            catalog_params,
            vec![],
            parameter_bytes,
        ))
    }

    pub(super) fn simple_prepared_command(
        &mut self,
        command: Command,
        session: &mut Session,
        implicit: &mut bool,
        out: &mut Vec<u8>,
    ) -> R<()> {
        if let Command::Execute { name, args } = command {
            let mut portal = self.bind_sql_execute(&name, &args, session, implicit)?;
            if let Some(fields) = portal.s.catalog_fields() {
                catalog_wire::description(fields, &[], out)?;
            } else if !portal.s.d.columns.is_empty() {
                row_description(&portal.s.d, &[], out)?;
            }
            self.execute_session_portal(&mut portal, session, implicit, 0, 0, out)
        } else {
            self.execute_prepared_utility(command, session, *implicit, out)
        }
    }

    pub(super) fn execute_prepared_utility(
        &mut self,
        command: Command,
        session: &mut Session,
        implicit: bool,
        out: &mut Vec<u8>,
    ) -> R<()> {
        guard(session)?;
        self.diagnostic.phase = "prepared statement SQL";
        let tag = match command {
            Command::Prepare {
                name,
                types,
                query,
                source,
            } => {
                self.sql_prepare(name, &types, &query, source, session)?;
                "PREPARE"
            }
            Command::Deallocate(Some(name)) => {
                if !self.remove_statement(&name, session) {
                    return Err(self.missing_statement(&name));
                }
                "DEALLOCATE"
            }
            Command::Deallocate(None) => {
                self.deallocate_all(session);
                "DEALLOCATE ALL"
            }
            Command::Discard(Discard::Plans) => {
                self.discard_plans();
                "DISCARD PLANS"
            }
            Command::Discard(Discard::Temp) => "DISCARD TEMP",
            Command::Discard(Discard::Sequences) => {
                self.diagnostic.core(session.sql.discard_sequence_state())?;
                "DISCARD SEQUENCES"
            }
            Command::Discard(Discard::All) => {
                if session.in_transaction() || implicit {
                    self.diagnostic.wire_message =
                        Some("DISCARD ALL cannot run inside a transaction block".into());
                    return Err("25001");
                }
                self.cursors.close_all();
                self.portals.clear();
                let before = self.settings.current.clone();
                if matches!(session.role, Role::Database { .. }) {
                    use crate::pg_settings::{Setting, Show};
                    session.check_role_setting(Show::SessionAuthorization, None)?;
                    self.settings.apply(
                        &Setting::Database {
                            name: Show::SessionAuthorization,
                            value: None,
                            local: false,
                            reset: false,
                        },
                        false,
                    );
                    session.pin_role_settings(&mut self.settings.current)?;
                    session.sync_role_settings(&mut self.settings.current)?;
                }
                self.settings
                    .reset_all(replication_role_allowed(session)?, false);
                session.sync_role_settings(&mut self.settings.current)?;
                settings_status(&before, &self.settings, session, out)?;
                self.deallocate_all(session);
                session
                    .service
                    .advisory
                    .call(session, "pg_advisory_unlock_all", (false, 0))?;
                self.discard_plans();
                self.diagnostic.core(session.sql.discard_sequence_state())?;
                "DISCARD ALL"
            }
            Command::Execute { .. } => unreachable!("EXECUTE runs through its bound portal"),
        };
        complete(tag, &empty(), out)
    }

    pub(super) fn prepared_snapshot(
        &self,
        plan: &giggles_pg_catalog::Plan,
        mut snapshot: giggles_pg_catalog::CatalogSnapshot,
    ) -> giggles_pg_catalog::CatalogSnapshot {
        if plan.reads_prepared_statements() {
            snapshot.prepared_statements = self
                .statements
                .iter()
                .filter_map(|(name, s)| {
                    let origin = s.origin.as_ref()?;
                    let result_types =
                        (!s.d.columns.is_empty()).then(|| result_type_oids(&s.d));
                    Some(giggles_pg_catalog::PreparedStatementRow {
                        name: name.clone(),
                        statement: origin.source_text.as_deref().unwrap_or(&s.sql).to_owned(),
                        prepare_time: origin.prepare_time,
                        parameter_types: s.parameter_oids.clone(),
                        result_types,
                        from_sql: origin.source_text.is_some(),
                        generic_plans: origin.generic_plans,
                        custom_plans: origin.custom_plans,
                    })
                })
                .collect();
        }
        snapshot
    }
}
