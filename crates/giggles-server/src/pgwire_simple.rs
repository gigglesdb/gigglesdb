//! Simple Query dispatch. Agent requests retain the original whole-query path.
use super::*;

impl State {
    pub(super) fn simple_query(
        &mut self,
        sql: &str,
        session: &mut Session,
        out: &mut Vec<u8>,
        received: std::time::Instant,
    ) -> R<()> {
        let mut implicit = false;
        if !matches!(session.role, Role::Database { .. }) {
            return self.simple_statement(sql, session, out, received, &mut implicit);
        }
        let statements = split(sql)?;
        if statements.is_empty() {
            return frame(b'I', &[], out);
        }
        if statements.len() == 1 {
            return self.simple_statement(&statements[0], session, out, received, &mut implicit);
        }
        // Role/ACL and DROP TABLE operations publish outside the SQL snapshot.
        // Like CREATE DATABASE in PostgreSQL, these must be the sole statement
        // in a Query message, even if the message contains explicit COMMITs.
        if statements
            .iter()
            .any(|sql| crate::auth::statements::parse(sql).is_some())
        {
            return Err("25001");
        }
        let mut statements = statements.into_iter();
        while let Some(sql) = statements.next() {
            let completed = out.len();
            let result = (|| {
                if parse_control(&sql, session, &mut self.diagnostic)?.is_none_or(|command| {
                    matches!(
                        command,
                        crate::pg_transaction::Command::Set(_)
                            | crate::pg_transaction::Command::Session(_)
                    )
                }) {
                    begin_implicit(session, &mut implicit, &mut self.settings)?;
                }
                self.simple_statement(&sql, session, out, received, &mut implicit)?;
                // Reserve space for an error and ReadyForQuery before publishing
                // the implicit transaction, even near the aggregate wire cap.
                if out.len() > MAX_RESPONSE - 1024 {
                    return Err("54000");
                }
                Ok(())
            })();
            if let Err(code) = result {
                self.simple_failure(code, implicit, completed, session, out);
                return Ok(());
            }
            if let Some(copy) = self.copy.as_mut() {
                copy.remaining = statements.collect();
                return Ok(());
            }
            // A later BEGIN in this message must not preserve portals from a
            // transaction ended by an earlier COMMIT or ROLLBACK.
            if !session.in_transaction() {
                self.portals.clear();
            }
        }
        if implicit {
            let completed = out.len();
            if let Err(code) =
                execute_control("COMMIT", session, &mut implicit, &mut self.settings, out)
            {
                self.simple_failure(code, true, completed, session, out);
            }
        }
        Ok(())
    }

    fn simple_failure(
        &mut self,
        code: &'static str,
        implicit: bool,
        completed: usize,
        session: &mut Session,
        out: &mut Vec<u8>,
    ) {
        let before = self.settings.current.clone();
        session.abort();
        if implicit {
            session.sql.rollback();
            self.cursors.transaction_end("ROLLBACK");
            self.settings.transaction(true, false, "ROLLBACK");
            self.portals.clear();
        }
        // Keep successful statements' responses; discard only a partially
        // encoded failing statement. The caller emits exactly one ReadyForQuery.
        out.truncate(completed);
        let _ = settings_status(&before, &self.settings, session, out);
        let mut error = Vec::new();
        let message = self.diagnostic.message.unwrap_or(match code {
            "0A000" => "unsupported SQL feature or parameter type in this server profile",
            "42601" => "statement syntax is not supported by this server SQL profile",
            _ => "request rejected; consult documented code",
        });
        diagnostic_error(code, message, Some(&self.diagnostic), &mut error);
        out.extend(error);
    }

    // Shared by single and multi-statement Query messages, including every
    // authorization, admin, settings, catalog and core SQL execution path.
    pub(super) fn simple_statement(
        &mut self,
        sql: &str,
        session: &mut Session,
        out: &mut Vec<u8>,
        received: std::time::Instant,
        implicit: &mut bool,
    ) -> R<()> {
        // A multi-statement Query needs settings changes before its next SQL
        // statement, rather than only at the enclosing protocol-frame boundary.
        session.set_time_zone(&self.settings.current.timezone);
        session.set_extra_float_digits(self.settings.current.extra_float_digits);
        let result = self.simple_statement_inner(sql, session, out, received, implicit);
        session.set_time_zone(&self.settings.current.timezone);
        session.set_extra_float_digits(self.settings.current.extra_float_digits);
        result
    }

    fn simple_statement_inner(
        &mut self,
        sql: &str,
        session: &mut Session,
        out: &mut Vec<u8>,
        received: std::time::Instant,
        implicit: &mut bool,
    ) -> R<()> {
        if let Some(command) = cursor::parse(sql)? {
            self.cursors
                .execute(&command, session, &[], &[], &mut self.settings, true, out)?;
            return Ok(());
        }
        if is_copy_sql(sql) {
            let (copy, prepared) = core(session.prepare_copy(sql))?;
            read_only_check("INSERT", &self.settings, session, &mut self.diagnostic)?;
            self.settings.queried = session.in_transaction();
            self.start_copy(copy, Box::new(prepared), true, session, out)?;
            if let Some(copy) = self.copy.as_mut() {
                copy.simple_implicit = *implicit;
            }
            return Ok(());
        }
        core(session.authorize_sequence_sql(sql))?;
        if let Some(json) = sql.strip_prefix("GIGGLES ") {
            if !matches!(session.role, Role::Task { .. }) {
                return Err("42501");
            }
            let req: Request = serde_json::from_str(json).map_err(|_| "08P01")?;
            if matches!(req, Request::Sql { .. } | Request::Ping) {
                return Err("42501");
            }
            let response = session.handle_received(req, received);
            let value = serde_json::to_string(&response).map_err(|_| "XX000")?;
            let d = Description {
                typmods: vec![-1],
                parameters: vec![],
                columns: vec![giggles_core::relational::ColumnDef {
                    id: 0,
                    name: "decision".into(),
                    data_type: DataType::Text,
                    not_null: true,
                }],
            };
            let result = SqlResult {
                columns: vec!["decision".into()],
                rows: vec![giggles_core::relational::SqlRow {
                    values: vec![Value::Text(value)],
                    version: 0,
                }],
                affected: 0,
            };
            row_description(&d, &[], out)?;
            rows(
                self.settings.current.extra_float_digits,
                session.zone().unwrap_or(SessionZone::Fixed(0)),
                &result,
                &d,
                &[],
                0,
                1,
                out,
            )?;
            complete("SELECT", &result, out)?;
        } else if let Some(stmt) = crate::auth::statements::parse(sql) {
            read_only_check(
                admin_command(&stmt),
                &self.settings,
                session,
                &mut self.diagnostic,
            )?;
            let tag = session.execute_admin(&stmt)?;
            self.admin_completed = true;
            complete(tag, &SqlResult::default(), out)?;
        } else if crate::pg_settings::empty_sql(sql)? {
            frame(b'I', &[], out)?;
        } else if let Some(command) = parse_control(sql, session, &mut self.diagnostic)? {
            let actual = execute_mode(
                command,
                session,
                implicit,
                &mut self.settings,
                &mut self.diagnostic,
                out,
            )?;
            if !session.in_transaction() {
                self.cursors.transaction_end(actual);
            }
            complete(actual, &SqlResult::default(), out)?;
        } else if let Some(show) = show_settings(sql, session, &mut self.diagnostic)? {
            let (description, result) = inspect(show, &self.settings.current, session)?;
            row_description(&description, &[], out)?;
            rows(
                self.settings.current.extra_float_digits,
                session.zone().unwrap_or(SessionZone::Fixed(0)),
                &result,
                &description,
                &[],
                0,
                1,
                out,
            )?;
            complete("SHOW", &result, out)?;
        } else if let Some(settings) = parse_settings(sql, session, &mut self.diagnostic)? {
            if !session.role.is_sql() {
                return Err("42501");
            }
            if session.is_aborted() {
                return Err("25P02");
            }
            execute_settings(
                &settings,
                &mut self.settings,
                session,
                session.in_transaction(),
                out,
            )?;
        } else if let Some(function) =
            settings_functions::parse(sql, session, &mut self.diagnostic)?
        {
            self.simple_settings_function(&function, session, implicit, out)?;
        } else if !session.role.is_sql() && Prepared::rejects_unsupported_table_free_scalar(sql) {
            // DD-0068: authority precedes routine classification. Sessions without
            // SQL authority (task bindings) must not learn whether a routine
            // exists; they get the same 42501 that application prepare gives them.
            self.diagnostic.phase = "application SQL";
            return Err("42501");
        } else if let Some(plan) = catalog_plan(sql, &mut self.diagnostic, session)? {
            self.diagnostic.phase = "catalog execution";
            self.settings.queried = session.in_transaction();
            let (_, tables, indexes, typmods) = core(session.catalog_snapshot_with_typmods())?;
            let result = catalog_wire::execute(
                &plan,
                &catalog_wire::with_application_rows(
                    session,
                    &plan,
                    catalog_wire::snapshot(session, tables, indexes, typmods),
                )?,
                &[],
            )
            .map_err(catalog_wire::error)?;
            catalog_wire::retained_bytes(&result)?;
            catalog_wire::description(&result.fields, &[], out)?;
            catalog_wire::rows(
                self.settings.current.extra_float_digits,
                &result,
                &[],
                0,
                result.rows.len(),
                out,
            )?;
            catalog_complete(result.rows.len(), out)?;
        } else {
            self.diagnostic.phase = "application SQL";
            let was_transaction = session.in_transaction();
            // ROLLBACK must bypass normal prepare when already aborted.
            if session.is_aborted()
                && sql
                    .trim()
                    .trim_end_matches(';')
                    .eq_ignore_ascii_case("rollback")
            {
                let result = core(session.execute_sql(sql, &[]))?;
                self.settings
                    .transaction(was_transaction, session.in_transaction(), "ROLLBACK");
                complete("ROLLBACK", &result, out)?;
            } else {
                let p = core(session.prepare(sql))?;
                let d = core(session.describe(&p))?;
                read_only_check(p.command(), &self.settings, session, &mut self.diagnostic)?;
                self.settings.queried = session.in_transaction();
                let result = core(session.execute_prepared(&p, &[]))?;
                self.settings
                    .transaction(was_transaction, session.in_transaction(), p.command());
                if !d.columns.is_empty() {
                    row_description(&d, &[], out)?;
                    rows(
                        self.settings.current.extra_float_digits,
                        session.zone().unwrap_or(SessionZone::Fixed(0)),
                        &result,
                        &d,
                        &[],
                        0,
                        result.rows.len(),
                        out,
                    )?;
                }
                complete(p.command(), &result, out)?;
            }
        }
        Ok(())
    }
}

// A boundary lexer, not a second SQL parser. Preserve quoted bytes exactly;
// replace comments with whitespace so every dispatch path sees the same SQL.
pub(super) fn split(sql: &str) -> R<Vec<String>> {
    if sql.len() > giggles_core::relational::MAX_SQL {
        return Err("54000");
    }
    let b = sql.as_bytes();
    let mut statements = Vec::new();
    let mut statement = Vec::new();
    let mut at = 0;
    while at < b.len() {
        let start = at;
        if b[at..].starts_with(b"--") {
            while at < b.len() && !matches!(b[at], b'\n' | b'\r') {
                at += 1;
            }
            statement.push(b' ');
            continue;
        }
        if b[at..].starts_with(b"/*") {
            let mut depth = 1;
            at += 2;
            while depth > 0 {
                if at == b.len() {
                    return Err("42601");
                }
                if b[at..].starts_with(b"/*") {
                    depth += 1;
                    if depth > 16 {
                        return Err("54000");
                    }
                    at += 2;
                } else if b[at..].starts_with(b"*/") {
                    depth -= 1;
                    at += 2;
                } else {
                    // Newlines matter for PostgreSQL's adjacent string literals.
                    if matches!(b[at], b'\n' | b'\r') {
                        statement.push(b[at]);
                    }
                    at += 1;
                }
            }
            statement.push(b' ');
            continue;
        }
        if matches!(b[at], b'\'' | b'"') {
            let quote = b[at];
            let escape = quote == b'\''
                && at > 0
                && matches!(b[at - 1], b'e' | b'E')
                && (at == 1 || !identifier(b[at - 2]));
            at += 1;
            loop {
                if at == b.len() {
                    return Err("42601");
                }
                if escape && b[at] == b'\\' {
                    at += 1;
                    if at == b.len() {
                        return Err("42601");
                    }
                    at += 1;
                } else if b[at] == quote {
                    at += 1;
                    if b.get(at) == Some(&quote) {
                        at += 1;
                    } else {
                        break;
                    }
                } else {
                    at += 1;
                }
            }
        } else if b[at] == b'$'
            && (at == 0 || !identifier(b[at - 1]))
            && let Some(delimiter) = dollar_delimiter(&b[at..])
        {
            at += delimiter.len();
            let tail = &b[at..];
            let end = tail
                .windows(delimiter.len())
                .position(|w| w == delimiter)
                .ok_or("42601")?;
            at += end + delimiter.len();
        } else if b[at] == b';' {
            push_statement(&mut statements, &mut statement);
            at += 1;
            continue;
        } else {
            at += 1;
        }
        statement.extend_from_slice(&b[start..at]);
    }
    push_statement(&mut statements, &mut statement);
    Ok(statements)
}

fn identifier(b: u8) -> bool {
    b.is_ascii_alphanumeric() || matches!(b, b'_' | b'$' | 128..=255)
}

fn dollar_delimiter(b: &[u8]) -> Option<&[u8]> {
    let mut at = 1;
    if b.get(at) == Some(&b'$') {
        return Some(&b[..2]);
    }
    if !b
        .get(at)
        .is_some_and(|b| b.is_ascii_alphabetic() || *b == b'_' || *b >= 128)
    {
        return None;
    }
    at += 1;
    while b.get(at).is_some_and(|b| identifier(*b) && *b != b'$') {
        at += 1;
    }
    (b.get(at) == Some(&b'$')).then(|| &b[..at + 1])
}

fn push_statement(statements: &mut Vec<String>, statement: &mut Vec<u8>) {
    let text = String::from_utf8(std::mem::take(statement)).expect("SQL remains UTF-8");
    if !text.trim().is_empty() {
        statements.push(text);
    }
}

pub(super) fn parse_settings(
    sql: &str,
    session: &Session,
    diagnostic: &mut Diagnostic,
) -> R<Option<Vec<crate::pg_settings::Setting>>> {
    if matches!(session.role, Role::Database { .. }) {
        diagnostic.settings(crate::pg_settings::parse_database(sql))
    } else {
        crate::pg_settings::parse(sql)
    }
}
pub(super) fn show_settings(
    sql: &str,
    session: &Session,
    diagnostic: &mut Diagnostic,
) -> R<Option<crate::pg_settings::Show>> {
    if matches!(session.role, Role::Database { .. }) {
        diagnostic.settings(crate::pg_settings::show_database(sql))
    } else {
        crate::pg_settings::show(sql)
    }
}
pub(super) fn settings_status(
    before: &crate::pg_settings::Settings,
    settings: &crate::pg_settings::SessionSettings,
    session: &Session,
    out: &mut Vec<u8>,
) -> R<()> {
    if matches!(session.role, Role::Database { .. }) {
        for show in crate::pg_settings::REPORTABLE {
            let (name, value) = show.value(&settings.current);
            if value != show.value(before).1 {
                let mut body = Vec::new();
                z(name, &mut body);
                z(&value, &mut body);
                frame(b'S', &body, out)?;
            }
        }
    }
    Ok(())
}
pub(super) fn warning(
    message: &str,
    settings: &crate::pg_settings::Settings,
    out: &mut Vec<u8>,
) -> R<()> {
    if settings.client_min_messages == "error" {
        return Ok(());
    }
    let mut body = vec![b'S'];
    z("WARNING", &mut body);
    body.push(b'C');
    z("25P01", &mut body);
    body.push(b'M');
    z(message, &mut body);
    body.push(0);
    frame(b'N', &body, out)
}
pub(super) fn execute_settings(
    assignments: &[crate::pg_settings::Setting],
    settings: &mut crate::pg_settings::SessionSettings,
    session: &Session,
    local_allowed: bool,
    out: &mut Vec<u8>,
) -> R<()> {
    if !session.role.is_sql() {
        return Err("42501");
    }
    if session.is_aborted() {
        return Err("25P02");
    }
    for assignment in assignments {
        let before = settings.current.clone();
        let local = matches!(
            assignment,
            crate::pg_settings::Setting::Database { local: true, .. }
        );
        if settings.apply(
            assignment,
            session.in_transaction() && (!local || local_allowed),
        ) {
            warning(
                "SET LOCAL can only be used in transaction blocks",
                &settings.current,
                out,
            )?;
        }
        settings_status(&before, settings, session, out)?;
        complete(assignment.tag(), &SqlResult::default(), out)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::split;

    #[test]
    fn boundaries_respect_sql_quotes_comments_and_empty_statements() {
        for sql in [
            "SELECT 'a;b'; SELECT 2",
            "SELECT 'a'';b'; SELECT 2",
            "SELECT 1 AS \"a;\"\"b\"; SELECT 2",
            "SELECT $$a;'b$$; SELECT 2",
            "SELECT $tag$a;$other$b$tag$; SELECT 2",
            "SELECT $\u{e9}$a;b$\u{e9}$; SELECT 2",
            r"SELECT E'a\';b'; SELECT 2",
            "SELECT 1 -- ; ignored\n; SELECT 2",
            "SELECT /* ; /* nested ; */ */ 1; SELECT 2",
            ";;SELECT 1;;;SELECT 2;; -- done",
            "SELECT foo$tag$; SELECT 2",
        ] {
            assert_eq!(split(sql).unwrap().len(), 2, "{sql}");
        }
        assert!(split(" ; ; -- x\n /* ; */").unwrap().is_empty());
        assert_eq!(split("SELECT/* x */1").unwrap(), ["SELECT 1"]);
        assert_eq!(
            split("SELECT '\u{e9};\u{1f4ab}'").unwrap(),
            ["SELECT '\u{e9};\u{1f4ab}'"]
        );
    }

    #[test]
    fn malformed_lexemes_are_rejected_before_execution() {
        for sql in [
            "SELECT 1; SELECT '",
            "SELECT \"x",
            "SELECT $$x",
            "SELECT $tag$x",
            "SELECT 1; /* unterminated",
            r"SELECT E'x\'",
        ] {
            assert_eq!(split(sql), Err("42601"), "{sql}");
        }
        assert_eq!(split(&"/*".repeat(17)), Err("54000"));
        assert_eq!(split(&" ".repeat(65537)), Err("54000"));
    }
}
