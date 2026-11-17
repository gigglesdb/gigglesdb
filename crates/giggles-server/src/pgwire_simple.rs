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
        // GD-0061 R3-206: one statement per Query, admitted before it runs.
        if session.task().is_some() {
            return self.task_simple(sql, session, out);
        }
        let mut implicit = false;
        if !matches!(session.role, Role::Database { .. }) {
            return self.simple_statement(sql, session, out, received, &mut implicit);
        }
        let statements = split(sql)?;
        if statements.is_empty() {
            return frame(b'I', &[], out);
        }
        if statements.len() == 1 {
            let completed = out.len();
            let result = self.simple_statement(&statements[0], session, out, received, &mut implicit);
            if let Err(code) = result {
                if implicit {
                    self.simple_failure(code, true, completed, session, out);
                    return Ok(());
                }
                return Err(code);
            }
            if implicit && self.copy.is_none()
                && let Err(code) = execute_control("COMMIT", session, &mut implicit, &mut self.settings, out)
            {
                self.simple_failure(code, true, completed, session, out);
            }
            return Ok(());
        }
        let output = self.output.take();
        let result = self.simple_batch(statements, session, out, received);
        self.output = output;
        result
    }

    fn simple_batch(
        &mut self,
        statements: Vec<String>,
        session: &mut Session,
        out: &mut Vec<u8>,
        received: std::time::Instant,
    ) -> R<()> {
        let mut implicit = false;
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

    fn copy_stdout(
        &mut self,
        options: &crate::auth::statements::CopyOutput,
        session: &mut Session,
        out: &mut Vec<u8>,
    ) -> R<()> {
        let prepared = core(session.prepare(&options.select))?;
        let description = core(session.describe(&prepared))?;
        self.settings.queried = session.in_transaction();
        // PostgreSQL streams COPY TO STDOUT: a spilled v2 result is read from its
        // temp file in chunks and flushed as it goes, never materialised.
        let (result, spilled) = giggles_core::relational::spill::streaming(
            || session.execute_prepared(&prepared, &[]));
        let result = core(result)?;
        let rows: Box<dyn Iterator<Item = Result<giggles_core::relational::SqlRow, giggles_core::CoreError>>> =
            match spilled {
                Some(spilled) => Box::new(spilled),
                None => Box::new(result.rows.into_iter().map(Ok)),
            };
        let mut count = 0usize;
        let mut header = vec![u8::from(options.format == 2)];
        header.extend(
            u16::try_from(description.columns.len())
                .map_err(|_| "54000")?
                .to_be_bytes(),
        );
        for _ in &description.columns {
            header.extend(i16::from(options.format == 2).to_be_bytes());
        }
        frame(b'H', &header, out)?;
        if options.format == 2 {
            frame(b'd', b"PGCOPY\n\xff\r\n\0\0\0\0\0\0\0\0\0", out)?;
        }
        if options.header {
            let mut data = Vec::new();
            for (i, column) in description.columns.iter().enumerate() {
                if i > 0 {
                    data.push(options.delimiter);
                }
                copy_csv_field(column.name.as_bytes(), options, &mut data);
            }
            data.push(b'\n');
            frame(b'd', &data, out)?;
        }
        for row in rows {
            let row = core(row)?;
            count += 1;
            if count.is_multiple_of(CURSOR_ROWS) {
                flush_output(&self.output, out)?;
            }
            let mut data = Vec::new();
            if options.format == 2 {
                data.extend((description.columns.len() as i16).to_be_bytes());
            }
            for (i, (value, column)) in row.values.iter().zip(&description.columns).enumerate() {
                if i > 0 && options.format != 2 {
                    data.push(options.delimiter);
                }
                match encode(
                    value,
                    &column.data_type,
                    u16::from(options.format == 2),
                    &self.settings.current,
                    session.zone().unwrap_or(SessionZone::Fixed(0)),
                )? {
                    None if options.format == 2 => data.extend((-1i32).to_be_bytes()),
                    Some(value) if options.format == 2 => {
                        data.extend(
                            i32::try_from(value.len())
                                .map_err(|_| "54000")?
                                .to_be_bytes(),
                        );
                        data.extend(value);
                    }
                    None => data.extend(&options.null),
                    Some(value) if options.format == 1 => {
                        copy_csv_field(&value, options, &mut data)
                    }
                    Some(value) => {
                        let escaped = value
                            .iter()
                            .filter(|b| matches!(b, b'\\' | b'\t' | b'\n' | b'\r' | 8 | 12 | 11))
                            .count();
                        let size = data
                            .len()
                            .checked_add(value.len())
                            .and_then(|n| n.checked_add(escaped))
                            .and_then(|n| n.checked_add(1))
                            .ok_or("54000")?;
                        if size > MAX_RESPONSE.saturating_sub(out.len()).saturating_sub(32) {
                            return Err("54000");
                        }
                        data.try_reserve_exact(size - data.len())
                            .map_err(|_| "54000")?;
                        for b in value {
                            match b {
                                b'\\' => data.extend(b"\\\\"),
                                b'\t' => data.extend(b"\\t"),
                                b'\n' => data.extend(b"\\n"),
                                b'\r' => data.extend(b"\\r"),
                                8 => data.extend(b"\\b"),
                                12 => data.extend(b"\\f"),
                                11 => data.extend(b"\\v"),
                                b if b == options.delimiter => {
                                    data.push(b'\\');
                                    data.push(b);
                                }
                                _ => data.push(b),
                            }
                        }
                    }
                }
            }
            if options.format != 2 {
                data.push(b'\n');
            }
            frame(b'd', &data, out)?;
        }
        if options.format == 2 {
            frame(b'd', &(-1i16).to_be_bytes(), out)?;
        }
        frame(b'c', &[], out)?;
        complete(
            &format!("COPY {count}"),
            &SqlResult::default(),
            out,
        )
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
        session.set_search_path(&self.settings.current.search_path);
        session.set_extra_float_digits(self.settings.current.extra_float_digits);
        super::statement_deadline(&self.settings.current);
        let result = self.simple_statement_inner(sql, session, out, received, implicit);
        if !session.in_transaction() && !*implicit { session.service.advisory.release(session.backend_pid, true); }
        session.set_time_zone(&self.settings.current.timezone);
        session.set_search_path(&self.settings.current.search_path);
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
        if session.task().is_some() {
            return self.task_statement(sql, session, out);
        }
        if let Some(command) = cursor::parse(sql)? {
            self.cursors.execute(
                &command,
                session,
                &[],
                &[],
                &mut self.settings,
                true,
                &self.output,
                out,
            )?;
            return Ok(());
        }
        if let Some(tables) = crate::auth::statements::access_share_tables(sql)? {
            if !session.in_transaction() || *implicit {
                return Err("25P01");
            }
            validate_lock(session, &tables.tables, tables.mode, tables.nowait)?;
            return complete("LOCK TABLE", &SqlResult::default(), out);
        }
        if let Some(select) = crate::auth::statements::copy_stdout_select(sql)? {
            return self.copy_stdout(&select, session, out);
        }
        if is_copy_sql(sql) {
            let (copy, prepared) = core(session.prepare_copy(sql))?;
            read_only_check("INSERT", &self.settings, session, &mut self.diagnostic)?;
            if self.settings.replication_role_replica() {
                begin_implicit(session, implicit, &mut self.settings)?;
            }
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
                type_oids: vec![],
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
                &self.settings.current,
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
                &self.settings.current,
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
                &mut self.diagnostic,
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
        } else if let Some(query) = explain::prepare(sql, session)? {
            self.settings.queried = session.in_transaction();
            let result = explain::execute(&query, session)?;
            let description = explain::description();
            row_description(&description, &[], out)?;
            rows(&self.settings.current, session.zone().unwrap_or(SessionZone::Fixed(0)),
                &result, &description, &[], 0, result.rows.len(), out)?;
            complete("EXPLAIN", &result, out)?;
        } else if let Some(plan) = catalog_plan(sql, &mut self.diagnostic, session)? {
            self.diagnostic.phase = "catalog execution";
            self.settings.queried = session.in_transaction();
            let (epoch, tables, indexes, typmods, types) =
                core(session.catalog_snapshot_with_typmods())?;
            let result = catalog_wire::execute(
                &plan,
                &catalog_wire::with_application_rows(
                    session,
                    &plan,
                    catalog_wire::with_search_path(
                        catalog_wire::snapshot(session, epoch, tables, indexes, typmods, types)?,
                        &self.settings.current.search_path,
                    ),
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
                read_only_check(p.write_tables().first().map_or(p.command(), |(_, command)| *command), &self.settings, session, &mut self.diagnostic)?;
                self.settings.queried = session.in_transaction();
                if p.command() == "SELECT" && p.write_tables().is_empty() {
                    let mut cursor = self.diagnostic.core(session.open_cursor(&p, &[]))?;
                    row_description(&d, &[], out)?;
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
                        rows(
                            &self.settings.current,
                            session.zone().unwrap_or(SessionZone::Fixed(0)),
                            &result,
                            &d,
                            &[],
                            0,
                            n,
                            out,
                        )?;
                        count += n;
                        flush_output(&self.output, out)?;
                    }
                    catalog_complete(count, out)?;
                    return Ok(());
                }
                // Core's autocommit transaction starts inside execution, too late
                // to set its transaction-local replica flag. Pin it here instead.
                if self.settings.replication_role_replica() && !matches!(p.command(), "BEGIN" | "COMMIT" | "ROLLBACK") {
                    begin_implicit(session, implicit, &mut self.settings)?;
                }
                self.diagnostic.core(session.sync_replication_role(&self.settings))?;
                let (result, spilled) = giggles_core::relational::spill::streaming(
                    || session.execute_prepared(&p, &[]));
                let result = self.diagnostic.core(result)?;
                self.settings
                    .transaction(was_transaction, session.in_transaction(), p.command());
                if let Some(mut spilled) = spilled {
                    // v2 RETURNING past the in-memory threshold streams from its spill file.
                    row_description(&d, &[], out)?;
                    while !spilled.is_empty() {
                        let chunk = spilled.by_ref().take(CURSOR_ROWS).collect::<Result<Vec<_>, _>>();
                        let chunk = SqlResult { rows: self.diagnostic.core(chunk)?, ..SqlResult::default() };
                        rows(&self.settings.current, session.zone().unwrap_or(SessionZone::Fixed(0)),
                            &chunk, &d, &[], 0, chunk.rows.len(), out)?;
                        flush_output(&self.output, out)?;
                    }
                } else if !d.columns.is_empty() {
                    row_description(&d, &[], out)?;
                    rows(
                        &self.settings.current,
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
    if sql.len() > giggles_core::relational::MAX_STATEMENT_SQL {
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
        if let Ok(tokens) = crate::pg_settings::database::lex(sql) {
            use crate::pg_settings::database::{Parser, Token};
            let mut p = Parser { tokens, at: 0 };
            let reset = p.kw("RESET");
            if reset || p.kw("SET") {
                if !reset && !p.kw("SESSION") { p.kw("LOCAL"); }
                if matches!(p.tokens.get(p.at), Some(Token::Word(name) | Token::Ident(name)) if name.eq_ignore_ascii_case("session_replication_role")) {
                    p.at += 1;
                    diagnostic.replication_role_authorize(session)?;
                    if !reset {
                        if p.tokens.get(p.at) == Some(&Token::Equals) { p.at += 1; }
                        else { p.need("TO")?; }
                        if !p.kw("DEFAULT") && p.tokens.len() == p.at + 1
                            && let Some(Token::Word(value) | Token::Ident(value) | Token::Text(value)) = p.tokens.get(p.at)
                        {
                            diagnostic.replication_role_value(value)?;
                        }
                    }
                }
            }
        }
        let result = diagnostic.settings(crate::pg_settings::parse_database(sql));
        if matches!(result, Err("22023"))
            && let Ok(tokens) = crate::pg_settings::database::lex(sql) {
                use crate::pg_settings::database::Token;
                let mut p = crate::pg_settings::database::Parser { tokens, at: 0 };
                p.kw("SET");
                if !p.kw("SESSION") { p.kw("LOCAL"); }
                let zone = p.kw("TIMEZONE") || (p.kw("TIME") && p.kw("ZONE"));
                if zone {
                    diagnostic.wire_message = Some("invalid value for parameter \"TimeZone\"".into());
                    if p.tokens.iter().any(|t| matches!(t, Token::Word(w) if w.eq_ignore_ascii_case("INTERVAL"))) {
                        diagnostic.wire_detail = Some("Cannot specify days in time zone interval");
                    }
                }
            }
        result
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
    diagnostic: &mut Diagnostic,
    out: &mut Vec<u8>,
) -> R<()> {
    if !session.role.is_sql() {
        return Err("42501");
    }
    if session.is_aborted() {
        return Err("25P02");
    }
    for assignment in assignments {
        if matches!(assignment, crate::pg_settings::Setting::Database { name: crate::pg_settings::Show::SessionReplicationRole, .. }) {
            diagnostic.replication_role_authorize(session)?;
        }
    }
    for assignment in assignments {
        let before = settings.current.clone();
        // RESET ALL skips superuser-only parameters for unprivileged sessions.
        let preserve_replication_role = matches!(assignment, crate::pg_settings::Setting::ResetAll)
            && !replication_role_allowed(session)?;
        let local = matches!(
            assignment,
            crate::pg_settings::Setting::Database { local: true, .. }
        );
        let warn = if preserve_replication_role {
            settings.reset_all(false, session.in_transaction());
            false
        } else {
            settings.apply(assignment, session.in_transaction() && (!local || local_allowed))
        };
        if warn {
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

fn copy_csv_field(value: &[u8], options: &crate::auth::statements::CopyOutput, out: &mut Vec<u8>) {
    let quoted = value == options.null
        || value == b"\\."
        || value
            .iter()
            .any(|b| *b == options.delimiter || *b == options.quote || matches!(b, b'\n' | b'\r'));
    if quoted {
        out.push(options.quote);
    }
    for b in value {
        if quoted && *b == options.quote {
            out.push(*b);
        }
        out.push(*b);
    }
    if quoted {
        out.push(options.quote);
    }
}

// Validate every name and privilege before acquiring any explicit locks.
pub(super) fn validate_lock(
    session: &mut Session,
    tables: &[String],
    mode: giggles_core::SqlTableLockMode,
    nowait: bool,
) -> R<()> {
    let strong = !matches!(
        mode,
        giggles_core::SqlTableLockMode::AccessShare | giggles_core::SqlTableLockMode::RowShare
    );
    use giggles_core::{Error, roles::TablePrivilege};
    core(session.validate_access_share(tables))?;
    if !strong {
        return core(session.sql.lock_tables(tables, mode, nowait));
    }
    core(session.authority_scope(|session| {
        for table in tables {
            let prepared = session
                .sql
                .prepare(&format!("SELECT * FROM {table} LIMIT 0"))?;
            let table = prepared.table().ok_or(CoreError::Sql(
                giggles_core::relational::SqlError::UnknownTable,
            ))?;
            let allowed = match &session.role {
                Role::Database { name } => {
                    let roles = session.service.admin.roles();
                    let role = roles
                        .get(name)
                        .map_err(|_| CoreError::Policy(Error::Authentication))?
                        .ok_or(CoreError::Policy(Error::Authentication))?;
                    let authority = session.sql.table_authority_name(table)?;
                    let owner = roles
                        .table_owner(&authority)
                        .map_err(|_| CoreError::Policy(Error::Scope))?;
                    let privileges = roles
                        .table_privileges(&authority, name)
                        .map_err(|_| CoreError::Policy(Error::Scope))?;
                    role.superuser
                        || owner.as_deref() == Some(name)
                        || if strong {
                            privileges.contains(&TablePrivilege::Update)
                                || privileges.contains(&TablePrivilege::Delete)
                        } else {
                            privileges.contains(&TablePrivilege::Select)
                        }
                }
                Role::Application { tables, .. } => tables.contains(table),
                Role::Task { .. } => false,
            };
            if !allowed {
                return Err(CoreError::Policy(Error::Scope));
            }
            session.sql.describe(&prepared)?;
        }
        Ok(())
    }))?;
    core(session.sql.lock_tables(tables, mode, nowait))
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
        assert_eq!(split(&" ".repeat(giggles_core::relational::MAX_STATEMENT_SQL + 1)), Err("54000"));
    }
}
