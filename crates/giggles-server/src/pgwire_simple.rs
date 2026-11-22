//! Simple Query dispatch. Agent requests retain the original whole-query path.
use super::*;
use giggles_core::relational::spill::SpilledRows;

struct SimpleRows<'a> {
    settings: &'a crate::pg_settings::Settings,
    zone: SessionZone,
    description: &'a Description,
}

impl SimpleRows<'_> {
    fn encode(&self, result: &Released<impl Borrow<SqlResult>>, out: &mut Vec<u8>) -> R<()> {
        data_row_frames(
            self.settings,
            self.zone,
            result,
            self.description,
            &[],
            0..result.get().borrow().rows.len(),
            out,
        )
    }

    fn spilled(
        &self,
        spilled: &mut SpilledRows,
        out: &mut Vec<u8>,
        mut finish_chunk: impl FnMut(&mut Vec<u8>) -> Result<(), CoreError>,
    ) -> Result<(), CoreError> {
        row_description(self.description, &[], out).map_err(CoreError::SqlReplyEncoding)?;
        while !spilled.is_empty() {
            let rows = spilled.by_ref().take(CURSOR_ROWS).collect::<Result<Vec<_>, _>>()?;
            // Only plain sessions hand the caller a spill without a release tag.
            let chunk = Released::synthetic(SqlResult { rows, ..SqlResult::default() });
            self.encode(&chunk, out).map_err(CoreError::SqlReplyEncoding)?;
            finish_chunk(out)?;
        }
        Ok(())
    }
}

struct CopyRows<'a> {
    options: &'a crate::auth::statements::CopyOutput,
    description: &'a Description,
    settings: &'a crate::pg_settings::Settings,
    zone: SessionZone,
}

impl CopyRows<'_> {
    fn encode(
        &self,
        rows: impl Iterator<Item = Result<impl Borrow<giggles_core::relational::SqlRow>, CoreError>>,
        sink: &mut Option<copy_external::Sink>,
        diagnostic: &mut Diagnostic,
        out: &mut Vec<u8>,
        mut flush: impl FnMut(&mut Vec<u8>) -> R<()>,
    ) -> R<()> {
        let Self {
            options,
            description,
            settings,
            zone,
        } = *self;
        let converter = if options.format == 2 {
            None
        } else {
            options.encoding.and_then(|e| e.converter())
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
        if sink.is_none() { frame(b'H', &header, out)?; }
        if options.format == 2 {
            copy_output_data(sink, diagnostic, b"PGCOPY\n\xff\r\n\0\0\0\0\0\0\0\0\0", out)?;
        }
        if options.header {
            let mut data = Vec::new();
            for (i, column) in description.columns.iter().enumerate() {
                if i > 0 {
                    data.push(options.delimiter);
                }
                if options.format == 1 {
                    copy_csv_field(column.name.as_bytes(), options, false, &mut data);
                } else {
                    for b in column.name.as_bytes() {
                        match *b { b'\\' => data.extend(b"\\\\"), b'\t' => data.extend(b"\\t"), b'\n' => data.extend(b"\\n"), b'\r' => data.extend(b"\\r"), b if b == options.delimiter => { data.push(b'\\'); data.push(b); }, b => data.push(b) }
                    }
                }
            }
            data.push(b'\n');
            if let Some(encoding) = converter {
                data = encoding.encode(&data).map_err(|message| {
                    diagnostic.wire_message = Some(message);
                    "22P05"
                })?;
            }
            copy_output_data(sink, diagnostic, &data, out)?;
        }
        for row in rows {
            let row = core(row)?;
            count += 1;
            if sink.is_none() && count.is_multiple_of(CURSOR_ROWS) {
                flush(out)?;
            }
            let mut data = Vec::new();
            if options.format == 2 {
                data.extend((description.columns.len() as i16).to_be_bytes());
            }
            for (i, (value, column)) in row
                .borrow()
                .values
                .iter()
                .zip(&description.columns)
                .enumerate()
            {
                if i > 0 && options.format != 2 {
                    data.push(options.delimiter);
                }
                match encode(
                    value,
                    &column.data_type,
                    u16::from(options.format == 2),
                    settings,
                    zone,
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
                        copy_csv_field(&value, options, options.force_all || options.force_columns.contains(&column.name), &mut data)
                    }
                    Some(value) => {
                        let escaped = value
                            .iter()
                            .filter(|b| **b == options.delimiter || matches!(b, b'\\' | b'\t' | b'\n' | b'\r' | 8 | 12 | 11))
                            .count();
                        let size = data
                            .len()
                            .checked_add(value.len())
                            .and_then(|n| n.checked_add(escaped))
                            .and_then(|n| n.checked_add(1))
                            .ok_or("54000")?;
                        if size > MAX_RESPONSE.saturating_sub(out.len()).saturating_sub(5) {
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
            if let Some(encoding) = converter {
                data = encoding.encode(&data).map_err(|message| {
                    diagnostic.wire_message = Some(message);
                    "22P05"
                })?;
            }
            copy_output_data(sink, diagnostic, &data, out)?;
        }
        if options.format == 2 {
            copy_output_data(sink, diagnostic, &(-1i16).to_be_bytes(), out)?;
        }
        if let Some(sink) = sink.take() {
            sink.finish(diagnostic)?;
        } else {
            frame(b'c', &[], out)?;
        }
        complete(&format!("COPY {count}"), &empty(), out)
    }
}

fn copy_output_data(
    sink: &mut Option<copy_external::Sink>,
    diagnostic: &mut Diagnostic,
    data: &[u8],
    out: &mut Vec<u8>,
) -> R<()> {
    if let Some(sink) = sink {
        sink.data(data, diagnostic)
    } else {
        frame(b'd', data, out)
    }
}

impl State {
    pub(super) fn copy_output_options(
        &mut self,
        sql: &str,
    ) -> R<Option<crate::auth::statements::CopyOutput>> {
        let result = crate::auth::statements::copy_stdout_select(sql);
        if result.is_err() {
            self.diagnostic.wire_message = giggles_core::relational::take_sql_error_message();
        }
        result
    }

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
            let was_in_transaction = session.in_transaction();
            let completed = out.len();
            let result =
                self.simple_statement(&statements[0], session, out, received, &mut implicit);
            if let Err(code) = result {
                if implicit {
                    self.simple_failure(code, true, completed, session, out);
                    return Ok(());
                }
                return Err(code);
            }
            if implicit && self.copy.is_none() {
                let _durability = if was_in_transaction {
                    None
                } else {
                    giggles_core::Cancellation::current()
                        .and_then(|context| self.durability_registration(&context, session))
                };
                self.finish_simple_batch(session, out, completed);
            }
            return Ok(());
        }
        let output = self.output.take();
        let result = self.simple_batch(statements, session, out, received, &output);
        self.output = output;
        result
    }

    fn simple_batch(
        &mut self,
        statements: Vec<String>,
        session: &mut Session,
        out: &mut Vec<u8>,
        received: std::time::Instant,
        output: &Output,
    ) -> R<()> {
        // Role/ACL and DROP TABLE operations publish outside the SQL snapshot.
        // Like CREATE DATABASE in PostgreSQL, these must be the sole statement
        // in a Query message, even if the message contains explicit COMMITs.
        // GD-0061 R3-707: so are the approver forms, which write approval
        // records outside the SQL snapshot, and (R3-110) the task statements.
        if statements.iter().any(|sql| {
            crate::auth::statements::parse(sql).is_some()
                || crate::task_ddl::parse(sql).is_some()
                || matches!(crate::agent::preview::approver_statement(sql), Ok(Some(_)))
        }) {
            return Err("25001");
        }
        self.continue_simple_batch(
            statements.into_iter(),
            session,
            out,
            received,
            output,
            false,
        )
    }

    pub(super) fn continue_simple_batch(
        &mut self,
        mut statements: impl Iterator<Item = String>,
        session: &mut Session,
        out: &mut Vec<u8>,
        received: std::time::Instant,
        output: &Output,
        mut implicit: bool,
    ) -> R<()> {
        use crate::pg_transaction::Command;

        let mut published_commit = false;
        while let Some(sql) = statements.next() {
            // A publishing COMMIT ends the previous checked reply budget.
            // Send its reply before framing the next statement on this socket.
            if published_commit || out.len() > MAX_RESPONSE {
                flush_output(output, out).map_err(|code| self.diagnostic.reply_error(code))?;
            }
            let completed = out.len();
            let result = (|| {
                let command = parse_control(&sql, session, &mut self.diagnostic)?;
                published_commit = matches!(command, Some(Command::Commit))
                    && session.in_transaction()
                    && !session.is_aborted();
                if command.is_none_or(|command| matches!(command, Command::Set(_) | Command::Session(_)))
                {
                    begin_implicit(session, &mut implicit, &mut self.settings)?;
                }
                self.simple_statement(&sql, session, out, received, &mut implicit)
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
            self.finish_simple_batch(session, out, completed);
        }
        Ok(())
    }

    pub(super) fn finish_simple_batch(
        &mut self,
        session: &mut Session,
        out: &mut Vec<u8>,
        completed: usize,
    ) {
        // All rows are already checked, including when CopyDone resumes the
        // batch. execute_implicit_commit appends its control suffix on success.
        let mut implicit = true;
        match execute_implicit_commit(
            session,
            &mut implicit,
            &mut self.settings,
            &mut self.diagnostic,
            out,
        ) {
            Ok(actual) => self.diagnostic.committed |= actual == "COMMIT",
            Err(code) => self.simple_failure(code, true, completed, session, out),
        }
    }

    pub(super) fn copy_stdout(
        &mut self,
        options: &crate::auth::statements::CopyOutput,
        session: &mut Session,
        out: &mut Vec<u8>,
        implicit: &mut bool,
    ) -> R<()> {
        self.retain_durability_permits = true;
        self.copy_destination_privilege(session, &options.destination)?;
        let prepared = self.prepare_copy_output(options, session)?;
        let description = core(session.describe(&prepared))?;
        let writes = prepared.write_tables();
        let writing = !writes.is_empty();
        if writing {
            let command = prepared
                .row_lock_command()
                .filter(|_| !prepared.row_lock_tables().is_empty())
                .unwrap_or_else(|| {
                    writes
                        .first()
                        .map_or(prepared.command(), |(_, command)| *command)
                });
            read_only_check(command, &self.settings, session, &mut self.diagnostic)?;
        }
        for name in &options.force_columns {
            if !description.columns.iter().any(|c| c.name == *name) {
                self.diagnostic.wire_message = Some(format!(
                    "column \"{name}\" of relation \"{}\" does not exist",
                    options.table_name.as_deref().unwrap_or("*SELECT*")
                ));
                return Err("42703");
            }
        }
        if options.format != 2
            && let Some(message) = options.encoding.and_then(|e| e.missing_conversion(false))
        {
            self.diagnostic.wire_message = Some(message);
            return Err("42883");
        }
        let mut sink =
            copy_external::Sink::open(&options.destination, session, &mut self.diagnostic)?;
        if writing {
            // An external destination must finish writing before this block commits.
            if sink.is_some() || self.settings.replication_role_replica() {
                begin_implicit(session, implicit, &mut self.settings)?;
            }
            self.diagnostic
                .core(session.sync_replication_role(&self.settings))?;
        }
        self.settings.queried = session.in_transaction();
        let encoder = CopyRows {
            options,
            description: &description,
            settings: &self.settings.current,
            zone: session.zone().unwrap_or(SessionZone::Fixed(0)),
        };
        let can_flush = self.output.is_some();
        let completed = out.len();
        let mut checked = false;
        let mut checked_spill = false;
        let (result, spilled) = giggles_core::relational::spill::streaming(|| {
            if sink.is_some() || session.in_transaction() || !writing {
                return session.execute_prepared_unreleased(&prepared, &[]);
            }
            let mut check = |result: Released<&SqlResult>, spill: Option<&mut SpilledRows>| {
                checked = true;
                let diagnostic = &mut self.diagnostic;
                if let Some(spill) = spill {
                    checked_spill = true;
                    let mut scratch = out.clone();
                    encoder
                        .encode(spill, &mut None, diagnostic, &mut scratch, |buf| {
                            if can_flush {
                                buf.clear();
                            }
                            core(giggles_core::cancellation::check())
                        })
                        .map_err(CoreError::SqlReplyEncoding)
                } else {
                    // The COPY reply stays private until publication succeeds.
                    encoder
                        .encode(
                            result.get().rows.iter().map(Ok),
                            &mut None,
                            diagnostic,
                            out,
                            |_| Ok(()),
                        )
                        .map_err(CoreError::SqlReplyEncoding)
                }
            };
            session.execute_prepared_checked_unreleased(&prepared, &mut check)
        });
        if checked && result.is_err() {
            out.truncate(completed);
        }
        let result = self.diagnostic.core(result)?;
        self.diagnostic.committed |= checked;
        // GD-0061 R3-311: release the whole page before emitting the COPY header.
        let result = core(session.release_page(result))
            .map_err(|code| self.diagnostic.reply_error(code))?;
        debug_assert!(
            !checked_spill || spilled.is_some(),
            "checked COPY spill disappeared"
        );
        if checked_spill && spilled.is_none() {
            return Err("08007");
        }
        let mut flush = |out: &mut Vec<u8>| flush_output(&self.output, out);
        if let Some(spilled) = spilled {
            let after_commit = spilled.after_commit();
            let sent = encoder.encode(spilled, &mut sink, &mut self.diagnostic, out, &mut flush);
            sent.map_err(|code| if after_commit { "08007" } else { code })?;
        } else if !checked {
            encoder.encode(
                result.get().rows.iter().map(Ok),
                &mut sink,
                &mut self.diagnostic,
                out,
                &mut flush,
            )?;
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
        let before = self
            .diagnostic
            .pre_rollback_settings
            .take()
            .map_or_else(|| self.settings.current.clone(), |before| *before);
        self.diagnostic.authorize_duplicate_key(code, session);
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
        let _ = settings_status_with_limit(
            &before, &self.settings, session, out, ReplyLimit::Control,
        );
        let mut error = Vec::new();
        let message = self.diagnostic.message.unwrap_or(match code {
            "0A000" => "unsupported SQL feature or parameter type in this server profile",
            "42601" => "statement syntax is not supported by this server SQL profile",
            _ => "request rejected; consult documented code",
        });
        self.diagnostic.unknown_outcome = code == "08007";
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
        if let Some(admission) = self.execution_admission.as_mut()
            && let Err(refusal) = admission.statement(sql, session)
        {
            session.abort();
            self.diagnostic.wire_message = Some(refusal.message());
            return Err(refusal.sqlstate());
        }
        // A multi-statement Query needs settings changes before its next SQL
        // statement, rather than only at the enclosing protocol-frame boundary.
        session.set_time_zone(&self.settings.current.timezone);
        session.sync_role_settings(&mut self.settings.current)?;
        session.set_search_path(&self.settings.current.search_path);
        session.set_text_settings(&self.settings.current);
        session.set_max_value_bytes(self.settings.current.max_value_bytes);
        session.set_max_statement_tokens(self.settings.current.max_statement_tokens);
        super::statement_deadline(&self.settings.current);
        let result = self.simple_statement_inner(sql, session, out, received, implicit);
        if !self.copy_receiving() && let Some(admission) = self.execution_admission.as_mut() {
            admission.permits().role_permit = None;
        }
        if !session.in_transaction() && !*implicit {
            if self.copy.is_none() {
                self.retain_durability_permits = false;
            }
            session.service.advisory.release(session.backend_pid, true);
        }
        session.set_time_zone(&self.settings.current.timezone);
        if !self.durability_permits_released()
            && let Err(code) = session.sync_role_settings(&mut self.settings.current)
            && result.is_ok()
        {
            return Err(self.diagnostic.reply_error(code));
        }
        session.set_search_path(&self.settings.current.search_path);
        session.set_text_settings(&self.settings.current);
        session.set_max_value_bytes(self.settings.current.max_value_bytes);
        session.set_max_statement_tokens(self.settings.current.max_statement_tokens);
        if result.is_ok() {
            role_notices(
                session,
                &self.settings.current,
                out,
                ReplyLimit::after_commit(self.diagnostic.committed),
            )?;
        }
        result
    }

    fn simple_statement_inner(
        &mut self,
        sql: &str,
        session: &mut Session,
        out: &mut Vec<u8>,
        _received: std::time::Instant,
        implicit: &mut bool,
    ) -> R<()> {
        if session.task().is_some() {
            return self.task_statement(sql, session, out);
        }
        // GD-0061 R3-707: an approver form, before any other grammar reads it.
        if let Some(change) = crate::agent::preview::approver_statement(sql)? {
            crate::agent::preview::approver_guard(session)?;
            self.diagnostic.phase = "approval execution";
            // The approval summary the server builds: never core rows.
            let result = Released::synthetic(
                self.diagnostic
                    .core(crate::agent::preview::execute_approver(session, &change))?,
            );
            let description = crate::agent::preview::approver_description(&change);
            if !description.columns.is_empty() {
                row_description(&description, &[], out)?;
            }
            rows(
                &self.settings.current,
                session.zone().unwrap_or(SessionZone::Fixed(0)),
                &result,
                &description,
                &[],
                0,
                result.get().rows.len(),
                out,
            )?;
            return complete(crate::agent::preview::approver_command(&change), &result, out);
        }
        // GD-0061 R3-110: a task statement, before the role parser, which
        // claims every GRANT and REVOKE head.
        if let Some(stmt) = crate::task_ddl::parse(sql) {
            if let giggles_core::task::statements::TaskStatement::Error(code) = stmt {
                return Err(code);
            }
            if crate::task_ddl::writes(&stmt) {
                read_only_check(
                    crate::task_ddl::command(&stmt),
                    &self.settings,
                    session,
                    &mut self.diagnostic,
                )?;
            }
            self.diagnostic.phase = "task statement execution";
            let outcome = session
                .execute_task_ddl(&stmt)
                .map_err(|error| self.diagnostic.task_ddl(error))?;
            return match outcome {
                giggles_core::task::statements::TaskOutcome::Tag(tag) => {
                    self.admin_completed = true;
                    complete(tag, &empty(), out)
                }
                giggles_core::task::statements::TaskOutcome::Rows(description, result) => {
                    // The SHOW result the server builds from catalog and
                    // Kernel records: never core rows.
                    let result = Released::synthetic(result);
                    row_description(&description, &[], out)?;
                    rows(
                        &self.settings.current,
                        session.zone().unwrap_or(SessionZone::Fixed(0)),
                        &result,
                        &description,
                        &[],
                        0,
                        result.get().rows.len(),
                        out,
                    )?;
                    complete(crate::task_ddl::command(&stmt), &result, out)
                }
            };
        }
        if let Some(command) = cursor::parse(sql)? {
            self.cursors.execute(
                &command,
                session,
                &[],
                &[],
                &mut self.settings,
                &mut self.diagnostic,
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
            return complete("LOCK TABLE", &empty(), out);
        }
        if let Some(select) = self.copy_output_options(sql)? {
            return self.copy_stdout(&select, session, out, implicit);
        }
        if is_copy_sql(sql) {
            let (copy, prepared) = self.prepare_copy_checked(sql, session)?;
            read_only_check("INSERT", &self.settings, session, &mut self.diagnostic)?;
            // Keep standalone V2 database COPY input private until CopyDone
            // can publish through the ordinary simple batch commit path.
            if self.settings.replication_role_replica()
                || (matches!(session.role, Role::Database { .. })
                    && session.sql.has_native_transaction_blocks()
                    && matches!(copy.source, giggles_core::relational::CopySource::Stdin))
            {
                begin_implicit(session, implicit, &mut self.settings)?;
            }
            self.settings.queried = session.in_transaction();
            self.start_copy(copy, Box::new(prepared), Some(*implicit), session, out)?;
            if let Some(copy) = self.copy.as_mut() {
                copy.simple_implicit = *implicit;
            }
            return Ok(());
        }
        core(session.authorize_sequence_sql(sql))?;
        #[cfg(feature = "legacy-task")]
        if let Some(json) = sql.strip_prefix("GIGGLES ") {
            if !matches!(session.role, Role::Task { .. }) {
                return Err("42501");
            }
            let req: Request = serde_json::from_str(json).map_err(|_| "08P01")?;
            if matches!(req, Request::Sql { .. } | Request::Ping) {
                return Err("42501");
            }
            let response = session.handle_received(req, _received);
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
            // The decision the server built: never core rows.
            let result = Released::synthetic(SqlResult {
                columns: vec!["decision".into()],
                rows: vec![giggles_core::relational::SqlRow {
                    values: vec![Value::Text(value)],
                    version: 0,
                }],
                affected: 0,
            });
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
            return Ok(());
        }
        if let Some(stmt) = crate::auth::statements::parse(sql) {
            read_only_check(
                admin_command(&stmt),
                &self.settings,
                session,
                &mut self.diagnostic,
            )?;
            let tag = session.execute_admin(&stmt).map_err(|code| self.diagnostic.role_admin_error(session, &stmt, code))?;
            role_notices(session, &self.settings.current, out, ReplyLimit::Checked)?;
            self.admin_completed = true;
            complete(tag, &empty(), out)?;
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
        } else if let Some(show) = show_settings(sql, session, &mut self.diagnostic)? {
            let (description, result) = inspect(show, &self.settings.current, session)?;
            let result = &result;
            row_description(&description, &[], out)?;
            rows(
                &self.settings.current,
                session.zone().unwrap_or(SessionZone::Fixed(0)),
                result,
                &description,
                &[],
                0,
                1,
                out,
            )?;
            complete("SHOW", result, out)?;
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
            // Plan text the server builds (its core reads refuse a task session).
            let result = Released::synthetic(explain::execute(&query, session)?);
            let description = explain::description(&query);
            row_description(&description, &[], out)?;
            rows(&self.settings.current, session.zone().unwrap_or(SessionZone::Fixed(0)),
                &result, &description, &[], 0, result.get().rows.len(), out)?;
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
            let total = result.rows.len();
            // Built from the catalog snapshot, whose application rows were
            // released as they were read (`with_application_rows`, R3-312).
            catalog_wire::rows(
                &self.settings.current,
                &Released::synthetic(result),
                &[],
                0,
                total,
                out,
            )?;
            catalog_complete(total, out)?;
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
                // GD-0061 R3-311: ROLLBACK returns no rows; its completion is
                // built by the server and the core result is discarded.
                drop(core(session.execute_sql_unreleased(sql, &[]))?);
                self.settings
                    .transaction(was_transaction, session.in_transaction(), "ROLLBACK");
                complete("ROLLBACK", &empty(), out)?;
            } else {
                let p = self.diagnostic.core(session.prepare(sql))?;
                let d = self.diagnostic.core(session.describe(&p))?;
                let writes = p.write_tables();
                let command = p
                    .row_lock_command()
                    .filter(|_| !p.row_lock_tables().is_empty())
                    .unwrap_or_else(|| writes.first().map_or(p.command(), |(_, command)| *command));
                read_only_check(command, &self.settings, session, &mut self.diagnostic)?;
                self.settings.queried = session.in_transaction();
                if p.command() == "SELECT" && writes.is_empty() {
                    let mut cursor = self.diagnostic.core(session.open_cursor(&p, &[]))?;
                    row_description(&d, &[], out)?;
                    let mut count = 0;
                    while !cursor.is_exhausted() {
                        // GD-0061 R3-311: fetch unreleased, release, then encode.
                        let chunk = self.diagnostic.core(session.fetch_cursor_unreleased(
                            &mut cursor,
                            CURSOR_ROWS,
                            CURSOR_BYTES,
                        ))?;
                        let result = self
                            .diagnostic
                            .core(session.release_page(chunk))?
                            .map(|rows| SqlResult {
                                rows,
                                ..SqlResult::default()
                            });
                        let n = result.get().rows.len();
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
                // Keep the durability registration through the checked reply.
                let _durability = if !was_transaction {
                    giggles_core::Cancellation::current().and_then(|context| self.durability_registration(&context, session))
                } else {
                    None
                };
                // Core's autocommit transaction starts inside execution, too late
                // to set its transaction-local replica flag. Pin it here instead.
                if self.settings.replication_role_replica() && !matches!(p.command(), "BEGIN" | "COMMIT" | "ROLLBACK") {
                    begin_implicit(session, implicit, &mut self.settings)?;
                }
                self.diagnostic.core(session.sync_replication_role(&self.settings))?;
                let autocommit_write = !session.in_transaction() && (!writes.is_empty() || p.is_ddl());
                self.simple_prepared_reply(&p, &d, session, out, was_transaction, autocommit_write)?;
            }
        }
        Ok(())
    }
    fn simple_prepared_reply(
        &mut self,
        p: &Prepared,
        d: &Description,
        session: &mut Session,
        out: &mut Vec<u8>,
        was_transaction: bool,
        autocommit_write: bool,
    ) -> R<()> {
        // GD-0061 R3-311: only untasked results participate in the reply
        // check. The normal release still guards the returned page.
        let mut checked = false;
        let mut spill_completion = Vec::new();
        let completed = out.len();
        let zone = session.zone().unwrap_or(SessionZone::Fixed(0));
        let encoder = SimpleRows {
            settings: &self.settings.current,
            zone,
            description: d,
        };
        let (result, spilled) = giggles_core::relational::spill::streaming(|| {
            if session.in_transaction() || d.columns.is_empty() {
                return session.execute_prepared_unreleased(p, &[]);
            }
            let mut check = |result: Released<&SqlResult>, spill: Option<&mut SpilledRows>| {
                checked = true;
                if let Some(spill) = spill {
                    // Mirror delivery's buffer limits without sending any bytes.
                    let mut scratch = out.clone();
                    encoder.spilled(spill, &mut scratch, |buf| {
                        if self.output.is_some() {
                            buf.clear();
                        }
                        giggles_core::cancellation::check()
                    })?;
                    let suffix = scratch.len();
                    completion_frames(p.command(), &result, &mut scratch, ReplyLimit::Checked)
                        .map_err(CoreError::SqlReplyEncoding)?;
                    spill_completion.extend_from_slice(&scratch[suffix..]);
                    Ok(())
                } else {
                    // The per-message output is private until execution succeeds.
                    row_description(d, &[], out).map_err(CoreError::SqlReplyEncoding)?;
                    encoder.encode(&result, out).map_err(CoreError::SqlReplyEncoding)?;
                    completion_frames(p.command(), &result, out, ReplyLimit::Checked)
                        .map_err(CoreError::SqlReplyEncoding)
                }
            };
            session.execute_prepared_checked_unreleased(p, &mut check)
        });
        if checked && result.is_err() {
            out.truncate(completed);
        }
        self.retain_durability_permits |= spilled.is_some();
        let result = self.diagnostic.core(result)?;
        self.diagnostic.committed |= checked || autocommit_write;
        let result = self.diagnostic.core(session.release_page(result))
            .map_err(|code| if checked || autocommit_write { "08007" } else { code })?;
        debug_assert!(
            spill_completion.is_empty() || spilled.is_some(),
            "checked spill disappeared"
        );
        if !spill_completion.is_empty() && spilled.is_none() {
            return Err("08007");
        }
        self.settings
            .transaction(was_transaction, session.in_transaction(), p.command());
        let encoder = SimpleRows {
            settings: &self.settings.current,
            zone,
            description: d,
        };
        debug_assert!(checked || !autocommit_write || d.columns.is_empty());
        if let Some(mut spilled) = spilled {
            if session.task().is_some() {
                return Err("XX000");
            }
            let after_commit = spilled.after_commit();
            let sent = encoder.spilled(&mut spilled, out, |out| {
                flush_output(&self.output, out).map_err(CoreError::SqlReplyEncoding)
            });
            self.diagnostic.core(sent.map_err(|error| {
                if after_commit {
                    CoreError::Storage(error.to_string())
                } else {
                    error
                }
            }))?;
            if checked {
                out.extend(spill_completion);
            }
        } else if !checked && !d.columns.is_empty() {
            row_description(d, &[], out)
                .and_then(|()| encoder.encode(&result, out))
                .map_err(|code| if autocommit_write { "08007" } else { code })?;
        }
        if !checked {
            completion_frames(p.command(), &result, out, ReplyLimit::after_commit(autocommit_write))?;
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
        let mut syntax_at = None;
        let result = diagnostic.settings(crate::pg_settings::database::parse_database(
            sql,
            &mut syntax_at,
        ));
        if let Some(token) = syntax_at {
            diagnostic.wire_message = Some(format!(r#"syntax error at or near "{token}""#));
        }
        if matches!(result, Err("22023")) && let Ok(tokens) = crate::pg_settings::database::lex(sql) {
            use crate::pg_settings::database::Token;
            if tokens.iter().any(|t| matches!(t, Token::Word(n) | Token::Ident(n) if n.eq_ignore_ascii_case("gigglesdb.max_statement_tokens")))
                && let Some(Token::Word(value) | Token::Text(value)) = tokens.last() {
                diagnostic.wire_message = Some(format!("{value} is outside the valid range for parameter \"gigglesdb.max_statement_tokens\" (0 .. 2147483647)"));
            }
            if tokens.iter().any(|t| matches!(t, Token::Word(n) | Token::Ident(n) if n.eq_ignore_ascii_case("gigglesdb.max_value_bytes")))
                && let Some(Token::Word(value) | Token::Text(value)) = tokens.last() {
                diagnostic.wire_message = Some(format!("{value} is outside the valid range for parameter \"gigglesdb.max_value_bytes\" (65536 .. 1073741819)"));
            }
        }

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
                        diagnostic.wire_detail = Some("Cannot specify days in time zone interval".into());
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
    settings_status_with_limit(before, settings, session, out, ReplyLimit::Checked)
}

pub(super) fn settings_status_with_limit(
    before: &crate::pg_settings::Settings,
    settings: &crate::pg_settings::SessionSettings,
    session: &Session,
    out: &mut Vec<u8>,
    limit: ReplyLimit,
) -> R<()> {
    if matches!(session.role, Role::Database { .. }) {
        for show in crate::pg_settings::REPORTABLE {
            let (name, value) = show.value(&settings.current);
            if value != show.value(before).1 {
                parameter_status(name, &value, out, limit)?;
            }
        }
    }
    Ok(())
}

fn parameter_status(name: &str, value: &str, out: &mut Vec<u8>, limit: ReplyLimit) -> R<()> {
    let mut body = Vec::new();
    z(name, &mut body);
    z(value, &mut body);
    limit.frame(b'S', &body, out)
}

pub(super) fn commit_reply(
    settings: &crate::pg_settings::SessionSettings,
    session: &Session,
    command_complete: bool,
    out: &mut Vec<u8>,
) -> R<()> {
    if matches!(session.role, Role::Database { .. }) {
        for &show in crate::pg_settings::REPORTABLE {
            if let Some(value) = settings.post_commit_value(show) {
                let (name, current) = show.value(&settings.current);
                if value != current {
                    parameter_status(name, value, out, ReplyLimit::Control)?;
                }
            }
        }
    }
    if command_complete {
        completion_frames("COMMIT", &empty(), out, ReplyLimit::Control)?;
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
    session: &mut Session,
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
        if let crate::pg_settings::Setting::Database { name, value, .. } = assignment
            && matches!(name, crate::pg_settings::Show::Role | crate::pg_settings::Show::SessionAuthorization)
            && let Err(code) = session.check_role_setting(*name, value.as_deref()) {
                if let Some(value) = value {
                    diagnostic.wire_message = Some(if code == "22023" { format!("role \"{value}\" does not exist") }
                        else if *name == crate::pg_settings::Show::Role { format!("permission denied to set role \"{value}\"") }
                        else { format!("permission denied to set session authorization \"{value}\"") });
                }
                return Err(code);
        }
    }
    for assignment in assignments {
        if let crate::pg_settings::Setting::Database { name, value: Some(value), .. } = assignment {
            diagnostic.presentation_setting(*name, value, &settings.current)?;
        }
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
        if matches!(assignment, crate::pg_settings::Setting::Database { name: crate::pg_settings::Show::Role | crate::pg_settings::Show::SessionAuthorization, .. }) && !warn {
            session.pin_role_settings(&mut settings.current)?;
        }
        session.sync_role_settings(&mut settings.current)?;
        settings_status(&before, settings, session, out)?;
        complete(assignment.tag(), &empty(), out)?;
    }
    Ok(())
}

fn copy_csv_field(value: &[u8], options: &crate::auth::statements::CopyOutput, force: bool, out: &mut Vec<u8>) {
    let quoted = force || value == options.null
        || value == b"\\."
        || value
            .iter()
            .any(|b| *b == options.delimiter || *b == options.quote || matches!(b, b'\n' | b'\r'));
    if quoted {
        out.push(options.quote);
    }
    for b in value {
        if quoted && (*b == options.quote || *b == options.escape) {
            out.push(options.escape);
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
                        .map_err(crate::service::role_lookup_error)?
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
                Role::NoSql { .. } => false,
                #[cfg(feature = "legacy-task")]
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

pub(super) fn role_notices(
    session: &mut Session,
    settings: &crate::pg_settings::Settings,
    out: &mut Vec<u8>,
    limit: ReplyLimit,
) -> R<()> {
    for (warning, message) in std::mem::take(&mut session.role_notices) {
        if settings.client_min_messages == "error" || (!warning && settings.client_min_messages == "warning") { continue; }
        let mut body = Vec::new();
        for (field, value) in [(b'S', if warning { "WARNING" } else { "NOTICE" }), (b'C', if warning { "01000" } else { "00000" }), (b'M', message.as_str())] {
            body.push(field); z(value, &mut body);
        }
        body.push(0); limit.frame(b'N', &body, out)?;
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
        assert_eq!(split(&" ".repeat(giggles_core::relational::MAX_STATEMENT_SQL + 1)), Err("54000"));
    }
}
