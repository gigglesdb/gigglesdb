//! COPY FROM STDIN protocol state; rows remain private until CopyDone.
use super::*;
use giggles_core::relational::{Copy, CopyFormat};

pub(super) fn is_copy_sql(sql: &str) -> bool {
    Copy::is_copy(sql)
}
pub(super) struct CopyIn {
    descriptor: Copy,
    streaming: bool,
    prepared: Box<Prepared>,
    normalized: Option<Prepared>,
    clock: giggles_core::relational::StatementClock,
    columns: Vec<giggles_core::relational::ColumnDef>,
    decoder: copy::Decoder,
    rows: Vec<Vec<Value>>,
    raw_rows: Vec<Vec<Option<Value>>>,
    next_line: u64,
    skipped: u64,
    normalized_bytes: usize,
    input_error_message: Option<String>,
    pub(super) simple: bool,
    failed: bool,
    pub(super) simple_implicit: bool,
    pub(super) remaining: Vec<String>,
}
impl CopyIn {
    fn normalize_rows(&mut self, rows: Vec<Vec<Option<Value>>>, session: &mut Session) -> R<(Prepared, Vec<Vec<Value>>)> {
        // Legacy scalar errors can be field-less. A syntax error in the first
        // supplied integer is unambiguously the first conversion failure;
        // retain its PG diagnostic without replacing assignment coercion.
        let message = rows.first().and_then(|row| row.first()).and_then(|value| {
            let Some(Value::Text(value)) = value else { return None; };
            use giggles_core::relational::DataType;
            use giggles_pg_types::integer;
            let (name, error) = match self.columns.first()?.data_type {
                DataType::SmallInt => ("smallint", integer::parse_i16(value).err()),
                DataType::Integer => ("integer", integer::parse_i32(value).err()),
                DataType::BigInt => ("bigint", integer::parse_i64(value).err()),
                _ => return None,
            };
            matches!(error, Some(integer::IntegerError::Syntax))
                .then(|| format!("invalid input syntax for type {name}: \"{value}\""))
        });
        let options = self.descriptor.options.clone();
        let count = rows.len() as u64;
        let input = giggles_core::relational::CopyInput { rows, options, first_line: self.next_line, skipped: self.skipped };
        let result = session.normalize_copy_cells(&self.prepared, input, &self.clock);
        self.next_line += count;
        if matches!(result, Err(CoreError::Sql(giggles_core::relational::SqlError::InvalidTextRepresentation))) {
            self.input_error_message = message;
        }
        let (prepared, rows, skipped) = core(result)?;
        self.skipped += skipped;
        Ok((prepared, rows))
    }

    fn flush_page(&mut self, session: &mut Session, count: usize, out: &mut Vec<u8>) -> R<()> {
        let rows = self.raw_rows.drain(..count).collect();
        let (prepared, rows) = self.normalize_rows(rows, session)?;
        copy_notices(out)?;
        core(session.sql.feed_copy_input_at(&prepared, rows, &self.clock))?;
        self.normalized = Some(prepared);
        Ok(())
    }
    fn decoding_error(&mut self, code: &'static str, rows: Vec<Vec<copy::Field>>, session: &mut Session, out: &mut Vec<u8>) -> R<()> {
        let prefix = self.convert(rows, session, out).and_then(|()| {
            if self.streaming && !self.raw_rows.is_empty() { self.flush_page(session, self.raw_rows.len(), out)?; }
            Ok(())
        });
        if let Err(earlier) = prefix { self.decoder.clear_diagnostic(); return Err(earlier); }
        Err(code)
    }
    fn feed(&mut self, bytes: &[u8], session: &mut Session, out: &mut Vec<u8>) -> R<()> {
        match self.decoder.feed(bytes) {
            Ok(rows) => self.convert(rows, session, out),
            Err(code) => {
                let rows = self.decoder.take_completed_rows();
                self.decoding_error(code, rows, session, out)
            }
        }
    }

    fn finish(&mut self, session: &mut Session, out: &mut Vec<u8>) -> R<()> {
        match self.decoder.finish() {
            Ok(rows) => self.convert(rows, session, out),
            Err(code) => {
                let rows = self.decoder.take_completed_rows();
                self.decoding_error(code, rows, session, out)
            }
        }
    }
    fn convert(&mut self, rows: Vec<Vec<copy::Field>>, session: &mut Session, out: &mut Vec<u8>) -> R<()> {
        let zone = session.zone().unwrap_or(SessionZone::Fixed(0));
        let mut converted = vec![];
        let format = u16::from(matches!(self.descriptor.format, CopyFormat::Binary));
        for row in rows {
            if row.len() > self.columns.len() {
                return Err(crate::service::sqlstate(&CoreError::Sql(
                    giggles_core::relational::SqlError::BadCopyFileFormat,
                )));
            }
            let missing = row.len() < self.columns.len();
            let row = row.into_iter()
                    .zip(&self.columns)
                    .map(|(value, column)| {
                        let bytes = match value {
                            copy::Field::Default => return Ok(None),
                            copy::Field::Value(None) => return Ok(Some(Value::Null)),
                            copy::Field::Value(Some(bytes)) => bytes,
                        };
                        (if format == 1 {
                                decode_in_zone(
                                    &bytes,
                                    &column.data_type,
                                    1,
                                    zone,
                                    self.clock.transaction_us,
                                )
                            } else {
                                String::from_utf8(bytes)
                                    .map(Value::Text)
                                    .map_err(|_| "22021")
                            }).map(Some)
                    })
                    .collect::<R<Vec<_>>>()?;
            if missing {
                // A supplied conversion failure precedes the missing next field.
                self.accept_converted(std::mem::take(&mut converted), session, out)?;
                if self.streaming && !self.raw_rows.is_empty() { self.flush_page(session, self.raw_rows.len(), out)?; }
                let (_, accepted) = self.normalize_rows(vec![row], session)?;
                copy_notices(out)?;
                if self.descriptor.options.on_error_ignore && accepted.is_empty() { continue; }
                return Err("22P04");
            }
            converted.push(row);
        }
        self.accept_converted(converted, session, out)
    }
    fn accept_converted(&mut self, converted: Vec<Vec<Option<Value>>>, session: &mut Session, out: &mut Vec<u8>) -> R<()> {
        if self.streaming {
            self.raw_rows.extend(converted);
            while self.raw_rows.len() >= 200 { self.flush_page(session, 200, out)?; }
            return Ok(());
        }
        if self
            .rows
            .len()
            .checked_add(converted.len())
            .ok_or("54000")?
            > giggles_core::relational::MAX_ROWS
        {
            return Err("54000");
        }
        if !converted.is_empty() {
            let (prepared, rows) =
                self.normalize_rows(converted, session)?;
            copy_notices(out)?;
            for value in rows.iter().flatten() {
                let bytes = match value {
                    Value::Text(s) => s.len(),
                    Value::Bytes(b) => b.len(),
                    Value::Numeric(n) => n.to_string().len(),
                    Value::Float4(_) | Value::Date(_) => 4,
                    Value::Float8(_)
                    | Value::Time(_)
                    | Value::Timestamp(_)
                    | Value::TimestampTz(_) => 8,
                    Value::TimeTz(_) => 12,
                    Value::Interval(_) => 16,
                    Value::Null | Value::Bool(_) | Value::Int(_) => 9,
                };
                self.normalized_bytes = self.normalized_bytes.checked_add(bytes).ok_or("54000")?;
                if self.normalized_bytes > giggles_core::operations::MAX_INSERT_BYTES {
                    return Err("54000");
                }
            }
            self.normalized = Some(prepared);
            self.rows.extend(rows);
        }
        Ok(())
    }
}
fn copy_notices(out: &mut Vec<u8>) -> R<()> {
    copy_notices_with_limit(out, ReplyLimit::Checked)
}

fn copy_notices_with_limit(out: &mut Vec<u8>, limit: ReplyLimit) -> R<()> {
    for (message, detail, code) in giggles_core::relational::take_sql_notices() {
        let mut body = vec![];
        for (field, value) in [(b'S', "NOTICE"), (b'V', "NOTICE"), (b'C', code.unwrap_or("00000")), (b'M', message.as_str())] {
            body.push(field); z(value, &mut body);
        }
        if let Some(detail) = detail { body.push(b'D'); z(&detail, &mut body); }
        body.push(0); limit.frame(b'N', &body, out)?;
    }
    Ok(())
}
impl CopyIn {
    fn summary(&self, out: &mut Vec<u8>, limit: ReplyLimit) -> R<()> {
        copy_notices_with_limit(out, limit)?;
        if self.skipped > 0 && self.descriptor.options.log_verbosity != giggles_core::relational::CopyLogVerbosity::Silent {
            let message = if self.skipped == 1 { "1 row was skipped due to data type incompatibility".into() }
                else { format!("{} rows were skipped due to data type incompatibility", self.skipped) };
            let mut body = vec![];
            for (field, value) in [(b'S', "NOTICE"), (b'V', "NOTICE"), (b'C', "00000"), (b'M', message.as_str())] { body.push(field); z(value, &mut body); }
            body.push(0); limit.frame(b'N', &body, out)?;
        }
        Ok(())
    }
}

impl State {
    pub(super) fn copy_receiving(&self) -> bool {
        self.copy.as_ref().is_some_and(|copy| !copy.failed)
    }
    pub(super) fn copy_error(
        &mut self,
        code: &'static str,
        session: &mut Session,
        out: &mut Vec<u8>,
    ) {
        self.diagnostic.authorize_duplicate_key(code, session);
        if let Some(copy) = self.copy.as_mut() {
            if copy.failed {
                return;
            }
            copy.failed = true;
            copy.rows.clear();
            copy.raw_rows.clear();
            copy.remaining.clear();
            self.diagnostic.unknown_outcome = code == "08007";
            let mut response = Vec::new();
            if code == "25006" || self.diagnostic.wire_message.is_some() {
                diagnostic_error(code, GENERIC_MESSAGE, Some(&self.diagnostic), &mut response);
            } else {
                error(code, &mut response);
            }
            out.extend(response);
            if copy.simple {
                if copy.simple_implicit {
                    session.sql.rollback();
                    self.settings.transaction(true, false, "ROLLBACK");
                    self.portals.clear();
                }
                let _ = ready(session, out);
            }
        }
    }
    pub(super) fn start_copy(
        &mut self,
        descriptor: Copy,
        prepared: Box<Prepared>,
        simple: Option<bool>,
        session: &mut Session,
        out: &mut Vec<u8>,
    ) -> R<()> {
        let simple_implicit = simple.unwrap_or(false);
        let simple = simple.is_some();
        self.retain_durability_permits = true;
        let columns = core(session.copy_columns(&descriptor, &prepared))?;
        let binary = matches!(descriptor.format, CopyFormat::Binary);
        if !binary && let Some(message) = descriptor.options.encoding.and_then(|e| e.missing_conversion(true)) {
            self.diagnostic.wire_message = Some(message);
            return Err("42883");
        }
        let mut decoder = if binary {
            copy::Decoder::binary(columns.len())
        } else if descriptor.format == CopyFormat::Csv {
            copy::Decoder::csv(
                columns.len(),
                descriptor.delimiter,
                descriptor.null.as_bytes().to_vec(),
            )
        } else {
            copy::Decoder::text(
                columns.len(),
                descriptor.delimiter,
                descriptor.null.as_bytes().to_vec(),
            )
        };
        decoder.options(&descriptor.options, columns.iter().map(|c| c.name.clone()).collect());
        let mut response = vec![u8::from(binary)];
        response.extend(
            u16::try_from(columns.len())
                .map_err(|_| "54000")?
                .to_be_bytes(),
        );
        for _ in &columns {
            response.extend(u16::from(binary).to_be_bytes());
        }
        core(session.sync_replication_role(&self.settings))?;
        let clock = session.copy_clock();
        let streaming = !binary && core(session.sql.begin_copy_input())?;
        if streaming { decoder.stream(); }
        let external = !matches!(descriptor.source, giggles_core::relational::CopySource::Stdin);
        if !external { frame(b'G', &response, out)?; }
        self.copy = Some(CopyIn {
            streaming,
            prepared,
            normalized: None,
            clock,
            columns,
            decoder,
            rows: vec![],
            raw_rows: vec![],
            next_line: 1 + u64::from(descriptor.options.header != giggles_core::relational::CopyHeader::Off),
            skipped: 0,
            normalized_bytes: 0,
            input_error_message: None,
            simple,
            failed: false,
            simple_implicit,
            remaining: vec![],
            descriptor,
        });
        if self.copy.as_ref().unwrap().descriptor.target_is_view {
            let name = self.copy.as_ref().unwrap().descriptor.table.rsplit('\0').next().unwrap();
            self.diagnostic.wire_message = Some(format!("cannot copy to view \"{name}\""));
            self.diagnostic.wire_hint = Some("To enable copying to a view, provide an INSTEAD OF INSERT trigger.");
            session.sql.abort();
            self.copy_error("42809", session, out);
            return Ok(());
        }
        if self.copy.as_ref().unwrap().descriptor.options.freeze {
            let failure = if self.cursors.has_open() {
                Some(("25000", "cannot perform COPY FREEZE because of prior transaction activity"))
            } else if !core(session.sql.copy_freeze_allowed(&self.copy.as_ref().unwrap().descriptor))? {
                Some(("55000", "cannot perform COPY FREEZE because the table was not created or truncated in the current subtransaction"))
            } else { None };
            if let Some((code, message)) = failure {
                self.diagnostic.wire_message = Some(message.into());
                session.sql.abort();
                self.copy_error(code, session, out);
                return Ok(());
            }
        }
        if external { self.external_copy_input(session, out)?; }
        Ok(())
    }
    fn external_copy_input(&mut self, session: &mut Session, out: &mut Vec<u8>) -> R<()> {
        use giggles_core::relational::CopySource;
        use std::io::Read;
        let mut copy = self.copy.take().unwrap();
        let mut child = None;
        let mut reader: Box<dyn Read> = match &copy.descriptor.source {
            CopySource::Stdin => unreachable!(),
            CopySource::File(path) => Box::new(copy_external::input_file(session, path, &mut self.diagnostic)?),
            CopySource::Program(command) => {
                let mut process = copy_external::spawn(session, command, true, &mut self.diagnostic)?;
                let stdout = process.stdout.take().ok_or("58030")?;
                child = Some((process, command.clone()));
                Box::new(stdout)
            }
        };
        let result = (|| {
            let mut bytes = [0; 8192];
            loop {
                let count = reader.read(&mut bytes).map_err(|error| copy_external::read_error(&mut self.diagnostic, error))?;
                if count == 0 { break; }
                copy.feed(&bytes[..count], session, out)?;
                copy_notices(out)?;
            }
            copy.finish(session, out)?;
            if copy.streaming && !copy.raw_rows.is_empty() { copy.flush_page(session, copy.raw_rows.len(), out)?; }
            Ok(())
        })();
        drop(reader);
        if let Err(code) = result {
            if let Some(message) = giggles_core::relational::take_sql_error_message().or_else(|| copy.input_error_message.take()) { self.diagnostic.wire_message = Some(message); }
            self.diagnostic.wire_detail = self.diagnostic.read_core_detail();
            copy_notices(out)?;
            if let Some((mut process, _)) = child { let _ = process.kill(); let _ = process.wait(); }
            copy.decoder.diagnostic(&mut self.diagnostic);
            if let Some(line) = copy.decoder.encoding_error_line() {
                self.diagnostic.wire_context = Some(format!("COPY {}, line {line}", copy.descriptor.table));
            }
            return Err(code);
        }
        if let Some((mut process, command)) = child { copy_external::wait(&mut process, &command, &mut self.diagnostic)?; }
        let result = if copy.streaming {
            self.diagnostic.core(session.finish_copy_input(&copy.prepared, copy.normalized.as_ref().unwrap_or(&copy.prepared), &copy.clock))?
        } else {
            self.diagnostic.core(session.ingest_normalized_copy(&copy.prepared, copy.normalized.as_ref().unwrap_or(&copy.prepared), std::mem::take(&mut copy.rows), &copy.clock))?
        };
        let committed = !session.in_transaction();
        self.diagnostic.committed |= committed;
        let limit = ReplyLimit::after_commit(committed);
        copy.summary(out, limit)?;
        completion_frames(&format!("COPY {}", result.affected), &empty(), out, limit)
    }
    pub(super) fn copy_message(
        &mut self,
        tag: u8,
        b: &[u8],
        session: &mut Session,
        out: &mut Vec<u8>,
        received: std::time::Instant,
        output: &Output,
    ) -> R<bool> {
        let mut copy = self.copy.take().unwrap();
        if tag == b'X' {
            return Ok(false);
        }
        if copy.failed && (tag == b'S' || (copy.simple && tag == b'Q')) {
            self.drain = false;
            return self.handle_inner(tag, b, session, out, received);
        }
        if matches!(tag, b'H' | b'S') {
            self.copy = Some(copy);
            return Ok(true);
        }
        let done = matches!(tag, b'c' | b'f');
        let mut completed = out.len();
        if !copy.failed {
            let result = (|| {
                match tag {
                    b'd' => {
                        copy.feed(b, session, out)?;
                        copy_notices(out)?;
                    }
                    b'c' => {
                        if !b.is_empty() {
                            return Err("08P01");
                        }
                        copy.finish(session, out)?;
                        let result = if copy.streaming {
                            if !copy.raw_rows.is_empty() {
                                copy.flush_page(session, copy.raw_rows.len(), out)?;
                            }
                            self.diagnostic.core(session.finish_copy_input(
                                &copy.prepared,
                                copy.normalized.as_ref().unwrap_or(&copy.prepared),
                                &copy.clock,
                            ))?
                        } else {
                            self.diagnostic.core(session.ingest_normalized_copy(
                                &copy.prepared,
                                copy.normalized.as_ref().unwrap_or(&copy.prepared),
                                std::mem::take(&mut copy.rows),
                                &copy.clock,
                            ))?
                        };
                        // Core-owned COPY commits leave no transaction. The
                        // wire-owned simple block commits below instead.
                        let committed = !session.in_transaction();
                        self.diagnostic.committed |= committed;
                        let limit = ReplyLimit::after_commit(committed);
                        copy.summary(out, limit)?;
                        completed = out.len();
                        let mut body = vec![];
                        z(&format!("COPY {}", result.affected), &mut body);
                        if copy.simple {
                            limit.frame(b'C', &body, out)?;
                        } else {
                            frame(b'C', &body, &mut self.copy_completion)?;
                        }
                    }
                    b'f' => {
                        let mut reader = Reader { b, at: 0 };
                        reader.text()?;
                        reader.end()?;
                        return Err("57014");
                    }
                    _ => return Err("08P01"),
                }
                Ok(())
            })();
            if let Err(code) = result {
                let message = giggles_core::relational::take_sql_error_message().or_else(|| copy.input_error_message.take());
                if message.is_some() { self.diagnostic.wire_message = message; }
                self.diagnostic.wire_detail = self.diagnostic.read_core_detail();
                self.diagnostic.authorize_duplicate_key(code, session);
                session.abort();
                copy.failed = true;
                copy.rows.clear();
                copy.raw_rows.clear();
                copy.remaining.clear();
                copy.decoder.diagnostic(&mut self.diagnostic);
                if let Some(line) = copy.decoder.encoding_error_line() {
                    self.diagnostic.wire_context = Some(format!("COPY {}, line {line}", copy.descriptor.table));
                }
                self.diagnostic.unknown_outcome = code == "08007";
                copy_notices(out)?;
                let mut failure = vec![];
                if code == "25006" || self.diagnostic.wire_message.is_some() {
                    diagnostic_error(code, GENERIC_MESSAGE, Some(&self.diagnostic), &mut failure);
                } else {
                    error(code, &mut failure);
                }
                out.extend(failure);
                if copy.simple {
                    if copy.simple_implicit {
                        session.sql.rollback();
                        self.settings.transaction(true, false, "ROLLBACK");
                        self.portals.clear();
                    }
                    ready(session, out)?;
                }
            }
        }
        if !done {
            self.copy = Some(copy);
            return Ok(true);
        }
        if !copy.simple {
            self.drain = copy.failed;
            return Ok(true);
        }
        if copy.failed && copy.simple_implicit {
            session.sql.rollback();
            self.settings.transaction(true, false, "ROLLBACK");
            self.portals.clear();
        }
        if !copy.failed {
            if copy.remaining.is_empty() {
                if copy.simple_implicit {
                    // A terminal CopyDone keeps COPY n only if the simple
                    // block commits. The commit's control suffix is uncapped.
                    self.finish_simple_batch(session, out, completed);
                }
            } else {
                self.continue_simple_batch(
                    copy.remaining.into_iter(),
                    session,
                    out,
                    received,
                    output,
                    copy.simple_implicit,
                )?;
            }
            if self.copy.is_none() {
                ready(session, out)?;
            }
        }
        Ok(true)
    }
}
