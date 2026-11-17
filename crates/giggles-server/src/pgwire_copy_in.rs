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
    normalized_bytes: usize,
    simple: bool,
    failed: bool,
    pub(super) simple_implicit: bool,
    pub(super) remaining: Vec<String>,
}
impl CopyIn {
    fn flush_page(&mut self, session: &mut Session, count: usize) -> R<()> {
        let rows = self.rows.drain(..count).collect();
        let (prepared, rows) = core(session.normalize_copy_rows(&self.prepared, rows, &self.clock))?;
        core(session.sql.feed_copy_input_at(&prepared, rows, &self.clock))?;
        self.normalized = Some(prepared);
        Ok(())
    }
    fn convert(&mut self, rows: Vec<Vec<Option<Vec<u8>>>>, session: &mut Session) -> R<()> {
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
                        value.map_or(Ok(Value::Null), |bytes| {
                            if format == 1 {
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
                            }
                        })
                    })
                    .collect::<R<Vec<_>>>()?;
            if missing {
                // PG converts the supplied prefix before reporting missing data.
                // Normalization rejects the short row before evaluating defaults.
                core(session.normalize_copy_rows(&self.prepared, vec![row], &self.clock))?;
                return Err("22P04");
            }
            converted.push(row);
        }
        if self.streaming {
            self.rows.extend(converted);
            while self.rows.len() >= 200 { self.flush_page(session, 200)?; }
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
                core(session.normalize_copy_rows(&self.prepared, converted, &self.clock))?;
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
        if let Some(copy) = self.copy.as_mut() {
            if copy.failed {
                return;
            }
            copy.failed = true;
            copy.rows.clear();
            copy.remaining.clear();
            if code == "25006" {
                diagnostic_error(code, GENERIC_MESSAGE, Some(&self.diagnostic), out);
            } else {
                error(code, out);
            }
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
        simple: bool,
        session: &mut Session,
        out: &mut Vec<u8>,
    ) -> R<()> {
        let columns = core(session.copy_columns(&descriptor, &prepared))?;
        let binary = matches!(descriptor.format, CopyFormat::Binary);
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
        frame(b'G', &response, out)?;
        self.copy = Some(CopyIn {
            descriptor,
            streaming,
            prepared,
            normalized: None,
            clock,
            columns,
            decoder,
            rows: vec![],
            normalized_bytes: 0,
            simple,
            failed: false,
            simple_implicit: false,
            remaining: vec![],
        });
        Ok(())
    }
    pub(super) fn copy_message(
        &mut self,
        tag: u8,
        b: &[u8],
        session: &mut Session,
        out: &mut Vec<u8>,
        received: std::time::Instant,
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
        if !copy.failed {
            let result = (|| {
                match tag {
                    b'd' => {
                        for bytes in b.chunks(if copy.streaming { 128 } else { b.len().max(1) }) {
                            let rows = copy.decoder.feed(bytes)?;
                            copy.convert(rows, session)?;
                        }
                    }
                    b'c' => {
                        if !b.is_empty() {
                            return Err("08P01");
                        }
                        let rows = copy.decoder.finish()?;
                        copy.convert(rows, session)?;
                        let result = if copy.streaming {
                            if !copy.rows.is_empty() {
                                copy.flush_page(session, copy.rows.len())?;
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
                        let mut body = vec![];
                        z(&format!("COPY {}", result.affected), &mut body);
                        if copy.simple {
                            frame(b'C', &body, out)?;
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
                session.abort();
                copy.failed = true;
                copy.rows.clear();
                copy.remaining.clear();
                if code == "25006" {
                    diagnostic_error(code, GENERIC_MESSAGE, Some(&self.diagnostic), out);
                } else {
                    error(code, out);
                }
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
            let mut implicit = copy.simple_implicit;
            let mut remaining = copy.remaining.into_iter();
            while let Some(sql) = remaining.next() {
                if let Err(code) =
                    self.simple_statement(&sql, session, out, received, &mut implicit)
                {
                    session.abort();
                    error(code, out);
                    if implicit {
                        session.sql.rollback();
                        self.settings.transaction(true, false, "ROLLBACK");
                    }
                    break;
                }
                if let Some(next) = self.copy.as_mut() {
                    next.remaining = remaining.collect();
                    return Ok(true);
                }
            }
            if self.copy.is_some() {
                return Ok(true);
            }
            if implicit {
                execute_implicit_commit(session, &mut implicit, &mut self.settings, out)?;
            }
        }
        if !copy.failed {
            ready(session, out)?;
        }
        Ok(true)
    }
}
