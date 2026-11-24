//! Snapshot-backed SQL cursors, distinct from extended-protocol portals.
use super::*;

#[derive(Clone)]
pub(super) enum Command {
    Declare {
        name: String,
        hold: bool,
        sql: String,
        prepared: Option<Box<Prepared>>,
    },
    Fetch {
        name: String,
        count: usize,
        moving: bool,
    },
    Close(Option<String>),
}
struct Cursor {
    description: Description,
    result: giggles_core::Cursor,
    at: usize,
    hold: bool,
    exhausted: bool,
    pending: bool,
}
#[derive(Default)]
pub(super) struct Cursors(BTreeMap<String, Cursor>);
impl Cursors {
    pub(super) fn has_open(&self) -> bool { !self.0.is_empty() }
}

// Read only the cursor-command prefix; the SELECT is parsed by the SQL engine.
struct Prefix<'a>(&'a str);
impl Prefix<'_> {
    fn word(&mut self) -> R<(String, bool)> {
        self.0 = self.0.trim_start();
        if self.0.starts_with('"') {
            let mut name = String::new();
            let mut chars = self.0[1..].char_indices().peekable();
            while let Some((i, ch)) = chars.next() {
                if ch == '"' {
                    if chars.peek().is_some_and(|(_, c)| *c == '"') {
                        chars.next();
                        name.push('"');
                    } else {
                        self.0 = &self.0[i + 2..];
                        return Ok((name, true));
                    }
                } else {
                    name.push(ch);
                }
            }
            return Err("42601");
        }
        let end = self
            .0
            .find(|c: char| c.is_whitespace() || c == ';')
            .unwrap_or(self.0.len());
        if end == 0 {
            return Err("42601");
        }
        let value = self.0[..end].to_owned();
        self.0 = &self.0[end..];
        Ok((value, false))
    }
    fn kw(&mut self, kw: &str) -> bool {
        let saved = self.0;
        if self
            .word()
            .is_ok_and(|(s, quoted)| !quoted && s.eq_ignore_ascii_case(kw))
        {
            true
        } else {
            self.0 = saved;
            false
        }
    }
    fn need(&mut self, kw: &str) -> R<()> {
        if self.kw(kw) { Ok(()) } else { Err("42601") }
    }
    fn name(&mut self) -> R<String> {
        let (mut name, quoted) = self.word()?;
        if !quoted {
            if !name.chars().enumerate().all(|(i, c)| {
                c == '_' || c.is_alphabetic() || (i > 0 && (c.is_ascii_digit() || c == '$'))
            }) {
                return Err("42601");
            }
            name.make_ascii_lowercase();
        }
        while name.len() > 63 {
            name.pop();
        }
        Ok(name)
    }
    fn end(&self) -> R<()> {
        if self.0.trim().trim_end_matches(';').trim().is_empty() {
            Ok(())
        } else {
            Err("42601")
        }
    }
}
pub(super) fn parse(sql: &str) -> R<Option<Command>> {
    let mut p = Prefix(sql);
    let command = if p.kw("DECLARE") {
        let name = p.name()?;
        if p.kw("NO") {
            p.need("SCROLL")?;
        } else if p.kw("SCROLL") {
            return Err("0A000");
        }
        p.need("CURSOR")?;
        let hold = if p.kw("WITH") {
            p.need("HOLD")?;
            true
        } else {
            if p.kw("WITHOUT") {
                p.need("HOLD")?;
            }
            false
        };
        p.need("FOR")?;
        let sql = p.0.trim().to_owned();
        Command::Declare {
            name,
            hold,
            sql,
            prepared: None,
        }
    } else if p.kw("FETCH") || p.kw("MOVE") {
        let moving = sql
            .trim_start()
            .get(..4)
            .is_some_and(|s| s.eq_ignore_ascii_case("MOVE"));
        p.kw("FORWARD");
        let count = if p.kw("ALL") {
            usize::MAX
        } else if p.kw("NEXT") {
            1
        } else {
            let saved = p.0;
            match p.word() {
                Ok((s, false)) if s.parse::<i64>().is_ok() => {
                    let n = s.parse::<i64>().map_err(|_| "22003")?;
                    if n < 0 {
                        return Err("55000");
                    }
                    n as usize
                }
                _ => {
                    p.0 = saved;
                    1
                }
            }
        };
        if !p.kw("FROM") {
            p.kw("IN");
        }
        let name = p.name()?;
        p.end()?;
        Command::Fetch {
            name,
            count,
            moving,
        }
    } else if p.kw("CLOSE") {
        let name = if p.kw("ALL") { None } else { Some(p.name()?) };
        p.end()?;
        Command::Close(name)
    } else {
        return Ok(None);
    };
    Ok(Some(command))
}
// Snapshot-backed cursors cannot yet lock rows as FETCH advances.
fn reject_row_locks(prepared: &Prepared, hold: bool, diagnostic: &mut Diagnostic) -> R<()> {
    let Some(command) = prepared.row_lock_command() else { return Ok(()); };
    if hold {
        let clause = command.strip_prefix("SELECT ").unwrap_or(command);
        diagnostic.wire_message = Some(format!("DECLARE CURSOR WITH HOLD ... {clause} is not supported"));
        diagnostic.wire_detail = Some("Holdable cursors must be READ ONLY.".into());
    }
    Err("0A000")
}

impl Cursors {
    pub fn description(&self, name: &str) -> Option<&Description> {
        self.0.get(name).map(|c| &c.description)
    }
    pub(super) fn close_all(&mut self) { self.0.clear(); }

    pub fn close(&mut self, name: &str) {
        self.0.remove(name);
    }

    pub fn transaction_end(&mut self, command: &str) {
        if command != "COMMIT" && command != "ROLLBACK" {
            return;
        }
        self.0
            .retain(|_, c| c.hold && (command == "COMMIT" || !c.pending));
        for c in self.0.values_mut() {
            c.pending = false;
        }
    }
    pub fn describe(
        &self,
        command: &mut Command,
        session: &mut Session,
        oids: &[u32],
        diagnostic: &mut Diagnostic,
    ) -> R<Description> {
        if session.is_aborted() {
            return Err("25P02");
        }
        let mut d = Description {
            type_oids: vec![],
            parameters: vec![],
            columns: vec![],
            typmods: vec![],
        };
        match command {
            Command::Declare { sql, hold, prepared, .. } => {
                let (plan, desc) = prepare_declared_sql(session, sql, oids, false)?;
                let Plan::Sql(p) = plan else { unreachable!() };
                if p.command() != "SELECT" {
                    return Err("42601");
                }
                reject_row_locks(&p, *hold, diagnostic)?;
                d.parameters = desc.parameters;
                *prepared = Some(p);
            }
            Command::Fetch {
                name,
                moving: false,
                ..
            } => d = self.0.get(name).ok_or("34000")?.description.clone(),
            _ => {}
        }
        Ok(d)
    }
    #[allow(clippy::too_many_arguments)]
    pub fn execute(
        &mut self,
        command: &Command,
        session: &mut Session,
        params: &[Value],
        formats: &[u16],
        settings: &mut crate::pg_settings::SessionSettings,
        diagnostic: &mut Diagnostic,
        simple: bool,
        output: &Output,
        out: &mut Vec<u8>,
    ) -> R<()> {
        if !session.role.is_sql() {
            return Err("42501");
        }
        if session.is_aborted() {
            return Err("25P02");
        }
        match command {
            Command::Declare {
                name,
                hold,
                sql,
                prepared,
            } => {
                if !*hold && !session.in_transaction() {
                    return Err("25P01");
                }
                if self.0.contains_key(name) {
                    return Err("42P03");
                }
                if self.0.len() >= MAX_CURSORS {
                    return Err("54000");
                }
                let p = match prepared {
                    Some(p) => p.clone(),
                    None => Box::new(core(session.prepare(sql))?),
                };
                if p.command() != "SELECT" {
                    return Err("42601");
                }
                reject_row_locks(&p, *hold, diagnostic)?;
                let mut description = core(session.describe(&p))?;
                description.parameters.clear();
                let result = core(if prepared.is_some() {
                    session.open_cursor(&p, params)
                } else {
                    session.open_uncached_cursor(&p, params)
                })?;
                self.0.insert(
                    name.clone(),
                    Cursor {
                        description,
                        result,
                        at: 0,
                        hold: *hold,
                        exhausted: false,
                        pending: session.in_transaction(),
                    },
                );
                settings.queried = session.in_transaction();
                complete("DECLARE CURSOR", &empty(), out)
            }
            Command::Fetch {
                name,
                count,
                moving,
            } => {
                let c = self.0.get_mut(name).ok_or("34000")?;
                let current = c.at > 0 && !c.exhausted;
                if *count == 0 && !moving && current {
                    return Err("55000");
                }
                if !moving && simple {
                    row_description(&c.description, formats, out)?;
                }
                let mut fetched = 0usize;
                while fetched < *count && !c.result.is_exhausted() {
                    // GD-0061 R3-311: every chunk is released before it is
                    // encoded. A MOVE releases too: its rows were read and
                    // are charged as read, though none is sent.
                    let chunk = core(session.fetch_cursor_unreleased(
                        &mut c.result,
                        (*count - fetched).min(CURSOR_ROWS),
                        CURSOR_BYTES,
                    ))?;
                    let result = core(session.release_page(chunk))?.map(|rows| SqlResult {
                        rows,
                        ..SqlResult::default()
                    });
                    let n = result.get().rows.len();
                    if !moving {
                        rows(
                            &settings.current,
                            session.zone().unwrap_or(SessionZone::Fixed(0)),
                            &result,
                            &c.description,
                            formats,
                            0,
                            n,
                            out,
                        )?;
                        flush_output(output, out)?;
                    }
                    fetched += n;
                }
                if *count > fetched {
                    c.exhausted = true;
                }
                let count = if *moving && *count == 0 {
                    usize::from(current)
                } else {
                    fetched
                };
                c.at += fetched;
                complete(
                    &format!("{} {count}", if *moving { "MOVE" } else { "FETCH" }),
                    &empty(),
                    out,
                )
            }
            Command::Close(name) => {
                if let Some(name) = name {
                    self.0.remove(name).ok_or("34000")?;
                } else {
                    self.close_all();
                }
                complete("CLOSE CURSOR", &empty(), out)
            }
        }
    }
}
