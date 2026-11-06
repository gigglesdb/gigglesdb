//! Explicit protocol-v3 subset; not PostgreSQL server compatibility.
use crate::{
    Request, Role, Session, pg_catalog_wire as catalog_wire,
    transport::{IO_TIMEOUT, MAX_FRAME, MAX_RESPONSE},
};
use giggles_core::{
    CoreError,
    relational::{DataType, Description, Prepared, SqlResult, Value},
};
use std::{collections::BTreeMap, io};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    time::timeout,
};
type R<T> = Result<T, &'static str>;
const MAX_OBJECTS: usize = 32;
pub async fn ssl_request<S: AsyncRead + AsyncWrite + Unpin>(s: &mut S) -> io::Result<()> {
    timeout(IO_TIMEOUT, async {
        let mut b = [0; 8];
        s.read_exact(&mut b).await?;
        if b != [0, 0, 0, 8, 4, 210, 22, 47] {
            return Err(io::Error::other("TLS SSLRequest required"));
        }
        s.write_all(b"S").await?;
        s.flush().await
    })
    .await
    .map_err(io::Error::other)?
}
pub struct Reader<'a> {
    pub b: &'a [u8],
    pub at: usize,
}
impl<'a> Reader<'a> {
    pub fn take(&mut self, n: usize) -> R<&'a [u8]> {
        let end = self.at.checked_add(n).ok_or("08P01")?;
        let b = self.b.get(self.at..end).ok_or("08P01")?;
        self.at = end;
        Ok(b)
    }
    pub fn byte(&mut self) -> R<u8> {
        Ok(self.take(1)?[0])
    }
    pub fn u16(&mut self) -> R<usize> {
        Ok(u16::from_be_bytes(self.take(2)?.try_into().unwrap()) as usize)
    }
    pub fn u32(&mut self) -> R<u32> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }
    pub fn text(&mut self) -> R<String> {
        let n = self.b[self.at..]
            .iter()
            .position(|b| *b == 0)
            .ok_or("08P01")?;
        let b = self.take(n + 1)?;
        String::from_utf8(b[..n].to_vec()).map_err(|_| "22021")
    }
    pub fn end(&self) -> R<()> {
        if self.at == self.b.len() {
            Ok(())
        } else {
            Err("08P01")
        }
    }
}
pub fn frame(tag: u8, b: &[u8], out: &mut Vec<u8>) -> R<()> {
    let len = b.len().checked_add(4).ok_or("54000")?;
    let total = out
        .len()
        .checked_add(len)
        .and_then(|n| n.checked_add(1))
        .ok_or("54000")?;
    if total > MAX_RESPONSE {
        return Err("54000");
    }
    out.push(tag);
    out.extend(u32::try_from(len).map_err(|_| "54000")?.to_be_bytes());
    out.extend(b);
    Ok(())
}
pub fn z(s: &str, b: &mut Vec<u8>) {
    b.extend(s.as_bytes());
    b.push(0);
}
fn ready(session: &Session, out: &mut Vec<u8>) -> R<()> {
    frame(
        b'Z',
        &[if session.is_aborted() {
            b'E'
        } else if session.in_transaction() {
            b'T'
        } else {
            b'I'
        }],
        out,
    )
}
pub fn error(code: &str, out: &mut Vec<u8>) {
    error_message(code, "request rejected; consult documented code", out);
}
fn error_message(code: &str, message: &'static str, out: &mut Vec<u8>) {
    diagnostic_error(code, message, None, out);
}
#[derive(Default)]
struct Diagnostic {
    phase: &'static str,
    query_id: Option<String>,
}
impl Diagnostic {
    fn query(&mut self, sql: &str) {
        use sha2::{Digest, Sha256};
        self.query_id = Some(hex::encode(Sha256::digest(sql.as_bytes())));
    }
}
fn diagnostic_error(
    code: &str,
    message: &'static str,
    diagnostic: Option<&Diagnostic>,
    out: &mut Vec<u8>,
) {
    // Never log request text or values. Messages are fixed server diagnostics.
    let detail = diagnostic.map(|d| {
        format!(
            "phase={}; query_id={}",
            d.phase,
            d.query_id.as_deref().unwrap_or("none")
        )
    });
    if let Some(detail) = &detail {
        eprintln!("protocol=postgres sqlstate={code} {detail} reason={message}");
    } else {
        eprintln!("protocol=postgres sqlstate={code} reason={message}");
    }
    out.clear();
    let mut b = vec![b'S'];
    z("ERROR", &mut b);
    b.push(b'C');
    z(code, &mut b);
    b.push(b'M');
    z(message, &mut b);
    if let Some(detail) = detail {
        b.push(b'D');
        z(&detail, &mut b);
    }
    b.push(0);
    let _ = frame(b'E', &b, out);
}
pub fn oid(t: &DataType) -> u32 {
    match t {
        DataType::Boolean => 16,
        DataType::SmallInt => 21,
        DataType::Integer => 23,
        DataType::BigInt => 20,
        DataType::Text => 25,
        DataType::Bytes => 17,
    }
}
fn size(t: &DataType) -> i16 {
    match t {
        DataType::Boolean => 1,
        DataType::SmallInt => 2,
        DataType::Integer => 4,
        DataType::BigInt => 8,
        _ => -1,
    }
}
pub fn format_at(formats: &[u16], i: usize) -> u16 {
    if formats.len() == 1 {
        formats[0]
    } else {
        *formats.get(i).unwrap_or(&0)
    }
}
pub fn formats(r: &mut Reader<'_>, count: usize) -> R<Vec<u16>> {
    let n = r.u16()?;
    if n != 0 && n != 1 && n != count {
        return Err("08P01");
    }
    let mut v = Vec::with_capacity(n);
    for _ in 0..n {
        let f = r.u16()?;
        if f > 1 {
            return Err("0A000");
        }
        v.push(f as u16);
    }
    Ok(v)
}
pub fn row_description(d: &Description, formats: &[u16], out: &mut Vec<u8>) -> R<()> {
    if d.columns.is_empty() {
        return frame(b'n', &[], out);
    }
    let mut b = vec![];
    b.extend((d.columns.len() as u16).to_be_bytes());
    for (i, c) in d.columns.iter().enumerate() {
        z(&c.name, &mut b);
        b.extend(0u32.to_be_bytes());
        b.extend(0i16.to_be_bytes());
        b.extend(oid(&c.data_type).to_be_bytes());
        b.extend(size(&c.data_type).to_be_bytes());
        b.extend((-1i32).to_be_bytes());
        b.extend(format_at(formats, i).to_be_bytes());
    }
    frame(b'T', &b, out)
}
pub fn decode(b: &[u8], ty: &DataType, format: u16) -> R<Value> {
    if format == 0 {
        let s = std::str::from_utf8(b).map_err(|_| "22021")?;
        return match ty {
            DataType::Boolean => {
                let s = s.trim_ascii();
                let prefix = |word: &str| {
                    !s.is_empty()
                        && word
                            .get(..s.len())
                            .is_some_and(|p| p.eq_ignore_ascii_case(s))
                };
                let yes = s == "1" || ["true", "yes", "on"].into_iter().any(prefix);
                let no = s == "0" || ["false", "no", "off"].into_iter().any(prefix);
                match (yes, no) {
                    (true, false) => Ok(Value::Bool(true)),
                    (false, true) => Ok(Value::Bool(false)),
                    _ => Err("22P02"),
                }
            }
            DataType::SmallInt => s
                .parse::<i16>()
                .map(|v| Value::Int(v.into()))
                .map_err(|_| "22P02"),
            DataType::Integer => s
                .parse::<i32>()
                .map(|v| Value::Int(v.into()))
                .map_err(|_| "22P02"),
            DataType::BigInt => s.parse::<i64>().map(Value::Int).map_err(|_| "22P02"),
            DataType::Text => Ok(Value::Text(s.into())),
            DataType::Bytes => hex::decode(s.strip_prefix("\\x").ok_or("22P02")?)
                .map(Value::Bytes)
                .map_err(|_| "22P02"),
        };
    }
    match (ty, b.len()) {
        (DataType::Boolean, 1) if b[0] <= 1 => Ok(Value::Bool(b[0] != 0)),
        (DataType::SmallInt, 2) => Ok(Value::Int(i16::from_be_bytes(b.try_into().unwrap()).into())),
        (DataType::Integer, 4) => Ok(Value::Int(i32::from_be_bytes(b.try_into().unwrap()).into())),
        (DataType::BigInt, 8) => Ok(Value::Int(i64::from_be_bytes(b.try_into().unwrap()))),
        (DataType::Text, _) => String::from_utf8(b.to_vec())
            .map(Value::Text)
            .map_err(|_| "22021"),
        (DataType::Bytes, _) => Ok(Value::Bytes(b.to_vec())),
        _ => Err("22P03"),
    }
}
fn encode(v: &Value, ty: &DataType, format: u16) -> R<Option<Vec<u8>>> {
    if *v == Value::Null {
        return Ok(None);
    }
    if format == 0 {
        return Ok(Some(match v {
            Value::Null => unreachable!(),
            Value::Bool(v) => {
                if *v {
                    b"t".to_vec()
                } else {
                    b"f".to_vec()
                }
            }
            Value::Int(v) => v.to_string().into_bytes(),
            Value::Text(s) => s.as_bytes().to_vec(),
            Value::Bytes(b) => format!("\\x{}", hex::encode(b)).into_bytes(),
        }));
    }
    Ok(Some(match (v, ty) {
        (Value::Bool(v), DataType::Boolean) => vec![u8::from(*v)],
        (Value::Int(v), DataType::SmallInt) => i16::try_from(*v)
            .map_err(|_| "22003")?
            .to_be_bytes()
            .to_vec(),
        (Value::Int(v), DataType::Integer) => i32::try_from(*v)
            .map_err(|_| "22003")?
            .to_be_bytes()
            .to_vec(),
        (Value::Int(v), DataType::BigInt) => v.to_be_bytes().to_vec(),
        (Value::Text(s), DataType::Text) => s.as_bytes().to_vec(),
        (Value::Bytes(b), DataType::Bytes) => b.clone(),
        _ => return Err("42804"),
    }))
}
fn rows(
    result: &SqlResult,
    d: &Description,
    formats: &[u16],
    start: usize,
    end: usize,
    out: &mut Vec<u8>,
) -> R<()> {
    for row in &result.rows[start..end] {
        let mut b = vec![];
        b.extend((row.values.len() as u16).to_be_bytes());
        for (i, v) in row.values.iter().enumerate() {
            match encode(
                v,
                &d.columns.get(i).ok_or("XX000")?.data_type,
                format_at(formats, i),
            )? {
                None => b.extend((-1i32).to_be_bytes()),
                Some(v) => {
                    b.extend((v.len() as i32).to_be_bytes());
                    b.extend(v);
                }
            }
        }
        frame(b'D', &b, out)?;
    }
    Ok(())
}
fn complete(command: &str, r: &SqlResult, out: &mut Vec<u8>) -> R<()> {
    let tag = match command {
        "SELECT" => format!("SELECT {}", r.rows.len()),
        "INSERT" => format!("INSERT 0 {}", r.affected),
        "UPDATE" | "DELETE" => format!("{command} {}", r.affected),
        _ => command.into(),
    };
    let mut b = vec![];
    z(&tag, &mut b);
    frame(b'C', &b, out)
}
fn core<T>(r: Result<T, CoreError>) -> R<T> {
    r.map_err(|e| crate::service::sqlstate(&e))
}
fn catalog_plan(sql: &str, diagnostic: &mut Diagnostic) -> R<Option<giggles_pg_catalog::Plan>> {
    diagnostic.phase = "catalog planning";
    match giggles_pg_catalog::plan(sql) {
        // Preserve the finite application grammar's own diagnostics (for
        // example COPY rejection). Core still parses and authorizes the entire
        // original statement; no failed catalog plan is executed as metadata.
        Err(giggles_pg_catalog::Error::Syntax) => Ok(None),
        other => other.map_err(catalog_wire::error),
    }
}
#[derive(Clone)]
enum Plan {
    Empty,
    Control(&'static str),
    Settings(Vec<crate::pg_settings::Setting>),
    Inspect(crate::pg_settings::Show),
    Catalog(
        Box<giggles_pg_catalog::Plan>,
        Vec<giggles_pg_catalog::Field>,
        u64,
    ),
    Sql(Box<Prepared>),
    Task(crate::task_statement::TaskStatement),
}
#[derive(Clone)]
struct Statement {
    p: Plan,
    d: Description,
    parameter_oids: Vec<u32>,
    query_id: Option<String>,
}
impl Statement {
    fn describe(&self, session: &mut Session) -> R<Description> {
        match &self.p {
            Plan::Catalog(_, _, epoch) => {
                let (current, _) = core(session.catalog_snapshot())?;
                if current != *epoch {
                    return Err("0A000");
                }
                Ok(self.d.clone())
            }
            Plan::Sql(p) => core(session.sql.describe(p)),
            Plan::Task(_)
            | Plan::Empty
            | Plan::Settings(_)
            | Plan::Control(_)
            | Plan::Inspect(_) => Ok(self.d.clone()),
        }
    }
}
struct Portal {
    s: Statement,
    params: Vec<Value>,
    formats: Vec<u16>,
    result: Option<SqlResult>,
    catalog_params: Vec<giggles_pg_catalog::Value>,
    catalog_result: Option<giggles_pg_catalog::ResultSet>,
    parameter_bytes: usize,
    at: usize,
}
impl Portal {
    fn retained_bytes(&self) -> R<usize> {
        let sql = self
            .result
            .as_ref()
            .map(|r| serde_json::to_vec(r).map(|b| b.len()))
            .transpose()
            .map_err(|_| "54000")?
            .unwrap_or(0);
        let catalog = self
            .catalog_result
            .as_ref()
            .map(catalog_wire::retained_bytes)
            .transpose()?
            .unwrap_or(0);
        self.parameter_bytes
            .checked_add(sql)
            .and_then(|n| n.checked_add(catalog))
            .ok_or("54000")
    }
}
fn catalog_complete(count: usize, out: &mut Vec<u8>) -> R<()> {
    let mut b = Vec::new();
    z(&format!("SELECT {count}"), &mut b);
    frame(b'C', &b, out)
}
struct State {
    diagnostic: Diagnostic,
    implicit: bool,
    settings: crate::pg_settings::SessionSettings,
    statements: BTreeMap<String, Statement>,
    portals: BTreeMap<String, Portal>,
    drain: bool,
}
fn begin_implicit(
    session: &mut Session,
    implicit: &mut bool,
    settings: &mut crate::pg_settings::SessionSettings,
) -> R<()> {
    if !session.in_transaction() {
        core(session.execute_sql("BEGIN", &[]))?;
        settings.transaction(false, true, "BEGIN");
        *implicit = true;
    }
    Ok(())
}
fn control(sql: &str) -> Option<&'static str> {
    let sql = sql.trim().strip_suffix(';').unwrap_or(sql.trim()).trim();
    ["BEGIN", "COMMIT", "ROLLBACK"]
        .into_iter()
        .find(|command| sql.eq_ignore_ascii_case(command))
}
fn inspect(
    show: crate::pg_settings::Show,
    settings: &crate::pg_settings::Settings,
    session: &Session,
) -> R<(Description, SqlResult)> {
    if !matches!(session.role, Role::Application { .. }) {
        return Err("42501");
    }
    if session.is_aborted() {
        return Err("25P02");
    }
    let (name, value) = show.value(settings);
    Ok((
        Description {
            parameters: vec![],
            columns: vec![giggles_core::relational::ColumnDef {
                id: 0,
                name: name.into(),
                data_type: DataType::Text,
                not_null: true,
            }],
        },
        SqlResult {
            columns: vec![name.into()],
            rows: vec![giggles_core::relational::SqlRow {
                values: vec![Value::Text(value)],
                version: 0,
            }],
            affected: 0,
        },
    ))
}
fn execute_control(
    command: &'static str,
    session: &mut Session,
    implicit: &mut bool,
    settings: &mut crate::pg_settings::SessionSettings,
) -> R<&'static str> {
    if !matches!(session.role, Role::Application { .. }) {
        return Err("42501");
    }
    let was = session.in_transaction();
    let actual = if command == "COMMIT" && session.is_aborted() {
        "ROLLBACK"
    } else {
        command
    };
    if actual == "BEGIN" {
        if session.is_aborted() {
            return Err("25P02");
        }
        if !was {
            core(session.execute_sql("BEGIN", &[]))?;
        }
    } else if was && let Err(error) = session.execute_sql(actual, &[]) {
        session.sql.rollback();
        settings.transaction(true, false, "ROLLBACK");
        *implicit = false;
        return Err(crate::service::sqlstate(&error));
    }
    *implicit = false;
    settings.transaction(was, session.in_transaction(), actual);
    Ok(actual)
}
impl State {
    fn handle(
        &mut self,
        tag: u8,
        b: &[u8],
        session: &mut Session,
        out: &mut Vec<u8>,
        received: std::time::Instant,
    ) -> R<bool> {
        let mut r = Reader { b, at: 0 };
        if self.drain && tag != b'S' && tag != b'X' {
            return Ok(true);
        }
        if self.implicit && tag == b'Q' {
            return Err("08P01");
        }
        match tag {
            b'X' => {
                r.end()?;
                return Ok(false);
            }
            b'S' => {
                r.end()?;
                self.drain = false;
                if self.implicit {
                    self.implicit = false;
                    if session.is_aborted() {
                        session.sql.rollback();
                        self.settings.transaction(true, false, "ROLLBACK");
                    } else {
                        let commit = session.execute_sql("COMMIT", &[]);
                        if let Err(error) = commit {
                            session.sql.rollback();
                            self.settings.transaction(true, false, "ROLLBACK");
                            self.portals.clear();
                            return Err(crate::service::sqlstate(&error));
                        }
                        self.settings.transaction(true, false, "COMMIT");
                    }
                }
                if !session.in_transaction() {
                    self.portals.clear();
                }
                ready(session, out)?;
            }
            b'H' => {
                r.end()?;
            }
            b'Q' => {
                let sql = r.text()?;
                self.diagnostic.query(&sql);
                r.end()?;
                self.statements.remove("");
                self.portals.remove("");
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
                    rows(&result, &d, &[], 0, 1, out)?;
                    complete("SELECT", &result, out)?;
                } else if crate::pg_settings::empty_sql(&sql)? {
                    frame(b'I', &[], out)?;
                } else if let Some(command) = control(&sql) {
                    let actual =
                        execute_control(command, session, &mut self.implicit, &mut self.settings)?;
                    complete(actual, &SqlResult::default(), out)?;
                } else if let Some(show) = crate::pg_settings::show(&sql)? {
                    let (description, result) = inspect(show, &self.settings.current, session)?;
                    row_description(&description, &[], out)?;
                    rows(&result, &description, &[], 0, 1, out)?;
                    complete("SHOW", &result, out)?;
                } else if let Some(settings) = crate::pg_settings::parse(&sql)? {
                    if !matches!(session.role, Role::Application { .. }) {
                        return Err("42501");
                    }
                    if session.is_aborted() {
                        return Err("25P02");
                    }
                    for setting in &settings {
                        self.settings.current.apply(setting);
                        frame(b'C', b"SET\0", out)?;
                    }
                } else if let Some(plan) = catalog_plan(&sql, &mut self.diagnostic)? {
                    self.diagnostic.phase = "catalog execution";
                    let (_, tables) = core(session.catalog_snapshot())?;
                    let result = catalog_wire::execute(&plan, &catalog_wire::snapshot(tables), &[])
                        .map_err(catalog_wire::error)?;
                    catalog_wire::retained_bytes(&result)?;
                    catalog_wire::description(&result.fields, &[], out)?;
                    catalog_wire::rows(&result, &[], 0, result.rows.len(), out)?;
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
                        let result = core(session.execute_sql(&sql, &[]))?;
                        self.settings.transaction(
                            was_transaction,
                            session.in_transaction(),
                            "ROLLBACK",
                        );
                        complete("ROLLBACK", &result, out)?;
                    } else {
                        let p = core(session.prepare(&sql))?;
                        let d = core(session.sql.describe(&p))?;
                        let result = core(session.execute_prepared(&p, &[]))?;
                        self.settings.transaction(
                            was_transaction,
                            session.in_transaction(),
                            p.command(),
                        );
                        if !d.columns.is_empty() {
                            row_description(&d, &[], out)?;
                            rows(&result, &d, &[], 0, result.rows.len(), out)?;
                        }
                        complete(p.command(), &result, out)?;
                    }
                }
                if !session.in_transaction() {
                    self.portals.clear();
                }
                ready(session, out)?;
            }
            b'P' => {
                let name = r.text()?;
                let sql = r.text()?;
                self.diagnostic.query(&sql);
                let n = r.u16()?;
                if n > 256 {
                    return Err("54000");
                }
                let mut oids = vec![];
                for _ in 0..n {
                    oids.push(r.u32()?);
                }
                r.end()?;
                if name.len() > 63 {
                    return Err("54000");
                }
                if !name.is_empty() && self.statements.contains_key(&name) {
                    return Err("42P05");
                }
                if !self.statements.contains_key(&name) && self.statements.len() >= MAX_OBJECTS {
                    return Err("54000");
                }
                let (p, mut d) = if crate::pg_settings::empty_sql(&sql)? {
                    (
                        Plan::Empty,
                        Description {
                            columns: vec![],
                            parameters: vec![],
                        },
                    )
                } else if let Some(command) = control(&sql) {
                    if !matches!(session.role, Role::Application { .. }) {
                        return Err("42501");
                    }
                    (
                        Plan::Control(command),
                        Description {
                            columns: vec![],
                            parameters: vec![],
                        },
                    )
                } else if let Some(show) = crate::pg_settings::show(&sql)? {
                    let (description, _) = inspect(show, &self.settings.current, session)?;
                    (Plan::Inspect(show), description)
                } else if let Some(settings) = crate::pg_settings::parse(&sql)? {
                    if !matches!(session.role, Role::Application { .. }) {
                        return Err("42501");
                    }
                    if settings.len() != 1 {
                        return Err("42601");
                    }
                    (
                        Plan::Settings(settings),
                        Description {
                            columns: vec![],
                            parameters: vec![],
                        },
                    )
                } else if sql
                    .trim_start_matches(|c: char| c.is_ascii_whitespace())
                    .starts_with("GIGGLES")
                {
                    if !matches!(session.role, Role::Task { .. }) {
                        return Err("42501");
                    }
                    let task = crate::task_statement::TaskStatement::parse(&sql)?;
                    (Plan::Task(task), task.description())
                } else if let Some(plan) = catalog_plan(&sql, &mut self.diagnostic)? {
                    let (epoch, _) = core(session.catalog_snapshot())?;
                    let fields =
                        giggles_pg_catalog::describe(&plan).map_err(catalog_wire::error)?;
                    let d = Description {
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
                    (Plan::Catalog(Box::new(plan), fields, epoch), d)
                } else {
                    self.diagnostic.phase = "application prepare";
                    let p = core(session.prepare(&sql))?;
                    let d = core(session.sql.describe(&p))?;
                    (Plan::Sql(Box::new(p)), d)
                };
                if n > d.parameters.len() {
                    return Err("08P01");
                }
                let mut parameter_oids: Vec<u32> = match &p {
                    Plan::Catalog(plan, _, _) => {
                        plan.parameter_types().iter().map(|t| t.oid()).collect()
                    }
                    _ => d.parameters.iter().map(oid).collect(),
                };
                self.diagnostic.phase = "parameter type declaration";
                for (i, (o, t)) in oids.into_iter().zip(&mut d.parameters).enumerate() {
                    if matches!(p, Plan::Catalog(..)) {
                        if o != 0 {
                            catalog_wire::parameter_type(o)?;
                            // The catalog compiler fixes parameter and result
                            // types together. A wire override must not silently
                            // change only ParameterDescription while leaving a
                            // projected parameter's output type unchanged.
                            if o != parameter_oids[i] {
                                let safe_numeric = matches!(&p, Plan::Catalog(plan, _, _)
                                    if plan.allows_numeric_input_override())
                                    && matches!(o, 20 | 21 | 23 | 26)
                                    && matches!(parameter_oids[i], 20 | 21 | 23 | 26);
                                if !safe_numeric {
                                    return Err("0A000");
                                }
                            }
                            parameter_oids[i] = o;
                        }
                        continue;
                    }
                    if o != 0 && o != oid(t) {
                        // Application integer parameters share Value::Int, but
                        // decode using the client's declared wire width. Core
                        // still enforces the destination range at execution.
                        if !matches!(p, Plan::Sql(_)) {
                            return Err("0A000");
                        }
                        *t = match (&*t, o) {
                            (DataType::Text, 1043) => DataType::Text,
                            (DataType::SmallInt | DataType::Integer | DataType::BigInt, 21) => {
                                DataType::SmallInt
                            }
                            (DataType::SmallInt | DataType::Integer | DataType::BigInt, 23) => {
                                DataType::Integer
                            }
                            (DataType::SmallInt | DataType::Integer | DataType::BigInt, 20) => {
                                DataType::BigInt
                            }
                            _ => return Err("0A000"),
                        };
                    }
                    if o != 0 {
                        parameter_oids[i] = o;
                    }
                }
                self.statements.insert(
                    name,
                    Statement {
                        p,
                        d,
                        parameter_oids,
                        query_id: self.diagnostic.query_id.clone(),
                    },
                );
                frame(b'1', &[], out)?;
            }
            b'B' => {
                let name = r.text()?;
                let stmt = r.text()?;
                let s = self.statements.get(&stmt).ok_or("26000")?.clone();
                self.diagnostic.query_id = s.query_id.clone();
                s.describe(session)?;
                let fmts = formats(&mut r, s.d.parameters.len())?;
                let n = r.u16()?;
                if n != s.d.parameters.len() {
                    return Err("08P01");
                }
                let mut params = vec![];
                let mut catalog_params = vec![];
                let mut parameter_bytes = 0usize;
                for (i, ty) in s.d.parameters.iter().enumerate() {
                    let n = r.u32()?;
                    if matches!(s.p, Plan::Catalog(..)) {
                        let value = if n == u32::MAX {
                            giggles_pg_catalog::Value::Null
                        } else {
                            catalog_wire::decode(
                                r.take(usize::try_from(n).map_err(|_| "54000")?)?,
                                catalog_wire::parameter_type(s.parameter_oids[i])?,
                                format_at(&fmts, i),
                            )?
                        };
                        // Count decoded allocation, including array element overhead.
                        parameter_bytes = parameter_bytes
                            .checked_add(catalog_wire::value_bytes(&value)?)
                            .ok_or("54000")?;
                        catalog_params.push(value);
                        continue;
                    }
                    params.push(if n == u32::MAX {
                        Value::Null
                    } else {
                        decode(
                            r.take(usize::try_from(n).map_err(|_| "54000")?)?,
                            ty,
                            format_at(&fmts, i),
                        )?
                    });
                }
                if let Plan::Task(task) = s.p {
                    task.bind(&params)?;
                }
                let output = formats(&mut r, s.d.columns.len())?;
                r.end()?;
                if name.len() > 63 {
                    return Err("54000");
                }
                if !name.is_empty() && self.portals.contains_key(&name) {
                    return Err("42P03");
                }
                if !self.portals.contains_key(&name) && self.portals.len() >= MAX_OBJECTS {
                    return Err("54000");
                }
                parameter_bytes = parameter_bytes
                    .checked_add(serde_json::to_vec(&params).map_err(|_| "54000")?.len())
                    .ok_or("54000")?;
                let bound = self.portals.values().try_fold(0usize, |sum, p| {
                    sum.checked_add(p.retained_bytes()?).ok_or("54000")
                })?;
                if bound.checked_add(parameter_bytes).ok_or("54000")? > MAX_RESPONSE {
                    return Err("54000");
                }
                self.portals.insert(
                    name,
                    Portal {
                        s,
                        params,
                        formats: output,
                        result: None,
                        catalog_params,
                        catalog_result: None,
                        parameter_bytes,
                        at: 0,
                    },
                );
                frame(b'2', &[], out)?;
            }
            b'D' => {
                let kind = r.byte()?;
                let name = r.text()?;
                r.end()?;
                match kind {
                    b'S' => {
                        let s = self.statements.get(&name).ok_or("26000")?;
                        self.diagnostic.query_id = s.query_id.clone();
                        let d = s.describe(session)?;
                        let mut b = vec![];
                        b.extend((d.parameters.len() as u16).to_be_bytes());
                        for oid in &s.parameter_oids {
                            b.extend(oid.to_be_bytes());
                        }
                        frame(b't', &b, out)?;
                        if let Plan::Catalog(_, fields, _) = &s.p {
                            catalog_wire::description(fields, &[], out)?;
                        } else {
                            row_description(&d, &[], out)?;
                        }
                    }
                    b'P' => {
                        let p = self.portals.get(&name).ok_or("34000")?;
                        self.diagnostic.query_id = p.s.query_id.clone();
                        let d = p.s.describe(session)?;
                        if let Plan::Catalog(_, fields, _) = &p.s.p {
                            catalog_wire::description(fields, &p.formats, out)?;
                        } else {
                            row_description(&d, &p.formats, out)?;
                        }
                    }
                    _ => return Err("08P01"),
                }
            }
            b'E' => {
                let retained = self.portals.values().try_fold(0usize, |sum, p| {
                    sum.checked_add(p.retained_bytes()?).ok_or("54000")
                })?;
                let was_transaction = session.in_transaction();
                let name = r.text()?;
                let max = r.u32()?;
                r.end()?;
                if max > i32::MAX as u32 {
                    return Err("08P01");
                }
                let p = self.portals.get_mut(&name).ok_or("34000")?;
                self.diagnostic.query_id = p.s.query_id.clone();
                p.s.describe(session)?;
                if let Plan::Catalog(plan, fields, epoch) = &p.s.p {
                    self.diagnostic.phase = "catalog execution";
                    begin_implicit(session, &mut self.implicit, &mut self.settings)?;
                    let (current, tables) = core(session.catalog_snapshot())?;
                    if current != *epoch {
                        return Err("0A000");
                    }
                    if p.catalog_result.is_none() {
                        let result = catalog_wire::execute(
                            plan,
                            &catalog_wire::snapshot(tables),
                            &p.catalog_params,
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
                        p.catalog_result = Some(result);
                    }
                    let result = p.catalog_result.as_ref().unwrap();
                    let end = if max == 0 {
                        result.rows.len()
                    } else {
                        p.at.saturating_add(max as usize).min(result.rows.len())
                    };
                    catalog_wire::rows(result, &p.formats, p.at, end, out)?;
                    p.at = end;
                    if end < result.rows.len() {
                        frame(b's', &[], out)?;
                    } else {
                        catalog_complete(result.rows.len(), out)?;
                    }
                    return Ok(true);
                }
                if matches!(p.s.p, Plan::Empty) {
                    frame(b'I', &[], out)?;
                    return Ok(true);
                }
                if let Plan::Control(command) = p.s.p {
                    if p.result.is_some() {
                        return Err("55000");
                    }
                    let actual =
                        execute_control(command, session, &mut self.implicit, &mut self.settings)?;
                    p.result = Some(SqlResult::default());
                    complete(actual, &SqlResult::default(), out)?;
                    if !session.in_transaction() {
                        self.portals.clear();
                    }
                    return Ok(true);
                }
                if let Plan::Settings(settings) = &p.s.p {
                    if !matches!(session.role, Role::Application { .. }) {
                        return Err("42501");
                    }
                    if session.is_aborted() {
                        return Err("25P02");
                    }
                    if p.result.is_some() {
                        return Err("55000");
                    }
                    begin_implicit(session, &mut self.implicit, &mut self.settings)?;
                    for setting in settings {
                        self.settings.current.apply(setting);
                    }
                    p.result = Some(SqlResult::default());
                    frame(b'C', b"SET\0", out)?;
                    return Ok(true);
                }
                // Task results are never retained in a portal. Each Execute is a
                // fresh native invocation, including completed-portal replays.
                if let Plan::Inspect(show) = p.s.p {
                    // Recheck role/transaction even when releasing cached rows.
                    let (_, current) = inspect(show, &self.settings.current, session)?;
                    if p.result.is_none() {
                        let bytes = serde_json::to_vec(&current).map_err(|_| "54000")?.len();
                        if retained.checked_add(bytes).ok_or("54000")? > MAX_RESPONSE {
                            return Err("54000");
                        }
                        p.result = Some(current);
                    }
                    let result = p.result.as_ref().unwrap();
                    rows(result, &p.s.d, &p.formats, p.at, result.rows.len(), out)?;
                    p.at = result.rows.len();
                    complete("SHOW", result, out)?;
                    return Ok(true);
                }
                if let Plan::Task(task) = p.s.p {
                    let request = task.bind(&p.params)?;
                    let response = session.handle_prepared_task(request, received);
                    let value = serde_json::to_string(&response).map_err(|_| "XX000")?;
                    let result = SqlResult {
                        columns: vec!["decision".into()],
                        rows: vec![giggles_core::relational::SqlRow {
                            values: vec![Value::Text(value)],
                            version: 0,
                        }],
                        affected: 0,
                    };
                    rows(&result, &p.s.d, &p.formats, 0, 1, out)?;
                    complete("SELECT", &result, out)?;
                    return Ok(true);
                }
                let Plan::Sql(prepared) = &p.s.p else {
                    unreachable!()
                };
                if p.result.is_some() && p.s.d.columns.is_empty() {
                    return Err("55000");
                }
                if p.result.is_none() {
                    let command = prepared.command();
                    if !matches!(command, "BEGIN" | "COMMIT" | "ROLLBACK") {
                        begin_implicit(session, &mut self.implicit, &mut self.settings)?;
                    }
                    let result = if command == "BEGIN" && self.implicit {
                        self.implicit = false;
                        SqlResult::default()
                    } else {
                        core(session.execute_prepared(prepared, &p.params))?
                    };
                    if !session.in_transaction() {
                        self.implicit = false;
                    }
                    self.settings.transaction(
                        was_transaction,
                        session.in_transaction(),
                        prepared.command(),
                    );
                    let bytes = serde_json::to_vec(&result).map_err(|_| "54000")?.len();
                    if retained.checked_add(bytes).ok_or("54000")? > MAX_RESPONSE {
                        return Err("54000");
                    }
                    // Admit retained bytes before caching OR appending any rows.
                    p.result = Some(result);
                }
                let result = p.result.as_ref().unwrap();
                let end = if max == 0 {
                    result.rows.len()
                } else {
                    p.at.saturating_add(max as usize).min(result.rows.len())
                };
                rows(result, &p.s.d, &p.formats, p.at, end, out)?;
                p.at = end;
                if end < result.rows.len() {
                    frame(b's', &[], out)?;
                } else {
                    complete(prepared.command(), result, out)?;
                }
                if was_transaction && !session.in_transaction() {
                    self.portals.clear();
                }
            }
            b'C' => {
                let kind = r.byte()?;
                let name = r.text()?;
                r.end()?;
                match kind {
                    b'S' => {
                        self.statements.remove(&name);
                    }
                    b'P' => {
                        self.portals.remove(&name);
                    }
                    _ => return Err("08P01"),
                };
                frame(b'3', &[], out)?;
            }
            _ => return Err("0A000"),
        }
        Ok(true)
    }
}
async fn send<S: AsyncWrite + Unpin>(s: &mut S, b: &[u8]) -> io::Result<()> {
    timeout(IO_TIMEOUT, async {
        s.write_all(b).await?;
        s.flush().await
    })
    .await
    .map_err(io::Error::other)?
}
pub async fn run<S: AsyncRead + AsyncWrite + Unpin>(
    s: &mut S,
    mut session: Session,
) -> io::Result<()> {
    let blocking = session.service.blocking.clone();
    if !session.service.admission.request() {
        return Ok(());
    }
    let startup = timeout(IO_TIMEOUT, async {
        let n = s.read_u32().await?;
        if !(8..=8192).contains(&n) {
            return Err(io::Error::other("startup limit"));
        }
        let mut b = vec![0; n as usize - 4];
        s.read_exact(&mut b).await?;
        Ok(b)
    })
    .await
    .map_err(io::Error::other)??;
    // SSLRequest and mandatory mTLS already completed, including role binding.
    if startup.len() == 12 && startup[..4] == 80877102u32.to_be_bytes() {
        let pid = u32::from_be_bytes(startup[4..8].try_into().unwrap());
        let secret = u32::from_be_bytes(startup[8..12].try_into().unwrap());
        session.service.cancel_bound(&session, pid, secret);
        return Ok(());
    }
    let mut reason = "malformed PostgreSQL startup packet";
    let mut timezone = String::from("UTC");
    let mut startup_settings = crate::pg_settings::Settings::default();
    let validate = (|| -> R<()> {
        let mut r = Reader { b: &startup, at: 0 };
        if r.u32()? != 196608 {
            reason = "only PostgreSQL protocol version 3 is supported";
            return Err("0A000");
        }
        let mut fields = BTreeMap::new();
        loop {
            let k = r.text()?;
            if k.is_empty() {
                break;
            }
            let v = r.text()?;
            if fields.insert(k, v).is_some() {
                return Err("08P01");
            }
        }
        r.end()?;
        if fields.get("user").map(String::as_str) != Some("giggles") {
            reason = "startup user must be giggles; authentication uses the client certificate";
            return Err("28000");
        }
        if fields.get("database").map(String::as_str) != Some("giggles") {
            reason = "startup database must be giggles";
            return Err("28000");
        }
        if fields.keys().any(|k| {
            !matches!(
                k.as_str(),
                "user"
                    | "database"
                    | "client_encoding"
                    | "application_name"
                    | "DateStyle"
                    | "TimeZone"
                    | "extra_float_digits"
            )
        }) {
            reason = "unsupported startup parameter; allowed: user, database, client_encoding, application_name, DateStyle, TimeZone, extra_float_digits";
            return Err("0A000");
        }
        if fields
            .get("client_encoding")
            .is_some_and(|v| !v.eq_ignore_ascii_case("UTF8"))
        {
            reason = "startup client_encoding must be UTF8";
            return Err("0A000");
        }
        // Driver presentation hints have no effect on the current integer/text/
        // boolean/bytes type set. They never change clock or task authority.
        if fields
            .get("DateStyle")
            .is_some_and(|v| !v.eq_ignore_ascii_case("ISO"))
        {
            reason = "startup DateStyle must be ISO";
            return Err("0A000");
        }
        if let Some(value) = fields.get("TimeZone") {
            if value.is_empty()
                || value.len() > 128
                || !value
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"/_+-:.".contains(&b))
            {
                reason = "startup TimeZone must be a bounded timezone label";
                return Err("0A000");
            }
            timezone = value.clone();
        }
        if fields
            .get("extra_float_digits")
            .is_some_and(|v| v.parse::<i32>().map_or(true, |n| !(-15..=3).contains(&n)))
        {
            reason = "startup extra_float_digits must be an integer from -15 to 3";
            return Err("0A000");
        }
        if let Some(name) = fields.get("application_name") {
            if name.len() > 128 {
                reason = "startup application_name exceeds 128 bytes";
                return Err("54000");
            }
            startup_settings.application_name = name.clone();
        }
        if let Some(digits) = fields.get("extra_float_digits") {
            startup_settings.extra_float_digits = digits.parse().map_err(|_| "22023")?;
        }
        Ok(())
    })();
    let mut out = vec![];
    if let Err(code) = validate {
        error_message(code, reason, &mut out);
        send(s, &out).await?;
        return Ok(());
    }
    let backend = session
        .service
        .cancellations
        .register(&session.fingerprint, session.epoch)?;
    struct Close(std::sync::Arc<crate::cancel::Backend>);
    impl Drop for Close {
        fn drop(&mut self) {
            self.0.close();
        }
    }
    let _close = Close(backend.clone());
    frame(b'R', &0u32.to_be_bytes(), &mut out).map_err(io::Error::other)?;
    let key = [backend.pid.to_be_bytes(), backend.secret.to_be_bytes()].concat();
    frame(b'K', &key, &mut out).map_err(io::Error::other)?;
    for (key, value) in [
        ("server_version", "14.0"),
        ("client_encoding", "UTF8"),
        ("server_encoding", "UTF8"),
        ("standard_conforming_strings", "on"),
        ("integer_datetimes", "on"),
        ("DateStyle", "ISO"),
        ("TimeZone", timezone.as_str()),
    ] {
        let mut b = vec![];
        z(key, &mut b);
        z(value, &mut b);
        frame(b'S', &b, &mut out).map_err(io::Error::other)?;
    }
    ready(&session, &mut out).map_err(io::Error::other)?;
    send(s, &out).await?;
    let mut state = State {
        diagnostic: Diagnostic::default(),
        implicit: false,
        settings: crate::pg_settings::SessionSettings::default(),
        statements: BTreeMap::new(),
        portals: BTreeMap::new(),
        drain: false,
    };
    startup_settings.timezone = timezone;
    state.settings.current = startup_settings;
    loop {
        // Idle connections hold a bounded admission slot, not an active frame.
        // Once a frame starts, its remaining header and body have a deadline.
        let tag = match s.read_u8().await {
            Ok(tag) => tag,
            Err(_) => return Ok(()),
        };
        let incoming = timeout(IO_TIMEOUT, async {
            let n = s.read_u32().await?;
            if n < 4 || n as u64 > MAX_FRAME as u64 {
                return Err(io::Error::other("wire frame limit"));
            }
            let mut b = vec![0; n as usize - 4];
            s.read_exact(&mut b).await?;
            Ok((tag, b))
        })
        .await;
        let (tag, b) = match incoming {
            Ok(Ok(v)) => v,
            _ => return Ok(()),
        };
        if !session.service.admission.request() {
            return Ok(());
        }
        let received = std::time::Instant::now();
        // A successful implicit cycle publishes at Sync and must be cancellable
        // at that decision boundary. Recovery-only Sync remains uncancellable.
        let active = backend.begin(
            matches!(tag, b'Q' | b'P' | b'B' | b'D' | b'E')
                || (tag == b'S' && state.implicit && !session.is_aborted()),
        );
        let permit = match blocking.admit(&active.context).await {
            Ok(permit) => permit,
            Err(e) => {
                session.abort();
                let mut out = Vec::new();
                error(crate::service::sqlstate(&e), &mut out);
                if tag == b'S' && state.implicit {
                    session.sql.rollback();
                    state.settings.transaction(true, false, "ROLLBACK");
                    state.implicit = false;
                    state.portals.clear();
                }
                if matches!(tag, b'Q' | b'S') {
                    state.drain = false;
                    ready(&session, &mut out).map_err(io::Error::other)?;
                } else {
                    state.drain = true;
                }
                drop(active);
                send(s, &out).await?;
                continue;
            }
        };
        state.diagnostic = Diagnostic {
            phase: match tag {
                b'Q' => "simple query",
                b'P' => "parse",
                b'B' => "bind",
                b'D' => "describe",
                b'E' => "execute",
                b'S' => "sync",
                _ => "protocol message",
            },
            query_id: None,
        };
        let (returned_session, returned_state, response, keep) = blocking
            .run_admitted(permit, move || {
                let mut out = Vec::new();
                let keep = match active.context.scope(|| {
                    active
                        .context
                        .check()
                        .map_err(|e| crate::service::sqlstate(&e))?;
                    // The authority gate covers cached portal output and the
                    // actual core commit, not just Parse/Bind authorization.
                    session
                        .authority_scope(|session| {
                            Ok(state.handle(tag, &b, session, &mut out, received))
                        })
                        .map_err(|e| crate::service::sqlstate(&e))?
                }) {
                    Ok(keep) => Ok(keep),
                    Err(code) => {
                        session.abort();
                        // COMMIT may fail and finish rollback before returning
                        // its error. No portal can outlive that transaction.
                        if tag == b'Q' && !session.in_transaction() {
                            state.portals.clear();
                        }
                        if tag == b'S' && state.implicit {
                            session.sql.rollback();
                            state.settings.transaction(true, false, "ROLLBACK");
                            state.implicit = false;
                            state.portals.clear();
                        }
                        let message = match code {
                            "0A000" => {
                                "unsupported SQL feature or parameter type in this server profile"
                            }
                            "42601" => {
                                "statement syntax is not supported by this server SQL profile"
                            }
                            _ => "request rejected; consult documented code",
                        };
                        diagnostic_error(code, message, Some(&state.diagnostic), &mut out);
                        if matches!(tag, b'Q' | b'S') {
                            state.drain = false;
                            ready(&session, &mut out)
                                .map(|()| true)
                                .map_err(io::Error::other)
                        } else {
                            state.drain = true;
                            Ok(true)
                        }
                    }
                };
                drop(active);
                (session, state, out, keep)
            })
            .await?;
        session = returned_session;
        state = returned_state;
        if !keep? {
            return Ok(());
        }
        if session.check_binding().is_err() {
            return Ok(());
        }
        send(s, &response).await?;
    }
}
