//! Explicit protocol-v3 subset; not PostgreSQL server compatibility.
use crate::{
    Request, Role, Session,
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
struct Reader<'a> {
    b: &'a [u8],
    at: usize,
}
impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> R<&'a [u8]> {
        let end = self.at.checked_add(n).ok_or("08P01")?;
        let b = self.b.get(self.at..end).ok_or("08P01")?;
        self.at = end;
        Ok(b)
    }
    fn byte(&mut self) -> R<u8> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> R<usize> {
        Ok(u16::from_be_bytes(self.take(2)?.try_into().unwrap()) as usize)
    }
    fn u32(&mut self) -> R<u32> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn text(&mut self) -> R<String> {
        let n = self.b[self.at..]
            .iter()
            .position(|b| *b == 0)
            .ok_or("08P01")?;
        let b = self.take(n + 1)?;
        String::from_utf8(b[..n].to_vec()).map_err(|_| "22021")
    }
    fn end(&self) -> R<()> {
        if self.at == self.b.len() {
            Ok(())
        } else {
            Err("08P01")
        }
    }
}
fn frame(tag: u8, b: &[u8], out: &mut Vec<u8>) -> R<()> {
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
fn z(s: &str, b: &mut Vec<u8>) {
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
fn error(code: &str, out: &mut Vec<u8>) {
    out.clear();
    let mut b = vec![b'S'];
    z("ERROR", &mut b);
    b.push(b'C');
    z(code, &mut b);
    b.push(b'M');
    z("request rejected; consult documented code", &mut b);
    b.push(0);
    let _ = frame(b'E', &b, out);
}
fn oid(t: &DataType) -> u32 {
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
fn format_at(formats: &[u16], i: usize) -> u16 {
    if formats.len() == 1 {
        formats[0]
    } else {
        *formats.get(i).unwrap_or(&0)
    }
}
fn formats(r: &mut Reader<'_>, count: usize) -> R<Vec<u16>> {
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
fn row_description(d: &Description, formats: &[u16], out: &mut Vec<u8>) -> R<()> {
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
fn decode(b: &[u8], ty: &DataType, format: u16) -> R<Value> {
    if format == 0 {
        let s = std::str::from_utf8(b).map_err(|_| "22021")?;
        return match ty {
            DataType::Boolean => match s {
                "t" | "true" => Ok(Value::Bool(true)),
                "f" | "false" => Ok(Value::Bool(false)),
                _ => Err("22P02"),
            },
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
#[derive(Clone)]
struct Statement {
    p: Prepared,
    d: Description,
}
struct Portal {
    s: Statement,
    params: Vec<Value>,
    formats: Vec<u16>,
    result: Option<SqlResult>,
    at: usize,
}
struct State {
    statements: BTreeMap<String, Statement>,
    portals: BTreeMap<String, Portal>,
    drain: bool,
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
        match tag {
            b'X' => {
                r.end()?;
                return Ok(false);
            }
            b'S' => {
                r.end()?;
                self.drain = false;
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
                } else if sql.trim().is_empty() {
                    frame(b'I', &[], out)?;
                } else {
                    // ROLLBACK must bypass normal prepare when already aborted.
                    if session.is_aborted()
                        && sql
                            .trim()
                            .trim_end_matches(';')
                            .eq_ignore_ascii_case("rollback")
                    {
                        let result = core(session.execute_sql(&sql, &[]))?;
                        complete("ROLLBACK", &result, out)?;
                    } else {
                        let p = core(session.prepare(&sql))?;
                        let d = core(session.sql.describe(&p))?;
                        let result = core(session.execute_prepared(&p, &[]))?;
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
                let p = core(session.prepare(&sql))?;
                let d = core(session.sql.describe(&p))?;
                if n > d.parameters.len() {
                    return Err("08P01");
                }
                for (o, t) in oids.into_iter().zip(&d.parameters) {
                    if o != 0 && o != oid(t) {
                        return Err("0A000");
                    }
                }
                self.statements.insert(name, Statement { p, d });
                frame(b'1', &[], out)?;
            }
            b'B' => {
                let name = r.text()?;
                let stmt = r.text()?;
                let s = self.statements.get(&stmt).ok_or("26000")?.clone();
                core(session.sql.describe(&s.p))?;
                let fmts = formats(&mut r, s.d.parameters.len())?;
                let n = r.u16()?;
                if n != s.d.parameters.len() {
                    return Err("08P01");
                }
                let mut params = vec![];
                for (i, ty) in s.d.parameters.iter().enumerate() {
                    let n = r.u32()?;
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
                let bound = self
                    .portals
                    .values()
                    .try_fold(0usize, |sum, p| {
                        sum.checked_add(serde_json::to_vec(&p.params).ok()?.len())
                    })
                    .ok_or("54000")?;
                if bound
                    .checked_add(serde_json::to_vec(&params).map_err(|_| "54000")?.len())
                    .ok_or("54000")?
                    > MAX_RESPONSE
                {
                    return Err("54000");
                }
                self.portals.insert(
                    name,
                    Portal {
                        s,
                        params,
                        formats: output,
                        result: None,
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
                        let d = core(session.sql.describe(&s.p))?;
                        let mut b = vec![];
                        b.extend((d.parameters.len() as u16).to_be_bytes());
                        for t in &d.parameters {
                            b.extend(oid(t).to_be_bytes());
                        }
                        frame(b't', &b, out)?;
                        row_description(&d, &[], out)?;
                    }
                    b'P' => {
                        let p = self.portals.get(&name).ok_or("34000")?;
                        let d = core(session.sql.describe(&p.s.p))?;
                        row_description(&d, &p.formats, out)?;
                    }
                    _ => return Err("08P01"),
                }
            }
            b'E' => {
                let was_transaction = session.in_transaction();
                let name = r.text()?;
                let max = r.u32()?;
                r.end()?;
                if max > i32::MAX as u32 {
                    return Err("08P01");
                }
                let p = self.portals.get_mut(&name).ok_or("34000")?;
                core(session.sql.describe(&p.s.p))?;
                if p.result.is_none() {
                    p.result = Some(core(session.execute_prepared(&p.s.p, &p.params))?);
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
                    complete(p.s.p.command(), result, out)?;
                }
                if was_transaction && !session.in_transaction() {
                    self.portals.clear();
                }
                let retained = self
                    .portals
                    .values()
                    .filter_map(|p| p.result.as_ref())
                    .try_fold(0usize, |sum, r| {
                        sum.checked_add(serde_json::to_vec(r).ok()?.len())
                    })
                    .ok_or("54000")?;
                if retained > MAX_RESPONSE {
                    self.portals.clear();
                    return Err("54000");
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
        session
            .service
            .cancellations
            .cancel(&session.fingerprint, pid, secret);
        return Ok(());
    }
    let validate = (|| -> R<()> {
        let mut r = Reader { b: &startup, at: 0 };
        if r.u32()? != 196608 {
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
        if fields.get("user").map(String::as_str) != Some("giggles")
            || fields.get("database").map(String::as_str) != Some("giggles")
        {
            return Err("28000");
        }
        if fields.keys().any(|k| {
            !matches!(
                k.as_str(),
                "user" | "database" | "client_encoding" | "application_name"
            )
        }) {
            return Err("0A000");
        }
        if fields
            .get("client_encoding")
            .is_some_and(|v| !v.eq_ignore_ascii_case("UTF8"))
        {
            return Err("0A000");
        }
        Ok(())
    })();
    let mut out = vec![];
    if let Err(code) = validate {
        error(code, &mut out);
        send(s, &out).await?;
        return Ok(());
    }
    let backend = session
        .service
        .cancellations
        .register(&session.fingerprint)?;
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
    ] {
        let mut b = vec![];
        z(key, &mut b);
        z(value, &mut b);
        frame(b'S', &b, &mut out).map_err(io::Error::other)?;
    }
    ready(&session, &mut out).map_err(io::Error::other)?;
    send(s, &out).await?;
    let mut state = State {
        statements: BTreeMap::new(),
        portals: BTreeMap::new(),
        drain: false,
    };
    loop {
        let incoming = timeout(IO_TIMEOUT, async {
            let tag = s.read_u8().await?;
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
        let received = std::time::Instant::now();
        // Sync/Close/Terminate must remain usable for protocol recovery.
        let active = backend.begin(matches!(tag, b'Q' | b'P' | b'B' | b'D' | b'E'));
        let permit = match blocking.admit(&active.context).await {
            Ok(permit) => permit,
            Err(e) => {
                session.abort();
                let mut out = Vec::new();
                error(crate::service::sqlstate(&e), &mut out);
                if tag == b'Q' {
                    ready(&session, &mut out).map_err(io::Error::other)?;
                } else {
                    state.drain = true;
                }
                drop(active);
                send(s, &out).await?;
                continue;
            }
        };
        let (returned_session, returned_state, response, keep) = blocking
            .run_admitted(permit, move || {
                let mut out = Vec::new();
                let keep = match active.context.scope(|| {
                    active
                        .context
                        .check()
                        .map_err(|e| crate::service::sqlstate(&e))?;
                    state.handle(tag, &b, &mut session, &mut out, received)
                }) {
                    Ok(keep) => Ok(keep),
                    Err(code) => {
                        session.abort();
                        error(code, &mut out);
                        if tag == b'Q' {
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
        send(s, &response).await?;
    }
}
