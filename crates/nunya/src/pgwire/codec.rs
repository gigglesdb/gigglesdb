//! Strict, socket-free PostgreSQL protocol framing.
use crate::error::{NyCode, Refusal};
use bytes::{Buf, BufMut, BytesMut};
/// Maximum regular message length, including its length word.
pub const MAX_MESSAGE: usize = 64 * 1024 * 1024;
/// Maximum startup packet length.
pub const MAX_STARTUP: usize = 10_000;
/// An initial client packet.
#[derive(PartialEq, Eq)]
pub enum StartupMessage {
    /// Wire Startup value.
    Startup {
        /// Requested protocol version, 3.0 or 3.2.
        protocol: u32,
        /// Ordered parameters; bind None values represent SQL NULL.
        params: Vec<(String, String)>,
    },
    /// Wire SslRequest value.
    SslRequest,
    /// Wire GssEncRequest value.
    GssEncRequest,
    /// Wire CancelRequest value.
    CancelRequest {
        /// Backend process identifier for cancellation.
        pid: i32,
        /// Opaque cancellation secret, between 4 and 256 bytes.
        secret: Vec<u8>,
    },
}
/// A frontend message; password payloads are deliberately opaque.
#[derive(PartialEq, Eq)]
pub enum Frontend {
    /// Simple SQL query text.
    Query(String),
    /// Wire Parse value.
    Parse {
        /// Wire name value.
        name: String,
        /// Wire query value.
        query: String,
        /// Parameter type OIDs; zero leaves inference to the server.
        param_types: Vec<u32>,
    },
    /// Wire Bind value.
    Bind {
        /// Wire portal value.
        portal: String,
        /// Wire statement value.
        statement: String,
        /// Parameter formats: zero for text, one for binary.
        param_formats: Vec<i16>,
        /// Ordered parameters; bind None values represent SQL NULL.
        params: Vec<Option<Vec<u8>>>,
        /// Result formats: zero for text, one for binary.
        result_formats: Vec<i16>,
    },
    /// Wire Describe value.
    Describe {
        /// Target kind: b'S' for statement or b'P' for portal.
        kind: u8,
        /// Wire name value.
        name: String,
    },
    /// Wire Execute value.
    Execute {
        /// Wire portal value.
        portal: String,
        /// Maximum returned rows; zero requests all rows.
        max_rows: i32,
    },
    /// Wire Close value.
    Close {
        /// Target kind: b'S' for statement or b'P' for portal.
        kind: u8,
        /// Wire name value.
        name: String,
    },
    /// Wire Sync value.
    Sync,
    /// Wire Flush value.
    Flush,
    /// Wire Terminate value.
    Terminate,
    /// Opaque password or SASL authentication payload.
    Password(Vec<u8>),
    /// Explicit unsupported frontend message tag.
    Refused(u8),
}
/// One output column's wire metadata.
#[derive(Debug, PartialEq, Eq)]
pub struct FieldDescription {
    /// Wire name value.
    pub name: String,
    /// Origin table OID, or zero when unavailable.
    pub table_oid: u32,
    /// Origin attribute number, or zero when unavailable.
    pub column_attr: i16,
    /// PostgreSQL data type OID.
    pub type_oid: u32,
    /// Type size, or negative one for variable length.
    pub type_size: i16,
    /// Type-specific modifier, or negative one when absent.
    pub type_modifier: i32,
    /// Column format: zero for text, one for binary.
    pub format: i16,
}
/// A backend message; unsupported traffic is explicitly refused.
#[derive(PartialEq, Eq)]
pub enum Backend {
    /// Wire AuthenticationOk value.
    AuthenticationOk,
    /// Wire AuthenticationSasl value.
    AuthenticationSasl(Vec<String>),
    /// Wire AuthenticationSaslContinue value.
    AuthenticationSaslContinue(Vec<u8>),
    /// Wire AuthenticationSaslFinal value.
    AuthenticationSaslFinal(Vec<u8>),
    /// Wire ParameterStatus value.
    ParameterStatus(String, String),
    /// Wire BackendKeyData value.
    BackendKeyData {
        /// Backend process identifier for cancellation.
        pid: i32,
        /// Opaque cancellation secret, between 4 and 256 bytes.
        secret: Vec<u8>,
    },
    /// Wire ReadyForQuery value.
    ReadyForQuery(u8),
    /// Wire RowDescription value.
    RowDescription(Vec<FieldDescription>),
    /// Wire DataRow value.
    DataRow(Vec<Option<Vec<u8>>>),
    /// Wire CommandComplete value.
    CommandComplete(String),
    /// Wire EmptyQueryResponse value.
    EmptyQueryResponse,
    /// Acknowledgment of successful statement parsing.
    ParseComplete,
    /// Acknowledgment of successful portal binding.
    BindComplete,
    /// Acknowledgment of successful resource closure.
    CloseComplete,
    /// Wire NoData value.
    NoData,
    /// Wire PortalSuspended value.
    PortalSuspended,
    /// Wire ParameterDescription value.
    ParameterDescription(Vec<u32>),
    /// Wire ErrorResponse value.
    ErrorResponse(Vec<(u8, String)>),
    /// Wire NoticeResponse value.
    NoticeResponse(Vec<(u8, String)>),
    /// Wire NegotiateProtocolVersion value.
    NegotiateProtocolVersion {
        /// Wire newest_minor value.
        newest_minor: u32,
        /// Wire unrecognized value.
        unrecognized: Vec<String>,
    },
    /// Unsupported COPY response; encoding emits an empty text COPY descriptor.
    /// Other refused tags cannot be encoded and trigger a debug assertion.
    Refused(u8),
}
fn bad() -> Refusal {
    Refusal::new(NyCode::ProtocolNotAllowed, "invalid protocol message")
}
struct Reader<'a>(&'a [u8]);
impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], Refusal> {
        if n > self.0.len() {
            return Err(bad());
        }
        let (v, rest) = self.0.split_at(n);
        self.0 = rest;
        Ok(v)
    }
    fn byte(&mut self) -> Result<u8, Refusal> {
        Ok(self.take(1)?[0])
    }
    fn i16(&mut self) -> Result<i16, Refusal> {
        let b = self.take(2)?;
        Ok(i16::from_be_bytes([b[0], b[1]]))
    }
    fn u32(&mut self) -> Result<u32, Refusal> {
        let b = self.take(4)?;
        Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }
    fn i32(&mut self) -> Result<i32, Refusal> {
        Ok(self.u32()? as i32)
    }
    fn count(&mut self) -> Result<usize, Refusal> {
        let n = self.i16()? as u16 as usize;
        if n > self.0.len() {
            return Err(bad());
        }
        Ok(n)
    }
    fn string(&mut self) -> Result<String, Refusal> {
        let n = self.0.iter().position(|b| *b == 0).ok_or_else(bad)?;
        let s = std::str::from_utf8(self.take(n)?)
            .map_err(|_| bad())?
            .to_owned();
        self.take(1)?;
        Ok(s)
    }
    fn format(&mut self) -> Result<i16, Refusal> {
        let v = self.i16()?;
        if !matches!(v, 0 | 1) {
            return Err(bad());
        }
        Ok(v)
    }
    fn format_byte(&mut self) -> Result<u8, Refusal> {
        let v = self.byte()?;
        if v > 1 {
            return Err(bad());
        }
        Ok(v)
    }
    fn formats(&mut self) -> Result<Vec<i16>, Refusal> {
        let n = self.count()?;
        (0..n).map(|_| self.format()).collect()
    }
    fn values(&mut self) -> Result<Vec<Option<Vec<u8>>>, Refusal> {
        let n = self.count()?;
        (0..n)
            .map(|_| {
                let n = self.i32()?;
                match n {
                    -1 => Ok(None),
                    0.. => Ok(Some(self.take(n as usize)?.to_vec())),
                    _ => Err(bad()),
                }
            })
            .collect()
    }
    fn end(&self) -> Result<(), Refusal> {
        if self.0.is_empty() {
            Ok(())
        } else {
            Err(bad())
        }
    }
    fn kind(&mut self) -> Result<u8, Refusal> {
        let b = self.byte()?;
        if matches!(b, b'S' | b'P') {
            Ok(b)
        } else {
            Err(bad())
        }
    }
}
fn frame(buf: &mut BytesMut) -> Result<Option<(u8, BytesMut)>, Refusal> {
    if buf.len() < 5 {
        return Ok(None);
    }
    let n = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
    if !(4..=MAX_MESSAGE).contains(&n) {
        return Err(bad());
    }
    if buf.len() < n + 1 {
        return Ok(None);
    }
    let tag = buf[0];
    let mut packet = buf.split_to(n + 1);
    packet.advance(5);
    Ok(Some((tag, packet)))
}
/// Decode one startup packet without consuming incomplete input.
pub fn decode_startup(buf: &mut BytesMut) -> Result<Option<StartupMessage>, Refusal> {
    if buf.len() < 4 {
        return Ok(None);
    }
    let n = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    if !(8..=MAX_STARTUP).contains(&n) {
        return Err(bad());
    }
    if buf.len() < n {
        return Ok(None);
    }
    let packet = buf.split_to(n);
    let mut r = Reader(&packet[4..]);
    let protocol = r.u32()?;
    let msg = match protocol {
        80877103 => StartupMessage::SslRequest,
        80877104 => StartupMessage::GssEncRequest,
        80877102 => {
            let pid = r.i32()?;
            if !(4..=256).contains(&r.0.len()) {
                return Err(bad());
            }
            let secret = r.take(r.0.len())?.to_vec();
            StartupMessage::CancelRequest { pid, secret }
        }
        196608 | 196610 => {
            let mut params = Vec::new();
            let mut keys = std::collections::HashSet::new();
            loop {
                let key = r.string()?;
                if key.is_empty() {
                    break;
                }
                if !keys.insert(key.clone()) {
                    return Err(bad());
                }
                params.push((key, r.string()?));
            }
            StartupMessage::Startup { protocol, params }
        }
        _ => return Err(bad()),
    };
    r.end()?;
    Ok(Some(msg))
}
/// Decode a frontend frame, refusing malformed fields.
pub fn decode_frontend(buf: &mut BytesMut) -> Result<Option<Frontend>, Refusal> {
    let Some((tag, body)) = frame(buf)? else {
        return Ok(None);
    };
    let mut r = Reader(&body);
    let msg = match tag {
        b'Q' => Frontend::Query(r.string()?),
        b'P' => {
            let name = r.string()?;
            let query = r.string()?;
            let n = r.count()?;
            let param_types = (0..n).map(|_| r.u32()).collect::<Result<_, _>>()?;
            Frontend::Parse {
                name,
                query,
                param_types,
            }
        }
        b'B' => {
            let portal = r.string()?;
            let statement = r.string()?;
            let param_formats = r.formats()?;
            let params = r.values()?;
            if !matches!(param_formats.len(), 0 | 1) && param_formats.len() != params.len() {
                return Err(bad());
            }
            let result_formats = r.formats()?;
            Frontend::Bind {
                portal,
                statement,
                param_formats,
                params,
                result_formats,
            }
        }
        b'D' => Frontend::Describe {
            kind: r.kind()?,
            name: r.string()?,
        },
        b'E' => {
            let portal = r.string()?;
            let max_rows = r.i32()?;
            if max_rows < 0 {
                return Err(bad());
            }
            Frontend::Execute { portal, max_rows }
        }
        b'C' => Frontend::Close {
            kind: r.kind()?,
            name: r.string()?,
        },
        b'S' => Frontend::Sync,
        b'H' => Frontend::Flush,
        b'X' => Frontend::Terminate,
        b'p' => {
            r.take(r.0.len())?;
            Frontend::Password(body.to_vec())
        }
        _ => return Ok(Some(Frontend::Refused(tag))),
    };
    r.end()?;
    Ok(Some(msg))
}
fn string(out: &mut BytesMut, s: &str) {
    out.extend_from_slice(s.as_bytes());
    out.put_u8(0);
}
fn values(out: &mut BytesMut, xs: &[Option<Vec<u8>>]) {
    out.put_i16(xs.len() as i16);
    for x in xs {
        match x {
            None => out.put_i32(-1),
            Some(v) => {
                out.put_i32(v.len() as i32);
                out.extend_from_slice(v);
            }
        }
    }
}
fn formats(out: &mut BytesMut, xs: &[i16]) {
    out.put_i16(xs.len() as i16);
    for x in xs {
        out.put_i16(*x);
    }
}
fn emit(out: &mut BytesMut, tag: u8, body: BytesMut) {
    out.put_u8(tag);
    out.put_u32((body.len() + 4) as u32);
    out.extend_from_slice(&body);
}
/// Encode a supported frontend message for upstream forwarding.
pub fn encode_frontend(msg: &Frontend, out: &mut BytesMut) {
    let mut b = BytesMut::new();
    let tag = match msg {
        Frontend::Query(s) => {
            string(&mut b, s);
            b'Q'
        }
        Frontend::Parse {
            name,
            query,
            param_types,
        } => {
            string(&mut b, name);
            string(&mut b, query);
            b.put_i16(param_types.len() as i16);
            for t in param_types {
                b.put_u32(*t);
            }
            b'P'
        }
        Frontend::Bind {
            portal,
            statement,
            param_formats,
            params,
            result_formats,
        } => {
            string(&mut b, portal);
            string(&mut b, statement);
            formats(&mut b, param_formats);
            values(&mut b, params);
            formats(&mut b, result_formats);
            b'B'
        }
        Frontend::Describe { kind, name } => {
            b.put_u8(*kind);
            string(&mut b, name);
            b'D'
        }
        Frontend::Close { kind, name } => {
            b.put_u8(*kind);
            string(&mut b, name);
            b'C'
        }
        Frontend::Execute { portal, max_rows } => {
            string(&mut b, portal);
            b.put_i32(*max_rows);
            b'E'
        }
        Frontend::Sync => b'S',
        Frontend::Flush => b'H',
        Frontend::Terminate => b'X',
        Frontend::Password(v) => {
            b.extend_from_slice(v);
            b'p'
        }
        Frontend::Refused(_) => {
            debug_assert!(false, "cannot encode refused message");
            return;
        }
    };
    emit(out, tag, b);
}
/// Encode one backend frame.
pub fn encode_backend(msg: &Backend, out: &mut BytesMut) {
    let mut b = BytesMut::new();
    let tag = match msg {
        Backend::AuthenticationOk => {
            b.put_u32(0);
            b'R'
        }
        Backend::AuthenticationSasl(xs) => {
            b.put_u32(10);
            for x in xs {
                string(&mut b, x);
            }
            b.put_u8(0);
            b'R'
        }
        Backend::AuthenticationSaslContinue(v) => {
            b.put_u32(11);
            b.extend_from_slice(v);
            b'R'
        }
        Backend::AuthenticationSaslFinal(v) => {
            b.put_u32(12);
            b.extend_from_slice(v);
            b'R'
        }
        Backend::ParameterStatus(k, v) => {
            string(&mut b, k);
            string(&mut b, v);
            b'S'
        }
        Backend::BackendKeyData { pid, secret } => {
            b.put_i32(*pid);
            b.extend_from_slice(secret);
            b'K'
        }
        Backend::ReadyForQuery(s) => {
            b.put_u8(*s);
            b'Z'
        }
        Backend::RowDescription(xs) => {
            b.put_i16(xs.len() as i16);
            for x in xs {
                string(&mut b, &x.name);
                b.put_u32(x.table_oid);
                b.put_i16(x.column_attr);
                b.put_u32(x.type_oid);
                b.put_i16(x.type_size);
                b.put_i32(x.type_modifier);
                b.put_i16(x.format);
            }
            b'T'
        }
        Backend::DataRow(xs) => {
            values(&mut b, xs);
            b'D'
        }
        Backend::CommandComplete(s) => {
            string(&mut b, s);
            b'C'
        }
        Backend::EmptyQueryResponse => b'I',
        Backend::ParseComplete => b'1',
        Backend::BindComplete => b'2',
        Backend::CloseComplete => b'3',
        Backend::NoData => b'n',
        Backend::PortalSuspended => b's',
        Backend::ParameterDescription(xs) => {
            b.put_i16(xs.len() as i16);
            for x in xs {
                b.put_u32(*x);
            }
            b't'
        }
        Backend::ErrorResponse(xs) | Backend::NoticeResponse(xs) => {
            for (k, v) in xs {
                b.put_u8(*k);
                string(&mut b, v);
            }
            b.put_u8(0);
            if matches!(msg, Backend::ErrorResponse(_)) {
                b'E'
            } else {
                b'N'
            }
        }
        Backend::NegotiateProtocolVersion {
            newest_minor,
            unrecognized,
        } => {
            b.put_u32(*newest_minor);
            b.put_u32(unrecognized.len() as u32);
            for s in unrecognized {
                string(&mut b, s);
            }
            b'v'
        }
        Backend::Refused(tag @ (b'G' | b'H' | b'W')) => {
            b.put_u8(0);
            b.put_u16(0);
            *tag
        }
        Backend::Refused(_) => {
            debug_assert!(false, "cannot encode refused message");
            return;
        }
    };
    emit(out, tag, b);
}
/// Decode an upstream backend frame; unsupported tags remain explicit refusals.
pub fn decode_backend(buf: &mut BytesMut) -> Result<Option<Backend>, Refusal> {
    let Some((tag, body)) = frame(buf)? else {
        return Ok(None);
    };
    let mut r = Reader(&body);
    let msg = match tag {
        b'R' => match r.u32()? {
            0 => Backend::AuthenticationOk,
            10 => {
                let mut xs = Vec::new();
                loop {
                    let s = r.string()?;
                    if s.is_empty() {
                        break;
                    }
                    xs.push(s);
                }
                Backend::AuthenticationSasl(xs)
            }
            11 => Backend::AuthenticationSaslContinue(r.take(r.0.len())?.to_vec()),
            12 => Backend::AuthenticationSaslFinal(r.take(r.0.len())?.to_vec()),
            _ => return Err(bad()),
        },
        b'S' => Backend::ParameterStatus(r.string()?, r.string()?),
        b'K' => {
            let pid = r.i32()?;
            if !(4..=256).contains(&r.0.len()) {
                return Err(bad());
            }
            Backend::BackendKeyData {
                pid,
                secret: r.take(r.0.len())?.to_vec(),
            }
        }
        b'Z' => {
            let s = r.byte()?;
            if !matches!(s, b'I' | b'T' | b'E') {
                return Err(bad());
            }
            Backend::ReadyForQuery(s)
        }
        b'T' => {
            let n = r.count()?;
            let mut xs = Vec::new();
            for _ in 0..n {
                xs.push(FieldDescription {
                    name: r.string()?,
                    table_oid: r.u32()?,
                    column_attr: r.i16()?,
                    type_oid: r.u32()?,
                    type_size: r.i16()?,
                    type_modifier: r.i32()?,
                    format: r.format()?,
                });
            }
            Backend::RowDescription(xs)
        }
        b'D' => Backend::DataRow(r.values()?),
        b'C' => Backend::CommandComplete(r.string()?),
        b'I' => Backend::EmptyQueryResponse,
        b'1' => Backend::ParseComplete,
        b'2' => Backend::BindComplete,
        b'3' => Backend::CloseComplete,
        b'n' => Backend::NoData,
        b's' => Backend::PortalSuspended,
        b't' => {
            let n = r.count()?;
            Backend::ParameterDescription((0..n).map(|_| r.u32()).collect::<Result<_, _>>()?)
        }
        b'E' | b'N' => {
            let mut xs = Vec::new();
            loop {
                let k = r.byte()?;
                if k == 0 {
                    break;
                }
                xs.push((k, r.string()?));
            }
            if tag == b'E' {
                Backend::ErrorResponse(xs)
            } else {
                Backend::NoticeResponse(xs)
            }
        }
        b'v' => {
            let newest_minor = r.u32()?;
            let n = r.u32()? as usize;
            if n > r.0.len() {
                return Err(bad());
            }
            let unrecognized = (0..n).map(|_| r.string()).collect::<Result<_, _>>()?;
            Backend::NegotiateProtocolVersion {
                newest_minor,
                unrecognized,
            }
        }
        b'G' | b'H' | b'W' => {
            let overall = r.format_byte()?;
            let columns = r.formats()?;
            if overall == 0 && columns.iter().any(|v| *v != 0) {
                return Err(bad());
            }
            Backend::Refused(tag)
        }
        _ => return Err(bad()),
    };
    r.end()?;
    Ok(Some(msg))
}

impl std::fmt::Debug for Frontend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Frontend(<redacted>)")
    }
}
impl std::fmt::Debug for Backend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Backend(<redacted>)")
    }
}
impl std::fmt::Debug for StartupMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("StartupMessage(<redacted>)")
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn packet(tag: u8, payload: &[u8]) -> BytesMut {
        let mut b = BytesMut::new();
        emit(&mut b, tag, BytesMut::from(payload));
        b
    }
    fn refused<T>(r: Result<T, Refusal>) {
        assert!(matches!(r,Err(e) if e.code==NyCode::ProtocolNotAllowed));
    }
    #[test]
    fn backend_roundtrips() {
        let messages = vec![
            Backend::AuthenticationOk,
            Backend::AuthenticationSasl(vec!["SCRAM-SHA-256".into()]),
            Backend::AuthenticationSaslContinue(vec![1]),
            Backend::AuthenticationSaslFinal(vec![2]),
            Backend::ParameterStatus("a".into(), "b".into()),
            Backend::BackendKeyData {
                pid: 1,
                secret: vec![0; 256],
            },
            Backend::ReadyForQuery(b'I'),
            Backend::RowDescription(vec![FieldDescription {
                name: "a".into(),
                table_oid: 1,
                column_attr: 2,
                type_oid: 3,
                type_size: -1,
                type_modifier: -1,
                format: 1,
            }]),
            Backend::DataRow(vec![None, Some(vec![]), Some(vec![0, 1])]),
            Backend::CommandComplete("SELECT 1".into()),
            Backend::EmptyQueryResponse,
            Backend::ParseComplete,
            Backend::BindComplete,
            Backend::CloseComplete,
            Backend::NoData,
            Backend::PortalSuspended,
            Backend::ParameterDescription(vec![1, 2]),
            Backend::ErrorResponse(vec![(b'C', "NY010".into())]),
            Backend::NoticeResponse(vec![(b'M', "notice".into())]),
            Backend::NegotiateProtocolVersion {
                newest_minor: 2,
                unrecognized: vec!["a".into()],
            },
            Backend::Refused(b'G'),
            Backend::Refused(b'H'),
            Backend::Refused(b'W'),
        ];
        for m in messages {
            let mut b = BytesMut::new();
            encode_backend(&m, &mut b);
            for n in 0..b.len() {
                let mut partial = BytesMut::from(&b[..n]);
                assert_eq!(decode_backend(&mut partial).unwrap(), None);
                assert_eq!(&partial[..], &b[..n]);
            }
            assert_eq!(decode_backend(&mut b).unwrap(), Some(m));
            assert!(b.is_empty());
        }
    }
    #[test]
    fn frontend_handbuilt() {
        let cases = vec![
            (
                b'Q',
                b"select 1\0".to_vec(),
                Frontend::Query("select 1".into()),
            ),
            (
                b'P',
                b"s\0q\0\0\x01\0\0\0\x17".to_vec(),
                Frontend::Parse {
                    name: "s".into(),
                    query: "q".into(),
                    param_types: vec![23],
                },
            ),
            (
                b'B',
                b"p\0s\0\0\x01\0\x01\0\x02\xff\xff\xff\xff\0\0\0\x01x\0\0".to_vec(),
                Frontend::Bind {
                    portal: "p".into(),
                    statement: "s".into(),
                    param_formats: vec![1],
                    params: vec![None, Some(vec![b'x'])],
                    result_formats: vec![],
                },
            ),
            (
                b'D',
                b"Ss\0".to_vec(),
                Frontend::Describe {
                    kind: b'S',
                    name: "s".into(),
                },
            ),
            (
                b'E',
                b"p\0\0\0\0\x01".to_vec(),
                Frontend::Execute {
                    portal: "p".into(),
                    max_rows: 1,
                },
            ),
            (
                b'C',
                b"Pp\0".to_vec(),
                Frontend::Close {
                    kind: b'P',
                    name: "p".into(),
                },
            ),
            (b'S', vec![], Frontend::Sync),
            (b'H', vec![], Frontend::Flush),
            (b'X', vec![], Frontend::Terminate),
            (b'p', vec![0, 1], Frontend::Password(vec![0, 1])),
            (b'd', vec![1], Frontend::Refused(b'd')),
        ];
        for (tag, payload, want) in cases {
            let full = packet(tag, &payload);
            for n in 0..full.len() {
                let mut part = BytesMut::from(&full[..n]);
                assert_eq!(decode_frontend(&mut part).unwrap(), None);
                assert_eq!(&part[..], &full[..n]);
            }
            let mut b = full;
            assert_eq!(decode_frontend(&mut b).unwrap().as_ref(), Some(&want));
            if !matches!(want, Frontend::Refused(_)) {
                let mut encoded = BytesMut::new();
                encode_frontend(&want, &mut encoded);
                assert_eq!(encoded, packet(tag, &payload));
            }
        }
    }
    #[test]
    fn malformed() {
        for payload in [&b"no nul"[..], &b"\xff\0"[..], &b"ok\0extra"[..]] {
            refused(decode_frontend(&mut packet(b'Q', payload)));
        }
        for n in [3u32, u32::MAX, MAX_MESSAGE as u32 + 1] {
            let mut b = BytesMut::from(&b"Q"[..]);
            b.put_u32(n);
            refused(decode_frontend(&mut b));
        }
        refused(decode_frontend(&mut packet(
            b'B',
            b"\0\0\0\0\0\x01\xff\xff\xff\xfe\0\0",
        )));
        refused(decode_frontend(&mut packet(
            b'B',
            b"\0\0\0\x02\0\0\0\x01\0\0\0\0",
        )));
        refused(decode_frontend(&mut packet(
            b'B',
            b"\0\0\0\x01\0\x02\0\0\0\0",
        )));
        refused(decode_backend(&mut packet(b'?', b"")));
    }
    #[test]
    fn backend_malformed_and_unsigned_count() {
        for (tag, payload) in [
            (b'D', &b"\0\x01\xff\xff\xff\xfe"[..]),
            (b'E', &b"Munterminated"[..]),
            (b'S', &b"key\0unterminated"[..]),
            (b'1', &b"x"[..]),
            (b'G', &b"\x02\0\0"[..]),
            (b'H', &b"\0\0\x01\0\x01"[..]),
            (b'W', &b"\x01\0\x01\0\x02"[..]),
            (b'G', &b"\0\0"[..]),
        ] {
            refused(decode_backend(&mut packet(tag, payload)));
        }
        let msg = Backend::ParameterDescription(vec![23; 32768]);
        let mut wire = BytesMut::new();
        encode_backend(&msg, &mut wire);
        assert_eq!(decode_backend(&mut wire).unwrap(), Some(msg));
    }
    fn startup(protocol: u32, payload: &[u8]) -> BytesMut {
        let mut b = BytesMut::new();
        b.put_u32((8 + payload.len()) as u32);
        b.put_u32(protocol);
        b.extend_from_slice(payload);
        b
    }
    #[test]
    fn startup_packets() {
        for protocol in [196608, 196610] {
            let full = startup(protocol, b"user\0agent\0\0");
            for n in 0..full.len() {
                assert_eq!(
                    decode_startup(&mut BytesMut::from(&full[..n])).unwrap(),
                    None
                );
            }
            assert_eq!(
                decode_startup(&mut full.clone()).unwrap(),
                Some(StartupMessage::Startup {
                    protocol,
                    params: vec![("user".into(), "agent".into())]
                })
            );
        }
        assert_eq!(
            decode_startup(&mut startup(80877103, b"")).unwrap(),
            Some(StartupMessage::SslRequest)
        );
        assert_eq!(
            decode_startup(&mut startup(80877104, b"")).unwrap(),
            Some(StartupMessage::GssEncRequest)
        );
        for n in [4, 256] {
            let mut payload = vec![0, 0, 0, 1];
            payload.extend(vec![2; n]);
            assert_eq!(
                decode_startup(&mut startup(80877102, &payload)).unwrap(),
                Some(StartupMessage::CancelRequest {
                    pid: 1,
                    secret: vec![2; n]
                })
            );
        }
        for b in [
            startup(1, b""),
            startup(196608, b"a\0b\0a\0c\0\0"),
            startup(196608, b"a\0b\0"),
            startup(80877103, b"x"),
        ] {
            refused(decode_startup(&mut b.clone()));
        }
        for n in [7u32, MAX_STARTUP as u32 + 1] {
            let mut b = BytesMut::new();
            b.put_u32(n);
            refused(decode_startup(&mut b));
        }
    }
    proptest::proptest! {#![proptest_config(proptest::test_runner::Config { cases: 256, failure_persistence: None, ..Default::default() })]
        #[test] fn random_frontend_never_panics(data in proptest::collection::vec(proptest::prelude::any::<u8>(),0..2048)){let _=decode_frontend(&mut BytesMut::from(data.as_slice()));}
    }
}
