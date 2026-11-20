//! Explicit protocol-v3 subset; not PostgreSQL server compatibility.
use crate::{
    Role, Session, pg_catalog_wire as catalog_wire,
    transport::{IO_TIMEOUT, MAX_FRAME, MAX_RESPONSE},
};
#[cfg(feature = "legacy-task")]
use crate::Request;
use giggles_core::{
    CoreError,
    relational::{DataType, Description, Prepared, SqlResult, Value},
    task::Released,
};
use giggles_pg_types::temporal::SessionZone;
use std::{collections::BTreeMap, io};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    time::timeout,
};
#[path = "pgwire_copy.rs"]
mod copy;
#[path = "pgwire_copy_in.rs"]
mod copy_in;
#[path = "pgwire_copy_external.rs"]
mod copy_external;
#[path = "pgwire_settings_functions.rs"]
mod settings_functions;
#[path = "pgwire_simple.rs"]
mod simple;
#[path = "pgwire_explain.rs"]
mod explain;
use copy_in::{CopyIn, is_copy_sql};
use simple::{execute_settings, parse_settings, settings_status, show_settings};
#[path = "pgwire_cursor.rs"]
mod cursor;
#[path = "pgwire_transaction.rs"]
mod transaction;
#[path = "pgwire_task.rs"]
mod task_path;
use transaction::{admin_command, execute_mode, parse_control, read_only_check};

type R<T> = Result<T, &'static str>;
const MAX_NAMED_STATEMENTS: usize = 1024;
const MAX_NAMED_PORTALS: usize = 1024;
const MAX_CURSORS: usize = 1024;
// Every legal statement must fit, with headroom for another maximum-size statement.
const MAX_NAMED_STATEMENT_BYTES: usize = {
    let headroom = 2 * giggles_core::relational::MAX_STATEMENT_SQL;
    if headroom > 16 * 1024 * 1024 { headroom } else { 16 * 1024 * 1024 }
};
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
const GENERIC_MESSAGE: &str = "request rejected; consult documented code";
pub fn error(code: &str, out: &mut Vec<u8>) {
    error_message(code, GENERIC_MESSAGE, out);
}
pub(crate) fn error_message(code: &str, message: &'static str, out: &mut Vec<u8>) {
    diagnostic_error(code, message, None, out);
}
#[derive(Default)]
struct Diagnostic {
    unknown_outcome: bool,
    phase: &'static str,
    message: Option<&'static str>,
    wire_message: Option<String>,
    wire_detail: Option<String>,
    wire_hint: Option<&'static str>,
    sql_detail: Option<String>,
    query_id: Option<String>,
    /// GD-0061 R3-310: a task-session refusal (class, error) not yet written
    /// to the read log; the task path logs it with the statement text.
    task_refusal: Option<(giggles_core::task::StatementClass, CoreError)>,
}
fn replication_role_allowed(session: &Session) -> R<bool> {
    match &session.role {
        Role::Database { .. } => { let Role::Database { name } = session.authority_role() else { unreachable!() }; Ok(session.service.admin.roles().get(name)
            .map_err(|_| "42501")?.is_some_and(|role| role.superuser)) },
        _ => Ok(false),
    }
}
impl Diagnostic {
    /// GD-0061 R3-110: a task statement's error keeps its SQLSTATE and message.
    fn task_ddl(&mut self, error: giggles_core::task::catalog::TaskDdlError) -> &'static str {
        self.wire_message = Some(error.message);
        error.sqlstate
    }
    fn core<T>(&mut self, result: Result<T, CoreError>) -> R<T> {
        let message = giggles_core::relational::take_sql_error_message();
        if result.is_err() && message.as_deref().is_some_and(|m|
            m.starts_with("FOR ") || m.starts_with("syntax error at or near ") || m.starts_with("could not obtain lock on row ")
                || m == "canceling statement due to lock timeout"
                || m.starts_with("permission denied for table ") || m.starts_with("view ") || m.starts_with("sequence ") || m.starts_with("index ")
                || m.starts_with("relation ") && m.contains(" clause not found in FROM clause")) {
            self.wire_message = message.clone();
        }
        self.sql_detail = giggles_core::relational::take_sql_error_detail();
        self.wire_hint = giggles_core::relational::take_sql_error_hint();
        if result.is_err() && message.as_deref().is_some_and(|m| m.starts_with("function uuid_") || m.starts_with("extension ") || m.starts_with("cannot drop extension ") || m.starts_with("schema ")) { self.wire_message = message.clone(); }
        if matches!(
            &result,
            Err(CoreError::Sql(
                giggles_core::relational::SqlError::Limit
                    | giggles_core::relational::SqlError::CheckViolation
                    | giggles_core::relational::SqlError::InvalidParameterValue
                    | giggles_core::relational::SqlError::Null
                    | giggles_core::relational::SqlError::Unsupported
                    | giggles_core::relational::SqlError::InvalidTableDefinition
                    | giggles_core::relational::SqlError::Overflow
                    | giggles_core::relational::SqlError::InvalidArgumentForLogarithm
                    | giggles_core::relational::SqlError::InvalidArgumentForPowerFunction
            ))
        ) || matches!(
            &result,
            Err(CoreError::Sql(
                giggles_core::relational::SqlError::Type
                    | giggles_core::relational::SqlError::UndefinedObject
                    | giggles_core::relational::SqlError::CollationMismatch
                    | giggles_core::relational::SqlError::IndeterminateCollation
            ))
        ) && message.as_deref().is_some_and(|m| m.starts_with("collation"))
        {
            self.wire_message = message.clone();
        }
        if result.is_err() && message.as_deref().is_some_and(|m| m.starts_with("collation ") || m.starts_with("cannot drop collation ") || m.starts_with("cannot drop schema ") || m.starts_with("invalid locale name ") || m.starts_with("deterministic requires ") || m.starts_with("permission denied for schema ") || m.starts_with("must be owner of collation ")) {
            self.wire_message = message;
        }
        if let Err(error @ CoreError::WriteFenced { .. }) = &result {
            self.wire_message = Some(error.to_string());
        }
        if let Err(CoreError::Task(task)) = &result
            && task.name().is_some()
        {
            self.message = Some(task.message());
        }
        core(result)
    }

    // Only errors returned by the settings/mode parsers receive GUC diagnostics.
    fn role_admin_error(&mut self, session: &Session, stmt: &crate::auth::statements::AdminStatement, code: &'static str) -> &'static str {
        use crate::auth::statements::AdminStatement as A;
        match (code, stmt) {
            ("42P01", A::DropTable(_, names)) => {
                let sql = session.service.admin.sql_session();
                if let Some(name) = names.iter().find(|name| {
                    let key = name.strip_prefix("public\0").unwrap_or(name);
                    !sql.relation_exists(key).unwrap_or(true)
                }) {
                    let name = name.split_once('\0').map_or(name.as_str(), |(_, name)| name).trim_start_matches('\0');
                    self.wire_message = Some(format!("table \"{name}\" does not exist"));
                }
            }
            ("42939", A::Create(name, ..)) => {
                self.wire_message = Some(format!("role name \"{name}\" is reserved"));
                self.wire_detail = Some("Role names starting with \"pg_\" are reserved.".into());
            }
            ("42501", A::Create(..)) => {
                self.wire_message = Some("permission denied to create role".into());
                self.wire_detail = Some("Only roles with the CREATEROLE attribute may create roles.".into());
            }
            ("42501", A::Membership { grant, roles, grantor, .. }) => {
                let verb = if *grant { "grant" } else { "revoke" };
                let failed_role = crate::auth::statements::take_membership_error();
                if let Some(grantor) = grantor {
                    let catalog = session.service.admin.roles();
                    let grantor = match grantor.as_str() { "\0current_user" | "\0current_role" => match session.authority_role() { Role::Database { name } => name.as_str(), _ => grantor }, "\0session_user" => session.session_user_name().unwrap_or(grantor), _ => grantor };
                    let superuser = catalog.get(grantor).ok().flatten().is_some_and(|r| r.superuser);
                    let bootstrap = crate::auth::statements::bootstrap_grantor(&catalog).ok().flatten().as_deref() == Some(grantor);
                    if ((*grant && !bootstrap) || (!*grant && !superuser))
                        && let Some(role) = roles.iter().find(|role| !catalog.admin_of(grantor, role).unwrap_or(false)) {
                        self.wire_message = Some(format!("permission denied to grant privileges as role \"{grantor}\""));
                        self.wire_detail = Some(format!("The grantor must have the ADMIN option on role \"{role}\"."));
                        return code;
                    }
                }
                if let Some(role) = failed_role.as_ref().or_else(|| roles.iter().find(|role| {
                    let Role::Database { name } = session.authority_role() else { return true; };
                    !session.service.admin.roles().admin_of(name, role).unwrap_or(false)
                })) {
                    self.wire_message = Some(format!("permission denied to {verb} role \"{role}\""));
                    self.wire_detail = Some(format!("Only roles with the ADMIN option on role \"{role}\" may {verb} this role."));
                }
            }
            ("0LP01", A::Membership { .. }) => {
                self.wire_message = crate::auth::statements::take_membership_error();
            }
            ("0A000", A::Membership { roles, members, .. }) => {
                if roles.iter().any(|r| r == "pg_database_owner") { self.wire_message = Some("role \"pg_database_owner\" cannot have explicit members".into()); }
                else if members.iter().any(|r| r == "pg_database_owner") { self.wire_message = Some("role \"pg_database_owner\" cannot be a member of any role".into()); }
            }
            ("2BP01", A::Drop(_, roles)) => {
                if let Some(role) = roles.iter().find(|r| r.starts_with("pg_")) {
                    self.wire_message = Some(format!("cannot drop role {role} because it is required by the database system"));
                }
            }
            ("42939", A::Alter(name, ..)) => {
                self.wire_message = Some(format!("role name \"{name}\" is reserved"));
                self.wire_detail = Some("Cannot alter reserved roles.".into());
            }
            ("42704", A::Membership { .. }) => {
                if let Some(name) = crate::auth::statements::take_membership_error() {
                    self.wire_message = Some(format!("role \"{name}\" does not exist"));
                }
            }
            ("55006", A::Drop(_, roles)) => {
                let is_current = matches!(session.authority_role(), Role::Database { name } if roles.contains(name));
                self.wire_message = Some(if is_current { "current user cannot be dropped" } else { "session user cannot be dropped" }.into());
            }
            ("2BP01", A::Membership { .. }) => {
                self.wire_message = Some("dependent privileges exist".into());
                self.wire_hint = Some("Use CASCADE to revoke them too.");
            }
            _ => {}
        }
        code
    }
    fn settings<T>(&mut self, result: R<T>) -> R<T> {
        result.inspect_err(|code| {
            self.message = match *code {
                "42704" => Some("unrecognized configuration parameter"),
                "22023" => Some("invalid parameter value; not supported by this server profile"),
                _ => None,
            };
        })
    }
    fn replication_role_value(&mut self, value: &str) -> R<()> {
        crate::pg_settings::database::replication_role(value).map(|_| ()).inspect_err(|_| {
            self.wire_message = Some(format!("invalid value for parameter \"session_replication_role\": \"{value}\""));
            self.wire_hint = Some("Available values: origin, replica, local.");
        })
    }
    fn replication_role_authorize(&mut self, session: &Session) -> R<()> {
        // The registry has table owners only; CatalogSnapshot.database_owner is
        // a presentation default (OID 10), not database ownership authority.
        let allowed = replication_role_allowed(session)?;
        if allowed { return Ok(()); }
        self.wire_message = Some("permission denied to set parameter \"session_replication_role\"".into());
        Err("42501")
    }
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
    let message = match code {
        "21000" => "more than one row returned by a subquery used as an expression",
        "42P22" => "could not determine which collation to use for string comparison",
        "42P10" => "for SELECT DISTINCT, ORDER BY expressions must appear in select list",
        // Task codes: sites that only have the code still send a named message.
        _ if message == GENERIC_MESSAGE => {
            giggles_core::task::TaskError::message_for_sqlstate(code).unwrap_or(message)
        }
        _ => message,
    };
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
    z(
        diagnostic
            .and_then(|d| d.wire_message.as_deref())
            .unwrap_or(message),
        &mut b,
    );
    if let Some(detail) = detail {
        b.push(b'D');
        z(diagnostic.and_then(|d| d.sql_detail.as_deref().or(d.wire_detail.as_deref())).unwrap_or(&detail), &mut b);
    }
    if let Some(hint) = diagnostic.and_then(|d| d.wire_hint) {
        b.push(b'H');
        z(hint, &mut b);
    }
    if code == "0A000" && diagnostic.and_then(|d| d.wire_message.as_deref()) == Some("view projection bindings unavailable; recreate the view") {
        b.push(b'H');
        z("Recreate the view to store projection bindings.", &mut b);
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
        DataType::Varchar => 1043,
        DataType::Bpchar => 1042,
        DataType::Uuid => 2950,
        DataType::Jsonb => 3802,
        DataType::IntArray => 1007,
        DataType::BigIntArray => 1016,
        DataType::TextArray => 1009,
        DataType::RegType => 2206,

        DataType::Bytes => 17,
        DataType::Numeric => 1700,
        DataType::Real => 700,
        DataType::DoublePrecision => 701,
        DataType::Date => 1082,
        DataType::Time => 1083,
        DataType::TimeTz => 1266,
        DataType::Timestamp => 1114,
        DataType::TimestampTz => 1184,
        DataType::Interval => 1186,
    }
}
fn size(t: &DataType) -> i16 {
    match t {
        DataType::RegType => 4,
        DataType::Uuid => 16,
        DataType::Jsonb | DataType::IntArray | DataType::BigIntArray | DataType::TextArray => -1,
        DataType::Boolean => 1,
        DataType::Date => 4,
        DataType::Time => 8,
        DataType::TimeTz => 12,
        DataType::Timestamp => 8,
        DataType::TimestampTz => 8,
        DataType::Interval => 16,

        DataType::SmallInt => 2,
        DataType::Integer | DataType::Real => 4,
        DataType::BigInt | DataType::DoublePrecision => 8,
        DataType::Text
        | DataType::Varchar
        | DataType::Bpchar
        | DataType::Bytes
        | DataType::Numeric => -1,
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
        b.extend(
            d.type_oids
                .get(i)
                .copied()
                .filter(|id| *id != 0)
                .unwrap_or_else(|| oid(&c.data_type))
                .to_be_bytes(),
        );
        b.extend(if d.type_oids.get(i) == Some(&2278) { 4i16 } else { size(&c.data_type) }.to_be_bytes());
        b.extend(d.typmods.get(i).copied().unwrap_or(-1).to_be_bytes());
        b.extend(format_at(formats, i).to_be_bytes());
    }
    frame(b'T', &b, out)
}
fn integer_input_error(error: giggles_pg_types::integer::IntegerError) -> &'static str {
    match error {
        giggles_pg_types::integer::IntegerError::Syntax => "22P02",
        giggles_pg_types::integer::IntegerError::Range => "22003",
        giggles_pg_types::integer::IntegerError::Limit => "54000",
    }
}

pub fn decode(b: &[u8], ty: &DataType, format: u16) -> R<Value> {
    decode_in_zone(b, ty, format, SessionZone::Fixed(0), 0)
}
pub fn decode_in_zone(
    b: &[u8],
    ty: &DataType,
    format: u16,
    zone: SessionZone,
    transaction_us: i64,
) -> R<Value> {
    if format == 0 {
        let s = std::str::from_utf8(b).map_err(|_| "22021")?;
        return match ty {
            DataType::RegType => Err("0A000"),
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
            DataType::SmallInt | DataType::Integer | DataType::BigInt
                if s.as_bytes().contains(&0) =>
            {
                Err("22P02")
            }
            DataType::SmallInt => giggles_pg_types::integer::parse_i16(s)
                .map(|v| Value::Int(v.into()))
                .map_err(integer_input_error),
            DataType::Integer => giggles_pg_types::integer::parse_i32(s)
                .map(|v| Value::Int(v.into()))
                .map_err(integer_input_error),
            DataType::BigInt => giggles_pg_types::integer::parse_i64(s)
                .map(Value::Int)
                .map_err(integer_input_error),
            DataType::Numeric => giggles_pg_types::Numeric::parse(s)
                .map(Value::Numeric)
                .map_err(numeric_error),
            DataType::Real => giggles_pg_types::Float4::parse(s)
                .map(Value::Float4)
                .map_err(numeric_error),
            DataType::DoublePrecision => giggles_pg_types::Float8::parse(s)
                .map(Value::Float8)
                .map_err(numeric_error),
            DataType::Date => giggles_core::relational::Date::parse(
                s,
                &giggles_pg_types::temporal::ParserSettings {
                    date_order: giggles_pg_types::temporal::DateOrder::Ymd,
                    offset_east: 0,
                },
            )
            .map(Value::Date)
            .map_err(temporal_error),
            DataType::Time => giggles_core::relational::PgTime::parse(s)
                .map(Value::Time)
                .map_err(temporal_error),
            DataType::TimeTz => {
                use giggles_pg_types::temporal::{
                    Date, DateOrder, ParserSettings, TimeTz, TimestampTz,
                };
                let settings = ParserSettings {
                    date_order: DateOrder::Ymd,
                    offset_east: 0,
                };
                let parsed = TimeTz::parse(s, &settings).map_err(temporal_error)?;
                let other = TimeTz::parse(
                    s,
                    &ParserSettings {
                        offset_east: 3600,
                        ..settings
                    },
                )
                .map_err(temporal_error)?;
                if parsed == other {
                    Ok(Value::TimeTz(parsed))
                } else {
                    // StatementClock uses PostgreSQL-epoch microseconds, like TimestampTz.
                    let instant =
                        TimestampTz::from_microseconds(transaction_us).map_err(temporal_error)?;
                    let local = zone.to_local(instant).map_err(temporal_error)?;
                    let days = i32::try_from(local.microseconds().div_euclid(86_400_000_000))
                        .map_err(|_| "22008")?;
                    let date = Date::from_days(days).map_err(temporal_error)?;
                    TimeTz::in_zone_on_date(parsed.time(), date, zone)
                        .map(Value::TimeTz)
                        .map_err(temporal_error)
                }
            }
            DataType::Timestamp => giggles_core::relational::Timestamp::parse(
                s,
                &giggles_pg_types::temporal::ParserSettings {
                    date_order: giggles_pg_types::temporal::DateOrder::Ymd,
                    offset_east: 0,
                },
            )
            .map(Value::Timestamp)
            .map_err(temporal_error),
            DataType::TimestampTz => giggles_core::relational::TimestampTz::parse_in_session(
                s,
                &giggles_pg_types::temporal::SessionSettings {
                    date_order: giggles_pg_types::temporal::DateOrder::Ymd,
                    zone,
                },
            )
            .map(Value::TimestampTz)
            .map_err(temporal_error),
            DataType::Interval => giggles_core::relational::Interval::parse(s)
                .map(Value::Interval)
                .map_err(temporal_error),
            DataType::Text | DataType::Varchar | DataType::Bpchar if s.as_bytes().contains(&0) => {
                Err("22021")
            }
            DataType::Text | DataType::Varchar | DataType::Bpchar => Ok(Value::Text(s.into())),
            DataType::Uuid
            | DataType::Jsonb
            | DataType::IntArray
            | DataType::BigIntArray
            | DataType::TextArray => {
                giggles_core::relational::logical_types::coerce(Value::Text(s.into()), ty)
                    .map_err(|_| "22P02")
            }
            DataType::Bytes => {
                use giggles_pg_types::bytea::{ByteaInputError as E, prepare_bytea_text};
                let map_error = |error| match error {
                    E::HexInvalidDigit | E::HexOddDigits => "22023",
                    E::EscapeSyntax => "22P02",
                    E::NulInput => "22021",
                    E::OutputLimit | E::Allocation => "54000",
                };
                prepare_bytea_text(s, b.len())
                    .map_err(map_error)?
                    .decode()
                    .map(Value::Bytes)
                    .map_err(map_error)
            }
        };
    }
    if ty.is_extended_text() {
        return logical_binary_input(ty, b);
    }
    let temporal_size = match ty {
        DataType::Date => Some(4),
        DataType::Time | DataType::Timestamp | DataType::TimestampTz => Some(8),
        DataType::TimeTz => Some(12),
        DataType::Interval => Some(16),
        _ => None,
    };
    if let Some(size) = temporal_size {
        if b.len() < size {
            return Err("08P01");
        }
        if b.len() > size {
            return Err("22P03");
        }
        if matches!(ty, DataType::TimeTz)
            && !(-57_599..=57_599).contains(&i32::from_be_bytes(b[8..12].try_into().unwrap()))
        {
            return Err("22009");
        }
    }
    match (ty, b.len()) {
        (DataType::Boolean, 0)
        | (DataType::SmallInt, 0..2)
        | (DataType::Integer, 0..4)
        | (DataType::BigInt, 0..8)
        | (DataType::Real, 0..4)
        | (DataType::DoublePrecision | DataType::Numeric, 0..8) => Err("08P01"),
        (DataType::Boolean, 1) => Ok(Value::Bool(b[0] != 0)),
        (DataType::SmallInt, 2) => Ok(Value::Int(i16::from_be_bytes(b.try_into().unwrap()).into())),
        (DataType::Integer, 4) => Ok(Value::Int(i32::from_be_bytes(b.try_into().unwrap()).into())),
        (DataType::BigInt, 8) => Ok(Value::Int(i64::from_be_bytes(b.try_into().unwrap()))),
        (DataType::Text | DataType::Varchar | DataType::Bpchar, _) => String::from_utf8(b.to_vec())
            .map(Value::Text)
            .map_err(|_| "22021"),
        (DataType::Numeric, _) => giggles_pg_types::Numeric::from_binary(b)
            .map(Value::Numeric)
            .map_err(numeric_error),
        (DataType::Real, _) => giggles_pg_types::Float4::from_binary(b)
            .map(Value::Float4)
            .map_err(numeric_error),
        (DataType::DoublePrecision, _) => giggles_pg_types::Float8::from_binary(b)
            .map(Value::Float8)
            .map_err(numeric_error),
        (DataType::Date, _) => giggles_core::relational::Date::from_binary(b)
            .map(Value::Date)
            .map_err(temporal_error),
        (DataType::Time, _) => giggles_core::relational::PgTime::from_binary(b)
            .map(Value::Time)
            .map_err(temporal_error),
        (DataType::TimeTz, _) => giggles_core::relational::TimeTz::from_binary(b)
            .map(Value::TimeTz)
            .map_err(temporal_error),
        (DataType::Timestamp, _) => giggles_core::relational::Timestamp::from_binary(b)
            .map(Value::Timestamp)
            .map_err(temporal_error),
        (DataType::TimestampTz, _) => giggles_core::relational::TimestampTz::from_binary(b)
            .map(Value::TimestampTz)
            .map_err(temporal_error),
        (DataType::Interval, _) => giggles_core::relational::Interval::from_binary(b)
            .map(Value::Interval)
            .map_err(temporal_error),
        (DataType::Bytes, _) => Ok(Value::Bytes(b.to_vec())),
        _ => Err("22P03"),
    }
}
fn temporal_error(e: giggles_pg_types::Error) -> &'static str {
    match e {
        giggles_pg_types::Error::Range => "22008",
        giggles_pg_types::Error::TimeZoneDisplacement => "22009",
        giggles_pg_types::Error::Binary => "22P03",
        giggles_pg_types::Error::Limit => "54000",
        giggles_pg_types::Error::Syntax
        | giggles_pg_types::Error::DivisionByZero
        | giggles_pg_types::Error::Unimplemented => "22007",
    }
}
fn numeric_error(e: giggles_pg_types::Error) -> &'static str {
    match e {
        giggles_pg_types::Error::Range
        | giggles_pg_types::Error::Limit
        | giggles_pg_types::Error::TimeZoneDisplacement => "22003",
        giggles_pg_types::Error::Binary => "22P03",
        giggles_pg_types::Error::Syntax
        | giggles_pg_types::Error::DivisionByZero
        | giggles_pg_types::Error::Unimplemented => "22P02",
    }
}
fn encode(
    v: &Value,
    ty: &DataType,
    format: u16,
    settings: &crate::pg_settings::Settings,
    zone: SessionZone,
) -> R<Option<Vec<u8>>> {
    if format > 1 {
        return Err("08P01");
    }
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
            Value::Date(v) => v.to_string().into_bytes(),
            Value::Time(v) => v.to_string().into_bytes(),
            Value::TimeTz(v) => v.to_string().into_bytes(),
            Value::Timestamp(v) => v.to_string().into_bytes(),
            Value::TimestampTz(v) => v.format_in_zone(zone).map_err(temporal_error)?.into_bytes(),
            Value::Interval(v) => {
                use giggles_pg_types::temporal::IntervalOutputStyle as Style;
                let style = match settings.intervalstyle.as_str() {
                    "postgres_verbose" => Style::PostgresVerbose,
                    "sql_standard" => Style::SqlStandard,
                    "iso_8601" => Style::Iso8601,
                    _ => Style::Postgres,
                };
                v.format_with_style(style).into_bytes()
            },
            Value::Numeric(v) => v.to_string().into_bytes(),
            Value::Float4(v) => v
                .to_text_with_extra_float_digits(settings.extra_float_digits)
                .map_err(numeric_error)?
                .into_bytes(),
            Value::Float8(v) => v
                .to_text_with_extra_float_digits(settings.extra_float_digits)
                .map_err(numeric_error)?
                .into_bytes(),
            Value::Int(v) => v.to_string().into_bytes(),
            Value::Text(s) => s.as_bytes().to_vec(),
            Value::Bytes(b) => format!("\\x{}", hex::encode(b)).into_bytes(),
        }));
    }
    Ok(Some(match (v, ty) {
        (Value::Text(s), ty) if ty.is_extended_text() => logical_binary_output(ty, s)?,
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
        (Value::Text(s), DataType::Text | DataType::Varchar | DataType::Bpchar) => {
            s.as_bytes().to_vec()
        }
        (Value::Numeric(v), DataType::Numeric) => v.to_binary(),
        (Value::Float4(v), DataType::Real) => v.to_binary().to_vec(),
        (Value::Float8(v), DataType::DoublePrecision) => v.to_binary().to_vec(),
        (Value::Date(v), DataType::Date) => v.to_binary().to_vec(),
        (Value::Time(v), DataType::Time) => v.to_binary().to_vec(),
        (Value::TimeTz(v), DataType::TimeTz) => v.to_binary().to_vec(),
        (Value::Timestamp(v), DataType::Timestamp) => v.to_binary().to_vec(),
        (Value::TimestampTz(v), DataType::TimestampTz) => v.to_binary().to_vec(),
        (Value::Interval(v), DataType::Interval) => v.to_binary().to_vec(),
        (Value::Bytes(b), DataType::Bytes) => b.clone(),
        _ => return Err("42804"),
    }))
}
#[allow(clippy::too_many_arguments)]
/// Encodes rows `start..end` of a released result as DataRow messages. It
/// takes only `Released`: core rows reach it only through the release
/// protocol (GD-0061 R3-314).
fn rows(
    settings: &crate::pg_settings::Settings,
    zone: SessionZone,
    result: &Released<SqlResult>,
    d: &Description,
    formats: &[u16],
    start: usize,
    end: usize,
    out: &mut Vec<u8>,
) -> R<()> {
    for row in &result.get().rows[start..end] {
        let mut b = vec![];
        b.extend((row.values.len() as u16).to_be_bytes());
        for (i, v) in row.values.iter().enumerate() {
            match encode(
                v,
                &d.columns.get(i).ok_or("XX000")?.data_type,
                format_at(formats, i),
                settings,
                zone,
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
/// The notices and CommandComplete of a released result (GD-0061 R3-314).
fn complete(command: &str, r: &Released<SqlResult>, out: &mut Vec<u8>) -> R<()> {
    let r = r.get();
    for message in giggles_core::relational::take_sql_warnings() {
        let mut body = vec![];
        for (field, value) in [
            (b'S', "WARNING"), (b'V', "WARNING"),
            (b'C', "22023"), (b'M', message.as_str()),
        ] {
            body.push(field);
            z(value, &mut body);
        }
        body.push(0);
        frame(b'N', &body, out)?;
    }
    for (message, detail, code) in giggles_core::relational::take_sql_notices() {
        let severity = if matches!(code, Some("22023" | "01000")) { "WARNING" } else { "NOTICE" };
        let mut body = vec![];
        for (field, value) in [
            (b'S', severity),
            (b'V', severity),
            (b'C', code.unwrap_or("00000")),
            (b'M', message.as_str()),
        ] {
            body.push(field);
            z(value, &mut body);
        }
        if let Some(detail) = detail {
            body.push(b'D');
            z(&detail, &mut body);
        }
        body.push(0);
        frame(b'N', &body, out)?;
    }
    let tag = match command {
        "SELECT" => format!("SELECT {}", r.rows.len()),
        "INSERT" => format!("INSERT 0 {}", r.affected),
        "UPDATE" | "DELETE" | "MERGE" => format!("{command} {}", r.affected),
        _ => command.into(),
    };
    let mut b = vec![];
    z(&tag, &mut b);
    frame(b'C', &b, out)
}
fn core<T>(r: Result<T, CoreError>) -> R<T> {
    r.map_err(|e| crate::service::sqlstate(&e))
}
fn catalog_plan(
    sql: &str,
    diagnostic: &mut Diagnostic,
    session: &mut Session,
) -> R<Option<giggles_pg_catalog::Plan>> {
    diagnostic.phase = "catalog planning";
    // Core owns application DML admission and authorization. This hint only
    // avoids catalog parsing; it neither binds names nor returns diagnostics.
    if Prepared::application_dml(sql, giggles_pg_catalog::catalog_identifier) {
        return Ok(None);
    }
    // Sequence and extension calls use the database execution context, never
    // the immutable catalog evaluator or the positive table-free allowlist.
    if !Prepared::sequence_dependencies(sql).is_empty() || Prepared::extension_routine_call(sql) {
        return Ok(None);
    }
    let planned = match giggles_pg_catalog::plan_gateway(sql) {
        Err(giggles_pg_catalog::Error::MixedRelations) => {
            let (epoch, tables, indexes, typmods, types) =
                core(session.catalog_snapshot_with_typmods())?;
            let snapshot = catalog_wire::snapshot(session, epoch, tables, indexes, typmods, types)?;
            let relations = catalog_wire::application_schemas(session, &snapshot);
            giggles_pg_catalog::plan_gateway_with_relations(sql, &relations)
        }
        other => other,
    };
    match planned {
        Ok(None) if Prepared::rejects_unsupported_table_free_scalar(sql) => {
            Err(catalog_wire::error(giggles_pg_catalog::Error::Unsupported))
        }
        // Preserve the finite application grammar's own diagnostics (for
        // example COPY rejection). Core still parses and authorizes the entire
        // original statement; no failed catalog plan is executed as metadata.
        Err(giggles_pg_catalog::Error::Syntax) => Ok(None),
        other => other.map_err(catalog_wire::error),
    }
}

/// Explicit text/varchar cast operands may use integer or string declarations:
/// decode with the declared wire type, then the catalog applies PostgreSQL text
/// output semantics. Every use of that parameter must have the explicit cast.
/// Otherwise overrides require no projected parameters and stay within one
/// family (integer/oid or name/text/varchar), as needed by Npgsql identifiers.
fn catalog_input_override(
    plan: &giggles_pg_catalog::Plan,
    parameter: usize,
    declared: u32,
    inferred: u32,
) -> bool {
    (plan.allows_text_cast_input_override(parameter)
        && matches!(declared, 20 | 21 | 23 | 25 | 1043)
        && matches!(inferred, 25 | 1043))
        || plan.allows_numeric_input_override()
            && ((matches!(declared, 20 | 21 | 23 | 26) && matches!(inferred, 20 | 21 | 23 | 26))
                || (matches!(declared, 19 | 25 | 1043) && matches!(inferred, 19 | 25 | 1043)))
}

fn sql_declared_type(ty: &DataType, declared: u32) -> Option<DataType> {
    if declared == 0 || declared == oid(ty) {
        return Some(ty.clone());
    }
    match (ty, declared) {
        (DataType::Text | DataType::Varchar, 25) => Some(DataType::Text),
        (DataType::Text | DataType::Varchar, 1043) => Some(DataType::Varchar),
        (DataType::SmallInt | DataType::Integer | DataType::BigInt, 21) => Some(DataType::SmallInt),
        (DataType::SmallInt | DataType::Integer | DataType::BigInt, 23) => Some(DataType::Integer),
        (DataType::SmallInt | DataType::Integer | DataType::BigInt, 20) => Some(DataType::BigInt),
        _ => None,
    }
}

fn core_declared_type(oid: u32) -> Option<DataType> {
    match oid {
        16 => Some(DataType::Boolean),
        17 => Some(DataType::Bytes),
        20 => Some(DataType::BigInt),
        21 => Some(DataType::SmallInt),
        23 => Some(DataType::Integer),
        25 => Some(DataType::Text),
        1043 => Some(DataType::Varchar),
        1042 => Some(DataType::Bpchar),
        2950 => Some(DataType::Uuid),
        3802 => Some(DataType::Jsonb),
        1007 => Some(DataType::IntArray),
        1016 => Some(DataType::BigIntArray),
        1009 => Some(DataType::TextArray),

        1700 => Some(DataType::Numeric),
        700 => Some(DataType::Real),
        701 => Some(DataType::DoublePrecision),
        1082 => Some(DataType::Date),
        1083 => Some(DataType::Time),
        1266 => Some(DataType::TimeTz),
        1114 => Some(DataType::Timestamp),
        1184 => Some(DataType::TimestampTz),
        1186 => Some(DataType::Interval),
        _ => None,
    }
}

fn prepare_declared_sql(session: &mut Session, sql: &str, oids: &[u32], unnamed: bool) -> R<(Plan, Description)> {
    prepare_declared_sql_diagnostic(&mut Diagnostic::default(), session, sql, oids, unnamed)
}

fn prepare_declared_sql_diagnostic(diagnostic: &mut Diagnostic, session: &mut Session, sql: &str, oids: &[u32], unnamed: bool) -> R<(Plan, Description)> {
    let declared = oids
        .iter()
        .map(|&o| core_declared_type(o))
        .collect::<Vec<_>>();
    let prepared = diagnostic.core(if unnamed { session.prepare_unnamed(sql, &declared) } else { session.prepare_with_parameter_types(sql, &declared) })?;
    let description = diagnostic.core(session.describe(&prepared))?;
    if oids.len() > description.parameters.len() {
        return Err("08P01");
    }
    Ok((Plan::Sql(Box::new(prepared)), description))
}

/// R3-207/615: `prepare_declared_sql` for the UPDATE wrapped by a protected
/// form, prepared inside the session's protected scope (`class` admitted first).
fn prepare_protected_sql(
    diagnostic: &mut Diagnostic,
    session: &mut Session,
    class: giggles_core::task::StatementClass,
    sql: &str,
    oids: &[u32],
) -> R<(Prepared, Description)> {
    let declared = oids
        .iter()
        .map(|&o| core_declared_type(o))
        .collect::<Vec<_>>();
    // A task refusal (the column rules) keeps its own message.
    let prepared = diagnostic.core(session.prepare_protected(class, sql, &declared))?;
    let description = core(session.describe(&prepared))?;
    if oids.len() > description.parameters.len() {
        return Err("08P01");
    }
    Ok((prepared, description))
}

#[derive(Clone)]
enum Plan {
    Explain(giggles_pg_catalog::ExplainQuery),
    Empty,
    Cursor(cursor::Command),
    AccessShare(crate::auth::statements::TableLockRequest),
    Admin(crate::auth::statements::AdminStatement),
    Control(crate::pg_transaction::Command),
    Settings(Vec<crate::pg_settings::Setting>),
    Inspect(crate::pg_settings::Show),
    SettingsFunction(crate::pg_settings::Function),
    Catalog(
        Box<giggles_pg_catalog::Plan>,
        Vec<giggles_pg_catalog::Field>,
        u64,
    ),
    Sql(Box<Prepared>),
    Copy(giggles_core::relational::Copy, Box<Prepared>),
    CopyOut(Box<crate::auth::statements::CopyOutput>),
    #[cfg(feature = "legacy-task")]
    Task(crate::task_statement::TaskStatement),
    /// GD-0061 R3-207: a protected form; task sessions only.
    Protected(Box<crate::agent::statement::ProtectedStatement>),
    /// GD-0061 R3-707: an approver form (SHOW CHANGES, SHOW CHANGE, APPROVE,
    /// REVOKE APPROVAL); sessions without a task only.
    Change(Box<giggles_core::task::statements::ChangeStatement>),
    /// GD-0061 R3-110: a task statement (CREATE TASK, REVOKE TASK, SHOW TASK,
    /// GRANT / REVOKE ... ON TASK); sessions without a task only.
    TaskDdl(Box<giggles_core::task::statements::TaskStatement>),
}
#[derive(Clone, PartialEq, Eq)]
struct ParseContext {
    settings: crate::pg_settings::Settings,
    role: Role,
    session_user: Option<String>,
}
impl ParseContext {
    fn matches(&self, settings: &crate::pg_settings::Settings, session: &Session) -> bool {
        &self.settings == settings && &self.role == session.authority_role()
            && self.session_user.as_deref() == session.session_user_name()
    }
    fn current(settings: &crate::pg_settings::Settings, session: &Session) -> Self {
        Self { settings: settings.clone(), role: session.authority_role().clone(),
            session_user: session.session_user_name().map(str::to_owned) }
    }
}
#[derive(Clone)]
struct Statement {
    // Only an unnamed application SELECT prepared outside a transaction gets a key.
    parse_context: Option<std::sync::Arc<ParseContext>>,
    description_version: Option<u64>,
    sql_bytes: usize,
    sql: String,
    declared_oids: Vec<u32>,
    p: Plan,
    d: Description,
    parameter_oids: Vec<u32>,
    query_id: Option<String>,
}
impl Statement {
    fn authorize_description(&self, session: &mut Session) -> R<()> {
        if let Plan::Sql(p) = &self.p
            && session.task().is_none()
        {
            // An aborted transaction reports 25P02 before any authorization,
            // as core Describe does.
            if session.is_aborted() && !p.allows_aborted_transaction() {
                return Err("25P02");
            }
            // Description caches follow the transaction snapshot; grants do not.
            // Reuse Bind's current authority check without acquiring relation locks.
            core(session.authority_scope(|session| {
                session.authorize_prepared_bind(p).map(|_| ())
            }))?;
        }
        Ok(())
    }

    fn describe(&self, session: &mut Session) -> R<Description> {
        match &self.p {
            #[cfg(feature = "legacy-task")]
            Plan::Task(_) => Ok(self.d.clone()),
            // A task session holds only relation-free catalog plans, which
            // read no catalog (R3-206).
            Plan::Catalog(..) if session.task().is_some() => Ok(self.d.clone()),
            Plan::Catalog(plan, _, epoch) => {
                let (current, _, _) = core(session.catalog_snapshot())?;
                if current != *epoch && !plan.refreshes_view_definitions() {
                    return Err("0A000");
                }
                Ok(self.d.clone())
            }
            Plan::Sql(p) => {
                if self.description_version.is_some()
                    && self.description_version == session.sql.statement_catalog_version()
                    && session.sql.prepared_is_current(p)
                { return Ok(self.d.clone()); }
                let d = core(session.describe(p))?;
                if self.d.columns.len() != d.columns.len()
                    || self.d.columns.iter().zip(&d.columns).any(|(old, new)| {
                        old.name != new.name || old.data_type != new.data_type
                    })
                    || self.d.type_oids != d.type_oids || self.d.typmods != d.typmods
                {
                    return Err("0A000");
                }
                Ok(d)
            },
            Plan::Copy(copy, p) => {
                core(session.copy_columns(copy, p))?;
                Ok(self.d.clone())
            }
            Plan::CopyOut(_) | Plan::Explain(_) | Plan::Cursor(_)
            | Plan::AccessShare(_)
            | Plan::Admin(_)
            | Plan::Empty
            | Plan::Settings(_)
            | Plan::Control(_)
            | Plan::SettingsFunction(_)
            | Plan::Inspect(_)
            | Plan::Protected(_)
            | Plan::Change(_)
            | Plan::TaskDdl(_) => Ok(self.d.clone()),
        }
    }
}
struct Portal {
    s: Statement,
    params: Vec<Value>,
    formats: Vec<u16>,
    /// GD-0061 R3-310: a retained result is released (masked and charged)
    /// once, when it is produced; later slices only revalidate.
    result: Option<Released<SqlResult>>,
    // v2 RETURNING rows past the in-memory threshold, read once in order.
    spilled: Option<giggles_core::relational::spill::SpilledRows>,
    cursor: Option<giggles_core::Cursor>,
    catalog_params: Vec<giggles_pg_catalog::Value>,
    catalog_result: Option<Released<giggles_pg_catalog::ResultSet>>,
    parameter_bytes: usize,
    at: usize,
}
impl Portal {
    fn retained_bytes(&self) -> R<usize> {
        let sql = self
            .result
            .as_ref()
            .map(|r| serde_json::to_vec(r.get()).map(|b| b.len()))
            .transpose()
            .map_err(|_| "54000")?
            .unwrap_or(0);
        let catalog = self
            .catalog_result
            .as_ref()
            .map(|r| catalog_wire::retained_bytes(r.get()))
            .transpose()?
            .unwrap_or(0);
        self.parameter_bytes
            .checked_add(sql)
            .and_then(|n| n.checked_add(catalog))
            .ok_or("54000")
    }
}
/// The marker of a portal that ran and returned no rows: a server-built
/// result, never core rows.
fn ran() -> Option<Released<SqlResult>> {
    Some(empty())
}
/// The empty result of a completion the server builds itself: no rows.
fn empty() -> Released<SqlResult> {
    Released::synthetic(SqlResult::default())
}
/// GD-0061 R3-310: before a later slice (`at > 0`) of a retained result its
/// rows are re-served, not released again, so only the task's validity is
/// checked (revoked, expired, definition changed). No-op outside a task.
fn revalidate(session: &Session, at: usize, diagnostic: &mut Diagnostic) -> R<()> {
    if at > 0 {
        diagnostic.core(session.still_valid())?;
    }
    Ok(())
}
fn catalog_complete(count: usize, out: &mut Vec<u8>) -> R<()> {
    let mut b = Vec::new();
    z(&format!("SELECT {count}"), &mut b);
    frame(b'C', &b, out)
}
const CURSOR_ROWS: usize = 256;
const CURSOR_BYTES: usize = giggles_core::relational::MAX_RESULT_BYTES;
struct OutputChannel {
    sender: tokio::sync::mpsc::Sender<Vec<u8>>,
    prefix: std::sync::Mutex<Vec<u8>>,
}
type Output = Option<OutputChannel>;
fn flush_output(output: &Output, out: &mut Vec<u8>) -> R<()> {
    if let Some(output) = output {
        let mut prefix = output.prefix.lock().unwrap();
        if !prefix.is_empty() {
            output
                .sender
                .blocking_send(std::mem::take(&mut *prefix))
                .map_err(|_| "08006")?;
        }
        if !out.is_empty() {
            output
                .sender
                .blocking_send(std::mem::take(out))
                .map_err(|_| "08006")?;
        }
    }
    Ok(())
}
#[cfg(test)]
thread_local! {
    static PREPARE_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}
struct State {
    output: Output,
    diagnostic: Diagnostic,
    admin_completed: bool,
    implicit: bool,
    settings: crate::pg_settings::SessionSettings,
    statements: BTreeMap<String, Statement>,
    unnamed_cache: Vec<Statement>,
    named_statement_bytes: usize,
    portals: BTreeMap<String, Portal>,
    cursors: cursor::Cursors,
    drain: bool,
    copy: Option<CopyIn>,
    copy_completion: Vec<u8>,
    copy_active: Option<crate::cancel::Active>,
    copy_permit: Option<crate::admission::RolePermit>,
    execution_admission: Option<ExecutionAdmission>,
}
pub(crate) fn admission_exempt(sql: &str) -> bool {
    simple::split(sql).is_ok_and(|statements| statements.iter().all(|sql| {
        crate::pg_transaction::parse(sql).ok().flatten()
            .is_some_and(|command| crate::admission::control_exempt(&command))
    }))
}
// A Simple Query batch may contain recovery controls followed by ordinary SQL.
// Release both pools between its statements before waiting for the next role
// permit. Waiting here uses a Tokio worker but never a shared blocking-pool slot.
// Fields drop in declaration order, including when execution unwinds.
struct BlockingOwner<S, T, A, L, U> {
    session: S,
    state: T,
    _agent: A,
    _live: L,
    _submitted: U,
}

#[cfg(test)]
mod blocking_owner_tests {
    #[test]
    fn unwind_keeps_tracking_until_session_state_and_agent_retire() {
        use std::sync::{Arc, Mutex};
        struct Record(u8, Arc<Mutex<Vec<u8>>>);
        impl Drop for Record {
            fn drop(&mut self) { self.1.lock().unwrap().push(self.0); }
        }
        let order = Arc::new(Mutex::new(Vec::new()));
        let copy = order.clone();
        assert!(std::panic::catch_unwind(move || {
            let _owner = super::BlockingOwner {
                session: Record(0, copy.clone()), state: Record(1, copy.clone()),
                _agent: Record(2, copy.clone()), _live: Record(3, copy.clone()),
                _submitted: Record(4, copy),
            };
            panic!("execution unwind");
        }).is_err());
        assert_eq!(*order.lock().unwrap(), [0, 1, 2, 3, 4]);
    }
}

struct ExecutionAdmission {
    role_permit: Option<crate::admission::RolePermit>,
    blocking: Option<tokio::sync::OwnedSemaphorePermit>,
    runtime: tokio::runtime::Handle,
    context: giggles_core::Cancellation,
    first: bool,
}
impl ExecutionAdmission {
    fn statement(&mut self, sql: &str, session: &Session) -> Result<(), crate::admission::StatementRefusal> {
        if std::mem::take(&mut self.first) { return Ok(()); }
        self.role_permit = None;
        self.blocking = None;
        self.runtime.block_on(async {
            if !session.is_aborted() && !admission_exempt(sql) {
                self.role_permit = Some(session.service.admission.statement(&session.role, Some(&self.context)).await?);
            }
            self.blocking = Some(session.service.blocking.admit(&self.context).await
                .map_err(crate::admission::StatementRefusal::Core)?);
            Ok(())
        })
    }
}
impl State {
    fn admission_exempt(&self, tag: u8, body: &[u8]) -> bool {
        let mut reader = Reader { b: body, at: 0 };
        let Ok(text) = reader.text() else { return false; };
        if tag == b'Q' {
            return simple::split(&text).is_ok_and(|statements| {
                statements.first().is_none_or(|sql| admission_exempt(sql))
            });
        }
        self.portals.get(&text).is_some_and(|portal| {
            matches!(&portal.s.p, Plan::Control(command) if crate::admission::control_exempt(command))
        })
    }
}
fn statement_deadline(settings: &crate::pg_settings::Settings) {
    if let Some(context) = giggles_core::Cancellation::current() {
        context.set_lock_timeout((settings.lock_timeout > 0).then(|| std::time::Duration::from_millis(settings.lock_timeout)));
    }
    if settings.statement_timeout > 0 && let Some(context) = giggles_core::Cancellation::current() {
        context.deadline(std::time::Instant::now() + std::time::Duration::from_millis(settings.statement_timeout));
    }
}
fn begin_implicit(
    session: &mut Session,
    implicit: &mut bool,
    settings: &mut crate::pg_settings::SessionSettings,
) -> R<()> {
    if !session.in_transaction() {
        session.set_default_isolation(&settings.current);
        core(session.begin_implicit_block())?;
        settings.transaction(false, true, "BEGIN");
        *implicit = true;
    }
    Ok(())
}
// Bind and Execute must classify core control plans alike. Opening an
// implicit transaction for a control plan changes its profile semantics before
// re-preparation or execution (for example, turning a refusal into a rollback).
fn needs_implicit_transaction(prepared: &Prepared) -> bool {
    !matches!(prepared.command(), "BEGIN" | "COMMIT" | "ROLLBACK")
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
) -> R<(Description, Released<SqlResult>)> {
    if !session.role.is_sql() {
        return Err("42501");
    }
    if session.is_aborted() {
        return Err("25P02");
    }
    let (name, mut value) = show.value(settings);
    if show == crate::pg_settings::Show::SessionAuthorization && value.is_empty() { value = session.session_user_name().unwrap_or("giggles").into(); }
    Ok((
        Description {
            type_oids: vec![],
            typmods: vec![-1],
            parameters: vec![],
            columns: vec![giggles_core::relational::ColumnDef {
                id: 0,
                name: name.into(),
                data_type: DataType::Text,
                not_null: true,
            }],
        },
        // A setting's value, built here: never core rows.
        Released::synthetic(SqlResult {
            columns: vec![name.into()],
            rows: vec![giggles_core::relational::SqlRow {
                values: vec![Value::Text(value)],
                version: 0,
            }],
            affected: 0,
        }),
    ))
}
fn execute_control(
    command: &'static str, session: &mut Session, implicit: &mut bool,
    settings: &mut crate::pg_settings::SessionSettings, out: &mut Vec<u8>,
) -> R<&'static str> {
    execute_control_inner(command, session, implicit, settings, out, false)
}
fn execute_implicit_commit(
    session: &mut Session, implicit: &mut bool,
    settings: &mut crate::pg_settings::SessionSettings, out: &mut Vec<u8>,
) -> R<&'static str> {
    execute_control_inner("COMMIT", session, implicit, settings, out, true)
}
fn execute_control_inner(
    command: &'static str,
    session: &mut Session,
    implicit: &mut bool,
    settings: &mut crate::pg_settings::SessionSettings,
    out: &mut Vec<u8>,
    implicit_commit: bool,
) -> R<&'static str> {
    if !session.role.is_sql() {
        return Err("42501");
    }
    let before = settings.current.clone();
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
            session.set_default_isolation(&settings.current);
            core(session.run_control("BEGIN"))?;
        }
    } else if was && let Err(error) = if implicit_commit && *implicit && actual == "COMMIT" {
        session.commit_implicit_block()
    } else { session.run_control(actual) } {
        session.sql.rollback();
        settings.transaction(true, false, "ROLLBACK");
        *implicit = false;
        return Err(crate::service::sqlstate(&error));
    }
    *implicit = false;
    settings.transaction(was, session.in_transaction(), actual);
    if !session.in_transaction() { session.service.advisory.release(session.backend_pid, true); }
    settings_status(&before, settings, session, out)?;
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
        // Settings also change through set_config and transaction recovery. Keep
        // the execution clock's zone aligned on every dispatch exit, including errors.
        session.set_time_zone(&self.settings.current.timezone);
        session.sync_role_settings(&mut self.settings.current)?;
        session.set_search_path(&self.settings.current.search_path);
        session.set_extra_float_digits(self.settings.current.extra_float_digits);
        session.set_max_value_bytes(self.settings.current.max_value_bytes);
        session.set_max_statement_tokens(self.settings.current.max_statement_tokens);
        session.set_default_isolation(&self.settings.current);
        if tag == b'E' { statement_deadline(&self.settings.current); }
        let result = self.handle_inner(tag, b, session, out, received);
        if !session.in_transaction() {
            if !self.implicit { session.service.advisory.release(session.backend_pid, true); }
            self.cursors
                .transaction_end(if result.is_ok() { "COMMIT" } else { "ROLLBACK" });
        }
        session.set_time_zone(&self.settings.current.timezone);
        session.sync_role_settings(&mut self.settings.current)?;
        session.set_search_path(&self.settings.current.search_path);
        session.set_extra_float_digits(self.settings.current.extra_float_digits);
        session.set_max_value_bytes(self.settings.current.max_value_bytes);
        session.set_max_statement_tokens(self.settings.current.max_statement_tokens);
        if result.is_ok() { simple::role_notices(session, &self.settings.current, out)?; }
        result
    }
    fn prepare_statement(
        &mut self,
        sql: &str,
        oids: Vec<u32>,
        session: &mut Session,
        unnamed: bool,
    ) -> R<Statement> {
        #[cfg(test)]
        PREPARE_CALLS.with(|calls| calls.set(calls.get() + 1));
        self.invalidate_unnamed_for_sql(sql);
        let before_version = session.sql.statement_catalog_version();
        // GD-0061 R3-206: a task session admits the class before any catalog
        // or sequence read, on its own path.
        if session.task().is_some() {
            session
                .sql
                .set_task_entry(giggles_core::task::readlog::ENTRY_PGWIRE_EXTENDED);
            return self.task_prepare(sql, oids, session);
        }
        // GD-0061 R3-707: an approver form, before any other grammar reads it.
        if let Some(change) = crate::agent::preview::approver_statement(sql)? {
            // The forms take no parameters.
            if !oids.is_empty() {
                return Err("08P01");
            }
            let d = crate::agent::preview::approver_description(&change);
            return Ok(Statement {
                parse_context: None,
                description_version: None,
                sql_bytes: sql.len(),
                sql: sql.to_owned(),
                declared_oids: oids,
                p: Plan::Change(Box::new(change)),
                d,
                parameter_oids: vec![],
                query_id: self.diagnostic.query_id.clone(),
            });
        }
        // GD-0061 R3-110: a task statement, before the role parser, which
        // claims every GRANT and REVOKE head.
        if let Some(stmt) = crate::task_ddl::parse(sql) {
            if let giggles_core::task::statements::TaskStatement::Error(code) = stmt {
                return Err(code);
            }
            // The statements take no parameters.
            if !oids.is_empty() {
                return Err("08P01");
            }
            let d = crate::task_ddl::description(&stmt);
            return Ok(Statement {
                parse_context: None,
                description_version: None,
                sql_bytes: sql.len(),
                sql: sql.to_owned(),
                declared_oids: oids,
                p: Plan::TaskDdl(Box::new(stmt)),
                d,
                parameter_oids: vec![],
                query_id: self.diagnostic.query_id.clone(),
            });
        }
        if !is_copy_sql(sql) {
            core(session.authorize_sequence_sql(sql))?;
        }
        let mut plain_application = false;
        let (mut p, mut d) = if let Some(tables) =
            crate::auth::statements::access_share_tables(sql)?
        {
            (
                Plan::AccessShare(tables),
                Description {
                    type_oids: vec![],
                    typmods: vec![],
                    columns: vec![],
                    parameters: vec![],
                },
            )
        } else if let Some(options) = self.copy_output_options(sql)? {
            self.copy_destination_privilege(session, &options.destination)?;
            self.prepare_copy_output(&options, session)?;
            (Plan::CopyOut(Box::new(options)), Description { type_oids: vec![], typmods: vec![], columns: vec![], parameters: vec![] })
        } else if is_copy_sql(sql) {
            let (copy, prepared) = self.prepare_copy_checked(sql, session)?;
            (
                Plan::Copy(copy, Box::new(prepared)),
                Description {
                    type_oids: vec![],
                    typmods: vec![],
                    columns: vec![],
                    parameters: vec![],
                },
            )
        } else if let Some(mut command) = cursor::parse(sql)? {
            if !session.role.is_sql() {
                return Err("42501");
            }
            let d = self.cursors.describe(&mut command, session, &oids, &mut self.diagnostic)?;
            (Plan::Cursor(command), d)
        } else if let Some(stmt) = crate::auth::statements::parse(sql) {
            (
                Plan::Admin(stmt),
                Description {
                    type_oids: vec![],
                    typmods: vec![],
                    columns: vec![],
                    parameters: vec![],
                },
            )
        } else if crate::pg_settings::empty_sql(sql)? {
            (
                Plan::Empty,
                Description {
                    type_oids: vec![],
                    typmods: vec![],
                    columns: vec![],
                    parameters: vec![],
                },
            )
        } else if let Some(command) = parse_control(sql, session, &mut self.diagnostic)? {
            if !session.role.is_sql() {
                return Err("42501");
            }
            (
                Plan::Control(command),
                Description {
                    type_oids: vec![],
                    typmods: vec![],
                    columns: vec![],
                    parameters: vec![],
                },
            )
        } else if let Some(show) = show_settings(sql, session, &mut self.diagnostic)? {
            let (description, _) = inspect(show, &self.settings.current, session)?;
            (Plan::Inspect(show), description)
        } else if let Some(settings) = parse_settings(sql, session, &mut self.diagnostic)? {
            if !session.role.is_sql() {
                return Err("42501");
            }
            if settings.len() != 1 {
                return Err("42601");
            }
            (
                Plan::Settings(settings),
                Description {
                    type_oids: vec![],
                    typmods: vec![],
                    columns: vec![],
                    parameters: vec![],
                },
            )
        } else if cfg!(feature = "legacy-task") && sql
            .trim_start_matches(|c: char| c.is_ascii_whitespace())
            .starts_with("GIGGLES")
        {
            #[cfg(not(feature = "legacy-task"))]
            { return Err("42601"); }
            #[cfg(feature = "legacy-task")]
            {
                if !matches!(session.role, Role::Task { .. }) {
                    return Err("42501");
                }
                let task = crate::task_statement::TaskStatement::parse(sql)?;
                (Plan::Task(task), task.description())
            }
        } else if let Some(function) =
            settings_functions::parse(sql, session, &mut self.diagnostic)?
        {
            let description = settings_functions::description(&function);
            (Plan::SettingsFunction(function), description)
        } else if !session.role.is_sql() && Prepared::rejects_unsupported_table_free_scalar(sql) {
            // DD-0068: authority precedes routine classification. Sessions without
            // SQL authority (task bindings) must not learn whether a routine
            // exists; they get the same 42501 that application prepare gives them.
            self.diagnostic.phase = "application prepare";
            return Err("42501");
        } else if let Some(query) = explain::prepare(sql, session)? {
            { let description = explain::description(&query); (Plan::Explain(query), description) }
        } else if let Some(plan) = catalog_plan(sql, &mut self.diagnostic, session)? {
            let (epoch, _, _) = core(session.catalog_snapshot())?;
            let fields = giggles_pg_catalog::describe(&plan).map_err(catalog_wire::error)?;
            let d = Description {
                type_oids: vec![],
                typmods: vec![-1; fields.len()],
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
            plain_application = true;
            self.diagnostic.phase = "application prepare";
            prepare_declared_sql_diagnostic(&mut self.diagnostic, session, sql, &oids, unnamed)?
        };
        if oids.len() > d.parameters.len() {
            return Err("08P01");
        }
        let mut parameter_oids: Vec<u32> = match &p {
            Plan::Catalog(plan, _, _) => plan.parameter_types().iter().map(|t| t.oid()).collect(),
            _ => d.parameters.iter().map(oid).collect(),
        };
        // Retry only the catalog declaration mismatch that would otherwise be
        // 0A000. Keep catalog validation and successful numeric overrides intact.
        if let Plan::Catalog(plan, _, _) = &p {
            let mismatch =
                oids.iter()
                    .zip(&parameter_oids)
                    .enumerate()
                    .any(|(i, (o, inferred))| {
                        *o != 0 && o != inferred && !catalog_input_override(plan, i, *o, *inferred)
                    });
            if session.role.is_sql()
                && mismatch
                && oids.iter().all(|o| {
                    *o == 0
                        || core_declared_type(*o).is_some()
                        || catalog_wire::parameter_type(*o).is_ok()
                })
            {
                let was_aborted = session.is_aborted();
                match prepare_declared_sql_diagnostic(&mut self.diagnostic, session, sql, &oids, unnamed) {
                    Ok((application, description))
                        if oids.len() <= description.parameters.len()
                            && oids
                                .iter()
                                .zip(&description.parameters)
                                .all(|(o, ty)| sql_declared_type(ty, *o).is_some()) =>
                    {
                        p = application;
                        d = description;
                        parameter_oids = d.parameters.iter().map(oid).collect();
                    }
                    Err(error) if !was_aborted && session.is_aborted() => {
                        return Err(error);
                    }
                    _ => {}
                }
            }
        }
        self.diagnostic.phase = "parameter type declaration";
        for (i, (o, t)) in oids.iter().copied().zip(&mut d.parameters).enumerate() {
            if matches!(p, Plan::Catalog(..)) {
                if o != 0 {
                    catalog_wire::parameter_type(o)?;
                    // The catalog compiler fixes parameter and result
                    // types together. A wire override must not silently
                    // change only ParameterDescription while leaving a
                    // projected parameter's output type unchanged.
                    if o != parameter_oids[i] {
                        let safe = matches!(&p, Plan::Catalog(plan, _, _)
                            if catalog_input_override(plan, i, o, parameter_oids[i]));
                        if !safe {
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
                *t = sql_declared_type(t, o).ok_or("0A000")?;
            }
            if o != 0 {
                parameter_oids[i] = o;
            }
        }
        let description_version = before_version.filter(|version| {
            matches!(&p, Plan::Sql(plan) if !plan.is_ddl())
                && session.sql.statement_catalog_version() == Some(*version)
        });
        let parse_context = (unnamed && description_version.is_some()
            && !session.in_transaction() && !session.agent && session.task().is_none()
            && matches!(session.role, Role::Database { .. })
            && plain_application
            && matches!(&p, Plan::Sql(p) if !p.is_ddl() && p.command() == "SELECT"))
            .then(|| std::sync::Arc::new(ParseContext::current(&self.settings.current, session)));
        Ok(Statement {
            parse_context,
            description_version,
            sql_bytes: sql.len(),
            sql: sql.to_owned(),
            declared_oids: oids,
            p,
            d,
            parameter_oids,
            query_id: self.diagnostic.query_id.clone(),
        })
    }
    fn reuse_unnamed(&mut self, sql: &str, oids: &[u32], session: &mut Session) -> R<Option<Statement>> {
        #[cfg(test)]
        if sql.contains("r3_parse_source") { UNNAMED_REUSE_MISSES.fetch_add(1, std::sync::atomic::Ordering::Relaxed); }
        #[cfg(test)]
        if sql.contains("r3_parse_source") && UNNAMED_REUSE_DISABLED.load(std::sync::atomic::Ordering::Relaxed) { return Ok(None); }
        if session.in_transaction() || session.is_aborted() || session.agent || session.task().is_some() {
            return Ok(None);
        }
        let Some(index) = self.unnamed_cache.iter().position(|entry|
            entry.sql == sql && entry.declared_oids == oids) else { return Ok(None); };
        let old = &self.unnamed_cache[index];
        let Plan::Sql(p) = &old.p else { return Ok(None); };
        if !old.parse_context.as_ref().is_some_and(|context| context.matches(&self.settings.current, session))
            || p.is_ddl() || !session.sql.prepared_is_current(p) {
            #[cfg(test)]
            if sql.contains("r3_parse_source") { UNNAMED_CONTEXT_MISSES.fetch_add(1, std::sync::atomic::Ordering::SeqCst); }
            self.unnamed_cache.clear();
            return Ok(None);
        }
        let Some(version) = old.description_version else { return Ok(None); };
        if !matches!(session.sql.prepared_catalog_state(), giggles_core::PreparedCatalogState::Version(Some(v)) if v == version)
            || session.sql.committed_catalog_version() != Some(version) {
            self.unnamed_cache.clear();
            return Ok(None);
        }
        #[cfg(test)]
        if sql.contains("r3_parse_source") {
            let hook = UNNAMED_BEFORE_REAUTHORIZE.lock().unwrap().take();
            if let Some(hook) = hook { hook(); }
        }
        // Preserve the dispatch chain's first authorization, including sequence errors.
        if session.authorize_sequence_sql(sql).is_err() { return Ok(None); }
        // SELECT authorization proves visibility of every application dependency;
        // catalog_plan returned None for this unchanged SQL, so skipping it cannot
        // expose catalog relations or bypass their visibility checks.
        // Re-run the application path's live authority checks. Any error or drift
        // takes the full path, which owns error ordering and diagnostic fields.
        if session.reauthorize_unnamed_parse(sql, p).is_err() {
            self.unnamed_cache.clear();
            return Ok(None);
        }
        if session.sql.committed_catalog_version() != Some(version) {
            self.unnamed_cache.clear();
            return Ok(None);
        }
        giggles_core::relational::take_sql_error_message();
        giggles_core::relational::take_sql_error_detail();
        giggles_core::relational::take_sql_error_hint();
        giggles_core::relational::take_sql_warnings();
        let mut statement = old.clone();
        statement.query_id = self.diagnostic.query_id.clone();
        self.diagnostic.phase = "parameter type declaration";
        #[cfg(test)]
        if sql.contains("r3_parse_source") {
            UNNAMED_REUSE_HITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            UNNAMED_REUSE_MISSES.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        }
        Ok(Some(statement))
    }
    fn invalidate_unnamed_for_sql(&mut self, sql: &str) {
        // Conservative invalidation also covers SET/RESET ROLE, authorization,
        // DISCARD (including unsupported forms), and all transaction controls.
        if !sql.trim_start().get(..6).is_some_and(|head| head.eq_ignore_ascii_case("SELECT")) {
            self.unnamed_cache.clear();
        }
    }
    fn cache_unnamed(&mut self, statement: &Statement) {
        const SQL_BYTES: usize = 64 * 1024;
        if statement.parse_context.is_none() || statement.sql_bytes > SQL_BYTES { return; }
        self.unnamed_cache.retain(|old| old.sql != statement.sql || old.declared_oids != statement.declared_oids);
        while self.unnamed_cache.len() >= 16
            || self.unnamed_cache.iter().map(|old| old.sql_bytes).sum::<usize>() + statement.sql_bytes > SQL_BYTES {
            self.unnamed_cache.remove(0);
        }
        self.unnamed_cache.push(statement.clone());
    }
    fn refresh_statement(&mut self, name: &str, session: &mut Session) -> R<()> {
        let old = self.statements.get(name).ok_or("26000")?;
        let stale = match &old.p {
            Plan::Sql(p) if session.task().is_none() && !p.is_ddl() => {
                !session.sql.prepared_is_current(p)
                    || match session.sql.prepared_catalog_state() {
                        giggles_core::PreparedCatalogState::PrivateDdl => true,
                        giggles_core::PreparedCatalogState::Version(version) =>
                            old.description_version.is_none() || old.description_version != version,
                        giggles_core::PreparedCatalogState::UnknownBackend =>
                            old.description_version.is_some_and(|version| session.sql.committed_catalog_version()
                                .is_some_and(|current| current != version)),
                    }
            }
            Plan::Sql(p) => !session.sql.prepared_is_current(p),
            Plan::Copy(_, p) => !session.sql.prepared_is_current(p),
            Plan::Catalog(_, _, _) if session.task().is_some() => false,
            Plan::Catalog(plan, _, epoch) if !plan.depends_on_relations() => {
                let (current, _, _) = core(session.catalog_snapshot())?;
                current != *epoch
            }
            _ => false,
        };
        if !stale {
            return Ok(());
        }
        let old = old.clone();
        let new = self.prepare_statement(&old.sql, old.declared_oids.clone(), session, false)?;
        if old.parameter_oids != new.parameter_oids
            || old.d.columns.len() != new.d.columns.len()
            || old.d.columns.iter().zip(&new.d.columns).any(|(old, new)| {
                // Rebinding can change storage identities without changing the
                // result shape promised to the client.
                old.name != new.name || old.data_type != new.data_type
            })
            || old.d.type_oids != new.d.type_oids
            || old.d.typmods != new.d.typmods
        {
            self.diagnostic.message = Some("cached plan must not change result type");
            return Err("0A000");
        }
        self.statements.insert(name.to_owned(), new);
        Ok(())
    }
    fn handle_inner(
        &mut self,
        tag: u8,
        b: &[u8],
        session: &mut Session,
        out: &mut Vec<u8>,
        received: std::time::Instant,
    ) -> R<bool> {
        if matches!(tag, b'H' | b'S') && self.copy.is_none() {
            out.append(&mut self.copy_completion);
        }
        if self.copy.is_some() {
            // Remaining statements after COPY belong to the same bounded batch.
            let output = self.output.take();
            let result = self.copy_message(tag, b, session, out, received);
            self.output = output;
            return result;
        }
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
                let before = self.settings.current.clone();
                if self.implicit {
                    self.implicit = false;
                    if session.is_aborted() {
                        session.sql.rollback();
                        self.cursors.transaction_end("ROLLBACK");
                        self.settings.transaction(true, false, "ROLLBACK");
                    } else {
                        let commit = if session.in_transaction() { session.commit_implicit_block() } else { Ok(()) };
                        if let Err(error) = commit {
                            session.sql.rollback();
                            self.cursors.transaction_end("ROLLBACK");
                            self.settings.transaction(true, false, "ROLLBACK");
                            self.portals.clear();
                            if matches!(&error, CoreError::WriteFenced { .. }) {
                                self.diagnostic.wire_message = Some(error.to_string());
                            }
                            return Err(crate::service::sqlstate(&error));
                        }
                        self.settings.transaction(true, false, "COMMIT");
                    }
                }
                if !session.in_transaction() {
                    self.portals.clear();
                }
                settings_status(&before, &self.settings, session, out)?;
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
                self.invalidate_unnamed_for_sql(&sql);
                self.simple_query(&sql, session, out, received)?;
                if !session.in_transaction() {
                    self.portals.clear();
                }
                if self.copy.is_none() {
                    ready(session, out)?;
                }
            }
            b'P' => {
                let name = r.text()?;
                let sql = r.text()?;
                self.diagnostic.query(&sql);
                let n = r.u16()?;
                if n > giggles_core::relational::MAX_PARAMS {
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
                if !name.is_empty()
                    && (self.statements.keys().filter(|name| !name.is_empty()).count()
                        >= MAX_NAMED_STATEMENTS
                        || self.named_statement_bytes.checked_add(sql.len()).ok_or("54000")? > MAX_NAMED_STATEMENT_BYTES)
                {
                    return Err("54000");
                }
                let reused = if name.is_empty() { self.reuse_unnamed(&sql, &oids, session)? } else { None };
                let statement = match reused {
                    Some(statement) => statement,
                    None => self.prepare_statement(&sql, oids, session, name.is_empty())?,
                };
                if name.is_empty() {
                    self.cache_unnamed(&statement);
                }
                if !name.is_empty() {
                    self.named_statement_bytes += sql.len();
                }
                self.statements.insert(
                    name,
                    statement,
                );
                frame(b'1', &[], out)?;
            }
            b'B' => {
                let name = r.text()?;
                let stmt = r.text()?;
                let s = self.statements.get(&stmt).ok_or("26000")?;
                if let Plan::Sql(p) = &s.p
                    && session.task().is_none()
                    && needs_implicit_transaction(p)
                {
                    if !p.is_ddl() {
                        begin_implicit(session, &mut self.implicit, &mut self.settings)?;
                        session.sql.begin_prepared_statement();
                    }
                    core(session.bind_prepared_relations(p))?;
                }
                // One version comparison, after locks have stabilized dependencies.
                self.refresh_statement(&stmt, session)?;
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
                let mut transaction_us = None;
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
                        decode_in_zone(
                            r.take(usize::try_from(n).map_err(|_| "54000")?)?,
                            ty,
                            format_at(&fmts, i),
                            session.zone().unwrap_or(SessionZone::Fixed(0)),
                            if matches!(ty, DataType::TimeTz) && format_at(&fmts, i) == 0 {
                                *transaction_us
                                    .get_or_insert_with(|| session.copy_clock().transaction_us)
                            } else {
                                0
                            },
                        )?
                    });
                }
                #[cfg(feature = "legacy-task")]
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
                if !name.is_empty() && !self.portals.contains_key(&name) && self.portals.keys().filter(|name| !name.is_empty()).count() >= MAX_NAMED_PORTALS {
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
                if let Plan::Sql(p) = &s.p {
                    core(session.sql.validate_immutable_constants(p))?;
                }
                self.portals.insert(
                    name,
                    Portal {
                        s,
                        params,
                        formats: output,
                        result: None,
                        spilled: None,
                        cursor: None,
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
                        if session.task().is_none() {
                            session.sql.begin_prepared_statement();
                        }
                        self.statements.get(&name).ok_or("26000")?.authorize_description(session)?;
                        self.refresh_statement(&name, session)?;
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
                        if let Some(d) = self.cursors.description(&name) {
                            if !session.role.is_sql() {
                                return Err("42501");
                            }
                            if session.is_aborted() {
                                return Err("25P02");
                            }
                            row_description(d, &[], out)?;
                            return Ok(true);
                        }
                        let p = self.portals.get(&name).ok_or("34000")?;
                        self.diagnostic.query_id = p.s.query_id.clone();
                        p.s.authorize_description(session)?;
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
                // GD-0061 R3-206: a task session's portal runs on its own path.
                if session.task().is_some() {
                    return self.task_execute(&name, max, retained, session, out).map(|()| true);
                }
                let p = self.portals.get_mut(&name).ok_or("34000")?;
                self.diagnostic.query_id = p.s.query_id.clone();
                p.s.describe(session)?;
                if let Plan::Cursor(command) = &p.s.p {
                    if p.result.is_some() {
                        return Err("55000");
                    }
                    self.cursors.execute(
                        command,
                        session,
                        &p.params,
                        &p.formats,
                        &mut self.settings,
                        &mut self.diagnostic,
                        false,
                        &self.output,
                        out,
                    )?;
                    p.result = ran();
                    return Ok(true);
                }
                if let Plan::CopyOut(options) = &p.s.p {
                    if p.result.is_some() { return Err("55000"); }
                    let options = options.clone();
                    p.result = ran();
                    self.copy_stdout(&options, session, out)?;
                    return Ok(true);
                }
                if let Plan::Copy(copy, prepared) = &p.s.p {
                    if p.result.is_some() {
                        return Err("55000");
                    }
                    read_only_check("INSERT", &self.settings, session, &mut self.diagnostic)?;
                    begin_implicit(session, &mut self.implicit, &mut self.settings)?;
                    let copy = copy.clone();
                    let prepared = prepared.clone();
                    p.result = ran();
                    self.start_copy(copy, prepared, false, session, out)?;
                    return Ok(true);
                }
                if let Plan::Change(change) = &p.s.p {
                    // Runs once; a suspended portal hands out the rest of its
                    // rows (PostgreSQL's rows of a utility statement).
                    if p.result.is_none() {
                        let change = change.clone();
                        crate::agent::preview::approver_guard(session)?;
                        self.diagnostic.phase = "approval execution";
                        let result = self
                            .diagnostic
                            .core(crate::agent::preview::execute_approver(session, &change))?;
                        p.result = Some(Released::synthetic(result));
                    }
                    let result = p.result.as_ref().unwrap();
                    let total = result.get().rows.len();
                    let end = if max == 0 { total }
                        else { p.at.saturating_add(max as usize).min(total) };
                    rows(&self.settings.current, session.zone().unwrap_or(SessionZone::Fixed(0)),
                        result, &p.s.d, &p.formats, p.at, end, out)?;
                    p.at = end;
                    if end < total {
                        frame(b's', &[], out)?;
                    } else {
                        let Plan::Change(change) = &p.s.p else { unreachable!() };
                        complete(crate::agent::preview::approver_command(change), result, out)?;
                    }
                    return Ok(true);
                }
                if let Plan::TaskDdl(stmt) = &p.s.p {
                    let stmt = stmt.clone();
                    if p.result.is_none() {
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
                        match outcome {
                            giggles_core::task::statements::TaskOutcome::Tag(tag) => {
                                self.admin_completed = true;
                                p.result = ran();
                                complete(tag, &empty(), out)?;
                                return Ok(true);
                            }
                            // The SHOW result the server builds from catalog
                            // and Kernel records: never core rows.
                            giggles_core::task::statements::TaskOutcome::Rows(_, result) => {
                                p.result = Some(Released::synthetic(result));
                            }
                        }
                    } else if crate::task_ddl::writes(&stmt) {
                        return Err("55000");
                    }
                    // Runs once; a suspended portal hands out the rest of its
                    // rows, as for the approver forms.
                    let result = p.result.as_ref().unwrap();
                    let total = result.get().rows.len();
                    let end = if max == 0 { total }
                        else { p.at.saturating_add(max as usize).min(total) };
                    rows(&self.settings.current, session.zone().unwrap_or(SessionZone::Fixed(0)),
                        result, &p.s.d, &p.formats, p.at, end, out)?;
                    p.at = end;
                    if end < total {
                        frame(b's', &[], out)?;
                    } else {
                        complete(crate::task_ddl::command(&stmt), result, out)?;
                    }
                    return Ok(true);
                }
                if let Plan::Admin(stmt) = &p.s.p {
                    if p.result.is_some() {
                        return Err("55000");
                    }
                    read_only_check(
                        admin_command(stmt),
                        &self.settings,
                        session,
                        &mut self.diagnostic,
                    )?;
                    let tag = session.execute_admin(stmt).map_err(|code| self.diagnostic.role_admin_error(session, stmt, code))?;
                    simple::role_notices(session, &self.settings.current, out)?;
                    self.admin_completed = true;
                    p.result = ran();
                    complete(tag, &empty(), out)?;
                    return Ok(true);
                }
                if let Plan::Catalog(plan, fields, epoch) = &p.s.p {
                    self.diagnostic.phase = "catalog execution";
                    begin_implicit(session, &mut self.implicit, &mut self.settings)?;
                    let (current, tables, indexes, typmods, types) =
                        core(session.catalog_snapshot_with_typmods())?;
                    if current != *epoch && !plan.refreshes_view_definitions() {
                        return Err("0A000");
                    }
                    if p.catalog_result.is_none() {
                        self.settings.queried = true;
                        let result = catalog_wire::execute(
                            plan,
                            &catalog_wire::with_application_rows(
                                session,
                                plan,
                                catalog_wire::with_search_path(
                                    catalog_wire::snapshot(
                                        session, current, tables, indexes, typmods, types,
                                    )?,
                                    &self.settings.current.search_path,
                                ),
                            )?,
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
                        // Built from the catalog snapshot, whose application
                        // rows were released as they were read
                        // (`with_application_rows`, GD-0061 R3-312).
                        p.catalog_result = Some(Released::synthetic(result));
                    }
                    let result = p.catalog_result.as_ref().unwrap();
                    let total = result.get().rows.len();
                    let end = if max == 0 {
                        total
                    } else {
                        p.at.saturating_add(max as usize).min(total)
                    };
                    catalog_wire::rows(
                        self.settings.current.extra_float_digits,
                        result,
                        &p.formats,
                        p.at,
                        end,
                        out,
                    )?;
                    p.at = end;
                    if end < total {
                        frame(b's', &[], out)?;
                    } else {
                        catalog_complete(total, out)?;
                    }
                    return Ok(true);
                }
                if let Plan::Explain(query) = &p.s.p {
                    begin_implicit(session, &mut self.implicit, &mut self.settings)?;
                    if p.result.is_none() {
                        // Plan text the server builds (its core reads use the
                        // plain exits, which refuse a task session).
                        p.result = Some(Released::synthetic(explain::execute(query, session)?));
                    } else {
                        revalidate(session, p.at, &mut self.diagnostic)?;
                    }
                    let result = p.result.as_ref().unwrap();
                    let total = result.get().rows.len();
                    let end = if max == 0 { total }
                        else { p.at.saturating_add(max as usize).min(total) };
                    rows(&self.settings.current, session.zone().unwrap_or(SessionZone::Fixed(0)),
                        result, &p.s.d, &p.formats, p.at, end, out)?;
                    p.at = end;
                    if end < total { frame(b's', &[], out)?; }
                    else { complete("EXPLAIN", result, out)?; }
                    return Ok(true);
                }
                if matches!(p.s.p, Plan::Empty) {
                    frame(b'I', &[], out)?;
                    return Ok(true);
                }
                if matches!(p.s.p, Plan::SettingsFunction(_)) {
                    settings_functions::execute(
                        p,
                        session,
                        &mut self.implicit,
                        &mut self.settings,
                        &mut self.diagnostic,
                        retained,
                        out,
                    )?;
                    return Ok(true);
                }
                if let Plan::AccessShare(tables) = &p.s.p {
                    if !session.in_transaction() || self.implicit {
                        return Err("25P01");
                    }
                    simple::validate_lock(session, &tables.tables, tables.mode, tables.nowait)?;
                    complete("LOCK TABLE", &empty(), out)?;
                    return Ok(true);
                }
                if let Plan::Control(command) = &p.s.p {
                    if p.result.is_some() {
                        return Err("55000");
                    }
                    if matches!(command, crate::pg_transaction::Command::Session(_)) {
                        begin_implicit(session, &mut self.implicit, &mut self.settings)?;
                    }
                    let actual = execute_mode(
                        command.clone(),
                        session,
                        &mut self.implicit,
                        &mut self.settings,
                        &mut self.diagnostic,
                        out,
                    )?;
                    if !session.in_transaction() {
                        self.cursors.transaction_end(actual);
                    }
                    p.result = ran();
                    complete(actual, &empty(), out)?;
                    if !session.in_transaction() {
                        self.portals.clear();
                    }
                    return Ok(true);
                }
                if let Plan::Settings(settings) = &p.s.p {
                    if !session.role.is_sql() {
                        return Err("42501");
                    }
                    if session.is_aborted() {
                        return Err("25P02");
                    }
                    if p.result.is_some() {
                        return Err("55000");
                    }
                    if !matches!(
                        settings.as_slice(),
                        [crate::pg_settings::Setting::Database { local: true, .. }]
                    ) {
                        begin_implicit(session, &mut self.implicit, &mut self.settings)?;
                    }
                    execute_settings(
                        settings,
                        &mut self.settings,
                        session,
                        session.in_transaction() && !self.implicit,
                        &mut self.diagnostic,
                        out,
                    )?;
                    p.result = ran();
                    return Ok(true);
                }
                // Task results are never retained in a portal. Each Execute is a
                // fresh native invocation, including completed-portal replays.
                if let Plan::Inspect(show) = p.s.p {
                    // Recheck role/transaction even when releasing cached rows.
                    let (_, current) = inspect(show, &self.settings.current, session)?;
                    if p.result.is_none() {
                        let bytes = serde_json::to_vec(current.get()).map_err(|_| "54000")?.len();
                        if retained.checked_add(bytes).ok_or("54000")? > MAX_RESPONSE {
                            return Err("54000");
                        }
                        p.result = Some(current);
                    }
                    let result = p.result.as_ref().unwrap();
                    let total = result.get().rows.len();
                    rows(
                        &self.settings.current,
                        session.zone().unwrap_or(SessionZone::Fixed(0)),
                        result,
                        &p.s.d,
                        &p.formats,
                        p.at,
                        total,
                        out,
                    )?;
                    p.at = total;
                    complete("SHOW", result, out)?;
                    return Ok(true);
                }
                #[cfg(feature = "legacy-task")]
                if let Plan::Task(task) = p.s.p {
                    let request = task.bind(&p.params)?;
                    let response = session.handle_prepared_task(request, received);
                    let value = serde_json::to_string(&response).map_err(|_| "XX000")?;
                    // The decision the server built: never core rows.
                    let result = Released::synthetic(SqlResult {
                        columns: vec!["decision".into()],
                        rows: vec![giggles_core::relational::SqlRow {
                            values: vec![Value::Text(value)],
                            version: 0,
                        }],
                        affected: 0,
                    });
                    let result = &result;
                    rows(
                        &self.settings.current,
                        session.zone().unwrap_or(SessionZone::Fixed(0)),
                        result,
                        &p.s.d,
                        &p.formats,
                        0,
                        1,
                        out,
                    )?;
                    complete("SELECT", result, out)?;
                    return Ok(true);
                }
                let Plan::Sql(prepared) = &p.s.p else {
                    unreachable!()
                };
                if prepared.command() == "SELECT" && prepared.write_tables().is_empty() {
                    if p.cursor.is_none() {
                        read_only_check(prepared.row_lock_command().filter(|_| !prepared.row_lock_tables().is_empty()).unwrap_or("SELECT"), &self.settings, session, &mut self.diagnostic)?;
                        begin_implicit(session, &mut self.implicit, &mut self.settings)?;
                        self.settings.queried = true;
                        p.cursor = Some(
                            self.diagnostic
                                .core(session.open_cursor(prepared, &p.params))?,
                        );
                    }
                    let cursor = p.cursor.as_mut().expect("opened cursor");
                    let mut remaining = if max == 0 { usize::MAX } else { max as usize };
                    while remaining > 0 && !cursor.is_exhausted() {
                        // GD-0061 R3-310: fetch unreleased, release, then encode.
                        let chunk = self.diagnostic.core(session.fetch_cursor_unreleased(
                            cursor,
                            remaining.min(CURSOR_ROWS),
                            CURSOR_BYTES,
                        ))?;
                        let chunk = self.diagnostic.core(session.release_page(chunk))?;
                        let result = chunk.map(|rows| SqlResult {
                            rows,
                            ..SqlResult::default()
                        });
                        let count = result.get().rows.len();
                        rows(
                            &self.settings.current,
                            session.zone().unwrap_or(SessionZone::Fixed(0)),
                            &result,
                            &p.s.d,
                            &p.formats,
                            0,
                            count,
                            out,
                        )?;
                        p.at += count;
                        remaining -= count;
                        flush_output(&self.output, out)?;
                    }
                    if cursor.is_exhausted() {
                        catalog_complete(p.at, out)?;
                    } else {
                        frame(b's', &[], out)?;
                    }
                    return Ok(true);
                }
                if p.result.is_some() && p.s.d.columns.is_empty() {
                    return Err("55000");
                }
                if p.result.is_none() {
                    let command = prepared.command();
                    read_only_check(prepared.row_lock_command().filter(|_| !prepared.row_lock_tables().is_empty()).unwrap_or_else(|| prepared.write_tables().first().map_or(command, |(_, command)| *command)), &self.settings, session, &mut self.diagnostic)?;
                    if needs_implicit_transaction(prepared) {
                        begin_implicit(session, &mut self.implicit, &mut self.settings)?;
                    }
                    if needs_implicit_transaction(prepared) {
                        self.settings.queried = true;
                    }
                    let (result, spilled) = if command == "BEGIN" && self.implicit {
                        self.implicit = false;
                        (Released::synthetic(SqlResult::default()), None)
                    } else {
                        self.diagnostic.core(session.sync_replication_role(&self.settings))?;
                        // GD-0061 R3-310: the whole result is released once,
                        // here. A task session's spill never reaches this
                        // handle: core materialises it behind the release tag.
                        let (result, spilled) = giggles_core::relational::spill::streaming(
                            || session.execute_prepared_unreleased(prepared, &p.params));
                        let result = self.diagnostic.core(result)?;
                        (self.diagnostic.core(session.release_page(result))?, spilled)
                    };
                    if !session.in_transaction() {
                        self.implicit = false;
                    }
                    self.settings.transaction(
                        was_transaction,
                        session.in_transaction(),
                        prepared.command(),
                    );
                    let bytes = serde_json::to_vec(result.get()).map_err(|_| "54000")?.len();
                    if retained.checked_add(bytes).ok_or("54000")? > MAX_RESPONSE {
                        return Err("54000");
                    }
                    // Admit retained bytes before caching OR appending any rows.
                    p.result = Some(result);
                    p.spilled = spilled;
                } else {
                    revalidate(session, p.at, &mut self.diagnostic)?;
                }
                if let Some(spilled) = p.spilled.as_mut() {
                    // A task session's spill is materialised behind its
                    // release tag (core), so a spill handle only ever holds
                    // the rows of a plain session (GD-0061 R3-314).
                    if session.task().is_some() {
                        return Err("XX000");
                    }
                    // Stream spilled rows like a cursor; an exhausted handle replays completion.
                    let mut remaining = if max == 0 { usize::MAX } else { max as usize };
                    while remaining > 0 && !spilled.is_empty() {
                        let chunk = spilled.by_ref().take(remaining.min(CURSOR_ROWS))
                            .collect::<Result<Vec<_>, _>>();
                        // Released as a plain session's release is: a move.
                        let chunk = Released::synthetic(SqlResult { rows: self.diagnostic.core(chunk)?, ..SqlResult::default() });
                        let count = chunk.get().rows.len();
                        rows(&self.settings.current, session.zone().unwrap_or(SessionZone::Fixed(0)),
                            &chunk, &p.s.d, &p.formats, 0, count, out)?;
                        p.at += count;
                        remaining -= count;
                        flush_output(&self.output, out)?;
                    }
                    if spilled.is_empty() {
                        complete(prepared.command(), p.result.as_ref().expect("spilled result"), out)?;
                    } else {
                        frame(b's', &[], out)?;
                    }
                    if was_transaction && !session.in_transaction() {
                        self.portals.clear();
                    }
                    return Ok(true);
                }
                let result = p.result.as_ref().unwrap();
                let total = result.get().rows.len();
                let end = if max == 0 {
                    total
                } else {
                    p.at.saturating_add(max as usize).min(total)
                };
                rows(
                    &self.settings.current,
                    session.zone().unwrap_or(SessionZone::Fixed(0)),
                    result,
                    &p.s.d,
                    &p.formats,
                    p.at,
                    end,
                    out,
                )?;
                p.at = end;
                if end < total {
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
                        if let Some(statement) = self.statements.remove(&name)
                            && !name.is_empty()
                        {
                            self.named_statement_bytes -= statement.sql_bytes;
                        }
                        debug_assert_eq!(self.named_statement_bytes, self.statements.iter().filter(|(name, _)| !name.is_empty()).map(|(_, statement)| statement.sql_bytes).sum::<usize>());
                    }
                    b'P' => {
                        self.portals.remove(&name);
                        self.cursors.close(&name);
                    }
                    _ => return Err("08P01"),
                };
                frame(b'3', &[], out)?;
            }
            b'd' | b'c' | b'f' => {}
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
pub async fn run<S: AsyncRead + AsyncWrite + Unpin>(s: &mut S, session: Session) -> io::Result<()> {
    use crate::pgwire_startup::{
        StartupPolicy, parse_startup, read_startup_packet, reject_startup,
    };
    if !session.service.admission.request() {
        return Ok(());
    }
    let startup = read_startup_packet(s).await?;
    // SSLRequest and mandatory mTLS already completed, including role binding.
    if startup.len() == 12 && startup[..4] == 80877102u32.to_be_bytes() {
        let pid = u32::from_be_bytes(startup[4..8].try_into().unwrap());
        let secret = u32::from_be_bytes(startup[8..12].try_into().unwrap());
        session.service.cancel_bound(&session, pid, secret);
        return Ok(());
    }
    let startup = match parse_startup(&startup, &StartupPolicy::Agent) {
        Ok(startup) => startup,
        Err((code, reason)) => {
            reject_startup(s, code, reason).await?;
            return Ok(());
        }
    };
    serve_session(s, session, startup, false, &[]).await
}

/// The one message every agent connect refusal carries (design-2 section 3).
pub const AGENT_REFUSAL: &str = "agent credential is not authorized for a task session";

/// GD-0061 R3-205: a connection on the agent pgwire listener, after TLS, whose
/// certificate is in the agent map. Reads the startup packet (or a
/// CancelRequest), opens the task session and serves it. Every credential
/// refusal is the same FATAL 28000; the reason goes to stderr only.
pub(crate) async fn run_agent<S: AsyncRead + AsyncWrite + Unpin>(
    s: &mut S,
    service: &std::sync::Arc<crate::Service>,
    fingerprint: &str,
    databases: &[String],
    shared: std::sync::Arc<tokio::sync::OwnedSemaphorePermit>,
) -> io::Result<()> {
    use crate::pgwire_startup::{
        StartupPolicy, parse_startup, read_startup_packet, reject_startup,
    };
    if !service.admission.request() {
        return Ok(());
    }
    let startup = read_startup_packet(s).await?;
    if startup.len() == 12 && startup[..4] == 80877102u32.to_be_bytes() {
        let pid = u32::from_be_bytes(startup[4..8].try_into().unwrap());
        let secret = u32::from_be_bytes(startup[8..12].try_into().unwrap());
        service.cancel_agent(fingerprint, pid, secret);
        return Ok(());
    }
    let startup = match parse_startup(&startup, &StartupPolicy::TaskAgent { databases }) {
        Ok(startup) => startup,
        Err((code, reason)) => {
            eprintln!("protocol=agent_postgres rejected stage=startup code={code}");
            reject_startup(s, code, reason).await?;
            return Ok(());
        }
    };
    let mut session = match service.agent_session(fingerprint, startup.task.as_deref()) {
        Ok(session) => session,
        Err(error) => {
            eprintln!(
                "protocol=agent_postgres rejected stage=task_session fingerprint={} reason={error:?}",
                &fingerprint[..8]
            );
            return agent_refusal(s).await;
        }
    };
    let executor = match &session.role {
        Role::Database { name } if name == &startup.user => name.clone(),
        _ => {
            eprintln!(
                "protocol=agent_postgres rejected stage=task_session fingerprint={} reason=user_is_not_the_executor",
                &fingerprint[..8]
            );
            return agent_refusal(s).await;
        }
    };
    session.connection = Some(shared);
    session.backend_ssl = true;
    session.database = Some(startup.database.clone());
    let statuses = [
        ("is_superuser", "off".to_owned()),
        ("session_authorization", executor),
    ];
    serve_session(s, session, startup, false, &statuses).await
}

/// SQL-issued credential authentication, always inside the agent listener's TLS.
pub(crate) async fn run_agent_password<S: AsyncRead + AsyncWrite + Unpin>(
    s: &mut S,
    service: &std::sync::Arc<crate::Service>,
    databases: &[String],
    shared: std::sync::Arc<tokio::sync::OwnedSemaphorePermit>,
) -> io::Result<()> {
    use crate::pgwire_startup::{
        StartupPolicy, parse_startup, read_startup_packet, reject_startup,
    };
    if !service.admission.request() {
        return Ok(());
    }
    let packet = read_startup_packet(s).await?;
    if packet.len() == 12 && packet[..4] == 80877102u32.to_be_bytes() {
        let pid = u32::from_be_bytes(packet[4..8].try_into().unwrap());
        let secret = u32::from_be_bytes(packet[8..12].try_into().unwrap());
        service.cancellations.cancel_key("sqlcred:", pid, secret);
        return Ok(());
    }
    // TaskAgent already permits an omitted task; the credential selects it.
    let startup = match parse_startup(&packet, &StartupPolicy::TaskAgent { databases }) {
        Ok(startup) => startup,
        Err((code, reason)) => {
            reject_startup(s, code, reason).await?;
            return Ok(());
        }
    };
    let mut out = Vec::new();
    frame(b'R', &3u32.to_be_bytes(), &mut out).map_err(io::Error::other)?;
    send(s, &out).await?;
    let password = timeout(IO_TIMEOUT, async {
        let tag = s.read_u8().await?;
        let length = s.read_u32().await?;
        if tag != b'p' || !(5..=1028).contains(&length) {
            return Err(io::Error::other("invalid password message"));
        }
        let mut body = vec![0; length as usize - 4];
        s.read_exact(&mut body).await?;
        if body.pop() != Some(0) || body.contains(&0) {
            return Err(io::Error::other("invalid password message"));
        }
        String::from_utf8(body).map_err(|_| io::Error::other("invalid password message"))
    })
    .await;
    let password = match password {
        Ok(Ok(password)) => password,
        _ => return agent_password_error(s, "08P01", "malformed password message").await,
    };
    let mut session =
        match service.credential_session(&password, Some(&startup.user), startup.task.as_deref()) {
            Ok(session) => session,
            Err(_) => {
                eprintln!("protocol=agent_postgres rejected stage=credential");
                return agent_password_error(
                    s,
                    "28P01",
                    &format!(
                        "password authentication failed for user \"{}\"",
                        startup.user,
                    ),
                )
                .await;
            }
        };
    session.connection = Some(shared);
    session.backend_ssl = true;
    session.database = Some(startup.database.clone());
    let statuses = [
        ("is_superuser", "off".to_owned()),
        ("session_authorization", startup.user.clone()),
    ];
    // false emits AuthenticationOk once; no further challenge is sent here.
    serve_session(s, session, startup, false, &statuses).await
}

async fn agent_password_error<S: AsyncWrite + Unpin>(
    s: &mut S,
    code: &str,
    message: &str,
) -> io::Result<()> {
    let mut body = b"SFATAL\0VFATAL\0C".to_vec();
    z(code, &mut body);
    body.push(b'M');
    z(message, &mut body);
    body.push(0);
    let mut out = Vec::new();
    frame(b'E', &body, &mut out).map_err(io::Error::other)?;
    send(s, &out).await
}

async fn agent_refusal<S: AsyncWrite + Unpin>(s: &mut S) -> io::Result<()> {
    let mut body = b"SFATAL\0VFATAL\0C28000\0M".to_vec();
    z(AGENT_REFUSAL, &mut body);
    body.push(0);
    let mut out = Vec::new();
    frame(b'E', &body, &mut out).map_err(io::Error::other)?;
    send(s, &out).await
}

/// Serves one post-startup pgwire session through read and write buffers.
/// Replies to messages the client has already pipelined behind are held back
/// and leave in one write
/// with the next flushed reply (Sync, Query, Flush, or the reply after which
/// no client byte is waiting), as PostgreSQL does. Wire bytes are unchanged.
pub(crate) async fn serve_session<S: AsyncRead + AsyncWrite + Unpin>(
    s: &mut S,
    session: Session,
    startup: crate::pgwire_startup::Startup,
    authenticated_already: bool,
    extra_status: &[(&str, String)],
) -> io::Result<()> {
    // The frame reader reads tag, length and body separately. Without a read
    // buffer each piece is a recv syscall (measured: 41 recvfrom per read op).
    let mut s = tokio::io::BufWriter::with_capacity(
        16 * 1024,
        tokio::io::BufReader::with_capacity(16 * 1024, s),
    );
    let served =
        serve_buffered(&mut s, session, startup, authenticated_already, extra_status).await;
    // Deliver any held reply before the connection closes.
    let _ = timeout(IO_TIMEOUT, s.flush()).await;
    served
}

/// The next client message tag, if its first byte is already readable.
async fn ready_tag<S: AsyncRead + Unpin>(s: &mut S) -> Option<io::Result<u8>> {
    let mut read = std::pin::pin!(s.read_u8());
    std::future::poll_fn(|cx| match read.as_mut().poll(cx) {
        std::task::Poll::Ready(tag) => std::task::Poll::Ready(Some(tag)),
        std::task::Poll::Pending => std::task::Poll::Ready(None),
    })
    .await
}

const FRONTEND_BATCH_MESSAGES: usize = 256;
const FRONTEND_BATCH_BYTES: usize = 4 * 1024 * 1024;

fn leader_error(out: &mut Vec<u8>) -> io::Result<()> {
    let mut body = b"SFATAL\0VFATAL\0C57P01\0M".to_vec();
    z(
        "terminating connection: this node is not the replication leader",
        &mut body,
    );
    body.push(0);
    frame(b'E', &body, out).map_err(io::Error::other)
}

async fn read_frontend_body<S: AsyncRead + Unpin>(s: &mut S) -> io::Result<Vec<u8>> {
    timeout(IO_TIMEOUT, async {
        let n = s.read_u32().await?;
        // SQL plus Parse's bounded name/type declarations and frame header.
        // Other transports keep their existing frame budgets.
        let maximum = (giggles_core::relational::MAX_STATEMENT_SQL + 2048).max(MAX_FRAME);
        if n < 4 || n as u64 > maximum as u64 {
            return Err(io::Error::other("wire frame limit"));
        }
        let mut b = vec![0; n as usize - 4];
        s.read_exact(&mut b).await?;
        Ok(b)
    })
    .await
    .map_err(io::Error::other)?
}

impl State {
    /// Returns (keep session, leadership shutdown).
    fn buffered_message(
        &mut self,
        message: (u8, &[u8], std::time::Instant),
        session: &mut Session,
        out: &mut Vec<u8>,
        active: crate::cancel::Active,
        refusal: Option<&'static str>,
        admission_message: Option<String>,
    ) -> io::Result<(bool, bool)> {
        let (tag, b, received) = message;
        let context = active.context.clone();
        self.diagnostic = Diagnostic {
            unknown_outcome: false,
            wire_detail: None,
            wire_hint: None,
            sql_detail: None,
            wire_message: None,
            message: None,
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
            task_refusal: None,
        };
        if admission_message.is_some() {
            self.diagnostic.phase = "admission";
            self.diagnostic.wire_message = admission_message;
        }
        let keep = match context.scope(|| {
            if let Some(code) = refusal {
                return Err(code);
            }
            context.check().map_err(|e| crate::service::sqlstate(&e))?;
            session
                .authority_scope(|session| Ok(self.handle(tag, b, session, out, received)))
                .map_err(|e| crate::service::sqlstate(&e))?
        }) {
            Ok(keep) => Ok(keep),
            Err(code) => {
                session.abort();
                // COMMIT may fail and finish rollback before returning
                // its error. No portal can outlive that transaction.
                if tag == b'Q' && !session.in_transaction() {
                    self.portals.clear();
                }
                if tag == b'S' && self.implicit {
                    session.sql.rollback();
                    self.settings.transaction(true, false, "ROLLBACK");
                    session.set_time_zone(&self.settings.current.timezone);
                    session.sync_role_settings(&mut self.settings.current).map_err(std::io::Error::other)?;
        session.set_search_path(&self.settings.current.search_path);
                    session.set_extra_float_digits(self.settings.current.extra_float_digits);
                    session.set_max_value_bytes(self.settings.current.max_value_bytes);
                    session.set_max_statement_tokens(self.settings.current.max_statement_tokens);
                    self.implicit = false;
                    self.cursors.transaction_end("ROLLBACK");
                    self.portals.clear();
                }
                let message = self.diagnostic.message.unwrap_or(match code {
                    "0A000" => "unsupported SQL feature or parameter type in this server profile",
                    "42601" => "statement syntax is not supported by this server SQL profile",
                    _ => GENERIC_MESSAGE,
                });
                self.diagnostic.unknown_outcome = code == "08007";
                if self.copy.is_some() {
                    self.copy_error(code, session, out);
                    Ok(true)
                } else {
                    diagnostic_error(code, message, Some(&self.diagnostic), out);
                    // Error formatting discards partial result frames, but role
                    // diagnostics produced before a rollback must reach the client.
                    let error = std::mem::take(out);
                    simple::role_notices(session, &self.settings.current, out).map_err(io::Error::other)?;
                    out.extend(error);
                    if matches!(tag, b'Q' | b'S') {
                        self.drain = false;
                        ready(session, out).map(|()| true).map_err(io::Error::other)
                    } else {
                        self.drain = true;
                        Ok(true)
                    }
                }
            }
        };
        if self.diagnostic.unknown_outcome
            && session.service.statement_gate.get().is_some_and(|gate| {
                !matches!(
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                        || context.scope(|| gate())
                    )),
                    Ok(Ok(()))
                )
            })
        {
            // Parse/Bind/Describe cannot have committed. Other messages
            // retain their unknown-outcome error before the terminal refusal.
            if matches!(tag, b'P' | b'B' | b'D') {
                out.clear();
            }
            leader_error(out)?;
            return Ok((false, true));
        }
        if !session.in_transaction() && !self.implicit {
            session.service.advisory.release(session.backend_pid, true);
        }
        if self.copy_receiving() {
            self.copy_active = Some(active);
            self.copy_permit = self
                .execution_admission
                .as_mut()
                .and_then(|a| a.role_permit.take());
        } else {
            drop(active);
        }
        keep.map(|keep| (keep, false))
    }
}

async fn serve_buffered<S: AsyncRead + AsyncWrite + Unpin>(
    s: &mut S,
    mut session: Session,
    startup: crate::pgwire_startup::Startup,
    authenticated_already: bool,
    extra_status: &[(&str, String)],
) -> io::Result<()> {
    // DD-0068 F9: only PostgreSQL wire sessions opt into PostgreSQL diagnostics.
    session.surface = crate::service::Surface::Postgres;
    let blocking = session.service.blocking.clone();
    let mut startup_settings = startup.settings;
    let timezone = startup.timezone;
    let mut out = vec![];
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
    session.backend_pid = backend.pid as i32;
    if !authenticated_already {
        frame(b'R', &0u32.to_be_bytes(), &mut out).map_err(io::Error::other)?;
    }
    let key = [backend.pid.to_be_bytes(), backend.secret.to_be_bytes()].concat();
    frame(b'K', &key, &mut out).map_err(io::Error::other)?;
    for (key, value) in [
        ("server_version", "18.6"),
        ("client_encoding", "UTF8"),
        ("server_encoding", "UTF8"),
        ("standard_conforming_strings", "on"),
        ("integer_datetimes", "on"),
        ("DateStyle", "ISO, MDY"),
        ("TimeZone", timezone.as_str()),
    ]
    .into_iter()
    .chain(
        matches!(session.role, Role::Database { .. })
            .then_some(("IntervalStyle", startup_settings.intervalstyle.as_str())),
    ) {
        let mut b = vec![];
        z(key, &mut b);
        z(value, &mut b);
        frame(b'S', &b, &mut out).map_err(io::Error::other)?;
    }
    for (key, value) in extra_status {
        let mut b = vec![];
        z(key, &mut b);
        z(value, &mut b);
        frame(b'S', &b, &mut out).map_err(io::Error::other)?;
    }
    ready(&session, &mut out).map_err(io::Error::other)?;
    send(s, &out).await?;
    let mut state = State {
        output: None,
        diagnostic: Diagnostic::default(),
        admin_completed: false,
        implicit: false,
        settings: crate::pg_settings::SessionSettings::default(),
        statements: BTreeMap::new(),
        unnamed_cache: Vec::new(),
        named_statement_bytes: 0,
        portals: BTreeMap::new(),
        cursors: cursor::Cursors::default(),
        drain: false,
        copy: None,
        copy_completion: vec![],
        copy_active: None,
        copy_permit: None,
        execution_admission: None,
    };
    startup_settings.timezone = timezone;
    // Preserve the locked agent profile's existing presentation default.
    if !matches!(session.role, Role::Database { .. }) {
        startup_settings.search_path = "public".into();
    }
    startup_settings.engine = session.service.admin.engine();
    startup_settings.session_authorization = session.session_user_name().unwrap_or("").into();
    state.settings.defaults = startup_settings.clone();
    state.settings.current = startup_settings;
    session.set_time_zone(&state.settings.current.timezone);
    session.set_search_path(&state.settings.current.search_path);
    session.set_extra_float_digits(state.settings.current.extra_float_digits);
    session.set_max_value_bytes(state.settings.current.max_value_bytes);
    session.set_max_statement_tokens(state.settings.current.max_statement_tokens);
    let mut admin_sync = false;
    let mut pending_tag = None;
    let mut pending_frames = std::collections::VecDeque::<(u8, Vec<u8>, std::time::Instant)>::new();
    let mut binding_changes = session.service.binding_changes.subscribe();
    let mut binding_check_pending = true;
    loop {
        // Idle connections hold a bounded admission slot, not an active frame.
        // Once a frame starts, its remaining header and body have a deadline.
        let tag = loop {
            // Mark changes seen before checking, so a concurrent change still
            // triggers revalidation before waiting for an idle client's byte.
            if !admin_sync
                && (binding_check_pending || binding_changes.has_changed().unwrap_or(false))
            {
                binding_changes.borrow_and_update();
                binding_check_pending = false;
                // A transient failure (the gateway fenced, a range unavailable) is not a lost
                // binding: the next statement rechecks and reports it.
                if session
                    .check_binding()
                    .is_err_and(|e| !crate::service::transient(&e))
                {
                    session.abort();
                    // Absorb late client writes so the peer observes EOF without a reset.
                    let _ = timeout(std::time::Duration::from_secs(2), async {
                        let _ = s.shutdown().await;
                        let mut remaining = 64 * 1024;
                        let mut discard = [0; 4096];
                        while remaining > 0 {
                            let limit = remaining.min(discard.len());
                            match s.read(&mut discard[..limit]).await {
                                Ok(0) | Err(_) => break,
                                Ok(n) => remaining -= n,
                            }
                        }
                    })
                    .await;
                    return Ok(());
                }
            }
            if let Some((tag, _, _)) = pending_frames.front() {
                break Ok(*tag);
            }
            if let Some(tag) = pending_tag.take() {
                break Ok(tag);
            }
            tokio::select! {
                biased;
                _ = binding_changes.changed(), if !admin_sync => {
                    binding_check_pending = true;
                    continue;
                },
                tag = s.read_u8() => break tag,
            }
        };
        let tag = match tag {
            Ok(tag) => tag,
            Err(_) => return Ok(()),
        };
        let queued = !pending_frames.is_empty();
        let (b, received) = if queued {
            let (_, b, received) = pending_frames.pop_front().unwrap();
            (b, received)
        } else {
            match read_frontend_body(s).await {
                Ok(b) => (b, std::time::Instant::now()),
                Err(_) => return Ok(()),
            }
        };
        // An admin command may invalidate its own login generation. Deliver its
        // completion and finish the extended cycle, then reject further work.
        if admin_sync {
            // Keep the terminal admin cycle's old gate-before-close behavior.
            if matches!(tag, b'Q' | b'E')
                && let Some(gate) = session.service.statement_gate.get().cloned()
            {
                session
                    .service
                    .pgwire_blocking_tasks
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if !matches!(
                    {
                        let service = session.service.clone();
                        let submitted = service.pgwire_submission_token();
                        tokio::task::spawn_blocking(move || {
                            let _submitted = submitted;
                            let _live = service.pgwire_live_token();
                            gate()
                        }).await
                    },
                    Ok(Ok(()))
                ) {
                    let mut out = Vec::new();
                    leader_error(&mut out)?;
                    send(s, &out).await?;
                    s.shutdown().await?;
                    return Ok(());
                }
            }

            if tag == b'H' && b.is_empty() {
                continue;
            }
            if tag == b'S' && b.is_empty() {
                let mut out = Vec::new();
                ready(&session, &mut out).map_err(io::Error::other)?;
                send(s, &out).await?;
            }
            return Ok(());
        }
        state.admin_completed = false;
        if !queued && !session.service.admission.request() {
            return Ok(());
        }
        // A successful implicit cycle publishes at Sync and must be cancellable
        // at that decision boundary. Recovery-only Sync remains uncancellable.
        let active = state.copy_active.take().unwrap_or_else(|| {
            backend.begin(
                matches!(tag, b'Q' | b'P' | b'B' | b'D' | b'E' | b'd' | b'c' | b'f')
                    || (tag == b'S' && state.implicit && !session.is_aborted()),
            )
        });
        // GD-0061 R3-403/R3-404c: an agent request takes an agent slot first;
        // when the ceiling is full it waits (async, holding no role or blocking
        // permit) up to agent.ceiling_refusal_delay_ms for a freed slot, then
        // gets 54T01. Then the role pool and the blocking pool.
        // Release is the reverse: blocking and role permits, then the slot.
        let agent_slot = if session.agent {
            session.service.agent_admission.admit().await.map(Some)
        } else {
            Ok(None)
        };
        // GD-0075 R5-103: the role's statement pool sits in front of the
        // service-wide blocking pool; only COPY retains it between frames.
        let role_admission = if agent_slot.is_err() {
            // The refusal ends any COPY, so its retained role permit goes too.
            state.copy_permit = None;
            Ok(None)
        } else if matches!(tag, b'Q' | b'E')
            && !session.is_aborted()
            && !state.drain
            && state.copy.is_none()
            && !state.admission_exempt(tag, &b)
        {
            let admission = &session.service.admission;
            admission
                .statement(&session.role, Some(&active.context))
                .await
                .map(Some)
        } else {
            Ok(state.copy_permit.take())
        };
        let admitted = match (agent_slot, role_admission) {
            (Err(e), _) => Err((crate::service::sqlstate(&e), None)),
            (Ok(agent_slot), Ok(role)) => blocking
                .admit(&active.context)
                .await
                .map(|permit| (agent_slot, role, permit))
                .map_err(|e| (crate::service::sqlstate(&e), None)),
            (Ok(_), Err(refusal)) => Err((refusal.sqlstate(), Some(refusal.message()))),
        };
        let (agent_slot, role_permit, permit) = match admitted {
            Ok(admitted) => admitted,
            Err((code, message)) => {
                session.abort();
                let mut out = Vec::new();
                if state.copy.is_some() {
                    state.copy_error(code, &mut session, &mut out);
                } else {
                    if message.is_some() {
                        let diagnostic = Diagnostic {
                            phase: "admission",
                            wire_message: message,
                            ..Diagnostic::default()
                        };
                        diagnostic_error(code, GENERIC_MESSAGE, Some(&diagnostic), &mut out);
                    } else {
                        error(code, &mut out);
                    }
                    if tag == b'S' && state.implicit {
                        session.sql.rollback();
                        state.settings.transaction(true, false, "ROLLBACK");
                        session.set_time_zone(&state.settings.current.timezone);
                        session.set_search_path(&state.settings.current.search_path);
                        session.set_extra_float_digits(state.settings.current.extra_float_digits);
                        session.set_max_value_bytes(state.settings.current.max_value_bytes);
                        session.set_max_statement_tokens(state.settings.current.max_statement_tokens);
                        state.implicit = false;
                        state.cursors.transaction_end("ROLLBACK");
                        state.portals.clear();
                    }
                    if matches!(tag, b'Q' | b'S') {
                        state.drain = false;
                        ready(&session, &mut out).map_err(io::Error::other)?;
                    } else {
                        state.drain = true;
                    }
                }
                drop(active);
                send(s, &out).await?;
                continue;
            }
        };
        let mut batch = std::collections::VecDeque::from([(tag, b, received)]);
        let mut bytes = batch[0].1.len();
        // COPY may start at Execute; queued frames after it are retained for
        // the single-message COPY path, never executed in the same scope.
        while state.copy.is_none()
            && !state.drain
            && !matches!(
                batch.back().unwrap().0,
                b'S' | b'Q' | b'H' | b'X' | b'd' | b'c' | b'f'
            )
            && batch.len() < FRONTEND_BATCH_MESSAGES
            && bytes < FRONTEND_BATCH_BYTES
        {
            let next = if let Some((next, _, _)) = pending_frames.front() {
                Some(Ok(*next))
            } else if let Some(next) = pending_tag.take() {
                Some(Ok(next))
            } else {
                ready_tag(s).await
            };
            let next = match next {
                Some(Ok(next)) => next,
                Some(Err(_)) | None => break,
            };
            if matches!(next, b'X' | b'd' | b'c' | b'f') {
                pending_tag = Some(next);
                break;
            }
            let (body, received) = if let Some((_, body, received)) = pending_frames.pop_front() {
                (body, received)
            } else {
                let body = match read_frontend_body(s).await {
                    Ok(body) => body,
                    Err(_) => return Ok(()),
                };
                let received = std::time::Instant::now();
                if !session.service.admission.request() {
                    return Ok(());
                }
                (body, received)
            };
            if bytes.saturating_add(body.len()) > FRONTEND_BATCH_BYTES {
                pending_frames.push_front((next, body, received));
                break;
            }
            bytes += body.len();
            batch.push_back((next, body, received));
        }
        let (output, mut chunks) = tokio::sync::mpsc::channel(2);
        state.output = Some(OutputChannel {
            sender: output,
            prefix: std::sync::Mutex::new(Vec::new()),
        });
        let connection_closed = session.connection_closed.clone();
        state.execution_admission = Some(ExecutionAdmission {
            role_permit,
            blocking: Some(permit),
            runtime: tokio::runtime::Handle::current(),
            context: active.context.clone(),
            first: state.copy.is_none(),
        });
        let backend = backend.clone();
        let work = async move {
            session
                .service
                .pgwire_blocking_tasks
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let submitted = session.service.pgwire_submission_token();
            tokio::task::spawn_blocking(move || {
                let live = session.service.pgwire_live_token();
                let mut owner = BlockingOwner {
                    session, state, _agent: agent_slot, _live: live, _submitted: submitted,
                };
                let (session, state) = (&mut owner.session, &mut owner.state);
                let mut leader_shutdown = false;
                let mut first_active = Some(active);
                let mut last_tag = tag;
                let mut binding_lost = false;
                let mut keep = Ok(true);
                while let Some((tag, b, received)) = batch.pop_front() {
                    let mut out = Vec::new();
                    last_tag = tag;
                    state.admin_completed = false;
                    let active = first_active
                        .take()
                        .or_else(|| state.copy_active.take())
                        .unwrap_or_else(|| {
                            backend.begin(
                                matches!(
                                    tag,
                                    b'Q' | b'P' | b'B' | b'D' | b'E' | b'd' | b'c' | b'f'
                                ) || (tag == b'S' && state.implicit && !session.is_aborted()),
                            )
                        });
                    active.context.set_durable_tracker(session.service.durable_tracker());
                    if matches!(tag, b'Q' | b'E')
                        && session.service.statement_gate.get().is_some_and(|gate| {
                            !matches!(
                                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| active
                                    .context
                                    .scope(|| gate()))),
                                Ok(Ok(()))
                            )
                        })
                    {
                        keep = leader_error(&mut out).map(|()| false);
                        state
                            .output
                            .as_ref()
                            .unwrap()
                            .prefix
                            .lock()
                            .unwrap()
                            .extend(out);
                        leader_shutdown = true;
                        break;
                    }
                    let needs_role = matches!(tag, b'Q' | b'E')
                        && !session.is_aborted()
                        && !state.drain
                        && state.copy.is_none()
                        && !state.admission_exempt(tag, &b);
                    let admission = state.execution_admission.as_mut().unwrap();
                    admission.context = active.context.clone();
                    admission.first = state.copy.is_none();
                    let refusal = if needs_role && admission.role_permit.is_none() {
                        admission.blocking = None;
                        admission
                            .runtime
                            .block_on(async {
                                admission.role_permit = Some(
                                    session
                                        .service
                                        .admission
                                        .statement(&session.role, Some(&active.context))
                                        .await?,
                                );
                                admission.blocking = Some(
                                    session
                                        .service
                                        .blocking
                                        .admit(&active.context)
                                        .await
                                        .map_err(crate::admission::StatementRefusal::Core)?,
                                );
                                Ok::<_, crate::admission::StatementRefusal>(())
                            })
                            .err()
                    } else {
                        None
                    };
                    // Admission refusal uses the same error/abort/drain path.
                    let outcome = state.buffered_message(
                        (tag, &b, received),
                        session,
                        &mut out,
                        active,
                        refusal.as_ref().map(|e| e.sqlstate()),
                        refusal.as_ref().map(|e| e.message()),
                    );
                    keep = outcome.map(|(keep, shutdown)| {
                        leader_shutdown = shutdown;
                        keep
                    });
                    state
                        .output
                        .as_ref()
                        .unwrap()
                        .prefix
                        .lock()
                        .unwrap()
                        .extend(out);
                    binding_lost = session
                        .check_binding()
                        .is_err_and(|e| !crate::service::transient(&e));
                    if binding_lost
                        || !matches!(keep, Ok(true))
                        || state.copy.is_some()
                        || refusal.is_some()
                    {
                        break;
                    }
                }
                state.execution_admission = None;
                let out = state.output.take().unwrap().prefix.into_inner().unwrap();
                (
                    owner.session,
                    owner.state,
                    out,
                    keep,
                    batch,
                    last_tag,
                    binding_lost,
                    leader_shutdown,
                )
            })
            .await
            .map_err(io::Error::other)
        };
        tokio::pin!(work);
        let completed = loop {
            tokio::select! {
                chunk = chunks.recv() => match chunk {
                    Some(chunk) => send(s, &chunk).await?,
                    None => break work.await?,
                },
                result = &mut work => break result?,
                next = s.read_u8(), if pending_tag.is_none() => match next {
                    Ok(b'X') => { connection_closed.store(true, std::sync::atomic::Ordering::Release); return Ok(()); }
                    Ok(tag) => pending_tag = Some(tag),
                    Err(_) => { connection_closed.store(true, std::sync::atomic::Ordering::Release); return Ok(()); }
                },
            }
        };
        while let Some(chunk) = chunks.recv().await {
            send(s, &chunk).await?;
        }
        let (
            returned_session,
            returned_state,
            response,
            keep,
            remaining,
            tag,
            binding_lost,
            leader_shutdown,
        ) = completed;
        pending_frames = remaining.into_iter().chain(pending_frames).collect();
        session = returned_session;
        state = returned_state;
        if !keep? {
            send(s, &response).await?;
            if leader_shutdown {
                s.shutdown().await?;
            }
            return Ok(());
        }
        if binding_lost {
            if state.admin_completed {
                send(s, &response).await?;
                if tag == b'E' {
                    admin_sync = true;
                    continue;
                }
            }
            return Ok(());
        }
        // Hold this reply while the client has already pipelined its next
        // message (Parse/Bind/Describe/Execute before Sync). Sync, Query and
        // Flush always flush, as does every other send.
        if !matches!(tag, b'S' | b'Q' | b'H') {
            let next = match pending_tag.or_else(|| pending_frames.front().map(|f| f.0)) {
                Some(_) => Some(Ok(0)),
                None => ready_tag(s).await,
            };
            match next {
                Some(Ok(next)) => {
                    if pending_tag.is_none() && pending_frames.is_empty() {
                        pending_tag = Some(next);
                    }
                    timeout(IO_TIMEOUT, s.write_all(&response))
                        .await
                        .map_err(io::Error::other)??;
                    continue;
                }
                Some(Err(_)) => {
                    send(s, &response).await?;
                    return Ok(());
                }
                None => {}
            }
        }
        send(s, &response).await?;
    }
}

#[cfg(test)]
#[path = "pgwire_input_contract_tests.rs"]
mod wire_input_contract_tests;

#[cfg(test)]
#[path = "pgwire_timetz_tests.rs"]
mod timetz_tests;

#[cfg(all(test, feature = "engine-rocksdb"))]
#[path = "pgwire_task_release_tests.rs"]
mod task_release_tests;

#[cfg(all(test, feature = "engine-rocksdb"))]
#[path = "pgwire_task_catalog_tests.rs"]
mod task_catalog_tests;
fn logical_binary_output(ty: &DataType, s: &str) -> R<Vec<u8>> {
    use giggles_core::relational::logical_types::array_values;
    if *ty == DataType::Uuid {
        return hex::decode(s.replace('-', "")).map_err(|_| "22P02");
    }
    if *ty == DataType::Jsonb {
        let mut b = vec![1];
        b.extend_from_slice(s.as_bytes());
        return Ok(b);
    }
    let vals = array_values(s).map_err(|_| "22P02")?;
    let elem = match ty {
        DataType::IntArray => 23u32,
        DataType::BigIntArray => 20,
        _ => 25,
    };
    let mut b = Vec::new();
    b.extend_from_slice(&(if vals.is_empty() { 0i32 } else { 1 }).to_be_bytes());
    b.extend_from_slice(&i32::from(vals.contains(&None)).to_be_bytes());
    b.extend_from_slice(&elem.to_be_bytes());
    if !vals.is_empty() {
        b.extend_from_slice(&(vals.len() as i32).to_be_bytes());
        b.extend_from_slice(&1i32.to_be_bytes());
    }
    for val in vals {
        if let Some(s) = val {
            let v = match ty {
                DataType::IntArray => s
                    .parse::<i32>()
                    .map_err(|_| "22P02")?
                    .to_be_bytes()
                    .to_vec(),
                DataType::BigIntArray => s
                    .parse::<i64>()
                    .map_err(|_| "22P02")?
                    .to_be_bytes()
                    .to_vec(),
                _ => s.into_bytes(),
            };
            b.extend_from_slice(&(v.len() as i32).to_be_bytes());
            b.extend(v);
        } else {
            b.extend_from_slice(&(-1i32).to_be_bytes());
        }
    }
    Ok(b)
}
fn logical_binary_input(ty: &DataType, b: &[u8]) -> R<Value> {
    use giggles_core::relational::logical_types::{array_quote, coerce};
    if *ty == DataType::Uuid {
        if b.len() != 16 {
            return Err("22P03");
        }
        return coerce(Value::Text(hex::encode(b)), ty).map_err(|_| "22P02");
    }
    if *ty == DataType::Jsonb {
        if b.first() != Some(&1) {
            return Err("22P03");
        }
        let s = std::str::from_utf8(&b[1..]).map_err(|_| "22021")?;
        return coerce(Value::Text(s.into()), ty).map_err(|_| "22P02");
    }
    let mut at = 0;
    fn int(b: &[u8], at: &mut usize) -> R<i32> {
        let v = b.get(*at..*at + 4).ok_or("08P01")?;
        *at += 4;
        Ok(i32::from_be_bytes(v.try_into().unwrap()))
    }
    let ndim = int(b, &mut at)?;
    let hasnull = int(b, &mut at)?;
    let elem = int(b, &mut at)?;
    let expected = match ty {
        DataType::IntArray => 23,
        DataType::BigIntArray => 20,
        _ => 25,
    };
    if !(0..=1).contains(&ndim) || !(0..=1).contains(&hasnull) || elem != expected {
        return Err("22P03");
    }
    let count = if ndim == 1 {
        let n = int(b, &mut at)?;
        let lb = int(b, &mut at)?;
        if n < 0 || lb != 1 {
            return Err("22P03");
        }
        n as usize
    } else {
        0
    };
    if count > b.len() / 4 {
        return Err("08P01");
    }
    let mut vals = Vec::new();
    for _ in 0..count {
        let len = int(b, &mut at)?;
        if len == -1 {
            vals.push("NULL".into());
            continue;
        }
        let len = usize::try_from(len).map_err(|_| "22P03")?;
        let v = b.get(at..at + len).ok_or("08P01")?;
        at += len;
        vals.push(match ty {
            DataType::IntArray if len == 4 => i32::from_be_bytes(v.try_into().unwrap()).to_string(),
            DataType::BigIntArray if len == 8 => {
                i64::from_be_bytes(v.try_into().unwrap()).to_string()
            }
            DataType::TextArray => array_quote(std::str::from_utf8(v).map_err(|_| "22021")?),
            _ => return Err("22P03"),
        });
    }
    if at != b.len() {
        return Err("22P03");
    }
    coerce(Value::Text(format!("{{{}}}", vals.join(","))), ty).map_err(|_| "22P02")
}

#[cfg(test)]
mod logical_wire_tests {
    use super::*;
    #[test]
    fn subquery_and_distinct_error_wire_identities() {
        use giggles_core::relational::SqlError;
        for (error, code, message) in [
            (
                SqlError::CardinalityViolation,
                "21000",
                "more than one row returned by a subquery used as an expression",
            ),
            (
                SqlError::InvalidColumnReference,
                "42P10",
                "for SELECT DISTINCT, ORDER BY expressions must appear in select list",
            ),
        ] {
            let mapped = crate::service::sqlstate(&CoreError::Sql(error));
            assert_eq!(mapped, code);
            for diagnostic in [None, Some(&Diagnostic::default())] {
                let mut out = Vec::new();
                diagnostic_error(mapped, "fallback", diagnostic, &mut out);
                assert_eq!(out[0], b'E');
                let fields: Vec<_> = out[5..].split(|b| *b == 0).collect();
                assert!(fields.contains(&format!("C{code}").as_bytes()));
                assert!(fields.contains(&format!("M{message}").as_bytes()));
            }
        }
    }
    #[test]
    fn binary_outputs_roundtrip() {
        for (ty, text) in [
            (DataType::Uuid, "12345678-1234-5678-1234-567812345678"),
            (DataType::Jsonb, "{\"a\": 1}"),
            (DataType::IntArray, "{1,NULL,-3}"),
            (DataType::BigIntArray, "{9223372036854775807}"),
            (DataType::TextArray, "{\"a,b\",NULL,\"NULL\"}"),
        ] {
            let binary = logical_binary_output(&ty, text).unwrap();
            assert_eq!(
                logical_binary_input(&ty, &binary),
                Ok(Value::Text(text.into()))
            );
            if ty == DataType::Uuid {
                assert_eq!(binary.len(), 16);
            }
            if ty == DataType::Jsonb {
                assert_eq!(binary[0], 1);
            }
        }
        let binary = logical_binary_output(&DataType::IntArray, "{1,NULL,-3}").unwrap();
        assert_eq!(
            &binary[..20],
            &[0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 23, 0, 0, 0, 3, 0, 0, 0, 1]
        );
    }
}

#[cfg(test)]
#[path = "pgwire_user_types_tests.rs"]
mod user_types_tests;

#[cfg(test)]
mod catalog_text_cast_input_tests {
    use super::*;

    #[test]
    fn integer_wire_declarations_are_limited_to_explicit_text_cast_operands() {
        let plan = giggles_pg_catalog::plan("select $1::varchar from pg_catalog.pg_database")
            .unwrap()
            .unwrap();
        let inferred = plan.parameter_types()[0].oid();
        for declared in [20, 21, 23, 25, 1043] {
            assert!(catalog_input_override(&plan, 0, declared, inferred));
            let ty = catalog_wire::parameter_type(declared).unwrap();
            for format in [0, 1] {
                let value = if matches!(declared, 20 | 21 | 23) {
                    giggles_pg_catalog::Value::Int(-123)
                } else {
                    giggles_pg_catalog::Value::Text("-123".into())
                };
                let bytes = catalog_wire::encode(&value, ty, format).unwrap().unwrap();
                let decoded = catalog_wire::decode(&bytes, ty, format).unwrap();
                let result =
                    giggles_pg_catalog::execute(&plan, &Default::default(), &[decoded]).unwrap();
                assert_eq!(
                    result.rows,
                    [vec![giggles_pg_catalog::Value::Text("-123".into())]]
                );
            }
        }
        for declared in [16, 17, 19, 26, 28, 700, 701, 1700, 1007] {
            assert!(
                !catalog_input_override(&plan, 0, declared, inferred),
                "{declared}"
            );
        }
        for sql in [
            "select $1 from pg_catalog.pg_database",
            "select $1::varchar, $1 from pg_catalog.pg_database",
            "select $1::varchar from pg_catalog.pg_database where oid=$1",
            "select pg_catalog.age($1::xid)",
        ] {
            let plan = giggles_pg_catalog::plan(sql).unwrap().unwrap();
            for declared in [20, 21, 23] {
                assert!(
                    !catalog_input_override(&plan, 0, declared, plan.parameter_types()[0].oid()),
                    "{sql}: {declared}"
                );
            }
        }
    }
}

#[cfg(test)]
mod replication_role_diagnostic_tests {
    use super::*;
    #[test]
    fn pg_settings_replication_role_value_diagnostic() {
        let mut diagnostic = Diagnostic::default();
        assert_eq!(diagnostic.replication_role_value("REPLICA"), Ok(()));
        assert!(diagnostic.wire_hint.is_none());
        assert_eq!(diagnostic.replication_role_value("Bogus"), Err("22023"));
        assert_eq!(diagnostic.wire_message.as_deref(), Some("invalid value for parameter \"session_replication_role\": \"Bogus\""));
        assert_eq!(diagnostic.wire_hint, Some("Available values: origin, replica, local."));
        let mut out = vec![];
        diagnostic_error("22023", GENERIC_MESSAGE, Some(&diagnostic), &mut out);
        assert_eq!(out[0], b'E');
        let fields: Vec<_> = out[5..].split(|b| *b == 0).collect();
        assert!(fields.contains(&b"HAvailable values: origin, replica, local.".as_slice()));
        diagnostic_error("42501", GENERIC_MESSAGE, Some(&Diagnostic::default()), &mut out);
        assert!(!out[5..].split(|b| *b == 0).any(|f| f.first() == Some(&b'H')));
    }
}

#[cfg(test)]
mod collation_diagnostic_tests {
    use super::*;
    #[test]
    fn expression_collation_errors_keep_postgres_diagnostics() {
        let path = std::env::temp_dir().join(format!("collation-wire-{}-{}",std::process::id(),std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
        let admin = giggles_core::Admin::create(&path,giggles_core::Limits::default()).unwrap();
        let mut session = admin.sql_session();
        for (sql, code, message) in [
            (r#"SELECT 1 COLLATE "C""#, "42804", "collations are not supported by type integer"),
            (r#"SELECT 'a' COLLATE "UND-X-ICU""#, "42704", "collation \"UND-X-ICU\" for encoding \"UTF8\" does not exist"),
            (r#"SELECT 'a' COLLATE "C" < 'B' COLLATE "und-x-icu""#, "42P21", "collation mismatch between explicit collations \"C\" and \"und-x-icu\""),
        ] {
            let mut diagnostic = Diagnostic::default();
            assert_eq!(diagnostic.core(session.execute(sql,&[],None)).unwrap_err(),code);
            assert_eq!(diagnostic.wire_message.as_deref(),Some(message));
            let mut frame = Vec::new();
            diagnostic_error(code,"fallback",Some(&diagnostic),&mut frame);
            assert!(frame.windows(message.len()).any(|w| w == message.as_bytes()));
        }
        drop(session); drop(admin);
        if path.is_dir() { std::fs::remove_dir_all(path).unwrap(); } else { std::fs::remove_file(path).unwrap(); }
    }
}
#[cfg(all(test, feature = "engine-rocksdb"))]
#[path = "pgwire_implicit_block_tests.rs"]
mod implicit_block_tests;

#[cfg(test)]
static UNNAMED_REUSE_DISABLED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
#[cfg(test)]
pub(crate) static UNNAMED_REAUTHORIZE_FAIL: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
#[cfg(test)]
static UNNAMED_REUSE_MISSES: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
#[cfg(test)]
static UNNAMED_REUSE_HITS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
#[cfg(all(test, feature = "engine-rocksdb"))]
#[path = "../tests/unnamed_parse_network.inc"]
mod unnamed_parse_network;

#[cfg(test)]
static UNNAMED_BEFORE_REAUTHORIZE: std::sync::Mutex<Option<Box<dyn FnOnce() + Send>>> = std::sync::Mutex::new(None);
#[cfg(test)]
static UNNAMED_CONTEXT_MISSES: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
