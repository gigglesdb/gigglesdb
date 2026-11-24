//! Explicit protocol-v3 subset; not PostgreSQL server compatibility.
use crate::{
    Role, Session, pg_catalog_wire as catalog_wire,
    transport::{IO_TIMEOUT, MAX_FRAME, MAX_RESPONSE},
};
#[cfg(feature = "legacy-task")]
use crate::Request;
use giggles_core::{
    CoreError,
    relational::{DataType, Description, DuplicateKeyDetail, Prepared, SqlErrorDetail, SqlResult, Value},
    task::Released,
};
use giggles_pg_types::temporal::SessionZone;
use std::{borrow::Borrow, collections::{BTreeMap, btree_map::Entry}, io, sync::Arc};
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
#[path = "pgwire_prepare.rs"]
mod prepare;
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
    append_frame(tag, b, u32::try_from(len).map_err(|_| "54000")?, out);
    Ok(())
}

fn append_frame(tag: u8, body: &[u8], length: u32, out: &mut Vec<u8>) {
    out.push(tag);
    out.extend(length.to_be_bytes());
    out.extend(body);
}

#[derive(Clone, Copy)]
enum ReplyLimit {
    Checked,
    Control,
}

impl ReplyLimit {
    fn after_commit(committed: bool) -> Self {
        if committed { Self::Control } else { Self::Checked }
    }

    fn frame(self, tag: u8, body: &[u8], out: &mut Vec<u8>) -> R<()> {
        match self {
            Self::Checked => frame(tag, body, out),
            Self::Control => {
                // The reply size limit applies before publication. After a commit
                // (or when ending an aborted reply), send bounded control frames
                // even if the buffer is full. Data frames must pass the checked
                // encoder before commit, including spills validated before replay.
                debug_assert!(matches!(tag, b'C' | b'N' | b'S' | b'Z' | b'E'));
                let length = u32::try_from(body.len() + 4).expect("bounded control frame");
                append_frame(tag, body, length, out);
                Ok(())
            }
        }
    }
}

pub fn z(s: &str, b: &mut Vec<u8>) {
    b.extend(s.as_bytes());
    b.push(0);
}
fn ready(session: &Session, out: &mut Vec<u8>) -> R<()> {
    ReplyLimit::Control.frame(
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
    committed: bool,
    // A failed commit restores session settings before its error is framed.
    pre_rollback_settings: Option<Box<crate::pg_settings::Settings>>,
    phase: &'static str,
    message: Option<&'static str>,
    wire_message: Option<String>,
    wire_detail: Option<String>,
    wire_context: Option<String>,
    wire_hint: Option<std::borrow::Cow<'static, str>>,
    wire_registry_error: Option<crate::pg_settings::registry::Error>,
    sql_detail: Option<String>,
    duplicate_key: Option<DuplicateKeyDetail>,
    duplicate_key_visible: Option<bool>,
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
fn settings_read_allowed(session: &Session) -> R<bool> {
    if replication_role_allowed(session)? {
        return Ok(true);
    }
    let Role::Database { name } = session.authority_role() else {
        return Ok(false);
    };
    Ok(name == "pg_read_all_settings" || session.service.admin.roles().privileges_of(name)
        .map_err(|_| "42501")?.contains("pg_read_all_settings"))
}
impl Diagnostic {
    fn reply_error(&self, code: &'static str) -> &'static str {
        if self.committed { "08007" } else { code }
    }

    fn registry_error(&mut self, error: crate::pg_settings::registry::Error) -> &'static str {
        let code = error.code;
        self.wire_registry_error = Some(error);
        code
    }

    /// GD-0061 R3-110: a task statement's error keeps its SQLSTATE and message.
    fn task_ddl(&mut self, error: giggles_core::task::catalog::TaskDdlError) -> &'static str {
        self.wire_message = Some(error.message);
        error.sqlstate
    }
    fn read_core_detail(&mut self) -> Option<String> {
        match giggles_core::relational::take_sql_diagnostic_detail() {
            Some(SqlErrorDetail::DuplicateKey(detail)) => {
                self.duplicate_key = Some(detail);
                self.duplicate_key_visible = None;
                None
            }
            Some(SqlErrorDetail::ForeignKey { key, redacted }) => {
                self.duplicate_key = Some(key);
                self.duplicate_key_visible = None;
                Some(redacted)
            }
            Some(SqlErrorDetail::Text(detail)) => Some(detail),
            None => None,
        }
    }

    fn authorize_duplicate_key(&mut self, code: &str, session: &mut Session) {
        if matches!(code, "23503" | "23505")
            && self.duplicate_key_visible.is_none()
            && let Some(detail) = &self.duplicate_key
        {
            self.duplicate_key_visible = Some(session.can_read_duplicate_key(detail));
        }
    }

    fn catalog_error(&mut self, error: giggles_pg_catalog::Error) -> &'static str {
        use giggles_pg_catalog::Error as E;
        if matches!(&error, E::InvalidTypeModifier(_) | E::TypeModifierNotAllowed(_)
            | E::TypeNameSyntax { .. })
        {
            self.wire_message = Some(error.to_string());
        }
        if let E::TypeNameSyntax { input: Some(input), .. } = &error {
            let input = input.replace('\0', "\\0");
            self.wire_context = Some(format!(r#"invalid type name "{input}""#));
        }
        catalog_wire::error(error)
    }

    fn oid_text_error(&mut self, code: &str, text: &str) {
        let text = text.replace('\0', "\\0");
        self.wire_message = match code {
            "22003" => Some(format!(r#"value "{text}" is out of range for type oid"#)),
            "22P02" => Some(format!(r#"invalid input syntax for type oid: "{text}""#)),
            _ => None,
        };
    }

    fn core<T>(&mut self, result: Result<T, CoreError>) -> R<T> {
        let (message, context) = giggles_core::relational::take_sql_error_message_with_context();
        if result.is_err() && context.is_some() {
            self.wire_context = context;
        }
        if result.is_err() && message.as_deref().is_some_and(|m|
            m.starts_with("FOR ") || m.starts_with("syntax error at or near ")
                || m == "syntax error at end of input"
                || matches!(m, "invalid Unicode escape" | "invalid Unicode surrogate pair" | "unequal number of entries in row expressions")
                || m.starts_with("operator does not exist: ") || m.starts_with("operator is not unique: ")
                || m == "VARIADIC argument must be an array"
                || m.starts_with("function pg_catalog.normalize(") || m.starts_with("function num_nulls(")
                || m.starts_with("function num_nonnulls(") || m.starts_with("invalid input syntax for type ") || m.starts_with("could not obtain lock on row ")
                || m.starts_with("currval of sequence ") || m == "lastval is not yet defined in this session"
                || m.starts_with("enum label \"") || m.starts_with("invalid input value for enum ")
                || m.ends_with(" is not an enum") || m.starts_with("type \"") && m.ends_with(" does not exist")
                || m.starts_with("constraint \"") && m.ends_with(" does not exist")
                || m.starts_with("cannot drop constraint ") || m.starts_with("could not create unique index ")
                || m.starts_with("duplicate key value violates unique constraint ")
                || m == "canceling statement due to lock timeout"
                || m.starts_with("type modifier is not allowed for type ")
                || m.starts_with("permission denied for table ") || m.starts_with("view ") || m.starts_with("sequence ") || m.starts_with("index ")
                || m.starts_with("relation ") && m.contains(" clause not found in FROM clause")
                || m.starts_with("invalid reference to FROM-clause entry for table ")
                || m.starts_with("column ") && m.contains(" does not exist")
                || m.starts_with("table ") && m.contains(" columns available but ")
                || ["UNION types ", "INTERSECT types ", "EXCEPT types "].iter().any(|prefix| m.starts_with(prefix))
                    && m.ends_with(" cannot be matched")
                || matches!(m, "VALUES lists must all be the same length"
                    | "in an aggregate with DISTINCT, ORDER BY expressions must appear in argument list"
                    | "more than one row returned by a subquery used as an expression"
                    | "ON CONFLICT DO UPDATE command cannot affect row a second time")
                || m.starts_with("cannot assign to field ") && m.ends_with(" is not a composite type")
                || matches!(m, "field name must not be null" | "null value not allowed for object key"
                    | "key value must be scalar, not array, composite, or json"
                    | "could not identify an equality operator for type json")
                || m.starts_with("cannot extract elements from ")
                || m.starts_with("cannot call jsonb_")
                || m == "cannot set path in scalar"
                || m.starts_with("path element at position ")
                || m.starts_with("ORDER BY \"") && m.ends_with("\" is ambiguous")) {
            self.wire_message = message.clone();
        }
        self.sql_detail = self.read_core_detail();
        self.wire_hint = giggles_core::relational::take_sql_diagnostic_hint();
        use giggles_core::relational::SqlError;
        let postgres_diagnostic = match (&result, message.as_deref()) {
            (Err(CoreError::Sql(SqlError::InvalidIndexPredicate)), Some(m)) => {
                m == "functions in index predicate must be marked IMMUTABLE"
            }
            (Err(CoreError::Sql(SqlError::UndefinedParameter)), Some(m)) => {
                m.starts_with("there is no parameter $")
            }
            (Err(CoreError::Sql(SqlError::GroupingError)), Some(m)) => {
                m == "aggregate functions are not allowed in index predicates"
            }
            (Err(CoreError::Sql(SqlError::InvalidColumnReference)), Some(m)) => {
                m == "there is no unique or exclusion constraint matching the ON CONFLICT specification"
            }
            (Err(CoreError::Sql(SqlError::UnknownTable)), Some(m)) => {
                m.starts_with("missing FROM-clause entry for table ")
            }
            (Err(CoreError::Sql(SqlError::UnknownColumn)), Some(m)) => {
                m.starts_with("column \"") && m.ends_with(" does not exist")
            }
            (Err(CoreError::Sql(SqlError::Type)), Some(m)) => {
                m.starts_with("argument of WHERE must be type boolean, not type ")
            }
            (Err(CoreError::Sql(SqlError::UndefinedObject)), Some(m)) => {
                m.starts_with("constraint \"") && m.contains(" for table \"") && m.ends_with(" does not exist")
            }
            (Err(CoreError::Sql(SqlError::Duplicate)), Some(m)) if self.duplicate_key.is_some() => {
                m.starts_with("duplicate key value violates unique constraint ")
                    || m.starts_with("could not create unique index ")
            }
            (Err(CoreError::Sql(SqlError::ForeignKeyViolation)), Some(m)) if self.duplicate_key.is_some() => {
                m.starts_with("insert or update on table ")
            }
            (Err(CoreError::Sql(SqlError::UndefinedFunction | SqlError::AmbiguousFunction)), Some(m)) => {
                m.strip_prefix("function ")
                    .and_then(|m| m.split_once('('))
                    .is_some_and(|(name, _)| matches!(
                        name.strip_prefix("pg_catalog.").unwrap_or(name),
                        "gen_random_uuid" | "uuidv4" | "uuidv7" | "random" | "setseed"
                            | "uuid_extract_version" | "uuid_extract_timestamp"
                    ))
            }
            (Err(CoreError::Sql(SqlError::DatetimeFieldOverflow)), Some(m)) => matches!(
                m, "timestamp out of range" | "timestamp out of range for UUID version 7"
                    | "interval out of range for UUID version 7"
            ),
            _ => false,
        };
        if postgres_diagnostic {
            self.wire_message = message.clone();
        }
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
                    | giggles_core::relational::SqlError::InvalidArgumentForWidthBucket
                    | giggles_core::relational::SqlError::NullValueNotAllowed
                    | giggles_core::relational::SqlError::InvalidName
                    | giggles_core::relational::SqlError::UnsafeNewEnumValue
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
            self.wire_message = message.clone();
        }
        if let Err(CoreError::Sql(error)) = &result
            && let Some(message) = message
        {
            use giggles_core::relational::SqlError;
            let column_message = message.starts_with("column \"") && message.contains(" of relation \"");
            let common_type_context = |separator| {
                message.split_once(separator).is_some_and(|(context, _)| matches!(
                    context,
                    "UNION" | "INTERSECT" | "EXCEPT" | "VALUES" | "COALESCE"
                        | "CASE" | "CASE/WHEN" | "GREATEST" | "LEAST" | "ARRAY"
                ))
            };
            let allowed = match error {
                SqlError::CannotCoerce => common_type_context(" could not convert type "),
                SqlError::UndefinedFunction => message.ends_with(") does not exist")
                    && ["function lag(", "function lead(", "function width_bucket("]
                        .iter()
                        .any(|prefix| message.starts_with(prefix)),
                SqlError::InvalidTextRepresentation => message.starts_with("invalid input value for enum "),
                SqlError::UnknownColumn => column_message && message.ends_with(" does not exist"),
                SqlError::DuplicateColumn => column_message && message.ends_with(" already exists"),
                SqlError::UndefinedObject => message.starts_with("type \"") && message.ends_with(" does not exist"),
                SqlError::Type => (message.starts_with("column \"") || message.starts_with("default for column \""))
                    && message.contains(" cannot be cast automatically to type ")
                    || message.ends_with(" cannot be matched") && common_type_context(" types "),
                _ => false,
            };
            if allowed {
                self.wire_message = Some(message);
            }
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
                self.wire_hint = Some("Use CASCADE to revoke them too.".into());
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
            self.wire_hint = Some("Available values: origin, replica, local.".into());
        })
    }
    fn presentation_setting(
        &mut self,
        name: crate::pg_settings::Show,
        value: &str,
        current: &crate::pg_settings::Settings,
    ) -> R<()> {
        use crate::pg_settings::{Show, database};
        match name {
            Show::DateStyle => database::date_style(value, current.datestyle)
                .map(|_| ())
                .map_err(|error| {
                    self.wire_message = Some(format!("invalid value for parameter \"DateStyle\": \"{value}\""));
                    self.wire_detail = Some(error.detail());
                    "22023"
                }),
            Show::ByteaOutput => database::bytea_output(value).map(|_| ()).inspect_err(|_| {
                self.wire_message = Some(format!("invalid value for parameter \"bytea_output\": \"{value}\""));
                self.wire_hint = Some("Available values: escape, hex.".into());
            }),
            _ => Ok(()),
        }
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
    // Notices already produced by COPY input functions precede a later error.
    // Keep these frames when discarding a partial result response.
    let mut notices = vec![];
    let mut at = 0;
    while at + 5 <= out.len() {
        let length = u32::from_be_bytes(out[at + 1..at + 5].try_into().unwrap()) as usize;
        let Some(end) = at.checked_add(1).and_then(|n| n.checked_add(length)) else { break; };
        if length < 4 || end > out.len() { break; }
        if out[at] == b'N' { notices.extend_from_slice(&out[at..end]); }
        at = end;
    }
    out.clear();
    out.extend(notices);
    let mut b = vec![b'S'];
    z("ERROR", &mut b);
    b.push(b'C');
    z(code, &mut b);
    b.push(b'M');
    let registry_error = diagnostic.and_then(|d| d.wire_registry_error.as_ref());
    let wire_message = registry_error.map(|error| error.message.as_str())
        .or_else(|| diagnostic.and_then(|d| d.wire_message.as_deref()));
    z(wire_message.unwrap_or(message), &mut b);
    if let Some((diagnostic, key)) = diagnostic
        .filter(|_| matches!(code, "23503" | "23505"))
        .and_then(|d| d.duplicate_key.as_ref().map(|key| (d, key)))
    {
        let detail = if diagnostic.duplicate_key_visible == Some(true) {
            Some(key.values.as_str())
        } else {
            diagnostic.sql_detail.as_deref()
        };
        if let Some(detail) = detail {
            b.push(b'D');
            z(detail, &mut b);
        }
        for (field, name) in [(b's', &key.schema), (b't', &key.table), (b'n', &key.constraint)] {
            b.push(field);
            z(name, &mut b);
        }
    } else if let Some(detail) = match registry_error {
        Some(error) => error.detail,
        None => detail.as_deref().map(|detail| {
            diagnostic
                .and_then(|d| d.sql_detail.as_deref().or(d.wire_detail.as_deref()))
                .unwrap_or(detail)
        }),
    } {
        b.push(b'D');
        z(detail, &mut b);
    }
    if let Some(context) = diagnostic.and_then(|d| d.wire_context.as_deref()) { b.push(b'W'); z(context, &mut b); }
    if let Some(hint) = registry_error.and_then(|error| error.hint.as_deref())
        .or_else(|| diagnostic.and_then(|d| d.wire_hint.as_deref())) {
        b.push(b'H');
        z(hint, &mut b);
    }
    if code == "0A000" && diagnostic.and_then(|d| d.wire_message.as_deref()) == Some("view projection bindings unavailable; recreate the view") {
        b.push(b'H');
        z("Recreate the view to store projection bindings.", &mut b);
    }
    b.push(0);
    let _ = ReplyLimit::Control.frame(b'E', &b, out);
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
        DataType::UuidArray => 2951,
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
pub(crate) fn result_type_oids(d: &Description) -> Vec<u32> {
    d.columns.iter().enumerate().map(|(i, c)| {
        d.type_oids.get(i).copied().filter(|id| *id != 0)
            .unwrap_or_else(|| oid(&c.data_type))
    }).collect()
}
fn size(t: &DataType) -> i16 {
    match t {
        DataType::RegType => 4,
        DataType::Uuid => 16,
        DataType::Jsonb | DataType::IntArray | DataType::BigIntArray | DataType::TextArray
        | DataType::UuidArray => -1,
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
    read_formats(r, n)
}
fn read_formats(r: &mut Reader<'_>, count: usize) -> R<Vec<u16>> {
    let mut v = Vec::with_capacity(count);
    for _ in 0..count {
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
    let type_oids = result_type_oids(d);
    for (i, c) in d.columns.iter().enumerate() {
        if c.name.contains('\0') {
            return Err("XX000");
        }
        z(&c.name, &mut b);
        b.extend(0u32.to_be_bytes());
        b.extend(0i16.to_be_bytes());
        b.extend(type_oids[i].to_be_bytes());
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
    decode_with_order(b, ty, format, zone, transaction_us, giggles_pg_types::temporal::DateOrder::Mdy)
}

fn decode_with_order(
    b: &[u8],
    ty: &DataType,
    format: u16,
    zone: SessionZone,
    transaction_us: i64,
    date_order: giggles_pg_types::temporal::DateOrder,
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
                    date_order,
                    offset_east: 0,
                },
            )
            .map(Value::Date)
            .map_err(temporal_error),
            DataType::Time => giggles_core::relational::PgTime::parse(s)
                .map(Value::Time)
                .map_err(temporal_error),
            DataType::TimeTz => giggles_pg_types::temporal::TimeTz::parse_in_session(
                s,
                &giggles_pg_types::temporal::SessionSettings { date_order, zone },
                transaction_us,
            )
            .map(Value::TimeTz)
            .map_err(temporal_error),
            DataType::Timestamp => giggles_core::relational::Timestamp::parse(
                s,
                &giggles_pg_types::temporal::ParserSettings {
                    date_order,
                    offset_east: 0,
                },
            )
            .map(Value::Timestamp)
            .map_err(temporal_error),
            DataType::TimestampTz => giggles_core::relational::TimestampTz::parse_in_session(
                s,
                &giggles_pg_types::temporal::SessionSettings {
                    date_order,
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
            | DataType::TextArray
            | DataType::UuidArray => {
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
        return logical_binary_input(ty, b, None);
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
        giggles_pg_types::Error::UnknownTimeZone => "22023",
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
        | giggles_pg_types::Error::UnknownTimeZone
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
            Value::Date(v) => v.format_with_style(settings.datestyle.style, settings.datestyle.order).into_bytes(),
            Value::Time(v) => v.to_string().into_bytes(),
            Value::TimeTz(v) => v.to_string().into_bytes(),
            Value::Timestamp(v) => v.format_with_style(settings.datestyle.style, settings.datestyle.order).into_bytes(),
            Value::TimestampTz(v) => v.format_with_style(zone, settings.datestyle.style, settings.datestyle.order)
                .map_err(temporal_error)?.into_bytes(),
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
            Value::Bytes(b) => {
                if settings.bytea_output.text_len(b) > MAX_RESPONSE {
                    return Err("54000");
                }
                settings.bytea_output.format(b).into_bytes()
            }
        }));
    }
    Ok(Some(match (v, ty) {
        (Value::Text(s), ty) if ty.is_extended_text() => logical_binary_output(ty, s, 0)?,
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
    data_row_frames(settings, zone, result, d, formats, start..end, out)
}

// The pre-publication check borrows a released result rather than copying it.
fn data_row_frames(
    settings: &crate::pg_settings::Settings,
    zone: SessionZone,
    result: &Released<impl Borrow<SqlResult>>,
    d: &Description,
    formats: &[u16],
    range: std::ops::Range<usize>,
    out: &mut Vec<u8>,
) -> R<()> {
    for row in &result.get().borrow().rows[range] {
        let mut b = vec![];
        b.extend((row.values.len() as u16).to_be_bytes());
        for (i, v) in row.values.iter().enumerate() {
            let ty = &d.columns.get(i).ok_or("XX000")?.data_type;
            let format = format_at(formats, i);
            let type_oid = d.type_oids.get(i).copied().unwrap_or(0);
            let bytes = if format == 1 && *ty == DataType::TextArray
                && type_oid >= 65536 && let Value::Text(text) = v
            {
                Some(logical_binary_output(ty, text, type_oid)?)
            } else {
                encode(v, ty, format, settings, zone)?
            };
            match bytes {
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
    completion_frames(command, r, out, ReplyLimit::Checked)
}

fn completion_frames(
    command: &str,
    r: &Released<impl Borrow<SqlResult>>,
    out: &mut Vec<u8>,
    limit: ReplyLimit,
) -> R<()> {
    let r = r.get().borrow();
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
        limit.frame(b'N', &body, out)?;
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
        limit.frame(b'N', &body, out)?;
    }
    let tag = match command {
        "SELECT" => format!("SELECT {}", r.rows.len()),
        "INSERT" => format!("INSERT 0 {}", r.affected),
        "UPDATE" | "DELETE" | "MERGE" => format!("{command} {}", r.affected),
        _ => command.into(),
    };
    let mut b = vec![];
    z(&tag, &mut b);
    limit.frame(b'C', &b, out)
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
    // This routing hint skips catalog planning. Core still parses,
    // authorizes and classifies the original statement.
    if giggles_pg_catalog::application_read_hint(sql) {
        return Ok(None);
    }
    // Sequence and extension calls use the database execution context, never
    // the immutable catalog evaluator or the positive table-free allowlist.
    if !Prepared::sequence_calls_from_sql(sql).is_empty() || Prepared::extension_routine_call(sql) {
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
        // A table-free expression the catalog declines goes to core only for a
        // database role core authorizes as superuser (effective role, as in
        // Session::authorize), so core evaluates it or returns its own
        // per-routine SQLSTATE. Core still parses and authorizes the whole
        // statement. Other roles keep 0A000: core's conservative
        // table-authorization target would turn it into 42501.
        Ok(None)
            if !matches!(session.table_privilege_superuser(), Ok(true))
                && Prepared::rejects_unsupported_table_free_scalar(sql) =>
        {
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
        2951 => Some(DataType::UuidArray),

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
        .map(|(plan, description, _)| (plan, description))
}

fn prepare_declared_sql_diagnostic(
    diagnostic: &mut Diagnostic,
    session: &mut Session,
    sql: &str,
    oids: &[u32],
    unnamed: bool,
) -> R<(Plan, Description, giggles_core::relational::EnumParameters)> {
    let declared = oids
        .iter()
        .map(|&o| core_declared_type(o))
        .collect::<Vec<_>>();
    let prepared = diagnostic.core(if unnamed { session.prepare_unnamed(sql, &declared) } else { session.prepare_with_parameter_types(sql, &declared) })?;
    let (description, enum_parameters) = if prepared.is_ddl() {
        (diagnostic.core(session.describe(&prepared))?, Default::default())
    } else {
        diagnostic.core(session.sql.describe_with_enum_parameters(&prepared))?
    };
    if oids.len() > description.parameters.len() {
        return Err("08P01");
    }
    Ok((Plan::Sql(Box::new(prepared)), description, enum_parameters))
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

enum Plan {
    Explain(giggles_pg_catalog::ExplainQuery),
    Empty,
    Cursor(cursor::Command),
    PreparedCommand(Box<prepare::Invocation>),
    AccessShare(crate::auth::statements::TableLockRequest),
    Admin(crate::auth::statements::AdminStatement),
    Control(crate::pg_transaction::Command),
    Settings(Vec<crate::pg_settings::Setting>),
    Inspect(crate::pg_settings::SettingName),
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
    id: u64,
    // Syntax-only dependencies: binding may qualify names or add defaults.
    sequence_dependencies: std::sync::Arc<[giggles_core::relational::SequenceDependency]>,
    // Only an unnamed application SELECT prepared outside a transaction gets a key.
    parse_context: Option<std::sync::Arc<ParseContext>>,
    description_version: Option<u64>,
    validated: Option<(u64, usize, usize, u64)>,
    #[cfg(test)]
    constant_validations: usize,
    sql_bytes: usize,
    origin: Option<Box<prepare::Origin>>,
    discarded: bool,
    sql: String,
    declared_oids: Vec<u32>,
    // Parse finishes building both before a statement can be cached or bound.
    p: Arc<Plan>,
    d: Arc<Description>,
    parameter_oids: Vec<u32>,
    enum_parameters: giggles_core::relational::EnumParameters,
    query_id: Option<String>,
}
impl Statement {
    fn catalog_fields(&self) -> Option<&[giggles_pg_catalog::Field]> {
        match self.p.as_ref() {
            Plan::Catalog(_, fields, _) => Some(fields),
            Plan::PreparedCommand(invocation) => invocation.target.as_ref()
                .and_then(|target| target.catalog_fields()),
            _ => None,
        }
    }

    fn same_result(&self, other: &Self) -> bool {
        self.d.columns.len() == other.d.columns.len()
            && self.d.columns.iter().zip(&other.d.columns).all(|(old, new)| {
                old.name == new.name && old.data_type == new.data_type
            })
            && self.d.type_oids == other.d.type_oids
            && self.d.typmods == other.d.typmods
            && self.catalog_fields() == other.catalog_fields()
    }

    fn execute_result_mismatch(&self, live: &Self) -> Option<String> {
        let expected_oids = result_type_oids(&self.d);
        let returned_oids = result_type_oids(&live.d);
        for (i, (expected_oid, returned_oid)) in expected_oids.into_iter().zip(returned_oids).enumerate() {
            let expected_typmod = self.d.typmods.get(i).copied().unwrap_or(-1);
            let returned_typmod = live.d.typmods.get(i).copied().unwrap_or(-1);
            if returned_oid != expected_oid || expected_typmod >= 0 && returned_typmod != expected_typmod {
                let name = |oid| prepare::argument_type_name(oid).map_or_else(
                    |_| format!("type with OID {oid}"),
                    str::to_owned,
                );
                return Some(format!(
                    r#"Returned type {} does not match expected type {} in column "{}" (position {})."#,
                    name(returned_oid), name(expected_oid), self.d.columns[i].name, i + 1,
                ));
            }
        }
        (self.d.columns.len() != live.d.columns.len()).then(|| format!(
            "Number of returned columns ({}) does not match expected column count ({}).",
            live.d.columns.len(), self.d.columns.len(),
        ))
    }

    fn forget_cursor_plan(&self, session: &Session) {
        let prepared = match &*self.p {
            Plan::Sql(prepared) if prepared.command() == "SELECT" => prepared,
            Plan::Cursor(cursor::Command::Declare { prepared: Some(prepared), .. }) => prepared,
            _ => return,
        };
        session.sql.forget_cursor_plan(prepared);
    }

    // Binding reads the immutable Prepared (including its captured search_path,
    // types, schema user and literal DateOrder) and the catalog. Query binding installs catalog enum
    // and collation scopes; plan_constant excludes runtime/session nodes and only
    // evaluates immutable calls/casts. The two lexer/value budgets are the only
    // ambient limits. Keep a schema-context revision as an extra invalidator.
    fn validate_constants(&mut self, session: &mut Session, diagnostic: &mut Diagnostic) -> R<()> {
        let Plan::Sql(p) = &*self.p else { return Ok(()); };
        let key = session.sql.constants_validation_key();
        if key.is_some() && self.validated == key && session.sql.prepared_is_current(p) {
            core(session.sql.constants_validated_preflight(p))?;
            // The preflight inspected metadata after the key was read. Versions only
            // grow, so an unchanged key proves that inspection saw the validated
            // generation; otherwise a concurrent DDL landed in between, so fall
            // through to the full validation against the current catalog.
            if session.sql.constants_validation_key() == key {
                return Ok(());
            }
        }
        self.validated = None;
        #[cfg(test)]
        { self.constant_validations += 1; }
        diagnostic.core(session.sql.validate_immutable_constants(p))?;
        // Do not publish a success if the visible catalog moved during validation.
        self.validated = key.filter(|_| key == session.sql.constants_validation_key());
        Ok(())
    }

    fn authorize_description(&self, session: &mut Session) -> R<()> {
        if let Plan::PreparedCommand(invocation) = self.p.as_ref()
            && let Some(target) = &invocation.target
        {
            target.authorize_execute_description(session)?;
        }
        if let Plan::Sql(p) = self.p.as_ref()
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

    fn describe(&self, session: &mut Session) -> R<Arc<Description>> {
        match &*self.p {
            #[cfg(feature = "legacy-task")]
            Plan::Task(_) => Ok(Arc::clone(&self.d)),
            // A task session holds only relation-free catalog plans, which
            // read no catalog (R3-206).
            Plan::Catalog(..) if session.task().is_some() => Ok(Arc::clone(&self.d)),
            Plan::Catalog(plan, _, epoch) => {
                let (current, _, _) = core(session.catalog_snapshot())?;
                if current != *epoch && !plan.refreshes_view_definitions() {
                    return Err("0A000");
                }
                Ok(Arc::clone(&self.d))
            }
            Plan::Sql(p) => {
                if self.description_version.is_some()
                    && self.description_version == session.sql.statement_catalog_version()
                    && session.sql.prepared_is_current(p)
                { return Ok(Arc::clone(&self.d)); }
                let d = core(session.describe(p))?;
                if self.d.columns.len() != d.columns.len()
                    || self.d.columns.iter().zip(&d.columns).any(|(old, new)| {
                        old.name != new.name || old.data_type != new.data_type
                    })
                    || self.d.type_oids != d.type_oids || self.d.typmods != d.typmods
                {
                    return Err("0A000");
                }
                Ok(Arc::new(d))
            },
            Plan::Copy(copy, p) => {
                core(session.copy_columns(copy, p))?;
                Ok(Arc::clone(&self.d))
            }
            Plan::CopyOut(_) | Plan::Explain(_) | Plan::Cursor(_) | Plan::PreparedCommand(_)
            | Plan::AccessShare(_)
            | Plan::Admin(_)
            | Plan::Empty
            | Plan::Settings(_)
            | Plan::Control(_)
            | Plan::SettingsFunction(_)
            | Plan::Inspect(_)
            | Plan::Protected(_)
            | Plan::Change(_)
            | Plan::TaskDdl(_) => Ok(Arc::clone(&self.d)),
        }
    }
}
struct Portal {
    s: Statement,
    // EXECUTE can run a replacement statement while retaining the result
    // description promised when the portal was bound.
    bound_execute_target: Option<Box<Statement>>,
    statement_name: Option<String>,
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
    fn new(
        s: Statement, params: Vec<Value>, catalog_params: Vec<giggles_pg_catalog::Value>,
        formats: Vec<u16>, parameter_bytes: usize,
    ) -> Self {
        Self { s, bound_execute_target: None, statement_name: None, params, formats, result: None, spilled: None, cursor: None,
            catalog_params, catalog_result: None, parameter_bytes, at: 0 }
    }

    fn has_live_statement(&self, statements: &BTreeMap<String, Statement>) -> bool {
        // A bound portal can outlive its statement, but only the live entry
        // can release the binding on statement close or replacement.
        self.statement_name.as_ref().and_then(|name| statements.get(name))
            .is_some_and(|statement| Arc::ptr_eq(&statement.p, &self.s.p))
    }

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
#[derive(Default)]
struct OutputStatus {
    flushed: std::sync::atomic::AtomicBool,
    released_permits: std::sync::atomic::AtomicBool,
}
struct OutputChannel {
    status: std::sync::Arc<OutputStatus>,
    sender: tokio::sync::mpsc::Sender<Vec<u8>>,
    prefix: std::sync::Mutex<Vec<u8>>,
}
type Output = Option<OutputChannel>;
fn flush_output(output: &Output, out: &mut Vec<u8>) -> R<()> {
    if let Some(output) = output {
        output.status.flushed.store(true, std::sync::atomic::Ordering::Release);
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
    // COPY and spilled rows exclude the entire transaction from permit release.
    retain_durability_permits: bool,
    settings: crate::pg_settings::SessionSettings,
    next_statement_id: u64,
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

#[derive(Default)]
struct ExecutionPermits {
    role_permit: Option<crate::admission::RolePermit>,
    blocking: Option<tokio::sync::OwnedSemaphorePermit>,
}
struct ExecutionAdmission {
    permits: std::sync::Arc<std::sync::Mutex<ExecutionPermits>>,
    runtime: tokio::runtime::Handle,
    disconnected: tokio::sync::watch::Receiver<()>,
    context: giggles_core::Cancellation,
    first: bool,
}
impl ExecutionAdmission {
    fn permits(&self) -> std::sync::MutexGuard<'_, ExecutionPermits> {
        self.permits.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn reacquire(&mut self, session: &Session, needs_role: bool) -> Result<(), crate::admission::StatementRefusal> {
        use crate::admission::StatementRefusal;

        let old = std::mem::take(&mut *self.permits());
        drop(old);
        let permits = self.runtime.block_on(async {
            tokio::select! {
                biased;
                _ = self.disconnected.changed() => Err(StatementRefusal::Core(CoreError::Cancelled)),
                admitted = async {
                    let role_permit = if needs_role {
                        Some(session.service.admission.statement(&session.role, Some(&self.context)).await?)
                    } else {
                        None
                    };
                    let blocking = session.service.blocking.admit(&self.context).await.map_err(StatementRefusal::Core)?;
                    Ok(ExecutionPermits { role_permit, blocking: Some(blocking) })
                } => admitted,
            }
        })?;
        *self.permits() = permits;
        Ok(())
    }

    fn statement(&mut self, sql: &str, session: &Session) -> Result<(), crate::admission::StatementRefusal> {
        if std::mem::take(&mut self.first) { return Ok(()); }
        self.reacquire(session, !session.is_aborted() && !admission_exempt(sql))
    }
}
impl State {
    fn durability_registration(&self, context: &giggles_core::Cancellation, session: &Session) -> Option<giggles_core::cancellation::DurabilityRegistration> {
        if !matches!(session.role, Role::Database { .. })
            || session.agent
            || self.retain_durability_permits
            || self.copy.is_some()
            || self.portals.values().any(|p| p.spilled.is_some())
        {
            return None;
        }
        // Simple Query batches and COPY continuations suppress the output channel.
        let status = self.output.as_ref()?.status.clone();
        let admission = self.execution_admission.as_ref()?;
        let holder = admission.permits.clone();
        let service = session.service.clone();
        Some(context.register_durability_wait(move |_kind, release_admission| {
            if status.flushed.load(std::sync::atomic::Ordering::Acquire) {
                return None;
            }
            let permits = {
                let mut holder = holder.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                holder.blocking.as_ref()?;
                std::mem::take(&mut *holder)
            };
            // A woken worker can enter core as soon as these permits are dropped.
            release_admission();
            drop(permits);
            status.released_permits.store(true, std::sync::atomic::Ordering::Release);
            let wait = service.local_durability_release();
            Some(Box::new(wait))
        }))
    }
    fn durability_permits_released(&self) -> bool {
        self.output.as_ref().is_some_and(|output| {
            output.status.released_permits.load(std::sync::atomic::Ordering::Acquire)
        })
    }
    fn admission_exempt(&self, tag: u8, body: &[u8]) -> bool {
        let mut reader = Reader { b: body, at: 0 };
        let Ok(text) = reader.text() else { return false; };
        if tag == b'Q' {
            return simple::split(&text).is_ok_and(|statements| {
                statements.first().is_none_or(|sql| admission_exempt(sql))
            });
        }
        self.portals.get(&text).is_some_and(|portal| {
            matches!(&*portal.s.p, Plan::Control(command) if crate::admission::control_exempt(command))
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
    show: &crate::pg_settings::SettingName,
    settings: &crate::pg_settings::SessionSettings,
    session: &Session,
    diagnostic: &mut Diagnostic,
) -> R<(Description, Released<SqlResult>)> {
    use crate::pg_settings::{RegistryName, SettingName, Show};
    if !session.role.is_sql() {
        return Err("42501");
    }
    if session.is_aborted() {
        return Err("25P02");
    }
    let (names, rows) = if *show == SettingName::All {
        (
            vec!["name".into(), "setting".into(), "description".into()],
            crate::pg_settings::show_all(settings, settings_read_allowed(session)?),
        )
    } else {
        let privileged = if matches!(show, SettingName::Registry(RegistryName::Registered(entry)) if entry.restricted) {
            settings_read_allowed(session)?
        } else {
            false
        };
        let mut value = show.read(settings, privileged).map_err(|error| diagnostic.registry_error(error))?;
        if *show == SettingName::Native(Show::SessionAuthorization) && value.is_empty() {
            value = session.session_user_name().unwrap_or("giggles").into();
        }
        (vec![show.name(settings).into()], vec![vec![Value::Text(value)]])
    };
    Ok((
        Description {
            type_oids: vec![],
            typmods: vec![-1; names.len()],
            parameters: vec![],
            columns: names.iter().map(|name: &String| giggles_core::relational::ColumnDef {
                id: 0,
                name: name.clone(),
                data_type: DataType::Text,
                not_null: true,
            }).collect(),
        },
        Released::synthetic(SqlResult {
            columns: names,
            rows: rows.into_iter().map(|values| giggles_core::relational::SqlRow { values, version: 0 }).collect(),
            affected: 0,
        }),
    ))
}
fn execute_control(
    command: &'static str, session: &mut Session, implicit: &mut bool,
    settings: &mut crate::pg_settings::SessionSettings, diagnostic: &mut Diagnostic, out: &mut Vec<u8>,
) -> R<&'static str> {
    execute_control_inner(command, session, implicit, settings, diagnostic, out, false)
}
fn execute_implicit_commit(
    session: &mut Session, implicit: &mut bool,
    settings: &mut crate::pg_settings::SessionSettings, diagnostic: &mut Diagnostic, out: &mut Vec<u8>,
) -> R<&'static str> {
    execute_control_inner("COMMIT", session, implicit, settings, diagnostic, out, true)
}
fn execute_control_inner(
    command: &'static str,
    session: &mut Session,
    implicit: &mut bool,
    settings: &mut crate::pg_settings::SessionSettings,
    diagnostic: &mut Diagnostic,
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
    let explicit_commit = command == "COMMIT" && !implicit_commit;
    let publishing = was && actual == "COMMIT";
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
        diagnostic.pre_rollback_settings = Some(Box::new(before));
        *implicit = false;
        return Err(crate::service::sqlstate(&error));
    }
    if publishing {
        // Explicit COMMIT and simple implicit commits use the post-commit control path.
        simple::commit_reply(settings, session, explicit_commit, out)?;
    }
    *implicit = false;
    settings.transaction(was, session.in_transaction(), actual);
    if !session.in_transaction() { session.service.advisory.release(session.backend_pid, true); }
    if !publishing {
        settings_status(&before, settings, session, out)?;
        if explicit_commit {
            complete(actual, &empty(), out)?;
        }
    }
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
        session.set_text_settings(&self.settings.current);
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
        // Local data commits cannot invalidate the role profile validated at entry.
        if !self.durability_permits_released() {
            #[cfg(all(test, feature = "engine-rocksdb"))]
            let refreshed = implicit_block_tests::post_dispatch_role_refresh(session, tag)
                .and_then(|()| session.sync_role_settings(&mut self.settings.current));
            #[cfg(not(all(test, feature = "engine-rocksdb")))]
            let refreshed = session.sync_role_settings(&mut self.settings.current);
            if let Err(code) = refreshed
                && result.is_ok()
            {
                return Err(self.diagnostic.reply_error(code));
            }
        }
        session.set_search_path(&self.settings.current.search_path);
        session.set_text_settings(&self.settings.current);
        session.set_max_value_bytes(self.settings.current.max_value_bytes);
        session.set_max_statement_tokens(self.settings.current.max_statement_tokens);
        if result.is_ok() {
            simple::role_notices(
                session,
                &self.settings.current,
                out,
                ReplyLimit::after_commit(self.diagnostic.committed),
            )?;
            // Simple Query ends after its late role notices.
            if tag == b'Q' && !self.drain && self.copy.is_none() {
                ready(session, out)?;
            }
        }
        result
    }
    fn prepare_statement(
        &mut self,
        sql: &str,
        oids: Vec<u32>,
        unused_slots: &[usize],
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
        let id = self.allocate_statement_id();
        // GD-0061 R3-707: an approver form, before any other grammar reads it.
        if let Some(change) = crate::agent::preview::approver_statement(sql)? {
            // The forms take no parameters.
            if !oids.is_empty() {
                return Err("08P01");
            }
            let d = crate::agent::preview::approver_description(&change);
            return Ok(Statement {
                id,
                sequence_dependencies: Default::default(),
                parse_context: None,
                description_version: None,
                validated: None,
                #[cfg(test)]
                constant_validations: 0,
                sql_bytes: sql.len(),
                origin: None, discarded: false,
                sql: sql.to_owned(),
                declared_oids: oids,
                p: Arc::new(Plan::Change(Box::new(change))),
                d: Arc::new(d),
                parameter_oids: vec![],
                enum_parameters: Default::default(),
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
                id,
                sequence_dependencies: Default::default(),
                parse_context: None,
                description_version: None,
                validated: None,
                #[cfg(test)]
                constant_validations: 0,
                sql_bytes: sql.len(),
                origin: None, discarded: false,
                sql: sql.to_owned(),
                declared_oids: oids,
                p: Arc::new(Plan::TaskDdl(Box::new(stmt))),
                d: Arc::new(d),
                parameter_oids: vec![],
                enum_parameters: Default::default(),
                query_id: self.diagnostic.query_id.clone(),
            });
        }
        if !is_copy_sql(sql) {
            core(session.authorize_sequence_sql(sql))?;
        }
        let mut plain_application = false;
        let mut enum_parameters = giggles_core::relational::EnumParameters::new();
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
        } else if let Some(command) = prepare::parse_protocol(sql, &mut self.diagnostic)? {
            prepare::guard(session)?;
            self.prepare_command(command, &oids)?
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
            let (description, _) = inspect(&show, &self.settings, session, &mut self.diagnostic)?;
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
                type_oids: fields.iter().map(|field| field.data_type.oid()).collect(),
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
            let (plan, description, enums) =
                prepare_declared_sql_diagnostic(&mut self.diagnostic, session, sql, &oids, unnamed)?;
            enum_parameters = enums;
            (plan, description)
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
                        *o != 0
                            && o != inferred
                            && unused_slots.binary_search(&i).is_err()
                            && !catalog_input_override(plan, i, *o, *inferred)
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
                    Ok((application, description, enums))
                        if oids.len() <= description.parameters.len()
                            && oids
                                .iter()
                                .zip(&description.parameters)
                                .all(|(o, ty)| sql_declared_type(ty, *o).is_some()) =>
                    {
                        p = application;
                        d = description;
                        enum_parameters = enums;
                        parameter_oids = d.parameters.iter().map(oid).collect();
                    }
                    Err(error) if !was_aborted && session.is_aborted() => {
                        return Err(error);
                    }
                    _ => {}
                }
            }
        }
        for (&i, parameter) in &enum_parameters {
            parameter_oids[i] = parameter.oid();
        }
        self.diagnostic.phase = "parameter type declaration";
        for (i, (o, t)) in oids.iter().copied().zip(&mut d.parameters).enumerate() {
            if matches!(p, Plan::Catalog(..)) {
                if unused_slots.binary_search(&i).is_ok() {
                    // The declaration governs input validation for this slot.
                    // The catalog plan has no reference to its value.
                    *t = core_declared_type(o).unwrap_or(DataType::Text);
                    parameter_oids[i] = o;
                    continue;
                }
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
            if o != 0 && o != parameter_oids[i] {
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
        let sequence_dependencies = if parse_context.is_some() {
            Prepared::sequence_calls_from_sql(sql).into()
        } else {
            Default::default()
        };
        Ok(Statement {
            id,
            sequence_dependencies,
            parse_context,
            description_version,
            validated: None,
            #[cfg(test)]
            constant_validations: 0,
            sql_bytes: sql.len(),
            origin: None, discarded: false,
            sql: sql.to_owned(),
            declared_oids: oids,
            p: Arc::new(p),
            d: Arc::new(d),
            parameter_oids,
            enum_parameters,
            query_id: self.diagnostic.query_id.clone(),
        })
    }
    fn allocate_statement_id(&mut self) -> u64 {
        let id = self.next_statement_id;
        self.next_statement_id = id.checked_add(1).expect("statement ID exhausted");
        id
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
        let Plan::Sql(p) = &*old.p else { return Ok(None); };
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
        if session.authorize_sequence_dependencies(&old.sequence_dependencies).is_err() { return Ok(None); }
        // SELECT authorization proves visibility of every application dependency;
        // catalog_plan returned None for this unchanged SQL, so skipping it cannot
        // expose catalog relations or bypass their visibility checks.
        // Re-run the application path's live authority checks. Any error or drift
        // takes the full path, which owns error ordering and diagnostic fields.
        if session.reauthorize_unnamed_parse(sql, &old.sequence_dependencies, p).is_err() {
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
        let stale = old.discarded || match old.p.as_ref() {
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
            Plan::Catalog(plan, _, epoch)
                if !plan.depends_on_relations() || plan.reads_prepared_statements() =>
            {
                let (current, _, _) = core(session.catalog_snapshot())?;
                current != *epoch
            }
            _ => false,
        };
        if !stale {
            return Ok(());
        }
        let old = old.clone();
        let mut preparation = self.settings.current.clone();
        if let Plan::Sql(prepared) = &*old.p {
            preparation.datestyle.order = prepared.literal_date_order();
        }
        session.set_text_settings(&preparation);
        let unused_slots: &[usize] = old.origin.as_ref().map_or(&[], |origin| &origin.unused_slots);
        let new = self.prepare_statement(
            &old.sql, old.declared_oids.clone(), unused_slots, session, false,
        );
        session.set_text_settings(&self.settings.current);
        let mut new = new?;
        if let Some(origin) = &old.origin {
            new.append_unused_parameters(&origin.unused_types);
        }
        new.origin = old.origin.clone();
        new.sql_bytes = old.sql_bytes;
        // Rebinding can change storage identities without changing the
        // result shape promised to the client.
        if old.parameter_oids != new.parameter_oids || !old.same_result(&new) {
            self.diagnostic.message = Some("cached plan must not change result type");
            return Err("0A000");
        }
        old.forget_cursor_plan(session);
        self.statements.insert(name.to_owned(), new);
        Ok(())
    }
    fn bind_statement(
        &mut self, name: &str, session: &mut Session, implicit: &mut bool,
    ) -> R<Statement> {
        let statement = self.statements.get(name).ok_or("26000")?;
        if let Plan::Sql(prepared) = statement.p.as_ref()
            && session.task().is_none()
            && needs_implicit_transaction(prepared)
        {
            if !prepared.is_ddl() {
                begin_implicit(session, implicit, &mut self.settings)?;
                session.sql.begin_prepared_statement();
            }
            core(session.bind_prepared_relations(prepared))?;
        }
        // One version comparison, after locks have stabilized dependencies.
        self.refresh_statement(name, session)?;
        let statement = self.statements.get(name).ok_or("26000")?.clone();
        self.diagnostic.query_id = statement.query_id.clone();
        statement.describe(session)?;
        Ok(statement)
    }
    fn validate_statement_binding(
        &mut self, name: &str, statement: &mut Statement, session: &mut Session,
    ) -> R<()> {
        if matches!(statement.p.as_ref(), Plan::Sql(_)) {
            let validation = statement.validate_constants(session, &mut self.diagnostic);
            if name.is_empty() {
                if validation.is_err() {
                    statement.validated = None;
                }
                // A Bind refresh can replace the live statement while the
                // Parse cache still holds its earlier preparation.
                if let Some(entry) = self.unnamed_cache.iter_mut().find(|entry| {
                    entry.id == statement.id
                        && entry.sql == statement.sql
                        && entry.declared_oids == statement.declared_oids
                }) {
                    entry.validated = statement.validated;
                    #[cfg(test)]
                    { entry.constant_validations = statement.constant_validations; }
                }
            }
            if validation.is_ok() || name.is_empty() {
                let stored = self.statements.get_mut(name).ok_or("26000")?;
                stored.validated = statement.validated;
                #[cfg(test)]
                { stored.constant_validations = statement.constant_validations; }
            }
            validation?;
        }
        Ok(())
    }
    #[allow(clippy::too_many_arguments)]
    fn execute_session_portal(
        &mut self, p: &mut Portal, session: &mut Session, implicit: &mut bool,
        max: u32, mut retained: usize, out: &mut Vec<u8>,
    ) -> R<()> {
        self.diagnostic.query_id = p.s.query_id.clone();
        p.s.describe(session)?;
        if let Plan::PreparedCommand(invocation) = p.s.p.as_ref() {
            if p.result.is_some() { return Err("55000"); }
            let invocation = invocation.clone();
            if let prepare::Command::Execute { name, args } = &invocation.command {
                let mut target = self.bind_sql_execute(name, args, session, implicit)?;
                if let Some(bound) = &invocation.target
                    && let Some(detail) = bound.execute_result_mismatch(&target.s)
                {
                    self.diagnostic.message = Some("query result type does not match portal result type");
                    self.diagnostic.wire_detail = Some(detail);
                    return Err("42804");
                }
                target.bound_execute_target = invocation.target;
                if p.formats.len() > 1 && p.formats.len() != target.s.d.columns.len() {
                    return Err("08P01");
                }
                retained = (retained - p.parameter_bytes)
                    .checked_add(target.parameter_bytes).ok_or("54000")?;
                if retained > MAX_RESPONSE { return Err("54000"); }
                let formats = std::mem::take(&mut p.formats);
                *p = target;
                p.formats = formats;
            } else {
                self.execute_prepared_utility(invocation.command, session, *implicit, out)?;
                p.result = ran();
                return Ok(());
            }
        }
        let was_transaction = session.in_transaction();
        if let Plan::Catalog(plan, fields, epoch) = p.s.p.as_ref() {
            self.diagnostic.phase = "catalog execution";
            begin_implicit(session, implicit, &mut self.settings)?;
            let (current, tables, indexes, typmods, types) =
                core(session.catalog_snapshot_with_typmods())?;
            if current != *epoch && !plan.refreshes_view_definitions() {
                return Err("0A000");
            }
            if p.catalog_result.is_none() {
                self.settings.queried = true;
                let snapshot = catalog_wire::snapshot(session, current, tables, indexes, typmods, types)?;
                let snapshot = catalog_wire::with_search_path(snapshot, &self.settings.current.search_path);
                let snapshot = self.prepared_snapshot(plan, snapshot);
                let snapshot = catalog_wire::with_application_rows(session, plan, snapshot)?;
                let result = catalog_wire::execute(plan, &snapshot, &p.catalog_params)
                    .map_err(|error| self.diagnostic.catalog_error(error))?;
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
                &self.settings.current,
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
            return Ok(());
        }
        let Plan::Sql(prepared) = p.s.p.as_ref() else {
            unreachable!()
        };
        if prepared.command() == "SELECT" && prepared.write_tables().is_empty() {
            if p.cursor.is_none() {
                read_only_check(prepared.row_lock_command().filter(|_| !prepared.row_lock_tables().is_empty()).unwrap_or("SELECT"), &self.settings, session, &mut self.diagnostic)?;
                begin_implicit(session, implicit, &mut self.settings)?;
                self.settings.queried = true;
                let cursor = if p.has_live_statement(&self.statements) {
                    session.open_cursor(prepared, &p.params)
                } else {
                    session.open_uncached_cursor(prepared, &p.params)
                };
                p.cursor = Some(self.diagnostic.core(cursor)?);
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
            return Ok(());
        }
        if p.result.is_some() && p.s.d.columns.is_empty() {
            return Err("55000");
        }
        if p.result.is_none() {
            let command = prepared.command();
            read_only_check(prepared.row_lock_command().filter(|_| !prepared.row_lock_tables().is_empty()).unwrap_or_else(|| prepared.write_tables().first().map_or(command, |(_, command)| *command)), &self.settings, session, &mut self.diagnostic)?;
            if needs_implicit_transaction(prepared) {
                begin_implicit(session, implicit, &mut self.settings)?;
            }
            if needs_implicit_transaction(prepared) {
                self.settings.queried = true;
            }
            let (result, spilled) = if command == "BEGIN" && *implicit {
                *implicit = false;
                (Released::synthetic(SqlResult::default()), None)
            } else {
                self.diagnostic.core(session.sync_replication_role(&self.settings))?;
                // GD-0061 R3-310: the whole result is released once,
                // here. A task session's spill never reaches this
                // handle: core materialises it behind the release tag.
                let (result, spilled) = giggles_core::relational::spill::streaming(
                    || session.execute_prepared_unreleased(prepared, &p.params));
                self.retain_durability_permits |= spilled.is_some();
                let result = self.diagnostic.core(result)?;
                (self.diagnostic.core(session.release_page(result))?, spilled)
            };
            if !session.in_transaction() {
                *implicit = false;
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
            return Ok(());
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
            let result = self.copy_message(tag, b, session, out, received, &output);
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
                let publishing = self.implicit && !session.is_aborted() && session.in_transaction();
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
                            self.diagnostic.pre_rollback_settings = Some(Box::new(before));
                            self.portals.clear();
                            if matches!(&error, CoreError::WriteFenced { .. }) {
                                self.diagnostic.wire_message = Some(error.to_string());
                            }
                            return Err(crate::service::sqlstate(&error));
                        }
                        if publishing {
                            self.diagnostic.committed = true;
                            // Execute checked its rows. Sync adds only post-commit control.
                            simple::commit_reply(&self.settings, session, false, out)?;
                        }
                        self.settings.transaction(true, false, "COMMIT");
                    }
                }
                if !session.in_transaction() {
                    self.portals.clear();
                }
                if !publishing {
                    simple::settings_status_with_limit(
                        &before, &self.settings, session, out, ReplyLimit::Control,
                    )?;
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
                if let Some(statement) = self.statements.remove("") {
                    statement.forget_cursor_plan(session);
                }
                self.portals.remove("");
                self.invalidate_unnamed_for_sql(&sql);
                self.simple_query(&sql, session, out, received)?;
                if !session.in_transaction() {
                    self.portals.clear();
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
                if !name.is_empty() {
                    self.check_named_statement(&name, sql.len())?;
                }
                let reused = if name.is_empty() { self.reuse_unnamed(&sql, &oids, session)? } else { None };
                let mut statement = match reused {
                    Some(statement) => statement,
                    None => self.prepare_statement(&sql, oids, &[], session, name.is_empty())?,
                };
                if name.is_empty() {
                    self.cache_unnamed(&statement);
                }
                if name.is_empty() {
                    match self.statements.entry(name) {
                        Entry::Occupied(mut entry) => {
                            let previous = entry.get();
                            if !Arc::ptr_eq(&previous.p, &statement.p) {
                                previous.forget_cursor_plan(session);
                            }
                            entry.insert(statement);
                        }
                        Entry::Vacant(entry) => {
                            entry.insert(statement);
                        }
                    }
                } else {
                    statement.origin = Some(Box::new(prepare::Origin::protocol(session)));
                    self.store_named_statement(name, statement);
                }
                frame(b'1', &[], out)?;
            }
            b'B' => {
                let name = r.text()?;
                let stmt = r.text()?;
                let mut implicit = self.implicit;
                let statement = self.bind_statement(&stmt, session, &mut implicit);
                self.implicit = implicit;
                let mut s = statement?;
                if let Plan::PreparedCommand(invocation) = s.p.as_ref() {
                    let mut invocation = invocation.clone();
                    self.bind_execute_description(&mut invocation, Arc::make_mut(&mut s.d), session)?;
                    s.p = Arc::new(Plan::PreparedCommand(invocation));
                }
                let fmts = formats(&mut r, s.d.parameters.len())?;
                let n = r.u16()?;
                if n != s.d.parameters.len() {
                    return Err("08P01");
                }
                let mut params = vec![];
                let mut catalog_params = vec![];
                let mut parameter_bytes = 0usize;
                let mut transaction_us = None;
                let catalog = matches!(s.p.as_ref(), Plan::Catalog(..));
                for (i, ty) in s.d.parameters.iter().enumerate() {
                    let n = r.u32()?;
                    let unused = s.has_unused_slot(i);
                    let wire_oid = s.parameter_oids[i];
                    if (catalog && !unused)
                        || (unused && (catalog || core_declared_type(wire_oid).is_none())
                            && catalog_wire::parameter_type(wire_oid).is_ok())
                    {
                        let value = if n == u32::MAX {
                            giggles_pg_catalog::Value::Null
                        } else {
                            let bytes = r.take(usize::try_from(n).map_err(|_| "54000")?)?;
                            let format = format_at(&fmts, i);
                            catalog_wire::decode(bytes, catalog_wire::parameter_type(wire_oid)?, format)
                                .inspect_err(|&code| {
                                    if wire_oid == 26 && format == 0
                                        && let Ok(text) = std::str::from_utf8(bytes)
                                    {
                                        self.diagnostic.oid_text_error(code, text);
                                    }
                                })?
                        };
                        // Count decoded allocation, including array element overhead.
                        parameter_bytes = parameter_bytes
                            .checked_add(catalog_wire::value_bytes(&value)?)
                            .ok_or("54000")?;
                        if catalog {
                            catalog_params.push(if unused {
                                giggles_pg_catalog::Value::Null
                            } else {
                                value
                            });
                        }
                        continue;
                    }
                    let input = s.enum_parameters.get(&i);
                    let value = if n == u32::MAX {
                        Value::Null
                    } else {
                        let raw = r.take(usize::try_from(n).map_err(|_| "54000")?)?;
                        let format = format_at(&fmts, i);
                        if input.is_some() && *ty == DataType::TextArray && format == 1 {
                            logical_binary_input(ty, raw, Some(&mut self.diagnostic))?
                        } else {
                            decode_with_order(
                                raw, ty, format,
                                session.zone().unwrap_or(SessionZone::Fixed(0)),
                                if matches!(ty, DataType::TimeTz) && format == 0 {
                                    *transaction_us.get_or_insert_with(|| session.copy_clock().transaction_us)
                                } else {
                                    0
                                },
                                self.settings.current.datestyle.order,
                            )?
                        }
                    };
                    if let Some(parameter) = input {
                        self.diagnostic.core(parameter.validate(&value).map_err(CoreError::Sql))?;
                    }
                    if catalog {
                        // An unused declaration can use a type only core decodes.
                        catalog_params.push(giggles_pg_catalog::Value::Null);
                    } else {
                        params.push(value);
                    }
                }
                #[cfg(feature = "legacy-task")]
                if let Plan::Task(task) = &*s.p {
                    task.bind(&params)?;
                }
                let retained_slots = s.plan_parameter_count();
                params.truncate(retained_slots);
                catalog_params.truncate(retained_slots);
                let output = if let Plan::PreparedCommand(invocation) = s.p.as_ref()
                    && matches!(&invocation.command, prepare::Command::Execute { .. })
                {
                    let count = r.u16()?;
                    if invocation.target.is_some() && count > 1 && count != s.d.columns.len() {
                        self.diagnostic.wire_message = Some(format!(
                            "bind message has {count} result formats but query has {} columns",
                            s.d.columns.len(),
                        ));
                        return Err("08P01");
                    }
                    read_formats(&mut r, count)?
                } else {
                    formats(&mut r, s.d.columns.len())?
                };
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
                self.validate_statement_binding(&stmt, &mut s, session)?;
                self.note_plan_use(&stmt);
                let mut portal = Portal::new(s, params, catalog_params, output, parameter_bytes);
                portal.statement_name = Some(stmt);
                self.portals.insert(name, portal);
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
                        if let Plan::PreparedCommand(invocation) = s.p.as_ref() {
                            self.authorize_live_execute_target(invocation, session)?;
                        }
                        self.diagnostic.query_id = s.query_id.clone();
                        let d = s.describe(session)?;
                        let mut b = vec![];
                        b.extend((d.parameters.len() as u16).to_be_bytes());
                        for oid in &s.parameter_oids {
                            b.extend(oid.to_be_bytes());
                        }
                        frame(b't', &b, out)?;
                        if let Some(fields) = s.catalog_fields() {
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
                        let described = if let Some(bound) = &p.bound_execute_target {
                            bound.authorize_execute_description(session)?;
                            bound.as_ref()
                        } else {
                            &p.s
                        };
                        let d = described.describe(session)?;
                        if let Some(fields) = described.catalog_fields() {
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
                if self.portals.get(&name).is_some_and(|p| {
                    matches!(p.s.p.as_ref(), Plan::PreparedCommand(_) | Plan::Sql(_) | Plan::Catalog(..))
                }) {
                    let mut portal = self.portals.remove(&name).expect("checked portal");
                    let mut implicit = self.implicit;
                    let was_transaction = session.in_transaction();
                    let result = self.execute_session_portal(
                        &mut portal, session, &mut implicit, max, retained, out,
                    );
                    self.implicit = implicit;
                    if was_transaction && !session.in_transaction() {
                        self.portals.clear();
                    } else {
                        self.portals.insert(name, portal);
                    }
                    return result.map(|()| true);
                }
                let p = self.portals.get_mut(&name).ok_or("34000")?;
                self.diagnostic.query_id = p.s.query_id.clone();
                p.s.describe(session)?;
                if let Plan::Cursor(command) = &*p.s.p {
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
                        cursor::Execution::Extended { live_statement: p.has_live_statement(&self.statements) },
                        &self.output,
                        out,
                    )?;
                    p.result = ran();
                    return Ok(true);
                }
                if let Plan::CopyOut(options) = &*p.s.p {
                    if p.result.is_some() { return Err("55000"); }
                    let options = options.clone();
                    p.result = ran();
                    let mut implicit = self.implicit;
                    let result = self.copy_stdout(&options, session, out, &mut implicit);
                    self.implicit = implicit;
                    result?;
                    return Ok(true);
                }
                if let Plan::Copy(copy, prepared) = &*p.s.p {
                    if p.result.is_some() {
                        return Err("55000");
                    }
                    read_only_check("INSERT", &self.settings, session, &mut self.diagnostic)?;
                    begin_implicit(session, &mut self.implicit, &mut self.settings)?;
                    let copy = copy.clone();
                    let prepared = prepared.clone();
                    p.result = ran();
                    self.start_copy(copy, prepared, None, session, out)?;
                    return Ok(true);
                }
                if let Plan::Change(change) = &*p.s.p {
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
                        let Plan::Change(change) = &*p.s.p else { unreachable!() };
                        complete(crate::agent::preview::approver_command(change), result, out)?;
                    }
                    return Ok(true);
                }
                if let Plan::TaskDdl(stmt) = &*p.s.p {
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
                if let Plan::Admin(stmt) = &*p.s.p {
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
                    simple::role_notices(session, &self.settings.current, out, ReplyLimit::Checked)?;
                    self.admin_completed = true;
                    p.result = ran();
                    complete(tag, &empty(), out)?;
                    return Ok(true);
                }
                if let Plan::Explain(query) = p.s.p.as_ref() {
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
                if matches!(&*p.s.p, Plan::Empty) {
                    frame(b'I', &[], out)?;
                    return Ok(true);
                }
                if matches!(&*p.s.p, Plan::SettingsFunction(_)) {
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
                if let Plan::AccessShare(tables) = &*p.s.p {
                    if !session.in_transaction() || self.implicit {
                        return Err("25P01");
                    }
                    simple::validate_lock(session, &tables.tables, tables.mode, tables.nowait)?;
                    complete("LOCK TABLE", &empty(), out)?;
                    return Ok(true);
                }
                if let Plan::Control(command) = &*p.s.p {
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
                    if !session.in_transaction() {
                        self.portals.clear();
                    }
                    return Ok(true);
                }
                if let Plan::Settings(settings) = &*p.s.p {
                    if !session.role.is_sql() {
                        return Err("42501");
                    }
                    if session.is_aborted() {
                        return Err("25P02");
                    }
                    if p.result.is_some() {
                        return Err("55000");
                    }
                    if !settings.iter().all(crate::pg_settings::Setting::is_local) {
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
                if let Plan::Inspect(show) = p.s.p.as_ref() {
                    // Recheck role/transaction even when releasing cached rows.
                    let (_, current) = inspect(show, &self.settings, session, &mut self.diagnostic)?;
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
                if let Plan::Task(task) = &*p.s.p {
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
                unreachable!("all portal plan variants have a dispatch path");
            }
            b'C' => {
                let kind = r.byte()?;
                let name = r.text()?;
                r.end()?;
                match kind {
                    b'S' => {
                        self.remove_statement(&name, session);
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
    // This terminal frame can follow a buffered 08007 reply.
    ReplyLimit::Control.frame(b'E', &body, out).map_err(io::Error::other)
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
        // CopyDone and CopyFail consume the COPY state before role refresh.
        let simple = tag == b'Q'
            || (matches!(tag, b'c' | b'f') && self.copy.as_ref().is_some_and(|c| c.simple));
        let context = active.context.clone();
        self.diagnostic = Diagnostic {
            unknown_outcome: false,
            committed: false,
            pre_rollback_settings: None,
            wire_detail: None,
            wire_context: None,
            wire_hint: None,
            wire_registry_error: None,
            sql_detail: None,
            duplicate_key: None,
            duplicate_key_visible: None,
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
        // Sync ends the batch. Execute may have buffered work before Sync.
        let _durability = if refusal.is_none() && tag == b'S' && self.implicit && !session.is_aborted() {
            self.durability_registration(&context, session)
        } else {
            None
        };
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
                self.diagnostic.authorize_duplicate_key(code, session);
                session.abort();
                // A CopyDone rejected during post-dispatch refresh must not
                // leave a successful COPY completion queued for Flush or Sync.
                self.copy_completion.clear();
                // COMMIT may fail and finish rollback before returning
                // its error. No portal can outlive that transaction.
                if simple && !session.in_transaction() {
                    self.portals.clear();
                }
                if tag == b'S' && self.implicit {
                    session.sql.rollback();
                    self.settings.transaction(true, false, "ROLLBACK");
                    session.set_time_zone(&self.settings.current.timezone);
                    session.sync_role_settings(&mut self.settings.current).map_err(std::io::Error::other)?;
        session.set_search_path(&self.settings.current.search_path);
                    session.set_text_settings(&self.settings.current);
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
                    simple::role_notices(session, &self.settings.current, out, ReplyLimit::Control)
                        .map_err(io::Error::other)?;
                    if let Some(before) = self.diagnostic.pre_rollback_settings.take() {
                        simple::settings_status_with_limit(
                            &before, &self.settings, session, out, ReplyLimit::Control,
                        )
                        .map_err(io::Error::other)?;
                    }
                    out.extend(error);
                    if simple || tag == b'S' {
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
            if self.copy.is_none() {
                self.retain_durability_permits = false;
            }
            session.service.advisory.release(session.backend_pid, true);
        }
        if self.copy_receiving() {
            self.copy_active = Some(active);
            self.copy_permit = self
                .execution_admission
                .as_mut()
                .and_then(|a| a.permits().role_permit.take());
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
    session.set_postgres_surface();
    let blocking = session.service.blocking.clone();
    let mut startup_settings = startup.settings;
    let timezone = startup.timezone;
    let mut out = vec![];
    let backend = session
        .service
        .cancellations
        .register(&session.fingerprint, session.epoch)?;
    let (disconnect, disconnected) = tokio::sync::watch::channel(());
    struct Close {
        backend: std::sync::Arc<crate::cancel::Backend>,
        closed: std::sync::Arc<std::sync::atomic::AtomicBool>,
        _disconnect: tokio::sync::watch::Sender<()>,
    }
    impl Drop for Close {
        fn drop(&mut self) {
            self.closed.store(true, std::sync::atomic::Ordering::Release);
            self.backend.close();
        }
    }
    let _close = Close {
        backend: backend.clone(),
        closed: session.connection_closed.clone(),
        _disconnect: disconnect,
    };
    session.backend_pid = backend.pid as i32;
    if !authenticated_already {
        frame(b'R', &0u32.to_be_bytes(), &mut out).map_err(io::Error::other)?;
    }
    let key = [backend.pid.to_be_bytes(), backend.secret.to_be_bytes()].concat();
    frame(b'K', &key, &mut out).map_err(io::Error::other)?;
    let datestyle = startup_settings.datestyle.to_string();
    for (key, value) in [
        ("server_version", "18.6"),
        ("client_encoding", "UTF8"),
        ("server_encoding", "UTF8"),
        ("standard_conforming_strings", "on"),
        ("integer_datetimes", "on"),
        ("DateStyle", &datestyle),
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
        retain_durability_permits: false,
        settings: crate::pg_settings::SessionSettings::default(),
        statements: BTreeMap::new(),
        unnamed_cache: Vec::new(),
        next_statement_id: 0,
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
    session.set_text_settings(&state.settings.current);
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
                        session.set_text_settings(&state.settings.current);
                        session.set_max_value_bytes(state.settings.current.max_value_bytes);
                        session.set_max_statement_tokens(state.settings.current.max_statement_tokens);
                        state.implicit = false;
                        state.retain_durability_permits = false;
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
            status: Default::default(),
            sender: output,
            prefix: std::sync::Mutex::new(Vec::new()),
        });
        state.execution_admission = Some(ExecutionAdmission {
            permits: std::sync::Arc::new(std::sync::Mutex::new(ExecutionPermits {
                role_permit,
                blocking: Some(permit),
            })),
            runtime: tokio::runtime::Handle::current(),
            disconnected: disconnected.clone(),
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
                let mut released_permits = false;
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
                    let refusal = if needs_role && admission.permits().role_permit.is_none() {
                        // Already admitted frames, including a buffered Sync,
                        // still run after EOF. Only new admission needs a peer.
                        if session.connection_closed.load(std::sync::atomic::Ordering::Acquire) {
                            break;
                        }
                        admission.reacquire(session, true).err()
                    } else {
                        None
                    };
                    if let Some(output) = &state.output {
                        output.status.flushed.store(false, std::sync::atomic::Ordering::Release);
                    }
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
                    released_permits = state.durability_permits_released();
                    if released_permits {
                        // Query and Sync are batch boundaries. Let the socket
                        // flush their reply before revalidating the binding.
                        debug_assert!(batch.is_empty());
                        break;
                    }
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
                    released_permits,
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
                    Ok(b'X') | Err(_) => return Ok(()),
                    Ok(tag) => pending_tag = Some(tag),
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
            released_permits,
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
            // The dispatch may already have produced a terminal outcome
            // for a write that reached publication before the binding failed.
            if state.diagnostic.unknown_outcome {
                send(s, &response).await?;
                return Ok(());
            }
            if state.admin_completed {
                send(s, &response).await?;
                if tag == b'E' {
                    admin_sync = true;
                    continue;
                }
            }
            return Ok(());
        }
        if released_permits {
            send(s, &response).await?;
            binding_check_pending = true;
            continue;
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
fn logical_binary_output(ty: &DataType, s: &str, type_oid: u32) -> R<Vec<u8>> {
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
        DataType::UuidArray => 2950,
        DataType::TextArray if type_oid >= 65536 => type_oid - 1,
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
                DataType::UuidArray => logical_binary_output(&DataType::Uuid, &s, elem)?,
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
fn enum_array_label<'a>(bytes: &'a [u8], diagnostic: &mut Diagnostic) -> R<&'a str> {
    let text = std::str::from_utf8(bytes);
    let valid_len = text.as_ref().map_or_else(|error| error.valid_up_to(), |text| text.len());
    let invalid = if let Some(nul) = bytes[..valid_len].iter().position(|&byte| byte == 0) {
        &bytes[nul..=nul]
    } else if let Ok(text) = text {
        return Ok(text);
    } else {
        let width = match bytes[valid_len] {
            0xc0..=0xdf => 2,
            0xe0..=0xef => 3,
            0xf0..=0xf7 => 4,
            _ => 1,
        };
        &bytes[valid_len..bytes.len().min(valid_len + width)]
    };
    let hex = invalid.iter().map(|byte| format!("0x{byte:02x}")).collect::<Vec<_>>().join(" ");
    diagnostic.wire_message = Some(format!("invalid byte sequence for encoding \"UTF8\": {hex}"));
    Err("22021")
}

fn logical_binary_input(
    ty: &DataType,
    b: &[u8],
    mut enum_array_diagnostic: Option<&mut Diagnostic>,
) -> R<Value> {
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
    let elem = int(b, &mut at)? as u32;
    let expected = match ty {
        DataType::IntArray => 23,
        DataType::BigIntArray => 20,
        DataType::UuidArray => 2950,
        _ => 25,
    };
    // The parameter's enum type determines how to decode the labels, independent of the header OID.
    let element_matches = elem == expected || enum_array_diagnostic.is_some();
    if !(0..=1).contains(&ndim) || !(0..=1).contains(&hasnull) || !element_matches {
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
            DataType::UuidArray if len == 16 => hex::encode(v),
            DataType::TextArray => {
                let text = if let Some(diagnostic) = enum_array_diagnostic.as_deref_mut() {
                    enum_array_label(v, diagnostic)?
                } else {
                    std::str::from_utf8(v).map_err(|_| "22021")?
                };
                array_quote(text)
            }
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
            (DataType::UuidArray, "{NULL,12345678-1234-5678-1234-567812345678}"),
        ] {
            let binary = logical_binary_output(&ty, text, 0).unwrap();
            assert_eq!(
                logical_binary_input(&ty, &binary, None),
                Ok(Value::Text(text.into()))
            );
            if ty == DataType::Uuid {
                assert_eq!(binary.len(), 16);
            }
            if ty == DataType::Jsonb {
                assert_eq!(binary[0], 1);
            }
        }
        let binary = logical_binary_output(&DataType::IntArray, "{1,NULL,-3}", 0).unwrap();
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
        assert_eq!(diagnostic.wire_hint.as_deref(), Some("Available values: origin, replica, local."));
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

pub(crate) use task_path::api_settings_refusal;

#[cfg(all(test, feature = "engine-rocksdb"))]
mod prepared_validation_tests {
    use super::*;

    fn fixture() -> Session {
        static NEXT_FIXTURE: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let path = crate::test_support::fixture_root().join(format!(
            "validation-memo-{}-{}-{}", std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos(),
            NEXT_FIXTURE.fetch_add(1, std::sync::atomic::Ordering::Relaxed)));
        let admin = giggles_core::Admin::create_engine(path, giggles_core::Limits::default(), giggles_core::Engine::V2).unwrap();
        admin.roles().create(giggles_core::roles::NewRole {
            name: "owner".into(), login: true, superuser: true, ..Default::default()
        }).unwrap();
        crate::Service::new(admin, vec![], crate::clock::Clock::new(0, 3600).unwrap()).unwrap()
            .role_session("owner").unwrap()
    }
    fn statement(session: &mut Session, sql: &str) -> Statement {
        let p = session.sql.prepare(sql).unwrap();
        let d = session.sql.describe(&p).unwrap();
        Statement { id: 0, sequence_dependencies: Default::default(), parse_context: None,
            description_version: None, validated: None, constant_validations: 0,
            sql_bytes: sql.len(), origin: None, discarded: false, sql: sql.into(), declared_oids: vec![],
            p: Arc::new(Plan::Sql(Box::new(p))), d: Arc::new(d), parameter_oids: vec![], enum_parameters: Default::default(), query_id: None }
    }
    fn wire_state() -> State {
        State {
            output: None, diagnostic: Diagnostic::default(), admin_completed: false,
            implicit: false, retain_durability_permits: false, settings: crate::pg_settings::SessionSettings::default(),
            statements: BTreeMap::new(), unnamed_cache: Vec::new(), named_statement_bytes: 0,
            next_statement_id: 0,
            portals: BTreeMap::new(), cursors: cursor::Cursors::default(), drain: false,
            copy: None, copy_completion: vec![], copy_active: None, copy_permit: None,
            execution_admission: None,
        }
    }
    fn message(state: &mut State, session: &mut Session, tag: u8, body: &[u8]) -> R<Vec<u8>> {
        let mut out = Vec::new();
        state.handle(tag, body, session, &mut out, std::time::Instant::now())?;
        Ok(out)
    }
    fn parse_unnamed(state: &mut State, session: &mut Session, sql: &str, declared_oids: &[u32]) {
        let mut parse = vec![0];
        z(sql, &mut parse);
        parse.extend((declared_oids.len() as u16).to_be_bytes());
        for oid in declared_oids {
            parse.extend(oid.to_be_bytes());
        }
        assert_eq!(message(state, session, b'P', &parse).unwrap()[0], b'1');
    }
    fn bind_unnamed(state: &mut State, session: &mut Session, params: &[&str]) -> R<()> {
        let mut bind = vec![0; 4]; // Unnamed portal and statement, default text format.
        bind.extend((params.len() as u16).to_be_bytes());
        for param in params {
            bind.extend((param.len() as u32).to_be_bytes());
            bind.extend(param.as_bytes());
        }
        bind.extend([0; 2]); // Default output format.
        let result = message(state, session, b'B', &bind);
        message(state, session, b'S', &[]).unwrap();
        assert_eq!(result?[0], b'2');
        Ok(())
    }
    fn run_cursor_cycle(
        state: &mut State,
        session: &mut Session,
        bind: &[u8],
        describe: &[u8],
        execute: &[u8],
    ) {
        assert_eq!(message(state, session, b'B', bind).unwrap()[0], b'2');
        assert_eq!(message(state, session, b'D', describe).unwrap()[0], b'T');
        assert_eq!(message(state, session, b'E', execute).unwrap()[0], b'D');
        assert_eq!(message(state, session, b'S', &[]).unwrap()[0], b'Z');
    }

    #[test]
    fn repeated_unnamed_parse_reuses_cursor_binding_until_replaced() {
        let mut session = fixture();
        let mut state = wire_state();
        session.execute_sql("CREATE TABLE t(id int PRIMARY KEY)", &[]).unwrap();
        session.execute_sql("INSERT INTO t VALUES (7), (8)", &[]).unwrap();
        let sql = "SELECT id FROM public.t WHERE id = 7";
        let version = session.sql.committed_catalog_version().unwrap();
        let before = giggles_core::SqlSession::test_cursor_query_binds();

        for _ in 0..2 {
            parse_unnamed(&mut state, &mut session, sql, &[]);
            run_cursor_cycle(&mut state, &mut session, b"\0\0\0\0\0\0\0\0", b"P\0", b"\0\0\0\0\0");
            assert_eq!(giggles_core::SqlSession::test_cursor_query_binds(), before + 1);
        }
        let original_is_live = session.sql.test_cursor_binding_liveness().unwrap();
        assert!(original_is_live());
        assert_eq!(message(&mut state, &mut session, b'B', b"p\0\0\0\0\0\0\0\0").unwrap()[0], b'2');
        parse_unnamed(&mut state, &mut session, "SELECT id FROM public.t WHERE id = 8", &[]);
        assert!(!original_is_live());
        execute_bound_seven_and_sync(&mut state, &mut session);
        assert_eq!(session.sql.committed_catalog_version(), Some(version));
        assert_eq!(giggles_core::SqlSession::test_cursor_query_binds(), before + 2);
        run_cursor_cycle(&mut state, &mut session, b"\0\0\0\0\0\0\0\0", b"P\0", b"\0\0\0\0\0");
        assert_eq!(giggles_core::SqlSession::test_cursor_query_binds(), before + 3);
        assert!(session.sql.test_cursor_binding_liveness().unwrap()());
    }

    fn warm_named_cursor(state: &mut State, session: &mut Session) {
        session.execute_sql("CREATE TABLE t(id int PRIMARY KEY)", &[]).unwrap();
        session.execute_sql("INSERT INTO t VALUES (7)", &[]).unwrap();
        let parse = b"s\0SELECT id FROM public.t WHERE id = 7\0\0\0";
        assert_eq!(message(state, session, b'P', parse).unwrap()[0], b'1');
        run_cursor_cycle(state, session, b"p\0s\0\0\0\0\0\0\0", b"Pp\0", b"p\0\0\0\0\0");
    }

    // Text DataRow with one column containing the byte '7'.
    const SEVEN_ROW: &[u8] = b"D\0\0\0\x0b\0\x01\0\0\0\x017";

    fn execute_bound_seven_and_sync(state: &mut State, session: &mut Session) {
        let reply = message(state, session, b'E', b"p\0\0\0\0\0").unwrap();
        assert!(reply.starts_with(SEVEN_ROW));
        assert_eq!(message(state, session, b'S', &[]).unwrap()[0], b'Z');
        assert!(session.sql.test_cursor_binding_liveness().is_none());
    }

    #[test]
    fn close_statement_releases_cursor_binding_on_current_snapshot() {
        let mut session = fixture();
        let mut state = wire_state();
        warm_named_cursor(&mut state, &mut session);
        let version = session.sql.committed_catalog_version().unwrap();
        let binding_is_live = session.sql.test_cursor_binding_liveness().unwrap();
        assert!(binding_is_live());
        let bind = b"p\0s\0\0\0\0\0\0\0";
        assert_eq!(message(&mut state, &mut session, b'B', bind).unwrap()[0], b'2');
        assert_eq!(message(&mut state, &mut session, b'C', b"Ss\0").unwrap()[0], b'3');
        assert!(!binding_is_live());
        execute_bound_seven_and_sync(&mut state, &mut session);
        assert_eq!(session.sql.committed_catalog_version(), Some(version));
    }

    #[test]
    fn close_and_unnamed_parse_forget_only_cursor_statements() {
        let mut session = fixture();
        let mut state = wire_state();
        warm_named_cursor(&mut state, &mut session);
        let select_is_live = session.sql.test_cursor_binding_liveness().unwrap();
        assert!(select_is_live());

        let insert = b"dml\0INSERT INTO public.t VALUES (8)\0\0\0";
        assert_eq!(message(&mut state, &mut session, b'P', insert).unwrap()[0], b'1');
        let before = giggles_core::SqlSession::test_cursor_slot_locks();
        assert_eq!(message(&mut state, &mut session, b'C', b"Sdml\0").unwrap()[0], b'3');
        assert!(select_is_live());
        assert_eq!(giggles_core::SqlSession::test_cursor_slot_locks(), before, "DML Close");

        parse_unnamed(&mut state, &mut session, "INSERT INTO public.t VALUES (8)", &[]);
        let before = giggles_core::SqlSession::test_cursor_slot_locks();
        parse_unnamed(&mut state, &mut session, "DELETE FROM public.t WHERE id = 8", &[]);
        assert!(select_is_live());
        assert_eq!(giggles_core::SqlSession::test_cursor_slot_locks(), before, "DML replacement");

        assert_eq!(message(&mut state, &mut session, b'C', b"Ss\0").unwrap()[0], b'3');
        assert!(!select_is_live());

        let declare = b"d\0DECLARE c CURSOR WITH HOLD FOR SELECT id FROM public.t WHERE id = 7\0\0\0";
        assert_eq!(message(&mut state, &mut session, b'P', declare).unwrap()[0], b'1');
        assert_eq!(message(&mut state, &mut session, b'B', b"p\0d\0\0\0\0\0\0\0").unwrap()[0], b'2');
        assert_eq!(message(&mut state, &mut session, b'E', b"p\0\0\0\0\0").unwrap()[0], b'C');
        assert_eq!(message(&mut state, &mut session, b'S', &[]).unwrap()[0], b'Z');
        let declare_is_live = session.sql.test_cursor_binding_liveness().unwrap();
        assert!(declare_is_live());
        assert_eq!(message(&mut state, &mut session, b'C', b"Sd\0").unwrap()[0], b'3');
        assert!(!declare_is_live());
    }

    #[test]
    fn closed_statement_can_declare_a_cursor_without_retaining_a_binding() {
        let mut session = fixture();
        let mut state = wire_state();
        session.execute_sql("CREATE TABLE t(id int PRIMARY KEY)", &[]).unwrap();
        session.execute_sql("INSERT INTO t VALUES (7)", &[]).unwrap();
        let parse = b"s\0DECLARE c CURSOR WITH HOLD FOR SELECT id FROM public.t WHERE id = 7\0\0\0";
        assert_eq!(message(&mut state, &mut session, b'P', parse).unwrap()[0], b'1');
        let bind = b"p\0s\0\0\0\0\0\0\0";
        assert_eq!(message(&mut state, &mut session, b'B', bind).unwrap()[0], b'2');
        assert_eq!(message(&mut state, &mut session, b'C', b"Ss\0").unwrap()[0], b'3');
        assert_eq!(message(&mut state, &mut session, b'E', b"p\0\0\0\0\0").unwrap()[0], b'C');
        assert_eq!(message(&mut state, &mut session, b'S', &[]).unwrap()[0], b'Z');
        assert!(session.sql.test_cursor_binding_liveness().is_none());
        let reply = message(&mut state, &mut session, b'Q', b"FETCH 1 FROM c\0").unwrap();
        assert!(reply.windows(SEVEN_ROW.len()).any(|frame| frame == SEVEN_ROW));
    }

    fn simple_sql(state: &mut State, session: &mut Session, sql: &str) -> Vec<u8> {
        let mut query = Vec::new();
        z(sql, &mut query);
        message(state, session, b'Q', &query).unwrap_or_else(|code| panic!("{sql}: {code}"))
    }

    const BIND_SQL_PREPARED: &[u8] = b"p\0s\0\0\0\0\0\0\0";
    const EXECUTE_PREPARED_PORTAL: &[u8] = b"p\0\0\0\0\0";

    fn sql_prepared_cursor_fixture() -> (State, Session) {
        let mut state = wire_state();
        let mut session = fixture();
        simple_sql(&mut state, &mut session, "CREATE TABLE t(id int PRIMARY KEY)");
        simple_sql(&mut state, &mut session, "INSERT INTO t VALUES (7), (8)");
        simple_sql(&mut state, &mut session, "PREPARE s AS SELECT id FROM public.t WHERE id = 7");
        (state, session)
    }

    fn bind_and_execute_prepared_cursor(state: &mut State, session: &mut Session, row: &[u8]) {
        assert_eq!(message(state, session, b'B', BIND_SQL_PREPARED).unwrap()[0], b'2');
        let reply = message(state, session, b'E', EXECUTE_PREPARED_PORTAL).unwrap();
        assert!(reply.starts_with(row), "{reply:?}");
        assert_eq!(message(state, session, b'S', &[]).unwrap()[0], b'Z');
    }

    fn assert_sql_removal_releases_cursor_binding(sql: &str) {
        let (mut state, mut session) = sql_prepared_cursor_fixture();
        bind_and_execute_prepared_cursor(&mut state, &mut session, SEVEN_ROW);
        let binding_is_live = session.sql.test_cursor_binding_liveness().unwrap();
        assert!(binding_is_live());

        simple_sql(&mut state, &mut session, sql);
        assert!(!state.statements.contains_key("s"), "{sql}");
        assert!(session.sql.test_cursor_binding_liveness().is_none(), "{sql} left a binding in the slot");
        assert!(!binding_is_live(), "{sql} retained the bound query");
    }

    #[test]
    fn sql_prepared_binding_released_by_deallocate() {
        assert_sql_removal_releases_cursor_binding("DEALLOCATE s");
    }

    #[test]
    fn sql_prepared_binding_released_by_deallocate_all() {
        assert_sql_removal_releases_cursor_binding("DEALLOCATE ALL");
    }

    #[test]
    fn sql_prepared_binding_released_by_discard_all() {
        assert_sql_removal_releases_cursor_binding("DISCARD ALL");
    }

    const EIGHT_ROW: &[u8] = b"D\0\0\0\x0b\0\x01\0\0\0\x018";

    #[test]
    fn sql_prepared_replacement_binds_same_and_different_sql_afresh() {
        let (mut state, mut session) = sql_prepared_cursor_fixture();
        bind_and_execute_prepared_cursor(&mut state, &mut session, SEVEN_ROW);

        for (id, row) in [(7, SEVEN_ROW), (8, EIGHT_ROW)] {
            simple_sql(&mut state, &mut session, "DEALLOCATE s");
            simple_sql(
                &mut state,
                &mut session,
                &format!("PREPARE s AS SELECT id FROM public.t WHERE id = {id}"),
            );
            let before = giggles_core::SqlSession::test_cursor_query_binds();
            bind_and_execute_prepared_cursor(&mut state, &mut session, row);
            let binding_is_live = session.sql.test_cursor_binding_liveness().unwrap();
            assert!(binding_is_live());
            assert_eq!(giggles_core::SqlSession::test_cursor_query_binds(), before + 1);

            bind_and_execute_prepared_cursor(&mut state, &mut session, row);
            assert!(binding_is_live());
            assert_eq!(giggles_core::SqlSession::test_cursor_query_binds(), before + 1);
        }
    }

    #[test]
    fn sql_prepared_bound_portal_executes_uncached_after_statement_removal_or_refresh() {
        for sql in ["DEALLOCATE s", "DISCARD PLANS"] {
            let (mut state, mut session) = sql_prepared_cursor_fixture();
            bind_and_execute_prepared_cursor(&mut state, &mut session, SEVEN_ROW);
            let binding_is_live = session.sql.test_cursor_binding_liveness().unwrap();
            assert!(binding_is_live());

            // A named portal survives Sync and a simple query in this transaction.
            simple_sql(&mut state, &mut session, "BEGIN");
            assert_eq!(message(&mut state, &mut session, b'B', BIND_SQL_PREPARED).unwrap()[0], b'2');
            assert_eq!(message(&mut state, &mut session, b'S', &[]).unwrap()[0], b'Z');
            simple_sql(&mut state, &mut session, sql);
            if sql == "DISCARD PLANS" {
                // Describing the statement refreshes the discarded plan while
                // the already-bound portal keeps the previous plan.
                assert_eq!(message(&mut state, &mut session, b'D', b"Ss\0").unwrap()[0], b't');
            }
            assert!(!binding_is_live(), "{sql}");
            assert!(session.sql.test_cursor_binding_liveness().is_none(), "{sql}");
            execute_bound_seven_and_sync(&mut state, &mut session);
            simple_sql(&mut state, &mut session, "ROLLBACK");
        }
    }

    #[test]
    fn sql_prepared_discard_plans_releases_old_binding_on_rebind() {
        let (mut state, mut session) = sql_prepared_cursor_fixture();
        bind_and_execute_prepared_cursor(&mut state, &mut session, SEVEN_ROW);
        let old_binding_is_live = session.sql.test_cursor_binding_liveness().unwrap();
        assert!(old_binding_is_live());

        simple_sql(&mut state, &mut session, "DISCARD PLANS");
        assert_eq!(message(&mut state, &mut session, b'B', BIND_SQL_PREPARED).unwrap()[0], b'2');
        assert!(!old_binding_is_live());
        assert!(session.sql.test_cursor_binding_liveness().is_none());

        let before = giggles_core::SqlSession::test_cursor_query_binds();
        let reply = message(&mut state, &mut session, b'E', EXECUTE_PREPARED_PORTAL).unwrap();
        assert!(reply.starts_with(SEVEN_ROW), "{reply:?}");
        assert_eq!(message(&mut state, &mut session, b'S', &[]).unwrap()[0], b'Z');
        assert!(session.sql.test_cursor_binding_liveness().unwrap()());
        assert_eq!(giggles_core::SqlSession::test_cursor_query_binds(), before + 1);
    }

    #[test]
    fn sql_prepared_cursor_tracks_search_path_settings_and_rollback() {
        let mut state = wire_state();
        let mut session = fixture();
        for sql in [
            "CREATE SCHEMA alternate",
            "CREATE TABLE public.t(id int PRIMARY KEY)",
            "CREATE TABLE alternate.t(id int PRIMARY KEY)",
            "INSERT INTO public.t VALUES (7)",
            "INSERT INTO alternate.t VALUES (8)",
            "PREPARE s AS SELECT id FROM t",
        ] {
            simple_sql(&mut state, &mut session, sql);
        }
        bind_and_execute_prepared_cursor(&mut state, &mut session, SEVEN_ROW);

        for (commands, row) in [
            (&["SELECT set_config('search_path', 'alternate, public', false)"][..], EIGHT_ROW),
            (&["RESET search_path"][..], SEVEN_ROW),
            (&["BEGIN", "SET LOCAL search_path TO alternate, public"][..], EIGHT_ROW),
            (&["ROLLBACK"][..], SEVEN_ROW),
        ] {
            for sql in commands {
                simple_sql(&mut state, &mut session, sql);
            }
            let before = giggles_core::SqlSession::test_cursor_query_binds();
            bind_and_execute_prepared_cursor(&mut state, &mut session, row);
            let binding_is_live = session.sql.test_cursor_binding_liveness().unwrap();
            assert!(binding_is_live(), "{commands:?}");
            assert_eq!(giggles_core::SqlSession::test_cursor_query_binds(), before + 1, "{commands:?}");
            if session.in_transaction() {
                assert_eq!(message(&mut state, &mut session, b'C', b"Pp\0").unwrap()[0], b'3');
            }
            bind_and_execute_prepared_cursor(&mut state, &mut session, row);
            assert!(binding_is_live(), "{commands:?}");
            assert_eq!(giggles_core::SqlSession::test_cursor_query_binds(), before + 1, "{commands:?}");
        }
    }

    #[test]
    fn sql_prepared_execute_utility_leaves_cursor_binding_slot_empty() {
        let (mut state, mut session) = sql_prepared_cursor_fixture();
        let reply = simple_sql(&mut state, &mut session, "EXECUTE s");
        assert!(reply.windows(SEVEN_ROW.len()).any(|frame| frame == SEVEN_ROW));
        assert!(session.sql.test_cursor_binding_liveness().is_none());

        parse_unnamed(&mut state, &mut session, "EXECUTE s", &[]);
        let bind = b"p\0\0\0\0\0\0\0\0";
        assert_eq!(message(&mut state, &mut session, b'B', bind).unwrap()[0], b'2');
        execute_bound_seven_and_sync(&mut state, &mut session);
    }

    #[test]
    fn one_shot_cursors_leave_warmed_binding_intact() {
        let mut session = fixture();
        let mut state = wire_state();
        warm_named_cursor(&mut state, &mut session);
        let binding_is_live = session.sql.test_cursor_binding_liveness().unwrap();
        for query in [
            b"SELECT id FROM public.t WHERE id = 7\0".as_slice(),
            b"DECLARE c CURSOR WITH HOLD FOR SELECT id FROM public.t WHERE id = 7\0",
        ] {
            let before = giggles_core::SqlSession::test_cursor_query_binds();
            message(&mut state, &mut session, b'Q', query).unwrap();
            assert_eq!(giggles_core::SqlSession::test_cursor_query_binds(), before + 1);
            assert!(binding_is_live());
        }
    }

    #[test]
    fn success_memo_tracks_catalog_budgets_and_private_ddl() {
        let mut session = fixture();
        session.execute_sql("CREATE TABLE t(id int PRIMARY KEY, n int)", &[]).unwrap();
        let mut s = statement(&mut session, "SELECT id FROM t ORDER BY n");
        for _ in 0..5 { s.validate_constants(&mut session, &mut Diagnostic::default()).unwrap(); }
        assert_eq!(s.constant_validations, 1);
        for sql in ["ALTER TABLE t ADD COLUMN x int", "ALTER TABLE t DROP COLUMN x",
            "ALTER TABLE t ALTER COLUMN n TYPE text", "ALTER TABLE t DROP COLUMN n",
            "ALTER TABLE t ADD COLUMN n int", "DROP TABLE t",
            "CREATE TABLE t(id int PRIMARY KEY,n int)"] {
            session.execute_sql(sql, &[]).unwrap();
            let before = s.constant_validations;
            let result = s.validate_constants(&mut session, &mut Diagnostic::default());
            assert_eq!(s.constant_validations, before + 1, "{sql}");
            if sql == "DROP TABLE t" || sql == "ALTER TABLE t DROP COLUMN n" { assert!(result.is_err()); } else { result.unwrap(); }
        }
        session.execute_sql("CREATE SCHEMA earlier", &[]).unwrap();
        session.sql.set_search_path("earlier, public", "owner");
        s.validate_constants(&mut session, &mut Diagnostic::default()).unwrap();
        let before = s.constant_validations;
        session.execute_sql("CREATE TABLE earlier.t(id int PRIMARY KEY)", &[]).unwrap();
        s.validate_constants(&mut session, &mut Diagnostic::default()).unwrap();
        assert_eq!(s.constant_validations, before + 1);
        session.sql.set_search_path("public", "owner");
        s.validate_constants(&mut session, &mut Diagnostic::default()).unwrap();
        assert_eq!(s.constant_validations, before + 2);
        session.sql.set_max_statement_tokens(1);
        let before = s.constant_validations;
        let _ = s.validate_constants(&mut session, &mut Diagnostic::default());
        assert_eq!(s.constant_validations, before + 1);
        session.sql.set_max_statement_tokens(0);
        s.validate_constants(&mut session, &mut Diagnostic::default()).unwrap();
        session.sql.set_max_value_bytes(1024);
        let before = s.constant_validations;
        s.validate_constants(&mut session, &mut Diagnostic::default()).unwrap();
        assert_eq!(s.constant_validations, before + 1);
        session.execute_sql("BEGIN", &[]).unwrap();
        session.execute_sql("ALTER TABLE t ADD COLUMN x int", &[]).unwrap();
        assert!(session.sql.constants_validation_key().is_none());
        let before = s.constant_validations;
        for _ in 0..2 { s.validate_constants(&mut session, &mut Diagnostic::default()).unwrap(); }
        assert_eq!(s.constant_validations, before + 2);
        session.execute_sql("ROLLBACK", &[]).unwrap();
        s.validate_constants(&mut session, &mut Diagnostic::default()).unwrap();
        let before = s.constant_validations;
        s.validate_constants(&mut session, &mut Diagnostic::default()).unwrap();
        assert_eq!(s.constant_validations, before);
    }
    #[test]
    fn repeated_binds_publish_the_success_memo() {
        let mut session = fixture();
        let mut state = wire_state();
        let mut out = Vec::new();
        state.handle(b'P', b"named\0SELECT 1+2\0\0\0", &mut session, &mut out, std::time::Instant::now()).unwrap();
        let mut bind = b"\0named\0".to_vec();
        bind.extend([0; 6]);
        for _ in 0..5 {
            out.clear();
            state.handle(b'B', &bind, &mut session, &mut out, std::time::Instant::now()).unwrap();
            assert_eq!(out[0], b'2');
            state.handle(b'S', &[], &mut session, &mut out, std::time::Instant::now()).unwrap();
        }
        assert_eq!(state.statements["named"].constant_validations, 1);
    }
    #[test]
    fn unnamed_bind_memo_survives_parse_and_tracks_validation_context() {
        let mut session = fixture();
        let mut state = wire_state();
        session.execute_sql("CREATE TABLE t(id int PRIMARY KEY)", &[]).unwrap();
        let sql = "SELECT id, 1+2 FROM t";
        let prepare_calls = PREPARE_CALLS.get();
        for _ in 0..5 {
            parse_unnamed(&mut state, &mut session, sql, &[]);
            bind_unnamed(&mut state, &mut session, &[]).unwrap();
            assert_eq!(state.statements[""].constant_validations, 1);
            assert_eq!(state.unnamed_cache[0].constant_validations, 1);
        }
        assert_eq!(PREPARE_CALLS.get(), prepare_calls + 1);
        let old_id = state.statements[""].id;
        let old_key = state.unnamed_cache[0].validated.unwrap();
        assert_eq!(state.statements[""].validated, Some(old_key));

        // Bind refreshes the live statement after a catalog change. A stale
        // cached parse with the same text and OIDs must keep its own memo.
        session.execute_sql("CREATE TABLE other(id int PRIMARY KEY)", &[]).unwrap();
        bind_unnamed(&mut state, &mut session, &[]).unwrap();
        assert_ne!(state.statements[""].id, old_id);
        assert_eq!(state.statements[""].constant_validations, 1);
        assert_ne!(state.statements[""].validated.unwrap().0, old_key.0);
        assert_eq!(state.unnamed_cache[0].id, old_id);
        assert_eq!(state.unnamed_cache[0].validated, Some(old_key));
        parse_unnamed(&mut state, &mut session, sql, &[]);
        bind_unnamed(&mut state, &mut session, &[]).unwrap();
        let after_ddl = state.unnamed_cache[0].validated.unwrap();
        assert_ne!(after_ddl.0, old_key.0);
        assert_eq!(state.unnamed_cache[0].constant_validations, 1);

        message(&mut state, &mut session, b'Q', b"SET search_path TO public\0").unwrap();
        parse_unnamed(&mut state, &mut session, sql, &[]);
        bind_unnamed(&mut state, &mut session, &[]).unwrap();
        let after_path = state.unnamed_cache[0].validated.unwrap();
        assert_ne!(after_path.3, after_ddl.3);
        assert_eq!(state.unnamed_cache[0].constant_validations, 1);

        state.settings.current.max_value_bytes = giggles_core::relational::MAX_ROW_BYTES;
        bind_unnamed(&mut state, &mut session, &[]).unwrap();
        let after_budget = state.unnamed_cache[0].validated.unwrap();
        assert_ne!(after_budget.1, after_path.1);
        assert_eq!(state.statements[""].constant_validations, 2);
        assert_eq!(state.unnamed_cache[0].constant_validations, 2);
    }
    #[test]
    fn unnamed_bind_memo_distinguishes_text_and_declared_oids() {
        let mut session = fixture();
        let mut state = wire_state();
        session.execute_sql("CREATE TABLE t(id int PRIMARY KEY)", &[]).unwrap();
        let sql = "SELECT id FROM t WHERE id = $1";
        let other_sql = "SELECT id+1 FROM t WHERE id = $1";
        let mut ids = BTreeMap::new();
        for (sql, oid) in [(sql, 23), (other_sql, 23), (sql, 0), (sql, 23)] {
            parse_unnamed(&mut state, &mut session, sql, &[oid]);
            let s = &state.statements[""];
            if let Some(id) = ids.insert((sql, oid), s.id) {
                assert_eq!(s.id, id);
                assert!(s.validated.is_some());
            } else {
                assert_eq!(s.validated, None);
                assert_eq!(s.constant_validations, 0);
            }
            bind_unnamed(&mut state, &mut session, &["1"]).unwrap();
            assert_eq!(state.statements[""].constant_validations, 1);
        }
        assert_eq!(state.unnamed_cache.len(), 3);
    }
    #[test]
    fn unnamed_bind_errors_stay_unmemoized_after_reparse() {
        let mut session = fixture();
        let mut state = wire_state();
        session.execute_sql("CREATE TABLE t(id int PRIMARY KEY)", &[]).unwrap();
        let prepare_calls = PREPARE_CALLS.get();
        for attempts in 1..=3 {
            parse_unnamed(&mut state, &mut session, "SELECT 1/0 FROM t", &[]);
            assert_eq!(bind_unnamed(&mut state, &mut session, &[]), Err("22012"));
            assert!(state.statements[""].validated.is_none());
            assert!(state.unnamed_cache[0].validated.is_none());
            assert_eq!(state.statements[""].constant_validations, attempts);
            assert_eq!(state.unnamed_cache[0].constant_validations, attempts);
        }
        assert_eq!(PREPARE_CALLS.get(), prepare_calls + 1);
    }
    #[test]
    fn failed_unnamed_revalidation_clears_a_previous_memo() {
        let mut session = fixture();
        let mut state = wire_state();
        session.execute_sql("CREATE TABLE t(id int PRIMARY KEY)", &[]).unwrap();
        parse_unnamed(&mut state, &mut session, "SELECT repeat('x', 70000) FROM t", &[]);
        bind_unnamed(&mut state, &mut session, &[]).unwrap();
        let entry = state.unnamed_cache.last().unwrap();
        assert!(entry.validated.is_some());
        let id = entry.id;
        state.settings.current.max_value_bytes = giggles_core::relational::MAX_ROW_BYTES;
        assert_eq!(bind_unnamed(&mut state, &mut session, &[]), Err("54000"));
        let entry = state.unnamed_cache.last().unwrap();
        assert_eq!(state.statements[""].id, id);
        assert_eq!(entry.id, id);
        assert_eq!(entry.constant_validations, 2);
        assert!(entry.validated.is_none());
        assert!(state.statements[""].validated.is_none());
    }
    #[test]
    fn constant_errors_are_never_memoized() {
        let mut session = fixture();
        session.execute_sql("CREATE TABLE t(id int PRIMARY KEY)", &[]).unwrap();
        for sql in ["SELECT 1/0 FROM t", "SELECT id FROM t ORDER BY 1/0", "SELECT id FROM t WHERE 1/0=1"] {
            let mut s = statement(&mut session, sql);
            for n in 1..=3 {
                assert_eq!(s.validate_constants(&mut session, &mut Diagnostic::default()), Err("22012"));
                assert_eq!(s.constant_validations, n);
                assert!(s.validated.is_none());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn result_type_oids_matches_row_description() {
        for overrides in [vec![], vec![0, 2278, 20]] {
            let d = Description {
                columns: [DataType::Integer, DataType::Text, DataType::BigInt, DataType::Boolean]
                    .into_iter().enumerate().map(|(i, data_type)| giggles_core::relational::ColumnDef {
                        id: 0, name: format!("c{i}"), data_type, not_null: false,
                    }).collect(),
                type_oids: overrides,
                typmods: vec![-1, 7],
                parameters: vec![],
            };
            let types = result_type_oids(&d);
            assert_eq!(types, if d.type_oids.is_empty() { vec![23, 25, 20, 16] } else { vec![23, 2278, 20, 16] });
            // Independently assemble the original RowDescription layout, including
            // void's special size, typmods, and per-column result formats.
            let mut body = 4u16.to_be_bytes().to_vec();
            for (i, c) in d.columns.iter().enumerate() {
                z(&c.name, &mut body);
                body.extend(0u32.to_be_bytes());
                body.extend(0i16.to_be_bytes());
                body.extend(d.type_oids.get(i).copied().filter(|id| *id != 0)
                    .unwrap_or_else(|| oid(&c.data_type)).to_be_bytes());
                body.extend(if d.type_oids.get(i) == Some(&2278) { 4i16 } else { size(&c.data_type) }.to_be_bytes());
                body.extend(d.typmods.get(i).copied().unwrap_or(-1).to_be_bytes());
                body.extend(format_at(&[0, 1], i).to_be_bytes());
            }
            let mut expected = vec![];
            frame(b'T', &body, &mut expected).unwrap();
            let mut actual = vec![];
            row_description(&d, &[0, 1], &mut actual).unwrap();
            assert_eq!(actual, expected);
        }
    }
}
