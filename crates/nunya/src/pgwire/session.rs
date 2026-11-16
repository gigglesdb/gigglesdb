//! Fail-closed simple-query session state and downstream socket entry point.
use super::{
    auth::{self, ScramServer, ScramStep, ScramVerifier},
    codec::{self, Backend, FieldDescription, Frontend, StartupMessage},
    context::GatewayContext,
    redact::{nunya_error, redact_error},
    upstream::{Upstream, send_cancel},
    values,
};
use crate::{
    dialect::Lowering,
    error::{NyCode, Refusal},
    ir::{
        self, InputRewrite, OutputRewrite, ResolvedStatement, RewritePlan, StatementKind, UseKind,
        ValueSource,
    },
    policy::{DomainName, Policy, TokenizeSetting},
    readlog::{Outcome, ReadLogRecord, redact_tokenize_args, statement_fingerprint},
    state::{
        authority::{AdmitError, Operation, TokenizeError, TokenizeLimits},
        model::Mode,
    },
    token::{PrincipalId, ScopeKeys, SessionId, TokenCodec, keys::derive_scope_v2},
    value::SqlValue,
};
use bytes::BytesMut;
mod extended;
use extended::{Pending, Portal, Prepared};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, atomic::Ordering},
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
fn failure(code: NyCode) -> Refusal {
    Refusal::new(code, "session refused")
}
fn protocol() -> Refusal {
    failure(NyCode::ProtocolNotAllowed)
}
fn random<const N: usize>() -> Result<[u8; N], Refusal> {
    let mut b = [0; N];
    getrandom::fill(&mut b).map_err(|_| protocol())?;
    Ok(b)
}
fn allowed_status(name: &str) -> bool {
    matches!(
        name,
        "server_version"
            | "server_encoding"
            | "client_encoding"
            | "DateStyle"
            | "TimeZone"
            | "integer_datetimes"
            | "standard_conforming_strings"
            | "IntervalStyle"
            | "application_name"
    )
}
/// Messages emitted by one state-machine transition.
#[derive(Default)]
pub struct Actions {
    /// Requests to send upstream before processing another transition.
    pub upstream: Vec<Frontend>,
    /// Responses ready to send to the agent.
    pub client: Vec<Backend>,
    /// Close both transports after sending responses.
    pub close: bool,
}
struct Active {
    plan: RewritePlan,
    kind: StatementKind,
    record: ReadLogRecord,
    policy: Arc<Policy>,
    described: bool,
}
/// Socket-independent simple and extended protocol state with ordered responses.
pub struct Session {
    ctx: Arc<GatewayContext>,
    keys: ScopeKeys,
    session: SessionId,
    #[allow(dead_code)]
    principal: PrincipalId,
    credential: String,
    active: VecDeque<Active>,
    waiting: bool,
    status: u8,
    aborted: bool,
    skip: bool,
    empty_pending: bool,
    pending: VecDeque<Pending>,
    prepared: BTreeMap<String, Prepared>,
    portals: BTreeMap<String, Portal>,
    extended_deferred: VecDeque<Frontend>,
    extended_deferred_bytes: usize,
    extended_execute_forwarded: bool,
    rollback_on_sync: bool,
}
impl Session {
    /// Construct an authenticated session with independently derived scope keys.
    pub fn new(
        ctx: Arc<GatewayContext>,
        keys: ScopeKeys,
        session: SessionId,
        principal: PrincipalId,
        credential: String,
    ) -> Self {
        Self {
            ctx,
            keys,
            session,
            principal,
            credential,
            active: VecDeque::new(),
            waiting: false,
            status: b'I',
            aborted: false,
            skip: false,
            empty_pending: false,
            pending: VecDeque::new(),
            prepared: BTreeMap::new(),
            portals: BTreeMap::new(),
            extended_deferred: VecDeque::new(),
            extended_deferred_bytes: 0,
            extended_execute_forwarded: false,
            rollback_on_sync: false,
        }
    }
    /// Whether a forwarded write or commit still awaits completion.
    pub(crate) fn write_in_flight(&self) -> bool {
        let indeterminate =
            |active: &Active| writes(&active.kind) || matches!(active.kind, StatementKind::Commit);
        self.active.iter().any(indeterminate)
            || self.pending.iter().any(|pending| match pending {
                Pending::Execute(active, _) => indeterminate(active),
                Pending::Local(_)
                | Pending::Ack(_, _, _)
                | Pending::Hidden(_)
                | Pending::RefusalError
                | Pending::Describe(_, _, _)
                | Pending::Sync => false,
            })
    }
    /// Audit and close an ended connection, preserving uncertain write outcomes.
    pub(crate) fn end(&mut self) -> Actions {
        let code = if self.write_in_flight() {
            NyCode::WriteOutcomeUnknown
        } else {
            NyCode::SessionEnded
        };
        let mut actions = self.refuse("", code, false);
        actions.close = true;
        actions
    }
    fn admit(&self) -> Result<Mode, Refusal> {
        self.ctx
            .authority
            .admit(self.session, Operation::Read)
            .map(|admitted| admitted.mode)
            .map_err(|error| {
                failure(match error {
                    AdmitError::Unknown | AdmitError::Ended => NyCode::SessionEnded,
                    AdmitError::ReadOnly => NyCode::StatementNotAllowed,
                })
            })
    }
    fn ready(&self) -> Backend {
        Backend::ReadyForQuery(if self.aborted && self.status != b'I' {
            b'E'
        } else {
            self.status
        })
    }
    fn record(&self, sql: &str, outcome: Outcome) -> ReadLogRecord {
        let statement = redact_tokenize_args(sql);
        ReadLogRecord {
            ts_unix_ms: (self.ctx.clock)(),
            task: None,
            credential: self.credential.clone(),
            statement_fingerprint: statement_fingerprint(&statement),
            statement,
            outcome,
            rows: 0,
            tokens_released: vec![],
        }
    }
    fn refuse(&mut self, sql: &str, code: NyCode, ready: bool) -> Actions {
        self.ctx.metrics.refused(code);
        if self.status == b'T' {
            self.aborted = true;
        }
        let failed = self
            .ctx
            .readlog
            .write(&self.record(sql, Outcome::Refused(code)))
            .is_err();
        let mut out = Actions {
            client: vec![Backend::ErrorResponse(nunya_error(if failed {
                NyCode::ReadLogUnavailable
            } else {
                code
            }))],
            close: failed || code == NyCode::SessionEnded,
            ..Actions::default()
        };
        if ready && !out.close {
            out.client.push(self.ready());
        }
        out
    }
    fn record_tokenization(&self, domain: &DomainName) -> Result<(), Refusal> {
        let policy = self
            .ctx
            .policy
            .read()
            .map_err(|_| failure(NyCode::SessionEnded))?;
        let settings = policy
            .domains
            .get(domain)
            .ok_or_else(|| failure(NyCode::StatementNotAllowed))?;
        let TokenizeSetting::On {
            limit_per_session,
            limit_per_principal,
            principal_window,
        } = settings.tokenize
        else {
            return Err(failure(NyCode::StatementNotAllowed));
        };
        self.ctx
            .authority
            .record_tokenization(
                self.session,
                domain,
                &TokenizeLimits {
                    per_session: u64::from(limit_per_session),
                    per_principal: u64::from(limit_per_principal),
                    principal_window_ms: u64::try_from(principal_window.as_millis())
                        .unwrap_or(u64::MAX),
                },
            )
            .map_err(|error| {
                failure(match error {
                    TokenizeError::LimitReached => NyCode::TokenizeLimit,
                    TokenizeError::Ended => NyCode::SessionEnded,
                    // Unavailable authorization state must fail closed.
                    TokenizeError::Store => NyCode::SessionEnded,
                })
            })
    }
    /// Consume an agent request without performing network I/O.
    pub fn frontend(&mut self, msg: Frontend) -> Actions {
        if matches!(msg, Frontend::Terminate) {
            return Actions {
                close: true,
                ..Actions::default()
            };
        }
        if self.waiting {
            return Actions {
                client: vec![Backend::ErrorResponse(nunya_error(
                    NyCode::ProtocolNotAllowed,
                ))],
                close: true,
                ..Actions::default()
            };
        }
        if matches!(msg, Frontend::Query(_))
            && !self.skip
            && !self.pending.iter().any(|p| matches!(p, Pending::Sync))
        {
            if !self.pending.is_empty() {
                return Actions {
                    client: vec![Backend::ErrorResponse(nunya_error(
                        NyCode::ProtocolNotAllowed,
                    ))],
                    close: true,
                    ..Actions::default()
                };
            }
            if let Frontend::Query(sql) = msg {
                return self.query(&sql);
            }
        }
        self.extended_frontend(msg)
    }
    fn query(&mut self, sql: &str) -> Actions {
        let result = (|| {
            // Admit at processing time so deferred queries use current authority.
            let mode = self.admit()?;
            let policy = self.ctx.policy.read().map_err(|_| protocol())?.clone();
            let snapshot = self.ctx.snapshot.read().map_err(|_| protocol())?.clone();
            if (self.ctx.clock)().saturating_sub(snapshot.taken_at_unix_ms)
                > policy.snapshot_max_age_ms
            {
                return Err(failure(NyCode::SchemaStale));
            }
            let statements = match self.ctx.lowering.lower(sql, &snapshot, &policy, &[]) {
                Ok(s) => s,
                Err(_) if self.aborted => return Ok(None),
                Err(e) => return Err(e),
            };
            if !mode.permits_write() && statements.iter().any(|s| writes(&s.kind)) {
                return Err(failure(NyCode::StatementNotAllowed));
            }
            if self.aborted
                && (statements.len() != 1 || !statements.iter().all(|s| recovery(&s.kind)))
            {
                return Ok(None);
            }
            let plans = statements
                .iter()
                .map(|s| ir::check::check(s, &policy, &snapshot))
                .collect::<Result<Vec<_>, _>>()?;
            if plans.iter().any(|p| p.tokenize.is_some()) && plans.len() != 1 {
                return Err(failure(NyCode::StatementNotAllowed));
            }
            let noops = statements
                .iter()
                .filter(|s| matches!(s.kind, StatementKind::SessionNoop { .. }))
                .count();
            if noops != 0 && noops != statements.len() {
                return Err(failure(NyCode::StatementNotAllowed));
            }
            let rewritten = if noops != 0 {
                String::new()
            } else if self.aborted
                && matches!(
                    statements.first().map(|s| &s.kind),
                    Some(StatementKind::Commit)
                ) {
                "ROLLBACK".into()
            } else {
                rewrite_literals(
                    sql,
                    &statements,
                    &plans,
                    &policy,
                    &self.keys,
                    &self.ctx.codec,
                )?
            };
            // Persist all intent records before any upstream statement can execute.
            for statement in &statements {
                let text = sql
                    .get(statement.source_span.start..statement.source_span.end)
                    .ok_or_else(protocol)?;
                self.ctx
                    .readlog
                    .write(&self.record(text, Outcome::Allowed))?;
            }
            self.ctx
                .metrics
                .statements_allowed
                .fetch_add(statements.len() as u64, Ordering::Relaxed);
            if noops != 0 {
                // application_name is deliberately local: ParameterStatus stays unchanged.
                let mut client = Vec::new();
                for statement in &statements {
                    match statement.kind {
                        StatementKind::SessionNoop { tag } => {
                            client.push(Backend::CommandComplete(tag.into()))
                        }
                        _ => return Err(failure(NyCode::StatementNotAllowed)),
                    }
                }
                client.push(self.ready());
                return Ok(Some(Actions {
                    client,
                    ..Actions::default()
                }));
            }
            if let Some((domain, arg)) = plans.first().and_then(|p| p.tokenize.as_ref()) {
                let ty = policy
                    .columns
                    .iter()
                    .find_map(|(c, t)| match t {
                        crate::policy::Treatment::Blind { domain: d } if d == domain => {
                            snapshot.column(c).map(|c| c.ty.clone())
                        }
                        _ => None,
                    })
                    .ok_or_else(|| failure(NyCode::NotAnalysable))?;
                let ValueSource::Literal { text, .. } = arg else {
                    return Err(failure(NyCode::NotAnalysable));
                };
                let settings = policy.domains.get(domain).ok_or_else(protocol)?;
                let value = values::decode_text(&ty, text.as_bytes())?;
                self.record_tokenization(domain)?;
                let encoded = self.ctx.codec.encode(
                    &self.keys,
                    domain,
                    &ty,
                    settings.range.as_ref(),
                    &value,
                )?;
                let bytes = values::encode_text(&encoded)?;
                let mut record = self.record(sql, Outcome::Allowed);
                record.rows = 1;
                record
                    .tokens_released
                    .push(String::from_utf8(bytes.clone()).map_err(|_| protocol())?);
                self.ctx.readlog.write(&record)?;
                let field = FieldDescription {
                    name: "nunya_tokenize".into(),
                    table_oid: 0,
                    column_attr: 0,
                    type_oid: ty.pg_oid().ok_or_else(protocol)?,
                    type_size: -1,
                    type_modifier: -1,
                    format: 0,
                };
                return Ok(Some(Actions {
                    client: vec![
                        Backend::RowDescription(vec![field]),
                        Backend::DataRow(vec![Some(bytes)]),
                        Backend::CommandComplete("SELECT 1".into()),
                        self.ready(),
                    ],
                    ..Actions::default()
                }));
            }
            for (statement, plan) in statements.into_iter().zip(plans) {
                let text = sql
                    .get(statement.source_span.start..statement.source_span.end)
                    .ok_or_else(protocol)?;
                self.active.push_back(Active {
                    plan,
                    kind: statement.kind,
                    record: self.record(text, Outcome::Allowed),
                    policy: policy.clone(),
                    described: false,
                });
            }
            self.empty_pending = self.active.is_empty();
            self.waiting = true;
            Ok(Some(Actions {
                upstream: vec![Frontend::Query(rewritten)],
                ..Actions::default()
            }))
        })();
        match result {
            Ok(Some(out)) => out,
            Ok(None) => {
                let mut out = self.refuse(sql, NyCode::StatementNotAllowed, true);
                if !out.close {
                    out.client[0] = Backend::ErrorResponse(redact_error(&[(b'C', "25P02".into())]));
                }
                out
            }
            Err(e) => {
                let mut out = self.refuse(sql, e.code, true);
                if e.code == NyCode::ReadLogUnavailable {
                    out.close = true;
                    out.client.truncate(1);
                }
                out
            }
        }
    }
    /// Consume an upstream response, sanitizing every data-bearing result.
    pub fn backend(&mut self, msg: Backend) -> Actions {
        if !self.pending.is_empty() {
            return self.extended_backend(msg);
        }
        let result = self.response(msg);
        match result {
            Ok(client) => {
                let mut out = Actions {
                    client,
                    ..Actions::default()
                };
                self.drain_deferred(&mut out);
                out
            }
            Err(e) => {
                self.ctx.metrics.refused(e.code);
                Actions {
                    client: vec![Backend::ErrorResponse(nunya_error(e.code))],
                    close: true,
                    ..Actions::default()
                }
            }
        }
    }
    fn response(&mut self, msg: Backend) -> Result<Vec<Backend>, Refusal> {
        match msg {
            Backend::ReadyForQuery(status) => {
                if !self.waiting
                    || self.empty_pending
                    || !self.active.is_empty()
                    || !matches!(status, b'I' | b'T' | b'E')
                {
                    return Err(protocol());
                }
                self.waiting = false;
                self.status = status;
                if status == b'E' {
                    self.aborted = true;
                }
                if status == b'I' {
                    self.aborted = false;
                }
                Ok(vec![self.ready()])
            }
            Backend::RowDescription(mut fields) => {
                let active = self.active.front_mut().ok_or_else(protocol)?;
                if active.described
                    || fields.len() != active.plan.outputs.len()
                    || fields.iter().any(|f| f.format != 0)
                {
                    return Err(failure(NyCode::NotAnalysable));
                }
                for (field, output) in fields.iter_mut().zip(&active.plan.outputs) {
                    if !matches!(output, OutputRewrite::Pass) {
                        field.type_modifier = -1;
                    }
                }
                active.described = true;
                Ok(vec![Backend::RowDescription(fields)])
            }
            Backend::DataRow(row) => {
                let active = self.active.front_mut().ok_or_else(protocol)?;
                if !active.described || row.len() != active.plan.outputs.len() {
                    return Err(failure(NyCode::NotAnalysable));
                }
                let (row, tokens) = rewrite_row(
                    row,
                    &active.plan,
                    &[],
                    &active.policy,
                    &self.keys,
                    &self.ctx.codec,
                )
                .map_err(|_| failure(NyCode::BlindOperation))?;
                active.record.tokens_released.extend(tokens);
                active.record.rows += 1;
                Ok(vec![Backend::DataRow(row)])
            }
            Backend::CommandComplete(tag) => {
                let mut active = self.active.pop_front().ok_or_else(protocol)?;
                let rows = tag
                    .split_whitespace()
                    .last()
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(0);
                if active.described && rows != active.record.rows {
                    return Err(failure(NyCode::NotAnalysable));
                }
                active.record.rows = rows;
                self.ctx.readlog.write(&active.record)?;
                let commit = self.aborted && matches!(active.kind, StatementKind::Commit);
                if recovery(&active.kind) {
                    self.aborted = false;
                }
                Ok(vec![Backend::CommandComplete(if commit {
                    "ROLLBACK".into()
                } else {
                    tag
                })])
            }
            Backend::EmptyQueryResponse => {
                if !self.waiting || !self.empty_pending || !self.active.is_empty() {
                    return Err(protocol());
                }
                self.empty_pending = false;
                Ok(vec![Backend::EmptyQueryResponse])
            }
            Backend::ErrorResponse(fields) => {
                if !self.waiting || (self.active.is_empty() && !self.empty_pending) {
                    return Err(protocol());
                }
                self.empty_pending = false;
                // An upstream error terminates this query and skips its remaining statements.
                if let Some(active) = self.active.pop_front() {
                    self.ctx.readlog.write(&active.record)?;
                }
                self.active.clear();
                Ok(vec![Backend::ErrorResponse(redact_error(&fields))])
            }
            Backend::NoticeResponse(fields) => {
                Ok(vec![Backend::NoticeResponse(redact_error(&fields))])
            }
            Backend::ParameterStatus(name, value) => Ok(if allowed_status(&name) {
                vec![Backend::ParameterStatus(name, value)]
            } else {
                vec![]
            }),
            Backend::Refused(b'A') => Ok(vec![]),
            Backend::AuthenticationOk
            | Backend::AuthenticationSasl(_)
            | Backend::AuthenticationSaslContinue(_)
            | Backend::AuthenticationSaslFinal(_)
            | Backend::BackendKeyData { .. }
            | Backend::ParseComplete
            | Backend::BindComplete
            | Backend::CloseComplete
            | Backend::NoData
            | Backend::PortalSuspended
            | Backend::ParameterDescription(_)
            | Backend::NegotiateProtocolVersion { .. }
            | Backend::Refused(_) => Err(protocol()),
        }
    }
}
fn writes(kind: &StatementKind) -> bool {
    match kind {
        StatementKind::Insert | StatementKind::Update | StatementKind::Delete => true,
        StatementKind::Select
        | StatementKind::Begin
        | StatementKind::Commit
        | StatementKind::Rollback
        | StatementKind::Savepoint
        | StatementKind::ReleaseSavepoint
        | StatementKind::RollbackToSavepoint
        | StatementKind::SessionNoop { .. }
        | StatementKind::Tokenize { .. }
        | StatementKind::Refused(_) => false,
    }
}
fn recovery(kind: &StatementKind) -> bool {
    matches!(
        kind,
        StatementKind::Commit | StatementKind::Rollback | StatementKind::RollbackToSavepoint
    )
}
fn literal_text<'a>(statement: &'a ResolvedStatement, span: &ir::Span) -> Result<&'a str, Refusal> {
    let uses = statement.uses.iter().filter_map(|u| match &u.kind {
        UseKind::Equality { other } => Some(other),
        _ => None,
    });
    uses.chain(statement.writes.iter().map(|w| &w.value))
        .find_map(|v| match v {
            ValueSource::Literal { span: s, text } if s == span => Some(text.as_str()),
            _ => None,
        })
        .ok_or_else(|| failure(NyCode::NotAnalysable))
}
fn rewrite_literals(
    sql: &str,
    statements: &[ResolvedStatement],
    plans: &[RewritePlan],
    policy: &Policy,
    keys: &ScopeKeys,
    codec: &impl TokenCodec,
) -> Result<String, Refusal> {
    let mut replacements = vec![];
    for (statement, plan) in statements.iter().zip(plans) {
        for input in &plan.inputs {
            match input {
                InputRewrite::DecryptLiteral { span, domain, ty } => {
                    let text = literal_text(statement, span)?;
                    let value = if ty.is_textual() {
                        SqlValue::Text(text.into())
                    } else {
                        values::decode_text(ty, text.as_bytes())?
                    };
                    let range = policy
                        .domains
                        .get(domain)
                        .ok_or_else(protocol)?
                        .range
                        .as_ref();
                    let decoded = codec.decode(keys, domain, ty, range, &value)?;
                    replacements.push((span.clone(), ir::rewrite::literal_for(&decoded)?));
                }
                InputRewrite::DecryptParam { .. } => {}
            }
        }
    }
    let rewritten = ir::rewrite::splice(sql, &replacements)?;
    for (span, text) in string_literals(sql) {
        let consumed = replacements
            .iter()
            .any(|(s, _)| s.start <= span.start && s.end >= span.end);
        let exempt = statements.iter().zip(plans).any(|(statement, plan)| {
            plan.tokenize.is_some()
                && statement.source_span.start <= span.start
                && statement.source_span.end >= span.end
        });
        if !consumed && !exempt && crate::token::detect::looks_like_token(&text, policy) {
            return Err(failure(NyCode::TokenOutsideDomain));
        }
    }
    Ok(rewritten)
}

// Scan original SQL only; decrypted plaintext must never be classified as a token.
fn string_literals(sql: &str) -> Vec<(ir::Span, String)> {
    let bytes = sql.as_bytes();
    let mut out = vec![];
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i..].starts_with(b"--") {
            i += 2;
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
        } else if bytes[i..].starts_with(b"/*") {
            i += 2;
            let mut depth = 1;
            while i < bytes.len() && depth > 0 {
                if bytes[i..].starts_with(b"/*") {
                    depth += 1;
                    i += 2;
                } else if bytes[i..].starts_with(b"*/") {
                    depth -= 1;
                    i += 2;
                } else {
                    i += 1;
                }
            }
        } else if bytes[i] == b'"' {
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'"' {
                    i += 1;
                    if bytes.get(i) == Some(&b'"') {
                        i += 1;
                    } else {
                        break;
                    }
                } else {
                    i += 1;
                }
            }
        } else if bytes[i] == b'\'' {
            let start = i;
            i += 1;
            let mut text = String::new();
            let mut part = i;
            while i < bytes.len() {
                if bytes[i] == b'\'' {
                    text.push_str(&sql[part..i]);
                    i += 1;
                    if bytes.get(i) == Some(&b'\'') {
                        text.push('\'');
                        i += 1;
                        part = i;
                    } else {
                        break;
                    }
                } else {
                    i += 1;
                }
            }
            out.push((ir::Span { start, end: i }, text));
        } else if bytes[i].is_ascii_alphabetic() || bytes[i] == b'_' || bytes[i] >= 128 {
            i += 1;
            while i < bytes.len()
                && (bytes[i].is_ascii_alphanumeric()
                    || matches!(bytes[i], b'_' | b'$')
                    || bytes[i] >= 128)
            {
                i += 1;
            }
        } else if bytes[i] == b'$' {
            let start = i;
            let mut end = i + 1;
            while end < bytes.len() && (bytes[end].is_ascii_alphanumeric() || bytes[end] == b'_') {
                end += 1;
            }
            let valid_tag = end == i + 1 || !bytes[i + 1].is_ascii_digit();
            if valid_tag && bytes.get(end) == Some(&b'$') {
                let delimiter = &sql[start..=end];
                let body = end + 1;
                if let Some(close) = sql[body..].find(delimiter) {
                    i = body + close + delimiter.len();
                    out.push((ir::Span { start, end: i }, sql[body..body + close].into()));
                } else {
                    i = bytes.len();
                }
            } else {
                i += 1;
            }
        } else {
            i += 1;
        }
    }
    out
}
fn format_at(formats: &[i16], index: usize, len: usize) -> Result<i16, Refusal> {
    let f = match formats.len() {
        0 => 0,
        1 => formats[0],
        n if n == len => formats[index],
        _ => return Err(protocol()),
    };
    if matches!(f, 0 | 1) {
        Ok(f)
    } else {
        Err(protocol())
    }
}
type RewrittenRow = (Vec<Option<Vec<u8>>>, Vec<String>);

fn rewrite_row(
    row: Vec<Option<Vec<u8>>>,
    plan: &RewritePlan,
    formats: &[i16],
    policy: &Policy,
    keys: &ScopeKeys,
    codec: &impl TokenCodec,
) -> Result<RewrittenRow, Refusal> {
    if row.len() != plan.outputs.len() {
        return Err(failure(NyCode::NotAnalysable));
    }
    let len = row.len();
    let mut result = vec![];
    let mut tokens = vec![];
    for (i, (cell, output)) in row.into_iter().zip(&plan.outputs).enumerate() {
        let format = format_at(formats, i, len)?;
        result.push(match (cell, output) {
            (None, _) => None,
            (Some(bytes), OutputRewrite::Pass) => Some(bytes),
            (Some(_), OutputRewrite::Mask { placeholder }) => {
                placeholder.as_ref().map(|s| s.as_bytes().to_vec())
            }
            (Some(bytes), OutputRewrite::Tokenize { domain, ty }) => {
                let value = if format == 0 {
                    values::decode_text(ty, &bytes)?
                } else {
                    values::decode_binary(ty, &bytes)?
                };
                let range = policy
                    .domains
                    .get(domain)
                    .ok_or_else(protocol)?
                    .range
                    .as_ref();
                let token = codec.encode(keys, domain, ty, range, &value)?;
                tokens
                    .push(String::from_utf8(values::encode_text(&token)?).map_err(|_| protocol())?);
                Some(if format == 0 {
                    values::encode_text(&token)?
                } else {
                    values::encode_binary(ty, &token)?
                })
            }
        });
    }
    Ok((result, tokens))
}
fn rewrite_bind(
    params: &mut [Option<Vec<u8>>],
    formats: &[i16],
    declared: &[u32],
    plan: &RewritePlan,
    policy: &Policy,
    keys: &ScopeKeys,
    codec: &impl TokenCodec,
) -> Result<(), Refusal> {
    let len = params.len();
    if plan.tokenize.is_none() {
        for (i, param) in params.iter().enumerate() {
            let consumed = plan.inputs.iter().any(|input| {
                matches!(input,
                InputRewrite::DecryptParam { index, .. } if *index as usize == i + 1)
            });
            let format = format_at(formats, i, len)?;
            if !consumed && (format == 0 || matches!(declared.get(i), Some(25 | 1042 | 1043)))
                && let Some(bytes) = param
            {
                let text = std::str::from_utf8(bytes).map_err(|_| protocol())?;
                if crate::token::detect::looks_like_token(text, policy) {
                    return Err(failure(NyCode::TokenOutsideDomain));
                }
            }
        }
    }
    for input in &plan.inputs {
        match input {
            InputRewrite::DecryptParam { index, domain, ty } => {
                let i = usize::try_from(*index)
                    .map_err(|_| protocol())?
                    .checked_sub(1)
                    .filter(|i| *i < len)
                    .ok_or_else(protocol)?;
                let oid = declared.get(i).copied().unwrap_or(0);
                if oid != 0
                    && Some(oid) != ty.pg_oid()
                    && !(ty.is_textual() && matches!(oid, 25 | 1042 | 1043))
                {
                    return Err(failure(NyCode::NotAnalysable));
                }
                let format = format_at(formats, i, len)?;
                if let Some(bytes) = &params[i] {
                    let token = if format == 0 {
                        values::decode_text(ty, bytes)?
                    } else {
                        values::decode_binary(ty, bytes)?
                    };
                    let range = policy
                        .domains
                        .get(domain)
                        .ok_or_else(protocol)?
                        .range
                        .as_ref();
                    let value = codec.decode(keys, domain, ty, range, &token)?;
                    params[i] = Some(if format == 0 {
                        values::encode_text(&value)?
                    } else {
                        values::encode_binary(ty, &value)?
                    });
                }
            }
            InputRewrite::DecryptLiteral { .. } => {}
        }
    }
    Ok(())
}
fn encode_limited(msg: &Backend, out: &mut BytesMut, limit: usize) -> Result<(), Refusal> {
    let start = out.len();
    codec::encode_backend(msg, out);
    // The wire length includes the four-byte length word, but excludes the tag.
    if out.len().saturating_sub(start).saturating_sub(1) > limit {
        out.truncate(start);
        return Err(protocol());
    }
    Ok(())
}
trait Transport: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Transport for T {}
struct Agent {
    io: Box<dyn Transport>,
    buffer: BytesMut,
    ctx: Arc<GatewayContext>,
}
impl Agent {
    async fn fill(&mut self) -> Result<(), Refusal> {
        let mut bytes = [0; 8192];
        let n = self.io.read(&mut bytes).await.map_err(|_| protocol())?;
        if n == 0 {
            return Err(protocol());
        }
        self.ctx
            .metrics
            .bytes
            .fetch_add(n as u64, Ordering::Relaxed);
        self.buffer.extend_from_slice(&bytes[..n]);
        Ok(())
    }
    async fn startup(&mut self) -> Result<StartupMessage, Refusal> {
        loop {
            if let Some(msg) = codec::decode_startup(&mut self.buffer)? {
                return Ok(msg);
            }
            self.fill().await?;
        }
    }
    async fn recv(&mut self) -> Result<Frontend, Refusal> {
        loop {
            if let Some(msg) = codec::decode_frontend(&mut self.buffer)? {
                return Ok(msg);
            }
            self.fill().await?;
        }
    }
    async fn send(&mut self, messages: &[Backend]) -> Result<(), Refusal> {
        let mut bytes = BytesMut::new();
        for msg in messages {
            encode_limited(msg, &mut bytes, codec::MAX_MESSAGE)?;
        }
        self.ctx
            .metrics
            .bytes
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        self.io.write_all(&bytes).await.map_err(|_| protocol())?;
        self.io.flush().await.map_err(|_| protocol())
    }
}
struct CancelGuard {
    ctx: Arc<GatewayContext>,
    key: (i32, [u8; 4]),
}
impl Drop for CancelGuard {
    fn drop(&mut self) {
        if let Ok(mut map) = self.ctx.cancel_map.lock() {
            map.remove(&self.key);
        }
    }
}
/// Serve one agent TCP connection; all errors are data-free and close the transport.
pub async fn serve_tcp(tcp: tokio::net::TcpStream, ctx: Arc<GatewayContext>) {
    ctx.metrics.connections.fetch_add(1, Ordering::Relaxed);
    let mut agent = Agent {
        io: Box::new(tcp),
        buffer: BytesMut::new(),
        ctx: ctx.clone(),
    };
    if let Err(error) = serve(&mut agent, ctx).await {
        let _ = agent
            .send(&[Backend::ErrorResponse(nunya_error(error.code))])
            .await;
    }
    let _ = agent.io.shutdown().await;
}
async fn end_connection(
    agent: &mut Agent,
    upstream: &mut Upstream,
    session: &mut Session,
) -> Result<(), Refusal> {
    if session.waiting || session.write_in_flight() {
        let (pid, secret) = upstream.backend_key();
        let _ = send_cancel(&session.ctx.upstream, pid, &secret).await;
    }
    let actions = session.end();
    agent.send(&actions.client).await
}
async fn serve(agent: &mut Agent, ctx: Arc<GatewayContext>) -> Result<(), Refusal> {
    let mut tls = false;
    let user = loop {
        match agent.startup().await? {
            StartupMessage::SslRequest => {
                if tls {
                    return Err(protocol());
                }
                if let Some(config) = &ctx.tls {
                    // Reject bytes pipelined ahead of TLS: they cannot be treated as encrypted application data.
                    if !agent.buffer.is_empty() {
                        return Err(protocol());
                    }
                    agent.io.write_all(b"S").await.map_err(|_| protocol())?;
                    agent.io.flush().await.map_err(|_| protocol())?;
                    let old = std::mem::replace(&mut agent.io, Box::new(tokio::io::duplex(1).0));
                    let encrypted = tokio_rustls::TlsAcceptor::from(config.clone())
                        .accept(old)
                        .await
                        .map_err(|_| protocol())?;
                    agent.io = Box::new(encrypted);
                    tls = true;
                } else {
                    agent.io.write_all(b"N").await.map_err(|_| protocol())?;
                }
            }
            StartupMessage::GssEncRequest => {
                agent.io.write_all(b"N").await.map_err(|_| protocol())?;
            }
            StartupMessage::CancelRequest { pid, secret } => {
                if let Ok(secret) = <[u8; 4]>::try_from(secret.as_slice()) {
                    let target = ctx
                        .cancel_map
                        .lock()
                        .ok()
                        .and_then(|map| map.get(&(pid, secret)).cloned());
                    if let Some((pid, secret)) = target {
                        let _ = send_cancel(&ctx.upstream, pid, &secret).await;
                    }
                }
                return Ok(());
            }
            StartupMessage::Startup {
                protocol: version,
                params,
            } => {
                if !tls && !ctx.allow_plaintext {
                    return Err(protocol());
                }
                if version == 196610 {
                    agent
                        .send(&[Backend::NegotiateProtocolVersion {
                            newest_minor: 0,
                            unrecognized: vec![],
                        }])
                        .await?;
                }
                break params
                    .into_iter()
                    .find(|(key, _)| key == "user")
                    .map(|(_, value)| value)
                    .ok_or_else(protocol)?;
            }
        }
    };
    let login = auth::parse_session_user(&user).and_then(|id| {
        ctx.authority
            .session_login(id, &ctx.route)
            .map(|login| (id, login))
    });
    let candidate = login
        .as_ref()
        .map(|(_, login)| login.verifiers.iter().map(|v| v.clone().0).collect());
    let verifiers = match candidate {
        Some(v) => v,
        None => vec![ScramVerifier {
            salt: random::<16>()?.to_vec(),
            iterations: 4096,
            stored_key: random::<32>()?,
            server_key: random::<32>()?,
        }],
    };
    let nonce = random::<18>()?;
    let mut scram = ScramServer::new(verifiers, || nonce);
    agent
        .send(&[Backend::AuthenticationSasl(ScramServer::mechanisms())])
        .await?;
    let Frontend::Password(initial) = agent.recv().await? else {
        return Err(failure(NyCode::TokenNotValid));
    };
    let (mechanism, data) = auth::parse_sasl_initial(&initial)?;
    let ScramStep::Continue(first) = scram.handle_initial(&mechanism, &data)? else {
        return Err(protocol());
    };
    agent
        .send(&[Backend::AuthenticationSaslContinue(first)])
        .await?;
    let Frontend::Password(final_msg) = agent.recv().await? else {
        return Err(failure(NyCode::TokenNotValid));
    };
    let ScramStep::Done { server_final } = scram
        .handle_final(&final_msg)
        .map_err(|_| failure(NyCode::TokenNotValid))?
    else {
        return Err(protocol());
    };
    let (id, login) = login.ok_or_else(|| failure(NyCode::TokenNotValid))?;
    ctx.authority
        .admit(id, crate::state::authority::Operation::Read)
        .map_err(|_| failure(NyCode::TokenNotValid))?;
    let keys = derive_scope_v2(
        &ctx.root_key,
        &login.scope.salt,
        &login.scope.deployment_id,
        login.scope.route.as_str(),
    )
    .map_err(|_| failure(NyCode::TokenNotValid))?;
    let mut upstream = Upstream::connect(&ctx.upstream, &ctx.search_path).await?;
    agent
        .send(&[
            Backend::AuthenticationSaslFinal(server_final),
            Backend::AuthenticationOk,
        ])
        .await?;
    for (name, value) in upstream.parameter_statuses() {
        if allowed_status(name) {
            agent
                .send(&[Backend::ParameterStatus(name.clone(), value.clone())])
                .await?;
        }
    }
    let key = loop {
        let key = (
            i32::from_be_bytes(random::<4>()?) & i32::MAX,
            random::<4>()?,
        );
        let mut map = ctx.cancel_map.lock().map_err(|_| protocol())?;
        if let std::collections::hash_map::Entry::Vacant(entry) = map.entry(key) {
            entry.insert(upstream.backend_key());
            break key;
        }
    };
    let _guard = CancelGuard {
        ctx: ctx.clone(),
        key,
    };
    let deadline = ctx.authority.session(id).map(|row| {
        tokio::time::Instant::now()
            + std::time::Duration::from_millis(row.expires_ms.saturating_sub(crate::serve::now()))
    });
    let (_connection_guard, mut end) =
        ctx.registry
            .register(id, login.principal, ctx.route.as_str(), deadline);
    let mut session = Session::new(ctx.clone(), keys, id, login.principal, user);
    // Recheck after registration so revocation between login and registration cannot be missed.
    if ctx.authority.admit(id, Operation::Read).is_err() {
        let actions = session.refuse("", NyCode::SessionEnded, false);
        agent.send(&actions.client).await?;
        return Ok(());
    }
    agent
        .send(&[
            Backend::BackendKeyData {
                pid: key.0,
                secret: key.1.to_vec(),
            },
            Backend::ReadyForQuery(b'I'),
        ])
        .await?;
    let mut deferred = VecDeque::new();
    let mut deferred_bytes = 0usize;
    loop {
        if end.try_ended().is_some() {
            end_connection(agent, &mut upstream, &mut session).await?;
            return Ok(());
        }
        let actions = if session.waiting {
            tokio::select! {
                biased;
                _=end.ended()=>{
                    end_connection(agent, &mut upstream, &mut session).await?;
                    return Ok(());
                }
                response=upstream.recv()=>session.backend(response?),
                request=agent.recv()=>{
                    let request=request?;
                    if matches!(request,Frontend::Terminate){break;}
                    let mut encoded=BytesMut::new();
                    if matches!(request,Frontend::Refused(_)){deferred_bytes=deferred_bytes.saturating_add(5);}else{codec::encode_frontend(&request,&mut encoded);deferred_bytes=deferred_bytes.saturating_add(encoded.len());}
                    if deferred.len()>=64 || deferred_bytes>codec::MAX_MESSAGE{return Err(protocol());}
                    deferred.push_back(request);
                    Actions::default()
                }
            }
        } else if let Some(request) = deferred.pop_front() {
            if deferred.is_empty() {
                deferred_bytes = 0;
            }
            session.frontend(request)
        } else {
            tokio::select! {
                biased;
                _=end.ended()=>{
                    end_connection(agent, &mut upstream, &mut session).await?;
                    return Ok(());
                }
                response=upstream.recv()=>session.backend(response?),
                request=agent.recv()=>session.frontend(request?),
            }
        };
        if !actions.upstream.is_empty() {
            upstream.send(&actions.upstream).await?;
        }
        agent.send(&actions.client).await?;
        if actions.close {
            break;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::ColumnType;
    use crate::{
        pgwire::{
            context::Metrics,
            upstream::{UpstreamConfig, UpstreamTls},
        },
        policy::{DomainSettings, ReadLogSink, Treatment},
        readlog::ReadLog,
        schema::{Column, ColumnRef, Relation, RelationKind, Snapshot},
        token::{StandardCodec, keys::RootKey},
    };
    use std::{
        path::PathBuf,
        sync::{Mutex, RwLock, atomic::AtomicBool},
    };
    struct Audit {
        fail: Arc<AtomicBool>,
        allowed_records: Arc<std::sync::atomic::AtomicU64>,
        records: Arc<Mutex<Vec<(u64, usize)>>>,
    }
    impl ReadLog for Audit {
        fn write(&self, r: &ReadLogRecord) -> Result<(), Refusal> {
            if self.fail.load(Ordering::Relaxed) {
                return Err(failure(NyCode::ReadLogUnavailable));
            }
            if matches!(r.outcome, Outcome::Allowed) {
                self.allowed_records.fetch_add(1, Ordering::Relaxed);
            }
            self.records
                .lock()
                .unwrap()
                .push((r.rows, r.tokens_released.len()));
            Ok(())
        }
    }
    struct Fixture {
        access: std::sync::OnceLock<(SessionId, PrincipalId)>,
        ctx: Arc<GatewayContext>,
        dir: PathBuf,
        fail: Arc<AtomicBool>,
        allowed_records: Arc<std::sync::atomic::AtomicU64>,
        records: Arc<Mutex<Vec<(u64, usize)>>>,
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }
    impl Fixture {
        fn new() -> Self {
            let domain = DomainName("secret".into());
            let col = ColumnRef {
                schema: "public".into(),
                table: "items".into(),
                column: "value".into(),
            };
            let policy = Policy {
                columns: [(
                    col,
                    Treatment::Blind {
                        domain: domain.clone(),
                    },
                )]
                .into(),
                domains: [(
                    domain,
                    DomainSettings {
                        tokenize: TokenizeSetting::On {
                            limit_per_session: 2,
                            limit_per_principal: 20,
                            principal_window: std::time::Duration::from_secs(86400),
                        },
                        range: None,
                    },
                )]
                .into(),
                snapshot_max_age_ms: 100,
                readlog: ReadLogSink::Stdout,
            };
            let snapshot = Snapshot::new(
                vec![Relation {
                    schema: "public".into(),
                    name: "items".into(),
                    kind: RelationKind::Table,
                    columns: vec![Column {
                        name: "value".into(),
                        ty: ColumnType::Text,
                        nullable: true,
                        generated: None,
                        default: None,
                    }],
                    view_definition: None,
                    primary_key: vec![],
                }],
                vec![],
                vec!["public".into()],
                100,
            );
            let dir = std::env::temp_dir().join(format!(
                "session-{}",
                data_encoding::HEXLOWER.encode(&random::<16>().unwrap())
            ));
            let fail = Arc::new(AtomicBool::new(false));
            let records = Arc::new(Mutex::new(vec![]));
            let allowed_records = Arc::new(std::sync::atomic::AtomicU64::new(0));
            let runtime = crate::state::runtime::StateRuntime::open(
                &dir.join("state"),
                &RootKey::from_hex(&"00".repeat(32)).unwrap(),
                &crate::config::SessionSettings::default(),
                &[],
            )
            .unwrap();
            let ctx = Arc::new(GatewayContext {
                authority: runtime.authority.clone(),
                registry: runtime.registry.clone(),
                route: crate::state::model::RouteId::new("postgres").unwrap(),
                policy: RwLock::new(Arc::new(policy)),
                snapshot: RwLock::new(Arc::new(snapshot)),
                root_key: RootKey::from_hex(&"00".repeat(32)).unwrap(),
                readlog: Box::new(Audit {
                    fail: fail.clone(),
                    allowed_records: allowed_records.clone(),
                    records: records.clone(),
                }),
                upstream: UpstreamConfig {
                    host: "unused".into(),
                    port: 5432,
                    user: "unused".into(),
                    password: zeroize::Zeroizing::new(String::new()),
                    dbname: "unused".into(),
                    tls: UpstreamTls::Disable,
                    application_name: "nunya".into(),
                    connect_timeout_ms: 100,
                },
                search_path: vec!["public".into()],
                lowering: crate::dialect::pg::PgLowering,
                codec: StandardCodec,
                clock: Arc::new(|| 100),
                cancel_map: Mutex::new(Default::default()),
                metrics: Metrics::default(),
                tls: None,
                allow_plaintext: true,
            });
            Self {
                access: std::sync::OnceLock::new(),
                ctx,
                dir,
                fail,
                allowed_records,
                records,
            }
        }
        fn session(&self) -> Session {
            let &(id, principal) = self.access.get_or_init(|| acquire_login(self, "postgres"));
            self.for_access(id, principal)
        }
        fn for_access(&self, id: SessionId, principal: PrincipalId) -> Session {
            let row = self.ctx.authority.session(id).unwrap();
            let scope = self.ctx.authority.scope(row.scope).unwrap();
            let keys = derive_scope_v2(
                &self.ctx.root_key,
                &scope.salt,
                &scope.deployment_id,
                scope.route.as_str(),
            )
            .unwrap();
            Session::new(self.ctx.clone(), keys, id, principal, "agent".into())
        }
        fn second_session(&self) -> Session {
            use crate::state::{authority::AcquireRequest, model::Verifier};
            let first = self.session();
            let row = self
                .ctx
                .authority
                .acquire_session(
                    first.principal,
                    AcquireRequest {
                        route: self.ctx.route.clone(),
                        mode: None,
                        lifetime_ms: None,
                        isolated: false,
                        label: None,
                        idempotency_key: "second".into(),
                        verifier: Verifier(ScramVerifier::from_password(
                            "test",
                            b"test-salt",
                            4096,
                        )),
                        secret_sha256: [1; 32],
                    },
                )
                .unwrap();
            self.for_access(row.id, first.principal)
        }
    }

    fn acquire_login(f: &Fixture, route: &str) -> (SessionId, PrincipalId) {
        acquire_login_with_mode(f, route, Mode::ReadWrite)
    }
    fn acquire_login_with_mode(f: &Fixture, route: &str, mode: Mode) -> (SessionId, PrincipalId) {
        use crate::state::{
            authority::AcquireRequest,
            model::{PrincipalKind, RouteId, Verifier},
        };
        use sha2::{Digest, Sha256};
        let a = &f.ctx.authority;
        let p = a
            .create_principal("api-agent", PrincipalKind::Agent, false, "login")
            .unwrap();
        let route = RouteId::new(route).unwrap();
        a.set_grants(p.id, vec![(route.clone(), mode)])
            .unwrap();
        a.add_principal_credential(
            p.id,
            Verifier(ScramVerifier::from_password(
                "login-secret",
                b"test-salt",
                4096,
            )),
            None,
        )
        .unwrap();
        let s = a
            .acquire_session(
                p.id,
                AcquireRequest {
                    route,
                    mode: None,
                    lifetime_ms: None,
                    isolated: false,
                    label: None,
                    idempotency_key: "login".into(),
                    verifier: Verifier(ScramVerifier::from_password(
                        "login-secret",
                        b"test-salt",
                        4096,
                    )),
                    secret_sha256: Sha256::digest(b"login-secret").into(),
                },
            )
            .unwrap();
        (s.id, p.id)
    }
    fn prepare_admission_portal(session: &mut Session, sql: &str) {
        let out = session.frontend(Frontend::Parse {
            name: "s".into(),
            query: sql.into(),
            param_types: vec![],
        });
        assert!(matches!(out.upstream.as_slice(), [Frontend::Parse { .. }]));
        assert!(matches!(
            session.backend(Backend::ParseComplete).client.as_slice(),
            [Backend::ParseComplete]
        ));
        let out = session.frontend(Frontend::Bind {
            portal: "p".into(),
            statement: "s".into(),
            param_formats: vec![],
            params: vec![],
            result_formats: vec![],
        });
        assert!(matches!(out.upstream.as_slice(), [Frontend::Bind { .. }]));
        assert!(matches!(
            session.backend(Backend::BindComplete).client.as_slice(),
            [Backend::BindComplete]
        ));
    }
    #[test]
    fn admission_simple_query_after_revocation() {
        let f = Fixture::new();
        let mut session = f.session();
        f.ctx
            .authority
            .revoke(session.session, crate::state::model::EndReason::AdminRevoked)
            .unwrap();
        let out = session.frontend(Frontend::Query("SELECT value FROM items".into()));
        assert_eq!(code(&out), "NY011");
        assert!(out.close);
        assert!(out.upstream.is_empty());
        assert_eq!(out.client.len(), 1);
        assert_eq!(*f.records.lock().unwrap(), vec![(0, 0)]);
    }
    #[test]
    fn admission_execute_after_revocation() {
        let f = Fixture::new();
        let mut session = f.session();
        prepare_admission_portal(&mut session, "SELECT value FROM items");
        f.ctx
            .authority
            .revoke(session.session, crate::state::model::EndReason::AdminRevoked)
            .unwrap();
        let out = session.frontend(Frontend::Execute {
            portal: "p".into(),
            max_rows: 0,
        });
        assert_eq!(code(&out), "NY011");
        assert!(out.close);
        assert!(out.upstream.is_empty());
        assert_eq!(*f.records.lock().unwrap(), vec![(0, 0)]);
    }
    #[test]
    fn admission_read_only_simple_write_refused_read_forwarded() {
        let f = Fixture::new();
        let (id, principal) = acquire_login_with_mode(&f, "postgres", Mode::ReadOnly);
        let mut session = f.for_access(id, principal);
        let out = session.frontend(Frontend::Query(
            "INSERT INTO items(value) VALUES (NULL)".into(),
        ));
        assert_eq!(code(&out), "NY006");
        assert!(!out.close);
        assert!(out.upstream.is_empty());
        let out = session.frontend(Frontend::Query("SELECT value FROM items".into()));
        assert!(matches!(out.upstream.as_slice(), [Frontend::Query(_)]));
    }
    #[test]
    fn admission_read_only_execute_write_refused() {
        let f = Fixture::new();
        let (id, principal) = acquire_login_with_mode(&f, "postgres", Mode::ReadOnly);
        let mut session = f.for_access(id, principal);
        prepare_admission_portal(&mut session, "INSERT INTO items(value) VALUES (NULL)");
        let out = session.frontend(Frontend::Execute {
            portal: "p".into(),
            max_rows: 0,
        });
        assert_eq!(code(&out), "NY006");
        assert!(!out.close);
        assert!(out.upstream.is_empty());
    }
    #[test]
    fn admission_reduced_grant_refuses_next_write_allows_read() {
        let f = Fixture::new();
        let mut session = f.session();
        prepare_admission_portal(&mut session, "INSERT INTO items(value) VALUES (NULL)");
        f.ctx
            .authority
            .set_grants(session.principal, vec![(f.ctx.route.clone(), Mode::ReadOnly)])
            .unwrap();
        let out = session.frontend(Frontend::Execute {
            portal: "p".into(),
            max_rows: 0,
        });
        assert_eq!(code(&out), "NY006");
        assert!(out.upstream.is_empty());
        let sync = session.frontend(Frontend::Sync);
        assert!(matches!(sync.upstream.as_slice(), [Frontend::Sync]));
        session.backend(Backend::ReadyForQuery(b'I'));
        let out = session.frontend(Frontend::Query(
            "INSERT INTO items(value) VALUES (NULL)".into(),
        ));
        assert_eq!(code(&out), "NY006");
        assert!(out.upstream.is_empty());
        let out = session.frontend(Frontend::Query("SELECT value FROM items".into()));
        assert!(matches!(out.upstream.as_slice(), [Frontend::Query(_)]));
    }
    #[test]
    fn admission_read_only_mixed_query_refused_as_a_whole() {
        let f = Fixture::new();
        let (id, principal) = acquire_login_with_mode(&f, "postgres", Mode::ReadOnly);
        let mut session = f.for_access(id, principal);
        let out = session.frontend(Frontend::Query(
            "SELECT value FROM items; INSERT INTO items(value) VALUES (NULL)".into(),
        ));
        assert_eq!(code(&out), "NY006");
        assert!(out.upstream.is_empty());
        assert!(session.active.is_empty());
        assert_eq!(f.allowed_records.load(Ordering::Relaxed), 0);
        assert_eq!(*f.records.lock().unwrap(), vec![(0, 0)]);
    }
    async fn assert_login_refused(f: &mut Fixture, user: &str, password: &str) {
        let upstream = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ctx = Arc::get_mut(&mut f.ctx).unwrap();
        ctx.upstream.host = "127.0.0.1".into();
        ctx.upstream.port = upstream.local_addr().unwrap().port();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let ctx = f.ctx.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            serve_tcp(stream, ctx).await;
        });
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            tokio_postgres::Config::new()
                .host("127.0.0.1")
                .port(address.port())
                .user(user)
                .password(password)
                .dbname("app")
                .connect(tokio_postgres::NoTls),
        )
        .await
        .unwrap();
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("login unexpectedly accepted"),
        };
        assert_eq!(error.code().map(|c| c.code()), Some("NY003"));
        server.await.unwrap();
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(25), upstream.accept())
                .await
                .is_err()
        );
    }
    fn acquire_overlap(f: &Fixture) -> SessionId {
        use crate::state::model::Verifier;
        use sha2::{Digest, Sha256};
        let (id, principal) = acquire_login(f, "postgres");
        f.ctx
            .authority
            .renew(
                principal,
                id,
                None,
                Some((
                    Verifier(ScramVerifier::from_password(
                        "rotated-secret",
                        b"test-salt",
                        4096,
                    )),
                    Sha256::digest(b"rotated-secret").into(),
                )),
            )
            .unwrap();
        id
    }
    async fn assert_login_accepted(f: &mut Fixture, id: SessionId, password: &str) {
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let ctx = Arc::get_mut(&mut f.ctx).unwrap();
            ctx.upstream.host = "127.0.0.1".into();
            ctx.upstream.port = listener.local_addr().unwrap().port();
            let fake_ctx = f.ctx.clone();
            let fake = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                let mut wire = Agent { io: Box::new(stream), buffer: BytesMut::new(), ctx: fake_ctx };
                assert!(matches!(wire.startup().await.unwrap(), StartupMessage::Startup { .. }));
                wire.send(&[
                    Backend::AuthenticationOk,
                    Backend::BackendKeyData { pid: 123, secret: vec![1, 2, 3, 4] },
                    Backend::ReadyForQuery(b'I'),
                ]).await.unwrap();
                assert!(matches!(wire.recv().await.unwrap(), Frontend::Query(sql) if sql.starts_with("SET search_path")));
                wire.send(&[Backend::CommandComplete("SET".into()), Backend::ReadyForQuery(b'I')]).await.unwrap();
                let mut byte = [0];
                while wire.io.read(&mut byte).await.unwrap() != 0 {}
            });
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let ctx = f.ctx.clone();
            let server = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                serve_tcp(stream, ctx).await;
            });
            let (client, connection) = tokio_postgres::Config::new()
                .host("127.0.0.1").port(address.port())
                .user(format!("session:{}", id.to_hex())).password(password)
                .dbname("app").connect(tokio_postgres::NoTls).await.unwrap();
            let connection = tokio::spawn(connection);
            drop(client);
            connection.await.unwrap().unwrap();
            server.await.unwrap();
            fake.await.unwrap();
        }).await.unwrap();
    }
    #[tokio::test]
    async fn overlap_accepts_both_session_passwords() {
        let mut f = Fixture::new();
        let id = acquire_overlap(&f);
        assert_login_accepted(&mut f, id, "login-secret").await;
        assert_login_accepted(&mut f, id, "rotated-secret").await;
    }
    #[tokio::test]
    async fn revoked_overlap_password_refused_before_upstream() {
        use sha2::{Digest, Sha256};
        let mut f = Fixture::new();
        let id = acquire_overlap(&f);
        f.ctx
            .authority
            .revoke_login_credential(id, Sha256::digest(b"login-secret").into());
        assert_login_refused(&mut f, &format!("session:{}", id.to_hex()), "login-secret").await;
        assert_login_accepted(&mut f, id, "rotated-secret").await;
    }
    #[tokio::test]
    async fn principal_credentials_are_not_database_logins() {
        let mut f = Fixture::new();
        acquire_login(&f, "postgres");
        assert_login_refused(&mut f, "api-agent", "login-secret").await;
    }
    #[tokio::test]
    async fn task_login_refused() {
        let mut f = Fixture::new();
        assert_login_refused(
            &mut f,
            "task:0123456789abcdef0123456789abcdef",
            "login-secret",
        )
        .await;
    }
    #[tokio::test]
    async fn session_wrong_password_refused() {
        let mut f = Fixture::new();
        let (id, _) = acquire_login(&f, "postgres");
        assert_login_refused(&mut f, &format!("session:{}", id.to_hex()), "wrong").await;
    }
    #[tokio::test]
    async fn spanner_session_refused_on_postgres() {
        let mut f = Fixture::new();
        let (id, _) = acquire_login(&f, "spanner-native");
        assert_login_refused(&mut f, &format!("session:{}", id.to_hex()), "login-secret").await;
    }
    #[tokio::test]
    async fn revoked_session_refused() {
        let mut f = Fixture::new();
        let (id, _) = acquire_login(&f, "postgres");
        f.ctx
            .authority
            .revoke(id, crate::state::model::EndReason::Requested)
            .unwrap();
        assert_login_refused(&mut f, &format!("session:{}", id.to_hex()), "login-secret").await;
    }
    #[test]
    fn session_settings_simple_are_local_and_mixes_refused() {
        let f = Fixture::new();
        let mut session = f.session();
        let out = session.frontend(Frontend::Query(
            "SET extra_float_digits = 3; SET application_name = 'tok_secret_a'".into(),
        ));
        assert!(out.upstream.is_empty());
        assert!(
            matches!(out.client.as_slice(), [Backend::CommandComplete(a), Backend::CommandComplete(b), Backend::ReadyForQuery(b'I')] if a == "SET" && b == "SET")
        );
        assert_eq!(*f.records.lock().unwrap(), vec![(0, 0), (0, 0)]);
        assert_eq!(f.allowed_records.load(Ordering::Relaxed), 2);
        let mixed = session.frontend(Frontend::Query(
            "SET extra_float_digits = 3; SELECT value FROM items".into(),
        ));
        assert!(mixed.upstream.is_empty());
        assert_eq!(code(&mixed), "NY006");
        let transaction = session.frontend(Frontend::Query("SET TRANSACTION READ ONLY".into()));
        assert!(
            matches!(transaction.upstream.as_slice(), [Frontend::Query(q)] if q == "SET TRANSACTION READ ONLY")
        );
    }
    #[test]
    fn session_setting_pipeline_preserves_upstream_order() {
        let f = Fixture::new();
        let mut session = f.session();
        let first = session.frontend(Frontend::Parse {
            name: "remote".into(),
            query: "SELECT value FROM items".into(),
            param_types: vec![],
        });
        assert_eq!(first.upstream.len(), 1);
        let local = session.frontend(Frontend::Parse {
            name: "local".into(),
            query: "SET extra_float_digits = 3".into(),
            param_types: vec![],
        });
        assert!(local.client.is_empty());
        assert!(local.upstream.is_empty());
        assert!(matches!(
            session.backend(Backend::ParseComplete).client.as_slice(),
            [Backend::ParseComplete, Backend::ParseComplete]
        ));
    }
    #[test]
    fn session_settings_extended_are_local() {
        for query in [
            "SET extra_float_digits = 3",
            "SET application_name = 'tok_secret_a'",
        ] {
            let f = Fixture::new();
            let mut session = f.session();
            let out = session.frontend(Frontend::Parse {
                name: "s".into(),
                query: query.into(),
                param_types: vec![],
            });
            assert!(out.upstream.is_empty());
            assert!(matches!(out.client.as_slice(), [Backend::ParseComplete]));
            let out = session.frontend(Frontend::Bind {
                portal: "p".into(),
                statement: "s".into(),
                param_formats: vec![],
                params: vec![],
                result_formats: vec![],
            });
            assert!(out.upstream.is_empty());
            assert!(matches!(out.client.as_slice(), [Backend::BindComplete]));
            for kind in *b"SP" {
                let out = session.frontend(Frontend::Describe {
                    kind,
                    name: if kind == b'S' { "s" } else { "p" }.into(),
                });
                assert!(out.upstream.is_empty());
                assert!(matches!(out.client.last(), Some(Backend::NoData)));
            }
            let out = session.frontend(Frontend::Execute {
                portal: "p".into(),
                max_rows: 0,
            });
            assert!(out.upstream.is_empty());
            assert!(
                matches!(out.client.as_slice(), [Backend::CommandComplete(tag)] if tag == "SET")
            );
            assert_eq!(*f.records.lock().unwrap(), vec![(0, 0)]);
            assert_eq!(f.allowed_records.load(Ordering::Relaxed), 1);
            for (kind, name) in [(b'P', "p"), (b'S', "s")] {
                let out = session.frontend(Frontend::Close {
                    kind,
                    name: name.into(),
                });
                assert!(out.upstream.is_empty());
                assert!(matches!(out.client.as_slice(), [Backend::CloseComplete]));
            }
            // Sync retains the local-tokenize path's upstream synchronization.
            assert!(matches!(
                session.frontend(Frontend::Sync).upstream.as_slice(),
                [Frontend::Sync]
            ));
            assert!(matches!(
                session
                    .backend(Backend::ReadyForQuery(b'I'))
                    .client
                    .as_slice(),
                [Backend::ReadyForQuery(b'I')]
            ));
        }
    }
    #[test]
    fn write_in_flight_tracks_simple_and_extended_completion() {
        let f = Fixture::new();
        let mut s = f.session();
        assert!(!s.write_in_flight());
        for (sql, tag, expected) in [
            ("INSERT INTO items(value) VALUES (NULL)", "INSERT 0 1", true),
            ("COMMIT", "COMMIT", true),
            ("SELECT value FROM items", "SELECT 0", false),
        ] {
            let out = s.frontend(Frontend::Query(sql.into()));
            assert!(matches!(out.upstream.as_slice(), [Frontend::Query(_)]));
            assert_eq!(s.write_in_flight(), expected);
            s.backend(Backend::CommandComplete(tag.into()));
            assert!(!s.write_in_flight());
            s.backend(Backend::ReadyForQuery(b'I'));
            assert!(!s.write_in_flight());
        }
        prepare_admission_portal(&mut s, "INSERT INTO items(value) VALUES (NULL)");
        assert!(!s.write_in_flight());
        let out = s.frontend(Frontend::Execute {
            portal: "p".into(),
            max_rows: 0,
        });
        assert!(matches!(
            out.upstream.as_slice(),
            [Frontend::Execute { .. }]
        ));
        assert!(s.write_in_flight());
        s.backend(Backend::CommandComplete("INSERT 0 1".into()));
        assert!(!s.write_in_flight());
    }
    #[test]
    fn end_audits_commit_uncertainty_and_select_revocation() {
        for (sql, expected) in [("COMMIT", "NY012"), ("SELECT value FROM items", "NY011")] {
            let f = Fixture::new();
            let mut s = f.session();
            assert!(!s.frontend(Frontend::Query(sql.into())).upstream.is_empty());
            f.ctx
                .authority
                .revoke(s.session, crate::state::model::EndReason::AdminRevoked)
                .unwrap();
            let before = f.records.lock().unwrap().len();
            let out = s.end();
            assert_eq!(code(&out), expected);
            assert!(out.close);
            assert!(out.upstream.is_empty());
            assert_eq!(out.client.len(), 1);
            assert_eq!(f.records.lock().unwrap().len(), before + 1);
        }
    }
    #[test]
    fn end_audit_failure_returns_ny009() {
        let f = Fixture::new();
        let mut s = f.session();
        s.frontend(Frontend::Query("COMMIT".into()));
        f.fail.store(true, Ordering::Relaxed);
        let out = s.end();
        assert_eq!(code(&out), "NY009");
        assert!(out.close);
        assert!(out.upstream.is_empty());
        assert_eq!(out.client.len(), 1);
    }
    #[tokio::test]
    async fn registered_connection_receives_authority_revocation() {
        let f = Fixture::new();
        let s = f.session();
        let deadline = f.ctx.authority.session(s.session).map(|row| {
            tokio::time::Instant::now()
                + std::time::Duration::from_millis(
                    row.expires_ms.saturating_sub(crate::serve::now()),
                )
        });
        let (_guard, mut end) =
            f.ctx
                .registry
                .register(s.session, s.principal, f.ctx.route.as_str(), deadline);
        assert!(f.ctx.authority.admit(s.session, Operation::Read).is_ok());
        assert!(end.try_ended().is_none());
        f.ctx
            .authority
            .revoke(s.session, crate::state::model::EndReason::AdminRevoked)
            .unwrap();
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(1), end.ended())
                .await
                .unwrap(),
            crate::state::registry::EndReason::Revoked,
        );
    }
    #[tokio::test]
    async fn tcp_revocation_cancels_forwarded_commit_with_ny012() {
        tokio::time::timeout(std::time::Duration::from_secs(15), async {
            let mut f = Fixture::new();
            let (id, _) = acquire_login(&f, "postgres");
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let ctx = Arc::get_mut(&mut f.ctx).unwrap();
            ctx.upstream.host = "127.0.0.1".into();
            ctx.upstream.port = listener.local_addr().unwrap().port();
            ctx.upstream.connect_timeout_ms = 5000;
            let fake_ctx = f.ctx.clone();
            let (forwarded_tx, forwarded_rx) = tokio::sync::oneshot::channel();
            let fake = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                let mut wire = Agent { io: Box::new(stream), buffer: BytesMut::new(), ctx: fake_ctx.clone() };
                assert!(matches!(wire.startup().await.unwrap(), StartupMessage::Startup { .. }));
                wire.send(&[
                    Backend::AuthenticationOk,
                    Backend::BackendKeyData { pid: 123, secret: vec![1, 2, 3, 4] },
                    Backend::ReadyForQuery(b'I'),
                ]).await.unwrap();
                assert!(matches!(wire.recv().await.unwrap(), Frontend::Query(_)));
                wire.send(&[Backend::CommandComplete("SET".into()), Backend::ReadyForQuery(b'I')]).await.unwrap();
                assert!(matches!(wire.recv().await.unwrap(), Frontend::Query(sql) if sql == "COMMIT"));
                forwarded_tx.send(()).unwrap();
                let (cancel, _) = listener.accept().await.unwrap();
                let mut cancel = Agent { io: Box::new(cancel), buffer: BytesMut::new(), ctx: fake_ctx };
                assert!(matches!(cancel.startup().await.unwrap(), StartupMessage::CancelRequest { pid: 123, secret } if secret == [1, 2, 3, 4]));
                // Keep the operation transport open until the gateway closes it.
                let mut byte = [0];
                assert_eq!(wire.io.read(&mut byte).await.unwrap(), 0);
            });
            let gateway = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = gateway.local_addr().unwrap();
            let ctx = f.ctx.clone();
            let server = tokio::spawn(async move {
                let (stream, _) = gateway.accept().await.unwrap();
                serve_tcp(stream, ctx).await;
            });
            let (client, connection) = tokio_postgres::Config::new()
                .host("127.0.0.1").port(address.port())
                .user(format!("session:{}", id.to_hex())).password("login-secret")
                .dbname("app").connect(tokio_postgres::NoTls).await.unwrap();
            let connection = tokio::spawn(connection);
            let query = tokio::spawn(async move { client.batch_execute("COMMIT").await });
            forwarded_rx.await.unwrap();
            f.ctx.authority.revoke(id, crate::state::model::EndReason::AdminRevoked).unwrap();
            let error = query.await.unwrap().unwrap_err();
            assert_eq!(error.code().map(|code| code.code()), Some("NY012"));
            server.await.unwrap();
            fake.await.unwrap();
            let _ = connection.await.unwrap();
        }).await.unwrap();
    }
    fn code(actions: &Actions) -> &str {
        let Backend::ErrorResponse(fields) = &actions.client[0] else {
            panic!("expected error")
        };
        fields.iter().find(|(k, _)| *k == b'C').unwrap().1.as_str()
    }
    fn field() -> FieldDescription {
        FieldDescription {
            name: "value".into(),
            table_oid: 0,
            column_attr: 0,
            type_oid: 25,
            type_size: -1,
            type_modifier: 7,
            format: 0,
        }
    }
    fn notes_fixture() -> Fixture {
        let f = Fixture::new();
        let mut snapshot = (**f.ctx.snapshot.read().unwrap()).clone();
        snapshot.relations.push(Relation {
            schema: "public".into(),
            name: "notes".into(),
            kind: RelationKind::Table,
            columns: vec![
                Column {
                    name: "body".into(),
                    ty: ColumnType::Text,
                    nullable: true,
                    generated: None,
                    default: None,
                },
                Column {
                    name: "id".into(),
                    ty: ColumnType::Int4,
                    nullable: false,
                    generated: None,
                    default: None,
                },
            ],
            view_definition: None,
            primary_key: vec![],
        });
        *f.ctx.snapshot.write().unwrap() = Arc::new(snapshot);
        f
    }
    #[test]
    fn token_shaped_plain_literals_are_refused_in_simple_and_parse() {
        let f = notes_fixture();
        for sql in [
            "UPDATE notes SET body = 'tok_secret_a' WHERE id = 3",
            "UPDATE notes SET body = $$tok_secret_a$$ WHERE id = 3",
            "UPDATE notes SET body = $tag$tok_secret_a$tag$ WHERE id = 3",
        ] {
            assert_eq!(
                code(&f.session().frontend(Frontend::Query(sql.into()))),
                "NY001"
            );
            assert_eq!(
                code(&f.session().frontend(Frontend::Parse {
                    name: "s".into(),
                    query: sql.into(),
                    param_types: vec![],
                })),
                "NY001"
            );
        }
        for sql in [
            "UPDATE notes SET body = 'tok_notadomain_x' WHERE id = 3",
            "-- 'tok_secret_x'\nUPDATE notes SET body = 'ordinary' WHERE id = 3",
        ] {
            let out = f.session().frontend(Frontend::Query(sql.into()));
            assert!(!out.upstream.is_empty());
        }
        let token = f
            .ctx
            .codec
            .encode(
                &f.session().keys,
                &DomainName("secret".into()),
                &ColumnType::Text,
                None,
                &SqlValue::Text("tok_secret_plaintext".into()),
            )
            .unwrap();
        let SqlValue::Text(token) = token else {
            panic!("text token required")
        };
        let out = f.session().frontend(Frontend::Query(format!(
            "SELECT value FROM items WHERE value = '{token}'"
        )));
        assert!(!out.upstream.is_empty());
        let out = f.session().frontend(Frontend::Query(
            "SELECT nunya_tokenize('secret', 'tok_secret_a')".into(),
        ));
        assert!(
            out.client
                .iter()
                .any(|msg| matches!(msg, Backend::DataRow(_)))
        );
        assert!(
            !out.client
                .iter()
                .any(|msg| matches!(msg, Backend::ErrorResponse(_)))
        );
    }
    #[test]
    fn token_shaped_plain_bind_is_refused_in_text_and_binary() {
        for (format, oid) in [(0, 0), (1, 25), (1, 1042), (1, 1043)] {
            let f = notes_fixture();
            let mut session = f.session();
            let parse = session.frontend(Frontend::Parse {
                name: "s".into(),
                query: "UPDATE notes SET body = $1 WHERE id = 3".into(),
                param_types: vec![oid],
            });
            assert!(!parse.upstream.is_empty());
            session.backend(Backend::ParseComplete);
            let out = session.frontend(Frontend::Bind {
                portal: "p".into(),
                statement: "s".into(),
                param_formats: vec![format],
                params: vec![Some(b"tok_secret_a".to_vec())],
                result_formats: vec![],
            });
            assert_eq!(code(&out), "NY001");
            assert!(out.upstream.is_empty());
        }
    }
    #[test]
    fn string_scanner_skips_identifiers_comments_and_decodes_quotes() {
        let sql = "SELECT \"tok_secret_x\", 'tok_secret_a''b', $tag$tok_secret_c$tag$, $1 -- 'tok_secret_d'\n/* outer /* 'tok_secret_e' */ */";
        assert_eq!(
            string_literals("UPDATE notes SET body$tag$='plain', body='tok_secret_a$tag$'")
                .iter()
                .map(|(_, text)| text.as_str())
                .collect::<Vec<_>>(),
            vec!["plain", "tok_secret_a$tag$"]
        );
        let found = string_literals(sql);
        assert_eq!(
            found
                .iter()
                .map(|(_, text)| text.as_str())
                .collect::<Vec<_>>(),
            vec!["tok_secret_a'b", "tok_secret_c"]
        );
    }
    #[test]
    fn outbound_frame_limit_refuses_without_emitting_partial_frame() {
        let mut out = BytesMut::new();
        assert!(encode_limited(&Backend::DataRow(vec![Some(vec![1; 20])]), &mut out, 12).is_err());
        assert!(out.is_empty());
        assert!(encode_limited(&Backend::ReadyForQuery(b'I'), &mut out, 5).is_ok());
    }
    #[test]
    fn numeric_literal_splice_with_real_lowering() {
        let f = Fixture::new();
        let session = f.session();
        let mut snapshot = (**f.ctx.snapshot.read().unwrap()).clone();
        snapshot.relations[0].columns[0].ty = ColumnType::Int8;
        let policy = f.ctx.policy.read().unwrap();
        let token = f
            .ctx
            .codec
            .encode(
                &session.keys,
                &DomainName("secret".into()),
                &ColumnType::Int8,
                None,
                &SqlValue::Int8(42),
            )
            .unwrap();
        let sql = format!(
            "SELECT value FROM items WHERE value = {}",
            String::from_utf8(values::encode_text(&token).unwrap()).unwrap()
        );
        let statements = f.ctx.lowering.lower(&sql, &snapshot, &policy, &[]).unwrap();
        let plans = statements
            .iter()
            .map(|s| ir::check::check(s, &policy, &snapshot).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            rewrite_literals(
                &sql,
                &statements,
                &plans,
                &policy,
                &session.keys,
                &f.ctx.codec
            )
            .unwrap(),
            "SELECT value FROM items WHERE value = 42"
        );
    }
    #[test]
    fn response_pump_tokenizes_and_checks_shapes() {
        let f = Fixture::new();
        let mut session = f.session();
        assert_eq!(
            session
                .frontend(Frontend::Query("SELECT value FROM items".into()))
                .upstream
                .len(),
            1
        );
        let out = session.backend(Backend::RowDescription(vec![field()]));
        let Backend::RowDescription(fields) = &out.client[0] else {
            panic!("description")
        };
        assert_eq!(fields[0].type_modifier, -1);
        let out = session.backend(Backend::DataRow(vec![Some(b"O'Brien".to_vec())]));
        let Backend::DataRow(row) = &out.client[0] else {
            panic!("row")
        };
        let token = values::decode_text(&ColumnType::Text, row[0].as_ref().unwrap()).unwrap();
        assert_eq!(
            f.ctx
                .codec
                .decode(
                    &session.keys,
                    &DomainName("secret".into()),
                    &ColumnType::Text,
                    None,
                    &token
                )
                .unwrap(),
            SqlValue::Text("O'Brien".into())
        );
        assert!(
            !session
                .backend(Backend::CommandComplete("SELECT 1".into()))
                .close
        );
        assert!(!session.backend(Backend::ReadyForQuery(b'I')).close);
        assert_eq!(*f.records.lock().unwrap(), vec![(0, 0), (1, 1)]);
        session.frontend(Frontend::Query("SELECT value FROM items".into()));
        assert!(session.backend(Backend::RowDescription(vec![])).close);
    }
    #[test]
    fn row_mask_null_and_width() {
        let f = Fixture::new();
        let session = f.session();
        let policy = f.ctx.policy.read().unwrap();
        let plan = RewritePlan {
            outputs: vec![
                OutputRewrite::Pass,
                OutputRewrite::Mask {
                    placeholder: Some("hidden".into()),
                },
                OutputRewrite::Mask { placeholder: None },
            ],
            inputs: vec![],
            tokenize: None,
        };
        let (row, _) = rewrite_row(
            vec![None, Some(b"secret".to_vec()), Some(b"secret".to_vec())],
            &plan,
            &[],
            &policy,
            &session.keys,
            &f.ctx.codec,
        )
        .unwrap();
        assert_eq!(row, vec![None, Some(b"hidden".to_vec()), None]);
        assert!(rewrite_row(vec![], &plan, &[], &policy, &session.keys, &f.ctx.codec).is_err());
    }
    #[test]
    fn real_lowering_literal_splice_preserves_quoting() {
        let f = Fixture::new();
        let session = f.session();
        let domain = DomainName("secret".into());
        let ty = ColumnType::Text;
        let token = f
            .ctx
            .codec
            .encode(
                &session.keys,
                &domain,
                &ty,
                None,
                &SqlValue::Text("O'Brien".into()),
            )
            .unwrap();
        let SqlValue::Text(token) = token else {
            panic!("text")
        };
        let policy = f.ctx.policy.read().unwrap();
        let snapshot = f.ctx.snapshot.read().unwrap();
        for literal in [
            format!("'{}'", token.replace("'", "''")),
            format!("$tag${token}$tag$"),
        ] {
            let sql = format!("SELECT value FROM items WHERE value = {literal}");
            let statements = f.ctx.lowering.lower(&sql, &snapshot, &policy, &[]).unwrap();
            let plans = statements
                .iter()
                .map(|s| ir::check::check(s, &policy, &snapshot).unwrap())
                .collect::<Vec<_>>();
            assert_eq!(
                rewrite_literals(
                    &sql,
                    &statements,
                    &plans,
                    &policy,
                    &session.keys,
                    &f.ctx.codec
                )
                .unwrap(),
                "SELECT value FROM items WHERE value = 'O''Brien'"
            );
        }
    }
    #[test]
    fn bind_rewrite_text_binary_int8_and_text() {
        let f = Fixture::new();
        let session = f.session();
        let policy = f.ctx.policy.read().unwrap();
        let domain = DomainName("secret".into());
        for (ty, value) in [
            (ColumnType::Int8, SqlValue::Int8(42)),
            (ColumnType::Text, SqlValue::Text("value".into())),
        ] {
            let token = f
                .ctx
                .codec
                .encode(&session.keys, &domain, &ty, None, &value)
                .unwrap();
            let plan = RewritePlan {
                outputs: vec![],
                inputs: vec![InputRewrite::DecryptParam {
                    index: 1,
                    domain: domain.clone(),
                    ty: ty.clone(),
                }],
                tokenize: None,
            };
            for format in [0, 1] {
                let mut params = vec![Some(if format == 0 {
                    values::encode_text(&token).unwrap()
                } else {
                    values::encode_binary(&ty, &token).unwrap()
                })];
                rewrite_bind(
                    &mut params,
                    &[format],
                    &[ty.pg_oid().unwrap()],
                    &plan,
                    &policy,
                    &session.keys,
                    &f.ctx.codec,
                )
                .unwrap();
                let actual = if format == 0 {
                    values::decode_text(&ty, params[0].as_ref().unwrap()).unwrap()
                } else {
                    values::decode_binary(&ty, params[0].as_ref().unwrap()).unwrap()
                };
                assert_eq!(actual, value);
            }
        }
    }
    #[test]
    fn aborted_transaction_commit_rolls_back_and_other_sql_is_25p02() {
        let f = Fixture::new();
        let mut session = f.session();
        session.status = b'T';
        assert_eq!(
            code(&session.frontend(Frontend::Query("CREATE TABLE nope(x int)".into()))),
            "NY006"
        );
        assert!(session.aborted);
        let refused = session.frontend(Frontend::Query("SELECT value FROM items".into()));
        assert_eq!(code(&refused), "25P02");
        assert!(refused.upstream.is_empty());
        let commit = session.frontend(Frontend::Query("COMMIT".into()));
        assert!(matches!(&commit.upstream[0],Frontend::Query(s) if s=="ROLLBACK"));
        let response = session.backend(Backend::CommandComplete("ROLLBACK".into()));
        assert!(matches!(&response.client[0],Backend::CommandComplete(s) if s=="ROLLBACK"));
        session.backend(Backend::ReadyForQuery(b'I'));
        assert!(!session.aborted);
    }
    #[test]
    fn audit_and_staleness_fail_closed() {
        let f = Fixture::new();
        let mut session = f.session();
        f.fail.store(true, Ordering::Relaxed);
        let out = session.frontend(Frontend::Query("SELECT value FROM items".into()));
        assert!(out.upstream.is_empty());
        assert_eq!(code(&out), "NY009");
        f.fail.store(false, Ordering::Relaxed);
        let mut snapshot = (**f.ctx.snapshot.read().unwrap()).clone();
        snapshot.taken_at_unix_ms = 0;
        let mut policy = (**f.ctx.policy.read().unwrap()).clone();
        policy.snapshot_max_age_ms = 1;
        *f.ctx.snapshot.write().unwrap() = Arc::new(snapshot);
        *f.ctx.policy.write().unwrap() = Arc::new(policy);
        let out = session.frontend(Frontend::Query("SELECT value FROM items".into()));
        assert_eq!(code(&out), "NY008");
        assert!(out.upstream.is_empty());
    }
    #[test]
    fn final_audit_failure_closes_before_completion() {
        let f = Fixture::new();
        let mut session = f.session();
        session.frontend(Frontend::Query("SELECT value FROM items".into()));
        session.backend(Backend::RowDescription(vec![field()]));
        f.fail.store(true, Ordering::Relaxed);
        let out = session.backend(Backend::CommandComplete("SELECT 0".into()));
        assert!(out.close);
        assert_eq!(code(&out), "NY009");
    }
    fn tokenize(s: &mut Session, extended: bool) -> Actions {
        if !extended {
            return s.frontend(Frontend::Query(
                "SELECT nunya_tokenize('secret', 'private')".into(),
            ));
        }
        let parse = s.frontend(Frontend::Parse {
            name: String::new(),
            query: "SELECT nunya_tokenize('secret', $1)".into(),
            param_types: vec![25],
        });
        assert!(matches!(parse.client.first(), Some(Backend::ParseComplete)));
        let bind = s.frontend(Frontend::Bind {
            portal: String::new(),
            statement: String::new(),
            param_formats: vec![],
            params: vec![Some(b"private".to_vec())],
            result_formats: vec![],
        });
        assert!(matches!(bind.client.first(), Some(Backend::BindComplete)));
        s.frontend(Frontend::Execute {
            portal: String::new(),
            max_rows: 0,
        })
    }
    fn set_tokenize_limits(f: &Fixture, session: u32, principal: u32) {
        let mut policy = (**f.ctx.policy.read().unwrap()).clone();
        policy
            .domains
            .get_mut(&DomainName("secret".into()))
            .unwrap()
            .tokenize = TokenizeSetting::On {
            limit_per_session: session,
            limit_per_principal: principal,
            principal_window: std::time::Duration::from_secs(86400),
        };
        *f.ctx.policy.write().unwrap() = Arc::new(policy);
    }
    #[test]
    fn durable_tokenize_session_limit_survives_reconnect() {
        for extended in [false, true] {
            let f = Fixture::new();
            let mut first = f.session();
            let mut reconnect = f.session();
            assert_eq!(first.session, reconnect.session);
            assert!(
                tokenize(&mut first, extended)
                    .client
                    .iter()
                    .any(|message| matches!(message, Backend::DataRow(_)))
            );
            assert!(
                tokenize(&mut reconnect, extended)
                    .client
                    .iter()
                    .any(|message| matches!(message, Backend::DataRow(_)))
            );
            let refused = tokenize(&mut reconnect, extended);
            assert_eq!(code(&refused), "NY007");
            assert!(!refused.close);
            assert_eq!(
                f.ctx
                    .authority
                    .session_tokenizations(first.session, &DomainName("secret".into()))
                    .unwrap(),
                2
            );
        }
    }
    #[test]
    fn durable_tokenize_principal_limit_spans_access_sessions() {
        for extended in [false, true] {
            let f = Fixture::new();
            set_tokenize_limits(&f, 10, 1);
            let mut first = f.session();
            let mut second = f.second_session();
            assert_ne!(first.session, second.session);
            assert_eq!(first.principal, second.principal);
            assert!(
                tokenize(&mut first, extended)
                    .client
                    .iter()
                    .any(|message| matches!(message, Backend::DataRow(_)))
            );
            let refused = tokenize(&mut second, extended);
            assert_eq!(code(&refused), "NY007");
            assert!(!refused.close);
            assert_eq!(
                f.ctx
                    .authority
                    .session_tokenizations(second.session, &DomainName("secret".into()))
                    .unwrap(),
                0
            );
        }
    }
    #[test]
    fn revoked_tokenize_session_refuses_and_closes() {
        for extended in [false, true] {
            let f = Fixture::new();
            let mut s = f.session();
            f.ctx
                .authority
                .revoke(s.session, crate::state::model::EndReason::Requested)
                .unwrap();
            let refused = tokenize(&mut s, extended);
            assert_eq!(code(&refused), "NY011");
            assert!(refused.close);
            assert!(refused.upstream.is_empty());
            assert_eq!(
                f.ctx
                    .authority
                    .session_tokenizations(s.session, &DomainName("secret".into()))
                    .unwrap(),
                0
            );
        }
    }
    #[test]
    fn tokenize_reload_applies_next_limit_without_resetting_counters() {
        for extended in [false, true] {
            let f = Fixture::new();
            let mut s = f.session();
            assert!(
                tokenize(&mut s, extended)
                    .client
                    .iter()
                    .any(|message| matches!(message, Backend::DataRow(_)))
            );
            set_tokenize_limits(&f, 1, 20);
            assert_eq!(code(&tokenize(&mut s, extended)), "NY007");
            assert_eq!(
                f.ctx
                    .authority
                    .session_tokenizations(s.session, &DomainName("secret".into()))
                    .unwrap(),
                1
            );
        }
    }
    #[test]
    fn prepared_tokenize_uses_reloaded_limit_at_execute() {
        let f = Fixture::new();
        let mut s = f.session();
        assert!(
            tokenize(&mut s, true)
                .client
                .iter()
                .any(|message| matches!(message, Backend::DataRow(_)))
        );
        s.frontend(Frontend::Bind {
            portal: "next".into(),
            statement: String::new(),
            param_formats: vec![],
            params: vec![Some(b"private".to_vec())],
            result_formats: vec![],
        });
        set_tokenize_limits(&f, 1, 20);
        let refused = s.frontend(Frontend::Execute {
            portal: "next".into(),
            max_rows: 0,
        });
        assert_eq!(code(&refused), "NY007");
    }
    #[test]
    fn local_tokenize_limit_and_mixed_query() {
        let f = Fixture::new();
        let mut session = f.session();
        let out = session.frontend(Frontend::Query(
            "SELECT nunya_tokenize('secret', 'plaintext'); SELECT value FROM items".into(),
        ));
        assert_eq!(code(&out), "NY006");
        assert!(out.upstream.is_empty());
        for _ in 0..2 {
            let out = session.frontend(Frontend::Query(
                "SELECT nunya_tokenize('secret', 'plaintext')".into(),
            ));
            assert!(out.upstream.is_empty());
            assert!(matches!(out.client[1], Backend::DataRow(_)));
        }
        assert_eq!(
            code(&session.frontend(Frontend::Query(
                "SELECT nunya_tokenize('secret', 'plaintext')".into()
            ))),
            "NY007"
        );
    }
    trait FakeWire {
        fn receive(&mut self) -> Option<Backend>;
    }
    struct MemoryWire(VecDeque<Backend>);
    impl FakeWire for MemoryWire {
        fn receive(&mut self) -> Option<Backend> {
            self.0.pop_front()
        }
    }
    #[test]
    fn simple_query_refusal_preflights_the_whole_implicit_transaction() {
        let f = Fixture::new();
        let mut s = f.session();
        let out = s.frontend(Frontend::Query(
            "INSERT INTO items(value) VALUES (NULL); CREATE TABLE nope(x int)".into(),
        ));
        assert!(out.upstream.is_empty());
        assert!(!out.close);
        assert_eq!(code(&out), "NY006");
        assert_eq!(out.client.len(), 2);
        assert!(matches!(out.client[1], Backend::ReadyForQuery(b'I')));
        assert_eq!(f.allowed_records.load(Ordering::Relaxed), 0);
        assert!(s.active.is_empty());
    }
    #[test]
    fn local_refusal_aborts_forwarded_execute_until_sync() {
        for explicit in [false, true] {
            for completed in [false, true] {
                let f = Fixture::new();
                let mut s = f.session();
                if explicit {
                    s.frontend(Frontend::Query("BEGIN".into()));
                    s.backend(Backend::CommandComplete("BEGIN".into()));
                    s.backend(Backend::ReadyForQuery(b'T'));
                }
                let mut client = vec![];
                for request in [
                    Frontend::Parse {
                        name: String::new(),
                        query: "INSERT INTO items(value) VALUES (NULL)".into(),
                        param_types: vec![],
                    },
                    Frontend::Bind {
                        portal: "p".into(),
                        statement: String::new(),
                        param_formats: vec![],
                        params: vec![],
                        result_formats: vec![],
                    },
                    Frontend::Execute {
                        portal: "p".into(),
                        max_rows: 0,
                    },
                ] {
                    let out = s.frontend(request);
                    assert!(!out.close);
                    assert_eq!(out.upstream.len(), 1);
                    assert!(out.client.is_empty());
                }
                let mut wire = MemoryWire(VecDeque::from([
                    Backend::ParseComplete,
                    Backend::BindComplete,
                    Backend::CommandComplete("INSERT 0 1".into()),
                ]));
                if completed {
                    while let Some(response) = wire.receive() {
                        let out = s.backend(response);
                        assert!(!out.close);
                        client.extend(out.client);
                    }
                }
                let refused = s.frontend(Frontend::Parse {
                    name: "bad".into(),
                    query: "CREATE TABLE nope(x int)".into(),
                    param_types: vec![],
                });
                assert!(!refused.close);
                client.extend(refused.client);
                let sync = s.frontend(Frontend::Sync);
                assert!(matches!(sync.upstream.as_slice(), [
                    Frontend::Parse { name, query, param_types }, Frontend::Sync
                ] if name.is_empty() && query == "SELECT nunya_refused_statement(" && param_types.is_empty()));
                assert!(!s.prepared.contains_key(""));
                wire.0.extend([
                    Backend::ErrorResponse(vec![(b'C', "42601".into())]),
                    Backend::ReadyForQuery(if explicit { b'E' } else { b'I' }),
                ]);
                while let Some(response) = wire.receive() {
                    let out = s.backend(response);
                    assert!(!out.close);
                    client.extend(out.client);
                }
                assert_eq!(client.len(), 5);
                assert!(matches!(client[0], Backend::ParseComplete));
                assert!(matches!(client[1], Backend::BindComplete));
                assert!(matches!(&client[2], Backend::CommandComplete(tag) if tag == "INSERT 0 1"));
                assert!(matches!(&client[3], Backend::ErrorResponse(fields)
                    if fields.iter().any(|(key, value)| *key == b'C' && value == "NY006")));
                assert!(matches!(client[4], Backend::ReadyForQuery(status)
                    if status == if explicit { b'E' } else { b'I' }));
                if explicit {
                    let commit = s.frontend(Frontend::Query("COMMIT".into()));
                    assert!(
                        matches!(commit.upstream.as_slice(), [Frontend::Query(sql)] if sql == "ROLLBACK")
                    );
                    let done = s.backend(Backend::CommandComplete("ROLLBACK".into()));
                    assert!(
                        matches!(&done.client[0], Backend::CommandComplete(tag) if tag == "ROLLBACK")
                    );
                    s.backend(Backend::ReadyForQuery(b'I'));
                }
                // A completed segment must not arm rollback in the next segment.
                s.frontend(Frontend::Parse {
                    name: "bad_again".into(),
                    query: "CREATE TABLE nope(x int)".into(),
                    param_types: vec![],
                });
                assert!(matches!(
                    s.frontend(Frontend::Sync).upstream.as_slice(),
                    [Frontend::Sync]
                ));
            }
        }
    }
    #[test]
    fn earlier_upstream_error_is_visible_and_supersedes_queued_local_refusal() {
        for synced in [false, true] {
            let f = Fixture::new();
            let mut s = f.session();
            s.frontend(Frontend::Parse {
                name: "s".into(),
                query: "INSERT INTO items(value) VALUES (NULL)".into(),
                param_types: vec![],
            });
            s.backend(Backend::ParseComplete);
            s.frontend(Frontend::Bind {
                portal: "p".into(),
                statement: "s".into(),
                param_formats: vec![],
                params: vec![],
                result_formats: vec![],
            });
            s.backend(Backend::BindComplete);
            s.frontend(Frontend::Execute {
                portal: "p".into(),
                max_rows: 0,
            });
            assert!(
                s.frontend(Frontend::Parse {
                    name: "bad".into(),
                    query: "CREATE TABLE nope(x int)".into(),
                    param_types: vec![],
                })
                .client
                .is_empty()
            );
            if synced {
                s.frontend(Frontend::Sync);
            }
            let error = s.backend(Backend::ErrorResponse(vec![(b'C', "23505".into())]));
            assert!(!error.close);
            assert_eq!(error.client.len(), 1);
            assert_eq!(code(&error), "23505");
            if !synced {
                assert!(matches!(
                    s.frontend(Frontend::Sync).upstream.as_slice(),
                    [Frontend::Sync]
                ));
            }
            let ready = s.backend(Backend::ReadyForQuery(b'I'));
            assert!(!ready.close);
            assert!(matches!(
                ready.client.as_slice(),
                [Backend::ReadyForQuery(b'I')]
            ));
        }
    }
    #[test]
    fn pipeline_local_refusal_follows_forwarded_parse() {
        let f = Fixture::new();
        let mut s = f.session();
        let first = s.frontend(Frontend::Parse {
            name: "ok".into(),
            query: "SELECT value FROM items".into(),
            param_types: vec![],
        });
        assert_eq!(first.upstream.len(), 1);
        assert!(first.client.is_empty());
        let refused = s.frontend(Frontend::Parse {
            name: "bad".into(),
            query: "CREATE TABLE nope(x int)".into(),
            param_types: vec![],
        });
        assert!(refused.client.is_empty());
        assert!(matches!(refused.upstream.first(), Some(Frontend::Flush)));
        assert_eq!(s.frontend(Frontend::Sync).upstream.len(), 1);
        let mut wire = MemoryWire(VecDeque::from([
            Backend::ParseComplete,
            Backend::ReadyForQuery(b'I'),
        ]));
        let response = s.backend(wire.receive().unwrap());
        assert!(matches!(response.client[0], Backend::ParseComplete));
        assert_eq!(
            code(&Actions {
                client: response.client.into_iter().skip(1).collect(),
                ..Actions::default()
            }),
            "NY006"
        );
        assert!(matches!(
            s.backend(wire.receive().unwrap()).client[0],
            Backend::ReadyForQuery(b'I')
        ));
    }
    #[test]
    fn stale_execute_refuses_forwarded_and_local_before_side_effects() {
        for local in [false, true] {
            let mut f = Fixture::new();
            let now = Arc::new(std::sync::atomic::AtomicU64::new(100));
            let clock = now.clone();
            Arc::get_mut(&mut f.ctx).unwrap().clock =
                Arc::new(move || clock.load(Ordering::Relaxed));
            let mut s = f.session();
            let parse = s.frontend(Frontend::Parse {
                name: "s".into(),
                query: if local {
                    "SELECT nunya_tokenize('secret', 'private')"
                } else {
                    "SELECT value FROM items"
                }
                .into(),
                param_types: vec![],
            });
            if local {
                assert!(parse.upstream.is_empty());
            } else {
                s.backend(Backend::ParseComplete);
            }
            s.frontend(Frontend::Bind {
                portal: "p".into(),
                statement: "s".into(),
                param_formats: vec![],
                params: vec![],
                result_formats: vec![],
            });
            if !local {
                s.backend(Backend::BindComplete);
            }
            // Execute must read the current snapshot and the retained Parse policy.
            let mut snapshot = (**f.ctx.snapshot.read().unwrap()).clone();
            snapshot.taken_at_unix_ms = 200;
            *f.ctx.snapshot.write().unwrap() = Arc::new(snapshot);
            let mut policy = (**f.ctx.policy.read().unwrap()).clone();
            policy.snapshot_max_age_ms = u64::MAX;
            *f.ctx.policy.write().unwrap() = Arc::new(policy);
            now.store(301, Ordering::Relaxed);
            s.status = b'T';
            s.aborted = true;
            let out = s.frontend(Frontend::Execute {
                portal: "p".into(),
                max_rows: 0,
            });
            assert_eq!(code(&out), "NY008");
            assert!(out.upstream.is_empty());
            assert_eq!(
                f.ctx
                    .authority
                    .session_tokenizations(s.session, &DomainName("secret".into()))
                    .unwrap(),
                0
            );
            assert_eq!(f.ctx.metrics.statements_allowed.load(Ordering::Relaxed), 0);
            assert_eq!(&*f.records.lock().unwrap(), &[(0, 0)]);
            assert_eq!(f.allowed_records.load(Ordering::Relaxed), 0);
            assert!(s.skip);
        }
    }
    #[test]
    fn extended_execute_tokenizes_text_and_binary_without_describe() {
        for format in [0, 1] {
            let f = Fixture::new();
            let mut s = f.session();
            let mut snap = (**f.ctx.snapshot.read().unwrap()).clone();
            snap.relations[0].columns[0].ty = ColumnType::Int8;
            *f.ctx.snapshot.write().unwrap() = Arc::new(snap);
            s.frontend(Frontend::Parse {
                name: "s".into(),
                query: "SELECT value FROM items".into(),
                param_types: vec![],
            });
            assert!(!s.backend(Backend::ParseComplete).close);
            s.frontend(Frontend::Bind {
                portal: "p".into(),
                statement: "s".into(),
                param_formats: vec![],
                params: vec![],
                result_formats: vec![format],
            });
            assert!(!s.backend(Backend::BindComplete).close);
            assert_eq!(
                s.frontend(Frontend::Execute {
                    portal: "p".into(),
                    max_rows: 0
                })
                .upstream
                .len(),
                1
            );
            let result = s.backend(Backend::DataRow(vec![Some(if format == 0 {
                b"42".to_vec()
            } else {
                42i64.to_be_bytes().to_vec()
            })]));
            let Backend::DataRow(row) = &result.client[0] else {
                panic!("row")
            };
            let token = if format == 0 {
                values::decode_text(&ColumnType::Int8, row[0].as_ref().unwrap())
            } else {
                values::decode_binary(&ColumnType::Int8, row[0].as_ref().unwrap())
            }
            .unwrap();
            assert_eq!(
                f.ctx
                    .codec
                    .decode(
                        &s.keys,
                        &DomainName("secret".into()),
                        &ColumnType::Int8,
                        None,
                        &token
                    )
                    .unwrap(),
                SqlValue::Int8(42)
            );
            assert!(!s.backend(Backend::CommandComplete("SELECT 1".into())).close);
        }
    }
    #[test]
    fn local_parameter_tokenize_infers_types_and_exhausts_portal() {
        for cell in [
            Some(b"private".to_vec()),
            Some(b"tok_secret_a".to_vec()),
            None,
        ] {
            let f = Fixture::new();
            let mut s = f.session();
            let parse = s.frontend(Frontend::Parse {
                name: "s".into(),
                query: "SELECT nunya_tokenize('secret', $1)".into(),
                param_types: vec![0],
            });
            assert!(matches!(parse.client.first(), Some(Backend::ParseComplete)));
            let describe = s.frontend(Frontend::Describe {
                kind: b'S',
                name: "s".into(),
            });
            assert!(
                matches!(&describe.client[0],Backend::ParameterDescription(types) if types==&vec![25])
            );
            let bind = s.frontend(Frontend::Bind {
                portal: "p".into(),
                statement: "s".into(),
                param_formats: vec![1],
                params: vec![cell.clone()],
                result_formats: vec![1],
            });
            assert!(matches!(bind.client.first(), Some(Backend::BindComplete)));
            let execute = s.frontend(Frontend::Execute {
                portal: "p".into(),
                max_rows: 0,
            });
            assert!(execute.upstream.is_empty());
            let Backend::DataRow(row) = &execute.client[0] else {
                panic!("row")
            };
            assert_eq!(row[0].is_none(), cell.is_none());
            let again = s.frontend(Frontend::Execute {
                portal: "p".into(),
                max_rows: 0,
            });
            assert!(matches!(&again.client[0],Backend::CommandComplete(tag) if tag=="SELECT 0"));
            assert_eq!(
                f.ctx
                    .authority
                    .session_tokenizations(s.session, &DomainName("secret".into()))
                    .unwrap(),
                1
            );
        }
    }
    #[test]
    fn prepared_commit_after_local_abort_executes_real_rollback() {
        let f = Fixture::new();
        let mut s = f.session();
        s.status = b'T';
        s.frontend(Frontend::Parse {
            name: "commit".into(),
            query: "COMMIT".into(),
            param_types: vec![],
        });
        s.backend(Backend::ParseComplete);
        s.frontend(Frontend::Bind {
            portal: "commit".into(),
            statement: "commit".into(),
            param_formats: vec![],
            params: vec![],
            result_formats: vec![],
        });
        s.backend(Backend::BindComplete);
        s.frontend(Frontend::Parse {
            name: "bad".into(),
            query: "CREATE TABLE nope(x int)".into(),
            param_types: vec![],
        });
        s.frontend(Frontend::Sync);
        s.backend(Backend::ReadyForQuery(b'T'));
        let execute = s.frontend(Frontend::Execute {
            portal: "commit".into(),
            max_rows: 0,
        });
        assert!(matches!(&execute.upstream[0],Frontend::Parse{query,..} if query=="ROLLBACK"));
        assert!(s.backend(Backend::ParseComplete).client.is_empty());
        assert!(s.backend(Backend::BindComplete).client.is_empty());
        assert!(
            matches!(&s.backend(Backend::CommandComplete("ROLLBACK".into())).client[0],Backend::CommandComplete(tag) if tag=="ROLLBACK")
        );
    }
    #[test]
    fn pending_begin_then_local_refusal_marks_sync_aborted() {
        let f = Fixture::new();
        let mut s = f.session();
        s.frontend(Frontend::Parse {
            name: "b".into(),
            query: "BEGIN".into(),
            param_types: vec![],
        });
        s.backend(Backend::ParseComplete);
        s.frontend(Frontend::Bind {
            portal: "b".into(),
            statement: "b".into(),
            param_formats: vec![],
            params: vec![],
            result_formats: vec![],
        });
        s.backend(Backend::BindComplete);
        s.frontend(Frontend::Execute {
            portal: "b".into(),
            max_rows: 0,
        });
        assert!(
            s.frontend(Frontend::Parse {
                name: "bad".into(),
                query: "CREATE TABLE nope(x int)".into(),
                param_types: vec![]
            })
            .client
            .is_empty()
        );
        s.frontend(Frontend::Sync);
        let response = s.backend(Backend::CommandComplete("BEGIN".into()));
        assert_eq!(response.client.len(), 2);
        assert!(
            s.backend(Backend::ErrorResponse(vec![(b'C', "42601".into())]))
                .client
                .is_empty()
        );
        assert!(matches!(
            s.backend(Backend::ReadyForQuery(b'E')).client[0],
            Backend::ReadyForQuery(b'E')
        ));
        let invalid = s.frontend(Frontend::Parse {
            name: "bad".into(),
            query: "SELECT (".into(),
            param_types: vec![],
        });
        assert_eq!(code(&invalid), "25P02");
    }
    #[test]
    fn upstream_error_preserves_later_sync_segment() {
        let f = Fixture::new();
        let mut s = f.session();
        for name in ["first", "second"] {
            s.frontend(Frontend::Parse {
                name: name.into(),
                query: "SELECT value FROM items".into(),
                param_types: vec![],
            });
            s.frontend(Frontend::Sync);
        }
        assert!(
            !s.backend(Backend::ErrorResponse(vec![(b'C', "42601".into())]))
                .close
        );
        assert!(!s.backend(Backend::ReadyForQuery(b'I')).close);
        assert!(!s.backend(Backend::ParseComplete).close);
        assert!(!s.backend(Backend::ReadyForQuery(b'I')).close);
        assert!(!s.skip);
        assert!(
            s.backend(Backend::ErrorResponse(vec![(b'C', "42601".into())]))
                .close
        );
    }
    #[test]
    fn sync_barrier_defers_next_pipeline_until_abort_is_known() {
        let f = Fixture::new();
        let mut s = f.session();
        s.frontend(Frontend::Parse {
            name: "b".into(),
            query: "BEGIN".into(),
            param_types: vec![],
        });
        s.backend(Backend::ParseComplete);
        s.frontend(Frontend::Bind {
            portal: "b".into(),
            statement: "b".into(),
            param_formats: vec![],
            params: vec![],
            result_formats: vec![],
        });
        s.backend(Backend::BindComplete);
        s.frontend(Frontend::Execute {
            portal: "b".into(),
            max_rows: 0,
        });
        s.frontend(Frontend::Parse {
            name: "bad".into(),
            query: "CREATE TABLE nope(x int)".into(),
            param_types: vec![],
        });
        s.frontend(Frontend::Sync);
        for request in [
            Frontend::Parse {
                name: "q".into(),
                query: "SELECT value FROM items".into(),
                param_types: vec![],
            },
            Frontend::Bind {
                portal: "q".into(),
                statement: "q".into(),
                param_formats: vec![],
                params: vec![],
                result_formats: vec![],
            },
            Frontend::Execute {
                portal: "q".into(),
                max_rows: 0,
            },
        ] {
            assert!(s.frontend(request).upstream.is_empty());
        }
        s.backend(Backend::CommandComplete("BEGIN".into()));
        assert!(
            s.backend(Backend::ErrorResponse(vec![(b'C', "42601".into())]))
                .client
                .is_empty()
        );
        let ready = s.backend(Backend::ReadyForQuery(b'E'));
        assert!(ready.upstream.is_empty());
        assert!(matches!(ready.client[0], Backend::ReadyForQuery(b'E')));
        assert_eq!(
            code(&Actions {
                client: ready.client.into_iter().skip(1).collect(),
                ..Actions::default()
            }),
            "25P02"
        );
    }
    #[test]
    fn sync_barrier_resumes_pipelined_simple_queries_one_at_a_time() {
        let f = Fixture::new();
        let mut s = f.session();
        s.frontend(Frontend::Sync);
        assert!(
            s.frontend(Frontend::Query("SELECT value FROM items".into()))
                .upstream
                .is_empty()
        );
        assert!(
            s.frontend(Frontend::Query("SELECT value FROM items".into()))
                .upstream
                .is_empty()
        );
        let ready = s.backend(Backend::ReadyForQuery(b'I'));
        assert_eq!(ready.upstream.len(), 1);
        assert!(!ready.close);
        s.backend(Backend::RowDescription(vec![field()]));
        s.backend(Backend::CommandComplete("SELECT 0".into()));
        let ready = s.backend(Backend::ReadyForQuery(b'I'));
        assert_eq!(ready.upstream.len(), 1);
        assert!(!ready.close);
        s.backend(Backend::RowDescription(vec![field()]));
        s.backend(Backend::CommandComplete("SELECT 0".into()));
        assert!(!s.backend(Backend::ReadyForQuery(b'I')).close);
    }
    #[test]
    fn commit_prepared_while_aborted_stays_commit_after_recovery() {
        let f = Fixture::new();
        let mut s = f.session();
        s.status = b'T';
        s.aborted = true;
        let parse = s.frontend(Frontend::Parse {
            name: "c".into(),
            query: "COMMIT".into(),
            param_types: vec![],
        });
        assert!(matches!(&parse.upstream[0],Frontend::Parse{query,..} if query=="COMMIT"));
        s.backend(Backend::ParseComplete);
        s.frontend(Frontend::Bind {
            portal: "c".into(),
            statement: "c".into(),
            param_formats: vec![],
            params: vec![],
            result_formats: vec![],
        });
        s.backend(Backend::BindComplete);
        s.frontend(Frontend::Query("ROLLBACK TO SAVEPOINT earlier".into()));
        s.backend(Backend::CommandComplete("ROLLBACK".into()));
        s.backend(Backend::ReadyForQuery(b'T'));
        assert!(!s.aborted);
        let execute = s.frontend(Frontend::Execute {
            portal: "c".into(),
            max_rows: 0,
        });
        assert_eq!(execute.upstream.len(), 1);
        assert!(matches!(&execute.upstream[0],Frontend::Execute{portal,..} if portal=="c"));
    }
}
