//! Fail-closed simple-query session state and downstream socket entry point.
use super::{
    auth::{self, ScramServer, ScramStep, ScramVerifier},
    codec::{self, Backend, FieldDescription, Frontend, StartupMessage},
    context::GatewayContext,
    redact::{nunya_error, nunya_refusal, redact_error},
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
    policy::{DomainName, Policy, TokenizeSetting, fence::{ReleaseSlot, tightens}},
    readlog::{LifecycleActor, LifecycleKind, LifecycleRecord, Outcome, ReadLogRecord, redact_tokenize_args, statement_fingerprint},
    state::{
        authority::{AdmitError, Operation, TokenizeError, TokenizeLimits},
        model::Mode,
    },
    token::{PrincipalId, ScopeKeys, SessionId, TokenCodec, keys::derive_scope_v2},
    value::{ColumnType, SqlValue},
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
fn tokenize_refusal(error: TokenizeError) -> NyCode {
    match error {
        TokenizeError::LimitReached => NyCode::TokenizeLimit,
        TokenizeError::Ended => NyCode::SessionEnded,
        TokenizeError::Store => NyCode::ReadLogUnavailable,
    }
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
    epoch: u64,
    described: bool,
}
impl Active {
    fn log_refusal(&mut self, ctx: &GatewayContext, code: NyCode) -> Result<(), Refusal> {
        // A release check may already have recorded this refusal.
        if matches!(self.record.outcome, Outcome::Refused(current) if current == code) {
            return Ok(());
        }
        self.record.outcome = Outcome::Refused(code);
        ctx.readlog.write(&self.record)
    }
}
fn check_serving(ctx: &GatewayContext) -> Result<(), Refusal> {
    if ctx.serving_gate.as_ref().is_some_and(|g| !g.allow()) || !ctx.serving_state().available() {
        return Err(failure(NyCode::ServingUnavailable));
    }
    Ok(())
}
fn check_release(ctx: &GatewayContext, release: &ReleaseSlot, active: &mut Active) -> Result<(), Refusal> {
    if let Err(e) = check_serving(ctx) {
        active.log_refusal(ctx, e.code)?;
        return Err(e);
    }
    let epoch = release.begin(&ctx.fence);
    if epoch != active.epoch {
        let current = ctx.policy.read().map_err(|_| protocol())?;
        if tightens(&active.policy, &current) {
            active.log_refusal(ctx, NyCode::SchemaStale)?;
            return Err(failure(NyCode::SchemaStale));
        }
        active.epoch = epoch;
        release.advance(epoch);
    }
    Ok(())
}
fn is_statement_refusal(code: NyCode) -> bool {
    matches!(
        code,
        NyCode::NotAnalysable | NyCode::BlindOperation | NyCode::MaskedExpression | NyCode::SchemaStale
    )
}

/// Socket-independent simple and extended protocol state with ordered responses.
pub struct Session {
    ctx: Arc<GatewayContext>,
    release: Arc<ReleaseSlot>,
    pinned_path: Vec<String>,
    snapshot_cache: Option<(Arc<crate::schema::Snapshot>, Arc<crate::schema::Snapshot>)>,
    keys: ScopeKeys,
    session: SessionId,
    principal: PrincipalId,
    credential: Option<crate::state::model::CredentialId>,
    scope: crate::token::ScopeId,
    connection: Option<u64>,
    active: VecDeque<Active>,
    waiting: bool,
    // Keep matching upstream replies against their queue until ReadyForQuery.
    discard_result: bool,
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
    // A CommandComplete does not decide an implicit transaction before its RFQ.
    segment_forwarded_write: bool,
    segment_known_failure: bool,
    // A transaction ended before an error can abort the current transaction.
    segment_unresolved_before_failure: bool,
    rollback_on_sync: bool,
    last_row_refresh_ms: Option<u64>,
    statement_revision: Option<String>,
    end_requested: Option<crate::state::registry::EndReason>,
}
impl Session {
    /// Construct an authenticated session with independently derived scope keys.
    pub fn new(
        ctx: Arc<GatewayContext>,
        keys: ScopeKeys,
        session: SessionId,
        principal: PrincipalId,
        credential: Option<crate::state::model::CredentialId>,
        scope: crate::token::ScopeId,
    ) -> Self {
        let pinned_path = ctx.snapshot.read().unwrap_or_else(std::sync::PoisonError::into_inner).search_path.clone();
        let release = ctx.fence.register();
        Self {
            release,
            pinned_path,
            snapshot_cache: None,
            ctx,
            keys,
            session,
            principal,
            credential,
            scope,
            connection: None,
            active: VecDeque::new(),
            waiting: false,
            discard_result: false,
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
            segment_forwarded_write: false,
            segment_known_failure: false,
            segment_unresolved_before_failure: false,
            rollback_on_sync: false,
            last_row_refresh_ms: None,
            statement_revision: None,
            end_requested: None,
        }
    }
    /// Finish handing the current release batch to the agent socket.
    pub(crate) fn release_done(&self) {
        self.release.end();
    }
    #[cfg(test)]
    fn snapshot(&mut self) -> Result<Arc<crate::schema::Snapshot>, Refusal> {
        let current = self.ctx.snapshot.read().map_err(|_| protocol())?.clone();
        self.pin_snapshot(current)
    }
    fn pin_snapshot(&mut self, current: Arc<crate::schema::Snapshot>) -> Result<Arc<crate::schema::Snapshot>, Refusal> {
        let captured = current.captured_schemas.as_ref().unwrap_or(&current.search_path);
        if !self.pinned_path.iter().all(|schema| captured.contains(schema)) {
            return Err(failure(NyCode::SessionEnded));
        }
        if current.search_path == self.pinned_path { return Ok(current); }
        if let Some((source, pinned)) = &self.snapshot_cache
            && Arc::ptr_eq(source, &current)
        {
            return Ok(pinned.clone());
        }
        let mut pinned = (*current).clone();
        pinned.search_path = self.pinned_path.clone();
        let pinned = Arc::new(pinned);
        self.snapshot_cache = Some((current, pinned.clone()));
        Ok(pinned)
    }
    /// Attach the registered connection identifier.
    pub fn set_connection(&mut self, id: u64) {
        self.connection = Some(id);
    }
    /// Whether a forwarded write or commit still awaits completion.
    pub(crate) fn write_in_flight(&self) -> bool {
        let indeterminate =
            |active: &Active| writes(&active.kind) || matches!(active.kind, StatementKind::Commit);
        (self.segment_forwarded_write
            && (!self.segment_known_failure || self.segment_unresolved_before_failure))
            || self.active.iter().any(indeterminate)
            || self.pending.iter().any(|pending| match pending {
                Pending::Execute(active, _) => indeterminate(active),
                Pending::Local(_)
                | Pending::Ack(..)
                | Pending::Hidden(_)
                | Pending::RefusalError
                | Pending::Describe(_, _, _)
                | Pending::Sync => false,
            })
    }
    fn audit_unknown(&self, reason: &'static str) {
        if self.ctx.readlog.write_lifecycle(&LifecycleRecord { policy: None,
            kind: LifecycleKind::WriteOutcomeUnknown,
            at_ms: (self.ctx.clock)(),
            principal: self.principal.to_hex(),
            session: Some(self.session.to_hex()),
            route: Some(self.ctx.route.as_str().into()),
            connection: self.connection,
            actor: LifecycleActor::System,
            reason,
        }).is_err() {
            eprintln!("audit: write outcome unknown record unavailable");
        }
    }
    fn upstream_lost(&self, error: Refusal, write_before_batch: bool) -> Refusal {
        if error.reason == super::upstream::SEND_BEFORE_ANY_BYTE && !write_before_batch {
            return error;
        }
        if self.write_in_flight() {
            self.audit_unknown("upstream_lost");
            failure(NyCode::WriteOutcomeUnknown)
        } else {
            error
        }
    }
    /// Audit and close an ended connection, preserving uncertain write outcomes.
    pub(crate) fn end(&mut self, reason: crate::state::registry::EndReason) -> Actions {
        let code = if self.write_in_flight() {
            NyCode::WriteOutcomeUnknown
        } else {
            NyCode::SessionEnded
        };
        self.ctx.metrics.refused(code);
        let record = LifecycleRecord { policy: None,
            kind: LifecycleKind::ConnectionEnded,
            at_ms: (self.ctx.clock)(),
            principal: self.principal.to_hex(),
            session: Some(self.session.to_hex()),
            route: Some(self.ctx.route.as_str().into()),
            connection: self.connection,
            actor: LifecycleActor::System,
            reason: match reason {
                crate::state::registry::EndReason::Revoked => "revoked",
                crate::state::registry::EndReason::Expired => "expired",
            },
        };
        let failed = self.ctx.readlog.write_lifecycle(&record).is_err();
        if code == NyCode::WriteOutcomeUnknown {
            self.audit_unknown(record.reason);
        }
        Actions {
            client: vec![Backend::ErrorResponse(nunya_error(if failed && code == NyCode::SessionEnded {
                NyCode::ReadLogUnavailable
            } else { code }))],
            close: true,
            ..Actions::default()
        }
    }
    fn admit(&self) -> Result<Mode, Refusal> {
        self.ctx
            .serving_state()
            .admit(self.session, Operation::Read)
            .map(|admitted| admitted.mode)
            .map_err(|error| {
                failure(match error {
                    AdmitError::Unknown | AdmitError::Ended => NyCode::SessionEnded,
                    AdmitError::ReadOnly => NyCode::StatementNotAllowed,
                    #[cfg(feature = "kubernetes")]
                    AdmitError::Unavailable => NyCode::ServingUnavailable,
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
            principal: self.principal,
            session: self.session,
            scope: self.scope,
            connection: self.connection,
            credential_id: self.credential,
            route: self.ctx.route.as_str().into(),
            policy_revision: self.statement_revision.clone(),
            config_revision: None,
            statement_fingerprint: statement_fingerprint(&statement),
            statement,
            outcome,
            rows: 0,
            tokens_released: vec![],
        }
    }
    fn refuse(&mut self, sql: &str, error: &Refusal, ready: bool) -> Actions {
        let code = error.code;
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
            client: vec![Backend::ErrorResponse(if failed {
                nunya_error(NyCode::ReadLogUnavailable)
            } else {
                nunya_refusal(error)
            })],
            close: failed || code == NyCode::SessionEnded,
            ..Actions::default()
        };
        if ready && !out.close {
            out.client.push(self.ready());
        }
        out
    }
    fn record_tokenization(&self, policy: &Policy, domain: &DomainName) -> Result<(), Refusal> {
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
            .serving_state()
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
            .map_err(|error| failure(tokenize_refusal(error)))
    }
    /// Consume an agent request without performing network I/O.
    pub fn frontend(&mut self, msg: Frontend) -> Actions {
        if matches!(msg, Frontend::Terminate) {
            return Actions {
                close: true,
                ..Actions::default()
            };
        }
        if let Err(error) = check_serving(&self.ctx) {
            return Actions {
                client: vec![Backend::ErrorResponse(nunya_error(error.code))],
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
        self.statement_revision = None;
        let result = (|| {
            // Admit at processing time so deferred queries use current authority.
            let mode = self.admit()?;
            let epoch = self.ctx.fence.epoch();
            let (policy, revision, snapshot) = self.ctx.policy_and_snapshot()?;
            self.statement_revision = revision;
            let snapshot = self.pin_snapshot(snapshot)?;
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
                if plans.first().is_none_or(|plan| plan.tokenize.is_none()) {
                    self.ctx
                        .readlog
                        .write(&self.record(text, Outcome::Allowed))?;
                }
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
                self.record_tokenization(&policy, domain)?;
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
                    epoch,
                    plan,
                    kind: statement.kind,
                    record: self.record(text, Outcome::Allowed),
                    policy: policy.clone(),
                    described: false,
                });
            }
            self.segment_forwarded_write |= self.active.iter().any(|a| {
                writes(&a.kind) || matches!(a.kind, StatementKind::Commit)
            });
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
                let mut out = self.refuse(sql, &failure(NyCode::StatementNotAllowed), true);
                if !out.close {
                    out.client[0] = Backend::ErrorResponse(redact_error(&[
                        (b'S', "ERROR".into()), (b'C', "25P02".into()),
                    ]));
                }
                out
            }
            Err(e) => self.refuse(sql, &e, true),
        }
    }
    /// Consume an upstream response, sanitizing every data-bearing result.
    pub fn backend(&mut self, msg: Backend) -> Actions {
        if self.end_requested.is_some() {
            return Actions::default();
        }
        if matches!(msg, Backend::DataRow(_)) && !self.discard_result {
            let now = (self.ctx.clock)();
            if self.last_row_refresh_ms.is_none_or(|last| now.saturating_sub(last) >= 1000) {
                if self.ctx.serving_state().admit(self.session, Operation::Read).is_err() {
                    use crate::state::{model::EndReason as StoredEnd, registry::EndReason};
                    self.end_requested = Some(match self.ctx.serving_state().session(self.session)
                        .and_then(|row| row.revoke_reason) {
                        Some(StoredEnd::Expired | StoredEnd::IdleTimeout) => EndReason::Expired,
                        _ => EndReason::Revoked,
                    });
                    // The socket loop must cancel upstream and audit before its final error.
                    return Actions::default();
                }
                self.last_row_refresh_ms = Some(now);
            }
        }
        if !self.pending.is_empty() || (self.discard_result && !self.waiting) {
            return self.extended_backend(msg);
        }
        let completed = matches!(msg, Backend::CommandComplete(_));
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
            Err(error) => self.simple_result_error(error.code, completed),
        }
    }
    fn simple_result_error(&mut self, code: NyCode, completed: bool) -> Actions {
        if self.discard_result
            || !is_statement_refusal(code)
            || (self.status != b'T' && self.segment_forwarded_write)
            || self.segment_unresolved_before_failure
            || !self.active.iter().all(|a| is_read(&a.kind))
        {
            return self.result_error(code, true);
        }
        let Some(active) = self.active.front_mut() else {
            return self.result_error(NyCode::ProtocolNotAllowed, true);
        };
        if let Err(error) = active.log_refusal(&self.ctx, code) {
            return self.result_error(error.code, true);
        }
        if completed {
            self.active.pop_front();
        }
        self.start_result_discard(code)
    }
    fn result_error(&self, code: NyCode, close: bool) -> Actions {
        self.ctx.metrics.refused(code);
        Actions {
            client: vec![Backend::ErrorResponse(nunya_error(code))],
            close,
            ..Actions::default()
        }
    }
    fn start_result_discard(&mut self, code: NyCode) -> Actions {
        self.discard_result = true;
        self.aborted |= self.status == b'T';
        self.result_error(code, false)
    }
    fn finish_segment(&mut self, status: u8) -> Backend {
        self.status = status;
        self.aborted = status == b'E'
            || (status == b'T' && (self.aborted || self.discard_result));
        self.discard_result = false;
        self.segment_forwarded_write = false;
        self.segment_known_failure = false;
        self.segment_unresolved_before_failure = false;
        self.ready()
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
                Ok(vec![self.finish_segment(status)])
            }
            Backend::RowDescription(mut fields) => {
                let active = self.active.front_mut().ok_or_else(protocol)?;
                if active.described {
                    return Err(protocol());
                }
                active.described = true;
                if self.discard_result {
                    return Ok(vec![]);
                }
                if fields.iter().any(|f| f.format != 0) {
                    return Err(failure(NyCode::NotAnalysable));
                }
                extended::rewrite_fields(&mut fields, &active.plan)?;
                Ok(vec![Backend::RowDescription(fields)])
            }
            Backend::DataRow(row) => {
                let active = self.active.front_mut().ok_or_else(protocol)?;
                if !active.described {
                    return Err(protocol());
                }
                if self.discard_result {
                    return Ok(vec![]);
                }
                if row.len() != active.plan.outputs.len() {
                    return Err(failure(NyCode::NotAnalysable));
                }
                check_release(&self.ctx, &self.release, active)?;
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
                if self.discard_result {
                    self.active.pop_front().ok_or_else(protocol)?;
                    return Ok(vec![]);
                }
                let active = self.active.front_mut().ok_or_else(protocol)?;
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
                let kind = &active.kind;
                if self.segment_forwarded_write
                    && matches!(kind, StatementKind::Commit | StatementKind::Rollback)
                {
                    self.segment_unresolved_before_failure = true;
                }
                let commit = self.aborted && matches!(kind, StatementKind::Commit);
                if matches!(kind, StatementKind::Begin) {
                    self.status = b'T';
                }
                if recovery(kind) {
                    self.aborted = false;
                }
                self.active.pop_front();
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
                if !self.waiting || (self.discard_result && self.segment_known_failure) {
                    return Err(protocol());
                }
                // Upstream::recv already excludes FATAL/PANIC. An ordinary error
                // aborts all writes in the current transaction, including at commit.
                self.segment_known_failure = true;
                // An implicit commit can fail after the last CommandComplete;
                // the Query still owns responses until its ReadyForQuery.
                self.empty_pending = false;
                // An upstream error terminates this query and skips its remaining statements.
                if self.discard_result {
                    self.active.clear();
                    return Ok(vec![]);
                }
                if let Some(active) = self.active.pop_front() {
                    self.ctx.readlog.write(&active.record)?;
                }
                self.active.clear();
                Ok(vec![Backend::ErrorResponse(redact_error(&fields))])
            }
            Backend::NoticeResponse(fields) => Ok(if self.discard_result {
                vec![]
            } else {
                vec![Backend::NoticeResponse(redact_error(&fields))]
            }),
            Backend::ParameterStatus(name, value) => Ok(if !self.discard_result && allowed_status(&name) {
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
fn is_read(kind: &StatementKind) -> bool {
    matches!(kind, StatementKind::Select | StatementKind::JsonSelect { .. })
}
fn writes(kind: &StatementKind) -> bool {
    match kind {
        StatementKind::Insert | StatementKind::Update | StatementKind::Delete => true,
        StatementKind::JsonSelect { .. }
        | StatementKind::Select
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
        UseKind::JsonLeaf { kind } => match kind.as_ref() {
            UseKind::Equality { other } => Some(other),
            _ => None,
        },
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
        if let Some(canonical) = statement.canonical_sql {
            replacements.push((statement.source_span.clone(), canonical.to_owned()));
        }
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
                    let decoded = codec.decode_checked(keys, domain, ty, range, &value, &policy.retired_domains)?;
                    replacements.push((span.clone(), ir::rewrite::literal_for(&decoded)?));
                }
                InputRewrite::DecryptParam { .. } => {}
                InputRewrite::ReplaceSpan { span, text } => replacements.push((span.clone(), text.clone())),
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
fn validate_json_fields(fields: &[FieldDescription], plan: &RewritePlan) -> Result<(), Refusal> {
    for (field, output) in fields.iter().zip(&plan.outputs) {
        if let OutputRewrite::JsonExtract { base, text, .. } = output {
            let matches_oid = if *text {
                // PGAdapter describes text expressions as varchar. Both formats
                // carry the same string bytes used by the JSON extraction codec.
                matches!(field.type_oid, 25 | 1043)
            } else {
                base.pg_oid() == Some(field.type_oid)
            };
            if !matches!(base, ColumnType::Json | ColumnType::Jsonb) || !matches_oid {
                return Err(failure(NyCode::NotAnalysable));
            }
        }
        if let OutputRewrite::JsonFilter { ty, .. } = output
            && (!matches!(ty, ColumnType::Json | ColumnType::Jsonb)
                || ty.pg_oid() != Some(field.type_oid))
        {
            return Err(failure(NyCode::NotAnalysable));
        }
    }
    Ok(())
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
            (Some(bytes), OutputRewrite::JsonFilter { filter, ty }) => {
                use crate::json::{codec::JsonFormat, filter::{JsonInput, JsonOutput}};
                let format = match (ty, format) {
                    (ColumnType::Json, 0 | 1) => JsonFormat::PgJsonText,
                    (ColumnType::Jsonb, 0) => JsonFormat::PgJsonbText,
                    (ColumnType::Jsonb, 1) => JsonFormat::PgJsonbBinary,
                    _ => return Err(failure(NyCode::NotAnalysable)),
                };
                match crate::json::filter::apply_collect(
                    &filter.rules, filter.mode, keys, codec, &policy.domains,
                    &crate::json::JsonLimits::default(), JsonInput { format, bytes: &bytes }, &mut tokens,
                )? {
                    JsonOutput::Document(bytes) => Some(bytes),
                    JsonOutput::SqlNull => None,
                }
            }
            (Some(bytes), OutputRewrite::JsonExtract { filter, base, text, keep_empty }) => {
                use crate::json::{codec::{self, JsonFormat}, filter::{JsonInput, JsonOutput}, JsonLimits, JsonValue};
                let wire = match (base, *text, format) {
                    (ColumnType::Json, _, 0 | 1) => JsonFormat::PgJsonText,
                    (ColumnType::Jsonb, true, 0 | 1) | (ColumnType::Jsonb, false, 0) => JsonFormat::PgJsonbText,
                    (ColumnType::Jsonb, false, 1) => JsonFormat::PgJsonbBinary,
                    _ => return Err(failure(NyCode::NotAnalysable)),
                };
                match crate::json::filter::extraction_output(crate::json::filter::apply_collect(&filter.rules, filter.mode, keys, codec,
                    &policy.domains, &JsonLimits::default(), JsonInput { format: wire, bytes: &bytes }, &mut tokens)?, filter.mode, *keep_empty, wire)? {
                    JsonOutput::SqlNull => None,
                    JsonOutput::Document(bytes) if !text => Some(bytes),
                    JsonOutput::Document(bytes) => {
                        let tree = codec::decode(wire, &bytes, &JsonLimits::default())?;
                        match &tree {
                            JsonValue::Null => None,
                            JsonValue::String(s) => Some(s.as_bytes().to_vec()),
                            JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::Array(_) | JsonValue::Object(_) => Some(bytes),
                        }
                    }
                }
            }
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
                    let value = codec.decode_checked(keys, domain, ty, range, &token, &policy.retired_domains)?;
                    params[i] = Some(if format == 0 {
                        values::encode_text(&value)?
                    } else {
                        values::encode_binary(ty, &value)?
                    });
                }
            }
            InputRewrite::DecryptLiteral { .. } | InputRewrite::ReplaceSpan { .. } => {}
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
async fn send_or_end(
    agent: &mut Agent,
    end: &mut crate::state::registry::EndSignal,
    messages: &[Backend],
) -> Result<Option<crate::state::registry::EndReason>, Refusal> {
    tokio::select! {
        biased;
        reason = end.ended() => Ok(Some(reason)),
        result = agent.send(messages) => result.map(|()| None),
    }
}
const FINAL_SEND_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);
const DRAIN_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(1500);
const DRAIN_BYTE_CAP: usize = 1024 * 1024;

async fn linger_close(io: &mut dyn Transport) {
    // TLS shutdown sends close_notify before shutting down the TCP write half.
    let _ = tokio::time::timeout(FINAL_SEND_TIMEOUT, io.shutdown()).await;
    let _ = tokio::time::timeout(DRAIN_TIMEOUT, async {
        let mut buffer = [0; 4096];
        let mut remaining = DRAIN_BYTE_CAP;
        while remaining > 0 {
            let size = buffer.len().min(remaining);
            match io.read(&mut buffer[..size]).await {
                Ok(0) | Err(_) => break,
                Ok(count) => remaining -= count,
            }
        }
    }).await;
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
    serve_tcp_inner(tcp, ctx, None).await;
}
/// Serve an admitted connection, releasing capacity before the bounded close drain.
pub(crate) async fn serve_tcp_admitted(
    tcp: tokio::net::TcpStream,
    ctx: Arc<GatewayContext>,
    permit: crate::config_reload::ConnectionGuard,
) {
    serve_tcp_inner(tcp, ctx, Some(permit)).await;
}
async fn serve_tcp_inner(
    tcp: tokio::net::TcpStream,
    ctx: Arc<GatewayContext>,
    permit: Option<crate::config_reload::ConnectionGuard>,
) {
    ctx.metrics.connections.fetch_add(1, Ordering::Relaxed);
    let mut agent = Agent {
        io: Box::new(tcp),
        buffer: BytesMut::new(),
        ctx: ctx.clone(),
    };
    if let Err(error) = serve(&mut agent, ctx).await {
        let _ = tokio::time::timeout(
            FINAL_SEND_TIMEOUT,
            agent.send(&[Backend::ErrorResponse(nunya_error(error.code))]),
        ).await;
    }
    drop(permit);
    linger_close(agent.io.as_mut()).await;
}
async fn end_connection(
    agent: &mut Agent,
    upstream: &mut Upstream,
    session: &mut Session,
    reason: crate::state::registry::EndReason,
) -> Result<(), Refusal> {
    if session.waiting || !session.pending.is_empty() || session.write_in_flight() {
        let (pid, secret) = upstream.backend_key();
        let _ = send_cancel(&session.ctx.upstream, pid, &secret).await;
    }
    let actions = session.end(reason);
    // A failed or timed-out best-effort final send just closes the connection.
    let _ = tokio::time::timeout(FINAL_SEND_TIMEOUT, agent.send(&actions.client)).await;
    Ok(())
}
async fn serve(agent: &mut Agent, ctx: Arc<GatewayContext>) -> Result<(), Refusal> {
    check_serving(&ctx)?;
    let mut tls = false;
    let user = loop {
        match agent.startup().await? {
            StartupMessage::SslRequest => {
                if tls {
                    return Err(protocol());
                }
                let current = ctx.tls.read().map_err(|_| protocol())?.clone();
                if let Some(config) = current {
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
                check_serving(&ctx)?;
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
    if !ctx.serving_state().available() { return Err(failure(NyCode::ServingUnavailable)); }
    let login = auth::parse_session_user(&user).and_then(|id| {
        ctx.serving_state()
            .session_login(id, &ctx.route)
            .map(|login| (id, login))
    });
    let candidate = login
        .as_ref()
        .map(|(_, login)| login.verifiers.iter().map(|(_, v)| v.clone().0).collect());
    let verifiers = match candidate {
        Some(v) => v,
        None => vec![ScramVerifier {
            salt: auth::dummy_scram_salt(&ctx.root_key, &user),
            iterations: auth::SESSION_SCRAM_ITERATIONS,
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
    let ScramStep::Done { server_final, matched } = scram
        .handle_final(&final_msg)
        .map_err(|_| failure(NyCode::TokenNotValid))?
    else {
        return Err(protocol());
    };
    let (id, login) = login.ok_or_else(|| failure(NyCode::TokenNotValid))?;
    let credential = login.verifiers.get(matched).map(|(id, _)| *id)
        .ok_or_else(|| failure(NyCode::TokenNotValid))?;
    ctx.serving_state()
        .admit(id, crate::state::authority::Operation::Read)
        .map_err(|_| failure(NyCode::TokenNotValid))?;
    let _connection_report = crate::state::serving_state::ConnectionReport::start(ctx.serving_state(), id);
    let keys = derive_scope_v2(
        &ctx.root_key,
        &login.scope.salt,
        &login.scope.deployment_id,
        login.scope.route.as_str(),
    )
    .map_err(|_| failure(NyCode::TokenNotValid))?;
    let pinned_path = ctx.snapshot.read().map_err(|_| protocol())?.search_path.clone();
    let mut upstream = Upstream::connect(&ctx.upstream, &pinned_path).await?;
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
    let deadline = ctx.serving_state().session(id).map(|row| {
        tokio::time::Instant::now()
            + std::time::Duration::from_millis(row.expires_ms.saturating_sub(crate::serve::now()))
    });
    let (_connection_guard, mut end) =
        ctx.registry
            .register(id, login.principal, ctx.route.as_str(), deadline);
    let mut session = Session::new(ctx.clone(), keys, id, login.principal, Some(credential), login.scope.id);
    session.pinned_path = pinned_path;
    session.set_connection(_connection_guard.id().get());
    // Recheck after registration so revocation between login and registration cannot be missed.
    if ctx.serving_state().admit(id, Operation::Read).is_err() {
        let actions = session.refuse("", &failure(NyCode::SessionEnded), false);
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
        if let Some(reason) = end.try_ended() {
            end_connection(agent, &mut upstream, &mut session, reason).await?;
            return Ok(());
        }
        let write_before_batch = session.write_in_flight();
        let actions = if session.waiting {
            tokio::select! {
                biased;
                reason=end.ended()=>{
                    end_connection(agent, &mut upstream, &mut session, reason).await?;
                    return Ok(());
                }
                response=upstream.recv()=>session.backend(response.map_err(|error| session.upstream_lost(error, true))?),
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
                reason=end.ended()=>{
                    end_connection(agent, &mut upstream, &mut session, reason).await?;
                    return Ok(());
                }
                response=upstream.recv()=>session.backend(response.map_err(|error| session.upstream_lost(error, true))?),
                request=agent.recv()=>session.frontend(request?),
            }
        };
        if let Some(reason) = session.end_requested.take() {
            end_connection(agent, &mut upstream, &mut session, reason).await?;
            return Ok(());
        }
        if !actions.upstream.is_empty() {
            tokio::select! {
                biased;
                reason = end.ended() => {
                    end_connection(agent, &mut upstream, &mut session, reason).await?;
                    return Ok(());
                }
                result = upstream.send(&actions.upstream) => result.map_err(|error| session.upstream_lost(error, write_before_batch))?,
            }
        }
        let sent = send_or_end(agent, &mut end, &actions.client).await;
        session.release_done();
        if let Some(reason) = sent? {
            end_connection(agent, &mut upstream, &mut session, reason).await?;
            return Ok(());
        }
        if actions.close {
            break;
        }
    }
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
    include!("session/revision_tests.rs");
    include!("session/send_failure_tests.rs");
    include!("session/sql_error_tests.rs");
    include!("session/segment_tests.rs");
    include!("session/result_refusal_tests.rs");
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
        identities: Arc<Mutex<Vec<ReadLogRecord>>>,
        lifecycle: Arc<Mutex<Vec<LifecycleRecord>>>,
    }
    impl ReadLog for Audit {
        fn write_lifecycle(&self, r: &LifecycleRecord) -> Result<(), Refusal> {
            if self.fail.load(Ordering::Relaxed) {
                return Err(failure(NyCode::ReadLogUnavailable));
            }
            self.lifecycle.lock().unwrap().push(r.clone());
            Ok(())
        }
        fn write(&self, r: &ReadLogRecord) -> Result<(), Refusal> {
            if self.fail.load(Ordering::Relaxed) {
                return Err(failure(NyCode::ReadLogUnavailable));
            }
            if matches!(r.outcome, Outcome::Allowed) {
                self.allowed_records.fetch_add(1, Ordering::Relaxed);
            }
            self.identities.lock().unwrap().push(r.clone());
            self.records
                .lock()
                .unwrap()
                .push((r.rows, r.tokens_released.len()));
            Ok(())
        }
    }
    pub(crate) struct Fixture {
        access: std::sync::OnceLock<(SessionId, PrincipalId)>,
        pub(crate) ctx: Arc<GatewayContext>,
        dir: PathBuf,
        fail: Arc<AtomicBool>,
        allowed_records: Arc<std::sync::atomic::AtomicU64>,
        records: Arc<Mutex<Vec<(u64, usize)>>>,
        identities: Arc<Mutex<Vec<ReadLogRecord>>>,
        lifecycle: Arc<Mutex<Vec<LifecycleRecord>>>,
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }
    impl Fixture {
        pub(crate) fn new() -> Self {
            let domain = DomainName("secret".into());
            let col = ColumnRef {
                schema: "public".into(),
                table: "items".into(),
                column: "value".into(),
            };
            let policy = Policy {
                retired_domains: Default::default(),
                json_filters: Default::default(),
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
            let identities = Arc::new(Mutex::new(vec![]));
            let lifecycle = Arc::new(Mutex::new(vec![]));
            let allowed_records = Arc::new(std::sync::atomic::AtomicU64::new(0));
            let runtime = crate::state::runtime::StateRuntime::open(
                &dir.join("state"),
                &RootKey::from_hex(&"00".repeat(32)).unwrap(),
                &crate::config::SessionSettings::default(),
                &[],
            )
            .unwrap();
            let ctx = Arc::new(GatewayContext {
                fence: Default::default(),
                serving_gate: None,
                authority: runtime.authority.clone(),
                #[cfg(feature = "kubernetes")]
                peer_state: None,
                registry: runtime.registry.clone(),
                route: crate::state::model::RouteId::new("postgres").unwrap(),
                policy_revision: RwLock::new(None),
                policy: RwLock::new(Arc::new(policy)),
                snapshot: RwLock::new(Arc::new(snapshot)),
                root_key: RootKey::from_hex(&"00".repeat(32)).unwrap(),
                readlog: Box::new(Audit {
                    fail: fail.clone(),
                    allowed_records: allowed_records.clone(),
                    records: records.clone(),
                    identities: identities.clone(),
                    lifecycle: lifecycle.clone(),
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
                lowering: crate::dialect::pg::PgLowering,
                codec: StandardCodec,
                clock: Arc::new(|| 100),
                cancel_map: Mutex::new(Default::default()),
                metrics: Metrics::default(),
                tls: Arc::new(std::sync::RwLock::new(None)),
                allow_plaintext: true,
            });
            Self {
                access: std::sync::OnceLock::new(),
                ctx,
                dir,
                fail,
                allowed_records,
                records,
                identities,
                lifecycle,
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
            Session::new(self.ctx.clone(), keys, id, principal, None, scope.id)
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

    #[test]
    fn reload_uncaptured_pinned_path_refuses_simple_query() {
        assert_uncaptured_pinned_path_refused(false);
    }

    #[test]
    fn reload_uncaptured_pinned_path_refuses_extended_parse() {
        assert_uncaptured_pinned_path_refused(true);
    }

    fn assert_uncaptured_pinned_path_refused(extended: bool) {
        for (captured, path) in [
            (Some(vec!["public".into()]), vec!["public".into()]),
            (Some(vec!["Other".into(), "public".into()]), vec!["other".into(), "public".into()]),
            (Some(vec![]), vec!["other".into(), "public".into()]),
            (None, vec!["public".into()]),
        ] {
            let f = Fixture::new();
            let mut snapshot = (**f.ctx.snapshot.read().unwrap()).clone();
            snapshot.search_path = vec!["other".into(), "public".into()];
            snapshot = snapshot.with_captured_schemas(vec!["other".into(), "public".into()]);
            *f.ctx.snapshot.write().unwrap() = Arc::new(snapshot.clone());
            let mut session = f.session();
            // Populate the cache first, then replace its source with an invalid capture.
            snapshot.search_path = vec!["public".into(), "other".into()];
            *f.ctx.snapshot.write().unwrap() = Arc::new(snapshot.clone());
            assert!(session.snapshot().is_ok());
            snapshot.search_path = path;
            snapshot.captured_schemas = captured;
            *f.ctx.snapshot.write().unwrap() = Arc::new(snapshot);
            let sql = "SELECT value FROM items";
            let out = session.frontend(if extended {
                Frontend::Parse { name: "s".into(), query: sql.into(), param_types: vec![] }
            } else {
                Frontend::Query(sql.into())
            });
            assert_eq!(code(&out), "NY011");
            assert!(out.close);
            assert!(out.upstream.is_empty());
            assert_eq!(out.client.len(), 1);
            let records = f.identities.lock().unwrap();
            assert_eq!(records.len(), 1);
            assert!(matches!(records[0].outcome, Outcome::Refused(NyCode::SessionEnded)));
            assert_eq!(records[0].statement, sql);
            assert_eq!(records[0].rows, 0);
            assert!(records[0].tokens_released.is_empty());
            assert_eq!(f.allowed_records.load(Ordering::Relaxed), 0);
        }
    }

    #[test]
    fn reload_unknown_capture_accepts_fully_present_pinned_path() {
        for extended in [false, true] {
            let f = Fixture::new();
            let mut snapshot = (**f.ctx.snapshot.read().unwrap()).clone();
            let mut plain = snapshot.relations[0].clone();
            plain.schema = "other".into();
            snapshot.relations.push(plain);
            snapshot.search_path = vec!["public".into(), "other".into()];
            *f.ctx.snapshot.write().unwrap() = Arc::new(snapshot.clone());
            let mut session = f.session();
            snapshot.search_path = vec!["other".into(), "public".into()];
            *f.ctx.snapshot.write().unwrap() = Arc::new(snapshot);
            let pinned = session.snapshot().unwrap();
            assert_eq!(pinned.captured_schemas, None);
            assert_eq!(pinned.resolve_relation(None, "items").unwrap().schema, "public");
            assert!(Arc::ptr_eq(&pinned, &session.snapshot().unwrap()));
            if extended {
                prepare_admission_portal(&mut session, "SELECT value FROM items");
                let out = session.frontend(Frontend::Execute { portal: "p".into(), max_rows: 0 });
                assert!(!out.close);
                assert_eq!(out.upstream.len(), 1);
            } else {
                let out = session.frontend(Frontend::Query("SELECT value FROM items".into()));
                assert!(!out.close);
                assert_eq!(out.upstream.len(), 1);
            }
            session.backend(Backend::RowDescription(vec![field()]));
            let out = session.backend(Backend::DataRow(vec![Some(b"private".to_vec())]));
            let row = out.client.iter().find_map(|message| match message {
                Backend::DataRow(row) => Some(row),
                _ => None,
            }).unwrap();
            assert!(row[0].is_some());
            assert_ne!(row[0].as_deref(), Some(b"private".as_slice()));
        }
    }

    #[test]
    fn reload_pins_simple_and_extended_resolution_and_caches_each_catalog() {
        for extended in [false, true] {
            let f = Fixture::new();
            let mut snapshot = (**f.ctx.snapshot.read().unwrap()).clone();
            let mut plain = snapshot.relations[0].clone();
            plain.schema = "other".into();
            snapshot.relations.push(plain);
            snapshot = snapshot.with_captured_schemas(vec!["public".into(), "other".into()]);
            let policy = (**f.ctx.policy.read().unwrap()).clone();
            let policy = crate::policy::effective::legacy_policy(policy, &snapshot);
            *f.ctx.policy.write().unwrap() = Arc::new(policy);
            *f.ctx.snapshot.write().unwrap() = Arc::new(snapshot.clone());
            let mut old = f.session();
            snapshot.search_path = vec!["other".into()];
            *f.ctx.snapshot.write().unwrap() = Arc::new(snapshot);
            let cached = old.snapshot().unwrap();
            assert_eq!(cached.search_path, ["public"]);
            assert!(Arc::ptr_eq(&cached, &old.snapshot().unwrap()));
            for (mut session, blind) in [(old, true), (f.session(), false)] {
                assert_eq!(session.pinned_path, if blind { vec!["public"] } else { vec!["other"] });
                if extended {
                    for message in [
                        Frontend::Parse { name: "s".into(), query: "SELECT value FROM items".into(), param_types: vec![] },
                        Frontend::Bind { portal: "p".into(), statement: "s".into(), param_formats: vec![], params: vec![], result_formats: vec![] },
                        Frontend::Execute { portal: "p".into(), max_rows: 0 },
                    ] {
                        let out = session.frontend(message);
                        assert!(!out.close);
                        assert_eq!(out.upstream.len(), 1);
                    }
                    session.backend(Backend::ParseComplete);
                    session.backend(Backend::BindComplete);
                } else {
                    assert_eq!(session.frontend(Frontend::Query("SELECT value FROM items".into())).upstream.len(), 1);
                }
                session.backend(Backend::RowDescription(vec![field()]));
                let out = session.backend(Backend::DataRow(vec![Some(b"private".to_vec())]));
                let row = out.client.iter().find_map(|b| match b { Backend::DataRow(r) => Some(r), _ => None }).unwrap();
                assert_eq!(row[0].as_deref() == Some(b"private"), !blind);
            }
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
            let expected_path = f.ctx.snapshot.read().unwrap().search_path.iter()
                .map(|s| format!("\"{}\"", s.replace('"', "\"\""))).collect::<Vec<_>>().join(", ");
            let expected_set = format!("SET search_path = {expected_path}; SET standard_conforming_strings = on; SET TimeZone = 'UTC'; SET DateStyle = 'ISO, MDY'; SET IntervalStyle = 'postgres'");
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
                assert!(matches!(wire.recv().await.unwrap(), Frontend::Query(sql) if sql == expected_set));
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
            assert!(client.simple_query("DROP TABLE items").await.is_err());
            {
                let records = f.identities.lock().unwrap();
                let record = records.last().unwrap();
                let row = f.ctx.authority.session(id).unwrap();
                let credential = f.ctx.authority.session_secret_credential(id, password.as_bytes()).unwrap();
                assert_eq!(record.principal, row.principal);
                assert_eq!(record.session, id);
                assert_eq!(record.scope, row.scope);
                assert_eq!(record.route, f.ctx.route.as_str());
                assert_eq!(record.credential_id, Some(credential));
                assert!(record.connection.is_some());
            }
            drop(client);
            connection.await.unwrap().unwrap();
            server.await.unwrap();
            fake.await.unwrap();
        }).await.unwrap();
    }
    #[tokio::test]
    async fn new_connection_sets_current_snapshot_search_path() {
        let mut f = Fixture::new();
        let id = acquire_overlap(&f);
        let mut snapshot = (**f.ctx.snapshot.read().unwrap()).clone();
        snapshot.search_path = vec!["other".into()];
        *f.ctx.snapshot.write().unwrap() = Arc::new(snapshot);
        assert_login_accepted(&mut f, id, "login-secret").await;
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
    fn metadata_canonical_query_and_parse_preserve_row_description() {
        let f = Fixture::new();
        for (sql, canonical, name) in [
            ("select current_schema()", "SELECT pg_catalog.current_schema()", "current_schema"),
            ("SELECT current_database()", "SELECT pg_catalog.current_database()", "current_database"),
            ("select current_catalog", "SELECT current_catalog", "current_catalog"),
        ] {
            let mut simple = f.session();
            let out = simple.frontend(Frontend::Query(sql.into()));
            assert!(matches!(out.upstream.as_slice(), [Frontend::Query(q)] if q == canonical));
            let fields = || vec![FieldDescription {
                name: name.into(), table_oid: 0, column_attr: 0, type_oid: 19,
                type_size: 64, type_modifier: -1, format: 0,
            }];
            let response = simple.backend(Backend::RowDescription(fields()));
            assert!(matches!(response.client.as_slice(), [Backend::RowDescription(actual)] if actual == &fields()));
            let mut extended = f.session();
            let out = extended.frontend(Frontend::Parse {
                name: "metadata".into(), query: sql.into(), param_types: vec![],
            });
            assert!(matches!(out.upstream.as_slice(), [Frontend::Parse { query, .. }] if query == canonical));
            extended.backend(Backend::ParseComplete);
            extended.frontend(Frontend::Describe { kind: b'S', name: "metadata".into() });
            extended.backend(Backend::ParameterDescription(vec![]));
            let response = extended.backend(Backend::RowDescription(fields()));
            assert!(matches!(response.client.as_slice(), [Backend::RowDescription(actual)] if actual == &fields()));
        }
        let mut simple = f.session();
        let out = simple.frontend(Frontend::Query("select current_schema(); select current_database()".into()));
        let [Frontend::Query(query)] = out.upstream.as_slice() else { panic!("metadata query was refused") };
        assert_eq!(query.split(';').map(str::trim).collect::<Vec<_>>(),
            ["SELECT pg_catalog.current_schema()", "SELECT pg_catalog.current_database()"]);
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
            assert_eq!(s.write_in_flight(), expected);
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
        assert!(s.write_in_flight());
        s.frontend(Frontend::Sync);
        s.backend(Backend::ReadyForQuery(b'I'));
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
            s.set_connection(42);
            let before = f.records.lock().unwrap().len();
            let out = s.end(crate::state::registry::EndReason::Revoked);
            assert_eq!(code(&out), expected);
            assert!(out.close);
            assert!(out.upstream.is_empty());
            assert_eq!(out.client.len(), 1);
            assert_eq!(f.records.lock().unwrap().len(), before);
            let lifecycle = f.lifecycle.lock().unwrap();
            assert_eq!(lifecycle.len(), if expected == "NY012" { 2 } else { 1 });
            if expected == "NY012" {
                assert_eq!(lifecycle[1].kind, LifecycleKind::WriteOutcomeUnknown);
                assert_eq!(lifecycle[1].reason, "revoked");
            }
            let record = &lifecycle[0];
            assert_eq!(record.kind, LifecycleKind::ConnectionEnded);
            assert_eq!(record.actor, LifecycleActor::System);
            assert_eq!(record.reason, "revoked");
            assert_eq!(record.connection, Some(42));
            assert_eq!(record.session, Some(s.session.to_hex()));
            assert_eq!(record.principal, s.principal.to_hex());
            assert_eq!(record.route.as_deref(), Some("postgres"));
        }
    }
    #[test]
    fn end_audits_expired_connection() {
        let f = Fixture::new();
        let mut s = f.session();
        s.set_connection(7);
        let out = s.end(crate::state::registry::EndReason::Expired);
        assert_eq!(code(&out), "NY011");
        assert!(out.close);
        let records = f.lifecycle.lock().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].reason, "expired");
        assert_eq!(records[0].connection, Some(7));
        assert!(f.identities.lock().unwrap().is_empty());
    }
    #[test]
    fn upstream_loss_preserves_unknown_write_and_never_replays() {
        for kind in ["COMMIT", "extended", "SELECT value FROM items", "idle"] {
            let f = Fixture::new();
            let mut s = f.session();
            if kind == "extended" {
                prepare_admission_portal(&mut s, "INSERT INTO items(value) VALUES (NULL)");
                s.frontend(Frontend::Execute { portal: "p".into(), max_rows: 0 });
            } else if kind != "idle" {
                s.frontend(Frontend::Query(kind.into()));
            }
            let write = kind == "COMMIT" || kind == "extended";
            let error = s.upstream_lost(failure(NyCode::ProtocolNotAllowed), true);
            assert_eq!(error.code, if write { NyCode::WriteOutcomeUnknown } else { NyCode::ProtocolNotAllowed });
            let records = f.lifecycle.lock().unwrap();
            assert_eq!(records.len(), usize::from(write));
            if write {
                assert_eq!(records[0].kind, LifecycleKind::WriteOutcomeUnknown);
                assert_eq!(records[0].reason, "upstream_lost");
            }
        }
    }
    #[test]
    fn expired_write_has_both_lifecycle_records() {
        let f = Fixture::new();
        let mut s = f.session();
        s.frontend(Frontend::Query("COMMIT".into()));
        assert_eq!(code(&s.end(crate::state::registry::EndReason::Expired)), "NY012");
        let records = f.lifecycle.lock().unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[1].kind, LifecycleKind::WriteOutcomeUnknown);
        assert_eq!(records[1].reason, "expired");
    }
    #[test]
    fn end_audit_failure_with_write_in_flight_is_ny012() {
        let f = Fixture::new();
        let mut s = f.session();
        s.frontend(Frontend::Query("COMMIT".into()));
        f.fail.store(true, Ordering::Relaxed);
        let out = s.end(crate::state::registry::EndReason::Revoked);
        assert_eq!(code(&out), "NY012");
        assert!(out.close);
        assert!(out.upstream.is_empty());
        assert_eq!(out.client.len(), 1);
    }
    #[test]
    fn end_audit_failure_with_read_in_flight_is_ny009() {
        let f = Fixture::new();
        let mut s = f.session();
        assert_eq!(s.frontend(Frontend::Query("SELECT 1".into())).upstream.len(), 1);
        f.fail.store(true, Ordering::Relaxed);
        let out = s.end(crate::state::registry::EndReason::Revoked);
        assert_eq!(code(&out), "NY009");
        assert!(out.close);
        assert_eq!(out.client.len(), 1);
    }
    #[tokio::test]
    async fn blocked_agent_send_observes_revocation() {
        let f = Fixture::new();
        let s = f.session();
        let (_guard, mut end) = f.ctx.registry.register(
            s.session, s.principal, f.ctx.route.as_str(), None,
        );
        let (io, _unread) = tokio::io::duplex(64);
        let mut agent = Agent { io: Box::new(io), buffer: BytesMut::new(), ctx: f.ctx.clone() };
        let messages = [Backend::DataRow(vec![Some(vec![b'x'; 1024])])];
        let mut sending = Box::pin(send_or_end(&mut agent, &mut end, &messages));
        // Poll until the tiny pipe blocks; the receiver never drains it.
        assert!(tokio::time::timeout(std::time::Duration::from_millis(20), &mut sending).await.is_err());
        f.ctx.authority.revoke(s.session, crate::state::model::EndReason::AdminRevoked).unwrap();
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(1), sending).await.unwrap().unwrap(),
            Some(crate::state::registry::EndReason::Revoked),
        );
    }
    fn row_clock_fixture() -> (Fixture, Arc<std::sync::atomic::AtomicU64>) {
        use crate::state::{authority::{Authority, SessionLimits}, store::RedbStore};
        let mut f = Fixture::new();
        let now = Arc::new(std::sync::atomic::AtomicU64::new(100));
        let store_clock = now.clone();
        let store = Arc::new(RedbStore::with_clock(
            &f.dir.join("row-clock"), &RootKey::from_hex(&"00".repeat(32)).unwrap(),
            Arc::new(move || store_clock.load(Ordering::Relaxed)),
        ).unwrap());
        let ctx = Arc::get_mut(&mut f.ctx).unwrap();
        ctx.authority = Authority::load(store, SessionLimits {
            idle_timeout_ms: Some(1500), ..SessionLimits::default()
        }).unwrap();
        let gateway_clock = now.clone();
        ctx.clock = Arc::new(move || gateway_clock.load(Ordering::Relaxed));
        (f, now)
    }
    fn start_rows(f: &Fixture, extended: bool) -> Session {
        let mut s = f.session();
        if extended {
            prepare_admission_portal(&mut s, "SELECT value FROM items");
            assert_eq!(s.frontend(Frontend::Execute { portal: "p".into(), max_rows: 0 }).upstream.len(), 1);
        } else {
            assert_eq!(s.frontend(Frontend::Query("SELECT value FROM items".into())).upstream.len(), 1);
            assert!(!s.backend(Backend::RowDescription(vec![field()])).close);
        }
        s
    }
    #[test]
    fn policy_reset_refuses_suspended_portal_resume_with_ny011() {
        let f = Fixture::new();
        let mut s = start_rows(&f, true);
        s.backend(Backend::DataRow(vec![Some(b"value".to_vec())]));
        s.backend(Backend::PortalSuspended);
        let (_guard, signal) = f.ctx.registry.register(s.session, s.principal, f.ctx.route.as_str(), None);
        f.ctx.authority.end_route_sessions(&f.ctx.route, crate::state::model::EndReason::PolicyReset).unwrap();
        // Fixture has no runtime registry hook; signal as RegistryHook does after commit.
        f.ctx.registry.signal_session(&s.session, crate::state::registry::EndReason::Revoked);
        assert!(signal.try_ended().is_some());
        let out = s.frontend(Frontend::Execute { portal: "p".into(), max_rows: 1 });
        assert_eq!(code(&out), "NY011");
        assert!(out.upstream.is_empty());
        assert!(!out.client.iter().any(|message| matches!(message, Backend::DataRow(_))));
    }

    #[test]
    fn relayed_rows_refresh_activity_beyond_idle_timeout() {
        for extended in [false, true] {
            let (f, now) = row_clock_fixture();
            let mut s = start_rows(&f, extended);
            for tick in [1100, 2100, 3100, 4100, 5100] {
                now.store(tick, Ordering::Relaxed);
                let out = s.backend(Backend::DataRow(vec![Some(b"value".to_vec())]));
                assert!(!out.close);
                assert!(matches!(out.client.as_slice(), [Backend::DataRow(_)]));
                assert_eq!(f.ctx.authority.session(s.session).unwrap().last_used_ms, tick);
                assert!(s.end_requested.is_none());
            }
            assert!(f.ctx.authority.admit(s.session, Operation::Read).is_ok());
        }
    }
    #[test]
    fn row_activity_refresh_is_limited_to_once_per_second() {
        for extended in [false, true] {
            let (f, now) = row_clock_fixture();
            let mut s = start_rows(&f, extended);
            for (tick, expected) in [(200, 200), (400, 200), (1199, 200), (1200, 1200), (1300, 1200)] {
                now.store(tick, Ordering::Relaxed);
                let out = s.backend(Backend::DataRow(vec![None]));
                assert!(matches!(out.client.as_slice(), [Backend::DataRow(_)]));
                assert_eq!(f.ctx.authority.session(s.session).unwrap().last_used_ms, expected);
            }
        }
    }
    #[test]
    fn failed_row_refresh_requests_end_without_releasing_more_rows() {
        for extended in [false, true] {
            for write in [false, true] {
                let (f, now) = row_clock_fixture();
                let mut s = start_rows(&f, extended);
                if write {
                    // A forwarded commit remains indeterminate when the read response ends.
                    let mut commit = f.session();
                    assert_eq!(commit.frontend(Frontend::Query("COMMIT".into())).upstream.len(), 1);
                    s.active.extend(commit.active);
                }
                now.store(1700, Ordering::Relaxed);
                let out = s.backend(Backend::DataRow(vec![Some(b"value".to_vec())]));
                assert!(out.client.is_empty());
                assert_eq!(s.end_requested, Some(crate::state::registry::EndReason::Expired));
                // A second row cannot escape even while refresh is rate limited.
                assert!(s.backend(Backend::DataRow(vec![None])).client.is_empty());
                let out = s.end(s.end_requested.unwrap());
                assert_eq!(code(&out), if write { "NY012" } else { "NY011" });
                assert!(out.close);
            }
        }
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
    #[tokio::test]
    async fn tcp_upstream_recv_loss_never_replays_writes() {
        tokio::time::timeout(std::time::Duration::from_secs(20), async {
            for variant in ["COMMIT", "extended", "SELECT value FROM items"] {
                let mut f = Fixture::new();
                let (id, _) = acquire_login(&f, "postgres");
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let ctx = Arc::get_mut(&mut f.ctx).unwrap();
                ctx.upstream.host = "127.0.0.1".into();
                ctx.upstream.port = listener.local_addr().unwrap().port();
                let fake_ctx = f.ctx.clone();
                let fake = tokio::spawn(async move {
                    let (stream, _) = listener.accept().await.unwrap();
                    let mut wire = Agent { io: Box::new(stream), buffer: BytesMut::new(), ctx: fake_ctx };
                    wire.startup().await.unwrap();
                    wire.send(&[Backend::AuthenticationOk,
                        Backend::BackendKeyData { pid: 123, secret: vec![1,2,3,4] },
                        Backend::ReadyForQuery(b'I')]).await.unwrap();
                    assert!(matches!(wire.recv().await.unwrap(), Frontend::Query(_)));
                    wire.send(&[Backend::CommandComplete("SET".into()), Backend::ReadyForQuery(b'I')]).await.unwrap();
                    loop {
                        let reply = match wire.recv().await.unwrap() {
                            Frontend::Query(sql) => { assert_eq!(sql, variant); break; }
                            Frontend::Execute { .. } => { assert_eq!(variant, "extended"); break; }
                            Frontend::Parse { query, .. } => {
                                assert_eq!(query, "INSERT INTO items(value) VALUES (NULL)");
                                vec![Backend::ParseComplete]
                            }
                            Frontend::Describe { .. } => vec![Backend::ParameterDescription(vec![]), Backend::NoData],
                            Frontend::Bind { .. } => vec![Backend::BindComplete],
                            Frontend::Sync => vec![Backend::ReadyForQuery(b'I')],
                            Frontend::Flush => vec![],
                            _ => panic!("unexpected fixture request"),
                        };
                        wire.send(&reply).await.unwrap();
                    }
                    // EOF on the response side, but keep reading to detect any replay.
                    wire.io.shutdown().await.unwrap();
                    loop {
                        match wire.recv().await {
                            Ok(Frontend::Sync | Frontend::Flush | Frontend::Terminate) => {},
                            Err(_) => break,
                            Ok(_) => panic!("gateway replayed work after upstream loss"),
                        }
                    }
                    assert!(tokio::time::timeout(std::time::Duration::from_millis(50), listener.accept()).await.is_err());
                });
                let gateway = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let addr = gateway.local_addr().unwrap();
                let ctx = f.ctx.clone();
                let server = tokio::spawn(async move {
                    let (stream, _) = gateway.accept().await.unwrap();
                    serve_tcp(stream, ctx).await;
                });
                let (client, connection) = tokio_postgres::Config::new().host("127.0.0.1").port(addr.port())
                    .user(format!("session:{}", id.to_hex())).password("login-secret")
                    .dbname("app").connect(tokio_postgres::NoTls).await.unwrap();
                let driver = tokio::spawn(connection);
                let error = if variant == "extended" {
                    client.execute("INSERT INTO items(value) VALUES (NULL)", &[]).await.unwrap_err()
                } else {
                    client.batch_execute(variant).await.unwrap_err()
                };
                let write = variant != "SELECT value FROM items";
                assert_eq!(error.code().map(|c| c.code()), Some(if write { "NY012" } else { "NY010" }));
                drop(client);
                server.await.unwrap();
                fake.await.unwrap();
                let _ = driver.await.unwrap();
                let audit = f.lifecycle.lock().unwrap();
                assert_eq!(audit.len(), usize::from(write));
                if write {
                    assert_eq!(audit[0].kind, LifecycleKind::WriteOutcomeUnknown);
                    assert_eq!(audit[0].reason, "upstream_lost");
                    assert_eq!(audit[0].session, Some(id.to_hex()));
                }
            }
        }).await.unwrap();
    }
    fn code(actions: &Actions) -> &str {
        let Backend::ErrorResponse(fields) = &actions.client[0] else {
            panic!("expected error")
        };
        fields.iter().find(|(k, _)| *k == b'C').unwrap().1.as_str()
    }
    #[test]
    fn extended_session_metadata_and_empty_query() {
        for sql in ["SHOW TRANSACTION ISOLATION LEVEL", "select current_schema()", "select current_catalog", "", " \t\n"] {
            let f = Fixture::new();
            let mut s = f.session();
            let empty = sql.trim().is_empty();
            let parse = s.frontend(Frontend::Parse { name: "s".into(), query: sql.into(), param_types: vec![] });
            if empty {
                assert!(parse.upstream.is_empty());
                assert!(matches!(parse.client.as_slice(), [Backend::ParseComplete]));
            } else {
                let expected = match sql {
                    "select current_schema()" => "SELECT pg_catalog.current_schema()",
                    "select current_catalog" => "SELECT current_catalog",
                    "SHOW TRANSACTION ISOLATION LEVEL" => sql,
                    _ => panic!("unexpected metadata fixture"),
                };
                assert!(matches!(&parse.upstream[0], Frontend::Parse { query, .. } if query == expected));
                assert!(matches!(s.backend(Backend::ParseComplete).client.as_slice(), [Backend::ParseComplete]));
            }
            let bind = s.frontend(Frontend::Bind { portal: "p".into(), statement: "s".into(), param_formats: vec![], params: vec![], result_formats: vec![] });
            if empty { assert!(matches!(bind.client.as_slice(), [Backend::BindComplete])); }
            else { assert!(matches!(s.backend(Backend::BindComplete).client.as_slice(), [Backend::BindComplete])); }
            for kind in *b"SP" {
                let describe = s.frontend(Frontend::Describe { kind, name: if kind == b'S' { "s" } else { "p" }.into() });
                if empty {
                    assert!(matches!(describe.client.last(), Some(Backend::NoData)));
                    assert_eq!(describe.client.len(), if kind == b'S' { 2 } else { 1 });
                    if kind == b'S' { assert!(matches!(&describe.client[0], Backend::ParameterDescription(p) if p.is_empty())); }
                } else {
                    assert_eq!(describe.upstream.len(), 1);
                    if kind == b'S' { s.backend(Backend::ParameterDescription(vec![])); }
                    assert!(matches!(s.backend(Backend::RowDescription(vec![field()])).client.as_slice(), [Backend::RowDescription(_)]));
                }
            }
            let execute = s.frontend(Frontend::Execute { portal: "p".into(), max_rows: 0 });
            if empty {
                assert!(execute.upstream.is_empty());
                assert!(matches!(execute.client.as_slice(), [Backend::EmptyQueryResponse]));
            } else {
                assert_eq!(execute.upstream.len(), 1);
                assert!(matches!(s.backend(Backend::DataRow(vec![Some(b"metadata".to_vec())])).client.as_slice(), [Backend::DataRow(_)]));
                assert!(matches!(s.backend(Backend::CommandComplete("SELECT 1".into())).client.as_slice(), [Backend::CommandComplete(_)]));
            }
            s.frontend(Frontend::Sync);
            assert!(!s.backend(Backend::ReadyForQuery(b'I')).close);
            assert_eq!(f.allowed_records.load(Ordering::Relaxed), if empty { 1 } else { 2 });
            // Forwarded selects have an intent and completion record; local noops have one.
            assert!(f.identities.lock().unwrap().iter().all(|r| matches!(r.outcome, Outcome::Allowed)));
        }
    }
    #[test]
    fn extended_empty_query_refuses_parameters_and_nonempty_zero_statements() {
        for sql in ["", " "] {
            let f = Fixture::new(); let mut s = f.session();
            s.frontend(Frontend::Parse { name: "s".into(), query: sql.into(), param_types: vec![] });
            let out = s.frontend(Frontend::Bind { portal: "p".into(), statement: "s".into(), param_formats: vec![], params: vec![Some(b"x".to_vec())], result_formats: vec![] });
            assert_eq!(code(&out), "NY004");
            assert_eq!(f.allowed_records.load(Ordering::Relaxed), 0);
        }
        for (sql, param_types) in [("", vec![25]), ("-- comment", vec![]), (";", vec![])] {
            let f = Fixture::new(); let mut s = f.session();
            let out = s.frontend(Frontend::Parse { name: "s".into(), query: sql.into(), param_types });
            assert_eq!(code(&out), "NY004");
        }
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
        let policy = (**f.ctx.policy.read().unwrap()).clone();
        let policy = crate::policy::effective::legacy_policy(policy, &snapshot);
        *f.ctx.policy.write().unwrap() = Arc::new(policy);
        *f.ctx.snapshot.write().unwrap() = Arc::new(snapshot);
        f
    }
    #[test]
    fn retired_token_literal_in_show_column_is_refused() {
        let f = notes_fixture();
        let mut policy = (**f.ctx.policy.read().unwrap()).clone();
        policy.retired_domains.insert(DomainName("retired".into()));
        *f.ctx.policy.write().unwrap() = Arc::new(policy);
        let out = f.session().frontend(Frontend::Query("UPDATE notes SET body = 'tok_retired_a' WHERE id = 3".into()));
        assert_eq!(code(&out), "NY001"); assert!(out.upstream.is_empty());
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
    fn retired_tokens_refused_in_literals_and_bind_before_send() {
        for retired in [false, true] {
            let f = Fixture::new();
            let mut session = f.session();
            let old = DomainName("auto_retired_test".into());
            let domain = if retired { &old } else { &DomainName("secret".into()) };
            let token = f.ctx.codec.encode(&session.keys, domain, &ColumnType::Text, None,
                &SqlValue::Text("private@example.com".into())).unwrap();
            let bytes = values::encode_text(&token).unwrap();
            let mut policy = (**f.ctx.policy.read().unwrap()).clone();
            policy.retired_domains.insert(old);
            *f.ctx.policy.write().unwrap() = Arc::new(policy);
            let sql = format!("SELECT value FROM items WHERE value = '{}'", String::from_utf8(bytes.clone()).unwrap());
            let out = session.frontend(Frontend::Query(sql));
            if retired { assert_eq!(code(&out), "NY001"); assert!(out.upstream.is_empty()); }
            else { assert!(matches!(&out.upstream[0], Frontend::Query(sql) if sql.contains("private@example.com"))); }
            let mut session = f.session();
            // Encode for the new session's scope.
            let domain = if retired { DomainName("auto_retired_test".into()) } else { DomainName("secret".into()) };
            let token = f.ctx.codec.encode(&session.keys, &domain, &ColumnType::Text, None,
                &SqlValue::Text("private@example.com".into())).unwrap();
            session.frontend(Frontend::Parse { name: "s".into(), query: "SELECT value FROM items WHERE value = $1".into(), param_types: vec![25] });
            session.backend(Backend::ParseComplete);
            let out = session.frontend(Frontend::Bind { portal: "p".into(), statement: "s".into(), param_formats: vec![],
                params: vec![Some(values::encode_text(&token).unwrap())], result_formats: vec![] });
            if retired { assert_eq!(code(&out), "NY001"); assert!(out.upstream.is_empty()); }
            else { assert!(matches!(&out.upstream[0], Frontend::Bind { params, .. } if params[0].as_deref() == Some(b"private@example.com"))); }
        }
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
    fn aggregate_filter_rewrites_text_tokens_in_simple_and_extended_queries() {
        let f = Fixture::new();
        let mut simple = f.session();
        let clear = SqlValue::Text("O'Brien".into());
        let token = f.ctx.codec.encode(
            &simple.keys,
            &DomainName("secret".into()),
            &ColumnType::Text,
            None,
            &clear,
        ).unwrap();
        let token = values::encode_text(&token).unwrap();
        let sql = format!(
            "SELECT count(*) FILTER (WHERE value = '{}') FROM items",
            std::str::from_utf8(&token).unwrap()
        );
        let out = simple.frontend(Frontend::Query(sql));
        assert!(matches!(out.upstream.as_slice(), [Frontend::Query(sql)]
            if sql == "SELECT count(*) FILTER (WHERE value = 'O''Brien') FROM items"));

        let mut extended = f.session();
        let out = extended.frontend(Frontend::Parse {
            name: "s".into(),
            query: "SELECT count(*) FILTER (WHERE value = $1) FROM items".into(),
            param_types: vec![25],
        });
        assert!(matches!(out.upstream.as_slice(), [Frontend::Parse { .. }]));
        extended.backend(Backend::ParseComplete);
        let out = extended.frontend(Frontend::Bind {
            portal: "p".into(),
            statement: "s".into(),
            param_formats: vec![],
            params: vec![Some(token)],
            result_formats: vec![],
        });
        assert!(matches!(out.upstream.as_slice(), [Frontend::Bind { params, .. }]
            if params[0].as_deref() == Some(b"O'Brien")));
    }

    #[test]
    fn coalesce_output_is_tokenized_before_release() {
        let f = Fixture::new();
        let mut session = f.session();
        let out = session.frontend(Frontend::Query("SELECT coalesce(value, value) FROM items".into()));
        assert!(matches!(out.upstream.as_slice(), [Frontend::Query(_)]));
        let out = session.backend(Backend::RowDescription(vec![field()]));
        assert!(matches!(out.client.as_slice(), [Backend::RowDescription(_)]));
        let clear = "private@example.com";
        let out = session.backend(Backend::DataRow(vec![Some(clear.as_bytes().to_vec())]));
        let [Backend::DataRow(row)] = out.client.as_slice() else {
            panic!("expected tokenized row");
        };
        let token = row[0].as_ref().unwrap();
        assert!(token.starts_with(b"tok_secret_"));
        assert_ne!(token, clear.as_bytes());
        let token = values::decode_text(&ColumnType::Text, token).unwrap();
        assert_eq!(
            f.ctx.codec.decode(&session.keys, &DomainName("secret".into()), &ColumnType::Text, None, &token).unwrap(),
            SqlValue::Text(clear.into())
        );
    }

    #[test]
    fn plaintext_literal_and_bind_in_blind_equality_return_ny003() {
        let f = Fixture::new();
        let clear = "olivia.smith@example.com";
        let mut simple = f.session();
        let literal = simple.frontend(Frontend::Query(format!(
            "SELECT value FROM items WHERE value = '{clear}'"
        )));

        let mut extended = f.session();
        let out = extended.frontend(Frontend::Parse {
            name: "s".into(),
            query: "SELECT value FROM items WHERE value = $1".into(),
            param_types: vec![25],
        });
        assert!(matches!(out.upstream.as_slice(), [Frontend::Parse { .. }]));
        extended.backend(Backend::ParseComplete);
        let bind = extended.frontend(Frontend::Bind {
            portal: "p".into(),
            statement: "s".into(),
            param_formats: vec![],
            params: vec![Some(clear.as_bytes().to_vec())],
            result_formats: vec![],
        });

        for out in [literal, bind] {
            assert_eq!(code(&out), "NY003");
            assert!(out.upstream.is_empty());
            let Backend::ErrorResponse(fields) = &out.client[0] else {
                panic!("expected error response");
            };
            assert!(fields.iter().all(|(_, value)| !value.contains(clear)));
        }
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
        let mismatch = session.backend(Backend::RowDescription(vec![]));
        assert_result_error(&mismatch, NyCode::NotAnalysable, false);
        assert_discarded(session.backend(Backend::CommandComplete("SELECT 0".into())));
        assert_result_ready(&mut session, b'I', b'I');
    }
    /// Construct a path filter for wire-format and prepared-policy regression tests.
    pub(super) fn json_test_filter(mode: crate::json::filter::JsonMode, root: bool, blind: bool) -> crate::policy::JsonColumnFilter {
        use crate::policy::{JsonPathRule, JsonPathTreatment, JsonTokenType, PathSegment};
        crate::policy::JsonColumnFilter {
            mode,
            rules: vec![JsonPathRule {
                column: crate::schema::ColumnRef { schema: "public".into(), table: "items".into(), column: "value".into() },
                path: if root { vec![] } else { vec![PathSegment::Key("protected".into())] },
                treatment: if blind {
                    JsonPathTreatment::Blind { domain: DomainName("secret".into()), ty: JsonTokenType::Text }
                } else { JsonPathTreatment::Masked },
            }],
        }
    }

    #[test]
    fn json_rows_formats_modes_nulls_and_refusals() {
        use crate::json::filter::JsonMode;
        let f = Fixture::new();
        let session = f.session();
        let policy = f.ctx.policy.read().unwrap();
        for (ty, format) in [(ColumnType::Json, 0), (ColumnType::Json, 1), (ColumnType::Jsonb, 0), (ColumnType::Jsonb, 1)] {
            for (mode, blind) in [(JsonMode::Show, false), (JsonMode::Tokenize, true)] {
                let mut filter = json_test_filter(mode, false, blind);
                let mut plan = RewritePlan { outputs: vec![OutputRewrite::Pass, OutputRewrite::JsonFilter { filter: filter.clone(), ty: ty.clone() }], inputs: vec![], tokenize: None };
                let wire = |text: &str| {
                    let mut bytes = if ty == ColumnType::Jsonb && format == 1 { vec![1] } else { vec![] };
                    bytes.extend_from_slice(text.as_bytes()); bytes
                };
                let run = |cell, plan: &RewritePlan| rewrite_row(vec![Some(b"earlier".to_vec()), cell], plan, &[format], &policy, &session.keys, &f.ctx.codec);
                let token = f.ctx.codec.encode(&session.keys, &DomainName("secret".into()), &ColumnType::Text, None, &SqlValue::Text("hidden".into())).unwrap();
                let SqlValue::Text(token) = token else { panic!("expected text token") };
                let expected = if blind { format!(r#"{{"protected":"{token}"}}"#) } else { r#"{"unknown":"visible"}"#.into() };
                let expected = if ty == ColumnType::Jsonb { expected.replace("\":\"", "\": \"") } else { expected };
                if !blind {
                    let row = run(Some(wire(r#"{"protected":{"nested":"hidden"},"unknown":"visible"}"#)), &plan).unwrap().0;
                    assert_eq!(row[1], Some(wire(&expected)));
                }
                let (row, released) = run(Some(wire(r#"{"protected":"hidden","unknown":"visible"}"#)), &plan).unwrap();
                assert_eq!(released, if blind { vec![token.clone()] } else { vec![] });
                assert_eq!(row, vec![Some(b"earlier".to_vec()), Some(wire(&expected))]);
                assert_eq!(run(None, &plan).unwrap().0[1], None);
                let error = run(Some(wire(r#"{"protected":"hidden","protected":"hidden"}"#)), &plan).unwrap_err();
                assert!(!format!("{error:?}").contains("hidden"));
                filter = json_test_filter(mode, true, false);
                plan.outputs[1] = OutputRewrite::JsonFilter { filter, ty: ty.clone() };
                assert_eq!(run(Some(wire(r#"{"protected":"hidden"}"#)), &plan).unwrap().0[1], None);
            }
        }
    }

    #[test]
    fn json_extract_rows_preserve_tokens_types_and_wire_formats() {
        use crate::{json::filter::JsonMode, policy::{JsonPathTreatment, JsonLeafType}};
        let f = Fixture::new();
        let session = f.session();
        let policy = f.ctx.policy.read().unwrap();
        for base in [ColumnType::Json, ColumnType::Jsonb] {
            for format in [0, 1] {
                for text in [false, true] {
                    let wire = |value: &str, text_result: bool| {
                        let mut bytes = if base == ColumnType::Jsonb && format == 1 && !text_result { vec![1] } else { vec![] };
                        bytes.extend_from_slice(value.as_bytes()); bytes
                    };
                    let mut filter = json_test_filter(JsonMode::Tokenize, true, true);
                    let make = |filter| RewritePlan { outputs: vec![OutputRewrite::JsonExtract { filter, base: base.clone(), text, keep_empty: false }], inputs: vec![], tokenize: None };
                    let run = |bytes, plan: &RewritePlan| rewrite_row(vec![bytes], plan, &[format], &policy, &session.keys, &f.ctx.codec);
                    let plan = make(filter.clone());
                    let (row, tokens) = run(Some(wire(r#""hidden""#, text)), &plan).unwrap();
                    let whole = RewritePlan { outputs: vec![OutputRewrite::JsonFilter { filter: json_test_filter(JsonMode::Tokenize, false, true), ty: base.clone() }], inputs: vec![], tokenize: None };
                    let (document, whole_tokens) = run(Some(wire(r#"{"protected":"hidden"}"#, false)), &whole).unwrap();
                    assert!(document[0].is_some());
                    assert_eq!(tokens, whole_tokens);
                    assert_eq!(row[0], Some(if text { tokens[0].as_bytes().to_vec() } else { wire(&format!("\"{}\"", tokens[0]), false) }));
                    assert_eq!(run(None, &plan).unwrap().0, vec![None]);
                    assert_eq!(run(Some(wire("{}", text)), &plan).unwrap().0, vec![None]);
                    filter.rules[0].treatment = JsonPathTreatment::Show { ty: JsonLeafType::String };
                    let plan = make(filter);
                    for input in ["123", "{}", "[]"] {
                        assert_eq!(run(Some(wire(input, text)), &plan).unwrap().0, vec![None]);
                    }
                    let expected = if text { b"hello".to_vec() } else { wire(r#""hello""#, false) };
                    assert_eq!(run(Some(wire(r#""hello""#, text)), &plan).unwrap().0, vec![Some(expected)]);
                    let null = run(Some(wire("null", text)), &plan).unwrap().0;
                    assert_eq!(null, vec![if text { None } else { Some(wire("null", false)) }]);
                    for (ty, input) in [(JsonLeafType::Number, "123"), (JsonLeafType::Boolean, "true")] {
                        let mut numeric = json_test_filter(JsonMode::Show, true, false);
                        numeric.rules[0].treatment = JsonPathTreatment::Show { ty };
                        let plan = make(numeric);
                        assert_eq!(run(Some(wire(input, text)), &plan).unwrap().0,
                            vec![Some(if text { input.as_bytes().to_vec() } else { wire(input, false) })]);
                    }
                    let parent = json_test_filter(JsonMode::Show, false, false);
                    let input = r#"{"protected":"hidden","other":"visible"}"#;
                    let extracted = make(parent.clone());
                    let whole = RewritePlan { outputs: vec![OutputRewrite::JsonFilter { filter: parent, ty: base.clone() }], inputs: vec![], tokenize: None };
                    let a = run(Some(wire(input, text)), &extracted).unwrap().0;
                    let b = run(Some(wire(input, false)), &whole).unwrap().0;
                    let mut expected = b[0].clone().unwrap();
                    if text && base == ColumnType::Jsonb && format == 1 { expected.remove(0); }
                    assert_eq!(a, vec![Some(expected)]);
                }
            }
        }
    }

    #[test]
    fn json_extraction_simple_extended_and_suspended_portals() {
        use crate::json::filter::JsonMode;
        for base in [ColumnType::Json, ColumnType::Jsonb] {
            for text in [false, true] {
                for extended in [false, true] {
                    for format in if extended { vec![0, 1] } else { vec![0] } {
                        let f = Fixture::new();
                        let mut snapshot = (**f.ctx.snapshot.read().unwrap()).clone();
                        snapshot.relations[0].columns[0].ty = base.clone();
                        *f.ctx.snapshot.write().unwrap() = Arc::new(snapshot);
                        let mut policy = (**f.ctx.policy.read().unwrap()).clone();
                        let filter = json_test_filter(JsonMode::Tokenize, false, true);
                        policy.columns.insert(filter.rules[0].column.clone(), Treatment::Masked { placeholder: None });
                        policy.json_filters.insert(filter.rules[0].column.clone(), filter);
                        *f.ctx.policy.write().unwrap() = Arc::new(policy);
                        let mut session = f.session();
                        let query = format!("SELECT value->{}'protected' FROM items", if text { ">" } else { "" });
                        let action = if extended {
                            let action = session.frontend(Frontend::Parse { name: "s".into(), query, param_types: vec![] });
                            assert!(!session.backend(Backend::ParseComplete).close);
                            session.frontend(Frontend::Bind { portal: "p".into(), statement: "s".into(), param_formats: vec![], params: vec![], result_formats: vec![format] });
                            assert!(!session.backend(Backend::BindComplete).close);
                            session.frontend(Frontend::Execute { portal: "p".into(), max_rows: 1 });
                            action
                        } else {
                            let action = session.frontend(Frontend::Query(query));
                            let mut field = field();
                            field.type_oid = if text { 25 } else { base.pg_oid().unwrap() };
                            assert!(!session.backend(Backend::RowDescription(vec![field])).close);
                            action
                        };
                        assert_eq!(action.upstream.len(), 1);
                        let sql = match &action.upstream[0] {
                            Frontend::Parse { query, .. } | Frontend::Query(query) => query,
                            _ => panic!("query expected"),
                        };
                        assert_eq!(sql.contains("::text"), text);
                        let mut bytes = if base == ColumnType::Jsonb && !text && format == 1 { vec![1] } else { vec![] };
                        bytes.extend_from_slice(br#""hidden""#);
                        let mut first = None;
                        for iteration in 0..if extended { 2 } else { 1 } {
                            let action = session.backend(Backend::DataRow(vec![Some(bytes.clone())]));
                            assert!(!action.close);
                            let Backend::DataRow(row) = &action.client[0] else { panic!("expected row") };
                            assert!(row[0].is_some());
                            if iteration == 0 { first = Some(row.clone()); } else { assert_eq!(Some(row.clone()), first); }
                            if extended && iteration == 0 {
                                assert!(!session.backend(Backend::PortalSuspended).close);
                                assert_eq!(session.frontend(Frontend::Execute { portal: "p".into(), max_rows: 1 }).upstream.len(), 1);
                            }
                        }
                        assert!(!session.backend(Backend::CommandComplete("SELECT 1".into())).close);
                        assert!(f.records.lock().unwrap().iter().any(|(rows, tokens)| *rows > 0 && *tokens > 0));
                    }
                }
            }
        }
    }

    #[test]
    fn prepared_json_extraction_tracks_filter_dependencies_even_when_plain() {
        use crate::json::filter::JsonMode;
        for expression in ["value->>'protected'", "length(value->>'other')"] {
            let f = Fixture::new();
            let mut snapshot = (**f.ctx.snapshot.read().unwrap()).clone();
            snapshot.relations[0].columns[0].ty = ColumnType::Jsonb;
            *f.ctx.snapshot.write().unwrap() = Arc::new(snapshot);
            let mut policy = (**f.ctx.policy.read().unwrap()).clone();
            let filter = json_test_filter(JsonMode::Show, false, true);
            let column = filter.rules[0].column.clone();
            policy.columns.insert(column.clone(), Treatment::Masked { placeholder: None });
            policy.json_filters.insert(column.clone(), filter);
            *f.ctx.policy.write().unwrap() = Arc::new(policy);
            let mut session = f.session();
            let parsed = session.frontend(Frontend::Parse { name: "s".into(), query: format!("SELECT {expression} FROM items"), param_types: vec![] });
            assert_eq!(parsed.upstream.len(), 1);
            session.backend(Backend::ParseComplete);
            let mut policy = (**f.ctx.policy.read().unwrap()).clone();
            policy.json_filters.get_mut(&column).unwrap().rules[0].treatment = crate::policy::JsonPathTreatment::Masked;
            *f.ctx.policy.write().unwrap() = Arc::new(policy);
            let action = session.frontend(Frontend::Bind { portal: "p".into(), statement: "s".into(), param_formats: vec![], params: vec![], result_formats: vec![] });
            assert!(action.upstream.is_empty());
            assert_eq!(code(&action), "NY008");
        }
    }

    #[test]
    fn json_row_description_requires_matching_oid() {
        for (ty, oid) in [(ColumnType::Json, 114), (ColumnType::Jsonb, 3802)] {
            let plan = RewritePlan { outputs: vec![OutputRewrite::JsonFilter { filter: json_test_filter(crate::json::filter::JsonMode::Show, false, false), ty }], inputs: vec![], tokenize: None };
            let mut field = field();
            field.type_oid = oid;
            assert!(validate_json_fields(std::slice::from_ref(&field), &plan).is_ok());
            assert!(super::extended::rewrite_fields(std::slice::from_mut(&mut field), &plan).is_ok());
            field.type_oid = if oid == 114 { 3802 } else { 114 };
            assert_eq!(validate_json_fields(std::slice::from_ref(&field), &plan).unwrap_err().code, NyCode::NotAnalysable);
            assert!(super::extended::rewrite_fields(&mut [field], &plan).is_err());
        }
    }

    #[test]
    fn json_text_row_description_accepts_pgadapter_varchar() {
        use crate::json::filter::JsonMode;
        let filter = json_test_filter(JsonMode::Tokenize, true, true);
        for base in [ColumnType::Json, ColumnType::Jsonb] {
            for text in [false, true] {
                let plan = RewritePlan {
                    outputs: vec![OutputRewrite::JsonExtract {
                        filter: filter.clone(), base: base.clone(), text, keep_empty: false,
                    }],
                    inputs: vec![],
                    tokenize: None,
                };
                for oid in [20, 25, 1043, 114, 3802] {
                    let mut field = field();
                    field.type_oid = oid;
                    let expected = if text { matches!(oid, 25 | 1043) } else { base.pg_oid() == Some(oid) };
                    assert_eq!(validate_json_fields(std::slice::from_ref(&field), &plan).is_ok(), expected);
                    assert_eq!(super::extended::rewrite_fields(&mut [field], &plan).is_ok(), expected);
                }
            }
        }
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
    fn tokenization_store_refusal_keeps_session_open() {
        assert_eq!(tokenize_refusal(TokenizeError::Store), NyCode::ReadLogUnavailable);
        assert_eq!(tokenize_refusal(TokenizeError::Ended), NyCode::SessionEnded);
        assert_eq!(tokenize_refusal(TokenizeError::LimitReached), NyCode::TokenizeLimit);
        for extended in [false, true] {
            let f = Fixture::new();
            let mut s = f.session();
            let sql = "SELECT nunya_tokenize('secret', 'private')";
            let out = if extended {
                s.ext_refuse(sql, &failure(tokenize_refusal(TokenizeError::Store)), false)
            } else {
                s.refuse(sql, &failure(tokenize_refusal(TokenizeError::Store)), true)
            };
            assert_eq!(code(&out), "NY009");
            assert!(!out.close);
            assert!(out.upstream.is_empty());
            let ready = if extended {
                assert!(!s.frontend(Frontend::Sync).close);
                s.backend(Backend::ReadyForQuery(b'I'))
            } else {
                out
            };
            assert!(ready.client.iter().any(|m| matches!(m, Backend::ReadyForQuery(b'I'))));
            assert!(f.ctx.authority.session_login(s.session, &f.ctx.route).is_some());
        }
    }
    #[test]
    fn tokenize_limit_audit_has_only_refused_record() {
        for extended in [false, true] {
            let f = Fixture::new();
            set_tokenize_limits(&f, 1, 20);
            let mut s = f.session();
            assert!(tokenize(&mut s, extended).client.iter().any(|m| matches!(m, Backend::DataRow(_))));
            f.identities.lock().unwrap().clear();
            let allowed = f.allowed_records.load(Ordering::Relaxed);
            assert_eq!(code(&tokenize(&mut s, extended)), "NY007");
            assert_eq!(f.allowed_records.load(Ordering::Relaxed), allowed);
            let records = f.identities.lock().unwrap();
            assert_eq!(records.len(), 1);
            assert!(matches!(records[0].outcome, Outcome::Refused(NyCode::TokenizeLimit)));
        }
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
    fn prepared_tokenize_refuses_domain_limit_tightening_at_execute() {
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
        assert_eq!(code(&refused), "NY008");
        assert!(!refused.client.iter().any(|message| matches!(message, Backend::DataRow(_))));
        // Refused Execute consumes no additional durable tokenization budget.
        assert_eq!(f.ctx.authority.session_tokenizations(s.session, &DomainName("secret".into())).unwrap(), 1);
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
    #[test]
    fn prepared_treatment_refresh_refuses_bind_and_existing_portal_execute() {
        for bind_before_refresh in [false, true] {
            let f = Fixture::new();
            let column = ColumnRef { schema: "public".into(), table: "items".into(), column: "value".into() };
            let mut initial = (**f.ctx.policy.read().unwrap()).clone();
            initial.columns.insert(column.clone(), Treatment::Show);
            *f.ctx.policy.write().unwrap() = Arc::new(initial);
            let mut s = f.session();
            s.frontend(Frontend::Parse { name: "s".into(), query: "SELECT value FROM items".into(), param_types: vec![] });
            s.backend(Backend::ParseComplete);
            let bind = || Frontend::Bind { portal: "p".into(), statement: "s".into(), param_formats: vec![], params: vec![], result_formats: vec![] };
            if bind_before_refresh {
                s.frontend(bind());
                s.backend(Backend::BindComplete);
            }
            let mut next = (**f.ctx.policy.read().unwrap()).clone();
            next.columns.insert(column, Treatment::Masked { placeholder: None });
            *f.ctx.policy.write().unwrap() = Arc::new(next);
            let out = if bind_before_refresh {
                s.frontend(Frontend::Execute { portal: "p".into(), max_rows: 0 })
            } else { s.frontend(bind()) };
            assert!(out.upstream.is_empty());
            assert!(out.client.iter().any(|m| matches!(m, Backend::ErrorResponse(fields) if *fields == nunya_error(NyCode::SchemaStale))));
        }
    }

    #[test]
    fn prepared_output_and_write_treatment_presence_changes_refuse() {
        for sql in ["SELECT value FROM items", "UPDATE items SET value = 'x'"] {
            let f = Fixture::new();
            let column = ColumnRef { schema: "public".into(), table: "items".into(), column: "value".into() };
            let mut initial = (**f.ctx.policy.read().unwrap()).clone();
            initial.columns.insert(column.clone(), Treatment::Show);
            *f.ctx.policy.write().unwrap() = Arc::new(initial);
            let mut s = f.session();
            let parsed = s.frontend(Frontend::Parse { name: "s".into(), query: sql.into(), param_types: vec![] });
            assert!(matches!(parsed.upstream.as_slice(), [Frontend::Parse { .. }]), "{sql}");
            s.backend(Backend::ParseComplete);
            let mut next = (**f.ctx.policy.read().unwrap()).clone();
            next.columns.remove(&column);
            *f.ctx.policy.write().unwrap() = Arc::new(next);
            let out = s.frontend(Frontend::Bind { portal: "p".into(), statement: "s".into(), param_formats: vec![], params: vec![], result_formats: vec![] });
            assert!(out.upstream.is_empty(), "{sql}");
            assert!(out.client.iter().any(|m| matches!(m, Backend::ErrorResponse(fields) if *fields == nunya_error(NyCode::SchemaStale))), "{sql}");
        }
    }

    #[test]
    fn prepared_non_output_use_treatment_refresh_refuses() {
        let f = Fixture::new();
        let mut s = f.session();
        let parsed = s.frontend(Frontend::Parse { name: "s".into(), query: "SELECT count(value) FROM items".into(), param_types: vec![] });
        assert!(matches!(parsed.upstream.as_slice(), [Frontend::Parse { .. }]));
        s.backend(Backend::ParseComplete);
        let mut next = (**f.ctx.policy.read().unwrap()).clone();
        for treatment in next.columns.values_mut() { *treatment = Treatment::Masked { placeholder: None }; }
        *f.ctx.policy.write().unwrap() = Arc::new(next);
        let out = s.frontend(Frontend::Bind { portal: "p".into(), statement: "s".into(), param_formats: vec![], params: vec![], result_formats: vec![] });
        assert!(out.upstream.is_empty());
        assert!(out.client.iter().any(|m| matches!(m, Backend::ErrorResponse(fields) if *fields == nunya_error(NyCode::SchemaStale))));
    }

    #[test]
    fn policy_catalog_capture_retains_publication_guard_until_snapshot_read() {
        let f = Fixture::new();
        let revision = f.ctx.policy_revision.write().unwrap();
        let snapshot = f.ctx.snapshot.write().unwrap();
        let reader_ctx = f.ctx.clone();
        let reader = std::thread::spawn(move || reader_ctx.policy_and_snapshot().unwrap());
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        let reader_holds_policy = loop {
            match f.ctx.policy.try_write() {
                Err(std::sync::TryLockError::WouldBlock) => break true,
                Err(std::sync::TryLockError::Poisoned(_)) => break false,
                Ok(guard) => drop(guard),
            }
            if std::time::Instant::now() >= deadline { break false; }
            std::thread::yield_now();
        };
        drop(revision);
        let writer_ctx = f.ctx.clone();
        let (sent, received) = std::sync::mpsc::channel();
        let writer = std::thread::spawn(move || {
            let _policy = writer_ctx.policy.write().unwrap();
            sent.send(()).unwrap();
        });
        let blocked = matches!(received.recv_timeout(std::time::Duration::from_millis(200)), Err(std::sync::mpsc::RecvTimeoutError::Timeout));
        drop(snapshot);
        reader.join().unwrap();
        writer.join().unwrap();
        assert!(reader_holds_policy);
        assert!(blocked, "publication guard released before catalog capture completed");
    }

    #[test]
    fn unrelated_treatment_refresh_keeps_prepared_execution() {
        let f = Fixture::new();
        let mut s = f.session();
        s.frontend(Frontend::Parse { name: "s".into(), query: "SELECT value FROM items".into(), param_types: vec![] });
        s.backend(Backend::ParseComplete);
        let mut next = (**f.ctx.policy.read().unwrap()).clone();
        next.columns.insert(ColumnRef { schema: "public".into(), table: "other".into(), column: "value".into() }, Treatment::Masked { placeholder: None });
        *f.ctx.policy.write().unwrap() = Arc::new(next);
        let bound = s.frontend(Frontend::Bind { portal: "p".into(), statement: "s".into(), param_formats: vec![], params: vec![], result_formats: vec![] });
        assert!(matches!(bound.upstream.as_slice(), [Frontend::Bind { .. }]));
        s.backend(Backend::BindComplete);
        let out = s.frontend(Frontend::Execute { portal: "p".into(), max_rows: 0 });
        assert!(matches!(out.upstream.as_slice(), [Frontend::Execute { .. }]));
        assert!(out.client.is_empty());
    }

    #[test]
    fn statement_bound_before_domain_tightening_is_refused_at_execute() {
        for sql in ["SELECT value FROM items", "SELECT nunya_tokenize('secret', 'private')"] {
            let f = Fixture::new();
            *f.ctx.policy_revision.write().unwrap() = Some("R1".into());
            let mut s = f.session();
            s.frontend(Frontend::Parse { name: "s".into(), query: sql.into(), param_types: vec![] });
            let local = sql.contains("nunya_tokenize");
            if !local { s.backend(Backend::ParseComplete); }
            s.frontend(Frontend::Bind { portal: "p".into(), statement: "s".into(), param_formats: vec![], params: vec![], result_formats: vec![] });
            if !local { s.backend(Backend::BindComplete); }
            {
                let mut current = f.ctx.policy.write().unwrap();
                let old = current.clone();
                let mut next = (**current).clone();
                if let TokenizeSetting::On { limit_per_session, .. } = &mut next.domains.get_mut(&DomainName("secret".into())).unwrap().tokenize {
                    *limit_per_session = 0;
                }
                *current = Arc::new(next);
                assert!(f.ctx.fence.publish(&old, &current).is_some());
                *f.ctx.policy_revision.write().unwrap() = Some("R2".into());
            }
            // Local tokenize has no row release check, but extended Execute's
            // whole-policy admission check also refuses this local portal.
            let out = s.frontend(Frontend::Execute { portal: "p".into(), max_rows: 0 });
            assert!(out.upstream.is_empty());
            assert!(out.client.iter().any(|m| matches!(m, Backend::ErrorResponse(fields)
                if fields.iter().any(|(key, value)| *key == b'C' && value == "NY008"))));
            assert!(!out.client.iter().any(|m| matches!(m, Backend::DataRow(_))));
            s.frontend(Frontend::Sync);
            s.backend(Backend::ReadyForQuery(b'I'));
            assert!(f.identities.lock().unwrap().iter().all(|r| r.policy_revision.as_deref() == Some("R1")));
            f.identities.lock().unwrap().clear();
            let out = s.frontend(Frontend::Query(sql.into()));
            if local { assert!(out.client.iter().any(|m| matches!(m, Backend::ErrorResponse(_)))); }
            let records = f.identities.lock().unwrap();
            assert!(!records.is_empty());
            if local { assert!(matches!(records[0].outcome, Outcome::Refused(NyCode::TokenizeLimit))); }
            assert!(records.iter().all(|r| r.policy_revision.as_deref() == Some("R2")));
        }
    }

    #[test]
    fn identical_republish_keeps_epoch_and_bound_revision() {
        let f = Fixture::new();
        *f.ctx.policy_revision.write().unwrap() = Some("R1".into());
        let mut s = f.session();
        prepare_admission_portal(&mut s, "SELECT value FROM items");
        let epoch = f.ctx.fence.epoch();
        {
            let mut current = f.ctx.policy.write().unwrap();
            let old = current.clone();
            *current = Arc::new((**current).clone());
            assert_eq!(f.ctx.fence.publish(&old, &current), None);
            *f.ctx.policy_revision.write().unwrap() = Some("R2".into());
        }
        assert_eq!(f.ctx.fence.epoch(), epoch);
        let out = s.frontend(Frontend::Execute { portal: "p".into(), max_rows: 0 });
        assert!(matches!(out.upstream.as_slice(), [Frontend::Execute { .. }]));
        s.backend(Backend::RowDescription(vec![field()]));
        assert_revision_row(&revision_row(&mut s), true);
        s.release_done();
        s.backend(Backend::CommandComplete("SELECT 1".into()));
        let records = f.identities.lock().unwrap();
        assert!(!records.is_empty());
        assert!(records.iter().all(|r| r.policy_revision.as_deref() == Some("R1")));
    }

    #[tokio::test]
    async fn tls_reload_rotates_new_pg_handshakes_and_preserves_old() {
        use crate::config_reload::{tests::{Fixture as ConfigFixture, certificate, connector}, ConfigReloader, Transports};
        let config = ConfigFixture::new(); let a = certificate(&config);
        let hub = config.hub();
        let reload = ConfigReloader::new(config.load(&config.text), Arc::downgrade(&hub));
        let fixture = Fixture::new();
        let mut transports = Transports::new(&config.load(&config.text)).unwrap();
        *fixture.ctx.tls.write().unwrap() = transports.postgres.read().unwrap().clone();
        Arc::get_mut(&mut transports).unwrap().postgres = fixture.ctx.tls.clone();
        reload.attach_transports(transports.clone());
        let b = certificate(&config);
        let client = connector(&[a.clone(), b.clone()], false);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let ctx = fixture.ctx.clone();
        let server = tokio::spawn(async move {
            let mut sessions = tokio::task::JoinSet::new();
            loop {
                let (tcp, _) = listener.accept().await.unwrap();
                let ctx = ctx.clone(); sessions.spawn(async move { serve_tcp(tcp, ctx).await });
            }
        });
        async fn pg_connect(addr: std::net::SocketAddr, client: &tokio_rustls::TlsConnector)
            -> tokio_rustls::client::TlsStream<tokio::net::TcpStream> {
            let mut tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
            tcp.write_all(&[0,0,0,8,4,210,22,47]).await.unwrap();
            let mut answer = [0]; tcp.read_exact(&mut answer).await.unwrap(); assert_eq!(answer, *b"S");
            client.connect(rustls::pki_types::ServerName::try_from("localhost").unwrap(), tcp).await.unwrap()
        }
        let mut old = pg_connect(addr, &client).await;
        assert_eq!(old.get_ref().1.peer_certificates().unwrap()[0], a);
        reload.reload().await; assert_eq!(reload.last_reload(), "applied");
        let new = pg_connect(addr, &client).await;
        assert_eq!(new.get_ref().1.peer_certificates().unwrap()[0], b);
        old.write_all(&[0,0,0,8,4,210,22,48]).await.unwrap();
        let mut answer = [0]; old.read_exact(&mut answer).await.unwrap(); assert_eq!(answer, *b"N");
        server.abort(); let _ = server.await;
    }


    // Real sockets are essential: an in-memory duplex cannot reproduce Windows RSTs.
    async fn close_test_upstream(f: &mut Fixture) -> tokio::task::JoinHandle<()> {
        fake_query_upstream(f, |_| {
            vec![Backend::CommandComplete("SELECT 1".into()), Backend::ReadyForQuery(b'I')]
        }).await
    }

    async fn fake_query_upstream(
        f: &mut Fixture,
        response: fn(&str) -> Vec<Backend>,
    ) -> tokio::task::JoinHandle<()> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ctx = Arc::get_mut(&mut f.ctx).unwrap();
        ctx.upstream.host = "127.0.0.1".into();
        ctx.upstream.port = listener.local_addr().unwrap().port();
        ctx.upstream.connect_timeout_ms = 5000;
        let ctx = f.ctx.clone();
        tokio::spawn(async move {
            let mut tasks = tokio::task::JoinSet::new();
            loop {
                let (tcp, _) = listener.accept().await.unwrap();
                let ctx = ctx.clone();
                tasks.spawn(async move {
                    let mut wire = Agent { io: Box::new(tcp), buffer: BytesMut::new(), ctx };
                    assert!(matches!(wire.startup().await.unwrap(), StartupMessage::Startup { .. }));
                    wire.send(&[Backend::AuthenticationOk, Backend::ReadyForQuery(b'I')]).await.unwrap();
                    while let Ok(Frontend::Query(sql)) = wire.recv().await {
                        if wire.send(&response(&sql)).await.is_err() {
                            break;
                        }
                    }
                });
            }
        })
    }

    async fn close_test_recv(wire: &mut Agent) -> Backend {
        loop {
            if let Some(message) = codec::decode_backend(&mut wire.buffer).unwrap() {
                return message;
            }
            assert_ne!(wire.io.read_buf(&mut wire.buffer).await.unwrap(), 0, "unexpected EOF");
        }
    }

    async fn close_test_connect(f: &Fixture, id: SessionId, tls: bool)
        -> (Agent, tokio::task::JoinHandle<()>) {
        use postgres_protocol::authentication::sasl::{ChannelBinding, ScramSha256};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let ctx = f.ctx.clone();
        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            serve_tcp(tcp, ctx).await;
        });
        let mut tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
        let io: Box<dyn Transport> = if tls {
            use crate::config_reload::tests::{Fixture as ConfigFixture, certificate, connector};
            let config = ConfigFixture::new();
            let cert = certificate(&config);
            let transports = crate::config_reload::Transports::new(&config.load(&config.text)).unwrap();
            *f.ctx.tls.write().unwrap() = transports.postgres.read().unwrap().clone();
            tcp.write_all(&[0,0,0,8,4,210,22,47]).await.unwrap();
            assert_eq!(tcp.read_u8().await.unwrap(), b'S');
            Box::new(connector(&[cert], false).connect(
                rustls::pki_types::ServerName::try_from("localhost").unwrap(), tcp,
            ).await.unwrap())
        } else { Box::new(tcp) };
        let mut wire = Agent { io, buffer: BytesMut::new(), ctx: f.ctx.clone() };
        let mut startup = vec![0,0,0,0,0,3,0,0];
        startup.extend_from_slice(format!("user\0session:{}\0database\0app\0\0", id.to_hex()).as_bytes());
        let length = startup.len() as u32;
        startup[..4].copy_from_slice(&length.to_be_bytes());
        wire.io.write_all(&startup).await.unwrap();
        assert!(matches!(close_test_recv(&mut wire).await, Backend::AuthenticationSasl(_)));
        let mut scram = ScramSha256::new(b"login-secret", ChannelBinding::unsupported());
        let mut initial = b"SCRAM-SHA-256\0".to_vec();
        initial.extend_from_slice(&(scram.message().len() as i32).to_be_bytes());
        initial.extend_from_slice(scram.message());
        close_test_send(&mut wire, Frontend::Password(initial)).await;
        let Backend::AuthenticationSaslContinue(data) = close_test_recv(&mut wire).await else { panic!("expected SCRAM challenge") };
        scram.update(&data).unwrap();
        close_test_send(&mut wire, Frontend::Password(scram.message().to_vec())).await;
        let Backend::AuthenticationSaslFinal(data) = close_test_recv(&mut wire).await else { panic!("expected SCRAM final") };
        scram.finish(&data).unwrap();
        loop {
            match close_test_recv(&mut wire).await {
                Backend::ReadyForQuery(b'I') => break,
                Backend::AuthenticationOk | Backend::ParameterStatus(..) | Backend::BackendKeyData { .. } => {},
                _ => panic!("unexpected login response"),
            }
        }
        (wire, server)
    }

    async fn close_test_send(wire: &mut Agent, message: Frontend) {
        let mut bytes = BytesMut::new();
        codec::encode_frontend(&message, &mut bytes);
        wire.io.write_all(&bytes).await.unwrap();
        wire.io.flush().await.unwrap();
    }

    async fn close_test_error(wire: &mut Agent) {
        let Backend::ErrorResponse(error) = close_test_recv(wire).await else { panic!("expected final error") };
        assert!(error.iter().any(|(key, value)| *key == b'C' && value == "NY011"));
        assert!(wire.buffer.is_empty());
        assert_eq!(wire.io.read(&mut [0; 1]).await.unwrap(), 0);
    }

    async fn close_test_revoke(f: &Fixture, id: SessionId) {
        f.ctx.authority.revoke(id, crate::state::model::EndReason::AdminRevoked).unwrap();
        // This connection is the only login for this session; do not consume its error.
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            loop {
                let ended = f.lifecycle.lock().unwrap().iter().any(|record| {
                    record.kind == LifecycleKind::ConnectionEnded
                        && record.session.as_deref() == Some(id.to_hex().as_str())
                        && record.connection.is_some()
                        && record.reason == "revoked"
                });
                if ended { break; }
                tokio::task::yield_now().await;
            }
        }).await.unwrap();
    }

    #[tokio::test]
    async fn linger_tcp_revoke_write_before_read_sees_ny011() {
        let mut f = Fixture::new();
        let (id, _) = acquire_login(&f, "postgres");
        let fake = close_test_upstream(&mut f).await;
        let (mut wire, server) = close_test_connect(&f, id, false).await;
        close_test_revoke(&f, id).await;
        close_test_send(&mut wire, Frontend::Query("SELECT 1".into())).await;
        close_test_error(&mut wire).await;
        wire.io.shutdown().await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(4), server).await.unwrap().unwrap();
        fake.abort(); let _ = fake.await;
    }

    #[tokio::test]
    async fn linger_tls_revoke_write_before_read_sees_ny011() {
        let mut f = Fixture::new();
        let (id, _) = acquire_login(&f, "postgres");
        let fake = close_test_upstream(&mut f).await;
        let (mut wire, server) = close_test_connect(&f, id, true).await;
        close_test_revoke(&f, id).await;
        close_test_send(&mut wire, Frontend::Query("SELECT 1".into())).await;
        close_test_error(&mut wire).await;
        wire.io.shutdown().await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(4), server).await.unwrap().unwrap();
        fake.abort(); let _ = fake.await;
    }

    #[tokio::test]
    async fn linger_byte_cap_stops_without_eof() {
        let (mut client, mut server) = tokio::io::duplex(8192);
        let drain = tokio::spawn(async move { linger_close(&mut server).await; });
        // No EOF: exactly the cap must finish the drain without waiting for its timer.
        tokio::time::timeout(DRAIN_TIMEOUT / 2, async {
            client.write_all(&vec![0; DRAIN_BYTE_CAP]).await.unwrap();
            drain.await.unwrap();
        }).await.unwrap();
    }

    #[tokio::test]
    async fn linger_releases_admission_before_silent_drain() {
        use crate::config_reload::tests::Fixture as ConfigFixture;
        let config = ConfigFixture::new();
        let transports = crate::config_reload::Transports::new(
            &config.load(&config.text.replace("max_connections=256", "max_connections=1")),
        ).unwrap();
        let permit = transports.connections.acquire().unwrap();
        assert!(transports.connections.acquire().is_none());
        let f = Fixture::new();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut client = tokio::net::TcpStream::connect(listener.local_addr().unwrap()).await.unwrap();
        let (tcp, _) = listener.accept().await.unwrap();
        let server = tokio::spawn(serve_tcp_admitted(tcp, f.ctx.clone(), permit));
        // A malformed startup ends the session and sends a final error.
        client.write_all(&[0, 0, 0, 4]).await.unwrap();
        let replacement = tokio::time::timeout(DRAIN_TIMEOUT / 2, async {
            loop {
                if let Some(permit) = transports.connections.acquire() { break permit; }
                tokio::task::yield_now().await;
            }
        }).await.unwrap();
        assert!(!server.is_finished());
        drop(replacement);
        client.shutdown().await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn linger_late_read_first_preserves_pending_ny011() {
        let mut f = Fixture::new();
        let (id, _) = acquire_login(&f, "postgres");
        let fake = close_test_upstream(&mut f).await;
        let (mut wire, server) = close_test_connect(&f, id, false).await;
        close_test_revoke(&f, id).await;
        // Completion proves that the bounded drain has expired before the read.
        tokio::time::timeout(std::time::Duration::from_secs(4), server).await.unwrap().unwrap();
        close_test_error(&mut wire).await;
        fake.abort(); let _ = fake.await;
    }

    #[tokio::test]
    async fn linger_continuous_writer_is_bounded() {
        let mut f = Fixture::new();
        let (id, _) = acquire_login(&f, "postgres");
        let fake = close_test_upstream(&mut f).await;
        let (mut wire, server) = close_test_connect(&f, id, false).await;
        let start = tokio::time::Instant::now();
        close_test_revoke(&f, id).await;
        let writer = tokio::spawn(async move {
            loop {
                let _ = wire.io.write_all(&[0; 4096]).await;
                tokio::task::yield_now().await;
            }
        });
        tokio::time::timeout(std::time::Duration::from_secs(4), server).await.unwrap().unwrap();
        assert!(start.elapsed() < std::time::Duration::from_secs(4));
        writer.abort(); let _ = writer.await;
        fake.abort(); let _ = fake.await;
    }

    #[tokio::test]
    async fn linger_silent_client_is_bounded() {
        let mut f = Fixture::new();
        let (id, _) = acquire_login(&f, "postgres");
        let fake = close_test_upstream(&mut f).await;
        let (_wire, server) = close_test_connect(&f, id, false).await;
        let start = tokio::time::Instant::now();
        close_test_revoke(&f, id).await;
        tokio::time::timeout(std::time::Duration::from_secs(4), server).await.unwrap().unwrap();
        assert!(start.elapsed() >= DRAIN_TIMEOUT);
        assert!(start.elapsed() < std::time::Duration::from_secs(4));
        fake.abort(); let _ = fake.await;
    }

    #[tokio::test]
    async fn linger_prompt_eof_finishes_early() {
        let mut f = Fixture::new();
        let (id, _) = acquire_login(&f, "postgres");
        let fake = close_test_upstream(&mut f).await;
        let (mut wire, server) = close_test_connect(&f, id, false).await;
        close_test_revoke(&f, id).await;
        close_test_error(&mut wire).await;
        let start = tokio::time::Instant::now();
        wire.io.shutdown().await.unwrap();
        tokio::time::timeout(DRAIN_TIMEOUT / 2, server).await.unwrap().unwrap();
        assert!(start.elapsed() < DRAIN_TIMEOUT / 2);
        fake.abort(); let _ = fake.await;
    }

    #[tokio::test]
    async fn linger_other_connection_can_query_during_drain() {
        let mut f = Fixture::new();
        use crate::state::{authority::AcquireRequest, model::Verifier};
        use sha2::{Digest, Sha256};
        let (first, principal) = acquire_login(&f, "postgres");
        let second = f.ctx.authority.acquire_session(principal, AcquireRequest {
            route: f.ctx.route.clone(), mode: None, lifetime_ms: None, isolated: false,
            label: None, idempotency_key: "second-login".into(),
            verifier: Verifier(ScramVerifier::from_password("login-secret", b"test-salt", 4096)),
            secret_sha256: Sha256::digest(b"login-secret").into(),
        }).unwrap().id;
        let fake = close_test_upstream(&mut f).await;
        let (mut draining, first_server) = close_test_connect(&f, first, false).await;
        let (mut active, second_server) = close_test_connect(&f, second, false).await;
        close_test_revoke(&f, first).await;
        close_test_error(&mut draining).await;
        assert!(!first_server.is_finished());
        tokio::time::timeout(DRAIN_TIMEOUT / 2, async {
            close_test_send(&mut active, Frontend::Query("SELECT 1".into())).await;
            assert!(matches!(close_test_recv(&mut active).await, Backend::CommandComplete(_)));
            assert!(matches!(close_test_recv(&mut active).await, Backend::ReadyForQuery(b'I')));
        }).await.unwrap();
        assert!(!first_server.is_finished());
        draining.io.shutdown().await.unwrap();
        close_test_send(&mut active, Frontend::Terminate).await;
        active.io.shutdown().await.unwrap();
        first_server.await.unwrap(); second_server.await.unwrap();
        fake.abort(); let _ = fake.await;
    }

    #[tokio::test]
    async fn json_pgadapter_varchar_and_metadata_refusal_keep_connection_usable() {
        use crate::{json::filter::JsonMode, policy::{JsonLeafType, JsonPathTreatment, PathSegment}};
        let mut f = Fixture::new();
        let mut snapshot = (**f.ctx.snapshot.read().unwrap()).clone();
        snapshot.relations[0].columns[0].ty = ColumnType::Jsonb;
        *f.ctx.snapshot.write().unwrap() = Arc::new(snapshot);
        let mut policy = (**f.ctx.policy.read().unwrap()).clone();
        let mut filter = json_test_filter(JsonMode::Tokenize, false, true);
        let column = filter.rules[0].column.clone();
        let mut shown = filter.rules[0].clone();
        shown.path = vec![PathSegment::Key("locale".into())];
        shown.treatment = JsonPathTreatment::Show { ty: JsonLeafType::String };
        filter.rules.push(shown);
        policy.columns.insert(column.clone(), Treatment::Masked { placeholder: None });
        policy.json_filters.insert(column, filter);
        *f.ctx.policy.write().unwrap() = Arc::new(policy);
        let (id, _) = acquire_login(&f, "postgres");
        let fake = fake_query_upstream(&mut f, |sql| {
            let mut frames = Vec::new();
            if sql.contains("::text") {
                let fields = (0..2).map(|_| {
                    let mut field = field();
                    field.type_oid = if sql.contains("bad_type") { 20 } else { 1043 };
                    field
                }).collect();
                frames.push(Backend::RowDescription(fields));
                frames.push(Backend::DataRow(vec![
                    Some(br#""en-US""#.to_vec()), Some(br#""hidden""#.to_vec()),
                ]));
            }
            frames.extend([Backend::CommandComplete("SELECT 1".into()), Backend::ReadyForQuery(b'I')]);
            frames
        }).await;
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            let (mut wire, gateway) = close_test_connect(&f, id, false).await;
            for alias in ["good_type", "bad_type"] {
                close_test_send(&mut wire, Frontend::Query(format!(
                    "SELECT value->>'locale' AS {alias}, value->>'protected' FROM items"
                ))).await;
                if alias == "good_type" {
                    let Backend::RowDescription(fields) = close_test_recv(&mut wire).await else {
                        panic!("expected metadata");
                    };
                    assert!(fields.iter().all(|f| f.type_oid == 1043));
                    let Backend::DataRow(row) = close_test_recv(&mut wire).await else {
                        panic!("expected row");
                    };
                    assert_eq!(row[0].as_deref(), Some(b"en-US".as_slice()));
                    let token = row[1].as_deref().unwrap();
                    assert!(token.starts_with(b"tok_secret_"));
                    assert_ne!(token, b"hidden");
                    assert!(matches!(close_test_recv(&mut wire).await, Backend::CommandComplete(_)));
                } else {
                    assert!(matches!(close_test_recv(&mut wire).await, Backend::ErrorResponse(fields)
                        if fields.contains(&(b'C', "NY004".into()))));
                }
                assert!(matches!(close_test_recv(&mut wire).await, Backend::ReadyForQuery(b'I')));
                close_test_send(&mut wire, Frontend::Query("SELECT 1".into())).await;
                assert!(matches!(close_test_recv(&mut wire).await, Backend::CommandComplete(_)));
                assert!(matches!(close_test_recv(&mut wire).await, Backend::ReadyForQuery(b'I')));
            }
            close_test_send(&mut wire, Frontend::Terminate).await;
            wire.io.shutdown().await.unwrap();
            gateway.await.unwrap();
        }).await.unwrap();
        fake.abort();
        let _ = fake.await;
    }

    #[test]
    fn json_predicate_splice_guards_and_decrypts_literal() {
        let f = Fixture::new();
        let session = f.session();
        let mut snapshot = (**f.ctx.snapshot.read().unwrap()).clone();
        snapshot.relations[0].columns[0].ty = ColumnType::Jsonb;
        let mut policy = (**f.ctx.policy.read().unwrap()).clone();
        let filter = json_test_filter(crate::json::filter::JsonMode::Tokenize, false, true);
        policy.columns.insert(filter.rules[0].column.clone(), Treatment::Masked { placeholder: None });
        policy.json_filters.insert(filter.rules[0].column.clone(), filter);
        let domain = DomainName("secret".into());
        let token = f.ctx.codec.encode(&session.keys, &domain, &ColumnType::Text, None, &SqlValue::Text("O'Brien".into())).unwrap();
        let SqlValue::Text(token) = token else { panic!("text") };
        let sql = format!("SELECT 1 FROM items WHERE value->>'protected' = '{}'", token.replace("'", "''"));
        let statements = f.ctx.lowering.lower(&sql, &snapshot, &policy, &[]).unwrap();
        let plans = statements.iter().map(|s| ir::check::check(s, &policy, &snapshot).unwrap()).collect::<Vec<_>>();
        let rewritten = rewrite_literals(&sql, &statements, &plans, &policy, &session.keys, &f.ctx.codec).unwrap();
        assert!(rewritten.contains("CASE WHEN") && rewritten.contains("jsonb_typeof(value->'protected') = 'string'"));
        assert!(rewritten.ends_with(" = 'O''Brien'"));
        assert!(!rewritten.contains(&token));
        pg_query::parse(&rewritten, 0).unwrap();
    }

}
