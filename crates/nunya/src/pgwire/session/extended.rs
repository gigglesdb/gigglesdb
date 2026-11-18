//! Ordered extended-query request and response handling.
use super::*;
use crate::value::ColumnType;
#[derive(Clone)]
pub(super) struct Prepared {
    empty_query: bool,
    generation: [u8; 16],
    plan: RewritePlan,
    columns: Vec<crate::schema::ColumnRef>,
    epoch: u64,
    param_count: u32,
    kind: StatementKind,
    param_types: Vec<u32>,
    policy: Arc<Policy>,
    policy_revision: Option<String>,
    sql: String,
    rewritten: String,
    token_ty: Option<ColumnType>,
}
#[derive(Clone)]
pub(super) struct Portal {
    generation: [u8; 16],
    prepared: Prepared,
    formats: Vec<i16>,
    token_value: Option<SqlValue>,
    exhausted: bool,
}
pub(super) enum Pending {
    Local(Actions),
    Ack(u8, String, [u8; 16]),
    Hidden(u8),
    RefusalError,
    Describe(Prepared, bool, Vec<i16>),
    Execute(Active, Vec<i16>),
    Sync,
}
fn oid_type(oid: u32) -> Option<ColumnType> {
    use ColumnType::*;
    Some(match oid {
        25 => Text,
        1043 => Varchar(None),
        1042 => Bpchar(None),
        21 => Int2,
        23 => Int4,
        20 => Int8,
        2950 => Uuid,
        1082 => Date,
        1114 => Timestamp,
        1184 => Timestamptz,
        16 => Bool,
        1700 => Numeric,
        17 => Bytea,
        114 => Json,
        3802 => Jsonb,
        _ => return None,
    })
}
impl Prepared {
    fn check_treatments(&self, current: &Arc<Policy>) -> Result<(), Refusal> {
        if current.retired_domains != self.policy.retired_domains
            || (!Arc::ptr_eq(current, &self.policy)
            && self.columns.iter().any(|column| current.treatment(column) != self.policy.treatment(column)
                || current.json_filter(column) != self.policy.json_filter(column)))
        {
            return Err(failure(NyCode::SchemaStale));
        }
        Ok(())
    }
    fn local(&self) -> bool {
        self.empty_query || self.plan.tokenize.is_some() || matches!(self.kind, StatementKind::SessionNoop { .. })
    }
}
impl Session {
    fn drain_local(&mut self, out: &mut Actions) {
        while matches!(self.pending.front(), Some(Pending::Local(_))) {
            if let Some(Pending::Local(local)) = self.pending.pop_front() {
                if self.status == b'T'
                    && local
                        .client
                        .iter()
                        .any(|m| matches!(m, Backend::ErrorResponse(_)))
                {
                    self.aborted = true;
                }
                out.client.extend(local.client);
                out.close |= local.close;
            }
        }
    }
    fn local(&mut self, local: Actions) -> Actions {
        self.pending.push_back(Pending::Local(local));
        let mut out = Actions::default();
        self.drain_local(&mut out);
        out
    }
    /// Refuse a local extended request and wait for Sync unless the session must close.
    pub(super) fn ext_refuse(&mut self, sql: &str, code: NyCode, aborted: bool) -> Actions {
        self.skip = true;
        self.rollback_on_sync |= self.extended_execute_forwarded;
        let mut local = self.refuse(sql, code, false);
        if aborted && !local.close {
            local.client[0] = Backend::ErrorResponse(redact_error(&[
                (b'S', "ERROR".into()), (b'C', "25P02".into()),
            ]));
        }
        let buffered = !self.pending.is_empty();
        let mut out = self.local(local);
        if buffered {
            out.upstream.push(Frontend::Flush);
        }
        out
    }
    fn prepare(&mut self, sql: &str, param_types: Vec<u32>) -> Result<Prepared, Refusal> {
        let epoch = self.ctx.fence.epoch();
        let (policy, policy_revision, snapshot) = self.ctx.policy_and_snapshot()?;
        self.statement_revision = policy_revision.clone();
        let snapshot = self.pin_snapshot(snapshot)?;
        if (self.ctx.clock)().saturating_sub(snapshot.taken_at_unix_ms) > policy.snapshot_max_age_ms
        {
            return Err(failure(NyCode::SchemaStale));
        }
        let types = param_types.iter().map(|o| oid_type(*o)).collect::<Vec<_>>();
        let mut statements = self.ctx.lowering.lower(sql, &snapshot, &policy, &types)?;
        let empty_query = statements.is_empty() && sql.trim().is_empty();
        if empty_query {
            if !param_types.is_empty() { return Err(failure(NyCode::NotAnalysable)); }
            statements.push(ResolvedStatement {
                canonical_sql: None,
                kind: StatementKind::SessionNoop { tag: "" },
                outputs: vec![], uses: vec![], writes: vec![], param_count: 0,
                plain_params: vec![], source_span: ir::Span { start: 0, end: sql.len() },
            });
        }
        if statements.len() != 1 {
            return Err(failure(NyCode::NotAnalysable));
        }
        let statement = &statements[0];
        if self.aborted && !recovery(&statement.kind) {
            return Err(failure(NyCode::StatementNotAllowed));
        }
        let plan = ir::check::check(statement, &policy, &snapshot)?;
        let rewritten = if matches!(statement.kind, StatementKind::SessionNoop { .. }) {
            String::new()
        } else {
            rewrite_literals(
                sql,
                &statements,
                std::slice::from_ref(&plan),
                &policy,
                &self.keys,
                &self.ctx.codec,
            )?
        };
        let token_ty = plan.tokenize.as_ref().and_then(|(domain, _)| {
            policy.columns.iter().find_map(|(c, t)| match t {
                crate::policy::Treatment::Blind { domain: d } if d == domain => {
                    snapshot.column(c).map(|c| c.ty.clone())
                }
                _ => None,
            })
        });
        let mut param_types = param_types;
        let param_count = statement
            .param_count
            .max(u32::try_from(param_types.len()).map_err(|_| protocol())?);
        param_types.resize(param_count as usize, 0);
        if let Some((_, ValueSource::Param(index))) = &plan.tokenize {
            let ty = token_ty.as_ref().ok_or_else(protocol)?;
            let slot = param_types
                .get_mut((*index as usize).checked_sub(1).ok_or_else(protocol)?)
                .ok_or_else(protocol)?;
            if *slot != 0
                && Some(*slot) != ty.pg_oid()
                && !(ty.is_textual() && matches!(*slot, 25 | 1042 | 1043))
            {
                return Err(failure(NyCode::NotAnalysable));
            }
            *slot = ty.pg_oid().ok_or_else(protocol)?;
        }
        Ok(Prepared {
            epoch,
            empty_query,
            generation: random::<16>()?,
            plan,
            columns: statement.outputs.iter().filter_map(|output| output.source.clone())
                .chain(statement.uses.iter().map(|usage| usage.column.clone()))
                .chain(statement.uses.iter().filter_map(|usage| match &usage.kind {
                    UseKind::Equality { other: ValueSource::Column { column, .. } } => Some(column.clone()),
                    _ => None,
                }))
                .chain(statement.writes.iter().map(|write| write.column.clone()))
                .chain(statement.writes.iter().filter_map(|write| match &write.value {
                    ValueSource::Column { column, .. } => Some(column.clone()),
                    _ => None,
                }))
                .collect(),
            param_count,
            kind: statement.kind.clone(),
            param_types,
            policy,
            policy_revision,
            sql: sql.into(),
            rewritten,
            token_ty,
        })
    }
    pub(super) fn extended_frontend(&mut self, msg: Frontend) -> Actions {
        if self.pending.iter().any(|p| matches!(p, Pending::Sync)) {
            let mut bytes = BytesMut::new();
            if matches!(msg, Frontend::Refused(_)) {
                bytes.resize(5, 0);
            } else {
                codec::encode_frontend(&msg, &mut bytes);
            }
            self.extended_deferred_bytes = self.extended_deferred_bytes.saturating_add(bytes.len());
            if self.extended_deferred.len() >= 64
                || self.extended_deferred_bytes > codec::MAX_MESSAGE
            {
                return Actions {
                    client: vec![Backend::ErrorResponse(nunya_error(
                        NyCode::ProtocolNotAllowed,
                    ))],
                    close: true,
                    ..Actions::default()
                };
            }
            self.extended_deferred.push_back(msg);
            return Actions::default();
        }
        if self.skip && !matches!(msg, Frontend::Sync) {
            return Actions::default();
        }
        let sql = match &msg {
            Frontend::Parse { query, .. } => query.clone(),
            Frontend::Bind { statement, .. } => self
                .prepared
                .get(statement)
                .map(|p| p.sql.clone())
                .unwrap_or_default(),
            Frontend::Execute { portal, .. } => self
                .portals
                .get(portal)
                .map(|p| p.prepared.sql.clone())
                .unwrap_or_default(),
            _ => String::new(),
        };
        self.statement_revision = match &msg {
            Frontend::Bind { statement, .. } => self.prepared.get(statement).and_then(|p| p.policy_revision.clone()),
            Frontend::Execute { portal, .. } => self.portals.get(portal).and_then(|p| p.prepared.policy_revision.clone()),
            Frontend::Describe { kind: b'S', name } => self.prepared.get(name).and_then(|p| p.policy_revision.clone()),
            Frontend::Describe { kind: b'P', name } => self.portals.get(name).and_then(|p| p.prepared.policy_revision.clone()),
            _ => None,
        };
        let result = self.ext_request(msg);
        match result {
            Ok(out) => out,
            Err((_, e, aborted)) => self.ext_refuse(&sql, e.code, aborted),
        }
    }
    fn ext_request(&mut self, msg: Frontend) -> Result<Actions, (String, Refusal, bool)> {
        let mut out = Actions::default();
        let mut stale_execute = false;
        let result: Result<(), Refusal> = (|| {
            match msg {
                Frontend::Parse {
                    name,
                    query,
                    param_types,
                } => {
                    let p = match self.prepare(&query, param_types.clone()) {
                        Ok(p) => p,
                        Err(e) => return Err(e),
                    };
                    if p.local() {
                        self.prepared.insert(name, p);
                        out = self.local(Actions {
                            client: vec![Backend::ParseComplete],
                            ..Actions::default()
                        });
                    } else {
                        let rewritten = p.rewritten.clone();
                        let generation = p.generation;
                        self.prepared.insert(name.clone(), p);
                        self.pending
                            .push_back(Pending::Ack(b'1', name.clone(), generation));
                        out.upstream.push(Frontend::Parse {
                            name,
                            query: rewritten,
                            param_types,
                        });
                    }
                }
                Frontend::Bind {
                    portal,
                    statement,
                    param_formats,
                    mut params,
                    result_formats,
                } => {
                    let p = self
                        .prepared
                        .get(&statement)
                        .cloned()
                        .ok_or_else(|| failure(NyCode::NotAnalysable))?;
                    let (current, _) = self.ctx.policy_snapshot()?;
                    p.check_treatments(&current)?;
                    if params.len() != p.param_count as usize {
                        return Err(failure(NyCode::NotAnalysable));
                    }
                    for i in 0..params.len() {
                        format_at(&param_formats, i, params.len())?;
                    }
                    let output_count = if p.plan.tokenize.is_some() {
                        1
                    } else {
                        p.plan.outputs.len()
                    };
                    for i in 0..output_count {
                        format_at(&result_formats, i, output_count)?;
                    }
                    rewrite_bind(
                        &mut params,
                        &param_formats,
                        &p.param_types,
                        &p.plan,
                        &current,
                        &self.keys,
                        &self.ctx.codec,
                    )?;
                    let token_value = if let Some((_, arg)) = &p.plan.tokenize {
                        let ty = p.token_ty.as_ref().ok_or_else(protocol)?;
                        Some(match arg {
                            ValueSource::Literal { text, .. } => {
                                values::decode_text(ty, text.as_bytes())?
                            }
                            ValueSource::Param(index) => {
                                let i = (*index as usize).checked_sub(1).ok_or_else(protocol)?;
                                let cell = params.get(i).ok_or_else(protocol)?;
                                if cell.is_none() {
                                    SqlValue::Null
                                } else {
                                    let bytes = cell.as_ref().ok_or_else(protocol)?;
                                    if format_at(&param_formats, i, params.len())? == 0 {
                                        values::decode_text(ty, bytes)?
                                    } else {
                                        values::decode_binary(ty, bytes)?
                                    }
                                }
                            }
                            _ => return Err(failure(NyCode::NotAnalysable)),
                        })
                    } else {
                        None
                    };
                    let local = p.local();
                    let generation = random::<16>()?;
                    self.portals.insert(
                        portal.clone(),
                        Portal {
                            generation,
                            prepared: p,
                            formats: result_formats.clone(),
                            token_value,
                            exhausted: false,
                        },
                    );
                    if local {
                        out = self.local(Actions {
                            client: vec![Backend::BindComplete],
                            ..Actions::default()
                        });
                    } else {
                        self.pending
                            .push_back(Pending::Ack(b'2', portal.clone(), generation));
                        out.upstream.push(Frontend::Bind {
                            portal,
                            statement,
                            param_formats,
                            params,
                            result_formats,
                        });
                    }
                }
                Frontend::Describe { kind, name } => {
                    let (p, formats) = match kind {
                        b'S' => (
                            self.prepared
                                .get(&name)
                                .cloned()
                                .ok_or_else(|| failure(NyCode::NotAnalysable))?,
                            vec![],
                        ),
                        b'P' => {
                            let portal = self
                                .portals
                                .get(&name)
                                .ok_or_else(|| failure(NyCode::NotAnalysable))?;
                            (portal.prepared.clone(), portal.formats.clone())
                        }
                        _ => return Err(protocol()),
                    };
                    if p.local() {
                        let mut client = vec![];
                        if kind == b'S' {
                            client.push(Backend::ParameterDescription(p.param_types.clone()));
                        }
                        if matches!(p.kind, StatementKind::SessionNoop { .. }) {
                            client.push(Backend::NoData);
                        } else {
                            client.push(Backend::RowDescription(vec![
                                self.tokenize_field(&p, &formats)?,
                            ]));
                        }
                        out = self.local(Actions {
                            client,
                            ..Actions::default()
                        });
                    } else {
                        self.pending
                            .push_back(Pending::Describe(p, kind == b'S', formats));
                        out.upstream.push(Frontend::Describe { kind, name });
                    }
                }
                Frontend::Execute { portal, max_rows } => {
                    // Admit at processing time so deferred executes use current authority.
                    let mode = self.admit()?;
                    let portal_state = self
                        .portals
                        .get(&portal)
                        .cloned()
                        .ok_or_else(|| failure(NyCode::NotAnalysable))?;
                    let p = portal_state.prepared;
                    if !mode.permits_write() && writes(&p.kind) {
                        return Err(failure(NyCode::StatementNotAllowed));
                    }
                    let (current, _, snapshot) = self.ctx.policy_and_snapshot()?;
                    if crate::policy::fence::tightens(&p.policy, &current) {
                        stale_execute = true;
                        return Err(failure(NyCode::SchemaStale));
                    }
                    let snapshot = self.pin_snapshot(snapshot)?;
                    if (self.ctx.clock)().saturating_sub(snapshot.taken_at_unix_ms)
                        > p.policy.snapshot_max_age_ms
                    {
                        stale_execute = true;
                        return Err(failure(NyCode::SchemaStale));
                    }
                    if self.aborted && !recovery(&p.kind) {
                        return Err(failure(NyCode::StatementNotAllowed));
                    }
                    if let StatementKind::SessionNoop { tag } = p.kind {
                        // No ParameterStatus change: application_name remains the startup value.
                        self.ctx
                            .readlog
                            .write(&self.record(if p.empty_query { "" } else { &p.sql }, Outcome::Allowed))?;
                        self.ctx
                            .metrics
                            .statements_allowed
                            .fetch_add(1, Ordering::Relaxed);
                        out = self.local(Actions {
                            client: vec![if p.empty_query { Backend::EmptyQueryResponse } else { Backend::CommandComplete(tag.into()) }],
                            ..Actions::default()
                        });
                    } else if p.plan.tokenize.is_some() {
                        if portal_state.exhausted {
                            out = self.local(Actions {
                                client: vec![Backend::CommandComplete("SELECT 0".into())],
                                ..Actions::default()
                            });
                            return Ok(());
                        }
                        let (domain, _) = p.plan.tokenize.as_ref().ok_or_else(protocol)?;
                        let ty = p.token_ty.as_ref().ok_or_else(protocol)?;
                        let settings = p.policy.domains.get(domain).ok_or_else(protocol)?;
                        self.record_tokenization(&p.policy, domain)?;
                        self.ctx
                            .metrics
                            .statements_allowed
                            .fetch_add(1, Ordering::Relaxed);
                        let mut record = self.record(&p.sql, Outcome::Allowed);
                        let token = self.ctx.codec.encode(
                            &self.keys,
                            domain,
                            ty,
                            settings.range.as_ref(),
                            portal_state.token_value.as_ref().ok_or_else(protocol)?,
                        )?;
                        let text = if matches!(token, SqlValue::Null) {
                            vec![]
                        } else {
                            values::encode_text(&token)?
                        };
                        record.rows = 1;
                        if !matches!(token, SqlValue::Null) {
                            record
                                .tokens_released
                                .push(String::from_utf8(text.clone()).map_err(|_| protocol())?);
                        }
                        self.ctx.readlog.write(&record)?;
                        let bytes = if format_at(&portal_state.formats, 0, 1)? == 0 {
                            text
                        } else if matches!(token, SqlValue::Null) {
                            vec![]
                        } else {
                            values::encode_binary(ty, &token)?
                        };
                        if let Some(saved) = self.portals.get_mut(&portal) {
                            saved.exhausted = true;
                        }
                        out = self.local(Actions {
                            client: vec![
                                Backend::DataRow(vec![if matches!(token, SqlValue::Null) {
                                    None
                                } else {
                                    Some(bytes)
                                }]),
                                Backend::CommandComplete("SELECT 1".into()),
                            ],
                            ..Actions::default()
                        });
                    } else {
                        let record = self.record(&p.sql, Outcome::Allowed);
                        self.ctx.readlog.write(&record)?;
                        self.ctx
                            .metrics
                            .statements_allowed
                            .fetch_add(1, Ordering::Relaxed);
                        let mut execute_portal = portal;
                        let mut cleanup = None;
                        if self.aborted && matches!(p.kind, StatementKind::Commit) {
                            let name = format!(
                                "nunya_rollback_{}",
                                data_encoding::HEXLOWER.encode(&random::<16>()?)
                            );
                            self.pending.push_back(Pending::Hidden(b'1'));
                            self.pending.push_back(Pending::Hidden(b'2'));
                            out.upstream.push(Frontend::Parse {
                                name: name.clone(),
                                query: "ROLLBACK".into(),
                                param_types: vec![],
                            });
                            out.upstream.push(Frontend::Bind {
                                portal: name.clone(),
                                statement: name.clone(),
                                param_formats: vec![],
                                params: vec![],
                                result_formats: vec![],
                            });
                            cleanup = Some(name.clone());
                            execute_portal = name;
                        }
                        self.segment_forwarded_write |=
                            writes(&p.kind) || matches!(p.kind, StatementKind::Commit);
                        self.pending.push_back(Pending::Execute(
                            Active {
                                epoch: p.epoch,
                                plan: p.plan,
                                kind: p.kind,
                                record,
                                policy: p.policy,
                                described: false,
                            },
                            portal_state.formats,
                        ));
                        out.upstream.push(Frontend::Execute {
                            portal: execute_portal,
                            max_rows,
                        });
                        self.extended_execute_forwarded = true;
                        if let Some(name) = cleanup {
                            for kind in *b"PS" {
                                out.upstream.push(Frontend::Close {
                                    kind,
                                    name: name.clone(),
                                });
                                self.pending.push_back(Pending::Hidden(b'3'));
                            }
                        }
                    }
                }
                Frontend::Close { kind, name } => {
                    let local = match kind {
                        b'S' => self.prepared.remove(&name).is_some_and(|p| p.local()),
                        b'P' => self
                            .portals
                            .remove(&name)
                            .is_some_and(|p| p.prepared.local()),
                        _ => return Err(protocol()),
                    };
                    if local {
                        out = self.local(Actions {
                            client: vec![Backend::CloseComplete],
                            ..Actions::default()
                        });
                    } else {
                        self.pending
                            .push_back(Pending::Ack(b'3', String::new(), [0; 16]));
                        out.upstream.push(Frontend::Close { kind, name });
                    }
                }
                Frontend::Sync => {
                    self.skip = false;
                    if self.rollback_on_sync {
                        self.prepared.remove("");
                        self.pending.push_back(Pending::RefusalError);
                        out.upstream.push(Frontend::Parse {
                            name: String::new(),
                            query: "SELECT nunya_refused_statement(".into(),
                            param_types: vec![],
                        });
                    }
                    self.extended_execute_forwarded = false;
                    self.rollback_on_sync = false;
                    self.pending.push_back(Pending::Sync);
                    out.upstream.push(Frontend::Sync);
                }
                Frontend::Flush => out.upstream.push(Frontend::Flush),
                Frontend::Query(_)
                | Frontend::Password(_)
                | Frontend::Refused(_)
                | Frontend::Terminate => return Err(protocol()),
            }
            Ok(())
        })();
        result
            .map(|()| out)
            .map_err(|e| (String::new(), e, self.aborted && !stale_execute))
    }
    fn tokenize_field(&self, p: &Prepared, formats: &[i16]) -> Result<FieldDescription, Refusal> {
        let ty = p.token_ty.as_ref().ok_or_else(protocol)?;
        Ok(FieldDescription {
            name: "nunya_tokenize".into(),
            table_oid: 0,
            column_attr: 0,
            type_oid: ty.pg_oid().ok_or_else(protocol)?,
            type_size: -1,
            type_modifier: -1,
            format: format_at(formats, 0, 1)?,
        })
    }
    pub(super) fn extended_backend(&mut self, msg: Backend) -> Actions {
        let result = self.ext_response(msg);
        match result {
            Ok(client) => {
                let mut out = Actions {
                    client,
                    ..Actions::default()
                };
                self.drain_local(&mut out);
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
            },
        }
    }
    pub(super) fn drain_deferred(&mut self, out: &mut Actions) {
        while !out.close
            && !self.waiting
            && !self.pending.iter().any(|p| matches!(p, Pending::Sync))
        {
            let Some(request) = self.extended_deferred.pop_front() else {
                self.extended_deferred_bytes = 0;
                break;
            };
            let next = self.frontend(request);
            out.client.extend(next.client);
            out.upstream.extend(next.upstream);
            out.close |= next.close;
        }
    }
    fn ext_response(&mut self, msg: Backend) -> Result<Vec<Backend>, Refusal> {
        match msg {
            Backend::NoticeResponse(fields) => {
                return Ok(vec![Backend::NoticeResponse(redact_error(&fields))]);
            }
            Backend::ParameterStatus(n, v) => {
                return Ok(if allowed_status(&n) {
                    vec![Backend::ParameterStatus(n, v)]
                } else {
                    vec![]
                });
            }
            Backend::Refused(b'A') => return Ok(vec![]),
            Backend::ErrorResponse(fields) => {
                // Terminating errors are intercepted by Upstream::recv.
                self.segment_known_failure = true;
                if matches!(self.pending.front(), Some(Pending::Sync)) {
                    // Sync can fail while committing the implicit transaction. Its
                    // ReadyForQuery still ends this segment; later work is deferred.
                    self.rollback_on_sync = false;
                    self.extended_execute_forwarded = false;
                    return Ok(vec![Backend::ErrorResponse(redact_error(&fields))]);
                }
                let hidden = matches!(self.pending.front(), Some(Pending::RefusalError));
                if !matches!(
                    self.pending.front(),
                    Some(
                        Pending::Ack(_, _, _)
                            | Pending::Hidden(_)
                            | Pending::Describe(_, _, _)
                            | Pending::Execute(_, _)
                            | Pending::RefusalError
                    )
                ) {
                    return Err(protocol());
                }
                if let Some(Pending::Execute(a, _)) = self.pending.front() {
                    self.ctx.readlog.write(&a.record)?;
                }
                // An earlier upstream failure already aborts this segment; only the
                // synthetic Parse's own error belongs to the hidden response slot.
                self.rollback_on_sync = false;
                self.extended_execute_forwarded = false;
                while !matches!(self.pending.front(), Some(Pending::Sync) | None) {
                    match self.pending.pop_front() {
                        Some(Pending::Ack(b'1', name, generation)) => {
                            if self
                                .prepared
                                .get(&name)
                                .is_some_and(|p| p.generation == generation)
                            {
                                self.prepared.remove(&name);
                            }
                        }
                        Some(Pending::Ack(b'2', name, generation)) => {
                            if self
                                .portals
                                .get(&name)
                                .is_some_and(|p| p.generation == generation)
                            {
                                self.portals.remove(&name);
                            }
                        }
                        Some(
                            Pending::Local(_)
                            | Pending::Hidden(_)
                            | Pending::RefusalError
                            | Pending::Describe(_, _, _)
                            | Pending::Execute(_, _)
                            | Pending::Ack(_, _, _)
                            | Pending::Sync,
                        )
                        | None => {}
                    }
                }
                self.skip = true;
                return Ok(if hidden {
                    vec![]
                } else {
                    vec![Backend::ErrorResponse(redact_error(&fields))]
                });
            }
            _ => {}
        }
        let pending = self.pending.front_mut().ok_or_else(protocol)?;
        let mut done = true;
        let response = match (pending, msg) {
            (Pending::Hidden(b'1'), Backend::ParseComplete)
            | (Pending::Hidden(b'2'), Backend::BindComplete)
            | (Pending::Hidden(b'3'), Backend::CloseComplete) => {
                self.pending.pop_front();
                return Ok(vec![]);
            }
            (Pending::Ack(b'1', _, _), Backend::ParseComplete) => Backend::ParseComplete,
            (Pending::Ack(b'2', _, _), Backend::BindComplete) => Backend::BindComplete,
            (Pending::Ack(b'3', _, _), Backend::CloseComplete) => Backend::CloseComplete,
            (Pending::Describe(_, parameter, _), Backend::ParameterDescription(oids))
                if *parameter =>
            {
                *parameter = false;
                done = false;
                Backend::ParameterDescription(oids)
            }
            (Pending::Describe(p, false, formats), Backend::RowDescription(mut fields)) => {
                rewrite_fields(&mut fields, &p.plan)?;
                for (i, field) in fields.iter().enumerate() {
                    if field.format != format_at(formats, i, fields.len())? {
                        return Err(failure(NyCode::NotAnalysable));
                    }
                }
                Backend::RowDescription(fields)
            }
            (Pending::Describe(p, false, _), Backend::NoData) if p.plan.outputs.is_empty() => {
                Backend::NoData
            }
            (Pending::Execute(a, formats), Backend::DataRow(row)) => {
                if row.len() != a.plan.outputs.len() {
                    return Err(failure(NyCode::NotAnalysable));
                }
                check_release(&self.ctx, &self.release, a)?;
                let (row, tokens) = rewrite_row(
                    row,
                    &a.plan,
                    formats,
                    &a.policy,
                    &self.keys,
                    &self.ctx.codec,
                )
                .map_err(|_| failure(NyCode::BlindOperation))?;
                a.record.rows += 1;
                a.record.tokens_released.extend(tokens);
                done = false;
                Backend::DataRow(row)
            }
            (Pending::Execute(a, formats), Backend::RowDescription(mut fields)) => {
                if a.described {
                    return Err(protocol());
                }
                a.described = true;
                rewrite_fields(&mut fields, &a.plan)?;
                for (i, field) in fields.iter().enumerate() {
                    if field.format != format_at(formats, i, fields.len())? {
                        return Err(failure(NyCode::NotAnalysable));
                    }
                }
                done = false;
                Backend::RowDescription(fields)
            }
            (Pending::Execute(a, _), Backend::CommandComplete(tag)) => {
                self.ctx.readlog.write(&a.record)?;
                if self.segment_forwarded_write
                    && matches!(a.kind, StatementKind::Commit | StatementKind::Rollback)
                {
                    self.segment_unresolved_before_failure = true;
                }
                let commit = self.aborted && matches!(a.kind, StatementKind::Commit);
                if matches!(a.kind, StatementKind::Begin) {
                    self.status = b'T';
                }
                if recovery(&a.kind) {
                    self.aborted = false;
                }
                Backend::CommandComplete(if commit { "ROLLBACK".into() } else { tag })
            }
            (Pending::Execute(a, _), Backend::PortalSuspended) => {
                self.ctx.readlog.write(&a.record)?;
                Backend::PortalSuspended
            }
            (Pending::Sync, Backend::ReadyForQuery(status))
                if matches!(status, b'I' | b'T' | b'E') =>
            {
                self.segment_forwarded_write = false;
                self.segment_known_failure = false;
                self.segment_unresolved_before_failure = false;
                self.status = status;
                if status == b'E' {
                    self.aborted = true;
                }
                self.skip = false;
                if status == b'I' {
                    self.aborted = false;
                }
                self.ready()
            }
            _ => return Err(protocol()),
        };
        if done {
            self.pending.pop_front();
        }
        Ok(vec![response])
    }
}
/// Validate upstream output types and clear modifiers for rewritten fields.
pub(super) fn rewrite_fields(fields: &mut [FieldDescription], plan: &RewritePlan) -> Result<(), Refusal> {
    if fields.len() != plan.outputs.len() {
        return Err(failure(NyCode::NotAnalysable));
    }
    validate_json_fields(fields, plan)?;
    for (field, output) in fields.iter_mut().zip(&plan.outputs) {
        if !matches!(output, OutputRewrite::Pass) {
            field.type_modifier = -1;
        }
    }
    Ok(())
}

#[cfg(test)]
mod treatment_tests {
    use super::*;

    #[test]
    fn cached_dependency_absent_to_present_refuses() {
        let fixture = super::super::tests::Fixture::new();
        let mut planned = (**fixture.ctx.policy.read().unwrap()).clone();
        let column = planned.columns.keys().next().unwrap().clone();
        planned.columns.clear();
        let mut current = planned.clone();
        current.columns.insert(column.clone(), crate::policy::Treatment::Show);
        let prepared = Prepared {
            epoch: fixture.ctx.fence.epoch(),
            empty_query: false,
            generation: [0; 16],
            plan: RewritePlan { outputs: vec![], inputs: vec![], tokenize: None },
            columns: vec![column],
            param_count: 0,
            kind: StatementKind::Select,
            param_types: vec![],
            policy: Arc::new(planned),
            policy_revision: None,
            sql: String::new(),
            rewritten: String::new(),
            token_ty: None,
        };
        assert_eq!(prepared.check_treatments(&Arc::new(current)).unwrap_err().code, NyCode::SchemaStale);
        let mut retired = (*prepared.policy).clone();
        retired.retired_domains.insert(crate::policy::DomainName("retired_generated".into()));
        assert_eq!(prepared.check_treatments(&Arc::new(retired)).unwrap_err().code, NyCode::SchemaStale);
        let mut current = (*prepared.policy).clone();
        let filter = super::super::tests::json_test_filter(crate::json::filter::JsonMode::Show, false, false);
        current.json_filters.insert(prepared.columns[0].clone(), filter.clone());
        assert_eq!(prepared.check_treatments(&Arc::new(current.clone())).unwrap_err().code, NyCode::SchemaStale);
        let mut prepared = prepared;
        prepared.policy = Arc::new(current.clone());
        assert!(prepared.check_treatments(&Arc::new(current.clone())).is_ok());
        current.json_filters.get_mut(&prepared.columns[0]).unwrap().rules.clear();
        assert_eq!(prepared.check_treatments(&Arc::new(current)).unwrap_err().code, NyCode::SchemaStale);

    }
}
