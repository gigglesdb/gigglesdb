//! Pure fail-closed policy checks.
use super::*;
use crate::{
    error::Refusal,
    policy::{Policy, TokenizeSetting, Treatment},
    schema::Snapshot,
};
use std::collections::BTreeMap;
fn refusal(code: NyCode) -> Refusal {
    Refusal::new(code, "resolved statement violates policy")
}
fn ty(snapshot: &Snapshot, col: &ColumnRef) -> Result<ColumnType, Refusal> {
    snapshot
        .column(col)
        .map(|c| c.ty.clone())
        .ok_or_else(|| refusal(NyCode::NotAnalysable))
}
fn source(value: &ValueSource, snapshot: &Snapshot) -> Result<(), Refusal> {
    match value {
        ValueSource::Column {
            column,
            label: Label::Blind(_) | Label::Masked,
        } => {
            ty(snapshot, column)?;
        }
        ValueSource::Column {
            label: Label::Plain | Label::Tainted,
            ..
        }
        | ValueSource::Param(_)
        | ValueSource::Literal { .. }
        | ValueSource::Null
        | ValueSource::Computed(_) => {}
    }
    Ok(())
}
fn blind(
    value: &ValueSource,
    domain: &DomainName,
    ty: ColumnType,
    inputs: &mut Vec<InputRewrite>,
) -> Result<(), Refusal> {
    match value {
        ValueSource::Param(index) => inputs.push(InputRewrite::DecryptParam {
            index: *index,
            domain: domain.clone(),
            ty,
        }),
        ValueSource::Literal { span, .. } => inputs.push(InputRewrite::DecryptLiteral {
            span: span.clone(),
            domain: domain.clone(),
            ty,
        }),
        ValueSource::Column {
            label: Label::Blind(other),
            ..
        } => {
            if domain != other {
                return Err(refusal(NyCode::TokenOutsideDomain));
            }
        }
        ValueSource::Column {
            label: Label::Plain | Label::Masked | Label::Tainted,
            ..
        }
        | ValueSource::Computed(_)
        | ValueSource::Null => return Err(refusal(NyCode::BlindOperation)),
    }
    Ok(())
}
/// Check resolved information flows and build a deterministic rewrite plan.
pub fn check(
    stmt: &ResolvedStatement,
    policy: &Policy,
    snapshot: &Snapshot,
) -> Result<RewritePlan, Refusal> {
    let mut plan = RewritePlan {
        outputs: vec![],
        inputs: vec![],
        tokenize: None,
    };
    match &stmt.kind {
        StatementKind::Refused(code) => return Err(refusal(*code)),
        StatementKind::Begin
        | StatementKind::Commit
        | StatementKind::Rollback
        | StatementKind::Savepoint
        | StatementKind::ReleaseSavepoint
        | StatementKind::RollbackToSavepoint
        | StatementKind::SessionNoop { .. } => {
            if !stmt.uses.is_empty() || !stmt.writes.is_empty() || !stmt.outputs.is_empty() {
                return Err(refusal(NyCode::NotAnalysable));
            }
            return Ok(plan);
        }
        StatementKind::Tokenize { domain, arg } => {
            match policy.domains.get(domain).map(|s| &s.tokenize) {
                Some(TokenizeSetting::On { .. }) => {}
                Some(TokenizeSetting::Off) | None => {
                    return Err(refusal(NyCode::StatementNotAllowed));
                }
            }
            match arg {
                ValueSource::Param(_) | ValueSource::Literal { .. } => {}
                ValueSource::Column { .. } | ValueSource::Null | ValueSource::Computed(_) => {
                    return Err(refusal(NyCode::NotAnalysable));
                }
            }
            if !stmt.uses.is_empty() || !stmt.writes.is_empty() || !stmt.outputs.is_empty() {
                return Err(refusal(NyCode::NotAnalysable));
            }
            plan.tokenize = Some((domain.clone(), arg.clone()));
            return Ok(plan);
        }
        StatementKind::Select
        | StatementKind::Insert
        | StatementKind::Update
        | StatementKind::Delete => {}
    }
    for usage in &stmt.uses {
        match &usage.label {
            Label::Plain => continue,
            Label::Tainted => return Err(refusal(NyCode::BlindOperation)),
            Label::Masked => {
                ty(snapshot, &usage.column)?;
                if !matches!(
                    (&usage.kind, &usage.context),
                    (UseKind::Output, Context::SelectList | Context::Returning)
                ) {
                    return Err(refusal(NyCode::MaskedExpression));
                }
            }
            Label::Blind(domain) => {
                let column_ty = ty(snapshot, &usage.column)?;
                match &usage.kind {
                    UseKind::Output => {
                        if !matches!(usage.context, Context::SelectList | Context::Returning) {
                            return Err(refusal(NyCode::BlindOperation));
                        }
                    }
                    UseKind::Equality { other } => {
                        if !matches!(
                            usage.context,
                            Context::Where | Context::JoinOn | Context::Having
                        ) {
                            return Err(refusal(NyCode::BlindOperation));
                        }
                        source(other, snapshot)?;
                        blind(other, domain, column_ty, &mut plan.inputs)?;
                    }
                    UseKind::GroupBy => {
                        if usage.context != Context::GroupBy {
                            return Err(refusal(NyCode::BlindOperation));
                        }
                    }
                    UseKind::Count | UseKind::CountDistinct => {
                        if !matches!(
                            usage.context,
                            Context::SelectList | Context::Having | Context::Returning
                        ) {
                            return Err(refusal(NyCode::BlindOperation));
                        }
                    }
                    UseKind::IsNull => {
                        if matches!(usage.context, Context::SetClause | Context::Values) {
                            return Err(refusal(NyCode::BlindOperation));
                        }
                    }
                    UseKind::Comparison
                    | UseKind::Pattern
                    | UseKind::Order
                    | UseKind::Function { .. }
                    | UseKind::Cast
                    | UseKind::Aggregate { .. }
                    | UseKind::Other { .. } => return Err(refusal(NyCode::BlindOperation)),
                }
            }
        }
    }
    for write in &stmt.writes {
        match &write.label {
            Label::Tainted => return Err(refusal(NyCode::NotAnalysable)),
            Label::Blind(domain) => {
                let column_ty = ty(snapshot, &write.column)?;
                source(&write.value, snapshot)?;
                if write.value != ValueSource::Null {
                    blind(&write.value, domain, column_ty, &mut plan.inputs)?;
                }
            }
            Label::Masked | Label::Plain => {
                if write.label == Label::Masked {
                    ty(snapshot, &write.column)?;
                }
                source(&write.value, snapshot)?;
                match &write.value {
                    ValueSource::Param(_)
                    | ValueSource::Literal { .. }
                    | ValueSource::Null
                    | ValueSource::Column {
                        label: Label::Plain,
                        ..
                    }
                    | ValueSource::Computed(Label::Plain) => {}
                    ValueSource::Column { label, .. } | ValueSource::Computed(label) => {
                        let code = if write.label == Label::Masked {
                            NyCode::MaskedExpression
                        } else {
                            match label {
                                Label::Blind(_) => NyCode::TokenOutsideDomain,
                                Label::Masked => NyCode::MaskedExpression,
                                Label::Tainted => NyCode::BlindOperation,
                                Label::Plain => return Err(refusal(NyCode::NotAnalysable)),
                            }
                        };
                        return Err(refusal(code));
                    }
                }
            }
        }
    }
    let mut params: BTreeMap<u32, (DomainName, ColumnType)> = BTreeMap::new();
    let mut literals = vec![];
    for input in plan.inputs {
        match input {
            InputRewrite::DecryptParam { index, domain, ty } => {
                if let Some((previous_domain, previous_ty)) = params.get(&index) {
                    if previous_domain != &domain {
                        return Err(refusal(NyCode::TokenOutsideDomain));
                    }
                    if previous_ty != &ty {
                        return Err(refusal(NyCode::NotAnalysable));
                    }
                }
                params.insert(index, (domain, ty));
            }
            InputRewrite::DecryptLiteral { span, domain, ty } => literals.push((span, domain, ty)),
        }
    }
    for index in params.keys() {
        if stmt.plain_params.contains(index) {
            return Err(refusal(NyCode::BlindOperation));
        }
        if *index == 0 || *index > stmt.param_count {
            return Err(refusal(NyCode::NotAnalysable));
        }
    }
    literals.sort_by_key(|(span, _, _)| (span.start, span.end));
    let mut end = None;
    for (span, _, _) in &literals {
        if span.start >= span.end || end.is_some_and(|end| span.start < end) {
            return Err(refusal(NyCode::NotAnalysable));
        }
        end = Some(span.end);
    }
    plan.inputs = params
        .into_iter()
        .map(|(index, (domain, ty))| InputRewrite::DecryptParam { index, domain, ty })
        .collect();
    plan.inputs.extend(
        literals
            .into_iter()
            .map(|(span, domain, ty)| InputRewrite::DecryptLiteral { span, domain, ty }),
    );
    for output in &stmt.outputs {
        plan.outputs.push(match &output.label {
            Label::Plain => OutputRewrite::Pass,
            Label::Tainted => return Err(refusal(NyCode::BlindOperation)),
            Label::Blind(domain) => {
                let source = output
                    .source
                    .as_ref()
                    .ok_or_else(|| refusal(NyCode::BlindOperation))?;
                let ty = ty(snapshot, source)?;
                if policy.domain_of(source) != Some(domain) {
                    return Err(refusal(NyCode::NotAnalysable));
                }
                OutputRewrite::Tokenize {
                    domain: domain.clone(),
                    ty,
                }
            }
            Label::Masked => {
                let source = output
                    .source
                    .as_ref()
                    .ok_or_else(|| refusal(NyCode::MaskedExpression))?;
                ty(snapshot, source)?;
                match policy.treatment(source) {
                    Some(Treatment::Masked { placeholder }) => OutputRewrite::Mask {
                        placeholder: placeholder.clone(),
                    },
                    Some(Treatment::Blind { .. }) | Some(Treatment::Show) | None => {
                        return Err(refusal(NyCode::NotAnalysable));
                    }
                }
            }
        });
    }
    Ok(plan)
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        policy::{DomainSettings, ReadLogSink},
        schema::{Column, Relation, RelationKind},
    };
    #[test]
    fn persisted_links_allow_join_and_write_only_while_linked() {
        use crate::{
            policy::{
                bindings,
                effective::{Effective, effective_all},
                file::parse_policy_document,
            },
            schema::ColumnId,
            state::{model::RouteId, store::RedbStore},
            token::{self, ScopeKeys, StandardCodec, TokenCodec, keys::RootKey},
            value::SqlValue,
        };
        let path = std::path::PathBuf::from(std::env::var_os("TEMP").unwrap()).join(format!(
            "ir-links-{}",
            bindings::generate_domain_name().unwrap().0
        ));
        std::fs::create_dir_all(&path).unwrap();
        let store = RedbStore::open(&path, &RootKey::from_hex(&"01".repeat(32)).unwrap()).unwrap();
        let route = RouteId::new("ir-links").unwrap();
        let (_, snapshot) = fixture();
        let email = col("customers", "email");
        let address = col("outbox", "to_address");
        let snapshot = snapshot.with_column_ids(BTreeMap::from([
            (
                email.clone(),
                ColumnId::Pg {
                    relation_oid: 1,
                    attnum: 1,
                },
            ),
            (
                address.clone(),
                ColumnId::Pg {
                    relation_oid: 2,
                    attnum: 1,
                },
            ),
        ]));
        let plain_doc = "version = 2\ndefault = 'tokenize'\n";
        let linked_doc = format!(
            "{plain_doc}[[link]]\nfields = ['public.customers.email', 'public.outbox.to_address']\n"
        );
        let keys = ScopeKeys::new([1; 32]);
        let mut old_token = None;
        for (phase, source) in [plain_doc, linked_doc.as_str(), plain_doc]
            .into_iter()
            .enumerate()
        {
            let doc = parse_policy_document(source).unwrap();
            let (records, _) =
                bindings::refresh_membership(&store, &route, &doc, &snapshot).unwrap();
            let labels: BTreeMap<_, _> = effective_all(&doc, &snapshot, &records)
                .into_iter()
                .map(|(column, (effective, _))| {
                    let label = match effective {
                        Effective::Blind { domain }
                        | Effective::BlindPrivate { domain }
                        | Effective::BlindLinked { domain } => Label::Blind(domain),
                        Effective::Show => Label::Plain,
                        Effective::Masked { .. } => Label::Masked,
                    };
                    (column, label)
                })
                .collect();
            let mut join = stmt();
            join.uses.push(ColumnUse {
                column: email.clone(),
                label: labels[&email].clone(),
                context: Context::JoinOn,
                kind: UseKind::Equality {
                    other: column("outbox", "to_address", labels[&address].clone()),
                },
            });
            let mut write = stmt();
            write.kind = StatementKind::Insert;
            write.writes.push(WriteTarget {
                column: address.clone(),
                label: labels[&address].clone(),
                value: column("customers", "email", labels[&email].clone()),
            });
            for statement in [join, write] {
                let result = check(&statement, &doc.policy, &snapshot);
                if phase == 1 {
                    assert!(result.is_ok());
                } else {
                    assert_eq!(result.unwrap_err().code, NyCode::TokenOutsideDomain);
                }
            }
            if phase == 1 {
                let Label::Blind(shared) = &labels[&email] else {
                    panic!("expected blind label")
                };
                let SqlValue::Text(token) = StandardCodec
                    .encode(
                        &keys,
                        shared,
                        &ColumnType::Text,
                        None,
                        &SqlValue::Text("example".into()),
                    )
                    .unwrap()
                else {
                    panic!("expected text token")
                };
                old_token = Some(token);
            }
        }
        let retired = bindings::load_retired(&store, &route).unwrap();
        assert_eq!(
            token::check_not_retired(&retired, old_token.as_ref().unwrap())
                .unwrap_err()
                .code,
            NyCode::TokenOutsideDomain
        );
        drop(store);
        std::fs::remove_dir_all(path).unwrap();
    }

    fn domain(name: &str) -> DomainName {
        DomainName(name.into())
    }
    fn col(table: &str, column: &str) -> ColumnRef {
        ColumnRef {
            schema: "public".into(),
            table: table.into(),
            column: column.into(),
        }
    }
    fn label(name: &str) -> Label {
        Label::Blind(domain(name))
    }
    fn fixture() -> (Policy, Snapshot) {
        let mut columns = BTreeMap::new();
        columns.insert(
            col("customers", "email"),
            Treatment::Blind {
                domain: domain("email"),
            },
        );
        columns.insert(
            col("customers", "phone"),
            Treatment::Blind {
                domain: domain("phone"),
            },
        );
        columns.insert(
            col("customers", "country"),
            Treatment::Masked {
                placeholder: Some("hidden".into()),
            },
        );
        columns.insert(
            col("outbox", "to_address"),
            Treatment::Blind {
                domain: domain("email"),
            },
        );
        let policy = Policy {
            columns,
            domains: ["email", "phone"]
                .into_iter()
                .map(|name| {
                    (
                        domain(name),
                        DomainSettings {
                            tokenize: TokenizeSetting::On {
                                limit_per_session: 0,
                                limit_per_principal: 1,
                                principal_window: std::time::Duration::from_secs(86400),
                            },
                            range: None,
                        },
                    )
                })
                .collect(),
            snapshot_max_age_ms: 100,
            readlog: ReadLogSink::Stdout,
        };
        let relations = [
            (
                "customers",
                vec![
                    ("id", ColumnType::Int4),
                    ("email", ColumnType::Text),
                    ("country", ColumnType::Text),
                    ("phone", ColumnType::Text),
                ],
            ),
            (
                "outbox",
                vec![
                    ("to_address", ColumnType::Text),
                    ("template", ColumnType::Text),
                ],
            ),
            (
                "notes",
                vec![("id", ColumnType::Int4), ("body", ColumnType::Text)],
            ),
        ]
        .into_iter()
        .map(|(name, columns)| Relation {
            schema: "public".into(),
            name: name.into(),
            kind: RelationKind::Table,
            columns: columns
                .into_iter()
                .map(|(name, ty)| Column {
                    name: name.into(),
                    ty,
                    nullable: true,
                    generated: None,
                    default: None,
                })
                .collect(),
            view_definition: None,
            primary_key: vec![],
        })
        .collect();
        (
            policy,
            Snapshot::new(relations, vec![], vec!["public".into()], 0),
        )
    }
    fn stmt() -> ResolvedStatement {
        ResolvedStatement {
            canonical_sql: None,
            kind: StatementKind::Select,
            outputs: vec![],
            uses: vec![],
            writes: vec![],
            param_count: 5,
            plain_params: vec![],
            source_span: Span { start: 0, end: 100 },
        }
    }
    fn usage(kind: UseKind, context: Context) -> ColumnUse {
        ColumnUse {
            column: col("customers", "email"),
            label: label("email"),
            kind,
            context,
        }
    }
    fn literal(start: usize, end: usize) -> ValueSource {
        ValueSource::Literal {
            span: Span { start, end },
            text: "token".into(),
        }
    }
    fn column(table: &str, name: &str, label: Label) -> ValueSource {
        ValueSource::Column {
            column: col(table, name),
            label,
        }
    }
    fn run(s: &ResolvedStatement) -> Result<RewritePlan, Refusal> {
        let (p, snap) = fixture();
        check(s, &p, &snap)
    }
    fn denied(s: &ResolvedStatement, code: NyCode) {
        assert_eq!(run(s).unwrap_err().code, code);
    }
    fn contexts() -> Vec<Context> {
        vec![
            Context::SelectList,
            Context::Where,
            Context::JoinOn,
            Context::Having,
            Context::GroupBy,
            Context::OrderBy,
            Context::Returning,
            Context::Values,
            Context::SetClause,
            Context::Other,
        ]
    }
    #[test]
    fn statement_kinds_and_transactions() {
        for kind in [
            StatementKind::Select,
            StatementKind::Insert,
            StatementKind::Update,
            StatementKind::Delete,
        ] {
            let mut s = stmt();
            s.kind = kind;
            assert!(run(&s).is_ok());
        }
        for kind in [
            StatementKind::Begin,
            StatementKind::Commit,
            StatementKind::Rollback,
            StatementKind::Savepoint,
            StatementKind::ReleaseSavepoint,
            StatementKind::RollbackToSavepoint,
            StatementKind::SessionNoop { tag: "SET" },
        ] {
            let mut s = stmt();
            s.kind = kind;
            assert_eq!(
                run(&s).unwrap(),
                RewritePlan {
                    inputs: vec![],
                    outputs: vec![],
                    tokenize: None
                }
            );
            s.uses.push(usage(UseKind::IsNull, Context::Where));
            denied(&s, NyCode::NotAnalysable);
            s.uses.clear();
            s.writes.push(WriteTarget {
                column: col("notes", "body"),
                label: Label::Plain,
                value: ValueSource::Null,
            });
            denied(&s, NyCode::NotAnalysable);
            s.writes.clear();
            s.outputs.push(OutputColumn {
                name: "x".into(),
                label: Label::Plain,
                source: None,
                ty: None,
            });
            denied(&s, NyCode::NotAnalysable);
        }
        for code in NyCode::ALL {
            let mut s = stmt();
            s.kind = StatementKind::Refused(code);
            denied(&s, code);
        }
    }
    #[test]
    fn tokenize_on_off_missing_and_argument_rules() {
        let (mut p, snap) = fixture();
        for arg in [ValueSource::Param(1), literal(0, 3)] {
            let mut s = stmt();
            s.kind = StatementKind::Tokenize {
                domain: domain("email"),
                arg: arg.clone(),
            };
            assert_eq!(
                check(&s, &p, &snap).unwrap().tokenize,
                Some((domain("email"), arg))
            );
            s.outputs.push(OutputColumn {
                name: "x".into(),
                label: Label::Plain,
                source: None,
                ty: None,
            });
            assert_eq!(
                check(&s, &p, &snap).unwrap_err().code,
                NyCode::NotAnalysable
            );
        }
        for arg in [
            ValueSource::Null,
            ValueSource::Computed(Label::Plain),
            column("notes", "body", Label::Plain),
        ] {
            let mut s = stmt();
            s.kind = StatementKind::Tokenize {
                domain: domain("email"),
                arg,
            };
            assert_eq!(
                check(&s, &p, &snap).unwrap_err().code,
                NyCode::NotAnalysable
            );
        }
        let mut s = stmt();
        s.kind = StatementKind::Tokenize {
            domain: domain("email"),
            arg: ValueSource::Param(1),
        };
        p.domains.get_mut(&domain("email")).unwrap().tokenize = TokenizeSetting::Off;
        assert_eq!(
            check(&s, &p, &snap).unwrap_err().code,
            NyCode::StatementNotAllowed
        );
        p.domains.clear();
        assert_eq!(
            check(&s, &p, &snap).unwrap_err().code,
            NyCode::StatementNotAllowed
        );
    }
    #[test]
    fn blind_use_context_matrix() {
        for context in contexts() {
            for (kind, allowed) in [
                (
                    UseKind::Output,
                    matches!(context, Context::SelectList | Context::Returning),
                ),
                (
                    UseKind::Equality {
                        other: ValueSource::Param(1),
                    },
                    matches!(context, Context::Where | Context::JoinOn | Context::Having),
                ),
                (UseKind::GroupBy, context == Context::GroupBy),
                (
                    UseKind::Count,
                    matches!(
                        context,
                        Context::SelectList | Context::Having | Context::Returning
                    ),
                ),
                (
                    UseKind::CountDistinct,
                    matches!(
                        context,
                        Context::SelectList | Context::Having | Context::Returning
                    ),
                ),
                (
                    UseKind::IsNull,
                    !matches!(context, Context::SetClause | Context::Values),
                ),
            ] {
                let mut s = stmt();
                s.uses.push(usage(kind, context.clone()));
                if allowed {
                    assert!(run(&s).is_ok());
                } else {
                    denied(&s, NyCode::BlindOperation);
                }
            }
        }
    }
    #[test]
    fn blind_forbidden_operations() {
        for kind in [
            UseKind::Comparison,
            UseKind::Pattern,
            UseKind::Order,
            UseKind::Function { name: "f".into() },
            UseKind::Cast,
            UseKind::Aggregate { name: "sum".into() },
            UseKind::Other { what: "unknown" },
        ] {
            for context in contexts() {
                let mut s = stmt();
                s.uses.push(usage(kind.clone(), context));
                denied(&s, NyCode::BlindOperation);
            }
        }
    }
    #[test]
    fn equality_counterpart_matrix() {
        for other in [
            ValueSource::Param(1),
            literal(4, 8),
            column("outbox", "to_address", label("email")),
        ] {
            let mut s = stmt();
            s.uses
                .push(usage(UseKind::Equality { other }, Context::JoinOn));
            assert!(run(&s).is_ok());
        }
        for other in [
            column("customers", "phone", label("phone")),
            column("notes", "body", Label::Plain),
            column("customers", "country", Label::Masked),
            column("notes", "body", Label::Tainted),
            ValueSource::Computed(Label::Plain),
            ValueSource::Computed(label("email")),
            ValueSource::Computed(Label::Masked),
            ValueSource::Computed(Label::Tainted),
            ValueSource::Null,
        ] {
            let code = if matches!(
                &other,
                ValueSource::Column {
                    label: Label::Blind(_),
                    ..
                }
            ) {
                NyCode::TokenOutsideDomain
            } else {
                NyCode::BlindOperation
            };
            let mut s = stmt();
            s.uses
                .push(usage(UseKind::Equality { other }, Context::Where));
            denied(&s, code);
        }
    }
    #[test]
    fn plain_tainted_and_masked_uses() {
        let mut s = stmt();
        let mut u = usage(UseKind::Pattern, Context::Where);
        u.label = Label::Plain;
        s.uses.push(u.clone());
        assert!(run(&s).is_ok());
        u.label = Label::Tainted;
        s.uses = vec![u];
        denied(&s, NyCode::BlindOperation);
        for context in contexts() {
            for kind in [
                UseKind::Output,
                UseKind::IsNull,
                UseKind::Equality {
                    other: ValueSource::Param(1),
                },
            ] {
                let allowed = kind == UseKind::Output
                    && matches!(context, Context::SelectList | Context::Returning);
                s.uses = vec![ColumnUse {
                    column: col("customers", "country"),
                    label: Label::Masked,
                    kind,
                    context: context.clone(),
                }];
                if allowed {
                    assert!(run(&s).is_ok());
                } else {
                    denied(&s, NyCode::MaskedExpression);
                }
            }
        }
    }
    #[test]
    fn blind_write_matrix() {
        for value in [
            ValueSource::Param(1),
            literal(2, 8),
            ValueSource::Null,
            column("customers", "email", label("email")),
        ] {
            let mut s = stmt();
            s.kind = StatementKind::Insert;
            s.writes.push(WriteTarget {
                column: col("outbox", "to_address"),
                label: label("email"),
                value,
            });
            assert!(run(&s).is_ok());
        }
        for value in [
            column("customers", "phone", label("phone")),
            column("notes", "body", Label::Plain),
            column("customers", "country", Label::Masked),
            column("notes", "body", Label::Tainted),
            ValueSource::Computed(Label::Plain),
            ValueSource::Computed(label("email")),
            ValueSource::Computed(Label::Masked),
            ValueSource::Computed(Label::Tainted),
        ] {
            let code = if matches!(
                &value,
                ValueSource::Column {
                    label: Label::Blind(_),
                    ..
                }
            ) {
                NyCode::TokenOutsideDomain
            } else {
                NyCode::BlindOperation
            };
            let mut s = stmt();
            s.writes.push(WriteTarget {
                column: col("outbox", "to_address"),
                label: label("email"),
                value,
            });
            denied(&s, code);
        }
    }
    #[test]
    fn plain_masked_and_tainted_write_matrix() {
        for target in [Label::Plain, Label::Masked] {
            let target_col = if target == Label::Plain {
                col("notes", "body")
            } else {
                col("customers", "country")
            };
            for value in [
                ValueSource::Param(1),
                literal(0, 2),
                ValueSource::Null,
                column("notes", "body", Label::Plain),
                ValueSource::Computed(Label::Plain),
            ] {
                let mut s = stmt();
                s.writes.push(WriteTarget {
                    column: target_col.clone(),
                    label: target.clone(),
                    value,
                });
                assert!(run(&s).is_ok());
            }
            for value_label in [label("email"), Label::Masked, Label::Tainted] {
                let source_col = if value_label == Label::Masked {
                    col("customers", "country")
                } else {
                    col("customers", "email")
                };
                for value in [
                    ValueSource::Column {
                        column: source_col,
                        label: value_label.clone(),
                    },
                    ValueSource::Computed(value_label.clone()),
                ] {
                    let code = if target == Label::Masked || value_label == Label::Masked {
                        NyCode::MaskedExpression
                    } else if value_label == Label::Tainted {
                        NyCode::BlindOperation
                    } else {
                        NyCode::TokenOutsideDomain
                    };
                    let mut s = stmt();
                    s.writes.push(WriteTarget {
                        column: target_col.clone(),
                        label: target.clone(),
                        value,
                    });
                    denied(&s, code);
                }
            }
        }
        let mut s = stmt();
        s.writes.push(WriteTarget {
            column: col("notes", "body"),
            label: Label::Tainted,
            value: ValueSource::Null,
        });
        denied(&s, NyCode::NotAnalysable);
    }
    #[test]
    fn parameters_deduplicated_sorted_and_conflicts_refused() {
        let mut s = stmt();
        for index in [3, 1, 3, 2] {
            s.uses.push(usage(
                UseKind::Equality {
                    other: ValueSource::Param(index),
                },
                Context::Where,
            ));
        }
        assert_eq!(
            run(&s).unwrap().inputs,
            [1, 2, 3].map(|index| InputRewrite::DecryptParam {
                index,
                domain: domain("email"),
                ty: ColumnType::Text
            })
        );
        s.plain_params.push(1);
        denied(&s, NyCode::BlindOperation);
        s.plain_params.clear();
        for index in [0, 6] {
            let mut invalid = stmt();
            invalid.uses.push(usage(
                UseKind::Equality {
                    other: ValueSource::Param(index),
                },
                Context::Where,
            ));
            denied(&invalid, NyCode::NotAnalysable);
        }
        s.uses.push(ColumnUse {
            column: col("customers", "phone"),
            label: label("phone"),
            kind: UseKind::Equality {
                other: ValueSource::Param(1),
            },
            context: Context::Where,
        });
        denied(&s, NyCode::TokenOutsideDomain);
        s.uses.pop();
        let (p, mut snap) = fixture();
        snap.relations[1].columns[0].ty = ColumnType::Int4;
        s.writes.push(WriteTarget {
            column: col("outbox", "to_address"),
            label: label("email"),
            value: ValueSource::Param(1),
        });
        assert_eq!(
            check(&s, &p, &snap).unwrap_err().code,
            NyCode::NotAnalysable
        );
    }
    #[test]
    fn literals_sorted_after_params_and_overlap_refused() {
        let mut s = stmt();
        for value in [
            literal(20, 25),
            ValueSource::Param(2),
            literal(3, 8),
            ValueSource::Param(1),
        ] {
            s.uses
                .push(usage(UseKind::Equality { other: value }, Context::Where));
        }
        assert_eq!(
            run(&s).unwrap().inputs,
            vec![
                InputRewrite::DecryptParam {
                    index: 1,
                    domain: domain("email"),
                    ty: ColumnType::Text
                },
                InputRewrite::DecryptParam {
                    index: 2,
                    domain: domain("email"),
                    ty: ColumnType::Text
                },
                InputRewrite::DecryptLiteral {
                    span: Span { start: 3, end: 8 },
                    domain: domain("email"),
                    ty: ColumnType::Text
                },
                InputRewrite::DecryptLiteral {
                    span: Span { start: 20, end: 25 },
                    domain: domain("email"),
                    ty: ColumnType::Text
                }
            ]
        );
        for (a, b) in [(7, 9), (3, 8), (5, 5), (9, 2)] {
            let mut bad = s.clone();
            bad.uses.push(usage(
                UseKind::Equality {
                    other: literal(a, b),
                },
                Context::Where,
            ));
            denied(&bad, NyCode::NotAnalysable);
        }
    }
    #[test]
    fn output_rewrites_and_missing_sources() {
        let mut s = stmt();
        s.uses.push(usage(UseKind::Output, Context::SelectList));
        s.outputs = vec![
            OutputColumn {
                name: "id".into(),
                label: Label::Plain,
                source: None,
                ty: None,
            },
            OutputColumn {
                name: "email".into(),
                label: label("email"),
                source: Some(col("customers", "email")),
                ty: Some(ColumnType::Int4),
            },
            OutputColumn {
                name: "country".into(),
                label: Label::Masked,
                source: Some(col("customers", "country")),
                ty: None,
            },
        ];
        assert_eq!(
            run(&s).unwrap().outputs,
            vec![
                OutputRewrite::Pass,
                OutputRewrite::Tokenize {
                    domain: domain("email"),
                    ty: ColumnType::Text
                },
                OutputRewrite::Mask {
                    placeholder: Some("hidden".into())
                }
            ]
        );
        let (mut p, snap) = fixture();
        p.columns.insert(
            col("customers", "country"),
            Treatment::Masked { placeholder: None },
        );
        assert_eq!(
            check(&s, &p, &snap).unwrap().outputs[2],
            OutputRewrite::Mask { placeholder: None }
        );
        for (label, source, code) in [
            (Label::Tainted, None, NyCode::BlindOperation),
            (label("email"), None, NyCode::BlindOperation),
            (Label::Masked, None, NyCode::MaskedExpression),
            (
                label("phone"),
                Some(col("customers", "email")),
                NyCode::NotAnalysable,
            ),
            (
                Label::Masked,
                Some(col("customers", "email")),
                NyCode::NotAnalysable,
            ),
            (
                Label::Masked,
                Some(col("notes", "body")),
                NyCode::NotAnalysable,
            ),
        ] {
            s.outputs = vec![OutputColumn {
                name: "x".into(),
                label,
                source,
                ty: None,
            }];
            denied(&s, code);
        }
    }
    #[test]
    fn every_blind_and_masked_column_must_exist() {
        let (p, mut snap) = fixture();
        snap.relations.clear();
        for label in [label("email"), Label::Masked] {
            for kind in [
                UseKind::Output,
                UseKind::Count,
                UseKind::GroupBy,
                UseKind::IsNull,
            ] {
                let mut s = stmt();
                s.uses.push(ColumnUse {
                    column: col("customers", "email"),
                    label: label.clone(),
                    kind,
                    context: Context::SelectList,
                });
                assert_eq!(
                    check(&s, &p, &snap).unwrap_err().code,
                    NyCode::NotAnalysable
                );
            }
            let mut s = stmt();
            s.writes.push(WriteTarget {
                column: col("customers", "email"),
                label: label.clone(),
                value: ValueSource::Null,
            });
            assert_eq!(
                check(&s, &p, &snap).unwrap_err().code,
                NyCode::NotAnalysable
            );
            s.writes.clear();
            s.outputs.push(OutputColumn {
                name: "x".into(),
                label,
                source: Some(col("customers", "email")),
                ty: None,
            });
            assert_eq!(
                check(&s, &p, &snap).unwrap_err().code,
                NyCode::NotAnalysable
            );
        }
        let mut s = stmt();
        s.uses.push(usage(
            UseKind::Equality {
                other: column("missing", "x", label("email")),
            },
            Context::Where,
        ));
        denied(&s, NyCode::NotAnalysable);
    }
}
