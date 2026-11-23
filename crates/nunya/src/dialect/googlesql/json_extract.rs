//! Strict GoogleSQL JSON extraction and guarded policy leaf lowering.
use super::*;
use crate::json::extract::{self, Extraction, Step};
fn denied() -> Refusal {
    Refusal::new(NyCode::MaskedExpression, "JSON extraction violates policy")
}
/// A classified direct base-column extraction.
pub(super) struct Extract {
    source: ColumnRef,
    column_prefix: String,
    qualified: bool,
    treatment: Extraction,
    path: Vec<Step>,
    text: bool,
    /// Exact source boundaries of the original function call.
    pub(super) span: Span,
    query: String,
    guards: Vec<String>,
    keep_empty: bool,
}
impl Extract {
    pub(super) fn check_clause_aliases(&self, outputs: &[Output]) -> Result<()> {
        // A SELECT alias can shadow the input name in a grouping or sort key.
        // Reuse the input binding only when an unqualified alias denotes the
        // same unchanged column.
        if outputs.iter().any(|out| {
            out.ir.name.to_lowercase() == self.column_prefix
                && (self.qualified || out.ir.source.as_ref() != Some(&self.source))
        }) {
            return Err(denied());
        }
        Ok(())
    }
}

fn offset(sql: &str, location: sqlparser::tokenizer::Location) -> Result<usize> {
    let (mut line, mut column) = (1, 1);
    for (i, c) in sql.char_indices() {
        if line == location.line && column == location.column {
            return Ok(i);
        }
        if c == '\n' {
            line += 1;
            column = 1;
        } else {
            column += 1;
        }
    }
    Err(denied())
}
fn ws(bytes: &[u8], i: &mut usize) {
    while bytes.get(*i).is_some_and(u8::is_ascii_whitespace) {
        *i += 1;
    }
}
fn quoted(sql: &str, i: &mut usize) -> Result<String> {
    let b = sql.as_bytes();
    if b.get(*i) != Some(&b'\'') || b.get(*i..*i + 3) == Some(b"'''") {
        return Err(denied());
    }
    *i += 1;
    let start = *i;
    while let Some(c) = b.get(*i) {
        match c {
            b'\\' | 0 => return Err(denied()),
            b'\'' => {
                let value = sql.get(start..*i).ok_or_else(denied)?.to_owned();
                *i += 1;
                return Ok(value);
            }
            _ => *i += 1,
        }
    }
    Err(denied())
}
fn path(text: &str) -> Result<Vec<Step>> {
    let b = text.as_bytes();
    if b.first() != Some(&b'$') {
        return Err(denied());
    }
    let mut i = 1;
    let mut steps = vec![];
    while i < b.len() {
        match b[i] {
            b'.' => {
                i += 1;
                let key = if b.get(i) == Some(&b'"') {
                    i += 1;
                    let start = i;
                    while let Some(c) = b.get(i) {
                        if *c == b'"' {
                            break;
                        }
                        if *c == b'\\' || *c == 0 {
                            return Err(denied());
                        }
                        i += 1;
                    }
                    if b.get(i) != Some(&b'"') {
                        return Err(denied());
                    }
                    let key = text[start..i].to_owned();
                    i += 1;
                    key
                } else {
                    let start = i;
                    while b
                        .get(i)
                        .is_some_and(|c| c.is_ascii_alphanumeric() || *c == b'_')
                    {
                        i += 1;
                    }
                    if i == start {
                        return Err(denied());
                    }
                    text[start..i].to_owned()
                };
                // Spanner may interpret a digit-only key as an array index.
                if !key.is_empty() && key.bytes().all(|b| b.is_ascii_digit()) {
                    return Err(denied());
                }
                steps.push(Step::Key(key));
            }
            b'[' => {
                i += 1;
                let start = i;
                while b.get(i).is_some_and(u8::is_ascii_digit) {
                    i += 1;
                }
                if i == start || b.get(i) != Some(&b']') {
                    return Err(denied());
                }
                steps.push(Step::Index(text[start..i].parse().map_err(|_| denied())?));
                i += 1;
            }
            _ => return Err(denied()),
        }
    }
    Ok(steps)
}
/// Locate the equality literal closest to its guarded counterpart.
pub(super) fn literal_span(sql: &str, value: &str, boundary: usize, before: bool) -> Result<Span> {
    let tokens = Tokenizer::new(&BigQueryDialect {}, sql)
        .tokenize_with_location()
        .map_err(|_| denied())?;
    let mut found: Option<Span> = None;
    for token in tokens {
        if matches!(&token.token, Token::SingleQuotedString(s) if s == value) {
            let start = offset(sql, token.span.start)?;
            if (before && start >= boundary) || (!before && start < boundary) {
                continue;
            }
            if found.as_ref().is_some_and(|previous| {
                if before {
                    previous.start > start
                } else {
                    previous.start < start
                }
            }) {
                continue;
            }
            let mut end = start;
            if quoted(sql, &mut end)? != value {
                return Err(denied());
            }
            found = Some(Span { start, end });
        }
    }
    found.ok_or_else(denied)
}

impl State<'_> {
    /// Recognize and classify the restricted extraction call grammar.
    pub(super) fn json_extraction(&self, n: &a::Expr, scope: &Scope) -> Result<Option<Extract>> {
        let a::Expr::Function(f) = n else {
            return Ok(None);
        };
        if f.name.0.len() != 1
            || !matches!(
                f.name.0[0].value.to_ascii_lowercase().as_str(),
                "json_value" | "json_query"
            )
        {
            return Ok(None);
        }
        if f.name.0[0].quote_style.is_some()
            || f.uses_odbc_syntax
            || f.parameters != a::FunctionArguments::None
            || f.filter.is_some()
            || f.null_treatment.is_some()
            || f.over.is_some()
            || !f.within_group.is_empty()
            || self.view_depth != 0
            || self.snapshot.user_routines.iter().any(|n| {
                ["json_value", "json_query", "json_type", "to_json_string"]
                    .iter()
                    .any(|name| n.eq_ignore_ascii_case(name))
            })
        {
            return Err(denied());
        }
        let a::FunctionArguments::List(args) = &f.args else {
            return Err(denied());
        };
        if args.duplicate_treatment.is_some() || !args.clauses.is_empty() {
            return Err(denied());
        }
        let [
            a::FunctionArg::Unnamed(a::FunctionArgExpr::Expr(col)),
            a::FunctionArg::Unnamed(a::FunctionArgExpr::Expr(a::Expr::Value(
                a::Value::SingleQuotedString(p),
            ))),
        ] = args.args.as_slice()
        else {
            return Err(denied());
        };
        // A visible value prefix can shadow a table qualifier in GoogleSQL.
        if let a::Expr::CompoundIdentifier(ids) = col
            && ids.len() == 2
            && std::iter::once(scope)
                .chain(self.outers.iter().rev())
                .any(|bindings| {
                    bindings
                        .iter()
                        .any(|b| b.visible && b.output.ir.name.eq_ignore_ascii_case(&ids[0].value))
                })
        {
            return Err(denied());
        }
        let output = match col {
            a::Expr::Identifier(id) => self.resolve(std::slice::from_ref(id), scope)?,
            a::Expr::CompoundIdentifier(ids) => self.resolve(ids, scope)?,
            _ => return Err(denied()),
        };
        if !output.base || output.ir.ty != Some(ColumnType::Json) {
            return Err(denied());
        }
        let source = output.ir.source.ok_or_else(denied)?;
        let filter = self.policy.json_filter(&source).ok_or_else(denied)?;
        let start = offset(self.sql, f.name.0[0].span.start)?;
        let mut i = offset(self.sql, f.name.0[0].span.end)?;
        let b = self.sql.as_bytes();
        ws(b, &mut i);
        if b.get(i) != Some(&b'(') {
            return Err(denied());
        }
        i += 1;
        ws(b, &mut i);
        let col_start = i;
        let ids: &[a::Ident] = match col {
            a::Expr::Identifier(id) => std::slice::from_ref(id),
            a::Expr::CompoundIdentifier(ids) => ids,
            _ => return Err(denied()),
        };
        for (j, id) in ids.iter().enumerate() {
            if j > 0 {
                ws(b, &mut i);
                if b.get(i) != Some(&b'.') {
                    return Err(denied());
                }
                i += 1;
                ws(b, &mut i);
            }
            let spelling = id.to_string();
            if !self.sql.get(i..).is_some_and(|s| s.starts_with(&spelling)) {
                return Err(denied());
            }
            i += spelling.len();
        }
        let col_end = i;
        ws(b, &mut i);
        if b.get(i) != Some(&b',') {
            return Err(denied());
        }
        i += 1;
        ws(b, &mut i);
        let path_start = i;
        let decoded = quoted(self.sql, &mut i)?;
        if decoded != *p {
            return Err(denied());
        }
        let path_end = i;
        ws(b, &mut i);
        if b.get(i) != Some(&b')') {
            return Err(denied());
        }
        i += 1;
        let query = format!(
            "JSON_QUERY({}, {})",
            &self.sql[col_start..col_end],
            &self.sql[path_start..path_end]
        );
        let steps = path(&decoded)?;
        let mut prefix = "$".to_owned();
        let mut guards = Vec::new();
        for step in &steps {
            match step {
                Step::Key(k) => { prefix.push('.'); prefix.push_str(&serde_json::to_string(k).map_err(|_| denied())?); },
                Step::Index(i) => {
                    let literal = prefix.replace("'", "''");
                    guards.push(format!("JSON_TYPE(JSON_QUERY({}, '{literal}')) = 'array'", &self.sql[col_start..col_end]));
                    prefix.push_str(&format!("[{i}]"));
                }
            }
        }
        let keep_empty = matches!(steps.last(), Some(Step::Index(_)));
        Ok(Some(Extract {
            source,
            column_prefix: ids.first().ok_or_else(denied)?.value.to_lowercase(),
            qualified: ids.len() > 1,
            treatment: extract::classify(&steps, filter)?,
            path: steps,
            text: f.name.0[0].value.eq_ignore_ascii_case("json_value"),
            span: Span { start, end: i },
            query,
            guards,
            keep_empty,
        }))
    }
    /// Attach the output action and any type-preserving SQL replacement.
    pub(super) fn json_projection(&mut self, e: Extract, ordinal: usize) -> Result<expr::Expr> {
        let mut out = expr::Expr::plain();
        out.ty = Some(if e.text {
            ColumnType::Text
        } else {
            ColumnType::Json
        });
        let original = self.sql.get(e.span.start..e.span.end).ok_or_else(denied)?;
        let filtered_text = e.text && matches!(e.treatment, Extraction::Filter(_));
        if !e.guards.is_empty() || filtered_text {
            let value = if filtered_text { format!("TO_JSON_STRING({})", e.query) } else { original.to_owned() };
            self.json_inputs.push(InputRewrite::ReplaceSpan { span: e.span.clone(), text: if e.guards.is_empty() { value } else { format!("(CASE WHEN {} THEN {value} END)", e.guards.join(" AND ")) } });
        }
        let action = match e.treatment {
            Extraction::Plain => return Ok(out),
            Extraction::Masked => OutputRewrite::Mask { placeholder: None },
            Extraction::Filter(filter) => {
                OutputRewrite::JsonExtract {
                    filter,
                    base: ColumnType::Json,
                    text: e.text,
                    keep_empty: e.keep_empty,
                }
            }
        };
        out.label = Label::Masked;
        out.value = ValueSource::Computed(Label::Masked);
        out.inputs = vec![(e.source, Label::Masked)];
        self.record(&out, UseKind::Output, &Context::SelectList);
        self.json_outputs.push((ordinal, action));
        Ok(out)
    }
    /// Lower a declared Show scalar using the same guards as a predicate.
    pub(super) fn json_grouping(&mut self, e: Extract) -> Result<expr::Expr> {
        if self.depth != 1 || self.view_depth != 0 {
            return Err(denied());
        }
        // A plain extraction (an unlisted path under a show-mode filter) is shown data.
        if e.treatment != Extraction::Plain {
            let filter = self.policy.json_filter(&e.source).ok_or_else(denied)?;
            extract::require_show_leaf(&e.path, filter, &e.treatment)?;
        }
        self.json_predicate(e)
    }

    /// Guard a declared leaf and retain its Blind domain for equality.
    pub(super) fn json_predicate(&mut self, e: Extract) -> Result<expr::Expr> {
        use crate::policy::{JsonLeafType, JsonPathTreatment, JsonTokenType};
        if !e.text || self.depth != 1 {
            return Err(denied());
        }
        let mut out = expr::Expr::plain();
        out.ty = Some(ColumnType::Text);
        let kind = match &e.treatment {
            Extraction::Plain => {
                if !e.guards.is_empty() {
                    let original = self.sql.get(e.span.start..e.span.end).ok_or_else(denied)?;
                    self.json_inputs.push(InputRewrite::ReplaceSpan { span: e.span, text: format!("(CASE WHEN {} THEN {original} END)", e.guards.join(" AND ")) });
                }
                return Ok(out);
            },
            Extraction::Masked => return Err(denied()),
            Extraction::Filter(filter) => {
                let [rule] = filter.rules.as_slice() else {
                    return Err(denied());
                };
                if !rule.path.is_empty() {
                    return Err(denied());
                }
                match &rule.treatment {
                    JsonPathTreatment::Show { ty } => match ty {
                        JsonLeafType::String => "string",
                        JsonLeafType::Number => "number",
                        JsonLeafType::Boolean => "boolean",
                    },
                    JsonPathTreatment::Blind {
                        domain,
                        ty: JsonTokenType::Text,
                    } => {
                        out.label = Label::Blind(domain.clone());
                        out.value = ValueSource::JsonLeaf(domain.clone());
                        out.inputs = vec![(e.source, out.label.clone())];
                        out.bare = true;
                        "string"
                    }
                    _ => return Err(denied()),
                }
            }
        };
        let original = self.sql.get(e.span.start..e.span.end).ok_or_else(denied)?;
        self.json_inputs.push(InputRewrite::ReplaceSpan {
            span: e.span,
            text: format!(
                "(CASE WHEN {}JSON_TYPE({}) = '{kind}' THEN {original} END)",
                if e.guards.is_empty() { String::new() } else { format!("{} AND ", e.guards.join(" AND ")) }, e.query
            ),
        });
        Ok(out)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        json::filter::JsonMode,
        policy::{
            DomainName, JsonColumnFilter, JsonLeafType, JsonPathRule, JsonPathTreatment,
            JsonTokenType, PathSegment, ReadLogSink,
        },
        schema::{Column, Relation},
    };
    fn fixture(mode: JsonMode) -> (Snapshot, Policy) {
        let column = ColumnRef {
            schema: "".into(),
            table: "docs".into(),
            column: "doc".into(),
        };
        let domain = DomainName("d".into());
        let snapshot = Snapshot::new(
            vec![Relation {
                schema: "".into(),
                name: "docs".into(),
                kind: RelationKind::Table,
                view_definition: None,
                primary_key: vec![],
                columns: vec![
                    Column {
                        name: "doc".into(),
                        ty: ColumnType::Json,
                        nullable: true,
                        generated: None,
                        default: None,
                    },
                    Column {
                        name: "email".into(),
                        ty: ColumnType::Text,
                        nullable: true,
                        generated: None,
                        default: None,
                    },
                ],
            }],
            vec![],
            vec!["".into()],
            0,
        );
        let rules = [
            (
                "secret",
                JsonPathTreatment::Blind {
                    domain: domain.clone(),
                    ty: JsonTokenType::Text,
                },
            ),
            (
                "shown",
                JsonPathTreatment::Show {
                    ty: JsonLeafType::String,
                },
            ),
            ("hidden", JsonPathTreatment::Masked),
        ]
        .into_iter()
        .map(|(key, treatment)| JsonPathRule {
            column: column.clone(),
            path: vec![PathSegment::Key(key.into())],
            treatment,
        })
        .collect();
        let policy = Policy {
            columns: BTreeMap::from([
                (column.clone(), Treatment::Masked { placeholder: None }),
                (
                    ColumnRef {
                        column: "email".into(),
                        ..column.clone()
                    },
                    Treatment::Blind { domain },
                ),
            ]),
            json_filters: BTreeMap::from([(column, JsonColumnFilter { mode, rules })]),
            domains: BTreeMap::new(),
            retired_domains: Default::default(),
            snapshot_max_age_ms: 0,
            readlog: ReadLogSink::Stdout,
        };
        (snapshot, policy)
    }
    fn plan(sql: &str, mode: JsonMode) -> Result<RewritePlan> {
        let (snapshot, policy) = fixture(mode);
        let (stmt, _) = lower_gsql(sql, &snapshot, &policy)?;
        crate::ir::check::check(&stmt, &policy, &snapshot)
    }
    #[test]
    fn then_return_of_json_filtered_column_is_refused() {
        for mode in [JsonMode::Show, JsonMode::Tokenize] {
            for sql in [
                "DELETE FROM docs WHERE TRUE THEN RETURN doc",
                "DELETE FROM docs WHERE TRUE THEN RETURN *",
                "UPDATE docs SET email = NULL WHERE TRUE THEN RETURN doc",
            ] {
                assert_eq!(plan(sql, mode).unwrap_err().code, NyCode::MaskedExpression, "{sql}");
            }
        }
    }
    #[test]
    fn normalizes_like_pg_and_refuses_unknown_paths() {
        let (_, policy) = fixture(JsonMode::Show);
        let filter = policy.json_filters.values().next().unwrap();
        let mut pg = vec![];
        extract::normalize(&mut pg, "->", "'a'", filter).unwrap();
        extract::normalize(&mut pg, "->", "'b'", filter).unwrap();
        assert_eq!(path("$.a.b").unwrap(), pg);
        assert_eq!(path("$.\"a\".b").unwrap(), pg);
        assert_eq!(
            path("$.a[0]").unwrap(),
            vec![Step::Key("a".into()), Step::Index(0)]
        );
        for p in [
            "",
            "a",
            "$.*",
            "$..a",
            "$[?(@.a)]",
            "$['a']",
            "$[-1]",
            "$[1.0]",
            "$[9223372036854775808]",
            "lax $.a",
            "strict $.a",
            "$.\"a\\b\"",
            "$.",
        ] {
            assert!(path(p).is_err(), "{p}");
        }
    }
    #[test]
    fn projection_modes_and_rewrites() {
        for mode in [JsonMode::Show, JsonMode::Tokenize] {
            for name in ["JSON_VALUE", "JSON_QUERY"] {
                let sql = format!("SELECT {name}(doc, '$.secret') AS v FROM docs");
                let p = plan(&sql, mode).unwrap();
                assert!(
                    matches!(&p.outputs[0], OutputRewrite::JsonExtract { text, base: ColumnType::Json, .. } if *text == (name == "JSON_VALUE"))
                );
                assert_eq!(p.inputs.len(), usize::from(name == "JSON_VALUE"));
                if name == "JSON_VALUE" {
                    assert!(
                        matches!(&p.inputs[0], InputRewrite::ReplaceSpan { span, text } if &sql[span.start..span.end] == "JSON_VALUE(doc, '$.secret')" && text == "TO_JSON_STRING(JSON_QUERY(doc, '$.secret'))")
                    );
                }
                let masked =
                    plan(&format!("SELECT {name}(doc, '$.hidden') FROM docs"), mode).unwrap();
                assert!(matches!(
                    masked.outputs[0],
                    OutputRewrite::Mask { placeholder: None }
                ));
                let unknown =
                    plan(&format!("SELECT {name}(doc, '$.other') FROM docs"), mode).unwrap();
                assert!(matches!(
                    (&mode, &unknown.outputs[0]),
                    (JsonMode::Show, OutputRewrite::Pass)
                        | (JsonMode::Tokenize, OutputRewrite::Mask { .. })
                ));
                assert!(plan(&format!("SELECT {name}(doc, '$') FROM docs"), mode).is_ok());
                assert!(
                    plan(
                        &format!("SELECT {name}(doc, '$.\"secret\"') FROM docs"),
                        mode
                    )
                    .is_ok()
                );
                let (snapshot, policy) = fixture(mode);
                let statement = lower_gsql(
                    &format!("SELECT {name}(doc, '$.secret') FROM docs"),
                    &snapshot,
                    &policy,
                )
                .unwrap()
                .0;
                assert_eq!(statement.outputs[0].name, "");
            }
        }
    }
    #[test]
    fn guards_predicates_and_preserves_blind_domains() {
        for mode in [JsonMode::Show, JsonMode::Tokenize] {
            for tail in [
                "JSON_VALUE(doc, '$.secret') = @p",
                "JSON_VALUE(doc, '$.secret') = 'token'",
                "JSON_VALUE(doc, '$.secret') = 'token' AND JSON_VALUE(doc, '$.secret') = 'token'",
                "'token' = JSON_VALUE(doc, '$.secret') AND 'token' = JSON_VALUE(doc, '$.secret')",
                "JSON_VALUE(doc, '$.secret') = email",
                "email = JSON_VALUE(doc, '$.secret')",
                "JSON_VALUE(doc, '$.secret') = JSON_VALUE(doc, '$.secret')",
                "JSON_VALUE(doc, '$.shown') = 'visible'",
                "JSON_VALUE(doc, '$.secret') IS NULL",
            ] {
                let p = plan(&format!("SELECT email FROM docs WHERE {tail}"), mode).unwrap();
                assert!(p.inputs.iter().any(|i| matches!(i, InputRewrite::ReplaceSpan { text, .. } if text.contains("CASE WHEN JSON_TYPE(JSON_QUERY(") && text.contains("= 'string' THEN"))));
            }
            assert!(plan(
                "SELECT d.email FROM docs d JOIN docs e ON JSON_VALUE(d.doc, '$.secret') = e.email",
                mode
            )
            .is_ok());
        }
    }
    #[test]
    fn rejects_unanalysable_or_sensitive_uses() {
        for mode in [JsonMode::Show, JsonMode::Tokenize] {
            for expression in [
                "JSON_VALUE(doc)",
                "JSON_VALUE(doc, '$', '$')",
                "JSON_VALUE(doc, @p)",
                "JSON_VALUE(doc, CONCAT('$', '.secret'))",
                "JSON_VALUE(doc, r'$.secret')",
                "JSON_VALUE(doc, b'$.secret')",
                "JSON_VALUE(doc, '''$.secret''')",
                "JSON_VALUE(doc, \"$.secret\")",
                "JSON_VALUE(doc, '$.se\\cret')",
                "JSON_VALUE(doc, '$.*')",
                "JSON_VALUE(doc, '$[-1]')",
                "JSON_QUERY_ARRAY(doc, '$')",
                "JSON_VALUE_ARRAY(doc, '$')",
                "JSON_KEYS(doc)",
                "JSON_TYPE(doc)",
                "TO_JSON_STRING(doc)",
                "SAFE.JSON_VALUE(doc, '$.secret')",
                "doc.secret",
                "doc['secret']",
                "LOWER(JSON_VALUE(doc, '$.secret'))",
            ] {
                assert!(
                    plan(&format!("SELECT {expression} FROM docs"), mode).is_err(),
                    "accepted {expression}"
                );
            }
            for predicate in [
                "JSON_QUERY(doc, '$.secret') = @p",
                "JSON_VALUE(doc, '$.hidden') = @p",
                "JSON_VALUE(doc, '$') = @p",
                "JSON_VALUE(doc, '$.secret') > @p",
                "JSON_VALUE(doc, '$.secret') = 1",
                "JSON_VALUE(doc, '$.secret') IN (@p)",
                "JSON_VALUE(doc, '$.secret') = CAST(@p AS STRING)",
                "JSON_VALUE(doc, '$.secret') = JSON_VALUE(doc, '$.shown')",
            ] {
                assert!(
                    plan(&format!("SELECT email FROM docs WHERE {predicate}"), mode).is_err(),
                    "accepted {predicate}"
                );
            }
            for sql in [
                "SELECT DISTINCT JSON_VALUE(doc, '$.secret') FROM docs",
                "SELECT JSON_VALUE(doc, '$.secret') AS x FROM docs ORDER BY x",
                "SELECT JSON_VALUE(doc, '$.secret') FROM docs UNION ALL SELECT email FROM docs",
                "SELECT JSON_VALUE(doc, '$.secret') FROM (SELECT doc FROM docs) d",
                "WITH d AS (SELECT doc FROM docs) SELECT JSON_VALUE(doc, '$.secret') FROM d",
                "WITH d AS (SELECT * FROM docs) SELECT JSON_VALUE(doc, '$.secret') FROM d",
                "SELECT JSON_VALUE(doc, '$.secret') FROM (SELECT * FROM docs) d",
                "DELETE FROM docs WHERE JSON_VALUE(doc, '$.secret') = @p",
            ] {
                assert!(plan(sql, mode).is_err(), "accepted {sql}");
            }
        }
    }
    #[test]
    fn show_json_value_group_order_and_distinct() {
        let guarded = "(CASE WHEN JSON_TYPE(JSON_QUERY(doc, '$.shown')) = 'string' THEN JSON_VALUE(doc, '$.shown') END)";
        for mode in [JsonMode::Show, JsonMode::Tokenize] {
            for shape in [
                "SELECT LEAF, COUNT(*) FROM docs GROUP BY LEAF ORDER BY 1",
                "SELECT LEAF AS v, COUNT(*) FROM docs GROUP BY 1 ORDER BY v",
                "SELECT LEAF, COUNT(*) FROM docs GROUP BY LEAF ORDER BY LEAF",
                "SELECT email FROM docs ORDER BY LEAF",
                "SELECT email FROM docs ORDER BY (LEAF)",
                "SELECT LEAF AS v FROM docs ORDER BY v",
                "SELECT LEAF AS v FROM docs ORDER BY (v)",
                "SELECT LEAF FROM docs ORDER BY 1",
                "SELECT DISTINCT LEAF FROM docs",
            ] {
                let sql = shape.replace("LEAF", "JSON_VALUE(doc, '$.shown')");
                let allowed = plan(&sql, mode).unwrap();
                assert_eq!(allowed.outputs[0], if shape.starts_with("SELECT email") {
                    OutputRewrite::Tokenize { domain: DomainName("d".into()), ty: ColumnType::Text }
                } else {
                    OutputRewrite::Pass
                });
                let replacements: Vec<_> = allowed.inputs.into_iter().map(|input| match input {
                    InputRewrite::ReplaceSpan { span, text } => (span, text),
                    _ => panic!("unexpected input rewrite"),
                }).collect();
                assert_eq!(
                    crate::ir::rewrite::splice(&sql, &replacements).unwrap(),
                    shape.replace("LEAF", guarded),
                );
                if mode == JsonMode::Show {
                    // Show mode passes unlisted paths through, so they group like any shown value.
                    let sql = shape.replace("LEAF", "JSON_VALUE(doc, '$.unlisted')");
                    plan(&sql, mode).expect(&sql);
                }
                for (path, reason) in [
                    ("$.secret", crate::error::JSON_GROUP_BLIND),
                    ("$.hidden", crate::error::JSON_GROUP_MASKED),
                    ("$.unlisted", crate::error::JSON_GROUP_UNLISTED),
                    ("$", crate::error::JSON_GROUP_NOT_LEAF),
                ] {
                    if mode == JsonMode::Show && path == "$.unlisted" {
                        continue;
                    }
                    let sql = shape.replace("LEAF", &format!("JSON_VALUE(doc, '{path}')"));
                    let error = plan(&sql, mode).expect_err(&sql);
                    assert_eq!(error.code, NyCode::MaskedExpression, "{sql}");
                    assert_eq!(error.message(), reason, "{sql}");
                }
            }
            for sql in [
                "SELECT JSON_VALUE(doc, '$.shown') FROM (SELECT doc FROM docs) d GROUP BY 1",
                "SELECT * FROM (SELECT JSON_VALUE(doc, '$.shown') AS v FROM docs ORDER BY v) d",
                "SELECT JSON_QUERY(doc, '$') AS doc FROM docs ORDER BY JSON_VALUE(doc, '$.shown')",
                "SELECT email AS doc FROM docs GROUP BY JSON_VALUE(doc, '$.shown')",
            ] {
                assert_eq!(plan(sql, mode).expect_err(sql).code, NyCode::MaskedExpression, "{sql}");
            }
        }
    }

    #[test]
    fn generated_functions_and_ambiguous_prefixes_fail_closed() {
        for name in ["json_value", "json_query", "json_type", "to_json_string"] {
            let (mut snapshot, policy) = fixture(JsonMode::Show);
            snapshot.user_routines.push(name.into());
            assert!(
                lower_gsql(
                    "SELECT JSON_VALUE(doc, '$.secret') FROM docs",
                    &snapshot,
                    &policy
                )
                .is_err()
            );
        }
        assert!(
            plan(
                "SELECT JSON_VALUE(doc.doc, '$.secret') FROM docs doc",
                JsonMode::Show
            )
            .is_err()
        );
        let (snapshot, mut policy) = fixture(JsonMode::Show);
        let email = ColumnRef {
            schema: "".into(),
            table: "docs".into(),
            column: "email".into(),
        };
        policy.columns.insert(
            email,
            Treatment::Blind {
                domain: DomainName("other".into()),
            },
        );
        assert!(
            lower_gsql(
                "SELECT email FROM docs WHERE JSON_VALUE(doc, '$.secret') = email",
                &snapshot,
                &policy
            )
            .is_err()
        );
    }

    #[test]
    fn f1b_each_index_has_container_guard_in_projection_and_predicate() {
        let (snapshot, mut policy) = fixture(JsonMode::Show);
        let filter = policy.json_filters.values_mut().next().unwrap();
        filter.rules[0].path = crate::policy::json_path::parse_path("$.a[*].b[*].secret").unwrap();
        for sql in ["SELECT JSON_VALUE(doc, '$.a[0].b[1].secret') FROM docs", "SELECT email FROM docs WHERE JSON_VALUE(doc, '$.a[0].b[1].secret') = @p", "SELECT JSON_VALUE(doc, '$.a[0].other') FROM docs", "SELECT email FROM docs WHERE JSON_VALUE(doc, '$.a[0].other') = 'x'"] {
            let (stmt, _) = lower_gsql(sql, &snapshot, &policy).unwrap();
            let plan = crate::ir::check::check(&stmt, &policy, &snapshot).unwrap();
            let guards = plan.inputs.iter().filter_map(|i| if let InputRewrite::ReplaceSpan { text, .. } = i { Some(text.as_str()) } else { None }).collect::<Vec<_>>().join(" ");
            assert_eq!(guards.matches("= 'array'").count(), if sql.contains("b[1]") { 2 } else { 1 });
            assert!(guards.contains("CASE WHEN"));
        }
    }
    #[test]
    fn r3_digit_only_keys_refuse_projection_and_predicate() {
        let (snapshot, mut policy) = fixture(JsonMode::Show);
        let filter = policy.json_filters.values_mut().next().unwrap();
        filter.rules.truncate(1);
        filter.rules[0].path = crate::policy::json_path::parse_path("$.a.named.b").unwrap();
        filter.rules[0].treatment = JsonPathTreatment::Masked;
        for path in ["$.a.0", "$.a.\"0\"", "$.a.00", "$.a.\"123\""] {
            for function in ["JSON_VALUE", "JSON_QUERY"] {
                let expression = format!("{function}(doc, '{path}')");
                for sql in [format!("SELECT {expression} FROM docs"), format!("SELECT email FROM docs WHERE {expression} = 'x'")] {
                    assert_eq!(lower_gsql(&sql, &snapshot, &policy).unwrap_err().code, NyCode::MaskedExpression);
                }
            }
        }
        assert!(plan("SELECT JSON_VALUE(doc, '$.a.a0') FROM docs", JsonMode::Show).is_ok());
    }

}
