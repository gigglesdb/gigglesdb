//! Fail-closed reassembly and rewriting of streamed Spanner rows.
use super::values::{from_proto, to_proto};
use crate::{
    error::{NyCode, Refusal},
    ir::OutputRewrite,
    policy::DomainName,
    value::{ColumnType, SqlValue},
};
use googleapis_tonic_google_spanner_v1::google::spanner::v1::{PartialResultSet, ResultSetMetadata, ResultSetStats};
use prost_types::{value::Kind, Value};
/// Multiplexed-session tokens held until a safe outgoing stream message.
#[derive(Default)]
pub(super) struct Precommit {
    pending: Option<
        googleapis_tonic_google_spanner_v1::google::spanner::v1::MultiplexedSessionPrecommitToken,
    >,
    highest: Option<i32>,
    last: bool,
}
impl Precommit {
    /// Hold the latest token and remember upstream completion.
    pub(super) fn observe(&mut self, part: &mut PartialResultSet) {
        self.last |= std::mem::take(&mut part.last);
        if let Some(token) = part.precommit_token.take()
            && self.highest.is_none_or(|seq| token.seq_num >= seq)
        {
            self.highest = Some(token.seq_num);
            self.pending = Some(token);
        }
    }
    /// Attach the held token to a complete outgoing message.
    pub(super) fn attach(&mut self, part: &mut PartialResultSet) {
        part.precommit_token = self.pending.take();
    }
    /// Flush control fields after normal row reassembly completes.
    pub(super) fn finish(self) -> Option<PartialResultSet> {
        if self.pending.is_none() && !self.last {
            return None;
        }
        Some(PartialResultSet {
            precommit_token: self.pending,
            last: self.last,
            ..Default::default()
        })
    }
}
fn bad() -> Refusal {
    Refusal::new(NyCode::NotAnalysable, "malformed Spanner stream")
}
/// A complete upstream metadata, row, or statistics event.
pub enum Event {
    /// Initial result metadata.
    Metadata(ResultSetMetadata),
    /// A complete row.
    Row(Vec<Value>),
    /// Upstream result statistics.
    Stats(ResultSetStats),
}
fn chunkable(v: &Value) -> bool {
    matches!(
        v.kind,
        Some(Kind::StringValue(_) | Kind::ListValue(_) | Kind::StructValue(_))
    )
}
fn merge(a: &mut Value, b: Value, depth: usize) -> Result<(), Refusal> {
    if depth > 128 {
        return Err(bad());
    }
    match (a.kind.as_mut(), b.kind) {
        (Some(Kind::StringValue(a)), Some(Kind::StringValue(b))) => a.push_str(&b),
        (Some(Kind::ListValue(a)), Some(Kind::ListValue(mut b))) => {
            if let (Some(last), Some(first)) = (a.values.last_mut(), b.values.first())
                && matches!(
                    (&last.kind, &first.kind),
                    (Some(Kind::StringValue(_)), Some(Kind::StringValue(_)))
                        | (Some(Kind::ListValue(_)), Some(Kind::ListValue(_)))
                        | (Some(Kind::StructValue(_)), Some(Kind::StructValue(_)))
                )
            {
                merge(last, b.values.remove(0), depth + 1)?;
            }
            a.values.extend(b.values);
        }
        (Some(Kind::StructValue(a)), Some(Kind::StructValue(b))) => {
            for (key, value) in b.fields {
                if let Some(old) = a.fields.get_mut(&key) {
                    merge(old, value, depth + 1)?;
                } else {
                    a.fields.insert(key, value);
                }
            }
        }
        _ => return Err(bad()),
    }
    Ok(())
}
/// Stateful complete-row stream reassembler.
#[derive(Default)]
pub struct Reassembler {
    columns: Option<usize>,
    pending: Option<Value>,
    row: Vec<Value>,
    last_safe: Option<Vec<u8>>,
    failed: bool,
}
impl Reassembler {
    /// Create an empty stream reassembler.
    pub fn new() -> Self {
        Self::default()
    }
    /// Feed one response and return complete events plus a safe current resume token.
    pub fn push(
        &mut self,
        prs: PartialResultSet,
    ) -> Result<(Vec<Event>, Option<Vec<u8>>), Refusal> {
        if self.failed {
            return Err(bad());
        }
        let result = self.push_inner(prs);
        if result.is_err() {
            self.failed = true;
        }
        result
    }
    fn push_inner(
        &mut self,
        prs: PartialResultSet,
    ) -> Result<(Vec<Event>, Option<Vec<u8>>), Refusal> {
        let mut events = Vec::new();
        if let Some(metadata) = prs.metadata {
            if self.columns.is_some() {
                return Err(bad());
            }
            self.columns = Some(metadata.row_type.as_ref().map_or(0, |r| r.fields.len()));
            events.push(Event::Metadata(metadata));
        }
        if self.columns.is_none() {
            return Err(bad());
        }
        let columns = self.columns.ok_or_else(bad)?;
        let mut values = prs.values;
        if let Some(mut pending) = self.pending.take() {
            if values.is_empty() {
                return Err(bad());
            }
            merge(&mut pending, values.remove(0), 0)?;
            values.insert(0, pending);
        }
        if prs.chunked_value {
            let pending = values.pop().ok_or_else(bad)?;
            if !chunkable(&pending) {
                return Err(bad());
            }
            self.pending = Some(pending);
        }
        if columns == 0 && (!values.is_empty() || self.pending.is_some()) {
            return Err(bad());
        }
        for value in values {
            if value.kind.is_none() {
                return Err(bad());
            }
            self.row.push(value);
            if self.row.len() == columns {
                events.push(Event::Row(std::mem::take(&mut self.row)));
            }
        }
        if let Some(stats) = prs.stats {
            events.push(Event::Stats(stats));
        }
        let safe = if self.pending.is_none() && self.row.is_empty() && !prs.resume_token.is_empty()
        {
            Some(prs.resume_token)
        } else {
            None
        };
        if safe.is_some() {
            self.last_safe = safe.clone();
        }
        Ok((events, safe))
    }
    /// Refuse a failed stream or an unfinished value or row.
    pub fn finish(self) -> Result<(), Refusal> {
        if self.failed || self.pending.is_some() || !self.row.is_empty() {
            Err(bad())
        } else {
            Ok(())
        }
    }
}
/// Emit complete rows as one unchunked upstream-shaped response.
pub fn emit(
    metadata: Option<ResultSetMetadata>,
    rows: Vec<Vec<Value>>,
    resume_token: Option<Vec<u8>>,
    stats: Option<ResultSetStats>,
) -> PartialResultSet {
    PartialResultSet {
        metadata,
        values: rows.into_iter().flatten().collect(),
        chunked_value: false,
        resume_token: resume_token.unwrap_or_default(),
        stats,
        ..Default::default()
    }
}
type JsonRewrite<'a> = dyn FnMut(
    &crate::policy::JsonColumnFilter,
    &[u8],
    &mut Vec<String>,
) -> Result<crate::json::filter::JsonOutput, Refusal> + 'a;

/// Apply a complete row's rewrite plan and return released token text.
pub fn rewrite_row(
    row: &mut [Value],
    plan: &[OutputRewrite],
    types: &[ColumnType],
    tokenize: &mut dyn FnMut(&DomainName, &ColumnType, &SqlValue) -> Result<SqlValue, Refusal>,
    json: &mut JsonRewrite<'_>,
) -> Result<Vec<String>, Refusal> {
    if row.len() != plan.len() || row.len() != types.len() {
        return Err(bad());
    }
    let mut rewritten = row.to_vec();
    let mut released = Vec::new();
    for ((value, action), actual) in rewritten.iter_mut().zip(plan).zip(types) {
        if let OutputRewrite::JsonFilter { ty, .. } = action
            && (ty != actual || !matches!(ty, ColumnType::Json))
        {
            return Err(bad());
        }
        if matches!(value.kind, Some(Kind::NullValue(0))) {
            continue;
        }
        match action {
            OutputRewrite::JsonFilter { filter, .. } => {
                let Some(Kind::StringValue(text)) = &value.kind else { return Err(bad()); };
                value.kind = Some(match json(filter, text.as_bytes(), &mut released)? {
                    crate::json::filter::JsonOutput::Document(bytes) =>
                        Kind::StringValue(String::from_utf8(bytes).map_err(|_| bad())?),
                    crate::json::filter::JsonOutput::SqlNull => Kind::NullValue(0),
                });
            }
            OutputRewrite::Pass => {}
            OutputRewrite::Mask { placeholder } => {
                *value = Value {
                    kind: Some(
                        placeholder
                            .as_ref()
                            .map_or(Kind::NullValue(0), |s| Kind::StringValue(s.clone())),
                    ),
                }
            }
            OutputRewrite::Tokenize { domain, ty } => {
                if ty != actual {
                    return Err(bad());
                }
                let clear = from_proto(ty, value)?;
                let token = tokenize(domain, ty, &clear)?;
                *value = to_proto(ty, &token)?;
                match &value.kind {
                    Some(Kind::StringValue(s)) => released.push(s.clone()),
                    Some(Kind::BoolValue(b)) => released.push(b.to_string()),
                    Some(Kind::NullValue(0)) => {}
                    _ => return Err(bad()),
                }
            }
        }
    }
    row.clone_from_slice(&rewritten);
    Ok(released)
}
#[cfg(test)]
mod tests {
    use super::*;
    use googleapis_tonic_google_spanner_v1::google::spanner::v1::{struct_type::Field, StructType};
    fn s(s: &str) -> Value {
        Value {
            kind: Some(Kind::StringValue(s.into())),
        }
    }
    fn meta(n: usize) -> ResultSetMetadata {
        ResultSetMetadata {
            row_type: Some(StructType {
                fields: (0..n).map(|_| Field::default()).collect(),
            }),
            ..Default::default()
        }
    }
    fn part(values: Vec<Value>, chunked_value: bool) -> PartialResultSet {
        PartialResultSet {
            values,
            chunked_value,
            resume_token: vec![1],
            ..Default::default()
        }
    }
    fn precommit(
        seq_num: i32,
    ) -> googleapis_tonic_google_spanner_v1::google::spanner::v1::MultiplexedSessionPrecommitToken
    {
        googleapis_tonic_google_spanner_v1::google::spanner::v1::MultiplexedSessionPrecommitToken {
            seq_num,
            precommit_token: vec![0, 255, seq_num as u8],
        }
    }
    #[test]
    fn held_precommit_on_fragment_only_last_is_flushed() {
        let mut forwarding = Precommit::default();
        let mut fragment = part(vec![s("fragment")], true);
        fragment.precommit_token = Some(precommit(7));
        fragment.last = true;
        forwarding.observe(&mut fragment);
        assert!(fragment.precommit_token.is_none());
        assert!(!fragment.last);
        let final_part = forwarding.finish().unwrap();
        assert_eq!(final_part.precommit_token, Some(precommit(7)));
        assert!(final_part.last);
        assert!(final_part.values.is_empty());
    }
    #[test]
    fn precommit_keeps_highest_and_never_reorders_lower() {
        let mut forwarding = Precommit::default();
        for seq in [7, 9, 8] {
            forwarding.observe(&mut PartialResultSet {
                precommit_token: Some(precommit(seq)),
                ..Default::default()
            });
        }
        let mut output = PartialResultSet::default();
        forwarding.attach(&mut output);
        assert_eq!(output.precommit_token, Some(precommit(9)));
        forwarding.observe(&mut PartialResultSet {
            precommit_token: Some(precommit(7)),
            ..Default::default()
        });
        assert!(forwarding.finish().is_none());
        let mut forwarding = Precommit::default();
        forwarding.observe(&mut PartialResultSet {
            last: true,
            ..Default::default()
        });
        let output = forwarding.finish().unwrap();
        assert!(output.last);
        assert!(output.precommit_token.is_none());
    }
    #[test]
    fn string_chunks_and_tokens() {
        let mut r = Reassembler::new();
        let mut p = part(vec![s("a")], true);
        p.metadata = Some(meta(1));
        assert!(r.push(p).unwrap().1.is_none());
        assert!(r.push(part(vec![s("b")], true)).unwrap().1.is_none());
        let (e, t) = r.push(part(vec![s("c")], false)).unwrap();
        assert_eq!(t, Some(vec![1]));
        assert!(matches!(&e[0],Event::Row(v) if v==&vec![s("abc")]));
        r.finish().unwrap();
    }
    #[test]
    fn rows_and_emit() {
        let mut r = Reassembler::new();
        let mut p = part(vec![s("a")], false);
        p.metadata = Some(meta(2));
        assert!(r.push(p).unwrap().1.is_none());
        let (e, t) = r.push(part(vec![s("b")], false)).unwrap();
        assert!(t.is_some());
        assert!(matches!(&e[0],Event::Row(v) if v.len()==2));
        r.finish().unwrap();
        let mut r = Reassembler::new();
        let (e, _) = r
            .push(emit(
                Some(meta(1)),
                vec![vec![s("x")]],
                None,
                Some(ResultSetStats::default()),
            ))
            .unwrap();
        assert_eq!(e.len(), 3);
        r.finish().unwrap();
    }
    #[test]
    fn malformed_streams() {
        assert!(Reassembler::new().push(part(vec![s("x")], false)).is_err());
        for (values, chunked) in [(vec![s("x")], true), (vec![s("x")], false)] {
            let mut r = Reassembler::new();
            let mut p = part(values, chunked);
            p.metadata = Some(meta(2));
            r.push(p).unwrap();
            assert!(r.finish().is_err());
        }
        let mut r = Reassembler::new();
        let mut p = part(
            vec![Value {
                kind: Some(Kind::BoolValue(true)),
            }],
            true,
        );
        p.metadata = Some(meta(1));
        assert!(r.push(p).is_err());
    }
    #[test]
    fn nested_list_chunks() {
        fn list(v: Vec<Value>) -> Value {
            Value {
                kind: Some(Kind::ListValue(prost_types::ListValue { values: v })),
            }
        }
        let mut r = Reassembler::new();
        let mut p = part(vec![list(vec![list(vec![s("a")])])], true);
        p.metadata = Some(meta(1));
        r.push(p).unwrap();
        let (e, _) = r
            .push(part(vec![list(vec![list(vec![s("b")])])], false))
            .unwrap();
        assert!(matches!(&e[0],Event::Row(v) if v==&vec![list(vec![list(vec![s("ab")])])]));
    }
    #[test]
    fn rewriting() {
        let mut row = vec![
            s("secret"),
            s("hidden"),
            s("plain"),
            Value {
                kind: Some(Kind::NullValue(0)),
            },
        ];
        let plan = vec![
            OutputRewrite::Tokenize {
                domain: DomainName("d".into()),
                ty: ColumnType::Text,
            },
            OutputRewrite::Mask {
                placeholder: Some("masked".into()),
            },
            OutputRewrite::Pass,
            OutputRewrite::Mask {
                placeholder: Some("masked".into()),
            },
        ];
        let tokens = rewrite_row(
            &mut row,
            &plan,
            &vec![ColumnType::Text; 4],
            &mut |_, _, _| Ok(SqlValue::Text("token".into())),
            &mut |_, _, _| unreachable!(),
        )
        .unwrap();
        assert_eq!(tokens, vec!["token"]);
        assert_eq!(row[0], s("token"));
        assert_eq!(row[1], s("masked"));
        assert_eq!(row[2], s("plain"));
        assert!(matches!(row[3].kind, Some(Kind::NullValue(0))));
        assert!(rewrite_row(&mut row, &[], &[], &mut |_, _, _| unreachable!(), &mut |_, _, _| unreachable!()).is_err());
    }
    #[test]
    fn mixed_list_boundaries_and_objects() {
        fn list(values: Vec<Value>) -> Value {
            Value {
                kind: Some(Kind::ListValue(prost_types::ListValue { values })),
            }
        }
        for next in [
            Value {
                kind: Some(Kind::BoolValue(true)),
            },
            list(vec![s("b")]),
        ] {
            let mut a = list(vec![s("a")]);
            merge(&mut a, list(vec![next.clone()]), 0).unwrap();
            assert_eq!(a, list(vec![s("a"), next]));
        }
        fn object(fields: Vec<(&str, Value)>) -> Value {
            Value {
                kind: Some(Kind::StructValue(prost_types::Struct {
                    fields: fields.into_iter().map(|(k, v)| (k.into(), v)).collect(),
                })),
            }
        }
        let mut a = object(vec![("a", s("one"))]);
        merge(&mut a, object(vec![("a", s("two")), ("b", s("three"))]), 0).unwrap();
        assert_eq!(a, object(vec![("a", s("onetwo")), ("b", s("three"))]));
    }
    fn json_filter(mode: crate::json::filter::JsonMode, root: bool, blind: bool) -> crate::policy::JsonColumnFilter {
        use crate::policy::{JsonPathRule, JsonPathTreatment, JsonTokenType, PathSegment};
        crate::policy::JsonColumnFilter {
            mode,
            rules: vec![JsonPathRule {
                column: crate::schema::ColumnRef { schema: "".into(), table: "events".into(), column: "payload".into() },
                path: if root { vec![] } else { vec![PathSegment::Key("protected".into())] },
                treatment: if blind { JsonPathTreatment::Blind { domain: DomainName("d".into()), ty: JsonTokenType::Text } } else { JsonPathTreatment::Masked },
            }],
        }
    }
    fn apply_json(filter: &crate::policy::JsonColumnFilter, bytes: &[u8], released: &mut Vec<String>) -> Result<crate::json::filter::JsonOutput, Refusal> {
        crate::json::filter::apply_collect(
            &filter.rules, filter.mode, &crate::token::ScopeKeys::new([7;32]), &crate::token::StandardCodec,
            &std::collections::BTreeMap::from([(DomainName("d".into()), crate::policy::DomainSettings { tokenize: crate::policy::TokenizeSetting::Off, range: None })]),
            &crate::json::JsonLimits::default(), crate::json::filter::JsonInput { format: crate::json::codec::JsonFormat::SpannerJson, bytes }, released,
        )
    }
    #[test]
    fn json_rows_modes_nulls_kinds_and_atomic_refusal() {
        use crate::{json::filter::JsonMode, token::TokenCodec};
        for (mode, blind) in [(JsonMode::Show, false), (JsonMode::Tokenize, true)] {
            let filter = json_filter(mode, false, blind);
            let mut plan = vec![OutputRewrite::Mask { placeholder: None }, OutputRewrite::JsonFilter { filter, ty: ColumnType::Json }];
            let types = [ColumnType::Text, ColumnType::Json];
            let run = |row: &mut [Value], plan: &[OutputRewrite], types: &[ColumnType]| rewrite_row(row, plan, types, &mut |_, _, _| unreachable!(), &mut apply_json);
            let mut row = vec![s("earlier"), s(r#"{"protected":"hidden","unknown":"visible"}"#)];
            let released = run(&mut row, &plan, &types).unwrap();
            let token = crate::token::StandardCodec.encode(&crate::token::ScopeKeys::new([7;32]), &DomainName("d".into()), &ColumnType::Text, None, &SqlValue::Text("hidden".into())).unwrap();
            let SqlValue::Text(token) = token else { panic!("expected text token") };
            assert_eq!(released, if blind { vec![token.clone()] } else { vec![] });
            let expected = if blind { format!(r#"{{"protected":"{token}"}}"#) } else { r#"{"unknown":"visible"}"#.into() };
            assert_eq!(row[1], s(&expected));
            if !blind {
                row[1] = s(r#"{"protected":{"nested":"hidden"},"unknown":"visible"}"#);
                run(&mut row, &plan, &types).unwrap();
                assert_eq!(row[1], s(&expected));
            }
            for cell in [s(r#"{"protected":"hidden","protected":"hidden"}"#), Value { kind: Some(Kind::BoolValue(true)) }, Value { kind: None }] {
                let mut row = vec![s("earlier"), cell];
                let before = row.clone();
                let error = run(&mut row, &plan, &types).unwrap_err();
                assert!(!format!("{error:?}").contains("hidden"));
                assert_eq!(row, before);
            }
            let mut row = vec![s("earlier"), Value { kind: Some(Kind::NullValue(0)) }];
            run(&mut row, &plan, &types).unwrap();
            assert!(matches!(row[1].kind, Some(Kind::NullValue(0))));
            assert!(run(&mut row, &plan, &[ColumnType::Text, ColumnType::Text]).is_err());
            plan[1] = OutputRewrite::JsonFilter { filter: json_filter(mode, true, false), ty: ColumnType::Json };
            row[1] = s(r#"{"protected":"hidden"}"#);
            run(&mut row, &plan, &types).unwrap();
            assert!(matches!(row[1].kind, Some(Kind::NullValue(0))));
        }
        let plan = [OutputRewrite::JsonFilter { filter: json_filter(JsonMode::Show, false, false), ty: ColumnType::Json }];
        let mut row = vec![s("{}")];
        assert!(rewrite_row(&mut row, &plan, &[ColumnType::Json], &mut |_, _, _| unreachable!(), &mut |_, _, _| Ok(crate::json::filter::JsonOutput::Document(vec![255]))).is_err());
        assert_eq!(row, vec![s("{}")]);
    }
    #[test]
    fn json_split_chunks_filter_once_after_assembly() {
        let mut reassembler = Reassembler::new();
        let mut first = part(vec![s(r#"{"protected":"hid"#)], true);
        first.metadata = Some(meta(1));
        let (events, _) = reassembler.push(first).unwrap();
        assert!(!events.iter().any(|e| matches!(e, Event::Row(_))));
        let (events, _) = reassembler.push(part(vec![s(r#"den","unknown":"visible"}"#)], false)).unwrap();
        let mut calls = 0;
        let mut rows = 0;
        for event in events {
            if let Event::Row(mut row) = event {
                rewrite_row(&mut row, &[OutputRewrite::JsonFilter { filter: json_filter(crate::json::filter::JsonMode::Show, false, false), ty: ColumnType::Json }], &[ColumnType::Json], &mut |_, _, _| unreachable!(), &mut |filter, bytes, released| { calls += 1; apply_json(filter, bytes, released) }).unwrap();
                assert_eq!(row, vec![s(r#"{"unknown":"visible"}"#)]);
                rows += 1;
            }
        }
        assert_eq!((calls, rows), (1, 1));
    }
    #[test]
    fn rewrite_failure_is_atomic() {
        let mut row = vec![s("a"), s("b")];
        let original = row.clone();
        let plan = vec![
            OutputRewrite::Mask { placeholder: None },
            OutputRewrite::Tokenize {
                domain: DomainName("d".into()),
                ty: ColumnType::Text,
            },
        ];
        assert!(rewrite_row(
            &mut row,
            &plan,
            &[ColumnType::Text, ColumnType::Text],
            &mut |_, _, _| Err(bad()),
            &mut |_, _, _| unreachable!(),
        )
        .is_err());
        assert_eq!(row, original);
        let json_plan = vec![
            OutputRewrite::Mask { placeholder: None },
            OutputRewrite::JsonFilter {
                filter: json_filter(crate::json::filter::JsonMode::Show, false, false),
                ty: ColumnType::Json,
            },
        ];
        assert!(rewrite_row(
            &mut row, &json_plan, &[ColumnType::Text, ColumnType::Json],
            &mut |_, _, _| unreachable!(), &mut apply_json,
        ).is_err());
        assert_eq!(row, original);
    }
}
