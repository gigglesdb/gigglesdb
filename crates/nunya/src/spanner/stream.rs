//! Fail-closed reassembly and rewriting of streamed Spanner rows.
use super::values::{from_proto, to_proto};
use crate::{
    error::{NyCode, Refusal},
    ir::OutputRewrite,
    policy::DomainName,
    value::{ColumnType, SqlValue},
};
use google_cloud_googleapis::spanner::v1::{PartialResultSet, ResultSetMetadata, ResultSetStats};
use prost_types::{value::Kind, Value};
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
    }
}
/// Apply a complete row's rewrite plan and return released token text.
pub fn rewrite_row(
    row: &mut [Value],
    plan: &[OutputRewrite],
    types: &[ColumnType],
    tokenize: &mut dyn FnMut(&DomainName, &ColumnType, &SqlValue) -> Result<SqlValue, Refusal>,
) -> Result<Vec<String>, Refusal> {
    if row.len() != plan.len() || row.len() != types.len() {
        return Err(bad());
    }
    let mut rewritten = row.to_vec();
    let mut released = Vec::new();
    for ((value, action), actual) in rewritten.iter_mut().zip(plan).zip(types) {
        if matches!(value.kind, Some(Kind::NullValue(0))) {
            continue;
        }
        match action {
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
    use google_cloud_googleapis::spanner::v1::{struct_type::Field, StructType};
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
        )
        .unwrap();
        assert_eq!(tokens, vec!["token"]);
        assert_eq!(row[0], s("token"));
        assert_eq!(row[1], s("masked"));
        assert_eq!(row[2], s("plain"));
        assert!(matches!(row[3].kind, Some(Kind::NullValue(0))));
        assert!(rewrite_row(&mut row, &[], &[], &mut |_, _, _| unreachable!()).is_err());
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
            &mut |_, _, _| Err(bad())
        )
        .is_err());
        assert_eq!(row, original);
    }
}
