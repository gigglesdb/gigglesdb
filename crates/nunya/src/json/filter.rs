//! Policy-directed JSON filtering with fail-closed leaf conversion.

use super::{
    JsonLimits, JsonValue,
    codec::{self, JsonFormat},
};
use crate::{
    error::{NyCode, Refusal},
    pgwire::values::{decode_text, encode_text},
    policy::{
        DomainName, DomainSettings, JsonLeafType, JsonPathRule, JsonPathTreatment, JsonTokenType,
        PathSegment,
    },
    token::{ScopeKeys, TokenCodec},
    value::{ColumnType, SqlValue},
};
use std::collections::{BTreeMap, HashSet};

/// What an unlisted member means for a document that has path rules.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum JsonMode {
    /// Publish only explicitly approved members.
    Tokenize,
    /// Publish unlisted members unchanged.
    Show,
}

/// A JSON document in its database wire format.
pub struct JsonInput<'a> {
    /// Database wire format.
    pub format: JsonFormat,
    /// Encoded document bytes.
    pub bytes: &'a [u8],
}

/// Filtered document or suppression of the entire root.
#[derive(Debug, PartialEq, Eq)]
pub enum JsonOutput {
    /// Document encoded in the input format.
    Document(Vec<u8>),
    /// Suppressed root, to be sent as SQL NULL.
    SqlNull,
}

fn refuse(reason: &'static str) -> Refusal {
    Refusal::new(NyCode::NotAnalysable, reason)
}

/// Apply one column's nonoverlapping path rules and preserve its wire format.
#[allow(clippy::too_many_arguments)]
pub fn apply(
    rules: &[JsonPathRule],
    mode: JsonMode,
    keys: &ScopeKeys,
    codec: &dyn TokenCodec,
    domains: &BTreeMap<DomainName, DomainSettings>,
    limits: &JsonLimits,
    input: JsonInput<'_>,
) -> Result<JsonOutput, Refusal> {
    let first = rules
        .first()
        .ok_or_else(|| refuse("missing JSON path rules"))?;
    for rule in rules {
        if rule.column != first.column {
            return Err(refuse("inconsistent JSON rule columns"));
        }
        if let JsonPathTreatment::Blind { domain, .. } = &rule.treatment
            && !domains.contains_key(domain)
        {
            return Err(refuse("missing JSON token domain"));
        }
    }
    let tree = codec::decode(input.format, input.bytes, limits)?;
    let filter = Filter {
        mode,
        keys,
        codec,
        domains,
    };
    match filter.node(&tree, &rules.iter().collect::<Vec<_>>(), 0, true)? {
        Some(value) => Ok(JsonOutput::Document(codec::encode(input.format, &value)?)),
        None => Ok(JsonOutput::SqlNull),
    }
}

struct Filter<'a> {
    mode: JsonMode,
    keys: &'a ScopeKeys,
    codec: &'a dyn TokenCodec,
    domains: &'a BTreeMap<DomainName, DomainSettings>,
}

impl Filter<'_> {
    fn leaf(
        &self,
        value: &JsonValue,
        treatment: &JsonPathTreatment,
    ) -> Result<Option<JsonValue>, Refusal> {
        match treatment {
            JsonPathTreatment::Masked => Ok(None),
            JsonPathTreatment::Show { ty } => Ok(match (value, ty) {
                (JsonValue::Null, _)
                | (JsonValue::String(_), JsonLeafType::String)
                | (JsonValue::Number(_), JsonLeafType::Number)
                | (JsonValue::Bool(_), JsonLeafType::Boolean) => Some(value.clone()),
                _ => None,
            }),
            JsonPathTreatment::Blind { domain, ty } => {
                if matches!(value, JsonValue::Null) {
                    return Ok(Some(JsonValue::Null));
                }
                let column_type = column_type(*ty);
                let Some(plain) = sql_value(value, *ty, &column_type) else {
                    return Ok(None);
                };
                let settings = self
                    .domains
                    .get(domain)
                    .ok_or_else(|| refuse("missing JSON token domain"))?;
                let token = self.codec.encode(
                    self.keys,
                    domain,
                    &column_type,
                    settings.range.as_ref(),
                    &plain,
                )?;
                let rendered = match (ty, &token) {
                    (JsonTokenType::Text, SqlValue::Text(text)) => JsonValue::String(text.clone()),
                    (JsonTokenType::Int4, SqlValue::Int4(n)) => JsonValue::Number(n.to_string()),
                    (JsonTokenType::Int8, SqlValue::Int8(n)) => JsonValue::Number(n.to_string()),
                    (JsonTokenType::Uuid, SqlValue::Uuid(_))
                    | (JsonTokenType::Date, SqlValue::Date(_))
                    | (JsonTokenType::Timestamp, SqlValue::Timestamp(_))
                    | (JsonTokenType::Timestamptz, SqlValue::Timestamptz(_)) => {
                        let bytes =
                            encode_text(&token).map_err(|_| refuse("invalid JSON token result"))?;
                        JsonValue::String(
                            String::from_utf8(bytes)
                                .map_err(|_| refuse("invalid JSON token result"))?,
                        )
                    }
                    _ => return Err(refuse("invalid JSON token result")),
                };
                Ok(Some(rendered))
            }
        }
    }

    fn node(
        &self,
        value: &JsonValue,
        rules: &[&JsonPathRule],
        depth: usize,
        keep_empty: bool,
    ) -> Result<Option<JsonValue>, Refusal> {
        if let Some(rule) = rules.iter().find(|rule| rule.path.len() == depth) {
            if rules.len() != 1 {
                return Err(refuse("overlapping JSON path rules"));
            }
            return self.leaf(value, &rule.treatment);
        }
        let key_rules: Vec<_> = rules
            .iter()
            .copied()
            .filter(|r| matches!(r.path.get(depth), Some(PathSegment::Key(_))))
            .collect();
        let array_rules: Vec<_> = rules
            .iter()
            .copied()
            .filter(|r| matches!(r.path.get(depth), Some(PathSegment::AnyElement)))
            .collect();
        match value {
            JsonValue::Object(members) if !key_rules.is_empty() => {
                let mut seen = HashSet::new();
                let mut output = Vec::new();
                for (key, child) in members {
                    let matching: Vec<_> = key_rules
                        .iter()
                        .copied()
                        .filter(
                            |r| matches!(r.path.get(depth), Some(PathSegment::Key(k)) if k == key),
                        )
                        .collect();
                    if matching.is_empty() {
                        if self.mode == JsonMode::Show {
                            output.push((key.clone(), child.clone()));
                        }
                    } else {
                        if !seen.insert(key.as_str()) {
                            return Err(refuse("duplicate protected JSON member"));
                        }
                        if let Some(filtered) = self.node(child, &matching, depth + 1, false)? {
                            output.push((key.clone(), filtered));
                        }
                    }
                }
                if output.is_empty() && !keep_empty && self.mode == JsonMode::Tokenize {
                    Ok(None)
                } else {
                    Ok(Some(JsonValue::Object(output)))
                }
            }
            JsonValue::Array(elements) if !array_rules.is_empty() => {
                let output = elements
                    .iter()
                    .map(|child| {
                        self.node(child, &array_rules, depth + 1, true)
                            .map(|v| v.unwrap_or(JsonValue::Null))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Some(JsonValue::Array(output)))
            }
            JsonValue::Null if self.mode == JsonMode::Show => Ok(Some(JsonValue::Null)),
            JsonValue::Null
            | JsonValue::Bool(_)
            | JsonValue::Number(_)
            | JsonValue::String(_)
            | JsonValue::Object(_)
            | JsonValue::Array(_) => Ok(None),
        }
    }
}

fn column_type(ty: JsonTokenType) -> ColumnType {
    match ty {
        JsonTokenType::Text => ColumnType::Text,
        JsonTokenType::Int4 => ColumnType::Int4,
        JsonTokenType::Int8 => ColumnType::Int8,
        JsonTokenType::Uuid => ColumnType::Uuid,
        JsonTokenType::Date => ColumnType::Date,
        JsonTokenType::Timestamp => ColumnType::Timestamp,
        JsonTokenType::Timestamptz => ColumnType::Timestamptz,
    }
}

fn sql_value(value: &JsonValue, ty: JsonTokenType, column_type: &ColumnType) -> Option<SqlValue> {
    match (ty, value) {
        (JsonTokenType::Text, JsonValue::String(s)) => Some(SqlValue::Text(s.clone())),
        (JsonTokenType::Int4 | JsonTokenType::Int8, JsonValue::Number(n)) => {
            let digits = n.strip_prefix('-').unwrap_or(n);
            if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
                return None;
            }
            decode_text(column_type, n.as_bytes()).ok()
        }
        (
            JsonTokenType::Uuid
            | JsonTokenType::Date
            | JsonTokenType::Timestamp
            | JsonTokenType::Timestamptz,
            JsonValue::String(s),
        ) => decode_text(column_type, s.as_bytes()).ok(),
        _ => None,
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        policy::{TokenizeSetting, TypeRange, json_path::parse_path},
        schema::ColumnRef,
        token::StandardCodec,
    };

    fn rule(path: &str, treatment: JsonPathTreatment) -> JsonPathRule {
        JsonPathRule {
            column: ColumnRef {
                schema: "public".into(),
                table: "events".into(),
                column: "payload".into(),
            },
            path: parse_path(path).unwrap(),
            treatment,
        }
    }
    fn show(path: &str) -> JsonPathRule {
        rule(
            path,
            JsonPathTreatment::Show {
                ty: JsonLeafType::String,
            },
        )
    }
    fn blind(path: &str, ty: JsonTokenType) -> JsonPathRule {
        rule(
            path,
            JsonPathTreatment::Blind {
                domain: DomainName("email".into()),
                ty,
            },
        )
    }
    fn domains() -> BTreeMap<DomainName, DomainSettings> {
        BTreeMap::from([(
            DomainName("email".into()),
            DomainSettings {
                tokenize: TokenizeSetting::Off,
                range: None,
            },
        )])
    }
    fn run_with(
        rules: &[JsonPathRule],
        mode: JsonMode,
        text: &str,
        domains: &BTreeMap<DomainName, DomainSettings>,
    ) -> Result<JsonOutput, Refusal> {
        apply(
            rules,
            mode,
            &ScopeKeys::new([7; 32]),
            &StandardCodec,
            domains,
            &JsonLimits::default(),
            JsonInput {
                format: JsonFormat::PgJsonText,
                bytes: text.as_bytes(),
            },
        )
    }
    fn run(rules: &[JsonPathRule], mode: JsonMode, text: &str) -> JsonOutput {
        run_with(rules, mode, text, &domains()).unwrap()
    }
    fn doc(text: &str) -> JsonOutput {
        JsonOutput::Document(text.as_bytes().to_vec())
    }

    #[test]
    fn tokenize_example_matches_codec_and_omits_unknown_members() {
        let rules = [
            show("$.status"),
            blind("$.recipients[*].email", JsonTokenType::Text),
        ];
        let token = StandardCodec
            .encode(
                &ScopeKeys::new([7; 32]),
                &DomainName("email".into()),
                &ColumnType::Text,
                None,
                &SqlValue::Text("secret@example.com".into()),
            )
            .unwrap();
        let SqlValue::Text(token) = token else {
            panic!("expected text token")
        };
        assert_eq!(
            run(
                &rules,
                JsonMode::Tokenize,
                r#"{"status":"ok","recipients":[{"email":"secret@example.com","phone":"hidden"}],"processor_response":"hidden"}"#
            ),
            doc(&format!(
                r#"{{"status":"ok","recipients":[{{"email":"{token}"}}]}}"#
            ))
        );
    }

    #[test]
    fn show_example_masks_whole_subtree_and_shape_changes() {
        let rules = [rule("$.payment.card", JsonPathTreatment::Masked)];
        for card in [
            r#"{"number":"secret","new_key":"secret"}"#,
            r#""secret""#,
            "null",
        ] {
            assert_eq!(
                run(
                    &rules,
                    JsonMode::Show,
                    &format!(
                        r#"{{"event_type":"paid","payment":{{"card":{card},"currency":"USD"}}}}"#
                    )
                ),
                doc(r#"{"event_type":"paid","payment":{"currency":"USD"}}"#)
            );
        }
    }

    #[test]
    fn approved_leaf_null_missing_and_wrong_shapes() {
        let rules = [show("$.approved")];
        for mode in [JsonMode::Tokenize, JsonMode::Show] {
            assert_eq!(
                run(&rules, mode, r#"{"approved":null}"#),
                doc(r#"{"approved":null}"#)
            );
            for text in [
                "{}",
                r#"{"approved":{"secret":1}}"#,
                r#"{"approved":[]}"#,
                r#"{"approved":true}"#,
            ] {
                assert_eq!(run(&rules, mode, text), doc("{}"));
            }
        }
        assert_eq!(
            run(
                &[blind("$.approved", JsonTokenType::Text)],
                JsonMode::Tokenize,
                r#"{"approved":null}"#
            ),
            doc(r#"{"approved":null}"#)
        );
    }

    #[test]
    fn interior_empty_objects_and_nulls() {
        let rules = [show("$.a.b")];
        assert_eq!(run(&rules, JsonMode::Tokenize, r#"{"a":{}}"#), doc("{}"));
        assert_eq!(
            run(&rules, JsonMode::Show, r#"{"a":{}}"#),
            doc(r#"{"a":{}}"#)
        );
        assert_eq!(run(&rules, JsonMode::Tokenize, r#"{"a":null}"#), doc("{}"));
        assert_eq!(
            run(&rules, JsonMode::Show, r#"{"a":null}"#),
            doc(r#"{"a":null}"#)
        );
        for mode in [JsonMode::Tokenize, JsonMode::Show] {
            assert_eq!(run(&rules, mode, r#"{"a":"wrong shape"}"#), doc("{}"));
        }
    }

    #[test]
    fn mixed_arrays_keep_length_and_empty_object_elements() {
        let rules = [show("$.items[*].name")];
        let input = r#"{"items":[{"name":"ok"},{"name":1},{},"wrong",[],null]}"#;
        for mode in [JsonMode::Tokenize, JsonMode::Show] {
            assert_eq!(
                run(&rules, mode, input),
                doc(r#"{"items":[{"name":"ok"},{},{},null,null,null]}"#)
            );
        }
        let rules = [show("$[*]")];
        assert_eq!(
            run(&rules, JsonMode::Tokenize, r#"["ok",{},1,null]"#),
            doc(r#"["ok",null,null,null]"#)
        );
    }

    #[test]
    fn duplicate_protected_keys_refuse_unrelated_duplicates_survive_show() {
        let rules = [show("$.a.b")];
        for mode in [JsonMode::Tokenize, JsonMode::Show] {
            for text in [
                r#"{"a":{},"\u0061":{}}"#,
                r#"{"a":{"b":null,"b":"secret"}}"#,
            ] {
                let err = run_with(&rules, mode, text, &domains()).unwrap_err();
                assert_eq!(err.code, NyCode::NotAnalysable);
                assert!(!format!("{err:?} {err}").contains("secret"));
            }
        }
        assert_eq!(
            run(
                &rules,
                JsonMode::Show,
                r#"{"x":1,"x":2,"a":{"b":"ok","z":1,"z":2}}"#
            ),
            doc(r#"{"x":1,"x":2,"a":{"b":"ok","z":1,"z":2}}"#)
        );
        assert_eq!(
            run(&rules, JsonMode::Tokenize, r#"{"x":1,"x":2}"#),
            doc("{}")
        );
    }

    #[test]
    fn unmatched_root_suppresses_in_both_modes() {
        for mode in [JsonMode::Tokenize, JsonMode::Show] {
            for text in [r#""secret""#, "[]", "42", "true"] {
                assert_eq!(run(&[show("$.x")], mode, text), JsonOutput::SqlNull);
            }
        }
    }

    #[test]
    fn typed_blind_leaves_match_codec() {
        for (ty, literal) in [
            (JsonTokenType::Int4, "-123"),
            (JsonTokenType::Int8, "9223372036854775807"),
            (
                JsonTokenType::Uuid,
                r#""12345678-1234-5678-9abc-123456789abc""#,
            ),
            (JsonTokenType::Date, r#""2024-01-02""#),
            (JsonTokenType::Timestamp, r#""2024-01-02 03:04:05""#),
            (JsonTokenType::Timestamptz, r#""2024-01-02 03:04:05+00""#),
        ] {
            let column_type = column_type(ty);
            let tree = super::super::parse(literal.as_bytes(), &JsonLimits::default()).unwrap();
            let plain = sql_value(&tree, ty, &column_type).unwrap();
            let token = StandardCodec
                .encode(
                    &ScopeKeys::new([7; 32]),
                    &DomainName("email".into()),
                    &column_type,
                    None,
                    &plain,
                )
                .unwrap();
            let text = String::from_utf8(encode_text(&token).unwrap()).unwrap();
            let rendered = if matches!(ty, JsonTokenType::Int4 | JsonTokenType::Int8) {
                text
            } else {
                format!("\"{text}\"")
            };
            assert_eq!(
                run(
                    &[blind("$.x", ty)],
                    JsonMode::Tokenize,
                    &format!(r#"{{"x":{literal}}}"#)
                ),
                doc(&format!(r#"{{"x":{rendered}}}"#))
            );
        }
    }

    #[test]
    fn invalid_blind_conversions_suppress_without_tokenization() {
        for (ty, literals) in [
            (
                JsonTokenType::Int4,
                vec!["1.0", "1e0", "2147483648", r#""1""#, "true", "{}"],
            ),
            (
                JsonTokenType::Int8,
                vec!["9223372036854775808", "-9223372036854775809", "1e0"],
            ),
            (JsonTokenType::Uuid, vec![r#""secret""#, "12"]),
            (JsonTokenType::Date, vec![r#""2024-99-99""#, "12"]),
        ] {
            for literal in literals {
                assert_eq!(
                    run(
                        &[blind("$.x", ty)],
                        JsonMode::Show,
                        &format!(r#"{{"x":{literal}}}"#)
                    ),
                    doc("{}")
                );
            }
        }
    }

    #[test]
    fn quoted_keys_and_all_show_scalar_types() {
        assert_eq!(
            run(
                &[show(r#"$["a.b"]"#)],
                JsonMode::Tokenize,
                r#"{"a.b":"ok","a":{"b":"hidden"}}"#
            ),
            doc(r#"{"a.b":"ok"}"#)
        );
        let rules = [
            rule(
                "$.n",
                JsonPathTreatment::Show {
                    ty: JsonLeafType::Number,
                },
            ),
            rule(
                "$.b",
                JsonPathTreatment::Show {
                    ty: JsonLeafType::Boolean,
                },
            ),
        ];
        assert_eq!(
            run(&rules, JsonMode::Tokenize, r#"{"n":1.00e400,"b":false}"#),
            doc(r#"{"n":1.00e400,"b":false}"#)
        );
    }

    #[test]
    fn invalid_rules_and_missing_domains_refuse_without_document_text() {
        let mut mixed = vec![show("$.x"), show("$.y")];
        mixed[1].column.column = "other".into();
        for rules in [vec![], mixed, vec![blind("$.missing", JsonTokenType::Text)]] {
            let err = run_with(
                &rules,
                JsonMode::Show,
                r#"{"distinctive_secret":"distinctive_secret"}"#,
                &BTreeMap::new(),
            )
            .unwrap_err();
            assert_eq!(err.code, NyCode::NotAnalysable);
            assert!(!format!("{err:?} {err}").contains("distinctive_secret"));
        }
    }

    #[test]
    fn codec_refusal_propagates_and_uses_declared_range() {
        let domains = BTreeMap::from([(
            DomainName("email".into()),
            DomainSettings {
                tokenize: TokenizeSetting::Off,
                range: Some(TypeRange {
                    min: 0,
                    max: 2_000_000,
                }),
            },
        )]);
        let rules = [blind("$.x", JsonTokenType::Int4)];
        let plain = SqlValue::Int4(50);
        let token = StandardCodec
            .encode(
                &ScopeKeys::new([7; 32]),
                &DomainName("email".into()),
                &ColumnType::Int4,
                domains.values().next().unwrap().range.as_ref(),
                &plain,
            )
            .unwrap();
        let SqlValue::Int4(n) = token else {
            panic!("expected integer token")
        };
        assert_eq!(
            run_with(&rules, JsonMode::Tokenize, r#"{"x":50}"#, &domains).unwrap(),
            doc(&format!(r#"{{"x":{n}}}"#))
        );
        let expected = StandardCodec
            .encode(
                &ScopeKeys::new([7; 32]),
                &DomainName("email".into()),
                &ColumnType::Int4,
                domains.values().next().unwrap().range.as_ref(),
                &SqlValue::Int4(2_000_001),
            )
            .unwrap_err();
        let actual =
            run_with(&rules, JsonMode::Tokenize, r#"{"x":2000001}"#, &domains).unwrap_err();
        assert_eq!(actual, expected);
    }

    #[test]
    fn preserves_wire_formats() {
        for format in [
            JsonFormat::PgJsonText,
            JsonFormat::PgJsonbText,
            JsonFormat::PgJsonbBinary,
            JsonFormat::SpannerJson,
        ] {
            let mut bytes = Vec::new();
            if format == JsonFormat::PgJsonbBinary {
                bytes.push(1);
            }
            bytes.extend_from_slice(br#"{"x":"ok","hidden":1}"#);
            let output = apply(
                &[show("$.x")],
                JsonMode::Tokenize,
                &ScopeKeys::new([7; 32]),
                &StandardCodec,
                &domains(),
                &JsonLimits::default(),
                JsonInput {
                    format,
                    bytes: &bytes,
                },
            )
            .unwrap();
            let tree = super::super::parse(br#"{"x":"ok"}"#, &JsonLimits::default()).unwrap();
            assert_eq!(
                output,
                JsonOutput::Document(codec::encode(format, &tree).unwrap())
            );
        }
    }
}
