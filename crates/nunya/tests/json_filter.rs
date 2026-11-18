//! Public-API acceptance tests and an independently implemented JSON policy oracle.
use nunya::{
    json::{
        JsonLimits, JsonValue,
        codec::{JsonFormat, decode, encode},
        filter::{JsonInput, JsonMode, JsonOutput, apply},
        parse, render_compact,
    },
    policy::{
        DomainName, DomainSettings, JsonLeafType, JsonPathRule, JsonPathTreatment, JsonTokenType,
        PathSegment, TokenizeSetting, json_path::parse_path,
    },
    schema::ColumnRef,
    token::{ScopeKeys, StandardCodec, TokenCodec},
    value::{ColumnType, SqlValue},
};
use proptest::prelude::*;
use serde_json::{Value, json};
use std::collections::BTreeMap;

const FORMATS: [JsonFormat; 4] = [
    JsonFormat::PgJsonText,
    JsonFormat::PgJsonbText,
    JsonFormat::PgJsonbBinary,
    JsonFormat::SpannerJson,
];
fn domains() -> BTreeMap<DomainName, DomainSettings> {
    BTreeMap::from([(
        DomainName("email".into()),
        DomainSettings {
            tokenize: TokenizeSetting::Off,
            range: None,
        },
    )])
}
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

// Reference implementation: consume the rule suffixes on serde_json values. No
// production tree, walker, conversion, or codec helper participates in this oracle.
fn reference(value: &Value, rules: &[JsonPathRule], mode: JsonMode) -> Result<Option<Value>, ()> {
    let paths = rules
        .iter()
        .map(|r| (r.path.as_slice(), &r.treatment))
        .collect::<Vec<_>>();
    reference_at(value, &paths, mode, true)
}
type Grant<'a> = (&'a [PathSegment], &'a JsonPathTreatment);
fn reference_at(
    value: &Value,
    grants: &[Grant<'_>],
    mode: JsonMode,
    retain_empty: bool,
) -> Result<Option<Value>, ()> {
    if let Some((_, treatment)) = grants.iter().find(|(path, _)| path.is_empty()) {
        return reference_leaf(value, treatment);
    }
    if value.is_null() {
        return Ok((mode == JsonMode::Show).then_some(Value::Null));
    }
    if let Some(object) = value.as_object() {
        if !grants
            .iter()
            .any(|(p, _)| matches!(p.first(), Some(PathSegment::Key(_))))
        {
            return Ok(None);
        }
        let mut output = if mode == JsonMode::Show {
            object.clone()
        } else {
            Default::default()
        };
        for (key, child) in object {
            let descendants = grants
                .iter()
                .filter_map(|(p, t)| match p.split_first() {
                    Some((PathSegment::Key(name), tail)) if name == key => Some((tail, *t)),
                    _ => None,
                })
                .collect::<Vec<_>>();
            if !descendants.is_empty() {
                output.remove(key);
                if let Some(filtered) = reference_at(child, &descendants, mode, false)? {
                    output.insert(key.clone(), filtered);
                }
            }
        }
        return Ok(
            (!output.is_empty() || retain_empty || mode == JsonMode::Show)
                .then_some(Value::Object(output)),
        );
    }
    if let Some(array) = value.as_array() {
        let descendants = grants
            .iter()
            .filter_map(|(p, t)| match p.split_first() {
                Some((PathSegment::AnyElement, tail)) => Some((tail, *t)),
                _ => None,
            })
            .collect::<Vec<_>>();
        if descendants.is_empty() {
            return Ok(None);
        }
        return array
            .iter()
            .map(|child| {
                reference_at(child, &descendants, mode, true).map(|v| v.unwrap_or(Value::Null))
            })
            .collect::<Result<Vec<_>, _>>()
            .map(|a| Some(Value::Array(a)));
    }
    Ok(None)
}
fn reference_leaf(value: &Value, treatment: &JsonPathTreatment) -> Result<Option<Value>, ()> {
    match treatment {
        JsonPathTreatment::Masked => Ok(None),
        JsonPathTreatment::Show { ty } => Ok((value.is_null()
            || match ty {
                JsonLeafType::String => value.is_string(),
                JsonLeafType::Number => value.is_number(),
                JsonLeafType::Boolean => value.is_boolean(),
            })
        .then(|| value.clone())),
        JsonPathTreatment::Blind { domain, ty } => {
            if value.is_null() {
                return Ok(Some(Value::Null));
            }
            let converted = match ty {
                JsonTokenType::Text => value
                    .as_str()
                    .map(|s| (ColumnType::Text, SqlValue::Text(s.into()))),
                JsonTokenType::Int4 => value
                    .as_i64()
                    .and_then(|i| i32::try_from(i).ok())
                    .map(|i| (ColumnType::Int4, SqlValue::Int4(i))),
                JsonTokenType::Int8 => value
                    .as_i64()
                    .map(|i| (ColumnType::Int8, SqlValue::Int8(i))),
                // The generated oracle deliberately covers only Text/Int4; other
                // token kinds are rejected here rather than guessed or copied.
                JsonTokenType::Uuid
                | JsonTokenType::Date
                | JsonTokenType::Timestamp
                | JsonTokenType::Timestamptz => return Err(()),
            };
            let Some((ty, plain)) = converted else {
                return Ok(None);
            };
            let token = StandardCodec
                .encode(
                    &ScopeKeys::new([7; 32]),
                    domain,
                    &ty,
                    domains().get(domain).unwrap().range.as_ref(),
                    &plain,
                )
                .map_err(|_| ())?;
            match token {
                SqlValue::Text(s) => Ok(Some(Value::String(s))),
                SqlValue::Int4(i) => Ok(Some(json!(i))),
                SqlValue::Int8(i) => Ok(Some(json!(i))),
                _ => Err(()),
            }
        }
    }
}
fn wire(format: JsonFormat, text: &str) -> Vec<u8> {
    let mut bytes = Vec::new();
    if format == JsonFormat::PgJsonbBinary {
        bytes.push(1);
    }
    bytes.extend_from_slice(text.as_bytes());
    bytes
}
fn run(
    format: JsonFormat,
    rules: &[JsonPathRule],
    mode: JsonMode,
    text: &str,
) -> Result<Option<Value>, ()> {
    let bytes = wire(format, text);
    let output = apply(
        rules,
        mode,
        &ScopeKeys::new([7; 32]),
        &StandardCodec,
        &domains(),
        &JsonLimits::default(),
        JsonInput {
            format,
            bytes: &bytes,
        },
    )
    .map_err(|_| ())?;
    match output {
        JsonOutput::SqlNull => Ok(None),
        JsonOutput::Document(bytes) => {
            let tree = decode(format, &bytes, &JsonLimits::default()).unwrap();
            Ok(Some(serde_json::from_str(&render_compact(&tree)).unwrap()))
        }
    }
}
fn check_all(rules: &[JsonPathRule], mode: JsonMode, input: &str, expected: Option<Value>) {
    let parsed = serde_json::from_str(input).unwrap();
    assert_eq!(
        reference(&parsed, rules, mode),
        Ok(expected.clone()),
        "reference example"
    );
    for format in FORMATS {
        assert_eq!(
            run(format, rules, mode, input),
            Ok(expected.clone()),
            "{format:?}: {input}"
        );
    }
}

#[test]
fn specification_examples() {
    let rules = [
        show("$.status"),
        blind("$.recipients[*].email", JsonTokenType::Text),
    ];
    let email = reference_leaf(&json!("person@example.test"), &rules[1].treatment)
        .unwrap()
        .unwrap();
    check_all(
        &rules,
        JsonMode::Tokenize,
        r#"{"status":"ok","recipients":[{"email":"person@example.test","phone":"never-phone"}],"processor_response":"never-response"}"#,
        Some(json!({"status":"ok", "recipients":[{"email":email}]})),
    );
    let rules = [rule("$.payment.card", JsonPathTreatment::Masked)];
    check_all(
        &rules,
        JsonMode::Show,
        r#"{"event_type":"paid","payment":{"card":{"number":"secret","future":{"key":"secret"}},"currency":"USD"}}"#,
        Some(json!({"event_type":"paid","payment":{"currency":"USD"}})),
    );
    let rules = [blind("$.a.b", JsonTokenType::Text)];
    let token = reference_leaf(&json!("secret"), &rules[0].treatment)
        .unwrap()
        .unwrap();
    check_all(
        &rules,
        JsonMode::Tokenize,
        r#"{"a":{"b":"secret","sibling":"hidden"},"other":1}"#,
        Some(json!({"a":{"b":token}})),
    );
}

#[test]
fn acceptance_shapes_nulls_missing_and_masked_evolution() {
    for mode in [JsonMode::Tokenize, JsonMode::Show] {
        check_all(
            &[show("$.items[*].a")],
            mode,
            r#"{"items":[{"a":"ok"},{"a":{}},{},1,[],null]}"#,
            Some(json!({"items":[{"a":"ok"},{},{},null,null,null]})),
        );
        for rules in [[show("$.a")], [blind("$.a", JsonTokenType::Text)]] {
            for (input, expected) in [
                ("{}", json!({})),
                (r#"{"a":null}"#, json!({"a":null})),
                (r#"{"a":{"new_secret":"hidden"}}"#, json!({})),
            ] {
                check_all(&rules, mode, input, Some(expected));
            }
        }
        for input in [r#"{"a":null}"#, r#"{"a":{}}"#] {
            let expected = if mode == JsonMode::Show {
                serde_json::from_str(input).unwrap()
            } else {
                json!({})
            };
            check_all(&[show("$.a.b")], mode, input, Some(expected));
        }
        for root in ["42", "true", "[]", r#""wrong""#] {
            check_all(&[show("$.a")], mode, root, None);
        }
    }
    for card in [
        r#"{"future_key":{"nested":"secret"}}"#,
        r#""secret""#,
        "null",
    ] {
        check_all(
            &[rule("$.payment.card", JsonPathTreatment::Masked)],
            JsonMode::Show,
            &format!(r#"{{"event_type":"new","payment":{{"card":{card}}}}}"#),
            Some(json!({"event_type":"new", "payment":{}})),
        );
    }
}

#[test]
fn unknown_keys_absent_from_output_bytes_and_input_order_kept() {
    let text = r#"{"c":"C","dynamic_unknown_marker":{"a":"hidden"},"a":"A","b":{"unknown_nested_marker":true}}"#;
    let rules = [show("$.a"), show("$.c"), show("$.b.approved")];
    for format in FORMATS {
        let bytes = wire(format, text);
        let JsonOutput::Document(output) = apply(
            &rules,
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
        .unwrap() else {
            panic!("expected document")
        };
        for marker in ["dynamic_unknown_marker", "unknown_nested_marker"] {
            assert!(!output.windows(marker.len()).any(|w| w == marker.as_bytes()));
        }
        let tree = decode(format, &output, &JsonLimits::default()).unwrap();
        assert_eq!(render_compact(&tree), r#"{"c":"C","a":"A"}"#);
    }
}

#[test]
fn duplicate_protected_refused_unrelated_duplicates_preserved() {
    for mode in [JsonMode::Show, JsonMode::Tokenize] {
        for input in [r#"{"a":{},"\u0061":{}}"#, r#"{"a":{"b":"one","b":"two"}}"#] {
            assert_eq!(
                run(JsonFormat::PgJsonText, &[show("$.a.b")], mode, input),
                Err(())
            );
        }
    }
    let input = br#"{"x":1,"x":2,"a":{"b":"ok","z":1,"z":2}}"#;
    let result = apply(
        &[show("$.a.b")],
        JsonMode::Show,
        &ScopeKeys::new([7; 32]),
        &StandardCodec,
        &domains(),
        &JsonLimits::default(),
        JsonInput {
            format: JsonFormat::PgJsonText,
            bytes: input,
        },
    )
    .unwrap();
    assert_eq!(result, JsonOutput::Document(input.to_vec()));
}

#[test]
fn quoted_key_is_not_a_nested_path() {
    check_all(
        &[show(r#"$["a.b"]"#)],
        JsonMode::Tokenize,
        r#"{"a.b":"literal","a":{"b":"nested"}}"#,
        Some(json!({"a.b":"literal"})),
    );
    check_all(
        &[show("$.a.b")],
        JsonMode::Tokenize,
        r#"{"a.b":"literal","a":{"b":"nested"}}"#,
        Some(json!({"a":{"b":"nested"}})),
    );
    check_all(
        &[show(r#"$["a.b"]"#)],
        JsonMode::Tokenize,
        r#"{"a":{"b":"nested"}}"#,
        Some(json!({})),
    );
}

#[test]
fn integer_literal_conversion_and_range_boundaries() {
    for mode in [JsonMode::Show, JsonMode::Tokenize] {
        for ty in [JsonTokenType::Int4, JsonTokenType::Int8] {
            for literal in [
                "1.0",
                "1e0",
                "9223372036854775808",
                "-9223372036854775809",
                "true",
                "{}",
                r#""1""#,
            ] {
                check_all(
                    &[blind("$.a", ty)],
                    mode,
                    &format!(r#"{{"a":{literal}}}"#),
                    Some(json!({})),
                );
            }
        }
        for literal in ["2147483648", "-2147483649"] {
            check_all(
                &[blind("$.a", JsonTokenType::Int4)],
                mode,
                &format!(r#"{{"a":{literal}}}"#),
                Some(json!({})),
            );
        }
        for (ty, n) in [
            (JsonTokenType::Int4, i64::from(i32::MIN)),
            (JsonTokenType::Int4, i64::from(i32::MAX)),
            (JsonTokenType::Int8, i64::MIN),
            (JsonTokenType::Int8, i64::MAX),
        ] {
            let rules = [blind("$.a", ty)];
            let input = json!({"a":n});
            let expected = reference(&input, &rules, mode).unwrap();
            check_all(&rules, mode, &input.to_string(), expected);
        }
    }
}

fn document() -> BoxedStrategy<Value> {
    let leaf = prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::Bool),
        (-10000i64..10000).prop_map(|i| json!(i)),
        (-10000i32..10000).prop_map(|i| json!(f64::from(i) + 0.5)),
        "[a-zA-Z0-9@.]{0,16}".prop_map(Value::String)
    ];
    leaf.prop_recursive(5, 80, 5, |inner| {
        prop_oneof![
            proptest::collection::vec(inner.clone(), 0..5).prop_map(Value::Array),
            proptest::collection::btree_map(
                prop::sample::select(vec!["a", "b", "c", "x.y", "extra"]),
                inner,
                0..5
            )
            .prop_map(|o| Value::Object(o.into_iter().map(|(k, v)| (k.into(), v)).collect()))
        ]
    })
    .boxed()
}
fn generated_rules() -> BoxedStrategy<Vec<JsonPathRule>> {
    // Prefix filtering keeps paths nonoverlapping even with mixed key/array steps.
    proptest::collection::vec((proptest::collection::vec(0u8..5, 1..5), 0u8..6), 1..7)
        .prop_map(|candidates| {
            let mut result: Vec<JsonPathRule> = Vec::new();
            for (steps, treatment) in candidates {
                let mut r = show("$.a");
                r.path = steps
                    .into_iter()
                    .map(|s| match s {
                        0 => PathSegment::Key("a".into()),
                        1 => PathSegment::Key("b".into()),
                        2 => PathSegment::Key("c".into()),
                        3 => PathSegment::Key("x.y".into()),
                        4 => PathSegment::AnyElement,
                        _ => unreachable!("bounded path generator"),
                    })
                    .collect();
                r.treatment = match treatment {
                    0 => JsonPathTreatment::Show {
                        ty: JsonLeafType::String,
                    },
                    1 => JsonPathTreatment::Show {
                        ty: JsonLeafType::Number,
                    },
                    2 => JsonPathTreatment::Show {
                        ty: JsonLeafType::Boolean,
                    },
                    3 => JsonPathTreatment::Blind {
                        domain: DomainName("email".into()),
                        ty: JsonTokenType::Text,
                    },
                    4 => JsonPathTreatment::Blind {
                        domain: DomainName("email".into()),
                        ty: JsonTokenType::Int4,
                    },
                    5 => JsonPathTreatment::Masked,
                    _ => unreachable!("bounded treatment generator"),
                };
                if !result
                    .iter()
                    .any(|old| old.path.starts_with(&r.path) || r.path.starts_with(&old.path))
                {
                    result.push(r);
                }
            }
            result
        })
        .boxed()
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 2000, failure_persistence: None, .. ProptestConfig::default() })]
    #[test]
    fn generated_documents_match_reference_and_codec_roundtrips(value in document(), rules in generated_rules()) {
        let text = value.to_string();
        let tree: JsonValue = parse(text.as_bytes(), &JsonLimits::default()).unwrap();
        for format in FORMATS {
            let bytes = encode(format, &tree).unwrap();
            if format == JsonFormat::PgJsonbBinary { prop_assert_eq!(bytes.first(), Some(&1)); }
            prop_assert_eq!(decode(format, &bytes, &JsonLimits::default()).unwrap(), tree.clone());
            for mode in [JsonMode::Tokenize, JsonMode::Show] {
                prop_assert_eq!(run(format, &rules, mode, &text), reference(&value, &rules, mode),
                    "format={:?}, input={}, rules={:?}", format, text, rules);
            }
        }
    }
}

#[test]
fn limits_refuse_without_document_in_diagnostics() {
    let marker = "private_document_marker";
    let limits = JsonLimits::default();
    let deep = format!(
        "{}\"{marker}\"{}",
        "[".repeat(limits.max_depth + 1),
        "]".repeat(limits.max_depth + 1)
    );
    let oversized = format!(r#"{{"a":"{marker}"}}"#);
    for format in FORMATS {
        for (text, limits) in [
            (&deep, limits),
            (
                &oversized,
                JsonLimits {
                    max_bytes: 8,
                    ..limits
                },
            ),
        ] {
            let bytes = wire(format, text);
            let err = apply(
                &[show("$.a")],
                JsonMode::Show,
                &ScopeKeys::new([7; 32]),
                &StandardCodec,
                &domains(),
                &limits,
                JsonInput {
                    format,
                    bytes: &bytes,
                },
            )
            .unwrap_err();
            for message in [format!("{err:?}"), format!("{err}")] {
                assert!(!message.contains(marker));
                assert!(!message.contains(text));
            }
        }
    }
}
