//! JSON wire codecs. PostgreSQL `json` source whitespace and string escapes are
//! normalized when re-rendered; order, duplicate members and number text survive.

use super::{JsonLimits, JsonValue, parse, render_compact, render_pg_jsonb};
use crate::error::{NyCode, Refusal};

/// JSON formats delivered by supported database protocols.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonFormat {
    /// PostgreSQL json in text format.
    PgJsonText,
    /// PostgreSQL jsonb in text format.
    PgJsonbText,
    /// PostgreSQL jsonb binary version one.
    PgJsonbBinary,
    /// Spanner JSON string UTF-8 bytes.
    SpannerJson,
}

fn check_jsonb(v: &JsonValue) -> Result<(), Refusal> {
    let mut pending = vec![v];
    while let Some(v) = pending.pop() {
        match v {
            JsonValue::String(s) if s.contains('\0') => return Err(nul_refusal()),
            JsonValue::Array(elements) => pending.extend(elements),
            JsonValue::Object(members) => {
                for (key, value) in members {
                    if key.contains('\0') {
                        return Err(nul_refusal());
                    }
                    pending.push(value);
                }
            }
            JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::String(_) => {}
        }
    }
    Ok(())
}

fn nul_refusal() -> Refusal {
    Refusal::new(NyCode::NotAnalysable, "unsupported JSON string character")
}

/// Decode a protocol JSON value under the supplied document limits.
pub fn decode(format: JsonFormat, bytes: &[u8], limits: &JsonLimits) -> Result<JsonValue, Refusal> {
    let text = match format {
        JsonFormat::PgJsonbBinary => bytes.strip_prefix(&[1]).ok_or_else(|| {
            Refusal::new(NyCode::NotAnalysable, "unsupported JSON binary version")
        })?,
        JsonFormat::PgJsonText | JsonFormat::PgJsonbText | JsonFormat::SpannerJson => bytes,
    };
    let value = parse(text, limits)?;
    match format {
        JsonFormat::PgJsonbText | JsonFormat::PgJsonbBinary => check_jsonb(&value)?,
        JsonFormat::PgJsonText | JsonFormat::SpannerJson => {}
    }
    Ok(value)
}

/// Encode a JSON tree, refusing null characters in jsonb keys and strings.
pub fn encode(format: JsonFormat, v: &JsonValue) -> Result<Vec<u8>, Refusal> {
    match format {
        JsonFormat::PgJsonText | JsonFormat::SpannerJson => Ok(render_compact(v).into_bytes()),
        JsonFormat::PgJsonbText | JsonFormat::PgJsonbBinary => {
            check_jsonb(v)?;
            let mut bytes = Vec::new();
            if format == JsonFormat::PgJsonbBinary {
                bytes.push(1);
            }
            bytes.extend_from_slice(render_pg_jsonb(v).as_bytes());
            Ok(bytes)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const FORMATS: [JsonFormat; 4] = [
        JsonFormat::PgJsonText,
        JsonFormat::PgJsonbText,
        JsonFormat::PgJsonbBinary,
        JsonFormat::SpannerJson,
    ];

    #[test]
    fn roundtrips_and_rendering() {
        for format in FORMATS {
            for text in [
                r#" {"a":1,"b":[true,null]} "#,
                r#"{"a":1,"a":2}"#,
                r#"["\uD83D\uDE00",-0,1e400,1.0,{},[]]"#,
            ] {
                let mut input = Vec::new();
                if format == JsonFormat::PgJsonbBinary {
                    input.push(1);
                }
                input.extend_from_slice(text.as_bytes());
                let tree = decode(format, &input, &JsonLimits::default()).unwrap();
                let encoded = encode(format, &tree).unwrap();
                assert_eq!(
                    decode(format, &encoded, &JsonLimits::default()).unwrap(),
                    tree
                );
            }
        }
        let tree = parse(br#"{"a":1,"b":[true,null]}"#, &JsonLimits::default()).unwrap();
        let spaced = br#"{"a": 1, "b": [true, null]}"#;
        assert_eq!(encode(JsonFormat::PgJsonbText, &tree).unwrap(), spaced);
        let binary = encode(JsonFormat::PgJsonbBinary, &tree).unwrap();
        assert_eq!(binary[0], 1);
        assert_eq!(&binary[1..], spaced);
        assert_eq!(
            encode(JsonFormat::PgJsonText, &tree).unwrap(),
            br#"{"a":1,"b":[true,null]}"#
        );
    }

    #[test]
    fn binary_version_refused() {
        for bytes in [&b""[..], &b"\x02{}"[..], &b"\x00{}"[..], &b"\x01"[..]] {
            assert!(decode(JsonFormat::PgJsonbBinary, bytes, &JsonLimits::default()).is_err());
        }
    }

    #[test]
    fn jsonb_rejects_nul_in_keys_and_nested_strings() {
        for text in [
            r#"{"\u0000":1}"#,
            r#"{"a":[{"b":"distinctive_secret_marker\u0000"}]}"#,
        ] {
            let value = parse(text.as_bytes(), &JsonLimits::default()).unwrap();
            for format in FORMATS {
                match format {
                    JsonFormat::PgJsonbText | JsonFormat::PgJsonbBinary => {
                        let err = encode(format, &value).unwrap_err();
                        assert!(!format!("{err:?} {err}").contains("distinctive_secret_marker"));
                        let mut bytes = Vec::new();
                        if format == JsonFormat::PgJsonbBinary {
                            bytes.push(1);
                        }
                        bytes.extend_from_slice(text.as_bytes());
                        assert!(decode(format, &bytes, &JsonLimits::default()).is_err());
                    }
                    JsonFormat::PgJsonText | JsonFormat::SpannerJson => {
                        let bytes = encode(format, &value).unwrap();
                        assert_eq!(
                            decode(format, &bytes, &JsonLimits::default()).unwrap(),
                            value
                        );
                    }
                }
            }
        }
    }
}
