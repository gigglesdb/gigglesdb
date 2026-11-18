//! JSON policy path grammar and declaration types.
use super::{DomainName, file::PolicyError};
use crate::schema::ColumnRef;

/// A normalized JSON path step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathSegment {
    /// Object key.
    Key(String),
    /// Every element of an array.
    AnyElement,
}
/// Plaintext JSON leaf type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonLeafType {
    /// JSON string.
    String,
    /// JSON number.
    Number,
    /// JSON boolean.
    Boolean,
}
/// SQL scalar type used to tokenize a JSON leaf.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonTokenType {
    /// Text scalar.
    Text,
    /// Signed 32-bit integer.
    Int4,
    /// Signed 64-bit integer.
    Int8,
    /// UUID scalar.
    Uuid,
    /// Calendar date.
    Date,
    /// Timestamp without time zone.
    Timestamp,
    /// Timestamp with time zone.
    Timestamptz,
}
/// Treatment declared for one JSON path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JsonPathTreatment {
    /// Explicit plaintext leaf grant.
    Show {
        /// Required JSON leaf type.
        ty: JsonLeafType,
    },
    /// Tokenized leaf grant.
    Blind {
        /// Declared token domain.
        domain: DomainName,
        /// Required SQL token type.
        ty: JsonTokenType,
    },
    /// Mask the entire subtree.
    Masked,
}
/// A JSON declaration containing policy names and paths only.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsonPathRule {
    /// Owning column.
    pub column: ColumnRef,
    /// Normalized path steps.
    pub path: Vec<PathSegment>,
    /// Declared leaf or subtree treatment.
    pub treatment: JsonPathTreatment,
}

/// Parse the restricted policy JSON path grammar without echoing invalid input.
pub fn parse_path(path: &str) -> Result<Vec<PathSegment>, PolicyError> {
    let invalid = || PolicyError {
        message: "json_path: invalid path".into(),
    };
    let bytes = path.as_bytes();
    if bytes.first() != Some(&b'$') {
        return Err(invalid());
    }
    let mut i = 1;
    let mut result = Vec::new();
    while i < bytes.len() {
        if bytes[i] == b'.' {
            i += 1;
            let start = i;
            if !bytes
                .get(i)
                .is_some_and(|b| b.is_ascii_alphabetic() || *b == b'_')
            {
                return Err(invalid());
            }
            i += 1;
            while bytes
                .get(i)
                .is_some_and(|b| b.is_ascii_alphanumeric() || *b == b'_')
            {
                i += 1;
            }
            result.push(PathSegment::Key(path[start..i].into()));
        } else if bytes[i..].starts_with(b"[*]") {
            result.push(PathSegment::AnyElement);
            i += 3;
        } else if bytes[i..].starts_with(b"[\"") {
            i += 1;
            let start = i;
            i += 1;
            let mut closed = false;
            while i < bytes.len() {
                match bytes[i] {
                    b'\\' => {
                        // Only JSON's quote and backslash key escapes are admitted.
                        if !matches!(bytes.get(i + 1), Some(b'"' | b'\\')) {
                            return Err(invalid());
                        }
                        i += 2;
                    }
                    b'"' => {
                        i += 1;
                        closed = true;
                        break;
                    }
                    0..=31 => return Err(invalid()),
                    _ => i += 1,
                }
            }
            if !closed || bytes.get(i) != Some(&b']') {
                return Err(invalid());
            }
            let key: String = serde_json::from_str(&path[start..i]).map_err(|_| invalid())?;
            if key.is_empty() {
                return Err(invalid());
            }
            result.push(PathSegment::Key(key));
            i += 1;
        } else {
            return Err(invalid());
        }
    }
    if result.is_empty() {
        return Err(invalid());
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn normalized_keys_and_wildcards() {
        assert_eq!(parse_path("$.a"), parse_path(r#"$["a"]"#));
        assert_eq!(
            parse_path(r#"$["a.b"]"#).unwrap(),
            vec![PathSegment::Key("a.b".into())]
        );
        assert_eq!(
            parse_path("$.recipients[*].email").unwrap(),
            vec![
                PathSegment::Key("recipients".into()),
                PathSegment::AnyElement,
                PathSegment::Key("email".into())
            ]
        );
        assert_eq!(
            parse_path(r#"$["a\"b\\c"]"#).unwrap(),
            vec![PathSegment::Key("a\"b\\c".into())]
        );
        assert!(parse_path("$[\"\u{e9} key\"]").is_ok());
    }
    #[test]
    fn rejects_invalid_paths() {
        for path in [
            "",
            "$",
            "a",
            "$..a",
            "$[0]",
            "$[?(@.a)]",
            "$.a.",
            r#"$[""]"#,
            "$. a",
            " $.a",
            "$.a ",
            "$[ *]",
            r#"$["a" ]"#,
            "$._a[",
            r#"$["a\n"]"#,
            r#"$["a\"]"#,
        ] {
            assert!(parse_path(path).is_err(), "invalid path accepted");
        }
    }
}
