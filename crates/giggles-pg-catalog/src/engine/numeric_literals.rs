//! Normalize numeric tokens before sqlparser can mistake their suffix for an alias.
use crate::{Error, R};
use std::borrow::Cow;

fn ident_start(b: u8) -> bool {
    b.is_ascii_alphabetic() || b == b'_' || b >= 0x80
}
fn ident(b: u8) -> bool {
    ident_start(b) || b.is_ascii_digit() || b == b'$'
}

pub(super) fn normalize(sql: &str) -> R<Cow<'_, str>> {
    let bytes = sql.as_bytes();
    let mut pos = 0;
    let mut copied = 0;
    let mut output = String::new();
    while pos < bytes.len() {
        match bytes[pos] {
            b'-' if bytes.get(pos + 1) == Some(&b'-') => {
                pos += 2;
                while pos < bytes.len() && !matches!(bytes[pos], b'\n' | b'\r') {
                    pos += 1;
                }
            }
            b'/' if bytes.get(pos + 1) == Some(&b'*') => {
                pos += 2;
                let mut depth = 1;
                while pos < bytes.len() && depth != 0 {
                    if bytes[pos..].starts_with(b"/*") {
                        depth += 1;
                        pos += 2;
                    } else if bytes[pos..].starts_with(b"*/") {
                        depth -= 1;
                        pos += 2;
                    } else {
                        pos += 1;
                    }
                }
                if depth != 0 {
                    return Err(Error::Syntax);
                }
            }
            quote @ (b'\'' | b'"') => {
                let escape = quote == b'\''
                    && pos > 0
                    && matches!(bytes[pos - 1], b'e' | b'E')
                    && (pos == 1 || !ident(bytes[pos - 2]));
                pos += 1;
                loop {
                    let Some(&b) = bytes.get(pos) else {
                        return Err(Error::Syntax);
                    };
                    pos += 1;
                    if escape && b == b'\\' {
                        pos = (pos + 1).min(bytes.len());
                    } else if b == quote {
                        if bytes.get(pos) == Some(&quote) {
                            pos += 1;
                        } else {
                            break;
                        }
                    }
                }
            }
            b'$' => {
                let start = pos;
                pos += 1;
                if bytes.get(pos).is_some_and(|b| ident_start(*b)) {
                    while bytes
                        .get(pos)
                        .is_some_and(|b| ident_start(*b) || b.is_ascii_digit())
                    {
                        pos += 1;
                    }
                }
                if bytes.get(pos) == Some(&b'$') {
                    pos += 1;
                    let delimiter = &sql[start..pos];
                    let end = sql[pos..].find(delimiter).ok_or(Error::Syntax)?;
                    pos += end + delimiter.len();
                } else {
                    // Parameters are not numeric literals.
                    while bytes.get(pos).is_some_and(|b| ident(*b)) {
                        pos += 1;
                    }
                }
            }
            b if ident_start(b) => {
                pos += 1;
                while bytes.get(pos).is_some_and(|b| ident(*b)) {
                    pos += 1;
                }
            }
            b if b.is_ascii_digit()
                || (b == b'.' && bytes.get(pos + 1).is_some_and(u8::is_ascii_digit)) =>
            {
                let (number, end) =
                    giggles_pg_types::numeric_literal(sql, pos).map_err(|e| match e {
                        giggles_pg_types::Error::Limit => Error::Limit,
                        _ => Error::Syntax,
                    })?;
                if sql[pos..end] != number {
                    output.push_str(&sql[copied..pos]);
                    output.push_str(&number);
                    copied = end;
                }
                pos = end;
            }
            _ => pos += 1,
        }
    }
    if copied == 0 {
        Ok(Cow::Borrowed(sql))
    } else {
        output.push_str(&sql[copied..]);
        Ok(Cow::Owned(output))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan_gateway;

    fn assert_numeric_result(sql: &str, expected: &[(&str, u32)]) {
        let plan = plan_gateway(sql).unwrap().expect("catalog literal plan");
        let fields = crate::describe(&plan).unwrap();
        assert_eq!(fields.len(), expected.len(), "{sql}");
        for (field, (_, oid)) in fields.iter().zip(expected) {
            assert_eq!(field.name, "?column?", "{sql}");
            assert_eq!(field.data_type.oid(), *oid, "{sql}");
        }
        let result = crate::execute(&plan, &crate::CatalogSnapshot::default(), &[]).unwrap();
        assert_eq!(result.fields, fields, "{sql}");
        let values = expected
            .iter()
            .map(|(text, oid)| match oid {
                1700 => crate::Value::Numeric(giggles_pg_types::Numeric::parse(text).unwrap()),
                23 | 20 => crate::Value::Int(text.parse().unwrap()),
                _ => panic!("unexpected type OID"),
            })
            .collect::<Vec<_>>();
        assert_eq!(result.rows, vec![values], "{sql}");
        // Numeric equality ignores scale, so also check the exact rendered text.
        for (value, (text, _)) in result.rows[0].iter().zip(expected) {
            if let crate::Value::Numeric(value) = value {
                assert_eq!(value.to_string(), *text, "{sql}");
            }
        }
    }

    #[test]
    fn gateway_decimal_literals_have_numeric_values_and_oids() {
        assert_numeric_result("SELECT 1.5, 2;", &[("1.5", 1700), ("2", 23)]);
        for (literal, text) in [
            ("0.5", "0.5"),
            ("1000.2500", "1000.2500"),
            ("12e10", "120000000000"),
            ("1.5e-3", "0.0015"),
            (".50", "0.50"),
            ("1.E+2", "100"),
        ] {
            assert_numeric_result(&format!("SELECT {literal};"), &[(text, 1700)]);
        }
        assert_numeric_result("SELECT 2147483648;", &[("2147483648", 20)]);
    }

    #[test]
    fn gateway_mixed_numeric_spellings_keep_values_scale_and_oids() {
        assert_numeric_result(
            "SELECT 12_345_678, 1_000.25_00, 1_2e1_0, 0xF_F, 0o7_7, 0b10_01;",
            &[
                ("12345678", 23),
                ("1000.2500", 1700),
                ("120000000000", 1700),
                ("255", 23),
                ("63", 23),
                ("9", 23),
            ],
        );
    }

    #[test]
    fn catalog_relation_decimal_literals_have_numeric_values_and_oids() {
        for (literal, text) in [
            ("1000.2500", "1000.2500"),
            ("12e10", "120000000000"),
            ("1.5e-3", "0.0015"),
            ("1_000.25_00", "1000.2500"),
        ] {
            assert_numeric_result(
                &format!(
                    "SELECT {literal} FROM pg_catalog.pg_namespace WHERE nspname = 'pg_catalog'"
                ),
                &[(text, 1700)],
            );
        }
    }

    #[test]
    fn gateway_numeric_literals_execute_without_suffix_aliases() {
        for (sql, expected) in [
            ("SELECT 1_000", "SELECT 1000"),
            ("SELECT 1_000, 2", "SELECT 1000, 2"),
            ("SELECT 0x10", "SELECT 16"),
            ("SELECT 0xF_F + 1", "SELECT 255 + 1"),
            ("SELECT 0o7_7, 0b10_01", "SELECT 63, 9"),
        ] {
            assert_eq!(normalize(sql).unwrap(), expected);
            let actual = plan_gateway(sql).unwrap();
            let canonical = plan_gateway(expected).unwrap();
            match (actual, canonical) {
                (Some(actual), Some(canonical)) => {
                    let snapshot = crate::CatalogSnapshot::default();
                    let result = crate::execute(&actual, &snapshot, &[]).unwrap();
                    assert_eq!(result, crate::execute(&canonical, &snapshot, &[]).unwrap());
                    assert!(result.fields.iter().all(|field| field.name == "?column?"));
                    let values = match sql {
                        "SELECT 1_000" => vec![crate::Value::Int(1000)],
                        "SELECT 1_000, 2" => vec![crate::Value::Int(1000), crate::Value::Int(2)],
                        "SELECT 0x10" => vec![crate::Value::Int(16)],
                        "SELECT 0o7_7, 0b10_01" => {
                            vec![crate::Value::Int(63), crate::Value::Int(9)]
                        }
                        _ => panic!("unexpected catalog route: {sql}"),
                    };
                    assert_eq!(result.rows, vec![values], "{sql}");
                }
                (None, None) => {}
                _ => panic!("numeric spelling changed gateway routing: {sql}"),
            }
        }
    }

    #[test]
    fn normalizes_decimal_fraction_and_exponent_separators() {
        assert_eq!(
            normalize("SELECT .5_0, 1_2e1_0").unwrap(),
            "SELECT .50, 12e10"
        );
    }

    #[test]
    fn rejects_invalid_numeric_spelling_before_alias_parsing() {
        for literal in ["1abc", "1__0", "0x", "1_", "0b2", "1e+", "1\u{e9}"] {
            let sql = format!("SELECT {literal}");
            assert!(matches!(plan_gateway(&sql), Err(Error::Syntax)), "{sql}");
        }
    }

    #[test]
    fn preserves_non_numeric_tokens_and_plain_decimals_byte_for_byte() {
        for sql in [
            "SELECT '1_000'",
            "SELECT 'a''1_0'",
            "SELECT E'a\\'1_0'",
            "SELECT \"a1_0\"",
            "SELECT \"a\"\"1_0\"",
            "SELECT $$1_0$$",
            "SELECT $tag1$1__0$tag1$",
            "SELECT 1 -- 1_0",
            "SELECT t1 FROM t2",
            "SELECT $1, $12",
            "SELECT t$1_0, caf\u{e9}1_0 FROM t1",
            "SELECT 1 /* 1__0 /* 0x */ 1abc */",
            "SELECT 001.00, .5, 1.e+02",
        ] {
            assert!(
                matches!(normalize(sql).unwrap(), Cow::Borrowed(s) if s == sql),
                "{sql}"
            );
        }
    }

    #[test]
    fn resumes_after_each_quoted_or_commented_region() {
        for (sql, expected) in [
            ("SELECT '1_0', 1_0", "SELECT '1_0', 10"),
            ("SELECT E'a\\'1_0', 0x10", "SELECT E'a\\'1_0', 16"),
            ("SELECT $$1_0$$, 1_0", "SELECT $$1_0$$, 10"),
            ("SELECT $t$1_0$t$, 1_0", "SELECT $t$1_0$t$, 10"),
            ("SELECT \"a1_0\", 1_0", "SELECT \"a1_0\", 10"),
            ("SELECT 1 -- 1__0\n, 1_0", "SELECT 1 -- 1__0\n, 10"),
            (
                "SELECT /* /* 0x */ 1_0 */ 1_0",
                "SELECT /* /* 0x */ 1_0 */ 10",
            ),
        ] {
            assert_eq!(normalize(sql).unwrap(), expected, "{sql}");
        }
    }
}
