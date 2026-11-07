// PostgreSQL REL_18_0-derived algorithm/data.
// Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
// Portions Copyright (c) 1994, Regents of the University of California
// PostgreSQL Database Management System
// (also known as Postgres, formerly known as Postgres95)
//
// Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//
// Portions Copyright (c) 1994, The Regents of the University of California
//
// Permission to use, copy, modify, and distribute this software and its
// documentation for any purpose, without fee, and without a written agreement
// is hereby granted, provided that the above copyright notice and this
// paragraph and the following two paragraphs appear in all copies.
//
// IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
// DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
// LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
// DOCUMENTATION, EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.
//
// THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
// AND FITNESS FOR A PARTICULAR PURPOSE.  THE SOFTWARE PROVIDED HEREUNDER IS
// ON AN "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO
// PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
//! Bounded PostgreSQL18 text quoting under quote_all_identifiers=off.
//! Nontext polymorphic siblings and nondefault identifier quoting are outside
//! this profile. No keyword lookup touches the live catalog or SQL lexer.
use super::*;
#[path = "quoting_keywords.rs"]
mod keywords;

pub(super) fn keyword_requires_quotes(name: &str) -> bool {
    keywords::REQUIRES_QUOTES.binary_search(&name).is_ok()
}

type Signature = (Vec<Option<DataType>>, DataType);

pub(super) fn known_name(name: &str) -> bool {
    matches!(name, "quote_ident" | "quote_literal" | "quote_nullable")
}

pub(super) fn signature(
    name: &str,
    count: usize,
    argument: Option<&DataType>,
) -> Option<R<Signature>> {
    if !known_name(name) {
        return None;
    }
    Some(if count != 1 {
        Err(SqlError::UndefinedFunction)
    } else if argument.is_some_and(|ty| *ty != DataType::Text) {
        // quote_ident has no nontext overload. The other two do have stable
        // anyelement siblings, but their context-dependent formatting is not
        // implemented by this explicit text profile.
        Err(if name == "quote_ident" {
            SqlError::UndefinedFunction
        } else {
            SqlError::Unsupported
        })
    } else {
        Ok((vec![Some(DataType::Text)], DataType::Text))
    })
}

pub(super) fn eval(name: &str, values: &[Value], budget: &mut usize) -> Option<R<Value>> {
    if !known_name(name) {
        return None;
    }
    Some((|| {
        if values.len() != 1 {
            return Err(SqlError::UndefinedFunction);
        }
        match &values[0] {
            Value::Null if name == "quote_nullable" => {
                reserve(budget, 4)?;
                Ok(Value::Text("NULL".into()))
            }
            Value::Null => Ok(Value::Null),
            Value::Text(input) => quote(name, input, budget),
            _ => Err(if name == "quote_ident" {
                SqlError::UndefinedFunction
            } else {
                SqlError::Unsupported
            }),
        }
    })())
}

fn quote(name: &str, input: &str, budget: &mut usize) -> R<Value> {
    // Cap the scan before inspecting any bytes. Only two bounded scans occur:
    // exact-size/classification, then the one admitted output construction.
    if input.len() > MAX_ROW_BYTES {
        return Err(SqlError::Limit);
    }
    let identifier = name == "quote_ident";
    let mut safe = identifier
        && input
            .as_bytes()
            .first()
            .is_some_and(|c| c.is_ascii_lowercase() || *c == b'_');
    let mut doubles = 0usize;
    let mut backslash = false;
    for byte in input.bytes() {
        if identifier {
            if !(byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_') {
                safe = false;
            }
            if byte == b'"' {
                doubles = doubles.checked_add(1).ok_or(SqlError::Limit)?;
            }
        } else if byte == b'\'' || byte == b'\\' {
            doubles = doubles.checked_add(1).ok_or(SqlError::Limit)?;
            backslash |= byte == b'\\';
        }
    }
    if safe && keyword_requires_quotes(input) {
        safe = false;
    }
    let delimiters = if identifier && safe { 0 } else { 2 };
    let prefix = usize::from(!identifier && backslash);
    let size = input
        .len()
        .checked_add(if safe { 0 } else { doubles })
        .and_then(|n| n.checked_add(delimiters))
        .and_then(|n| n.checked_add(prefix))
        .ok_or(SqlError::Limit)?;
    reserve(budget, size)?;
    let mut output = String::with_capacity(size);
    if safe {
        output.push_str(input);
    } else {
        if backslash && !identifier {
            output.push('E');
        }
        let delimiter = if identifier { '"' } else { '\'' };
        output.push(delimiter);
        for character in input.chars() {
            if character == delimiter || !identifier && character == '\\' {
                output.push(character);
            }
            output.push(character);
        }
        output.push(delimiter);
    }
    debug_assert_eq!(output.len(), size);
    Ok(Value::Text(output))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn quote_semantics_keywords_unicode_and_null_are_explicit() {
        for (name, input, expected) in [
            ("quote_ident", "abort", "abort"),
            ("quote_ident", "select", "\"select\""),
            ("quote_ident", "between", "\"between\""),
            ("quote_ident", "authorization", "\"authorization\""),
            ("quote_ident", "_safe123", "_safe123"),
            ("quote_ident", "a\"b", "\"a\"\"b\""),
            ("quote_ident", "\u{00e9}", "\"\u{00e9}\""),
            ("quote_ident", "", "\"\""),
            ("quote_literal", "", "''"),
            ("quote_literal", "a'b", "'a''b'"),
            ("quote_literal", "a\\b", "E'a\\\\b'"),
            ("quote_nullable", "x'\\y", "E'x''\\\\y'"),
        ] {
            let value = Value::Text(input.into());
            let original = value.clone();
            let mut budget = 0;
            assert_eq!(
                eval(name, std::slice::from_ref(&value), &mut budget).unwrap(),
                Ok(Value::Text(expected.into()))
            );
            assert_eq!(budget, expected.len());
            assert_eq!(value, original);
        }
        for name in ["quote_ident", "quote_literal"] {
            let mut budget = 17;
            assert_eq!(
                eval(name, &[Value::Null], &mut budget).unwrap(),
                Ok(Value::Null)
            );
            assert_eq!(budget, 17);
        }
        let mut budget = 0;
        assert_eq!(
            eval("quote_nullable", &[Value::Null], &mut budget).unwrap(),
            Ok(Value::Text("NULL".into()))
        );
        assert_eq!(budget, 4);
        assert_eq!(keywords::REQUIRES_QUOTES.len(), 164);
        assert!(keywords::REQUIRES_QUOTES.windows(2).all(|w| w[0] < w[1]));
    }
    #[test]
    fn exact_expansion_and_shared_budget_admit_before_output_allocation() {
        for (name, input, bytes) in [
            ("quote_ident", "a".repeat(MAX_ROW_BYTES), MAX_ROW_BYTES),
            (
                "quote_ident",
                "\"".repeat((MAX_ROW_BYTES - 2) / 2),
                MAX_ROW_BYTES,
            ),
            (
                "quote_ident",
                "\u{00e9}".repeat((MAX_ROW_BYTES - 2) / 2),
                MAX_ROW_BYTES,
            ),
            (
                "quote_literal",
                "'".repeat((MAX_ROW_BYTES - 2) / 2),
                MAX_ROW_BYTES,
            ),
            (
                "quote_literal",
                "\\".repeat((MAX_ROW_BYTES - 3) / 2),
                MAX_ROW_BYTES - 1,
            ),
        ] {
            let value = Value::Text(input);
            let original = value.clone();
            let mut budget = MAX_RESULT_BYTES - bytes;
            let result = eval(name, std::slice::from_ref(&value), &mut budget)
                .unwrap()
                .unwrap();
            assert!(matches!(result,Value::Text(s) if s.len()==bytes));
            assert_eq!(budget, MAX_RESULT_BYTES);
            assert_eq!(value, original);
            let mut budget = MAX_RESULT_BYTES - bytes + 1;
            let before = budget;
            assert_eq!(
                eval(name, std::slice::from_ref(&value), &mut budget).unwrap(),
                Err(SqlError::Limit)
            );
            assert_eq!(budget, before);
        }
        for (name, input) in [
            ("quote_ident", "a".repeat(MAX_ROW_BYTES + 1)),
            ("quote_ident", "\"".repeat(MAX_ROW_BYTES / 2)),
            ("quote_ident", "\u{00e9}".repeat(MAX_ROW_BYTES / 2)),
            ("quote_literal", "'".repeat(MAX_ROW_BYTES / 2)),
            ("quote_literal", "\\".repeat((MAX_ROW_BYTES - 3) / 2 + 1)),
        ] {
            let mut budget = 19;
            assert_eq!(
                eval(name, &[Value::Text(input)], &mut budget).unwrap(),
                Err(SqlError::Limit)
            );
            assert_eq!(budget, 19);
        }
        let mut budget = MAX_RESULT_BYTES - 3;
        let before = budget;
        assert_eq!(
            eval("quote_nullable", &[Value::Null], &mut budget).unwrap(),
            Err(SqlError::Limit)
        );
        assert_eq!(budget, before);
    }
}
