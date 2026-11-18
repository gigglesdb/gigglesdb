//! Shared concrete JSON extraction path normalization and policy classification.
use super::filter::JsonMode;
use crate::{
    error::{NyCode, Refusal},
    policy::{JsonColumnFilter, JsonPathTreatment, PathSegment},
};

/// One concrete object key or array subscript.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Step {
    /// A key compared byte for byte.
    Key(String),
    /// A signed array subscript, including negative subscripts.
    Index(i64),
}

/// Required treatment of a normalized extraction.
#[derive(Clone, Debug, PartialEq)]
pub enum Extraction {
    /// An unrelated path in Show mode.
    Plain,
    /// A suppressed path or a read inside a declared leaf.
    Masked,
    /// A root leaf or a subtree with rebased rules.
    Filter(JsonColumnFilter),
}

fn refuse() -> Refusal {
    Refusal::new(NyCode::MaskedExpression, "JSON extraction violates policy")
}

fn matches(step: &Step, rule: &PathSegment) -> bool {
    match (step, rule) {
        (Step::Key(a), PathSegment::Key(b)) => a == b,
        (Step::Index(_), PathSegment::AnyElement) => true,
        (Step::Key(_), PathSegment::AnyElement) | (Step::Index(_), PathSegment::Key(_)) => false,
    }
}

/// Decode only a standard SQL string literal, with doubled single quotes.
pub fn standard_string(raw: &str) -> Result<String, Refusal> {
    let inner = raw
        .strip_prefix('\'')
        .and_then(|s| s.strip_suffix('\''))
        .ok_or_else(refuse)?;
    let mut chars = inner.chars();
    let mut value = String::new();
    while let Some(c) = chars.next() {
        if c == '\'' && chars.next() != Some('\'') {
            return Err(refuse());
        }
        if c == '\0' {
            return Err(refuse());
        }
        value.push(c);
    }
    Ok(value)
}

/// Whether a path element starts with PostgreSQL's C `isspace` characters.
pub fn pg_leading_whitespace(element: &str) -> bool {
    element.as_bytes().first().is_some_and(|b| matches!(b, b' ' | b'\t' | b'\n' | b'\x0b' | b'\x0c' | b'\r'))
}

/// Append one operator operand to the shared concrete path representation.
pub fn normalize(
    path: &mut Vec<Step>,
    op: &str,
    raw: &str,
    filter: &JsonColumnFilter,
) -> Result<(), Refusal> {
    match op {
        "->" | "->>" => {
            if raw.starts_with('\'') {
                path.push(Step::Key(standard_string(raw)?));
            } else {
                let digits = raw.strip_prefix('-').unwrap_or(raw);
                if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
                    return Err(refuse());
                }
                path.push(Step::Index(raw.parse().map_err(|_| refuse())?));
            }
        }
        "#>" | "#>>" => {
            for element in text_array(&standard_string(raw)?)? {
                // PostgreSQL tests array path elements with strtoint. Numeric
                // elements can name object keys too. Choose the policy's sole
                // container shape; consumers guard that shape. Mixed shapes refuse.
                if pg_leading_whitespace(&element) {
                    return Err(refuse());
                }
                let digits = element.strip_prefix(['+', '-']).unwrap_or(&element);
                let integer = if !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()) {
                    element.parse::<i32>().ok()
                } else {
                    None
                };
                if let Some(index) = integer {
                    let related = filter.rules.iter().filter(|r| {
                        r.path.len() > path.len()
                            && path.iter().zip(&r.path).all(|(a, b)| matches(a, b))
                    });
                    let mut key = false;
                    let mut array = false;
                    for rule in related {
                        match &rule.path[path.len()] {
                            PathSegment::Key(_) => key = true,
                            PathSegment::AnyElement => array = true,
                        }
                    }
                    if key && array { return Err(refuse()); }
                    path.push(if key { Step::Key(element) } else { Step::Index(i64::from(index)) });
                } else {
                    // strtoint rejects overflow and trailing whitespace, so
                    // those spellings can only select an object key.
                    path.push(Step::Key(element));
                }
            }
        }
        _ => return Err(refuse()),
    }
    Ok(())
}

/// Parse the one-dimensional PostgreSQL text-array input used by path operators.
pub fn text_array(text: &str) -> Result<Vec<String>, Refusal> {
    let bytes = text.as_bytes();
    let mut i = 0;
    let whitespace = |b: u8| matches!(b, b' ' | b'\t' | b'\n' | b'\r' | 11 | 12);
    while bytes.get(i).is_some_and(|b| whitespace(*b)) {
        i += 1;
    }
    if bytes.get(i) != Some(&b'{') {
        return Err(refuse());
    }
    i += 1;
    let mut result = Vec::new();
    loop {
        while bytes.get(i).is_some_and(|b| whitespace(*b)) {
            i += 1;
        }
        let quoted = bytes.get(i) == Some(&b'"');
        let mut escaped = false;
        let mut value = Vec::new();
        let mut significant = 0;
        if quoted {
            i += 1;
            loop {
                match bytes.get(i).copied() {
                    Some(b'"') => {
                        i += 1;
                        break;
                    }
                    Some(b'\\') => {
                        i += 1;
                        value.push(*bytes.get(i).ok_or_else(refuse)?);
                        i += 1;
                    }
                    Some(0) | None => return Err(refuse()),
                    Some(b) => {
                        value.push(b);
                        i += 1;
                    }
                }
            }
            while bytes.get(i).is_some_and(|b| whitespace(*b)) {
                i += 1;
            }
        } else {
            loop {
                match bytes.get(i).copied() {
                    Some(b',' | b'}') => break,
                    Some(b'\\') => {
                        escaped = true;
                        i += 1;
                        value.push(*bytes.get(i).ok_or_else(refuse)?);
                        significant = value.len();
                        i += 1;
                    }
                    Some(b'{' | b'"' | 0) | None => return Err(refuse()),
                    Some(b) => {
                        value.push(b);
                        i += 1;
                        if !whitespace(b) {
                            significant = value.len();
                        }
                    }
                }
            }
            value.truncate(significant);
            if value.is_empty() || (!escaped && value.eq_ignore_ascii_case(b"NULL")) {
                return Err(refuse());
            }
        }
        result.push(String::from_utf8(value).map_err(|_| refuse())?);
        match bytes.get(i) {
            Some(b',') => i += 1,
            Some(b'}') => {
                i += 1;
                break;
            }
            _ => return Err(refuse()),
        }
    }
    while bytes.get(i).is_some_and(|b| whitespace(*b)) {
        i += 1;
    }
    if i != bytes.len() {
        return Err(refuse());
    }
    Ok(result)
}

/// Classify a concrete path and rebase descendant rules, rejecting conflicts.
pub fn classify(path: &[Step], filter: &JsonColumnFilter) -> Result<Extraction, Refusal> {
    // Validation is repeated at this boundary because callers may construct IR
    // or policy values directly without passing through the policy file loader.
    for (i, a) in filter.rules.iter().enumerate() {
        for b in &filter.rules[i + 1..] {
            if a.column != b.column || a.path.iter().zip(&b.path).all(|(a, b)| a == b) {
                return Err(refuse());
            }
        }
    }
    let mut rebased = Vec::new();
    for rule in &filter.rules {
        for (step, segment) in path.iter().zip(&rule.path) {
            if matches!(
                (step, segment),
                (Step::Key(_), PathSegment::AnyElement) | (Step::Index(_), PathSegment::Key(_))
            ) {
                return Ok(Extraction::Masked);
            }
            if !matches(step, segment) {
                break;
            }
        }
        if !path.iter().zip(&rule.path).all(|(a, b)| matches(a, b)) {
            continue;
        }
        if rule.path.len() < path.len()
            || matches!(rule.treatment, JsonPathTreatment::Masked) && rule.path.len() == path.len()
        {
            return Ok(Extraction::Masked);
        }
        let mut rule = rule.clone();
        rule.path = rule.path[path.len()..].to_vec();
        rebased.push(rule);
    }
    if !rebased.is_empty() {
        Ok(Extraction::Filter(JsonColumnFilter {
            rules: rebased,
            mode: filter.mode,
        }))
    } else {
        Ok(match filter.mode {
            JsonMode::Show => Extraction::Plain,
            JsonMode::Tokenize => Extraction::Masked,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        policy::{JsonLeafType, JsonPathRule},
        schema::ColumnRef,
    };
    fn filter(mode: JsonMode) -> JsonColumnFilter {
        JsonColumnFilter {
            mode,
            rules: vec![JsonPathRule {
                column: ColumnRef {
                    schema: "public".into(),
                    table: "events".into(),
                    column: "doc".into(),
                },
                path: vec![PathSegment::Key("a".into()), PathSegment::Key("b".into())],
                treatment: JsonPathTreatment::Show {
                    ty: JsonLeafType::String,
                },
            }],
        }
    }
    #[test]
    fn equivalent_chains() {
        let f = filter(JsonMode::Show);
        let mut chain = vec![];
        normalize(&mut chain, "->", "'a'", &f).unwrap();
        normalize(&mut chain, "->", "'b'", &f).unwrap();
        for raw in ["'{a,b}'", "'{\"a\",b}'"] {
            let mut path = vec![];
            normalize(&mut path, "#>", raw, &f).unwrap();
            assert_eq!(path, chain);
        }
    }
    #[test]
    fn array_quoting_escaping_and_refusals() {
        assert_eq!(
            text_array(r#" { a ,"c d","",N\ULL,a\,b,"x\"y"} "#).unwrap(),
            ["a", "c d", "", "NULL", "a,b", "x\"y"]
        );
        for raw in [
            "{}", "{NULL}", "{null}", "{{a}}", "{a,}", "{,a}", "{a}x", "{\"a\"x}", "{a\"b}",
            "{a\\}",
        ] {
            assert!(text_array(raw).is_err(), "invalid array accepted");
        }
        for raw in [
            "E'a'",
            "U&'a'",
            "$$a$$",
            "'a'::text",
            "$1",
            "NULL",
            "1+2",
            "1.0",
        ] {
            assert!(normalize(&mut vec![], "->", raw, &filter(JsonMode::Show)).is_err());
        }
    }
    #[test]
    fn classification_and_numeric_ambiguity() {
        for mode in [JsonMode::Show, JsonMode::Tokenize] {
            let f = filter(mode);
            let mut p = vec![];
            normalize(&mut p, "#>", "'{a,b}'", &f).unwrap();
            let Extraction::Filter(root) = classify(&p, &f).unwrap() else {
                panic!("expected leaf filter")
            };
            assert!(root.rules[0].path.is_empty());
            p.push(Step::Key("inside".into()));
            assert_eq!(classify(&p, &f).unwrap(), Extraction::Masked);
            assert!(matches!(
                classify(&[Step::Key("a".into())], &f).unwrap(),
                Extraction::Filter(_)
            ));
            assert!(normalize(&mut vec![], "#>", "'{1}'", &f).is_ok());
            let mut unrelated = vec![];
            normalize(&mut unrelated, "#>", "'{other,1}'", &f).unwrap();
            assert_eq!(
                classify(&unrelated, &f).unwrap(),
                if mode == JsonMode::Show {
                    Extraction::Plain
                } else {
                    Extraction::Masked
                }
            );
        }
    }
    #[test]
    fn rebases_array_steps_and_refuses_conflicting_rules() {
        let mut f = filter(JsonMode::Tokenize);
        f.rules[0].path = vec![
            PathSegment::Key("a".into()),
            PathSegment::AnyElement,
            PathSegment::Key("b".into()),
        ];
        let p = [Step::Key("a".into()), Step::Index(-1)];
        let Extraction::Filter(rebased) = classify(&p, &f).unwrap() else {
            panic!("subtree filter")
        };
        assert_eq!(rebased.rules[0].path, vec![PathSegment::Key("b".into())]);
        f.rules[0].treatment = JsonPathTreatment::Masked;
        assert_eq!(
            classify(
                &[Step::Key("a".into()), Step::Index(0), Step::Key("b".into())],
                &f
            )
            .unwrap(),
            Extraction::Masked
        );
        let mut ancestor = f.rules[0].clone();
        ancestor.path.truncate(1);
        f.rules.push(ancestor);
        assert!(classify(&p, &f).is_err());
    }
    #[test]
    fn postgres_integer_spelling_and_overflow() {
        let f = filter(JsonMode::Show);
        for raw in ["'{2147483648}'", "'{\"1 \"}'"] {
            let mut path = vec![];
            normalize(&mut path, "#>", raw, &f).unwrap();
            assert!(matches!(path[0], Step::Key(_)));
        }
        for raw in ["'{+1}'", "'{-2147483648}'"] {
            let mut path = vec![];
            normalize(&mut path, "#>", raw, &f).unwrap();
            assert!(matches!(path[0], Step::Key(_)));
        }
    }
    #[test]
    fn mismatched_container_kinds_are_masked() {
        for mode in [JsonMode::Show, JsonMode::Tokenize] {
            let mut f = filter(mode);
            f.rules[0].treatment = JsonPathTreatment::Masked;
            f.rules[0].path = vec![
                PathSegment::Key("items".into()),
                PathSegment::AnyElement,
                PathSegment::Key("card".into()),
            ];
            assert_eq!(
                classify(&[Step::Key("items".into()), Step::Key("0".into())], &f).unwrap(),
                Extraction::Masked
            );
            f.rules[0].path[1] = PathSegment::Key("named".into());
            assert_eq!(
                classify(&[Step::Key("items".into()), Step::Index(0)], &f).unwrap(),
                Extraction::Masked
            );
            assert_eq!(
                classify(&[Step::Key("unrelated".into()), Step::Index(0)], &f).unwrap(),
                if mode == JsonMode::Show {
                    Extraction::Plain
                } else {
                    Extraction::Masked
                }
            );
        }
    }
}

#[cfg(test)]
mod numeric_shape_tests {
    use super::*;
    #[test]
    fn numeric_elements_choose_only_related_shape_and_refuse_mixed_shapes() {
        let column = crate::schema::ColumnRef { schema: "".into(), table: "docs".into(), column: "doc".into() };
        let rule = |path: &str| crate::policy::JsonPathRule { column: column.clone(), path: crate::policy::json_path::parse_path(path).unwrap(), treatment: JsonPathTreatment::Masked };
        for (rules, expected) in [(vec![rule("$.a[*].b")], Step::Index(0)), (vec![rule("$.a.named.b")], Step::Key("0".into())), (vec![rule("$.unrelated[*].b")], Step::Index(0))] {
            let filter = JsonColumnFilter { mode: JsonMode::Show, rules };
            let mut path = vec![];
            normalize(&mut path, "#>", "'{a,0,b}'", &filter).unwrap();
            assert_eq!(path[1], expected);
        }
        let filter = JsonColumnFilter { mode: JsonMode::Show, rules: vec![rule("$.a[*].b"), rule("$.a.named.b")] };
        assert!(normalize(&mut vec![], "#>", "'{a,0,b}'", &filter).is_err());
    }
}

#[cfg(test)]
mod r1_tests {
    use super::*;
    #[test]
    fn r1_c_whitespace_path_elements_are_refused() {
        let filter = JsonColumnFilter { mode: JsonMode::Show, rules: vec![] };
        for whitespace in [' ', '\t', '\n', '\x0b', '\x0c', '\r'] {
            for op in ["#>", "#>>"] {
                let raw = format!("'{{a,\"{whitespace}0\"}}'");
                assert_eq!(normalize(&mut vec![], op, &raw, &filter).unwrap_err().code, NyCode::MaskedExpression);
            }
        }
    }
}
