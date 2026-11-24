use std::collections::BTreeMap;

use giggles_pg_regex::api::{
    self, CaptureRow, InstrOptions, RegexError, RegexLocale, RegexpContext, ReplaceOptions,
    SubstrOptions,
};

#[test]
fn engine_free() {
    replay(false);
}

#[test]
fn engine_dependent() {
    replay(true);
}

fn replay(engine: bool) {
    let cases = Json::read(&mut include_str!("data/api_cases.json").trim());
    let mut count = 0;
    for case in cases.array() {
        if case["engine"] != Json::Bool(engine) {
            continue;
        }
        let args = case["args"].array();
        let locale = match case["locale"].text() {
            "C" => RegexLocale::C,
            "CUtf8" => RegexLocale::CUtf8,
            name => panic!("unknown locale {name:?}"),
        };
        let actual =
            execute(case["op"].text(), args, &RegexpContext::new(locale)).unwrap_or_else(|error| {
                Json::object([(
                    "error",
                    Json::object([
                        ("sqlstate", Json::from(error.sqlstate)),
                        ("message", Json::String(error.message)),
                        ("detail", Json::from(error.detail)),
                        ("hint", Json::from(error.hint)),
                    ]),
                )])
            });
        assert_eq!(
            actual,
            case["expected"],
            "{}\nSQL: {}\ninputs: {args:?}",
            case["source"].text(),
            case["sql"].text(),
        );
        count += 1;
    }
    assert!(count > 0, "fixture has no cases in this test");
    println!("passed {count} API cases with engine-dependent={engine}");
}

fn execute(op: &str, args: &[Json], ctx: &RegexpContext<'_>) -> Result<Json, RegexError> {
    let text = |i| args.get(i).and_then(Json::optional_text);
    if op == "similar_escape" {
        let value = api::similar_escape(text(0), text(1), ctx)?;
        return Ok(Json::value(Json::from(value.as_deref())));
    }
    // The SQL caller implements strictness. Strict set-returning functions
    // emit no rows for a NULL argument.
    if args.contains(&Json::Null) {
        return Ok(Json::value(match op {
            "regexp_matches" | "regexp_split_to_table" => Json::Array(Vec::new()),
            _ => Json::Null,
        }));
    }

    let a = args[0].text();
    if op == "similar_to_escape" {
        return Ok(Json::value(Json::String(api::similar_to_escape(
            a,
            text(1),
            ctx,
        )?)));
    }
    let p = args[1].text();
    let int = |i, default| args.get(i).map_or(default, Json::integer);
    let value = match op {
        "textregexeq" => Json::Bool(api::textregexeq(a, p, ctx)?),
        "textregexne" => Json::Bool(api::textregexne(a, p, ctx)?),
        "texticregexeq" => Json::Bool(api::texticregexeq(a, p, ctx)?),
        "texticregexne" => Json::Bool(api::texticregexne(a, p, ctx)?),
        "textregexsubstr" => Json::from(api::textregexsubstr(a, p, ctx)?),
        "similar_to" => Json::Bool(api::similar_to(a, p, text(2), ctx)?),
        "not_similar_to" => Json::Bool(!api::similar_to(a, p, text(2), ctx)?),
        "substring_similar" => Json::from(api::substring_similar(a, p, args[2].text(), ctx)?),
        "regexp_count" => Json::Integer(api::regexp_count(a, p, int(2, 1), text(3), ctx)?),
        "regexp_instr" => {
            let options = InstrOptions {
                start: int(2, 1),
                n: int(3, 1),
                endoption: int(4, 0),
                flags: text(5),
                subexpr: int(6, 0),
            };
            Json::Integer(api::regexp_instr(a, p, options, ctx)?)
        }
        "regexp_like" => Json::Bool(api::regexp_like(a, p, text(2), ctx)?),
        "regexp_match" => api::regexp_match(a, p, text(2), ctx)?.map_or(Json::Null, capture),
        "regexp_matches" => Json::Array(
            api::regexp_matches(a, p, text(2), ctx)?
                .into_iter()
                .map(capture)
                .collect(),
        ),
        "regexp_replace" => {
            let options = match args {
                [_, _, _] => ReplaceOptions::Default,
                [_, _, _, Json::String(flags)] => ReplaceOptions::Flags(flags),
                [_, _, _, Json::Integer(start)] => ReplaceOptions::Start(*start),
                _ => ReplaceOptions::Occurrence {
                    start: int(3, 1),
                    n: int(4, 1),
                    flags: text(5),
                },
            };
            Json::String(api::regexp_replace(a, p, args[2].text(), options, ctx)?)
        }
        "regexp_split_to_array" => Json::Array(
            api::regexp_split_to_array(a, p, text(2), ctx)?
                .into_iter()
                .map(Json::from)
                .collect(),
        ),
        "regexp_split_to_table" => Json::Array(
            api::regexp_split_to_table(a, p, text(2), ctx)?
                .into_iter()
                .map(Json::from)
                .collect(),
        ),
        "regexp_substr" => {
            let options = SubstrOptions {
                start: int(2, 1),
                n: int(3, 1),
                flags: text(4),
                subexpr: int(5, 0),
            };
            Json::from(api::regexp_substr(a, p, options, ctx)?)
        }
        _ => panic!("unknown fixture operation {op}"),
    };
    Ok(Json::value(value))
}

fn capture(row: CaptureRow<'_>) -> Json {
    Json::Array(row.into_iter().map(Json::from).collect())
}

// A reader for the fixture's JSON subset, keeping the standalone crate
// dependency-free. Numbers are int32 and Python emits Unicode as UTF-8.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Json {
    Null,
    Bool(bool),
    Integer(i32),
    String(String),
    Array(Vec<Self>),
    Object(BTreeMap<String, Self>),
}

impl Json {
    fn read(input: &mut &str) -> Self {
        match input.trim_start().as_bytes().first() {
            Some(b'n') => {
                require(input, "null");
                Self::Null
            }
            Some(b't') => {
                require(input, "true");
                Self::Bool(true)
            }
            Some(b'f') => {
                require(input, "false");
                Self::Bool(false)
            }
            Some(b'"') => Self::String(read_string(input)),
            Some(b'[') => Self::Array(read_list(input, "[", "]", Self::read)),
            Some(b'{') => Self::Object(
                read_list(input, "{", "}", |input| {
                    let name = read_string(input);
                    require(input, ":");
                    (name, Self::read(input))
                })
                .into_iter()
                .collect(),
            ),
            Some(b'-' | b'0'..=b'9') => {
                *input = input.trim_start();
                let len = input
                    .bytes()
                    .take_while(|b| *b == b'-' || b.is_ascii_digit())
                    .count();
                let (number, remainder) = input.split_at(len);
                *input = remainder;
                Self::Integer(number.parse().expect("fixture int32"))
            }
            _ => panic!("invalid JSON at {input:?}"),
        }
    }

    fn object<const N: usize>(entries: [(&str, Self); N]) -> Self {
        Self::Object(
            entries
                .into_iter()
                .map(|(key, value)| (key.to_owned(), value))
                .collect(),
        )
    }

    fn value(value: Self) -> Self {
        Self::object([("value", value)])
    }

    fn array(&self) -> &[Self] {
        match self {
            Self::Array(values) => values,
            _ => panic!("expected array, got {self:?}"),
        }
    }

    fn integer(&self) -> i32 {
        match self {
            Self::Integer(value) => *value,
            _ => panic!("expected integer, got {self:?}"),
        }
    }

    fn text(&self) -> &str {
        self.optional_text().expect("expected text")
    }

    fn optional_text(&self) -> Option<&str> {
        match self {
            Self::Null => None,
            Self::String(value) => Some(value),
            _ => panic!("expected text, got {self:?}"),
        }
    }
}

impl std::ops::Index<&str> for Json {
    type Output = Self;

    fn index(&self, field: &str) -> &Self {
        match self {
            Self::Object(fields) => &fields[field],
            _ => panic!("cannot access {field} in {self:?}"),
        }
    }
}

impl From<&str> for Json {
    fn from(value: &str) -> Self {
        Self::String(value.to_owned())
    }
}

impl From<Option<&str>> for Json {
    fn from(value: Option<&str>) -> Self {
        value.map_or(Self::Null, Self::from)
    }
}

fn consume(input: &mut &str, token: &str) -> bool {
    if let Some(remainder) = input.trim_start().strip_prefix(token) {
        *input = remainder;
        true
    } else {
        false
    }
}

fn require(input: &mut &str, token: &str) {
    assert!(consume(input, token), "expected JSON token {token}");
}

fn read_list<T>(
    input: &mut &str,
    open: &str,
    close: &str,
    read_item: impl Fn(&mut &str) -> T,
) -> Vec<T> {
    require(input, open);
    let mut values = Vec::new();
    if !consume(input, close) {
        loop {
            values.push(read_item(input));
            if consume(input, close) {
                break;
            }
            require(input, ",");
        }
    }
    values
}

fn take_char(input: &mut &str) -> char {
    let ch = input.chars().next().expect("unterminated JSON string");
    *input = &input[ch.len_utf8()..];
    ch
}

fn read_string(input: &mut &str) -> String {
    require(input, "\"");
    let mut value = String::new();
    loop {
        value.push(match take_char(input) {
            '"' => return value,
            '\\' => match take_char(input) {
                ch @ ('"' | '\\' | '/') => ch,
                'b' => '\u{8}',
                'f' => '\u{c}',
                'n' => '\n',
                'r' => '\r',
                't' => '\t',
                'u' => {
                    let (hex, remainder) = input.split_at(4);
                    *input = remainder;
                    let code = u32::from_str_radix(hex, 16).expect("invalid JSON hex escape");
                    char::from_u32(code).expect("invalid Unicode scalar")
                }
                ch => panic!("invalid JSON escape {ch:?}"),
            },
            ch => ch,
        });
    }
}

#[test]
fn named_control_characters_match_postgres() {
    let ctx = RegexpContext::new(RegexLocale::CUtf8);
    for (name, control, letter) in [
        ("backspace", "\u{8}", "b"),
        ("tab", "\t", "t"),
        ("newline", "\n", "n"),
        ("vertical-tab", "\u{b}", "v"),
        ("form-feed", "\u{c}", "f"),
        ("carriage-return", "\r", "r"),
    ] {
        for marker in ['.', '='] {
            let pattern = format!("[[{marker}{name}{marker}]]");
            assert_eq!(
                api::textregexeq(control, &pattern, &ctx),
                Ok(true),
                "{pattern}"
            );
            assert_eq!(
                api::textregexeq(letter, &pattern, &ctx),
                Ok(false),
                "{pattern}"
            );
        }
    }
}

#[test]
fn case_insensitive_ranges_use_postgres_transient_capacity() {
    let ctx = RegexpContext::new(RegexLocale::CUtf8);
    for (pattern, variants) in [
        ("[ǅ-ǅ]", ["Ǆ", "ǅ", "ǆ"]),
        ("[ǈ-ǈ]", ["Ǉ", "ǈ", "ǉ"]),
        ("[ǋ-ǋ]", ["Ǌ", "ǋ", "ǌ"]),
        ("[ǲ-ǲ]", ["Ǳ", "ǲ", "ǳ"]),
    ] {
        for source in variants {
            assert_eq!(
                api::regexp_match(source, pattern, Some("i"), &ctx),
                Ok(Some(vec![Some(source)])),
                "{pattern} against {source}",
            );
        }
    }

    for (source, pattern) in [("Aǅ", "[A-Ð][ǅ-ǅ]"), ("\tǅ", "[[:cntrl:]][ǅ-ǅ]")] {
        assert_eq!(
            api::regexp_match(source, pattern, Some("i"), &ctx),
            Ok(Some(vec![Some(source)])),
            "{pattern} against {source:?}",
        );
    }

    // The large range leaves one range slot. A class that needs two slots
    // replaces the vector and resets the space available for case variants.
    for pattern in ["[A-Ð][[:cntrl:]][ǅ-ǅ]", "[A-Ð][[:cntrl:]]a[ǅ-ǅ]"] {
        let error = api::regexp_match("ǅ", pattern, Some("i"), &ctx).unwrap_err();
        assert_eq!(error.sqlstate, "2201B", "{pattern}");
        assert_eq!(
            error.message, "invalid regular expression: regular expression is too complex",
            "{pattern}",
        );
    }
}

#[test]
fn cancellation_reaches_compilation_execution_and_global_scans() {
    use std::cell::Cell;

    let locale = RegexLocale::CUtf8;
    let plain = RegexpContext::new(locale);
    let calls = Cell::new(0);
    const CANCEL_AFTER: usize = 64;
    let cancel = || {
        calls.set(calls.get() + 1);
        calls.get() >= CANCEL_AFTER
    };
    let ctx = RegexpContext {
        locale,
        cancel: Some(&cancel),
    };
    let assert_cancelled = |error: RegexError| {
        assert_eq!(error.sqlstate, "57014");
        assert_eq!(error.message, "canceling statement due to user request");
        assert_eq!(calls.replace(0), CANCEL_AFTER);
    };

    let long_pattern = "[ab]".repeat(1000);
    assert_cancelled(api::regexp_match("a", &long_pattern, None, &ctx).unwrap_err());

    // Prime the cache with a short input, so the hook flips during execution.
    let pattern = r"(?:(a)|b)*\1";
    api::regexp_match("a", pattern, None, &plain).unwrap();
    assert_cancelled(api::regexp_match(&"ab".repeat(1000), pattern, None, &ctx).unwrap_err());

    let text = "a".repeat(800);
    api::regexp_match("a", ".", None, &plain).unwrap();
    api::regexp_split_to_array("a", ".", None, &plain).unwrap();
    assert_cancelled(api::regexp_matches(&text, ".", Some("g"), &ctx).unwrap_err());
    assert_cancelled(
        api::regexp_replace(&text, ".", "", ReplaceOptions::Flags("g"), &ctx).unwrap_err(),
    );
    assert_cancelled(api::regexp_split_to_array(&text, ".", None, &ctx).unwrap_err());
    assert_cancelled(api::regexp_split_to_table(&text, ".", None, &ctx).unwrap_err());
    assert_cancelled(api::regexp_count(&text, ".", 1, None, &ctx).unwrap_err());
    assert_cancelled(api::regexp_instr(&text, ".", InstrOptions::default(), &ctx).unwrap_err());
    assert_cancelled(api::regexp_substr(&text, ".", SubstrOptions::default(), &ctx).unwrap_err());
}
