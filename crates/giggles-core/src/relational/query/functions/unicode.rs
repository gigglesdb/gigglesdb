//! Unicode character operations for the default C.UTF-8 collation.
use super::*;
use unicode_normalization::UnicodeNormalization;

const _: () = assert!(matches!(unicode_normalization::UNICODE_VERSION, (18, 0, 0)));

fn contains(ranges: &[(u32, u32)], c: char) -> bool {
    ranges
        .get(ranges.partition_point(|&(_, end)| end < c as u32))
        .is_some_and(|&(start, _)| start <= c as u32)
}

pub(super) fn simple_case(c: char, upper: bool) -> char {
    let ranges = if upper {
        unicode_data::UPPER
    } else {
        unicode_data::LOWER
    };
    let code = c as u32;
    let i = ranges.partition_point(|&(_, end, ..)| end < code);
    if let Some(&(start, _, step, delta)) = ranges.get(i)
        && code >= start
        && (code - start).is_multiple_of(step)
    {
        char::from_u32(code.wrapping_add_signed(delta)).expect("Unicode simple mapping")
    } else {
        c
    }
}

pub(super) fn casing(name: &str, input: &str, budget: &mut usize) -> R<Value> {
    let chars = || {
        let mut in_word = false;
        input.chars().map(move |c| {
            let upper = name == "upper" || name == "initcap" && !in_word;
            if name == "initcap" {
                in_word = if c.is_ascii() {
                    c.is_ascii_alphanumeric()
                } else {
                    contains(unicode_data::WORD, c)
                };
            }
            if c.is_ascii() {
                if upper {
                    c.to_ascii_uppercase()
                } else {
                    c.to_ascii_lowercase()
                }
            } else {
                simple_case(c, upper)
            }
        })
    };
    let size = if input.is_ascii() {
        input.len()
    } else {
        chars().map(char::len_utf8).sum()
    };
    reserve(budget, size)?;
    let mut output = String::with_capacity(size);
    output.extend(chars());
    Ok(Value::Text(output))
}

#[derive(Clone, Copy)]
enum Form {
    Nfc,
    Nfd,
    Nfkc,
    Nfkd,
}

fn form(word: &str) -> Option<Form> {
    Some(match word {
        "nfc" => Form::Nfc,
        "nfd" => Form::Nfd,
        "nfkc" => Form::Nfkc,
        "nfkd" => Form::Nfkd,
        _ => return None,
    })
}

pub(in crate::relational::query) fn take_form(p: &mut Parser) -> Option<Scalar> {
    let Some(Token::Word(word)) = p.tokens.get(p.at) else {
        return None;
    };
    form(word)?;
    p.at += 1;
    Some(Scalar::Literal(Value::Text(word.clone())))
}

pub(super) fn invalid_form(p: &Parser) -> SqlError {
    if let Some(token) = p.tokens.get(p.at) {
        let spelling = match token {
            Token::Word(word) => word.to_ascii_uppercase(),
            Token::String(text) => format!("'{text}'"),
            _ => return SqlError::Syntax,
        };
        user_types::view_replace_error(format!("syntax error at or near \"{spelling}\""));
    }
    SqlError::Syntax
}

fn normalized(input: &str, form: Form, mut emit: impl FnMut(char)) {
    let post_16 = |c| contains(unicode_data::POST_16, c);
    // Newly assigned characters are inert starters in Unicode 16. Normalize
    // the runs between them using the upstream normalization stability guarantee.
    for chunk in input.split_inclusive(post_16) {
        let tail = chunk.chars().next_back().filter(|&c| post_16(c));
        let run = &chunk[..chunk.len() - tail.map_or(0, char::len_utf8)];
        match form {
            Form::Nfc => run.nfc().for_each(&mut emit),
            Form::Nfd => run.nfd().for_each(&mut emit),
            Form::Nfkc => run.nfkc().for_each(&mut emit),
            Form::Nfkd => run.nfkd().for_each(&mut emit),
        }
        if let Some(c) = tail {
            emit(c);
        }
    }
}

pub(super) fn signature(name: &str, count: usize) -> Option<R<(Vec<Option<DataType>>, DataType)>> {
    let ty = match name {
        "normalize" | "unistr" => DataType::Text,
        "is_normalized" => DataType::Boolean,
        _ => return None,
    };
    Some(if count == 1 || count == 2 && name != "unistr" {
        Ok((vec![Some(DataType::Text); count], ty))
    } else {
        Err(SqlError::UndefinedFunction)
    })
}

pub(super) fn eval(name: &str, values: &[Value], budget: &mut usize) -> Option<R<Value>> {
    if !matches!(name, "normalize" | "is_normalized" | "unistr") {
        return None;
    }
    Some((|| {
        let Value::Text(input) = &values[0] else {
            return Err(SqlError::Type);
        };
        if name == "unistr" {
            let mut size = 0;
            decode(input, |c| size += c.len_utf8())?;
            reserve(budget, size)?;
            let mut output = String::with_capacity(size);
            decode(input, |c| output.push(c))?;
            return Ok(Value::Text(output));
        }
        let form = match values.get(1) {
            None => Form::Nfc,
            Some(Value::Text(word)) => {
                form(&word.to_ascii_lowercase()).ok_or_else(|| {
                    user_types::view_replace_error(format!("invalid normalization form: {word}"));
                    SqlError::InvalidParameterValue
                })?
            }
            _ => return Err(SqlError::Type),
        };
        if name == "is_normalized" {
            let mut chars = input.chars();
            let mut equal = true;
            normalized(input, form, |c| equal &= chars.next() == Some(c));
            return Ok(Value::Bool(equal && chars.next().is_none()));
        }
        let mut size = 0;
        normalized(input, form, |c| size += c.len_utf8());
        reserve(budget, size)?;
        let mut output = String::with_capacity(size);
        normalized(input, form, |c| output.push(c));
        Ok(Value::Text(output))
    })())
}

fn invalid_escape() -> SqlError {
    user_types::view_replace_error("invalid Unicode escape".into());
    user_types::sql_error_hint(r"Unicode escapes must be \XXXX, \+XXXXXX, \uXXXX, or \UXXXXXXXX.");
    SqlError::Syntax
}
fn invalid_surrogate() -> SqlError {
    user_types::view_replace_error("invalid Unicode surrogate pair".into());
    SqlError::Syntax
}

fn decode(mut input: &str, mut emit: impl FnMut(char)) -> R<()> {
    let mut high: Option<u32> = None;
    while let Some(c) = input.chars().next() {
        input = &input[c.len_utf8()..];
        let code = if c == '\\' {
            if let Some(rest) = input.strip_prefix('\\') {
                input = rest;
                u32::from('\\')
            } else {
                let (digits, width) = match input.as_bytes().first() {
                    Some(b'+') => (&input[1..], 6),
                    Some(b'u') => (&input[1..], 4),
                    Some(b'U') => (&input[1..], 8),
                    _ => (input, 4),
                };
                let hex = digits
                    .get(..width)
                    .filter(|s| s.bytes().all(|b| b.is_ascii_hexdigit()))
                    .ok_or_else(invalid_escape)?;
                input = &digits[width..];
                u32::from_str_radix(hex, 16).map_err(|_| invalid_escape())?
            }
        } else {
            u32::from(c)
        };
        if code == 0 || code > 0x10ffff {
            user_types::view_replace_error(format!("invalid Unicode code point: {code:04X}"));
            return Err(SqlError::InvalidParameterValue);
        }
        let code = if let Some(first) = high.take() {
            if !(0xdc00..=0xdfff).contains(&code) {
                return Err(invalid_surrogate());
            }
            0x10000 + ((first - 0xd800) << 10) + code - 0xdc00
        } else if (0xd800..=0xdbff).contains(&code) {
            high = Some(code);
            continue;
        } else if (0xdc00..=0xdfff).contains(&code) {
            return Err(invalid_surrogate());
        } else {
            code
        };
        emit(char::from_u32(code).ok_or_else(invalid_surrogate)?);
    }
    if high.is_some() {
        return Err(invalid_surrogate());
    }
    Ok(())
}
