//! Bytea overloads from the pinned PostgreSQL 18 catalog.
use super::*;
use sha2::{Digest, Sha224, Sha256, Sha384, Sha512};
type Signature = (Vec<Option<DataType>>, DataType);
pub(super) fn signature(n: &str, count: usize, first: Option<&DataType>) -> Option<R<Signature>> {
    use DataType::*;
    let binary = first == Some(&Bytes);
    let (types, result) = match n {
        "length" if binary => (vec![Bytes], Integer),
        "reverse" if binary => (vec![Bytes], Bytes),
        "btrim" | "ltrim" | "rtrim" if binary => (vec![Bytes, Bytes], Bytes),
        "substring" | "substr" if binary && (2..=3).contains(&count) => {
            let mut t = vec![Bytes, Integer];
            if count == 3 {
                t.push(Integer);
            }
            (t, Bytes)
        }
        "convert_to" => (vec![Text, Text], Bytes),
        "convert_from" => (vec![Bytes, Text], Text),
        "encode" => (vec![Bytes, Text], Text),
        "decode" => (vec![Text, Text], Bytes),
        "get_byte" => (vec![Bytes, Integer], Integer),
        "get_bit" => (vec![Bytes, BigInt], Integer),
        "set_byte" => (vec![Bytes, Integer, Integer], Bytes),
        "set_bit" => (vec![Bytes, BigInt, Integer], Bytes),
        "sha224" | "sha256" | "sha384" | "sha512" => (vec![Bytes], Bytes),
        _ => return None,
    };
    Some(if types.len() != count {
        Err(SqlError::UndefinedFunction)
    } else {
        Ok((types.into_iter().map(Some).collect(), result))
    })
}
pub(super) fn eval(n: &str, v: &[Value], budget: &mut usize) -> Option<R<Value>> {
    let first = v.first().map(|v| {
        if matches!(v, Value::Bytes(_)) {
            DataType::Bytes
        } else {
            DataType::Text
        }
    });
    if let Err(e) = signature(n, v.len(), first.as_ref())? {
        return Some(Err(e));
    }
    Some(evaluate(n, v, budget))
}
fn evaluate(n: &str, v: &[Value], budget: &mut usize) -> R<Value> {
    if v.contains(&Value::Null) {
        return Ok(Value::Null);
    }
    if n == "encode" || n == "decode" {
        return codec(n, v, budget);
    }
    if matches!(n, "convert_to" | "convert_from") {
        return convert(n, v, budget);
    }
    let Value::Bytes(b) = &v[0] else {
        return Err(SqlError::Type);
    };
    let num = |i: usize| match v[i] {
        Value::Int(n) => Ok(n),
        _ => Err(SqlError::Type),
    };
    let bytes = |s: &[u8], budget: &mut usize| {
        reserve(budget, s.len())?;
        Ok(Value::Bytes(s.to_vec()))
    };
    match n {
        "length" => {
            reserve(budget, 8)?;
            Ok(Value::Int(
                i32::try_from(b.len()).map_err(|_| SqlError::Overflow)? as i64,
            ))
        }
        "reverse" => {
            reserve(budget, b.len())?;
            Ok(Value::Bytes(b.iter().rev().copied().collect()))
        }
        "btrim" | "ltrim" | "rtrim" => {
            let Value::Bytes(set) = &v[1] else {
                return Err(SqlError::Type);
            };
            // Byte membership is bounded independently of duplicate trim bytes.
            let mut members = [false; 256];
            for byte in set {
                members[*byte as usize] = true;
            }
            let mut lo = 0;
            let mut hi = b.len();
            if n != "rtrim" {
                while lo < hi && members[b[lo] as usize] {
                    lo += 1;
                }
            }
            if n != "ltrim" {
                while hi > lo && members[b[hi - 1] as usize] {
                    hi -= 1;
                }
            }
            bytes(&b[lo..hi], budget)
        }
        "substring" | "substr" => {
            let start = num(1)?;
            let end = if v.len() == 3 {
                let len = num(2)?;
                if len < 0 {
                    return Err(SqlError::SubstringError);
                }
                start + len
            } else {
                i64::MAX
            };
            let lo = ((start.max(1) - 1) as usize).min(b.len());
            let hi = ((end.max(1) - 1) as usize).min(b.len()).max(lo);
            bytes(&b[lo..hi], budget)
        }
        "get_byte" | "get_bit" | "set_byte" | "set_bit" => {
            let index = usize::try_from(num(1)?).map_err(|_| SqlError::Type)?;
            let bit = n.ends_with("bit");
            let byte = if bit { index / 8 } else { index };
            let old = *b.get(byte).ok_or(SqlError::Type)?;
            if n.starts_with("get") {
                reserve(budget, 8)?;
                return Ok(Value::Int(
                    if bit { (old >> (index % 8)) & 1 } else { old } as i64
                ));
            }
            let value = num(2)?;
            if bit && !(0..=1).contains(&value) {
                return Err(SqlError::Type);
            }
            reserve(budget, b.len())?;
            let mut out = b.clone();
            out[byte] = if bit {
                (old & !(1 << (index % 8))) | ((value as u8) << (index % 8))
            } else {
                value as u8
            };
            Ok(Value::Bytes(out))
        }
        "sha224" | "sha256" | "sha384" | "sha512" => {
            reserve(
                budget,
                match n {
                    "sha224" => 28,
                    "sha256" => 32,
                    "sha384" => 48,
                    _ => 64,
                },
            )?;
            Ok(Value::Bytes(match n {
                "sha224" => Sha224::digest(b).to_vec(),
                "sha256" => Sha256::digest(b).to_vec(),
                "sha384" => Sha384::digest(b).to_vec(),
                _ => Sha512::digest(b).to_vec(),
            }))
        }
        _ => Err(SqlError::Type),
    }
}
fn invalid_parameter(message: String) -> SqlError {
    user_types::view_replace_error(message);
    SqlError::InvalidParameterValue
}

fn convert(name: &str, args: &[Value], budget: &mut usize) -> R<Value> {
    let Value::Text(encoding) = &args[1] else {
        return Err(SqlError::Type);
    };
    // The PostgreSQL encoding argument is a name, with a 63-byte payload.
    let encoding = &encoding[..encoding.floor_char_boundary(63)];
    let from = name == "convert_from";
    let encoding = CopyEncoding::lookup(encoding).ok_or_else(|| {
        let role = if from { "source" } else { "destination" };
        invalid_parameter(format!("invalid {role} encoding name \"{encoding}\""))
    })?;
    if !matches!(encoding.name(), "UTF8" | "SQL_ASCII") {
        let (source, destination) = if from {
            (encoding.name(), "UTF8")
        } else {
            ("UTF8", encoding.name())
        };
        user_types::view_replace_error(format!(
            "encoding conversion from \"{source}\" to \"{destination}\" is not supported"
        ));
        return Err(SqlError::Unsupported);
    }
    match (from, &args[0]) {
        (false, Value::Text(text)) => {
            checked_utf8(text.as_bytes())?;
            reserve(budget, text.len())?;
            Ok(Value::Bytes(text.as_bytes().to_vec()))
        }
        (true, Value::Bytes(bytes)) => {
            // SQL_ASCII checks for NUL before the resulting UTF8 text is validated.
            if encoding.name() == "SQL_ASCII" && bytes.contains(&0) {
                user_types::view_replace_error(
                    "invalid byte sequence for encoding \"SQL_ASCII\": 0x00".into(),
                );
                return Err(SqlError::CharacterNotInRepertoire);
            }
            let text = checked_utf8(bytes)?;
            reserve(budget, text.len())?;
            Ok(Value::Text(text.into()))
        }
        _ => Err(SqlError::Type),
    }
}

fn checked_utf8(bytes: &[u8]) -> R<&str> {
    let decoded = std::str::from_utf8(bytes);
    let valid_end = decoded
        .as_ref()
        .map_or_else(|error| error.valid_up_to(), |_| bytes.len());
    let invalid_start = bytes[..valid_end]
        .iter()
        .position(|byte| *byte == 0)
        .or_else(|| decoded.as_ref().err().map(|error| error.valid_up_to()));
    if let Some(start) = invalid_start {
        let width = match bytes[start] {
            0xc0..=0xdf => 2,
            0xe0..=0xef => 3,
            0xf0..=0xf7 => 4,
            _ => 1,
        };
        let invalid = bytes[start..][..width.min(bytes.len() - start)]
            .iter()
            .map(|byte| format!("0x{byte:02x}"))
            .collect::<Vec<_>>()
            .join(" ");
        user_types::view_replace_error(format!(
            "invalid byte sequence for encoding \"UTF8\": {invalid}"
        ));
    }
    match (decoded, invalid_start) {
        (Ok(text), None) => Ok(text),
        _ => Err(SqlError::CharacterNotInRepertoire),
    }
}

const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
fn codec(n: &str, v: &[Value], budget: &mut usize) -> R<Value> {
    let Value::Text(format) = &v[1] else {
        return Err(SqlError::Type);
    };
    let format = if format.eq_ignore_ascii_case("hex") {
        0
    } else if format.eq_ignore_ascii_case("base64") {
        1
    } else if format.eq_ignore_ascii_case("escape") {
        2
    } else {
        return Err(invalid_parameter(format!(
            "unrecognized encoding: \"{format}\""
        )));
    };
    if n == "decode" {
        let Value::Text(s) = &v[0] else {
            return Err(SqlError::Type);
        };
        // Validate and count before allocating any output, including expansion.
        let mut len = 0usize;
        decode(s, format, |_| {
            len += 1;
        })?;
        reserve(budget, len)?;
        let mut out = Vec::with_capacity(len);
        decode(s, format, |b| out.push(b))?;
        return Ok(Value::Bytes(out));
    }
    let Value::Bytes(b) = &v[0] else {
        return Err(SqlError::Type);
    };
    let len = match format {
        0 => b.len().checked_mul(2),
        1 => b
            .len()
            .checked_add(2)
            .and_then(|n| (n / 3).checked_mul(4))
            .and_then(|n| n.checked_add(b.len() / 57)),
        _ => b.iter().try_fold(0usize, |n, b| {
            n.checked_add(if *b == b'\\' {
                2
            } else if *b == 0 || *b >= 128 {
                4
            } else {
                1
            })
        }),
    }
    .ok_or(SqlError::Limit)?;
    reserve(budget, len)?;
    let mut out = String::with_capacity(len);
    let hex = b"0123456789abcdef";
    match format {
        0 => {
            for b in b {
                out.push(hex[(b >> 4) as usize] as char);
                out.push(hex[(b & 15) as usize] as char);
            }
        }
        1 => {
            let mut col = 0;
            for chunk in b.chunks(3) {
                let word = ((chunk[0] as u32) << 16)
                    | ((chunk.get(1).copied().unwrap_or(0) as u32) << 8)
                    | chunk.get(2).copied().unwrap_or(0) as u32;
                for i in 0..4 {
                    out.push(if i > chunk.len() {
                        '='
                    } else {
                        B64[((word >> (18 - 6 * i)) & 63) as usize] as char
                    });
                    col += 1;
                    if col == 76 && chunk.len() == 3 {
                        out.push('\n');
                        col = 0;
                    }
                }
            }
        }
        _ => {
            for b in b {
                if *b == b'\\' {
                    out.push_str("\\\\");
                } else if *b == 0 || *b >= 128 {
                    out.push('\\');
                    out.push((b'0' + (b >> 6)) as char);
                    out.push((b'0' + ((b >> 3) & 7)) as char);
                    out.push((b'0' + (b & 7)) as char);
                } else {
                    out.push(*b as char);
                }
            }
        }
    }
    Ok(Value::Text(out))
}
fn decode(input: &str, format: u8, mut emit: impl FnMut(u8)) -> R<()> {
    let s = input.as_bytes();
    let hex = |i: usize| match s[i] {
        b @ b'0'..=b'9' => Ok(b - b'0'),
        b @ b'a'..=b'f' => Ok(b - b'a' + 10),
        b @ b'A'..=b'F' => Ok(b - b'A' + 10),
        _ => Err(invalid_parameter(format!(
            "invalid hexadecimal digit: \"{}\"",
            input[i..].chars().next().expect("hex digit")
        ))),
    };
    let ws = |b: u8| matches!(b, b' ' | b'\t' | b'\r' | b'\n');
    let mut i = 0;
    match format {
        0 => {
            while i < s.len() {
                if ws(s[i]) {
                    i += 1;
                    continue;
                }
                let a = hex(i)?;
                if i + 1 == s.len() {
                    return Err(invalid_parameter(
                        "invalid hexadecimal data: odd number of digits".into(),
                    ));
                }
                let b = hex(i + 1)?;
                i += 2;
                emit((a << 4) | b);
            }
        }
        1 => {
            let mut word = 0u32;
            let mut count = 0;
            // PG18 retains the first padding width across subsequent quartets.
            let mut width = 3;
            for (index, b) in s.iter().enumerate() {
                if ws(*b) {
                    continue;
                }
                let digit = if *b == b'=' {
                    if width == 3 {
                        width = match count {
                            2 => 1,
                            3 => 2,
                            _ => {
                                return Err(invalid_parameter(
                                    "unexpected \"=\" while decoding base64 sequence".into(),
                                ));
                            }
                        };
                    }
                    0
                } else {
                    B64.iter().position(|v| v == b).ok_or_else(|| {
                        invalid_parameter(format!(
                            "invalid symbol \"{}\" found while decoding base64 sequence",
                            input[index..].chars().next().expect("base64 digit")
                        ))
                    })? as u32
                };
                word = (word << 6) | digit;
                count += 1;
                if count == 4 {
                    for shift in (0..width).map(|i| 16 - 8 * i) {
                        emit((word >> shift) as u8);
                    }
                    word = 0;
                    count = 0;
                }
            }
            if count != 0 {
                user_types::sql_error_hint(
                    "Input data is missing padding, is truncated, or is otherwise corrupted.",
                );
                return Err(invalid_parameter("invalid base64 end sequence".into()));
            }
        }
        _ => {
            let invalid_escape = || {
                user_types::view_replace_error("invalid input syntax for type bytea".into());
                SqlError::InvalidTextRepresentation
            };
            while i < s.len() {
                let b = s[i];
                i += 1;
                if b != b'\\' {
                    emit(b);
                    continue;
                }
                if s.get(i) == Some(&b'\\') {
                    emit(b'\\');
                    i += 1;
                    continue;
                }
                let q = s.get(i..i + 3).ok_or_else(invalid_escape)?;
                if !(b'0'..=b'3').contains(&q[0])
                    || !(b'0'..=b'7').contains(&q[1])
                    || !(b'0'..=b'7').contains(&q[2])
                {
                    return Err(invalid_escape());
                }
                emit(((q[0] - b'0') << 6) | ((q[1] - b'0') << 3) | (q[2] - b'0'));
                i += 3;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exact_binary_budgets_and_codec_edges() {
        let mut budget = MAX_RESULT_BYTES - 2;
        assert_eq!(
            evaluate("reverse", &[Value::Bytes(vec![0, 255])], &mut budget),
            Ok(Value::Bytes(vec![255, 0]))
        );
        assert_eq!(budget, MAX_RESULT_BYTES);
        let before = budget;
        assert_eq!(
            evaluate(
                "decode",
                &[Value::Text("00".into()), Value::Text("hex".into())],
                &mut budget
            ),
            Err(SqlError::Limit)
        );
        assert_eq!(budget, before);
        for len in [0, 1, 2, 3, 56, 57, 58, 114, 65536] {
            let b = vec![255; len];
            for format in ["hex", "base64", "escape"] {
                let mut budget = 0;
                let encoded = codec(
                    "encode",
                    &[Value::Bytes(b.clone()), Value::Text(format.into())],
                    &mut budget,
                );
                if len > 20000 && format != "base64" {
                    assert_eq!(encoded, Err(SqlError::Limit));
                    continue;
                }
                if let Ok(Value::Text(encoded)) = encoded {
                    assert_eq!(budget, encoded.len());
                    assert_eq!(
                        codec(
                            "decode",
                            &[Value::Text(encoded), Value::Text(format.into())],
                            &mut 0
                        ),
                        Ok(Value::Bytes(b.clone()))
                    );
                }
            }
        }
        for name in ["sha224", "sha256", "sha384", "sha512"] {
            let mut budget = 0;
            let Value::Bytes(hash) = evaluate(name, &[Value::Bytes(vec![])], &mut budget).unwrap()
            else {
                panic!()
            };
            assert_eq!(budget, hash.len());
        }
    }
}

#[cfg(test)]
mod review_tests {
    use super::*;
    #[test]
    fn repeated_large_trim_sets_are_linear_and_reserve_before_clone() {
        let mut set = vec![0; 65535];
        set.push(255);
        let input = vec![255; 65536];
        for name in ["btrim", "ltrim", "rtrim"] {
            assert_eq!(
                evaluate(
                    name,
                    &[Value::Bytes(input.clone()), Value::Bytes(set.clone())],
                    &mut 0
                ),
                Ok(Value::Bytes(vec![]))
            );
            let mut budget = MAX_RESULT_BYTES;
            assert_eq!(
                evaluate(
                    name,
                    &[Value::Bytes(vec![1; 65536]), Value::Bytes(set.clone())],
                    &mut budget
                ),
                Err(SqlError::Limit)
            );
            assert_eq!(budget, MAX_RESULT_BYTES);
        }
    }
}
