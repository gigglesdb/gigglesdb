use super::temporal as t;
use super::{EncodedBytes, Jsonb, MappingError, Numeric, PgType, Value, invalid};

pub(crate) fn parse(ty: &PgType, text: Option<&[u8]>, depth: usize) -> Result<Value, MappingError> {
    if depth > 32 {
        return Err(invalid("text nesting exceeds 32"));
    }
    let Some(bytes) = text else {
        structural_check(ty, &Value::Null, depth)?;
        return Ok(Value::Null);
    };
    let string =
        || std::str::from_utf8(bytes).map_err(|_| invalid("UTF8/ASCII required for scalar text"));
    let encoded = |encoding: &String| EncodedBytes {
        bytes: bytes.to_vec(),
        encoding: encoding.clone(),
    };
    let value = match ty {
        PgType::Bool => match bytes {
            b"t" | b"true" => Value::Bool(true),
            b"f" | b"false" => Value::Bool(false),
            _ => return Err(invalid("boolean")),
        },
        PgType::Int2 => Value::Int2(string()?.parse().map_err(|_| invalid("int2"))?),
        PgType::Int4 => Value::Int4(string()?.parse().map_err(|_| invalid("int4"))?),
        PgType::Int8 => Value::Int8(string()?.parse().map_err(|_| invalid("int8"))?),
        PgType::Numeric => Value::Numeric(Numeric::parse(string()?)?),
        PgType::Float4 => Value::Float4(float4(string()?)?.to_bits()),
        PgType::Float8 => Value::Float8(float8(string()?)?.to_bits()),
        PgType::Text { encoding } => Value::Text(encoded(encoding)),
        PgType::Varchar { encoding } => Value::Varchar(encoded(encoding)),
        PgType::Char { encoding } => Value::Char(encoded(encoding)),
        PgType::Bytea => Value::Bytea(parse_bytea(bytes)?),
        PgType::Uuid => Value::Uuid(
            *uuid::Uuid::parse_str(string()?)
                .map_err(|_| invalid("UUID"))?
                .as_bytes(),
        ),
        PgType::Date => Value::Date(t::parse_date(string()?)?),
        PgType::Time => Value::Time(t::parse_clock(string()?, true)?),
        PgType::Timetz => {
            let (clock, offset_east) = t::split_zone(string()?)?;
            Value::Timetz {
                micros: t::parse_clock(clock, true)?,
                offset_east,
            }
        }
        PgType::Timestamp => Value::Timestamp(t::parse_timestamp(string()?, false)?),
        PgType::Timestamptz => Value::Timestamptz(t::parse_timestamp(string()?, true)?),
        PgType::Interval => {
            let (months, days, micros) = t::parse_interval(string()?)?;
            Value::Interval {
                months,
                days,
                micros,
            }
        }
        PgType::Json => Value::Json(string()?.into()),
        PgType::Jsonb => Value::Jsonb(Jsonb::parse(string()?)?),
        PgType::Array(element) => super::array::parse(element, bytes, depth + 1)?,
        PgType::Enum {
            type_id, encoding, ..
        } => Value::Enum {
            type_id: type_id.clone(),
            label: encoded(encoding),
        },
        PgType::Domain {
            type_id,
            base,
            constraint_ref,
        } => Value::Domain {
            type_id: type_id.clone(),
            constraint_ref: constraint_ref.clone(),
            base: Box::new(parse(base, text, depth + 1)?),
        },
        PgType::Other { name, known_lossy } => {
            return Err(if *known_lossy {
                MappingError::Unsupported(name.clone())
            } else {
                MappingError::Unverified(name.clone())
            });
        }
    };
    structural_check(ty, &value, depth)?;
    Ok(value)
}

// Codecs validate representation and identity; Mapping additionally evaluates
// constraints. Decoding alone never asserts that a domain is supported.
fn structural_check(ty: &PgType, value: &Value, depth: usize) -> Result<(), MappingError> {
    match ty.classification(true) {
        super::Classification::Unverified => {
            return Err(MappingError::Unverified("unknown text type".into()));
        }
        super::Classification::Unsupported => {
            return Err(MappingError::Unsupported("known-lossy text type".into()));
        }
        super::Classification::Supported => {}
    }
    ty.check(value, None, depth).or_else(|e| {
        if matches!(e, MappingError::Unverified(_)) {
            Ok(())
        } else {
            Err(e)
        }
    })
}

pub(crate) fn format(
    value: &Value,
    ty: &PgType,
    depth: usize,
) -> Result<Option<Vec<u8>>, MappingError> {
    if depth > 32 {
        return Err(invalid("text nesting exceeds 32"));
    }
    structural_check(ty, value, depth)?;
    let out = match value {
        Value::Null => return Ok(None),
        Value::Bool(b) => if *b { "t" } else { "f" }.into(),
        Value::Int2(i) => i.to_string(),
        Value::Int4(i) => i.to_string(),
        Value::Int8(i) => i.to_string(),
        Value::Numeric(n) => n.to_string(),
        Value::Float4(bits) => {
            let v = f32::from_bits(*bits);
            let s = format_float(f64::from(v), v.to_string());
            if float4(&s)?.to_bits() != *bits {
                return Err(MappingError::LossyText(
                    "float4 NaN bits; use binary/canonical".into(),
                ));
            }
            s
        }
        Value::Float8(bits) => {
            let v = f64::from_bits(*bits);
            let s = format_float(v, v.to_string());
            if float8(&s)?.to_bits() != *bits {
                return Err(MappingError::LossyText(
                    "float8 NaN bits; use binary/canonical".into(),
                ));
            }
            s
        }
        Value::Text(e) | Value::Varchar(e) | Value::Char(e) | Value::Enum { label: e, .. } => {
            return Ok(Some(e.bytes.clone()));
        }
        Value::Bytea(b) => {
            let mut out = Vec::with_capacity(2 + b.len() * 2);
            out.extend(b"\\x");
            for byte in b {
                out.push(b"0123456789abcdef"[(byte >> 4) as usize]);
                out.push(b"0123456789abcdef"[(byte & 15) as usize]);
            }
            return Ok(Some(out));
        }
        Value::Uuid(u) => uuid::Uuid::from_bytes(*u).to_string(),
        Value::Date(d) => t::format_date(*d),
        Value::Time(v) => t::format_clock(*v),
        Value::Timetz {
            micros,
            offset_east,
        } => format!(
            "{}{}",
            t::format_clock(*micros),
            t::format_zone(*offset_east)
        ),
        Value::Timestamp(v) => t::format_timestamp(*v, false),
        Value::Timestamptz(v) => t::format_timestamp(*v, true),
        Value::Interval {
            months,
            days,
            micros,
        } => format!("{months} mons {days} days {}", t::format_clock(*micros)),
        Value::Json(s) => s.clone(),
        Value::Jsonb(j) => j.as_str().into(),
        Value::Array(a) => return super::array::format(a, depth).map(Some),
        Value::Domain { base, .. } => {
            let PgType::Domain { base: ty, .. } = ty else {
                return Err(invalid("domain type"));
            };
            return format(base, ty, depth + 1);
        }
    };
    Ok(Some(out.into_bytes()))
}

fn float4(s: &str) -> Result<f32, MappingError> {
    let v = match s {
        "NaN" => f32::NAN,
        "Infinity" => f32::INFINITY,
        "-Infinity" => f32::NEG_INFINITY,
        _ => s.parse().map_err(|_| invalid("float4"))?,
    };
    if v.is_infinite() && !matches!(s, "Infinity" | "-Infinity" | "inf" | "-inf") {
        return Err(invalid("float4 overflow"));
    }
    if v == 0.0 && nonzero_mantissa(s) {
        return Err(invalid("float4 underflow"));
    }
    Ok(v)
}
fn float8(s: &str) -> Result<f64, MappingError> {
    let v = match s {
        "NaN" => f64::NAN,
        "Infinity" => f64::INFINITY,
        "-Infinity" => f64::NEG_INFINITY,
        _ => s.parse().map_err(|_| invalid("float8"))?,
    };
    if v.is_infinite() && !matches!(s, "Infinity" | "-Infinity" | "inf" | "-inf") {
        return Err(invalid("float8 overflow"));
    }
    if v == 0.0 && nonzero_mantissa(s) {
        return Err(invalid("float8 underflow"));
    }
    Ok(v)
}
fn nonzero_mantissa(s: &str) -> bool {
    s.split(['e', 'E'])
        .next()
        .unwrap_or_default()
        .bytes()
        .any(|c| (b'1'..=b'9').contains(&c))
}
fn format_float(v: f64, finite: String) -> String {
    if v.is_nan() {
        "NaN".into()
    } else if v == f64::INFINITY {
        "Infinity".into()
    } else if v == f64::NEG_INFINITY {
        "-Infinity".into()
    } else {
        finite
    }
}

fn parse_bytea(b: &[u8]) -> Result<Vec<u8>, MappingError> {
    if let Some(hex) = b.strip_prefix(b"\\x") {
        if !hex.len().is_multiple_of(2) {
            return Err(invalid("bytea hex length"));
        }
        return hex
            .as_chunks::<2>()
            .0
            .iter()
            .map(|p| {
                let h = (p[0] as char)
                    .to_digit(16)
                    .ok_or_else(|| invalid("bytea hex"))?;
                let l = (p[1] as char)
                    .to_digit(16)
                    .ok_or_else(|| invalid("bytea hex"))?;
                Ok((h * 16 + l) as u8)
            })
            .collect();
    }
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        if b[i] != b'\\' {
            out.push(b[i]);
            i += 1;
        } else if b.get(i + 1) == Some(&b'\\') {
            out.push(b'\\');
            i += 2;
        } else {
            let p = b.get(i + 1..i + 4).ok_or_else(|| invalid("bytea escape"))?;
            if !(b'0'..=b'3').contains(&p[0]) || !p[1..].iter().all(|c| (b'0'..=b'7').contains(c)) {
                return Err(invalid("bytea octal"));
            }
            out.push((p[0] - b'0') * 64 + (p[1] - b'0') * 8 + (p[2] - b'0'));
            i += 4;
        }
    }
    Ok(out)
}
