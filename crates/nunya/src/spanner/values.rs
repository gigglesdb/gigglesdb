//! Strict Spanner wire value conversions.
use crate::{
    error::{NyCode, Refusal},
    value::{ColumnType, SqlValue},
};
use googleapis_tonic_google_spanner_v1::google::spanner::v1::{Type, TypeCode};
use prost_types::{value::Kind, Value};
fn bad() -> Refusal {
    Refusal::new(
        NyCode::NotAnalysable,
        "unsupported or malformed Spanner value",
    )
}
const DAY: i64 = 86_400_000_000;
// Gregorian civil-date algorithms use astronomical years and Euclidean eras.
fn days(y: i64, m: i64, d: i64) -> i64 {
    let y = y - i64::from(m <= 2);
    let era = y.div_euclid(400);
    let yo = y - era * 400;
    let mp = m + if m > 2 { -3 } else { 9 };
    era * 146097 + yo * 365 + yo / 4 - yo / 100 + (153 * mp + 2) / 5 + d - 1 - 730425
}
fn civil(z: i64) -> (i64, i64, i64) {
    let z = z + 730425;
    let era = z.div_euclid(146097);
    let doe = z - era * 146097;
    let yo = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yo + era * 400;
    let doy = doe - (365 * yo + yo / 4 - yo / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = mp + if mp < 10 { 3 } else { -9 };
    (y + i64::from(m <= 2), m, d)
}

fn digits(s: &str) -> Result<i64, Refusal> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return Err(bad());
    }
    s.parse().map_err(|_| bad())
}
fn parse_date(s: &str) -> Result<i32, Refusal> {
    if !s.is_ascii() || s.len() != 10 || &s[4..5] != "-" || &s[7..8] != "-" {
        return Err(bad());
    }
    let (y, m, d) = (digits(&s[..4])?, digits(&s[5..7])?, digits(&s[8..])?);
    if !(1..=9999).contains(&y) || !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return Err(bad());
    }
    let n = days(y, m, d);
    if civil(n) != (y, m, d) {
        return Err(bad());
    }
    Ok(n as i32)
}
fn format_date(n: i64) -> Result<String, Refusal> {
    let (y, m, d) = civil(n);
    if !(1..=9999).contains(&y) {
        return Err(bad());
    }
    Ok(format!("{y:04}-{m:02}-{d:02}"))
}
fn parse_timestamp(s: &str) -> Result<i64, Refusal> {
    if !s.is_ascii()
        || s.len() < 20
        || !matches!(&s[10..11], "T" | "t")
        || &s[13..14] != ":"
        || &s[16..17] != ":"
    {
        return Err(bad());
    }
    let d = parse_date(&s[..10])?;
    let (h, m, sec) = (
        digits(&s[11..13])?,
        digits(&s[14..16])?,
        digits(&s[17..19])?,
    );
    if h > 23 || m > 59 || sec > 59 {
        return Err(bad());
    }
    let rest = &s[19..];
    let pos = rest.find(['Z', 'z', '+', '-']).ok_or_else(bad)?;
    let frac = &rest[..pos];
    let us = if frac.is_empty() {
        0
    } else {
        let f = frac.strip_prefix('.').ok_or_else(bad)?;
        if f.is_empty() || f.len() > 9 {
            return Err(bad());
        }
        digits(f)?;
        if f.len() > 6 && !f.as_bytes()[6..].iter().all(|b| *b == b'0') {
            return Err(bad());
        }
        let f = &f[..f.len().min(6)];
        digits(f)? * 10_i64.pow(6 - f.len() as u32)
    };
    let zone = &rest[pos..];
    let offset = if matches!(zone, "Z" | "z") {
        0
    } else {
        if zone.len() != 6 || !matches!(zone.as_bytes()[0], b'+' | b'-') || &zone[3..4] != ":" {
            return Err(bad());
        }
        let (oh, om) = (digits(&zone[1..3])?, digits(&zone[4..])?);
        if oh > 23 || om > 59 {
            return Err(bad());
        }
        (oh * 60 + om) * 60_000_000 * if zone.starts_with('-') { -1 } else { 1 }
    };
    let n = i64::from(d) * DAY + (h * 3600 + m * 60 + sec) * 1_000_000 + us - offset;
    format_date(n.div_euclid(DAY))?;
    Ok(n)
}
fn format_timestamp(n: i64) -> Result<String, Refusal> {
    let d = format_date(n.div_euclid(DAY))?;
    let t = n.rem_euclid(DAY);
    let sec = t / 1_000_000;
    let us = t % 1_000_000;
    let mut s = format!("{d}T{:02}:{:02}:{:02}", sec / 3600, sec / 60 % 60, sec % 60);
    if us != 0 {
        s.push('.');
        s.push_str(format!("{us:06}").trim_end_matches('0'));
    }
    s.push('Z');
    Ok(s)
}
/// Map a Spanner schema type to its SQL counterpart.
pub fn column_type(t: &Type) -> ColumnType {
    match TypeCode::try_from(t.code) {
        Ok(TypeCode::Int64) => ColumnType::Int8,
        Ok(TypeCode::String) => ColumnType::Text,
        Ok(TypeCode::Date) => ColumnType::Date,
        Ok(TypeCode::Timestamp) => ColumnType::Timestamptz,
        Ok(TypeCode::Bool) => ColumnType::Bool,
        Ok(TypeCode::Bytes) => ColumnType::Bytea,
        Ok(TypeCode::Numeric) => ColumnType::Numeric,
        Ok(TypeCode::Json) => ColumnType::Json,
        Ok(TypeCode::Array) => ColumnType::Array(Box::new(
            t.array_element_type
                .as_deref()
                .map(column_type)
                .unwrap_or(ColumnType::Other("UNKNOWN".into())),
        )),
        Ok(code) => ColumnType::Other(code.as_str_name().into()),
        Err(_) => ColumnType::Other("UNKNOWN".into()),
    }
}
/// Decode a supported Spanner value without exposing its contents in errors.
pub fn from_proto(ty: &ColumnType, v: &Value) -> Result<SqlValue, Refusal> {
    Ok(match (ty, v.kind.as_ref()) {
        (_, Some(Kind::NullValue(0))) => SqlValue::Null,
        (ColumnType::Text, Some(Kind::StringValue(s))) => SqlValue::Text(s.clone()),
        (ColumnType::Int8, Some(Kind::StringValue(s))) => {
            let unsigned = s.strip_prefix(['+', '-']).unwrap_or(s);
            if unsigned.is_empty() || !unsigned.bytes().all(|b| b.is_ascii_digit()) {
                return Err(bad());
            }
            SqlValue::Int8(s.parse().map_err(|_| bad())?)
        }
        (ColumnType::Date, Some(Kind::StringValue(s))) => SqlValue::Date(parse_date(s)?),
        (ColumnType::Timestamptz, Some(Kind::StringValue(s))) => {
            SqlValue::Timestamptz(parse_timestamp(s)?)
        }
        (ColumnType::Bool, Some(Kind::BoolValue(b))) => SqlValue::Bool(*b),
        _ => return Err(bad()),
    })
}
/// Encode a supported SQL value with exact declared type validation.
pub fn to_proto(ty: &ColumnType, v: &SqlValue) -> Result<Value, Refusal> {
    Ok(Value {
        kind: Some(match (ty, v) {
            (_, SqlValue::Null) => Kind::NullValue(0),
            (ColumnType::Text, SqlValue::Text(s)) => Kind::StringValue(s.clone()),
            (ColumnType::Int8, SqlValue::Int8(n)) => Kind::StringValue(n.to_string()),
            (ColumnType::Date, SqlValue::Date(n)) => Kind::StringValue(format_date(i64::from(*n))?),
            (ColumnType::Timestamptz, SqlValue::Timestamptz(n)) => {
                Kind::StringValue(format_timestamp(*n)?)
            }
            (ColumnType::Bool, SqlValue::Bool(b)) => Kind::BoolValue(*b),
            _ => return Err(bad()),
        }),
    })
}
#[cfg(test)]
mod tests {
    use super::*;
    fn string(s: &str) -> Value {
        Value {
            kind: Some(Kind::StringValue(s.into())),
        }
    }
    #[test]
    fn roundtrips() {
        for (ty, v) in [
            (ColumnType::Text, SqlValue::Text("hello".into())),
            (ColumnType::Int8, SqlValue::Int8(i64::MIN)),
            (ColumnType::Int8, SqlValue::Int8(i64::MAX)),
            (ColumnType::Date, SqlValue::Date(0)),
            (ColumnType::Timestamptz, SqlValue::Timestamptz(-1)),
            (ColumnType::Bool, SqlValue::Bool(true)),
            (ColumnType::Text, SqlValue::Null),
        ] {
            assert_eq!(from_proto(&ty, &to_proto(&ty, &v).unwrap()).unwrap(), v);
        }
    }
    #[test]
    fn timestamps() {
        assert_eq!(
            from_proto(
                &ColumnType::Timestamptz,
                &string("2000-01-01T01:00:00.123456000+01:00")
            )
            .unwrap(),
            SqlValue::Timestamptz(123456)
        );
        for s in [
            "2000-01-01T00:00:00.123456001Z",
            "0001-01-01T00:00:00+01:00",
            "9999-12-31T23:59:59-01:00",
            "2000-01-01T00:00:00+24:00",
            "1900-02-29T00:00:00Z",
        ] {
            assert_eq!(
                from_proto(&ColumnType::Timestamptz, &string(s))
                    .unwrap_err()
                    .code,
                NyCode::NotAnalysable
            );
        }
    }
    #[test]
    fn types() {
        for code in [
            TypeCode::Unspecified,
            TypeCode::Bool,
            TypeCode::Int64,
            TypeCode::Float64,
            TypeCode::Float32,
            TypeCode::Timestamp,
            TypeCode::Date,
            TypeCode::String,
            TypeCode::Bytes,
            TypeCode::Array,
            TypeCode::Struct,
            TypeCode::Numeric,
            TypeCode::Json,
            TypeCode::Proto,
            TypeCode::Enum,
        ] {
            let t = Type {
                code: code as i32,
                ..Default::default()
            };
            let mapped = column_type(&t);
            let expected = match code {
                TypeCode::Bool => ColumnType::Bool,
                TypeCode::Int64 => ColumnType::Int8,
                TypeCode::Timestamp => ColumnType::Timestamptz,
                TypeCode::Date => ColumnType::Date,
                TypeCode::String => ColumnType::Text,
                TypeCode::Bytes => ColumnType::Bytea,
                TypeCode::Numeric => ColumnType::Numeric,
                TypeCode::Json => ColumnType::Json,
                TypeCode::Array => ColumnType::Array(Box::new(ColumnType::Other("UNKNOWN".into()))),
                _ => ColumnType::Other(code.as_str_name().into()),
            };
            assert_eq!(mapped, expected);
        }
    }
}
