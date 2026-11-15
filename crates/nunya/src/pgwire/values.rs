//! Data-free PostgreSQL wire value conversions.
use crate::{
    error::{NyCode, Refusal},
    value::{ColumnType, SqlValue},
};
fn bad() -> Refusal {
    Refusal::new(NyCode::NotAnalysable, "unsupported or malformed wire value")
}
const DAY: i64 = 86_400_000_000;
const DATE_MIN: i32 = -2_451_545;
const DATE_MAX: i32 = 2_145_031_948;
const TS_MIN: i64 = -211_813_488_000_000_000;
const TS_MAX: i64 = 9_223_371_331_199_999_999;
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
fn date(s: &str, bc: bool) -> Result<i32, Refusal> {
    let p: Vec<_> = s.split('-').collect();
    if p.len() != 3 || p[0].len() < 4 || p[1].len() != 2 || p[2].len() != 2 {
        return Err(bad());
    }
    let year = digits(p[0])?;
    if year == 0 || year > 6_000_000 {
        return Err(bad());
    }
    let y = if bc { 1 - year } else { year };
    let m = digits(p[1])?;
    let d = digits(p[2])?;
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return Err(bad());
    }
    let n = days(y, m, d);
    if civil(n) != (y, m, d) {
        return Err(bad());
    }
    let n = i32::try_from(n).map_err(|_| bad())?;
    if !(DATE_MIN..=DATE_MAX).contains(&n) {
        return Err(bad());
    }
    Ok(n)
}
fn timestamp(s: &str, tz: bool) -> Result<i64, Refusal> {
    let (s, bc) = s.strip_suffix(" BC").map_or((s, false), |x| (x, true));
    let (ds, rest) = s.split_once(' ').ok_or_else(bad)?;
    let (clock, offset) = if tz {
        let pos = rest.find(['+', '-']).ok_or_else(bad)?;
        (&rest[..pos], Some(&rest[pos..]))
    } else {
        (rest, None)
    };
    let parts: Vec<_> = clock.split(':').collect();
    if parts.len() != 3 || parts[0].len() != 2 || parts[1].len() != 2 {
        return Err(bad());
    }
    let h = digits(parts[0])?;
    let m = digits(parts[1])?;
    let (sec, frac) = parts[2]
        .split_once('.')
        .map_or((parts[2], None), |(a, b)| (a, Some(b)));
    if sec.len() != 2 {
        return Err(bad());
    }
    let sec = digits(sec)?;
    if h > 23 || m > 59 || sec > 59 {
        return Err(bad());
    }
    let us = match frac {
        Some(f) if f.len() <= 6 => digits(f)? * 10_i64.pow(6 - f.len() as u32),
        Some(_) => return Err(bad()),
        None => 0,
    };
    let off = if let Some(o) = offset {
        let p: Vec<_> = o[1..].split(':').collect();
        if p.is_empty() || p.len() > 2 || p[0].len() != 2 || (p.len() == 2 && p[1].len() != 2) {
            return Err(bad());
        }
        let oh = digits(p[0])?;
        let om = if p.len() == 2 { digits(p[1])? } else { 0 };
        if oh > 15 || om > 59 {
            return Err(bad());
        }
        (oh * 60 + om) * 60_000_000 * if o.starts_with('-') { -1 } else { 1 }
    } else {
        0
    };
    let v = i64::from(date(ds, bc)?)
        .checked_mul(DAY)
        .and_then(|v| v.checked_add((h * 3600 + m * 60 + sec) * 1_000_000 + us))
        .and_then(|v| v.checked_sub(off))
        .ok_or_else(bad)?;
    if !(TS_MIN..=TS_MAX).contains(&v) {
        return Err(bad());
    }
    Ok(v)
}
fn date_string(n: i64) -> (String, bool) {
    let (y, m, d) = civil(n);
    let bc = y <= 0;
    (
        format!("{:04}-{m:02}-{d:02}", if bc { 1 - y } else { y }),
        bc,
    )
}
fn timestamp_string(v: i64, tz: bool) -> Result<String, Refusal> {
    if !(TS_MIN..=TS_MAX).contains(&v) {
        return Err(bad());
    }
    let (d, bc) = date_string(v.div_euclid(DAY));
    let t = v.rem_euclid(DAY);
    let s = t / 1_000_000;
    let us = t % 1_000_000;
    let mut out = format!("{d} {:02}:{:02}:{:02}", s / 3600, s / 60 % 60, s % 60);
    if us != 0 {
        out.push_str(&format!(".{us:06}"));
    }
    if tz {
        out.push_str("+00");
    }
    if bc {
        out.push_str(" BC");
    }
    Ok(out)
}
/// Decode a non-null PostgreSQL text value.
pub fn decode_text(ty: &ColumnType, bytes: &[u8]) -> Result<SqlValue, Refusal> {
    let s = std::str::from_utf8(bytes).map_err(|_| bad())?;
    Ok(match ty {
        ColumnType::Text | ColumnType::Varchar(_) | ColumnType::Bpchar(_) => {
            SqlValue::Text(s.into())
        }
        ColumnType::Int2 | ColumnType::Int4 | ColumnType::Int8 => {
            let unsigned = s.strip_prefix(['+', '-']).unwrap_or(s);
            if unsigned.is_empty() || !unsigned.bytes().all(|b| b.is_ascii_digit()) {
                return Err(bad());
            }
            match ty {
                ColumnType::Int2 => SqlValue::Int2(s.parse().map_err(|_| bad())?),
                ColumnType::Int4 => SqlValue::Int4(s.parse().map_err(|_| bad())?),
                ColumnType::Int8 => SqlValue::Int8(s.parse().map_err(|_| bad())?),
                _ => return Err(bad()),
            }
        }
        ColumnType::Uuid => {
            if s.len() != 36 || ![8, 13, 18, 23].iter().all(|&i| s.as_bytes()[i] == b'-') {
                return Err(bad());
            }
            let mut out = [0; 16];
            let mut n = 0;
            for (i, b) in s.bytes().enumerate() {
                if [8, 13, 18, 23].contains(&i) {
                    continue;
                }
                let x = (b as char).to_digit(16).ok_or_else(bad)? as u8;
                out[n / 2] |= x << if n % 2 == 0 { 4 } else { 0 };
                n += 1;
            }
            SqlValue::Uuid(out)
        }
        ColumnType::Date => {
            let (s, bc) = s.strip_suffix(" BC").map_or((s, false), |x| (x, true));
            SqlValue::Date(date(s, bc)?)
        }
        ColumnType::Timestamp => SqlValue::Timestamp(timestamp(s, false)?),
        ColumnType::Timestamptz => SqlValue::Timestamptz(timestamp(s, true)?),
        ColumnType::Bool => SqlValue::Bool(match s {
            "t" => true,
            "f" => false,
            _ => return Err(bad()),
        }),
        _ => return Err(bad()),
    })
}
/// Encode a non-null PostgreSQL text value.
pub fn encode_text(v: &SqlValue) -> Result<Vec<u8>, Refusal> {
    Ok(match v {
        SqlValue::Text(s) => s.clone(),
        SqlValue::Int2(n) => n.to_string(),
        SqlValue::Int4(n) => n.to_string(),
        SqlValue::Int8(n) => n.to_string(),
        SqlValue::Uuid(b) => {
            let mut s = String::new();
            for (i, b) in b.iter().enumerate() {
                if [4, 6, 8, 10].contains(&i) {
                    s.push('-');
                }
                s.push_str(&format!("{b:02x}"));
            }
            s
        }
        SqlValue::Date(n) => {
            if !(DATE_MIN..=DATE_MAX).contains(n) {
                return Err(bad());
            }
            let (mut s, bc) = date_string(i64::from(*n));
            if bc {
                s.push_str(" BC");
            }
            s
        }
        SqlValue::Timestamp(n) => timestamp_string(*n, false)?,
        SqlValue::Timestamptz(n) => timestamp_string(*n, true)?,
        SqlValue::Bool(b) => if *b { "t" } else { "f" }.into(),
        SqlValue::Null => return Err(bad()),
    }
    .into_bytes())
}
/// Decode a non-null PostgreSQL binary value.
pub fn decode_binary(ty: &ColumnType, b: &[u8]) -> Result<SqlValue, Refusal> {
    let value = match ty {
        ColumnType::Text | ColumnType::Varchar(_) | ColumnType::Bpchar(_) => {
            return decode_text(ty, b);
        }
        ColumnType::Int2 => SqlValue::Int2(i16::from_be_bytes(b.try_into().map_err(|_| bad())?)),
        ColumnType::Int4 => SqlValue::Int4(i32::from_be_bytes(b.try_into().map_err(|_| bad())?)),
        ColumnType::Int8 => SqlValue::Int8(i64::from_be_bytes(b.try_into().map_err(|_| bad())?)),
        ColumnType::Uuid => SqlValue::Uuid(b.try_into().map_err(|_| bad())?),
        ColumnType::Date => SqlValue::Date(i32::from_be_bytes(b.try_into().map_err(|_| bad())?)),
        ColumnType::Timestamp => {
            SqlValue::Timestamp(i64::from_be_bytes(b.try_into().map_err(|_| bad())?))
        }
        ColumnType::Timestamptz => {
            SqlValue::Timestamptz(i64::from_be_bytes(b.try_into().map_err(|_| bad())?))
        }
        ColumnType::Bool => SqlValue::Bool(match b {
            [0] => false,
            [1] => true,
            _ => return Err(bad()),
        }),
        _ => return Err(bad()),
    };
    encode_text(&value)?;
    Ok(value)
}
/// Encode a non-null PostgreSQL binary value, checking its declared type.
pub fn encode_binary(ty: &ColumnType, v: &SqlValue) -> Result<Vec<u8>, Refusal> {
    encode_text(v)?;
    Ok(match (ty, v) {
        (ColumnType::Text | ColumnType::Varchar(_) | ColumnType::Bpchar(_), SqlValue::Text(s)) => {
            s.as_bytes().to_vec()
        }
        (ColumnType::Int2, SqlValue::Int2(n)) => n.to_be_bytes().to_vec(),
        (ColumnType::Int4, SqlValue::Int4(n)) => n.to_be_bytes().to_vec(),
        (ColumnType::Int8, SqlValue::Int8(n)) => n.to_be_bytes().to_vec(),
        (ColumnType::Uuid, SqlValue::Uuid(n)) => n.to_vec(),
        (ColumnType::Date, SqlValue::Date(n)) => n.to_be_bytes().to_vec(),
        (ColumnType::Timestamp, SqlValue::Timestamp(n))
        | (ColumnType::Timestamptz, SqlValue::Timestamptz(n)) => n.to_be_bytes().to_vec(),
        (ColumnType::Bool, SqlValue::Bool(b)) => vec![u8::from(*b)],
        _ => return Err(bad()),
    })
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn round_trips() {
        let cases = [
            (ColumnType::Text, "hello"),
            (ColumnType::Varchar(None), ""),
            (ColumnType::Bpchar(None), "x   "),
            (ColumnType::Int2, "-32768"),
            (ColumnType::Int4, "2147483647"),
            (ColumnType::Int8, "9223372036854775807"),
            (ColumnType::Uuid, "ABCDEF01-2345-6789-ABCD-EF0123456789"),
            (ColumnType::Date, "1999-12-31"),
            (ColumnType::Date, "2000-01-01"),
            (ColumnType::Date, "2000-02-29"),
            (ColumnType::Date, "0001-02-29 BC"),
            (ColumnType::Timestamp, "0001-01-01 00:00:00.123456 BC"),
            (ColumnType::Timestamp, "2000-01-01 01:02:03.123456"),
            (ColumnType::Timestamptz, "2000-01-01 01:02:03.123456+00"),
            (ColumnType::Bool, "t"),
            (ColumnType::Bool, "f"),
        ];
        for (ty, s) in cases {
            let v = decode_text(&ty, s.as_bytes()).unwrap();
            assert_eq!(decode_text(&ty, &encode_text(&v).unwrap()).unwrap(), v);
            assert_eq!(
                decode_binary(&ty, &encode_binary(&ty, &v).unwrap()).unwrap(),
                v
            );
        }
        assert_eq!(
            decode_text(&ColumnType::Date, b"2000-01-01").unwrap(),
            SqlValue::Date(0)
        );
        assert_eq!(
            decode_text(&ColumnType::Date, b"1999-12-31").unwrap(),
            SqlValue::Date(-1)
        );
    }
    #[test]
    fn integer_boundaries() {
        for (ty, limits) in [
            (
                ColumnType::Int2,
                [i16::MIN.to_string(), i16::MAX.to_string()],
            ),
            (
                ColumnType::Int4,
                [i32::MIN.to_string(), i32::MAX.to_string()],
            ),
            (
                ColumnType::Int8,
                [i64::MIN.to_string(), i64::MAX.to_string()],
            ),
        ] {
            for s in limits {
                let v = decode_text(&ty, s.as_bytes()).unwrap();
                assert_eq!(encode_text(&v).unwrap(), s.as_bytes());
            }
        }
    }
    #[test]
    fn calendar_known_values() {
        for (s, n) in [
            ("1970-01-01", -10957),
            ("2000-02-29", 59),
            ("0001-01-01 BC", -730485),
            ("5874897-12-31", DATE_MAX),
        ] {
            assert_eq!(
                decode_text(&ColumnType::Date, s.as_bytes()).unwrap(),
                SqlValue::Date(n)
            );
        }
        assert_eq!(
            decode_text(&ColumnType::Timestamp, b"2000-01-01 00:00:00.000001").unwrap(),
            SqlValue::Timestamp(1)
        );
    }
    #[test]
    fn temporal_boundaries() {
        for n in [DATE_MIN, DATE_MAX] {
            let v = SqlValue::Date(n);
            assert_eq!(
                decode_text(&ColumnType::Date, &encode_text(&v).unwrap()).unwrap(),
                v
            );
        }
        for n in [TS_MIN, TS_MAX] {
            let v = SqlValue::Timestamp(n);
            assert_eq!(
                decode_text(&ColumnType::Timestamp, &encode_text(&v).unwrap()).unwrap(),
                v
            );
        }
        assert!(encode_text(&SqlValue::Date(DATE_MIN - 1)).is_err());
        assert!(encode_text(&SqlValue::Timestamp(TS_MAX + 1)).is_err());
    }
    #[test]
    fn timezone() {
        assert_eq!(
            encode_text(
                &decode_text(&ColumnType::Timestamptz, b"2000-01-01 05:30:00+05:30").unwrap()
            )
            .unwrap(),
            b"2000-01-01 00:00:00+00"
        );
    }
    #[test]
    fn malformed() {
        for (ty, s) in [
            (ColumnType::Date, "infinity"),
            (ColumnType::Timestamp, "-infinity"),
            (ColumnType::Date, "1900-02-29"),
            (ColumnType::Date, "0000-01-01"),
            (ColumnType::Timestamp, "2000-01-01 24:00:00"),
            (ColumnType::Int4, " 1"),
            (ColumnType::Int8, "--1"),
            (ColumnType::Uuid, "secret"),
            (ColumnType::Numeric, "1"),
        ] {
            let e = decode_text(&ty, s.as_bytes()).unwrap_err();
            assert_eq!(e.code, NyCode::NotAnalysable);
            assert!(!e.reason.contains(s));
        }
        assert!(decode_binary(&ColumnType::Bool, &[2]).is_err());
        assert!(decode_binary(&ColumnType::Date, &i32::MAX.to_be_bytes()).is_err());
        assert!(decode_binary(&ColumnType::Timestamp, &i64::MIN.to_be_bytes()).is_err());
    }
}
