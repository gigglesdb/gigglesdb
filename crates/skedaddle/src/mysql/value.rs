//! MySQL opaque v1 canonical bytes (no session timezone or charset conversion).
//!
//! Identity is `mysql.v1:<lowercase COLUMN_TYPE>` with `;cs=<charset>;co=<collation>`
//! for character columns. Collation names are lowercase; the descriptor also
//! retains the numeric wire collation for the exact Text inverse.
//! Null: None (distinct from an empty non-NULL byte string).
//! Signed/Unsigned: decimal ASCII, without a plus sign or leading zeroes.
//! Float/Double: Rust's shortest round-trip decimal ASCII; finite values only.
//! Decimal: the server's decimal text, preserved byte for byte.
//! DateTime: `YYYY-MM-DD hh:mm:ss[.fraction]`, including DATE at midnight;
//! fraction has exactly the column's fsp (0..6) digits; zero dates are retained.
//! Timestamp: the same calendar form in UTC, converted from the decoder's epoch
//! seconds text. The inverse restores epoch seconds with exactly fsp digits.
//! Time: `[-]hh:mm:ss[.fraction]`, hours = days*24+hours (at least two digits),
//! including negative zero; fraction has exactly fsp digits.
//! Year: four decimal digits. Bit/Binary: unmodified raw bytes.
//! Text: unmodified bytes in the column charset, with matching wire collation.
//! Enum: its label (ordinal zero is empty). Set: selected labels comma-joined
//! in definition order. Label/ordinal/bitmap disagreement is rejected. Definitions
//! are decoded with MySQL quote/backslash escaping; UTF-8, ASCII, binary and
//! latin1 definition labels are supported, others fail explicitly.
//! Json: compact serde_json serialization. Inverses reject a different type id,
//! noncanonical bytes, missing enum/set labels, mismatched variants, and values
//! with precision that the declared fsp would lose.
use super::{
    binlog::MysqlValue as V,
    client::{MysqlError, Result},
    schema::ColumnDescriptor,
};
use crate::event::OpaqueValue;
const BAD: MysqlError = MysqlError::Value("value does not fit column or is not canonical");
fn identity(c: &ColumnDescriptor) -> Result<String> {
    let mut id = format!("mysql.v1:{}", c.column_type.to_lowercase());
    match (&c.character_set, &c.collation) {
        (Some(cs), Some(co)) => id.push_str(&format!(
            ";cs={};co={}",
            cs.to_lowercase(),
            co.name.to_lowercase()
        )),
        (None, None) => {}
        _ => return Err(BAD),
    }
    Ok(id)
}
fn ty(c: &ColumnDescriptor) -> String {
    c.data_type.to_ascii_lowercase()
}
fn fsp(c: &ColumnDescriptor) -> Result<usize> {
    let s = c
        .column_type
        .split_once('(')
        .map(|(_, n)| n.trim_end_matches(')'))
        .unwrap_or("0");
    let n: usize = s.parse().map_err(|_| BAD)?;
    if n > 6 {
        return Err(BAD);
    }
    Ok(n)
}
fn fraction(c: &ColumnDescriptor, micros: u32) -> Result<String> {
    let f = fsp(c)?;
    if micros >= 1_000_000 || !micros.is_multiple_of(10u32.pow(6 - f as u32)) {
        return Err(BAD);
    }
    Ok(if f == 0 {
        String::new()
    } else {
        format!(".{:0width$}", micros / 10u32.pow(6 - f as u32), width = f)
    })
}
fn integer(c: &ColumnDescriptor, unsigned: bool) -> bool {
    matches!(
        ty(c).as_str(),
        "tinyint" | "smallint" | "mediumint" | "int" | "integer" | "bigint"
    ) && c.column_type.to_ascii_lowercase().contains("unsigned") == unsigned
}
fn decimal(s: &str) -> bool {
    let s = s.strip_prefix('-').unwrap_or(s);
    !s.is_empty()
        && s.bytes().filter(|b| *b == b'.').count() <= 1
        && s.bytes().any(|b| b.is_ascii_digit())
        && s.bytes().all(|b| b.is_ascii_digit() || b == b'.')
}
fn labels(c: &ColumnDescriptor) -> Result<Vec<Vec<u8>>> {
    let s = c
        .column_type
        .split_once('(')
        .and_then(|(_, s)| s.strip_suffix(')'))
        .ok_or(BAD)?;
    let mut chars = s.chars().peekable();
    let mut labels = Vec::new();
    while chars.next() == Some('\'') {
        let mut label = String::new();
        loop {
            match chars.next().ok_or(BAD)? {
                '\'' if chars.peek() == Some(&'\'') => {
                    chars.next();
                    label.push('\'');
                }
                '\'' => break,
                '\\' => label.push(match chars.next().ok_or(BAD)? {
                    '0' => '\0',
                    'n' => '\n',
                    'r' => '\r',
                    't' => '\t',
                    'b' => '\u{8}',
                    'Z' => '\u{1a}',
                    ch => ch,
                }),
                ch => label.push(ch),
            }
        }
        labels.push(match c.character_set.as_deref() {
            Some("utf8mb4" | "utf8mb3" | "utf8" | "binary") => label.into_bytes(),
            Some("ascii") if label.is_ascii() => label.into_bytes(),
            Some("latin1") => label
                .chars()
                .map(|ch| u8::try_from(ch as u32).map_err(|_| BAD))
                .collect::<Result<_>>()?,
            _ => return Err(BAD),
        });
        match chars.next() {
            None => return Ok(labels),
            Some(',') => {}
            _ => return Err(BAD),
        }
    }
    Err(BAD)
}
pub fn to_opaque(c: &ColumnDescriptor, value: &V) -> Result<OpaqueValue> {
    let t = ty(c);
    let text = match value {
        V::Null if c.nullable => {
            return Ok(OpaqueValue {
                type_id: identity(c)?,
                bytes: None,
            });
        }
        V::Signed(n) if integer(c, false) => n.to_string(),
        V::Unsigned(n) if integer(c, true) => n.to_string(),
        V::Float(n) if t == "float" && n.is_finite() => n.to_string(),
        V::Double(n) if matches!(t.as_str(), "double" | "real") && n.is_finite() => n.to_string(),
        V::Decimal(s) if matches!(t.as_str(), "decimal" | "numeric") && decimal(s) => s.clone(),
        V::DateTime(y, m, d, h, min, s, us) if matches!(t.as_str(), "date" | "datetime") => {
            if *y > 9999 || *m > 12 || *d > 31 || *h > 23 || *min > 59 || *s > 59 {
                return Err(BAD);
            }
            if t == "date" && (*h != 0 || *min != 0 || *s != 0 || *us != 0) {
                return Err(BAD);
            }
            format!(
                "{y:04}-{m:02}-{d:02} {h:02}:{min:02}:{s:02}{}",
                fraction(c, *us)?
            )
        }
        V::Timestamp(s) if t == "timestamp" => {
            let (seconds, us) = split_fraction(s)?;
            let seconds: u32 = seconds.parse().map_err(|_| BAD)?;
            if *s != format!("{seconds}{}", fraction(c, us)?) {
                return Err(BAD);
            }
            let (y, m, d) = civil(i64::from(seconds / 86400));
            format!(
                "{y:04}-{m:02}-{d:02} {:02}:{:02}:{:02}{}",
                seconds / 3600 % 24,
                seconds / 60 % 60,
                seconds % 60,
                fraction(c, us)?
            )
        }
        V::Time {
            negative,
            days,
            hours,
            minutes,
            seconds,
            micros,
        } if t == "time" => {
            let h = u64::from(*days) * 24 + u64::from(*hours);
            if h > 838 || *hours > 23 || *minutes > 59 || *seconds > 59 {
                return Err(BAD);
            }
            format!(
                "{}{:02}:{minutes:02}:{seconds:02}{}",
                if *negative { "-" } else { "" },
                h,
                fraction(c, *micros)?
            )
        }
        V::Year(s) if t == "year" && s.len() == 4 && s.bytes().all(|b| b.is_ascii_digit()) => {
            s.clone()
        }
        V::Bit(b) if t == "bit" => return opaque(c, b.clone()),
        V::Binary(b)
            if matches!(
                t.as_str(),
                "binary" | "varbinary" | "tinyblob" | "blob" | "mediumblob" | "longblob"
            ) =>
        {
            return opaque(c, b.clone());
        }
        V::Text { bytes, collation }
            if matches!(
                t.as_str(),
                "char" | "varchar" | "tinytext" | "text" | "mediumtext" | "longtext"
            ) && c.collation.as_ref().is_some_and(|co| co.id == *collation) =>
        {
            return opaque(c, bytes.clone());
        }
        V::Enum {
            ordinal,
            label: Some(label),
        } if t == "enum" => {
            let labels = labels(c)?;
            let expected = if *ordinal == 0 {
                &[][..]
            } else {
                labels.get((*ordinal - 1) as usize).ok_or(BAD)?.as_slice()
            };
            if label != expected {
                return Err(BAD);
            }
            // Empty/duplicate labels cannot carry distinct ordinals in this
            // label-only representation. Fail instead of losing identity.
            if *ordinal != 0
                && (label.is_empty()
                    || labels.iter().position(|l| l == label).map(|i| i as u64 + 1)
                        != Some(*ordinal))
            {
                return Err(BAD);
            }
            return opaque(c, label.clone());
        }
        V::Set {
            bitmap,
            labels: Some(selected),
        } if t == "set" => {
            let all = labels(c)?;
            if all.len() > 64 || (all.len() < 64 && bitmap >> all.len() != 0) {
                return Err(BAD);
            }
            let expected: Vec<_> = all
                .into_iter()
                .enumerate()
                .filter(|(i, _)| bitmap & (1u64 << i) != 0)
                .map(|(_, s)| s)
                .collect();
            if &expected != selected {
                return Err(BAD);
            }
            if selected.iter().any(|s| s.is_empty() || s.contains(&b',')) {
                return Err(BAD);
            }
            return opaque(c, selected.join(&b','));
        }
        V::Json(j) if t == "json" => serde_json::to_string(j).map_err(|_| BAD)?,
        _ => return Err(BAD),
    };
    opaque(c, text.into_bytes())
}
fn opaque(c: &ColumnDescriptor, bytes: Vec<u8>) -> Result<OpaqueValue> {
    Ok(OpaqueValue {
        type_id: identity(c)?,
        bytes: Some(bytes),
    })
}
fn split_fraction(s: &str) -> Result<(&str, u32)> {
    let (whole, f) = s.split_once('.').unwrap_or((s, ""));
    if f.len() > 6 || !f.bytes().all(|b| b.is_ascii_digit()) {
        return Err(BAD);
    }
    let us = if f.is_empty() {
        0
    } else {
        f.parse::<u32>().map_err(|_| BAD)? * 10u32.pow(6 - f.len() as u32)
    };
    Ok((whole, us))
}
pub fn from_opaque(c: &ColumnDescriptor, value: &OpaqueValue) -> Result<V> {
    if value.type_id != identity(c)? {
        return Err(BAD);
    }
    let Some(b) = &value.bytes else {
        return if c.nullable { Ok(V::Null) } else { Err(BAD) };
    };
    let text = || std::str::from_utf8(b).map_err(|_| BAD);
    let t = ty(c);
    let v = match t.as_str() {
        _ if integer(c, false) => V::Signed(text()?.parse().map_err(|_| BAD)?),
        _ if integer(c, true) => V::Unsigned(text()?.parse().map_err(|_| BAD)?),
        "float" => V::Float(text()?.parse().map_err(|_| BAD)?),
        "double" | "real" => V::Double(text()?.parse().map_err(|_| BAD)?),
        "decimal" | "numeric" => V::Decimal(text()?.into()),
        "year" => V::Year(text()?.into()),
        "date" | "datetime" | "timestamp" => {
            let s = text()?;
            if !s.is_ascii()
                || s.len() < 19
                || s.as_bytes()[4] != b'-'
                || s.as_bytes()[7] != b'-'
                || s.as_bytes()[10] != b' '
            {
                return Err(BAD);
            }
            let y = s[0..4].parse::<u16>().map_err(|_| BAD)?;
            let m = s[5..7].parse::<u8>().map_err(|_| BAD)?;
            let d = s[8..10].parse::<u8>().map_err(|_| BAD)?;
            let (h, min, sec, us) = clock(&s[11..])?;
            if t == "timestamp" {
                if y < 1970 || m == 0 || d == 0 || h > 23 {
                    return Err(BAD);
                }
                let seconds = days(y, m, d) * 86400
                    + i64::from(h) * 3600
                    + i64::from(min) * 60
                    + i64::from(sec);
                let seconds = u32::try_from(seconds).map_err(|_| BAD)?;
                V::Timestamp(format!("{seconds}{}", fraction(c, us)?))
            } else {
                V::DateTime(y, m, d, u8::try_from(h).map_err(|_| BAD)?, min, sec, us)
            }
        }
        "time" => {
            let s = text()?;
            let negative = s.starts_with('-');
            let (h, minutes, seconds, micros) = clock(s.strip_prefix('-').unwrap_or(s))?;
            V::Time {
                negative,
                days: h / 24,
                hours: (h % 24) as u8,
                minutes,
                seconds,
                micros,
            }
        }
        "bit" => V::Bit(b.clone()),
        "binary" | "varbinary" | "tinyblob" | "blob" | "mediumblob" | "longblob" => {
            V::Binary(b.clone())
        }
        "char" | "varchar" | "tinytext" | "text" | "mediumtext" | "longtext" => V::Text {
            bytes: b.clone(),
            collation: c.collation.as_ref().ok_or(BAD)?.id,
        },
        "json" => V::Json(serde_json::from_slice(b).map_err(|_| BAD)?),
        "enum" => {
            let ordinal = if b.is_empty() {
                0
            } else {
                labels(c)?.iter().position(|l| l == b).ok_or(BAD)? as u64 + 1
            };
            V::Enum {
                ordinal,
                label: Some(b.clone()),
            }
        }
        "set" => {
            let selected: Vec<Vec<u8>> = if b.is_empty() {
                Vec::new()
            } else {
                b.split(|ch| *ch == b',').map(<[u8]>::to_vec).collect()
            };
            let all = labels(c)?;
            let mut bitmap = 0;
            for label in &selected {
                let i = all.iter().position(|l| l == label).ok_or(BAD)?;
                if i >= 64 {
                    return Err(BAD);
                }
                bitmap |= 1u64 << i;
            }
            V::Set {
                bitmap,
                labels: Some(selected),
            }
        }
        _ => return Err(BAD),
    };
    if to_opaque(c, &v)? != *value {
        return Err(BAD);
    }
    Ok(v)
}
fn clock(s: &str) -> Result<(u32, u8, u8, u32)> {
    let (s, us) = split_fraction(s)?;
    let p: Vec<_> = s.split(':').collect();
    if p.len() != 3 {
        return Err(BAD);
    }
    Ok((
        p[0].parse().map_err(|_| BAD)?,
        p[1].parse().map_err(|_| BAD)?,
        p[2].parse().map_err(|_| BAD)?,
        us,
    ))
}
// Proleptic Gregorian civil/epoch conversion; epoch day zero is 1970-01-01.
fn civil(z: i64) -> (i64, i64, i64) {
    let z = z + 719468;
    let era = z.div_euclid(146097);
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = mp + if mp < 10 { 3 } else { -9 };
    (y + i64::from(m <= 2), m, d)
}
fn days(y: u16, m: u8, d: u8) -> i64 {
    let y = i64::from(y) - i64::from(m <= 2);
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = i64::from(m) + if m > 2 { -3 } else { 9 };
    era * 146097 + yoe * 365 + yoe / 4 - yoe / 100 + (153 * mp + 2) / 5 + i64::from(d) - 1 - 719468
}
