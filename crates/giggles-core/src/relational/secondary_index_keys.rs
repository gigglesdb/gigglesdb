//! Memcomparable secondary components. Primary keys use the separate row codec.
use super::{Interval, R, Value, append_key_iter, escaped};

enum TemporalKey {
    Scalar(i64),
    ZonedTime([u8; 12]),
    Span([u8; 16]),
}

impl TemporalKey {
    fn from_value(value: &Value) -> Option<Self> {
        Some(match *value {
            Value::Date(date) => Self::Scalar(i64::from(date.days())),
            Value::Time(time) => Self::Scalar(time.microseconds()),
            Value::Timestamp(timestamp) => Self::Scalar(timestamp.microseconds()),
            Value::TimestampTz(timestamp) => Self::Scalar(timestamp.microseconds()),
            Value::TimeTz(time) => {
                // PostgreSQL compares the UTC-adjusted time, then seconds west.
                let west = -time.offset_east();
                let utc = time.time().microseconds() + i64::from(west) * 1_000_000;
                let mut bytes = [0; 12];
                bytes[..8].copy_from_slice(&(utc ^ i64::MIN).to_be_bytes());
                bytes[8..].copy_from_slice(&(west ^ i32::MIN).to_be_bytes());
                Self::ZonedTime(bytes)
            }
            Value::Interval(span) => {
                // The comparison span must fit even with maximum months, days
                // and microseconds. PostgreSQL compares every month as 30 days.
                let total = if span.same_components(Interval::NEG_INFINITY) {
                    i128::MIN
                } else if span.same_components(Interval::INFINITY) {
                    i128::MAX
                } else {
                    (i128::from(span.months()) * 30 + i128::from(span.days())) * 86_400_000_000
                        + i128::from(span.microseconds())
                };
                Self::Span((total ^ i128::MIN).to_be_bytes())
            }
            _ => return None,
        })
    }

    fn bytes(&self) -> &[u8] {
        match self {
            Self::ZonedTime(bytes) => bytes,
            Self::Span(bytes) => bytes,
            Self::Scalar(_) => unreachable!("scalar temporal key"),
        }
    }
}

pub(super) fn temporal_len(value: &Value) -> Option<usize> {
    let key = TemporalKey::from_value(value)?;
    if matches!(key, TemporalKey::Scalar(_)) {
        return Some(9);
    }
    let bytes = key.bytes();
    Some(bytes.len() + bytes.iter().filter(|&&byte| byte == 0).count() + 3)
}

pub(crate) fn component(value: &Value) -> R<Vec<u8>> {
    let mut out = Vec::new();
    append_component(&mut out, value)?;
    Ok(out)
}

pub(crate) fn append_component(out: &mut Vec<u8>, value: &Value) -> R<()> {
    let start = out.len();
    let result = match TemporalKey::from_value(value) {
        Some(TemporalKey::Scalar(n)) => append_key_iter(out, std::iter::once(&Value::Int(n))),
        Some(key) => {
            // Reuse the existing byte component framing. A descriptor fixes
            // each component's SQL type via the referenced column.
            out.push(4);
            escaped(key.bytes(), out);
            Ok(())
        }
        None => append_key_iter(out, std::iter::once(value)),
    };
    if result.is_err() {
        out.truncate(start);
    }
    result
}

/// The exclusive end of a byte prefix, including all suffixes at that prefix.
pub(super) fn successor(mut prefix: Vec<u8>) -> Option<Vec<u8>> {
    while let Some(last) = prefix.pop() {
        if last != u8::MAX {
            prefix.push(last + 1);
            return Some(prefix);
        }
    }
    None
}
