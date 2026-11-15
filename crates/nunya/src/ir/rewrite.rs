//! Safe literal rendering and byte-span SQL replacement.
use super::Span;
use crate::{
    error::{NyCode, Refusal},
    value::SqlValue,
};
fn invalid() -> Refusal {
    Refusal::new(
        NyCode::NotAnalysable,
        "invalid SQL literal or replacement span",
    )
}
/// Quote text as a standard-conforming SQL string, rejecting NUL bytes.
pub fn sql_string_literal(s: &str) -> Result<String, Refusal> {
    if s.contains('\0') {
        return Err(invalid());
    }
    Ok(format!("'{}'", s.replace('\'', "''")))
}
/// Replace validated non-overlapping byte spans, regardless of input order.
pub fn splice(sql: &str, replacements: &[(Span, String)]) -> Result<String, Refusal> {
    let mut ordered: Vec<_> = replacements.iter().collect();
    ordered.sort_by_key(|(span, _)| (span.start, span.end));
    let mut cursor = 0;
    let mut result = String::new();
    for (span, text) in ordered {
        if span.start < cursor
            || span.start > span.end
            || span.end > sql.len()
            || !sql.is_char_boundary(span.start)
            || !sql.is_char_boundary(span.end)
        {
            return Err(invalid());
        }
        result.push_str(&sql[cursor..span.start]);
        result.push_str(text);
        cursor = span.end;
    }
    result.push_str(&sql[cursor..]);
    Ok(result)
}
// Gregorian civil date using 400-year eras; intermediates fit i64 for every input.
fn date(days: i64) -> (String, bool) {
    let z = days + 730_425; // days from 0000-03-01 to 2000-01-01
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let mut year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    year += i64::from(month <= 2);
    let bc = year <= 0;
    let display_year = if bc { 1 - year } else { year };
    (format!("{display_year:04}-{month:02}-{day:02}"), bc)
}
fn timestamp(micros: i64, timezone: bool) -> String {
    const DAY: i64 = 86_400_000_000;
    let (date, bc) = date(micros.div_euclid(DAY));
    let remainder = micros.rem_euclid(DAY);
    let hours = remainder / 3_600_000_000;
    let minutes = remainder / 60_000_000 % 60;
    let seconds = remainder / 1_000_000 % 60;
    let fraction = remainder % 1_000_000;
    let zone = if timezone { "+00" } else { "" };
    let era = if bc { " BC" } else { "" };
    format!("'{date} {hours:02}:{minutes:02}:{seconds:02}.{fraction:06}{zone}{era}'")
}
/// Render a typed decrypted value as SQL literal text.
pub fn literal_for(value: &SqlValue) -> Result<String, Refusal> {
    Ok(match value {
        SqlValue::Text(text) => return sql_string_literal(text),
        SqlValue::Int2(value) => value.to_string(),
        SqlValue::Int4(value) => value.to_string(),
        SqlValue::Int8(value) => value.to_string(),
        SqlValue::Uuid(bytes) => {
            let hex: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
            format!(
                "'{}-{}-{}-{}-{}'",
                &hex[..8],
                &hex[8..12],
                &hex[12..16],
                &hex[16..20],
                &hex[20..]
            )
        }
        SqlValue::Date(days) => {
            let (date, bc) = date(i64::from(*days));
            format!("'{date}{}'", if bc { " BC" } else { "" })
        }
        SqlValue::Timestamp(value) => timestamp(*value, false),
        SqlValue::Timestamptz(value) => timestamp(*value, true),
        SqlValue::Bool(true) => "true".into(),
        SqlValue::Bool(false) => "false".into(),
        SqlValue::Null => "NULL".into(),
    })
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn quote_doubling_and_nul() {
        assert_eq!(sql_string_literal("O'Brien\\").unwrap(), "'O''Brien\\'");
        assert_eq!(sql_string_literal("").unwrap(), "''");
        assert_eq!(
            sql_string_literal("a\0b").unwrap_err().code,
            NyCode::NotAnalysable
        );
    }
    #[test]
    fn splice_out_of_order_adjacent_and_empty() {
        assert_eq!(
            splice(
                "abc def",
                &[
                    (Span { start: 4, end: 7 }, "Y".into()),
                    (Span { start: 0, end: 3 }, "X".into())
                ]
            )
            .unwrap(),
            "X Y"
        );
        assert_eq!(
            splice(
                "ab",
                &[
                    (Span { start: 1, end: 2 }, "Y".into()),
                    (Span { start: 0, end: 1 }, "X".into())
                ]
            )
            .unwrap(),
            "XY"
        );
        assert_eq!(
            splice("ab", &[(Span { start: 2, end: 2 }, "!".into())]).unwrap(),
            "ab!"
        );
        assert_eq!(splice("abc", &[]).unwrap(), "abc");
    }
    #[test]
    fn invalid_spans_refused() {
        for spans in [
            vec![(0, 2), (1, 3)],
            vec![(0, 2), (0, 2)],
            vec![(2, 1)],
            vec![(0, 4)],
            vec![(usize::MAX, usize::MAX)],
        ] {
            let replacements: Vec<_> = spans
                .into_iter()
                .map(|(start, end)| (Span { start, end }, "x".into()))
                .collect();
            assert_eq!(
                splice("abc", &replacements).unwrap_err().code,
                NyCode::NotAnalysable
            );
        }
        for span in [Span { start: 1, end: 2 }, Span { start: 0, end: 1 }] {
            assert_eq!(
                splice("�", &[(span, "x".into())]).unwrap_err().code,
                NyCode::NotAnalysable
            );
        }
    }
    #[test]
    fn scalar_literals() {
        for (value, expected) in [
            (SqlValue::Text("a'b".into()), "'a''b'"),
            (SqlValue::Int2(-12), "-12"),
            (SqlValue::Int4(i32::MIN), "-2147483648"),
            (SqlValue::Int8(i64::MIN), "-9223372036854775808"),
            (SqlValue::Bool(true), "true"),
            (SqlValue::Bool(false), "false"),
            (SqlValue::Null, "NULL"),
            (
                SqlValue::Uuid([0xab; 16]),
                "'abababab-abab-abab-abab-abababababab'",
            ),
        ] {
            assert_eq!(literal_for(&value).unwrap(), expected);
        }
        assert!(literal_for(&SqlValue::Text("\0".into())).is_err());
    }
    #[test]
    fn dates_and_gregorian_leap_days() {
        for (days, expected) in [
            (0, "'2000-01-01'"),
            (-1, "'1999-12-31'"),
            (8825, "'2024-02-29'"),
            (60, "'2000-03-01'"),
            (-36524, "'1900-01-01'"),
            (-730119, "'0001-01-01'"),
            (-730120, "'0001-12-31 BC'"),
        ] {
            assert_eq!(literal_for(&SqlValue::Date(days)).unwrap(), expected);
        }
    }
    #[test]
    fn timestamps_microseconds_negatives_and_zone() {
        for (micros, expected) in [
            (0, "'2000-01-01 00:00:00.000000'"),
            (1, "'2000-01-01 00:00:00.000001'"),
            (3_723_456_789, "'2000-01-01 01:02:03.456789'"),
            (-1, "'1999-12-31 23:59:59.999999'"),
            (-86_400_000_001, "'1999-12-30 23:59:59.999999'"),
        ] {
            assert_eq!(literal_for(&SqlValue::Timestamp(micros)).unwrap(), expected);
        }
        assert_eq!(
            literal_for(&SqlValue::Timestamptz(-1)).unwrap(),
            "'1999-12-31 23:59:59.999999+00'"
        );
        for value in [
            SqlValue::Date(i32::MIN),
            SqlValue::Date(i32::MAX),
            SqlValue::Timestamp(i64::MIN),
            SqlValue::Timestamp(i64::MAX),
            SqlValue::Timestamptz(i64::MIN),
            SqlValue::Timestamptz(i64::MAX),
        ] {
            assert!(literal_for(&value).is_ok());
        }
    }
}
