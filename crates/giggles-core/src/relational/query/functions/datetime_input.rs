//! Session-aware input for unknown datetime literals.
use super::*;
use giggles_pg_types::temporal::{DateOrder, ParserSettings, SessionSettings};

pub(super) fn target(ty: &DataType) -> bool {
    matches!(
        ty,
        DataType::Date
            | DataType::Timestamp
            | DataType::TimestampTz
            | DataType::Time
            | DataType::TimeTz
    )
}

pub(super) fn clock_word(text: &str) -> bool {
    matches!(
        text.trim_ascii().to_ascii_lowercase().as_str(),
        "now" | "today" | "tomorrow" | "yesterday"
    )
}

pub(super) fn timetz_needs_session(text: &str, date_order: DateOrder) -> bool {
    // An abbreviation may be specific to the execution zone.
    if text.bytes().any(|b| b.is_ascii_alphabetic()) {
        return true;
    }
    let parse = |offset_east| {
        TimeTz::parse(
            text,
            &ParserSettings {
                date_order,
                offset_east,
            },
        )
    };
    matches!((parse(0), parse(3600)), (Ok(a), Ok(b)) if a != b)
}

pub(super) fn parse(
    text: String,
    target: &DataType,
    clock: &crate::relational::clock::ExecutionClock,
) -> R<Value> {
    const DAY: i64 = 86_400_000_000;
    let word = text.trim_ascii().to_ascii_lowercase();
    if word == "epoch" {
        if matches!(target, DataType::Time | DataType::TimeTz) {
            return Err(SqlError::InvalidDatetimeFormat);
        }
        return cast_value(
            Value::Text(if *target == DataType::TimestampTz {
                "1970-01-01 00:00:00+00".into()
            } else if *target == DataType::Timestamp {
                "1970-01-01 00:00:00".into()
            } else {
                "1970-01-01".into()
            }),
            target,
            &mut 0,
        );
    }
    if word == "allballs" {
        let midnight = Time::from_microseconds(0).map_err(pg_error)?;
        return match target {
            DataType::Time => Ok(Value::Time(midnight)),
            DataType::TimeTz => TimeTz::new(midnight, 0)
                .map(Value::TimeTz)
                .map_err(pg_error),
            _ => Err(SqlError::InvalidDatetimeFormat),
        };
    }
    if clock_word(&text) {
        if word != "now" && matches!(target, DataType::Time | DataType::TimeTz) {
            return Err(SqlError::InvalidDatetimeFormat);
        }
        let (transaction, zone) = clock.comparison_context()?;
        let instant = TimestampTz::from_microseconds(transaction).map_err(pg_error)?;
        if word == "now" && *target == DataType::TimestampTz {
            return Ok(Value::TimestampTz(instant));
        }
        if word == "now" && *target == DataType::TimeTz {
            return assignment::convert(
                Value::TimestampTz(instant),
                &DataType::TimestampTz,
                target,
                Some(clock),
            );
        }
        let local = zone.to_local(instant).map_err(pg_error)?;
        let day = local.microseconds().div_euclid(DAY)
            + match word.as_str() {
                "tomorrow" => 1,
                "yesterday" => -1,
                _ => 0,
            };
        if *target == DataType::Date {
            return Date::from_days(
                i32::try_from(day).map_err(|_| SqlError::DatetimeFieldOverflow)?,
            )
            .map(Value::Date)
            .map_err(pg_error);
        }
        if *target == DataType::Time {
            return Time::from_microseconds(local.microseconds().rem_euclid(DAY))
                .map(Value::Time)
                .map_err(pg_error);
        }
        let timestamp = if word == "now" {
            local
        } else {
            Timestamp::from_microseconds(
                day.checked_mul(DAY)
                    .ok_or(SqlError::DatetimeFieldOverflow)?,
            )
            .map_err(pg_error)?
        };
        return if *target == DataType::TimestampTz {
            zone.to_instant(timestamp)
                .map(Value::TimestampTz)
                .map_err(pg_error)
        } else {
            Ok(Value::Timestamp(timestamp))
        };
    }
    if *target == DataType::TimestampTz {
        return TimestampTz::parse_in_session(
            &text,
            &SessionSettings {
                date_order: clock.date_style().order,
                zone: clock
                    .zone()
                    .unwrap_or(giggles_pg_types::temporal::SessionZone::Fixed(0)),
            },
        )
        .map(Value::TimestampTz)
        .map_err(|e| temporal::parse_error(e, false));
    }
    if *target == DataType::TimeTz {
        let order = clock.date_style().order;
        // An explicit numeric offset needs no zone or clock (and records no zone observation).
        if !timetz_needs_session(&text, order) {
            return TimeTz::parse(&text, &ParserSettings { date_order: order, offset_east: 0 })
                .map(Value::TimeTz)
                .map_err(|e| temporal::parse_error(e, false));
        }
        let (transaction_us, zone) = clock.comparison_context()?;
        return TimeTz::parse_in_session(
            &text,
            &SessionSettings {
                date_order: clock.date_style().order,
                zone,
            },
            transaction_us,
        )
        .map(Value::TimeTz)
        .map_err(|e| temporal::parse_error(e, false));
    }
    if *target == DataType::Date {
        return Date::parse(&text, &ParserSettings { date_order: clock.date_style().order, offset_east: 0 })
            .map(Value::Date)
            .map_err(|e| temporal::parse_error(e, false));
    }
    cast_with_date_order(Value::Text(text), target, &mut 0, clock.date_style().order)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_dates_expand_years_and_words_return_syntax_errors() {
        let clock =
            crate::relational::clock::ExecutionClock::new(&crate::relational::StatementClock {
                unavailable: None,
                transaction_us: 0,
                statement_us: 0,
                zone: Some(giggles_pg_types::temporal::SessionZone::Fixed(0)),
                wall: None,
            });
        for context in [None, Some(&clock)] {
            for text in ["bad", "x", " Ab "] {
                assert_eq!(
                    coerce_clocked(Value::Text(text.into()), &DataType::Date, context),
                    Err(SqlError::InvalidDatetimeFormat)
                );
            }
            assert_eq!(
                coerce_clocked(Value::Text("01-01-01".into()), &DataType::Date, context),
                Ok(Value::Date(Date::from_ymd(2001, 1, 1).unwrap()))
            );
        }
        // Exact PostgreSQL 18.6 ISO, MDY output: 01-01-01|2001-01-01; 1/2/3|2003-01-02.
        let mdy = ParserSettings {
            date_order: giggles_pg_types::temporal::DateOrder::Mdy,
            ..ParserSettings::default()
        };
        assert_eq!(Date::parse("1/2/3", &mdy), Date::from_ymd(2003, 1, 2));
    }

    #[test]
    fn timetz_with_numeric_offset_needs_no_session_zone() {
        // No zone and no clock: an explicit offset still converts (PostgreSQL 18.6:
        // '03:04:05+02'::timetz -> 03:04:05+02); zone-dependent input still needs the zone.
        let clock =
            crate::relational::clock::ExecutionClock::new(&crate::relational::StatementClock {
                unavailable: None,
                transaction_us: 0,
                statement_us: 0,
                zone: None,
                wall: None,
            });
        let expected = TimeTz::parse("03:04:05+02", &ParserSettings::default()).unwrap();
        assert_eq!(
            coerce_clocked(Value::Text("03:04:05+02".into()), &DataType::TimeTz, Some(&clock)),
            Ok(Value::TimeTz(expected))
        );
        assert_eq!(
            coerce_clocked(Value::Text("03:04:05".into()), &DataType::TimeTz, Some(&clock)),
            Err(SqlError::Unsupported)
        );
    }
}
