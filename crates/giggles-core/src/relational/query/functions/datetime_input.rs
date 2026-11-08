//! Session-aware input for unknown datetime literals.
use super::*;
use giggles_pg_types::temporal::{ParserSettings, SessionSettings};

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

pub(super) fn timetz(text: &str, offset_east: i32) -> R<TimeTz> {
    TimeTz::parse(
        text,
        &ParserSettings {
            offset_east,
            ..ParserSettings::default()
        },
    )
    .map_err(|e| temporal::parse_error(e, false))
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
                date_order: Default::default(),
                zone: clock
                    .zone()
                    .unwrap_or(giggles_pg_types::temporal::SessionZone::Fixed(0)),
            },
        )
        .map(Value::TimestampTz)
        .map_err(|e| temporal::parse_error(e, false));
    }
    if *target == DataType::TimeTz {
        let parsed = timetz(&text, 0)?;
        if parsed == timetz(&text, 3600)? {
            return Ok(Value::TimeTz(parsed));
        }
        return assignment::convert(
            Value::Time(parsed.time()),
            &DataType::Time,
            target,
            Some(clock),
        );
    }
    if *target == DataType::Date {
        return Date::parse(&text, &ParserSettings::default())
            .map(Value::Date)
            .map_err(|error| {
                // The date parser reserves short year fields before checking
                // their syntax. Bare alphabetic junk is not a short year.
                let input = text.trim_ascii();
                if matches!(error, giggles_pg_types::Error::Unimplemented)
                    && (1..4).contains(&input.len())
                    && input.bytes().all(|byte| byte.is_ascii_alphabetic())
                {
                    SqlError::InvalidDatetimeFormat
                } else {
                    temporal::parse_error(error, false)
                }
            });
    }
    cast_value(Value::Text(text), target, &mut 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn malformed_short_date_words_are_not_unsupported_years() {
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
            for text in ["01-01-01", "1/2/3"] {
                assert_eq!(
                    coerce_clocked(Value::Text(text.into()), &DataType::Date, context),
                    Err(SqlError::Unsupported)
                );
            }
        }
    }
}
