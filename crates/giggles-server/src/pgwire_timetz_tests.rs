use super::*;
use giggles_pg_types::temporal::{ParserSettings, TimeTz, TimestampTz};

fn instant(text: &str) -> i64 {
    TimestampTz::parse(text, &ParserSettings::default())
        .unwrap()
        .microseconds()
}

#[test]
fn omitted_zone_uses_transaction_date_in_session_zone() {
    for (zone, january, july) in [
        (
            SessionZone::named("America/Chicago").unwrap(),
            "01:02:00-06",
            "01:02:00-05",
        ),
        (
            SessionZone::named("Australia/Sydney").unwrap(),
            "01:02:00+11",
            "01:02:00+10",
        ),
        (
            SessionZone::named("Asia/Kolkata").unwrap(),
            "01:02:00+05:30",
            "01:02:00+05:30",
        ),
        (SessionZone::Fixed(0), "01:02:00+00", "01:02:00+00"),
    ] {
        for (date, expected) in [
            ("2024-01-15 12:00:00+00", january),
            ("2024-07-15 12:00:00+00", july),
        ] {
            let value =
                decode_in_zone(b"01:02", &DataType::TimeTz, 0, zone, instant(date)).unwrap();
            assert_eq!(
                value,
                Value::TimeTz(TimeTz::parse(expected, &ParserSettings::default()).unwrap())
            );
        }
    }
}

#[test]
fn explicit_zone_and_binary_are_preserved() {
    let zone = SessionZone::named("America/Chicago").unwrap();
    let transaction = instant("2024-07-15 12:00:00+00");
    let expected = TimeTz::parse("12:00+05:30", &ParserSettings::default()).unwrap();
    assert_eq!(
        decode_in_zone(b"12:00+05:30", &DataType::TimeTz, 0, zone, transaction),
        Ok(Value::TimeTz(expected))
    );
    assert_eq!(
        decode_in_zone(b"12:00+16", &DataType::TimeTz, 0, zone, transaction),
        Err("22009")
    );
    assert_eq!(
        decode_in_zone(
            &expected.to_binary(),
            &DataType::TimeTz,
            1,
            zone,
            transaction
        ),
        Ok(Value::TimeTz(expected))
    );
}
