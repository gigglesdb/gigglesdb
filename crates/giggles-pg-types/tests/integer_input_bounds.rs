use giggles_pg_types::integer::{IntegerError, MAX_INPUT_BYTES, parse_i16, parse_i32, parse_i64};

#[test]
fn all_smallints_and_seeded_wider_values_round_trip_in_every_base() {
    for n in i16::MIN..=i16::MAX {
        let magnitude = n.unsigned_abs();
        let sign = if n < 0 { "-" } else { "+" };
        for text in [
            format!("{n}"),
            format!("{sign}0x_{magnitude:x}"),
            format!("{sign}0o_{magnitude:o}"),
            format!("{sign}0b_{magnitude:b}"),
        ] {
            assert_eq!(parse_i16(&text), Ok(n));
        }
    }
    let mut seed = 680180u64;
    for _ in 0..4096 {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let n = seed as i64;
        let sign = if n < 0 { "-" } else { "+" };
        let mag = n.unsigned_abs();
        for text in [
            format!("{n}"),
            format!("{sign}0x_{mag:x}"),
            format!("{sign}0o_{mag:o}"),
            format!("{sign}0b_{mag:b}"),
        ] {
            assert_eq!(parse_i64(&text), Ok(n));
            assert_eq!(
                parse_i32(&text),
                i32::try_from(n).map_err(|_| IntegerError::Range)
            );
        }
    }
}

#[test]
fn hard_byte_cap_and_long_zero_or_whitespace_inputs() {
    let zeros = "0".repeat(MAX_INPUT_BYTES);
    assert_eq!(parse_i16(&zeros), Ok(0));
    assert_eq!(parse_i32(&zeros), Ok(0));
    assert_eq!(parse_i64(&zeros), Ok(0));
    let spaces = " ".repeat(MAX_INPUT_BYTES);
    assert_eq!(parse_i64(&spaces), Err(IntegerError::Syntax));
    for text in [zeros + "0", spaces + "1", "x".repeat(MAX_INPUT_BYTES + 1)] {
        assert_eq!(parse_i16(&text), Err(IntegerError::Limit));
        assert_eq!(parse_i32(&text), Err(IntegerError::Limit));
        assert_eq!(parse_i64(&text), Err(IntegerError::Limit));
    }
    assert_eq!(IntegerError::Syntax.sqlstate(), Some("22P02"));
    assert_eq!(IntegerError::Range.sqlstate(), Some("22003"));
    assert_eq!(IntegerError::Limit.sqlstate(), None);
    for text in ["1\0", "\0", "\u{a0}1", "\u{661}"] {
        assert_eq!(parse_i64(text), Err(IntegerError::Syntax));
    }
}

#[test]
fn nul_is_not_a_terminator_and_prefix_range_precedence_is_explicit() {
    // NUL is outside PostgreSQL text admission; these are API-only controls.
    let text = "99999999999999999999999999999999999999\0";
    assert_eq!(parse_i16(text), Err(IntegerError::Range));
    assert_eq!(parse_i32(text), Err(IntegerError::Range));
    assert_eq!(parse_i64(text), Err(IntegerError::Range));
    assert_eq!(parse_i16("32768\0"), Err(IntegerError::Syntax));
    assert_eq!(parse_i32("2147483648\0"), Err(IntegerError::Syntax));
    assert_eq!(
        parse_i64("9223372036854775808\0"),
        Err(IntegerError::Syntax)
    );
}
