use giggles_pg_types::temporal::Interval;
#[test]
fn fractional_seconds_round_like_pg18_decode_interval() {
    for (s, us) in [
        ("-0.0000005 seconds", 0),
        ("0.0000005 seconds", 0),
        ("0.0000015 seconds", 1),
        ("-0.0000015 seconds", -1),
        ("0.0000025 seconds", 2),
        ("1.0000005 seconds", 1_000_000),
    ] {
        assert_eq!(Interval::parse(s).unwrap().microseconds(), us, "{s}");
    }
    assert!(Interval::parse("9223372036855.0000005 seconds").is_err());
}
