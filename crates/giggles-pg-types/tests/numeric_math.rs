use giggles_pg_types::{Error, Numeric};

#[test]
fn pinned_pg18_text_and_binary() {
    for row in include_str!("fixtures/numeric_sqrt_pg18.txt").lines() {
        let fields: Vec<_> = row.split('|').collect();
        let value = Numeric::parse(fields[0]).unwrap().checked_sqrt().unwrap();
        assert_eq!(value.to_string(), fields[1], "{}", fields[0]);
        let binary: String = value
            .to_binary()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        assert_eq!(binary, fields[2], "{}", fields[0]);
    }
}

#[test]
fn negative_domains() {
    for input in ["-1", "-0.000001", "-Infinity"] {
        assert!(matches!(
            Numeric::parse(input).unwrap().checked_sqrt(),
            Err(Error::Range)
        ));
    }
}

#[test]
fn perfect_integer_squares() {
    for n in 0u64..1000 {
        let value = Numeric::parse(&(n * n).to_string())
            .unwrap()
            .checked_sqrt()
            .unwrap();
        assert_eq!(value, Numeric::parse(&n.to_string()).unwrap());
    }
}

#[test]
fn exact_midpoint_and_neighbors() {
    // At selected scale 16, sqrt(0.25 + 5e-17 + 25e-34) is
    // exactly 0.50000000000000005: the midpoint rounds upward.
    for (input, expected) in [
        (
            "0.2500000000000000500000000000000025",
            "0.5000000000000000500000000000000000",
        ),
        ("-0.000", "0.000000000000000"),
    ] {
        // Input dscale is preserved unless PG's display-scale cap applies.
        assert_eq!(
            Numeric::parse(input)
                .unwrap()
                .checked_sqrt()
                .unwrap()
                .to_string(),
            expected
        );
    }
}

#[test]
fn maximum_input_and_scale_caps() {
    let tiny = Numeric::parse("1e-16383").unwrap().checked_sqrt().unwrap();
    assert_eq!(tiny.scale(), Some(1000));
    assert_eq!(tiny, Numeric::parse("0").unwrap());
    let huge = Numeric::parse("1e16383").unwrap().checked_sqrt().unwrap();
    assert_eq!(huge.scale(), Some(0));
    assert_eq!(huge.to_string().len(), 8192);
}

#[test]
fn half_way_at_display_cap() {
    // Force PG's 1000-digit display cap, making a representable exact tie.
    let ten = num_bigint::BigInt::from(10u8).pow(1001);
    let midpoint = &ten / 2u8 + 5u8;
    let squared = &midpoint * &midpoint;
    let input = format!("0.{:0>2002}", squared.to_string());
    let expected = format!("0.5{}1", "0".repeat(998));
    let above = Numeric::parse(&input).unwrap().checked_sqrt().unwrap();
    assert_eq!(above.to_string(), expected);
    let below_input = format!("0.{:0>2002}", (squared - 1u8).to_string());
    let below = Numeric::parse(&below_input)
        .unwrap()
        .checked_sqrt()
        .unwrap();
    assert_eq!(below.to_string(), format!("0.5{}", "0".repeat(999)));
}
