use giggles_pg_types::{
    Numeric,
    typmod::{NumericTypmod, TypmodError},
};
use proptest::prelude::*;

#[test]
fn postgres_numeric_typmod_fixture() {
    let mut apply_checked = 0;
    let mut modifier_checked = 0;
    let mut packed_checked = 0;
    for line in include_str!("fixtures/typmod_pg18.tsv").lines().skip(1) {
        let fields: Vec<_> = line.split('\t').collect();
        if fields[0] == "numeric_apply" {
            let (p, s) = fields[2].split_once(',').unwrap();
            let modifier = NumericTypmod::new(p.parse().unwrap(), s.parse().unwrap()).unwrap();
            let actual = Numeric::parse(fields[1]).unwrap().apply_typmod(modifier);
            if fields[3] == "ERROR" {
                let error = actual.unwrap_err();
                assert_eq!(error.sqlstate(), fields[4], "{line}");
                assert_eq!(error.detail(), fields[5], "{line}");
            } else {
                let actual = actual.unwrap();
                assert_eq!(actual, Numeric::parse(fields[3]).unwrap(), "{line}");
                assert_eq!(actual.to_string(), fields[3], "{line}");
            }
            apply_checked += 1;
        } else if fields[0] == "numeric_modifier" {
            let args = fields[2].trim_start_matches('(').trim_end_matches(')');
            let parts: Vec<_> = args.split(',').collect();
            let error = if parts.len() > 2 || parts.is_empty() || args.is_empty() {
                TypmodError::InvalidModifier
            } else {
                let p = parts[0].parse().unwrap();
                let s = parts.get(1).map_or(0, |s| s.parse().unwrap());
                NumericTypmod::new(p, s).unwrap_err()
            };
            assert_eq!(error.sqlstate(), fields[4], "{line}");
            assert_eq!(error.to_string(), fields[5], "{line}");
            modifier_checked += 1;
        } else if fields[0] == "typmod" && fields[1].starts_with("numeric(") {
            let args = fields[1]
                .strip_prefix("numeric(")
                .unwrap()
                .strip_suffix(')')
                .unwrap();
            let mut parts = args.split(',');
            let p = parts.next().unwrap().parse().unwrap();
            let s = parts.next().map_or(0, |s| s.parse().unwrap());
            let modifier = NumericTypmod::new(p, s).unwrap();
            let (packed, formatted) = fields[3].split_once(',').unwrap();
            assert_eq!(
                modifier.to_typmod(),
                packed.parse::<i32>().unwrap(),
                "{line}"
            );
            assert_eq!(
                NumericTypmod::from_typmod(modifier.to_typmod()),
                Some(modifier),
                "{line}"
            );
            assert_eq!(format!("numeric{modifier}"), formatted, "{line}");
            packed_checked += 1;
        }
    }
    assert!(apply_checked >= 150);
    assert_eq!(modifier_checked, 4);
    assert_eq!(packed_checked, 4);
}

#[test]
fn numeric_validation_and_raw_decode() {
    assert_eq!(
        NumericTypmod::new(0, 1001),
        Err(TypmodError::NumericPrecision(0))
    );
    assert_eq!(
        NumericTypmod::new(1, -1001),
        Err(TypmodError::NumericScale(-1001))
    );
    assert_eq!(
        TypmodError::NumericPrecision(0).to_string(),
        "NUMERIC precision 0 must be between 1 and 1000"
    );
    assert_eq!(
        TypmodError::NumericScale(-1001).to_string(),
        "NUMERIC scale -1001 must be between -1000 and 1000"
    );
    assert_eq!(TypmodError::NumericScale(-1001).sqlstate(), "22023");
    for raw in [i32::MIN, -1, 0, 3] {
        assert_eq!(NumericTypmod::from_typmod(raw), None);
    }
    let edge = NumericTypmod::from_typmod(i32::MAX).unwrap();
    assert_eq!((edge.precision(), edge.scale()), (32767, -5));
    assert_eq!(NumericTypmod::from_typmod(edge.to_typmod()), Some(edge));
    let raw = NumericTypmod::from_typmod(4).unwrap();
    assert_eq!((raw.precision(), raw.scale()), (0, 0));
    assert_eq!(NumericTypmod::from_typmod(1028).unwrap().scale(), -1024);
}

#[test]
fn numeric_rounding_bounds_scales_and_specials() {
    for (input, p, s, expected) in [
        ("99.94", 3, 1, "99.9"),
        ("-1.25", 2, 1, "-1.3"),
        ("1499", 2, -2, "1500"),
        ("0.0049", 2, 4, "0.0049"),
        ("0", 1, 4, "0.0000"),
        ("1", 4, 3, "1.000"),
        ("NaN", 1, 0, "NaN"),
    ] {
        let result = Numeric::parse(input)
            .unwrap()
            .apply_typmod(NumericTypmod::new(p, s).unwrap())
            .unwrap();
        assert_eq!(result.to_string(), expected);
    }
    for input in ["99.95", "-99.95", "Infinity", "-Infinity"] {
        let error = Numeric::parse(input)
            .unwrap()
            .apply_typmod(NumericTypmod::new(3, 1).unwrap())
            .unwrap_err();
        assert_eq!(error.sqlstate(), "22003");
        assert_eq!(error.to_string(), "numeric field overflow");
        assert_eq!(
            error.detail(),
            if input.contains("Infinity") {
                "A field with precision 3, scale 1 cannot hold an infinite value."
            } else {
                "A field with precision 3, scale 1 must round to an absolute value less than 10^2."
            }
        );
    }
    let e = Numeric::parse("0.01")
        .unwrap()
        .apply_typmod(NumericTypmod::new(1, 3).unwrap())
        .unwrap_err();
    assert_eq!(
        e.detail(),
        "A field with precision 1, scale 3 must round to an absolute value less than 10^-2."
    );
    let e = Numeric::parse("1")
        .unwrap()
        .apply_typmod(NumericTypmod::new(1, 1).unwrap())
        .unwrap_err();
    assert_eq!(
        e.detail(),
        "A field with precision 1, scale 1 must round to an absolute value less than 1."
    );
    // Raw catalog values are decoded without numerictypmodin validation.
    let raw = NumericTypmod::from_typmod(i32::MAX).unwrap();
    let _ = Numeric::parse("1").unwrap().apply_typmod(raw);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]
    #[test]
    fn full_range_typmod_roundtrip(p in 1i32..=1000, s in -1000i32..=1000) {
        let modifier = NumericTypmod::new(p,s).unwrap();
        prop_assert_eq!(NumericTypmod::from_typmod(modifier.to_typmod()), Some(modifier));
        prop_assert_eq!(modifier.to_string(), format!("({p},{s})"));
    }
}
