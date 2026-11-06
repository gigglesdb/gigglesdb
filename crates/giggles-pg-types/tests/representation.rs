use giggles_pg_types::*;
use proptest::prelude::*;
fn n(s: &str) -> Numeric {
    Numeric::parse(s).unwrap()
}
#[test]
fn numeric_semantics() {
    assert_eq!(n("1_000.2_0e+0_1").to_string(), "10002.0");
    assert_eq!(Numeric::parse("0xFF").unwrap().to_string(), "255");
    assert_eq!(
        Numeric::checked_sum([&n("1.00"), &n("2.0")])
            .unwrap()
            .unwrap()
            .to_string(),
        "3.00"
    );
    assert_eq!(
        n("1").checked_div(&n("3")).unwrap().to_string(),
        "0.33333333333333333333"
    );
    assert_eq!(n("1.00"), n("1"));
    assert_eq!(n("-0.00").to_string(), "0.00");
    assert!(n("NaN") > n("Infinity"));
    assert!(n("-Infinity") < n("-1e1000"));
    assert_eq!(
        n("12345678901234567890123456789012345678901234567890.00001").to_string(),
        "12345678901234567890123456789012345678901234567890.00001"
    );
    assert_eq!(n("1.2300e2").to_string(), "123.00");
    for s in ["", ".", "1.2.3", "1e", "--1", "+NaN", "1e1e1"] {
        assert!(Numeric::parse(s).is_err(), "{s}")
    }
    assert_eq!(Numeric::parse("1e999999999"), Err(Error::Limit));
    assert!(Numeric::parse(&"1".repeat(MAX_DIGITS + 1)).is_err());
    assert_eq!(n("-2.5").round(0).unwrap(), n("-3"));
    assert_eq!(n("150").round(-2).unwrap(), n("200"));
    assert_eq!(
        n("1.20").checked_add(&n("2.300")).unwrap().to_string(),
        "3.500"
    );
    assert_eq!(
        n("1.2").checked_mul(&n("2.00")).unwrap().to_string(),
        "2.400"
    );
    assert_eq!(n("1").div_at_scale(&n("8"), 2).unwrap().to_string(), "0.13");
    assert_eq!(n("1").div_at_scale(&n("0"), 0), Err(Error::DivisionByZero));
    assert_eq!(n("NaN").checked_add(&n("1")).unwrap().to_string(), "NaN");
}
#[test]
fn binary_validation() {
    for len in 0..8 {
        assert!(Numeric::from_binary(&vec![0; len]).is_err())
    }
    for b in [
        vec![0, 2, 0, 0, 0, 0, 0, 0, 0, 0],
        vec![0, 1, 0, 0, 0, 0, 0, 0, 0x27, 0x10],
        vec![0, 0, 0, 0, 0x20, 0, 0, 0],
        vec![0, 0, 0, 0, 0, 0, 0x40, 0],
    ] {
        assert!(Numeric::from_binary(&b).is_err())
    }
    assert!(Numeric::from_binary(&[255, 255, 0, 0, 0, 0, 0, 0]).is_err());
    assert!(Numeric::from_binary(&[0, 1, 127, 255, 0, 0, 0, 0, 0, 1]).is_err());
    assert_eq!(
        Numeric::from_binary(&[0, 1, 0, 0, 0, 0, 0, 0, 0, 1]).unwrap(),
        n("1")
    );
    assert_eq!(
        Numeric::from_binary(&[0, 1, 255, 255, 0, 0, 0, 0, 0, 1]).unwrap(),
        n("0")
    );
}
#[test]
fn float_semantics() {
    assert_eq!(Float8::from_bits(0), Float8::from_bits(1 << 63));
    assert_eq!(
        Float8::from_bits(0x7ff8000000000001),
        Float8::from_bits(0x7ff8000000001234)
    );
    assert!(Float8::parse("NaN").unwrap() > Float8::parse("Infinity").unwrap());
    assert_eq!(
        Float4::parse("7.038531e-26").unwrap().to_bits(),
        "7.038531e-26".parse::<f32>().unwrap().to_bits()
    );
    for s in ["1e999", "1e-999"] {
        assert_eq!(Float4::parse(s), Err(Error::Range));
        assert_eq!(Float8::parse(s), Err(Error::Range));
    }
    assert!(Float4::from_binary(&[0; 3]).is_err());
    assert!(Float8::from_binary(&[0; 9]).is_err());
}
proptest! {
 #![proptest_config(ProptestConfig::with_cases(1024))]
 #[test] fn decimal_roundtrip(c in any::<i128>(),s in 0u32..100){let x=n(&format!("{c}e-{s}"));let y=Numeric::from_binary(&x.to_binary()).unwrap();prop_assert_eq!(x.to_string(),y.to_string());prop_assert_eq!(y.to_binary(),x.to_binary());}
 #[test] fn float_bits(b in any::<u64>(),a in any::<u32>()){let x=Float8::from_bits(b);prop_assert_eq!(Float8::from_binary(&x.to_binary()).unwrap().to_bits(),b);let y=Float4::from_bits(a);prop_assert_eq!(Float4::from_binary(&y.to_binary()).unwrap().to_bits(),a);if x.value().is_finite(){prop_assert_eq!(Float8::parse(&x.to_text()).unwrap().to_bits(),b)}
if y.value().is_finite(){prop_assert_eq!(Float4::parse(&y.to_text()).unwrap().to_bits(),a)}}
 #[test] fn arithmetic(a in -1000000000i64..1000000000,b in -1000000000i64..1000000000){prop_assert_eq!(n(&a.to_string()).checked_add(&n(&b.to_string())).unwrap(),n(&(a+b).to_string()));prop_assert_eq!(n(&a.to_string()).checked_sub(&n(&b.to_string())).unwrap(),n(&(a-b).to_string()));prop_assert_eq!(n(&a.to_string()).checked_mul(&n(&b.to_string())).unwrap(),n(&(i128::from(a)*i128::from(b)).to_string()));}
 #[test] fn malformed_never_panics(bytes in prop::collection::vec(any::<u8>(),0..512)){let _=Numeric::from_binary(&bytes);}
}
