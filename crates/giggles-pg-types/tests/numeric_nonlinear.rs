use giggles_pg_types::{Error, Numeric};
#[test]
fn pg18_nonlinear_text_scale_binary() {
    for row in include_str!("fixtures/numeric_nonlinear_pg18.txt").lines() {
        let f: Vec<_> = row.split('|').collect();
        let x = Numeric::parse(f[1]).unwrap();
        let y = match f[0] {
            "ln" => x.checked_ln(),
            "exp" => x.checked_exp(),
            "log" => Numeric::checked_log(&Numeric::parse("3").unwrap(), &x),
            op if op.starts_with("log:") => {
                Numeric::checked_log(&Numeric::parse(&op[4..]).unwrap(), &x)
            }
            _ => panic!(),
        }
        .unwrap();
        assert_eq!(y.to_string(), f[2], "{} {}", f[0], f[1]);
        assert_eq!(y.scale().unwrap().to_string(), f[3]);
        assert_eq!(
            y.to_binary()
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>(),
            f[4]
        );
    }
}
#[test]
fn domains_and_specials() {
    for x in ["0", "-1", "-Infinity"] {
        assert_eq!(
            Numeric::parse(x).unwrap().checked_ln().unwrap_err(),
            Error::Range
        );
    }
    for x in ["NaN", "Infinity"] {
        assert_eq!(
            Numeric::parse(x).unwrap().checked_ln().unwrap().to_string(),
            x
        );
    }
    assert_eq!(
        Numeric::parse("-Infinity")
            .unwrap()
            .checked_exp()
            .unwrap()
            .to_string(),
        "0"
    );
    assert_eq!(
        Numeric::checked_log(&Numeric::parse("1").unwrap(), &Numeric::parse("2").unwrap())
            .unwrap_err(),
        Error::DivisionByZero
    );
}

#[test]
fn logarithm_error_precedence_and_infinities() {
    let n = |s| Numeric::parse(s).unwrap();
    for (a, b, want) in [
        ("NaN", "-1", "NaN"),
        ("-1", "NaN", "NaN"),
        ("Infinity", "Infinity", "NaN"),
        ("Infinity", "0.5", "0"),
        ("0.5", "Infinity", "Infinity"),
        ("1", "Infinity", "Infinity"),
    ] {
        assert_eq!(
            Numeric::checked_log(&n(a), &n(b)).unwrap().to_string(),
            want
        );
    }
    for (a, b) in [
        ("Infinity", "0"),
        ("0", "Infinity"),
        ("-Infinity", "2"),
        ("2", "-Infinity"),
        ("1", "-1"),
    ] {
        assert_eq!(
            Numeric::checked_log(&n(a), &n(b)).unwrap_err(),
            Error::Range
        );
    }
    assert_eq!(n("6000").checked_exp().unwrap_err(), Error::Range);
    assert_eq!(n("1e16383").checked_exp().unwrap_err(), Error::Range);
    let tiny = n("-1e16383").checked_exp().unwrap();
    assert_eq!(tiny, n("0"));
    assert_eq!(tiny.scale(), Some(1000));
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(100))]
    #[test]
    fn nonlinear_order_and_identity(i in 1u32..100000, j in 1u32..100000) {
        let a=Numeric::parse(&format!("{}.{:03}",i/1000,i%1000)).unwrap();
        let b=Numeric::parse(&format!("{}.{:03}",j/1000,j%1000)).unwrap();
        proptest::prop_assert_eq!(a.checked_ln().unwrap().cmp(&b.checked_ln().unwrap()),a.cmp(&b));
        proptest::prop_assert_eq!(a.checked_exp().unwrap().cmp(&b.checked_exp().unwrap()),a.cmp(&b));
        if i!=1000 {proptest::prop_assert_eq!(Numeric::checked_log(&a,&a).unwrap(),Numeric::parse("1").unwrap());}
    }
}
