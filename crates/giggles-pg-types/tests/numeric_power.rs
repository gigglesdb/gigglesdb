use giggles_pg_types::{Error, Numeric};
#[test]
fn pg18_power_and_independent_nonlinear_audit() {
    for row in include_str!("fixtures/numeric_power_audit_pg18.txt").lines() {
        let f: Vec<_> = row.split('|').collect();
        let a = Numeric::parse(f[1]).unwrap();
        let b = Numeric::parse(f[2]).unwrap();
        let result = match f[0] {
            "pow" => a.checked_pow(&b),
            "exp" => a.checked_exp(),
            "ln" => a.checked_ln(),
            "log" => Numeric::checked_log(&a, &b),
            _ => unreachable!(),
        };
        if f[3].bytes().filter(u8::is_ascii_digit).count() > giggles_pg_types::MAX_DIGITS {
            assert_eq!(result.unwrap_err(), Error::Limit);
        } else if f[3] == "ERROR" {
            assert_eq!(result.unwrap_err(), Error::Range, "{row}");
        } else {
            let y = result.unwrap_or_else(|e| panic!("{e:?}: {row}"));
            assert_eq!(y.to_string(), f[3], "{} {} {}", f[0], f[1], f[2]);
            assert_eq!(
                y.scale()
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| "".into()),
                f[4],
                "{row}"
            );
            assert_eq!(
                y.to_binary()
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .collect::<String>(),
                f[5],
                "{row}"
            );
        }
    }
}

#[test]
fn exact_integer_properties_and_explicit_limits() {
    let n = |s: &str| Numeric::parse(s).unwrap();
    for base in -20i64..=20 {
        for exp in 0u32..=10 {
            let got = n(&base.to_string())
                .checked_pow(&n(&exp.to_string()))
                .unwrap();
            assert_eq!(got, n(&base.pow(exp).to_string()));
        }
    }
    assert_eq!(n("1e10000").checked_pow(&n("2")), Err(Error::Limit));
    assert_eq!(n("0").checked_pow(&n("-1")), Err(Error::Range));
    assert_eq!(n("-2").checked_pow(&n("0.5")), Err(Error::Range));
}
