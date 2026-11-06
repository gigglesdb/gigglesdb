use giggles_pg_types::{Error, Float4, Float8};
use std::io::Write;
use std::process::{Command, Stdio};
fn sql(q: &str) -> String {
    let container =
        std::env::var("DD68_FLOAT_PG_CONTAINER").unwrap_or("dd68-numeric-oracle".into());
    let mut child = Command::new("docker")
        .args([
            "exec",
            "-i",
            &container,
            "psql",
            "-U",
            "postgres",
            "-v",
            "ON_ERROR_STOP=1",
            "-Atq",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    let input = q.as_bytes().to_vec();
    let writer = std::thread::spawn(move || stdin.write_all(&input).unwrap());
    let out = child.wait_with_output().unwrap();
    writer.join().unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap()
}
fn hex(b: &[u8]) -> String {
    b.iter().map(|b| format!("{b:02x}")).collect()
}
fn literal(bits: u64, p: u32, bias: i32) -> String {
    let eb = if p == 24 { 8 } else { 11 };
    let exp = ((bits >> (p - 1)) & ((1 << eb) - 1)) as i32;
    let frac = bits & ((1 << (p - 1)) - 1);
    let sign = if bits >> (p - 1 + eb) != 0 { "-" } else { "" };
    if exp == (1 << eb) - 1 {
        if frac == 0 {
            format!("{sign}Infinity")
        } else {
            format!("{sign}NaN({frac})")
        }
    } else {
        let m = frac | if exp == 0 { 0 } else { 1 << (p - 1) };
        format!("{sign}0x{m:x}p{}", exp.max(1) - bias - (p as i32 - 1))
    }
}
#[test]
fn syntax_range_and_edges() {
    for s in [
        "", ".", "0x", "0x.p1", "0x1p", "0x1p+", "0x1.2.3", "0x1p2p3", "1_0", "NaN(-1)", "1e",
        "1e1e1", " 1 2", "\u{221e}",
    ] {
        assert_eq!(Float4::parse(s), Err(Error::Syntax), "{s}");
        assert_eq!(Float8::parse(s), Err(Error::Syntax), "{s}");
    }
    for s in [
        "1e99999999999999",
        "1e-99999999999999",
        "0x1p9999999999999",
        "0x123p9223372036854775807",
        "0x1p-99999999999999",
    ] {
        assert_eq!(Float4::parse(s), Err(Error::Range), "{s}");
        assert_eq!(Float8::parse(s), Err(Error::Range), "{s}");
    }
    for s in ["-0e99999999999999999", "-0x0p9999999999999999"] {
        assert_eq!(Float4::parse(s).unwrap().to_bits(), 1 << 31);
        assert_eq!(Float8::parse(s).unwrap().to_bits(), 1 << 63);
    }
    assert_eq!(Float4::from_bits(1).to_text(), "1e-45");
    assert_eq!(Float8::from_bits(1).to_text(), "5e-324");
    assert_eq!(Float4::parse("0x1p-150"), Err(Error::Range));
    assert_eq!(Float8::parse("0x1p-1075"), Err(Error::Range));
}
#[test]
#[ignore = "requires live PostgreSQL 18.0 container; DD68_FLOAT_PG_CONTAINER overrides name"]
fn pg18_random_bits_and_modes() {
    assert_eq!(sql("show server_version_num;\n").trim(), "180000");
    let mut seed = 0xd068_f10a_2026_u64;
    let mut bits = vec![
        0,
        1,
        2,
        3,
        0x8000000000000000,
        0x7fefffffffffffff,
        0x0010000000000000,
        0x7ff0000000000000,
        0xfff0000000000000,
        0x7ff8000000000042,
        0xfff8000000001234,
    ];
    for _ in 0..2048 {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        bits.push(seed);
    }
    for value in [
        1e-5f64,
        1e-4,
        1e14,
        1e15,
        1e16,
        1.0000000000000002,
        9007199254740992.0,
    ] {
        for b in value.to_bits() - 2..=value.to_bits() + 2 {
            bits.push(b);
        }
    }
    // Float4 subnormal/normal/max boundaries, exponent transitions and integer
    // midpoints (including decimals that round-trip but touch an open bound).
    for b in [
        0x007fffffu64,
        0x00800000,
        0x7f7fffff,
        0x80000000,
        0x7fc01234,
        0xffc01234,
    ] {
        bits.push(b);
    }
    for exp in 1u64..255 {
        for offset in 0..=2 {
            bits.push((exp << 23) + offset);
        }
    }
    let mut queries = String::new();
    let mut expected = Vec::new();
    for b in bits {
        for (p, bias) in [(24, 127), (53, 1023)] {
            let b = if p == 24 { b & 0xffffffff } else { b };
            let lit = literal(b, p, bias);
            let (binary, texts) = if p == 24 {
                let f = Float4::from_bits(b as u32);
                if f.value().is_finite() {
                    assert_eq!(Float4::parse(&f.to_text()).unwrap().to_bits(), b as u32);
                    assert_eq!(Float4::parse(&lit).unwrap().to_bits(), b as u32);
                }
                assert_eq!(f.to_binary(), (b as u32).to_be_bytes());
                (
                    hex(&Float4::parse(&lit).unwrap().to_binary()),
                    (-15..=3)
                        .map(|e| f.to_text_with_extra_float_digits(e).unwrap())
                        .collect::<Vec<_>>(),
                )
            } else {
                let f = Float8::from_bits(b);
                if f.value().is_finite() {
                    assert_eq!(Float8::parse(&f.to_text()).unwrap().to_bits(), b);
                    assert_eq!(Float8::parse(&lit).unwrap().to_bits(), b);
                }
                assert_eq!(f.to_binary(), b.to_be_bytes());
                (
                    hex(&Float8::parse(&lit).unwrap().to_binary()),
                    (-15..=3)
                        .map(|e| f.to_text_with_extra_float_digits(e).unwrap())
                        .collect::<Vec<_>>(),
                )
            };
            for (extra, text) in (-15..=3).zip(texts) {
                queries.push_str(&format!("set extra_float_digits={extra}; select encode(float{sz}send('{lit}'::float{sz}),'hex'),'{lit}'::float{sz}::text;\n",sz=if p==24 {4}else{8}));
                expected.push((format!("{binary}|{text}"), lit.clone(), extra));
            }
        }
    }
    // Feed SQL concurrently with stdout drainage to avoid pipe deadlock.
    let observed = sql(&queries);
    assert_eq!(observed.lines().count(), expected.len());
    for (row, (want, lit, extra)) in observed.lines().zip(expected) {
        assert_eq!(row, want, "{lit}, extra={extra}");
    }
}

#[test]
#[ignore = "requires live PostgreSQL 18.0"]
fn pg18_input_semantics() {
    let cases = [
        "-NaN",
        "+NaN",
        "NaN()",
        "NaN(0x42)",
        "-NaN(123)",
        "NaN(word)",
        "NaN(077)",
        "NaN(18446744073709551616)",
        "0x1.8",
        "0X.8P+1",
        "0x1p-149",
        "0x1p-150",
        "0x1.000001p0",
        "1.000000059604644775390625",
        "1.00000000000000011102230246251565404236316680908203125",
        "3.4028234663852886e38",
        "1.7976931348623157e308",
        "5e-324",
        "1e-9999",
        "1e9999",
        "1e9999x",
        "1e-9999x",
        "0x1p9999x",
        "0x1p-9999x",
        "-0e999999999999999999999",
        "0x0p-999999999999999999999",
        "",
        "0x",
        "0x1p",
        "1_0",
        "nan(-1)",
        "1.2.3",
        "1e",
        "1e1x",
        " inf ",
        "\t-0\n",
        "\u{b}-0\u{b}",
    ];
    for width in [4, 8] {
        let mut q = String::new();
        let mut expected = Vec::new();
        for s in cases {
            let result = if width == 4 {
                Float4::parse(s).map(|f| hex(&f.to_binary()))
            } else {
                Float8::parse(s).map(|f| hex(&f.to_binary()))
            };
            expected.push(match result {
                Ok(b) => b,
                Err(Error::Range) => "22003".into(),
                Err(Error::Syntax) => "22P02".into(),
                e => panic!("unexpected {e:?}"),
            });
            q.push_str(&format!("select case when pg_input_is_valid(x,'float{width}') then encode(float{width}send(x::float{width}),'hex') else (pg_input_error_info(x,'float{width}')).sql_error_code end from (select '{s}'::text as x offset 0) t;\n"));
        }
        let out = sql(&q);
        for ((s, want), row) in cases.iter().zip(expected).zip(out.lines()) {
            assert_eq!(row, want, "float{width}: {s}");
        }
    }
}
