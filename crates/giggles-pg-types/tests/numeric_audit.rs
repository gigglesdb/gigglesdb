use giggles_pg_types::{Error, MAX_DIGITS, Numeric};
use proptest::prelude::*;
use std::{
    io::Write,
    process::{Command, Stdio},
};
fn n(s: &str) -> Numeric {
    Numeric::parse(s).unwrap()
}
fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}
#[test]
fn numeric_regressions() {
    for (s, expected) in [
        ("0x_Ff", "255"),
        ("-0o_777", "-511"),
        ("+0b_101", "5"),
        ("0000001e2", "100"),
    ] {
        assert_eq!(n(s).to_string(), expected);
    }
    for s in ["0x", "0x_", "0x__1", "0o8", "0b2", "0x1_", "0x1.0", "1e_2"] {
        assert!(Numeric::parse(s).is_err(), "{s}");
    }
    assert_eq!(
        n("1").checked_div(&n("3")).unwrap().to_string(),
        "0.33333333333333333333"
    );
    assert_eq!(n("NaN").checked_div(&n("0")).unwrap(), n("NaN"));
    assert_eq!(
        n("Infinity").checked_div(&n("0")),
        Err(Error::DivisionByZero)
    );
    assert_eq!(n("Infinity").checked_sub(&n("Infinity")).unwrap(), n("NaN"));
    assert_eq!(n("0.00").checked_mul(&n("Infinity")).unwrap(), n("NaN"));
    assert_eq!(
        n("1.00").checked_div(&n("-Infinity")).unwrap().to_string(),
        "0"
    );
    let s = "9".repeat(MAX_DIGITS);
    assert_eq!(n(&s).to_string(), s);
    assert!(Numeric::parse(&(s.clone() + "0")).is_err());
    assert_eq!(n(&("0".repeat(MAX_DIGITS) + "1")).to_string(), "1");
    let b = n(&s).to_binary();
    assert_eq!(Numeric::from_binary(&b).unwrap().to_binary(), b);
    // Leading group padding must not make a boundary value overflow.
    let mut padded = b.clone();
    let count = u16::from_be_bytes([b[0], b[1]]) + 1;
    let weight = i16::from_be_bytes([b[2], b[3]]) + 1;
    padded[0..2].copy_from_slice(&count.to_be_bytes());
    padded[2..4].copy_from_slice(&weight.to_be_bytes());
    padded.splice(8..8, [0, 0]);
    assert_eq!(Numeric::from_binary(&padded).unwrap().to_binary(), b);
    assert_eq!(
        Numeric::from_binary(&[0, 0, 127, 255, 0, 0, 0, 2])
            .unwrap()
            .to_string(),
        "0.00"
    );
}
proptest! {
 #![proptest_config(ProptestConfig::with_cases(1024))]
 #[test] fn arbitrary_precision_roundtrip(ds in "[1-9][0-9]{40,600}", scale in 0u32..1000, neg in any::<bool>()) {
    let s = format!("{}{}e-{scale}", if neg {"-"}else{""},ds);
    let x=n(&s); let b=x.to_binary(); let y=Numeric::from_binary(&b).unwrap();
    prop_assert_eq!(x.to_string(), y.to_string()); prop_assert_eq!(b,y.to_binary());
    prop_assert_eq!(x.checked_sub(&x).unwrap(),n("0"));
    prop_assert_eq!(x.checked_add(&n("0")).unwrap(),x);
 }
 #[test] fn random_binary_canonicalization(bytes in prop::collection::vec(any::<u8>(),0..9000)) {
    if let Ok(n)=Numeric::from_binary(&bytes) { let canonical=n.to_binary(); prop_assert_eq!(Numeric::from_binary(&canonical).unwrap().to_binary(),canonical); }
 }
}
fn sql(q: &str) -> String {
    let mut child = Command::new("docker")
        .args([
            "exec",
            "-i",
            "dd68-numeric-oracle",
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
    child.stdin.take().unwrap().write_all(q.as_bytes()).unwrap();
    let o = child.wait_with_output().unwrap();
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    String::from_utf8(o.stdout).unwrap().trim_end().into()
}
#[test]
#[ignore]
fn numeric_pg180_oracle() {
    let image = "postgres@sha256:41fc5342eefba6cc2ccda736aaf034bbbb7c3df0fdb81516eba1ba33f360162c";
    let id = |args: &[&str]| Command::new("docker").args(args).output().unwrap().stdout;
    assert_eq!(
        id(&["inspect", "dd68-numeric-oracle", "--format", "{{.Image}}"]),
        id(&["image", "inspect", image, "--format", "{{.Id}}"])
    );
    assert_eq!(sql("show server_version_num"), "180000");
    let mut queries = Vec::new();
    let mut expected = Vec::new();
    let inputs = [
        "NaN",
        "Infinity",
        "-Infinity",
        "0",
        "-0.000",
        "1",
        "-1",
        "1.000000000000000000000000000001",
        "99999999999999999999999999999999999999999999999.1234567",
        "-0.000000000000000000000000000123456789123456789",
        "10000000000000000000000000000000000000000",
    ];
    for a in inputs {
        for b in inputs {
            let x = n(a);
            let y = n(b);
            for (op, r) in [
                ("+", x.checked_add(&y)),
                ("-", x.checked_sub(&y)),
                ("*", x.checked_mul(&y)),
                ("/", x.checked_div(&y)),
            ] {
                if let Ok(r) = r {
                    let expr = format!("('{a}'::numeric {op} '{b}'::numeric)");
                    queries.push(format!(
                        "select {expr}::text,encode(numeric_send({expr}),'hex')"
                    ));
                    expected.push(format!("{}|{}", r, hex(&r.to_binary())));
                } else {
                    let o = Command::new("docker")
                        .args([
                            "exec",
                            "dd68-numeric-oracle",
                            "psql",
                            "-U",
                            "postgres",
                            "-v",
                            "VERBOSITY=verbose",
                            "-Atqc",
                            &format!("select '{a}'::numeric {op} '{b}'::numeric"),
                        ])
                        .output()
                        .unwrap();
                    assert!(!o.status.success());
                    assert!(String::from_utf8_lossy(&o.stderr).contains("22012"));
                }
            }
        }
    }
    for i in 1..=128usize {
        let a = format!(
            "{}{}.{}e{}",
            if i % 2 == 0 { "-" } else { "" },
            "1234567890987654321".repeat(i % 15 + 3),
            "987654321".repeat(i % 11 + 1),
            i as i32 % 51 - 25
        );
        let b = format!(
            "{}{}.{}e{}",
            if i % 3 == 0 { "-" } else { "" },
            "987654321123456789".repeat(i % 9 + 3),
            "123456789".repeat(i % 7 + 1),
            i as i32 % 39 - 19
        );
        let x = n(&a);
        let y = n(&b);
        for (op, r) in [
            ("+", x.checked_add(&y)),
            ("-", x.checked_sub(&y)),
            ("*", x.checked_mul(&y)),
            ("/", x.checked_div(&y)),
        ] {
            let r = r.unwrap();
            let expr = format!("('{a}'::numeric {op} '{b}'::numeric)");
            queries.push(format!(
                "select {expr}::text,encode(numeric_send({expr}),'hex')"
            ));
            expected.push(format!("{}|{}", r, hex(&r.to_binary())));
        }
    }
    // Ordering compares arbitrary precision operands and specials independently.
    for a in inputs {
        for b in inputs {
            let expected = match n(a).cmp(&n(b)) {
                std::cmp::Ordering::Less => "-1",
                std::cmp::Ordering::Equal => "0",
                std::cmp::Ordering::Greater => "1",
            };
            assert_eq!(
                sql(&format!(
                    "select case when '{a}'::numeric < '{b}'::numeric then -1 when '{a}'::numeric = '{b}'::numeric then 0 else 1 end"
                )),
                expected
            );
        }
    }
    // Keep individual command lines below Windows limits.
    for (qs, es) in queries.chunks(10).zip(expected.chunks(10)) {
        let out = sql(&qs.join(";"));
        assert_eq!(out.lines().count(), es.len());
        for (o, e) in out.lines().zip(es) {
            assert_eq!(o, e, "query batch {qs:?}");
        }
    }
    let mut literal_count = 0;
    for s in [
        "0x_Ff".to_owned(),
        "-0o_777".into(),
        "0b_101".into(),
        "00001e-3".into(),
        "9".repeat(MAX_DIGITS),
        format!("1e-{}", 16383),
    ] {
        assert_eq!(
            sql(&format!(
                "select '{s}'::numeric::text,encode(numeric_send('{s}'::numeric),'hex')"
            )),
            format!("{}|{}", n(&s), hex(&n(&s).to_binary()))
        );
        literal_count += 1;
    }
    // Exercise numeric_recv through real binary COPY, including hidden fractions,
    // redundant zeros, negative zero, specials, invalid groups/sign/scale/length.
    sql("create temp table unused(x numeric); create table if not exists dd68_recv(x numeric)");
    let mut payloads = vec![
        vec![0, 0, 127, 255, 0, 0, 0, 2],
        vec![0, 1, 255, 255, 0, 0, 0, 0, 0, 1],
        vec![0, 1, 0, 0, 0, 0, 0, 0, 0x27, 0x10],
        vec![0, 0, 0, 0, 0x20, 0, 0, 0],
        vec![0, 0, 0, 0, 0, 0, 0x40, 0],
    ];
    for i in 0..32u16 {
        let mut b = Vec::new();
        for w in [
            3,
            i % 5,
            if i % 2 == 0 { 0 } else { 0x4000 },
            i % 17,
            0,
            i * 311 % 10000,
            i * 997 % 10000,
        ] {
            b.extend(w.to_be_bytes());
        }
        payloads.push(b);
    }
    let mut seed = 0x6818_0000_u64;
    for i in 0..64u16 {
        let mut b = Vec::new();
        let mut words = vec![
            3,
            (i as i16 % 15 - 7) as u16,
            if i % 2 == 0 { 0 } else { 0x4000 },
            i % 29,
        ];
        for _ in 0..3 {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            words.push(((seed >> 32) % if i % 7 == 0 { 65536 } else { 10000 }) as u16);
        }
        for w in words {
            b.extend(w.to_be_bytes());
        }
        payloads.push(b);
    }
    let boundary = n(&"9".repeat(MAX_DIGITS)).to_binary();
    payloads.push(boundary.clone());
    let mut padded = boundary.clone();
    padded[0..2].copy_from_slice(&4097u16.to_be_bytes());
    padded[2..4].copy_from_slice(&4096i16.to_be_bytes());
    padded.splice(8..8, [0, 0]);
    payloads.push(padded);
    for bytes in &payloads {
        sql("truncate dd68_recv");
        let mut stream = b"PGCOPY\n\xff\r\n\0".to_vec();
        stream.extend([0; 8]);
        stream.extend(1i16.to_be_bytes());
        stream.extend((bytes.len() as i32).to_be_bytes());
        stream.extend(bytes);
        stream.extend((-1i16).to_be_bytes());
        let mut child = Command::new("docker")
            .args([
                "exec",
                "-i",
                "dd68-numeric-oracle",
                "psql",
                "-U",
                "postgres",
                "-v",
                "ON_ERROR_STOP=1",
                "-Atqc",
                "copy dd68_recv from stdin binary",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child.stdin.take().unwrap().write_all(&stream).unwrap();
        let o = child.wait_with_output().unwrap();
        match Numeric::from_binary(bytes) {
            Ok(n) => {
                assert!(
                    o.status.success(),
                    "{:?}: {}",
                    bytes,
                    String::from_utf8_lossy(&o.stderr)
                );
                assert_eq!(
                    sql("select x::text,encode(numeric_send(x),'hex') from dd68_recv"),
                    format!("{}|{}", n, hex(&n.to_binary()))
                );
            }
            Err(_) => assert!(!o.status.success(), "PG accepted {:?}", bytes),
        }
    }
    println!(
        "PG18.0 pinned: {} arithmetic text+numeric_send, {literal_count} literals/boundaries, {} binary COPY cases; division-by-zero SQLSTATE22012 checked",
        expected.len(),
        payloads.len()
    );
}
