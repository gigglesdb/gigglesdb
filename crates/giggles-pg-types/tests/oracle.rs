use giggles_pg_types::*;
use std::process::Command;
const IMAGE: &str =
    "postgres@sha256:41fc5342eefba6cc2ccda736aaf034bbbb7c3df0fdb81516eba1ba33f360162c";
fn sql(q: &str) -> String {
    let o = Command::new("docker")
        .args([
            "exec",
            "dd68-pgtypes-oracle",
            "psql",
            "-U",
            "postgres",
            "-v",
            "ON_ERROR_STOP=1",
            "-Atqc",
            q,
        ])
        .output()
        .unwrap();
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    String::from_utf8(o.stdout).unwrap().trim_end().into()
}
fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}
/// Explicit live check; create the isolated container described in README first.
#[test]
#[ignore]
fn pg180_differential() {
    let o = Command::new("docker")
        .args(["inspect", "dd68-pgtypes-oracle", "--format", "{{.Image}}"])
        .output()
        .unwrap();
    assert!(o.status.success());
    let pin = Command::new("docker")
        .args(["image", "inspect", IMAGE, "--format", "{{.Id}}"])
        .output()
        .unwrap();
    assert!(pin.status.success());
    assert_eq!(o.stdout, pin.stdout);
    assert_eq!(sql("show server_version_num"), "180000");
    let mut inputs = vec![
        "0".to_string(),
        "-0.00".into(),
        "1.2300".into(),
        "1.2300e2".into(),
        ".00001".into(),
        "NaN".into(),
        "Infinity".into(),
        "-Infinity".into(),
        "99999999999999999999999999999999999999999999999999.99999".into(),
    ];
    for i in 1..=100 {
        inputs.push(format!("-{}.{:06}e{}", i * 19373, i * 751, i % 21 - 10));
    }
    let rows = inputs
        .iter()
        .map(|s| format!("('{s}')"))
        .collect::<Vec<_>>()
        .join(",");
    let out = sql(&format!(
        "select x::numeric::text,encode(numeric_send(x::numeric),'hex') from (values {rows}) t(x)"
    ));
    for (s, row) in inputs.iter().zip(out.lines()) {
        let (text, b) = row.split_once('|').unwrap();
        let n = Numeric::parse(s).unwrap();
        assert_eq!(n.to_string(), text, "{s}");
        assert_eq!(hex(&n.to_binary()), b, "{s}");
    }
    let mut arithmetic_queries = Vec::new();
    let mut arithmetic_expected = Vec::new();
    for i in 1..=100 {
        let a = format!("-{}.0025", i * 917);
        let b = format!("{}.030", i * 113);
        let x = Numeric::parse(&a).unwrap();
        let y = Numeric::parse(&b).unwrap();
        for (op, value) in [
            ("+", x.checked_add(&y).unwrap()),
            ("-", x.checked_sub(&y).unwrap()),
            ("*", x.checked_mul(&y).unwrap()),
        ] {
            arithmetic_queries.push(format!("select ('{a}'::numeric {op} '{b}'::numeric)::text"));
            arithmetic_expected.push(value.to_string());
        }
    }
    let arithmetic_rows = sql(&arithmetic_queries.join(";"));
    assert_eq!(arithmetic_rows.lines().count(), 300);
    for (observed, expected) in arithmetic_rows.lines().zip(arithmetic_expected) {
        assert_eq!(observed, expected);
    }
    let mut queries = Vec::new();
    let mut expected = Vec::new();
    for a in [
        "-2.5",
        "1.2500",
        "999.99",
        "0.00005",
        "123456789012345678901234567890",
    ] {
        for scale in [-3, -1, 0, 1, 4] {
            queries.push(format!("select round('{a}'::numeric,{scale})::text"));
            expected.push(Numeric::parse(a).unwrap().round(scale).unwrap().to_string());
        }
    }
    for (q, e) in queries.iter().zip(expected) {
        assert_eq!(sql(q), e, "{q}")
    }
    for (ty, values) in [
        (
            "float4",
            vec![
                "0",
                "-0",
                "7.038531e-26",
                "1.17549435e-38",
                "1e-45",
                "3.4028234e38",
                "NaN",
                "Infinity",
                "-Infinity",
            ],
        ),
        (
            "float8",
            vec![
                "0",
                "-0",
                "1e-300",
                "5e-324",
                "2.2250738585072014e-308",
                "1.7976931348623157e308",
                "NaN",
                "Infinity",
                "-Infinity",
            ],
        ),
    ] {
        for s in values {
            let oracle = sql(&format!("select encode({ty}send('{s}'::{ty}),'hex')"));
            let bytes = if ty == "float4" {
                hex(&Float4::parse(s).unwrap().to_binary())
            } else {
                hex(&Float8::parse(s).unwrap().to_binary())
            };
            assert_eq!(bytes, oracle, "{ty} {s}");
            let text = if ty == "float4" {
                Float4::parse(s).unwrap().to_text()
            } else {
                Float8::parse(s).unwrap().to_text()
            };
            assert_eq!(text, sql(&format!("select '{s}'::{ty}::text")), "{ty} {s}");
        }
    }
    for (ty, s) in [
        ("float4", "1e39"),
        ("float4", "1e-100"),
        ("float8", "1e309"),
        ("float8", "1e-400"),
    ] {
        let o = Command::new("docker")
            .args([
                "exec",
                "dd68-pgtypes-oracle",
                "psql",
                "-U",
                "postgres",
                "-v",
                "ON_ERROR_STOP=1",
                "-Atqc",
                &format!("select '{s}'::{ty}"),
            ])
            .output()
            .unwrap();
        assert!(!o.status.success());
    }
    println!(
        "PG18.0 pinned:109 numeric text/binary,300 arithmetic,25 rounding,18 float text/binary,4 float range cases passed"
    );
}
