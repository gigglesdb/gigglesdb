use giggles_glibm as m;
use std::path::Path;
type Unary = fn(f64) -> f64;
const UNARY: &[(&str, Unary)] = &[
    ("cbrt", m::cbrt),
    ("exp", m::exp),
    ("log", m::log),
    ("log10", m::log10),
    ("sinh", m::sinh),
    ("cosh", m::cosh),
    ("tanh", m::tanh),
    ("asinh", m::asinh),
    ("acosh", m::acosh),
    ("atanh", m::atanh),
    ("erf", m::erf),
    ("erfc", m::erfc),
    ("tgamma", m::tgamma),
    ("lgamma", m::lgamma),
];
const IBM: &[(&str, Unary)] = &[
    ("sin", m::sin),
    ("cos", m::cos),
    ("tan", m::tan),
    ("asin", m::asin),
    ("acos", m::acos),
    ("atan", m::atan),
];
fn names() -> Vec<&'static str> {
    let mut names: Vec<_> = UNARY.iter().map(|(n, _)| *n).chain(["pow"]).collect();
    names.extend(IBM.iter().map(|(n, _)| *n));
    names.push("atan2");
    names
}
fn run(name: &str, records: impl Iterator<Item = Vec<u64>>) -> usize {
    let mut failures = 0;
    let mut count = 0;
    for v in records {
        let actual = if name == "pow" {
            m::pow(f64::from_bits(v[0]), f64::from_bits(v[1]))
        } else if name == "atan2" {
            m::atan2(f64::from_bits(v[0]), f64::from_bits(v[1]))
        } else {
            let f = UNARY.iter().chain(IBM).find(|(n, _)| *n == name).unwrap().1;
            f(f64::from_bits(v[0]))
        }
        .to_bits();
        count += 1;
        if actual != *v.last().unwrap() {
            if failures < 3 {
                eprintln!(
                    "{name} input={:016x?} expected={:016x} got={actual:016x}",
                    &v[..v.len() - 1],
                    v.last().unwrap()
                );
            }
            failures += 1;
        }
    }
    eprintln!("{name}: {count} records, {failures} mismatches");
    failures
}
#[test]
fn fixtures() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let mut failures = 0;
    for name in names() {
        let data = std::fs::read_to_string(root.join(format!("{name}.txt"))).unwrap();
        failures += run(
            name,
            data.lines().map(|l| {
                l.split_whitespace()
                    .map(|x| u64::from_str_radix(x, 16).unwrap())
                    .collect()
            }),
        );
    }
    assert_eq!(failures, 0);
}
#[test]
fn sweep() {
    let Some(dir) = std::env::var_os("GIGGLES_GLIBM_SWEEP_DIR") else {
        eprintln!("skip sweep: GIGGLES_GLIBM_SWEEP_DIR unset");
        return;
    };
    let mut failures = 0;
    for name in names() {
        let data = std::fs::read(Path::new(&dir).join(format!("{name}.bin"))).unwrap();
        let size = if name == "pow" || name == "atan2" {
            24
        } else {
            16
        };
        assert_eq!(data.len() % size, 0);
        assert!(data.len() / size >= 1_000_000);
        failures += run(
            name,
            data.chunks_exact(size).map(|r| {
                r.as_chunks::<8>()
                    .0
                    .iter()
                    .map(|b| u64::from_le_bytes(*b))
                    .collect()
            }),
        );
    }
    assert_eq!(failures, 0);
}
#[test]
fn abi_identity() {
    assert_eq!(
        m::IDENTITY.to_bytes_with_nul(),
        b"giggles-glibm/1 glibc-2.41-12+deb13u4 x86_64-fma\0"
    );
    // The export points at the same static C string.
    assert_eq!(m::giggles_glibm_identity(), m::IDENTITY.as_ptr());
}
