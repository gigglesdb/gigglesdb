use giggles_pg_json::{Error, Json, Jsonb, Limits, Result};
fn unhex(s: &str) -> Vec<u8> {
    s.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|v| u8::from_str_radix(std::str::from_utf8(v).unwrap(), 16).unwrap())
        .collect()
}
fn hex(s: &[u8]) -> String {
    s.iter().map(|b| format!("{b:02x}")).collect()
}
fn run(t: &str, s: &str, op: &str) -> Result<Option<String>> {
    let l = Limits::default();
    if t == "json" {
        let v = Json::parse(s, l)?;
        Ok(match op {
            "text" => Some(v.text()?.to_owned()),
            "wire" => Some(hex(&v.send()?)),
            "type" => Some(v.typeof_().into()),
            "length" => Some(v.array_length()?.to_string()),
            "keys" => {
                let k = v.object_keys()?;
                if k.is_empty() {
                    None
                } else {
                    Some(
                        k.iter()
                            .map(|k| hex(k.as_bytes()))
                            .collect::<Vec<_>>()
                            .join(","),
                    )
                }
            }
            "field" => v
                .field("a")?
                .map(|v| v.text().map(str::to_owned))
                .transpose()?,
            "fieldtext" => v.field_text("a")?,
            "element" => v
                .element(-1)?
                .map(|v| v.text().map(str::to_owned))
                .transpose()?,
            "elementtext" => v.element_text(-1)?,
            _ => panic!(),
        })
    } else {
        let v = Jsonb::parse(s, l)?;
        Ok(match op {
            "text" => Some(v.text()?),
            "wire" => Some(hex(&v.send()?)),
            "type" => Some(v.typeof_().into()),
            "length" => Some(v.array_length()?.to_string()),
            "keys" => {
                let k = v.object_keys()?;
                if k.is_empty() {
                    None
                } else {
                    Some(
                        k.iter()
                            .map(|k| hex(k.as_bytes()))
                            .collect::<Vec<_>>()
                            .join(","),
                    )
                }
            }
            "field" => v.field("a").map(|v| v.text()).transpose()?,
            "fieldtext" => v.field_text("a")?,
            "element" => v.element(-1).map(|v| v.text()).transpose()?,
            "elementtext" => v.element_text(-1)?,
            _ => panic!(),
        })
    }
}
#[test]
fn pg18_durable_oracle() {
    let fixture = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/oracle.tsv"
    ))
    .unwrap();
    let mut failures = Vec::new();
    let mut count = 0;
    for (i, row) in fixture.lines().enumerate() {
        let a: Vec<_> = row.split('\t').collect();
        let bytes = unhex(a[1]);
        let s = std::str::from_utf8(&bytes).unwrap();
        let got = run(a[0], s, a[2]);
        let pass = match (&got, a[3]) {
            (Ok(None), "NULL") => true,
            (Ok(Some(v)), expected) => expected == format!("OK:{}", hex(v.as_bytes())),
            (Err(e), expected) => e
                .sqlstate()
                .is_some_and(|state| expected == format!("ERR:{state}")),
            _ => false,
        };
        if !pass {
            failures.push(format!(
                "{} {} {} {:?}: got {:?}, expected {}",
                i, a[0], a[2], s, got, a[3]
            ));
        }
        count += 1;
    }
    assert!(count > 2000);
    assert!(
        failures.is_empty(),
        "{} failures:\n{}",
        failures.len(),
        failures
            .iter()
            .take(35)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n")
    );
}
#[test]
fn bounds_and_roundtrips() {
    let l = Limits::default();
    for n in 0..64 {
        let s = format!("{}1.00{}", "[".repeat(n), "]".repeat(n));
        let b = Jsonb::parse(&s, l).unwrap();
        let wire = b.send().unwrap();
        assert_eq!(
            Jsonb::receive(&wire, l).unwrap().text().unwrap(),
            b.text().unwrap()
        );
        assert_eq!(
            Json::receive(s.as_bytes(), l).unwrap().send().unwrap(),
            s.as_bytes()
        );
    }
    assert_eq!(
        Jsonb::receive(&[2, b'0'], l).unwrap_err(),
        Error::BinaryVersion
    );
    for s in ["[0,1]", "{\"a\":1}", "\"abc\""] {
        assert_eq!(
            Json::parse(
                s,
                Limits {
                    input_bytes: 1,
                    ..l
                }
            )
            .unwrap_err(),
            Error::Limit
        );
    }
    assert_eq!(Json::parse(&"[".repeat(200), l).unwrap_err(), Error::Limit);
    assert_eq!(
        Json::parse("[0,1]", Limits { nodes: 2, ..l }).unwrap_err(),
        Error::Limit
    );
    assert_eq!(
        Jsonb::parse(
            "[1,2]",
            Limits {
                output_bytes: 1,
                ..l
            }
        )
        .unwrap()
        .text()
        .unwrap_err(),
        Error::Limit
    );
    assert_eq!(
        Json::parse("[null]", l).unwrap().element_text(-1).unwrap(),
        None
    );
    assert!(
        Json::parse("[null]", l)
            .unwrap()
            .element(-1)
            .unwrap()
            .is_some()
    );
}
#[test]
fn numeric_scale_and_indices() {
    let l = Limits::default();
    let b = Jsonb::parse("1.00", l).unwrap();
    assert_eq!(b.numeric().unwrap().unwrap().scale(), Some(2));
    assert_eq!(b.element(0).unwrap().text().unwrap(), "1.00");
    assert!(b.element(1).is_none());
    let a = Jsonb::parse("[0,1,null]", l).unwrap();
    for (i, e) in [(-3, "0"), (-2, "1"), (-1, "null"), (0, "0"), (2, "null")] {
        assert_eq!(a.element(i).unwrap().text().unwrap(), e);
    }
    for i in [i32::MIN, -4, 3, i32::MAX] {
        assert!(a.element(i).is_none());
    }
    assert_eq!(
        Jsonb::receive(&[1, 0xff], l).unwrap_err().sqlstate(),
        Some("22021")
    );
}
#[test]
#[ignore = "requires owned pinned PostgreSQL container; regenerates durable fixtures"]
fn live_pg18() {
    let result = std::process::Command::new("python")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/generate_oracle.py"
        ))
        .status()
        .unwrap();
    assert!(result.success());
    pg18_durable_oracle();
}
