use giggles_pg_json::{Jsonb, Limits, Result};
fn decode(s: &str) -> String {
    String::from_utf8(
        s.as_bytes()
            .as_chunks::<2>()
            .0
            .iter()
            .map(|b| u8::from_str_radix(std::str::from_utf8(b).unwrap(), 16).unwrap())
            .collect(),
    )
    .unwrap()
}
fn encode(s: &str) -> String {
    s.as_bytes().iter().map(|b| format!("{b:02x}")).collect()
}
fn run(a: &str, b: &str, op: &str) -> Result<Option<String>> {
    let a = Jsonb::parse(a, Limits::default())?;
    let boolean = match op {
        "eq" | "contains" | "contained" | "less" => {
            let b = Jsonb::parse(b, Limits::default())?;
            Some(match op {
                "eq" => a.sql_eq(&b),
                "less" => a.compare_c(&b).is_lt(),
                "contains" => a.contains(&b),
                _ => a.contained_by(&b),
            })
        }
        "exists" => Some(a.exists("a")),
        "any" => Some(a.exists_any(&[Some("a"), None, Some("\u{e9}")])),
        "all" => Some(a.exists_all(&[Some("a"), None, Some("\u{e9}")])),
        _ => None,
    };
    if let Some(b) = boolean {
        return Ok(Some(b.to_string()));
    }
    match op {
        "concatwire" => Ok(Some(
            a.concat(&Jsonb::parse(b, Limits::default())?)?
                .send()?
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>(),
        )),
        "concat" => a
            .concat(&Jsonb::parse(b, Limits::default())?)?
            .text()
            .map(Some),
        "strip" | "striparrays" => a.strip_nulls(op == "striparrays")?.text().map(Some),
        "deletepath" => a.delete_path(&[Some("a"), Some("-1")])?.text().map(Some),
        "nullpath" => a.delete_path(&[None])?.text().map(Some),
        "badpath" => a.delete_path(&[Some("x")])?.text().map(Some),
        "deletekey" => a.delete_key("a")?.text().map(Some),
        "deleteindex" => a.delete_index(-1)?.text().map(Some),
        "path" => a
            .path(&[Some("a"), Some("-1")])?
            .map(|v| v.text())
            .transpose(),
        "pathtext" => a.path_text(&[Some("a"), Some("-1")]),
        "emptytext" => a.path_text(&[]),
        _ => panic!("unknown operation"),
    }
}
#[test]
fn pg18_operator_matrix() {
    let file = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/operators.tsv"
    ))
    .unwrap();
    for (i, line) in file.lines().enumerate() {
        let p: Vec<_> = line.split('\t').collect();
        let a = decode(p[0]);
        let b = decode(p[1]);
        let got = match run(&a, &b, p[2]) {
            Ok(Some(s)) => format!("OK:{}", encode(&s)),
            Ok(None) => "NULL".into(),
            Err(e) => format!("ERR:{}", e.sqlstate().unwrap_or("LIMIT")),
        };
        assert_eq!(got, p[3], "row {i}: {a} {} {b}", p[2]);
    }
}
#[test]
fn bounded_and_atomic() {
    let l = Limits {
        output_bytes: 2,
        ..Limits::default()
    };
    let v = Jsonb::parse("[1]", l).unwrap();
    assert!(v.delete_index(0).is_err());
    assert_eq!(v.array_length().unwrap(), 1);
    let v = Jsonb::parse("{\"a\":null}", Limits::default()).unwrap();
    assert!(v.path(&[None]).unwrap().is_none());
    assert!(v.path(&vec![Some("a"); 129]).is_err());
    assert_eq!(v.path(&[Some("a")]).unwrap().unwrap().typeof_(), "null");
    let a = Jsonb::parse("1.00", Limits::default()).unwrap();
    let b = Jsonb::parse("1", Limits::default()).unwrap();
    assert!(a.sql_eq(&b));
    assert_eq!(a.text().unwrap(), "1.00");
}
#[test]
fn operator_properties_and_resource_bounds() {
    let l = Limits::default();
    for i in 0..200 {
        let a = Jsonb::parse(&format!("{{\"a\":{i}.00,\"n\":null,\"v\":[null,{i}]}}"), l).unwrap();
        let b = Jsonb::parse(&format!("{{\"a\":{i}}}"), l).unwrap();
        assert!(a.contains(&a));
        assert!(a.contains(&b));
        assert!(b.contained_by(&a));
        let stripped = a.strip_nulls(true).unwrap();
        assert!(a.contains(&stripped));
        assert!(stripped.sql_eq(&stripped.strip_nulls(true).unwrap()));
        assert_eq!(a.compare_c(&b), b.compare_c(&a).reverse());
        let c = a.concat(&b).unwrap();
        assert!(c.contains(&b));
        assert!(c.sql_eq(&Jsonb::receive(&c.send().unwrap(), l).unwrap()));
    }
    for limits in [
        Limits { nodes: 4, ..l },
        Limits { depth: 1, ..l },
        Limits {
            output_bytes: 5,
            ..l
        },
    ] {
        let a = Jsonb::parse("[0]", limits).unwrap();
        let b = Jsonb::parse("[[1,2,3]]", l).unwrap();
        assert!(a.concat(&b).is_err());
        assert_eq!(a.text().unwrap(), "[0]");
    }
    let a = Jsonb::parse(
        "[]",
        Limits {
            input_bytes: 4,
            ..l
        },
    )
    .unwrap();
    assert!(a.path(&[Some("xxxxx")]).is_err());
    let a = Jsonb::parse("[1,2]", l).unwrap();
    assert_eq!(
        a.path(&[Some(" -1")]).unwrap().unwrap().text().unwrap(),
        "2"
    );
    for s in ["1 ", "2147483648", "-2147483649", ""] {
        assert!(a.path(&[Some(s)]).unwrap().is_none());
    }
}
