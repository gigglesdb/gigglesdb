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
fn run(p: &[&str]) -> Result<Option<String>> {
    let a = if p[0] == "-" {
        None
    } else {
        Some(Jsonb::parse(&decode(p[0]), Limits::default())?)
    };
    let path: Option<Vec<Option<String>>> = if p[1] == "~" {
        None
    } else if p[1].is_empty() {
        Some(vec![])
    } else {
        Some(
            p[1].split(',')
                .map(|s| {
                    if s == "-" {
                        None
                    } else {
                        Some(decode(&s[1..]))
                    }
                })
                .collect(),
        )
    };
    let path_ref = path
        .as_ref()
        .map(|p| p.iter().map(|s| s.as_deref()).collect::<Vec<_>>());
    let v = if p[2] == "-" {
        None
    } else {
        Some(Jsonb::parse(&decode(p[2]), Limits::default())?)
    };
    let flag = if p[3] == "N" { None } else { Some(p[3] == "1") };
    let t = if p[5] == "-" {
        None
    } else {
        Some(decode(p[5]))
    };
    let r = if p[4] == "jsonb_set_lax" {
        Jsonb::set_lax(
            a.as_ref(),
            path_ref.as_deref(),
            v.as_ref(),
            flag,
            t.as_deref(),
        )?
    } else if let (Some(a), Some(path), Some(v), Some(f)) =
        (a.as_ref(), path_ref.as_deref(), v.as_ref(), flag)
    {
        Some(if p[4] == "jsonb_set" {
            a.set(path, v, f)?
        } else {
            a.insert(path, v, f)?
        })
    } else {
        None
    };
    r.map(|v| v.text()).transpose()
}
#[test]
fn pg18_mutation_matrix() {
    let file = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/mutation.tsv"
    ))
    .unwrap();
    for (i, line) in file.lines().enumerate() {
        let p: Vec<_> = line.split('\t').collect();
        let got = match run(&p) {
            Ok(Some(s)) => format!("OK:{}", encode(&s)),
            Ok(None) => "NULL".into(),
            Err(e) => format!("ERR:{}", e.sqlstate().unwrap_or("LIMIT")),
        };
        assert_eq!(got, p[6], "row {i}: {p:?}");
    }
}

#[test]
fn growth_boundaries_atomic_and_properties() {
    use giggles_pg_json::Error;
    let defaults = Limits::default();
    let v = Jsonb::parse("[1]", defaults).unwrap();
    for nodes in [3, 4] {
        let a = Jsonb::parse("[0]", Limits { nodes, ..defaults }).unwrap();
        let result = a.insert(&[Some("0")], &v, false);
        assert_eq!(result.is_ok(), nodes == 4); // inserted array adds two nodes: total four
        assert_eq!(a.text().unwrap(), "[0]");
    }
    let nested = Jsonb::parse("[[0]]", defaults).unwrap();
    let wide = Jsonb::parse(&format!("[{}]", vec!["0"; 1000].join(",")), defaults).unwrap();
    for output_bytes in [5, 6] {
        let a = Jsonb::parse(
            "[0]",
            Limits {
                output_bytes,
                ..defaults
            },
        )
        .unwrap();
        assert_eq!(
            a.insert(&[Some("99")], &Jsonb::parse("1", defaults).unwrap(), false)
                .is_ok(),
            output_bytes == 6
        );
        assert_eq!(a.text().unwrap(), "[0]");
    }
    let a = Jsonb::parse(
        "[0]",
        Limits {
            depth: 1,
            ..defaults
        },
    )
    .unwrap();
    assert_eq!(
        a.set(&[Some("0")], &nested, true).unwrap_err(),
        Error::Limit
    );
    assert_eq!(a.concat(&nested).unwrap_err(), Error::Limit);
    let a = Jsonb::parse(
        "[0]",
        Limits {
            nodes: 1001,
            ..defaults
        },
    )
    .unwrap();
    assert_eq!(a.concat(&wide).unwrap_err(), Error::Limit);
    assert_eq!(
        a.insert(&[Some("0")], &wide, false).unwrap_err(),
        Error::Limit
    );
    assert_eq!(a.text().unwrap(), "[0]");
    for i in 0..500 {
        let a = Jsonb::parse(&format!("{{\"a\":[{i},null]}}"), defaults).unwrap();
        let value = Jsonb::parse(&format!("{i}.00"), defaults).unwrap();
        let r = a.set(&[Some("a"), Some("-1")], &value, true).unwrap();
        assert!(
            r.path(&[Some("a"), Some("1")])
                .unwrap()
                .unwrap()
                .text()
                .unwrap()
                .ends_with(".00")
        );
        assert!(
            r.delete_path(&[Some("a"), Some("1")])
                .unwrap()
                .sql_eq(&a.delete_path(&[Some("a"), Some("1")]).unwrap())
        );
        let unchanged = a.set(&[Some("absent"), None], &value, true).unwrap();
        assert!(unchanged.sql_eq(&a));
    }
    let deep = Jsonb::parse(&("[".repeat(64) + "0" + &"]".repeat(64)), defaults).unwrap();
    assert!(deep.contains(&deep));
    assert_eq!(
        deep.insert(&[Some("0")], &deep, false).unwrap_err(),
        Error::Limit
    );
    let scalar = Jsonb::parse("0", defaults).unwrap();
    assert!(
        Jsonb::set_lax(
            Some(&scalar),
            Some(&[None]),
            None,
            Some(true),
            Some("return_target")
        )
        .unwrap()
        .unwrap()
        .sql_eq(&scalar)
    );
    assert_eq!(
        Jsonb::set_lax(
            Some(&scalar),
            Some(&[None]),
            None,
            Some(true),
            Some("raise_exception")
        )
        .unwrap_err(),
        Error::NullPath
    );
    assert_eq!(
        Jsonb::set_lax(Some(&scalar), Some(&[None]), Some(&v), Some(true), None).unwrap_err(),
        Error::Shape
    );
}

#[test]
#[ignore = "requires the owned pinned PG18 mutation container"]
fn live_mutation_pg18() {
    assert!(
        std::process::Command::new("python")
            .arg(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/generate_mutation.py"
            ))
            .status()
            .unwrap()
            .success()
    );
    pg18_mutation_matrix();
}
