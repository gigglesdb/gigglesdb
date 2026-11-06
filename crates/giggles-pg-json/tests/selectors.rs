use giggles_pg_json::{Jsonb, Limits, jsonpath::*};
fn dec(s: &str) -> String {
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
fn enc(s: &str) -> String {
    s.bytes().map(|b| format!("{b:02x}")).collect()
}
#[test]
fn pg18_selector_matrix() {
    for (i, row) in include_str!("fixtures/selectors.tsv")
        .lines()
        .chain(include_str!("fixtures/selector-contexts.tsv").lines())
        .enumerate()
    {
        let c: Vec<_> = row.split('\t').collect();
        let l = PathLimits::default();
        let t = if c[0] == "-" {
            None
        } else {
            Some(Jsonb::parse(&dec(c[0]), Limits::default()).unwrap())
        };
        let p = JsonPath::parse(&dec(c[1]), l).unwrap();
        let v = Jsonb::parse(&dec(c[2]), Limits::default()).unwrap();
        let s = Some(c[3] == "1");
        let result = match c[4] {
            "query" => jsonb_path_query(t.as_ref(), Some(&p), Some(&v), s, l).map(|a| {
                a.filter(|a| !a.is_empty()).map(|a| {
                    format!(
                        "[{}]",
                        a.iter()
                            .map(|v| v.text().unwrap())
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                })
            }),
            "query_array" => jsonb_path_query_array(t.as_ref(), Some(&p), Some(&v), s, l)
                .map(|a| a.map(|a| a.text().unwrap())),
            "query_first" => jsonb_path_query_first(t.as_ref(), Some(&p), Some(&v), s, l)
                .map(|a| a.map(|a| a.text().unwrap())),
            "exists" => jsonb_path_exists(t.as_ref(), Some(&p), Some(&v), s, l)
                .map(|a| a.map(|a| a.to_string())),
            "match" => jsonb_path_match(t.as_ref(), Some(&p), Some(&v), s, l)
                .map(|a| a.map(|a| a.to_string())),
            _ => unreachable!(),
        };
        let actual = match result {
            Ok(None) => "NULL".into(),
            Ok(Some(s)) => format!("OK:{}", enc(&s)),
            Err(e) => format!(
                "ERR:{}",
                match e {
                    PathError::Structural => "2203A",
                    PathError::Object => "2203C",
                    PathError::Array => "22039",
                    PathError::Subscript => "22033",
                    PathError::Variable => "42704",
                    PathError::Predicate => "22038",
                    _ => panic!("unexpected {e:?}"),
                }
            ),
        };
        // Structural errors have separate PostgreSQL SQLSTATEs for member/type/subscript failures.
        assert!(
            actual == c[5],
            "row {i}: {} {} {} {} expected {} actual {}",
            dec(c[0].replace('-', "6e756c6c").as_str()),
            dec(c[1]),
            c[3],
            c[4],
            c[5],
            actual
        );
    }
}

#[test]
fn selector_limits_duplicates_and_immutable_inputs() {
    let l = PathLimits::default();
    let t = Jsonb::parse("[0,1,2]", Limits::default()).unwrap();
    let vars = Jsonb::parse("{}", Limits::default()).unwrap();
    let original = t.text().unwrap();
    let duplicate = JsonPath::parse("$[2,0,2,last-1,0 to 1]", l).unwrap();
    assert_eq!(
        jsonb_path_query_array(Some(&t), Some(&duplicate), Some(&vars), Some(false), l)
            .unwrap()
            .unwrap()
            .text()
            .unwrap(),
        "[2, 0, 2, 1, 0, 1]"
    );
    let many = format!(
        "$[{}]",
        std::iter::repeat_n("0", 128).collect::<Vec<_>>().join(",")
    );
    let p = JsonPath::parse(&many, l).unwrap();
    assert_eq!(
        jsonb_path_query(Some(&t), Some(&p), Some(&vars), Some(false), l)
            .unwrap()
            .unwrap()
            .len(),
        128
    );
    assert!(matches!(
        JsonPath::parse(&many.replace(']', ",0]"), l),
        Err(PathError::Limit)
    ));
    for text in ["$[0,0]", "$.**[0,last].**[last-1,last]", "$.size()[0,0]"] {
        let p = JsonPath::parse(text, l).unwrap();
        for silent in [false, true] {
            for cap in [
                PathLimits { sequence: 1, ..l },
                PathLimits { work: 1, ..l },
                PathLimits {
                    output_bytes: 0,
                    ..l
                },
                PathLimits { steps: 0, ..l },
            ] {
                assert!(
                    matches!(
                        jsonb_path_query(Some(&t), Some(&p), Some(&vars), Some(silent), cap),
                        Err(PathError::Limit)
                    ),
                    "{text} {cap:?}"
                );
            }
        }
    }
    for cap in [
        PathLimits { steps: 1, ..l },
        PathLimits { work: 1, ..l },
        PathLimits { sequence: 1, ..l },
    ] {
        assert!(matches!(
            JsonPath::parse("$[0,0]", cap),
            Err(PathError::Limit)
        ));
        assert_eq!(
            jsonb_path_exists(Some(&t), Some(&duplicate), Some(&vars), Some(true), cap),
            Err(PathError::Limit)
        );
    }
    for text in [
        "$[1+1]",
        "$[last*2]",
        "$[last+-1]",
        "$[$v]",
        "$[abs(1)]",
        "$[(1)]",
        "$[last-last]",
        "$[last+1+1]",
        "$[0,1.5]",
        "$[18446744073709551616]",
        "$[last+18446744073709551616]",
        "$ ? (@ == 1 || exists(@[last*2]))",
    ] {
        assert_eq!(
            JsonPath::parse(text, l).unwrap_err(),
            PathError::Unsupported,
            "{text}"
        );
    }
    for text in ["$[]", "$[0,]", "$[,0]", "$[0,,1]"] {
        assert_eq!(
            JsonPath::parse(text, l).unwrap_err(),
            PathError::Syntax,
            "{text}"
        );
    }
    assert_eq!(t.text().unwrap(), original);
    let p = JsonPath::parse("strict $[0,2147483648,1]", l).unwrap();
    assert_eq!(
        jsonb_path_query_array(Some(&t), Some(&p), Some(&vars), Some(true), l)
            .unwrap()
            .unwrap()
            .text()
            .unwrap(),
        "[0]"
    );
    assert!(matches!(
        jsonb_path_query(Some(&t), Some(&p), Some(&vars), Some(false), l),
        Err(PathError::Subscript)
    ));
    let p = JsonPath::parse("lax $[0,2147483648]", l).unwrap();
    assert_eq!(
        jsonb_path_exists(Some(&t), Some(&p), Some(&vars), Some(false), l),
        Ok(Some(true))
    );
    let p = JsonPath::parse("strict $[0,2147483648]", l).unwrap();
    assert_eq!(
        jsonb_path_exists(Some(&t), Some(&p), Some(&vars), Some(false), l),
        Err(PathError::Subscript)
    );
}
