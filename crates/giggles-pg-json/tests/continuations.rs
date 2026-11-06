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
fn pg18_continuation_matrix() {
    for (i, row) in include_str!("fixtures/continuations.tsv")
        .lines()
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
fn contextual_limits_order_and_strictness() {
    let l = PathLimits::default();
    let t = Jsonb::parse(
        r#"[{"a":[1,2]}, {"a":{"a":true}}, null]"#,
        Limits::default(),
    )
    .unwrap();
    let v = Jsonb::parse("{}", Limits::default()).unwrap();
    let before = t.text().unwrap();
    for path in [
        "strict $.**.a",
        "lax $.**.**.a",
        "strict $.** ? (exists(@.**.a))",
    ] {
        let p = JsonPath::parse(path, l).unwrap();
        for silent in [false, true] {
            for cap in [
                PathLimits { work: 2, ..l },
                PathLimits { sequence: 1, ..l },
                PathLimits {
                    output_bytes: 1,
                    ..l
                },
            ] {
                assert!(
                    matches!(
                        jsonb_path_query(Some(&t), Some(&p), Some(&v), Some(silent), cap),
                        Err(PathError::Limit)
                    ),
                    "{path} {cap:?}"
                );
            }
        }
    }
    assert_eq!(t.text().unwrap(), before);
    let array = Jsonb::parse("[1,2]", Limits::default()).unwrap();
    for mode in ["lax", "strict"] {
        let p = JsonPath::parse(&format!("{mode} $.** ? (@ == 1 || @ == $missing)"), l).unwrap();
        for silent in [false, true] {
            let got = jsonb_path_exists(Some(&array), Some(&p), Some(&v), Some(silent), l);
            if mode == "lax" {
                assert_eq!(got, Ok(Some(true)));
            } else {
                assert_eq!(got, Err(PathError::Variable));
            }
        }
        let p = JsonPath::parse(&format!("{mode} $.** ? (@ == $missing || @ == 1)"), l).unwrap();
        assert_eq!(
            jsonb_path_exists(Some(&array), Some(&p), Some(&v), Some(true), l),
            Err(PathError::Variable)
        );
    }
    let p = JsonPath::parse("lax $.** ? (@ == 1 || @ == $missing)", l).unwrap();
    assert_eq!(
        jsonb_path_exists(
            Some(&array),
            Some(&p),
            Some(&v),
            Some(true),
            PathLimits { work: 1, ..l }
        ),
        Err(PathError::Limit)
    );
    // Descent suppresses errors, but strict mode still does not unwrap arrays.
    let p = JsonPath::parse("strict $.**{0}.a", l).unwrap();
    assert!(
        jsonb_path_query(Some(&t), Some(&p), Some(&v), Some(false), l)
            .unwrap()
            .unwrap()
            .is_empty()
    );
    for path in [
        "$.**.abs()",
        "$.** ? (@.datetime() == 1)",
        "exists($.**.a) || $.**.abs() == 1",
    ] {
        assert_eq!(
            JsonPath::parse(path, l).unwrap_err(),
            PathError::Unsupported
        );
    }
}

#[test]
fn qualified_selector_list_continuation() {
    let l = PathLimits::default();
    let t = Jsonb::parse("[1,2]", Limits::default()).unwrap();
    let vars = Jsonb::parse("{}", Limits::default()).unwrap();
    let path = JsonPath::parse("strict $.**[0,1]", l).unwrap();
    assert_eq!(
        jsonb_path_query_array(Some(&t), Some(&path), Some(&vars), Some(false), l)
            .unwrap()
            .unwrap()
            .text()
            .unwrap(),
        "[1, 2]"
    );
}
