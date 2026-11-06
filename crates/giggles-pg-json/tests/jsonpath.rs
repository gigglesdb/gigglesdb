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
fn pg18_navigation_matrix() {
    for (i, row) in include_str!("fixtures/jsonpath.tsv")
        .lines()
        .chain(include_str!("fixtures/predicates.tsv").lines())
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
fn budgets_and_atomic_inputs() {
    let limits = Limits::default();
    let l = PathLimits::default();
    let t = Jsonb::parse("[1,2,3]", limits).unwrap();
    let v = Jsonb::parse("{}", limits).unwrap();
    let p = JsonPath::parse("$[*]", l).unwrap();
    let original = t.text().unwrap();
    for silent in [false, true] {
        for cap in [
            PathLimits { sequence: 2, ..l },
            PathLimits { work: 1, ..l },
            PathLimits {
                output_bytes: 1,
                ..l
            },
        ] {
            assert!(matches!(
                jsonb_path_query(Some(&t), Some(&p), Some(&v), Some(silent), cap),
                Err(PathError::Limit)
            ));
        }
    }
    assert_eq!(t.text().unwrap(), original);
    let continuation = JsonPath::parse("$.**.a", l).unwrap();
    let nested = Jsonb::parse(r#"{"a":1,"b":{"a":2}}"#, limits).unwrap();
    let observed = jsonb_path_query(Some(&nested), Some(&continuation), Some(&v), Some(false), l)
        .unwrap()
        .unwrap()
        .into_iter()
        .map(|n| n.text().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(observed, ["1", "2"]);
    for path in ["$.datetime()", "$ ? (@.abs() == 1)"] {
        assert!(matches!(
            JsonPath::parse(path, l),
            Err(PathError::Unsupported)
        ));
    }
    let deep = format!("${}", "[0]".repeat(129));
    assert!(matches!(
        JsonPath::parse(
            &deep,
            PathLimits {
                steps: usize::MAX,
                ..l
            }
        ),
        Err(PathError::Limit)
    ));
    assert!(matches!(
        JsonPath::parse(
            "$",
            PathLimits {
                input_bytes: 0,
                ..l
            }
        ),
        Err(PathError::Limit)
    ));
    assert!(
        jsonb_path_query(None, Some(&p), Some(&v), Some(false), l)
            .unwrap()
            .is_none()
    );
    let n = Jsonb::parse("null", limits).unwrap();
    assert!(matches!(
        jsonb_path_query(Some(&t), Some(&p), Some(&n), Some(true), l),
        Err(PathError::VariablesShape)
    ));
    for i in 0..100 {
        let text = format!("[{},{}]", i, i + 1);
        let t = Jsonb::parse(&text, limits).unwrap();
        let a = jsonb_path_query_array(Some(&t), Some(&p), Some(&v), Some(false), l)
            .unwrap()
            .unwrap();
        assert_eq!(a.compare_c(&t), std::cmp::Ordering::Equal);
    }
}

#[test]
#[ignore = "requires owned pinned PostgreSQL JSONPath container"]
fn live_jsonpath_pg18() {
    let status = std::process::Command::new("python")
        .arg("tests/generate_jsonpath.py")
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .status()
        .unwrap();
    assert!(status.success());
}
#[test]
fn wide_deep_and_aggregate_admission() {
    let l = PathLimits::default();
    let limits = Limits::default();
    let v = Jsonb::parse("{}", limits).unwrap();
    let p = JsonPath::parse("$[*]", l).unwrap();
    let text = format!("[{}]", vec!["0"; 10001].join(","));
    let t = Jsonb::parse(&text, limits).unwrap();
    for silent in [false, true] {
        assert!(matches!(
            jsonb_path_query(Some(&t), Some(&p), Some(&v), Some(silent), l),
            Err(PathError::Limit)
        ));
    }
    assert_eq!(t.array_length().unwrap(), 10001);
    let small = Jsonb::parse("[0]", Limits { nodes: 2, ..limits }).unwrap();
    assert!(
        jsonb_path_query_array(
            Some(&small),
            Some(&JsonPath::parse("$", l).unwrap()),
            Some(&v),
            Some(false),
            l
        )
        .is_err()
    );
    let deep = format!("{}0{}", "[".repeat(129), "]".repeat(129));
    assert!(
        Jsonb::parse(
            &deep,
            Limits {
                depth: usize::MAX,
                ..limits
            }
        )
        .is_err()
    );
}

#[test]
fn predicate_properties_and_adversarial_limits() {
    let l = PathLimits::default();
    let limits = Limits::default();
    let vars = Jsonb::parse("{}", limits).unwrap();
    for i in 0..100 {
        let t = Jsonb::parse(&format!("[{}, {}, null, false, \"x\"]", i, i + 1), limits).unwrap();
        let p = JsonPath::parse(&format!("$ ? (@ >= {i} && @ != null)"), l).unwrap();
        let actual = jsonb_path_query_array(Some(&t), Some(&p), Some(&vars), Some(false), l)
            .unwrap()
            .unwrap();
        assert_eq!(actual.text().unwrap(), format!("[{}, {}]", i, i + 1));
    }
    let t = Jsonb::parse(&format!("[{}]", vec!["1"; 500].join(",")), limits).unwrap();
    let original = t.text().unwrap();
    let p = JsonPath::parse("strict $[*] == $[*]", l).unwrap();
    for silent in [false, true] {
        assert!(matches!(
            jsonb_path_query(
                Some(&t),
                Some(&p),
                Some(&vars),
                Some(silent),
                PathLimits { work: 5000, ..l }
            ),
            Err(PathError::Limit)
        ));
        let p = JsonPath::parse("$ ? (@ == 1)", l).unwrap();
        assert!(matches!(
            jsonb_path_query(
                Some(&t),
                Some(&p),
                Some(&vars),
                Some(silent),
                PathLimits { sequence: 10, ..l }
            ),
            Err(PathError::Limit)
        ));
    }
    assert_eq!(t.text().unwrap(), original);
    assert_eq!(vars.text().unwrap(), "{}");
    for expr in ["$ ? (@ == 1", "$ ? )", "$[0", "$ ? ((@ == 1])"] {
        assert!(matches!(JsonPath::parse(expr, l), Err(PathError::Syntax)));
    }
    for expr in [
        "$.abs()",
        "$ ? (@.datetime() == 1)",
        "$ like_regex \"a\"",
        "$ + 1",
        "$ ? (@ == 1 || @.abs() == 2)",
    ] {
        assert!(
            matches!(JsonPath::parse(expr, l), Err(PathError::Unsupported)),
            "{expr}"
        );
    }
    let previous = JsonPath::parse("strict $[last - 1]", l).unwrap();
    let array = Jsonb::parse("[1,2,3]", limits).unwrap();
    assert_eq!(
        jsonb_path_query_array(Some(&array), Some(&previous), Some(&vars), Some(false), l)
            .unwrap()
            .unwrap()
            .text()
            .unwrap(),
        "[2]"
    );
    let strict = JsonPath::parse("strict $.**{0}.a", l).unwrap();
    let array = Jsonb::parse("[{\"a\":1}]", Limits::default()).unwrap();
    assert_eq!(
        jsonb_path_query_array(Some(&array), Some(&strict), Some(&vars), Some(false), l)
            .unwrap()
            .unwrap()
            .text()
            .unwrap(),
        "[]"
    );
    let deep = format!("{}$ == 1{}", "(".repeat(10000), ")".repeat(10000));
    assert!(matches!(JsonPath::parse(&deep, l), Err(PathError::Limit)));
    let chain = std::iter::repeat_n("$ == 1", 1000)
        .collect::<Vec<_>>()
        .join(" || ");
    assert!(matches!(JsonPath::parse(&chain, l), Err(PathError::Limit)));
}
