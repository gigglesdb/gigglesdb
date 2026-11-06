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
fn pg18_method_matrix() {
    for (i, row) in include_str!("fixtures/methods.tsv").lines().enumerate() {
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
fn generated_scalars_limits_and_immutable_inputs() {
    let l = PathLimits::default();
    let target = Jsonb::parse(
        r#"{"a":[[1,2],true,null],"type":7,"size":8}"#,
        Limits::default(),
    )
    .unwrap();
    let vars = Jsonb::parse("{}", Limits::default()).unwrap();
    let original = target.text().unwrap();
    for text in [
        "$.**.type()",
        "$.**.size()[0].type()",
        "$.** ? (@.size() == 1)",
    ] {
        let path = JsonPath::parse(text, l).unwrap();
        for silent in [false, true] {
            for limits in [
                PathLimits { work: 1, ..l },
                PathLimits { sequence: 1, ..l },
                PathLimits {
                    output_bytes: 0,
                    ..l
                },
            ] {
                assert!(
                    matches!(
                        jsonb_path_query(
                            Some(&target),
                            Some(&path),
                            Some(&vars),
                            Some(silent),
                            limits
                        ),
                        Err(PathError::Limit)
                    ),
                    "{text} {limits:?}"
                );
                assert!(
                    matches!(
                        jsonb_path_query_array(
                            Some(&target),
                            Some(&path),
                            Some(&vars),
                            Some(silent),
                            limits
                        ),
                        Err(PathError::Limit)
                    ),
                    "{text} {limits:?}"
                );
                assert!(
                    matches!(
                        jsonb_path_query_first(
                            Some(&target),
                            Some(&path),
                            Some(&vars),
                            Some(silent),
                            limits
                        ),
                        Err(PathError::Limit)
                    ),
                    "{text} {limits:?}"
                );
            }
        }
    }
    assert_eq!(target.text().unwrap(), original);
    for (path, expected) in [
        (r#"$."type""#, "7"),
        (r#"$."size""#, "8"),
        ("$.a.size().type()", r#""number""#),
        ("$.a.type()", r#""array""#),
    ] {
        let p = JsonPath::parse(path, l).unwrap();
        assert_eq!(
            jsonb_path_query_first(Some(&target), Some(&p), Some(&vars), Some(false), l)
                .unwrap()
                .unwrap()
                .text()
                .unwrap(),
            expected
        );
    }
    for text in [
        "$.abs()",
        "$.type(1)",
        "$.size(1)",
        "$.type().datetime()",
        "$.** ? (@.type() == \"number\" || @.abs() == 1)",
    ] {
        assert_eq!(
            JsonPath::parse(text, l).unwrap_err(),
            PathError::Unsupported
        );
    }
    let p = JsonPath::parse("strict $.size()", l).unwrap();
    assert_eq!(
        jsonb_path_exists(Some(&target), Some(&p), Some(&vars), Some(false), l),
        Err(PathError::Array)
    );
    assert_eq!(
        jsonb_path_exists(Some(&target), Some(&p), Some(&vars), Some(true), l),
        Ok(None)
    );
    let malformed_vars = Jsonb::parse("[]", Limits::default()).unwrap();
    assert_eq!(
        jsonb_path_exists(
            Some(&target),
            Some(&p),
            Some(&malformed_vars),
            Some(true),
            l
        ),
        Err(PathError::VariablesShape)
    );
    assert!(
        jsonb_path_query(None, Some(&p), Some(&vars), Some(false), l)
            .unwrap()
            .is_none()
    );
}
