use giggles_language::{
    DurationLiteral, Error, Intent, LanguageVersion, MAX_DEPTH, MAX_INPUT, MAX_TOKENS, OnceBody,
    OnceEnvelope, OutcomeQuery, ParsedInput, Statement, TransactionStep, Value, ValueExpr, parse,
    parse_input_with_version, parse_once_with_version, parse_outcome_with_version,
    parse_transaction_with_version, parse_with_version,
};

const VERSION: LanguageVersion = LanguageVersion::Embedded011;
const OLD_VERSIONS: [LanguageVersion; 10] = [
    LanguageVersion::Embedded01,
    LanguageVersion::Embedded02,
    LanguageVersion::Embedded03,
    LanguageVersion::Embedded04,
    LanguageVersion::Embedded05,
    LanguageVersion::Embedded06,
    LanguageVersion::Embedded07,
    LanguageVersion::Embedded08,
    LanguageVersion::Embedded09,
    LanguageVersion::Embedded010,
];

fn write(retention: &str) -> String {
    format!("update items where id=1 set n=-1 once $key for {retention} return id")
}

#[test]
fn once_parser_accepts_reference_clause_order_and_exact_duration_units() {
    for (literal, seconds) in [
        ("1000000us", 1),
        ("1000ms", 1),
        ("1s", 1),
        ("15m", 900),
        ("24h", 86400),
        ("7d", 604800),
        ("duration(\"24h\")", 86400),
        ("DURATION(\"1000ms\")", 1),
    ] {
        let text = write(literal);
        let parsed = parse_once_with_version(&text, VERSION).unwrap();
        assert_eq!(parsed.language_version, VERSION);
        assert_eq!(parsed.key, ValueExpr::Parameter("key".into()));
        assert_eq!(
            parsed.retention,
            DurationLiteral {
                microseconds: seconds * 1_000_000
            }
        );
        let OnceBody::Statement(Statement::Returning {
            statement,
            projection,
        }) = &parsed.body
        else {
            panic!("outer RETURN remains in the keyed statement: {parsed:?}");
        };
        assert_eq!(projection.len(), 1);
        assert!(
            matches!(statement.as_ref(), Statement::Update { values, intent: Intent::One, .. }
            if values == &vec![("n".into(), ValueExpr::Literal(Value::Int(-1)))])
        );
        assert_eq!(
            parse_input_with_version(&text, VERSION).unwrap(),
            ParsedInput::Once(parsed)
        );
    }
    let parsed = parse_once_with_version(
        "insert many into items(id,n) values(2,4),(1,3) on conflict(id) do update set n=incoming.n at most 2 once \"batch\" for 1h return id,n;",
        VERSION,
    ).unwrap();
    let OnceBody::Statement(Statement::Returning { statement, .. }) = parsed.body else {
        panic!("RETURN expected");
    };
    let Statement::OnConflict { statement, .. } = *statement else {
        panic!("conflict expected");
    };
    assert!(matches!(*statement, Statement::InsertMany { rows, .. } if rows.len()==2));

    for text in [
        "insert into items set id=1,n=2 once \"legacy\" for 1s",
        "delete items where id=1 once \"delete\" for 1s return id",
        "delete many items where n=0 at most 2 once \"delete_many\" for 1s",
        "claim items where n=0 order by id set n=1 once \"claim\" for 1s return id",
    ] {
        assert!(parse_once_with_version(text, VERSION).is_ok(), "{text}");
    }
    let transaction = parse_once_with_version(
        "transaction once $request for 24h { let changed=(update items where id=$id set n=2 return id); emit @changed.row.id; }",
        VERSION,
    ).unwrap();
    let OnceBody::Transaction(body) = transaction.body else {
        panic!("transaction expected");
    };
    assert!(!body.read_only);
    assert!(matches!(&body.steps[..], [TransactionStep::Let { .. }]));
    assert_eq!(body.emit.unwrap().len(), 1);
    assert_eq!(
        parse_outcome_with_version("outcome $request;", VERSION).unwrap(),
        OutcomeQuery {
            language_version: VERSION,
            key: ValueExpr::Parameter("request".into()),
        }
    );
}

#[test]
fn once_parser_preserves_old_profiles_and_contextual_names() {
    let standalone = write("24h");
    let block = "transaction once $key for 24h { update items where id=1 set n=1 }";
    for version in OLD_VERSIONS {
        for text in [standalone.as_str(), block, "outcome $key"] {
            assert!(
                parse_input_with_version(text, version).is_err(),
                "{version:?}: {text}"
            );
        }
        assert_eq!(
            parse_once_with_version(&standalone, version),
            Err(Error::Unsupported)
        );
        assert_eq!(
            parse_outcome_with_version("outcome $key", version),
            Err(Error::Unsupported)
        );
        for text in [
            "outcome",
            "outcome where id=1 select id",
            "transaction where id=1",
            "for select for",
            "duration select duration",
        ] {
            assert!(
                parse_with_version(text, version).is_ok(),
                "{version:?}: {text}"
            );
            assert!(
                matches!(
                    parse_input_with_version(text, VERSION),
                    Ok(ParsedInput::Statement(_))
                ),
                "{text}"
            );
        }
    }
    assert!(parse(&standalone).is_err());
    for text in [
        "`once` select `once`",
        "outcome select for",
        "items where n=1and true",
        "transaction { let outcome=(items select id); emit @outcome.count }",
    ] {
        assert!(parse_input_with_version(text, VERSION).is_ok(), "{text}");
    }
    let escaped = parse_once_with_version(
        "update items where id=1 set n=2 once \"line\\nquote\\\"slash\\\\\" for 1s",
        VERSION,
    )
    .unwrap();
    assert_eq!(
        escaped.key,
        ValueExpr::Literal(Value::Text("line\nquote\"slash\\".into()))
    );
    let query = parse_outcome_with_version("OuTcOmE \"For\"", VERSION).unwrap();
    assert_eq!(query.key, ValueExpr::Literal(Value::Text("For".into())));
}

#[test]
fn once_parser_rejects_misplaced_or_context_free_envelopes() {
    let standalone = write("24h");
    let block = "transaction once $key for 24h { update items where id=1 set n=1 }";
    // These APIs return only unkeyed trees and must never strip a header.
    assert!(parse_with_version(&standalone, VERSION).is_err());
    assert!(parse_transaction_with_version(block, VERSION).is_err());
    assert!(parse_with_version("outcome $key", VERSION).is_err());
    for text in [
        "items once \"read\" for 1s",
        "transaction read only once \"read\" for 1s { items }",
        "create table t(id int64,primary key(id)) once \"ddl\" for 1s",
        "update items where id=1 set n=1 return id once \"late\" for 1s",
        "update many items where n=0 set n=1 once \"early\" for 1s at most 1",
        "update items where id=1 set n=1 once \"a\" for 1s once \"b\" for 1s",
        "transaction { update items where id=1 set n=1 once \"inner\" for 1s }",
        "transaction once \"outer\" for 1s { let x=(update items where id=1 set n=1 once \"inner\" for 1s) }",
        "transaction once \"outer\" for 1s { transaction { items } }",
        "transaction once \"outer\" for 1s { claim items where n=0 order by id set n=1 }",
        "outcome \"key\" return id",
        "outcome \"key\"; update items where id=1 set n=1",
    ] {
        assert!(parse_input_with_version(text, VERSION).is_err(), "{text}");
    }
    for key in ["null", "true", "42"] {
        assert_eq!(
            parse_once_with_version(
                &format!("update items where id=1 set n=1 once {key} for 1s"),
                VERSION
            ),
            Err(Error::Type)
        );
        assert_eq!(
            parse_outcome_with_version(&format!("outcome {key}"), VERSION),
            Err(Error::Type)
        );
    }
    for duration in [
        "$retention",
        "24H",
        "1MS",
        "duration(\"24H\")",
        "duration(\"1month\")",
    ] {
        assert_eq!(
            parse_once_with_version(&write(duration), VERSION),
            Err(Error::Unsupported),
            "{duration}"
        );
    }
    for duration in [
        "24",
        "\"24h\"",
        "250ms",
        "1us",
        "0s",
        "-1s",
        "1.5s",
        "1e3s",
        "24 h",
        "duration($retention)",
        "duration(\"1 h\")",
        "1s+1s",
    ] {
        assert!(
            parse_once_with_version(&write(duration), VERSION).is_err(),
            "{duration}"
        );
    }
    assert!(
        parse_with_version("items where n=1s", VERSION).is_err(),
        "retention literals do not introduce a general duration scalar"
    );
}

#[test]
fn once_parser_enforces_input_and_duration_bounds() {
    for literal in ["604800s", "604800000ms", "604800000000us", "168h", "7d"] {
        assert!(
            parse_once_with_version(&write(literal), VERSION).is_ok(),
            "{literal}"
        );
    }
    for literal in ["604801s", "8d", "duration(\"604801s\")"] {
        assert_eq!(
            parse_once_with_version(&write(literal), VERSION),
            Err(Error::Limit)
        );
    }
    for literal in [
        "9223372036854775808us",
        "9223372036854775807d",
        "duration(\"9223372036854775808s\")",
    ] {
        assert_eq!(
            parse_once_with_version(&write(literal), VERSION),
            Err(Error::Overflow)
        );
    }
    let text = |key: &str| format!("update items where id=1 set n=1 once \"{key}\" for 1s");
    assert!(parse_once_with_version(&text(&"é".repeat(128)), VERSION).is_ok());
    assert_eq!(
        parse_once_with_version(&text(&"é".repeat(129)), VERSION),
        Err(Error::Limit)
    );
    assert_eq!(
        parse_outcome_with_version(&format!("outcome \"{}\"", "é".repeat(129)), VERSION),
        Err(Error::Limit)
    );
    assert_eq!(
        parse_once_with_version(&text(""), VERSION),
        Err(Error::Bind("once key".into()))
    );
    assert_eq!(
        parse_outcome_with_version("outcome \"\"", VERSION),
        Err(Error::Bind("once key".into()))
    );
    assert_eq!(
        parse_once_with_version(&" ".repeat(MAX_INPUT + 1), VERSION),
        Err(Error::Limit)
    );
    let many_tokens = format!(
        "update items where id=1 set n={} once \"key\" for 1s",
        vec!["1"; MAX_TOKENS].join("+")
    );
    assert_eq!(
        parse_once_with_version(&many_tokens, VERSION),
        Err(Error::Limit)
    );
    let deep = format!(
        "update items where {}true set n=1 once \"key\" for 1s",
        "not ".repeat(MAX_DEPTH + 2)
    );
    assert_eq!(parse_once_with_version(&deep, VERSION), Err(Error::Limit));
    let empty = "update items where id=1 set payload=\"\" once \"key\" for 1s";
    let full_text = format!(
        "update items where id=1 set payload=\"{}\" once \"key\" for 1s",
        "x".repeat(MAX_INPUT - empty.len())
    );
    assert_eq!(full_text.len(), MAX_INPUT);
    assert_eq!(
        parse_once_with_version(&full_text, VERSION),
        Err(Error::Limit),
        "the bounded serialized envelope adds metadata beyond the text literal"
    );

    let parsed = parse_once_with_version(&write("24h"), VERSION).unwrap();
    let roundtrip: OnceEnvelope =
        serde_json::from_slice(&serde_json::to_vec(&parsed).unwrap()).unwrap();
    assert_eq!(
        roundtrip, parsed,
        "the selected language version is retained with a saved AST"
    );
}
