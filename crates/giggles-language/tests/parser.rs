use giggles_language::*;
#[test]
fn strings_comments_and_parameters() {
    let s =
        parse("-- hello\nINSERT into Items set id=$ID, note=\"hi \\u263a \\\"quoted\\\"\"; -- end")
            .unwrap();
    assert_eq!(
        s,
        Statement::Insert {
            table: "items".into(),
            values: vec![
                ("id".into(), ValueExpr::Parameter("id".into())),
                (
                    "note".into(),
                    ValueExpr::Literal(Value::Text("hi ☺ \"quoted\"".into()))
                )
            ]
        }
    );
}
#[test]
fn all_intents() {
    for s in [
        "items",
        "items where id=1 order by id desc limit $n select id",
        "insert items set id=1",
        "update items where id=1 when note is null set note=\"x\"",
        "update many items where id>1 set note=null at most 3",
        "delete items where id=1 when true",
        "delete many items where id>1 at most $cap",
        "claim items where note=null order by id set note=\"x\"",
    ] {
        assert!(parse(s).is_ok(), "{s}")
    }
}
#[test]
fn malformed_and_injection() {
    for s in [
        "",
        "items; delete items where true",
        "insert items set note='x'",
        "items where id=${id}",
        "items where id=$1",
        "items where id=\"unterminated",
        "items where id=\"bad\\q\"",
        "items where id=1=2",
        "items where (id=1",
        "items limit 1 where true",
        "items select id,",
        "items --\n where id=1 /*comment*/",
    ] {
        assert!(parse(s).is_err(), "{s}")
    }
}
#[test]
fn overflow_and_limits() {
    for n in [i64::MIN.to_string(), i64::MAX.to_string()] {
        assert!(parse(&format!("items where id={n}")).is_ok())
    }
    for n in [
        "9223372036854775808",
        "-9223372036854775809",
        "1.2",
        "1e2",
        "-",
    ] {
        assert!(parse(&format!("items where id={n}")).is_err())
    }
    assert_eq!(parse(&" ".repeat(MAX_INPUT + 1)), Err(Error::Limit));
    assert!(
        parse(&format!(
            "items where {}true{}",
            "(".repeat(50),
            ")".repeat(50)
        ))
        .is_err()
    );
}
#[test]
fn unsupported_contracts() {
    for s in [
        "once $key insert items set id=1",
        "atomic {items}",
        "items select uuid(\"a\")",
        "create table items",
        "items join others on true",
        "items where id+1=3",
        "items at snapshot $s",
        "items select * once $key",
        "items where id=1 returning id",
        "claim items where true set note=\"x\"",
        "update many items where true set note=null",
    ] {
        assert!(parse(s).is_err(), "{s}")
    }
}
#[test]
fn precedence_and_null() {
    let s = parse("items where not id=1 or note is not null and true").unwrap();
    if let Statement::Read {
        filter: Some(Expr::Or(a, b)),
        ..
    } = s
    {
        assert!(matches!(*a, Expr::Not(_)));
        assert!(matches!(*b, Expr::And(..)))
    } else {
        panic!("precedence")
    }
}
