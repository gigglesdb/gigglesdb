use giggles_language::*;
const V: LanguageVersion = LanguageVersion::Embedded019;
fn col(name: &str) -> QueryExpr { QueryExpr::Column { source: None, column: name.into() } }
fn agg(function: AggregateFunction, column: Option<&str>) -> QueryExpr {
    QueryExpr::Aggregate { function, source: None, column: column.map(str::to_owned) }
}
fn projection(expression: QueryExpr, alias: Option<&str>) -> QueryProjection {
    QueryProjection { expression, alias: alias.map(str::to_owned) }
}
fn query(text: &str) -> Query {
    let Statement::Query(q) = parse_with_version(text, V).unwrap() else { panic!("typed query expected") }; *q
}
#[test]
fn typed_reads_parse() {
    let first = "orders group by item select item, count(*) as n, sum(qty) as total";
    let mut expected = Query { source: QuerySource { table: "orders".into(), alias: None },
        join: None, filter: None, group_by: vec![col("item")], having: None, order: vec![], limit: None,
        projection: vec![projection(col("item"), None), projection(agg(AggregateFunction::Count, None), Some("n")),
            projection(agg(AggregateFunction::Sum, Some("qty")), Some("total"))] };
    assert_eq!(query(first), expected);
    let second = "orders where qty > 1 group by item having count(*) > 1 order by item desc limit $n select item, max(qty)";
    let one = QueryExpr::Value(ValueExpr::Literal(Value::Int(1)));
    expected.filter = Some(QueryPredicate::Compare(col("qty"), Compare::Gt, one.clone()));
    expected.having = Some(QueryPredicate::Compare(agg(AggregateFunction::Count, None), Compare::Gt, one));
    expected.order = vec![QueryOrder { expression: col("item"), descending: true }];
    expected.limit = Some(ValueExpr::Parameter("n".into()));
    expected.projection = vec![projection(col("item"), None), projection(agg(AggregateFunction::Max, Some("qty")), None)];
    assert_eq!(query(second), expected);
    for text in [first, second] { assert!(parse_with_version(text, LanguageVersion::Embedded018).is_err()); }
    assert!(parse_with_version("orders select item, count(*) group by item", V).is_err());
    assert!(parse_with_version("orders group by item select item, count(*)", V).is_ok());
}
#[test]
fn joins_and_predicate_precedence() {
    let qualified = |source: &str, column: &str| QueryExpr::Column { source: Some(source.into()), column: column.into() };
    let source = |table: &str, alias: &str| QuerySource { table: table.into(), alias: Some(alias.into()) };
    let expected = Query { source: source("items", "i"), join: Some(QueryJoin { kind: JoinKind::Inner,
        source: source("orders", "o"), on: QueryPredicate::Compare(qualified("i", "id"), Compare::Eq, qualified("o", "item")) }),
        filter: None, group_by: vec![], having: None, order: vec![], limit: None,
        projection: vec![projection(qualified("i", "id"), None), projection(qualified("o", "qty"), None)] };
    assert_eq!(query("items i join orders o on i.id = o.item select i.id, o.qty"), expected);
    let mut left = expected;
    left.join.as_mut().unwrap().kind = JoinKind::Left;
    left.filter = Some(QueryPredicate::IsNull(qualified("o", "qty"), false));
    left.projection.pop();
    assert_eq!(query("items i left join orders o on i.id = o.item where o.qty is null select i.id"), left);
    assert!(matches!(parse_with_version("items inner join orders on items.id = orders.item select items.id, count(orders.qty) as n group by items.id", V), Err(Error::Syntax { .. })));
    assert!(parse_with_version("items inner join orders on items.id = orders.item group by items.id select items.id, count(orders.qty) as n", V).is_ok());
    let q = query("orders where not (qty is null or item is not null) and qty > 1 select count(*)");
    assert!(matches!(q.filter, Some(QueryPredicate::And(a, _)) if matches!(a.as_ref(), QueryPredicate::Not(p) if matches!(p.as_ref(), QueryPredicate::Or(..)))));
}
#[test]
fn old_reads_keep_old_ast() {
    for name in ["inner", "group", "join", "having", "left"] {
        let text = format!("transaction {{ let {name} = (items); }}");
        let old = parse_transaction_with_version(&text, LanguageVersion::Embedded018).unwrap();
        assert_eq!(parse_transaction_with_version(&text, V).unwrap(), old);
    }
    assert!(parse_transaction_with_version("transaction { savepoint inner; release inner; }", V).is_ok());
    assert!(matches!(parse_with_version("items select items.id", LanguageVersion::Embedded018), Err(Error::Syntax { .. })));
    assert_eq!(query("items select items.id").projection,
        vec![projection(QueryExpr::Column { source: Some("items".into()), column: "id".into() }, None)]);
    for text in ["items", "items where id=1 order by id desc limit $n select id",
        "items select id + 1 as next", "items select id", "app.items where id=1"] {
        let old = parse_with_version(text, LanguageVersion::Embedded018).unwrap();
        assert_eq!(parse_with_version(text, V).unwrap(), old, "{text}");
    }
}
#[test]
fn typed_reads_are_opt_in() {
    let text = "orders select count(*)";
    assert_eq!(lower_text(text, &Bindings::new(), V), Err(Error::Unsupported));
    assert_eq!(lower_text("transaction { orders select count(*); }", &Bindings::new(), V), Err(Error::Unsupported));
    assert_eq!(parse_explain_with_version("explain orders select count(*)", V), Err(Error::Unsupported));
    assert_eq!(lower_text(&format!("orders where {}qty = 1 select count(*)", "not ".repeat(100)), &Bindings::new(), V), Err(Error::Limit));
    for (count, accepted) in [(31, true), (32, false)] {
        assert_eq!(parse_with_version(&format!("orders where {}qty = 1 select count(*)", "not ".repeat(count)), V).is_ok(), accepted);
    }
    let q = query("orders where qty = 1 or qty = 2 and qty = 3 select count(*)");
    assert!(matches!(q.filter, Some(QueryPredicate::Or(_, b)) if matches!(b.as_ref(), QueryPredicate::And(..))));
    let huge = format!("orders select {}", vec!["count(*)"; MAX_EXPRESSION_NODES + 1].join(","));
    assert_eq!(parse_with_version(&huge, V), Err(Error::Limit));
}
