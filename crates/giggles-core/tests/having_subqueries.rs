//! PG18.6 oracle: r2/core-fix2/pg18-pg1.txt and coordinator WO2a probes.
mod common;
use giggles_core::{Admin, CoreError, Engine, Limits, relational::*};
fn exercise(engine: Engine) {
    let path = common::fresh("having-subqueries");
    let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
    let mut s = admin.sql_session();
    for sql in ["CREATE TABLE orders(id int PRIMARY KEY,item text,qty int,price numeric)",
        "CREATE TABLE items(id text PRIMARY KEY,name text)", "CREATE TABLE u(n int)",
        "INSERT INTO orders VALUES(1,'a',1,1.5),(2,'a',2,2.5),(3,'b',5,NULL),(4,'c',NULL,3)",
        "INSERT INTO items VALUES('a','A'),('b','B'),('d','D')", "INSERT INTO u VALUES(1),(NULL)"] {
        s.execute(sql, &[], None).unwrap();
    }
    let expected = |items: &[(&str, i64)]| items.iter().map(|(a,n)|
        vec![Value::Text((*a).into()), Value::Int(*n)]).collect::<Vec<_>>();
    for (predicate, result) in [
        ("item IN(SELECT id FROM items)", expected(&[("a",2),("b",1)])),
        ("item IN(SELECT id FROM items) AND EXISTS(SELECT 1 FROM items) AND NOT EXISTS(SELECT 1 FROM items WHERE false)", expected(&[("a",2),("b",1)])),
        ("EXISTS(SELECT 1 FROM items WHERE items.id=orders.item)", expected(&[("a",2),("b",1)])),
        ("NOT EXISTS(SELECT 1 FROM items WHERE items.id=orders.item)", expected(&[("c",1)])),
        ("EXISTS(SELECT 1 FROM items WHERE EXISTS(SELECT 1 FROM items i WHERE i.id=orders.item))", expected(&[("a",2),("b",1)])),
        ("count(*) NOT IN(SELECT n FROM u)", expected(&[])),
        ("count(*) NOT IN(SELECT n FROM u WHERE n IS NOT NULL)", expected(&[("a",2)])),
    ] {
        let sql = format!("SELECT item,count(*) FROM orders GROUP BY item HAVING {predicate} ORDER BY item");
        let actual = s.execute(&sql, &[], None).unwrap_or_else(|e| panic!("{sql}: {e:?}"));
        assert_eq!(actual.rows.into_iter().map(|r| r.values).collect::<Vec<_>>(), result, "{sql}");
    }
    let sql = "SELECT item,count(*) FROM orders GROUP BY item HAVING item IN(SELECT id FROM items WHERE name=$1)";
    let actual = s.execute(sql, &[Value::Text("A".into())], None).unwrap();
    assert_eq!(actual.rows.into_iter().map(|r| r.values).collect::<Vec<_>>(), expected(&[("a",2)]));
    for (predicate, error) in [
        ("item IN(SELECT id,name FROM items)", SqlError::Syntax),
        ("EXISTS(SELECT nosuch FROM items)", SqlError::UnknownColumn),
        ("qty IN(SELECT n FROM u)", SqlError::GroupingError),
        ("EXISTS(SELECT 1 FROM items WHERE items.id=orders.qty::text)", SqlError::GroupingError),
        ("item IN(SELECT id FROM items WHERE items.id=orders.item)", SqlError::Unsupported),
    ] {
        let sql = format!("SELECT item,count(*) FROM orders GROUP BY item HAVING {predicate}");
        assert_eq!(s.execute(&sql, &[], None), Err(CoreError::Sql(error)), "{sql}");
    }
    assert_eq!(s.execute("SELECT item || 'x',count(*) FROM orders GROUP BY item || 'x' HAVING EXISTS(SELECT 1 FROM items WHERE items.id=orders.item)", &[], None),
        Err(CoreError::Sql(SqlError::GroupingError)));
    for predicate in [ReadPredicate::In { expr: ReadExpr::Column("item".into()),
        subquery: Box::new(SingleSourceRead { table: "items".into(), projection: vec![ReadProjection {
            expression: ReadExpr::Column("id".into()), alias: None }], filter: None }) },
        ReadPredicate::Exists(Box::new(SingleSourceRead { table: "items".into(), projection: vec![ReadProjection {
            expression: ReadExpr::Column("id".into()), alias: None }], filter: Some(ReadPredicate::Compare(
                ReadExpr::QualifiedColumn { source: "items".into(), column: "id".into() }, ReadCompare::Eq,
                ReadExpr::QualifiedColumn { source: "orders".into(), column: "item".into() })) }))] {
        let typed = SingleSourceRead { table: "orders".into(), projection: vec![
            ReadProjection { expression: ReadExpr::Column("item".into()), alias: None },
            ReadProjection { expression: ReadExpr::Aggregate(ReadAggregate::CountStar), alias: None }], filter: None }
            .with_clauses(ReadClauses { group_by: vec!["item".into()], having: Some(predicate),
                order_by: vec![ReadOrder { expression: ReadExpr::Column("item".into()), descending: false, nulls_first: None }],
                ..ReadClauses::default() });
        assert_eq!(s.execute_typed_read(typed, None).unwrap().rows.into_iter().map(|r| r.values).collect::<Vec<_>>(),
            expected(&[("a",2),("b",1)]));
    }
    drop(s); drop(admin); std::fs::remove_dir_all(path).unwrap();
}
#[test] fn legacy() { exercise(Engine::Legacy); }
#[cfg(feature = "engine-rocksdb")]
#[test] fn v2() { exercise(Engine::V2); }
