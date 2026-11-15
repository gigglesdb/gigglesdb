//! PG18.6: core-fix2/pg18-pg1.txt and WO2b candidate-side probes.
mod common;
use giggles_core::{Admin, CoreError, Engine, Limits, relational::*};
fn exercise(engine: Engine) {
    let path = common::fresh("join-on-subqueries");
    let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
    let mut s = admin.sql_session();
    for sql in ["CREATE TABLE orders(id int PRIMARY KEY,item text,qty int,price numeric)",
        "CREATE TABLE items(id text PRIMARY KEY,name text)", "CREATE TABLE u(n int)",
        "INSERT INTO orders VALUES(1,'a',1,1.5),(2,'a',2,2.5),(3,'b',5,NULL),(4,'c',NULL,3)",
        "INSERT INTO items VALUES('a','A'),('b','B'),('d','D')", "INSERT INTO u VALUES(1),(NULL)"] {
        s.execute(sql, &[], None).unwrap();
    }
    let expected = |rows: &[(&str, Option<i64>)]| rows.iter().map(|(a,n)| vec![
        Value::Text((*a).into()), n.map_or(Value::Null, Value::Int)]).collect::<Vec<_>>();
    for (kind, predicate, rows) in [
        ("LEFT", "i.id=o.item AND o.qty IN(SELECT n FROM u)", expected(&[("a",Some(1)),("b",None),("d",None)])),
        ("LEFT", "EXISTS(SELECT 1 FROM u WHERE u.n=o.qty) AND o.item=i.id", expected(&[("a",Some(1)),("b",None),("d",None)])),
        ("INNER", "o.item=i.id AND NOT EXISTS(SELECT 1 FROM u WHERE u.n=o.qty)", expected(&[("a",Some(2)),("b",Some(3))])),
        ("INNER", "o.qty NOT IN(SELECT n FROM u)", vec![]),
        ("LEFT", "EXISTS(SELECT 1 FROM items x WHERE x.id=i.id AND x.id=o.item)", expected(&[("a",Some(1)),("a",Some(2)),("b",Some(3)),("d",None)])),
        ("LEFT", "EXISTS(SELECT 1 FROM items x WHERE x.id=i.id) AND o.item=i.id", expected(&[("a",Some(1)),("a",Some(2)),("b",Some(3)),("d",None)])),
        ("INNER", "i.id=o.item AND EXISTS(SELECT 1 FROM u)", expected(&[("a",Some(1)),("a",Some(2)),("b",Some(3))])),
        ("LEFT", "i.id=o.item AND NOT EXISTS(SELECT 1 FROM u)", expected(&[("a",None),("b",None),("d",None)])),
    ] {
        let sql = format!("SELECT i.id,o.id FROM items i {kind} JOIN orders o ON {predicate} ORDER BY i.id,o.id");
        let actual = s.execute(&sql, &[], None).unwrap_or_else(|e| panic!("{sql}: {e:?}"));
        assert_eq!(actual.rows.into_iter().map(|r| r.values).collect::<Vec<_>>(), rows, "{sql}");
    }
    let qty = s.execute("SELECT i.id,o.qty FROM items i LEFT JOIN orders o ON i.id=o.item AND o.qty IN(SELECT n FROM u) ORDER BY i.id", &[], None).unwrap();
    assert_eq!(qty.rows.into_iter().map(|r| r.values).collect::<Vec<_>>(), expected(&[("a",Some(1)),("b",None),("d",None)]));
    for sql in ["SELECT i.id,o.id FROM items i JOIN orders o ON i.id=o.item AND o.qty IN(SELECT n FROM u WHERE n=$1) ORDER BY i.id,o.id",
        "SELECT i.id,o.id FROM items i JOIN orders o ON i.id=o.item AND o.qty IN(SELECT n FROM u LIMIT $1) ORDER BY i.id,o.id"] {
        assert_eq!(s.execute(sql, &[Value::Int(1)], None).unwrap().rows.into_iter().map(|r| r.values).collect::<Vec<_>>(), expected(&[("a",Some(1))]));
    }
    for (predicate, error) in [
        ("o.item IN(SELECT id,name FROM items)", SqlError::Syntax),
        ("EXISTS(SELECT 1 FROM u WHERE missing=o.qty)", SqlError::UnknownColumn),
        ("o.qty IN(SELECT n FROM u WHERE n=o.qty)", SqlError::Unsupported),
    ] {
        let sql = format!("SELECT i.id FROM items i JOIN orders o ON {predicate}");
        assert_eq!(s.execute(&sql, &[], None), Err(CoreError::Sql(error)), "{sql}");
    }
    assert_eq!(s.execute("SELECT i.id FROM items i JOIN orders o ON EXISTS(SELECT 1 FROM u WHERE u.n=z.n) JOIN u z ON true", &[], None), Err(CoreError::Sql(SqlError::UnknownTable)));
    let q = |source: &str, column: &str| ReadExpr::QualifiedColumn { source: source.into(), column: column.into() };
    for (predicate, sql_predicate) in [
        (ReadPredicate::In { expr: q("o", "qty"), subquery: Box::new(SingleSourceRead { table: "u".into(), projection: vec![ReadProjection { expression: ReadExpr::Column("n".into()), alias: None }], filter: None }) }, "o.qty IN(SELECT n FROM u)"),
        (ReadPredicate::Exists(Box::new(SingleSourceRead { table: "u".into(), projection: vec![ReadProjection { expression: ReadExpr::Literal(Value::Int(1)), alias: None }], filter: Some(ReadPredicate::Compare(q("u", "n"), ReadCompare::Eq, q("o", "qty"))) })), "EXISTS(SELECT 1 FROM u WHERE u.n=o.qty)"),
    ] {
        let typed = TwoSourceRead { left: ReadSource { table: "items".into(), alias: "i".into() },
            right: ReadSource { table: "orders".into(), alias: "o".into() }, kind: ReadJoinKind::Left,
            on: ReadPredicate::And(Box::new(ReadPredicate::Compare(q("i", "id"), ReadCompare::Eq, q("o", "item"))), Box::new(predicate)),
            group_by: vec![], projection: vec![ReadProjection { expression: q("i", "id"), alias: None }, ReadProjection { expression: q("o", "id"), alias: None }], filter: None };
        let sql = format!("SELECT i.id,o.id FROM items i LEFT JOIN orders o ON i.id=o.item AND {sql_predicate}");
        let actual = s.execute_typed_join(typed, None).unwrap();
        assert_eq!(actual, s.execute(&sql, &[], None).unwrap());
        assert_eq!(actual.rows.len(), 3);
    }
    drop(s); drop(admin); std::fs::remove_dir_all(path).unwrap();
}
#[test] fn legacy() { exercise(Engine::Legacy); }
#[cfg(feature = "engine-rocksdb")]
#[test] fn v2() { exercise(Engine::V2); }
