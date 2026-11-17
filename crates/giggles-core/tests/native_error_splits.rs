mod common;
use giggles_core::{Admin, Limits, operations::{Bindings, Compare, Engine, Error, Expr, Intent, Request, ValueExpr}, relational::Value as V};
fn lit(v: V) -> ValueExpr { ValueExpr::Literal(v) }
fn expr(v: V) -> Expr { Expr::Value(lit(v)) }
fn read(table: &str, filter: Option<Expr>) -> Request {
    Request::Read { table: table.into(), filter, order: vec![], limit: None, projection: vec![] }
}
// Frozen SQLSTATE mappings and deliberate native contract differences.
// PostgreSQL 18.6 evidence: r2/core-gaps/pg/item34.log.
#[test]
fn assignment_sites_and_duplicate_precedence() {
    let path = common::fresh("native-error-splits");
    let admin = Admin::create(&path, Limits::default()).unwrap();
    let engine = Engine::new(admin.clone());
    let mut sql = admin.sql_session();
    sql.execute("CREATE TABLE t(id int PRIMARY KEY,v smallint NOT NULL,b boolean)", &[], None).unwrap();
    sql.execute("INSERT INTO t VALUES(1,1,true)", &[], None).unwrap();
    for (value, source, state) in [(V::Null, "NULL", "23502"), (V::Int(32768), "32768::int8", "22003"), (V::Bool(true), "true::boolean", "42804"), (V::Text("x".into()), "'x'::text", "42804")] {
        for mode in 0..6 {
            let values = vec![("id".into(), lit(V::Int(2))), ("v".into(), lit(value.clone()))];
            let request = match mode {
                0 => Request::Insert { table: "t".into(), values },
                1 => Request::InsertExpressions { table: "t".into(), values: values.into_iter().map(|(n,v)| (n,Expr::Value(v))).collect() },
                2 => Request::InsertMany { table: "t".into(), columns: vec!["id".into(),"v".into()], rows: vec![vec![expr(V::Int(2)),expr(value.clone())]], cap: lit(V::Int(1)) },
                3 => Request::Update { table: "t".into(), filter: expr(V::Bool(true)), guard: None, values: vec![("v".into(),lit(value.clone()))], intent: Intent::One },
                _ => Request::UpdateExpressions { table: "t".into(), filter: expr(V::Bool(mode == 4)), guard: None, values: vec![("v".into(),expr(value.clone()))], intent: Intent::One },
            };
            if mode == 5 && !matches!(value,V::Bool(_) | V::Text(_)) { continue; }
            let actual = engine.execute(&request, &Bindings::new()).unwrap_err();
            assert_eq!(actual, if state == "22003" { Error::Overflow } else { Error::Sql { sqlstate: state.into() } }, "{source} mode {mode}");
            let statement = if mode < 3 { format!("INSERT INTO t(id,v) VALUES(2,{source})") }
                else { format!("UPDATE t SET v={source} WHERE {}", if mode == 5 { "false" } else { "true" }) };
            assert_eq!(giggles_core::sqlstate(&sql.execute(&statement, &[], None).unwrap_err()), state, "{statement}");
        }
    }
    for mode in 0..5 {
        let mut values = vec![("v".into(),lit(V::Int(1))),("v".into(),lit(V::Int(2)))];
        if mode < 3 { values.insert(0,("id".into(),lit(V::Int(2)))); }
        let request = match mode {
            0 => Request::Insert { table: "t".into(), values },
            1 => Request::InsertExpressions { table: "t".into(), values: values.into_iter().map(|(n,v)| (n,Expr::Value(v))).collect() },
            2 => Request::InsertMany { table: "t".into(), columns: vec!["id".into(),"v".into(),"v".into()], rows: vec![vec![expr(V::Int(2)),expr(V::Int(1)),expr(V::Int(2))]], cap: lit(V::Int(1)) },
            3 => Request::Update { table: "t".into(), filter: expr(V::Bool(true)), guard: None, values, intent: Intent::One },
            _ => Request::UpdateExpressions { table: "t".into(), filter: expr(V::Bool(true)), guard: None, values: values.into_iter().map(|(n,v)| (n,Expr::Value(v))).collect(), intent: Intent::One },
        };
        assert_eq!(engine.execute(&request,&Bindings::new()).unwrap_err(),if mode < 3 { Error::Sql { sqlstate:"42701".into() } } else { Error::Sql { sqlstate:"42601".into() } });
        let statement = if mode < 3 { "INSERT INTO t(id,v,v) VALUES(2,1,2)" } else { "UPDATE t SET v=1,v=2" };
        assert_eq!(giggles_core::sqlstate(&sql.execute(statement,&[],None).unwrap_err()),if mode < 3 { "42701" } else { "42601" });
    }
    let duplicate_bad_value = Request::Insert { table: "t".into(), values: vec![("id".into(),lit(V::Int(2))),("v".into(),lit(V::Bool(true))),("v".into(),lit(V::Int(1)))] };
    assert_eq!(engine.execute(&duplicate_bad_value,&Bindings::new()).unwrap_err(),Error::Sql { sqlstate:"42701".into() });
    assert_eq!(giggles_core::sqlstate(&sql.execute("INSERT INTO t(id,v,v) VALUES(2,true::boolean,1)",&[],None).unwrap_err()),"42701");
    for first in [false,true] { for expression in [false,true] {
        let mut values=vec![("v".into(),lit(V::Bool(true))),("v".into(),lit(V::Int(1)))]; if !first { values.reverse(); }
        let request=if expression { Request::UpdateExpressions { table:"t".into(),filter:expr(V::Bool(true)),guard:None,values:values.into_iter().map(|(n,v)| (n,Expr::Value(v))).collect(),intent:Intent::One } } else { Request::Update { table:"t".into(),filter:expr(V::Bool(true)),guard:None,values,intent:Intent::One } };
        assert_eq!(engine.execute(&request,&Bindings::new()).unwrap_err().sqlstate(),Some("42804"));
        assert_eq!(giggles_core::sqlstate(&sql.execute(if first { "UPDATE t SET v=true::boolean,v=1" } else { "UPDATE t SET v=1,v=true::boolean" },&[],None).unwrap_err()),"42804");
    } }
    for value in [V::Null,V::Int(32768)] { for expression in [false,true] {
        let values=vec![("v".into(),lit(value.clone())),("v".into(),lit(V::Int(1)))];
        let request=if expression { Request::UpdateExpressions { table:"t".into(),filter:expr(V::Bool(true)),guard:None,values:values.into_iter().map(|(n,v)| (n,Expr::Value(v))).collect(),intent:Intent::One } } else { Request::Update { table:"t".into(),filter:expr(V::Bool(true)),guard:None,values,intent:Intent::One } };
        assert_eq!(engine.execute(&request,&Bindings::new()).unwrap_err().sqlstate(),Some("42601"));
        assert_eq!(giggles_core::sqlstate(&sql.execute(&format!("UPDATE t SET v={},v=1",if value == V::Null { "NULL" } else { "32768::int8" }),&[],None).unwrap_err()),"42601");
    } }
    sql.execute("ALTER TABLE t ADD COLUMN w smallint",&[],None).unwrap();
    for many in [false,true] {
        let row=vec![expr(V::Int(2)),expr(V::Null),expr(V::Int(32768))]; let columns=vec!["id".into(),"v".into(),"w".into()];
        let request=if many { Request::InsertMany { table:"t".into(),columns,rows:vec![row],cap:lit(V::Int(1)) } } else { Request::InsertExpressions { table:"t".into(),values:columns.into_iter().zip(row).collect() } };
        assert_eq!(engine.execute(&request,&Bindings::new()).unwrap_err(),Error::Overflow);
        assert_eq!(giggles_core::sqlstate(&sql.execute("INSERT INTO t(id,v,w) VALUES(2,NULL,32768::int8)",&[],None).unwrap_err()),"22003");
    }
    let combined=Request::Update { table:"t".into(),filter:expr(V::Bool(true)),guard:None,values:vec![("v".into(),lit(V::Null)),("w".into(),lit(V::Int(32768)))],intent:Intent::One };
    assert_eq!(engine.execute(&combined,&Bindings::new()).unwrap_err(),Error::Overflow);
    assert_eq!(giggles_core::sqlstate(&sql.execute("UPDATE t SET v=NULL,w=32768::int8",&[],None).unwrap_err()),"22003");
    let empty_update=Request::Update { table:"t".into(),filter:expr(V::Bool(false)),guard:None,values:vec![("v".into(),lit(V::Null))],intent:Intent::One };
    assert_eq!(engine.execute(&empty_update,&Bindings::new()).unwrap_err(),Error::Sql { sqlstate:"23502".into() });
    assert_eq!(sql.execute("UPDATE t SET v=NULL WHERE false",&[],None).unwrap().affected,0);
    // Missing relation wins over both unknown column and incompatible comparison.
    let bad_filter = Expr::Compare(Box::new(Expr::Field("absent".into())),Compare::Eq,Box::new(expr(V::Bool(true))));
    assert_eq!(engine.execute(&read("missing",Some(bad_filter)),&Bindings::new()).unwrap_err(),Error::Sql { sqlstate:"42P01".into() });
    assert_eq!(giggles_core::sqlstate(&sql.execute("SELECT * FROM missing WHERE absent=true",&[],None).unwrap_err()),"42P01");
    for (value,source) in [(V::Bool(true),"true::boolean"),(V::Text("x".into()),"'x'::text")] {
        let bad_filter = Expr::Compare(Box::new(Expr::Field("v".into())),Compare::Eq,Box::new(expr(value)));
        assert_eq!(engine.execute(&read("t",Some(bad_filter)),&Bindings::new()).unwrap_err(),Error::Sql { sqlstate:"42883".into() });
        assert_eq!(giggles_core::sqlstate(&sql.execute(&format!("SELECT * FROM t WHERE v={source}"),&[],None).unwrap_err()),"42883");
    }
    drop(sql); drop(engine); drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn kept_expression_and_result_contracts() {
    use giggles_core::operations::{Arithmetic, Projection, ScalarFunction};
    let path = common::fresh("native-error-keeps");
    let admin = Admin::create(&path,Limits::default()).unwrap();
    let engine = Engine::new(admin.clone()); let mut sql=admin.sql_session();
    sql.execute("CREATE TABLE t(id int PRIMARY KEY,v int,txt text)",&[],None).unwrap();
    let int=expr(V::Int(1)); let boolean=expr(V::Bool(true));
    let cases=vec![
        (int.clone(),"SELECT * FROM t WHERE 1::int8","42804"),
        (Expr::Not(Box::new(int.clone())),"SELECT * FROM t WHERE NOT 1::int8","42804"),
        (Expr::And(Box::new(int.clone()),Box::new(boolean.clone())),"SELECT * FROM t WHERE 1::int8 AND true","42804"),
        (Expr::Or(Box::new(int.clone()),Box::new(boolean.clone())),"SELECT * FROM t WHERE 1::int8 OR true","42804"),
        (Expr::Arithmetic(Box::new(int.clone()),Arithmetic::Add,Box::new(boolean.clone())),"SELECT 1::int8 + true::boolean FROM t","42883"),
        (Expr::Negate(Box::new(boolean.clone())),"SELECT -true::boolean FROM t","42883"),
        (Expr::Case { operand: None, arms: vec![(boolean.clone(),int.clone())], else_expr: Some(Box::new(boolean.clone())) },"SELECT CASE WHEN true THEN 1::int8 ELSE true::boolean END FROM t","42804"),
        (Expr::Case { operand:Some(Box::new(int.clone())),arms:vec![(boolean.clone(),int.clone())],else_expr:None },"SELECT CASE 1::int8 WHEN true::boolean THEN 1 END FROM t","42883"),
        (Expr::Case { operand:None,arms:vec![(int.clone(),int.clone())],else_expr:None },"SELECT CASE WHEN 1::int8 THEN 1 END FROM t","42804"),
        (Expr::Call { function: ScalarFunction::Coalesce, arguments: vec![int.clone(),boolean.clone()] },"SELECT coalesce(1::int8,true::boolean) FROM t","42804"),
        (Expr::Call { function: ScalarFunction::Abs, arguments: vec![boolean.clone()] },"SELECT abs(true::boolean) FROM t","42883"),
        (Expr::Call { function: ScalarFunction::Length, arguments: vec![int] },"SELECT length(1::int8) FROM t","42883"),
    ];
    for (expression,statement,state) in cases {
        let request=if statement.starts_with("SELECT *") { read("t",Some(expression)) }
            else { Request::ReadExpressions { table:"t".into(),filter:None,order:vec![],limit:None,projection:vec![Projection { expression,alias:Some("x".into()) }] } };
        assert_eq!(engine.execute(&request,&Bindings::new()).unwrap_err(),Error::Type,"{statement}");
        assert_eq!(giggles_core::sqlstate(&sql.execute(statement,&[],None).unwrap_err()),state,"{statement}");
    }
    let request=Request::Insert { table:"t".into(),values:vec![("id".into(),lit(V::Int(1))),("txt".into(),lit(V::Int(1)))] };
    assert_eq!(engine.execute(&request,&Bindings::new()).unwrap_err(),Error::Type);
    sql.execute("INSERT INTO t(id,txt) VALUES(1,1::int8)",&[],None).unwrap();
    let comparison=Expr::Compare(Box::new(Expr::Field("v".into())),Compare::Eq,Box::new(expr(V::Numeric(giggles_core::relational::Numeric::parse("1").unwrap()))));
    assert_eq!(engine.execute(&read("t",Some(comparison)),&Bindings::new()).unwrap_err(),Error::Type);
    sql.execute("SELECT * FROM t WHERE v=1::numeric",&[],None).unwrap();
    let projection=Projection { expression:Expr::Field("v".into()),alias:None };
    let duplicate=Request::ReadExpressions { table:"t".into(),filter:None,order:vec![],limit:None,projection:vec![projection.clone(),projection] };
    assert_eq!(engine.execute(&duplicate,&Bindings::new()).unwrap_err(),Error::Bind("duplicate projection".into()));
    // SQL permits duplicate output names and order keys; native results require unique names.
    let request=Request::Read { table:"t".into(),filter:None,order:vec![],limit:None,projection:vec!["v".into(),"v".into()] };
    assert_eq!(engine.execute(&request,&Bindings::new()).unwrap_err(),Error::Bind("duplicate projection".into()));
    sql.execute("SELECT v,v FROM t",&[],None).unwrap();
    drop(sql);drop(engine);drop(admin);std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn route_shape_and_catalog_keeps() {
    use giggles_core::operations::{CatalogRequest, ColumnSpec, InspectionRequest, Projection, Sort, Transaction, TransactionStep};
    let path=common::fresh("native-error-routes"); let admin=Admin::create(&path,Limits::default()).unwrap();
    let engine=Engine::new(admin.clone()); let mut sql=admin.sql_session();
    sql.execute("CREATE TABLE t(id int PRIMARY KEY,v int)",&[],None).unwrap();
    let missing=read("missing",None); let expected=Error::Sql { sqlstate:"42P01".into() };
    assert_eq!(engine.explain_read(&missing,&Bindings::new()).unwrap_err(),expected);
    assert_eq!(engine.inspect(&InspectionRequest::Table { table:"missing".into() }).unwrap_err(),expected);
    assert_eq!(engine.execute_transaction(&Transaction { read_only:true,steps:vec![TransactionStep::Execute(missing)],emit:None },&Bindings::new()).unwrap_err(),expected);
    assert_eq!(giggles_core::sqlstate(&sql.execute("SELECT * FROM missing",&[],None).unwrap_err()),"42P01");
    let scoped=Request::InsertExpressions { table:"t".into(),values:vec![("id".into(),Expr::Field("id".into()))] };
    assert_eq!(engine.execute(&scoped,&Bindings::new()).unwrap_err(),Error::Bind("column scope".into()));
    assert_eq!(giggles_core::sqlstate(&sql.execute("INSERT INTO t(id) VALUES(id)",&[],None).unwrap_err()),"42703");
    let malformed=Request::InsertMany { table:"t".into(),columns:vec!["id".into(),"v".into()],rows:vec![vec![expr(V::Int(1))]],cap:lit(V::Int(1)) };
    assert_eq!(engine.execute(&malformed,&Bindings::new()).unwrap_err(),Error::Type);
    assert_eq!(giggles_core::sqlstate(&sql.execute("INSERT INTO t(id,v) VALUES(1)",&[],None).unwrap_err()),"42601");
    let mut negative=read("t",None); if let Request::Read { limit,.. }=&mut negative { *limit=Some(lit(V::Int(-1))); }
    assert_eq!(engine.execute(&negative,&Bindings::new()).unwrap_err(),Error::Type);
    assert_eq!(giggles_core::sqlstate(&sql.execute("SELECT * FROM t LIMIT -1",&[],None).unwrap_err()),"2201W");
    let index=CatalogRequest::CreateIndex { table:"t".into(),name:"ix".into(),columns:vec!["v".into(),"v".into()] };
    assert_eq!(engine.execute_catalog(&index).unwrap_err(),Error::Bind("duplicate column".into()));
    assert_eq!(giggles_core::sqlstate(&sql.execute("CREATE INDEX ix ON t(v,v)",&[],None).unwrap_err()),"23505");
    let key=ColumnSpec { name:"id".into(),data_type:giggles_core::relational::DataType::Integer,not_null:false,default:None,generated:None };
    assert_eq!(engine.execute_catalog(&CatalogRequest::CreateTable { table:"pk".into(),columns:vec![key],primary_key:vec!["id".into()] }).unwrap_err(),Error::Type);
    sql.execute("CREATE TABLE pk(id integer PRIMARY KEY)",&[],None).unwrap();
    for (expression,alias,error,statement) in [(expr(V::Null),Some("x".into()),Error::Type,"SELECT NULL AS x FROM t"),(expr(V::Int(1)),None,Error::Bind("projection alias".into()),"SELECT 1 FROM t")] {
        let request=Request::ReadExpressions { table:"t".into(),filter:None,order:vec![],limit:None,projection:vec![Projection { expression,alias }] };
        assert_eq!(engine.execute(&request,&Bindings::new()).unwrap_err(),error); sql.execute(statement,&[],None).unwrap();
    }
    let sort=Sort { field:"v".into(),descending:false };
    let request=Request::Read { table:"t".into(),filter:None,order:vec![sort.clone(),sort],limit:None,projection:vec![] };
    assert_eq!(engine.execute(&request,&Bindings::new()).unwrap_err(),Error::Bind("duplicate order".into()));
    sql.execute("SELECT * FROM t ORDER BY v,v",&[],None).unwrap();
    drop(sql);drop(engine);drop(admin);std::fs::remove_dir_all(path).unwrap();
}
