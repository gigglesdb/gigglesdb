mod common;
use giggles_core::{
    Admin, Limits,
    operations::{Bindings, ConflictAction, Engine, Error, Expr, Intent, Request, ValueExpr},
    relational::{Float8, Numeric, Value as V},
};
fn lit(v: V) -> ValueExpr {
    ValueExpr::Literal(v)
}
fn expr(v: V) -> Expr {
    Expr::Value(lit(v))
}
fn num(v: &str) -> V {
    V::Numeric(Numeric::parse(v).unwrap())
}
fn float(v: &str) -> V {
    V::Float8(Float8::parse(v).unwrap())
}
fn insert(table: &str, v: V) -> Request {
    Request::Insert { table: table.into(), values: vec![("id".into(), lit(V::Int(1))), ("v".into(), lit(v))] }
}
fn exercise(admin: Admin) {
    let engine = Engine::new(admin.clone());
    let mut sql = admin.sql_session();
    // Frozen PostgreSQL 18.6 results: r2/core-gaps/pg/item1.log.
    let cases = vec![
        ("numeric", V::Int(7), "7::int8", None),
        ("numeric(5,2)", V::Int(7), "7::int8", None),
        ("numeric(5,2)", V::Int(1234), "1234::int8", Some("22003")),
        ("numeric(5,2)", num("1.005"), "1.005::numeric", None),
        ("numeric(5,2)", num("-0.005"), "-0.005::numeric", None),
        ("float8", V::Int(2147483648), "2147483648::int8", None),
        ("real", V::Int(16777217), "16777217::int8", None),
        ("integer", num("2.5"), "2.5::numeric", None),
        ("integer", float("2.5"), "2.5::float8", None),
        ("integer", float("3.5"), "3.5::float8", None),
        ("integer", num("NaN"), "'NaN'::numeric", Some("0A000")),
        ("bigint", num("1e30"), "1e30::numeric", Some("22003")),
        ("float8", num("1.25"), "1.25::numeric", None),
        ("numeric", float("0.1"), "0.1::float8", None),
        ("numeric", float("NaN"), "'NaN'::float8", None),
    ];
    for (case, (ty, value, source, expected)) in cases.into_iter().enumerate() {
        for mode in 0..3 {
            let native = format!("n{case}_{mode}");
            let reference = format!("s{case}_{mode}");
            for table in [&native, &reference] {
                sql.execute(&format!("CREATE TABLE {table}(id int PRIMARY KEY,v {ty})"), &[], None).unwrap();
            }
            let request = match mode {
                0 => insert(&native, value.clone()),
                1 => Request::InsertExpressions {
                    table: native.clone(),
                    values: vec![("id".into(), expr(V::Int(1))), ("v".into(), expr(value.clone()))],
                },
                _ => Request::InsertMany {
                    table: native.clone(),
                    columns: vec!["id".into(), "v".into()],
                    rows: vec![vec![expr(V::Int(1)), expr(value.clone())]],
                    cap: lit(V::Int(1)),
                },
            };
            let actual = engine.execute(&request, &Bindings::new());
            let reference_result = sql.execute(&format!("INSERT INTO {reference} VALUES(1,{source})"), &[], None);
            if let Some(state) = expected {
                assert_eq!(actual.unwrap_err().sqlstate(), Some(state), "{ty} {source} mode {mode}");
                assert_eq!(giggles_core::sqlstate(&reference_result.unwrap_err()), state);
            } else {
                let actual = actual.unwrap_or_else(|e| panic!("{ty} {source} mode {mode}: {e:?}"));
                assert_eq!(actual.affected as u64, reference_result.unwrap().affected);
            }
            let read = |table: &str| format!("SELECT id,v FROM {table}");
            assert_eq!(
                sql.execute(&read(&native), &[], None).unwrap().rows.into_iter().map(|r| r.values).collect::<Vec<_>>(),
                sql.execute(&read(&reference), &[], None)
                    .unwrap()
                    .rows
                    .into_iter()
                    .map(|r| r.values)
                    .collect::<Vec<_>>(),
                "{ty} {source} mode {mode}"
            );
        }
    }
    for table in ["updates", "reference"] {
        sql.execute(
            &format!("CREATE TABLE {table}(id int PRIMARY KEY,n numeric,f float8,n52 numeric(5,2))"),
            &[],
            None,
        )
        .unwrap();
        sql.execute(&format!("INSERT INTO {table} VALUES(1,0,0,0)"), &[], None).unwrap();
    }
    for expression in [false, true] {
        let values = vec![
            ("n".into(), lit(V::Int(5))),
            ("f".into(), lit(V::Int(3))),
            ("n52".into(), ValueExpr::Parameter("bound".into())),
        ];
        let request = if expression {
            Request::UpdateExpressions {
                table: "updates".into(),
                filter: expr(V::Bool(true)),
                guard: None,
                values: values.into_iter().map(|(n, v)| (n, Expr::Value(v))).collect(),
                intent: Intent::One,
            }
        } else {
            Request::Update {
                table: "updates".into(),
                filter: expr(V::Bool(true)),
                guard: None,
                values,
                intent: Intent::One,
            }
        };
        let actual = engine.execute(&request, &Bindings::from([("bound".into(), V::Int(9))])).unwrap();
        assert_eq!(
            actual.affected as u64,
            sql.execute("UPDATE reference SET n=5::int8,f=3::int8,n52=9::int8", &[], None).unwrap().affected
        );
        assert_eq!(
            sql.execute("SELECT * FROM updates", &[], None)
                .unwrap()
                .rows
                .into_iter()
                .map(|r| r.values)
                .collect::<Vec<_>>(),
            sql.execute("SELECT * FROM reference", &[], None)
                .unwrap()
                .rows
                .into_iter()
                .map(|r| r.values)
                .collect::<Vec<_>>()
        );
        let overflow = if expression {
            Request::UpdateExpressions {
                table: "updates".into(),
                filter: expr(V::Bool(true)),
                guard: None,
                values: vec![("n52".into(), expr(V::Int(1234)))],
                intent: Intent::One,
            }
        } else {
            Request::Update {
                table: "updates".into(),
                filter: expr(V::Bool(true)),
                guard: None,
                values: vec![("n52".into(), lit(V::Int(1234)))],
                intent: Intent::One,
            }
        };
        assert_eq!(engine.execute(&overflow, &Bindings::new()).unwrap_err().sqlstate(), Some("22003"));
        assert_eq!(
            giggles_core::sqlstate(&sql.execute("UPDATE reference SET n52=1234::int8", &[], None).unwrap_err()),
            "22003"
        );
        assert_eq!(
            sql.execute("SELECT * FROM updates", &[], None)
                .unwrap()
                .rows
                .into_iter()
                .map(|r| r.values)
                .collect::<Vec<_>>(),
            sql.execute("SELECT * FROM reference", &[], None)
                .unwrap()
                .rows
                .into_iter()
                .map(|r| r.values)
                .collect::<Vec<_>>()
        );
    }
    sql.execute("CREATE TABLE conflicts(id int PRIMARY KEY,v numeric)", &[], None).unwrap();
    engine.execute(&insert("conflicts", V::Int(1)), &Bindings::new()).unwrap();
    let conflict = Request::OnConflict {
        request: Box::new(insert("conflicts", V::Int(2))),
        target: vec!["id".into()],
        action: ConflictAction::Update { guard: None, values: vec![("v".into(), expr(V::Int(3)))] },
    };
    engine.execute(&conflict, &Bindings::new()).unwrap();
    assert_eq!(sql.execute("SELECT v FROM conflicts", &[], None).unwrap().rows[0].values, vec![num("3")]);
    sql.execute("CREATE TABLE conflict_reference(id int PRIMARY KEY,v numeric)", &[], None).unwrap();
    sql.execute("INSERT INTO conflict_reference VALUES(1,1::int8)", &[], None).unwrap();
    sql.execute("INSERT INTO conflict_reference VALUES(1,2::int8) ON CONFLICT(id) DO UPDATE SET v=3::int8", &[], None)
        .unwrap();
    assert_eq!(
        sql.execute("SELECT v FROM conflicts", &[], None).unwrap().rows[0].values,
        sql.execute("SELECT v FROM conflict_reference", &[], None).unwrap().rows[0].values
    );
    sql.execute("CREATE TABLE mismatch(id int PRIMARY KEY,v numeric)", &[], None).unwrap();
    assert_eq!(engine.execute(&insert("mismatch", V::Text("7".into())), &Bindings::new()), Err(Error::Sql { sqlstate: "42804".into() }));
    assert_eq!(giggles_core::sqlstate(&sql.execute("INSERT INTO mismatch VALUES(1,'7'::text)", &[], None).unwrap_err()), "42804");
    sql.execute("CREATE TABLE int_range(id int PRIMARY KEY,v int)", &[], None).unwrap();
    sql.execute("CREATE TABLE small_range(id int PRIMARY KEY,v smallint)", &[], None).unwrap();
    assert_eq!(engine.execute(&insert("small_range", V::Int(32768)), &Bindings::new()), Err(Error::Overflow));
    sql.execute("CREATE TABLE text_boundary(id int PRIMARY KEY,v text)", &[], None).unwrap();
    assert_eq!(engine.execute(&insert("text_boundary", num("7")), &Bindings::new()), Err(Error::Type));
    sql.execute("INSERT INTO text_boundary VALUES(1,7::numeric)", &[], None).unwrap();
    // Native integer-family overflow preserves SQLSTATE 22003.
    assert_eq!(engine.execute(&insert("int_range", V::Int(2147483648)), &Bindings::new()), Err(Error::Overflow));
    assert_eq!(
        giggles_core::sqlstate(
            &sql.execute("INSERT INTO int_range VALUES(1,2147483648::int8)", &[], None).unwrap_err()
        ),
        "22003"
    );
}
#[test]
fn legacy_numeric_assignment() {
    let path = common::fresh("native-numeric-legacy");
    exercise(Admin::create(&path, Limits::default()).unwrap());
    std::fs::remove_dir_all(path).unwrap();
}
#[cfg(feature = "engine-rocksdb")]
#[test]
fn v2_numeric_assignment() {
    let path = common::fresh("native-numeric-v2");
    exercise(Admin::create_engine(&path, Limits::default(), giggles_core::Engine::V2).unwrap());
    std::fs::remove_dir_all(path).unwrap();
}
