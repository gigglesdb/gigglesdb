mod common;
use giggles_core::{Admin, CoreError, Limits, SqlSession, operations::{Bindings, CatalogRequest as C, ColumnDefault, ColumnSpec, Engine, Error, Generated, Request, SequenceCall as S}, relational::{DataType, Value}};
fn sql(s: &mut SqlSession, text: &str) -> Result<i64, Error> {
    let clock = giggles_core::relational::StatementClock { unavailable: None, transaction_us: 100, statement_us: 100, zone: None, wall: None };
    s.execute_at(text, &[], None, &clock).map(|r| match r.rows.first().and_then(|r| r.values.first()) { Some(Value::Int(v)) => *v, _ => 0 }).map_err(|e| match e { CoreError::Sql(e) => Error::Sql { sqlstate: giggles_core::sqlstate(&CoreError::Sql(e)).into() }, other => panic!("{other:?}") })
}
fn err(code: &str) -> Error { Error::Sql { sqlstate: code.into() } }
fn create(name: &str) -> C { C::CreateSequence { name: name.into() } }
fn drop_sequence(name: &str, cascade: bool) -> C { C::DropSequence { name: name.into(), cascade } }
fn run(backend: giggles_core::Engine) {
    let path = common::fresh("native-sequences");
    let admin = Admin::create_engine(&path, Limits::default(), backend).unwrap();
    let engine = Engine::new(admin.clone());
    let mut native = admin.sql_session();
    let mut reference = admin.sql_session();
    engine.execute_sequence_catalog(&create("sq")).unwrap();
    sql(&mut reference, "CREATE SEQUENCE reference").unwrap();
    assert_eq!(engine.execute_catalog(&create("unused")), Err(Error::Unsupported));
    assert_eq!(engine.prepare_catalog_bound(&create("unused")).unwrap_err(), Error::Unsupported);
    for (call, text, expected) in [
        (S::Currval("sq".into()), "SELECT currval('reference')", Err(err("55000"))),
        (S::Nextval("sq".into()), "SELECT nextval('reference')", Ok(1)),
        (S::Currval("sq".into()), "SELECT currval('reference')", Ok(1)),
    ] {
        let actual = native.execute_sequence_native(&call);
        if expected == Err(err("22003")) { assert_eq!(actual, Err(Error::Overflow)); }
        assert_eq!(actual.map_err(|e| err(e.sqlstate().unwrap())), expected);
        assert_eq!(sql(&mut reference, text), expected);
    }
    native.begin_native().unwrap(); sql(&mut reference, "BEGIN").unwrap();
    assert_eq!(native.execute_sequence_native(&S::Nextval("sq".into())), Ok(2));
    assert_eq!(sql(&mut reference, "SELECT nextval('reference')"), Ok(2));
    native.rollback_native().unwrap(); sql(&mut reference, "ROLLBACK").unwrap();
    for (call, text, expected) in [
        (S::Nextval("sq".into()), "SELECT nextval('reference')", Ok(3)),
        (S::Setval { name: "sq".into(), value: 100, is_called: None }, "SELECT setval('reference',100)", Ok(100)),
        (S::Nextval("sq".into()), "SELECT nextval('reference')", Ok(101)),
        (S::Setval { name: "sq".into(), value: 200, is_called: Some(false) }, "SELECT setval('reference',200,false)", Ok(200)),
        (S::Nextval("sq".into()), "SELECT nextval('reference')", Ok(200)),
        (S::Setval { name: "sq".into(), value: 0, is_called: None }, "SELECT setval('reference',0)", Err(err("22003"))),
        (S::Nextval("missing".into()), "SELECT nextval('missing')", Err(err("42P01"))),
        (S::Currval("missing".into()), "SELECT currval('missing')", Err(err("42P01"))),
        (S::Setval { name: "missing".into(), value: 1, is_called: None }, "SELECT setval('missing',1)", Err(err("42P01"))),
    ] {
        let actual = native.execute_sequence_native(&call);
        if expected == Err(err("22003")) { assert_eq!(actual, Err(Error::Overflow)); }
        assert_eq!(actual.map_err(|e| err(e.sqlstate().unwrap())), expected);
        assert_eq!(sql(&mut reference, text), expected);
    }
    assert_eq!(engine.execute_sequence_catalog(&create("sq")), Err(err("42P07")));
    assert_eq!(sql(&mut reference, "CREATE SEQUENCE reference"), Err(err("42P07")));
    assert_eq!(engine.execute_sequence_catalog(&drop_sequence("missing", false)), Err(err("42P01")));
    assert_eq!(sql(&mut reference, "DROP SEQUENCE missing"), Err(err("42P01")));
    for name in ["limited", "limited_ref"] { sql(&mut reference, &format!("CREATE SEQUENCE {name} MAXVALUE 2")).unwrap(); }
    for expected in [Ok(1), Ok(2), Err(err("2200H"))] {
        assert_eq!(native.execute_sequence_native(&S::Nextval("limited".into())), expected);
        assert_eq!(sql(&mut reference, "SELECT nextval('limited_ref')"), expected);
    }
    engine.execute_catalog(&C::CreateTable { table: "items".into(), columns: vec![
        ColumnSpec { name: "id".into(), data_type: DataType::BigInt, not_null: true, default: Some(ColumnDefault::Nextval("sq".into())), generated: None },
        ColumnSpec { name: "marker".into(), data_type: DataType::BigInt, not_null: false, default: None, generated: None }
    ], primary_key: vec!["id".into()] }).unwrap();
    sql(&mut reference, "CREATE TABLE ref_items (id BIGINT PRIMARY KEY DEFAULT nextval('reference'), marker BIGINT)").unwrap();
    engine.execute_catalog(&C::CreateTable { table: "owned".into(), columns: vec![ColumnSpec { name: "id".into(), data_type: DataType::BigInt, not_null: true, default: None, generated: Some(Generated::Serial) }], primary_key: vec!["id".into()] }).unwrap();
    engine.execute(&Request::Insert { table: "items".into(), values: vec![("marker".into(), giggles_core::operations::ValueExpr::Literal(Value::Int(7)))] }, &Bindings::new()).unwrap();
    sql(&mut reference, "INSERT INTO ref_items (marker) VALUES (7)").unwrap();
    assert_eq!(sql(&mut native, "SELECT id FROM items"), Ok(201));
    assert_eq!(sql(&mut reference, "SELECT id FROM ref_items"), Ok(201));
    assert_eq!(engine.execute_sequence_catalog(&drop_sequence("sq", false)), Err(err("2BP01")));
    assert_eq!(sql(&mut reference, "DROP SEQUENCE reference"), Err(err("2BP01")));
    engine.execute_sequence_catalog(&drop_sequence("sq", true)).unwrap();
    sql(&mut reference, "DROP SEQUENCE reference CASCADE").unwrap();
    assert_eq!(native.execute_sequence_native(&S::Nextval("sq".into())), Err(err("42P01")));
    assert_eq!(sql(&mut reference, "SELECT nextval('reference')"), Err(err("42P01")));
    // The wire information_schema.columns adapter consumes these default fields.
    let presentation = admin.catalog_presentation().unwrap();
    for table in admin.catalog().unwrap().1.iter().filter(|t| ["items", "ref_items"].contains(&t.name.as_str())) {
        assert!(!presentation.defaults.contains_key(&table.columns[0].id));
    }
    assert_eq!(engine.execute_sequence_catalog(&drop_sequence("owned_id_seq", false)), Err(err("2BP01")));
    sql(&mut reference, "CREATE TABLE ref_owned (id BIGSERIAL PRIMARY KEY)").unwrap();
    assert_eq!(sql(&mut reference, "DROP SEQUENCE ref_owned_id_seq"), Err(err("2BP01")));
    engine.execute_sequence_catalog(&create("reopen")).unwrap();
    sql(&mut reference, "CREATE SEQUENCE reopen_ref").unwrap();
    assert_eq!(engine.execute_sequence(&S::Nextval("reopen".into())), Ok(1));
    assert_eq!(sql(&mut reference, "SELECT nextval('reopen_ref')"), Ok(1));
    assert_eq!(engine.execute_sequence(&S::Currval("reopen".into())), Err(err("55000")));
    assert_eq!(sql(&mut admin.sql_session(), "SELECT currval('reopen_ref')"), Err(err("55000")));
    for name in ["Bad", "a.b.c", ""] {
        assert_eq!(engine.execute_sequence_catalog(&create(name)), Err(Error::Bind("identifier".into())));
        assert_eq!(engine.execute_sequence(&S::Nextval(name.into())), Err(Error::Bind("identifier".into())));
    }
    drop(native); drop(reference); drop(engine); drop(admin);
    let admin = Admin::open(&path, Limits::default()).unwrap();
    let engine = Engine::new(admin.clone());
    let next = engine.execute_sequence(&S::Nextval("reopen".into())).unwrap();
    assert!(next > 1);
    assert_eq!(sql(&mut admin.sql_session(), "SELECT nextval('reopen_ref')"), Ok(next));
}
#[test]
fn legacy() { run(giggles_core::Engine::Legacy); }
#[cfg(feature = "engine-rocksdb")]
#[test]
fn v2() { run(giggles_core::Engine::V2); }
#[test]
fn strict_decode() {
    for text in [r#"{"CreateSequence":{"name":"s","extra":true}}"#, r#"{"DropSequence":{"name":"s","cascade":false,"extra":true}}"#] {
        assert!(serde_json::from_str::<C>(text).is_err());
    }
}

#[test]
fn qualified_sequences_match_sql() {
    for backend in [giggles_core::Engine::Legacy, giggles_core::Engine::V2] {
        if backend == giggles_core::Engine::V2 && !cfg!(feature = "engine-rocksdb") { continue; }
        let admin = Admin::create_engine(common::fresh("qualified-sequences"), Limits::default(), backend).unwrap();
        let engine = Engine::new(admin.clone());
        let mut native = admin.sql_session(); let mut reference = admin.sql_session();
        sql(&mut reference, "CREATE SCHEMA app").unwrap();
        for name in ["app.s", "public.s"] { engine.execute_sequence_catalog(&create(name)).unwrap(); }
        for name in ["app.r", "public.r"] { sql(&mut reference, &format!("CREATE SEQUENCE {name}")).unwrap(); }
        native.set_search_path("app, public", "giggles"); reference.set_search_path("app, public", "giggles");
        for (call, text, expected) in [
            (S::Currval("app.s".into()), "SELECT currval('app.r')", Err(err("55000"))),
            (S::Setval { name: "app.s".into(), value: 41, is_called: Some(false) }, "SELECT setval('app.r',41,false)", Ok(41)),
            (S::Currval("s".into()), "SELECT currval('r')", Err(err("55000"))),
            (S::Nextval("s".into()), "SELECT nextval('r')", Ok(41)),
            (S::Currval("app.s".into()), "SELECT currval('app.r')", Ok(41)),
            (S::Nextval("public.s".into()), "SELECT nextval('public.r')", Ok(1)),
            (S::Setval { name: "s".into(), value: 100, is_called: None }, "SELECT setval('r',100)", Ok(100)),
            (S::Currval("s".into()), "SELECT currval('r')", Ok(100)),
            (S::Nextval("nope.s".into()), "SELECT nextval('nope.s')", Err(err("42P01"))),
            (S::Nextval("app.missing".into()), "SELECT nextval('app.missing')", Err(err("42P01"))),
        ] {
            assert_eq!(native.execute_sequence_native(&call), expected, "{call:?}");
            assert_eq!(sql(&mut reference, text), expected, "{text}");
        }
        assert_eq!(engine.execute_sequence_catalog(&create("nope.s")), Err(err("3F000")));
        assert_eq!(sql(&mut reference, "CREATE SEQUENCE nope.s"), Err(err("3F000")));
        // Repo SQL reports a missing relation (42P01); PG18 reports missing schema (3F000).
        assert_eq!(engine.execute_sequence_catalog(&drop_sequence("nope.s", false)), Err(err("42P01")));
        assert_eq!(sql(&mut reference, "DROP SEQUENCE nope.s"), Err(err("42P01")));
        engine.execute_sequence_catalog(&drop_sequence("app.s", false)).unwrap();
        sql(&mut reference, "DROP SEQUENCE app.r").unwrap();
        assert_eq!(native.execute_sequence_native(&S::Nextval("s".into())), Ok(2));
        assert_eq!(sql(&mut reference, "SELECT nextval('r')"), Ok(2));
        for name in ["app.s.x", "App.s", "app.", ".s"] {
            for request in [create(name), drop_sequence(name, false)] {
                assert_eq!(engine.execute_sequence_catalog(&request), Err(Error::Bind("identifier".into())));
            }
            for call in [S::Nextval(name.into()), S::Currval(name.into()), S::Setval { name: name.into(), value: 1, is_called: None }] {
                assert_eq!(native.execute_sequence_native(&call), Err(Error::Bind("identifier".into())));
            }
        }
    }
}
