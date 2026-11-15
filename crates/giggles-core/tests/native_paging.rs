mod common;
use giggles_core::{Admin, Engine as StorageEngine, Limits, operations::*};

fn request() -> Request {
    Request::Read {
        table: "paged".into(), filter: None,
        order: vec![Sort { field: "bucket".into(), descending: true },
            Sort { field: "tenant".into(), descending: false },
            Sort { field: "id".into(), descending: true }],
        limit: Some(ValueExpr::Literal(Value::Int(1024))), projection: vec!["note".into()],
    }
}

fn exercise(storage: StorageEngine) {
    let path = common::fresh("native-paging");
    let admin = Admin::create_engine(&path, Limits::default(), storage).unwrap();
    let engine = Engine::new(admin.clone());
    let bindings = Bindings::new();
    let mut sql = admin.sql_session();
    sql.execute("CREATE TABLE paged (tenant BIGINT, id BIGINT, bucket BIGINT, note TEXT, PRIMARY KEY (tenant,id))", &[], None).unwrap();
    let empty = engine.read_page(&request(), None, &bindings).unwrap();
    assert!(empty.outcome.rows.is_empty() && empty.next.is_none() && empty.outcome.complete);
    for batch in 0..6 {
        let tuples = (batch * 500..(batch + 1) * 500).map(|i| {
            let bucket = if i % 13 == 0 { "NULL".into() } else { (i % 7).to_string() };
            format!("({}, {}, {}, 'row{}')", i % 3, i * 2, bucket, i)
        }).collect::<Vec<_>>().join(",");
        sql.execute(&format!("INSERT INTO paged VALUES {tuples}"), &[], None).unwrap();
    }
    let request = request();
    assert_eq!(engine.execute(&request, &bindings), Err(Error::Limit));
    let expected = sql.execute("SELECT note FROM paged ORDER BY bucket DESC, tenant, id DESC", &[], None).unwrap();
    let mut all = vec![];
    let mut after = None;
    let mut lengths = vec![];
    loop {
        let page = engine.read_page(&request, after.as_deref(), &bindings).unwrap();
        assert_eq!(page.outcome.columns, ["note"]);
        assert_eq!(page.outcome.complete, page.next.is_none());
        lengths.push(page.outcome.rows.len());
        let last_note = page.outcome.rows.last().unwrap().values[0].clone();
        all.extend(page.outcome.rows.into_iter().map(|row| row.values));
        if page.next.is_none() {
            // The final row is (bucket=0, tenant=2, id=28).
            assert_eq!(last_note, Value::Text("row14".into()));
            let empty = engine.read_page(&request, Some(&[Value::Int(0), Value::Int(2), Value::Int(28)]), &bindings).unwrap();
            assert!(empty.outcome.rows.is_empty());
            assert!(empty.next.is_none());
            break;
        }
        after = page.next;
    }
    assert_eq!(lengths, [1024, 1024, 952]);
    assert_eq!(all, expected.rows.into_iter().map(|row| row.values).collect::<Vec<_>>());
    let mut invalid = request.clone();
    if let Request::Read { order, .. } = &mut invalid { order.pop(); }
    assert_eq!(engine.read_page(&invalid, None, &bindings), Err(Error::Bind("paging order must end in the full primary key".into())));
    assert_eq!(engine.read_page(&request, Some(&[]), &bindings), Err(Error::Bind("paging key length".into())));
    assert_eq!(engine.read_page(&request, Some(&[Value::Text("bad".into()), Value::Int(0), Value::Int(0)]), &bindings), Err(Error::Type));
    let mut default_size = request.clone();
    if let Request::Read { limit, .. } = &mut default_size { *limit = None; }
    assert_eq!(engine.read_page(&default_size, None, &bindings).unwrap().outcome.rows.len(), 100);
    let mut filtered = request.clone();
    if let Request::Read { filter, limit, .. } = &mut filtered {
        *filter = Some(Expr::Compare(Box::new(Expr::Field("id".into())), Compare::Lt,
            Box::new(Expr::Value(ValueExpr::Parameter("ceiling".into())))));
        *limit = Some(ValueExpr::Literal(Value::Int(3)));
    }
    let args = Bindings::from([("ceiling".into(), Value::Int(20))]);
    let page = engine.read_page(&filtered, None, &args).unwrap();
    assert_eq!((page.outcome.matched, page.outcome.rows.len()), (10, 3));
    let expected = sql.execute("SELECT note FROM paged WHERE id < 20 ORDER BY bucket DESC, tenant, id DESC LIMIT 3", &[], None).unwrap();
    assert_eq!(page.outcome.rows.iter().map(|r| &r.values).collect::<Vec<_>>(), expected.rows.iter().map(|r| &r.values).collect::<Vec<_>>());
    if let Request::Read { limit, .. } = &mut filtered { *limit = Some(ValueExpr::Literal(Value::Int(0))); }
    let zero = engine.read_page(&filtered, None, &args).unwrap();
    assert!(zero.outcome.rows.is_empty());
    assert!(zero.next.is_none());

    let first = engine.read_page(&request, None, &bindings).unwrap();
    let key = first.next.unwrap();
    // Insert on both sides of the cursor; only the strictly later row is visible.
    let [Value::Int(bucket), Value::Int(tenant), Value::Int(id)] = key.as_slice() else { panic!("non-null cursor") };
    sql.execute(&format!("INSERT INTO paged VALUES ({tenant},{},{bucket},'later'),({tenant},{},{bucket},'earlier')", id - 1, id + 1), &[], None).unwrap();
    let mut session = admin.sql_session();
    session.begin_native().unwrap();
    let second = session.read_page(&request, Some(&key), &bindings).unwrap();
    assert!(session.in_transaction());
    assert!(second.outcome.rows.iter().any(|row| row.values == [Value::Text("later".into())]));
    assert!(!second.outcome.rows.iter().any(|row| row.values == [Value::Text("earlier".into())]));
    session.rollback_native().unwrap();
    session.begin_native().unwrap();
    assert!(session.read_page(&invalid, None, &bindings).is_err());
    assert!(session.is_aborted());
    session.rollback_native().unwrap();
    assert_eq!(engine.execute(&request, &bindings), Err(Error::Limit));
    drop(session);
    drop(sql);
    drop(engine);
    drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn legacy_pages() { exercise(StorageEngine::Legacy); }
#[cfg(feature = "engine-rocksdb")]
#[test]
fn v2_pages() { exercise(StorageEngine::V2); }

#[test]
fn temporal_order_keys() {
    for storage in [StorageEngine::Legacy, #[cfg(feature = "engine-rocksdb")] StorageEngine::V2] {
        let path = common::fresh("native-paging-date");
        let admin = Admin::create_engine(&path, Limits::default(), storage).unwrap();
        let mut sql = admin.sql_session();
        sql.execute("CREATE TABLE dates (id BIGINT PRIMARY KEY, d DATE)", &[], None).unwrap();
        sql.execute("INSERT INTO dates VALUES (1,'2025-01-01'),(2,NULL),(3,'2024-01-01')", &[], None).unwrap();
        let request = Request::Read { table: "dates".into(), filter: None,
            order: vec![Sort { field: "d".into(), descending: false }, Sort { field: "id".into(), descending: false }],
            limit: Some(ValueExpr::Literal(Value::Int(1))), projection: vec!["id".into()] };
        let engine = Engine::new(admin.clone());
        let mut after = None;
        let mut values = vec![];
        loop {
            let page = engine.read_page(&request, after.as_deref(), &Bindings::new()).unwrap();
            values.extend(page.outcome.rows.into_iter().map(|r| r.values));
            after = page.next;
            if after.is_none() { break; }
        }
        assert_eq!(values, sql.execute("SELECT id FROM dates ORDER BY d, id", &[], None).unwrap().rows.into_iter().map(|r| r.values).collect::<Vec<_>>());
        drop(sql); drop(engine); drop(admin);
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn qualified_pages_and_session_search_path() {
    for backend in [StorageEngine::Legacy, #[cfg(feature = "engine-rocksdb")] StorageEngine::V2] {
        let path = common::fresh("qualified-pages");
        let admin = Admin::create_engine(&path, Limits::default(), backend).unwrap();
        let mut sql = admin.sql_session();
        for text in ["CREATE SCHEMA app", "CREATE TABLE app.items(id bigint PRIMARY KEY)",
            "CREATE TABLE public.items(id bigint PRIMARY KEY)", "INSERT INTO app.items VALUES(11)",
            "INSERT INTO public.items VALUES(22)"] { sql.execute(text, &[], None).unwrap(); }
        let read = |table: &str| Request::Read { table: table.into(), filter: None,
            order: vec![Sort { field: "id".into(), descending: false }], limit: None, projection: vec!["id".into()] };
        let engine = Engine::new(admin.clone());
        let bindings = Bindings::new();
        for (table, id) in [("app.items", 11), ("items", 22)] {
            assert_eq!(engine.read_page(&read(table), None, &bindings).unwrap().outcome.rows[0].values, [Value::Int(id)]);
        }
        sql.set_search_path("app, public", "giggles");
        assert_eq!(sql.read_page(&read("items"), None, &bindings).unwrap().outcome.rows[0].values, [Value::Int(11)]);
        let missing = engine.read_page(&read("absent.items"), None, &bindings).unwrap_err();
        assert_eq!(missing.sqlstate(), Some("42P01"));
        assert_eq!(missing.sqlstate(), engine.execute(&read("absent.items"), &bindings).unwrap_err().sqlstate());
        assert_eq!(engine.read_page(&read("App.items"), None, &bindings), Err(Error::Bind("identifier".into())));
        drop(sql); drop(engine); drop(admin);
        std::fs::remove_dir_all(path).unwrap();
    }
}
