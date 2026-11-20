//! Differential and physical-input proofs for native V2 access paths.
#![cfg(feature = "engine-rocksdb")]
use giggles_core::{Admin, Engine as StorageEngine, Limits, operations::{force_native_full_scan, native_input_rows}};
use giggles_language::{Bindings, Engine, Error, LanguageVersion, SqlRow, Value};
use std::{path::PathBuf, sync::atomic::{AtomicU64, Ordering}};

struct Database(PathBuf);
impl Database {
    fn fresh() -> (Self, Admin) {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!("native-access-{}-{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)));
        let admin = Admin::create_engine(&path, Limits::default(), StorageEngine::V2).unwrap();
        (Self(path), admin)
    }
}
impl Drop for Database {
    fn drop(&mut self) { std::fs::remove_dir_all(&self.0).unwrap(); }
}
fn sql(admin: &Admin, text: &str) -> Vec<Vec<Value>> {
    values(admin.sql_session().execute(text, &[], None).unwrap_or_else(|e| panic!("{text}: {e:?}")).rows)
}
fn values(rows: Vec<SqlRow>) -> Vec<Vec<Value>> { rows.into_iter().map(|r| r.values).collect() }
fn seed(admin: &Admin) {
    sql(admin, "create table items(id bigint primary key, n bigint, u bigint)");
    let mut writer = admin.sql_session();
    writer.execute("begin", &[], None).unwrap();
    for start in (0..5000).step_by(500) {
        let tuples = (start..start+500).map(|i| format!("({i},{}, {i})", i%10)).collect::<Vec<_>>().join(",");
        writer.execute(&format!("insert into items values{tuples}"), &[], None).unwrap();
    }
    writer.execute("commit", &[], None).unwrap();
    sql(admin, "create table composite(a bigint, b text, n bigint, primary key(a,b))");
    sql(admin, "insert into composite values(1,'x',1),(1,'z',2),(2,'y',3),(3,'q',4)");
    sql(admin, "create table words(id text primary key, n bigint)");
    sql(admin, "insert into words values('a',1),('b',2),('c',3),('z',4)");
    sql(admin, "create table nullable(id bigint primary key, n bigint)");
    sql(admin, "insert into nullable values(1,null),(2,2),(3,null),(4,4)");
}
fn engine(admin: &Admin) -> Engine { Engine::with_version(admin.clone(), LanguageVersion::Embedded021) }

#[test]
fn reads_match_full_scan_and_sql_with_physical_input_proofs() {
    let (_db, admin) = Database::fresh();
    let (_reference_db, reference) = Database::fresh();
    seed(&admin); seed(&reference);
    let engine = engine(&admin);
    let bindings = Bindings::from_iter([("id".into(), Value::Int(7)), ("after".into(), Value::Int(20)), ("max".into(), Value::Int(40))]);
    // Expected decoded rows distinguish physical narrowing from post-scan filtering.
    for (table, predicate, suffix, decoded) in [
        ("items", "id=7", "", 1),
        ("items", "id=$id", "", 1),
        ("items", "7=id", "", 1),
        ("items", "id=9 or id=1 or id=4 or id=9", "", 3),
        ("items", "id=9 or id=1 or id=4 or id=2", "limit 3", 4),
        ("items", "(id=9 or id=1 or id=4) and n>=4", "", 3),
        ("items", "id>=1 and id<=9 and (id=1 or id=9)", "", 2),
        ("items", "id>=20 and id<30", "", 10),
        ("items", "id>19 and id<=29", "", 10),
        ("items", "id>4990", "", 9),
        ("items", "id>=4990", "", 10),
        ("items", "id<10", "", 10),
        ("items", "id<=10", "", 11),
        ("items", "10>id", "", 10),
        ("items", "20<=id and 30>id", "", 10),
        ("items", "id>10 and id<5", "", 0),
        ("items", "id>=10 and id<10", "", 0),
        ("items", "id>5000", "", 0),
        ("items", "id>=20 and id<=40", "order by id desc limit 3", 21),
        ("items", "id>$after and id<=$max", "order by id limit 5", 20),
        ("items", "id>=1 and id<=9 and n=1", "", 9),
        ("items", "id>=2 and (n=1 or n=2)", "limit 3", 4998),
        ("items", "id=1 and id=1", "", 1),
        ("items", "id=1 and id=2", "", 1),
        ("composite", "(a=1 and b=\"x\") or (a=2 and b=\"y\")", "", 2),
        ("composite", "a=1 and b=\"x\"", "", 1),
        ("composite", "a=1 and b=\"x\" and (n=1 or n=2)", "", 1),
        ("words", "id=\"z\" or id=\"a\" or id=\"z\"", "limit 1", 2),
        ("words", "id>\"a\"", "", 4),
        ("nullable", "(id=1 or id=3) and n>=0", "", 2),
        ("nullable", "id>=1 and n is null", "", 4),
        ("items", "id>=0 and id+0>=0", "limit 3", 5000),
        ("items", "id>=0 and n is null", "", 5000),
        ("items", "id>=0 and not (n=1)", "limit 3", 5000),
        ("items", "id>=0 and abs(n)>=0", "limit 3", 5000),
        ("items", "id>=0 and n=u", "limit 3", 5000),
    ] {
        let text = format!("{table} where {predicate} {suffix}");
        let before = native_input_rows().0;
        let actual = engine.execute_text(&text, &bindings_for(&text, &bindings));
        let count = native_input_rows().0-before;
        let full = force_native_full_scan(|| engine.execute_text(&text, &bindings_for(&text, &bindings)));
        assert_eq!(actual, full, "{text}");
        assert_eq!(count, decoded, "{text}");
        let result = actual.unwrap_or_else(|e| panic!("{text}: {e:?}"));
        let predicate = predicate.replace("$id", "7").replace("$after", "20").replace("$max", "40").replace('"', "'");
        let order = if suffix.starts_with("order") { suffix.to_string() } else {
            format!("order by {} {suffix}", if table == "composite" { "a,b" } else { "id" })
        };
        assert_eq!(values(result.rows), sql(&reference, &format!("select * from {table} where {predicate} {order}")), "{text}");
    }
    // Cross-class literals are rejected by binding before any storage visitor.
    let text = "items where id=\"7\"";
    let before = native_input_rows();
    let actual = engine.execute_text(text, &Bindings::new());
    assert!(actual.is_err());
    assert_eq!(actual, force_native_full_scan(|| engine.execute_text(text, &Bindings::new())));
    assert!(matches!(actual, Err(Error::Sql { sqlstate }) if sqlstate == "42883"));
    assert_eq!(native_input_rows(), before);
    let text = "items where id>=20 and id<30 limit 3 select id+1 as next";
    let actual = engine.execute_text(text, &Bindings::new()).unwrap();
    assert_eq!(actual, force_native_full_scan(|| engine.execute_text(text, &Bindings::new()).unwrap()));
    assert_eq!(values(actual.rows), sql(&reference, "select id+1 as next from items where id>=20 and id<30 order by id limit 3"));
}
fn bindings_for(text: &str, bindings: &Bindings) -> Bindings {
    bindings.iter().filter(|(key, _)| text.contains(&format!("${key}"))).map(|(k,v)| (k.clone(), v.clone())).collect()
}

#[test]
fn writes_match_full_scan_and_sql_including_caps_and_claim() {
    let (_left_db, left) = Database::fresh();
    let (_right_db, right) = Database::fresh();
    let (_sql_db, reference) = Database::fresh();
    for admin in [&left, &right, &reference] { seed(admin); }
    for (text, statement, error) in [
        ("update many items where id=9 or id=1 or id=4 or id=9 set n=n+1 at most 3", "update items set n=n+1 where id in (1,4,9)", None),
        ("update many items where id=9 or id=1 or id=4 set n=7 at most 2", "", Some(Error::TooManyMatches)),
        ("update items where id=9 or id=1 set n=8", "", Some(Error::AmbiguousMatch)),
        ("claim items where (id=9 or id=1 or id=4) and n!=8 order by id set n=8", "update items set n=8 where id=1", None),
        ("update items where id=$id set n=4", "update items set n=4 where id=7", None),
        ("delete many items where id=9 or id=1 or id=4 at most 2", "", Some(Error::TooManyMatches)),
        ("delete many items where id=9 or id=1 or id=4 or id=9 at most 3", "delete from items where id in (1,4,9)", None),
        ("update many items where id>=10 and id<20 set n=3 at most 10", "update items set n=3 where id>=10 and id<20", None),
    ] {
        let bindings = if text.contains("$id") { Bindings::from_iter([("id".into(), Value::Int(7))]) } else { Bindings::new() };
        let actual = engine(&left).execute_text(text, &bindings);
        let full = force_native_full_scan(|| engine(&right).execute_text(text, &bindings));
        assert_eq!(actual, full, "{text}");
        if let Some(error) = error { assert_eq!(actual, Err(error), "{text}"); }
        else { actual.unwrap(); sql(&reference, statement); }
        let dump = "select * from items order by id";
        assert_eq!(sql(&left, dump), sql(&right, dump), "{text}");
        assert_eq!(sql(&left, dump), sql(&reference, dump), "{text}");
    }
}

#[test]
fn error_terms_scan_and_hook_restores_on_unwind() {
    let (_db, admin) = Database::fresh(); seed(&admin);
    let engine = engine(&admin);
    // And evaluates both sides: the nonmatching first row still divides by zero.
    let text = "items where id=4999 and 10/n>0";
    let before = native_input_rows().0;
    let actual = engine.execute_text(text, &Bindings::new());
    assert_eq!(actual, Err(Error::DivisionByZero));
    assert_eq!(native_input_rows().0-before, 1);
    assert_eq!(actual, force_native_full_scan(|| engine.execute_text(text, &Bindings::new())));
    let (_reference_db, reference) = Database::fresh(); seed(&reference);
    let text = "items where id=0 and 10/n>0";
    let actual = engine.execute_text(text, &Bindings::new());
    assert_eq!(actual, force_native_full_scan(|| engine.execute_text(text, &Bindings::new())));
    assert_eq!(actual, Err(Error::DivisionByZero));
    assert!(matches!(reference.sql_session().execute("select * from items where id=0 and 10/n>0", &[], None),
        Err(giggles_core::CoreError::Sql(giggles_core::relational::SqlError::DivisionByZero))));
    let _ = std::panic::catch_unwind(|| force_native_full_scan(|| panic!("restore test")));
    let before = native_input_rows().0;
    engine.execute_text("items where id=7", &Bindings::new()).unwrap();
    assert_eq!(native_input_rows().0-before, 1);
}

#[test]
fn secondary_index_reads_match_scan_and_sql() {
    let (_db, admin) = Database::fresh();
    let (_reference_db, reference) = Database::fresh();
    for db in [&admin, &reference] {
        sql(db, "create table users(id bigint primary key, email text unique not null, grp bigint, name text)");
        let mut writer = db.sql_session();
        writer.execute("begin", &[], None).unwrap();
        for start in (0..3000).step_by(300) {
            let tuples = (start..start+300).rev().map(|id|
                format!("({id},'user{:04}',{},'name{}')", 2999-id, id%50, id%3)
            ).collect::<Vec<_>>().join(",");
            writer.execute(&format!("insert into users values{tuples}"), &[], None).unwrap();
        }
        writer.execute("commit", &[], None).unwrap();
        sql(db, "create index on users(grp)");
    }
    let engine = engine(&admin);
    let bindings = Bindings::from_iter([("email".into(), Value::Text("user2992".into()))]);
    for (predicate, suffix, decoded) in [
        ("email=$email", "", 1),
        ("email=\"user2992\"", "", 1),
        ("email=\"missing\"", "", 0),
        ("grp=7", "", 60),
        ("grp=7", "limit 3", 60),
        ("grp=7", "order by name desc limit 4", 60),
        ("grp>=10 and grp<12", "", 120),
        ("grp=7 and name=\"name1\"", "", 60),
        ("email>\"user2\"", "", 1000),
        ("grp=7 or grp=8", "", 3000),
        ("grp=7 and name is null", "", 3000),
        ("grp=7 and id+0>=0", "limit 3", 3000),
        ("7=grp", "limit 2", 60),
        ("grp>47", "order by name limit 5", 120),
        ("grp<=1", "limit 4", 120),
        ("grp=7 and (name=\"name1\" or name=\"name2\")", "limit 3", 60),
        ("grp=7 and grp=8", "", 0),
        ("id>=7 and id<10 and grp=7", "", 3),
    ] {
        let text = format!("users where {predicate} {suffix}");
        let bindings = bindings_for(&text, &bindings);
        let before = native_input_rows().0;
        let actual = engine.execute_text(&text, &bindings);
        assert_eq!(native_input_rows().0-before, decoded, "{text}");
        assert_eq!(actual, force_native_full_scan(|| engine.execute_text(&text, &bindings)), "{text}");
        let predicate = predicate.replace("$email", "'user2992'").replace('"', "'");
        let order = if suffix.starts_with("order") {
            // Native resolves caller-order ties by PK, including before LIMIT.
            suffix.replace(" limit", ",id limit")
        } else { format!("order by id {}", if suffix.is_empty() { "limit 100" } else { suffix }) };
        assert_eq!(values(actual.unwrap().rows), sql(&reference,
            &format!("select * from users where {predicate} {order}")), "{text}");
    }
    let text = "users where grp=7 limit 3 select id+1 as next";
    let before = native_input_rows().0;
    let actual = engine.execute_text(text, &Bindings::new());
    assert_eq!(native_input_rows().0-before, 60);
    assert_eq!(actual, force_native_full_scan(|| engine.execute_text(text, &Bindings::new())));
    assert_eq!(values(actual.unwrap().rows), sql(&reference,
        "select id+1 as next from users where grp=7 order by id limit 3"));
}

#[test]
fn bench_http_read_shapes_visit_only_access_candidates() {
    let (_db, admin) = Database::fresh();
    sql(&admin, "create table users(id bigserial primary key, email text unique not null, name text, created_at timestamptz default now(), active boolean default true, version integer default 0)");
    let mut writer = admin.sql_session();
    writer.execute("begin", &[], None).unwrap();
    for start in (1..=1000).step_by(100) {
        let tuples = (start..start+100).map(|id|
            format!("({id},'user{id}@example.com','User {id}',timestamptz '2026-01-01 00:00:00+00',true,0)")
        ).collect::<Vec<_>>().join(",");
        writer.execute(&format!("insert into users(id,email,name,created_at,active,version) values{tuples}"), &[], None).unwrap();
    }
    writer.execute("commit", &[], None).unwrap();
    let engine = engine(&admin);
    // HTTP native JSON {"intValue":"7"} -> proto IntValue(7) -> Value::Int(7),
    // and textValue -> Value::Text, through native_codec::bindings.
    let bindings = Bindings::from_iter([
        ("id".into(), Value::Int(7)),
        ("email".into(), Value::Text("user7@example.com".into())),
        ("after".into(), Value::Int(20)), ("max".into(), Value::Int(40)),
        ("lo".into(), Value::Int(20)), ("hi".into(), Value::Int(40)),
    ]);
    // Variants checked against native_access / primary_native_access: PK
    // points win before integer ranges; unique email uses the secondary index.
    for (shape, text, access, visited, matched, returned, complete) in [
        ("pk", "users where id=$id", "Keys", 1, 1, 1, true),
        ("email", "users where email=$email", "Index", 1, 1, 1, true),
        ("keyset", "users where id>$after and id<=$max order by id limit 5 select id", "Range", 20, 20, 5, false),
        ("range_in", "users where id>=$lo and id<=$hi and (id=$lo or id=$hi) select id", "Keys", 2, 2, 2, true),
    ] {
        let bindings = bindings_for(text, &bindings);
        let before = native_input_rows().0;
        let actual = engine.execute_text(text, &bindings).unwrap();
        let decoded = native_input_rows().0 - before;
        eprintln!("bench {shape}: NativeAccess::{access}, visited={decoded}, matched={}, returned={}, complete={}", actual.matched, actual.rows.len(), actual.complete);
        assert_eq!(decoded, visited, "{shape}");
        assert_eq!((actual.matched, actual.rows.len(), actual.complete), (matched, returned, complete), "{shape}");
        assert_eq!(actual, force_native_full_scan(|| engine.execute_text(text, &bindings).unwrap()), "{shape}");
    }
}
