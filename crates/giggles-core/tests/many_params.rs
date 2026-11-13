use giggles_core::{Admin, Engine, Limits, relational::Value};
#[test]
fn parameter_sizes_and_timing() {
 for engine in [Engine::Legacy, #[cfg(feature="engine-rocksdb")] Engine::V2] {
 let path=std::env::temp_dir().join(format!("f13-{}-{engine:?}",std::process::id()));
 let admin=Admin::create_engine(&path,Limits::default(),engine).unwrap();
 let mut s=admin.sql_session();
 s.execute("CREATE TABLE t(id int PRIMARY KEY)",&[],None).unwrap();
 s.execute("INSERT INTO t VALUES(1),(2)",&[],None).unwrap();
 for n in [257,1000,2044] {
 let sql=format!("SELECT id FROM t WHERE id IN ({})",(1..=n).map(|i|format!("${i}")).collect::<Vec<_>>().join(","));
 if engine==Engine::Legacy && (sql.len()>65536 || n*9>65536) { if sql.len()>65536 { assert!(s.prepare(&sql).is_err()); } continue; }
 let params=vec![Value::Int(1);n];let start=std::time::Instant::now();
 let p=s.prepare(&sql).unwrap();let rows=s.execute_prepared(&p,&params,None).unwrap();
 eprintln!("F13 {engine:?} N={n} prepare+execute {:?}",start.elapsed());assert_eq!(rows.rows.len(),1);
 }
 let sql=format!("SELECT id FROM t WHERE id IN ({})",(1..=2045).map(|i|format!("${i}")).collect::<Vec<_>>().join(","));
 assert!(s.prepare(&sql).is_err());
 if engine==Engine::Legacy {
 let p=s.prepare("SELECT $1").unwrap();
 assert!(s.execute_prepared(&p,&[Value::Text("x".repeat(65537))],None).is_err());
 }
 let deep=format!("SELECT {}1{}", "(".repeat(40), ")".repeat(40));
 assert!(s.prepare(&deep).is_err());
 let over=format!("SELECT id FROM t WHERE id IN ({})",vec!["1";70000].join(","));
 if let Ok(p)=s.prepare(&over) { assert!(s.execute_prepared(&p,&[],None).is_err()); }
 assert!(s.prepare("SELECT $65536").is_err());
 drop(s);drop(admin);std::fs::remove_dir_all(path).unwrap();
 }
}
