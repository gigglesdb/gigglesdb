//! Language reads of views match SQL `SELECT ... FROM <view>` (GD-0001 R2).
use giggles_core::{Admin, Engine as Storage, Limits};
use giggles_language::*;
use std::{path::PathBuf, sync::atomic::{AtomicU64, Ordering}};
const V: LanguageVersion = LanguageVersion::Embedded020;
struct Database(PathBuf);
impl Drop for Database { fn drop(&mut self) { let _ = std::fs::remove_dir_all(&self.0); } }

#[test]
fn view_reads_with_expressions_match_sql() {
    for storage in [Storage::Legacy, #[cfg(feature = "engine-rocksdb")] Storage::V2] {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!("parity-view-reads-{}-{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)));
        let admin = Admin::create_engine(&path, Limits::default(), storage).unwrap();
        let _db = Database(path);
        let mut s = admin.sql_session();
        s.execute("create table orders(id bigint primary key, qty bigint)", &[], None).unwrap();
        s.execute("insert into orders values(1,5),(2,null),(3,7)", &[], None).unwrap();
        let created = s.execute("create view v as select id, qty from orders", &[], None);
        assert_eq!(created.is_ok(), storage == Storage::V2);
        let engine = Engine::with_version(admin.clone(), V);
        for (native, sql) in [
            ("v", "select * from v"),
            ("v select qty*2 as d", "select qty*2 as d from v"),
            ("v where qty > 4 order by id desc limit 1 select id, qty+1 as e", "select id, qty+1 as e from v where qty > 4 order by id desc limit 1"),
            ("v select qty/0 as z", "select qty/0 as z from v"),
        ] {
            match (engine.execute_text(native, &Default::default()), s.execute(sql, &[], None)) {
                (Ok(native), Ok(sql)) => {
                    assert_eq!(native.columns, sql.columns, "{native:?}");
                    let values = |rows: Vec<giggles_core::relational::SqlRow>| rows.into_iter().map(|r| r.values).collect::<Vec<_>>();
                    assert_eq!(values(native.rows), values(sql.rows));
                }
                (Err(native), Err(error)) => assert_eq!(native.sqlstate(), Some(giggles_core::sqlstate(&error)), "{sql}"),
                (native, sql) => panic!("{sql:?} vs {native:?}"),
            }
        }
    }
}
