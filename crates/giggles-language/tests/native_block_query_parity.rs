//! Typed block steps retain one native session, without SQL text parsing.
use giggles_core::{Admin, Engine as Storage, Limits, SqlSession};
use giggles_core::relational::with_sql_parser_forbidden;
use giggles_language::*;
use std::{path::PathBuf, sync::atomic::{AtomicU64, Ordering}};
const VERSION: LanguageVersion = LanguageVersion::Embedded020;
struct Database(PathBuf);
impl Database {
    fn fresh(storage: Storage) -> (Self, Admin) {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!("native-block-query-{}-{}",
            std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)));
        let admin = Admin::create_engine(&path, Limits::default(), storage).unwrap();
        for text in ["create table orders(id bigint primary key,item bigint,qty bigint)",
            "create table items(id bigint primary key)", "create table dst(id bigint primary key,item bigint,qty bigint)",
            "insert into items values(1),(2),(3)", "insert into orders values(1,1,2),(2,1,4),(3,2,null)"] {
            admin.sql_session().execute(text, &[], None).unwrap();
        }
        (Self(path), admin)
    }
}
impl Drop for Database { fn drop(&mut self) { std::fs::remove_dir_all(&self.0).unwrap(); } }
fn storages() -> Vec<Storage> { vec![Storage::Legacy, #[cfg(feature = "engine-rocksdb")] Storage::V2] }
fn snapshot(session: &mut SqlSession) -> String {
    let mut m = session.catalog_metadata().unwrap(); m.presentation.epoch = 0;
    let mut result = format!("{:?}", (&m.tables, &m.indexes, &m.presentation));
    for table in ["orders", "items", "dst"] {
        result.push_str(&format!("{:?}", session.execute(&format!("select * from {table} order by id"), &[], None).unwrap().rows));
    }
    result
}
fn native(engine: &Engine, text: &str) -> Result<TransactionOutcome> {
    with_sql_parser_forbidden(|| engine.execute_transaction_text(text, &Bindings::new()))
}
fn rows(out: &TransactionOutcome, index: usize) -> (&[giggles_core::relational::ColumnDef], &[giggles_core::relational::SqlRow]) {
    let EmittedValue::Rows { columns, rows, complete } = &out.emitted[index] else { panic!("rows expected") };
    assert!(*complete); (columns, rows)
}
#[test]
fn typed_steps_in_blocks_match_sql() {
    for storage in storages() {
        let (_ld, left) = Database::fresh(storage); let (_rd, right) = Database::fresh(storage);
        let engine = Engine::with_version(right.clone(), VERSION); let mut sql = left.sql_session();
        sql.execute("BEGIN", &[], None).unwrap();
        sql.execute("insert into orders values(9,1,5)", &[], None).unwrap();
        let expected = sql.execute("select item,count(*) as n,sum(qty) as total from orders group by item order by item", &[], None).unwrap();
        let inserted = sql.execute("insert into dst(id,item,qty) select id,item,qty from orders where qty>1", &[], None).unwrap();
        sql.execute("COMMIT", &[], None).unwrap();
        let out = native(&engine, "transaction { insert into orders(id,item,qty) values(9,1,5); let g = (orders group by item order by item select item,count(*) as n,sum(qty) as total); insert into dst(id,item,qty) from orders where qty > 1 select id,item,qty; emit @g.rows }").unwrap();
        assert_eq!(rows(&out, 0).1, expected.rows);
        assert_eq!(rows(&out, 0).0.iter().map(|c| c.name.clone()).collect::<Vec<_>>(), expected.columns);
        assert_eq!(out.summaries.iter().map(|s| (s.item_index,s.matched,s.affected,s.changed,s.complete,s.conflict_results.is_none())).collect::<Vec<_>>(),
            [(0,1,1,1,true,true),(1,expected.rows.len(),0,0,true,true),(2,inserted.affected as usize,inserted.affected as usize,inserted.affected as usize,true,true)]);
        assert_eq!(snapshot(&mut sql), snapshot(&mut right.sql_session()));
        let join = sql.execute("select i.id,o.qty from items i left join orders o on i.id=o.item order by i.id,o.qty", &[], None).unwrap();
        let out = native(&engine, "transaction { let j=(items i left join orders o on i.id=o.item order by i.id,o.qty select i.id,o.qty); let r=(orders order by id); emit @r.rows, @j.rows, @r.rows }").unwrap();
        assert_eq!(rows(&out,1).1, join.rows); assert_eq!(out.emitted[0],out.emitted[2]);
        let before = snapshot(&mut sql);
        let out = native(&engine, "transaction read only { let g=(orders select count(*)); emit @g.rows }").unwrap();
        assert_eq!(rows(&out,0).1, sql.execute("select count(*) from orders", &[], None).unwrap().rows);
        assert_eq!(snapshot(&mut right.sql_session()), before);
        sql.execute("BEGIN", &[], None).unwrap(); sql.execute("SAVEPOINT a", &[], None).unwrap();
        let err = sql.execute("insert into dst select * from orders where qty>1", &[], None).unwrap_err();
        assert_eq!(giggles_core::sqlstate(&err), "23505");
        sql.execute("ROLLBACK TO a", &[], None).unwrap();
        let expected = sql.execute("select count(*) from dst", &[], None).unwrap(); sql.execute("COMMIT", &[], None).unwrap();
        let out = native(&engine, "transaction { savepoint a; insert into dst(id,item,qty) from orders where qty>1 select id,item,qty; rollback to a; let g=(dst select count(*)); emit @g.rows }").unwrap();
        assert_eq!(rows(&out,0).1, expected.rows);
        assert_eq!(out.summaries.iter().map(|s| s.item_index).collect::<Vec<_>>(), [3]);
        sql.execute("BEGIN", &[], None).unwrap(); sql.execute("insert into orders values(10,1,6)", &[], None).unwrap();
        let err = sql.execute("insert into dst select * from orders where qty>1", &[], None).unwrap_err(); sql.execute("ROLLBACK", &[], None).unwrap();
        let actual = native(&engine, "transaction { insert into orders(id,item,qty) values(10,1,6); insert into dst(id,item,qty) from orders where qty>1 select id,item,qty }").unwrap_err();
        assert!(matches!(&actual, Error::Sql { sqlstate } if sqlstate == giggles_core::sqlstate(&err)));
        assert_eq!(snapshot(&mut sql), snapshot(&mut right.sql_session()));
    }
}
#[test]
fn typed_steps_in_blocks_are_opt_in() {
    for storage in storages() {
        let (_db, admin) = Database::fresh(storage); let engine = Engine::with_version(admin.clone(), VERSION);
        let before = snapshot(&mut admin.sql_session());
        let block = "transaction { let g=(orders select count(*)); emit @g.rows }";
        assert!(matches!(native(&Engine::with_version(admin.clone(), LanguageVersion::Embedded019), "transaction { let g=(orders select count(*)); emit @g.rows }"), Err(Error::Unsupported)));
        for text in ["transaction { orders select count(*); require true }",
            "transaction { let g=(orders select count(*)); emit 1 }",
            "transaction { let g=(orders select count(*)); emit @g.row }",
            "transaction { let g=(orders select count(*)); orders where id=@g.count select id }",
            "transaction read only { insert into dst(id,item,qty) from orders select id,item,qty }"] {
            assert!(matches!(native(&engine,text), Err(Error::Unsupported)), "{text}");
        }
        with_sql_parser_forbidden(|| {
            assert!(matches!(engine.prepare_transaction_bound_text(block,&Bindings::new()), Err(Error::Unsupported)));
            assert!(matches!(lower_text(block,&Bindings::new(),VERSION), Err(Error::Unsupported)));
        });
        assert_eq!(snapshot(&mut admin.sql_session()), before);
    }
}
