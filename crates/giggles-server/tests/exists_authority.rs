use giggles_core::{*, roles::{NewRole, TablePrivilege}, relational::Value};
use giggles_server::{Service, clock::Clock};
#[test]
fn exists_authority_precedes_binding_and_rechecks_prepared_reads() {
    for engine in [Engine::Legacy, #[cfg(feature="engine-rocksdb")] Engine::V2] {
        let path = std::env::temp_dir().join(format!("c8-authority-{}-{engine:?}", std::process::id()));
        let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
        admin.roles().create(NewRole { name:"reader".into(), login:true, ..NewRole::default() }).unwrap();
        let mut setup = admin.sql_session();
        for sql in ["CREATE TABLE t(id int PRIMARY KEY)", "CREATE TABLE secret(id int PRIMARY KEY)",
            "INSERT INTO t VALUES(1)", "INSERT INTO secret VALUES(1)"] { setup.execute(sql, &[], None).unwrap(); }
        admin.roles().grant_table("t", "reader", &[TablePrivilege::Select,TablePrivilege::Update,TablePrivilege::Delete]).unwrap();
        let service = Service::new(admin.clone(), vec![], Clock::new(0,3600).unwrap()).unwrap();
        let mut reader = service.role_session("reader").unwrap();
        for sql in ["SELECT id FROM t WHERE EXISTS(SELECT missing FROM secret)",
            "SELECT id FROM t WHERE EXISTS(SELECT 1/0 FROM secret)",
            "SELECT id FROM t WHERE EXISTS(SELECT nosuchfn() FROM secret)",
            "SELECT id FROM t WHERE id IN(SELECT id FROM t WHERE EXISTS(SELECT missing FROM secret))",
            "SELECT id FROM secret o WHERE EXISTS(SELECT o.id)",
            "UPDATE t SET id=2 WHERE EXISTS(SELECT missing FROM secret)",
            "DELETE FROM t WHERE EXISTS(SELECT 1/0 FROM secret)"] {
            assert!(matches!(reader.prepare(sql), Err(CoreError::Policy(Error::Scope))), "{engine:?}: {sql}");
            assert!(matches!(reader.execute_sql(sql,&[]), Err(CoreError::Policy(Error::Scope))), "{engine:?}: {sql}");
        }
        admin.roles().grant_table("secret", "reader", &[TablePrivilege::Select]).unwrap();
        let sql = "SELECT id FROM t WHERE EXISTS(SELECT secret.id FROM secret WHERE secret.id=t.id)";
        let p = reader.prepare(sql).unwrap();
        assert_eq!(reader.execute_prepared(&p,&[]).unwrap().rows[0].values, vec![Value::Int(1)]);
        let projection = reader.prepare("SELECT id FROM t WHERE EXISTS(SELECT id FROM secret)").unwrap();
        assert_eq!(reader.execute_prepared(&projection,&[]).unwrap().rows.len(), 1);
        admin.roles().revoke_table("secret", "reader", &[TablePrivilege::Select]).unwrap();
        assert!(matches!(reader.execute_prepared(&p,&[]), Err(CoreError::Policy(Error::Scope))));
        assert!(matches!(reader.open_cursor(&p,&[]), Err(CoreError::Policy(Error::Scope))));
        assert!(matches!(reader.execute_prepared(&projection,&[]), Err(CoreError::Policy(Error::Scope))));
        // The relation appears only in the target list; EXISTS still checks its authority.
        let p = reader.prepare("SELECT id FROM t WHERE EXISTS(SELECT t.id)").unwrap();
        admin.roles().revoke_table("t", "reader", &[TablePrivilege::Select]).unwrap();
        assert!(matches!(reader.execute_prepared(&p,&[]), Err(CoreError::Policy(Error::Scope))));
        admin.roles().grant_table("secret", "reader", &[TablePrivilege::Select]).unwrap();
        assert_eq!(reader.execute_sql("UPDATE t SET id=2 WHERE EXISTS(SELECT id FROM secret)",&[]).unwrap().affected, 1);
        assert_eq!(reader.execute_sql("DELETE FROM t WHERE EXISTS(SELECT id FROM secret)",&[]).unwrap().affected, 1);
        drop(p); drop(reader); drop(service); drop(setup); drop(admin);
        std::fs::remove_dir_all(path).unwrap();
    }
}
