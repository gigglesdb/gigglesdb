use giggles_core::{Admin, Engine, Limits, roles::NewRole};
use giggles_server::{Service, Request, clock::Clock};
#[test]
fn derived_base_table_requires_select_before_binding() {
    for engine in [Engine::Legacy, #[cfg(feature="engine-rocksdb")] Engine::V2] {
        let path = std::env::temp_dir().join(format!("fix7-authority-{}-{engine:?}", std::process::id()));
        let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
        admin.roles().create(NewRole { name:"reader".into(), login:true, ..NewRole::default() }).unwrap();
        admin.sql_session().execute("CREATE TABLE secret(id int PRIMARY KEY)", &[], None).unwrap();
        let service = Service::new(admin.clone(), vec![], Clock::new(0,3600).unwrap()).unwrap();
        let mut reader = service.role_session("reader").unwrap();
        for sql in ["SELECT count(*) FROM (SELECT id FROM secret LIMIT 1 OFFSET 1) sub", "SELECT count(*) FROM (SELECT missing FROM secret) sub"] {
            assert_eq!(reader.handle(Request::Sql { sql: sql.into(), params: vec![] }).code, "42501");
        }
        drop(reader); drop(service); drop(admin); std::fs::remove_dir_all(path).unwrap();
    }
}
