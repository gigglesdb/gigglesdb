mod common;
use giggles_core::{Admin, Engine, Limits};
use giggles_core::relational::ForeignKeyTarget;
fn exercise(engine: Engine) {
    let path = common::fresh("fk-catalog-target");
    let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
    let mut s = admin.sql_session();
    for sql in ["CREATE TABLE p(id int not null)", "CREATE UNIQUE INDEX s ON p(id)",
        "ALTER TABLE p ADD PRIMARY KEY(id)", "CREATE TABLE c(id int REFERENCES p(id))",
        "CREATE TABLE d(id int REFERENCES p)"] {
        s.execute(sql, &[], None).unwrap();
    }
    let metadata = s.catalog_metadata().unwrap();
    let index = metadata.indexes.iter().find(|i| i.name == "s").unwrap().id;
    let c = metadata.tables.iter().find(|t| t.name == "c").unwrap().id;
    let d = metadata.tables.iter().find(|t| t.name == "d").unwrap().id;
    for (table, expected) in [(c, ForeignKeyTarget::Index(index)), (d, ForeignKeyTarget::PrimaryKey)] {
        let fk = metadata.presentation.constraints.iter().find(|c| c.table == table && c.kind == 'f').unwrap();
        assert_eq!(fk.foreign_key_target, Some(expected));
    }
    drop(s); drop(admin);
    std::fs::remove_dir_all(path).unwrap();
}
#[test]
fn legacy_presents_saved_target() { exercise(Engine::Legacy); }
#[cfg(feature = "engine-rocksdb")]
#[test]
fn v2_presents_saved_target() { exercise(Engine::V2); }
