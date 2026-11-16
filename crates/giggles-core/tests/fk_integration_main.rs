mod common;
use giggles_core::{Admin, Engine as Backend, Limits, operations::*, relational::DataType};

fn run(admin: &Admin, sql: &str) {
    admin.sql_session().execute(sql, &[], None).unwrap_or_else(|e| panic!("{sql}: {e:?}"));
}
fn snapshot(admin: &Admin) -> String {
    let mut metadata = admin.sql_session().catalog_metadata().unwrap();
    metadata.presentation.epoch = 0;
    metadata.schemas.epoch = 0;
    metadata.types.epoch = 0;
    format!("{:?}", (metadata.tables, metadata.indexes, metadata.schemas, metadata.types, metadata.typmods, metadata.presentation, admin.sql_session().foreign_key_targets().unwrap()))
}
fn same(native: &Admin, sql: &Admin) {
    assert_eq!(snapshot(native), snapshot(sql));
    assert_eq!(native.sql_session().foreign_key_targets().unwrap(), sql.sql_session().foreign_key_targets().unwrap());
}
fn pair(backend: Backend) -> (Vec<std::path::PathBuf>, Vec<Admin>) {
    let paths: Vec<_> = (0..2).map(|_| common::fresh("fk-integration-main")).collect();
    let admins = paths.iter().map(|p| Admin::create_engine(p, Limits::default(), backend).unwrap()).collect();
    (paths, admins)
}
fn foreign(table: &str, parent: &str, explicit: bool, name: &str) -> CatalogRequest {
    CatalogRequest::AddConstraint { table: table.into(), name: Some(name.into()), constraint: ConstraintSpec::ForeignKey {
        columns: vec!["id".into()], ref_table: parent.into(), ref_columns: explicit.then(|| vec!["id".into()]),
        on_delete: ForeignKeyAction::NoAction, on_update: ForeignKeyAction::NoAction,
    } }
}
fn reopen(paths: &[std::path::PathBuf], admins: Vec<Admin>) -> Vec<Admin> {
    let targets: Vec<_> = admins.iter().map(|a| a.sql_session().foreign_key_targets().unwrap()).collect();
    drop(admins);
    paths.iter().zip(targets).map(|(path, target)| {
        let admin = Admin::open(path, Limits::default()).unwrap();
        assert_eq!(admin.sql_session().foreign_key_targets().unwrap(), target);
        admin
    }).collect()
}
fn exercise(backend: Backend) {
    // PG18.6 basis: r2/r2-33/pg18.txt (creation order and implicit PK selection).
    for qualified in [false, true] { for unique_first in [false, true] {
        let (paths, admins) = pair(backend);
        let prefix = if qualified { "s." } else { "" };
        let p = format!("{prefix}p");
        for admin in &admins {
            if qualified { run(admin, "CREATE SCHEMA s"); }
            run(admin, &format!("CREATE TABLE {p}(id bigint NOT NULL)"));
            let pk = format!("ALTER TABLE {p} ADD CONSTRAINT p_pk PRIMARY KEY(id)");
            let uq = format!("CREATE UNIQUE INDEX p_u ON {p}(id)");
            for ddl in if unique_first { [&uq, &pk] } else { [&pk, &uq] } { run(admin, ddl); }
            for c in ["c0", "c1"] { run(admin, &format!("CREATE TABLE {prefix}{c}(id bigint)")); }
        }
        for explicit in [false, true] {
            let n = usize::from(explicit);
            let c = format!("{prefix}c{n}");
            let name = format!("fk{n}");
            Engine::new(admins[0].clone()).execute_catalog(&foreign(&c, &p, explicit, &name)).unwrap();
            run(&admins[1], &format!("ALTER TABLE {c} ADD CONSTRAINT {name} FOREIGN KEY(id) REFERENCES {p}{}", if explicit { "(id)" } else { "" }));
            same(&admins[0], &admins[1]);
            let expected = if explicit && unique_first { "index:p_u" } else { "pk:p_pk" };
            assert_eq!(admins[0].sql_session().foreign_key_targets().unwrap()[&name], expected);
        }
        let admins = reopen(&paths, admins);
        same(&admins[0], &admins[1]);
        drop(admins);
        for path in paths { std::fs::remove_dir_all(path).unwrap(); }
    } }
    // PG18.6 basis: fkidx/t13/report.md and sql-fixes/report.md.
    let (paths, admins) = pair(backend);
    for admin in &admins {
        for ddl in ["CREATE SCHEMA s", "CREATE TABLE s.p(id bigint NOT NULL)",
            "CREATE UNIQUE INDEX chosen ON s.p(id)", "CREATE TABLE s.c(id bigint)"] { run(admin, ddl); }
    }
    Engine::new(admins[0].clone()).execute_catalog(&foreign("s.c", "s.p", true, "bound_fk")).unwrap();
    run(&admins[1], "ALTER TABLE s.c ADD CONSTRAINT bound_fk FOREIGN KEY(id) REFERENCES s.p(id)");
    same(&admins[0], &admins[1]);
    let admins = reopen(&paths, admins);
    for cascade in [false, true] {
        let before: Vec<_> = admins.iter().map(snapshot).collect();
        let native = Engine::new(admins[0].clone()).execute_catalog(&CatalogRequest::DropIndex { name: "s.chosen".into(), cascade });
        let sql = admins[1].sql_session().execute(if cascade { "DROP INDEX s.chosen CASCADE" } else { "DROP INDEX s.chosen" }, &[], None);
        if cascade {
            native.unwrap(); sql.unwrap();
            assert!(admins[0].sql_session().foreign_key_targets().unwrap().is_empty());
        } else {
            assert_eq!(native.unwrap_err().sqlstate(), Some("2BP01"));
            assert_eq!(giggles_core::sqlstate(&sql.unwrap_err()), "2BP01");
            assert_eq!(admins.iter().map(snapshot).collect::<Vec<_>>(), before);
        }
        same(&admins[0], &admins[1]);
    }
    let create = CatalogRequest::CreateTable { table: "nope.c".into(), columns: vec![ColumnSpec {
        name: "id".into(), data_type: DataType::BigInt, not_null: true,
        default: None, generated: None,
    }], primary_key: vec!["id".into()] };
    // Native CreateTable has no FK field; exercise its supported missing-schema form.
    for (request, ddl) in [
        (create, "CREATE TABLE nope.c(id bigint PRIMARY KEY)"),
        (foreign("s.c", "nope.p", true, "bad"), "ALTER TABLE s.c ADD CONSTRAINT bad FOREIGN KEY(id) REFERENCES nope.p(id)"),
        (foreign("nope.c", "s.p", true, "bad"), "ALTER TABLE nope.c ADD CONSTRAINT bad FOREIGN KEY(id) REFERENCES s.p(id)"),
    ] {
        let before: Vec<_> = admins.iter().map(snapshot).collect();
        let error = Engine::new(admins[0].clone()).execute_catalog(&request).unwrap_err();
        assert_eq!(error.sqlstate(), Some("3F000"), "{request:?}: {error:?}");
        assert_eq!(giggles_core::sqlstate(&admins[1].sql_session().execute(ddl, &[], None).unwrap_err()), "3F000");
        assert_eq!(admins.iter().map(snapshot).collect::<Vec<_>>(), before);
    }
    let before = snapshot(&admins[1]);
    assert_eq!(giggles_core::sqlstate(&admins[1].sql_session().execute("CREATE TABLE bad(id bigint REFERENCES nope.p)", &[], None).unwrap_err()), "3F000");
    assert_eq!(snapshot(&admins[1]), before);
    let admins = reopen(&paths, admins);
    same(&admins[0], &admins[1]);
    drop(admins);
    for path in paths { std::fs::remove_dir_all(path).unwrap(); }
}
#[test]
fn legacy_native_sql_fk_integration() { exercise(Backend::Legacy); }
#[cfg(feature = "engine-rocksdb")]
#[test]
fn v2_native_sql_fk_integration() { exercise(Backend::V2); }
