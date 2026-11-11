mod common;
use giggles_core::relational::{SqlError, Value};
use giggles_core::{Admin, CoreError, Engine, Limits};

fn exercise(engine: Engine) {
    let path = common::fresh("ctid");
    let admin = Admin::create_engine(&path, Limits::default(), engine).unwrap();
    let mut s = admin.sql_session();
    s.execute("CREATE TABLE table_name (test integer)", &[], None)
        .unwrap();
    s.execute("INSERT INTO table_name VALUES (1),(2),(3)", &[], None)
        .unwrap();
    for sql in [
        "SELECT t.*, CTID FROM public.table_name t LIMIT 501",
        "SELECT t.*, t.ctid FROM public.table_name t",
        "SELECT t.*, \"ctid\" FROM public.table_name t",
    ] {
        let prepared = s.prepare(sql).unwrap();
        let description = s.describe(&prepared).unwrap();
        assert_eq!(description.type_oids, vec![0, 27]);
        assert_eq!(description.typmods[1], -1);
        let result = s.execute_prepared(&prepared, &[], None).unwrap();
        assert_eq!(result.rows.len(), 3);
        let values: Vec<_> = result.rows.iter().map(|r| r.values[1].clone()).collect();
        assert!(values.iter().all(|v| matches!(v, Value::Text(_))));
        assert_ne!(values[0], values[1]);
        assert_ne!(values[1], values[2]);
    }
    assert_eq!(
        s.execute("SELECT * FROM table_name", &[], None)
            .unwrap()
            .columns
            .len(),
        1
    );
    assert_eq!(
        s.execute("SELECT t.* FROM table_name t", &[], None)
            .unwrap()
            .columns
            .len(),
        1
    );
    for sql in [
        "SELECT ctid FROM table_name",
        "SELECT CTID FROM table_name",
        "SELECT \"ctid\" FROM table_name",
        "SELECT t.ctid FROM table_name t",
    ] {
        let prepared = s.prepare(sql).unwrap();
        assert_eq!(s.describe(&prepared).unwrap().type_oids, vec![27]);
        let result = s.execute_prepared(&prepared, &[], None).unwrap();
        assert_eq!(result.columns.len(), 1);
        assert_eq!(result.rows.len(), 3);
        let values = result
            .rows
            .iter()
            .map(|r| format!("{:?}", r.values[0]))
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(values.len(), 3);
    }
    for sql in [
        "CREATE TABLE bad (ctid integer)",
        "CREATE TABLE bad (\"ctid\" integer)",
        "ALTER TABLE table_name ADD COLUMN ctid integer",
    ] {
        assert_eq!(
            s.execute(sql, &[], None),
            Err(CoreError::Sql(SqlError::DuplicateColumn)),
            "{sql}"
        );
    }
    s.execute("CREATE TABLE keyed (id integer PRIMARY KEY)", &[], None)
        .unwrap();
    assert_eq!(
        s.execute("SELECT ctid FROM keyed", &[], None),
        Err(CoreError::Sql(SqlError::Unsupported))
    );
    s.execute("CREATE TYPE mood AS ENUM ('happy')", &[], None)
        .unwrap();
    s.execute("CREATE TABLE moods (m mood)", &[], None).unwrap();
    let plain = s.prepare("SELECT m FROM moods").unwrap();
    let enum_oid = s.describe(&plain).unwrap().type_oids[0];
    assert_ne!(enum_oid, 0);
    let mixed = s.prepare("SELECT m, ctid FROM moods").unwrap();
    assert_eq!(s.describe(&mixed).unwrap().type_oids, vec![enum_oid, 27]);
}

#[test]
fn legacy() {
    exercise(Engine::Legacy);
}

#[cfg(feature = "engine-rocksdb")]
#[test]
fn v2() {
    exercise(Engine::V2);
}
