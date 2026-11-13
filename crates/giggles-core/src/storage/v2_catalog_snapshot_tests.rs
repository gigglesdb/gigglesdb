use super::*;
use crate::{Admin, Engine, Limits, relational::Value};
use std::{path::PathBuf, sync::Barrier, thread};

struct Fixture {
    admin: Option<Admin>,
    path: PathBuf,
}
impl Fixture {
    fn new() -> Self {
        let mut nonce = [0; 16];
        getrandom::fill(&mut nonce).unwrap();
        let path =
            std::env::temp_dir().join(format!("f7b-catalog-{:032x}", u128::from_be_bytes(nonce)));
        Self {
            admin: Some(Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap()),
            path,
        }
    }
    fn admin(&self) -> &Admin {
        self.admin.as_ref().unwrap()
    }
    fn db(&self) -> &V2Db {
        match &self.admin().inner.backend {
            super::super::Backend::V2(db) => db,
            _ => unreachable!(),
        }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.admin.take();
        std::fs::remove_dir_all(&self.path).unwrap();
    }
}

#[test]
fn drop_schema_with_persisted_view_reports_dependency() {
    use engine::catalog_codec::sql_views::{SqlViewColumn, SqlViewDef, SqlViewDependency};
    let f = Fixture::new(); let mut sql = f.admin().sql_session();
    sql.execute("CREATE SCHEMA s", &[], None).unwrap();
    sql.execute("CREATE TABLE base(c bigint)", &[], None).unwrap();
    let db = f.db(); let mut tx = db.begin(engine::txn::Isolation::ReadCommitted);
    let old = db.catalog().parts(&mut tx).unwrap(); let mut new = old.clone();
    let ids = db.catalog().reserve_ids(2).unwrap();
    let base = &old.tables["base"]; let source = (base.id, base.columns[0].id);
    new.sql_views.insert(ids.start, SqlViewDef { schema: old.ext.schemas["s"].id, name: "v".into(),
        columns: vec![SqlViewColumn { id: ids.start+1, name: "c".into(), storage_type: crate::relational::DataType::BigInt, pg_type_oid: 20, typmod: -1, source: Some(source) }],
        select: "SELECT c FROM base".into(), dependencies: vec![SqlViewDependency { relation: source.0, columns: vec![source.1] }] });
    db.catalog().apply_diff(&mut tx, &old, &new, &ids, None, &|| false).unwrap(); db.catalog().commit_ddl(tx).unwrap();
    assert_eq!(sql.execute("DROP SCHEMA s", &[], None).unwrap_err(), CoreError::Sql(crate::relational::SqlError::DependentObjects));
}

#[test]
fn borrowed_metadata_preserves_statement_epochs_and_matches_owned_images() {
    let f = Fixture::new();
    let admin = f.admin();
    let mut sql = admin.sql_session();
    sql.execute(
        "CREATE TABLE base(id bigint PRIMARY KEY, label varchar(12))",
        &[],
        None,
    )
    .unwrap();
    sql.execute("CREATE INDEX base_label ON base(label)", &[], None)
        .unwrap();
    sql.execute("INSERT INTO base VALUES(1,'before')", &[], None)
        .unwrap();
    let db = f.db();
    let mut tx = db.begin(engine::txn::Isolation::ReadCommitted);
    let before = db.catalog().snapshot(&mut tx).unwrap().image().unwrap();
    let epoch = db.sequencer.visible_ts().0;
    assert_eq!(admin.catalog().unwrap().0, epoch);
    sql.execute("UPDATE base SET label='after' WHERE id=1", &[], None)
        .unwrap();
    let epoch = db.sequencer.visible_ts().0;
    let mut tx2 = db.begin(engine::txn::Isolation::ReadCommitted);
    assert!(Arc::ptr_eq(
        &before,
        &db.catalog().snapshot(&mut tx2).unwrap().image().unwrap()
    ));
    assert_eq!(admin.catalog().unwrap().0, epoch);
    assert_eq!(admin.catalog_generation().unwrap().0, epoch);
    assert_eq!(admin.catalog_schema_metadata().unwrap().epoch, epoch);
    assert_eq!(admin.catalog_presentation().unwrap().epoch, epoch);
    assert_eq!(admin.secondary_index_catalog().unwrap().0, epoch);
    assert_eq!(sql.schema_metadata().unwrap().epoch, epoch);
    assert_eq!(sql.user_type_catalog().unwrap().epoch, epoch);
    let owned = db.metadata_state(None).unwrap();
    let mut expected = (*before).clone();
    expected.epoch = epoch;
    assert_eq!(owned.sql, expected);
    let p = sql.prepare("SELECT label FROM base WHERE id=1").unwrap();
    assert_eq!(sql.describe(&p).unwrap().columns.len(), 1);
    sql.validate_immutable_constants(&p).unwrap();
    let mut cursor = sql.open_cursor(&p, &[]).unwrap();
    assert_eq!(
        cursor.fetch(10, 4096).unwrap()[0].values,
        vec![Value::Text("after".into())]
    );
}

#[test]
fn sql_private_ddl_and_savepoint_rollback_do_not_publish_or_reuse_private_images() {
    let f = Fixture::new();
    let mut own = f.admin().sql_session();
    let other = f.admin().sql_session();
    own.execute("CREATE TABLE base(id bigint PRIMARY KEY)", &[], None)
        .unwrap();
    own.execute("BEGIN", &[], None).unwrap();
    own.execute("SAVEPOINT outer_point", &[], None).unwrap();
    own.execute(
        "CREATE TABLE private_table(id bigint PRIMARY KEY)",
        &[],
        None,
    )
    .unwrap();
    own.execute("SAVEPOINT created", &[], None).unwrap();
    own.execute(
        "ALTER TABLE private_table ADD COLUMN extra bigint",
        &[],
        None,
    )
    .unwrap();
    let p = own.prepare("SELECT * FROM private_table").unwrap();
    assert_eq!(own.describe(&p).unwrap().columns.len(), 2);
    assert!(!other.relation_exists("private_table").unwrap());
    own.execute("ROLLBACK TO created", &[], None).unwrap();
    let p = own.prepare("SELECT * FROM private_table").unwrap();
    assert_eq!(own.describe(&p).unwrap().columns.len(), 1);
    assert!(
        own.open_cursor(&p, &[])
            .unwrap()
            .fetch(10, 4096)
            .unwrap()
            .is_empty()
    );
    own.execute("DROP TABLE base", &[], None).unwrap();
    assert!(!own.relation_exists("base").unwrap());
    assert!(other.relation_exists("base").unwrap());
    own.execute("ROLLBACK TO outer_point", &[], None).unwrap();
    assert!(!own.relation_exists("private_table").unwrap());
    assert!(own.relation_exists("base").unwrap());
    own.execute("ALTER TABLE base ADD COLUMN extra bigint", &[], None)
        .unwrap();
    own.execute("COMMIT", &[], None).unwrap();
    let mut other = other;
    let p = other.prepare("SELECT * FROM base").unwrap();
    assert_eq!(other.describe(&p).unwrap().columns.len(), 2);
}

#[test]
fn sql_concurrent_ddl_and_point_dml_do_not_observe_stale_catalog_images() {
    let f = Fixture::new();
    let admin = f.admin();
    let mut setup = admin.sql_session();
    setup
        .execute(
            "CREATE TABLE base(id bigint PRIMARY KEY, value bigint)",
            &[],
            None,
        )
        .unwrap();
    for id in 1..=4 {
        setup
            .execute(&format!("INSERT INTO base VALUES({id},0)"), &[], None)
            .unwrap();
    }
    let start = Barrier::new(5);
    thread::scope(|scope| {
        let start = &start;
        scope.spawn(move || {
            let mut sql = admin.sql_session();
            start.wait();
            for n in 0..12 {
                sql.execute(
                    &format!("CREATE TABLE changing_{n}(id bigint PRIMARY KEY)"),
                    &[],
                    None,
                )
                .unwrap();
                sql.execute(
                    &format!("ALTER TABLE changing_{n} ADD COLUMN extra bigint"),
                    &[],
                    None,
                )
                .unwrap();
                sql.execute(&format!("DROP TABLE changing_{n}"), &[], None)
                    .unwrap();
            }
        });
        for client in 1..=4 {
            scope.spawn(move || {
                let mut sql = admin.sql_session();
                start.wait();
                for n in 0..50 {
                    sql.execute(
                        &format!("UPDATE base SET value={n} WHERE id={client}"),
                        &[],
                        None,
                    )
                    .unwrap();
                    let p = sql
                        .prepare(&format!("SELECT value FROM base WHERE id={client}"))
                        .unwrap();
                    assert_eq!(
                        sql.open_cursor(&p, &[]).unwrap().fetch(2, 4096).unwrap()[0].values,
                        vec![Value::Int(n)]
                    );
                }
            });
        }
    });
}

#[test]
fn projection_view_source_bindings_survive_reopen() {
    let mut f = Fixture::new(); let mut sql = f.admin().sql_session();
    for statement in ["CREATE TABLE t(a int, b int)",
        "CREATE VIEW reordered AS SELECT b,a FROM t",
        "CREATE VIEW repeated AS SELECT a AS x,a AS y FROM t",
        "CREATE VIEW star AS SELECT * FROM t",
        "CREATE VIEW nested AS SELECT y,x FROM repeated"] {
        sql.execute(statement, &[], None).unwrap();
    }
    let db = f.db(); let mut tx = db.begin(engine::txn::Isolation::ReadCommitted);
    let wanted = db.catalog().parts(&mut tx).unwrap(); tx.rollback();
    let t = &wanted.tables["t"]; let (a,b) = (t.columns[0].id, t.columns[1].id);
    for (name, sources) in [("reordered", vec![b,a]), ("repeated", vec![a,a]), ("star", vec![a,b])] {
        let v = wanted.sql_views.values().find(|v| v.name == name).unwrap();
        assert_eq!(v.columns.iter().map(|c| c.source).collect::<Vec<_>>(), sources.iter().map(|c| Some((t.id,*c))).collect::<Vec<_>>());
        let expected = if name == "repeated" { vec![a] } else { vec![a,b] };
        assert_eq!(v.dependencies[0].columns, expected);
    }
    let (id, repeated) = wanted.sql_views.iter().find(|(_,v)| v.name == "repeated").unwrap();
    let nested = wanted.sql_views.values().find(|v| v.name == "nested").unwrap();
    assert_eq!(nested.columns.iter().map(|c| c.source).collect::<Vec<_>>(), vec![Some((*id,repeated.columns[1].id)),Some((*id,repeated.columns[0].id))]);
    assert_eq!(sql.execute("ALTER TABLE t DROP COLUMN a", &[], None), Err(CoreError::Sql(crate::relational::SqlError::DependentObjects)));
    drop(sql); f.admin.take();
    f.admin = Some(Admin::open(&f.path, Limits::default()).unwrap());
    let db = f.db(); let mut tx = db.begin(engine::txn::Isolation::ReadCommitted);
    assert_eq!(db.catalog().parts(&mut tx).unwrap().sql_views, wanted.sql_views);
}

#[test]
fn legacy_view_bytes_survive_unrelated_ddl_and_reopen() {
    use engine::catalog_codec::sql_views::{encode_view, view_key};
    let mut f = Fixture::new(); let mut sql = f.admin().sql_session();
    sql.execute("CREATE TABLE base(c int)", &[], None).unwrap();
    sql.execute("CREATE VIEW v AS SELECT c FROM base", &[], None).unwrap();
    let db = f.db(); let mut tx = db.begin(engine::txn::Isolation::ReadCommitted);
    let old = db.catalog().parts(&mut tx).unwrap(); let mut new = old.clone();
    let (&id, view) = new.sql_views.iter_mut().next().unwrap();
    for column in &mut view.columns { column.source = None; }
    let legacy = encode_view(view).unwrap(); assert_eq!(legacy[0], 1);
    db.catalog().apply_diff(&mut tx, &old, &new, &(0..0), None, &|| false).unwrap();
    db.catalog().commit_ddl(tx).unwrap();
    for statement in ["CREATE TABLE unrelated(x int)", "COMMENT ON TABLE unrelated IS 'retained'"] {
        sql.execute(statement, &[], None).unwrap();
    }
    drop(sql); f.admin.take();
    f.admin = Some(Admin::open(&f.path, Limits::default()).unwrap());
    let db = f.db(); let mut tx = db.begin(engine::txn::Isolation::ReadCommitted);
    assert_eq!(tx.get(engine::kv::Keyspace::Catalog, &view_key(id)).unwrap(), Some(legacy));
    assert_eq!(db.catalog().parts(&mut tx).unwrap().sql_views, new.sql_views);
}
