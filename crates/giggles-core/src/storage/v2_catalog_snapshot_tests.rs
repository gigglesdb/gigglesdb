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

#[test]
fn cow_catalog_write_visibility_across_isolation_levels() {
    for level in [crate::SqlIsolation::ReadCommitted, crate::SqlIsolation::RepeatableRead, crate::SqlIsolation::Serializable] {
        for read_table in [false, true] {
            let f = Fixture::new();
            let mut own = f.admin().sql_session();
            let mut other = f.admin().sql_session();
            own.set_isolation(level);
            other.execute("CREATE TABLE changing(id bigint PRIMARY KEY)", &[], None).unwrap();
            own.execute("BEGIN", &[], None).unwrap();
            let alter = "ALTER TABLE changing ADD COLUMN extra bigint DEFAULT 7";
            if read_table {
                // AccessShare prevents concurrent ALTER until transaction end.
                own.execute("SELECT id FROM changing", &[], None).unwrap();
                let limits = crate::task::definition::LoadLimits { lock_wait_ms: 1, ..Default::default() };
                {
                    let _meter = crate::task::limits::metered(&limits);
                    assert_eq!(other.execute(alter, &[], None).unwrap_err(), CoreError::LockNotAvailable);
                }
                own.execute("ROLLBACK", &[], None).unwrap();
            } else {
                own.execute("SELECT 1", &[], None).unwrap();
            }
            other.execute(alter, &[], None).unwrap();
            if read_table { own.execute("BEGIN", &[], None).unwrap(); }
            let insert = "INSERT INTO changing(id, extra) VALUES(2, 9)";
            if !read_table && level != crate::SqlIsolation::ReadCommitted {
                assert_eq!(own.execute(insert, &[], None).unwrap_err(),
                    CoreError::Sql(crate::relational::SqlError::Conflict), "{level:?}");
                own.execute("ROLLBACK", &[], None).unwrap();
                own.execute("BEGIN", &[], None).unwrap();
            }
            own.execute(insert, &[], None).unwrap();
            let p = own.prepare("SELECT id, extra FROM changing").unwrap();
            let rows = own.open_cursor(&p, &[]).unwrap().fetch(10, 4096).unwrap();
            assert_eq!(rows.iter().map(|r| r.values.clone()).collect::<Vec<_>>(),
                vec![vec![Value::Int(2), Value::Int(9)]], "{level:?}");
            own.execute("ROLLBACK", &[], None).unwrap();
        }
    }
}

#[test]
fn cow_private_ddl_writes_and_savepoint_visibility() {
    let f = Fixture::new();
    let mut own = f.admin().sql_session();
    let mut other = f.admin().sql_session();
    own.execute("CREATE TABLE base(id bigint PRIMARY KEY)", &[], None).unwrap();
    let missing_sequence = other.execute("SELECT nextval('never_created_seq')", &[], None).unwrap_err();
    let before = other.execute("INSERT INTO base(id, extra) VALUES(1, 9)", &[], None).unwrap_err();
    own.execute("BEGIN", &[], None).unwrap();
    own.execute("SAVEPOINT s", &[], None).unwrap();
    own.execute("CREATE SEQUENCE private_seq START WITH 20", &[], None).unwrap();
    own.execute("ALTER TABLE base ADD COLUMN extra bigint DEFAULT 7", &[], None).unwrap();
    own.execute("INSERT INTO base(id) VALUES(1),(2)", &[], None).unwrap();
    let p = own.prepare("SELECT extra FROM base ORDER BY id").unwrap();
    assert_eq!(own.open_cursor(&p, &[]).unwrap().fetch(10, 4096).unwrap().iter().map(|r|r.values.clone()).collect::<Vec<_>>(), vec![vec![Value::Int(7)],vec![Value::Int(7)]]);
    let public = other.prepare("SELECT * FROM base").unwrap();
    assert_eq!(other.describe(&public).unwrap().columns.len(), 1);
    own.execute("ROLLBACK TO s", &[], None).unwrap();
    assert_eq!(own.execute("INSERT INTO base(id, extra) VALUES(3, 9)", &[], None).unwrap_err(), before);
    own.execute("ROLLBACK TO s", &[], None).unwrap();
    assert_eq!(own.execute("SELECT nextval('private_seq')", &[], None).unwrap_err(), missing_sequence);
    own.execute("ROLLBACK TO s", &[], None).unwrap();
    own.execute("INSERT INTO base VALUES(4)", &[], None).unwrap();
    own.execute("COMMIT", &[], None).unwrap();
}

#[test]
fn cow_v2_template_survives_dml_hydration_cascades_and_upsert() {
    let f = Fixture::new();
    let mut sql = f.admin().sql_session();
    for statement in [
        "CREATE TABLE parent(id bigint PRIMARY KEY, n bigint)",
        "CREATE TABLE child(id bigint PRIMARY KEY, pid bigint REFERENCES parent(id) ON DELETE CASCADE)",
    ] { sql.execute(statement, &[], None).unwrap(); }
    let db = f.db();
    let mut tx = db.begin(engine::txn::Isolation::ReadCommitted);
    let template = db.catalog().snapshot(&mut tx).unwrap().image().unwrap();
    let before = template.catalog_parts();
    tx.rollback();
    for statement in [
        "INSERT INTO parent VALUES(1,2),(2,3)",
        "INSERT INTO child VALUES(10,1),(20,2)",
        "INSERT INTO parent VALUES(1,8) ON CONFLICT(id) DO UPDATE SET n=excluded.n",
        "UPDATE parent SET n=9 WHERE id=2",
        "DELETE FROM parent WHERE id=1",
    ] {
        sql.execute(statement, &[], None).unwrap();
        assert_eq!(template.catalog_parts(), before);
        let mut tx = db.begin(engine::txn::Isolation::ReadCommitted);
        let after = db.catalog().snapshot(&mut tx).unwrap().image().unwrap();
        assert!(after.shares_catalog_with(&template), "{statement}");
        let fork = after.catalog_image_for_statement(u64::MAX, 1);
        assert!(fork.shares_catalog_with(&template));
        tx.rollback();
    }
    for statement in ["ALTER TABLE parent ADD COLUMN extra bigint DEFAULT 7", "CREATE SEQUENCE seq", "ALTER SEQUENCE seq INCREMENT BY 3"] {
        sql.execute(statement, &[], None).unwrap();
        assert_eq!(template.catalog_parts(), before);
        let mut tx = db.begin(engine::txn::Isolation::ReadCommitted);
        let after = db.catalog().snapshot(&mut tx).unwrap().image().unwrap();
        assert!(!after.shares_catalog_with(&template), "{statement}");
        let (tables, ext, _, _) = after.catalog_parts();
        assert!(tables["parent"].columns.iter().any(|column| column.name == "extra"));
        if statement.starts_with("CREATE SEQUENCE") {
            assert!(ext.sequences.values().any(|seq| seq.name == "seq" && seq.increment == 1));
        } else if statement.starts_with("ALTER SEQUENCE") {
            assert!(ext.sequences.values().any(|seq| seq.name == "seq" && seq.increment == 3));
        }
        tx.rollback();
    }
    let p = sql.prepare("SELECT id FROM child ORDER BY id").unwrap();
    assert_eq!(sql.open_cursor(&p, &[]).unwrap().fetch(10,4096).unwrap()[0].values, vec![Value::Int(20)]);
}
