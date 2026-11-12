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
