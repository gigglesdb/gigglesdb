use crate::{
    Admin, Limits,
    operations::{CatalogRequest, ColumnSpec, Engine, Error},
    relational::{DataType, Value},
};
use std::{
    cell::{Cell, RefCell},
    rc::Rc,
};

type PublishHook = Box<dyn FnMut()>;
thread_local! {
    static PUBLISH_HOOK: RefCell<Option<PublishHook>> = const { RefCell::new(None) };
}
pub(super) fn before_publish() {
    PUBLISH_HOOK.with_borrow_mut(|hook| {
        if let Some(hook) = hook {
            hook();
        }
    });
}
pub(super) fn with_publish_hook<T>(hook: impl FnMut() + 'static, work: impl FnOnce() -> T) -> T {
    struct Restore(Option<PublishHook>);
    impl Drop for Restore {
        fn drop(&mut self) {
            PUBLISH_HOOK.with_borrow_mut(|hook| *hook = self.0.take());
        }
    }
    let _restore = Restore(PUBLISH_HOOK.with_borrow_mut(|current| current.replace(Box::new(hook))));
    work()
}
fn fixture() -> (Admin, Engine) {
    let root = crate::test_artifact_root();
    std::fs::create_dir_all(&root).unwrap();
    let mut token = [0; 16];
    getrandom::fill(&mut token).unwrap();
    let path = root.join(format!(
        "catalog-snapshot-{:032x}",
        u128::from_be_bytes(token)
    ));
    let admin = Admin::create(&path, Limits::default()).unwrap();
    admin
        .sql_session()
        .execute("CREATE TABLE items (id BIGINT PRIMARY KEY)", &[], None)
        .unwrap();
    (admin.clone(), Engine::new(admin))
}
fn request() -> CatalogRequest {
    CatalogRequest::CreateTable {
        table: "proposed".into(),
        columns: vec![
            ColumnSpec {
                name: "id".into(),
                data_type: DataType::BigInt,
                not_null: true,
                default: None, generated: None,
            },
            ColumnSpec {
                name: "label".into(),
                data_type: DataType::Text,
                not_null: false,
                default: None, generated: None,
            },
        ],
        primary_key: vec!["id".into()],
    }
}

#[test]
fn catalog_create_certifies_empty_snapshot_against_interleaved_sql_commit() {
    for prepared in [false, true] {
        for competing_ddl in [false, true] {
            let (admin, engine) = fixture();
            let request = request();
            let bound = prepared.then(|| engine.prepare_catalog_bound(&request).unwrap());
            let original_catalog = admin.catalog().unwrap();
            let outside_catalog = Rc::new(RefCell::new(None));
            let captured_catalog = outside_catalog.clone();
            let calls = Rc::new(Cell::new(0));
            let captured_calls = calls.clone();
            let outside = admin.clone();
            let outcome = with_publish_hook(
                move || {
                    // Native CREATE has staged its descriptor, but an independent
                    // SQL reader must still see only the committed catalog.
                    assert_eq!(outside.catalog().unwrap(), original_catalog);
                    let mut sql = outside.sql_session();
                    if competing_ddl {
                        sql.execute("CREATE TABLE competitor (id BIGINT PRIMARY KEY)", &[], None)
                            .unwrap();
                    } else {
                        // This changes population without changing catalog epoch.
                        sql.execute("INSERT INTO items VALUES (1)", &[], None)
                            .unwrap();
                    }
                    *captured_catalog.borrow_mut() = Some(outside.catalog().unwrap());
                    captured_calls.set(captured_calls.get() + 1);
                },
                || match &bound {
                    Some(bound) => engine.execute_catalog_bound(bound),
                    None => engine.execute_catalog(&request),
                },
            );
            assert_eq!(calls.get(), 1);
            assert_eq!(outcome, Err(Error::Conflict));
            let after = admin.catalog().unwrap();
            assert_eq!(Some(&after), outside_catalog.borrow().as_ref());
            assert!(after.1.iter().all(|table| table.name != "proposed"));

            // A rejected native publication consumes neither schema IDs nor
            // row versions. Existing SQL certification decision metadata may
            // advance, so checking an export digest here would be incorrect.
            let next_id = after
                .1
                .iter()
                .flat_map(|table| {
                    std::iter::once(table.id).chain(table.columns.iter().map(|c| c.id))
                })
                .max()
                .unwrap()
                + 1;
            let mut sql = admin.sql_session();
            sql.execute(
                "CREATE TABLE after_conflict (id BIGINT PRIMARY KEY)",
                &[],
                None,
            )
            .unwrap();
            let catalog = admin.catalog().unwrap();
            assert_eq!(catalog.0, after.0 + 1);
            let created = catalog
                .1
                .iter()
                .find(|table| table.name == "after_conflict")
                .unwrap();
            assert_eq!(created.id, next_id);
            assert_eq!(created.columns[0].id, next_id + 1);
            sql.execute("INSERT INTO items VALUES (2)", &[], None)
                .unwrap();
            let rows = sql
                .execute("SELECT id FROM items LIMIT 1024", &[], None)
                .unwrap()
                .rows;
            if competing_ddl {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].values, vec![Value::Int(2)]);
                assert_eq!(rows[0].version, 1);
            } else {
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0].values, vec![Value::Int(1)]);
                assert_eq!(rows[0].version, 1);
                assert_eq!(rows[1].values, vec![Value::Int(2)]);
                assert_eq!(rows[1].version, 2);
            }
        }
    }
}
