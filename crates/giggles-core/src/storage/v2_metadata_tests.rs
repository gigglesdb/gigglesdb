#[cfg(feature = "legacy-task")]
use crate::CoreError;
use crate::{Admin, Engine, Limits, roles::NewRole};

#[test]
fn metadata_survives_reopen_and_unsupported_native_operations() {
    let mut nonce = [0; 16];
    getrandom::fill(&mut nonce).unwrap();
    let path =
        std::env::temp_dir().join(format!("v2-metadata-{:032x}", u128::from_be_bytes(nonce)));
    let admin = Admin::create_engine(&path, Limits::default(), Engine::V2).unwrap();
    admin
        .roles()
        .create(NewRole {
            name: "owner".into(),
            login: true,
            superuser: true,
            ..Default::default()
        })
        .unwrap();
    let mut session = admin.sql_session();
    session
        .execute(
            "CREATE TABLE items (id bigint PRIMARY KEY, label varchar(12))",
            &[],
            None,
        )
        .unwrap();
    admin.roles().set_table_owner("items", "owner").unwrap();
    let authority = admin.authority_metadata().unwrap();
    let (epoch, tables) = admin.catalog().unwrap();
    assert_eq!(tables.len(), 1);
    let (typmod_epoch, typmods) = admin.catalog_typmods().unwrap();
    assert_eq!(typmod_epoch, epoch);
    assert_eq!(typmods.get(&tables[0].columns[1].id), Some(&16));
    assert_eq!(admin.secondary_index_catalog().unwrap(), (epoch, vec![]));
    assert_eq!(session.user_type_catalog().unwrap().epoch, epoch);
    assert_eq!(
        admin.roles().table_owner("items").unwrap().as_deref(),
        Some("owner")
    );
    admin.export(path.join("export")).unwrap();
    Admin::verify_export(path.join("export")).unwrap();
    #[cfg(feature = "legacy-task")]
    assert!(matches!(
        admin.task_resources(crate::TaskId(0)),
        Err(CoreError::EngineUnsupported {
            feature: "task executors",
            ..
        })
    ));
    let plan = session.prepare("SELECT * FROM items").unwrap();
    assert_eq!(session.describe(&plan).unwrap().columns.len(), 2);
    session.validate_immutable_constants(&plan).unwrap();
    session.execute("BEGIN", &[], None).unwrap();
    session
        .execute(
            "CREATE TABLE private_items (id bigint PRIMARY KEY)",
            &[],
            None,
        )
        .unwrap();
    assert!(session.relation_exists("private_items").unwrap());
    assert!(
        !admin
            .sql_session()
            .relation_exists("private_items")
            .unwrap()
    );
    let private = session.prepare("SELECT * FROM private_items").unwrap();
    assert_eq!(session.describe(&private).unwrap().columns.len(), 1);
    session.execute("ROLLBACK", &[], None).unwrap();
    assert!(!session.relation_exists("private_items").unwrap());
    drop(session);
    drop(admin);
    let reopened = Admin::open(&path, Limits::default()).unwrap();
    assert_eq!(reopened.authority_metadata().unwrap(), authority);
    assert_eq!(reopened.catalog().unwrap(), (epoch, tables));
    assert_eq!(
        reopened.roles().table_owner("items").unwrap().as_deref(),
        Some("owner")
    );
    assert!(reopened.drop_table("items", false).unwrap());
    assert!(!reopened.drop_table("items", true).unwrap());
    assert!(reopened.roles().table_owner("items").unwrap().is_none());
    assert_eq!(reopened.authority_metadata().unwrap(), authority);
    drop(reopened);
    std::fs::remove_dir_all(path).unwrap();
}
