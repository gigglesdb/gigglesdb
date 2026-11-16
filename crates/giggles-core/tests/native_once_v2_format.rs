#![cfg(feature = "engine-rocksdb")]
use giggles_core::{Admin, Engine, Limits};

#[test]
fn receipt_free_export_bytes_and_restore_remain_stable() {
    let root = std::env::temp_dir().join(format!("native-v2-format-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let admin = Admin::create_engine(root.join("db"), Limits::default(), Engine::V2).unwrap();
    admin
        .sql_session()
        .execute("CREATE TABLE items (id BIGINT PRIMARY KEY)", &[], None)
        .unwrap();
    admin
        .sql_session()
        .execute("INSERT INTO items VALUES (1)", &[], None)
        .unwrap();
    admin.export(root.join("first")).unwrap();
    admin.export(root.join("second")).unwrap();
    let bytes = std::fs::read(root.join("first")).unwrap();
    assert_eq!(bytes, std::fs::read(root.join("second")).unwrap());
    assert_eq!(&bytes[..12], b"GDEXP002\0\0\0\x01");
    let restored =
        Admin::restore(root.join("first"), root.join("restored"), Limits::default()).unwrap();
    assert_eq!(
        restored
            .sql_session()
            .execute("SELECT * FROM items", &[], None)
            .unwrap()
            .rows
            .len(),
        1
    );
    drop(restored);
    drop(admin);
    std::fs::remove_dir_all(root).unwrap();
}
