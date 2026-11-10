use super::*;
use crate::{Engine, relational::Value, roles::NewRole};

fn directory() -> std::path::PathBuf {
    let mut nonce = [0; 16];
    getrandom::fill(&mut nonce).unwrap();
    std::env::temp_dir().join(format!("v2-export-{:032x}", u128::from_be_bytes(nonce)))
}

fn rows(admin: &Admin) -> Vec<Vec<Value>> {
    admin
        .sql_session()
        .execute("SELECT * FROM items ORDER BY id", &[], None)
        .unwrap()
        .rows
        .into_iter()
        .map(|row| row.values)
        .collect()
}

#[test]
fn restore_preserves_immediate_origin_across_reopen_and_generations() {
    let root = directory();
    fs::create_dir(&root).unwrap();
    let source = Admin::create_engine(root.join("db"), Limits::default(), Engine::V2).unwrap();
    source
        .sql_session()
        .execute("CREATE TABLE items (id BIGINT PRIMARY KEY)", &[], None)
        .unwrap();
    source
        .sql_session()
        .execute("INSERT INTO items VALUES (1),(2)", &[], None)
        .unwrap();
    let expected = rows(&source);
    let mut domain = source.authority_metadata().unwrap().0;
    drop(source);
    for generation in 0..2 {
        let from = if generation == 0 { "db" } else { "restored0" };
        let source = Admin::open(root.join(from), Limits::default()).unwrap();
        source.export(root.join("backup")).unwrap();
        drop(source);
        let path = root.join(format!("restored{generation}"));
        let restored = Admin::restore(root.join("backup"), &path, Limits::default()).unwrap();
        let fresh = restored.authority_metadata().unwrap().0;
        assert_ne!(fresh, domain);
        let check = |copy: Admin| {
            assert_eq!(copy.authority_metadata().unwrap().3, Some(domain));
            let BackupDb(db) = BackupDb::from_admin(&copy);
            assert_eq!(db.metadata_state(None).unwrap().origin, Some(domain));
            assert_eq!(rows(&copy), expected);
        };
        check(restored);
        check(Admin::open(&path, Limits::default()).unwrap());
        domain = fresh;
        fs::remove_file(root.join("backup")).unwrap();
    }
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn pinned_export_allows_commits_and_restores_snapshot_roles_and_features() {
    let root = directory();
    fs::create_dir(&root).unwrap();
    let admin = Admin::create_engine(root.join("db"), Limits::default(), Engine::V2).unwrap();
    admin
        .roles()
        .create(NewRole {
            name: "owner".into(),
            login: true,
            superuser: true,
            ..Default::default()
        })
        .unwrap();
    let mut sql = admin.sql_session();
    sql.execute(
        "CREATE TABLE items (id BIGINT PRIMARY KEY, body TEXT)",
        &[],
        None,
    )
    .unwrap();
    sql.execute(
        "INSERT INTO items VALUES (1,'one'),(2,'two'),(3,'three')",
        &[],
        None,
    )
    .unwrap();
    sql.execute("UPDATE items SET body='updated' WHERE id=1", &[], None)
        .unwrap();
    sql.execute("DELETE FROM items WHERE id=3", &[], None)
        .unwrap();
    sql.execute("CREATE TABLE heap_items (body TEXT)", &[], None)
        .unwrap();
    sql.execute("INSERT INTO heap_items VALUES ('before')", &[], None)
        .unwrap();
    sql.execute("CREATE SEQUENCE tickets START WITH 10", &[], None)
        .unwrap();
    let clock = crate::relational::StatementClock {
        unavailable: None,
        transaction_us: 1,
        statement_us: 1,
        zone: None,
        wall: None,
    };
    sql.execute_at("SELECT nextval('tickets')", &[], None, &clock)
        .unwrap();
    admin.roles().set_table_owner("items", "owner").unwrap();
    let expected = rows(&admin);
    let roles = format!("{:?}", admin.roles().list().unwrap());
    let BackupDb(db) = BackupDb::from_admin(&admin);
    let before = db.sequencer.visible_ts();
    let (start, started) = std::sync::mpsc::channel();
    let (done, finished) = std::sync::mpsc::channel();
    std::thread::scope(|scope| {
        let writer_admin = &admin;
        scope.spawn(move || {
            started.recv().unwrap();
            let mut writer = writer_admin.sql_session();
            for id in 4..20 {
                writer
                    .execute(
                        "INSERT INTO items VALUES ($1,'later')",
                        &[Value::Int(id)],
                        None,
                    )
                    .unwrap();
            }
            writer
                .execute("UPDATE items SET body='later' WHERE id=1", &[], None)
                .unwrap();
            writer
                .execute("DELETE FROM items WHERE id=2", &[], None)
                .unwrap();
            writer_admin
                .roles()
                .create(NewRole {
                    name: "later".into(),
                    ..Default::default()
                })
                .unwrap();
            done.send(()).unwrap();
        });
        export_inner(db, &root.join("backup"), RecoveryPoint::None, |ts| {
            assert_eq!(ts, before);
            start.send(()).unwrap();
            // These commits must complete while the export still owns its pin.
            finished
                .recv_timeout(std::time::Duration::from_secs(30))
                .unwrap();
        })
        .unwrap();
    });
    let summary = Admin::verify_export(root.join("backup")).unwrap();
    assert_eq!(summary.schema, before.0);
    let read = ExportRead::open(&root.join("backup")).unwrap();
    let features = engine::meta::validate_meta(db.kv.as_ref())
        .unwrap()
        .features;
    assert_eq!(read.features, features);
    let restored = Admin::restore(
        root.join("backup"),
        root.join("restored"),
        Limits::default(),
    )
    .unwrap();
    assert_eq!(rows(&restored), expected);
    assert_ne!(rows(&admin), expected);
    assert_eq!(format!("{:?}", restored.roles().list().unwrap()), roles);
    assert_eq!(
        restored.roles().table_owner("items").unwrap().as_deref(),
        Some("owner")
    );
    let BackupDb(copy) = BackupDb::from_admin(&restored);
    assert_eq!(copy.meta.features, features);
    // Restored allocator high waters must not reuse table/column/heap IDs.
    restored
        .sql_session()
        .execute("CREATE TABLE next_table (id BIGINT PRIMARY KEY)", &[], None)
        .unwrap();
    let mut copy_sql = restored.sql_session();
    copy_sql
        .execute("INSERT INTO heap_items VALUES ('after')", &[], None)
        .unwrap();
    assert_eq!(
        copy_sql
            .execute("SELECT * FROM heap_items", &[], None)
            .unwrap()
            .rows
            .len(),
        2
    );
    assert_eq!(
        copy_sql
            .execute_at("SELECT nextval('tickets')", &[], None, &clock)
            .unwrap()
            .rows[0]
            .values,
        // Restart/restore skips the engine's 32-value durable reservation.
        vec![Value::Int(43)]
    );
    drop(copy_sql);
    drop(read);
    drop(restored);
    drop(sql);
    drop(admin);
    fs::remove_dir_all(root).unwrap();
}

struct BackupDb<'a>(&'a v2::V2Db);
impl<'a> BackupDb<'a> {
    fn from_admin(admin: &'a Admin) -> Self {
        match &admin.inner.backend {
            Backend::V2(db) => Self(db),
            _ => panic!("expected v2"),
        }
    }
}

#[test]
fn streaming_export_exceeds_legacy_limit_and_detects_corruption() {
    let root = directory();
    fs::create_dir(&root).unwrap();
    let admin = Admin::create_engine(root.join("db"), Limits::default(), Engine::V2).unwrap();
    let mut sql = admin.sql_session();
    sql.execute(
        "CREATE TABLE items (id BIGINT PRIMARY KEY, body TEXT)",
        &[],
        None,
    )
    .unwrap();
    let body = "x".repeat(60 * 1024);
    let BackupDb(db) = BackupDb::from_admin(&admin);
    let mut txn = db.begin(engine::txn::Isolation::ReadCommitted);
    let entry = db
        .catalog()
        .resolve(
            &mut txn,
            "items",
            engine::locks::LockMode::RowExclusive,
            None,
            &|| false,
        )
        .unwrap();
    // SQL's statement image has independent byte limits. Populate through the
    // row-store API so this proof measures the backup's total-size limit.
    for id in 0..300 {
        db.rows
            .insert(
                &mut txn,
                &entry,
                vec![Value::Int(id), Value::Text(body.clone())],
                None,
                &|| false,
            )
            .unwrap();
    }
    txn.commit().unwrap();
    let digest = admin.export(root.join("backup")).unwrap();
    let bytes = fs::read(root.join("backup")).unwrap();
    assert!(bytes.len() > MAX_BYTES as usize);
    assert_eq!(Sha256::digest(&bytes[..bytes.len() - 32])[..], digest);
    let restored = Admin::restore(
        root.join("backup"),
        root.join("restored"),
        Limits::default(),
    )
    .unwrap();
    let BackupDb(copy) = BackupDb::from_admin(&restored);
    let mut txn = copy.begin(engine::txn::Isolation::ReadCommitted);
    let mut count = 0;
    for row in copy
        .rows
        .scan(
            &mut txn,
            &entry,
            Bound::Unbounded,
            Bound::Unbounded,
            Direction::Forward,
            None,
        )
        .unwrap()
    {
        let (_, row) = row.unwrap();
        assert_eq!(
            row.values,
            vec![Value::Int(count), Value::Text(body.clone())]
        );
        count += 1;
    }
    assert_eq!(count, 300);
    drop(txn);
    assert!(matches!(
        Admin::restore(
            root.join("backup"),
            root.join("restored"),
            Limits::default()
        ),
        Err(CoreError::Exists)
    ));
    let mut damaged = bytes.clone();
    damaged[100] ^= 1;
    fs::write(root.join("corrupt"), damaged).unwrap();
    assert!(matches!(
        Admin::verify_export(root.join("corrupt")),
        Err(CoreError::Corrupt(_))
    ));
    // A valid digest cannot disguise malformed record framing or catalog bytes.
    for catalog_damage in [false, true] {
        let mut damaged = bytes.clone();
        if catalog_damage {
            let key_len = u32::from_be_bytes(damaged[64..68].try_into().unwrap()) as usize;
            damaged[68 + key_len] = 255;
        } else {
            damaged[28..32].copy_from_slice(&u32::MAX.to_be_bytes());
        }
        let end = damaged.len() - 32;
        let digest = Sha256::digest(&damaged[..end]);
        damaged[end..].copy_from_slice(&digest);
        fs::write(root.join("malformed"), damaged).unwrap();
        assert!(Admin::verify_export(root.join("malformed")).is_err());
    }
    drop(restored);
    drop(sql);
    drop(admin);
    fs::remove_dir_all(root).unwrap();
}
