use super::*;
use crate::Engine;

#[test]
fn native_bit_receipt_and_compacted_identity_refuse_export_before_staging() {
    for case in 0..4 {
        let root = crate::test_artifact_root()
            .join(format!("native-backup-{}-{case}", std::process::id()));
        fs::create_dir_all(&root).unwrap();
        let admin = Admin::create_engine(root.join("db"), Limits::default(), Engine::V2).unwrap();
        let db = admin.test_v2();
        let mut batch = WriteBatch::default();
        if case != 3 {
            engine::meta::put_features(&mut batch, 2).unwrap();
        }
        batch.put(
            Keyspace::Meta,
            b"subversion".to_vec(),
            2u32.to_be_bytes().to_vec(),
        );
        if case != 0 {
            let mut ledger = 1u16.to_be_bytes().to_vec();
            for n in [110u64, 1] {
                ledger.extend(n.to_be_bytes());
            }
            ledger.extend(99u128.to_be_bytes());
            ledger.extend(1u64.to_be_bytes());
            ledger.push(42);
            ledger.extend([1; 32]);
            ledger.extend([2; 32]);
            ledger.extend(120u64.to_be_bytes());
            ledger.push(u8::from(case == 1));
            if case == 1 {
                let payload = crate::operations::once::outcome::encode(
                    &crate::operations::once::OnceDescription {
                        catalog_epoch: 1,
                        output: crate::operations::once::OnceSchema::Operation(vec![]),
                    },
                    &Err(crate::operations::Error::NotFound),
                )
                .unwrap();
                ledger.extend((payload.len() as u64).to_be_bytes());
                ledger.extend(payload);
            }
            for n in [10u64, 100, 110] {
                ledger.extend(n.to_be_bytes());
            }
            let metadata = encode(&Metadata {
                format: 4,
                domain: u128::from_be_bytes(db.meta.domain),
                schema: 0,
                last_time: 0,
                next_approval: 0,
                origin: None,
            })
            .unwrap();
            for (key, value) in [(vec![0], metadata), (vec![7], ledger)] {
                batch.put(
                    Keyspace::Kernel,
                    codec::user_key_seek_at(&key, Ts(1)),
                    mvcc::encode_live(&value),
                );
            }
        }
        db.kv.write(batch, Durability::Sync).unwrap();
        if case == 1 || case == 2 {
            engine::meta::open_checks(db.kv.as_ref()).unwrap();
        }
        let existing = root.join("existing");
        fs::write(&existing, b"unchanged").unwrap();
        let before = fs::read_dir(&root).unwrap().count();
        assert_eq!(
            admin.export(root.join("absent")),
            Err(CoreError::Unsupported)
        );
        assert_eq!(admin.export(&existing), Err(CoreError::Unsupported));
        assert_eq!(fs::read(&existing).unwrap(), b"unchanged");
        assert!(!root.join("absent").exists());
        assert_eq!(fs::read_dir(&root).unwrap().count(), before);
        drop(db);
        drop(admin);
        fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn native_export_header_refuses_restore_before_destination_creation() {
    let root = crate::test_artifact_root().join(format!("native-import-{}", std::process::id()));
    fs::create_dir_all(&root).unwrap();
    let admin = Admin::create_engine(root.join("db"), Limits::default(), Engine::V2).unwrap();
    let backup = root.join("backup");
    admin.export(&backup).unwrap();
    let mut bytes = fs::read(&backup).unwrap();
    bytes[12..20].copy_from_slice(&2u64.to_be_bytes());
    let end = bytes.len() - 32;
    let digest = Sha256::digest(&bytes[..end]);
    bytes[end..].copy_from_slice(&digest);
    fs::write(&backup, bytes).unwrap();
    let destination = root.join("absent");
    assert!(matches!(
        Admin::restore(&backup, &destination, Limits::default()),
        Err(CoreError::Unsupported)
    ));
    assert!(!destination.exists());
    fs::create_dir(&destination).unwrap();
    fs::write(destination.join("sentinel"), b"keep").unwrap();
    assert!(matches!(
        Admin::restore(&backup, &destination, Limits::default()),
        Err(CoreError::Unsupported)
    ));
    assert_eq!(fs::read(destination.join("sentinel")).unwrap(), b"keep");
    drop(admin);
    fs::remove_dir_all(root).unwrap();
}
