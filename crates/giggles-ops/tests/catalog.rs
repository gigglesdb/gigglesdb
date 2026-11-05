mod common;
use common::*;
use giggles_ops::*;
use sha2::{Digest, Sha256};
#[test]
fn ids_reject_paths_and_reserved_shapes() {
    for s in ["con", "NUL", "aux", "COM1", "lpt9"] {
        assert!(BackupId::new(s).is_err());
    }
    for s in [
        "", "../x", "/x", "C:\\x", "a/b", "a\\b", "a.json", "a:b", "a\0", "a ", "-x", "é",
    ] {
        assert!(BackupId::new(s).is_err(), "{s:?}");
    }
    assert!(BackupId::new("a".repeat(65)).is_err());
    for byte in 0..=255u8 {
        let s = format!("a{}", char::from(byte));
        let expected = byte.is_ascii_alphanumeric() || byte == b'-';
        assert_eq!(BackupId::new(s).is_ok(), expected);
    }
}
#[test]
fn backup_roundtrip_and_retained_stage() {
    let (root, c, a) = fixture();
    let m = c.backup(&a, &id()).unwrap();
    assert_eq!(
        m.source_domain,
        format!("{:032x}", a.snapshot().unwrap().domain)
    );
    assert_eq!(c.inspect(&id()).unwrap(), m);
    assert_eq!(c.verify(&id()).unwrap().operations, 0);
    assert!(root.join(&m.snapshot).exists());
    assert!(
        root.join(&m.snapshot)
            .parent()
            .unwrap()
            .join("incomplete.json")
            .exists()
    );
    assert_eq!(c.list().unwrap().len(), 2);
}
#[test]
fn duplicate_backup_preserves_original() {
    let (root, c, a) = fixture();
    let m = c.backup(&a, &id()).unwrap();
    let original = std::fs::read(root.join(&m.snapshot)).unwrap();
    assert!(matches!(c.backup(&a, &id()), Err(OpsError::Exists)));
    assert_eq!(c.inspect(&id()).unwrap(), m);
    assert_eq!(std::fs::read(root.join(&m.snapshot)).unwrap(), original);
}
#[test]
fn concurrent_same_id_one_winner() {
    let (_, c, a) = fixture();
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(4));
    let joins: Vec<_> = (0..4)
        .map(|_| {
            let c = c.clone();
            let a = a.clone();
            let b = barrier.clone();
            std::thread::spawn(move || {
                b.wait();
                c.backup(&a, &id()).is_ok()
            })
        })
        .collect();
    assert_eq!(
        joins
            .into_iter()
            .filter(|j| j.thread().id() != std::thread::current().id())
            .map(|j| usize::from(j.join().unwrap()))
            .sum::<usize>(),
        1
    );
    c.verify(&id()).unwrap();
}
#[test]
fn fault_edges_never_false_complete() {
    for point in [
        FaultPoint::BeforeExport,
        FaultPoint::AfterSnapshot,
        FaultPoint::BeforePublish,
        FaultPoint::AfterPublish,
    ] {
        let (root, c, a) = fixture();
        assert!(
            c.backup_with_fault(
                &a,
                &id(),
                Fault {
                    point,
                    action: FaultAction::Error
                }
            )
            .is_err()
        );
        assert_eq!(
            root.join("backup-1.json").exists(),
            point == FaultPoint::AfterPublish
        );
        assert_eq!(c.verify(&id()).is_ok(), point == FaultPoint::AfterPublish);
        assert!(!c.list().unwrap().is_empty());
    }
}
#[test]
fn manifest_strict_fields_and_versions() {
    let (root, c, a) = fixture();
    let m = c.backup(&a, &id()).unwrap();
    let baseline = serde_json::to_value(&m).unwrap();
    let cases = [
        ("format_version", serde_json::json!(2)),
        ("state", serde_json::json!("incomplete")),
        ("source_domain", serde_json::json!("123")),
        ("source_generation", serde_json::json!(1)),
        ("backup_id", serde_json::json!("other")),
        ("sha256", serde_json::json!("00")),
        ("tool_version", serde_json::json!("future")),
        ("envelope_version", serde_json::json!(99)),
        ("content_length", serde_json::json!(u64::MAX)),
        ("unknown", serde_json::json!(true)),
    ];
    for (key, value) in cases {
        let mut v = baseline.clone();
        v[key] = value;
        std::fs::write(root.join("backup-1.json"), serde_json::to_vec(&v).unwrap()).unwrap();
        assert!(c.inspect(&id()).is_err(), "{key}");
    }
    let b = serde_json::to_string(&m).unwrap();
    let duplicate = b.replacen('{', "{\"format_version\":1,", 1);
    std::fs::write(root.join("backup-1.json"), duplicate).unwrap();
    assert!(c.inspect(&id()).is_err());
}
#[test]
fn manifest_path_traversal_matrix() {
    let (root, c, a) = fixture();
    let mut m = c.backup(&a, &id()).unwrap();
    for path in [
        "../snapshot.gdb",
        "/snapshot.gdb",
        "C:/x",
        "\\\\server\\share",
        "staging-00000000000000000000000000000000/../snapshot.gdb",
        "staging-00000000000000000000000000000000\\snapshot.gdb",
        "staging-00000000000000000000000000000000/snapshot.gdb:ads",
    ] {
        m.snapshot = path.into();
        write_manifest(&root, &m);
        assert!(c.verify(&id()).is_err(), "{path}");
    }
}
#[test]
fn bounded_manifest_and_catalog() {
    let (root, c, a) = fixture();
    let m = c.backup(&a, &id()).unwrap();
    std::fs::write(
        root.join("backup-1.json"),
        vec![b' '; MAX_MANIFEST as usize + 1],
    )
    .unwrap();
    assert!(matches!(c.inspect(&id()), Err(OpsError::Limit)));
    write_manifest(&root, &m);
    let small = Catalog::open(
        &root,
        OpsLimits {
            catalog_entries: 1,
            ..Default::default()
        },
    )
    .unwrap();
    assert!(matches!(small.list(), Err(OpsError::Limit)));
    let small = Catalog::open(
        &root,
        OpsLimits {
            snapshot_bytes: 52,
            ..Default::default()
        },
    )
    .unwrap();
    assert!(matches!(small.verify(&id()), Err(OpsError::Limit)));
    for limits in [
        OpsLimits {
            manifest_bytes: 0,
            ..Default::default()
        },
        OpsLimits {
            snapshot_bytes: MAX_SNAPSHOT + 1,
            ..Default::default()
        },
        OpsLimits {
            catalog_entries: 1025,
            ..Default::default()
        },
    ] {
        assert!(Catalog::open(&root, limits).is_err());
    }
}
#[test]
fn tamper_matrix() {
    for mode in 0..7 {
        let (root, c, a) = fixture();
        let mut m = c.backup(&a, &id()).unwrap();
        let path = root.join(&m.snapshot);
        let mut bytes = std::fs::read(&path).unwrap();
        match mode {
            0 => bytes[55] ^= 1,
            1 => bytes.push(0),
            2 => {
                bytes.pop();
            }
            3 => {
                m.content_length += 1;
                write_manifest(&root, &m)
            }
            4 => {
                m.source_domain = "0".repeat(32);
                write_manifest(&root, &m)
            }
            5 => {
                m.snapshot = m.snapshot.replace("staging-", "staging-f");
                write_manifest(&root, &m)
            }
            6 => bytes[8..12].copy_from_slice(&99u32.to_be_bytes()),
            _ => unreachable!(),
        }
        std::fs::write(&path, bytes).unwrap();
        assert!(c.verify(&id()).is_err(), "mode{mode}");
        let target = fresh("negative").join("target");
        assert!(c.restore(&id(), &target).is_err());
        assert!(!target.exists());
    }
}
#[test]
fn forged_outer_hash_inner_corruption() {
    let (root, c, a) = fixture();
    let mut m = c.backup(&a, &id()).unwrap();
    let path = root.join(&m.snapshot);
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[55] ^= 1;
    m.sha256 = format!("{:x}", Sha256::digest(&bytes));
    std::fs::write(path, bytes).unwrap();
    write_manifest(&root, &m);
    assert!(matches!(c.verify(&id()), Err(OpsError::Core(_))));
}
#[test]
fn malformed_json_and_missing_blob_reject() {
    let (root, c, a) = fixture();
    let m = c.backup(&a, &id()).unwrap();
    for b in [
        b"{".as_slice(),
        b"null",
        b"[]",
        b"{\"format_version\":1e9999}",
    ] {
        std::fs::write(root.join("backup-1.json"), b).unwrap();
        assert!(c.inspect(&id()).is_err());
    }
    let mut m = m;
    m.snapshot = format!("staging-{}/snapshot.gdb", "0".repeat(32));
    write_manifest(&root, &m);
    assert!(c.verify(&id()).is_err());
}
#[test]
fn paths_reject_symlinks_or_reparse() {
    let real = fresh("real");
    let parent = fresh("links");
    let link = parent.join("link");
    #[cfg(unix)]
    std::os::unix::fs::symlink(&real, &link).unwrap();
    #[cfg(windows)]
    {
        let status = std::process::Command::new("cmd")
            .args(["/c", "mklink", "/J"])
            .arg(&link)
            .arg(&real)
            .output()
            .unwrap();
        assert!(status.status.success(), "{:?}", status);
    }
    assert!(matches!(
        Catalog::open(&link, OpsLimits::default()),
        Err(OpsError::UnsafePath)
    ));
    let (_, c, a) = fixture();
    let mut m = c.backup(&a, &id()).unwrap();
    m.snapshot = format!("staging-{}/snapshot.gdb", "0".repeat(32));
    let stage = c.root().join(format!("staging-{}", "0".repeat(32)));
    #[cfg(unix)]
    std::os::unix::fs::symlink(&real, &stage).unwrap();
    #[cfg(windows)]
    assert!(
        std::process::Command::new("cmd")
            .args(["/c", "mklink", "/J"])
            .arg(&stage)
            .arg(&real)
            .output()
            .unwrap()
            .status
            .success()
    );
    write_manifest(c.root(), &m);
    assert!(matches!(c.verify(&id()), Err(OpsError::UnsafePath)));
    assert!(c.list().is_err());
}

#[test]
fn backup_small_admission_retains_incomplete_only() {
    for limits in [
        OpsLimits {
            manifest_bytes: 1,
            ..Default::default()
        },
        OpsLimits {
            snapshot_bytes: 52,
            ..Default::default()
        },
    ] {
        let (root, _, a) = fixture();
        let c = Catalog::open(&root, limits).unwrap();
        assert!(matches!(c.backup(&a, &id()), Err(OpsError::Limit)));
        assert!(!root.join("backup-1.json").exists());
        assert!(
            c.list()
                .unwrap()
                .iter()
                .all(|entry| matches!(entry, CatalogEntry::Incomplete { .. }))
        );
    }
}

#[test]
fn catalog_admission_nonblocking_and_reserves_publication_capacity() {
    let (root, c, a) = fixture();
    let path = root.join(".ops-admission");
    let lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&path)
        .unwrap();
    lock.try_lock().unwrap();
    assert!(matches!(c.backup(&a, &id()), Err(OpsError::Busy)));
    assert!(!root.join("backup-1.json").exists());
    drop(lock);
    let small = Catalog::open(
        &root,
        OpsLimits {
            catalog_entries: 2,
            ..Default::default()
        },
    )
    .unwrap();
    assert!(matches!(small.backup(&a, &id()), Err(OpsError::Limit)));
    assert_eq!(std::fs::read_dir(&root).unwrap().count(), 1);
    let exact = Catalog::open(
        &root,
        OpsLimits {
            catalog_entries: 3,
            ..Default::default()
        },
    )
    .unwrap();
    exact.backup(&a, &id()).unwrap();
    assert_eq!(exact.list().unwrap().len(), 2);
    assert!(matches!(
        exact.backup(&a, &BackupId::new("next").unwrap()),
        Err(OpsError::Limit)
    ));
}

#[cfg(windows)]
#[test]
fn locked_snapshot_and_manifest_fail_closed() {
    use std::os::windows::fs::OpenOptionsExt;
    let (root, c, a) = fixture();
    let m = c.backup(&a, &id()).unwrap();
    let lock = std::fs::OpenOptions::new()
        .read(true)
        .share_mode(0)
        .open(root.join(&m.snapshot))
        .unwrap();
    assert!(c.verify(&id()).is_err());
    drop(lock);
    c.verify(&id()).unwrap();
    let lock = std::fs::OpenOptions::new()
        .read(true)
        .share_mode(0)
        .open(root.join("backup-1.json"))
        .unwrap();
    assert!(c.inspect(&id()).is_err());
    drop(lock);
    c.inspect(&id()).unwrap();
}
